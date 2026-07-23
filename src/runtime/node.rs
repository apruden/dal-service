//! The assembled node process (DESIGN §10–11, M8).
//!
//! Owns one [`Storage`], the meta group (when this node is a meta voter), the
//! data partitions it hosts, the client gateway, and the inbound control/bulk
//! `ROUTER` servers — all over the production ZMQ transport. [`Node::start`]
//! wires the pieces and binds the sockets; [`Node::bootstrap`] drives the
//! resumable genesis (meta initialize + seed, then each hosted data group's
//! genesis) and is idempotent, so a restart re-runs it harmlessly.
//!
//! Scope (M8 slice): a static cluster derived from a [`BootstrapDescriptor`].
//! The dynamic control loops — heartbeat-driven liveness and reconcile/rebalance
//! — are a follow-up; membership here is the genesis placement.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use crate::api::gateway::{ClientGateway, PartitionMap, RoutingSource};
use crate::api::ops::RoutingInfo;
use crate::config::{NodeConfig, RaftTuning, Timeouts};
use crate::error::Result;
use crate::meta::bootstrap::{self, BootstrapDescriptor};
use crate::meta::failure::HeartbeatTracker;
use crate::meta::node::MetaNode;
use crate::partition::node::PartitionNode;
use crate::runtime::config_file::cluster_id_hex;
use crate::runtime::dispatch::{RootDispatch, now_ms};
use crate::runtime::http::{
    ClusterStatus, MetaStatus, PartitionStatus, PlanStatus, Role, StatusSource,
};
use crate::runtime::rebalance::RebalanceDriver;
use crate::storage::Storage;
use crate::transport::dealer::ZmqTransport;
use crate::transport::raft_net::{AddrBook, RaftPeerFactory};
use crate::transport::router::ZmqServer;
use crate::transport::{
    Transport,
    codec::{Envelope, MsgType},
    raft_wire::{BootstrapStatusBody, BootstrapStatusReply, HeartbeatBody},
};
use crate::types::{
    BootstrapGroup, ClusterConfig, ClusterId, GroupId, LearnerAdmission, LogId, MetaCommand,
    NodeDirectoryEntry, NodeId, NodeState, Placement,
};

/// How long [`Node::bootstrap`] waits for genesis to seed and replicate before
/// failing loudly, rather than hanging `dal run` when a quorum never forms.
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll cadence while waiting for genesis to complete.
const BOOTSTRAP_POLL: Duration = Duration::from_millis(50);

/// A routing snapshot fixed at genesis from the bootstrap descriptor. Routing is
/// advisory (the serving gate is authority), so a static snapshot is sufficient
/// for redirects and client `MetaQuery` replies until the reconcile loop lands.
struct StaticRouting(RoutingInfo);

impl StaticRouting {
    fn from_descriptor(desc: &BootstrapDescriptor) -> StaticRouting {
        let directory = desc
            .directory
            .iter()
            .map(|d| NodeDirectoryEntry {
                node_id: d.node_id,
                control_addr: d.control_addr.clone(),
                bulk_addr: d.bulk_addr.clone(),
                state: NodeState::Active,
                incarnation: 0,
            })
            .collect();
        let placements = desc
            .data_placements
            .iter()
            .map(|(p, voters)| {
                (
                    *p,
                    Placement {
                        voters: voters.clone(),
                        voters_log_id: LogId::new(0, 0),
                        r#move: None,
                    },
                )
            })
            .collect();
        StaticRouting(RoutingInfo {
            cluster_id: desc.cluster_id,
            p: desc.config.p,
            hash_spec: desc.config.hash_spec,
            directory,
            placements,
        })
    }
}

impl RoutingSource for StaticRouting {
    fn routing(&self) -> RoutingInfo {
        self.0.clone()
    }
}

pub struct Node {
    node_id: NodeId,
    cluster: ClusterConfig,
    meta_voters: Vec<NodeId>,
    hosted: Vec<(u16, Vec<NodeId>)>,
    meta: Option<Arc<MetaNode>>,
    partitions: PartitionMap,
    desc: BootstrapDescriptor,
    control: ZmqTransport,
    meta_controls: Vec<String>,
    // Background control loops (heartbeat emitter, failure detector); aborted on
    // shutdown.
    tasks: Vec<JoinHandle<()>>,
    // Dropping these stops the poller threads and closes the sockets.
    _control_srv: ZmqServer,
    _bulk_srv: ZmqServer,
}

impl Node {
    /// Assemble and start the node: open storage, start every group this node is
    /// a voter of over the ZMQ network, and bind the control and bulk servers.
    /// Does not drive genesis — call [`Node::bootstrap`] once the peers are up.
    pub async fn start(
        ctx: zmq::Context,
        cfg: NodeConfig,
        desc: BootstrapDescriptor,
    ) -> Result<Node> {
        validate_node_descriptor(&cfg, &desc)?;

        // Bind the optional admin listener before starting Raft or background
        // tasks. A malformed or occupied address now fails without leaving
        // detached work behind.
        let http_listener = match &cfg.http_addr {
            Some(http_addr) => {
                let addr: std::net::SocketAddr = http_addr.parse().map_err(|e| {
                    crate::error::Error::Config(format!("http_addr {http_addr}: {e}"))
                })?;
                Some(
                    tokio::net::TcpListener::bind(addr)
                        .await
                        .map_err(crate::error::Error::Io)?,
                )
            }
            None => None,
        };
        let storage = Arc::new(Storage::open_checked(
            &cfg.data_dir,
            cfg.cluster_id,
            cfg.node_id,
        )?);
        let heartbeat_incarnation = storage.next_heartbeat_incarnation()?;

        let cluster = desc.config.clone();
        let is_meta_voter = desc.meta_voters.contains(&cfg.node_id);
        let hosted: Vec<(u16, Vec<NodeId>)> = desc
            .data_placements
            .iter()
            .filter(|(_, voters)| voters.contains(&cfg.node_id))
            .cloned()
            .collect();

        // Durable admission markers so `authorize_group_start` accepts each group
        // (ground rule 8: state is created only via bootstrap or learner records).
        if is_meta_voter {
            bootstrap::ensure_bootstrap_group(
                &storage,
                &BootstrapGroup {
                    cluster_id: cfg.cluster_id,
                    group: GroupId::Meta,
                    members: desc.meta_voters.clone(),
                },
            )?;
        }
        for (p, voters) in &hosted {
            bootstrap::ensure_bootstrap_group(
                &storage,
                &BootstrapGroup {
                    cluster_id: cfg.cluster_id,
                    group: GroupId::Data(*p),
                    members: voters.clone(),
                },
            )?;
        }

        // Directory for peer address resolution.
        let addrs = AddrBook::new();
        for d in &desc.directory {
            addrs.set(d.node_id, d.control_addr.clone(), d.bulk_addr.clone());
        }
        let meta_controls: Vec<String> = desc
            .directory
            .iter()
            .filter(|d| desc.meta_voters.contains(&d.node_id))
            .map(|d| d.control_addr.clone())
            .collect();

        let control = ZmqTransport::new(ctx.clone(), cfg.timeouts.request);
        let bulk = ZmqTransport::new(ctx.clone(), cfg.timeouts.request);
        let tuning = RaftTuning::default();

        let meta = if is_meta_voter {
            let net = RaftPeerFactory::new(
                GroupId::Meta,
                cfg.cluster_id,
                addrs.clone(),
                control.clone(),
                bulk.clone(),
            );
            Some(Arc::new(
                MetaNode::start_with_network(cfg.node_id, storage.clone(), net, tuning).await?,
            ))
        } else {
            None
        };

        let mut map: HashMap<u16, Arc<PartitionNode>> = HashMap::new();
        for (p, _) in &hosted {
            let group = GroupId::Data(*p);
            let net = RaftPeerFactory::new(
                group,
                cfg.cluster_id,
                addrs.clone(),
                control.clone(),
                bulk.clone(),
            );
            let node =
                PartitionNode::start_with_network(cfg.node_id, group, storage.clone(), net, tuning)
                    .await?;
            map.insert(*p, Arc::new(node));
        }
        let partitions: PartitionMap = Arc::new(RwLock::new(map));

        let routing = Arc::new(StaticRouting::from_descriptor(&desc));
        let gateway = Arc::new(ClientGateway::new(
            cfg.cluster_id,
            cluster.p,
            cluster.hash_spec,
            partitions.clone(),
            routing,
        ));
        let heartbeats = Arc::new(Mutex::new(HeartbeatTracker::new()));
        let starter = Arc::new(PartitionStarter {
            node_id: cfg.node_id,
            cluster_id: cfg.cluster_id,
            storage: storage.clone(),
            addrs: addrs.clone(),
            control: control.clone(),
            bulk: bulk.clone(),
            tuning,
            partitions: partitions.clone(),
        });
        let dispatch = Arc::new(RootDispatch::new(
            cfg.cluster_id,
            gateway,
            meta.clone(),
            partitions.clone(),
            heartbeats.clone(),
            Some(starter),
        ));

        let control_srv = ZmqServer::bind(ctx.clone(), &cfg.control_addr, dispatch.clone())?;
        let bulk_srv = ZmqServer::bind(ctx.clone(), &cfg.bulk_addr, dispatch.clone())?;

        // Control loops: every node beats to the meta voters; the meta leader
        // turns the collected evidence into directory transitions.
        let hb_interval = (cfg.timeouts.suspect / 3).max(Duration::from_millis(50));
        let hb_control = ZmqTransport::new(ctx.clone(), hb_interval);
        let mut tasks = Vec::new();
        tasks.push(tokio::spawn(heartbeat_emitter(
            cfg.node_id,
            cfg.cluster_id,
            heartbeat_incarnation,
            hb_control,
            meta_controls.clone(),
            hb_interval,
        )));
        if let Some(meta) = &meta {
            tasks.push(tokio::spawn(failure_detector(
                meta.clone(),
                heartbeats.clone(),
                cfg.timeouts,
                hb_interval,
            )));
        }
        // The rebalance driver runs on every node: its data-leader role must run
        // wherever a partition is led, including on non-meta-voter nodes (which
        // read the plan and report observations over the network).
        let driver = RebalanceDriver::new(
            cfg.node_id,
            cfg.cluster_id,
            cluster.p,
            meta.clone(),
            partitions.clone(),
            control.clone(),
            addrs.clone(),
            meta_controls.clone(),
        );
        tasks.push(tokio::spawn(driver.run()));

        // Read-only HTTP admin plane. Its listener was bound before runtime
        // assembly; the serving task is aborted on shutdown with the other
        // loops.
        if let Some(listener) = http_listener {
            let src: Arc<dyn StatusSource> = Arc::new(NodeStatus {
                node_id: cfg.node_id,
                cluster: cluster.clone(),
                meta: meta.clone(),
                partitions: partitions.clone(),
            });
            tasks.push(tokio::spawn(async move {
                let _ = axum::serve(listener, crate::runtime::http::router(src)).await;
            }));
        }

        Ok(Node {
            node_id: cfg.node_id,
            cluster,
            meta_voters: desc.meta_voters.clone(),
            hosted,
            meta,
            partitions,
            desc,
            control,
            meta_controls,
            tasks,
            _control_srv: control_srv,
            _bulk_srv: bulk_srv,
        })
    }

    /// Drive the resumable genesis: the designated meta voter initializes and
    /// seeds the cluster; the designated voter of each hosted data group
    /// initializes it. Idempotent — a resumed node whose groups are already
    /// initialized does nothing.
    pub async fn bootstrap(&self) -> Result<()> {
        if let Some(meta) = &self.meta {
            if !meta.is_initialized().await?
                && bootstrap::designated(&self.meta_voters) == Some(self.node_id)
            {
                meta.initialize(&self.meta_voters).await?;
            }
            // Every meta process participates in resumption. Only the current
            // leader submits; followers wait until that leader's seeded
            // placement has replicated locally instead of failing startup.
            let deadline = Instant::now() + BOOTSTRAP_TIMEOUT;
            while !self.meta_seeded_locally(meta)? {
                if meta.current_leader() == Some(self.node_id) {
                    bootstrap::seed_cluster_if_leader(meta, &self.desc).await?;
                }
                if Instant::now() >= deadline {
                    return Err(crate::error::Error::Raft(format!(
                        "node {} timed out waiting for meta genesis to seed",
                        self.node_id
                    )));
                }
                tokio::time::sleep(BOOTSTRAP_POLL).await;
            }
        }

        for (p, voters) in &self.hosted {
            if bootstrap::designated(voters) != Some(self.node_id) {
                continue;
            }
            // `.cloned()` drops the read guard before any await below.
            let Some(node) = self.partitions.read().unwrap().get(p).cloned() else {
                continue;
            };
            let deadline = Instant::now() + BOOTSTRAP_TIMEOUT;
            while !self.placement_seeded(*p, voters).await? {
                if Instant::now() >= deadline {
                    return Err(crate::error::Error::Raft(format!(
                        "node {} timed out waiting for placement {p} to seed",
                        self.node_id
                    )));
                }
                tokio::time::sleep(BOOTSTRAP_POLL).await;
            }
            if !node.is_initialized().await? {
                node.initialize(voters).await?;
            }
        }
        Ok(())
    }

    fn meta_seeded_locally(&self, meta: &MetaNode) -> Result<bool> {
        self.desc
            .data_placements
            .iter()
            .try_fold(true, |ready, (p, voters)| {
                if !ready {
                    return Ok(false);
                }
                Ok(matches!(
                    meta.local_placement(GroupId::Data(*p))?,
                    Some(placement) if placement.voters == *voters && placement.r#move.is_none()
                ))
            })
    }

    async fn placement_seeded(&self, partition: u16, voters: &[NodeId]) -> Result<bool> {
        if let Some(meta) = &self.meta {
            return Ok(matches!(
                meta.local_placement(GroupId::Data(partition))?,
                Some(placement) if placement.voters == voters && placement.r#move.is_none()
            ));
        }

        let body = BootstrapStatusBody {
            group: GroupId::Data(partition),
            voters: voters.to_vec(),
        };
        for addr in &self.meta_controls {
            let request = Envelope::new(
                self.desc.cluster_id,
                MsgType::BootstrapStatus,
                GroupId::Meta,
                0,
                crate::codec::encode(&body),
            );
            let Ok(reply) = self.control.call(addr, request).await else {
                continue;
            };
            if reply.cluster_id != self.desc.cluster_id
                || reply.msg_type != MsgType::BootstrapStatus
            {
                continue;
            }
            if let Ok(BootstrapStatusReply { ready: true }) = crate::codec::decode(&reply.payload) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Whether this node currently hosts the given data partition.
    pub fn hosts_partition(&self, partition: u16) -> bool {
        self.partitions.read().unwrap().contains_key(&partition)
    }

    /// The data-Raft leader this node believes serves `partition`, if it hosts
    /// it. Used by tests to pick a non-leader replica to drain.
    pub fn partition_leader(&self, partition: u16) -> Option<NodeId> {
        self.partitions
            .read()
            .unwrap()
            .get(&partition)
            .and_then(|n| n.current_leader())
    }

    /// The committed directory state of `node_id` as seen locally, if this node
    /// runs the meta group. Used by tests to observe failure-detector output.
    pub fn local_node_state(&self, node_id: NodeId) -> Result<Option<NodeState>> {
        match &self.meta {
            Some(meta) => Ok(meta.local_node(node_id)?.map(|e| e.state)),
            None => Ok(None),
        }
    }

    /// The committed voter set of a partition's meta placement, and whether a
    /// move is still in flight. Used by tests to observe rebalance completion.
    pub fn local_placement_voters(&self, partition: u16) -> Result<Option<(Vec<NodeId>, bool)>> {
        match &self.meta {
            Some(meta) => Ok(meta
                .local_placement(GroupId::Data(partition))?
                .map(|p| (p.voters, p.r#move.is_some()))),
            None => Ok(None),
        }
    }

    pub fn cluster(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// Gracefully stop: shut down every Raft group, then drop the inbound
    /// servers (their `Drop` stops the poller threads).
    pub async fn shutdown(self) -> Result<()> {
        for task in &self.tasks {
            task.abort();
        }
        if let Some(meta) = &self.meta {
            meta.shutdown().await?;
        }
        let partitions: Vec<Arc<PartitionNode>> =
            self.partitions.read().unwrap().values().cloned().collect();
        for node in partitions {
            node.shutdown().await?;
        }
        Ok(())
    }
}

/// Builds the read-only `/status` snapshot from node-local state. Holds the same
/// shared handles as the node, so the HTTP task sees live partition membership.
struct NodeStatus {
    node_id: NodeId,
    cluster: ClusterConfig,
    meta: Option<Arc<MetaNode>>,
    partitions: PartitionMap,
}

impl StatusSource for NodeStatus {
    fn status(&self) -> ClusterStatus {
        let meta = self.meta.as_ref().map(|m| MetaStatus {
            is_leader: m.current_leader() == Some(self.node_id),
            leader: m.current_leader(),
            applied: m.applied_index(),
            voters: m.voters(),
        });
        let directory = self
            .meta
            .as_ref()
            .and_then(|m| m.local_directory().ok())
            .unwrap_or_default();

        let mut partitions: Vec<PartitionStatus> = {
            let map = self.partitions.read().unwrap();
            map.iter()
                .map(|(&partition, node)| {
                    let committed = node.committed_voter_set();
                    let leader = node.current_leader();
                    let role = if leader == Some(self.node_id) {
                        Role::Leader
                    } else if committed.contains(&self.node_id) {
                        Role::Voter
                    } else {
                        Role::Learner
                    };
                    let plan = self
                        .meta
                        .as_ref()
                        .and_then(|m| m.local_placement(GroupId::Data(partition)).ok().flatten())
                        .and_then(|p| p.r#move)
                        .map(|mv| PlanStatus {
                            plan_id: mv.plan_id,
                            aborting: mv.aborting,
                        });
                    PartitionStatus {
                        partition,
                        role,
                        leader,
                        applied: node.applied_index(),
                        committed_voters: committed.into_iter().collect(),
                        serving: node.is_serving(),
                        plan,
                    }
                })
                .collect()
        };
        partitions.sort_by_key(|s| s.partition);

        ClusterStatus {
            node_id: self.node_id,
            cluster_id: cluster_id_hex(self.cluster.cluster_id),
            protocol_version: self.cluster.protocol_version,
            meta,
            partitions,
            directory,
        }
    }
}

/// Starts a data partition as a learner in response to a `BecomeLearner` frame
/// (DESIGN §7.2). Holds exactly what starting a `PartitionNode` over the ZMQ
/// network needs, plus the shared partition map to publish the new group into.
pub struct PartitionStarter {
    node_id: NodeId,
    cluster_id: ClusterId,
    storage: Arc<Storage>,
    addrs: AddrBook,
    control: ZmqTransport,
    bulk: ZmqTransport,
    tuning: RaftTuning,
    partitions: PartitionMap,
}

impl PartitionStarter {
    /// Admit this node as a learner for `group` under `plan_id`: write the
    /// durable admission record (the second lawful way group state is created —
    /// ground rule 8), start the partition's Raft runtime as an uninitialized
    /// learner, and publish it so the gateway and dispatcher can serve/route it.
    /// Idempotent: a group already hosted returns `Ok` without restarting.
    pub async fn admit_learner(&self, group: GroupId, plan_id: u64) -> Result<()> {
        let GroupId::Data(partition) = group else {
            return Err(crate::error::Error::Raft(format!(
                "cannot admit a learner for {group:?}"
            )));
        };
        if self.partitions.read().unwrap().contains_key(&partition) {
            return Ok(());
        }

        bootstrap::ensure_learner_admission(
            &self.storage,
            &LearnerAdmission {
                cluster_id: self.cluster_id,
                group,
                plan_id,
            },
        )?;

        let net = RaftPeerFactory::new(
            group,
            self.cluster_id,
            self.addrs.clone(),
            self.control.clone(),
            self.bulk.clone(),
        );
        let node = PartitionNode::start_with_network(
            self.node_id,
            group,
            self.storage.clone(),
            net,
            self.tuning,
        )
        .await?;
        self.partitions
            .write()
            .unwrap()
            .insert(partition, Arc::new(node));
        Ok(())
    }
}

/// Every node periodically sends liveness evidence to the meta voters. Sends are
/// concurrent with a short timeout so one unreachable voter cannot stall the
/// round and make this node look silent to the others (DESIGN §9.1).
async fn heartbeat_emitter(
    node_id: NodeId,
    cluster_id: crate::types::ClusterId,
    incarnation: u64,
    control: ZmqTransport,
    meta_controls: Vec<String>,
    interval: Duration,
) {
    let mut seq = 1u64;
    loop {
        let body = HeartbeatBody {
            node_id,
            incarnation,
            seq,
        };
        let payload = crate::codec::encode(&body);
        let sends = meta_controls.iter().map(|addr| {
            let env = Envelope::new(
                cluster_id,
                MsgType::Heartbeat,
                GroupId::Meta,
                0,
                payload.clone(),
            );
            control.call(addr, env)
        });
        let _ = futures::future::join_all(sends).await;
        seq = seq.wrapping_add(1);
        tokio::time::sleep(interval).await;
    }
}

/// The meta leader's failure detector: turn collected heartbeat evidence into
/// committed directory transitions. Only the current leader proposes; the
/// incarnation guard in the meta state machine rejects a transition against a
/// rejoined node (DESIGN §9.1).
async fn failure_detector(
    meta: Arc<MetaNode>,
    tracker: Arc<Mutex<HeartbeatTracker>>,
    timeouts: Timeouts,
    interval: Duration,
) {
    loop {
        tokio::time::sleep(interval).await;
        if meta.current_leader() != Some(meta.node_id()) {
            continue;
        }
        let Ok(directory) = meta.local_directory() else {
            continue;
        };
        let states: Vec<(NodeId, NodeState)> =
            directory.iter().map(|e| (e.node_id, e.state)).collect();
        let transitions = {
            let tracker = tracker.lock().unwrap();
            tracker.evaluate(now_ms(), &timeouts, &states)
        };
        for (node_id, state) in transitions {
            let incarnation = directory
                .iter()
                .find(|e| e.node_id == node_id)
                .map(|e| e.incarnation)
                .unwrap_or(0);
            let _ = meta
                .propose(MetaCommand::SetNodeState {
                    node_id,
                    state,
                    incarnation,
                })
                .await;
        }
    }
}

fn validate_node_descriptor(cfg: &NodeConfig, desc: &BootstrapDescriptor) -> Result<()> {
    cfg.validate()?;
    if cfg.cluster_id != desc.cluster_id || desc.config.cluster_id != desc.cluster_id {
        return Err(crate::error::Error::Config(
            "node config and bootstrap descriptor disagree on cluster_id".into(),
        ));
    }
    let Some(directory_entry) = desc.directory.iter().find(|d| d.node_id == cfg.node_id) else {
        return Err(crate::error::Error::Config(format!(
            "node {} is not present in the bootstrap descriptor",
            cfg.node_id
        )));
    };
    if directory_entry.control_addr != cfg.control_addr
        || directory_entry.bulk_addr != cfg.bulk_addr
    {
        return Err(crate::error::Error::Config(format!(
            "node {} addresses do not match the bootstrap descriptor",
            cfg.node_id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::bootstrap::DirEntry;
    use crate::types::{HashSpec, PROTOCOL_VERSION};

    fn descriptor() -> BootstrapDescriptor {
        BootstrapDescriptor {
            cluster_id: 7,
            config: ClusterConfig {
                cluster_id: 7,
                protocol_version: PROTOCOL_VERSION,
                p: 1,
                r: 3,
                hash_spec: HashSpec::CANONICAL,
            },
            meta_voters: vec![1, 2, 3],
            directory: vec![DirEntry {
                node_id: 1,
                control_addr: "tcp://node-1-control".into(),
                bulk_addr: "tcp://node-1-bulk".into(),
            }],
            data_placements: vec![(0, vec![1, 2, 3])],
        }
    }

    fn config(node_id: NodeId) -> NodeConfig {
        NodeConfig {
            cluster_id: 7,
            node_id,
            control_addr: "tcp://node-1-control".into(),
            bulk_addr: "tcp://node-1-bulk".into(),
            http_addr: None,
            seeds: Vec::new(),
            data_dir: std::path::PathBuf::from("/tmp/dal-node-test"),
            timeouts: crate::config::Timeouts::default(),
        }
    }

    #[test]
    fn rejects_config_outside_descriptor_before_opening_storage() {
        let err = validate_node_descriptor(&config(9), &descriptor()).unwrap_err();
        assert!(
            err.to_string()
                .contains("not present in the bootstrap descriptor")
        );
    }

    #[test]
    fn rejects_address_mismatch_before_opening_storage() {
        let mut cfg = config(1);
        cfg.control_addr = "tcp://wrong-control".into();
        let err = validate_node_descriptor(&cfg, &descriptor()).unwrap_err();
        assert!(err.to_string().contains("addresses do not match"));
    }

    #[tokio::test]
    async fn invalid_http_address_fails_before_opening_storage_or_starting_tasks() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("node-data");
        let mut cfg = config(1);
        cfg.data_dir = data_dir.clone();
        cfg.http_addr = Some("not-a-socket-address".into());

        let err = match Node::start(zmq::Context::new(), cfg, descriptor()).await {
            Ok(_) => panic!("invalid HTTP address unexpectedly started a node"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("http_addr"));
        assert!(
            !data_dir.exists(),
            "storage must not be opened on a bad bind"
        );
    }
}
