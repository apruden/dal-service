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
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::api::gateway::{ClientGateway, PartitionMap, RoutingSource};
use crate::api::ops::RoutingInfo;
use crate::config::{NodeConfig, RaftTuning};
use crate::error::Result;
use crate::meta::bootstrap::{self, BootstrapDescriptor};
use crate::meta::node::MetaNode;
use crate::partition::node::PartitionNode;
use crate::runtime::dispatch::RootDispatch;
use crate::storage::Storage;
use crate::transport::dealer::ZmqTransport;
use crate::transport::raft_net::{AddrBook, RaftPeerFactory};
use crate::transport::router::ZmqServer;
use crate::transport::{
    Transport,
    codec::{Envelope, MsgType},
    raft_wire::{BootstrapStatusBody, BootstrapStatusReply},
};
use crate::types::{
    BootstrapGroup, ClusterConfig, GroupId, LogId, NodeDirectoryEntry, NodeId, NodeState, Placement,
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
        let storage = Arc::new(Storage::open_checked(
            &cfg.data_dir,
            cfg.cluster_id,
            cfg.node_id,
        )?);

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
        let dispatch = Arc::new(RootDispatch::new(
            cfg.cluster_id,
            gateway,
            meta.clone(),
            partitions.clone(),
        ));

        let control_srv = ZmqServer::bind(ctx.clone(), &cfg.control_addr, dispatch.clone())?;
        let bulk_srv = ZmqServer::bind(ctx.clone(), &cfg.bulk_addr, dispatch.clone())?;

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

    pub fn cluster(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// Gracefully stop: shut down every Raft group, then drop the inbound
    /// servers (their `Drop` stops the poller threads).
    pub async fn shutdown(self) -> Result<()> {
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
}
