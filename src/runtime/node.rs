//! The assembled node process (DESIGN §10–11, M8).
//!
//! Owns one [`Storage`], the meta group (when this node is a meta voter), the
//! data partitions it hosts, the client gateway, and the inbound control/bulk
//! `ROUTER` servers — all over the production ZMQ transport. [`Node::start`]
//! wires the pieces and binds the sockets; [`Node::bootstrap`] drives the
//! resumable genesis (meta initialize + seed, then each hosted data group's
//! genesis) and is idempotent, so a restart re-runs it harmlessly.
//!
//! The bootstrap descriptor supplies immutable identity and genesis membership;
//! heartbeat-driven liveness, fenced rebalance, reclamation, and live routing
//! keep the running membership synchronized with committed meta state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::api::gateway::{ClientGateway, PartitionMap, RoutingSource, SearchCatalogSource};
use crate::api::ops::{RoutingInfo, RoutingQuery, SearchCatalogQuery, SearchCatalogReply};
use crate::config::{InitConfig, NodeConfig, RaftTuning, Timeouts, validate_routing_shape};
use crate::error::{Error, Result};
use crate::meta::bootstrap::{self, BootstrapDescriptor};
use crate::meta::failure::HeartbeatTracker;
use crate::meta::node::{MetaNode, MetaRead};
use crate::partition::node::PartitionNode;
use crate::runtime::config_file::cluster_id_hex;
use crate::runtime::directory::fetch_authoritative_directory;
use crate::runtime::dispatch::{RootDispatch, now_ms};
use crate::runtime::http::{
    ClusterStatus, MetaStatus, PartitionStatus, PlanStatus, Role, StatusSource,
};
use crate::runtime::readiness::RuntimeReadiness;
use crate::runtime::rebalance::RebalanceDriver;
use crate::storage::Storage;
use crate::transport::dealer::ZmqTransport;
use crate::transport::raft_net::{AddrBook, RaftPeerFactory};
use crate::transport::router::ZmqServer;
use crate::transport::{
    Transport,
    codec::{Envelope, Lane, MsgType},
    raft_wire::{
        BootstrapStatusBody, BootstrapStatusReply, HeartbeatBody, PlacementQueryBody,
        PlacementQueryReply, RecoveryFenceReply, RecoveryFenceRequest,
    },
};
use crate::types::{
    BootstrapGroup, ClusterConfig, ClusterId, GroupId, LearnerAdmission, LogId, MetaCommand,
    NodeDirectoryEntry, NodeId, NodeState, PROTOCOL_VERSION, Placement, ProcessIdentityGate,
    RegistrationBinding, ServingState,
};

/// How long [`Node::bootstrap`] waits for genesis to seed and replicate before
/// failing loudly, rather than hanging `dal run` when a quorum never forms.
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll cadence while waiting for genesis to complete.
const BOOTSTRAP_POLL: Duration = Duration::from_millis(50);

/// The node's meta-group runtime, shared and mutable so a node can start hosting
/// the meta group at runtime or stop after a drain.
pub type MetaHandle = Arc<RwLock<Option<Arc<MetaNode>>>>;

/// Live advisory routing. A meta host answers from its locally-applied state;
/// another node proxies to a descriptor peer with a meta handle. The immutable
/// descriptor remains only a bootstrap fallback while meta is unavailable.
struct LiveRouting {
    fallback: RoutingInfo,
    meta: MetaHandle,
    control: ZmqTransport,
    addrs: AddrBook,
    local_control: String,
}

impl LiveRouting {
    fn from_descriptor(
        desc: &BootstrapDescriptor,
        meta: MetaHandle,
        control: ZmqTransport,
        addrs: AddrBook,
        local_control: String,
    ) -> LiveRouting {
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
        LiveRouting {
            fallback: RoutingInfo {
                cluster_id: desc.cluster_id,
                p: desc.config.p,
                hash_spec: desc.config.hash_spec,
                directory,
                placements,
            },
            meta,
            control,
            addrs,
            local_control,
        }
    }

    fn local(&self) -> Option<RoutingInfo> {
        let meta = self.meta.read().unwrap().clone()?;
        let directory = meta.local_directory().ok()?;
        if directory.is_empty() {
            return None;
        }
        let placements: Vec<_> = (0..self.fallback.p)
            .filter_map(|p| {
                meta.local_placement(GroupId::Data(p))
                    .ok()
                    .flatten()
                    .map(|placement| (p, placement))
            })
            .collect();
        // A newly admitted meta learner is published before it has installed
        // or replayed the cluster snapshot. Do not advertise that transient
        // empty/partial view: proxy to a caught-up replica (or use the complete
        // bootstrap fallback) until every invariant data placement is present.
        if placements.len() != self.fallback.p as usize {
            return None;
        }
        Some(RoutingInfo {
            cluster_id: self.fallback.cluster_id,
            p: self.fallback.p,
            hash_spec: self.fallback.hash_spec,
            directory,
            placements,
        })
    }
}

impl RoutingSource for LiveRouting {
    fn routing(
        &self,
        local_only: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<RoutingInfo>> + Send + '_>> {
        Box::pin(async move {
            if let Some(local) = self.local() {
                return Some(local);
            }
            if local_only {
                return None;
            }

            let payload = crate::codec::encode(&RoutingQuery { local_only: true });
            for addr in self.addrs.control_addrs() {
                if addr == self.local_control.as_str() {
                    continue;
                }
                let request = Envelope::new(
                    self.fallback.cluster_id,
                    MsgType::MetaQuery,
                    GroupId::Meta,
                    0,
                    payload.clone(),
                );
                let Ok(reply) = self.control.call(&addr, request).await else {
                    continue;
                };
                if reply.cluster_id != self.fallback.cluster_id
                    || reply.msg_type != MsgType::MetaQuery
                {
                    continue;
                }
                if let Ok(info) = crate::codec::decode::<RoutingInfo>(&reply.payload)
                    && info.cluster_id == self.fallback.cluster_id
                {
                    return Some(info);
                }
            }
            Some(self.fallback.clone())
        })
    }
}

struct LiveSearchCatalog {
    cluster_id: ClusterId,
    meta: MetaHandle,
    control: ZmqTransport,
    addrs: AddrBook,
    local_control: String,
}

impl SearchCatalogSource for LiveSearchCatalog {
    fn index(
        &self,
        name: String,
        local_only: bool,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<Option<crate::search::SearchIndexRecord>, String>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let local_meta = self.meta.read().unwrap().clone();
            if let Some(meta) = local_meta {
                match meta.read_search_index(name.clone()).await {
                    Ok(MetaRead::Value(record)) => return Ok(record),
                    Ok(MetaRead::NotLeader { .. }) => {}
                    Err(error) if local_only => return Err(error.to_string()),
                    Err(_) => {}
                }
            }
            if local_only {
                return Err("local node is not the current meta leader".into());
            }
            let payload = crate::codec::encode(&SearchCatalogQuery {
                name,
                local_only: true,
                all: false,
            });
            for reply in self.ask_meta_voters(payload).await {
                if let SearchCatalogReply::Record(record) = reply {
                    return Ok(record);
                }
            }
            Err("no meta leader answered the search catalog query".into())
        })
    }

    fn catalog(
        &self,
        local_only: bool,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<Vec<crate::search::SearchIndexRecord>, String>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let local_meta = self.meta.read().unwrap().clone();
            if let Some(meta) = local_meta
                && let Ok(MetaRead::Value(records)) = meta.read_search_catalog().await
            {
                return Ok(records);
            }
            if local_only {
                return Err("local node is not the current meta leader".into());
            }
            let payload = crate::codec::encode(&SearchCatalogQuery {
                name: String::new(),
                local_only: true,
                all: true,
            });
            for reply in self.ask_meta_voters(payload).await {
                if let SearchCatalogReply::Catalog(records) = reply {
                    return Ok(records);
                }
            }
            Err("no meta leader answered the search catalog query".into())
        })
    }
}

impl LiveSearchCatalog {
    /// Ask each peer in turn, yielding the replies that decoded. Only the meta
    /// leader answers with a record; followers report `Unavailable`.
    async fn ask_meta_voters(&self, payload: Vec<u8>) -> Vec<SearchCatalogReply> {
        let mut replies = Vec::new();
        for addr in self.addrs.control_addrs() {
            if addr == self.local_control {
                continue;
            }
            let request = Envelope::new(
                self.cluster_id,
                MsgType::SearchCatalogQuery,
                GroupId::Meta,
                0,
                payload.clone(),
            );
            let Ok(reply) = self.control.call(&addr, request).await else {
                continue;
            };
            if reply.cluster_id != self.cluster_id || reply.msg_type != MsgType::SearchCatalogQuery
            {
                continue;
            }
            if let Ok(decoded) = crate::codec::decode::<SearchCatalogReply>(&reply.payload) {
                replies.push(decoded);
            }
        }
        replies
    }
}

pub struct Node {
    node_id: NodeId,
    cluster: ClusterConfig,
    meta_voters: Vec<NodeId>,
    hosted: Vec<(u16, Vec<NodeId>)>,
    meta: MetaHandle,
    partitions: PartitionMap,
    desc: BootstrapDescriptor,
    control: ZmqTransport,
    /// Shared live address book. Bootstrap queries use the same discovery
    /// candidates as steady-state control work; descriptor endpoints are only
    /// initial hints and must not become a permanent bootstrap dependency.
    addrs: AddrBook,
    readiness: RuntimeReadiness,
    identity_gate: ProcessIdentityGate,
    search: Arc<crate::search::SearchService>,
    // Background control loops (heartbeat emitter, failure detector); aborted on
    // shutdown.
    tasks: Vec<JoinHandle<()>>,
    // Search reconciliation may be inside `spawn_blocking`, which Tokio cannot
    // abort once running. Signal it separately and await it during shutdown so
    // no RocksDB/Tantivy owner outlives the node.
    search_shutdown: watch::Sender<bool>,
    search_task: Option<JoinHandle<()>>,
    // Dropping these stops the poller threads and closes the sockets.
    _control_srv: Option<ZmqServer>,
    _bulk_srv: Option<ZmqServer>,
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
        let control = ZmqTransport::new(ctx.clone(), cfg.timeouts.request, Lane::Control);
        let bulk = ZmqTransport::new(ctx.clone(), cfg.timeouts.request, Lane::Bulk);
        let registration = resolve_registration_binding(&cfg, &desc, &storage, &control).await?;
        let identity_gate = ProcessIdentityGate::default();
        let readiness = RuntimeReadiness::default();
        let heartbeat_incarnation = storage.next_heartbeat_incarnation()?;

        let cluster = desc.config.clone();
        let genesis_meta_voter = desc.meta_voters.contains(&cfg.node_id);
        // A genesis meta voter that was drained and durably reclaimed must not
        // restart its meta group: `authorize_group_start` refuses a `NonServing`
        // group and would hard-fail startup.
        let is_meta_voter = genesis_meta_voter
            && storage.serving_state(GroupId::Meta)? != Some(ServingState::NonServing);
        let hosted: Vec<(u16, Vec<NodeId>)> = desc
            .data_placements
            .iter()
            .filter(|(_, voters)| voters.contains(&cfg.node_id))
            // A partition this node durably reclaimed after a drain stays gone:
            // never re-host a group whose local serving gate is `NonServing`, or
            // `authorize_group_start` would refuse it and fail startup.
            .filter(|(p, _)| {
                !matches!(
                    storage.serving_state(GroupId::Data(*p)),
                    Ok(Some(ServingState::NonServing))
                )
            })
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
        // Descriptor endpoints are discovery hints, not incarnation authority.
        // Leaving their incarnation unset lets the first ReadIndex-fenced
        // directory replace a stale immutable bootstrap address even when the
        // replicated entry is still at incarnation one.
        // A newly joined node is intentionally absent from the immutable
        // bootstrap descriptor. It still needs its own address in the live book
        // before the replicated directory refresh reaches it.
        addrs.set(cfg.node_id, cfg.control_addr.clone(), cfg.bulk_addr.clone());
        addrs.set_incarnation(cfg.node_id, registration.directory_incarnation);
        for seed in &cfg.seeds {
            addrs.add_control_seed(seed.clone());
        }
        let tuning = RaftTuning::default();

        let meta = if is_meta_voter {
            let net = RaftPeerFactory::new(
                GroupId::Meta,
                cfg.cluster_id,
                addrs.clone(),
                control.clone(),
                bulk.clone(),
            )
            .with_identity_gate(identity_gate.clone());
            Some(Arc::new(
                MetaNode::start_with_network(cfg.node_id, storage.clone(), net, tuning).await?,
            ))
        } else {
            None
        };
        // Shared, mutable meta handle: a node promoted to a meta voter by a later
        // rebalance publishes its `MetaNode` here, and a drained one clears it.
        let meta_handle: MetaHandle = Arc::new(RwLock::new(meta.clone()));

        let mut map: HashMap<u16, Arc<PartitionNode>> = HashMap::new();
        for (p, _) in &hosted {
            let group = GroupId::Data(*p);
            let net = RaftPeerFactory::new(
                group,
                cfg.cluster_id,
                addrs.clone(),
                control.clone(),
                bulk.clone(),
            )
            .with_identity_gate(identity_gate.clone());
            let node =
                PartitionNode::start_with_network(cfg.node_id, group, storage.clone(), net, tuning)
                    .await?;
            map.insert(*p, Arc::new(node));
        }
        let partitions: PartitionMap = Arc::new(RwLock::new(map));

        let routing: Arc<dyn RoutingSource> = Arc::new(LiveRouting::from_descriptor(
            &desc,
            meta_handle.clone(),
            control.clone(),
            addrs.clone(),
            cfg.control_addr.clone(),
        ));
        let gateway = Arc::new(ClientGateway::new(
            cfg.cluster_id,
            cluster.p,
            cluster.hash_spec,
            partitions.clone(),
            routing.clone(),
        ));
        let search = Arc::new(crate::search::SearchService::new(storage.clone())?);
        gateway.set_search_service(search.clone());
        let catalog: Arc<dyn SearchCatalogSource> = Arc::new(LiveSearchCatalog {
            cluster_id: cfg.cluster_id,
            meta: meta_handle.clone(),
            control: control.clone(),
            addrs: addrs.clone(),
            local_control: cfg.control_addr.clone(),
        });
        gateway.set_search_catalog(catalog.clone());
        gateway.set_shard_source(Arc::new(crate::runtime::search::LiveShardSource {
            cluster_id: cfg.cluster_id,
            node_id: cfg.node_id,
            partitions: partitions.clone(),
            search: search.clone(),
            routing,
            control: control.clone(),
            addrs: addrs.clone(),
        }));
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
            meta: meta_handle.clone(),
            search: search.clone(),
            verification_timeout: cfg.timeouts.request.mul_f64(0.8),
            lifecycle: AsyncMutex::new(()),
            identity_gate: identity_gate.clone(),
        });
        let meta_starter = Arc::new(MetaStarter {
            node_id: cfg.node_id,
            cluster_id: cfg.cluster_id,
            storage: storage.clone(),
            addrs: addrs.clone(),
            control: control.clone(),
            bulk: bulk.clone(),
            tuning,
            meta: meta_handle.clone(),
            verification_timeout: cfg.timeouts.request.mul_f64(0.8),
            lifecycle: AsyncMutex::new(()),
            identity_gate: identity_gate.clone(),
        });

        // Startup reconciliation (DESIGN §5.2): the genesis voters above come
        // from the descriptor, but a partition gained through a later rebalance
        // is not in it. Resume every partition still held on disk that has not
        // been durably reclaimed — its admission record authorizes the restart,
        // and a `NonServing` group stays gone. Already-hosted genesis partitions
        // are skipped by `resume_partition`. Steady-state divergence from the
        // meta record (a group this node no longer votes for) is corrected by the
        // rebalance driver's reclaim pass.
        for p in 0..cluster.p {
            let group = GroupId::Data(p);
            if storage.serving_state(group)? == Some(ServingState::NonServing) {
                continue;
            }
            if storage.group_exists(group) {
                starter.resume_partition(p).await?;
            }
        }
        // The meta group's startup reconciliation: a node promoted to a meta
        // voter by an earlier rebalance holds the meta group on disk but is not a
        // genesis meta voter (so the block above did not start it). Resume it
        // unless it was durably reclaimed.
        if !genesis_meta_voter
            && storage.serving_state(GroupId::Meta)? != Some(ServingState::NonServing)
            && storage.group_exists(GroupId::Meta)
        {
            meta_starter.resume_meta().await?;
        }

        let dispatch = Arc::new(
            RootDispatch::new(
                cfg.cluster_id,
                gateway,
                meta_handle.clone(),
                partitions.clone(),
                heartbeats.clone(),
                Some(starter.clone()),
                Some(meta_starter.clone()),
            )
            .with_identity_gate(identity_gate.clone()),
        );

        let control_srv = ZmqServer::bind(
            ctx.clone(),
            &cfg.control_addr,
            dispatch.clone(),
            Lane::Control,
        )?;
        let bulk_srv = ZmqServer::bind(ctx.clone(), &cfg.bulk_addr, dispatch.clone(), Lane::Bulk)?;

        // Control loops: every node beats to every node it knows, so a node
        // promoted to meta voter later already holds liveness evidence; the
        // meta leader turns that evidence into directory transitions. The cost
        // is O(N) frames per node per interval — revisit if clusters outgrow it.
        let hb_interval = (cfg.timeouts.suspect / 3).max(Duration::from_millis(50));
        let directory_timeout = cfg.timeouts.request;
        let hb_control = ZmqTransport::new(ctx.clone(), hb_interval, Lane::Control);
        let mut tasks = Vec::new();
        tasks.push(tokio::spawn(heartbeat_emitter(
            registration.clone(),
            heartbeat_incarnation,
            hb_control,
            addrs.clone(),
            hb_interval,
            identity_gate.clone(),
        )));
        // Spawn on every node. The shared handle becomes populated if this node
        // is promoted to a meta voter later, at which point it can immediately
        // participate in failure detection and assume the leader role.
        tasks.push(tokio::spawn(failure_detector(
            meta_handle.clone(),
            heartbeats.clone(),
            cfg.timeouts,
            hb_interval,
            identity_gate.clone(),
        )));
        // The rebalance driver runs on every node: its data-leader role must run
        // wherever a partition is led, including on non-meta-voter nodes (which
        // read the plan and report observations over the network).
        let driver = RebalanceDriver::new(
            cfg.node_id,
            cfg.cluster_id,
            cluster.p,
            cluster.r as usize,
            desc.data_placements.iter().cloned().collect(),
            meta_handle.clone(),
            partitions.clone(),
            control.clone(),
            addrs.clone(),
            starter,
            meta_starter,
            directory_timeout,
            registration,
            identity_gate.clone(),
            readiness.clone(),
        )
        .with_search_catalog(catalog.clone());
        tasks.push(tokio::spawn(driver.run()));
        // Search-index lifecycle: build the generations this node's partitions
        // owe the catalog, report readiness, and (as meta leader) activate.
        let (search_shutdown, search_shutdown_rx) = watch::channel(false);
        let search_task = tokio::spawn(
            crate::runtime::search::SearchDriver::new(
                cfg.node_id,
                cfg.cluster_id,
                meta_handle.clone(),
                partitions.clone(),
                search.clone(),
                catalog,
                control.clone(),
                addrs.clone(),
                identity_gate.clone(),
                readiness.clone(),
                search_shutdown_rx,
            )
            .run(),
        );
        tasks.push(tokio::spawn(recovery_fence_driver(
            cfg.node_id,
            cfg.cluster_id,
            partitions.clone(),
            control.clone(),
            addrs.clone(),
            directory_timeout,
            identity_gate.clone(),
        )));

        // Read-only HTTP admin plane. Its listener was bound before runtime
        // assembly; the serving task is aborted on shutdown with the other
        // loops.
        if let Some(listener) = http_listener {
            let src: Arc<dyn StatusSource> = Arc::new(NodeStatus {
                node_id: cfg.node_id,
                cluster: cluster.clone(),
                storage: storage.clone(),
                meta: meta_handle.clone(),
                partitions: partitions.clone(),
                search: search.clone(),
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
            meta: meta_handle,
            partitions,
            desc,
            control,
            addrs,
            readiness,
            identity_gate,
            search,
            tasks,
            search_shutdown,
            search_task: Some(search_task),
            _control_srv: Some(control_srv),
            _bulk_srv: Some(bulk_srv),
        })
    }

    pub fn search_service(&self) -> Arc<crate::search::SearchService> {
        self.search.clone()
    }

    /// Drive the resumable genesis: the designated meta voter initializes and
    /// seeds the cluster; the designated voter of each hosted data group
    /// initializes it. Idempotent — a resumed node whose groups are already
    /// initialized does nothing.
    pub async fn bootstrap(&self) -> Result<()> {
        self.require_identity_open()?;
        if let Some(meta) = self.meta() {
            let meta = &*meta;
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
                self.require_identity_open()?;
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
            // A restarted group has already established its membership through
            // its durable Raft state. Its genesis placement may legitimately
            // have changed, so only an uninitialized group waits for the exact
            // descriptor placement before first initialization.
            if node.is_initialized().await? {
                continue;
            }
            let deadline = Instant::now() + BOOTSTRAP_TIMEOUT;
            while !self.placement_seeded(*p, voters).await? {
                self.require_identity_open()?;
                if Instant::now() >= deadline {
                    return Err(crate::error::Error::Raft(format!(
                        "node {} timed out waiting for placement {p} to seed",
                        self.node_id
                    )));
                }
                tokio::time::sleep(BOOTSTRAP_POLL).await;
            }
            node.initialize(voters).await?;
        }
        self.readiness.mark_ready();
        Ok(())
    }

    fn require_identity_open(&self) -> Result<()> {
        if self.identity_gate.is_open() {
            Ok(())
        } else {
            Err(Error::Raft("local process identity is fenced".into()))
        }
    }

    fn meta_seeded_locally(&self, meta: &MetaNode) -> Result<bool> {
        self.desc
            .data_placements
            .iter()
            .try_fold(true, |ready, (p, _voters)| {
                if !ready {
                    return Ok(false);
                }
                // Genesis is seeded once every descriptor partition has a
                // placement. On restart that placement may legitimately have
                // moved or carry an in-flight recovery plan, neither of which
                // should make bootstrap replay the immutable descriptor.
                Ok(meta.local_placement(GroupId::Data(*p))?.is_some())
            })
    }

    /// Snapshot the meta handle. Callers clone out the `Arc<MetaNode>` and drop
    /// the guard before awaiting; the lock is never held across an `.await`.
    fn meta(&self) -> Option<Arc<MetaNode>> {
        self.meta.read().unwrap().clone()
    }

    async fn placement_seeded(&self, partition: u16, voters: &[NodeId]) -> Result<bool> {
        if let Some(meta) = self.meta() {
            let placement = meta.local_placement(GroupId::Data(partition))?;
            if placement
                .as_ref()
                .is_some_and(|placement| placement.r#move.is_some())
            {
                return Err(Error::Raft(format!(
                    "data group {partition} has a move before genesis initialization"
                )));
            }
            return Ok(placement.is_some_and(|placement| placement.voters == voters));
        }

        let body = BootstrapStatusBody {
            group: GroupId::Data(partition),
            voters: voters.to_vec(),
        };
        for addr in self.addrs.control_addrs() {
            let request = Envelope::new(
                self.desc.cluster_id,
                MsgType::BootstrapStatus,
                GroupId::Meta,
                0,
                crate::codec::encode(&body),
            );
            let Ok(reply) = self.control.call(&addr, request).await else {
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

            // A move for an uninitialized genesis group cannot be reconciled:
            // there is no data Raft configuration for the plan driver to
            // change. Surface that invariant violation immediately instead of
            // disguising it as an eventual bootstrap timeout.
            let query = Envelope::new(
                self.desc.cluster_id,
                MsgType::PlacementQuery,
                GroupId::Meta,
                0,
                crate::codec::encode(&PlacementQueryBody {
                    group: GroupId::Data(partition),
                }),
            );
            if let Ok(reply) = self.control.call(&addr, query).await
                && let Ok(PlacementQueryReply {
                    placement: Some(placement),
                }) = crate::codec::decode(&reply.payload)
                && placement.r#move.is_some()
            {
                return Err(Error::Raft(format!(
                    "data group {partition} has a move before genesis initialization"
                )));
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
        match self.meta() {
            Some(meta) => Ok(meta.local_node(node_id)?.map(|e| e.state)),
            None => Ok(None),
        }
    }

    /// The committed voter set of a partition's meta placement, and whether a
    /// move is still in flight. Used by tests to observe rebalance completion.
    pub fn local_placement_voters(&self, partition: u16) -> Result<Option<(Vec<NodeId>, bool)>> {
        match self.meta() {
            Some(meta) => Ok(meta
                .local_placement(GroupId::Data(partition))?
                .map(|p| (p.voters, p.r#move.is_some()))),
            None => Ok(None),
        }
    }

    /// Whether this node currently hosts the meta group. Used by tests to observe
    /// meta-voter drain (a reclaimed meta group clears the handle).
    pub fn hosts_meta(&self) -> bool {
        self.meta.read().unwrap().is_some()
    }

    /// This node's view of the meta leader, if it hosts the meta group.
    pub fn meta_leader(&self) -> Option<NodeId> {
        self.meta().and_then(|m| m.current_leader())
    }

    /// This node's committed meta voter set, if it hosts the meta group. Used by
    /// tests to observe meta membership change.
    pub fn meta_voters_of(&self) -> Option<Vec<NodeId>> {
        self.meta().map(|m| m.voters())
    }

    pub fn cluster(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// Cumulative RocksDB counters used by the opt-in write-path benchmark.
    pub fn rocks_counters(&self) -> crate::perf::RocksCounters {
        // Every Raft group on this runtime shares this one node-local DB.
        self.partitions
            .read()
            .unwrap()
            .values()
            .next()
            .map(|node| node.rocks_counters())
            .or_else(|| self.meta().map(|node| node.rocks_counters()))
            .unwrap_or_default()
    }

    /// Gracefully stop inbound work and background loops, then shut down every
    /// Raft group and release all storage owners before returning.
    pub async fn shutdown(mut self) -> Result<()> {
        // Unlike the other loops, search reconciliation can own non-abortable
        // blocking work. Ask it to stop and let the current pass drain before
        // releasing storage.
        let _ = self.search_shutdown.send(true);
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
        if let Some(task) = self.search_task.take() {
            let _ = task.await;
        }

        // First fence new inbound work, but do not wait for handlers yet: an
        // already-dispatched client write may be blocked inside Raft and needs
        // the Raft shutdown below to wake it. Waiting first creates a shutdown
        // dependency cycle.
        if let Some(server) = self._control_srv.as_mut() {
            server.stop_accepting();
        }
        if let Some(server) = self._bulk_srv.as_mut() {
            server.stop_accepting();
        }

        let mut first_error = None;
        let meta = self.meta();
        if let Some(node) = &meta
            && let Err(error) = node.shutdown().await
        {
            first_error = Some(error);
        }
        let partitions: Vec<Arc<PartitionNode>> =
            self.partitions.read().unwrap().values().cloned().collect();
        for node in &partitions {
            if let Err(error) = node.shutdown().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        self.partitions.write().unwrap().clear();
        *self.meta.write().unwrap() = None;
        drop(partitions);
        drop(meta);

        // Raft waiters have now been released. Drain the handlers and drop the
        // dispatcher's starter/storage handles so an immediate restart can
        // reacquire RocksDB's LOCK deterministically.
        if let Some(server) = self._control_srv.take() {
            server.shutdown().await;
        }
        if let Some(server) = self._bulk_srv.take() {
            server.shutdown().await;
        }

        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }
}

/// Reopen async follower-local reads without waiting for a user mutation. Each
/// hosted partition independently obtains a current-leader quorum fence and
/// applies it only if the local term, membership, and recovery epoch still
/// match after catch-up.
async fn recovery_fence_driver(
    node_id: NodeId,
    cluster_id: ClusterId,
    partitions: PartitionMap,
    control: ZmqTransport,
    addrs: AddrBook,
    request_timeout: Duration,
    identity_gate: ProcessIdentityGate,
) {
    let poll = Duration::from_millis(100);
    loop {
        if !identity_gate.is_open() {
            return;
        }
        let nodes: Vec<Arc<PartitionNode>> = partitions.read().unwrap().values().cloned().collect();
        let mut recoveries = FuturesUnordered::new();
        for node in nodes {
            let materialized = node.materialized_state_status();
            if materialized.recovery_ready || materialized.failed.is_some() {
                continue;
            }
            let Some(leader) = node.current_leader() else {
                continue;
            };
            let control = control.clone();
            let addrs = addrs.clone();
            recoveries.push(async move {
                let reply = if leader == node_id {
                    node.issue_recovery_fence(node_id).await
                } else {
                    let Some(addr) = addrs.control(leader) else {
                        return;
                    };
                    let request = Envelope::new(
                        cluster_id,
                        MsgType::DataRecoveryFence,
                        node.group(),
                        0,
                        crate::codec::encode(&RecoveryFenceRequest { requester: node_id }),
                    );
                    let Ok(envelope) = control.call(&addr, request).await else {
                        return;
                    };
                    if envelope.cluster_id != cluster_id
                        || envelope.msg_type != MsgType::DataRecoveryFence
                        || envelope.group_id != node.group()
                    {
                        return;
                    }
                    let Ok(reply) = crate::codec::decode::<RecoveryFenceReply>(&envelope.payload)
                    else {
                        return;
                    };
                    reply
                };
                if let RecoveryFenceReply::Fence(fence) = reply {
                    let _ =
                        tokio::time::timeout(request_timeout, node.apply_recovery_fence(&fence))
                            .await;
                }
            });
        }
        while recoveries.next().await.is_some() {}
        tokio::time::sleep(poll).await;
    }
}

/// Builds the read-only `/status` snapshot from node-local state. Holds the same
/// shared handles as the node, so the HTTP task sees live partition membership.
struct NodeStatus {
    node_id: NodeId,
    cluster: ClusterConfig,
    storage: Arc<Storage>,
    meta: MetaHandle,
    partitions: PartitionMap,
    search: Arc<crate::search::SearchService>,
}

impl StatusSource for NodeStatus {
    fn status(&self) -> ClusterStatus {
        let meta_node = self.meta.read().unwrap().clone();
        let meta = meta_node.as_ref().map(|m| MetaStatus {
            is_leader: m.current_leader() == Some(self.node_id),
            leader: m.current_leader(),
            applied: m.applied_index(),
            voters: m.voters(),
        });
        let directory = meta_node
            .as_ref()
            .and_then(|m| m.local_directory().ok())
            .unwrap_or_default();

        let mut partitions: Vec<PartitionStatus> = {
            let map = self.partitions.read().unwrap();
            map.iter()
                .map(|(&partition, node)| {
                    let committed = node.committed_voter_set();
                    let leader = node.current_leader();
                    let materialized = node.materialized_state_status();
                    let last_log_index = node.raft().metrics().borrow().last_log_index;
                    let retained_log_entries = match (
                        last_log_index,
                        materialized.durable.map(|log_id| log_id.index),
                    ) {
                        (Some(last), Some(durable)) => last.saturating_sub(durable),
                        (Some(last), None) => last.saturating_add(1),
                        (None, _) => 0,
                    };
                    let role = if leader == Some(self.node_id) {
                        Role::Leader
                    } else if committed.contains(&self.node_id) {
                        Role::Voter
                    } else {
                        Role::Learner
                    };
                    let plan = meta_node
                        .as_ref()
                        .and_then(|m| m.local_placement(GroupId::Data(partition)).ok().flatten())
                        .and_then(|p| p.r#move)
                        .map(|mv| PlanStatus {
                            plan_id: mv.plan_id,
                            aborting: mv.aborting,
                        });
                    let (search, search_outbox_entries, search_outbox_bytes) = self
                        .search
                        .partition_status(partition, node.applied_index().unwrap_or(0));
                    PartitionStatus {
                        partition,
                        role,
                        leader,
                        applied: node.applied_index(),
                        materialized_visible: materialized.visible.map(|log_id| log_id.index),
                        materialized_durable: materialized.durable.map(|log_id| log_id.index),
                        materialized_pending_entries: materialized.pending_entries,
                        materialized_pending_bytes: materialized.pending_bytes,
                        materialized_oldest_pending_ms: materialized.oldest_pending_ms,
                        materialized_last_flush_latency_ms: materialized.last_flush_latency_ms,
                        materialized_max_flush_latency_ms: materialized.max_flush_latency_ms,
                        materialized_flush_failures: materialized.flush_failures,
                        materialized_recovery_ready: materialized.recovery_ready,
                        materialized_recovery_epoch: materialized.recovery_epoch,
                        materialized_recovery_target: materialized
                            .recovery_target
                            .map(|log_id| log_id.index),
                        materialized_recovery_attempts: materialized.recovery_attempts,
                        materialized_recovery_duration_ms: materialized.recovery_duration_ms,
                        materialized_replayed_entries: materialized.replayed_entries,
                        materialized_purge_waits: materialized.purge_waits,
                        materialized_purge_wait_ms: materialized.purge_wait_ms,
                        materialized_snapshot_waits: materialized.snapshot_waits,
                        materialized_snapshot_wait_ms: materialized.snapshot_wait_ms,
                        materialized_retained_log_entries: retained_log_entries,
                        materialized_failed: materialized.failed.is_some(),
                        committed_voters: committed.into_iter().collect(),
                        serving: node.is_serving(),
                        plan,
                        search,
                        search_outbox_entries,
                        search_outbox_bytes,
                    }
                })
                .collect()
        };
        partitions.sort_by_key(|s| s.partition);

        ClusterStatus {
            node_id: self.node_id,
            cluster_id: cluster_id_hex(self.cluster.cluster_id),
            protocol_version: self.cluster.protocol_version,
            storage_failed: self.storage.database_failure().is_some(),
            meta,
            partitions,
            directory,
        }
    }
}

/// Read the authoritative placement before accepting a learner admission. A
/// local meta leader performs ReadIndex directly; every other node asks the meta
/// voters through `PlacementQuery`, whose handler also performs ReadIndex.
async fn live_placement(
    cluster_id: ClusterId,
    group: GroupId,
    meta: &MetaHandle,
    control: &ZmqTransport,
    addrs: &AddrBook,
    verification_timeout: Duration,
) -> Result<Placement> {
    let local = { meta.read().unwrap().clone() };
    if let Some(local) = local {
        match local.read_placement(group).await {
            Ok(MetaRead::Value(Some(placement))) => return Ok(placement),
            Ok(MetaRead::Value(None)) | Ok(MetaRead::NotLeader { .. }) | Err(_) => {}
        }
    }

    let payload = crate::codec::encode(&PlacementQueryBody { group });
    let query = async {
        let mut attempts: FuturesUnordered<_> = addrs
            .control_addrs()
            .into_iter()
            .map(|addr| {
                let request = Envelope::new(
                    cluster_id,
                    MsgType::PlacementQuery,
                    GroupId::Meta,
                    0,
                    payload.clone(),
                );
                async move { control.call(&addr, request).await }
            })
            .collect();
        while let Some(reply) = attempts.next().await {
            let Ok(reply) = reply else {
                continue;
            };
            if reply.cluster_id != cluster_id || reply.msg_type != MsgType::PlacementQuery {
                continue;
            }
            if let Ok(PlacementQueryReply {
                placement: Some(placement),
            }) = crate::codec::decode::<PlacementQueryReply>(&reply.payload)
            {
                return Some(placement);
            }
        }
        None
    };
    if let Ok(Some(placement)) = tokio::time::timeout(verification_timeout, query).await {
        return Ok(placement);
    }

    Err(crate::error::Error::Raft(format!(
        "no meta leader confirmed placement for {group:?}"
    )))
}

struct LearnerPlanContext<'a> {
    node_id: NodeId,
    cluster_id: ClusterId,
    meta: &'a MetaHandle,
    control: &'a ZmqTransport,
    addrs: &'a AddrBook,
    verification_timeout: Duration,
}

async fn verify_learner_plan(
    group: GroupId,
    plan_id: u64,
    context: LearnerPlanContext<'_>,
) -> Result<()> {
    let placement = live_placement(
        context.cluster_id,
        group,
        context.meta,
        context.control,
        context.addrs,
        context.verification_timeout,
    )
    .await?;
    let Some(plan) = placement.r#move else {
        return Err(crate::error::Error::Raft(format!(
            "learner admission for {group:?} has no live plan"
        )));
    };
    if plan.plan_id != plan_id {
        return Err(crate::error::Error::Raft(format!(
            "learner admission plan mismatch for {group:?}: expected {}, got {plan_id}",
            plan.plan_id
        )));
    }
    if plan.aborting {
        return Err(crate::error::Error::Raft(format!(
            "learner admission plan {plan_id} for {group:?} is aborting"
        )));
    }
    let current = crate::types::voter_set(placement.voters);
    let target = crate::types::voter_set(plan.target_voters);
    let added: Vec<NodeId> = target.difference(&current).copied().collect();
    if added != [context.node_id] {
        return Err(crate::error::Error::Raft(format!(
            "learner admission plan {plan_id} for {group:?} does not add node {}",
            context.node_id
        )));
    }
    Ok(())
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
    meta: MetaHandle,
    search: Arc<crate::search::SearchService>,
    verification_timeout: Duration,
    lifecycle: AsyncMutex<()>,
    identity_gate: ProcessIdentityGate,
}

impl PartitionStarter {
    /// Admit this node as a learner for `group` under `plan_id`: write the
    /// durable admission record (the second lawful way group state is created —
    /// ground rule 8), start the partition's Raft runtime as an uninitialized
    /// learner, and publish it so the gateway and dispatcher can serve/route it.
    /// Idempotent: a group already hosted returns `Ok` without restarting.
    pub async fn admit_learner(&self, group: GroupId, plan_id: u64) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let GroupId::Data(partition) = group else {
            return Err(crate::error::Error::Raft(format!(
                "cannot admit a learner for {group:?}"
            )));
        };
        verify_learner_plan(
            group,
            plan_id,
            LearnerPlanContext {
                node_id: self.node_id,
                cluster_id: self.cluster_id,
                meta: &self.meta,
                control: &self.control,
                addrs: &self.addrs,
                verification_timeout: self.verification_timeout,
            },
        )
        .await?;

        self.storage
            .record_verified_learner_admission(&LearnerAdmission {
                cluster_id: self.cluster_id,
                group,
                plan_id,
            })?;
        if self.partitions.read().unwrap().contains_key(&partition) {
            return Ok(());
        }
        self.start_and_publish(partition).await
    }

    /// Resume hosting a partition this node holds durable state for but is not
    /// currently running — one gained through an earlier rebalance and left on
    /// disk after a restart (startup reconciliation, DESIGN §5.2). Unlike
    /// [`PartitionStarter::admit_learner`] it writes no admission record: it
    /// relies on the existing bootstrap or admission record, and
    /// `authorize_group_start` refuses a group that has neither (the amnesia
    /// rule) or that was durably reclaimed. Idempotent.
    pub async fn resume_partition(&self, partition: u16) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        self.start_and_publish(partition).await
    }

    /// Start `partition`'s Raft runtime over the ZMQ network and publish it into
    /// the shared map so the gateway and dispatcher can serve/route it. The
    /// caller has already established the durable record that
    /// `authorize_group_start` (inside `start_with_network`) requires. Idempotent
    /// — a group already hosted returns `Ok` without restarting.
    async fn start_and_publish(&self, partition: u16) -> Result<()> {
        if self.partitions.read().unwrap().contains_key(&partition) {
            return Ok(());
        }
        let group = GroupId::Data(partition);
        let net = RaftPeerFactory::new(
            group,
            self.cluster_id,
            self.addrs.clone(),
            self.control.clone(),
            self.bulk.clone(),
        )
        .with_identity_gate(self.identity_gate.clone());
        let node = PartitionNode::start_with_network(
            self.node_id,
            group,
            self.storage.clone(),
            net,
            self.tuning,
        )
        .await?;
        self.search.allow_partition(partition).await;
        self.partitions
            .write()
            .unwrap()
            .insert(partition, Arc::new(node));
        Ok(())
    }

    /// Stop hosting `partition` and reclaim its local data after a drain removed
    /// this node from the group (DESIGN §7.3, §7.4) — the inverse of
    /// [`PartitionStarter::admit_learner`]. Unpublish it so the gateway and
    /// dispatcher stop routing to it, shut down its Raft runtime, then durably
    /// record `NonServing` and drop the CFs. Ordering matters: the runtime is
    /// stopped before the CFs are dropped, and `reclaim_group` records
    /// `NonServing` before the drop so a crash mid-reclaim cannot leave
    /// servable-looking state. Idempotent — a partition not hosted returns `Ok`.
    pub async fn reclaim_partition(&self, partition: u16) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        if !self.partitions.read().unwrap().contains_key(&partition) {
            return Ok(());
        }

        // The driver's placement read is only a prefilter. Revalidate while
        // holding the admission/reclaim lifecycle lock so a stale reclaim
        // decision cannot delete a replica that a newer plan has retained or
        // is currently admitting. If a new plan appears after this check, its
        // admission waits for reclaim and then starts from the durable marker.
        let group = GroupId::Data(partition);
        let placement = live_placement(
            self.cluster_id,
            group,
            &self.meta,
            &self.control,
            &self.addrs,
            self.verification_timeout,
        )
        .await?;
        if placement.r#move.is_some() || placement.voters.contains(&self.node_id) {
            return Ok(());
        }

        let node = self.partitions.write().unwrap().remove(&partition);
        let Some(node) = node else {
            return Ok(());
        };
        if let Err(error) = node.shutdown().await {
            // Keep the stopped-or-partially-stopped runtime reachable so the
            // reconciliation loop can retry shutdown on its next tick. The
            // lifecycle lock prevents learner admission from publishing a
            // replacement between removal and restoration.
            self.partitions.write().unwrap().insert(partition, node);
            return Err(error);
        }
        // Release the open Tantivy readers/writers before `reclaim_group_durable`
        // unlinks the index directory, so nothing keeps serving — or later
        // re-adopts — a projection of the partition we no longer host.
        self.search.forget_partition(partition).await;
        self.storage.reclaim_group_durable(group).await?;
        Ok(())
    }
}

/// Starts and stops this node's meta-group runtime at rebalance time — the
/// meta-group analogue of [`PartitionStarter`] (DESIGN §7.2/§7.3 for the meta
/// group). Publishes into the shared [`MetaHandle`] so the dispatcher, driver,
/// and status source see the change.
pub struct MetaStarter {
    node_id: NodeId,
    cluster_id: ClusterId,
    storage: Arc<Storage>,
    addrs: AddrBook,
    control: ZmqTransport,
    bulk: ZmqTransport,
    tuning: RaftTuning,
    meta: MetaHandle,
    verification_timeout: Duration,
    lifecycle: AsyncMutex<()>,
    identity_gate: ProcessIdentityGate,
}

impl MetaStarter {
    /// Admit this node as a meta learner under `plan_id`: write the durable
    /// `LearnerAdmission` for the meta group (ground rule 8), start the
    /// `MetaNode` as an uninitialized learner, and publish it. Idempotent — a
    /// node already hosting the meta group returns `Ok` without restarting.
    pub async fn admit_meta_learner(&self, plan_id: u64) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        verify_learner_plan(
            GroupId::Meta,
            plan_id,
            LearnerPlanContext {
                node_id: self.node_id,
                cluster_id: self.cluster_id,
                meta: &self.meta,
                control: &self.control,
                addrs: &self.addrs,
                verification_timeout: self.verification_timeout,
            },
        )
        .await?;

        self.storage
            .record_verified_learner_admission(&LearnerAdmission {
                cluster_id: self.cluster_id,
                group: GroupId::Meta,
                plan_id,
            })?;
        if self.meta.read().unwrap().is_some() {
            return Ok(());
        }
        self.start_and_publish().await
    }

    /// Resume hosting the meta group from an existing durable record after a
    /// restart of a node promoted to a meta voter (startup reconciliation). Writes
    /// no admission record; `authorize_group_start` gates on the existing one.
    pub async fn resume_meta(&self) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        self.start_and_publish().await
    }

    /// Start the `MetaNode` runtime over the ZMQ network and publish it into the
    /// shared handle. The caller has established the durable record that
    /// `authorize_group_start` (inside `start_with_network`) requires. Idempotent.
    async fn start_and_publish(&self) -> Result<()> {
        if self.meta.read().unwrap().is_some() {
            return Ok(());
        }
        let net = RaftPeerFactory::new(
            GroupId::Meta,
            self.cluster_id,
            self.addrs.clone(),
            self.control.clone(),
            self.bulk.clone(),
        )
        .with_identity_gate(self.identity_gate.clone());
        let node =
            MetaNode::start_with_network(self.node_id, self.storage.clone(), net, self.tuning)
                .await?;
        *self.meta.write().unwrap() = Some(Arc::new(node));
        Ok(())
    }

    /// Stop hosting the meta group and reclaim its local data after a drain
    /// removed this node from the meta voter set — the inverse of
    /// [`MetaStarter::admit_meta_learner`]. Unpublish first, shut down the Raft
    /// runtime, then durably record `NonServing` before dropping the CFs.
    /// Idempotent — a node not hosting the meta group returns `Ok`.
    pub async fn reclaim_meta(&self) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        if self.meta.read().unwrap().is_none() {
            return Ok(());
        }

        // Revalidate after acquiring the same lock as meta learner admission.
        // A removed node's local meta view can be frozen, so `live_placement`
        // falls back to a quorum-serving current member before any local data
        // is destroyed.
        let placement = live_placement(
            self.cluster_id,
            GroupId::Meta,
            &self.meta,
            &self.control,
            &self.addrs,
            self.verification_timeout,
        )
        .await?;
        if placement.r#move.is_some() || placement.voters.contains(&self.node_id) {
            return Ok(());
        }

        let node = self.meta.write().unwrap().take();
        let Some(node) = node else {
            return Ok(());
        };
        if let Err(error) = node.shutdown().await {
            // As with data partitions, retain the handle after a partial
            // shutdown so the periodic reclaim role can retry it.
            *self.meta.write().unwrap() = Some(node);
            return Err(error);
        }
        self.storage.reclaim_group_durable(GroupId::Meta).await?;
        Ok(())
    }
}

/// Every node periodically sends liveness evidence to the meta voters. Sends are
/// concurrent with a short timeout so one unreachable voter cannot stall the
/// round and make this node look silent to the others (DESIGN §9.1).
async fn heartbeat_emitter(
    registration: RegistrationBinding,
    process_incarnation: u64,
    control: ZmqTransport,
    addrs: AddrBook,
    interval: Duration,
    identity_gate: ProcessIdentityGate,
) {
    let mut seq = 1u64;
    loop {
        if !identity_gate.is_open() {
            return;
        }
        let body = HeartbeatBody {
            node_id: registration.node_id,
            incarnation: registration.directory_incarnation,
            process_incarnation,
            seq,
        };
        let payload = crate::codec::encode(&body);
        let control_addrs = addrs.control_addrs();
        let sends = control_addrs.iter().map(|addr| {
            let env = Envelope::new(
                registration.cluster_id,
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
    meta_handle: MetaHandle,
    tracker: Arc<Mutex<HeartbeatTracker>>,
    timeouts: Timeouts,
    interval: Duration,
    identity_gate: ProcessIdentityGate,
) {
    loop {
        if !identity_gate.is_open() {
            return;
        }
        tokio::time::sleep(interval).await;
        let Some(meta) = ({ meta_handle.read().unwrap().clone() }) else {
            continue;
        };
        if meta.current_leader() != Some(meta.node_id()) {
            continue;
        }
        let Ok(directory) = meta.local_directory() else {
            continue;
        };
        let states: Vec<(NodeId, NodeState, u64)> = directory
            .iter()
            .map(|e| (e.node_id, e.state, e.incarnation))
            .collect();
        let transitions = {
            let mut tracker = tracker.lock().unwrap();
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

/// Resolve and durably bind the exact replicated registration this process may
/// use. Genesis descriptor entries are the sole offline exception: their first
/// registration is deterministically incarnation one. Dynamic nodes and any
/// endpoint change must be confirmed by a ReadIndex-fenced directory read.
async fn resolve_registration_binding(
    cfg: &NodeConfig,
    desc: &BootstrapDescriptor,
    storage: &Storage,
    control: &ZmqTransport,
) -> Result<RegistrationBinding> {
    let previous = storage.registration_binding()?;
    let endpoints_match = previous
        .as_ref()
        .is_some_and(|binding| binding.matches_endpoints(&cfg.control_addr, &cfg.bulk_addr));

    // Prefer a live registration on restart, allowing an operator-authorized
    // rejoin at a higher incarnation to take effect. If meta is unavailable,
    // an exact durable endpoint binding remains sufficient to restart; it will
    // be checked again by the authoritative refresh loop.
    if endpoints_match {
        let previous = previous.expect("checked as some above");
        let seeds = cfg.seeds.iter().cloned().chain(
            desc.directory
                .iter()
                .map(|entry| entry.control_addr.clone()),
        );
        let Ok(directory) =
            fetch_authoritative_directory(control, cfg.cluster_id, seeds, cfg.timeouts.request)
                .await
        else {
            return Ok(previous);
        };
        if !directory.iter().any(|entry| entry.node_id == cfg.node_id)
            && previous.directory_incarnation == 1
            && desc.directory.iter().any(|entry| {
                entry.node_id == cfg.node_id
                    && entry.control_addr == cfg.control_addr
                    && entry.bulk_addr == cfg.bulk_addr
            })
        {
            // Crash-recovery during genesis may observe ClusterInit before this
            // node's deterministic RegisterNode entry has been applied.
            return Ok(previous);
        }
        return bind_authoritative_registration(cfg, storage, directory);
    }

    // Fresh genesis data directories may start before a meta leader exists.
    // RegisterNode during bootstrap deterministically creates incarnation one.
    if previous.is_none()
        && desc.directory.iter().any(|entry| {
            entry.node_id == cfg.node_id
                && entry.control_addr == cfg.control_addr
                && entry.bulk_addr == cfg.bulk_addr
        })
    {
        let binding = RegistrationBinding {
            cluster_id: cfg.cluster_id,
            node_id: cfg.node_id,
            control_addr: cfg.control_addr.clone(),
            bulk_addr: cfg.bulk_addr.clone(),
            directory_incarnation: 1,
        };
        storage.record_registration_binding(&binding)?;
        return Ok(binding);
    }

    let seeds = cfg.seeds.iter().cloned().chain(
        desc.directory
            .iter()
            .map(|entry| entry.control_addr.clone()),
    );
    let directory =
        fetch_authoritative_directory(control, cfg.cluster_id, seeds, cfg.timeouts.request).await?;
    bind_authoritative_registration(cfg, storage, directory)
}

fn bind_authoritative_registration(
    cfg: &NodeConfig,
    storage: &Storage,
    directory: Vec<NodeDirectoryEntry>,
) -> Result<RegistrationBinding> {
    let entry = directory
        .into_iter()
        .find(|entry| entry.node_id == cfg.node_id)
        .ok_or_else(|| {
            Error::Config(format!(
                "node {} is not present in the authoritative directory; run dal join first",
                cfg.node_id
            ))
        })?;
    if entry.control_addr != cfg.control_addr || entry.bulk_addr != cfg.bulk_addr {
        return Err(Error::Config(format!(
            "node {} config endpoints do not match its authoritative registration",
            cfg.node_id
        )));
    }
    let binding = RegistrationBinding {
        cluster_id: cfg.cluster_id,
        node_id: cfg.node_id,
        control_addr: entry.control_addr,
        bulk_addr: entry.bulk_addr,
        directory_incarnation: entry.incarnation,
    };
    storage.record_registration_binding(&binding)?;
    Ok(binding)
}

fn validate_node_descriptor(cfg: &NodeConfig, desc: &BootstrapDescriptor) -> Result<()> {
    cfg.validate()?;
    validate_routing_shape(desc.config.p, desc.config.r)?;
    if cfg.cluster_id != desc.cluster_id || desc.config.cluster_id != desc.cluster_id {
        return Err(crate::error::Error::Config(
            "node config and bootstrap descriptor disagree on cluster_id".into(),
        ));
    }
    if desc.config.protocol_version != PROTOCOL_VERSION {
        return Err(crate::error::Error::Config(format!(
            "bootstrap descriptor protocol version {} is incompatible with runtime version {PROTOCOL_VERSION}",
            desc.config.protocol_version
        )));
    }
    if desc.directory.len() > crate::types::MAX_CLUSTER_NODES {
        return Err(crate::error::Error::Config(format!(
            "bootstrap descriptor has more than {} nodes",
            crate::types::MAX_CLUSTER_NODES
        )));
    }
    let init = InitConfig {
        p: desc.config.p,
        r: desc.config.r,
        bootstrap_nodes: desc.directory.iter().map(|entry| entry.node_id).collect(),
        meta_voters: desc.meta_voters.clone(),
        hash_spec: desc.config.hash_spec,
    };
    init.validate()?;

    let mut addresses = std::collections::HashSet::new();
    for entry in &desc.directory {
        for address in [&entry.control_addr, &entry.bulk_addr] {
            if address.is_empty() || address.len() > crate::types::MAX_ENDPOINT_BYTES {
                return Err(crate::error::Error::Config(format!(
                    "bootstrap descriptor endpoints must be 1..={} bytes",
                    crate::types::MAX_ENDPOINT_BYTES
                )));
            }
            if !addresses.insert(address) {
                return Err(crate::error::Error::Config(
                    "bootstrap descriptor endpoints must be globally unique".into(),
                ));
            }
        }
    }

    if desc.data_placements.len() != desc.config.p as usize {
        return Err(crate::error::Error::Config(format!(
            "bootstrap descriptor must contain exactly {} data placements",
            desc.config.p
        )));
    }
    let directory: std::collections::HashSet<NodeId> =
        desc.directory.iter().map(|entry| entry.node_id).collect();
    let mut partitions = std::collections::HashSet::new();
    for (partition, voters) in &desc.data_placements {
        if *partition >= desc.config.p || !partitions.insert(*partition) {
            return Err(crate::error::Error::Config(
                "bootstrap descriptor has an invalid or duplicate partition".into(),
            ));
        }
        let distinct: std::collections::HashSet<NodeId> = voters.iter().copied().collect();
        if voters.len() != desc.config.r as usize
            || distinct.len() != voters.len()
            || !distinct.iter().all(|node| directory.contains(node))
        {
            return Err(crate::error::Error::Config(format!(
                "partition {partition} must have exactly R distinct directory voters"
            )));
        }
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
            directory: (1..=3)
                .map(|node_id| DirEntry {
                    node_id,
                    control_addr: format!("tcp://node-{node_id}-control"),
                    bulk_addr: format!("tcp://node-{node_id}-bulk"),
                })
                .collect(),
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
    fn allows_config_for_a_dynamically_joined_node() {
        let mut cfg = config(9);
        cfg.control_addr = "tcp://node-9-control".into();
        cfg.bulk_addr = "tcp://node-9-bulk".into();
        assert!(validate_node_descriptor(&cfg, &descriptor()).is_ok());
    }

    #[test]
    fn defers_descriptor_address_change_to_live_registration_check() {
        let mut cfg = config(1);
        cfg.control_addr = "tcp://wrong-control".into();
        assert!(validate_node_descriptor(&cfg, &descriptor()).is_ok());
    }

    #[test]
    fn rejects_an_incompatible_protocol_descriptor() {
        let mut desc = descriptor();
        desc.config.protocol_version = PROTOCOL_VERSION - 1;
        let err = validate_node_descriptor(&config(1), &desc).unwrap_err();
        assert!(err.to_string().contains("protocol version"));
    }

    #[test]
    fn rejects_incomplete_or_malformed_placement_descriptors() {
        let mut desc = descriptor();
        desc.data_placements.clear();
        assert!(validate_node_descriptor(&config(1), &desc).is_err());

        let mut desc = descriptor();
        desc.data_placements[0].1 = vec![1, 1, 3];
        assert!(validate_node_descriptor(&config(1), &desc).is_err());

        let mut desc = descriptor();
        desc.directory[1].control_addr = desc.directory[0].control_addr.clone();
        assert!(validate_node_descriptor(&config(1), &desc).is_err());
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
