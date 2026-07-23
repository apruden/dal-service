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
use crate::types::{
    BootstrapGroup, ClusterConfig, GroupId, LogId, NodeDirectoryEntry, NodeId, NodeState, Placement,
};

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
                MetaNode::start_with_network(cfg.node_id, storage.clone(), net, tuning)
                    .await?,
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
            _control_srv: control_srv,
            _bulk_srv: bulk_srv,
        })
    }

    /// Drive the resumable genesis: the designated meta voter initializes and
    /// seeds the cluster; the designated voter of each hosted data group
    /// initializes it. Idempotent — a resumed node whose groups are already
    /// initialized does nothing.
    pub async fn bootstrap(&self) -> Result<()> {
        let designated_meta = self
            .meta
            .as_ref()
            .filter(|_| bootstrap::designated(&self.meta_voters) == Some(self.node_id));
        if let Some(meta) = designated_meta {
            if !meta.is_initialized().await? {
                meta.initialize(&self.meta_voters).await?;
            }
            bootstrap::seed_cluster(std::slice::from_ref(meta), &self.desc).await?;
        }

        for (p, voters) in &self.hosted {
            if bootstrap::designated(voters) != Some(self.node_id) {
                continue;
            }
            // `.cloned()` drops the read guard before any await below.
            let Some(node) = self.partitions.read().unwrap().get(p).cloned() else {
                continue;
            };
            if !node.is_initialized().await? {
                node.initialize(voters).await?;
            }
        }
        Ok(())
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
