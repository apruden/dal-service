//! The meta group's Raft runtime and its linearizable read path (DESIGN §3.1,
//! §5, M5).
//!
//! Analogous to [`crate::partition::node::PartitionNode`], but the replicated
//! command is [`MetaCommand`] and reads return cluster/directory/placement
//! records. Proposals go through `client_write` (durable majority commit); reads
//! go through `ensure_linearizable` (ReadIndex) then a local get, so a
//! partitioned-away old leader cannot serve stale routing state.

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::Config;

use crate::error::{Error, Result};
use crate::keyspace;
use crate::meta::raft_types::{MetaTypeConfig, Node, NodeId, Raft};
use crate::meta::sm::MetaRaftStateMachine;
use crate::meta::state_machine::MetaApplyResult;
use crate::partition::log_store::RocksLogStore;
use crate::partition::network::{ChannelNetworkFactory, Faults, Registry};
use crate::storage::Storage;
use crate::types::{ClusterConfig, GroupId, MetaCommand, NodeDirectoryEntry, Placement};

/// Outcome of a proposal submitted to the meta group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposeOutcome {
    Applied(MetaApplyResult),
    NotLeader { leader: Option<NodeId> },
}

/// Outcome of a linearizable meta read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaRead<T> {
    Value(T),
    NotLeader { leader: Option<NodeId> },
}

pub struct MetaNode {
    node_id: NodeId,
    raft: Raft,
    storage: Arc<Storage>,
}

impl MetaNode {
    pub async fn start(
        node_id: NodeId,
        storage: Arc<Storage>,
        registry: Registry<MetaTypeConfig>,
        faults: Faults,
    ) -> Result<MetaNode> {
        storage.ensure_group(GroupId::Meta)?;

        let config = Config {
            cluster_name: GroupId::Meta.token(),
            election_timeout_min: 200,
            election_timeout_max: 400,
            heartbeat_interval: 100,
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| Error::Raft(format!("config: {e}")))?,
        );

        let log_store = RocksLogStore::new(storage.clone(), GroupId::Meta);
        let sm = MetaRaftStateMachine::new(storage.clone());
        let network = ChannelNetworkFactory::new(node_id, registry.clone(), faults);

        let raft = Raft::new(node_id, config, network, log_store, sm)
            .await
            .map_err(|e| Error::Raft(format!("raft new: {e}")))?;

        registry.register(node_id, raft.clone());

        Ok(MetaNode {
            node_id,
            raft,
            storage,
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    /// Bootstrap the meta group with its initial voter set (DESIGN §3.1).
    pub async fn initialize(&self, voters: &[NodeId]) -> Result<()> {
        let members: BTreeMap<NodeId, Node> =
            voters.iter().map(|&id| (id, Node::default())).collect();
        self.raft
            .initialize(members)
            .await
            .map_err(|e| Error::Raft(format!("initialize: {e}")))
    }

    /// Submit a meta command through the group's serving gate.
    pub async fn propose(&self, cmd: MetaCommand) -> Result<ProposeOutcome> {
        match self.raft.client_write(cmd).await {
            Ok(resp) => Ok(ProposeOutcome::Applied(resp.data)),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                Ok(ProposeOutcome::NotLeader { leader: f.leader_id })
            }
            Err(e) => Err(Error::Raft(e.to_string())),
        }
    }

    /// Run a linearizable read: ReadIndex, then a local read of committed state.
    async fn linearizable<T>(&self, read: impl FnOnce() -> Result<T>) -> Result<MetaRead<T>> {
        match self.raft.ensure_linearizable().await {
            Ok(_) => Ok(MetaRead::Value(read()?)),
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => {
                Ok(MetaRead::NotLeader { leader: f.leader_id })
            }
            Err(e) => Err(Error::Raft(e.to_string())),
        }
    }

    pub async fn read_cluster(&self) -> Result<MetaRead<Option<ClusterConfig>>> {
        let storage = self.storage.clone();
        self.linearizable(move || {
            storage.get_state_record(GroupId::Meta, &keyspace::meta_cluster_key())
        })
        .await
    }

    pub async fn read_placement(&self, group: GroupId) -> Result<MetaRead<Option<Placement>>> {
        let storage = self.storage.clone();
        self.linearizable(move || {
            storage.get_state_record(GroupId::Meta, &keyspace::meta_placement_key(group))
        })
        .await
    }

    /// Local (non-linearizable) reads for tests and startup reconciliation.
    pub fn local_placement(&self, group: GroupId) -> Result<Option<Placement>> {
        self.storage
            .get_state_record(GroupId::Meta, &keyspace::meta_placement_key(group))
    }

    pub fn local_node(&self, node_id: NodeId) -> Result<Option<NodeDirectoryEntry>> {
        self.storage
            .get_state_record(GroupId::Meta, &keyspace::meta_node_key(node_id))
    }

    pub fn applied_index(&self) -> Option<u64> {
        self.raft.metrics().borrow().last_applied.map(|l| l.index)
    }

    pub fn current_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| Error::Raft(format!("shutdown: {e}")))
    }
}
