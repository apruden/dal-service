//! A single partition's Raft runtime and its serving gate (DESIGN §7.4, M3).
//!
//! [`PartitionHandle`] is the *only* path to the state machine: `write` goes
//! through `client_write` (durable majority commit), `read` through
//! `ensure_linearizable` (ReadIndex) then a local get. A removed or
//! partitioned-away old leader cannot pass the ReadIndex quorum check, so it
//! cannot serve stale data.

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::Config;

use crate::error::{Error, Result};
use crate::partition::log_store::RocksLogStore;
use crate::partition::network::{ChannelNetworkFactory, Faults, Registry};
use crate::partition::raft_types::{Node, NodeId, Raft, TypeConfig};
use crate::partition::sm::RocksStateMachine;
use crate::partition::state_machine::{ApplyResult, DataStateMachine};
use crate::storage::Storage;
use crate::types::{DataRequest, GroupId, Version};

/// Outcome of a write submitted through the serving gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    Applied(ApplyResult),
    /// This node is not the leader; retry the hinted leader (DESIGN §8.2).
    NotLeader { leader: Option<NodeId> },
}

/// Outcome of a linearizable read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOutcome {
    Value(Option<(Version, Vec<u8>)>),
    NotLeader { leader: Option<NodeId> },
}

pub struct PartitionNode {
    node_id: NodeId,
    group: GroupId,
    raft: Raft,
    storage: Arc<Storage>,
    data: DataStateMachine,
}

impl PartitionNode {
    /// Build the log store, state machine, and network, then start the Raft
    /// instance and register it so peers can reach it.
    pub async fn start(
        node_id: NodeId,
        group: GroupId,
        storage: Arc<Storage>,
        registry: Registry<TypeConfig>,
        faults: Faults,
    ) -> Result<PartitionNode> {
        storage.ensure_group(group)?;

        let config = Config {
            cluster_name: group.token(),
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

        let log_store = RocksLogStore::new(storage.clone(), group);
        let sm = RocksStateMachine::new(storage.clone(), group);
        let network = ChannelNetworkFactory::new(node_id, registry.clone(), faults);

        let raft = Raft::new(node_id, config, network, log_store, sm)
            .await
            .map_err(|e| Error::Raft(format!("raft new: {e}")))?;

        registry.register(node_id, raft.clone());

        Ok(PartitionNode {
            node_id,
            group,
            raft,
            storage,
            data: DataStateMachine::new(group),
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn group(&self) -> GroupId {
        self.group
    }

    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    /// Bootstrap this group with the given voter set (DESIGN §3.1). Idempotent
    /// per openraft: a second call on an initialized group errors.
    pub async fn initialize(&self, voters: &[NodeId]) -> Result<()> {
        let members: BTreeMap<NodeId, Node> =
            voters.iter().map(|&id| (id, Node::default())).collect();
        self.raft
            .initialize(members)
            .await
            .map_err(|e| Error::Raft(format!("initialize: {e}")))
    }

    /// Whether this partition has an initial membership committed. Lets the
    /// bootstrap driver skip re-initializing a group that already exists.
    pub async fn is_initialized(&self) -> Result<bool> {
        self.raft
            .is_initialized()
            .await
            .map_err(|e| Error::Raft(format!("is_initialized: {e}")))
    }

    /// Admit `id` as a learner and block until it reaches this leader's
    /// committed log point (DESIGN §7.2 step 3). Only a caught-up learner may
    /// later be promoted.
    pub async fn add_learner(&self, id: NodeId) -> Result<()> {
        self.raft
            .add_learner(id, Node::default(), true)
            .await
            .map(|_| ())
            .map_err(|e| Error::Raft(format!("add_learner: {e}")))
    }

    /// Change the voter set via joint consensus (DESIGN §7.2 step 4); removed
    /// voters are dropped, not retained.
    pub async fn change_voters(&self, voters: &[NodeId]) -> Result<()> {
        let set: std::collections::BTreeSet<NodeId> = voters.iter().copied().collect();
        self.raft
            .change_membership(set, false)
            .await
            .map(|_| ())
            .map_err(|e| Error::Raft(format!("change_membership: {e}")))
    }

    /// The committed membership: its log id and the effective voter set. During
    /// joint consensus the voter set is the union of both configs (DESIGN §5.2).
    pub fn committed_config(&self) -> (Option<crate::types::LogId>, Vec<NodeId>) {
        let metrics = self.raft.metrics();
        let sm = metrics.borrow().membership_config.clone();
        let log_id = sm
            .log_id()
            .map(|l| crate::types::LogId::new(l.leader_id.term, l.index));
        let voters = sm.membership().voter_ids().collect();
        (log_id, voters)
    }

    /// The committed voter set as a comparable set (learners ignored).
    pub fn committed_voter_set(&self) -> std::collections::BTreeSet<NodeId> {
        self.committed_config().1.into_iter().collect()
    }

    /// Submit a mutation through the serving gate.
    pub async fn write(&self, req: DataRequest) -> Result<WriteOutcome> {
        match self.raft.client_write(req).await {
            Ok(resp) => Ok(WriteOutcome::Applied(resp.data)),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                Ok(WriteOutcome::NotLeader { leader: f.leader_id })
            }
            Err(e) => Err(Error::Raft(e.to_string())),
        }
    }

    /// Perform a linearizable read: ReadIndex, then a local get.
    pub async fn read(&self, key: &[u8]) -> Result<ReadOutcome> {
        match self.raft.ensure_linearizable().await {
            Ok(_) => {
                let value = self.data.get(&self.storage, key)?;
                Ok(ReadOutcome::Value(value))
            }
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => {
                Ok(ReadOutcome::NotLeader { leader: f.leader_id })
            }
            Err(e) => Err(Error::Raft(e.to_string())),
        }
    }

    /// Read this node's *local* applied value for a key, bypassing the serving
    /// gate. Not linearizable — for tests and reconciliation that inspect a
    /// specific replica's applied state.
    pub fn local_get(&self, key: &[u8]) -> Result<Option<(Version, Vec<u8>)>> {
        self.data.get(&self.storage, key)
    }

    /// The highest log index this node has applied, if any.
    pub fn applied_index(&self) -> Option<u64> {
        self.raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
    }

    /// The node this replica currently believes is leader, if any.
    pub fn current_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// Gracefully stop the Raft runtime.
    pub async fn shutdown(&self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| Error::Raft(format!("shutdown: {e}")))
    }
}
