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
use openraft::{Config, RaftNetworkFactory};

use crate::config::RaftTuning;
use crate::error::{Error, Result};
use crate::keyspace;
use crate::meta::raft_types::{MetaTypeConfig, Node, NodeId, Raft};
use crate::meta::sm::MetaRaftStateMachine;
use crate::meta::state_machine::MetaApplyResult;
use crate::partition::log_store::RocksLogStore;
use crate::partition::network::{ChannelNetworkFactory, Faults, Registry};
use crate::search::SearchIndexRecord;
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
    storage_failure_monitor: tokio::task::JoinHandle<()>,
}

impl Drop for MetaNode {
    fn drop(&mut self) {
        self.storage_failure_monitor.abort();
    }
}

fn spawn_storage_failure_monitor(storage: Arc<Storage>, raft: Raft) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut metrics = raft.metrics();
        tokio::select! {
            _ = storage.wait_for_failure() => {
                let _ = raft.shutdown().await;
            }
            _ = async {
                loop {
                    if metrics.borrow().running_state.is_err() || metrics.changed().await.is_err() {
                        return;
                    }
                }
            } => {}
        }
    })
}

impl MetaNode {
    /// In-process variant used by tests and single-process harnesses: wires a
    /// [`ChannelNetworkFactory`] and registers the raft handle for peers.
    pub async fn start(
        node_id: NodeId,
        storage: Arc<Storage>,
        registry: Registry<MetaTypeConfig>,
        faults: Faults,
    ) -> Result<MetaNode> {
        let network = ChannelNetworkFactory::new(node_id, registry.clone(), faults);
        let node =
            Self::start_with_network(node_id, storage, network, RaftTuning::default()).await?;
        registry.register(node_id, node.raft.clone());
        Ok(node)
    }

    /// Start the meta Raft runtime over an arbitrary network (production wires
    /// the ZMQ-backed factory here).
    pub async fn start_with_network<NF>(
        node_id: NodeId,
        storage: Arc<Storage>,
        network: NF,
        tuning: RaftTuning,
    ) -> Result<MetaNode>
    where
        NF: RaftNetworkFactory<MetaTypeConfig>,
    {
        storage.authorize_group_start(GroupId::Meta, node_id)?;

        let config = Config {
            cluster_name: GroupId::Meta.token(),
            election_timeout_min: tuning.election_timeout_min,
            election_timeout_max: tuning.election_timeout_max,
            heartbeat_interval: tuning.heartbeat_interval,
            max_payload_entries: crate::config::RAFT_MAX_PAYLOAD_ENTRIES,
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| Error::Raft(format!("config: {e}")))?,
        );

        let log_store = RocksLogStore::new(storage.clone(), GroupId::Meta);
        let sm = MetaRaftStateMachine::new(storage.clone());

        let raft = Raft::new(node_id, config, network, log_store, sm)
            .await
            .map_err(|e| Error::Raft(format!("raft new: {e}")))?;
        let storage_failure_monitor = spawn_storage_failure_monitor(storage.clone(), raft.clone());

        Ok(MetaNode {
            node_id,
            raft,
            storage,
            storage_failure_monitor,
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    pub fn rocks_counters(&self) -> crate::perf::RocksCounters {
        self.storage.rocks_counters()
    }

    pub(crate) fn raft_storage_healthy(&self) -> bool {
        self.storage.require_group_healthy(GroupId::Meta).is_ok()
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

    /// Whether this group has an initial membership committed (used to make the
    /// bootstrap driver resumable — never re-initialize a live group).
    pub async fn is_initialized(&self) -> Result<bool> {
        self.raft
            .is_initialized()
            .await
            .map_err(|e| Error::Raft(format!("is_initialized: {e}")))
    }

    /// Admit `id` as a non-voting learner and block until it has caught up
    /// (DESIGN §7.2 — learner-first membership change). The caller is
    /// responsible for the durable admission record on the joining node.
    pub async fn add_learner(&self, id: NodeId) -> Result<()> {
        self.raft
            .add_learner(id, Node::default(), true)
            .await
            .map(|_| ())
            .map_err(|e| Error::Raft(format!("add_learner: {e}")))
    }

    /// Drop a node that is only a learner during abort rollback. The absent
    /// case returns without proposing an unchanged membership entry, so a
    /// retrying abort cannot grow the meta log while its report is unavailable.
    pub async fn remove_learner(&self, id: NodeId) -> Result<()> {
        let membership = self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .clone();
        if !membership.learner_ids().any(|learner| learner == id) {
            if membership.voter_ids().any(|voter| voter == id) {
                return Err(Error::Raft(format!(
                    "remove_learner: node {id} is still a voter"
                )));
            }
            return Ok(());
        }
        self.raft
            .change_membership(
                openraft::ChangeMembers::<NodeId, Node>::RemoveNodes(std::iter::once(id).collect()),
                false,
            )
            .await
            .map(|_| ())
            .map_err(|e| Error::Raft(format!("remove_learner: {e}")))
    }

    /// Replace the voter set (openraft `ReplaceAllVoters`); removed voters are
    /// dropped, not retained. Meta-specific policy (replacement or single-voter
    /// removal, floor of three) is enforced by the meta state machine plan
    /// record; this is the mechanical membership change.
    pub async fn change_voters(&self, voters: &[NodeId]) -> Result<()> {
        let set: std::collections::BTreeSet<NodeId> = voters.iter().copied().collect();
        self.raft
            .change_membership(set, false)
            .await
            .map(|_| ())
            .map_err(|e| Error::Raft(format!("change_membership: {e}")))
    }

    /// The committed voter set this node currently believes is in effect.
    pub fn voters(&self) -> Vec<NodeId> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect()
    }

    /// The committed membership: its log id and the effective voter set. During
    /// joint consensus the voter set is the union of both configs (DESIGN §5.2).
    /// Mirrors `PartitionNode::committed_config` so the reconcile/gate helpers
    /// treat the meta group exactly like a data group.
    pub fn committed_config(&self) -> (Option<crate::types::LogId>, Vec<NodeId>) {
        let metrics = self.raft.metrics();
        let sm = metrics.borrow().membership_config.clone();
        let log_id = sm
            .log_id()
            .map(|l| crate::types::LogId::new(l.leader_id.term, l.index));
        let voters = sm.membership().voter_ids().collect();
        (log_id, voters)
    }

    /// Confirm leadership with ReadIndex before observing the committed
    /// membership. This fences configuration reports from a stale former meta
    /// leader after leadership or quorum has moved elsewhere.
    pub async fn confirmed_committed_config(
        &self,
    ) -> Result<(Option<crate::types::LogId>, Vec<NodeId>)> {
        self.storage.require_serving(GroupId::Meta)?;
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|e| Error::Raft(format!("confirm committed membership: {e}")))?;
        Ok(self.committed_config())
    }

    /// The committed voter set as a comparable set (learners ignored).
    pub fn committed_voter_set(&self) -> std::collections::BTreeSet<NodeId> {
        self.committed_config().1.into_iter().collect()
    }

    /// Submit a meta command through the group's serving gate.
    pub async fn propose(&self, cmd: MetaCommand) -> Result<ProposeOutcome> {
        self.storage.require_serving(GroupId::Meta)?;
        match self.raft.client_write(cmd).await {
            Ok(resp) => Ok(ProposeOutcome::Applied(resp.data)),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                Ok(ProposeOutcome::NotLeader {
                    leader: f.leader_id,
                })
            }
            Err(e) => Err(Error::Raft(e.to_string())),
        }
    }

    /// Run a linearizable read: ReadIndex, then a local read of committed state.
    async fn linearizable<T>(&self, read: impl FnOnce() -> Result<T>) -> Result<MetaRead<T>> {
        self.storage.require_serving(GroupId::Meta)?;
        match self.raft.ensure_linearizable().await {
            Ok(_) => Ok(MetaRead::Value(read()?)),
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => {
                Ok(MetaRead::NotLeader {
                    leader: f.leader_id,
                })
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

    pub async fn read_node(&self, node_id: NodeId) -> Result<MetaRead<Option<NodeDirectoryEntry>>> {
        let storage = self.storage.clone();
        self.linearizable(move || {
            storage.get_state_record(GroupId::Meta, &keyspace::meta_node_key(node_id))
        })
        .await
    }

    pub async fn read_search_index(
        &self,
        name: String,
    ) -> Result<MetaRead<Option<SearchIndexRecord>>> {
        crate::search::validate_index_name(&name)?;
        self.linearizable(move || {
            self.storage
                .get_state_record(GroupId::Meta, &keyspace::meta_search_index_key(&name))
        })
        .await
    }

    pub async fn read_search_catalog(&self) -> Result<MetaRead<Vec<SearchIndexRecord>>> {
        let storage = self.storage.clone();
        self.linearizable(move || scan_search_catalog(&storage))
            .await
    }

    /// Read the complete directory after a ReadIndex barrier. This is the
    /// authoritative source for runtime endpoint/incarnation state; callers
    /// must not substitute the advisory `MetaQuery` follower view.
    pub async fn read_directory(&self) -> Result<MetaRead<Vec<NodeDirectoryEntry>>> {
        let storage = self.storage.clone();
        self.linearizable(move || scan_directory(&storage)).await
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

    /// Every node-directory entry from committed local state (non-linearizable),
    /// used by the failure detector to evaluate liveness across the cluster.
    pub fn local_directory(&self) -> Result<Vec<NodeDirectoryEntry>> {
        scan_directory(&self.storage)
    }

    pub fn local_search_catalog(&self) -> Result<Vec<SearchIndexRecord>> {
        scan_search_catalog(&self.storage)
    }

    pub fn applied_index(&self) -> Option<u64> {
        self.raft.metrics().borrow().last_applied.map(|l| l.index)
    }

    pub fn current_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// Ask this voter to campaign immediately. Used for graceful leadership
    /// handoff before the current meta leader is removed from membership.
    pub async fn trigger_election(&self) -> Result<()> {
        self.raft
            .trigger()
            .elect()
            .await
            .map_err(|e| Error::Raft(format!("trigger election: {e}")))
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| Error::Raft(format!("shutdown: {e}")))
    }
}

fn scan_directory(storage: &Storage) -> Result<Vec<NodeDirectoryEntry>> {
    let prefix = keyspace::meta_node_prefix();
    storage
        .scan_state(GroupId::Meta)?
        .into_iter()
        .filter(|(k, _)| k.starts_with(&prefix))
        .map(|(_, v)| crate::codec::decode(&v).map_err(crate::error::Error::codec))
        .collect()
}

fn scan_search_catalog(storage: &Storage) -> Result<Vec<SearchIndexRecord>> {
    let prefix = keyspace::meta_search_index_prefix();
    storage
        .scan_state(GroupId::Meta)?
        .into_iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .map(|(_, value)| crate::codec::decode(&value).map_err(crate::error::Error::codec))
        .collect()
}
