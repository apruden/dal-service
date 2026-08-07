//! A single partition's Raft runtime and its serving gate (DESIGN §7.4, M3).
//!
//! [`PartitionNode`] is the *only* path to the state machine: `write` goes
//! through `client_write` (durable majority commit), `read` through
//! `ensure_linearizable` (ReadIndex) then a local get. A removed or
//! partitioned-away old leader cannot pass the ReadIndex quorum check, so it
//! cannot serve stale data.

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::{Config, RaftNetworkFactory};

use crate::config::RaftTuning;
use crate::error::{Error, Result};
use crate::partition::log_store::RocksLogStore;
use crate::partition::network::{ChannelNetworkFactory, Faults, Registry};
use crate::partition::raft_types::{Node, NodeId, Raft, TypeConfig};
use crate::partition::sm::RocksStateMachine;
use crate::partition::state_machine::{ApplyObserver, ApplyResult, DataStateMachine};
use crate::perf::WriteStage;
use crate::storage::Storage;
use crate::transport::raft_wire::{RecoveryFence, RecoveryFenceReply};
use crate::types::{Consistency, DataRequest, GroupId, Version};

/// Outcome of a write submitted through the serving gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    Applied(ApplyResult),
    /// This node is not the leader; retry the hinted leader (DESIGN §8.2).
    NotLeader {
        leader: Option<NodeId>,
    },
}

/// Outcome of a read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadOutcome {
    Value(Option<(Version, Vec<u8>)>),
    NotLeader {
        leader: Option<NodeId>,
    },
    /// A stale read was refused: this replica is not currently attached to a
    /// leader, or is behind the caller's `min_version`. Retry a fresher
    /// candidate (DESIGN §8.3, §15).
    TooStale {
        leader: Option<NodeId>,
    },
}

/// ReadIndex outcome used to fence a strict derived-index read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchBarrier {
    Ready(Option<crate::partition::raft_types::LogId>),
    NotLeader { leader: Option<NodeId> },
}

pub struct PartitionNode {
    node_id: NodeId,
    group: GroupId,
    raft: Raft,
    storage: Arc<Storage>,
    data: DataStateMachine,
    storage_failure_monitor: tokio::task::JoinHandle<()>,
}

impl Drop for PartitionNode {
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

impl PartitionNode {
    /// In-process variant used by tests and single-process harnesses: wires a
    /// [`ChannelNetworkFactory`] and registers the raft handle for peers.
    pub async fn start(
        node_id: NodeId,
        group: GroupId,
        storage: Arc<Storage>,
        registry: Registry<TypeConfig>,
        faults: Faults,
    ) -> Result<PartitionNode> {
        let network = ChannelNetworkFactory::new(node_id, registry.clone(), faults);
        let node = Self::start_with_network_inner(
            node_id,
            group,
            storage,
            network,
            RaftTuning::default(),
            None,
        )
        .await?;
        registry.register(node_id, node.raft.clone());
        Ok(node)
    }

    /// In-process constructor with apply-boundary instrumentation for system
    /// verification. The observer sees a committed client entry after its
    /// atomic state batch becomes visible, even if the leader stops before the
    /// client receives the result.
    pub async fn start_with_apply_observer(
        node_id: NodeId,
        group: GroupId,
        storage: Arc<Storage>,
        registry: Registry<TypeConfig>,
        faults: Faults,
        observer: Arc<dyn ApplyObserver>,
    ) -> Result<PartitionNode> {
        let network = ChannelNetworkFactory::new(node_id, registry.clone(), faults);
        let node = Self::start_with_network_inner(
            node_id,
            group,
            storage,
            network,
            RaftTuning::default(),
            Some(observer),
        )
        .await?;
        registry.register(node_id, node.raft.clone());
        Ok(node)
    }

    /// Start this group's Raft runtime over an arbitrary network (production
    /// wires the ZMQ-backed factory here).
    pub async fn start_with_network<NF>(
        node_id: NodeId,
        group: GroupId,
        storage: Arc<Storage>,
        network: NF,
        tuning: RaftTuning,
    ) -> Result<PartitionNode>
    where
        NF: RaftNetworkFactory<TypeConfig>,
    {
        Self::start_with_network_inner(node_id, group, storage, network, tuning, None).await
    }

    async fn start_with_network_inner<NF>(
        node_id: NodeId,
        group: GroupId,
        storage: Arc<Storage>,
        network: NF,
        tuning: RaftTuning,
        apply_observer: Option<Arc<dyn ApplyObserver>>,
    ) -> Result<PartitionNode>
    where
        NF: RaftNetworkFactory<TypeConfig>,
    {
        if !matches!(group, GroupId::Data(_)) {
            return Err(Error::Config(
                "PartitionNode requires a data-group id".into(),
            ));
        }
        storage.authorize_group_start(group, node_id)?;

        let config = Config {
            cluster_name: group.token(),
            election_timeout_min: tuning.election_timeout_min,
            election_timeout_max: tuning.election_timeout_max,
            heartbeat_interval: tuning.heartbeat_interval,
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| Error::Raft(format!("config: {e}")))?,
        );

        let log_store = RocksLogStore::new(storage.clone(), group);
        let sm = match apply_observer {
            Some(observer) => {
                RocksStateMachine::new_with_observer(storage.clone(), group, observer)
            }
            None => RocksStateMachine::new(storage.clone(), group),
        };

        let raft = Raft::new(node_id, config, network, log_store, sm)
            .await
            .map_err(|e| Error::Raft(format!("raft new: {e}")))?;
        let storage_failure_monitor = spawn_storage_failure_monitor(storage.clone(), raft.clone());

        Ok(PartitionNode {
            node_id,
            group,
            raft,
            storage,
            data: DataStateMachine::new(group),
            storage_failure_monitor,
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

    pub fn rocks_counters(&self) -> crate::perf::RocksCounters {
        self.storage.rocks_counters()
    }

    /// Gate a shard search. Strict searches establish a fresh quorum barrier;
    /// eventual searches may skip that barrier, but remain leader-only and may
    /// not serve from a learner, removed replica, or unopened recovery epoch.
    pub async fn search_barrier(&self, linearizable: bool) -> Result<SearchBarrier> {
        self.storage.require_serving(self.group)?;
        if !linearizable {
            let leader = self.current_leader();
            let authorized = self.committed_voter_set().contains(&self.node_id)
                && leader == Some(self.node_id)
                && self.storage.state_recovery_ready(self.group);
            return if authorized {
                Ok(SearchBarrier::Ready(None))
            } else {
                Ok(SearchBarrier::NotLeader {
                    // Never redirect a recovery-fenced leader back to itself.
                    leader: leader.filter(|leader| *leader != self.node_id),
                })
            };
        }
        match self.raft.ensure_linearizable().await {
            Ok(log_id) => {
                self.open_state_recovery(log_id)?;
                Ok(SearchBarrier::Ready(log_id))
            }
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(forward))) => {
                Ok(SearchBarrier::NotLeader {
                    leader: forward.leader_id,
                })
            }
            Err(error) => Err(Error::Raft(format!("search ReadIndex: {error}"))),
        }
    }

    pub(crate) fn materialized_state_status(&self) -> crate::storage::ApplyDurabilitySnapshot {
        self.storage.state_durability(self.group)
    }

    pub(crate) fn raft_storage_healthy(&self) -> bool {
        self.storage.require_group_healthy(self.group).is_ok()
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
        let (log_id, voters) = self.committed_config_identity();
        let log_id = log_id.map(|l| crate::types::LogId::new(l.leader_id.term, l.index));
        (log_id, voters)
    }

    fn committed_config_identity(
        &self,
    ) -> (Option<crate::partition::raft_types::LogId>, Vec<NodeId>) {
        let metrics = self.raft.metrics();
        let sm = metrics.borrow().membership_config.clone();
        let log_id = *sm.log_id();
        let voters = sm.membership().voter_ids().collect();
        (log_id, voters)
    }

    /// Confirm leadership with ReadIndex before observing the committed
    /// membership. Configuration observations are used to resolve replicated
    /// move plans, so a deposed or partitioned former leader must not be able
    /// to report its stale local membership to the meta group.
    pub async fn confirmed_committed_config(
        &self,
    ) -> Result<(Option<crate::types::LogId>, Vec<NodeId>)> {
        self.storage.require_serving(self.group)?;
        let target = self
            .raft
            .ensure_linearizable()
            .await
            .map_err(|e| Error::Raft(format!("confirm committed membership: {e}")))?;
        self.open_state_recovery(target)?;
        Ok(self.committed_config())
    }

    fn open_state_recovery(
        &self,
        target: Option<crate::partition::raft_types::LogId>,
    ) -> Result<()> {
        if self.storage.state_recovery_ready(self.group) {
            return Ok(());
        }
        let epoch = self.storage.begin_state_recovery(self.group, target);
        self.storage
            .mark_state_recovery_ready(self.group, epoch, target.as_ref())
    }

    /// Issue a current-leader, quorum-confirmed recovery target to a restarted
    /// voter. The caller must still validate this identity after catching up.
    pub async fn issue_recovery_fence(&self, requester: NodeId) -> RecoveryFenceReply {
        if let Err(error) = self.storage.require_serving(self.group) {
            return RecoveryFenceReply::Rejected(error.to_string());
        }
        if self.current_leader() != Some(self.node_id) {
            return RecoveryFenceReply::NotLeader {
                leader: self.current_leader(),
            };
        }
        let target = match self.raft.ensure_linearizable().await {
            Ok(target) => target,
            Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(forward))) => {
                return RecoveryFenceReply::NotLeader {
                    leader: forward.leader_id,
                };
            }
            Err(error) => return RecoveryFenceReply::Rejected(error.to_string()),
        };
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow().clone();
        if metrics.current_leader != Some(self.node_id) {
            return RecoveryFenceReply::NotLeader {
                leader: metrics.current_leader,
            };
        }
        let (membership_log_id, voters) = self.committed_config_identity();
        if !voters.contains(&requester) {
            return RecoveryFenceReply::Rejected(format!(
                "node {requester} is not a committed voter of {:?}",
                self.group
            ));
        }
        RecoveryFenceReply::Fence(RecoveryFence {
            leader: self.node_id,
            leader_term: metrics.current_term,
            membership_log_id,
            voters,
            target,
        })
    }

    /// Apply a leader-issued recovery target to this process epoch. Readiness
    /// opens only after the exact target is visible and leader/membership
    /// identity still matches, preventing old in-flight fences from crossing a
    /// leadership or configuration change.
    pub async fn apply_recovery_fence(&self, fence: &RecoveryFence) -> Result<()> {
        self.storage.require_serving(self.group)?;
        if self.storage.state_recovery_ready(self.group) {
            return Ok(());
        }
        let epoch = self.storage.begin_state_recovery(self.group, fence.target);
        self.validate_recovery_fence(fence)?;
        if let Some(target) = &fence.target {
            self.storage.wait_state_visible(self.group, target).await?;
        }
        self.validate_recovery_fence(fence)?;
        self.storage
            .mark_state_recovery_ready(self.group, epoch, fence.target.as_ref())
    }

    fn validate_recovery_fence(&self, fence: &RecoveryFence) -> Result<()> {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow().clone();
        if metrics.current_leader != Some(fence.leader) || metrics.current_term != fence.leader_term
        {
            return Err(Error::Raft(format!(
                "recovery fence leader changed: expected {} term {}, observed {:?} term {}",
                fence.leader, fence.leader_term, metrics.current_leader, metrics.current_term
            )));
        }
        let (membership_log_id, voters) = self.committed_config_identity();
        if membership_log_id != fence.membership_log_id || voters != fence.voters {
            return Err(Error::Raft(
                "recovery fence membership changed while catch-up was in flight".into(),
            ));
        }
        if !voters.contains(&self.node_id) {
            return Err(Error::Raft(format!(
                "node {} is not a committed voter of {:?}",
                self.node_id, self.group
            )));
        }
        Ok(())
    }

    /// The committed voter set as a comparable set (learners ignored).
    pub fn committed_voter_set(&self) -> std::collections::BTreeSet<NodeId> {
        self.committed_config().1.into_iter().collect()
    }

    /// Submit a mutation through the serving gate.
    pub async fn write(&self, req: DataRequest) -> Result<WriteOutcome> {
        self.storage.require_serving(self.group)?;
        let _profile = crate::perf::timer(WriteStage::RaftClientWrite);
        match self.raft.client_write(req).await {
            Ok(resp) => {
                self.open_state_recovery(Some(resp.log_id))?;
                Ok(WriteOutcome::Applied(resp.data))
            }
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                Ok(WriteOutcome::NotLeader {
                    leader: f.leader_id,
                })
            }
            Err(e) => Err(Error::Raft(e.to_string())),
        }
    }

    /// Perform a read at the requested consistency. Linearizable runs ReadIndex
    /// then a local get; stale skips ReadIndex and serves local applied state,
    /// still gated by the serving gate and a coarse freshness/version check.
    pub async fn read(&self, key: &[u8], consistency: Consistency) -> Result<ReadOutcome> {
        // The serving gate is authority on both paths (DESIGN §7.4): an
        // amnesiac ex-voter or a node that applied its removal still refuses,
        // so relaxing consistency never resurrects a ghost replica.
        self.storage.require_serving(self.group)?;

        let min_version = match consistency {
            Consistency::Linearizable => {
                return match self.raft.ensure_linearizable().await {
                    Ok(target) => {
                        self.open_state_recovery(target)?;
                        Ok(ReadOutcome::Value(
                            self.data.get(self.storage.as_ref(), key)?,
                        ))
                    }
                    Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => {
                        Ok(ReadOutcome::NotLeader {
                            leader: f.leader_id,
                        })
                    }
                    Err(e) => Err(Error::Raft(e.to_string())),
                };
            }
            Consistency::Stale { min_version } => min_version,
        };

        // Only committed voters may serve follower reads. Learner admission opens
        // the durable process gate before catch-up, and a removed voter is
        // reclaimed asynchronously, so the process gate alone is not authority
        // for this weaker read path.
        if !self.committed_voter_set().contains(&self.node_id) {
            return Ok(ReadOutcome::TooStale {
                leader: self.current_leader(),
            });
        }

        // An isolated follower eventually starts an election and loses its
        // committed-leader view. An isolated leader does not, so `Some(self)` is
        // not freshness evidence: require a quorum barrier before letting the
        // leader use the stale fast path.
        let leader = self.current_leader();
        if leader.is_none() {
            return Ok(ReadOutcome::TooStale { leader: None });
        }
        if leader == Some(self.node_id) {
            match self.raft.ensure_linearizable().await {
                Ok(target) => self.open_state_recovery(target)?,
                Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => {
                    return Ok(ReadOutcome::TooStale {
                        leader: f.leader_id,
                    });
                }
                Err(_) => return Ok(ReadOutcome::TooStale { leader }),
            }
        }
        // Async state can recover behind the pre-crash visible prefix. A
        // follower must observe a post-start committed apply before serving
        // local state; a leader opens the same gate with the quorum barrier
        // above.
        if !self.storage.state_recovery_ready(self.group) {
            return Ok(ReadOutcome::TooStale { leader });
        }
        // Read-your-writes / monotonic: the replica must have caught up to the
        // version the caller last observed.
        if let Some(min) = min_version
            && self.applied_index().unwrap_or(0) < min
        {
            return Ok(ReadOutcome::TooStale { leader });
        }
        // No ReadIndex, no fsync: just this replica's locally-applied state.
        Ok(ReadOutcome::Value(
            self.data.get(self.storage.as_ref(), key)?,
        ))
    }

    /// Read this node's *local* applied value for a key, bypassing the serving
    /// gate. Not linearizable — for tests and reconciliation that inspect a
    /// specific replica's applied state.
    pub fn local_get(&self, key: &[u8]) -> Result<Option<(Version, Vec<u8>)>> {
        self.data.get(self.storage.as_ref(), key)
    }

    /// The highest log index this node has applied, if any.
    pub fn applied_index(&self) -> Option<u64> {
        self.raft.metrics().borrow().last_applied.map(|l| l.index)
    }

    /// The node this replica currently believes is leader, if any.
    pub fn current_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// Whether the local serving gate is open for this group.
    pub fn is_serving(&self) -> bool {
        self.storage.require_serving(self.group).is_ok()
    }

    /// Gracefully stop the Raft runtime.
    pub async fn shutdown(&self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| Error::Raft(format!("shutdown: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use rocksdb::{WriteBatch, WriteOptions};
    use tempfile::TempDir;

    use super::*;
    use crate::meta::bootstrap::ensure_bootstrap_group;
    use crate::partition::log_store::KEY_COMMITTED;
    use crate::partition::network::{Faults, Registry};
    use crate::storage::Identity;
    use crate::types::{
        BootstrapGroup, DataOp, DataRequest, IfVersion, KeyPresence, MutationResult,
    };

    const CLUSTER_ID: u128 = 0xDA1;
    const TEST_GROUP: GroupId = GroupId::Data(0);
    const VOTERS: [NodeId; 3] = [1, 2, 3];

    struct AsyncCluster {
        dirs: Vec<TempDir>,
        registry: Registry<TypeConfig>,
        faults: Faults,
    }

    impl AsyncCluster {
        fn new() -> Self {
            Self {
                dirs: (0..3).map(|_| tempfile::tempdir().unwrap()).collect(),
                registry: Registry::default(),
                faults: Faults::default(),
            }
        }

        async fn start(&self, idx: usize) -> (PartitionNode, Arc<Storage>) {
            let node_id = VOTERS[idx];
            // The long, fixed collection window makes the client-visible A>D
            // boundary deterministic instead of racing a production-speed
            // sub-millisecond flush.
            let storage = Arc::new(
                Storage::open_delayed_test(self.dirs[idx].path(), Duration::from_millis(250))
                    .unwrap(),
            );
            storage
                .set_identity(Identity {
                    cluster_id: CLUSTER_ID,
                    node_id,
                })
                .unwrap();
            ensure_bootstrap_group(
                &storage,
                &BootstrapGroup {
                    cluster_id: CLUSTER_ID,
                    group: TEST_GROUP,
                    members: VOTERS.to_vec(),
                },
            )
            .unwrap();
            let node = PartitionNode::start(
                node_id,
                TEST_GROUP,
                storage.clone(),
                self.registry.clone(),
                self.faults.clone(),
            )
            .await
            .unwrap();
            (node, storage)
        }
    }

    fn request(client_id: u128, sequence: u64, op: DataOp) -> DataRequest {
        DataRequest {
            client_id,
            sequence,
            op,
        }
    }

    fn put(sequence: u64, key: &[u8], value: &[u8], if_version: Option<IfVersion>) -> DataRequest {
        request(
            7,
            sequence,
            DataOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                if_version,
            },
        )
    }

    fn delete(sequence: u64, key: &[u8], if_version: Option<IfVersion>) -> DataRequest {
        request(
            7,
            sequence,
            DataOp::Delete {
                key: key.to_vec(),
                if_version,
            },
        )
    }

    async fn eventually<F: Fn() -> bool>(what: &str, f: F) {
        for _ in 0..200 {
            if f() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    fn leader_idx(nodes: &[PartitionNode]) -> Option<usize> {
        let leader = nodes.iter().find_map(PartitionNode::current_leader)?;
        nodes.iter().position(|node| node.node_id() == leader)
    }

    async fn write_to_leader(nodes: &[PartitionNode], req: &DataRequest) -> ApplyResult {
        for _ in 0..200 {
            if let Some(idx) = leader_idx(nodes) {
                match nodes[idx].write(req.clone()).await {
                    Ok(WriteOutcome::Applied(result)) => return result,
                    Ok(WriteOutcome::NotLeader { .. }) | Err(_) => {}
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no leader accepted request {req:?}");
    }

    fn active_leader_idx(nodes: &[Option<PartitionNode>]) -> Option<usize> {
        let leader = nodes
            .iter()
            .flatten()
            .find_map(PartitionNode::current_leader)?;
        nodes
            .iter()
            .position(|node| node.as_ref().is_some_and(|node| node.node_id() == leader))
    }

    async fn write_to_active_leader(
        nodes: &[Option<PartitionNode>],
        req: &DataRequest,
    ) -> ApplyResult {
        for _ in 0..240 {
            if let Some(idx) = active_leader_idx(nodes) {
                match nodes[idx].as_ref().unwrap().write(req.clone()).await {
                    Ok(WriteOutcome::Applied(result)) => return result,
                    Ok(WriteOutcome::NotLeader { .. }) | Err(_) => {}
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("no active leader accepted request {req:?}");
    }

    #[tokio::test]
    async fn database_failure_stops_raft_participation() {
        let cluster = AsyncCluster::new();
        let (node, storage) = cluster.start(0).await;
        let mut metrics = node.raft().metrics();

        storage.fail_durability_test("injected database failure");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if metrics.borrow().running_state.is_err() {
                    return;
                }
                metrics.changed().await.unwrap();
            }
        })
        .await
        .expect("Raft runtime kept participating after database failure");
        cluster.registry.remove(node.node_id());
    }

    #[tokio::test]
    async fn eventual_search_remains_leader_authorized() {
        let cluster = AsyncCluster::new();
        let mut nodes = Vec::new();
        for idx in 0..3 {
            nodes.push(cluster.start(idx).await.0);
        }
        nodes[0].initialize(&VOTERS).await.unwrap();
        eventually("leader election", || leader_idx(&nodes).is_some()).await;
        let leader = leader_idx(&nodes).unwrap();

        assert!(matches!(
            nodes[leader].search_barrier(true).await.unwrap(),
            SearchBarrier::Ready(_)
        ));
        assert_eq!(
            nodes[leader].search_barrier(false).await.unwrap(),
            SearchBarrier::Ready(None)
        );
        for (index, node) in nodes.iter().enumerate() {
            if index != leader {
                assert!(matches!(
                    node.search_barrier(false).await.unwrap(),
                    SearchBarrier::NotLeader { .. }
                ));
            }
        }
    }

    async fn fence_node(nodes: &[PartitionNode], target: usize) {
        for _ in 0..200 {
            if let Some(leader) = leader_idx(nodes)
                && let RecoveryFenceReply::Fence(fence) = nodes[leader]
                    .issue_recovery_fence(nodes[target].node_id())
                    .await
                && nodes[target].apply_recovery_fence(&fence).await.is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!(
            "recovery fence did not open node {}",
            nodes[target].node_id()
        );
    }

    async fn fence_active_node(nodes: &[Option<PartitionNode>], target: usize) {
        for _ in 0..240 {
            if let Some(leader) = active_leader_idx(nodes) {
                let leader_node = nodes[leader].as_ref().unwrap();
                let target_node = nodes[target].as_ref().unwrap();
                if let RecoveryFenceReply::Fence(fence) = leader_node
                    .issue_recovery_fence(target_node.node_id())
                    .await
                    && target_node.apply_recovery_fence(&fence).await.is_ok()
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("recovery fence did not open active node {target}");
    }

    #[derive(Clone)]
    struct DurablePrefix {
        state: Vec<(Vec<u8>, Vec<u8>)>,
        committed: Option<Vec<u8>>,
    }

    fn capture_durable_prefix(storage: &Storage) -> DurablePrefix {
        let log_cf = storage.log_cf(TEST_GROUP).unwrap();
        DurablePrefix {
            state: storage.scan_state(TEST_GROUP).unwrap(),
            committed: storage.db().get_cf(&log_cf, KEY_COMMITTED).unwrap(),
        }
    }

    /// Model the disk image after a power failure discards every state write
    /// above D. Raft log/vote records are deliberately left untouched; only the
    /// materialized state and its applied-aligned committed hint are rewound.
    fn restore_durable_prefix(path: &std::path::Path, prefix: &DurablePrefix) {
        let storage = Storage::open(path).unwrap();
        storage.ensure_group(TEST_GROUP).unwrap();
        let state_cf = storage.state_cf(TEST_GROUP).unwrap();
        let log_cf = storage.log_cf(TEST_GROUP).unwrap();
        let mut batch = WriteBatch::default();
        for item in storage
            .db()
            .iterator_cf(&state_cf, rocksdb::IteratorMode::Start)
        {
            let (key, _) = item.unwrap();
            batch.delete_cf(&state_cf, key);
        }
        for (key, value) in &prefix.state {
            batch.put_cf(&state_cf, key, value);
        }
        match &prefix.committed {
            Some(committed) => batch.put_cf(&log_cf, KEY_COMMITTED, committed),
            None => batch.delete_cf(&log_cf, KEY_COMMITTED),
        }
        let mut options = WriteOptions::default();
        options.set_sync(true);
        storage.db().write_opt(batch, &options).unwrap();
    }

    #[derive(Clone, Copy, Debug)]
    enum CrashScope {
        Follower,
        Leader,
        FollowerMajority,
        FullCluster,
    }

    impl CrashScope {
        fn suffix(self, baseline_version: u64) -> DataRequest {
            match self {
                Self::Follower => put(2, b"put-after-D", b"put-value", None),
                Self::Leader => put(
                    2,
                    b"baseline",
                    b"must-not-apply",
                    Some(IfVersion::Number(baseline_version + 1)),
                ),
                Self::FollowerMajority => {
                    delete(2, b"baseline", Some(IfVersion::Number(baseline_version)))
                }
                Self::FullCluster => put(
                    2,
                    b"baseline",
                    b"updated",
                    Some(IfVersion::Number(baseline_version)),
                ),
            }
        }

        fn targets(self, leader: usize) -> Vec<usize> {
            let followers: Vec<_> = (0..VOTERS.len()).filter(|&idx| idx != leader).collect();
            match self {
                Self::Follower => vec![followers[0]],
                Self::Leader => vec![leader],
                Self::FollowerMajority => followers,
                Self::FullCluster => (0..VOTERS.len()).collect(),
            }
        }

        fn expected_state(self, mutation: MutationResult) -> (&'static [u8], Option<Vec<u8>>) {
            match self {
                Self::Follower => (b"put-after-D", Some(b"put-value".to_vec())),
                Self::Leader => (b"baseline", Some(b"baseline-value".to_vec())),
                Self::FollowerMajority => (b"baseline", None),
                Self::FullCluster => {
                    assert!(matches!(mutation, MutationResult::Applied { .. }));
                    (b"baseline", Some(b"updated".to_vec()))
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_state_cluster_operations_are_visible_ordered_replayed_and_recovered() {
        let cluster = AsyncCluster::new();
        let mut nodes = Vec::new();
        let mut storages = Vec::new();
        for idx in 0..3 {
            let (node, storage) = cluster.start(idx).await;
            assert!(!storage.state_recovery_ready(TEST_GROUP));
            nodes.push(node);
            storages.push(storage);
        }

        nodes[0].initialize(&VOTERS).await.unwrap();
        eventually("leader election", || leader_idx(&nodes).is_some()).await;

        // A successful reply is allowed at visibility, before the state WAL
        // flush. The same process must already read that value and its sequence
        // result, while the durable watermark remains behind.
        let first = put(1, b"key", b"one", None);
        let first_result = write_to_leader(&nodes, &first).await;
        let first_version = match first_result {
            ApplyResult::Decided(MutationResult::Applied { version }) => version,
            other => panic!("unexpected initial put result: {other:?}"),
        };
        let leader = leader_idx(&nodes).unwrap();
        let state = storages[leader].state_durability(TEST_GROUP);
        assert_eq!(
            state.visible.map(|log_id| log_id.index),
            Some(first_version)
        );
        assert!(state.durable.map(|log_id| log_id.index) < Some(first_version));
        assert!(state.pending_entries > 0);
        assert_eq!(
            nodes[leader].local_get(b"key").unwrap(),
            Some((first_version, b"one".to_vec()))
        );
        assert_eq!(
            nodes[leader]
                .read(b"key", Consistency::Linearizable)
                .await
                .unwrap(),
            ReadOutcome::Value(Some((first_version, b"one".to_vec())))
        );

        // An identical retry returns the original result. Conditional failure,
        // successful CAS, conditional delete failure, and successful delete
        // then execute in the same per-client stream without observing stale
        // materialized state.
        assert_eq!(
            write_to_leader(&nodes, &first).await,
            ApplyResult::Replayed(MutationResult::Applied {
                version: first_version,
            })
        );
        let failed_put = put(
            2,
            b"key",
            b"wrong",
            Some(IfVersion::Number(first_version + 100)),
        );
        let failed_result = MutationResult::ConditionFailed {
            current: KeyPresence::Present {
                version: first_version,
            },
        };
        assert_eq!(
            write_to_leader(&nodes, &failed_put).await,
            ApplyResult::Decided(failed_result)
        );
        assert_eq!(
            write_to_leader(&nodes, &failed_put).await,
            ApplyResult::Replayed(failed_result)
        );

        let updated = write_to_leader(
            &nodes,
            &put(3, b"key", b"two", Some(IfVersion::Number(first_version))),
        )
        .await;
        let updated_version = match updated {
            ApplyResult::Decided(MutationResult::Applied { version }) => version,
            other => panic!("unexpected conditional put result: {other:?}"),
        };
        assert_eq!(
            write_to_leader(
                &nodes,
                &delete(4, b"key", Some(IfVersion::Number(first_version))),
            )
            .await,
            ApplyResult::Decided(MutationResult::ConditionFailed {
                current: KeyPresence::Present {
                    version: updated_version,
                },
            })
        );
        let deleted = write_to_leader(
            &nodes,
            &delete(5, b"key", Some(IfVersion::Number(updated_version))),
        )
        .await;
        assert!(matches!(
            deleted,
            ApplyResult::Decided(MutationResult::Applied { .. })
        ));
        let leader = leader_idx(&nodes).unwrap();
        assert_eq!(
            nodes[leader]
                .read(b"key", Consistency::Linearizable)
                .await
                .unwrap(),
            ReadOutcome::Value(None)
        );

        // Retain an acknowledged value across a follower restart. Async state
        // starts every process epoch fenced even when the recovered state is
        // fully durable; a leader-fenced target reopens stale reads without a
        // subsequent user mutation.
        let retained = write_to_leader(&nodes, &put(6, b"retained", b"value", None)).await;
        let retained_version = match retained {
            ApplyResult::Decided(MutationResult::Applied { version }) => version,
            other => panic!("unexpected retained put result: {other:?}"),
        };
        eventually("all replicas apply retained value", || {
            nodes.iter().all(|node| {
                node.local_get(b"retained").unwrap() == Some((retained_version, b"value".to_vec()))
            })
        })
        .await;
        for storage in &storages {
            let visible = storage
                .state_durability(TEST_GROUP)
                .visible
                .expect("replica has visible state");
            storage
                .wait_state_durable(TEST_GROUP, &visible)
                .await
                .unwrap();
        }

        let leader = leader_idx(&nodes).unwrap();
        let follower = (leader + 1) % nodes.len();
        let follower_id = nodes[follower].node_id();
        let stopped = nodes.remove(follower);
        stopped.shutdown().await.unwrap();
        cluster.registry.remove(follower_id);
        drop(stopped);
        drop(storages.remove(follower));
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (restarted, restarted_storage) = cluster.start(follower).await;
        assert!(!restarted_storage.state_recovery_ready(TEST_GROUP));
        assert_eq!(
            restarted.local_get(b"retained").unwrap(),
            Some((retained_version, b"value".to_vec()))
        );
        nodes.insert(follower, restarted);
        storages.insert(follower, restarted_storage);
        eventually("restarted follower observes leader", || {
            nodes[follower].current_leader().is_some()
        })
        .await;
        assert!(matches!(
            nodes[follower]
                .read(b"retained", Consistency::Stale { min_version: None })
                .await
                .unwrap(),
            ReadOutcome::TooStale { .. }
        ));

        fence_node(&nodes, follower).await;
        assert!(storages[follower].state_recovery_ready(TEST_GROUP));
        assert_eq!(
            nodes[follower]
                .read(b"retained", Consistency::Stale { min_version: None })
                .await
                .unwrap(),
            ReadOutcome::Value(Some((retained_version, b"value".to_vec())))
        );

        let after_restart =
            write_to_leader(&nodes, &put(7, b"after-restart", b"ready", None)).await;
        assert!(matches!(
            after_restart,
            ApplyResult::Decided(MutationResult::Applied { .. })
        ));
        eventually("restarted follower applies later write", || {
            matches!(
                nodes[follower].local_get(b"after-restart").unwrap(),
                Some((_, ref value)) if value == b"ready"
            )
        })
        .await;
        assert_eq!(
            nodes[follower]
                .read(b"retained", Consistency::Stale { min_version: None })
                .await
                .unwrap(),
            ReadOutcome::Value(Some((retained_version, b"value".to_vec())))
        );

        // Finally remove the current leader and verify that a new quorum still
        // serves every acknowledged operation.
        let old_leader = leader_idx(&nodes).unwrap();
        let old_leader_id = nodes[old_leader].node_id();
        nodes[old_leader].shutdown().await.unwrap();
        cluster.registry.remove(old_leader_id);
        let survivors: Vec<usize> = (0..nodes.len()).filter(|&idx| idx != old_leader).collect();
        let mut served = false;
        for _ in 0..200 {
            if let Some(new_leader) = survivors
                .iter()
                .copied()
                .find(|&idx| nodes[idx].current_leader() == Some(nodes[idx].node_id()))
                && let Ok(ReadOutcome::Value(Some((version, value)))) = nodes[new_leader]
                    .read(b"retained", Consistency::Linearizable)
                    .await
            {
                assert_eq!(version, retained_version);
                assert_eq!(value, b"value");
                served = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(served, "new leader did not preserve the acknowledged value");

        for idx in survivors {
            nodes[idx].shutdown().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovery_fence_rejects_leadership_and_membership_races() {
        // A fence from an isolated old leader cannot open a follower after a
        // higher-term replacement is elected.
        let cluster = AsyncCluster::new();
        let mut nodes = Vec::new();
        for idx in 0..3 {
            nodes.push(cluster.start(idx).await.0);
        }
        nodes[0].initialize(&VOTERS).await.unwrap();
        eventually("leader election", || leader_idx(&nodes).is_some()).await;
        let _ = write_to_leader(&nodes, &put(1, b"race", b"value", None)).await;
        let old_leader = leader_idx(&nodes).unwrap();
        let target = (0..nodes.len()).find(|&idx| idx != old_leader).unwrap();
        let old_fence = match nodes[old_leader]
            .issue_recovery_fence(nodes[target].node_id())
            .await
        {
            RecoveryFenceReply::Fence(fence) => fence,
            other => panic!("leader did not issue recovery fence: {other:?}"),
        };
        let peers: Vec<_> = VOTERS
            .iter()
            .copied()
            .filter(|id| *id != nodes[old_leader].node_id())
            .collect();
        cluster.faults.isolate(nodes[old_leader].node_id(), &peers);
        eventually("replacement leader", || {
            nodes.iter().enumerate().any(|(idx, node)| {
                idx != old_leader && node.current_leader() == Some(node.node_id())
            })
        })
        .await;
        assert!(
            nodes[target]
                .apply_recovery_fence(&old_fence)
                .await
                .is_err()
        );
        cluster.faults.heal();
        for node in nodes {
            node.shutdown().await.unwrap();
        }

        // A fence naming an old committed configuration is invalid after the
        // requester observes a newer configuration, even when it remains a
        // voter in that configuration.
        let cluster = AsyncCluster::new();
        let mut nodes = Vec::new();
        for idx in 0..3 {
            nodes.push(cluster.start(idx).await.0);
        }
        nodes[0].initialize(&VOTERS).await.unwrap();
        eventually("leader election", || leader_idx(&nodes).is_some()).await;
        let leader = leader_idx(&nodes).unwrap();
        let target = (0..nodes.len()).find(|&idx| idx != leader).unwrap();
        let removed = (0..nodes.len())
            .find(|&idx| idx != leader && idx != target)
            .unwrap();
        let fence = match nodes[leader]
            .issue_recovery_fence(nodes[target].node_id())
            .await
        {
            RecoveryFenceReply::Fence(fence) => fence,
            other => panic!("leader did not issue recovery fence: {other:?}"),
        };
        let retained: Vec<_> = VOTERS
            .iter()
            .copied()
            .filter(|id| *id != nodes[removed].node_id())
            .collect();
        nodes[leader].change_voters(&retained).await.unwrap();
        eventually("retained voter observes new membership", || {
            nodes[target].committed_voter_set()
                == retained
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
        })
        .await;
        assert!(nodes[target].apply_recovery_fence(&fence).await.is_err());
        for node in nodes {
            node.shutdown().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn async_state_converges_with_delay_reorder_and_duplicate_delivery() {
        let cluster = AsyncCluster::new();
        let mut nodes = Vec::new();
        for idx in 0..3 {
            nodes.push(cluster.start(idx).await.0);
        }
        nodes[0].initialize(&VOTERS).await.unwrap();
        eventually("leader election", || leader_idx(&nodes).is_some()).await;
        let leader = leader_idx(&nodes).unwrap();
        let followers: Vec<_> = (0..nodes.len()).filter(|&idx| idx != leader).collect();
        let leader_id = nodes[leader].node_id();
        cluster
            .faults
            .duplicate(leader_id, nodes[followers[0]].node_id());
        cluster
            .faults
            .reorder(leader_id, nodes[followers[0]].node_id());
        cluster.faults.delay(
            leader_id,
            nodes[followers[1]].node_id(),
            Duration::from_millis(5),
        );

        let mut last = None;
        for sequence in 1..=20 {
            let value = format!("value-{sequence}").into_bytes();
            let request = put(sequence, b"faulted", &value, None);
            last = Some((
                request.clone(),
                value,
                write_to_leader(&nodes, &request).await,
            ));
        }
        let (last_request, last_value, last_result) = last.unwrap();
        assert_eq!(
            write_to_leader(&nodes, &last_request).await,
            match last_result {
                ApplyResult::Decided(result) => ApplyResult::Replayed(result),
                other => panic!("unexpected final apply result: {other:?}"),
            }
        );
        cluster.faults.heal();
        eventually("faulted replicas converge", || {
            nodes.iter().all(|node| {
                node.local_get(b"faulted")
                    .unwrap()
                    .is_some_and(|(_, ref value)| value == &last_value)
            })
        })
        .await;
        for node in nodes {
            node.shutdown().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn acknowledged_async_suffix_replays_after_simulated_power_loss_matrix() {
        for scope in [
            CrashScope::Follower,
            CrashScope::Leader,
            CrashScope::FollowerMajority,
            CrashScope::FullCluster,
        ] {
            let cluster = AsyncCluster::new();
            let mut nodes = Vec::new();
            let mut storages = Vec::new();
            for idx in 0..VOTERS.len() {
                let (node, storage) = cluster.start(idx).await;
                nodes.push(Some(node));
                storages.push(Some(storage));
            }
            nodes[0]
                .as_ref()
                .unwrap()
                .initialize(&VOTERS)
                .await
                .unwrap();
            eventually("matrix leader election", || {
                active_leader_idx(&nodes).is_some()
            })
            .await;

            // Establish D and capture the exact state/hint prefix each replica
            // is allowed to recover after the simulated power loss.
            let baseline = put(1, b"baseline", b"baseline-value", None);
            let baseline_result = write_to_active_leader(&nodes, &baseline).await;
            let baseline_version = match baseline_result {
                ApplyResult::Decided(MutationResult::Applied { version }) => version,
                other => panic!("{scope:?}: unexpected baseline result: {other:?}"),
            };
            eventually("baseline reaches every replica", || {
                nodes.iter().flatten().all(|node| {
                    node.local_get(b"baseline").unwrap()
                        == Some((baseline_version, b"baseline-value".to_vec()))
                })
            })
            .await;
            for storage in storages.iter().flatten() {
                let visible = storage
                    .state_durability(TEST_GROUP)
                    .visible
                    .expect("baseline is visible");
                storage
                    .wait_state_durable(TEST_GROUP, &visible)
                    .await
                    .unwrap();
            }
            let durable_prefixes: Vec<_> = storages
                .iter()
                .map(|storage| capture_durable_prefix(storage.as_ref().unwrap()))
                .collect();

            // This operation is acknowledged from A while its state batch is
            // still pending durability. Its Raft log entry is already durable
            // on a quorum and is the sole recovery authority after rewind.
            let suffix = scope.suffix(baseline_version);
            let suffix_result = write_to_active_leader(&nodes, &suffix).await;
            let mutation = match suffix_result {
                ApplyResult::Decided(mutation) => mutation,
                other => panic!("{scope:?}: unexpected suffix result: {other:?}"),
            };
            let leader = active_leader_idx(&nodes).unwrap();
            let leader_state = storages[leader]
                .as_ref()
                .unwrap()
                .state_durability(TEST_GROUP);
            assert!(
                leader_state.visible > leader_state.durable,
                "{scope:?}: response did not expose the intended A>D cut"
            );
            let suffix_index = leader_state.visible.unwrap().index;
            eventually("suffix applies on every replica", || {
                nodes.iter().flatten().all(|node| {
                    node.applied_index()
                        .is_some_and(|index| index >= suffix_index)
                })
            })
            .await;

            let targets = scope.targets(leader);
            for &idx in &targets {
                let node = nodes[idx].take().unwrap();
                node.shutdown().await.unwrap();
                cluster.registry.remove(node.node_id());
                drop(node);
                drop(storages[idx].take());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            for &idx in &targets {
                restore_durable_prefix(cluster.dirs[idx].path(), &durable_prefixes[idx]);
            }

            // Reopen the damaged replicas. For the full-cluster case a new-term
            // blank entry re-establishes the forgotten committed suffix before
            // any local/stale state may be trusted.
            for &idx in &targets {
                let (node, storage) = cluster.start(idx).await;
                assert!(!storage.state_recovery_ready(TEST_GROUP));
                nodes[idx] = Some(node);
                storages[idx] = Some(storage);
            }
            eventually("post-crash leader election", || {
                active_leader_idx(&nodes).is_some()
            })
            .await;

            let (expected_key, expected_value) = scope.expected_state(mutation);
            eventually("Raft log replays the lost materialized suffix", || {
                nodes.iter().flatten().all(|node| {
                    node.local_get(expected_key)
                        .unwrap()
                        .map(|(_, value)| value)
                        == expected_value
                })
            })
            .await;
            for &idx in &targets {
                fence_active_node(&nodes, idx).await;
            }
            eventually("replayed replicas pass their recovery fence", || {
                targets.iter().all(|&idx| {
                    storages[idx]
                        .as_ref()
                        .unwrap()
                        .state_recovery_ready(TEST_GROUP)
                })
            })
            .await;

            // Reissuing the exact client/sequence bytes must return the
            // original decision, including a failed-CAS result, without
            // creating a new version.
            assert_eq!(
                write_to_active_leader(&nodes, &suffix).await,
                ApplyResult::Replayed(mutation),
                "{scope:?}: replay result changed after state loss"
            );

            for node in nodes.into_iter().flatten() {
                node.shutdown().await.unwrap();
            }
        }
    }
}
