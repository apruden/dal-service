//! The rebalance driver loop (DESIGN §7, M6 mechanics over the network).
//!
//! Several roles run each tick on every node:
//!
//! - **meta-leader role** (only while this node leads the meta group): runs the
//!   deterministic placement planner to evacuate ineligible voters and balance
//!   replicas across `Active` nodes, and replaces ineligible meta voters.
//! - **data-leader role** (for each hosted partition this node leads): read the
//!   plan from committed meta state, gate it, ensure the target has started its
//!   learner runtime (`BecomeLearner`), drive `add_learner` +
//!   `change_voters`, then submit a quorum-fenced result to the meta group with
//!   a `DataConfigObservation` frame.
//!
//! A data leader needs no meta leadership; the only meta write it performs is
//! the observation, submitted over the network.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::codec;
use crate::meta::bootstrap;
use crate::meta::node::{MetaNode, MetaRead};
use crate::meta::rebalancer::create_plan;
use crate::meta::reconcile::{GateDecision, ReconcileAction, gate, reconcile};
use crate::partition::node::PartitionNode;
use crate::transport::Transport;
use crate::transport::codec::{Envelope, MsgType};
use crate::transport::dealer::ZmqTransport;
use crate::transport::raft_net::AddrBook;
use crate::transport::raft_wire::{
    BecomeLearnerBody, LearnerReply, ObservationBody, PlacementQueryBody, PlacementQueryReply,
    SearchIndexStatusBody, SearchIndexStatusReply, SubmitReply,
};
use crate::types::{
    ClusterId, DataConfigObservation, GroupId, MetaCommand, MovePlan, NodeId, NodeState, Placement,
    ProcessIdentityGate, RegistrationBinding, voter_set,
};

use crate::api::gateway::{PartitionMap, SearchCatalogSource};
use crate::runtime::directory::fetch_authoritative_directory;
use crate::runtime::node::{MetaHandle, MetaStarter, PartitionStarter};
use crate::runtime::readiness::RuntimeReadiness;

const REBALANCE_INTERVAL: Duration = Duration::from_millis(150);
/// How often a node re-reads the authoritative directory. Off the meta leader
/// this is a fan-out to every control address, each answered by a ReadIndex and
/// a directory scan — O(N^2) cluster-wide if it rides the rebalance tick.
/// Membership changes rarely, so refresh far less often than roles are driven;
/// this also bounds how long a superseded process can run before it is fenced.
const DIRECTORY_REFRESH_INTERVAL: Duration = Duration::from_millis(1_000);

/// Per-node rebalance driver, spawned on every node. The meta-leader role is a
/// no-op unless this node runs and leads the meta group; the data-leader role
/// runs wherever a partition is led, reading the plan locally when this node is
/// a meta voter and over the network otherwise.
pub struct RebalanceDriver {
    node_id: NodeId,
    cluster_id: ClusterId,
    partition_count: u16,
    replication_factor: usize,
    bootstrap_placements: BTreeMap<u16, Vec<NodeId>>,
    meta: MetaHandle,
    partitions: PartitionMap,
    control: ZmqTransport,
    addrs: AddrBook,
    starter: Arc<PartitionStarter>,
    meta_starter: Arc<MetaStarter>,
    directory_timeout: Duration,
    registration: RegistrationBinding,
    identity_gate: ProcessIdentityGate,
    readiness: RuntimeReadiness,
    /// Absent in tests that do not wire search; the promotion fence is then a
    /// no-op rather than a hard dependency of rebalancing.
    search_catalog: std::sync::RwLock<Option<Arc<dyn SearchCatalogSource>>>,
}

impl RebalanceDriver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        cluster_id: ClusterId,
        partition_count: u16,
        replication_factor: usize,
        bootstrap_placements: BTreeMap<u16, Vec<NodeId>>,
        meta: MetaHandle,
        partitions: PartitionMap,
        control: ZmqTransport,
        addrs: AddrBook,
        starter: Arc<PartitionStarter>,
        meta_starter: Arc<MetaStarter>,
        directory_timeout: Duration,
        registration: RegistrationBinding,
        identity_gate: ProcessIdentityGate,
        readiness: RuntimeReadiness,
    ) -> RebalanceDriver {
        RebalanceDriver {
            node_id,
            cluster_id,
            partition_count,
            replication_factor,
            bootstrap_placements,
            meta,
            partitions,
            control,
            addrs,
            starter,
            meta_starter,
            directory_timeout,
            registration,
            identity_gate,
            readiness,
            search_catalog: std::sync::RwLock::new(None),
        }
    }

    pub fn with_search_catalog(self, catalog: Arc<dyn SearchCatalogSource>) -> Self {
        *self.search_catalog.write().unwrap() = Some(catalog);
        self
    }

    /// Snapshot the handle; the guard is dropped before any `.await`.
    fn search_catalog(&self) -> Option<Arc<dyn SearchCatalogSource>> {
        self.search_catalog.read().unwrap().clone()
    }

    /// Snapshot the meta handle; the guard is dropped before any `.await`.
    fn meta(&self) -> Option<Arc<MetaNode>> {
        self.meta.read().unwrap().clone()
    }

    pub async fn run(self) {
        let mut next_directory_refresh = Instant::now();
        loop {
            if !self.identity_gate.is_open() {
                return;
            }
            if Instant::now() >= next_directory_refresh {
                self.refresh_addr_book().await;
                next_directory_refresh = Instant::now() + DIRECTORY_REFRESH_INTERVAL;
            }
            if !self.identity_gate.is_open() {
                return;
            }
            // Recovery of an already-durable plan remains available before
            // bootstrap is marked ready. Creating a new plan or reclaiming a
            // group cannot be allowed to race first initialization, though.
            self.drive_data_role().await;
            self.drive_meta_data_role().await;
            if self.readiness.is_ready() {
                self.drive_meta_role().await;
                self.drive_reclaim_role().await;
                self.drive_meta_reclaim_role().await;
            }
            tokio::time::sleep(REBALANCE_INTERVAL).await;
        }
    }

    /// Merge only a ReadIndex-fenced directory into the address book shared by
    /// every Raft network factory and control-plane loop on this process.
    /// Advisory `MetaQuery` snapshots are intentionally excluded here.
    async fn refresh_addr_book(&self) {
        if let Some(meta) = self.meta() {
            match meta.read_directory().await {
                Ok(MetaRead::Value(directory)) if !directory.is_empty() => {
                    self.merge_authoritative_directory(&directory);
                    return;
                }
                Ok(MetaRead::Value(_)) | Ok(MetaRead::NotLeader { .. }) | Err(_) => {}
            }
        }

        if let Ok(directory) = fetch_authoritative_directory(
            &self.control,
            self.cluster_id,
            self.addrs.control_addrs(),
            self.directory_timeout,
        )
        .await
        {
            self.merge_authoritative_directory(&directory);
        }
    }

    fn merge_authoritative_directory(&self, directory: &[crate::types::NodeDirectoryEntry]) {
        if let Some(entry) = directory.iter().find(|entry| entry.node_id == self.node_id)
            && (entry.incarnation != self.registration.directory_incarnation
                || !self
                    .registration
                    .matches_endpoints(&entry.control_addr, &entry.bulk_addr))
        {
            self.identity_gate.fence();
            return;
        }
        let _ = self.addrs.update_directory(directory);
    }

    // -- meta-leader role ---------------------------------------------------

    /// Meta writes that only the leader may make: create deterministic
    /// replacement/load-balancing plans, and abort a plan whose learner target
    /// has been declared `Down` (§7.5) — such a plan can never complete, so it
    /// must roll back.
    async fn drive_meta_role(&self) {
        let Some(meta) = self.meta() else {
            return; // not a meta node; nothing to plan
        };
        let meta = &meta;
        if meta.current_leader() != Some(self.node_id) {
            return;
        }
        if !self.genesis_data_ready(meta).await {
            return;
        }
        let Ok(directory) = meta.local_directory() else {
            return;
        };
        let state_of = |id: NodeId| directory.iter().find(|e| e.node_id == id).map(|e| e.state);
        let active: Vec<NodeId> = directory
            .iter()
            .filter(|e| e.state == NodeState::Active)
            .map(|e| e.node_id)
            .collect();

        // A meta leader cannot remove itself while it is the only process that
        // drives meta membership. Ask a healthy current voter to campaign; its
        // higher term makes this node step down, and that new leader can then
        // plan this node's replacement on the next tick.
        if state_of(self.node_id) != Some(NodeState::Active)
            && let Ok(Some(meta_placement)) = meta.local_placement(GroupId::Meta)
            && meta_placement.voters.contains(&self.node_id)
            && let Some(target) = meta_placement
                .voters
                .iter()
                .copied()
                .find(|id| *id != self.node_id && state_of(*id) == Some(NodeState::Active))
        {
            if let Some(addr) = self.addrs.control(target) {
                let request = Envelope::new(
                    self.cluster_id,
                    MsgType::TransferLeadership,
                    GroupId::Meta,
                    0,
                    Vec::new(),
                );
                let _ = self.control.call(&addr, request).await;
            }
            return;
        }

        let mut placements = BTreeMap::new();
        for partition in 0..self.partition_count {
            let group = GroupId::Data(partition);
            let Ok(Some(placement)) = meta.local_placement(group) else {
                // Never balance from an incomplete load snapshot.
                return;
            };
            // Healthy plan whose learner target is Down: abort it.
            if let Some(plan) = &placement.r#move
                && !plan.aborting
            {
                let current = voter_set(placement.voters.clone());
                let target_set = voter_set(plan.target_voters.clone());
                if let Some(&learner) = target_set.difference(&current).next()
                    && state_of(learner) == Some(NodeState::Down)
                {
                    let _ = meta
                        .propose(MetaCommand::MarkAborting {
                            group,
                            plan_id: plan.plan_id,
                        })
                        .await;
                }
            }
            placements.insert(partition, placement);
        }

        // Keep one data migration active cluster-wide. Besides bounding the
        // migration blast radius, this means a fresh plan is never calculated
        // against a configuration that is still settling in another group.
        if !placements
            .values()
            .any(|placement| placement.r#move.is_some())
        {
            // The pure planner handles both evacuation of every ineligible
            // voter (Down, Draining, Suspect, or unknown) and redistribution
            // onto newly joined Active nodes.
            if let Some(proposal) =
                crate::placement::propose(&directory, &placements, self.replication_factor)
            {
                let _ = create_plan(
                    std::slice::from_ref(meta),
                    proposal.group,
                    &proposal.target_voters,
                )
                .await;
            }
        }

        // Meta-voter replacement: swap any ineligible non-leader meta voter for
        // an Active non-voter, keeping the set size (and quorum). An ineligible
        // current leader was handed off above, so it becomes eligible for this
        // branch as soon as the new leader runs it.
        if let Ok(Some(placement)) = meta.local_placement(GroupId::Meta) {
            match &placement.r#move {
                None => {
                    let drained =
                        placement.voters.iter().copied().find(|v| {
                            state_of(*v) != Some(NodeState::Active) && *v != self.node_id
                        });
                    if let Some(drained) = drained
                        && let Some(&replacement) =
                            active.iter().find(|a| !placement.voters.contains(a))
                    {
                        let mut target: Vec<NodeId> = placement
                            .voters
                            .iter()
                            .copied()
                            .filter(|&v| v != drained)
                            .collect();
                        target.push(replacement);
                        target.sort_unstable();
                        let _ =
                            create_plan(std::slice::from_ref(meta), GroupId::Meta, &target).await;
                    }
                }
                Some(plan) if !plan.aborting => {
                    let current = voter_set(placement.voters.clone());
                    let target_set = voter_set(plan.target_voters.clone());
                    if let Some(&learner) = target_set.difference(&current).next()
                        && state_of(learner) == Some(NodeState::Down)
                    {
                        let _ = meta
                            .propose(MetaCommand::MarkAborting {
                                group: GroupId::Meta,
                                plan_id: plan.plan_id,
                            })
                            .await;
                    }
                }
                Some(_) => {}
            }
        }
    }

    /// Confirm that every original data group has crossed its first Raft
    /// initialization before this meta leader is allowed to create a plan.
    ///
    /// The process-local readiness gate prevents its own bootstrap from racing
    /// the planner. This second, distributed check closes the remaining race:
    /// a meta voter can finish its local bootstrap while the designated voter
    /// for another data group is still waiting to initialize. Once a placement
    /// has diverged from the descriptor (or has a durable move), it is
    /// necessarily post-genesis and no longer needs this check on restart.
    async fn genesis_data_ready(&self, meta: &MetaNode) -> bool {
        for (&partition, initial_voters) in &self.bootstrap_placements {
            let Ok(Some(placement)) = meta.local_placement(GroupId::Data(partition)) else {
                return false;
            };
            if placement.voters != *initial_voters || placement.r#move.is_some() {
                continue;
            }

            let Some(designated) = bootstrap::designated(initial_voters) else {
                return false;
            };
            if designated == self.node_id {
                let node = self.partitions.read().unwrap().get(&partition).cloned();
                let Some(node) = node else {
                    return false;
                };
                if !node.is_initialized().await.unwrap_or(false) {
                    return false;
                }
                continue;
            }
            let Some(addr) = self.addrs.control(designated) else {
                return false;
            };
            let request = Envelope::new(
                self.cluster_id,
                MsgType::BootstrapStatus,
                GroupId::Data(partition),
                0,
                codec::encode(&crate::transport::raft_wire::BootstrapStatusBody {
                    group: GroupId::Data(partition),
                    voters: initial_voters.clone(),
                }),
            );
            let Ok(reply) = self.control.call(&addr, request).await else {
                return false;
            };
            if reply.cluster_id != self.cluster_id
                || reply.msg_type != MsgType::BootstrapStatus
                || !matches!(
                    codec::decode(&reply.payload),
                    Ok(crate::transport::raft_wire::BootstrapStatusReply { ready: true })
                )
            {
                return false;
            }
        }
        true
    }

    // -- data-leader role ---------------------------------------------------

    /// Advance any in-flight plan for the partitions this node leads.
    async fn drive_data_role(&self) {
        let hosted: Vec<(u16, Arc<PartitionNode>)> = self
            .partitions
            .read()
            .unwrap()
            .iter()
            .map(|(p, n)| (*p, n.clone()))
            .collect();

        for (partition, node) in hosted {
            if node.current_leader() != Some(self.node_id) {
                continue;
            }
            let group = GroupId::Data(partition);
            let Some(placement) = self.read_placement(group).await else {
                continue;
            };
            let Some(plan) = placement.r#move.clone() else {
                continue;
            };

            match reconcile(&placement, &node.committed_voter_set()) {
                ReconcileAction::ResumePlan => {
                    if plan.aborting {
                        self.report_abort(&node, group, &placement, &plan).await;
                    } else {
                        self.execute_move(
                            &node,
                            group,
                            &placement,
                            &plan.target_voters,
                            plan.plan_id,
                        )
                        .await;
                    }
                }
                ReconcileAction::CompleteJoint => {
                    // Resolve the joint config to the target either way; then
                    // finalize a healthy plan or report the abort's benign
                    // completion.
                    if node.change_voters(&plan.target_voters).await.is_ok() {
                        if plan.aborting {
                            self.report_abort(&node, group, &placement, &plan).await;
                        } else {
                            self.report_finalize(&node, group, plan.plan_id, &plan.target_voters)
                                .await;
                        }
                    }
                }
                ReconcileAction::Finalize => {
                    if plan.aborting {
                        self.report_abort(&node, group, &placement, &plan).await;
                    } else {
                        self.report_finalize(&node, group, plan.plan_id, &plan.target_voters)
                            .await;
                    }
                }
                ReconcileAction::NoPlan | ReconcileAction::Error => {}
            }
        }
    }

    // -- reclaim role -------------------------------------------------------

    /// Stop and reclaim any partition this node still hosts but is no longer a
    /// voter of (DESIGN §7.3, §7.4). The decision is read purely from committed
    /// meta state: reclaim only once the move that removed this node has resolved
    /// (no in-flight plan) and the committed `voters` exclude it. That makes it
    /// safe — the cluster has already committed a config without this node — and
    /// idempotent, so a missed tick simply retries. A partition whose committed
    /// placement cannot be read is left untouched, never reclaimed on a guess.
    async fn drive_reclaim_role(&self) {
        let hosted: Vec<u16> = self.partitions.read().unwrap().keys().copied().collect();
        for partition in hosted {
            let Some(placement) = self.read_placement(GroupId::Data(partition)).await else {
                continue;
            };
            // An in-flight move may still name this node (a source mid-move or a
            // target learner): wait for the plan to resolve before reclaiming.
            if placement.r#move.is_some() {
                continue;
            }
            if placement.voters.contains(&self.node_id) {
                continue; // still a voter: keep serving
            }
            // Best-effort: a failed reclaim retries on the next tick.
            let _ = self.starter.reclaim_partition(partition).await;
        }
    }

    // -- meta membership role (self-referential) ----------------------------

    /// Advance an in-flight meta-group membership change. The meta leader drives
    /// the change to its *own* voter set — the meta-group analogue of
    /// `drive_data_role`. v1 only reaches here for a non-leader drain, so the
    /// leader stays leader across `add_learner`/`change_voters` and finalizes.
    async fn drive_meta_data_role(&self) {
        let Some(meta) = self.meta() else {
            return;
        };
        if meta.current_leader() != Some(self.node_id) {
            return; // only the meta leader drives its own membership change
        }
        let Some(placement) = meta.local_placement(GroupId::Meta).ok().flatten() else {
            return;
        };
        let Some(plan) = placement.r#move.clone() else {
            return;
        };

        match reconcile(&placement, &meta.committed_voter_set()) {
            ReconcileAction::ResumePlan => {
                if plan.aborting {
                    self.report_abort_meta(&meta, &placement, &plan).await;
                } else {
                    self.execute_meta_move(&meta, &placement, &plan.target_voters, plan.plan_id)
                        .await;
                }
            }
            ReconcileAction::CompleteJoint => {
                if meta.change_voters(&plan.target_voters).await.is_ok() {
                    if plan.aborting {
                        self.report_abort_meta(&meta, &placement, &plan).await;
                    } else {
                        self.report_finalize_meta(&meta, plan.plan_id, &plan.target_voters)
                            .await;
                    }
                }
            }
            ReconcileAction::Finalize => {
                if plan.aborting {
                    self.report_abort_meta(&meta, &placement, &plan).await;
                } else {
                    self.report_finalize_meta(&meta, plan.plan_id, &plan.target_voters)
                        .await;
                }
            }
            ReconcileAction::NoPlan | ReconcileAction::Error => {}
        }
    }

    /// The §7.2 healthy-plan steps for the meta group: gate, admit the replacement
    /// as a meta learner, add it, switch to the target voter set, then finalize.
    async fn execute_meta_move(
        &self,
        meta: &MetaNode,
        placement: &Placement,
        target: &[NodeId],
        plan_id: u64,
    ) {
        if gate(placement, &meta.committed_voter_set()) != GateDecision::Accept {
            return;
        }
        let current = voter_set(placement.voters.clone());
        let target_set = voter_set(target.to_vec());
        let Some(&learner) = target_set.difference(&current).next() else {
            return;
        };
        if !self
            .ensure_learner_admitted(learner, GroupId::Meta, plan_id)
            .await
        {
            return;
        }
        if meta.add_learner(learner).await.is_err() {
            return;
        }
        if meta.change_voters(target).await.is_err() {
            return;
        }
        self.report_finalize_meta(meta, plan_id, target).await;
    }

    /// Submit the meta membership change's completion, once the target config is
    /// committed. Mirrors `report_finalize` with `MetaNode` in place of the data
    /// leader.
    async fn report_finalize_meta(&self, meta: &MetaNode, plan_id: u64, target: &[NodeId]) {
        let Ok((config_log_id, committed)) = meta.confirmed_committed_config().await else {
            return;
        };
        if voter_set(committed) != voter_set(target.to_vec()) {
            return;
        }
        let Some(config_log_id) = config_log_id else {
            return;
        };
        let observation = DataConfigObservation {
            group: GroupId::Meta,
            plan_id,
            voter_set: target.to_vec(),
            config_log_id,
        };
        self.submit_observation(ObservationBody::Finalize {
            group: GroupId::Meta,
            plan_id,
            observation,
        })
        .await;
    }

    /// Resolve an aborting meta plan (§7.5). Mirrors `report_abort`.
    async fn report_abort_meta(&self, meta: &MetaNode, placement: &Placement, plan: &MovePlan) {
        let Ok((config_log_id, committed)) = meta.confirmed_committed_config().await else {
            return;
        };
        let committed = voter_set(committed);
        let report = if committed == voter_set(placement.voters.clone()) {
            placement.voters.clone()
        } else if committed == voter_set(plan.target_voters.clone()) {
            plan.target_voters.clone()
        } else {
            return;
        };
        let Some(config_log_id) = config_log_id else {
            return;
        };
        let observation = DataConfigObservation {
            group: GroupId::Meta,
            plan_id: plan.plan_id,
            voter_set: report,
            config_log_id,
        };
        self.submit_observation(ObservationBody::Abort {
            group: GroupId::Meta,
            plan_id: plan.plan_id,
            observation,
        })
        .await;
    }

    /// Stop and reclaim the local meta group once a drain has removed this node
    /// from the committed meta voter set — the meta-group analogue of
    /// `drive_reclaim_role`. A removed meta voter's *own* meta view freezes at
    /// removal (the leader stops replicating to it), so unlike the data case it
    /// cannot read the finalized placement locally. Instead it asks the current
    /// meta voters over the network: reclaim once one reports a resolved meta
    /// placement (no move) whose voter set excludes this node.
    async fn drive_meta_reclaim_role(&self) {
        if self.meta().is_none() {
            return; // not hosting the meta group
        }
        let body = PlacementQueryBody {
            group: GroupId::Meta,
        };
        let mut excluded = false;
        for addr in self.addrs.control_addrs() {
            let env = Envelope::new(
                self.cluster_id,
                MsgType::PlacementQuery,
                GroupId::Meta,
                0,
                codec::encode(&body),
            );
            if let Ok(reply) = self.control.call(&addr, env).await
                && let Ok(PlacementQueryReply { placement: Some(p) }) =
                    codec::decode::<PlacementQueryReply>(&reply.payload)
                && p.r#move.is_none()
                && !p.voters.contains(&self.node_id)
            {
                excluded = true;
                break;
            }
        }
        if excluded {
            let _ = self.meta_starter.reclaim_meta().await;
        }
    }

    /// The §7.2 healthy-plan steps: gate, admit the learner, add it, switch to
    /// the target voter set, then finalize. Each step is best-effort and
    /// resumable — a failure just retries on the next tick.
    async fn execute_move(
        &self,
        node: &PartitionNode,
        group: GroupId,
        placement: &Placement,
        target: &[NodeId],
        plan_id: u64,
    ) {
        if gate(placement, &node.committed_voter_set()) != GateDecision::Accept {
            return;
        }
        let current = voter_set(placement.voters.clone());
        let target_set = voter_set(target.to_vec());
        let Some(&learner) = target_set.difference(&current).next() else {
            return;
        };

        // Start the target's learner runtime before replicating to it, so
        // add_learner never blocks on an unstarted group.
        if !self.ensure_learner_admitted(learner, group, plan_id).await {
            return;
        }
        if node.add_learner(learner).await.is_err() {
            return;
        }
        // Design §11.1: a learner may not become a voter until it can serve
        // every active index. Promoting first would let a leader election hand
        // search traffic to a replica with no projection, which fails closed
        // but needlessly removes a partition from every strict search. A
        // not-ready learner pauses the move; the next tick retries.
        if !self.learner_indexes_ready(learner, group).await {
            return;
        }
        if node.change_voters(target).await.is_err() {
            return;
        }
        self.report_finalize(node, group, plan_id, target).await;
    }

    /// Ask a learner whether it has built every active index generation for
    /// the group it is about to vote in. An unreachable or not-ready learner
    /// answers `false`, which only delays the move.
    async fn learner_indexes_ready(&self, learner: NodeId, group: GroupId) -> bool {
        let Some(catalog) = self.search_catalog() else {
            return true;
        };
        let Ok(records) = catalog.catalog(false).await else {
            // The catalog is the only source of what must be ready; without it
            // the fence cannot be evaluated, so hold the move rather than
            // promote blind.
            return false;
        };
        let generations: Vec<(String, u64, [u8; 32])> = records
            .iter()
            .filter_map(|record| {
                record
                    .active
                    .as_ref()
                    .map(|active| (record.name.clone(), active.id, active.definition_hash))
            })
            .collect();
        if generations.is_empty() {
            return true;
        }
        let Some(addr) = self.addrs.control(learner) else {
            return false;
        };
        let envelope = Envelope::new(
            self.cluster_id,
            MsgType::SearchIndexStatus,
            group,
            0,
            codec::encode(&SearchIndexStatusBody::Query { group, generations }),
        );
        match self.control.call(&addr, envelope).await {
            Ok(reply) => match codec::decode::<SearchIndexStatusReply>(&reply.payload) {
                Ok(SearchIndexStatusReply::Status { ready: true, .. }) => true,
                Ok(SearchIndexStatusReply::Status { detail, .. }) => {
                    tracing::debug!(learner, ?group, detail, "search index fence holding move");
                    false
                }
                _ => false,
            },
            Err(_) => false,
        }
    }

    /// Report the completed move to the meta group. Only submits once the target
    /// config is committed; otherwise it retries next tick.
    async fn report_finalize(
        &self,
        node: &PartitionNode,
        group: GroupId,
        plan_id: u64,
        target: &[NodeId],
    ) {
        let Ok((config_log_id, committed)) = node.confirmed_committed_config().await else {
            return;
        };
        if voter_set(committed) != voter_set(target.to_vec()) {
            return;
        }
        let Some(config_log_id) = config_log_id else {
            return;
        };
        let observation = DataConfigObservation {
            group,
            plan_id,
            voter_set: target.to_vec(),
            config_log_id,
        };
        self.submit_observation(ObservationBody::Finalize {
            group,
            plan_id,
            observation,
        })
        .await;
    }

    /// Resolve an aborting plan (§7.5): report exactly `voters` (the move never
    /// promoted the learner — roll back) or exactly `target_voters` (the move
    /// completed before the abort — benign finalize). A joint config still in
    /// flight is left for a later tick after the data leader resolves it.
    async fn report_abort(
        &self,
        node: &PartitionNode,
        group: GroupId,
        placement: &Placement,
        plan: &MovePlan,
    ) {
        let Ok((config_log_id, committed)) = node.confirmed_committed_config().await else {
            return;
        };
        let committed = voter_set(committed);
        let report = if committed == voter_set(placement.voters.clone()) {
            placement.voters.clone()
        } else if committed == voter_set(plan.target_voters.clone()) {
            plan.target_voters.clone()
        } else {
            return;
        };
        let Some(config_log_id) = config_log_id else {
            return;
        };
        let observation = DataConfigObservation {
            group,
            plan_id: plan.plan_id,
            voter_set: report,
            config_log_id,
        };
        self.submit_observation(ObservationBody::Abort {
            group,
            plan_id: plan.plan_id,
            observation,
        })
        .await;
    }

    /// Send a `BecomeLearner` frame and confirm the target admitted it.
    async fn ensure_learner_admitted(&self, learner: NodeId, group: GroupId, plan_id: u64) -> bool {
        let Some(addr) = self.addrs.control(learner) else {
            return false;
        };
        let env = Envelope::new(
            self.cluster_id,
            MsgType::BecomeLearner,
            group,
            0,
            codec::encode(&BecomeLearnerBody { plan_id }),
        );
        match self.control.call(&addr, env).await {
            Ok(reply) => matches!(
                codec::decode::<LearnerReply>(&reply.payload),
                Ok(LearnerReply::Admitted)
            ),
            Err(_) => false,
        }
    }

    /// Read a group's placement through a meta ReadIndex barrier, locally when
    /// this node leads meta or over the network otherwise. This is the plan
    /// fence: cached pre-abort state must never authorize a membership change.
    async fn read_placement(&self, group: GroupId) -> Option<Placement> {
        if let Some(meta) = self.meta()
            && let Ok(MetaRead::Value(placement)) = meta.read_placement(group).await
        {
            return placement;
        }
        let body = PlacementQueryBody { group };
        for addr in self.addrs.control_addrs() {
            let env = Envelope::new(
                self.cluster_id,
                MsgType::PlacementQuery,
                GroupId::Meta,
                0,
                codec::encode(&body),
            );
            if let Ok(reply) = self.control.call(&addr, env).await
                && let Ok(PlacementQueryReply { placement: Some(p) }) =
                    codec::decode::<PlacementQueryReply>(&reply.payload)
            {
                return Some(p);
            }
        }
        None
    }

    /// Submit a config observation to the meta group: locally if this node leads
    /// meta, otherwise via a `DataConfigObservation` frame to a meta voter.
    async fn submit_observation(&self, body: ObservationBody) {
        if let Some(meta) = self.meta()
            && meta.current_leader() == Some(self.node_id)
        {
            let _ = meta.propose(body.into_meta_command()).await;
            return;
        }
        for addr in self.addrs.control_addrs() {
            let env = Envelope::new(
                self.cluster_id,
                MsgType::DataConfigObservation,
                GroupId::Meta,
                0,
                codec::encode(&body),
            );
            if let Ok(reply) = self.control.call(&addr, env).await
                && let Ok(SubmitReply::Outcome(_)) = codec::decode::<SubmitReply>(&reply.payload)
            {
                return;
            }
        }
    }
}
