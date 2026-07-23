//! The rebalance driver loop (DESIGN §7, M6 mechanics over the network).
//!
//! Two roles run each tick on every meta-voter node:
//!
//! - **meta-leader role** (only while this node leads the meta group): a drain
//!   trigger that, for each partition with a `Draining` voter and no active plan,
//!   creates a move plan swapping that voter for an `Active` non-voter.
//! - **data-leader role** (for each hosted partition this node leads): read the
//!   plan from *local* committed meta state, gate it, ensure the target has
//!   started its learner runtime (`BecomeLearner`), drive `add_learner` +
//!   `change_voters`, then report the result to the meta group with a
//!   `DataConfigObservation` frame.
//!
//! Reading the plan locally means the data leader needs no meta leadership; the
//! only meta write it performs is the observation, submitted over the network.
//! Constraint (M8 slice): the driver runs on meta voters, so a partition whose
//! data leader is not a meta voter is not driven — true for small clusters where
//! every node is a meta voter.

use std::sync::Arc;
use std::time::Duration;

use crate::codec;
use crate::meta::node::MetaNode;
use crate::meta::rebalancer::create_plan;
use crate::meta::reconcile::{GateDecision, ReconcileAction, gate, reconcile};
use crate::partition::node::PartitionNode;
use crate::transport::Transport;
use crate::transport::codec::{Envelope, MsgType};
use crate::transport::dealer::ZmqTransport;
use crate::transport::raft_net::AddrBook;
use crate::transport::raft_wire::{
    BecomeLearnerBody, LearnerReply, ObservationBody, PlacementQueryBody, PlacementQueryReply,
    SubmitReply,
};
use crate::types::{
    ClusterId, DataConfigObservation, GroupId, MetaCommand, MovePlan, NodeId, NodeState, Placement,
    voter_set,
};

use crate::api::gateway::PartitionMap;
use crate::runtime::node::{MetaHandle, PartitionStarter};

const REBALANCE_INTERVAL: Duration = Duration::from_millis(150);

/// Per-node rebalance driver, spawned on every node. The meta-leader role is a
/// no-op unless this node runs and leads the meta group; the data-leader role
/// runs wherever a partition is led, reading the plan locally when this node is
/// a meta voter and over the network otherwise.
pub struct RebalanceDriver {
    node_id: NodeId,
    cluster_id: ClusterId,
    partition_count: u16,
    meta: MetaHandle,
    partitions: PartitionMap,
    control: ZmqTransport,
    addrs: AddrBook,
    meta_controls: Vec<String>,
    starter: Arc<PartitionStarter>,
}

impl RebalanceDriver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        cluster_id: ClusterId,
        partition_count: u16,
        meta: MetaHandle,
        partitions: PartitionMap,
        control: ZmqTransport,
        addrs: AddrBook,
        meta_controls: Vec<String>,
        starter: Arc<PartitionStarter>,
    ) -> RebalanceDriver {
        RebalanceDriver {
            node_id,
            cluster_id,
            partition_count,
            meta,
            partitions,
            control,
            addrs,
            meta_controls,
            starter,
        }
    }

    /// Snapshot the meta handle; the guard is dropped before any `.await`.
    fn meta(&self) -> Option<Arc<MetaNode>> {
        self.meta.read().unwrap().clone()
    }

    pub async fn run(self) {
        loop {
            tokio::time::sleep(REBALANCE_INTERVAL).await;
            self.drive_meta_role().await;
            self.drive_data_role().await;
            self.drive_reclaim_role().await;
        }
    }

    // -- meta-leader role ---------------------------------------------------

    /// Meta writes that only the leader may make: create drain plans, and abort
    /// a plan whose learner target has been declared `Down` (§7.5) — such a plan
    /// can never complete, so it must roll back.
    async fn drive_meta_role(&self) {
        let Some(meta) = self.meta() else {
            return; // not a meta node; nothing to plan
        };
        let meta = &meta;
        if meta.current_leader() != Some(self.node_id) {
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
        let draining: Vec<NodeId> = directory
            .iter()
            .filter(|e| e.state == NodeState::Draining)
            .map(|e| e.node_id)
            .collect();

        for partition in 0..self.partition_count {
            let group = GroupId::Data(partition);
            let Ok(Some(placement)) = meta.local_placement(group) else {
                continue;
            };
            match &placement.r#move {
                // No plan: drain a `Draining` voter onto an `Active` spare.
                None => {
                    let Some(&drained) = placement.voters.iter().find(|v| draining.contains(v))
                    else {
                        continue;
                    };
                    let Some(&replacement) = active.iter().find(|a| !placement.voters.contains(a))
                    else {
                        continue; // no free Active node to take over
                    };
                    let mut target: Vec<NodeId> = placement
                        .voters
                        .iter()
                        .copied()
                        .filter(|&v| v != drained)
                        .collect();
                    target.push(replacement);
                    target.sort_unstable();
                    let _ = create_plan(std::slice::from_ref(meta), group, &target).await;
                }
                // Healthy plan whose learner target is Down: abort it.
                Some(plan) if !plan.aborting => {
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
                Some(_) => {} // already aborting; resolution is the data-leader role's job
            }
        }
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
        if node.change_voters(target).await.is_err() {
            return;
        }
        self.report_finalize(node, group, plan_id, target).await;
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
        if node.committed_voter_set() != voter_set(target.to_vec()) {
            return;
        }
        let (config_log_id, _) = node.committed_config();
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
        let committed = node.committed_voter_set();
        let report = if committed == voter_set(placement.voters.clone()) {
            placement.voters.clone()
        } else if committed == voter_set(plan.target_voters.clone()) {
            plan.target_voters.clone()
        } else {
            return;
        };
        let (config_log_id, _) = node.committed_config();
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

    /// Read a group's committed placement: locally when this node is a meta
    /// voter, otherwise over the network from a meta voter.
    async fn read_placement(&self, group: GroupId) -> Option<Placement> {
        if let Some(meta) = self.meta() {
            return meta.local_placement(group).ok().flatten();
        }
        let body = PlacementQueryBody { group };
        for addr in &self.meta_controls {
            let env = Envelope::new(
                self.cluster_id,
                MsgType::PlacementQuery,
                GroupId::Meta,
                0,
                codec::encode(&body),
            );
            if let Ok(reply) = self.control.call(addr, env).await
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
        for addr in &self.meta_controls {
            let env = Envelope::new(
                self.cluster_id,
                MsgType::DataConfigObservation,
                GroupId::Meta,
                0,
                codec::encode(&body),
            );
            if let Ok(reply) = self.control.call(addr, env).await
                && let Ok(SubmitReply::Outcome(_)) = codec::decode::<SubmitReply>(&reply.payload)
            {
                return;
            }
        }
    }
}
