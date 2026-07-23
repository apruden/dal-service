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
    BecomeLearnerBody, LearnerReply, ObservationBody, SubmitReply,
};
use crate::types::{
    ClusterId, DataConfigObservation, GroupId, NodeId, NodeState, Placement, voter_set,
};

use crate::api::gateway::PartitionMap;

const REBALANCE_INTERVAL: Duration = Duration::from_millis(150);

/// Per-node rebalance driver. Spawned only on meta voters.
pub struct RebalanceDriver {
    node_id: NodeId,
    cluster_id: ClusterId,
    partition_count: u16,
    meta: Arc<MetaNode>,
    partitions: PartitionMap,
    control: ZmqTransport,
    addrs: AddrBook,
    meta_controls: Vec<String>,
}

impl RebalanceDriver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        cluster_id: ClusterId,
        partition_count: u16,
        meta: Arc<MetaNode>,
        partitions: PartitionMap,
        control: ZmqTransport,
        addrs: AddrBook,
        meta_controls: Vec<String>,
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
        }
    }

    pub async fn run(self) {
        loop {
            tokio::time::sleep(REBALANCE_INTERVAL).await;
            self.drive_meta_role().await;
            self.drive_data_role().await;
        }
    }

    // -- meta-leader role ---------------------------------------------------

    /// Create move plans to drain `Draining` voters off their partitions. Runs
    /// only on the meta leader (plan creation is a meta write).
    async fn drive_meta_role(&self) {
        if self.meta.current_leader() != Some(self.node_id) {
            return;
        }
        let Ok(directory) = self.meta.local_directory() else {
            return;
        };
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
        if draining.is_empty() {
            return;
        }

        for partition in 0..self.partition_count {
            let group = GroupId::Data(partition);
            let Ok(Some(placement)) = self.meta.local_placement(group) else {
                continue;
            };
            if placement.r#move.is_some() {
                continue; // a plan is already in flight for this partition
            }
            let Some(&drained) = placement.voters.iter().find(|v| draining.contains(v)) else {
                continue;
            };
            let Some(&replacement) = active
                .iter()
                .find(|a| !placement.voters.contains(a))
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
            let _ = create_plan(std::slice::from_ref(&self.meta), group, &target).await;
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
            let Ok(Some(placement)) = self.meta.local_placement(group) else {
                continue;
            };
            let Some(plan) = placement.r#move.clone() else {
                continue;
            };

            match reconcile(&placement, &node.committed_voter_set()) {
                ReconcileAction::ResumePlan if !plan.aborting => {
                    self.execute_move(&node, group, &placement, &plan.target_voters, plan.plan_id)
                        .await;
                }
                ReconcileAction::CompleteJoint if !plan.aborting => {
                    if node.change_voters(&plan.target_voters).await.is_ok() {
                        self.report_finalize(&node, group, plan.plan_id, &plan.target_voters)
                            .await;
                    }
                }
                ReconcileAction::Finalize if !plan.aborting => {
                    self.report_finalize(&node, group, plan.plan_id, &plan.target_voters)
                        .await;
                }
                // Aborting plans and error phases are left to the (future)
                // abort driver; the move driver only advances healthy plans.
                _ => {}
            }
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

    /// Submit a config observation to the meta group: locally if this node leads
    /// meta, otherwise via a `DataConfigObservation` frame to a meta voter.
    async fn submit_observation(&self, body: ObservationBody) {
        if self.meta.current_leader() == Some(self.node_id) {
            let _ = self.meta.propose(body.into_meta_command()).await;
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
