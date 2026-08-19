//! The fenced move and abort drivers (DESIGN §7.2, §7.5, IMPLEMENTATION §M6).
//!
//! The data-Raft leader is the single serialization point for a partition's
//! membership change; the meta group is a durable work queue and routing aid.
//! Every step is idempotent so a crash resumes from the data group's committed
//! config (§5.2). These drivers orchestrate already-running meta and data nodes
//! over the ChannelNetwork; the target learner's runtime (durable admission +
//! group start) is a precondition, exactly as a real node would arrange before
//! answering `BecomeLearner`.

use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::meta::node::{MetaNode, MetaRead, ProposeOutcome};
use crate::meta::reconcile::{GateDecision, ReconcileAction, gate, reconcile};
use crate::meta::state_machine::MetaApplyResult;
use crate::partition::node::PartitionNode;
use crate::types::{
    DataConfigObservation, GroupId, MetaCommand, NodeId, NodeState, Placement, voter_set,
};

/// Bound on the §7.2 step-3 catch-up wait in this linear driver. Generous
/// enough for a snapshot-fed learner in tests; a learner that dies mid-move is
/// surfaced as an error instead of parking the caller forever.
const LEARNER_CATCHUP_TIMEOUT: Duration = Duration::from_secs(60);

/// A quorum-confirmed reading of the data group's committed configuration.
///
/// §7.5 requires every observation these drivers act on to be fenced. Local
/// Raft metrics are not: a deposed leader still reports the voter set it last
/// knew, so reporting from them can roll back a move the real leader already
/// completed, or gate a decision on membership that no longer holds. The
/// ReadIndex barrier inside `confirmed_committed_config` is what makes the
/// observation safe to submit.
async fn confirmed_voters(
    data_leader: &PartitionNode,
) -> Result<(
    Option<crate::types::LogId>,
    std::collections::BTreeSet<NodeId>,
)> {
    let (config_log_id, voters) = data_leader.confirmed_committed_config().await?;
    Ok((config_log_id, voters.into_iter().collect()))
}

/// Commit a `CreatePlan` for `group` and return its assigned `plan_id` (the meta
/// log index of creation — DESIGN §5.2).
pub async fn create_plan(
    meta: &[Arc<MetaNode>],
    group: GroupId,
    target_voters: &[NodeId],
) -> Result<u64> {
    accept(
        propose(
            meta,
            MetaCommand::CreatePlan {
                group,
                plan_id: 0,
                target_voters: target_voters.to_vec(),
            },
        )
        .await?,
        "CreatePlan",
    )?;
    let placement = read_placement(meta, group)
        .await?
        .ok_or_else(|| Error::Raft("placement missing after CreatePlan".into()))?;
    placement
        .r#move
        .map(|m| m.plan_id)
        .ok_or_else(|| Error::Raft("move missing after CreatePlan".into()))
}

/// Mark a plan aborting (operator command, or failure handling when the planned
/// learner is `Down` — DESIGN §7.5). One-way.
pub async fn mark_aborting(meta: &[Arc<MetaNode>], group: GroupId, plan_id: u64) -> Result<()> {
    accept(
        propose(meta, MetaCommand::MarkAborting { group, plan_id }).await?,
        "MarkAborting",
    )
}

/// Drive a healthy plan to completion (DESIGN §7.2 steps 3–5). Preconditions:
/// the plan is committed and the added learner's `PartitionNode` is already
/// started and reachable (its durable admission recorded).
pub async fn execute_move(
    meta: &[Arc<MetaNode>],
    data_leader: &PartitionNode,
    group: GroupId,
    plan_id: u64,
) -> Result<()> {
    let placement = read_placement(meta, group)
        .await?
        .ok_or_else(|| Error::Raft("no placement for move".into()))?;

    // §7.1 gate: the leader re-checks the record against its own committed
    // config before starting or resuming.
    let (_, gate_voters) = confirmed_voters(data_leader).await?;
    match gate(&placement, &gate_voters) {
        GateDecision::Accept => {}
        other => return Err(Error::Raft(format!("move gate refused: {other:?}"))),
    }
    let plan = placement.r#move.as_ref().unwrap();
    let target = plan.target_voters.clone();

    // The learner is the single node the target adds over the current voters.
    let current = voter_set(placement.voters.clone());
    let target_set = voter_set(target.clone());
    let learner = *target_set
        .difference(&current)
        .next()
        .ok_or_else(|| Error::Raft("target adds no learner".into()))?;

    // Step 3: add as learner and block (bounded) for exact durable catch-up
    // to the fixed membership entry that admitted it.
    data_leader.add_learner(learner).await?;
    data_leader
        .wait_learner_caught_up(learner, LEARNER_CATCHUP_TIMEOUT)
        .await?;
    // Step 4: joint-consensus change to the target voter set.
    data_leader.change_voters(&target).await?;
    // Step 5: barrier on the new config, then a single-voter-set observation.
    let _ = target_set;
    finalize(meta, data_leader, group, plan_id, &target).await
}

/// Step 5 in isolation: barrier on the target config being committed and
/// applied, then submit a single-voter-set `DataConfigObservation`. Split out so
/// the resume driver can finalize a move whose membership change already
/// committed before a crash.
async fn finalize(
    meta: &[Arc<MetaNode>],
    data_leader: &PartitionNode,
    group: GroupId,
    plan_id: u64,
    target: &[NodeId],
) -> Result<()> {
    let target_set = voter_set(target.to_vec());
    wait_committed(data_leader, &target_set).await?;
    let (config_log_id, _) = confirmed_voters(data_leader).await?;
    let config_log_id =
        config_log_id.ok_or_else(|| Error::Raft("committed config has no log id".into()))?;
    let observation = DataConfigObservation {
        group,
        plan_id,
        voter_set: target.to_vec(),
        config_log_id,
    };
    accept(
        propose(
            meta,
            MetaCommand::FinalizePlan {
                group,
                plan_id,
                observation,
            },
        )
        .await?,
        "FinalizePlan",
    )
}

/// Startup / crash reconciliation for one group (DESIGN §5.2, IMPLEMENTATION
/// §M6). Reads the meta record and the data group's committed voter set, then
/// drives the plan to a consistent end state from whatever durable point a
/// crash left behind — resume, complete the joint change, finalize, or confirm
/// an abort. Idempotent: a fully-resolved plan reconciles to a no-op.
pub async fn resume_move(
    meta: &[Arc<MetaNode>],
    data_leader: &PartitionNode,
    group: GroupId,
) -> Result<()> {
    let Some(placement) = read_placement(meta, group).await? else {
        return Ok(());
    };
    let Some(plan) = placement.r#move.clone() else {
        return Ok(());
    };
    let (_, committed) = confirmed_voters(data_leader).await?;

    match reconcile(&placement, &committed) {
        ReconcileAction::NoPlan => Ok(()),
        // Committed config still equals `voters`: the change never happened.
        ReconcileAction::ResumePlan => {
            if plan.aborting {
                execute_abort(meta, data_leader, group, plan.plan_id).await
            } else {
                execute_move(meta, data_leader, group, plan.plan_id).await
            }
        }
        // A joint config is in effect: complete the change, then resolve.
        ReconcileAction::CompleteJoint => {
            data_leader.change_voters(&plan.target_voters).await?;
            if plan.aborting {
                execute_abort(meta, data_leader, group, plan.plan_id).await
            } else {
                finalize(meta, data_leader, group, plan.plan_id, &plan.target_voters).await
            }
        }
        // Final config committed but the plan is not yet cleared: finalize (or,
        // for an aborting plan, report the benign late completion).
        ReconcileAction::Finalize => {
            if plan.aborting {
                execute_abort(meta, data_leader, group, plan.plan_id).await
            } else {
                finalize(meta, data_leader, group, plan.plan_id, &plan.target_voters).await
            }
        }
        ReconcileAction::Error => Err(Error::Raft(
            "reconcile: committed config matches no plan phase".into(),
        )),
    }
}

/// Drive an aborting plan to resolution (DESIGN §7.5). Handles the common case
/// where the learner was never promoted (committed config still equals
/// `voters`): the leader reports `voters` and the meta group clears the plan. If
/// the move actually completed (config equals `target_voters`) it finalizes
/// benignly instead.
pub async fn execute_abort(
    meta: &[Arc<MetaNode>],
    data_leader: &PartitionNode,
    group: GroupId,
    plan_id: u64,
) -> Result<()> {
    let placement = read_placement(meta, group)
        .await?
        .ok_or_else(|| Error::Raft("no placement for abort".into()))?;
    let plan = placement
        .r#move
        .as_ref()
        .ok_or_else(|| Error::Raft("no move to abort".into()))?;
    if !plan.aborting {
        return Err(Error::Raft("plan is not marked aborting".into()));
    }

    let voters = voter_set(placement.voters.clone());
    let target = voter_set(plan.target_voters.clone());
    let (config_log_id, committed) = confirmed_voters(data_leader).await?;
    let config_log_id =
        config_log_id.ok_or_else(|| Error::Raft("committed config has no log id".into()))?;

    // A joint config must be resolved by the leader before it can report a
    // single voter set (§7.5). The abort report always carries exactly `voters`
    // (roll back / clear) or exactly `target_voters` (the move completed before
    // the abort — meta finalizes benignly).
    let rollback = committed == voters;
    let voter_set_report = if rollback {
        placement.voters.clone()
    } else if committed == target {
        plan.target_voters.clone()
    } else {
        return Err(Error::Raft(
            "joint config in flight; leader must complete the change first".into(),
        ));
    };

    // Remove before clearing the plan, while its target still identifies the
    // learner. `remove_learner` is a true no-op once a previous attempt has
    // already committed the removal.
    if rollback {
        for learner in target.difference(&voters) {
            data_leader.remove_learner(*learner).await?;
        }
    }

    let observation = DataConfigObservation {
        group,
        plan_id,
        voter_set: voter_set_report,
        config_log_id,
    };
    accept(
        propose(
            meta,
            MetaCommand::AbortReport {
                group,
                plan_id,
                observation,
            },
        )
        .await?,
        "abort resolution",
    )
}

/// Commit a directory state transition (DESIGN §9.1). The incarnation must be
/// at least the recorded one; the meta state machine rejects a stale value.
pub async fn set_node_state(
    meta: &[Arc<MetaNode>],
    node_id: NodeId,
    state: NodeState,
    incarnation: u64,
) -> Result<()> {
    accept(
        propose(
            meta,
            MetaCommand::SetNodeState {
                node_id,
                state,
                incarnation,
            },
        )
        .await?,
        "SetNodeState",
    )
}

/// Graceful decommission of one partition replica (DESIGN §7.3a). Marks the
/// draining node `Draining`, then runs a fenced move that swaps it out for
/// `replacement` — the replacement joins as a learner and catches up *before*
/// the draining node leaves, so the partition never drops below majority.
///
/// The caller drains a non-leader (or transfers leadership first); openraft
/// steps a removed leader down after the final config commits, but draining a
/// follower keeps the serialization point stable.
pub async fn drain_partition(
    meta: &[Arc<MetaNode>],
    data_leader: &PartitionNode,
    group: GroupId,
    draining: NodeId,
    replacement: NodeId,
) -> Result<()> {
    // Mark Draining, reusing the node's current incarnation (Active -> Draining
    // is a real transition, so it commits rather than no-ops).
    let entry = read_node(meta, draining)
        .await?
        .ok_or_else(|| Error::Raft(format!("draining node {draining} not in directory")))?;
    set_node_state(meta, draining, NodeState::Draining, entry.incarnation).await?;

    let placement = read_placement(meta, group)
        .await?
        .ok_or_else(|| Error::Raft("no placement to drain".into()))?;
    let mut target: Vec<NodeId> = placement
        .voters
        .iter()
        .copied()
        .filter(|&v| v != draining)
        .collect();
    target.push(replacement);
    target.sort_unstable();

    let plan_id = create_plan(meta, group, &target).await?;
    execute_move(meta, data_leader, group, plan_id).await
}

// -- internals --------------------------------------------------------------

async fn read_node(
    meta: &[Arc<MetaNode>],
    node_id: NodeId,
) -> Result<Option<crate::types::NodeDirectoryEntry>> {
    for _ in 0..200 {
        if let Some(leader) = meta
            .iter()
            .find(|n| n.current_leader() == Some(n.node_id()))
        {
            match leader.read_node(node_id).await? {
                MetaRead::Value(e) => return Ok(e),
                MetaRead::NotLeader { .. } => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Error::Raft("no meta leader for node read".into()))
}

async fn wait_committed(
    node: &PartitionNode,
    target: &std::collections::BTreeSet<NodeId>,
) -> Result<()> {
    for _ in 0..200 {
        if &node.committed_voter_set() == target {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(Error::Raft("timed out waiting for committed config".into()))
}

async fn read_placement(meta: &[Arc<MetaNode>], group: GroupId) -> Result<Option<Placement>> {
    for _ in 0..200 {
        if let Some(leader) = meta
            .iter()
            .find(|n| n.current_leader() == Some(n.node_id()))
        {
            match leader.read_placement(group).await? {
                MetaRead::Value(p) => return Ok(p),
                MetaRead::NotLeader { .. } => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Error::Raft("no meta leader for placement read".into()))
}

async fn propose(meta: &[Arc<MetaNode>], cmd: MetaCommand) -> Result<MetaApplyResult> {
    for _ in 0..200 {
        if let Some(leader) = meta
            .iter()
            .find(|n| n.current_leader() == Some(n.node_id()))
        {
            match leader.propose(cmd.clone()).await? {
                ProposeOutcome::Applied(r) => return Ok(r),
                ProposeOutcome::NotLeader { .. } => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Error::Raft("no meta leader accepted the command".into()))
}

fn accept(result: MetaApplyResult, what: &str) -> Result<()> {
    match result {
        MetaApplyResult::Applied | MetaApplyResult::NoOp => Ok(()),
        MetaApplyResult::Rejected(r) => Err(Error::Raft(format!("{what} rejected: {r:?}"))),
    }
}
