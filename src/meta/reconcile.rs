//! Pure move-gate and startup-reconciliation logic (DESIGN §5.2, §7.1).
//!
//! Config comparisons throughout §7 are over **voter sets** — learners are
//! ignored, so a committed `add_learner` still compares equal to `voters` and a
//! crash at the learner stage resumes the plan rather than erroring. These
//! functions are pure over the meta placement record and the data group's
//! committed voter set, so they are trivially unit-testable and identical on
//! every replica.

use std::collections::BTreeSet;

use crate::types::{NodeId, Placement, voter_set};

/// Whether a data-Raft leader may start or resume a plan (DESIGN §7.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateDecision {
    /// The plan is live, healthy, and this leader's committed config matches its
    /// `voters` with a legal single-voter target — proceed.
    Accept,
    /// No move is recorded for the group.
    NoPlan,
    /// The plan is marked aborting; no leader may start or resume it (§7.5).
    RejectAborting,
    /// The leader's committed voter set does not equal the record's `voters`
    /// (a change is already in progress, or the record is stale).
    RejectConfigMismatch,
    /// The target is not a single-voter replacement of `voters`.
    RejectNotSingleVoter,
}

/// The §7.1 gate: a data-Raft leader accepts a plan only when it is not
/// aborting, its committed config equals the record's `voters`, and the target
/// replaces exactly one voter.
pub fn gate(placement: &Placement, committed_voters: &BTreeSet<NodeId>) -> GateDecision {
    let Some(plan) = &placement.r#move else {
        return GateDecision::NoPlan;
    };
    if plan.aborting {
        return GateDecision::RejectAborting;
    }
    let voters = voter_set(placement.voters.clone());
    if &voters != committed_voters {
        return GateDecision::RejectConfigMismatch;
    }
    let target = voter_set(plan.target_voters.clone());
    let added = target.difference(&voters).count();
    let removed = voters.difference(&target).count();
    if added != 1 || removed != 1 {
        return GateDecision::RejectNotSingleVoter;
    }
    GateDecision::Accept
}

/// What a recovering node should do for a group, from its data-Raft committed
/// config versus the meta record (DESIGN §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileAction {
    /// No plan present: nothing to reconcile.
    NoPlan,
    /// Committed config equals `voters`: resume the plan (or, if aborting,
    /// confirm the abort — §7.5).
    ResumePlan,
    /// Committed config equals `voters ∪ target_voters`: a joint config is in
    /// effect; complete the membership change.
    CompleteJoint,
    /// Committed config equals `target_voters`: finalize the plan.
    Finalize,
    /// Committed config matches none of the above — operator-visible error.
    Error,
}

/// Reconcile one group on startup (DESIGN §5.2). `committed_voters` is the data
/// group's committed voter set (union of both configs while joint).
pub fn reconcile(placement: &Placement, committed_voters: &BTreeSet<NodeId>) -> ReconcileAction {
    let Some(plan) = &placement.r#move else {
        return ReconcileAction::NoPlan;
    };
    let voters = voter_set(placement.voters.clone());
    let target = voter_set(plan.target_voters.clone());
    let union: BTreeSet<NodeId> = voters.union(&target).copied().collect();

    if committed_voters == &voters {
        ReconcileAction::ResumePlan
    } else if committed_voters == &target {
        ReconcileAction::Finalize
    } else if committed_voters == &union {
        ReconcileAction::CompleteJoint
    } else {
        ReconcileAction::Error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{GroupId, LogId, MovePlan};

    fn placement(voters: &[NodeId], target: Option<(&[NodeId], bool)>) -> Placement {
        Placement {
            voters: voters.to_vec(),
            voters_log_id: LogId::new(1, 1),
            r#move: target.map(|(t, aborting)| MovePlan {
                plan_id: 42,
                target_voters: t.to_vec(),
                aborting,
            }),
        }
    }

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    #[test]
    fn gate_accepts_healthy_single_voter_plan() {
        let p = placement(&[1, 2, 3], Some((&[1, 2, 4], false)));
        assert_eq!(gate(&p, &set(&[1, 2, 3])), GateDecision::Accept);
    }

    #[test]
    fn gate_rejects_aborting() {
        let p = placement(&[1, 2, 3], Some((&[1, 2, 4], true)));
        assert_eq!(gate(&p, &set(&[1, 2, 3])), GateDecision::RejectAborting);
    }

    #[test]
    fn gate_rejects_config_mismatch() {
        // Leader already moved to the joint/target config.
        let p = placement(&[1, 2, 3], Some((&[1, 2, 4], false)));
        assert_eq!(
            gate(&p, &set(&[1, 2, 4])),
            GateDecision::RejectConfigMismatch
        );
    }

    #[test]
    fn gate_no_plan() {
        let p = placement(&[1, 2, 3], None);
        assert_eq!(gate(&p, &set(&[1, 2, 3])), GateDecision::NoPlan);
    }

    #[test]
    fn reconcile_covers_all_phases() {
        let p = placement(&[1, 2, 3], Some((&[1, 2, 4], false)));
        // learner stage / not started: committed == voters
        assert_eq!(reconcile(&p, &set(&[1, 2, 3])), ReconcileAction::ResumePlan);
        // joint config: union of voters and target
        assert_eq!(
            reconcile(&p, &set(&[1, 2, 3, 4])),
            ReconcileAction::CompleteJoint
        );
        // final config committed
        assert_eq!(reconcile(&p, &set(&[1, 2, 4])), ReconcileAction::Finalize);
        // nonsense config
        assert_eq!(reconcile(&p, &set(&[5, 6, 7])), ReconcileAction::Error);
    }

    #[test]
    fn reconcile_no_plan_is_noop() {
        let p = placement(&[1, 2, 3], None);
        assert_eq!(reconcile(&p, &set(&[1, 2, 3])), ReconcileAction::NoPlan);
        let _ = GroupId::Meta;
    }
}
