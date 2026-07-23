//! Failure detection: the pure liveness classifier and directory transition
//! table (DESIGN §9.1).
//!
//! Heartbeats are *liveness evidence only*; `Suspect`, `Down`, and reactivation
//! are committed meta-group decisions. This module computes what transition the
//! evidence justifies — the meta leader commits it via `SetNodeState`, whose
//! incarnation guard (the meta state machine) makes a stale or replayed
//! heartbeat unable to reactivate a `Down` node.

use std::collections::HashMap;
use std::time::Duration;

use crate::config::Timeouts;
use crate::types::{NodeId, NodeState};

/// Liveness inferred from time since the last heartbeat (DESIGN §9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liveness {
    Alive,
    Suspect,
    Down,
}

/// Classify liveness from the gap since the last heartbeat. `Down` dominates
/// `Suspect`; both are only *evidence* — the meta group still has to commit the
/// transition.
pub fn classify(since_last_heartbeat: Duration, timeouts: &Timeouts) -> Liveness {
    if since_last_heartbeat >= timeouts.down {
        Liveness::Down
    } else if since_last_heartbeat >= timeouts.suspect {
        Liveness::Suspect
    } else {
        Liveness::Alive
    }
}

/// The directory transition a piece of liveness evidence justifies for a node in
/// `current` state, or `None` if no change is warranted (DESIGN §9.1).
///
/// Reactivation from `Down` is deliberately *not* produced here: a `Down` node
/// re-enters only through an explicit rejoin/incarnation bump, never from a
/// heartbeat, so a stale heartbeat can never revive it.
pub fn next_state(current: NodeState, evidence: Liveness) -> Option<NodeState> {
    match (current, evidence) {
        // Fresh evidence clears a Suspect back to Active; a Down node needs an
        // explicit rejoin, so heartbeat evidence never reactivates it.
        (NodeState::Suspect, Liveness::Alive) => Some(NodeState::Active),
        (NodeState::Active, Liveness::Suspect) => Some(NodeState::Suspect),
        // Suspect or Active may progress to Down on down-timeout evidence.
        (NodeState::Active, Liveness::Down) | (NodeState::Suspect, Liveness::Down) => {
            Some(NodeState::Down)
        }
        // A draining node is being decommissioned; liveness does not move it.
        // Everything else (including any evidence for a Down node) is a no-op.
        _ => None,
    }
}

/// Tracks per-node heartbeat liveness on the meta leader (DESIGN §9.1). Time is
/// injected (`now_ms`) so the detector is deterministic and testable without a
/// clock; the caller supplies wall-clock milliseconds at runtime.
///
/// Heartbeats are liveness evidence only. `evaluate` returns the directory
/// transitions the evidence *justifies*; the meta leader commits them via
/// `SetNodeState`, whose incarnation guard is the actual defence against a
/// stale heartbeat reactivating a `Down` node. `observe` additionally drops a
/// non-increasing sequence, so a replayed heartbeat frame is ignored outright.
#[derive(Default)]
pub struct HeartbeatTracker {
    last_seen_ms: HashMap<NodeId, u64>,
    last_seq: HashMap<NodeId, u64>,
}

impl HeartbeatTracker {
    pub fn new() -> Self {
        HeartbeatTracker::default()
    }

    /// Record a heartbeat. Returns `false` (ignored) when `seq` is not strictly
    /// greater than the last seen sequence for `node` — a stale or replayed
    /// frame cannot refresh liveness.
    pub fn observe(&mut self, node: NodeId, seq: u64, now_ms: u64) -> bool {
        if let Some(&prev) = self.last_seq.get(&node) {
            if seq <= prev {
                return false;
            }
        }
        self.last_seq.insert(node, seq);
        self.last_seen_ms.insert(node, now_ms);
        true
    }

    /// Time since the last accepted heartbeat, or `u64::MAX` if never seen.
    fn silence_ms(&self, node: NodeId, now_ms: u64) -> u64 {
        self.last_seen_ms
            .get(&node)
            .map(|&t| now_ms.saturating_sub(t))
            .unwrap_or(u64::MAX)
    }

    /// The directory transitions justified by current liveness, given each
    /// node's committed state. Only nodes needing a change are returned.
    pub fn evaluate(
        &self,
        now_ms: u64,
        timeouts: &Timeouts,
        states: &[(NodeId, NodeState)],
    ) -> Vec<(NodeId, NodeState)> {
        states
            .iter()
            .filter_map(|&(node, current)| {
                let evidence =
                    classify(Duration::from_millis(self.silence_ms(node, now_ms)), timeouts);
                next_state(current, evidence).map(|s| (node, s))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timeouts() -> Timeouts {
        Timeouts::default()
    }

    fn ms(d: Duration) -> u64 {
        d.as_millis() as u64
    }

    #[test]
    fn tracker_ignores_stale_and_replayed_heartbeats() {
        let mut t = HeartbeatTracker::new();
        assert!(t.observe(1, 5, 1000));
        // Same or lower sequence is ignored (replay / stale).
        assert!(!t.observe(1, 5, 2000));
        assert!(!t.observe(1, 4, 2000));
        // A newer sequence refreshes liveness.
        assert!(t.observe(1, 6, 3000));
    }

    #[test]
    fn evaluate_proposes_suspect_then_down() {
        let t0 = 10_000u64;
        let mut tr = HeartbeatTracker::new();
        tr.observe(1, 1, t0);
        let to = timeouts();

        // Fresh: no transition.
        assert!(tr
            .evaluate(t0, &to, &[(1, NodeState::Active)])
            .is_empty());
        // Past suspect_timeout: Active -> Suspect.
        assert_eq!(
            tr.evaluate(t0 + ms(to.suspect), &to, &[(1, NodeState::Active)]),
            vec![(1, NodeState::Suspect)]
        );
        // Past down_timeout: Suspect -> Down.
        assert_eq!(
            tr.evaluate(t0 + ms(to.down), &to, &[(1, NodeState::Suspect)]),
            vec![(1, NodeState::Down)]
        );
    }

    #[test]
    fn evaluate_never_reactivates_down_from_silence_or_beat() {
        let mut tr = HeartbeatTracker::new();
        tr.observe(1, 1, 0);
        // Even a fresh heartbeat produces no transition for a Down node — only an
        // explicit rejoin (incarnation bump via the meta SM) may revive it.
        let out = tr.evaluate(0, &timeouts(), &[(1, NodeState::Down)]);
        assert!(out.is_empty());
    }

    #[test]
    fn classify_thresholds() {
        let t = timeouts();
        assert_eq!(classify(Duration::from_millis(0), &t), Liveness::Alive);
        assert_eq!(classify(t.suspect, &t), Liveness::Suspect);
        assert_eq!(classify(t.down, &t), Liveness::Down);
        assert_eq!(
            classify(t.down + Duration::from_secs(1), &t),
            Liveness::Down
        );
    }

    #[test]
    fn active_to_suspect_to_down_and_back() {
        assert_eq!(
            next_state(NodeState::Active, Liveness::Suspect),
            Some(NodeState::Suspect)
        );
        assert_eq!(
            next_state(NodeState::Suspect, Liveness::Down),
            Some(NodeState::Down)
        );
        assert_eq!(
            next_state(NodeState::Suspect, Liveness::Alive),
            Some(NodeState::Active)
        );
    }

    #[test]
    fn down_is_never_reactivated_by_heartbeat() {
        assert_eq!(next_state(NodeState::Down, Liveness::Alive), None);
        assert_eq!(next_state(NodeState::Down, Liveness::Suspect), None);
        assert_eq!(next_state(NodeState::Down, Liveness::Down), None);
    }

    #[test]
    fn stable_states_are_noops() {
        assert_eq!(next_state(NodeState::Active, Liveness::Alive), None);
        assert_eq!(next_state(NodeState::Draining, Liveness::Down), None);
    }
}
