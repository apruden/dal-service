//! Failure detection: the pure liveness classifier and directory transition
//! table (DESIGN §9.1).
//!
//! Heartbeats are *liveness evidence only*; `Suspect`, `Down`, and reactivation
//! are committed meta-group decisions. This module computes what transition the
//! evidence justifies — the meta leader commits it via `SetNodeState`, whose
//! incarnation guard (the meta state machine) makes a stale or replayed
//! heartbeat unable to reactivate a `Down` node.

use std::time::Duration;

use crate::config::Timeouts;
use crate::types::NodeState;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn timeouts() -> Timeouts {
        Timeouts::default()
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
