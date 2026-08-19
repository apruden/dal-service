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
use crate::types::{MAX_CLUSTER_NODES, NodeId, NodeState};

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
/// stale heartbeat reactivating a `Down` node. `observe` additionally fences on
/// the replicated directory incarnation and drops a non-increasing process
/// epoch/sequence, so a replay is ignored without rejecting a restarted process
/// that begins at sequence one.
#[derive(Default)]
pub struct HeartbeatTracker {
    last_seen_ms: HashMap<NodeId, u64>,
    /// `(directory incarnation, durable process incarnation, sequence)`.
    last_stamp: HashMap<NodeId, (u64, u64, u64)>,
    /// Start of the grace period for a directory incarnation for which no
    /// matching heartbeat has been observed. This prevents a newly registered
    /// node (or a freshly started detector) from becoming Down immediately.
    unobserved_since_ms: HashMap<NodeId, (u64, u64)>,
}

/// Ceiling on distinct nodes tracked at once. Heartbeat frames are
/// unauthenticated and carry a caller-chosen `node_id`, so without this the
/// maps grow with whatever a peer sends rather than with the cluster. A
/// legitimate cluster never exceeds [`MAX_CLUSTER_NODES`].
const MAX_TRACKED_NODES: usize = MAX_CLUSTER_NODES;

impl HeartbeatTracker {
    pub fn new() -> Self {
        HeartbeatTracker::default()
    }

    /// Record a heartbeat. A lower directory incarnation is stale. Within one
    /// directory incarnation, a higher durable process incarnation starts a
    /// fresh stream and only a strictly increasing sequence is accepted. Thus a
    /// clean restart can begin at sequence one while its old process can no
    /// longer refresh liveness.
    ///
    /// Once [`MAX_TRACKED_NODES`] distinct nodes are held, an *unknown* node is
    /// refused rather than evicting an existing one: eviction would clear a
    /// live node's stamp and restart its grace period, letting a flood of
    /// synthetic ids push real nodes toward `Down`. Refusing instead caps
    /// memory while leaving established liveness untouched; [`Self::evaluate`]
    /// reclaims the slots of nodes the directory no longer lists.
    pub fn observe(
        &mut self,
        node: NodeId,
        incarnation: u64,
        process_incarnation: u64,
        seq: u64,
        now_ms: u64,
    ) -> bool {
        match self.last_stamp.get(&node) {
            Some(&(previous_incarnation, previous_process, previous_seq)) => {
                if incarnation < previous_incarnation
                    || (incarnation == previous_incarnation
                        && (process_incarnation < previous_process
                            || (process_incarnation == previous_process && seq <= previous_seq)))
                {
                    return false;
                }
            }
            None if self.last_stamp.len() >= MAX_TRACKED_NODES => return false,
            None => {}
        }
        self.last_stamp
            .insert(node, (incarnation, process_incarnation, seq));
        self.last_seen_ms.insert(node, now_ms);
        true
    }

    /// Time since the last heartbeat matching the directory's committed
    /// incarnation. `None` starts a full grace period and intentionally causes
    /// no transition during this evaluation. Heartbeats from an older or
    /// uncommitted future incarnation never refresh that grace period.
    fn silence_ms(&mut self, node: NodeId, incarnation: u64, now_ms: u64) -> Option<u64> {
        if self
            .last_stamp
            .get(&node)
            .is_some_and(|(observed, _, _)| *observed == incarnation)
        {
            self.unobserved_since_ms.remove(&node);
            return self
                .last_seen_ms
                .get(&node)
                .map(|&seen| now_ms.saturating_sub(seen));
        }

        match self.unobserved_since_ms.get_mut(&node) {
            Some((expected, since)) if *expected == incarnation => {
                Some(now_ms.saturating_sub(*since))
            }
            Some(entry) => {
                *entry = (incarnation, now_ms);
                None
            }
            None => {
                self.unobserved_since_ms.insert(node, (incarnation, now_ms));
                None
            }
        }
    }

    /// The directory transitions justified by current liveness, given each
    /// node's committed state and incarnation. Only nodes needing a change are
    /// returned.
    pub fn evaluate(
        &mut self,
        now_ms: u64,
        timeouts: &Timeouts,
        states: &[(NodeId, NodeState, u64)],
    ) -> Vec<(NodeId, NodeState)> {
        // The committed directory is the authority on which nodes exist, so
        // anything absent from it is either departed or never real. Dropping
        // those here is what keeps the `observe` cap from being held down by
        // ids the cluster has no record of.
        let known: std::collections::HashSet<NodeId> =
            states.iter().map(|&(node, _, _)| node).collect();
        self.last_stamp.retain(|node, _| known.contains(node));
        self.last_seen_ms.retain(|node, _| known.contains(node));
        self.unobserved_since_ms
            .retain(|node, _| known.contains(node));

        states
            .iter()
            .filter_map(|&(node, current, incarnation)| {
                let silence = self.silence_ms(node, incarnation, now_ms)?;
                let evidence = classify(Duration::from_millis(silence), timeouts);
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
        assert!(t.observe(1, 1, 1, 5, 1000));
        // Same or lower sequence is ignored (replay / stale).
        assert!(!t.observe(1, 1, 1, 5, 2000));
        assert!(!t.observe(1, 1, 1, 4, 2000));
        // A newer sequence refreshes liveness.
        assert!(t.observe(1, 1, 1, 6, 3000));
        // A restarted process advances its process incarnation and restarts at
        // sequence one within the same replicated directory incarnation.
        assert!(t.observe(1, 1, 2, 1, 4000));
        // The prior process cannot refresh liveness after that restart.
        assert!(!t.observe(1, 1, 1, 7, 5000));
    }

    /// Heartbeat frames are unauthenticated and name their own `node_id`, so a
    /// peer must not be able to grow the tracker past the cluster bound.
    #[test]
    fn tracker_refuses_unknown_nodes_past_the_cap() {
        let mut t = HeartbeatTracker::new();
        for node in 0..MAX_TRACKED_NODES as u64 {
            assert!(t.observe(node, 1, 1, 1, 1000));
        }
        assert_eq!(t.last_stamp.len(), MAX_TRACKED_NODES);

        // A further distinct id is refused rather than admitted or swapped in.
        assert!(!t.observe(9_999_999, 1, 1, 1, 2000));
        assert_eq!(t.last_stamp.len(), MAX_TRACKED_NODES);

        // An already-tracked node still refreshes at capacity, so a flood
        // cannot stall liveness for nodes that are already established.
        assert!(t.observe(0, 1, 1, 2, 3000));
        assert_eq!(t.last_seen_ms.get(&0), Some(&3000));
    }

    /// Evaluating against the committed directory reclaims slots held by ids
    /// the cluster does not list, so a flood cannot permanently lock out a
    /// genuinely new node.
    #[test]
    fn evaluate_prunes_nodes_absent_from_the_directory() {
        let mut t = HeartbeatTracker::new();
        for node in 0..MAX_TRACKED_NODES as u64 {
            assert!(t.observe(node, 1, 1, 1, 1000));
        }
        assert!(!t.observe(4242, 1, 1, 1, 1000));

        // The directory lists only node 0; everything else was never real.
        let states = [(0u64, NodeState::Active, 1u64)];
        t.evaluate(1500, &timeouts(), &states);
        assert_eq!(t.last_stamp.len(), 1);

        // The freed capacity now admits the new node.
        assert!(t.observe(4242, 1, 1, 1, 2000));
    }

    #[test]
    fn evaluate_proposes_suspect_then_down() {
        let t0 = 10_000u64;
        let mut tr = HeartbeatTracker::new();
        tr.observe(1, 1, 1, 1, t0);
        let to = timeouts();

        // Fresh: no transition.
        assert!(
            tr.evaluate(t0, &to, &[(1, NodeState::Active, 1)])
                .is_empty()
        );
        // Past suspect_timeout: Active -> Suspect.
        assert_eq!(
            tr.evaluate(t0 + ms(to.suspect), &to, &[(1, NodeState::Active, 1)]),
            vec![(1, NodeState::Suspect)]
        );
        // Past down_timeout: Suspect -> Down.
        assert_eq!(
            tr.evaluate(t0 + ms(to.down), &to, &[(1, NodeState::Suspect, 1)]),
            vec![(1, NodeState::Down)]
        );
    }

    #[test]
    fn evaluate_never_reactivates_down_from_silence_or_beat() {
        let mut tr = HeartbeatTracker::new();
        tr.observe(1, 1, 1, 1, 0);
        // Even a fresh heartbeat produces no transition for a Down node — only an
        // explicit rejoin (incarnation bump via the meta SM) may revive it.
        let out = tr.evaluate(0, &timeouts(), &[(1, NodeState::Down, 1)]);
        assert!(out.is_empty());
    }

    #[test]
    fn unobserved_node_gets_a_full_grace_period() {
        let mut tr = HeartbeatTracker::new();
        let to = timeouts();
        let start = 10_000;

        assert!(
            tr.evaluate(start, &to, &[(7, NodeState::Active, 1)])
                .is_empty()
        );
        assert_eq!(
            tr.evaluate(start + ms(to.suspect), &to, &[(7, NodeState::Active, 1)]),
            vec![(7, NodeState::Suspect)]
        );
        assert_eq!(
            tr.evaluate(start + ms(to.down), &to, &[(7, NodeState::Suspect, 1)]),
            vec![(7, NodeState::Down)]
        );
    }

    #[test]
    fn old_incarnation_cannot_refresh_current_liveness() {
        let mut tr = HeartbeatTracker::new();
        let to = timeouts();
        let start = 20_000;

        assert!(tr.observe(3, 1, 1, 1, start));
        // The directory has committed incarnation 2, so incarnation 1 starts
        // no liveness stream for the current registration.
        assert!(
            tr.evaluate(start, &to, &[(3, NodeState::Active, 2)])
                .is_empty()
        );
        assert!(tr.observe(3, 1, 1, 2, start + ms(to.down) - 1));
        assert_eq!(
            tr.evaluate(start + ms(to.down), &to, &[(3, NodeState::Suspect, 2)]),
            vec![(3, NodeState::Down)]
        );
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
