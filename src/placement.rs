//! The deterministic placement balancer (DESIGN §5.1, §5.2).
//!
//! A pure function of the node directory and current placement map: it proposes
//! at most one single-voter replacement, breaking every tie by `NodeId` so the
//! same inputs always yield the same plan. Two concerns, in priority order:
//!
//! 1. **Drain** — a partition holding a voter that is no longer eligible (Down,
//!    Draining, or unknown) sheds it to an eligible non-replica node. This
//!    handles node leave/failure.
//! 2. **Rebalance** — once every replica is eligible, even out replica counts by
//!    moving one slot from the most-loaded eligible node to the least-loaded
//!    eligible non-replica node, but only while the gap is at least two (so the
//!    balancer converges to within one slot and does not oscillate).
//!
//! A partition with a move already in flight is skipped: never more than one
//! move per partition (§5.2). The caller commits a proposal as a `CreatePlan`;
//! the balancer itself writes nothing.

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{GroupId, NodeDirectoryEntry, NodeId, NodeState, Placement};

/// A proposed single-voter replacement for one group. `target_voters` differs
/// from the group's current voters by exactly one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanProposal {
    pub group: GroupId,
    pub target_voters: Vec<NodeId>,
}

/// The set of nodes eligible to host replicas: those the directory lists as
/// `Active`. Sorted for determinism.
fn eligible_nodes(directory: &[NodeDirectoryEntry]) -> BTreeSet<NodeId> {
    directory
        .iter()
        .filter(|e| e.state == NodeState::Active)
        .map(|e| e.node_id)
        .collect()
}

/// Replica load per node across all non-moving partitions.
fn load_map(placements: &BTreeMap<u16, Placement>) -> BTreeMap<NodeId, usize> {
    let mut load = BTreeMap::new();
    for placement in placements.values() {
        for &v in &placement.voters {
            *load.entry(v).or_insert(0) += 1;
        }
    }
    load
}

/// Propose the next single-voter move, or `None` if the cluster is already
/// balanced and fully placed on eligible nodes. `r` is the replication factor.
pub fn propose(
    directory: &[NodeDirectoryEntry],
    placements: &BTreeMap<u16, Placement>,
    r: usize,
) -> Option<PlanProposal> {
    let eligible = eligible_nodes(directory);
    if eligible.len() < r {
        return None;
    }

    let load = load_map(placements);
    let load_of = |n: NodeId| load.get(&n).copied().unwrap_or(0);

    // -- Priority 1: drain an ineligible voter -----------------------------
    // Partitions in ascending id; within a partition, the lowest ineligible
    // voter. Destination: the eligible non-replica of least load (tie: NodeId).
    for (&partition, placement) in placements {
        if placement.r#move.is_some() {
            continue;
        }
        let voters: BTreeSet<NodeId> = placement.voters.iter().copied().collect();
        if let Some(&drop) = placement
            .voters
            .iter()
            .filter(|v| !eligible.contains(v))
            .min()
            && let Some(dest) = least_loaded_non_replica(&eligible, &voters, &load_of)
        {
            return Some(replacement(
                GroupId::Data(partition),
                &placement.voters,
                drop,
                dest,
            ));
        }
    }

    // -- Priority 2: even out load among eligible nodes --------------------
    // Only when every replica is already eligible (drain pass found nothing).
    let mut best: Option<(NodeId, NodeId, u16)> = None; // (source, dest, partition)
    for (&partition, placement) in placements {
        if placement.r#move.is_some() {
            continue;
        }
        let voters: BTreeSet<NodeId> = placement.voters.iter().copied().collect();
        // Source: the most-loaded eligible voter of this partition.
        let Some(&source) = placement
            .voters
            .iter()
            .filter(|v| eligible.contains(v))
            .max_by(|a, b| load_of(**a).cmp(&load_of(**b)).then(b.cmp(a)))
        else {
            continue;
        };
        let Some(dest) = least_loaded_non_replica(&eligible, &voters, &load_of) else {
            continue;
        };
        // A move is worthwhile only if it strictly reduces the spread.
        if load_of(source) < load_of(dest) + 2 {
            continue;
        }
        let candidate = (source, dest, partition);
        best = Some(match best {
            None => candidate,
            Some(cur) => better_rebalance(cur, candidate, &load_of),
        });
    }

    best.map(|(source, dest, partition)| {
        let voters = &placements[&partition].voters;
        replacement(GroupId::Data(partition), voters, source, dest)
    })
}

/// The eligible node that is not already a voter here and carries the least
/// load; ties broken by lowest `NodeId`.
fn least_loaded_non_replica(
    eligible: &BTreeSet<NodeId>,
    voters: &BTreeSet<NodeId>,
    load_of: &impl Fn(NodeId) -> usize,
) -> Option<NodeId> {
    eligible
        .iter()
        .filter(|n| !voters.contains(n))
        .copied()
        .min_by(|a, b| load_of(*a).cmp(&load_of(*b)).then(a.cmp(b)))
}

/// Prefer the move that relieves the most-loaded source; ties by lowest
/// partition, then lowest source, then lowest dest — fully deterministic.
fn better_rebalance(
    cur: (NodeId, NodeId, u16),
    cand: (NodeId, NodeId, u16),
    load_of: &impl Fn(NodeId) -> usize,
) -> (NodeId, NodeId, u16) {
    let key = |t: (NodeId, NodeId, u16)| {
        // Higher source load first (negated via Reverse ordering below).
        (std::cmp::Reverse(load_of(t.0)), t.2, t.0, t.1)
    };
    if key(cand) < key(cur) { cand } else { cur }
}

/// Build a replacement voter list: `drop` out, `add` in, order normalised
/// ascending so equal sets encode identically.
fn replacement(group: GroupId, voters: &[NodeId], drop: NodeId, add: NodeId) -> PlanProposal {
    let mut target: Vec<NodeId> = voters.iter().copied().filter(|&v| v != drop).collect();
    target.push(add);
    target.sort_unstable();
    PlanProposal {
        group,
        target_voters: target,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LogId;

    fn node(id: NodeId, state: NodeState) -> NodeDirectoryEntry {
        NodeDirectoryEntry {
            node_id: id,
            control_addr: format!("c{id}"),
            bulk_addr: format!("b{id}"),
            state,
            incarnation: 1,
        }
    }

    fn active(ids: &[NodeId]) -> Vec<NodeDirectoryEntry> {
        ids.iter().map(|&i| node(i, NodeState::Active)).collect()
    }

    fn placement(voters: &[NodeId]) -> Placement {
        Placement {
            voters: voters.to_vec(),
            voters_log_id: LogId::new(1, 1),
            r#move: None,
        }
    }

    fn map(entries: &[(u16, &[NodeId])]) -> BTreeMap<u16, Placement> {
        entries.iter().map(|(p, v)| (*p, placement(v))).collect()
    }

    #[test]
    fn balanced_cluster_proposes_nothing() {
        let dir = active(&[1, 2, 3]);
        let pl = map(&[(0, &[1, 2, 3]), (1, &[1, 2, 3])]);
        assert_eq!(propose(&dir, &pl, 3), None);
    }

    #[test]
    fn too_few_eligible_nodes_proposes_nothing() {
        let dir = active(&[1, 2]);
        let pl = map(&[(0, &[1, 2, 3])]);
        assert_eq!(propose(&dir, &pl, 3), None);
    }

    #[test]
    fn drains_an_ineligible_voter() {
        // Node 3 is Down; node 4 is a fresh Active node. Partition 0's replica
        // on 3 must move to 4.
        let mut dir = active(&[1, 2, 4]);
        dir.push(node(3, NodeState::Down));
        let pl = map(&[(0, &[1, 2, 3])]);
        let proposal = propose(&dir, &pl, 3).unwrap();
        assert_eq!(proposal.group, GroupId::Data(0));
        assert_eq!(proposal.target_voters, vec![1, 2, 4]);
    }

    #[test]
    fn proposal_differs_by_exactly_one_voter() {
        let dir = active(&[1, 2, 3, 4]);
        // 4 hosts nothing; 1/2/3 are overloaded → one slot should move to 4.
        let pl = map(&[(0, &[1, 2, 3]), (1, &[1, 2, 3]), (2, &[1, 2, 3])]);
        let proposal = propose(&dir, &pl, 3).unwrap();
        let before: BTreeSet<_> = pl[&partition_of(&proposal)]
            .voters
            .iter()
            .copied()
            .collect();
        let after: BTreeSet<_> = proposal.target_voters.iter().copied().collect();
        assert_eq!(after.difference(&before).count(), 1);
        assert_eq!(before.difference(&after).count(), 1);
        assert_eq!(proposal.target_voters.len(), 3);
    }

    fn partition_of(p: &PlanProposal) -> u16 {
        match p.group {
            GroupId::Data(id) => id,
            GroupId::Meta => panic!("expected data group"),
        }
    }

    #[test]
    fn is_deterministic() {
        let dir = active(&[1, 2, 3, 4, 5]);
        let pl = map(&[
            (0, &[1, 2, 3]),
            (1, &[1, 2, 3]),
            (2, &[1, 2, 3]),
            (3, &[1, 2, 3]),
        ]);
        let a = propose(&dir, &pl, 3);
        let b = propose(&dir, &pl, 3);
        assert_eq!(a, b);
        assert!(a.is_some());
    }

    #[test]
    fn skips_partition_with_move_in_flight() {
        let dir = active(&[1, 2, 4]);
        let mut m = node(3, NodeState::Down);
        m.state = NodeState::Down;
        let mut dir2 = dir.clone();
        dir2.push(m);
        let mut pl = map(&[(0, &[1, 2, 3])]);
        pl.get_mut(&0).unwrap().r#move = Some(crate::types::MovePlan {
            plan_id: 7,
            target_voters: vec![1, 2, 4],
            aborting: false,
        });
        // The only draining candidate is already moving → nothing to propose.
        assert_eq!(propose(&dir2, &pl, 3), None);
    }
}
