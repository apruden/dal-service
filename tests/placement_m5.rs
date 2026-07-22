//! M5 gate: balancer property tests (DESIGN §5.1) — determinism, single-voter
//! diff on every proposal, and convergence to near-even replica counts.

use std::collections::{BTreeMap, BTreeSet};

use dal::placement::propose;
use dal::types::{GroupId, LogId, NodeDirectoryEntry, NodeId, NodeState, Placement};

use proptest::prelude::*;

const R: usize = 3;

fn directory(n: usize) -> Vec<NodeDirectoryEntry> {
    (1..=n as u64)
        .map(|id| NodeDirectoryEntry {
            node_id: id,
            control_addr: format!("c{id}"),
            bulk_addr: format!("b{id}"),
            state: NodeState::Active,
            incarnation: 1,
        })
        .collect()
}

fn placements(parts: &[Vec<NodeId>]) -> BTreeMap<u16, Placement> {
    parts
        .iter()
        .enumerate()
        .map(|(i, voters)| {
            (
                i as u16,
                Placement {
                    voters: voters.clone(),
                    voters_log_id: LogId::new(1, 1),
                    r#move: None,
                },
            )
        })
        .collect()
}

/// A random cluster: `n` active nodes and `P` partitions, each with `R` distinct
/// voters drawn from those nodes (arbitrary, possibly unbalanced).
fn cluster() -> impl Strategy<Value = (usize, Vec<Vec<NodeId>>)> {
    (R..=7usize).prop_flat_map(|n| {
        let nodes: Vec<NodeId> = (1..=n as u64).collect();
        let one_partition = proptest::sample::subsequence(nodes, R);
        proptest::collection::vec(one_partition, 1..=8).prop_map(move |parts| (n, parts))
    })
}

fn load(placements: &BTreeMap<u16, Placement>) -> BTreeMap<NodeId, usize> {
    let mut m = BTreeMap::new();
    for p in placements.values() {
        for &v in &p.voters {
            *m.entry(v).or_insert(0) += 1;
        }
    }
    m
}

fn partition_of(group: GroupId) -> u16 {
    match group {
        GroupId::Data(p) => p,
        GroupId::Meta => panic!("balancer proposed a meta move"),
    }
}

proptest! {
    #[test]
    fn proposals_change_exactly_one_voter((n, parts) in cluster()) {
        let dir = directory(n);
        let pl = placements(&parts);
        if let Some(proposal) = propose(&dir, &pl, R) {
            let part = partition_of(proposal.group);
            let before: BTreeSet<NodeId> = pl[&part].voters.iter().copied().collect();
            let after: BTreeSet<NodeId> = proposal.target_voters.iter().copied().collect();
            prop_assert_eq!(after.len(), R, "target must have R distinct voters");
            prop_assert_eq!(after.difference(&before).count(), 1);
            prop_assert_eq!(before.difference(&after).count(), 1);
        }
    }

    #[test]
    fn is_deterministic((n, parts) in cluster()) {
        let dir = directory(n);
        let pl = placements(&parts);
        prop_assert_eq!(propose(&dir, &pl, R), propose(&dir, &pl, R));
    }

    /// Repeatedly committing the balancer's proposals terminates and leaves
    /// replica counts within one slot across the eligible nodes.
    #[test]
    fn converges_to_near_even((n, parts) in cluster()) {
        let dir = directory(n);
        let mut pl = placements(&parts);

        let mut steps = 0;
        while let Some(proposal) = propose(&dir, &pl, R) {
            let part = partition_of(proposal.group);
            pl.get_mut(&part).unwrap().voters = proposal.target_voters;
            steps += 1;
            prop_assert!(steps < 1000, "balancer failed to converge");
        }

        // At the fixed point every node's load is within one of every other's.
        let load = load(&pl);
        let counts: Vec<usize> = (1..=n as u64).map(|id| load.get(&id).copied().unwrap_or(0)).collect();
        let max = *counts.iter().max().unwrap();
        let min = *counts.iter().min().unwrap();
        prop_assert!(max - min <= 1, "not near-even: counts {counts:?}");
    }
}
