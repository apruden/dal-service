//! Whole-run oracles beyond per-key linearizability (IMPLEMENTATION §M7).
//!
//! These are pure checks over data the harness records: exactly-once
//! application, placement convergence, and no acknowledged write lost. Each
//! returns `Ok(())` or a description of the first violation.

use std::collections::{BTreeSet, HashMap};

use crate::types::NodeId;

/// One applied command as observed by the exactly-once oracle.
#[derive(Debug, Clone)]
pub struct Applied {
    pub client_id: u128,
    pub partition: u16,
    pub sequence: u64,
    /// Digest of the canonical command bytes, so a duplicate response can be
    /// confirmed to be for a byte-identical command (DESIGN §8.4).
    pub digest: u128,
}

/// Exactly-once: every `(client_id, partition, sequence)` is applied at most
/// once, and any repeat carries the identical command digest (a replayed
/// response for the same bytes, never a different command reusing the key).
pub fn exactly_once(applied: &[Applied]) -> Result<(), String> {
    let mut seen: HashMap<(u128, u16, u64), u128> = HashMap::new();
    for a in applied {
        let key = (a.client_id, a.partition, a.sequence);
        match seen.get(&key) {
            Some(&digest) if digest != a.digest => {
                return Err(format!(
                    "sequence {key:?} applied for two different commands \
                     ({digest:#x} vs {:#x})",
                    a.digest
                ));
            }
            _ => {
                seen.insert(key, a.digest);
            }
        }
    }
    Ok(())
}

/// One partition's convergence facts, sampled at quiescence.
#[derive(Debug, Clone)]
pub struct PartitionState {
    pub partition: u16,
    /// The meta group's recorded voter set.
    pub meta_voters: BTreeSet<NodeId>,
    /// The data group's committed voter set.
    pub data_voters: BTreeSet<NodeId>,
    /// Whether a move plan is still present, and if so whether it is aborting.
    pub plan: Option<bool>,
}

/// Convergence: at quiescence, every partition's meta voter set equals its
/// data-Raft committed voter set, and no partition is left with a non-aborting
/// stale plan (DESIGN §12.7).
pub fn converged(states: &[PartitionState]) -> Result<(), String> {
    for s in states {
        if s.meta_voters != s.data_voters {
            return Err(format!(
                "partition {} diverged: meta {:?} != data {:?}",
                s.partition, s.meta_voters, s.data_voters
            ));
        }
        if let Some(aborting) = s.plan {
            if !aborting {
                return Err(format!(
                    "partition {} left with a live non-aborting plan",
                    s.partition
                ));
            }
        }
    }
    Ok(())
}

/// No acknowledged write is lost: every value the cluster acknowledged as
/// applied is still present in the final observed state at its version or a
/// later one (a subsequent overwrite is fine; a disappearance is not).
pub fn no_lost_write(
    acknowledged: &[(Vec<u8>, u64)],
    final_state: &HashMap<Vec<u8>, u64>,
) -> Result<(), String> {
    for (key, acked_version) in acknowledged {
        match final_state.get(key) {
            Some(&final_version) if final_version >= *acked_version => {}
            Some(&final_version) => {
                return Err(format!(
                    "key {key:?} regressed: acked version {acked_version}, final {final_version}"
                ));
            }
            None => {
                return Err(format!(
                    "key {key:?} acked at version {acked_version} but absent at end"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exactly_once_accepts_identical_replays() {
        let a = vec![
            Applied { client_id: 1, partition: 0, sequence: 1, digest: 0xAA },
            Applied { client_id: 1, partition: 0, sequence: 1, digest: 0xAA }, // replay
        ];
        assert!(exactly_once(&a).is_ok());
    }

    #[test]
    fn exactly_once_rejects_reused_sequence_for_different_command() {
        let a = vec![
            Applied { client_id: 1, partition: 0, sequence: 1, digest: 0xAA },
            Applied { client_id: 1, partition: 0, sequence: 1, digest: 0xBB },
        ];
        assert!(exactly_once(&a).is_err());
    }

    #[test]
    fn converged_detects_divergence_and_stale_plans() {
        let ok = PartitionState {
            partition: 0,
            meta_voters: [1, 2, 3].into_iter().collect(),
            data_voters: [1, 2, 3].into_iter().collect(),
            plan: None,
        };
        assert!(converged(&[ok]).is_ok());

        let diverged = PartitionState {
            partition: 1,
            meta_voters: [1, 2, 3].into_iter().collect(),
            data_voters: [1, 2, 4].into_iter().collect(),
            plan: None,
        };
        assert!(converged(&[diverged]).is_err());

        let stale_plan = PartitionState {
            partition: 2,
            meta_voters: [1, 2, 3].into_iter().collect(),
            data_voters: [1, 2, 3].into_iter().collect(),
            plan: Some(false),
        };
        assert!(converged(&[stale_plan]).is_err());
    }

    #[test]
    fn no_lost_write_flags_disappearance_and_regression() {
        let acked = vec![(b"k".to_vec(), 10u64)];
        let mut good = HashMap::new();
        good.insert(b"k".to_vec(), 12u64); // overwritten later — fine
        assert!(no_lost_write(&acked, &good).is_ok());

        let mut gone = HashMap::new();
        gone.insert(b"other".to_vec(), 5u64);
        assert!(no_lost_write(&acked, &gone).is_err());

        let mut regressed = HashMap::new();
        regressed.insert(b"k".to_vec(), 5u64);
        assert!(no_lost_write(&acked, &regressed).is_err());
    }
}
