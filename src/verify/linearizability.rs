//! An executable single-register linearizability checker (IMPLEMENTATION §M7,
//! DESIGN §12.7).
//!
//! Each key is an independent register, so per-key histories are small and a
//! Wing–Gong backtracking search is cheap. The checker looks for one sequential
//! order that (a) respects real time — if op A returned before op B was called,
//! A precedes B — and (b) is legal under the register semantics, including
//! version outcomes and CAS. It returns a witness order on success, or `None`
//! with enough to reproduce the failure.
//!
//! The register value carries a version (the committing Raft log index in the
//! real system), which is strictly monotonic across successful writes; the
//! checker enforces that, so a stale read or an out-of-order version is caught.

use crate::types::IfVersion;

/// A client-issued operation on one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invocation {
    Read,
    Put {
        value: Vec<u8>,
        if_version: Option<IfVersion>,
    },
}

/// The observed response. `Rejected` (protocol-level dedup/gap errors) carry no
/// register semantics and are filtered out before checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// A successful mutation committed at `version`.
    Applied { version: u64 },
    /// A CAS whose predicate failed; `present` is the current version, or `None`
    /// if the key was absent.
    ConditionFailed { present: Option<u64> },
    /// A read returning the current `(version, value)` or absence.
    Value(Option<(u64, Vec<u8>)>),
}

/// One completed operation with its real-time interval. `call`/`ret` are logical
/// timestamps from a monotonic counter shared across all clients.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op {
    pub call: u64,
    pub ret: u64,
    pub inv: Invocation,
    pub out: Outcome,
}

/// The register state: the last committed `(version, value)`, or `None`.
type State = Option<(u64, Vec<u8>)>;

fn version_of(state: &State) -> u64 {
    state.as_ref().map(|(v, _)| *v).unwrap_or(0)
}

/// Apply `op` to `state`, returning the resulting state if the recorded outcome
/// is consistent with the register semantics at this point, else `None`.
fn apply(state: &State, op: &Op) -> Option<State> {
    match (&op.inv, &op.out) {
        // A read must return exactly the current committed value.
        (Invocation::Read, Outcome::Value(v)) => {
            if v == state {
                Some(state.clone())
            } else {
                None
            }
        }
        // Unconditional put always applies; its version must advance.
        (
            Invocation::Put {
                value,
                if_version: None,
            },
            Outcome::Applied { version },
        ) => {
            if *version > version_of(state) {
                Some(Some((*version, value.clone())))
            } else {
                None
            }
        }
        // Create-only put.
        (
            Invocation::Put {
                value,
                if_version: Some(IfVersion::Absent),
            },
            out,
        ) => match (state, out) {
            (None, Outcome::Applied { version }) if *version > 0 => {
                Some(Some((*version, value.clone())))
            }
            (Some((v, _)), Outcome::ConditionFailed { present: Some(p) }) if p == v => {
                Some(state.clone())
            }
            _ => None,
        },
        // Numeric compare-and-set.
        (
            Invocation::Put {
                value,
                if_version: Some(IfVersion::Number(n)),
            },
            out,
        ) => match (state, out) {
            // Predicate holds: applies and advances.
            (Some((v, _)), Outcome::Applied { version }) if v == n && *version > *v => {
                Some(Some((*version, value.clone())))
            }
            // Predicate fails against a present key with a different version.
            (Some((v, _)), Outcome::ConditionFailed { present: Some(p) }) if p == v && v != n => {
                Some(state.clone())
            }
            // Predicate fails against an absent key.
            (None, Outcome::ConditionFailed { present: None }) => Some(state.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Search for a legal linearization respecting real time. Returns the witness
/// order (op indices into the input) if the history is linearizable.
pub fn linearize(ops: &[Op]) -> Option<Vec<usize>> {
    let mut used = vec![false; ops.len()];
    let mut order = Vec::with_capacity(ops.len());
    if search(ops, &mut used, &None, ops.len(), &mut order) {
        Some(order)
    } else {
        None
    }
}

/// Whether the history is linearizable.
pub fn is_linearizable(ops: &[Op]) -> bool {
    linearize(ops).is_some()
}

fn search(
    ops: &[Op],
    used: &mut [bool],
    state: &State,
    remaining: usize,
    order: &mut Vec<usize>,
) -> bool {
    if remaining == 0 {
        return true;
    }
    // The earliest return time among not-yet-linearized ops. An op may be placed
    // next only if it started no later than this — otherwise some op provably
    // completed before it began and must precede it (real-time order).
    let min_ret = ops
        .iter()
        .enumerate()
        .filter(|(i, _)| !used[*i])
        .map(|(_, o)| o.ret)
        .min()
        .unwrap();

    for i in 0..ops.len() {
        if used[i] || ops[i].call > min_ret {
            continue;
        }
        if let Some(next) = apply(state, &ops[i]) {
            used[i] = true;
            order.push(i);
            if search(ops, used, &next, remaining - 1, order) {
                return true;
            }
            order.pop();
            used[i] = false;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(call: u64, ret: u64, value: &[u8], version: u64) -> Op {
        Op {
            call,
            ret,
            inv: Invocation::Put {
                value: value.to_vec(),
                if_version: None,
            },
            out: Outcome::Applied { version },
        }
    }

    fn read(call: u64, ret: u64, value: Option<(u64, &[u8])>) -> Op {
        Op {
            call,
            ret,
            inv: Invocation::Read,
            out: Outcome::Value(value.map(|(v, b)| (v, b.to_vec()))),
        }
    }

    #[test]
    fn empty_history_is_linearizable() {
        assert!(is_linearizable(&[]));
    }

    #[test]
    fn sequential_write_then_read_is_linearizable() {
        let h = vec![put(0, 1, b"a", 10), read(2, 3, Some((10, b"a")))];
        assert!(is_linearizable(&h));
    }

    #[test]
    fn read_of_stale_value_after_write_returns_is_not_linearizable() {
        // Write of "a" returned at t=1; a later read (starts t=2) sees nothing.
        let h = vec![put(0, 1, b"a", 10), read(2, 3, None)];
        assert!(!is_linearizable(&h));
    }

    #[test]
    fn concurrent_reads_can_pick_either_side_of_a_write() {
        // Read overlaps the write: it may observe the old (absent) or new value.
        let old = vec![put(1, 3, b"a", 10), read(0, 2, None)];
        let new = vec![put(0, 2, b"a", 10), read(1, 3, Some((10, b"a")))];
        assert!(is_linearizable(&old));
        assert!(is_linearizable(&new));
    }

    #[test]
    fn versions_must_be_monotonic() {
        // Two writes whose versions decrease along real time cannot be ordered.
        let h = vec![put(0, 1, b"a", 20), put(2, 3, b"b", 10)];
        assert!(!is_linearizable(&h));
    }

    #[test]
    fn non_monotonic_reads_violate() {
        // A read sees version 20, then a strictly-later read sees version 10.
        let h = vec![
            put(0, 1, b"a", 10),
            put(2, 3, b"b", 20),
            read(4, 5, Some((20, b"b"))),
            read(6, 7, Some((10, b"a"))),
        ];
        assert!(!is_linearizable(&h));
    }

    #[test]
    fn cas_success_and_failure() {
        let cas_ok = Op {
            call: 2,
            ret: 3,
            inv: Invocation::Put {
                value: b"b".to_vec(),
                if_version: Some(IfVersion::Number(10)),
            },
            out: Outcome::Applied { version: 11 },
        };
        let h = vec![put(0, 1, b"a", 10), cas_ok, read(4, 5, Some((11, b"b")))];
        assert!(is_linearizable(&h));

        let cas_fail = Op {
            call: 2,
            ret: 3,
            inv: Invocation::Put {
                value: b"b".to_vec(),
                if_version: Some(IfVersion::Number(999)),
            },
            out: Outcome::ConditionFailed { present: Some(10) },
        };
        let h = vec![put(0, 1, b"a", 10), cas_fail, read(4, 5, Some((10, b"a")))];
        assert!(is_linearizable(&h));
    }

    #[test]
    fn create_only_second_attempt_fails() {
        let create = Op {
            call: 0,
            ret: 1,
            inv: Invocation::Put {
                value: b"a".to_vec(),
                if_version: Some(IfVersion::Absent),
            },
            out: Outcome::Applied { version: 10 },
        };
        let dup = Op {
            call: 2,
            ret: 3,
            inv: Invocation::Put {
                value: b"b".to_vec(),
                if_version: Some(IfVersion::Absent),
            },
            out: Outcome::ConditionFailed { present: Some(10) },
        };
        assert!(is_linearizable(&[create, dup]));
    }
}
