//! The data-group state machine: `put`/`delete`, numeric and create-only CAS,
//! and durable per-client idempotency records (DESIGN §4.2, §8.4).
//!
//! This layer is pure with respect to committed input: given `(command,
//! log_index)` it produces the same result on every replica. No clocks, no
//! randomness, no node-local state (IMPLEMENTATION ground rule 1). Raft enters
//! at M3; here the state machine is exercised directly.

use std::collections::HashMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::codec;
use crate::error::Result;
use crate::keyspace;
use crate::storage::{StateMutation, Storage};
use crate::types::{
    ClientId, DataOp, DataRequest, GroupId, IfVersion, KeyPresence, MutationResult, Sequence,
    Version,
};

/// A read seam over a group's state CF. `evaluate` decides an entry purely from
/// these reads, so the same logic serves two callers: the committed CF directly
/// ([`Storage`]), or a [`StateOverlay`] that layers not-yet-durable mutations
/// from earlier entries in the same apply batch on top of it. The two are
/// observationally identical, which is what lets the Raft applier coalesce a
/// batch into one fsync without changing any decision.
pub trait StateRead {
    fn read_state<T: DeserializeOwned>(&self, group: GroupId, key: &[u8]) -> Result<Option<T>>;
}

impl StateRead for Storage {
    fn read_state<T: DeserializeOwned>(&self, group: GroupId, key: &[u8]) -> Result<Option<T>> {
        self.get_state_record(group, key)
    }
}

/// An in-memory overlay of pending state-CF writes for one group, layered over
/// the committed CF for the duration of a single Raft apply batch.
///
/// Writing each committed entry with its own fsync is correct but pays one fsync
/// per entry. Coalescing the whole batch into one durable write requires that a
/// later entry still observe an earlier entry's effects (a CAS chain on one key,
/// or two ops on one client's sequence stream). This overlay provides exactly
/// that: staged mutations are visible to subsequent [`StateRead`] calls, while
/// the actual durable write happens once at the end of the batch. Reads fall
/// through to the committed CF for any key the batch has not touched.
pub struct StateOverlay<'a> {
    storage: &'a Storage,
    pending: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl<'a> StateOverlay<'a> {
    pub fn new(storage: &'a Storage) -> StateOverlay<'a> {
        StateOverlay {
            storage,
            pending: HashMap::new(),
        }
    }

    /// Fold an entry's decided mutations into the overlay so later entries in the
    /// same batch read them. Last write to a key wins, matching the order-in-time
    /// semantics a per-entry commit would produce.
    pub fn stage(&mut self, mutations: &[StateMutation]) {
        for m in mutations {
            match m {
                StateMutation::Put { key, value } => {
                    self.pending.insert(key.clone(), Some(value.clone()));
                }
                StateMutation::Delete { key } => {
                    self.pending.insert(key.clone(), None);
                }
            }
        }
    }

    /// Collapse the batch into one mutation per touched key. Distinct keys in a
    /// `WriteBatch` are order-independent, so the deduped set is equivalent to
    /// replaying every staged mutation in order.
    pub fn into_mutations(self) -> Vec<StateMutation> {
        self.pending
            .into_iter()
            .map(|(key, value)| match value {
                Some(value) => StateMutation::Put { key, value },
                None => StateMutation::Delete { key },
            })
            .collect()
    }
}

impl StateRead for StateOverlay<'_> {
    fn read_state<T: DeserializeOwned>(&self, group: GroupId, key: &[u8]) -> Result<Option<T>> {
        match self.pending.get(key) {
            Some(Some(bytes)) => Ok(Some(codec::decode(bytes)?)),
            Some(None) => Ok(None),
            None => self.storage.get_state_record(group, key),
        }
    }
}

/// One key's durable record: its value and the log index of its last mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct KeyRecord {
    version: Version,
    value: Vec<u8>,
}

/// Per-client idempotency record (DESIGN §8.4): the highest *decided* sequence,
/// a digest of that command's canonical bytes, and its stored result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SeqRecord {
    highest: Sequence,
    /// 128-bit xxh3 of the canonical command bytes. Values reach 16 MiB, so raw
    /// bytes are not retained; collision is negligible under the non-Byzantine
    /// model (IMPLEMENTATION §M2).
    digest: u128,
    result: MutationResult,
}

/// The outcome of applying one committed entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyResult {
    /// `sequence == highest + 1`: freshly decided (applied or CAS-failed).
    Decided(MutationResult),
    /// `sequence == highest` with a matching digest: the stored result is
    /// returned without reapplying.
    Replayed(MutationResult),
    /// A deterministic non-mutation: only `last_applied` advanced, the client
    /// sequence record is untouched (DESIGN §8.4).
    Rejected(RejectReason),
    /// A non-client entry (Raft blank or membership) that carries no business
    /// result but still advanced `last_applied` (M3).
    NoOp,
}

impl ApplyResult {
    /// The client-visible mutation result, if this entry produced one.
    pub fn mutation(&self) -> Option<MutationResult> {
        match self {
            ApplyResult::Decided(r) | ApplyResult::Replayed(r) => Some(*r),
            ApplyResult::Rejected(_) | ApplyResult::NoOp => None,
        }
    }
}

/// Why a committed entry was rejected without mutating state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    /// `sequence > highest + 1`: a hole in the client's stream.
    SequenceGap { expected: Sequence, got: Sequence },
    /// `sequence < highest`, or `== highest` with no stored result.
    StaleSequence { highest: Sequence, got: Sequence },
    /// Idempotency key reused for a *different* command (DESIGN §8.4).
    SequenceMismatch,
    /// Structurally invalid command that slipped past the API gate, e.g. a
    /// `delete` carrying the `ABSENT` sentinel (DESIGN §4.2).
    Malformed,
    /// The client exhausted the `u64` sequence space. Further commands are
    /// refused deterministically instead of overflowing during Raft apply.
    SequenceExhausted,
}

/// Compute the idempotency digest over a command's canonical bytes. Taken over
/// the operation only (not the client/sequence framing), so a retry at the same
/// sequence must carry an identical operation to match.
fn digest(op: &DataOp) -> u128 {
    xxhash_rust::xxh3::xxh3_128(&codec::encode(op))
}

#[derive(Clone)]
pub struct DataStateMachine {
    group: GroupId,
}

impl DataStateMachine {
    pub fn new(group: GroupId) -> Self {
        DataStateMachine { group }
    }

    pub fn group(&self) -> GroupId {
        self.group
    }

    fn key_record<R: StateRead>(&self, reader: &R, key: &[u8]) -> Result<Option<KeyRecord>> {
        reader.read_state(self.group, &keyspace::user_key(key))
    }

    fn seq_record<R: StateRead>(
        &self,
        reader: &R,
        client_id: ClientId,
    ) -> Result<Option<SeqRecord>> {
        reader.read_state(self.group, &keyspace::seq_key(client_id))
    }

    /// Public linearizable-read helper: the current value/version of a key.
    pub fn get<R: StateRead>(&self, reader: &R, key: &[u8]) -> Result<Option<(Version, Vec<u8>)>> {
        Ok(self.key_record(reader, key)?.map(|r| (r.version, r.value)))
    }

    /// Apply one committed entry at `log_index`, writing atomically. Used by the
    /// pre-Raft (M2) tests; the Raft wrapper (M3) uses [`Self::evaluate`] so it
    /// can fold membership state into the same batch. Never returns a logical
    /// error for a bad command — every committed entry advances `last_applied`
    /// so Raft can never wedge (DESIGN §4.2). `Err` is reserved for storage
    /// faults.
    pub fn apply(
        &self,
        storage: &Storage,
        req: &DataRequest,
        log_index: u64,
    ) -> Result<ApplyResult> {
        let (result, muts) = self.evaluate(storage, req, log_index)?;
        storage.apply_state(self.group, &muts, log_id(log_index))?;
        Ok(result)
    }

    /// Decide the outcome of a committed entry and return the state-CF
    /// mutations it implies — without writing anything. The caller is
    /// responsible for the atomic write (business mutations + `last_applied`,
    /// plus, for a Raft group, membership). Replay and reject paths mutate
    /// nothing and return an empty mutation list.
    pub fn evaluate<R: StateRead>(
        &self,
        reader: &R,
        req: &DataRequest,
        log_index: u64,
    ) -> Result<(ApplyResult, Vec<StateMutation>)> {
        // Structural validation of commands the API layer should have caught.
        if let DataOp::Delete {
            if_version: Some(IfVersion::Absent),
            ..
        } = req.op
        {
            return Ok((ApplyResult::Rejected(RejectReason::Malformed), vec![]));
        }

        let seq = self.seq_record(reader, req.client_id)?;
        let highest = seq.as_ref().map(|s| s.highest).unwrap_or(0);

        // Sequence gate (DESIGN §8.4).
        if req.sequence == highest {
            return Ok(match seq {
                Some(s) if s.digest == digest(&req.op) => (ApplyResult::Replayed(s.result), vec![]),
                Some(_) => (
                    ApplyResult::Rejected(RejectReason::SequenceMismatch),
                    vec![],
                ),
                // `highest == 0` with no record: sequence 0 was never decided.
                None => (
                    ApplyResult::Rejected(RejectReason::StaleSequence {
                        highest,
                        got: req.sequence,
                    }),
                    vec![],
                ),
            });
        }
        if req.sequence < highest {
            return Ok((
                ApplyResult::Rejected(RejectReason::StaleSequence {
                    highest,
                    got: req.sequence,
                }),
                vec![],
            ));
        }
        let Some(expected) = highest.checked_add(1) else {
            return Ok((
                ApplyResult::Rejected(RejectReason::SequenceExhausted),
                vec![],
            ));
        };
        if req.sequence > expected {
            return Ok((
                ApplyResult::Rejected(RejectReason::SequenceGap {
                    expected,
                    got: req.sequence,
                }),
                vec![],
            ));
        }

        // sequence == highest + 1: decide.
        let (result, key_mutation) = self.decide(reader, &req.op, log_index)?;
        let seq_record = SeqRecord {
            highest: req.sequence,
            digest: digest(&req.op),
            result,
        };

        let mut muts = Vec::new();
        if let Some(m) = key_mutation {
            muts.push(m);
        }
        muts.push(StateMutation::Put {
            key: keyspace::seq_key(req.client_id),
            value: codec::encode(&seq_record),
        });
        Ok((ApplyResult::Decided(result), muts))
    }

    /// Evaluate CAS and produce the decided result plus any key mutation.
    fn decide<R: StateRead>(
        &self,
        reader: &R,
        op: &DataOp,
        log_index: u64,
    ) -> Result<(MutationResult, Option<StateMutation>)> {
        let current = self.key_record(reader, op.key())?;
        let presence = match &current {
            Some(r) => KeyPresence::Present { version: r.version },
            None => KeyPresence::Absent,
        };

        match op {
            DataOp::Put {
                key,
                value,
                if_version,
            } => {
                let ok = match if_version {
                    None => true,
                    Some(IfVersion::Number(v)) => matches!(&current, Some(r) if r.version == *v),
                    Some(IfVersion::Absent) => current.is_none(),
                };
                if !ok {
                    return Ok((MutationResult::ConditionFailed { current: presence }, None));
                }
                let record = KeyRecord {
                    version: log_index,
                    value: value.clone(),
                };
                Ok((
                    MutationResult::Applied { version: log_index },
                    Some(StateMutation::Put {
                        key: keyspace::user_key(key),
                        value: codec::encode(&record),
                    }),
                ))
            }
            DataOp::Delete { key, if_version } => {
                let ok = match if_version {
                    None => true,
                    Some(IfVersion::Number(v)) => matches!(&current, Some(r) if r.version == *v),
                    // Absent sentinel on delete is rejected earlier as Malformed.
                    Some(IfVersion::Absent) => false,
                };
                if !ok {
                    return Ok((MutationResult::ConditionFailed { current: presence }, None));
                }
                // Unconditional delete of an absent key is still Applied and
                // returns this index as its version, creating no tombstone.
                let mutation = current.is_some().then(|| StateMutation::Delete {
                    key: keyspace::user_key(key),
                });
                Ok((MutationResult::Applied { version: log_index }, mutation))
            }
        }
    }
}

/// Standalone (pre-Raft) log ids carry a synthetic term of 0; M3 supplies real
/// terms from openraft. Only the index is load-bearing for `last_applied`.
fn log_id(index: u64) -> crate::types::LogId {
    crate::types::LogId::new(0, index)
}
