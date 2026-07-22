//! The data-group state machine: `put`/`delete`, numeric and create-only CAS,
//! and durable per-client idempotency records (DESIGN §4.2, §8.4).
//!
//! This layer is pure with respect to committed input: given `(command,
//! log_index)` it produces the same result on every replica. No clocks, no
//! randomness, no node-local state (IMPLEMENTATION ground rule 1). Raft enters
//! at M3; here the state machine is exercised directly.

use serde::{Deserialize, Serialize};

use crate::codec;
use crate::error::Result;
use crate::keyspace;
use crate::storage::{StateMutation, Storage};
use crate::types::{
    ClientId, DataOp, DataRequest, GroupId, IfVersion, KeyPresence, MutationResult, Sequence,
    Version,
};

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

    fn key_record(&self, storage: &Storage, key: &[u8]) -> Result<Option<KeyRecord>> {
        storage.get_state_record(self.group, &keyspace::user_key(key))
    }

    fn seq_record(&self, storage: &Storage, client_id: ClientId) -> Result<Option<SeqRecord>> {
        storage.get_state_record(self.group, &keyspace::seq_key(client_id))
    }

    /// Public linearizable-read helper: the current value/version of a key.
    pub fn get(&self, storage: &Storage, key: &[u8]) -> Result<Option<(Version, Vec<u8>)>> {
        Ok(self
            .key_record(storage, key)?
            .map(|r| (r.version, r.value)))
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
    pub fn evaluate(
        &self,
        storage: &Storage,
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

        let seq = self.seq_record(storage, req.client_id)?;
        let highest = seq.as_ref().map(|s| s.highest).unwrap_or(0);

        // Sequence gate (DESIGN §8.4).
        if req.sequence == highest {
            return Ok(match seq {
                Some(s) if s.digest == digest(&req.op) => (ApplyResult::Replayed(s.result), vec![]),
                Some(_) => (ApplyResult::Rejected(RejectReason::SequenceMismatch), vec![]),
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
        if req.sequence > highest + 1 {
            return Ok((
                ApplyResult::Rejected(RejectReason::SequenceGap {
                    expected: highest + 1,
                    got: req.sequence,
                }),
                vec![],
            ));
        }

        // sequence == highest + 1: decide.
        let (result, key_mutation) = self.decide(storage, &req.op, log_index)?;
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
    fn decide(
        &self,
        storage: &Storage,
        op: &DataOp,
        log_index: u64,
    ) -> Result<(MutationResult, Option<StateMutation>)> {
        let current = self.key_record(storage, op.key())?;
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
                let mutation = current
                    .is_some()
                    .then(|| StateMutation::Delete {
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
