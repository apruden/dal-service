//! `RaftStateMachine` + `RaftSnapshotBuilder` wrapping the M2 data state
//! machine (DESIGN §6, M3).
//!
//! Business mutations, the applied `LogId`, and membership are written in one
//! atomic batch via [`Storage::apply_raft`], so recovery is a clean prefix.
//! Snapshots serialize the replicated state-CF records only; node-local
//! authority state lives in the default CF and is never copied or erased by a
//! snapshot (DESIGN §6).

// OpenRaft storage traits prescribe this error type, so these helpers cannot
// box it without immediately unboxing at the trait boundary.
#![allow(clippy::result_large_err)]

use std::io::Cursor;
use std::sync::{Arc, Mutex};

use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::StorageError;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;

use crate::codec;
use crate::partition::raft_types::{
    Entry, LogId, Node, NodeId, Snapshot, SnapshotData, SnapshotMeta, StoredMembership, TypeConfig,
    read_err, write_err,
};
use crate::partition::state_machine::{ApplyResult, DataStateMachine, StateOverlay};
use crate::perf::WriteStage;
use crate::storage::Storage;
use crate::types::GroupId;

/// Whether a Raft apply batch is committed with one fsync (coalesced) or one
/// fsync per entry (the original path). Coalescing is the default; set
/// `DAL_APPLY_COALESCE=0` to fall back to per-entry writes for an A/B
/// measurement against the benchmark. Read once and cached, so it never touches
/// the apply hot path after startup.
fn coalesce_apply() -> bool {
    static FLAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *FLAG.get_or_init(|| {
        !matches!(
            std::env::var("DAL_APPLY_COALESCE").ok().as_deref(),
            Some("0") | Some("false") | Some("off")
        )
    })
}

/// The persisted Raft applied-state: last applied log id and membership.
type Applied = (Option<LogId>, StoredMembership);

#[derive(Clone)]
pub struct RocksStateMachine {
    storage: Arc<Storage>,
    group: GroupId,
    data: DataStateMachine,
    /// OpenRaft builds snapshots on a task that runs concurrently with apply.
    /// Every clone shares this lock so snapshot metadata and its state-CF scan
    /// describe one applied prefix.
    state_view: Arc<Mutex<()>>,
}

impl RocksStateMachine {
    pub fn new(storage: Arc<Storage>, group: GroupId) -> Self {
        RocksStateMachine {
            storage,
            group,
            data: DataStateMachine::new(group),
            state_view: Arc::new(Mutex::new(())),
        }
    }

    fn read_applied(&self) -> Result<Applied, StorageError<NodeId>> {
        match self.storage.raft_applied(self.group).map_err(read_err)? {
            Some(bytes) => codec::decode(&bytes).map_err(read_err),
            None => Ok((None, StoredMembership::default())),
        }
    }

    /// Coalesced apply: decide every entry against a [`StateOverlay`] so later
    /// entries see earlier ones, then commit the whole batch in a single
    /// fsync-durable write with `last_applied` set to the final entry. Atomic and
    /// crash-safe: a crash before the write recovers to the previous
    /// `last_applied`, and Raft re-delivers the same contiguous, deterministic
    /// batch (idempotent via the per-client sequence records).
    fn apply_coalesced<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let (_, mut membership) = self.read_applied()?;
        let mut overlay = StateOverlay::new(&self.storage);
        let mut results = Vec::new();
        let mut last_log_id = None;

        for entry in entries {
            let log_id = entry.log_id;
            let (result, muts) = match entry.payload {
                EntryPayload::Blank => (ApplyResult::NoOp, Vec::new()),
                EntryPayload::Normal(req) => self
                    .data
                    .evaluate(&overlay, &req, log_id.index)
                    .map_err(|e| StorageError::from(openraft::StorageIOError::apply(log_id, &e)))?,
                EntryPayload::Membership(m) => {
                    membership = StoredMembership::new(Some(log_id), m);
                    (ApplyResult::NoOp, Vec::new())
                }
            };
            overlay.stage(&muts);
            results.push(result);
            last_log_id = Some(log_id);
        }

        // Persist once. Even a batch of only Blank/Membership entries (no state
        // mutations) must still advance the durable applied state, so the write
        // is keyed on having seen any entry, not on there being mutations.
        if let Some(log_id) = last_log_id {
            let applied: Applied = (Some(log_id), membership);
            self.storage
                .apply_raft(
                    self.group,
                    &overlay.into_mutations(),
                    &codec::encode(&applied),
                )
                .map_err(write_err)?;
        }
        Ok(results)
    }

    /// Original apply: one fsync-durable write per entry. Retained as the
    /// `DAL_APPLY_COALESCE=0` baseline for benchmarking the coalesced path.
    fn apply_per_entry<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let (_, mut membership) = self.read_applied()?;
        let mut results = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;
            let (result, muts) = match entry.payload {
                EntryPayload::Blank => (ApplyResult::NoOp, Vec::new()),
                EntryPayload::Normal(req) => self
                    .data
                    .evaluate(self.storage.as_ref(), &req, log_id.index)
                    .map_err(|e| StorageError::from(openraft::StorageIOError::apply(log_id, &e)))?,
                EntryPayload::Membership(m) => {
                    membership = StoredMembership::new(Some(log_id), m);
                    (ApplyResult::NoOp, Vec::new())
                }
            };

            let applied: Applied = (Some(log_id), membership.clone());
            self.storage
                .apply_raft(self.group, &muts, &codec::encode(&applied))
                .map_err(write_err)?;
            results.push(result);
        }
        Ok(results)
    }

    fn build_snapshot_now(&self) -> Result<Snapshot, StorageError<NodeId>> {
        let _view = self.state_view.lock().unwrap();
        let (last_log_id, membership) = self.read_applied()?;
        let pairs = self.storage.scan_state(self.group).map_err(read_err)?;
        let data = codec::encode(&pairs);
        let snapshot_id = match &last_log_id {
            Some(l) => format!("{l}"),
            None => "empty".to_string(),
        };
        let meta = SnapshotMeta {
            last_log_id,
            last_membership: membership,
            snapshot_id,
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RocksStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError<NodeId>> {
        self.build_snapshot_now()
    }
}

impl RaftStateMachine<TypeConfig> for RocksStateMachine {
    type SnapshotBuilder = RocksStateMachine;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembership), StorageError<NodeId>> {
        self.read_applied()
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let _profile = crate::perf::timer(WriteStage::StateApplyTotal);
        // Clone the shared lock handle before acquiring it so the guard does
        // not keep an immutable field borrow of `self` while the apply helper
        // mutably borrows the state-machine wrapper.
        let state_view = self.state_view.clone();
        let _view = state_view.lock().unwrap();
        if coalesce_apply() {
            self.apply_coalesced(entries)
        } else {
            self.apply_per_entry(entries)
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let _view = self.state_view.lock().unwrap();
        let bytes = (*snapshot).into_inner();
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = codec::decode(&bytes).map_err(read_err)?;
        let applied: Applied = (meta.last_log_id, meta.last_membership.clone());
        self.storage
            .install_state(self.group, &pairs, &codec::encode(&applied))
            .map_err(write_err)?;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError<NodeId>> {
        let (last_log_id, _) = self.read_applied()?;
        if last_log_id.is_none() {
            return Ok(None);
        }
        Ok(Some(self.build_snapshot_now()?))
    }
}

// Silence unused-import lint for `Node` (referenced only via type aliases).
const _: fn() = || {
    let _: Option<Node> = None;
};

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, EntryPayload};

    use crate::types::{DataOp, DataRequest, IfVersion, MutationResult};

    fn log_id(index: u64) -> LogId {
        LogId::new(CommittedLeaderId::new(1, 1), index)
    }

    fn entry(index: u64, sequence: u64, op: DataOp) -> Entry {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(DataRequest {
                client_id: 7,
                sequence,
                op,
            }),
        }
    }

    #[test]
    fn coalesced_batch_observes_same_client_and_same_key_predecessors() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).unwrap());
        let group = GroupId::Data(0);
        storage.ensure_group(group).unwrap();
        let mut sm = RocksStateMachine::new(storage.clone(), group);

        let results = sm
            .apply_coalesced(vec![
                entry(
                    10,
                    1,
                    DataOp::Put {
                        key: b"k".to_vec(),
                        value: b"v1".to_vec(),
                        if_version: None,
                    },
                ),
                entry(
                    11,
                    2,
                    DataOp::Put {
                        key: b"k".to_vec(),
                        value: b"v2".to_vec(),
                        if_version: Some(IfVersion::Number(10)),
                    },
                ),
                entry(
                    12,
                    3,
                    DataOp::Delete {
                        key: b"k".to_vec(),
                        if_version: Some(IfVersion::Number(11)),
                    },
                ),
            ])
            .unwrap();

        assert_eq!(
            results,
            vec![
                ApplyResult::Decided(MutationResult::Applied { version: 10 }),
                ApplyResult::Decided(MutationResult::Applied { version: 11 }),
                ApplyResult::Decided(MutationResult::Applied { version: 12 }),
            ]
        );
        assert_eq!(
            DataStateMachine::new(group)
                .get(storage.as_ref(), b"k")
                .unwrap(),
            None
        );
        assert_eq!(sm.read_applied().unwrap().0, Some(log_id(12)));
    }
}
