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
use std::sync::Arc;

use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::StorageError;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use tokio::sync::Mutex;

use crate::codec;
use crate::partition::raft_types::{
    Entry, LogId, Node, NodeId, Snapshot, SnapshotData, SnapshotMeta, StoredMembership, TypeConfig,
    read_err, write_err,
};
use crate::partition::state_machine::{ApplyResult, DataStateMachine, StateOverlay};
use crate::perf::WriteStage;
use crate::storage::{MAX_PENDING_STATE_BYTES, MAX_PENDING_STATE_ENTRIES, Storage};
use crate::types::GroupId;

/// Keep normal coalesced chunks comfortably below the final RocksDB batch
/// limits. The final storage boundary still measures and rejects an oversized
/// batch, so unusual membership records or encoding growth cannot bypass the
/// hard limit.
const APPLY_CHUNK_INPUT_BYTES: usize = MAX_PENDING_STATE_BYTES / 2;
const APPLY_CHUNK_ENTRIES: usize = MAX_PENDING_STATE_ENTRIES / 2;

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
        self.storage.require_healthy().map_err(read_err)?;
        match self.storage.raft_applied(self.group).map_err(read_err)? {
            Some(bytes) => codec::decode(&bytes).map_err(read_err),
            None => Ok((None, StoredMembership::default())),
        }
    }

    /// Coalesced apply: decide bounded chunks against a [`StateOverlay`] so
    /// later entries see earlier ones, then atomically commit each chunk with
    /// `last_applied` set to its final entry. A crash between chunks recovers a
    /// clean prefix, and Raft re-delivers the remaining contiguous,
    /// deterministic entries (idempotent via the per-client sequence records).
    async fn apply_coalesced<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ApplyResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut all_results = Vec::new();
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0usize;

        for entry in entries {
            let entry_bytes = codec::encode(&entry).len();
            let would_exceed = !chunk.is_empty()
                && (chunk.len() >= APPLY_CHUNK_ENTRIES
                    || chunk_bytes.saturating_add(entry_bytes) > APPLY_CHUNK_INPUT_BYTES);
            if would_exceed {
                all_results.extend(
                    self.apply_coalesced_chunk(std::mem::take(&mut chunk))
                        .await?,
                );
                chunk_bytes = 0;
            }
            chunk_bytes = chunk_bytes.saturating_add(entry_bytes);
            chunk.push(entry);
        }

        if !chunk.is_empty() {
            all_results.extend(self.apply_coalesced_chunk(chunk).await?);
        }
        Ok(all_results)
    }

    /// Apply one bounded coalesced chunk. A multi-chunk call remains crash-safe:
    /// every chunk advances the atomic applied record only after becoming
    /// visible, so restart observes a prefix and OpenRaft re-delivers the rest.
    async fn apply_coalesced_chunk(
        &mut self,
        entries: Vec<Entry>,
    ) -> Result<Vec<ApplyResult>, StorageError<NodeId>> {
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
        // mutations) must still advance the applied state, so the write is
        // keyed on having seen any entry, not on there being mutations.
        if let Some(log_id) = last_log_id {
            let applied: Applied = (Some(log_id), membership);
            let committed = codec::encode(&Some(log_id));
            let mutations = overlay.into_mutations();
            self.storage
                .apply_raft(
                    self.group,
                    &mutations,
                    log_id,
                    &codec::encode(&applied),
                    &committed,
                    results.len(),
                )
                .await
                .map_err(write_err)?;
        }
        Ok(results)
    }

    /// Original apply: one atomic write per entry. Retained as the
    /// `DAL_APPLY_COALESCE=0` baseline for benchmarking the coalesced path.
    async fn apply_per_entry<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ApplyResult>, StorageError<NodeId>>
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
            let committed = codec::encode(&Some(log_id));
            self.storage
                .apply_raft(
                    self.group,
                    &muts,
                    log_id,
                    &codec::encode(&applied),
                    &committed,
                    1,
                )
                .await
                .map_err(write_err)?;
            results.push(result);
        }
        Ok(results)
    }

    async fn build_snapshot_now(&self) -> Result<Snapshot, StorageError<NodeId>> {
        let _view = self.state_view.lock().await;
        let (last_log_id, membership) = self.read_applied()?;
        if let Some(log_id) = &last_log_id {
            self.storage
                .wait_state_durable_for_snapshot(self.group, log_id)
                .await
                .map_err(write_err)?;
        }
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
        self.build_snapshot_now().await
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
        let _view = state_view.lock().await;
        if coalesce_apply() {
            self.apply_coalesced(entries).await
        } else {
            self.apply_per_entry(entries).await
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotData>, StorageError<NodeId>> {
        self.storage.require_healthy().map_err(write_err)?;
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let _view = self.state_view.lock().await;
        let bytes = (*snapshot).into_inner();
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = codec::decode(&bytes).map_err(read_err)?;
        self.storage
            .validate_state_install(self.group, meta.last_log_id.as_ref())
            .map_err(write_err)?;
        let applied: Applied = (meta.last_log_id, meta.last_membership.clone());
        self.storage
            .install_state(self.group, &pairs, &codec::encode(&applied))
            .map_err(write_err)?;
        self.storage
            .record_state_installed(self.group, meta.last_log_id);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError<NodeId>> {
        let (last_log_id, _) = self.read_applied()?;
        if last_log_id.is_none() {
            return Ok(None);
        }
        Ok(Some(self.build_snapshot_now().await?))
    }
}

// Silence unused-import lint for `Node` (referenced only via type aliases).
const _: fn() = || {
    let _: Option<Node> = None;
};

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::RaftLogStorage;
    use openraft::{CommittedLeaderId, EntryPayload};
    use std::time::Duration;

    use crate::partition::log_store::RocksLogStore;
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

    #[tokio::test]
    async fn coalesced_batch_observes_same_client_and_same_key_predecessors() {
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
            .await
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
        let mut log_store = RocksLogStore::new(storage, group);
        let committed =
            <RocksLogStore as RaftLogStorage<TypeConfig>>::read_committed(&mut log_store)
                .await
                .unwrap();
        assert_eq!(committed, Some(log_id(12)));
    }

    #[tokio::test]
    async fn coalesced_apply_chunks_input_larger_than_the_dirty_entry_budget() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).unwrap());
        let group = GroupId::Data(4);
        storage.ensure_group(group).unwrap();
        let mut sm = RocksStateMachine::new(storage, group);
        let last_index = MAX_PENDING_STATE_ENTRIES as u64 + 1;
        let entries = (1..=last_index).map(|index| Entry {
            log_id: log_id(index),
            payload: EntryPayload::Blank,
        });

        let results = sm.apply_coalesced(entries).await.unwrap();

        assert_eq!(results.len(), last_index as usize);
        assert!(results.iter().all(|result| *result == ApplyResult::NoOp));
        assert_eq!(sm.read_applied().unwrap().0, Some(log_id(last_index)));
    }

    #[tokio::test]
    async fn snapshot_waits_until_its_visible_applied_prefix_is_durable() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            Arc::new(Storage::open_delayed_test(dir.path(), Duration::from_millis(200)).unwrap());
        let group = GroupId::Data(5);
        storage.ensure_group(group).unwrap();
        let mut sm = RocksStateMachine::new(storage.clone(), group);
        sm.apply_coalesced(vec![entry(
            10,
            1,
            DataOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                if_version: None,
            },
        )])
        .await
        .unwrap();
        assert_eq!(storage.state_durability(group).durable, None);

        let mut builder = sm.clone();
        let snapshot = tokio::spawn(async move { builder.build_snapshot().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!snapshot.is_finished());

        let snapshot = snapshot.await.unwrap().unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(log_id(10)));
        let materialized = storage.state_durability(group);
        assert_eq!(materialized.durable, Some(log_id(10)));
        assert_eq!(materialized.snapshot_waits, 1);
        assert!(materialized.snapshot_wait_ms >= 100);
    }

    #[tokio::test]
    async fn snapshot_transfer_from_a_dirty_source_installs_a_durable_fenced_prefix() {
        let source_dir = tempfile::tempdir().unwrap();
        let source = Arc::new(
            Storage::open_delayed_test(source_dir.path(), Duration::from_millis(150)).unwrap(),
        );
        let group = GroupId::Data(6);
        source.ensure_group(group).unwrap();
        let mut source_sm = RocksStateMachine::new(source.clone(), group);
        source_sm
            .apply_coalesced(vec![entry(
                20,
                1,
                DataOp::Put {
                    key: b"snapshot-key".to_vec(),
                    value: b"snapshot-value".to_vec(),
                    if_version: None,
                },
            )])
            .await
            .unwrap();
        let before = source.state_durability(group);
        assert!(before.visible > before.durable);

        let mut builder = source_sm.clone();
        let snapshot = builder.build_snapshot().await.unwrap();
        assert_eq!(source.state_durability(group).durable, Some(log_id(20)));

        let target_dir = tempfile::tempdir().unwrap();
        let target = Arc::new(
            Storage::open_delayed_test(target_dir.path(), Duration::from_millis(150)).unwrap(),
        );
        target.ensure_group(group).unwrap();
        let mut target_sm = RocksStateMachine::new(target.clone(), group);
        target_sm
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();

        assert_eq!(
            DataStateMachine::new(group)
                .get(target.as_ref(), b"snapshot-key")
                .unwrap(),
            Some((20, b"snapshot-value".to_vec()))
        );
        let installed = target.state_durability(group);
        assert_eq!(installed.visible, Some(log_id(20)));
        assert_eq!(installed.durable, Some(log_id(20)));
        assert!(
            !installed.recovery_ready,
            "snapshot installation must still require a current leader fence"
        );
    }
}
