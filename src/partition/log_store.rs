//! `RaftLogStorage` + `RaftLogReader` over `cf_log_<group>` (DESIGN §6, M3).
//!
//! Every append and vote save is fsync-durable before it is acknowledged; the
//! leader only counts durable follower replies toward commit (DESIGN §2). The
//! optional committed-position marker is folded into the same atomic,
//! durable state-machine batch as the applied pointer.
//!
//! The store is generic over the openraft type config `C` (blanket impls below),
//! so the data groups and the meta group (M5) share one log implementation —
//! only the command/response types they carry differ. `NodeId`/`Node`/entry
//! encoding are identical for every group.

// OpenRaft storage traits prescribe this error type, so these helpers cannot
// box it without immediately unboxing at the trait boundary.
#![allow(clippy::result_large_err)]

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::RaftTypeConfig;
use openraft::StorageError;
use openraft::storage::LogFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};

use crate::codec;
use crate::partition::raft_types::{read_err, write_err};
use crate::perf::WriteStage;
use crate::storage::Storage;
use crate::types::GroupId;

/// The one node-id type every group shares.
type Nid = u64;
type LogId = openraft::LogId<Nid>;
type Vote = openraft::Vote<Nid>;

const KEY_VOTE: [u8; 2] = [0x00, b'v'];
const KEY_PURGED: [u8; 2] = [0x00, b'p'];
pub(crate) const KEY_COMMITTED: [u8; 2] = [0x00, b'c'];
const ENTRY_PREFIX: u8 = 0x01;
/// One past the entry prefix, used to seek the reverse iterator to the end.
const ENTRY_PREFIX_END: u8 = 0x02;

fn entry_key(index: u64) -> [u8; 9] {
    let mut k = [0u8; 9];
    k[0] = ENTRY_PREFIX;
    k[1..].copy_from_slice(&index.to_be_bytes());
    k
}

fn entry_index(key: &[u8]) -> Option<u64> {
    if key.len() == 9 && key[0] == ENTRY_PREFIX {
        Some(u64::from_be_bytes(key[1..].try_into().ok()?))
    } else {
        None
    }
}

/// Bounds shared by both trait impls: `C` carries `u64` node ids, `BasicNode`
/// nodes, the default `Entry<C>`, and that entry must (de)serialize.
trait GroupConfig:
    RaftTypeConfig<NodeId = Nid, Node = BasicNode, Entry = openraft::Entry<Self>>
{
}

impl<C> GroupConfig for C where
    C: RaftTypeConfig<NodeId = Nid, Node = BasicNode, Entry = openraft::Entry<Self>>
{
}

#[derive(Clone)]
pub struct RocksLogStore {
    storage: Arc<Storage>,
    group: GroupId,
}

impl RocksLogStore {
    pub fn new(storage: Arc<Storage>, group: GroupId) -> Self {
        RocksLogStore { storage, group }
    }

    fn sync_wo() -> WriteOptions {
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        wo
    }

    /// Default-on rollback/A-B switch for asynchronous database-wide WAL
    /// group commit. Read once per process so environment lookup never touches
    /// the append hot path.
    fn group_commit_enabled() -> bool {
        static FLAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *FLAG.get_or_init(|| {
            !matches!(
                std::env::var("DAL_LOG_GROUP_COMMIT").ok().as_deref(),
                Some("0") | Some("false") | Some("off")
            )
        })
    }

    fn read_log_singleton<T: serde::de::DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<T>, StorageError<Nid>> {
        let cf = self.storage.log_cf(self.group).map_err(read_err)?;
        let raw = self
            .storage
            .db()
            .get_cf(&cf, key)
            .map_err(|e| read_err(e.into()))?;
        match raw {
            Some(bytes) => Ok(Some(codec::decode(&bytes).map_err(read_err)?)),
            None => Ok(None),
        }
    }

    fn last_entry_id<C>(&self) -> Result<Option<LogId>, StorageError<Nid>>
    where
        C: GroupConfig,
        openraft::Entry<C>: serde::de::DeserializeOwned,
    {
        let cf = self.storage.log_cf(self.group).map_err(read_err)?;
        // Seek just past the entry range and step backwards to the highest entry.
        let mut iter = self.storage.db().iterator_cf(
            &cf,
            IteratorMode::From(&[ENTRY_PREFIX_END], Direction::Reverse),
        );
        if let Some(item) = iter.next() {
            let (k, v) = item.map_err(|e| read_err(e.into()))?;
            if entry_index(&k).is_some() {
                let entry: openraft::Entry<C> = codec::decode(&v).map_err(read_err)?;
                return Ok(Some(entry.log_id));
            }
        }
        Ok(None)
    }
}

impl<C> RaftLogReader<C> for RocksLogStore
where
    C: GroupConfig,
    openraft::Entry<C>: serde::Serialize + serde::de::DeserializeOwned,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<C>>, StorageError<Nid>> {
        let start = match range.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(i) => i + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(i) => *i + 1,
            Bound::Excluded(i) => *i,
            Bound::Unbounded => u64::MAX,
        };

        let cf = self.storage.log_cf(self.group).map_err(read_err)?;
        let mut out = Vec::new();
        let iter = self.storage.db().iterator_cf(
            &cf,
            IteratorMode::From(&entry_key(start), Direction::Forward),
        );
        for item in iter {
            let (k, v) = item.map_err(|e| read_err(e.into()))?;
            match entry_index(&k) {
                Some(idx) if idx < end => {
                    out.push(codec::decode::<openraft::Entry<C>>(&v).map_err(read_err)?);
                }
                // Past the entry range or the requested end: stop.
                _ => break,
            }
        }
        Ok(out)
    }
}

impl<C> RaftLogStorage<C> for RocksLogStore
where
    C: GroupConfig,
    openraft::Entry<C>: serde::Serialize + serde::de::DeserializeOwned,
{
    type LogReader = RocksLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<Nid>> {
        let last_purged: Option<LogId> = self.read_log_singleton(&KEY_PURGED)?;
        let last = self.last_entry_id::<C>()?;
        let last_log_id = last.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError<Nid>> {
        let cf = self.storage.log_cf(self.group).map_err(write_err)?;
        let _profile = crate::perf::timer(WriteStage::SaveVoteSynced);
        self.storage
            .db()
            .put_cf_opt(&cf, KEY_VOTE, codec::encode(vote), &Self::sync_wo())
            .map_err(|e| write_err(e.into()))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError<Nid>> {
        self.read_log_singleton(&KEY_VOTE)
    }

    async fn save_committed(&mut self, _committed: Option<LogId>) -> Result<(), StorageError<Nid>> {
        // The state machine folds its final applied LogId into the same cross-CF
        // batch as the applied pointer and business mutations. OpenRaft permits
        // this method to be a no-op for a durable state machine; a crash before
        // apply therefore needs no marker.
        crate::perf::record_committed_marker_folded();
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError<Nid>> {
        // Stored as Option<LogId>; flatten the double-Option.
        Ok(self
            .read_log_singleton::<Option<LogId>>(&KEY_COMMITTED)?
            .flatten())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<Nid>>
    where
        I: IntoIterator<Item = openraft::Entry<C>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let (batch, wal_bytes) = {
            let cf = self.storage.log_cf(self.group).map_err(write_err)?;
            let mut batch = WriteBatch::default();
            let mut wal_bytes = 0usize;
            let _profile = crate::perf::timer(WriteStage::LogEncode);
            for entry in entries {
                let key = entry_key(entry.log_id.index);
                let value = codec::encode(&entry);
                wal_bytes = wal_bytes.saturating_add(key.len().saturating_add(value.len()));
                batch.put_cf(&cf, key, value);
            }
            (batch, wal_bytes)
        };
        if Self::group_commit_enabled() {
            let durable_wait = crate::perf::timer(WriteStage::WalDurabilityWait);
            self.storage
                .write_log_batch(
                    batch,
                    wal_bytes,
                    Box::new(move |result| {
                        drop(durable_wait);
                        callback.log_io_completed(result);
                    }),
                )
                .await
                .map_err(write_err)?;
            // The entries are readable now. OpenRaft waits on the callback,
            // which the group-commit worker completes after a durable WAL flush.
        } else {
            let _profile = crate::perf::timer(WriteStage::LogWriteSynced);
            self.storage
                .db()
                .write_opt(batch, &Self::sync_wo())
                .map_err(|error| write_err(error.into()))?;
            callback.log_io_completed(Ok(()));
        }
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError<Nid>> {
        // Delete all entries with index >= log_id.index.
        let cf = self.storage.log_cf(self.group).map_err(write_err)?;
        self.storage
            .db()
            .delete_range_cf(&cf, entry_key(log_id.index), entry_key(u64::MAX))
            .map_err(|e| write_err(e.into()))?;
        // delete_range is end-exclusive; remove the final index explicitly.
        self.storage
            .db()
            .delete_cf_opt(&cf, entry_key(u64::MAX), &Self::sync_wo())
            .map_err(|e| write_err(e.into()))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError<Nid>> {
        // A snapshot/purge boundary may only discard the replay source after
        // the materialized state through the same log id is WAL-durable.
        self.storage
            .wait_state_durable(self.group, &log_id)
            .await
            .map_err(write_err)?;
        let cf = self.storage.log_cf(self.group).map_err(write_err)?;
        // Record the purge point first so recovery never re-reads purged logs.
        self.storage
            .db()
            .put_cf_opt(&cf, KEY_PURGED, codec::encode(&log_id), &Self::sync_wo())
            .map_err(|e| write_err(e.into()))?;
        // Delete entries with index <= log_id.index (end-exclusive + explicit last).
        self.storage
            .db()
            .delete_range_cf(&cf, entry_key(0), entry_key(log_id.index))
            .map_err(|e| write_err(e.into()))?;
        self.storage
            .db()
            .delete_cf_opt(&cf, entry_key(log_id.index), &Self::sync_wo())
            .map_err(|e| write_err(e.into()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;
    use std::time::Duration;

    use crate::partition::raft_types::TypeConfig;

    fn log_id(index: u64) -> LogId {
        LogId::new(CommittedLeaderId::new(1, 1), index)
    }

    #[tokio::test]
    async fn purge_waits_for_the_same_materialized_prefix_to_be_durable() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            Arc::new(Storage::open_async_test(dir.path(), Duration::from_millis(200)).unwrap());
        let group = GroupId::Data(4);
        storage.ensure_group(group).unwrap();
        let target = log_id(6);

        let mut purger = RocksLogStore::new(storage.clone(), group);
        let purge = tokio::spawn(async move {
            <RocksLogStore as RaftLogStorage<TypeConfig>>::purge(&mut purger, target).await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!purge.is_finished());

        let cf = storage.state_cf(group).unwrap();
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf, b"state-key", b"state-value");
        let wal_bytes = batch.size_in_bytes();
        storage
            .write_state_batch(group, target, 1, batch, wal_bytes)
            .await
            .unwrap();

        // Visibility alone must not release purge.
        assert!(!purge.is_finished());
        purge.await.unwrap().unwrap();
        let mut reader = RocksLogStore::new(storage.clone(), group);
        let purged = <RocksLogStore as RaftLogStorage<TypeConfig>>::get_log_state(&mut reader)
            .await
            .unwrap()
            .last_purged_log_id;
        assert_eq!(purged, Some(target));
    }
}
