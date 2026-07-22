//! `RaftLogStorage` + `RaftLogReader` over `cf_log_<group>` (DESIGN §6, M3).
//!
//! Every append and vote save is fsync-durable before it is acknowledged; the
//! leader only counts durable follower replies toward commit (DESIGN §2).

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use openraft::OptionalSend;
use openraft::StorageError;
use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};

use crate::codec;
use crate::partition::raft_types::{read_err, write_err, Entry, LogId, NodeId, TypeConfig, Vote};
use crate::storage::Storage;
use crate::types::GroupId;

const KEY_VOTE: [u8; 2] = [0x00, b'v'];
const KEY_PURGED: [u8; 2] = [0x00, b'p'];
const KEY_COMMITTED: [u8; 2] = [0x00, b'c'];
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

    fn read_log_singleton<T: serde::de::DeserializeOwned>(
        &self,
        key: &[u8],
    ) -> Result<Option<T>, StorageError<NodeId>> {
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

    fn last_entry_id(&self) -> Result<Option<LogId>, StorageError<NodeId>> {
        let cf = self.storage.log_cf(self.group).map_err(read_err)?;
        // Seek just past the entry range and step backwards to the highest entry.
        let mut iter = self.storage.db().iterator_cf(
            &cf,
            IteratorMode::From(&[ENTRY_PREFIX_END], Direction::Reverse),
        );
        if let Some(item) = iter.next() {
            let (k, v) = item.map_err(|e| read_err(e.into()))?;
            if entry_index(&k).is_some() {
                let entry: Entry = codec::decode(&v).map_err(read_err)?;
                return Ok(Some(entry.log_id));
            }
        }
        Ok(None)
    }
}

impl RaftLogReader<TypeConfig> for RocksLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<NodeId>> {
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
                    out.push(codec::decode::<Entry>(&v).map_err(read_err)?);
                }
                // Past the entry range or the requested end: stop.
                _ => break,
            }
        }
        Ok(out)
    }
}

impl RaftLogStorage<TypeConfig> for RocksLogStore {
    type LogReader = RocksLogStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged: Option<LogId> = self.read_log_singleton(&KEY_PURGED)?;
        let last = self.last_entry_id()?;
        let last_log_id = last.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError<NodeId>> {
        let cf = self.storage.log_cf(self.group).map_err(write_err)?;
        self.storage
            .db()
            .put_cf_opt(&cf, KEY_VOTE, codec::encode(vote), &Self::sync_wo())
            .map_err(|e| write_err(e.into()))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError<NodeId>> {
        self.read_log_singleton(&KEY_VOTE)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId>,
    ) -> Result<(), StorageError<NodeId>> {
        let cf = self.storage.log_cf(self.group).map_err(write_err)?;
        self.storage
            .db()
            .put_cf_opt(&cf, KEY_COMMITTED, codec::encode(&committed), &Self::sync_wo())
            .map_err(|e| write_err(e.into()))?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError<NodeId>> {
        // Stored as Option<LogId>; flatten the double-Option.
        Ok(self
            .read_log_singleton::<Option<LogId>>(&KEY_COMMITTED)?
            .flatten())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let cf = self.storage.log_cf(self.group).map_err(write_err)?;
        let mut batch = WriteBatch::default();
        for entry in entries {
            batch.put_cf(&cf, entry_key(entry.log_id.index), codec::encode(&entry));
        }
        self.storage
            .db()
            .write_opt(batch, &Self::sync_wo())
            .map_err(|e| write_err(e.into()))?;
        // Entries are durable: signal completion.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError<NodeId>> {
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

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError<NodeId>> {
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
