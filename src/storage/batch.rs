//! The one helper that applies state-machine mutations and advances
//! `last_applied` in a single atomic RocksDB `WriteBatch` (DESIGN §6).
//!
//! Every applier — data and meta — goes through this module. Doing the mutation,
//! committed marker, and `last_applied` bump in one atomic batch is what makes
//! recovery a clean prefix replay. Raft batches use the database-wide
//! durability worker and data groups complete at visibility; the legacy M2
//! helper and meta state remain synchronously durable.

use rocksdb::{WriteBatch, WriteOptions};

use crate::codec;
use crate::error::Result;
use crate::keyspace;
use crate::partition::log_store::KEY_COMMITTED;
use crate::perf::WriteStage;
use crate::storage::rocks::Storage;
use crate::types::{GroupId, LogId};

/// A single state-CF mutation. The state machine (M2) decides key encoding; the
/// batch helper only moves bytes atomically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateMutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Simulate a crash at a durable-write boundary. Compiles to nothing unless the
/// `fail` crate's failpoints are active (tests only), so production builds pay
/// no cost. `_name` is consumed only by the macro, hence the underscore for the
/// compiled-out path.
fn crash_point(_name: &str) -> Result<()> {
    fail::fail_point!(_name, |_| Err(crate::error::Error::Io(
        std::io::Error::other(format!("injected crash at {_name}"))
    )));
    Ok(())
}

impl Storage {
    /// Atomically apply `mutations` to `cf_state_<group>` and set the group's
    /// `last_applied`, fsync-durable. This is the legacy M2 helper; OpenRaft
    /// state uses [`Self::apply_raft`].
    pub fn apply_state(
        &self,
        group: GroupId,
        mutations: &[StateMutation],
        last_applied: LogId,
    ) -> Result<()> {
        let cf = self.state_cf(group)?;

        let mut batch = WriteBatch::default();
        for m in mutations {
            match m {
                StateMutation::Put { key, value } => batch.put_cf(&cf, key, value),
                StateMutation::Delete { key } => batch.delete_cf(&cf, key),
            }
        }
        batch.put_cf(
            &cf,
            keyspace::last_applied_key(),
            codec::encode(&last_applied),
        );

        // Crash boundary: everything above is buffered in memory; nothing is
        // durable yet. A crash here must recover to the *previous* last_applied.
        crash_point("apply_state::before_write")?;

        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        {
            let _profile = crate::perf::timer(WriteStage::StateApplySynced);
            self.db().write_opt(batch, &wo)?;
        }

        // Crash boundary: the batch is durable. A crash here recovers to the
        // *new* last_applied with every mutation visible.
        crash_point("apply_state::after_write")?;

        Ok(())
    }

    /// The Raft state machine's atomic apply (M3): business mutations plus the
    /// opaque Raft applied-state blob (openraft `LogId` + membership) in one
    /// atomic batch. Shares the same write boundaries as [`Self::apply_state`],
    /// with durability completion tracked separately for async data groups.
    pub async fn apply_raft(
        &self,
        group: GroupId,
        mutations: &[StateMutation],
        applied_log_id: openraft::LogId<u64>,
        applied_record: &[u8],
        committed_record: &[u8],
        applied_entries: usize,
    ) -> Result<()> {
        let batch = {
            let cf = self.state_cf(group)?;
            let log_cf = self.log_cf(group)?;

            let mut batch = WriteBatch::default();
            for m in mutations {
                match m {
                    StateMutation::Put { key, value } => batch.put_cf(&cf, key, value),
                    StateMutation::Delete { key } => batch.delete_cf(&cf, key),
                }
            }
            batch.put_cf(&cf, keyspace::raft_applied_key(), applied_record);
            // Persist the optional recovery hint with exactly the state prefix
            // it describes. Cross-CF WriteBatch atomicity prevents the two
            // recovery pointers from disagreeing.
            batch.put_cf(&log_cf, KEY_COMMITTED, committed_record);
            batch
        };

        let wal_bytes = batch.size_in_bytes();
        crate::perf::record_apply_batch(applied_entries, mutations.len(), wal_bytes);

        crash_point("apply_state::before_write")?;

        self.write_state_batch(group, applied_log_id, applied_entries, batch, wal_bytes)
            .await?;

        crash_point("apply_state::after_write")?;

        Ok(())
    }

    /// Read the raw Raft applied-state blob, if any.
    pub fn raft_applied(&self, group: GroupId) -> Result<Option<Vec<u8>>> {
        self.get_state(group, &keyspace::raft_applied_key())
    }
}
