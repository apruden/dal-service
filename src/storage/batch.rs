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

/// How often the apply path trims the dirty-key outbox. Keyed off the applied
/// log index so it needs no per-group counter; the scan stops at the first
/// entry a registered consumer still needs, so a well-behaved group pays
/// almost nothing.
const SEARCH_OUTBOX_PRUNE_APPLIES: u64 = 4096;

fn crossed_search_prune_boundary(applied_index: u64, applied_entries: usize) -> bool {
    let applied_entries = u64::try_from(applied_entries).unwrap_or(u64::MAX);
    let previous_index = applied_index.saturating_sub(applied_entries);
    applied_index / SEARCH_OUTBOX_PRUNE_APPLIES > previous_index / SEARCH_OUTBOX_PRUNE_APPLIES
}

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
            if matches!(group, GroupId::Data(_)) {
                let stored = self.get_local::<u64>(&keyspace::search_epoch_key(group))?;
                let epoch = stored.unwrap_or(1);
                if stored.is_none() {
                    batch.put(keyspace::search_epoch_key(group), codec::encode(&epoch));
                }
                for mutation in mutations {
                    let state_key = match mutation {
                        StateMutation::Put { key, .. } | StateMutation::Delete { key } => key,
                    };
                    if let Some(user_key) = state_key.strip_prefix(&[keyspace::TAG_USER]) {
                        let outbox_key = crate::search::encode_outbox_key(
                            group,
                            epoch,
                            &applied_log_id,
                            user_key,
                        )?;
                        batch.put(outbox_key, []);
                    }
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

        // Retention is owned by the write path, not by the search worker: a
        // group with no registered consumer never runs one, and its outbox
        // would otherwise grow for the lifetime of the database. Every entry is
        // discardable there, because a generation registered later rebuilds
        // from a full state snapshot.
        if matches!(group, GroupId::Data(_))
            && crossed_search_prune_boundary(applied_log_id.index, applied_entries)
            && let Err(error) = self.prune_search_outbox(group)
        {
            // The state batch above is already visible and the Raft entry is
            // committed; failing apply now would report failure after the
            // entry was materialized. Retained outbox entries replay safely,
            // but a RocksDB/on-disk invariant failure must still trip the
            // database-wide fail-stop fence before we acknowledge.
            let error = self.poison_raft_error("search outbox prune failed", error);
            tracing::error!(?group, %error, "search outbox prune failed; storage fenced");
        }

        Ok(())
    }

    /// Read the raw Raft applied-state blob, if any.
    pub fn raft_applied(&self, group: GroupId) -> Result<Option<Vec<u8>>> {
        self.get_state(group, &keyspace::raft_applied_key())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesced_applies_trigger_when_they_cross_a_prune_boundary() {
        assert!(!crossed_search_prune_boundary(4095, 128));
        assert!(crossed_search_prune_boundary(4096, 1));
        assert!(crossed_search_prune_boundary(4097, 2));
        assert!(crossed_search_prune_boundary(9000, 5000));
    }
}
