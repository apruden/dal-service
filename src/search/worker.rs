use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::search::{IndexCheckpoint, LocalSearchIndex, SearchSourceSnapshot};
use crate::storage::Storage;
use crate::types::GroupId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchCatchUp {
    pub rebuilt: bool,
    pub projected_keys: usize,
    pub rejected_documents: u64,
    pub checkpoint_index: Option<u64>,
}

/// Drives one local Tantivy generation from a database-wide source snapshot.
/// Delivery is at-least-once; projection is idempotent delete-then-add.
pub struct SearchIndexWorker {
    storage: Arc<Storage>,
    group: GroupId,
    name: String,
    index: Arc<LocalSearchIndex>,
    run: tokio::sync::Mutex<()>,
}

impl SearchIndexWorker {
    pub fn new(
        storage: Arc<Storage>,
        group: GroupId,
        name: String,
        index: Arc<LocalSearchIndex>,
    ) -> Result<Self> {
        if !matches!(group, GroupId::Data(_)) {
            return Err(Error::Search("search worker requires a data group".into()));
        }
        Ok(Self {
            storage,
            group,
            name,
            index,
            run: tokio::sync::Mutex::new(()),
        })
    }

    /// Rebuild when identity/epoch is not usable; otherwise consume all dirty
    /// keys through one consistent source prefix. Tantivy is committed only
    /// after the RocksDB WAL is known durable through that prefix.
    pub async fn catch_up(&self) -> Result<SearchCatchUp> {
        // A consumer released from outbox retention for exceeding its lag
        // budget cannot resume incrementally: the journal it would have read
        // was truncated on its behalf (design §6.5).
        let forced = self.storage.search_consumer_needs_rebuild(
            self.group,
            &self.name,
            self.index.generation().id,
        )?;
        self.catch_up_rebuilding(forced).await
    }

    /// Catch up this generation, forcing a full authoritative-state rebuild
    /// when its durable consumer record carries no checkpoint — a fresh
    /// registration, or a registration whose first rebuild never committed
    /// before a crash. A valid-looking on-disk checkpoint is not proof of
    /// continuity in either case: while no checkpoint was held, pruning was
    /// allowed to discard this consumer's outbox gap.
    pub(crate) async fn catch_up_rebuilding(&self, force_rebuild: bool) -> Result<SearchCatchUp> {
        let _run = self.run.lock().await;

        // Pick the scan shape before snapshotting: an index whose checkpoint is
        // already valid for the current epoch only needs the rows its outbox
        // marks dirty, while an invalid one needs every row. The epoch is
        // re-read from the snapshot, so a snapshot install racing this read is
        // caught below and redone as a rebuild.
        let hint = if force_rebuild {
            None
        } else {
            self.incremental_position()?
        };
        if hint.is_none() {
            self.begin_rebuild()?;
        }
        let mut snapshot = self.load_source(hint).await?;
        let checkpoint = if force_rebuild {
            None
        } else {
            self.usable_checkpoint(&snapshot)?
        };
        if checkpoint.is_none() && hint.is_some() {
            // This index's checkpoint was already rejected above, so it must
            // not be resumed from. Re-testing it against the wider snapshot
            // would let it back in: an ahead-of-source checkpoint stops looking
            // ahead as soon as the authoritative prefix advances past it, which
            // would silently resume the projection just declared untrustworthy
            // (§7.2) and leave the diverged prefix's documents in place.
            self.begin_rebuild()?;
            snapshot = self.load_source(None).await?;
        }

        let Some(checkpoint) = checkpoint else {
            let projected_keys = snapshot.records.len();
            let checkpoint_index = snapshot.applied.map(|log_id| log_id.index);
            let rejected_documents = self
                .project_in_blocking(move |index| index.rebuild(&snapshot))
                .await?;
            self.persist_checkpoint_and_prune().await?;
            return Ok(SearchCatchUp {
                rebuilt: true,
                projected_keys,
                rejected_documents,
                checkpoint_index,
            });
        };

        let after = checkpoint
            .source_log_id
            .as_ref()
            .map(|log_id| log_id.index)
            .unwrap_or(0);
        let through = snapshot
            .applied
            .as_ref()
            .map(|log_id| log_id.index)
            .unwrap_or(0);
        let dirty: HashSet<Vec<u8>> = snapshot
            .outbox
            .iter()
            .filter(|entry| {
                entry.source_log_id.index > after && entry.source_log_id.index <= through
            })
            .map(|entry| entry.user_key.clone())
            .collect();
        // Nothing to project and no prefix to advance: skip the commit rather
        // than fsync a Tantivy segment and reload the reader for a no-op, which
        // is the common case when a search runs against an idle partition. The
        // whole log id must match, not just the index, so a term change is
        // still recorded.
        if dirty.is_empty() && checkpoint.source_log_id == snapshot.applied {
            // The Tantivy commit is already correct, but the durable consumer
            // record may not be: a crash between a commit and the record write
            // that follows it leaves the retention watermark behind the commit,
            // and on an idle partition nothing would ever advance it again — so
            // the outbox stays pinned until the lag budget forces a rebuild.
            // Re-publishing is cheap; what the early return avoids is the
            // Tantivy commit and reader reload, not this point read.
            let durable = self.storage.search_consumer_checkpoint(
                self.group,
                &self.name,
                self.index.generation().id,
            )?;
            if durable.as_ref() != Some(&checkpoint) {
                self.persist_checkpoint_and_prune().await?;
            }
            return Ok(SearchCatchUp {
                rebuilt: false,
                projected_keys: 0,
                rejected_documents: 0,
                checkpoint_index: (through != 0).then_some(through),
            });
        }

        let projected_keys = dirty.len();
        let rejected_documents = self
            .project_in_blocking(move |index| {
                let source: HashMap<&[u8], (u64, &[u8])> = snapshot
                    .records
                    .iter()
                    .map(|(key, version, value)| (key.as_slice(), (*version, value.as_slice())))
                    .collect();
                let mut rejected_documents = 0u64;
                for key in &dirty {
                    rejected_documents +=
                        index.project(key, source.get(key.as_slice()).copied())? as u64;
                }
                // Advance through no-op/rejected Raft commands too: state at A
                // is unchanged and every actual mutation in (C,A] was covered.
                index.commit(snapshot.epoch, snapshot.applied)?;
                Ok(rejected_documents)
            })
            .await?;
        self.persist_checkpoint_and_prune().await?;
        Ok(SearchCatchUp {
            rebuilt: false,
            projected_keys,
            rejected_documents,
            checkpoint_index: (through != 0).then_some(through),
        })
    }

    /// A checkpoint the worker may resume from: identity/epoch-valid and not
    /// ahead of the authoritative snapshot. An ahead checkpoint cannot be
    /// trusted — with equal epochs a projection must never move backward — so
    /// it is discarded and the generation rebuilds from authoritative state
    /// (design §7.2) instead of failing every catch-up forever.
    fn usable_checkpoint(
        &self,
        snapshot: &SearchSourceSnapshot,
    ) -> Result<Option<IndexCheckpoint>> {
        let Some(checkpoint) = self.index.validate_checkpoint(snapshot.epoch)? else {
            return Ok(None);
        };
        if checkpoint_ahead_of_source(&checkpoint, snapshot.applied.as_ref()) {
            tracing::warn!(
                group = ?self.group,
                name = self.name,
                "search checkpoint is ahead of authoritative state; discarding for rebuild"
            );
            return Ok(None);
        }
        Ok(Some(checkpoint))
    }

    /// Clear the durable pruning watermark before the authoritative snapshot
    /// that will seed a rebuild. A crash after this point leaves `None`, so the
    /// next install also rebuilds and retention remains pinned.
    fn begin_rebuild(&self) -> Result<()> {
        self.storage
            .begin_search_consumer_rebuild(self.group, &self.name, self.index.generation())
    }

    /// The source index a valid checkpoint has already projected through, or
    /// `None` when the index needs a full rebuild.
    fn incremental_position(&self) -> Result<Option<u64>> {
        let epoch = self.storage.search_projection_epoch(self.group)?;
        Ok(self.index.validate_checkpoint(epoch)?.map(|checkpoint| {
            checkpoint
                .source_log_id
                .as_ref()
                .map(|log_id| log_id.index)
                .unwrap_or(0)
        }))
    }

    async fn load_source(&self, after: Option<u64>) -> Result<SearchSourceSnapshot> {
        let storage = self.storage.clone();
        let group = self.group;
        let snapshot = self
            .in_blocking(move || storage.search_source_snapshot(group, after))
            .await?;
        if let Some(applied) = snapshot.applied.as_ref() {
            self.storage.wait_state_durable(self.group, applied).await?;
        }
        Ok(snapshot)
    }

    /// Run one projection pass, discarding whatever it buffered if it fails.
    ///
    /// The writer outlives any single pass, so adds left behind by a failed one
    /// would be published by whatever commits next — including a rebuild, whose
    /// `delete_all_documents` does not drop them — under a checkpoint that
    /// claims a prefix those documents are not part of (I5).
    async fn project_in_blocking<T, F>(&self, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&LocalSearchIndex) -> Result<T> + Send + 'static,
    {
        let index = self.index.clone();
        self.in_blocking(move || {
            let outcome = work(&index);
            if outcome.is_err()
                && let Err(error) = index.rollback_uncommitted()
            {
                tracing::error!(%error, "could not discard a failed search projection pass");
            }
            outcome
        })
        .await
    }

    /// RocksDB scans and Tantivy indexing are synchronous and disk-bound, so
    /// they must not run on a runtime worker thread.
    async fn in_blocking<T, F>(&self, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T> + Send + 'static,
    {
        tokio::task::spawn_blocking(work)
            .await
            .map_err(|error| Error::Search(format!("search projection task failed: {error}")))?
    }

    async fn persist_checkpoint_and_prune(&self) -> Result<()> {
        let storage = self.storage.clone();
        let index = self.index.clone();
        let group = self.group;
        let name = self.name.clone();
        self.in_blocking(move || {
            let checkpoint = index
                .checkpoint()?
                .ok_or_else(|| Error::Search("Tantivy commit has no checkpoint".into()))?;
            storage.record_search_consumer_checkpoint(
                group,
                &name,
                index.generation().id,
                checkpoint,
            )?;
            storage.prune_search_outbox(group)?;
            Ok(())
        })
        .await
    }
}

fn checkpoint_ahead_of_source(
    checkpoint: &IndexCheckpoint,
    source: Option<&openraft::LogId<u64>>,
) -> bool {
    match (checkpoint.source_log_id.as_ref(), source) {
        (Some(checkpoint), Some(source)) => checkpoint.index > source.index,
        (Some(_), None) => true,
        _ => false,
    }
}
