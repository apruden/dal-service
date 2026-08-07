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
        self.catch_up_rebuilding(false).await
    }

    /// Catch up this generation, forcing a full authoritative-state rebuild
    /// when its consumer registration is new. A valid-looking on-disk
    /// checkpoint is not proof of continuity in that case: while the consumer
    /// was absent, no-consumer pruning was allowed to discard its outbox gap.
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
        let mut snapshot = self.load_source(hint).await?;
        let mut checkpoint = if force_rebuild {
            None
        } else {
            self.index.validate_checkpoint(snapshot.epoch)?
        };
        if checkpoint.is_none() && hint.is_some() {
            snapshot = self.load_source(None).await?;
            checkpoint = self.index.validate_checkpoint(snapshot.epoch)?;
        }

        let Some(checkpoint) = checkpoint else {
            let index = self.index.clone();
            let projected_keys = snapshot.records.len();
            let checkpoint_index = snapshot.applied.map(|log_id| log_id.index);
            let rejected_documents = self.in_blocking(move || index.rebuild(&snapshot)).await?;
            self.persist_checkpoint_and_prune().await?;
            return Ok(SearchCatchUp {
                rebuilt: true,
                projected_keys,
                rejected_documents,
                checkpoint_index,
            });
        };

        if checkpoint_ahead_of_source(&checkpoint, snapshot.applied.as_ref()) {
            // A snapshot install can lower the log index while changing epoch;
            // equal epochs must never allow a checkpoint to move backward.
            return Err(Error::Corrupt(
                "search checkpoint".into(),
                "Tantivy checkpoint is ahead of authoritative state".into(),
            ));
        }

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
            return Ok(SearchCatchUp {
                rebuilt: false,
                projected_keys: 0,
                rejected_documents: 0,
                checkpoint_index: (through != 0).then_some(through),
            });
        }

        let index = self.index.clone();
        let projected_keys = dirty.len();
        let rejected_documents = self
            .in_blocking(move || {
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
