use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::search::{IndexCheckpoint, LocalSearchIndex};
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
        let _run = self.run.lock().await;
        let snapshot = self.storage.search_source_snapshot(self.group)?;
        if let Some(applied) = snapshot.applied.as_ref() {
            self.storage.wait_state_durable(self.group, applied).await?;
        }

        let checkpoint = self.index.validate_checkpoint(snapshot.epoch)?;
        let Some(checkpoint) = checkpoint else {
            let rejected_documents = self.index.rebuild(&snapshot)?;
            self.persist_checkpoint_and_prune()?;
            return Ok(SearchCatchUp {
                rebuilt: true,
                projected_keys: snapshot.records.len(),
                rejected_documents,
                checkpoint_index: snapshot.applied.map(|log_id| log_id.index),
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
        let source: HashMap<&[u8], (u64, &[u8])> = snapshot
            .records
            .iter()
            .map(|(key, version, value)| (key.as_slice(), (*version, value.as_slice())))
            .collect();
        let mut rejected_documents = 0u64;
        for key in &dirty {
            rejected_documents +=
                self.index
                    .project(key, source.get(key.as_slice()).copied())? as u64;
        }
        // Advance through no-op/rejected Raft commands too: state at A is
        // unchanged and every actual mutation in (C,A] was covered above.
        self.index.commit(snapshot.epoch, snapshot.applied)?;
        self.persist_checkpoint_and_prune()?;
        Ok(SearchCatchUp {
            rebuilt: false,
            projected_keys: dirty.len(),
            rejected_documents,
            checkpoint_index: (through != 0).then_some(through),
        })
    }

    fn persist_checkpoint_and_prune(&self) -> Result<()> {
        let checkpoint = self
            .index
            .checkpoint()?
            .ok_or_else(|| Error::Search("Tantivy commit has no checkpoint".into()))?;
        self.storage.record_search_consumer_checkpoint(
            self.group,
            &self.name,
            self.index.generation().id,
            checkpoint,
        )?;
        self.storage.prune_search_outbox(self.group)?;
        Ok(())
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
