use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

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

fn projection_slots() -> &'static Arc<tokio::sync::Semaphore> {
    static SLOTS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(4)))
}

/// Drives one local Tantivy generation from a database-wide source snapshot.
/// Delivery is at-least-once; projection is idempotent delete-then-add.
#[derive(Clone)]
pub struct SearchIndexWorker {
    storage: Arc<Storage>,
    group: GroupId,
    name: String,
    index: Arc<LocalSearchIndex>,
    run: Arc<tokio::sync::Mutex<()>>,
    closed: Arc<AtomicBool>,
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
            run: Arc::new(tokio::sync::Mutex::new(())),
            closed: Arc::new(AtomicBool::new(false)),
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
        // Acquire before launching so cancelled waiters do not enqueue work.
        // Once launched, the owned guard stays with the entire job, including
        // every blocking stage and durable checkpoint publication.
        let guard = self.run.clone().lock_owned().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Search("search generation is closed".into()));
        }
        let worker = self.clone();
        tokio::spawn(async move {
            let _guard = guard;
            worker.run_pass(force_rebuild).await
        })
        .await
        .map_err(|error| Error::Search(format!("search projection task failed: {error}")))?
    }

    /// Fence future passes and wait for a pass whose caller has gone away.
    pub(crate) async fn close_and_drain(&self) {
        self.closed.store(true, Ordering::Release);
        let _guard = self.run.lock().await;
    }

    async fn run_pass(&self, force_rebuild: bool) -> Result<SearchCatchUp> {
        let _slot = projection_slots()
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Search("search projection executor is closed".into()))?;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Search("search generation is closed".into()));
        }
        // An invalid checkpoint always starts a full scan. Once a pass has an
        // owned job guard, every source read and index commit stays serialized.
        let hint = if force_rebuild {
            None
        } else {
            self.incremental_position()?
        };
        let Some(_after) = hint else {
            self.begin_rebuild()?;
            return self.rebuild_streaming().await;
        };

        let storage = self.storage.clone();
        let group = self.group;
        let name = self.name.clone();
        let generation = self.index.generation().id;
        let handle = tokio::runtime::Handle::current();
        let result = self
            .project_in_blocking(move |index| {
                storage.with_search_source_view(group, |source| {
                    let Some(checkpoint) = index.validate_checkpoint(source.epoch())? else {
                        return Ok(None);
                    };
                    if checkpoint_ahead_of_source(&checkpoint, source.applied().as_ref()) {
                        return Ok(None);
                    }
                    if let Some(applied) = source.applied() {
                        handle.block_on(storage.wait_state_durable(group, &applied))?;
                    }
                    let after = checkpoint.source_log_id.map(|id| id.index).unwrap_or(0);
                    let mut rejected_documents = 0u64;
                    let projected_keys = source.for_each_dirty(after, |key, value| {
                        rejected_documents += index.project(key, value)? as u64;
                        #[cfg(test)]
                        if group == GroupId::Data(u16::MAX - 1) {
                            fail::fail_point!("search_worker::after_project_for_cancellation_test");
                        } else if group == GroupId::Data(u16::MAX - 2) {
                            if let Some((entered, release)) =
                                cancellation_tests::epoch_gate().lock().unwrap().take()
                            {
                                let _ = entered.send(());
                                let _ = release.recv();
                            }
                        }
                        Ok(())
                    })?;
                    let through = source.applied().map(|id| id.index).unwrap_or(0);
                    if projected_keys == 0 && checkpoint.source_log_id == source.applied() {
                        let repaired =
                            storage.with_search_epoch_fence(group, source.epoch(), || {
                                let durable =
                                    storage.search_consumer_checkpoint(group, &name, generation)?;
                                if durable.as_ref() != Some(&checkpoint) {
                                    storage.record_search_consumer_checkpoint(
                                        group, &name, generation, checkpoint,
                                    )?;
                                    return Ok(true);
                                }
                                Ok(false)
                            })?;
                        if repaired {
                            storage.prune_search_outbox(group)?;
                        }
                        return Ok(Some(SearchCatchUp {
                            rebuilt: false,
                            projected_keys: 0,
                            rejected_documents: 0,
                            checkpoint_index: (through != 0).then_some(through),
                        }));
                    }
                    storage.with_search_epoch_fence(group, source.epoch(), || {
                        index.commit(source.epoch(), source.applied())?;
                        let committed = index.checkpoint()?.ok_or_else(|| {
                            Error::Search("Tantivy commit has no checkpoint".into())
                        })?;
                        storage
                            .record_search_consumer_checkpoint(group, &name, generation, committed)
                    })?;
                    storage.prune_search_outbox(group)?;
                    Ok(Some(SearchCatchUp {
                        rebuilt: false,
                        projected_keys,
                        rejected_documents,
                        checkpoint_index: (through != 0).then_some(through),
                    }))
                })
            })
            .await?;
        if let Some(result) = result {
            return Ok(result);
        }
        self.begin_rebuild()?;
        self.rebuild_streaming().await
    }

    /// Clear the durable pruning watermark before the authoritative snapshot
    /// that will seed a rebuild. A crash after this point leaves `None`, so the
    /// next install also rebuilds and retention remains pinned.
    fn begin_rebuild(&self) -> Result<()> {
        self.storage
            .begin_search_consumer_rebuild(self.group, &self.name, self.index.generation())
    }

    async fn rebuild_streaming(&self) -> Result<SearchCatchUp> {
        let storage = self.storage.clone();
        let group = self.group;
        let name = self.name.clone();
        let handle = tokio::runtime::Handle::current();
        self.project_in_blocking(move |index| {
            storage.with_search_source_view(group, |source| {
                if let Some(applied) = source.applied() {
                    handle.block_on(storage.wait_state_durable(group, &applied))?;
                }
                let checkpoint_index = source.applied().map(|id| id.index);
                storage.record_search_consumer_retention_floor(
                    group,
                    &name,
                    index.generation().id,
                    (source.epoch(), checkpoint_index.unwrap_or(0)),
                )?;
                let (projected_keys, rejected_documents) = index.rebuild_from_view(source)?;
                storage.with_search_epoch_fence(group, source.epoch(), || {
                    index.commit(source.epoch(), source.applied())?;
                    let checkpoint = index
                        .checkpoint()?
                        .ok_or_else(|| Error::Search("Tantivy rebuild has no checkpoint".into()))?;
                    storage.record_search_consumer_checkpoint(
                        group,
                        &name,
                        index.generation().id,
                        checkpoint,
                    )
                })?;
                storage.prune_search_outbox(group)?;
                Ok(SearchCatchUp {
                    rebuilt: true,
                    projected_keys,
                    rejected_documents,
                    checkpoint_index,
                })
            })
        })
        .await
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
        let closed = self.closed.clone();
        self.in_blocking(move || {
            let outcome = work(&index);
            if let Err(error) = &outcome
                && let Err(rollback) = index.rollback_uncommitted()
            {
                closed.store(true, Ordering::Release);
                return Err(Error::Search(format!(
                    "search projection failed: {error}; writer rollback failed: {rollback}"
                )));
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

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use crate::search::*;
    use crate::storage::StateMutation;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;

    type EpochPause = (
        tokio::sync::oneshot::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    );

    pub(super) fn epoch_gate() -> &'static Mutex<Option<EpochPause>> {
        static GATE: OnceLock<Mutex<Option<EpochPause>>> = OnceLock::new();
        GATE.get_or_init(|| Mutex::new(None))
    }

    struct ReleaseOnDrop(Option<std::sync::mpsc::Sender<()>>);

    impl ReleaseOnDrop {
        fn release(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.release();
        }
    }

    fn log_id(index: u64) -> openraft::LogId<u64> {
        openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), index)
    }

    fn generation() -> SearchIndexGeneration {
        SearchIndexGeneration::new(
            9,
            SearchIndexDefinition {
                document_type: "article".into(),
                fields: vec![SearchField {
                    name: "title".into(),
                    source_path: vec![PathSegment::Key("title".into())],
                    kind: FieldKind::Text {
                        tokenizer: "default".into(),
                        positions: true,
                    },
                    required: true,
                    multi_valued: false,
                    indexed: true,
                    stored: true,
                    fast: false,
                }],
                default_search_fields: vec!["title".into()],
            },
        )
        .unwrap()
    }

    async fn apply(storage: &Storage, group: GroupId, version: u64) {
        #[derive(serde::Serialize)]
        struct Record {
            version: u64,
            value: Vec<u8>,
        }
        let value = encode_search_value(
            "article",
            &flexbuffers::to_vec(serde_json::json!({"title": "term"})).unwrap(),
        )
        .unwrap();
        let mutations: Vec<_> = [b"a", b"b"]
            .into_iter()
            .map(|key| StateMutation::Put {
                key: crate::keyspace::user_key(key),
                value: crate::codec::encode(&Record {
                    version,
                    value: value.clone(),
                }),
            })
            .collect();
        let applied = crate::codec::encode(&(
            Some(log_id(version)),
            openraft::StoredMembership::<u64, openraft::BasicNode>::default(),
        ));
        storage
            .apply_raft(
                group,
                &mutations,
                log_id(version),
                &applied,
                &crate::codec::encode(&Some(log_id(version))),
                1,
            )
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_waiter_cannot_interleave_a_newer_projection() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).unwrap());
        let group = GroupId::Data(u16::MAX - 1);
        storage.ensure_group(group).unwrap();
        let generation = generation();
        storage
            .register_search_consumer(group, "articles", &generation)
            .unwrap();
        let index = Arc::new(
            LocalSearchIndex::open_or_create(&dir.path().join("index"), group, generation).unwrap(),
        );
        let worker = Arc::new(
            SearchIndexWorker::new(storage.clone(), group, "articles".into(), index.clone())
                .unwrap(),
        );
        worker.catch_up().await.unwrap();
        apply(&storage, group, 2).await;

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered_tx)));
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut release_guard = ReleaseOnDrop(Some(release_tx));
        let release = Arc::new(Mutex::new(release_rx));
        let fired = Arc::new(AtomicBool::new(false));
        fail::cfg_callback(
            "search_worker::after_project_for_cancellation_test",
            move || {
                if !fired.swap(true, Ordering::SeqCst) {
                    entered.lock().unwrap().take().unwrap().send(()).unwrap();
                    release.lock().unwrap().recv().unwrap();
                }
            },
        )
        .unwrap();
        let first_worker = worker.clone();
        let first = tokio::spawn(async move { first_worker.catch_up().await });
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        apply(&storage, group, 3).await;
        let second_worker = worker.clone();
        let second = tokio::spawn(async move { second_worker.catch_up().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !second.is_finished(),
            "new projection entered while older job was paused"
        );
        release_guard.release();
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        fail::remove("search_worker::after_project_for_cancellation_test");

        let checkpoint = index.checkpoint().unwrap().unwrap();
        assert_eq!(checkpoint.source_log_id, Some(log_id(3)));
        assert_eq!(storage.search_outbox_usage(group).unwrap().0, 0);
        let held = index
            .hold(
                &SearchQuery::MatchAll,
                GenerationSelection::Exact(9),
                10,
                checkpoint,
            )
            .unwrap();
        let reply = index
            .execute(u16::MAX - 1, &held, 10, held.local_statistics())
            .unwrap();
        assert_eq!(reply.hits.len(), 2);
        assert!(reply.hits.iter().all(|hit| hit.version == 3));
    }

    #[tokio::test]
    async fn repeated_dirty_keys_stream_to_one_final_version() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).unwrap());
        let group = GroupId::Data(60);
        storage.ensure_group(group).unwrap();
        let generation = generation();
        storage
            .register_search_consumer(group, "articles", &generation)
            .unwrap();
        let index = Arc::new(
            LocalSearchIndex::open_or_create(&dir.path().join("index"), group, generation).unwrap(),
        );
        let worker =
            SearchIndexWorker::new(storage.clone(), group, "articles".into(), index.clone())
                .unwrap();
        worker.catch_up().await.unwrap();
        apply(&storage, group, 2).await;
        apply(&storage, group, 3).await;
        worker.catch_up().await.unwrap();
        let checkpoint = index.checkpoint().unwrap().unwrap();
        assert_eq!(checkpoint.source_log_id, Some(log_id(3)));
        let held = index
            .hold(
                &SearchQuery::MatchAll,
                GenerationSelection::Exact(9),
                10,
                checkpoint,
            )
            .unwrap();
        let reply = index
            .execute(60, &held, 10, held.local_statistics())
            .unwrap();
        assert_eq!(reply.hits.len(), 2);
        assert!(reply.hits.iter().all(|hit| hit.version == 3));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn snapshot_install_fences_a_paused_old_epoch_projection() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).unwrap());
        let group = GroupId::Data(u16::MAX - 2);
        storage.ensure_group(group).unwrap();
        let generation = generation();
        storage
            .register_search_consumer(group, "articles", &generation)
            .unwrap();
        let index = Arc::new(
            LocalSearchIndex::open_or_create(&dir.path().join("index"), group, generation).unwrap(),
        );
        let worker = Arc::new(
            SearchIndexWorker::new(storage.clone(), group, "articles".into(), index.clone())
                .unwrap(),
        );
        worker.catch_up().await.unwrap();
        apply(&storage, group, 2).await;

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut release_guard = ReleaseOnDrop(Some(release_tx));
        *epoch_gate().lock().unwrap() = Some((entered_tx, release_rx));
        let running = worker.clone();
        let pass = tokio::spawn(async move { running.catch_up().await });
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        let installed = crate::codec::encode(&(
            Some(log_id(3)),
            openraft::StoredMembership::<u64, openraft::BasicNode>::default(),
        ));
        let installing_storage = storage.clone();
        let install = tokio::task::spawn_blocking(move || {
            installing_storage.install_state(group, &[], &installed)
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if storage.search_projection_epoch(group).unwrap() == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        release_guard.release();
        assert!(
            pass.await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("epoch changed")
        );
        install.await.unwrap().unwrap();
        storage.record_state_installed(group, Some(log_id(3)));

        worker.catch_up().await.unwrap();
        let checkpoint = index.checkpoint().unwrap().unwrap();
        assert_eq!(checkpoint.projection_epoch, 2);
        assert_eq!(checkpoint.source_log_id, Some(log_id(3)));
        let held = index
            .hold(
                &SearchQuery::MatchAll,
                GenerationSelection::Exact(9),
                10,
                checkpoint,
            )
            .unwrap();
        let reply = index
            .execute(u16::MAX - 2, &held, 10, held.local_statistics())
            .unwrap();
        assert!(reply.hits.is_empty());
    }
}
