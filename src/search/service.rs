use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::partition::{PartitionNode, SearchBarrier};
use crate::search::{
    GenerationSelection, GlobalBm25Statistics, HeldSearch, IndexCheckpoint, LocalSearchIndex,
    SEARCH_MAX_SESSIONS, SEARCH_SESSION_TTL_MS, SearchIndexGeneration, SearchIndexWorker,
    SearchReply, SearchRequest, SearchSessionId, ShardExecuteReply, ShardExecuteRequest,
    ShardPrepareReply, ShardPrepareRequest, ShardPrepared, validate_index_name,
};
use crate::storage::Storage;
use crate::types::GroupId;

struct LoadedGeneration {
    index: Arc<LocalSearchIndex>,
    worker: SearchIndexWorker,
}

/// One prepared distributed query, pinning the exact Tantivy commit that
/// produced the statistics the coordinator summed.
struct HeldSession {
    partition: u16,
    index: Arc<LocalSearchIndex>,
    held: HeldSearch,
    window: usize,
    /// The epoch this searcher was pinned under. A snapshot install bumps the
    /// group's epoch, and phase two must not serve a projection from before it
    /// just because the session has not expired yet (I8).
    projection_epoch: u64,
    expires_at: Instant,
}

/// The outcome of a shard search. Not-leader stays structured so the gateway
/// can route it through the same redirect channel as a point operation instead
/// of flattening it into a terminal error.
pub enum ShardSearchOutcome {
    Reply(SearchReply),
    NotLeader {
        leader: Option<crate::types::NodeId>,
    },
}

/// Node-local registry of rebuildable per-partition Tantivy generations.
pub struct SearchService {
    storage: Arc<Storage>,
    loaded: RwLock<HashMap<(u16, String, u64), Arc<LoadedGeneration>>>,
    active: RwLock<HashMap<(u16, String), u64>>,
    unavailable: RwLock<HashSet<u16>>,
    sessions: RwLock<HashMap<SearchSessionId, Arc<HeldSession>>>,
    session_incarnation: u64,
    next_session: AtomicU64,
    /// Bounds concurrent shard work so search cannot consume the handler
    /// capacity Raft replication and point operations need (design §13).
    shard_slots: Arc<tokio::sync::Semaphore>,
    coordinator_slots: Arc<tokio::sync::Semaphore>,
    lifecycle: tokio::sync::RwLock<()>,
    install: tokio::sync::Mutex<()>,
}

impl SearchService {
    pub fn new(storage: Arc<Storage>) -> Result<Self> {
        let session_incarnation = storage.next_search_session_incarnation()?;
        Ok(Self {
            storage,
            loaded: RwLock::new(HashMap::new()),
            active: RwLock::new(HashMap::new()),
            unavailable: RwLock::new(HashSet::new()),
            sessions: RwLock::new(HashMap::new()),
            session_incarnation,
            next_session: AtomicU64::new(1),
            shard_slots: Arc::new(tokio::sync::Semaphore::new(
                crate::search::SEARCH_MAX_CONCURRENT_SHARD,
            )),
            coordinator_slots: Arc::new(tokio::sync::Semaphore::new(
                crate::search::SEARCH_MAX_CONCURRENT_GLOBAL,
            )),
            lifecycle: tokio::sync::RwLock::new(()),
            install: tokio::sync::Mutex::new(()),
        })
    }

    fn next_session_id(&self) -> Result<SearchSessionId> {
        let sequence = self
            .next_session
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| Error::Search("search session sequence exhausted".into()))?;
        Ok(SearchSessionId {
            incarnation: self.session_incarnation,
            sequence,
        })
    }

    /// Admit one whole coordinated search. Held for both phases, so the number
    /// of in-flight fan-outs — and therefore the number of held sessions a
    /// single node can create — is bounded.
    pub fn admit_coordinated(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        self.coordinator_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Search("too many concurrent searches on this node".into()))
    }

    /// Admit one shard phase. Rejecting rather than queueing keeps a shard's
    /// latency bounded by its own deadline instead of by the backlog.
    fn admit_shard(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        self.shard_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Search("too many concurrent shard searches on this replica".into()))
    }

    /// Install/recover one generation. Marking it active is a separate local
    /// reflection of the already-committed control-plane activation.
    pub async fn install_generation(
        &self,
        partition: u16,
        name: &str,
        generation: SearchIndexGeneration,
        active: bool,
    ) -> Result<()> {
        let group = GroupId::Data(partition);
        let key = (partition, name.to_string(), generation.id);
        let generation_id = generation.id;
        let definition_hash = generation.definition_hash;

        // Open and register under the guards; the backfill below runs without
        // them.
        let (loaded, opened, registered, needs_rebuild) = {
            let _lifecycle = self.lifecycle.read().await;
            let _install = self.install.lock().await;
            if self.unavailable.read().unwrap().contains(&partition) {
                return Err(Error::Search(format!(
                    "partition {partition} is closed for search"
                )));
            }
            validate_index_name(name)?;
            let path = self
                .storage
                .path()
                .join("search")
                .join(group.token())
                .join(name)
                .join(generation.id.to_string());
            // A registered consumer with no checkpoint holds outbox retention
            // for the whole partition, so every failure past this point must
            // undo whatever this call created — and only what this call
            // created, since an earlier healthy install may own the same
            // records.
            let registration = self
                .storage
                .register_search_consumer(group, name, &generation)?;
            let registered = registration.created;

            let cached = self.loaded.read().unwrap().get(&key).cloned();
            let (loaded, opened) = match cached {
                Some(loaded) => {
                    if loaded.index.generation().definition_hash != definition_hash {
                        self.roll_back(group, name, generation_id, registered, None);
                        return Err(Error::Search("loaded generation identity mismatch".into()));
                    }
                    (loaded, false)
                }
                None => match self.open(group, &path, name, generation) {
                    Ok(loaded) => {
                        self.loaded
                            .write()
                            .unwrap()
                            .insert(key.clone(), loaded.clone());
                        (loaded, true)
                    }
                    Err(error) => {
                        self.roll_back(group, name, generation_id, registered, None);
                        return Err(error);
                    }
                },
            };
            (loaded, opened, registered, registration.needs_rebuild)
        };

        // A new generation backfills the whole partition, so this await can run
        // for minutes. Holding the lifecycle guard across it would block every
        // writer — `forget_partition`, `allow_partition`, `remove_generation` —
        // and so stall reclamation and rebalance for the same duration. The
        // worker serializes its own passes, so a concurrent install of this
        // generation is safe here.
        let backfill = loaded.worker.catch_up_rebuilding(needs_rebuild).await;

        let _lifecycle = self.lifecycle.read().await;
        if let Err(error) = backfill {
            self.roll_back(
                group,
                name,
                generation_id,
                registered,
                opened.then_some(key),
            );
            return Err(error);
        }
        // Reclamation may have dropped this partition or generation while the
        // backfill ran unguarded. Publishing now would re-adopt an index the
        // node no longer hosts, so re-check before touching `active`.
        if self.unavailable.read().unwrap().contains(&partition)
            || !self.loaded.read().unwrap().contains_key(&key)
        {
            self.roll_back(
                group,
                name,
                generation_id,
                registered,
                opened.then_some(key),
            );
            return Err(Error::Search(format!(
                "partition {partition} was closed for search during install"
            )));
        }
        if active {
            self.active
                .write()
                .unwrap()
                .insert((partition, name.to_string()), generation_id);
        }
        Ok(())
    }

    fn open(
        &self,
        group: GroupId,
        path: &std::path::Path,
        name: &str,
        generation: SearchIndexGeneration,
    ) -> Result<Arc<LoadedGeneration>> {
        let index = Arc::new(LocalSearchIndex::open_or_create(path, group, generation)?);
        let worker =
            SearchIndexWorker::new(self.storage.clone(), group, name.to_string(), index.clone())?;
        Ok(Arc::new(LoadedGeneration { index, worker }))
    }

    fn roll_back(
        &self,
        group: GroupId,
        name: &str,
        generation: u64,
        registered: bool,
        opened: Option<(u16, String, u64)>,
    ) {
        if let Some(key) = opened {
            self.loaded.write().unwrap().remove(&key);
        }
        if registered
            && let Err(error) = self
                .storage
                .unregister_search_consumer(group, name, generation)
        {
            tracing::warn!(?group, name, %error, "failed to roll back search consumer");
        }
    }

    /// Drop every cached generation for a partition this node no longer hosts.
    /// The on-disk index and the consumer records are removed by `drop_group`;
    /// without this the maps would keep serving — and re-adopt — an index built
    /// from the previous incarnation of the partition.
    pub async fn forget_partition(&self, partition: u16) {
        let _lifecycle = self.lifecycle.write().await;
        self.unavailable.write().unwrap().insert(partition);
        // Close the search serving gate before the caller drops the group's
        // files: no in-flight query may reopen them afterwards (design §11.3).
        self.expire_sessions(partition);
        self.loaded
            .write()
            .unwrap()
            .retain(|(hosted, _, _), _| *hosted != partition);
        self.active
            .write()
            .unwrap()
            .retain(|(hosted, _), _| *hosted != partition);
    }

    /// Reopen search installation and serving after a fresh partition runtime
    /// has been admitted. Reclamation and admission share the runtime's
    /// partition lifecycle lock, so this cannot overlap the corresponding
    /// durable group deletion.
    pub async fn allow_partition(&self, partition: u16) {
        let _lifecycle = self.lifecycle.write().await;
        self.unavailable.write().unwrap().remove(&partition);
    }

    /// Point new `Active` searches at a loaded generation. This only mirrors
    /// an activation the control plane already committed; it never decides one.
    pub async fn activate(&self, partition: u16, name: &str, generation: u64) {
        let _lifecycle = self.lifecycle.read().await;
        if !self.is_loaded(partition, name, generation) {
            return;
        }
        self.active
            .write()
            .unwrap()
            .insert((partition, name.to_string()), generation);
    }

    pub async fn deactivate(&self, partition: u16, name: &str) {
        let _lifecycle = self.lifecycle.read().await;
        self.active
            .write()
            .unwrap()
            .remove(&(partition, name.to_string()));
    }

    /// Stop retaining outbox history for a catalog generation after it has
    /// entered `retiring` and all readers have been routed away from it.
    pub async fn remove_generation(
        &self,
        partition: u16,
        name: &str,
        generation: u64,
    ) -> Result<()> {
        // Exclusive, like `forget_partition`: a read guard would let a shard
        // phase prepare a fresh session for this generation after
        // `expire_sessions` ran and before its directory is deleted.
        let _lifecycle = self.lifecycle.write().await;
        self.loaded
            .write()
            .unwrap()
            .remove(&(partition, name.to_string(), generation));
        let mut active = self.active.write().unwrap();
        if active.get(&(partition, name.to_string())) == Some(&generation) {
            active.remove(&(partition, name.to_string()));
        }
        drop(active);
        // Expire before deleting: a session still holding this generation's
        // searcher would otherwise keep reading files that are on their way
        // out (design §11.3).
        self.expire_sessions(partition);
        self.storage
            .unregister_search_consumer(GroupId::Data(partition), name, generation)?;
        // Reclaim the retired generation's disk. Tantivy files are derived, so
        // a failure here is a warning, not a correctness problem.
        let path = self.generation_path(partition, name, generation);
        if path.exists()
            && let Err(error) = std::fs::remove_dir_all(&path)
        {
            tracing::warn!(?path, %error, "failed to remove retired search index directory");
        }
        Ok(())
    }

    fn generation_path(&self, partition: u16, name: &str, generation: u64) -> std::path::PathBuf {
        self.storage
            .path()
            .join("search")
            .join(GroupId::Data(partition).token())
            .join(name)
            .join(generation.to_string())
    }

    /// Every generation this node currently has loaded, as
    /// `(partition, index name, generation id)`.
    pub fn loaded_generations(&self) -> Vec<(u16, String, u64)> {
        self.loaded.read().unwrap().keys().cloned().collect()
    }

    pub fn is_loaded(&self, partition: u16, name: &str, generation: u64) -> bool {
        self.loaded
            .read()
            .unwrap()
            .contains_key(&(partition, name.to_string(), generation))
    }

    /// Whether this replica can actually serve a generation: loaded, matching
    /// the catalog's definition, and holding a checkpoint valid for the live
    /// projection epoch. Used as the promotion fence before a learner becomes
    /// a voter (design §11.1).
    pub async fn generation_readiness(
        &self,
        partition: u16,
        name: &str,
        generation: u64,
        definition_hash: [u8; 32],
    ) -> Result<()> {
        let _lifecycle = self.lifecycle.read().await;
        let loaded = self
            .loaded
            .read()
            .unwrap()
            .get(&(partition, name.to_string(), generation))
            .cloned()
            .ok_or_else(|| Error::Search("index generation is not loaded".into()))?;
        if loaded.index.generation().definition_hash != definition_hash {
            return Err(Error::Search("index definition hash mismatch".into()));
        }
        let epoch = self
            .storage
            .search_projection_epoch(GroupId::Data(partition))?;
        loaded
            .index
            .validate_checkpoint(epoch)?
            .ok_or_else(|| Error::Search("index has no valid checkpoint for this epoch".into()))?;
        Ok(())
    }

    /// Node-local status for every generation hosted on a partition, plus the
    /// size of the journal they share (design §14). Read-only and best effort:
    /// a generation whose checkpoint cannot be read is reported as `Failed`
    /// rather than failing the whole status page.
    pub fn partition_status(
        &self,
        partition: u16,
        applied: u64,
    ) -> (Vec<crate::runtime::http::SearchIndexStatus>, u64, u64) {
        use crate::runtime::http::{SearchIndexState, SearchIndexStatus};

        let group = GroupId::Data(partition);
        let epoch = self.storage.search_projection_epoch(group).unwrap_or(0);
        let active = self.active.read().unwrap().clone();
        let loaded: Vec<_> = self
            .loaded
            .read()
            .unwrap()
            .iter()
            .filter(|((hosted, _, _), _)| *hosted == partition)
            .map(|((_, name, generation), entry)| (name.clone(), *generation, entry.index.clone()))
            .collect();

        let mut statuses = Vec::with_capacity(loaded.len());
        for (name, generation, index) in loaded {
            let checkpoint = index.validate_checkpoint(epoch).ok().flatten();
            let searchable_through = checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.source_log_id.as_ref())
                .map(|log_id| log_id.index);
            let needs_rebuild = self
                .storage
                .search_consumer_needs_rebuild(group, &name, generation)
                .unwrap_or(false);
            let state = if needs_rebuild {
                SearchIndexState::NeedsRebuild
            } else if checkpoint.is_none() {
                SearchIndexState::Failed
            } else if active.get(&(partition, name.clone())) == Some(&generation) {
                SearchIndexState::Active
            } else {
                SearchIndexState::Building
            };
            statuses.push(SearchIndexStatus {
                name,
                generation,
                definition_hash: index
                    .generation()
                    .definition_hash
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
                state,
                projection_epoch: epoch,
                searchable_through,
                lag_entries: applied.saturating_sub(searchable_through.unwrap_or(0)),
            });
        }
        statuses.sort_by(|left, right| {
            (&left.name, left.generation).cmp(&(&right.name, right.generation))
        });
        let (entries, bytes) = self.storage.search_outbox_usage(group).unwrap_or((0, 0));
        (statuses, entries, bytes)
    }

    /// Hold the group's dirty-key journal inside its budget, releasing the
    /// slowest projection for rebuild if it has grown past it (design §6.5).
    pub fn enforce_outbox_bounds(&self, partition: u16) -> Result<Option<(String, u64)>> {
        self.storage.enforce_search_outbox_bounds(
            GroupId::Data(partition),
            crate::search::SEARCH_MAX_OUTBOX_ENTRIES,
            crate::search::SEARCH_MAX_OUTBOX_BYTES,
        )
    }

    /// Advance one loaded generation over its outbox. Searches catch the
    /// serving generation up on demand, but a generation that is still
    /// building — or one on an idle partition — only makes progress here, and
    /// an index that never advances pins outbox retention (design §6.5).
    pub async fn catch_up_generation(
        &self,
        partition: u16,
        name: &str,
        generation: u64,
    ) -> Result<crate::search::SearchCatchUp> {
        let _lifecycle = self.lifecycle.read().await;
        let loaded = self
            .loaded
            .read()
            .unwrap()
            .get(&(partition, name.to_string(), generation))
            .cloned()
            .ok_or_else(|| Error::Search("index generation is not loaded".into()))?;
        loaded.worker.catch_up().await
    }

    pub async fn readiness_observation(
        &self,
        partition: u16,
        name: &str,
        generation: u64,
        node_id: crate::types::NodeId,
        voters_log_id: crate::types::LogId,
    ) -> Result<crate::search::SearchIndexReady> {
        let _lifecycle = self.lifecycle.read().await;
        let loaded = self
            .loaded
            .read()
            .unwrap()
            .get(&(partition, name.to_string(), generation))
            .cloned()
            .ok_or_else(|| Error::Search("index generation is not loaded".into()))?;
        let epoch = self
            .storage
            .search_projection_epoch(GroupId::Data(partition))?;
        let checkpoint = loaded
            .index
            .validate_checkpoint(epoch)?
            .ok_or_else(|| Error::Search("index generation has no valid checkpoint".into()))?;
        let source = checkpoint
            .source_log_id
            .ok_or_else(|| Error::Search("index has not projected a Raft entry".into()))?;
        Ok(crate::search::SearchIndexReady {
            generation,
            definition_hash: loaded.index.generation().definition_hash,
            engine_revision: loaded.index.generation().engine_revision,
            group: GroupId::Data(partition),
            node_id,
            voters_log_id,
            projection_epoch: epoch,
            source_log_id: crate::types::LogId::new(source.leader_id.term, source.index),
        })
    }

    /// Fence the shard and bring its projection up to the barrier: leader
    /// check, catch-up, epoch/identity validation, then the §I9 coverage test.
    /// Returns the loaded generation and the exact checkpoint that satisfied
    /// the barrier, so callers never re-read a checkpoint that may have moved.
    async fn gate(
        &self,
        node: &PartitionNode,
        index: &str,
        generation: GenerationSelection,
        strict: bool,
    ) -> Result<GateOutcome> {
        let GroupId::Data(partition) = node.group() else {
            return Err(Error::Search(
                "shard search requires a data partition".into(),
            ));
        };
        // Leader gate first: a follower that never loaded this generation must
        // still surface as a redirect, not a terminal per-partition error.
        let barrier = match node.search_barrier(strict).await? {
            SearchBarrier::Ready(barrier) => barrier,
            SearchBarrier::NotLeader { leader } => return Ok(GateOutcome::NotLeader { leader }),
        };

        let generation = match generation {
            GenerationSelection::Active => self
                .active
                .read()
                .unwrap()
                .get(&(partition, index.to_string()))
                .copied()
                .ok_or_else(|| Error::Search("active index generation is not ready".into()))?,
            GenerationSelection::Exact(generation) => generation,
        };
        let loaded = self
            .loaded
            .read()
            .unwrap()
            .get(&(partition, index.to_string(), generation))
            .cloned()
            .ok_or_else(|| Error::Search("index generation is not ready on this replica".into()))?;

        loaded.worker.catch_up().await?;
        let epoch = self.storage.search_projection_epoch(node.group())?;
        let checkpoint = loaded
            .index
            .validate_checkpoint(epoch)?
            .ok_or_else(|| Error::Search("index checkpoint identity/epoch mismatch".into()))?;
        if let Some(barrier) = barrier {
            let covered = checkpoint
                .source_log_id
                .as_ref()
                .map(|checkpoint| checkpoint.index >= barrier.index)
                .unwrap_or(false);
            if !covered {
                return Err(Error::Search(format!(
                    "index checkpoint does not cover ReadIndex barrier {}",
                    barrier.index
                )));
            }
        }
        Ok(GateOutcome::Ready {
            partition,
            loaded,
            checkpoint,
        })
    }

    pub async fn shard_search(
        &self,
        node: &PartitionNode,
        request: &SearchRequest,
    ) -> Result<ShardSearchOutcome> {
        let _lifecycle = self.lifecycle.read().await;
        let _slot = self.admit_shard()?;
        let strict = matches!(
            request.consistency,
            crate::search::SearchConsistency::Strict
        );
        let (partition, loaded, checkpoint) = match self
            .gate(node, &request.index, request.generation, strict)
            .await?
        {
            GateOutcome::Ready {
                partition,
                loaded,
                checkpoint,
            } => (partition, loaded, checkpoint),
            GateOutcome::NotLeader { leader } => {
                return Ok(ShardSearchOutcome::NotLeader { leader });
            }
        };
        let request = request.clone();
        let pinned_epoch = checkpoint.projection_epoch;
        let reply = tokio::task::spawn_blocking(move || {
            loaded.index.search(partition, &request, checkpoint)
        })
        .await
        .map_err(|error| Error::Search(format!("shard search task failed: {error}")))??;

        // `gate` read the epoch before the searcher was pinned, and pinning
        // only re-checks that the loaded checkpoint is unchanged — which an
        // install does not alter, since it replaces the authoritative state
        // rather than the already-loaded commit. Re-reading here settles it
        // after the fact: an unchanged epoch proves no install completed before
        // the pin, so this reply cannot be a previous-epoch commit (I8). The
        // two-phase path gets the same treatment in `shard_execute`.
        let epoch = self.storage.search_projection_epoch(node.group())?;
        if epoch != pinned_epoch {
            return Err(Error::Search(
                "snapshot install replaced the projection during this search".into(),
            ));
        }
        Ok(ShardSearchOutcome::Reply(reply))
    }

    /// Phase one of a global-scoring search (design §8.1, §9.2): fence the
    /// leader, pin one searcher, and report the statistics measured against
    /// exactly that commit. The session keeps the searcher alive so phase two
    /// scores the same documents the statistics described.
    pub async fn shard_prepare(
        &self,
        node: &PartitionNode,
        request: &ShardPrepareRequest,
    ) -> Result<ShardPrepareReply> {
        let _lifecycle = self.lifecycle.read().await;
        let _slot = self.admit_shard()?;
        validate_index_name(&request.index)?;
        let strict = matches!(
            request.consistency,
            crate::search::SearchConsistency::Strict
        );
        let (partition, loaded, checkpoint) = match self
            .gate(
                node,
                &request.index,
                GenerationSelection::Exact(request.generation),
                strict,
            )
            .await?
        {
            GateOutcome::Ready {
                partition,
                loaded,
                checkpoint,
            } => (partition, loaded, checkpoint),
            GateOutcome::NotLeader { leader } => {
                return Ok(ShardPrepareReply::NotLeader { leader });
            }
        };
        // The coordinator resolved the catalog generation; a replica serving a
        // different definition under the same id must fail rather than blend
        // two schemas into one merged result (I10).
        if loaded.index.generation().definition_hash != request.definition_hash {
            return Ok(ShardPrepareReply::Error(
                "shard index definition hash does not match the coordinator".into(),
            ));
        }

        let window = request.window;
        let query = request.query.clone();
        let index = loaded.index.clone();
        let (held, statistics) = tokio::task::spawn_blocking(move || {
            let held = index.hold(
                &query,
                GenerationSelection::Exact(index.generation().id),
                window,
                checkpoint,
            )?;
            let statistics = index.shard_statistics(&held)?;
            Ok::<_, Error>((held, statistics))
        })
        .await
        .map_err(|error| Error::Search(format!("shard prepare task failed: {error}")))??;

        // Never publish a session whose accompanying statistics violate the
        // coordinator's wire contract. Otherwise the coordinator must reject
        // the reply after this replica has already pinned a searcher.
        statistics.validate()?;

        let checkpoint_index = held
            .checkpoint()
            .source_log_id
            .as_ref()
            .map(|log_id| log_id.index)
            .unwrap_or(0);
        let session_id = self.next_session_id()?;
        let session = Arc::new(HeldSession {
            partition,
            index: loaded.index.clone(),
            projection_epoch: held.checkpoint().projection_epoch,
            held,
            window: window as usize,
            expires_at: Instant::now() + Duration::from_millis(SEARCH_SESSION_TTL_MS),
        });
        {
            let mut sessions = self.sessions.write().unwrap();
            let now = Instant::now();
            sessions.retain(|_, session| session.expires_at > now);
            if sessions.len() >= SEARCH_MAX_SESSIONS {
                return Ok(ShardPrepareReply::Error(
                    "too many held search sessions on this replica".into(),
                ));
            }
            sessions.insert(session_id, session);
        }
        Ok(ShardPrepareReply::Prepared(ShardPrepared {
            session_id,
            checkpoint_index,
            statistics,
        }))
    }

    /// Phase two: score the pinned searcher with the coordinator's summed
    /// statistics. The session is consumed, releasing the searcher whether or
    /// not scoring succeeds.
    pub async fn shard_execute(&self, request: &ShardExecuteRequest) -> ShardExecuteReply {
        // Phase two reads the same pinned segment files phase one opened, so it
        // must hold the serving gate too: reclamation and generation removal
        // expire sessions under the write guard and then delete those files
        // (design §11.3).
        let _lifecycle = self.lifecycle.read().await;
        let _slot = match self.admit_shard() {
            Ok(slot) => slot,
            Err(error) => return ShardExecuteReply::Error(error.to_string()),
        };
        let session = self.sessions.write().unwrap().remove(&request.session_id);
        let Some(session) = session else {
            return ShardExecuteReply::UnknownSession;
        };
        if Instant::now() > session.expires_at {
            return ShardExecuteReply::UnknownSession;
        }
        // A snapshot install between the two phases replaced the authoritative
        // state and bumped the epoch; the pinned commit now describes a prefix
        // that no longer exists, so it must not be scored (I8).
        match self
            .storage
            .search_projection_epoch(GroupId::Data(session.partition))
        {
            Ok(epoch) if epoch == session.projection_epoch => {}
            Ok(_) => return ShardExecuteReply::UnknownSession,
            Err(error) => return ShardExecuteReply::Error(error.to_string()),
        }
        if let Err(error) = request.statistics.validate() {
            return ShardExecuteReply::Error(error.to_string());
        }
        let statistics = request.statistics.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let global = GlobalBm25Statistics::new(session.index.schema(), &statistics)?;
            session
                .index
                .execute(session.partition, &session.held, session.window, &global)
        })
        .await;
        match outcome {
            Ok(Ok(reply)) => ShardExecuteReply::Result(reply),
            Ok(Err(error)) => ShardExecuteReply::Error(error.to_string()),
            Err(error) => ShardExecuteReply::Error(format!("shard execute task failed: {error}")),
        }
    }

    /// Hand a held session back without scoring it, releasing its pinned
    /// searcher immediately instead of at expiry.
    pub fn release_session(&self, session_id: SearchSessionId) {
        self.sessions.write().unwrap().remove(&session_id);
    }

    /// Drop every held session for a partition leaving this node. Reclamation
    /// must not leave a searcher holding files the group is about to delete
    /// (design §11.3).
    fn expire_sessions(&self, partition: u16) {
        self.sessions
            .write()
            .unwrap()
            .retain(|_, session| session.partition != partition);
    }
}

enum GateOutcome {
    Ready {
        partition: u16,
        loaded: Arc<LoadedGeneration>,
        checkpoint: IndexCheckpoint,
    },
    NotLeader {
        leader: Option<crate::types::NodeId>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_ids_do_not_alias_across_service_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).unwrap());

        let first = SearchService::new(storage.clone()).unwrap();
        let first_id = first.next_session_id().unwrap();
        drop(first);

        let second = SearchService::new(storage).unwrap();
        let second_id = second.next_session_id().unwrap();

        assert_eq!(first_id.sequence, 1);
        assert_eq!(second_id.sequence, 1);
        assert!(second_id.incarnation > first_id.incarnation);
        assert_ne!(first_id, second_id);
    }
}
