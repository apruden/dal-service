use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use crate::error::{Error, Result};
use crate::partition::{PartitionNode, SearchBarrier};
use crate::search::{
    GenerationSelection, LocalSearchIndex, SearchIndexGeneration, SearchIndexWorker, SearchReply,
    SearchRequest, validate_index_name,
};
use crate::storage::Storage;
use crate::types::GroupId;

struct LoadedGeneration {
    index: Arc<LocalSearchIndex>,
    worker: SearchIndexWorker,
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
    lifecycle: tokio::sync::RwLock<()>,
    install: tokio::sync::Mutex<()>,
}

impl SearchService {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            loaded: RwLock::new(HashMap::new()),
            active: RwLock::new(HashMap::new()),
            unavailable: RwLock::new(HashSet::new()),
            lifecycle: tokio::sync::RwLock::new(()),
            install: tokio::sync::Mutex::new(()),
        }
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
        let _lifecycle = self.lifecycle.read().await;
        let _install = self.install.lock().await;
        if self.unavailable.read().unwrap().contains(&partition) {
            return Err(Error::Search(format!(
                "partition {partition} is closed for search"
            )));
        }
        validate_index_name(name)?;
        let group = GroupId::Data(partition);
        let path = self
            .storage
            .path()
            .join("search")
            .join(group.token())
            .join(name)
            .join(generation.id.to_string());
        let key = (partition, name.to_string(), generation.id);
        let generation_id = generation.id;
        let definition_hash = generation.definition_hash;
        // A registered consumer with no checkpoint holds outbox retention for
        // the whole partition, so every failure past this point must undo
        // whatever this call created — and only what this call created, since
        // an earlier healthy install may own the same records.
        let registered = self
            .storage
            .register_search_consumer(group, name, &generation)?;

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

        if let Err(error) = loaded.worker.catch_up_rebuilding(registered).await {
            self.roll_back(
                group,
                name,
                generation_id,
                registered,
                opened.then_some(key),
            );
            return Err(error);
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
        let _lifecycle = self.lifecycle.read().await;
        self.loaded
            .write()
            .unwrap()
            .remove(&(partition, name.to_string(), generation));
        let mut active = self.active.write().unwrap();
        if active.get(&(partition, name.to_string())) == Some(&generation) {
            active.remove(&(partition, name.to_string()));
        }
        drop(active);
        self.storage
            .unregister_search_consumer(GroupId::Data(partition), name, generation)
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

    pub async fn shard_search(
        &self,
        node: &PartitionNode,
        request: &SearchRequest,
    ) -> Result<ShardSearchOutcome> {
        let _lifecycle = self.lifecycle.read().await;
        let GroupId::Data(partition) = node.group() else {
            return Err(Error::Search(
                "shard search requires a data partition".into(),
            ));
        };
        let generation = match request.generation {
            GenerationSelection::Active => self
                .active
                .read()
                .unwrap()
                .get(&(partition, request.index.clone()))
                .copied()
                .ok_or_else(|| Error::Search("active index generation is not ready".into()))?,
            GenerationSelection::Exact(generation) => generation,
        };
        let loaded = self
            .loaded
            .read()
            .unwrap()
            .get(&(partition, request.index.clone(), generation))
            .cloned()
            .ok_or_else(|| Error::Search("index generation is not ready on this replica".into()))?;

        let linearizable = matches!(
            request.consistency,
            crate::search::SearchConsistency::Strict
        );
        let barrier = match node.search_barrier(linearizable).await? {
            SearchBarrier::Ready(barrier) => barrier,
            SearchBarrier::NotLeader { leader } => {
                return Ok(ShardSearchOutcome::NotLeader { leader });
            }
        };

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
        Ok(ShardSearchOutcome::Reply(
            loaded.index.search(partition, request)?,
        ))
    }
}
