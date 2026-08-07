use std::collections::HashMap;
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

/// Node-local registry of rebuildable per-partition Tantivy generations.
pub struct SearchService {
    storage: Arc<Storage>,
    loaded: RwLock<HashMap<(u16, String, u64), Arc<LoadedGeneration>>>,
    active: RwLock<HashMap<(u16, String), u64>>,
    install: tokio::sync::Mutex<()>,
}

impl SearchService {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            loaded: RwLock::new(HashMap::new()),
            active: RwLock::new(HashMap::new()),
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
        let _install = self.install.lock().await;
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
        self.storage
            .register_search_consumer(group, name, &generation)?;
        let loaded = if let Some(loaded) = self.loaded.read().unwrap().get(&key).cloned() {
            if loaded.index.generation().definition_hash != generation.definition_hash {
                return Err(Error::Search("loaded generation identity mismatch".into()));
            }
            loaded
        } else {
            let index = Arc::new(LocalSearchIndex::open_or_create(&path, group, generation)?);
            let worker = SearchIndexWorker::new(
                self.storage.clone(),
                group,
                name.to_string(),
                index.clone(),
            )?;
            let loaded = Arc::new(LoadedGeneration { index, worker });
            let mut guard = self.loaded.write().unwrap();
            guard
                .entry(key.clone())
                .or_insert_with(|| loaded.clone())
                .clone()
        };
        loaded.worker.catch_up().await?;
        if active {
            self.active
                .write()
                .unwrap()
                .insert((partition, name.to_string()), key.2);
        }
        Ok(())
    }

    pub fn deactivate(&self, partition: u16, name: &str) {
        self.active
            .write()
            .unwrap()
            .remove(&(partition, name.to_string()));
    }

    /// Stop retaining outbox history for a catalog generation after it has
    /// entered `retiring` and all readers have been routed away from it.
    pub fn remove_generation(&self, partition: u16, name: &str, generation: u64) -> Result<()> {
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

    pub fn readiness_observation(
        &self,
        partition: u16,
        name: &str,
        generation: u64,
        node_id: crate::types::NodeId,
        voters_log_id: crate::types::LogId,
    ) -> Result<crate::search::SearchIndexReady> {
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
    ) -> Result<SearchReply> {
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

        let barrier = if matches!(
            request.consistency,
            crate::search::SearchConsistency::Strict
        ) {
            match node.search_barrier().await? {
                SearchBarrier::Ready(barrier) => barrier,
                SearchBarrier::NotLeader { leader } => {
                    return Err(Error::Search(format!(
                        "not partition leader; leader={leader:?}"
                    )));
                }
            }
        } else {
            None
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
        loaded.index.search(partition, request)
    }
}
