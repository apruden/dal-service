//! RocksDB handle and per-group column-family lifecycle (DESIGN §6).
//!
//! One RocksDB instance per node. Every Raft group has one log CF and one active
//! state-CF generation; snapshot install fills a new inactive generation and
//! atomically switches a durable pointer. The default CF holds that pointer and
//! node-local authority state that must survive snapshot install and reclamation.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options,
    WriteOptions, statistics::Ticker,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::codec;
use crate::error::{Error, Result};
use crate::keyspace;
use crate::perf::{RocksCounters, WriteStage};
use crate::storage::apply_durability::{
    ApplyDurabilityLimits, ApplyDurabilityRegistry, ApplyDurabilitySnapshot, DurabilityWaitKind,
};
use crate::storage::durability::{DurabilityConfig, DurabilityKind, OnDurable, WalDurability};
use crate::types::{
    BootstrapGroup, ClusterId, GroupId, LearnerAdmission, LogId, NodeId, RegistrationBinding,
    ServingState,
};

/// MultiThreaded so CFs can be created/dropped through `&self` while other
/// threads read — the runtime pattern for on-line rebalancing (DESIGN §6).
type Db = DBWithThreadMode<MultiThreaded>;
type RaftLogId = openraft::LogId<u64>;
type RaftApplied = (
    Option<RaftLogId>,
    openraft::StoredMembership<u64, openraft::BasicNode>,
);

/// The node identity record persisted in the default CF. Opening a data dir
/// whose identity mismatches the config is a hard error (DESIGN §12.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
}

pub struct Storage {
    // Drop the coordinator first: it drains callbacks and joins its worker
    // before the final RocksDB handle is released.
    durability: WalDurability,
    apply_durability: Arc<ApplyDurabilityRegistry>,
    db: Arc<Db>,
    profile_options: Option<Options>,
    path: PathBuf,
    /// The complete state generation visible for each group. Switching this
    /// cache follows a sync-durable pointer write, so readers see either the old
    /// complete CF or the new complete CF.
    active_state_cfs: std::sync::Mutex<HashMap<GroupId, String>>,
    /// Serializes short CF-generation allocation/switch/drop critical sections.
    snapshot_lifecycle: std::sync::Mutex<()>,
    /// Serializes consumer registration with outbox pruning so a new backfill
    /// cannot lose a mutation between registration and its source snapshot.
    search_lifecycle: std::sync::Mutex<()>,
}

/// A point-in-time RocksDB view whose iterator remains valid after the Raft
/// apply mutex is released.
pub(crate) struct StateSnapshot<'a> {
    snapshot: rocksdb::SnapshotWithThreadMode<'a, Db>,
    cf: Arc<BoundColumnFamily<'a>>,
}

impl StateSnapshot<'_> {
    pub(crate) fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        let records = self
            .snapshot
            .iterator_cf(&self.cf, rocksdb::IteratorMode::Start)
            .map(|item| item.map_err(Error::from));
        crate::snapshot::encode_records(writer, records)
    }
}

/// Incremental builder for a complete, inactive state-CF generation. Nothing
/// becomes visible until [`StateInstaller::finish`] durably switches the local
/// generation pointer.
pub(crate) struct StateInstaller<'a> {
    storage: &'a Storage,
    group: GroupId,
    name: String,
    batch: rocksdb::WriteBatch,
    last_key: Option<Vec<u8>>,
    activated: bool,
}

impl StateInstaller<'_> {
    const BATCH_BYTES: usize = 4 * 1024 * 1024;

    pub(crate) fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if self
            .last_key
            .as_ref()
            .is_some_and(|previous| previous.as_slice() >= key.as_slice())
        {
            return Err(Error::Codec(
                "snapshot state keys must be strictly increasing".into(),
            ));
        }
        let cf = self.storage.db.cf_handle(&self.name).ok_or_else(|| {
            Error::Corrupt(self.name.clone(), "snapshot staging CF disappeared".into())
        })?;
        self.batch.put_cf(&cf, &key, value);
        self.last_key = Some(key);
        if self.batch.size_in_bytes() >= Self::BATCH_BYTES {
            self.flush_batch(false)?;
        }
        Ok(())
    }

    fn flush_batch(&mut self, sync: bool) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(&mut self.batch);
        let mut options = WriteOptions::default();
        options.set_sync(sync);
        self.storage.db.write_opt(batch, &options)?;
        Ok(())
    }

    pub(crate) fn finish(mut self, applied_record: &[u8]) -> Result<()> {
        let cf = self.storage.db.cf_handle(&self.name).ok_or_else(|| {
            Error::Corrupt(self.name.clone(), "snapshot staging CF disappeared".into())
        })?;
        self.batch
            .put_cf(&cf, keyspace::raft_applied_key(), applied_record);
        drop(cf);
        self.flush_batch(false)?;
        // Every staged chunk must survive before the pointer can expose it.
        self.storage.db.flush_wal(true)?;

        fail::fail_point!("snapshot_install::before_write", |_| Err(Error::Io(
            std::io::Error::other("injected crash at snapshot_install::before_write")
        )));

        let _lifecycle = self.storage.snapshot_lifecycle.lock().unwrap();
        let mut active = self.storage.active_state_cfs.lock().unwrap();
        let old_name = self
            .storage
            .active_state_cf_name_locked(self.group, &mut active)?;
        let old_cf = self.storage.db.cf_handle(&old_name);
        let mut pointer = rocksdb::WriteBatch::default();
        pointer.put(
            keyspace::state_cf_pointer_key(self.group),
            codec::encode(&self.name),
        );
        if matches!(self.group, GroupId::Data(_)) {
            let next_epoch = self
                .storage
                .search_projection_epoch(self.group)?
                .checked_add(1)
                .ok_or_else(|| Error::Search("search projection epoch exhausted".into()))?;
            pointer.put(
                keyspace::search_epoch_key(self.group),
                codec::encode(&next_epoch),
            );
        }
        self.storage.db.write_opt(pointer, &sync_write())?;
        active.insert(self.group, self.name.clone());
        self.activated = true;
        drop(active);
        drop(_lifecycle);

        fail::fail_point!("snapshot_install::after_write", |_| Err(Error::Io(
            std::io::Error::other("injected crash at snapshot_install::after_write")
        )));

        if old_name != self.name
            && let Some(old_cf) = old_cf
        {
            self.storage.retire_state_cf(&old_name, old_cf)?;
        }
        Ok(())
    }
}

impl Drop for StateInstaller<'_> {
    fn drop(&mut self) {
        self.batch = rocksdb::WriteBatch::default();
        if !self.activated && self.storage.db.cf_handle(&self.name).is_some() {
            let _ = self.storage.db.drop_cf(&self.name);
        }
    }
}

fn sync_write() -> WriteOptions {
    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    wo
}

impl Storage {
    /// Open (or create) the store, re-attaching every existing column family.
    pub fn open(path: impl AsRef<Path>) -> Result<Storage> {
        Self::open_with_durability(path, DurabilityConfig::default())
    }

    fn open_with_durability(
        path: impl AsRef<Path>,
        durability_config: DurabilityConfig,
    ) -> Result<Storage> {
        Self::open_with_limits(path, durability_config, ApplyDurabilityLimits::default())
    }

    fn open_with_limits(
        path: impl AsRef<Path>,
        durability_config: DurabilityConfig,
        apply_limits: ApplyDurabilityLimits,
    ) -> Result<Storage> {
        let path = path.as_ref().to_path_buf();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        if crate::perf::write_path_enabled() {
            opts.enable_statistics();
        }

        // On a fresh dir there is no descriptor list yet; start with default.
        let existing = Db::list_cf(&opts, &path).unwrap_or_else(|_| vec!["default".to_string()]);
        let descriptors: Vec<ColumnFamilyDescriptor> = existing
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();

        let db = Arc::new(Db::open_cf_descriptors(&opts, &path, descriptors)?);
        let apply_durability = Arc::new(ApplyDurabilityRegistry::with_limits(apply_limits));
        ApplyDurabilityRegistry::spawn_watchdog(&apply_durability)?;
        let write_db = db.clone();
        let flush_db = db.clone();
        let write_health = apply_durability.clone();
        let flush_health = apply_durability.clone();
        let mut wal_options = WriteOptions::default();
        wal_options.set_sync(false);
        let durability = WalDurability::with_config(
            durability_config,
            move |batch| {
                let result = write_db
                    .write_opt(batch, &wal_options)
                    .map_err(|error| std::io::Error::other(error.to_string()));
                if let Err(error) = &result {
                    write_health.fail_all(format!("database WAL write failed: {error}"));
                }
                result
            },
            move || {
                let result = flush_db
                    .flush_wal(true)
                    .map_err(|error| std::io::Error::other(error.to_string()));
                if let Err(error) = &result {
                    flush_health.fail_all(format!("WAL flush failed: {error}"));
                }
                result
            },
        )?;
        apply_durability.set_failure_hook(durability.failure_hook());
        let storage = Storage {
            durability,
            apply_durability,
            db,
            profile_options: crate::perf::write_path_enabled().then_some(opts),
            path,
            active_state_cfs: std::sync::Mutex::new(HashMap::new()),
            snapshot_lifecycle: std::sync::Mutex::new(()),
            search_lifecycle: std::sync::Mutex::new(()),
        };
        storage.cleanup_snapshot_cfs(&existing)?;
        storage.cleanup_snapshot_temp_files();
        Ok(storage)
    }

    #[cfg(test)]
    pub(crate) fn open_delayed_test(
        path: impl AsRef<Path>,
        flush_delay: std::time::Duration,
    ) -> Result<Storage> {
        Self::open_with_durability(
            path,
            DurabilityConfig {
                max_pending_requests: 8,
                max_pending_bytes: 1_024 * 1_024,
                max_batch_delay: flush_delay,
                adaptive_batching: false,
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn fail_durability_test(&self, error: impl Into<String>) {
        self.apply_durability.fail_all(error.into());
    }

    #[cfg(test)]
    pub(crate) fn open_limited_test(
        path: impl AsRef<Path>,
        flush_delay: std::time::Duration,
        limits: ApplyDurabilityLimits,
    ) -> Result<Storage> {
        Self::open_with_limits(
            path,
            DurabilityConfig {
                max_pending_requests: 8,
                max_pending_bytes: 1_024 * 1_024,
                max_batch_delay: flush_delay,
                adaptive_batching: false,
            },
            limits,
        )
    }

    /// Open and bind identity: on a fresh dir persist `(cluster_id, node_id)`;
    /// on a reused dir require an exact match (DESIGN §12.1).
    pub fn open_checked(
        path: impl AsRef<Path>,
        cluster_id: ClusterId,
        node_id: NodeId,
    ) -> Result<Storage> {
        let storage = Storage::open(path)?;
        match storage.identity()? {
            Some(found) => {
                if found.cluster_id != cluster_id || found.node_id != node_id {
                    return Err(Error::IdentityMismatch {
                        found_cluster: found.cluster_id,
                        found_node: found.node_id,
                        want_cluster: cluster_id,
                        want_node: node_id,
                    });
                }
            }
            None => {
                storage.set_identity(Identity {
                    cluster_id,
                    node_id,
                })?;
            }
        }
        Ok(storage)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    // ---- identity ----------------------------------------------------------

    pub fn identity(&self) -> Result<Option<Identity>> {
        self.get_local(&keyspace::identity_key())
    }

    pub fn set_identity(&self, id: Identity) -> Result<()> {
        self.put_local(&keyspace::identity_key(), &id)
    }

    /// Allocate the next durable heartbeat incarnation for this process start.
    /// The in-memory sequence may then restart at one without being mistaken
    /// for a replay from a prior process instance.
    pub fn next_heartbeat_incarnation(&self) -> Result<u64> {
        let previous: u64 = self
            .get_local(&keyspace::heartbeat_incarnation_key())?
            .unwrap_or(0);
        let next = previous
            .checked_add(1)
            .ok_or_else(|| Error::Config("heartbeat incarnation exhausted".into()))?;
        self.put_local(&keyspace::heartbeat_incarnation_key(), &next)?;
        Ok(next)
    }

    /// Allocate the next durable incarnation for held-search session handles.
    /// The lifecycle lock makes two service instances opened over the same
    /// storage allocate distinct epochs even inside one process.
    pub fn next_search_session_incarnation(&self) -> Result<u64> {
        let _lifecycle = self.search_lifecycle.lock().unwrap();
        let previous: u64 = self
            .get_local(&keyspace::search_session_incarnation_key())?
            .unwrap_or(0);
        let next = previous
            .checked_add(1)
            .ok_or_else(|| Error::Search("search session incarnation exhausted".into()))?;
        self.put_local(&keyspace::search_session_incarnation_key(), &next)?;
        Ok(next)
    }

    pub fn registration_binding(&self) -> Result<Option<RegistrationBinding>> {
        self.get_local(&keyspace::registration_binding_key())
    }

    /// Persist the registration identity before the process starts serving.
    /// Equal incarnations must be byte-identical and incarnations never move
    /// backwards, even if a caller presents a stale directory snapshot.
    pub fn record_registration_binding(&self, binding: &RegistrationBinding) -> Result<()> {
        let identity = self.identity()?.ok_or_else(|| {
            Error::Config("registration binding requires a bound node identity".into())
        })?;
        if binding.cluster_id != identity.cluster_id || binding.node_id != identity.node_id {
            return Err(Error::IdentityMismatch {
                found_cluster: identity.cluster_id,
                found_node: identity.node_id,
                want_cluster: binding.cluster_id,
                want_node: binding.node_id,
            });
        }
        if binding.directory_incarnation == 0 {
            return Err(Error::Config(
                "registration binding incarnation must be non-zero".into(),
            ));
        }
        if let Some(previous) = self.registration_binding()? {
            if binding.directory_incarnation < previous.directory_incarnation {
                return Err(Error::Config(format!(
                    "registration incarnation regressed from {} to {}",
                    previous.directory_incarnation, binding.directory_incarnation
                )));
            }
            if binding.directory_incarnation == previous.directory_incarnation {
                if binding == &previous {
                    return Ok(());
                }
                return Err(Error::Config(format!(
                    "registration endpoints conflict at incarnation {}",
                    binding.directory_incarnation
                )));
            }
        }
        self.put_local(&keyspace::registration_binding_key(), binding)
    }

    // ---- node-local default-CF records -------------------------------------

    /// Read a typed node-local record from the default CF.
    pub fn get_local<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
        match self.db.get(key)? {
            Some(bytes) => Ok(Some(codec::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Write a typed node-local record fsync-durable.
    pub fn put_local<T: Serialize>(&self, key: &[u8], value: &T) -> Result<()> {
        self.db.put_opt(key, codec::encode(value), &sync_write())?;
        Ok(())
    }

    pub fn delete_local(&self, key: &[u8]) -> Result<()> {
        self.db.delete_opt(key, &sync_write())?;
        Ok(())
    }

    fn delete_local_prefix(&self, prefix: &[u8]) -> Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        for item in self.db.iterator(rocksdb::IteratorMode::From(
            prefix,
            rocksdb::Direction::Forward,
        )) {
            let (key, _) = item?;
            if !key.starts_with(prefix) {
                break;
            }
            batch.delete(key);
        }
        if !batch.is_empty() {
            self.db.write_opt(batch, &sync_write())?;
        }
        Ok(())
    }

    // ---- column-family lifecycle -------------------------------------------

    /// Idempotently create both CFs for a group.
    pub fn ensure_group(&self, group: GroupId) -> Result<()> {
        let _lifecycle = self.snapshot_lifecycle.lock().unwrap();
        let log_name = group.cf_log();
        if self.db.cf_handle(&log_name).is_none() {
            self.db.create_cf(&log_name, &Options::default())?;
        }
        let state_name = self.active_state_cf_name(group)?;
        if self.db.cf_handle(&state_name).is_none() {
            if state_name != group.cf_state() {
                return Err(Error::Corrupt(
                    state_name,
                    "active snapshot state CF is missing".into(),
                ));
            }
            self.db.create_cf(&state_name, &Options::default())?;
        }
        self.apply_durability
            .register(group, self.recovered_raft_applied(group)?);
        Ok(())
    }

    /// Idempotently drop both CFs for a group (reclamation, DESIGN §7.4).
    pub fn drop_group(&self, group: GroupId) -> Result<()> {
        self.apply_durability.begin_close(group);
        self.apply_durability.ensure_drained(group)?;
        let _lifecycle = self.snapshot_lifecycle.lock().unwrap();
        self.active_state_cfs.lock().unwrap().remove(&group);
        let mut names =
            Db::list_cf(&Options::default(), &self.path).unwrap_or_else(|_| vec!["default".into()]);
        let state_prefix = format!("{}__snapshot_", group.cf_state());
        names.retain(|name| {
            name == &group.cf_log() || name == &group.cf_state() || name.starts_with(&state_prefix)
        });
        for name in names {
            if self.db.cf_handle(&name).is_some() {
                self.db.drop_cf(&name)?;
            }
        }
        self.delete_local(&keyspace::state_cf_pointer_key(group))?;
        self.apply_durability.remove(group);
        if matches!(group, GroupId::Data(_)) {
            self.delete_local_prefix(&keyspace::search_prefix(group))?;
            let index_path = self.path.join("search").join(group.token());
            if index_path.exists() {
                std::fs::remove_dir_all(index_path)?;
            }
        }
        Ok(())
    }

    /// This node's durable serving-gate state for a group (DESIGN §7.4). Lives in
    /// the default CF so it survives snapshot install and CF reclamation.
    pub fn serving_state(&self, group: GroupId) -> Result<Option<ServingState>> {
        self.get_local(&keyspace::serving_key(group))
    }

    pub fn set_serving_state(&self, group: GroupId, state: ServingState) -> Result<()> {
        self.put_local(&keyspace::serving_key(group), &state)
    }

    /// Refuse client-facing Raft work unless the durable local serving gate is
    /// open. This is checked on every serving path so a concurrent reclaim or
    /// administrative stop cannot leave a live runtime answering requests.
    pub fn require_serving(&self, group: GroupId) -> Result<()> {
        if self.serving_state(group)? != Some(ServingState::Serving) {
            return Err(Error::Raft(format!(
                "group {:?} is not permitted to serve on this node",
                group
            )));
        }
        self.require_group_healthy(group)
    }

    /// Database-wide fail-stop fence. Once a WAL write, WAL flush, dirty-age
    /// watchdog, or materialization invariant fails, every client and Raft I/O
    /// path must refuse further participation for the rest of this process.
    pub(crate) fn require_healthy(&self) -> Result<()> {
        self.apply_durability.ensure_healthy()
    }

    /// Convert a local Raft-storage failure into the database-wide fail-stop
    /// state before returning it to OpenRaft. Every Raft group shares this DB;
    /// allowing sibling groups to continue after one group observes an I/O or
    /// on-disk invariant failure would let them acknowledge work against a
    /// storage device whose durability is no longer trustworthy.
    pub(crate) fn poison_raft_error(&self, context: &str, error: Error) -> Error {
        self.apply_durability
            .fail_all(format!("{context}: {error}"));
        error
    }

    pub(crate) fn require_group_healthy(&self, group: GroupId) -> Result<()> {
        self.require_healthy()?;
        match self.apply_durability.group_failure(group) {
            Some(error) => Err(Error::Io(std::io::Error::other(error))),
            None => Ok(()),
        }
    }

    pub(crate) fn database_failure(&self) -> Option<String> {
        self.apply_durability.failure()
    }

    pub(crate) async fn wait_for_failure(&self) -> String {
        self.apply_durability.wait_for_failure().await
    }

    /// Reclaim a removed group's local data (DESIGN §7.4): record `NonServing`
    /// *before* dropping the CFs, so a crash mid-reclaim can never leave
    /// servable-looking state behind. After the drop, the absence of local Raft
    /// state itself enforces non-participation (the amnesia rule).
    pub fn reclaim_group(&self, group: GroupId) -> Result<()> {
        self.set_serving_state(group, ServingState::NonServing)?;
        self.drop_group(group)?;
        Ok(())
    }

    /// Async reclamation fence used by the runtime. Once closing begins, new
    /// state applies are rejected; already visible applies must become durable
    /// before either column family can be dropped.
    pub async fn reclaim_group_durable(&self, group: GroupId) -> Result<()> {
        self.set_serving_state(group, ServingState::NonServing)?;
        self.apply_durability.begin_close(group);
        self.apply_durability.drain(group).await?;
        self.drop_group(group)
    }

    /// Persist an admission that has just been verified against a live,
    /// linearizable meta plan. Unlike the low-level bootstrap helper, this may
    /// replace an older plan's admission and may reopen a reclaimed group.
    /// Admission and the serving-state transition are one durable local batch,
    /// so a crash cannot retain one without the other.
    pub fn record_verified_learner_admission(&self, record: &LearnerAdmission) -> Result<()> {
        let identity = self.identity()?.ok_or_else(|| {
            Error::Config("learner admission requires a bound node identity".into())
        })?;
        if identity.cluster_id != record.cluster_id {
            return Err(Error::IdentityMismatch {
                found_cluster: identity.cluster_id,
                found_node: identity.node_id,
                want_cluster: record.cluster_id,
                want_node: identity.node_id,
            });
        }

        let mut batch = rocksdb::WriteBatch::default();
        batch.put(keyspace::admission_key(record.group), codec::encode(record));
        batch.put(
            keyspace::serving_key(record.group),
            codec::encode(&ServingState::Serving),
        );
        self.db.write_opt(batch, &sync_write())?;
        Ok(())
    }

    pub fn group_exists(&self, group: GroupId) -> bool {
        self.active_state_cf_name(group)
            .ok()
            .and_then(|name| self.db.cf_handle(&name))
            .is_some()
            && self.db.cf_handle(&group.cf_log()).is_some()
    }

    /// Authorize a local Raft runtime to open `group`. Group state may be
    /// created only from a durable bootstrap record or learner admission. A
    /// group marked non-serving stays fenced unless a newly live-verified plan
    /// first records a fresh admission with
    /// [`Self::record_verified_learner_admission`].
    pub fn authorize_group_start(&self, group: GroupId, node_id: NodeId) -> Result<()> {
        if self.serving_state(group)? == Some(ServingState::NonServing) {
            return Err(Error::Raft(format!(
                "refusing to restart non-serving group {:?}",
                group
            )));
        }

        let identity = self
            .identity()?
            .ok_or_else(|| Error::Config("group startup requires a bound node identity".into()))?;
        if identity.node_id != node_id {
            return Err(Error::IdentityMismatch {
                found_cluster: identity.cluster_id,
                found_node: identity.node_id,
                want_cluster: identity.cluster_id,
                want_node: node_id,
            });
        }

        let bootstrap: Option<BootstrapGroup> = self.get_local(&keyspace::bootstrap_key(group))?;
        let admitted_by_bootstrap = bootstrap.is_some_and(|record| {
            record.cluster_id == identity.cluster_id
                && record.group == group
                && record.members.contains(&node_id)
        });
        let admission: Option<LearnerAdmission> =
            self.get_local(&keyspace::admission_key(group))?;
        let admitted_as_learner = admission.is_some_and(|record| {
            record.cluster_id == identity.cluster_id && record.group == group
        });

        if !(admitted_by_bootstrap || admitted_as_learner) {
            return Err(Error::Raft(format!(
                "refusing to create unadmitted group {:?}",
                group
            )));
        }

        self.ensure_group(group)?;
        self.set_serving_state(group, ServingState::Serving)?;
        Ok(())
    }

    pub(crate) fn state_cf(&self, group: GroupId) -> Result<Arc<BoundColumnFamily<'_>>> {
        // Keep the cache lock through `cf_handle`: once an installer switches
        // the cached name, no later caller can acquire a handle to the retired
        // generation, allowing it to be dropped after outstanding handles drain.
        let mut active = self.active_state_cfs.lock().unwrap();
        let name = self.active_state_cf_name_locked(group, &mut active)?;
        self.db
            .cf_handle(&name)
            .ok_or_else(|| Error::Corrupt(name, "state CF not open".into()))
    }

    fn active_state_cf_name(&self, group: GroupId) -> Result<String> {
        let mut active = self.active_state_cfs.lock().unwrap();
        self.active_state_cf_name_locked(group, &mut active)
    }

    fn active_state_cf_name_locked(
        &self,
        group: GroupId,
        active: &mut HashMap<GroupId, String>,
    ) -> Result<String> {
        if let Some(name) = active.get(&group) {
            return Ok(name.clone());
        }
        let name = self
            .get_local::<String>(&keyspace::state_cf_pointer_key(group))?
            .unwrap_or_else(|| group.cf_state());
        let base = group.cf_state();
        if name != base && !name.starts_with(&format!("{base}__snapshot_")) {
            return Err(Error::Corrupt(
                base,
                format!("invalid active state CF pointer {name:?}"),
            ));
        }
        active.insert(group, name.clone());
        Ok(name)
    }

    /// Discard snapshot scratch files left behind by a previous process.
    ///
    /// `SnapshotFile` unlinks its own file on drop, so these only survive a
    /// crash — but nothing else ever collects them, so without this they
    /// accumulate across crashes until the disk fills. Every file in this
    /// directory is scratch owned by whoever is running: the build and receive
    /// paths both unlink before any durable state refers to them, so none of it
    /// is load-bearing at open. Best-effort by design — a file that resists
    /// removal is a disk-space problem, never a correctness one, and must not
    /// keep the node from starting.
    fn cleanup_snapshot_temp_files(&self) {
        let dir = self.snapshot_temp_dir();
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "tmp")
                && let Err(error) = std::fs::remove_file(&path)
            {
                tracing::warn!(?path, %error, "failed to remove orphaned snapshot temp file");
            }
        }
    }

    fn cleanup_snapshot_cfs(&self, existing: &[String]) -> Result<()> {
        let mut active_generations = HashSet::new();
        let mut switched_families = Vec::new();
        let mut interrupted_reclaims = Vec::new();
        let prefix = keyspace::state_cf_pointer_prefix();
        for item in self.db.iterator(rocksdb::IteratorMode::From(
            prefix,
            rocksdb::Direction::Forward,
        )) {
            let (key, value) = item?;
            if !key.starts_with(prefix) {
                break;
            }
            let active = codec::decode::<String>(&value)?;
            let token = std::str::from_utf8(&key[prefix.len()..])
                .map_err(|error| Error::Corrupt("state CF pointer".into(), error.to_string()))?;
            // A pointer is written (sync-durable) only after its generation CF
            // exists and is complete, and only `drop_group` removes a
            // generation still named by its pointer — so a pointer naming a
            // missing CF means a reclaim crashed between the CF drops and the
            // pointer delete. Complete that reclaim below; left in place, the
            // dangling pointer would make `ensure_group` refuse every future
            // re-admission of the group on this node.
            if !existing.iter().any(|name| name == &active) {
                let group = GroupId::from_token(token).ok_or_else(|| {
                    Error::Corrupt(
                        "state CF pointer".into(),
                        format!("bad group token {token:?}"),
                    )
                })?;
                interrupted_reclaims.push(group);
                continue;
            }
            switched_families.push((format!("cf_state_{token}"), active.clone()));
            active_generations.insert(active);
        }
        for name in existing {
            let belongs_to_switched_family = switched_families.iter().any(|(base, active)| {
                name != active && (name == base || name.starts_with(&format!("{base}__snapshot_")))
            });
            if (name.contains("__snapshot_") && !active_generations.contains(name))
                || belongs_to_switched_family
            {
                self.db.drop_cf(name)?;
            }
        }
        // Finish interrupted reclamations (mirrors `drop_group`). The pointer
        // delete is deliberately last: it is the marker that re-runs this
        // cleanup after a crash mid-way through, and deleting it first would
        // let a later re-admission resurrect a stale base state CF.
        for group in interrupted_reclaims {
            for name in [group.cf_log(), group.cf_state()] {
                if self.db.cf_handle(&name).is_some() {
                    self.db.drop_cf(&name)?;
                }
            }
            if matches!(group, GroupId::Data(_)) {
                self.delete_local_prefix(&keyspace::search_prefix(group))?;
                let index_path = self.path.join("search").join(group.token());
                if index_path.exists() {
                    std::fs::remove_dir_all(index_path)?;
                }
            }
            self.delete_local(&keyspace::state_cf_pointer_key(group))?;
        }
        Ok(())
    }

    fn retire_state_cf(&self, name: &str, cf: Arc<BoundColumnFamily<'_>>) -> Result<()> {
        // `state_cf()` takes the active-name lock through handle acquisition, so
        // after the pointer switch no new handle to `name` can appear. Wait a
        // short bounded interval for reads already in progress; if a long scan
        // still owns the generation, startup cleanup will reclaim it safely.
        for _ in 0..100 {
            if Arc::strong_count(&cf) <= 2 {
                drop(cf);
                if self.db.cf_handle(name).is_some() {
                    self.db.drop_cf(name)?;
                }
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Ok(())
    }

    // Used by the Raft log store (M3).
    #[allow(dead_code)]
    pub(crate) fn log_cf(&self, group: GroupId) -> Result<Arc<BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(&group.cf_log())
            .ok_or_else(|| Error::Corrupt(group.cf_log(), "log CF not open".into()))
    }

    pub(crate) fn db(&self) -> &Db {
        &self.db
    }

    pub fn rocks_counters(&self) -> RocksCounters {
        let Some(options) = &self.profile_options else {
            return RocksCounters::default();
        };
        RocksCounters {
            wal_syncs: options.get_ticker_count(Ticker::WalFileSynced),
            wal_bytes: options.get_ticker_count(Ticker::WalFileBytes),
            stall_micros: options.get_ticker_count(Ticker::StallMicros),
        }
    }

    /// Ask the database worker to WAL-write a Raft log batch. Return once the
    /// entries are readable; complete `callback` only after the shared durable
    /// flush covers them.
    pub(crate) async fn write_log_batch(
        &self,
        batch: rocksdb::WriteBatch,
        wal_bytes: usize,
        callback: OnDurable,
    ) -> Result<()> {
        self.require_healthy()?;
        // Reserve first so bytes waiting for durability remain bounded even if
        // the disk becomes slower than Raft producers.
        let reservation = {
            let _profile = crate::perf::timer(WriteStage::WalCapacityWait);
            self.durability.reserve(wal_bytes).await?
        };
        let (written_tx, written_rx) = tokio::sync::oneshot::channel();
        if let Err(error) = self.durability.submit(
            reservation,
            batch,
            DurabilityKind::Log,
            Some(Box::new(move |result| {
                let _ = written_tx.send(result);
            })),
            callback,
        ) {
            return Err(
                self.poison_raft_error("Raft log durability submission failed", Error::Io(error))
            );
        }
        match written_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                Err(self.poison_raft_error("Raft log WAL write failed", Error::Io(error)))
            }
            Err(_) => Err(self.poison_raft_error(
                "Raft log durability worker stopped",
                Error::Io(std::io::Error::other("database write worker stopped")),
            )),
        }
    }

    /// Enqueue an atomic state-machine batch. Data groups return at the
    /// RocksDB-visible boundary; meta groups wait for WAL durability.
    pub(crate) async fn write_state_batch(
        &self,
        group: GroupId,
        log_id: RaftLogId,
        applied_entries: usize,
        batch: rocksdb::WriteBatch,
        wal_bytes: usize,
    ) -> Result<()> {
        self.require_healthy()?;
        // Admit against this group's dirty budget before taking database-wide
        // WAL capacity: holding shared capacity while parked on one group's
        // backlog would let that group's durability lag stall every sibling
        // group's log appends.
        self.apply_durability
            .begin(group, log_id, applied_entries, wal_bytes)
            .await?;
        let reservation = {
            let _profile = crate::perf::timer(WriteStage::StateApplyCapacityWait);
            match self.durability.reserve(wal_bytes).await {
                Ok(reservation) => reservation,
                Err(error) => {
                    self.apply_durability
                        .cancel(group, log_id, error.to_string());
                    return Err(Error::Io(error));
                }
            }
        };

        let written_tracker = self.apply_durability.clone();
        let written_log_id = log_id;
        let (written_tx, written_rx) = tokio::sync::oneshot::channel();
        let (durable_tx, durable_rx) = tokio::sync::oneshot::channel();
        let durable_tracker = self.apply_durability.clone();
        let submit_result = self.durability.submit(
            reservation,
            batch,
            DurabilityKind::State,
            Some(Box::new(move |result| {
                match &result {
                    Ok(()) => written_tracker.visible(group, written_log_id),
                    Err(error) => written_tracker.cancel(group, written_log_id, error.to_string()),
                }
                let _ = written_tx.send(result);
            })),
            Box::new(move |result| {
                match &result {
                    Ok(()) => durable_tracker.durable(group, log_id),
                    Err(error) => durable_tracker.cancel(group, log_id, error.to_string()),
                }
                let _ = durable_tx.send(result);
            }),
        );
        if let Err(error) = submit_result {
            self.apply_durability
                .cancel(group, log_id, error.to_string());
            return Err(Error::Io(error));
        }

        match written_rx.await {
            Ok(result) => result.map_err(Error::Io)?,
            Err(_) => {
                let error = "database write worker stopped".to_string();
                self.apply_durability.cancel(group, log_id, error.clone());
                return Err(Error::Io(std::io::Error::other(error)));
            }
        }

        if matches!(group, GroupId::Data(_)) {
            return Ok(());
        }

        let _wait = crate::perf::timer(WriteStage::StateApplyDurabilityWait);
        match durable_rx.await {
            Ok(result) => result.map_err(Error::Io),
            Err(_) => {
                let error = "database durability worker stopped".to_string();
                self.apply_durability.cancel(group, log_id, error.clone());
                Err(Error::Io(std::io::Error::other(error)))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn record_state_sync(&self, group: GroupId, log_id: RaftLogId) {
        self.apply_durability.visible(group, log_id);
        self.apply_durability.durable(group, log_id);
    }

    pub(crate) async fn wait_state_durable(
        &self,
        group: GroupId,
        log_id: &RaftLogId,
    ) -> Result<()> {
        self.apply_durability
            .wait_durable(group, log_id, DurabilityWaitKind::General)
            .await
    }

    pub(crate) async fn wait_state_durable_for_purge(
        &self,
        group: GroupId,
        log_id: &RaftLogId,
    ) -> Result<()> {
        self.apply_durability
            .wait_durable(group, log_id, DurabilityWaitKind::Purge)
            .await
    }

    pub(crate) async fn wait_state_durable_for_snapshot(
        &self,
        group: GroupId,
        log_id: &RaftLogId,
    ) -> Result<()> {
        self.apply_durability
            .wait_durable(group, log_id, DurabilityWaitKind::Snapshot)
            .await
    }

    pub(crate) fn state_durability(&self, group: GroupId) -> ApplyDurabilitySnapshot {
        self.apply_durability.snapshot(group)
    }

    pub(crate) fn state_recovery_ready(&self, group: GroupId) -> bool {
        self.apply_durability.recovery_ready(group)
    }

    pub(crate) fn begin_state_recovery(&self, group: GroupId, target: Option<RaftLogId>) -> u64 {
        self.apply_durability.begin_recovery_attempt(group, target)
    }

    pub(crate) async fn wait_state_visible(
        &self,
        group: GroupId,
        target: &RaftLogId,
    ) -> Result<()> {
        self.apply_durability.wait_visible(group, target).await
    }

    pub(crate) fn mark_state_recovery_ready(
        &self,
        group: GroupId,
        epoch: u64,
        target: Option<&RaftLogId>,
    ) -> Result<()> {
        self.apply_durability
            .mark_recovery_ready(group, epoch, target)
    }

    pub(crate) fn record_state_installed(&self, group: GroupId, log_id: Option<RaftLogId>) {
        self.apply_durability.installed(group, log_id);
    }

    pub(crate) fn validate_state_install(
        &self,
        group: GroupId,
        log_id: Option<&RaftLogId>,
    ) -> Result<()> {
        self.apply_durability.validate_install(group, log_id)
    }

    // ---- state-CF reads (writes go through the batch helper) ---------------

    /// Raw read of a state-CF key.
    pub fn get_state(&self, group: GroupId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let cf = self.state_cf(group)?;
        Ok(self.db.get_cf(&cf, key)?)
    }

    /// Typed read of a state-CF record.
    pub fn get_state_record<T: DeserializeOwned>(
        &self,
        group: GroupId,
        key: &[u8],
    ) -> Result<Option<T>> {
        match self.get_state(group, key)? {
            Some(bytes) => Ok(Some(codec::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// The group's durable `last_applied`, or `None` before the first apply.
    pub fn last_applied(&self, group: GroupId) -> Result<Option<LogId>> {
        self.get_state_record(group, &keyspace::last_applied_key())
    }

    /// Full key/value dump of a group's state CF, for snapshot building (M3).
    pub fn scan_state(&self, group: GroupId) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let cf = self.state_cf(group)?;
        let mut out = Vec::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (k, v) = item?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    /// Capture a stable state view in O(1); callers may release the Raft apply
    /// mutex before streaming its records to disk.
    pub(crate) fn state_snapshot(&self, group: GroupId) -> Result<StateSnapshot<'_>> {
        let cf = self.state_cf(group)?;
        Ok(StateSnapshot {
            snapshot: self.db.snapshot(),
            cf,
        })
    }

    pub(crate) fn snapshot_temp_dir(&self) -> PathBuf {
        self.path.join("raft-snapshots")
    }

    pub(crate) fn begin_state_install(&self, group: GroupId) -> Result<StateInstaller<'_>> {
        let _lifecycle = self.snapshot_lifecycle.lock().unwrap();
        let generation = self
            .get_local::<u64>(&keyspace::state_cf_generation_key(group))?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Error::Config("snapshot state generation exhausted".into()))?;
        self.put_local(&keyspace::state_cf_generation_key(group), &generation)?;
        let name = format!("{}__snapshot_{generation}", group.cf_state());
        if self.db.cf_handle(&name).is_some() {
            return Err(Error::Corrupt(
                name,
                "new snapshot state generation already exists".into(),
            ));
        }
        self.db.create_cf(&name, &Options::default())?;
        self.db.cf_handle(&name).ok_or_else(|| {
            Error::Corrupt(name.clone(), "created snapshot state CF is missing".into())
        })?;
        Ok(StateInstaller {
            storage: self,
            group,
            name,
            batch: rocksdb::WriteBatch::default(),
            last_key: None,
            activated: false,
        })
    }

    /// Current local search projection epoch. Epoch one is implicit on stores
    /// created before search was enabled and is persisted with the first dirty
    /// record or snapshot install.
    pub fn search_projection_epoch(&self, group: GroupId) -> Result<u64> {
        if !matches!(group, GroupId::Data(_)) {
            return Err(Error::Search(
                "search projection requires a data group".into(),
            ));
        }
        Ok(self
            .get_local::<u64>(&keyspace::search_epoch_key(group))?
            .unwrap_or(1))
    }

    /// Scan one epoch of the persistent local dirty-key outbox in source-log
    /// order. Values are intentionally empty; source values are read from the
    /// authoritative state CF.
    pub fn scan_search_outbox(
        &self,
        group: GroupId,
        epoch: u64,
    ) -> Result<Vec<crate::search::SearchOutboxEntry>> {
        let prefix = keyspace::search_outbox_epoch_prefix(group, epoch);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &prefix,
            rocksdb::Direction::Forward,
        ));
        let mut entries = Vec::new();
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            entries.push(crate::search::decode_outbox_key(group, &key)?);
        }
        Ok(entries)
    }

    /// Register an outbox consumer. `created` tells a failed install whether it
    /// may unregister the record again or whether it belongs to an earlier,
    /// healthy install; `needs_rebuild` is derived from the durable record so
    /// a crash between registration and the first committed rebuild cannot
    /// resurrect a stale on-disk index over a pruned outbox gap.
    pub fn register_search_consumer(
        &self,
        group: GroupId,
        name: &str,
        generation: &crate::search::SearchIndexGeneration,
    ) -> Result<crate::search::SearchConsumerRegistration> {
        let _lifecycle = self.search_lifecycle.lock().unwrap();
        crate::search::validate_index_name(name)?;
        generation.validate_identity()?;
        let key = keyspace::search_consumer_key(group, name, generation.id);
        if let Some(existing) = self.get_local::<crate::search::SearchConsumerState>(&key)? {
            if existing.definition_hash != generation.definition_hash
                || existing.engine_revision != generation.engine_revision
            {
                return Err(Error::Corrupt(
                    "search consumer".into(),
                    "consumer identity differs from catalog generation".into(),
                ));
            }
            return Ok(crate::search::SearchConsumerRegistration {
                created: false,
                needs_rebuild: existing.checkpoint.is_none(),
            });
        }
        self.put_local(
            &key,
            &crate::search::SearchConsumerState {
                definition_hash: generation.definition_hash,
                engine_revision: generation.engine_revision,
                checkpoint: None,
                retention_floor: None,
                needs_rebuild: false,
            },
        )?;
        Ok(crate::search::SearchConsumerRegistration {
            created: true,
            needs_rebuild: true,
        })
    }

    /// Record the retention floor of a rebuild pass: the `(epoch, applied)`
    /// point of the source snapshot it will project from. Safe to write for a
    /// released (`needs_rebuild`) consumer — the floor never grants retention,
    /// it only narrows what an unreleased builder pins — and a floor left by
    /// an abandoned pass stays safe because a later pass snapshots at a point
    /// at least as high. A missing record is a benign race with
    /// unregistration; the pass fails later at its checkpoint write.
    pub fn record_search_consumer_retention_floor(
        &self,
        group: GroupId,
        name: &str,
        generation: u64,
        floor: (u64, u64),
    ) -> Result<()> {
        let _lifecycle = self.search_lifecycle.lock().unwrap();
        let key = keyspace::search_consumer_key(group, name, generation);
        let Some(mut state) = self.get_local::<crate::search::SearchConsumerState>(&key)? else {
            return Ok(());
        };
        state.retention_floor = Some(floor);
        self.put_local(&key, &state)
    }

    /// Whether this consumer was marked past its lag budget and owes a full
    /// rebuild before it may advance again.
    pub fn search_consumer_needs_rebuild(
        &self,
        group: GroupId,
        name: &str,
        generation: u64,
    ) -> Result<bool> {
        let key = keyspace::search_consumer_key(group, name, generation);
        Ok(self
            .get_local::<crate::search::SearchConsumerState>(&key)?
            .is_some_and(|state| state.needs_rebuild))
    }

    /// The checkpoint currently recorded for a consumer, which is what pins
    /// outbox retention — distinct from the checkpoint inside the loaded
    /// Tantivy commit, which is the projection's own truth (I4). The two are
    /// equal in steady state and diverge only between a commit and the record
    /// write that follows it.
    pub fn search_consumer_checkpoint(
        &self,
        group: GroupId,
        name: &str,
        generation: u64,
    ) -> Result<Option<crate::search::IndexCheckpoint>> {
        let key = keyspace::search_consumer_key(group, name, generation);
        Ok(self
            .get_local::<crate::search::SearchConsumerState>(&key)?
            .and_then(|state| state.checkpoint))
    }

    pub fn record_search_consumer_checkpoint(
        &self,
        group: GroupId,
        name: &str,
        generation: u64,
        checkpoint: crate::search::IndexCheckpoint,
    ) -> Result<()> {
        // Same lock as unregister/prune: an unlocked get-then-put here could
        // interleave with unregistration and recreate a deleted record, leaving
        // a ghost consumer that pins outbox retention forever.
        let _lifecycle = self.search_lifecycle.lock().unwrap();
        let key = keyspace::search_consumer_key(group, name, generation);
        let Some(mut state) = self.get_local::<crate::search::SearchConsumerState>(&key)? else {
            return Err(Error::Corrupt(
                "search consumer".into(),
                "checkpoint written for an unregistered consumer".into(),
            ));
        };
        if state.definition_hash != checkpoint.definition_hash
            || state.engine_revision != checkpoint.engine_revision
        {
            return Err(Error::Corrupt(
                "search consumer".into(),
                "checkpoint identity differs from consumer".into(),
            ));
        }
        // The consumer was marked for rebuild after this pass took its source
        // snapshot, which means retention was released and the journal it was
        // reading may have been truncated underneath it. This applies to full
        // rebuilds too: `begin_search_consumer_rebuild` clears the flag before
        // snapshotting, so seeing it set here proves the rebuild was released
        // again while it was running. Refuse the checkpoint and retry from a
        // fresh authoritative snapshot.
        if state.needs_rebuild {
            return Err(Error::Search(
                "search consumer was released from retention while projection was running".into(),
            ));
        }
        state.checkpoint = Some(checkpoint);
        state.needs_rebuild = false;
        self.put_local(&key, &state)
    }

    /// Enforce the outbox lag budget for a group (design §6.5). When the
    /// retained journal exceeds its entry or byte limit, the consumer holding
    /// the oldest watermark is marked for rebuild and stops pinning retention,
    /// then the newly unblocked prefix is pruned. One consumer is released per
    /// call; the caller re-evaluates on its next pass.
    ///
    /// An unhealthy derived index must never grow the outbox without bound and
    /// exhaust the disk the authoritative database depends on.
    pub fn enforce_search_outbox_bounds(
        &self,
        group: GroupId,
        max_entries: u64,
        max_bytes: u64,
    ) -> Result<Option<(String, u64)>> {
        let (entries, bytes) = self.search_outbox_usage(group)?;
        if entries <= max_entries && bytes <= max_bytes {
            return Ok(None);
        }
        let laggard = {
            let _lifecycle = self.search_lifecycle.lock().unwrap();
            let epoch = self.search_projection_epoch(group)?;
            let mut laggard: Option<(Vec<u8>, crate::search::SearchConsumerState, u64)> = None;
            for (key, state) in self.search_consumers(group)? {
                if !state.holds_retention() {
                    continue;
                }
                // A builder without a checkpoint retains only above its
                // current-epoch snapshot floor; without one it retains
                // everything and is the most constraining watermark there is.
                // Using the floor keeps a fresh backfill from being released
                // (and restarted forever) when a checkpointed consumer is the
                // one actually pinning the journal.
                let watermark = match (&state.checkpoint, state.retention_floor) {
                    (Some(checkpoint), _) => checkpoint
                        .source_log_id
                        .as_ref()
                        .map(|log_id| log_id.index)
                        .unwrap_or(0),
                    (None, Some((floor_epoch, floor))) if floor_epoch == epoch => floor,
                    (None, _) => 0,
                };
                if laggard
                    .as_ref()
                    .is_none_or(|(_, _, lowest)| watermark < *lowest)
                {
                    laggard = Some((key, state, watermark));
                }
            }
            match laggard {
                Some((key, mut state, _)) => {
                    state.needs_rebuild = true;
                    state.checkpoint = None;
                    state.retention_floor = None;
                    self.put_local(&key, &state)?;
                    Some(key)
                }
                None => None,
            }
        };
        let Some(key) = laggard else {
            return Ok(None);
        };
        let identity = keyspace::decode_search_consumer_key(group, &key)?;
        tracing::warn!(
            ?group,
            index = identity.0,
            generation = identity.1,
            entries,
            bytes,
            "search outbox exceeded its budget; marking the slowest index for rebuild"
        );
        self.prune_search_outbox(group)?;
        Ok(Some(identity))
    }

    /// Retained outbox size for a group, counting encoded key bytes.
    pub fn search_outbox_usage(&self, group: GroupId) -> Result<(u64, u64)> {
        let prefix = keyspace::search_outbox_prefix(group);
        let mut entries = 0u64;
        let mut bytes = 0u64;
        for item in self.db.iterator(rocksdb::IteratorMode::From(
            &prefix,
            rocksdb::Direction::Forward,
        )) {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            entries += 1;
            bytes += key.len() as u64;
        }
        Ok((entries, bytes))
    }

    fn search_consumers(
        &self,
        group: GroupId,
    ) -> Result<Vec<(Vec<u8>, crate::search::SearchConsumerState)>> {
        let prefix = keyspace::search_consumer_prefix(group);
        let mut consumers = Vec::new();
        for item in self.db.iterator(rocksdb::IteratorMode::From(
            &prefix,
            rocksdb::Direction::Forward,
        )) {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            consumers.push((
                key.to_vec(),
                codec::decode::<crate::search::SearchConsumerState>(&value)?,
            ));
        }
        Ok(consumers)
    }

    /// Pin this consumer's outbox retention before taking a full rebuild
    /// snapshot. Clearing the durable watermark first is essential when the
    /// local index checkpoint is ahead of recovered source state: otherwise a
    /// concurrent prune could trust the stale watermark and delete mutations
    /// that occur after the rebuild snapshot.
    pub fn begin_search_consumer_rebuild(
        &self,
        group: GroupId,
        name: &str,
        generation: &crate::search::SearchIndexGeneration,
    ) -> Result<()> {
        let _lifecycle = self.search_lifecycle.lock().unwrap();
        let key = keyspace::search_consumer_key(group, name, generation.id);
        let Some(mut state) = self.get_local::<crate::search::SearchConsumerState>(&key)? else {
            return Err(Error::Corrupt(
                "search consumer".into(),
                "rebuild started for an unregistered consumer".into(),
            ));
        };
        if state.definition_hash != generation.definition_hash
            || state.engine_revision != generation.engine_revision
        {
            return Err(Error::Corrupt(
                "search consumer".into(),
                "rebuild identity differs from consumer".into(),
            ));
        }
        state.checkpoint = None;
        // A lag-budget release sets this flag and removes the consumer from
        // retention. Re-acquire retention *before* taking the rebuild snapshot,
        // otherwise writes concurrent with Tantivy indexing can be pruned and
        // then skipped permanently by the rebuilt checkpoint.
        state.needs_rebuild = false;
        self.put_local(&key, &state)?;
        Ok(())
    }

    pub fn unregister_search_consumer(
        &self,
        group: GroupId,
        name: &str,
        generation: u64,
    ) -> Result<()> {
        {
            let _lifecycle = self.search_lifecycle.lock().unwrap();
            self.delete_local(&keyspace::search_consumer_key(group, name, generation))?;
        }
        self.prune_search_outbox(group)?;
        Ok(())
    }

    /// Delete only the outbox prefix covered by every registered local
    /// generation. No-consumer groups may discard all entries because a newly
    /// registered generation starts with a full state snapshot.
    pub fn prune_search_outbox(&self, group: GroupId) -> Result<usize> {
        let _lifecycle = self.search_lifecycle.lock().unwrap();
        // A consumer awaiting rebuild has forfeited its claim: it will
        // re-derive its documents from a full authoritative scan, so retaining
        // its gap would pin disk for nothing (design §6.5).
        let consumers: Vec<crate::search::SearchConsumerState> = self
            .search_consumers(group)?
            .into_iter()
            .map(|(_, state)| state)
            .filter(|state| state.holds_retention())
            .collect();
        let epoch = self.search_projection_epoch(group)?;
        let mut minimum: Option<u64> = None;
        for consumer in &consumers {
            let watermark = match (&consumer.checkpoint, consumer.retention_floor) {
                (Some(checkpoint), _) => {
                    if checkpoint.projection_epoch != epoch {
                        return Ok(0);
                    }
                    // A consumer that has committed a checkpoint but not yet
                    // projected any Raft entry still needs every retained
                    // entry: it must count as a watermark of zero, never be
                    // skipped.
                    match checkpoint.source_log_id.as_ref() {
                        Some(log_id) => log_id.index,
                        None => return Ok(0),
                    }
                }
                // A builder's rebuild pass projects from its authoritative
                // source snapshot, so entries at or below its recorded
                // current-epoch floor are covered (I5/I7) and prunable even
                // before its first checkpoint exists.
                (None, Some((floor_epoch, floor))) if floor_epoch == epoch => floor,
                // No checkpoint and no usable floor: retain everything.
                (None, _) => return Ok(0),
            };
            minimum = Some(minimum.map_or(watermark, |lowest| lowest.min(watermark)));
        }

        let prefix = keyspace::search_outbox_prefix(group);
        let mut batch = rocksdb::WriteBatch::default();
        let mut deleted = 0usize;
        for item in self.db.iterator(rocksdb::IteratorMode::From(
            &prefix,
            rocksdb::Direction::Forward,
        )) {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let entry = crate::search::decode_outbox_key(group, &key)?;
            let covered = consumers.is_empty()
                || entry.epoch < epoch
                || (entry.epoch == epoch
                    && minimum.is_some_and(|minimum| entry.source_log_id.index <= minimum));
            if !covered {
                // Entries sort by epoch then source index, and coverage is
                // monotonic in that order, so the first uncovered entry ends
                // the retained prefix.
                break;
            }
            batch.delete(key);
            deleted += 1;
        }
        if deleted != 0 {
            self.db.write_opt(batch, &sync_write())?;
        }
        Ok(deleted)
    }

    /// Capture the state rows, projection epoch, and applied prefix from one
    /// database-wide RocksDB snapshot. This prevents a backfill from pairing
    /// state from one prefix with an outbox/applied marker from another.
    ///
    /// `incremental_after` picks the shape. `None` loads every user row, which
    /// is what a rebuild needs. `Some(index)` loads only the rows the outbox
    /// marks dirty in `(index, applied]` — a caught-up index needs nothing
    /// else, and scanning the whole partition per catch-up would make the cost
    /// of a search scale with the partition rather than with the change set.
    pub fn search_source_snapshot(
        &self,
        group: GroupId,
        incremental_after: Option<u64>,
    ) -> Result<crate::search::SearchSourceSnapshot> {
        if !matches!(group, GroupId::Data(_)) {
            return Err(Error::Search(
                "search projection requires a data group".into(),
            ));
        }
        // The state CF handle and the RocksDB snapshot must describe the same
        // generation. A snapshot install switches the CF pointer and bumps the
        // projection epoch in one write, so resolving the handle outside this
        // lock lets an install land in between: the handle would still name the
        // pre-install CF while the snapshot already reports the post-install
        // epoch, and the projection would then commit pre-install documents
        // under the new epoch — accepted by `validate_checkpoint`, violating
        // both the exact-prefix (I5) and epoch-fence (I8) invariants.
        //
        // `StateInstaller::finish` takes these in the same order.
        let _lifecycle = self.snapshot_lifecycle.lock().unwrap();
        let cf = self.state_cf(group)?;
        let snapshot = self.db.snapshot();
        drop(_lifecycle);
        let epoch = match snapshot.get(keyspace::search_epoch_key(group))? {
            Some(bytes) => codec::decode(&bytes)?,
            None => 1,
        };
        let applied = match snapshot.get_cf(&cf, keyspace::raft_applied_key())? {
            Some(bytes) => {
                let (applied, _membership): RaftApplied = codec::decode(&bytes)?;
                applied
            }
            None => None,
        };
        let outbox_prefix = keyspace::search_outbox_epoch_prefix(group, epoch);
        let mut outbox = Vec::new();
        for item in snapshot.iterator(rocksdb::IteratorMode::From(
            &outbox_prefix,
            rocksdb::Direction::Forward,
        )) {
            let (key, _) = item?;
            if !key.starts_with(&outbox_prefix) {
                break;
            }
            outbox.push(crate::search::decode_outbox_key(group, &key)?);
        }

        let mut records = Vec::new();
        match incremental_after {
            None => {
                for item in snapshot.iterator_cf(
                    &cf,
                    rocksdb::IteratorMode::From(&[keyspace::TAG_USER], rocksdb::Direction::Forward),
                ) {
                    let (key, value) = item?;
                    let Some(user_key) = key.strip_prefix(&[keyspace::TAG_USER]) else {
                        break;
                    };
                    let record: crate::partition::state_machine::KeyRecord = codec::decode(&value)?;
                    records.push((user_key.to_vec(), record.version, record.value));
                }
            }
            Some(after) => {
                let through = applied.as_ref().map(|log_id| log_id.index).unwrap_or(0);
                outbox.retain(|entry| {
                    entry.source_log_id.index > after && entry.source_log_id.index <= through
                });
                let mut loaded = std::collections::HashSet::new();
                for entry in &outbox {
                    if !loaded.insert(entry.user_key.clone()) {
                        continue;
                    }
                    let mut state_key = Vec::with_capacity(entry.user_key.len() + 1);
                    state_key.push(keyspace::TAG_USER);
                    state_key.extend_from_slice(&entry.user_key);
                    // A key absent from the snapshot was deleted; the caller
                    // projects that as a document removal.
                    let Some(value) = snapshot.get_cf(&cf, &state_key)? else {
                        continue;
                    };
                    let record: crate::partition::state_machine::KeyRecord = codec::decode(&value)?;
                    records.push((entry.user_key.clone(), record.version, record.value));
                }
            }
        }
        Ok(crate::search::SearchSourceSnapshot {
            epoch,
            applied,
            records,
            outbox,
        })
    }

    /// Atomically replace a group's replicated state from a snapshot using a
    /// bounded staged generation. This compatibility helper accepts an existing
    /// slice; the Raft path streams records directly into the same installer.
    pub fn install_state(
        &self,
        group: GroupId,
        pairs: &[(Vec<u8>, Vec<u8>)],
        applied_record: &[u8],
    ) -> Result<()> {
        let mut installer = self.begin_state_install(group)?;
        for (k, v) in pairs {
            installer.put(k.clone(), v.clone())?;
        }
        installer.finish(applied_record)
    }

    fn recovered_raft_applied(&self, group: GroupId) -> Result<Option<RaftLogId>> {
        let Some(bytes) = self.raft_applied(group)? else {
            return Ok(None);
        };
        let (last_applied, _membership): RaftApplied = codec::decode(&bytes)?;
        Ok(last_applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;
    use std::sync::mpsc;
    use std::time::Duration;

    fn delayed_config(delay: Duration) -> DurabilityConfig {
        DurabilityConfig {
            max_pending_requests: 8,
            max_pending_bytes: 1_024,
            max_batch_delay: delay,
            adaptive_batching: false,
        }
    }

    fn raft_log_id(term: u64, index: u64) -> RaftLogId {
        RaftLogId::new(CommittedLeaderId::new(term, 1), index)
    }

    #[tokio::test]
    async fn log_batch_is_readable_before_its_durable_callback() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            Storage::open_with_durability(dir.path(), delayed_config(Duration::from_millis(100)))
                .unwrap();
        let (tx, rx) = mpsc::channel();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put(b"log-key", b"log-value");

        storage
            .write_log_batch(batch, 18, Box::new(move |result| tx.send(result).unwrap()))
            .await
            .unwrap();

        assert_eq!(storage.db().get(b"log-key").unwrap().unwrap(), b"log-value");
        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap().is_ok());
    }

    #[tokio::test]
    async fn dropping_storage_durably_drains_pending_log_callbacks() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            Storage::open_with_durability(dir.path(), delayed_config(Duration::from_secs(10)))
                .unwrap();
        let (tx, rx) = mpsc::channel();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put(b"shutdown-key", b"shutdown-value");
        storage
            .write_log_batch(batch, 26, Box::new(move |result| tx.send(result).unwrap()))
            .await
            .unwrap();

        drop(storage);

        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap().is_ok());
        let reopened = Storage::open(dir.path()).unwrap();
        assert_eq!(
            reopened.db().get(b"shutdown-key").unwrap().unwrap(),
            b"shutdown-value"
        );
    }

    #[tokio::test]
    async fn data_state_returns_visible_before_wal_durability() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            Storage::open_with_durability(dir.path(), delayed_config(Duration::from_millis(250)))
                .unwrap();
        let group = GroupId::Data(7);
        storage.ensure_group(group).unwrap();
        let cf = storage.state_cf(group).unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, b"state-key", b"state-value");
        let wal_bytes = batch.size_in_bytes();
        let log_id = raft_log_id(2, 11);

        storage
            .write_state_batch(group, log_id, 1, batch, wal_bytes)
            .await
            .unwrap();

        assert_eq!(
            storage.get_state(group, b"state-key").unwrap().unwrap(),
            b"state-value"
        );
        let visible = storage.state_durability(group);
        assert_eq!(visible.visible, Some(log_id));
        assert_eq!(visible.durable, None);
        assert_eq!(visible.pending_entries, 1);

        storage.wait_state_durable(group, &log_id).await.unwrap();
        let durable = storage.state_durability(group);
        assert_eq!(durable.durable, Some(log_id));
        assert_eq!(durable.pending_entries, 0);
        assert_eq!(durable.pending_bytes, 0);
    }

    #[tokio::test]
    async fn meta_state_stays_synchronous() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            Storage::open_with_durability(dir.path(), delayed_config(Duration::from_millis(50)))
                .unwrap();
        storage.ensure_group(GroupId::Meta).unwrap();
        let cf = storage.state_cf(GroupId::Meta).unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, b"meta-key", b"meta-value");
        let wal_bytes = batch.size_in_bytes();
        let log_id = raft_log_id(1, 4);

        storage
            .write_state_batch(GroupId::Meta, log_id, 1, batch, wal_bytes)
            .await
            .unwrap();

        let state = storage.state_durability(GroupId::Meta);
        assert_eq!(state.visible, Some(log_id));
        assert_eq!(state.durable, Some(log_id));
        assert_eq!(state.pending_entries, 0);
    }

    #[tokio::test]
    async fn reclaim_refuses_to_drop_a_group_with_an_unflushed_state_apply() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            Storage::open_with_durability(dir.path(), delayed_config(Duration::from_millis(250)))
                .unwrap();
        let group = GroupId::Data(9);
        storage.ensure_group(group).unwrap();
        let cf = storage.state_cf(group).unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, b"state-key", b"state-value");
        let wal_bytes = batch.size_in_bytes();
        let log_id = raft_log_id(3, 8);
        storage
            .write_state_batch(group, log_id, 1, batch, wal_bytes)
            .await
            .unwrap();

        assert!(storage.reclaim_group(group).is_err());
        assert!(storage.group_exists(group));

        storage.wait_state_durable(group, &log_id).await.unwrap();
        storage.reclaim_group(group).unwrap();
        assert!(!storage.group_exists(group));
    }

    #[tokio::test]
    async fn async_reclamation_closes_admission_and_waits_for_state_durability() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            Storage::open_with_durability(dir.path(), delayed_config(Duration::from_millis(200)))
                .unwrap(),
        );
        let group = GroupId::Data(10);
        storage.ensure_group(group).unwrap();
        let cf = storage.state_cf(group).unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, b"state-key", b"state-value");
        let wal_bytes = batch.size_in_bytes();
        storage
            .write_state_batch(group, raft_log_id(4, 9), 1, batch, wal_bytes)
            .await
            .unwrap();

        let reclaimer = storage.clone();
        let reclaim = tokio::spawn(async move { reclaimer.reclaim_group_durable(group).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!reclaim.is_finished());
        assert!(
            storage
                .apply_durability
                .begin(group, raft_log_id(4, 10), 1, 1)
                .await
                .is_err(),
            "closing must reject a newly admitted state batch"
        );

        reclaim.await.unwrap().unwrap();
        assert!(!storage.group_exists(group));
        assert_eq!(
            storage.serving_state(group).unwrap(),
            Some(ServingState::NonServing)
        );
    }

    #[tokio::test]
    async fn per_group_dirty_limit_backpressures_until_flush() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(
            Storage::open_limited_test(
                dir.path(),
                Duration::from_millis(200),
                ApplyDurabilityLimits {
                    max_pending_entries: 1,
                    max_pending_bytes: 1_024,
                    max_dirty_age: Duration::from_secs(2),
                },
            )
            .unwrap(),
        );
        let group = GroupId::Data(11);
        storage.ensure_group(group).unwrap();
        let cf = storage.state_cf(group).unwrap();

        let mut first = rocksdb::WriteBatch::default();
        first.put_cf(&cf, b"first", b"visible");
        let bytes = first.size_in_bytes();
        storage
            .write_state_batch(group, raft_log_id(1, 1), 1, first, bytes)
            .await
            .unwrap();

        let second_storage = storage.clone();
        let second = tokio::spawn(async move {
            let (batch, bytes) = {
                let cf = second_storage.state_cf(group).unwrap();
                let mut batch = rocksdb::WriteBatch::default();
                batch.put_cf(&cf, b"second", b"after-flush");
                let bytes = batch.size_in_bytes();
                (batch, bytes)
            };
            second_storage
                .write_state_batch(group, raft_log_id(1, 2), 1, batch, bytes)
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !second.is_finished(),
            "entry limit did not apply backpressure"
        );
        second.await.unwrap().unwrap();
        let snapshot = storage.state_durability(group);
        assert!(snapshot.last_flush_latency_ms.is_some());
        assert!(snapshot.max_flush_latency_ms >= snapshot.last_flush_latency_ms.unwrap());
    }

    #[tokio::test]
    async fn dirty_age_watchdog_fails_storage_closed() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open_limited_test(
            dir.path(),
            Duration::from_millis(500),
            ApplyDurabilityLimits {
                max_pending_entries: 8,
                max_pending_bytes: 1_024,
                max_dirty_age: Duration::from_millis(40),
            },
        )
        .unwrap();
        let group = GroupId::Data(12);
        storage.ensure_group(group).unwrap();
        let cf = storage.state_cf(group).unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, b"dirty", b"visible");
        let bytes = batch.size_in_bytes();
        storage
            .write_state_batch(group, raft_log_id(1, 1), 1, batch, bytes)
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if storage.state_durability(group).failed.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let state = storage.state_durability(group);
        assert!(state.failed.unwrap().contains("dirty-age limit"));
        assert!(state.flush_failures > 0 || state.oldest_pending_ms.is_some());
    }

    #[test]
    fn abrupt_exit_child_writes_visible_batch() {
        let Ok(path) = std::env::var("DAL_TEST_ABRUPT_STATE_DIR") else {
            return;
        };
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let storage = Storage::open_delayed_test(&path, Duration::from_secs(10)).unwrap();
            let group = GroupId::Data(13);
            storage.ensure_group(group).unwrap();
            let state_cf = storage.state_cf(group).unwrap();
            let log_cf = storage.log_cf(group).unwrap();
            let mut batch = rocksdb::WriteBatch::default();
            batch.put_cf(&state_cf, b"business", b"visible");
            batch.put_cf(&log_cf, b"crash-hint", b"same-batch");
            let bytes = batch.size_in_bytes();
            storage
                .write_state_batch(group, raft_log_id(1, 1), 1, batch, bytes)
                .await
                .unwrap();
            assert_eq!(
                storage.get_state(group, b"business").unwrap(),
                Some(b"visible".to_vec())
            );
            // Deliberately bypass every destructor and the durability worker's
            // drain path, modelling an abrupt process crash after visibility.
            std::process::exit(86);
        });
    }

    #[test]
    fn abrupt_process_exit_reopens_to_an_atomic_state_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let test_binary = std::env::current_exe().unwrap();
        let status = std::process::Command::new(test_binary)
            .args([
                "--exact",
                "storage::rocks::tests::abrupt_exit_child_writes_visible_batch",
                "--nocapture",
            ])
            .env("DAL_TEST_ABRUPT_STATE_DIR", dir.path())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86));

        let storage = Storage::open(dir.path()).unwrap();
        let group = GroupId::Data(13);
        storage.ensure_group(group).unwrap();
        let state = storage.get_state(group, b"business").unwrap();
        let log_cf = storage.log_cf(group).unwrap();
        let hint = storage.db().get_cf(&log_cf, b"crash-hint").unwrap();
        assert_eq!(
            state.is_some(),
            hint.is_some(),
            "cross-CF WriteBatch recovered as a partial entry"
        );
        if let Some(state) = state {
            assert_eq!(state, b"visible");
            assert_eq!(hint.unwrap(), b"same-batch");
        }
    }
}
