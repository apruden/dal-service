//! RocksDB handle and per-group column-family lifecycle (DESIGN §6).
//!
//! One RocksDB instance per node. Two CFs per Raft group
//! (`cf_log_<group>` / `cf_state_<group>`) created and dropped on demand; the
//! default CF holds node-local authority state that must survive both snapshot
//! install and CF reclamation.

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
        Ok(Storage {
            durability,
            apply_durability,
            db,
            profile_options: crate::perf::write_path_enabled().then_some(opts),
            path,
        })
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

    // ---- column-family lifecycle -------------------------------------------

    /// Idempotently create both CFs for a group.
    pub fn ensure_group(&self, group: GroupId) -> Result<()> {
        for name in [group.cf_log(), group.cf_state()] {
            if self.db.cf_handle(&name).is_none() {
                self.db.create_cf(&name, &Options::default())?;
            }
        }
        self.apply_durability
            .register(group, self.recovered_raft_applied(group)?);
        Ok(())
    }

    /// Idempotently drop both CFs for a group (reclamation, DESIGN §7.4).
    pub fn drop_group(&self, group: GroupId) -> Result<()> {
        self.apply_durability.begin_close(group);
        self.apply_durability.ensure_drained(group)?;
        for name in [group.cf_log(), group.cf_state()] {
            if self.db.cf_handle(&name).is_some() {
                self.db.drop_cf(&name)?;
            }
        }
        self.apply_durability.remove(group);
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
        match self.apply_durability.snapshot(group).failed {
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
        self.db.cf_handle(&group.cf_state()).is_some()
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
        self.db
            .cf_handle(&group.cf_state())
            .ok_or_else(|| Error::Corrupt(group.cf_state(), "state CF not open".into()))
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
            self.durability.reserve(wal_bytes)?
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
        let reservation = {
            let _profile = crate::perf::timer(WriteStage::StateApplyCapacityWait);
            self.durability.reserve(wal_bytes)?
        };
        self.apply_durability
            .begin(group, log_id, applied_entries, wal_bytes)
            .await?;

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

    #[cfg(test)]
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

    /// Atomically replace a group's replicated state from a snapshot. Clearing
    /// the existing keys and writing the replacement happen in one durable
    /// WriteBatch, so a crash exposes either the old complete state or the new
    /// complete state—never an empty CF between drop/recreate operations.
    /// The log CF and node-local authority state are untouched.
    pub fn install_state(
        &self,
        group: GroupId,
        pairs: &[(Vec<u8>, Vec<u8>)],
        applied_record: &[u8],
    ) -> Result<()> {
        let cf = self.state_cf(group)?;
        let mut batch = rocksdb::WriteBatch::default();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, _) = item?;
            batch.delete_cf(&cf, key);
        }
        for (k, v) in pairs {
            batch.put_cf(&cf, k, v);
        }
        batch.put_cf(&cf, keyspace::raft_applied_key(), applied_record);
        fail::fail_point!("snapshot_install::before_write", |_| Err(Error::Io(
            std::io::Error::other("injected crash at snapshot_install::before_write")
        )));
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db.write_opt(batch, &wo)?;
        fail::fail_point!("snapshot_install::after_write", |_| Err(Error::Io(
            std::io::Error::other("injected crash at snapshot_install::after_write")
        )));
        Ok(())
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
