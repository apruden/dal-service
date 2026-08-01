//! RocksDB handle and per-group column-family lifecycle (DESIGN §6).
//!
//! One RocksDB instance per node. Two CFs per Raft group
//! (`cf_log_<group>` / `cf_state_<group>`) created and dropped on demand; the
//! default CF holds node-local authority state that must survive both snapshot
//! install and CF reclamation.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
use crate::storage::durability::{DurabilityConfig, OnDurable, WalDurability};
use crate::types::{
    BootstrapGroup, ClusterId, GroupId, LearnerAdmission, LogId, NodeId, RegistrationBinding,
    ServingState,
};

/// MultiThreaded so CFs can be created/dropped through `&self` while other
/// threads read — the runtime pattern for on-line rebalancing (DESIGN §6).
type Db = DBWithThreadMode<MultiThreaded>;

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
    db: Arc<Db>,
    profile_options: Option<Options>,
    path: PathBuf,
    /// Orders each non-sync WAL write with registration of its durability
    /// callback. The worker itself never takes this lock while flushing.
    log_append_gate: Mutex<()>,
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
        let flush_db = db.clone();
        let durability = WalDurability::with_config(durability_config, move || {
            flush_db
                .flush_wal(true)
                .map_err(|error| std::io::Error::other(error.to_string()))
        })?;
        Ok(Storage {
            durability,
            db,
            profile_options: crate::perf::write_path_enabled().then_some(opts),
            path,
            log_append_gate: Mutex::new(()),
        })
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
        Ok(())
    }

    /// Idempotently drop both CFs for a group (reclamation, DESIGN §7.4).
    pub fn drop_group(&self, group: GroupId) -> Result<()> {
        for name in [group.cf_log(), group.cf_state()] {
            if self.db.cf_handle(&name).is_some() {
                self.db.drop_cf(&name)?;
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
        if self.serving_state(group)? == Some(ServingState::Serving) {
            Ok(())
        } else {
            Err(Error::Raft(format!(
                "group {:?} is not permitted to serve on this node",
                group
            )))
        }
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

    /// Write a Raft log batch to the WAL without syncing it, then register its
    /// callback with the database-wide group-commit worker. The callback is
    /// never completed successfully until `flush_wal(true)` covers this write.
    pub(crate) fn write_log_batch(
        &self,
        batch: rocksdb::WriteBatch,
        wal_bytes: usize,
        callback: OnDurable,
    ) -> Result<()> {
        // Reserve first so bytes waiting for durability remain bounded even if
        // the disk becomes slower than Raft producers.
        let reservation = {
            let _profile = crate::perf::timer(WriteStage::WalCapacityWait);
            self.durability.reserve(wal_bytes)?
        };
        let _append = {
            let _profile = crate::perf::timer(WriteStage::WalAppendLockWait);
            self.log_append_gate
                .lock()
                .map_err(|_| std::io::Error::other("Raft log append lock poisoned"))?
        };

        let mut options = WriteOptions::default();
        options.set_sync(false);
        {
            let _profile = crate::perf::timer(WriteStage::WalWriteUnsynced);
            self.db.write_opt(batch, &options)?;
        }
        self.durability
            .submit(reservation, callback)
            .map_err(Error::Io)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    fn delayed_config(delay: Duration) -> DurabilityConfig {
        DurabilityConfig {
            max_pending_requests: 8,
            max_pending_bytes: 1_024,
            max_batch_delay: delay,
        }
    }

    #[test]
    fn log_batch_is_readable_before_its_durable_callback() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            Storage::open_with_durability(dir.path(), delayed_config(Duration::from_millis(100)))
                .unwrap();
        let (tx, rx) = mpsc::channel();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put(b"log-key", b"log-value");

        storage
            .write_log_batch(batch, 18, Box::new(move |result| tx.send(result).unwrap()))
            .unwrap();

        assert_eq!(storage.db().get(b"log-key").unwrap().unwrap(), b"log-value");
        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap().is_ok());
    }

    #[test]
    fn dropping_storage_durably_drains_pending_log_callbacks() {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            Storage::open_with_durability(dir.path(), delayed_config(Duration::from_secs(10)))
                .unwrap();
        let (tx, rx) = mpsc::channel();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put(b"shutdown-key", b"shutdown-value");
        storage
            .write_log_batch(batch, 26, Box::new(move |result| tx.send(result).unwrap()))
            .unwrap();

        drop(storage);

        assert!(rx.recv_timeout(Duration::from_secs(1)).unwrap().is_ok());
        let reopened = Storage::open(dir.path()).unwrap();
        assert_eq!(
            reopened.db().get(b"shutdown-key").unwrap().unwrap(),
            b"shutdown-value"
        );
    }
}
