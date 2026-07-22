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
    WriteOptions,
};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

use crate::codec;
use crate::error::{Error, Result};
use crate::keyspace;
use crate::types::{ClusterId, GroupId, LogId, NodeId};

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
    db: Db,
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
        let path = path.as_ref().to_path_buf();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // On a fresh dir there is no descriptor list yet; start with default.
        let existing = Db::list_cf(&opts, &path).unwrap_or_else(|_| vec!["default".to_string()]);
        let descriptors: Vec<ColumnFamilyDescriptor> = existing
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();

        let db = Db::open_cf_descriptors(&opts, &path, descriptors)?;
        Ok(Storage { db, path })
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

    pub fn group_exists(&self, group: GroupId) -> bool {
        self.db.cf_handle(&group.cf_state()).is_some()
            && self.db.cf_handle(&group.cf_log()).is_some()
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

    /// Replace a group's state CF wholesale from a snapshot: drop and recreate
    /// the CF, then write `pairs` plus the Raft applied-state blob in one
    /// fsync-durable batch (M3 snapshot install). The log CF is untouched;
    /// node-local authority state lives in the default CF and also survives.
    pub fn install_state(
        &self,
        group: GroupId,
        pairs: &[(Vec<u8>, Vec<u8>)],
        applied_record: &[u8],
    ) -> Result<()> {
        let name = group.cf_state();
        if self.db.cf_handle(&name).is_some() {
            self.db.drop_cf(&name)?;
        }
        self.db.create_cf(&name, &Options::default())?;

        let cf = self.state_cf(group)?;
        let mut batch = rocksdb::WriteBatch::default();
        for (k, v) in pairs {
            batch.put_cf(&cf, k, v);
        }
        batch.put_cf(&cf, keyspace::raft_applied_key(), applied_record);
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db.write_opt(batch, &wo)?;
        Ok(())
    }
}
