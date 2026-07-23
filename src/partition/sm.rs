//! `RaftStateMachine` + `RaftSnapshotBuilder` wrapping the M2 data state
//! machine (DESIGN §6, M3).
//!
//! Business mutations, the applied `LogId`, and membership are written in one
//! atomic batch via [`Storage::apply_raft`], so recovery is a clean prefix.
//! Snapshots serialize the replicated state-CF records only; node-local
//! authority state lives in the default CF and is never copied or erased by a
//! snapshot (DESIGN §6).

// OpenRaft storage traits prescribe this error type, so these helpers cannot
// box it without immediately unboxing at the trait boundary.
#![allow(clippy::result_large_err)]

use std::io::Cursor;
use std::sync::Arc;

use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::StorageError;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;

use crate::codec;
use crate::partition::raft_types::{
    Entry, LogId, Node, NodeId, Snapshot, SnapshotData, SnapshotMeta, StoredMembership, TypeConfig,
    read_err, write_err,
};
use crate::partition::state_machine::{ApplyResult, DataStateMachine};
use crate::storage::Storage;
use crate::types::GroupId;

/// The persisted Raft applied-state: last applied log id and membership.
type Applied = (Option<LogId>, StoredMembership);

#[derive(Clone)]
pub struct RocksStateMachine {
    storage: Arc<Storage>,
    group: GroupId,
    data: DataStateMachine,
}

impl RocksStateMachine {
    pub fn new(storage: Arc<Storage>, group: GroupId) -> Self {
        RocksStateMachine {
            storage,
            group,
            data: DataStateMachine::new(group),
        }
    }

    fn read_applied(&self) -> Result<Applied, StorageError<NodeId>> {
        match self.storage.raft_applied(self.group).map_err(read_err)? {
            Some(bytes) => codec::decode(&bytes).map_err(read_err),
            None => Ok((None, StoredMembership::default())),
        }
    }

    fn build_snapshot_now(&self) -> Result<Snapshot, StorageError<NodeId>> {
        let (last_log_id, membership) = self.read_applied()?;
        let pairs = self.storage.scan_state(self.group).map_err(read_err)?;
        let data = codec::encode(&pairs);
        let snapshot_id = match &last_log_id {
            Some(l) => format!("{l}"),
            None => "empty".to_string(),
        };
        let meta = SnapshotMeta {
            last_log_id,
            last_membership: membership,
            snapshot_id,
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RocksStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError<NodeId>> {
        self.build_snapshot_now()
    }
}

impl RaftStateMachine<TypeConfig> for RocksStateMachine {
    type SnapshotBuilder = RocksStateMachine;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembership), StorageError<NodeId>> {
        self.read_applied()
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ApplyResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let (_, mut membership) = self.read_applied()?;
        let mut results = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;
            let (result, muts) = match entry.payload {
                EntryPayload::Blank => (ApplyResult::NoOp, Vec::new()),
                EntryPayload::Normal(req) => self
                    .data
                    .evaluate(&self.storage, &req, log_id.index)
                    .map_err(|e| StorageError::from(openraft::StorageIOError::apply(log_id, &e)))?,
                EntryPayload::Membership(m) => {
                    membership = StoredMembership::new(Some(log_id), m);
                    (ApplyResult::NoOp, Vec::new())
                }
            };

            let applied: Applied = (Some(log_id), membership.clone());
            self.storage
                .apply_raft(self.group, &muts, &codec::encode(&applied))
                .map_err(write_err)?;
            results.push(result);
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = (*snapshot).into_inner();
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = codec::decode(&bytes).map_err(read_err)?;
        let applied: Applied = (meta.last_log_id, meta.last_membership.clone());
        self.storage
            .install_state(self.group, &pairs, &codec::encode(&applied))
            .map_err(write_err)?;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError<NodeId>> {
        let (last_log_id, _) = self.read_applied()?;
        if last_log_id.is_none() {
            return Ok(None);
        }
        Ok(Some(self.build_snapshot_now()?))
    }
}

// Silence unused-import lint for `Node` (referenced only via type aliases).
const _: fn() = || {
    let _: Option<Node> = None;
};
