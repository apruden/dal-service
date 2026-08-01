//! `RaftStateMachine` + `RaftSnapshotBuilder` wrapping the meta state machine
//! (DESIGN §5, M5). Mirrors the data wrapper ([`crate::partition::sm`]): business
//! mutations, the applied `LogId`, and membership commit in one atomic batch via
//! [`Storage::apply_raft`], so recovery is a clean prefix.

// OpenRaft storage traits prescribe this error type, so these helpers cannot
// box it without immediately unboxing at the trait boundary.
#![allow(clippy::result_large_err)]

use std::io::Cursor;
use std::sync::{Arc, Mutex};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, OptionalSend, StorageError};

use crate::codec;
use crate::meta::raft_types::{
    Entry, LogId, MetaTypeConfig, NodeId, Snapshot, SnapshotData, SnapshotMeta, StoredMembership,
    to_crate_log_id,
};
use crate::meta::state_machine::{MetaApplyResult, MetaStateMachine};
use crate::partition::raft_types::{read_err, write_err};
use crate::perf::WriteStage;
use crate::storage::Storage;
use crate::types::GroupId;

type Applied = (Option<LogId>, StoredMembership);

#[derive(Clone)]
pub struct MetaRaftStateMachine {
    storage: Arc<Storage>,
    sm: Arc<MetaStateMachine>,
    /// Shared by the live state machine and every snapshot-builder clone. This
    /// is required because OpenRaft builds snapshots concurrently with apply.
    state_view: Arc<Mutex<()>>,
}

impl MetaRaftStateMachine {
    pub fn new(storage: Arc<Storage>) -> Self {
        MetaRaftStateMachine {
            storage,
            sm: Arc::new(MetaStateMachine::new()),
            state_view: Arc::new(Mutex::new(())),
        }
    }

    fn read_applied(&self) -> Result<Applied, StorageError<NodeId>> {
        match self.storage.raft_applied(GroupId::Meta).map_err(read_err)? {
            Some(bytes) => codec::decode(&bytes).map_err(read_err),
            None => Ok((None, StoredMembership::default())),
        }
    }

    fn build_snapshot_now(&self) -> Result<Snapshot, StorageError<NodeId>> {
        let _view = self.state_view.lock().unwrap();
        let (last_log_id, membership) = self.read_applied()?;
        let pairs = self.storage.scan_state(GroupId::Meta).map_err(read_err)?;
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

impl RaftSnapshotBuilder<MetaTypeConfig> for MetaRaftStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError<NodeId>> {
        self.build_snapshot_now()
    }
}

impl RaftStateMachine<MetaTypeConfig> for MetaRaftStateMachine {
    type SnapshotBuilder = MetaRaftStateMachine;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembership), StorageError<NodeId>> {
        self.read_applied()
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MetaApplyResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let _profile = crate::perf::timer(WriteStage::StateApplyTotal);
        let _view = self.state_view.lock().unwrap();
        let (_, mut membership) = self.read_applied()?;
        let mut results = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;
            let (result, muts) = match entry.payload {
                EntryPayload::Blank => (MetaApplyResult::NoOp, Vec::new()),
                EntryPayload::Normal(cmd) => self
                    .sm
                    .evaluate(&self.storage, &cmd, to_crate_log_id(log_id))
                    .map_err(|e| StorageError::from(openraft::StorageIOError::apply(log_id, &e)))?,
                EntryPayload::Membership(m) => {
                    membership = StoredMembership::new(Some(log_id), m);
                    (MetaApplyResult::NoOp, Vec::new())
                }
            };

            let applied: Applied = (Some(log_id), membership.clone());
            self.storage
                .apply_raft(GroupId::Meta, &muts, &codec::encode(&applied))
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
        let _view = self.state_view.lock().unwrap();
        let bytes = (*snapshot).into_inner();
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = codec::decode(&bytes).map_err(read_err)?;
        let applied: Applied = (meta.last_log_id, meta.last_membership.clone());
        self.storage
            .install_state(GroupId::Meta, &pairs, &codec::encode(&applied))
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
