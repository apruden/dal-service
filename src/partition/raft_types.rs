//! openraft type configuration and shared aliases for a data group (M3).

use openraft::AnyError;
use openraft::StorageError;
use openraft::StorageIOError;

use crate::partition::state_machine::ApplyResult;
use crate::types::DataRequest;

openraft::declare_raft_types!(
    /// One data partition's Raft type config. `NodeId=u64`, `Node=BasicNode`,
    /// `Entry=Entry<Self>` comes from the macro defaults; snapshots use a
    /// bounded temporary file instead of `Cursor<Vec<u8>>`.
    pub TypeConfig:
        D = DataRequest,
        R = ApplyResult,
        SnapshotData = crate::snapshot::SnapshotFile,
);

pub type NodeId = u64;
pub type Node = openraft::BasicNode;
pub type Entry = openraft::Entry<TypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type Vote = openraft::Vote<NodeId>;
pub type StoredMembership = openraft::StoredMembership<NodeId, Node>;
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, Node>;
pub type Snapshot = openraft::Snapshot<TypeConfig>;
pub type SnapshotData = crate::snapshot::SnapshotFile;
pub type Raft = openraft::Raft<TypeConfig>;

/// Map a storage-layer error into a Raft read `StorageError`.
pub fn read_err(e: crate::error::Error) -> StorageError<NodeId> {
    StorageIOError::read(AnyError::new(&e)).into()
}

/// Map a storage-layer error into a Raft write `StorageError`.
pub fn write_err(e: crate::error::Error) -> StorageError<NodeId> {
    StorageIOError::write(AnyError::new(&e)).into()
}
