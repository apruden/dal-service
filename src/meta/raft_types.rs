//! openraft type configuration for the meta group (M5).
//!
//! Identical machinery to a data group (`NodeId = u64`, `Node = BasicNode`,
//! `Cursor<Vec<u8>>` snapshots) — only the replicated command and response types
//! differ. The generic log store and channel network are shared; the state
//! machine wrapper ([`super::sm`]) is meta-specific.

use std::io::Cursor;

use crate::meta::state_machine::MetaApplyResult;
use crate::types::MetaCommand;

openraft::declare_raft_types!(
    pub MetaTypeConfig:
        D = MetaCommand,
        R = MetaApplyResult,
);

pub type NodeId = u64;
pub type Node = openraft::BasicNode;
pub type Entry = openraft::Entry<MetaTypeConfig>;
pub type LogId = openraft::LogId<NodeId>;
pub type StoredMembership = openraft::StoredMembership<NodeId, Node>;
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, Node>;
pub type Snapshot = openraft::Snapshot<MetaTypeConfig>;
pub type SnapshotData = Cursor<Vec<u8>>;
pub type Raft = openraft::Raft<MetaTypeConfig>;

/// Convert an openraft log id into the crate's `(term, index)` form, used for
/// the placement `voters_log_id` the meta state machine records.
pub fn to_crate_log_id(id: LogId) -> crate::types::LogId {
    crate::types::LogId::new(id.leader_id.term, id.index)
}
