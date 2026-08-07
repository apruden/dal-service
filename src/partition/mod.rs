//! Per-partition data group: state machine (M2), then Raft storage/runtime (M3).

pub mod log_store;
pub mod network;
pub mod node;
pub mod raft_types;
pub mod sm;
pub mod state_machine;

pub use node::{PartitionNode, ReadOutcome, SearchBarrier, WriteOutcome};
pub use raft_types::{Raft, TypeConfig};
pub use state_machine::{
    ApplyObservation, ApplyObserver, ApplyResult, DataStateMachine, RejectReason,
};
