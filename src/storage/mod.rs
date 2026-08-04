//! Storage layer: RocksDB handle, CF lifecycle, and the atomic apply helper.

mod apply_durability;
pub mod batch;
mod durability;
pub mod rocks;

pub(crate) use apply_durability::ApplyDurabilitySnapshot;
pub use batch::StateMutation;
pub use rocks::{Identity, Storage};

/// Production bounds for one data-group state batch awaiting WAL durability.
/// The state-machine wrapper chunks normal coalesced applies below these
/// limits; storage still validates them defensively at the final boundary.
pub(crate) const MAX_PENDING_STATE_ENTRIES: usize = 4_096;
pub(crate) const MAX_PENDING_STATE_BYTES: usize = 64 * 1024 * 1024;

/// The largest outstanding WAL request. The extra MiB above the 64 MiB Raft
/// transport payload accounts for RocksDB keys and WriteBatch framing while
/// retaining a strict, finite reservation bound.
pub(crate) const MAX_PENDING_WAL_BYTES: usize = 65 * 1024 * 1024;
