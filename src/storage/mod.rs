//! Storage layer: RocksDB handle, CF lifecycle, and the atomic apply helper.

mod apply_durability;
pub mod batch;
mod durability;
pub mod rocks;

pub(crate) use apply_durability::ApplyDurabilitySnapshot;
pub use batch::StateMutation;
pub use rocks::{Identity, Storage};
