//! Storage layer: RocksDB handle, CF lifecycle, and the atomic apply helper.

pub mod batch;
pub mod rocks;

pub use batch::StateMutation;
pub use rocks::{Identity, Storage};
