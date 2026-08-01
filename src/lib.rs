//! DAL Service — a partitioned, strongly-consistent key-value store.
//!
//! See `DESIGN.md` for the full design and `IMPLEMENTATION.md` for the
//! milestone plan. Modules are introduced milestone by milestone; this is the
//! M1 foundation (types, config, storage).

pub mod api;
pub mod codec;
pub mod config;
pub mod error;
pub mod keyspace;
pub mod meta;
pub mod partition;
pub mod perf;
pub mod placement;
pub mod runtime;
pub mod storage;
pub mod transport;
pub mod types;
pub mod verify;

pub use error::{Error, Result};
