//! DAL Service — a partitioned, strongly-consistent key-value store.
//!
//! See `DESIGN.md` for the full design and `IMPLEMENTATION.md` for the
//! milestone plan. Modules are introduced milestone by milestone; this is the
//! M1 foundation (types, config, storage).

pub mod codec;
pub mod config;
pub mod error;
pub mod keyspace;
pub mod partition;
pub mod storage;
pub mod types;

pub use error::{Error, Result};
