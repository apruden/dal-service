//! Serialization for durable records and (later) wire payloads.
//!
//! bincode gives a compact, deterministic, codegen-free encoding. Determinism
//! matters: the command digest (M2) and byte-identical bootstrap/admission
//! retries (ground rule 8) both depend on the same value always producing the
//! same bytes.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// bincode configured for fixed-int, little-endian encoding — stable across
/// runs and machines.
pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    // bincode 1.x default config is deterministic (fixint, little-endian).
    // A serialize failure here means a non-serializable type, which is a bug.
    bincode::serialize(value).expect("record serialization is infallible for our types")
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    bincode::deserialize(bytes).map_err(Error::codec)
}

/// The exact length [`encode`] would produce, without allocating or writing the
/// bytes. Use this wherever only the size is wanted (batch sizing, budgeting).
pub fn encoded_len<T: Serialize>(value: &T) -> usize {
    bincode::serialized_size(value).expect("record serialization is infallible for our types")
        as usize
}
