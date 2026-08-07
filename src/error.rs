//! Crate-wide error type.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration invalid: {0}")]
    Config(String),

    #[error("storage: {0}")]
    Rocks(#[from] rocksdb::Error),

    #[error("codec: {0}")]
    Codec(String),

    #[error(
        "identity mismatch: data dir holds cluster {found_cluster:#x} node {found_node}, \
         config expects cluster {want_cluster:#x} node {want_node}"
    )]
    IdentityMismatch {
        found_cluster: u128,
        found_node: u64,
        want_cluster: u128,
        want_node: u64,
    },

    #[error("corrupt on-disk record in {0}: {1}")]
    Corrupt(String, String),

    #[error("raft: {0}")]
    Raft(String),

    #[error("search: {0}")]
    Search(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    pub fn codec<E: std::fmt::Display>(e: E) -> Self {
        Error::Codec(e.to_string())
    }
}
