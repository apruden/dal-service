//! Per-partition full-text search projection and public wire types.
//!
//! RocksDB remains authoritative. Tantivy indexes are local, rebuildable
//! projections fed by the persistent dirty-key outbox.

mod coordinator;
mod definition;
mod index;
mod outbox;
mod service;
mod value;
mod worker;

pub use coordinator::{SearchCoordinator, ShardSearchSource};
pub use definition::*;
pub use index::{IndexCheckpoint, LocalSearchIndex, SearchSourceSnapshot};
pub use outbox::{SearchConsumerState, SearchOutboxEntry, decode_outbox_key, encode_outbox_key};
pub use service::{SearchService, ShardSearchOutcome};
pub use value::{ExtractedDocument, ExtractedValue, encode_search_value, extract_document};
pub use worker::{SearchCatchUp, SearchIndexWorker};

/// Bump when the on-disk schema or scoring implementation changes.
pub const SEARCH_ENGINE_REVISION: u32 = 1;
pub const SEARCH_MAX_KEY_BYTES: usize = 4 * 1024;
pub const SEARCH_MAX_FIELDS: usize = 64;
pub const SEARCH_MAX_NAME_BYTES: usize = 128;
pub const SEARCH_MAX_PATH_SEGMENTS: usize = 16;
pub const SEARCH_MAX_DEFINITION_BYTES: usize = 64 * 1024;
pub const SEARCH_MAX_CATALOG_BYTES: usize = 256 * 1024;
/// Bound the only variable-size catalog field outside the two definitions, so
/// every valid catalog record fits its transport frame.
pub const SEARCH_MAX_RETIRING_GENERATIONS: usize = 4096;
pub const SEARCH_MAX_FIELD_BYTES: usize = 256 * 1024;
pub const SEARCH_MAX_VALUES_PER_FIELD: usize = 256;
pub const SEARCH_MAX_EXTRACTED_BYTES: usize = 1024 * 1024;
