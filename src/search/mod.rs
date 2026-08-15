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
pub use index::{
    GlobalBm25Statistics, HeldSearch, IndexCheckpoint, LocalSearchIndex, SearchSourceSnapshot,
};
pub use outbox::{
    SearchConsumerRegistration, SearchConsumerState, SearchOutboxEntry, decode_outbox_key,
    encode_outbox_key,
};
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
/// Bound the complete catalog so its internal full-catalog reply has a finite
/// wire size. The meta state machine rejects creation beyond this count.
pub const SEARCH_MAX_INDEXES: usize = 64;
/// Per-record and full-catalog bounds are distinct: a legal catalog can contain
/// many individually legal records.
pub const SEARCH_MAX_CATALOG_BYTES: usize = 256 * 1024;
pub const SEARCH_MAX_CATALOG_FRAME_BYTES: usize =
    SEARCH_MAX_INDEXES * SEARCH_MAX_CATALOG_BYTES + 64 * 1024;
/// Bound the only variable-size catalog field outside the two definitions, so
/// every valid catalog record fits its transport frame.
pub const SEARCH_MAX_RETIRING_GENERATIONS: usize = 4096;
pub const SEARCH_MAX_FIELD_BYTES: usize = 256 * 1024;
pub const SEARCH_MAX_VALUES_PER_FIELD: usize = 256;
pub const SEARCH_MAX_EXTRACTED_BYTES: usize = 1024 * 1024;
/// Distributed BM25 costs one document-frequency probe and one wire entry per
/// expanded term, so term expansion must be bounded before it reaches the
/// blocking pool (design §9.2, §13).
pub const SEARCH_MAX_QUERY_TERMS: usize = 1024;
/// A statistics term key is (field name, type tag, value); the value is a
/// single token, so this only has to clear a long token plus the name bound.
pub const SEARCH_MAX_TERM_KEY_BYTES: usize = 4 * 1024;
/// Held searcher sessions pin Tantivy segment files, so their count and
/// lifetime are bounded independently of query concurrency (design §8.1).
pub const SEARCH_MAX_SESSIONS: usize = 256;
pub const SEARCH_SESSION_TTL_MS: u64 = 60_000;
/// Search work is admitted separately from client and Raft work so a fan-out
/// storm cannot starve elections or point operations (design §13). A global
/// search occupies one coordinator slot for its whole two-phase lifetime,
/// while each shard phase occupies a shard slot only while it runs.
pub const SEARCH_MAX_CONCURRENT_GLOBAL: usize = 32;
pub const SEARCH_MAX_CONCURRENT_SHARD: usize = 64;
/// Outbox retention is bounded so a failed or hopelessly lagging projection
/// cannot fill the disk the authoritative database depends on (design §6.5,
/// §12). Crossing either bound releases the slowest consumer and rebuilds it.
pub const SEARCH_MAX_OUTBOX_ENTRIES: u64 = 2_000_000;
pub const SEARCH_MAX_OUTBOX_BYTES: u64 = 512 * 1024 * 1024;
/// Every shard must return the whole requested page, because a global merge
/// can draw all of it from one partition — so the page bound is what actually
/// bounds per-shard collection work (design §9.3, §13).
pub const SEARCH_MAX_LIMIT: u32 = 1_000;
pub const SEARCH_MAX_WINDOW: u32 = 1_000;
