use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::search::{
    SEARCH_ENGINE_REVISION, SEARCH_MAX_DEFINITION_BYTES, SEARCH_MAX_FIELDS, SEARCH_MAX_NAME_BYTES,
    SEARCH_MAX_PATH_SEGMENTS,
};

pub type SearchIndexGenerationId = u64;

pub fn validate_index_name(name: &str) -> Result<()> {
    validate_name("index name", name)?;
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(Error::Search(
            "index name may contain only ASCII letters, digits, '-', '_', and '.'".into(),
        ));
    }
    // The name is a path component of the on-disk Tantivy directory, so a
    // dot-only name would resolve outside the per-group index root and collide
    // across partitions.
    if name.bytes().all(|byte| byte == b'.') {
        return Err(Error::Search(
            "index name may not consist only of '.' characters".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathSegment {
    Key(String),
    Index(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldKind {
    Text {
        tokenizer: String,
        positions: bool,
    },
    Keyword,
    U64,
    I64,
    F64,
    Bool,
    /// Unix timestamp in milliseconds.
    Date,
    Facet,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchField {
    pub name: String,
    pub source_path: Vec<PathSegment>,
    pub kind: FieldKind,
    pub required: bool,
    pub multi_valued: bool,
    pub indexed: bool,
    pub stored: bool,
    pub fast: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchIndexDefinition {
    pub document_type: String,
    pub fields: Vec<SearchField>,
    pub default_search_fields: Vec<String>,
}

impl SearchIndexDefinition {
    pub fn validate(&self) -> Result<()> {
        validate_name("document type", &self.document_type)?;
        if self.fields.is_empty() || self.fields.len() > SEARCH_MAX_FIELDS {
            return Err(Error::Search(format!(
                "an index must define 1..={SEARCH_MAX_FIELDS} fields"
            )));
        }

        let mut names = HashSet::with_capacity(self.fields.len());
        for field in &self.fields {
            validate_name("field", &field.name)?;
            if field.name.starts_with('_') {
                return Err(Error::Search(format!(
                    "field name {:?} is reserved",
                    field.name
                )));
            }
            if !names.insert(field.name.as_str()) {
                return Err(Error::Search(format!(
                    "duplicate field name {:?}",
                    field.name
                )));
            }
            if field.source_path.is_empty() || field.source_path.len() > SEARCH_MAX_PATH_SEGMENTS {
                return Err(Error::Search(format!(
                    "field {:?} path must have 1..={SEARCH_MAX_PATH_SEGMENTS} segments",
                    field.name
                )));
            }
            for segment in &field.source_path {
                if let PathSegment::Key(key) = segment {
                    validate_name("path key", key)?;
                }
            }
            if !field.indexed && !field.stored && !field.fast {
                return Err(Error::Search(format!(
                    "field {:?} has no index, store, or fast option",
                    field.name
                )));
            }
            if field.fast && matches!(field.kind, FieldKind::Text { .. }) {
                return Err(Error::Search(format!(
                    "text field {:?} cannot be fast",
                    field.name
                )));
            }
            if matches!(field.kind, FieldKind::Facet) && (!field.indexed || !field.fast) {
                return Err(Error::Search(format!(
                    "facet field {:?} must be indexed and fast",
                    field.name
                )));
            }
            if let FieldKind::Text { tokenizer, .. } = &field.kind {
                validate_name("tokenizer", tokenizer)?;
                if !matches!(
                    tokenizer.as_str(),
                    "default" | "raw" | "en_stem" | "whitespace"
                ) {
                    return Err(Error::Search(format!(
                        "field {:?} uses unsupported tokenizer {:?}",
                        field.name, tokenizer
                    )));
                }
            }
        }

        let mut defaults = HashSet::new();
        for name in &self.default_search_fields {
            if !defaults.insert(name) {
                return Err(Error::Search(format!(
                    "duplicate default search field {name:?}"
                )));
            }
            let field = self
                .fields
                .iter()
                .find(|field| field.name == *name)
                .ok_or_else(|| Error::Search(format!("unknown default search field {name:?}")))?;
            if !field.indexed || !matches!(field.kind, FieldKind::Text { .. } | FieldKind::Keyword)
            {
                return Err(Error::Search(format!(
                    "default search field {name:?} must be indexed text or keyword"
                )));
            }
        }

        let encoded = bincode::serialize(self).map_err(Error::codec)?;
        if encoded.len() > SEARCH_MAX_DEFINITION_BYTES {
            return Err(Error::Search(format!(
                "encoded index definition exceeds {SEARCH_MAX_DEFINITION_BYTES} bytes"
            )));
        }
        Ok(())
    }

    pub fn definition_hash(&self) -> Result<[u8; 32]> {
        self.validate()?;
        let bytes = bincode::serialize(self).map_err(Error::codec)?;
        Ok(Sha256::digest(bytes).into())
    }
}

fn validate_name(kind: &str, name: &str) -> Result<()> {
    if name.is_empty() || name.len() > SEARCH_MAX_NAME_BYTES {
        return Err(Error::Search(format!(
            "{kind} must be 1..={SEARCH_MAX_NAME_BYTES} bytes"
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(Error::Search(format!(
            "{kind} contains a control character"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchIndexGeneration {
    pub id: SearchIndexGenerationId,
    pub definition_hash: [u8; 32],
    pub engine_revision: u32,
    pub definition: SearchIndexDefinition,
}

impl SearchIndexGeneration {
    /// Proposer-side constructor: stamps this binary's engine revision.
    pub fn new(id: SearchIndexGenerationId, definition: SearchIndexDefinition) -> Result<Self> {
        Self::with_revision(id, SEARCH_ENGINE_REVISION, definition)
    }

    /// Replicated-apply constructor: the engine revision comes from the
    /// committed command, never from the local binary, so every meta replica
    /// applies the same command to the same record regardless of its version.
    pub fn with_revision(
        id: SearchIndexGenerationId,
        engine_revision: u32,
        definition: SearchIndexDefinition,
    ) -> Result<Self> {
        if id == 0 {
            return Err(Error::Search("index generation must be non-zero".into()));
        }
        if engine_revision == 0 {
            return Err(Error::Search(
                "index engine revision must be non-zero".into(),
            ));
        }
        let definition_hash = definition.definition_hash()?;
        Ok(Self {
            id,
            definition_hash,
            engine_revision,
            definition,
        })
    }

    /// Deterministic structural checks, safe inside replicated state-machine
    /// apply: no comparison against this binary's engine revision, which would
    /// diverge across mixed-version replicas or poison the meta group after a
    /// revision bump.
    pub fn validate_integrity(&self) -> Result<()> {
        if self.id == 0 {
            return Err(Error::Search("index generation must be non-zero".into()));
        }
        if self.engine_revision == 0 {
            return Err(Error::Search(
                "index engine revision must be non-zero".into(),
            ));
        }
        if self.definition_hash != self.definition.definition_hash()? {
            return Err(Error::Search("index definition hash mismatch".into()));
        }
        Ok(())
    }

    /// Node-local check for actually building/serving this generation: this
    /// binary must implement the recorded engine revision.
    pub fn validate_identity(&self) -> Result<()> {
        self.validate_integrity()?;
        if self.engine_revision != SEARCH_ENGINE_REVISION {
            return Err(Error::Search(format!(
                "index engine revision {} is not supported (expected {SEARCH_ENGINE_REVISION})",
                self.engine_revision
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SearchIndexRecord {
    pub name: String,
    pub active: Option<SearchIndexGeneration>,
    pub building: Option<SearchIndexGeneration>,
    pub retiring: Vec<SearchIndexGenerationId>,
}

impl SearchIndexRecord {
    pub fn validate(&self) -> Result<()> {
        validate_index_name(&self.name)?;
        if self.retiring.len() > crate::search::SEARCH_MAX_RETIRING_GENERATIONS {
            return Err(Error::Search(format!(
                "search index has more than {} retiring generations",
                crate::search::SEARCH_MAX_RETIRING_GENERATIONS
            )));
        }
        // Integrity only: catalog records are read inside deterministic meta
        // apply, where a record written by a different binary revision must
        // stay readable rather than fail-stop the group.
        if let Some(active) = &self.active {
            active.validate_integrity()?;
        }
        if let Some(building) = &self.building {
            building.validate_integrity()?;
        }
        let encoded = bincode::serialized_size(self).map_err(Error::codec)? as usize;
        if encoded > crate::search::SEARCH_MAX_CATALOG_BYTES {
            return Err(Error::Search(format!(
                "encoded search catalog record exceeds {} bytes",
                crate::search::SEARCH_MAX_CATALOG_BYTES
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchIndexReady {
    pub generation: SearchIndexGenerationId,
    pub definition_hash: [u8; 32],
    pub engine_revision: u32,
    pub group: crate::types::GroupId,
    pub node_id: crate::types::NodeId,
    pub voters_log_id: crate::types::LogId,
    pub projection_epoch: u64,
    pub source_log_id: crate::types::LogId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum GenerationSelection {
    #[default]
    Active,
    Exact(SearchIndexGenerationId),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SearchQuery {
    Text { query: String, fields: Vec<String> },
    MatchAll,
}

impl SearchQuery {
    /// Validate all request-controlled query shape before a coordinator clones
    /// or fans it out. Shard parsing repeats this check for direct callers.
    pub fn validate(&self) -> Result<()> {
        let SearchQuery::Text { query, fields } = self else {
            return Ok(());
        };
        if query.is_empty() || query.len() > 4 * 1024 {
            return Err(Error::Search("text query must be 1..=4096 bytes".into()));
        }
        if query.chars().any(|character| {
            matches!(
                character,
                '*' | '?' | '~' | '/' | '[' | ']' | '{' | '}' | ':'
            )
        }) {
            return Err(Error::Search(
                "wildcard, fuzzy, regex, range, and field-override syntax is disabled".into(),
            ));
        }
        if fields.len() > SEARCH_MAX_FIELDS {
            return Err(Error::Search(format!(
                "query fields exceed the {SEARCH_MAX_FIELDS} field limit"
            )));
        }
        let mut unique = HashSet::with_capacity(fields.len());
        for field in fields {
            validate_name("query field", field)?;
            if !unique.insert(field) {
                return Err(Error::Search(format!("duplicate query field {field:?}")));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ScoringMode {
    #[default]
    GlobalBm25,
    LocalBm25,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SearchConsistency {
    #[default]
    Strict,
    Eventual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortField {
    pub field: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchRequest {
    pub index: String,
    pub generation: GenerationSelection,
    pub query: SearchQuery,
    pub limit: u32,
    pub offset: u32,
    pub sort: Vec<SortField>,
    pub scoring: ScoringMode,
    pub consistency: SearchConsistency,
    pub allow_partial: bool,
    pub deadline_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SortValue {
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredField {
    pub name: String,
    pub values: Vec<SortValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    pub partition: u16,
    pub key: Vec<u8>,
    pub version: u64,
    pub score: Option<f32>,
    pub sort_values: Vec<SortValue>,
    pub stored_fields: Vec<StoredField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionFailure {
    pub partition: u16,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchReply {
    pub generation: SearchIndexGenerationId,
    pub definition_hash: [u8; 32],
    pub hits: Vec<SearchHit>,
    pub total_hits: u64,
    pub partial: bool,
    pub failed_partitions: Vec<PartitionFailure>,
}

impl SearchReply {
    pub fn validate_wire_size(&self) -> Result<()> {
        let bytes = bincode::serialized_size(self).map_err(Error::codec)? as usize;
        if bytes > crate::search::SEARCH_REPLY_BODY_BUDGET {
            return Err(Error::Search(format!(
                "search reply requires {bytes} bytes, exceeding the {} byte response budget; reduce the result window or stored fields",
                crate::search::SEARCH_REPLY_BODY_BUDGET
            )));
        }
        Ok(())
    }
}

/// BM25 inputs measured against one held searcher, or the coordinator's sum of
/// them (design §9.2). Local BM25 scores are not comparable across partitions
/// because document frequency and average field length differ per shard; these
/// are the values that make them comparable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ShardStatistics {
    pub total_docs: u64,
    /// Indexed tokens per scored field, keyed by field name.
    pub field_tokens: Vec<(String, u64)>,
    /// Document frequency per expanded query term, keyed by an encoding of
    /// (field name, value type, value bytes).
    pub term_doc_freq: Vec<(Vec<u8>, u64)>,
}

impl ShardStatistics {
    /// Structural bounds checked on every shard reply before it is summed, so
    /// a misbehaving or mismatched replica cannot inflate coordinator memory.
    pub fn validate(&self) -> Result<()> {
        if self.term_doc_freq.len() > crate::search::SEARCH_MAX_QUERY_TERMS
            || self.field_tokens.len() > SEARCH_MAX_FIELDS + 3
        {
            return Err(Error::Search(
                "shard statistics exceed the query term or field limit".into(),
            ));
        }
        for (name, _) in &self.field_tokens {
            validate_name("statistics field", name)?;
        }
        for (term, _) in &self.term_doc_freq {
            if term.len() > crate::search::SEARCH_MAX_TERM_KEY_BYTES {
                return Err(Error::Search("statistics term key is too long".into()));
            }
        }
        let bytes = bincode::serialized_size(self).map_err(Error::codec)? as usize;
        if bytes > crate::search::SEARCH_MAX_STATISTICS_BYTES {
            return Err(Error::Search(format!(
                "shard statistics require {bytes} bytes, exceeding the {} byte limit",
                crate::search::SEARCH_MAX_STATISTICS_BYTES
            )));
        }
        Ok(())
    }

    /// Accumulate one shard's contribution. Sums saturate rather than wrap: a
    /// wrapped corpus size would silently corrupt every score in the response.
    pub fn merge(&mut self, other: &ShardStatistics) {
        self.total_docs = self.total_docs.saturating_add(other.total_docs);
        merge_sums(&mut self.field_tokens, &other.field_tokens);
        merge_sums(&mut self.term_doc_freq, &other.term_doc_freq);
    }
}

fn merge_sums<K: Ord + Clone>(into: &mut Vec<(K, u64)>, from: &[(K, u64)]) {
    for (key, value) in from {
        match into.binary_search_by(|(existing, _)| existing.cmp(key)) {
            Ok(at) => into[at].1 = into[at].1.saturating_add(*value),
            Err(at) => into.insert(at, (key.clone(), *value)),
        }
    }
}

/// Phase-one shard request: fence the leader, pin a searcher, and report the
/// statistics the coordinator needs to make scores comparable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardPrepareRequest {
    pub index: String,
    pub generation: SearchIndexGenerationId,
    pub definition_hash: [u8; 32],
    pub query: SearchQuery,
    pub consistency: SearchConsistency,
    /// Top-N the shard must be able to return in phase two.
    pub window: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardPrepared {
    pub session_id: SearchSessionId,
    pub checkpoint_index: u64,
    pub statistics: ShardStatistics,
}

/// Node-local held-session identity. The durable incarnation prevents a
/// delayed phase-two request from aliasing a different query after restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SearchSessionId {
    pub incarnation: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardPrepareReply {
    Prepared(ShardPrepared),
    NotLeader {
        leader: Option<crate::types::NodeId>,
    },
    Error(String),
}

/// Phase-two shard request: score the pinned searcher with cluster-wide
/// statistics and return its top window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShardExecuteRequest {
    pub session_id: SearchSessionId,
    pub statistics: std::sync::Arc<ShardStatistics>,
}

/// What a `SearchExecute` frame asks of a held session. A search that fails
/// before phase two must hand its sessions back rather than let them pin
/// Tantivy segment files until they expire — the failure case is exactly when
/// the cluster can least afford the extra retention.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardExecuteBody {
    Execute(ShardExecuteRequest),
    Release { session_id: SearchSessionId },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardExecuteReply {
    Result(SearchReply),
    /// The session expired or was never held here; the coordinator must retry
    /// the whole request rather than silently drop a partition.
    UnknownSession,
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition() -> SearchIndexDefinition {
        SearchIndexDefinition {
            document_type: "article".into(),
            fields: vec![SearchField {
                name: "title".into(),
                source_path: vec![PathSegment::Key("title".into())],
                kind: FieldKind::Text {
                    tokenizer: "default".into(),
                    positions: true,
                },
                required: true,
                multi_valued: false,
                indexed: true,
                stored: true,
                fast: false,
            }],
            default_search_fields: vec!["title".into()],
        }
    }

    #[test]
    fn hash_is_stable_and_identity_is_checked() {
        let generation = SearchIndexGeneration::new(7, definition()).unwrap();
        generation.validate_identity().unwrap();
        assert_eq!(
            generation.definition_hash,
            definition().definition_hash().unwrap()
        );

        let mut changed = generation;
        changed.definition.document_type = "comment".into();
        assert!(changed.validate_identity().is_err());
    }

    #[test]
    fn rejects_reserved_and_duplicate_fields() {
        let mut value = definition();
        value.fields[0].name = "_key".into();
        assert!(value.validate().is_err());

        let mut value = definition();
        value.fields.push(value.fields[0].clone());
        assert!(value.validate().is_err());
    }

    #[test]
    fn query_shape_is_bounded_before_fan_out() {
        let duplicate = SearchQuery::Text {
            query: "term".into(),
            fields: vec!["title".into(), "title".into()],
        };
        assert!(duplicate.validate().is_err());

        let too_many = SearchQuery::Text {
            query: "term".into(),
            fields: (0..=SEARCH_MAX_FIELDS).map(|i| format!("f{i}")).collect(),
        };
        assert!(too_many.validate().is_err());
    }

    #[test]
    fn oversized_search_reply_is_refused_before_transport_encoding() {
        let reply = SearchReply {
            generation: 1,
            definition_hash: [0; 32],
            hits: vec![SearchHit {
                partition: 0,
                key: vec![0],
                version: 1,
                score: Some(1.0),
                sort_values: Vec::new(),
                stored_fields: vec![StoredField {
                    name: "body".into(),
                    values: vec![SortValue::String(
                        "x".repeat(crate::search::SEARCH_REPLY_BODY_BUDGET),
                    )],
                }],
            }],
            total_hits: 1,
            partial: false,
            failed_partitions: Vec::new(),
        };
        assert!(reply.validate_wire_size().is_err());
    }

    #[test]
    fn aggregate_statistics_must_fit_the_prepare_frame() {
        let statistics = ShardStatistics {
            total_docs: 1,
            field_tokens: vec![("title".into(), 1)],
            term_doc_freq: (0..crate::search::SEARCH_MAX_QUERY_TERMS)
                .map(|term| {
                    let mut key = vec![b'x'; 64];
                    key.extend_from_slice(&term.to_be_bytes());
                    (key, 1)
                })
                .collect(),
        };
        assert!(statistics.validate().is_err());
    }
}
