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
    pub fn new(id: SearchIndexGenerationId, definition: SearchIndexDefinition) -> Result<Self> {
        if id == 0 {
            return Err(Error::Search("index generation must be non-zero".into()));
        }
        let definition_hash = definition.definition_hash()?;
        Ok(Self {
            id,
            definition_hash,
            engine_revision: SEARCH_ENGINE_REVISION,
            definition,
        })
    }

    pub fn validate_identity(&self) -> Result<()> {
        if self.engine_revision != SEARCH_ENGINE_REVISION {
            return Err(Error::Search(format!(
                "index engine revision {} is not supported (expected {SEARCH_ENGINE_REVISION})",
                self.engine_revision
            )));
        }
        if self.definition_hash != self.definition.definition_hash()? {
            return Err(Error::Search("index definition hash mismatch".into()));
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
        if let Some(active) = &self.active {
            active.validate_identity()?;
        }
        if let Some(building) = &self.building {
            building.validate_identity()?;
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
}
