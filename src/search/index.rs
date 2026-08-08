use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{AllQuery, Query, QueryParser};
use tantivy::schema::{
    BytesOptions, DateOptions, Facet, FacetOptions, Field, IndexRecordOption, NumericOptions,
    Schema, TextFieldIndexing, TextOptions, Value,
};
use tantivy::{DateTime, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};

use crate::error::{Error, Result};
use crate::search::{
    ExtractedDocument, ExtractedValue, FieldKind, SearchHit, SearchIndexGeneration, SearchQuery,
    SearchRequest, StoredField,
};
use crate::types::{GroupId, Version};

type RaftLogId = openraft::LogId<u64>;

const WRITER_MEMORY_BYTES: usize = 50_000_000;
const MAX_LIMIT: u32 = 1_000;
const MAX_WINDOW: u32 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCheckpoint {
    pub projection_epoch: u64,
    pub definition_hash: [u8; 32],
    pub engine_revision: u32,
    pub source_log_id: Option<RaftLogId>,
}

#[derive(Debug, Clone)]
pub struct SearchSourceSnapshot {
    pub epoch: u64,
    pub applied: Option<RaftLogId>,
    pub records: Vec<(Vec<u8>, Version, Vec<u8>)>,
    pub outbox: Vec<crate::search::SearchOutboxEntry>,
}

#[derive(Clone)]
struct Fields {
    key: Field,
    version: Field,
    document_type: Field,
    configured: HashMap<String, Field>,
}

/// One local `(partition, index generation)` Tantivy projection.
pub struct LocalSearchIndex {
    group: GroupId,
    generation: SearchIndexGeneration,
    index: Index,
    fields: Fields,
    writer: Mutex<IndexWriter>,
    reader: IndexReader,
}

impl LocalSearchIndex {
    pub fn generation(&self) -> &SearchIndexGeneration {
        &self.generation
    }

    pub fn open_or_create(
        path: &Path,
        group: GroupId,
        generation: SearchIndexGeneration,
    ) -> Result<Self> {
        if !matches!(group, GroupId::Data(_)) {
            return Err(Error::Search(
                "a local search index requires a data group".into(),
            ));
        }
        generation.validate_identity()?;
        let (schema, fields) = build_schema(&generation)?;
        fs::create_dir_all(path)?;
        let index = if path.join("meta.json").exists() {
            match Index::open_in_dir(path) {
                Ok(index) if index.schema() == schema => index,
                outcome => {
                    // Design §7.2: an unreadable or mismatched directory is
                    // quarantined and rebuilt from authoritative state, never
                    // a permanent failure — index files are derived data.
                    let reason = match outcome {
                        Ok(_) => {
                            "on-disk Tantivy schema does not match index definition".to_string()
                        }
                        Err(error) => error.to_string(),
                    };
                    quarantine_index_dir(path, &reason)?;
                    fs::create_dir_all(path)?;
                    Index::create_in_dir(path, schema.clone()).map_err(search_error)?
                }
            }
        } else {
            Index::create_in_dir(path, schema.clone()).map_err(search_error)?
        };
        let writer = index.writer(WRITER_MEMORY_BYTES).map_err(search_error)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(search_error)?;
        Ok(Self {
            group,
            generation,
            index,
            fields,
            writer: Mutex::new(writer),
            reader,
        })
    }

    pub fn checkpoint(&self) -> Result<Option<IndexCheckpoint>> {
        let payload = self.index.load_metas().map_err(search_error)?.payload;
        payload
            .map(|payload| {
                serde_json::from_str(&payload).map_err(|error| {
                    Error::Search(format!("invalid Tantivy checkpoint payload: {error}"))
                })
            })
            .transpose()
    }

    pub fn validate_checkpoint(&self, epoch: u64) -> Result<Option<IndexCheckpoint>> {
        // An unreadable or unparseable checkpoint payload invalidates the
        // projection rather than failing every caller forever: `None` routes
        // the worker into a full rebuild (design §7.2).
        let checkpoint = match self.checkpoint() {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                tracing::warn!(
                    group = ?self.group,
                    %error,
                    "unreadable search checkpoint; treating index as unprojected"
                );
                return Ok(None);
            }
        };
        let Some(checkpoint) = checkpoint else {
            return Ok(None);
        };
        if checkpoint.projection_epoch != epoch
            || checkpoint.definition_hash != self.generation.definition_hash
            || checkpoint.engine_revision != self.generation.engine_revision
        {
            return Ok(None);
        }
        Ok(Some(checkpoint))
    }

    /// Replace all projected documents and durably commit the exact source
    /// prefix represented by `snapshot`.
    pub fn rebuild(&self, snapshot: &SearchSourceSnapshot) -> Result<u64> {
        let mut writer = self.writer.lock().unwrap();
        writer.delete_all_documents().map_err(search_error)?;
        let mut rejected = 0u64;
        for (key, version, value) in &snapshot.records {
            if let Some(error) = self.add_if_indexable(&mut writer, key, *version, value)? {
                tracing::warn!(group = ?self.group, key_bytes = key.len(), %error, "search document rejected");
                rejected += 1;
            }
        }
        self.commit_locked(&mut writer, snapshot.epoch, snapshot.applied)?;
        Ok(rejected)
    }

    /// Idempotent delete-then-add projection for one authoritative key.
    pub fn project(&self, key: &[u8], source: Option<(Version, &[u8])>) -> Result<bool> {
        let mut writer = self.writer.lock().unwrap();
        writer.delete_term(Term::from_field_bytes(self.fields.key, key));
        let mut rejected = false;
        if let Some((version, value)) = source
            && let Some(error) = self.add_if_indexable(&mut writer, key, version, value)?
        {
            tracing::warn!(group = ?self.group, key_bytes = key.len(), %error, "search document rejected");
            rejected = true;
        }
        Ok(rejected)
    }

    pub fn commit(&self, epoch: u64, source_log_id: Option<RaftLogId>) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        self.commit_locked(&mut writer, epoch, source_log_id)
    }

    fn commit_locked(
        &self,
        writer: &mut IndexWriter,
        epoch: u64,
        source_log_id: Option<RaftLogId>,
    ) -> Result<()> {
        let checkpoint = IndexCheckpoint {
            projection_epoch: epoch,
            definition_hash: self.generation.definition_hash,
            engine_revision: self.generation.engine_revision,
            source_log_id,
        };
        let payload = serde_json::to_string(&checkpoint)
            .map_err(|error| Error::Search(format!("encode index checkpoint: {error}")))?;
        let mut commit = writer.prepare_commit().map_err(search_error)?;
        commit.set_payload(&payload);
        commit.commit().map_err(search_error)?;
        self.reader.reload().map_err(search_error)
    }

    /// Attempt to index one authoritative record. `Ok(Some(reason))` is a
    /// rejected document — outside the searchable domain by data shape.
    /// `Ok(None)` was indexed or is not a document of this type. `Err` is an
    /// engine failure and must fail the projection: counting it as a rejection
    /// would let a later commit claim coverage through a prefix while silently
    /// omitting a valid document (I5).
    fn add_if_indexable(
        &self,
        writer: &mut IndexWriter,
        key: &[u8],
        version: Version,
        encoded: &[u8],
    ) -> Result<Option<Error>> {
        if key.len() > crate::search::SEARCH_MAX_KEY_BYTES {
            return Ok(Some(Error::Search(
                "key exceeds searchable key limit".into(),
            )));
        }
        let extracted = match crate::search::extract_document(encoded, &self.generation.definition)
        {
            Ok(Some(extracted)) => extracted,
            Ok(None) => return Ok(None),
            Err(error) => return Ok(Some(error)),
        };
        let document = match self.to_tantivy_document(key, version, &extracted) {
            Ok(document) => document,
            Err(error) => return Ok(Some(error)),
        };
        writer.add_document(document).map_err(search_error)?;
        Ok(None)
    }

    fn to_tantivy_document(
        &self,
        key: &[u8],
        version: Version,
        extracted: &ExtractedDocument,
    ) -> Result<TantivyDocument> {
        let mut document = TantivyDocument::default();
        document.add_bytes(self.fields.key, key);
        document.add_u64(self.fields.version, version);
        document.add_text(
            self.fields.document_type,
            &self.generation.definition.document_type,
        );
        for definition in &self.generation.definition.fields {
            let Some(values) = extracted.fields.get(&definition.name) else {
                continue;
            };
            let field = self.fields.configured[&definition.name];
            for value in values {
                match value {
                    ExtractedValue::Text(value) => document.add_text(field, value),
                    ExtractedValue::U64(value) => document.add_u64(field, *value),
                    ExtractedValue::I64(value) => document.add_i64(field, *value),
                    ExtractedValue::F64(value) => document.add_f64(field, *value),
                    ExtractedValue::Bool(value) => document.add_bool(field, *value),
                    ExtractedValue::Date(value) => {
                        document.add_date(field, DateTime::from_timestamp_millis(*value));
                    }
                    ExtractedValue::Facet(value) => {
                        let facet = Facet::from_text(value).map_err(search_error)?;
                        document.add_facet(field, facet);
                    }
                }
            }
        }
        Ok(document)
    }

    /// Execute a bounded local shard query against one loaded commit.
    pub fn search(
        &self,
        partition: u16,
        request: &SearchRequest,
    ) -> Result<crate::search::SearchReply> {
        if let crate::search::GenerationSelection::Exact(generation) = request.generation
            && generation != self.generation.id
        {
            return Err(Error::Search(
                "requested index generation is not loaded".into(),
            ));
        }
        if matches!(request.scoring, crate::search::ScoringMode::GlobalBm25) {
            return Err(Error::Search(
                "GlobalBm25 requires the distributed two-phase coordinator".into(),
            ));
        }
        if !request.sort.is_empty() {
            return Err(Error::Search("field sorting is not implemented".into()));
        }
        if request.limit > MAX_LIMIT
            || request
                .offset
                .checked_add(request.limit)
                .is_none_or(|window| window > MAX_WINDOW)
        {
            return Err(Error::Search(format!(
                "search page exceeds limit {MAX_LIMIT} or result window {MAX_WINDOW}"
            )));
        }
        let wanted = request
            .limit
            .checked_add(request.offset)
            .ok_or_else(|| Error::Search("search page overflow".into()))?
            as usize;
        let query = self.parse_query(&request.query)?;
        let searcher = self.reader.searcher();
        let total_hits = searcher
            .search(query.as_ref(), &Count)
            .map_err(search_error)? as u64;
        let top = if wanted == 0 {
            Vec::new()
        } else {
            searcher
                .search(
                    query.as_ref(),
                    &TopDocs::with_limit(wanted).order_by_score(),
                )
                .map_err(search_error)?
        };
        let mut hits = Vec::with_capacity(request.limit as usize);
        for (score, address) in top.into_iter().skip(request.offset as usize) {
            let document = searcher
                .doc::<TantivyDocument>(address)
                .map_err(search_error)?;
            let key = document
                .get_first(self.fields.key)
                .and_then(|value| value.as_bytes())
                .ok_or_else(|| Error::Search("indexed hit is missing stored _key".into()))?
                .to_vec();
            let version = document
                .get_first(self.fields.version)
                .and_then(|value| value.as_u64())
                .ok_or_else(|| Error::Search("indexed hit is missing stored _version".into()))?;
            let stored_fields = self.read_stored_fields(&document)?;
            hits.push(SearchHit {
                partition,
                key,
                version,
                score: Some(score),
                sort_values: Vec::new(),
                stored_fields,
            });
        }
        Ok(crate::search::SearchReply {
            generation: self.generation.id,
            definition_hash: self.generation.definition_hash,
            hits,
            total_hits,
            partial: false,
            failed_partitions: Vec::new(),
        })
    }

    fn parse_query(&self, query: &SearchQuery) -> Result<Box<dyn Query>> {
        match query {
            SearchQuery::MatchAll => Ok(Box::new(AllQuery)),
            SearchQuery::Text { query, fields } => {
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
                        "wildcard, fuzzy, regex, range, and field-override syntax is disabled"
                            .into(),
                    ));
                }
                let names = if fields.is_empty() {
                    &self.generation.definition.default_search_fields
                } else {
                    fields
                };
                if names.is_empty() {
                    return Err(Error::Search("text query has no search fields".into()));
                }
                let mut tantivy_fields = Vec::with_capacity(names.len());
                for name in names {
                    let definition = self
                        .generation
                        .definition
                        .fields
                        .iter()
                        .find(|field| field.name == *name)
                        .ok_or_else(|| Error::Search(format!("unknown query field {name:?}")))?;
                    if !definition.indexed
                        || !matches!(definition.kind, FieldKind::Text { .. } | FieldKind::Keyword)
                    {
                        return Err(Error::Search(format!(
                            "query field {name:?} is not indexed text"
                        )));
                    }
                    tantivy_fields.push(self.fields.configured[name]);
                }
                QueryParser::for_index(&self.index, tantivy_fields)
                    .parse_query(query)
                    .map_err(search_error)
            }
        }
    }

    fn read_stored_fields(&self, document: &TantivyDocument) -> Result<Vec<StoredField>> {
        let mut stored = Vec::new();
        for definition in self
            .generation
            .definition
            .fields
            .iter()
            .filter(|field| field.stored)
        {
            let field = self.fields.configured[&definition.name];
            let mut values = Vec::new();
            for value in document.get_all(field) {
                let value = match &definition.kind {
                    FieldKind::Text { .. } | FieldKind::Keyword => value
                        .as_str()
                        .map(|value| crate::search::SortValue::String(value.to_string())),
                    FieldKind::Facet => value.as_facet().and_then(|encoded| {
                        Facet::from_encoded(encoded.as_bytes().to_vec())
                            .ok()
                            .map(|facet| crate::search::SortValue::String(facet.to_path_string()))
                    }),
                    FieldKind::U64 => value.as_u64().map(crate::search::SortValue::U64),
                    FieldKind::I64 => value.as_i64().map(crate::search::SortValue::I64),
                    FieldKind::F64 => value.as_f64().map(crate::search::SortValue::F64),
                    FieldKind::Bool => value.as_bool().map(crate::search::SortValue::Bool),
                    FieldKind::Date => value
                        .as_datetime()
                        .map(|value| crate::search::SortValue::I64(value.into_timestamp_millis())),
                }
                .ok_or_else(|| {
                    Error::Search(format!("stored field {:?} has wrong type", definition.name))
                })?;
                values.push(value);
            }
            stored.push(StoredField {
                name: definition.name.clone(),
                values,
            });
        }
        Ok(stored)
    }
}

fn build_schema(generation: &SearchIndexGeneration) -> Result<(Schema, Fields)> {
    let mut builder = Schema::builder();
    let key = builder.add_bytes_field("_key", BytesOptions::default().set_indexed().set_stored());
    let version = builder.add_u64_field(
        "_version",
        NumericOptions::default().set_stored().set_fast(),
    );
    let document_type = builder.add_text_field(
        "_type",
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::Basic),
        ),
    );
    let mut configured = HashMap::new();
    for field in &generation.definition.fields {
        let tantivy_field = match &field.kind {
            FieldKind::Text {
                tokenizer,
                positions,
            } => {
                let mut options = TextOptions::default();
                if field.indexed {
                    options = options.set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer(tokenizer)
                            .set_index_option(if *positions {
                                IndexRecordOption::WithFreqsAndPositions
                            } else {
                                IndexRecordOption::WithFreqs
                            }),
                    );
                }
                if field.stored {
                    options = options.set_stored();
                }
                builder.add_text_field(&field.name, options)
            }
            FieldKind::Keyword => {
                let mut options = TextOptions::default();
                if field.indexed {
                    options = options.set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("raw")
                            .set_index_option(IndexRecordOption::Basic),
                    );
                }
                if field.stored {
                    options = options.set_stored();
                }
                if field.fast {
                    options = options.set_fast(Some("raw"));
                }
                builder.add_text_field(&field.name, options)
            }
            FieldKind::U64 => builder.add_u64_field(&field.name, numeric_options(field)),
            FieldKind::I64 => builder.add_i64_field(&field.name, numeric_options(field)),
            FieldKind::F64 => builder.add_f64_field(&field.name, numeric_options(field)),
            FieldKind::Bool => builder.add_bool_field(&field.name, numeric_options(field)),
            FieldKind::Date => {
                let mut options = DateOptions::default();
                if field.indexed {
                    options = options.set_indexed();
                }
                if field.stored {
                    options = options.set_stored();
                }
                if field.fast {
                    options = options.set_fast();
                }
                builder.add_date_field(&field.name, options)
            }
            FieldKind::Facet => {
                let options = if field.stored {
                    FacetOptions::default().set_stored()
                } else {
                    FacetOptions::default()
                };
                builder.add_facet_field(&field.name, options)
            }
        };
        configured.insert(field.name.clone(), tantivy_field);
    }
    Ok((
        builder.build(),
        Fields {
            key,
            version,
            document_type,
            configured,
        },
    ))
}

fn numeric_options(field: &crate::search::SearchField) -> NumericOptions {
    let mut options = NumericOptions::default();
    if field.indexed {
        options = options.set_indexed();
    }
    if field.stored {
        options = options.set_stored();
    }
    if field.fast {
        options = options.set_fast();
    }
    options
}

/// Move a corrupt index directory aside, keeping the latest copy for
/// diagnosis, so the caller can create a fresh directory in its place.
fn quarantine_index_dir(path: &Path, reason: &str) -> Result<()> {
    let name = path
        .file_name()
        .ok_or_else(|| Error::Search("index path has no directory name".into()))?;
    let mut quarantined = name.to_os_string();
    quarantined.push(".quarantined");
    let quarantined = path.with_file_name(quarantined);
    if quarantined.exists() {
        fs::remove_dir_all(&quarantined)?;
    }
    fs::rename(path, &quarantined)?;
    tracing::warn!(
        ?path,
        ?quarantined,
        reason,
        "quarantined corrupt search index directory"
    );
    Ok(())
}

fn search_error(error: impl std::fmt::Display) -> Error {
    Error::Search(error.to_string())
}
