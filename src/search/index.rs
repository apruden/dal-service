use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Mutex, RwLock};

use serde::{Deserialize, Serialize};
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{AllQuery, Bm25StatisticsProvider, Query, QueryParser};
use tantivy::schema::{
    BytesOptions, DateOptions, Facet, FacetOptions, Field, IndexRecordOption, NumericOptions,
    Schema, TextFieldIndexing, TextOptions, Value,
};
use tantivy::{
    DateTime, Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TantivyDocument, Term,
};

use crate::error::{Error, Result};
use crate::search::{
    ExtractedDocument, ExtractedValue, FieldKind, SearchHit, SearchIndexGeneration, SearchQuery,
    SearchRequest, ShardStatistics, StoredField,
};
use crate::types::{GroupId, Version};

type RaftLogId = openraft::LogId<u64>;

const WRITER_MEMORY_BYTES: usize = 50_000_000;
use crate::search::{SEARCH_MAX_LIMIT as MAX_LIMIT, SEARCH_MAX_WINDOW as MAX_WINDOW};

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

/// One pinned Tantivy commit plus the query parsed against it. Holding the
/// searcher for the lifetime of a distributed query is what stops statistics
/// and scoring from observing different commits (design §8.1); Tantivy keeps
/// the referenced segment files readable even after a later commit and merge
/// retire them.
pub struct HeldSearch {
    searcher: Searcher,
    query: Box<dyn Query>,
    checkpoint: IndexCheckpoint,
}

impl HeldSearch {
    pub fn checkpoint(&self) -> &IndexCheckpoint {
        &self.checkpoint
    }

    /// The pinned searcher's own statistics, which score this partition in
    /// isolation. Comparable across shards only when every shard holds the
    /// whole corpus, so distributed scoring uses summed statistics instead.
    pub fn local_statistics(&self) -> &dyn Bm25StatisticsProvider {
        &self.searcher
    }
}

/// Cluster-wide BM25 inputs, summed by the coordinator from every shard's
/// held searcher. A lookup miss is a protocol violation rather than a zero: it
/// would silently score one shard against different statistics than its peers,
/// which is exactly the incomparability global scoring exists to remove.
pub struct GlobalBm25Statistics {
    schema: Schema,
    total_docs: u64,
    field_tokens: HashMap<Field, u64>,
    term_doc_freq: HashMap<Vec<u8>, u64>,
}

impl GlobalBm25Statistics {
    pub fn new(schema: Schema, statistics: &ShardStatistics) -> Result<Self> {
        let mut field_tokens = HashMap::with_capacity(statistics.field_tokens.len());
        for (name, tokens) in &statistics.field_tokens {
            let field = schema.get_field(name).map_err(|_| {
                Error::Search(format!("global statistics name an unknown field {name:?}"))
            })?;
            field_tokens.insert(field, *tokens);
        }
        Ok(Self {
            schema,
            total_docs: statistics.total_docs,
            field_tokens,
            term_doc_freq: statistics.term_doc_freq.iter().cloned().collect(),
        })
    }
}

impl Bm25StatisticsProvider for GlobalBm25Statistics {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        self.field_tokens.get(&field).copied().ok_or_else(|| {
            tantivy::TantivyError::InvalidArgument(
                "global search statistics are missing a scored field".into(),
            )
        })
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.total_docs)
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        self.term_doc_freq
            .get(&term_key(&self.schema, term))
            .copied()
            .ok_or_else(|| {
                tantivy::TantivyError::InvalidArgument(
                    "global search statistics are missing a query term".into(),
                )
            })
    }
}

/// A term identity that is stable across replicas and self-describing on the
/// wire: the field *name* rather than its schema-position id, then the value's
/// type tag and bytes. Keying by name means a statistics frame can never be
/// silently misapplied to a different field.
fn term_key(schema: &Schema, term: &Term) -> Vec<u8> {
    let name = schema.get_field_name(term.field());
    let value = term.serialized_value_bytes();
    let mut key = Vec::with_capacity(4 + name.len() + 1 + value.len());
    key.extend_from_slice(&(name.len() as u32).to_be_bytes());
    key.extend_from_slice(name.as_bytes());
    key.push(term.typ().to_code());
    key.extend_from_slice(value);
    key
}

/// One live reader generation and the checkpoint stored in that exact commit.
/// Keeping them behind the same lock prevents either direction of mismatch:
/// an old searcher with new metadata after reload failure, or a new searcher
/// with a checkpoint a caller validated just before a concurrent commit.
struct LoadedCommit {
    searcher: Searcher,
    checkpoint: Option<IndexCheckpoint>,
}

/// One local `(partition, index generation)` Tantivy projection.
pub struct LocalSearchIndex {
    group: GroupId,
    generation: SearchIndexGeneration,
    index: Index,
    fields: Fields,
    writer: Mutex<IndexWriter>,
    reader: IndexReader,
    loaded: RwLock<LoadedCommit>,
}

impl LocalSearchIndex {
    pub fn generation(&self) -> &SearchIndexGeneration {
        &self.generation
    }

    pub fn schema(&self) -> Schema {
        self.index.schema()
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
        // No writer can change the index while it is being opened, so this
        // payload belongs to the same latest commit the new reader loaded.
        let loaded_checkpoint = match read_checkpoint(&index) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                tracing::warn!(
                    ?group,
                    %error,
                    "unreadable search checkpoint; treating index as unprojected"
                );
                None
            }
        };
        Ok(Self {
            group,
            generation,
            index,
            fields,
            writer: Mutex::new(writer),
            loaded: RwLock::new(LoadedCommit {
                searcher: reader.searcher(),
                checkpoint: loaded_checkpoint,
            }),
            reader,
        })
    }

    pub fn checkpoint(&self) -> Result<Option<IndexCheckpoint>> {
        read_checkpoint(&self.index)
    }

    /// Validate the checkpoint paired with the reader searches will actually
    /// use, not merely the newest durable commit. A commit can succeed before a
    /// reader reload fails; in that state the durable checkpoint is useful for
    /// crash recovery, but it must not describe the still-old live searcher.
    pub fn validate_checkpoint(&self, epoch: u64) -> Result<Option<IndexCheckpoint>> {
        let checkpoint = self.loaded.read().unwrap().checkpoint.clone();
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

    /// Discard everything this writer has buffered since its last commit.
    ///
    /// A pass that fails between `project` and `commit` leaves its adds in the
    /// long-lived writer, where the next commit would publish them under a
    /// checkpoint that does not describe them (I5).
    pub fn rollback_uncommitted(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.rollback().map_err(search_error)?;
        Ok(())
    }

    /// Replace all projected documents and durably commit the exact source
    /// prefix represented by `snapshot`.
    pub fn rebuild(&self, snapshot: &SearchSourceSnapshot) -> Result<u64> {
        let mut writer = self.writer.lock().unwrap();
        // `delete_all_documents` only drops committed *segments*; documents an
        // earlier failed pass left buffered in this writer survive it and would
        // be committed below as part of a prefix they do not belong to.
        writer.rollback().map_err(search_error)?;
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
        fail::fail_point!(
            "search_index::before_reader_reload",
            self.group == GroupId::Data(u16::MAX),
            |_| Err(Error::Search(
                "injected search reader reload failure".into()
            ))
        );
        self.reader.reload().map_err(search_error)?;
        *self.loaded.write().unwrap() = LoadedCommit {
            searcher: self.reader.searcher(),
            checkpoint: Some(checkpoint),
        };
        Ok(())
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

    /// Pin one loaded Tantivy commit together with the parsed query, so shard
    /// statistics and the scored result they feed cannot observe two different
    /// commits (design §8.1). The caller supplies the checkpoint it already
    /// validated for the current projection epoch.
    pub fn hold(
        &self,
        query: &SearchQuery,
        generation: crate::search::GenerationSelection,
        window: u32,
        checkpoint: IndexCheckpoint,
    ) -> Result<HeldSearch> {
        if let crate::search::GenerationSelection::Exact(generation) = generation
            && generation != self.generation.id
        {
            return Err(Error::Search(
                "requested index generation is not loaded".into(),
            ));
        }
        if window > MAX_WINDOW {
            return Err(Error::Search(format!(
                "search result window exceeds {MAX_WINDOW}"
            )));
        }
        let query = self.parse_query(query)?;
        let loaded = self.loaded.read().unwrap();
        if loaded.checkpoint.as_ref() != Some(&checkpoint) {
            return Err(Error::Search(
                "index checkpoint changed before its searcher was pinned".into(),
            ));
        }
        Ok(HeldSearch {
            query,
            searcher: loaded.searcher.clone(),
            checkpoint,
        })
    }

    /// Local scoring inputs for one held searcher: corpus size, indexed tokens
    /// per field the query touches, and the document frequency of every
    /// expanded term. Terms are keyed by their serialized Tantivy encoding,
    /// which is comparable across replicas because an identical
    /// `definition_hash` implies an identical schema and field numbering.
    pub fn shard_statistics(&self, held: &HeldSearch) -> Result<ShardStatistics> {
        let schema = self.index.schema();
        let mut fields: HashMap<Field, u64> = HashMap::new();
        let mut terms: Vec<(Vec<u8>, u64)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut collect = Vec::new();
        held.query.query_terms(&mut |term, _positions| {
            collect.push(term.clone());
        });
        for term in collect {
            let encoded = term_key(&schema, &term);
            if !seen.insert(encoded.clone()) {
                continue;
            }
            if terms.len() >= crate::search::SEARCH_MAX_QUERY_TERMS {
                return Err(Error::Search(format!(
                    "query expands past the {} term statistics limit",
                    crate::search::SEARCH_MAX_QUERY_TERMS
                )));
            }
            let doc_freq = held.searcher.doc_freq(&term).map_err(search_error)?;
            terms.push((encoded, doc_freq));
            if let std::collections::hash_map::Entry::Vacant(slot) = fields.entry(term.field()) {
                slot.insert(
                    held.searcher
                        .total_num_tokens(term.field())
                        .map_err(search_error)?,
                );
            }
        }
        let mut field_tokens = Vec::with_capacity(fields.len());
        for (field, tokens) in fields {
            field_tokens.push((schema.get_field_name(field).to_string(), tokens));
        }
        // Deterministic order keeps the summed statistics — and therefore the
        // scores every shard computes — independent of hash-map iteration.
        field_tokens.sort();
        terms.sort();
        Ok(ShardStatistics {
            total_docs: held.searcher.num_docs(),
            field_tokens,
            term_doc_freq: terms,
        })
    }

    /// Score and collect the top `window` hits from a held searcher, taking
    /// BM25 inputs from `statistics`. Passing the held searcher itself yields
    /// partition-local scores; passing summed cluster statistics yields
    /// globally comparable ones (design §9.2).
    pub fn execute(
        &self,
        partition: u16,
        held: &HeldSearch,
        window: usize,
        statistics: &dyn Bm25StatisticsProvider,
    ) -> Result<crate::search::SearchReply> {
        let searcher = &held.searcher;
        let total_hits = searcher
            .search(held.query.as_ref(), &Count)
            .map_err(search_error)? as u64;
        let top = if window == 0 {
            Vec::new()
        } else {
            searcher
                .search_with_statistics_provider(
                    held.query.as_ref(),
                    &TopDocs::with_limit(window).order_by_score(),
                    statistics,
                )
                .map_err(search_error)?
        };
        let mut hits = Vec::with_capacity(top.len());
        for (score, address) in top {
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

    /// Execute a bounded local shard query against one loaded commit, scored
    /// from this partition's own statistics.
    pub fn search(
        &self,
        partition: u16,
        request: &SearchRequest,
        checkpoint: IndexCheckpoint,
    ) -> Result<crate::search::SearchReply> {
        if matches!(request.scoring, crate::search::ScoringMode::GlobalBm25) {
            return Err(Error::Search(
                "GlobalBm25 requires the distributed two-phase coordinator".into(),
            ));
        }
        if !request.sort.is_empty() {
            return Err(Error::Search("field sorting is not implemented".into()));
        }
        if request.limit > MAX_LIMIT {
            return Err(Error::Search(format!("search limit exceeds {MAX_LIMIT}")));
        }
        let window = request
            .limit
            .checked_add(request.offset)
            .ok_or_else(|| Error::Search("search page overflow".into()))?;
        let held = self.hold(&request.query, request.generation, window, checkpoint)?;
        let mut reply = self.execute(partition, &held, window as usize, &held.searcher)?;
        reply
            .hits
            .drain(..(request.offset as usize).min(reply.hits.len()));
        Ok(reply)
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

fn read_checkpoint(index: &Index) -> Result<Option<IndexCheckpoint>> {
    let payload = index.load_metas().map_err(search_error)?.payload;
    payload
        .map(|payload| {
            serde_json::from_str(&payload).map_err(|error| {
                Error::Search(format!("invalid Tantivy checkpoint payload: {error}"))
            })
        })
        .transpose()
}

fn search_error(error: impl std::fmt::Display) -> Error {
    Error::Search(error.to_string())
}
