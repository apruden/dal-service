use dal::search::{
    FieldKind, GenerationSelection, GlobalBm25Statistics, IndexCheckpoint, LocalSearchIndex,
    PathSegment, ScoringMode, SearchConsistency, SearchField, SearchIndexDefinition,
    SearchIndexGeneration, SearchQuery, SearchRequest, SearchService, SearchSourceSnapshot,
    ShardStatistics, encode_search_value,
};
use dal::storage::{StateMutation, Storage};
use dal::types::GroupId;
use openraft::{CommittedLeaderId, LogId};
use serde::Serialize;
use std::sync::Arc;

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

#[derive(Serialize)]
struct Article<'a> {
    title: &'a str,
}

#[derive(Serialize)]
struct StoredKeyRecord {
    version: u64,
    value: Vec<u8>,
}

/// The loaded commit's own checkpoint. The service passes the checkpoint it
/// validated against the live projection epoch; a direct index test has no
/// epoch to validate against, so it reads back what the last commit wrote.
fn loaded(index: &LocalSearchIndex) -> IndexCheckpoint {
    index.checkpoint().unwrap().unwrap()
}

fn applied_record(log_id: LogId<u64>) -> Vec<u8> {
    dal::codec::encode(&(
        Some(log_id),
        openraft::StoredMembership::<u64, openraft::BasicNode>::default(),
    ))
}

fn stored_record(version: u64, value: Vec<u8>) -> Vec<u8> {
    dal::codec::encode(&StoredKeyRecord { version, value })
}

fn value(title: &str) -> Vec<u8> {
    let payload = flexbuffers::to_vec(Article { title }).unwrap();
    encode_search_value("article", &payload).unwrap()
}

#[test]
fn tantivy_projection_persists_checkpoint_and_searches_stored_identity() {
    let dir = tempfile::tempdir().unwrap();
    let generation = SearchIndexGeneration::new(9, definition()).unwrap();
    let index =
        LocalSearchIndex::open_or_create(dir.path(), GroupId::Data(2), generation.clone()).unwrap();
    index
        .rebuild(&SearchSourceSnapshot {
            epoch: 3,
            applied: Some(LogId::new(CommittedLeaderId::new(4, 1), 22)),
            records: vec![(
                b"doc-1".to_vec(),
                17,
                value("distributed search correctness"),
            )],
            outbox: Vec::new(),
        })
        .unwrap();

    let reply = index
        .search(
            2,
            &SearchRequest {
                index: "articles".into(),
                generation: GenerationSelection::Exact(9),
                query: SearchQuery::Text {
                    query: "correctness".into(),
                    fields: vec![],
                },
                limit: 10,
                offset: 0,
                sort: vec![],
                scoring: ScoringMode::LocalBm25,
                consistency: SearchConsistency::Eventual,
                allow_partial: false,
                deadline_ms: 1_000,
            },
            loaded(&index),
        )
        .unwrap();
    assert_eq!(reply.total_hits, 1);
    assert_eq!(reply.hits[0].key, b"doc-1");
    assert_eq!(reply.hits[0].version, 17);
    assert_eq!(
        index
            .checkpoint()
            .unwrap()
            .unwrap()
            .source_log_id
            .unwrap()
            .index,
        22
    );

    drop(index);
    let reopened =
        LocalSearchIndex::open_or_create(dir.path(), GroupId::Data(2), generation).unwrap();
    assert!(reopened.validate_checkpoint(3).unwrap().is_some());
    assert!(reopened.validate_checkpoint(4).unwrap().is_none());

    reopened
        .project(b"doc-1", Some((18, value("updated projection").as_slice())))
        .unwrap();
    reopened
        .commit(3, Some(LogId::new(CommittedLeaderId::new(4, 1), 23)))
        .unwrap();
    let mut updated = SearchRequest {
        index: "articles".into(),
        generation: GenerationSelection::Exact(9),
        query: SearchQuery::Text {
            query: "updated".into(),
            fields: vec![],
        },
        limit: 10,
        offset: 0,
        sort: vec![],
        scoring: ScoringMode::LocalBm25,
        consistency: SearchConsistency::Eventual,
        allow_partial: false,
        deadline_ms: 1_000,
    };
    assert_eq!(
        reopened
            .search(2, &updated, loaded(&reopened))
            .unwrap()
            .total_hits,
        1
    );
    updated.query = SearchQuery::Text {
        query: "correctness".into(),
        fields: vec![],
    };
    assert_eq!(
        reopened
            .search(2, &updated, loaded(&reopened))
            .unwrap()
            .total_hits,
        0
    );

    reopened.project(b"doc-1", None).unwrap();
    reopened
        .commit(3, Some(LogId::new(CommittedLeaderId::new(4, 1), 24)))
        .unwrap();
    updated.query = SearchQuery::MatchAll;
    assert_eq!(
        reopened
            .search(2, &updated, loaded(&reopened))
            .unwrap()
            .total_hits,
        0
    );
}

/// Design §9.2 and §16.3: locally computed BM25 is not comparable across
/// partitions, so a strict global search sums per-shard statistics and scores
/// every shard against them. The result must be indistinguishable from
/// searching one index holding the whole corpus.
/// I5: a rebuild publishes exactly the documents its snapshot selects.
///
/// `delete_all_documents` only drops committed segments, so adds a failed pass
/// left buffered in the long-lived writer survive it. Without an explicit
/// rollback they are committed by the rebuild and counted as part of a source
/// prefix they were never in.
#[test]
fn rebuild_discards_documents_buffered_by_a_failed_pass() {
    let dir = tempfile::tempdir().unwrap();
    let generation = SearchIndexGeneration::new(9, definition()).unwrap();
    let index = LocalSearchIndex::open_or_create(dir.path(), GroupId::Data(2), generation).unwrap();

    // A healthy incremental pass.
    index.project(b"k1", Some((1, &value("first")))).unwrap();
    index.commit(1, Some(log_id(1))).unwrap();

    // A pass that projects k2 and then fails before committing: the add stays
    // buffered in the writer the index owns.
    index.project(b"k2", Some((1, &value("second")))).unwrap();

    // Recovery rebuilds from an authoritative snapshot holding only k1.
    index
        .rebuild(&SearchSourceSnapshot {
            epoch: 2,
            applied: Some(log_id(5)),
            records: vec![(b"k1".to_vec(), 1, value("first"))],
            outbox: vec![],
        })
        .unwrap();

    let checkpoint = loaded(&index);
    assert_eq!(checkpoint.projection_epoch, 2);
    assert_eq!(checkpoint.source_log_id, Some(log_id(5)));
    assert_eq!(
        searched_keys(&index, checkpoint),
        vec![b"k1".to_vec()],
        "rebuild published a document absent from the snapshot it claims to cover"
    );
}

/// Every key the index currently returns for a match-all query.
fn searched_keys(index: &LocalSearchIndex, checkpoint: IndexCheckpoint) -> Vec<Vec<u8>> {
    let request = SearchRequest {
        index: "articles".into(),
        generation: GenerationSelection::Active,
        query: SearchQuery::MatchAll,
        limit: 100,
        offset: 0,
        sort: vec![],
        scoring: ScoringMode::LocalBm25,
        consistency: SearchConsistency::Eventual,
        allow_partial: false,
        deadline_ms: 5_000,
    };
    let mut keys: Vec<Vec<u8>> = index
        .search(0, &request, checkpoint)
        .unwrap()
        .hits
        .into_iter()
        .map(|hit| hit.key)
        .collect();
    keys.sort();
    keys
}

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}

#[test]
fn global_bm25_matches_a_single_index_reference_corpus() {
    let corpus: Vec<(&[u8], &str)> = vec![
        (b"a", "distributed search correctness"),
        (b"b", "search correctness"),
        (b"c", "distributed systems"),
        (b"d", "correctness proofs for distributed search"),
        (b"e", "an unrelated document about gardening"),
        (b"f", "search search search"),
    ];
    let build = |dir: &std::path::Path, partition: u16, rows: &[(&[u8], &str)]| {
        let index = LocalSearchIndex::open_or_create(
            dir,
            GroupId::Data(partition),
            SearchIndexGeneration::new(9, definition()).unwrap(),
        )
        .unwrap();
        index
            .rebuild(&SearchSourceSnapshot {
                epoch: 1,
                applied: Some(LogId::new(CommittedLeaderId::new(1, 1), 10)),
                records: rows
                    .iter()
                    .map(|(key, title)| (key.to_vec(), 1, value(title)))
                    .collect(),
                outbox: Vec::new(),
            })
            .unwrap();
        index
    };

    let query = SearchQuery::Text {
        query: "distributed search correctness".into(),
        fields: vec![],
    };
    let window = 10;

    let reference_dir = tempfile::tempdir().unwrap();
    let reference = build(reference_dir.path(), 0, &corpus);
    let reference_held = reference
        .hold(
            &query,
            GenerationSelection::Exact(9),
            window,
            loaded(&reference),
        )
        .unwrap();
    let expected = reference
        .execute(
            0,
            &reference_held,
            window as usize,
            reference_held.local_statistics(),
        )
        .unwrap();

    // The same corpus split unevenly across two partitions, so per-shard
    // document frequencies and average field lengths genuinely differ.
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let first = build(first_dir.path(), 0, &corpus[..2]);
    let second = build(second_dir.path(), 1, &corpus[2..]);

    let shards = [(0u16, &first), (1u16, &second)];
    let held: Vec<_> = shards
        .iter()
        .map(|(_, index)| {
            index
                .hold(&query, GenerationSelection::Exact(9), window, loaded(index))
                .unwrap()
        })
        .collect();

    let mut global = ShardStatistics::default();
    for ((_, index), held) in shards.iter().zip(&held) {
        global.merge(&index.shard_statistics(held).unwrap());
    }
    // Summing must reproduce the reference corpus exactly, or every score
    // derived from it is measuring a different collection.
    assert_eq!(
        global,
        reference.shard_statistics(&reference_held).unwrap(),
        "summed shard statistics must equal the whole-corpus statistics"
    );

    let mut merged = Vec::new();
    for ((partition, index), held) in shards.iter().zip(&held) {
        let statistics = GlobalBm25Statistics::new(index.schema(), &global).unwrap();
        merged.extend(
            index
                .execute(*partition, held, window as usize, &statistics)
                .unwrap()
                .hits,
        );
    }
    merged.sort_by(|left, right| {
        right
            .score
            .unwrap()
            .total_cmp(&left.score.unwrap())
            .then_with(|| left.key.cmp(&right.key))
    });

    let ranking = |hits: &[dal::search::SearchHit]| {
        hits.iter()
            .map(|hit| (hit.key.clone(), hit.score.unwrap().to_bits()))
            .collect::<Vec<_>>()
    };
    let mut reference_hits = expected.hits.clone();
    reference_hits.sort_by(|left, right| {
        right
            .score
            .unwrap()
            .total_cmp(&left.score.unwrap())
            .then_with(|| left.key.cmp(&right.key))
    });
    assert_eq!(ranking(&reference_hits), ranking(&merged));
    assert!(merged.len() >= 4, "corpus should produce several matches");
}

/// A shard that scores against statistics missing one of its query terms is
/// scoring a different collection than its peers. That must fail rather than
/// silently contribute incomparable hits to the merge.
#[test]
fn global_statistics_missing_a_term_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let index = LocalSearchIndex::open_or_create(
        dir.path(),
        GroupId::Data(0),
        SearchIndexGeneration::new(9, definition()).unwrap(),
    )
    .unwrap();
    index
        .rebuild(&SearchSourceSnapshot {
            epoch: 1,
            applied: Some(LogId::new(CommittedLeaderId::new(1, 1), 10)),
            records: vec![(b"a".to_vec(), 1, value("distributed search"))],
            outbox: Vec::new(),
        })
        .unwrap();
    let query = SearchQuery::Text {
        query: "distributed search".into(),
        fields: vec![],
    };
    let held = index
        .hold(&query, GenerationSelection::Exact(9), 10, loaded(&index))
        .unwrap();

    let mut truncated = index.shard_statistics(&held).unwrap();
    truncated.term_doc_freq.pop();
    let statistics = GlobalBm25Statistics::new(index.schema(), &truncated).unwrap();
    assert!(index.execute(0, &held, 10, &statistics).is_err());
}

#[tokio::test]
async fn raft_apply_publishes_user_mutation_and_outbox_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).unwrap());
    let group = GroupId::Data(1);
    storage.ensure_group(group).unwrap();
    let log_id = LogId::new(CommittedLeaderId::new(2, 7), 12);
    storage
        .apply_raft(
            group,
            &[StateMutation::Put {
                key: dal::keyspace::user_key(b"opaque/\0/key"),
                value: b"record".to_vec(),
            }],
            log_id,
            b"applied",
            b"committed",
            1,
        )
        .await
        .unwrap();

    assert_eq!(
        storage
            .get_state(group, &dal::keyspace::user_key(b"opaque/\0/key"))
            .unwrap(),
        Some(b"record".to_vec())
    );
    let outbox = storage.scan_search_outbox(group, 1).unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].source_log_id, log_id);
    assert_eq!(outbox[0].user_key, b"opaque/\0/key");
}

#[test]
fn snapshot_install_advances_projection_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let group = GroupId::Data(3);
    storage.ensure_group(group).unwrap();
    assert_eq!(storage.search_projection_epoch(group).unwrap(), 1);
    storage
        .install_state(group, &[], b"snapshot-applied")
        .unwrap();
    assert_eq!(storage.search_projection_epoch(group).unwrap(), 2);
}

#[tokio::test]
async fn outbox_pruning_waits_for_the_slowest_registered_generation() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let group = GroupId::Data(4);
    storage.ensure_group(group).unwrap();
    let first = SearchIndexGeneration::new(1, definition()).unwrap();
    let second = SearchIndexGeneration::new(2, definition()).unwrap();
    storage
        .register_search_consumer(group, "articles", &first)
        .unwrap();
    storage
        .register_search_consumer(group, "articles", &second)
        .unwrap();

    for index in [11, 12] {
        storage
            .apply_raft(
                group,
                &[StateMutation::Put {
                    key: dal::keyspace::user_key(format!("k{index}").as_bytes()),
                    value: vec![index as u8],
                }],
                LogId::new(CommittedLeaderId::new(2, 7), index),
                b"applied",
                b"committed",
                1,
            )
            .await
            .unwrap();
    }
    let checkpoint = |generation: &SearchIndexGeneration, index| IndexCheckpoint {
        projection_epoch: 1,
        definition_hash: generation.definition_hash,
        engine_revision: generation.engine_revision,
        source_log_id: Some(LogId::new(CommittedLeaderId::new(2, 7), index)),
    };
    storage
        .record_search_consumer_checkpoint(group, "articles", 1, checkpoint(&first, 12))
        .unwrap();
    assert_eq!(storage.prune_search_outbox(group).unwrap(), 0);
    storage
        .record_search_consumer_checkpoint(group, "articles", 2, checkpoint(&second, 11))
        .unwrap();
    assert_eq!(storage.prune_search_outbox(group).unwrap(), 1);
    assert_eq!(storage.scan_search_outbox(group, 1).unwrap().len(), 1);
    storage
        .record_search_consumer_checkpoint(group, "articles", 2, checkpoint(&second, 12))
        .unwrap();
    assert_eq!(storage.prune_search_outbox(group).unwrap(), 1);
    assert!(storage.scan_search_outbox(group, 1).unwrap().is_empty());
}

#[tokio::test]
async fn rebuild_clears_an_ahead_watermark_before_snapshotting() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let group = GroupId::Data(10);
    storage.ensure_group(group).unwrap();
    let generation = SearchIndexGeneration::new(1, definition()).unwrap();
    storage
        .register_search_consumer(group, "articles", &generation)
        .unwrap();

    for index in [11, 12] {
        let log_id = LogId::new(CommittedLeaderId::new(2, 7), index);
        storage
            .apply_raft(
                group,
                &[StateMutation::Put {
                    key: dal::keyspace::user_key(format!("k{index}").as_bytes()),
                    value: stored_record(index, value("article")),
                }],
                log_id,
                &applied_record(log_id),
                b"committed",
                1,
            )
            .await
            .unwrap();
    }
    storage
        .record_search_consumer_checkpoint(
            group,
            "articles",
            generation.id,
            IndexCheckpoint {
                projection_epoch: 1,
                definition_hash: generation.definition_hash,
                engine_revision: generation.engine_revision,
                source_log_id: Some(LogId::new(CommittedLeaderId::new(2, 7), 100)),
            },
        )
        .unwrap();

    storage
        .begin_search_consumer_rebuild(group, "articles", &generation)
        .unwrap();
    assert_eq!(storage.prune_search_outbox(group).unwrap(), 0);
    assert_eq!(storage.scan_search_outbox(group, 1).unwrap().len(), 2);
}

#[tokio::test]
async fn prune_failure_fences_storage_without_failing_the_applied_entry() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let group = GroupId::Data(11);
    storage.ensure_group(group).unwrap();
    storage
        .put_local(
            &dal::keyspace::search_consumer_key(group, "articles", 1),
            &1u8,
        )
        .unwrap();

    let boundary = LogId::new(CommittedLeaderId::new(2, 7), 4096);
    storage
        .apply_raft(
            group,
            &[],
            boundary,
            &applied_record(boundary),
            b"committed",
            1,
        )
        .await
        .unwrap();

    let next = LogId::new(CommittedLeaderId::new(2, 7), 4097);
    assert!(
        storage
            .apply_raft(group, &[], next, &applied_record(next), b"committed", 1,)
            .await
            .is_err()
    );
}

/// A consumer that has committed a checkpoint but not yet projected any Raft
/// entry still needs every retained entry. Dropping it from the watermark would
/// delete outbox entries it has not consumed, and it would then advance past
/// them with no error — silently losing those documents from its index.
#[tokio::test]
async fn outbox_pruning_retains_everything_for_a_consumer_that_has_projected_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let group = GroupId::Data(5);
    storage.ensure_group(group).unwrap();
    let unprojected = SearchIndexGeneration::new(1, definition()).unwrap();
    let advanced = SearchIndexGeneration::new(2, definition()).unwrap();
    storage
        .register_search_consumer(group, "articles", &unprojected)
        .unwrap();
    storage
        .register_search_consumer(group, "articles", &advanced)
        .unwrap();

    for index in [11, 12] {
        storage
            .apply_raft(
                group,
                &[StateMutation::Put {
                    key: dal::keyspace::user_key(format!("k{index}").as_bytes()),
                    value: vec![index as u8],
                }],
                LogId::new(CommittedLeaderId::new(2, 7), index),
                b"applied",
                b"committed",
                1,
            )
            .await
            .unwrap();
    }

    storage
        .record_search_consumer_checkpoint(
            group,
            "articles",
            1,
            IndexCheckpoint {
                projection_epoch: 1,
                definition_hash: unprojected.definition_hash,
                engine_revision: unprojected.engine_revision,
                source_log_id: None,
            },
        )
        .unwrap();
    storage
        .record_search_consumer_checkpoint(
            group,
            "articles",
            2,
            IndexCheckpoint {
                projection_epoch: 1,
                definition_hash: advanced.definition_hash,
                engine_revision: advanced.engine_revision,
                source_log_id: Some(LogId::new(CommittedLeaderId::new(2, 7), 12)),
            },
        )
        .unwrap();

    assert_eq!(storage.prune_search_outbox(group).unwrap(), 0);
    assert_eq!(storage.scan_search_outbox(group, 1).unwrap().len(), 2);
}

#[tokio::test]
async fn a_new_consumer_rebuilds_when_an_old_index_has_an_outbox_gap() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).unwrap());
    let group = GroupId::Data(6);
    storage.ensure_group(group).unwrap();
    let generation = SearchIndexGeneration::new(9, definition()).unwrap();
    let first = LogId::new(CommittedLeaderId::new(2, 7), 1);
    storage
        .apply_raft(
            group,
            &[StateMutation::Put {
                key: dal::keyspace::user_key(b"doc"),
                value: stored_record(1, value("old projection")),
            }],
            first,
            &applied_record(first),
            b"committed",
            1,
        )
        .await
        .unwrap();

    let service = SearchService::new(storage.clone()).unwrap();
    service
        .install_generation(6, "articles", generation.clone(), false)
        .await
        .unwrap();
    // Release the writer but leave the committed index on disk, then drop the
    // durable consumer record. That is the state this test is about: a stale
    // but valid-looking Tantivy commit whose outbox history is prunable.
    drop(service);
    storage
        .unregister_search_consumer(group, "articles", generation.id)
        .unwrap();

    let second = LogId::new(CommittedLeaderId::new(2, 7), 2);
    storage
        .apply_raft(
            group,
            &[StateMutation::Put {
                key: dal::keyspace::user_key(b"doc"),
                value: stored_record(2, value("new projection")),
            }],
            second,
            &applied_record(second),
            b"committed",
            1,
        )
        .await
        .unwrap();
    assert_eq!(storage.prune_search_outbox(group).unwrap(), 1);

    let service = SearchService::new(storage.clone()).unwrap();
    service
        .install_generation(6, "articles", generation.clone(), false)
        .await
        .unwrap();
    drop(service);

    let path = dir
        .path()
        .join("search")
        .join(group.token())
        .join("articles")
        .join(generation.id.to_string());
    let index = LocalSearchIndex::open_or_create(&path, group, generation).unwrap();
    let request = SearchRequest {
        index: "articles".into(),
        generation: GenerationSelection::Exact(9),
        query: SearchQuery::Text {
            query: "new".into(),
            fields: vec![],
        },
        limit: 10,
        offset: 0,
        sort: vec![],
        scoring: ScoringMode::LocalBm25,
        consistency: SearchConsistency::Eventual,
        allow_partial: false,
        deadline_ms: 1_000,
    };
    assert_eq!(
        index
            .search(6, &request, loaded(&index))
            .unwrap()
            .total_hits,
        1
    );
}

/// A crash after a consumer registration became durable but before its first
/// rebuild committed must still force a rebuild on the next install. The
/// durable record carries no checkpoint, so the stale on-disk Tantivy commit
/// is not proof of continuity across the pruned outbox gap.
#[tokio::test]
async fn reinstall_after_registration_crash_rebuilds_over_the_pruned_gap() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).unwrap());
    let group = GroupId::Data(9);
    storage.ensure_group(group).unwrap();
    let generation = SearchIndexGeneration::new(9, definition()).unwrap();
    let first = LogId::new(CommittedLeaderId::new(2, 7), 1);
    storage
        .apply_raft(
            group,
            &[StateMutation::Put {
                key: dal::keyspace::user_key(b"doc"),
                value: stored_record(1, value("old projection")),
            }],
            first,
            &applied_record(first),
            b"committed",
            1,
        )
        .await
        .unwrap();

    let service = SearchService::new(storage.clone()).unwrap();
    service
        .install_generation(9, "articles", generation.clone(), false)
        .await
        .unwrap();
    // Keep the committed index on disk; only the consumer record goes away.
    // The stale commit is what must not be mistaken for continuity below.
    drop(service);
    storage
        .unregister_search_consumer(group, "articles", generation.id)
        .unwrap();

    // While no consumer is registered, the gap is pruned.
    let second = LogId::new(CommittedLeaderId::new(2, 7), 2);
    storage
        .apply_raft(
            group,
            &[StateMutation::Put {
                key: dal::keyspace::user_key(b"doc"),
                value: stored_record(2, value("new projection")),
            }],
            second,
            &applied_record(second),
            b"committed",
            1,
        )
        .await
        .unwrap();
    storage.prune_search_outbox(group).unwrap();

    // Simulate the crash window: registration is durable (no checkpoint), but
    // the process died before the forced rebuild committed.
    let registration = storage
        .register_search_consumer(group, "articles", &generation)
        .unwrap();
    assert!(registration.created);

    // Restart: registration already exists, yet the install must rebuild
    // rather than resume from the stale on-disk commit.
    let service = SearchService::new(storage.clone()).unwrap();
    service
        .install_generation(9, "articles", generation.clone(), false)
        .await
        .unwrap();
    drop(service);

    let path = dir
        .path()
        .join("search")
        .join(group.token())
        .join("articles")
        .join(generation.id.to_string());
    let index = LocalSearchIndex::open_or_create(&path, group, generation).unwrap();
    let request = SearchRequest {
        index: "articles".into(),
        generation: GenerationSelection::Exact(9),
        query: SearchQuery::Text {
            query: "new".into(),
            fields: vec![],
        },
        limit: 10,
        offset: 0,
        sort: vec![],
        scoring: ScoringMode::LocalBm25,
        consistency: SearchConsistency::Eventual,
        allow_partial: false,
        deadline_ms: 1_000,
    };
    assert_eq!(
        index
            .search(9, &request, loaded(&index))
            .unwrap()
            .total_hits,
        1
    );
}

/// Design §6.5 and §12: a projection that stops making progress must not pin
/// the dirty-key journal forever. Once the journal crosses its budget the
/// slowest consumer is released, the space is reclaimed, and that consumer is
/// forced to rebuild rather than resume over a truncated journal.
#[tokio::test]
async fn a_lagging_index_is_released_so_the_outbox_cannot_grow_without_bound() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let group = GroupId::Data(11);
    storage.ensure_group(group).unwrap();
    let generation = SearchIndexGeneration::new(1, definition()).unwrap();
    storage
        .register_search_consumer(group, "articles", &generation)
        .unwrap();
    storage
        .record_search_consumer_checkpoint(
            group,
            "articles",
            1,
            IndexCheckpoint {
                projection_epoch: 1,
                definition_hash: generation.definition_hash,
                engine_revision: generation.engine_revision,
                source_log_id: Some(LogId::new(CommittedLeaderId::new(2, 7), 1)),
            },
        )
        .unwrap();

    for index in 2..=6u64 {
        let log_id = LogId::new(CommittedLeaderId::new(2, 7), index);
        storage
            .apply_raft(
                group,
                &[StateMutation::Put {
                    key: dal::keyspace::user_key(format!("doc-{index}").as_bytes()),
                    value: stored_record(index, value("indexed")),
                }],
                log_id,
                &applied_record(log_id),
                b"committed",
                1,
            )
            .await
            .unwrap();
    }
    // The consumer is stuck at index 1, so nothing is prunable.
    assert_eq!(storage.prune_search_outbox(group).unwrap(), 0);
    assert_eq!(storage.search_outbox_usage(group).unwrap().0, 5);

    // Under budget: the lagging consumer keeps its claim.
    assert_eq!(
        storage
            .enforce_search_outbox_bounds(group, 100, u64::MAX)
            .unwrap(),
        None
    );
    assert_eq!(storage.search_outbox_usage(group).unwrap().0, 5);

    // Over budget: it is released and the journal is reclaimed.
    assert_eq!(
        storage
            .enforce_search_outbox_bounds(group, 2, u64::MAX)
            .unwrap(),
        Some(("articles".to_string(), 1))
    );
    assert_eq!(storage.search_outbox_usage(group).unwrap().0, 0);
    assert!(
        storage
            .search_consumer_needs_rebuild(group, "articles", 1)
            .unwrap()
    );

    // Having lost its journal, the consumer may not claim an incremental
    // checkpoint — that would assert coverage of keys it never projected.
    let incremental = IndexCheckpoint {
        projection_epoch: 1,
        definition_hash: generation.definition_hash,
        engine_revision: generation.engine_revision,
        source_log_id: Some(LogId::new(CommittedLeaderId::new(2, 7), 6)),
    };
    assert!(
        storage
            .record_search_consumer_checkpoint(group, "articles", 1, incremental.clone())
            .is_err()
    );
    // Beginning the full rebuild re-acquires retention before its authoritative
    // snapshot. A mutation arriving while Tantivy is rebuilding must therefore
    // remain in the journal until the rebuilt checkpoint is durable.
    storage
        .begin_search_consumer_rebuild(group, "articles", &generation)
        .unwrap();
    let concurrent = LogId::new(CommittedLeaderId::new(2, 7), 7);
    storage
        .apply_raft(
            group,
            &[StateMutation::Put {
                key: dal::keyspace::user_key(b"concurrent"),
                value: stored_record(7, value("during rebuild")),
            }],
            concurrent,
            &applied_record(concurrent),
            b"committed",
            1,
        )
        .await
        .unwrap();
    assert_eq!(storage.prune_search_outbox(group).unwrap(), 0);
    storage
        .record_search_consumer_checkpoint(group, "articles", 1, incremental)
        .unwrap();
    assert_eq!(storage.scan_search_outbox(group, 1).unwrap().len(), 1);
    assert!(
        !storage
            .search_consumer_needs_rebuild(group, "articles", 1)
            .unwrap()
    );
}

/// Design §7.2: an unreadable index directory is quarantined and replaced,
/// never a permanent failure — Tantivy files are derived, rebuildable state.
#[test]
fn corrupt_index_directory_is_quarantined_and_recreated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("9");
    let generation = SearchIndexGeneration::new(9, definition()).unwrap();
    let index =
        LocalSearchIndex::open_or_create(&path, GroupId::Data(2), generation.clone()).unwrap();
    index
        .rebuild(&SearchSourceSnapshot {
            epoch: 3,
            applied: Some(LogId::new(CommittedLeaderId::new(4, 1), 22)),
            records: vec![(b"doc-1".to_vec(), 17, value("some article"))],
            outbox: Vec::new(),
        })
        .unwrap();
    drop(index);

    std::fs::write(path.join("meta.json"), b"not tantivy metadata").unwrap();
    let reopened = LocalSearchIndex::open_or_create(&path, GroupId::Data(2), generation).unwrap();
    // The recreated index is empty and unprojected: it must rebuild.
    assert!(reopened.validate_checkpoint(3).unwrap().is_none());
    assert!(dir.path().join("9.quarantined").is_dir());
}

#[tokio::test]
async fn coalesced_apply_crossing_a_boundary_prunes_without_consumers() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let group = GroupId::Data(7);
    storage.ensure_group(group).unwrap();
    let log_id = LogId::new(CommittedLeaderId::new(2, 7), 4097);
    storage
        .apply_raft(
            group,
            &[StateMutation::Put {
                key: dal::keyspace::user_key(b"doc"),
                value: b"record".to_vec(),
            }],
            log_id,
            b"applied",
            b"committed",
            2,
        )
        .await
        .unwrap();
    assert!(storage.scan_search_outbox(group, 1).unwrap().is_empty());
}

#[tokio::test]
async fn forgotten_partitions_reject_installs_until_readmitted() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).unwrap());
    storage.ensure_group(GroupId::Data(8)).unwrap();
    let service = SearchService::new(storage).unwrap();
    let generation = SearchIndexGeneration::new(1, definition()).unwrap();

    service.forget_partition(8).await;
    assert!(
        service
            .install_generation(8, "articles", generation.clone(), false)
            .await
            .is_err()
    );
    service.allow_partition(8).await;
    service
        .install_generation(8, "articles", generation, false)
        .await
        .unwrap();
}

/// The index name is a path component of the on-disk Tantivy directory, so a
/// dot-only name would escape the per-group index root and collide across every
/// partition on the node.
#[test]
fn dot_only_index_names_are_rejected() {
    for name in [".", "..", "..."] {
        assert!(
            dal::search::validate_index_name(name).is_err(),
            "index name {name:?} must be rejected"
        );
    }
    for name in ["articles", "articles.v2", "a.b-c_d"] {
        assert!(
            dal::search::validate_index_name(name).is_ok(),
            "index name {name:?} must be accepted"
        );
    }
}

/// A catalog record embeds a full definition in both `active` and `building`,
/// so the frame limit has to clear two maximum-size definitions plus framing or
/// a legal reply cannot be decoded.
#[test]
fn catalog_frames_fit_two_maximum_definitions() {
    const CATALOG_FRAMING_HEADROOM: usize = 4 * 1024;
    assert!(
        dal::transport::codec::MsgType::SearchCatalogQuery.max_payload()
            > 2 * dal::search::SEARCH_MAX_DEFINITION_BYTES
                + dal::search::SEARCH_MAX_RETIRING_GENERATIONS
                    * std::mem::size_of::<dal::search::SearchIndexGenerationId>()
                + CATALOG_FRAMING_HEADROOM
    );
    assert_eq!(
        dal::transport::codec::MsgType::SearchCatalogQuery.max_payload(),
        dal::search::SEARCH_MAX_CATALOG_FRAME_BYTES
    );
    const {
        assert!(
            dal::search::SEARCH_MAX_CATALOG_FRAME_BYTES
                > dal::search::SEARCH_MAX_INDEXES * dal::search::SEARCH_MAX_CATALOG_BYTES
        );
    }
}

#[test]
fn catalog_records_bound_the_retiring_generation_list() {
    let mut record = dal::search::SearchIndexRecord {
        name: "articles".into(),
        active: None,
        building: None,
        retiring: vec![1; dal::search::SEARCH_MAX_RETIRING_GENERATIONS],
    };
    assert!(record.validate().is_ok());
    record.retiring.push(2);
    assert!(record.validate().is_err());
}
