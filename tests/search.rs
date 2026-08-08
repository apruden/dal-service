use dal::search::{
    FieldKind, GenerationSelection, IndexCheckpoint, LocalSearchIndex, PathSegment, ScoringMode,
    SearchConsistency, SearchField, SearchIndexDefinition, SearchIndexGeneration, SearchQuery,
    SearchRequest, SearchService, SearchSourceSnapshot, encode_search_value,
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
    assert_eq!(reopened.search(2, &updated).unwrap().total_hits, 1);
    updated.query = SearchQuery::Text {
        query: "correctness".into(),
        fields: vec![],
    };
    assert_eq!(reopened.search(2, &updated).unwrap().total_hits, 0);

    reopened.project(b"doc-1", None).unwrap();
    reopened
        .commit(3, Some(LogId::new(CommittedLeaderId::new(4, 1), 24)))
        .unwrap();
    updated.query = SearchQuery::MatchAll;
    assert_eq!(reopened.search(2, &updated).unwrap().total_hits, 0);
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

    let service = SearchService::new(storage.clone());
    service
        .install_generation(6, "articles", generation.clone(), false)
        .await
        .unwrap();
    service
        .remove_generation(6, "articles", generation.id)
        .await
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

    service
        .install_generation(6, "articles", generation.clone(), false)
        .await
        .unwrap();
    service
        .remove_generation(6, "articles", generation.id)
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
    assert_eq!(index.search(6, &request).unwrap().total_hits, 1);
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

    let service = SearchService::new(storage.clone());
    service
        .install_generation(9, "articles", generation.clone(), false)
        .await
        .unwrap();
    service
        .remove_generation(9, "articles", generation.id)
        .await
        .unwrap();
    drop(service);

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
    let service = SearchService::new(storage.clone());
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
    assert_eq!(index.search(9, &request).unwrap().total_hits, 1);
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
    let service = SearchService::new(storage);
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
        dal::search::SEARCH_MAX_CATALOG_BYTES
    );
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
