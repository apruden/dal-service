use dal::search::{
    FieldKind, GenerationSelection, IndexCheckpoint, LocalSearchIndex, PathSegment, ScoringMode,
    SearchConsistency, SearchField, SearchIndexDefinition, SearchIndexGeneration, SearchQuery,
    SearchRequest, SearchSourceSnapshot, encode_search_value,
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
