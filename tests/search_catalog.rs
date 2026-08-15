use dal::keyspace;
use dal::meta::state_machine::{MetaApplyResult, MetaReject, MetaStateMachine};
use dal::search::{
    FieldKind, PathSegment, SEARCH_ENGINE_REVISION, SEARCH_MAX_INDEXES, SearchField,
    SearchIndexDefinition, SearchIndexReady, SearchIndexRecord,
};
use dal::storage::Storage;
use dal::types::{ClusterConfig, GroupId, HashSpec, LogId, MetaCommand, PROTOCOL_VERSION};
use serde::Serialize;

fn apply(
    state: &MetaStateMachine,
    storage: &Storage,
    index: &mut u64,
    command: MetaCommand,
) -> MetaApplyResult {
    *index += 1;
    state
        .apply(storage, &command, LogId::new(1, *index))
        .unwrap()
}

fn definition() -> SearchIndexDefinition {
    SearchIndexDefinition {
        document_type: "article".into(),
        fields: vec![SearchField {
            name: "body".into(),
            source_path: vec![PathSegment::Key("body".into())],
            kind: FieldKind::Text {
                tokenizer: "default".into(),
                positions: true,
            },
            required: true,
            multi_valued: false,
            indexed: true,
            stored: false,
            fast: false,
        }],
        default_search_fields: vec!["body".into()],
    }
}

#[test]
fn catalog_count_is_bounded_to_fit_full_catalog_frames() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open_checked(dir.path(), 1, 1).unwrap();
    storage.ensure_group(GroupId::Meta).unwrap();
    let state = MetaStateMachine::new();
    let mut index = 0u64;
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::ClusterInit {
                config: ClusterConfig {
                    cluster_id: 1,
                    protocol_version: PROTOCOL_VERSION,
                    p: 1,
                    r: 3,
                    hash_spec: HashSpec::CANONICAL,
                },
                meta_voters: vec![1, 2, 3],
            }
        ),
        MetaApplyResult::Applied
    );

    for ordinal in 0..SEARCH_MAX_INDEXES {
        assert_eq!(
            apply(
                &state,
                &storage,
                &mut index,
                MetaCommand::CreateSearchIndexWithRevision {
                    name: format!("index-{ordinal}"),
                    definition: definition(),
                    engine_revision: SEARCH_ENGINE_REVISION,
                }
            ),
            MetaApplyResult::Applied
        );
    }
    assert!(matches!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::CreateSearchIndexWithRevision {
                name: "one-too-many".into(),
                definition: definition(),
                engine_revision: SEARCH_ENGINE_REVISION,
            }
        ),
        MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
    ));
}

#[test]
fn activation_requires_every_current_partition_voter() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open_checked(dir.path(), 1, 1).unwrap();
    storage.ensure_group(GroupId::Meta).unwrap();
    let state = MetaStateMachine::new();
    let mut index = 0u64;
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::ClusterInit {
                config: ClusterConfig {
                    cluster_id: 1,
                    protocol_version: PROTOCOL_VERSION,
                    p: 1,
                    r: 3,
                    hash_spec: HashSpec::CANONICAL,
                },
                meta_voters: vec![1, 2, 3],
            }
        ),
        MetaApplyResult::Applied
    );
    for node_id in 1..=3 {
        assert_eq!(
            apply(
                &state,
                &storage,
                &mut index,
                MetaCommand::RegisterNode {
                    node_id,
                    control_addr: format!("c{node_id}"),
                    bulk_addr: format!("b{node_id}"),
                }
            ),
            MetaApplyResult::Applied
        );
    }
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::SeedPlacement {
                group: GroupId::Data(0),
                voters: vec![1, 2, 3]
            }
        ),
        MetaApplyResult::Applied
    );
    let placement: dal::types::Placement = storage
        .get_state_record(
            GroupId::Meta,
            &keyspace::meta_placement_key(GroupId::Data(0)),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::CreateSearchIndexWithRevision {
                name: "articles".into(),
                definition: definition(),
                engine_revision: SEARCH_ENGINE_REVISION,
            }
        ),
        MetaApplyResult::Applied
    );
    let generation = index;
    let definition_hash = definition().definition_hash().unwrap();

    for node_id in 1..=2 {
        assert_eq!(
            apply(
                &state,
                &storage,
                &mut index,
                MetaCommand::ReportSearchIndexReady {
                    name: "articles".into(),
                    observation: SearchIndexReady {
                        generation,
                        definition_hash,
                        engine_revision: SEARCH_ENGINE_REVISION,
                        group: GroupId::Data(0),
                        node_id,
                        voters_log_id: placement.voters_log_id,
                        projection_epoch: 1,
                        source_log_id: LogId::new(3, 20),
                    },
                }
            ),
            MetaApplyResult::Applied
        );
    }
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::ActivateSearchIndexGeneration {
                name: "articles".into(),
                generation
            }
        ),
        MetaApplyResult::Rejected(MetaReject::SearchGenerationNotReady)
    );
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::ReportSearchIndexReady {
                name: "articles".into(),
                observation: SearchIndexReady {
                    generation,
                    definition_hash,
                    engine_revision: SEARCH_ENGINE_REVISION,
                    group: GroupId::Data(0),
                    node_id: 3,
                    voters_log_id: placement.voters_log_id,
                    projection_epoch: 1,
                    source_log_id: LogId::new(3, 20),
                },
            }
        ),
        MetaApplyResult::Applied
    );
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::ActivateSearchIndexGeneration {
                name: "articles".into(),
                generation
            }
        ),
        MetaApplyResult::Applied
    );
    let record: SearchIndexRecord = storage
        .get_state_record(GroupId::Meta, &keyspace::meta_search_index_key("articles"))
        .unwrap()
        .unwrap();
    assert_eq!(record.active.unwrap().id, generation);
    assert!(record.building.is_none());
}

#[test]
fn engine_revision_is_part_of_search_command_idempotency() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open_checked(dir.path(), 1, 1).unwrap();
    storage.ensure_group(GroupId::Meta).unwrap();
    let state = MetaStateMachine::new();
    let mut index = 0u64;
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::ClusterInit {
                config: ClusterConfig {
                    cluster_id: 1,
                    protocol_version: PROTOCOL_VERSION,
                    p: 1,
                    r: 3,
                    hash_spec: HashSpec::CANONICAL,
                },
                meta_voters: vec![1, 2, 3],
            },
        ),
        MetaApplyResult::Applied
    );
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::CreateSearchIndexWithRevision {
                name: "articles".into(),
                definition: definition(),
                engine_revision: SEARCH_ENGINE_REVISION,
            },
        ),
        MetaApplyResult::Applied
    );
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::CreateSearchIndexWithRevision {
                name: "articles".into(),
                definition: definition(),
                engine_revision: SEARCH_ENGINE_REVISION,
            },
        ),
        MetaApplyResult::NoOp
    );
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::CreateSearchIndexWithRevision {
                name: "articles".into(),
                definition: definition(),
                engine_revision: SEARCH_ENGINE_REVISION + 1,
            },
        ),
        MetaApplyResult::Rejected(MetaReject::SearchIndexExists)
    );
    assert_eq!(
        apply(
            &state,
            &storage,
            &mut index,
            MetaCommand::CreateSearchIndexGenerationWithRevision {
                name: "articles".into(),
                definition: definition(),
                engine_revision: SEARCH_ENGINE_REVISION + 1,
            },
        ),
        MetaApplyResult::Rejected(MetaReject::SearchGenerationExists)
    );
}

#[test]
fn legacy_search_command_encoding_remains_decodable() {
    // The first eight placeholder variants preserve the legacy command's enum
    // discriminant. Their payloads do not affect this instantiated variant.
    #[allow(dead_code)]
    #[derive(Serialize)]
    enum LegacyMetaCommand {
        ClusterInit,
        RegisterNode,
        SeedPlacement,
        SetNodeState,
        CreatePlan,
        MarkAborting,
        FinalizePlan,
        AbortReport,
        CreateSearchIndex {
            name: String,
            definition: SearchIndexDefinition,
        },
    }

    let encoded = dal::codec::encode(&LegacyMetaCommand::CreateSearchIndex {
        name: "articles".into(),
        definition: definition(),
    });
    let decoded: MetaCommand = dal::codec::decode(&encoded).unwrap();
    assert_eq!(
        decoded,
        MetaCommand::CreateSearchIndex {
            name: "articles".into(),
            definition: definition(),
        }
    );
}
