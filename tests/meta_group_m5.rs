//! M5: the *running* meta Raft group (DESIGN §3.1, §5). Exercises the same
//! generic log store / channel network as the data groups (M3), but carrying
//! `MetaCommand`: bootstrap + init, command replication and convergence,
//! linearizable reads through the serving gate, leader failover preserving
//! committed placement, and follower restart from log.

use std::sync::Arc;
use std::time::Duration;

use dal::meta::bootstrap::ensure_bootstrap_group;
use dal::meta::node::{MetaNode, MetaRead, ProposeOutcome};
use dal::meta::raft_types::MetaTypeConfig;
use dal::meta::state_machine::{MetaApplyResult, MetaReject};
use dal::partition::network::{Faults, Registry};
use dal::search::{
    FieldKind, PathSegment, SEARCH_ENGINE_REVISION, SearchField, SearchIndexDefinition,
    SearchIndexReady,
};
use dal::storage::Storage;
use dal::types::{
    BootstrapGroup, ClusterConfig, DataConfigObservation, GroupId, HashSpec, LogId, MetaCommand,
    NodeId, PROTOCOL_VERSION,
};

use tempfile::TempDir;

const CID: u128 = 0xDA1;
const VOTERS: [u64; 3] = [1, 2, 3];

fn config() -> ClusterConfig {
    ClusterConfig {
        cluster_id: CID,
        protocol_version: PROTOCOL_VERSION,
        p: 8,
        r: 3,
        hash_spec: HashSpec::CANONICAL,
    }
}

struct Harness {
    dirs: Vec<TempDir>,
    registry: Registry<MetaTypeConfig>,
    faults: Faults,
}

impl Harness {
    fn new() -> Self {
        Harness {
            dirs: (0..3).map(|_| tempfile::tempdir().unwrap()).collect(),
            registry: Registry::default(),
            faults: Faults::default(),
        }
    }

    async fn start(&self, idx: usize) -> Arc<MetaNode> {
        let node_id = VOTERS[idx];
        let storage = Arc::new(Storage::open_checked(self.dirs[idx].path(), CID, node_id).unwrap());
        ensure_bootstrap_group(
            &storage,
            &BootstrapGroup {
                cluster_id: CID,
                group: GroupId::Meta,
                members: VOTERS.to_vec(),
            },
        )
        .unwrap();
        Arc::new(
            MetaNode::start(node_id, storage, self.registry.clone(), self.faults.clone())
                .await
                .unwrap(),
        )
    }
}

async fn eventually<F: Fn() -> bool>(what: &str, f: F) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

fn leader_idx(nodes: &[Arc<MetaNode>]) -> Option<usize> {
    let leader = nodes.iter().find_map(|n| n.current_leader())?;
    nodes.iter().position(|n| n.node_id() == leader)
}

/// Propose to whichever node is currently leader, retrying redirects. Returns
/// the applied result (which may be `NoOp` if a retry lands after commit).
async fn propose_to_leader(nodes: &[Arc<MetaNode>], cmd: MetaCommand) -> MetaApplyResult {
    for _ in 0..200 {
        if let Some(li) = leader_idx(nodes) {
            match nodes[li].propose(cmd.clone()).await {
                Ok(ProposeOutcome::Applied(r)) => return r,
                Ok(ProposeOutcome::NotLeader { .. }) | Err(_) => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader accepted the proposal");
}

async fn bootstrapped() -> (Harness, Vec<Arc<MetaNode>>) {
    let h = Harness::new();
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(h.start(i).await);
    }
    nodes[0].initialize(&VOTERS).await.unwrap();
    eventually("a leader is elected", || leader_idx(&nodes).is_some()).await;
    (h, nodes)
}

/// Drive a full bootstrap: init the cluster, register the voter nodes, and seed
/// one data partition's placement.
async fn init_register_seed(nodes: &[Arc<MetaNode>], partition: u16, voters: &[NodeId]) {
    init_register_seed_with_config(nodes, partition, voters, config()).await;
}

async fn init_register_seed_with_config(
    nodes: &[Arc<MetaNode>],
    partition: u16,
    voters: &[NodeId],
    cluster_config: ClusterConfig,
) {
    assert!(matches!(
        propose_to_leader(
            nodes,
            MetaCommand::ClusterInit {
                config: cluster_config,
                meta_voters: VOTERS.to_vec(),
            }
        )
        .await,
        MetaApplyResult::Applied
    ));
    for &n in &VOTERS {
        propose_to_leader(
            nodes,
            MetaCommand::RegisterNode {
                node_id: n,
                control_addr: format!("c{n}"),
                bulk_addr: format!("b{n}"),
            },
        )
        .await;
    }
    assert!(matches!(
        propose_to_leader(
            nodes,
            MetaCommand::SeedPlacement {
                group: GroupId::Data(partition),
                voters: voters.to_vec(),
            }
        )
        .await,
        MetaApplyResult::Applied
    ));
}

fn search_definition() -> SearchIndexDefinition {
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

async fn ready_search_index(nodes: &[Arc<MetaNode>]) -> (u64, SearchIndexReady) {
    let definition = search_definition();
    assert_eq!(
        propose_to_leader(
            nodes,
            MetaCommand::CreateSearchIndexWithRevision {
                name: "articles".into(),
                definition: definition.clone(),
                engine_revision: SEARCH_ENGINE_REVISION,
            }
        )
        .await,
        MetaApplyResult::Applied
    );
    let leader = &nodes[leader_idx(nodes).unwrap()];
    let placement = leader.local_placement(GroupId::Data(0)).unwrap().unwrap();
    let catalog = leader.local_search_catalog().unwrap();
    let generation = catalog[0].building.as_ref().unwrap().id;
    let observation = |node_id| SearchIndexReady {
        generation,
        definition_hash: definition.definition_hash().unwrap(),
        engine_revision: SEARCH_ENGINE_REVISION,
        group: GroupId::Data(0),
        node_id,
        voters_log_id: placement.voters_log_id,
        projection_epoch: 1,
        source_log_id: LogId::new(3, 20),
    };
    for node_id in VOTERS {
        assert_eq!(
            propose_to_leader(
                nodes,
                MetaCommand::ReportSearchIndexReady {
                    name: "articles".into(),
                    observation: observation(node_id),
                }
            )
            .await,
            MetaApplyResult::Applied
        );
    }
    (generation, observation(1))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_move_and_maintenance_begin_follow_log_order() {
    let (_h, nodes) = bootstrapped().await;
    init_register_seed(&nodes, 0, &VOTERS).await;
    assert_eq!(
        propose_to_leader(
            &nodes,
            MetaCommand::RegisterNode {
                node_id: 4,
                control_addr: "c4".into(),
                bulk_addr: "b4".into(),
            }
        )
        .await,
        MetaApplyResult::Applied
    );
    let start = Arc::new(tokio::sync::Barrier::new(3));
    let move_nodes = nodes.clone();
    let move_start = start.clone();
    let move_writer = tokio::spawn(async move {
        move_start.wait().await;
        propose_to_leader(
            &move_nodes,
            MetaCommand::CreatePlan {
                group: GroupId::Data(0),
                plan_id: 0,
                target_voters: vec![1, 2, 4],
            },
        )
        .await
    });
    let begin_nodes = nodes.clone();
    let begin_start = start.clone();
    let begin_writer = tokio::spawn(async move {
        begin_start.wait().await;
        propose_to_leader(
            &begin_nodes,
            MetaCommand::BeginMaintenance {
                operation_id: 0x51,
                generation: 1,
                participants: vec![1, 2, 3, 4],
            },
        )
        .await
    });
    start.wait().await;
    let move_result = move_writer.await.unwrap();
    let begin_result = begin_writer.await.unwrap();
    let leader = &nodes[leader_idx(&nodes).unwrap()];
    match (move_result, begin_result) {
        (MetaApplyResult::Applied, MetaApplyResult::Rejected(MetaReject::MaintenanceBusy)) => {
            assert!(
                leader
                    .local_placement(GroupId::Data(0))
                    .unwrap()
                    .unwrap()
                    .r#move
                    .is_some()
            );
            assert!(leader.automatic_meta_changes_allowed());
        }
        (MetaApplyResult::Rejected(MetaReject::MaintenanceActive), MetaApplyResult::Applied) => {
            assert!(
                leader
                    .local_placement(GroupId::Data(0))
                    .unwrap()
                    .unwrap()
                    .r#move
                    .is_none()
            );
            assert!(!leader.automatic_meta_changes_allowed());
        }
        other => panic!("move and maintenance violated log order: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_search_activation_and_maintenance_begin_follow_log_order() {
    let (_h, nodes) = bootstrapped().await;
    let mut cluster_config = config();
    cluster_config.p = 1;
    init_register_seed_with_config(&nodes, 0, &VOTERS, cluster_config).await;
    let (generation, _) = ready_search_index(&nodes).await;
    let start = Arc::new(tokio::sync::Barrier::new(3));
    let search_nodes = nodes.clone();
    let search_start = start.clone();
    let search_writer = tokio::spawn(async move {
        search_start.wait().await;
        propose_to_leader(
            &search_nodes,
            MetaCommand::ActivateSearchIndexGeneration {
                name: "articles".into(),
                generation,
            },
        )
        .await
    });
    let begin_nodes = nodes.clone();
    let begin_start = start.clone();
    let begin_writer = tokio::spawn(async move {
        begin_start.wait().await;
        propose_to_leader(
            &begin_nodes,
            MetaCommand::BeginMaintenance {
                operation_id: 0x51,
                generation: 1,
                participants: VOTERS.to_vec(),
            },
        )
        .await
    });
    start.wait().await;
    let search_result = search_writer.await.unwrap();
    let begin_result = begin_writer.await.unwrap();
    assert_eq!(search_result, MetaApplyResult::Applied);
    let leader = &nodes[leader_idx(&nodes).unwrap()];
    let catalog = leader.local_search_catalog().unwrap();
    assert_eq!(catalog[0].active.as_ref().unwrap().id, generation);
    match begin_result {
        MetaApplyResult::Applied => assert!(!leader.automatic_meta_changes_allowed()),
        MetaApplyResult::Rejected(MetaReject::MaintenanceBusy) => {
            assert!(leader.automatic_meta_changes_allowed())
        }
        other => panic!("search activation and maintenance violated log order: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_search_and_move_proposals_stay_behind_committed_maintenance() {
    let (_h, nodes) = bootstrapped().await;
    let mut cluster_config = config();
    cluster_config.p = 1;
    init_register_seed_with_config(&nodes, 0, &VOTERS, cluster_config).await;
    assert_eq!(
        propose_to_leader(
            &nodes,
            MetaCommand::RegisterNode {
                node_id: 4,
                control_addr: "c4".into(),
                bulk_addr: "b4".into(),
            }
        )
        .await,
        MetaApplyResult::Applied
    );
    let (generation, observation) = ready_search_index(&nodes).await;
    assert_eq!(
        propose_to_leader(
            &nodes,
            MetaCommand::ActivateSearchIndexGeneration {
                name: "articles".into(),
                generation,
            }
        )
        .await,
        MetaApplyResult::Applied
    );

    // These are the proposal shapes emitted by the automatic search and
    // rebalance drivers. Hold them after the producer's observation, so the
    // maintenance command can commit before their Raft submissions.
    let queued = [
        MetaCommand::ReportSearchIndexReady {
            name: "articles".into(),
            observation,
        },
        MetaCommand::ActivateSearchIndexGeneration {
            name: "articles".into(),
            generation,
        },
        MetaCommand::FinalizeSearchIndexDrop {
            name: "articles".into(),
            generation,
        },
        MetaCommand::CreatePlan {
            group: GroupId::Data(0),
            plan_id: 0,
            target_voters: vec![1, 2, 4],
        },
        MetaCommand::MarkAborting {
            group: GroupId::Data(0),
            plan_id: 0,
        },
        MetaCommand::FinalizePlan {
            group: GroupId::Data(0),
            plan_id: 0,
            observation: DataConfigObservation {
                group: GroupId::Data(0),
                plan_id: 0,
                voter_set: vec![1, 2, 4],
                config_log_id: LogId::new(3, 30),
            },
        },
        MetaCommand::AbortReport {
            group: GroupId::Data(0),
            plan_id: 0,
            observation: DataConfigObservation {
                group: GroupId::Data(0),
                plan_id: 0,
                voter_set: vec![1, 2, 3],
                config_log_id: LogId::new(3, 31),
            },
        },
    ];
    let ready = Arc::new(tokio::sync::Barrier::new(queued.len() + 1));
    let release = Arc::new(tokio::sync::Barrier::new(queued.len() + 1));
    let mut writers = Vec::new();
    for command in queued {
        let nodes = nodes.clone();
        let ready = ready.clone();
        let release = release.clone();
        writers.push(tokio::spawn(async move {
            ready.wait().await;
            release.wait().await;
            propose_to_leader(&nodes, command).await
        }));
    }
    ready.wait().await;
    assert_eq!(
        propose_to_leader(
            &nodes,
            MetaCommand::BeginMaintenance {
                operation_id: 0x51,
                generation: 1,
                participants: vec![1, 2, 3, 4],
            }
        )
        .await,
        MetaApplyResult::Applied
    );
    eventually("maintenance producer gate closes on every voter", || {
        nodes
            .iter()
            .all(|node| !node.automatic_meta_changes_allowed())
    })
    .await;
    let before_placement = nodes[leader_idx(&nodes).unwrap()]
        .local_placement(GroupId::Data(0))
        .unwrap();
    let before_catalog = nodes[leader_idx(&nodes).unwrap()]
        .local_search_catalog()
        .unwrap();
    release.wait().await;
    for writer in writers {
        assert_eq!(
            writer.await.unwrap(),
            MetaApplyResult::Rejected(MetaReject::MaintenanceActive)
        );
    }
    eventually("rejected proposals leave every voter unchanged", || {
        nodes.iter().all(|node| {
            node.local_placement(GroupId::Data(0)).unwrap() == before_placement
                && node.local_search_catalog().unwrap() == before_catalog
        })
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_init_and_linearizable_read() {
    let (_h, nodes) = bootstrapped().await;
    assert!(matches!(
        propose_to_leader(
            &nodes,
            MetaCommand::ClusterInit {
                config: config(),
                meta_voters: VOTERS.to_vec(),
            }
        )
        .await,
        MetaApplyResult::Applied
    ));

    let li = leader_idx(&nodes).unwrap();
    match nodes[li].read_cluster().await.unwrap() {
        MetaRead::Value(Some(cfg)) => assert_eq!(cfg, config()),
        other => panic!("leader cluster read returned {other:?}"),
    }

    // A follower must redirect, not serve routing state.
    let fi = (li + 1) % 3;
    assert!(matches!(
        nodes[fi].read_cluster().await.unwrap(),
        MetaRead::NotLeader { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commands_replicate_and_converge() {
    let (_h, nodes) = bootstrapped().await;
    init_register_seed(&nodes, 0, &[1, 2, 3]).await;

    // Every replica eventually holds the seeded placement locally.
    eventually("all replicas hold the placement", || {
        nodes.iter().all(|n| {
            matches!(
                n.local_placement(GroupId::Data(0)).unwrap(),
                Some(ref p) if p.voters == vec![1, 2, 3]
            )
        })
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_preserves_committed_meta() {
    let (_h, nodes) = bootstrapped().await;
    init_register_seed(&nodes, 0, &[1, 2, 3]).await;

    let old = leader_idx(&nodes).unwrap();
    nodes[old].shutdown().await.unwrap();

    let survivors: Vec<usize> = (0..3).filter(|&i| i != old).collect();
    eventually("survivors elect a new leader", || {
        survivors.iter().any(|&i| {
            nodes[i]
                .current_leader()
                .map(|l| l != nodes[old].node_id())
                .unwrap_or(false)
        })
    })
    .await;

    // The new leader still serves the committed placement via ReadIndex.
    let mut served = false;
    for _ in 0..200 {
        let li = survivors
            .iter()
            .copied()
            .find(|&i| nodes[i].current_leader() == Some(nodes[i].node_id()));
        if let Some(li) = li
            && let MetaRead::Value(Some(p)) =
                nodes[li].read_placement(GroupId::Data(0)).await.unwrap()
        {
            assert_eq!(p.voters, vec![1, 2, 3]);
            served = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(served, "new leader never served the committed placement");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_restarts_from_log() {
    let h = Harness::new();
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(h.start(i).await);
    }
    nodes[0].initialize(&VOTERS).await.unwrap();
    eventually("a leader is elected", || leader_idx(&nodes).is_some()).await;
    init_register_seed(&nodes, 0, &[1, 2, 3]).await;

    // Crash a follower (fully drop so RocksDB releases its lock), then restart.
    let li = leader_idx(&nodes).unwrap();
    let fi = (li + 1) % 3;
    let fnode_id = nodes[fi].node_id();
    let follower = nodes.remove(fi);
    follower.shutdown().await.unwrap();
    h.registry.remove(fnode_id);
    drop(follower);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let restarted = h.start(fi).await;

    eventually("restarted follower recovers the placement", || {
        matches!(
            restarted.local_placement(GroupId::Data(0)).unwrap(),
            Some(ref p) if p.voters == vec![1, 2, 3]
        )
    })
    .await;
}
