//! M5 gate: the plan-validation matrix inside the meta state machine (DESIGN
//! §3.1, §5.2, §7.5). Each command is applied against a real RocksDB-backed meta
//! group and the resulting placement/directory state is inspected. The balancer
//! property tests live in `placement_m5.rs`.

use dal::keyspace;
use dal::meta::state_machine::{MetaApplyResult, MetaReject, MetaStateMachine};
use dal::search::{
    FieldKind, PathSegment, SEARCH_ENGINE_REVISION, SearchField, SearchIndexDefinition,
    SearchIndexReady,
};
use dal::storage::Storage;
use dal::types::{
    ClusterConfig, DataConfigObservation, GroupId, HashSpec, LogId, MetaCommand,
    NodeDirectoryEntry, NodeId, NodeState, PROTOCOL_VERSION, Placement,
};

use tempfile::TempDir;

const CID: u128 = 0xDA1;

fn config(p: u16, r: u8) -> ClusterConfig {
    ClusterConfig {
        cluster_id: CID,
        protocol_version: PROTOCOL_VERSION,
        p,
        r,
        hash_spec: HashSpec::CANONICAL,
    }
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

struct Meta {
    _dir: TempDir,
    storage: Storage,
    sm: MetaStateMachine,
    idx: u64,
}

impl Meta {
    fn new() -> Meta {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open_checked(dir.path(), CID, 1).unwrap();
        storage.ensure_group(GroupId::Meta).unwrap();
        Meta {
            _dir: dir,
            storage,
            sm: MetaStateMachine::new(),
            idx: 0,
        }
    }

    fn apply(&mut self, cmd: MetaCommand) -> MetaApplyResult {
        self.idx += 1;
        self.sm
            .apply(&self.storage, &cmd, LogId::new(1, self.idx))
            .unwrap()
    }

    fn placement(&self, group: GroupId) -> Option<Placement> {
        self.storage
            .get_state_record(GroupId::Meta, &keyspace::meta_placement_key(group))
            .unwrap()
    }

    fn node(&self, node_id: NodeId) -> Option<NodeDirectoryEntry> {
        self.storage
            .get_state_record(GroupId::Meta, &keyspace::meta_node_key(node_id))
            .unwrap()
    }

    /// Init the cluster and register `nodes` as Active.
    fn bootstrap(&mut self, p: u16, r: u8, meta_voters: &[NodeId], nodes: &[NodeId]) {
        assert_eq!(
            self.apply(MetaCommand::ClusterInit {
                config: config(p, r),
                meta_voters: meta_voters.to_vec(),
            }),
            MetaApplyResult::Applied
        );
        for &n in nodes {
            self.apply(MetaCommand::RegisterNode {
                node_id: n,
                control_addr: format!("c{n}"),
                bulk_addr: format!("b{n}"),
            });
        }
    }

    fn seed(&mut self, group: GroupId, voters: &[NodeId]) -> MetaApplyResult {
        self.apply(MetaCommand::SeedPlacement {
            group,
            voters: voters.to_vec(),
        })
    }

    fn create_plan(&mut self, group: GroupId, target: &[NodeId]) -> (MetaApplyResult, u64) {
        let r = self.apply(MetaCommand::CreatePlan {
            group,
            plan_id: 0,
            target_voters: target.to_vec(),
        });
        (r, self.idx)
    }
}

fn obs(group: GroupId, plan_id: u64, voters: &[NodeId]) -> DataConfigObservation {
    DataConfigObservation {
        group,
        plan_id,
        voter_set: voters.to_vec(),
        config_log_id: LogId::new(2, 99),
    }
}

// ---------------------------------------------------------------------------
// Cluster init: immutability and byte-identical retry
// ---------------------------------------------------------------------------

#[test]
fn cluster_init_is_immutable_but_idempotent() {
    let mut m = Meta::new();
    assert_eq!(
        m.apply(MetaCommand::ClusterInit {
            config: config(8, 3),
            meta_voters: vec![1, 2, 3],
        }),
        MetaApplyResult::Applied
    );
    // Byte-identical retry is accepted as a no-op.
    assert_eq!(
        m.apply(MetaCommand::ClusterInit {
            config: config(8, 3),
            meta_voters: vec![3, 2, 1],
        }),
        MetaApplyResult::NoOp
    );
    // A differing re-init is a fork attempt and is rejected.
    assert_eq!(
        m.apply(MetaCommand::ClusterInit {
            config: config(16, 3),
            meta_voters: vec![1, 2, 3],
        }),
        MetaApplyResult::Rejected(MetaReject::ClusterConflict)
    );
}

#[test]
fn init_validates_config_and_meta_voters() {
    let mut m = Meta::new();
    // R below the minimum.
    assert!(matches!(
        m.apply(MetaCommand::ClusterInit {
            config: config(8, 2),
            meta_voters: vec![1, 2, 3],
        }),
        MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
    ));
    let mut wrong_cluster = config(8, 3);
    wrong_cluster.cluster_id = CID + 1;
    assert!(matches!(
        m.apply(MetaCommand::ClusterInit {
            config: wrong_cluster,
            meta_voters: vec![1, 2, 3],
        }),
        MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
    ));
    let mut wrong_protocol = config(8, 3);
    wrong_protocol.protocol_version = PROTOCOL_VERSION + 1;
    assert!(matches!(
        m.apply(MetaCommand::ClusterInit {
            config: wrong_protocol,
            meta_voters: vec![1, 2, 3],
        }),
        MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
    ));
    // A topology whose worst-case routing snapshot cannot fit MetaQuery.
    assert!(matches!(
        m.apply(MetaCommand::ClusterInit {
            config: config(u16::MAX, 3),
            meta_voters: vec![1, 2, 3],
        }),
        MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
    ));
    // Even meta voter set.
    assert!(matches!(
        m.apply(MetaCommand::ClusterInit {
            config: config(8, 3),
            meta_voters: vec![1, 2, 3, 4],
        }),
        MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
    ));
}

#[test]
fn node_registration_enforces_eight_node_limit_but_allows_rejoin() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3]);
    for node_id in 4..=8 {
        assert_eq!(
            m.apply(MetaCommand::RegisterNode {
                node_id,
                control_addr: format!("c{node_id}"),
                bulk_addr: format!("b{node_id}"),
            }),
            MetaApplyResult::Applied
        );
    }
    assert!(matches!(
        m.apply(MetaCommand::RegisterNode {
            node_id: 9,
            control_addr: "c9".into(),
            bulk_addr: "b9".into(),
        }),
        MetaApplyResult::Rejected(MetaReject::InvalidConfig(message)) if message.contains("<= 8")
    ));
    assert!(m.node(9).is_none());
    assert_eq!(
        m.apply(MetaCommand::RegisterNode {
            node_id: 8,
            control_addr: "c8-new".into(),
            bulk_addr: "b8-new".into(),
        }),
        MetaApplyResult::Applied
    );
}

#[test]
fn retirement_frees_capacity_and_tombstones_survive_snapshot_and_restart() {
    let mut m = Meta::new();
    m.bootstrap(1, 3, &[1, 2, 3], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(
        m.seed(GroupId::Data(0), &[1, 2, 3]),
        MetaApplyResult::Applied
    );
    assert_eq!(
        m.apply(MetaCommand::SetNodeState {
            node_id: 8,
            state: NodeState::Down,
            incarnation: 1
        }),
        MetaApplyResult::Applied
    );
    let register = |id| MetaCommand::RegisterNode {
        node_id: id,
        control_addr: format!("c{id}"),
        bulk_addr: format!("b{id}"),
    };
    assert!(matches!(
        m.apply(register(9)),
        MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
    ));
    assert_eq!(
        m.apply(MetaCommand::RetireNode {
            node_id: 8,
            incarnation: 1
        }),
        MetaApplyResult::Rejected(MetaReject::IneligibleTarget)
    );

    // More than eight lifetime identities, never more than eight current ones.
    for id in 8..=16 {
        assert_eq!(
            m.apply(MetaCommand::SetNodeState {
                node_id: id,
                state: NodeState::Draining,
                incarnation: 1
            }),
            MetaApplyResult::Applied
        );
        assert_eq!(
            m.apply(MetaCommand::RetireNode {
                node_id: id,
                incarnation: 2
            }),
            MetaApplyResult::Rejected(MetaReject::StaleIncarnation)
        );
        assert_eq!(
            m.apply(MetaCommand::RetireNode {
                node_id: id,
                incarnation: 1
            }),
            MetaApplyResult::Applied
        );
        assert_eq!(
            m.apply(MetaCommand::RetireNode {
                node_id: id,
                incarnation: 1
            }),
            MetaApplyResult::NoOp
        );
        assert!(m.node(id).is_none());
        assert_eq!(m.apply(register(id + 1)), MetaApplyResult::Applied);
        assert!(matches!(
            m.apply(register(id + 2)),
            MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
        ));
        assert!(matches!(
            m.apply(register(id)),
            MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
        ));
    }
    let snapshot_dir = tempfile::tempdir().unwrap();
    let snapshot = Storage::open_checked(snapshot_dir.path(), CID, 2).unwrap();
    snapshot.ensure_group(GroupId::Meta).unwrap();
    let applied: (
        Option<openraft::LogId<u64>>,
        openraft::StoredMembership<u64, openraft::BasicNode>,
    ) = (None, Default::default());
    snapshot
        .install_state(
            GroupId::Meta,
            &m.storage.scan_state(GroupId::Meta).unwrap(),
            &dal::codec::encode(&applied),
        )
        .unwrap();
    drop(snapshot);
    let snapshot = Storage::open_checked(snapshot_dir.path(), CID, 2).unwrap();
    for id in 8..=16 {
        assert_eq!(
            snapshot
                .get_state_record::<u64>(GroupId::Meta, &keyspace::meta_retired_node_key(id))
                .unwrap(),
            Some(1)
        );
        assert!(matches!(
            m.sm.apply(&snapshot, &register(id), LogId::new(2, m.idx + id))
                .unwrap(),
            MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
        ));
    }
}

#[test]
fn retirement_waits_for_all_voters_and_pending_or_aborting_learners() {
    for group in [GroupId::Meta, GroupId::Data(0)] {
        let mut m = Meta::new();
        m.bootstrap(1, 3, &[1, 2, 3], &[1, 2, 3, 4]);
        m.seed(GroupId::Data(0), &[1, 2, 3]);
        let (result, plan_id) = m.create_plan(group, &[1, 2, 4]);
        assert_eq!(result, MetaApplyResult::Applied);
        assert_eq!(
            m.apply(MetaCommand::SetNodeState {
                node_id: 4,
                state: NodeState::Draining,
                incarnation: 1
            }),
            MetaApplyResult::Applied
        );
        assert!(matches!(
            m.apply(MetaCommand::RetireNode {
                node_id: 4,
                incarnation: 1
            }),
            MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
        ));
        assert_eq!(
            m.apply(MetaCommand::MarkAborting { group, plan_id }),
            MetaApplyResult::Applied
        );
        assert!(matches!(
            m.apply(MetaCommand::RetireNode {
                node_id: 4,
                incarnation: 1
            }),
            MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
        ));
        assert_eq!(
            m.apply(MetaCommand::AbortReport {
                group,
                plan_id,
                observation: obs(group, plan_id, &[1, 2, 3])
            }),
            MetaApplyResult::Applied
        );
        assert_eq!(
            m.apply(MetaCommand::RetireNode {
                node_id: 4,
                incarnation: 1
            }),
            MetaApplyResult::Applied
        );
    }
    for (id, voters) in [(1, vec![1, 2, 3]), (4, vec![1, 2, 4])] {
        let mut m = Meta::new();
        m.bootstrap(1, 3, &[1, 2, 3], &[1, 2, 3, 4]);
        m.seed(GroupId::Data(0), &voters);
        m.apply(MetaCommand::SetNodeState {
            node_id: id,
            state: NodeState::Draining,
            incarnation: 1,
        });
        assert!(matches!(
            m.apply(MetaCommand::RetireNode {
                node_id: id,
                incarnation: 1
            }),
            MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
        ));
    }
}

#[test]
fn commands_before_init_are_rejected() {
    let mut m = Meta::new();
    assert_eq!(
        m.apply(MetaCommand::RegisterNode {
            node_id: 1,
            control_addr: "c1".into(),
            bulk_addr: "b1".into(),
        }),
        MetaApplyResult::Rejected(MetaReject::ClusterNotInitialized)
    );
    assert_eq!(
        m.seed(GroupId::Data(0), &[1, 2, 3]),
        MetaApplyResult::Rejected(MetaReject::ClusterNotInitialized)
    );
}

#[test]
fn explicit_registration_revives_a_down_node_at_the_same_addresses() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3]);
    assert_eq!(
        m.apply(MetaCommand::SetNodeState {
            node_id: 3,
            state: NodeState::Down,
            incarnation: 1,
        }),
        MetaApplyResult::Applied
    );

    assert_eq!(
        m.apply(MetaCommand::RegisterNode {
            node_id: 3,
            control_addr: "c3".into(),
            bulk_addr: "b3".into(),
        }),
        MetaApplyResult::Applied
    );
    let node = m.node(3).unwrap();
    assert_eq!(node.state, NodeState::Active);
    assert_eq!(node.incarnation, 2);
}

#[test]
fn registration_rejects_addresses_owned_by_another_node() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3]);
    assert!(matches!(
        m.apply(MetaCommand::RegisterNode {
            node_id: 4,
            control_addr: "b2".into(),
            bulk_addr: "b4".into(),
        }),
        MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
    ));
    assert!(m.node(4).is_none());

    assert!(matches!(
        m.apply(MetaCommand::RegisterNode {
            node_id: 4,
            control_addr: "c".repeat(dal::types::MAX_ENDPOINT_BYTES + 1),
            bulk_addr: "b4".into(),
        }),
        MetaApplyResult::Rejected(MetaReject::InvalidConfig(_))
    ));
}

// ---------------------------------------------------------------------------
// Seed placement
// ---------------------------------------------------------------------------

#[test]
fn seed_placement_rules() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3, 4]);

    assert_eq!(
        m.seed(GroupId::Data(0), &[1, 2, 3]),
        MetaApplyResult::Applied
    );
    // Identical re-seed is a no-op; a conflicting one is rejected.
    assert_eq!(m.seed(GroupId::Data(0), &[3, 2, 1]), MetaApplyResult::NoOp);
    assert_eq!(
        m.seed(GroupId::Data(0), &[1, 2, 4]),
        MetaApplyResult::Rejected(MetaReject::PlacementConflict)
    );
    // Wrong voter count and an ineligible target.
    assert!(matches!(
        m.seed(GroupId::Data(1), &[1, 2]),
        MetaApplyResult::Rejected(MetaReject::IllegalVoterChange(_))
    ));
    assert_eq!(
        m.seed(GroupId::Data(1), &[1, 2, 9]),
        MetaApplyResult::Rejected(MetaReject::IneligibleTarget)
    );
    // The meta placement is written by init, never by SeedPlacement.
    assert_eq!(
        m.seed(GroupId::Meta, &[1, 2, 3]),
        MetaApplyResult::Rejected(MetaReject::SeedMetaForbidden)
    );
    assert!(matches!(
        m.seed(GroupId::Data(8), &[1, 2, 3]),
        MetaApplyResult::Rejected(MetaReject::IllegalVoterChange(_))
    ));
    assert!(m.placement(GroupId::Data(8)).is_none());
}

// ---------------------------------------------------------------------------
// Create plan
// ---------------------------------------------------------------------------

#[test]
fn create_plan_requires_single_voter_change() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3, 4]);
    m.seed(GroupId::Data(0), &[1, 2, 3]);

    // A valid single-voter replacement.
    let (res, plan_idx) = m.create_plan(GroupId::Data(0), &[1, 2, 4]);
    assert_eq!(res, MetaApplyResult::Applied);
    let plan = m.placement(GroupId::Data(0)).unwrap().r#move.unwrap();
    assert_eq!(plan.plan_id, plan_idx, "plan_id is the meta log index");
    assert_eq!(plan.target_voters, vec![1, 2, 4]);

    // A second plan while one is in flight is refused.
    assert_eq!(
        m.create_plan(GroupId::Data(0), &[1, 2, 3]).0,
        MetaApplyResult::Rejected(MetaReject::PlanExists)
    );
}

#[test]
fn create_plan_rejects_bad_targets() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3, 4]);
    m.seed(GroupId::Data(0), &[1, 2, 3]);

    // Two-voter difference.
    assert!(matches!(
        m.create_plan(GroupId::Data(0), &[1, 4, 5]).0,
        MetaApplyResult::Rejected(MetaReject::IllegalVoterChange(_))
    ));
    // Wrong voter count.
    assert!(matches!(
        m.create_plan(GroupId::Data(0), &[1, 2, 3, 4]).0,
        MetaApplyResult::Rejected(MetaReject::IllegalVoterChange(_))
    ));
    // Target includes an ineligible node.
    assert_eq!(
        m.create_plan(GroupId::Data(0), &[1, 2, 9]).0,
        MetaApplyResult::Rejected(MetaReject::IneligibleTarget)
    );
    // No placement for this group at all.
    assert_eq!(
        m.create_plan(GroupId::Data(7), &[1, 2, 3]).0,
        MetaApplyResult::Rejected(MetaReject::NoPlacement)
    );
}

#[test]
fn meta_replacement_is_not_blocked_by_data_replication_factor() {
    let mut m = Meta::new();
    // Only four Active nodes exist, fewer than data replication factor five.
    // A valid three-voter meta replacement still preserves its own quorum and
    // must not be rejected by the data-group capacity rule.
    m.bootstrap(8, 5, &[1, 2, 3], &[1, 2, 3, 4]);
    assert_eq!(
        m.create_plan(GroupId::Meta, &[1, 2, 4]).0,
        MetaApplyResult::Applied
    );
}

// ---------------------------------------------------------------------------
// Finalize / abort (DESIGN §7.5)
// ---------------------------------------------------------------------------

#[test]
fn finalize_commits_target_voters() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3, 4]);
    m.seed(GroupId::Data(0), &[1, 2, 3]);
    let (_, plan_id) = m.create_plan(GroupId::Data(0), &[1, 2, 4]);

    // A finalize whose observation matches the target voters commits them.
    assert_eq!(
        m.apply(MetaCommand::FinalizePlan {
            group: GroupId::Data(0),
            plan_id,
            observation: obs(GroupId::Data(0), plan_id, &[1, 2, 4]),
        }),
        MetaApplyResult::Applied
    );
    let placement = m.placement(GroupId::Data(0)).unwrap();
    assert_eq!(placement.voters, vec![1, 2, 4]);
    assert!(placement.r#move.is_none());
    assert_eq!(placement.voters_log_id, LogId::new(2, 99));
}

#[test]
fn finalize_rejects_mismatched_observation() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3, 4]);
    m.seed(GroupId::Data(0), &[1, 2, 3]);
    let (_, plan_id) = m.create_plan(GroupId::Data(0), &[1, 2, 4]);

    // Wrong plan id.
    assert_eq!(
        m.apply(MetaCommand::FinalizePlan {
            group: GroupId::Data(0),
            plan_id: plan_id + 1,
            observation: obs(GroupId::Data(0), plan_id + 1, &[1, 2, 4]),
        }),
        MetaApplyResult::Rejected(MetaReject::PlanIdMismatch)
    );
    // Observation voter set does not equal the planned target.
    assert_eq!(
        m.apply(MetaCommand::FinalizePlan {
            group: GroupId::Data(0),
            plan_id,
            observation: obs(GroupId::Data(0), plan_id, &[1, 2, 3]),
        }),
        MetaApplyResult::Rejected(MetaReject::ObservationMismatch)
    );
}

#[test]
fn abort_only_clears_an_aborting_plan_rolled_back() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3, 4]);
    m.seed(GroupId::Data(0), &[1, 2, 3]);
    let (_, plan_id) = m.create_plan(GroupId::Data(0), &[1, 2, 4]);

    // An abort report for a healthy (non-aborting) plan is rejected outright.
    assert_eq!(
        m.apply(MetaCommand::AbortReport {
            group: GroupId::Data(0),
            plan_id,
            observation: obs(GroupId::Data(0), plan_id, &[1, 2, 3]),
        }),
        MetaApplyResult::Rejected(MetaReject::NotAborting)
    );

    // Mark aborting (one-way); re-marking is a no-op.
    assert_eq!(
        m.apply(MetaCommand::MarkAborting {
            group: GroupId::Data(0),
            plan_id,
        }),
        MetaApplyResult::Applied
    );
    assert_eq!(
        m.apply(MetaCommand::MarkAborting {
            group: GroupId::Data(0),
            plan_id,
        }),
        MetaApplyResult::NoOp
    );

    // Finalize is now barred; only an abort report resolves it.
    assert_eq!(
        m.apply(MetaCommand::FinalizePlan {
            group: GroupId::Data(0),
            plan_id,
            observation: obs(GroupId::Data(0), plan_id, &[1, 2, 4]),
        }),
        MetaApplyResult::Rejected(MetaReject::PlanAborting)
    );

    // An abort report must carry exactly `voters` or `target_voters`; anything
    // else is rejected (DESIGN §7.5).
    assert_eq!(
        m.apply(MetaCommand::AbortReport {
            group: GroupId::Data(0),
            plan_id,
            observation: obs(GroupId::Data(0), plan_id, &[1, 2, 9]),
        }),
        MetaApplyResult::Rejected(MetaReject::ObservationMismatch)
    );
    // Reporting `voters` clears the plan and rolls back.
    assert_eq!(
        m.apply(MetaCommand::AbortReport {
            group: GroupId::Data(0),
            plan_id,
            observation: obs(GroupId::Data(0), plan_id, &[1, 2, 3]),
        }),
        MetaApplyResult::Applied
    );
    let placement = m.placement(GroupId::Data(0)).unwrap();
    assert_eq!(placement.voters, vec![1, 2, 3], "voters roll back on abort");
    assert!(placement.r#move.is_none());
}

#[test]
fn abort_report_with_target_finalizes_a_completed_move() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3, 4]);
    m.seed(GroupId::Data(0), &[1, 2, 3]);
    let (_, plan_id) = m.create_plan(GroupId::Data(0), &[1, 2, 4]);
    m.apply(MetaCommand::MarkAborting {
        group: GroupId::Data(0),
        plan_id,
    });

    // The move actually completed (config == target) before the abort resolved:
    // the report carries `target_voters` and meta finalizes benignly.
    assert_eq!(
        m.apply(MetaCommand::AbortReport {
            group: GroupId::Data(0),
            plan_id,
            observation: obs(GroupId::Data(0), plan_id, &[1, 2, 4]),
        }),
        MetaApplyResult::Applied
    );
    let placement = m.placement(GroupId::Data(0)).unwrap();
    assert_eq!(placement.voters, vec![1, 2, 4], "late completion finalizes");
    assert!(placement.r#move.is_none());
}

// ---------------------------------------------------------------------------
// Meta membership: replacement / removal / floor of 3 voters
// ---------------------------------------------------------------------------

#[test]
fn meta_membership_change_rules() {
    let mut m = Meta::new();
    // Five meta voters so a single removal still leaves >= 3.
    m.bootstrap(8, 3, &[1, 2, 3, 4, 5], &[1, 2, 3, 4, 5, 6]);

    // A same-size single-voter replacement is allowed.
    assert_eq!(
        m.create_plan(GroupId::Meta, &[1, 2, 3, 4, 6]).0,
        MetaApplyResult::Applied
    );
    // Resolve it so the next test can propose again.
    let plan_id = m.placement(GroupId::Meta).unwrap().r#move.unwrap().plan_id;
    m.apply(MetaCommand::FinalizePlan {
        group: GroupId::Meta,
        plan_id,
        observation: obs(GroupId::Meta, plan_id, &[1, 2, 3, 4, 6]),
    });

    // A single-voter removal (5 → 4 voters) is allowed.
    assert_eq!(
        m.create_plan(GroupId::Meta, &[1, 2, 3, 4]).0,
        MetaApplyResult::Applied
    );
}

#[test]
fn meta_removal_cannot_drop_below_three() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3, 4]);
    // Removing from a 3-voter meta group would leave 2 — rejected.
    assert!(matches!(
        m.create_plan(GroupId::Meta, &[1, 2]).0,
        MetaApplyResult::Rejected(MetaReject::IllegalVoterChange(_))
    ));
}

// ---------------------------------------------------------------------------
// Node state transitions
// ---------------------------------------------------------------------------

#[test]
fn set_node_state_guards_incarnation() {
    let mut m = Meta::new();
    m.bootstrap(8, 3, &[1, 2, 3], &[1, 2, 3]);

    // Unknown node.
    assert_eq!(
        m.apply(MetaCommand::SetNodeState {
            node_id: 99,
            state: NodeState::Down,
            incarnation: 1,
        }),
        MetaApplyResult::Rejected(MetaReject::UnknownNode)
    );
    // Advance node 3 to Down at its current incarnation.
    assert_eq!(
        m.apply(MetaCommand::SetNodeState {
            node_id: 3,
            state: NodeState::Down,
            incarnation: 1,
        }),
        MetaApplyResult::Applied
    );
    // The same incarnation cannot reactivate a Down node.
    assert_eq!(
        m.apply(MetaCommand::SetNodeState {
            node_id: 3,
            state: NodeState::Active,
            incarnation: 1,
        }),
        MetaApplyResult::Rejected(MetaReject::StaleIncarnation)
    );
    // Nor can it leave Down for any other state (a Down -> Suspect -> Active
    // two-step must not bypass the rejoin requirement).
    assert_eq!(
        m.apply(MetaCommand::SetNodeState {
            node_id: 3,
            state: NodeState::Suspect,
            incarnation: 1,
        }),
        MetaApplyResult::Rejected(MetaReject::StaleIncarnation)
    );
    // Nor may a fabricated-higher incarnation force the reactivation. Only
    // `RegisterNode` advances the committed incarnation (§9.1), so liveness
    // evidence quoting anything but the recorded value is refused — otherwise
    // a forged `SetNodeState` could revive a Down node without the rejoin.
    assert_eq!(
        m.apply(MetaCommand::SetNodeState {
            node_id: 3,
            state: NodeState::Active,
            incarnation: 2,
        }),
        MetaApplyResult::Rejected(MetaReject::StaleIncarnation)
    );
    // Reactivation is the explicit operator-authorized rejoin, which advances
    // the incarnation itself.
    assert_eq!(
        m.apply(MetaCommand::RegisterNode {
            node_id: 3,
            control_addr: "c3".into(),
            bulk_addr: "b3".into(),
        }),
        MetaApplyResult::Applied
    );
    let entry = m.node(3).unwrap();
    assert_eq!(entry.state, NodeState::Active);
    assert_eq!(entry.incarnation, 2);
    // Liveness evidence must now quote the post-rejoin incarnation.
    assert_eq!(
        m.apply(MetaCommand::SetNodeState {
            node_id: 3,
            state: NodeState::Suspect,
            incarnation: 1,
        }),
        MetaApplyResult::Rejected(MetaReject::StaleIncarnation)
    );
    assert_eq!(
        m.apply(MetaCommand::SetNodeState {
            node_id: 3,
            state: NodeState::Suspect,
            incarnation: 2,
        }),
        MetaApplyResult::Applied
    );
}

fn maintenance_meta() -> Meta {
    let mut meta = Meta::new();
    meta.bootstrap(1, 3, &[1, 2, 3], &[1, 2, 3, 4]);
    assert_eq!(
        meta.seed(GroupId::Data(0), &[1, 2, 3]),
        MetaApplyResult::Applied
    );
    meta
}

fn begin_maintenance(meta: &mut Meta) -> MetaApplyResult {
    meta.apply(MetaCommand::BeginMaintenance {
        operation_id: 0x51,
        generation: 1,
        participants: vec![1, 2, 3, 4],
    })
}

#[test]
fn maintenance_waits_for_inflight_move_and_search_build() {
    let mut moving = maintenance_meta();
    assert_eq!(
        moving.create_plan(GroupId::Data(0), &[1, 2, 4]).0,
        MetaApplyResult::Applied
    );
    assert_eq!(
        begin_maintenance(&mut moving),
        MetaApplyResult::Rejected(MetaReject::MaintenanceBusy)
    );
    assert!(moving.placement(GroupId::Data(0)).unwrap().r#move.is_some());

    let mut building = maintenance_meta();
    assert_eq!(
        building.apply(MetaCommand::CreateSearchIndexWithRevision {
            name: "articles".into(),
            definition: search_definition(),
            engine_revision: SEARCH_ENGINE_REVISION,
        }),
        MetaApplyResult::Applied
    );
    assert_eq!(
        begin_maintenance(&mut building),
        MetaApplyResult::Rejected(MetaReject::MaintenanceBusy)
    );
    let record: dal::search::SearchIndexRecord = building
        .storage
        .get_state_record(GroupId::Meta, &keyspace::meta_search_index_key("articles"))
        .unwrap()
        .unwrap();
    assert!(record.building.is_some());
}

#[test]
fn queued_search_and_move_commands_cannot_cross_maintenance_fence() {
    let mut meta = maintenance_meta();
    let placement = meta.placement(GroupId::Data(0)).unwrap();
    let definition = search_definition();
    let definition_hash = definition.definition_hash().unwrap();
    assert_eq!(
        meta.apply(MetaCommand::CreateSearchIndexWithRevision {
            name: "articles".into(),
            definition: definition.clone(),
            engine_revision: SEARCH_ENGINE_REVISION,
        }),
        MetaApplyResult::Applied
    );
    let generation = meta.idx;
    let ready = |node_id| SearchIndexReady {
        generation,
        definition_hash,
        engine_revision: SEARCH_ENGINE_REVISION,
        group: GroupId::Data(0),
        node_id,
        voters_log_id: placement.voters_log_id,
        projection_epoch: 1,
        source_log_id: LogId::new(3, 20),
    };
    for node_id in 1..=3 {
        assert_eq!(
            meta.apply(MetaCommand::ReportSearchIndexReady {
                name: "articles".into(),
                observation: ready(node_id),
            }),
            MetaApplyResult::Applied
        );
    }
    assert_eq!(
        meta.apply(MetaCommand::ActivateSearchIndexGeneration {
            name: "articles".into(),
            generation,
        }),
        MetaApplyResult::Applied
    );
    assert_eq!(begin_maintenance(&mut meta), MetaApplyResult::Applied);

    let queued = vec![
        MetaCommand::CreatePlan {
            group: GroupId::Data(0),
            plan_id: 0,
            target_voters: vec![1, 2, 4],
        },
        MetaCommand::MarkAborting {
            group: GroupId::Data(0),
            plan_id: 99,
        },
        MetaCommand::FinalizePlan {
            group: GroupId::Data(0),
            plan_id: 99,
            observation: obs(GroupId::Data(0), 99, &[1, 2, 4]),
        },
        MetaCommand::AbortReport {
            group: GroupId::Data(0),
            plan_id: 99,
            observation: obs(GroupId::Data(0), 99, &[1, 2, 3]),
        },
        MetaCommand::CreateSearchIndexWithRevision {
            name: "other".into(),
            definition: definition.clone(),
            engine_revision: SEARCH_ENGINE_REVISION,
        },
        MetaCommand::CreateSearchIndexGenerationWithRevision {
            name: "articles".into(),
            definition,
            engine_revision: SEARCH_ENGINE_REVISION,
        },
        MetaCommand::ReportSearchIndexReady {
            name: "articles".into(),
            observation: ready(1),
        },
        MetaCommand::ActivateSearchIndexGeneration {
            name: "articles".into(),
            generation,
        },
        MetaCommand::DropSearchIndex {
            name: "articles".into(),
        },
        MetaCommand::FinalizeSearchIndexDrop {
            name: "articles".into(),
            generation,
        },
    ];
    let meta_records = |meta: &Meta| {
        meta.storage
            .scan_state(GroupId::Meta)
            .unwrap()
            .into_iter()
            .filter(|(key, _)| key.first() == Some(&keyspace::TAG_META))
            .collect::<Vec<_>>()
    };
    let assert_fenced = |meta: &mut Meta| {
        let before = meta_records(meta);
        for command in &queued {
            assert_eq!(
                meta.apply(command.clone()),
                MetaApplyResult::Rejected(MetaReject::MaintenanceActive),
                "queued command crossed maintenance fence: {command:?}"
            );
        }
        assert_eq!(meta_records(meta), before);
    };
    assert_fenced(&mut meta);
    for node_id in 1..=4 {
        assert_eq!(
            meta.apply(MetaCommand::AcknowledgeMaintenance {
                operation_id: 0x51,
                generation: 1,
                node_id,
                incarnation: 1,
            }),
            MetaApplyResult::Applied
        );
    }
    assert_eq!(
        meta.apply(MetaCommand::QuiesceMaintenance {
            operation_id: 0x51,
            generation: 1,
        }),
        MetaApplyResult::Applied
    );
    assert_fenced(&mut meta);
    assert_eq!(
        meta.apply(MetaCommand::EnterMaintenanceVerification {
            operation_id: 0x51,
            generation: 1,
        }),
        MetaApplyResult::Applied
    );
    assert_fenced(&mut meta);
}
