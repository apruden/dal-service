//! M5 gate: the plan-validation matrix inside the meta state machine (DESIGN
//! §3.1, §5.2, §7.5). Each command is applied against a real RocksDB-backed meta
//! group and the resulting placement/directory state is inspected. The balancer
//! property tests live in `placement_m5.rs`.

use dal::keyspace;
use dal::meta::state_machine::{MetaApplyResult, MetaReject, MetaStateMachine};
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
    // Reactivation is a rejoin and must carry a new incarnation.
    assert_eq!(
        m.apply(MetaCommand::SetNodeState {
            node_id: 3,
            state: NodeState::Active,
            incarnation: 2,
        }),
        MetaApplyResult::Applied
    );
}
