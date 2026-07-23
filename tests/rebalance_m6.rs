//! M6 gate (ChannelNetwork): the fenced move and abort drivers (DESIGN §7).
//! A join-driven move adds a node to a partition and finalizes; a planned
//! learner that never joins is aborted and a replacement plan then succeeds.
//! The pure gate/reconcile/failure logic is unit-tested in the library.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dal::meta::bootstrap::{
    record_data_bootstrap, record_meta_bootstrap, seed_cluster, BootstrapDescriptor, DirEntry,
};
use dal::meta::node::MetaNode;
use dal::meta::raft_types::MetaTypeConfig;
use dal::meta::node::MetaRead;
use dal::meta::rebalancer::{
    create_plan, drain_partition, execute_abort, execute_move, mark_aborting, resume_move,
};
use dal::meta::reconcile::{reconcile, ReconcileAction};
use dal::partition::network::{Faults, Registry};
use dal::partition::node::{PartitionNode, WriteOutcome};
use dal::partition::TypeConfig;
use dal::storage::Storage;
use dal::types::{
    ClusterConfig, DataOp, DataRequest, GroupId, HashSpec, NodeId, NodeState, PROTOCOL_VERSION,
};

use tempfile::TempDir;

const CID: u128 = 0xDA1;
const META_VOTERS: [u64; 3] = [1, 2, 3];
const DATA_VOTERS: [u64; 3] = [1, 2, 3];
const PART: u16 = 0;

fn config() -> ClusterConfig {
    ClusterConfig {
        cluster_id: CID,
        protocol_version: PROTOCOL_VERSION,
        p: 8,
        r: 3,
        hash_spec: HashSpec::CANONICAL,
    }
}

fn descriptor() -> BootstrapDescriptor {
    BootstrapDescriptor {
        cluster_id: CID,
        config: config(),
        meta_voters: META_VOTERS.to_vec(),
        directory: (1..=5)
            .map(|id| DirEntry {
                node_id: id,
                control_addr: format!("c{id}"),
                bulk_addr: format!("b{id}"),
            })
            .collect(),
        data_placements: vec![(PART, DATA_VOTERS.to_vec())],
    }
}

struct Cluster {
    paths: HashMap<NodeId, std::path::PathBuf>,
    _dirs: Vec<TempDir>,
    meta_reg: Registry<MetaTypeConfig>,
    data_reg: Registry<TypeConfig>,
    faults: Faults,
    storages: HashMap<NodeId, Arc<Storage>>,
    meta_nodes: HashMap<NodeId, Arc<MetaNode>>,
    data_nodes: HashMap<NodeId, Arc<PartitionNode>>,
}

impl Cluster {
    fn new() -> Cluster {
        let mut paths = HashMap::new();
        let mut dirs = Vec::new();
        for id in 1..=5u64 {
            let dir = tempfile::tempdir().unwrap();
            paths.insert(id, dir.path().to_path_buf());
            dirs.push(dir);
        }
        Cluster {
            paths,
            _dirs: dirs,
            meta_reg: Registry::default(),
            data_reg: Registry::default(),
            faults: Faults::default(),
            storages: HashMap::new(),
            meta_nodes: HashMap::new(),
            data_nodes: HashMap::new(),
        }
    }

    fn storage(&mut self, id: NodeId) -> Arc<Storage> {
        self.storages
            .entry(id)
            .or_insert_with(|| Arc::new(Storage::open_checked(&self.paths[&id], CID, id).unwrap()))
            .clone()
    }

    fn storage_list(&mut self, ids: &[NodeId]) -> Vec<(NodeId, Arc<Storage>)> {
        ids.iter().map(|&id| (id, self.storage(id))).collect()
    }

    async fn start_meta(&mut self, id: NodeId) -> Arc<MetaNode> {
        let storage = self.storage(id);
        let node = Arc::new(
            MetaNode::start(id, storage, self.meta_reg.clone(), self.faults.clone())
                .await
                .unwrap(),
        );
        self.meta_nodes.insert(id, node.clone());
        node
    }

    async fn start_data(&mut self, id: NodeId) -> Arc<PartitionNode> {
        let storage = self.storage(id);
        let node = Arc::new(
            PartitionNode::start(
                id,
                GroupId::Data(PART),
                storage,
                self.data_reg.clone(),
                self.faults.clone(),
            )
            .await
            .unwrap(),
        );
        self.data_nodes.insert(id, node.clone());
        node
    }

    fn meta_slice(&self) -> Vec<Arc<MetaNode>> {
        self.meta_nodes.values().cloned().collect()
    }

    fn meta_leader(&self) -> Option<Arc<MetaNode>> {
        self.meta_nodes
            .values()
            .find(|n| n.current_leader() == Some(n.node_id()))
            .cloned()
    }

    fn data_leader(&self) -> Option<Arc<PartitionNode>> {
        self.data_nodes
            .values()
            .find(|n| n.current_leader() == Some(n.node_id()))
            .cloned()
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

async fn bootstrap(c: &mut Cluster) {
    let desc = descriptor();
    record_meta_bootstrap(&c.storage_list(&META_VOTERS), &desc).unwrap();
    for &id in &META_VOTERS {
        c.start_meta(id).await;
    }
    c.meta_nodes[&1].initialize(&META_VOTERS).await.unwrap();
    let meta = c.meta_slice();
    eventually("meta leader", || meta.iter().any(|n| n.current_leader().is_some())).await;
    seed_cluster(&c.meta_slice(), &desc).await.unwrap();

    record_data_bootstrap(&c.storage_list(&DATA_VOTERS), &desc).unwrap();
    for &id in &DATA_VOTERS {
        c.start_data(id).await;
    }
    c.data_nodes[&1].initialize(&DATA_VOTERS).await.unwrap();
    let data: Vec<_> = c.data_nodes.values().cloned().collect();
    eventually("data leader", || data.iter().any(|n| n.current_leader().is_some())).await;
}

fn put(seq: u64, key: &[u8], val: &[u8]) -> DataRequest {
    DataRequest {
        client_id: 0xC0,
        sequence: seq,
        op: DataOp::Put {
            key: key.to_vec(),
            value: val.to_vec(),
            if_version: None,
        },
    }
}

async fn write_to_leader(c: &Cluster, req: DataRequest) {
    for _ in 0..200 {
        if let Some(leader) = c.data_leader() {
            if let Ok(WriteOutcome::Applied(_)) = leader.write(req.clone()).await {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no data leader accepted the write");
}

/// Meta view of the data partition's committed voter set.
async fn meta_voters(c: &Cluster) -> Vec<NodeId> {
    let leader = c.meta_leader().unwrap();
    match leader.read_placement(GroupId::Data(PART)).await.unwrap() {
        dal::meta::node::MetaRead::Value(Some(p)) => {
            let mut v = p.voters;
            v.sort_unstable();
            v
        }
        other => panic!("placement read: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn move_adds_node_and_finalizes() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;
    write_to_leader(&c, put(1, b"k", b"v1")).await;

    // Start node 4's data runtime so it can be add_learner'd.
    c.start_data(4).await;

    // Replace a non-leader voter with node 4, so leadership is not disturbed.
    let leader = c.data_leader().unwrap();
    let victim = DATA_VOTERS
        .iter()
        .copied()
        .find(|&v| v != leader.node_id())
        .unwrap();
    let mut target: Vec<NodeId> = DATA_VOTERS.iter().copied().filter(|&v| v != victim).collect();
    target.push(4);
    target.sort_unstable();

    let plan_id = create_plan(&c.meta_slice(), GroupId::Data(PART), &target)
        .await
        .unwrap();
    execute_move(&c.meta_slice(), &leader, GroupId::Data(PART), plan_id)
        .await
        .unwrap();

    // Meta now records the new voter set with no move in flight.
    assert_eq!(meta_voters(&c).await, target);
    // Node 4 caught up and holds the pre-move write.
    eventually("node 4 holds the data", || {
        matches!(c.data_nodes[&4].local_get(b"k").unwrap(), Some((_, ref v)) if v == b"v1")
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn planned_learner_never_joins_then_abort_and_replace() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;

    let leader = c.data_leader().unwrap();
    let victim = DATA_VOTERS
        .iter()
        .copied()
        .find(|&v| v != leader.node_id())
        .unwrap();

    // Plan to bring in node 4, which never starts (its runtime is down).
    let mut target4: Vec<NodeId> = DATA_VOTERS.iter().copied().filter(|&v| v != victim).collect();
    target4.push(4);
    target4.sort_unstable();
    let plan_id = create_plan(&c.meta_slice(), GroupId::Data(PART), &target4)
        .await
        .unwrap();

    // The planned learner is Down: abort the plan. The leader's committed config
    // is still the original voters, so the abort rolls back and clears it.
    mark_aborting(&c.meta_slice(), GroupId::Data(PART), plan_id)
        .await
        .unwrap();
    execute_abort(&c.meta_slice(), &leader, GroupId::Data(PART), plan_id)
        .await
        .unwrap();

    let mut original = DATA_VOTERS.to_vec();
    original.sort_unstable();
    assert_eq!(meta_voters(&c).await, original, "abort restores original voters");

    // A replacement plan with a live node (5) now succeeds.
    c.start_data(5).await;
    let mut target5: Vec<NodeId> = DATA_VOTERS.iter().copied().filter(|&v| v != victim).collect();
    target5.push(5);
    target5.sort_unstable();
    let plan_id2 = create_plan(&c.meta_slice(), GroupId::Data(PART), &target5)
        .await
        .unwrap();
    execute_move(&c.meta_slice(), &leader, GroupId::Data(PART), plan_id2)
        .await
        .unwrap();

    assert_eq!(meta_voters(&c).await, target5);
}

/// Graceful decommission (§7.3a): draining a follower swaps in a replacement
/// via a fenced move; the partition never drops below majority.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drain_replaces_a_follower() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;
    write_to_leader(&c, put(1, b"k", b"v1")).await;
    c.start_data(4).await;

    let leader = c.data_leader().unwrap();
    let draining = DATA_VOTERS
        .iter()
        .copied()
        .find(|&v| v != leader.node_id())
        .unwrap();

    drain_partition(&c.meta_slice(), &leader, GroupId::Data(PART), draining, 4)
        .await
        .unwrap();

    // The drained node is out of the voter set; node 4 is in and holds the data.
    let mut expected: Vec<NodeId> = DATA_VOTERS.iter().copied().filter(|&v| v != draining).collect();
    expected.push(4);
    expected.sort_unstable();
    assert_eq!(meta_voters(&c).await, expected);

    // The directory records the drained node as Draining.
    let meta_leader = c.meta_leader().unwrap();
    match meta_leader.read_node(draining).await.unwrap() {
        MetaRead::Value(Some(e)) => assert_eq!(e.state, NodeState::Draining),
        other => panic!("node read: {other:?}"),
    }

    eventually("node 4 holds the drained partition's data", || {
        matches!(c.data_nodes[&4].local_get(b"k").unwrap(), Some((_, ref v)) if v == b"v1")
    })
    .await;
}

/// A move interrupted after the learner is added (before promotion) resumes:
/// reconcile sees the committed config still equals `voters`, and re-running the
/// driver completes and finalizes idempotently (§5.2, §7.2).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn interrupted_move_resumes_from_committed_config() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;
    c.start_data(4).await;

    let leader = c.data_leader().unwrap();
    let victim = DATA_VOTERS
        .iter()
        .copied()
        .find(|&v| v != leader.node_id())
        .unwrap();
    let mut target: Vec<NodeId> = DATA_VOTERS.iter().copied().filter(|&v| v != victim).collect();
    target.push(4);
    target.sort_unstable();

    let plan_id = create_plan(&c.meta_slice(), GroupId::Data(PART), &target)
        .await
        .unwrap();

    // Simulate a crash after step 3 (learner added) but before promotion: the
    // committed config is still the original voters.
    leader.add_learner(4).await.unwrap();

    let placement = match c.meta_leader().unwrap().read_placement(GroupId::Data(PART)).await.unwrap() {
        MetaRead::Value(Some(p)) => p,
        other => panic!("placement: {other:?}"),
    };
    assert_eq!(
        reconcile(&placement, &leader.committed_voter_set()),
        ReconcileAction::ResumePlan,
        "an added-but-unpromoted learner must reconcile to ResumePlan"
    );

    // Re-run the driver: it resumes and finalizes.
    execute_move(&c.meta_slice(), &leader, GroupId::Data(PART), plan_id)
        .await
        .unwrap();
    assert_eq!(meta_voters(&c).await, target);
}

/// The replace-a-follower target for a plan that keeps the current leader.
fn follower_target(c: &Cluster) -> (Vec<NodeId>, u64) {
    let leader = c.data_leader().unwrap();
    let victim = DATA_VOTERS
        .iter()
        .copied()
        .find(|&v| v != leader.node_id())
        .unwrap();
    let mut target: Vec<NodeId> = DATA_VOTERS.iter().copied().filter(|&v| v != victim).collect();
    target.push(4);
    target.sort_unstable();
    (target, victim)
}

// -- crash-after-every-step resume matrix (DESIGN §5.2, §7) -------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn resume_after_create_plan() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;
    c.start_data(4).await;
    let (target, _) = follower_target(&c);
    let leader = c.data_leader().unwrap();

    // Crash right after step 1 (plan committed, nothing executed).
    create_plan(&c.meta_slice(), GroupId::Data(PART), &target)
        .await
        .unwrap();
    resume_move(&c.meta_slice(), &leader, GroupId::Data(PART))
        .await
        .unwrap();
    assert_eq!(meta_voters(&c).await, target);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn resume_after_add_learner() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;
    c.start_data(4).await;
    let (target, _) = follower_target(&c);
    let leader = c.data_leader().unwrap();

    create_plan(&c.meta_slice(), GroupId::Data(PART), &target)
        .await
        .unwrap();
    // Crash after step 3 (learner added, not promoted).
    leader.add_learner(4).await.unwrap();
    resume_move(&c.meta_slice(), &leader, GroupId::Data(PART))
        .await
        .unwrap();
    assert_eq!(meta_voters(&c).await, target);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn resume_after_change_membership() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;
    c.start_data(4).await;
    let (target, _) = follower_target(&c);
    let leader = c.data_leader().unwrap();

    create_plan(&c.meta_slice(), GroupId::Data(PART), &target)
        .await
        .unwrap();
    // Crash after step 4 (membership committed, meta not yet finalized).
    leader.add_learner(4).await.unwrap();
    leader.change_voters(&target).await.unwrap();
    resume_move(&c.meta_slice(), &leader, GroupId::Data(PART))
        .await
        .unwrap();
    assert_eq!(meta_voters(&c).await, target);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn duplicate_finalize_is_benign() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;
    c.start_data(4).await;
    let (target, _) = follower_target(&c);
    let leader = c.data_leader().unwrap();

    let plan_id = create_plan(&c.meta_slice(), GroupId::Data(PART), &target)
        .await
        .unwrap();
    execute_move(&c.meta_slice(), &leader, GroupId::Data(PART), plan_id)
        .await
        .unwrap();
    assert_eq!(meta_voters(&c).await, target);

    // A resume after the plan is fully resolved is a no-op (NoPlan): a delayed
    // or duplicate finalization cannot re-drive a cleared plan.
    resume_move(&c.meta_slice(), &leader, GroupId::Data(PART))
        .await
        .unwrap();
    assert_eq!(meta_voters(&c).await, target);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn abort_racing_a_completed_move_finalizes_benignly() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;
    c.start_data(4).await;
    let (target, _) = follower_target(&c);
    let leader = c.data_leader().unwrap();

    let plan_id = create_plan(&c.meta_slice(), GroupId::Data(PART), &target)
        .await
        .unwrap();
    // The move actually completes (committed config == target) ...
    leader.add_learner(4).await.unwrap();
    leader.change_voters(&target).await.unwrap();
    // ... but an abort is marked concurrently. The leader observes the target
    // config and finalizes benignly rather than rolling back (§7.5).
    mark_aborting(&c.meta_slice(), GroupId::Data(PART), plan_id)
        .await
        .unwrap();
    execute_abort(&c.meta_slice(), &leader, GroupId::Data(PART), plan_id)
        .await
        .unwrap();
    assert_eq!(meta_voters(&c).await, target);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cleared_plan_cannot_be_resurrected() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;
    let leader = c.data_leader().unwrap();
    let (target, _) = follower_target(&c);

    let plan_id = create_plan(&c.meta_slice(), GroupId::Data(PART), &target)
        .await
        .unwrap();
    // Abort before the learner ever joins: the plan clears, rolling back.
    mark_aborting(&c.meta_slice(), GroupId::Data(PART), plan_id)
        .await
        .unwrap();
    execute_abort(&c.meta_slice(), &leader, GroupId::Data(PART), plan_id)
        .await
        .unwrap();

    let mut original = DATA_VOTERS.to_vec();
    original.sort_unstable();
    assert_eq!(meta_voters(&c).await, original);

    // A replayed abort report for the cleared plan is refused, not honoured.
    assert!(
        execute_abort(&c.meta_slice(), &leader, GroupId::Data(PART), plan_id)
            .await
            .is_err(),
        "a cleared plan must not be re-abortable"
    );
    // And reconciliation is a no-op (no plan to resume).
    resume_move(&c.meta_slice(), &leader, GroupId::Data(PART))
        .await
        .unwrap();
    assert_eq!(meta_voters(&c).await, original);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn drain_removes_the_current_leader() {
    let mut c = Cluster::new();
    bootstrap(&mut c).await;
    write_to_leader(&c, put(1, b"k", b"v1")).await;
    c.start_data(4).await;

    // Drain the *leader*: change_membership removes it, openraft steps it down
    // after the final config commits, and the move still finalizes.
    let leader = c.data_leader().unwrap();
    let draining = leader.node_id();
    drain_partition(&c.meta_slice(), &leader, GroupId::Data(PART), draining, 4)
        .await
        .unwrap();

    let mut expected: Vec<NodeId> = DATA_VOTERS.iter().copied().filter(|&v| v != draining).collect();
    expected.push(4);
    expected.sort_unstable();
    assert_eq!(meta_voters(&c).await, expected);
}
