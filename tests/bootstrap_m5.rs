//! M5 gate: the resumable bootstrap driver and cluster lifecycle (DESIGN §3.1,
//! §5). Runs full nodes hosting a meta group and a data partition on shared
//! per-node storage, and exercises: byte-identical / conflicting bootstrap
//! records, a full bootstrap that then serves, resumability, full-cluster
//! restart recovery, meta-quorum loss while a data group keeps serving, and
//! learner-first meta membership growth surviving restart.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use dal::meta::bootstrap::{
    BootstrapDescriptor, BootstrapOutcome, DirEntry, admit_and_promote, ensure_bootstrap_group,
    ensure_learner_admission, record_data_bootstrap, record_meta_bootstrap, seed_cluster,
};
use dal::meta::node::{MetaNode, MetaRead};
use dal::meta::raft_types::MetaTypeConfig;
use dal::partition::TypeConfig;
use dal::partition::network::{Faults, Registry};
use dal::partition::node::{PartitionNode, ReadOutcome, WriteOutcome};
use dal::storage::Storage;
use dal::types::{
    BootstrapGroup, ClusterConfig, DataOp, DataRequest, GroupId, HashSpec, LearnerAdmission,
    NodeId, PROTOCOL_VERSION,
};

use tempfile::TempDir;

const CID: u128 = 0xDA1;
const META_VOTERS: [u64; 3] = [1, 2, 3];
const DATA_VOTERS: [u64; 3] = [3, 4, 5];
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

/// Full-node harness: per-node storage shared by that node's meta and data Raft
/// instances, plus one registry per group type.
struct Cluster {
    paths: HashMap<NodeId, PathBuf>,
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

    /// Stop every running Raft instance and release all storage handles so the
    /// dirs can be reopened (models a full-cluster shutdown).
    async fn stop_all(&mut self) {
        for n in self.meta_nodes.values() {
            let _ = n.shutdown().await;
            self.meta_reg.remove(n.node_id());
        }
        for n in self.data_nodes.values() {
            let _ = n.shutdown().await;
            self.data_reg.remove(n.node_id());
        }
        self.meta_nodes.clear();
        self.data_nodes.clear();
        self.storages.clear();
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    /// Kill one node's meta instance only (its data instance, if any, keeps
    /// running) — used to drop meta below quorum.
    async fn kill_meta(&mut self, id: NodeId) {
        if let Some(n) = self.meta_nodes.remove(&id) {
            let _ = n.shutdown().await;
            self.meta_reg.remove(id);
        }
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

/// Run the full resumable bootstrap: meta records + group, seed cluster, data
/// records + group. Idempotent, so callable twice.
async fn full_bootstrap(c: &mut Cluster) {
    let desc = descriptor();

    // Phase 2: durable meta bootstrap records, then start the meta group.
    record_meta_bootstrap(&c.storage_list(&META_VOTERS), &desc).unwrap();
    for &id in &META_VOTERS {
        c.start_meta(id).await;
    }
    // Phase 3: the deterministic designated node initializes the meta group.
    let meta_designated = *META_VOTERS.iter().min().unwrap();
    let node = c.meta_nodes[&meta_designated].clone();
    if !node.is_initialized().await.unwrap() {
        node.initialize(&META_VOTERS).await.unwrap();
    }
    let meta = c.meta_slice();
    eventually("meta leader", || {
        meta.iter().any(|n| n.current_leader().is_some())
    })
    .await;

    // Phases 3–4: commit cluster record, directory, placements.
    seed_cluster(&c.meta_slice(), &desc).await.unwrap();

    // Phase 5: durable data bootstrap records, start the data group, init it.
    record_data_bootstrap(&c.storage_list(&DATA_VOTERS), &desc).unwrap();
    for &id in &DATA_VOTERS {
        c.start_data(id).await;
    }
    let data_designated = *DATA_VOTERS.iter().min().unwrap();
    let dnode = c.data_nodes[&data_designated].clone();
    if !dnode.is_initialized().await.unwrap() {
        dnode.initialize(&DATA_VOTERS).await.unwrap();
    }
    let data: Vec<_> = c.data_nodes.values().cloned().collect();
    eventually("data leader", || {
        data.iter().any(|n| n.current_leader().is_some())
    })
    .await;
}

async fn data_write(c: &Cluster, req: DataRequest) -> bool {
    for _ in 0..200 {
        if let Some(leader) = c.data_leader()
            && let Ok(WriteOutcome::Applied(_)) = leader.write(req.clone()).await
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn data_read(c: &Cluster, key: &[u8]) -> Option<Vec<u8>> {
    for _ in 0..200 {
        if let Some(leader) = c.data_leader()
            && let Ok(ReadOutcome::Value(v)) = leader.read(key).await
        {
            return v.map(|(_, bytes)| bytes);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
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

// ---------------------------------------------------------------------------
// Durable record semantics
// ---------------------------------------------------------------------------

#[test]
fn bootstrap_records_accept_identical_reject_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open_checked(dir.path(), CID, 1).unwrap();

    let record = BootstrapGroup {
        cluster_id: CID,
        group: GroupId::Meta,
        members: vec![1, 2, 3],
    };
    assert_eq!(
        ensure_bootstrap_group(&storage, &record).unwrap(),
        BootstrapOutcome::Created
    );
    // Byte-identical retry is accepted.
    assert_eq!(
        ensure_bootstrap_group(&storage, &record).unwrap(),
        BootstrapOutcome::AlreadyPresent
    );
    // A conflicting descriptor is a fork attempt and fails.
    let conflicting = BootstrapGroup {
        cluster_id: CID,
        group: GroupId::Meta,
        members: vec![1, 2, 4],
    };
    assert!(ensure_bootstrap_group(&storage, &conflicting).is_err());

    // Learner admission has the same idempotent/conflict semantics.
    let adm = LearnerAdmission {
        cluster_id: CID,
        group: GroupId::Data(0),
        plan_id: 7,
    };
    assert_eq!(
        ensure_learner_admission(&storage, &adm).unwrap(),
        BootstrapOutcome::Created
    );
    assert_eq!(
        ensure_learner_admission(&storage, &adm).unwrap(),
        BootstrapOutcome::AlreadyPresent
    );
    let adm2 = LearnerAdmission {
        cluster_id: CID,
        group: GroupId::Data(0),
        plan_id: 8,
    };
    assert!(ensure_learner_admission(&storage, &adm2).is_err());
}

// ---------------------------------------------------------------------------
// Full bootstrap + resumability
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn full_bootstrap_serves_meta_and_data() {
    let mut c = Cluster::new();
    full_bootstrap(&mut c).await;

    // The meta group holds the committed data placement.
    let leader = c.meta_leader().unwrap();
    match leader.read_placement(GroupId::Data(PART)).await.unwrap() {
        MetaRead::Value(Some(p)) => assert_eq!(p.voters, DATA_VOTERS.to_vec()),
        other => panic!("meta placement read returned {other:?}"),
    }

    // The data group serves a write and reads it back.
    assert!(data_write(&c, put(1, b"k", b"v1")).await);
    assert_eq!(data_read(&c, b"k").await, Some(b"v1".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn bootstrap_is_resumable() {
    let mut c = Cluster::new();
    full_bootstrap(&mut c).await;

    // Re-running every phase is a no-op: records already present, commands
    // replay as NoOp, groups already initialized.
    let desc = descriptor();
    record_meta_bootstrap(&c.storage_list(&META_VOTERS), &desc).unwrap();
    record_data_bootstrap(&c.storage_list(&DATA_VOTERS), &desc).unwrap();
    seed_cluster(&c.meta_slice(), &desc).await.unwrap();

    // State is unchanged and still serving.
    let leader = c.meta_leader().unwrap();
    assert!(matches!(
        leader.read_placement(GroupId::Data(PART)).await.unwrap(),
        MetaRead::Value(Some(_))
    ));
}

// ---------------------------------------------------------------------------
// Full-cluster restart recovery
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn full_cluster_restart_recovers_meta_and_data() {
    let mut c = Cluster::new();
    full_bootstrap(&mut c).await;
    assert!(data_write(&c, put(1, b"k", b"survive")).await);
    eventually("data replicates before restart", || {
        c.data_nodes
            .values()
            .all(|n| matches!(n.local_get(b"k").unwrap(), Some((_, ref v)) if v == b"survive"))
    })
    .await;

    // Full shutdown, then reopen every store and restart — no re-init.
    c.stop_all().await;
    for &id in &META_VOTERS {
        c.start_meta(id).await;
    }
    for &id in &DATA_VOTERS {
        c.start_data(id).await;
    }

    let meta = c.meta_slice();
    eventually("meta recovers a leader", || {
        meta.iter().any(|n| n.current_leader().is_some())
    })
    .await;
    let data: Vec<_> = c.data_nodes.values().cloned().collect();
    eventually("data recovers a leader", || {
        data.iter().any(|n| n.current_leader().is_some())
    })
    .await;

    // Both groups recovered their committed state.
    let leader = c.meta_leader().unwrap();
    assert!(matches!(
        leader.read_placement(GroupId::Data(PART)).await.unwrap(),
        MetaRead::Value(Some(ref p)) if p.voters == DATA_VOTERS.to_vec()
    ));
    assert_eq!(data_read(&c, b"k").await, Some(b"survive".to_vec()));
}

// ---------------------------------------------------------------------------
// Meta-quorum loss while the data group keeps serving
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn meta_quorum_loss_data_still_serves() {
    let mut c = Cluster::new();
    full_bootstrap(&mut c).await;
    assert!(data_write(&c, put(1, b"k", b"v1")).await);

    // Kill two of three meta voters: the control plane loses quorum. The data
    // partition (voters 3,4,5) is untouched.
    c.kill_meta(1).await;
    c.kill_meta(2).await;

    // The lone surviving meta node cannot serve a linearizable read.
    let survivor = c.meta_nodes[&3].clone();
    let mut meta_unavailable = false;
    for _ in 0..40 {
        match survivor.read_placement(GroupId::Data(PART)).await {
            Ok(MetaRead::Value(_)) => {}
            _ => {
                meta_unavailable = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(meta_unavailable, "meta must not serve without quorum");

    // The data group still serves reads and writes via its own Raft.
    assert_eq!(data_read(&c, b"k").await, Some(b"v1".to_vec()));
    assert!(data_write(&c, put(2, b"k2", b"v2")).await);
    assert_eq!(data_read(&c, b"k2").await, Some(b"v2".to_vec()));
}

// ---------------------------------------------------------------------------
// Learner-first meta membership growth surviving restart
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn meta_membership_grows_and_survives_restart() {
    let mut c = Cluster::new();
    full_bootstrap(&mut c).await;

    // Admit node 4 as a meta voter: durable admission record first (ground rule
    // 8), then start it, then learner-first promote to a 4-voter set.
    let adm = LearnerAdmission {
        cluster_id: CID,
        group: GroupId::Meta,
        plan_id: 1,
    };
    ensure_learner_admission(&c.storage(4), &adm).unwrap();
    c.start_meta(4).await;

    let new_voters = [1, 2, 3, 4];
    let leader = c.meta_leader().unwrap();
    admit_and_promote(&leader, 4, &new_voters).await.unwrap();
    // Release this handle so its storage Arc does not hold the RocksDB lock
    // across the restart below.
    drop(leader);

    eventually("node 4 is a committed voter", || {
        c.meta_nodes
            .values()
            .any(|n| n.node_id() == 4 && n.voters().contains(&4))
    })
    .await;

    // Restart the whole meta group; the 4-voter membership persists.
    c.stop_all().await;
    for id in new_voters {
        c.start_meta(id).await;
    }
    let meta = c.meta_slice();
    eventually("meta recovers a leader after growth", || {
        meta.iter().any(|n| n.current_leader().is_some())
    })
    .await;

    let leader = c.meta_leader().unwrap();
    let mut voters = leader.voters();
    voters.sort_unstable();
    assert_eq!(voters, new_voters.to_vec());
}
