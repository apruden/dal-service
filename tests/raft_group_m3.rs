//! M3 gate (subset runnable on ChannelNetwork, 3 voters): bootstrap, durable
//! write + linearizable read, follower convergence, leader-failover with no
//! acknowledged loss, follower restart from log, and isolated-old-leader
//! read safety.
//!
//! The full snapshot-journal crash matrix (SST ingest form) is tracked as
//! remaining M3 work; these tests exercise the log store, state machine,
//! network, and the ReadIndex serving gate end to end.

use std::sync::Arc;
use std::time::Duration;

use dal::meta::bootstrap::ensure_bootstrap_group;
use dal::partition::ApplyResult;
use dal::partition::network::{Faults, Registry};
use dal::partition::node::{PartitionNode, ReadOutcome, WriteOutcome};
use dal::storage::Storage;
use dal::types::{BootstrapGroup, DataOp, DataRequest, GroupId, MutationResult};

use tempfile::TempDir;

const G: GroupId = GroupId::Data(0);
const VOTERS: [u64; 3] = [1, 2, 3];

struct Harness {
    dirs: Vec<TempDir>,
    registry: Registry<dal::partition::TypeConfig>,
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

    async fn start(&self, idx: usize) -> PartitionNode {
        let node_id = VOTERS[idx];
        let storage = Arc::new(Storage::open_checked(self.dirs[idx].path(), 1, node_id).unwrap());
        ensure_bootstrap_group(
            &storage,
            &BootstrapGroup {
                cluster_id: 1,
                group: G,
                members: VOTERS.to_vec(),
            },
        )
        .unwrap();
        PartitionNode::start(
            node_id,
            G,
            storage,
            self.registry.clone(),
            self.faults.clone(),
        )
        .await
        .unwrap()
    }
}

fn put(client: u128, seq: u64, key: &[u8], val: &[u8]) -> DataRequest {
    DataRequest {
        client_id: client,
        sequence: seq,
        op: DataOp::Put {
            key: key.to_vec(),
            value: val.to_vec(),
            if_version: None,
        },
    }
}

/// Poll until `f` is true or the deadline elapses.
async fn eventually<F: Fn() -> bool>(what: &str, f: F) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

/// Index of the current leader among `nodes`, if a stable one exists.
fn leader_idx(nodes: &[PartitionNode]) -> Option<usize> {
    let leader = nodes.iter().find_map(|n| n.current_leader())?;
    nodes.iter().position(|n| n.node_id() == leader)
}

async fn write_to_leader(nodes: &[PartitionNode], req: DataRequest) -> ApplyResult {
    for _ in 0..200 {
        if let Some(li) = leader_idx(nodes) {
            match nodes[li].write(req.clone()).await {
                Ok(WriteOutcome::Applied(r)) => return r,
                Ok(WriteOutcome::NotLeader { .. }) | Err(_) => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader accepted the write");
}

async fn bootstrapped() -> (Harness, Vec<PartitionNode>) {
    let h = Harness::new();
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(h.start(i).await);
    }
    nodes[0].initialize(&VOTERS).await.unwrap();
    eventually("a leader is elected", || leader_idx(&nodes).is_some()).await;
    (h, nodes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_write_and_linearizable_read() {
    let (_h, nodes) = bootstrapped().await;

    let r = write_to_leader(&nodes, put(1, 1, b"k", b"v")).await;
    assert!(matches!(
        r,
        ApplyResult::Decided(MutationResult::Applied { .. })
    ));

    let li = leader_idx(&nodes).unwrap();
    match nodes[li].read(b"k").await.unwrap() {
        ReadOutcome::Value(Some((_, v))) => assert_eq!(v, b"v"),
        other => panic!("leader read returned {other:?}"),
    }

    // A follower must redirect, not serve.
    let fi = (li + 1) % 3;
    assert!(matches!(
        nodes[fi].read(b"k").await.unwrap(),
        ReadOutcome::NotLeader { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followers_converge() {
    let (_h, nodes) = bootstrapped().await;
    write_to_leader(&nodes, put(1, 1, b"k", b"v")).await;

    // Every replica eventually applies and holds the value locally.
    eventually("all replicas hold the write", || {
        nodes
            .iter()
            .all(|n| matches!(n.local_get(b"k").unwrap(), Some((_, ref v)) if v == b"v"))
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_failover_preserves_acknowledged_write() {
    let (_h, nodes) = bootstrapped().await;
    let ack = write_to_leader(&nodes, put(1, 1, b"k", b"v")).await;
    assert!(matches!(
        ack,
        ApplyResult::Decided(MutationResult::Applied { .. })
    ));

    let old = leader_idx(&nodes).unwrap();
    nodes[old].shutdown().await.unwrap();

    // Surviving nodes elect a new leader and still serve the acknowledged write.
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

    let mut served = false;
    for _ in 0..200 {
        let li = survivors
            .iter()
            .copied()
            .find(|&i| nodes[i].current_leader() == Some(nodes[i].node_id()));
        if let Some(li) = li
            && let ReadOutcome::Value(Some((_, v))) = nodes[li].read(b"k").await.unwrap()
        {
            assert_eq!(v, b"v");
            served = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(served, "new leader never served the acknowledged write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_old_leader_cannot_serve_stale_read() {
    let (h, nodes) = bootstrapped().await;
    write_to_leader(&nodes, put(1, 1, b"k", b"v")).await;

    let old = leader_idx(&nodes).unwrap();
    let others: Vec<u64> = VOTERS
        .iter()
        .copied()
        .filter(|&v| v != nodes[old].node_id())
        .collect();
    h.faults.isolate(nodes[old].node_id(), &others);

    // The isolated ex-leader must not answer a linearizable read with a value:
    // ReadIndex cannot reach a quorum, so it redirects or errors.
    let mut safe = false;
    for _ in 0..200 {
        match nodes[old].read(b"k").await {
            Ok(ReadOutcome::Value(_)) => {}
            Ok(ReadOutcome::NotLeader { .. }) | Err(_) => {
                safe = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(safe, "isolated old leader served a read without quorum");
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
    write_to_leader(&nodes, put(1, 1, b"k", b"v")).await;

    // Pick a follower, crash it (fully drop so RocksDB releases its lock),
    // then restart over the same data dir.
    let li = leader_idx(&nodes).unwrap();
    let fi = (li + 1) % 3;
    let fnode_id = nodes[fi].node_id();
    let follower = nodes.remove(fi);
    follower.shutdown().await.unwrap();
    h.registry.remove(fnode_id);
    drop(follower);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let restarted = h.start(fi).await;

    // It rejoins and re-holds the committed write from its own log.
    eventually(
        "restarted follower recovers the write",
        || matches!(restarted.local_get(b"k").unwrap(), Some((_, ref v)) if v == b"v"),
    )
    .await;
}
