//! M5: the *running* meta Raft group (DESIGN §3.1, §5). Exercises the same
//! generic log store / channel network as the data groups (M3), but carrying
//! `MetaCommand`: bootstrap + init, command replication and convergence,
//! linearizable reads through the serving gate, leader failover preserving
//! committed placement, and follower restart from log.

use std::sync::Arc;
use std::time::Duration;

use dal::meta::node::{MetaNode, MetaRead, ProposeOutcome};
use dal::meta::raft_types::MetaTypeConfig;
use dal::meta::state_machine::MetaApplyResult;
use dal::partition::network::{Faults, Registry};
use dal::storage::Storage;
use dal::types::{ClusterConfig, GroupId, HashSpec, MetaCommand, NodeId, PROTOCOL_VERSION};

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
    assert!(matches!(
        propose_to_leader(
            nodes,
            MetaCommand::ClusterInit {
                config: config(),
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
        survivors
            .iter()
            .any(|&i| nodes[i].current_leader().map(|l| l != nodes[old].node_id()).unwrap_or(false))
    })
    .await;

    // The new leader still serves the committed placement via ReadIndex.
    let mut served = false;
    for _ in 0..200 {
        let li = survivors
            .iter()
            .copied()
            .find(|&i| nodes[i].current_leader() == Some(nodes[i].node_id()));
        if let Some(li) = li {
            if let MetaRead::Value(Some(p)) = nodes[li].read_placement(GroupId::Data(0)).await.unwrap()
            {
                assert_eq!(p.voters, vec![1, 2, 3]);
                served = true;
                break;
            }
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
