//! M8: end-to-end smoke test for the assembled `runtime::Node`.
//!
//! Forms a real three-node cluster over ZeroMQ `inproc://` (shared context, no
//! TCP ports), drives genesis, and proves a client `put`/`get` round-trips
//! through the gateway to the data-group leader. Correctness fuzzing lives in
//! the M7 harness; here we only prove the production assembly boots and serves.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use dal::api::client::Client;
use dal::config::{NodeConfig, Timeouts};
use dal::meta::bootstrap::{BootstrapDescriptor, DirEntry};
use dal::runtime::node::Node;
use dal::transport::Transport;
use dal::transport::dealer::ZmqTransport;
use dal::transport::router::settle;
use dal::types::{ClusterConfig, ClusterId, HashSpec, NodeId, PROTOCOL_VERSION};

const CID: ClusterId = 0x0000_0000_0000_0000_0000_0000_0000_0DA1;
const VOTERS: [NodeId; 3] = [1, 2, 3];

fn descriptor() -> BootstrapDescriptor {
    let directory = VOTERS
        .iter()
        .map(|&id| DirEntry {
            node_id: id,
            control_addr: format!("inproc://m8-ctrl-{id}"),
            bulk_addr: format!("inproc://m8-bulk-{id}"),
        })
        .collect();
    BootstrapDescriptor {
        cluster_id: CID,
        config: ClusterConfig {
            cluster_id: CID,
            protocol_version: PROTOCOL_VERSION,
            p: 1,
            r: 3,
            hash_spec: HashSpec::CANONICAL,
        },
        meta_voters: VOTERS.to_vec(),
        directory,
        // Single partition replicated across all three nodes.
        data_placements: vec![(0, VOTERS.to_vec())],
    }
}

fn node_config(id: NodeId, dir: PathBuf) -> NodeConfig {
    NodeConfig {
        cluster_id: CID,
        node_id: id,
        control_addr: format!("inproc://m8-ctrl-{id}"),
        bulk_addr: format!("inproc://m8-bulk-{id}"),
        http_addr: None,
        seeds: VOTERS
            .iter()
            .filter(|&&v| v != id)
            .map(|&v| format!("inproc://m8-ctrl-{v}"))
            .collect(),
        data_dir: dir,
        timeouts: Timeouts::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn become_learner_starts_a_hosted_partition() {
    use dal::transport::codec::{Envelope, MsgType};
    use dal::transport::raft_wire::{BecomeLearnerBody, LearnerReply};
    use dal::types::GroupId;

    let ctx = zmq::Context::new();

    // Four-node directory; node 4 is data-only and hosts nothing at genesis.
    let directory = (1..=4)
        .map(|id| DirEntry {
            node_id: id,
            control_addr: format!("inproc://m8bl-ctrl-{id}"),
            bulk_addr: format!("inproc://m8bl-bulk-{id}"),
        })
        .collect();
    let desc = BootstrapDescriptor {
        cluster_id: CID,
        config: ClusterConfig {
            cluster_id: CID,
            protocol_version: PROTOCOL_VERSION,
            p: 1,
            r: 3,
            hash_spec: HashSpec::CANONICAL,
        },
        meta_voters: vec![1, 2, 3],
        directory,
        data_placements: vec![(0, vec![1, 2, 3])],
    };

    let dir = tempfile::tempdir().unwrap();
    let cfg = NodeConfig {
        cluster_id: CID,
        node_id: 4,
        control_addr: "inproc://m8bl-ctrl-4".into(),
        bulk_addr: "inproc://m8bl-bulk-4".into(),
        http_addr: None,
        seeds: Vec::new(),
        data_dir: dir.path().to_path_buf(),
        timeouts: Timeouts::default(),
    };

    // Node 4 alone is enough to exercise the handler: admission is local.
    let node = Arc::new(Node::start(ctx.clone(), cfg, desc).await.unwrap());
    node.bootstrap().await.unwrap();
    settle();
    assert!(!node.hosts_partition(0), "node 4 should host nothing yet");

    let transport = ZmqTransport::new(ctx.clone(), Duration::from_secs(2));
    let admit = || async {
        let env = Envelope::new(
            CID,
            MsgType::BecomeLearner,
            GroupId::Data(0),
            0,
            dal::codec::encode(&BecomeLearnerBody { plan_id: 1 }),
        );
        let reply = transport.call("inproc://m8bl-ctrl-4", env).await.unwrap();
        dal::codec::decode::<LearnerReply>(&reply.payload).unwrap()
    };

    assert_eq!(admit().await, LearnerReply::Admitted);
    assert!(node.hosts_partition(0), "node 4 should now host partition 0");
    // Idempotent: a repeat admission still succeeds and does not restart.
    assert_eq!(admit().await, LearnerReply::Admitted);
    assert!(node.hosts_partition(0));

    Arc::try_unwrap(node).ok().unwrap().shutdown().await.unwrap();
}

fn short_timeouts() -> Timeouts {
    Timeouts {
        suspect: Duration::from_millis(400),
        down: Duration::from_millis(1200),
        request: Duration::from_millis(1000),
    }
}

async fn start_and_bootstrap(
    ctx: &zmq::Context,
    desc: &BootstrapDescriptor,
    dirs: &[tempfile::TempDir],
    timeouts: Timeouts,
) -> Vec<Arc<Node>> {
    let mut nodes = Vec::new();
    for (i, &id) in VOTERS.iter().enumerate() {
        let mut cfg = node_config(id, dirs[i].path().to_path_buf());
        cfg.timeouts = timeouts.clone();
        nodes.push(Arc::new(
            Node::start(ctx.clone(), cfg, desc.clone()).await.unwrap(),
        ));
    }
    settle();
    let handles: Vec<_> = nodes
        .iter()
        .map(|n| {
            let n = n.clone();
            tokio::spawn(async move { n.bootstrap().await })
        })
        .collect();
    for h in handles {
        h.await.unwrap().unwrap();
    }
    nodes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failure_detector_marks_a_silent_node_down() {
    use dal::types::NodeState;

    let ctx = zmq::Context::new();
    let desc = descriptor();
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = start_and_bootstrap(&ctx, &desc, &dirs, short_timeouts()).await;

    // Crash node 3: its emitter stops, so the meta leader sees it fall silent.
    // Two voters remain, preserving meta quorum to commit the transition.
    let victim = nodes.remove(2);
    Arc::try_unwrap(victim).ok().unwrap().shutdown().await.unwrap();

    // A surviving voter's committed directory should progress node 3 to Down.
    let mut reached_down = false;
    for _ in 0..80 {
        if nodes[0].local_node_state(3).unwrap() == Some(NodeState::Down) {
            reached_down = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reached_down, "node 3 was never marked Down");

    for n in nodes {
        Arc::try_unwrap(n).ok().unwrap().shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_serves_a_client_op() {
    let ctx = zmq::Context::new();
    let desc = descriptor();

    // Temp data dirs kept alive for the duration of the test.
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();

    let mut nodes = Vec::new();
    for (i, &id) in VOTERS.iter().enumerate() {
        let cfg = node_config(id, dirs[i].path().to_path_buf());
        nodes.push(Arc::new(
            Node::start(ctx.clone(), cfg, desc.clone()).await.unwrap(),
        ));
    }
    settle();

    // Drive genesis concurrently; only the designated node does meta/data init.
    let handles: Vec<_> = nodes
        .iter()
        .map(|n| {
            let n = n.clone();
            tokio::spawn(async move { n.bootstrap().await })
        })
        .collect();
    for h in handles {
        h.await.unwrap().unwrap();
    }

    // Client over the real ZMQ transport, seeded with every node's control addr.
    let transport = ZmqTransport::new(ctx.clone(), Duration::from_secs(2));
    let client = Client::new(
        CID,
        1,
        VOTERS
            .iter()
            .map(|&v| format!("inproc://m8-ctrl-{v}"))
            .collect(),
        transport,
    );

    // Retry until the data group has elected a leader and serves the write.
    let key = b"greeting";
    let mut wrote = false;
    for _ in 0..40 {
        match client.put(key, b"hello", None).await {
            Ok(dal::api::ops::WriteReply::Applied { .. }) => {
                wrote = true;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    assert!(wrote, "client put never applied");

    let got = client.get(key).await.unwrap();
    assert_eq!(got.map(|(_, v)| v), Some(b"hello".to_vec()));

    for n in nodes {
        Arc::try_unwrap(n).ok().unwrap().shutdown().await.unwrap();
    }
}
