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
async fn draining_a_node_migrates_its_partition() {
    use dal::transport::codec::{Envelope, MsgType};
    use dal::transport::raft_wire::LeaveBody;

    let ctx = zmq::Context::new();

    // Three meta voters (an odd set); partition 0 lives on {1,2,3}; node 4 is a
    // data-only Active spare that the drain will move the partition onto.
    let all = [1u64, 2, 3, 4];
    let directory = all
        .iter()
        .map(|&id| DirEntry {
            node_id: id,
            control_addr: format!("inproc://m8dr-ctrl-{id}"),
            bulk_addr: format!("inproc://m8dr-bulk-{id}"),
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

    let dirs: Vec<tempfile::TempDir> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = Vec::new();
    for (i, &id) in all.iter().enumerate() {
        let cfg = NodeConfig {
            cluster_id: CID,
            node_id: id,
            control_addr: format!("inproc://m8dr-ctrl-{id}"),
            bulk_addr: format!("inproc://m8dr-bulk-{id}"),
            http_addr: None,
            seeds: Vec::new(),
            data_dir: dirs[i].path().to_path_buf(),
            timeouts: Timeouts::default(),
        };
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

    // Drain a *non-leader* voter so the surviving meta-voter data leader can
    // finalize the move (a drained leader would step down mid-change). Wait for
    // a stable partition leader first.
    let mut leader = None;
    for _ in 0..100 {
        if let Some(l) = nodes[0].partition_leader(0) {
            leader = Some(l);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let leader = leader.expect("partition 0 never elected a leader");
    let victim = *[1u64, 2, 3].iter().find(|&&v| v != leader).unwrap();

    let transport = ZmqTransport::new(ctx.clone(), Duration::from_secs(2));
    let leave = Envelope::new(
        CID,
        MsgType::LeaveRequest,
        dal::types::GroupId::Meta,
        0,
        dal::codec::encode(&LeaveBody { node_id: victim }),
    );
    transport.call("inproc://m8dr-ctrl-1", leave).await.unwrap();

    // Expected post-move voters: {1,2,3} minus the drained node, plus spare 4.
    let mut expected: Vec<u64> = [1u64, 2, 3].into_iter().filter(|&v| v != victim).collect();
    expected.push(4);
    expected.sort_unstable();

    let mut migrated = false;
    for _ in 0..200 {
        let placement = nodes[0].local_placement_voters(0).unwrap();
        if placement == Some((expected.clone(), false)) && nodes[3].hosts_partition(0) {
            migrated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(migrated, "partition 0 never migrated off the drained node");

    for n in nodes {
        Arc::try_unwrap(n).ok().unwrap().shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abort_driver_rolls_back_a_plan_whose_target_dies() {
    use dal::transport::codec::{Envelope, MsgType};
    use dal::transport::raft_wire::LeaveBody;
    use dal::types::NodeState;

    let ctx = zmq::Context::new();
    let all = [1u64, 2, 3, 4];
    let directory = all
        .iter()
        .map(|&id| DirEntry {
            node_id: id,
            control_addr: format!("inproc://m8ab-ctrl-{id}"),
            bulk_addr: format!("inproc://m8ab-bulk-{id}"),
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
    // Wide-ish suspect window so the drain plan is created while the spare is
    // still Active, before it is later declared Down.
    let timeouts = Timeouts {
        suspect: Duration::from_millis(800),
        down: Duration::from_millis(1600),
        request: Duration::from_millis(500),
    };

    let dirs: Vec<tempfile::TempDir> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = Vec::new();
    for (i, &id) in all.iter().enumerate() {
        let cfg = NodeConfig {
            cluster_id: CID,
            node_id: id,
            control_addr: format!("inproc://m8ab-ctrl-{id}"),
            bulk_addr: format!("inproc://m8ab-bulk-{id}"),
            http_addr: None,
            seeds: Vec::new(),
            data_dir: dirs[i].path().to_path_buf(),
            timeouts: timeouts.clone(),
        };
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
    // Let the spare heartbeat so it is firmly Active before we crash it.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Crash the only spare, then immediately drain node 3. A plan to move
    // partition 0 onto node 4 is created while node 4 still reads Active, but the
    // move can never promote a dead learner; once node 4 is declared Down the
    // driver aborts the plan and rolls back to the original voters.
    let spare = nodes.remove(3);
    Arc::try_unwrap(spare)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    let transport = ZmqTransport::new(ctx.clone(), Duration::from_secs(2));
    let leave = Envelope::new(
        CID,
        MsgType::LeaveRequest,
        dal::types::GroupId::Meta,
        0,
        dal::codec::encode(&LeaveBody { node_id: 3 }),
    );
    transport.call("inproc://m8ab-ctrl-1", leave).await.unwrap();

    // First confirm a plan is actually created (target node still reads Active),
    // so the later rollback is distinguishable from the pre-plan initial state.
    let mut plan_created = false;
    for _ in 0..100 {
        if matches!(nodes[0].local_placement_voters(0).unwrap(), Some((_, true))) {
            plan_created = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(plan_created, "drain never created a plan");

    // Once the dead target is declared Down, the plan aborts and rolls back to
    // the original voters; with no Active spare left, it stays that way.
    let mut rolled_back = false;
    for _ in 0..300 {
        if nodes[0].local_node_state(4).unwrap() == Some(NodeState::Down)
            && nodes[0].local_placement_voters(0).unwrap() == Some((vec![1, 2, 3], false))
        {
            rolled_back = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        rolled_back,
        "aborting plan never rolled back after target went Down"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        nodes[0].local_placement_voters(0).unwrap(),
        Some((vec![1, 2, 3], false))
    );

    for n in nodes {
        Arc::try_unwrap(n).ok().unwrap().shutdown().await.unwrap();
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
    assert!(
        node.hosts_partition(0),
        "node 4 should now host partition 0"
    );
    // Idempotent: a repeat admission still succeeds and does not restart.
    assert_eq!(admit().await, LearnerReply::Admitted);
    assert!(node.hosts_partition(0));

    Arc::try_unwrap(node)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn non_meta_voter_data_leader_drives_a_move() {
    use dal::transport::codec::{Envelope, MsgType};
    use dal::transport::raft_wire::LeaveBody;

    let ctx = zmq::Context::new();

    // Meta lives on {1,2,3}; partition 0 lives entirely on non-meta nodes
    // {4,5,6}, so whichever of them leads it must drive the move over the network.
    let all: Vec<u64> = (1..=6).collect();
    let directory = all
        .iter()
        .map(|&id| DirEntry {
            node_id: id,
            control_addr: format!("inproc://m8nm-ctrl-{id}"),
            bulk_addr: format!("inproc://m8nm-bulk-{id}"),
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
        data_placements: vec![(0, vec![4, 5, 6])],
    };

    let dirs: Vec<tempfile::TempDir> = (0..6).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = Vec::new();
    for (i, &id) in all.iter().enumerate() {
        let cfg = NodeConfig {
            cluster_id: CID,
            node_id: id,
            control_addr: format!("inproc://m8nm-ctrl-{id}"),
            bulk_addr: format!("inproc://m8nm-bulk-{id}"),
            http_addr: None,
            seeds: Vec::new(),
            data_dir: dirs[i].path().to_path_buf(),
            timeouts: Timeouts::default(),
        };
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

    // Drain a non-leader voter of partition 0. The non-meta leader among {4,5,6}
    // reads the plan and reports the finalize over the network; the spare picked
    // is the lowest Active non-voter, node 1.
    let mut leader = None;
    for _ in 0..100 {
        if let Some(l) = nodes[3].partition_leader(0) {
            leader = Some(l);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let leader = leader.expect("partition 0 never elected a leader");
    assert!(
        (4..=6).contains(&leader),
        "partition 0 leader should be a non-meta node, got {leader}"
    );
    let victim = *[4u64, 5, 6].iter().find(|&&v| v != leader).unwrap();

    let transport = ZmqTransport::new(ctx.clone(), Duration::from_secs(2));
    let leave = Envelope::new(
        CID,
        MsgType::LeaveRequest,
        dal::types::GroupId::Meta,
        0,
        dal::codec::encode(&LeaveBody { node_id: victim }),
    );
    transport.call("inproc://m8nm-ctrl-1", leave).await.unwrap();

    let mut expected: Vec<u64> = [4u64, 5, 6].into_iter().filter(|&v| v != victim).collect();
    expected.push(1);
    expected.sort_unstable();

    let mut migrated = false;
    for _ in 0..200 {
        // Query a meta voter for the committed placement.
        if nodes[0].local_placement_voters(0).unwrap() == Some((expected.clone(), false))
            && nodes[0].hosts_partition(0)
        {
            migrated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(migrated, "non-meta data leader never completed the move");

    for n in nodes {
        Arc::try_unwrap(n).ok().unwrap().shutdown().await.unwrap();
    }
}

fn http_get(addr: &str, path: &str) -> String {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).unwrap();
    let mut buf = String::new();
    stream.read_to_string(&mut buf).unwrap();
    buf
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_status_reports_node_local_view() {
    use dal::runtime::http::{ClusterStatus, Role};

    let ctx = zmq::Context::new();
    let desc = descriptor();

    // Reserve an ephemeral port for node 1's HTTP admin plane.
    let http_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let http_addr = format!("127.0.0.1:{http_port}");

    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut nodes = Vec::new();
    for (i, &id) in VOTERS.iter().enumerate() {
        let mut cfg = node_config(id, dirs[i].path().to_path_buf());
        if id == 1 {
            cfg.http_addr = Some(http_addr.clone());
        }
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

    // /health is a bare liveness probe.
    let health = http_get(&http_addr, "/health");
    assert!(health.starts_with("HTTP/1.1 200"), "health: {health}");

    // /status reports node 1's local view: it runs meta and hosts partition 0.
    let raw = http_get(&http_addr, "/status");
    let body = raw.split_once("\r\n\r\n").expect("http body").1;
    let status: ClusterStatus = serde_json::from_str(body).unwrap();
    assert_eq!(status.node_id, 1);
    assert!(status.meta.is_some(), "node 1 runs the meta group");
    let p0 = status
        .partitions
        .iter()
        .find(|p| p.partition == 0)
        .expect("hosts partition 0");
    assert!(matches!(p0.role, Role::Leader | Role::Voter));
    assert_eq!(p0.committed_voters, vec![1, 2, 3]);
    assert!(p0.serving);

    for n in nodes {
        Arc::try_unwrap(n).ok().unwrap().shutdown().await.unwrap();
    }
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
    Arc::try_unwrap(victim)
        .ok()
        .unwrap()
        .shutdown()
        .await
        .unwrap();

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_cli_commands_query_and_mutate_the_cluster() {
    use dal::meta::state_machine::{MetaApplyResult, MetaReject};
    use dal::runtime::admin;
    use dal::types::{GroupId, NodeState};

    let ctx = zmq::Context::new();
    let desc = descriptor();
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let nodes = start_and_bootstrap(&ctx, &desc, &dirs, short_timeouts()).await;

    // `status`: any node answers the routing snapshot from its cached placement,
    // so no leader is required.
    let info = admin::status(ctx.clone(), &desc, None).await.unwrap();
    assert_eq!(info.cluster_id, CID);
    assert_eq!(info.p, 1);
    assert_eq!(info.directory.len(), 3);

    // `abort-plan` for a group with no active plan reaches the meta leader
    // (following `NotLeader`) and is deterministically rejected — exercising the
    // same submit path as `leave` and `join`.
    let aborted = admin::abort_plan(ctx.clone(), &desc, GroupId::Data(0), 999, None)
        .await
        .unwrap();
    assert_eq!(aborted, MetaApplyResult::Rejected(MetaReject::NoPlan));

    // `leave` marks node 3 Draining; the committed directory reflects it.
    let left = admin::leave(ctx.clone(), &desc, 3, None).await.unwrap();
    assert!(matches!(
        left,
        MetaApplyResult::Applied | MetaApplyResult::NoOp
    ));

    let mut draining = false;
    for _ in 0..40 {
        if nodes[0].local_node_state(3).unwrap() == Some(NodeState::Draining) {
            draining = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(draining, "node 3 never reached Draining after leave");

    for n in nodes {
        Arc::try_unwrap(n).ok().unwrap().shutdown().await.unwrap();
    }
}
