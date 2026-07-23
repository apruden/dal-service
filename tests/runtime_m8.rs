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
