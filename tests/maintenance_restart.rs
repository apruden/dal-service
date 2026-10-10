//! Phase 5: maintenance admission on data-only and routing-only runtimes.

use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dal::api::ops::{ClientReply, ClientRequest};
use dal::config::{NodeConfig, Timeouts};
use dal::meta::bootstrap::{BootstrapDescriptor, DirEntry};
use dal::meta::state_machine::MetaApplyResult;
use dal::runtime::node::Node;
use dal::transport::Transport;
use dal::transport::codec::{Envelope, Lane, MsgType};
use dal::transport::dealer::ZmqTransport;
use dal::transport::raft_wire::{MaintenanceBeginBody, MaintenanceReleaseBody, SubmitReply};
use dal::transport::router::settle;
use dal::types::{ClusterConfig, DataOp, DataRequest, GroupId, HashSpec, PROTOCOL_VERSION};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CID: u128 = 0xDA1_5A7E;
const OPERATION: u128 = 0xA51;
const ALL: [u64; 7] = [1, 2, 3, 4, 5, 6, 7];

fn control(id: u64) -> String {
    format!("inproc://maintenance-restart-control-{id}")
}

fn bulk(id: u64) -> String {
    format!("inproc://maintenance-restart-bulk-{id}")
}

fn http_addresses() -> (Vec<String>, Vec<Option<TcpListener>>) {
    let listeners = (0..ALL.len())
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect();
    (addresses, listeners.into_iter().map(Some).collect())
}

fn descriptor() -> BootstrapDescriptor {
    BootstrapDescriptor {
        cluster_id: CID,
        config: ClusterConfig {
            cluster_id: CID,
            protocol_version: PROTOCOL_VERSION,
            p: 1,
            r: 3,
            hash_spec: HashSpec::CANONICAL,
        },
        meta_voters: vec![1, 2, 3],
        directory: ALL
            .iter()
            .map(|&id| DirEntry {
                node_id: id,
                control_addr: control(id),
                bulk_addr: bulk(id),
            })
            .collect(),
        data_placements: vec![(0, vec![4, 5, 6])],
    }
}

fn config(id: u64, dir: PathBuf, http: &str) -> NodeConfig {
    NodeConfig {
        cluster_id: CID,
        node_id: id,
        control_addr: control(id),
        bulk_addr: bulk(id),
        http_addr: Some(http.into()),
        seeds: ALL
            .iter()
            .filter(|&&peer| peer != id)
            .map(|&peer| control(peer))
            .collect(),
        data_dir: dir,
        timeouts: Timeouts::default(),
    }
}

async fn http(address: &str, path: &str) -> Option<Vec<u8>> {
    let mut stream = tokio::net::TcpStream::connect(address).await.ok()?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .ok()?;
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut reply))
        .await
        .ok()?
        .ok()?;
    Some(reply)
}

async fn ready(address: &str) -> bool {
    http(address, "/readyz")
        .await
        .is_some_and(|reply| reply.starts_with(b"HTTP/1.1 200"))
}

async fn status(address: &str) -> Option<serde_json::Value> {
    let reply = http(address, "/status").await?;
    if !reply.starts_with(b"HTTP/1.1 200") {
        return None;
    }
    let body = reply.windows(4).position(|bytes| bytes == b"\r\n\r\n")? + 4;
    serde_json::from_slice(&reply[body..]).ok()
}

async fn wait_for_status<F: Fn(&serde_json::Value) -> bool>(
    address: &str,
    check: F,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut last_status = None;
    loop {
        if let Some(value) = status(address).await {
            if check(&value) {
                return value;
            }
            last_status = Some(value);
        }
        assert!(
            Instant::now() < deadline,
            "status at {address} did not converge: {last_status:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_ready(address: &str) {
    let deadline = Instant::now() + Duration::from_secs(45);
    while !ready(address).await {
        assert!(
            Instant::now() < deadline,
            "node at {address} never became ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn submit(
    transport: &ZmqTransport,
    leader: u64,
    kind: MsgType,
    body: Vec<u8>,
) -> SubmitReply {
    let reply = transport
        .call(
            &control(leader),
            Envelope::new(CID, kind, GroupId::Meta, 1, body),
        )
        .await
        .unwrap();
    dal::codec::decode(&reply.payload).unwrap()
}

async fn assert_fenced(transport: &ZmqTransport, id: u64) {
    let request = ClientRequest::Mutate(DataRequest {
        recovery_epoch: 0,
        client_id: 0x70,
        sequence: 1,
        op: DataOp::Put {
            key: b"fenced".to_vec(),
            value: b"must-not-commit".to_vec(),
            if_version: None,
        },
    });
    let reply = transport
        .call(
            &control(id),
            Envelope::new(
                CID,
                MsgType::ClientOp,
                GroupId::Data(0),
                1,
                dal::codec::encode(&request),
            ),
        )
        .await
        .unwrap();
    let outcome: ClientReply = dal::codec::decode(&reply.payload).unwrap();
    assert!(
        matches!(outcome, ClientReply::Unavailable(ref reason) if reason == "maintenance admission is closed"),
        "node {id} accepted client work while fenced: {outcome:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn data_leader_and_routing_only_restart_stay_fenced_through_verification() {
    let ctx = zmq::Context::new();
    let desc = descriptor();
    let dirs = (0..ALL.len())
        .map(|_| tempfile::tempdir().unwrap())
        .collect::<Vec<_>>();
    let (http, mut reservations) = http_addresses();
    let mut nodes = Vec::new();
    for (index, &id) in ALL.iter().enumerate() {
        drop(reservations[index].take());
        nodes.push(Some(Arc::new(
            Node::start_insecure_for_tests(
                ctx.clone(),
                config(id, dirs[index].path().to_path_buf(), &http[index]),
                desc.clone(),
            )
            .await
            .unwrap(),
        )));
    }
    settle();
    let handles = nodes
        .iter()
        .map(|node| {
            let node = node.as_ref().unwrap().clone();
            tokio::spawn(async move { node.bootstrap().await })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.await.unwrap().unwrap();
    }
    for address in &http {
        wait_ready(address).await;
        assert_eq!(
            status(address).await.unwrap()["maintenance_admission_open"],
            true
        );
    }
    let transport = ZmqTransport::new(ctx.clone(), Duration::from_secs(8), Lane::Control);
    let meta_leader = nodes[0].as_ref().unwrap().meta_leader().unwrap();
    assert_eq!(
        submit(
            &transport,
            meta_leader,
            MsgType::MaintenanceBegin,
            dal::codec::encode(&MaintenanceBeginBody {
                operation_id: OPERATION
            }),
        )
        .await,
        SubmitReply::Outcome(MetaApplyResult::Applied)
    );
    for (index, address) in http.iter().enumerate() {
        wait_for_status(address, |value| {
            value["maintenance_admission_open"] == false
                && (index >= 3 || value["maintenance"]["stage"] == "quiesced")
        })
        .await;
        assert!(!ready(address).await);
    }

    let data_leader = nodes[3].as_ref().unwrap().partition_leader(0).unwrap();
    assert!((4..=6).contains(&data_leader));
    for id in [data_leader, 7] {
        let index = (id - 1) as usize;
        let node = nodes[index].take().unwrap();
        Arc::try_unwrap(node)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
    }
    for id in [data_leader, 7] {
        let index = (id - 1) as usize;
        let node = Arc::new(
            Node::start_insecure_for_tests(
                ctx.clone(),
                config(id, dirs[index].path().to_path_buf(), &http[index]),
                desc.clone(),
            )
            .await
            .unwrap(),
        );
        node.bootstrap().await.unwrap();
        nodes[index] = Some(node);
        wait_for_status(&http[index], |value| {
            value["maintenance_admission_open"] == false
        })
        .await;
        assert!(!ready(&http[index]).await);
        assert_fenced(&transport, id).await;
    }

    let meta_leader = nodes[0].as_ref().unwrap().meta_leader().unwrap();
    assert_eq!(
        submit(
            &transport,
            meta_leader,
            MsgType::MaintenanceVerify,
            dal::codec::encode(&MaintenanceReleaseBody {
                operation_id: OPERATION,
                generation: 1
            }),
        )
        .await,
        SubmitReply::Outcome(MetaApplyResult::Applied)
    );
    wait_for_status(&http[0], |value| {
        value["maintenance"]["stage"] == "verifying"
            && value["maintenance"]["recovered"]
                .as_array()
                .is_some_and(|nodes| nodes.len() == ALL.len())
    })
    .await;
    for address in &http {
        assert!(!ready(address).await);
    }
    for id in [data_leader, 7] {
        assert_fenced(&transport, id).await;
    }
    for node in nodes.into_iter().flatten() {
        Arc::try_unwrap(node)
            .ok()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
    }
}
