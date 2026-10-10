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
struct PeerAddresses {
    addresses: Vec<(String, String)>,
    reservations: Vec<Option<TcpListener>>,
}

impl PeerAddresses {
    fn control(&self, id: u64) -> String {
        self.addresses[(id - 1) as usize].0.clone()
    }

    fn bulk(&self, id: u64) -> String {
        self.addresses[(id - 1) as usize].1.clone()
    }

    fn release(&mut self, id: u64) {
        let index = (id - 1) as usize;
        drop(self.reservations[index * 2].take());
        drop(self.reservations[index * 2 + 1].take());
    }
}

fn peer_addresses() -> PeerAddresses {
    // A DEALER connected to an inproc endpoint does not reconnect after its
    // ROUTER is rebound. TCP models the process restart exercised here.
    let listeners = (0..ALL.len() * 2)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = listeners
        .chunks_exact(2)
        .map(|pair| {
            (
                format!("tcp://{}", pair[0].local_addr().unwrap()),
                format!("tcp://{}", pair[1].local_addr().unwrap()),
            )
        })
        .collect();
    PeerAddresses {
        addresses,
        reservations: listeners.into_iter().map(Some).collect(),
    }
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

fn descriptor(peers: &PeerAddresses) -> BootstrapDescriptor {
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
                control_addr: peers.control(id),
                bulk_addr: peers.bulk(id),
            })
            .collect(),
        data_placements: vec![(0, vec![4, 5, 6])],
    }
}

fn config(id: u64, dir: PathBuf, http: &str, peers: &PeerAddresses) -> NodeConfig {
    NodeConfig {
        cluster_id: CID,
        node_id: id,
        control_addr: peers.control(id),
        bulk_addr: peers.bulk(id),
        http_addr: Some(http.into()),
        seeds: ALL
            .iter()
            .filter(|&&peer| peer != id)
            .map(|&peer| peers.control(peer))
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
    peers: &PeerAddresses,
    leader: u64,
    kind: MsgType,
    body: Vec<u8>,
) -> SubmitReply {
    let reply = transport
        .call(
            &peers.control(leader),
            Envelope::new(CID, kind, GroupId::Meta, 1, body),
        )
        .await
        .unwrap();
    dal::codec::decode(&reply.payload).unwrap()
}

async fn assert_fenced(transport: &ZmqTransport, peers: &PeerAddresses, id: u64) {
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
            &peers.control(id),
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
    let mut peers = peer_addresses();
    let desc = descriptor(&peers);
    let dirs = (0..ALL.len())
        .map(|_| tempfile::tempdir().unwrap())
        .collect::<Vec<_>>();
    let (http, mut reservations) = http_addresses();
    let mut nodes = Vec::new();
    for (index, &id) in ALL.iter().enumerate() {
        peers.release(id);
        drop(reservations[index].take());
        nodes.push(Some(Arc::new(
            Node::start_insecure_for_tests(
                ctx.clone(),
                config(id, dirs[index].path().to_path_buf(), &http[index], &peers),
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
            &peers,
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
    let frozen_directory = status(&http[0]).await.unwrap()["directory"].clone();

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
        // Each planned stop exceeds the configured Down timeout. A spare
        // Active node exists, but the maintenance fence must suppress failure
        // transitions and replacement moves throughout the stopped interval.
        tokio::time::sleep(Timeouts::default().down + Duration::from_secs(2)).await;
        if id == data_leader {
            let other_voters = [4u64, 5, 6]
                .into_iter()
                .filter(|&voter| voter != data_leader)
                .collect::<Vec<_>>();
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if other_voters.iter().any(|&voter| {
                    nodes[(voter - 1) as usize]
                        .as_ref()
                        .unwrap()
                        .partition_leader(0)
                        == Some(voter)
                }) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "surviving data quorum lost leadership: {:?}",
                    other_voters
                        .iter()
                        .map(|&voter| nodes[(voter - 1) as usize]
                            .as_ref()
                            .unwrap()
                            .partition_leader(0))
                        .collect::<Vec<_>>()
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        for meta in nodes.iter().take(3) {
            let meta = meta.as_ref().unwrap();
            assert_eq!(
                meta.local_node_state(id).unwrap(),
                Some(dal::types::NodeState::Active)
            );
            assert_eq!(
                meta.local_placement_voters(0).unwrap(),
                Some((vec![4, 5, 6], false))
            );
            assert_eq!(meta.meta_voters_of(), Some(vec![1, 2, 3]));
        }
        assert_eq!(
            status(&http[0]).await.unwrap()["directory"],
            frozen_directory
        );
        let node = Arc::new(
            Node::start_insecure_for_tests(
                ctx.clone(),
                config(id, dirs[index].path().to_path_buf(), &http[index], &peers),
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
        assert_fenced(&transport, &peers, id).await;
        assert_eq!(
            status(&http[0]).await.unwrap()["directory"],
            frozen_directory
        );
    }

    let meta_leader = nodes[0].as_ref().unwrap().meta_leader().unwrap();
    assert_eq!(
        submit(
            &transport,
            &peers,
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
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let meta = status(&http[0]).await.unwrap();
        if meta["maintenance"]["stage"] == "verifying"
            && meta["maintenance"]["recovered"]
                .as_array()
                .is_some_and(|nodes| nodes.len() == ALL.len())
        {
            break;
        }
        if Instant::now() >= deadline {
            let mut voter_statuses = Vec::new();
            for id in [4u64, 5, 6] {
                voter_statuses.push((
                    id,
                    status(&http[(id - 1) as usize])
                        .await
                        .map(|status| status["partitions"].clone()),
                ));
            }
            panic!(
                "recovery proofs stalled: recovered={:?}, data voter statuses={voter_statuses:?}, router status={:?}",
                meta["maintenance"]["recovered"],
                status(&http[6])
                    .await
                    .map(|status| status["partitions"].clone()),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for address in &http {
        assert!(!ready(address).await);
    }
    for id in [data_leader, 7] {
        assert_fenced(&transport, &peers, id).await;
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
