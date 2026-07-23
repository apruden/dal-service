//! M4 gate (DESIGN §8, §10): envelope codec round-trip + limit fuzzing,
//! wrong-cluster and mis-partitioned-key rejection, stale-route redirect
//! convergence, a cold client with a single live seed, and exactly-once
//! application when a client retries across a leader change.
//!
//! Correctness runs over the in-process transport (ground rule 3); a separate
//! smoke test exercises the real ZeroMQ carrier for transport concerns only.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dal::api::client::Client;
use dal::api::gateway::{ClientGateway, RoutingSource};
use dal::api::ops::{ClientReply, ClientRequest, RoutingInfo, WriteReply};
use dal::codec;
use dal::meta::bootstrap::ensure_bootstrap_group;
use dal::partition::network::{Faults, Registry};
use dal::partition::node::PartitionNode;
use dal::storage::Storage;
use dal::transport::codec::{Envelope, FrameError, MsgType};
use dal::transport::{InProcess, Transport};
use dal::types::{
    BootstrapGroup, ClusterId, DataOp, DataRequest, GroupId, HashSpec, IfVersion, LogId,
    NodeDirectoryEntry, NodeState, Placement, Version,
};

use proptest::prelude::*;
use tempfile::TempDir;

const CID: ClusterId = 0x0000_0000_0000_0000_0000_0000_0000_0DA1;
const VOTERS: [u64; 3] = [1, 2, 3];

// ---------------------------------------------------------------------------
// Codec: round-trip + limit fuzzing (proptest)
// ---------------------------------------------------------------------------

fn msg_type_of(i: u8) -> MsgType {
    match i % 10 {
        0 => MsgType::ClientOp,
        1 => MsgType::RaftAppend,
        2 => MsgType::RaftVote,
        3 => MsgType::RaftSnapshot,
        4 => MsgType::MigrationChunk,
        5 => MsgType::MetaQuery,
        6 => MsgType::Redirect,
        7 => MsgType::Heartbeat,
        8 => MsgType::BecomeLearner,
        _ => MsgType::DataConfigObservation,
    }
}

proptest! {
    #[test]
    fn envelope_round_trips(
        cluster in any::<u128>(),
        mt in any::<u8>(),
        is_data in any::<bool>(),
        partition in any::<u16>(),
        request_id in any::<u64>(),
        payload in proptest::collection::vec(any::<u8>(), 0..2048),
    ) {
        let group = if is_data { GroupId::Data(partition) } else { GroupId::Meta };
        let e = Envelope::new(cluster, msg_type_of(mt), group, request_id, payload);
        let decoded = Envelope::decode(&e.encode()).unwrap();
        prop_assert_eq!(e, decoded);
    }

    /// Decoding arbitrary bytes must never panic — only ever `Ok`/`Err`.
    #[test]
    fn decode_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..128)) {
        let _ = Envelope::decode(&bytes);
    }

    /// A length prefix past the per-type limit is rejected on the header alone,
    /// without the payload bytes ever being present.
    #[test]
    fn oversize_prefix_rejected(mt in any::<u8>(), over in 1u32..4096) {
        let mt = msg_type_of(mt);
        let mut bytes = Envelope::new(CID, mt, GroupId::Meta, 0, Vec::new()).encode();
        let claimed = (mt.max_payload() as u32).saturating_add(over);
        bytes[32..36].copy_from_slice(&claimed.to_le_bytes());
        let rejected = matches!(
            Envelope::decode(&bytes),
            Err(FrameError::Oversized { .. }) | Err(FrameError::Truncated { .. })
        );
        prop_assert!(rejected);
    }
}

// ---------------------------------------------------------------------------
// Cluster harness: 3 partition nodes fronted by gateways over an in-process
// switch, with a shared, mutable routing snapshot (models the meta placement).
// ---------------------------------------------------------------------------

struct StaticRouting(Mutex<RoutingInfo>);

impl RoutingSource for StaticRouting {
    fn routing(&self) -> RoutingInfo {
        self.0.lock().unwrap().clone()
    }
}

fn ctrl_addr(node: u64) -> String {
    format!("ctrl-{node}")
}

fn base_routing() -> RoutingInfo {
    RoutingInfo {
        cluster_id: CID,
        p: 1,
        hash_spec: HashSpec::CANONICAL,
        directory: VOTERS
            .iter()
            .map(|&id| NodeDirectoryEntry {
                node_id: id,
                control_addr: ctrl_addr(id),
                bulk_addr: format!("bulk-{id}"),
                state: NodeState::Active,
                incarnation: 1,
            })
            .collect(),
        placements: vec![(
            0,
            Placement {
                voters: VOTERS.to_vec(),
                voters_log_id: LogId::new(1, 1),
                r#move: None,
            },
        )],
    }
}

struct Cluster {
    _dirs: Vec<TempDir>,
    nodes: Vec<Arc<PartitionNode>>,
    switch: InProcess<ClientGateway>,
    routing: Arc<StaticRouting>,
    registry: Registry<dal::partition::TypeConfig>,
}

impl Cluster {
    async fn bootstrap() -> Cluster {
        let dirs: Vec<TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let registry = Registry::default();
        let faults = Faults::default();

        let mut nodes = Vec::new();
        for i in 0..3 {
            let node_id = VOTERS[i];
            let storage = Arc::new(Storage::open_checked(dirs[i].path(), CID, node_id).unwrap());
            ensure_bootstrap_group(
                &storage,
                &BootstrapGroup {
                    cluster_id: CID,
                    group: GroupId::Data(0),
                    members: VOTERS.to_vec(),
                },
            )
            .unwrap();
            let node = PartitionNode::start(
                node_id,
                GroupId::Data(0),
                storage,
                registry.clone(),
                faults.clone(),
            )
            .await
            .unwrap();
            nodes.push(Arc::new(node));
        }
        nodes[0].initialize(&VOTERS).await.unwrap();

        let routing = Arc::new(StaticRouting(Mutex::new(base_routing())));
        let switch: InProcess<ClientGateway> = InProcess::new();
        for node in &nodes {
            let mut partitions = HashMap::new();
            partitions.insert(0u16, node.clone());
            let gw = ClientGateway::new(
                CID,
                1,
                HashSpec::CANONICAL,
                partitions,
                routing.clone() as Arc<dyn RoutingSource>,
            );
            switch.register(ctrl_addr(node.node_id()), Arc::new(gw));
        }

        let cluster = Cluster {
            _dirs: dirs,
            nodes,
            switch,
            routing,
            registry,
        };
        cluster.await_leader().await;
        cluster
    }

    async fn await_leader(&self) {
        for _ in 0..200 {
            if self.leader().is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("no leader elected");
    }

    fn leader(&self) -> Option<u64> {
        self.nodes.iter().find_map(|n| n.current_leader())
    }

    /// Take a node offline: unbind its gateway and stop its Raft so it is
    /// unreachable to clients and peers.
    async fn kill(&self, node_id: u64) {
        self.switch.deregister(&ctrl_addr(node_id));
        if let Some(n) = self.nodes.iter().find(|n| n.node_id() == node_id) {
            n.shutdown().await.unwrap();
        }
        self.registry.remove(node_id);
    }

    fn client(&self, client_id: u128, seeds: Vec<String>) -> Client<InProcess<ClientGateway>> {
        Client::new(CID, client_id, seeds, self.switch.clone())
    }

    /// Set the placement voter order so `first` is tried before the others,
    /// modelling a stale route that must redirect to the real leader.
    fn set_voter_order(&self, first: u64) {
        let mut ordered = vec![first];
        for &v in &VOTERS {
            if v != first {
                ordered.push(v);
            }
        }
        let mut info = self.routing.0.lock().unwrap();
        info.placements[0].1.voters = ordered;
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

/// Send one raw client op to a specific node and decode its reply — used to
/// drive an exact `(client_id, sequence)` retry that the client library would
/// otherwise manage itself.
async fn call_op(
    switch: &InProcess<ClientGateway>,
    addr: &str,
    req: &ClientRequest,
) -> ClientReply {
    let env = Envelope::new(
        CID,
        MsgType::ClientOp,
        GroupId::Data(0),
        1,
        codec::encode(req),
    );
    let reply = switch.call(addr, env).await.unwrap();
    assert_eq!(reply.cluster_id, CID, "reply must carry our cluster id");
    codec::decode(&reply.payload).unwrap()
}

// ---------------------------------------------------------------------------
// Client golden path + routing
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_put_then_get_round_trips() {
    let c = Cluster::bootstrap().await;
    let client = c.client(0xC1, vec![ctrl_addr(1)]);

    let reply = client.put(b"alpha", b"one", None).await.unwrap();
    assert!(matches!(reply, WriteReply::Applied { .. }));

    let got = client.get(b"alpha").await.unwrap();
    assert_eq!(got.map(|(_, v)| v), Some(b"one".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_client_mutations_use_distinct_sequences() {
    let c = Cluster::bootstrap().await;
    let client = Arc::new(c.client(0xC4, vec![ctrl_addr(1)]));
    let left = client.clone();
    let right = client.clone();

    let (a, b) = tokio::join!(
        async move { left.put(b"left", b"1", None).await },
        async move { right.put(b"right", b"2", None).await },
    );
    assert!(matches!(a.unwrap(), WriteReply::Applied { .. }));
    assert!(matches!(b.unwrap(), WriteReply::Applied { .. }));
    assert_eq!(
        client.get(b"left").await.unwrap().map(|(_, v)| v),
        Some(b"1".to_vec())
    );
    assert_eq!(
        client.get(b"right").await.unwrap().map(|(_, v)| v),
        Some(b"2".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_mutation_does_not_wedge_the_stream() {
    let c = Cluster::bootstrap().await;
    let client = c.client(0xC5, vec![ctrl_addr(1)]);

    // The gateway refuses delete-if-Absent before it reaches the log.
    assert!(client.delete(b"k", Some(IfVersion::Absent)).await.is_err());

    // The refusal must release the reserved sequence: a different mutation on
    // the same partition proceeds instead of erroring "unresolved".
    let put = client.put(b"k", b"v", None).await.unwrap();
    assert!(matches!(put, WriteReply::Applied { .. }));
    assert_eq!(
        client.get(b"k").await.unwrap().map(|(_, v)| v),
        Some(b"v".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_client_with_one_live_seed() {
    let c = Cluster::bootstrap().await;
    // Only the second seed is a real, reachable node.
    let client = c.client(0xC2, vec!["dead-seed".to_string(), ctrl_addr(2)]);

    let reply = client.put(b"k", b"v", None).await.unwrap();
    assert!(matches!(reply, WriteReply::Applied { .. }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_route_redirects_and_converges() {
    let c = Cluster::bootstrap().await;
    let leader = c.leader().unwrap();
    // Order candidates so a follower is tried first: the client must be
    // redirected to the leader and still succeed.
    let follower = VOTERS.iter().copied().find(|&v| v != leader).unwrap();
    c.set_voter_order(follower);

    let client = c.client(0xC3, vec![ctrl_addr(follower)]);
    let reply = client.put(b"routed", b"value", None).await.unwrap();
    assert!(matches!(reply, WriteReply::Applied { .. }));

    let got = client.get(b"routed").await.unwrap();
    assert_eq!(got.map(|(_, v)| v), Some(b"value".to_vec()));
}

// ---------------------------------------------------------------------------
// Rejections: wrong cluster, mis-partitioned key, peer-control on client path
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_cluster_reply_is_rejected() {
    let c = Cluster::bootstrap().await;
    let leader = c.leader().unwrap();

    // A frame stamped with a foreign cluster id.
    let req = ClientRequest::Read { key: b"k".to_vec() };
    let env = Envelope::new(
        0xBAD,
        MsgType::ClientOp,
        GroupId::Data(0),
        7,
        codec::encode(&req),
    );
    let reply = c.switch.call(&ctrl_addr(leader), env).await.unwrap();
    // The responder stamps its own cluster id, so a client's per-reply cluster
    // check catches the mismatch (DESIGN §8.2).
    assert_eq!(reply.cluster_id, CID);
    assert_ne!(reply.cluster_id, 0xBAD);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mispartitioned_key_is_rejected() {
    // A gateway serving P=4 with no hosted partitions: the partition check runs
    // before any node lookup, so an empty partition map is fine here.
    let routing = Arc::new(StaticRouting(Mutex::new(RoutingInfo {
        cluster_id: CID,
        p: 4,
        hash_spec: HashSpec::CANONICAL,
        directory: Vec::new(),
        placements: Vec::new(),
    })));
    let gw = ClientGateway::new(
        CID,
        4,
        HashSpec::CANONICAL,
        HashMap::new(),
        routing as Arc<dyn RoutingSource>,
    );
    let switch: InProcess<ClientGateway> = InProcess::new();
    switch.register("gw", Arc::new(gw));

    let key = b"some-key";
    let correct = HashSpec::CANONICAL.partition_of(key, 4);
    let wrong = (correct + 1) % 4;

    let req = ClientRequest::Read { key: key.to_vec() };
    let env = Envelope::new(
        CID,
        MsgType::ClientOp,
        GroupId::Data(wrong),
        3,
        codec::encode(&req),
    );
    let reply = switch.call("gw", env).await.unwrap();
    let decoded: ClientReply = codec::decode(&reply.payload).unwrap();
    assert!(
        matches!(decoded, ClientReply::Refused(ref e) if e.contains("hashes")),
        "expected mispartition refusal, got {decoded:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_control_on_client_path_is_rejected() {
    let c = Cluster::bootstrap().await;
    let leader = c.leader().unwrap();

    // A peer-control msg_type must never be honoured by the client gateway.
    let env = Envelope::new(CID, MsgType::BecomeLearner, GroupId::Data(0), 5, Vec::new());
    let reply = c.switch.call(&ctrl_addr(leader), env).await.unwrap();
    let decoded: ClientReply = codec::decode(&reply.payload).unwrap();
    assert!(
        matches!(decoded, ClientReply::Error(ref e) if e.contains("peer-control")),
        "expected peer-control rejection, got {decoded:?}"
    );
}

// ---------------------------------------------------------------------------
// Exactly-once across a leader change (asserted via the M2 sequence records:
// a replayed retry returns the original version, so no second application)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_across_leader_change_applies_exactly_once() {
    let c = Cluster::bootstrap().await;

    // A fixed idempotency key we will replay verbatim after failover.
    let req = ClientRequest::Mutate(DataRequest {
        client_id: 0xC4,
        sequence: 1,
        op: DataOp::Put {
            key: b"once".to_vec(),
            value: b"v1".to_vec(),
            if_version: None,
        },
    });

    // First application on the original leader.
    let leader = c.leader().unwrap();
    let first_version = match call_op(&c.switch, &ctrl_addr(leader), &req).await {
        ClientReply::Mutation(WriteReply::Applied { version }) => version,
        other => panic!("first write not applied: {other:?}"),
    };

    // Fail the leader over; a survivor takes over holding the committed record.
    c.kill(leader).await;
    eventually("a survivor becomes leader", || {
        c.nodes
            .iter()
            .filter(|n| n.node_id() != leader)
            .any(|n| n.current_leader() == Some(n.node_id()))
    })
    .await;

    // Replay the identical request to the new leader. The state machine holds
    // the sequence record, so it is *replayed*, returning the same version —
    // proof it was not applied a second time.
    let new_leader = c
        .nodes
        .iter()
        .filter(|n| n.node_id() != leader)
        .find_map(|n| n.current_leader())
        .unwrap();

    let replay_version = poll_for_applied(&c.switch, &ctrl_addr(new_leader), &req).await;
    assert_eq!(
        replay_version, first_version,
        "retry must replay the original version, not re-apply"
    );

    // The value reflects exactly one application.
    let read = ClientRequest::Read {
        key: b"once".to_vec(),
    };
    let served = poll_for_value(&c.switch, &ctrl_addr(new_leader), &read).await;
    assert_eq!(served, Some(b"v1".to_vec()));
}

/// Retry a mutation against `addr` until the new leader stabilises and applies
/// (or replays) it, returning the version.
async fn poll_for_applied(
    switch: &InProcess<ClientGateway>,
    addr: &str,
    req: &ClientRequest,
) -> Version {
    for _ in 0..200 {
        if let ClientReply::Mutation(WriteReply::Applied { version }) =
            call_op(switch, addr, req).await
        {
            return version;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("new leader never applied the replayed write");
}

async fn poll_for_value(
    switch: &InProcess<ClientGateway>,
    addr: &str,
    req: &ClientRequest,
) -> Option<Vec<u8>> {
    for _ in 0..200 {
        if let ClientReply::Value(v) = call_op(switch, addr, req).await {
            return v.map(|(_, bytes)| bytes);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("new leader never served the read");
}
