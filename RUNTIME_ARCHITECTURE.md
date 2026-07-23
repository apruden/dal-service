# Runtime architecture

How a `dal` node is assembled and started as a process. Describes the design
(DESIGN §10–11) and the current tree, marking what exists versus the seams still
to build. The keystone piece — an assembled `runtime::Node` launched by
`dal run` — does **not** exist yet; today the composition lives only in test
harnesses.

## Layers

### 1. Consensus (openraft) — built
- **One meta Raft group** (`MetaNode`, `MetaTypeConfig`, command = `MetaCommand`):
  cluster config, node directory, placements, move plans. Reads via
  `ensure_linearizable` (ReadIndex); writes via `client_write`.
- **P data Raft groups** (`PartitionNode`, `TypeConfig`, command = `DataRequest`):
  one per partition, holding the KV data. A single physical node hosts the meta
  group (only if it is a meta voter) **plus** whichever data partitions its
  placement assigns it.

### 2. Network seam (`RaftNetworkFactory`) — one impl built, one missing
- The `RaftNetworkFactory` trait is the isolation seam (ground rule 3). Nodes
  take it via `start_with_network(...)`.
- **`ChannelNetworkFactory`** (`partition/network.rs`): in-process, forwards each
  RPC straight to the target's `Raft` handle via a `Registry` (node_id → handle),
  with seeded, directional fault injection. Every correctness test and the M7
  harness runs on this.
- **ZMQ-backed factory**: the production seam, **not built**. No `RaftNetwork`
  impl dials sockets yet; `start_with_network`'s doc comment marks the hole.

### 3. Storage (RocksDB) — built
- One `Storage`/DB per process, **column-family per group**. `RocksLogStore` per
  group holds the raft log; the state machine plus durable control records
  (serving gate, learner admission, bootstrap markers, sequence/idempotency
  records, snapshots via SST export/ingest) live in `Storage`.

### 4. Transport planes — partially built
- **Control lane** (`control_addr`): small RPCs. Inbound = ZMQ `ROUTER`
  (`ZmqServer`, `transport/router.rs`); outbound = `DEALER`
  (`transport/dealer.rs`). Single `Envelope` codec; `MsgType` discriminates.
- **Bulk lane** (`bulk_addr`): snapshot install traffic, off the control lane
  (config enforces `control_addr != bulk_addr`).
- **HTTP lane** (`http_addr`): read-only `/status` + `/health` — planned (see
  `M8_HTTP_STATUS_PLAN.md`).

### 5. Dispatch / envelope routing (§10.2)
Inbound frame → decode `Envelope` → split by `MsgType::is_peer_control()`:
- **Client frames** (`ClientOp`, `MetaQuery`) → `ClientGateway` (built): hosts a
  map of `partition → PartitionNode`, checks the key hashes to the claimed
  partition, serves or redirects, and **rejects any peer-control frame** (ground
  rule 9).
- **Peer/operator-control frames** (Raft append/vote/snapshot, heartbeat,
  become-learner, config observations, join/leave/abort-plan) → a peer-control
  dispatcher that **does not exist yet**. The `transport/raft_wire.rs` body types
  are the payloads for exactly this dispatcher.

### 6. Threading model (§11)
- A tokio multi-thread runtime runs all async work (raft, gateway handlers, HTTP).
- ZMQ sockets are not `Send`, so each `ROUTER` lives on **one dedicated poller
  thread** that bridges to the runtime via channels — a slow handler cannot block
  the poller.

## The `runtime::Node` struct (proposed)

```rust
// src/runtime/node.rs
pub struct Node {
    cfg: NodeConfig,
    cluster: ClusterConfig,                 // p, r, hash_spec, cluster_id — recovered from meta state
    storage: Arc<Storage>,                  // one RocksDB, CF-per-group

    // Consensus handles
    meta: Option<Arc<MetaNode>>,            // Some iff this node is a meta voter
    partitions: Arc<RwLock<HashMap<u16, Arc<PartitionNode>>>>,  // SHARED + MUTABLE (see notes)

    // Client + control planes
    gateway: Arc<ClientGateway>,            // reads `partitions` + routing
    routing: Arc<dyn RoutingSource>,        // meta-placement-backed snapshot

    // Production network seam (UNBUILT)
    net_meta: ZmqNetworkFactory<MetaTypeConfig>,
    net_data: ZmqNetworkFactory<TypeConfig>,

    // Inbound servers (each owns a poller thread)
    control_srv: ZmqServer,                 // ROUTER on control_addr, root dispatcher
    bulk_srv: ZmqServer,                    // ROUTER on bulk_addr, snapshot lane
    http: Option<JoinHandle<()>>,           // /status + /health, if http_addr set

    // Background drivers + lifecycle
    tasks: Vec<JoinHandle<()>>,             // heartbeat emitter, reconcile loop, leader rebalancer
    shutdown: watch::Sender<bool>,
}
```

Two supporting pieces this struct forces into being, neither of which exists yet:

```rust
// Production RaftNetwork: dials control_addr (RPC) / bulk_addr (snapshot)
// resolved from the meta directory, over the ZMQ DEALER pool. Replaces
// ChannelNetworkFactory at the `start_with_network` seam.
struct ZmqNetworkFactory<C> { resolver: Arc<DirectoryResolver>, dealer: DealerPool, /* ... */ }

// The peer/operator-control dispatcher: the missing Server.
struct RootDispatch {
    gateway: Arc<ClientGateway>,
    meta: Option<Arc<MetaNode>>,
    partitions: Arc<RwLock<HashMap<u16, Arc<PartitionNode>>>>,
    storage: Arc<Storage>,
}
impl Server for RootDispatch {
    async fn serve(&self, env: Envelope) -> Envelope {
        if env.msg_type.is_peer_control() {
            self.serve_control(env).await   // Raft append/vote/snapshot, heartbeat,
                                            // become-learner, observations, join/leave/abort
        } else {
            self.gateway.serve(env).await   // ClientOp / MetaQuery
        }
    }
}
```

### Architectural consequences
1. **`partitions` must be a shared, mutable registry**, not the plain `HashMap`
   `ClientGateway::new` takes today. Rebalancing adds/removes hosted partitions at
   runtime (a `BecomeLearner` frame starts a new `PartitionNode`; a completed
   drain stops one). Gateway and control dispatcher must see the same live set →
   `Arc<RwLock<HashMap<..>>>`. **Refactor required:** `ClientGateway` reads that
   shared handle.
2. **Inbound server must be bound before quorum can form, yet tolerate
   "group not up yet."** Peers must reach each other's ROUTER to elect, so bind
   early; the dispatcher returns a retryable error for frames whose target group
   is not started yet.

## Startup sequence (`Node::run(cfg)`)

1. **Open storage** — `Storage::open(cfg.data_dir)` (all CFs). Precedes any raft;
   log stores live in it.
2. **Recover identity / bootstrap** — read the committed meta cluster config:
   - *Resume:* found → take `p, r, hash_spec` from it.
   - *Fresh:* not found → run the bootstrap driver from the descriptor
     (`ensure_bootstrap_group`; on the `designated` node, `seed_cluster`).
     Bootstrap is idempotent/resumable — a crash mid-bootstrap re-runs safely.
3. **Build the directory resolver** — `node_id → (control_addr, bulk_addr)` from
   the meta directory (or the bootstrap descriptor initially). Shared
   `zmq::Context` + DEALER pool created here.
4. **Construct the ZMQ network factories** for meta and data groups over the
   resolver. *(Unbuilt seam.)*
5. **Start the meta group** — if `node_id ∈ meta_voters`:
   `MetaNode::start_with_network(node_id, storage, net_meta, tuning)` → `Arc`
   (`authorize_group_start` runs inside).
6. **Start hosted data partitions** — for each committed placement whose
   `voters ∪ target_voters` contains `node_id`:
   `PartitionNode::start_with_network(node_id, Data(p), storage, net_data, tuning)`;
   reconcile its serving gate via `reconcile::gate(placement, committed_voters)`
   against durable admission records; insert into the shared `partitions` map.
7. **Build routing + gateway** — a meta-backed `RoutingSource` (reads `MetaNode`
   local placements + directory), then `ClientGateway` over the shared
   `partitions` handle.
8. **Assemble `RootDispatch`** (gateway + meta + partitions + storage).
9. **Bind inbound planes** — `ZmqServer::bind(ctx, control_addr, root_dispatch)`
   and a bulk `ZmqServer` on `bulk_addr`.
10. **Spawn HTTP** —
    `if let Some(a) = cfg.http_addr { spawn http::serve(a.parse()?, status_source, shutdown_rx) }`.
    `None` → skip.
11. **Spawn background drivers** — heartbeat emitter (liveness → meta voters), the
    reconcile loop (reacts to placement changes: start/stop partitions, honor
    `BecomeLearner`), and — active only while meta leader — the rebalancer/plan
    driver.
12. **Return `Node`**; `run` awaits the shutdown signal.

## Shutdown (`Node::shutdown`)
Signal `watch` → drop the two `ZmqServer`s (their `Drop` stops the poller
threads, so no new frames) → `raft.shutdown()` on meta + every partition → abort
background tasks → drop `Storage` (flush/close). Graceful HTTP shutdown is wired
to the same signal.

## Example: client write path (once assembled)
`Client` (DEALER) → target `control_addr` ROUTER → `ClientGateway` →
`PartitionNode::write` → data-group `client_write` → majority commit →
state-machine apply → reply. Not-leader/not-hosted → `Redirect` with candidate
voters (advisory; the serving gate is authority).

## Net new work this implies
- `ZmqNetworkFactory` + `DirectoryResolver` + DEALER pool — the production
  `RaftNetwork`.
- `RootDispatch::serve_control` — the peer/operator dispatcher (consumes the
  `raft_wire.rs` bodies).
- `ClientGateway` refactor to the shared partition registry.
- The reconcile loop that mutates the hosted partition set at runtime.
- The `runtime::Node` assembly itself and `dal run` wiring.
