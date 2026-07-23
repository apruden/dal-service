# Runtime architecture

How a `dal` node is assembled and started as a process. Describes the design
(DESIGN §10–11) and the current tree. The keystone piece — an assembled
`runtime::Node` launched by `dal run` — is **built** (M8, `src/runtime/`), and
the `runtime_m8` suite exercises a three-node cluster over real ZeroMQ
`inproc://`. What remains open is operability, not assembly: the operator CLI
subcommands are stubs and there is no runtime teardown of a locally-removed
partition (see "Status" at the end).

## Layers

### 1. Consensus (openraft) — built
- **One meta Raft group** (`MetaNode`, `MetaTypeConfig`, command = `MetaCommand`):
  cluster config, node directory, placements, move plans. Reads via
  `ensure_linearizable` (ReadIndex); writes via `client_write`.
- **P data Raft groups** (`PartitionNode`, `TypeConfig`, command = `DataRequest`):
  one per partition, holding the KV data. A single physical node hosts the meta
  group (only if it is a meta voter) **plus** whichever data partitions its
  placement assigns it.

### 2. Network seam (`RaftNetworkFactory`) — both impls built
- The `RaftNetworkFactory` trait is the isolation seam (ground rule 3). Nodes
  take it via `start_with_network(...)`.
- **`ChannelNetworkFactory`** (`partition/network.rs`): in-process, forwards each
  RPC straight to the target's `Raft` handle via a `Registry` (node_id → handle),
  with seeded, directional fault injection. Every correctness test and the M7
  harness runs on this.
- **`RaftPeerFactory<T>`** (`transport/raft_net.rs`): the production seam. Dials a
  peer's `control_addr` (append/vote) and `bulk_addr` (snapshot) resolved from the
  meta directory over the DEALER pool. `runtime::Node` passes it into
  `start_with_network` for both the meta and each data group; the `runtime_m8`
  tests run it over ZeroMQ `inproc://`.

### 3. Storage (RocksDB) — built
- One `Storage`/DB per process, **column-family per group**. `RocksLogStore` per
  group holds the raft log; the state machine plus durable control records
  (serving gate, learner admission, bootstrap markers, sequence/idempotency
  records, snapshots via SST export/ingest) live in `Storage`.

### 4. Transport planes — built
- **Control lane** (`control_addr`): small RPCs. Inbound = ZMQ `ROUTER`
  (`ZmqServer`, `transport/router.rs`); outbound = `DEALER`
  (`transport/dealer.rs`). Single `Envelope` codec; `MsgType` discriminates.
- **Bulk lane** (`bulk_addr`): snapshot install traffic, off the control lane
  (config enforces `control_addr != bulk_addr`).
- **HTTP lane** (`http_addr`): read-only `/status` + `/health`, built
  (`runtime/http.rs`, `M8_HTTP_STATUS_PLAN.md`). Listener bound before storage
  opens; serving task aborted on shutdown.

### 5. Dispatch / envelope routing (§10.2)
Inbound frame → decode `Envelope` → split by `MsgType::is_peer_control()`:
- **Client frames** (`ClientOp`, `MetaQuery`) → `ClientGateway` (built): hosts a
  map of `partition → PartitionNode`, checks the key hashes to the claimed
  partition, serves or redirects, and **rejects any peer-control frame** (ground
  rule 9).
- **Peer/operator-control frames** (Raft append/vote/snapshot, heartbeat,
  become-learner, config observations, join/leave/abort-plan) → the peer-control
  dispatcher `RootDispatch` (`runtime/dispatch.rs`), built. The
  `transport/raft_wire.rs` body types are its payloads.

### 6. Threading model (§11)
- A tokio multi-thread runtime runs all async work (raft, gateway handlers, HTTP).
- ZMQ sockets are not `Send`, so each `ROUTER` lives on **one dedicated poller
  thread** that bridges to the runtime via channels — a slow handler cannot block
  the poller.

## The `runtime::Node` struct

Built in `src/runtime/node.rs`. The sketch below is the original design intent;
field-level details (e.g. the production factory is carried into
`start_with_network` rather than stored, and inbound servers are `ZmqServer`s
owning `RootDispatch`) live in the code — read it for the exact shape.

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

    // Production network seam — built as RaftPeerFactory<T>; the real Node
    // passes it into start_with_network rather than storing these fields.
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

Two supporting pieces this struct forced into being, both now built:

```rust
// Production RaftNetwork: dials control_addr (RPC) / bulk_addr (snapshot)
// resolved from the meta directory, over the ZMQ DEALER pool. Replaces
// ChannelNetworkFactory at the `start_with_network` seam.
// Built as `RaftPeerFactory<T>` in transport/raft_net.rs.
struct ZmqNetworkFactory<C> { resolver: Arc<DirectoryResolver>, dealer: DealerPool, /* ... */ }

// The peer/operator-control dispatcher. Built in runtime/dispatch.rs.
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
   `Arc<RwLock<HashMap<..>>>`. Done for *start*: `ClientGateway` reads the shared
   handle and `BecomeLearner` inserts a freshly started `PartitionNode`. *Stop* is
   still open — a completed drain does not yet remove/reclaim the local group.
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

## Status

Built (M8):
- `RaftPeerFactory<T>` + directory-backed address resolution + DEALER pool — the
  production `RaftNetwork` (`transport/raft_net.rs`).
- `RootDispatch` — the peer/operator dispatcher consuming the `raft_wire.rs`
  bodies (`runtime/dispatch.rs`).
- `ClientGateway` over the shared partition registry; `BecomeLearner` starts and
  inserts a new `PartitionNode` at runtime.
- The `runtime::Node` assembly, `Node::bootstrap` (resumable genesis), and the
  `dal run` binary wiring.
- Background drivers: heartbeat emitter (durable incarnation), failure detector,
  rebalance/abort driver.

Open:
- **Operator CLI subcommands** `init`/`join`/`leave`/`abort-plan`/`status` are
  stubs in `main.rs` (only `run` is wired). `RootDispatch` serves the matching
  frames, but no `api` client issues them.
- **Runtime partition teardown.** A drain that removes this node from a
  partition swaps membership but does not stop the orphaned local `PartitionNode`
  or reclaim its CFs (DESIGN §7.3). Dynamic *start* exists; dynamic *stop* does
  not — no reconcile loop tears down locally-removed groups.
