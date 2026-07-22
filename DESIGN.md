# DAL Service — Distributed Key-Value Store Design

A partitioned, strongly-consistent key-value store. Keys are distributed across
partitions with **consistent hashing**; each partition is independently
replicated with **Raft** (via [`openraft`](https://github.com/databendlabs/openraft)).
Each replica persists to **RocksDB**. All inter-node and client traffic uses
**ZeroMQ**.

Design priorities, in order: **correctness first, availability second,
performance third.** Every decision below that trades one for another resolves
in that order.

---

## 1. Goals and non-goals

### Goals
- Linearizable `get` / `put` / `delete` on a single key.
- Horizontal scaling by adding nodes; graceful shrink by removing them.
- No data loss and no stale reads across node add/remove and node failure,
  provided a majority of each partition's replicas survives.
- Availability of unaffected partitions while other partitions rebalance.

### Non-goals (v1)
- Multi-key transactions / cross-partition atomicity.
- Secondary indexes, range scans across partitions, SQL.
- Geo-replication / multi-region Raft tuning.
- Automatic capacity-based load balancing (placement is hash-driven, not
  size-driven). Hooks are left for a future balancer.

---

## 2. System model and consistency

- **Consistency:** linearizable per key. A successful `put`/`delete` is durable
  on a majority of the partition's replicas before acknowledgement. A `get`
  reads through the Raft leader (read-index / lease read) so it never returns a
  value older than the last acknowledged write.
- **Durability:** a write is acked only after it is committed by Raft (majority
  quorum) *and* the leader's RocksDB write for the log entry is fsync-durable.
- **Fault model:** crash-recovery, non-Byzantine. Nodes may crash, restart,
  lose in-flight state, and rejoin. The network may drop, delay, reorder, and
  duplicate messages. We assume clocks are *not* perfectly synchronized; leases
  use a bounded clock-drift assumption (see §9.3).
- **Liveness:** a partition remains available for reads and writes as long as a
  majority of its replicas are up and can communicate.

---

## 3. High-level architecture

```
                         ┌─────────────────────────────────────────┐
                         │              Client library              │
                         │  hash(key) → partition → route to leader │
                         └───────────────┬─────────────────────────┘
                                         │ ZeroMQ (DEALER→ROUTER)
        ┌────────────────────────────────┼────────────────────────────────┐
        │                                 │                                │
 ┌──────▼───────┐                  ┌──────▼───────┐                 ┌──────▼───────┐
 │    Node A    │                  │    Node B    │                 │    Node C    │
 │              │                  │              │                 │              │
 │ Router/Sock  │◄──── ZeroMQ ────►│ Router/Sock  │◄──── ZeroMQ ───►│ Router/Sock  │
 │ Partition    │  (Raft RPC +     │ Partition    │  (Raft RPC +    │ Partition    │
 │  replicas:   │   redirects +    │  replicas:   │   snapshots)    │  replicas:   │
 │  P1(L) P2(F) │   migration)     │  P2(L) P3(F) │                 │  P3(L) P1(F) │
 │  P3(F)       │                  │  P1(F)       │                 │  P2(F)       │
 │              │                  │              │                 │              │
 │ RocksDB      │                  │ RocksDB      │                 │ RocksDB      │
 │  (per part.  │                  │  (per part.  │                 │  (per part.  │
 │   CF + log)  │                  │   CF + log)  │                 │   CF + log)  │
 └──────────────┘                  └──────────────┘                 └──────────────┘

         L = Raft leader replica for that partition,  F = follower replica
```

Two independent control loops run in the cluster:

1. **Metadata / cluster-control group** — a *single* dedicated Raft group (the
   "meta group") that stores authoritative cluster state: the node membership
   list, the partition→replica placement map, and the epoch counter. This is the
   source of truth every node and client consults.
2. **Per-partition data groups** — one Raft group per partition, replicating the
   actual key-value data.

Separating the two means routing/placement decisions are themselves replicated
and linearizable, and a partition rebalance is driven by committed meta-group
decisions rather than ad-hoc gossip.

---

## 4. Data model and API

### 4.1 Value model
- Key: opaque bytes (≤ 4 KiB recommended).
- Value: opaque bytes (≤ 16 MiB, configurable).
- Each key carries internal metadata: a monotonic per-key version (Raft log
  index of the last mutation) used for idempotency and debugging.

### 4.2 Operations
| Op       | Request                          | Response                              |
|----------|----------------------------------|---------------------------------------|
| `get`    | `{ key }`                        | `{ found, value?, version }`          |
| `put`    | `{ key, value, if_version? }`    | `{ ok, version }` or `{ redirect }`   |
| `delete` | `{ key, if_version? }`           | `{ ok, version }` or `{ redirect }`   |

- `if_version` gives optional compare-and-set (CAS): the mutation applies only
  if the current version matches. This is the single-key primitive that lets
  clients build safe read-modify-write without cross-key transactions.
- Every request carries a client-generated `request_id` (UUID) for idempotent
  retries (§8.4).

---

## 5. Partitioning: consistent hashing

### 5.1 Two-level mapping (key → partition → nodes)

We deliberately do **not** hash keys directly onto physical nodes. Instead:

```
key ──hash──► partition_id ──placement map──► [replica node set]
```

- **Fixed partition count `P`** (e.g. `P = 4096`), chosen once at cluster
  creation and never changed. `partition_id = xxhash64(key) % P`.
- A **consistent hash ring** places *physical nodes* (via `V` virtual nodes
  each) on a 64-bit ring. A partition's **primary owner** is the first physical
  node found walking clockwise from `hash(partition_id)`. Its **replicas** are
  that node plus the next `R-1` *distinct* physical nodes clockwise (`R` =
  replication factor, e.g. 3).

Why this hybrid?
- Direct key→node consistent hashing would make every key its own Raft decision —
  impossible. Fixed partitions give us a bounded number (`P`) of Raft groups.
- Consistent hashing over the ring (rather than `node = partition % N`) means
  adding/removing a node reshuffles only `O(P/N)` partitions instead of nearly
  all of them. This is the core property that makes scaling cheap.

`R`, `P`, and `V` are cluster constants stored in the meta group.

### 5.2 Placement map

The consistent-hash ring computes a *desired* placement. The meta group stores
the *actual, committed* placement map:

```
partition_id → {
    epoch:        u64,          // bumped on every membership change to this partition
    replicas:     [NodeId; R],  // current committed Raft voters
    learners:     [NodeId],     // nodes catching up (non-voting), during rebalance
    leader_hint:  NodeId,       // last known leader, for routing (advisory)
    state:        Stable | Rebalancing,
}
```

The ring is a *function that proposes* placement; the map is the *replicated
truth*. A rebalance is the process of driving `actual` toward `desired` one safe
Raft membership change at a time (§7).

---

## 6. Storage layer (RocksDB)

Each node runs a single RocksDB instance with **column families keyed by
partition**, so partitions are physically isolated (cheap to drop/ingest on
migration):

- `cf_data_<partition>` — the applied key-value state machine for that partition.
- `cf_raftlog_<partition>` — Raft log entries (openraft `RaftLogStorage`).
- `cf_meta_<partition>` — Raft hard state (term, vote), last-applied index,
  snapshot pointer, and the partition epoch.
- `cf_cluster` — a local cache of the meta-group state (rebuilt from the meta
  group on startup; not authoritative).

Implementation notes:
- We implement openraft's `RaftLogStorage` and `RaftStateMachine` traits over
  these CFs. Log writes use `WriteOptions{ sync: true }` (fsync) before ack so a
  crash cannot lose a committed entry.
- **Snapshots** are produced with a RocksDB checkpoint / SST export of
  `cf_data_<partition>`, streamed to a new replica during catch-up. This makes
  Raft snapshot install and bulk data migration the *same mechanism*.
- Applying a committed entry (`put`/`delete`) and advancing `last_applied` occur
  in **one atomic `WriteBatch`**, so recovery never double-applies or skips.
- `delete` writes a tombstone through Raft; RocksDB compaction reclaims space.
  Tombstones are also required so a migrating replica learns about deletions.

---

## 7. Cluster membership changes (the hard part)

This section is the heart of the design: how the cluster stays correct and
available while nodes are **added** and **removed**. The invariant we protect:

> **A partition's data is never served from, nor acknowledged by, a replica set
> that has not committed that data via Raft majority.** Ownership transfer only
> completes after the new owners have caught up *inside* the Raft group.

### 7.1 The two membership layers must not be conflated

- **Ring/placement change** (a node joins or leaves) changes the *desired* owner
  set of many partitions at once.
- **Raft membership change** (`openraft::Raft::change_membership`) changes the
  *actual* voter set of *one* partition, using joint consensus, and is only safe
  when the new members are caught up.

The rebalancer's whole job is to translate the first into a *sequence* of the
second, safely and idempotently. We never "just point the ring at new nodes" and
start serving — that would risk stale reads and lost writes.

### 7.2 Adding a node

Trigger: an operator (or autoscaler) calls `join(node_id, addr)` against the
meta group.

1. **Register the node.** Meta group commits the new node into the membership
   list and inserts its virtual nodes into the ring. This is a metadata-only
   commit; no data moves yet. The node starts empty.
2. **Compute the diff.** For each partition, recompute desired replicas from the
   new ring. A subset `S` of partitions now want the new node as a replica
   (roughly `R·P/N` of them). The meta group marks those partitions
   `state = Rebalancing` and records the target replica set.
3. **Add as learner first.** For each partition in `S`, the current Raft leader
   runs `add_learner(new_node)`. openraft begins streaming log + snapshot to the
   new (non-voting) replica. **The new node cannot vote or acknowledge quorum
   yet, so it cannot affect correctness while catching up.** Availability of the
   partition is unaffected.
4. **Promote when caught up.** Once the learner's `matched` index is within a
   small threshold of the leader's commit index, the leader calls
   `change_membership` (joint consensus) to make the new replica set the voters.
   If a replica is being *displaced* (the ring no longer wants an old node for
   this partition), the same joint-consensus step removes it.
5. **Retire displaced replicas.** After the joint config commits and the removed
   voter is no longer needed by *any* partition, its `cf_*_<partition>` is
   dropped and disk reclaimed. The meta group flips the partition back to
   `state = Stable`, bumps its `epoch`, and updates `replicas`.

Throttling: rebalances run with a bounded concurrency (e.g. ≤ N partitions
migrating cluster-wide, ≤ 1 per partition) so snapshot streaming can't saturate
the network and starve foreground traffic. Correctness never depends on the
throttle; it only protects availability/latency.

### 7.3 Removing a node

Two flavors:

**(a) Graceful decommission** — `leave(node_id)` on the meta group:
1. Meta group marks the node `Draining` and removes its virtual nodes from the
   ring, producing a new desired placement for every partition the node hosted.
2. For each such partition, add the *replacement* node as a learner (§7.2 step
   3), let it catch up, then `change_membership` to swap the draining node out
   for the replacement. Leadership is transferred away first if the draining
   node was the leader (`openraft` leadership transfer).
3. When the node hosts no more voters or learners, meta group removes it from
   the membership list. The operator can now power it off. Its RocksDB can be
   deleted.

Draining is the **safe** path: no partition ever drops below majority because
the replacement joins *before* the old replica leaves (joint consensus moves
from `{old set}` to `{new set}` atomically, and the new voter is already
caught up).

**(b) Sudden failure** — a node crashes without draining:
1. Failure detection (§9.1) marks the node `Suspect` then `Down` in the meta
   group after a timeout.
2. Each partition that had a replica on the down node is now under-replicated but
   **still available as long as a majority (`⌊R/2⌋+1`) of its replicas
   remain.** With `R=3`, one failure leaves 2/3 — still a quorum. Reads/writes
   continue on the surviving majority.
3. The meta group treats the down node like a decommission target: it picks a
   replacement node from the ring (next node clockwise not already a replica),
   adds it as a learner to each affected partition, catches it up from a
   surviving leader's snapshot, and promotes it via `change_membership`.
4. If the crashed node returns, it discovers (via the meta group + Raft term)
   that it has been replaced for some partitions; it drops those CFs and, for any
   partition where it is still a member, rejoins Raft and catches up from its
   persisted log.

**Correctness boundary:** if *more* than a minority fail simultaneously (e.g. 2
of 3), that partition loses quorum and becomes **unavailable for writes and
linearizable reads** until enough replicas return. We deliberately choose
unavailability over serving possibly-stale data — this is the correctness-first
tradeoff. Operators can, as a last-resort manual action, invoke an unsafe
`force_recover(partition, surviving_replica)` that rebuilds a single-voter group
from one survivor (documented as data-loss-possible).

### 7.4 Epochs fence stale actors

Every partition carries a monotonically increasing `epoch`, bumped on each
committed membership change. Every data request and every routed message carries
the epoch the client/node believes is current.
- A replica rejects a request stamped with an **older** epoch and returns the
  new placement, forcing the caller to refresh.
- This fences a just-removed node (or a partitioned-away old leader) from
  serving traffic under a stale view. Combined with Raft's own term-based leader
  fencing, no zombie replica can ack writes or serve reads after it's been
  replaced.

---

## 8. Request routing and the client

### 8.1 Client-side routing
The client library:
1. Fetches (and caches, with TTL + epoch) the cluster state from any node
   (which serves it from its meta-group cache).
2. Computes `partition_id = hash(key) % P`.
3. Looks up `leader_hint` for that partition and sends the request there over a
   ZeroMQ `DEALER` socket.

### 8.2 Redirects
Routing hints are advisory. If a request reaches a non-leader (or a replica with
a newer epoch), the replica replies with `{ redirect: leader_addr, epoch, replicas }`
instead of processing it. The client updates its cache and retries. This keeps
clients correct even with a stale cache — the authoritative check happens at the
Raft leader.

### 8.3 Read path
`get` is linearizable via openraft's **read-index / lease read**: the leader
confirms it is still leader (via heartbeat quorum or a valid lease) before
answering from its local applied state. No log write is needed for reads, so
reads are cheap but never stale.

### 8.4 Idempotent retries
Each mutation carries a `request_id`. The partition state machine keeps a bounded
dedup cache (`request_id → result`, persisted in `cf_data`). A retried `put`
after a network timeout returns the original result instead of applying twice.
This makes client retries safe across leader changes and rebalances.

---

## 9. Failure detection and correctness details

### 9.1 Failure detection
- Each node sends periodic heartbeats to the meta group and to peers it shares
  partitions with. Missed heartbeats past `suspect_timeout` → `Suspect`; past
  `down_timeout` → the meta group *commits* a `Down` transition (a replicated
  decision, not a local guess), which triggers §7.3(b).
- Only the meta group may declare a node `Down`, so all nodes act on one
  consistent view — avoiding split-brain re-replication storms.

### 9.2 Split brain / network partitions
- A minority-side leader of any Raft group cannot commit (no majority) and its
  lease expires, so it stops serving linearizable reads. Clients on the minority
  side get errors/redirects, not stale data.
- The meta group itself is a Raft group; only its majority side can commit
  placement changes. A minority partition of the cluster cannot rebalance,
  which is correct.

### 9.3 Lease reads and clock drift
Lease reads assume bounded clock drift `ε`. A leader treats its lease as valid
for `heartbeat_interval − ε`. If clocks are untrustworthy, the read path falls
back to full read-index (an explicit heartbeat round) — slower but not
drift-dependent. Configurable per deployment.

### 9.4 Why data can't be lost or read stale during rebalance
- New replicas join as **learners** and receive data through Raft before they
  can vote → they never serve or ack data they don't have.
- Voter set changes go through **joint consensus** → there is never a moment when
  two disjoint majorities exist.
- **Epoch fencing** + Raft term fencing → replaced/partitioned replicas can't
  serve under stale ownership.
- Writes ack only after **majority commit + fsync** → an acked write survives any
  minority failure.

---

## 10. Transport (ZeroMQ)

### 10.1 Socket topology
- **Per node, one `ROUTER` socket** bound for inbound RPC (from clients and
  peers). ROUTER preserves peer identity for replies and multiplexes many peers.
- **Per node, `DEALER` sockets** for outbound RPC to peers (Raft
  append-entries, vote, snapshot, migration), one connection per peer, pooled.
- Clients use a `DEALER` per node connection, load-balanced by the client
  library's routing (not by ZMQ round-robin — routing is key-directed).
- Snapshot / bulk migration streams use a dedicated `ROUTER/DEALER` pair (or a
  separate `PUSH/PULL` pipe) on a second port so large transfers don't
  head-of-line-block small Raft heartbeats.

### 10.2 Message framing
- Envelope: `[ msg_type | partition_id | epoch | request_id | payload ]`.
- Payloads serialized with a compact binary codec (e.g. `bincode`/`prost`).
- `msg_type` covers: `ClientOp`, `RaftAppend`, `RaftVote`, `RaftSnapshot`,
  `MigrationChunk`, `MetaQuery`, `Redirect`, `Heartbeat`.

### 10.3 Reliability concerns with ZeroMQ
ZeroMQ gives us framing and reconnection but **not** delivery guarantees, and a
`REQ/REP` socket can wedge on a lost reply. Therefore:
- We use `ROUTER/DEALER` (never `REQ/REP`) so a lost message can't deadlock a
  socket state machine.
- Every RPC has an application-level **timeout + retry** with the idempotency
  key; correctness never relies on ZMQ delivering exactly once.
- Raft already tolerates message loss/reorder/duplication, so the transport only
  needs best-effort datagrams with reconnection — which is exactly what
  `ROUTER/DEALER` provides.
- Backpressure: bounded send high-water-marks; when a peer is slow, Raft's own
  flow control (inflight limits) plus migration throttling prevent unbounded
  queue growth.

---

## 11. Rust module layout

```
src/
  main.rs               // node bootstrap: config, sockets, spawn runtimes
  config.rs             // P, R, V, timeouts, paths, ports
  transport/
    mod.rs
    router.rs           // inbound ROUTER loop, dispatch by msg_type
    dealer.rs           // outbound DEALER pool, retries, timeouts
    codec.rs            // envelope framing + (de)serialization
  hashing/
    ring.rs             // consistent hash ring, virtual nodes
    partition.rs        // key → partition_id, desired placement calc
  meta/
    group.rs            // meta Raft group: membership + placement map
    rebalancer.rs       // desired vs actual diff → sequenced Raft changes
    failure.rs          // heartbeats, suspect/down state machine
  partition/
    node.rs             // one openraft instance per partition
    state_machine.rs    // apply put/delete, dedup cache, CAS
    log_store.rs        // openraft RaftLogStorage over RocksDB CF
    snapshot.rs         // checkpoint/SST export + install (== migration)
  storage/
    rocks.rs            // RocksDB handle, CF lifecycle (create/drop)
    batch.rs            // atomic apply + last_applied advance
  api/
    ops.rs              // get/put/delete request handling, redirects
    client.rs           // client library (routing, cache, retries)
```

Runtime: Tokio for async task orchestration; each partition's openraft instance
runs its own log/apply loops. ZeroMQ integrated via `zmq` + a dedicated poller
thread that bridges sockets to Tokio channels (ZMQ sockets are not `Send` across
arbitrary threads, so each socket lives on one owning task/thread).

---

## 12. Key parameters (defaults)

| Param | Meaning | Default |
|-------|---------|---------|
| `P` | partition count | 4096 |
| `R` | replicas per partition | 3 |
| `V` | virtual nodes per physical node | 128 |
| `suspect_timeout` | heartbeat miss → Suspect | 3 s |
| `down_timeout` | Suspect → committed Down | 15 s |
| `max_concurrent_migrations` | cluster-wide rebalance throttle | N (node count) |
| `snapshot_chunk` | migration stream chunk size | 4 MiB |
| value size cap | max value bytes | 16 MiB |

---

## 13. Failure-scenario summary

| Scenario | Behavior |
|----------|----------|
| Leader of a partition crashes | Raft elects a new leader from survivors; clients redirected; no data loss. |
| One replica of `R=3` fails | Partition stays available (2/3 quorum); meta group re-replicates to a new node. |
| Majority of a partition fails | Partition unavailable for writes + linearizable reads (correctness over availability) until quorum returns or manual `force_recover`. |
| Node added | Metadata commit → learner catch-up → joint-consensus promotion; only `~R·P/N` partitions move; rest untouched. |
| Node gracefully removed | Replacement joins as learner and is promoted *before* old replica leaves; never drops below majority. |
| Network partition | Only the majority side of each Raft group serves; minority returns errors/redirects, never stale data. |
| Stale client cache | Redirect + epoch fencing corrects it on the next request. |
| Duplicate/retried write | Idempotency dedup cache returns the original result. |

---

## 14. Open questions / future work

- **Load-aware placement:** current placement is purely hash-driven; a future
  balancer could move partitions off hot/full nodes (the migration mechanism
  already exists; only the *decision* input changes).
- **Read replicas / follower reads** with bounded staleness for read-heavy
  workloads.
- **Multi-key transactions** via a 2PC coordinator layered over per-partition
  Raft groups.
- **Dynamic `P`** (partition splitting/merging) to avoid choosing `P` up front.
- **Compression / TTL** at the RocksDB layer.
