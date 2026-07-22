# DAL Service — Distributed Key-Value Store Design

A partitioned, strongly-consistent key-value store. Keys are hashed onto a
fixed set of partitions; partitions are assigned to nodes by an explicit,
replicated placement map. Each partition is independently replicated with
**Raft** (via [`openraft`](https://github.com/databendlabs/openraft)).
Each replica persists to **RocksDB**. All inter-node and client traffic uses
**ZeroMQ**.

Design priorities, in order: **correctness first, simplicity second,
availability third, performance fourth.** Every decision below that trades one
for another resolves in that order. Where a mechanism is advisory rather than
authoritative, we choose the simplest form that preserves the correctness
argument.

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
- Automatic capacity-based load balancing (placement balances replica counts,
  not data size or load). Hooks are left for a future balancer.

---

## 2. System model and consistency

- **Consistency:** linearizable per key. A successful `put`/`delete` is durably
  committed by a Raft majority before acknowledgement. A `get` is served only
  by the current Raft leader after a quorum read check, so it never returns a
  value older than the last acknowledged write.
- **Durability:** a voter acknowledges an append only after its log entry and
  any required hard-state update are fsync-durable. The leader acknowledges the
  client only after a quorum of such durable acknowledgements commits the entry.
  The leader's own durable append is therefore necessary but not sufficient.
- **Fault model:** crash-recovery, non-Byzantine. Nodes may crash, restart,
  lose in-flight state, and rejoin. The network may drop, delay, reorder, and
  duplicate messages. No clock-synchronization or bounded-drift assumption is
  made: linearizable reads use ReadIndex (§8.3), never leases.
- **Liveness:** a partition remains available for reads and writes as long as a
  majority of its replicas are up and can communicate. Loss of meta-group
  quorum stops topology changes, but does not stop already-configured data
  groups: routing state is served from cached/follower copies anyway (§8.1),
  and the data group itself remains the authority on whether it can serve.

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
   "meta group") that stores authoritative cluster state for node membership,
   desired placement, and fenced rebalance plans. It is the source of truth for
   *control decisions*, not an atomic mirror of every data group's Raft config.
   The committed membership entry in each partition's Raft log is authoritative
   for that partition's current voters.
2. **Per-partition data groups** — one Raft group per partition, replicating the
   actual key-value data.

Separating the two means routing/placement decisions are themselves replicated
and linearizable, and a partition rebalance is driven by committed meta-group
decisions rather than ad-hoc gossip.

### 3.1 Bootstrap and control-plane availability

Cluster creation is an explicit administrative operation: it writes immutable
cluster identity, `P`, `R`, protocol version, and the initial meta-group
voter set to durable storage before any data partition is created. It then
creates each initial partition with an explicit durable voter configuration;
creation fails unless at least `R` eligible, distinct nodes exist. The initial
meta group is itself a normal Raft group with its own documented bootstrap,
membership-change, snapshot, and disaster-recovery procedure. Joining nodes
verify the cluster identity and protocol version before they are admitted.

The meta group must retain a quorum for joins, leaves, failure declarations, and
rebalance plans. It is deliberately not placed in the foreground data path:
once a partition is configured, its data Raft group can continue to serve using
the serving gate in §7.4 even when the control plane is unavailable. No
rebalancer action may start or advance without meta-group quorum.

---

## 4. Data model and API

### 4.1 Value model
- Key: opaque bytes (≤ 4 KiB recommended).
- Value: opaque bytes (≤ 16 MiB, configurable).
- Each key carries internal metadata: a monotonic per-key version (the Raft log
  index of its last mutation). Versions are log indexes, so they are strictly
  monotonic per partition and never reused — even across delete/recreate. A
  deleted key leaves **no tombstone**; it is simply absent (see §4.2 for CAS
  semantics on absent keys).

### 4.2 Operations
| Op       | Request                          | Response                              |
|----------|----------------------------------|---------------------------------------|
| `get`    | `{ key }`                        | `{ found, value?, version }`          |
| `put`    | `{ key, value, if_version?, client_id, sequence }` | `{ ok, version }` or `{ redirect }` |
| `delete` | `{ key, if_version?, client_id, sequence }` | `{ ok, version }` or `{ redirect }` |

- `if_version` gives optional compare-and-set (CAS): the mutation applies only
  if the current version matches. Against an absent key any numeric
  `if_version` fails; the distinguished sentinel `ABSENT` succeeds only if the
  key is currently absent (create-only). `get` on an absent key returns
  `{ found: false }` with no version. Because versions are never reused,
  version-based ABA is impossible without tombstones. This is the single-key
  primitive that lets clients build safe read-modify-write without cross-key
  transactions.
- A mutation is identified by `(client_id, partition_id, sequence)`. Sequences
  are strictly increasing and serialized per client/partition; this keeps a
  fixed-size, durable idempotency record per client/partition without an unsafe
  TTL eviction (§8.4).

---

## 5. Partitioning and placement

### 5.1 Two-level mapping (key → partition → nodes)

We deliberately do **not** hash keys directly onto physical nodes. Instead:

```
key ──hash──► partition_id ──placement map──► [replica node set]
```

- **Fixed partition count `P`** (default `P = 128`), chosen once at cluster
  creation and never changed. `partition_id = xxhash64(key) % P`.
- **Placement is an explicit, replicated assignment**, not a hash ring. The
  meta group stores which `R` nodes host each partition (`R` = replication
  factor, e.g. 3). A simple greedy balancer proposes changes: when a node
  joins, it takes over an equal share of replica slots from the most-loaded
  nodes; when a node leaves, its slots go to the least-loaded nodes not
  already hosting that partition. Only affected partitions move — `O(R·P/N)`
  per membership change, the same movement bound a consistent-hash ring gives,
  with strictly better balance and no virtual-node machinery.

Why fixed partitions? Hashing keys directly to nodes would make every key its
own placement decision — impossible to replicate with Raft. A fixed `P` bounds
the number of Raft groups and makes placement a small explicit table. `P = 128`
with `R = 3` keeps per-node Raft-instance and RocksDB column-family counts low
(§6) while still giving ~1% rebalance granularity for clusters into the tens
of nodes. Larger clusters need a larger `P` chosen at creation; dynamic
splitting is future work (§14).

`R` and `P` are cluster constants stored in the meta group.

### 5.2 Placement map

The balancer computes a *desired* placement. The meta group stores the
*actual, committed* placement map:

```
partition_id → {
    active_voters:     [NodeId; R],  // last observed committed data-Raft voters
    learners:          [NodeId],     // nodes catching up (non-voting)
    state:             Stable
                     | Moving {
                         plan_id:       u64,          // fencing token = meta log
                                                      //   index of plan creation
                         expected_old:  [NodeId; R],  // voter set the plan assumed
                         target_voters: [NodeId; R],
                       },
    leader_hint:       NodeId,       // advisory only
}
```

Two states suffice: the data-Raft leader — not the meta group — is the
serialization point for the actual membership change (§7.1), so finer-grained
meta phases would be bookkeeping, not safety. The balancer *proposes*
placement; the map is the replicated control plan and routing aid. A rebalance
drives the observed data-Raft config toward the plan one safe membership
change at a time (§7). If the map and a partition Raft log disagree after a
crash, recovery first inspects the committed partition config and resumes or
repairs the fenced plan; it never treats stale metadata as permission to alter
or serve the data group.

---

## 6. Storage layer (RocksDB)

Each node runs a single RocksDB instance with **column families keyed by
partition**, so partitions are physically isolated (cheap to drop/ingest on
migration). With `P = 128` this is at most `3P + 1` CFs even on a node hosting
every partition — within RocksDB's comfortable range:

- `cf_data_<partition>` — the applied key-value state machine for that partition.
- `cf_raftlog_<partition>` — Raft log entries (openraft `RaftLogStorage`).
- `cf_meta_<partition>` — Raft hard state (term, vote), last-applied index,
  snapshot pointer, committed membership/config state, and local serving state.
- `cf_cluster` — a local cache of the meta-group state (rebuilt from the meta
  group on startup; not authoritative).

Implementation notes:
- We implement openraft's `RaftLogStorage` and `RaftStateMachine` traits over
  these CFs. Every voter makes a log append and associated term/vote state
  durable with `WriteOptions{ sync: true }` before replying success to the
  leader. The leader only counts those durable replies toward commit.
- **Snapshots** are produced as a manifest of immutable, checksummed SST files
  plus the exact last-applied `LogId` and membership/config state. Installation
  stages files in a unique directory, verifies every checksum, fsyncs the
  manifest and directory, then atomically installs it with the corresponding
  last-applied state. Log truncation is allowed only after that durable snapshot
  point. This is also the mechanism used for learner catch-up.
- Applying a committed entry (`put`/`delete`) and advancing `last_applied` occur
  in **one atomic `WriteBatch`**, so recovery never double-applies or skips.
- `delete` removes the key and its version record in the same atomic batch. No
  logical tombstone is kept: versions are log indexes and never repeat, so
  RocksDB deletion markers may compact freely without affecting CAS or
  absent-key semantics (§4.2).

---

## 7. Cluster membership changes (the hard part)

This section is the heart of the design: how the cluster stays correct and
available while nodes are **added** and **removed**. The invariant we protect:

> **A partition's data is never served from, nor acknowledged by, a replica set
> that has not committed that data via Raft majority.** Ownership transfer only
> completes after the new owners have caught up *inside* the Raft group.

### 7.1 The two membership layers must not be conflated

- **Placement change** (a node joins or leaves) changes the *desired* owner
  set of many partitions at once.
- **Raft membership change** (`openraft::Raft::change_membership`) changes the
  *actual* voter set of *one* partition, using joint consensus, and is only safe
  when the new members are caught up.

The rebalancer translates the first into a *fenced sequence* of the second. A
worker acts only while a linearizable meta-group read still names its `plan_id`,
expected old voter set, and target set — but that read is advisory: it can be
invalidated the moment it returns. The serialization point is the data-Raft
leader, which verifies that its currently committed config equals the plan's
expected old set *atomically with* proposing the membership change, and rejects
a proposal while another change is in progress. `plan_id` is the meta-group log
index of the plan-creation entry, so tokens are totally ordered and a leader
also rejects any plan older than the newest it has observed for that partition.
A stale worker therefore stops and reconciles instead of applying an obsolete
desired placement. We never just repoint routing at new nodes and start
serving — that would risk stale reads and lost writes.

### 7.2 Adding a node

Trigger: an operator (or autoscaler) calls `join(node_id, addr)` against the
meta group.

1. **Register the node.** Meta group commits the new node into the membership
   list; the balancer computes a new desired placement. This is a
   metadata-only commit; no data moves yet. The node starts empty.
2. **Prepare a fenced plan.** For each affected partition, a meta-group entry
   moves the partition to `Moving`, recording a fresh `plan_id`, the observed
   old voter set, and the target voter set. A subset `S` of partitions now want the new
   node as a replica (roughly `R·P/N` of them). The plan is durable before any
   data-group action, but it does not itself change routing authority.
3. **Add as learner first.** The current data-Raft leader rechecks the plan and
   its own committed config, then runs `add_learner(new_node)`. It streams log
   and/or a verified snapshot. The learner must durably reach the leader's
   current committed log point before promotion; a merely "nearby" match is not
   a completion condition. The learner cannot vote or serve client operations.
4. **Commit the data configuration.** The leader rechecks `plan_id` and uses
   Raft joint consensus to change exactly from the expected old set to the
   target voters. The reconfiguration is complete only when the final config is
   committed and applied by the data group. Normal writes remain governed by the
   joint configuration throughout; no separate metadata quorum is counted.
5. **Finalize metadata, then retire.** The leader reports the committed config
   `LogId` to the meta group. Only a matching `plan_id` may atomically update
   `active_voters` and return the partition to `Stable`. While a plan
   is active, the routing candidate set is `active_voters ∪ target_voters`; this
   bridges the unavoidable gap between the two independent Raft commits. After
   finalization, a removed node may reclaim `cf_*_<partition>` only after its
   local service gate has durably recorded that it is no longer a voter. A crash
   in any phase resumes by inspecting data Raft's committed membership first.

Throttling: rebalances run with a bounded concurrency (e.g. ≤ N partitions
migrating cluster-wide, ≤ 1 per partition) so snapshot streaming can't saturate
the network and starve foreground traffic. Correctness never depends on the
throttle; it only protects availability/latency.

### 7.3 Removing a node

Two flavors:

**(a) Graceful decommission** — `leave(node_id)` on the meta group:
1. Meta group marks the node `Draining`; the balancer produces a new desired
   placement and fenced plan for every partition the node hosted.
2. For each such partition, add the *replacement* node as a learner (§7.2 step
   3), let it catch up, then `change_membership` to swap the draining node out
   for the replacement. Leadership is transferred away first if the draining
   node was the leader (`openraft` leadership transfer).
3. When matching finalized plans show the node hosts no more voters or learners,
   meta group removes it from the membership list. The operator can then power
   it off. Its RocksDB can be deleted.

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
3. The meta group treats the down node like a decommission target: the balancer
   picks a replacement node (a least-loaded node not already a replica), the
   meta group creates a fenced replacement plan, adds it as a learner to each affected
   partition, catches it up from a surviving leader's verified snapshot/log,
   and promotes it via `change_membership` as in §7.2.
4. If the crashed node returns, it discovers (via the meta group + Raft term)
   that it has been replaced for some partitions; it marks those CFs non-serving
   before reclaiming them and, for any partition where it is still a member,
   rejoins Raft and catches up from its persisted log.

**Correctness boundary:** if *more* than a minority fail simultaneously (e.g. 2
of 3), that partition loses quorum and becomes **unavailable for writes and
linearizable reads** until enough replicas return. We deliberately choose
unavailability over serving possibly-stale data — this is the correctness-first
tradeoff. Operators can, as a last-resort manual action, invoke an unsafe
`force_recover(partition, surviving_replica)` that rebuilds a single-voter group
from one survivor (documented as data-loss-possible).

### 7.4 Serving gate

Routing metadata is advisory; serving authority comes only from the
partition's own Raft state:

- A write is accepted only by the current data-Raft leader and succeeds only
  after its current membership commits it durably.
- A linearizable read performs ReadIndex against the current membership and
  waits until the local state machine applies that index. A removed or
  partitioned-away old leader cannot pass this quorum check.
- A node that has applied its removal records a non-serving state before it
  reclaims local data. On startup it must establish membership through its
  partition Raft state before serving any client request.
- A response may carry a leader hint and candidate routes to speed retries.
  Stale client routing state is never a safety problem — a mis-routed request
  is redirected (§8.2), not served. This also preserves availability when the
  meta group is unreachable.

---

## 8. Request routing and the client

### 8.1 Client-side routing
The client library:
1. Obtains cluster state from **any** meta-group replica or any node's cached
   copy — no quorum read is needed, because routing data is advisory and the
   serving gate (§7.4) is the sole authority. The client caches candidate
   routes and refreshes them when it receives a redirect.
2. Computes `partition_id = hash(key) % P`.
3. Sends the request to `leader_hint`, then tries the advertised candidate set
   (`active_voters`, plus `target_voters` while a plan is active) on timeout or
   redirect. During a meta outage, cached candidates can still reach an
   unchanged, healthy data group.

### 8.2 Redirects
Routing hints are advisory. If a request reaches a non-leader, non-voter, or a
node that cannot pass the serving gate, it replies with `{ redirect:
leader_addr?, candidates }` rather than processing it. The client updates its
cache and retries candidates. This keeps clients
correct even with stale metadata: authority is checked by partition Raft, not by
the client cache.

### 8.3 Read path
`get` is linearizable via **ReadIndex** by default: the leader confirms a quorum
in its current membership — during joint consensus, in *both* the old and new
configurations, the same rule as commit — and waits until its state machine has
applied the returned index before answering. No log write is needed. There is
no lease-read fast path: ReadIndex is the only read path, which keeps the
design free of clock-drift assumptions entirely (§14).

### 8.4 Idempotent retries
Each mutation carries `(client_id, partition_id, sequence)`. The replicated
state machine persists, per client/partition, the highest *decided* sequence and
its result. Every decided outcome advances this record — including a CAS whose
`if_version` check fails; if a failed CAS did not advance `highest`, the
client's next sequence would be rejected as a gap and its stream would wedge.
`sequence == highest` returns the stored result without reapplying;
`sequence == highest + 1` is decided (applied, or recorded as a CAS failure);
lower or gapped sequences are rejected without mutation. Clients serialize mutations per client/partition and retain
the result until they observe a response. A client starts a new, unique
`client_id` only after abandoning its previous stream. This makes retries safe
across leader changes and rebalances without a bounded dedup cache silently
turning an old retry into a new write.

---

## 9. Failure detection and correctness details

### 9.1 Failure detection
- Each node sends periodic heartbeats to the meta group. (Per-partition Raft
  leaders already heartbeat their followers, so no separate peer-to-peer
  detection layer exists.) Missed heartbeats past `suspect_timeout` mark the
  node `Suspect`; after `down_timeout` the meta leader proposes `Down`, which
  takes effect only as a committed meta-group decision.
- A simple timeout suffices because a false `Down` is safe: it can only trigger
  the fenced, learner-first replacement of §7.2, which cannot bypass data-Raft
  quorum. The cost of a false positive is wasted data movement, bounded by the
  migration throttle — an availability/efficiency cost, not a correctness risk.
- Only the meta group may start a replacement plan, so all nodes act on one
  consistent plan — avoiding split-brain re-replication storms.

### 9.2 Split brain / network partitions
- A minority-side leader of any Raft group cannot commit (no majority) and
  cannot complete ReadIndex, so it stops serving linearizable reads. Clients on
  the minority side get errors/redirects, not stale data.
- The meta group itself is a Raft group; only its majority side can commit
  placement changes. A minority partition of the cluster cannot rebalance,
  which is correct.

### 9.3 Why data can't be lost or read stale during rebalance
- New replicas join as **learners** and receive data through Raft before they
  can vote → they never serve or ack data they don't have.
- Voter set changes go through **joint consensus** → there is never a moment when
  two disjoint majorities exist.
- **Raft quorum read checks** + term/config fencing → replaced or partitioned
  replicas cannot serve a linearizable read under stale ownership.
- Writes ack only after **durable majority commit** → an acknowledged write
  survives any minority failure in the stated crash-recovery model.

---

## 10. Transport (ZeroMQ)

### 10.1 Socket topology
- **Per node, one `ROUTER` socket** bound for inbound RPC (from clients and
  peers). ROUTER preserves peer identity for replies and multiplexes many peers.
- **Per node, `DEALER` sockets** for outbound RPC to peers (Raft
  append-entries, vote, snapshot, migration), one connection per peer, pooled.
- Clients use a `DEALER` per node connection, load-balanced by the client
  library's routing (not by ZMQ round-robin — routing is key-directed).
- Snapshot / bulk migration streams share the same `ROUTER/DEALER` channel;
  `snapshot_chunk` (4 MiB) framing bounds how long a transfer can
  head-of-line-block small Raft messages. A dedicated bulk channel on a second
  port is a future optimization, adopted only if measurement shows heartbeat
  starvation.

### 10.2 Message framing
- Envelope: `[ msg_type | partition_id | client_id? | sequence? |
  plan_id? | payload ]`. Raft RPCs additionally carry their Raft group and
  term/log identifiers; membership-changing control RPCs carry `plan_id`.
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
  placement.rs          // key → partition_id, greedy desired-placement calc
  meta/
    group.rs            // meta Raft group: membership + placement map
    rebalancer.rs       // desired vs actual diff → sequenced, fenced Raft changes
    failure.rs          // heartbeats, suspect/down state machine
  partition/
    node.rs             // one openraft instance per partition
    state_machine.rs    // apply put/delete, client sequence records, CAS
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
| `P` | partition count | 128 |
| `R` | replicas per partition | 3 |
| `suspect_timeout` | heartbeat miss → Suspect | 3 s |
| `down_timeout` | minimum suspicion period before a `Down` decision | 15 s |
| `max_concurrent_migrations` | cluster-wide rebalance throttle | N (node count) |
| `snapshot_chunk` | migration stream chunk size | 4 MiB |
| value size cap | max value bytes | 16 MiB |

---

## 13. Failure-scenario summary

| Scenario | Behavior |
|----------|----------|
| Leader of a partition crashes | Raft elects a new leader from survivors; clients retry candidates; no acknowledged data loss. |
| One replica of `R=3` fails | Partition stays available (2/3 quorum); meta group re-replicates to a new node. |
| Majority of a partition fails | Partition unavailable for writes + linearizable reads (correctness over availability) until quorum returns or manual `force_recover`. |
| Node added | Fenced plan → verified learner catch-up → joint-consensus promotion → metadata finalization; only `~R·P/N` partitions move. |
| Node gracefully removed | Replacement joins as learner and is promoted *before* old replica leaves; never drops below majority. |
| Network partition | Only the majority side of each Raft group serves; minority returns errors/redirects, never stale data. |
| Stale client cache | Client retries advertised candidates; partition Raft's serving gate, not the client cache, decides authority. |
| Duplicate/retried write | Durable client/partition sequence record returns the original result without reapplying. |

---

## 14. Open questions / future work

- **Load-aware placement:** current placement balances replica counts only; a
  future balancer could move partitions off hot/full nodes (the migration
  mechanism already exists; only the *decision* input changes).
- **Read replicas / follower reads** with bounded staleness for read-heavy
  workloads.
- **Multi-key transactions** via a 2PC coordinator layered over per-partition
  Raft groups.
- **Dynamic `P`** (partition splitting/merging) to avoid choosing `P` up front.
- **Compression / TTL** at the RocksDB layer.
- **Idempotency-record GC:** per-client/partition sequence records (§8.4) grow
  without bound as clients come and go. Needs a session mechanism whose expiry
  is a replicated Raft decision (à la ZooKeeper session leases), never a local
  TTL — evicting a live client's record reintroduces the duplicate-apply bug.
- **Lease-read fast path:** deliberately absent from v1 (§8.3). Reintroduce
  only with a full clock-drift/membership-transition/lease-invalidation proof,
  and only if ReadIndex latency is measured to be a problem.
- **Scaling `P`:** `P = 128` keeps per-node Raft-instance and column-family
  counts trivial, but caps practical cluster size and rebalance granularity.
  Very large clusters need a bigger `P` at creation plus multi-Raft
  optimizations (coalesced heartbeats, group quiescing) — deliberately out of
  scope for v1.
