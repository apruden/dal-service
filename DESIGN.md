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
  groups: a client with a cached route can still reach the data group, which is
  the authority on whether it can serve (§7.4). A client with neither a working
  seed endpoint nor a cached route cannot discover a route during that outage.

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
cluster identity, `P`, `R`, the canonical key-hash specification, protocol
version, and the initial meta-group
voter set to the meta group's durable state machine before any data partition
is created. It then
creates each initial partition with an explicit durable voter configuration;
creation fails unless at least `R` eligible, distinct nodes exist and the
initial meta voter set is an odd set of at least three distinct nodes.
Creation is resumable: its durable bootstrap records are accepted on retry
only when byte-identical, and a conflicting re-initialization fails rather
than forking the cluster. The initial
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
| `get`    | `{ key }`                        | `{ found, value?, version? }`         |
| `put`    | `{ key, value, if_version?, client_id, sequence }` | `MutationResult` or `{ redirect }` |
| `delete` | `{ key, if_version?, client_id, sequence }` | `MutationResult` or `{ redirect }` |

- `MutationResult` is either `{ outcome: Applied, version }` or
  `{ outcome: ConditionFailed, current: Absent | Present { version } }`.
  A successful mutation, including an unconditional delete of an absent key,
  is a Raft log entry and returns that entry's index as `version`; the latter
  does not create a key record or tombstone.
- `if_version` gives optional compare-and-set (CAS): a numeric value applies
  only if the current key version matches. Against an absent key any numeric
  value fails. The distinguished sentinel `ABSENT` is accepted by `put` only
  and succeeds only if the key is absent (create-only). A `delete` carrying
  `ABSENT` is rejected as malformed before any Raft proposal and does not
  consume the client's `sequence`. `get` on an absent key returns
  `{ found: false }` with no version. Because versions are never reused, version-based ABA is impossible
  without tombstones. This is the single-key primitive that lets clients build
  safe read-modify-write without cross-key transactions.
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
  factor, e.g. 3). A deterministic greedy balancer proposes single-voter
  replacements: choose an overloaded source and an eligible, non-replica
  destination, breaking ties by `NodeId`. A partition never has more than one
  move in flight; moves for distinct partitions may run concurrently up to the
  migration throttle (§7.2). This gives near-even replica counts (within one slot when feasible),
  moves only `O(R·P/N)` slots for one join/leave, and needs no virtual-node
  machinery. The balancer validates `eligible_nodes >= R` and every target has
  exactly `R` distinct voters before committing a plan.

Why fixed partitions? Hashing keys directly to nodes would make every key its
own placement decision — impossible to replicate with Raft. A fixed `P` bounds
the number of Raft groups and makes placement a small explicit table. `P = 128`
with `R = 3` keeps per-node Raft-instance and RocksDB column-family counts low
(§6) while still giving ~1% rebalance granularity for clusters into the tens
of nodes. Larger clusters need a larger `P` chosen at creation; dynamic
splitting is future work (§15).

`R` and `P` are cluster constants stored in the meta group.

### 5.2 Placement map

The meta group stores the last confirmed data-Raft voter set and, at most, one
immutable move plan per partition:

```
partition_id → {
    voters: [NodeId; R],             // last confirmed committed data-Raft voters
    voters_log_id: LogId,            // data-Raft log id that committed `voters`
    move: Option<{
        plan_id: u64,                // meta log index of plan creation
        target_voters: [NodeId; R],  // differs from voters by one node
        aborting: bool,              // set once; bars start/resume (§7.5)
    }>,
}
```

The planned learner is `target_voters - voters`; leader hints belong in client
caches, not replicated metadata. While `move` is present it is never replaced:
later balancing work waits. A stuck plan may be marked `aborting`, but only
the fenced report of §7.5 clears it. This makes the token stable between
plan creation and resolution, avoiding an impossible attempt to atomically
compare two independent Raft logs. The data-Raft leader serializes the actual
change (§7.1); metadata is a durable work queue and routing aid. Config
comparisons here and throughout §7 are over **voter sets** — learners are
ignored, so a committed `add_learner` entry still compares equal to `voters`
and a crash at the learner stage resumes the plan rather than raising an
error. On recovery,
inspect the partition's committed config first: if it equals `voters`, resume
the plan (or confirm its abort if it is marked `aborting`, §7.5); if it is
the joint configuration for `voters` and `target_voters`,
complete that change; if it equals `target_voters`, finalize it; otherwise stop
and raise an operator-visible reconciliation error. Metadata never authorizes
serving. The meta group's own membership changes are tracked by an analogous
record keyed `meta`; its target may be a same-size single-voter replacement
or a single-voter removal, and never leaves fewer than three voters.

---

## 6. Storage layer (RocksDB)

Each node runs a single RocksDB instance with **two column families per Raft
group**. The meta group uses group id `meta`; data groups use their partition
id. This keeps a group physically isolated (cheap to drop/ingest on migration)
without a third metadata CF: at most `2(P + 1)` CFs for `P` data groups plus the
meta group.

- `cf_log_<group>` — Raft log entries, vote/hard state, committed membership,
  and snapshot metadata (openraft `RaftLogStorage`).
- `cf_state_<group>` — the state machine: key/value/version records and client
  sequence records for a data group; cluster identity, node directory,
  placement records, and plans for the meta group. It also stores
  `last_applied`.
- Node-local authority state — the node identity record, serving-gate and
  learner-admission records, and the leader's pending-report journal — lives
  in the default CF, never in a group's CFs: snapshot installation replaces
  `cf_state_<group>` wholesale and reclamation drops both group CFs, so
  records that gate serving or participation must survive both.

Implementation notes:
- We implement openraft's `RaftLogStorage` and `RaftStateMachine` traits over
  these CFs. Every voter makes a log append and associated term/vote state
  durable with `WriteOptions{ sync: true }` before replying success to the
  leader. The leader only counts those durable replies toward commit.
- **Snapshots** are produced as a manifest of immutable, checksummed SST files
  plus the exact last-applied `LogId` and membership/config state. Installation
  stages files in a unique directory, verifies every checksum, fsyncs the
  manifest and directory, then atomically installs it with the corresponding
  last-applied state. Install progress is tracked in a sync-durable journal in
  `cf_log_<group>`, so a crash at any point either resumes the install from
  the verified stage or discards it — a partially installed state machine is
  never served. Log truncation is allowed only after that durable snapshot
  point. This is also the mechanism used for learner catch-up.
- Applying a committed entry (`put`/`delete`) and advancing `last_applied` occur
  in **one atomic `WriteBatch`** against `cf_state_<group>`, so recovery never
  double-applies or skips. The same rule applies to all meta-state changes.
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

The rebalancer translates the first into a *fenced sequence* of the second. The
meta group creates a plan only when the partition record has no existing plan;
it does not supersede it. Before it starts or resumes a move, the data-Raft
leader performs a linearizable meta read of that record; a newly elected
leader repeats this check itself before continuing a predecessor's move. It
accepts the plan only when it is not marked `aborting` (§7.5), its current
committed config equals the record's `voters`, it has no membership change in
progress, and the target replaces exactly one voter.
`plan_id` is the meta log index of creation and is included in every
finalization request. The data-Raft leader is the serialization point for the
membership change; the two Raft groups are not and cannot be made atomic. A
crash is reconciled from the data group's committed config as specified in
§5.2. We never just repoint routing at new nodes and start serving — that would
risk stale reads and lost writes.

### 7.2 Adding a node

Trigger: an operator (or autoscaler) calls `join(node_id, addr)` against the
meta group.

1. **Register the node.** Meta group commits the new node into the membership
   list; the balancer computes a new desired placement. This is a
   metadata-only commit; no data moves yet. The node starts empty.
2. **Prepare one fenced plan.** For an affected stable partition, a meta-group
   entry records its current `voters`, a fresh `plan_id`, and a target that
   replaces exactly one voter with the new node. Further moves for that
   partition wait until the plan is finalized or aborted (§7.5). The plan is
   durable before any data-group
   action, but it does not itself change routing authority.
3. **Add as learner first.** The current data-Raft leader rechecks the plan and
   its own committed config, then runs `add_learner(new_node)`. It streams log
   and/or a verified snapshot. The learner must durably reach the leader's
   current committed log point before promotion; a merely "nearby" match is not
   a completion condition. The learner cannot vote or serve client operations.
4. **Commit the data configuration.** The leader uses Raft joint consensus to
   change from the record's `voters` to `target_voters`. The reconfiguration is
   complete only when the final config is committed and applied by the data
   group. Normal writes remain governed by the joint configuration throughout;
   no separate metadata quorum is counted.
5. **Finalize metadata, then retire.** The leader reports the committed config
   `LogId` to the meta group. Only a matching `plan_id` and exact target config
   may atomically replace `voters` and clear `move`. While a plan is active,
   the routing candidate set is `voters ∪ target_voters`; this bridges the
   unavoidable gap between the two independent Raft commits. After
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
3. When the node appears in no partition record's `voters` or `move.target_voters`,
   meta group removes it from the membership list. If the node is also a
   meta-group voter, an operator-driven, learner-first meta-Raft membership
   change (§3.1) must remove it from the meta voter set before this step. The
   operator can then power it off. Its RocksDB can be deleted.

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
   and promotes it via `change_membership` as in §7.2. If the down node is the
   planned learner of an in-flight move, the meta group instead marks that
   plan `aborting` (§7.5); a replacement plan may be created only after the
   abort resolves. If the down node is the voter the in-flight move is already
   replacing, the existing plan is the replacement — it is left to finish.
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
- A node holding no local Raft state for a group refuses that group's vote and
  append RPCs rather than lazily creating empty state; it re-enters only
  through the learner-add path of a fenced plan. This is what makes CF
  reclamation safe: after the drop, the absence of local state itself enforces
  non-participation, so an amnesiac ex-voter can never help elect a leader
  that lacks committed entries.
- A response may carry a leader hint and candidate routes to speed retries.
  Stale client routing state is never a safety problem — a mis-routed request
  is redirected (§8.2), not served. This also preserves availability when the
  meta group is unreachable.

### 7.5 Aborting a stuck plan

A plan whose planned learner dies before promotion would otherwise block that
partition's rebalancing — and any drain of its current voters — forever, since
plans are never replaced. The escape is a fenced abort that keeps the
data-Raft leader as the serialization point:

1. The meta group marks the plan `aborting` (by operator command, or by
   failure handling when the planned learner is `Down`). From this point no
   leader passes the §7.1 gate for this plan, so the move can no longer start
   or resume. The plan is not yet cleared.
2. The current data-Raft leader, observing `aborting`, makes a
   quorum-confirmed (ReadIndex-style) observation of its own committed config
   with no membership change in flight and reports it with `plan_id`:
   - config equals `voters` → the meta group atomically clears `move`; the
     leader removes any learner the plan added. The abort succeeded.
   - config equals `target_voters` → the move actually completed; finalize
     normally (§7.2 step 5). The abort arrived too late, benignly.
   - joint configuration → the leader completes the membership change first,
     then finalizes. The report itself always carries a single voter set —
     exactly `voters` or exactly `target_voters`; a joint configuration is
     never reported.

The meta group accepts an abort report only for a plan still present and
marked `aborting`; a report for a healthy plan is rejected outright and can
never clear it.

This is safe without comparing the two Raft logs atomically: a deposed leader
cannot produce the report because its quorum confirmation fails, the reporting
leader itself refuses the plan after confirming, and any newer leader must
re-pass the §7.1 gate, which refuses an aborting plan. A plan cleared as
aborted therefore can never be completed afterward.

---

## 8. Request routing and the client

### 8.1 Client-side routing
Each client is configured with the immutable cluster id and one or more
meta-group seed addresses; it learns `P` from the cluster rather than local
config, so a wrong partition count cannot be configured. The client library:
1. Obtains `P`, the node directory, and the placement map from any reachable
   meta-group replica. This is a follower/cached read, not a quorum read, because routing
   is advisory and the serving gate (§7.4) is the sole authority. The client
   caches candidate routes and refreshes them after a redirect. A cold client
   retries its seed addresses until one is reachable.
2. Computes `partition_id = hash(key) % P`.
3. Sends the request to its cached `leader_hint`, then tries the advertised candidate set
   (`voters`, plus `target_voters` while a move is active) on timeout or
   redirect. During a meta outage, cached candidates can still reach an
   unchanged, healthy data group.

### 8.2 Redirects
Routing hints are advisory. If a request reaches a non-leader, non-voter, or a
node that cannot pass the serving gate, it replies with `{ redirect:
leader_addr?, candidates }` rather than processing it. The client updates its
cache and retries candidates. The response includes the cluster id and the
node addresses known to the responder; a client rejects a mismatched cluster
id. This keeps clients correct even with stale metadata: authority is checked
by partition Raft, not by the client cache.

### 8.3 Read path
`get` is linearizable via **ReadIndex** by default: the leader confirms a quorum
in its current membership — during joint consensus, in *both* the old and new
configurations, the same rule as commit — and waits until its state machine has
applied the returned index before answering. No log write is needed. There is
no lease-read fast path: ReadIndex is the only read path, which keeps the
design free of clock-drift assumptions entirely (§15).

### 8.4 Idempotent retries
Each mutation carries `(client_id, partition_id, sequence)`. The replicated
state machine persists, per client/partition, the highest *decided* sequence,
a digest of the command's canonical bytes, and its result; a retry returns
the stored result only when its digest matches, so reusing a sequence for a
different command yields a stable error, never the other command's result.
Every decided outcome advances this record — including a CAS whose
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
  detection layer exists.) Heartbeats carry a durable node incarnation and a
  monotonically increasing sequence and are liveness evidence only; `Suspect`,
  `Down`, and reactivation are committed directory transitions. Missed
  heartbeats past `suspect_timeout` mark the node `Suspect`; after
  `down_timeout` the meta leader proposes `Down`, which takes effect only as a
  committed meta-group decision. A `Down` node becomes eligible again only
  through an explicit rejoin that passes an incarnation check — a stale or
  replayed heartbeat can never reactivate it.
- A simple timeout suffices because a false `Down` is safe: it can only trigger
  the fenced, learner-first replacement of §7.2, which cannot bypass data-Raft
  quorum. A false positive can cause needless data movement and temporarily
  reduce availability or fault tolerance while the move runs; it is not a
  linearizability or acknowledged-data-loss risk. The migration throttle bounds
  its blast radius.
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
- Snapshot / bulk migration streams use a dedicated `ROUTER/DEALER` pair on a
  second port. Control traffic (Raft append/vote/heartbeat and client traffic)
  never shares its outbound queue with a snapshot, so a bulk transfer cannot
  starve elections. The bulk lane is still rate-limited by the migration
  throttle.

### 10.2 Message framing
- Envelope: `[ protocol_version | cluster_id | msg_type | group_id |
  request_id | payload ]`. `group_id` is `meta` or a partition id;
  `request_id` correlates replies but has no correctness role. Client mutation
  payloads carry `client_id` and `sequence`; membership-changing control
  payloads carry `plan_id`; Raft RPCs carry the Raft term/log identifiers.
- Receivers reject an unsupported protocol version, wrong cluster id, malformed
  group id, or a message larger than its configured per-type limit before it
  reaches Raft or the state machine. A `ClientOp` whose key does not hash to
  the envelope's `group_id` is also rejected — a client with a wrong partition
  count must get an error, not a successful write into the wrong partition.
- Payloads serialized with a compact binary codec (e.g. `bincode`/`prost`).
- `msg_type` covers: `ClientOp`, `RaftAppend`, `RaftVote`, `RaftSnapshot`,
  `MigrationChunk`, `MetaQuery`, `Redirect`, `Heartbeat`, plus the
  peer-control types `BecomeLearner` and `DataConfigObservation`
  (finalization/abort reports). Peer-control types are dispatched separately
  from client operations; there is no generic "propose a meta command"
  message, so no client code path can submit `FinalizePlan` or `AbortReport`
  (§7.5).

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
  config.rs             // cluster id, P, R, timeouts, paths, seed addresses
  transport/
    mod.rs
    router.rs           // inbound ROUTER loop, dispatch by msg_type
    dealer.rs           // outbound DEALER pool, retries, timeouts
    codec.rs            // envelope framing + (de)serialization
  placement.rs          // key → partition_id, deterministic one-step balancer
  meta/
    group.rs            // meta Raft group: membership + placement map
    rebalancer.rs       // durable one-step plans → fenced Raft changes
    failure.rs          // heartbeats, suspect/down state machine
  partition/
    node.rs             // one openraft instance per partition
    state_machine.rs    // apply put/delete, client sequence records, CAS
    log_store.rs        // openraft RaftLogStorage over cf_log_<group>
    snapshot.rs         // checkpoint/SST export + install (== migration)
  storage/
    rocks.rs            // RocksDB handle, per-group CF lifecycle (create/drop)
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

## 12. Implementation plan

Build in the following order. A later phase is not started until the preceding
phase's listed tests pass; this keeps the control plane from masking a broken
single-partition implementation.

1. **Types, configuration, and storage.** Define wire types, `NodeId`,
   `ClusterId`, `GroupId`, log entries, `MutationResult`, and validated static
   configuration. Require `P > 0`, `P <= u16::MAX`, `R >= 3`, at least `R`
   bootstrap nodes, and an odd meta-voter set of at least three.
   Implement `cf_log_<group>` / `cf_state_<group>` creation, crash-safe
   `WriteBatch` application, and restart recovery. Test torn-process recovery
   at every write boundary and reject a mismatched cluster id.
2. **One local data state machine.** Implement `put`, `delete`, numeric CAS,
   create-only `put(ABSENT)`, and durable client sequence records. Apply every
   mutation request through the state machine, including condition failures and
   absent deletes. Unit-test retries, lost responses, CAS failure sequencing,
   delete/recreate, and ABA prevention.
3. **One Raft group.** Implement the OpenRaft network and storage traits, then
   bootstrap a single three-voter group. Expose write forwarding and ReadIndex
   reads only through the serving gate. Test leader changes, message loss and
   duplication, follower restart, snapshot install, and recovery from a
   snapshot plus a suffix log.
4. **Transport and client.** Add versioned, cluster-scoped control and bulk
   sockets, bounded codecs, timeouts, and retry correlation. Build seed-based
   discovery, route caching, redirects, and idempotent mutation retry. Test
   stale routes, wrong-cluster messages, mis-partitioned keys, and a cold
   client with only one live seed.
5. **Meta group and bootstrap.** Reuse the same Raft storage/runtime for group
   `meta`; implement the node directory, immutable cluster record, placement
   records, and operator-only `init`/`join` commands. Create data groups only
   after the meta bootstrap entry commits. Test a full-cluster restart and
   meta-quorum loss while an already-configured data group continues to serve.
6. **Rebalancing and failure handling.** Implement exactly one durable,
   single-replacement plan per partition: add learner, verify its committed
   match index, change membership, and finalize or abort the plan (§7.5).
   Reconcile plans after each restart from data-Raft membership. Add
   heartbeats, `Suspect`/`Down`, graceful drain, throttling, and explicit
   operator status. Test crashes after every step, false `Down`, a planned
   learner that dies mid-move (fenced abort), old-leader isolation, and
   removal of the current leader.
7. **System verification.** Run deterministic simulated-network tests and
   fault-injection tests with random crash/restart, delay, reorder, duplicate,
   and partition events. Assert linearizability of each partition, no duplicate
   mutation application, and that every completed move has the meta voter set
   equal to the data-Raft committed configuration. Add metrics for quorum
   health, applied/committed lag, learner lag, plan age, and bulk-lane backlog.
8. **Process assembly and operability.** Assemble the phase 1–7 components into
   a runnable process: the production ZeroMQ `RaftNetwork`, the `runtime::Node`
   startup/shutdown lifecycle, the peer/operator-control dispatcher, the
   background drivers (heartbeat, failure detection, rebalance/abort), a
   durable heartbeat incarnation, a read-only HTTP `/status` plane, and the
   `dal run` binary. Adds no protocol; gated on the phase-6 behaviors holding
   over real sockets (a ZeroMQ three-node cluster serves ops, migrates on
   drain, rolls back a doomed plan, and marks a silent node `Down`).

The v1 *correctness* acceptance gate is phase 7: a three-node cluster completing
the scenarios in §14 under fault injection, with no unsafe recovery command
exercised. Phase 8 is the operable-binary layer on top; it inherits the same
invariants over the production transport (see IMPLEMENTATION.md §2 M8 for its
gate and the operator-CLI / partition-teardown items still open).

---

## 13. Key parameters (defaults)

| Param | Meaning | Default |
|-------|---------|---------|
| `P` | partition count | 128 |
| `R` | replicas per partition | 3 |
| `suspect_timeout` | heartbeat miss → Suspect | 3 s |
| `down_timeout` | minimum suspicion period before a `Down` decision | 15 s |
| `max_concurrent_migrations` | cluster-wide rebalance throttle | N (node count) |
| `snapshot_chunk` | migration stream chunk size | 4 MiB |
| control/bulk ports | independent ZMQ endpoints per node | configured |
| value size cap | max value bytes | 16 MiB |

---

## 14. Failure-scenario summary

| Scenario | Behavior |
|----------|----------|
| Leader of a partition crashes | Raft elects a new leader from survivors; clients retry candidates; no acknowledged data loss. |
| One replica of `R=3` fails | Partition stays available (2/3 quorum); meta group re-replicates to a new node. |
| Majority of a partition fails | Partition unavailable for writes + linearizable reads (correctness over availability) until quorum returns or manual `force_recover`. |
| Node added | Fenced plan → verified learner catch-up → joint-consensus promotion → metadata finalization; only `~R·P/N` partitions move. |
| Planned learner dies mid-move | Plan marked `aborting`; the data leader's quorum-confirmed report clears or finalizes it (§7.5), then a fresh plan replaces the dead node. |
| Node gracefully removed | Replacement joins as learner and is promoted *before* old replica leaves; never drops below majority. |
| Network partition | Only the majority side of each Raft group serves; minority returns errors/redirects, never stale data. |
| Stale client cache | Client retries advertised candidates; partition Raft's serving gate, not the client cache, decides authority. |
| Duplicate/retried write | Durable client/partition sequence record returns the original result without reapplying. |

---

## 15. Open questions / future work

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
