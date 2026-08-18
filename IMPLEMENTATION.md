# DAL Service — Implementation Plan

Expands DESIGN.md §12 into concrete milestones. Priorities inherit from the
design: **correctness first, simplicity second**. Where this plan chooses
between a faithful mechanism and a clever one, it picks the one whose
correctness is easiest to see and to test.

---

## 0. Ground rules

These bind every milestone:

1. **All decisions at apply time.** Sequence checks, CAS evaluation, and plan
   validation execute deterministically inside the replicated state machine.
   The leader may pre-reject obviously invalid requests as an optimization,
   but the state machine result is authoritative and identical on every
   replica. No clocks, randomness, or node-local state inside `apply`.
2. **Real durability in tests.** Storage tests run against real RocksDB in a
   temp dir — never a mock. Crash tests use fail-points at every durable-write
   boundary (see §3) and re-open the real files.
3. **Two networks behind one trait.** openraft's `RaftNetwork` gets two
   implementations: `ChannelNetwork` (in-process, seeded fault injection:
   drop/duplicate/delay/partition per link) and `ZmqNetwork` (production).
   Every multi-node correctness test runs on `ChannelNetwork`; ZMQ is tested
   separately for transport concerns only. This keeps consensus tests fast,
   repeatable, and independent of socket behavior.
4. **No hand-rolled consensus.** Use openraft built-ins: `change_membership`
   (joint consensus), `add_learner`, `ensure_linearizable()` (ReadIndex),
   leadership transfer. We implement storage, network, and state machines —
   nothing that alters the protocol.
5. **Voter sets, not raw configs.** One function
   `voter_set(&Membership) -> BTreeSet<NodeId>` is the only way any code
   compares membership (DESIGN §5.2). Grep-enforceable.
6. **One binary.** `dal` with subcommands: `run`, `init`, `join`, `leave`,
   `abort-plan`, `status`. No separate tools, no feature flags, no config
   hot-reload.
7. **Gates are automated.** A milestone is done when its listed tests pass in
   CI; the next milestone does not start before that (DESIGN §12 ordering).
8. **Group state is created in exactly two ways:** a durable, byte-identical
   `BootstrapGroup` record during cluster `init`, or an explicit
   `BecomeLearner{group_id, plan_id}` control message from that group's
   incumbent Raft leader. `BecomeLearner` applies to data and meta groups; the
   receiver first verifies the live, non-aborting plan and its target through a
   linearizable meta read, then durably records the admission before creating
   CFs. Every admission — including a byte-identical retry — re-runs that
   verification, so a replay arriving after an abort or CF reclamation cannot
   resurrect the group; a conflicting or stale admission is rejected. A node
   never lazily creates group state in response to
   vote/append RPCs (amnesia rule, DESIGN §7.4).
9. **Control reports are not client operations.** `FinalizePlan` and
   `AbortReport` are submitted only by the internal, peer-control path after
   the reporting data leader has made the specified quorum-confirmed
   observation. Public meta APIs cannot propose either command. This is a
   protocol boundary (v1 still assumes a trusted network), not a claim that the
   meta state machine can atomically inspect a data-Raft log.

---

## 1. Toolchain and dependencies

Rust stable, edition 2024. Dependencies are deliberately few; each addition
needs a reason this table can hold:

| Crate | Purpose |
|-------|---------|
| `openraft` (0.9.x) | Raft protocol, joint consensus, ReadIndex |
| `rocksdb` | storage; `SstFileWriter` + `ingest_external_file_cf` for snapshots |
| `tokio` | async runtime |
| `zmq` | ROUTER/DEALER transport (poller thread bridges to Tokio) |
| `serde` + `bincode` | payload codec (no codegen step) |
| `xxhash-rust` | `partition_id = xxhash64(key) % P` |
| `bytes`, `thiserror` | plumbing |
| `tracing` | logging/metrics counters |
| dev: `proptest`, `fail` | property tests, crash-point injection |

Explicitly avoided: gRPC/protobuf stacks, actor frameworks, ORM-ish storage
layers, async-trait sugar beyond what openraft requires.

---

## 2. Milestones

### M1 — Types, configuration, storage (DESIGN §4, §6, §12.1)

**Scope:** `config.rs`, `types.rs`, `storage/rocks.rs`, `storage/batch.rs`.

- Core types: `NodeId = u64`, `ClusterId = u128` (random at `init`),
  `GroupId = Meta | Data(u16)`, `Version = u64` (Raft log index),
  `MutationResult = Applied { version } | ConditionFailed { current }`,
  `IfVersion = Number(u64) | Absent`, data command enum (`Put`, `Delete`),
  meta command enum (`ClusterInit`, `RegisterNode`, `SetNodeState`,
  `CreatePlan`, `MarkAborting`, `FinalizePlan`, `AbortReport`). Separate
  node-local control records are `BootstrapGroup`, a durable
  `LearnerAdmission`, and the pending-report journal (a leader's unconfirmed
  observations); the peer-control protocol carries a
  `DataConfigObservation { group_id, plan_id, voter_set, config_log_id }`.
  Placement records retain the last confirmed data config `LogId`. All are
  serde-derived; envelope byte layout is deferred to M4.
- Config: `cluster_id`, `node_id`, control/bulk listen addrs, seed addrs,
  data dir, timeouts; `P` and `R` appear only in `init` input. Validation:
  `P > 0`, `P <= u16::MAX`, `R >= 3`, ≥ `R` distinct bootstrap nodes, an
  odd meta-voter set of ≥ 3 distinct nodes, and a worst-case `P`/`R` routing
  snapshot that fits the 4 MiB `MetaQuery` frame. The directory is bounded to
  1,024 nodes with 256-byte endpoints. `ClusterInit` persists a canonical hash
  specification (`xxh64`, seed 0, raw key bytes) along with `P`; this is a
  protocol constant, not a crate-version default.
- Storage: open DB; create/drop `cf_log_<group>` / `cf_state_<group>` pairs;
  a node-local identity record `{cluster_id, node_id}` in the default CF —
  opening a data dir whose identity mismatches the config is a hard error.
- `batch.rs`: the single helper that writes state-machine mutations and
  `last_applied` in one `WriteBatch` with `sync: true`. All appliers use it.
  Snapshot-install journals live in the group's log CF; serving-gate records,
  `LearnerAdmission`, and the pending-report journal live in the node-local
  default CF, which survives both state-CF replacement and group-CF
  reclamation — a snapshot can never copy or erase local authority state.

**Gate:** config validation matrix (including `u16` partition bounds and hash
spec persistence); identity-mismatch rejection; fail-point crash at every
write boundary followed by reopen with invariants intact (`last_applied`
consistent, no partial batch visible).

### M2 — Data state machine, no Raft yet (DESIGN §4.2, §8.4, §12.2)

**Scope:** `partition/state_machine.rs` operating on `cf_state_<group>`
through M1's batch helper.

- Key record: one entry per key holding `(version, value)` — created,
  replaced, or deleted atomically (satisfies §6's "same atomic batch" rule
  with one record instead of two).
- Sequence record per `(client_id)` within the partition CF:
  `(highest_sequence, command_digest, stored MutationResult)`, where
  `command_digest` is a 128-bit xxh3 of the canonical command bytes (values
  reach 16 MiB, so raw bytes are not retained; accidental collision is
  negligible under the non-Byzantine model, and M7's oracle still checks true
  byte identity). A retry at `highest_sequence` succeeds only when its digest
  matches; reuse of an idempotency key for another command returns a stable
  `SequenceMismatch` error and never returns a result for the wrong request.
- `apply(cmd, log_index)` order of operations: sequence check first
  (`== highest` → return stored result; `== highest + 1` → decide;
  else → deterministic rejection), then CAS evaluation against current state,
  then mutation + sequence-record advance + `last_applied`, all in one batch.
  Every decided outcome — including `ConditionFailed` and delete-of-absent —
  advances the sequence record.
- `delete` with `if_version: Absent` never reaches the state machine (API
  layer rejects pre-proposal, sequence not consumed). Every committed entry,
  including a defensively handled malformed, stale, or gapped command, instead
  produces a deterministic non-mutation apply result and atomically advances
  `last_applied`; no apply error may wedge Raft progress. Only *decided*
  commands advance the client sequence record.

**Gate:** unit + property tests: retry returns stored result byte-identical;
failed CAS advances `highest` (the §8.4 wedge case); same-sequence,
different-command retries are rejected; delete/recreate keeps
versions strictly increasing; numeric CAS against absent fails; `put(ABSENT)`
create-only; malformed/gapped/stale committed entries advance only
`last_applied`; crash between any two applies recovers to a prefix.

### M3 — One Raft group (DESIGN §6, §7.4, §8.3, §12.3)

**Scope:** `partition/log_store.rs`, `partition/node.rs`, `snapshot.rs`,
`ChannelNetwork`.

- `RaftLogStorage` over `cf_log_<group>`: append (sync before ack), vote/hard
  state, truncate, purge (only past durable snapshot point), committed
  membership recovery on open.
- `RaftStateMachine` wraps M2. Snapshot build captures a point-in-time RocksDB
  view under the apply mutex, then releases the mutex and streams sorted records
  into a checksummed temporary file. Memory is bounded by one record, buffered
  file I/O, and OpenRaft's wire chunk. The snapshot contains replicated records
  only (keys, sequence records, and replicated metadata), never node-local
  serving/admission state. Install verifies the stream while filling a uniquely
  named inactive state CF in bounded batches. After its WAL is sync-durable, a
  sync-durable pointer in the default CF atomically activates the complete
  generation together with the search projection epoch. The previous generation
  is retained until outstanding readers drain. Recovery discards unreferenced
  generations, so the pointer always selects either the old or new complete
  state without a destructive partial-replace window.
- `PartitionHandle` — the serving gate as an API: `write(cmd)` →
  `MutationResult` or `NotLeader{hint}`; `read(key)` → `ensure_linearizable()`
  then local get. There is no other path to the state machine.
- `ChannelNetwork` with per-link, seed-driven fault injection.

**Gate (all on ChannelNetwork, 3 voters):** leader crash/re-election with no
acknowledged loss; message loss/duplication/reorder; follower restart from
log; restart from snapshot + suffix log; snapshot install mid-stream crash;
ReadIndex never returns a stale value while a deposed leader is isolated;
crash before and after snapshot-generation activation never exposes a partial
state machine; corrupt/truncated streams never activate.

### M4 — Transport and client (DESIGN §8, §10, §12.4)

**Scope:** `transport/{router,dealer,codec}.rs`, `api/{ops,client}.rs`,
`ZmqNetwork`.

- Envelope `[protocol_version | cluster_id | msg_type | group_id | request_id
  | payload]`; per-`msg_type` size limits enforced before decode. Reject:
  unsupported version, wrong cluster id, malformed group id, oversized frame,
  and `ClientOp` whose key does not hash to `group_id`.
- The codec has no generic "propose a meta command" frame. Public traffic may
  invoke only documented client/admin operations; `FinalizePlan`, `AbortReport`,
  learner admission, and Raft membership controls use a distinct peer-control
  dispatcher reachable only from configured node endpoints. The v1 trusted
  network assumption is explicit, but this separation prevents an ordinary
  client code path from bypassing the plan driver.
- One ROUTER (control) + one ROUTER (bulk: `RaftSnapshot`, `MigrationChunk`)
  per node; DEALER pools outbound; each ZMQ socket owned by one poller thread
  bridged to Tokio channels. Application-level timeout + retry; `request_id`
  correlation only.
- Client library: seeds → fetch `P`, node directory, placement (follower
  read); route cache with `leader_hint`; on timeout/redirect walk
  `voters ∪ target_voters`; idempotent retry via `(client_id, partition_id,
  sequence)`; reject mismatched cluster id in any response.

**Gate:** codec round-trip + limit fuzzing (proptest); wrong-cluster and
mis-partitioned-key rejection; stale-route redirect convergence; cold client
with one live seed; client retry across a leader change yields exactly-once
application (asserted via M2 sequence records).

### M5 — Meta group and bootstrap (DESIGN §3.1, §5, §12.5)

**Scope:** `meta/group.rs`, `placement.rs`, CLI subcommands `init`/`join`/
`status`.

- The meta group reuses M3's storage/runtime with `GroupId::Meta`. Its state
  machine applies the meta command enum with all validation inside `apply`:
  cluster record immutable; `CreatePlan` only if no existing plan — a
  data-group plan additionally requires at least `R` eligible nodes and its
  target has exactly `R` distinct voters differing from `voters` by one, while
  a meta-group target is a same-size
  single-voter replacement or a single-voter removal and never leaves fewer
  than 3 voters; `FinalizePlan` only with matching `plan_id`, group, exact
  target voter set, and a `DataConfigObservation` whose voter set exactly
  equals the planned target and whose config `LogId` is present;
  `AbortReport` carries the same plan/group/config-log-id binding and is
  additionally accepted only for a plan already marked `aborting` — a report
  for a healthy plan is rejected outright, so no spurious or replayed report
  can clear a live, non-aborting move; `MarkAborting` sets a one-way flag.
  The state machine validates tokens and state transitions, while the internal
  control handler is responsible for obtaining the cross-group quorum
  observation before it can submit a report.
- `init` is a resumable protocol, not a local shortcut: (1) obtain one
  immutable bootstrap descriptor (cluster identity, hash spec, meta voters,
  and group genesis configurations) — a retried `init` first asks every
  reachable bootstrap node for an existing durable descriptor and reuses it,
  deriving a fresh one (with its random `cluster_id`) only when none exists,
  so a partially seeded bootstrap never self-conflicts; (2) write
  `BootstrapGroup` and identity
  records with sync durability on every initial meta voter, accepting retries
  only when byte-identical; (3) the deterministic designated bootstrap node
  invokes the pinned OpenRaft initialization flow with the full pre-admitted
  meta voter configuration, then commits `ClusterInit`; (4) commit the initial
  partition-placement records; (5) issue byte-identical `BootstrapGroup`
  records to every initial data voter, then the deterministic designated voter
  initializes each data group with its recorded full configuration. Startup
  reconciliation resumes these durable phases; a conflicting init fails, and
  a data group is never initialized before its placement record commits.
  `join` registers a node. Balancer
  (`placement.rs`) is a pure function `(directory, placement) ->
  Option<PlanProposal>`, deterministic with `NodeId` tie-breaks — trivially
  unit-testable.
- Meta membership has an explicit policy and its own learner-first driver:
  choose eligible directory nodes, admit the meta learner through rule 8,
  wait for durable catch-up, use `change_membership`, and persist/reconcile
  its membership plan under a `GroupId::Meta` record with the meta-specific
  validation above (replacement or single-voter removal, never below 3
  voters), otherwise exactly as for a data group. `join` alone never changes
  meta
  voters; removal, replacement, and leadership transfer are explicit operator
  actions. This permits safe meta-voter replacement before a directory entry is
  removed.
- `MetaQuery` serves directory/placement/`P` as advisory follower reads;
  `status` renders it. Runtime endpoint/incarnation authority is separate:
  peer-control `DirectoryQuery` returns a directory value only after a meta
  ReadIndex barrier; follower directory data may resolve its leader hint but is
  never merged into an address book.

**Gate:** balancer property tests (near-even counts, single-voter diff,
determinism); plan-validation matrix inside the meta SM; crash/retry at every
bootstrap phase (including partial initial voter admission), byte-identical
init retry and conflicting-init rejection; meta learner add/replace/remove and
restart recovery; full-cluster restart recovers meta + data groups; meta-quorum
loss while a configured data group keeps serving reads and writes via cached
routes.

### M6 — Rebalancing, failure handling, abort (DESIGN §5.2, §7, §9.1, §12.6)

**Scope:** `meta/rebalancer.rs`, `meta/failure.rs`, drain, serving-gate
records, CLI `leave`/`abort-plan`.

- Move driver (runs beside the meta leader; every step idempotent, crash
  points between all steps):
  1. balancer proposal → `CreatePlan` committed;
  2. ask the current data-Raft leader to execute: leader does a linearizable
     meta read of the record; accepts only if not `aborting`, its committed
     **voter set** equals `voters`, no change in progress, single-voter diff;
  3. leader sends `BecomeLearner{group_id, plan_id}`. The target independently
     confirms, through a linearizable meta read, that this exact plan is live,
     non-aborting, and names it as target; it sync-durably records the
     admission, then creates the group. A byte-identical retry is accepted
     only after re-running the same meta verification — a replay arriving
     after an abort or CF reclamation is refused, not resurrected — and
     stale/conflicting admissions are refused. The leader then `add_learner`s
     it and streams snapshot/log on the bulk lane;
  4. promotion only at durable match with the leader's committed index;
     `change_membership(voters → target_voters)` via joint consensus;
  5. only after the final configuration is committed and applied, the current
     leader passes a ReadIndex barrier in that configuration and constructs a
     `DataConfigObservation { group_id, plan_id, voter_set: target_voters,
     config_log_id }`, persisted in its pending-report journal until meta
     confirms it (a rejection for an already-cleared plan counts as
     confirmation and stops the retry). It
     submits this through the internal peer-control path; meta `FinalizePlan`
     stores the confirmed config `LogId`, swaps `voters`, and clears `move`.
     No public API can manufacture this report.
- Abort driver (§7.5): `MarkAborting` (operator command or `Down` planned
  learner) → the current data leader, if it observes the joint config, first
  completes the membership change (only the data leader can); it then makes a
  quorum-confirmed observation with no change in flight and submits a
  journaled internal `AbortReport{plan_id, voter_set, config_log_id}` whose
  voter set is exactly `voters` (meta clears the plan) or exactly
  `target_voters` (meta finalizes) — a joint config is never reported, so a
  single voter set always suffices. Meta rejects reports for cleared plans
  and for plans not marked `aborting`. Delayed, duplicate, premature, and
  deposed-leader reports are idempotent or rejected by the plan's
  aborting/config-log-id state; none may clear a live, non-aborting move.
- Startup reconciliation: for every local group, compare committed voter set
  against the meta record per §5.2 (learners ignored) and resume / complete /
  finalize / raise an operator-visible error.
- Failure detection: node heartbeats to the meta leader carry a durable node
  incarnation and monotonically increasing heartbeat sequence. The replicated
  directory has an explicit transition table: heartbeats supply liveness
  evidence only; `Suspect`, `Down`, and reactivation are committed transitions,
  and a node declared `Down` may become eligible again only through an explicit
  rejoin/incarnation check. `Suspect` follows `suspect_timeout`, `Down` follows
  `down_timeout`; `Down` triggers replacement plans, or `MarkAborting` when
  the down node is a planned learner; a down move-source just lets its plan
  finish.
- Drain: `leave` marks `Draining`, plans replace it everywhere, leadership
  transferred first, meta-voter removal precedes directory removal; the node
  reclaims CFs only after durably recording non-voter state.
- Throttle: one data migration is allowed cluster-wide. The planner counts a
  healthy in-flight plan's target voters as anticipated load, and suppresses
  fresh work while an abort is unresolved.

**Gate (ChannelNetwork):** crash + restart after every numbered step resumes
to a consistent end state; planned learner dies mid-move → abort clears and a
replacement plan succeeds; abort racing a completing move finalizes benignly;
duplicate, delayed, or premature `BecomeLearner`, finalization, and abort
reports (including an abort report for a healthy plan, and replays after an
abort or CF reclamation) cannot resurrect a group or wrongly clear a plan;
meta-voter replacement/removal preserves meta quorum; stale heartbeats
cannot reactivate a `Down` node; false `Down` causes movement but no
linearizability violation; isolated old
leader cannot serve reads or finalize; removing the current leader; drain of
a node hosting leaders; returning crashed node refuses participation in
groups it was removed from.

### M7 — System verification (DESIGN §12.7)

**Scope:** test harness, oracles, minimal metrics.

- Harness: N in-process nodes on `ChannelNetwork`, seeded event generator
  (crash/restart, delay, reorder, duplicate, partition, join/leave/`Down`)
  with every run reproducible from its seed. Escalate to a deterministic
  scheduler (e.g. madsim) only if seed-replay proves insufficient to
  reproduce failures — not before.
- Oracles asserted continuously and at quiescence:
  - per-key linearizability: an executable single-register model checks every
    invocation/response interval, including CAS/version outcomes, redirects,
    retries, and operations left pending by a crash. The checker either finds a
    legal real-time-respecting order or emits the seed plus a minimized history;
    small per-key histories keep this cheap;
  - exactly-once: every `(client_id, partition_id, sequence)` applied at most
    once across the whole run, and every duplicate response is for byte-identical
    command bytes;
  - convergence: every finalized plan has meta `voters` == data-Raft
    committed voter set and its recorded config `LogId`; no partition left with
    a non-`aborting` stale plan and a live target;
  - no acknowledged write lost across any minority of failures.
- Metrics via `status`: quorum health, applied/committed lag, learner lag,
  plan age, bulk-lane backlog. Counters only — no metrics framework.

**Gate = v1 acceptance:** a three-node cluster (growing to four and shrinking
back during the run) completes every DESIGN §14 scenario under fault
injection with all oracles green and `force_recover` never invoked.

### M8 — Process assembly and operability (DESIGN §10–11, RUNTIME_ARCHITECTURE.md)

M1–M7 prove correctness on `ChannelNetwork` inside test harnesses; M8 assembles
those components into a runnable process over the production ZMQ transport. It
adds no consensus mechanism and relaxes no invariant — it wires existing pieces
and is gated on the same behaviors holding over real sockets.

**Scope:** `transport/raft_net.rs`, `runtime/{node,dispatch,http,rebalance,
config_file}.rs`, `main.rs`, plus the durable heartbeat incarnation in
`storage/rocks.rs`.

- **Production Raft network (rule 3's second impl).** `RaftPeerFactory<T>`
  (`transport/raft_net.rs`) is the `RaftNetworkFactory` that dials peers'
  `control_addr` (append/vote) and `bulk_addr` (snapshot) resolved from the meta
  directory over the DEALER pool. It replaces `ChannelNetworkFactory` at the
  `start_with_network` seam; consensus code is unchanged.
- **Registration and address fencing.** `Storage` fsyncs the process's exact
  `(cluster_id, node_id, control_addr, bulk_addr, directory_incarnation)`
  registration before serving. Address-book merges ignore lower incarnations
  and reject conflicting endpoints at an equal incarnation. An authoritative
  self-entry that differs from the startup binding closes a shared one-way
  process fence used by inbound dispatch, outbound Raft, heartbeats, failure
  detection, rebalance, and dynamically started groups.
- **`runtime::Node` assembly** (`runtime/node.rs`, startup sequence in
  RUNTIME_ARCHITECTURE.md): open storage → recover-or-bootstrap identity → start
  the meta group iff this node is a meta voter → start each hosted data partition
  into a shared, mutable `Arc<RwLock<HashMap<u16, PartitionNode>>>` registry →
  build routing + `ClientGateway` over that shared handle → assemble
  `RootDispatch` → bind control and bulk `ZmqServer`s → spawn background drivers.
  `Node::bootstrap` drives resumable genesis from the descriptor, querying the
  shared live address book rather than retaining descriptor-only meta endpoints.
  `Node::shutdown` first stops inbound acceptance, shuts down every Raft, then
  asynchronously drains already-dispatched handlers before storage is released.
- **`RootDispatch` — the peer/operator-control dispatcher** (`runtime/dispatch.rs`):
  the inbound `Server` that splits on `MsgType::is_peer_control()`. Client frames
  (`ClientOp`, `MetaQuery`) go to the `ClientGateway`; peer/operator frames are
  served here — `RaftAppend`/`RaftVote`/`RaftSnapshot`, `DataConfigObservation`,
  `JoinRequest`, `LeaveRequest`, `AbortPlanRequest`, `BootstrapStatus`,
  `PlacementQuery`, `DirectoryQuery`, `Heartbeat`, and `BecomeLearner` (which
  starts a new hosted partition through the rule-8 admission path). A client
  frame can never reach a peer-control handler (rule 9).
- **Background drivers** (`runtime/node.rs`, `runtime/rebalance.rs`): a heartbeat
  emitter (replicated directory incarnation + durable process incarnation +
  monotonic sequence, per §M6 failure detection); a failure detector that becomes
  active whenever the local node hosts meta and turns collected liveness evidence into
  `SetNodeState` transitions; and the rebalance/abort driver, which runs on *every*
  node so a partition's data-leader role can drive its own move and report
  observations even on a non-meta-voter node. New planning and reclamation are
  held behind a bootstrap readiness fence; the meta leader additionally confirms
  each designated genesis data voter has initialized before it creates a plan.
- **Heartbeat incarnation fencing** (`storage/rocks.rs`, `meta/failure.rs`): each
  heartbeat carries the fixed, durable registration incarnation selected at
  startup and a monotonic process incarnation allocated from storage once per
  start. It never adopts an incarnation from a later address-book refresh. The
  detector tracks both with the sequence, so restart may reset sequence to one
  while an old process or pre-rejoin identity cannot refresh current liveness.
- **Read-only HTTP admin plane** (`runtime/http.rs`, `M8_HTTP_STATUS_PLAN.md`):
  `GET /status` + `GET /health`, node-local and best-effort (never
  `ensure_linearizable`), a third inbound plane off the correctness path. Its
  listener is bound before storage opens or any task spawns, so a bad `http_addr`
  fails startup without leaking detached work; the serving task is aborted on
  shutdown with the other loops.
- **Config + binary** (`runtime/config_file.rs`, `main.rs`): JSON node config
  (`http_addr`, initial placements) and immutable init descriptor; `dal run
  --config <node> --cluster <init>` starts a node, drives genesis, and serves
  until SIGINT.

**Gate:** the `runtime_m8` suite on a real ZeroMQ `inproc://` three-node cluster
(production `RaftPeerFactory`, not `ChannelNetwork`): serves a client op end to
end; a drained node's partition migrates to a spare; an aborting plan whose target
dies rolls back to the original voters; `BecomeLearner` starts a hosted partition
idempotently; a non-meta-voter data leader drives its own move; the failure
detector progresses a silenced node to `Down`; `/status` reports the node-local
view. Plus: heartbeat incarnation persists and advances across restarts
(`storage_m1`), and an invalid `http_addr` fails before storage is opened
(`runtime::node` unit test).

**Operator CLI** (`runtime/admin.rs`, `main.rs`): `join`, `leave`, `abort-plan`,
and `status` are wired as thin ZMQ clients that send one typed control frame and
render the reply, discovering the live directory before falling back to the
immutable descriptor and following `NotLeader` hints to the meta leader. `join`/`leave`/
`abort-plan` submit to the meta group (`SubmitReply`); `status` reads any node's
cached routing (`MetaQuery`). There is no `init` subcommand — genesis is driven by
`dal run` from the `--cluster` descriptor.

**Partition-stop / CF reclamation** (`runtime/rebalance.rs`, `runtime/node.rs`,
`storage/rocks.rs`): a reclaim pass runs each rebalance tick. For every hosted
partition it reads the committed placement; once the move that removed this node
has resolved (no in-flight plan) and the committed `voters` exclude it, it calls
`PartitionStarter::reclaim_partition` — the inverse of `admit_learner`: unpublish
the group, shut down its Raft runtime, then `Storage::reclaim_group` records
`NonServing` durably *before* dropping the CFs (crash-safe, §7.4). The decision is
revalidated through a linearizable current meta member while holding the same
lifecycle lock as learner admission, so a newer plan cannot race with deletion.
The operation is idempotent. `Node::start` skips any
group whose durable serving gate is `NonServing`, so a reclaimed node never
re-hosts it on restart. A rolled-back plan leaves the node a voter, so it is not
reclaimed.

**Startup reconciliation** (`runtime/node.rs`): `Node::start` first starts the
genesis voters from the descriptor (needed for `Node::bootstrap`), then resumes
every partition still held on disk (`Storage::group_exists`) that has not been
durably reclaimed (`serving_state != NonServing`), via
`PartitionStarter::resume_partition` — so a partition *gained* through a
post-genesis rebalance is re-hosted after a restart. The decision is local and
deadlock-free (a synchronous meta read at startup can't work: the meta quorum
needs the control servers bound, which happens after data-partition start);
`resume_partition` writes no admission record, so `authorize_group_start`'s
existing bootstrap/admission record is what gates it (the amnesia rule). Steady-
state divergence from the meta record — a group this node no longer votes for —
is corrected by the driver's reclaim pass, so startup + reclaim together
reconcile the hosted set against the committed placement.

**Meta-voter membership drain** (`runtime/{node,rebalance,dispatch}.rs`,
`meta/node.rs`): a `leave` on a **non-leader** meta voter triggers a
size-preserving meta-voter *replacement*. The meta handle is shared and mutable
(`MetaHandle = Arc<RwLock<Option<Arc<MetaNode>>>>`) so a node can start or stop
hosting the meta group at runtime; `MetaStarter` is the meta-group analogue of
`PartitionStarter` (`admit_meta_learner`/`resume_meta`/`reclaim_meta`), and a
`BecomeLearner` frame addressed to the meta group routes to it. The rebalance
driver's meta-leader role creates a `CreatePlan{group: Meta}` (SM-validated as a
single-voter replacement, floor 3) for an ineligible non-leader meta voter, then
the meta leader drives its *own* membership change (`add_learner` →
`change_voters` → `FinalizePlan{Meta}`) — mirroring the data path via
`reconcile`/`gate` over `MetaNode::committed_voter_set`. The drained node reclaims
its meta group once a **current** meta voter reports (over the network) a resolved
meta placement excluding it — a removed voter's own meta view freezes at removal,
so it cannot read the finalize locally. `Node::start` skips a durably `NonServing`
meta group on restart and resumes a meta group gained by an earlier promotion.
**v1 limitation:** draining the current meta *leader* is not supported (it needs
Raft leadership transfer); the plan simply waits until leadership moves. Follow-
ups: leader drain and meta *removal* (shrink) drain.

---

## 3. Cross-cutting test strategy

- **Fail-points:** `fail` crate scenarios at each durable-write boundary
  (bootstrap marker/admission, log append, batch apply, every snapshot-journal
  transition and SST ingest, meta apply, pending-report journal, serving-gate
  record, CF drop). Each
  M-gate lists which points it must cover; M7 fuzzes across all of them.
- **Property tests:** codec round-trips, balancer invariants, state-machine
  sequence/CAS algebra.
- **No mocked durability, no mocked consensus:** fault injection lives in the
  network and process layers only, where the design says faults occur.

## 4. Explicitly deferred (from DESIGN §15, plus)

Dynamic `P`, load-aware placement, follower reads, multi-key transactions,
lease reads, idempotency-record GC, compression/TTL, coalesced heartbeats.
Also deferred: authentication/encryption on the wire — `cluster_id` guards
against misconfiguration, not adversaries; v1 assumes a trusted network
(consistent with the non-Byzantine fault model, but worth stating).

## 5. Known risks

- `rocksdb` crate coverage of SST export/ingest edge cases → M3 proves the
  snapshot path early, including recovery after the destructive CF-replace
  point, before any multi-node work depends on it.
- openraft 0.9 API surface (`ensure_linearizable`, learner promotion
  semantics, initialization, and meta/data membership transitions) → M3/M5
  pin the version and encode assumptions as tests, so an upgrade that changes
  behavior fails loudly.
- ZMQ identity/reconnect quirks → confined to M4 by rule 3; consensus
  correctness never depends on them.
