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
8. **Group state is created in exactly two places:** cluster `init` (initial
   voters) and an explicit `BecomeLearner{plan_id}` control message from a
   data-Raft leader executing a plan. A node never lazily creates group state
   in response to vote/append RPCs (amnesia rule, DESIGN §7.4).

---

## 1. Toolchain and dependencies

Rust stable, edition 2021. Dependencies are deliberately few; each addition
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
  `CreatePlan`, `MarkAborting`, `FinalizePlan`, `AbortReport`). All
  serde-derived; envelope byte layout is deferred to M4.
- Config: `cluster_id`, `node_id`, control/bulk listen addrs, seed addrs,
  data dir, timeouts; `P` and `R` appear only in `init` input. Validation:
  `P > 0`, `R >= 3`, ≥ `R` distinct bootstrap nodes.
- Storage: open DB; create/drop `cf_log_<group>` / `cf_state_<group>` pairs;
  a node-local identity record `{cluster_id, node_id}` in the default CF —
  opening a data dir whose identity mismatches the config is a hard error.
- `batch.rs`: the single helper that writes state-machine mutations and
  `last_applied` in one `WriteBatch` with `sync: true`. All appliers use it.

**Gate:** config validation matrix; identity-mismatch rejection; fail-point
crash at every write boundary followed by reopen with invariants intact
(`last_applied` consistent, no partial batch visible).

### M2 — Data state machine, no Raft yet (DESIGN §4.2, §8.4, §12.2)

**Scope:** `partition/state_machine.rs` operating on `cf_state_<group>`
through M1's batch helper.

- Key record: one entry per key holding `(version, value)` — created,
  replaced, or deleted atomically (satisfies §6's "same atomic batch" rule
  with one record instead of two).
- Sequence record per `(client_id)` within the partition CF:
  `(highest_sequence, stored MutationResult)`.
- `apply(cmd, log_index)` order of operations: sequence check first
  (`== highest` → return stored result; `== highest + 1` → decide;
  else → deterministic rejection), then CAS evaluation against current state,
  then mutation + sequence-record advance + `last_applied`, all in one batch.
  Every decided outcome — including `ConditionFailed` and delete-of-absent —
  advances the sequence record.
- `delete` with `if_version: Absent` never reaches the state machine (API
  layer rejects pre-proposal, sequence not consumed); the SM treats it as a
  malformed entry error if it ever appears.

**Gate:** unit + property tests: retry returns stored result byte-identical;
failed CAS advances `highest` (the §8.4 wedge case); delete/recreate keeps
versions strictly increasing; numeric CAS against absent fails; `put(ABSENT)`
create-only; gapped/stale sequences rejected without mutation; crash between
any two applies recovers to a prefix.

### M3 — One Raft group (DESIGN §6, §7.4, §8.3, §12.3)

**Scope:** `partition/log_store.rs`, `partition/node.rs`,
`partition/snapshot.rs`, `ChannelNetwork`.

- `RaftLogStorage` over `cf_log_<group>`: append (sync before ack), vote/hard
  state, truncate, purge (only past durable snapshot point), committed
  membership recovery on open.
- `RaftStateMachine` wraps M2. Snapshot build: sorted scan of
  `cf_state_<group>` → `SstFileWriter` chunks + manifest (per-file checksums,
  last-applied `LogId`, membership). Install: stage in a unique dir, verify
  checksums, fsync manifest + dir, drop/create the CF, `ingest_external_file_cf`,
  then write `last_applied` — with a staging marker so a crash at any point
  either resumes the install or discards the stage, never serves a partial CF.
- `PartitionHandle` — the serving gate as an API: `write(cmd)` →
  `MutationResult` or `NotLeader{hint}`; `read(key)` → `ensure_linearizable()`
  then local get. There is no other path to the state machine.
- `ChannelNetwork` with per-link, seed-driven fault injection.

**Gate (all on ChannelNetwork, 3 voters):** leader crash/re-election with no
acknowledged loss; message loss/duplication/reorder; follower restart from
log; restart from snapshot + suffix log; snapshot install mid-stream crash;
ReadIndex never returns a stale value while a deposed leader is isolated.

### M4 — Transport and client (DESIGN §8, §10, §12.4)

**Scope:** `transport/{router,dealer,codec}.rs`, `api/{ops,client}.rs`,
`ZmqNetwork`.

- Envelope `[protocol_version | cluster_id | msg_type | group_id | request_id
  | payload]`; per-`msg_type` size limits enforced before decode. Reject:
  unsupported version, wrong cluster id, malformed group id, oversized frame,
  and `ClientOp` whose key does not hash to `group_id`.
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
  cluster record immutable; `CreatePlan` only if no existing plan, target has
  exactly `R` distinct voters differing from `voters` by one, eligible
  nodes ≥ `R`; `FinalizePlan` only with matching `plan_id` and exact target
  voter set; `MarkAborting` sets a one-way flag.
- `init`: commits `ClusterInit` (identity, `P`, `R`, protocol version, meta
  voters), then creates the `P` partition records and the initial data groups
  on their assigned nodes — data groups only after the bootstrap entry
  commits. `join` registers a node. Balancer (`placement.rs`) is a pure
  function `(directory, placement) -> Option<PlanProposal>`, deterministic
  with `NodeId` tie-breaks — trivially unit-testable.
- `MetaQuery` serves directory/placement/`P` as advisory follower reads;
  `status` renders it.

**Gate:** balancer property tests (near-even counts, single-voter diff,
determinism); plan-validation matrix inside the meta SM; full-cluster restart
recovers meta + data groups; meta-quorum loss while a configured data group
keeps serving reads and writes via cached routes.

### M6 — Rebalancing, failure handling, abort (DESIGN §5.2, §7, §9.1, §12.6)

**Scope:** `meta/rebalancer.rs`, `meta/failure.rs`, drain, serving-gate
records, CLI `leave`/`abort-plan`.

- Move driver (runs beside the meta leader; every step idempotent, crash
  points between all steps):
  1. balancer proposal → `CreatePlan` committed;
  2. ask the current data-Raft leader to execute: leader does a linearizable
     meta read of the record; accepts only if not `aborting`, its committed
     **voter set** equals `voters`, no change in progress, single-voter diff;
  3. leader sends `BecomeLearner{plan_id}` (the only lazy-state-creation
     path, rule 8), `add_learner`, streams snapshot/log on the bulk lane;
  4. promotion only at durable match with the leader's committed index;
     `change_membership(voters → target_voters)` via joint consensus;
  5. leader reports committed config; meta `FinalizePlan` swaps `voters`,
     clears `move`.
- Abort driver (§7.5): `MarkAborting` (operator command or `Down` planned
  learner) → current data leader makes a quorum-confirmed observation with no
  change in flight → `AbortReport{plan_id, voter_set}` → meta clears the plan
  (== `voters`), finalizes (== `target_voters`), or completes-then-finalizes
  (joint). Meta rejects reports for cleared plans.
- Startup reconciliation: for every local group, compare committed voter set
  against the meta record per §5.2 (learners ignored) and resume / complete /
  finalize / raise an operator-visible error.
- Failure detection: node heartbeats to the meta leader; `Suspect` after
  `suspect_timeout`, `Down` as a committed entry after `down_timeout`; `Down`
  triggers replacement plans, or `MarkAborting` when the down node is a
  planned learner; a down move-source just lets its plan finish.
- Drain: `leave` marks `Draining`, plans replace it everywhere, leadership
  transferred first, meta-voter removal precedes directory removal; the node
  reclaims CFs only after durably recording non-voter state.
- Throttle: `max_concurrent_migrations` in the driver; ≤ 1 per partition is
  structural (one plan).

**Gate (ChannelNetwork):** crash + restart after every numbered step resumes
to a consistent end state; planned learner dies mid-move → abort clears and a
replacement plan succeeds; abort racing a completing move finalizes benignly;
false `Down` causes movement but no linearizability violation; isolated old
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
  - per-key linearizability: single-register histories per key checked with a
    WGL-style checker (small, per-key histories keep this cheap);
  - exactly-once: every `(client_id, partition_id, sequence)` applied at most
    once across the whole run;
  - convergence: every finalized plan has meta `voters` == data-Raft
    committed voter set; no partition left with a non-`aborting` stale plan
    and a live target;
  - no acknowledged write lost across any minority of failures.
- Metrics via `status`: quorum health, applied/committed lag, learner lag,
  plan age, bulk-lane backlog. Counters only — no metrics framework.

**Gate = v1 acceptance:** a three-node cluster (growing to four and shrinking
back during the run) completes every DESIGN §14 scenario under fault
injection with all oracles green and `force_recover` never invoked.

---

## 3. Cross-cutting test strategy

- **Fail-points:** `fail` crate scenarios at each durable-write boundary
  (log append, batch apply, snapshot stage/install, meta apply, serving-gate
  record, CF drop). Each M-gate lists which points it must cover; M7 fuzzes
  across all of them.
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
  snapshot path early, before any multi-node work depends on it.
- openraft 0.9 API surface (`ensure_linearizable`, learner promotion
  semantics) → M3 pins the version and encodes assumptions as tests, so an
  upgrade that changes behavior fails loudly.
- ZMQ identity/reconnect quirks → confined to M4 by rule 3; consensus
  correctness never depends on them.
