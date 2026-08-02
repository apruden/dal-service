# Async Materialized State Implementation Plan

Date: 2026-08-02

Status: implementation in progress — opt-in core and durability fences landed;
process-kill crash matrix and follower recovery-fence RPC remain rollout gates

## Implementation progress (2026-08-02)

Implemented behind `DAL_ASYNC_MATERIALIZED_STATE=1`:

- per-group visible/durable applied watermarks and bounded pending accounting;
- data-state completion after RocksDB visibility, with meta state still waiting
  for WAL durability;
- database-wide fail-closed propagation on WAL write or flush failure;
- durable-prefix fences for snapshot build and log purge;
- close-before-drain fencing for group reclamation;
- process-epoch stale-read fencing, leader ReadIndex opening, and
  snapshot-install recovery integration;
- `/status` visibility for the materialized visible/durable indices, pending
  entries/bytes, recovery readiness, and failure state;
- focused ordering tests plus the existing all-target/all-feature suite in both
  default and selected async-state configurations.

Still required before enabling by default:

- the leader-fenced follower recovery RPC described in Phase 4, so a caught-up
  restarted follower can reopen stale reads without waiting for a new applied
  entry;
- process-kill tests that deliberately lose the `A-D` suffix, the full seeded
  crash matrix, and mixed-version capability negotiation;
- canary measurements and rollout gates.

## Objective

Remove the RocksDB state-machine WAL flush from the client-write critical path
without weakening Raft's durable-majority guarantee.

The Raft log remains the durable source of truth. Applying a committed entry
must make its materialized state immediately visible and produce the client
result before returning to OpenRaft, but the state WAL may become durable
shortly afterward. If the node loses that not-yet-durable state, it reconstructs
the exact state and idempotency results from retained committed log entries.

This plan applies to data groups first. The meta group remains synchronously
durable until the data-group implementation has passed the crash, replay,
snapshot, membership, and rolling-upgrade gates in this document.

## Non-goals

- Do not relax Raft log, vote, or snapshot-install durability.
- Do not acknowledge a log append to a leader before its WAL flush completes.
- Do not introduce `ONE` consistency or asynchronous Raft replication.
- Do not let state writes reorder, become partially visible, or run ahead of
  committed Raft entries.
- Do not make raw RocksDB WAL records the replication protocol.
- Do not enable asynchronous meta-group state in the initial rollout.
- Do not tune a longer state-flush interval until the correctness design is
  complete and dirty-state memory/log-retention limits are enforced.

## Decision summary

The target data-group write path is:

```text
durably append and replicate Raft entry
    -> establish majority commit
    -> deterministically evaluate committed entry
    -> atomically write business state + sequence record + applied record
       to RocksDB with sync=false
    -> wait only until that WriteBatch is visible/readable
    -> return the apply result to OpenRaft and the client
    -> shared durability worker flushes the RocksDB WAL
    -> advance the per-group durable-applied watermark
```

The optimization removes only the final wait. It does not move logical state
application into an unordered background task: the next apply and every read
must observe the preceding WriteBatch before `apply()` returns.

The final fast mode keeps the existing applied-aligned committed record as a
recovery hint, not as a promise that every previously acknowledged commit is
immediately known after restart. A restarted group is therefore fenced from
serving stale/local reads until a current leader has performed a quorum read
barrier and the replica has visibly applied through the returned index.

## State model and invariants

For each Raft group, define:

- `L`: last locally durable Raft log entry;
- `C`: greatest cluster-committed entry present in this replica's local log,
  whether or not this restarted process currently remembers that it committed;
- `A`: greatest entry atomically applied and visible in the live state machine;
- `D`: greatest entry known to be covered by a successful state-WAL flush;
- `P`: greatest locally purged Raft log entry;
- `S`: last applied index represented by a snapshot being advertised or
  installed.

The implementation must maintain:

```text
P <= D <= A <= C <= L
```

The cluster-wide commit can be ahead of a lagging replica's `L`; `C` names only
the committed prefix present locally. A follower can also have an uncommitted
suffix beyond `C`; it must never apply that suffix.

The following invariants are release blockers:

1. **Durable log authority.** A successful client reply is backed by a Raft
   entry durably stored on a quorum. State-WAL durability is not counted toward
   that quorum.
2. **Atomic visible apply.** Business mutations, the per-client sequence/result
   record, applied `LogId`, membership, and the applied-aligned recovery hint
   are one cross-column-family RocksDB `WriteBatch`.
3. **Ordered visibility.** `apply(N)` does not return until the batch is
   readable, and `apply(N+1)` evaluates against the visible effects of `N`.
4. **Durable watermark truth.** `D` advances only from a successful
   `flush_wal(true)` completion covering that batch. Submission or
   `write_opt(sync=false)` completion must not advance it.
5. **Recovery prefix.** After a machine/power failure, the recovered state and
   applied record describe one atomic prefix. Recovery may observe an index
   newer than the last callback-recorded `D`, but never a partially applied
   entry.
6. **Replay availability.** Every log entry after the recovered applied index
   and through the re-established committed index remains available locally,
   unless a sync-durable installed snapshot already covers it.
7. **Purge fence.** A log entry may be purged only when `P <= D` has been
   established on that node. OpenRaft's volatile `last_applied` is not adequate
   evidence.
8. **Snapshot fence.** Before a locally built snapshot at `S` is returned to
   OpenRaft, state durability must satisfy `D >= S`. Snapshot installation
   remains sync-durable before returning.
9. **No reverted reads.** After every process start, stale/local serving stays
   closed until a current leader has quorum-fenced a committed target and the
   local visible state satisfies `A >= target`.
10. **Deterministic replay.** Replaying a committed request produces the same
    value, version, CAS result, client-sequence record, membership, and reply as
    the original apply.
11. **Bounded dirty state.** The `A - D` gap is bounded by entries, bytes, and
    age. Reaching a limit blocks later applies on durability rather than
    dropping work or allowing unbounded log retention.
12. **Fail closed on storage error.** An asynchronous state-flush failure closes
    serving, prevents purge/reclamation, and causes subsequent storage/Raft I/O
    to fail. Earlier client replies remain valid because their quorum logs are
    durable.

## Current blockers

The current implementation cannot safely be changed by merely removing the
await in `Storage::write_state_batch`:

1. `Storage::apply_raft` currently returns only after `flush_wal(true)`, so it
   has no separate visible and durable completions.
2. The per-group applied pointer is persisted, but there is no in-memory
   durable-applied tracker suitable for fencing log purge.
3. `RocksLogStore::purge` trusts OpenRaft's applied/snapshot decision and does
   not independently wait for state durability.
4. The optional committed marker is normally folded into the state batch.
   After asynchronous apply, a crash can legitimately recover an older applied
   marker while newer acknowledged entries remain in the Raft log.
5. A restarted follower's stale-read gate checks serving authority, voter
   membership, leader visibility, and optional `min_version`, but does not prove
   that the current leader has re-established the post-crash committed prefix.
6. Snapshot build scans visible state without first proving that the same
   applied prefix is durable.
7. Group reclamation and shutdown currently assume that a completed apply has
   no outstanding state-durability callback.
8. An asynchronous flush error has no path to revoke serving after `apply()` has
   already returned.

## Architecture

### 1. Per-group apply durability tracker

Add a tracker owned by `Storage` and registered for every live group:

```text
ApplyDurability {
    visible_applied: Option<LogId>,
    durable_applied: Option<LogId>,
    pending_entries: usize,
    pending_bytes: usize,
    oldest_pending_at: Option<Instant>,
    lifecycle: Open | Quiescing | Failed | Closed,
    notification: watch/Notify,
}
```

Initialization reads the atomic `raft_applied` record recovered by RocksDB and
sets both `A` and `D` to that value. A machine crash may have persisted more
than callbacks reported before death; the recovered record is authoritative
for the new process.

Expose these operations:

- `record_visible(group, log_id, entries, bytes)`;
- `record_durable(group, log_id)`;
- `wait_durable(group, log_id)`;
- `quiesce_group(group)`;
- `fail_group` and database-wide `fail_storage`;
- metrics for `A`, `D`, entry/byte lag, oldest lag, waits, and failures.

Watermarks compare full `LogId`s and move only forward. A callback from an old
batch after snapshot installation must never move a watermark backward.

### 2. Split written and durable state completions

Extend the shared durability worker's existing `on_written`/`on_durable`
mechanism to state writes:

1. Reserve bounded pending request/byte capacity.
2. Enqueue the atomic RocksDB state batch.
3. Complete `on_written` only after `write_opt(sync=false)` succeeds and the
   batch is readable.
4. `Storage::write_state_batch_visible` awaits `on_written`, records `A`, and
   returns to the state machine.
5. The reservation remains held until the flush completes, preserving the
   current bound on not-yet-durable bytes.
6. `on_durable` records `D` only after `flush_wal(true)` succeeds.

Synchronous write failure is returned from `apply()` and produces no client
reply. A later flush failure marks the shared RocksDB storage failed, wakes all
durability and purge waiters with an error, and closes every affected serving
gate.

Do not submit state evaluation itself to this worker. OpenRaft continues to
serialize apply, and each apply waits for RocksDB visibility before the next
entry can observe state.

### 3. Committed-position strategy

Implement the transition in two modes:

#### Correctness-oracle mode

Initially run asynchronous state apply with `DAL_COMMITTED_SYNC=1`. OpenRaft
sync-durably saves `C` before invoking state apply. On restart, its storage
helper replays `D+1..C`. This mode may retain a second fsync and is not the
performance target; it isolates and validates asynchronous state, replay, and
purge correctness.

#### Final fast mode

Keep `save_committed` optional and fold the recovery hint into the same batch as
the applied state, as today. Therefore recovered `committed_hint == D` for the
atomic recovered prefix. Do not claim that this hint is the cluster's complete
pre-crash `C`.

Entries in `(D, C]` remain durable in the Raft log. A current-term leader must
re-establish the committed boundary before the restarted state may serve
stale/local reads. This uses the recovery-serving fence below.

This recovery argument depends on ordinary Raft Leader Completeness and on
OpenRaft 0.9.24 committing a blank entry in the new leader's term. Committing
that entry re-establishes the preceding committed prefix after a full-cluster
restart even when the optional committed marker reverted to `D`. Pin this as a
tested storage/protocol contract when upgrading OpenRaft; do not treat it as an
unstated library implementation detail.

Startup validation must reject the unsafe combination:

```text
async state + non-durable committed hint + recovery serving fence disabled
```

### 4. Recovery-serving fence

Introduce a per-process, per-group recovery epoch. It always starts closed,
even after a clean-looking RocksDB open; the process cannot locally prove that
no acknowledged commit existed above recovered `D` immediately before a
machine failure.

Add an internal data-group read-fence operation:

1. A current leader runs `ensure_linearizable()`.
2. It returns a fence containing group, leader term, membership/config identity,
   and the read/committed index established by that quorum barrier.
3. A follower waits until its OpenRaft visible applied index reaches the target.
4. It verifies that the group is still serving, still a committed voter, and
   has not changed recovery epoch.
5. Only then does it open stale/local serving for that process epoch.

Leader linearizable reads already perform the required barrier and may open
their local epoch after the corresponding state is visibly applied. A
successful client write also proves that its result and all preceding committed
entries were visibly applied, but stale reads should use the explicit fence
rather than infer readiness from `current_leader`.

While closed:

- linearizable reads may perform the barrier and recover normally;
- writes may proceed through Raft and open readiness only after successful
  apply;
- stale reads return `TooStale`/redirect;
- node-local diagnostic reads must be labelled non-authoritative;
- no placement, admission, or reconciliation decision may consume the data as
  authoritative state.

The first rollout leaves meta apply synchronous, so startup, placement,
admission, failure handling, and rebalancing do not depend on this new fence.

### 5. Purge and snapshot fences

Change `RocksLogStore::purge(log_id)` to wait asynchronously for
`durable_applied >= log_id` before writing `KEY_PURGED` or deleting entries.
Never silently clamp the requested index: OpenRaft records the requested purge
after the method returns, so returning after purging less would corrupt its I/O
state. A durability failure returns a storage error and leaves the logs intact.

Snapshot build must:

1. acquire the existing shared state-view lock;
2. read visible applied state `S`;
3. wait for `D >= S` while holding the view against later apply;
4. scan and serialize exactly that state prefix;
5. return metadata for `S`.

This keeps the current non-persistent snapshot representation valid because the
underlying state prefix is durable before the snapshot is advertised. The
purge fence remains defense in depth.

Snapshot installation continues to use one `sync=true` replacement batch. On
success it sets both `A` and `D` to the installed snapshot index and opens
readiness only after OpenRaft accepts the current configuration/leader fence.
Reject an install whose `S` is behind the current visible `A`; do not rely only
on the caller to suppress stale snapshots.
Pending callbacks from older batches may release reservations but cannot move
the watermark backward or overwrite the installed state because the
state-view lock requires every earlier batch to have become visible before the
install write.

### 6. Lifecycle and bounded lag

Before snapshot replacement, CF reclamation, or group shutdown:

1. transition the tracker to `Quiescing` so no new state batch is admitted;
2. stop or fence the group's Raft apply producer;
3. wait for every accepted state batch to become durable;
4. perform the sync-durable install/drop/reclamation transition;
5. close and remove the tracker only after callbacks can no longer reference
   the group.

Normal process shutdown should drain state durability for fast restart, but
correctness must not depend on graceful shutdown.

Set explicit limits for dirty entries, dirty bytes, and dirty age. The initial
limits should be conservative and reuse the existing database-wide request and
64 MiB byte reservations. If any per-group limit is reached, later apply calls
wait for `D` rather than accumulating an unbounded replay/log-retention gap.

### 7. Primary code touchpoints

- `src/storage/durability.rs`: separate state written/durable completions,
  bounded reservations, and late-error propagation.
- `src/storage/rocks.rs`: tracker registry, visible-state submission,
  durability waits, quiescing, and health.
- `src/storage/batch.rs`: select durable versus visible apply per group without
  changing atomic batch contents.
- `src/partition/sm.rs`: data apply returns after visibility; snapshot build
  waits for the captured durable prefix.
- `src/meta/sm.rs`: retain synchronous apply and assert that policy explicitly.
- `src/partition/log_store.rs`: durable-applied purge fence and committed-hint
  mode.
- `src/partition/node.rs`, client/peer wire types, and runtime dispatch: process
  recovery epochs and the leader-fenced recovery RPC.
- runtime group shutdown/reclamation: quiesce and drain before dropping CFs.
- `src/perf.rs` and status reporting: `A-D`, recovery, purge-wait, and failure
  metrics.
- storage, Raft-group, runtime, verification, and benchmark tests: the crash
  and linearizability gates below.

## Implementation phases

## Phase 0: Lock down the storage contract

### Implementation

- Document `A`, `D`, `C`, and `P` in `DESIGN.md` and `IMPLEMENTATION.md`.
- Add a storage-model test that nondeterministically advances visible apply,
  durable apply, commit, snapshot, purge, and crash/recovery.
- Assert after every generated transition that `P <= D <= A <= C` and that
  replay from recovered `D` reproduces the reference state through `C`.
- Run OpenRaft 0.9.24's storage suite, including committed-log reapply, against
  the RocksDB log/state implementation.
- Add configuration validation for async state, committed-marker mode, and the
  recovery-serving fence.

### Exit criteria

- The model finds an error if purge is allowed to use visible `A` instead of
  durable `D`.
- The model finds an error if a restarted replica serves before re-establishing
  `C`.
- The existing synchronous behavior is unchanged.

## Phase 1: Add durability tracking without changing replies

### Implementation

- Add `ApplyDurability` tracking in `src/storage`.
- Pass `(group, final_log_id, entry_count, bytes)` through state durability
  callbacks.
- Initialize watermarks from the recovered atomic applied record.
- Record visible/durable lag metrics and callback errors.
- Keep `Storage::write_state_batch` awaiting durable completion, so `A == D`
  whenever `apply()` returns in this phase.

### Tests

- Multiple groups sharing one WAL flush advance only their covered callbacks.
- Callbacks arriving around snapshot install never move a watermark backward.
- Flush failure wakes all waiters and makes later reservations fail.
- Tracker removal waits for pending callbacks.

### Exit criteria

- Metrics show zero externally visible `A-D` gap in synchronous mode.
- No behavior or benchmark regression beyond disabled/low-cost instrumentation.

## Phase 2: Return state apply after visibility

### Implementation

- Add `Storage::write_state_batch_visible` in `src/storage/rocks.rs`.
- Extend `src/storage/durability.rs` so state writes expose both written and
  durable completion.
- Change data-group `Storage::apply_raft` to await written completion and let
  the durable callback advance `D`.
- Retain one atomic batch for mutations, sequence result, membership, applied
  record, and recovery hint.
- Keep meta-group calls on the existing durable wait.
- Enable this phase only with the sync-durable committed marker.

### Tests

- A read issued immediately after `apply()` sees the new value and sequence
  result while the durability callback is deliberately held.
- Two coalesced/serial apply batches preserve CAS and sequence ordering while
  both are not yet durable.
- Client response is emitted after visible apply but before a held state flush.
- Synchronous write error prevents the apply result; asynchronous flush error
  fails the node after the already committed result.

### Exit criteria

- OpenRaft restarts from persisted `C` and replays every deliberately lost
  state batch.
- Replayed replies are byte-identical to the original replies.

## Phase 3: Fence compaction and lifecycle

### Implementation

- Make `RocksLogStore::purge` await the per-group durable watermark.
- Make both data and meta snapshot builders wait for their captured prefix to
  become durable; meta should be an immediate no-op while it remains sync.
- Update snapshot install to reset the watermarks monotonically.
- Quiesce and drain state callbacks before group CF reclamation and shutdown.
- Add dirty-entry, byte, and age backpressure plus metrics.

### Tests

- Hold `D` behind `A`, trigger snapshot/purge, and verify no log at or above
  `D+1` is deleted.
- Release the flush and verify purge proceeds in the requested order.
- Inject a flush error while purge waits and verify logs and `KEY_PURGED`
  remain unchanged.
- Crash at every boundary of snapshot build, purge marker, range deletion,
  snapshot install, and CF reclamation.

### Exit criteria

- No code path deletes a log based only on OpenRaft's volatile applied metric.
- Every snapshot returned to OpenRaft represents a durable state prefix.

## Phase 4: Add restart recovery fencing

### Implementation

- Add the per-group process-epoch readiness state.
- Add the internal leader-fenced data read/recovery RPC and capability bit.
- Close stale serving at every group start.
- Open readiness only after a quorum-fenced target is visibly applied in the
  same epoch and membership/serving authority still matches.
- Keep linearizable reads available through their own barrier; keep stale reads
  fail-closed until readiness.
- Audit every direct/local data read used by startup, status, reconciliation,
  migration, and tests. Mark advisory reads explicitly and route authoritative
  decisions through a fence.

### Tests

- Restart a follower with state deliberately reverted below an acknowledged
  write; `stale_get(None)` must refuse until the leader fence and replay finish.
- Restart all three voters below the acknowledged state prefix; after election
  and current-term quorum commit, linearizable and stale reads return the
  acknowledged value.
- Isolate the old leader during restart; neither its old leader hint nor its
  local state opens readiness.
- Change membership while a fence RPC is in flight; the old fence must not
  open the new epoch/configuration.
- If the leader has no newer application entry, a quorum read fence still
  opens a fully caught-up follower without requiring a client write.

### Exit criteria

- No client-facing path can observe state reversion after restart.
- Recovery makes progress without requiring a new user mutation.

## Phase 5: Enable final fast committed-hint mode for data groups

### Implementation

- Switch data groups from sync-durable `save_committed` to the atomic
  applied-aligned recovery hint.
- Keep the synchronous committed-marker path as an independent rollback and
  correctness-oracle mode.
- Add startup assertions that recovered `committed_hint` and applied state
  match exactly in fast mode.
- Keep all meta-group state and committed-position behavior synchronous.

### Crash matrix

For each cut point, kill/reopen one node, the leader, a follower majority, and
all three nodes:

1. log batch visible but not durable;
2. log durable locally but not committed;
3. majority commit established before state write;
4. state WriteBatch visible before client response;
5. client response emitted before state flush;
6. state flush completed before callback/watermark update;
7. snapshot waiting on `D`;
8. purge after `D` but before `KEY_PURGED`;
9. purge marker durable before range deletion;
10. recovery replay visible before its state flush;
11. recovery fence response racing membership or leadership change.

### Exit criteria

- Every acknowledged write survives any allowed minority failure and a full
  cluster restart, assuming the quorum log fsyncs satisfy the storage model.
- Rejected, replayed, and failed-CAS results remain identical after recovery.
- Uncommitted suffixes are never applied during recovery.

## Phase 6: Fault handling, operations, and upgrades

### Implementation

- Add a database health state consumed by `require_serving`, Raft storage
  methods, snapshot/purge, and reclamation.
- On asynchronous flush failure, stop new serving immediately and initiate
  bounded Raft shutdown/restart rather than continuing with an unknown storage
  device.
- Export:
  - visible and durable applied indexes per group;
  - dirty entries/bytes/age;
  - state flush latency and failures;
  - purge/snapshot durability wait;
  - recovery-fence state, target, attempts, and duration;
  - replayed entries and recovery duration.
- Define alerts for dirty-age/byte limits, repeated recovery fencing, failed
  flush, and log growth caused by a stalled `D`.
- On graceful shutdown and before an incompatible binary upgrade, force
  `D == A` for every group.
- Version state-machine command/replay semantics, or require backward-compatible
  evaluation of retained log entries. A new binary must not reinterpret an old
  CAS/idempotency entry differently.
- Require recovery-fence capability on all voters before enabling fast mode in
  a rolling deployment. An older leader causes stale reads to remain closed,
  not to bypass the fence.

### Exit criteria

- Storage failure is operator-visible and fail-closed.
- A rolling downgrade to synchronous apply can drain dirty state without data
  conversion.
- Replay compatibility is covered across the oldest supported on-disk/log
  version.

## Phase 7: Meta-group evaluation

Do not automatically copy the data-group setting to `GroupId::Meta`.

Before a separate meta rollout, audit every local meta read involved in:

- bootstrap and registration;
- serving/admission authorization;
- placement and membership reconciliation;
- failure detection and incarnation fencing;
- rebalance plan creation/finalization;
- group reclamation.

The meta group may use asynchronous materialization only after all authoritative
reads have an equivalent recovery fence, durable snapshots cover every purge,
and cluster startup has no circular dependency on recovering meta state through
a service that itself requires meta state. Keeping meta synchronous is an
acceptable permanent design because its write rate is low and its state is
control-plane authority.

## Verification strategy

### Deterministic tests

- Unit-test the durability worker with separate visible and durable fake maps.
- Property-test the `C/A/D/P` transition model with crash at every transition.
- Run OpenRaft's storage conformance suite in both synchronous-committed and
  folded-hint modes where applicable.
- Verify full-`LogId` monotonicity across terms, not only indexes.

### Real RocksDB crash tests

- Use child processes and abrupt exit to verify process-crash recovery.
- Use failpoints/fake durability to model machine failure that discards every
  write after `D`; a normal process kill is insufficient because the kernel may
  retain and later write page-cache contents.
- Reopen the real RocksDB directory after every sync-durable crash boundary and
  verify atomic state/applied/hint records.

### Cluster tests

- Reuse the three-voter `ChannelNetwork` fault injector for loss, duplication,
  reorder, partitions, and leader replacement.
- Run the linearizability checker over histories containing writes, CAS,
  retries, linearizable reads, stale reads with `min_version`, node crashes, and
  full-cluster restart.
- Stress every new crash scenario for at least 100 seeded iterations and retain
  the seed for every failure.
- Exercise snapshot transfer and membership changes while `A > D`.

### Validation commands

After every phase:

```text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc
git diff --check
```

Before rollout, run the ignored release benchmark with profiling disabled and
enabled. Performance is not a correctness gate, but the profile must confirm
that client responses no longer wait for `StateApplyDurabilityWait` and that
log durability remains unchanged.

## Rollout and rollback

1. Land the tracker and metrics with synchronous behavior.
2. Enable async data state plus sync-durable committed marker in tests and one
   canary cluster.
3. Enable the recovery-serving fence while state is still synchronous; verify
   it cannot be bypassed and measure recovery availability.
4. Enable folded-hint fast mode for data groups on a small canary after every
   voter advertises fence capability.
5. Expand only while `A-D`, replay, log growth, snapshot waits, and stale-read
   refusals remain bounded.
6. Keep meta state synchronous.

Rollback to synchronous state apply requires no on-disk migration. Stop new
apply, drain until `D == A`, then restart/flip the mode. If a node cannot drain
because storage failed, leave it non-serving and recover it from a healthy
Raft replica or snapshot; never force purge or mark its state durable.

## Final acceptance criteria

- Raft log and vote fsync behavior is byte-for-byte and callback-for-callback
  unchanged.
- The state machine returns only after atomic visibility, but not after WAL
  durability, for enabled data groups.
- Every purged index is less than or equal to the locally durable applied
  index.
- Every locally built snapshot represents a durable state prefix.
- No restarted node serves a state prefix older than a previously acknowledged
  write after it passes its recovery fence.
- Full-cluster restart reconstructs acknowledged state and exact idempotency
  results from durable quorum logs.
- Flush failure, queue saturation, shutdown, snapshot install, and reclamation
  all fail closed with bounded resources.
- Meta-group authority remains synchronously durable until separately approved.
- The implementation passes the deterministic model, OpenRaft storage suite,
  crash matrix, linearizability histories, and seeded cluster stress gates.
