# DAL Service Performance Implementation Plan

## Purpose

Improve steady-state throughput and p50/p95/p99 latency without weakening the
service's durability, linearizability, membership fencing, learner admission,
or reclamation rules.

The work is ordered by expected impact and dependency. Each phase establishes a
measurable baseline, preserves a named correctness invariant, and can be rolled
back independently. Optimizations must not be accepted from an in-memory or
`tmpfs`-backed benchmark alone.

## Current performance risks

1. The 150 ms rebalance loop performs linearizable meta reads even when there
   is no plan or placement change. Across a cluster, the data-leader and reclaim
   scans alone issue approximately `(P + P * R) / 0.150s` ReadIndex operations.
   At `P=128, R=3`, this is about 3,413 meta ReadIndex operations per second,
   before address refresh, genesis checks, meta reclamation, or client work.
2. With `DAL_LOG_GROUP_COMMIT=0`, RocksDB log fsync calls execute directly
   inside async OpenRaft storage methods. The default Phase 3/4 path instead
   sends log and state-machine batches through one bounded database worker,
   folds the committed marker into state apply, and releases callbacks only
   after a shared WAL flush. Vote writes and lifecycle operations still
   execute synchronously on the async path.
3. The inbound ROUTER notices completed replies through a 1 ms receive timeout.
   The delay can recur on the client and Raft RPC legs of one operation.
4. Redirects and routing queries can rebuild and serialize the complete routing
   snapshot. Route lookup itself linearly scans placement and directory vectors.
5. Snapshot build/install buffers and rewrites an entire partition while
   holding the state-view lock, creating unbounded memory use and long apply
   stalls as partitions grow.
6. Every linearizable read starts its own quorum confirmation. Stale reads are
   rotated across all voters, but a stale request reaching the leader still
   performs ReadIndex.

The existing data apply coalescing remains enabled throughout this plan. Its
recorded ext4 A/B result (approximately 366 to 762 concurrent writes/s and p99
from 258 ms to 118 ms in a one-group test) demonstrates that reducing durable
write count is the right direction, but it does not address cross-group fsyncs
or background control traffic.

## Correctness invariants

Every phase must preserve these invariants:

1. A Raft log-flush callback fires only after every covered entry is durable.
2. When `RaftLogStorage::append` returns, appended entries are immediately
   readable and the log contains no hole.
3. A successful client mutation response implies majority-durable Raft commit
   and durable state-machine application.
4. Cached metadata may suppress work or act as a prefilter, but only a current,
   authoritative observation may authorize membership change, learner
   admission, identity fencing, or local reclamation.
5. Linearizable reads include only requests whose invocation precedes their
   quorum barrier. A request must never join an already-started barrier if doing
   so could omit an earlier completed write.
6. Serving-gate publication and reclamation retain their crash-safe ordering.
7. Snapshot install exposes either the prior complete state or the new complete
   state, never a partial replacement.
8. Backpressure may reject or delay work explicitly; it must not silently lose
   a Raft RPC or claim success for a dropped client reply.

## Phase 0: Establish trustworthy measurement

### Implementation

- `DAL_BENCH_DIR` now selects the parent of the three RocksDB benchmark
  directories. When it is unset, the harness prominently warns that the OS
  temporary directory must be checked for durability.
- Retain the fast three-node `inproc://` smoke, but add a multi-process TCP
  profile with one RocksDB directory per process.
- Allow the benchmark to run with the full control plane or with background
  reconciliation disabled. Report both so control-plane tax is visible rather
  than accidentally excluded.
- Share one cloned `ZmqTransport` across benchmark clients in the primary
  profile. Keep a separate "transport per client" profile to expose thread and
  socket scaling.
- Record retries, redirects, transport backpressure, timeouts, and failed
  operations separately. Do not hide them behind a single latency sample.
- `DAL_PROFILE_WRITE_PATH=1` now enables stage timers and RocksDB WAL sync/byte
  counters for the benchmark. It is opt-in because global timing counters and
  RocksDB statistics perturb throughput; disabled mode does not read the clock
  or enable RocksDB statistics.
- Add lightweight histograms/counters for:
  - end-to-end operation latency by operation and consistency;
  - Raft log append queue time, write time, fsync time, entries, and bytes;
  - state apply queue time, fsync time, entries, mutations, and bytes;
  - `save_committed` calls and durable writes;
  - ReadIndex requests by caller (`client`, `rebalance`, `directory`,
    `reclaim`, `bootstrap`);
  - ROUTER receive-to-dispatch, handler, reply-queue, and reply-send time;
  - in-flight handlers, queue depth, dropped/rejected sends, and retries;
  - routing snapshot builds, encoded bytes, cache hits, and redirect retries;
  - snapshot build/install duration, pause time, bytes, and peak buffered bytes.
- Add benchmark dimensions for `P={1,16,128}`, clients `{1,16,64}`, values
  `{128 B, 4 KiB, 1 MiB}`, sequential and concurrent writes, linearizable and
  stale reads, and mixed traffic.
- Run sustained profiles long enough to cross the snapshot threshold and a
  leader-change profile that includes cold routing and redirects.

### Acceptance criteria

- Release-mode results identify the filesystem, transport, process topology,
  CPU count, value size, partition count, client count, and control-plane mode.
- Every reported throughput number includes successful operation count and
  retry/error counts.
- A baseline report is checked into project documentation before Phase 1 is
  measured.

## Phase 1: Remove steady-state reconciliation amplification

### Implementation

- Introduce an immutable, versioned meta snapshot containing directory,
  placements, and the meta applied index. Publish it through a cheap shared
  handle when local meta state advances or a newer authoritative remote
  snapshot is learned.
- Change the rebalance driver to use the snapshot as a prefilter:
  - inspect a data group only when its placement contains a plan;
  - perform an exact ReadIndex-fenced placement read immediately before a
    membership-changing action;
  - consider reclamation only when the cached placement is resolved and
    excludes the local node, then repeat the authoritative check before
    stopping or deleting anything.
- Read directory and all placements once per driver pass. Do not call
  `read_placement` independently from the data-leader and reclaim roles.
- Replace the 150 ms authoritative directory refresh with change-driven
  refresh plus a slow safety interval measured in seconds. Heartbeats and Raft
  address failures may request an immediate refresh.
- Add a replicated or otherwise safely reconstructable `genesis_complete`
  marker. Once observed, stop probing the designated voter for every original
  partition on every tick. A newly elected meta leader may verify the marker,
  but must not restart the complete partition-by-partition scan indefinitely.
- Run meta-reclaim network checks only when local evidence indicates this node
  is draining, excluded, or participating in a meta move.
- Replace fixed-rate full scans with notifications where practical. Retain a
  slow idempotent safety sweep for recovery from missed notifications.

### Tests

- At `P=128, R=3`, run an idle healthy cluster for at least 30 seconds and
  assert that rebalance/reclaim ReadIndex count does not scale with `P`.
- Create, abort, resume, and finalize a plan while notifications are delayed or
  dropped. The safety sweep must eventually reconcile it.
- Feed a stale cached placement that excludes the local node; verify the
  authoritative recheck prevents reclamation when the live placement still
  includes it.
- Change directory endpoints/incarnations and verify a prompt authoritative
  refresh still fences conflicting local identity.
- Elect a new meta leader after genesis and verify it does not generate an
  indefinite `P`-wide bootstrap probe storm.

### Acceptance criteria

- In an idle healthy cluster, control-plane ReadIndex traffic is `O(nodes)` per
  safety interval, not `O(P * R)` per 150 ms.
- No membership change, admission, or reclamation is authorized solely from a
  cached snapshot.
- Foreground p99 improves or remains neutral in the full-control-plane profile.

## Phase 2: Make ROUTER reply delivery event-driven and bounded

### Implementation

- Add a reply wake socket or eventfd to `ZmqServer`, analogous to the DEALER
  wake mechanism. Poll inbound ROUTER activity and completed replies together.
- Remove `set_rcvtimeo(1)` as the normal reply-flush mechanism.
- Replace the unbounded reply channel and unbounded handler spawning with
  explicit limits:
  - a bounded in-flight handler budget;
  - bounded reply buffering;
  - observable rejection/backpressure when capacity is exhausted.
- Ensure one failed or backpressured peer cannot block the socket-owner thread.
  Record failed reply sends and surface retryable outcomes where the protocol
  permits it.
- Evaluate a dedicated client ingress endpoint or sharded client ROUTERs while
  retaining a protected peer-control endpoint. At minimum, reserve capacity so
  client floods cannot starve Raft heartbeats, votes, and append replies.
- Preserve async shutdown: stopping accepts, joining the poller, and awaiting
  active handlers remain separate operations.

### Tests

- Add a sequential inproc echo benchmark proving replies do not wait for a
  periodic timer.
- Saturate client handlers and verify peer-control requests still make bounded
  progress.
- Fill handler/reply capacity and verify memory stays bounded and each caller
  receives success, explicit backpressure, or timeout—never false success.
- Repeat shutdown tests with full queues and handlers completing concurrently.

### Acceptance criteria

- Idle sequential echo latency no longer clusters around a 1 ms polling floor.
- Queue and handler memory are bounded by configuration.
- Peer-control latency remains bounded under client overload.

## Phase 3: Add asynchronous, cross-group Raft log durability

### Implementation

- Add one durability coordinator per RocksDB instance.
- In `RaftLogStorage::append`:
  1. encode and enqueue the entries for the bounded database worker;
  2. let the worker write the batch to RocksDB/WAL without synchronous fsync;
  3. signal append-return only after entries are readable;
  4. group currently active log and state batches across Raft groups;
  5. execute one `flush_wal(true)` for the database-wide batch;
  6. complete every covered `LogFlushed` callback and state waiter in order.
- Bound the coordinator queue and bytes. Apply backpressure before accepting
  unbounded log data.
- Define failure semantics precisely: a failed WAL flush completes every
  covered callback with an error and prevents a later successful callback from
  overtaking it.
- State-machine apply writes now use the bounded database worker. Snapshot, CF
  lifecycle, purge, and truncate remain candidates for the same executor. Keep
  short cached point reads synchronous only if measurement shows they do not
  starve Tokio.
- Avoid one independent unbounded blocking task per partition. Executor
  concurrency should match the storage device and preserve per-group ordering.
- Retain the current per-apply entry coalescing and add equivalent coalescing to
  the meta state machine where bursts can occur.

### Tests

- Verify entries are readable immediately after `append` returns but callbacks
  do not complete before the durable flush.
- Crash after the non-sync write and before callback: no acknowledged entry may
  be lost.
- Crash after group flush and before callback delivery: replay remains safe and
  callbacks/retries cannot corrupt ordering.
- Inject a flush error covering multiple groups and verify all covered Raft
  instances receive an error.
- Stress many partitions on a one-worker Tokio runtime and verify storage I/O
  does not prevent timers, RPC handlers, or shutdown from progressing.
- Compare one hot group with many moderately active groups to verify cross-group
  batching.

### Acceptance criteria

- No RocksDB fsync executes on a Tokio worker thread.
- Log append callbacks retain the exact durability contract.
- Concurrent write throughput materially exceeds the Phase 1 baseline without
  increasing acknowledged-write loss risk or p99 from unbounded batching.

### Measured A/B result (2026-08-01)

The release-mode benchmark was run in alternating order three times per mode.
The first measurements using the harness default were discarded after
`findmnt` identified `/tmp` as `tmpfs`; only the explicit ext4 results below
are reported.

- Revision: `2b5b7f1` plus the working-tree Phase 3 implementation.
- Storage: ext4 on `/dev/nvme0n1p2`, with `DAL_BENCH_DIR` set to
  `/home/alex/workspace/dal-service/target/benchmark-data`.
- Host: Linux 6.18.36-1-lts, Intel i7-8650U, 4 cores / 8 logical CPUs.
- Topology: three runtime nodes in one process over ZeroMQ `inproc://`, `R=3`,
  16 partitions, normal background control loops enabled.
- Workload: 128-byte values, 1,000 sequential operations, 16 concurrent
  clients, and 6,000 operations in each concurrent phase.
- Both modes used `DAL_APPLY_COALESCE=1`. The baseline used
  `DAL_LOG_GROUP_COMMIT=0`; the candidate used `DAL_LOG_GROUP_COMMIT=1`.
- Every reported phase completed its requested successful-operation count with
  no terminal test failure. The current harness does not expose internal retry
  counts, fsync counts/duration, CPU, or disk utilization.

Median of three trials per mode:

| Workload | Sync append throughput | Group commit throughput | Throughput change | Sync p50 / p95 / p99 | Group p50 / p95 / p99 |
|---|---:|---:|---:|---:|---:|
| Sequential write | 146 ops/s | 131 ops/s | -10.3% | 6.95 / 8.27 / 9.42 ms | 7.51 / 8.41 / 9.05 ms |
| Concurrent write | 411 ops/s | 501 ops/s | +21.9% | 36.56 / 62.43 / 73.32 ms | 30.35 / 48.54 / 57.34 ms |
| Concurrent mixed 50/50 | 781 ops/s | 991 ops/s | +26.9% | 18.27 / 41.13 / 50.75 ms | 14.51 / 34.27 / 41.37 ms |
| Sequential linearizable read | 414 ops/s | 413 ops/s | -0.2% | 2.65 / 3.19 / 3.37 ms | 2.67 / 3.15 / 3.33 ms |

For this durable-filesystem profile, database-wide group commit meets the
concurrent-throughput criterion and improves median concurrent tail latency.
It does not improve the sequential write path: the bounded 200 microsecond
collection window reduces sequential throughput and raises p50. In addition,
one candidate concurrent-write trial recorded a 122.71 ms p99 even though the
other two recorded 57.34 ms and 55.23 ms. Keep
`DAL_LOG_GROUP_COMMIT=0` as the immediate rollback, and do not treat Phase 3 as
fully rollout-qualified until sustained runs report retries, fsync count and
duration, queue depth, CPU/disk utilization, and snapshot/control-plane
interference. Phase 3 also remains structurally incomplete: append fsync moved
off Tokio, but vote, committed-marker, state-apply, snapshot, purge, and
truncate I/O have not yet moved onto the planned bounded storage executor.

Reproduction command (run three alternating trials for each flag value):

```text
DAL_BENCH_DIR=<durable-ext4-directory> \
DAL_APPLY_COALESCE=1 \
DAL_LOG_GROUP_COMMIT=<0-or-1> \
cargo test --release --test benchmark_e2e \
  end_to_end_benchmark_three_nodes -- --ignored --nocapture
```

### Write-path profile (2026-08-01)

One synchronous baseline and two coordinator runs were captured on the same
ext4 profile with `DAL_PROFILE_WRITE_PATH=1`. Profiling results are diagnostic,
not throughput acceptance data: stage totals aggregate all three nodes and
concurrent stages overlap. The second coordinator run is shown below because
its 501 concurrent writes/s matched the unprofiled median rather than the first
profiled run's tail-latency outlier.

| Stage/counter | Sync append baseline | Group commit |
|---|---:|---:|
| Successful client write workload | 10,000 | 10,000 |
| Raft `client_write` calls | 10,008 | 10,008 |
| Replica log appends | 28,466 | 28,077 |
| Log sync write | 1.976 ms mean | replaced by coordinator |
| Log append-to-durable-callback | synchronous return | 3.064 ms mean |
| Coordinator batch collection | n/a | 0.297 ms mean |
| Coordinator `flush_wal(true)` | n/a | 20,663 calls, 1.324 ms mean |
| Callbacks per coordinator flush | n/a | 1.36 average, 9 maximum |
| Committed-marker sync write | 1.957 ms mean | 1.891 ms mean |
| State-apply sync write | 2.236 ms mean | 1.878 ms mean |
| Physical RocksDB WAL syncs | 56,118 | 58,113 |
| Physical WAL syncs/client write | 5.61 | 5.81 |
| RocksDB write-stall time | 0 ms | 0 ms |

The profile changes the optimization target:

1. RocksDB already groups concurrent `sync=true` writers. Replacing 28,077 log
   sync calls with 20,663 explicit coordinator flushes reduced API-level sync
   operations, but physical WAL syncs increased by 3.6% in the matched profile.
   Many baseline log appends were evidently sharing RocksDB write groups with
   other synchronous writes.
2. The coordinator rarely builds a large cross-group batch: 1.36 callbacks per
   flush. Its 200 microsecond collection window and worker handoff explain the
   sequential-write regression, while moving append fsync off Tokio still helps
   concurrent scheduling in the unprofiled benchmark.
3. Committed-marker and state-apply sync writes remain the dominant repeated
   durable work. In the representative coordinator run they consumed 51.95 s
   and 51.60 s of aggregate wall time, versus 27.35 s in coordinator flushes.
   State-apply evaluation/encoding added only about 0.105 ms beyond its 1.878 ms
   sync write.
4. Encoding is not a bottleneck (about 9 microseconds per replica append), the
   durability queue is not capacity-bound, and RocksDB reported no write stall.

Before tuning the collection window further, Phase 4 should measure and remove
the redundant committed/apply durability boundaries. The storage executor
should then coordinate all durable writes per DB so explicit log flushes do not
compete with or duplicate RocksDB's native write grouping.

## Phase 4: Reduce durable writes on the committed/apply path

### Implementation

- In the default mode, `save_committed` is the optional no-op allowed by
  OpenRaft for a durable state machine. `apply_raft` writes the final applied
  LogId as the committed marker in the same atomic cross-column-family batch as
  the applied pointer, membership, business mutations, and idempotency records.
- The database worker now owns both log and Raft-state non-sync writes. State
  apply asynchronously waits for the same `flush_wal(true)` that releases log
  callbacks, allowing one physical sync to cover multiple groups and both
  kinds of durable work.
- Adaptive collection flushes a truly lone write immediately. Once a second
  active writer is observed, that flush cycle uses the bounded 200 microsecond
  window to retain concurrent batching.
- State apply always uses the unified durability worker;
  `DAL_ADAPTIVE_DURABILITY=0` restores the fixed collection window.
- Profiling reports each benchmark phase separately and records log/state
  writes per durability flush, state batch entries/mutations/bytes, and folded
  marker counts.
- Never weaken vote durability or follower log durability.
- Keep the final state-machine mutation, idempotency record, membership, and
  applied pointer atomic and durable before replying to the client.

The crash states are:

1. Before the worker writes the state batch, recovery starts from the previous
   durable applied prefix and no client has been acknowledged.
2. After the non-sync batch write but before the shared flush, its atomic
   contents may be visible to the running process but no state waiter or client
   response is released. A machine crash either loses that WAL suffix or
   recovers the whole RocksDB WriteBatch; it cannot recover a partial mutation
   or a marker/applied mismatch.
3. After `flush_wal(true)`, every earlier collected batch is durable. Only then
   are state waiters and log callbacks completed. Blank and membership-only
   entries still write the marker and applied record even without a business
   mutation.
4. After apply returns, the business mutation, idempotency record, membership,
   committed marker, and applied pointer are durable. OpenRaft sends the client
   response only after this point. A crash before delivery is handled by the
   existing idempotent retry.

On startup the marker and applied pointer from a completed default-mode apply
name the same prefix. OpenRaft still reconciles old-format data by taking a
newer durable `last_applied` or replaying a durable log when an older committed
marker is ahead.

### Tests

- Fail-point every boundary between log durability, committed-marker update,
  state apply, and client response; reopen and verify a clean prefix.
- Verify membership entries and blank entries recover correctly when there is
  no business mutation to force an apply batch.
- Run the complete linearizability and no-lost-acknowledged-write suites for
  both the old and experimental modes.

### Acceptance criteria

- The selected design removes a measured critical-path fsync or is rejected.
- Sequential write p50/p99 improve without reducing durability.
- The old mode remains available until crash and soak testing are complete.

### Measured A/B result (2026-08-01)

The same ext4 three-node benchmark used for Phase 3 was run in alternating
order three times per mode, with `DAL_APPLY_COALESCE=1` and
`DAL_LOG_GROUP_COMMIT=1` fixed. The baseline used the former synchronous-marker
implementation; the candidate folded the marker into state apply. Data directories were under
`/home/alex/dal-bench` on `/dev/nvme0n1p2` (ext4). Each trial completed 1,000
sequential writes and reads plus 6,000 operations in every concurrent phase,
with no terminal failure.

Median of three unprofiled trials per mode:

| Workload | Sync marker throughput | Deferred marker throughput | Throughput change | Sync p50 / p95 / p99 | Deferred p50 / p95 / p99 |
|---|---:|---:|---:|---:|---:|
| Sequential write | 130 ops/s | 153 ops/s | +17.7% | 7.98 / 8.43 / 10.35 ms | 6.88 / 7.30 / 7.62 ms |
| Concurrent write | 488 ops/s | 824 ops/s | +68.9% | 30.77 / 49.31 / 59.46 ms | 18.15 / 30.23 / 37.32 ms |
| Concurrent mixed 50/50 | 1,042 ops/s | 1,493 ops/s | +43.3% | 13.75 / 32.64 / 39.31 ms | 9.62 / 23.00 / 28.23 ms |
| Sequential linearizable read | 404 ops/s | 406 ops/s | +0.5% | 2.69 / 3.31 / 3.40 ms | 2.67 / 3.19 / 3.38 ms |

One matched profiled run per mode explains the improvement:

| Stage/counter | Sync marker | Deferred marker |
|---|---:|---:|
| Raft `client_write` | 23.211 ms mean | 14.076 ms mean |
| Committed marker | 27,425 sync writes, 1.871 ms mean | 27,355 WAL writes, 0.580 ms mean |
| State-apply sync write | 1.859 ms mean | 1.535 ms mean |
| Coordinator flushes | 20,424 | 16,699 |
| Callbacks per coordinator flush | 1.37 average | 1.68 average |
| Physical RocksDB WAL syncs | 57,538 | 38,392 (-33.3%) |
| Physical WAL syncs/client write | 5.75 | 3.84 |
| RocksDB write-stall time | 0 ms | 0 ms |

This meets the Phase 4 performance criteria: it removes one measured
critical-path fsync, improves sequential p50/p99 by 13.8%/26.4%, and improves
concurrent throughput and tail latency. Read-only results remain within normal
trial variance. The synchronous-marker rollback path was subsequently removed.

The synchronous-marker comparison is historical; the current tree contains
only the folded-marker implementation. Run the current benchmark with:

```text
DAL_BENCH_DIR=<durable-ext4-directory> \
DAL_APPLY_COALESCE=1 \
DAL_LOG_GROUP_COMMIT=1 \
cargo test --release --test benchmark_e2e \
  end_to_end_benchmark_three_nodes -- --ignored --nocapture
```

### Unified durability follow-up (2026-08-01)

The next profile showed that the remaining state-apply sync writes competed
with the log coordinator and prevented cross-kind batching. The default path
therefore now submits both log and state batches to one database worker, folds
the committed marker directly into the state batch, and uses adaptive
collection: a lone request flushes immediately, while observed concurrency
opens the bounded 200 microsecond collection window.

The previous deferred-marker implementation and the unified candidate were
run on the same ext4 filesystem with the workload and data sizes above. These
are medians of three unprofiled candidate trials compared with the checked-in
three-trial deferred-marker median:

| Workload | Previous throughput | Unified throughput | Throughput change | Previous p50 / p95 / p99 | Unified p50 / p95 / p99 |
|---|---:|---:|---:|---:|---:|
| Sequential write | 153 ops/s | 167 ops/s | +9.2% | 6.88 / 7.30 / 7.62 ms | 5.93 / 7.18 / 7.40 ms |
| Concurrent write | 824 ops/s | 1,874 ops/s | +127.4% | 18.15 / 30.23 / 37.32 ms | 7.81 / 12.24 / 16.20 ms |
| Concurrent mixed 50/50 | 1,493 ops/s | 3,452 ops/s | +131.2% | 9.62 / 23.00 / 28.23 ms | 4.81 / 9.50 / 12.49 ms |
| Sequential linearizable read | 406 ops/s | 409 ops/s | +0.7% | 2.67 / 3.19 / 3.38 ms | 2.67 / 3.21 / 3.37 ms |

One phase-separated profiled candidate run, compared with a fresh profile of
the previous default, explains the gain. Counts aggregate the three nodes and
the 10,000 successful writes in the sequential, concurrent-write, and
mixed-workload phases; stage timings overlap and profiling itself perturbs
throughput.

| Stage/counter | Previous deferred-marker path | Unified durability path |
|---|---:|---:|
| Committed-marker persistence | 27,355 standalone non-sync writes | 27,532 markers folded into state batches |
| State persistence | 27,356 separate `sync=true` writes | 27,532 worker-owned non-sync writes |
| Durability writes | log only, 1.62 callbacks/flush | 28,222 log + 27,532 state, 3.25 writes/flush |
| Physical RocksDB WAL syncs | 39,118 | 17,158 (-56.1%) |
| Physical WAL syncs/client write | 3.91 | 1.72 |
| Raft `client_write` mean | 16.179 ms | 6.413 ms (-60.4%) |
| RocksDB write-stall time | 0 ms | 0 ms |

In the unified concurrent phases, the worker's state batch write averaged
38.7--39.6 microseconds, state durability wait averaged 1.577--1.589 ms, and
the shared flush averaged about 0.91 ms. This identifies WAL durability—not
encoding or the RocksDB batch write itself—as the remaining write-path cost.
The adaptive policy also recovers the sequential latency that a fixed
collection window gave up.

The full all-target/all-feature suite passes with the unified durability path.
The remaining collection-policy diagnostic switch is independent:

```text
DAL_ADAPTIVE_DURABILITY=0 \
cargo test --all-targets --all-features -- --test-threads=1
```

To reproduce the candidate benchmark, set `DAL_BENCH_DIR` to a durable
filesystem and leave the tuning switches unset. Use
`DAL_PROFILE_WRITE_PATH=1` for per-phase stage and RocksDB counters; profiling
runs are diagnostic and should not be used as throughput acceptance trials.

### Transport-attribution follow-up (2026-08-01)

The next phase-separated profile added timers around the client transport and
gateway, data-Raft AppendEntries transport and handler, and the interval from a
completed handler entering the ROUTER reply queue until the socket-owner thread
drains it. The same ext4/NVMe benchmark used 1,000 sequential writes, 6,000
concurrent writes, and 6,000 mixed operations with 16 clients. These numbers
are diagnostic results from one profiled run, not acceptance throughput.

| Stage | Sequential write mean | Concurrent write mean |
|---|---:|---:|
| End-to-end client operation | 5.98 ms | 7.43 ms |
| Client transport call | 5.959 ms | 7.406 ms |
| Client gateway handler | 4.916 ms | 6.518 ms |
| Raft `client_write` | 4.880 ms | 6.517 ms |
| Client ROUTER reply queue | 0.596 ms | 0.566 ms |
| Data-Raft append transport | 1.777 ms | 1.724 ms |
| Data-Raft append handler | 0.607 ms | 0.918 ms |
| Data-Raft ROUTER reply queue | 0.697 ms | 0.509 ms |
| Shared WAL flush | 0.972 ms | 0.888 ms |
| State durability wait | 1.301 ms | 1.538 ms |

For the sequential phase, transport outside the handler cost 1.043 ms on the
client leg and 1.169 ms on an average data-Raft append leg. The periodic ROUTER
reply drain directly accounted for 57% and 60% of those gaps, respectively.
The nearly identical roughly 0.45 ms residual on both legs includes outbound
handoff/wakeup, inbound dispatch, frame work, socket send/receive, and runtime
scheduling; it should be split further after removing the known polling delay.

Storage is still durability-bound: sequential WAL flushes averaged 0.972 ms,
while log encoding averaged 9 microseconds, log/state non-sync writes averaged
89/105 microseconds, capacity waits stayed below 1 microsecond, and RocksDB
reported zero write-stall time. Under concurrency the worker grouped 32,804 log
and state writes into 6,900 flushes (4.75 writes per flush), versus effectively
one worker write per flush in the sequential phase. Apply batches still held
only 1.11 entries on average, but their non-sync write cost is too small to
prioritize ahead of transport and WAL durability.

This makes Phase 2's event-driven ROUTER reply delivery the next software
bottleneck to remove. It imposes about 0.6--0.7 ms on each idle reply leg and
also sits on Raft quorum traffic. WAL sync latency remains the primary storage
floor; collection-window or apply-batch tuning should be evaluated only with an
A/B showing that any additional queueing improves the durability/latency trade.

## Phase 5: Cache and index routing

### Implementation

- Cache the complete `RoutingInfo` by meta applied index. Rebuild it once per
  metadata generation, not once per query or redirect.
- Store partition routes in a partition-indexed vector and node endpoints in a
  node-id map. Route lookup should not scan all placements or all directory
  entries.
- Give `ClientGateway` a cheap candidate lookup instead of invoking the full
  asynchronous routing snapshot path for every redirect.
- On the client, retry a redirected leader immediately when its endpoint is
  already cached. Refresh routing only when the hinted node cannot be resolved,
  the generation is known stale, or every candidate is exhausted.
- Use immutable snapshots/`Arc` values so normal lookups avoid cloning full
  placement and directory structures.
- Share a cloned `ZmqTransport` across clients in the supported client factory.
  Retain per-client idempotency and sequence state independently of the shared
  transport.

### Tests

- Count RocksDB reads and snapshot encodes while issuing repeated redirects;
  they must not scale as `requests * P`.
- Redirect to a known leader and verify no MetaQuery occurs before the retry.
- Redirect to an unknown newly joined leader and verify refresh discovers its
  endpoint and converges.
- Replace a routing generation concurrently with lookups and verify each
  request observes one internally consistent snapshot.

### Acceptance criteria

- Warm route and endpoint lookup are expected `O(1)` operations.
- A known-leader redirect costs one extra request/reply, not a complete routing
  rebuild and refresh.
- Client count does not imply one new I/O thread per client/destination pair in
  the standard client construction path.

## Phase 6: Stream snapshots and isolate their I/O

### Implementation

- Replace full-CF materialization with RocksDB checkpoint/SST-based snapshot
  production or a bounded streaming format.
- Build snapshots on the storage executor from a stable RocksDB snapshot or
  checkpoint. Do not hold the apply mutex while copying and serializing the
  entire partition.
- Transfer bounded chunks over the bulk lane and apply explicit I/O and
  concurrency limits so migration does not saturate the device.
- Install through verified staged files and atomic/restartable SST ingestion,
  following the journaling design in `IMPLEMENTATION.md` and `DESIGN.md`.
- Bound memory by chunk size plus fixed metadata, independent of partition
  size.
- Separate snapshot/compaction metrics from foreground log/apply I/O.

### Tests

- Build and install a snapshot larger than available benchmark memory if fully
  materialized; peak memory must remain bounded.
- Write continuously during snapshot build and verify apply stalls remain
  bounded and the snapshot describes one applied prefix.
- Crash at every staging, replacement, ingest, and finalization boundary.
- Run foreground traffic during build/install and enforce a p99 regression
  budget.

### Acceptance criteria

- Snapshot memory is `O(chunk size)`, not `O(partition size)`.
- Apply is not blocked for the full snapshot scan/serialization duration.
- Install remains atomic and restartable.

## Phase 7: Reduce ReadIndex amplification

### Implementation

- Add a per-partition linearizable-read coordinator that batches only requests
  queued before a new quorum barrier starts. Requests arriving after that point
  wait for the next barrier unless their ordering is otherwise proven safe.
- After the shared barrier reaches the required applied index, read each key
  from local state and complete its waiter independently.
- Bound batch size and maximum queueing delay so throughput batching does not
  create unbounded single-read latency.
- For stale reads, prefer known followers before the leader. Keep the leader as
  fallback because its existing stale-read safety check may require ReadIndex.
- Correct benchmark labels and metrics so stale reads that hit the leader are
  counted separately from follower fast-path reads.
- Treat a leader lease as a separate future project requiring an explicit
  clock, membership-transition, invalidation, and failover proof. It is not a
  prerequisite for this plan.

### Tests

- Complete a write between two read invocations while a barrier is active;
  verify the later read cannot incorrectly join the earlier barrier.
- Batch concurrent reads and verify one quorum barrier serves the eligible
  group.
- Change leadership and membership while reads queue; all results must be
  linearizable or explicitly redirected/failed.
- Isolate followers and leaders and verify stale-read behavior retains the
  existing serving-gate and freshness rules.

### Acceptance criteria

- Concurrent linearizable reads require substantially fewer quorum barriers
  than requests while preserving linearizability.
- Stale traffic uses the follower fast path whenever a suitable follower is
  available.
- Added batching delay stays within the configured budget.

## Phase 8: Remove residual CPU and allocation costs

Perform this phase only after profiles show storage, control traffic, and
transport waiting no longer dominate.

### Implementation

- Cache the durable serving gate in a shared atomic/runtime handle loaded at
  startup and updated only through the existing lifecycle fence. Preserve
  durable publication and reclamation ordering with fail-point tests.
- Avoid reading the current key for unconditional puts; no CAS decision uses
  it. Evaluate whether unconditional deletes can avoid the point read without
  violating the no-tombstone storage policy.
- Use pinned RocksDB reads where they reduce intermediate copies.
- Replace envelope payload copying with an owned shared byte buffer or a wire
  representation that can move the received frame into the decoder.
- Avoid cloning request payloads and endpoint strings on every route attempt.
- Replace the DEALER timeout table's 50 ms full scan/allocation with a deadline
  heap or timer wheel if profiles show it material at high in-flight counts.
- Coalesce DEALER wakeups so a burst does not send one wake frame per request.
- Tune RocksDB block cache, write buffers, background jobs, compaction, and
  compression only from measured workloads. Account for the large number of
  per-group column families and set an explicit memory budget.

### Acceptance criteria

- Each micro-optimization has a profile showing the removed cost.
- Large-value throughput improves without increasing peak memory unexpectedly.
- RocksDB tuning remains explicit, documented, and bounded per node.

## Rollout and feature flags

- Keep separate flags for asynchronous log durability, unified state
  durability, adaptive collection, event-driven reconciliation, and read
  batching until each has passed crash, stress, and
  soak gates.
- Emit the active performance-mode configuration at startup and in `/status`.
- Do not allow a mixed cluster to select incompatible durability or wire
  semantics. Purely local scheduling/caching modes may roll out node by node;
  protocol-visible changes require version gating.
- Define rollback before enabling each phase. A rollback must drain pending
  durability callbacks and storage work before switching modes or stopping.

## Validation matrix

After every phase:

```text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc
git diff --check
```

Before enabling a phase by default:

1. Run fail-point crash recovery at every modified durable boundary.
2. Run the linearizability, no-lost-write, leader-change, partition, restart,
   rebalance, and reclamation suites.
3. Stress the affected concurrency scenario for at least 100 iterations.
4. Run a sustained release benchmark on a durable filesystem, including a
   snapshot cycle and full background control plane.
5. Compare throughput, p50/p95/p99/max, CPU, disk utilization, fsync count and
   duration, queue depths, retries, and memory against the checked-in baseline.
6. Reject an optimization that merely transfers latency to retries, background
   traffic, recovery, or snapshot pauses.

## Recommended execution order

1. Phase 0: measurement and instrumentation.
2. Phase 1: reconciliation/control-plane amplification.
3. Phase 2: event-driven ROUTER replies and overload bounds.
4. Phase 3: asynchronous cross-group log durability.
5. Phase 4: committed/apply durable-write reduction.
6. Phase 5: routing cache and redirect behavior.
7. Phase 6: bounded snapshot production/install.
8. Phase 7: ReadIndex batching and follower-first stale reads.
9. Phase 8: profile-guided allocation, copying, and RocksDB tuning.

Phases 1 and 2 are independent after Phase 0 and may be developed in parallel.
Phase 4 depends on Phase 3's durability coordinator and measurements. Phase 8
must remain last so micro-optimization does not distract from control-plane,
fsync, transport, and snapshot costs.
