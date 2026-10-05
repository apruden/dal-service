# Correctness and Performance Fix Plan

Date: 2026-10-04

Scope: address the correctness defects and performance risks found in the current
implementation review. The maximum user value is **1 MiB (1,048,576 bytes)**,
inclusive. The correctness and bounded-work changes in phases 0–5 are
implemented. Phase 6 has three-process TCP measurements, recovery workloads,
resource counters, and paired batching trials. The batching candidate failed
the latency gate, so the production one-entry policy remains in place. The
1 MiB baseline is still too variable to support a speedup claim.

This plan follows the project's current-binary, new-cluster scope in
[Async Materialized State Implementation Plan](ASYNC_MATERIALIZED_STATE_IMPLEMENTATION_PLAN.md).
It supplements the existing implementation plans with the findings from this
review. Historical benchmark results remain historical evidence. The new
numbers below are diagnostic measurements of this working tree.

## Work order

| Phase | Status | Deliverable | Dependency |
| --- | --- | --- | --- |
| 0 | Complete | Enforce the 1 MiB value limit | None |
| 1 | Implemented | Keep search projection serialized after caller cancellation | None |
| 2 | Implemented | Reserve transport request and reply capacity for Raft | Phase 0 sizing |
| 3 | Implemented | Reject oversized complete requests before reserving a sequence | Phase 0 framing |
| 4 | Implemented | Bound search scans and projection memory | Phase 1 ownership |
| 5 | Implemented | Buffer snapshot I/O and move blocking work off Tokio | Preserve existing durability fences |
| 6 | Measurements complete; batching candidate rejected | Benchmark realistic workloads and gate further tuning | Same hardware and workload for baseline/candidate |

## Delivered implementation

- Search projection keeps an owned generation lock until its blocking pass,
  index commit, consumer checkpoint, and pruning finish. Closing a generation
  drains accepted work. The final commit and checkpoint hold a source-epoch
  fence against snapshot installation. A global four-pass semaphore bounds
  concurrent projection jobs.
- ROUTER admission reserves request and reply bytes for peer traffic alongside
  the existing handler slots. Client ceilings cannot consume those reserves;
  an encoded reply cannot exceed its admission reservation. Configuration is
  checked against legal frame sizes.
- The client validates the complete serialized operation before recording a
  pending mutation sequence. Reads and deletes use the same frame validation.
- The production TCP runtime uses a 1,000 ms Raft heartbeat interval with a
  3,000–6,000 ms election window. OpenRaft uses that interval as an append RPC
  deadline; the former 250 ms deadline repeatedly timed out legal 1 MiB
  appends on ext4 even when followers returned success. In-process test
  clusters retain their fast tuning. The production election window has a
  corresponding failover-latency tradeoff.
- Search reads one stable RocksDB source view and seeks the outbox immediately
  after the consumer checkpoint. Full rebuilds stream records into Tantivy;
  repeated keys are idempotent. Outbox maintenance runs separately from index
  reconciliation and its blocking scan remains owned through shutdown.
- Snapshot build and install use blocking tasks with source-view ownership.
  Sequential write and read paths use 64 KiB buffers and preserve checksum,
  length, flush, fsync, and generation-switch checks.
- `tests/benchmark_e2e.rs` accepts values through 1 MiB, and
  `tests/benchmark_multiprocess.rs` runs three node processes over TCP on a
  selectable filesystem with configurable partitions, clients, and value
  sizes. Its output includes successful operations, errors, retries, latency
  percentiles, throughput, and process RSS.

The benchmark harness passed small 4 KiB and 1 MiB functional runs on ext4.
The 1 MiB run used three TCP node processes, one partition, two concurrent
clients, two writes and reads per client, and completed with zero errors and
zero retries. Those numbers are not a baseline/candidate comparison, so no
speedup is claimed.
Release-mode trials now cover value sizes, concurrent search and rebuild,
automatic snapshot build, and a deliberately lagging follower. The results and
remaining measurement gates are below.

## Correctness requirements

- A search checkpoint describes every indexed document at one source prefix
  and projection epoch. Within that epoch, an older pass cannot overwrite a
  newer commit or its durable consumer checkpoint.
- Query cancellation stops waiting for work. Any accepted work that continues
  retains its serialization, lifecycle, and resource ownership until completion.
- Saturating client admission leaves bounded capacity for Raft requests and
  replies, including the traffic needed to complete those clients' writes.
- A mutation that definitely could not be sent leaves its sequence available.
  An ambiguous network outcome keeps the same pending command and sequence.
- Data-group replies retain the current majority-durable Raft guarantee and
  visible state application. State WAL durability may follow later. Snapshot
  publication and log purge retain their durable-prefix fences; meta state
  continues to await durability.
- Snapshot installation publishes a complete generation atomically. Projection
  and snapshot jobs retain the source generation for as long as they use it.

## Phase 0 — Enforce the value limit

### Implemented changes

- Set `types::MAX_VALUE_BYTES` to `1024 * 1024`. Existing client and gateway
  validation use this constant and reject larger puts before proposal.
- Add the same check to `PartitionNode::write` for direct library callers.
- Reject oversized committed puts as `RejectReason::Malformed` in state-machine
  evaluation. Rejection changes neither user data nor the sequence record;
  application still advances `last_applied` through the rejected log entry.
- Set the `ClientOp` payload ceiling to the value limit plus 64 KiB for keys
  and operation metadata. This extra space cannot increase the value limit.
- Update design documentation, the transport budget explanation, and the Raft
  maximum-value framing test. Keep the conservative one-entry replication batch
  pending the measurement and byte-budget work in phase 6.

### Verification

- An exactly 1 MiB value with a 4 KiB key passes through the client, gateway,
  Raft, and read path. Encoded request and reply bodies fit the client envelope.
- A value one byte above the limit is refused by the client; a following valid
  mutation succeeds. A direct node write is rejected without advancing apply.
- The gateway rejects a decoded oversized value even when it fits the frame's
  metadata allowance.
- A committed oversized put advances `last_applied`, preserves the previous
  value/version, and leaves its sequence reusable by a valid put.

Executed:

```sh
cargo test --locked --lib --test transport_m4 --test state_machine_m2 --test raft_group_m3
```

Result: **232 passed, 0 failed**. Tests are in
`tests/transport_m4.rs`, `tests/state_machine_m2.rs`, and
`src/transport/raft_wire.rs`.

The value check does not bound a complete request with an arbitrarily large key.
Phase 3 handles that remaining client failure path.

## Phase 1 — Preserve search job ownership through cancellation

### Defect and affected code

`SearchIndexWorker::catch_up_rebuilding` holds an async mutex while awaiting
`spawn_blocking`. Dropping the caller releases that mutex while its blocking
projection continues. Per-document writer locks allow another pass to interleave
projection, commit a newer prefix, and prune its outbox before the older pass
resumes.

The review reproduced a checkpoint claiming source 3 while indexed document
versions were `[3, 2]`. Query deadlines can trigger this path through
`src/search/coordinator.rs` and `src/runtime/search.rs`.

Primary files: `src/search/worker.rs`, `src/search/index.rs`,
`src/search/service.rs`, and search lifecycle/shutdown call sites.

### Implementation

1. Introduce an owned, tracked job per generation. Move serialization ownership
   into that job before starting source capture or projection. A caller awaits
   its result through a separate waiter; dropping the waiter cannot release
   the job's ownership.
2. Serialize the entire pass: choose a source snapshot, await source durability,
   project, commit Tantivy, publish the consumer checkpoint, and prune. Holding
   a lock only around individual document updates or the blocking closure is
   insufficient.
3. Bound active and queued work. Coalesce concurrent catch-up requests for a
   generation, while ensuring each waiter observes a checkpoint that satisfies
   its own required prefix. Keep source snapshots and writer ownership inside
   the tracked job.
4. Transfer the required service lifecycle and resource guards to accepted jobs.
   Generation removal, snapshot epoch changes, reclamation, and shutdown must
   fence new work and drain or safely invalidate existing jobs before releasing
   their storage/index resources. Define one lock order to prevent drain
   deadlocks.
5. Preserve rollback of uncommitted writer changes on failure. A failed rollback
   must fence the generation for rebuild; subsequent work must not publish those
   documents under a new checkpoint. Recheck epoch validity before publishing
   readiness or consumer progress.

### Acceptance tests

- Pause a source-2 pass after its first document, drop its waiter, and request
  source 3. The second pass cannot enter projection until the first job finishes.
  After both complete, all documents and both checkpoints agree with source 3.
- Repeat using an actual coordinator deadline and the local shard path.
- Race cancellation with projection failure, commit, consumer checkpoint write,
  and prune. Reopen the index and verify exact source-prefix contents.
- Race generation removal and snapshot installation with a paused blocking job.
  No retired generation is accessed or published as ready; shutdown drains work
  and releases permits without hanging.

## Phase 2 — Protect Raft admission under client saturation

### Defect and affected code

`src/transport/router.rs` reserves handler slots for peers, but request and reply
byte semaphores are shared. Client requests can consume every byte permit while
waiting for replication. Free peer handler slots then provide no progress.

With the new client frame limit, each client operation reserves 2 MiB of reply
capacity after rounding. At the default 512 MiB budget, **256 concurrent client
operations can exhaust all reply capacity**, below the 768-client handler cap.
Large client requests can exhaust request capacity too. The review reproduced
peer rejection with handler slots still free under the former value limit;
repeat that test using the new limit.

### Implementation

1. Add explicit client ceilings beneath total request and reply byte budgets.
   Peer requests use the total budgets; clients must also acquire their smaller
   class budgets. Reserve capacity in both directions and retain the existing
   handler reservation. Client traffic must never borrow the reserved capacity.
2. Derive minimum reserves from complete permitted peer frames, including the
   header and allocation rounding. Validate configuration at startup. A 64 MiB
   Raft append payload plus its header currently requires 65 MiB of request
   permits; a large directory/search reply requires more than a Raft ACK.
3. Retain reservations through reply enqueue/send or explicit discard. Release
   every acquired permit on admission failure, handler error, disconnected
   clients, and shutdown. Keep actual encoded reply sizes within reservations.
4. Audit libzmq receive queues, reply channels, and maximum frame sizes together
   with handler admission. Report their separate memory bounds; admission occurs
   after libzmq has assembled a frame and cannot bound that earlier allocation.
5. Expose admission failures and occupied/reserved bytes by traffic class and
   direction so load tests can distinguish capacity pressure from network loss.

### Acceptance tests

- Hold enough legal client requests to reach each client byte ceiling. Verify
  append, vote, and heartbeat handlers and replies still complete while client
  handlers remain blocked; exercise request and reply saturation separately.
- Repeat over the actual ZMQ ROUTER with small valid budgets and with sustained
  1 MiB writes on a three-node cluster. Verify replication and leader stability
  under overload, rather than checking handler-slot availability alone.
- Test impossible budget configurations, maximum supported peer frames, dead
  clients, full reply queues, and shutdown. Assert bounded queues and complete
  permit recovery.

## Phase 3 — Validate complete requests before reserving sequences

### Defect and affected code

`Client::mutate` in `src/api/client.rs` records a pending sequence before the
transport validates the encoded envelope. `route` treats local encoding errors
as retryable network failures. The unresolved mutation then blocks a different
valid mutation in the same partition.

The 1 MiB value check alone does not prevent this: a 1 MiB value plus a 64 KiB
key and operation framing exceeds the new payload ceiling.

### Implementation

1. While holding the per-partition mutation lock, determine the candidate
   sequence without recording a new pending operation. Construct and validate
   the complete encoded request, including client ID, sequence, operation,
   key/value lengths, and optional CAS version.
2. Use shared envelope size validation, then reserve the pending command and
   reuse the encoded body for retries. Avoid maintaining an approximate second
   serialization formula that can drift from the wire format.
3. Distinguish deterministic local validation failures from transport failures
   after a request may have been sent. Never clear an existing pending mutation
   merely because a later attempt failed locally or timed out.
4. Apply complete-frame validation to deletes and reads as well as puts. Return
   a direct size error for oversized keys/requests. Preserve same-command,
   same-sequence retries after ambiguous outcomes.

### Acceptance tests

- Reject a valid-size value whose key makes the encoded request too large;
  immediately send a different valid mutation on that partition and succeed.
- Cover oversized deletes, reads, CAS metadata, and requests immediately below,
  at, and above the actual payload boundary.
- Use the real encoding transport path and assert deterministic rejection
  happens before any network attempt or new pending mutation.
- Simulate timeout after commit: a different mutation stays blocked and an
  identical retry obtains the original result without applying twice.

## Phase 4 — Bound search source scans and memory

### Current cost and affected code

`Storage::search_source_snapshot` in `src/storage/rocks.rs` loads the entire
epoch's retained outbox before filtering `(checkpoint, applied]`. A lagging
consumer can therefore make a caught-up consumer scan old history on every
query. Full rebuilds also collect all user values into a `Vec`, including
non-indexable values. A 1 MiB per-value cap does not bound total partition RAM.

Primary files: `src/storage/rocks.rs`, `src/search/outbox.rs`,
`src/search/worker.rs`, `src/search/index.rs`, and `src/runtime/search.rs`.

### Implementation

1. Seek directly to the first outbox key after the checkpoint using the existing
   group/epoch/index ordering. Stop at the captured applied index or epoch
   boundary. Handle `u64::MAX`, multiple keys per log index, and term metadata
   explicitly. A caught-up consumer should not decode retained older entries.
2. Replace the owned collection of all source values with bounded chunks read
   from one stable RocksDB snapshot. Capture the state CF, epoch, and applied
   marker consistently and retain the CF handle until the scan finishes.
3. Bound incremental key deduplication and value buffers by bytes. Processing a
   repeated key across chunks is safe when every projection uses the same source
   snapshot. Never mix values from different snapshots under one checkpoint.
4. Stream full rebuilds into the owned projection job from phase 1. Keep a
   bounded writer budget and publish one final checkpoint for the captured
   source prefix. On failure, discard partial writer changes and restart safely.
5. Preserve source durability fences, epoch validation, rebuild retention floors,
   and consumer checkpoint ordering. A rebuild floor may release old journal
   entries only with the existing guarantee that its captured source covers
   them, or a failed rebuild will recapture authoritative state. Incremental
   consumer progress must follow a successful index commit.
6. Schedule expiry sweeps and outbox-bound enforcement independently of a long
   rebuild. Bound rebuild concurrency across partitions and ensure a consumer
   released from retention cannot publish stale progress or resume across a
   pruned gap.

### Acceptance tests and measurements

- Pin a large retained outbox with one lagging consumer; a second caught-up
  consumer visits no historical entries. For a small delta, decoded entries
  scale with that delta, regardless of retained history length.
- Rebuild increasingly large partitions with indexable and opaque values up to
  1 MiB. Instrument buffered bytes and confirm application source buffers stay
  within the configured bound; report RocksDB/Tantivy memory separately.
- Compare final documents and checkpoints with an authoritative full rebuild
  after updates, deletes, repeated keys, epoch changes, and cancellation.
- Hold a rebuild open while writes continue. Expiry and retention enforcement
  still run; released consumers require a full rebuild before serving again.

## Phase 5 — Buffer and offload snapshot work

### Current cost and affected code

Data and meta snapshot builders call synchronous scan/encode/fsync work inside
async methods. `SnapshotFile::build` writes each record component to a raw
`std::fs::File`; `decode_records` performs small separate reads on a Tokio file.
Snapshot installation also performs synchronous storage work. These operations
can consume runtime threads and add unnecessary I/O scheduling overhead.

Primary files: `src/snapshot.rs`, `src/partition/sm.rs`, `src/meta/sm.rs`, and
snapshot installation/CF retirement in `src/storage/rocks.rs`.

### Implementation

1. Capture the stable source and its existing durable-prefix fence under the
   required locks, then run scan/encode/fsync on a bounded blocking executor.
   Retain source ownership until that work finishes, even if its caller leaves.
2. Add buffered output, explicitly flush it, then sync the underlying file
   before publishing a snapshot. Buffer sequential decoding as well. Keep the
   streaming format, length limits, checksum, count, and trailing-byte checks.
3. Move synchronous staged install writes, flushes, and CF retirement waits off
   runtime worker threads. Bound queued records by bytes; retain lifecycle
   ownership through the final durable generation switch and cleanup.
4. Drain accepted jobs during shutdown. Cancelled or failed installation must
   leave the previous generation authoritative and reclaim temporary resources;
   a completed generation switch must remain recoverable after a crash.

### Acceptance tests and measurements

- Measure build/install throughput and foreground p95/p99 during snapshots for
  many small records and records with 1 MiB values. Check runtime responsiveness
  and I/O call counts before and after buffering.
- Repeat truncated/corrupt/checksum failure tests across buffer boundaries.
  Verify maximum-size records without buffering an entire partition.
- Repeat durable-prefix, concurrent reader, epoch, install crash-cut, purge,
  cancellation, and shutdown tests. Never expose a partial state generation.

## Phase 6 — Measure performance and gate tuning

Extend `tests/benchmark_e2e.rs`, whose current values are fixed at 128 bytes and
whose three nodes share one process with `inproc://` transport.

1. Add configurable value sizes of 128 B, 4 KiB, 64 KiB, and 1 MiB; reject
   benchmark configurations above the project limit. Include mixed reads/writes,
   concurrent searches, slow consumers, rebuilds, and snapshot transfers.
2. Retain the fast in-process smoke and add three separate processes over TCP,
   with database directories on an identified durable filesystem. Record CPU,
   storage, partition count, concurrency, and control-plane settings.
3. Record successful throughput, p50/p95/p99 latency, retries/errors, RSS,
   application buffer peaks, WAL syncs/bytes, write stalls, queue occupancy,
   search scan counts, and snapshot duration. Separate foreground metrics from
   background progress and include runs that cross snapshot thresholds.
4. Run repeated release-mode baseline/candidate trials in randomized order on
   the same workload and hardware. Set acceptable latency and memory regression
   limits from the measured baseline before accepting each optimization.
5. After correctness fixes, evaluate byte-bounded Raft batching against the
   existing one-entry policy. Include full request/membership framing and worst
   legal keys; never infer a safe entry count from value size alone.
6. Revisit ROUTER reply wakeups only if profiles still justify it. The earlier
   candidate in `WRITE_PATH_PERFORMANCE_IMPLEMENTATION_PLAN.md` failed its
   benchmark gate; require new measurements before replacing the current path.

## Completion gates

- Implement deterministic regressions for phases 1–3 before their fixes, then
  verify each fails on the reviewed implementation and passes on the change.
- Run affected tests after each phase. After all correctness/lifecycle changes,
  run `cargo test --all-targets --locked`, including the crash campaign, and
  retain the output with the revision and configuration.
- Publish performance comparisons with successful operation counts and error
  rates. Passing correctness tests alone does not establish a speedup.
- Mark each phase complete only when its implementation, regression tests, and
  relevant resource/measurement gates are satisfied.

## Verification of the combined implementation

On the working tree based on `9573091` (with uncommitted project changes),
`cargo test --all-targets --locked` passed, including the 100-seed crash
campaign, runtime integration tests, the search cancellation and snapshot
epoch regressions, transport saturation, and the client sequence regression.
Output is retained at `/tmp/dal-all-fixes-production-timing-tests.log` in this
workspace session. `cargo fmt --all -- --check` and `git diff --check` passed.

The ignored three-process TCP harness also passed on ext4 at `/var/tmp`:

```sh
DAL_BENCH_DIR=/var/tmp DAL_BENCH_PARTITIONS=1 DAL_BENCH_CLIENTS=2 \
DAL_BENCH_WRITES=2 DAL_BENCH_VALUE_BYTES=1048576 \
DAL_BENCH_TRIAL_ID=final-max-value \
cargo test --locked --test benchmark_multiprocess -- --ignored --nocapture
```

It completed four writes and four reads, with zero errors and zero retries.
This was a small debug-build functional run; the later release measurements
are reported below.

## Phase 6 measurements and recovery finding (2026-10-04)

The release harness now samples per-process RSS and CPU ticks, `/status`
materialization and search-outbox bounds, Raft snapshot indexes, and, in
profiling mode, RocksDB WAL syncs, bytes, and stall time. It runs optional
concurrent search, index rebuild, and a paused follower that must recover from
a snapshot. Failed runs retain node files and print final cluster status.
`/status` exposes ROUTER admission and reply-send counters for diagnosis.

The host has an Intel i7-8650U (4 cores, 8 threads), 15 GiB RAM, and Linux
6.18.54-1-lts. Database directories were on `/var/tmp` on ext4/NVMe.
The clean-cluster samples used three release node processes over TCP, two
partitions, four clients, and 50 writes plus 50 linearizable reads per client.
Three trials per size ran in randomized size order with profiling disabled.
All 12 runs completed 200 writes and 200 reads with zero errors and retries.
These samples preceded the no-leader client retry fix; that fix affects only
redirects during recovery.

| Value | Median operations/s (range) | Median write p95 ms (range) | Highest sampled node RSS |
| --- | ---: | ---: | ---: |
| 128 B | 346 (343–1,009) | 31.5 (9.5–37.5) | 25.4 MiB |
| 4 KiB | 340 (336–1,001) | 32.2 (8.9–32.9) | 26.9 MiB |
| 64 KiB | 314 (157–418) | 45.6 (30.7–94.2) | 52.8 MiB |
| 1 MiB | 56.9 (15.4–97.5) | 385.0 (97.9–895.6) | 324.1 MiB |

The spread, especially at 1 MiB, is too large to set a credible latency or
throughput acceptance limit. No speedup or tuning decision follows from these
samples. Profiling-mode diagnostics counted 720 WAL syncs across three nodes
for the 4 KiB trial and 1,048 for the 1 MiB trial, with zero reported RocksDB
stall time; those counters cover the sampled foreground interval and profiling
adds overhead.

A 128 B workload with one partition, 6,000 writes, and 6,000 reads crossed
OpenRaft's 5,000-log snapshot threshold. All operations succeeded, and all
three nodes reported snapshot indexes around 5,000. Pausing one follower until
5,500 writes forced a snapshot transfer after it resumed. Repeated pre-fix
trials ended near write 5,500 with `no candidate served` errors even though the
replicas later reported healthy, caught-up state. Client traces showed rapid
leader-unknown redirects exhausting 16 rounds during recovery. The client now
continues leader-unknown retries for an eight-second window, with a
100 ms delay between those rounds; known-leader routing retains its existing
fast path. A deterministic two-second redirect test covers this behavior.

Three release trials of the paused-follower workload then completed all 6,000
writes and 6,000 reads with zero errors and retries. Follower snapshot catch-up
took 5.0–6.2 seconds. The two trials recording maximum latency saw individual
write/read waits of 4.9–5.6 seconds; p99 remained below 32 ms because only a
few requests overlapped recovery. A combined trial with the paused follower,
800 concurrent searches, and an index rebuild also completed 6,000 writes and
6,000 reads with zero errors and retries. Rebuild activation took 22.1 seconds.

The three-process harness also exposed two correctness defects during its
smoke runs. Search projection panicked when an ordinary value shorter than
eight bytes reached FlatBuffers identifier checking; a length guard and focused
test now cover it. Multi-partition process bootstrap compared rotated genesis
voters by vector order against sorted committed voters; both local and remote
readiness checks now compare sets, with a normal-suite regression test.

Logs for these initial measurements and recovery diagnostics are retained in
`/var/tmp/dal-phase6-20261004/`. At this point the harness still needed paired
tuning trials, buffer and queue measurements, search scan counts, and direct
snapshot timing. The continuation below records those measurements and the
decision to keep one-entry Raft replication batching and the timeout-driven
ROUTER loop.

The combined working tree passed `cargo test --all-targets --locked`: 341 tests
passed, none failed, and the two explicit benchmarks stayed ignored. This
includes the 100-seed leader-crash campaign. Output is in
`/var/tmp/dal-phase6-20261004/all-targets.log`. `cargo fmt --all -- --check`
and `git diff --check` also passed.

## Phase 6 continuation: resource gates and batching decision (2026-10-04)

Commit `94d89a3` was pushed to `origin/main` before these measurements. The
host, ext4/NVMe filesystem, and three-process TCP topology were the same as
above. The harness now reserves all nine listening ports together and retains
node logs on a bootstrap failure. One pre-fix trial aborted before measurement
with `Address already in use`; its retained log identified the port collision.
Aborted trials are excluded from the tables below.

Three longer clean-cluster 1 MiB baseline trials each completed 800 writes and
800 reads with zero errors and retries. They produced 53.4, 23.8, and 59.7
operations/s, with write p95 of 372, 623, and 278 ms. Longer trials therefore
did not remove the spread. The host uses the `powersave` CPU governor, and
measured I/O pressure varied considerably between nearby trials. Throughput
numbers from this host are diagnostic, not an acceptance target.

The batching experiment offered up to 32 entries with an 8 MiB encoded-RPC
target, then eight entries with 4 MiB and 2 MiB targets. It checked the full
encoded AppendEntries request, including the largest legal client key and a
membership entry, and used OpenRaft's `PayloadTooLarge` hint to split an
oversized offer. A final variant avoided measuring and then reserializing
single-entry requests. The production binary was compared with the saved
`94d89a3` release binary in alternating order. Each trial used a fresh
three-node cluster and zero client errors or retries.

| Workload | One-entry baseline | Final eight-entry, 2 MiB candidate | Decision |
| --- | ---: | ---: | --- |
| 4 KiB, follower paused for 1,000 of 1,200 writes: catch-up | 3.17–3.38 s (two adjacent trials) | 0.93–0.94 s | Catch-up improved |
| Same workload: write p95 | 10.9–11.0 ms | 19.8–25.6 ms | Latency regression |
| 1 MiB, follower paused for 300 of 400 writes: catch-up | 6.4–19.4 s (nearby trials) | 5.9–10.4 s (three trials) | Too variable for a gain claim |
| Same workload: adjacent write p95 | 92.4 ms | 230.7 ms | Latency regression in one pair |

The 4 MiB byte-target candidate also had two 1 MiB catch-up trials at
13.6–15.1 seconds, compared with nearby baseline trials at 6.4–8.7 seconds.
The acceptance guard was zero errors and no more than 20% paired regression in
write p95 or peak node RSS while improving catch-up. The final candidate
exceeded the p95 guard in both value classes. The one-entry production setting
and existing ROUTER reply loop therefore remain unchanged. No speedup is
claimed. Paired logs, including CPU ticks, host I/O pressure, and sampled RSS,
are in `/var/tmp/dal-phase6-20261004/` under `small-*` and `large-*` names.

The final one-entry binary then ran two profiled 1 MiB search workloads. Each
used two partitions, 200 writes, 200 reads, 20 searches, and a concurrent
rebuild. Both had zero errors and retries. Waiting for all active projections
to catch up verified **201 hits** for indexable values and **one hit** for
opaque values (the seeded search document). The indexable rebuild activated in
1.05 s; the opaque rebuild in 0.67 s. Completed full-source scans visited
7–25 records per node because the rebuild began early; incremental outbox
scans visited 207–219 entries per node to cover the remaining writes. The
largest application-owned source row observed was 2,097,252 bytes, including
the RocksDB-encoded value and its decoded copy. WAL queues peaked at roughly
3–4 MiB, and RocksDB reported zero write-stall time in these runs.

RocksDB's approximate memtable peak was 238–245 MB per node, with about 13 KiB
of table-reader memory reported and no cache bytes returned by its memory
consumer API. Four loaded Tantivy generations had a combined configured writer
budget of 200 MB per node; Tantivy does not expose actual writer allocation
through the API used here. Peak process RSS was 384,672 KiB in the indexable run
and 353,492 KiB in the opaque run. The source-row and RocksDB numbers describe distinct
known components; RSS is the combined process measure. Profiling adds overhead,
so these figures do not replace the unprofiled throughput baseline.

A profiled recovery workload crossed the 5,000-entry snapshot threshold while
running 6,000 writes, 6,000 reads, 800 searches, a rebuild, and a paused
follower. It completed with zero errors and retries. The follower's snapshot
catch-up took 5.54 s; maximum observed snapshot build and install work took
36.75 and 77.34 ms respectively. Rebuild activation took 23.14 s. The highest
sampled node RSS was 75,128 KiB and the exact WAL reservation peak was 1,635
bytes. A separate 512-record stream of 1 MiB values built at 166.7 MiB/s and
decoded at 193.4 MiB/s. Installing the same 512 MiB through the staged
RocksDB generation and durable pointer switch took 4.61 s (111.0 MiB/s);
the build for that install took 2.84 s (180.6 MiB/s). The largest snapshot
record buffer was 1,048,584 bytes. The existing checksum, corruption,
generation-switch, and crash-cut tests remain in the normal suite.

Phase 6's measurement and tuning decision are complete. Future optimization
work needs a quieter host or paired trials with host I/O pressure controlled;
the current measurements support rejecting batching, not predicting a stable
throughput gain. The new status fields report exact application reservation
peaks, completed scan counts, snapshot work durations, approximate RocksDB
internal memory, and Tantivy's configured writer budget. They do not claim
allocator-level attribution of all process RSS.

The continuation working tree based on `94d89a3` passed
`cargo test --all-targets --locked`: **341 passed, zero failed, four ignored**
(the four explicit measurements). The 100-seed leader-crash campaign passed in
309.65 seconds. Output is retained at
`/var/tmp/dal-phase6-20261004/all-targets-continuation.log`.
`cargo fmt --all -- --check` and `git diff --check` passed.
