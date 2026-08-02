# Write-Path Performance Follow-up Plan

Date: 2026-08-01

Status: in progress — the shared-transport benchmark topology is implemented;
the event-driven ROUTER candidate was removed after failing its release
performance gate. Reply-count admission and peer-control
reservation and benchmark-visible saturation counters are implemented;
reply-byte reservation and startup-validated limit configuration are
implemented; deterministic saturation coverage is in place, while stress
acceptance profiling remains pending.

### Initial release candidate trials (2026-08-01)

Five unprofiled shared-transport, event-driven ROUTER trials completed on the
workspace ext4/NVMe filesystem with the standard 16-partition, 16-client,
6,000-operation workload. All runs completed with zero admission rejections,
reply `EAGAIN`s, and reply-send failures. The medians were 189 sequential
writes/s, 1,240 concurrent writes/s (p50/p95/p99 6.40/41.40/54.09 ms), and
4,591 mixed operations/s. Concurrent-write spread was high (841--2,499
ops/s), so these candidate-only trials are diagnostic and do not meet the
release acceptance gate until compared with five rollback-mode trials in a
randomized A/B order.

The immediate rollback comparison completed with a 1,977 concurrent-write/s
median (p50/p95/p99 7.66/11.35/15.77 ms), versus the event-driven candidate's
1,240/s and 6.40/41.40/54.09 ms. The candidate therefore fails the no-more-than
5% regression gate and has been removed; the timeout-driven loop is the only
implementation. The trials were not randomized, so
they are not a final causal attribution, but the regression is large enough to
block default rollout.

### Detailed transport-boundary profile (2026-08-01)

Opt-in probes now account for DEALER queue-to-send, sender-side round trip,
reply-to-waiter resumption, ROUTER receive-to-handler scheduling, complete
handler time, handler-to-reply enqueue, reply queueing, ROUTER send, and
envelope framing. The probes preserve the wire format and do not read the clock
when `DAL_PROFILE_WRITE_PATH` is disabled.

On the standard 16-partition, 16-client, 6,000-write workload, the detailed
event-driven trial measured 2,486 writes/s with 6.31 ms mean latency. Treat that
throughput as diagnostic only: an immediately preceding event-driven trial
measured 1,715 writes/s while its WAL flush mean rose from 0.86 ms to 1.30 ms.
The direction of the transport-stage changes was stable, but filesystem
variation remains large enough that release decisions still require randomized
unprofiled trials.

| Event-driven concurrent-write stage | Client leg | Data-Raft append leg |
|---|---:|---:|
| Complete transport call | 6.291 ms | 1.240 ms |
| Remote ROUTER handler | 5.989 ms | 0.927 ms |
| Total transport cost outside handler | 0.303 ms | 0.313 ms |
| DEALER queue to send | 0.050 ms | 0.063 ms |
| ROUTER receive to handler start | 0.047 ms | 0.055 ms |
| Reply queue | 0.065 ms | 0.054 ms |
| Reply to Tokio waiter resumption | 0.051 ms | 0.049 ms |
| ROUTER send | 0.006 ms | 0.006 ms |
| Combined ZeroMQ legs and remaining bookkeeping | 0.079 ms | 0.079 ms |

Envelope encode/decode stayed below one microsecond per call. The same detailed
timeout-driven profile put reply queueing at 0.55/0.52 ms on the client/Raft
legs. Once that polling delay is removed, no individual residual transport
stage clears the plan's 0.10 ms selection threshold.

The newly exposed floor is durability. In the event-driven concurrent phase,
log append-to-callback averaged 1.457 ms, state durability wait averaged
1.504 ms, physical `flush_wal(true)` averaged 0.859 ms, and batch collection
averaged 0.224 ms. The worker grouped 32,875 logical log/state writes into
6,130 flushes (5.36 writes/flush), or 1.02 physical WAL syncs per successful
client write across all three nodes. Non-sync log/state writes remained only
20/31 microseconds and RocksDB reported no stalls.

This changes the next-work ranking:

1. keep the timeout-driven reply queue as the first deployed-path transport
   issue until the event-driven candidate passes its randomized acceptance
   gate;
2. after polling is removed, collect durability wait/flush distributions and
   queue/callback boundaries, then tune the 200 microsecond adaptive collection
   policy through isolated A/B trials;
3. attribute OpenRaft scheduling time left between the two durable phases only
   after those distributions are available;
4. defer serialization, cloning, and individual transport handoff changes;
   their measured costs are below the optimization threshold.

## Objective

Reduce write latency and raise sustained write throughput without weakening
Raft durability, reply correctness, overload behavior, or shutdown safety.
Work is ordered by the measured cost from the latest phase-separated profile:

1. remove timer-driven ROUTER reply latency;
2. bound transport concurrency and reserve progress for Raft traffic;
3. attribute and address the remaining transport cost;
4. tune WAL batching only through workload-specific A/B measurements.

Each milestone is independently testable and revertible. Do not combine the
event-driven transport change with WAL policy changes in one acceptance run.

## Measured baseline

The release benchmark used a three-node, `R=3`, 16-partition cluster over
ZeroMQ `inproc://`, with RocksDB directories on ext4/NVMe. The profiled write
load contained 1,000 sequential writes, 6,000 concurrent writes from 16
clients, and 3,008 writes in the mixed phase.

| Stage | Sequential mean | Concurrent-write mean |
|---|---:|---:|
| End-to-end operation | 5.98 ms | 7.43 ms |
| Client transport call | 5.959 ms | 7.406 ms |
| Client gateway handler | 4.916 ms | 6.518 ms |
| Raft `client_write` | 4.880 ms | 6.517 ms |
| Client ROUTER reply queue | 0.596 ms | 0.566 ms |
| Data-Raft append transport | 1.777 ms | 1.724 ms |
| Data-Raft append handler | 0.607 ms | 0.918 ms |
| Data-Raft ROUTER reply queue | 0.697 ms | 0.509 ms |
| Shared WAL flush | 0.972 ms | 0.888 ms |
| State durability wait | 1.301 ms | 1.538 ms |

The client and Raft transport gaps outside their handlers were 1.043 ms and
1.169 ms in the sequential phase. Periodic ROUTER reply draining directly
accounted for 57% and 60% of those gaps. The roughly 0.45 ms remaining on each
leg has not yet been attributed precisely.

Storage remains durability-bound rather than CPU- or RocksDB-stall-bound:

- log encoding averaged 9 microseconds;
- log/state non-sync writes averaged 89/105 microseconds;
- capacity waits stayed below 1 microsecond;
- RocksDB reported zero write-stall time;
- the concurrent durability worker grouped 32,804 writes into 6,900 flushes,
  or 4.75 writes per flush;
- apply batches contained only 1.11 entries on average, but their non-sync
  write cost is not large enough to prioritize ahead of transport or fsync.

Profiled throughput is diagnostic and must not be used as the acceptance
baseline. Acceptance comparisons use unprofiled medians from identical
release workloads.

## Correctness and operational invariants

Every milestone must preserve these rules:

1. A successful write reply is emitted only after durable Raft commit and
   durable state-machine apply.
2. Vote, follower-log, committed-state, idempotency, and applied-pointer
   durability are unchanged.
3. A reply lost after a mutation was applied must result in timeout/retry, not
   a definitive refusal. The existing idempotency protocol resolves the retry.
4. Overload may reject a request only before its handler starts. Once a
   mutation handler starts, failure to deliver its result is ambiguous and must
   be surfaced as a retryable transport loss or timeout.
5. Raft append, vote, and heartbeat traffic retains reserved capacity under a
   client flood.
6. The ROUTER and DEALER sockets remain owned by their dedicated threads; no
   socket is moved into a Tokio task.
7. Reply correlation by `request_id` and per-peer multiplexing remain intact.
8. All queues, handler counts, buffered reply bytes, and shutdown work are
   explicitly bounded.
9. Shutdown stops admission, wakes the socket thread, drains or fails accepted
   work, and never blocks a Tokio worker waiting for a handler that needs that
   worker.

## Milestone 0: Make the benchmark topology trustworthy

The current benchmark constructs a separate `ZmqTransport` for each client,
which implies separate peer socket threads. Retain that topology as a stress
dimension, but do not use it as the primary production-shaped result.

### Implementation

- Change `tests/benchmark_e2e.rs` so the primary client factory receives clones
  of one shared `ZmqTransport`.
- Add `DAL_BENCH_TRANSPORT_PER_CLIENT=1` to restore the current topology for an
  explicit comparison.
- Report transport mode, process/thread count, successful operations, retries,
  redirects, timeouts, queue-full errors, and failed reply sends.
- Keep the existing per-phase reset and RocksDB counters.
- Add `DAL_BENCH_TRIAL_ID` or print a reproducible command/configuration block
  so trial outputs can be matched without changing workload semantics.

### Tests

- Verify cloned clients share the same internal transport and peer connection.
- Verify per-client mode still produces independent transports.
- Verify sequence/idempotency state remains per client even when transport is
  shared.

### Exit criteria

- The primary concurrent benchmark uses shared transport.
- Both transport modes report errors and retries rather than silently counting
  only successful samples.
- A fresh five-trial unprofiled baseline is recorded for the shared mode.

## Milestone 1: Deliver ROUTER replies event-first

### Design

Replace the 1 ms `recv` timeout in `src/transport/router.rs` with an in-process
wake socket, analogous to the DEALER wake mechanism:

```text
Tokio handler
    -> bounded completed-reply channel
    -> nonblocking PAIR wake
    -> socket-owner poll(ROUTER input, reply wake, shutdown wake)
    -> drain replies
    -> ROUTER send
```

The wake receiver and ROUTER remain on the socket-owner thread. A small sender
socket protected by a mutex may be shared by handler tasks. Sending a wake is
nonblocking; queued replies remain the source of truth.

### Implementation

- Add a context-unique `inproc://` wake endpoint to `ZmqServer::bind`.
- Poll the ROUTER and wake receiver together instead of using
  `socket.set_rcvtimeo(1)`.
- Wake the poller after enqueueing a completed reply and when shutdown starts.
- Drain wake bytes, then drain completed replies before accepting another
  bounded request burst.
- Limit each inbound drain iteration so a continuous request stream cannot
  starve completed replies.
- Keep every socket send nonblocking. Count `EAGAIN` and other send failures;
  never pin the sole socket thread on one peer.
- The event-driven candidate was removed after failing its performance gate;
  retain the timeout-driven loop as the only implementation.
- Preserve the existing opt-in client and Raft reply-queue timers.

### Race conditions to cover explicitly

- A reply completes immediately before the poller begins polling.
- A reply completes while the poller drains wake bytes.
- Multiple completions coalesce into one wake or fill the wake socket HWM.
- Shutdown races with an empty poll, an inbound request, handler completion,
  and a queued reply.
- The remote peer disconnects after the handler applies a mutation but before
  the reply is sent.

The implementation must use the reply channel as truth and treat wake bytes as
hints, so a coalesced or dropped redundant wake cannot strand a reply.

### Tests

- Add a focused sequential `inproc://` echo benchmark with warm sockets. Its
  latency distribution must not cluster at the former 1 ms polling interval.
- Stress completions around poll transitions and assert every accepted request
  receives a reply or a documented timeout; no reply remains stranded.
- Run concurrent client and Raft traffic and verify request IDs never cross.
- Disconnect a peer around reply send and verify the caller times out/retries
  without receiving false success or a definitive mutation refusal.
- Extend `shutdown_yields_while_an_inflight_handler_finishes` to cover a poller
  blocked without inbound traffic and a handler completing during shutdown.

### Exit criteria

- Client and data-Raft reply-queue means are below 0.15 ms on the established
  `inproc://` profile, with p95 below 0.30 ms.
- Five-trial unprofiled sequential-write median improves by at least 10%, or
  the measured reply-queue reduction is fully explained by another newly
  exposed stage.
- Concurrent-write throughput does not regress by more than 5%, and its p99
  does not regress by more than 5%.
- There are zero unexpected timeouts, retries, dropped replies, or request-ID
  mismatches in the normal benchmark.

## Milestone 2: Bound handlers and protect peer-control traffic

Event-driven delivery removes latency but does not by itself fix the current
unbounded handler spawning and reply channel.

### Design

Introduce an admission budget whose permit lives from request admission until
the resulting reply is handed to the ROUTER socket. This bounds the sum of
running handlers and completed buffered replies.

Use separate admission classes:

- `peer_control`: Raft append/vote/snapshot and internal control requests;
- `client`: ClientOp and MetaQuery;
- `operator/background`: lower-rate administrative work if later measurement
  shows it needs an independent bound.

Configure a total limit and a lower client limit. The difference is reserved
for peer-control work. The bounded reply channel capacity must be at least the
total number of admitted permits so an admitted handler can always enqueue one
bounded reply.

### Implementation

- Add a nonblocking admission budget before spawning a Tokio handler.
- Transfer its permit into the queued reply and release it after send succeeds
  or the send is definitively abandoned.
- Replace the unbounded reply channel with a bounded channel.
- Bound reply bytes as well as reply count; use the envelope maximum as a hard
  upper bound and track actual queued bytes.
- Reserve peer-control permits that client traffic cannot consume.
- For overload before handler start, return a typed retryable busy response
  where the protocol supports it. Until such a wire response exists, drop
  before execution and let the caller's existing timeout/retry semantics apply.
- Never send a definitive busy/refused response after a mutation handler has
  started.
- Add configuration defaults and validation for total handlers, client
  handlers, reply count, reply bytes, and inbound burst size.
- Export current/max handler counts, reply depth/bytes, admission rejections,
  wake attempts, reply sends, send failures, and request timeouts.

### Tests

- Hold client handlers open, exceed the client limit, and verify active counts
  and buffered bytes never exceed configuration.
- While client capacity is exhausted, send Raft heartbeats, votes, and appends;
  reserved peer-control capacity must make bounded progress.
- Fill reply capacity while the poller is delayed and verify accepted handlers
  can enqueue exactly one reply without blocking a Tokio worker.
- Verify an overloaded mutation is either rejected before execution or may be
  retried after timeout, and is never applied after a definitive refusal.
- Run shutdown with full handler and reply capacity and with completions racing
  the stop signal.

### Exit criteria

- Memory and active work stay within configured limits under saturation.
- Peer-control p99 under a client flood remains below the Raft heartbeat
  interval and no healthy leader steps down because client work consumed all
  transport capacity.
- Every request is successful, explicitly rejected before execution, or times
  out and is safely retryable; there is no false success or ambiguous refusal.
- Normal-load throughput and p99 stay within 5% of Milestone 1.

## Milestone 3: Attribute the residual transport cost

After event-driven replies land, repeat the profile before selecting another
transport optimization. Add opt-in timers/counters at these local boundaries:

- client/raft call enqueue to DEALER send;
- DEALER send to ROUTER receive;
- ROUTER receive to Tokio handler start;
- handler completion to reply-channel enqueue;
- reply dequeue to ROUTER send completion;
- ROUTER send to DEALER receive;
- DEALER receive to oneshot waiter resumption;
- envelope encode/decode time and encoded bytes by message type;
- queue depth, wake calls, coalesced bursts, and scheduler handoffs.

`Instant` values are process-local. Do not infer cross-process network time by
subtracting clocks; for TCP/multi-process profiles, use sender-side round trips
and receiver-local intervals only.

Select changes only when a stage is at least 10% of end-to-end latency or 0.10
ms on a critical leg. Candidate changes, in measurement order, are:

1. coalesce DEALER wakes for request bursts;
2. batch reply-channel drains and ROUTER sends without delaying an idle reply;
3. remove avoidable envelope/payload clones and repeated serialization;
4. reduce Tokio spawn/scheduling handoffs if handler-start delay dominates;
5. replace per-call address/string work with shared immutable endpoint data;
6. replace the DEALER timeout table's full sweep with a deadline heap only if
   its scan appears in CPU or latency profiles.

### Exit criteria

- At least 90% of client and Raft transport round-trip wall time is attributed
  to named local stages plus remote handler time.
- Every selected optimization has an isolated A/B and a rollback path.
- CPU/allocation changes wait for a sampling or allocation profile; wall-time
  residuals alone are not evidence of CPU cost.

## Milestone 4: Tune durability batching from distributions

WAL sync is the remaining storage floor, but changing durability batching can
trade throughput for latency. Preserve the current unified worker and atomic
state batches while collecting enough detail to make that trade explicit.

### Measurement additions

- Record histograms, not just aggregate means, for flush duration, durability
  wait, collection delay, queue depth, queued bytes, writes per flush, and
  entries per state apply.
- Split lone, adaptively collected, size-limited, count-limited, and
  deadline-limited flushes.
- Report physical WAL syncs per successful client write and per MiB.
- Attribute log and state waiters separately while retaining the fact that
  their aggregate wall times overlap.

### A/B matrix

Run release trials on the same ext4/NVMe filesystem for:

- clients: 1, 16, 64;
- partitions: 1, 16, 128;
- values: 128 B, 4 KiB, 1 MiB;
- collection windows: 0, 50, 100, 200, 400 microseconds;
- sequential writes, concurrent writes, and 50/50 mixed traffic.

Use at least five unprofiled trials per acceptance comparison, with a separate
profiled trial for causal attribution. Randomize candidate/baseline order to
reduce filesystem and thermal drift.

### Candidate policy

- Keep immediate flush for a truly lone writer.
- Open a collection window only after concurrency is observed.
- Allow the window to close early at request/byte limits.
- Consider a queue-depth-adaptive window only if the fixed-window matrix shows
  distinct low- and high-concurrency optima.
- Do not attempt to fold a state apply into the same durability boundary as the
  log record it depends on; Raft commit and state application are causally
  separated durability points.
- Do not disable WAL, relax `sync`, acknowledge callbacks before flush, or
  weaken vote/state durability.

### Exit criteria

- A new policy must improve concurrent-write throughput by at least 10% or
  reduce physical syncs per successful write by at least 15%.
- Sequential p50/p95 and concurrent p99 may not regress by more than 5%.
- There are no new RocksDB stalls, capacity-limit violations, or latency modes
  hidden by an aggregate mean.
- Crash, restart, retry/idempotency, and rollback-mode suites pass unchanged.
- If no candidate meets all gates, retain the current adaptive 200 microsecond
  policy and record the negative result.

## Milestone 5: Validate production-shaped behavior

The in-process benchmark isolates software overhead but does not represent
network scheduling, separate runtimes, or independent page caches.

### Implementation and tests

- Add a three-process TCP benchmark with one RocksDB directory and one runtime
  per node.
- Run both idle-control-plane and full-control-plane modes.
- Test sustained load long enough to cross log purge/snapshot thresholds.
- Run a leader-change profile with cold routing, redirects, and retries.
- Saturate client ingress while monitoring Raft heartbeat/vote/append latency.
- Record CPU, RSS, thread count, socket count, disk latency, WAL bytes/s,
  physical syncs, and errors alongside application histograms.

### Exit criteria

- Event-driven reply delivery remains neutral or positive over TCP.
- Transport bounds prevent memory growth and protect Raft progress under load.
- Foreground p99 remains within its budget during snapshot and leader-change
  scenarios.
- No optimization is enabled by default until both in-process and
  production-shaped gates pass.

## Verification commands

Run formatting and the complete correctness suite after each code milestone:

```bash
cargo fmt --all -- --check
git diff --check
cargo test --all-targets --all-features -- --test-threads=1
```

Run the established release profile on a durable filesystem:

```bash
DAL_BENCH_DIR=/path/on/ext4 \
DAL_PROFILE_WRITE_PATH=1 \
DAL_BENCH_PARTITIONS=16 \
DAL_BENCH_WRITES=1000 \
DAL_BENCH_CLIENTS=16 \
DAL_BENCH_OPS=6000 \
cargo test --release --test benchmark_e2e \
  end_to_end_benchmark_three_nodes -- --ignored --nocapture
```

Run unprofiled candidate and rollback trials with identical workload settings.
For benchmark topology, compare shared transport with
`DAL_BENCH_TRANSPORT_PER_CLIENT=1`.

## Delivery sequence

1. Land benchmark transport sharing and error counters.
2. Evaluate event-driven ROUTER wakeup (completed; removed after regression).
3. Land bounded admission/replies and peer-control reservation.
4. Re-profile and optimize only the measured residual transport stage.
5. Run the WAL batching matrix; keep the current policy unless a candidate
   clears every throughput, latency, and correctness gate.
6. Validate the final candidate in the multi-process TCP and failure profiles.
