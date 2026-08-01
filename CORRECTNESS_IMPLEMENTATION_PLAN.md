# Correctness Remediation Implementation Plan

## Scope

Address the correctness and liveness issues found in the current runtime
implementation:

1. bootstrap discovery still uses immutable meta endpoints;
2. server shutdown can block the Tokio worker needed by active handlers;
3. the rebalance driver can create a move before genesis is ready;
4. load balancing does not account for the targets of in-flight moves.

The changes should preserve the existing safety model: directory authority comes
only from ReadIndex-fenced meta reads, learner admission remains plan-fenced, and
local data is reclaimed only after a linearizable placement check.

## Phase 1: Use live discovery during bootstrap

### Implementation

- Remove `Node::meta_controls`, which is derived from the immutable bootstrap
  descriptor.
- Retain the shared `AddrBook` on `Node`, or introduce a small bootstrap query
  helper that receives it.
- Change `Node::placement_seeded` to query `AddrBook::control_addrs()`. This set
  includes configured seeds and endpoints learned from authoritative directory
  refreshes.
- Prefer a local linearizable meta read when the node hosts meta. For remote
  reads, continue using `BootstrapStatus`, whose handler performs ReadIndex.
- On restart, check `node.is_initialized()` before waiting for the immutable
  genesis placement. An already initialized data group must not require the
  original placement to still be current.
- Keep the exact-placement check for a genuinely uninitialized group so it
  cannot be initialized from a conflicting descriptor.

### Tests

- Restart an initialized, designated non-meta data voter after all meta control
  endpoints have changed. Give it one current seed and a stale descriptor; its
  bootstrap must complete.
- Verify that an uninitialized group still refuses to initialize when the live
  placement differs from the descriptor.
- Verify startup with a follower seed follows live discovery to a current meta
  leader.

### Acceptance criteria

- No steady-state runtime path uses descriptor-only meta endpoints.
- An initialized partition restart does not depend on the genesis placement
  remaining unchanged.

## Phase 2: Make request draining asynchronous

### Implementation

- Replace the blocking `Condvar` drain in `ZmqServer` with async-compatible
  completion tracking, such as an atomic active count plus `tokio::sync::Notify`.
- Split shutdown into:
  1. a synchronous `stop_accepting` operation that joins only the socket-owner
     thread;
  2. an async `shutdown`/`wait_for_handlers` operation that yields while active
     handlers finish.
- Update `Node::shutdown` to await both server drains after shutting down the
  Raft runtimes.
- Keep `Drop` non-blocking. It should stop the poller and release resources, but
  must not wait for Tokio tasks from a synchronous destructor.
- Ensure the active count is decremented when a handler completes, panics, or is
  cancelled.

### Tests

- Run a node on a `current_thread` Tokio runtime, leave an inbound handler in
  flight, and verify `Node::shutdown` completes within a bounded timeout.
- Repeat with a one-worker multi-thread runtime.
- Verify an immediate restart can still reacquire the RocksDB lock after
  graceful shutdown.
- Verify handler cancellation and handler panic do not leave the drain count
  stuck.

### Acceptance criteria

- No synchronous method waits for an async handler.
- Graceful shutdown works with one Tokio worker and still drains all dispatched
  requests before storage is released.

## Phase 3: Fence rebalancing until genesis is ready

### Implementation

- Add an explicit runtime/genesis readiness gate shared with the rebalance
  driver. Keep it closed until `Node::bootstrap` has confirmed or initialized
  every descriptor data group.
- While the gate is closed, allow address-book refresh and failure evidence
  collection, but do not create data or meta placement plans and do not reclaim
  groups.
- Make readiness durable or reconstructable: after restart, already initialized
  groups should allow the gate to open without requiring the original placement
  to remain current.
- Keep plan execution enabled for a plan that was durably created before a
  process restart; the gate must distinguish unfinished cluster genesis from
  ordinary recovery.
- If a plan is unexpectedly present for an uninitialized data group, report a
  clear startup error instead of waiting until the generic bootstrap timeout.

### Tests

- Bootstrap an intentionally unbalanced valid descriptor with an Active spare;
  assert that no plan is created before every data group is initialized.
- Delay one bootstrap voter past the suspect timeout and verify failure
  detection may record suspicion but planning waits for genesis readiness.
- Restart with an existing in-flight plan and verify reconciliation proceeds
  without being blocked by the genesis gate.

### Acceptance criteria

- A newly created move always has an initialized data-Raft group capable of
  executing or reconciling it.
- Bootstrap cannot wait forever on a move created by its own background driver.

## Phase 4: Account for in-flight target placement

### Implementation

- Build an anticipated load map:
  - use `placement.voters` when no move exists;
  - use `move.target_voters` for a healthy in-flight move;
  - for an aborting move, use the currently quorum-confirmed configuration when
    available, or conservatively suppress new balancing plans until the abort
    resolves.
- Continue skipping an in-flight partition as the subject of a new proposal,
  while including its anticipated target in global source/destination load.
- Add a configurable cluster-wide migration limit. For v1, a limit of one
  healthy move at a time is the simplest deterministic throttle; if parallel
  moves are retained, cap them and calculate all targets against anticipated
  load.
- Preserve drain priority over load balancing.

### Tests

- Starting from four partitions on nodes `{1,2,3}` with node 4 empty, leave the
  first plan unresolved and verify the next proposal does not choose the same
  overloaded source and destination based on stale load.
- Simulate plan creation and finalization in varying orders and property-test
  convergence to a replica-count spread of at most one.
- Verify aborting plans neither contribute an unjustified target load nor allow
  unbounded new migrations.
- Verify every proposal still changes exactly one voter and targets only Active
  nodes.

### Acceptance criteria

- In-flight work cannot make the planner systematically overshoot a balanced
  placement.
- The number of concurrent migrations is explicitly bounded.

## Validation sequence

After each phase:

```text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc
git diff --check
```

Before completion, repeat the new runtime scenarios under stress (at least 25
iterations each) to expose election and shutdown timing races. The ignored
benchmark is not a correctness gate, but it should be run once in release mode
to ensure anticipated-load accounting and asynchronous draining do not cause a
material regression.

## Recommended implementation order

1. Fix live bootstrap discovery, because it is isolated and removes a concrete
   restart failure.
2. Make server draining asynchronous, since later runtime tests need reliable
   teardown on single-worker runtimes.
3. Add the genesis readiness fence.
4. Correct anticipated-load accounting and add migration throttling.
5. Run the full validation and stress matrix, then update `IMPLEMENTATION.md`
   and `RUNTIME_ARCHITECTURE.md` to describe the final mechanisms.
