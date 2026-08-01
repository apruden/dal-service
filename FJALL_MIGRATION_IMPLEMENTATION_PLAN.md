# Fjall 3 Storage Migration Implementation Plan

## Purpose

Replace the node-local RocksDB backend with Fjall 3 while preserving DAL's
Raft durability, crash recovery, serving fences, online group lifecycle, and
snapshot correctness.

This is a storage-format migration and a snapshot-architecture change, not a
crate-name substitution. The implementation must retain the existing rule that
an acknowledged Raft write is durable, and it must never expose a partial state
machine after snapshot installation or group reclamation.

The target version for the initial implementation is Fjall 3.1.8. Pin the
version through `Cargo.lock` and review subsequent upgrades deliberately.

## Outcomes

At completion:

1. the production binary does not link RocksDB or `librocksdb-sys`;
2. each node uses one Fjall `Database`;
3. node-local authority records live in one stable `local` keyspace;
4. every Raft group owns one log keyspace and one active state-keyspace
   generation;
5. Raft truncate and purge are crash-safe without a range-delete primitive;
6. snapshots use bounded memory, a file-backed transfer format, an inactive
   state generation, and a small atomic activation batch;
7. restart recovery removes or resumes orphaned work deterministically;
8. an explicit upgrade procedure handles existing RocksDB data; and
9. performance and fault-injection results justify the production switch.

## Non-goals

- Do not change the replicated command encoding, client semantics, placement
  algorithm, or Raft membership protocol.
- Do not use Fjall transactions for state-machine decisions. DAL already
  evaluates commands deterministically and needs atomic write batches, not
  storage-level read-modify-write transactions.
- Do not treat Fjall's in-process MVCC `Snapshot` as the transferable Raft
  snapshot. It is the consistent source view used to build that artifact.
- Do not make snapshot generation keyspaces part of replicated state. Active
  generation selection is node-local storage metadata.
- Do not claim that replacing RocksDB removes blocking I/O from Tokio workers.
  Fjall `SyncAll`, ingestion, iteration, recovery, and compaction-facing work
  still belong on the bounded storage executor described in the performance
  plan.

## Current implementation surface

RocksDB is directly coupled to four production files:

- `Cargo.toml` and `src/error.rs` select and expose the engine;
- `src/storage/rocks.rs` owns database open, CF lifecycle, local records,
  scans, and snapshot replacement;
- `src/storage/batch.rs` implements atomic state-machine application; and
- `src/partition/log_store.rs` implements OpenRaft log storage.

Backend-specific names also appear in `RocksLogStore`, `RocksStateMachine`,
documentation, tests, `.cargo/config.toml`, and operational comments. Most of
the service already calls the `Storage` facade, so the data, meta, transport,
and runtime layers should not need semantic rewrites.

The current snapshot implementation scans the complete state CF into a
`Vec<(Vec<u8>, Vec<u8>)>`, serializes it into `Cursor<Vec<u8>>`, and installs
it using a full-CF delete-and-reinsert write batch. That is correct for small
partitions but has memory, pause-time, and batch-size costs proportional to the
partition size.

## Target on-disk layout

Use one Fjall database per node with these keyspaces:

```text
local
  storage/format                         -> { engine: "fjall", schema: 1 }
  group/<group>/active_generation        -> GenerationRecord
  group/<group>/snapshot_install         -> SnapshotInstallRecord
  group/<group>/current_snapshot         -> CurrentSnapshotRecord
  identity, registration, admission, serving, and other existing local keys

log_<group>
  vote, committed, last_purged, last_log, and Raft entry keys

state_<group>_g<generation>
  replicated business/meta records
  raft_applied
```

Generation names must be deterministic, bounded to Fjall's keyspace-name
limit, and unambiguous across restart. Use a monotonically allocated `u64`
generation stored in `local`; do not derive uniqueness only from an OpenRaft
snapshot ID string.

`GenerationRecord` should include at least:

```text
generation
keyspace_name
created_by_snapshot_id: optional
```

`SnapshotInstallRecord` should include at least:

```text
snapshot_id
generation
keyspace_name
last_log_id
membership digest or encoded membership
format version
status: Receiving | Verified | Installed
```

The active-generation record is the only durable selector for state reads and
applies. A generation not named by that record is never served.

## Correctness invariants

Every phase must preserve these invariants:

1. A Raft log-flush callback fires only after all covered entries are durable.
2. Votes, committed markers, log bounds, state mutations, and applied metadata
   use `PersistMode::SyncAll` before success is returned unless a separately
   proven durability coordinator covers the operation.
3. Applying business mutations and advancing `raft_applied` is one atomic
   Fjall batch.
4. Log readers never expose entries below the durable purge bound or above the
   durable last-log bound, even when obsolete physical records remain.
5. A snapshot source contains state and applied metadata from one Fjall MVCC
   view.
6. Snapshot activation exposes either the previous complete generation or the
   new complete generation, never an incomplete generation.
7. A crash before generation activation leaves the old generation active. A
   crash after activation recovers the new generation as active.
8. Node-local authority records are never copied into, removed with, or
   replaced by a replicated state snapshot.
9. Reclamation records `NonServing` durably before deleting log or state
   keyspaces.
10. A missing active keyspace is corruption. Startup must not silently create
    an empty keyspace for an existing active-generation record.
11. Storage I/O errors that poison Fjall stop the affected node/storage runtime;
    later calls must not be treated as successful recovery in the same process.
12. Existing RocksDB data is never opened as an empty Fjall database by
    accident.

## Phase 0: Freeze the baseline and migration policy

### Implementation

- Record the current Rust version, RocksDB crate version, database options,
  test results, release benchmark, snapshot format, and representative data
  sizes.
- Capture release-mode baselines on a real filesystem for:
  - sequential and concurrent Raft writes;
  - state-machine apply throughput;
  - linearizable and stale reads;
  - follower restart and log recovery;
  - log purge and truncate at several log lengths;
  - snapshot build/install at 1 GiB and a larger representative partition;
  - process RSS, disk usage, compaction interference, and restart time.
- Decide and document which existing-data upgrade modes are supported:
  - destructive reset for development/pre-production;
  - offline per-node RocksDB-to-Fjall conversion; and/or
  - replacement by new node IDs followed by normal Raft rebalancing.
- Default to an offline converter when deployed data must be preserved. Do not
  assume that wiping and restarting an existing voter is authorized by the
  amnesia and admission rules.
- Inventory all RocksDB-specific operational scripts, data-directory checks,
  metrics, documentation, and deployment manifests.

### Tests and evidence

- Run the full existing validation sequence before changing dependencies.
- Preserve one representative RocksDB fixture for converter and rollback
  tests. It must contain the meta group, a data group, local authority state,
  purged logs, an installed snapshot, and a reclaimed group.
- Store baseline benchmark output with filesystem and hardware metadata.

### Acceptance criteria

- The pre-migration build and test results are reproducible.
- The project has an explicit existing-data policy; no deployment can silently
  fall through to an empty Fjall directory.
- Performance comparison thresholds are chosen before Fjall measurements are
  observed.

## Phase 1: Introduce the Fjall database and lifecycle registry

### Implementation

- Add `fjall = "3.1.8"` and initially retain RocksDB only for the migration
  utility or an explicitly temporary comparison feature.
- Rename the engine error variant from `Rocks` to a backend-neutral `Storage`,
  with conversion from `fjall::Error`.
- Replace `src/storage/rocks.rs` with `src/storage/fjall.rs`, while retaining
  the public `Storage` API where its semantics remain valid.
- Before opening Fjall, preflight the directory. Permit an empty directory or a
  directory containing an fsync-durable DAL sidecar marker that explicitly
  names Fjall and the storage schema. Refuse every non-empty unmarked directory,
  including a RocksDB directory, before `Database::builder(...).open()` can
  create Fjall files alongside it.
- Open one `fjall::Database` and one stable `local` keyspace. Mirror and
  validate the storage-format marker inside `local`; the converter must create
  both markers.
- Add a lifecycle registry protected by an application-level `RwLock`:

  ```text
  GroupId -> { log: Keyspace, active_generation: u64, state: Keyspace }
  ```

  Reads clone handles under the read lock. Group creation, activation,
  reclamation, and deletion use the write lock. This prevents the
  check-then-create race caused by Fjall's combined create/open `keyspace()`
  API.
- On open, enumerate keyspaces and rebuild the registry only from durable
  active-generation records. Treat malformed names, duplicate active records,
  and missing active/log keyspaces as corruption.
- Make `ensure_group` generation-aware:
  1. create a log keyspace and a fresh state generation;
  2. verify both can be reopened;
  3. publish the active-generation record using a `SyncAll` batch; and
  4. add the handles to the in-memory registry.
- If creation crashes before publication, startup sees inactive orphan
  keyspaces and deletes them. If publication exists, startup must find both
  keyspaces.
- Keep the current reclamation order: write `NonServing` durably, remove the
  registry entry, delete active/staging state and log keyspaces, then remove
  obsolete generation metadata while retaining the durable serving fence.
- Route all backend work through helper methods; higher layers must not call
  `Database::keyspace`, `Keyspace::insert`, or `Database::persist` directly.

### Configuration

- Add explicit, bounded configuration for Fjall block cache, worker threads,
  maximum journal size, per-log-keyspace memtable size, and per-state-keyspace
  memtable size.
- Calculate and document the worst-case memory bound across
  `2 * hosted_groups + local + staging generations`. Do not multiply Fjall's
  default 64 MiB memtable target blindly across hundreds of keyspaces.
- Start with LZ4 compression and no key-value separation. Enable key-value
  separation only after DAL-specific large-value benchmarks show a benefit.

### Tests

- Port the identity, registration, admission, group lifecycle, reclaim, and
  immediate reopen tests in `tests/storage_m1.rs`.
- Add concurrent open/read/drop tests that exercise the lifecycle registry.
- Kill a subprocess after keyspace creation but before active-generation
  publication; restart must keep the old state or clean the orphan.
- Kill a subprocess after publication; restart must find the complete group.
- Hold a cloned keyspace handle while deleting the group and verify it cannot
  recreate or serve the group through a stale handle.
- Verify a second process cannot open the same Fjall database.

### Acceptance criteria

- Dynamic group creation, restart, re-admission, and reclamation preserve the
  existing serving and amnesia rules.
- There is no create-on-read path.
- Orphan cleanup is deterministic and idempotent.

## Phase 2: Port durable local and state-machine writes

### Implementation

- Add one internal durable-batch constructor:

  ```rust
  db.batch().durability(Some(PersistMode::SyncAll))
  ```

- Implement single-key durable puts/deletes as one-item durable batches. Do not
  perform a visible `insert` followed by a separate `persist` call.
- Port `record_verified_learner_admission` as an atomic batch in `local`.
- Port `apply_state` and `apply_raft` to Fjall batches against the current state
  generation. Preserve the existing failpoints immediately before commit and
  after a successful durable commit.
- Continue deduplicating coalesced state mutations before constructing the
  batch. Avoid depending on backend-specific ordering for repeated operations
  on the same key in one batch.
- Convert returned Fjall guards/slices into owned bytes before retaining them
  beyond the immediate read; otherwise a small logical value may pin a larger
  cached block.
- Replace full-state scans used outside the new snapshot path with generation
  iterators and explicit error handling for key/value guards.
- Keep synchronous durability for correctness in this phase. Reconcile the
  performance plan's future cross-group durability coordinator with Fjall only
  after the direct `SyncAll` implementation passes all crash gates.

### Tests

- Run all state-machine unit and property tests unchanged at the public API.
- Retain the before-commit and after-commit prefix-recovery tests.
- Add subprocess `SIGKILL` loops around state apply and admission batches; a
  restart must observe the complete old or complete new batch.
- Add a cross-keyspace batch recovery test covering a staged generation record
  and `local` metadata.
- Inject a Fjall write/persist failure where practical and verify the node does
  not continue issuing successful storage acknowledgements after poison.

### Acceptance criteria

- Business mutations, client sequence records, membership, and applied state
  recover to one common Raft prefix.
- No acknowledged state or authority write is lost across process restart.
- Higher layers remain unaware of Fjall guard and batch types.

## Phase 3: Replace Raft range deletion with logical log bounds

### Data model

Each `log_<group>` keyspace stores:

```text
KEY_VOTE
KEY_COMMITTED
KEY_PURGED       -> Option<LogId>
KEY_LAST_LOG     -> Option<LogId> for the highest physically relevant entry
ENTRY_PREFIX + big-endian index -> encoded Entry
```

`KEY_PURGED` is the inclusive lower deletion watermark. `KEY_LAST_LOG` is the
inclusive upper visibility watermark. Physical records outside
`(last_purged.index, last_log.index]` are garbage and must never affect reads or
`LogState`.

### Implementation

- Rename `RocksLogStore` to `FjallLogStore`, or preferably to the neutral
  `LogStore` if a second production backend is not retained.
- Port vote and committed-state reads/writes to point operations and one-item
  `SyncAll` batches.
- Implement append as one durable batch that writes every entry and updates
  `KEY_LAST_LOG` to the final entry. Fire `LogFlushed` only after commit
  succeeds.
- Make log-range reads intersect the caller's range with both durable logical
  bounds before opening the Fjall range iterator.
- Use one Fjall database snapshot for each logical log read so its purge bound,
  last-log bound, and entry iterator come from one visible database sequence.
- Implement `get_log_state` from `KEY_PURGED` and `KEY_LAST_LOG` in one such
  view; do not scan obsolete suffix entries to decide the logical last log.
- Implement truncate in two stages:
  1. determine the retained predecessor `LogId` or purge point and publish the
     new `KEY_LAST_LOG` durably;
  2. scan and delete the now-invisible suffix in bounded garbage-collection
     batches.
- Implement purge in two stages:
  1. advance `KEY_PURGED` durably and adjust `KEY_LAST_LOG` if every remaining
     physical entry is purged;
  2. scan and delete the now-invisible prefix in bounded garbage-collection
     batches.
- Correctness depends only on the logical bounds. A crash during physical GC
  may waste disk but cannot resurrect a log entry.
- Serialize append, truncate, purge, and their physical GC for a group. A GC
  task must not delete an entry that was re-appended after truncation.
- Physical GC batches may use buffered durability because durable logical
  bounds already make the records unreachable. Semantic bound changes and
  entry appends always remain `SyncAll`; a crash may resurrect garbage on disk,
  never in the logical log.
- On restart, schedule bounded cleanup for records outside the logical bounds.
  Apply backpressure and disk-space alarms if cleanup cannot keep up.
- Handle `0`, `u64::MAX`, inclusive/exclusive caller bounds, empty logs, and a
  fully purged log without arithmetic overflow.

### Tests

- Add a reference-model property test generating append, truncate, purge,
  restart, and range-read operations.
- Crash after publishing a truncate bound but before deleting any suffix;
  restart must hide the complete suffix.
- Crash after deleting only part of a suffix or prefix; restart must expose the
  same logical log as before the crash.
- Truncate, re-append a shorter suffix, crash, and verify an older longer suffix
  never reappears.
- Purge all entries and verify `last_log_id == last_purged_log_id`.
- Verify the append callback is not fired when durable batch commit fails.
- Benchmark purge and truncate at increasing log lengths and verify each GC
  batch has a configured item/byte bound.

### Acceptance criteria

- No production path requires Fjall range deletion.
- Truncate and purge are logically constant-size durable decisions followed by
  bounded, restartable physical cleanup.
- Stale physical entries cannot affect OpenRaft recovery or reads.

## Phase 4: Introduce file-backed, versioned snapshot artifacts

### Snapshot format

Define a DAL-owned snapshot format independent of RocksDB and Fjall internals:

```text
header:
  magic
  format_version
  group_id
  snapshot_id
  last_log_id
  encoded membership
  encoding/schema version

chunks:
  ordinal
  encoded byte length
  record count
  first key / last key
  checksum
  sorted framed key/value records

trailer:
  total records
  total logical bytes
  whole-snapshot checksum
```

Exclude `raft_applied` from the ordinary record stream. The receiver writes the
authoritative applied record during the final activation batch.

### Implementation

- Replace `Cursor<Vec<u8>>` as `SnapshotData` with a file-backed type satisfying
  OpenRaft's `AsyncRead`, `AsyncWrite`, `AsyncSeek`, `Unpin`, `OptionalSend`, and
  `'static` requirements. The wrapper should own cleanup of its temporary file
  and support retransmission by byte offset.
- Add a configurable snapshot work directory, chunk target, maximum concurrent
  builds/installs, and free-space threshold.
- Build a snapshot by:
  1. briefly acquiring the group state-view/lifecycle lock;
  2. cloning the active state keyspace handle;
  3. opening a Fjall database snapshot;
  4. reading `raft_applied` from that same view;
  5. releasing the group lock;
  6. iterating sorted records into a temporary file using fixed-size buffers;
  7. fsyncing the completed file and parent directory; and
  8. publishing `CurrentSnapshotRecord` durably.
- Keep the Fjall MVCC snapshot only while producing the file. Network transfer
  reads the immutable file and therefore does not pin old LSM versions for the
  duration of a slow peer transfer.
- A single record may exceed the target chunk size. Define the true memory
  bound as fixed buffers plus the configured maximum record size, and reject a
  record above the service limit before allocation.
- Retain only a bounded number of completed snapshot files per group and delete
  them after OpenRaft no longer references them.
- Run encoding, checksumming, file I/O, and Fjall iteration on the bounded
  storage executor, not a Tokio worker.

### Mixed-version compatibility

- Give the new decoder explicit support for the current legacy bincode snapshot
  and the new framed format during the migration window.
- Do not let a new leader send the framed format to an old binary that can only
  decode the legacy format.
- Choose one rollout strategy before implementation:
  - deploy a RocksDB compatibility release that can receive both formats and
    advertises support, then enable framed sending only when every voter is
    capable;
  - add an operator-controlled `snapshot_send_format=legacy|framed` gate and
    keep `legacy` until the cluster is fully upgraded; or
  - perform a coordinated offline conversion and restart with no mixed-version
    interval.
- Keep the snapshot envelope version independent of the storage-engine marker.

### Tests

- Golden-test header, chunk, trailer, and legacy decoding.
- Corrupt every field and chunk boundary; installation must fail before
  activation.
- Verify retransmission and seek from arbitrary valid offsets.
- Build snapshots while applies continue and verify state plus applied metadata
  always represent one prefix.
- Measure peak heap while snapshotting progressively larger partitions; it
  must remain bounded by configured buffers and maximum record size.
- Fill the snapshot disk threshold and verify explicit backpressure/error, not
  an out-of-space partial artifact.

### Acceptance criteria

- Snapshot memory does not grow with partition size.
- The apply lock is not held while scanning, encoding, or transferring the
  complete partition.
- Snapshot files are checksummed, seekable, fsync-durable, and bounded in
  retention.
- Mixed-version behavior is explicit and tested.

## Phase 5: Install snapshots through inactive state generations

### Implementation

- `begin_receiving_snapshot` creates a unique temporary file and does not touch
  the active state generation.
- After OpenRaft finishes the transfer, validate the header, group, snapshot
  ID, size limits, and complete checksum before creating an install record.
  Require the file's last log ID and membership to match the `SnapshotMeta`
  supplied by OpenRaft exactly.
- Under the group lifecycle lock, allocate a new generation and persist a
  `Receiving` install record before bulk loading it.
- Create `state_<group>_g<new>` and stream the verified file through
  `Keyspace::start_ingestion()` on the storage executor:
  - require strictly increasing keys;
  - verify each chunk before feeding its records;
  - enforce key, value, record-count, and logical-byte limits; and
  - never write `raft_applied` from the untrusted record stream.
- For the first implementation, restart the installation from a clean inactive
  generation after any process crash before activation. This is bounded and
  simple even though it may retransmit or re-ingest work.
- After ingestion finishes:
  1. reopen/inspect the staging keyspace;
  2. verify record count, first/last key, and logical checksum;
  3. mark the install `Verified` durably;
  4. acquire the state-machine exclusive lock and reject a snapshot older than
     the currently applied state;
  5. commit one cross-keyspace `SyncAll` batch that writes `raft_applied` into
     the staged state keyspace, changes the active-generation record, and marks
     the installation `Installed`;
  6. update the in-memory active handle before releasing the lock; and
  7. return success to OpenRaft so it may apply the log suffix and later purge
     its old prefix.
- Do not rely on keyspace creation/deletion being part of the activation batch.
  Atomicity comes from the small active-generation pointer update.
- Delete the prior state generation asynchronously after active snapshots and
  cloned handles release it. Keep cleanup idempotent across restart.
- On startup:
  - `Receiving` or `Verified` with the old active pointer means delete the
    inactive generation and restart installation;
  - `Installed` with the new active pointer means use the new generation and
    clean the old one;
  - an active pointer to a missing generation is fatal corruption; and
  - unreferenced generation keyspaces are orphaned and safe to remove after
    validating they are not active.

### Optional resumable-chunk extension

- Add resumability only after the clean-restart implementation is stable.
- Finish each independently sorted chunk ingestion before writing a durable
  chunk-complete marker into the inactive keyspace.
- If ingestion completes but its marker does not, re-ingesting the identical
  chunk must be proven idempotent before enabling resume.
- Activation still requires every chunk marker and the whole-snapshot digest;
  chunk markers never make a generation active.

### Crash matrix

Test a hard process stop at each boundary:

```text
before install record
after Receiving record
after keyspace creation
during every ingestion chunk/table boundary
after ingestion finish
after verification
before activation batch
after activation batch, before in-memory handle update
after handle update, before old-generation deletion
during old-generation deletion
after deletion, before install-journal cleanup
```

For every case, restart must select one complete generation, report one applied
state and membership, and either finish or remove all orphaned work.

### Acceptance criteria

- No snapshot-install crash exposes an empty, mixed, or partial state machine.
- Activation is a small `SyncAll` batch independent of snapshot size.
- Peak installation memory is bounded by decoding and ingestion buffers plus
  one maximum-size record.
- Old-generation disk reclamation is observable and eventually completes.

## Phase 6: Convert or replace existing RocksDB nodes

### Offline converter

If deployed data must survive in place, add a separate
`dal-storage-migrate` utility that temporarily links both engines. The
production service must not retain the RocksDB dependency after the migration
window.

The converter must:

1. require the DAL process to be stopped and acquire the RocksDB lock;
2. refuse identical source/destination paths and any non-empty unrecognized
   destination;
3. create Fjall in a new sibling temporary directory;
4. copy the default CF into `local` without changing encoded values;
5. map each log CF into `log_<group>` and derive/verify `KEY_LAST_LOG`;
6. map each state CF into generation zero and create its active-generation
   record;
7. preserve identity, registration, admission, serving, bootstrap, vote,
   committed, purge, state-machine, membership, and client-sequence records;
8. copy sorted records using bounded ingestion rather than whole-CF
   materialization;
9. persist a conversion journal and source manifest so an interrupted run can
   restart safely;
10. close and reopen the Fjall output, compare per-keyspace counts and canonical
    logical hashes, and run typed invariant checks;
11. fsync the completed output and parent directory before an atomic final
    rename; and
12. retain the RocksDB source as a rollback backup until the operator explicitly
    removes it.

Never mutate the source RocksDB directory.

### Rolling replacement alternative

- Add Fjall nodes under new node IDs while old RocksDB binaries remain running.
- Let normal learner admission, snapshot catch-up, promotion, and drain move
  every group to the Fjall nodes.
- Keep snapshot wire compatibility during the mixed cluster.
- Remove RocksDB nodes only after every group's committed membership and the
  meta directory exclude them and their serving gates are durably closed.
- This path avoids file conversion but requires spare capacity and exercises
  the complete rebalancing protocol.

### Tests and rehearsal

- Convert the preserved Phase 0 fixture and run all typed recovery assertions.
- Compare reads, Raft `LogState`, votes, committed state, applied state,
  membership, identity, admissions, and serving status before and after.
- Start a three-node cluster from converted stores and perform writes, reads,
  leader changes, snapshot catch-up, and another full restart.
- Interrupt conversion at every journal phase and verify source integrity and
  deterministic resume/cleanup.
- Rehearse rollback before deleting any RocksDB source directory.

### Acceptance criteria

- The chosen production migration path is automated and documented.
- Converted or rebalanced nodes retain quorum safety and local authority
  fences.
- A failed conversion cannot damage the RocksDB source.

## Phase 7: Remove RocksDB and neutralize the architecture

### Implementation

- Remove the RocksDB dependency from the production crate and delete
  `.cargo/config.toml` if it is only needed for the C++ build workaround.
- Remove `librocksdb-sys`, compression sys crates, bindgen, Clang, and C++ build
  requirements from production CI/images.
- Rename backend-specific production types and files:
  - `RocksLogStore` -> `LogStore`;
  - `RocksStateMachine` -> `RaftStateMachine` or another role-based name;
  - RocksDB-specific storage comments -> Fjall/generic terminology.
- Keep `Storage`, `StateMutation`, and the state-machine public API backend
  neutral.
- Update `DESIGN.md`, `IMPLEMENTATION.md`, `RUNTIME_ARCHITECTURE.md`, the
  performance plan, tests, deployment documentation, and operator runbooks.
- Replace RocksDB CF/SST/checkpoint language with Fjall keyspaces, logical
  snapshot files, ingestion, generations, and activation records.
- Add Fjall-specific metrics:
  - journal persist latency/failures;
  - batch sizes and `SyncAll` count;
  - per-keyspace memtable/disk usage where available;
  - compaction backlog and stalls where available;
  - active/staging generation bytes;
  - orphan count and cleanup age;
  - log-GC invisible bytes and cleanup lag; and
  - snapshot source/build/transfer/ingestion/activation durations.

### Acceptance criteria

- `cargo tree` for the production service contains no RocksDB or C/C++ storage
  dependencies.
- Documentation describes the code that is actually deployed.
- Operators can identify storage poison, stalled log GC, orphan generations,
  snapshot disk pressure, and conversion status.

## Phase 8: Production validation and rollout gate

### Correctness validation

Run after every phase and on the final branch:

```text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc
git diff --check
```

In addition:

- run storage, state-machine, data-Raft, meta-Raft, bootstrap, rebalance, runtime,
  and verification tests repeatedly under multiple seeds;
- run subprocess kill/restart loops for durable batch, log-bound, generation
  activation, reclamation, and converter boundaries;
- run at least one full-cluster simultaneous restart over Fjall stores;
- exercise disk-full, permission, corrupted snapshot, corrupted record,
  database-lock, and poisoned-storage behavior; and
- verify immediate shutdown/restart releases the Fjall database lock.

Failpoints around a method call are necessary but not sufficient. The final
gate must include actual process termination after writes reach the filesystem.

### Performance validation

Repeat the Phase 0 matrix on the same hardware and filesystem. Report:

- successful operations, errors, retries, and redirects;
- throughput and p50/p95/p99/p99.9;
- journal write and `SyncAll` latency;
- snapshot build/install memory and disk amplification;
- apply pause during snapshot view capture and generation activation;
- restart/recovery time;
- log-GC lag after purge/truncate;
- per-keyspace and total memory;
- compaction CPU/I/O interference; and
- sustained disk growth under update/delete-heavy workloads.

Minimum snapshot-specific gates:

- peak heap is independent of partition size except for the documented maximum
  record allowance;
- no complete-partition scan occurs while holding the apply lock;
- no snapshot-sized Fjall write batch is constructed;
- activation pause is independent of snapshot size; and
- temporary disk-space requirements are calculated and enforced before build
  or install begins.

Use the Phase 0 performance thresholds as the accept/reject gate. Any material
regression must be explained, tuned, or explicitly accepted before rollout.

### Rollout

1. Deploy to development and repeatedly recreate/convert stores.
2. Run a long-lived three-node soak with snapshots, membership changes,
   leader churn, node restarts, and update/delete-heavy traffic.
3. Rehearse the exact production migration and rollback procedure.
4. Canary one failure domain while retaining a valid rollback copy or enough
   RocksDB voters for quorum.
5. Expand one node/failure domain at a time, checking storage, Raft, snapshot,
   compaction, and disk metrics between steps.
6. Remove RocksDB backups only after the rollback window and an independently
   verified Fjall backup/recovery drill.

## Recommended implementation order

1. Freeze the RocksDB correctness/performance baseline and select the deployed
   data migration mode.
2. Build the Fjall database, local keyspace, lifecycle registry, and generation
   catalog.
3. Port local records and atomic state-machine batches with direct `SyncAll`.
4. Add logical Raft log bounds and bounded physical GC.
5. Pass all existing restart and Raft tests on the fixed-size snapshot path.
6. Introduce the versioned file-backed snapshot artifact.
7. Install through inactive generations and the atomic pointer switch.
8. Complete the hard-crash snapshot matrix.
9. Implement and rehearse the converter or rolling replacement path.
10. Remove RocksDB from production and update architecture/operations docs.
11. Repeat the full correctness, stress, performance, soak, migration, and
    rollback gates before production rollout.

## Stop conditions

Pause or abandon the replacement if any of these remain unresolved:

- Fjall cannot demonstrate durable atomic recovery for the batches DAL
  acknowledges to Raft;
- a poisoned database can continue serving or acknowledging storage work;
- logical log bounds permit stale-entry resurrection under any tested crash;
- snapshot ingestion or activation requires memory proportional to partition
  size;
- a mixed-version cluster can exchange an unsupported snapshot format;
- temporary snapshot/generation disk amplification cannot be bounded safely;
- keyspace count or per-keyspace write buffers make the configured partition
  count operationally impractical;
- restart, compaction, or sustained-write tail latency exceeds the preselected
  production threshold without an accepted mitigation; or
- no tested migration and rollback path exists for deployed RocksDB data.
