# DAL Service — Partitioned Full-Text Search Design

Status: **implemented**, with one deviation noted below. This document defines
the correctness requirements for Tantivy-backed full-text indexes in DAL;
invariants I1–I12 are enforced by the code in `src/search/` and the runtime
integration points listed in §15.

Not yet implemented:

- **§10.1 dedicated `search_addr` lane.** Search shares the control lane.
  Admission control (§13) is enforced instead by per-node bounds on concurrent
  coordinated searches, concurrent shard phases, and held sessions, so search
  cannot exhaust the handler capacity Raft needs — but it is not yet physically
  isolated from it.
- **§9.3 sort-by-fast-field.** `sort` is rejected; ranking is by score.
- **§8.2 per-shard checkpoints in `Eventual` replies.**

Design priorities remain: **correctness first, simplicity second, availability
third, performance fourth**.

---

## 1. Goals and non-goals

### Goals

- Define search indexes in the replicated meta/control plane.
- Maintain a Tantivy projection for every hosted data-partition replica.
- Send each shard query to a current partition leader.
- Scatter over all data partitions and merge one deterministic result.
- Include every valid indexed write completed before a strict search begins.
- Recover safely across crashes, snapshots, leadership changes, and rebalancing.
- Keep Tantivy failure out of the authoritative KV/Raft correctness path.

### Non-goals

- A single atomic snapshot across all partition groups.
- Cross-partition transactions.
- Making Tantivy files part of Raft state or Raft snapshots.
- Serving the complete stored value from the search index.
- Arbitrary user-supplied tokenizer code.
- Unbounded query syntax, result sizes, or aggregations.

---

## 2. System model

The existing RocksDB state machine remains authoritative. A Tantivy index is a
discardable, locally materialized projection of a committed data-partition
prefix.

```text
                           authoritative
client write -> data Raft -> RocksDB state at LogId A
                                  |
                                  | persistent dirty-key outbox
                                  v
                           local index worker
                                  |
                                  | Tantivy commit { source = A }
                                  v
                           searchable projection
```

One physical Tantivy index exists for each tuple:

```text
(data partition, logical index name, index generation)
```

Every voter and learner maintains the indexes for its hosted partition. Only a
leader executes a strict shard query. Indexing only the current leader is
incorrect because an ordinary Raft leadership change would leave the new leader
without a searchable projection.

### 2.1 Terminology

- **Source prefix**: a data group's committed/applied Raft `LogId`.
- **Projection epoch**: a node-local generation bumped when a snapshot replaces
  a group's state. Outbox records and Tantivy commits belong to one epoch.
- **Index generation**: an immutable control-plane index definition and Tantivy
  schema.
- **Engine revision**: a protocol value covering tokenizer, query, and scoring
  behavior that can affect results across nodes.
- **Searchable checkpoint**: the source prefix recorded in the currently loaded
  Tantivy commit.
- **Strict search**: leader-fenced search that waits for its index to cover its
  ReadIndex barrier.
- **Partial search**: explicitly requested search that may omit failed shards.

---

## 3. Correctness invariants

The implementation must preserve all of the following.

### I1. RocksDB is authoritative

Tantivy never decides whether a write succeeds and never changes Raft state.
Deleting all Tantivy files cannot lose authoritative data.

### I2. Atomic source/outbox publication

For every successful user-key mutation, its dirty-key outbox record is written
in the same RocksDB `WriteBatch` as the user state and Raft applied marker.
Recovery exposes both or neither.

### I3. Durable-source-before-index-commit

A Tantivy commit through `A` is allowed only after RocksDB reports the state
prefix through `A` WAL-durable. A durable derived projection may never outrun
its durable source.

### I4. Checkpoint truth is inside Tantivy

The authoritative index checkpoint is the payload of the loaded Tantivy
commit, not an in-memory counter or a separately persisted local record.

```text
loaded_commit = {
    projection_epoch,
    definition_hash,
    engine_revision,
    source_log_id,
}
```

### I5. Exact-prefix projection

If a loaded commit says `source_log_id = A`, its searchable documents equal the
valid documents selected by that definition from the RocksDB snapshot at `A`.

### I6. At-least-once projection

Outbox delivery may repeat. Applying one dirty key is idempotent:

```text
delete every Tantivy document with _key = key
if source record is valid and matches document_type:
    add its current document
```

### I7. Safe outbox deletion

An outbox record may be deleted only after every local active/building consumer
that needs it has a durable Tantivy commit at or beyond that record.

### I8. Snapshot epoch fence

After snapshot installation, no Tantivy commit from the previous projection
epoch may serve. Every generation rebuilds from the installed state.

### I9. Leader-fenced shard search

A strict shard result is returned only after that partition has completed a
ReadIndex barrier and the held searcher covers at least that barrier.

### I10. Definition identity

Coordinator and shard must agree on `(index name, generation, definition_hash,
engine_revision)`. A mismatch fails; it never silently searches another schema
or scoring implementation.

### I11. Complete-by-default scatter/gather

A strict global response contains a successful result from every partition.
Any missing partition fails the request unless the client explicitly permits a
partial response.

### I12. Deterministic merge

Shard results use comparable scoring/sort semantics. Global ties resolve by:

```text
(sort values, score descending, partition ascending, raw key ascending)
```

Tantivy `DocAddress` is never a distributed tie-breaker.

---

## 4. Value and index model

Stored values are FlatBuffers envelopes with a type discriminator and a
FlexBuffers payload.

```text
FlatBuffer value
  +-- type_id
  `-- payload: [ubyte] (flexbuffer)
        `-- root map
```

An index selects one `type_id` and maps FlexBuffer paths to immutable Tantivy
fields.

```rust
struct SearchIndexDefinition {
    document_type: String,
    fields: Vec<SearchField>,
    default_search_fields: Vec<String>,
}

struct SearchField {
    name: String,
    source_path: Vec<PathSegment>,
    kind: FieldKind,
    required: bool,
    multi_valued: bool,
    indexed: bool,
    stored: bool,
    fast: bool,
}

enum FieldKind {
    Text { tokenizer: String, positions: bool },
    Keyword,
    U64,
    I64,
    F64,
    Bool,
    Date,
    Facet,
}
```

Every Tantivy document also contains:

| Field | Options | Purpose |
|---|---|---|
| `_key` | indexed bytes + stored | exact delete/reindex and returned identity |
| `_version` | stored + fast `u64` | source key version |
| `_type` | exact keyword | defensive type filter |

V1 defines `SEARCH_MAX_KEY_BYTES = 4 KiB`, matching the recommended primary-key
limit. Longer keys remain valid KV keys but are outside the searchable document
domain and are counted as rejected. This bound guarantees `_key` can be encoded
as one exact Tantivy term for collision-free deletion.

The raw value is not stored in Tantivy. A search hit returns the key, version,
score/sort values, and explicitly stored projections. Fetching the full value is
a separate KV operation and is not snapshot-coupled to the search.

### 4.1 Extraction rules

1. Verify the FlatBuffer envelope before access.
2. Compare `type_id` exactly; do not coerce it.
3. Require the FlexBuffer root to have the configured shape.
4. Traverse paths with explicit FlexBuffer type checks.
5. Do not use FlexBuffer's lossy/convenience scalar coercions.
6. Missing optional fields add no Tantivy value.
7. Missing required fields or wrong types reject the document from that index.
8. Rejecting a document never rejects or rolls back its authoritative KV write.

The searchable domain is the set of valid documents for the active definition.
Rejected-document counts and bounded diagnostic samples are exposed in status.

### 4.2 Update semantics

Tantivy documents are immutable. Every dirty key is projected as delete followed
by optional add. This handles:

- ordinary updates;
- deletes;
- changes from an indexed type to another type;
- changes from another type into the indexed type;
- at-least-once replay.

---

## 5. Control-plane catalog

Index definitions live in meta Raft state, separate from immutable cluster
configuration and partition placement.

```rust
struct SearchIndexRecord {
    name: String,
    active: Option<SearchIndexGeneration>,
    building: Option<SearchIndexGeneration>,
    retiring: Vec<SearchIndexGenerationId>,
}

struct SearchIndexGeneration {
    id: SearchIndexGenerationId, // committed meta LogId
    definition_hash: [u8; 32],
    engine_revision: u32,
    definition: SearchIndexDefinition,
}
```

Definitions are canonically encoded before hashing. Field names, paths,
tokenizers, flags, field counts, and definition size are bounded and validated
inside meta state-machine apply. V1 also bounds the catalog to 64 indexes, which
keeps a complete reconciliation snapshot within its explicitly bounded control
frame.

### 5.1 Generation state machine

```text
Absent --create--> Building(g1) --all voters ready--> Active(g1)
                         ^                              |
                         `------ retry on failure -----+

Active(g1) --update--> Active(g1) + Building(g2)
                               |
                               | all voters ready
                               v
                      Active(g2) + Retiring(g1)
                               |
                               | readers drained
                               v
                           Active(g2)

Active(g) --drop--> Dropping(g) --readers drained--> Absent
```

An update never mutates a Tantivy schema in place. The old active generation
continues serving while the new generation backfills.

### 5.2 Meta commands

- `CreateSearchIndex { name, definition }`
- `CreateSearchIndexGeneration { name, definition }`
- `ReportSearchIndexReady { name, generation, group, node, ... }`
- `ActivateSearchIndexGeneration { name, generation }`
- `DropSearchIndex { name }`
- `FinalizeSearchIndexDrop { name, generation }`

Readiness observations bind to:

```text
(index generation, definition hash, engine revision, partition, node,
 partition voters_log_id, projection epoch, source checkpoint)
```

Meta accepts readiness only from a current voter for the stated placement.
Activation is allowed only when:

- no data-partition placement move is active;
- every partition's current `voters_log_id` still matches its observations;
- every current voter has reported the generation built.

These checks are deterministic meta-state-machine rules. Readiness reports are
non-Byzantine observations, matching the service fault model.

After activation, local corruption or file loss may invalidate one replica's
copy. That replica fails search with `IndexNotReady` until rebuilt; it does not
revoke the cluster-wide catalog generation.

---

## 6. Persistent dirty-key outbox

The outbox is node-local, per partition, persistent, and at-least-once. It is
not replicated and is not included in Raft snapshots.

The producer emits dirty keys independently of its cached catalog view. With no
registered consumers, the janitor may prune durable records immediately. A new
consumer registration and retention calculation are serialized before its
backfill snapshot, so catalog-propagation races cannot create an unrecorded gap.

### 6.1 Record shape

```text
local/search/<group>/epoch/<epoch>
local/search/<group>/outbox/<epoch>/<source-log-id>/<encoded-user-key>
local/search/<group>/consumer/<index>/<generation>
```

Epoch, `LogId`, and raw-key components use a canonical length-delimited,
big-endian encoding so range scans preserve source order without delimiter
ambiguity.

The outbox stores keys, not values. Values can reach 16 MiB and already exist in
the authoritative state CF.

One outbox entry is emitted for each final user-key mutation in a coalesced
state apply. Multiple changes to one key within the same apply chunk may collapse
to one dirty key because projection correctness depends on the chunk's final
state.

### 6.2 Producer transaction

```text
RocksDB WriteBatch for data group at A
  +-- state CF: final user mutations
  +-- state CF: raft_applied = A
  +-- log CF: committed recovery hint = A
  `-- default CF: dirty keys under (projection_epoch, A)
```

The batch is submitted through the existing state durability coordinator. The
outbox adds no independent success boundary to Raft apply.

### 6.3 Consumer algorithm

For a consumer whose loaded checkpoint is `C`:

```text
1. Take one DB-wide RocksDB snapshot S.
2. From S, read current group epoch E and applied prefix A.
3. From S, read outbox keys in E with C < source <= A.
4. Deduplicate keys.
5. From S, read each key's current state.
6. Wait until RocksDB state through A is WAL-durable.
7. Recheck that the current group epoch is still E; otherwise abort.
8. For each key, issue Tantivy delete(_key), then optional add(document).
9. Commit Tantivy payload { epoch=E, definition_hash, engine_revision,
   source=A }.
10. Reload the reader and publish its loaded checkpoint A.
11. Reclaim outbox records covered by every consumer.
```

The DB-wide snapshot is essential: `A`, the outbox range, and key values must
describe the same state prefix.

### 6.4 Commit and cleanup sequence

```text
index worker        RocksDB/outbox          Tantivy
     |                    |                    |
     |-- snapshot ------->|                    |
     |<-- A, keys, state -|                    |
     |-- wait durable A ->|                    |
     |<-- durable --------|                    |
     |---------------- delete/add ----------->|
     |---------------- commit(A) ------------>|
     |<--------------- committed -------------|
     |---------------- reload ---------------->|
     |<------------ searcher at A -------------|
     |-- prune <= min consumer checkpoint ---->|
```

If cleanup fails, replay is safe. Cleanup must never precede the Tantivy commit
and reload.

### 6.5 Retention and lag

The retained outbox begins after the minimum checkpoint of all active/building
local consumers. A new generation registers itself as a consumer before taking
its backfill snapshot, preventing a cleanup race.

If one consumer exceeds its configured lag/bytes limit:

1. mark it `NeedsRebuild`;
2. remove it from incremental-retention calculation;
3. prune the newly unblocked outbox prefix;
4. rebuild it from a fresh RocksDB snapshot.

An unhealthy derived index must not grow the outbox without bound or poison the
primary write path through disk exhaustion.

---

## 7. Backfill and recovery

### 7.1 New generation backfill

```text
1. Persistently register the generation as a local consumer.
2. Take DB-wide snapshot S at (epoch E, applied A).
3. Scan TAG_USER records from S and build a fresh temporary Tantivy directory.
4. Wait for RocksDB prefix A to become durable.
5. Commit { E, definition_hash, engine_revision, A } and reload its reader.
6. Atomically publish/rename the temporary directory as ready.
7. Tail outbox records newer than A.
8. Report readiness to meta.
```

Register-before-snapshot guarantees that every mutation after snapshot `A`
remains in the outbox. Mutations before or at `A` are represented by the full
scan.

### 7.2 Process restart

For every local Tantivy directory:

1. Open the latest complete Tantivy commit.
2. Validate index generation, definition hash, and engine revision.
3. Validate its projection epoch against the group's local epoch.
4. Validate its source checkpoint is not ahead of recovered RocksDB applied
   state.
5. If valid, resume outbox consumption after its checkpoint.
6. Otherwise quarantine/delete it and backfill.

The service never guesses a checkpoint from file timestamps or directory names.

### 7.3 Snapshot installation

A Raft snapshot replaces the authoritative state without producing one dirty-key
event per replaced record. Snapshot install therefore performs this atomic local
transition:

```text
install_state WriteBatch
  +-- replace state CF with snapshot contents
  +-- write Raft applied/membership record
  `-- default CF: projection_epoch = projection_epoch + 1
```

Search immediately rejects old-epoch indexes. All generations rebuild from the
installed state, and old-epoch outbox records are discarded after the install is
durable.

Tantivy files are deliberately excluded from Raft snapshots: they are derived,
engine-version-specific, and independently recoverable.

---

## 8. Shard search correctness

### 8.1 Strict shard search

```text
coordinator                  partition leader              index worker
     |                              |                           |
     |-- SearchPrepare -----------> |                           |
     |                              |-- require serving         |
     |                              |-- ensure_linearizable     |
     |                              |<-- barrier B              |
     |                              |-- wait searchable >= B -->|
     |                              |<-- held searcher C >= B ---|
     |<-- session + stats --------- |                           |
     |                              |                           |
     |-- SearchExecute(session) --> |                           |
     |<-- top hits at C ----------- |                           |
```

`SearchPrepare` fails with a redirect if the contacted node cannot complete the
ReadIndex barrier as leader. The returned session pins one loaded Tantivy
searcher and its exact checkpoint for the short lifetime of the distributed
query.

Holding the searcher prevents statistics and result scoring from observing
different Tantivy commits. Sessions are bounded by count, memory, and deadline;
expiration makes the coordinator retry the whole request.

### 8.2 Consistency guarantee

A successful strict search:

- includes all valid matching writes acknowledged before search invocation;
- may include writes concurrent with the search;
- uses one consistent prefix per partition;
- does not promise that all partition prefixes represent one wall-clock instant.

There is no cross-partition transaction or timestamp oracle, so a single atomic
cluster-wide snapshot is outside this design.

An optional `Eventual` mode may search the leader's currently loaded commit
without ReadIndex or checkpoint waiting. Its response must include each shard's
checkpoint and must not be labeled strict.

---

## 9. Scatter/gather and scoring

### 9.1 Coordinator flow

```text
                         +-> P0 leader --+
client -> coordinator --+-> P1 leader --+-> deterministic global merge
                         +-> ... --------+
                         `-> PN leader --+
```

The coordinator:

1. resolves the active catalog generation;
2. fans out `SearchPrepare` concurrently to every partition;
3. follows leader redirects and refreshes stale placement once;
4. computes global scoring statistics;
5. fans out `SearchExecute` against the held shard sessions;
6. merges hits and hit counts;
7. returns only after every shard succeeds, unless partial mode was explicit.

Fanout uses a global deadline and bounded concurrency. Retries never extend the
client deadline.

### 9.2 Exact distributed BM25

Locally computed BM25 scores are not comparable because document frequency and
average field length differ per partition. Strict relevance scoring uses two
phases:

```text
phase 1: each held shard searcher -> local term/doc/token statistics
         coordinator             -> summed global statistics

phase 2: global statistics -> each shard's Bm25StatisticsProvider
         each shard         -> top (offset + limit)
         coordinator        -> global top page
```

For a fixed held searcher, each shard reports:

- total documents;
- total indexed tokens per scored field;
- document frequency for every expanded query term.

The coordinator sums these values and shards build their query weights using a
custom Tantivy `Bm25StatisticsProvider`.

V1 query syntax is restricted to query forms whose term expansion and global
statistics are bounded and testable. Regex/fuzzy/wildcard expansion requires an
explicit expansion limit; exceeding it rejects the query.

`LocalBm25` may be exposed only as an explicitly approximate mode. Sorting by a
defined fast field is globally comparable without BM25 statistics.

### 9.3 Pagination and ordering

Each shard returns at least `offset + limit` hits. The coordinator merges by the
requested sort, then the stable distributed tie-breaker `(partition, raw key)`.

V1 bounds `offset`; it does not promise snapshot-stable pagination across
separate requests. A future point-in-time cursor must pin every shard searcher
and encode the catalog generation, definition hash, query hash, and per-shard
session/checkpoint.

### 9.4 Partial failures

Default behavior is fail closed:

```text
one missing, stale, mismatched, or failed partition -> whole search fails
```

With `allow_partial = true`, the response includes:

```rust
partial: true,
failed_partitions: Vec<PartitionFailure>,
```

Partial hit counts and ranking are explicitly non-complete.

---

## 10. Public and internal API

Search remains on the binary data plane; HTTP remains read-only observability.

```rust
struct SearchRequest {
    index: String,
    generation: GenerationSelection, // Active (default) or Exact(id)
    query: SearchQuery,
    limit: u32,
    offset: u32,
    sort: Vec<SortField>,
    scoring: ScoringMode,             // GlobalBm25 by default
    consistency: SearchConsistency,   // Strict by default
    allow_partial: bool,              // false by default
    deadline_ms: u32,
}

struct SearchReply {
    generation: SearchIndexGenerationId,
    definition_hash: [u8; 32],
    hits: Vec<SearchHit>,
    total_hits: u64,
    partial: bool,
    failed_partitions: Vec<PartitionFailure>,
}

struct SearchHit {
    partition: u16,
    key: Vec<u8>,
    version: u64,
    score: Option<f32>,
    sort_values: Vec<SortValue>,
    stored_fields: Vec<StoredField>,
}
```

New envelope message types are appended without renumbering existing tags:

- `SearchOp`: public coordinator request/reply, addressed to `GroupId::Meta`;
- `SearchPrepare`: internal shard preparation/statistics request;
- `SearchExecute`: internal held-session execution request;
- `SearchIndexStatus`: readiness/rebuild observation.

`GenerationSelection::Active` uses a ReadIndex-fenced meta catalog lookup. An
explicit generation may use a cached catalog but every shard still validates
the generation and definition hash.

### 10.1 Search transport lane

Production nodes add a dedicated `search_addr` ZMQ lane. The node directory,
join registration, address book, and node config carry this endpoint.

```text
control_addr : Raft, routing, operator control, point KV operations
bulk_addr    : snapshots
search_addr  : coordinator and shard search traffic
http_addr    : read-only status/health
```

Separating search prevents fanout and expensive replies from consuming the
handler capacity required for Raft elections and replication.

---

## 11. Rebalancing and reclamation

### 11.1 Learner promotion

For every active index generation, the move driver adds an index fence between
Raft catch-up and voter promotion:

```text
admit learner
    -> Raft add_learner(wait=true)
    -> install/replay state
    -> rebuild/tail every active search generation
    -> verify generation/hash/epoch/checkpoint readiness
    -> change_voters(target)
```

A rebuild failure pauses the move but does not corrupt or stop the existing
partition. An operator may drop/repair the index or abort the move.

### 11.2 Leadership change

All voters normally have ready indexes. A new leader can serve after its own
ReadIndex barrier and checkpoint wait. If its local index is missing or corrupt,
it returns `IndexNotReady`; it does not serve an older or mismatched generation.

Search readiness does not participate in Raft election eligibility or the KV
serving gate. Derived-index failure must not reduce authoritative data safety.

### 11.3 Group reclamation

Reclamation order is:

```text
close KV serving gate
  -> close search serving gate and expire sessions
  -> drain authoritative state durability
  -> stop index workers/readers/writers
  -> remove group CFs
  -> remove group outbox/consumer records
  -> remove group Tantivy directories
```

After the search gate closes, no file may be reopened by an in-flight query.

---

## 12. Failure behavior

Tantivy errors are search-scoped. They do not set the database-wide RocksDB
failure fence and do not stop Raft.

The outbox itself is RocksDB state written by authoritative apply. A RocksDB
failure while writing it follows the existing database fail-stop policy. Lag
bounds and rebuild fallback prevent a failed Tantivy consumer from retaining an
unbounded outbox and indirectly exhausting that database.

| Failure point | Required recovery |
|---|---|
| Before atomic RocksDB apply | Neither state nor outbox is visible; Raft redelivers |
| After visible apply, before WAL durability | Index worker waits; crash recovers only durable source |
| After source durability, before Tantivy commit | Outbox remains; worker retries |
| After Tantivy commit, before reader reload | Commit survives; restart/reload discovers it |
| After reader reload, before outbox cleanup | Outbox replays delete/add safely |
| During backfill before final commit | Temporary directory is discarded and rebuilt |
| Corrupt/missing Tantivy directory | Shard fails closed and rebuilds from RocksDB |
| Snapshot replaces state | Projection epoch changes; old index cannot serve |
| Leader changes during scatter | Redirect/retry within the original deadline |
| One shard misses deadline | Strict request fails; explicit partial request reports omission |
| Meta quorum unavailable | No index lifecycle changes; exact active-generation resolution fails |

### 12.1 Index format upgrades

Each directory records:

```text
DAL search format version
Tantivy index format version
index generation
definition hash
engine revision
projection epoch
source checkpoint
```

An unreadable or unsupported format triggers rebuild. Index files are never
the only copy of data, so in-place format migration is unnecessary.

---

## 13. Resource and abuse bounds

Search work is bounded independently from ordinary client and Raft work:

- maximum concurrent global searches;
- maximum concurrent shard searches;
- maximum fanout in flight per coordinator;
- dedicated blocking CPU pools for indexing and querying;
- node-wide Tantivy writer memory budget;
- bounded open-writer count with idle eviction;
- query length, AST depth, clause count, and term-expansion limits;
- maximum `limit`, `offset`, sort fields, stored-field bytes, and reply bytes;
- bounded held-searcher sessions and lifetime;
- outbox byte/age limits with rebuild fallback;
- end-to-end deadline propagated to every phase.

The 32 MiB search frame is enforced on both encode and decode. Shards derive a
per-hit stored-field budget from the requested result window, and coordinators
retain only the global top window while replies arrive; a result that cannot fit
is rejected explicitly rather than materialized as an oversized frame.

Timeout alone is not cancellation for arbitrary blocking Tantivy work. Query
validation and admission are required before entering the blocking pool.

---

## 14. Observability

`GET /status` adds, per hosted partition and index generation:

```text
name
generation
definition_hash
state: Building | Ready | Active | NeedsRebuild | Failed
projection_epoch
searchable_through
source_visible
source_durable
lag_entries
outbox_entries / outbox_bytes / oldest_outbox_ms
last_commit_latency_ms
backfill_progress
rejected_documents
last_error
```

Search-index failure does not change the existing database `/health` result.
Search calls expose their own readiness/failure status, while `/status` makes
degradation diagnosable.

Metrics include query latency by phase, fanout width, redirects, shard failures,
partial responses, index throughput, commits, merge time, rebuilds, outbox lag,
and rejected-document counts.

---

## 15. Implementation map

```text
src/search/
  catalog.rs       compiled definition + hash validation
  extract.rs       FlatBuffer/FlexBuffer verification and extraction
  outbox.rs        persistent dirty-key journal and retention
  index.rs         Tantivy generation lifecycle and commit payload
  worker.rs        backfill, incremental projection, checkpoint waiters
  query.rs         bounded query AST and shard execution
  coordinator.rs   scatter/gather, retries, statistics, global merge
  status.rs        local index status snapshots
```

Existing integration points:

| Area | Change |
|---|---|
| `types.rs`, `keyspace.rs` | catalog types, meta/local keys, generation ids |
| `meta/state_machine.rs`, `meta/node.rs` | deterministic catalog commands and reads |
| `storage/batch.rs`, `storage/rocks.rs` | atomic outbox writes, DB snapshots, epoch install |
| `partition/sm.rs` | pass final changed user keys to storage apply |
| `partition/node.rs` | strict leader-fenced shard search |
| `api/ops.rs`, `api/client.rs` | public request/reply and `Client::search` |
| `api/gateway.rs`, `runtime/dispatch.rs` | coordinator and shard dispatch separation |
| `transport/codec.rs`, `transport/router.rs` | message tags, size/admission limits |
| `runtime/node.rs`, `runtime/config_file.rs` | index manager and search lane lifecycle |
| `runtime/rebalance.rs` | learner index-readiness fence and cleanup |
| `runtime/http.rs` | node-local search status |

---

## 16. Verification plan

### 16.1 State/projection model tests

- Put, update, delete, type change, and repeated delivery produce the expected
  document set.
- FlexBuffer extraction is deterministic for every supported field type.
- Definition encoding and hash are stable.
- Global merge is deterministic under score and sort ties.

### 16.2 Crash-boundary tests

Add failpoints around:

```text
RocksDB apply before/after write
source durability wait
Tantivy prepare/commit
reader reload
outbox cleanup
backfill publication
snapshot epoch bump
group reclamation
```

After every injected crash, assert either the prior searchable checkpoint or a
new complete checkpoint—never a falsely advanced or mixed projection.

### 16.3 Distributed tests

- A strict search observes every write acknowledged before invocation.
- Concurrent writes may appear but never create malformed mixed documents.
- Leader failover serves from the new leader or fails `IndexNotReady`; it never
  serves from an unconfirmed former leader.
- A learner is not promoted before active indexes are ready.
- Snapshot catch-up invalidates old epoch indexes and rebuilds correctly.
- Strict scatter fails when any partition is unavailable.
- Partial scatter identifies every omitted partition.
- Global BM25 results match a single-index reference corpus.
- Definition activation cannot cross a placement-generation change.

### 16.4 Property to continuously check

For every successful strict shard result with checkpoint `C`:

```text
returned_hits
    == evaluate(definition, query, authoritative_partition_state_at(C))
```

For a strict global result, the same holds independently for every partition,
followed by the specified deterministic global merge.

---

## 17. Delivery sequence

1. Catalog types, validation, and immutable generations.
2. Value verifier/extractor and local single-partition Tantivy projection.
3. Persistent outbox, commit checkpoints, crash recovery, and backfill.
4. Snapshot epoch invalidation and group reclamation.
5. Strict leader-fenced shard search.
6. Scatter/gather with deterministic sort and fail-closed semantics.
7. Exact global BM25 statistics and held searcher sessions.
8. Index activation/readiness and rebalance promotion fence.
9. Dedicated search lane, admission controls, observability, and soak tests.

No phase is production-ready until its crash-boundary and failover properties
are verified.

---

## 18. Upstream constraints

- Tantivy is a local search library; distributed search is implemented here:
  <https://github.com/quickwit-oss/tantivy>
- Updates require delete/reindex and become visible after commit plus reader
  reload:
  <https://docs.rs/tantivy/latest/tantivy/indexer/struct.IndexWriter.html>
- Immutable schema changes require index generations:
  <https://docs.rs/tantivy/latest/tantivy/schema/struct.SchemaBuilder.html>
- Exact cross-shard BM25 can supply global statistics through
  `Bm25StatisticsProvider`:
  <https://docs.rs/tantivy/latest/tantivy/query/enum.EnableScoring.html>
- FlexBuffer extraction must inspect actual types rather than depend on coercion:
  <https://flatbuffers.dev/flexbuffers/>
