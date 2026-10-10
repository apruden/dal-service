# DAL service implementation plan

Date: 2026-10-06

Status: implementation scope through Phase 5

## Objective and scope

Implement a persistent DAL service that preserves acknowledged writes within
its Raft fault model, authenticates network participants, exposes health and
diagnostics, and can recover from a verified backup.

The implementation assumes Linux, durable local disks, TCP between independent
hosts, at least three meta voters, and three replicas per data partition. Keep
RocksDB and the current consistency model. Storage-engine replacement is
outside this plan.

The initial backup implementation uses coordinated maintenance. Online backup
is a separate feature if later required. The provisional
[operating profile](docs/operations/production-profile.md) supplies workload
assumptions; deployment qualification and operations are outside this plan.

This plan extends the current-binary, new-cluster scope in
[the materialization plan](ASYNC_MATERIALIZED_STATE_IMPLEMENTATION_PLAN.md).
This project has not been deployed to production. Earlier development data,
storage directories, snapshots, and wire formats are disposable. This plan
targets fresh directories and the current binary only; it does not include
migrations or backward compatibility with earlier development revisions.

## Existing implementation and evidence

The implementation baseline is commit `6fabff4`. An earlier captured test log
reports **342 passed, zero failed, and four ignored measurement tests**, with a
leader-crash campaign taking 307.47 seconds. The original log lacks a revision
header and captured seed list, so those outcomes are diagnostic until reproduced
with provenance. Tie future results to the exact revision, toolchain,
configuration, and seed list.

Phase 0 commit `57e7c3e` reproduced the full suite with an explicit clean
revision, Rust/Cargo versions, command, and seeds 0 through 99: **342 passed,
zero failed, and four ignored measurement tests**. Its leader-crash campaign
completed in 302.93 seconds. The archived log remains single-host regression
evidence.

Retain the existing majority-durable Raft log, asynchronous data-state
materialization, recovery fencing, deterministic idempotency, streamed
snapshots, atomic snapshot generation changes, and transport resource reserves.
These are implemented foundations, not new milestones.

The latest [correctness and performance results](CORRECTNESS_PERFORMANCE_FIX_PLAN.md)
include three-process TCP workloads, 1 MiB values, search rebuilds, and follower
snapshot recovery. They are useful regression evidence for implementation
changes.

## Invariants for every phase

1. A successful mutation remains backed by a majority-durable Raft entry and
   visible, ordered local application. Authentication, shutdown, and backup
   must not introduce an earlier success response.
2. Data-group materialization may lag durability. Log purge and snapshot
   publication retain their durable-prefix fences; recovery retains its read
   serving gate. Meta application remains synchronous.
3. A timeout can leave a mutation outcome unknown. Retrying the same logical
   command preserves its identity and result across restarts of the current
   version. Validation failure before the first send does not reserve a new
   sequence. A committed CAS failure retains its decided-sequence semantics.
   Drain, maintenance, or authorization rejection of a later attempt cannot
   prove that an earlier attempt did not commit. Preserve pending identity on
   those responses; reserve `ClientReply::Refused` for deterministic refusals
   based only on request bytes and cluster constants. Disaster recovery to an
   older point follows the explicit recovery-epoch contract in Phase 5.
4. Cached health and routing information cannot authorize membership changes,
   reclamation, learner admission, or a linearizable read.
5. Authentication identifies the sender. Authorization checks its operation
   and identity binding. Neither replaces existing Raft and placement checks.
6. Probes, log collection, and metric scrapes perform bounded local work and
   cannot consume the capacity reserved for Raft progress.
7. Shutdown never reports a clean durable stop while owned work is still using
   storage. A forced stop remains an explicit crash-recovery path.
8. A restore never starts a second reachable copy of the same cluster identity.
   A normal restart never silently discards acknowledged writes.
9. Planned cluster stops use a replicated maintenance fence that also controls
   automatic directory, placement, and search changes. A process signal alone
   cannot suspend failure detection or authorize maintenance release.

## Delivery order

Phases 0–5 cover service implementation and its stated evidence. Shipping,
deployment, qualification, and operations are outside this plan.

| Phase | Deliverable | Dependencies | Relative effort |
| --- | --- | --- | --- |
| 0 | Workload and recovery assumptions | None | Small |
| 1 | Lifecycle, termination, and cheap health probes | 0; planned-stop integration uses 5 | Medium |
| 2 | Authenticated transport and operation authorization | 0 | Large |
| 3 | Bounded logs, metrics, and audit events | 1; audit integration from 2 | Medium |
| 4 | Current format and restore metadata contract | 0; integrate 2 | Small |
| 5 | Coordinated backup and verified restore | 1, 2, format definitions from 4 | Large |

Implement Phase 1's local shutdown independently, then integrate its planned-stop
behavior with Phase 5. Define maintenance commands, trust-generation requirements,
and recovery epochs in Phase 4 before implementing their persisted behavior and
the backup format in Phase 5. Independent probe, logging, security, and current
format work can proceed before the full fence.

## Phase 0 Define implementation assumptions

### Implementation

- Record assumed filesystem and disk guarantees, host count, failure-domain
  placement, partition count, replica count, data size, index count, and
  resource limits. State the non-Byzantine fault model and quorum-loss behavior.
- Record workload values used to design and evaluate the implementation:
  sustained operations per second, concurrent clients, value-size distribution,
  p95/p99 latency, accepted error rate, failover time, search freshness, and
  maximum recovery backlog. Include the 1 MiB value class explicitly.
- Define recovery assumptions. Node failure with a surviving durable quorum
  must lose zero acknowledged writes. Total-cluster loss can recover only
  through the last verified backup unless a separate
  continuous recovery mechanism is implemented. State the backup and restore
  limits that the implementation can actually enforce.

### Completion gate

The implementation assumptions state the workload and recovery boundaries
used by later phases, including explicit latency, backup, and restore targets
or an explicit record that those targets remain unknown. Unspecified deployment
targets do not block service work.

## Phase 1 Implement lifecycle and health reporting

Primary changes: [main](src/main.rs), [runtime node](src/runtime/node.rs),
[readiness](src/runtime/readiness.rs), [HTTP](src/runtime/http.rs),
[gateway](src/api/gateway.rs), [router](src/transport/router.rs), and storage
durability shutdown seams.

### Implementation

1. Introduce an explicit lifecycle shared by the CLI, runtime, admission, and
   health source: `Starting`, `Running`, `Draining`, `Failed`, and `Stopped`.
   Keep this separate from the one-way bootstrap gate and durable membership
   authority. Closing readiness must not mark a healthy member as reclaimed.
2. Register SIGINT and SIGTERM before awaiting bootstrap. Route signals,
   bootstrap failure, and fatal background-task failure through one shutdown
   coordinator. Cover cancellation while discovery or bootstrap is waiting.
3. On drain, mark the process unready and reject new client work. Keep peer
   traffic available while bounded accepted work completes. Use a distinct
   unavailable response that preserves pending mutation identity, rather than
   `ClientReply::Refused`. Attempt a bounded best-effort meta leadership handoff
   using an eligible caught-up voter; the existing path triggers an election
   and may need retries. Initially, data groups fail over by normal election
   after Raft shutdown; there is no data-group handoff RPC today. Measure that
   interruption against Phase 0 and add explicit per-group handoff only if
   needed to meet its target. Preserve the existing rule that handlers blocked
   in Raft must be released by Raft shutdown before waiting for all handlers.
   The signal handler must not propose membership removal. A planned cluster
   stop first installs the Phase 5 fence to suppress failure-driven directory
   transitions and replacement; an unfenced process stop retains ordinary
   failure detection and can require explicit rejoin if declared `Down`.
4. Stop producers and drain owned search jobs, snapshot jobs, Raft applies,
   and the WAL worker in a documented order. Verify visible and durable state
   agree after successful draining and before releasing the final storage owner.
   Observe background-task panics and errors instead of silently dropping their
   `JoinHandle` results.
5. Add a configurable application shutdown deadline and a longer supervisor
   termination grace period. Start with proposed defaults of 60 and 90 seconds,
   then validate them against the production profile. Exceeding the deadline
   emits a failure and exits through a bounded forced-stop path; it does not
   claim a clean drain. Account for non-cancellable blocking tasks and runtime
   destruction so a timeout wrapper alone cannot leave the process hanging.
6. Serve probes from a cheap in-memory health view. Replace the former
   `/health` path, which called `status()` and could scan search outboxes, with
   a bounded background sampler and exposed sample age. Preserve `/health` as
   a documented compatibility liveness alias.

### Probe contracts

| Endpoint | Meaning and exclusions |
| --- | --- |
| `/livez` | The process and required local runtime tasks are functioning. No network, disk scan, or quorum round trip. Quorum loss alone does not trigger a restart. |
| `/readyz` | The configured serving role has bootstrapped, identity is valid, local serving/recovery gates are satisfied, maintenance is released, and the node is neither draining nor failed. Healthy followers can be ready. |
| `/status` | Bounded diagnostics including lifecycle, maintenance operation/stage, active trust generation, recovery epoch, per-group state, storage failure, search degradation, and diagnostic sample age. |
| Search readiness | Report separately if search is required by the deployment; an unusable derived index does not make authoritative KV storage fail liveness. |

Specify readiness by role: a routing node needs usable routing and admission;
a data-serving node needs the gates for its required local groups. Readiness
is not proof that the next request can reach a quorum. Measure end-to-end
availability with a rate-limited background canary and retain request-time
ReadIndex checks. Keep peer discovery and recovery reachable even while client
readiness is closed. Supervisors must start the required peers without waiting
for another node's client readiness: recovery may itself need a leader and
quorum. This follows
the separation of startup, liveness, and readiness in the
[Kubernetes probe model](https://kubernetes.io/docs/concepts/workloads/pods/probes/).

### Tests and completion gate

- Add child-process SIGTERM/SIGINT tests during bootstrap, active writes,
  snapshot install, search rebuild, and idle operation. Reopen the same data
  directory and check acknowledged writes and mutation retry identity.
- Test failed WAL flush, background-task panic, shutdown deadline expiry, and
  immediate restart. Verify exit status and whether shutdown was clean.
- Commit a mutation but lose its response, then drain the node before retry.
  Verify the unavailable response preserves the pending command and sequence;
  retry after recovery returns the original result without a second mutation.
- Measure data-group election failover and bounded meta handoff under load.
  After Phase 5 integration, stop nodes sequentially beyond the failure timeout
  under maintenance, restart them, and verify unchanged directory state,
  incarnations, and placement. Include spare Active nodes to expose unwanted
  replacement; separately verify normal failure detection outside maintenance.
- Test readiness closure before client admission stops; keep peer progress
  available until its shutdown stage. Test followers, routing-only roles,
  storage fencing, and a recoverable network partition.
- Probe latency and allocations remain bounded as data and outbox size grow.
  No probe calls `search_outbox_usage` or performs ReadIndex.

The actual executable responds to both signals, drains or terminates within
its configured bound, and exposes the specified lifecycle throughout shutdown.
Tokio's [signal documentation](https://docs.rs/tokio/latest/tokio/signal/)
provides the Unix signal APIs; `ctrl_c()` alone covers SIGINT.

## Phase 2 Authenticate and authorize network operations

Primary changes: [transport interfaces](src/transport/mod.rs),
[DEALER](src/transport/dealer.rs), [ROUTER](src/transport/router.rs),
[dispatch](src/runtime/dispatch.rs), [admin client](src/runtime/admin.rs),
[client](src/api/client.rs), [configuration](src/runtime/config_file.rs), and
new transport security modules.

### Implementation

1. Use libzmq CURVE for encrypted connections and ZAP for credential admission.
   Configure every external control and bulk socket, including CLI, client,
   heartbeat, discovery, and snapshot connections. Check CURVE support at
   startup and fail before binding if security configuration cannot be loaded.
   Production mode has no plaintext fallback. Local test transports use explicit
   test principals rather than a bypass accessible through TCP configuration.
2. Provision distinct credentials for nodes, operators, and applications.
   Use an operator-controlled, cluster-scoped trust policy as the authority for
   principal bindings, roles, permitted server keys, and a monotonically
   increasing generation. Initially distribute it as an atomically replaced
   protected file; a deployment secret provider may deliver the same artifact.
   Configure the authority out of band on nodes, operators, and application
   clients. A routing redirect or self-reported directory entry cannot establish
   a new key's trust. Reject policy rollback and retain the authoritative
   generation outside cluster backups so restore cannot reinstate revoked trust.
   Load private keys separately from protected files or the secret provider;
   exclude secrets from logs, status, command-line arguments, and artifacts.
3. Preserve the authenticated principal from the libzmq message through
   dispatch. The current `recv_multipart` conversion to byte vectors loses
   message metadata. Receive `zmq::Message` frames, capture ZAP `User-Id`, and
   pass an immutable request context alongside `Envelope`. Never derive the
   principal from caller-supplied envelope fields or ROUTER routing identity.
4. Implement and test a deny-by-default operation matrix. Applications may
   perform authorized KV, search, and advisory routing requests. Operators may
   perform explicitly granted administrative actions. Nodes may perform the
   internal Raft, discovery, recovery, search, and reconciliation operations
   required by their identity and role. Define join enrollment separately from
   the permissions of an already admitted node. Before `RegisterNode` makes a
   node Active and eligible for automatic placement, distribute its binding and
   server keys to the required peers and client deployments and confirm the
   target policy generation. Enforce that enrollment gate through the operator
   path; a candidate's self-reported policy version is insufficient evidence.
5. Bind authenticated node identity to the relevant sender fields in heartbeats,
   observations, and Raft requests, respecting each RPC's actual semantics.
   Retain membership, term, incarnation, placement, and learner-plan checks.
   Define application ownership of client sequence identities so one principal
   cannot impersonate another application's mutation stream. Coordinate any
   persistent identity-layout change with Phase 4. Authorization denials must
   not use `ClientReply::Refused` or clear a previously pending mutation. Return
   a distinct denial that stops automatic retries without claiming the earlier
   outcome is known; resolution after reauthorization preserves the original
   identity. Do not disclose stored results to an unauthorized principal.
6. Select peer admission reserves from the authenticated role and permitted
   operation, not just `MsgType`. Bound authentication work and malformed-frame
   handling. Reject excess multipart frames, invalid length declarations, and
   oversized or deeply nested decoded structures before uncontrolled allocation.
7. Implement staged rotation and revocation using that policy generation.
   Report the active generation in node `/status` and client diagnostics.
   Publish new trust, confirm installation, switch keys and sockets, then remove
   old trust. Recheck authorization on existing connections so removing a key
   takes effect without waiting for a new handshake. Define the required rollout
   set before each operation: remaining authorized members and clients must
   enforce the target policy, while stale deployments are updated or isolated.
   Revocation completion does not wait for the revoked principal to acknowledge
   its own removal. Persist progress, reject stale generations after restart,
   and make interrupted rollout resumable. A first rotation procedure may use
   maintenance, but cannot claim completion while stale authorized deployments
   remain reachable. Document the required isolation and convergence evidence.
8. Bind HTTP diagnostics to loopback by default. Remote access requires an
   authenticated TLS endpoint or a specified authenticated proxy whose direct
   upstream is unreachable externally. Separate access to minimal probes from
   detailed status and metrics. An insecure non-loopback admin bind fails
   production configuration validation.
9. Keep `dal check-config` non-mutating. It rejects unknown, inconsistent, or
   insecure service configuration before any storage or socket mutation.

[CURVE](https://libzmq.readthedocs.io/en/latest/zmq_curve.html) supplies transport
authentication and confidentiality. [ZAP](https://rfc.zeromq.org/spec/27/)
provides the admission exchange, and
[message metadata](https://libzmq.readthedocs.io/en/latest/zmq_msg_gets.html)
exposes its authenticated user identity. Operation authorization remains DAL's
responsibility. Credential theft by a trusted node does not turn Raft into a
Byzantine-tolerant protocol.

### Tests and completion gate

- Real TCP tests reject unknown, wrong-cluster, revoked, and incorrectly pinned
  credentials on both lanes. Reject application credentials attempting peer
  messages, administrative commands, or peer-only resource reservations.
- Attempt forged node IDs, routing IDs, observations, heartbeat incarnations,
  and client sequence identities. Verify no authoritative state changes.
- Test rotation and revocation on already established connections and across
  reconnects. A missing ZAP handler or unreadable credential file fails closed.
- Delay policy installation on one peer or client deployment. Verify enrollment
  and rotation cannot complete prematurely; test interrupted distribution,
  stale-generation restart, and revocation with an unreachable removed node.
- Commit a mutation with a lost reply, revoke its principal, and retry. Preserve
  the unresolved client stream without exposing its result while unauthorized;
  after reauthorization, retry returns the original result exactly once.
- Fuzz framing and bounded decoding; repeat saturation and maximum-value tests
  with authentication enabled. Inspect output artifacts for credential leakage.

Every externally reachable operation has an authenticated principal, an explicit
authorization decision, and the existing consensus/state-machine checks.

## Phase 3 Expose bounded diagnostics

Primary changes: [main](src/main.rs), [HTTP status](src/runtime/http.rs),
[profiling](src/perf.rs), runtime/background-task error reporting, and new
metrics and audit modules.

### Implementation

- Install a configured `tracing` subscriber before configuration loading and
  runtime startup. Default production logs to structured JSON with timestamp,
  severity, build, node/group identity, and bounded request correlation. Do not
  log raw values, credentials, or arbitrary keys. See the
  [tracing subscriber model](https://docs.rs/tracing/latest/tracing/#subscribers).
- Add ordinary operational counters and latency histograms independent of
  `DAL_PROFILE_WRITE_PATH`. Keep the detailed benchmark probes optional.
  Bound metric labels; never label by arbitrary keys, queries, request IDs,
  or unlimited client identities.
- Export request latency/errors/retries, authentication refusals, admission
  saturation, Raft role and progress, visible/durable lag, recovery gate age,
  WAL failures, retained logs, snapshot work, search lag, outbox usage,
  backup age, lifecycle and maintenance state, trust rollout progress, and the
  active recovery epoch. Sample expensive disk/engine statistics
  outside the HTTP request path and report stale samples explicitly.
- Add audit events for authenticated administrative requests and their terminal
  outcomes, credential-policy changes, maintenance enter/release, backup/restore,
  and recovery-epoch changes. Link a committed operation to its Raft
  identifier when available; an attempted
  operation is not automatically a committed operation.
- Bound diagnostic queues. Logging or a metrics backend outage cannot block
  Raft or make buffers grow indefinitely. Expose dropped-log counts; keep
  critical shutdown/fatal failures visible through a fallback output.
- Audit aggregate resource limits, not only individual queue bounds. Account
  for total peer connections and in-flight RPCs, pending reply bytes,
  concurrent snapshots, loaded index writers, and retained logs. Bound each
  total against the assumed resource envelope; a bounded send queue alone
  does not bound requests already in flight.

### Tests and completion gate

Start the real binary and assert a structured startup event, an injected
runtime error, and a shutdown outcome reach its configured output. Verify
redaction, bounded label cardinality, and continued progress when the collector
is unavailable. Induce a stalled flush, disk-pressure signal, and search backlog
and verify the corresponding diagnostic signals.

The diagnostics identify the affected node and group, serving state, and
recovery blocker without requiring an expensive or authoritative HTTP read.

## Phase 4 Define the current format and restore metadata

Primary changes: [codec](src/codec.rs), [types](src/types.rs),
[storage open](src/storage/rocks.rs), state machines, and snapshot handling.

### Implementation

1. Define the current wire, durable record, snapshot, backup, and search-engine
   formats independently. Include maintenance operation/stage and commands,
   runtime fence acknowledgements, required trust generation, recovery epoch,
   and client error semantics before Phase 5 persists them.
2. Keep an atomically published format manifest on new data directories. Fail
   before database open on a missing, corrupt, or unrecognized marker in a
   nonempty directory. Earlier development directories are disposable; start
   with a fresh directory when the format changes.
3. Keep fixtures for the current binary's command, identity, snapshot, and
   database recovery paths where they help detect accidental changes within
   the current binary. Update or replace them when the current format
   deliberately changes. Do not build a source-to-target version matrix or
   migration path.
4. Bound client idempotency-record growth. Enforce a client-identity admission
   cap suited to the assumed workload, or define a durable session/expiry
   protocol that cannot turn an old retry into a fresh write. Arbitrary TTL
   deletion of sequence records is unsafe.

### Completion gate

The current binary opens newly created directories and restarts and replays its
own durable state. A malformed or unrecognized directory is rejected before
mutation. Phase 5 verifies backup and restore artifacts written by that binary.

## Phase 5 Implement coordinated backup and verified restore

Primary changes: [CLI](src/main.rs), runtime administration, group barriers,
[storage](src/storage/rocks.rs), [snapshot format](src/snapshot.rs), and proposed
`src/backup/` modules and `tests/backup_restore.rs`. Maintenance enforcement also
changes [failure detection](src/runtime/node.rs),
[rebalancing](src/runtime/rebalance.rs),
[automatic search transitions](src/runtime/search.rs), and both state machines.

Implementation status: the replicated `Preparing` record pauses automatic
meta writers and failure transitions. Participant runtimes start with client
admission closed, obtain a ReadIndex-confirmed maintenance view, drain accepted
writes, and persist the local generation fence before submitting an authenticated
acknowledgement. The meta leader advances to `Quiesced` only after every listed
participant acknowledges the same operation and generation. The replicated
`Verifying` stage keeps admission closed and requires a fresh recovery
acknowledgement from every participant before release. A runtime submits that
acknowledgement only after bootstrap and its hosted voting groups' recovery
gates are open; meta compares its voter-set digest with frozen placements and
records the durable process incarnation so a restarted node must acknowledge
again. Recovery acknowledgements also record each participant's effective
trust-policy digest. Release requires all digests to agree with the current
meta leader's policy, and an authenticated operator release request waits for
a new heartbeat from every exact recovery process before submitting its
directory and process bindings to replicated apply. An operator can inspect
the ReadIndex-confirmed record with `dal maintenance-status`. The
`maintenance-release` CLI addresses an exact operation and generation and uses
the typed operator RPC after recovery proofs are complete. The
[missing-participant recovery procedure](docs/operations/maintenance-recovery.md)
keeps the operation fenced until the same node returns with durable state and
reproves recovery. Permanent loss still requires the planned verified restore
path; no participant-removal or force-release path exists. Authenticated begin,
quiesce, verify, and release commands are implemented. Offline export checks a
stopped node's fence and recovery epoch, then packages full database checkpoints
from every Active participant only when voter applied positions and frozen meta
inventory agree. The package records versions, search definitions, trust state,
checksums, and the recovery point; verification precedes catalog publication.
Restore requires a cataloged package and operator fencing attestation, stages
each node under its recorded identity and topology, installs a fresh epoch, and
invalidates derived search checkpoints. Restored nodes require a newer trust
policy that excludes the backed-up credentials. Recovery acknowledgement waits
for active local search generations to have valid checkpoints, and an operator
can inspect retained client sequence outcomes under maintenance.

The offline exported-artifact drill in `tests/backup_restore.rs` verifies CLI
package inspection, catalog publication, restore initialization, dry-run and
restore, retained data and sequence state, epoch rejection, and trust exclusion.
It also rejects leftover partial export and publication stages, an incomplete
participant set, mixed artifacts, tampering, and an unknown package format.
A second drill exercises the secure three-process maintenance, backup, complete
loss, and restored-cluster release path. It passed with the system's CURVE-enabled
libzmq on 2026-10-10 (112 seconds), including CLI export of real Raft-applied
positions, rejection of an incomplete assembly and tampered package, old-policy
startup refusal, restored maintenance fencing, explicit verification and release,
old-epoch rejection, and retained data after complete loss. The default vendored
libzmq reports CURVE unavailable, so this drill skips in the default build; run
it with a CURVE-enabled libzmq and `DAL_REQUIRE_CURVE_TESTS=1` when changing this
path. The remaining failure and recovery checks below still need evidence before
Phase 5 can be declared verified.

### Maintenance protocol

- Store a unique operation ID, monotonically ordered fence generation, stage,
  and participant inventory in replicated meta state. Use explicit `Preparing`,
  `Quiesced`, `Verifying`, and `Released` stages with authenticated, resumable
  transitions. Include this state in snapshots and backups. Coordinator loss,
  process restart, and timeout never implicitly release maintenance.
- In `Preparing`, close new client mutations at gateways and partition entry
  points, block enrollment and new placement/index-definition changes, and
  drain already admitted mutations. Direct reads and searches may continue
  during preparation until planned shutdown, while `/readyz` remains closed.
  Stop failure-driven `SetNodeState` proposals
  and new placement plans before stopping any process. Continue heartbeat
  collection and expose prolonged maintenance through status and alerts.
  Resolve existing moves through a bounded completion or abort sequence; if
  they cannot settle, fail preparation and require an explicit recovery action.
- Before `Quiesced`, inventory every automatic meta writer and classify permitted
  transitions by stage. Cover `ReportSearchIndexReady`,
  `ActivateSearchIndexGeneration`, `FinalizeSearchIndexDrop`, `FinalizePlan`,
  `AbortReport`, and `MarkAborting`, as well as directory/membership commands.
  Pause their producers and enforce the stage/generation in meta application so
  a stale or already queued proposal cannot bypass the fence. Settle permitted
  in-flight work before the quiescence barrier. Keep Raft replication, elections,
  recovery, barriers, and narrowly scoped maintenance administration available.
- Every participating runtime durably records the installed fence before
  acknowledging that its admission is closed and accepted writes are drained.
  Do not declare `Quiesced` until all required acknowledgements match the same
  operation/generation and the meta freeze is applied. A restarting runtime,
  including a data leader or gateway without a meta replica, starts with client
  admission closed and obtains a ReadIndex-confirmed meta view. It may reopen
  only on an authoritative release at least as new as its durable fence and
  after its ordinary recovery gates pass. Stale cached release state cannot
  authorize reopening; a runtime joining the operation must acknowledge it.
- Capture barriers only in `Quiesced`. Directory, placement, and search metadata
  remain frozen through export and planned shutdown. `Verifying` keeps client
  writes fenced while allowing explicitly authorized restore/rebuild work;
  enumerate those internal commands and bind them to the operation. Entering
  verification invalidates any unfinished capture, which must be restarted
  through quiescence rather than mixing artifacts across stages.
- An authenticated release checks participant recovery, expected topology,
  trust-policy convergence, and any restored recovery epoch. Require fresh
  heartbeat evidence before rearming failure detection; discard pre-maintenance
  timeout decisions rather than replaying them as immediate `Down` transitions.
  Preserve prior directory state and incarnations, including any pre-existing
  exclusions. Aborting maintenance uses the same safe release checks. A missing
  participant leaves the operation fenced until an explicit recovery procedure
  resolves it; never silently reopen a partial cluster. An ordinary SIGTERM
  outside this protocol does not disable failure handling.

### Initial maintenance backup

1. Enter the maintenance protocol and reach `Quiesced`. Use the pending-preserving
   unavailable response from Phase 1 at every write entry point; declining this
   attempt does not establish the outcome of an earlier timed-out attempt.
   Confirm that automatic meta writers and failure-driven replacement are
   fenced, and keep peer and authenticated recovery/admin paths available.
2. Obtain a committed barrier for meta and every data group. Inventory all
   configured voters, partition placements, active state generations, identities,
   and search definitions. Require the full node set for this initial backup
   implementation; if a required member is unavailable, fail before publishing
   a usable backup rather than assemble an unproven subset.
3. Wait for each required replica to apply and durably cover the captured
   target, then shut down cleanly while the ingress fence remains closed.
   Verify no new client writes were admitted after admission closed, and no
   client mutations or frozen metadata changes crossed the quiescence barriers.
   Planned shutdown must not commit `Down` transitions or replacement plans.
4. Export each closed node database under exclusive ownership using RocksDB's
   supported checkpoint/backup mechanisms. Include all column families,
   replay-required logs, default-CF local authority, and database metadata.
   Copying selected SSTs or an individual partition snapshot is insufficient.
5. Publish a manifest only after all node artifacts are present and verified.
   Record a unique backup ID, cluster/node IDs, topology, per-group barrier and
   captured applied positions, binary/format versions, sizes, and checksums.
   Include the maintenance operation/generation, recovery epoch, and required
   trust generation; trust-policy contents and private keys are provisioned
   independently and cannot be rolled back from the backup.
   Include the original immutable bootstrap descriptor and current operational
   configuration with secrets excluded; do not regenerate genesis from the
   current placement map.
   Reject incomplete packages and artifacts from different backup runs.
6. Transfer the complete immutable package outside the cluster's failure domain
   using the configured backup destination. Apply access control, encryption,
   and authenticated manifest protection through established tooling. Verify
   the uploaded objects before marking success; an on-disk checkpoint or hard
   link on the source volume alone is not a disaster backup.
7. Persist completion in the backup catalog. For a normal backup, restart through
   the ordinary recovery gates, enter `Verifying`, and explicitly release
   maintenance after validation. Start all peers independently of client
   readiness. A failed
   backup leaves a clear resumable or abortable maintenance state. Retention
   cannot delete the last verified usable backup.

A [RocksDB checkpoint](https://github.com/facebook/rocksdb/wiki/Checkpoints)
captures one database. The cluster fence, inventory, manifest, and recovery
verification above supply the additional DAL guarantees. Record per-group
positions; the service does not otherwise offer a cross-partition transaction
snapshot. This initial backup intentionally requires a write-maintenance window.

### Restore procedure

- Add offline `backup verify` and `restore` commands with a dry-run inventory.
  Verify format support, all artifacts, checksums, free space, and destination
  identities before writing. Restore into fresh staged directories and refuse
  to overwrite a live or nonempty destination implicitly.
- Verify out-of-band fencing of the old deployment before any restored node
  connects: stop and disable its processes or isolate its peer and client
  networks, and prevent automatic restart or old endpoint routing. Record the
  checks and retain that isolation throughout recovery. Restore the documented
  topology and same node identities into an isolated environment; provision new
  server/node keys through a current trust generation that excludes old keys.
  Confirm convergence on the remaining authorized nodes and clients. Key
  rotation alone cannot prevent old servers from serving old clients. The normal
  restart/admission and quorum recovery paths must succeed without weakening
  amnesia or reclamation fences, while client readiness remains closed.
- The first supported procedure preserves the recorded node/address topology,
  using equivalent network configuration if hosts are replaced. Changing node
  identities, rewriting cluster IDs, or importing into a new topology requires
  a separately implemented offline recovery conversion and its own tests.
- Treat Tantivy files as rebuildable. If they are omitted, invalidate restored
  consumer checkpoints/retention state consistently and force rebuild from the
  restored authoritative source; stale consumer metadata cannot authorize
  incremental catch-up across a missing index. Verify strict search before
  opening search readiness.
- Use a node-wide search rebuild coordinator and disk-space admission check
  for automatic recovery, corrupt-index quarantine, explicit rebuilds, and
  restore reconstruction. Account for active, building, retiring, and
  quarantined generations. Keep a rebuild's reservation through retiring
  generation reclamation, serialize restore rebuilds through the same path,
  and never silently delete a quarantine to make room. Refuse a rebuild that
  cannot fit the assumed disk and memory limits.
- Validate the recorded data, versions, client sequence results, placements,
  Raft recovery, and index counts before reopening ingress. Measure elapsed
  restore time and the actual recovered point against Phase 0 objectives.
  Preserve the restored maintenance fence until an authenticated operator
  explicitly releases it after verification.

### Client streams after disaster recovery

- Distinguish ordinary restart and backup resume from disaster restore to an
  older point. The former preserve the recovery epoch and retry identity.
  The latter can lose acknowledged operations after the backup's recovery point;
  record that boundary and require application reconciliation before reopening.
- Provision a new unique recovery epoch for each disaster restore, recorded in
  the external recovery catalog and durably installed in meta and every data
  group while maintenance remains closed. Preserve cluster/node identities and
  restored sequence records. Include the epoch in mutation identity and enforce
  it at admission and deterministic application, so queued requests from an old
  epoch cannot be evaluated as fresh writes after the transition. Define these
  fields and commands under the current Phase 4 format contract.
- Return an explicit recovery-epoch mismatch for old client streams. Clients
  retain unresolved commands and stop automatic replay; they must not silently
  update the epoch, reset sequence counters, or assign old work a new identity.
  Applications reconcile the declared recovery point and explicitly establish
  new client identities/streams under the new epoch before sending fresh work.
  Any deliberate resubmission is a new application decision, not an exactly-once
  guarantee across lost history. Provide an authenticated operator path to
  inspect restored sequence outcomes without admitting old-epoch mutations.

### Tests and completion gate

Test interrupted export/upload/publication, truncated and tampered artifacts,
mixed backup IDs, unrecognized format markers, absent meta or partition artifacts,
disk exhaustion, and an active destination. Restore a populated cluster after
removing every original test data directory. Verify all writes acknowledged
before the backup fence, preserved sequence outcomes, and eventual index rebuild.
The test must exercise the CLI and exported artifacts, not an in-memory shortcut.

- Interrupt fence installation and release, change meta leadership, and restart
  a non-meta-voter data leader and a routing-only node. Each must stay closed
  until the authoritative operation permits serving; stale release caches and
  partial acknowledgements must not bypass maintenance.
- Race search readiness/activation/drop and move completion/abort with fence
  installation, including queued proposals. Verify forbidden transitions cannot
  cross quiescence, and authorized rebuild work runs only in verification.
- Sequentially stop and restart nodes for longer than the configured Suspect
  and Down timeouts with spare Active nodes present. Verify no unintended
  directory, incarnation, data-placement, or meta-membership changes, and no
  delayed timeout transition immediately after maintenance release.
- Lose a committed write's reply, install maintenance, and retry. After normal
  backup resume, the same command returns its original result. Verify the client
  never clears or reuses its pending sequence merely because admission closed.
- Back up a client at sequence 10, acknowledge 11–20, and leave another mutation
  ambiguous before total loss. After restore, reject both the old stream's next
  sequence 21 and its pending retry by epoch, without applying either. Exercise
  explicit application reconciliation and fresh streams; inspect retained
  outcomes for pre-backup writes through the authorized recovery path.
- Keep an old deployment running inside an isolated test network. Verify restore
  cannot proceed without fencing evidence, old endpoints remain unreachable to
  production clients, and old credentials cannot join the restored deployment.
- Cold-start every voter with the maintenance fence installed. Start peers
  without waiting for client readiness, form quorums, and verify that client
  readiness stays closed until recovery checks and explicit release succeed.

Exercise restore from exported artifacts with the command-line tools and verify
that the restored cluster remains fenced until its recovery checks pass.

### Additional work if online backup is implemented

Implement online coordination as a separate feature: pin the topology and
recovery-required prefixes for the entire capture, preserve CF/snapshot
lifecycle ownership, and prove the exported meta/data/log set can re-establish
committed state under concurrent writes and moves. Define the supported
cross-group recovery semantics explicitly. Independent live checkpoints from
arbitrary nodes do not satisfy this gate. Test capture, coordinator failure,
retention release, and restore under the full concurrent workload before relying
on online backups for recovery.

## Suggested pull request sequence

| Change | Reviewable result | Required evidence |
| --- | --- | --- |
| 1 | Workload assumptions and baseline fixtures | Documented technical limits and reproducible baseline |
| 2 | Lifecycle model and cheap probe source | Role-aware probe and bootstrap tests |
| 3 | Early signal handling, pending-preserving unavailable replies, and bounded shutdown | Child-process termination/recovery and lost-reply/drain tests; measured election failover |
| 4 | Structured logs and bounded metric collection | Real-binary output, redaction, scrape-cost tests |
| 5 | CURVE/ZAP socket setup and authenticated request context | Both-lane TCP authentication tests |
| 6 | Authorization, trust distribution, enrollment gates, and rotation/revocation | Partial rollout, established-session revocation, lost-reply/denial, forgery, and saturation tests |
| 7 | Current format, maintenance commands, and recovery-epoch schema | Fresh-directory replay, malformed-marker rejection, and current-binary fixtures |
| 8 | Replicated maintenance protocol and planned-stop integration | Automatic-writer races, non-meta node restart, timeout suppression, and pending-retry tests |
| 9 | Offline backup export and manifest | Interrupted, incomplete, and mixed-run backup tests |
| 10 | Offline restore, recovery epochs, and derived-index reconstruction | Complete loss/restore drill, old-deployment fencing, and client reconciliation |

Online backup support is separate implementation work if required. Do not
bundle a broad performance rewrite into the backup and restore changes.

## Validation commands and completion record

For implementation changes, run focused regressions first, then applicable
repository checks. Example commands are:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo test --locked --all-targets --all-features
```

Keep security, lifecycle, format, and restore checks tied to the current
revision and configuration. Record workload parameters for performance
measurements. Run performance measurements on an optimized binary and a
verified durable filesystem; record `DAL_BENCH_DIR` and every workload
parameter when ignored measurement cases are run.

Track each phase as proposed, in progress, or implemented, with the exact
revision and evidence when changing status. Update the applicable sections of
`DESIGN.md`, `IMPLEMENTATION.md`, and `RUNTIME_ARCHITECTURE.md` when behavior is
implemented; keep historical benchmark results labeled as historical evidence.
