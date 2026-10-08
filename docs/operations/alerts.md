# DAL alert response

Load [`deploy/prometheus/dal-alerts.yml`](../../deploy/prometheus/dal-alerts.yml)
as a Prometheus rule file and scrape each node's `/metrics` endpoint. These
rules use only metrics currently exported by DAL. Thresholds are initial
operational defaults; validate them against the deployed workload before
enabling paging. A local node metric does not establish cluster-wide quorum.
The cached `/status` sample includes `storage_filesystem_available_bytes` and
`storage_filesystem_total_bytes`. `/metrics` exports these as
`dal_storage_filesystem_available_bytes` and
`dal_storage_filesystem_total_bytes`, plus `dal_storage_filesystem_sample_ok`.
The capacity gauges are absent when sampling fails.

## DALStorageFailed

Stop routing new client traffic to the affected node. Preserve its data
directory and logs for diagnosis. Check the storage failure message and disk
and filesystem health; do not delete WAL or database files to clear the alert.
Restore service only after the node passes its normal storage and recovery
gates.

## DALDiskHeadroomLow

Check the data filesystem's available and total bytes, including other users
of the volume. The 40–45% warning band precedes the proposed 40% qualification
floor. Find growth in RocksDB, retained logs, search generations, and backup
staging before scheduling cleanup. Preserve live WAL and SST files and the log
purge fence. Verify free space recovers above the alert threshold.

## DALDiskHeadroomCritical

The filesystem is below the proposed 40% operating floor. Pause capacity-heavy
work and investigate immediately using the disk-headroom procedure above.
Restore headroom before resuming rebuilds, staging, or expansion; verify the
sample and free-space ratio recover on the affected node.

## DALDiskCapacitySampleFailed

Check the data directory mount and node logs for filesystem errors. Confirm
the filesystem is accessible to the DAL service user, then verify that
`dal_storage_filesystem_sample_ok` returns to 1. Treat the free-space state as
unknown until sampling recovers.

## DALPartitionNotRecovering

Inspect `/status` for the partition recovery epoch, attempts, applied and
durable positions, and local readiness. Check peer reachability and snapshot
transfer progress. Keep the node out of client routing until the recovery gate
opens; do not force readiness manually.

## DALRaftTermChurn

Inspect the affected group's current leader, term, voter set, peer reachability,
and node lifecycle in `/status`. Check recent node restarts, network loss, and
disk stalls. The term counter can advance during unsuccessful campaigns; a
change does not prove a leader was elected. Verify the term stabilizes and
operations on the affected group resume.
One election can raise this ticket on several voters; group alerts by the
affected cluster and group during triage.

## DALLeaderQuorumAckStale

The local leader has not seen a recent quorum acknowledgement. Check voter
connectivity and durable replication progress; avoid planned voter stops until
the group is stable. The metric exists only while this node has a leader-side
acknowledgement sample. Its absence cannot prove cluster-wide quorum, so use
external client canaries to establish service availability. Verify the age
falls and valid operations succeed. DAL advances a published Openraft age using
its own monotonic clock; an age that rises while the term and applied index are
static can indicate a stalled Raft core.

## DALDiagnosticSampleStale

Check the node logs and status worker for a blocked filesystem or runtime task.
`/metrics` serves the last cached sample and marks it stale after three seconds.
Treat its other values as historical until `dal_diagnostic_sample_stale` returns
to zero. Confirm the node's liveness and readiness probes recover.

## DALMaterializationLagGrowing

Inspect pending entries and bytes for the partition in `/status`. Check storage
latency, WAL pressure, and competing snapshot or search work. Escalate if the
lag keeps growing or approaches configured capacity; the one-minute threshold
is a signal, not a data-loss boundary.

## DALClientOpHighLatency

Compare gateway p99 with request volume, reply outcomes, admission rejections,
durable lag, and disk pressure on the affected node. The histogram measures
time inside the client gateway, including authorization and reply encoding; it
does not include transport or client retry time. Its fixed buckets are 10, 25,
50, 100, 250, 500, 1000, and 5000 ms plus infinity. The ClientOp outcome counter has
fixed applied, condition_failed, rejected, value, redirect, refused, error,
unavailable, and denied classes. Verify latency and errors return to their
normal workload baseline before closing the incident.

## DALLogDrops

Inspect the configured log collector and node output immediately. The dropped
counter is cumulative; confirm the alert expression observes a positive
increase. Resolve collector backpressure or output failure and retain local
fallback diagnostics for incident review.

## DALAuthorizationDenialsSpike

Review the authenticated-admin audit events and recent trust-policy or
credential changes. Confirm operators and peer pins against the approved
configuration. The metric does not identify principals, and the threshold can
also be triggered by stale or misconfigured clients; do not weaken transport
authentication to silence it.

## DALAuthenticationDenialsSpike

Inspect the node's CURVE/ZAP health, recent trust-policy changes, peer pins,
and source connection volume. This counter increments for rejected ZAP
decisions before a request reaches authorization, including fail-closed
responses after authenticator failure. Investigate a flood or stale credential
rollout without weakening the trust policy. Verify the denial rate falls and
legitimate peers can reconnect.

## Coverage limitations

The current endpoint does not expose backup age or end-to-end client latency and
cluster-wide quorum. These rules therefore do
not cover those release gates. Add corresponding instrumentation and alerts
before relying on them for production operations.
