# Production operating profile

Status: proposed qualification profile. Numeric targets and named owners require
agreement before production release. This document defines what Phase 7 must
measure; passing a local test does not qualify the service on independent hosts.

## Supported deployment candidate

| Area | Initial qualification target |
| --- | --- |
| Deployment | Linux service units on three independent hosts and failure domains. Kubernetes requires its own qualification. |
| Storage | Local SSDs with durable `fsync`, ext4, at least 3 TiB usable per host, and no shared volume between voters. Verify filesystem, device, and power-loss guarantees on the actual hosts. |
| Host resources | At least 8 vCPU and 32 GiB RAM per host for the trial. DAL RSS below 24 GiB is an acceptance threshold measured under load, not an enforced process cap. CPU, disk, and network contention must be recorded. These are test inputs, not proven minimums. |
| Network | TCP between hosts on a dedicated or measured network with at least 1 Gbit/s of usable capacity. Record RTT, loss, and bandwidth during tests. |
| Topology | Three meta voters, three data replicas per partition, initially three nodes. Qualify at most 16 partitions with eight active search indexes for the initial operating envelope. Test 1 and 16 partitions at full load; 128 partitions is an exploratory scalability case until its aggregate memory use is bounded and separately qualified. |
| Data and search | Qualify at least 50 GiB of authoritative data per cluster at the 16-partition, eight-index limit. Exercise compaction and index rebuild at that size. The code's 1 MiB value cap and 64-index limit are protocol bounds, not qualified operating maxima. |
| Fault model | Crash, omission, delay, and disk failure of nodes or links; no Byzantine or stolen-node-key tolerance. Each three-voter group tolerates one failed voter if the other two remain reachable and durable. Losing a data-group quorum stops writes and linearizable reads for that partition. Stale reads may continue only until election timeout clears the cached leader; they then fail until quorum returns. Losing the meta quorum stops membership, placement, enrollment, search-catalog changes, and search queries, while point operations on data groups with quorum can still serve. |

Use the actual production disk and network configuration for final qualification.
The existing three-process TCP benchmarks ran on one host and cannot establish
cross-host latency, failure-domain independence, or these resource limits.

At the proposed maximum, 16 partitions × 8 indexes × the current 50 MB Tantivy
writer budget is 6.4 GB of configured writer budgets per node; one serialized
full-index rebuild can add another 0.8 GB across 16 partitions. This is before
RocksDB buffers, Raft state, transport queues, and operating-system overhead.
At 128 partitions the active-writer product becomes 51.2 GB per node and cannot
fit the proposed 32 GiB host. Phase 6 must bound aggregate search writers and RocksDB memory,
or the supported partition/index combination must be reduced before claiming
the 24 GiB RSS target. An operating-system memory limit would add an OOM failure
mode and requires separate fault testing; this profile does not assume one.

## Proposed service targets

These numbers are provisional engineering targets for workload design. The
engineering owner and operational reviewer must agree to them, or replace them
with product requirements, before a release can pass Phase 0. Measure after
steady-state warmup on the deployment above. Record success, refusal, timeout,
and error counts separately, including while maintenance closes client ingress.

| Measure | Proposed gate and measurement |
| --- | --- |
| 4 KiB writes | At least 300 successful writes/s sustained for 72 hours with 16 concurrent clients; p95 below 100 ms and p99 below 500 ms under normal load. Use a fixed keyspace with overwrites so the live dataset remains at about 50 GiB. |
| 1 MiB writes | At least 25 successful writes/s for a one-hour trial at 16 concurrent clients; p95 below 1.5 s and p99 below 5 s under normal load. Use a fixed keyspace and measure disk writes, RSS, and replication lag. |
| Mixed KV load | At least 500 successful operations/s for 72 hours with 16 clients: 60% linearizable reads, 10% stale reads, 20% writes, and 10% CAS. Use 70% 4 KiB, 15% 128 B, 10% 64 KiB, and 5% 1 MiB values by operation count. Use the fixed mixed keyspace below so mutations overwrite existing keys. Measure per-operation p95/p99; proposed limits are 100/500 ms for reads and 150/750 ms for writes and CAS. |
| Search | At least 50 queries/s for one hour, 50% single-term strict text, 25% multi-term strict text, and 25% `MatchAll`, across the eight indexes. p95 below 500 ms; p99 index freshness lag below 60 s after normal writes. Report rebuild and concurrent search separately. |
| Value classes and concurrency | Also run isolated 128 B, 4 KiB, 64 KiB, and 1 MiB values; hot and broad keys; opaque and indexable values; and 1, 16, and 64 clients. Record all latency percentiles by operation and value class. Peak supported concurrency is provisionally 64 clients. |
| Failover | Run at least 300 independent node-crash trials where every affected group retains quorum. Every trial must resume successful writes within 15 s; also report empirical p99 and maximum, ambiguous write outcomes, and quorum-loss intervals. Zero failures in 300 trials gives about 95% confidence that the within-15-second rate exceeds 99%. |
| Shutdown | Under normal load, close readiness before admission, drain, and stop within the proposed 60 s application deadline and 90 s supervisor grace period in every qualification trial. Report p99 and maximum drain time and classify forced stops as crashes. |
| Availability and error rate | At least 99% time availability in each 30-day window, including planned maintenance. Send rate-limited valid read/write canaries to every partition and count the cluster unavailable if any required partition fails; report per-partition results too. Measure meta-dependent search separately. A failed or timed-out canary counts unavailable, while deterministically invalid input is excluded. Separately, below 0.1% errors/timeouts on valid operations during normal load with quorum. Safety requires zero lost acknowledged writes after any failure that leaves a durable quorum. |
| Backlog and resources | Under the supported workload, zero forced search-consumer releases or rebuilds due to the compiled-in outbox bounds of 2,000,000 entries and 512 MiB per group. Keep retained Raft logs below 1 GiB per group and catch one node up from the resulting 17 GiB maximum across 16 data groups plus meta within 10 minutes. Add retained-byte accounting before enforcing this gate. Measure DAL RSS below 24 GiB and at least 40% free disk on each trial host. Implement aggregate bounds or reduce the envelope if these targets fail. |
| Client identities | Applications reuse stable client identities; ephemeral per-request identities are unsupported. The operator trust policy explicitly binds permitted IDs to application principals and contains at most 100,000 IDs cluster-wide. Provision no more than 200 new IDs/day: one year × that rate × a 1.25 safety factor is at most 91,250. Alert at 80% and reject policy additions over the cap without affecting existing streams. Before one year or exhaustion, implement safe expiry or qualify a larger cap. Include the resulting per-partition sequence records in disk accounting. |
| Disaster recovery | Provisional RPO at most 24 hours, measured from the last verified off-host backup, and RTO at most 4 hours for the 50 GiB dataset. Backup cadence must be shorter than 24 hours by the worst-case backup completion delay; report both actual recovered point and timed restore. |

Run each isolated value-size trial on a fresh 50 GiB dataset rather than adding
it to the previous trial: about 12.5 million 4 KiB keys or 50,000 1 MiB keys,
with equivalent sizing for the other classes. For the mixed soak, use about
8 million 4 KiB keys, 2 million 128 B keys, 250,000 64 KiB keys, and 4,000
1 MiB keys, distributed across every partition. This is about 50 GiB of value
payload before keys and metadata. All mutations select from that fixed keyspace.

Budget each 3 TiB host conservatively: at most 150 GiB for authoritative RocksDB
state plus compaction headroom, 17 GiB for the 1 GiB retained-log limit across
16 data groups plus meta, 900 GiB for eight active search projections, at most
one quarantine per index, and one concurrent full-index rebuild, 150 GiB for
backup/checkpoint staging, and 50 GiB for service logs and temporary files.
Phase 6 adds a node-wide rebuild coordinator and disk preflight. An existing
quarantine blocks silent replacement and raises an operator alert; the runbook
archives or removes it before another quarantine for that index. Restore rebuilds
run through the same coordinator. Keep at least 40% (about 1.2 TiB) free
throughout qualification. Measure actual projection, quarantine, and rebuild
size rather than assuming each equals source size, plus physical write volume
and disk amplification; reduce the operating envelope if any component exceeds
its budget.

The 17 GiB whole-node retained-log budget takes about 2.4 minutes to transfer
over an ideal 1 Gbit/s link. The 10-minute catch-up target leaves room for
protocol overhead and concurrent foreground traffic; qualification must measure
that contention rather than relying on the ideal calculation.

The provisional 99% time-availability target permits about 7.2 hours of total
unavailability in 30 days. A 50 GiB transfer over an ideal 1 Gbit/s link alone
takes about 7.2 minutes per copy. The offline procedure also needs quiescence,
shutdown, checksums, verification, restart, and release; transferring multiple
replicas or using a slower backup destination increases the window. A daily
offline backup cannot credibly fit the availability budget at this dataset
size, and a backup started exactly every 24 hours cannot guarantee a 24-hour
RPO once capture and verification take time. For these provisional targets,
the plan's **online backup gates are required before release**. Schedule
verified off-host backups more often than every 24 hours, with a margin for the
measured worst-case completion delay. Maintenance-mode backup remains the first
implementation and upgrade safety path. Planned upgrade downtime also counts
against 7.2 hours. If agreed product targets differ, repeat this feasibility
calculation; no availability or recovery objective has been qualified yet.

## Release evidence and responsibility

| Gate | Engineering owner | Operational reviewer | Evidence required |
| --- | --- | --- | --- |
| Capacity and latency | Service maintainer, named before release | Operations lead, named before release | Independent-host workload history and resource report |
| Security and enrollment | Security implementer, named before release | Security reviewer, named before release | Authenticated transport, denial, rotation, and revocation tests |
| Backup and restore | Storage implementer, named before release | Operator other than implementer, named before release | Verified artifact and timed CLI restore drill |
| Upgrade and rollback | Release engineer, named before release | Operations lead, named before release | Source/target digests, migration fixture, and rollback drill |
| Rollout | Release engineer, named before release | On-call owner, named before release | Staging and pilot observations, alert response, and handoff |

Store each run's exact revision, binary digest, `Cargo.lock`, Rust toolchain,
host and filesystem details, environment settings, seed list, load parameters,
raw operation history, logs, and results in durable release artifacts. Results
without those identifiers are diagnostic only. The 72-hour soak and seven-day
pilot specified in the [readiness plan](../../PRODUCTION_READINESS_IMPLEMENTATION_PLAN.md)
remain release gates.

## Archived starting point

The original [captured test log](evidence/baseline-unattributed-all-features.log)
reports 342 passed, none failed, and four ignored measurement tests; its
leader-crash test took 307.47 s. SHA-256:
`e4157ed81b3c6213ac83b2e9411191f94e784ffa214e9cf6c1600f0d12cdf8c9`.
That log lacks a revision header, toolchain inventory, and per-seed output, so
it is diagnostic evidence.

The [provenance-captured run](evidence/baseline-57e7c3e-all-features.log)
records commit `57e7c3e75d2c80d1e1f0628714e4a2347c5d045f`, UTC time,
Rust and Cargo versions, the exact command, and every verification seed from 0
through 99. It passed 342 tests with none failed and four ignored measurement
tests; the leader-crash campaign took 302.93 s. SHA-256:
`0dbdd5715c44cd933472afc457eed72be8760bcf8a1284ff5be1f8f1c151675e`.
This is regression evidence on one development host; it does not qualify the
independent-host production profile.
