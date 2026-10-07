# Production operating profile

Status: proposed qualification profile. Numeric targets and named owners require
agreement before production release. This document defines what Phase 7 must
measure; passing a local test does not qualify the service on independent hosts.

## Supported deployment candidate

| Area | Initial qualification target |
| --- | --- |
| Deployment | Linux service units on three independent hosts and failure domains. Kubernetes requires its own qualification. |
| Storage | Local SSDs with durable `fsync`, ext4, at least 500 GiB usable per host, and no shared volume between voters. Verify filesystem, device, and power-loss guarantees on the actual hosts. |
| Host resources | At least 8 vCPU and 32 GiB RAM per host for the trial. DAL RSS below 24 GiB is an acceptance threshold measured under load, not an enforced process cap. CPU, disk, and network contention must be recorded. These are test inputs, not proven minimums. |
| Network | TCP between hosts on a dedicated or measured network with at least 1 Gbit/s of usable capacity. Record RTT, loss, and bandwidth during tests. |
| Topology | Three meta voters, three data replicas per partition, initially three nodes. Qualify at most 16 partitions with eight active search indexes for the initial operating envelope. Test 1 and 16 partitions at full load; 128 partitions is an exploratory scalability case until its aggregate memory use is bounded and separately qualified. |
| Data and search | Qualify at least 50 GiB of authoritative data per cluster at the 16-partition, eight-index limit. Exercise compaction and index rebuild at that size. The code's 1 MiB value cap and 64-index limit are protocol bounds, not qualified operating maxima. |
| Fault model | Crash, omission, delay, and disk failure of nodes or links; no Byzantine or stolen-node-key tolerance. Each three-voter group tolerates one failed voter if the other two remain reachable and durable. Losing a data-group quorum stops writes and linearizable reads for that partition; stale reads may continue. Losing the meta quorum stops membership, placement, enrollment, and search-catalog changes while data groups with quorum can still serve. |

Use the actual production disk and network configuration for final qualification.
The existing three-process TCP benchmarks ran on one host and cannot establish
cross-host latency, failure-domain independence, or these resource limits.

At the proposed maximum, 16 partitions × 8 indexes × the current 50 MB Tantivy
writer budget is 6.4 GB of configured writer budgets per node, before RocksDB
buffers, Raft state, transport queues, and operating-system overhead. At 128
partitions that product becomes 51.2 GB per node and cannot fit the proposed
32 GiB host. Phase 6 must bound aggregate search writers and RocksDB memory,
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
| Mixed KV load | At least 500 successful operations/s for 72 hours with 16 clients: 60% linearizable reads, 10% stale reads, 20% writes, and 10% CAS. Use 70% 4 KiB, 15% 128 B, 10% 64 KiB, and 5% 1 MiB values by operation count. Measure per-operation p95/p99; proposed limits are 100/500 ms for reads and 150/750 ms for writes and CAS. |
| Search | At least 50 queries/s for one hour, 50% single-term strict text, 25% multi-term strict text, and 25% `MatchAll`, across the eight indexes. p95 below 500 ms; p99 index freshness lag below 60 s after normal writes. Report rebuild and concurrent search separately. |
| Value classes and concurrency | Also run isolated 128 B, 4 KiB, 64 KiB, and 1 MiB values; hot and broad keys; opaque and indexable values; and 1, 16, and 64 clients. Record all latency percentiles by operation and value class. Peak supported concurrency is provisionally 64 clients. |
| Failover | After one node crashes, p99 time to resume successful writes at most 15 s where every affected group retains quorum. Report the maximum, ambiguous write outcomes, and quorum-loss intervals separately. |
| Availability and error rate | At least 99% time availability in each 30-day window, including planned maintenance. A rate-limited valid write/read canary defines availability; a failed or timed-out canary counts unavailable, while deterministically invalid input is excluded. Separately, below 0.1% errors/timeouts on valid operations during normal load with quorum. Safety requires zero lost acknowledged writes after any failure that leaves a durable quorum. |
| Backlog and resources | Under the supported workload, zero forced search-consumer releases or rebuilds due to the compiled-in outbox bounds of 2,000,000 entries and 512 MiB per group. Keep retained Raft logs below 4 GiB per data group and catch a follower up from that backlog within 10 minutes. Measure DAL RSS below 24 GiB and at least 40% free disk on each trial host. Implement aggregate bounds or reduce the envelope if these targets fail. |
| Disaster recovery | Provisional RPO at most 24 hours, measured from the last verified off-host backup, and RTO at most 4 hours for the 50 GiB dataset. Backup cadence must be shorter than 24 hours by the worst-case backup completion delay; report both actual recovered point and timed restore. |

Use a fixed set of roughly ten million 4 KiB keys for the steady-state 4 KiB
soak and overwrite existing keys. The one-hour 1 MiB trial uses at most 50,000
keys. Start with about 50 GiB of live data and verify that compaction, Raft
retention, search indexes, and backup staging leave at least 200 GiB free on
each 500 GiB host. Report physical write volume and disk amplification.

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
records clean commit `57e7c3e75d2c80d1e1f0628714e4a2347c5d045f`, UTC time,
Rust and Cargo versions, the exact command, and every verification seed from 0
through 99. It passed 342 tests with none failed and four ignored measurement
tests; the leader-crash campaign took 302.93 s. SHA-256:
`0dbdd5715c44cd933472afc457eed72be8760bcf8a1284ff5be1f8f1c151675e`.
This is regression evidence on one development host; it does not qualify the
independent-host production profile.
