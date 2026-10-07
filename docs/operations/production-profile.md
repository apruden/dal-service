# Production operating profile

Status: proposed qualification profile. Numeric targets and named owners require
agreement before production release. This document defines what Phase 7 must
measure; passing a local test does not qualify the service on independent hosts.

## Supported deployment candidate

| Area | Initial qualification target |
| --- | --- |
| Deployment | Linux service units on three independent hosts and failure domains. Kubernetes requires its own qualification. |
| Storage | Local SSDs with durable `fsync`, ext4, at least 500 GiB usable per host, and no shared volume between voters. Verify filesystem, device, and power-loss guarantees on the actual hosts. |
| Host resources | At least 8 vCPU and 32 GiB RAM per host for the trial; cap DAL RSS at 24 GiB. CPU, disk, and network contention must be recorded. These are test inputs, not proven minimums. |
| Network | TCP between hosts on a dedicated or measured network with at least 1 Gbit/s of usable capacity. Record RTT, loss, and bandwidth during tests. |
| Topology | Three meta voters, three data replicas per partition, initially three nodes. Test 1, 16, and 128 partitions; a larger cluster or more partitions needs separate qualification. |
| Data and search | Qualify at least 50 GiB of authoritative data per cluster and eight active search indexes. Exercise compaction and index rebuild at that size. The code's 1 MiB value cap and 64-index limit are protocol bounds, not qualified operating maxima. |
| Fault model | Crash, omission, delay, and disk failure of nodes or links; no Byzantine or stolen-node-key tolerance. One failed voter can retain a majority in each three-voter group if its two peers remain reachable and durable. A lost quorum stops authoritative writes and linearizable reads until recovery. |

Use the actual production disk and network configuration for final qualification.
The existing three-process TCP benchmarks ran on one host and cannot establish
cross-host latency, failure-domain independence, or these resource limits.

## Proposed service targets

These numbers are provisional engineering targets for workload design. The
engineering owner and operational reviewer must agree to them, or replace them
with product requirements, before a release can pass Phase 0. Measure after
steady-state warmup on the deployment above; include errors and timeouts in
the availability denominator.

| Measure | Proposed gate and measurement |
| --- | --- |
| 4 KiB writes | At least 300 successful writes/s sustained for 72 hours with 16 concurrent clients; p95 below 100 ms and p99 below 500 ms under normal load. |
| 1 MiB writes | At least 25 successful writes/s at 16 concurrent clients; p95 below 1.5 s and p99 below 5 s under normal load. Measure resident memory and replication lag. |
| Value classes | Run 128 B, 4 KiB, 64 KiB, and 1 MiB values, hot and broad keys, opaque and indexable values, with 1, 16, and 64 clients. Record all latency percentiles by operation and value class. |
| Search | p95 query latency below 500 ms for a declared query mix; p99 index freshness lag below 60 s after normal writes. Rebuild and concurrent search have separate reported results. |
| Failover | After one node crashes, resume successful writes within 15 s where every affected group retains quorum. Report p99 recovery time and ambiguous write outcomes separately. |
| Availability | At least 99% successful eligible operations per 30-day window, including planned maintenance. Safety requires zero lost acknowledged writes after any failure that leaves a durable quorum. |
| Backlog and resources | No unbounded retained logs or outbox growth. Keep per-group search outbox below the current 512 MiB configured cap and DAL RSS below 24 GiB on the trial hosts. Measure recovery time for maximum permitted backlog. |
| Disaster recovery | Provisional RPO at most 24 hours from the last verified off-host backup and RTO at most 4 hours for the 50 GiB qualification dataset. Report actual recovered point and timed restore. |

With the proposed 99% availability target, a 30-day period allows about 7.2
hours of total unavailability. Daily 10-minute offline backup windows use about
5 hours, leaving about 2.2 hours for upgrades and all other outages. The
maintenance design therefore passes only if timed backup and failure drills fit
that budget. If they do not, change the product target or implement the plan's
online backup and rolling upgrade gates before release. No backup duration or
availability percentage has been validated yet.

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

Commit `6fabff4aaed38ecd1693d93d4f941d038b052c63` passed
`cargo test --locked --all-targets --all-features` on 2026-10-06: 342 passed,
none failed, four ignored measurement tests. The leader-crash test ran its
default seeds 0 through 99 and completed in 307.47 s. The captured command
output is [archived here](evidence/baseline-6fabff4-all-features.log), SHA-256
`e4157ed81b3c6213ac83b2e9411191f94e784ffa214e9cf6c1600f0d12cdf8c9`.
The run used Rust 1.98.1 on a single development host and is regression evidence,
not production qualification. The log does not contain a deployment inventory,
so no host-capacity claim follows from it.
