# M8 — HTTP status plane (implementation plan)

Read-only HTTP observability plane for a running node, exposing `/status` and
`/health`; production lifecycle work later added `/livez` and `/readyz` while
retaining `/health` as a compatibility liveness alias. Operator mutations stay
on the typed ZMQ control frames (ground rule
9); HTTP is observability only.

## Locked decisions

- **axum** (0.8) for the HTTP server; accept the hyper 1 + tower dependency.
- **`/status` is node-local, best-effort** — built from local `RaftMetrics` and
  cached reads, never `ensure_linearizable`, so it answers during elections.
- **`dal status` stays on ZMQ** (`MetaQuery` / `RoutingInfo`, linearizable). The
  CLI does *not* call HTTP. `/status` is the machine/ops-facing surface only.
- **Plan reporting: id + state only** — no `plan.age` in v1 (avoids adding a
  timestamp to the placement record).
- **HTTP is read-only** — routes are `GET /status`, `GET /health`, `GET /livez`,
  and `GET /readyz`. No
  mutation routes.
- The HTTP layer is built and tested against a narrow `StatusSource` trait so it
  did not block on the runtime. (The full `runtime::Node` assembly has since
  landed and implements that source via a `NodeStatus` struct.)

## Dependencies (`Cargo.toml`)

- Add `axum = "0.8"`.
- Add `"net"` to the tokio feature list (`axum::serve` needs
  `tokio::net::TcpListener`). Current: `["rt","rt-multi-thread","sync","macros","time"]`.
- Add `tower = "0.5"` to `[dev-dependencies]` for `ServiceExt::oneshot` in tests.
- `serde` / `serde_json` already present.

## Module layout

```
src/runtime/
  mod.rs          // add: pub mod http;  (pub mod node; added later)
  http.rs         // axum router, handlers, ClusterStatus DTO, StatusSource trait
  config_file.rs  // unchanged
```

## Step 1 — `StatusSource` trait + DTO (`src/runtime/http.rs`)

Decouples HTTP from the concrete node (built later as `NodeStatus`).

```rust
pub trait StatusSource: Send + Sync {
    fn status(&self) -> ClusterStatus;   // cheap, node-local, no quorum round-trip
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterStatus {
    pub node_id: NodeId,
    pub cluster_id: String,        // hex, via config_file::cluster_id_hex
    pub protocol_version: u32,
    pub meta: Option<MetaStatus>,  // None if this node does not run meta
    pub partitions: Vec<PartitionStatus>,
    pub directory: Vec<NodeDirectoryEntry>,
    pub placements: Vec<(u16, Placement)>,
}

pub struct MetaStatus   { is_voter: bool, leader: Option<NodeId>, applied: Option<u64>, voters: Vec<NodeId> }
pub struct PartitionStatus {
    id: u16,
    role: Role,                    // Leader | Voter | Learner
    applied: Option<u64>,
    committed_lag: u64,            // last_log_index - last_applied
    serving: bool,
    plan: Option<PlanStatus>,      // { id, state } — no age
}
```

Field → source mapping:

| Field | Source |
|---|---|
| `node_id`, `cluster_id`, `protocol_version` | `NodeConfig` / `ClusterConfig` |
| `meta.*` | `MetaNode::voters/current_leader/applied_index` |
| `partitions[].role/applied/committed_lag` | `PartitionNode::current_leader/applied_index` + `raft().metrics()` |
| `partitions[].serving` | `Storage` serving-gate record for the group |
| `partitions[].plan` | `MetaNode::local_placement(group)` (cached, non-linearizable) |
| `directory`, `placements` | cached `RoutingInfo` |

## Step 2 — axum router + handlers (`src/runtime/http.rs`)

- `GET /status` → `Json<ClusterStatus>` from `State<Arc<dyn StatusSource>>`.
- `GET /health` → compatibility liveness alias: `200 "ok"` while lifecycle,
  identity, cached storage health, and diagnostic freshness are healthy;
  otherwise `503 "failed"`.
- `GET /livez` has the same health decision with `"live"` as its success body.
  `GET /readyz` additionally requires lifecycle `Running` and every hosted
  voter group to pass its serving and recovery gates. Learners do not close
  process readiness.
- Router: `http::router(src)` returns the axum `Router`; the caller owns the
  listener and the serving task.

`Node::start` binds the `TcpListener` itself before storage opens, so a bad
`http_addr` fails startup (`Error::Config` on parse, `Error::Io` on bind)
without leaking background tasks. The node owns the HTTP server and diagnostic
sampler separately. Shutdown first changes lifecycle to `Draining`, joins any
in-flight blocking diagnostic collection, then drives axum's graceful-shutdown
future after the data plane has drained. Probes therefore remain available
during the drain.

## Step 3 — CLI / runtime wiring (built)

- `Node::start` parses and binds `cfg.http_addr` up front (before opening
  storage), then spawns the axum serve task over that listener. `None` → the
  plane is not started.
- The `StatusSource` is a dedicated `NodeStatus` struct (holding `node_id`,
  cluster config, `meta`, and the shared `partitions` handle), not `Node`
  itself.
- A background task samples that source once per second. Request handlers only
  clone the cached snapshot or read precomputed health flags. A sample older
  than three seconds closes liveness, and a sampler panic changes lifecycle to
  `Failed`. A separate storage fail-stop watcher also closes liveness without
  waiting for the next sample.
- `dal status` is unchanged by this work (stays on ZMQ); the `status` CLI
  subcommand is currently a stub (see IMPLEMENTATION.md §2 M8 open items).

## Step 4 — Tests (`src/runtime/http.rs` `#[cfg(test)]`)

- Stub `StatusSource` returning a fixed `ClusterStatus`; `oneshot` a
  `GET /status`, assert JSON body decodes back to the fixture.
- Probe handlers never call the live collector; storage, identity, stale-sample,
  lifecycle, voter, and learner cases exercise the cached decisions.
- New status fields accept older JSON payloads through serde defaults.
- Sampler tests cover a blocking collection during shutdown and a collector
  panic changing lifecycle to `Failed`.

## Explicitly deferred

- `runtime::Node` process assembly and `dal run` — **since built** (separate M8
  slice, now landed; see RUNTIME_ARCHITECTURE.md and IMPLEMENTATION.md §2 M8).
- `plan.age` (needs a timestamp on the placement/plan record).
- `/metrics`, per-node counters — room left in the router, not built now.
- Operator mutations over HTTP — stay on ZMQ control frames.

## Correctness notes

- `/status` performs no consensus reads: it cannot block on quorum and cannot be
  a side-door to meta commands.
- HTTP is a third inbound plane (alongside ZMQ control + bulk), off the
  correctness path — consistent with ground rule 3 (transport isolation) and
  rule 9 (no client/HTTP path to peer-control).
