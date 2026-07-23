# M8 — HTTP status plane (implementation plan)

Read-only HTTP observability plane for a running node, exposing `/status` and
`/health`. Operator mutations stay on the typed ZMQ control frames (ground rule
9); HTTP is observability only.

## Locked decisions

- **axum** (0.8) for the HTTP server; accept the hyper 1 + tower dependency.
- **`/status` is node-local, best-effort** — built from local `RaftMetrics` and
  cached reads, never `ensure_linearizable`, so it answers during elections.
- **`dal status` stays on ZMQ** (`MetaQuery` / `RoutingInfo`, linearizable). The
  CLI does *not* call HTTP. `/status` is the machine/ops-facing surface only.
- **Plan reporting: id + state only** — no `plan.age` in v1 (avoids adding a
  timestamp to the placement record).
- **HTTP is read-only** — routes are `GET /status` and `GET /health`. No
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
- `GET /health` → `200 "ok"` (liveness only; reads no state).
- Router: `http::router(src)` returns the axum `Router`; the caller owns the
  listener and the serving task.

**As built (drift from the original sketch):** there is no `serve(addr, src,
shutdown)` helper with `.with_graceful_shutdown`. `Node::start` binds the
`TcpListener` itself — before storage opens, so a bad `http_addr` fails startup
(`Error::Config` on parse, `Error::Io` on bind) without leaking background
tasks — then spawns `axum::serve(listener, http::router(src))` into the node's
task set. Shutdown aborts that task with the other loops rather than driving a
graceful-shutdown future.

## Step 3 — CLI / runtime wiring (built)

- `Node::start` parses and binds `cfg.http_addr` up front (before opening
  storage), then spawns the axum serve task over that listener. `None` → the
  plane is not started.
- The `StatusSource` is a dedicated `NodeStatus` struct (holding `node_id`,
  cluster config, `meta`, and the shared `partitions` handle), not `Node`
  itself.
- `dal status` is unchanged by this work (stays on ZMQ); the `status` CLI
  subcommand is currently a stub (see IMPLEMENTATION.md §2 M8 open items).

## Step 4 — Tests (`src/runtime/http.rs` `#[cfg(test)]`)

- Stub `StatusSource` returning a fixed `ClusterStatus`; `oneshot` a
  `GET /status`, assert JSON body decodes back to the fixture.
- `GET /health` returns 200 unconditionally.
- `ClusterStatus` serialize → deserialize equality (round-trip).
- No sockets, no runtime, no tokio multi-thread needed for these.

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
