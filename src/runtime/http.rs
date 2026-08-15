//! Read-only HTTP admin plane (DESIGN §10; `M8_HTTP_STATUS_PLAN.md`).
//!
//! A third inbound plane alongside the ZMQ control and bulk lanes, bound on
//! `http_addr`. It exposes `GET /status` and `GET /health` and nothing else:
//! observability only, off the correctness path, so it can never be a side door
//! to consensus or operator commands (ground rule 9). `/status` is built from
//! node-local Raft metrics and cached reads — never a linearizable round trip —
//! so it answers cheaply even during an election.

use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::get};
use serde::{Deserialize, Serialize};

use crate::types::{NodeDirectoryEntry, NodeId};

/// A node's self-reported status. All fields come from node-local state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub node_id: NodeId,
    /// Cluster id as a hex string (JSON cannot carry a u128 reliably).
    pub cluster_id: String,
    pub protocol_version: u32,
    /// Database-wide fail-stop state. This includes meta-only nodes and remains
    /// true for the rest of the process after any materialization/WAL failure.
    pub storage_failed: bool,
    /// Present when this node runs the meta group.
    pub meta: Option<MetaStatus>,
    pub partitions: Vec<PartitionStatus>,
    /// The committed node directory as this node sees it (empty on non-meta
    /// nodes). Reflects failure-detector transitions.
    pub directory: Vec<NodeDirectoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaStatus {
    pub is_leader: bool,
    pub leader: Option<NodeId>,
    pub applied: Option<u64>,
    pub voters: Vec<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Leader,
    Voter,
    Learner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionStatus {
    pub partition: u16,
    pub role: Role,
    pub leader: Option<NodeId>,
    pub applied: Option<u64>,
    /// Highest state-machine entry visible to reads in this process.
    pub materialized_visible: Option<u64>,
    /// Highest state-machine entry covered by a completed RocksDB WAL flush.
    pub materialized_durable: Option<u64>,
    pub materialized_pending_entries: usize,
    pub materialized_pending_bytes: usize,
    pub materialized_oldest_pending_ms: Option<u64>,
    pub materialized_last_flush_latency_ms: Option<u64>,
    pub materialized_max_flush_latency_ms: u64,
    pub materialized_flush_failures: u64,
    /// Whether this process epoch may serve local/stale state after recovery.
    pub materialized_recovery_ready: bool,
    pub materialized_recovery_epoch: u64,
    pub materialized_recovery_target: Option<u64>,
    pub materialized_recovery_attempts: u64,
    pub materialized_recovery_duration_ms: Option<u64>,
    pub materialized_replayed_entries: u64,
    pub materialized_purge_waits: u64,
    pub materialized_purge_wait_ms: u64,
    pub materialized_snapshot_waits: u64,
    pub materialized_snapshot_wait_ms: u64,
    pub materialized_retained_log_entries: u64,
    pub materialized_failed: bool,
    pub committed_voters: Vec<NodeId>,
    pub serving: bool,
    pub plan: Option<PlanStatus>,
    /// Derived search projections hosted for this partition (design §14).
    /// Search failure never changes `/health` — an unusable index degrades
    /// search only — so this is where that degradation becomes diagnosable.
    pub search: Vec<SearchIndexStatus>,
    /// Retained dirty-key journal for the partition, shared by every
    /// generation below. Growth here is what the lag budget bounds.
    pub search_outbox_entries: u64,
    pub search_outbox_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchIndexState {
    /// Loaded and reachable as the index's current generation.
    Active,
    /// Loaded and projecting, but not yet the generation searches resolve to.
    Building,
    /// Released from outbox retention for exceeding its lag budget; it will
    /// rebuild from authoritative state before it can serve again.
    NeedsRebuild,
    /// Loaded but holding no checkpoint valid for the live projection epoch.
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchIndexStatus {
    pub name: String,
    pub generation: u64,
    /// Hex-encoded, so a mismatch between replicas is visible at a glance.
    pub definition_hash: String,
    pub state: SearchIndexState,
    pub projection_epoch: u64,
    /// Source prefix the loaded Tantivy commit covers.
    pub searchable_through: Option<u64>,
    /// Applied entries not yet reflected in the loaded commit.
    pub lag_entries: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStatus {
    pub plan_id: u64,
    pub aborting: bool,
}

/// Supplies a node-local status snapshot. Implemented by the runtime node;
/// the HTTP layer depends only on this so it stays decoupled and testable.
pub trait StatusSource: Send + Sync {
    fn status(&self) -> ClusterStatus;
}

/// The admin router: `GET /status` and `GET /health`.
pub fn router(src: Arc<dyn StatusSource>) -> Router {
    Router::new()
        .route("/status", get(status))
        .route("/health", get(health))
        .with_state(src)
}

async fn status(State(src): State<Arc<dyn StatusSource>>) -> Json<ClusterStatus> {
    Json(src.status())
}

async fn health(
    State(src): State<Arc<dyn StatusSource>>,
) -> (axum::http::StatusCode, &'static str) {
    let status = src.status();
    if status.storage_failed
        || status
            .partitions
            .iter()
            .any(|partition| partition.materialized_failed)
    {
        (axum::http::StatusCode::SERVICE_UNAVAILABLE, "failed")
    } else {
        (axum::http::StatusCode::OK, "ok")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    struct StubStatus;
    impl StatusSource for StubStatus {
        fn status(&self) -> ClusterStatus {
            ClusterStatus {
                node_id: 7,
                cluster_id: "0xda1".into(),
                protocol_version: 1,
                storage_failed: false,
                meta: Some(MetaStatus {
                    is_leader: true,
                    leader: Some(7),
                    applied: Some(42),
                    voters: vec![7, 8, 9],
                }),
                partitions: vec![PartitionStatus {
                    partition: 0,
                    role: Role::Leader,
                    leader: Some(7),
                    applied: Some(10),
                    materialized_visible: Some(10),
                    materialized_durable: Some(9),
                    materialized_pending_entries: 1,
                    materialized_pending_bytes: 128,
                    materialized_oldest_pending_ms: Some(4),
                    materialized_last_flush_latency_ms: Some(3),
                    materialized_max_flush_latency_ms: 7,
                    materialized_flush_failures: 0,
                    materialized_recovery_ready: true,
                    materialized_recovery_epoch: 2,
                    materialized_recovery_target: Some(10),
                    materialized_recovery_attempts: 1,
                    materialized_recovery_duration_ms: Some(5),
                    materialized_replayed_entries: 2,
                    materialized_purge_waits: 1,
                    materialized_purge_wait_ms: 2,
                    materialized_snapshot_waits: 1,
                    materialized_snapshot_wait_ms: 3,
                    materialized_retained_log_entries: 1,
                    materialized_failed: false,
                    committed_voters: vec![7, 8, 9],
                    serving: true,
                    plan: None,
                    search: vec![SearchIndexStatus {
                        name: "articles".into(),
                        generation: 12,
                        definition_hash: "ab".repeat(32),
                        state: SearchIndexState::Active,
                        projection_epoch: 1,
                        searchable_through: Some(10),
                        lag_entries: 0,
                    }],
                    search_outbox_entries: 3,
                    search_outbox_bytes: 96,
                }],
                directory: Vec::new(),
            }
        }
    }

    struct FailedStatus;
    impl StatusSource for FailedStatus {
        fn status(&self) -> ClusterStatus {
            let mut status = StubStatus.status();
            // Model a meta-only node: database health must not depend on a
            // data partition being present in the status response.
            status.storage_failed = true;
            status.partitions.clear();
            status
        }
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let app = router(Arc::new(StubStatus));
        let resp = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_fails_closed_with_materialization_failure() {
        let app = router(Arc::new(FailedStatus));
        let resp = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn status_returns_the_snapshot_as_json() {
        let app = router(Arc::new(StubStatus));
        let resp = app
            .oneshot(Request::get("/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let got: ClusterStatus = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(got, StubStatus.status());
        assert_eq!(got.node_id, 7);
        assert_eq!(got.partitions[0].role, Role::Leader);
        assert_eq!(got.partitions[0].search[0].name, "articles");
        assert_eq!(got.partitions[0].search_outbox_entries, 3);
    }

    /// Design §14: an unusable derived index degrades search only. It must not
    /// change the database health result, which is what an operator's failover
    /// automation keys on.
    #[tokio::test]
    async fn a_failed_search_index_does_not_change_health() {
        struct BrokenIndex;
        impl StatusSource for BrokenIndex {
            fn status(&self) -> ClusterStatus {
                let mut status = StubStatus.status();
                status.partitions[0].search[0].state = SearchIndexState::Failed;
                status.partitions[0].search[0].searchable_through = None;
                status.partitions[0].search[0].lag_entries = 10;
                status
            }
        }
        let resp = router(Arc::new(BrokenIndex))
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
