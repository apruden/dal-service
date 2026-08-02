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
    /// Whether this process epoch may serve local/stale state after recovery.
    pub materialized_recovery_ready: bool,
    pub materialized_failed: bool,
    pub committed_voters: Vec<NodeId>,
    pub serving: bool,
    pub plan: Option<PlanStatus>,
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

async fn health() -> &'static str {
    "ok"
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
                    materialized_recovery_ready: true,
                    materialized_failed: false,
                    committed_voters: vec![7, 8, 9],
                    serving: true,
                    plan: None,
                }],
                directory: Vec::new(),
            }
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
    }
}
