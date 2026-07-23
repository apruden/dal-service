//! The node's inbound frame dispatcher (DESIGN §10.2, ground rule 9).
//!
//! One `ROUTER` receives every inbound frame; this [`Server`] splits them by
//! [`MsgType::is_peer_control`]: client frames go to the [`ClientGateway`],
//! peer/operator frames are routed here. Raft RPCs (append/vote/snapshot) go to
//! the addressed group's `Raft` handle; config observations and operator
//! commands (join/leave/abort-plan) are submitted to the meta group. A client
//! code path can never reach this half — the gateway refuses peer-control
//! frames — so plans and membership changes stay off the client surface.
//!
//! Deferred (M8 follow-up): `Heartbeat` liveness tracking and `BecomeLearner`
//! admission, which drive the failure detector and rebalancing loops.

use crate::api::gateway::{ClientGateway, PartitionMap};
use crate::codec;
use crate::meta::failure::HeartbeatTracker;
use crate::meta::node::{MetaNode, ProposeOutcome};
use crate::meta::raft_types::MetaTypeConfig;
use crate::partition::raft_types::TypeConfig;
use crate::runtime::node::PartitionStarter;
use crate::transport::Server;
use crate::transport::codec::{Envelope, MsgType};
use crate::transport::raft_wire::{
    AbortPlanBody, BecomeLearnerBody, BootstrapStatusBody, BootstrapStatusReply, HeartbeatBody,
    JoinBody, LeaveBody, LearnerReply, ObservationBody, PlacementQueryBody, PlacementQueryReply,
    SubmitReply,
};
use crate::types::{ClusterId, GroupId, MetaCommand, NodeState};

use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock milliseconds, the time base the [`HeartbeatTracker`] is fed.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Routes inbound frames to the client gateway or the peer/operator handlers.
pub struct RootDispatch {
    cluster_id: ClusterId,
    gateway: Arc<ClientGateway>,
    meta: Option<Arc<MetaNode>>,
    partitions: PartitionMap,
    heartbeats: Arc<Mutex<HeartbeatTracker>>,
    starter: Option<Arc<PartitionStarter>>,
}

impl RootDispatch {
    pub fn new(
        cluster_id: ClusterId,
        gateway: Arc<ClientGateway>,
        meta: Option<Arc<MetaNode>>,
        partitions: PartitionMap,
        heartbeats: Arc<Mutex<HeartbeatTracker>>,
        starter: Option<Arc<PartitionStarter>>,
    ) -> RootDispatch {
        RootDispatch {
            cluster_id,
            gateway,
            meta,
            partitions,
            heartbeats,
            starter,
        }
    }

    fn reply(&self, req: &Envelope, payload: Vec<u8>) -> Envelope {
        Envelope::new(
            self.cluster_id,
            req.msg_type,
            req.group_id,
            req.request_id,
            payload,
        )
    }

    /// A raft reply the sender cannot decode, surfaced as a network error on its
    /// side so openraft retries. Used when the target group is not hosted here or
    /// the request body is malformed.
    fn raft_unavailable(&self, req: &Envelope) -> Envelope {
        self.reply(req, Vec::new())
    }

    async fn serve_control(&self, req: Envelope) -> Envelope {
        match req.msg_type {
            MsgType::RaftAppend | MsgType::RaftVote | MsgType::RaftSnapshot => {
                self.serve_raft(req).await
            }
            MsgType::DataConfigObservation => self.serve_observation(req).await,
            MsgType::JoinRequest => self.serve_join(req).await,
            MsgType::LeaveRequest => self.serve_leave(req).await,
            MsgType::AbortPlanRequest => self.serve_abort_plan(req).await,
            MsgType::BootstrapStatus => self.serve_bootstrap_status(req).await,
            MsgType::PlacementQuery => self.serve_placement_query(req),
            MsgType::Heartbeat => self.serve_heartbeat(req),
            MsgType::BecomeLearner => self.serve_become_learner(req).await,
            // Client/reply types are handled before reaching serve_control (or
            // never arrive as requests); reply unusably so a sender retries.
            _ => self.raft_unavailable(&req),
        }
    }

    async fn serve_raft(&self, req: Envelope) -> Envelope {
        match req.group_id {
            GroupId::Meta => {
                let Some(meta) = self.meta.clone() else {
                    return self.raft_unavailable(&req);
                };
                let raft = meta.raft();
                match req.msg_type {
                    MsgType::RaftAppend => {
                        let Ok(rpc) =
                            codec::decode::<AppendEntriesRequest<MetaTypeConfig>>(&req.payload)
                        else {
                            return self.raft_unavailable(&req);
                        };
                        self.reply(&req, codec::encode(&raft.append_entries(rpc).await))
                    }
                    MsgType::RaftVote => {
                        let Ok(rpc) = codec::decode::<VoteRequest<u64>>(&req.payload) else {
                            return self.raft_unavailable(&req);
                        };
                        self.reply(&req, codec::encode(&raft.vote(rpc).await))
                    }
                    _ => {
                        let Ok(rpc) =
                            codec::decode::<InstallSnapshotRequest<MetaTypeConfig>>(&req.payload)
                        else {
                            return self.raft_unavailable(&req);
                        };
                        self.reply(&req, codec::encode(&raft.install_snapshot(rpc).await))
                    }
                }
            }
            GroupId::Data(p) => {
                let Some(node) = self.partitions.read().unwrap().get(&p).cloned() else {
                    return self.raft_unavailable(&req);
                };
                let raft = node.raft();
                match req.msg_type {
                    MsgType::RaftAppend => {
                        let Ok(rpc) =
                            codec::decode::<AppendEntriesRequest<TypeConfig>>(&req.payload)
                        else {
                            return self.raft_unavailable(&req);
                        };
                        self.reply(&req, codec::encode(&raft.append_entries(rpc).await))
                    }
                    MsgType::RaftVote => {
                        let Ok(rpc) = codec::decode::<VoteRequest<u64>>(&req.payload) else {
                            return self.raft_unavailable(&req);
                        };
                        self.reply(&req, codec::encode(&raft.vote(rpc).await))
                    }
                    _ => {
                        let Ok(rpc) =
                            codec::decode::<InstallSnapshotRequest<TypeConfig>>(&req.payload)
                        else {
                            return self.raft_unavailable(&req);
                        };
                        self.reply(&req, codec::encode(&raft.install_snapshot(rpc).await))
                    }
                }
            }
        }
    }

    /// Submit a mapped command to the meta group and reply with its outcome.
    async fn submit(&self, req: &Envelope, cmd: MetaCommand) -> Envelope {
        let reply = match self.meta.clone() {
            None => SubmitReply::Error("node does not run the meta group".into()),
            Some(meta) => match meta.propose(cmd).await {
                Ok(ProposeOutcome::Applied(outcome)) => SubmitReply::Outcome(outcome),
                Ok(ProposeOutcome::NotLeader { leader }) => SubmitReply::NotLeader { leader },
                Err(e) => SubmitReply::Error(e.to_string()),
            },
        };
        self.reply(req, codec::encode(&reply))
    }

    async fn serve_observation(&self, req: Envelope) -> Envelope {
        let Ok(body) = codec::decode::<ObservationBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&SubmitReply::Error("malformed".into())));
        };
        self.submit(&req, body.into_meta_command()).await
    }

    async fn serve_join(&self, req: Envelope) -> Envelope {
        let Ok(body) = codec::decode::<JoinBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&SubmitReply::Error("malformed".into())));
        };
        let cmd = MetaCommand::RegisterNode {
            node_id: body.node_id,
            control_addr: body.control_addr,
            bulk_addr: body.bulk_addr,
        };
        self.submit(&req, cmd).await
    }

    async fn serve_leave(&self, req: Envelope) -> Envelope {
        let Ok(body) = codec::decode::<LeaveBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&SubmitReply::Error("malformed".into())));
        };
        // Draining reuses the node's current incarnation (Active -> Draining is a
        // same-incarnation transition; DESIGN §9.2). We need the directory entry
        // to read it, which only a meta node can serve.
        let Some(meta) = self.meta.clone() else {
            return self.reply(
                &req,
                codec::encode(&SubmitReply::Error(
                    "node does not run the meta group".into(),
                )),
            );
        };
        let incarnation = match meta.local_node(body.node_id) {
            Ok(Some(entry)) => entry.incarnation,
            Ok(None) => {
                return self.reply(
                    &req,
                    codec::encode(&SubmitReply::Error(format!(
                        "node {} is not registered",
                        body.node_id
                    ))),
                );
            }
            Err(e) => return self.reply(&req, codec::encode(&SubmitReply::Error(e.to_string()))),
        };
        let cmd = MetaCommand::SetNodeState {
            node_id: body.node_id,
            state: NodeState::Draining,
            incarnation,
        };
        self.submit(&req, cmd).await
    }

    async fn serve_abort_plan(&self, req: Envelope) -> Envelope {
        let Ok(body) = codec::decode::<AbortPlanBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&SubmitReply::Error("malformed".into())));
        };
        let cmd = MetaCommand::MarkAborting {
            group: body.group,
            plan_id: body.plan_id,
        };
        self.submit(&req, cmd).await
    }

    /// Admit this node as a learner for the addressed group under the plan in
    /// the body (DESIGN §7.2). The group is created from the durable admission
    /// record and its Raft runtime starts uninitialized, ready to receive the
    /// data leader's `add_learner` catch-up.
    async fn serve_become_learner(&self, req: Envelope) -> Envelope {
        let Ok(body) = codec::decode::<BecomeLearnerBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&LearnerReply::Error("malformed".into())));
        };
        let reply = match &self.starter {
            None => LearnerReply::Error("node cannot host data partitions".into()),
            Some(starter) => match starter.admit_learner(req.group_id, body.plan_id).await {
                Ok(()) => LearnerReply::Admitted,
                Err(e) => LearnerReply::Error(e.to_string()),
            },
        };
        self.reply(&req, codec::encode(&reply))
    }

    /// Return a group's committed placement from local meta state, so a data
    /// leader that is not a meta voter can read its own plan. A local (committed)
    /// read suffices: a committed plan is stable until the move that this reply
    /// helps drive resolves it.
    fn serve_placement_query(&self, req: Envelope) -> Envelope {
        let placement = match (codec::decode::<PlacementQueryBody>(&req.payload), &self.meta) {
            (Ok(body), Some(meta)) => meta.local_placement(body.group).ok().flatten(),
            _ => None,
        };
        self.reply(&req, codec::encode(&PlacementQueryReply { placement }))
    }

    /// Record heartbeat liveness evidence. The reply is a bare ack — the emitter
    /// only needs to know the frame was delivered. A stale/replayed sequence is
    /// dropped by the tracker; the meta leader's detector loop turns accepted
    /// evidence into `SetNodeState` transitions.
    fn serve_heartbeat(&self, req: Envelope) -> Envelope {
        if let Ok(body) = codec::decode::<HeartbeatBody>(&req.payload) {
            self.heartbeats
                .lock()
                .unwrap()
                .observe(body.node_id, body.seq, now_ms());
        }
        self.reply(&req, Vec::new())
    }

    /// Confirm that a data placement has committed before its designated voter
    /// initializes the group. This uses a linearizable meta read so observing a
    /// reply is sufficient to establish the bootstrap phase ordering.
    async fn serve_bootstrap_status(&self, req: Envelope) -> Envelope {
        let Ok(body) = codec::decode::<BootstrapStatusBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&BootstrapStatusReply { ready: false }));
        };
        let ready = match self.meta.clone() {
            Some(meta) => match meta.read_placement(body.group).await {
                Ok(crate::meta::node::MetaRead::Value(Some(placement))) => {
                    placement.voters == body.voters && placement.r#move.is_none()
                }
                Ok(crate::meta::node::MetaRead::Value(None))
                | Ok(crate::meta::node::MetaRead::NotLeader { .. })
                | Err(_) => false,
            },
            None => false,
        };
        self.reply(&req, codec::encode(&BootstrapStatusReply { ready }))
    }
}

impl Server for RootDispatch {
    async fn serve(&self, request: Envelope) -> Envelope {
        // The gateway applies this check for client traffic. Peer-control
        // frames bypass the gateway, so enforce the same cluster boundary here
        // before a foreign Raft or operator request reaches a local group.
        if request.cluster_id != self.cluster_id {
            return self.raft_unavailable(&request);
        }
        if request.msg_type.is_peer_control() {
            self.serve_control(request).await
        } else {
            self.gateway.serve(request).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::RwLock;

    use crate::api::gateway::RoutingSource;
    use crate::api::ops::RoutingInfo;
    use crate::types::HashSpec;

    struct EmptyRouting;

    impl RoutingSource for EmptyRouting {
        fn routing(&self) -> RoutingInfo {
            RoutingInfo {
                cluster_id: 7,
                p: 1,
                hash_spec: HashSpec::CANONICAL,
                directory: Vec::new(),
                placements: Vec::new(),
            }
        }
    }

    #[tokio::test]
    async fn foreign_peer_control_is_rejected_before_dispatch() {
        let partitions = Arc::new(RwLock::new(HashMap::new()));
        let gateway = Arc::new(ClientGateway::new(
            7,
            1,
            HashSpec::CANONICAL,
            partitions.clone(),
            Arc::new(EmptyRouting),
        ));
        let dispatch = RootDispatch::new(
            7,
            gateway,
            None,
            partitions,
            Arc::new(Mutex::new(HeartbeatTracker::new())),
            None,
        );
        let request = Envelope::new(
            8,
            MsgType::JoinRequest,
            GroupId::Meta,
            42,
            codec::encode(&JoinBody {
                node_id: 9,
                control_addr: "tcp://foreign-control".into(),
                bulk_addr: "tcp://foreign-bulk".into(),
            }),
        );

        let reply = dispatch.serve(request).await;
        assert_eq!(reply.cluster_id, 7);
        assert_eq!(reply.msg_type, MsgType::JoinRequest);
        assert_eq!(reply.request_id, 42);
        assert!(reply.payload.is_empty());
    }
}
