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
//! `Heartbeat` liveness tracking feeds the failure detector; `BecomeLearner`
//! admission drives rebalancing. The operator commands (`join`/`leave`/
//! `abort-plan`) are issued by the `dal` CLI via [`crate::runtime::admin`].

use crate::api::gateway::{ClientGateway, PartitionMap};
use crate::codec;
use crate::meta::failure::HeartbeatTracker;
use crate::meta::node::{MetaNode, ProposeOutcome};
use crate::meta::raft_types::MetaTypeConfig;
use crate::partition::raft_types::TypeConfig;
use crate::perf::WriteStage;
use crate::runtime::node::{MetaHandle, MetaStarter, PartitionStarter};
use crate::transport::Server;
use crate::transport::codec::{Envelope, MsgType};
use crate::transport::raft_wire::{
    AbortPlanBody, BecomeLearnerBody, BootstrapStatusBody, BootstrapStatusReply,
    DirectoryQueryReply, HeartbeatBody, JoinBody, LeadershipTransferReply, LearnerReply, LeaveBody,
    MaintenanceAckBody, MaintenanceQueryReply, MaintenanceRecoveryAckBody, ObservationBody,
    PlacementQueryBody, PlacementQueryReply, RecoveryFenceReply, RecoveryFenceRequest,
    SearchIndexAdminBody, SearchIndexStatusBody, SearchIndexStatusReply, SubmitReply,
};
use crate::transport::security::{Permission, TrustPolicy};
use crate::types::{ClusterId, GroupId, MetaCommand, NodeId, NodeState, ProcessIdentityGate};

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
    meta: MetaHandle,
    partitions: PartitionMap,
    heartbeats: Arc<Mutex<HeartbeatTracker>>,
    starter: Option<Arc<PartitionStarter>>,
    meta_starter: Option<Arc<MetaStarter>>,
    identity_gate: ProcessIdentityGate,
    trust_policy: Option<Arc<TrustPolicy>>,
    server_key_principals: Option<std::collections::BTreeMap<String, String>>,
}

impl RootDispatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_id: ClusterId,
        gateway: Arc<ClientGateway>,
        meta: MetaHandle,
        partitions: PartitionMap,
        heartbeats: Arc<Mutex<HeartbeatTracker>>,
        starter: Option<Arc<PartitionStarter>>,
        meta_starter: Option<Arc<MetaStarter>>,
    ) -> RootDispatch {
        RootDispatch {
            cluster_id,
            gateway,
            meta,
            partitions,
            heartbeats,
            starter,
            meta_starter,
            identity_gate: ProcessIdentityGate::default(),
            trust_policy: None,
            server_key_principals: None,
        }
    }

    pub fn with_identity_gate(mut self, identity_gate: ProcessIdentityGate) -> Self {
        self.identity_gate = identity_gate;
        self
    }

    pub fn with_trust_policy(mut self, policy: Arc<TrustPolicy>) -> Self {
        self.trust_policy = Some(policy);
        self
    }

    pub fn with_server_key_principals(
        mut self,
        principals: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.server_key_principals = Some(principals);
        self
    }

    /// Snapshot the meta handle; the guard is dropped before any `.await`.
    fn meta(&self) -> Option<Arc<MetaNode>> {
        self.meta.read().unwrap().clone()
    }

    fn authenticated_as_node(
        &self,
        context: &crate::transport::RequestContext,
        node_id: NodeId,
    ) -> bool {
        if self.trust_policy.is_none() {
            return true;
        }
        context
            .authenticated_user_id
            .as_deref()
            .and_then(|identity| identity.strip_prefix("node:"))
            .and_then(|id| id.parse::<NodeId>().ok())
            == Some(node_id)
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

    async fn serve_control(
        &self,
        req: Envelope,
        context: crate::transport::RequestContext,
    ) -> Envelope {
        match req.msg_type {
            MsgType::RaftAppend | MsgType::RaftVote | MsgType::RaftSnapshot => {
                self.serve_raft(req, &context).await
            }
            MsgType::DataConfigObservation => self.serve_observation(req, &context).await,
            MsgType::JoinRequest => self.serve_join(req).await,
            MsgType::LeaveRequest => self.serve_leave(req).await,
            MsgType::AbortPlanRequest => self.serve_abort_plan(req).await,
            MsgType::BootstrapStatus => self.serve_bootstrap_status(req).await,
            MsgType::PlacementQuery => self.serve_placement_query(req).await,
            MsgType::DirectoryQuery => self.serve_directory_query(req).await,
            MsgType::MaintenanceQuery | MsgType::MaintenanceStatus => {
                self.serve_maintenance_query(req).await
            }
            MsgType::MaintenanceAck => self.serve_maintenance_ack(req, &context).await,
            MsgType::MaintenanceRecoveryAck => {
                self.serve_maintenance_recovery_ack(req, &context).await
            }
            MsgType::DataRecoveryFence => self.serve_recovery_fence(req, &context).await,
            MsgType::Heartbeat => self.serve_heartbeat(req, &context),
            MsgType::BecomeLearner => self.serve_become_learner(req).await,
            MsgType::SearchPrepare => self.serve_search_prepare(req).await,
            MsgType::SearchExecute => self.serve_search_execute(req).await,
            MsgType::SearchIndexStatus => self.serve_search_index_status(req, &context).await,
            MsgType::SearchIndexAdmin => self.serve_search_index_admin(req).await,
            MsgType::TransferLeadership => self.serve_transfer_leadership(req).await,
            // Client/reply types are handled before reaching serve_control (or
            // never arrive as requests); reply unusably so a sender retries.
            _ => self.raft_unavailable(&req),
        }
    }

    async fn serve_raft(
        &self,
        req: Envelope,
        context: &crate::transport::RequestContext,
    ) -> Envelope {
        match req.group_id {
            GroupId::Meta => {
                let Some(meta) = self.meta() else {
                    return self.raft_unavailable(&req);
                };
                if !meta.raft_storage_healthy() {
                    return self.raft_unavailable(&req);
                }
                let raft = meta.raft();
                match req.msg_type {
                    MsgType::RaftAppend => {
                        let Ok(rpc) =
                            codec::decode::<AppendEntriesRequest<MetaTypeConfig>>(&req.payload)
                        else {
                            return self.raft_unavailable(&req);
                        };
                        if !rpc
                            .vote
                            .leader_id
                            .voted_for()
                            .is_some_and(|id| self.authenticated_as_node(context, id))
                        {
                            return self.raft_unavailable(&req);
                        }
                        self.reply(&req, codec::encode(&raft.append_entries(rpc).await))
                    }
                    MsgType::RaftVote => {
                        let Ok(rpc) = codec::decode::<VoteRequest<u64>>(&req.payload) else {
                            return self.raft_unavailable(&req);
                        };
                        if !rpc
                            .vote
                            .leader_id
                            .voted_for()
                            .is_some_and(|id| self.authenticated_as_node(context, id))
                        {
                            return self.raft_unavailable(&req);
                        }
                        self.reply(&req, codec::encode(&raft.vote(rpc).await))
                    }
                    _ => {
                        let Ok(rpc) =
                            codec::decode::<InstallSnapshotRequest<MetaTypeConfig>>(&req.payload)
                        else {
                            return self.raft_unavailable(&req);
                        };
                        if !rpc
                            .vote
                            .leader_id
                            .voted_for()
                            .is_some_and(|id| self.authenticated_as_node(context, id))
                        {
                            return self.raft_unavailable(&req);
                        }
                        self.reply(&req, codec::encode(&raft.install_snapshot(rpc).await))
                    }
                }
            }
            GroupId::Data(p) => {
                let Some(node) = self.partitions.read().unwrap().get(&p).cloned() else {
                    return self.raft_unavailable(&req);
                };
                if !node.raft_storage_healthy() {
                    return self.raft_unavailable(&req);
                }
                let raft = node.raft();
                match req.msg_type {
                    MsgType::RaftAppend => {
                        let _profile = crate::perf::timer(WriteStage::RaftAppendHandle);
                        let Ok(rpc) =
                            codec::decode::<AppendEntriesRequest<TypeConfig>>(&req.payload)
                        else {
                            return self.raft_unavailable(&req);
                        };
                        if !rpc
                            .vote
                            .leader_id
                            .voted_for()
                            .is_some_and(|id| self.authenticated_as_node(context, id))
                        {
                            return self.raft_unavailable(&req);
                        }
                        self.reply(&req, codec::encode(&raft.append_entries(rpc).await))
                    }
                    MsgType::RaftVote => {
                        let Ok(rpc) = codec::decode::<VoteRequest<u64>>(&req.payload) else {
                            return self.raft_unavailable(&req);
                        };
                        if !rpc
                            .vote
                            .leader_id
                            .voted_for()
                            .is_some_and(|id| self.authenticated_as_node(context, id))
                        {
                            return self.raft_unavailable(&req);
                        }
                        self.reply(&req, codec::encode(&raft.vote(rpc).await))
                    }
                    _ => {
                        let Ok(rpc) =
                            codec::decode::<InstallSnapshotRequest<TypeConfig>>(&req.payload)
                        else {
                            return self.raft_unavailable(&req);
                        };
                        if !rpc
                            .vote
                            .leader_id
                            .voted_for()
                            .is_some_and(|id| self.authenticated_as_node(context, id))
                        {
                            return self.raft_unavailable(&req);
                        }
                        self.reply(&req, codec::encode(&raft.install_snapshot(rpc).await))
                    }
                }
            }
        }
    }

    /// Submit a mapped command to the meta group and reply with its outcome.
    async fn submit(&self, req: &Envelope, cmd: MetaCommand) -> Envelope {
        let reply = match self.meta() {
            None => SubmitReply::Error("node does not run the meta group".into()),
            Some(meta) => match meta.propose(cmd).await {
                Ok(ProposeOutcome::Applied(outcome)) => SubmitReply::Outcome(outcome),
                Ok(ProposeOutcome::NotLeader { leader }) => SubmitReply::NotLeader { leader },
                Err(e) => SubmitReply::Error(e.to_string()),
            },
        };
        self.reply(req, codec::encode(&reply))
    }

    async fn serve_observation(
        &self,
        req: Envelope,
        context: &crate::transport::RequestContext,
    ) -> Envelope {
        let Ok(body) = codec::decode::<ObservationBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&SubmitReply::Error("malformed".into())));
        };
        let sender_node_id = match &body {
            ObservationBody::Finalize { sender_node_id, .. }
            | ObservationBody::Abort { sender_node_id, .. } => *sender_node_id,
        };
        if !self.authenticated_as_node(context, sender_node_id) {
            return self.reply(
                &req,
                codec::encode(&SubmitReply::Error(
                    "authenticated node does not match observation sender".into(),
                )),
            );
        }
        self.submit(&req, body.into_meta_command()).await
    }

    async fn serve_join(&self, req: Envelope) -> Envelope {
        let Ok(body) = codec::decode::<JoinBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&SubmitReply::Error("malformed".into())));
        };
        if body.node_id == 0
            || body.control_addr.is_empty()
            || body.bulk_addr.is_empty()
            || body.control_addr.len() > crate::types::MAX_ENDPOINT_BYTES
            || body.bulk_addr.len() > crate::types::MAX_ENDPOINT_BYTES
            || body.control_addr == body.bulk_addr
        {
            return self.reply(
                &req,
                codec::encode(&SubmitReply::Error(
                    "invalid joining node identity or endpoints".into(),
                )),
            );
        }
        if let Some(policy) = &self.trust_policy {
            if body.policy_generation != policy.generation() {
                return self.reply(
                    &req,
                    codec::encode(&SubmitReply::Error(
                        "joining node trust-policy generation does not match this peer".into(),
                    )),
                );
            }
            let encoded_key = match zmq::z85_encode(&body.curve_public_key) {
                Ok(encoded) => encoded,
                Err(_) => {
                    return self.reply(
                        &req,
                        codec::encode(&SubmitReply::Error(
                            "joining node CURVE public key is invalid".into(),
                        )),
                    );
                }
            };
            let expected_principal = format!("node:{}", body.node_id);
            let candidate_trusted =
                policy
                    .principal_for_curve_key(&encoded_key)
                    .is_some_and(|principal| {
                        principal.user_id == expected_principal
                            && principal.permissions.contains(&Permission::PeerRpc)
                    });
            if !candidate_trusted {
                return self.reply(
                    &req,
                    codec::encode(&SubmitReply::Error(format!(
                        "trust policy does not bind the joining key to {expected_principal}"
                    ))),
                );
            }
            let pins_candidate = self.server_key_principals.as_ref().is_some_and(|pins| {
                pins.get(&body.control_addr) == Some(&expected_principal)
                    && pins.get(&body.bulk_addr) == Some(&expected_principal)
            });
            if !pins_candidate {
                return self.reply(
                    &req,
                    codec::encode(&SubmitReply::Error(format!(
                        "this peer has no control and bulk pins for {expected_principal}"
                    ))),
                );
            }
        } else if body.preflight {
            return self.reply(
                &req,
                codec::encode(&SubmitReply::Error(
                    "join preflight requires authenticated transport".into(),
                )),
            );
        }
        if body.preflight {
            return self.reply(&req, codec::encode(&SubmitReply::JoinPreflightReady));
        }
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
        let Some(meta) = self.meta() else {
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

    async fn serve_search_index_admin(&self, req: Envelope) -> Envelope {
        let Ok(body) = codec::decode::<SearchIndexAdminBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&SubmitReply::Error("malformed".into())));
        };
        let cmd = match body {
            SearchIndexAdminBody::Create { name, definition } => {
                MetaCommand::CreateSearchIndexWithRevision {
                    name,
                    definition,
                    engine_revision: crate::search::SEARCH_ENGINE_REVISION,
                }
            }
            SearchIndexAdminBody::Rebuild { name, definition } => {
                MetaCommand::CreateSearchIndexGenerationWithRevision {
                    name,
                    definition,
                    engine_revision: crate::search::SEARCH_ENGINE_REVISION,
                }
            }
            SearchIndexAdminBody::Drop { name } => MetaCommand::DropSearchIndex { name },
        };
        self.submit(&req, cmd).await
    }

    async fn serve_transfer_leadership(&self, req: Envelope) -> Envelope {
        let reply = match self.meta() {
            Some(meta) => match meta.trigger_election().await {
                Ok(()) => LeadershipTransferReply::Triggered,
                Err(error) => LeadershipTransferReply::Error(error.to_string()),
            },
            None => LeadershipTransferReply::Error("node does not run the meta group".into()),
        };
        self.reply(&req, codec::encode(&reply))
    }

    /// Admit this node as a learner for the addressed group under the plan in
    /// the body (DESIGN §7.2). The group is created from the durable admission
    /// record and its Raft runtime starts uninitialized, ready to receive the
    /// data leader's `add_learner` catch-up.
    async fn serve_become_learner(&self, req: Envelope) -> Envelope {
        let Ok(body) = codec::decode::<BecomeLearnerBody>(&req.payload) else {
            return self.reply(
                &req,
                codec::encode(&LearnerReply::Error("malformed".into())),
            );
        };
        let admit = match req.group_id {
            GroupId::Meta => match &self.meta_starter {
                Some(meta_starter) => meta_starter.admit_meta_learner(body.plan_id).await,
                None => Err(crate::error::Error::Raft(
                    "node cannot host the meta group".into(),
                )),
            },
            GroupId::Data(_) => match &self.starter {
                Some(starter) => starter.admit_learner(req.group_id, body.plan_id).await,
                None => Err(crate::error::Error::Raft(
                    "node cannot host data partitions".into(),
                )),
            },
        };
        let reply = match admit {
            Ok(()) => LearnerReply::Admitted,
            Err(e) => LearnerReply::Error(e.to_string()),
        };
        self.reply(&req, codec::encode(&reply))
    }

    /// Phase one of a coordinated search: fence this replica as leader, pin a
    /// searcher, and report its BM25 statistics. Peer-only, because the held
    /// session it creates is an internal handle rather than a client result.
    async fn serve_search_prepare(&self, req: Envelope) -> Envelope {
        let reply = match (
            req.group_id,
            codec::decode::<crate::search::ShardPrepareRequest>(&req.payload),
        ) {
            (GroupId::Data(partition), Ok(body)) => {
                let node = self.partitions.read().unwrap().get(&partition).cloned();
                match (node, self.gateway.search_service()) {
                    (Some(node), Some(search)) => match search.shard_prepare(&node, &body).await {
                        Ok(reply) => reply,
                        Err(error) => crate::search::ShardPrepareReply::Error(error.to_string()),
                    },
                    // Not hosting the partition is a routing miss, not a
                    // failure: answer as a redirect so the coordinator keeps
                    // looking instead of failing the whole fan-out.
                    (None, _) => crate::search::ShardPrepareReply::NotLeader { leader: None },
                    (_, None) => crate::search::ShardPrepareReply::Error(
                        "search service is not configured".into(),
                    ),
                }
            }
            _ => crate::search::ShardPrepareReply::Error(
                "SearchPrepare must address a data partition".into(),
            ),
        };
        self.reply(&req, codec::encode(&reply))
    }

    /// Phase two: score the session's pinned searcher with the coordinator's
    /// summed statistics.
    async fn serve_search_execute(&self, req: Envelope) -> Envelope {
        let reply = match codec::decode::<crate::search::ShardExecuteBody>(&req.payload) {
            Ok(body) => match self.gateway.search_service() {
                Some(search) => match body {
                    crate::search::ShardExecuteBody::Execute(request) => {
                        search.shard_execute(&request).await
                    }
                    // The coordinator is abandoning the query; the session is
                    // gone either way, so report it as such.
                    crate::search::ShardExecuteBody::Release { session_id } => {
                        search.release_session(session_id);
                        crate::search::ShardExecuteReply::UnknownSession
                    }
                },
                None => crate::search::ShardExecuteReply::Error(
                    "search service is not configured".into(),
                ),
            },
            Err(error) => {
                crate::search::ShardExecuteReply::Error(format!("malformed SearchExecute: {error}"))
            }
        };
        self.reply(&req, codec::encode(&reply))
    }

    /// Either relay a replica's readiness observation to the meta group, or
    /// answer whether this node has built the generations a move driver needs
    /// before it promotes this node to voter (design §5.2, §11.1).
    async fn serve_search_index_status(
        &self,
        req: Envelope,
        context: &crate::transport::RequestContext,
    ) -> Envelope {
        let Ok(body) = codec::decode::<SearchIndexStatusBody>(&req.payload) else {
            return self.reply(
                &req,
                codec::encode(&SearchIndexStatusReply::Submitted(SubmitReply::Error(
                    "malformed".into(),
                ))),
            );
        };
        match body {
            SearchIndexStatusBody::Report { name, observation } => {
                if !self.authenticated_as_node(context, observation.node_id) {
                    return self.reply(
                        &req,
                        codec::encode(&SearchIndexStatusReply::Submitted(SubmitReply::Error(
                            "authenticated node does not match observation node_id".into(),
                        ))),
                    );
                }
                let cmd = MetaCommand::ReportSearchIndexReady { name, observation };
                let outcome = match self.meta() {
                    None => SubmitReply::Error("node does not run the meta group".into()),
                    Some(meta) => match meta.propose(cmd).await {
                        Ok(ProposeOutcome::Applied(outcome)) => SubmitReply::Outcome(outcome),
                        Ok(ProposeOutcome::NotLeader { leader }) => {
                            SubmitReply::NotLeader { leader }
                        }
                        Err(error) => SubmitReply::Error(error.to_string()),
                    },
                };
                self.reply(
                    &req,
                    codec::encode(&SearchIndexStatusReply::Submitted(outcome)),
                )
            }
            SearchIndexStatusBody::Query { group, generations } => {
                let reply = self.search_generations_ready(group, &generations).await;
                self.reply(&req, codec::encode(&reply))
            }
        }
    }

    async fn search_generations_ready(
        &self,
        group: GroupId,
        generations: &[(String, u64, [u8; 32])],
    ) -> SearchIndexStatusReply {
        let GroupId::Data(partition) = group else {
            return SearchIndexStatusReply::Status {
                ready: false,
                detail: "search readiness requires a data partition".into(),
            };
        };
        let Some(search) = self.gateway.search_service() else {
            return SearchIndexStatusReply::Status {
                ready: false,
                detail: "search service is not configured".into(),
            };
        };
        if self.partitions.read().unwrap().get(&partition).is_none() {
            return SearchIndexStatusReply::Status {
                ready: false,
                detail: format!("partition {partition} is not hosted here"),
            };
        }
        for (name, generation, definition_hash) in generations {
            // Bring the projection up to date first: a learner that has just
            // finished Raft catch-up is usually one outbox tail behind.
            let _ = search
                .catch_up_generation(partition, name, *generation)
                .await;
            match search
                .generation_readiness(partition, name, *generation, *definition_hash)
                .await
            {
                Ok(()) => {}
                Err(error) => {
                    return SearchIndexStatusReply::Status {
                        ready: false,
                        detail: format!("{name}/{generation}: {error}"),
                    };
                }
            }
        }
        SearchIndexStatusReply::Status {
            ready: true,
            detail: String::new(),
        }
    }

    /// Return a group's placement only after a ReadIndex barrier. Move execution
    /// and learner admission both use this as a fencing read, so a follower's
    /// cached pre-abort plan must never be returned.
    async fn serve_placement_query(&self, req: Envelope) -> Envelope {
        let placement = match (
            codec::decode::<PlacementQueryBody>(&req.payload),
            self.meta(),
        ) {
            (Ok(body), Some(meta)) => match meta.read_placement(body.group).await {
                Ok(crate::meta::node::MetaRead::Value(placement)) => placement,
                Ok(crate::meta::node::MetaRead::NotLeader { .. }) | Err(_) => None,
            },
            _ => None,
        };
        self.reply(&req, codec::encode(&PlacementQueryReply { placement }))
    }

    async fn serve_recovery_fence(
        &self,
        req: Envelope,
        context: &crate::transport::RequestContext,
    ) -> Envelope {
        let reply = match (
            req.group_id,
            codec::decode::<RecoveryFenceRequest>(&req.payload),
        ) {
            (GroupId::Data(partition), Ok(body)) => {
                if !self.authenticated_as_node(context, body.requester) {
                    return self.reply(
                        &req,
                        codec::encode(&RecoveryFenceReply::Rejected(
                            "authenticated node does not match requester".into(),
                        )),
                    );
                }
                let node = self.partitions.read().unwrap().get(&partition).cloned();
                match node {
                    Some(node) => node.issue_recovery_fence(body.requester).await,
                    None => RecoveryFenceReply::Rejected(format!(
                        "partition {partition} is not hosted on this node"
                    )),
                }
            }
            _ => RecoveryFenceReply::Rejected("malformed recovery-fence request".into()),
        };
        self.reply(&req, codec::encode(&reply))
    }

    /// Return an authoritative directory only from the current meta leader
    /// after ReadIndex. Followers include their local view only as a leader-id
    /// resolution hint; callers must never merge that hint into authority state.
    async fn serve_directory_query(&self, req: Envelope) -> Envelope {
        let reply = match self.meta() {
            Some(meta) if meta.current_leader() != Some(meta.node_id()) => {
                DirectoryQueryReply::NotLeader {
                    leader: meta.current_leader(),
                    directory_hint: meta.local_directory().unwrap_or_default(),
                }
            }
            Some(meta) => match meta.read_directory().await {
                Ok(crate::meta::node::MetaRead::Value(directory)) => {
                    DirectoryQueryReply::Value(directory)
                }
                Ok(crate::meta::node::MetaRead::NotLeader { leader }) => {
                    DirectoryQueryReply::NotLeader {
                        leader,
                        directory_hint: meta.local_directory().unwrap_or_default(),
                    }
                }
                Err(_) => DirectoryQueryReply::Unavailable,
            },
            None => DirectoryQueryReply::Unavailable,
        };
        self.reply(&req, codec::encode(&reply))
    }

    async fn serve_maintenance_query(&self, req: Envelope) -> Envelope {
        let reply = match self.meta() {
            Some(meta) if meta.current_leader() != Some(meta.node_id()) => {
                MaintenanceQueryReply::NotLeader
            }
            Some(meta) => match meta.read_maintenance().await {
                Ok(crate::meta::node::MetaRead::Value(record)) => {
                    MaintenanceQueryReply::Value(record)
                }
                Ok(crate::meta::node::MetaRead::NotLeader { .. }) => {
                    MaintenanceQueryReply::NotLeader
                }
                Err(_) => MaintenanceQueryReply::Unavailable,
            },
            None => MaintenanceQueryReply::Unavailable,
        };
        self.reply(&req, codec::encode(&reply))
    }

    async fn serve_maintenance_ack(
        &self,
        req: Envelope,
        context: &crate::transport::RequestContext,
    ) -> Envelope {
        let Ok(body) = codec::decode::<MaintenanceAckBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&SubmitReply::Error("malformed".into())));
        };
        if !self.authenticated_as_node(context, body.node_id) {
            return self.reply(
                &req,
                codec::encode(&SubmitReply::Error(
                    "authenticated node does not match maintenance participant".into(),
                )),
            );
        }
        self.submit(
            &req,
            MetaCommand::AcknowledgeMaintenance {
                operation_id: body.operation_id,
                generation: body.generation,
                node_id: body.node_id,
                incarnation: body.incarnation,
            },
        )
        .await
    }

    async fn serve_maintenance_recovery_ack(
        &self,
        req: Envelope,
        context: &crate::transport::RequestContext,
    ) -> Envelope {
        let Ok(body) = codec::decode::<MaintenanceRecoveryAckBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&SubmitReply::Error("malformed".into())));
        };
        if !self.authenticated_as_node(context, body.node_id) {
            return self.reply(
                &req,
                codec::encode(&SubmitReply::Error(
                    "authenticated node does not match maintenance participant".into(),
                )),
            );
        }
        self.submit(
            &req,
            MetaCommand::AcknowledgeMaintenanceRecovery {
                operation_id: body.operation_id,
                generation: body.generation,
                node_id: body.node_id,
                incarnation: body.incarnation,
                process_incarnation: body.process_incarnation,
                topology_digest: body.topology_digest,
            },
        )
        .await
    }

    /// Record heartbeat liveness evidence. The reply is a bare ack — the emitter
    /// only needs to know the frame was delivered. A stale/replayed sequence is
    /// dropped by the tracker; the meta leader's detector loop turns accepted
    /// evidence into `SetNodeState` transitions.
    fn serve_heartbeat(
        &self,
        req: Envelope,
        context: &crate::transport::RequestContext,
    ) -> Envelope {
        // Meta hosts retain this evidence for leader changes and maintenance
        // timeout suspension. Recording it on non-meta nodes would
        // accumulate state from unauthenticated frames that nothing consumes.
        if self.meta().is_none() {
            return self.reply(&req, Vec::new());
        }
        if let Ok(body) = codec::decode::<HeartbeatBody>(&req.payload) {
            if !self.authenticated_as_node(context, body.node_id) {
                return self.reply(&req, Vec::new());
            }
            self.heartbeats.lock().unwrap().observe(
                body.node_id,
                body.incarnation,
                body.process_incarnation,
                body.seq,
                now_ms(),
            );
        }
        self.reply(&req, Vec::new())
    }

    /// Confirm bootstrap progress. A query addressed to the meta group confirms
    /// that a descriptor placement has committed via a linearizable meta read;
    /// a query addressed to a data group confirms that its local Raft runtime
    /// has completed first initialization.
    async fn serve_bootstrap_status(&self, req: Envelope) -> Envelope {
        let Ok(body) = codec::decode::<BootstrapStatusBody>(&req.payload) else {
            return self.reply(&req, codec::encode(&BootstrapStatusReply { ready: false }));
        };
        // The envelope identifies which runtime answers. Bootstrap placement
        // checks are addressed to `Meta` but carry the *data* group in the
        // body; the genesis fence addresses the data runtime itself.
        let ready = match req.group_id {
            GroupId::Meta => match self.meta() {
                Some(meta) => match meta.read_placement(body.group).await {
                    Ok(crate::meta::node::MetaRead::Value(Some(placement))) => {
                        crate::types::voter_set(placement.voters)
                            == crate::types::voter_set(body.voters)
                            && placement.r#move.is_none()
                    }
                    Ok(crate::meta::node::MetaRead::Value(None))
                    | Ok(crate::meta::node::MetaRead::NotLeader { .. })
                    | Err(_) => false,
                },
                None => false,
            },
            GroupId::Data(partition) => {
                if body.group != GroupId::Data(partition) {
                    return self.reply(&req, codec::encode(&BootstrapStatusReply { ready: false }));
                }
                let node = self.partitions.read().unwrap().get(&partition).cloned();
                match node {
                    Some(node) => node.is_initialized().await.unwrap_or(false),
                    None => false,
                }
            }
        };
        self.reply(&req, codec::encode(&BootstrapStatusReply { ready }))
    }
}

impl Server for RootDispatch {
    async fn serve(&self, request: Envelope) -> Envelope {
        self.serve_with_context(request, crate::transport::RequestContext::default())
            .await
    }

    async fn serve_with_context(
        &self,
        request: Envelope,
        context: crate::transport::RequestContext,
    ) -> Envelope {
        // The gateway applies this check for client traffic. Peer-control
        // frames bypass the gateway, so enforce the same cluster boundary here
        // before a foreign Raft or operator request reaches a local group.
        if request.cluster_id != self.cluster_id || !self.identity_gate.is_open() {
            return self.raft_unavailable(&request);
        }
        if request.msg_type.is_peer_control() {
            if let Some(policy) = &self.trust_policy {
                let required = match request.msg_type {
                    MsgType::JoinRequest
                    | MsgType::LeaveRequest
                    | MsgType::AbortPlanRequest
                    | MsgType::SearchIndexAdmin => Permission::Operator,
                    MsgType::MaintenanceStatus => Permission::Operator,
                    _ => Permission::PeerRpc,
                };
                let authorized = context
                    .authenticated_user_id
                    .as_deref()
                    .and_then(|user_id| policy.authorize(user_id, required))
                    .is_some();
                if !authorized {
                    crate::perf::record_authorization_denial();
                    return if matches!(
                        request.msg_type,
                        MsgType::JoinRequest
                            | MsgType::LeaveRequest
                            | MsgType::AbortPlanRequest
                            | MsgType::SearchIndexAdmin
                            | MsgType::MaintenanceStatus
                    ) {
                        self.reply(
                            &request,
                            codec::encode(&SubmitReply::Error("authorization denied".into())),
                        )
                    } else {
                        self.raft_unavailable(&request)
                    };
                }
            }
            let operation = request.msg_type;
            let principal = context.authenticated_user_id.clone();
            let reply = self.serve_control(request, context).await;
            if matches!(
                operation,
                MsgType::JoinRequest
                    | MsgType::LeaveRequest
                    | MsgType::AbortPlanRequest
                    | MsgType::SearchIndexAdmin
            ) {
                tracing::info!(
                    principal = principal.as_deref().unwrap_or("unknown"),
                    operation = ?operation,
                    result = audit_result(&reply),
                    "authenticated admin request completed"
                );
            }
            reply
        } else {
            self.gateway.serve_with_context(request, context).await
        }
    }
}

fn audit_result(reply: &Envelope) -> &'static str {
    match codec::decode::<SubmitReply>(&reply.payload) {
        Ok(SubmitReply::Outcome(crate::meta::state_machine::MetaApplyResult::Applied)) => "applied",
        Ok(SubmitReply::Outcome(crate::meta::state_machine::MetaApplyResult::NoOp)) => "no_op",
        Ok(SubmitReply::Outcome(crate::meta::state_machine::MetaApplyResult::Rejected(_))) => {
            "rejected"
        }
        Ok(SubmitReply::JoinPreflightReady) => "preflight_ready",
        Ok(SubmitReply::NotLeader { .. }) => "redirected",
        Ok(SubmitReply::Error(_)) => "error",
        Err(_) => "unavailable",
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
        fn routing(
            &self,
            _local_only: bool,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<RoutingInfo>> + Send + '_>>
        {
            Box::pin(async {
                Some(RoutingInfo {
                    cluster_id: 7,
                    p: 1,
                    hash_spec: HashSpec::CANONICAL,
                    directory: Vec::new(),
                    placements: Vec::new(),
                })
            })
        }
    }

    #[test]
    fn admin_audit_result_uses_only_coarse_outcomes() {
        let reply = Envelope::new(
            7,
            MsgType::JoinRequest,
            GroupId::Meta,
            1,
            codec::encode(&SubmitReply::Error("sensitive request detail".into())),
        );
        assert_eq!(audit_result(&reply), "error");
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
            gateway.clone(),
            Arc::new(RwLock::new(None)),
            partitions.clone(),
            Arc::new(Mutex::new(HeartbeatTracker::new())),
            None,
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
                policy_generation: 0,
                curve_public_key: [0; 32],
                preflight: false,
            }),
        );

        let reply = dispatch.serve(request).await;
        assert_eq!(reply.cluster_id, 7);
        assert_eq!(reply.msg_type, MsgType::JoinRequest);
        assert_eq!(reply.request_id, 42);
        assert!(reply.payload.is_empty());
    }

    #[tokio::test]
    async fn peer_identity_without_operator_permission_cannot_register_nodes() {
        use crate::transport::security::{Permission, Principal, TrustPolicy};
        let partitions = Arc::new(RwLock::new(HashMap::new()));
        let gateway = Arc::new(ClientGateway::new(
            7,
            1,
            HashSpec::CANONICAL,
            partitions.clone(),
            Arc::new(EmptyRouting),
        ));
        let policy = Arc::new(
            TrustPolicy::new(
                7,
                1,
                vec![Principal {
                    user_id: "node:1".into(),
                    curve_public_key: "A".repeat(40),
                    permissions: vec![Permission::PeerRpc],
                    client_ids: Vec::new(),
                }],
            )
            .unwrap(),
        );
        let dispatch = RootDispatch::new(
            7,
            gateway.clone(),
            Arc::new(RwLock::new(None)),
            partitions.clone(),
            Arc::new(Mutex::new(HeartbeatTracker::new())),
            None,
            None,
        )
        .with_trust_policy(policy);
        let request = Envelope::new(
            7,
            MsgType::JoinRequest,
            GroupId::Meta,
            1,
            codec::encode(&JoinBody {
                node_id: 9,
                control_addr: "tcp://new-control".into(),
                bulk_addr: "tcp://new-bulk".into(),
                policy_generation: 0,
                curve_public_key: [0; 32],
                preflight: false,
            }),
        );
        let reply = dispatch
            .serve_with_context(
                request,
                crate::transport::RequestContext {
                    authenticated_user_id: Some("node:1".into()),
                },
            )
            .await;
        assert!(matches!(codec::decode::<SubmitReply>(&reply.payload),
            Ok(SubmitReply::Error(error)) if error == "authorization denied"));
    }

    #[tokio::test]
    async fn join_preflight_requires_current_policy_and_local_candidate_pins() {
        use crate::transport::security::{Permission, Principal, TrustPolicy};
        let partitions = Arc::new(RwLock::new(HashMap::new()));
        let gateway = Arc::new(ClientGateway::new(
            7,
            1,
            HashSpec::CANONICAL,
            partitions.clone(),
            Arc::new(EmptyRouting),
        ));
        let candidate_public_key = [17; 32];
        let candidate_key_z85 = zmq::z85_encode(&candidate_public_key).unwrap();
        let policy = Arc::new(
            TrustPolicy::new(
                7,
                4,
                vec![
                    Principal {
                        user_id: "ops:join".into(),
                        curve_public_key: "C".repeat(40),
                        permissions: vec![Permission::Operator],
                        client_ids: Vec::new(),
                    },
                    Principal {
                        user_id: "node:9".into(),
                        curve_public_key: candidate_key_z85,
                        permissions: vec![Permission::PeerRpc],
                        client_ids: Vec::new(),
                    },
                ],
            )
            .unwrap(),
        );
        let pins = std::collections::BTreeMap::from([
            ("tcp://new-control".into(), "node:9".into()),
            ("tcp://new-bulk".into(), "node:9".into()),
        ]);
        let dispatch = RootDispatch::new(
            7,
            gateway.clone(),
            Arc::new(RwLock::new(None)),
            partitions.clone(),
            Arc::new(Mutex::new(HeartbeatTracker::new())),
            None,
            None,
        )
        .with_trust_policy(policy.clone())
        .with_server_key_principals(pins);

        let preflight = |policy_generation| {
            Envelope::new(
                7,
                MsgType::JoinRequest,
                GroupId::Meta,
                12,
                codec::encode(&JoinBody {
                    node_id: 9,
                    control_addr: "tcp://new-control".into(),
                    bulk_addr: "tcp://new-bulk".into(),
                    policy_generation,
                    curve_public_key: candidate_public_key,
                    preflight: true,
                }),
            )
        };
        let context = crate::transport::RequestContext {
            authenticated_user_id: Some("ops:join".into()),
        };
        let ready = dispatch
            .serve_with_context(preflight(4), context.clone())
            .await;
        assert!(matches!(
            codec::decode::<SubmitReply>(&ready.payload),
            Ok(SubmitReply::JoinPreflightReady)
        ));
        let stale = dispatch.serve_with_context(preflight(3), context).await;
        assert!(matches!(
            codec::decode::<SubmitReply>(&stale.payload),
            Ok(SubmitReply::Error(error)) if error.contains("generation")
        ));
        let missing_pins = RootDispatch::new(
            7,
            gateway,
            Arc::new(RwLock::new(None)),
            partitions,
            Arc::new(Mutex::new(HeartbeatTracker::new())),
            None,
            None,
        )
        .with_trust_policy(policy)
        .with_server_key_principals(std::collections::BTreeMap::new());
        let rejected = missing_pins
            .serve_with_context(
                preflight(4),
                crate::transport::RequestContext {
                    authenticated_user_id: Some("ops:join".into()),
                },
            )
            .await;
        assert!(matches!(
            codec::decode::<SubmitReply>(&rejected.payload),
            Ok(SubmitReply::Error(error)) if error.contains("pins")
        ));
    }

    #[tokio::test]
    async fn peer_cannot_spoof_the_sender_of_a_config_observation() {
        use crate::transport::security::{Permission, Principal, TrustPolicy};
        let partitions = Arc::new(RwLock::new(HashMap::new()));
        let gateway = Arc::new(ClientGateway::new(
            7,
            1,
            HashSpec::CANONICAL,
            partitions.clone(),
            Arc::new(EmptyRouting),
        ));
        let policy = Arc::new(
            TrustPolicy::new(
                7,
                1,
                vec![Principal {
                    user_id: "node:1".into(),
                    curve_public_key: "A".repeat(40),
                    permissions: vec![Permission::PeerRpc],
                    client_ids: Vec::new(),
                }],
            )
            .unwrap(),
        );
        let dispatch = RootDispatch::new(
            7,
            gateway,
            Arc::new(RwLock::new(None)),
            partitions,
            Arc::new(Mutex::new(HeartbeatTracker::new())),
            None,
            None,
        )
        .with_trust_policy(policy);
        let body = ObservationBody::Finalize {
            sender_node_id: 2,
            group: GroupId::Data(0),
            plan_id: 1,
            observation: crate::types::DataConfigObservation {
                group: GroupId::Data(0),
                plan_id: 1,
                voter_set: vec![1, 2, 3],
                config_log_id: crate::types::LogId::new(1, 4),
            },
        };
        let request = Envelope::new(
            7,
            MsgType::DataConfigObservation,
            GroupId::Meta,
            5,
            codec::encode(&body),
        );
        let reply = dispatch
            .serve_with_context(
                request,
                crate::transport::RequestContext {
                    authenticated_user_id: Some("node:1".into()),
                },
            )
            .await;
        assert!(matches!(codec::decode::<SubmitReply>(&reply.payload),
            Ok(SubmitReply::Error(error)) if error.contains("does not match observation sender")));
    }

    #[test]
    fn peer_sender_identity_must_match_raft_node_id() {
        use crate::transport::security::{Permission, Principal, TrustPolicy};
        let partitions = Arc::new(RwLock::new(HashMap::new()));
        let gateway = Arc::new(ClientGateway::new(
            7,
            1,
            HashSpec::CANONICAL,
            partitions.clone(),
            Arc::new(EmptyRouting),
        ));
        let policy = Arc::new(
            TrustPolicy::new(
                7,
                1,
                vec![Principal {
                    user_id: "node:1".into(),
                    curve_public_key: "A".repeat(40),
                    permissions: vec![Permission::PeerRpc],
                    client_ids: Vec::new(),
                }],
            )
            .unwrap(),
        );
        let dispatch = RootDispatch::new(
            7,
            gateway,
            Arc::new(RwLock::new(None)),
            partitions,
            Arc::new(Mutex::new(HeartbeatTracker::new())),
            None,
            None,
        )
        .with_trust_policy(policy);
        let context = crate::transport::RequestContext {
            authenticated_user_id: Some("node:1".into()),
        };
        assert!(dispatch.authenticated_as_node(&context, 1));
        assert!(!dispatch.authenticated_as_node(&context, 2));
        assert!(!dispatch.authenticated_as_node(&crate::transport::RequestContext::default(), 1));
    }
}
