//! Node-side client gateway (DESIGN §8.2, §10.2).
//!
//! Answers `ClientOp` and `MetaQuery` frames: validates cluster id and
//! partition-of-key, routes reads/mutations to the local partition's serving
//! gate, and replies with a value, a mutation result, or an advisory redirect.
//! Peer-control frames are refused here — they reach the cluster only through a
//! separate dispatcher (ground rule 9), so no client code path can submit a
//! plan or membership change.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::api::ops::{
    ClientReply, ClientRequest, Redirect, RejectFrame, RoutingInfo, WriteReply, check_partition,
};
use crate::codec;
use crate::partition::node::{PartitionNode, ReadOutcome, WriteOutcome};
use crate::transport::Server;

/// The set of data partitions a node currently hosts, shared between the gateway
/// and the control dispatcher and mutated as rebalancing adds/removes partitions.
pub type PartitionMap = Arc<RwLock<HashMap<u16, Arc<PartitionNode>>>>;
use crate::transport::codec::{Envelope, MsgType};
use crate::types::{ClusterId, DataOp, GroupId, HashSpec, IfVersion};

/// The source of a client's routing snapshot (DESIGN §8.1). In M4 a fixed
/// implementation answers this; M5 backs it with the meta group's placement map.
pub trait RoutingSource: Send + Sync {
    fn routing(&self) -> RoutingInfo;
}

/// A gateway fronting the partitions this node hosts.
pub struct ClientGateway {
    cluster_id: ClusterId,
    p: u16,
    hash_spec: HashSpec,
    partitions: PartitionMap,
    routing: Arc<dyn RoutingSource>,
}

impl ClientGateway {
    pub fn new(
        cluster_id: ClusterId,
        p: u16,
        hash_spec: HashSpec,
        partitions: PartitionMap,
        routing: Arc<dyn RoutingSource>,
    ) -> ClientGateway {
        ClientGateway {
            cluster_id,
            p,
            hash_spec,
            partitions,
            routing,
        }
    }

    /// Wrap a reply body in an envelope stamped with *this* node's cluster id,
    /// so a client that reached the wrong cluster detects it on the reply
    /// (DESIGN §8.2).
    fn reply(&self, msg_type: MsgType, group: GroupId, request_id: u64, body: Vec<u8>) -> Envelope {
        Envelope::new(self.cluster_id, msg_type, group, request_id, body)
    }

    fn client_reply(&self, group: GroupId, request_id: u64, reply: ClientReply) -> Envelope {
        self.reply(MsgType::ClientOp, group, request_id, codec::encode(&reply))
    }

    /// The candidate voter set for a partition, from the routing snapshot.
    fn candidates(&self, partition: u16) -> Vec<u64> {
        self.routing.routing().candidates(partition)
    }

    async fn handle_client_op(&self, env: &Envelope) -> ClientReply {
        let req: ClientRequest = match codec::decode(&env.payload) {
            Ok(r) => r,
            Err(e) => return ClientReply::Refused(format!("malformed ClientOp: {e}")),
        };

        let partition = match check_partition(&req, env.group_id, self.p, &self.hash_spec) {
            Ok(p) => p,
            Err(RejectFrame::MispartitionedKey { expected, got }) => {
                return ClientReply::Refused(
                    RejectFrame::MispartitionedKey { expected, got }.to_string(),
                );
            }
            Err(e) => return ClientReply::Refused(e.to_string()),
        };

        let node = self.partitions.read().unwrap().get(&partition).cloned();
        let Some(node) = node else {
            // We do not host this partition: redirect to its candidate voters.
            return ClientReply::Redirect(Redirect {
                cluster_id: self.cluster_id,
                leader: None,
                candidates: self.candidates(partition),
            });
        };

        match req {
            ClientRequest::Mutate(data) => {
                // `Absent` is meaningful only for create-only puts. Reject a
                // malformed delete before it reaches the replicated log, so it
                // cannot consume a Raft entry or masquerade as a decided
                // client operation.
                if matches!(
                    data.op,
                    DataOp::Delete {
                        if_version: Some(IfVersion::Absent),
                        ..
                    }
                ) {
                    return ClientReply::Refused(
                        "delete does not support if_version=Absent".into(),
                    );
                }
                match node.write(data).await {
                    Ok(WriteOutcome::Applied(result)) => {
                        ClientReply::Mutation(WriteReply::from_apply(result))
                    }
                    Ok(WriteOutcome::NotLeader { leader }) => ClientReply::Redirect(Redirect {
                        cluster_id: self.cluster_id,
                        leader,
                        candidates: self.candidates(partition),
                    }),
                    Err(e) => ClientReply::Error(format!("write failed: {e}")),
                }
            }
            ClientRequest::Read { key, consistency } => match node.read(&key, consistency).await {
                Ok(ReadOutcome::Value(v)) => ClientReply::Value(v),
                // A stale read refused for freshness reuses the redirect channel,
                // so the client just walks to a fresher candidate (DESIGN §8.3).
                Ok(ReadOutcome::NotLeader { leader }) | Ok(ReadOutcome::TooStale { leader }) => {
                    ClientReply::Redirect(Redirect {
                        cluster_id: self.cluster_id,
                        leader,
                        candidates: self.candidates(partition),
                    })
                }
                Err(e) => ClientReply::Error(format!("read failed: {e}")),
            },
        }
    }
}

impl Server for ClientGateway {
    async fn serve(&self, request: Envelope) -> Envelope {
        // Wrong cluster: reply stamped with our real cluster id so the client's
        // per-reply cluster check rejects it (DESIGN §8.2).
        if request.cluster_id != self.cluster_id {
            return self.client_reply(
                request.group_id,
                request.request_id,
                ClientReply::Error("wrong cluster".to_string()),
            );
        }

        // Client dispatch never accepts peer-control frames (ground rule 9).
        if request.msg_type.is_peer_control() {
            return self.client_reply(
                request.group_id,
                request.request_id,
                ClientReply::Error(RejectFrame::PeerControlOnClientPath.to_string()),
            );
        }

        match request.msg_type {
            MsgType::MetaQuery => {
                let info = self.routing.routing();
                self.reply(
                    MsgType::MetaQuery,
                    GroupId::Meta,
                    request.request_id,
                    codec::encode(&info),
                )
            }
            MsgType::ClientOp => {
                let reply = self.handle_client_op(&request).await;
                self.client_reply(request.group_id, request.request_id, reply)
            }
            other => self.client_reply(
                request.group_id,
                request.request_id,
                ClientReply::Error(format!("unsupported client msg_type {other:?}")),
            ),
        }
    }
}
