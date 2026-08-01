//! Client operation payloads, replies, and the routing snapshot (DESIGN §8).
//!
//! These are the serde bodies carried inside a [`crate::transport::codec::Envelope`].
//! The envelope frames and size-limits them; here we define what a `ClientOp`,
//! `MetaQuery` reply, and `Redirect` actually contain, plus the partition-of-key
//! check that rejects a misrouted client (DESIGN §10.2).

use serde::{Deserialize, Serialize};

use crate::partition::state_machine::{ApplyResult, RejectReason};
use crate::types::{
    ClusterId, Consistency, DataRequest, GroupId, HashSpec, KeyPresence, NodeDirectoryEntry,
    NodeId, Placement, Version,
};

/// A client-submitted operation (the `ClientOp` payload). A mutation carries its
/// full idempotency framing in [`DataRequest`]; a read carries only the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientRequest {
    Mutate(DataRequest),
    Read {
        key: Vec<u8>,
        consistency: Consistency,
    },
}

/// Controls whether a `MetaQuery` may proxy to another node. Public clients send
/// an empty payload (equivalent to `local_only = false`). Runtime-to-runtime
/// lookups set `local_only` so a non-meta node fails fast instead of recursively
/// proxying the same query through its peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingQuery {
    pub local_only: bool,
}

impl ClientRequest {
    /// The key this operation targets, used to check it hashes to the envelope's
    /// partition (DESIGN §10.2).
    pub fn key(&self) -> &[u8] {
        match self {
            ClientRequest::Mutate(req) => req.op.key(),
            ClientRequest::Read { key, .. } => key,
        }
    }
}

/// The outcome of a mutation, as seen by the client. Both a fresh decision and a
/// replayed idempotent result collapse to the same shape; a protocol-level
/// rejection (sequence gap, digest mismatch) is surfaced distinctly so the
/// client can tell "your CAS failed" from "your retry was malformed".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteReply {
    Applied {
        version: Version,
    },
    ConditionFailed {
        current: KeyPresence,
    },
    /// The state machine refused the entry without mutating (e.g. a reused
    /// sequence for a different command). Carries the reason for diagnostics.
    Rejected(RejectReason),
}

impl WriteReply {
    /// Collapse an [`ApplyResult`] into the client-visible reply. A `NoOp` can
    /// only arise from a non-client entry and never reaches this path; it is
    /// mapped to a malformed rejection defensively.
    pub fn from_apply(result: ApplyResult) -> WriteReply {
        use crate::types::MutationResult::*;
        match result {
            ApplyResult::Decided(Applied { version })
            | ApplyResult::Replayed(Applied { version }) => WriteReply::Applied { version },
            ApplyResult::Decided(ConditionFailed { current })
            | ApplyResult::Replayed(ConditionFailed { current }) => {
                WriteReply::ConditionFailed { current }
            }
            ApplyResult::Rejected(reason) => WriteReply::Rejected(reason),
            ApplyResult::NoOp => WriteReply::Rejected(RejectReason::Malformed),
        }
    }
}

/// An advisory redirect (DESIGN §8.2). Routing is advisory; the serving gate is
/// authority, so a redirect just updates the client's cache. Every redirect
/// carries the responder's cluster id so a client can reject a cross-cluster
/// reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redirect {
    pub cluster_id: ClusterId,
    pub leader: Option<NodeId>,
    /// Nodes the responder believes could serve this partition (`voters`, plus
    /// `target_voters` while a move is active).
    pub candidates: Vec<NodeId>,
}

/// A reply to a `ClientOp`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientReply {
    Mutation(WriteReply),
    Value(Option<(Version, Vec<u8>)>),
    Redirect(Redirect),
    /// The gateway refused the operation before proposing it to Raft, based
    /// only on the request bytes and cluster-wide constants (`P`, hash spec) —
    /// so every replica refuses it identically and it cannot have committed
    /// anywhere. A client may safely abandon the mutation's sequence.
    Refused(String),
    /// A terminal error with an *uncertain* outcome: the operation may have
    /// reached the replicated log (e.g. a Raft error after proposal). A client
    /// must not abandon an in-flight mutation sequence on this reply.
    Error(String),
}

impl ClientReply {
    /// The cluster id stamped on a redirect, if any — used by the client to
    /// reject a mismatched-cluster response (DESIGN §8.2).
    pub fn redirect_cluster(&self) -> Option<ClusterId> {
        match self {
            ClientReply::Redirect(r) => Some(r.cluster_id),
            _ => None,
        }
    }
}

/// A client's routing snapshot, learned from any meta replica (DESIGN §8.1).
/// This is a follower/cached read: advisory, since the serving gate is the sole
/// authority. In M4 a fixed source answers it; M5 wires it to the meta group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingInfo {
    pub cluster_id: ClusterId,
    pub p: u16,
    pub hash_spec: HashSpec,
    pub directory: Vec<NodeDirectoryEntry>,
    /// Placement per data partition. Absent partitions have no known placement
    /// yet and fall back to seed/redirect discovery.
    pub placements: Vec<(u16, Placement)>,
}

impl RoutingInfo {
    /// The control address for a node id, if the directory knows it.
    pub fn control_addr(&self, node: NodeId) -> Option<&str> {
        self.directory
            .iter()
            .find(|e| e.node_id == node)
            .map(|e| e.control_addr.as_str())
    }

    /// The candidate voter set for a partition: `voters`, plus `target_voters`
    /// while a move is active (DESIGN §8.1).
    pub fn candidates(&self, partition: u16) -> Vec<NodeId> {
        let Some((_, placement)) = self.placements.iter().find(|(p, _)| *p == partition) else {
            return Vec::new();
        };
        let mut out = placement.voters.clone();
        if let Some(m) = &placement.r#move {
            for v in &m.target_voters {
                if !out.contains(v) {
                    out.push(*v);
                }
            }
        }
        out
    }
}

/// Why an inbound frame was refused at the API boundary, distinct from a
/// transport [`FrameError`](crate::transport::codec::FrameError): here the frame
/// was structurally valid but violates cluster/partition/dispatch policy
/// (DESIGN §10.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectFrame {
    /// The envelope's cluster id is not ours.
    WrongCluster { expected: ClusterId, got: ClusterId },
    /// A `ClientOp` whose key does not hash to the envelope's `group_id`.
    MispartitionedKey { expected: GroupId, got: GroupId },
    /// The envelope's group is not a data partition where one is required.
    NotADataPartition(GroupId),
    /// A peer-control message arrived on the client dispatch path (ground rule
    /// 9): client traffic can never reach `FinalizePlan`/`AbortReport`/learner
    /// admission.
    PeerControlOnClientPath,
}

impl std::fmt::Display for RejectFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectFrame::WrongCluster { expected, got } => {
                write!(f, "wrong cluster id: expected {expected:#x}, got {got:#x}")
            }
            RejectFrame::MispartitionedKey { expected, got } => {
                write!(f, "key hashes to {expected:?} but was sent to {got:?}")
            }
            RejectFrame::NotADataPartition(g) => write!(f, "{g:?} is not a data partition"),
            RejectFrame::PeerControlOnClientPath => {
                write!(f, "peer-control message on client dispatch path")
            }
        }
    }
}

impl std::error::Error for RejectFrame {}

/// Check that a decoded client operation belongs on this envelope's partition
/// (DESIGN §10.2). A client configured with the wrong `P` hashes the key to a
/// different partition than the envelope claims, and must get an error rather
/// than a silent write into the wrong partition.
pub fn check_partition(
    req: &ClientRequest,
    envelope_group: GroupId,
    p: u16,
    hash_spec: &HashSpec,
) -> Result<u16, RejectFrame> {
    let GroupId::Data(claimed) = envelope_group else {
        return Err(RejectFrame::NotADataPartition(envelope_group));
    };
    let actual = hash_spec.partition_of(req.key(), p);
    if actual != claimed {
        return Err(RejectFrame::MispartitionedKey {
            expected: GroupId::Data(actual),
            got: envelope_group,
        });
    }
    Ok(claimed)
}
