//! Typed wire payload bodies for peer-control frames (DESIGN §10.2).
//!
//! Raft RPC payloads are openraft's own request/response types serialized with
//! [`crate::codec`] (bincode; the `serde` feature makes them serializable).
//! Everything else gets a small named body here. There is deliberately no
//! generic "propose a meta command" body: each frame maps to one narrow,
//! typed action, preserving ground rule 9.

use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse};
use serde::{Deserialize, Serialize};

use crate::meta::state_machine::MetaApplyResult;
use crate::types::{DataConfigObservation, GroupId, MetaCommand, NodeId};

/// Reply payload for `RaftAppend` frames. Responses are generic over the node
/// id only, so one alias serves both the meta and data type configs.
pub type AppendReply = Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>;

/// Reply payload for `RaftVote` frames.
pub type VoteReply = Result<VoteResponse<NodeId>, RaftError<NodeId>>;

/// Reply payload for `RaftSnapshot` frames (bulk lane).
pub type SnapshotReply =
    Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>;

/// `MsgType::Heartbeat` — liveness evidence sent by every node to the meta
/// voters (DESIGN §9.1). The incarnation lets the tracker drop evidence from a
/// stale incarnation of a rejoined node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatBody {
    pub node_id: NodeId,
    pub incarnation: u64,
    pub seq: u64,
}

/// `MsgType::BecomeLearner` — the group is addressed by the envelope header;
/// the body carries the plan under which admission is authorized (§7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BecomeLearnerBody {
    pub plan_id: u64,
}

/// `MsgType::DataConfigObservation` — a data leader's finalize/abort report
/// (§7.5). Restricted to exactly these two meta commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObservationBody {
    Finalize {
        group: GroupId,
        plan_id: u64,
        observation: DataConfigObservation,
    },
    Abort {
        group: GroupId,
        plan_id: u64,
        observation: DataConfigObservation,
    },
}

impl ObservationBody {
    pub fn into_meta_command(self) -> MetaCommand {
        match self {
            ObservationBody::Finalize {
                group,
                plan_id,
                observation,
            } => MetaCommand::FinalizePlan {
                group,
                plan_id,
                observation,
            },
            ObservationBody::Abort {
                group,
                plan_id,
                observation,
            } => MetaCommand::AbortReport {
                group,
                plan_id,
                observation,
            },
        }
    }
}

/// `MsgType::JoinRequest` — operator registration of a new node (§6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinBody {
    pub node_id: NodeId,
    pub control_addr: String,
    pub bulk_addr: String,
}

/// `MsgType::LeaveRequest` — operator drain of a node (§9.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaveBody {
    pub node_id: NodeId,
}

/// `MsgType::AbortPlanRequest` — operator abort of a stuck move plan (§7.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortPlanBody {
    pub group: GroupId,
    pub plan_id: u64,
}

/// Reply for frames that submit to the meta group (observation, join, leave,
/// abort-plan). `NotLeader` tells the sender which voter to retry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmitReply {
    Outcome(MetaApplyResult),
    NotLeader { leader: Option<NodeId> },
    Error(String),
}

/// Reply for `BecomeLearner`: whether the admission record is durably in place
/// and the group runtime is starting (or already running).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LearnerReply {
    Admitted,
    Error(String),
}

/// `MsgType::BootstrapStatus` — ask a meta voter whether a data placement from
/// the immutable bootstrap descriptor is committed. The expected voters are
/// included so a node never initializes a data group based on a placement from
/// a conflicting descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapStatusBody {
    pub group: GroupId,
    pub voters: Vec<NodeId>,
}

/// Reply to [`BootstrapStatusBody`]. `ready` is true only after a linearizable
/// meta read observes the exact expected placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapStatusReply {
    pub ready: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec;
    use crate::meta::raft_types::MetaTypeConfig;
    use crate::partition::raft_types::TypeConfig;
    use crate::types::{DataOp, DataRequest, LogId, MetaCommand};
    use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
    use openraft::{CommittedLeaderId, Entry, EntryPayload, Membership, SnapshotMeta, Vote};

    fn log_id(term: u64, index: u64) -> openraft::LogId<u64> {
        openraft::LogId::new(CommittedLeaderId::new(term, 1), index)
    }

    #[test]
    fn append_entries_round_trips_for_data_group() {
        let req = AppendEntriesRequest::<TypeConfig> {
            vote: Vote::new_committed(3, 1),
            prev_log_id: Some(log_id(2, 9)),
            entries: vec![
                Entry {
                    log_id: log_id(3, 10),
                    payload: EntryPayload::Blank,
                },
                Entry {
                    log_id: log_id(3, 11),
                    payload: EntryPayload::Normal(DataRequest {
                        client_id: 1,
                        sequence: 1,
                        op: DataOp::Put {
                            key: b"k".to_vec(),
                            value: b"v".to_vec(),
                            if_version: None,
                        },
                    }),
                },
                Entry {
                    log_id: log_id(3, 12),
                    payload: EntryPayload::Membership(Membership::new(
                        vec![[1u64, 2, 3].into_iter().collect()],
                        None,
                    )),
                },
            ],
            leader_commit: Some(log_id(3, 11)),
        };
        let bytes = codec::encode(&req);
        let back: AppendEntriesRequest<TypeConfig> = codec::decode(&bytes).unwrap();
        assert_eq!(format!("{req:?}"), format!("{back:?}"));
    }

    #[test]
    fn append_entries_round_trips_for_meta_group() {
        let req = AppendEntriesRequest::<MetaTypeConfig> {
            vote: Vote::new_committed(1, 2),
            prev_log_id: None,
            entries: vec![Entry {
                log_id: log_id(1, 1),
                payload: EntryPayload::Normal(MetaCommand::SetNodeState {
                    node_id: 4,
                    state: crate::types::NodeState::Suspect,
                    incarnation: 2,
                }),
            }],
            leader_commit: None,
        };
        let bytes = codec::encode(&req);
        let back: AppendEntriesRequest<MetaTypeConfig> = codec::decode(&bytes).unwrap();
        assert_eq!(format!("{req:?}"), format!("{back:?}"));
    }

    #[test]
    fn vote_round_trips() {
        let req = VoteRequest::<u64>::new(Vote::new(7, 3), Some(log_id(6, 42)));
        let bytes = codec::encode(&req);
        let back: VoteRequest<u64> = codec::decode(&bytes).unwrap();
        assert_eq!(req, back);

        let ok: VoteReply = Ok(VoteResponse {
            vote: Vote::new(7, 3),
            vote_granted: true,
            last_log_id: Some(log_id(6, 42)),
        });
        let back: VoteReply = codec::decode(&codec::encode(&ok)).unwrap();
        assert_eq!(format!("{ok:?}"), format!("{back:?}"));
    }

    #[test]
    fn install_snapshot_round_trips() {
        let req = InstallSnapshotRequest::<TypeConfig> {
            vote: Vote::new_committed(3, 1),
            meta: SnapshotMeta {
                last_log_id: Some(log_id(3, 100)),
                last_membership: openraft::StoredMembership::new(
                    Some(log_id(3, 12)),
                    Membership::new(vec![[1u64, 2, 3].into_iter().collect()], None),
                ),
                snapshot_id: "3-100".to_string(),
            },
            offset: 4096,
            data: vec![0xAB; 1024],
            done: false,
        };
        let bytes = codec::encode(&req);
        let back: InstallSnapshotRequest<TypeConfig> = codec::decode(&bytes).unwrap();
        assert_eq!(format!("{req:?}"), format!("{back:?}"));
    }

    #[test]
    fn append_reply_round_trips() {
        for reply in [
            Ok(AppendEntriesResponse::Success),
            Ok(AppendEntriesResponse::Conflict),
        ] {
            let reply: AppendReply = reply;
            let back: AppendReply = codec::decode(&codec::encode(&reply)).unwrap();
            assert_eq!(format!("{reply:?}"), format!("{back:?}"));
        }
    }

    #[test]
    fn control_bodies_round_trip() {
        let hb = HeartbeatBody {
            node_id: 3,
            incarnation: 2,
            seq: 99,
        };
        assert_eq!(
            hb,
            codec::decode::<HeartbeatBody>(&codec::encode(&hb)).unwrap()
        );

        let obs = ObservationBody::Finalize {
            group: GroupId::Data(7),
            plan_id: 11,
            observation: DataConfigObservation {
                group: GroupId::Data(7),
                plan_id: 11,
                voter_set: vec![1, 2, 4],
                config_log_id: LogId::new(3, 20),
            },
        };
        let back: ObservationBody = codec::decode(&codec::encode(&obs)).unwrap();
        assert_eq!(obs, back);
        assert!(matches!(
            back.into_meta_command(),
            MetaCommand::FinalizePlan { plan_id: 11, .. }
        ));

        let join = JoinBody {
            node_id: 9,
            control_addr: "tcp://h:1".into(),
            bulk_addr: "tcp://h:2".into(),
        };
        assert_eq!(
            join,
            codec::decode::<JoinBody>(&codec::encode(&join)).unwrap()
        );

        let reply = SubmitReply::NotLeader { leader: Some(2) };
        assert_eq!(
            reply,
            codec::decode::<SubmitReply>(&codec::encode(&reply)).unwrap()
        );
    }
}
