//! Wire envelope framing (DESIGN §10.2).
//!
//! Frame layout, fixed 36-byte header then payload, all little-endian:
//!
//! ```text
//!   0  protocol_version : u32
//!   4  cluster_id       : u128
//!  20  msg_type         : u8
//!  21  group_tag        : u8   (0 = Meta, 1 = Data)
//!  22  group_partition  : u16  (meaningful only when tag == 1)
//!  24  request_id       : u64
//!  32  payload_len      : u32
//!  36  payload          : payload_len bytes
//! ```
//!
//! The header is validated — version, msg type, group structure, and the
//! per-`msg_type` size limit — *before* the payload is decoded, so an oversized
//! or malformed frame is rejected without allocating or deserializing it
//! (DESIGN §10.2). Cluster-id and partition-of-key checks are policy applied a
//! layer up in [`crate::api`], where `P` and the decoded key are available.

use crate::types::{ClusterId, GroupId, PROTOCOL_VERSION};

const HEADER_LEN: usize = 36;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// The kinds of message the transport carries (DESIGN §10.2). The `u8` tag is
/// stable on the wire; never renumber an existing variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MsgType {
    ClientOp = 0,
    RaftAppend = 1,
    RaftVote = 2,
    RaftSnapshot = 3,
    MigrationChunk = 4,
    MetaQuery = 5,
    Redirect = 6,
    Heartbeat = 7,
    /// Peer-control: admit a node as a learner (§7.2). Dispatched only from a
    /// configured node endpoint, never from client traffic.
    BecomeLearner = 8,
    /// Peer-control: a leader's finalize/abort configuration observation
    /// (§7.5). Also peer-only.
    DataConfigObservation = 9,
    /// Operator-control: register a node in the directory (`dal join`). Narrow
    /// and typed — never a generic meta-command carrier (§10.2).
    JoinRequest = 10,
    /// Operator-control: mark a node draining (`dal leave`).
    LeaveRequest = 11,
    /// Operator-control: mark a stuck move plan aborting (`dal abort-plan`).
    AbortPlanRequest = 12,
    /// Peer-control: ask a meta voter whether a descriptor's data placement is
    /// committed. Used only by startup bootstrap reconciliation.
    BootstrapStatus = 13,
    /// Peer-control: read a group's committed meta placement. Lets a data leader
    /// that is not a meta voter drive its own move (§7).
    PlacementQuery = 14,
}

impl MsgType {
    fn from_u8(v: u8) -> Option<MsgType> {
        Some(match v {
            0 => MsgType::ClientOp,
            1 => MsgType::RaftAppend,
            2 => MsgType::RaftVote,
            3 => MsgType::RaftSnapshot,
            4 => MsgType::MigrationChunk,
            5 => MsgType::MetaQuery,
            6 => MsgType::Redirect,
            7 => MsgType::Heartbeat,
            8 => MsgType::BecomeLearner,
            9 => MsgType::DataConfigObservation,
            10 => MsgType::JoinRequest,
            11 => MsgType::LeaveRequest,
            12 => MsgType::AbortPlanRequest,
            13 => MsgType::BootstrapStatus,
            14 => MsgType::PlacementQuery,
            _ => return None,
        })
    }

    /// The largest payload this message type may carry. Values reach 16 MiB
    /// (§4.2); bulk-lane snapshot/migration frames are larger; control frames
    /// are tiny. The limit is enforced before decode.
    pub fn max_payload(self) -> usize {
        match self {
            // A client value can be 16 MiB; leave headroom for op framing.
            MsgType::ClientOp => 16 * MIB + 64 * KIB,
            // A batch of client entries plus Raft framing.
            MsgType::RaftAppend => 64 * MIB,
            MsgType::RaftVote => 4 * KIB,
            // Bulk lane.
            MsgType::RaftSnapshot | MsgType::MigrationChunk => 256 * MIB,
            MsgType::MetaQuery => 4 * KIB,
            MsgType::Redirect => 64 * KIB,
            MsgType::Heartbeat => 4 * KIB,
            MsgType::BecomeLearner => 4 * KIB,
            MsgType::DataConfigObservation => 4 * KIB,
            MsgType::JoinRequest
            | MsgType::LeaveRequest
            | MsgType::AbortPlanRequest
            | MsgType::BootstrapStatus
            | MsgType::PlacementQuery => 4 * KIB,
        }
    }

    /// Whether this type is peer/operator-control (dispatched on the node
    /// control path, never through the client gateway — DESIGN §10.2, ground
    /// rule 9).
    pub fn is_peer_control(self) -> bool {
        matches!(
            self,
            MsgType::RaftAppend
                | MsgType::RaftVote
                | MsgType::RaftSnapshot
                | MsgType::MigrationChunk
                | MsgType::Heartbeat
                | MsgType::BecomeLearner
                | MsgType::DataConfigObservation
                | MsgType::JoinRequest
                | MsgType::LeaveRequest
                | MsgType::AbortPlanRequest
                | MsgType::BootstrapStatus
                | MsgType::PlacementQuery
        )
    }
}

/// Why a frame was rejected at the transport boundary, before any payload
/// decode (DESIGN §10.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Fewer bytes than the fixed header, or a payload shorter than its length
    /// prefix promises.
    Truncated { need: usize, got: usize },
    /// A protocol version this build does not speak.
    UnsupportedVersion(u32),
    /// The `msg_type` byte is not a known type.
    BadMsgType(u8),
    /// The group tag byte is neither `Meta` (0) nor `Data` (1).
    BadGroupId(u8),
    /// The payload exceeds the per-type limit.
    Oversized {
        msg_type: MsgType,
        limit: usize,
        got: usize,
    },
    /// Bytes past the declared payload end.
    TrailingBytes { extra: usize },
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Truncated { need, got } => {
                write!(f, "truncated frame: need {need} bytes, got {got}")
            }
            FrameError::UnsupportedVersion(v) => write!(f, "unsupported protocol version {v}"),
            FrameError::BadMsgType(t) => write!(f, "unknown msg_type {t}"),
            FrameError::BadGroupId(t) => write!(f, "malformed group id tag {t}"),
            FrameError::Oversized {
                msg_type,
                limit,
                got,
            } => write!(f, "{msg_type:?} payload {got} exceeds limit {limit}"),
            FrameError::TrailingBytes { extra } => write!(f, "{extra} trailing bytes after frame"),
        }
    }
}

impl std::error::Error for FrameError {}

/// A parsed wire envelope. The `payload` is still opaque bytes here; the
/// dispatcher decodes it according to `msg_type`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub cluster_id: ClusterId,
    pub msg_type: MsgType,
    pub group_id: GroupId,
    pub request_id: u64,
    pub payload: Vec<u8>,
}

impl Envelope {
    pub fn new(
        cluster_id: ClusterId,
        msg_type: MsgType,
        group_id: GroupId,
        request_id: u64,
        payload: Vec<u8>,
    ) -> Envelope {
        Envelope {
            cluster_id,
            msg_type,
            group_id,
            request_id,
            payload,
        }
    }

    /// Serialize to a single frame. The current [`PROTOCOL_VERSION`] is stamped
    /// in. Payloads are trusted to be within limits here; the receiver enforces
    /// them on decode.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_LEN + self.payload.len());
        buf.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
        buf.extend_from_slice(&self.cluster_id.to_le_bytes());
        buf.push(self.msg_type as u8);
        let (tag, partition) = match self.group_id {
            GroupId::Meta => (0u8, 0u16),
            GroupId::Data(p) => (1u8, p),
        };
        buf.push(tag);
        buf.extend_from_slice(&partition.to_le_bytes());
        buf.extend_from_slice(&self.request_id.to_le_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Parse and validate a frame. Validation order matches DESIGN §10.2:
    /// structural length, version, msg type, group tag, then the size limit —
    /// all before the payload is read out.
    pub fn decode(bytes: &[u8]) -> Result<Envelope, FrameError> {
        if bytes.len() < HEADER_LEN {
            return Err(FrameError::Truncated {
                need: HEADER_LEN,
                got: bytes.len(),
            });
        }

        let version = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if version != PROTOCOL_VERSION {
            return Err(FrameError::UnsupportedVersion(version));
        }

        let cluster_id = u128::from_le_bytes(bytes[4..20].try_into().unwrap());

        let msg_type = MsgType::from_u8(bytes[20]).ok_or(FrameError::BadMsgType(bytes[20]))?;

        let group_tag = bytes[21];
        let partition = u16::from_le_bytes(bytes[22..24].try_into().unwrap());
        let group_id = match group_tag {
            0 => GroupId::Meta,
            1 => GroupId::Data(partition),
            other => return Err(FrameError::BadGroupId(other)),
        };

        let request_id = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let payload_len = u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as usize;

        let limit = msg_type.max_payload();
        if payload_len > limit {
            return Err(FrameError::Oversized {
                msg_type,
                limit,
                got: payload_len,
            });
        }

        let end = HEADER_LEN + payload_len;
        if bytes.len() < end {
            return Err(FrameError::Truncated {
                need: end,
                got: bytes.len(),
            });
        }
        if bytes.len() > end {
            return Err(FrameError::TrailingBytes {
                extra: bytes.len() - end,
            });
        }

        Ok(Envelope {
            cluster_id,
            msg_type,
            group_id,
            request_id,
            payload: bytes[HEADER_LEN..end].to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(msg_type: MsgType, group: GroupId, payload: Vec<u8>) -> Envelope {
        Envelope::new(
            0xABCD_1234_5678_9ABC_DEF0_1122_3344_5566,
            msg_type,
            group,
            42,
            payload,
        )
    }

    #[test]
    fn round_trip_meta_and_data() {
        for group in [GroupId::Meta, GroupId::Data(0), GroupId::Data(65535)] {
            let e = env(MsgType::ClientOp, group, b"hello".to_vec());
            let decoded = Envelope::decode(&e.encode()).unwrap();
            assert_eq!(e, decoded);
        }
    }

    #[test]
    fn empty_payload_round_trips() {
        let e = env(MsgType::MetaQuery, GroupId::Meta, Vec::new());
        assert_eq!(e, Envelope::decode(&e.encode()).unwrap());
    }

    #[test]
    fn truncated_header_rejected() {
        assert!(matches!(
            Envelope::decode(&[0u8; 10]),
            Err(FrameError::Truncated { .. })
        ));
    }

    #[test]
    fn unsupported_version_rejected() {
        let mut bytes = env(MsgType::MetaQuery, GroupId::Meta, Vec::new()).encode();
        bytes[0] = 0xFF;
        assert!(matches!(
            Envelope::decode(&bytes),
            Err(FrameError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn bad_msg_type_rejected() {
        let mut bytes = env(MsgType::MetaQuery, GroupId::Meta, Vec::new()).encode();
        bytes[20] = 200;
        assert!(matches!(
            Envelope::decode(&bytes),
            Err(FrameError::BadMsgType(200))
        ));
    }

    #[test]
    fn bad_group_tag_rejected() {
        let mut bytes = env(MsgType::MetaQuery, GroupId::Meta, Vec::new()).encode();
        bytes[21] = 9;
        assert!(matches!(
            Envelope::decode(&bytes),
            Err(FrameError::BadGroupId(9))
        ));
    }

    #[test]
    fn oversized_payload_rejected_before_read() {
        // Claim a payload larger than the Heartbeat limit without actually
        // providing the bytes: rejection must be on the length prefix alone.
        let mut bytes = env(MsgType::Heartbeat, GroupId::Meta, Vec::new()).encode();
        let huge = (MsgType::Heartbeat.max_payload() as u32) + 1;
        bytes[32..36].copy_from_slice(&huge.to_le_bytes());
        assert!(matches!(
            Envelope::decode(&bytes),
            Err(FrameError::Oversized { .. })
        ));
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut bytes = env(MsgType::MetaQuery, GroupId::Meta, Vec::new()).encode();
        bytes.push(0);
        assert!(matches!(
            Envelope::decode(&bytes),
            Err(FrameError::TrailingBytes { .. })
        ));
    }

    #[test]
    fn peer_control_classification() {
        assert!(MsgType::RaftAppend.is_peer_control());
        assert!(MsgType::BecomeLearner.is_peer_control());
        assert!(MsgType::JoinRequest.is_peer_control());
        assert!(MsgType::LeaveRequest.is_peer_control());
        assert!(MsgType::AbortPlanRequest.is_peer_control());
        assert!(!MsgType::ClientOp.is_peer_control());
        assert!(!MsgType::MetaQuery.is_peer_control());
    }

    #[test]
    fn operator_frames_round_trip() {
        for t in [
            MsgType::JoinRequest,
            MsgType::LeaveRequest,
            MsgType::AbortPlanRequest,
        ] {
            let e = env(t, GroupId::Meta, b"body".to_vec());
            assert_eq!(e, Envelope::decode(&e.encode()).unwrap());
        }
    }
}
