//! Core wire and state types shared across every module.
//!
//! These are deliberately dependency-light (serde only) so the storage and
//! state-machine layers can be built and tested before openraft or ZMQ enter
//! the picture. openraft's own `LogId`/`Membership` are mapped onto the local
//! [`LogId`]/voter-set representations at the M3 boundary.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

/// Wire/protocol version. Bumped only on an incompatible envelope or command
/// change; receivers reject anything they do not recognise (DESIGN §10.2).
pub const PROTOCOL_VERSION: u32 = 2;

/// Stable node identity, assigned at `join` and never reused.
pub type NodeId = u64;

/// Random 128-bit cluster identity minted once at `init` (DESIGN §3.1).
pub type ClusterId = u128;

/// Per-client identity for idempotent retries (DESIGN §8.4).
pub type ClientId = u128;

/// Strictly increasing, per-client/partition mutation sequence.
pub type Sequence = u64;

/// Shared one-way fence for a running process identity. Once a newer or
/// conflicting authoritative registration is observed, every network surface
/// using this gate stops participating until the process is restarted.
#[derive(Debug, Clone)]
pub struct ProcessIdentityGate(Arc<AtomicBool>);

impl Default for ProcessIdentityGate {
    fn default() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }
}

impl ProcessIdentityGate {
    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub fn fence(&self) {
        self.0.store(false, Ordering::Release);
    }
}

/// A key's version is the Raft log index of its last mutation, so versions are
/// strictly monotonic per partition and never reused (DESIGN §4.1).
pub type Version = u64;

/// Read consistency, selected per request (DESIGN §8.3, §15). The default is
/// linearizable, so a caller must explicitly opt into staleness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Consistency {
    /// ReadIndex against the current membership, then serve applied state. The
    /// only path that never returns a value older than the last ack'd write.
    Linearizable,
    /// Serve from any voter's locally-applied state, skipping ReadIndex. May
    /// return a superseded value. `min_version`, when set, enforces
    /// read-your-writes / monotonic reads: the replica must have applied at
    /// least that version or it redirects to fresher candidates.
    Stale { min_version: Option<Version> },
}

/// Identifies one Raft group: the single meta group or one data partition.
///
/// `Ord` is derived so groups can live in `BTreeSet`/`BTreeMap` with a stable
/// order; `Meta` sorts before any `Data`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum GroupId {
    Meta,
    Data(u16),
}

impl GroupId {
    /// Canonical stable token used in column-family names and the wire
    /// envelope. Never change these strings — they are on-disk identifiers.
    pub fn token(&self) -> String {
        match self {
            GroupId::Meta => "meta".to_string(),
            GroupId::Data(p) => format!("data_{p}"),
        }
    }

    pub fn cf_log(&self) -> String {
        format!("cf_log_{}", self.token())
    }

    pub fn cf_state(&self) -> String {
        format!("cf_state_{}", self.token())
    }
}

/// A Raft log identifier. A local mirror of openraft's `LogId` carrying enough
/// to compare configuration commit points (`config_log_id`, `voters_log_id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogId {
    pub term: u64,
    pub index: u64,
}

impl LogId {
    pub const fn new(term: u64, index: u64) -> Self {
        LogId { term, index }
    }
}

// ---------------------------------------------------------------------------
// Client data operations (DESIGN §4.2)
// ---------------------------------------------------------------------------

/// Optional compare-and-set predicate carried by `put`/`delete`.
///
/// `Number(v)` applies only if the current key version equals `v`; against an
/// absent key any numeric value fails. `Absent` is the create-only sentinel:
/// accepted by `put` only, and a `delete` carrying it is rejected as malformed
/// before any Raft proposal (DESIGN §4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IfVersion {
    Number(u64),
    Absent,
}

/// The mutating operation, independent of the client/sequence framing. The
/// command digest (M2) is taken over the canonical bytes of this value, so a
/// retry at the same sequence matches only an identical operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataOp {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        if_version: Option<IfVersion>,
    },
    Delete {
        key: Vec<u8>,
        if_version: Option<IfVersion>,
    },
}

impl DataOp {
    pub fn key(&self) -> &[u8] {
        match self {
            DataOp::Put { key, .. } | DataOp::Delete { key, .. } => key,
        }
    }
}

/// A full replicated data command: a client-tagged operation. This is what a
/// data-group Raft log entry carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataRequest {
    pub client_id: ClientId,
    pub sequence: Sequence,
    pub op: DataOp,
}

/// The current presence of a key, returned inside a failed CAS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyPresence {
    Absent,
    Present { version: Version },
}

/// Result of a decided mutation (DESIGN §4.2). A successful mutation — including
/// an unconditional delete of an absent key — returns the committing log index
/// as `version`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationResult {
    Applied { version: Version },
    ConditionFailed { current: KeyPresence },
}

// ---------------------------------------------------------------------------
// Cluster configuration record (DESIGN §3.1, §5.1)
// ---------------------------------------------------------------------------

/// The canonical key-hash specification, persisted at `init` as a protocol
/// constant rather than a crate-version default (IMPLEMENTATION §M1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HashAlgo {
    /// xxhash64 over the raw key bytes.
    Xxh64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashSpec {
    pub algo: HashAlgo,
    pub seed: u64,
}

impl HashSpec {
    /// The v1 canonical spec: xxh64, seed 0, raw key bytes.
    pub const CANONICAL: HashSpec = HashSpec {
        algo: HashAlgo::Xxh64,
        seed: 0,
    };

    /// Map a key onto a partition id in `0..p`. `p` must be non-zero.
    pub fn partition_of(&self, key: &[u8], p: u16) -> u16 {
        let h = match self.algo {
            HashAlgo::Xxh64 => xxhash_rust::xxh64::xxh64(key, self.seed),
        };
        (h % (p as u64)) as u16
    }
}

/// Immutable cluster record committed by `ClusterInit` (DESIGN §3.1). Once
/// written it never changes; a conflicting re-init fails rather than forking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub cluster_id: ClusterId,
    pub protocol_version: u32,
    /// Fixed partition count, `1..=u16::MAX`.
    pub p: u16,
    /// Replication factor, `>= 3`.
    pub r: u8,
    pub hash_spec: HashSpec,
}

// ---------------------------------------------------------------------------
// Placement and plans (DESIGN §5.2)
// ---------------------------------------------------------------------------

/// One immutable in-flight move plan for a group. Never replaced while present;
/// only the fenced report of §7.5 clears it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovePlan {
    /// Meta log index of plan creation; stable token through resolution.
    pub plan_id: u64,
    /// Differs from `voters` by exactly one node.
    pub target_voters: Vec<NodeId>,
    /// One-way flag; bars start/resume once set (§7.5).
    pub aborting: bool,
}

/// The meta group's placement record for one data partition (DESIGN §5.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    /// Last confirmed committed data-Raft voter set, length `R`.
    pub voters: Vec<NodeId>,
    /// Data-Raft log id that committed `voters`.
    pub voters_log_id: LogId,
    pub r#move: Option<MovePlan>,
}

/// The single canonical way any code derives a comparable voter set from a
/// voter list: sets, not raw configs (IMPLEMENTATION ground rule 5). The
/// openraft `Membership` overload arrives in M3 and delegates here.
pub fn voter_set<I: IntoIterator<Item = NodeId>>(voters: I) -> BTreeSet<NodeId> {
    voters.into_iter().collect()
}

// ---------------------------------------------------------------------------
// Node directory (DESIGN §9.1)
// ---------------------------------------------------------------------------

/// Replicated node lifecycle state. Transitions are committed meta-group
/// decisions, never inferred locally (DESIGN §9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Active,
    Suspect,
    Down,
    Draining,
}

/// A directory entry for one registered node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeDirectoryEntry {
    pub node_id: NodeId,
    pub control_addr: String,
    pub bulk_addr: String,
    pub state: NodeState,
    /// Durable incarnation; bumped on rejoin so stale heartbeats can't
    /// reactivate a `Down` node (DESIGN §9.1).
    pub incarnation: u64,
}

// ---------------------------------------------------------------------------
// Meta commands (IMPLEMENTATION §M1, DESIGN §5, §7)
// ---------------------------------------------------------------------------

/// The replicated meta-group command set. All validation runs inside the meta
/// state machine's `apply` (DESIGN §5.2). `FinalizePlan`/`AbortReport` are
/// submittable only by the internal peer-control path (ground rule 9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaCommand {
    ClusterInit {
        config: ClusterConfig,
        meta_voters: Vec<NodeId>,
    },
    RegisterNode {
        node_id: NodeId,
        control_addr: String,
        bulk_addr: String,
    },
    /// Seed one group's initial voter configuration during bootstrap (DESIGN
    /// §3.1 step 4). Peer-control only; accepted once per group, then only on a
    /// byte-identical retry. Never authorises serving — that is the group's own
    /// Raft state (§7.4).
    SeedPlacement {
        group: GroupId,
        voters: Vec<NodeId>,
    },
    SetNodeState {
        node_id: NodeId,
        state: NodeState,
        incarnation: u64,
    },
    CreatePlan {
        group: GroupId,
        plan_id: u64,
        target_voters: Vec<NodeId>,
    },
    MarkAborting {
        group: GroupId,
        plan_id: u64,
    },
    FinalizePlan {
        group: GroupId,
        plan_id: u64,
        observation: DataConfigObservation,
    },
    AbortReport {
        group: GroupId,
        plan_id: u64,
        observation: DataConfigObservation,
    },
}

// ---------------------------------------------------------------------------
// Node-local control records (DESIGN §6, §7.4) — live in the default CF, never
// in a group's CFs, so they survive snapshot install and CF reclamation.
// ---------------------------------------------------------------------------

/// A durable bootstrap descriptor: one of exactly two ways group state is
/// created (ground rule 8). Accepted on retry only when byte-identical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapGroup {
    pub cluster_id: ClusterId,
    pub group: GroupId,
    /// The full pre-admitted initial voter configuration for this group.
    pub members: Vec<NodeId>,
}

/// A durable record that this node admitted itself as a learner for a specific
/// plan (ground rule 8). Written before the group's CFs are created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LearnerAdmission {
    pub cluster_id: ClusterId,
    pub group: GroupId,
    pub plan_id: u64,
}

/// The exact replicated directory identity this process was authorized to use.
/// Heartbeats and endpoint ownership remain bound to this tuple for the entire
/// process lifetime; a later directory incarnation requires a restart (or
/// fences this process when discovered by the live verifier).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistrationBinding {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub control_addr: String,
    pub bulk_addr: String,
    pub directory_incarnation: u64,
}

impl RegistrationBinding {
    pub fn matches_endpoints(&self, control_addr: &str, bulk_addr: &str) -> bool {
        self.control_addr == control_addr && self.bulk_addr == bulk_addr
    }
}

/// A leader's pending, not-yet-meta-confirmed configuration observation
/// (DESIGN §7.2 step 5, §7.5). Retained in the pending-report journal until
/// meta confirms or rejects it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataConfigObservation {
    pub group: GroupId,
    pub plan_id: u64,
    /// Exactly `voters` or exactly `target_voters` — never a joint config.
    pub voter_set: Vec<NodeId>,
    pub config_log_id: LogId,
}

/// Node-local serving-gate record: whether this node may serve/participate in a
/// group. A node that has applied its removal records non-serving before
/// reclaiming CFs (DESIGN §7.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServingState {
    /// Established membership through the group's own Raft state; may serve.
    Serving,
    /// Applied its removal; must not serve and may reclaim local data.
    NonServing,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_cf_names_are_stable() {
        assert_eq!(GroupId::Meta.cf_log(), "cf_log_meta");
        assert_eq!(GroupId::Meta.cf_state(), "cf_state_meta");
        assert_eq!(GroupId::Data(5).cf_log(), "cf_log_data_5");
        assert_eq!(GroupId::Data(5).cf_state(), "cf_state_data_5");
    }

    #[test]
    fn group_ordering_meta_before_data() {
        assert!(GroupId::Meta < GroupId::Data(0));
        assert!(GroupId::Data(1) < GroupId::Data(2));
    }

    #[test]
    fn partition_of_is_in_range_and_deterministic() {
        let spec = HashSpec::CANONICAL;
        for p in [1u16, 2, 3, 128, u16::MAX] {
            for key in [b"".as_slice(), b"a", b"hello world", b"\x00\xff"] {
                let a = spec.partition_of(key, p);
                let b = spec.partition_of(key, p);
                assert_eq!(a, b, "hash must be deterministic");
                assert!(a < p, "partition {a} out of range for P={p}");
            }
        }
    }

    #[test]
    fn voter_set_dedups_and_sorts() {
        let vs = voter_set([3u64, 1, 2, 1]);
        assert_eq!(vs.into_iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }
}
