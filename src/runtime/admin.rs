//! Operator admin client for the `dal` CLI subcommands.
//!
//! `status`, `join`, `leave`, and `abort-plan` are thin ZMQ clients that send a
//! single typed control frame to a cluster member and render the reply. They
//! speak the same envelope/codec as the peer-control dispatcher
//! ([`crate::runtime::dispatch::RootDispatch`]), not the data-path
//! [`crate::api::client`]. Genesis has no admin frame — `dal run` drives it from
//! the bootstrap descriptor, so there is no `init` here.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use futures::StreamExt;

use crate::api::ops::RoutingInfo;
use crate::codec;
use crate::config::NodeConfig;
use crate::error::{Error, Result};
use crate::meta::bootstrap::BootstrapDescriptor;
use crate::meta::state_machine::MetaApplyResult;
use crate::runtime::directory::fetch_authoritative_directory;
use crate::transport::Transport;
use crate::transport::codec::{Envelope, Lane, MsgType};
use crate::transport::dealer::ZmqTransport;
use crate::transport::raft_wire::{
    AbortPlanBody, JoinBody, LeaveBody, MaintenanceQueryReply, MaintenanceReleaseBody,
    SearchIndexAdminBody, SubmitReply,
};
use crate::transport::security::CurveClientConfig;
use crate::types::{ClusterId, GroupId, MaintenanceRecord, NodeId};

/// Per-call request timeout for an operator command. Operator commands are
/// interactive one-shots, so a few seconds is ample and a hung member should
/// surface quickly rather than block the CLI.
const ADMIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound on member contacts for one command, so a cluster with no reachable
/// leader fails rather than loops.
const MAX_ATTEMPTS: usize = 32;

/// Where to send a command and how to follow a `NotLeader` hint. `candidates`
/// are tried in order; `directory` resolves a hinted leader id to its address.
struct Targets {
    candidates: Vec<String>,
    directory: HashMap<NodeId, String>,
}

impl Targets {
    /// Contact `contact_ids` (resolved through the descriptor directory), or a
    /// single explicit `seed` when the operator pins one. The full directory is
    /// always retained so a `NotLeader` hint for any node can be followed.
    fn from_descriptor(
        desc: &BootstrapDescriptor,
        seed: Option<&str>,
        contact_ids: &[NodeId],
    ) -> Targets {
        let directory: HashMap<NodeId, String> = desc
            .directory
            .iter()
            .map(|e| (e.node_id, e.control_addr.clone()))
            .collect();
        let mut candidates = Vec::new();
        if let Some(addr) = seed {
            candidates.push(addr.to_string());
        }
        candidates.extend(
            contact_ids
                .iter()
                .filter_map(|id| directory.get(id).cloned()),
        );
        let mut seen = HashSet::new();
        candidates.retain(|address| seen.insert(address.clone()));
        Targets {
            candidates,
            directory,
        }
    }

    /// Replace immutable bootstrap targets with a ReadIndex-fenced live
    /// directory when possible. If no leader is reachable, an advisory routing
    /// snapshot may expand the contact set, but remains discovery-only.
    async fn discover_live(mut self, transport: &ZmqTransport, cluster_id: ClusterId) -> Targets {
        if let Ok(live) = fetch_authoritative_directory(
            transport,
            cluster_id,
            self.candidates.clone(),
            ADMIN_TIMEOUT,
        )
        .await
        {
            self.directory = live
                .iter()
                .map(|entry| (entry.node_id, entry.control_addr.clone()))
                .collect();
            for entry in live {
                if !self.candidates.contains(&entry.control_addr) {
                    self.candidates.push(entry.control_addr);
                }
            }
            return self;
        }

        for addr in self.candidates.clone() {
            let request =
                Envelope::new(cluster_id, MsgType::MetaQuery, GroupId::Meta, 0, Vec::new());
            let Ok(reply) = transport
                .call_with_timeout(&addr, request, Some(ADMIN_TIMEOUT))
                .await
            else {
                continue;
            };
            if reply.cluster_id != cluster_id || reply.msg_type != MsgType::MetaQuery {
                continue;
            }
            let Ok(info) = codec::decode::<RoutingInfo>(&reply.payload) else {
                continue;
            };
            if info.cluster_id != cluster_id {
                continue;
            }
            for entry in info.directory {
                self.directory
                    .insert(entry.node_id, entry.control_addr.clone());
                if !self.candidates.contains(&entry.control_addr) {
                    self.candidates.push(entry.control_addr);
                }
            }
            break;
        }
        self
    }
}

/// Submit a meta-group command to the first member that is (or points at) the
/// leader, following `NotLeader` hints. Returns the leader's apply outcome.
async fn submit(
    transport: &ZmqTransport,
    cluster_id: ClusterId,
    targets: &Targets,
    msg_type: MsgType,
    body: Vec<u8>,
) -> Result<MetaApplyResult> {
    let mut queue: VecDeque<String> = targets.candidates.iter().cloned().collect();
    let mut tried: HashSet<String> = HashSet::new();
    let mut last_error: Option<String> = None;

    while let Some(addr) = queue.pop_front() {
        if tried.len() >= MAX_ATTEMPTS {
            break;
        }
        if !tried.insert(addr.clone()) {
            continue;
        }

        let env = Envelope::new(
            cluster_id,
            msg_type,
            GroupId::Meta,
            tried.len() as u64,
            body.clone(),
        );
        let reply = match transport.call(&addr, env).await {
            Ok(reply) => reply,
            // Unreachable or timed out: try the next candidate.
            Err(_) => continue,
        };
        if reply.cluster_id != cluster_id {
            last_error = Some(format!(
                "reply from cluster {:#x}, expected {cluster_id:#x}",
                reply.cluster_id
            ));
            continue;
        }
        match codec::decode::<SubmitReply>(&reply.payload) {
            // The leader decided the command: this is authoritative.
            Ok(SubmitReply::Outcome(outcome)) => return Ok(outcome),
            Ok(SubmitReply::NotLeader { leader }) => {
                if let Some(id) = leader
                    && let Some(hint) = targets.directory.get(&id)
                    && !tried.contains(hint)
                {
                    queue.push_front(hint.clone());
                }
            }
            Ok(SubmitReply::JoinPreflightReady) => {
                last_error = Some("unexpected join-preflight reply to an admin command".into());
            }
            // Only the leader (or a non-meta node) answers with an error; record
            // it and keep walking, since followers reply `NotLeader`.
            Ok(SubmitReply::Error(e)) => last_error = Some(e),
            Err(e) => last_error = Some(format!("undecodable reply: {e}")),
        }
    }

    Err(Error::Raft(last_error.unwrap_or_else(|| {
        "no cluster member accepted the command".into()
    })))
}

/// `dal status`: fetch the cluster routing snapshot from the first reachable
/// member. Any node answers a `MetaQuery` from its cached routing, so no leader
/// is required.
pub async fn status(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    seed: Option<&str>,
) -> Result<RoutingInfo> {
    status_with_transport(
        ZmqTransport::new(ctx, ADMIN_TIMEOUT, Lane::Control),
        desc,
        seed,
    )
    .await
}

pub async fn status_secure(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    seed: Option<&str>,
    curve: CurveClientConfig,
) -> Result<RoutingInfo> {
    status_with_transport(secure_transport(ctx, curve, seed)?, desc, seed).await
}

fn secure_transport(
    ctx: zmq::Context,
    curve: CurveClientConfig,
    seed: Option<&str>,
) -> Result<ZmqTransport> {
    secure_transport_with_timeout(ctx, curve, seed, ADMIN_TIMEOUT)
}

fn secure_transport_with_timeout(
    ctx: zmq::Context,
    curve: CurveClientConfig,
    seed: Option<&str>,
    timeout: Duration,
) -> Result<ZmqTransport> {
    if let Some(seed) = seed {
        curve.server_key(seed)?;
    }
    Ok(ZmqTransport::new_curve(ctx, timeout, Lane::Control, curve))
}

async fn status_with_transport(
    transport: ZmqTransport,
    desc: &BootstrapDescriptor,
    seed: Option<&str>,
) -> Result<RoutingInfo> {
    let contact_ids: Vec<NodeId> = desc.directory.iter().map(|e| e.node_id).collect();
    let targets = Targets::from_descriptor(desc, seed, &contact_ids)
        .discover_live(&transport, desc.cluster_id)
        .await;

    let mut last_error: Option<String> = None;
    for addr in &targets.candidates {
        let env = Envelope::new(
            desc.cluster_id,
            MsgType::MetaQuery,
            GroupId::Meta,
            1,
            Vec::new(),
        );
        let reply = match transport.call(addr, env).await {
            Ok(reply) => reply,
            Err(e) => {
                last_error = Some(e.to_string());
                continue;
            }
        };
        if reply.cluster_id != desc.cluster_id || reply.msg_type != MsgType::MetaQuery {
            last_error = Some(format!("unexpected routing reply from {addr}"));
            continue;
        }
        match codec::decode::<RoutingInfo>(&reply.payload) {
            Ok(info) => return Ok(info),
            Err(e) => last_error = Some(format!("undecodable routing reply from {addr}: {e}")),
        }
    }
    Err(Error::Raft(last_error.unwrap_or_else(|| {
        "no cluster member answered a routing query".into()
    })))
}

/// Read the maintenance operation from the current meta leader through the
/// operator-only control path. A follower's cached record is never returned.
pub async fn maintenance_status_secure(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    seed: Option<&str>,
    curve: CurveClientConfig,
) -> Result<Option<MaintenanceRecord>> {
    let transport = secure_transport(ctx, curve, seed)?;
    let targets = Targets::from_descriptor(desc, seed, &desc.meta_voters)
        .discover_live(&transport, desc.cluster_id)
        .await;
    let mut last_error = None;
    for address in &targets.candidates {
        let request = Envelope::new(
            desc.cluster_id,
            MsgType::MaintenanceStatus,
            GroupId::Meta,
            0,
            Vec::new(),
        );
        let reply = match transport.call(address, request).await {
            Ok(reply) => reply,
            Err(error) => {
                last_error = Some(error.to_string());
                continue;
            }
        };
        if reply.cluster_id != desc.cluster_id
            || reply.msg_type != MsgType::MaintenanceStatus
            || reply.group_id != GroupId::Meta
        {
            last_error = Some(format!("unexpected maintenance reply from {address}"));
            continue;
        }
        match codec::decode::<MaintenanceQueryReply>(&reply.payload) {
            Ok(MaintenanceQueryReply::Value(record)) => return Ok(record),
            Ok(MaintenanceQueryReply::NotLeader | MaintenanceQueryReply::Unavailable) => {}
            Err(_) => {
                if let Ok(SubmitReply::Error(error)) = codec::decode(&reply.payload) {
                    last_error = Some(error);
                } else {
                    last_error = Some(format!("undecodable maintenance reply from {address}"));
                }
            }
        }
    }
    Err(Error::Raft(last_error.unwrap_or_else(|| {
        "no meta leader answered the maintenance status query".into()
    })))
}

/// Request release of the exact verified operation. The leader gathers fresh
/// heartbeat evidence; callers cannot supply or bypass that evidence.
pub async fn maintenance_release_secure(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    operation_id: u128,
    generation: u64,
    seed: Option<&str>,
    curve: CurveClientConfig,
    timeout: Duration,
) -> Result<MetaApplyResult> {
    if operation_id == 0 || generation == 0 || timeout.is_zero() {
        return Err(Error::Config(
            "maintenance release identity and timeout are required".into(),
        ));
    }
    let transport = secure_transport_with_timeout(ctx, curve, seed, timeout)?;
    let targets = Targets::from_descriptor(desc, seed, &desc.meta_voters)
        .discover_live(&transport, desc.cluster_id)
        .await;
    submit(
        &transport,
        desc.cluster_id,
        &targets,
        MsgType::MaintenanceRelease,
        codec::encode(&MaintenanceReleaseBody {
            operation_id,
            generation,
        }),
    )
    .await
}

/// `dal join`: register this node with the cluster it is configured for, using
/// its own config for identity and its seeds as the contact list.
pub async fn join(ctx: zmq::Context, cfg: &NodeConfig) -> Result<MetaApplyResult> {
    join_with_transport(
        ZmqTransport::new(ctx, ADMIN_TIMEOUT, Lane::Control),
        cfg,
        0,
        [0; 32],
        false,
    )
    .await
}

pub async fn join_secure(
    ctx: zmq::Context,
    cfg: &NodeConfig,
    curve: CurveClientConfig,
    policy_generation: u64,
    candidate_public_key: [u8; 32],
) -> Result<MetaApplyResult> {
    for seed in &cfg.seeds {
        curve.server_key(seed)?;
    }
    join_with_transport(
        secure_transport(ctx, curve, None)?,
        cfg,
        policy_generation,
        candidate_public_key,
        true,
    )
    .await
}

async fn join_with_transport(
    transport: ZmqTransport,
    cfg: &NodeConfig,
    policy_generation: u64,
    candidate_public_key: [u8; 32],
    require_peer_preflight: bool,
) -> Result<MetaApplyResult> {
    if cfg.seeds.is_empty() {
        return Err(Error::Config(
            "join needs at least one seed in the node config".into(),
        ));
    }
    let targets = if require_peer_preflight {
        let directory = fetch_authoritative_directory(
            &transport,
            cfg.cluster_id,
            cfg.seeds.clone(),
            ADMIN_TIMEOUT,
        )
        .await
        .map_err(|error| {
            Error::Raft(format!(
                "join requires an authoritative live directory for enrollment preflight: {error}"
            ))
        })?;
        let mut targets = Targets {
            candidates: cfg.seeds.clone(),
            directory: directory
                .iter()
                .map(|entry| (entry.node_id, entry.control_addr.clone()))
                .collect(),
        };
        for entry in directory {
            if !targets.candidates.contains(&entry.control_addr) {
                targets.candidates.push(entry.control_addr);
            }
        }
        targets
    } else {
        Targets {
            candidates: cfg.seeds.clone(),
            directory: HashMap::new(),
        }
        .discover_live(&transport, cfg.cluster_id)
        .await
    };
    if targets.directory.is_empty() {
        return Err(Error::Raft(
            "join could not discover the current node directory for enrollment preflight".into(),
        ));
    }
    if targets.directory.len() > crate::types::MAX_CLUSTER_NODES {
        return Err(Error::Config(
            "join directory exceeds the node limit".into(),
        ));
    }
    if require_peer_preflight {
        preflight_join_peers(
            &transport,
            cfg,
            &targets,
            policy_generation,
            candidate_public_key,
        )
        .await?;
    }
    let body = codec::encode(&JoinBody {
        node_id: cfg.node_id,
        control_addr: cfg.control_addr.clone(),
        bulk_addr: cfg.bulk_addr.clone(),
        policy_generation,
        curve_public_key: candidate_public_key,
        preflight: false,
    });
    submit(
        &transport,
        cfg.cluster_id,
        &targets,
        MsgType::JoinRequest,
        body,
    )
    .await
}

async fn preflight_join_peers(
    transport: &ZmqTransport,
    cfg: &NodeConfig,
    targets: &Targets,
    policy_generation: u64,
    candidate_public_key: [u8; 32],
) -> Result<()> {
    let expected = targets.directory.len();
    let mut addresses: Vec<_> = targets.directory.values().cloned().collect();
    addresses.sort();
    addresses.dedup();
    if addresses.len() != expected {
        return Err(Error::Config(
            "join directory contains duplicate node endpoints".into(),
        ));
    }
    let replies = futures::stream::iter(addresses.into_iter().map(|address| async move {
        let request = Envelope::new(
            cfg.cluster_id,
            MsgType::JoinRequest,
            GroupId::Meta,
            0,
            codec::encode(&JoinBody {
                node_id: cfg.node_id,
                control_addr: cfg.control_addr.clone(),
                bulk_addr: cfg.bulk_addr.clone(),
                policy_generation,
                curve_public_key: candidate_public_key,
                preflight: true,
            }),
        );
        (address.clone(), transport.call(&address, request).await)
    }))
    .buffer_unordered(32)
    .collect::<Vec<_>>()
    .await;
    for (address, reply) in replies {
        let reply = reply.map_err(|error| {
            Error::Raft(format!(
                "join preflight could not contact peer {address}: {error}"
            ))
        })?;
        if reply.cluster_id != cfg.cluster_id || reply.msg_type != MsgType::JoinRequest {
            return Err(Error::Raft(format!(
                "join preflight received an invalid reply from peer {address}"
            )));
        }
        match codec::decode::<SubmitReply>(&reply.payload) {
            Ok(SubmitReply::JoinPreflightReady) => {}
            Ok(SubmitReply::Error(error)) => {
                return Err(Error::Config(format!(
                    "peer {address} rejected join preflight: {error}"
                )));
            }
            _ => {
                return Err(Error::Raft(format!(
                    "peer {address} did not confirm join preflight"
                )));
            }
        }
    }
    Ok(())
}

/// `dal leave`: mark a node `Draining` so the cluster migrates its partitions
/// away (DESIGN §9.2).
pub async fn leave(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    node_id: NodeId,
    seed: Option<&str>,
) -> Result<MetaApplyResult> {
    leave_with_transport(
        ZmqTransport::new(ctx, ADMIN_TIMEOUT, Lane::Control),
        desc,
        node_id,
        seed,
    )
    .await
}

pub async fn leave_secure(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    node_id: NodeId,
    seed: Option<&str>,
    curve: CurveClientConfig,
) -> Result<MetaApplyResult> {
    leave_with_transport(secure_transport(ctx, curve, seed)?, desc, node_id, seed).await
}

async fn leave_with_transport(
    transport: ZmqTransport,
    desc: &BootstrapDescriptor,
    node_id: NodeId,
    seed: Option<&str>,
) -> Result<MetaApplyResult> {
    let targets = Targets::from_descriptor(desc, seed, &desc.meta_voters)
        .discover_live(&transport, desc.cluster_id)
        .await;
    let body = codec::encode(&LeaveBody { node_id });
    submit(
        &transport,
        desc.cluster_id,
        &targets,
        MsgType::LeaveRequest,
        body,
    )
    .await
}

/// `dal abort-plan`: mark a stuck move plan `aborting` so the driver rolls it
/// back (DESIGN §7.5).
pub async fn abort_plan(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    group: GroupId,
    plan_id: u64,
    seed: Option<&str>,
) -> Result<MetaApplyResult> {
    abort_plan_with_transport(
        ZmqTransport::new(ctx, ADMIN_TIMEOUT, Lane::Control),
        desc,
        group,
        plan_id,
        seed,
    )
    .await
}

pub async fn abort_plan_secure(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    group: GroupId,
    plan_id: u64,
    seed: Option<&str>,
    curve: CurveClientConfig,
) -> Result<MetaApplyResult> {
    abort_plan_with_transport(
        secure_transport(ctx, curve, seed)?,
        desc,
        group,
        plan_id,
        seed,
    )
    .await
}

async fn abort_plan_with_transport(
    transport: ZmqTransport,
    desc: &BootstrapDescriptor,
    group: GroupId,
    plan_id: u64,
    seed: Option<&str>,
) -> Result<MetaApplyResult> {
    let targets = Targets::from_descriptor(desc, seed, &desc.meta_voters)
        .discover_live(&transport, desc.cluster_id)
        .await;
    let body = codec::encode(&AbortPlanBody { group, plan_id });
    submit(
        &transport,
        desc.cluster_id,
        &targets,
        MsgType::AbortPlanRequest,
        body,
    )
    .await
}

/// Create, rebuild, or drop a search index through the meta leader. The body is
/// deliberately a narrow operator command rather than a generic MetaCommand.
pub async fn search_index_admin(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    command: SearchIndexAdminBody,
    seed: Option<&str>,
) -> Result<MetaApplyResult> {
    search_index_admin_with_transport(
        ZmqTransport::new(ctx, ADMIN_TIMEOUT, Lane::Control),
        desc,
        command,
        seed,
    )
    .await
}

pub async fn search_index_admin_secure(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    command: SearchIndexAdminBody,
    seed: Option<&str>,
    curve: CurveClientConfig,
) -> Result<MetaApplyResult> {
    search_index_admin_with_transport(secure_transport(ctx, curve, seed)?, desc, command, seed)
        .await
}

async fn search_index_admin_with_transport(
    transport: ZmqTransport,
    desc: &BootstrapDescriptor,
    command: SearchIndexAdminBody,
    seed: Option<&str>,
) -> Result<MetaApplyResult> {
    let targets = Targets::from_descriptor(desc, seed, &desc.meta_voters)
        .discover_live(&transport, desc.cluster_id)
        .await;
    submit(
        &transport,
        desc.cluster_id,
        &targets,
        MsgType::SearchIndexAdmin,
        codec::encode(&command),
    )
    .await
}
