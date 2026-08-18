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

use crate::api::ops::RoutingInfo;
use crate::codec;
use crate::config::NodeConfig;
use crate::error::{Error, Result};
use crate::meta::bootstrap::BootstrapDescriptor;
use crate::meta::state_machine::MetaApplyResult;
use crate::runtime::directory::fetch_authoritative_directory;
use crate::transport::Transport;
use crate::transport::codec::{Envelope, MsgType};
use crate::transport::dealer::ZmqTransport;
use crate::transport::raft_wire::{
    AbortPlanBody, JoinBody, LeaveBody, SearchIndexAdminBody, SubmitReply,
};
use crate::types::{ClusterId, GroupId, NodeId};

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
            let Ok(reply) = transport.call(&addr, request).await else {
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
    let transport = ZmqTransport::new(ctx, ADMIN_TIMEOUT);
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

/// `dal join`: register this node with the cluster it is configured for, using
/// its own config for identity and its seeds as the contact list.
pub async fn join(ctx: zmq::Context, cfg: &NodeConfig) -> Result<MetaApplyResult> {
    if cfg.seeds.is_empty() {
        return Err(Error::Config(
            "join needs at least one seed in the node config".into(),
        ));
    }
    let transport = ZmqTransport::new(ctx, ADMIN_TIMEOUT);
    let targets = Targets {
        candidates: cfg.seeds.clone(),
        directory: HashMap::new(),
    }
    .discover_live(&transport, cfg.cluster_id)
    .await;
    let body = codec::encode(&JoinBody {
        node_id: cfg.node_id,
        control_addr: cfg.control_addr.clone(),
        bulk_addr: cfg.bulk_addr.clone(),
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

/// `dal leave`: mark a node `Draining` so the cluster migrates its partitions
/// away (DESIGN §9.2).
pub async fn leave(
    ctx: zmq::Context,
    desc: &BootstrapDescriptor,
    node_id: NodeId,
    seed: Option<&str>,
) -> Result<MetaApplyResult> {
    let transport = ZmqTransport::new(ctx, ADMIN_TIMEOUT);
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
    let transport = ZmqTransport::new(ctx, ADMIN_TIMEOUT);
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
    let transport = ZmqTransport::new(ctx, ADMIN_TIMEOUT);
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
