//! The resumable bootstrap driver (DESIGN §3.1, IMPLEMENTATION §M5).
//!
//! Cluster creation is an explicit, *resumable* protocol, not a local shortcut.
//! Its durable records ([`BootstrapGroup`], [`LearnerAdmission`]) are accepted on
//! retry only when byte-identical, and a conflicting record fails rather than
//! forking the cluster. Group state is created in exactly two ways — a bootstrap
//! descriptor or a learner admission (ground rule 8) — enforced here.
//!
//! The phases (per §3.1): (1) an immutable descriptor; (2) sync-durable
//! `BootstrapGroup` + identity on every initial meta voter; (3) the designated
//! node initializes the meta Raft group and commits `ClusterInit`; (4) the
//! initial placement records commit; (5) each data group's voters record their
//! `BootstrapGroup` and the designated data voter initializes it. Startup
//! reconciliation re-runs these idempotently.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::keyspace;
use crate::meta::node::{MetaNode, ProposeOutcome};
use crate::meta::state_machine::MetaApplyResult;
use crate::storage::Storage;
use crate::types::{
    BootstrapGroup, ClusterConfig, ClusterId, GroupId, LearnerAdmission, MetaCommand, NodeId,
};

/// One node's directory coordinates, carried in the immutable descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirEntry {
    pub node_id: NodeId,
    pub control_addr: String,
    pub bulk_addr: String,
}

/// The immutable bootstrap descriptor: cluster identity, config, the initial
/// meta voter set, the node directory, and each data partition's genesis voter
/// configuration. Derived once (fresh `cluster_id`) or reused from a durable
/// record on retry (DESIGN §3.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapDescriptor {
    pub cluster_id: ClusterId,
    pub config: ClusterConfig,
    pub meta_voters: Vec<NodeId>,
    pub directory: Vec<DirEntry>,
    pub data_placements: Vec<(u16, Vec<NodeId>)>,
}

/// Whether a durable record was freshly written or already present (identical).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapOutcome {
    Created,
    AlreadyPresent,
}

/// The deterministic designated node for a group's genesis: the lowest voter id
/// (DESIGN §3.1). Every node computes the same one, so exactly one initializes.
pub fn designated(voters: &[NodeId]) -> Option<NodeId> {
    voters.iter().copied().min()
}

/// Idempotently write a group's `BootstrapGroup` marker. On retry it is accepted
/// only when byte-identical; a differing record is a fork attempt and fails.
pub fn ensure_bootstrap_group(
    storage: &Storage,
    record: &BootstrapGroup,
) -> Result<BootstrapOutcome> {
    let key = keyspace::bootstrap_key(record.group);
    match storage.get_local::<BootstrapGroup>(&key)? {
        Some(existing) if &existing == record => Ok(BootstrapOutcome::AlreadyPresent),
        Some(_) => Err(Error::Config(format!(
            "conflicting bootstrap record for {:?}",
            record.group
        ))),
        None => {
            storage.put_local(&key, record)?;
            Ok(BootstrapOutcome::Created)
        }
    }
}

/// Idempotently write a learner-admission record, the *other* way group state
/// may be created (ground rule 8). Accepted on retry only when byte-identical.
pub fn ensure_learner_admission(
    storage: &Storage,
    record: &LearnerAdmission,
) -> Result<BootstrapOutcome> {
    let key = keyspace::admission_key(record.group);
    match storage.get_local::<LearnerAdmission>(&key)? {
        Some(existing) if &existing == record => Ok(BootstrapOutcome::AlreadyPresent),
        Some(_) => Err(Error::Config(format!(
            "conflicting learner admission for {:?}",
            record.group
        ))),
        None => {
            storage.put_local(&key, record)?;
            Ok(BootstrapOutcome::Created)
        }
    }
}

/// Write the meta `BootstrapGroup` marker on each initial meta voter's storage
/// (phase 2). `storages` are the per-node stores keyed alongside their ids.
pub fn record_meta_bootstrap(
    storages: &[(NodeId, Arc<Storage>)],
    desc: &BootstrapDescriptor,
) -> Result<()> {
    let record = BootstrapGroup {
        cluster_id: desc.cluster_id,
        group: GroupId::Meta,
        members: desc.meta_voters.clone(),
    };
    for (id, storage) in storages {
        if desc.meta_voters.contains(id) {
            ensure_bootstrap_group(storage, &record)?;
        }
    }
    Ok(())
}

/// Write each data group's `BootstrapGroup` marker on its voters' storage
/// (phase 5, before the group is initialized).
pub fn record_data_bootstrap(
    storages: &[(NodeId, Arc<Storage>)],
    desc: &BootstrapDescriptor,
) -> Result<()> {
    for (partition, voters) in &desc.data_placements {
        let record = BootstrapGroup {
            cluster_id: desc.cluster_id,
            group: GroupId::Data(*partition),
            members: voters.clone(),
        };
        for (id, storage) in storages {
            if voters.contains(id) {
                ensure_bootstrap_group(storage, &record)?;
            }
        }
    }
    Ok(())
}

/// Commit the cluster record, node directory, and initial placements through the
/// running meta group (phases 3–4). Idempotent: every command replays as a
/// `NoOp` on a resumed bootstrap.
pub async fn seed_cluster(nodes: &[Arc<MetaNode>], desc: &BootstrapDescriptor) -> Result<()> {
    accept(
        propose_to_leader(
            nodes,
            MetaCommand::ClusterInit {
                config: desc.config.clone(),
                meta_voters: desc.meta_voters.clone(),
            },
        )
        .await?,
        "ClusterInit",
    )?;

    for d in &desc.directory {
        accept(
            propose_to_leader(
                nodes,
                MetaCommand::RegisterNode {
                    node_id: d.node_id,
                    control_addr: d.control_addr.clone(),
                    bulk_addr: d.bulk_addr.clone(),
                },
            )
            .await?,
            "RegisterNode",
        )?;
    }

    for (partition, voters) in &desc.data_placements {
        accept(
            propose_to_leader(
                nodes,
                MetaCommand::SeedPlacement {
                    group: GroupId::Data(*partition),
                    voters: voters.clone(),
                },
            )
            .await?,
            "SeedPlacement",
        )?;
    }
    Ok(())
}

/// Attempt to seed through one local meta replica. Unlike [`seed_cluster`],
/// this does not wait for another replica to become leader: it returns `false`
/// as soon as this replica forwards to a leader. Startup calls this from every
/// meta process, allowing whichever replica currently leads to resume a
/// partially completed genesis after a restart.
pub async fn seed_cluster_if_leader(node: &MetaNode, desc: &BootstrapDescriptor) -> Result<bool> {
    let mut commands = Vec::with_capacity(1 + desc.directory.len() + desc.data_placements.len());
    commands.push(MetaCommand::ClusterInit {
        config: desc.config.clone(),
        meta_voters: desc.meta_voters.clone(),
    });
    commands.extend(desc.directory.iter().map(|d| MetaCommand::RegisterNode {
        node_id: d.node_id,
        control_addr: d.control_addr.clone(),
        bulk_addr: d.bulk_addr.clone(),
    }));
    commands.extend(desc.data_placements.iter().map(|(partition, voters)| {
        MetaCommand::SeedPlacement {
            group: GroupId::Data(*partition),
            voters: voters.clone(),
        }
    }));

    for command in commands {
        match node.propose(command).await? {
            ProposeOutcome::Applied(result) => accept(result, "bootstrap command")?,
            ProposeOutcome::NotLeader { .. } => return Ok(false),
        }
    }
    Ok(true)
}

/// Learner-first meta membership change (DESIGN §7.2): admit the new node as a
/// learner, block for durable catch-up, then replace the voter set. The joining
/// node's durable admission record must already be written (ground rule 8).
pub async fn admit_and_promote(
    leader: &MetaNode,
    new_id: NodeId,
    new_voters: &[NodeId],
) -> Result<()> {
    leader.add_learner(new_id).await?;
    leader.change_voters(new_voters).await?;
    Ok(())
}

// -- internals --------------------------------------------------------------

/// A committed command must be freshly applied or an idempotent replay; a
/// rejection during bootstrap is a hard error.
fn accept(result: MetaApplyResult, what: &str) -> Result<()> {
    match result {
        MetaApplyResult::Applied | MetaApplyResult::NoOp => Ok(()),
        MetaApplyResult::Rejected(r) => {
            Err(Error::Config(format!("bootstrap {what} rejected: {r:?}")))
        }
    }
}

async fn propose_to_leader(nodes: &[Arc<MetaNode>], cmd: MetaCommand) -> Result<MetaApplyResult> {
    for _ in 0..200 {
        if let Some(leader) = nodes
            .iter()
            .find(|n| n.current_leader() == Some(n.node_id()))
        {
            match leader.propose(cmd.clone()).await? {
                ProposeOutcome::Applied(r) => return Ok(r),
                ProposeOutcome::NotLeader { .. } => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Error::Raft(
        "no meta leader accepted a bootstrap command".into(),
    ))
}
