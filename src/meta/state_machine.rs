//! The meta group's state machine: cluster identity, the node directory, and
//! the placement map (DESIGN §3.1, §5.2). Every rule lives inside apply, so the
//! decision is a deterministic function of committed input and never depends on
//! who proposed it (IMPLEMENTATION §M5, ground rule 1).
//!
//! Like the data state machine (M2) this is split into a pure [`Self::evaluate`]
//! (decision + mutations, no write) and a thin [`Self::apply`] that commits them
//! atomically with `last_applied`. The Raft wrapper reuses `evaluate` so it can
//! fold the applied `LogId` into the same batch.

use serde::{Deserialize, Serialize};

use crate::codec;
use crate::config::{MIN_META_VOTERS, MIN_REPLICATION};
use crate::error::Result;
use crate::keyspace;
use crate::storage::{StateMutation, Storage};
use crate::types::{
    voter_set, ClusterConfig, GroupId, LogId, MetaCommand, MovePlan, NodeDirectoryEntry, NodeId,
    NodeState, Placement,
};

/// The outcome of applying one committed meta command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaApplyResult {
    /// The command changed replicated state.
    Applied,
    /// A byte-identical retry of an already-applied bootstrap record, or a
    /// no-change transition (e.g. re-marking an already-aborting plan). Accepted
    /// without changing state (DESIGN §3.1).
    NoOp,
    /// The command was refused; state is untouched.
    Rejected(MetaReject),
}

/// Why a meta command was refused. Every rejection leaves state unchanged and is
/// deterministic in the committed input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaReject {
    ClusterNotInitialized,
    /// A re-init whose config or meta voters differ from the committed cluster
    /// record — a fork attempt (DESIGN §3.1).
    ClusterConflict,
    InvalidConfig(String),
    /// `SeedPlacement`/`CreatePlan` for a group with no placement record.
    NoPlacement,
    /// `SeedPlacement` for a group already placed with a different config.
    PlacementConflict,
    /// A move already exists for this group (§5.2: one move per partition).
    PlanExists,
    /// No move exists (or `plan_id` did not match) where one was required.
    NoPlan,
    PlanIdMismatch,
    /// A target/observation that is not a single-voter change, wrong size, or
    /// carries duplicate voters.
    IllegalVoterChange(String),
    /// A target voter is not an `Active` directory node, or too few eligible
    /// nodes exist.
    IneligibleTarget,
    /// A finalize/abort observation whose voter set or binding does not match
    /// the plan (DESIGN §7.5).
    ObservationMismatch,
    /// `FinalizePlan` on a plan already marked aborting — it must be resolved by
    /// an `AbortReport` (§7.5).
    PlanAborting,
    /// `AbortReport` for a plan that is not marked aborting — a report can never
    /// clear a live, healthy move (§7.5).
    NotAborting,
    UnknownNode,
    /// A `SetNodeState` carrying an incarnation older than the recorded one.
    StaleIncarnation,
    /// `SeedPlacement` may seed only data groups; the meta placement is written
    /// by `ClusterInit`.
    SeedMetaForbidden,
}

pub struct MetaStateMachine;

impl MetaStateMachine {
    pub fn new() -> Self {
        MetaStateMachine
    }

    // -- typed accessors ----------------------------------------------------

    fn cluster(&self, s: &Storage) -> Result<Option<ClusterConfig>> {
        s.get_state_record(GroupId::Meta, &keyspace::meta_cluster_key())
    }

    fn node(&self, s: &Storage, id: NodeId) -> Result<Option<NodeDirectoryEntry>> {
        s.get_state_record(GroupId::Meta, &keyspace::meta_node_key(id))
    }

    fn placement(&self, s: &Storage, g: GroupId) -> Result<Option<Placement>> {
        s.get_state_record(GroupId::Meta, &keyspace::meta_placement_key(g))
    }

    /// Every node-directory entry, for eligibility counting.
    fn directory(&self, s: &Storage) -> Result<Vec<NodeDirectoryEntry>> {
        let prefix = keyspace::meta_node_prefix();
        let mut out = Vec::new();
        for (k, v) in s.scan_state(GroupId::Meta)? {
            if k.len() >= 2 && k[0] == prefix[0] && k[1] == prefix[1] {
                out.push(codec::decode(&v)?);
            }
        }
        Ok(out)
    }

    fn is_active(&self, s: &Storage, id: NodeId) -> Result<bool> {
        Ok(matches!(self.node(s, id)?, Some(e) if e.state == NodeState::Active))
    }

    fn eligible_count(&self, s: &Storage) -> Result<usize> {
        Ok(self
            .directory(s)?
            .iter()
            .filter(|e| e.state == NodeState::Active)
            .count())
    }

    // -- apply --------------------------------------------------------------

    /// Decide and commit one command, advancing `last_applied` atomically.
    pub fn apply(
        &self,
        storage: &Storage,
        cmd: &MetaCommand,
        log_id: LogId,
    ) -> Result<MetaApplyResult> {
        let (result, muts) = self.evaluate(storage, cmd, log_id)?;
        storage.apply_state(GroupId::Meta, &muts, log_id)?;
        Ok(result)
    }

    /// Decide the outcome and the state-CF mutations, writing nothing.
    pub fn evaluate(
        &self,
        s: &Storage,
        cmd: &MetaCommand,
        log_id: LogId,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        match cmd {
            MetaCommand::ClusterInit {
                config,
                meta_voters,
            } => self.cluster_init(s, config, meta_voters, log_id),
            MetaCommand::RegisterNode {
                node_id,
                control_addr,
                bulk_addr,
            } => self.register_node(s, *node_id, control_addr, bulk_addr),
            MetaCommand::SeedPlacement { group, voters } => {
                self.seed_placement(s, *group, voters, log_id)
            }
            MetaCommand::SetNodeState {
                node_id,
                state,
                incarnation,
            } => self.set_node_state(s, *node_id, *state, *incarnation),
            MetaCommand::CreatePlan {
                group,
                plan_id: _,
                target_voters,
            } => self.create_plan(s, *group, target_voters, log_id),
            MetaCommand::MarkAborting { group, plan_id } => self.mark_aborting(s, *group, *plan_id),
            MetaCommand::FinalizePlan {
                group,
                plan_id,
                observation,
            } => self.finalize_plan(s, *group, *plan_id, observation, false),
            MetaCommand::AbortReport {
                group,
                plan_id,
                observation,
            } => self.finalize_plan(s, *group, *plan_id, observation, true),
        }
    }

    // -- command handlers ---------------------------------------------------

    fn cluster_init(
        &self,
        s: &Storage,
        config: &ClusterConfig,
        meta_voters: &[NodeId],
        log_id: LogId,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        if let Some(existing) = self.cluster(s)? {
            let existing_meta = self.placement(s, GroupId::Meta)?.map(|p| voter_set(p.voters));
            let incoming_meta = Some(voter_set(meta_voters.to_vec()));
            if &existing == config && existing_meta == incoming_meta {
                return Ok((MetaApplyResult::NoOp, vec![]));
            }
            return Ok((reject(MetaReject::ClusterConflict), vec![]));
        }

        if config.r < MIN_REPLICATION {
            return Ok((invalid("R below minimum"), vec![]));
        }
        if config.p == 0 {
            return Ok((invalid("P must be non-zero"), vec![]));
        }
        let mv = voter_set(meta_voters.to_vec());
        if mv.len() != meta_voters.len() {
            return Ok((invalid("duplicate meta voter"), vec![]));
        }
        if mv.len() < MIN_META_VOTERS || mv.len() % 2 == 0 {
            return Ok((invalid("meta voters must be an odd set of >= 3"), vec![]));
        }

        let muts = vec![
            put(keyspace::meta_cluster_key(), config),
            put(
                keyspace::meta_placement_key(GroupId::Meta),
                &Placement {
                    voters: mv.into_iter().collect(),
                    voters_log_id: log_id,
                    r#move: None,
                },
            ),
        ];
        Ok((MetaApplyResult::Applied, muts))
    }

    fn register_node(
        &self,
        s: &Storage,
        node_id: NodeId,
        control_addr: &str,
        bulk_addr: &str,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        if self.cluster(s)?.is_none() {
            return Ok((reject(MetaReject::ClusterNotInitialized), vec![]));
        }
        let entry = match self.node(s, node_id)? {
            Some(e) if e.control_addr == control_addr && e.bulk_addr == bulk_addr => {
                return Ok((MetaApplyResult::NoOp, vec![]));
            }
            // Re-register with a changed address is a rejoin: bump incarnation so
            // stale heartbeats for the old incarnation cannot reactivate it.
            Some(e) => NodeDirectoryEntry {
                node_id,
                control_addr: control_addr.to_string(),
                bulk_addr: bulk_addr.to_string(),
                state: NodeState::Active,
                incarnation: e.incarnation + 1,
            },
            None => NodeDirectoryEntry {
                node_id,
                control_addr: control_addr.to_string(),
                bulk_addr: bulk_addr.to_string(),
                state: NodeState::Active,
                incarnation: 1,
            },
        };
        Ok((
            MetaApplyResult::Applied,
            vec![put(keyspace::meta_node_key(node_id), &entry)],
        ))
    }

    fn set_node_state(
        &self,
        s: &Storage,
        node_id: NodeId,
        state: NodeState,
        incarnation: u64,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        let Some(mut entry) = self.node(s, node_id)? else {
            return Ok((reject(MetaReject::UnknownNode), vec![]));
        };
        if incarnation < entry.incarnation {
            return Ok((reject(MetaReject::StaleIncarnation), vec![]));
        }
        if entry.state == state && entry.incarnation == incarnation {
            return Ok((MetaApplyResult::NoOp, vec![]));
        }
        entry.state = state;
        entry.incarnation = incarnation;
        Ok((
            MetaApplyResult::Applied,
            vec![put(keyspace::meta_node_key(node_id), &entry)],
        ))
    }

    fn seed_placement(
        &self,
        s: &Storage,
        group: GroupId,
        voters: &[NodeId],
        log_id: LogId,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        let Some(config) = self.cluster(s)? else {
            return Ok((reject(MetaReject::ClusterNotInitialized), vec![]));
        };
        if group == GroupId::Meta {
            return Ok((reject(MetaReject::SeedMetaForbidden), vec![]));
        }
        let vs = voter_set(voters.to_vec());
        if vs.len() != voters.len() {
            return Ok((illegal("duplicate voter"), vec![]));
        }
        if vs.len() != config.r as usize {
            return Ok((illegal("initial voters must number exactly R"), vec![]));
        }
        for &v in voters {
            if !self.is_active(s, v)? {
                return Ok((reject(MetaReject::IneligibleTarget), vec![]));
            }
        }
        match self.placement(s, group)? {
            Some(existing)
                if existing.r#move.is_none() && voter_set(existing.voters.clone()) == vs =>
            {
                Ok((MetaApplyResult::NoOp, vec![]))
            }
            Some(_) => Ok((reject(MetaReject::PlacementConflict), vec![])),
            None => Ok((
                MetaApplyResult::Applied,
                vec![put(
                    keyspace::meta_placement_key(group),
                    &Placement {
                        voters: vs.into_iter().collect(),
                        voters_log_id: log_id,
                        r#move: None,
                    },
                )],
            )),
        }
    }

    fn create_plan(
        &self,
        s: &Storage,
        group: GroupId,
        target_voters: &[NodeId],
        log_id: LogId,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        let Some(config) = self.cluster(s)? else {
            return Ok((reject(MetaReject::ClusterNotInitialized), vec![]));
        };
        let Some(mut placement) = self.placement(s, group)? else {
            return Ok((reject(MetaReject::NoPlacement), vec![]));
        };
        if placement.r#move.is_some() {
            return Ok((reject(MetaReject::PlanExists), vec![]));
        }

        let current = voter_set(placement.voters.clone());
        let target = voter_set(target_voters.to_vec());
        if target.len() != target_voters.len() {
            return Ok((illegal("duplicate voter"), vec![]));
        }
        let added = target.difference(&current).count();
        let removed = current.difference(&target).count();

        match group {
            GroupId::Data(_) => {
                if target.len() != config.r as usize {
                    return Ok((illegal("data target must have exactly R voters"), vec![]));
                }
                if added != 1 || removed != 1 {
                    return Ok((illegal("data move must change exactly one voter"), vec![]));
                }
            }
            GroupId::Meta => {
                if target.len() < MIN_META_VOTERS {
                    return Ok((illegal("meta move must not leave < 3 voters"), vec![]));
                }
                let replacement = added == 1 && removed == 1 && target.len() == current.len();
                let removal = added == 0 && removed == 1 && target.len() + 1 == current.len();
                if !(replacement || removal) {
                    return Ok((
                        illegal("meta move must be single-voter replacement or removal"),
                        vec![],
                    ));
                }
            }
        }

        // The added voter(s) must be eligible; a pure removal adds none.
        for v in target.difference(&current) {
            if !self.is_active(s, *v)? {
                return Ok((reject(MetaReject::IneligibleTarget), vec![]));
            }
        }
        if self.eligible_count(s)? < config.r as usize {
            return Ok((reject(MetaReject::IneligibleTarget), vec![]));
        }

        let mut sorted: Vec<NodeId> = target.into_iter().collect();
        sorted.sort_unstable();
        placement.r#move = Some(MovePlan {
            plan_id: log_id.index,
            target_voters: sorted,
            aborting: false,
        });
        Ok((
            MetaApplyResult::Applied,
            vec![put(keyspace::meta_placement_key(group), &placement)],
        ))
    }

    fn mark_aborting(
        &self,
        s: &Storage,
        group: GroupId,
        plan_id: u64,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        let Some(mut placement) = self.placement(s, group)? else {
            return Ok((reject(MetaReject::NoPlan), vec![]));
        };
        match &mut placement.r#move {
            Some(m) if m.plan_id == plan_id => {
                if m.aborting {
                    return Ok((MetaApplyResult::NoOp, vec![]));
                }
                m.aborting = true;
                Ok((
                    MetaApplyResult::Applied,
                    vec![put(keyspace::meta_placement_key(group), &placement)],
                ))
            }
            Some(_) => Ok((reject(MetaReject::PlanIdMismatch), vec![])),
            None => Ok((reject(MetaReject::NoPlan), vec![])),
        }
    }

    /// Resolve a plan: `FinalizePlan` (aborting = false) commits the target voter
    /// set; `AbortReport` (aborting = true) clears a plan already marked
    /// aborting and rolls back to the original voters (DESIGN §7.5).
    fn finalize_plan(
        &self,
        s: &Storage,
        group: GroupId,
        plan_id: u64,
        observation: &crate::types::DataConfigObservation,
        is_abort: bool,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        let Some(placement) = self.placement(s, group)? else {
            return Ok((reject(MetaReject::NoPlan), vec![]));
        };
        let Some(plan) = placement.r#move.clone() else {
            return Ok((reject(MetaReject::NoPlan), vec![]));
        };
        if plan.plan_id != plan_id {
            return Ok((reject(MetaReject::PlanIdMismatch), vec![]));
        }
        if observation.group != group || observation.plan_id != plan_id {
            return Ok((reject(MetaReject::ObservationMismatch), vec![]));
        }

        let observed = voter_set(observation.voter_set.clone());
        let (new_voters, muts_ok): (Vec<NodeId>, bool) = if is_abort {
            // An abort report resolves only a plan already marked aborting. Its
            // single voter set is either `voters` (the move never completed —
            // clear/roll back) or `target_voters` (the move actually completed
            // before the abort — finalize benignly). DESIGN §7.5.
            if !plan.aborting {
                return Ok((reject(MetaReject::NotAborting), vec![]));
            }
            if observed == voter_set(placement.voters.clone()) {
                (placement.voters.clone(), true)
            } else if observed == voter_set(plan.target_voters.clone()) {
                (plan.target_voters.clone(), true)
            } else {
                (placement.voters.clone(), false)
            }
        } else {
            // A finalize completes a healthy plan; the observed config must
            // equal the planned target.
            if plan.aborting {
                return Ok((reject(MetaReject::PlanAborting), vec![]));
            }
            let matches = observed == voter_set(plan.target_voters.clone());
            (plan.target_voters.clone(), matches)
        };
        if !muts_ok {
            return Ok((reject(MetaReject::ObservationMismatch), vec![]));
        }

        let mut sorted = new_voters;
        sorted.sort_unstable();
        let resolved = Placement {
            voters: sorted,
            voters_log_id: observation.config_log_id,
            r#move: None,
        };
        Ok((
            MetaApplyResult::Applied,
            vec![put(keyspace::meta_placement_key(group), &resolved)],
        ))
    }
}

impl Default for MetaStateMachine {
    fn default() -> Self {
        MetaStateMachine::new()
    }
}

fn put<T: serde::Serialize>(key: Vec<u8>, value: &T) -> StateMutation {
    StateMutation::Put {
        key,
        value: codec::encode(value),
    }
}

fn reject(r: MetaReject) -> MetaApplyResult {
    MetaApplyResult::Rejected(r)
}

fn invalid(msg: &str) -> MetaApplyResult {
    MetaApplyResult::Rejected(MetaReject::InvalidConfig(msg.to_string()))
}

fn illegal(msg: &str) -> MetaApplyResult {
    MetaApplyResult::Rejected(MetaReject::IllegalVoterChange(msg.to_string()))
}
