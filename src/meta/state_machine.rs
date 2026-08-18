//! The meta group's state machine: cluster identity, the node directory, and
//! the placement map (DESIGN §3.1, §5.2). Every rule lives inside apply, so the
//! decision is a deterministic function of committed input and never depends on
//! who proposed it (IMPLEMENTATION §M5, ground rule 1).
//!
//! Like the data state machine (M2) this is split into a pure
//! [`MetaStateMachine::evaluate`] (decision + mutations, no write) and a thin
//! [`MetaStateMachine::apply`] that commits them atomically with `last_applied`.
//! The Raft wrapper reuses `evaluate` so it can fold the applied `LogId` into
//! the same batch.

use serde::{Deserialize, Serialize};

use crate::codec;
use crate::config::{MIN_META_VOTERS, MIN_REPLICATION, validate_routing_shape};
use crate::error::Result;
use crate::keyspace;
use crate::search::{
    SearchIndexDefinition, SearchIndexGeneration, SearchIndexGenerationId, SearchIndexReady,
    SearchIndexRecord, validate_index_name,
};
use crate::storage::{StateMutation, Storage};
use crate::types::{
    ClusterConfig, GroupId, LogId, MAX_CLUSTER_NODES, MAX_ENDPOINT_BYTES, MetaCommand, MovePlan,
    NodeDirectoryEntry, NodeId, NodeState, PROTOCOL_VERSION, Placement, voter_set,
};

/// Engine identity implied by the legacy search commands that predate an
/// explicit revision field. Never replace this with the local binary's current
/// revision: old log replay must remain deterministic after engine upgrades.
const LEGACY_SEARCH_ENGINE_REVISION: u32 = 1;

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
    SearchIndexExists,
    SearchIndexNotFound,
    SearchGenerationExists,
    SearchGenerationMismatch,
    SearchGenerationNotReady,
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
            MetaCommand::CreateSearchIndex { name, definition } => self.create_search_index(
                s,
                name,
                definition,
                LEGACY_SEARCH_ENGINE_REVISION,
                log_id,
                false,
            ),
            MetaCommand::CreateSearchIndexGeneration { name, definition } => self
                .create_search_index(
                    s,
                    name,
                    definition,
                    LEGACY_SEARCH_ENGINE_REVISION,
                    log_id,
                    true,
                ),
            MetaCommand::ReportSearchIndexReady { name, observation } => {
                self.report_search_ready(s, name, observation)
            }
            MetaCommand::ActivateSearchIndexGeneration { name, generation } => {
                self.activate_search_generation(s, name, *generation)
            }
            MetaCommand::DropSearchIndex { name } => self.drop_search_index(s, name),
            MetaCommand::FinalizeSearchIndexDrop { name, generation } => {
                self.finalize_search_drop(s, name, *generation)
            }
            MetaCommand::CreateSearchIndexWithRevision {
                name,
                definition,
                engine_revision,
            } => self.create_search_index(s, name, definition, *engine_revision, log_id, false),
            MetaCommand::CreateSearchIndexGenerationWithRevision {
                name,
                definition,
                engine_revision,
            } => self.create_search_index(s, name, definition, *engine_revision, log_id, true),
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
            let existing_meta = self
                .placement(s, GroupId::Meta)?
                .map(|p| voter_set(p.voters));
            let incoming_meta = Some(voter_set(meta_voters.to_vec()));
            if &existing == config && existing_meta == incoming_meta {
                return Ok((MetaApplyResult::NoOp, vec![]));
            }
            return Ok((reject(MetaReject::ClusterConflict), vec![]));
        }

        let Some(identity) = s.identity()? else {
            return Ok((
                invalid("cluster init requires a bound storage identity"),
                vec![],
            ));
        };
        if config.cluster_id != identity.cluster_id {
            return Ok((
                invalid("cluster_id differs from the storage identity"),
                vec![],
            ));
        }
        if config.protocol_version != PROTOCOL_VERSION {
            return Ok((invalid("unsupported protocol version"), vec![]));
        }

        if config.r < MIN_REPLICATION {
            return Ok((invalid("R below minimum"), vec![]));
        }
        if config.p == 0 {
            return Ok((invalid("P must be non-zero"), vec![]));
        }
        if let Err(error) = validate_routing_shape(config.p, config.r) {
            return Ok((invalid(&error.to_string()), vec![]));
        }
        let mv = voter_set(meta_voters.to_vec());
        if mv.len() != meta_voters.len() {
            return Ok((invalid("duplicate meta voter"), vec![]));
        }
        if mv.len() < MIN_META_VOTERS || mv.len().is_multiple_of(2) {
            return Ok((invalid("meta voters must be an odd set of >= 3"), vec![]));
        }
        if mv.len() > MAX_CLUSTER_NODES {
            return Ok((
                invalid("meta voter set exceeds the cluster node limit"),
                vec![],
            ));
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
        if node_id == 0 {
            return Ok((invalid("node_id must be non-zero"), vec![]));
        }
        if control_addr.is_empty() || bulk_addr.is_empty() {
            return Ok((invalid("node addresses must be non-empty"), vec![]));
        }
        if control_addr.len() > MAX_ENDPOINT_BYTES || bulk_addr.len() > MAX_ENDPOINT_BYTES {
            return Ok((
                invalid(&format!(
                    "node addresses must be <= {MAX_ENDPOINT_BYTES} bytes"
                )),
                vec![],
            ));
        }
        if control_addr == bulk_addr {
            return Ok((invalid("control_addr and bulk_addr must differ"), vec![]));
        }
        // Endpoint ownership is part of the replicated identity fence. Reject
        // both same-lane and cross-lane collisions so two node ids can never be
        // routed to the same socket.
        let directory = self.directory(s)?;
        if directory.iter().any(|other| {
            other.node_id != node_id
                && (other.control_addr == control_addr
                    || other.bulk_addr == control_addr
                    || other.control_addr == bulk_addr
                    || other.bulk_addr == bulk_addr)
        }) {
            return Ok((
                invalid("node addresses are already registered to another node"),
                vec![],
            ));
        }
        if directory.len() >= MAX_CLUSTER_NODES
            && !directory.iter().any(|entry| entry.node_id == node_id)
        {
            return Ok((
                invalid(&format!(
                    "cluster node count must be <= {MAX_CLUSTER_NODES}"
                )),
                vec![],
            ));
        }
        let entry = match self.node(s, node_id)? {
            Some(mut e) if e.control_addr == control_addr && e.bulk_addr == bulk_addr => {
                // A committed Down state is sticky against heartbeats, but an
                // explicit RegisterNode is the operator-authorized rejoin. Even
                // with unchanged addresses it must advance the incarnation so
                // stale liveness evidence cannot cross the restart boundary.
                if e.state != NodeState::Down {
                    return Ok((MetaApplyResult::NoOp, vec![]));
                }
                let Some(incarnation) = e.incarnation.checked_add(1) else {
                    return Ok((invalid("node incarnation exhausted"), vec![]));
                };
                e.state = NodeState::Active;
                e.incarnation = incarnation;
                e
            }
            // Re-register with a changed address is a rejoin: bump incarnation so
            // stale heartbeats for the old incarnation cannot reactivate it.
            Some(e) => {
                let Some(incarnation) = e.incarnation.checked_add(1) else {
                    return Ok((invalid("node incarnation exhausted"), vec![]));
                };
                NodeDirectoryEntry {
                    node_id,
                    control_addr: control_addr.to_string(),
                    bulk_addr: bulk_addr.to_string(),
                    state: NodeState::Active,
                    incarnation,
                }
            }
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
        // A node declared Down must not be revived by ordinary liveness
        // evidence. Leaving Down is an explicit rejoin and therefore needs a
        // strictly newer incarnation for *any* target state — guarding only
        // Down -> Active would let a replayed/stale command revive the node
        // through a Down -> Suspect -> Active two-step.
        if entry.state == NodeState::Down
            && state != NodeState::Down
            && incarnation == entry.incarnation
        {
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
        let GroupId::Data(partition) = group else {
            unreachable!("meta group rejected above")
        };
        if partition >= config.p {
            return Ok((illegal("partition is outside the configured range"), vec![]));
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
                if self.eligible_count(s)? < config.r as usize {
                    return Ok((reject(MetaReject::IneligibleTarget), vec![]));
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

    fn search_index(&self, s: &Storage, name: &str) -> Result<Option<SearchIndexRecord>> {
        let record: Option<SearchIndexRecord> =
            s.get_state_record(GroupId::Meta, &keyspace::meta_search_index_key(name))?;
        if let Some(record) = &record {
            record.validate()?;
        }
        Ok(record)
    }

    fn create_search_index(
        &self,
        s: &Storage,
        name: &str,
        definition: &SearchIndexDefinition,
        engine_revision: u32,
        log_id: LogId,
        update: bool,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        if self.cluster(s)?.is_none() {
            return Ok((reject(MetaReject::ClusterNotInitialized), vec![]));
        }
        if let Err(error) = validate_index_name(name) {
            return Ok((invalid_owned(error.to_string()), vec![]));
        }
        let generation = match SearchIndexGeneration::with_revision(
            log_id.index,
            engine_revision,
            definition.clone(),
        ) {
            Ok(generation) => generation,
            Err(error) => return Ok((invalid_owned(error.to_string()), vec![])),
        };
        let existing = self.search_index(s, name)?;
        if existing.is_none() && !update {
            let prefix = keyspace::meta_search_index_prefix();
            let count = s
                .scan_state(GroupId::Meta)?
                .into_iter()
                .filter(|(key, _)| key.starts_with(&prefix))
                .count();
            if count >= crate::search::SEARCH_MAX_INDEXES {
                return Ok((
                    invalid("search catalog has reached its maximum index count"),
                    vec![],
                ));
            }
        }
        let mut record = match (existing, update) {
            (None, false) => SearchIndexRecord {
                name: name.to_string(),
                active: None,
                building: None,
                retiring: Vec::new(),
            },
            (None, true) => return Ok((reject(MetaReject::SearchIndexNotFound), vec![])),
            (Some(record), false) => {
                if record.active.is_none()
                    && record.building.as_ref().is_some_and(|building| {
                        building.definition_hash == generation.definition_hash
                            && building.engine_revision == generation.engine_revision
                    })
                {
                    return Ok((MetaApplyResult::NoOp, vec![]));
                }
                return Ok((reject(MetaReject::SearchIndexExists), vec![]));
            }
            (Some(record), true) => record,
        };
        if record.building.is_some() {
            if record.building.as_ref().is_some_and(|building| {
                building.definition_hash == generation.definition_hash
                    && building.engine_revision == generation.engine_revision
            }) {
                return Ok((MetaApplyResult::NoOp, vec![]));
            }
            return Ok((reject(MetaReject::SearchGenerationExists), vec![]));
        }
        if update && record.active.is_none() {
            return Ok((reject(MetaReject::SearchIndexNotFound), vec![]));
        }
        if update && record.retiring.len() >= crate::search::SEARCH_MAX_RETIRING_GENERATIONS {
            return Ok((
                invalid("too many retiring search generations; finalize one before updating"),
                vec![],
            ));
        }
        record.building = Some(generation);
        if let Err(error) = record.validate() {
            return Ok((invalid_owned(error.to_string()), vec![]));
        }
        Ok((
            MetaApplyResult::Applied,
            vec![put(keyspace::meta_search_index_key(name), &record)],
        ))
    }

    fn report_search_ready(
        &self,
        s: &Storage,
        name: &str,
        observation: &SearchIndexReady,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        let Some(record) = self.search_index(s, name)? else {
            return Ok((reject(MetaReject::SearchIndexNotFound), vec![]));
        };
        let Some(building) = record.building.as_ref() else {
            return Ok((reject(MetaReject::SearchGenerationMismatch), vec![]));
        };
        if observation.generation != building.id
            || observation.definition_hash != building.definition_hash
            || observation.engine_revision != building.engine_revision
        {
            return Ok((reject(MetaReject::SearchGenerationMismatch), vec![]));
        }
        let GroupId::Data(partition) = observation.group else {
            return Ok((reject(MetaReject::ObservationMismatch), vec![]));
        };
        let Some(config) = self.cluster(s)? else {
            return Ok((reject(MetaReject::ClusterNotInitialized), vec![]));
        };
        if partition >= config.p || observation.projection_epoch == 0 {
            return Ok((reject(MetaReject::ObservationMismatch), vec![]));
        }
        let Some(placement) = self.placement(s, observation.group)? else {
            return Ok((reject(MetaReject::NoPlacement), vec![]));
        };
        if placement.r#move.is_some()
            || placement.voters_log_id != observation.voters_log_id
            || !placement.voters.contains(&observation.node_id)
        {
            return Ok((reject(MetaReject::ObservationMismatch), vec![]));
        }
        let ready_key = keyspace::meta_search_ready_key(
            name,
            observation.generation,
            observation.group,
            observation.node_id,
        );
        if let Some(previous) = s.get_state_record::<SearchIndexReady>(GroupId::Meta, &ready_key)? {
            if &previous == observation {
                return Ok((MetaApplyResult::NoOp, vec![]));
            }
            if observation.projection_epoch < previous.projection_epoch
                || (observation.projection_epoch == previous.projection_epoch
                    && observation.source_log_id.index < previous.source_log_id.index)
            {
                return Ok((reject(MetaReject::ObservationMismatch), vec![]));
            }
        }
        Ok((MetaApplyResult::Applied, vec![put(ready_key, observation)]))
    }

    fn activate_search_generation(
        &self,
        s: &Storage,
        name: &str,
        generation: SearchIndexGenerationId,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        let Some(mut record) = self.search_index(s, name)? else {
            return Ok((reject(MetaReject::SearchIndexNotFound), vec![]));
        };
        if record.active.as_ref().map(|active| active.id) == Some(generation)
            && record.building.is_none()
        {
            return Ok((MetaApplyResult::NoOp, vec![]));
        }
        let Some(building) = record.building.as_ref() else {
            return Ok((reject(MetaReject::SearchGenerationMismatch), vec![]));
        };
        if building.id != generation {
            return Ok((reject(MetaReject::SearchGenerationMismatch), vec![]));
        }
        let Some(config) = self.cluster(s)? else {
            return Ok((reject(MetaReject::ClusterNotInitialized), vec![]));
        };
        let ready_prefix = keyspace::meta_search_ready_prefix(name, generation);
        let readiness: Vec<(Vec<u8>, SearchIndexReady)> = s
            .scan_state(GroupId::Meta)?
            .into_iter()
            .filter(|(key, _)| key.starts_with(&ready_prefix))
            .map(|(key, value)| codec::decode(&value).map(|ready| (key, ready)))
            .collect::<Result<_>>()?;
        for partition in 0..config.p {
            let group = GroupId::Data(partition);
            let Some(placement) = self.placement(s, group)? else {
                return Ok((reject(MetaReject::SearchGenerationNotReady), vec![]));
            };
            if placement.r#move.is_some() {
                return Ok((reject(MetaReject::SearchGenerationNotReady), vec![]));
            }
            for node_id in &placement.voters {
                if !readiness.iter().any(|(_, ready)| {
                    ready.generation == generation
                        && ready.group == group
                        && ready.node_id == *node_id
                        && ready.voters_log_id == placement.voters_log_id
                        && ready.definition_hash == building.definition_hash
                        && ready.engine_revision == building.engine_revision
                }) {
                    return Ok((reject(MetaReject::SearchGenerationNotReady), vec![]));
                }
            }
        }
        let activated = record.building.take().expect("checked building");
        if record.active.as_ref().is_some_and(|previous| {
            !record.retiring.contains(&previous.id)
                && record.retiring.len() >= crate::search::SEARCH_MAX_RETIRING_GENERATIONS
        }) {
            return Ok((
                invalid("too many retiring search generations; finalize one before activation"),
                vec![],
            ));
        }
        if let Some(previous) = record.active.replace(activated)
            && !record.retiring.contains(&previous.id)
        {
            record.retiring.push(previous.id);
        }
        if let Err(error) = record.validate() {
            return Ok((invalid_owned(error.to_string()), vec![]));
        }
        let mut mutations = vec![put(keyspace::meta_search_index_key(name), &record)];
        mutations.extend(
            readiness
                .into_iter()
                .map(|(key, _)| StateMutation::Delete { key }),
        );
        Ok((MetaApplyResult::Applied, mutations))
    }

    fn drop_search_index(
        &self,
        s: &Storage,
        name: &str,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        let Some(mut record) = self.search_index(s, name)? else {
            return Ok((MetaApplyResult::NoOp, vec![]));
        };
        if record.active.is_none() && record.building.is_none() {
            return Ok((MetaApplyResult::NoOp, vec![]));
        }
        let building = record.building.take();
        let ready_prefix = building
            .as_ref()
            .map(|generation| keyspace::meta_search_ready_prefix(name, generation.id));
        let retiring = [record.active.take(), building]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let additions = retiring
            .iter()
            .filter(|generation| !record.retiring.contains(&generation.id))
            .count();
        if record.retiring.len().saturating_add(additions)
            > crate::search::SEARCH_MAX_RETIRING_GENERATIONS
        {
            return Ok((
                invalid("too many retiring search generations; finalize one before dropping"),
                vec![],
            ));
        }
        for generation in retiring {
            if !record.retiring.contains(&generation.id) {
                record.retiring.push(generation.id);
            }
        }
        if let Err(error) = record.validate() {
            return Ok((invalid_owned(error.to_string()), vec![]));
        }
        let mut mutations = vec![put(keyspace::meta_search_index_key(name), &record)];
        if let Some(prefix) = ready_prefix {
            mutations.extend(
                s.scan_state(GroupId::Meta)?
                    .into_iter()
                    .filter(|(key, _)| key.starts_with(&prefix))
                    .map(|(key, _)| StateMutation::Delete { key }),
            );
        }
        Ok((MetaApplyResult::Applied, mutations))
    }

    fn finalize_search_drop(
        &self,
        s: &Storage,
        name: &str,
        generation: SearchIndexGenerationId,
    ) -> Result<(MetaApplyResult, Vec<StateMutation>)> {
        let Some(mut record) = self.search_index(s, name)? else {
            return Ok((MetaApplyResult::NoOp, vec![]));
        };
        let before = record.retiring.len();
        record.retiring.retain(|id| *id != generation);
        if record.retiring.len() == before {
            return Ok((MetaApplyResult::NoOp, vec![]));
        }
        let mutation =
            if record.active.is_none() && record.building.is_none() && record.retiring.is_empty() {
                StateMutation::Delete {
                    key: keyspace::meta_search_index_key(name),
                }
            } else {
                put(keyspace::meta_search_index_key(name), &record)
            };
        Ok((MetaApplyResult::Applied, vec![mutation]))
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

fn invalid_owned(msg: String) -> MetaApplyResult {
    MetaApplyResult::Rejected(MetaReject::InvalidConfig(msg))
}

fn illegal(msg: &str) -> MetaApplyResult {
    MetaApplyResult::Rejected(MetaReject::IllegalVoterChange(msg.to_string()))
}
