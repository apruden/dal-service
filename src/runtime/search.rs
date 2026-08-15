//! Node-side search coordination (DESIGN §9.1).
//!
//! A public `SearchOp` addressed to the meta group is answered by whichever
//! node received it. That node fans `SearchPrepare`/`SearchExecute` out to the
//! current leader of every data partition over the peer-control lane, so the
//! two-phase global-scoring protocol never crosses the client surface.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::api::gateway::{PartitionMap, RoutingSource};
use crate::codec;
use crate::runtime::node::MetaHandle;
use crate::runtime::readiness::RuntimeReadiness;
use crate::search::{
    SearchIndexGeneration, SearchIndexRecord, SearchReply, SearchRequest, SearchService,
    ShardExecuteBody, ShardExecuteReply, ShardExecuteRequest, ShardPrepareReply,
    ShardPrepareRequest, ShardPrepared, ShardSearchOutcome,
};
use crate::transport::Transport;
use crate::transport::codec::{Envelope, MsgType};
use crate::transport::dealer::ZmqTransport;
use crate::transport::raft_net::AddrBook;
use crate::transport::raft_wire::{SearchIndexStatusBody, SearchIndexStatusReply, SubmitReply};
use crate::types::{ClusterId, GroupId, NodeId};

use crate::error::{Error, Result};

/// How many times one shard phase re-resolves a leader before giving up. The
/// coordinator's deadline is the real bound; this only stops a redirect loop.
const MAX_ROUNDS: usize = 3;

/// How often a node reconciles its local projections against the catalog.
/// Index lifecycle changes are rare and every step is idempotent, so this only
/// has to be frequent enough that a backfill is noticed promptly.
const SEARCH_RECONCILE_INTERVAL: Duration = Duration::from_millis(500);

/// Where a partition's held session lives, so phase two reaches the same
/// replica — and therefore the same pinned searcher — that phase one used.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionHost {
    Local,
    Remote(NodeId),
}

/// One query's view of the cluster's shards. Created per coordinated search:
/// the session map is scoped to that query and needs no expiry of its own.
pub struct NodeShardSource {
    cluster_id: ClusterId,
    node_id: NodeId,
    partitions: PartitionMap,
    search: Arc<SearchService>,
    routing: Arc<dyn RoutingSource>,
    control: ZmqTransport,
    addrs: AddrBook,
    sessions: Mutex<HashMap<(u16, crate::search::SearchSessionId), SessionHost>>,
}

impl NodeShardSource {
    pub fn new(
        cluster_id: ClusterId,
        node_id: NodeId,
        partitions: PartitionMap,
        search: Arc<SearchService>,
        routing: Arc<dyn RoutingSource>,
        control: ZmqTransport,
        addrs: AddrBook,
    ) -> Self {
        Self {
            cluster_id,
            node_id,
            partitions,
            search,
            routing,
            control,
            addrs,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Replicas that could currently lead this partition, the hinted leader
    /// first. Routing is advisory; the shard's own leader gate is authority.
    async fn candidates(&self, partition: u16, hint: Option<NodeId>) -> Vec<NodeId> {
        let mut candidates = self
            .routing
            .routing(true)
            .await
            .map(|routing| routing.candidates(partition))
            .unwrap_or_default();
        if let Some(hint) = hint {
            candidates.retain(|node| *node != hint);
            candidates.insert(0, hint);
        }
        candidates
    }

    async fn call_shard(
        &self,
        node: NodeId,
        partition: u16,
        msg_type: MsgType,
        payload: Vec<u8>,
    ) -> Option<Envelope> {
        let addr = self.addrs.control(node)?;
        let request = Envelope::new(
            self.cluster_id,
            msg_type,
            GroupId::Data(partition),
            0,
            payload,
        );
        let reply = self.control.call(&addr, request).await.ok()?;
        (reply.cluster_id == self.cluster_id && reply.msg_type == msg_type).then_some(reply)
    }

    async fn prepare_shard(
        &self,
        partition: u16,
        request: &ShardPrepareRequest,
    ) -> Result<ShardPrepared> {
        let payload = codec::encode(request);
        let mut hint = None;
        for _round in 0..MAX_ROUNDS {
            // A partition this node hosts is prepared in-process: it skips a
            // network hop and, more importantly, avoids the node calling its
            // own router while occupying one of its handler slots.
            let hosted = self.partitions.read().unwrap().get(&partition).cloned();
            if let Some(node) = hosted {
                match self.search.shard_prepare(&node, request).await {
                    Ok(ShardPrepareReply::Prepared(prepared)) => {
                        self.remember(partition, prepared.session_id, SessionHost::Local);
                        return Ok(prepared);
                    }
                    Ok(ShardPrepareReply::NotLeader { leader }) => {
                        hint = leader.filter(|leader| *leader != self.node_id).or(hint);
                    }
                    Ok(ShardPrepareReply::Error(error)) => return Err(Error::Search(error)),
                    Err(error) => return Err(error),
                }
            }

            let mut redirected = false;
            for candidate in self.candidates(partition, hint).await {
                if candidate == self.node_id {
                    continue;
                }
                let Some(reply) = self
                    .call_shard(
                        candidate,
                        partition,
                        MsgType::SearchPrepare,
                        payload.clone(),
                    )
                    .await
                else {
                    continue;
                };
                match codec::decode::<ShardPrepareReply>(&reply.payload) {
                    Ok(ShardPrepareReply::Prepared(prepared)) => {
                        self.remember(
                            partition,
                            prepared.session_id,
                            SessionHost::Remote(candidate),
                        );
                        return Ok(prepared);
                    }
                    Ok(ShardPrepareReply::NotLeader { leader }) => {
                        if let Some(leader) = leader.filter(|leader| *leader != candidate) {
                            hint = Some(leader);
                            redirected = true;
                            break;
                        }
                    }
                    Ok(ShardPrepareReply::Error(error)) => return Err(Error::Search(error)),
                    Err(_) => continue,
                }
            }
            if !redirected {
                break;
            }
        }
        Err(Error::Search(format!(
            "no replica prepared a search on partition {partition}"
        )))
    }

    fn remember(
        &self,
        partition: u16,
        session_id: crate::search::SearchSessionId,
        host: SessionHost,
    ) {
        self.sessions
            .lock()
            .unwrap()
            .insert((partition, session_id), host);
    }

    async fn execute_shard(
        &self,
        partition: u16,
        request: &ShardExecuteRequest,
    ) -> Result<SearchReply> {
        let host = self
            .sessions
            .lock()
            .unwrap()
            .get(&(partition, request.session_id))
            .copied()
            .ok_or_else(|| {
                Error::Search(format!("no prepared session for partition {partition}"))
            })?;
        let reply = match host {
            SessionHost::Local => self.search.shard_execute(request).await,
            SessionHost::Remote(node) => {
                let payload = codec::encode(&ShardExecuteBody::Execute(request.clone()));
                let reply = self
                    .call_shard(node, partition, MsgType::SearchExecute, payload)
                    .await
                    .ok_or_else(|| {
                        Error::Search(format!(
                            "partition {partition} did not answer its prepared session"
                        ))
                    })?;
                codec::decode::<ShardExecuteReply>(&reply.payload)
                    .map_err(|error| Error::Search(format!("malformed shard reply: {error}")))?
            }
        };
        match reply {
            ShardExecuteReply::Result(reply) => Ok(reply),
            // Losing a session mid-query is a coordinator-level retry, not a
            // partial result: the shard's statistics are already summed into
            // everyone else's scores.
            ShardExecuteReply::UnknownSession => Err(Error::Search(format!(
                "partition {partition} expired its held search session"
            ))),
            ShardExecuteReply::Error(error) => Err(Error::Search(error)),
        }
    }

    /// Best effort: a release that does not land leaves the shard to expire
    /// the session on its own.
    async fn release_shard(&self, partition: u16, session_id: crate::search::SearchSessionId) {
        let host = self
            .sessions
            .lock()
            .unwrap()
            .get(&(partition, session_id))
            .copied();
        match host {
            Some(SessionHost::Local) => self.search.release_session(session_id),
            Some(SessionHost::Remote(node)) => {
                let payload = codec::encode(&ShardExecuteBody::Release { session_id });
                let _ = self
                    .call_shard(node, partition, MsgType::SearchExecute, payload)
                    .await;
            }
            None => {}
        }
    }

    async fn search_shard(&self, partition: u16, request: &SearchRequest) -> Result<SearchReply> {
        let payload = codec::encode(request);
        let mut hint = None;
        for _round in 0..MAX_ROUNDS {
            let hosted = self.partitions.read().unwrap().get(&partition).cloned();
            if let Some(node) = hosted {
                match self.search.shard_search(&node, request).await {
                    Ok(ShardSearchOutcome::Reply(reply)) => return Ok(reply),
                    Ok(ShardSearchOutcome::NotLeader { leader }) => {
                        hint = leader.filter(|leader| *leader != self.node_id).or(hint);
                    }
                    Err(error) => return Err(error),
                }
            }

            let mut redirected = false;
            for candidate in self.candidates(partition, hint).await {
                if candidate == self.node_id {
                    continue;
                }
                let Some(reply) = self
                    .call_shard(candidate, partition, MsgType::SearchOp, payload.clone())
                    .await
                else {
                    continue;
                };
                match codec::decode::<crate::api::ops::SearchShardReply>(&reply.payload) {
                    Ok(crate::api::ops::SearchShardReply::Result(reply)) => return Ok(reply),
                    Ok(crate::api::ops::SearchShardReply::Redirect(redirect)) => {
                        if let Some(leader) = redirect.leader.filter(|leader| *leader != candidate)
                        {
                            hint = Some(leader);
                            redirected = true;
                            break;
                        }
                    }
                    Ok(crate::api::ops::SearchShardReply::Refused(error))
                    | Ok(crate::api::ops::SearchShardReply::Error(error)) => {
                        return Err(Error::Search(error));
                    }
                    Err(_) => continue,
                }
            }
            if !redirected {
                break;
            }
        }
        Err(Error::Search(format!(
            "no replica served a search on partition {partition}"
        )))
    }
}

/// Drives the search-index lifecycle on one node (design §5.1, §5.2).
///
/// Every node builds the generations its hosted partitions need and reports
/// readiness; the meta leader turns a fully-reported `building` generation
/// into the `active` one and retires its predecessor. Nothing here decides
/// correctness — the meta state machine re-validates every observation — so a
/// missed tick only delays activation.
pub struct SearchDriver {
    node_id: NodeId,
    cluster_id: ClusterId,
    meta: MetaHandle,
    partitions: PartitionMap,
    search: Arc<SearchService>,
    catalog: Arc<dyn crate::api::gateway::SearchCatalogSource>,
    control: ZmqTransport,
    addrs: AddrBook,
    identity_gate: crate::types::ProcessIdentityGate,
    readiness: RuntimeReadiness,
}

impl SearchDriver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        cluster_id: ClusterId,
        meta: MetaHandle,
        partitions: PartitionMap,
        search: Arc<SearchService>,
        catalog: Arc<dyn crate::api::gateway::SearchCatalogSource>,
        control: ZmqTransport,
        addrs: AddrBook,
        identity_gate: crate::types::ProcessIdentityGate,
        readiness: RuntimeReadiness,
    ) -> Self {
        Self {
            node_id,
            cluster_id,
            meta,
            partitions,
            search,
            catalog,
            control,
            addrs,
            identity_gate,
            readiness,
        }
    }

    /// Snapshot the meta handle; the guard is dropped before any `.await`.
    fn meta(&self) -> Option<Arc<crate::meta::node::MetaNode>> {
        self.meta.read().unwrap().clone()
    }

    pub async fn run(self) {
        loop {
            if !self.identity_gate.is_open() {
                return;
            }
            if self.readiness.is_ready() {
                self.reconcile().await;
            }
            tokio::time::sleep(SEARCH_RECONCILE_INTERVAL).await;
        }
    }

    async fn reconcile(&self) {
        // Only act on a catalog we actually read. Treating an unavailable
        // control plane as "no indexes exist" would drop every local
        // projection and force a cluster-wide rebuild.
        let Ok(catalog) = self.catalog.catalog(false).await else {
            return;
        };
        let hosted: Vec<u16> = self.partitions.read().unwrap().keys().copied().collect();
        for partition in &hosted {
            for record in &catalog {
                self.reconcile_partition_index(*partition, record).await;
            }
        }
        self.drop_unlisted(&catalog, &hosted).await;
        self.advance_local_projections().await;
        // After advancing, whatever journal remains is genuinely blocked by a
        // lagging consumer rather than by scheduling.
        for partition in &hosted {
            match self.search.enforce_outbox_bounds(*partition) {
                Ok(Some((name, generation))) => {
                    tracing::warn!(
                        partition,
                        index = name,
                        generation,
                        "search index released for rebuild to bound outbox growth"
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(partition, %error, "failed to enforce search outbox bounds")
                }
            }
        }
        self.drive_activation(&catalog).await;
    }

    /// Build and report the generations one hosted partition owes this index.
    async fn reconcile_partition_index(&self, partition: u16, record: &SearchIndexRecord) {
        if let Some(active) = &record.active
            && !self.search.is_loaded(partition, &record.name, active.id)
            && let Err(error) = self
                .search
                .install_generation(partition, &record.name, active.clone(), true)
                .await
        {
            tracing::warn!(partition, index = record.name, %error, "failed to install active search generation");
        }
        // Re-mark the active generation each tick: a generation that was
        // installed while still building is loaded but not yet serving, and
        // activation is what flips it.
        if let Some(active) = &record.active
            && self.search.is_loaded(partition, &record.name, active.id)
        {
            self.search
                .activate(partition, &record.name, active.id)
                .await;
        }

        let Some(building) = &record.building else {
            return;
        };
        if !self.search.is_loaded(partition, &record.name, building.id)
            && let Err(error) = self
                .search
                .install_generation(partition, &record.name, building.clone(), false)
                .await
        {
            tracing::warn!(partition, index = record.name, %error, "failed to build search generation");
            return;
        }
        self.report_ready(partition, &record.name, building).await;
    }

    /// Report this replica's built generation to the meta group. The meta
    /// state machine independently checks that the reporter is a current voter
    /// at the stated placement generation, so an observation built from a
    /// stale placement read is rejected rather than trusted.
    async fn report_ready(&self, partition: u16, name: &str, building: &SearchIndexGeneration) {
        let group = GroupId::Data(partition);
        let Some(placement) = self.read_placement(group).await else {
            return;
        };
        if placement.r#move.is_some() || !placement.voters.contains(&self.node_id) {
            return;
        }
        let observation = match self
            .search
            .readiness_observation(
                partition,
                name,
                building.id,
                self.node_id,
                placement.voters_log_id,
            )
            .await
        {
            Ok(observation) => observation,
            // Not built yet is the normal case while a backfill runs.
            Err(_) => return,
        };
        self.submit_ready(name, observation).await;
    }

    async fn submit_ready(&self, name: &str, observation: crate::search::SearchIndexReady) {
        let leading = self
            .meta()
            .filter(|meta| meta.current_leader() == Some(self.node_id));
        if let Some(meta) = leading {
            let _ = meta
                .propose(crate::types::MetaCommand::ReportSearchIndexReady {
                    name: name.to_string(),
                    observation,
                })
                .await;
            return;
        }
        let body = SearchIndexStatusBody::Report {
            name: name.to_string(),
            observation,
        };
        for addr in self.addrs.control_addrs() {
            let envelope = Envelope::new(
                self.cluster_id,
                MsgType::SearchIndexStatus,
                GroupId::Meta,
                0,
                codec::encode(&body),
            );
            if let Ok(reply) = self.control.call(&addr, envelope).await
                && let Ok(SearchIndexStatusReply::Submitted(SubmitReply::Outcome(_))) =
                    codec::decode::<SearchIndexStatusReply>(&reply.payload)
            {
                return;
            }
        }
    }

    /// Drop generations the catalog no longer lists. Reading the catalog
    /// rather than the `retiring` list means a generation is also reclaimed
    /// when the whole index record disappears.
    async fn drop_unlisted(&self, catalog: &[SearchIndexRecord], hosted: &[u16]) {
        for (partition, name, generation) in self.search.loaded_generations() {
            if !hosted.contains(&partition) {
                continue;
            }
            let listed = catalog.iter().any(|record| {
                record.name == name
                    && [record.active.as_ref(), record.building.as_ref()]
                        .into_iter()
                        .flatten()
                        .any(|candidate| candidate.id == generation)
            });
            if listed {
                continue;
            }
            if let Err(error) = self
                .search
                .remove_generation(partition, &name, generation)
                .await
            {
                tracing::warn!(partition, index = name, generation, %error, "failed to retire search generation");
            }
        }
    }

    /// Keep every loaded projection moving even with no search traffic, so a
    /// quiet partition cannot accumulate an unbounded dirty-key outbox.
    async fn advance_local_projections(&self) {
        for (partition, name, generation) in self.search.loaded_generations() {
            if let Err(error) = self
                .search
                .catch_up_generation(partition, &name, generation)
                .await
            {
                tracing::debug!(partition, index = name, generation, %error, "search catch-up deferred");
            }
        }
    }

    /// Meta-leader role: promote a fully reported generation, then shrink the
    /// catalog's retiring list. Both commands are validated inside meta apply,
    /// so proposing them optimistically each tick is safe.
    async fn drive_activation(&self, catalog: &[SearchIndexRecord]) {
        let Some(meta) = self.meta() else {
            return;
        };
        if meta.current_leader() != Some(self.node_id) {
            return;
        }
        for record in catalog {
            if let Some(building) = &record.building {
                let _ = meta
                    .propose(crate::types::MetaCommand::ActivateSearchIndexGeneration {
                        name: record.name.clone(),
                        generation: building.id,
                    })
                    .await;
            }
            for retiring in &record.retiring {
                let _ = meta
                    .propose(crate::types::MetaCommand::FinalizeSearchIndexDrop {
                        name: record.name.clone(),
                        generation: *retiring,
                    })
                    .await;
            }
        }
    }

    async fn read_placement(&self, group: GroupId) -> Option<crate::types::Placement> {
        if let Some(meta) = self.meta()
            && let Ok(crate::meta::node::MetaRead::Value(placement)) =
                meta.read_placement(group).await
        {
            return placement;
        }
        let body = crate::transport::raft_wire::PlacementQueryBody { group };
        for addr in self.addrs.control_addrs() {
            let envelope = Envelope::new(
                self.cluster_id,
                MsgType::PlacementQuery,
                GroupId::Meta,
                0,
                codec::encode(&body),
            );
            if let Ok(reply) = self.control.call(&addr, envelope).await
                && let Ok(crate::transport::raft_wire::PlacementQueryReply {
                    placement: Some(placement),
                }) = codec::decode(&reply.payload)
            {
                return Some(placement);
            }
        }
        None
    }
}

/// Long-lived handles the gateway uses to open a per-query fan-out.
pub struct LiveShardSource {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub partitions: PartitionMap,
    pub search: Arc<SearchService>,
    pub routing: Arc<dyn RoutingSource>,
    pub control: ZmqTransport,
    pub addrs: AddrBook,
}

impl crate::api::gateway::ShardSourceFactory for LiveShardSource {
    fn open(&self) -> Box<dyn crate::search::ShardSearchSource> {
        Box::new(NodeShardSource::new(
            self.cluster_id,
            self.node_id,
            self.partitions.clone(),
            self.search.clone(),
            self.routing.clone(),
            self.control.clone(),
            self.addrs.clone(),
        ))
    }
}

impl crate::search::ShardSearchSource for NodeShardSource {
    fn search(
        &self,
        partition: u16,
        request: SearchRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SearchReply>> + Send + '_>> {
        Box::pin(async move { self.search_shard(partition, &request).await })
    }

    fn prepare(
        &self,
        partition: u16,
        request: ShardPrepareRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ShardPrepared>> + Send + '_>>
    {
        Box::pin(async move { self.prepare_shard(partition, &request).await })
    }

    fn execute(
        &self,
        partition: u16,
        request: ShardExecuteRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<SearchReply>> + Send + '_>> {
        Box::pin(async move { self.execute_shard(partition, &request).await })
    }

    fn release(
        &self,
        partition: u16,
        session_id: crate::search::SearchSessionId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move { self.release_shard(partition, session_id).await })
    }
}
