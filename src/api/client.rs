//! Client library: routing, route cache, redirects, and idempotent retry
//! (DESIGN §8.1, §8.2, §8.4).
//!
//! The client learns `P`, the node directory, and placement from any reachable
//! meta replica or node (a follower/cached read — routing is advisory, the
//! serving gate is authority). It caches a `leader_hint` per partition, tries it
//! first, and on timeout or redirect walks the candidate voter set. Every
//! mutation reuses a fixed `(client_id, partition, sequence)` across retries, so
//! a retry across a leader change applies exactly once (asserted by the M2
//! sequence records).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Mutex as AsyncMutex;

use crate::api::ops::{
    ClientReply, ClientRequest, RoutingInfo, SearchCatalogQuery, SearchCatalogReply,
    SearchShardReply, WriteReply,
};
use crate::codec;
use crate::error::{Error, Result};
use crate::perf::WriteStage;
use crate::transport::Transport;
use crate::transport::codec::{Envelope, MsgType};
use crate::types::{
    ClientId, ClusterId, Consistency, DataOp, DataRequest, GroupId, HashSpec, IfVersion, LogId,
    NodeId, Sequence, Version,
};

/// How many refresh-and-retry rounds a single operation will attempt before
/// giving up. Bounded so a persistently unreachable target fails rather than
/// spins.
const MAX_ROUNDS: usize = 16;

#[derive(Default)]
struct Cache {
    routing: Option<RoutingInfo>,
    /// Best-known leader per partition, tried first (DESIGN §8.1).
    leader_hint: HashMap<u16, NodeId>,
    /// Candidates learned from redirects newer than the cached routing snapshot.
    redirect_candidates: HashMap<u16, RedirectHints>,
    /// Next unused sequence per partition for this client (DESIGN §8.4).
    /// Sequence 0 is never decided, so streams start at 1.
    next_seq: HashMap<u16, Sequence>,
}

/// Candidates a redirect advertised, tagged with the placement generation they
/// were learned against so a later snapshot can retire them.
#[derive(Default)]
struct RedirectHints {
    /// `voters_log_id` of the cached placement when the redirect arrived, or
    /// `None` if the client had no placement for the partition yet.
    learned_at: Option<LogId>,
    candidates: Vec<NodeId>,
}

impl RedirectHints {
    /// Whether a snapshot whose placement committed at `log_id` already
    /// accounts for everything this redirect taught us.
    fn subsumed_by(&self, log_id: LogId) -> bool {
        match self.learned_at {
            Some(learned) => (log_id.term, log_id.index) > (learned.term, learned.index),
            None => true,
        }
    }
}

/// A mutation that may have committed even though the client has not yet
/// observed its response. A later operation on the same stream must retry this
/// exact operation rather than reuse its sequence for different bytes.
#[derive(Clone)]
struct PendingMutation {
    sequence: Sequence,
    op: DataOp,
}

pub struct Client<T: Transport> {
    cluster_id: ClusterId,
    client_id: ClientId,
    seeds: Vec<String>,
    transport: T,
    cache: Mutex<Cache>,
    /// One stream per `(client_id, partition)`. These locks enforce the
    /// serialization required by the sequence protocol without holding a
    /// blocking mutex across network awaits.
    mutation_locks: Mutex<HashMap<u16, Arc<AsyncMutex<()>>>>,
    pending: Mutex<HashMap<u16, PendingMutation>>,
    request_id: AtomicU64,
    /// Rotates the candidate start position for spread (stale) reads, so
    /// successive stale reads distribute across replicas (DESIGN §10.1).
    rotation: AtomicU64,
}

/// How [`Client::route`] orders the candidate nodes for an operation.
#[derive(Clone, Copy)]
enum RouteOrder {
    /// Cached `leader_hint` first, then the voter set. Writes and linearizable
    /// reads must reach the leader, so they try it first.
    LeaderFirst,
    /// Rotate the voter set per request, spreading stale reads across replicas
    /// instead of hammering the leader. Every candidate is still tried within a
    /// round, so a satisfiable read always resolves. When `prefer_hint` is set
    /// (a `min_version` floor), the cached leader hint — the replica most likely
    /// to be caught up — leads, then the rotated remainder follows as fallback.
    Spread { prefer_hint: bool },
}

impl<T: Transport> Client<T> {
    pub fn new(
        cluster_id: ClusterId,
        client_id: ClientId,
        seeds: Vec<String>,
        transport: T,
    ) -> Client<T> {
        Client {
            cluster_id,
            client_id,
            seeds,
            transport,
            cache: Mutex::new(Cache::default()),
            mutation_locks: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            request_id: AtomicU64::new(1),
            rotation: AtomicU64::new(0),
        }
    }

    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }

    // -- Public operations --------------------------------------------------

    pub async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        if_version: Option<IfVersion>,
    ) -> Result<WriteReply> {
        self.mutate(
            key,
            DataOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
                if_version,
            },
        )
        .await
    }

    pub async fn delete(&self, key: &[u8], if_version: Option<IfVersion>) -> Result<WriteReply> {
        self.mutate(
            key,
            DataOp::Delete {
                key: key.to_vec(),
                if_version,
            },
        )
        .await
    }

    /// Linearizable read (DESIGN §8.3): the default, one quorum round trip.
    pub async fn get(&self, key: &[u8]) -> Result<Option<(Version, Vec<u8>)>> {
        self.get_with(key, Consistency::Linearizable).await
    }

    /// Eventually-consistent read (DESIGN §15): any voter may serve from its
    /// applied state, skipping ReadIndex. `min_version` opts into
    /// read-your-writes / monotonic reads; `None` accepts any applied value.
    pub async fn stale_get(
        &self,
        key: &[u8],
        min_version: Option<Version>,
    ) -> Result<Option<(Version, Vec<u8>)>> {
        self.get_with(key, Consistency::Stale { min_version }).await
    }

    /// Scatter a search to every partition leader and merge deterministically.
    /// The generation must come from a ReadIndex-fenced control-plane lookup.
    pub async fn search(
        &self,
        generation: &crate::search::SearchIndexGeneration,
        request: &crate::search::SearchRequest,
    ) -> Result<crate::search::SearchReply> {
        let (partitions, _) = self.routing_params().await?;
        crate::search::SearchCoordinator::new(partitions, ClientShardSource { client: self })?
            .search(generation, request)
            .await
    }

    /// Resolve the active generation through a meta ReadIndex and execute the
    /// complete-by-default scatter/gather search.
    pub async fn search_active(
        &self,
        request: &crate::search::SearchRequest,
    ) -> Result<crate::search::SearchReply> {
        let record = self
            .search_index(&request.index)
            .await?
            .ok_or_else(|| Error::Search(format!("index {:?} does not exist", request.index)))?;
        let generation = record.active.ok_or_else(|| {
            Error::Search(format!(
                "index {:?} has no active generation",
                request.index
            ))
        })?;
        self.search(&generation, request).await
    }

    pub async fn search_index(
        &self,
        name: &str,
    ) -> Result<Option<crate::search::SearchIndexRecord>> {
        crate::search::validate_index_name(name)?;
        let payload = codec::encode(&SearchCatalogQuery {
            name: name.to_string(),
            local_only: false,
        });
        for addr in &self.seeds {
            let request = Envelope::new(
                self.cluster_id,
                MsgType::SearchCatalogQuery,
                GroupId::Meta,
                self.next_request_id(),
                payload.clone(),
            );
            let Ok(reply) = self.transport.call(addr, request).await else {
                continue;
            };
            if reply.cluster_id != self.cluster_id || reply.msg_type != MsgType::SearchCatalogQuery
            {
                continue;
            }
            match codec::decode::<SearchCatalogReply>(&reply.payload) {
                Ok(SearchCatalogReply::Record(record)) => return Ok(record),
                Ok(SearchCatalogReply::Error(error)) => return Err(Error::Search(error)),
                _ => continue,
            }
        }
        Err(Error::Search(
            "no seed answered the search catalog query".into(),
        ))
    }

    async fn get_with(
        &self,
        key: &[u8],
        consistency: Consistency,
    ) -> Result<Option<(Version, Vec<u8>)>> {
        let partition = self.partition_of(key).await?;
        // A stale read may be served by any voter, so spread across replicas; a
        // linearizable read must reach the leader, so try the hint first. A stale
        // read with a `min_version` floor still spreads, but leads with the hint
        // since the leader is likeliest to have applied that version.
        let order = match consistency {
            Consistency::Linearizable => RouteOrder::LeaderFirst,
            Consistency::Stale { min_version } => RouteOrder::Spread {
                prefer_hint: min_version.is_some(),
            },
        };
        let request = ClientRequest::Read {
            key: key.to_vec(),
            consistency,
        };
        let reply = self
            .route(
                partition,
                MsgType::ClientOp,
                GroupId::Data(partition),
                &request,
                order,
            )
            .await?;
        match reply {
            ClientReply::Value(v) => Ok(v),
            ClientReply::Refused(e) => Err(Error::Raft(format!("read refused: {e}"))),
            ClientReply::Error(e) => Err(Error::Raft(format!("read rejected: {e}"))),
            other => Err(Error::Raft(format!("unexpected read reply: {other:?}"))),
        }
    }

    async fn mutate(&self, key: &[u8], op: DataOp) -> Result<WriteReply> {
        let partition = self.partition_of(key).await?;
        let lock = self.mutation_lock(partition);
        let _stream = lock.lock().await;
        let sequence = self.pending_sequence(partition, &op)?;
        let request = ClientRequest::Mutate(DataRequest {
            client_id: self.client_id,
            sequence,
            op: op.clone(),
        });

        let reply = self
            .route(
                partition,
                MsgType::ClientOp,
                GroupId::Data(partition),
                &request,
                RouteOrder::LeaderFirst,
            )
            .await?;

        match reply {
            ClientReply::Mutation(w) => {
                // The mutation is decided: advance the client's stream so the
                // next mutation uses a fresh sequence (DESIGN §8.4).
                self.commit_sequence(partition, sequence);
                self.pending.lock().unwrap().remove(&partition);
                Ok(w)
            }
            ClientReply::Refused(e) => {
                // Refusal is a pure function of the request bytes and
                // cluster-wide constants, so no replica — including one that
                // timed out on an earlier attempt — could have proposed this
                // op. The sequence was never at risk: release it (without
                // advancing the stream) so the partition does not wedge.
                self.pending.lock().unwrap().remove(&partition);
                Err(Error::Raft(format!("write refused: {e}")))
            }
            ClientReply::Error(e) => Err(Error::Raft(format!("write rejected: {e}"))),
            other => Err(Error::Raft(format!("unexpected write reply: {other:?}"))),
        }
    }

    // -- Routing ------------------------------------------------------------

    /// Ensure routing is loaded, then hash the key to a partition. A wrong local
    /// `P` cannot arise: `P` comes from the cluster, not client config.
    async fn partition_of(&self, key: &[u8]) -> Result<u16> {
        let (p, spec) = self.routing_params().await?;
        Ok(spec.partition_of(key, p))
    }

    async fn routing_params(&self) -> Result<(u16, HashSpec)> {
        if let Some(info) = self.cache.lock().unwrap().routing.as_ref() {
            return Ok((info.p, info.hash_spec));
        }
        self.refresh_routing().await?;
        let guard = self.cache.lock().unwrap();
        let info = guard
            .routing
            .as_ref()
            .expect("refresh_routing populates routing on success");
        Ok((info.p, info.hash_spec))
    }

    fn reserve_sequence(&self, partition: u16) -> Sequence {
        let mut cache = self.cache.lock().unwrap();
        *cache.next_seq.entry(partition).or_insert(1)
    }

    fn mutation_lock(&self, partition: u16) -> Arc<AsyncMutex<()>> {
        let mut locks = self.mutation_locks.lock().unwrap();
        locks
            .entry(partition)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    /// Return the durable stream sequence for `op`. An unresolved mutation
    /// retains its sequence; callers may retry only byte-identical bytes until
    /// its outcome is observed.
    fn pending_sequence(&self, partition: u16, op: &DataOp) -> Result<Sequence> {
        let mut pending = self.pending.lock().unwrap();
        if let Some(existing) = pending.get(&partition) {
            if existing.op == *op {
                return Ok(existing.sequence);
            }
            return Err(Error::Raft(format!(
                "mutation sequence {} for partition {} is unresolved; retry that operation first",
                existing.sequence, partition
            )));
        }

        let sequence = self.reserve_sequence(partition);
        pending.insert(
            partition,
            PendingMutation {
                sequence,
                op: op.clone(),
            },
        );
        Ok(sequence)
    }

    fn commit_sequence(&self, partition: u16, decided: Sequence) {
        let mut cache = self.cache.lock().unwrap();
        let slot = cache.next_seq.entry(partition).or_insert(1);
        // Only advance forward; a late duplicate reply must not rewind.
        if *slot <= decided {
            *slot = decided + 1;
        }
    }

    /// Fetch a routing snapshot from the first reachable seed (DESIGN §8.1). A
    /// cold client walks its seeds until one answers.
    async fn refresh_routing(&self) -> Result<()> {
        let seeds = self.seeds.clone();
        for addr in &seeds {
            let env = Envelope::new(
                self.cluster_id,
                MsgType::MetaQuery,
                GroupId::Meta,
                self.next_request_id(),
                Vec::new(),
            );
            let Ok(reply) = self.transport.call(addr, env).await else {
                continue;
            };
            if reply.cluster_id != self.cluster_id || reply.msg_type != MsgType::MetaQuery {
                continue;
            }
            if let Ok(info) = codec::decode::<RoutingInfo>(&reply.payload) {
                let mut cache = self.cache.lock().unwrap();
                // A snapshot newer than the redirect that taught us a candidate
                // already lists whoever still serves the partition. Retiring the
                // hint keeps a decommissioned node from being retried forever.
                cache.redirect_candidates.retain(|partition, hints| {
                    !info
                        .placement_log_id(*partition)
                        .is_some_and(|log_id| hints.subsumed_by(log_id))
                });
                cache.routing = Some(info);
                return Ok(());
            }
        }
        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "no seed answered a routing query",
        )))
    }

    /// The ordered candidate list for a partition (DESIGN §8.1). `LeaderFirst`
    /// puts the cached `leader_hint` ahead of the voter set; `Spread` omits the
    /// hint and rotates the voter set per call so stale reads distribute.
    fn candidate_nodes(&self, partition: u16, order: RouteOrder) -> Vec<NodeId> {
        let cache = self.cache.lock().unwrap();
        let lead_with_hint = matches!(
            order,
            RouteOrder::LeaderFirst | RouteOrder::Spread { prefer_hint: true }
        );

        let Some(info) = cache.routing.as_ref() else {
            // No routing yet: the only lead we might have is a redirect hint.
            let mut out = cache
                .redirect_candidates
                .get(&partition)
                .map(|hints| hints.candidates.clone())
                .unwrap_or_default();
            if lead_with_hint
                && let Some(&hint) = cache.leader_hint.get(&partition)
                && !out.contains(&hint)
            {
                out.insert(0, hint);
            }
            return out;
        };

        let mut voters = info.candidates(partition);
        if let Some(hints) = cache.redirect_candidates.get(&partition) {
            for &candidate in &hints.candidates {
                if !voters.contains(&candidate) {
                    voters.push(candidate);
                }
            }
        }
        if let RouteOrder::Spread { .. } = order
            && !voters.is_empty()
        {
            let shift = (self.rotation.fetch_add(1, Ordering::Relaxed) as usize) % voters.len();
            voters.rotate_left(shift);
        }

        let mut out = Vec::new();
        if lead_with_hint && let Some(&hint) = cache.leader_hint.get(&partition) {
            out.push(hint);
        }
        for c in voters {
            if !out.contains(&c) {
                out.push(c);
            }
        }
        out
    }

    fn resolve_addr(&self, node: NodeId) -> Option<String> {
        self.cache
            .lock()
            .unwrap()
            .routing
            .as_ref()
            .and_then(|info| info.control_addr(node).map(str::to_string))
    }

    /// Send `request` to the partition, honouring redirects and walking
    /// candidates on timeout. The same envelope body is reused every attempt so
    /// a mutation retries with a fixed idempotency key.
    async fn route(
        &self,
        partition: u16,
        msg_type: MsgType,
        group: GroupId,
        request: &ClientRequest,
        order: RouteOrder,
    ) -> Result<ClientReply> {
        let payload = codec::encode(request);

        for _round in 0..MAX_ROUNDS {
            let candidates = self.candidate_nodes(partition, order);
            let mut redirected = false;

            for node in candidates {
                let Some(addr) = self.resolve_addr(node) else {
                    continue;
                };
                let env = Envelope::new(
                    self.cluster_id,
                    msg_type,
                    group,
                    self.next_request_id(),
                    payload.clone(),
                );
                let transport_call = crate::perf::timer(WriteStage::ClientTransportCall);
                let reply = self.transport.call(&addr, env).await;
                drop(transport_call);
                let Ok(reply_env) = reply else {
                    // Unreachable/timeout: try the next candidate.
                    continue;
                };

                // Reject a mismatched-cluster response outright (DESIGN §8.2).
                if reply_env.cluster_id != self.cluster_id {
                    return Err(Error::Raft(format!(
                        "reply from cluster {:#x}, expected {:#x}",
                        reply_env.cluster_id, self.cluster_id
                    )));
                }

                // An empty or undecodable body is how a node reports that it
                // cannot serve right now (identity-fenced, shedding load): it
                // carries no client-visible outcome, so treat it like a timeout
                // and walk to the next candidate rather than failing the whole
                // operation on a node that is not participating.
                let Ok(reply) = codec::decode::<ClientReply>(&reply_env.payload) else {
                    continue;
                };
                match reply {
                    ClientReply::Redirect(r) => {
                        if r.cluster_id != self.cluster_id {
                            return Err(Error::Raft(format!(
                                "redirect from cluster {:#x}, expected {:#x}",
                                r.cluster_id, self.cluster_id
                            )));
                        }
                        let restart_for_hint = r.leader.is_some_and(|leader| leader != node);
                        self.apply_redirect(partition, node, &r);
                        redirected = true;
                        if restart_for_hint {
                            break;
                        }
                        continue;
                    }
                    other => return Ok(other),
                }
            }

            // Redirects name node ids but carry no addresses. Refresh after one
            // so a newly promoted/joined candidate can actually be resolved;
            // keep the redirect hint when seeds are temporarily unavailable.
            // With no redirect, exhausting every candidate requires a
            // successful refresh before another identical round is useful.
            if redirected {
                let _ = self.refresh_routing().await;
            } else {
                self.refresh_routing().await?;
            }
        }

        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("no candidate served partition {partition} within {MAX_ROUNDS} rounds"),
        )))
    }

    async fn route_search(
        &self,
        partition: u16,
        request: &crate::search::SearchRequest,
    ) -> Result<crate::search::SearchReply> {
        let payload = codec::encode(request);
        for _round in 0..MAX_ROUNDS {
            let candidates = self.candidate_nodes(partition, RouteOrder::LeaderFirst);
            let mut redirected = false;
            for node in candidates {
                let Some(addr) = self.resolve_addr(node) else {
                    continue;
                };
                let env = Envelope::new(
                    self.cluster_id,
                    MsgType::SearchOp,
                    GroupId::Data(partition),
                    self.next_request_id(),
                    payload.clone(),
                );
                let Ok(reply_env) = self.transport.call(&addr, env).await else {
                    continue;
                };
                if reply_env.cluster_id != self.cluster_id {
                    return Err(Error::Search(
                        "search reply came from another cluster".into(),
                    ));
                }
                let Ok(reply) = codec::decode::<SearchShardReply>(&reply_env.payload) else {
                    continue;
                };
                match reply {
                    SearchShardReply::Result(reply) => return Ok(reply),
                    SearchShardReply::Redirect(redirect) => {
                        if redirect.cluster_id != self.cluster_id {
                            return Err(Error::Search(
                                "search redirect came from another cluster".into(),
                            ));
                        }
                        let restart = redirect.leader.is_some_and(|leader| leader != node);
                        self.apply_redirect(partition, node, &redirect);
                        redirected = true;
                        if restart {
                            break;
                        }
                    }
                    SearchShardReply::Refused(error) | SearchShardReply::Error(error) => {
                        return Err(Error::Search(error));
                    }
                }
            }
            if redirected {
                let _ = self.refresh_routing().await;
            } else {
                self.refresh_routing().await?;
            }
        }
        Err(Error::Search(format!(
            "no candidate served search partition {partition}"
        )))
    }

    /// Fold a redirect into the cache: adopt the hinted leader and merge any
    /// newly advertised candidates. Advisory only (DESIGN §8.2).
    fn apply_redirect(&self, partition: u16, from: NodeId, r: &crate::api::ops::Redirect) {
        let mut cache = self.cache.lock().unwrap();
        let learned_at = cache
            .routing
            .as_ref()
            .and_then(|info| info.placement_log_id(partition));
        let hints = cache.redirect_candidates.entry(partition).or_default();
        hints.learned_at = learned_at;
        for &candidate in &r.candidates {
            if !hints.candidates.contains(&candidate) {
                hints.candidates.push(candidate);
            }
        }
        match r.leader {
            Some(leader) => {
                cache.leader_hint.insert(partition, leader);
            }
            None => {
                // The node we hit is not the leader and named none: stop
                // preferring it so the next round tries other voters.
                if cache.leader_hint.get(&partition) == Some(&from) {
                    cache.leader_hint.remove(&partition);
                }
            }
        }
    }
}

struct ClientShardSource<'a, T: Transport> {
    client: &'a Client<T>,
}

impl<T: Transport> crate::search::ShardSearchSource for ClientShardSource<'_, T> {
    fn search(
        &self,
        partition: u16,
        request: crate::search::SearchRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<crate::search::SearchReply>> + Send + '_>,
    > {
        Box::pin(async move { self.client.route_search(partition, &request).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ops::{Redirect, RoutingInfo};
    use crate::transport::{InProcess, Server};
    use crate::types::{HashSpec, LogId, NodeDirectoryEntry, NodeState, Placement};

    struct NullServer;
    impl Server for NullServer {
        async fn serve(&self, request: Envelope) -> Envelope {
            request
        }
    }

    /// A client whose cache already knows partition 0's voter set, so
    /// `candidate_nodes` can be exercised without any network.
    fn client_with_voters(voters: Vec<NodeId>) -> Client<InProcess<NullServer>> {
        let client = Client::new(1, 1, Vec::new(), InProcess::new());
        client.cache.lock().unwrap().routing = Some(RoutingInfo {
            cluster_id: 1,
            p: 1,
            hash_spec: HashSpec::CANONICAL,
            directory: Vec::new(),
            placements: vec![(
                0,
                Placement {
                    voters,
                    voters_log_id: LogId::new(1, 1),
                    r#move: None,
                },
            )],
        });
        client
    }

    #[test]
    fn leader_first_prepends_hint_and_dedupes() {
        let client = client_with_voters(vec![1, 2, 3]);
        client.cache.lock().unwrap().leader_hint.insert(0, 3);
        // The hint leads; the remaining voters follow without duplicating it.
        assert_eq!(
            client.candidate_nodes(0, RouteOrder::LeaderFirst),
            vec![3, 1, 2]
        );
    }

    #[test]
    fn spread_omits_hint_and_rotates_over_all_voters() {
        let client = client_with_voters(vec![1, 2, 3]);
        // A stale hint must not bias the spread order (min_version: None).
        client.cache.lock().unwrap().leader_hint.insert(0, 3);

        // Successive stale reads start at rotating positions, so load spreads
        // instead of always hitting the same replica.
        let orders: Vec<Vec<NodeId>> = (0..3)
            .map(|_| client.candidate_nodes(0, RouteOrder::Spread { prefer_hint: false }))
            .collect();
        assert_eq!(orders[0], vec![1, 2, 3]);
        assert_eq!(orders[1], vec![2, 3, 1]);
        assert_eq!(orders[2], vec![3, 1, 2]);

        // Each order is still a full permutation of the voter set, so every
        // candidate is tried within a round and a satisfiable read never starves.
        for order in &orders {
            let mut sorted = order.clone();
            sorted.sort();
            assert_eq!(sorted, vec![1, 2, 3]);
        }
    }

    #[test]
    fn spread_with_min_version_leads_with_hint_then_rotates() {
        let client = client_with_voters(vec![1, 2, 3]);
        client.cache.lock().unwrap().leader_hint.insert(0, 3);

        // A `min_version` floor keeps the hinted leader first every attempt (it
        // is likeliest caught up), while the remaining voters still rotate as a
        // spread fallback.
        let a = client.candidate_nodes(0, RouteOrder::Spread { prefer_hint: true });
        let b = client.candidate_nodes(0, RouteOrder::Spread { prefer_hint: true });
        assert_eq!(a, vec![3, 1, 2]);
        assert_eq!(b, vec![3, 2, 1]);

        // Still full permutations: every candidate reachable within a round.
        for order in [&a, &b] {
            let mut sorted = order.clone();
            sorted.sort();
            assert_eq!(sorted, vec![1, 2, 3]);
        }
    }

    #[test]
    fn redirect_candidates_extend_a_stale_placement() {
        let client = client_with_voters(vec![1, 2, 3]);
        client.apply_redirect(
            0,
            1,
            &crate::api::ops::Redirect {
                cluster_id: 1,
                leader: None,
                candidates: vec![2, 3, 4],
            },
        );
        assert_eq!(
            client.candidate_nodes(0, RouteOrder::LeaderFirst),
            vec![1, 2, 3, 4]
        );
    }

    enum ScriptedReply {
        Redirect,
        Leader,
        Routing(RoutingInfo),
        /// A node that cannot serve — identity-fenced, or shedding load at the
        /// router — answers with an empty body carrying no client outcome.
        Unavailable,
    }

    struct ScriptedServer(ScriptedReply);

    impl Server for ScriptedServer {
        async fn serve(&self, request: Envelope) -> Envelope {
            let payload = match &self.0 {
                ScriptedReply::Unavailable => Vec::new(),
                ScriptedReply::Redirect => codec::encode(&ClientReply::Redirect(Redirect {
                    cluster_id: 1,
                    leader: Some(4),
                    candidates: vec![4],
                })),
                ScriptedReply::Leader => {
                    codec::encode(&ClientReply::Value(Some((7, b"new".to_vec()))))
                }
                ScriptedReply::Routing(info) => codec::encode(info),
            };
            Envelope::new(
                1,
                request.msg_type,
                request.group_id,
                request.request_id,
                payload,
            )
        }
    }

    fn directory_entry(node_id: NodeId, addr: &str) -> NodeDirectoryEntry {
        NodeDirectoryEntry {
            node_id,
            control_addr: addr.into(),
            bulk_addr: format!("{addr}-bulk"),
            state: NodeState::Active,
            incarnation: 1,
        }
    }

    fn routing(voters: Vec<NodeId>, voters_log_id: LogId) -> RoutingInfo {
        RoutingInfo {
            cluster_id: 1,
            p: 1,
            hash_spec: HashSpec::CANONICAL,
            directory: voters
                .iter()
                .map(|&id| directory_entry(id, &format!("node-{id}")))
                .collect(),
            placements: vec![(
                0,
                Placement {
                    voters,
                    voters_log_id,
                    r#move: None,
                },
            )],
        }
    }

    #[tokio::test]
    async fn an_unavailable_candidate_is_skipped_rather_than_failing_the_operation() {
        let transport = InProcess::new();
        transport.register(
            "node-1",
            Arc::new(ScriptedServer(ScriptedReply::Unavailable)),
        );
        transport.register("node-4", Arc::new(ScriptedServer(ScriptedReply::Leader)));

        let client = Client::new(1, 9, Vec::new(), transport);
        client.cache.lock().unwrap().routing = Some(routing(vec![1, 4], LogId::new(1, 1)));

        // Node 1 answers first and cannot serve; the operation must walk to the
        // healthy replica instead of failing on an undecodable body.
        assert_eq!(
            client.get(b"key").await.unwrap(),
            Some((7, b"new".to_vec()))
        );
    }

    #[tokio::test]
    async fn a_newer_placement_snapshot_retires_redirect_candidates() {
        let transport = InProcess::new();
        transport.register(
            "seed",
            Arc::new(ScriptedServer(ScriptedReply::Routing(routing(
                vec![1, 2, 3],
                LogId::new(2, 10),
            )))),
        );
        let client = Client::new(1, 9, vec!["seed".into()], transport);
        client.cache.lock().unwrap().routing = Some(routing(vec![1, 2, 3], LogId::new(1, 1)));

        client.apply_redirect(
            0,
            1,
            &Redirect {
                cluster_id: 1,
                leader: None,
                candidates: vec![9],
            },
        );
        assert_eq!(
            client.candidate_nodes(0, RouteOrder::LeaderFirst),
            vec![1, 2, 3, 9]
        );

        // The refreshed placement is newer than the redirect that taught us
        // node 9, so it already names everyone who can serve: stop carrying an
        // id that may since have been decommissioned.
        client.refresh_routing().await.unwrap();
        assert_eq!(
            client.candidate_nodes(0, RouteOrder::LeaderFirst),
            vec![1, 2, 3]
        );
    }

    #[tokio::test]
    async fn a_redirect_newer_than_the_refreshed_snapshot_is_kept() {
        let transport = InProcess::new();
        transport.register(
            "seed",
            Arc::new(ScriptedServer(ScriptedReply::Routing(routing(
                vec![1, 2, 3],
                LogId::new(1, 1),
            )))),
        );
        let client = Client::new(1, 9, vec!["seed".into()], transport);
        client.cache.lock().unwrap().routing = Some(routing(vec![1, 2, 3], LogId::new(1, 1)));

        client.apply_redirect(
            0,
            1,
            &Redirect {
                cluster_id: 1,
                leader: None,
                candidates: vec![9],
            },
        );
        // Same placement generation: the seed's snapshot is no fresher than the
        // redirect, so the hint is still the client's only lead on node 9.
        client.refresh_routing().await.unwrap();
        assert_eq!(
            client.candidate_nodes(0, RouteOrder::LeaderFirst),
            vec![1, 2, 3, 9]
        );
    }

    #[tokio::test]
    async fn redirect_to_an_unknown_node_refreshes_its_address() {
        let transport = InProcess::new();
        transport.register("node-1", Arc::new(ScriptedServer(ScriptedReply::Redirect)));
        transport.register("node-4", Arc::new(ScriptedServer(ScriptedReply::Leader)));

        let fresh = RoutingInfo {
            cluster_id: 1,
            p: 1,
            hash_spec: HashSpec::CANONICAL,
            directory: vec![
                NodeDirectoryEntry {
                    node_id: 1,
                    control_addr: "node-1".into(),
                    bulk_addr: "node-1-bulk".into(),
                    state: NodeState::Active,
                    incarnation: 1,
                },
                NodeDirectoryEntry {
                    node_id: 4,
                    control_addr: "node-4".into(),
                    bulk_addr: "node-4-bulk".into(),
                    state: NodeState::Active,
                    incarnation: 1,
                },
            ],
            placements: vec![(
                0,
                Placement {
                    voters: vec![1, 2, 4],
                    voters_log_id: LogId::new(2, 10),
                    r#move: None,
                },
            )],
        };
        transport.register(
            "seed",
            Arc::new(ScriptedServer(ScriptedReply::Routing(fresh))),
        );

        let client = Client::new(1, 9, vec!["seed".into()], transport);
        client.cache.lock().unwrap().routing = Some(RoutingInfo {
            cluster_id: 1,
            p: 1,
            hash_spec: HashSpec::CANONICAL,
            directory: vec![NodeDirectoryEntry {
                node_id: 1,
                control_addr: "node-1".into(),
                bulk_addr: "node-1-bulk".into(),
                state: NodeState::Active,
                incarnation: 1,
            }],
            placements: vec![(
                0,
                Placement {
                    voters: vec![1],
                    voters_log_id: LogId::new(1, 1),
                    r#move: None,
                },
            )],
        });

        assert_eq!(
            client.get(b"key").await.unwrap(),
            Some((7, b"new".to_vec()))
        );
    }
}
