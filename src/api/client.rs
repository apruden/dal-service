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

use crate::api::ops::{ClientReply, ClientRequest, RoutingInfo, WriteReply};
use crate::codec;
use crate::error::{Error, Result};
use crate::transport::Transport;
use crate::transport::codec::{Envelope, MsgType};
use crate::types::{
    ClientId, ClusterId, DataOp, DataRequest, GroupId, HashSpec, IfVersion, NodeId, Sequence,
    Version,
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
    /// Next unused sequence per partition for this client (DESIGN §8.4).
    /// Sequence 0 is never decided, so streams start at 1.
    next_seq: HashMap<u16, Sequence>,
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

    pub async fn get(&self, key: &[u8]) -> Result<Option<(Version, Vec<u8>)>> {
        let partition = self.partition_of(key).await?;
        let request = ClientRequest::Read { key: key.to_vec() };
        let reply = self
            .route(
                partition,
                MsgType::ClientOp,
                GroupId::Data(partition),
                &request,
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
                self.cache.lock().unwrap().routing = Some(info);
                return Ok(());
            }
        }
        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "no seed answered a routing query",
        )))
    }

    /// The ordered candidate list for a partition: cached `leader_hint` first,
    /// then the placement's voter set (DESIGN §8.1).
    fn candidate_nodes(&self, partition: u16) -> Vec<NodeId> {
        let cache = self.cache.lock().unwrap();
        let mut out = Vec::new();
        if let Some(&hint) = cache.leader_hint.get(&partition) {
            out.push(hint);
        }
        if let Some(info) = cache.routing.as_ref() {
            for c in info.candidates(partition) {
                if !out.contains(&c) {
                    out.push(c);
                }
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
    ) -> Result<ClientReply> {
        let payload = codec::encode(request);

        for _round in 0..MAX_ROUNDS {
            let candidates = self.candidate_nodes(partition);
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
                let Ok(reply_env) = self.transport.call(&addr, env).await else {
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

                let reply: ClientReply = codec::decode(&reply_env.payload)?;
                match reply {
                    ClientReply::Redirect(r) => {
                        self.apply_redirect(partition, node, &r);
                        redirected = true;
                        break;
                    }
                    other => return Ok(other),
                }
            }

            // A redirect updated the cache: restart the round so the new leader
            // hint is tried first. Otherwise all candidates were unreachable —
            // refresh routing from the seeds and try again.
            if !redirected {
                self.refresh_routing().await?;
            }
        }

        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("no candidate served partition {partition} within {MAX_ROUNDS} rounds"),
        )))
    }

    /// Fold a redirect into the cache: adopt the hinted leader and merge any
    /// newly advertised candidates. Advisory only (DESIGN §8.2).
    fn apply_redirect(&self, partition: u16, from: NodeId, r: &crate::api::ops::Redirect) {
        let mut cache = self.cache.lock().unwrap();
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
