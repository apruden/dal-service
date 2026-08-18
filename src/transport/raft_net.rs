//! Production `RaftNetwork` over the transport seam (DESIGN §10.1).
//!
//! The in-process [`ChannelNetwork`](crate::partition::network) forwards RPCs
//! straight to a peer's `Raft` handle; this factory instead serializes each RPC
//! into an [`Envelope`] and ships it over a [`Transport`] (ZMQ `DEALER` in
//! production). AppendEntries/Vote ride the control lane; InstallSnapshot rides
//! the bulk lane (`bulk_addr`), keeping large transfers off the control path.
//!
//! One factory serves a single Raft group (its [`GroupId`] tags every frame so
//! the receiving dispatcher routes to the right handle). It is generic over the
//! openraft type config, so the meta and data groups reuse it. Transport
//! failures map to [`Unreachable`] so openraft retries another leader/candidate;
//! a decoded [`RaftError`] from the peer maps to [`RemoteError`].

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

use openraft::error::{NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftTypeConfig};
use serde::Serialize;

use crate::codec;
use crate::perf::WriteStage;
use crate::transport::Transport;
use crate::transport::codec::{Envelope, MsgType};
use crate::transport::raft_wire::{AppendReply, SnapshotReply, VoteReply};
use crate::types::{ClusterId, GroupId, NodeDirectoryEntry, NodeId, ProcessIdentityGate};

type Node = BasicNode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectoryMergeError {
    ConflictingIncarnation { node_id: NodeId, incarnation: u64 },
}

impl std::fmt::Display for DirectoryMergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DirectoryMergeError::ConflictingIncarnation {
                node_id,
                incarnation,
            } => write!(
                f,
                "node {node_id} has conflicting endpoints at incarnation {incarnation}"
            ),
        }
    }
}

impl std::error::Error for DirectoryMergeError {}

/// Shared, updatable map of `node_id -> (control_addr, bulk_addr)`. Seeded from
/// the bootstrap directory and refreshed as the meta directory grows, so a
/// factory built before a node joined can still reach it later.
#[derive(Clone, Default)]
pub struct AddrBook {
    inner: Arc<RwLock<HashMap<NodeId, (String, String)>>>,
    incarnations: Arc<RwLock<HashMap<NodeId, u64>>>,
    /// Operator-configured control seeds remain usable even when this node is
    /// absent from the immutable bootstrap descriptor or has not yet refreshed
    /// the replicated directory.
    control_seeds: Arc<RwLock<BTreeSet<String>>>,
}

impl AddrBook {
    pub fn new() -> AddrBook {
        AddrBook::default()
    }

    pub fn set(&self, id: NodeId, control_addr: String, bulk_addr: String) {
        self.inner
            .write()
            .unwrap()
            .insert(id, (control_addr, bulk_addr));
    }

    pub fn add_control_seed(&self, control_addr: String) {
        self.control_seeds.write().unwrap().insert(control_addr);
    }

    pub fn set_incarnation(&self, id: NodeId, incarnation: u64) {
        self.incarnations.write().unwrap().insert(id, incarnation);
    }

    pub fn incarnation(&self, id: NodeId) -> Option<u64> {
        self.incarnations.read().unwrap().get(&id).copied()
    }

    pub fn control(&self, id: NodeId) -> Option<String> {
        self.inner.read().unwrap().get(&id).map(|(c, _)| c.clone())
    }

    pub fn bulk(&self, id: NodeId) -> Option<String> {
        self.inner.read().unwrap().get(&id).map(|(_, b)| b.clone())
    }

    /// Merge a replicated directory snapshot into the live address book.
    /// Existing Raft factories share the same inner map and observe updates
    /// without being rebuilt.
    pub fn update_directory(
        &self,
        directory: &[NodeDirectoryEntry],
    ) -> Result<(), DirectoryMergeError> {
        let mut inner = self.inner.write().unwrap();
        let mut incarnations = self.incarnations.write().unwrap();
        let mut next_inner = inner.clone();
        let mut next_incarnations = incarnations.clone();
        for entry in directory {
            let previous_incarnation = next_incarnations.get(&entry.node_id).copied();
            match previous_incarnation {
                Some(previous) if entry.incarnation < previous => continue,
                Some(previous) if entry.incarnation == previous => {
                    if let Some((control, bulk)) = next_inner.get(&entry.node_id)
                        && (control != &entry.control_addr || bulk != &entry.bulk_addr)
                    {
                        return Err(DirectoryMergeError::ConflictingIncarnation {
                            node_id: entry.node_id,
                            incarnation: entry.incarnation,
                        });
                    }
                }
                _ => {
                    next_inner.insert(
                        entry.node_id,
                        (entry.control_addr.clone(), entry.bulk_addr.clone()),
                    );
                    next_incarnations.insert(entry.node_id, entry.incarnation);
                }
            }
        }
        *inner = next_inner;
        *incarnations = next_incarnations;
        Ok(())
    }

    /// Every currently known control endpoint, in node-id order. Control-plane
    /// discovery may contact non-meta nodes harmlessly; current meta hosts are
    /// the ones that return authoritative replies.
    pub fn control_addrs(&self) -> Vec<String> {
        let mut entries: Vec<(NodeId, String)> = self
            .inner
            .read()
            .unwrap()
            .iter()
            .map(|(&id, (control, _))| (id, control.clone()))
            .collect();
        entries.sort_by_key(|(id, _)| *id);
        let mut addresses: Vec<String> = entries.into_iter().map(|(_, address)| address).collect();
        for seed in self.control_seeds.read().unwrap().iter() {
            if !addresses.contains(seed) {
                addresses.push(seed.clone());
            }
        }
        addresses
    }
}

/// A `RaftNetworkFactory` for one group, carried over an arbitrary [`Transport`].
pub struct RaftPeerFactory<T: Transport> {
    group: GroupId,
    cluster_id: ClusterId,
    addrs: AddrBook,
    control: T,
    bulk: T,
    identity_gate: ProcessIdentityGate,
}

impl<T: Transport + Clone> RaftPeerFactory<T> {
    pub fn new(
        group: GroupId,
        cluster_id: ClusterId,
        addrs: AddrBook,
        control: T,
        bulk: T,
    ) -> RaftPeerFactory<T> {
        RaftPeerFactory {
            group,
            cluster_id,
            addrs,
            control,
            bulk,
            identity_gate: ProcessIdentityGate::default(),
        }
    }

    pub fn with_identity_gate(mut self, identity_gate: ProcessIdentityGate) -> Self {
        self.identity_gate = identity_gate;
        self
    }
}

/// A per-target client produced by [`RaftPeerFactory`].
pub struct RaftPeer<T: Transport> {
    group: GroupId,
    cluster_id: ClusterId,
    target: NodeId,
    addrs: AddrBook,
    control: T,
    bulk: T,
    identity_gate: ProcessIdentityGate,
}

fn unreachable<E>(target: NodeId, what: &str) -> RPCError<NodeId, Node, E>
where
    E: std::error::Error,
{
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
        "node {target}: {what}"
    ))))
}

fn transport_err<E>(target: NodeId, e: crate::error::Error) -> RPCError<NodeId, Node, E>
where
    E: std::error::Error,
{
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
        "node {target}: transport: {e}"
    ))))
}

fn bad_reply<E>(target: NodeId, e: crate::error::Error) -> RPCError<NodeId, Node, E>
where
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
        "node {target}: malformed reply: {e}"
    ))))
}

fn validate_reply_envelope(
    target: NodeId,
    reply: &Envelope,
    cluster_id: ClusterId,
    msg_type: MsgType,
    group: GroupId,
) -> std::io::Result<()> {
    if reply.cluster_id != cluster_id || reply.msg_type != msg_type || reply.group_id != group {
        return Err(std::io::Error::other(format!(
            "node {target}: reply envelope mismatch: expected cluster={cluster_id} type={msg_type:?} group={group:?}, got cluster={} type={:?} group={:?}",
            reply.cluster_id, reply.msg_type, reply.group_id
        )));
    }
    Ok(())
}

impl<C, T> RaftNetworkFactory<C> for RaftPeerFactory<T>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node>,
    AppendEntriesRequest<C>: Serialize,
    InstallSnapshotRequest<C>: Serialize,
    T: Transport + Clone + Send + Sync + 'static,
{
    type Network = RaftPeer<T>;

    async fn new_client(&mut self, target: NodeId, _node: &Node) -> RaftPeer<T> {
        RaftPeer {
            group: self.group,
            cluster_id: self.cluster_id,
            target,
            addrs: self.addrs.clone(),
            control: self.control.clone(),
            bulk: self.bulk.clone(),
            identity_gate: self.identity_gate.clone(),
        }
    }
}

impl<C, T> RaftNetwork<C> for RaftPeer<T>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = Node>,
    AppendEntriesRequest<C>: Serialize,
    InstallSnapshotRequest<C>: Serialize,
    T: Transport + Send + Sync + 'static,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let _profile = matches!(self.group, GroupId::Data(_))
            .then(|| crate::perf::timer(WriteStage::RaftAppendTransportCall));
        if !self.identity_gate.is_open() {
            return Err(unreachable(self.target, "local process identity is fenced"));
        }
        let addr = self
            .addrs
            .control(self.target)
            .ok_or_else(|| unreachable(self.target, "no control address"))?;
        let env = Envelope::new(
            self.cluster_id,
            MsgType::RaftAppend,
            self.group,
            0,
            codec::encode(&rpc),
        );
        let reply = self
            .control
            .call(&addr, env)
            .await
            .map_err(|e| transport_err(self.target, e))?;
        validate_reply_envelope(
            self.target,
            &reply,
            self.cluster_id,
            MsgType::RaftAppend,
            self.group,
        )
        .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let reply: AppendReply =
            codec::decode(&reply.payload).map_err(|e| bad_reply(self.target, e))?;
        reply.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        if !self.identity_gate.is_open() {
            return Err(unreachable(self.target, "local process identity is fenced"));
        }
        let addr = self
            .addrs
            .control(self.target)
            .ok_or_else(|| unreachable(self.target, "no control address"))?;
        let env = Envelope::new(
            self.cluster_id,
            MsgType::RaftVote,
            self.group,
            0,
            codec::encode(&rpc),
        );
        let reply = self
            .control
            .call(&addr, env)
            .await
            .map_err(|e| transport_err(self.target, e))?;
        validate_reply_envelope(
            self.target,
            &reply,
            self.cluster_id,
            MsgType::RaftVote,
            self.group,
        )
        .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let reply: VoteReply =
            codec::decode(&reply.payload).map_err(|e| bad_reply(self.target, e))?;
        reply.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        if !self.identity_gate.is_open() {
            return Err(unreachable(self.target, "local process identity is fenced"));
        }
        let addr = self
            .addrs
            .bulk(self.target)
            .ok_or_else(|| unreachable(self.target, "no bulk address"))?;
        let env = Envelope::new(
            self.cluster_id,
            MsgType::RaftSnapshot,
            self.group,
            0,
            codec::encode(&rpc),
        );
        let reply = self
            .bulk
            .call(&addr, env)
            .await
            .map_err(|e| transport_err(self.target, e))?;
        validate_reply_envelope(
            self.target,
            &reply,
            self.cluster_id,
            MsgType::RaftSnapshot,
            self.group,
        )
        .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let reply: SnapshotReply =
            codec::decode(&reply.payload).map_err(|e| bad_reply(self.target, e))?;
        reply.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::raft_types::MetaTypeConfig;
    use crate::transport::{InProcess, Server};
    use crate::types::NodeState;
    use openraft::Vote;
    use std::sync::Arc;

    /// A stub peer that decodes a Vote request and replies with a canned grant,
    /// so the encode -> transport -> decode path is exercised without a real Raft.
    struct StubPeer;
    impl Server for StubPeer {
        async fn serve(&self, request: Envelope) -> Envelope {
            assert_eq!(request.msg_type, MsgType::RaftVote);
            assert_eq!(request.group_id, GroupId::Meta);
            let req: VoteRequest<NodeId> = codec::decode(&request.payload).unwrap();
            let reply: VoteReply = Ok(VoteResponse {
                vote: req.vote,
                vote_granted: true,
                last_log_id: None,
            });
            Envelope::new(
                request.cluster_id,
                MsgType::RaftVote,
                request.group_id,
                request.request_id,
                codec::encode(&reply),
            )
        }
    }

    struct WrongClusterPeer;
    impl Server for WrongClusterPeer {
        async fn serve(&self, request: Envelope) -> Envelope {
            let req: VoteRequest<NodeId> = codec::decode(&request.payload).unwrap();
            let reply: VoteReply = Ok(VoteResponse {
                vote: req.vote,
                vote_granted: true,
                last_log_id: None,
            });
            Envelope::new(
                request.cluster_id + 1,
                request.msg_type,
                request.group_id,
                request.request_id,
                codec::encode(&reply),
            )
        }
    }

    #[tokio::test]
    async fn vote_round_trips_over_transport() {
        let switch = InProcess::new();
        switch.register("peer-2", Arc::new(StubPeer));

        let addrs = AddrBook::new();
        addrs.set(2, "peer-2".into(), "peer-2-bulk".into());

        let mut factory =
            RaftPeerFactory::new(GroupId::Meta, 0x1234, addrs, switch.clone(), switch.clone());
        let mut peer = <RaftPeerFactory<_> as RaftNetworkFactory<MetaTypeConfig>>::new_client(
            &mut factory,
            2,
            &Node::default(),
        )
        .await;

        let req = VoteRequest::<NodeId>::new(Vote::new(5, 1), None);
        let resp = <RaftPeer<_> as RaftNetwork<MetaTypeConfig>>::vote(
            &mut peer,
            req,
            RPCOption::new(std::time::Duration::from_secs(1)),
        )
        .await
        .unwrap();
        assert!(resp.vote_granted);
        assert_eq!(resp.vote, Vote::new(5, 1));
    }

    #[tokio::test]
    async fn raft_reply_from_the_wrong_cluster_is_rejected_before_payload_decode() {
        let switch = InProcess::new();
        switch.register("peer-2", Arc::new(WrongClusterPeer));
        let addrs = AddrBook::new();
        addrs.set(2, "peer-2".into(), "peer-2-bulk".into());
        let mut factory =
            RaftPeerFactory::new(GroupId::Meta, 0x1234, addrs, switch.clone(), switch);
        let mut peer = <RaftPeerFactory<_> as RaftNetworkFactory<MetaTypeConfig>>::new_client(
            &mut factory,
            2,
            &Node::default(),
        )
        .await;

        let result = <RaftPeer<_> as RaftNetwork<MetaTypeConfig>>::vote(
            &mut peer,
            VoteRequest::<NodeId>::new(Vote::new(5, 1), None),
            RPCOption::new(std::time::Duration::from_secs(1)),
        )
        .await;
        assert!(matches!(result, Err(RPCError::Network(_))));
    }

    #[tokio::test]
    async fn missing_address_is_unreachable() {
        let switch: InProcess<StubPeer> = InProcess::new();
        let mut factory = RaftPeerFactory::new(
            GroupId::Meta,
            0x1234,
            AddrBook::new(),
            switch.clone(),
            switch.clone(),
        );
        let mut peer = <RaftPeerFactory<_> as RaftNetworkFactory<MetaTypeConfig>>::new_client(
            &mut factory,
            7,
            &Node::default(),
        )
        .await;
        let err = <RaftPeer<_> as RaftNetwork<MetaTypeConfig>>::vote(
            &mut peer,
            VoteRequest::<NodeId>::new(Vote::new(1, 1), None),
            RPCOption::new(std::time::Duration::from_secs(1)),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, RPCError::Unreachable(_)));
    }

    #[test]
    fn directory_merge_never_regresses_or_rebinds_an_incarnation() {
        let bootstrap_hint = AddrBook::new();
        bootstrap_hint.set(7, "stale-control".into(), "stale-bulk".into());
        bootstrap_hint
            .update_directory(&[NodeDirectoryEntry {
                node_id: 7,
                control_addr: "live-control".into(),
                bulk_addr: "live-bulk".into(),
                state: NodeState::Active,
                incarnation: 1,
            }])
            .unwrap();
        assert_eq!(bootstrap_hint.control(7).as_deref(), Some("live-control"));

        let addrs = AddrBook::new();
        let current = NodeDirectoryEntry {
            node_id: 7,
            control_addr: "control-2".into(),
            bulk_addr: "bulk-2".into(),
            state: NodeState::Active,
            incarnation: 2,
        };
        addrs
            .update_directory(std::slice::from_ref(&current))
            .unwrap();

        let stale = NodeDirectoryEntry {
            control_addr: "control-1".into(),
            bulk_addr: "bulk-1".into(),
            incarnation: 1,
            ..current.clone()
        };
        addrs.update_directory(&[stale]).unwrap();
        assert_eq!(addrs.control(7).as_deref(), Some("control-2"));
        assert_eq!(addrs.incarnation(7), Some(2));

        let conflicting = NodeDirectoryEntry {
            control_addr: "control-conflict".into(),
            ..current
        };
        assert!(matches!(
            addrs.update_directory(&[conflicting]),
            Err(DirectoryMergeError::ConflictingIncarnation {
                node_id: 7,
                incarnation: 2
            })
        ));
        assert_eq!(addrs.control(7).as_deref(), Some("control-2"));
    }

    #[tokio::test]
    async fn fenced_factory_refuses_outbound_raft() {
        let switch = InProcess::new();
        switch.register("peer-2", Arc::new(StubPeer));
        let addrs = AddrBook::new();
        addrs.set(2, "peer-2".into(), "peer-2-bulk".into());
        let gate = ProcessIdentityGate::default();
        let mut factory =
            RaftPeerFactory::new(GroupId::Meta, 0x1234, addrs, switch.clone(), switch)
                .with_identity_gate(gate.clone());
        let mut peer = <RaftPeerFactory<_> as RaftNetworkFactory<MetaTypeConfig>>::new_client(
            &mut factory,
            2,
            &Node::default(),
        )
        .await;
        gate.fence();

        let result = <RaftPeer<_> as RaftNetwork<MetaTypeConfig>>::vote(
            &mut peer,
            VoteRequest::<NodeId>::new(Vote::new(1, 1), None),
            RPCOption::new(std::time::Duration::from_secs(1)),
        )
        .await;
        assert!(matches!(result, Err(RPCError::Unreachable(_))));
    }

    #[test]
    fn control_seeds_remain_discovery_candidates() {
        let addrs = AddrBook::new();
        addrs.set(2, "node-2".into(), "node-2-bulk".into());
        addrs.add_control_seed("seed-only".into());
        addrs.add_control_seed("node-2".into());

        assert_eq!(addrs.control_addrs(), vec!["node-2", "seed-only"]);
    }
}
