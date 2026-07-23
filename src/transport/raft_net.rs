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

use std::collections::HashMap;
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
use crate::transport::Transport;
use crate::transport::codec::{Envelope, MsgType};
use crate::transport::raft_wire::{AppendReply, SnapshotReply, VoteReply};
use crate::types::{ClusterId, GroupId, NodeId};

type Node = BasicNode;

/// Shared, updatable map of `node_id -> (control_addr, bulk_addr)`. Seeded from
/// the bootstrap directory and refreshed as the meta directory grows, so a
/// factory built before a node joined can still reach it later.
#[derive(Clone, Default)]
pub struct AddrBook {
    inner: Arc<RwLock<HashMap<NodeId, (String, String)>>>,
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

    pub fn control(&self, id: NodeId) -> Option<String> {
        self.inner.read().unwrap().get(&id).map(|(c, _)| c.clone())
    }

    pub fn bulk(&self, id: NodeId) -> Option<String> {
        self.inner.read().unwrap().get(&id).map(|(_, b)| b.clone())
    }
}

/// A `RaftNetworkFactory` for one group, carried over an arbitrary [`Transport`].
pub struct RaftPeerFactory<T: Transport> {
    group: GroupId,
    cluster_id: ClusterId,
    addrs: AddrBook,
    control: T,
    bulk: T,
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
        }
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
        let reply: AppendReply =
            codec::decode(&reply.payload).map_err(|e| bad_reply(self.target, e))?;
        reply.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
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

    #[tokio::test]
    async fn vote_round_trips_over_transport() {
        let switch = InProcess::new();
        switch.register("peer-2", Arc::new(StubPeer));

        let addrs = AddrBook::new();
        addrs.set(2, "peer-2".into(), "peer-2-bulk".into());

        let mut factory =
            RaftPeerFactory::new(GroupId::Meta, 0x1234, addrs, switch.clone(), switch.clone());
        let mut peer =
            <RaftPeerFactory<_> as RaftNetworkFactory<MetaTypeConfig>>::new_client(
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
    async fn missing_address_is_unreachable() {
        let switch: InProcess<StubPeer> = InProcess::new();
        let mut factory = RaftPeerFactory::new(
            GroupId::Meta,
            0x1234,
            AddrBook::new(),
            switch.clone(),
            switch.clone(),
        );
        let mut peer =
            <RaftPeerFactory<_> as RaftNetworkFactory<MetaTypeConfig>>::new_client(
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
}
