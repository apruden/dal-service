//! `ChannelNetwork`: an in-process `RaftNetwork` that forwards RPCs directly to
//! the target node's `Raft` handle, with per-link fault injection (DESIGN §7,
//! IMPLEMENTATION ground rule 3).
//!
//! Every multi-node correctness test runs on this network; ZMQ (M4) is tested
//! separately for transport concerns only. Faults are seeded and directional so
//! a test can isolate one node (partition) deterministically.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use openraft::error::{NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RaftNetwork, RaftNetworkFactory, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::partition::raft_types::{Node, NodeId, Raft, TypeConfig};

/// Shared directory of live `Raft` handles, keyed by node id.
#[derive(Clone, Default)]
pub struct Registry {
    inner: Arc<Mutex<HashMap<NodeId, Raft>>>,
}

impl Registry {
    pub fn register(&self, id: NodeId, raft: Raft) {
        self.inner.lock().unwrap().insert(id, raft);
    }

    pub fn get(&self, id: NodeId) -> Option<Raft> {
        self.inner.lock().unwrap().get(&id).cloned()
    }

    /// Remove a node's handle, e.g. to model a crash before restart.
    pub fn remove(&self, id: NodeId) {
        self.inner.lock().unwrap().remove(&id);
    }
}

/// Directional link faults. A blocked `(from, to)` drops all RPCs, modelling a
/// one-way partition; block both directions for a full partition.
#[derive(Clone, Default)]
pub struct Faults {
    blocked: Arc<Mutex<HashSet<(NodeId, NodeId)>>>,
}

impl Faults {
    pub fn block(&self, from: NodeId, to: NodeId) {
        self.blocked.lock().unwrap().insert((from, to));
    }

    pub fn unblock(&self, from: NodeId, to: NodeId) {
        self.blocked.lock().unwrap().remove(&(from, to));
    }

    /// Fully isolate a node in both directions.
    pub fn isolate(&self, node: NodeId, peers: &[NodeId]) {
        for &p in peers {
            self.block(node, p);
            self.block(p, node);
        }
    }

    pub fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }

    fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.lock().unwrap().contains(&(from, to))
    }
}

#[derive(Clone)]
pub struct ChannelNetworkFactory {
    local: NodeId,
    registry: Registry,
    faults: Faults,
}

impl ChannelNetworkFactory {
    pub fn new(local: NodeId, registry: Registry, faults: Faults) -> Self {
        ChannelNetworkFactory {
            local,
            registry,
            faults,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for ChannelNetworkFactory {
    type Network = ChannelNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &Node) -> Self::Network {
        ChannelNetwork {
            local: self.local,
            target,
            registry: self.registry.clone(),
            faults: self.faults.clone(),
        }
    }
}

pub struct ChannelNetwork {
    local: NodeId,
    target: NodeId,
    registry: Registry,
    faults: Faults,
}

impl ChannelNetwork {
    fn link_down<E>(&self) -> RPCError<NodeId, Node, E>
    where
        E: std::error::Error,
    {
        RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
            "link {}->{} is partitioned",
            self.local, self.target
        ))))
    }

    fn target_gone<E>(&self) -> RPCError<NodeId, Node, E>
    where
        E: std::error::Error,
    {
        RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
            "node {} not registered",
            self.target
        ))))
    }
}

impl RaftNetwork<TypeConfig> for ChannelNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        if self.faults.is_blocked(self.local, self.target) {
            return Err(self.link_down());
        }
        let raft = self.registry.get(self.target).ok_or_else(|| self.target_gone())?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        if self.faults.is_blocked(self.local, self.target) {
            return Err(self.link_down());
        }
        let raft = self.registry.get(self.target).ok_or_else(|| self.target_gone())?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        if self.faults.is_blocked(self.local, self.target) {
            return Err(self.link_down());
        }
        let raft = self.registry.get(self.target).ok_or_else(|| self.target_gone())?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}
