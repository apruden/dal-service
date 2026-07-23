//! `ChannelNetwork`: an in-process `RaftNetwork` that forwards RPCs directly to
//! the target node's `Raft` handle, with per-link fault injection (DESIGN §7,
//! IMPLEMENTATION ground rule 3).
//!
//! Every multi-node correctness test runs on this network; ZMQ (M4) is tested
//! separately for transport concerns only. Faults are seeded and directional so
//! a test can isolate one node (partition) deterministically.
//!
//! Generic over the openraft type config `C` so both the data groups and the
//! meta group (M5) reuse it — each group keeps its own [`Registry`] of live
//! handles.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use openraft::error::{NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, RaftTypeConfig};

type Nid = u64;
type Node = BasicNode;

/// Shared directory of live `Raft` handles, keyed by node id.
pub struct Registry<C: RaftTypeConfig> {
    inner: Arc<Mutex<HashMap<Nid, openraft::Raft<C>>>>,
}

impl<C: RaftTypeConfig> Clone for Registry<C> {
    fn clone(&self) -> Self {
        Registry {
            inner: self.inner.clone(),
        }
    }
}

impl<C: RaftTypeConfig> Default for Registry<C> {
    fn default() -> Self {
        Registry {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<C: RaftTypeConfig<NodeId = Nid>> Registry<C> {
    pub fn register(&self, id: Nid, raft: openraft::Raft<C>) {
        self.inner.lock().unwrap().insert(id, raft);
    }

    pub fn get(&self, id: Nid) -> Option<openraft::Raft<C>> {
        self.inner.lock().unwrap().get(&id).cloned()
    }

    /// Remove a node's handle, e.g. to model a crash before restart.
    pub fn remove(&self, id: Nid) {
        self.inner.lock().unwrap().remove(&id);
    }
}

/// Directional link faults. A blocked `(from, to)` drops all RPCs, modelling a
/// one-way partition; block both directions for a full partition. Type-agnostic
/// — one `Faults` can gate several groups.
#[derive(Clone, Default)]
pub struct Faults {
    blocked: Arc<Mutex<HashSet<(Nid, Nid)>>>,
}

impl Faults {
    pub fn block(&self, from: Nid, to: Nid) {
        self.blocked.lock().unwrap().insert((from, to));
    }

    pub fn unblock(&self, from: Nid, to: Nid) {
        self.blocked.lock().unwrap().remove(&(from, to));
    }

    /// Fully isolate a node in both directions.
    pub fn isolate(&self, node: Nid, peers: &[Nid]) {
        for &p in peers {
            self.block(node, p);
            self.block(p, node);
        }
    }

    pub fn heal(&self) {
        self.blocked.lock().unwrap().clear();
    }

    fn is_blocked(&self, from: Nid, to: Nid) -> bool {
        self.blocked.lock().unwrap().contains(&(from, to))
    }
}

pub struct ChannelNetworkFactory<C: RaftTypeConfig> {
    local: Nid,
    registry: Registry<C>,
    faults: Faults,
}

impl<C: RaftTypeConfig> Clone for ChannelNetworkFactory<C> {
    fn clone(&self) -> Self {
        ChannelNetworkFactory {
            local: self.local,
            registry: self.registry.clone(),
            faults: self.faults.clone(),
        }
    }
}

impl<C: RaftTypeConfig<NodeId = Nid>> ChannelNetworkFactory<C> {
    pub fn new(local: Nid, registry: Registry<C>, faults: Faults) -> Self {
        ChannelNetworkFactory {
            local,
            registry,
            faults,
        }
    }
}

impl<C: RaftTypeConfig<NodeId = Nid, Node = Node>> RaftNetworkFactory<C>
    for ChannelNetworkFactory<C>
{
    type Network = ChannelNetwork<C>;

    async fn new_client(&mut self, target: Nid, _node: &Node) -> Self::Network {
        ChannelNetwork {
            local: self.local,
            target,
            registry: self.registry.clone(),
            faults: self.faults.clone(),
        }
    }
}

pub struct ChannelNetwork<C: RaftTypeConfig> {
    local: Nid,
    target: Nid,
    registry: Registry<C>,
    faults: Faults,
}

impl<C: RaftTypeConfig<NodeId = Nid, Node = Node>> ChannelNetwork<C> {
    fn link_down<E>(&self) -> RPCError<Nid, Node, E>
    where
        E: std::error::Error,
    {
        RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
            "link {}->{} is partitioned",
            self.local, self.target
        ))))
    }

    fn target_gone<E>(&self) -> RPCError<Nid, Node, E>
    where
        E: std::error::Error,
    {
        RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
            "node {} not registered",
            self.target
        ))))
    }
}

impl<C: RaftTypeConfig<NodeId = Nid, Node = Node>> RaftNetwork<C> for ChannelNetwork<C> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<Nid>, RPCError<Nid, Node, RaftError<Nid>>> {
        if self.faults.is_blocked(self.local, self.target) {
            return Err(self.link_down());
        }
        let raft = self
            .registry
            .get(self.target)
            .ok_or_else(|| self.target_gone())?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<Nid>,
        _option: RPCOption,
    ) -> Result<VoteResponse<Nid>, RPCError<Nid, Node, RaftError<Nid>>> {
        if self.faults.is_blocked(self.local, self.target) {
            return Err(self.link_down());
        }
        let raft = self
            .registry
            .get(self.target)
            .ok_or_else(|| self.target_gone())?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<Nid>,
        RPCError<Nid, Node, RaftError<Nid, openraft::error::InstallSnapshotError>>,
    > {
        if self.faults.is_blocked(self.local, self.target) {
            return Err(self.link_down());
        }
        let raft = self
            .registry
            .get(self.target)
            .ok_or_else(|| self.target_gone())?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}
