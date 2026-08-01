//! Authoritative node-directory discovery over the peer-control plane.
//!
//! Public `MetaQuery` replies are deliberately advisory follower reads. Runtime
//! authority (Raft endpoints and heartbeat registration incarnations) instead
//! comes through `DirectoryQuery`, whose `Value` response is emitted only after
//! a meta ReadIndex barrier. Follower directory data is used solely to resolve a
//! leader hint and is never returned as authoritative state.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};

use crate::codec;
use crate::error::{Error, Result};
use crate::transport::Transport;
use crate::transport::codec::{Envelope, MsgType};
use crate::transport::raft_wire::DirectoryQueryReply;
use crate::types::{ClusterId, GroupId, NodeDirectoryEntry, NodeId};

/// Fetch the current directory from the meta leader. Every round contacts all
/// newly discovered endpoints concurrently; follower hints may add the leader's
/// endpoint for the next round but are never accepted as the result.
pub async fn fetch_authoritative_directory<T: Transport>(
    transport: &T,
    cluster_id: ClusterId,
    seeds: impl IntoIterator<Item = String>,
    timeout: Duration,
) -> Result<Vec<NodeDirectoryEntry>> {
    let run = async {
        let mut tried = HashSet::new();
        let mut attempts = FuturesUnordered::new();
        for address in seeds {
            if tried.insert(address.clone()) {
                attempts.push(query_one(transport, cluster_id, address));
            }
        }

        while let Some((_addr, response)) = attempts.next().await {
            let Ok(envelope) = response else {
                continue;
            };
            if envelope.cluster_id != cluster_id || envelope.msg_type != MsgType::DirectoryQuery {
                continue;
            }
            match codec::decode::<DirectoryQueryReply>(&envelope.payload) {
                Ok(DirectoryQueryReply::Value(directory)) => return Ok(directory),
                Ok(DirectoryQueryReply::NotLeader {
                    leader,
                    directory_hint,
                }) => {
                    let hints: HashMap<NodeId, String> = directory_hint
                        .into_iter()
                        .map(|entry| (entry.node_id, entry.control_addr))
                        .collect();
                    let leader_address = leader.and_then(|id| hints.get(&id)).cloned();
                    // The leader id may be temporarily unknown. Follower
                    // endpoints remain safe as discovery candidates because
                    // none becomes a returned value without its own
                    // authoritative `Value` response.
                    for address in leader_address.into_iter().chain(hints.into_values()) {
                        if tried.insert(address.clone()) {
                            attempts.push(query_one(transport, cluster_id, address));
                        }
                    }
                }
                Ok(DirectoryQueryReply::Unavailable) | Err(_) => {}
            }
        }
        Err(Error::Raft(
            "no meta leader returned an authoritative directory".into(),
        ))
    };

    tokio::time::timeout(timeout, run)
        .await
        .map_err(|_| Error::Raft("authoritative directory query timed out".into()))?
}

async fn query_one<T: Transport>(
    transport: &T,
    cluster_id: ClusterId,
    address: String,
) -> (String, Result<Envelope>) {
    let request = Envelope::new(
        cluster_id,
        MsgType::DirectoryQuery,
        GroupId::Meta,
        0,
        Vec::new(),
    );
    let response = transport.call(&address, request).await;
    (address, response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::transport::{InProcess, Server};
    use crate::types::NodeState;

    enum DirectoryServer {
        Follower {
            leader: Option<NodeId>,
            hint: Vec<NodeDirectoryEntry>,
        },
        Leader(Vec<NodeDirectoryEntry>),
    }

    impl Server for DirectoryServer {
        async fn serve(&self, request: Envelope) -> Envelope {
            let reply = match self {
                DirectoryServer::Follower { leader, hint } => DirectoryQueryReply::NotLeader {
                    leader: *leader,
                    directory_hint: hint.clone(),
                },
                DirectoryServer::Leader(directory) => DirectoryQueryReply::Value(directory.clone()),
            };
            Envelope::new(
                request.cluster_id,
                MsgType::DirectoryQuery,
                GroupId::Meta,
                request.request_id,
                codec::encode(&reply),
            )
        }
    }

    fn entry(node_id: NodeId, control_addr: &str) -> NodeDirectoryEntry {
        NodeDirectoryEntry {
            node_id,
            control_addr: control_addr.into(),
            bulk_addr: format!("{control_addr}-bulk"),
            state: NodeState::Active,
            incarnation: 1,
        }
    }

    #[tokio::test]
    async fn follower_data_only_resolves_the_authoritative_leader() {
        let transport = InProcess::new();
        let directory = vec![entry(1, "follower"), entry(2, "leader")];
        transport.register(
            "follower",
            Arc::new(DirectoryServer::Follower {
                leader: Some(2),
                hint: directory.clone(),
            }),
        );
        transport.register(
            "leader",
            Arc::new(DirectoryServer::Leader(directory.clone())),
        );

        let result = fetch_authoritative_directory(
            &transport,
            7,
            ["follower".to_string()],
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(result, directory);
    }

    #[tokio::test]
    async fn follower_hint_is_never_returned_as_authority() {
        let transport = InProcess::new();
        transport.register(
            "follower",
            Arc::new(DirectoryServer::Follower {
                leader: None,
                hint: vec![entry(1, "follower")],
            }),
        );

        assert!(
            fetch_authoritative_directory(
                &transport,
                7,
                ["follower".to_string()],
                Duration::from_secs(1),
            )
            .await
            .is_err()
        );
    }
}
