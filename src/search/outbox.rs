use crate::error::{Error, Result};
use crate::keyspace;
use crate::types::GroupId;
use serde::{Deserialize, Serialize};

pub type RaftLogId = openraft::LogId<u64>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOutboxEntry {
    pub epoch: u64,
    pub source_log_id: RaftLogId,
    pub user_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchConsumerState {
    pub definition_hash: [u8; 32],
    pub engine_revision: u32,
    pub checkpoint: Option<crate::search::IndexCheckpoint>,
}

pub fn encode_outbox_key(
    group: GroupId,
    epoch: u64,
    source_log_id: &RaftLogId,
    user_key: &[u8],
) -> Result<Vec<u8>> {
    let key_len = u32::try_from(user_key.len())
        .map_err(|_| Error::Search("user key is too large for search outbox".into()))?;
    let prefix = keyspace::search_outbox_prefix(group);
    let mut encoded = Vec::with_capacity(prefix.len() + 8 * 4 + 4 + user_key.len());
    encoded.extend_from_slice(&prefix);
    encoded.extend_from_slice(&epoch.to_be_bytes());
    // Index is first so byte ordering follows the authoritative applied prefix.
    encoded.extend_from_slice(&source_log_id.index.to_be_bytes());
    encoded.extend_from_slice(&source_log_id.leader_id.term.to_be_bytes());
    encoded.extend_from_slice(&source_log_id.leader_id.node_id.to_be_bytes());
    encoded.extend_from_slice(&key_len.to_be_bytes());
    encoded.extend_from_slice(user_key);
    Ok(encoded)
}

pub fn decode_outbox_key(group: GroupId, key: &[u8]) -> Result<SearchOutboxEntry> {
    let prefix = keyspace::search_outbox_prefix(group);
    let bytes = key.strip_prefix(prefix.as_slice()).ok_or_else(|| {
        Error::Corrupt(
            "search outbox".into(),
            "key has the wrong group prefix".into(),
        )
    })?;
    const HEADER: usize = 8 * 4 + 4;
    if bytes.len() < HEADER {
        return Err(Error::Corrupt(
            "search outbox".into(),
            "key is shorter than its fixed header".into(),
        ));
    }
    let read_u64 =
        |at: usize| u64::from_be_bytes(bytes[at..at + 8].try_into().expect("bounded header"));
    let epoch = read_u64(0);
    let index = read_u64(8);
    let term = read_u64(16);
    let node_id = read_u64(24);
    let key_len = u32::from_be_bytes(bytes[32..36].try_into().expect("bounded header")) as usize;
    if bytes.len() != HEADER + key_len {
        return Err(Error::Corrupt(
            "search outbox".into(),
            format!(
                "encoded user-key length is {key_len}, actual is {}",
                bytes.len().saturating_sub(HEADER)
            ),
        ));
    }
    Ok(SearchOutboxEntry {
        epoch,
        source_log_id: RaftLogId::new(openraft::CommittedLeaderId::new(term, node_id), index),
        user_key: bytes[HEADER..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_encoding_round_trips_opaque_keys_and_orders_by_log_index() {
        let group = GroupId::Data(7);
        let first = RaftLogId::new(openraft::CommittedLeaderId::new(3, 9), 11);
        let second = RaftLogId::new(openraft::CommittedLeaderId::new(3, 9), 12);
        let raw = b"a/\0/b";
        let first_key = encode_outbox_key(group, 4, &first, raw).unwrap();
        let second_key = encode_outbox_key(group, 4, &second, raw).unwrap();
        assert!(first_key < second_key);
        assert_eq!(
            decode_outbox_key(group, &first_key).unwrap(),
            SearchOutboxEntry {
                epoch: 4,
                source_log_id: first,
                user_key: raw.to_vec()
            }
        );
    }

    #[test]
    fn rejects_truncated_and_wrong_group_keys() {
        assert!(decode_outbox_key(GroupId::Data(1), b"short").is_err());
        let log = RaftLogId::new(openraft::CommittedLeaderId::new(1, 1), 1);
        let key = encode_outbox_key(GroupId::Data(2), 1, &log, b"k").unwrap();
        assert!(decode_outbox_key(GroupId::Data(1), &key).is_err());
    }
}
