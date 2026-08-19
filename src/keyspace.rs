//! Byte-prefix conventions for keys inside a `cf_state_<group>` column family
//! and for node-local records in the default CF.
//!
//! User keys are opaque bytes, so state-machine-internal records must live in a
//! namespace users can never address. Every state-CF key therefore carries a
//! one-byte tag. User key records use [`TAG_USER`]; nothing else does, so a
//! user `put` can never overwrite `last_applied` or a sequence record.

use crate::types::GroupId;

/// Tag byte for state-machine-internal singletons (e.g. `last_applied`).
pub const TAG_INTERNAL: u8 = 0x00;
/// Tag byte for user key/value/version records (M2).
pub const TAG_USER: u8 = 0x01;
/// Tag byte for per-client sequence/idempotency records (M2).
pub const TAG_SEQ: u8 = 0x02;
/// Tag byte for meta-group records (cluster/directory/placement, M5).
pub const TAG_META: u8 = 0x03;

/// Length prefix for a name embedded in a key.
///
/// Names are length-prefixed so a name containing the delimiter cannot be
/// confused with a longer one, which only holds while the prefix is exact: a
/// truncated length would let two different names encode to the same key.
/// `validate_name` caps every name at `SEARCH_MAX_NAME_BYTES`, orders of
/// magnitude below the `u32` range, so the truncation is unreachable — this
/// pins that dependency so it fails loudly in tests if a caller ever encodes a
/// name that skipped validation.
fn name_len_prefix(name: &str) -> [u8; 4] {
    debug_assert!(
        u32::try_from(name.len()).is_ok(),
        "name length must fit its u32 key prefix",
    );
    (name.len() as u32).to_be_bytes()
}

/// The single reserved key holding a group's `last_applied` `LogId`.
pub fn last_applied_key() -> Vec<u8> {
    vec![TAG_INTERNAL, b'L']
}

/// The single reserved key holding the Raft-applied state: the openraft
/// `LogId` plus the last-applied membership (M3). Kept distinct from
/// [`last_applied_key`] because it carries openraft's own encoding.
pub fn raft_applied_key() -> Vec<u8> {
    vec![TAG_INTERNAL, b'R']
}

/// Encode a user key record key.
pub fn user_key(key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + key.len());
    k.push(TAG_USER);
    k.extend_from_slice(key);
    k
}

/// Encode a per-client sequence record key.
pub fn seq_key(client_id: u128) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 16);
    k.push(TAG_SEQ);
    k.extend_from_slice(&client_id.to_be_bytes());
    k
}

// ---------------------------------------------------------------------------
// Meta-group state-CF record keys (DESIGN §5, M5). All carry [`TAG_META`] so
// they share the meta group's `cf_state` without colliding with any other tag.
// ---------------------------------------------------------------------------

/// The single immutable cluster-configuration record.
pub fn meta_cluster_key() -> Vec<u8> {
    vec![TAG_META, b'C']
}

/// One node-directory entry, keyed by node id.
pub fn meta_node_key(node_id: crate::types::NodeId) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + 8);
    k.push(TAG_META);
    k.push(b'N');
    k.extend_from_slice(&node_id.to_be_bytes());
    k
}

/// The scan prefix covering every node-directory entry.
pub fn meta_node_prefix() -> [u8; 2] {
    [TAG_META, b'N']
}

/// One partition/meta placement record, keyed by group token.
pub fn meta_placement_key(group: GroupId) -> Vec<u8> {
    let mut k = Vec::with_capacity(2 + group.token().len());
    k.push(TAG_META);
    k.push(b'P');
    k.extend_from_slice(group.token().as_bytes());
    k
}

pub fn meta_search_index_prefix() -> [u8; 2] {
    [TAG_META, b'S']
}

pub fn meta_search_index_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 4 + name.len());
    key.extend_from_slice(&meta_search_index_prefix());
    key.extend_from_slice(&name_len_prefix(name));
    key.extend_from_slice(name.as_bytes());
    key
}

pub fn meta_search_ready_prefix(name: &str, generation: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 4 + name.len() + 8);
    key.extend_from_slice(&[TAG_META, b's']);
    key.extend_from_slice(&name_len_prefix(name));
    key.extend_from_slice(name.as_bytes());
    key.extend_from_slice(&generation.to_be_bytes());
    key
}

pub fn meta_search_ready_key(
    name: &str,
    generation: u64,
    group: GroupId,
    node_id: crate::types::NodeId,
) -> Vec<u8> {
    let mut key = meta_search_ready_prefix(name, generation);
    match group {
        GroupId::Data(partition) => {
            key.push(1);
            key.extend_from_slice(&partition.to_be_bytes());
        }
        GroupId::Meta => key.extend_from_slice(&[0, 0, 0]),
    }
    key.extend_from_slice(&node_id.to_be_bytes());
    key
}

// ---------------------------------------------------------------------------
// Node-local default-CF record keys (DESIGN §6). These survive snapshot install
// and CF reclamation because they never live in a group's CFs.
// ---------------------------------------------------------------------------

/// The single node identity record.
pub fn identity_key() -> Vec<u8> {
    b"local/identity".to_vec()
}

/// Monotonically increasing process epoch for heartbeat sequence numbers.
pub fn heartbeat_incarnation_key() -> Vec<u8> {
    b"local/heartbeat_incarnation".to_vec()
}

/// Monotonically increasing process epoch for node-local held-search sessions.
/// Session sequence numbers may restart at one only when this epoch advances.
pub fn search_session_incarnation_key() -> Vec<u8> {
    b"local/search_session_incarnation".to_vec()
}

/// Durable pointer to the complete state-CF generation currently visible for a
/// group. Absent means the original `cf_state_<group>` generation.
pub fn state_cf_pointer_key(group: GroupId) -> Vec<u8> {
    let mut key = b"local/state_cf/".to_vec();
    key.extend_from_slice(group.token().as_bytes());
    key
}

pub fn state_cf_pointer_prefix() -> &'static [u8] {
    b"local/state_cf/"
}

/// Monotonic allocator for snapshot staging CF names. It is deliberately kept
/// after reclamation so a re-admitted group cannot reuse an orphan's name.
pub fn state_cf_generation_key(group: GroupId) -> Vec<u8> {
    let mut key = b"local/state_cf_generation/".to_vec();
    key.extend_from_slice(group.token().as_bytes());
    key
}

/// The exact replicated registration used by this process identity.
pub fn registration_binding_key() -> Vec<u8> {
    b"local/registration_binding".to_vec()
}

pub fn serving_key(group: GroupId) -> Vec<u8> {
    format!("local/serving/{}", group.token()).into_bytes()
}

pub fn admission_key(group: GroupId) -> Vec<u8> {
    format!("local/admission/{}", group.token()).into_bytes()
}

pub fn pending_report_key(group: GroupId) -> Vec<u8> {
    format!("local/pending_report/{}", group.token()).into_bytes()
}

pub fn bootstrap_key(group: GroupId) -> Vec<u8> {
    format!("local/bootstrap/{}", group.token()).into_bytes()
}

pub fn search_prefix(group: GroupId) -> Vec<u8> {
    format!("local/search/{}/", group.token()).into_bytes()
}

pub fn search_epoch_key(group: GroupId) -> Vec<u8> {
    let mut key = search_prefix(group);
    key.extend_from_slice(b"epoch");
    key
}

pub fn search_outbox_prefix(group: GroupId) -> Vec<u8> {
    let mut key = search_prefix(group);
    key.extend_from_slice(b"outbox/");
    key
}

pub fn search_outbox_epoch_prefix(group: GroupId, epoch: u64) -> Vec<u8> {
    let mut key = search_outbox_prefix(group);
    key.extend_from_slice(&epoch.to_be_bytes());
    key
}

pub fn search_consumer_prefix(group: GroupId) -> Vec<u8> {
    let mut key = search_prefix(group);
    key.extend_from_slice(b"consumer/");
    key
}

pub fn search_consumer_key(group: GroupId, name: &str, generation: u64) -> Vec<u8> {
    let mut key = search_consumer_prefix(group);
    key.extend_from_slice(&name_len_prefix(name));
    key.extend_from_slice(name.as_bytes());
    key.extend_from_slice(&generation.to_be_bytes());
    key
}

/// Recover `(index name, generation)` from a consumer key.
pub fn decode_search_consumer_key(
    group: GroupId,
    key: &[u8],
) -> crate::error::Result<(String, u64)> {
    let corrupt = |detail: &str| {
        crate::error::Error::Corrupt("search consumer key".into(), detail.to_string())
    };
    let rest = key
        .strip_prefix(search_consumer_prefix(group).as_slice())
        .ok_or_else(|| corrupt("key has the wrong group prefix"))?;
    let len_bytes = rest.get(..4).ok_or_else(|| corrupt("key is truncated"))?;
    let name_len = u32::from_be_bytes(len_bytes.try_into().expect("bounded header")) as usize;
    let name = rest
        .get(4..4 + name_len)
        .ok_or_else(|| corrupt("name runs past the key"))?;
    let generation = rest
        .get(4 + name_len..4 + name_len + 8)
        .ok_or_else(|| corrupt("key has no generation"))?;
    Ok((
        String::from_utf8(name.to_vec()).map_err(|_| corrupt("name is not UTF-8"))?,
        u64::from_be_bytes(generation.try_into().expect("bounded header")),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_keys_never_collide_with_internal() {
        // A user key, however crafted, is tagged 0x01 and so can never equal
        // the internal last_applied key (tagged 0x00).
        assert_ne!(user_key(b"L"), last_applied_key());
        assert_ne!(user_key(&[]), last_applied_key());
        assert_ne!(user_key(&[TAG_INTERNAL, b'L']), last_applied_key());
    }

    #[test]
    fn tags_are_distinct() {
        let tags = [TAG_INTERNAL, TAG_USER, TAG_SEQ, TAG_META];
        let set: std::collections::BTreeSet<_> = tags.iter().collect();
        assert_eq!(set.len(), tags.len());
    }
}
