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

// ---------------------------------------------------------------------------
// Node-local default-CF record keys (DESIGN §6). These survive snapshot install
// and CF reclamation because they never live in a group's CFs.
// ---------------------------------------------------------------------------

/// The single node identity record.
pub fn identity_key() -> Vec<u8> {
    b"local/identity".to_vec()
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
