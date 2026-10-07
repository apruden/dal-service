//! Cluster-scoped principal policy used by authenticated transport admission
//! and operation authorization. Socket authentication and policy evaluation
//! are deliberately separate: a valid CURVE key alone grants no operation.

use std::collections::BTreeMap;

/// DAL-level permissions attached to an out-of-band authenticated principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Permission {
    ClientRead,
    ClientWrite,
    Search,
    PeerRpc,
    Operator,
}

/// Identity and permissions supplied by the trusted cluster policy. The
/// `user_id` is the ZAP identity; it must never be copied from an RPC payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub user_id: String,
    /// Z85-encoded CURVE public key (40 ASCII bytes), provisioned out of band.
    pub curve_public_key: String,
    pub permissions: Vec<Permission>,
}

/// Immutable policy snapshot. Replacing the snapshot is the atomic unit for
/// future policy reloads; callers can retain its generation for diagnostics.
#[derive(Debug, Clone)]
pub struct TrustPolicy {
    cluster_id: u128,
    generation: u64,
    principals: BTreeMap<String, Principal>,
    curve_keys: BTreeMap<String, String>,
}

impl TrustPolicy {
    pub fn new(
        cluster_id: u128,
        generation: u64,
        principals: Vec<Principal>,
    ) -> Result<Self, String> {
        if generation == 0 {
            return Err("trust policy generation must be non-zero".into());
        }
        if principals.len() > 100_000 {
            return Err("trust policy exceeds the 100000 principal cluster limit".into());
        }
        let mut indexed = BTreeMap::new();
        let mut curve_keys = BTreeMap::new();
        for principal in principals {
            if principal.user_id.is_empty() || principal.user_id.len() > 255 {
                return Err("principal user_id must be 1..=255 bytes".into());
            }
            if principal.permissions.is_empty() {
                return Err(format!(
                    "principal {} has no permissions",
                    principal.user_id
                ));
            }
            if principal.curve_public_key.len() != 40 || !principal.curve_public_key.is_ascii() {
                return Err(format!(
                    "principal {} CURVE public key must be 40 ASCII bytes",
                    principal.user_id
                ));
            }
            if curve_keys
                .insert(
                    principal.curve_public_key.clone(),
                    principal.user_id.clone(),
                )
                .is_some()
            {
                return Err("duplicate CURVE public key".into());
            }
            if indexed
                .insert(principal.user_id.clone(), principal)
                .is_some()
            {
                return Err("duplicate principal user_id".into());
            }
        }
        Ok(Self {
            cluster_id,
            generation,
            principals: indexed,
            curve_keys,
        })
    }

    pub fn cluster_id(&self) -> u128 {
        self.cluster_id
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Resolve an authenticated ZAP identity and require an explicit
    /// permission. Unknown identities and unlisted operations are denied.
    pub fn authorize(&self, user_id: &str, permission: Permission) -> Option<&Principal> {
        self.principals
            .get(user_id)
            .filter(|p| p.permissions.contains(&permission))
    }

    /// Check whether a ZAP identity is present. This does not validate the
    /// cluster ID; callers must compare [`Self::cluster_id`] with local config.
    pub fn admits(&self, user_id: &str) -> bool {
        self.principals.contains_key(user_id)
    }

    /// Resolve the trusted principal bound to a presented CURVE public key.
    pub fn principal_for_curve_key(&self, key: &str) -> Option<&Principal> {
        self.curve_keys
            .get(key)
            .and_then(|user_id| self.principals.get(user_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> TrustPolicy {
        TrustPolicy::new(
            7,
            1,
            vec![Principal {
                user_id: "app:orders".into(),
                curve_public_key: "A".repeat(40),
                permissions: vec![Permission::ClientRead, Permission::ClientWrite],
            }],
        )
        .unwrap()
    }

    #[test]
    fn policy_denies_unknown_principals_and_unlisted_permissions() {
        let policy = policy();
        assert!(policy.admits("app:orders"));
        assert_eq!(
            policy
                .principal_for_curve_key(&"A".repeat(40))
                .unwrap()
                .user_id,
            "app:orders"
        );
        assert!(!policy.admits("node:2"));
        assert!(
            policy
                .authorize("app:orders", Permission::ClientWrite)
                .is_some()
        );
        assert!(
            policy
                .authorize("app:orders", Permission::PeerRpc)
                .is_none()
        );
        assert!(policy.authorize("forged", Permission::ClientRead).is_none());
    }

    #[test]
    fn policy_rejects_ambiguous_or_empty_entries() {
        assert!(TrustPolicy::new(7, 1, vec![]).is_ok());
        assert!(TrustPolicy::new(7, 0, vec![]).is_err());
        let p = Principal {
            user_id: "same".into(),
            curve_public_key: "A".repeat(40),
            permissions: vec![Permission::ClientRead],
        };
        assert!(TrustPolicy::new(7, 1, vec![p.clone(), p.clone()]).is_err());
        assert!(
            TrustPolicy::new(
                7,
                1,
                vec![Principal {
                    user_id: "empty".into(),
                    curve_public_key: "B".repeat(40),
                    permissions: vec![]
                }]
            )
            .is_err()
        );
        let duplicate_key = Principal {
            user_id: "other".into(),
            curve_public_key: "A".repeat(40),
            permissions: vec![Permission::ClientRead],
        };
        assert!(TrustPolicy::new(7, 1, vec![p.clone(), duplicate_key]).is_err());
        let malformed_key = Principal {
            user_id: "bad-key".into(),
            curve_public_key: "short".into(),
            permissions: vec![Permission::ClientRead],
        };
        assert!(TrustPolicy::new(7, 1, vec![malformed_key]).is_err());
    }
}
