//! Cluster-scoped principal policy used by authenticated transport admission
//! and operation authorization. Socket authentication and policy evaluation
//! are deliberately separate: a valid CURVE key alone grants no operation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

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
            if !is_z85_public_key(&principal.curve_public_key) {
                return Err(format!(
                    "principal {} CURVE public key must be 40 valid Z85 bytes",
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

    /// Resolve the trusted principal bound to a presented CURVE public key.
    pub fn principal_for_curve_key(&self, key: &str) -> Option<&Principal> {
        self.curve_keys
            .get(key)
            .and_then(|user_id| self.principals.get(user_id))
    }
}

fn is_z85_public_key(key: &str) -> bool {
    const Z85: &str =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
    if key.len() != 40 || !key.bytes().all(|byte| Z85.as_bytes().contains(&byte)) {
        return false;
    }
    key.as_bytes().chunks(5).all(|chunk| {
        chunk
            .iter()
            .try_fold(0u64, |value, byte| {
                let digit = Z85
                    .as_bytes()
                    .iter()
                    .position(|candidate| candidate == byte)? as u64;
                Some(value * 85 + digit)
            })
            .is_some_and(|value| value <= u32::MAX as u64)
    })
}

/// Owns the process-wide ZeroMQ Authentication Protocol handler. libzmq
/// consults this REP socket for every CURVE connection using this domain.
pub struct ZapAuthenticator {
    running: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    domain: String,
}

impl ZapAuthenticator {
    pub fn start(
        ctx: zmq::Context,
        policy: Arc<TrustPolicy>,
        cluster_id: u128,
    ) -> crate::Result<Self> {
        if zmq::has("curve") != Some(true) {
            return Err(crate::Error::Config(
                "libzmq was built without CURVE support".into(),
            ));
        }
        if policy.cluster_id() != cluster_id {
            return Err(crate::Error::Config(
                "trust policy cluster_id does not match node cluster_id".into(),
            ));
        }
        let domain = zap_domain(cluster_id);
        let socket = ctx.socket(zmq::REP).map_err(zmq_error)?;
        socket.set_linger(0).map_err(zmq_error)?;
        socket.set_rcvtimeo(100).map_err(zmq_error)?;
        socket.set_sndtimeo(100).map_err(zmq_error)?;
        socket.bind("inproc://zeromq.zap.01").map_err(zmq_error)?;

        let running = Arc::new(AtomicBool::new(true));
        let worker_running = running.clone();
        let worker_domain = domain.clone();
        let worker = std::thread::Builder::new()
            .name("zmq-zap-authenticator".into())
            .spawn(move || {
                while worker_running.load(Ordering::Acquire) {
                    let request = match socket.recv_multipart(0) {
                        Ok(request) => request,
                        Err(zmq::Error::EAGAIN) => continue,
                        Err(_) => break,
                    };
                    let response = zap_response(&request, &worker_domain, &policy);
                    let _ = socket.send_multipart(response, 0);
                }
            })
            .map_err(crate::Error::Io)?;
        Ok(Self {
            running,
            worker: Some(worker),
            domain,
        })
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }
}

impl Drop for ZapAuthenticator {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn zap_domain(cluster_id: u128) -> String {
    format!("dal-{cluster_id:032x}")
}

fn zap_response(request: &[Vec<u8>], domain: &str, policy: &TrustPolicy) -> Vec<Vec<u8>> {
    let version = request.first().cloned().unwrap_or_default();
    let sequence = request.get(1).cloned().unwrap_or_default();
    let principal = if request.len() == 7
        && request[0] == b"1.0"
        && request[2] == domain.as_bytes()
        && request[5] == b"CURVE"
    {
        let key = match request[6].len() {
            32 => zmq::z85_encode(&request[6]).ok(),
            40 => String::from_utf8(request[6].clone()).ok(),
            _ => None,
        };
        key.and_then(|key| policy.principal_for_curve_key(&key))
    } else {
        None
    };
    match principal {
        Some(principal) => vec![
            version,
            sequence,
            b"200".to_vec(),
            b"OK".to_vec(),
            principal.user_id.as_bytes().to_vec(),
            Vec::new(),
        ],
        None => vec![
            version,
            sequence,
            b"400".to_vec(),
            b"DENIED".to_vec(),
            Vec::new(),
            Vec::new(),
        ],
    }
}

fn zmq_error(error: zmq::Error) -> crate::Error {
    crate::Error::Io(std::io::Error::other(format!("ZeroMQ ZAP: {error}")))
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
        assert_eq!(
            policy
                .principal_for_curve_key(&"A".repeat(40))
                .unwrap()
                .user_id,
            "app:orders"
        );
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
    fn zap_authenticates_curve_keys_and_returns_the_policy_identity() {
        if zmq::has("curve") != Some(true) {
            return;
        }
        let ctx = zmq::Context::new();
        let server_keys = zmq::CurveKeyPair::new().unwrap();
        let client_keys = zmq::CurveKeyPair::new().unwrap();
        let principal = Principal {
            user_id: "app:orders".into(),
            curve_public_key: zmq::z85_encode(&client_keys.public_key).unwrap(),
            permissions: vec![Permission::ClientRead],
        };
        let policy = Arc::new(TrustPolicy::new(7, 1, vec![principal]).unwrap());
        let zap = ZapAuthenticator::start(ctx.clone(), policy, 7).unwrap();

        let server = ctx.socket(zmq::ROUTER).unwrap();
        server.set_linger(0).unwrap();
        server.set_curve_server(true).unwrap();
        server.set_curve_secretkey(&server_keys.secret_key).unwrap();
        server.set_zap_domain(zap.domain()).unwrap();
        server.bind("tcp://127.0.0.1:0").unwrap();
        let endpoint = server.get_last_endpoint().unwrap().unwrap();

        let client = ctx.socket(zmq::DEALER).unwrap();
        client.set_linger(0).unwrap();
        client.set_rcvtimeo(2_000).unwrap();
        client.set_curve_publickey(&client_keys.public_key).unwrap();
        client.set_curve_secretkey(&client_keys.secret_key).unwrap();
        client.set_curve_serverkey(&server_keys.public_key).unwrap();
        client.connect(&endpoint).unwrap();
        client.send(b"ping".as_slice(), 0).unwrap();
        let identity = server.recv_msg(0).unwrap().to_vec();
        let mut payload = server.recv_msg(0).unwrap();
        assert_eq!(payload.gets("User-Id"), Some("app:orders"));
        server
            .send_multipart([identity, b"pong".to_vec()], 0)
            .unwrap();
        assert_eq!(client.recv_bytes(0).unwrap(), b"pong");
        drop(zap);
    }

    #[test]
    fn zap_admission_is_bound_to_domain_and_trusted_key() {
        let key = "A".repeat(40);
        let policy = TrustPolicy::new(
            7,
            1,
            vec![Principal {
                user_id: "node:1".into(),
                curve_public_key: key.clone(),
                permissions: vec![Permission::PeerRpc],
            }],
        )
        .unwrap();
        let domain = zap_domain(7);
        let request = vec![
            b"1.0".to_vec(),
            b"9".to_vec(),
            domain.as_bytes().to_vec(),
            b"127.0.0.1".to_vec(),
            b"routing-id".to_vec(),
            b"CURVE".to_vec(),
            zmq::z85_decode(&key).unwrap(),
        ];
        let allowed = zap_response(&request, &domain, &policy);
        assert_eq!(allowed[2], b"200");
        assert_eq!(allowed[4], b"node:1");

        let mut wrong_domain = request.clone();
        wrong_domain[2] = b"another-cluster".to_vec();
        assert_eq!(zap_response(&wrong_domain, &domain, &policy)[2], b"400");

        let mut unknown_key = request;
        unknown_key[6] = zmq::z85_decode(&"B".repeat(40)).unwrap();
        assert_eq!(zap_response(&unknown_key, &domain, &policy)[2], b"400");
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
        let invalid_z85 = Principal {
            user_id: "invalid-z85".into(),
            curve_public_key: "~".repeat(40),
            permissions: vec![Permission::ClientRead],
        };
        assert!(TrustPolicy::new(7, 1, vec![invalid_z85]).is_err());
        let out_of_range = Principal {
            user_id: "out-of-range".into(),
            curve_public_key: "#".repeat(40),
            permissions: vec![Permission::ClientRead],
        };
        assert!(TrustPolicy::new(7, 1, vec![out_of_range]).is_err());
    }
}
