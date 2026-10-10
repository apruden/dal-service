//! Cluster-scoped principal policy used by authenticated transport admission
//! and operation authorization. Socket authentication and policy evaluation
//! are deliberately separate: a valid CURVE key alone grants no operation.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use serde::Deserialize;
use sha2::{Digest, Sha256};

/// DAL-level permissions attached to an out-of-band authenticated principal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    ClientRead,
    ClientWrite,
    Search,
    PeerRpc,
    Operator,
}

/// Identity and permissions supplied by the trusted cluster policy. The
/// `user_id` is the ZAP identity; it must never be copied from an RPC payload.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Principal {
    pub user_id: String,
    /// Z85-encoded CURVE public key (40 ASCII bytes), provisioned out of band.
    pub curve_public_key: String,
    pub permissions: Vec<Permission>,
    /// Stable client identities owned by this principal, as hexadecimal
    /// strings (`0x` followed by 1..=32 digits). Used to prevent application
    /// principals from impersonating each other's mutation streams.
    #[serde(default)]
    pub client_ids: Vec<String>,
}

/// Immutable policy snapshot. Replacing the snapshot is the atomic unit for
/// future policy reloads; callers can retain its generation for diagnostics.
#[derive(Debug, Clone)]
pub struct TrustPolicy {
    cluster_id: u128,
    generation: u64,
    principals: BTreeMap<String, Principal>,
    curve_keys: BTreeMap<String, String>,
    client_owners: BTreeMap<u128, String>,
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
        let mut client_owners = BTreeMap::new();
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
            if !principal.client_ids.is_empty()
                && !principal.permissions.contains(&Permission::ClientWrite)
            {
                return Err(format!(
                    "principal {} owns client_ids but lacks client_write permission",
                    principal.user_id
                ));
            }
            if !is_z85_public_key(&principal.curve_public_key) {
                return Err(format!(
                    "principal {} CURVE public key must be 40 valid Z85 bytes",
                    principal.user_id
                ));
            }
            for client_id in &principal.client_ids {
                let hex = client_id.strip_prefix("0x").ok_or_else(|| {
                    format!(
                        "principal {} client_ids must use 0x-prefixed hexadecimal",
                        principal.user_id
                    )
                })?;
                if hex.is_empty()
                    || hex.len() > 32
                    || !hex.bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    return Err(format!(
                        "principal {} contains an invalid client id",
                        principal.user_id
                    ));
                }
                let parsed = u128::from_str_radix(hex, 16).map_err(|_| {
                    format!(
                        "principal {} contains an invalid client id",
                        principal.user_id
                    )
                })?;
                if client_owners.len() >= 100_000 {
                    return Err(
                        "trust policy exceeds the 100000 client identity cluster limit".into(),
                    );
                }
                if client_owners
                    .insert(parsed, principal.user_id.clone())
                    .is_some()
                {
                    return Err("client_id is assigned to multiple principals".into());
                }
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
            client_owners,
        })
    }

    /// Load the operator-controlled policy document. The file contains only
    /// public keys and permission bindings; private keys are loaded separately.
    pub fn load(path: &Path) -> crate::Result<Self> {
        let bytes = read_protected_file(path, 64 * 1024 * 1024)?;
        let document: TrustPolicyFile = serde_json::from_slice(&bytes).map_err(|error| {
            crate::Error::Config(format!("trust policy {}: {error}", path.display()))
        })?;
        let cluster_id = u128::from_str_radix(
            document
                .cluster_id
                .strip_prefix("0x")
                .unwrap_or(&document.cluster_id),
            16,
        )
        .map_err(|error| crate::Error::Config(format!("trust policy cluster_id: {error}")))?;
        Self::new(cluster_id, document.generation, document.principals)
            .map_err(crate::Error::Config)
    }

    pub fn cluster_id(&self) -> u128 {
        self.cluster_id
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Stable digest of the effective public trust policy. Input ordering and
    /// JSON formatting do not affect this identity.
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"dal-trust-policy-v1");
        hash.update(self.cluster_id.to_be_bytes());
        hash.update(self.generation.to_be_bytes());
        hash.update((self.principals.len() as u64).to_be_bytes());
        for principal in self.principals.values() {
            for value in [
                principal.user_id.as_bytes(),
                principal.curve_public_key.as_bytes(),
            ] {
                hash.update((value.len() as u64).to_be_bytes());
                hash.update(value);
            }
            let mut permissions: Vec<u8> = principal
                .permissions
                .iter()
                .map(|permission| match permission {
                    Permission::ClientRead => 1,
                    Permission::ClientWrite => 2,
                    Permission::Search => 3,
                    Permission::PeerRpc => 4,
                    Permission::Operator => 5,
                })
                .collect();
            permissions.sort_unstable();
            permissions.dedup();
            hash.update((permissions.len() as u64).to_be_bytes());
            hash.update(&permissions);
        }
        hash.update((self.client_owners.len() as u64).to_be_bytes());
        for (client_id, user_id) in &self.client_owners {
            hash.update(client_id.to_be_bytes());
            hash.update((user_id.len() as u64).to_be_bytes());
            hash.update(user_id.as_bytes());
        }
        hash.finalize().into()
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

    pub fn owns_client_id(&self, user_id: &str, client_id: u128) -> bool {
        self.client_owners
            .get(&client_id)
            .is_some_and(|owner| owner == user_id)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustPolicyFile {
    cluster_id: String,
    generation: u64,
    principals: Vec<Principal>,
}

/// Read a protected Z85-encoded CURVE key file and decode it to libzmq's
/// expected 32-byte binary form. The returned bytes must not be logged.
pub fn load_curve_key(path: &Path) -> crate::Result<[u8; 32]> {
    let contents = read_protected_file(path, 128)?;
    let text = std::str::from_utf8(&contents)
        .map_err(|_| crate::Error::Config(format!("CURVE key {} is not UTF-8", path.display())))?;
    let encoded = text.trim_end_matches(['\r', '\n']);
    if !is_z85_public_key(encoded) {
        return Err(crate::Error::Config(format!(
            "CURVE key {} is not a valid 40-byte Z85 key",
            path.display()
        )));
    }
    let decoded = zmq::z85_decode(encoded).map_err(|_| {
        crate::Error::Config(format!("CURVE key {} cannot be decoded", path.display()))
    })?;
    decoded
        .try_into()
        .map_err(|_| crate::Error::Config(format!("CURVE key {} has wrong length", path.display())))
}

fn read_protected_file(path: &Path, max_bytes: u64) -> crate::Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path).map_err(crate::Error::Io)?;
    if !metadata.file_type().is_file() {
        return Err(crate::Error::Config(format!(
            "security file {} must be a regular file",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(crate::Error::Config(format!(
                "security file {} must not be accessible by group or other users",
                path.display()
            )));
        }
    }
    if metadata.len() > max_bytes {
        return Err(crate::Error::Config(format!(
            "security file {} exceeds the {} byte limit",
            path.display(),
            max_bytes
        )));
    }
    let file = std::fs::File::open(path).map_err(crate::Error::Io)?;
    let mut contents = Vec::with_capacity(metadata.len() as usize);
    file.take(max_bytes + 1)
        .read_to_end(&mut contents)
        .map_err(crate::Error::Io)?;
    if contents.len() as u64 > max_bytes {
        return Err(crate::Error::Config(format!(
            "security file {} exceeds the {} byte limit",
            path.display(),
            max_bytes
        )));
    }
    Ok(contents)
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

/// Owns the ZeroMQ Authentication Protocol handler for one ZeroMQ context.
/// libzmq consults this REP socket for CURVE connections in this context.
/// Keep it alive until every CURVE server socket in the context is closed;
/// libzmq builds without ZAP domain enforcement may admit clients if this
/// endpoint is removed while those servers remain active.
pub struct ZapAuthenticator {
    context: zmq::Context,
    running: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<String>>>,
    worker: Option<JoinHandle<()>>,
    domain: String,
}

static ZAP_DENIALS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Authentication decisions rejected by this process's CURVE/ZAP handler.
pub fn authentication_denials() -> u64 {
    ZAP_DENIALS.load(Ordering::Relaxed)
}

/// Outbound CURVE identity and the pinned server public keys for destination
/// endpoints. Missing pins are configuration errors; secure transports never
/// fall back to plaintext for an unknown address.
#[derive(Clone)]
pub struct CurveClientConfig {
    public_key: [u8; 32],
    secret_key: [u8; 32],
    server_keys: BTreeMap<String, [u8; 32]>,
}

impl CurveClientConfig {
    pub fn new(
        public_key: [u8; 32],
        secret_key: [u8; 32],
        server_keys: BTreeMap<String, [u8; 32]>,
    ) -> crate::Result<Self> {
        if zmq::has("curve") != Some(true) {
            return Err(crate::Error::Config(
                "libzmq was built without CURVE support".into(),
            ));
        }
        if server_keys.is_empty() || server_keys.keys().any(String::is_empty) {
            return Err(crate::Error::Config(
                "CURVE client requires pinned server keys for its destinations".into(),
            ));
        }
        Ok(Self {
            public_key,
            secret_key,
            server_keys,
        })
    }

    pub(crate) fn client_public_key(&self) -> &[u8; 32] {
        &self.public_key
    }

    pub(crate) fn client_secret_key(&self) -> &[u8; 32] {
        &self.secret_key
    }

    pub(crate) fn server_key(&self, addr: &str) -> crate::Result<&[u8; 32]> {
        self.server_keys.get(addr).ok_or_else(|| {
            crate::Error::Config(format!("no pinned CURVE server key for endpoint {addr}"))
        })
    }
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
        let context = ctx.clone();
        let socket = ctx.socket(zmq::REP).map_err(zmq_error)?;
        socket.set_linger(0).map_err(zmq_error)?;
        socket.set_rcvtimeo(100).map_err(zmq_error)?;
        socket.set_sndtimeo(100).map_err(zmq_error)?;
        socket.bind("inproc://zeromq.zap.01").map_err(zmq_error)?;

        let running = Arc::new(AtomicBool::new(true));
        let worker_running = running.clone();
        let failure = Arc::new(Mutex::new(None));
        let worker_failure = failure.clone();
        let worker_domain = domain.clone();
        let worker = std::thread::Builder::new()
            .name("zmq-zap-authenticator".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    while worker_running.load(Ordering::Acquire) {
                        let request = match socket.recv_multipart(0) {
                            Ok(request) => request,
                            Err(zmq::Error::EAGAIN | zmq::Error::EINTR) => continue,
                            Err(error) => {
                                if worker_running.load(Ordering::Acquire) {
                                    fail_closed_until_stopped(
                                        &socket,
                                        &worker_running,
                                        &worker_failure,
                                        format!("ZAP receive failed: {error}"),
                                        None,
                                    );
                                }
                                break;
                            }
                        };
                        let response = zap_response(&request, &worker_domain, &policy);
                        if response.get(2).is_some_and(|status| status == b"400") {
                            ZAP_DENIALS.fetch_add(1, Ordering::Relaxed);
                        }
                        if let Err(error) = send_zap_response(&socket, &response, &worker_running) {
                            fail_closed_until_stopped(
                                &socket,
                                &worker_running,
                                &worker_failure,
                                format!("ZAP reply failed: {error}"),
                                Some(&response),
                            );
                            break;
                        }
                    }
                }));
                if result.is_err() && worker_running.load(Ordering::Acquire) {
                    fail_closed_until_stopped(
                        &socket,
                        &worker_running,
                        &worker_failure,
                        "ZAP worker panicked".into(),
                        None,
                    );
                }
            })
            .map_err(crate::Error::Io)?;
        Ok(Self {
            context,
            running,
            failure,
            worker: Some(worker),
            domain,
        })
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// The exact context that owns the ZAP endpoint. Secure sockets must be
    /// created from this context; ZeroMQ inproc authentication is context-local.
    pub fn context(&self) -> &zmq::Context {
        &self.context
    }

    /// Return an error after any fatal worker failure. Runtime supervision
    /// must use this to fail closed instead of continuing without ZAP.
    pub fn ensure_healthy(&self) -> crate::Result<()> {
        match self.failure.lock().unwrap().as_ref() {
            Some(reason) => Err(crate::Error::Config(format!(
                "CURVE ZAP authenticator failed: {reason}"
            ))),
            None => Ok(()),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.failure.lock().unwrap().is_none()
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

/// Keep the ZAP endpoint registered while the authenticator is unhealthy.
/// libzmq builds without ZAP domain enforcement can otherwise accept CURVE
/// connections after the handler socket is dropped.
fn fail_closed_until_stopped(
    socket: &zmq::Socket,
    running: &AtomicBool,
    failure: &Mutex<Option<String>>,
    reason: String,
    pending_response: Option<&[Vec<u8>]>,
) {
    *failure.lock().unwrap() = Some(reason);
    if let Some(response) = pending_response
        && !send_until_stopped(socket, response, running)
    {
        return;
    }
    while running.load(Ordering::Acquire) {
        match socket.recv_multipart(0) {
            Ok(request) => {
                let denial = zap_denial(&request);
                ZAP_DENIALS.fetch_add(1, Ordering::Relaxed);
                if !send_until_stopped(socket, &denial, running) {
                    return;
                }
            }
            Err(zmq::Error::EAGAIN | zmq::Error::EINTR) => {}
            // A terminating context is already taking all its sockets down;
            // return so context destruction cannot deadlock on this worker.
            Err(zmq::Error::ETERM) => return,
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(25)),
        }
    }
}

fn send_until_stopped(socket: &zmq::Socket, response: &[Vec<u8>], running: &AtomicBool) -> bool {
    while running.load(Ordering::Acquire) {
        match send_zap_response(socket, response, running) {
            Ok(()) => return true,
            Err(zmq::Error::ETERM) => return false,
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(25)),
        }
    }
    false
}

fn zap_denial(request: &[Vec<u8>]) -> Vec<Vec<u8>> {
    vec![
        request.first().cloned().unwrap_or_default(),
        request.get(1).cloned().unwrap_or_default(),
        b"400".to_vec(),
        b"DENIED".to_vec(),
        Vec::new(),
        Vec::new(),
    ]
}

fn send_zap_response(
    socket: &zmq::Socket,
    response: &[Vec<u8>],
    running: &AtomicBool,
) -> zmq::Result<()> {
    for (index, frame) in response.iter().enumerate() {
        let flags = if index + 1 < response.len() {
            zmq::SNDMORE
        } else {
            0
        };
        loop {
            match socket.send(frame, flags) {
                Err(zmq::Error::EINTR | zmq::Error::EAGAIN) => {
                    if !running.load(Ordering::Acquire) {
                        return Err(zmq::Error::ETERM);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                result => {
                    result?;
                    break;
                }
            }
        }
    }
    Ok(())
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
                client_ids: Vec::new(),
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
    fn policy_binds_client_mutation_identities_to_one_principal() {
        let owner = Principal {
            user_id: "app:orders".into(),
            curve_public_key: "A".repeat(40),
            permissions: vec![Permission::ClientWrite],
            client_ids: vec!["0x1234".into()],
        };
        let other = Principal {
            user_id: "app:billing".into(),
            curve_public_key: "B".repeat(40),
            permissions: vec![Permission::ClientWrite],
            client_ids: vec!["0x5678".into()],
        };
        let policy = TrustPolicy::new(7, 1, vec![owner.clone(), other]).unwrap();
        assert!(policy.owns_client_id("app:orders", 0x1234));
        assert!(!policy.owns_client_id("app:billing", 0x1234));
        assert!(!policy.owns_client_id("app:orders", 0x5678));
        let mut duplicate = owner.clone();
        duplicate.user_id = "app:other".into();
        duplicate.curve_public_key = "C".repeat(40);
        assert!(TrustPolicy::new(7, 1, vec![owner, duplicate]).is_err());
        let mut malformed = Principal {
            user_id: "app:malformed".into(),
            curve_public_key: "D".repeat(40),
            permissions: vec![Permission::ClientWrite],
            client_ids: vec!["1234".into()],
        };
        assert!(TrustPolicy::new(7, 1, vec![malformed.clone()]).is_err());
        malformed.client_ids = vec![format!("0x{}", "f".repeat(33))];
        assert!(TrustPolicy::new(7, 1, vec![malformed]).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn curve_router_and_dealer_preserve_the_zap_identity() {
        if zmq::has("curve") != Some(true) {
            return;
        }
        let ctx = zmq::Context::new();
        let server_keys = zmq::CurveKeyPair::new().unwrap();
        let client_keys = zmq::CurveKeyPair::new().unwrap();
        let (identity_tx, identity_rx) = std::sync::mpsc::channel();
        struct IdentityEcho(std::sync::mpsc::Sender<Option<String>>);
        impl crate::transport::Server for IdentityEcho {
            async fn serve(
                &self,
                request: crate::transport::codec::Envelope,
            ) -> crate::transport::codec::Envelope {
                crate::transport::codec::Envelope::new(
                    request.cluster_id,
                    request.msg_type,
                    request.group_id,
                    request.request_id,
                    b"pong".to_vec(),
                )
            }

            async fn serve_with_context(
                &self,
                request: crate::transport::codec::Envelope,
                context: crate::transport::RequestContext,
            ) -> crate::transport::codec::Envelope {
                let _ = self.0.send(context.authenticated_user_id);
                self.serve(request).await
            }
        }
        let principal = Principal {
            user_id: "app:orders".into(),
            curve_public_key: zmq::z85_encode(&client_keys.public_key).unwrap(),
            permissions: vec![Permission::ClientRead],
            client_ids: Vec::new(),
        };
        let policy = Arc::new(TrustPolicy::new(7, 1, vec![principal]).unwrap());
        let zap = Arc::new(ZapAuthenticator::start(ctx, policy, 7).unwrap());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let endpoint = format!("tcp://127.0.0.1:{port}");
        let server = crate::transport::router::ZmqServer::bind_curve(
            &endpoint,
            Arc::new(IdentityEcho(identity_tx)),
            crate::transport::codec::Lane::Control,
            server_keys.secret_key,
            zap,
        )
        .unwrap();
        let client_config = CurveClientConfig::new(
            client_keys.public_key,
            client_keys.secret_key,
            BTreeMap::from([(endpoint.clone(), server_keys.public_key)]),
        )
        .unwrap();
        let transport = crate::transport::dealer::ZmqTransport::new_curve(
            zmq::Context::new(),
            std::time::Duration::from_secs(2),
            crate::transport::codec::Lane::Control,
            client_config,
        );
        let request = crate::transport::codec::Envelope::new(
            7,
            crate::transport::codec::MsgType::MetaQuery,
            crate::types::GroupId::Meta,
            17,
            b"ping".to_vec(),
        );
        let reply = crate::transport::Transport::call(&transport, &endpoint, request)
            .await
            .unwrap();
        assert_eq!(reply.payload, b"pong");
        assert_eq!(
            identity_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap(),
            Some("app:orders".into())
        );
        drop(transport);
        drop(server);
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
                client_ids: Vec::new(),
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
    fn failed_authenticator_keeps_its_zap_endpoint_bound_until_shutdown() {
        let ctx = zmq::Context::new();
        let endpoint = "inproc://dal-zap-fail-closed-test";
        let socket = ctx.socket(zmq::REP).unwrap();
        socket.set_rcvtimeo(100).unwrap();
        socket.set_sndtimeo(100).unwrap();
        socket.bind(endpoint).unwrap();
        let running = Arc::new(AtomicBool::new(true));
        let worker_running = running.clone();
        let failure = Arc::new(Mutex::new(None));
        let worker_failure = failure.clone();
        let worker = std::thread::spawn(move || {
            fail_closed_until_stopped(
                &socket,
                &worker_running,
                &worker_failure,
                "injected failure".into(),
                None,
            );
        });
        let client = ctx.socket(zmq::REQ).unwrap();
        client.set_rcvtimeo(1_000).unwrap();
        client.connect(endpoint).unwrap();
        client
            .send_multipart(
                [
                    b"1.0".as_slice(),
                    b"42",
                    b"domain",
                    b"127.0.0.1",
                    b"id",
                    b"CURVE",
                    b"credential",
                ],
                0,
            )
            .unwrap();
        let denial = client.recv_multipart(0).unwrap();
        assert_eq!(denial[2], b"400");
        let probe = ctx.socket(zmq::REP).unwrap();
        assert!(probe.bind(endpoint).is_err());
        running.store(false, Ordering::Release);
        worker.join().unwrap();
        assert_eq!(failure.lock().unwrap().as_deref(), Some("injected failure"));
    }

    #[test]
    fn policy_rejects_ambiguous_or_empty_entries() {
        assert!(TrustPolicy::new(7, 1, vec![]).is_ok());
        assert!(TrustPolicy::new(7, 0, vec![]).is_err());
        let p = Principal {
            user_id: "same".into(),
            curve_public_key: "A".repeat(40),
            permissions: vec![Permission::ClientRead],
            client_ids: Vec::new(),
        };
        assert!(TrustPolicy::new(7, 1, vec![p.clone(), p.clone()]).is_err());
        assert!(
            TrustPolicy::new(
                7,
                1,
                vec![Principal {
                    user_id: "empty".into(),
                    curve_public_key: "B".repeat(40),
                    permissions: vec![],
                    client_ids: Vec::new(),
                }]
            )
            .is_err()
        );
        let duplicate_key = Principal {
            user_id: "other".into(),
            curve_public_key: "A".repeat(40),
            permissions: vec![Permission::ClientRead],
            client_ids: Vec::new(),
        };
        assert!(TrustPolicy::new(7, 1, vec![p.clone(), duplicate_key]).is_err());
        let malformed_key = Principal {
            user_id: "bad-key".into(),
            curve_public_key: "short".into(),
            permissions: vec![Permission::ClientRead],
            client_ids: Vec::new(),
        };
        assert!(TrustPolicy::new(7, 1, vec![malformed_key]).is_err());
        let invalid_z85 = Principal {
            user_id: "invalid-z85".into(),
            curve_public_key: "~".repeat(40),
            permissions: vec![Permission::ClientRead],
            client_ids: Vec::new(),
        };
        assert!(TrustPolicy::new(7, 1, vec![invalid_z85]).is_err());
        let out_of_range = Principal {
            user_id: "out-of-range".into(),
            curve_public_key: "#".repeat(40),
            permissions: vec![Permission::ClientRead],
            client_ids: Vec::new(),
        };
        assert!(TrustPolicy::new(7, 1, vec![out_of_range]).is_err());
    }

    #[test]
    fn protected_policy_and_curve_key_files_load_without_exposing_secrets() {
        let key = "A".repeat(40);
        let policy_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            policy_file.path(),
            format!(
                r#"{{"cluster_id":"0x7","generation":3,"principals":[{{"user_id":"node:1","curve_public_key":"{key}","permissions":["peer_rpc"]}}]}}"#
            ),
        )
        .unwrap();
        let key_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(key_file.path(), format!("{key}\n")).unwrap();
        #[cfg(unix)]
        for path in [policy_file.path(), key_file.path()] {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let policy = TrustPolicy::load(policy_file.path()).unwrap();
        let expected_key: [u8; 32] = zmq::z85_decode(&key).unwrap().try_into().unwrap();
        assert_eq!(policy.cluster_id(), 7);
        assert_eq!(policy.generation(), 3);
        assert_eq!(
            policy
                .authorize("node:1", Permission::PeerRpc)
                .unwrap()
                .user_id,
            "node:1"
        );
        assert_eq!(load_curve_key(key_file.path()).unwrap(), expected_key);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(key_file.path(), std::fs::Permissions::from_mode(0o644))
                .unwrap();
            assert!(load_curve_key(key_file.path()).is_err());
        }
    }
}
