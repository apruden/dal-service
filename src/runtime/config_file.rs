//! JSON config files for the `dal` binary.
//!
//! Two shapes mirror [`crate::config`]: a per-node file (`dal run --config`)
//! and a shared cluster-init file (`dal init --cluster`). `cluster_id` is a
//! hex string on disk because JSON tooling cannot represent u128 reliably.
//! Timeouts are milliseconds. Every node given the same init file derives a
//! byte-identical [`BootstrapDescriptor`] (placements are deterministic), so
//! the durable bootstrap records written independently on each node agree.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;

use crate::config::{InitConfig, NodeConfig, Timeouts, parse_partition_count};
use crate::error::{Error, Result};
use crate::meta::bootstrap::{BootstrapDescriptor, DirEntry};
use crate::transport::security::{Permission, TrustPolicy, load_curve_key};
use crate::types::{
    ClusterConfig, ClusterId, HashSpec, MAX_ENDPOINT_BYTES, NodeId, PROTOCOL_VERSION,
};

fn parse_cluster_id(s: &str) -> Result<ClusterId> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    u128::from_str_radix(hex, 16)
        .map_err(|e| Error::Config(format!("cluster_id must be a hex u128: {e}")))
}

/// Render a cluster id the way config files and `/status` spell it.
pub fn cluster_id_hex(id: ClusterId) -> String {
    format!("{id:#x}")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimeoutsFile {
    suspect_ms: u64,
    down_ms: u64,
    request_ms: u64,
    #[serde(default = "default_shutdown_ms")]
    shutdown_ms: u64,
}

fn default_shutdown_ms() -> u64 {
    Timeouts::default().shutdown.as_millis() as u64
}

impl Default for TimeoutsFile {
    fn default() -> Self {
        let t = Timeouts::default();
        TimeoutsFile {
            suspect_ms: t.suspect.as_millis() as u64,
            down_ms: t.down.as_millis() as u64,
            request_ms: t.request.as_millis() as u64,
            shutdown_ms: t.shutdown.as_millis() as u64,
        }
    }
}

impl From<TimeoutsFile> for Timeouts {
    fn from(f: TimeoutsFile) -> Timeouts {
        Timeouts {
            suspect: std::time::Duration::from_millis(f.suspect_ms),
            down: std::time::Duration::from_millis(f.down_ms),
            request: std::time::Duration::from_millis(f.request_ms),
            shutdown: std::time::Duration::from_millis(f.shutdown_ms),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeFile {
    cluster_id: String,
    node_id: NodeId,
    control_addr: String,
    bulk_addr: String,
    #[serde(default)]
    http_addr: Option<String>,
    #[serde(default)]
    seeds: Vec<String>,
    data_dir: PathBuf,
    #[serde(default)]
    timeouts: TimeoutsFile,
    #[serde(default)]
    security: Option<NodeSecurityFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeSecurityFile {
    trust_policy_file: PathBuf,
    /// Absolute, durable marker kept outside data_dir/backups.
    trust_generation_file: PathBuf,
    curve_public_key_file: PathBuf,
    curve_secret_key_file: PathBuf,
    /// Endpoint to trusted policy principal. Endpoints are provisioned out of
    /// band; redirects and directory data never create pins.
    server_key_pins: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorSecurityFile {
    cluster_id: String,
    trust_policy_file: PathBuf,
    /// Absolute, durable marker kept outside data backups.
    trust_generation_file: PathBuf,
    user_id: String,
    curve_public_key_file: PathBuf,
    curve_secret_key_file: PathBuf,
    /// Endpoint to trusted node principal; redirects never create pins.
    server_key_pins: BTreeMap<String, String>,
}

/// Validated node credentials loaded from protected files and an operator
/// policy. This value intentionally has no `Debug` implementation because it
/// contains the private CURVE secret key.
pub struct LoadedNodeSecurity {
    pub policy: Arc<TrustPolicy>,
    pub user_id: String,
    pub public_key: [u8; 32],
    pub secret_key: [u8; 32],
    pub server_keys: BTreeMap<String, [u8; 32]>,
    server_key_principals: BTreeMap<String, String>,
}

impl LoadedNodeSecurity {
    pub fn server_key_principals(&self) -> &BTreeMap<String, String> {
        &self.server_key_principals
    }
}

/// Operator credentials and out-of-band node pins loaded from a dedicated
/// operator auth file. This deliberately contains no node service identity.
pub struct LoadedOperatorSecurity {
    pub policy: TrustPolicy,
    pub user_id: String,
    pub public_key: [u8; 32],
    pub secret_key: [u8; 32],
    pub server_keys: BTreeMap<String, [u8; 32]>,
    server_key_principals: BTreeMap<String, String>,
}

impl LoadedOperatorSecurity {
    /// Require the operator and joining node to use the same installed policy
    /// generation, and confirm that the policy binds the candidate key to the
    /// declared node identity before the registration RPC is sent.
    pub fn validate_joining_node(
        &self,
        candidate: &LoadedNodeSecurity,
        node_id: NodeId,
    ) -> Result<()> {
        if self.policy.generation() != candidate.policy.generation() {
            return Err(Error::Config(
                "operator and candidate node trust-policy generations differ".into(),
            ));
        }
        let encoded = zmq::z85_encode(&candidate.public_key)
            .map_err(|_| Error::Config("candidate CURVE public key cannot be encoded".into()))?;
        let expected = format!("node:{node_id}");
        match self.policy.principal_for_curve_key(&encoded) {
            Some(principal)
                if principal.user_id == expected
                    && principal.permissions.contains(&Permission::PeerRpc) =>
            {
                Ok(())
            }
            _ => Err(Error::Config(format!(
                "operator trust policy must bind the candidate key to {expected} with peer_rpc permission"
            ))),
        }
    }

    pub fn validate_descriptor(&self, desc: &BootstrapDescriptor) -> Result<()> {
        for entry in &desc.directory {
            let expected = format!("node:{}", entry.node_id);
            for endpoint in [&entry.control_addr, &entry.bulk_addr] {
                if !self.server_keys.contains_key(endpoint)
                    || self.server_key_principals.get(endpoint) != Some(&expected)
                {
                    return Err(Error::Config(format!(
                        "operator server_key_pins must bind {endpoint} to {expected}"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Load a dedicated operator credential file. Its public key must resolve to
/// an explicit `operator` principal in the cluster trust policy.
pub fn load_operator_security(
    path: &Path,
    cluster_id: ClusterId,
) -> Result<LoadedOperatorSecurity> {
    let text = std::fs::read_to_string(path)?;
    let file: OperatorSecurityFile = serde_json::from_str(&text)
        .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
    let configured_cluster_id = parse_cluster_id(&file.cluster_id)?;
    if configured_cluster_id != cluster_id {
        return Err(Error::Config(
            "operator auth file cluster_id does not match target cluster".into(),
        ));
    }
    let resolve = |referenced: PathBuf| {
        if referenced.is_absolute() {
            referenced
        } else {
            path.parent()
                .unwrap_or_else(|| Path::new("."))
                .join(referenced)
        }
    };
    let policy = TrustPolicy::load(&resolve(file.trust_policy_file))?;
    if policy.cluster_id() != cluster_id {
        return Err(Error::Config(
            "trust policy cluster_id does not match target cluster".into(),
        ));
    }
    ensure_policy_generation(&resolve(file.trust_generation_file), policy.generation())?;
    let public_key = load_curve_key(&resolve(file.curve_public_key_file))?;
    let secret_key = load_curve_key(&resolve(file.curve_secret_key_file))?;
    let encoded = zmq::z85_encode(&public_key)
        .map_err(|_| Error::Config("operator CURVE public key cannot be encoded".into()))?;
    let principal = policy
        .principal_for_curve_key(&encoded)
        .filter(|principal| {
            principal.user_id == file.user_id
                && principal.permissions.contains(&Permission::Operator)
        })
        .ok_or_else(|| {
            Error::Config(format!(
                "operator CURVE key is not bound to {} with operator permission",
                file.user_id
            ))
        })?;
    let user_id = principal.user_id.clone();
    let mut server_keys = BTreeMap::new();
    let mut server_key_principals = BTreeMap::new();
    for (endpoint, principal_id) in file.server_key_pins {
        if endpoint.is_empty() || endpoint.len() > MAX_ENDPOINT_BYTES {
            return Err(Error::Config(
                "operator server_key_pins contains an invalid endpoint".into(),
            ));
        }
        let principal = policy
            .authorize(&principal_id, Permission::PeerRpc)
            .ok_or_else(|| {
                Error::Config(format!(
                    "server key pin {endpoint} references unknown or untrusted node {principal_id}"
                ))
            })?;
        let key = zmq::z85_decode(&principal.curve_public_key)
            .map_err(|_| Error::Config("trust policy contains an invalid CURVE public key".into()))?
            .try_into()
            .map_err(|_| Error::Config("trust policy CURVE public key has wrong length".into()))?;
        server_keys.insert(endpoint.clone(), key);
        server_key_principals.insert(endpoint, principal_id);
    }
    Ok(LoadedOperatorSecurity {
        policy,
        user_id,
        public_key,
        secret_key,
        server_keys,
        server_key_principals,
    })
}

/// Persist the highest trust-policy generation observed by this deployment
/// outside the database directory. The lock and atomic replace prevent two
/// concurrent starts from racing a newer generation back to an older one.
fn ensure_policy_generation(path: &Path, generation: u64) -> Result<()> {
    if !path.is_absolute() {
        return Err(Error::Config(
            "trust_generation_file must be an absolute path outside data backups".into(),
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| Error::Config("trust_generation_file has no parent directory".into()))?;
    if !parent.is_dir() {
        return Err(Error::Config(format!(
            "trust generation directory {} must already exist",
            parent.display()
        )));
    }
    let lock_path = path.with_extension("lock");
    let lock = open_private_file(&lock_path, true)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        unsafe extern "C" {
            fn flock(fd: i32, operation: i32) -> i32;
        }
        // POSIX LOCK_EX; lock is released when the descriptor is dropped.
        if unsafe { flock(lock.as_raw_fd(), 2) } != 0 {
            return Err(Error::Io(std::io::Error::last_os_error()));
        }
    }
    let current = match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() {
                return Err(Error::Config(
                    "trust_generation_file must be a regular file".into(),
                ));
            }
            validate_private_mode(path, &metadata)?;
            let value = std::fs::read_to_string(path)?;
            value.trim().parse::<u64>().map_err(|error| {
                Error::Config(format!("trust generation marker is invalid: {error}"))
            })?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(Error::Io(error)),
    };
    if current > generation {
        return Err(Error::Config(format!(
            "trust policy generation {generation} is older than the persisted generation {current}"
        )));
    }
    if generation > current {
        let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
        let mut file = open_private_file(&temporary, false)?;
        use std::io::Write;
        writeln!(file, "{generation}")?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        std::fs::File::open(parent)?.sync_all()?;
    }
    drop(lock);
    Ok(())
}

fn open_private_file(path: &Path, create: bool) -> Result<std::fs::File> {
    use std::fs::OpenOptions;
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    if create {
        options.create(true);
    } else {
        options.create_new(true);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    validate_private_mode(path, &metadata)?;
    Ok(file)
}

fn validate_private_mode(path: &Path, metadata: &std::fs::Metadata) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Config(format!(
                "{} must not be accessible by group or other users",
                path.display()
            )));
        }
    }
    Ok(())
}

impl LoadedNodeSecurity {
    /// Ensure every bootstrap peer endpoint is pinned before any socket starts.
    pub fn validate_descriptor(
        &self,
        desc: &BootstrapDescriptor,
        local_node_id: NodeId,
    ) -> Result<()> {
        for entry in &desc.directory {
            let expected_principal = format!("node:{}", entry.node_id);
            for endpoint in [&entry.control_addr, &entry.bulk_addr] {
                if !self.server_keys.contains_key(endpoint) {
                    return Err(Error::Config(format!(
                        "security.server_key_pins has no key for node {} endpoint {endpoint}",
                        entry.node_id
                    )));
                }
                if self.server_key_principals.get(endpoint) != Some(&expected_principal) {
                    return Err(Error::Config(format!(
                        "security.server_key_pins endpoint {endpoint} is not bound to node {}",
                        entry.node_id
                    )));
                }
            }
        }
        let local_principal = format!("node:{local_node_id}");
        if self.user_id != local_principal {
            return Err(Error::Config(
                "local node identity differs from the configured node id".into(),
            ));
        }
        Ok(())
    }
}

/// Load and validate a per-node config file.
pub fn load_node_config(path: &Path) -> Result<NodeConfig> {
    let text = std::fs::read_to_string(path)?;
    let file: NodeFile = serde_json::from_str(&text)
        .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
    let cfg = NodeConfig {
        cluster_id: parse_cluster_id(&file.cluster_id)?,
        node_id: file.node_id,
        control_addr: file.control_addr,
        bulk_addr: file.bulk_addr,
        http_addr: file.http_addr,
        seeds: file.seeds,
        data_dir: file.data_dir,
        timeouts: file.timeouts.into(),
    };
    cfg.validate()?;
    Ok(cfg)
}

/// Load trust and key material referenced by a node config. All paths are
/// relative to the node config file unless absolute. Every peer pin resolves
/// through the trust policy rather than accepting self-reported keys.
pub fn load_node_security(path: &Path, cfg: &NodeConfig) -> Result<LoadedNodeSecurity> {
    load_node_security_inner(path, cfg, true)
}

/// Validate node credentials and pins without advancing the durable trust
/// generation marker. Used by the non-mutating `check-config` command.
pub fn validate_node_security(path: &Path, cfg: &NodeConfig) -> Result<()> {
    load_node_security_inner(path, cfg, false).map(drop)
}

fn load_node_security_inner(
    path: &Path,
    cfg: &NodeConfig,
    persist_generation: bool,
) -> Result<LoadedNodeSecurity> {
    let text = std::fs::read_to_string(path)?;
    let file: NodeFile = serde_json::from_str(&text)
        .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
    let security = file
        .security
        .ok_or_else(|| Error::Config("node config must include a security section".into()))?;
    let resolve = |referenced: PathBuf| {
        if referenced.is_absolute() {
            referenced
        } else {
            path.parent()
                .unwrap_or_else(|| Path::new("."))
                .join(referenced)
        }
    };
    let policy = Arc::new(TrustPolicy::load(&resolve(security.trust_policy_file))?);
    if policy.cluster_id() != cfg.cluster_id {
        return Err(Error::Config(
            "trust policy cluster_id does not match node cluster_id".into(),
        ));
    }
    if !security.trust_generation_file.is_absolute()
        || (cfg.data_dir.is_absolute() && security.trust_generation_file.starts_with(&cfg.data_dir))
    {
        return Err(Error::Config(
            "security.trust_generation_file must be an absolute path outside data_dir and its backups".into(),
        ));
    }
    let public_key = load_curve_key(&resolve(security.curve_public_key_file))?;
    let secret_key = load_curve_key(&resolve(security.curve_secret_key_file))?;
    let encoded_public_key = zmq::z85_encode(&public_key)
        .map_err(|_| Error::Config("node CURVE public key cannot be encoded".into()))?;
    let principal = policy
        .principal_for_curve_key(&encoded_public_key)
        .ok_or_else(|| Error::Config("node CURVE public key is absent from trust policy".into()))?;
    let expected_user_id = format!("node:{}", cfg.node_id);
    if principal.user_id != expected_user_id
        || !principal.permissions.contains(&Permission::PeerRpc)
    {
        return Err(Error::Config(format!(
            "node CURVE key must be bound to {expected_user_id} with peer_rpc permission"
        )));
    }
    let user_id = principal.user_id.clone();
    let mut server_keys = BTreeMap::new();
    let mut server_key_principals = BTreeMap::new();
    for (endpoint, principal_id) in security.server_key_pins {
        if endpoint.is_empty() || endpoint.len() > MAX_ENDPOINT_BYTES {
            return Err(Error::Config(
                "security.server_key_pins contains an invalid endpoint".into(),
            ));
        }
        let principal = policy
            .authorize(&principal_id, Permission::PeerRpc)
            .ok_or_else(|| {
                Error::Config(format!(
                    "server key pin {endpoint} references unknown or untrusted node {principal_id}"
                ))
            })?;
        let key = zmq::z85_decode(&principal.curve_public_key)
            .map_err(|_| Error::Config("trust policy contains an invalid CURVE public key".into()))?
            .try_into()
            .map_err(|_| Error::Config("trust policy CURVE public key has wrong length".into()))?;
        server_keys.insert(endpoint.clone(), key);
        server_key_principals.insert(endpoint.clone(), principal_id);
    }
    for endpoint in [&cfg.control_addr, &cfg.bulk_addr] {
        match server_key_principals.get(endpoint) {
            Some(principal_id) if principal_id != &user_id => {
                return Err(Error::Config(format!(
                    "local endpoint {endpoint} is pinned to {principal_id}, expected {user_id}"
                )));
            }
            Some(_) => {}
            None => {
                server_key_principals.insert(endpoint.clone(), user_id.clone());
                server_keys.insert(endpoint.clone(), public_key);
            }
        }
    }
    for endpoint in &cfg.seeds {
        if !server_keys.contains_key(endpoint) {
            return Err(Error::Config(format!(
                "security.server_key_pins has no key for configured seed {endpoint}"
            )));
        }
    }
    if persist_generation {
        ensure_policy_generation(&security.trust_generation_file, policy.generation())?;
    } else {
        validate_policy_generation_marker(&security.trust_generation_file, policy.generation())?;
    }
    Ok(LoadedNodeSecurity {
        policy,
        user_id,
        public_key,
        secret_key,
        server_keys,
        server_key_principals,
    })
}

fn validate_policy_generation_marker(path: &Path, generation: u64) -> Result<()> {
    if !path.is_absolute() {
        return Err(Error::Config(
            "trust_generation_file must be an absolute path outside data backups".into(),
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| Error::Config("trust_generation_file has no parent directory".into()))?;
    if !parent.is_dir() {
        return Err(Error::Config(format!(
            "trust generation directory {} must already exist",
            parent.display()
        )));
    }
    let current = match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() {
                return Err(Error::Config(
                    "trust_generation_file must be a regular file".into(),
                ));
            }
            validate_private_mode(path, &metadata)?;
            std::fs::read_to_string(path)?
                .trim()
                .parse::<u64>()
                .map_err(|error| {
                    Error::Config(format!("trust generation marker is invalid: {error}"))
                })?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::Io(error)),
    };
    if current > generation {
        return Err(Error::Config(format!(
            "trust policy generation {generation} is older than the persisted generation {current}"
        )));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitNodeEntry {
    node_id: NodeId,
    control_addr: String,
    bulk_addr: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitFile {
    cluster_id: String,
    p: u64,
    r: u8,
    #[serde(default)]
    hash_spec: Option<HashSpec>,
    meta_voters: Vec<NodeId>,
    nodes: Vec<InitNodeEntry>,
}

/// Load and validate a cluster-init file, producing the immutable bootstrap
/// descriptor every bootstrap node derives identically (DESIGN §3.1).
pub fn load_init_descriptor(path: &Path) -> Result<BootstrapDescriptor> {
    let text = std::fs::read_to_string(path)?;
    let file: InitFile = serde_json::from_str(&text)
        .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;

    let cluster_id = parse_cluster_id(&file.cluster_id)?;
    let p = parse_partition_count(file.p)?;
    let hash_spec = file.hash_spec.unwrap_or(HashSpec::CANONICAL);

    let init = InitConfig {
        p,
        r: file.r,
        bootstrap_nodes: file.nodes.iter().map(|n| n.node_id).collect(),
        meta_voters: file.meta_voters.clone(),
        hash_spec,
    };
    init.validate()?;

    let mut directory: Vec<DirEntry> = file
        .nodes
        .into_iter()
        .map(|n| DirEntry {
            node_id: n.node_id,
            control_addr: n.control_addr,
            bulk_addr: n.bulk_addr,
        })
        .collect();
    // Determinism: descriptor bytes must not depend on file entry order.
    directory.sort_by_key(|e| e.node_id);
    let mut address_owner = std::collections::HashMap::new();
    for entry in &directory {
        for address in [&entry.control_addr, &entry.bulk_addr] {
            if address.is_empty() || address.len() > MAX_ENDPOINT_BYTES {
                return Err(Error::Config(format!(
                    "node {} addresses must be 1..={MAX_ENDPOINT_BYTES} bytes",
                    entry.node_id
                )));
            }
            if let Some(previous) = address_owner.insert(address.as_str(), entry.node_id) {
                return Err(Error::Config(format!(
                    "nodes {previous} and {} share address {address}",
                    entry.node_id
                )));
            }
        }
    }

    let node_ids: Vec<NodeId> = directory.iter().map(|e| e.node_id).collect();
    let data_placements = initial_placements(p, file.r, &node_ids);

    let mut meta_voters = file.meta_voters;
    meta_voters.sort_unstable();

    Ok(BootstrapDescriptor {
        cluster_id,
        config: ClusterConfig {
            cluster_id,
            protocol_version: PROTOCOL_VERSION,
            p,
            r: file.r,
            hash_spec,
        },
        meta_voters,
        directory,
        data_placements,
    })
}

/// Deterministic genesis placement: partition `i` gets `r` consecutive nodes
/// from the sorted node list starting at `i % n`. Callers pass sorted,
/// distinct ids with `n >= r` (guaranteed by [`InitConfig::validate`]).
pub fn initial_placements(p: u16, r: u8, sorted_nodes: &[NodeId]) -> Vec<(u16, Vec<NodeId>)> {
    let n = sorted_nodes.len();
    (0..p)
        .map(|i| {
            let voters = (0..r as usize)
                .map(|k| sorted_nodes[(i as usize + k) % n])
                .collect();
            (i, voters)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tmp(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f
    }

    #[test]
    fn node_file_round_trip() {
        let f = write_tmp(
            r#"{
                "cluster_id": "0xdeadbeef",
                "node_id": 1,
                "control_addr": "tcp://127.0.0.1:5001",
                "bulk_addr": "tcp://127.0.0.1:5002",
                "http_addr": "127.0.0.1:7001",
                "seeds": ["tcp://127.0.0.1:5011"],
                "data_dir": "/tmp/dal-1",
                "timeouts": { "suspect_ms": 1000, "down_ms": 5000, "request_ms": 2000 }
            }"#,
        );
        let cfg = load_node_config(f.path()).unwrap();
        assert_eq!(cfg.cluster_id, 0xdeadbeef);
        assert_eq!(cfg.node_id, 1);
        assert_eq!(cfg.http_addr.as_deref(), Some("127.0.0.1:7001"));
        assert_eq!(cfg.timeouts.suspect.as_millis(), 1000);
        assert_eq!(cfg.timeouts.down.as_millis(), 5000);
        assert_eq!(cfg.timeouts.shutdown, Timeouts::default().shutdown);
    }

    #[test]
    fn node_file_defaults() {
        let f = write_tmp(
            r#"{
                "cluster_id": "42",
                "node_id": 2,
                "control_addr": "tcp://127.0.0.1:5001",
                "bulk_addr": "tcp://127.0.0.1:5002",
                "data_dir": "/tmp/dal-2"
            }"#,
        );
        let cfg = load_node_config(f.path()).unwrap();
        assert_eq!(cfg.cluster_id, 0x42);
        assert_eq!(cfg.http_addr, None);
        assert!(cfg.seeds.is_empty());
        assert_eq!(cfg.timeouts, Timeouts::default());
    }

    #[test]
    fn node_security_resolves_pins_through_the_cluster_policy() {
        let dir = tempfile::tempdir().unwrap();
        let node_public = "A".repeat(40);
        let peer_public = "B".repeat(40);
        let policy_path = dir.path().join("trust-policy.json");
        std::fs::write(
            &policy_path,
            format!(
                r#"{{"cluster_id":"7","generation":1,"principals":[{{"user_id":"node:2","curve_public_key":"{node_public}","permissions":["peer_rpc"]}},{{"user_id":"node:3","curve_public_key":"{peer_public}","permissions":["peer_rpc"]}}]}}"#
            ),
        )
        .unwrap();
        let node_public_path = dir.path().join("node-public.key");
        let node_secret_path = dir.path().join("node-secret.key");
        let generation_path = dir.path().join("node-policy-generation");
        std::fs::write(&node_public_path, &node_public).unwrap();
        std::fs::write(&node_secret_path, &node_public).unwrap();
        #[cfg(unix)]
        for path in [&policy_path, &node_public_path, &node_secret_path] {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let node_config = dir.path().join("node.json");
        std::fs::write(
            &node_config,
            format!(
                r#"{{"cluster_id":"7","node_id":2,"control_addr":"tcp://127.0.0.1:5001","bulk_addr":"tcp://127.0.0.1:5002","seeds":["tcp://127.0.0.1:5003"],"data_dir":"/tmp/dal-node-2","security":{{"trust_policy_file":"{}","trust_generation_file":"{}","curve_public_key_file":"{}","curve_secret_key_file":"{}","server_key_pins":{{"tcp://127.0.0.1:5003":"node:3","tcp://127.0.0.1:5004":"node:3"}}}}}}"#,
                policy_path.display(),
                generation_path.display(),
                node_public_path.display(),
                node_secret_path.display(),
            ),
        )
        .unwrap();
        let cfg = load_node_config(&node_config).unwrap();
        validate_node_security(&node_config, &cfg).unwrap();
        assert!(!generation_path.exists());
        let security = load_node_security(&node_config, &cfg).unwrap();
        assert!(generation_path.exists());
        let expected_peer_key: [u8; 32] =
            zmq::z85_decode(&peer_public).unwrap().try_into().unwrap();
        assert_eq!(security.user_id, "node:2");
        assert_eq!(security.server_keys.len(), 4);
        assert_eq!(
            security.server_keys["tcp://127.0.0.1:5003"],
            expected_peer_key
        );
        let descriptor = BootstrapDescriptor {
            cluster_id: 7,
            config: ClusterConfig {
                cluster_id: 7,
                protocol_version: PROTOCOL_VERSION,
                p: 1,
                r: 3,
                hash_spec: HashSpec::CANONICAL,
            },
            meta_voters: vec![2, 3, 4],
            directory: vec![
                DirEntry {
                    node_id: 2,
                    control_addr: "tcp://127.0.0.1:5001".into(),
                    bulk_addr: "tcp://127.0.0.1:5002".into(),
                },
                DirEntry {
                    node_id: 3,
                    control_addr: "tcp://127.0.0.1:5003".into(),
                    bulk_addr: "tcp://127.0.0.1:5004".into(),
                },
            ],
            data_placements: Vec::new(),
        };
        security.validate_descriptor(&descriptor, 2).unwrap();
        let mut swapped = descriptor.clone();
        swapped.directory[1].node_id = 4;
        assert!(security.validate_descriptor(&swapped, 2).is_err());
    }

    #[test]
    fn operator_security_requires_operator_principal_and_loads_node_pins() {
        let dir = tempfile::tempdir().unwrap();
        let operator_public = "C".repeat(40);
        let node_public = "D".repeat(40);
        let policy_path = dir.path().join("policy.json");
        std::fs::write(&policy_path, format!(
            r#"{{"cluster_id":"7","generation":1,"principals":[{{"user_id":"ops:deploy","curve_public_key":"{operator_public}","permissions":["operator"]}},{{"user_id":"node:1","curve_public_key":"{node_public}","permissions":["peer_rpc"]}}]}}"#
        )).unwrap();
        let public_path = dir.path().join("operator-public.key");
        let secret_path = dir.path().join("operator-secret.key");
        let generation_path = dir.path().join("operator-policy-generation");
        std::fs::write(&public_path, &operator_public).unwrap();
        std::fs::write(&secret_path, &operator_public).unwrap();
        #[cfg(unix)]
        for file in [&policy_path, &public_path, &secret_path] {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let auth_path = dir.path().join("operator.json");
        std::fs::write(&auth_path, format!(
            r#"{{"cluster_id":"7","trust_policy_file":"{}","trust_generation_file":"{}","user_id":"ops:deploy","curve_public_key_file":"{}","curve_secret_key_file":"{}","server_key_pins":{{"tcp://127.0.0.1:5101":"node:1"}}}}"#,
            policy_path.display(), generation_path.display(), public_path.display(), secret_path.display()
        )).unwrap();
        let loaded = load_operator_security(&auth_path, 7).unwrap();
        assert_eq!(loaded.user_id, "ops:deploy");
        assert_eq!(loaded.server_keys.len(), 1);
        assert!(load_operator_security(&auth_path, 8).is_err());
    }

    #[test]
    fn operator_enrollment_requires_candidate_key_in_the_same_policy_generation() {
        let node_key = "C".repeat(40);
        let policy = TrustPolicy::new(
            7,
            4,
            vec![crate::transport::security::Principal {
                user_id: "node:9".into(),
                curve_public_key: node_key.clone(),
                permissions: vec![Permission::PeerRpc],
                client_ids: Vec::new(),
            }],
        )
        .unwrap();
        let operator = LoadedOperatorSecurity {
            policy: policy.clone(),
            user_id: "ops:test".into(),
            public_key: [0; 32],
            secret_key: [0; 32],
            server_keys: BTreeMap::new(),
            server_key_principals: BTreeMap::new(),
        };
        let candidate = LoadedNodeSecurity {
            policy: Arc::new(policy),
            user_id: "node:9".into(),
            public_key: zmq::z85_decode(&node_key).unwrap().try_into().unwrap(),
            secret_key: [0; 32],
            server_keys: BTreeMap::new(),
            server_key_principals: BTreeMap::new(),
        };
        operator.validate_joining_node(&candidate, 9).unwrap();
        assert!(operator.validate_joining_node(&candidate, 8).is_err());
        let stale_policy = TrustPolicy::new(7, 3, vec![]).unwrap();
        let stale_operator = LoadedOperatorSecurity {
            policy: stale_policy,
            user_id: "ops:test".into(),
            public_key: [0; 32],
            secret_key: [0; 32],
            server_keys: BTreeMap::new(),
            server_key_principals: BTreeMap::new(),
        };
        assert!(stale_operator.validate_joining_node(&candidate, 9).is_err());
    }

    #[test]
    fn trust_policy_generation_marker_rejects_rollback_and_advances_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("trust-generation");
        ensure_policy_generation(&marker, 2).unwrap();
        assert_eq!(std::fs::read_to_string(&marker).unwrap().trim(), "2");
        assert!(ensure_policy_generation(&marker, 1).is_err());
        ensure_policy_generation(&marker, 3).unwrap();
        assert_eq!(std::fs::read_to_string(&marker).unwrap().trim(), "3");
        let older_marker = dir.path().join("concurrent-generation");
        let path_a = older_marker.clone();
        let path_b = older_marker.clone();
        let a = std::thread::spawn(move || ensure_policy_generation(&path_a, 4));
        let b = std::thread::spawn(move || ensure_policy_generation(&path_b, 5));
        let _ = a.join().unwrap();
        let _ = b.join().unwrap();
        assert_eq!(std::fs::read_to_string(&older_marker).unwrap().trim(), "5");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&marker, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(ensure_policy_generation(&marker, 4).is_err());
        }
    }

    #[test]
    fn node_file_rejects_invalid() {
        // Same control/bulk addr fails NodeConfig::validate.
        let f = write_tmp(
            r#"{
                "cluster_id": "1", "node_id": 1,
                "control_addr": "tcp://x", "bulk_addr": "tcp://x",
                "data_dir": "/tmp/d"
            }"#,
        );
        assert!(load_node_config(f.path()).is_err());

        // A zero request timeout would expire every just-sent RPC.
        let f = write_tmp(
            r#"{
                "cluster_id": "1", "node_id": 1,
                "control_addr": "tcp://a", "bulk_addr": "tcp://b",
                "data_dir": "/tmp/d",
                "timeouts": { "suspect_ms": 1000, "down_ms": 5000, "request_ms": 0 }
            }"#,
        );
        assert!(load_node_config(f.path()).is_err());

        // Unknown fields are config errors, not silently ignored.
        let f = write_tmp(
            r#"{
                "cluster_id": "1", "node_id": 1, "typo_field": true,
                "control_addr": "tcp://a", "bulk_addr": "tcp://b",
                "data_dir": "/tmp/d"
            }"#,
        );
        assert!(load_node_config(f.path()).is_err());

        // Bad hex cluster id.
        let f = write_tmp(
            r#"{
                "cluster_id": "zz", "node_id": 1,
                "control_addr": "tcp://a", "bulk_addr": "tcp://b",
                "data_dir": "/tmp/d"
            }"#,
        );
        assert!(load_node_config(f.path()).is_err());

        // Bad timeouts (down < suspect).
        let f = write_tmp(
            r#"{
                "cluster_id": "1", "node_id": 1,
                "control_addr": "tcp://a", "bulk_addr": "tcp://b",
                "data_dir": "/tmp/d",
                "timeouts": { "suspect_ms": 5000, "down_ms": 1000, "request_ms": 1000 }
            }"#,
        );
        assert!(load_node_config(f.path()).is_err());
    }

    fn init_json() -> String {
        r#"{
            "cluster_id": "0xabc",
            "p": 8,
            "r": 3,
            "meta_voters": [1, 2, 3],
            "nodes": [
                { "node_id": 3, "control_addr": "tcp://127.0.0.1:5031", "bulk_addr": "tcp://127.0.0.1:5032" },
                { "node_id": 1, "control_addr": "tcp://127.0.0.1:5011", "bulk_addr": "tcp://127.0.0.1:5012" },
                { "node_id": 2, "control_addr": "tcp://127.0.0.1:5021", "bulk_addr": "tcp://127.0.0.1:5022" }
            ]
        }"#
        .to_string()
    }

    #[test]
    fn init_descriptor_is_deterministic_and_sorted() {
        let f = write_tmp(&init_json());
        let a = load_init_descriptor(f.path()).unwrap();
        let b = load_init_descriptor(f.path()).unwrap();
        assert_eq!(a, b);

        assert_eq!(a.cluster_id, 0xabc);
        assert_eq!(a.config.p, 8);
        assert_eq!(a.config.r, 3);
        assert_eq!(a.config.protocol_version, PROTOCOL_VERSION);
        assert_eq!(a.config.hash_spec, HashSpec::CANONICAL);
        assert_eq!(a.meta_voters, vec![1, 2, 3]);
        // Directory sorted by node id regardless of file order.
        let ids: Vec<NodeId> = a.directory.iter().map(|e| e.node_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(a.data_placements.len(), 8);
    }

    #[test]
    fn init_rejects_duplicate_addr() {
        let json = init_json().replace("tcp://127.0.0.1:5021", "tcp://127.0.0.1:5011");
        let f = write_tmp(&json);
        assert!(load_init_descriptor(f.path()).is_err());

        // Non-adjacent node ids must also be compared globally.
        let json = init_json().replace("tcp://127.0.0.1:5031", "tcp://127.0.0.1:5011");
        let f = write_tmp(&json);
        assert!(load_init_descriptor(f.path()).is_err());
    }

    #[test]
    fn init_rejects_invalid_init_config() {
        // meta voter not in node set.
        let json = init_json().replace("[1, 2, 3]", "[1, 2, 9]");
        let f = write_tmp(&json);
        assert!(load_init_descriptor(f.path()).is_err());
    }

    #[test]
    fn placements_cover_all_partitions_with_r_distinct_voters() {
        let nodes = vec![1u64, 2, 3, 4, 5];
        let placements = initial_placements(16, 3, &nodes);
        assert_eq!(placements.len(), 16);
        for (i, (part, voters)) in placements.iter().enumerate() {
            assert_eq!(*part, i as u16);
            assert_eq!(voters.len(), 3);
            let distinct: std::collections::BTreeSet<_> = voters.iter().collect();
            assert_eq!(distinct.len(), 3, "partition {part} has duplicate voters");
            for v in voters {
                assert!(nodes.contains(v));
            }
        }
        // Round-robin spreads leaders-by-first-voter across nodes.
        let firsts: std::collections::BTreeSet<NodeId> =
            placements.iter().map(|(_, v)| v[0]).collect();
        assert_eq!(firsts.len(), nodes.len());
    }

    #[test]
    fn placements_deterministic() {
        let nodes = vec![1u64, 2, 3];
        assert_eq!(
            initial_placements(4, 3, &nodes),
            initial_placements(4, 3, &nodes)
        );
    }

    #[test]
    fn cluster_id_hex_round_trips() {
        for id in [0u128, 1, 0xdeadbeef, u128::MAX] {
            assert_eq!(parse_cluster_id(&cluster_id_hex(id)).unwrap(), id);
        }
    }
}
