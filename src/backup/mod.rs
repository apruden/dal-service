//! Offline, maintenance-fenced node checkpoints. A complete cluster package
//! is assembled only after every required participant has contributed an
//! independently checked database artifact.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::codec;
use crate::error::{Error, Result};
use crate::keyspace;
use crate::meta::bootstrap::BootstrapDescriptor;
use crate::storage::Storage;
use crate::types::{
    ClusterConfig, GroupId, LogId, MaintenanceRecord, MaintenanceStage, NodeDirectoryEntry, NodeId,
    Placement,
};

const FORMAT: u32 = 1;
const NODE_MANIFEST: &str = "node.json";
const STORAGE_MARKER: &str = "DAL_FORMAT.json";

fn canonical_parent(path: &Path) -> Result<PathBuf> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .map_err(Into::into)
}

fn ensure_absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(Error::Config(format!(
            "destination {} already exists",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileDigest {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupPosition {
    pub group: GroupId,
    pub applied: LogId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaInventory {
    pub cluster: ClusterConfig,
    pub directory: Vec<NodeDirectoryEntry>,
    pub placements: Vec<(GroupId, Placement)>,
    pub search_state_sha256: String,
    pub state_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeArtifact {
    pub format: u32,
    pub backup_id: u128,
    pub cluster_id: u128,
    pub node_id: NodeId,
    pub maintenance: MaintenanceRecord,
    pub positions: Vec<GroupPosition>,
    pub meta: Option<MetaInventory>,
    pub files: Vec<FileDigest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageNode {
    pub node_id: NodeId,
    pub manifest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageManifest {
    pub format: u32,
    pub backup_id: u128,
    pub descriptor: BootstrapDescriptor,
    pub operation_id: u128,
    pub generation: u64,
    pub positions: Vec<GroupPosition>,
    pub meta: MetaInventory,
    pub trust_generation: u64,
    pub trust_policy_digest: String,
    pub nodes: Vec<PackageNode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustSnapshot {
    pub generation: u64,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FencingEvidence {
    pub cluster_id: String,
    pub backup_id: String,
    pub checked_by: String,
    pub old_processes_stopped: bool,
    pub automatic_restart_disabled: bool,
    pub old_peer_and_client_routes_isolated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryPlan {
    pub format: u32,
    pub cluster_id: u128,
    pub backup_id: u128,
    pub recovery_epoch: u128,
    pub minimum_trust_generation: u64,
    pub prior_trust_policy_digest: String,
    pub package_manifest_sha256: String,
    pub fencing_evidence_sha256: String,
    pub nodes: Vec<NodeId>,
    pub recovery_point: Vec<GroupPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreInventory {
    pub node_id: NodeId,
    pub files: usize,
    pub bytes: u64,
    pub recovery_epoch: u128,
}

fn sha256_file(path: &Path) -> Result<(u64, String)> {
    let mut source = File::open(path)?;
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = source.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes = bytes
            .checked_add(count as u64)
            .ok_or_else(|| Error::Config("backup file size overflow".into()))?;
        digest.update(&buffer[..count]);
    }
    Ok((bytes, format!("{:x}", digest.finalize())))
}

fn regular_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                files.push(entry.path());
            } else {
                return Err(Error::Config(format!(
                    "backup source contains a nonregular file: {}",
                    entry.path().display()
                )));
            }
        }
    }
    files.sort();
    Ok(files)
}

fn relative_file_digest(root: &Path, path: &Path) -> Result<FileDigest> {
    let relative = path
        .strip_prefix(root)
        .map_err(|error| Error::Config(error.to_string()))?;
    let name = relative
        .to_str()
        .ok_or_else(|| Error::Config("non-UTF8 backup path".into()))?;
    let (bytes, sha256) = sha256_file(path)?;
    Ok(FileDigest {
        path: name.into(),
        bytes,
        sha256,
    })
}

fn digest_rows(rows: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>) -> String {
    let mut digest = Sha256::new();
    for (key, value) in rows {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    format!("{:x}", digest.finalize())
}

fn meta_inventory(
    storage: &Storage,
    desc: &BootstrapDescriptor,
    local_fence: &MaintenanceRecord,
) -> Result<MetaInventory> {
    let cluster: ClusterConfig = storage
        .get_state_record(GroupId::Meta, &keyspace::meta_cluster_key())?
        .ok_or_else(|| Error::Config("meta cluster record missing".into()))?;
    if cluster != desc.config || cluster.cluster_id != desc.cluster_id {
        return Err(Error::Config(
            "backup meta cluster differs from bootstrap descriptor".into(),
        ));
    }
    let replicated_fence: MaintenanceRecord = storage
        .get_state_record(GroupId::Meta, &keyspace::meta_maintenance_key())?
        .ok_or_else(|| Error::Config("replicated maintenance fence missing".into()))?;
    if replicated_fence.operation_id != local_fence.operation_id
        || replicated_fence.generation != local_fence.generation
        || replicated_fence.stage != MaintenanceStage::Quiesced
        || replicated_fence.participants != local_fence.participants
    {
        return Err(Error::Config(
            "meta replica is not at the quiesced backup fence".into(),
        ));
    }
    let rows = storage.scan_state(GroupId::Meta)?;
    let directory = rows
        .iter()
        .filter(|(key, _)| key.starts_with(&keyspace::meta_node_prefix()))
        .map(|(_, value)| codec::decode::<NodeDirectoryEntry>(value))
        .collect::<Result<Vec<_>>>()?;
    let mut placements = Vec::new();
    for group in std::iter::once(GroupId::Meta).chain((0..cluster.p).map(GroupId::Data)) {
        let placement: Placement = storage
            .get_state_record(GroupId::Meta, &keyspace::meta_placement_key(group))?
            .ok_or_else(|| Error::Config(format!("backup placement {group:?} missing")))?;
        if placement.r#move.is_some() {
            return Err(Error::Config(
                "backup cannot contain an in-flight placement move".into(),
            ));
        }
        placements.push((group, placement));
    }
    let search_state_sha256 = digest_rows(
        rows.iter()
            .filter(|(key, _)| key.starts_with(&keyspace::meta_search_index_prefix()))
            .cloned(),
    );
    Ok(MetaInventory {
        cluster,
        directory,
        placements,
        search_state_sha256,
        state_sha256: digest_rows(rows),
    })
}

/// Export one stopped node. Opening RocksDB takes its process lock; a running
/// node therefore cannot be exported. The checkpoint includes all column
/// families and logs, while derived Tantivy files are intentionally omitted.
pub fn export_node(
    desc: &BootstrapDescriptor,
    node_id: NodeId,
    data_dir: &Path,
    backup_id: u128,
    operation_id: u128,
    generation: u64,
    destination: &Path,
) -> Result<NodeArtifact> {
    if backup_id == 0 || operation_id == 0 || generation == 0 {
        return Err(Error::Config(
            "backup IDs must be nonzero and destination must be new".into(),
        ));
    }
    ensure_absent(destination)?;
    let source = data_dir.canonicalize()?;
    let parent = canonical_parent(destination)?;
    if parent.starts_with(&source) {
        return Err(Error::Config(
            "backup destination must be outside the node data directory".into(),
        ));
    }
    let storage = Storage::open(&source)?;
    let identity = storage
        .identity()?
        .ok_or_else(|| Error::Config("node identity missing".into()))?;
    if identity.cluster_id != desc.cluster_id || identity.node_id != node_id {
        return Err(Error::Config("backup node identity mismatch".into()));
    }
    let maintenance: MaintenanceRecord = storage
        .get_local(&keyspace::maintenance_fence_key())?
        .ok_or_else(|| Error::Config("durable maintenance fence missing".into()))?;
    if maintenance.operation_id != operation_id
        || maintenance.generation != generation
        || maintenance.stage != MaintenanceStage::Quiesced
        || !maintenance.participants.contains(&node_id)
    {
        return Err(Error::Config(
            "node is not fenced for the quiesced backup operation".into(),
        ));
    }
    let mut positions = Vec::new();
    for group in std::iter::once(GroupId::Meta).chain((0..desc.config.p).map(GroupId::Data)) {
        if storage.group_exists(group) {
            let applied = storage
                .last_applied(group)?
                .ok_or_else(|| Error::Config(format!("{group:?} has no applied position")))?;
            positions.push(GroupPosition { group, applied });
        }
    }
    let meta = if storage.group_exists(GroupId::Meta) {
        Some(meta_inventory(&storage, desc, &maintenance)?)
    } else {
        None
    };
    let stage = parent.join(format!(
        ".dal-backup-{backup_id:032x}-node-{node_id}.partial"
    ));
    if fs::symlink_metadata(&stage).is_ok() {
        return Err(Error::Config(format!(
            "partial backup already exists: {}",
            stage.display()
        )));
    }
    fs::create_dir(&stage)?;
    let result = (|| {
        storage.checkpoint_offline(&stage.join("db"))?;
        fs::copy(
            source.join(STORAGE_MARKER),
            stage.join("db").join(STORAGE_MARKER),
        )?;
        for file in regular_files(&stage.join("db"))? {
            File::open(file)?.sync_all()?;
        }
        File::open(stage.join("db"))?.sync_all()?;
        let files = regular_files(&stage.join("db"))?
            .iter()
            .map(|path| relative_file_digest(&stage, path))
            .collect::<Result<Vec<_>>>()?;
        let manifest = NodeArtifact {
            format: FORMAT,
            backup_id,
            cluster_id: desc.cluster_id,
            node_id,
            maintenance,
            positions,
            meta,
            files,
        };
        let bytes = serde_json::to_vec_pretty(&manifest).map_err(Error::codec)?;
        let mut output = File::create(stage.join(NODE_MANIFEST))?;
        output.write_all(&bytes)?;
        output.sync_all()?;
        File::open(&stage)?.sync_all()?;
        fs::rename(&stage, destination)?;
        File::open(&parent)?.sync_all()?;
        Ok(manifest)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&stage);
    }
    result
}

/// Rehash every file and reject additions, omissions, symlinks, and path
/// traversal before an artifact can contribute to a cluster package.
pub fn verify_node(path: &Path) -> Result<NodeArtifact> {
    if !fs::symlink_metadata(path)?.file_type().is_dir()
        || !fs::symlink_metadata(path.join("db"))?.file_type().is_dir()
        || !fs::symlink_metadata(path.join(NODE_MANIFEST))?
            .file_type()
            .is_file()
    {
        return Err(Error::Config(
            "backup artifact contains an unsafe path".into(),
        ));
    }
    let mut root_entries = fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<Vec<_>>>()?;
    root_entries.sort();
    if root_entries != ["db", NODE_MANIFEST] {
        return Err(Error::Config(
            "backup node inventory contains extra or missing paths".into(),
        ));
    }
    let manifest: NodeArtifact =
        serde_json::from_slice(&fs::read(path.join(NODE_MANIFEST))?).map_err(Error::codec)?;
    if manifest.format != FORMAT || manifest.backup_id == 0 || manifest.cluster_id == 0 {
        return Err(Error::Config("unsupported or invalid node artifact".into()));
    }
    let mut actual = regular_files(&path.join("db"))?
        .iter()
        .map(|file| relative_file_digest(path, file))
        .collect::<Result<Vec<_>>>()?;
    actual.sort_by(|a, b| a.path.cmp(&b.path));
    let mut expected = manifest.files.clone();
    for file in &expected {
        let relative = Path::new(&file.path);
        if !relative.starts_with("db")
            || relative
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err(Error::Config("unsafe backup artifact path".into()));
        }
    }
    expected.sort_by(|a, b| a.path.cmp(&b.path));
    if expected != actual {
        return Err(Error::Config(format!(
            "backup artifact {} failed file verification",
            path.display()
        )));
    }
    Ok(manifest)
}

fn validate_cluster_artifacts(
    desc: &BootstrapDescriptor,
    artifacts: &[NodeArtifact],
) -> Result<(MetaInventory, Vec<GroupPosition>)> {
    let first = artifacts
        .first()
        .ok_or_else(|| Error::Config("backup has no node artifacts".into()))?;
    let fence = &first.maintenance;
    if first.cluster_id != desc.cluster_id || fence.stage != MaintenanceStage::Quiesced {
        return Err(Error::Config(
            "backup identity or quiesced fence mismatch".into(),
        ));
    }
    let mut ids: Vec<_> = artifacts.iter().map(|artifact| artifact.node_id).collect();
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) || ids != fence.participants {
        return Err(Error::Config(
            "backup is missing a maintenance participant".into(),
        ));
    }
    for artifact in artifacts {
        if artifact.format != FORMAT
            || artifact.backup_id != first.backup_id
            || artifact.cluster_id != desc.cluster_id
            || artifact.maintenance.operation_id != fence.operation_id
            || artifact.maintenance.generation != fence.generation
            || artifact.maintenance.participants != fence.participants
            || artifact.maintenance.stage != MaintenanceStage::Quiesced
        {
            return Err(Error::Config("backup contains mixed node artifacts".into()));
        }
    }
    let meta = artifacts
        .iter()
        .find_map(|artifact| artifact.meta.as_ref())
        .ok_or_else(|| Error::Config("backup has no meta inventory".into()))?
        .clone();
    if meta.cluster != desc.config {
        return Err(Error::Config(
            "backup cluster config differs from descriptor".into(),
        ));
    }
    let mut active: Vec<_> = meta
        .directory
        .iter()
        .filter(|entry| entry.state == crate::types::NodeState::Active)
        .map(|entry| entry.node_id)
        .collect();
    active.sort_unstable();
    if active != ids {
        return Err(Error::Config(
            "backup participants differ from frozen directory".into(),
        ));
    }
    let current_meta_voters = meta
        .placements
        .iter()
        .find(|(group, _)| *group == GroupId::Meta)
        .ok_or_else(|| Error::Config("meta placement missing from backup".into()))?
        .1
        .voters
        .clone();
    for node in &current_meta_voters {
        let artifact = artifacts
            .iter()
            .find(|artifact| artifact.node_id == *node)
            .ok_or_else(|| Error::Config(format!("meta voter {node} artifact missing")))?;
        if artifact.meta.as_ref() != Some(&meta) {
            return Err(Error::Config(
                "meta replicas differ at backup barrier".into(),
            ));
        }
    }
    let mut positions = Vec::new();
    for (group, placement) in &meta.placements {
        if placement.r#move.is_some() || placement.voters.is_empty() {
            return Err(Error::Config("invalid frozen backup placement".into()));
        }
        let mut applied = None;
        for voter in &placement.voters {
            let artifact = artifacts
                .iter()
                .find(|artifact| artifact.node_id == *voter)
                .ok_or_else(|| {
                    Error::Config(format!("{group:?} voter {voter} artifact missing"))
                })?;
            let position = artifact
                .positions
                .iter()
                .find(|position| position.group == *group)
                .ok_or_else(|| {
                    Error::Config(format!("{group:?} voter {voter} position missing"))
                })?;
            if applied.is_some_and(|prior| prior != position.applied) {
                return Err(Error::Config(format!(
                    "{group:?} replicas have different applied barriers"
                )));
            }
            applied = Some(position.applied);
        }
        positions.push(GroupPosition {
            group: *group,
            applied: applied.unwrap(),
        });
    }
    Ok((meta, positions))
}

fn copy_regular_tree(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            copy_regular_tree(&entry.path(), &target)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), &target)?;
            File::open(&target)?.sync_all()?;
        } else {
            return Err(Error::Config("backup contains a nonregular file".into()));
        }
    }
    File::open(destination)?.sync_all()?;
    Ok(())
}

/// Copy all node artifacts into a new immutable package. The copy breaks any
/// RocksDB checkpoint hard links to the source volume before publication.
pub fn assemble(
    desc: &BootstrapDescriptor,
    source_artifacts: &[PathBuf],
    destination: &Path,
    trust: TrustSnapshot,
) -> Result<PackageManifest> {
    if trust.generation == 0 || trust.digest == [0; 32] {
        return Err(Error::Config(
            "backup requires a current trust-policy snapshot".into(),
        ));
    }
    if fs::symlink_metadata(destination).is_ok() {
        return Err(Error::Config(
            "backup package destination already exists".into(),
        ));
    }
    let mut artifacts = Vec::new();
    for source in source_artifacts {
        artifacts.push(verify_node(source)?);
    }
    let (meta, positions) = validate_cluster_artifacts(desc, &artifacts)?;
    let backup_id = artifacts[0].backup_id;
    let fence = &artifacts[0].maintenance;
    let parent = canonical_parent(destination)?;
    for source in source_artifacts {
        let source = source.canonicalize()?;
        if parent.starts_with(&source) {
            return Err(Error::Config(
                "backup package cannot be nested inside an artifact".into(),
            ));
        }
    }
    let stage = parent.join(format!(".dal-package-{backup_id:032x}.partial"));
    if fs::symlink_metadata(&stage).is_ok() {
        return Err(Error::Config(
            "partial backup package already exists".into(),
        ));
    }
    fs::create_dir(&stage)?;
    let result = (|| {
        let mut nodes = Vec::new();
        for (source, artifact) in source_artifacts.iter().zip(&artifacts) {
            let target = stage.join(format!("node-{}", artifact.node_id));
            copy_regular_tree(source, &target)?;
            let copied = verify_node(&target)?;
            if copied != *artifact {
                return Err(Error::Config("copied node artifact changed".into()));
            }
            nodes.push(PackageNode {
                node_id: artifact.node_id,
                manifest_sha256: sha256_file(&target.join(NODE_MANIFEST))?.1,
            });
        }
        nodes.sort_by_key(|node| node.node_id);
        let package = PackageManifest {
            format: FORMAT,
            backup_id,
            descriptor: desc.clone(),
            operation_id: fence.operation_id,
            generation: fence.generation,
            positions,
            meta,
            trust_generation: trust.generation,
            trust_policy_digest: trust
                .digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
            nodes,
        };
        let bytes = serde_json::to_vec_pretty(&package).map_err(Error::codec)?;
        let mut output = File::create(stage.join("manifest.json"))?;
        output.write_all(&bytes)?;
        output.sync_all()?;
        File::open(&stage)?.sync_all()?;
        let verified = verify_package(&stage)?;
        fs::rename(&stage, destination)?;
        File::open(&parent)?.sync_all()?;
        Ok(verified)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&stage);
    }
    result
}

/// Read-only complete-package verification. A package with extra node
/// directories or altered artifacts is rejected even if the manifest parses.
pub fn verify_package(path: &Path) -> Result<PackageManifest> {
    if !fs::symlink_metadata(path)?.file_type().is_dir()
        || !fs::symlink_metadata(path.join("manifest.json"))?
            .file_type()
            .is_file()
    {
        return Err(Error::Config("unsafe backup package path".into()));
    }
    let package: PackageManifest =
        serde_json::from_slice(&fs::read(path.join("manifest.json"))?).map_err(Error::codec)?;
    if package.format != FORMAT
        || package.backup_id == 0
        || package.trust_generation == 0
        || package.trust_policy_digest.len() != 64
        || !package
            .trust_policy_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::Config("unsupported backup package format".into()));
    }
    let mut expected = vec!["manifest.json".to_string()];
    expected.extend(
        package
            .nodes
            .iter()
            .map(|node| format!("node-{}", node.node_id)),
    );
    expected.sort();
    let mut actual = fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<Vec<_>>>()?;
    actual.sort();
    if expected != actual {
        return Err(Error::Config(
            "backup package inventory differs from manifest".into(),
        ));
    }
    let mut artifacts = Vec::new();
    for node in &package.nodes {
        let node_path = path.join(format!("node-{}", node.node_id));
        if sha256_file(&node_path.join(NODE_MANIFEST))?.1 != node.manifest_sha256 {
            return Err(Error::Config("node manifest checksum mismatch".into()));
        }
        let artifact = verify_node(&node_path)?;
        if artifact.node_id != node.node_id {
            return Err(Error::Config("node artifact identity mismatch".into()));
        }
        artifacts.push(artifact);
    }
    let (meta, positions) = validate_cluster_artifacts(&package.descriptor, &artifacts)?;
    if package.backup_id != artifacts[0].backup_id
        || package.operation_id != artifacts[0].maintenance.operation_id
        || package.generation != artifacts[0].maintenance.generation
        || package.meta != meta
        || package.positions != positions
    {
        return Err(Error::Config(
            "backup manifest differs from node artifacts".into(),
        ));
    }
    Ok(package)
}

/// Create the one external recovery record shared by every restored node. The
/// evidence is an explicit operator attestation of checks made outside DAL;
/// this function validates its identity and records its digest, but cannot
/// inspect the old deployment's network or process manager itself.
pub fn initialize_restore(
    package_path: &Path,
    evidence_path: &Path,
    plan_path: &Path,
) -> Result<RecoveryPlan> {
    let package = verify_package(package_path)?;
    let evidence_bytes = fs::read(evidence_path)?;
    let evidence: FencingEvidence =
        serde_json::from_slice(&evidence_bytes).map_err(Error::codec)?;
    if evidence.cluster_id != format!("{:#x}", package.descriptor.cluster_id)
        || evidence.backup_id != format!("{:#x}", package.backup_id)
        || evidence.checked_by.trim().is_empty()
        || !evidence.old_processes_stopped
        || !evidence.automatic_restart_disabled
        || !evidence.old_peer_and_client_routes_isolated
    {
        return Err(Error::Config(
            "old deployment fencing attestation is incomplete".into(),
        ));
    }
    let mut random = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut random)?;
    let recovery_epoch = u128::from_be_bytes(random);
    if recovery_epoch == 0 {
        return Err(Error::Config("generated an invalid recovery epoch".into()));
    }
    let plan = RecoveryPlan {
        format: FORMAT,
        cluster_id: package.descriptor.cluster_id,
        backup_id: package.backup_id,
        recovery_epoch,
        minimum_trust_generation: package
            .trust_generation
            .checked_add(1)
            .ok_or_else(|| Error::Config("trust generation exhausted".into()))?,
        prior_trust_policy_digest: package.trust_policy_digest,
        package_manifest_sha256: sha256_file(&package_path.join("manifest.json"))?.1,
        fencing_evidence_sha256: format!("{:x}", Sha256::digest(&evidence_bytes)),
        nodes: package.nodes.iter().map(|node| node.node_id).collect(),
        recovery_point: package.positions,
    };
    let bytes = serde_json::to_vec_pretty(&plan).map_err(Error::codec)?;
    let parent = canonical_parent(plan_path)?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(plan_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    File::open(parent)?.sync_all()?;
    Ok(plan)
}

fn checked_recovery_plan(
    package_path: &Path,
    plan_path: &Path,
) -> Result<(PackageManifest, RecoveryPlan)> {
    if !fs::symlink_metadata(plan_path)?.file_type().is_file() {
        return Err(Error::Config("unsafe recovery record path".into()));
    }
    let package = verify_package(package_path)?;
    let plan: RecoveryPlan = serde_json::from_slice(&fs::read(plan_path)?).map_err(Error::codec)?;
    if plan.format != FORMAT
        || plan.cluster_id != package.descriptor.cluster_id
        || plan.backup_id != package.backup_id
        || plan.recovery_epoch == 0
        || plan.minimum_trust_generation
            != package
                .trust_generation
                .checked_add(1)
                .ok_or_else(|| Error::Config("trust generation exhausted".into()))?
        || plan.prior_trust_policy_digest != package.trust_policy_digest
        || plan.package_manifest_sha256 != sha256_file(&package_path.join("manifest.json"))?.1
        || plan.nodes
            != package
                .nodes
                .iter()
                .map(|node| node.node_id)
                .collect::<Vec<_>>()
        || plan.recovery_point != package.positions
    {
        return Err(Error::Config(
            "recovery record does not match backup package".into(),
        ));
    }
    Ok((package, plan))
}

#[cfg(unix)]
fn available_bytes(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| Error::Config("invalid restore destination path".into()))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(name.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let stat = unsafe { stat.assume_init() };
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

/// Validate all artifacts, package identity, capacity, and an empty target
/// before a restore writes anything. A caller may display this inventory as a
/// dry run, then invoke `restore_node` with the same recovery plan.
pub fn restore_inventory(
    package_path: &Path,
    plan_path: &Path,
    node_id: NodeId,
    destination: &Path,
) -> Result<RestoreInventory> {
    let (package, plan) = checked_recovery_plan(package_path, plan_path)?;
    let node = package
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .ok_or_else(|| Error::Config("node is absent from the recovery package".into()))?;
    if fs::symlink_metadata(destination).is_ok() {
        return Err(Error::Config("restore destination already exists".into()));
    }
    let parent = canonical_parent(destination)?;
    if parent.starts_with(&package_path.canonicalize()?) {
        return Err(Error::Config(
            "restore destination cannot be inside the package".into(),
        ));
    }
    let artifact = verify_node(&package_path.join(format!("node-{}", node.node_id)))?;
    let bytes = artifact.files.iter().try_fold(0u64, |sum, file| {
        sum.checked_add(file.bytes)
            .ok_or_else(|| Error::Config("restore size overflow".into()))
    })?;
    #[cfg(unix)]
    if available_bytes(&parent)? < bytes.saturating_add(bytes / 10) {
        return Err(Error::Config(
            "insufficient free space for restored database".into(),
        ));
    }
    Ok(RestoreInventory {
        node_id,
        files: artifact.files.len(),
        bytes,
        recovery_epoch: plan.recovery_epoch,
    })
}

/// Stage one node database from a verified package and publish it atomically.
/// Search files are omitted; every restored consumer is marked for full rebuild.
pub fn restore_node(
    package_path: &Path,
    plan_path: &Path,
    node_id: NodeId,
    destination: &Path,
) -> Result<RestoreInventory> {
    let inventory = restore_inventory(package_path, plan_path, node_id, destination)?;
    let (package, plan) = checked_recovery_plan(package_path, plan_path)?;
    let parent = canonical_parent(destination)?;
    let stage = parent.join(format!(
        ".dal-restore-{:032x}-node-{node_id}.partial",
        plan.recovery_epoch
    ));
    if fs::symlink_metadata(&stage).is_ok() {
        return Err(Error::Config(
            "partial restore directory already exists".into(),
        ));
    }
    let source = package_path.join(format!("node-{node_id}")).join("db");
    let result = (|| {
        copy_regular_tree(&source, &stage)?;
        let expected = verify_node(&package_path.join(format!("node-{node_id}")))?;
        let mut copied = regular_files(&stage)?
            .iter()
            .map(|file| relative_file_digest(&stage, file))
            .collect::<Result<Vec<_>>>()?;
        copied.sort_by(|left, right| left.path.cmp(&right.path));
        let mut expected_files = expected.files.clone();
        for file in &mut expected_files {
            file.path = file
                .path
                .strip_prefix("db/")
                .ok_or_else(|| Error::Config("unsafe node artifact path".into()))?
                .to_string();
        }
        expected_files.sort_by(|left, right| left.path.cmp(&right.path));
        if copied != expected_files {
            return Err(Error::Config(
                "restored database copy failed checksum verification".into(),
            ));
        }
        let storage = Storage::open(&stage)?;
        let identity = storage
            .identity()?
            .ok_or_else(|| Error::Config("restored node identity missing".into()))?;
        if identity.cluster_id != plan.cluster_id || identity.node_id != node_id {
            return Err(Error::Config("restored node identity mismatch".into()));
        }
        let fence: MaintenanceRecord = storage
            .get_local(&keyspace::maintenance_fence_key())?
            .ok_or_else(|| Error::Config("restored maintenance fence missing".into()))?;
        if fence.operation_id != package.operation_id
            || fence.generation != package.generation
            || fence.stage != MaintenanceStage::Quiesced
        {
            return Err(Error::Config("restored maintenance fence mismatch".into()));
        }
        storage.install_restored_epoch_offline(plan.recovery_epoch, package.descriptor.config.p)?;
        drop(storage);
        let mut marker = File::create(stage.join("DAL_RESTORE.json"))?;
        marker.write_all(&serde_json::to_vec_pretty(&plan).map_err(Error::codec)?)?;
        marker.sync_all()?;
        File::open(&stage)?.sync_all()?;
        fs::rename(&stage, destination)?;
        File::open(&parent)?.sync_all()?;
        Ok(inventory)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&stage);
    }
    result
}

/// Refuse to start a restored database if its external recovery marker is
/// absent, its epoch differs from any hosted state machine, or the installed
/// trust policy has not advanced beyond the backup's policy snapshot.
pub fn validate_restored_node(
    storage: &Storage,
    cluster_id: u128,
    node_id: NodeId,
    partition_count: u16,
    trust: Option<TrustSnapshot>,
) -> Result<()> {
    let epoch = storage.local_recovery_epoch()?;
    let marker_path = storage.path().join("DAL_RESTORE.json");
    if epoch == 0 {
        if fs::symlink_metadata(&marker_path).is_ok() {
            return Err(Error::Config(
                "restore marker exists without a recovery epoch".into(),
            ));
        }
        return Ok(());
    }
    if !fs::symlink_metadata(&marker_path)?.file_type().is_file() {
        return Err(Error::Config("unsafe restore marker path".into()));
    }
    let plan: RecoveryPlan =
        serde_json::from_slice(&fs::read(marker_path)?).map_err(Error::codec)?;
    if plan.format != FORMAT
        || plan.cluster_id != cluster_id
        || !plan.nodes.contains(&node_id)
        || plan.recovery_epoch != epoch
    {
        return Err(Error::Config(
            "restored node marker identity or epoch mismatch".into(),
        ));
    }
    for group in std::iter::once(GroupId::Meta).chain((0..partition_count).map(GroupId::Data)) {
        if storage.group_exists(group) && storage.recovery_epoch(group)? != epoch {
            return Err(Error::Config(format!(
                "restored {group:?} recovery epoch mismatch"
            )));
        }
    }
    if let Some(trust) = trust {
        let digest: String = trust
            .digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if trust.generation < plan.minimum_trust_generation
            || digest == plan.prior_trust_policy_digest
        {
            return Err(Error::Config(
                "restored node requires a newer trust policy excluding old credentials".into(),
            ));
        }
    }
    Ok(())
}
