//! JSON config files for the `dal` binary.
//!
//! Two shapes mirror [`crate::config`]: a per-node file (`dal run --config`)
//! and a shared cluster-init file (`dal init --cluster`). `cluster_id` is a
//! hex string on disk because JSON tooling cannot represent u128 reliably.
//! Timeouts are milliseconds. Every node given the same init file derives a
//! byte-identical [`BootstrapDescriptor`] (placements are deterministic), so
//! the durable bootstrap records written independently on each node agree.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::config::{InitConfig, NodeConfig, Timeouts, parse_partition_count};
use crate::error::{Error, Result};
use crate::meta::bootstrap::{BootstrapDescriptor, DirEntry};
use crate::types::{ClusterConfig, ClusterId, HashSpec, NodeId, PROTOCOL_VERSION};

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
}

impl Default for TimeoutsFile {
    fn default() -> Self {
        let t = Timeouts::default();
        TimeoutsFile {
            suspect_ms: t.suspect.as_millis() as u64,
            down_ms: t.down.as_millis() as u64,
            request_ms: t.request.as_millis() as u64,
        }
    }
}

impl From<TimeoutsFile> for Timeouts {
    fn from(f: TimeoutsFile) -> Timeouts {
        Timeouts {
            suspect: std::time::Duration::from_millis(f.suspect_ms),
            down: std::time::Duration::from_millis(f.down_ms),
            request: std::time::Duration::from_millis(f.request_ms),
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
    for w in directory.windows(2) {
        let addrs = [
            &w[0].control_addr,
            &w[0].bulk_addr,
            &w[1].control_addr,
            &w[1].bulk_addr,
        ];
        let distinct: std::collections::BTreeSet<_> = addrs.iter().collect();
        if distinct.len() != addrs.len() {
            return Err(Error::Config(format!(
                "nodes {} and {} share an address",
                w[0].node_id, w[1].node_id
            )));
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
