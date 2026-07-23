//! Static, validated configuration.
//!
//! Two distinct shapes: [`NodeConfig`] is what a running node is started with;
//! [`InitConfig`] is the cluster-creation input where `P` and `R` are chosen
//! once (DESIGN §3.1, §5.1). `P`/`R` never appear in `NodeConfig` — a node
//! learns them from the committed [`crate::types::ClusterConfig`].

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::types::{ClusterId, HashSpec, NodeId};

/// Failure-detection and request timeouts (DESIGN §13). Held here so the whole
/// system reads one source of truth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timeouts {
    pub suspect: Duration,
    pub down: Duration,
    pub request: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Timeouts {
            suspect: Duration::from_secs(3),
            down: Duration::from_secs(15),
            request: Duration::from_secs(5),
        }
    }
}

impl Timeouts {
    fn validate(&self) -> Result<()> {
        if self.suspect.is_zero() {
            return Err(Error::Config("suspect_timeout must be > 0".into()));
        }
        // A node must be Suspect before it is Down (DESIGN §9.1): the minimum
        // suspicion period is meaningless if it precedes suspicion.
        if self.down < self.suspect {
            return Err(Error::Config(
                "down_timeout must be >= suspect_timeout".into(),
            ));
        }
        Ok(())
    }
}

/// Per-node runtime configuration. Carries identity and endpoints only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub control_addr: String,
    pub bulk_addr: String,
    /// Optional read-only HTTP admin listener (`GET /status`). Absent means
    /// the admin plane is disabled.
    pub http_addr: Option<String>,
    pub seeds: Vec<String>,
    pub data_dir: PathBuf,
    pub timeouts: Timeouts,
}

impl NodeConfig {
    pub fn validate(&self) -> Result<()> {
        if self.node_id == 0 {
            // 0 is reserved as "unassigned"; real ids start at 1.
            return Err(Error::Config("node_id must be non-zero".into()));
        }
        if self.control_addr.is_empty() {
            return Err(Error::Config("control_addr must be set".into()));
        }
        if self.bulk_addr.is_empty() {
            return Err(Error::Config("bulk_addr must be set".into()));
        }
        if self.control_addr == self.bulk_addr {
            return Err(Error::Config(
                "control_addr and bulk_addr must differ (separate lanes, DESIGN §10.1)".into(),
            ));
        }
        if let Some(http) = &self.http_addr {
            if http.is_empty() {
                return Err(Error::Config("http_addr, if set, must be non-empty".into()));
            }
            if http == &self.control_addr || http == &self.bulk_addr {
                return Err(Error::Config(
                    "http_addr must differ from control_addr and bulk_addr".into(),
                ));
            }
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err(Error::Config("data_dir must be set".into()));
        }
        self.timeouts.validate()?;
        Ok(())
    }
}

/// The minimum replication factor. Below three there is no fault tolerance and
/// no odd meta-voter set is possible (DESIGN §3.1).
pub const MIN_REPLICATION: u8 = 3;

/// The minimum meta-voter set size.
pub const MIN_META_VOTERS: usize = 3;

/// Cluster-creation input. Validated once; the resulting immutable records are
/// what the meta group commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitConfig {
    pub p: u16,
    pub r: u8,
    /// Distinct node ids present at bootstrap, at least `R` of them.
    pub bootstrap_nodes: Vec<NodeId>,
    /// The initial meta voter set: an odd set of >= 3 distinct nodes, each a
    /// bootstrap node.
    pub meta_voters: Vec<NodeId>,
    pub hash_spec: HashSpec,
}

/// Parse an operator-supplied partition count, enforcing the `u16` bound
/// (DESIGN §12.1). A count above `u16::MAX` is a hard error, not a silent wrap.
pub fn parse_partition_count(n: u64) -> Result<u16> {
    if n == 0 {
        return Err(Error::Config("P must be > 0".into()));
    }
    if n > u16::MAX as u64 {
        return Err(Error::Config(format!(
            "P must be <= {} (u16 bound)",
            u16::MAX
        )));
    }
    Ok(n as u16)
}

impl InitConfig {
    pub fn validate(&self) -> Result<()> {
        if self.p == 0 {
            return Err(Error::Config("P must be > 0".into()));
        }
        if self.r < MIN_REPLICATION {
            return Err(Error::Config(format!("R must be >= {MIN_REPLICATION}")));
        }

        let bootstrap: BTreeSet<NodeId> = self.bootstrap_nodes.iter().copied().collect();
        if bootstrap.len() != self.bootstrap_nodes.len() {
            return Err(Error::Config("bootstrap_nodes must be distinct".into()));
        }
        if bootstrap.contains(&0) {
            return Err(Error::Config("node ids must be non-zero".into()));
        }
        if bootstrap.len() < self.r as usize {
            return Err(Error::Config(format!(
                "need at least R={} distinct bootstrap nodes, got {}",
                self.r,
                bootstrap.len()
            )));
        }

        let meta: BTreeSet<NodeId> = self.meta_voters.iter().copied().collect();
        if meta.len() != self.meta_voters.len() {
            return Err(Error::Config("meta_voters must be distinct".into()));
        }
        if meta.len() < MIN_META_VOTERS {
            return Err(Error::Config(format!(
                "meta voter set must have >= {MIN_META_VOTERS} distinct nodes"
            )));
        }
        if meta.len().is_multiple_of(2) {
            return Err(Error::Config("meta voter set must be odd".into()));
        }
        if !meta.is_subset(&bootstrap) {
            return Err(Error::Config(
                "every meta voter must be a bootstrap node".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::HashSpec;

    fn base_init() -> InitConfig {
        InitConfig {
            p: 128,
            r: 3,
            bootstrap_nodes: vec![1, 2, 3],
            meta_voters: vec![1, 2, 3],
            hash_spec: HashSpec::CANONICAL,
        }
    }

    #[test]
    fn valid_init_passes() {
        base_init().validate().unwrap();
    }

    #[test]
    fn partition_count_bounds() {
        assert!(parse_partition_count(0).is_err());
        assert_eq!(parse_partition_count(1).unwrap(), 1);
        assert_eq!(parse_partition_count(128).unwrap(), 128);
        assert_eq!(parse_partition_count(u16::MAX as u64).unwrap(), u16::MAX);
        assert!(parse_partition_count(u16::MAX as u64 + 1).is_err());
        assert!(parse_partition_count(1_000_000).is_err());
    }

    #[test]
    fn p_zero_rejected() {
        let mut c = base_init();
        c.p = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn r_below_three_rejected() {
        for r in [0u8, 1, 2] {
            let mut c = base_init();
            c.r = r;
            assert!(c.validate().is_err(), "R={r} should be rejected");
        }
    }

    #[test]
    fn too_few_bootstrap_nodes_rejected() {
        let mut c = base_init();
        c.r = 5;
        c.bootstrap_nodes = vec![1, 2, 3];
        c.meta_voters = vec![1, 2, 3];
        assert!(c.validate().is_err());
    }

    #[test]
    fn duplicate_bootstrap_nodes_rejected() {
        let mut c = base_init();
        c.bootstrap_nodes = vec![1, 2, 2];
        assert!(c.validate().is_err());
    }

    #[test]
    fn zero_node_id_rejected() {
        let mut c = base_init();
        c.bootstrap_nodes = vec![0, 1, 2];
        c.meta_voters = vec![0, 1, 2];
        assert!(c.validate().is_err());
    }

    #[test]
    fn even_meta_voter_set_rejected() {
        let mut c = base_init();
        c.r = 3;
        c.bootstrap_nodes = vec![1, 2, 3, 4];
        c.meta_voters = vec![1, 2, 3, 4];
        assert!(c.validate().is_err());
    }

    #[test]
    fn too_small_meta_voter_set_rejected() {
        let mut c = base_init();
        c.bootstrap_nodes = vec![1, 2, 3];
        c.meta_voters = vec![1];
        assert!(c.validate().is_err());
    }

    #[test]
    fn meta_voter_not_in_bootstrap_rejected() {
        let mut c = base_init();
        c.bootstrap_nodes = vec![1, 2, 3];
        c.meta_voters = vec![1, 2, 9];
        assert!(c.validate().is_err());
    }

    #[test]
    fn odd_five_meta_voters_ok() {
        let mut c = base_init();
        c.bootstrap_nodes = vec![1, 2, 3, 4, 5];
        c.meta_voters = vec![1, 2, 3, 4, 5];
        c.validate().unwrap();
    }

    #[test]
    fn node_config_validation() {
        let mut nc = NodeConfig {
            cluster_id: 42,
            node_id: 1,
            control_addr: "tcp://127.0.0.1:5001".into(),
            bulk_addr: "tcp://127.0.0.1:5002".into(),
            http_addr: Some("127.0.0.1:7001".into()),
            seeds: vec!["tcp://127.0.0.1:5001".into()],
            data_dir: PathBuf::from("/tmp/dal"),
            timeouts: Timeouts::default(),
        };
        nc.validate().unwrap();

        nc.http_addr = None;
        nc.validate().unwrap();
        nc.http_addr = Some("127.0.0.1:7001".into());

        nc.node_id = 0;
        assert!(nc.validate().is_err());
        nc.node_id = 1;

        nc.bulk_addr = nc.control_addr.clone();
        assert!(nc.validate().is_err());
        nc.bulk_addr = "tcp://127.0.0.1:5002".into();

        nc.http_addr = Some(nc.control_addr.clone());
        assert!(nc.validate().is_err());
        nc.http_addr = Some(String::new());
        assert!(nc.validate().is_err());
    }

    #[test]
    fn down_before_suspect_rejected() {
        let t = Timeouts {
            suspect: Duration::from_secs(10),
            down: Duration::from_secs(5),
            request: Duration::from_secs(5),
        };
        assert!(t.validate().is_err());
    }
}
