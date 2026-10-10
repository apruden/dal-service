//! Phase 5: exercise the real binary and exported artifacts after total loss.

#![cfg(target_os = "linux")]

use std::fs::OpenOptions;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use dal::api::client::Client;
use dal::api::ops::WriteReply;
use dal::transport::codec::Lane;
use dal::transport::dealer::ZmqTransport;
use dal::transport::security::CurveClientConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CID: u128 = 0xDA1_BACC;
const OPERATION: &str = "0xabc51";
const BACKUP: &str = "0xabc52";
const BACKUP_ID: u128 = 0xabc52;

struct Process {
    child: Child,
    log: PathBuf,
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn addresses(count: usize) -> Vec<String> {
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|listener| format!("tcp://{}", listener.local_addr().unwrap()))
        .collect()
}

fn protected_write(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

fn cli(args: &[&str], success: bool) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_dal"))
        .args(args)
        .output()
        .unwrap();
    assert_eq!(
        output.status.success(),
        success,
        "dal {}: stdout={} stderr={}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn spawn(config: &Path, cluster: &Path, log: &Path) -> Process {
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    Process {
        child: Command::new(env!("CARGO_BIN_EXE_dal"))
            .args(["run", "--config"])
            .arg(config)
            .arg("--cluster")
            .arg(cluster)
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .spawn()
            .unwrap(),
        log: log.to_path_buf(),
    }
}

async fn stop(process: &mut Process) {
    assert!(
        Command::new("kill")
            .args(["-TERM", &process.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(exit) = process.child.try_wait().unwrap() {
            assert!(
                exit.success(),
                "{}",
                std::fs::read_to_string(&process.log).unwrap()
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node did not stop: {}",
            process.log.display()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn status(address: &str) -> Option<serde_json::Value> {
    let mut stream = tokio::net::TcpStream::connect(address).await.ok()?;
    stream
        .write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .ok()?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response))
        .await
        .ok()?
        .ok()?;
    if !response.starts_with(b"HTTP/1.1 200") {
        return None;
    }
    let body = response.windows(4).position(|part| part == b"\r\n\r\n")? + 4;
    serde_json::from_slice(&response[body..]).ok()
}

async fn ready(address: &str) -> bool {
    let Ok(mut stream) = tokio::net::TcpStream::connect(address).await else {
        return false;
    };
    if stream
        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .is_err()
    {
        return false;
    }
    let mut response = Vec::new();
    matches!(
        tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response)).await,
        Ok(Ok(_))
    ) && response.starts_with(b"HTTP/1.1 200")
}

async fn wait_for<F: Fn(&serde_json::Value) -> bool>(
    address: &str,
    processes: &mut [Process],
    predicate: F,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut last_status = None;
    loop {
        if let Some(value) = status(address).await {
            if predicate(&value) {
                return value;
            }
            last_status = Some(value);
        }
        for process in processes.iter_mut() {
            if let Some(exit) = process.child.try_wait().unwrap() {
                panic!(
                    "node exited ({exit}): {}",
                    std::fs::read_to_string(&process.log).unwrap_or_default()
                );
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for cluster state at {address}: {last_status:?}; logs: {:?}",
            processes
                .iter()
                .map(|process| {
                    std::fs::read_to_string(&process.log)
                        .unwrap_or_default()
                        .lines()
                        .rev()
                        .take(8)
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_until_ready(address: &str, processes: &mut [Process]) {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if ready(address).await {
            return;
        }
        for process in processes.iter_mut() {
            if let Some(exit) = process.child.try_wait().unwrap() {
                panic!(
                    "node exited ({exit}): {}",
                    std::fs::read_to_string(&process.log).unwrap_or_default()
                );
            }
        }
        assert!(
            Instant::now() < deadline,
            "node did not become ready at {address}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_meta_leader(addresses: &[String], processes: &mut [Process]) {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        for address in addresses {
            if status(address)
                .await
                .is_some_and(|value| value["meta"]["is_leader"] == true)
            {
                return;
            }
        }
        for process in processes.iter_mut() {
            if let Some(exit) = process.child.try_wait().unwrap() {
                panic!(
                    "node exited before meta election ({exit}): {}",
                    process.log.display()
                );
            }
        }
        assert!(
            Instant::now() < deadline,
            "restored meta quorum did not elect a leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

struct Fixture {
    cluster: PathBuf,
    configs: Vec<PathBuf>,
    dirs: Vec<PathBuf>,
    controls: Vec<String>,
    http: Vec<String>,
    auth: PathBuf,
    root: PathBuf,
}

impl Fixture {
    fn new(root: &Path) -> Self {
        let addrs = addresses(9);
        let controls = addrs[0..3].to_vec();
        let bulks = addrs[3..6].to_vec();
        let http = addrs[6..9]
            .iter()
            .map(|addr| addr.trim_start_matches("tcp://").to_string())
            .collect::<Vec<_>>();
        let cluster = root.join("cluster.json");
        let nodes = (0..3)
            .map(|i| {
                serde_json::json!({
                    "node_id": i + 1,
                    "control_addr": controls[i],
                    "bulk_addr": bulks[i]
                })
            })
            .collect::<Vec<_>>();
        std::fs::write(
            &cluster,
            serde_json::to_vec(&serde_json::json!({
                "cluster_id": format!("{CID:#x}"), "p": 1, "r": 3,
                "meta_voters": [1, 2, 3], "nodes": nodes
            }))
            .unwrap(),
        )
        .unwrap();
        let configs = (0..3)
            .map(|i| root.join(format!("node-{}.json", i + 1)))
            .collect();
        let dirs = (0..3)
            .map(|i| root.join(format!("node-{}-data", i + 1)))
            .collect();
        Self {
            cluster,
            configs,
            dirs,
            controls,
            http,
            auth: root.join("operator.json"),
            root: root.to_path_buf(),
        }
    }

    fn security(&self, generation: u64) -> CurveClientConfig {
        let node_keys = (0..3)
            .map(|_| zmq::CurveKeyPair::new().unwrap())
            .collect::<Vec<_>>();
        let app = zmq::CurveKeyPair::new().unwrap();
        let operator = zmq::CurveKeyPair::new().unwrap();
        let policy = self.root.join("trust-policy.json");
        let mut principals = node_keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                serde_json::json!({
                    "user_id": format!("node:{}", i + 1),
                    "curve_public_key": zmq::z85_encode(&key.public_key).unwrap(),
                    "permissions": ["peer_rpc"]
                })
            })
            .collect::<Vec<_>>();
        principals.push(serde_json::json!({
            "user_id": "app:recovery",
            "curve_public_key": zmq::z85_encode(&app.public_key).unwrap(),
            "permissions": ["client_read", "client_write"],
            "client_ids": ["0x50", "0x60", "0x61"]
        }));
        principals.push(serde_json::json!({
            "user_id": "ops:recovery",
            "curve_public_key": zmq::z85_encode(&operator.public_key).unwrap(),
            "permissions": ["operator"]
        }));
        protected_write(
            &policy,
            &serde_json::to_vec(&serde_json::json!({
                "cluster_id": format!("{CID:#x}"), "generation": generation,
                "principals": principals
            }))
            .unwrap(),
        );
        let mut pins = serde_json::Map::new();
        for i in 0..3 {
            pins.insert(
                self.controls[i].clone(),
                serde_json::json!(format!("node:{}", i + 1)),
            );
        }
        let bulks =
            serde_json::from_slice::<serde_json::Value>(&std::fs::read(&self.cluster).unwrap())
                .unwrap()["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|node| node["bulk_addr"].as_str().unwrap().to_string())
                .collect::<Vec<_>>();
        for i in 0..3 {
            pins.insert(
                bulks[i].clone(),
                serde_json::json!(format!("node:{}", i + 1)),
            );
        }
        for i in 0..3 {
            let public = self.root.join(format!("node-{}-public.key", i + 1));
            let secret = self.root.join(format!("node-{}-secret.key", i + 1));
            protected_write(
                &public,
                zmq::z85_encode(&node_keys[i].public_key)
                    .unwrap()
                    .as_bytes(),
            );
            protected_write(
                &secret,
                zmq::z85_encode(&node_keys[i].secret_key)
                    .unwrap()
                    .as_bytes(),
            );
            std::fs::write(&self.configs[i], serde_json::to_vec(&serde_json::json!({
                "cluster_id": format!("{CID:#x}"), "node_id": i + 1,
                "control_addr": self.controls[i], "bulk_addr": bulks[i],
                "http_addr": self.http[i],
                "seeds": self.controls.iter().enumerate().filter_map(|(peer, addr)|
                    (peer != i).then_some(addr)
                ).collect::<Vec<_>>(),
                "data_dir": self.dirs[i],
                "security": {
                    "trust_policy_file": policy,
                    "trust_generation_file": self.root.join(format!("node-{}-generation", i + 1)),
                    "curve_public_key_file": public,
                    "curve_secret_key_file": secret,
                    "server_key_pins": pins
                },
                "timeouts": {
                    "suspect_ms": 3000, "down_ms": 12000,
                    "request_ms": 500, "shutdown_ms": 10000
                }
            })).unwrap()).unwrap();
        }
        let op_public = self.root.join("operator-public.key");
        let op_secret = self.root.join("operator-secret.key");
        protected_write(
            &op_public,
            zmq::z85_encode(&operator.public_key).unwrap().as_bytes(),
        );
        protected_write(
            &op_secret,
            zmq::z85_encode(&operator.secret_key).unwrap().as_bytes(),
        );
        std::fs::write(
            &self.auth,
            serde_json::to_vec(&serde_json::json!({
                "cluster_id": format!("{CID:#x}"),
                "trust_policy_file": policy,
                "trust_generation_file": self.root.join("operator-generation"),
                "user_id": "ops:recovery",
                "curve_public_key_file": op_public,
                "curve_secret_key_file": op_secret,
                "server_key_pins": pins
            }))
            .unwrap(),
        )
        .unwrap();
        let keys = self
            .controls
            .iter()
            .chain(bulks.iter())
            .enumerate()
            .map(|(i, addr)| (addr.clone(), node_keys[i % 3].public_key))
            .collect();
        CurveClientConfig::new(app.public_key, app.secret_key, keys).unwrap()
    }

    fn start(&self) -> Vec<Process> {
        (0..3)
            .map(|i| {
                spawn(
                    &self.configs[i],
                    &self.cluster,
                    &self.root.join(format!("node-{}.log", i + 1)),
                )
            })
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_cli_backup_restore_remains_fenced_until_verified() {
    assert_eq!(
        zmq::has("curve"),
        Some(true),
        "Phase 5 restore verification requires CURVE-enabled libzmq"
    );
    let temp = tempfile::tempdir().unwrap();
    let fixture = Fixture::new(temp.path());
    let old_curve = fixture.security(1);
    let mut nodes = fixture.start();
    for address in &fixture.http {
        wait_until_ready(address, &mut nodes).await;
    }
    let client = Client::new(
        CID,
        0x50,
        fixture.controls.clone(),
        ZmqTransport::new_curve(
            zmq::Context::new(),
            Duration::from_secs(2),
            Lane::Control,
            old_curve,
        ),
    );
    assert!(matches!(
        client
            .put(b"before-backup", b"retained", None)
            .await
            .unwrap(),
        WriteReply::Applied { .. }
    ));
    cli(
        &[
            "maintenance-begin",
            "--auth",
            fixture.auth.to_str().unwrap(),
            "--cluster",
            fixture.cluster.to_str().unwrap(),
            "--operation",
            OPERATION,
        ],
        true,
    );
    let quiesced = wait_for(&fixture.http[0], &mut nodes, |value| {
        value["maintenance"]["stage"] == "quiesced"
    })
    .await;
    let generation = quiesced["maintenance"]["generation"]
        .as_u64()
        .unwrap()
        .to_string();
    for address in &fixture.http[1..] {
        wait_for(address, &mut nodes, |value| {
            value["maintenance"]["stage"] == "quiesced"
        })
        .await;
    }
    for address in &fixture.http {
        assert!(!ready(address).await);
    }
    let fenced_write = client
        .put(b"during-fence", b"must-not-commit", None)
        .await
        .unwrap_err();
    assert!(
        fenced_write.to_string().contains("write unavailable"),
        "unexpected maintenance write outcome: {fenced_write}"
    );
    for node in &mut nodes {
        stop(node).await;
    }
    drop(nodes);

    let artifacts = (0..3)
        .map(|i| fixture.root.join(format!("artifact-{}", i + 1)))
        .collect::<Vec<_>>();
    for i in 0..3 {
        cli(
            &[
                "backup-export-node",
                "--config",
                fixture.configs[i].to_str().unwrap(),
                "--cluster",
                fixture.cluster.to_str().unwrap(),
                "--backup",
                BACKUP,
                "--operation",
                OPERATION,
                "--generation",
                &generation,
                "--destination",
                artifacts[i].to_str().unwrap(),
            ],
            true,
        );
    }
    let package = fixture.root.join("package");
    let catalog = fixture.root.join("catalog.json");
    let mut assembly = vec![
        "backup-assemble",
        "--auth",
        fixture.auth.to_str().unwrap(),
        "--cluster",
        fixture.cluster.to_str().unwrap(),
        "--destination",
        package.to_str().unwrap(),
        "--catalog",
        catalog.to_str().unwrap(),
    ];
    for artifact in &artifacts[..2] {
        assembly.extend(["--artifact", artifact.to_str().unwrap()]);
    }
    cli(&assembly, false);
    assembly.extend(["--artifact", artifacts[2].to_str().unwrap()]);
    cli(&assembly, true);
    cli(
        &["backup-verify", "--package", package.to_str().unwrap()],
        true,
    );
    let node_manifest = package.join("node-1/node.json");
    let original = std::fs::read(&node_manifest).unwrap();
    std::fs::write(&node_manifest, b"truncated").unwrap();
    cli(
        &["backup-verify", "--package", package.to_str().unwrap()],
        false,
    );
    std::fs::write(&node_manifest, original).unwrap();

    let evidence = fixture.root.join("fencing.json");
    let record = fixture.root.join("recovery.json");
    cli(
        &[
            "restore-init",
            "--package",
            package.to_str().unwrap(),
            "--catalog",
            catalog.to_str().unwrap(),
            "--fencing-evidence",
            evidence.to_str().unwrap(),
            "--record",
            record.to_str().unwrap(),
        ],
        false,
    );
    std::fs::write(
        &evidence,
        serde_json::to_vec(&serde_json::json!({
            "cluster_id": format!("{CID:#x}"), "backup_id": BACKUP,
            "checked_by": "isolated test harness", "old_processes_stopped": true,
            "automatic_restart_disabled": true, "old_peer_and_client_routes_isolated": true
        }))
        .unwrap(),
    )
    .unwrap();
    cli(
        &[
            "restore-init",
            "--package",
            package.to_str().unwrap(),
            "--catalog",
            catalog.to_str().unwrap(),
            "--fencing-evidence",
            evidence.to_str().unwrap(),
            "--record",
            record.to_str().unwrap(),
        ],
        true,
    );
    let epoch: u128 =
        serde_json::from_slice::<dal::backup::RecoveryPlan>(&std::fs::read(&record).unwrap())
            .unwrap()
            .recovery_epoch;
    for i in 0..3 {
        std::fs::remove_dir_all(&fixture.dirs[i]).unwrap();
        let args = [
            "restore",
            "--config",
            fixture.configs[i].to_str().unwrap(),
            "--package",
            package.to_str().unwrap(),
            "--record",
            record.to_str().unwrap(),
            "--node",
            match i {
                0 => "1",
                1 => "2",
                _ => "3",
            },
            "--destination",
            fixture.dirs[i].to_str().unwrap(),
        ];
        let mut dry = args.to_vec();
        dry.push("--dry-run");
        cli(&dry, true);
        assert!(!fixture.dirs[i].exists());
        cli(&args, true);
        cli(&args, false);
    }
    // The backup's old credentials must be rejected before restored peers start.
    let mut stale = spawn(
        &fixture.configs[0],
        &fixture.cluster,
        &fixture.root.join("stale.log"),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(exit) = stale.child.try_wait().unwrap() {
            assert!(!exit.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restored node accepted old policy"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    drop(stale);

    let new_curve = fixture.security(2);
    let mut nodes = fixture.start();
    let quiesced = wait_for(&fixture.http[0], &mut nodes, |value| {
        value["maintenance"]["stage"] == "quiesced"
    })
    .await;
    assert_eq!(
        quiesced["maintenance"]["generation"]
            .as_u64()
            .unwrap()
            .to_string(),
        generation
    );
    for address in &fixture.http[1..] {
        wait_for(address, &mut nodes, |value| {
            value["maintenance"]["stage"] == "quiesced"
        })
        .await;
    }
    for address in &fixture.http {
        assert!(!ready(address).await);
    }
    wait_for_meta_leader(&fixture.http, &mut nodes).await;
    cli(
        &[
            "maintenance-verify",
            "--auth",
            fixture.auth.to_str().unwrap(),
            "--cluster",
            fixture.cluster.to_str().unwrap(),
            "--operation",
            OPERATION,
            "--generation",
            &generation,
        ],
        true,
    );
    wait_for(&fixture.http[0], &mut nodes, |value| {
        value["maintenance"]["stage"] == "verifying"
            && value["maintenance"]["recovered"]
                .as_array()
                .is_some_and(|nodes| nodes.len() == 3)
    })
    .await;
    for address in &fixture.http {
        assert!(!ready(address).await);
    }
    cli(
        &[
            "maintenance-release",
            "--auth",
            fixture.auth.to_str().unwrap(),
            "--cluster",
            fixture.cluster.to_str().unwrap(),
            "--operation",
            OPERATION,
            "--generation",
            &generation,
            "--wait-seconds",
            "30",
        ],
        true,
    );
    for address in &fixture.http {
        wait_until_ready(address, &mut nodes).await;
    }
    let transport = ZmqTransport::new_curve(
        zmq::Context::new(),
        Duration::from_secs(2),
        Lane::Control,
        new_curve,
    );
    let old_epoch = Client::new(CID, 0x60, fixture.controls.clone(), transport.clone());
    assert!(
        old_epoch
            .put(b"old-epoch", b"rejected", None)
            .await
            .is_err()
    );
    let restored =
        Client::new_for_recovery_epoch(CID, 0x61, epoch, fixture.controls.clone(), transport);
    assert_eq!(
        restored.get(b"before-backup").await.unwrap().unwrap().1,
        b"retained"
    );
    assert!(restored.get(b"during-fence").await.unwrap().is_none());
    assert!(matches!(
        restored.put(b"after-restore", b"new", None).await.unwrap(),
        WriteReply::Applied { .. }
    ));
    for node in &mut nodes {
        stop(node).await;
    }
}

#[test]
fn offline_artifacts_survive_cli_verification_and_restore() {
    use dal::backup::{self, TrustSnapshot};
    use dal::meta::bootstrap::{BootstrapDescriptor, DirEntry};
    use dal::meta::state_machine::{MetaApplyResult, MetaStateMachine};
    use dal::partition::{ApplyResult, DataStateMachine, RejectReason};
    use dal::storage::{StateMutation, Storage};
    use dal::types::{
        ClusterConfig, DataOp, DataRequest, GroupId, HashSpec, LogId, MaintenanceRecord,
        MetaCommand, PROTOCOL_VERSION,
    };

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let ids = vec![1, 2, 3];
    let meta_index = 2 * ids.len() as u64 + 4;
    let data_index = meta_index + 1;
    let descriptor = BootstrapDescriptor {
        cluster_id: CID,
        config: ClusterConfig {
            cluster_id: CID,
            protocol_version: PROTOCOL_VERSION,
            p: 1,
            r: 3,
            hash_spec: HashSpec::CANONICAL,
        },
        meta_voters: ids.clone(),
        directory: ids
            .iter()
            .map(|id| DirEntry {
                node_id: *id,
                control_addr: format!("tcp://127.0.0.1:1{id}01"),
                bulk_addr: format!("tcp://127.0.0.1:1{id}02"),
            })
            .collect(),
        data_placements: vec![(0, ids.clone())],
    };
    let trust = TrustSnapshot {
        generation: 1,
        digest: [7; 32],
        key_fingerprints: vec!["11".repeat(32)],
    };
    let mut sources = Vec::new();
    for id in &ids {
        let source = root.join(format!("source-{id}"));
        let storage = Storage::open_checked(&source, CID, *id).unwrap();
        storage.ensure_group(GroupId::Meta).unwrap();
        storage.ensure_group(GroupId::Data(0)).unwrap();
        let meta = MetaStateMachine::new();
        let mut commands = vec![MetaCommand::ClusterInit {
            config: descriptor.config.clone(),
            meta_voters: ids.clone(),
        }];
        commands.extend(
            descriptor
                .directory
                .iter()
                .map(|node| MetaCommand::RegisterNode {
                    node_id: node.node_id,
                    control_addr: node.control_addr.clone(),
                    bulk_addr: node.bulk_addr.clone(),
                }),
        );
        commands.push(MetaCommand::SeedPlacement {
            group: GroupId::Data(0),
            voters: ids.clone(),
        });
        commands.push(MetaCommand::BeginMaintenance {
            operation_id: 0xabc51,
            generation: 1,
            participants: ids.clone(),
        });
        commands.extend(
            ids.iter()
                .map(|node_id| MetaCommand::AcknowledgeMaintenance {
                    operation_id: 0xabc51,
                    generation: 1,
                    node_id: *node_id,
                    incarnation: 1,
                }),
        );
        commands.push(MetaCommand::QuiesceMaintenance {
            operation_id: 0xabc51,
            generation: 1,
        });
        assert_eq!(commands.len() as u64, meta_index);
        for (index, command) in commands.iter().enumerate() {
            assert_eq!(
                meta.apply(&storage, command, LogId::new(1, index as u64 + 1))
                    .unwrap(),
                MetaApplyResult::Applied
            );
        }
        let data = DataStateMachine::new(GroupId::Data(0));
        data.apply(
            &storage,
            &DataRequest {
                recovery_epoch: 0,
                client_id: 0x50,
                sequence: 1,
                op: DataOp::Put {
                    key: b"before-backup".to_vec(),
                    value: b"retained".to_vec(),
                    if_version: None,
                },
            },
            data_index,
        )
        .unwrap();
        // The running Raft adapter writes this record. The direct state-machine
        // setup above does not, so install its equivalent before offline export.
        for (group, index) in [(GroupId::Meta, meta_index), (GroupId::Data(0), data_index)] {
            // Keep the Raft term distinct from apply_state's direct-apply term
            // so a regression to last_applied_key fails this export fixture.
            let applied = openraft::LogId::new(openraft::CommittedLeaderId::new(2, 1), index);
            let record: (
                Option<openraft::LogId<u64>>,
                openraft::StoredMembership<u64, openraft::BasicNode>,
            ) = (Some(applied), openraft::StoredMembership::default());
            storage
                .apply_state(
                    group,
                    &[StateMutation::Put {
                        key: dal::keyspace::raft_applied_key(),
                        value: dal::codec::encode(&record),
                    }],
                    LogId::new(1, index),
                )
                .unwrap();
            assert_eq!(
                storage.last_applied(group).unwrap(),
                Some(LogId::new(1, index))
            );
            assert_eq!(
                storage.raft_applied_position(group).unwrap(),
                Some(LogId::new(2, index))
            );
        }
        let fence: MaintenanceRecord = storage
            .get_state_record(GroupId::Meta, &dal::keyspace::meta_maintenance_key())
            .unwrap()
            .unwrap();
        storage
            .put_local(&dal::keyspace::maintenance_fence_key(), &fence)
            .unwrap();
        drop(storage);
        sources.push(source);
    }
    let artifacts = ids
        .iter()
        .map(|id| root.join(format!("artifact-{id}")))
        .collect::<Vec<_>>();
    let export_partial = root.join(format!(".dal-backup-{BACKUP_ID:032x}-node-1.partial"));
    std::fs::create_dir(&export_partial).unwrap();
    std::fs::write(export_partial.join("interrupted"), b"incomplete").unwrap();
    let error = backup::export_node(
        &descriptor,
        ids[0],
        &sources[0],
        BACKUP_ID,
        0xabc51,
        1,
        &artifacts[0],
        trust.clone(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("partial backup already exists"));
    assert!(!artifacts[0].exists());
    assert!(export_partial.join("interrupted").exists());
    std::fs::remove_dir_all(&export_partial).unwrap();
    for (id, (source, artifact)) in ids.iter().zip(sources.iter().zip(&artifacts)) {
        backup::export_node(
            &descriptor,
            *id,
            source,
            BACKUP_ID,
            0xabc51,
            1,
            artifact,
            trust.clone(),
        )
        .unwrap();
        let exported: backup::NodeArtifact =
            serde_json::from_slice(&std::fs::read(artifact.join("node.json")).unwrap()).unwrap();
        assert_eq!(exported.positions.len(), 2);
        for position in &exported.positions {
            let index = match position.group {
                GroupId::Meta => meta_index,
                GroupId::Data(0) => data_index,
                other => panic!("unexpected exported group: {other:?}"),
            };
            assert_eq!(position.applied, LogId::new(2, index));
        }
    }
    let package = root.join("package");
    assert!(
        backup::assemble(
            &descriptor,
            &artifacts[..2],
            &root.join("incomplete"),
            trust.clone()
        )
        .is_err()
    );
    let third_manifest = artifacts[2].join("node.json");
    let third_bytes = std::fs::read(&third_manifest).unwrap();
    let mut mixed: backup::NodeArtifact = serde_json::from_slice(&third_bytes).unwrap();
    mixed.backup_id += 1;
    std::fs::write(&third_manifest, serde_json::to_vec(&mixed).unwrap()).unwrap();
    assert!(backup::assemble(&descriptor, &artifacts, &root.join("mixed"), trust.clone()).is_err());
    mixed.backup_id -= 1;
    mixed.positions.clear();
    std::fs::write(&third_manifest, serde_json::to_vec(&mixed).unwrap()).unwrap();
    assert!(
        backup::assemble(
            &descriptor,
            &artifacts,
            &root.join("missing-group"),
            trust.clone()
        )
        .is_err()
    );
    std::fs::write(&third_manifest, third_bytes).unwrap();
    let package_partial = root.join(format!(".dal-package-{BACKUP_ID:032x}.partial"));
    std::fs::create_dir(&package_partial).unwrap();
    std::fs::write(package_partial.join("interrupted"), b"incomplete").unwrap();
    let error = backup::assemble(&descriptor, &artifacts, &package, trust.clone()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("partial backup package already exists")
    );
    assert!(!package.exists());
    assert!(package_partial.join("interrupted").exists());
    std::fs::remove_dir_all(&package_partial).unwrap();
    backup::assemble(&descriptor, &artifacts, &package, trust.clone()).unwrap();
    cli(
        &["backup-verify", "--package", package.to_str().unwrap()],
        true,
    );
    let node_manifest = package.join("node-2/node.json");
    let bytes = std::fs::read(&node_manifest).unwrap();
    std::fs::write(&node_manifest, b"truncated").unwrap();
    cli(
        &["backup-verify", "--package", package.to_str().unwrap()],
        false,
    );
    std::fs::write(&node_manifest, bytes).unwrap();
    let package_manifest = package.join("manifest.json");
    let manifest_bytes = std::fs::read(&package_manifest).unwrap();
    let mut unknown: backup::PackageManifest = serde_json::from_slice(&manifest_bytes).unwrap();
    unknown.format = u32::MAX;
    std::fs::write(&package_manifest, serde_json::to_vec(&unknown).unwrap()).unwrap();
    let error = backup::verify_package(&package).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported backup package format")
    );
    cli(
        &["backup-verify", "--package", package.to_str().unwrap()],
        false,
    );
    std::fs::write(&package_manifest, manifest_bytes).unwrap();
    let catalog = root.join("catalog.json");
    cli(
        &[
            "backup-catalog-add",
            "--package",
            package.to_str().unwrap(),
            "--catalog",
            catalog.to_str().unwrap(),
        ],
        true,
    );
    let evidence = root.join("evidence.json");
    let record = root.join("record.json");
    std::fs::write(
        &evidence,
        serde_json::to_vec(&serde_json::json!({
            "cluster_id": format!("{CID:#x}"), "backup_id": BACKUP,
            "checked_by": "offline test", "old_processes_stopped": true,
            "automatic_restart_disabled": true, "old_peer_and_client_routes_isolated": true
        }))
        .unwrap(),
    )
    .unwrap();
    cli(
        &[
            "restore-init",
            "--package",
            package.to_str().unwrap(),
            "--catalog",
            catalog.to_str().unwrap(),
            "--fencing-evidence",
            evidence.to_str().unwrap(),
            "--record",
            record.to_str().unwrap(),
        ],
        true,
    );
    let plan: backup::RecoveryPlan =
        serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
    assert_ne!(plan.recovery_epoch, 0);
    for (id, source) in ids.iter().zip(&sources) {
        std::fs::remove_dir_all(source).unwrap();
        let config = root.join(format!("restore-{id}.json"));
        let node = descriptor
            .directory
            .iter()
            .find(|node| node.node_id == *id)
            .unwrap();
        std::fs::write(
            &config,
            serde_json::to_vec(&serde_json::json!({
                "cluster_id": format!("{CID:#x}"), "node_id": id,
                "control_addr": node.control_addr, "bulk_addr": node.bulk_addr,
                "data_dir": source
            }))
            .unwrap(),
        )
        .unwrap();
        let id_text = id.to_string();
        let args = [
            "restore",
            "--config",
            config.to_str().unwrap(),
            "--package",
            package.to_str().unwrap(),
            "--record",
            record.to_str().unwrap(),
            "--node",
            &id_text,
            "--destination",
            source.to_str().unwrap(),
        ];
        let mut dry = args.to_vec();
        dry.push("--dry-run");
        cli(&dry, true);
        assert!(!source.exists());
        cli(&args, true);
        cli(&args, false);
        let storage = Storage::open_checked(source, CID, *id).unwrap();
        for (group, index) in [(GroupId::Meta, meta_index), (GroupId::Data(0), data_index)] {
            assert_eq!(
                storage.last_applied(group).unwrap(),
                Some(LogId::new(1, index))
            );
            assert_eq!(
                storage.raft_applied_position(group).unwrap(),
                Some(LogId::new(2, index))
            );
        }
        assert_eq!(storage.local_recovery_epoch().unwrap(), plan.recovery_epoch);
        assert_eq!(
            storage.recovery_epoch(GroupId::Meta).unwrap(),
            plan.recovery_epoch
        );
        assert_eq!(
            storage.recovery_epoch(GroupId::Data(0)).unwrap(),
            plan.recovery_epoch
        );
        let data = DataStateMachine::new(GroupId::Data(0));
        assert_eq!(
            data.get(&storage, b"before-backup").unwrap().unwrap().1,
            b"retained"
        );
        assert_eq!(
            data.inspect_sequence(&storage, 0x50)
                .unwrap()
                .unwrap()
                .highest,
            1
        );
        assert!(
            backup::validate_restored_node(&storage, CID, *id, 1, Some(trust.clone())).is_err()
        );
        assert!(
            backup::validate_restored_node(
                &storage,
                CID,
                *id,
                1,
                Some(TrustSnapshot {
                    generation: 2,
                    digest: [8; 32],
                    key_fingerprints: trust.key_fingerprints.clone(),
                }),
            )
            .is_err()
        );
        backup::validate_restored_node(
            &storage,
            CID,
            *id,
            1,
            Some(TrustSnapshot {
                generation: 2,
                digest: [8; 32],
                key_fingerprints: vec!["22".repeat(32)],
            }),
        )
        .unwrap();
        assert!(matches!(
            data.apply(
                &storage,
                &DataRequest {
                    recovery_epoch: 0,
                    client_id: 0x50,
                    sequence: 2,
                    op: DataOp::Put {
                        key: b"old-epoch".to_vec(),
                        value: b"rejected".to_vec(),
                        if_version: None,
                    },
                },
                11,
            )
            .unwrap(),
            ApplyResult::Rejected(RejectReason::RecoveryEpochMismatch { .. })
        ));
        assert!(data.get(&storage, b"old-epoch").unwrap().is_none());
    }
}
