//! Real-binary lifecycle coverage for the production signal path.

#![cfg(target_os = "linux")]

use std::fs::OpenOptions;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use dal::api::client::Client;
use dal::api::ops::WriteReply;
use dal::transport::codec::Lane;
use dal::transport::dealer::ZmqTransport;
use dal::transport::security::CurveClientConfig;

const CID: u128 = 0xDA1_51A1;

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

fn tcp_addrs(count: usize) -> Vec<String> {
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|listener| format!("tcp://{}", listener.local_addr().unwrap()))
        .collect()
}

fn spawn_node(config: &Path, cluster: &Path, log: &Path) -> Process {
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_dal"))
        .arg("run")
        .arg("--config")
        .arg(config)
        .arg("--cluster")
        .arg(cluster)
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .spawn()
        .unwrap();
    Process {
        child,
        log: log.to_path_buf(),
    }
}

fn signal(process: &Process, name: &str) {
    assert!(
        Command::new("kill")
            .arg(format!("-{name}"))
            .arg(process.child.id().to_string())
            .status()
            .unwrap()
            .success()
    );
}

async fn wait_exit(process: &mut Process, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = process.child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            let log = std::fs::read_to_string(&process.log).unwrap_or_default();
            panic!("process {} did not exit; log:\n{log}", process.child.id());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn http_ok(address: &str, path: &str) -> bool {
    let Ok(mut stream) = tokio::net::TcpStream::connect(address).await else {
        return false;
    };
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).await.is_err() {
        return false;
    }
    let mut response = Vec::new();
    matches!(
        tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response)).await,
        Ok(Ok(_))
    ) && response.starts_with(b"HTTP/1.1 200")
}

async fn status_json(address: &str) -> Option<serde_json::Value> {
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
    let body = response.windows(4).position(|bytes| bytes == b"\r\n\r\n")? + 4;
    serde_json::from_slice(&response[body..]).ok()
}

async fn wait_ready(address: &str, process: &mut Process) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if http_ok(address, "/readyz").await {
            return;
        }
        if let Some(status) = process.child.try_wait().unwrap() {
            let log = std::fs::read_to_string(&process.log).unwrap_or_default();
            panic!("process exited before ready ({status}); log:\n{log}");
        }
        assert!(Instant::now() < deadline, "process never became ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn write_cluster(
    root: &Path,
) -> (
    PathBuf,
    Vec<PathBuf>,
    Vec<String>,
    Vec<String>,
    Vec<PathBuf>,
    CurveClientConfig,
) {
    let addrs = tcp_addrs(9);
    let controls = addrs[0..3].to_vec();
    let bulks = addrs[3..6].to_vec();
    let http: Vec<String> = addrs[6..9]
        .iter()
        .map(|address| address.trim_start_matches("tcp://").to_string())
        .collect();
    let node_keys: Vec<_> = (0..3).map(|_| zmq::CurveKeyPair::new().unwrap()).collect();
    let client_keys = zmq::CurveKeyPair::new().unwrap();
    let nodes: Vec<_> = (0..3)
        .map(|index| {
            serde_json::json!({
                "node_id": index + 1,
                "control_addr": controls[index],
                "bulk_addr": bulks[index],
            })
        })
        .collect();
    let cluster = root.join("cluster.json");
    std::fs::write(
        &cluster,
        serde_json::to_vec(&serde_json::json!({
            "cluster_id": format!("{CID:#x}"),
            "p": 1,
            "r": 3,
            "meta_voters": [1, 2, 3],
            "nodes": nodes,
        }))
        .unwrap(),
    )
    .unwrap();

    let mut configs = Vec::new();
    let mut data_dirs = Vec::new();
    let policy_path = root.join("trust-policy.json");
    let mut principals: Vec<_> = node_keys
        .iter()
        .enumerate()
        .map(|(index, keys)| {
            serde_json::json!({
                "user_id": format!("node:{}", index + 1),
                "curve_public_key": zmq::z85_encode(&keys.public_key).unwrap(),
                "permissions": ["peer_rpc"]
            })
        })
        .collect();
    principals.push(serde_json::json!({
        "user_id": "app:lifecycle",
        "curve_public_key": zmq::z85_encode(&client_keys.public_key).unwrap(),
        "permissions": ["client_read", "client_write"]
    }));
    protected_write(
        &policy_path,
        &serde_json::to_vec(&serde_json::json!({
            "cluster_id": format!("{CID:#x}"), "generation": 1, "principals": principals
        }))
        .unwrap(),
    );
    let server_pins: serde_json::Map<String, serde_json::Value> = (0..3)
        .flat_map(|index| {
            [
                (controls[index].clone(), index),
                (bulks[index].clone(), index),
            ]
        })
        .map(|(endpoint, index)| {
            (
                endpoint,
                serde_json::Value::String(format!("node:{}", index + 1)),
            )
        })
        .collect();
    for index in 0..3 {
        let data_dir = root.join(format!("node-{}-data", index + 1));
        let config = root.join(format!("node-{}.json", index + 1));
        let public_key_path = root.join(format!("node-{}-public.key", index + 1));
        let secret_key_path = root.join(format!("node-{}-secret.key", index + 1));
        let generation_path = root.join(format!("node-{}-trust-generation", index + 1));
        protected_write(
            &public_key_path,
            zmq::z85_encode(&node_keys[index].public_key)
                .unwrap()
                .as_bytes(),
        );
        protected_write(
            &secret_key_path,
            zmq::z85_encode(&node_keys[index].secret_key)
                .unwrap()
                .as_bytes(),
        );
        std::fs::write(
            &config,
            serde_json::to_vec(&serde_json::json!({
                "cluster_id": format!("{CID:#x}"),
                "node_id": index + 1,
                "control_addr": controls[index],
                "bulk_addr": bulks[index],
                "http_addr": http[index],
                "seeds": controls.iter().enumerate().filter_map(|(peer, address)| {
                    (peer != index).then_some(address)
                }).collect::<Vec<_>>(),
                "data_dir": data_dir,
                "security": {
                    "trust_policy_file": policy_path,
                    "trust_generation_file": generation_path,
                    "curve_public_key_file": public_key_path,
                    "curve_secret_key_file": secret_key_path,
                    "server_key_pins": server_pins
                },
                "timeouts": {
                    "suspect_ms": 3000,
                    "down_ms": 15000,
                    "request_ms": 500,
                    "shutdown_ms": 10000
                }
            }))
            .unwrap(),
        )
        .unwrap();
        configs.push(config);
        data_dirs.push(data_dir);
    }
    let server_keys = controls
        .iter()
        .chain(bulks.iter())
        .enumerate()
        .map(|(index, addr)| {
            let node = if index < 3 { index } else { index - 3 };
            (addr.clone(), node_keys[node].public_key)
        })
        .collect();
    let client_curve =
        CurveClientConfig::new(client_keys.public_key, client_keys.secret_key, server_keys)
            .unwrap();
    (cluster, configs, controls, http, data_dirs, client_curve)
}

fn protected_write(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}

fn require_curve_support_or_skip() -> bool {
    if zmq::has("curve") == Some(true) {
        return true;
    }
    assert_ne!(
        std::env::var("DAL_REQUIRE_CURVE_TESTS").as_deref(),
        Ok("1"),
        "CI requires a libzmq build with CURVE support"
    );
    eprintln!("skipping: libzmq has no CURVE support");
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_during_bootstrap_drains_and_reopens_the_data_directory() {
    let root = tempfile::tempdir().unwrap();
    if !require_curve_support_or_skip() {
        return;
    }
    let (cluster, configs, _controls, _http, data_dirs, _client_curve) = write_cluster(root.path());
    let log = root.path().join("bootstrap.log");

    for _ in 0..2 {
        let mut process = spawn_node(&configs[0], &cluster, &log);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !data_dirs[0].exists() {
            assert!(
                Instant::now() < deadline,
                "node never opened its data directory"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
        signal(&process, "TERM");
        let status = wait_exit(&mut process, Duration::from_secs(10)).await;
        assert!(
            status.success(),
            "signal during bootstrap was not a clean stop ({status}); log:\n{}",
            std::fs::read_to_string(&log).unwrap_or_default()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_and_sigint_stop_and_restart_a_running_cluster() {
    let root = tempfile::tempdir().unwrap();
    if !require_curve_support_or_skip() {
        return;
    }
    let (cluster, configs, controls, http, _data_dirs, client_curve) = write_cluster(root.path());
    let logs: Vec<_> = (0..3)
        .map(|index| root.path().join(format!("node-{}.log", index + 1)))
        .collect();
    let mut processes: Vec<_> = (0..3)
        .map(|index| spawn_node(&configs[index], &cluster, &logs[index]))
        .collect();
    for index in 0..3 {
        wait_ready(&http[index], &mut processes[index]).await;
    }

    let client = Client::new(
        CID,
        91,
        controls.clone(),
        ZmqTransport::new_curve(
            zmq::Context::new(),
            Duration::from_secs(2),
            Lane::Control,
            client_curve,
        ),
    );
    let version = match client
        .put(b"survives-signal", b"value", None)
        .await
        .unwrap()
    {
        WriteReply::Applied { version } => version,
        other => panic!("unexpected write result: {other:?}"),
    };

    let leader = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            for (index, address) in http.iter().enumerate() {
                if status_json(address).await.is_some_and(|status| {
                    status
                        .pointer("/meta/is_leader")
                        .and_then(|value| value.as_bool())
                        == Some(true)
                }) {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("meta leader not reported");
    let failover_started = Instant::now();
    signal(&processes[leader], "TERM");
    let status = wait_exit(&mut processes[leader], Duration::from_secs(15)).await;
    assert!(status.success());
    let failover = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            for (index, address) in http.iter().enumerate() {
                if index != leader
                    && status_json(address).await.is_some_and(|status| {
                        status
                            .pointer("/meta/is_leader")
                            .and_then(|value| value.as_bool())
                            == Some(true)
                    })
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    if failover.is_err() {
        let logs = logs
            .iter()
            .map(|log| std::fs::read_to_string(log).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n--- node ---\n");
        panic!("meta leadership did not recover within the profile gate:\n{logs}");
    }
    assert!(failover_started.elapsed() < Duration::from_secs(15));
    processes[leader] = spawn_node(&configs[leader], &cluster, &logs[leader]);
    wait_ready(&http[leader], &mut processes[leader]).await;
    assert_eq!(
        client.get(b"survives-signal").await.unwrap(),
        Some((version, b"value".to_vec()))
    );

    for process in &processes {
        signal(process, "INT");
    }
    for process in &mut processes {
        let status = wait_exit(process, Duration::from_secs(15)).await;
        assert!(
            status.success(),
            "SIGINT stop failed; log:\n{}",
            std::fs::read_to_string(&process.log).unwrap_or_default()
        );
    }
}
