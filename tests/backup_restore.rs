//! Phase 5: exercise the real binary and exported artifacts after total loss.

#![cfg(target_os = "linux")]

use std::fs::OpenOptions;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dal::api::client::Client;
use dal::api::ops::{ClientReply, ClientRequest, WriteReply};
use dal::search::{
    FieldKind, GenerationSelection, PathSegment, ScoringMode, SearchConsistency, SearchField,
    SearchIndexDefinition, SearchQuery, SearchRequest, encode_search_value,
};
use dal::transport::Transport;
use dal::transport::codec::{Envelope, Lane, MsgType};
use dal::transport::dealer::ZmqTransport;
use dal::transport::security::CurveClientConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;

const CID: u128 = 0xDA1_BACC;
const OPERATION: &str = "0xabc51";
const BACKUP: &str = "0xabc52";
const BACKUP_ID: u128 = 0xabc52;

fn search_definition() -> SearchIndexDefinition {
    SearchIndexDefinition {
        document_type: "article".into(),
        fields: vec![SearchField {
            name: "title".into(),
            source_path: vec![PathSegment::Key("title".into())],
            kind: FieldKind::Text {
                tokenizer: "default".into(),
                positions: true,
            },
            required: true,
            multi_valued: false,
            indexed: true,
            stored: true,
            fast: false,
        }],
        default_search_fields: vec!["title".into()],
    }
}

fn indexed_value(title: &str) -> Vec<u8> {
    #[derive(serde::Serialize)]
    struct Article<'a> {
        title: &'a str,
    }
    encode_search_value("article", &flexbuffers::to_vec(Article { title }).unwrap()).unwrap()
}

fn search_request(query: &str) -> SearchRequest {
    SearchRequest {
        index: "articles".into(),
        generation: GenerationSelection::Active,
        query: SearchQuery::Text {
            query: query.into(),
            fields: Vec::new(),
        },
        limit: 10,
        offset: 0,
        sort: Vec::new(),
        scoring: ScoringMode::LocalBm25,
        consistency: SearchConsistency::Strict,
        allow_partial: false,
        deadline_ms: 5_000,
    }
}

async fn wait_for_search_hits<T: Transport>(
    client: &Client<T>,
    query: &str,
    expected: u64,
    key: Option<&[u8]>,
) {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if let Ok(reply) = client.search_active(&search_request(query)).await
            && reply.total_hits == expected
            && key.is_none_or(|key| {
                reply
                    .hits
                    .first()
                    .is_some_and(|hit| hit.key.as_slice() == key)
            })
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "search {query:?} did not return {expected} hits"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

struct Process {
    child: Child,
    log: PathBuf,
}

#[derive(Clone)]
struct WithheldWriteReply {
    inner: Arc<Mutex<ZmqTransport>>,
    hide_first_decision: Arc<AtomicBool>,
    decided: Arc<Notify>,
    resume: Arc<Notify>,
    original: Arc<Mutex<Option<WriteReply>>>,
}

impl Transport for WithheldWriteReply {
    async fn call(&self, addr: &str, request: Envelope) -> dal::Result<Envelope> {
        let mutation = request.msg_type == MsgType::ClientOp
            && matches!(
                dal::codec::decode::<ClientRequest>(&request.payload),
                Ok(ClientRequest::Mutate(_))
            );
        let inner = self.inner.lock().unwrap().clone();
        let reply = inner.call(addr, request).await?;
        if mutation
            && let Ok(ClientReply::Mutation(outcome @ WriteReply::Applied { .. })) =
                dal::codec::decode::<ClientReply>(&reply.payload)
            && self.hide_first_decision.swap(false, Ordering::AcqRel)
        {
            *self.original.lock().unwrap() = Some(outcome);
            self.decided.notify_one();
            self.resume.notified().await;
            return Err(dal::Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "simulated lost committed reply",
            )));
        }
        Ok(reply)
    }
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

async fn wait_for_meta_leader(addresses: &[String], processes: &mut [Process]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        for (index, address) in addresses.iter().enumerate() {
            if status(address)
                .await
                .is_some_and(|value| value["meta"]["is_leader"] == true)
            {
                return index;
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
            "meta quorum did not elect a leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_meta_convergence(addresses: &[String], processes: &mut [Process]) {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let mut applied = Vec::new();
        for address in addresses {
            let Some(value) = status(address).await else {
                break;
            };
            if value["maintenance"]["stage"] != "quiesced" {
                break;
            }
            let Some(index) = value["meta"]["applied"].as_u64() else {
                break;
            };
            applied.push(index);
        }
        if applied.len() == addresses.len() && applied.windows(2).all(|pair| pair[0] == pair[1]) {
            return;
        }
        for process in processes.iter_mut() {
            if let Some(exit) = process.child.try_wait().unwrap() {
                panic!(
                    "node exited before meta convergence ({exit}): {}",
                    process.log.display()
                );
            }
        }
        assert!(
            Instant::now() < deadline,
            "meta replicas did not reach one applied barrier: {applied:?}"
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
    spare: [String; 3],
}

impl Fixture {
    fn new(root: &Path) -> Self {
        let addrs = addresses(12);
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
            spare: [
                addrs[9].clone(),
                addrs[10].clone(),
                addrs[11].trim_start_matches("tcp://").to_string(),
            ],
        }
    }

    fn security(&self, generation: u64) -> CurveClientConfig {
        let node_keys = (0..4)
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
            "permissions": ["client_read", "client_write", "search"],
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
        for address in &self.spare[..2] {
            pins.insert(address.clone(), serde_json::json!("node:4"));
        }
        for i in 0..4 {
            let (config, data, control, bulk, http) = if i < 3 {
                (
                    self.configs[i].clone(),
                    self.dirs[i].clone(),
                    &self.controls[i],
                    &bulks[i],
                    &self.http[i],
                )
            } else {
                (
                    self.root.join("node-4.json"),
                    self.root.join("node-4-data"),
                    &self.spare[0],
                    &self.spare[1],
                    &self.spare[2],
                )
            };
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
            std::fs::write(&config, serde_json::to_vec(&serde_json::json!({
                "cluster_id": format!("{CID:#x}"), "node_id": i + 1,
                "control_addr": control, "bulk_addr": bulk,
                "http_addr": http,
                "seeds": self.controls.iter().enumerate().filter_map(|(peer, addr)|
                    (peer != i).then_some(addr)
                ).collect::<Vec<_>>(),
                "data_dir": data,
                "security": {
                    "trust_policy_file": policy,
                    "trust_generation_file": self.root.join(format!("node-{}-generation", i + 1)),
                    "curve_public_key_file": public,
                    "curve_secret_key_file": secret,
                    "server_key_pins": pins
                },
                // Keep the crashed participant Active long enough for the
                // authenticated begin command to install the fence. Raft
                // election and maintenance convergence do not wait for Down.
                "timeouts": {
                    "suspect_ms": 20000, "down_ms": 40000,
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

    fn set_failure_timeouts(&self, suspect_ms: u64, down_ms: u64) {
        for path in &self.configs {
            let mut config: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            config["timeouts"]["suspect_ms"].as_u64().unwrap();
            config["timeouts"]["down_ms"].as_u64().unwrap();
            config["timeouts"]["suspect_ms"] = serde_json::json!(suspect_ms);
            config["timeouts"]["down_ms"] = serde_json::json!(down_ms);
            std::fs::write(path, serde_json::to_vec(&config).unwrap()).unwrap();
        }
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
    let withheld = WithheldWriteReply {
        inner: Arc::new(Mutex::new(ZmqTransport::new_curve(
            zmq::Context::new(),
            Duration::from_secs(2),
            Lane::Control,
            old_curve,
        ))),
        hide_first_decision: Arc::new(AtomicBool::new(false)),
        decided: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
        original: Arc::new(Mutex::new(None)),
    };
    let client = Arc::new(Client::new(
        CID,
        0x50,
        fixture.controls.clone(),
        withheld.clone(),
    ));
    assert!(matches!(
        client
            .put(b"before-backup", b"retained", None)
            .await
            .unwrap(),
        WriteReply::Applied { .. }
    ));
    let mut indexed_version = None;
    for sequence in 2..=10 {
        let value = if sequence == 10 {
            indexed_value("retained searchable")
        } else {
            format!("retained-{sequence}").into_bytes()
        };
        let outcome = client
            .put(format!("before-backup-{sequence}").as_bytes(), &value, None)
            .await
            .unwrap();
        let version = match outcome {
            WriteReply::Applied { version } => version,
            other => panic!("pre-backup write {sequence} was not applied: {other:?}"),
        };
        if sequence == 10 {
            indexed_version = Some(version);
        }
    }
    let indexed_version = indexed_version.unwrap();
    let definition = fixture.root.join("articles.json");
    std::fs::write(
        &definition,
        serde_json::to_vec(&search_definition()).unwrap(),
    )
    .unwrap();
    cli(
        &[
            "index-create",
            "--auth",
            fixture.auth.to_str().unwrap(),
            "--cluster",
            fixture.cluster.to_str().unwrap(),
            "--name",
            "articles",
            "--definition",
            definition.to_str().unwrap(),
        ],
        true,
    );
    wait_for_search_hits(&client, "retained", 1, Some(b"before-backup-10")).await;
    let mut interrupted = nodes.remove(2);
    interrupted.child.kill().unwrap();
    interrupted.child.wait().unwrap();
    drop(interrupted);
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
    let preparing = wait_for(&fixture.http[0], &mut nodes, |value| {
        value["maintenance"]["stage"] == "preparing"
    })
    .await;
    assert!(
        preparing["maintenance"]["acknowledged"]
            .as_array()
            .is_some_and(|acks| acks.len() < 3)
    );
    for address in &fixture.http[..2] {
        assert!(!ready(address).await);
    }
    nodes.push(spawn(
        &fixture.configs[2],
        &fixture.cluster,
        &fixture.root.join("node-3-restarted.log"),
    ));
    let quiesced = wait_for(&fixture.http[0], &mut nodes, |value| {
        value["maintenance"]["stage"] == "quiesced"
    })
    .await;
    assert_eq!(
        quiesced["maintenance"]["acknowledged"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
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
    let old_leader = wait_for_meta_leader(&fixture.http, &mut nodes).await;
    let mut interrupted = nodes.remove(old_leader);
    stop(&mut interrupted).await;
    drop(interrupted);
    let surviving_http = fixture
        .http
        .iter()
        .enumerate()
        .filter_map(|(index, address)| (index != old_leader).then_some(address.clone()))
        .collect::<Vec<_>>();
    wait_for_meta_leader(&surviving_http, &mut nodes).await;
    for address in &surviving_http {
        let still_fenced = wait_for(address, &mut nodes, |value| {
            value["maintenance"]["stage"] == "quiesced"
        })
        .await;
        assert_eq!(
            still_fenced["maintenance"]["generation"],
            quiesced["maintenance"]["generation"]
        );
        assert!(!ready(address).await);
    }
    nodes.insert(
        old_leader,
        spawn(
            &fixture.configs[old_leader],
            &fixture.cluster,
            &fixture
                .root
                .join(format!("node-{}-restarted.log", old_leader + 1)),
        ),
    );
    wait_for(&fixture.http[old_leader], &mut nodes, |value| {
        value["maintenance"]["stage"] == "quiesced"
            && value["maintenance"]["acknowledged"]
                .as_array()
                .is_some_and(|acks| acks.len() == 3)
    })
    .await;
    assert!(!ready(&fixture.http[old_leader]).await);
    wait_for_meta_convergence(&fixture.http, &mut nodes).await;
    // A fenced attempt remains pending; use another identity so the backup
    // client's next successful mutation still has sequence 11.
    let fence_transport = withheld.inner.lock().unwrap().clone();
    let fenced_client = Client::new(CID, 0x60, fixture.controls.clone(), fence_transport);
    let fenced_write = fenced_client
        .put(b"during-fence", b"must-not-commit", None)
        .await
        .unwrap_err();
    assert!(
        matches!(
        fenced_write,
        dal::error::Error::Raft(ref message)
            if message == "write unavailable: maintenance admission is closed"
        ),
        "unexpected maintenance write outcome: {fenced_write}"
    );
    for node in &mut nodes {
        stop(node).await;
    }
    drop(nodes);
    for source in &fixture.dirs {
        assert!(source.join("search").exists());
    }

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
    for node_id in 1..=3 {
        assert!(!package.join(format!("node-{node_id}/db/search")).exists());
    }
    let node_manifest = package.join("node-1/node.json");
    let original = std::fs::read(&node_manifest).unwrap();
    std::fs::write(&node_manifest, b"truncated").unwrap();
    cli(
        &["backup-verify", "--package", package.to_str().unwrap()],
        false,
    );
    std::fs::write(&node_manifest, original).unwrap();

    // Resume the original cluster after the backup so its later acknowledged
    // history can diverge from the immutable recovery point.
    let mut resumed = fixture.start();
    for address in &fixture.http {
        wait_for(address, &mut resumed, |value| {
            value["maintenance"]["stage"] == "quiesced"
        })
        .await;
    }
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
    wait_for(&fixture.http[0], &mut resumed, |value| {
        value["maintenance"]["stage"] == "verifying"
            && value["maintenance"]["recovered"]
                .as_array()
                .is_some_and(|nodes| nodes.len() == 3)
    })
    .await;
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
        wait_until_ready(address, &mut resumed).await;
    }
    for sequence in 11..=20 {
        let value = if sequence == 11 {
            indexed_value("lost searchable")
        } else {
            format!("lost-{sequence}").into_bytes()
        };
        assert!(matches!(
            client
                .put(format!("after-backup-{sequence}").as_bytes(), &value, None,)
                .await
                .unwrap(),
            WriteReply::Applied { .. }
        ));
    }
    withheld.hide_first_decision.store(true, Ordering::Release);
    let pending_client = client.clone();
    let pending = tokio::spawn(async move {
        pending_client
            .put(b"after-backup-21", b"lost-21", None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(15), withheld.decided.notified())
        .await
        .expect("sequence 21 did not commit before its reply was withheld");
    assert!(matches!(
        withheld.original.lock().unwrap().as_ref(),
        Some(WriteReply::Applied { .. })
    ));
    let observer = Client::new(
        CID,
        0x60,
        fixture.controls.clone(),
        withheld.inner.lock().unwrap().clone(),
    );
    assert_eq!(
        observer.get(b"after-backup-21").await.unwrap().unwrap().1,
        b"lost-21"
    );
    // Dropping the in-flight call leaves Client::pending intact: mutate stores
    // the exact sequence and operation before awaiting the transport reply.
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    for node in &mut resumed {
        stop(node).await;
    }
    drop(resumed);

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
        assert!(!fixture.dirs[i].join("search").exists());
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
    let verified = wait_for(&fixture.http[0], &mut nodes, |value| {
        value["maintenance"]["stage"] == "verifying"
            && value["maintenance"]["recovered"]
                .as_array()
                .is_some_and(|nodes| nodes.len() == 3)
    })
    .await;
    let prior_processes = verified["maintenance"]["recovery_processes"].clone();
    for address in &fixture.http {
        assert!(!ready(address).await);
    }
    let leader = wait_for_meta_leader(&fixture.http, &mut nodes).await;
    let interrupted_index = (leader + 1) % 3;
    let mut interrupted = nodes.remove(interrupted_index);
    stop(&mut interrupted).await;
    drop(interrupted);
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
            "8",
        ],
        false,
    );
    for (index, address) in fixture.http.iter().enumerate() {
        if index == interrupted_index {
            continue;
        }
        wait_for(address, &mut nodes, |value| {
            value["maintenance"]["stage"] == "verifying"
        })
        .await;
        assert!(!ready(address).await);
    }
    nodes.insert(
        interrupted_index,
        spawn(
            &fixture.configs[interrupted_index],
            &fixture.cluster,
            &fixture
                .root
                .join(format!("node-{}-reverified.log", interrupted_index + 1)),
        ),
    );
    wait_for(&fixture.http[0], &mut nodes, |value| {
        value["maintenance"]["stage"] == "verifying"
            && value["maintenance"]["recovered"]
                .as_array()
                .is_some_and(|nodes| nodes.len() == 3)
            && value["maintenance"]["recovery_processes"] != prior_processes
    })
    .await;
    for address in &fixture.http {
        wait_for(address, &mut nodes, |value| {
            value["maintenance"]["stage"] == "verifying"
                && value["maintenance"]["recovered"]
                    .as_array()
                    .is_some_and(|nodes| nodes.len() == 3)
                && value["partitions"].as_array().is_some_and(|partitions| {
                    !partitions.is_empty()
                        && partitions.iter().all(|partition| {
                            partition["search"].as_array().is_some_and(|indexes| {
                                indexes.iter().any(|index| {
                                    index["name"] == "articles"
                                        && index["searchable_through"]
                                            .as_u64()
                                            .is_some_and(|position| position >= indexed_version)
                                        && (index["state"] == "Active"
                                            || index["state"] == "Building")
                                })
                            })
                        })
                })
        })
        .await;
    }
    for source in &fixture.dirs {
        assert!(source.join("search").exists());
    }
    assert!(!ready(&fixture.http[interrupted_index]).await);
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
    let retained_sequence = cli(
        &[
            "sequence-inspect",
            "--auth",
            fixture.auth.to_str().unwrap(),
            "--cluster",
            fixture.cluster.to_str().unwrap(),
            "--partition",
            "0",
            "--client",
            "0x50",
        ],
        true,
    );
    assert!(retained_sequence.contains("highest sequence: 10"));
    assert!(retained_sequence.contains("result: Applied"));
    let transport = ZmqTransport::new_curve(
        zmq::Context::new(),
        Duration::from_secs(2),
        Lane::Control,
        new_curve,
    );
    *withheld.inner.lock().unwrap() = transport.clone();
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
    let wrapper_probe = Client::new(CID, 0x60, fixture.controls.clone(), withheld.clone());
    assert_eq!(
        wrapper_probe
            .get(b"before-backup")
            .await
            .unwrap()
            .unwrap()
            .1,
        b"retained"
    );
    let retry_error = tokio::time::timeout(
        Duration::from_secs(30),
        client.put(b"after-backup-21", b"lost-21", None),
    )
    .await
    .expect("old-stream retry did not complete")
    .unwrap_err();
    assert!(
        matches!(retry_error, dal::Error::Raft(ref message) if message.contains("recovery epoch mismatch")),
        "unexpected old-stream retry outcome: {retry_error}"
    );
    let different_error = client
        .put(b"different-old-stream", b"blocked", None)
        .await
        .unwrap_err();
    assert!(
        matches!(different_error, dal::Error::Raft(ref message) if message.contains("mutation sequence 21") && message.contains("is unresolved")),
        "unexpected old-stream pending outcome: {different_error}"
    );
    for sequence in 2..=10 {
        let expected = if sequence == 10 {
            indexed_value("retained searchable")
        } else {
            format!("retained-{sequence}").into_bytes()
        };
        assert_eq!(
            restored
                .get(format!("before-backup-{sequence}").as_bytes())
                .await
                .unwrap()
                .unwrap()
                .1,
            expected
        );
    }
    for sequence in 11..=21 {
        assert!(
            restored
                .get(format!("after-backup-{sequence}").as_bytes())
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(restored.get(b"during-fence").await.unwrap().is_none());
    wait_for_search_hits(&restored, "retained", 1, Some(b"before-backup-10")).await;
    wait_for_search_hits(&restored, "lost", 0, None).await;
    assert!(matches!(
        restored.put(b"after-restore", b"new", None).await.unwrap(),
        WriteReply::Applied { .. }
    ));

    // A post-restore join that never hosts a group cannot learn the recovery
    // epoch from a snapshot. It must still participate in maintenance, including
    // after restarting while admission is closed.
    let spare_config = fixture.root.join("node-4.json");
    cli(
        &[
            "join",
            "--config",
            spare_config.to_str().unwrap(),
            "--auth",
            fixture.auth.to_str().unwrap(),
        ],
        true,
    );
    nodes.push(spawn(
        &spare_config,
        &fixture.cluster,
        &fixture.root.join("node-4.log"),
    ));
    wait_until_ready(&fixture.spare[2], &mut nodes).await;
    let spare_status = status(&fixture.spare[2]).await.unwrap();
    assert!(spare_status["partitions"].as_array().unwrap().is_empty());
    assert!(spare_status["meta"].is_null());
    cli(
        &[
            "maintenance-begin",
            "--auth",
            fixture.auth.to_str().unwrap(),
            "--cluster",
            fixture.cluster.to_str().unwrap(),
            "--operation",
            "0x992",
            "--wait-seconds",
            "30",
        ],
        true,
    );
    let closed = wait_for(&fixture.http[0], &mut nodes, |value| {
        value["maintenance"]["stage"] == "quiesced"
    })
    .await;
    let next_generation = closed["maintenance"]["generation"]
        .as_u64()
        .unwrap()
        .to_string();
    assert_eq!(
        closed["maintenance"]["participants"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
    stop(&mut nodes[3]).await;
    nodes[3] = spawn(
        &spare_config,
        &fixture.cluster,
        &fixture.root.join("node-4-restarted.log"),
    );
    wait_for(&fixture.spare[2], &mut nodes, |value| {
        value["maintenance_admission_open"] == false
    })
    .await;
    cli(
        &[
            "maintenance-verify",
            "--auth",
            fixture.auth.to_str().unwrap(),
            "--cluster",
            fixture.cluster.to_str().unwrap(),
            "--operation",
            "0x992",
            "--generation",
            &next_generation,
            "--wait-seconds",
            "30",
        ],
        true,
    );
    let verified = wait_for(&fixture.http[0], &mut nodes, |value| {
        value["maintenance"]["recovered"]
            .as_array()
            .is_some_and(|ids| ids.len() == 4)
    })
    .await;
    assert_eq!(verified["maintenance"]["stage"], "verifying");
    cli(
        &[
            "maintenance-release",
            "--auth",
            fixture.auth.to_str().unwrap(),
            "--cluster",
            fixture.cluster.to_str().unwrap(),
            "--operation",
            "0x992",
            "--generation",
            &next_generation,
            "--wait-seconds",
            "30",
        ],
        true,
    );
    wait_until_ready(&fixture.spare[2], &mut nodes).await;
    for node in &mut nodes {
        stop(node).await;
    }
    let spare =
        dal::storage::Storage::open_checked(&fixture.root.join("node-4-data"), CID, 4).unwrap();
    assert_eq!(spare.local_recovery_epoch().unwrap(), epoch);
    assert!(!spare.group_exists(dal::types::GroupId::Meta));
    assert!(!spare.group_exists(dal::types::GroupId::Data(0)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_reply_lost_before_maintenance_replays_after_release() {
    assert_eq!(zmq::has("curve"), Some(true));
    const DOWN_MS: u64 = 8_000;
    let temp = tempfile::tempdir().unwrap();
    let fixture = Fixture::new(temp.path());
    let curve = fixture.security(1);
    fixture.set_failure_timeouts(3_000, DOWN_MS);
    let mut nodes = fixture.start();
    for address in &fixture.http {
        wait_until_ready(address, &mut nodes).await;
    }

    let transport = ZmqTransport::new_curve(
        zmq::Context::new(),
        Duration::from_secs(2),
        Lane::Control,
        curve,
    );
    let withheld = WithheldWriteReply {
        inner: Arc::new(Mutex::new(transport.clone())),
        hide_first_decision: Arc::new(AtomicBool::new(true)),
        decided: Arc::new(Notify::new()),
        resume: Arc::new(Notify::new()),
        original: Arc::new(Mutex::new(None)),
    };
    let client = Arc::new(Client::new(
        CID,
        0x50,
        fixture.controls.clone(),
        withheld.clone(),
    ));
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.put(b"reply-lost", b"saved", None).await });
    tokio::time::timeout(Duration::from_secs(15), withheld.decided.notified())
        .await
        .expect("write did not commit before its reply was withheld");
    let original = withheld.original.lock().unwrap().clone().unwrap();
    let observer = Client::new(CID, 0x60, fixture.controls.clone(), transport);
    let before_fence = observer.get(b"reply-lost").await.unwrap().unwrap();
    assert_eq!(before_fence.1, b"saved");
    assert_eq!(
        original,
        WriteReply::Applied {
            version: before_fence.0
        }
    );

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
    for address in &fixture.http {
        wait_for(address, &mut nodes, |value| {
            value["maintenance"]["stage"] == "quiesced"
                && value["maintenance"]["generation"] == quiesced["maintenance"]["generation"]
        })
        .await;
        assert!(!ready(address).await);
    }

    withheld.resume.notify_one();
    let first_error = first.await.unwrap().unwrap_err();
    assert!(
        matches!(
            first_error,
            dal::Error::Raft(ref message)
                if message == "write unavailable: maintenance admission is closed"
        ),
        "unexpected first write error: {first_error}"
    );
    let different_error = client
        .put(b"different", b"blocked", None)
        .await
        .unwrap_err();
    assert!(matches!(
        different_error,
        dal::Error::Raft(ref message) if message.contains("is unresolved")
    ));
    let frozen_directory = status(&fixture.http[0]).await.unwrap()["directory"].clone();
    let stopped_index = (wait_for_meta_leader(&fixture.http, &mut nodes).await + 1) % 3;
    let mut stopped = nodes.remove(stopped_index);
    stop(&mut stopped).await;
    drop(stopped);
    tokio::time::sleep(Duration::from_millis(DOWN_MS + 2_000)).await;
    for (index, address) in fixture.http.iter().enumerate() {
        if index == stopped_index {
            continue;
        }
        assert_eq!(
            status(address).await.unwrap()["directory"],
            frozen_directory
        );
        assert!(!ready(address).await);
    }
    nodes.insert(
        stopped_index,
        spawn(
            &fixture.configs[stopped_index],
            &fixture.cluster,
            &fixture
                .root
                .join(format!("node-{}-long-stop.log", stopped_index + 1)),
        ),
    );
    wait_for(&fixture.http[stopped_index], &mut nodes, |value| {
        value["maintenance"]["stage"] == "quiesced" && value["directory"] == frozen_directory
    })
    .await;
    assert!(!ready(&fixture.http[stopped_index]).await);
    wait_for_meta_convergence(&fixture.http, &mut nodes).await;
    // Stop the quorum before waiting on any child: stopping sequentially can
    // elect a new leader and advance one replica beyond the backup barrier.
    for node in &mut nodes {
        node.child.kill().unwrap();
    }
    for node in &mut nodes {
        node.child.wait().unwrap();
    }
    drop(nodes);
    let artifacts = (0..3)
        .map(|index| fixture.root.join(format!("resume-artifact-{}", index + 1)))
        .collect::<Vec<_>>();
    for (index, artifact) in artifacts.iter().enumerate() {
        let node_id = (index + 1).to_string();
        cli(
            &[
                "backup-export-node",
                "--config",
                fixture.configs[index].to_str().unwrap(),
                "--cluster",
                fixture.cluster.to_str().unwrap(),
                "--backup",
                BACKUP,
                "--operation",
                OPERATION,
                "--generation",
                &generation,
                "--destination",
                artifact.to_str().unwrap(),
            ],
            true,
        );
        assert!(
            artifact.join("node.json").exists(),
            "node {node_id} did not export"
        );
    }
    let package = fixture.root.join("resume-package");
    let catalog = fixture.root.join("resume-catalog.json");
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
    for artifact in &artifacts {
        assembly.extend(["--artifact", artifact.to_str().unwrap()]);
    }
    cli(&assembly, true);
    cli(
        &["backup-verify", "--package", package.to_str().unwrap()],
        true,
    );
    let mut nodes = fixture.start();
    for address in &fixture.http {
        wait_for(address, &mut nodes, |value| {
            value["maintenance"]["stage"] == "quiesced"
        })
        .await;
        assert!(!ready(address).await);
    }
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
                .is_some_and(|recovered| recovered.len() == fixture.http.len())
    })
    .await;
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
    tokio::time::sleep(Duration::from_millis(DOWN_MS + 2_000)).await;
    for address in &fixture.http {
        assert_eq!(
            status(address).await.unwrap()["directory"],
            frozen_directory
        );
        assert!(ready(address).await);
    }
    assert_eq!(
        client.put(b"reply-lost", b"saved", None).await.unwrap(),
        original
    );
    assert_eq!(
        observer.get(b"reply-lost").await.unwrap().unwrap(),
        before_fence
    );
    assert!(matches!(
        client.put(b"different", b"next", None).await.unwrap(),
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
    let data_index = meta_index + 10;
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
        assert!(matches!(
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
                meta_index + 1,
            )
            .unwrap(),
            ApplyResult::Decided(_)
        ));
        for sequence in 2..=10 {
            assert!(matches!(
                data.apply(
                    &storage,
                    &DataRequest {
                        recovery_epoch: 0,
                        client_id: 0x50,
                        sequence,
                        op: DataOp::Put {
                            key: format!("before-backup-{sequence}").into_bytes(),
                            value: format!("retained-{sequence}").into_bytes(),
                            if_version: None,
                        },
                    },
                    meta_index + sequence,
                )
                .unwrap(),
                ApplyResult::Decided(_)
            ));
        }
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
    // Model the later source history after the immutable package's sequence-10
    // recovery point. Sequences 11–20 stand for acknowledged writes, and 21
    // stands for a decided write whose reply was lost. The direct state-machine
    // fixture does not exercise admission or client reply delivery.
    for (id, source) in ids.iter().zip(&sources) {
        let storage = Storage::open_checked(source, CID, *id).unwrap();
        let data = DataStateMachine::new(GroupId::Data(0));
        for sequence in 11..=21 {
            assert!(matches!(
                data.apply(
                    &storage,
                    &DataRequest {
                        recovery_epoch: 0,
                        client_id: 0x50,
                        sequence,
                        op: DataOp::Put {
                            key: format!("after-backup-{sequence}").into_bytes(),
                            value: format!("lost-{sequence}").into_bytes(),
                            if_version: None,
                        },
                    },
                    data_index + sequence - 10,
                )
                .unwrap(),
                ApplyResult::Decided(_)
            ));
        }
        assert_eq!(
            data.inspect_sequence(&storage, 0x50)
                .unwrap()
                .unwrap()
                .highest,
            21
        );
        assert_eq!(
            data.get(&storage, b"after-backup-21").unwrap().unwrap().1,
            b"lost-21"
        );
    }
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
        let retained = data.inspect_sequence(&storage, 0x50).unwrap().unwrap();
        assert_eq!(retained.highest, 10);
        assert_eq!(
            retained.result,
            dal::types::MutationResult::Applied {
                version: data_index
            }
        );
        for sequence in 2..=10 {
            assert_eq!(
                data.get(&storage, format!("before-backup-{sequence}").as_bytes())
                    .unwrap()
                    .unwrap()
                    .1,
                format!("retained-{sequence}").as_bytes()
            );
        }
        for sequence in 11..=21 {
            assert!(
                data.get(&storage, format!("after-backup-{sequence}").as_bytes())
                    .unwrap()
                    .is_none()
            );
        }
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
        for (offset, (key, value)) in [
            (b"old-next".as_slice(), b"rejected".as_slice()),
            (b"after-backup-21".as_slice(), b"lost-21".as_slice()),
        ]
        .into_iter()
        .enumerate()
        {
            assert!(matches!(
                data.apply(
                    &storage,
                    &DataRequest {
                        recovery_epoch: 0,
                        client_id: 0x50,
                        sequence: 21,
                        op: DataOp::Put {
                            key: key.to_vec(),
                            value: value.to_vec(),
                            if_version: None,
                        },
                    },
                    data_index + offset as u64 + 1,
                )
                .unwrap(),
                ApplyResult::Rejected(RejectReason::RecoveryEpochMismatch { .. })
            ));
            assert!(data.get(&storage, key).unwrap().is_none());
        }
        assert_eq!(
            data.inspect_sequence(&storage, 0x50).unwrap().unwrap(),
            retained
        );
    }
}
