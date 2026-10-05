//! Ignored release benchmark using three `dal run` processes over TCP.
//! Set DAL_BENCH_DIR to a durable filesystem and vary DAL_BENCH_VALUE_BYTES
//! (128, 4096, 65536, or 1048576) and DAL_BENCH_TRIAL_ID across trials.
//! DAL_BENCH_PARTITIONS, DAL_BENCH_CLIENTS, and DAL_BENCH_WRITES set load.
//! DAL_BENCH_SEARCHES enables concurrent searches per client; DAL_BENCH_REBUILD=1
//! starts a rebuild during that workload. DAL_BENCH_WAIT_SNAPSHOT=1 waits for
//! a reported Raft snapshot after foreground work. On Linux, a one-partition
//! run with more than 5,500 writes can set DAL_BENCH_PAUSE_FOLLOWER=1 to force
//! one voter to recover through a snapshot. DAL_BENCH_BINARY selects a node
//! binary; DAL_BENCH_KEEP_DIR=1 retains its node files and logs.
//! DAL_BENCH_RESUME_AFTER_WRITES and DAL_BENCH_REQUIRE_SNAPSHOT=0 can instead
//! exercise lagging-follower log catch-up below the snapshot threshold.
//! DAL_BENCH_INDEXABLE_VALUES=1 fills each benchmark value with an indexed
//! title and an unindexed padding field up to the selected value size.
//! DAL_BENCH_WAIT_SEARCH=1 waits for every active local projection to catch up
//! and checks the distributed hit count after foreground timing stops.

use std::fs::File;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use dal::api::client::Client;
use dal::api::ops::WriteReply;
use dal::search::{
    FieldKind, GenerationSelection, PathSegment, ScoringMode, SearchConsistency, SearchField,
    SearchIndexDefinition, SearchQuery, SearchRequest, encode_search_value,
};
use dal::transport::codec::Lane;
use dal::transport::dealer::ZmqTransport;
use dal::types::MAX_VALUE_BYTES;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CID: u128 = 0xDABA_2026;

struct Processes(Vec<Child>);

impl Drop for Processes {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn tcp_addrs(count: usize) -> Vec<String> {
    // Keep every reservation live until all endpoints are selected. Releasing
    // one ephemeral port before choosing the next can return the same port
    // twice and make a node fail bootstrap with EADDRINUSE.
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners
        .iter()
        .map(|listener| format!("tcp://{}", listener.local_addr().unwrap()))
        .collect()
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned integer")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("cannot read {name}: {error}"),
    }
}

fn percentile(samples: &mut [Duration], percent: usize) -> f64 {
    samples.sort_unstable();
    let rank = samples.len().saturating_mul(percent).div_ceil(100).max(1);
    samples[rank - 1].as_secs_f64() * 1000.0
}

#[cfg(target_os = "linux")]
fn resident_kib(pid: u32) -> Option<usize> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

#[cfg(target_os = "linux")]
fn cpu_ticks(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // `comm` is parenthesized and may contain spaces. Fields after its closing
    // parenthesis start at field 3; utime and stime are fields 14 and 15.
    let rest = stat.rsplit_once(") ")?.1;
    let fields: Vec<_> = rest.split_whitespace().collect();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

#[cfg(target_os = "linux")]
fn io_pressure_full_micros() -> Option<u64> {
    let pressure = std::fs::read_to_string("/proc/pressure/io").ok()?;
    pressure.lines().find_map(|line| {
        line.strip_prefix("full ")?
            .split_whitespace()
            .find_map(|field| field.strip_prefix("total=")?.parse().ok())
    })
}

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

fn search_request() -> SearchRequest {
    SearchRequest {
        index: "benchmark".into(),
        generation: GenerationSelection::Active,
        query: SearchQuery::Text {
            query: "benchmark".into(),
            fields: Vec::new(),
        },
        limit: 10,
        offset: 0,
        sort: Vec::new(),
        scoring: ScoringMode::LocalBm25,
        consistency: SearchConsistency::Eventual,
        allow_partial: false,
        deadline_ms: 5_000,
    }
}

fn benchmark_value(value_bytes: usize, indexable: bool) -> Vec<u8> {
    if !indexable {
        return vec![b'x'; value_bytes];
    }
    #[derive(Serialize)]
    struct SearchDocument<'a> {
        title: &'a str,
        padding: &'a str,
    }
    let padding = "x".repeat(value_bytes);
    let encode = |length: usize| {
        let payload = flexbuffers::to_vec(SearchDocument {
            title: "benchmark",
            padding: &padding[..length],
        })
        .unwrap();
        encode_search_value("article", &payload).unwrap()
    };
    assert!(
        encode(0).len() <= value_bytes,
        "value size too small for an indexable document"
    );
    let (mut low, mut high) = (0, value_bytes);
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if encode(middle).len() <= value_bytes {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let value = encode(low);
    assert!(value.len() <= value_bytes);
    value
}

async fn status(addr: &str) -> Option<serde_json::Value> {
    let mut stream = tokio::net::TcpStream::connect(addr).await.ok()?;
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

#[cfg(target_os = "linux")]
fn signal(pid: u32, name: &str) {
    assert!(
        Command::new("kill")
            .arg(format!("-{name}"))
            .arg(pid.to_string())
            .status()
            .expect("cannot run kill")
            .success(),
        "cannot send {name} to benchmark node {pid}"
    );
}

#[derive(Default)]
struct NodeSamples {
    peak_rss_kib: usize,
    first_cpu_ticks: Option<u64>,
    last_cpu_ticks: Option<u64>,
    peak_outbox_entries: u64,
    peak_outbox_bytes: u64,
    peak_pending_bytes: usize,
    peak_wal_pending_requests: usize,
    peak_wal_pending_bytes: usize,
    peak_rocks_memtables_bytes: u64,
    peak_rocks_unflushed_memtables_bytes: u64,
    peak_rocks_table_readers_bytes: u64,
    peak_rocks_cache_bytes: u64,
    peak_tantivy_writer_budget_bytes: u64,
    max_snapshot_index: u64,
    snapshot_builds: u64,
    snapshot_build_max_micros: u64,
    snapshot_installs: u64,
    snapshot_install_max_micros: u64,
    search_user_records_scanned: u64,
    search_outbox_entries_scanned: u64,
    search_source_row_peak_bytes: u64,
    snapshot_record_peak_bytes: u64,
    status_samples: usize,
    first_wal_syncs: Option<u64>,
    last_wal_syncs: Option<u64>,
    first_wal_bytes: Option<u64>,
    last_wal_bytes: Option<u64>,
    first_stall_micros: Option<u64>,
    last_stall_micros: Option<u64>,
}

async fn sample_nodes(
    pids: Vec<u32>,
    status_addrs: Vec<String>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> Vec<NodeSamples> {
    let mut samples: Vec<NodeSamples> = (0..pids.len()).map(|_| NodeSamples::default()).collect();
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        tokio::select! {
            _ = &mut stop => break,
            _ = tick.tick() => {
                for (i, pid) in pids.iter().enumerate() {
                    #[cfg(target_os = "linux")]
                    {
                        if let Some(rss) = resident_kib(*pid) {
                            samples[i].peak_rss_kib = samples[i].peak_rss_kib.max(rss);
                        }
                        if let Some(cpu) = cpu_ticks(*pid) {
                            samples[i].first_cpu_ticks.get_or_insert(cpu);
                            samples[i].last_cpu_ticks = Some(cpu);
                        }
                    }
                    if let Some(state) = status(&status_addrs[i]).await {
                        samples[i].status_samples += 1;
                        if let Some(value) = state["storage_wal_syncs"].as_u64() {
                            samples[i].first_wal_syncs.get_or_insert(value);
                            samples[i].last_wal_syncs = Some(value);
                        }
                        if let Some(value) = state["storage_wal_bytes"].as_u64() {
                            samples[i].first_wal_bytes.get_or_insert(value);
                            samples[i].last_wal_bytes = Some(value);
                        }
                        if let Some(value) = state["storage_stall_micros"].as_u64() {
                            samples[i].first_stall_micros.get_or_insert(value);
                            samples[i].last_stall_micros = Some(value);
                        }
                        if let Some(partitions) = state["partitions"].as_array() {
                            for partition in partitions {
                                samples[i].peak_outbox_entries = samples[i].peak_outbox_entries.max(partition["search_outbox_entries"].as_u64().unwrap_or(0));
                                samples[i].peak_outbox_bytes = samples[i].peak_outbox_bytes.max(partition["search_outbox_bytes"].as_u64().unwrap_or(0));
                                samples[i].peak_pending_bytes = samples[i].peak_pending_bytes.max(partition["materialized_pending_bytes"].as_u64().unwrap_or(0) as usize);
                                samples[i].max_snapshot_index = samples[i].max_snapshot_index.max(partition["raft_snapshot_index"].as_u64().unwrap_or(0));
                            }
                        }
                        samples[i].peak_wal_pending_requests = samples[i].peak_wal_pending_requests.max(state["storage_wal_peak_pending_requests"].as_u64().unwrap_or(0) as usize);
                        samples[i].peak_wal_pending_bytes = samples[i].peak_wal_pending_bytes.max(state["storage_wal_peak_pending_bytes"].as_u64().unwrap_or(0) as usize);
                        samples[i].peak_rocks_memtables_bytes = samples[i].peak_rocks_memtables_bytes.max(state["storage_rocks_memtables_bytes"].as_u64().unwrap_or(0));
                        samples[i].peak_rocks_unflushed_memtables_bytes = samples[i].peak_rocks_unflushed_memtables_bytes.max(state["storage_rocks_unflushed_memtables_bytes"].as_u64().unwrap_or(0));
                        samples[i].peak_rocks_table_readers_bytes = samples[i].peak_rocks_table_readers_bytes.max(state["storage_rocks_table_readers_bytes"].as_u64().unwrap_or(0));
                        samples[i].peak_rocks_cache_bytes = samples[i].peak_rocks_cache_bytes.max(state["storage_rocks_cache_bytes"].as_u64().unwrap_or(0));
                        samples[i].peak_tantivy_writer_budget_bytes = samples[i].peak_tantivy_writer_budget_bytes.max(state["search_tantivy_writer_budget_bytes"].as_u64().unwrap_or(0));
                        samples[i].snapshot_builds = samples[i].snapshot_builds.max(state["snapshot_builds"].as_u64().unwrap_or(0));
                        samples[i].snapshot_build_max_micros = samples[i].snapshot_build_max_micros.max(state["snapshot_build_max_micros"].as_u64().unwrap_or(0));
                        samples[i].snapshot_installs = samples[i].snapshot_installs.max(state["snapshot_installs"].as_u64().unwrap_or(0));
                        samples[i].snapshot_install_max_micros = samples[i].snapshot_install_max_micros.max(state["snapshot_install_max_micros"].as_u64().unwrap_or(0));
                        samples[i].search_user_records_scanned = samples[i].search_user_records_scanned.max(state["search_user_records_scanned"].as_u64().unwrap_or(0));
                        samples[i].search_outbox_entries_scanned = samples[i].search_outbox_entries_scanned.max(state["search_outbox_entries_scanned"].as_u64().unwrap_or(0));
                        samples[i].search_source_row_peak_bytes = samples[i].search_source_row_peak_bytes.max(state["search_source_row_peak_bytes"].as_u64().unwrap_or(0));
                        samples[i].snapshot_record_peak_bytes = samples[i].snapshot_record_peak_bytes.max(state["snapshot_record_peak_bytes"].as_u64().unwrap_or(0));
                    }
                }
            }
        }
    }
    samples
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "release benchmark: run with --ignored --nocapture and DAL_BENCH_DIR on a durable filesystem"]
async fn three_process_tcp_mixed_workload() {
    let value_bytes = env_usize("DAL_BENCH_VALUE_BYTES", 128);
    assert!((1..=MAX_VALUE_BYTES).contains(&value_bytes));
    let clients = env_usize("DAL_BENCH_CLIENTS", 4).max(1);
    let writes_per_client = env_usize("DAL_BENCH_WRITES", 50).max(1);
    let searches_per_client = env_usize("DAL_BENCH_SEARCHES", 0);
    let indexable_values = env_usize("DAL_BENCH_INDEXABLE_VALUES", 0) != 0;
    let wait_search = env_usize("DAL_BENCH_WAIT_SEARCH", 0) != 0;
    let rebuild = env_usize("DAL_BENCH_REBUILD", 0) != 0;
    let pause_follower = env_usize("DAL_BENCH_PAUSE_FOLLOWER", 0) != 0;
    let resume_after_writes = env_usize("DAL_BENCH_RESUME_AFTER_WRITES", 5_500);
    let require_follower_snapshot = env_usize("DAL_BENCH_REQUIRE_SNAPSHOT", 1) != 0;
    assert!(
        !rebuild || searches_per_client > 0,
        "rebuild requires search workload"
    );
    assert!(
        !indexable_values || searches_per_client > 0,
        "indexable values require search workload"
    );
    assert!(
        !wait_search || searches_per_client > 0,
        "search catch-up requires search workload"
    );
    let partitions = env_usize("DAL_BENCH_PARTITIONS", 4);
    assert!((1..=u16::MAX as usize).contains(&partitions));
    assert!(
        !pause_follower || partitions == 1,
        "follower pause requires one partition"
    );
    if pause_follower {
        assert!(
            clients * writes_per_client > resume_after_writes,
            "follower pause needs more writes than its resume point"
        );
    }
    let binary =
        std::env::var_os("DAL_BENCH_BINARY").unwrap_or_else(|| env!("CARGO_BIN_EXE_dal").into());

    let mut root = match std::env::var_os("DAL_BENCH_DIR") {
        Some(path) => tempfile::tempdir_in(path).unwrap(),
        None => tempfile::tempdir().unwrap(),
    };
    let addrs = tcp_addrs(9);
    let controls = addrs[0..3].to_vec();
    let bulks = addrs[3..6].to_vec();
    let http_addrs: Vec<_> = addrs[6..9]
        .iter()
        .map(|addr| addr.trim_start_matches("tcp://").to_string())
        .collect();
    let nodes: Vec<_> = (0..3)
        .map(|i| {
            serde_json::json!({
                "node_id": i + 1,
                "control_addr": controls[i],
                "bulk_addr": bulks[i],
            })
        })
        .collect();
    let cluster_file = root.path().join("cluster.json");
    std::fs::write(
        &cluster_file,
        serde_json::to_vec(&serde_json::json!({
            "cluster_id": format!("{CID:#x}"),
            "p": partitions,
            "r": 3,
            "meta_voters": [1, 2, 3],
            "nodes": nodes,
        }))
        .unwrap(),
    )
    .unwrap();

    let mut processes = Processes(Vec::new());
    for i in 0..3 {
        let config_file = root.path().join(format!("node-{}.json", i + 1));
        std::fs::write(&config_file, serde_json::to_vec(&serde_json::json!({
            "cluster_id": format!("{CID:#x}"),
            "node_id": i + 1,
            "control_addr": controls[i],
            "bulk_addr": bulks[i],
            "http_addr": http_addrs[i],
            "seeds": controls.iter().enumerate().filter(|(j, _)| *j != i).map(|(_, addr)| addr).collect::<Vec<_>>(),
            "data_dir": root.path().join(format!("node-{}-data", i + 1)),
        })).unwrap()).unwrap();
        let log = File::create(root.path().join(format!("node-{}.log", i + 1))).unwrap();
        let child = Command::new(&binary)
            .arg("run")
            .arg("--config")
            .arg(&config_file)
            .arg("--cluster")
            .arg(&cluster_file)
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        processes.0.push(child);
    }

    let transport = ZmqTransport::new(
        zmq::Context::new(),
        Duration::from_secs(env_usize("DAL_BENCH_TIMEOUT_SECS", 5) as u64),
        Lane::Control,
    );
    let ready = Client::new(CID, 1, controls.clone(), transport.clone());
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            for (index, child) in processes.0.iter_mut().enumerate() {
                if let Some(exit) = child.try_wait().expect("cannot query node status") {
                    root.disable_cleanup(true);
                    panic!(
                        "node {} exited during bootstrap ({exit}); see {}",
                        index + 1,
                        root.path()
                            .join(format!("node-{}.log", index + 1))
                            .display()
                    );
                }
            }
            if matches!(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    ready.put(b"benchmark-ready", b"ok", None)
                )
                .await,
                Ok(Ok(WriteReply::Applied { .. }))
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        root.disable_cleanup(true);
        panic!(
            "three-process cluster did not become ready; logs at {}",
            root.path().display()
        );
    });

    if searches_per_client > 0 {
        ready
            .create_search_index("benchmark", search_definition())
            .await
            .expect("search index creation failed");
        #[derive(Serialize)]
        struct SearchDocument<'a> {
            title: &'a str,
        }
        let document = encode_search_value(
            "article",
            &flexbuffers::to_vec(SearchDocument { title: "benchmark" }).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            ready
                .put(b"benchmark-search-document", &document, None)
                .await,
            Ok(WriteReply::Applied { .. })
        ));
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                if matches!(ready.search_active(&search_request()).await, Ok(reply) if reply.total_hits > 0) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("search index did not become searchable");
    }

    let completed_writes = Arc::new(AtomicUsize::new(0));
    let paused_follower = if pause_follower {
        #[cfg(target_os = "linux")]
        {
            let leader = status(&http_addrs[0])
                .await
                .and_then(|state| state["partitions"][0]["leader"].as_u64())
                .expect("no data leader before follower pause");
            let index = (0..3).find(|index| *index as u64 + 1 != leader).unwrap();
            let pid = processes.0[index].id();
            signal(pid, "STOP");
            Some((pid, http_addrs[index].clone()))
        }
        #[cfg(not(target_os = "linux"))]
        panic!("follower pause requires Linux");
    } else {
        None
    };

    let (stop_samples, samples_done) = tokio::sync::oneshot::channel();
    let samples = tokio::spawn(sample_nodes(
        processes.0.iter().map(Child::id).collect(),
        http_addrs.clone(),
        samples_done,
    ));
    let started = Instant::now();
    #[cfg(target_os = "linux")]
    let io_pressure_before = io_pressure_full_micros();
    let follower_task = paused_follower.map(|(pid, follower_addr)| {
        let progress = completed_writes.clone();
        tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(120), async {
                while progress.load(Ordering::Relaxed) < resume_after_writes {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("writes did not reach the follower-resume point");
            let began = Instant::now();
            #[cfg(target_os = "linux")]
            signal(pid, "CONT");
            tokio::time::timeout(Duration::from_secs(120), async {
                loop {
                    if let Some(state) = status(&follower_addr).await {
                        let partition = &state["partitions"][0];
                        let caught_up = if require_follower_snapshot {
                            partition["raft_snapshot_index"].as_u64().unwrap_or(0) > 0
                        } else {
                            partition["applied"].as_u64().unwrap_or(0) >= resume_after_writes as u64
                        };
                        if caught_up {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("lagging follower did not catch up");
            began.elapsed()
        })
    });
    let mut search_tasks = Vec::new();
    if searches_per_client > 0 {
        for search_no in 0..clients {
            let client = Client::new(
                CID,
                search_no as u128 + 10_000,
                controls.clone(),
                transport.clone(),
            );
            search_tasks.push(tokio::spawn(async move {
                let mut durations = Vec::new();
                let mut errors = 0;
                for _ in 0..searches_per_client {
                    let begin = Instant::now();
                    match client.search_active(&search_request()).await {
                        Ok(reply) if reply.total_hits > 0 => durations.push(begin.elapsed()),
                        outcome => {
                            eprintln!("search failed client={search_no} outcome={outcome:?}");
                            errors += 1;
                            break;
                        }
                    }
                }
                (durations, errors)
            }));
        }
    }
    let rebuild_task = if rebuild {
        let client = Client::new(CID, 20_000, controls.clone(), transport.clone());
        let previous_generation = ready
            .search_index("benchmark")
            .await
            .expect("search catalog lookup failed")
            .and_then(|record| record.active)
            .expect("search index is not active")
            .id;
        Some(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let began = Instant::now();
            client
                .rebuild_search_index("benchmark", search_definition())
                .await?;
            tokio::time::timeout(Duration::from_secs(90), async {
                loop {
                    if let Ok(Some(record)) = client.search_index("benchmark").await {
                        if record
                            .active
                            .as_ref()
                            .is_some_and(|active| active.id != previous_generation)
                        {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            })
            .await
            .expect("search rebuild did not become active");
            Ok::<Duration, dal::error::Error>(began.elapsed())
        }))
    } else {
        None
    };
    let value = Arc::new(benchmark_value(value_bytes, indexable_values));
    let actual_value_bytes = value.len();
    let mut tasks = Vec::new();
    for client_no in 0..clients {
        let client = Client::new(
            CID,
            client_no as u128 + 2,
            controls.clone(),
            transport.clone(),
        );
        let progress = completed_writes.clone();
        let value = value.clone();
        tasks.push(tokio::spawn(async move {
            let mut writes = Vec::new();
            let mut reads = Vec::new();
            let mut errors = 0usize;
            let mut retries = 0usize;
            for i in 0..writes_per_client {
                let key = format!("bench-{client_no}-{i}");
                let begin = Instant::now();
                let mut applied = false;
                for attempt in 0..3 {
                    if attempt > 0 {
                        retries += 1;
                    }
                    match client.put(key.as_bytes(), value.as_slice(), None).await {
                        Ok(WriteReply::Applied { .. }) => {
                            applied = true;
                            break;
                        }
                        outcome => eprintln!(
                            "write failed client={client_no} key={key} attempt={} outcome={outcome:?}",
                            attempt + 1
                        ),
                    }
                }
                if !applied {
                    errors += 1;
                    break;
                }
                writes.push(begin.elapsed());
                progress.fetch_add(1, Ordering::Relaxed);
                let begin = Instant::now();
                match client.get(key.as_bytes()).await {
                    Ok(Some((_, read))) if read.as_slice() == value.as_slice() => reads.push(begin.elapsed()),
                    Ok(Some((_, read))) => {
                        eprintln!(
                            "read mismatch client={client_no} key={key} bytes={} expected={value_bytes}",
                            read.len()
                        );
                        errors += 1;
                        break;
                    }
                    outcome => {
                        eprintln!("read failed client={client_no} key={key} outcome={outcome:?}");
                        errors += 1;
                        break;
                    }
                }
            }
            (writes, reads, errors, retries)
        }));
    }
    let mut writes = Vec::new();
    let mut reads = Vec::new();
    let mut errors = 0;
    let mut retries = 0;
    for task in tasks {
        let (mut w, mut r, failed, retried) = task.await.unwrap();
        writes.append(&mut w);
        reads.append(&mut r);
        errors += failed;
        retries += retried;
    }
    let mut searches = Vec::new();
    for task in search_tasks {
        let (mut durations, failed) = task.await.unwrap();
        searches.append(&mut durations);
        errors += failed;
    }
    let rebuild_duration = match rebuild_task {
        Some(task) => Some(task.await.unwrap().expect("search rebuild request failed")),
        None => None,
    };
    let follower_catchup = match follower_task {
        Some(task) => Some(task.await.expect("follower resume task failed")),
        None => None,
    };
    let elapsed = started.elapsed();
    #[cfg(target_os = "linux")]
    let io_pressure_after = io_pressure_full_micros();
    let snapshot_wait = if env_usize("DAL_BENCH_WAIT_SNAPSHOT", 0) != 0 {
        let began = Instant::now();
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                for addr in &http_addrs {
                    if let Some(state) = status(addr).await {
                        if state["partitions"].as_array().is_some_and(|parts| {
                            parts
                                .iter()
                                .any(|part| part["raft_snapshot_index"].as_u64().unwrap_or(0) > 0)
                        }) {
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("no Raft snapshot appeared after the workload");
        Some(began.elapsed())
    } else {
        None
    };
    let search_catchup = if wait_search {
        let began = Instant::now();
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                let mut all_caught_up = true;
                for addr in &http_addrs {
                    let Some(state) = status(addr).await else {
                        all_caught_up = false;
                        break;
                    };
                    let ready = state["partitions"].as_array().is_some_and(|partitions| {
                        !partitions.is_empty()
                            && partitions.iter().all(|partition| {
                                partition["search"].as_array().is_some_and(|indexes| {
                                    indexes.iter().any(|index| {
                                        index["name"] == "benchmark"
                                            && index["state"] == "Active"
                                            && index["lag_entries"].as_u64() == Some(0)
                                    })
                                })
                            })
                    });
                    if !ready {
                        all_caught_up = false;
                        break;
                    }
                }
                if all_caught_up {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("search projections did not catch up");
        let reply = ready.search_active(&search_request()).await.unwrap();
        let expected_hits = if indexable_values {
            writes.len() + 1
        } else {
            1
        };
        assert_eq!(reply.total_hits, expected_hits as u64);
        Some((began.elapsed(), reply.total_hits))
    } else {
        None
    };
    // Ensure status counters updated by the final background pass are sampled.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let _ = stop_samples.send(());
    let samples = samples.await.unwrap();
    assert!(!writes.is_empty(), "no successful writes");
    assert!(!reads.is_empty(), "no successful reads");
    println!(
        "trial={} binary={} topology=3-process TCP filesystem_root={} partitions={} clients={} value_bytes={} requested_value_bytes={} indexable_values={} writes={} reads={} searches={} errors={} retries={} elapsed_s={:.2} ops_per_s={:.1}",
        std::env::var("DAL_BENCH_TRIAL_ID").unwrap_or_else(|_| "unset".into()),
        std::path::Path::new(&binary).display(),
        root.path().display(),
        partitions,
        clients,
        actual_value_bytes,
        value_bytes,
        indexable_values,
        writes.len(),
        reads.len(),
        searches.len(),
        errors,
        retries,
        elapsed.as_secs_f64(),
        (writes.len() + reads.len() + searches.len()) as f64 / elapsed.as_secs_f64()
    );
    println!(
        "write_p50_ms={:.2} write_p95_ms={:.2} write_p99_ms={:.2}",
        percentile(&mut writes, 50),
        percentile(&mut writes, 95),
        percentile(&mut writes, 99)
    );
    println!(
        "read_p50_ms={:.2} read_p95_ms={:.2} read_p99_ms={:.2}",
        percentile(&mut reads, 50),
        percentile(&mut reads, 95),
        percentile(&mut reads, 99)
    );
    println!(
        "write_max_ms={:.2} read_max_ms={:.2}",
        writes.last().unwrap().as_secs_f64() * 1000.0,
        reads.last().unwrap().as_secs_f64() * 1000.0,
    );
    #[cfg(target_os = "linux")]
    println!(
        "host_io_full_ms={:?}",
        io_pressure_after
            .zip(io_pressure_before)
            .map(|(after, before)| after.saturating_sub(before) as f64 / 1000.0)
    );
    if !searches.is_empty() {
        println!(
            "search_p50_ms={:.2} search_p95_ms={:.2} search_p99_ms={:.2}",
            percentile(&mut searches, 50),
            percentile(&mut searches, 95),
            percentile(&mut searches, 99),
        );
    }
    if let Some(duration) = rebuild_duration {
        println!(
            "rebuild_activation_ms={:.2}",
            duration.as_secs_f64() * 1000.0
        );
    }
    if let Some(duration) = snapshot_wait {
        println!(
            "snapshot_wait_after_foreground_ms={:.2}",
            duration.as_secs_f64() * 1000.0
        );
    }
    if let Some((duration, hits)) = search_catchup {
        println!(
            "search_catchup_after_foreground_ms={:.2} final_search_total_hits={hits}",
            duration.as_secs_f64() * 1000.0
        );
    }
    if let Some(duration) = follower_catchup {
        println!(
            "follower_{}_catchup_ms={:.2}",
            if require_follower_snapshot {
                "snapshot"
            } else {
                "log"
            },
            duration.as_secs_f64() * 1000.0
        );
    }
    for (child, sample) in processes.0.iter().zip(samples) {
        println!(
            "node_pid={} peak_rss_kib={} cpu_ticks={:?} peak_outbox_entries={} peak_outbox_bytes={} peak_pending_bytes={} peak_wal_pending_requests={} peak_wal_pending_bytes={} peak_rocks_memtables_bytes={} peak_rocks_unflushed_memtables_bytes={} peak_rocks_table_readers_bytes={} peak_rocks_cache_bytes={} peak_tantivy_writer_budget_bytes={} snapshot_index={} snapshot_builds={} snapshot_build_max_ms={:.2} snapshot_installs={} snapshot_install_max_ms={:.2} search_user_records_scanned={} search_outbox_entries_scanned={} search_source_row_peak_bytes={} snapshot_record_peak_bytes={} wal_syncs={:?} wal_bytes={:?} stall_micros={:?} status_samples={}",
            child.id(),
            sample.peak_rss_kib,
            sample
                .last_cpu_ticks
                .zip(sample.first_cpu_ticks)
                .map(|(last, first)| last.saturating_sub(first)),
            sample.peak_outbox_entries,
            sample.peak_outbox_bytes,
            sample.peak_pending_bytes,
            sample.peak_wal_pending_requests,
            sample.peak_wal_pending_bytes,
            sample.peak_rocks_memtables_bytes,
            sample.peak_rocks_unflushed_memtables_bytes,
            sample.peak_rocks_table_readers_bytes,
            sample.peak_rocks_cache_bytes,
            sample.peak_tantivy_writer_budget_bytes,
            sample.max_snapshot_index,
            sample.snapshot_builds,
            sample.snapshot_build_max_micros as f64 / 1000.0,
            sample.snapshot_installs,
            sample.snapshot_install_max_micros as f64 / 1000.0,
            sample.search_user_records_scanned,
            sample.search_outbox_entries_scanned,
            sample.search_source_row_peak_bytes,
            sample.snapshot_record_peak_bytes,
            sample
                .last_wal_syncs
                .zip(sample.first_wal_syncs)
                .map(|(last, first)| last.saturating_sub(first)),
            sample
                .last_wal_bytes
                .zip(sample.first_wal_bytes)
                .map(|(last, first)| last.saturating_sub(first)),
            sample
                .last_stall_micros
                .zip(sample.first_stall_micros)
                .map(|(last, first)| last.saturating_sub(first)),
            sample.status_samples,
        );
    }
    if errors != 0 {
        for addr in &http_addrs {
            if let Some(state) = status(addr).await {
                eprintln!(
                    "failure_status node={} meta_leader={} leader={} applied={} snapshot={} serving={} storage_failed={} admission_rejections={} reply_eagain={} reply_failures={} max_handlers={}",
                    state["node_id"],
                    state["meta"]["leader"],
                    state["partitions"][0]["leader"],
                    state["partitions"][0]["applied"],
                    state["partitions"][0]["raft_snapshot_index"],
                    state["partitions"][0]["serving"],
                    state["storage_failed"],
                    state["router_admission_rejections"],
                    state["router_reply_send_eagain"],
                    state["router_reply_send_failures"],
                    state["router_max_active_handlers"],
                );
            }
        }
    }
    if errors != 0 || env_usize("DAL_BENCH_KEEP_DIR", 0) != 0 {
        eprintln!("benchmark node logs retained at {}", root.keep().display());
    }
    assert_eq!(errors, 0, "benchmark had failed operations");
}
