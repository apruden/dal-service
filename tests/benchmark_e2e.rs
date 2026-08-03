//! End-to-end benchmark over a real three-node cluster.
//!
//! Forms a production `runtime::Node` cluster of three nodes over ZeroMQ
//! `inproc://` (shared context, no TCP), drives genesis, then drives client
//! traffic through the gateway → data-group Raft → RocksDB path and reports
//! latency percentiles and throughput. This is a *performance* smoke, not a
//! correctness test: it asserts only that the cluster serves, and prints a
//! report (run with `-- --nocapture` to see it).
//!
//! Counts are overridable via env vars so the same harness scales from a quick
//! CI pass to a heavier local run:
//!   DAL_BENCH_PARTITIONS  (default 16)  data partitions, each on all 3 nodes
//!   DAL_BENCH_WRITES      (default 1000) sequential single-client writes
//!   DAL_BENCH_READS       (default 1000) sequential single-client reads
//!   DAL_BENCH_CLIENTS     (default 16)   concurrent clients
//!   DAL_BENCH_OPS         (default 6000) total ops in each concurrent phase
//!   DAL_BENCH_DIR          (default OS temp directory) parent for RocksDB dirs;
//!                          set this explicitly to a durable filesystem
//!   DAL_BENCH_TRANSPORT_PER_CLIENT=1 creates one ZMQ transport per concurrent
//!                          client. The default shares one production-shaped
//!                          transport across all benchmark clients.
//!   DAL_BENCH_TRIAL_ID     labels output from a reproducible benchmark trial.
//!
//! To A/B the Raft-apply fsync coalescing, run this benchmark twice:
//!   DAL_APPLY_COALESCE=0 ...  (baseline: one fsync per committed entry)
//!   DAL_APPLY_COALESCE=1 ...  (coalesced: one fsync per apply batch, default)
//! The write-throughput phases (3 and 4) are where the difference shows, since
//! that is where openraft applies committed entries in batches.
//!
//! To A/B database-wide Raft WAL group commit while keeping apply behavior
//! fixed, run twice with `DAL_APPLY_COALESCE=1`:
//!   DAL_LOG_GROUP_COMMIT=0 ... (baseline: synchronous fsync per log append)
//!   DAL_LOG_GROUP_COMMIT=1 ... (database-wide group commit, default)
//!
//! To A/B the unified log/state durability worker:
//!   DAL_UNIFIED_DURABILITY=0 ... (state apply uses sync=true directly)
//!   DAL_UNIFIED_DURABILITY=1 ... (state joins database-wide flushes, default)
//! Adaptive collection is default; `DAL_ADAPTIVE_DURABILITY=0` restores the
//! fixed collection window.
//!
//! Async data-group materialization is the default. To measure the synchronous
//! emergency mode, keep the unified worker enabled and run:
//!   default ...                         (apply returns after state visibility)
//!   DAL_SYNC_MATERIALIZED_STATE=1 ...  (apply waits for state WAL durability)

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dal::api::client::Client;
use dal::api::ops::WriteReply;
use dal::config::{NodeConfig, Timeouts};
use dal::meta::bootstrap::{BootstrapDescriptor, DirEntry};
use dal::perf::RocksCounters;
use dal::runtime::node::Node;
use dal::transport::dealer::ZmqTransport;
use dal::transport::router::{counters as router_counters, settle};
use dal::types::{ClusterConfig, ClusterId, HashSpec, NodeId, PROTOCOL_VERSION};

const CID: ClusterId = 0x0000_0000_0000_0000_0000_0000_0000_0DA1;
const VOTERS: [NodeId; 3] = [1, 2, 3];
const VALUE_BYTES: usize = 128;
const PREFIX: &str = "bench";

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn benchmark_dirs(count: usize) -> (Vec<tempfile::TempDir>, PathBuf, bool) {
    match std::env::var_os("DAL_BENCH_DIR") {
        Some(root) => {
            let root = PathBuf::from(root);
            std::fs::create_dir_all(&root).unwrap();
            let root = root.canonicalize().unwrap();
            let dirs = (0..count)
                .map(|_| {
                    tempfile::Builder::new()
                        .prefix("dal-bench-")
                        .tempdir_in(&root)
                        .unwrap()
                })
                .collect();
            (dirs, root, true)
        }
        None => {
            let dirs: Vec<tempfile::TempDir> =
                (0..count).map(|_| tempfile::tempdir().unwrap()).collect();
            let root = dirs[0].path().parent().unwrap().to_path_buf();
            (dirs, root, false)
        }
    }
}

fn rocks_counters(nodes: &[Arc<Node>]) -> RocksCounters {
    nodes.iter().fold(RocksCounters::default(), |sum, node| {
        sum + node.rocks_counters()
    })
}

fn profile_phase(label: &str, nodes: &[Arc<Node>], rocks_before: &mut RocksCounters) {
    let Some(report) = dal::perf::write_path_report() else {
        return;
    };
    println!("\n--- profile phase: {label} ---");
    print!("{report}");
    let current = rocks_counters(nodes);
    let rocks = current.saturating_sub(*rocks_before);
    println!(
        "  RocksDB: WAL syncs={}, WAL bytes={:.2} MiB, write-stall={:.2}ms",
        rocks.wal_syncs,
        rocks.wal_bytes as f64 / (1024.0 * 1024.0),
        rocks.stall_micros as f64 / 1_000.0,
    );
    *rocks_before = current;
    dal::perf::reset_write_path();
}

fn descriptor(p: u16) -> BootstrapDescriptor {
    let directory = VOTERS
        .iter()
        .map(|&id| DirEntry {
            node_id: id,
            control_addr: format!("inproc://{PREFIX}-ctrl-{id}"),
            bulk_addr: format!("inproc://{PREFIX}-bulk-{id}"),
        })
        .collect();
    BootstrapDescriptor {
        cluster_id: CID,
        config: ClusterConfig {
            cluster_id: CID,
            protocol_version: PROTOCOL_VERSION,
            p,
            r: 3,
            hash_spec: HashSpec::CANONICAL,
        },
        meta_voters: VOTERS.to_vec(),
        directory,
        // Every partition is replicated across all three nodes.
        data_placements: (0..p).map(|part| (part, VOTERS.to_vec())).collect(),
    }
}

fn node_config(id: NodeId, dir: PathBuf) -> NodeConfig {
    NodeConfig {
        cluster_id: CID,
        node_id: id,
        control_addr: format!("inproc://{PREFIX}-ctrl-{id}"),
        bulk_addr: format!("inproc://{PREFIX}-bulk-{id}"),
        http_addr: None,
        seeds: VOTERS
            .iter()
            .filter(|&&v| v != id)
            .map(|&v| format!("inproc://{PREFIX}-ctrl-{v}"))
            .collect(),
        data_dir: dir,
        timeouts: Timeouts::default(),
    }
}

fn client(transport: ZmqTransport, client_id: u128) -> Client<ZmqTransport> {
    Client::new(
        CID,
        client_id,
        VOTERS
            .iter()
            .map(|&v| format!("inproc://{PREFIX}-ctrl-{v}"))
            .collect(),
        transport,
    )
}

fn env_enabled(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref(),
        Some("1") | Some("true") | Some("on")
    )
}

/// Latency summary over a set of per-op durations, plus wall-clock throughput.
struct Stats {
    label: String,
    ops: usize,
    wall: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
    mean: Duration,
}

impl Stats {
    fn from(label: &str, mut samples: Vec<Duration>, wall: Duration) -> Stats {
        samples.sort_unstable();
        let n = samples.len().max(1);
        let pct = |q: f64| samples[((n as f64 * q) as usize).min(n - 1)];
        let sum: Duration = samples.iter().sum();
        Stats {
            label: label.to_string(),
            ops: samples.len(),
            wall,
            p50: pct(0.50),
            p95: pct(0.95),
            p99: pct(0.99),
            max: *samples.last().unwrap_or(&Duration::ZERO),
            mean: sum / n as u32,
        }
    }

    fn throughput(&self) -> f64 {
        self.ops as f64 / self.wall.as_secs_f64()
    }

    fn print(&self) {
        let ms = |d: Duration| d.as_secs_f64() * 1e3;
        println!(
            "  {:<26} ops={:>6}  {:>9.0} ops/s | p50 {:>6.2}ms  p95 {:>6.2}ms  p99 {:>6.2}ms  max {:>7.2}ms  mean {:>6.2}ms",
            self.label,
            self.ops,
            self.throughput(),
            ms(self.p50),
            ms(self.p95),
            ms(self.p99),
            ms(self.max),
            ms(self.mean),
        );
    }
}

fn value(i: usize) -> Vec<u8> {
    let mut v = vec![b'x'; VALUE_BYTES];
    let tag = format!("v{i}");
    v[..tag.len().min(VALUE_BYTES)].copy_from_slice(&tag.as_bytes()[..tag.len().min(VALUE_BYTES)]);
    v
}

async fn start_cluster(
    ctx: &zmq::Context,
    desc: &BootstrapDescriptor,
    dirs: &[tempfile::TempDir],
) -> Vec<Arc<Node>> {
    let mut nodes = Vec::new();
    for (i, &id) in VOTERS.iter().enumerate() {
        let cfg = node_config(id, dirs[i].path().to_path_buf());
        nodes.push(Arc::new(
            Node::start(ctx.clone(), cfg, desc.clone()).await.unwrap(),
        ));
    }
    settle();
    let handles: Vec<_> = nodes
        .iter()
        .map(|n| {
            let n = n.clone();
            tokio::spawn(async move { n.bootstrap().await })
        })
        .collect();
    for h in handles {
        h.await.unwrap().unwrap();
    }
    nodes
}

/// Force a leader election on every partition and confirm the cluster serves,
/// so the measured phases don't pay election latency. Writes one key per
/// partition, retrying until each applies.
async fn warm_up(c: &Client<ZmqTransport>, p: u16) {
    let spec = HashSpec::CANONICAL;
    for part in 0..p {
        // Find a key that hashes to this partition so every group is touched.
        let mut n = 0u64;
        let key = loop {
            let k = format!("{PREFIX}-warm-{part}-{n}").into_bytes();
            if spec.partition_of(&k, p) == part {
                break k;
            }
            n += 1;
        };
        let mut ok = false;
        for _ in 0..50 {
            if let Ok(WriteReply::Applied { .. }) = c.put(&key, &value(0), None).await {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(ok, "warm-up write to partition {part} never applied");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "benchmark: run `cargo test --release --test benchmark_e2e end_to_end_benchmark_three_nodes -- --ignored --nocapture`"]
async fn end_to_end_benchmark_three_nodes() {
    let partitions = env_usize("DAL_BENCH_PARTITIONS", 16) as u16;
    let writes = env_usize("DAL_BENCH_WRITES", 1000);
    let reads = env_usize("DAL_BENCH_READS", 1000);
    let clients = env_usize("DAL_BENCH_CLIENTS", 16).max(1);
    let concurrent_ops = env_usize("DAL_BENCH_OPS", 6000);

    let ctx = zmq::Context::new();
    let shared_transport = ZmqTransport::new(ctx.clone(), Duration::from_secs(5));
    let transport_per_client = env_enabled("DAL_BENCH_TRANSPORT_PER_CLIENT");
    let desc = descriptor(partitions);
    let (dirs, data_root, explicit_data_root) = benchmark_dirs(3);

    let boot = Instant::now();
    let nodes = start_cluster(&ctx, &desc, &dirs).await;
    let boot_elapsed = boot.elapsed();

    let writer = client(shared_transport.clone(), 1);
    let warm = Instant::now();
    warm_up(&writer, partitions).await;
    let warm_elapsed = warm.elapsed();
    let mut rocks_before = rocks_counters(&nodes);
    dal::perf::reset_write_path();

    let coalesce = !matches!(
        std::env::var("DAL_APPLY_COALESCE").ok().as_deref(),
        Some("0") | Some("false") | Some("off")
    );
    let log_group_commit = !matches!(
        std::env::var("DAL_LOG_GROUP_COMMIT").ok().as_deref(),
        Some("0") | Some("false") | Some("off")
    );
    let unified_durability = !matches!(
        std::env::var("DAL_UNIFIED_DURABILITY").ok().as_deref(),
        Some("0") | Some("false") | Some("off")
    );
    let adaptive_durability = !matches!(
        std::env::var("DAL_ADAPTIVE_DURABILITY").ok().as_deref(),
        Some("0") | Some("false") | Some("off")
    );
    let sync_materialized_state = env_enabled("DAL_SYNC_MATERIALIZED_STATE")
        || matches!(
            std::env::var("DAL_ASYNC_MATERIALIZED_STATE")
                .ok()
                .as_deref(),
            Some("0") | Some("false") | Some("off")
        );
    let async_materialized_state = unified_durability && !sync_materialized_state;

    println!("\n=== dal end-to-end benchmark (3 nodes over ZeroMQ inproc) ===");
    println!(
        "  transport mode: {} | trial: {}",
        if transport_per_client {
            "one transport per client"
        } else {
            "one shared transport"
        },
        std::env::var("DAL_BENCH_TRIAL_ID").unwrap_or_else(|_| "unset".into()),
    );
    println!(
        "  cluster: 3 nodes, R=3, {partitions} partitions, value={VALUE_BYTES}B | boot {:.2}s, warm-up {:.2}s",
        boot_elapsed.as_secs_f64(),
        warm_elapsed.as_secs_f64(),
    );
    println!("  RocksDB directory parent: {}", data_root.display());
    if !explicit_data_root {
        println!("  WARNING: DAL_BENCH_DIR is unset; verify that the OS temp directory is durable");
    }
    println!(
        "  apply mode: {} (DAL_APPLY_COALESCE)",
        if coalesce {
            "coalesced (1 fsync / batch)"
        } else {
            "per-entry (1 fsync / entry)"
        },
    );
    println!(
        "  log durability: {} (DAL_LOG_GROUP_COMMIT)",
        if log_group_commit {
            "database-wide group commit"
        } else {
            "synchronous fsync per append"
        },
    );
    println!("  committed marker: folded into atomic state apply");
    println!(
        "  state durability: {} (DAL_UNIFIED_DURABILITY)",
        if unified_durability {
            "shared database-wide worker"
        } else {
            "synchronous state write"
        },
    );
    println!(
        "  durability collection: {} (DAL_ADAPTIVE_DURABILITY)",
        if adaptive_durability {
            "adaptive"
        } else {
            "fixed window"
        },
    );
    println!(
        "  materialized-state reply: {} (default async; DAL_SYNC_MATERIALIZED_STATE=1 overrides)",
        if async_materialized_state {
            "after visibility; WAL durability is asynchronous"
        } else {
            "after WAL durability"
        },
    );
    println!("  ------------------------------------------------------------------------------");

    // Phase 1 — sequential write latency from a single client.
    let mut wl = Vec::with_capacity(writes);
    let t0 = Instant::now();
    for i in 0..writes {
        let key = format!("{PREFIX}-seq-{i}").into_bytes();
        let start = Instant::now();
        let reply = writer.put(&key, &value(i), None).await.unwrap();
        assert!(matches!(reply, WriteReply::Applied { .. }));
        wl.push(start.elapsed());
    }
    Stats::from("seq write (1 client)", wl, t0.elapsed()).print();
    profile_phase("sequential write", &nodes, &mut rocks_before);

    // Phase 2 — sequential read latency for the keys just written.
    let mut rl = Vec::with_capacity(reads);
    let t0 = Instant::now();
    for i in 0..reads {
        let key = format!("{PREFIX}-seq-{}", i % writes.max(1)).into_bytes();
        let start = Instant::now();
        let got = writer.get(&key).await.unwrap();
        assert!(got.is_some(), "read of a written key returned empty");
        rl.push(start.elapsed());
    }
    Stats::from("seq read  (1 client)", rl, t0.elapsed()).print();
    profile_phase("sequential linearizable read", &nodes, &mut rocks_before);

    // Phase 2b — sequential stale (eventually-consistent) read latency for the
    // same keys. Skips ReadIndex, so no quorum round trip; compare against the
    // linearizable seq read above.
    let mut sl = Vec::with_capacity(reads);
    let t0 = Instant::now();
    for i in 0..reads {
        let key = format!("{PREFIX}-seq-{}", i % writes.max(1)).into_bytes();
        let start = Instant::now();
        let got = writer.stale_get(&key, None).await.unwrap();
        assert!(got.is_some(), "stale read of a written key returned empty");
        sl.push(start.elapsed());
    }
    Stats::from("seq stale read (1 client)", sl, t0.elapsed()).print();
    profile_phase("sequential stale read", &nodes, &mut rocks_before);

    // Phase 3 — concurrent write throughput across many clients.
    let per_client = concurrent_ops.div_ceil(clients);
    let cw = run_concurrent(
        &ctx,
        &shared_transport,
        transport_per_client,
        clients,
        per_client,
        Workload::Write,
        1_000,
    )
    .await;
    Stats::from(&format!("conc write ({clients} clients)"), cw.0, cw.1).print();
    profile_phase("concurrent write", &nodes, &mut rocks_before);

    // Phase 4 — concurrent mixed 50/50 read/write throughput. Distinct client_id
    // base so fresh streams don't collide with phase 3's idempotency records.
    let cm = run_concurrent(
        &ctx,
        &shared_transport,
        transport_per_client,
        clients,
        per_client,
        Workload::Mixed,
        1_000_000,
    )
    .await;
    Stats::from(&format!("conc mixed ({clients} clients)"), cm.0, cm.1).print();
    profile_phase("concurrent mixed", &nodes, &mut rocks_before);

    // Phase 5 — concurrent linearizable read throughput over the phase-1 seq
    // keys. Every read funnels to its partition leader via ReadIndex.
    let cr = run_concurrent_reads(
        &ctx,
        &shared_transport,
        transport_per_client,
        clients,
        per_client,
        writes,
        false,
        2_000_000,
    )
    .await;
    Stats::from(&format!("conc read  ({clients} clients)"), cr.0, cr.1).print();
    profile_phase("concurrent linearizable read", &nodes, &mut rocks_before);

    // Phase 6 — concurrent stale read throughput over the same keys. Reads
    // spread across all three replicas and skip the quorum round trip; this is
    // the phase that shows the eventually-consistent path's headroom.
    let cs = run_concurrent_reads(
        &ctx,
        &shared_transport,
        transport_per_client,
        clients,
        per_client,
        writes,
        true,
        3_000_000,
    )
    .await;
    Stats::from(&format!("conc stale read ({clients} clients)"), cs.0, cs.1).print();
    profile_phase("concurrent stale read", &nodes, &mut rocks_before);
    let router = router_counters();
    println!(
        "  ROUTER: admission-rejections={}, max-active={}, reply-sends={}, reply-EAGAIN={}, reply-failures={}",
        router.admission_rejections,
        router.max_active_handlers,
        router.reply_sends,
        router.reply_send_eagain,
        router.reply_send_failures,
    );
    println!("  ==============================================================================\n");

    for n in nodes {
        Arc::try_unwrap(n).ok().unwrap().shutdown().await.unwrap();
    }
}

#[derive(Clone, Copy)]
enum Workload {
    Write,
    Mixed,
}

/// The client already retries redirects/timeouts internally; this absorbs the
/// rarer transport-level backpressure error that inproc raises under a heavy
/// concurrent flood (all seeds momentarily busy) so one blip doesn't abort the
/// whole run. A `put` is idempotent under retry (fixed sequence).
async fn retry_put(cl: &Client<ZmqTransport>, key: &[u8], val: &[u8]) -> WriteReply {
    for attempt in 0..8 {
        match cl.put(key, val, None).await {
            Ok(r) => return r,
            Err(_) => tokio::time::sleep(Duration::from_millis(10 * (attempt + 1))).await,
        }
    }
    cl.put(key, val, None)
        .await
        .expect("put failed after retries")
}

async fn retry_get(cl: &Client<ZmqTransport>, key: &[u8]) {
    for attempt in 0..8 {
        if cl.get(key).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10 * (attempt + 1))).await;
    }
    cl.get(key).await.expect("get failed after retries");
}

/// Drive `clients` concurrent clients, each issuing `per_client` ops against
/// distinct keys, and return every op's latency plus the aggregate wall time.
async fn run_concurrent(
    ctx: &zmq::Context,
    shared_transport: &ZmqTransport,
    transport_per_client: bool,
    clients: usize,
    per_client: usize,
    workload: Workload,
    id_base: u128,
) -> (Vec<Duration>, Duration) {
    let mut clients_to_run = Vec::with_capacity(clients);
    for c in 0..clients {
        let transport = if transport_per_client {
            ZmqTransport::new(ctx.clone(), Duration::from_secs(5))
        } else {
            shared_transport.clone()
        };
        let cl = client(transport, id_base + c as u128);
        // Warm this client's routing cache outside the timed section so a cold
        // MetaQuery or first redirect does not distort steady-state results.
        let _ = cl.get(format!("{PREFIX}-warm-0-0").as_bytes()).await;
        clients_to_run.push((c, cl));
    }

    let t0 = Instant::now();
    let mut tasks = Vec::with_capacity(clients);
    for (c, cl) in clients_to_run {
        // Distinct client_id per task so the per-(client,partition) stream lock
        // does not serialize independent clients. The base keeps each phase's
        // ids disjoint so replayed sequences never alias a prior phase's records.
        tasks.push(tokio::spawn(async move {
            let mut lat = Vec::with_capacity(per_client);
            for i in 0..per_client {
                let key = format!("{PREFIX}-c{c}-k{i}").into_bytes();
                let start = Instant::now();
                match workload {
                    Workload::Write => {
                        let r = retry_put(&cl, &key, &value(i)).await;
                        assert!(matches!(r, WriteReply::Applied { .. }));
                    }
                    Workload::Mixed => {
                        if i % 2 == 0 {
                            let r = retry_put(&cl, &key, &value(i)).await;
                            assert!(matches!(r, WriteReply::Applied { .. }));
                        } else {
                            // Read a key this client already wrote.
                            let rk = format!("{PREFIX}-c{c}-k{}", i - 1).into_bytes();
                            retry_get(&cl, &rk).await;
                        }
                    }
                }
                lat.push(start.elapsed());
            }
            lat
        }));
    }
    let mut all = Vec::with_capacity(clients * per_client);
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    (all, t0.elapsed())
}

/// Drive `clients` concurrent clients issuing read-only traffic over the
/// phase-1 sequential keys (which are already present on every replica). With
/// `stale` set, each read uses the eventually-consistent path (`stale_get`,
/// spread across replicas); otherwise it uses the linearizable path.
#[allow(clippy::too_many_arguments)]
async fn run_concurrent_reads(
    ctx: &zmq::Context,
    shared_transport: &ZmqTransport,
    transport_per_client: bool,
    clients: usize,
    per_client: usize,
    writes: usize,
    stale: bool,
    id_base: u128,
) -> (Vec<Duration>, Duration) {
    let mut clients_to_run = Vec::with_capacity(clients);
    for c in 0..clients {
        let transport = if transport_per_client {
            ZmqTransport::new(ctx.clone(), Duration::from_secs(5))
        } else {
            shared_transport.clone()
        };
        let cl = client(transport, id_base + c as u128);
        let _ = cl.get(format!("{PREFIX}-warm-0-0").as_bytes()).await;
        clients_to_run.push((c, cl));
    }

    let t0 = Instant::now();
    let mut tasks = Vec::with_capacity(clients);
    let span = writes.max(1);
    for (c, cl) in clients_to_run {
        tasks.push(tokio::spawn(async move {
            let mut lat = Vec::with_capacity(per_client);
            for i in 0..per_client {
                // Stagger each client's start key so replicas see a spread of
                // keys rather than every client hammering the same one.
                let key = format!("{PREFIX}-seq-{}", (c * per_client + i) % span).into_bytes();
                let start = Instant::now();
                if stale {
                    cl.stale_get(&key, None).await.expect("stale read failed");
                } else {
                    cl.get(&key).await.expect("read failed");
                }
                lat.push(start.elapsed());
            }
            lat
        }));
    }
    let mut all = Vec::with_capacity(clients * per_client);
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    (all, t0.elapsed())
}
