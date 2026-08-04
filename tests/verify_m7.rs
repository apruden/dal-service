//! M7 system verification (DESIGN §12.7, §14): a seeded, concurrent workload
//! against a real 3-node data group under leader-crash fault injection, checked
//! by the executable oracles — per-key linearizability, exactly-once, and
//! no-acknowledged-write-lost. Every run is reproducible from its seed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dal::meta::bootstrap::ensure_bootstrap_group;
use dal::partition::network::{Faults, Registry};
use dal::partition::node::{PartitionNode, ReadOutcome, WriteOutcome};
use dal::partition::{ApplyResult, TypeConfig};
use dal::storage::Storage;
use dal::transport::raft_wire::RecoveryFenceReply;
use dal::types::{
    BootstrapGroup, Consistency, DataOp, DataRequest, GroupId, IfVersion, KeyPresence,
    MutationResult,
};
use dal::verify::linearizability::{Invocation, Op, Outcome, is_linearizable};
use dal::verify::oracles::{Applied, exactly_once, no_lost_write};
use dal::verify::rng::Rng;

use tempfile::TempDir;

const CID: u128 = 0xDA1;
const VOTERS: [u64; 3] = [1, 2, 3];
const KEYS: usize = 6;
const OPS_PER_TASK: usize = 6;

#[derive(Clone)]
struct Recorded {
    key: Vec<u8>,
    call: u64,
    ret: u64,
    inv: Invocation,
    out: Outcome,
}

struct Shared {
    nodes: Vec<Arc<PartitionNode>>,
    alive: Vec<AtomicBool>,
    clock: AtomicU64,
    history: Mutex<Vec<Recorded>>,
    acked: Mutex<Vec<(Vec<u8>, u64)>>,
    applied: Mutex<Vec<Applied>>,
}

impl Shared {
    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::SeqCst)
    }

    fn leader(&self) -> Option<usize> {
        let who = self
            .nodes
            .iter()
            .enumerate()
            .find(|(i, n)| self.alive[*i].load(Ordering::SeqCst) && n.current_leader().is_some())
            .and_then(|(_, n)| n.current_leader())?;
        self.nodes.iter().position(|n| n.node_id() == who)
    }
}

fn key_bytes(i: usize) -> Vec<u8> {
    format!("k{i}").into_bytes()
}

fn digest(op: &DataOp) -> u128 {
    xxhash_rust::xxh3::xxh3_128(&dal::codec::encode(op))
}

fn present(p: KeyPresence) -> Option<u64> {
    match p {
        KeyPresence::Present { version } => Some(version),
        KeyPresence::Absent => None,
    }
}

async fn await_leader(s: &Shared) {
    for _ in 0..200 {
        if s.leader().is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no leader");
}

/// Issue a write, retrying across leader changes; returns the decided result.
async fn do_write(s: &Shared, req: &DataRequest) -> Option<ApplyResult> {
    for _ in 0..400 {
        if let Some(li) = s.leader() {
            match s.nodes[li].write(req.clone()).await {
                Ok(WriteOutcome::Applied(r)) => return Some(r),
                Ok(WriteOutcome::NotLeader { .. }) | Err(_) => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

async fn do_read(s: &Shared, key: &[u8]) -> Option<Option<(u64, Vec<u8>)>> {
    for _ in 0..400 {
        if let Some(li) = s.leader() {
            match s.nodes[li].read(key, Consistency::Linearizable).await {
                Ok(ReadOutcome::Value(v)) => return Some(v),
                Ok(ReadOutcome::NotLeader { .. }) | Ok(ReadOutcome::TooStale { .. }) | Err(_) => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

async fn stale_read_observer(s: Arc<Shared>) {
    for _ in 0..24 {
        for key_index in 0..KEYS {
            let key = key_bytes(key_index);
            let min_version = s
                .acked
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(acked_key, version)| (acked_key == &key).then_some(*version))
                .max();
            for (idx, node) in s.nodes.iter().enumerate() {
                if !s.alive[idx].load(Ordering::SeqCst) {
                    continue;
                }
                if let Ok(ReadOutcome::Value(value)) =
                    node.read(&key, Consistency::Stale { min_version }).await
                    && let (Some(minimum), Some((observed, _))) = (min_version, value)
                {
                    assert!(
                        observed >= minimum,
                        "stale read returned version {observed} below requested {minimum}"
                    );
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// One client: a mix of puts (mostly unconditional, some create-only) and reads
/// across the shared key space, recording each op's real-time interval.
async fn client(s: Arc<Shared>, client_id: u128, mut rng: Rng) {
    let mut seq = 1u64;
    for _ in 0..OPS_PER_TASK {
        let key = key_bytes(rng.below(KEYS as u64) as usize);

        if rng.below(3) == 0 {
            // Read.
            let call = s.tick();
            let Some(v) = do_read(&s, &key).await else {
                continue;
            };
            let ret = s.tick();
            s.history.lock().unwrap().push(Recorded {
                key,
                call,
                ret,
                inv: Invocation::Read,
                out: Outcome::Value(v),
            });
        } else {
            // Write: occasionally create-only, else unconditional.
            let create_only = rng.below(4) == 0;
            let value = format!("c{client_id}-s{seq}").into_bytes();
            let op = DataOp::Put {
                key: key.clone(),
                value: value.clone(),
                if_version: if create_only {
                    Some(IfVersion::Absent)
                } else {
                    None
                },
            };
            let dg = digest(&op);
            let req = DataRequest {
                client_id,
                sequence: seq,
                op,
            };

            let call = s.tick();
            let Some(result) = do_write(&s, &req).await else {
                continue;
            };
            let ret = s.tick();

            // A decided or replayed result advances the client stream.
            let Some(mutation) = result.mutation() else {
                // A protocol rejection carries no register semantics; skip it.
                seq += 1;
                continue;
            };
            let out = match mutation {
                MutationResult::Applied { version } => {
                    s.acked.lock().unwrap().push((key.clone(), version));
                    Outcome::Applied { version }
                }
                MutationResult::ConditionFailed { current } => Outcome::ConditionFailed {
                    present: present(current),
                },
            };
            s.applied.lock().unwrap().push(Applied {
                client_id,
                partition: 0,
                sequence: seq,
                digest: dg,
                fresh: matches!(&result, ApplyResult::Decided(_)),
            });

            // Deliberately append the same idempotency key again. The oracle
            // now observes both the original response and a concrete replay;
            // a second fresh decision is a real exactly-once violation.
            if let Some(replay) = do_write(&s, &req).await {
                assert_eq!(replay.mutation(), Some(mutation));
                s.applied.lock().unwrap().push(Applied {
                    client_id,
                    partition: 0,
                    sequence: seq,
                    digest: dg,
                    fresh: matches!(replay, ApplyResult::Decided(_)),
                });
            }
            s.history.lock().unwrap().push(Recorded {
                key,
                call,
                ret,
                inv: Invocation::Put {
                    value,
                    if_version: if create_only {
                        Some(IfVersion::Absent)
                    } else {
                        None
                    },
                },
                out,
            });
            seq += 1;
        }
    }
}

async fn run_once(seed: u64) {
    let dirs: Vec<TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let registry: Registry<TypeConfig> = Registry::default();
    let faults = Faults::default();

    let mut nodes = Vec::new();
    for i in 0..3 {
        let storage = Arc::new(Storage::open_checked(dirs[i].path(), CID, VOTERS[i]).unwrap());
        ensure_bootstrap_group(
            &storage,
            &BootstrapGroup {
                cluster_id: CID,
                group: GroupId::Data(0),
                members: VOTERS.to_vec(),
            },
        )
        .unwrap();
        nodes.push(Arc::new(
            PartitionNode::start(
                VOTERS[i],
                GroupId::Data(0),
                storage,
                registry.clone(),
                faults.clone(),
            )
            .await
            .unwrap(),
        ));
    }
    nodes[0].initialize(&VOTERS).await.unwrap();

    let shared = Arc::new(Shared {
        nodes,
        alive: (0..3).map(|_| AtomicBool::new(true)).collect(),
        clock: AtomicU64::new(0),
        history: Mutex::new(Vec::new()),
        acked: Mutex::new(Vec::new()),
        applied: Mutex::new(Vec::new()),
    });
    await_leader(&shared).await;

    // Open every process epoch through the same leader-issued quorum target
    // used in production before exercising follower-local reads.
    let leader = shared.leader().unwrap();
    for target in 0..shared.nodes.len() {
        let fence = match shared.nodes[leader]
            .issue_recovery_fence(shared.nodes[target].node_id())
            .await
        {
            RecoveryFenceReply::Fence(fence) => fence,
            other => panic!("verification leader did not issue recovery fence: {other:?}"),
        };
        shared.nodes[target]
            .apply_recovery_fence(&fence)
            .await
            .unwrap();
    }

    // Every seeded history now includes duplicate AppendEntries delivery,
    // alternating reordering delay, and a separately delayed follower.
    let followers: Vec<_> = (0..shared.nodes.len())
        .filter(|&idx| idx != leader)
        .collect();
    let leader_id = shared.nodes[leader].node_id();
    faults.duplicate(leader_id, shared.nodes[followers[0]].node_id());
    faults.reorder(leader_id, shared.nodes[followers[0]].node_id());
    faults.delay(
        leader_id,
        shared.nodes[followers[1]].node_id(),
        Duration::from_millis(3),
    );

    // Fault injection: after a short delay, crash the current leader once. The
    // surviving 2/3 keeps quorum, so acknowledged writes must not be lost.
    let fault = {
        let s = shared.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            if let Some(li) = s.leader() {
                // A shut-down node fails its RPCs, and `alive` steers clients
                // away — enough to model a crashed leader without a restart.
                s.alive[li].store(false, Ordering::SeqCst);
                let _ = s.nodes[li].shutdown().await;
            }
        })
    };

    // Concurrent clients.
    let mut tasks = Vec::new();
    for c in 0..4u128 {
        let s = shared.clone();
        let rng = Rng::new(seed ^ (c as u64).wrapping_mul(0x9E37_79B9));
        tasks.push(tokio::spawn(
            async move { client(s, 0xC000 + c, rng).await },
        ));
    }
    let stale_observer = tokio::spawn(stale_read_observer(shared.clone()));
    for t in tasks {
        t.await.unwrap();
    }
    stale_observer.await.unwrap();
    fault.await.unwrap();

    check_oracles(&shared).await;

    // A multi-seed stress run creates a fresh RocksDB/Raft cluster per seed.
    // Explicitly stop every survivor and remove registry-held Raft handles so
    // file descriptors and background tasks do not accumulate across seeds.
    for (idx, node) in shared.nodes.iter().enumerate() {
        if shared.alive[idx].swap(false, Ordering::SeqCst) {
            node.shutdown().await.unwrap();
        }
        registry.remove(node.node_id());
    }
}

async fn check_oracles(s: &Shared) {
    // Per-key linearizability.
    let history = s.history.lock().unwrap().clone();
    let mut by_key: HashMap<Vec<u8>, Vec<Op>> = HashMap::new();
    for r in &history {
        by_key.entry(r.key.clone()).or_default().push(Op {
            call: r.call,
            ret: r.ret,
            inv: r.inv.clone(),
            out: r.out.clone(),
        });
    }
    for (key, ops) in &by_key {
        assert!(
            is_linearizable(ops),
            "history for key {key:?} is not linearizable: {ops:#?}"
        );
    }

    // Exactly-once.
    let applied = s.applied.lock().unwrap().clone();
    exactly_once(&applied).expect("exactly-once violated");

    // No acknowledged write lost: read every key's final version and compare.
    let mut final_state: HashMap<Vec<u8>, u64> = HashMap::new();
    for i in 0..KEYS {
        let key = key_bytes(i);
        if let Some(Some((version, _))) = do_read(s, &key).await {
            final_state.insert(key, version);
        }
    }
    let acked = s.acked.lock().unwrap().clone();
    no_lost_write(&acked, &final_state).expect("acknowledged write lost");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn oracles_hold_under_leader_crash_multiple_seeds() {
    let seeds: Vec<u64> = match std::env::var("DAL_VERIFY_SEEDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        Some(count) if count > 0 => {
            let start = std::env::var("DAL_VERIFY_SEED_START")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            (start..start.saturating_add(count)).collect()
        }
        Some(_) => panic!("DAL_VERIFY_SEEDS must be greater than zero"),
        None => (0u64..100).collect(),
    };
    for seed in seeds {
        eprintln!("verification seed: {seed}");
        run_once(seed).await;
    }
}
