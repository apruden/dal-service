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
use dal::types::{
    BootstrapGroup, DataOp, DataRequest, GroupId, IfVersion, KeyPresence, MutationResult,
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
            match s.nodes[li].read(key).await {
                Ok(ReadOutcome::Value(v)) => return Some(v),
                Ok(ReadOutcome::NotLeader { .. }) | Err(_) => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
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
            let out = match result.mutation() {
                Some(MutationResult::Applied { version }) => {
                    s.acked.lock().unwrap().push((key.clone(), version));
                    Outcome::Applied { version }
                }
                Some(MutationResult::ConditionFailed { current }) => Outcome::ConditionFailed {
                    present: present(current),
                },
                // A protocol rejection carries no register semantics; skip it.
                None => {
                    seq += 1;
                    continue;
                }
            };
            s.applied.lock().unwrap().push(Applied {
                client_id,
                partition: 0,
                sequence: seq,
                digest: dg,
            });
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
    for t in tasks {
        t.await.unwrap();
    }
    fault.await.unwrap();

    check_oracles(&shared).await;
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
    for seed in [1u64, 7, 42, 1337] {
        run_once(seed).await;
    }
}
