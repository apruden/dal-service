//! Opt-in write-path profiling used by the release benchmark.
//!
//! Set `DAL_PROFILE_WRITE_PATH=1` before process start. Disabled mode avoids
//! reading the clock; enabled mode records aggregate wall time around the
//! durable stages that dominate the Raft write path.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub enum WriteStage {
    RaftClientWrite,
    LogEncode,
    WalCapacityWait,
    WalAppendLockWait,
    WalWriteUnsynced,
    WalDurabilityWait,
    WalBatchCollect,
    WalFlush,
    LogWriteSynced,
    SaveVoteSynced,
    SaveCommittedWal,
    SaveCommittedSynced,
    StateApplyTotal,
    StateApplySynced,
}

const STAGES: [WriteStage; 14] = [
    WriteStage::RaftClientWrite,
    WriteStage::LogEncode,
    WriteStage::WalCapacityWait,
    WriteStage::WalAppendLockWait,
    WriteStage::WalWriteUnsynced,
    WriteStage::WalDurabilityWait,
    WriteStage::WalBatchCollect,
    WriteStage::WalFlush,
    WriteStage::LogWriteSynced,
    WriteStage::SaveVoteSynced,
    WriteStage::SaveCommittedWal,
    WriteStage::SaveCommittedSynced,
    WriteStage::StateApplyTotal,
    WriteStage::StateApplySynced,
];

impl WriteStage {
    fn name(self) -> &'static str {
        match self {
            Self::RaftClientWrite => "raft client_write",
            Self::LogEncode => "log encode/batch",
            Self::WalCapacityWait => "WAL capacity wait",
            Self::WalAppendLockWait => "WAL append-lock wait",
            Self::WalWriteUnsynced => "WAL non-sync write",
            Self::WalDurabilityWait => "WAL append->callback",
            Self::WalBatchCollect => "WAL batch collection",
            Self::WalFlush => "WAL flush_wal(true)",
            Self::LogWriteSynced => "log sync write",
            Self::SaveVoteSynced => "vote sync write",
            Self::SaveCommittedWal => "committed WAL write",
            Self::SaveCommittedSynced => "committed sync write",
            Self::StateApplyTotal => "state apply total",
            Self::StateApplySynced => "state apply sync write",
        }
    }
}

static CALLS: [AtomicU64; STAGES.len()] = [const { AtomicU64::new(0) }; STAGES.len()];
static TOTAL_NS: [AtomicU64; STAGES.len()] = [const { AtomicU64::new(0) }; STAGES.len()];
static MAX_NS: [AtomicU64; STAGES.len()] = [const { AtomicU64::new(0) }; STAGES.len()];
static WAL_CALLBACKS: AtomicU64 = AtomicU64::new(0);
static WAL_LOGICAL_BYTES: AtomicU64 = AtomicU64::new(0);
static WAL_MAX_BATCH: AtomicU64 = AtomicU64::new(0);

pub fn write_path_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        matches!(
            std::env::var("DAL_PROFILE_WRITE_PATH").ok().as_deref(),
            Some("1") | Some("true") | Some("on")
        )
    })
}

pub struct StageTimer {
    stage: WriteStage,
    started: Option<Instant>,
}

pub fn timer(stage: WriteStage) -> StageTimer {
    StageTimer {
        stage,
        started: write_path_enabled().then(Instant::now),
    }
}

impl Drop for StageTimer {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            record(self.stage, started.elapsed());
        }
    }
}

fn record(stage: WriteStage, duration: Duration) {
    let index = stage as usize;
    let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
    CALLS[index].fetch_add(1, Ordering::Relaxed);
    TOTAL_NS[index].fetch_add(nanos, Ordering::Relaxed);
    MAX_NS[index].fetch_max(nanos, Ordering::Relaxed);
}

pub(crate) fn record_wal_batch(callbacks: usize, logical_bytes: usize) {
    if !write_path_enabled() {
        return;
    }
    let callbacks = callbacks.min(u64::MAX as usize) as u64;
    WAL_CALLBACKS.fetch_add(callbacks, Ordering::Relaxed);
    WAL_LOGICAL_BYTES.fetch_add(
        logical_bytes.min(u64::MAX as usize) as u64,
        Ordering::Relaxed,
    );
    WAL_MAX_BATCH.fetch_max(callbacks, Ordering::Relaxed);
}

pub fn reset_write_path() {
    for index in 0..STAGES.len() {
        CALLS[index].store(0, Ordering::Relaxed);
        TOTAL_NS[index].store(0, Ordering::Relaxed);
        MAX_NS[index].store(0, Ordering::Relaxed);
    }
    WAL_CALLBACKS.store(0, Ordering::Relaxed);
    WAL_LOGICAL_BYTES.store(0, Ordering::Relaxed);
    WAL_MAX_BATCH.store(0, Ordering::Relaxed);
}

pub fn write_path_report() -> Option<String> {
    if !write_path_enabled() {
        return None;
    }

    let mut report = String::from(
        "\n=== write-path profile (aggregate wall time; concurrent stages overlap) ===\n",
    );
    report.push_str("  stage                         calls      total       mean        max\n");
    for stage in STAGES {
        let index = stage as usize;
        let calls = CALLS[index].load(Ordering::Relaxed);
        if calls == 0 {
            continue;
        }
        let total_ns = TOTAL_NS[index].load(Ordering::Relaxed);
        let max_ns = MAX_NS[index].load(Ordering::Relaxed);
        let mean_us = total_ns as f64 / calls as f64 / 1_000.0;
        report.push_str(&format!(
            "  {:<28} {:>7}  {:>8.2}s  {:>8.2}us  {:>8.2}ms\n",
            stage.name(),
            calls,
            total_ns as f64 / 1e9,
            mean_us,
            max_ns as f64 / 1e6,
        ));
    }

    let flushes = CALLS[WriteStage::WalFlush as usize].load(Ordering::Relaxed);
    if flushes > 0 {
        let callbacks = WAL_CALLBACKS.load(Ordering::Relaxed);
        let logical_bytes = WAL_LOGICAL_BYTES.load(Ordering::Relaxed);
        let max_batch = WAL_MAX_BATCH.load(Ordering::Relaxed);
        report.push_str(&format!(
            "  WAL batches: flushes={flushes}, callbacks={callbacks}, avg_callbacks={:.2}, max_callbacks={max_batch}, logical_bytes={:.2} MiB\n",
            callbacks as f64 / flushes as f64,
            logical_bytes as f64 / (1024.0 * 1024.0),
        ));
    }
    report.push_str("  ====================================================================\n");
    Some(report)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RocksCounters {
    pub wal_syncs: u64,
    pub wal_bytes: u64,
    pub stall_micros: u64,
}

impl RocksCounters {
    pub fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            wal_syncs: self.wal_syncs.saturating_sub(earlier.wal_syncs),
            wal_bytes: self.wal_bytes.saturating_sub(earlier.wal_bytes),
            stall_micros: self.stall_micros.saturating_sub(earlier.stall_micros),
        }
    }
}

impl std::ops::Add for RocksCounters {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            wal_syncs: self.wal_syncs.saturating_add(rhs.wal_syncs),
            wal_bytes: self.wal_bytes.saturating_add(rhs.wal_bytes),
            stall_micros: self.stall_micros.saturating_add(rhs.stall_micros),
        }
    }
}
