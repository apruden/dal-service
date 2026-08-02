//! Opt-in write-path profiling used by the release benchmark.
//!
//! Set `DAL_PROFILE_WRITE_PATH=1` before process start. Disabled mode avoids
//! reading the clock; enabled mode records aggregate wall time around the
//! durable stages that dominate the Raft write path.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::transport::codec::MsgType;
use crate::types::GroupId;

#[derive(Debug, Clone, Copy)]
#[repr(usize)]
pub enum WriteStage {
    ClientTransportCall,
    ClientGatewayHandle,
    RaftClientWrite,
    RaftAppendTransportCall,
    RaftAppendHandle,
    ClientReplyQueueWait,
    RaftReplyQueueWait,
    ClientDealerQueueWait,
    RaftDealerQueueWait,
    ClientDealerRoundTrip,
    RaftDealerRoundTrip,
    ClientDealerWaiterResume,
    RaftDealerWaiterResume,
    ClientRouterSchedule,
    RaftRouterSchedule,
    ClientRouterHandler,
    RaftRouterHandler,
    ClientReplyEnqueue,
    RaftReplyEnqueue,
    ClientRouterSend,
    RaftRouterSend,
    ClientEnvelopeEncode,
    ClientEnvelopeDecode,
    RaftEnvelopeEncode,
    RaftEnvelopeDecode,
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
    StateApplyCapacityWait,
    StateApplyWalWrite,
    StateApplyDurabilityWait,
    StateApplySynced,
}

const STAGES: [WriteStage; 41] = [
    WriteStage::ClientTransportCall,
    WriteStage::ClientGatewayHandle,
    WriteStage::RaftClientWrite,
    WriteStage::RaftAppendTransportCall,
    WriteStage::RaftAppendHandle,
    WriteStage::ClientReplyQueueWait,
    WriteStage::RaftReplyQueueWait,
    WriteStage::ClientDealerQueueWait,
    WriteStage::RaftDealerQueueWait,
    WriteStage::ClientDealerRoundTrip,
    WriteStage::RaftDealerRoundTrip,
    WriteStage::ClientDealerWaiterResume,
    WriteStage::RaftDealerWaiterResume,
    WriteStage::ClientRouterSchedule,
    WriteStage::RaftRouterSchedule,
    WriteStage::ClientRouterHandler,
    WriteStage::RaftRouterHandler,
    WriteStage::ClientReplyEnqueue,
    WriteStage::RaftReplyEnqueue,
    WriteStage::ClientRouterSend,
    WriteStage::RaftRouterSend,
    WriteStage::ClientEnvelopeEncode,
    WriteStage::ClientEnvelopeDecode,
    WriteStage::RaftEnvelopeEncode,
    WriteStage::RaftEnvelopeDecode,
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
    WriteStage::StateApplyCapacityWait,
    WriteStage::StateApplyWalWrite,
    WriteStage::StateApplyDurabilityWait,
    WriteStage::StateApplySynced,
];

impl WriteStage {
    fn name(self) -> &'static str {
        match self {
            Self::ClientTransportCall => "client transport call",
            Self::ClientGatewayHandle => "client gateway handle",
            Self::RaftClientWrite => "raft client_write",
            Self::RaftAppendTransportCall => "Raft append transport",
            Self::RaftAppendHandle => "Raft append handler",
            Self::ClientReplyQueueWait => "client reply queue",
            Self::RaftReplyQueueWait => "Raft reply queue",
            Self::ClientDealerQueueWait => "client DEALER queue->send",
            Self::RaftDealerQueueWait => "Raft DEALER queue->send",
            Self::ClientDealerRoundTrip => "client DEALER wire roundtrip",
            Self::RaftDealerRoundTrip => "Raft DEALER wire roundtrip",
            Self::ClientDealerWaiterResume => "client reply->waiter resume",
            Self::RaftDealerWaiterResume => "Raft reply->waiter resume",
            Self::ClientRouterSchedule => "client ROUTER->handler start",
            Self::RaftRouterSchedule => "Raft ROUTER->handler start",
            Self::ClientRouterHandler => "client ROUTER handler total",
            Self::RaftRouterHandler => "Raft ROUTER handler total",
            Self::ClientReplyEnqueue => "client handler->reply enqueue",
            Self::RaftReplyEnqueue => "Raft handler->reply enqueue",
            Self::ClientRouterSend => "client ROUTER reply send",
            Self::RaftRouterSend => "Raft ROUTER reply send",
            Self::ClientEnvelopeEncode => "client envelope encode",
            Self::ClientEnvelopeDecode => "client envelope decode",
            Self::RaftEnvelopeEncode => "Raft envelope encode",
            Self::RaftEnvelopeDecode => "Raft envelope decode",
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
            Self::StateApplyCapacityWait => "state capacity wait",
            Self::StateApplyWalWrite => "state non-sync write",
            Self::StateApplyDurabilityWait => "state durability wait",
            Self::StateApplySynced => "state apply sync write",
        }
    }
}

static CALLS: [AtomicU64; STAGES.len()] = [const { AtomicU64::new(0) }; STAGES.len()];
static TOTAL_NS: [AtomicU64; STAGES.len()] = [const { AtomicU64::new(0) }; STAGES.len()];
static MAX_NS: [AtomicU64; STAGES.len()] = [const { AtomicU64::new(0) }; STAGES.len()];
static DURABILITY_LOG_WRITES: AtomicU64 = AtomicU64::new(0);
static DURABILITY_STATE_WRITES: AtomicU64 = AtomicU64::new(0);
static DURABILITY_LOGICAL_BYTES: AtomicU64 = AtomicU64::new(0);
static DURABILITY_MAX_BATCH: AtomicU64 = AtomicU64::new(0);
static COMMITTED_MARKERS_FOLDED: AtomicU64 = AtomicU64::new(0);
static APPLY_BATCHES: AtomicU64 = AtomicU64::new(0);
static APPLY_ENTRIES: AtomicU64 = AtomicU64::new(0);
static APPLY_MUTATIONS: AtomicU64 = AtomicU64::new(0);
static APPLY_LOGICAL_BYTES: AtomicU64 = AtomicU64::new(0);
static APPLY_MAX_ENTRIES: AtomicU64 = AtomicU64::new(0);
static CLIENT_ENVELOPE_ENCODE_BYTES: AtomicU64 = AtomicU64::new(0);
static CLIENT_ENVELOPE_DECODE_BYTES: AtomicU64 = AtomicU64::new(0);
static RAFT_ENVELOPE_ENCODE_BYTES: AtomicU64 = AtomicU64::new(0);
static RAFT_ENVELOPE_DECODE_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
pub(crate) enum TransportProfileClass {
    Client,
    DataRaftAppend,
}

pub(crate) fn transport_profile_class(
    msg_type: MsgType,
    group_id: GroupId,
) -> Option<TransportProfileClass> {
    match (msg_type, group_id) {
        (MsgType::ClientOp, _) => Some(TransportProfileClass::Client),
        (MsgType::RaftAppend, GroupId::Data(_)) => Some(TransportProfileClass::DataRaftAppend),
        _ => None,
    }
}

impl TransportProfileClass {
    pub(crate) fn stage(self, client: WriteStage, raft: WriteStage) -> WriteStage {
        match self {
            Self::Client => client,
            Self::DataRaftAppend => raft,
        }
    }
}

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
            record_duration(self.stage, started.elapsed());
        }
    }
}

pub(crate) fn record_duration(stage: WriteStage, duration: Duration) {
    let index = stage as usize;
    let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
    CALLS[index].fetch_add(1, Ordering::Relaxed);
    TOTAL_NS[index].fetch_add(nanos, Ordering::Relaxed);
    MAX_NS[index].fetch_max(nanos, Ordering::Relaxed);
}

pub(crate) fn record_envelope(msg_type: MsgType, encode: bool, duration: Duration, bytes: usize) {
    let (stage, byte_counter) = match (msg_type, encode) {
        (MsgType::ClientOp, true) => (
            WriteStage::ClientEnvelopeEncode,
            &CLIENT_ENVELOPE_ENCODE_BYTES,
        ),
        (MsgType::ClientOp, false) => (
            WriteStage::ClientEnvelopeDecode,
            &CLIENT_ENVELOPE_DECODE_BYTES,
        ),
        (MsgType::RaftAppend, true) => {
            (WriteStage::RaftEnvelopeEncode, &RAFT_ENVELOPE_ENCODE_BYTES)
        }
        (MsgType::RaftAppend, false) => {
            (WriteStage::RaftEnvelopeDecode, &RAFT_ENVELOPE_DECODE_BYTES)
        }
        _ => return,
    };
    record_duration(stage, duration);
    byte_counter.fetch_add(bytes.min(u64::MAX as usize) as u64, Ordering::Relaxed);
}

pub(crate) fn record_durability_batch(
    log_writes: usize,
    state_writes: usize,
    logical_bytes: usize,
) {
    if !write_path_enabled() {
        return;
    }
    let log_writes = log_writes.min(u64::MAX as usize) as u64;
    let state_writes = state_writes.min(u64::MAX as usize) as u64;
    DURABILITY_LOG_WRITES.fetch_add(log_writes, Ordering::Relaxed);
    DURABILITY_STATE_WRITES.fetch_add(state_writes, Ordering::Relaxed);
    DURABILITY_LOGICAL_BYTES.fetch_add(
        logical_bytes.min(u64::MAX as usize) as u64,
        Ordering::Relaxed,
    );
    DURABILITY_MAX_BATCH.fetch_max(log_writes.saturating_add(state_writes), Ordering::Relaxed);
}

pub(crate) fn record_committed_marker_folded() {
    if write_path_enabled() {
        COMMITTED_MARKERS_FOLDED.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) fn record_apply_batch(entries: usize, mutations: usize, logical_bytes: usize) {
    if !write_path_enabled() {
        return;
    }
    let entries = entries.min(u64::MAX as usize) as u64;
    APPLY_BATCHES.fetch_add(1, Ordering::Relaxed);
    APPLY_ENTRIES.fetch_add(entries, Ordering::Relaxed);
    APPLY_MUTATIONS.fetch_add(mutations.min(u64::MAX as usize) as u64, Ordering::Relaxed);
    APPLY_LOGICAL_BYTES.fetch_add(
        logical_bytes.min(u64::MAX as usize) as u64,
        Ordering::Relaxed,
    );
    APPLY_MAX_ENTRIES.fetch_max(entries, Ordering::Relaxed);
}

pub fn reset_write_path() {
    for index in 0..STAGES.len() {
        CALLS[index].store(0, Ordering::Relaxed);
        TOTAL_NS[index].store(0, Ordering::Relaxed);
        MAX_NS[index].store(0, Ordering::Relaxed);
    }
    DURABILITY_LOG_WRITES.store(0, Ordering::Relaxed);
    DURABILITY_STATE_WRITES.store(0, Ordering::Relaxed);
    DURABILITY_LOGICAL_BYTES.store(0, Ordering::Relaxed);
    DURABILITY_MAX_BATCH.store(0, Ordering::Relaxed);
    COMMITTED_MARKERS_FOLDED.store(0, Ordering::Relaxed);
    APPLY_BATCHES.store(0, Ordering::Relaxed);
    APPLY_ENTRIES.store(0, Ordering::Relaxed);
    APPLY_MUTATIONS.store(0, Ordering::Relaxed);
    APPLY_LOGICAL_BYTES.store(0, Ordering::Relaxed);
    APPLY_MAX_ENTRIES.store(0, Ordering::Relaxed);
    CLIENT_ENVELOPE_ENCODE_BYTES.store(0, Ordering::Relaxed);
    CLIENT_ENVELOPE_DECODE_BYTES.store(0, Ordering::Relaxed);
    RAFT_ENVELOPE_ENCODE_BYTES.store(0, Ordering::Relaxed);
    RAFT_ENVELOPE_DECODE_BYTES.store(0, Ordering::Relaxed);
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
        let log_writes = DURABILITY_LOG_WRITES.load(Ordering::Relaxed);
        let state_writes = DURABILITY_STATE_WRITES.load(Ordering::Relaxed);
        let writes = log_writes.saturating_add(state_writes);
        let logical_bytes = DURABILITY_LOGICAL_BYTES.load(Ordering::Relaxed);
        let max_batch = DURABILITY_MAX_BATCH.load(Ordering::Relaxed);
        report.push_str(&format!(
            "  durability batches: flushes={flushes}, log_writes={log_writes}, state_writes={state_writes}, avg_writes={:.2}, max_writes={max_batch}, logical_bytes={:.2} MiB\n",
            writes as f64 / flushes as f64,
            logical_bytes as f64 / (1024.0 * 1024.0),
        ));
    }
    let apply_batches = APPLY_BATCHES.load(Ordering::Relaxed);
    if apply_batches > 0 {
        let entries = APPLY_ENTRIES.load(Ordering::Relaxed);
        let mutations = APPLY_MUTATIONS.load(Ordering::Relaxed);
        let logical_bytes = APPLY_LOGICAL_BYTES.load(Ordering::Relaxed);
        let max_entries = APPLY_MAX_ENTRIES.load(Ordering::Relaxed);
        let folded = COMMITTED_MARKERS_FOLDED.load(Ordering::Relaxed);
        report.push_str(&format!(
            "  apply batches: batches={apply_batches}, entries={entries}, avg_entries={:.2}, max_entries={max_entries}, mutations={mutations}, logical_bytes={:.2} MiB, committed_markers_folded={folded}\n",
            entries as f64 / apply_batches as f64,
            logical_bytes as f64 / (1024.0 * 1024.0),
        ));
    }
    let client_encoded = CLIENT_ENVELOPE_ENCODE_BYTES.load(Ordering::Relaxed);
    let client_decoded = CLIENT_ENVELOPE_DECODE_BYTES.load(Ordering::Relaxed);
    let raft_encoded = RAFT_ENVELOPE_ENCODE_BYTES.load(Ordering::Relaxed);
    let raft_decoded = RAFT_ENVELOPE_DECODE_BYTES.load(Ordering::Relaxed);
    if client_encoded + client_decoded + raft_encoded + raft_decoded > 0 {
        report.push_str(&format!(
            "  envelope bytes: client encode/decode={:.2}/{:.2} MiB, Raft encode/decode={:.2}/{:.2} MiB\n",
            client_encoded as f64 / (1024.0 * 1024.0),
            client_decoded as f64 / (1024.0 * 1024.0),
            raft_encoded as f64 / (1024.0 * 1024.0),
            raft_decoded as f64 / (1024.0 * 1024.0),
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
