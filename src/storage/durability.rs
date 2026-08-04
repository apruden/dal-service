//! Database-wide RocksDB write and WAL durability coordination.
//!
//! A single bounded worker owns non-sync Raft log and state-machine writes for
//! one RocksDB instance. Log append and data-state apply return
//! after the worker has made their batches readable. Log `LogFlushed`
//! callbacks, state durability trackers, and synchronous meta-state waiters
//! complete only after one `flush_wal(true)` covers the collected writes.

use std::io;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rocksdb::WriteBatch;

use crate::perf::WriteStage;

pub(crate) type OnComplete = Box<dyn FnOnce(io::Result<()>) + Send + 'static>;
pub(crate) type OnDurable = OnComplete;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DurabilityKind {
    Log,
    State,
}

impl DurabilityKind {
    fn write_stage(self) -> WriteStage {
        match self {
            Self::Log => WriteStage::WalWriteUnsynced,
            Self::State => WriteStage::StateApplyWalWrite,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct DurabilityConfig {
    pub(super) max_pending_requests: usize,
    pub(super) max_pending_bytes: usize,
    pub(super) max_batch_delay: Duration,
    pub(super) adaptive_batching: bool,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            max_pending_requests: 1_024,
            max_pending_bytes: super::MAX_PENDING_WAL_BYTES,
            max_batch_delay: Duration::from_micros(200),
            // A lone request flushes immediately. Under concurrency, producers
            // reserve before enqueueing, which tells the worker to wait briefly
            // for the already-active writers and preserve cross-group batching.
            adaptive_batching: !matches!(
                std::env::var("DAL_ADAPTIVE_DURABILITY").ok().as_deref(),
                Some("0") | Some("false") | Some("off")
            ),
        }
    }
}

enum WorkerStatus {
    Running,
    Failed(String),
    Stopped,
}

struct BudgetState {
    status: WorkerStatus,
    pending_requests: usize,
    pending_bytes: usize,
}

struct Shared {
    state: Mutex<BudgetState>,
    capacity_available: Condvar,
    config: DurabilityConfig,
}

impl Shared {
    fn new(config: DurabilityConfig) -> Self {
        Self {
            state: Mutex::new(BudgetState {
                status: WorkerStatus::Running,
                pending_requests: 0,
                pending_bytes: 0,
            }),
            capacity_available: Condvar::new(),
            config,
        }
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> io::Result<Reservation> {
        if bytes > self.config.max_pending_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "WAL batch of {bytes} bytes exceeds durability limit of {} bytes",
                    self.config.max_pending_bytes
                ),
            ));
        }
        let charged_bytes = bytes;
        let mut state = self.state.lock().unwrap();

        loop {
            match &state.status {
                WorkerStatus::Running => {}
                WorkerStatus::Failed(error) => return Err(io::Error::other(error.clone())),
                WorkerStatus::Stopped => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "WAL durability coordinator stopped",
                    ));
                }
            }

            let request_fits = state.pending_requests < self.config.max_pending_requests;
            let bytes_fit =
                state.pending_bytes.saturating_add(charged_bytes) <= self.config.max_pending_bytes;
            if request_fits && bytes_fit {
                state.pending_requests += 1;
                state.pending_bytes += charged_bytes;
                return Ok(Reservation {
                    shared: self.clone(),
                    charged_bytes,
                    active: true,
                });
            }

            state = self.capacity_available.wait(state).unwrap();
        }
    }

    fn fail(&self, error: String) {
        let mut state = self.state.lock().unwrap();
        if matches!(state.status, WorkerStatus::Running) {
            state.status = WorkerStatus::Failed(error);
        }
        self.capacity_available.notify_all();
    }

    fn failure(&self) -> Option<String> {
        match &self.state.lock().unwrap().status {
            WorkerStatus::Failed(error) => Some(error.clone()),
            WorkerStatus::Running | WorkerStatus::Stopped => None,
        }
    }

    fn stop(&self) {
        let mut state = self.state.lock().unwrap();
        if matches!(state.status, WorkerStatus::Running) {
            state.status = WorkerStatus::Stopped;
        }
        self.capacity_available.notify_all();
    }

    fn pending_requests(&self) -> usize {
        self.state.lock().unwrap().pending_requests
    }

    fn current_error(&self) -> io::Error {
        let state = self.state.lock().unwrap();
        match &state.status {
            WorkerStatus::Failed(error) => io::Error::other(error.clone()),
            WorkerStatus::Running | WorkerStatus::Stopped => io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WAL durability coordinator is unavailable",
            ),
        }
    }
}

pub(crate) struct Reservation {
    shared: Arc<Shared>,
    charged_bytes: usize,
    active: bool,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.shared.state.lock().unwrap();
        state.pending_requests -= 1;
        state.pending_bytes -= self.charged_bytes;
        self.active = false;
        self.shared.capacity_available.notify_all();
    }
}

struct WriteRequest {
    reservation: Reservation,
    batch: WriteBatch,
    kind: DurabilityKind,
    on_written: Option<OnComplete>,
    on_durable: OnDurable,
}

struct PendingFlush {
    reservation: Reservation,
    kind: DurabilityKind,
    on_durable: OnDurable,
}

enum Command {
    Write(WriteRequest),
    Shutdown,
}

/// One bounded write/durability queue and worker per RocksDB instance.
pub(crate) struct WalDurability {
    sender: Option<SyncSender<Command>>,
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
}

impl WalDurability {
    pub(super) fn with_config<W, F>(
        config: DurabilityConfig,
        write_batch: W,
        flush_wal: F,
    ) -> io::Result<Self>
    where
        W: FnMut(WriteBatch) -> io::Result<()> + Send + 'static,
        F: FnMut() -> io::Result<()> + Send + 'static,
    {
        assert!(config.max_pending_requests > 0);
        assert!(config.max_pending_bytes > 0);

        let shared = Arc::new(Shared::new(config));
        let (sender, receiver) = mpsc::sync_channel(config.max_pending_requests);
        let worker_shared = shared.clone();
        let worker = thread::Builder::new()
            .name("dal-db-durability".into())
            .spawn(move || run_worker(receiver, worker_shared, write_batch, flush_wal))?;

        Ok(Self {
            sender: Some(sender),
            shared,
            worker: Some(worker),
        })
    }

    /// Reserve bounded not-yet-durable capacity before enqueueing a write.
    pub(crate) fn reserve(&self, bytes: usize) -> io::Result<Reservation> {
        self.shared.reserve(bytes)
    }

    /// A sink used by the database-wide health registry to fail this worker
    /// when a synchronous operation detects a storage fault.
    pub(crate) fn failure_hook(&self) -> Arc<dyn Fn(String) + Send + Sync> {
        let shared = self.shared.clone();
        Arc::new(move |error| shared.fail(error))
    }

    /// Enqueue a batch for the DB worker. `on_written` is used by Raft log
    /// append to preserve immediate readability; `on_durable` is released only
    /// after the shared WAL sync succeeds.
    pub(crate) fn submit(
        &self,
        reservation: Reservation,
        batch: WriteBatch,
        kind: DurabilityKind,
        on_written: Option<OnComplete>,
        on_durable: OnDurable,
    ) -> io::Result<()> {
        let request = WriteRequest {
            reservation,
            batch,
            kind,
            on_written,
            on_durable,
        };
        let Some(sender) = &self.sender else {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WAL durability coordinator stopped",
            ));
        };
        sender
            .send(Command::Write(request))
            .map_err(|_| self.shared.current_error())
    }
}

impl Drop for WalDurability {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            // All accepted writes precede Shutdown in this channel. The worker
            // writes and durably drains them before the DB handle is released.
            let _ = sender.send(Command::Shutdown);
        }
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            self.shared
                .fail("database durability coordinator panicked".into());
        }
    }
}

fn run_worker<W, F>(
    receiver: Receiver<Command>,
    shared: Arc<Shared>,
    mut write_batch: W,
    mut flush_wal: F,
) where
    W: FnMut(WriteBatch) -> io::Result<()>,
    F: FnMut() -> io::Result<()>,
{
    while let Ok(command) = receiver.recv() {
        let Command::Write(first) = command else {
            shared.stop();
            return;
        };

        if let Some(error) = shared.failure() {
            fail_unwritten(first, &error);
            fail_remaining(&receiver, &shared, &error);
            return;
        }

        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        let mut shutdown_after_flush = false;
        if let Err(error) = write_one(first, &mut write_batch, &mut batch, &mut batch_bytes) {
            shared.fail(error.clone());
            fail_remaining(&receiver, &shared, &error);
            return;
        }

        let deadline = Instant::now() + shared.config.max_batch_delay;
        let collect_profile = crate::perf::timer(WriteStage::WalBatchCollect);
        while batch.len() < shared.config.max_pending_requests
            && batch_bytes < shared.config.max_pending_bytes
        {
            let command = match receiver.try_recv() {
                Ok(command) => Some(command),
                Err(TryRecvError::Disconnected) => {
                    shutdown_after_flush = true;
                    None
                }
                Err(TryRecvError::Empty) => {
                    let active_writer_not_queued = shared.pending_requests() > batch.len();
                    // A truly lone write should not pay the batching window.
                    // Once concurrency is observed, retain the full window so
                    // subsequent producers can join this same durable flush.
                    let collect_for_concurrency = active_writer_not_queued || batch.len() > 1;
                    if shared.config.adaptive_batching && !collect_for_concurrency {
                        None
                    } else {
                        let now = Instant::now();
                        if now >= deadline {
                            None
                        } else {
                            match receiver.recv_timeout(deadline - now) {
                                Ok(command) => Some(command),
                                Err(RecvTimeoutError::Disconnected) => {
                                    shutdown_after_flush = true;
                                    None
                                }
                                Err(RecvTimeoutError::Timeout) => None,
                            }
                        }
                    }
                }
            };

            let Some(command) = command else {
                break;
            };
            match command {
                Command::Write(request) => {
                    if let Some(error) = shared.failure() {
                        drop(collect_profile);
                        fail_unwritten(request, &error);
                        complete_batch(batch, Some(&error));
                        fail_remaining(&receiver, &shared, &error);
                        return;
                    }
                    if let Err(error) =
                        write_one(request, &mut write_batch, &mut batch, &mut batch_bytes)
                    {
                        drop(collect_profile);
                        shared.fail(error.clone());
                        complete_batch(batch, Some(&error));
                        fail_remaining(&receiver, &shared, &error);
                        return;
                    }
                }
                Command::Shutdown => {
                    shutdown_after_flush = true;
                    break;
                }
            }
        }
        drop(collect_profile);

        if let Some(error) = shared.failure() {
            complete_batch(batch, Some(&error));
            fail_remaining(&receiver, &shared, &error);
            return;
        }

        let log_writes = batch
            .iter()
            .filter(|pending| pending.kind == DurabilityKind::Log)
            .count();
        let state_writes = batch.len() - log_writes;
        crate::perf::record_durability_batch(log_writes, state_writes, batch_bytes);

        let flush_result = {
            let _profile = crate::perf::timer(WriteStage::WalFlush);
            flush_wal()
        };
        match flush_result {
            Ok(()) => {
                if let Some(error) = shared.failure() {
                    complete_batch(batch, Some(&error));
                    fail_remaining(&receiver, &shared, &error);
                    return;
                }
                complete_batch(batch, None);
            }
            Err(error) => {
                let message = format!("WAL flush failed: {error}");
                shared.fail(message.clone());
                complete_batch(batch, Some(&message));
                fail_remaining(&receiver, &shared, &message);
                return;
            }
        }

        if shutdown_after_flush {
            shared.stop();
            return;
        }
    }

    shared.stop();
}

fn write_one<W>(
    request: WriteRequest,
    write_batch: &mut W,
    pending: &mut Vec<PendingFlush>,
    pending_bytes: &mut usize,
) -> Result<(), String>
where
    W: FnMut(WriteBatch) -> io::Result<()>,
{
    let WriteRequest {
        reservation,
        batch,
        kind,
        on_written,
        on_durable,
    } = request;
    let charged_bytes = reservation.charged_bytes;
    let result = {
        let _profile = crate::perf::timer(kind.write_stage());
        write_batch(batch)
    };
    match result {
        Ok(()) => {
            if let Some(callback) = on_written {
                callback(Ok(()));
            }
            *pending_bytes = pending_bytes.saturating_add(charged_bytes);
            pending.push(PendingFlush {
                reservation,
                kind,
                on_durable,
            });
            Ok(())
        }
        Err(error) => {
            let message = format!("database WAL write failed: {error}");
            if let Some(callback) = on_written {
                callback(Err(io::Error::other(message.clone())));
            } else {
                on_durable(Err(io::Error::other(message.clone())));
            }
            Err(message)
        }
    }
}

fn complete_batch(batch: Vec<PendingFlush>, error: Option<&str>) {
    for pending in batch {
        let PendingFlush {
            reservation,
            on_durable,
            ..
        } = pending;
        // Capacity can be reused as soon as the flush finishes. Callback
        // delivery remains serialized and ordered by this worker.
        drop(reservation);
        match error {
            Some(error) => on_durable(Err(io::Error::other(error.to_owned()))),
            None => on_durable(Ok(())),
        }
    }
}

fn fail_unwritten(request: WriteRequest, error: &str) {
    if let Some(callback) = request.on_written {
        callback(Err(io::Error::other(error.to_owned())));
    } else {
        (request.on_durable)(Err(io::Error::other(error.to_owned())));
    }
}

fn fail_remaining(receiver: &Receiver<Command>, shared: &Shared, error: &str) {
    // No new reservation can be acquired after `fail()`. Existing producers
    // may still be between reservation and enqueue, so keep draining until all
    // reservations have arrived or were released after enqueue failure.
    while shared.pending_requests() > 0 {
        match receiver.recv_timeout(Duration::from_millis(10)) {
            Ok(Command::Write(request)) => fail_unwritten(request, error),
            Ok(Command::Shutdown) | Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_config(adaptive_batching: bool) -> DurabilityConfig {
        DurabilityConfig {
            max_pending_requests: 8,
            max_pending_bytes: 1_024,
            max_batch_delay: Duration::from_millis(500),
            adaptive_batching,
        }
    }

    fn batch() -> WriteBatch {
        let mut batch = WriteBatch::default();
        batch.put(b"key", b"value");
        batch
    }

    #[test]
    fn groups_active_writers_into_one_flush() {
        let writes = Arc::new(AtomicUsize::new(0));
        let worker_writes = writes.clone();
        let flushes = Arc::new(AtomicUsize::new(0));
        let worker_flushes = flushes.clone();
        let durability = WalDurability::with_config(
            test_config(true),
            move |_| {
                worker_writes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            move || {
                worker_flushes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap();
        let (tx, rx) = mpsc::channel();

        // Reserve both first so adaptive collection knows another active writer
        // exists even if the worker receives the first command immediately.
        let first = durability.reserve(16).unwrap();
        let second = durability.reserve(16).unwrap();
        for (id, reservation) in [(1, first), (2, second)] {
            let tx = tx.clone();
            durability
                .submit(
                    reservation,
                    batch(),
                    DurabilityKind::Log,
                    None,
                    Box::new(move |result| tx.send((id, result.is_ok())).unwrap()),
                )
                .unwrap();
        }

        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), (1, true));
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), (2, true));
        assert_eq!(writes.load(Ordering::SeqCst), 2);
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn adaptive_batching_flushes_a_lone_writer_without_the_collection_delay() {
        let durability =
            WalDurability::with_config(test_config(true), |_| Ok(()), || Ok(())).unwrap();
        let (tx, rx) = mpsc::channel();
        let reservation = durability.reserve(16).unwrap();
        durability
            .submit(
                reservation,
                batch(),
                DurabilityKind::Log,
                None,
                Box::new(move |result| tx.send(result.is_ok()).unwrap()),
            )
            .unwrap();

        assert!(rx.recv_timeout(Duration::from_millis(100)).unwrap());
    }

    #[test]
    fn external_database_failure_fails_an_already_written_request() {
        let durability =
            WalDurability::with_config(test_config(false), |_| Ok(()), || Ok(())).unwrap();
        let (written_tx, written_rx) = mpsc::channel();
        let (durable_tx, durable_rx) = mpsc::channel();
        let reservation = durability.reserve(16).unwrap();
        durability
            .submit(
                reservation,
                batch(),
                DurabilityKind::Log,
                Some(Box::new(move |result| {
                    written_tx.send(result.is_ok()).unwrap()
                })),
                Box::new(move |result| durable_tx.send(result).unwrap()),
            )
            .unwrap();

        // The write is readable, but still inside the collection window and
        // has not received its durability callback.
        assert!(written_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        durability.failure_hook()("external database storage failure".into());

        let error = durable_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("external database storage failure")
        );
        assert!(durability.reserve(1).is_err());
    }

    #[test]
    fn oversized_batch_is_rejected_instead_of_undercharged() {
        let durability =
            WalDurability::with_config(test_config(true), |_| Ok(()), || Ok(())).unwrap();
        assert!(durability.reserve(1_025).is_err());
        assert_eq!(durability.shared.pending_requests(), 0);
    }

    #[test]
    fn flush_failure_fails_mixed_log_and_state_waiters() {
        let flushes = Arc::new(AtomicUsize::new(0));
        let worker_flushes = flushes.clone();
        let durability = WalDurability::with_config(
            test_config(false),
            |_| Ok(()),
            move || {
                worker_flushes.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("injected failure"))
            },
        )
        .unwrap();
        let (written_tx, written_rx) = mpsc::channel();
        let (durable_tx, durable_rx) = mpsc::channel();
        let log_reservation = durability.reserve(16).unwrap();
        let state_reservation = durability.reserve(16).unwrap();
        let log_durable_tx = durable_tx.clone();
        durability
            .submit(
                log_reservation,
                batch(),
                DurabilityKind::Log,
                Some(Box::new(move |result| {
                    written_tx.send(result.is_ok()).unwrap()
                })),
                Box::new(move |result| log_durable_tx.send(("log", result.is_err())).unwrap()),
            )
            .unwrap();
        durability
            .submit(
                state_reservation,
                batch(),
                DurabilityKind::State,
                None,
                Box::new(move |result| durable_tx.send(("state", result.is_err())).unwrap()),
            )
            .unwrap();

        assert!(written_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        let mut results = [
            durable_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            durable_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ];
        results.sort_unstable();
        assert_eq!(results, [("log", true), ("state", true)]);
        assert!(durability.reserve(1).is_err());
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
    }
}
