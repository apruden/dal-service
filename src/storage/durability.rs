//! Database-wide RocksDB WAL group commit.
//!
//! Raft appends first write to the WAL without `sync`, then register their
//! [`OnDurable`] callback here. A single worker coalesces callbacks from every
//! column family and completes them only after one `flush_wal(true)` has made
//! all preceding WAL writes durable.

use std::io;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::perf::WriteStage;

pub(crate) type OnDurable = Box<dyn FnOnce(io::Result<()>) + Send + 'static>;

#[derive(Clone, Copy)]
pub(super) struct DurabilityConfig {
    pub(super) max_pending_requests: usize,
    pub(super) max_pending_bytes: usize,
    pub(super) max_batch_delay: Duration,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            max_pending_requests: 1_024,
            max_pending_bytes: 64 * 1024 * 1024,
            // Long enough for concurrently active Raft groups to join a flush,
            // while adding substantially less than a normal storage fsync.
            max_batch_delay: Duration::from_micros(200),
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
        // One oversized append is allowed when the byte budget is otherwise
        // empty. Charging it at the limit prevents another request from being
        // admitted until that append is flushed.
        let charged_bytes = bytes.min(self.config.max_pending_bytes);
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

struct PendingFlush {
    _reservation: Reservation,
    callback: OnDurable,
}

enum Command {
    Flush(PendingFlush),
    Shutdown,
}

/// One bounded WAL durability queue and worker per RocksDB instance.
pub(crate) struct WalDurability {
    sender: Option<SyncSender<Command>>,
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
}

impl WalDurability {
    pub(super) fn with_config<F>(config: DurabilityConfig, flush_wal: F) -> io::Result<Self>
    where
        F: FnMut() -> io::Result<()> + Send + 'static,
    {
        assert!(config.max_pending_requests > 0);
        assert!(config.max_pending_bytes > 0);

        let shared = Arc::new(Shared::new(config));
        let (sender, receiver) = mpsc::sync_channel(config.max_pending_requests);
        let worker_shared = shared.clone();
        let worker = thread::Builder::new()
            .name("dal-wal-durability".into())
            .spawn(move || run_worker(receiver, worker_shared, flush_wal))?;

        Ok(Self {
            sender: Some(sender),
            shared,
            worker: Some(worker),
        })
    }

    /// Reserve bounded unflushed-WAL capacity before performing the RocksDB
    /// write, so producers apply backpressure instead of growing without bound.
    pub(crate) fn reserve(&self, bytes: usize) -> io::Result<Reservation> {
        self.shared.reserve(bytes)
    }

    /// Register a completed non-sync WAL write for the next durable flush.
    pub(crate) fn submit(&self, reservation: Reservation, callback: OnDurable) -> io::Result<()> {
        let pending = PendingFlush {
            _reservation: reservation,
            callback,
        };
        let Some(sender) = &self.sender else {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WAL durability coordinator stopped",
            ));
        };
        sender
            .send(Command::Flush(pending))
            .map_err(|_| self.shared.current_error())
    }
}

impl Drop for WalDurability {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            // Commands from all completed writes precede Shutdown in this
            // channel, so the worker durably drains them before exiting.
            let _ = sender.send(Command::Shutdown);
        }
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            self.shared
                .fail("WAL durability coordinator panicked".into());
        }
    }
}

fn run_worker<F>(receiver: Receiver<Command>, shared: Arc<Shared>, mut flush_wal: F)
where
    F: FnMut() -> io::Result<()>,
{
    while let Ok(command) = receiver.recv() {
        let Command::Flush(first) = command else {
            shared.stop();
            return;
        };

        let mut batch = vec![first];
        let mut batch_bytes = batch[0]._reservation.charged_bytes;
        let deadline = Instant::now() + shared.config.max_batch_delay;
        let mut shutdown_after_flush = false;
        let collect_profile = crate::perf::timer(WriteStage::WalBatchCollect);

        while batch.len() < shared.config.max_pending_requests
            && batch_bytes < shared.config.max_pending_bytes
        {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match receiver.recv_timeout(deadline - now) {
                Ok(Command::Flush(pending)) => {
                    batch_bytes += pending._reservation.charged_bytes;
                    batch.push(pending);
                }
                Ok(Command::Shutdown) => {
                    shutdown_after_flush = true;
                    break;
                }
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
            }
        }
        drop(collect_profile);
        crate::perf::record_wal_batch(batch.len(), batch_bytes);

        let flush_result = {
            let _profile = crate::perf::timer(WriteStage::WalFlush);
            flush_wal()
        };
        match flush_result {
            Ok(()) => complete_batch(batch, None),
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

fn complete_batch(batch: Vec<PendingFlush>, error: Option<&str>) {
    for pending in batch {
        let PendingFlush {
            _reservation: reservation,
            callback,
        } = pending;
        // Capacity can be reused as soon as the flush finishes. Callback
        // delivery remains serialized and ordered by this worker.
        drop(reservation);
        match error {
            Some(error) => callback(Err(io::Error::other(error.to_owned()))),
            None => callback(Ok(())),
        }
    }
}

fn fail_remaining(receiver: &Receiver<Command>, shared: &Shared, error: &str) {
    // No new reservation can be acquired after `fail()`. Existing producers
    // may still be between reservation and enqueue, so keep draining until all
    // reservations have either arrived here or were released after write error.
    while shared.pending_requests() > 0 {
        match receiver.recv_timeout(Duration::from_millis(10)) {
            Ok(Command::Flush(pending)) => complete_batch(vec![pending], Some(error)),
            Ok(Command::Shutdown) | Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_config() -> DurabilityConfig {
        DurabilityConfig {
            max_pending_requests: 8,
            max_pending_bytes: 1_024,
            max_batch_delay: Duration::from_millis(50),
        }
    }

    #[test]
    fn groups_pending_callbacks_into_one_flush() {
        let flushes = Arc::new(AtomicUsize::new(0));
        let worker_flushes = flushes.clone();
        let durability = WalDurability::with_config(test_config(), move || {
            worker_flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        let (tx, rx) = mpsc::channel();

        for id in 1..=2 {
            let reservation = durability.reserve(16).unwrap();
            let tx = tx.clone();
            durability
                .submit(
                    reservation,
                    Box::new(move |result| tx.send((id, result.is_ok())).unwrap()),
                )
                .unwrap();
        }

        assert!(rx.recv_timeout(Duration::from_millis(10)).is_err());
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), (1, true));
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), (2, true));
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn flush_failure_fails_the_batch_and_stops_later_success() {
        let flushes = Arc::new(AtomicUsize::new(0));
        let worker_flushes = flushes.clone();
        let durability = WalDurability::with_config(test_config(), move || {
            worker_flushes.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("injected failure"))
        })
        .unwrap();
        let (tx, rx) = mpsc::channel();

        for id in 1..=2 {
            let reservation = durability.reserve(16).unwrap();
            let tx = tx.clone();
            durability
                .submit(
                    reservation,
                    Box::new(move |result| tx.send((id, result.is_err())).unwrap()),
                )
                .unwrap();
        }

        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), (1, true));
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), (2, true));
        assert!(durability.reserve(1).is_err());
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
    }
}
