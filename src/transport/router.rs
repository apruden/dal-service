//! Inbound ZeroMQ transport (DESIGN §10.1): a `ROUTER` socket on one owning
//! poller thread, bridged to the async [`Server`] via the Tokio runtime.
//!
//! ZMQ sockets are not `Send`, so the socket lives on exactly one thread
//! (DESIGN §11). That thread receives `[identity, frame]`, hands the frame to an
//! async `serve` task on the runtime, and writes back `[identity, reply]` when
//! the task completes. A slow handler cannot block the poller because replies
//! are drained from a channel, not awaited inline.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::runtime::Handle;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::error::{Error, Result};
use crate::transport::Server;
use crate::transport::codec::Envelope;
use crate::transport::codec::MsgType;

const MAX_HANDLERS: usize = 1024;
const MAX_CLIENT_HANDLERS: usize = 768;
/// Ceiling on concurrently reserved reply bytes. A client op reserves its worst
/// case (a 16 MiB value read), so this budget — not `MAX_CLIENT_HANDLERS` — is
/// what bounds concurrent client ops, at `REPLY_BUDGET_MIB / 17`. Raise it to
/// trade resident reply memory for client concurrency.
const REPLY_BUDGET_MIB: usize = 512;
const MIB: usize = 1024 * 1024;

struct RouterLimits {
    total_handlers: usize,
    client_handlers: usize,
    reply_budget_mib: usize,
}

impl RouterLimits {
    fn from_env() -> Result<Self> {
        let total_handlers = env_limit("DAL_ROUTER_MAX_HANDLERS", MAX_HANDLERS)?;
        let client_handlers = env_limit("DAL_ROUTER_MAX_CLIENT_HANDLERS", MAX_CLIENT_HANDLERS)?;
        let reply_budget_mib = env_limit("DAL_ROUTER_REPLY_BUDGET_MIB", REPLY_BUDGET_MIB)?;
        if client_handlers >= total_handlers {
            return Err(Error::Config(
                "DAL_ROUTER_MAX_CLIENT_HANDLERS must be less than DAL_ROUTER_MAX_HANDLERS".into(),
            ));
        }
        Ok(Self {
            total_handlers,
            client_handlers,
            reply_budget_mib,
        })
    }
}

static ADMISSION_REJECTIONS: AtomicU64 = AtomicU64::new(0);
static REPLY_SENDS: AtomicU64 = AtomicU64::new(0);
static REPLY_SEND_EAGAIN: AtomicU64 = AtomicU64::new(0);
static REPLY_SEND_FAILURES: AtomicU64 = AtomicU64::new(0);
static ACTIVE_HANDLER_MAX: AtomicUsize = AtomicUsize::new(0);

/// Process-wide ROUTER scheduling counters, suitable for benchmark reporting.
#[derive(Debug, Clone, Copy, Default)]
pub struct RouterCounters {
    pub admission_rejections: u64,
    pub reply_sends: u64,
    pub reply_send_eagain: u64,
    pub reply_send_failures: u64,
    pub max_active_handlers: usize,
}

pub fn counters() -> RouterCounters {
    RouterCounters {
        admission_rejections: ADMISSION_REJECTIONS.load(Ordering::Relaxed),
        reply_sends: REPLY_SENDS.load(Ordering::Relaxed),
        reply_send_eagain: REPLY_SEND_EAGAIN.load(Ordering::Relaxed),
        reply_send_failures: REPLY_SEND_FAILURES.load(Ordering::Relaxed),
        max_active_handlers: ACTIVE_HANDLER_MAX.load(Ordering::Relaxed),
    }
}

struct PendingReply {
    identity: Vec<u8>,
    bytes: Vec<u8>,
    queued_at: Option<Instant>,
    queue_stage: Option<crate::perf::WriteStage>,
    send_stage: Option<crate::perf::WriteStage>,
    /// Absent for a shed request, which never held admission.
    _permit: Option<HeldAdmission>,
}

#[derive(Clone)]
struct Admission {
    total: Arc<Semaphore>,
    clients: Arc<Semaphore>,
    reply_bytes: Arc<Semaphore>,
}

struct HeldAdmission {
    _total: OwnedSemaphorePermit,
    _client: Option<OwnedSemaphorePermit>,
    _reply_bytes: OwnedSemaphorePermit,
}

impl Admission {
    fn try_acquire(&self, peer: bool, max_reply_bytes: usize) -> Option<HeldAdmission> {
        let total = self.total.clone().try_acquire_owned().ok()?;
        let mib = max_reply_bytes.div_ceil(MIB).max(1) as u32;
        let reply_bytes = self.reply_bytes.clone().try_acquire_many_owned(mib).ok()?;
        if peer {
            Some(HeldAdmission {
                _total: total,
                _client: None,
                _reply_bytes: reply_bytes,
            })
        } else {
            let client = self.clients.clone().try_acquire_owned().ok()?;
            Some(HeldAdmission {
                _total: total,
                _client: Some(client),
                _reply_bytes: reply_bytes,
            })
        }
    }
}

fn max_reply_bytes(msg_type: MsgType) -> usize {
    // Request and reply limits differ materially for Raft replication: append
    // requests can be large batches, but their acknowledgements are tiny.
    // Reserve the reply envelope's actual maximum shape, not the request's.
    match msg_type {
        MsgType::ClientOp => MsgType::ClientOp.max_payload() + 36,
        MsgType::DirectoryQuery => MsgType::DirectoryQuery.max_payload() + 36,
        MsgType::Redirect => MsgType::Redirect.max_payload() + 36,
        _ => 4 * 1024 + 36,
    }
}

/// A bound inbound endpoint. Dropping it stops the poller thread and closes the
/// socket.
pub struct ZmqServer {
    running: Arc<AtomicBool>,
    poller: Option<JoinHandle<()>>,
    active: Arc<ActiveHandlers>,
}

struct ActiveHandlers {
    count: std::sync::atomic::AtomicUsize,
    drained: Notify,
}

struct ActiveRequest(Arc<ActiveHandlers>);

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.drained.notify_waiters();
        }
    }
}

fn zmq_io(e: zmq::Error) -> Error {
    Error::Io(std::io::Error::other(format!("zmq: {e}")))
}

fn env_limit(name: &str, default: usize) -> Result<usize> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .ok()
            .filter(|&n| n > 0)
            .ok_or_else(|| Error::Config(format!("{name} must be a positive integer"))),
        Err(_) => Ok(default),
    }
}

impl ZmqServer {
    /// Bind `addr` on a `ROUTER` socket and serve inbound frames with `server`,
    /// spawning handler futures on the current Tokio runtime. Must be called
    /// from within a Tokio runtime (`Handle::current`).
    pub fn bind<S>(ctx: zmq::Context, addr: &str, server: Arc<S>) -> Result<ZmqServer>
    where
        S: Server + 'static,
    {
        let limits = RouterLimits::from_env()?;
        let socket = ctx.socket(zmq::ROUTER).map_err(zmq_io)?;
        socket.set_linger(0).map_err(zmq_io)?;
        socket.set_rcvtimeo(1).map_err(zmq_io)?;
        socket.bind(addr).map_err(zmq_io)?;

        let handle = Handle::current();
        let running = Arc::new(AtomicBool::new(true));
        let running_thread = running.clone();
        let active = Arc::new(ActiveHandlers {
            count: std::sync::atomic::AtomicUsize::new(0),
            drained: Notify::new(),
        });
        let active_thread = active.clone();
        let admission = Admission {
            total: Arc::new(Semaphore::new(limits.total_handlers)),
            clients: Arc::new(Semaphore::new(limits.client_handlers)),
            reply_bytes: Arc::new(Semaphore::new(limits.reply_budget_mib)),
        };
        let (reply_tx, reply_rx) = mpsc::sync_channel::<PendingReply>(limits.total_handlers);

        let poller = std::thread::Builder::new()
            .name(format!("zmq-router-{addr}"))
            .spawn(move || {
                Self::poll_loop(
                    socket,
                    server,
                    handle,
                    running_thread,
                    active_thread,
                    reply_tx,
                    reply_rx,
                    admission,
                )
            })
            .map_err(Error::Io)?;

        Ok(ZmqServer {
            running,
            poller: Some(poller),
            active,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn poll_loop<S>(
        socket: zmq::Socket,
        server: Arc<S>,
        handle: Handle,
        running: Arc<AtomicBool>,
        active: Arc<ActiveHandlers>,
        reply_tx: mpsc::SyncSender<PendingReply>,
        reply_rx: mpsc::Receiver<PendingReply>,
        admission: Admission,
    ) where
        S: Server + 'static,
    {
        while running.load(Ordering::Relaxed) {
            Self::flush_replies(&socket, &reply_rx);
            if let Ok(parts) = socket.recv_multipart(0) {
                Self::dispatch(parts, &server, &handle, &active, &reply_tx, &admission);
            }
        }
    }

    fn dispatch<S>(
        mut parts: Vec<Vec<u8>>,
        server: &Arc<S>,
        handle: &Handle,
        active: &Arc<ActiveHandlers>,
        reply_tx: &mpsc::SyncSender<PendingReply>,
        admission: &Admission,
    ) where
        S: Server + 'static,
    {
        let received_at = crate::perf::write_path_enabled().then(Instant::now);
        if parts.len() < 2 {
            return;
        }
        // ROUTER delivers [identity, payload]; extra frames are ignored (we
        // frame one payload per message).
        let payload = parts.pop().expect("checked multipart length");
        let identity = parts.remove(0);
        let Ok(env) = Envelope::decode(&payload) else {
            return;
        };
        let max_reply_bytes = max_reply_bytes(env.msg_type);
        let Some(permit) = admission.try_acquire(env.msg_type.is_peer_control(), max_reply_bytes)
        else {
            ADMISSION_REJECTIONS.fetch_add(1, Ordering::Relaxed);
            // Shed loudly: an empty-bodied reply carries no outcome, so the
            // caller treats it as "this node is not participating" and moves to
            // another candidate immediately. Dropping the frame instead would
            // cost the caller a full request timeout per shed request.
            let shed = Envelope::new(
                env.cluster_id,
                env.msg_type,
                env.group_id,
                env.request_id,
                Vec::new(),
            );
            let _ = reply_tx.try_send(PendingReply {
                identity,
                bytes: shed.encode(),
                queued_at: None,
                queue_stage: None,
                send_stage: None,
                _permit: None,
            });
            return;
        };
        let profile_class = crate::perf::write_path_enabled()
            .then(|| crate::perf::transport_profile_class(env.msg_type, env.group_id))
            .flatten();
        let queue_stage = profile_class.map(|class| {
            class.stage(
                crate::perf::WriteStage::ClientReplyQueueWait,
                crate::perf::WriteStage::RaftReplyQueueWait,
            )
        });
        let schedule_stage = profile_class.map(|class| {
            class.stage(
                crate::perf::WriteStage::ClientRouterSchedule,
                crate::perf::WriteStage::RaftRouterSchedule,
            )
        });
        let enqueue_stage = profile_class.map(|class| {
            class.stage(
                crate::perf::WriteStage::ClientReplyEnqueue,
                crate::perf::WriteStage::RaftReplyEnqueue,
            )
        });
        let handler_stage = profile_class.map(|class| {
            class.stage(
                crate::perf::WriteStage::ClientRouterHandler,
                crate::perf::WriteStage::RaftRouterHandler,
            )
        });
        let send_stage = profile_class.map(|class| {
            class.stage(
                crate::perf::WriteStage::ClientRouterSend,
                crate::perf::WriteStage::RaftRouterSend,
            )
        });
        let server = server.clone();
        let tx = reply_tx.clone();
        let active_now = active.count.fetch_add(1, Ordering::Release) + 1;
        ACTIVE_HANDLER_MAX.fetch_max(active_now, Ordering::Relaxed);
        let active = ActiveRequest(active.clone());
        handle.spawn(async move {
            let _active = active;
            if let (Some(stage), Some(started)) = (schedule_stage, received_at) {
                crate::perf::record_duration(stage, started.elapsed());
            }
            let handler_started = handler_stage.map(|_| Instant::now());
            let reply = server.serve(env).await;
            if let (Some(stage), Some(started)) = (handler_stage, handler_started) {
                crate::perf::record_duration(stage, started.elapsed());
            }
            let reply_completed_at = crate::perf::write_path_enabled().then(Instant::now);
            let bytes = reply.encode();
            let queued_at = crate::perf::write_path_enabled().then(Instant::now);
            if tx
                .try_send(PendingReply {
                    identity,
                    bytes,
                    queued_at,
                    queue_stage,
                    send_stage,
                    _permit: Some(permit),
                })
                .is_ok()
                && let (Some(stage), Some(started)) = (enqueue_stage, reply_completed_at)
            {
                crate::perf::record_duration(stage, started.elapsed());
            }
        });
    }

    fn flush_replies(socket: &zmq::Socket, reply_rx: &mpsc::Receiver<PendingReply>) {
        while let Ok(reply) = reply_rx.try_recv() {
            if let (Some(stage), Some(started)) = (reply.queue_stage, reply.queued_at) {
                crate::perf::record_duration(stage, started.elapsed());
            }
            let send_started = reply.send_stage.map(|_| Instant::now());
            // A disconnected or backpressured peer must never pin the sole
            // socket-owner thread during shutdown.
            match socket.send_multipart([reply.identity, reply.bytes], zmq::DONTWAIT) {
                Ok(()) => {
                    REPLY_SENDS.fetch_add(1, Ordering::Relaxed);
                }
                Err(zmq::Error::EAGAIN) => {
                    REPLY_SEND_EAGAIN.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    REPLY_SEND_FAILURES.fetch_add(1, Ordering::Relaxed);
                }
            }
            if let (Some(stage), Some(started)) = (reply.send_stage, send_started) {
                crate::perf::record_duration(stage, started.elapsed());
            }
        }
    }

    /// Stop accepting new requests and join the socket-owner thread. Existing
    /// async handlers continue running; callers may now shut down the services
    /// those handlers are waiting on before draining them with [`Self::shutdown`].
    pub fn stop_accepting(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.poller.take() {
            let _ = h.join();
        }
    }

    /// Stop accepting and asynchronously wait for every already-dispatched
    /// handler to exit. This must yield rather than block the Tokio worker: a
    /// handler may itself need that worker to observe Raft shutdown or an I/O
    /// timeout before it can release its active-request guard.
    pub async fn shutdown(mut self) {
        self.stop_accepting();
        self.wait_for_handlers().await;
    }

    async fn wait_for_handlers(&self) {
        loop {
            // Subscribe before checking the count so a completion between the
            // two operations leaves a notification permit for this waiter.
            let notified = self.active.drained.notified();
            if self.active.count.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for ZmqServer {
    fn drop(&mut self) {
        // Destructors cannot await. Stop accepting and join the socket owner,
        // but never synchronously wait for Tokio tasks that may need the
        // current runtime worker to make progress.
        self.stop_accepting();
    }
}

/// A short delay helper for callers waiting on a freshly bound endpoint. ZMQ
/// `inproc`/`tcp` connect is asynchronous; a brief pause lets the bind settle in
/// tests before the first request.
pub fn settle() {
    std::thread::sleep(Duration::from_millis(50));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission(total: usize, clients: usize, reply_mib: usize) -> Admission {
        Admission {
            total: Arc::new(Semaphore::new(total)),
            clients: Arc::new(Semaphore::new(clients)),
            reply_bytes: Arc::new(Semaphore::new(reply_mib)),
        }
    }

    #[test]
    fn peer_control_uses_capacity_reserved_from_clients() {
        let admission = admission(2, 1, 8);
        let client = admission
            .try_acquire(false, 1)
            .expect("first client admitted");
        assert!(admission.try_acquire(false, 1).is_none());

        let peer = admission
            .try_acquire(true, 1)
            .expect("peer control must use reserved capacity");
        assert!(admission.try_acquire(true, 1).is_none());
        drop(peer);
        drop(client);
        assert!(admission.try_acquire(false, 1).is_some());
    }

    #[test]
    fn reply_byte_reservation_is_held_until_admission_drops() {
        let admission = admission(2, 2, 2);
        let held = admission
            .try_acquire(false, 2 * MIB)
            .expect("two-MiB reservation admitted");
        assert!(admission.try_acquire(false, 1).is_none());
        drop(held);
        assert!(admission.try_acquire(false, 1).is_some());
    }
}
