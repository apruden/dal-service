//! Outbound ZeroMQ transport (DESIGN §10.1): a `DEALER`-based [`Transport`] that
//! sends a request frame to a peer `ROUTER` and awaits the correlated reply.
//!
//! Correctness never rides this path — every multi-node test uses the
//! in-process switch (ground rule 3). ZeroMQ is best-effort: application-level
//! timeout plus idempotent retry (DESIGN §10.3) live in the client and Raft
//! above, so here a lost reply simply surfaces as a timeout `Err` and the caller
//! retries another candidate.
//!
//! Each destination gets exactly one long-lived DEALER socket, owned by a
//! dedicated I/O thread. A DEALER is fully asynchronous, so that one socket
//! carries an unbounded number of in-flight requests at once: replies are
//! matched back to their waiters by the `request_id` header field (which the
//! server echoes), not by serializing one request/reply per socket. This means
//! independent Raft groups sharing a peer never head-of-line-block each other,
//! and a single slow reply cannot stall the others. `call` hands a request to
//! the I/O thread over a channel, wakes it through an inproc `PAIR` signal
//! socket, and awaits a `oneshot`; the I/O thread enforces the request timeout.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::transport::Transport;
use crate::transport::codec::Envelope;
use crate::transport::codec::MsgType;
use crate::types::GroupId;

/// Bounds the burst of not-yet-dispatched requests held for one peer. The I/O
/// thread drains this almost immediately, so it fills only under extreme load;
/// a full queue surfaces as a retryable `WouldBlock` to the caller.
const OUTBOUND_QUEUE_DEPTH: usize = 1024;

/// How often an otherwise-idle I/O thread wakes to expire timed-out requests.
/// This bounds timeout granularity, not the timeout itself.
const SWEEP_INTERVAL: Duration = Duration::from_millis(50);

/// Distinguishes the inproc wake endpoint of each peer connection within a
/// shared ZeroMQ context (inproc addresses are context-scoped).
static WAKE_SEQ: AtomicU64 = AtomicU64::new(0);

fn io_err(kind: ErrorKind, msg: &str) -> Error {
    Error::Io(std::io::Error::new(kind, msg))
}

struct Inbound {
    frame: Vec<u8>,
    received_at: Option<Instant>,
}

type ReplyTx = oneshot::Sender<Result<Inbound>>;

struct Outbound {
    id: u64,
    msg_type: MsgType,
    group_id: GroupId,
    enqueued_at: Option<Instant>,
    frame: Vec<u8>,
    reply: ReplyTx,
}

/// One peer's connection: a channel to its I/O thread plus the sending half of
/// the inproc `PAIR` used to wake that thread when work is queued.
struct PeerConn {
    tx: mpsc::SyncSender<Outbound>,
    waker: Mutex<zmq::Socket>,
}

impl PeerConn {
    fn new(ctx: zmq::Context, addr: String, timeout: Duration) -> PeerConn {
        let wake_id = WAKE_SEQ.fetch_add(1, Ordering::Relaxed);
        let wake_addr = format!("inproc://dal-dealer-wake-{wake_id}");
        // Bind before connect (inproc requires it) so there is no startup race.
        let signal = ctx.socket(zmq::PAIR).expect("create wake PAIR");
        signal.bind(&wake_addr).expect("bind wake PAIR");
        let waker = ctx.socket(zmq::PAIR).expect("create wake sender");
        waker.connect(&wake_addr).expect("connect wake sender");

        let (tx, rx) = mpsc::sync_channel(OUTBOUND_QUEUE_DEPTH);
        std::thread::Builder::new()
            .name("zmq-dealer".into())
            .spawn(move || conn_loop(ctx, addr, timeout, signal, rx))
            .expect("spawn ZeroMQ DEALER connection");
        PeerConn {
            tx,
            waker: Mutex::new(waker),
        }
    }

    fn submit(&self, out: Outbound) -> Result<()> {
        self.tx.try_send(out).map_err(|e| match e {
            mpsc::TrySendError::Full(_) => {
                io_err(ErrorKind::WouldBlock, "ZeroMQ peer queue is full")
            }
            mpsc::TrySendError::Disconnected(_) => io_err(
                ErrorKind::ConnectionAborted,
                "ZeroMQ peer connection stopped",
            ),
        })?;
        // A single byte nudges the poll loop; a dropped nudge (HWM/DONTWAIT) is
        // harmless because the queued request is still drained on the next
        // sweep, and pending bytes persist to wake the loop regardless.
        let waker = self.waker.lock().unwrap();
        let _ = waker.send(&[1u8][..], zmq::DONTWAIT);
        Ok(())
    }
}

struct TransportInner {
    ctx: zmq::Context,
    timeout: Duration,
    next_id: AtomicU64,
    peers: Mutex<HashMap<String, Arc<PeerConn>>>,
}

impl TransportInner {
    fn peer(&self, addr: &str) -> Arc<PeerConn> {
        let mut peers = self.peers.lock().unwrap();
        peers
            .entry(addr.to_string())
            .or_insert_with(|| {
                Arc::new(PeerConn::new(
                    self.ctx.clone(),
                    addr.to_string(),
                    self.timeout,
                ))
            })
            .clone()
    }
}

/// A ZeroMQ outbound transport. One DEALER socket per peer, owned by an I/O
/// thread and multiplexed across all in-flight requests; callers share this
/// handle cheaply.
#[derive(Clone)]
pub struct ZmqTransport {
    inner: Arc<TransportInner>,
}

impl ZmqTransport {
    pub fn new(ctx: zmq::Context, timeout: Duration) -> ZmqTransport {
        ZmqTransport {
            inner: Arc::new(TransportInner {
                ctx,
                timeout,
                next_id: AtomicU64::new(1),
                peers: Mutex::new(HashMap::new()),
            }),
        }
    }
}

fn zmq_io(e: zmq::Error) -> Error {
    Error::Io(std::io::Error::other(format!("zmq: {e}")))
}

fn open_socket(ctx: &zmq::Context, addr: &str) -> Result<zmq::Socket> {
    let socket = ctx.socket(zmq::DEALER).map_err(zmq_io)?;
    socket.set_linger(0).map_err(zmq_io)?;
    socket.connect(addr).map_err(zmq_io)?;
    Ok(socket)
}

/// Owns the peer's socket and pending table. Sends are non-blocking; replies are
/// correlated by `request_id`; timed-out and reset requests fail their waiters
/// so the layer above retries. Exits when the transport (and thus the sending
/// half of `rx`) is dropped.
fn conn_loop(
    ctx: zmq::Context,
    addr: String,
    timeout: Duration,
    signal: zmq::Socket,
    rx: mpsc::Receiver<Outbound>,
) {
    let mut dealer = open_socket(&ctx, &addr).ok();
    let mut pending: HashMap<u64, (Instant, MsgType, GroupId, ReplyTx)> = HashMap::new();
    let poll_timeout = SWEEP_INTERVAL.as_millis() as i64;

    loop {
        if dealer.is_none() {
            dealer = open_socket(&ctx, &addr).ok();
        }

        // Block until a reply or a wake arrives, or the sweep interval elapses.
        {
            let mut items = Vec::with_capacity(2);
            if let Some(d) = dealer.as_ref() {
                items.push(d.as_poll_item(zmq::POLLIN));
            }
            items.push(signal.as_poll_item(zmq::POLLIN));
            let _ = zmq::poll(&mut items, poll_timeout);
        }

        // Clear wake bytes; the queue itself is the source of truth below.
        while signal.recv_bytes(zmq::DONTWAIT).is_ok() {}

        let mut socket_faulted = false;

        // Complete any replies waiting on the socket.
        if let Some(d) = dealer.as_ref() {
            loop {
                match d.recv_multipart(zmq::DONTWAIT) {
                    Ok(mut parts) => {
                        if let Some(frame) = parts.pop()
                            && let Some(id) = Envelope::peek_request_id(&frame)
                            && let Some((sent_at, msg_type, group_id, tx)) = pending.remove(&id)
                        {
                            if crate::perf::write_path_enabled()
                                && let Some(class) =
                                    crate::perf::transport_profile_class(msg_type, group_id)
                            {
                                let stage = class.stage(
                                    crate::perf::WriteStage::ClientDealerRoundTrip,
                                    crate::perf::WriteStage::RaftDealerRoundTrip,
                                );
                                crate::perf::record_duration(stage, sent_at.elapsed());
                            }
                            let received_at = crate::perf::write_path_enabled().then(Instant::now);
                            let _ = tx.send(Ok(Inbound { frame, received_at }));
                        }
                    }
                    Err(zmq::Error::EAGAIN) => break,
                    Err(_) => {
                        socket_faulted = true;
                        break;
                    }
                }
            }
        }

        // Dispatch queued requests onto the socket.
        let mut disconnected = false;
        loop {
            match rx.try_recv() {
                Ok(Outbound {
                    id,
                    msg_type,
                    group_id,
                    enqueued_at,
                    frame,
                    reply,
                }) => match dealer.as_ref() {
                    Some(d) if !socket_faulted => match d.send(frame, zmq::DONTWAIT) {
                        Ok(()) => {
                            if let Some(started) = enqueued_at
                                && let Some(class) =
                                    crate::perf::transport_profile_class(msg_type, group_id)
                            {
                                let stage = class.stage(
                                    crate::perf::WriteStage::ClientDealerQueueWait,
                                    crate::perf::WriteStage::RaftDealerQueueWait,
                                );
                                crate::perf::record_duration(stage, started.elapsed());
                            }
                            pending.insert(id, (Instant::now(), msg_type, group_id, reply));
                        }
                        Err(_) => {
                            socket_faulted = true;
                            let _ = reply.send(Err(io_err(
                                ErrorKind::ConnectionAborted,
                                "ZeroMQ send failed",
                            )));
                        }
                    },
                    _ => {
                        let _ = reply.send(Err(io_err(
                            ErrorKind::ConnectionAborted,
                            "no ZeroMQ DEALER socket",
                        )));
                    }
                },
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        // A faulted socket may have lost in-flight replies. Rebuild it and fail
        // everything outstanding so the layer above retries a fresh candidate.
        if socket_faulted {
            dealer = None;
            for (_, (_, _, _, tx)) in pending.drain() {
                let _ = tx.send(Err(io_err(
                    ErrorKind::ConnectionAborted,
                    "ZeroMQ peer connection reset",
                )));
            }
        }

        // Expire requests that have outlived the timeout.
        let now = Instant::now();
        let expired: Vec<u64> = pending
            .iter()
            .filter(|(_, (started, _, _, _))| now.duration_since(*started) >= timeout)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some((_, _, _, tx)) = pending.remove(&id) {
                let _ = tx.send(Err(io_err(ErrorKind::TimedOut, "ZeroMQ request timed out")));
            }
        }

        // The transport was dropped: fail any stragglers and stop.
        if disconnected {
            for (_, (_, _, _, tx)) in pending.drain() {
                let _ = tx.send(Err(io_err(
                    ErrorKind::ConnectionAborted,
                    "ZeroMQ transport shut down",
                )));
            }
            return;
        }
    }
}

impl Transport for ZmqTransport {
    async fn call(&self, addr: &str, request: Envelope) -> Result<Envelope> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let mut request = request;
        request.request_id = id;
        let msg_type = request.msg_type;
        let group_id = request.group_id;

        let (tx, rx) = oneshot::channel();
        self.inner.peer(addr).submit(Outbound {
            id,
            msg_type,
            group_id,
            enqueued_at: crate::perf::write_path_enabled().then(Instant::now),
            frame: request.encode().map_err(Error::codec)?,
            reply: tx,
        })?;
        let reply = rx.await.map_err(|_| {
            io_err(
                ErrorKind::ConnectionAborted,
                "ZeroMQ connection dropped before replying",
            )
        })??;

        if let Some(started) = reply.received_at
            && let Some(class) = crate::perf::transport_profile_class(msg_type, group_id)
        {
            let stage = class.stage(
                crate::perf::WriteStage::ClientDealerWaiterResume,
                crate::perf::WriteStage::RaftDealerWaiterResume,
            );
            crate::perf::record_duration(stage, started.elapsed());
        }

        Envelope::decode(&reply.frame).map_err(Error::codec)
    }
}

#[cfg(test)]
mod tests {
    use super::ZmqTransport;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn clone_shares_the_peer_connection_directory() {
        let transport = ZmqTransport::new(zmq::Context::new(), Duration::from_secs(1));
        let clone = transport.clone();
        assert!(Arc::ptr_eq(&transport.inner, &clone.inner));

        let independent = ZmqTransport::new(zmq::Context::new(), Duration::from_secs(1));
        assert!(!Arc::ptr_eq(&transport.inner, &independent.inner));
    }
}
