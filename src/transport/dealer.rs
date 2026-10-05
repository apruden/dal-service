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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::transport::Transport;
use crate::transport::codec::Envelope;
use crate::transport::codec::{Lane, MsgType};
use crate::types::GroupId;

/// Bounds the burst of not-yet-dispatched requests held for one peer. The I/O
/// thread drains this almost immediately, so it fills only under extreme load;
/// a full queue surfaces as a retryable `WouldBlock` to the caller.
const OUTBOUND_QUEUE_DEPTH: usize = 1024;

/// Inbound reply queue depth for one peer socket. In-flight requests are
/// already bounded by `OUTBOUND_QUEUE_DEPTH` and the request timeout, so this
/// only needs to absorb a reply burst without letting a peer stream unbounded
/// assembled messages into memory.
const INBOUND_REPLY_HWM: i32 = 256;

/// Explicit send high-water-mark (DESIGN §10.3 "bounded send
/// high-water-marks") rather than libzmq's default. Matches the outbound
/// queue depth; a full send queue surfaces as a per-request retryable error,
/// not a socket fault.
const OUTBOUND_SEND_HWM: i32 = OUTBOUND_QUEUE_DEPTH as i32;

/// How often an otherwise-idle I/O thread wakes to expire timed-out requests.
/// This bounds timeout granularity, not the timeout itself.
const SWEEP_INTERVAL: Duration = Duration::from_millis(50);

/// How long a peer connection may sit with no calls and no in-flight requests
/// before it is evicted. Each `PeerConn` owns an OS thread and sockets that
/// reconnect-dial forever, so without eviction a replaced node's old address
/// strands a dialing thread for the life of the process. Conservative on
/// purpose: well past any retry/backoff cadence, so a live peer is never
/// churned. A module constant, not a config surface.
const PEER_IDLE_TTL: Duration = Duration::from_secs(300);

/// Replies whose frame failed to fully decode. Such a frame no longer
/// completes (and thus consumes) the pending waiter — the genuine reply may
/// still be in flight behind it.
static REPLY_DECODE_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Distinguishes the inproc wake endpoint of each peer connection within a
/// shared ZeroMQ context (inproc addresses are context-scoped).
static WAKE_SEQ: AtomicU64 = AtomicU64::new(0);

fn io_err(kind: ErrorKind, msg: &str) -> Error {
    Error::Io(std::io::Error::new(kind, msg))
}

struct Inbound {
    env: Envelope,
    received_at: Option<Instant>,
}

type ReplyTx = oneshot::Sender<Result<Inbound>>;

struct Outbound {
    id: u64,
    msg_type: MsgType,
    group_id: GroupId,
    enqueued_at: Option<Instant>,
    /// Per-request deadline: the transport default, or a per-call override
    /// (openraft's `hard_ttl` for Raft RPCs — snapshot chunks legitimately
    /// outlive a control-lane round-trip).
    timeout: Duration,
    frame: Vec<u8>,
    reply: ReplyTx,
}

/// A dispatched request awaiting its reply on the I/O thread.
struct PendingCall {
    sent_at: Instant,
    timeout: Duration,
    msg_type: MsgType,
    group_id: GroupId,
    tx: ReplyTx,
}

/// One peer's connection: a channel to its I/O thread plus the sending half of
/// the inproc `PAIR` used to wake that thread when work is queued.
struct PeerConn {
    tx: mpsc::SyncSender<Outbound>,
    waker: Mutex<zmq::Socket>,
    /// Calls currently borrowing this connection, held via [`InflightGuard`]
    /// across the whole request/reply exchange. A connection is never evicted
    /// while this is non-zero.
    inflight: AtomicUsize,
    /// Last checkout time. Written only under the peers-map lock.
    last_used: Mutex<Instant>,
}

impl PeerConn {
    /// Fallible because every step here consumes a process-wide resource (file
    /// descriptors, threads). Exhausting those is a transient operational
    /// condition the caller can retry, not a reason to abort the process.
    fn new(ctx: zmq::Context, addr: String, lane: Lane) -> Result<PeerConn> {
        let wake_id = WAKE_SEQ.fetch_add(1, Ordering::Relaxed);
        let wake_addr = format!("inproc://dal-dealer-wake-{wake_id}");
        // Bind before connect (inproc requires it) so there is no startup race.
        let signal = ctx.socket(zmq::PAIR).map_err(zmq_io)?;
        signal.bind(&wake_addr).map_err(zmq_io)?;
        let waker = ctx.socket(zmq::PAIR).map_err(zmq_io)?;
        waker.connect(&wake_addr).map_err(zmq_io)?;

        let (tx, rx) = mpsc::sync_channel(OUTBOUND_QUEUE_DEPTH);
        std::thread::Builder::new()
            .name("zmq-dealer".into())
            .spawn(move || conn_loop(ctx, addr, lane, signal, rx))
            .map_err(Error::Io)?;
        Ok(PeerConn {
            tx,
            waker: Mutex::new(waker),
            inflight: AtomicUsize::new(0),
            last_used: Mutex::new(Instant::now()),
        })
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

/// Borrows a peer connection for one whole request/reply exchange. Eviction
/// only considers connections with no outstanding guard, so a call in flight
/// can never have its I/O thread closed out from under it.
struct InflightGuard {
    conn: Arc<PeerConn>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.conn.inflight.fetch_sub(1, Ordering::Release);
    }
}

struct TransportInner {
    ctx: zmq::Context,
    timeout: Duration,
    lane: Lane,
    next_id: AtomicU64,
    peers: Mutex<HashMap<String, Arc<PeerConn>>>,
}

impl TransportInner {
    /// Check out the connection for `addr`, creating it if absent, and sweep
    /// connections that have gone idle past [`PEER_IDLE_TTL`].
    ///
    /// Both the `inflight` increment and the eviction test happen under the
    /// peers lock, so a connection observed at zero here cannot pick up a new
    /// borrow concurrently. Dropping the map's `Arc` closes the outbound
    /// channel; the I/O thread observes the disconnect on its next sweep,
    /// fails any stragglers, and exits — which is the point, since otherwise a
    /// replaced node's address strands a reconnect-dialing thread forever.
    fn peer(&self, addr: &str) -> Result<InflightGuard> {
        let mut peers = self.peers.lock().unwrap();
        let now = Instant::now();
        peers.retain(|peer_addr, conn| {
            peer_addr == addr
                || conn.inflight.load(Ordering::Acquire) > 0
                || now.duration_since(*conn.last_used.lock().unwrap()) < PEER_IDLE_TTL
        });
        let conn = match peers.get(addr) {
            Some(existing) => existing.clone(),
            None => {
                let conn = Arc::new(PeerConn::new(
                    self.ctx.clone(),
                    addr.to_string(),
                    self.lane,
                )?);
                peers.insert(addr.to_string(), conn.clone());
                conn
            }
        };
        *conn.last_used.lock().unwrap() = now;
        conn.inflight.fetch_add(1, Ordering::Release);
        Ok(InflightGuard { conn })
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
    /// `lane` scopes the socket-level frame cap: transports are lane-pure
    /// (the runtime constructs separate control and bulk instances), so a
    /// control transport must not accept bulk-sized replies during assembly.
    pub fn new(ctx: zmq::Context, timeout: Duration, lane: Lane) -> ZmqTransport {
        ZmqTransport {
            inner: Arc::new(TransportInner {
                ctx,
                timeout,
                lane,
                next_id: AtomicU64::new(1),
                peers: Mutex::new(HashMap::new()),
            }),
        }
    }
}

fn zmq_io(e: zmq::Error) -> Error {
    Error::Io(std::io::Error::other(format!("zmq: {e}")))
}

fn open_socket(ctx: &zmq::Context, addr: &str, lane: Lane) -> Result<zmq::Socket> {
    let socket = ctx.socket(zmq::DEALER).map_err(zmq_io)?;
    socket.set_linger(0).map_err(zmq_io)?;
    // A peer's reply is remote input too: cap it during assembly rather than
    // trusting the far side to respect the reply budget (DESIGN §10.3).
    socket
        .set_maxmsgsize(lane.max_frame_bytes() as i64)
        .map_err(zmq_io)?;
    socket.set_rcvhwm(INBOUND_REPLY_HWM).map_err(zmq_io)?;
    socket.set_sndhwm(OUTBOUND_SEND_HWM).map_err(zmq_io)?;
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
    lane: Lane,
    signal: zmq::Socket,
    rx: mpsc::Receiver<Outbound>,
) {
    let mut dealer = open_socket(&ctx, &addr, lane).ok();
    let mut pending: HashMap<u64, PendingCall> = HashMap::new();
    let poll_timeout = SWEEP_INTERVAL.as_millis() as i64;

    loop {
        if dealer.is_none() {
            dealer = open_socket(&ctx, &addr, lane).ok();
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
                        let Some(frame) = parts.pop() else { continue };
                        // Decode here rather than peeking the correlation id:
                        // the id lives in the same bytes the decoder validates,
                        // so a frame that will not decode is a frame that
                        // cannot be attributed to a waiter. Drop it and leave
                        // the waiter pending — the genuine reply may still be
                        // in flight behind this one, and if it never comes the
                        // per-call timeout below fails the waiter anyway.
                        let env = match Envelope::decode(&frame) {
                            Ok(env) => env,
                            Err(_) => {
                                REPLY_DECODE_FAILURES.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        };
                        if let Some(call) = pending.remove(&env.request_id) {
                            if crate::perf::write_path_enabled()
                                && let Some(class) = crate::perf::transport_profile_class(
                                    call.msg_type,
                                    call.group_id,
                                )
                            {
                                let stage = class.stage(
                                    crate::perf::WriteStage::ClientDealerRoundTrip,
                                    crate::perf::WriteStage::RaftDealerRoundTrip,
                                );
                                crate::perf::record_duration(stage, call.sent_at.elapsed());
                            }
                            let received_at = crate::perf::write_path_enabled().then(Instant::now);
                            let _ = call.tx.send(Ok(Inbound { env, received_at }));
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
                    timeout,
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
                            pending.insert(
                                id,
                                PendingCall {
                                    sent_at: Instant::now(),
                                    timeout,
                                    msg_type,
                                    group_id,
                                    tx: reply,
                                },
                            );
                        }
                        // A full send queue (SNDHWM) is the normal slow-peer
                        // signal (DESIGN §10.3), not a socket fault: fail only
                        // this request so the caller retries, and keep the
                        // socket — in-flight replies remain deliverable.
                        Err(zmq::Error::EAGAIN) => {
                            let _ = reply.send(Err(io_err(
                                ErrorKind::WouldBlock,
                                "ZeroMQ peer send queue is full",
                            )));
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
            for (_, call) in pending.drain() {
                let _ = call.tx.send(Err(io_err(
                    ErrorKind::ConnectionAborted,
                    "ZeroMQ peer connection reset",
                )));
            }
        }

        // Expire requests that have outlived their own deadline. The bound is
        // per-call, not per-connection: a snapshot chunk carrying openraft's
        // `hard_ttl` shares this socket with control RPCs that expire far
        // sooner, and neither may be judged by the other's clock.
        let now = Instant::now();
        let expired: Vec<u64> = pending
            .iter()
            .filter(|(_, call)| now.duration_since(call.sent_at) >= call.timeout)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(call) = pending.remove(&id) {
                let _ = call
                    .tx
                    .send(Err(io_err(ErrorKind::TimedOut, "ZeroMQ request timed out")));
            }
        }

        // The transport was dropped: fail any stragglers and stop.
        if disconnected {
            for (_, call) in pending.drain() {
                let _ = call.tx.send(Err(io_err(
                    ErrorKind::ConnectionAborted,
                    "ZeroMQ transport shut down",
                )));
            }
            return;
        }
    }
}

impl ZmqTransport {
    async fn exchange(
        &self,
        addr: &str,
        request: Envelope,
        timeout: Option<Duration>,
    ) -> Result<Envelope> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let mut request = request;
        request.request_id = id;
        let msg_type = request.msg_type;
        let group_id = request.group_id;

        let (tx, rx) = oneshot::channel();
        // The guard is held for the whole exchange, so the connection cannot
        // be evicted from under this call.
        let guard = self.inner.peer(addr)?;
        guard.conn.submit(Outbound {
            id,
            msg_type,
            group_id,
            enqueued_at: crate::perf::write_path_enabled().then(Instant::now),
            timeout: timeout.unwrap_or(self.inner.timeout),
            frame: request.encode().map_err(Error::codec)?,
            reply: tx,
        })?;
        let reply = rx.await.map_err(|_| {
            io_err(
                ErrorKind::ConnectionAborted,
                "ZeroMQ connection dropped before replying",
            )
        })??;
        drop(guard);

        if let Some(started) = reply.received_at
            && let Some(class) = crate::perf::transport_profile_class(msg_type, group_id)
        {
            let stage = class.stage(
                crate::perf::WriteStage::ClientDealerWaiterResume,
                crate::perf::WriteStage::RaftDealerWaiterResume,
            );
            crate::perf::record_duration(stage, started.elapsed());
        }

        // Already decoded and validated on the I/O thread.
        Ok(reply.env)
    }
}

impl Transport for ZmqTransport {
    async fn call(&self, addr: &str, request: Envelope) -> Result<Envelope> {
        self.exchange(addr, request, None).await
    }

    async fn call_with_timeout(
        &self,
        addr: &str,
        request: Envelope,
        timeout: Option<Duration>,
    ) -> Result<Envelope> {
        self.exchange(addr, request, timeout).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Correctness never rides this transport (see the module header), so the
    /// multi-node suites exercise the in-process switch instead. These cover
    /// the logic that only exists here.
    #[test]
    fn clone_shares_the_peer_connection_directory() {
        let transport =
            ZmqTransport::new(zmq::Context::new(), Duration::from_secs(1), Lane::Control);
        let clone = transport.clone();
        assert!(Arc::ptr_eq(&transport.inner, &clone.inner));

        let independent =
            ZmqTransport::new(zmq::Context::new(), Duration::from_secs(1), Lane::Control);
        assert!(!Arc::ptr_eq(&transport.inner, &independent.inner));
    }

    /// A replaced node's address would otherwise strand a reconnect-dialing
    /// thread for the life of the process — but a connection carrying a call
    /// must never be pulled out from under it.
    #[test]
    fn idle_peers_are_evicted_while_in_flight_ones_are_kept() {
        let transport =
            ZmqTransport::new(zmq::Context::new(), Duration::from_secs(1), Lane::Control);
        let idle = "inproc://dal-dealer-evict-idle";
        let busy = "inproc://dal-dealer-evict-busy";

        drop(transport.inner.peer(idle).unwrap());
        let busy_guard = transport.inner.peer(busy).unwrap();

        // Backdate both past the TTL. `checked_sub` because a monotonic clock
        // shortly after boot may not reach back that far.
        let Some(stale) = Instant::now().checked_sub(PEER_IDLE_TTL + Duration::from_secs(1)) else {
            return;
        };
        {
            let peers = transport.inner.peers.lock().unwrap();
            for conn in peers.values() {
                *conn.last_used.lock().unwrap() = stale;
            }
        }

        // Checking out an unrelated peer runs the sweep.
        let _other = transport
            .inner
            .peer("inproc://dal-dealer-evict-other")
            .unwrap();
        let peers = transport.inner.peers.lock().unwrap();
        assert!(
            !peers.contains_key(idle),
            "an idle connection past the TTL must be evicted"
        );
        assert!(
            peers.contains_key(busy),
            "a connection with a call in flight must never be evicted"
        );
        drop(peers);
        drop(busy_guard);
    }

    /// The motivating case for per-call deadlines: a snapshot chunk carrying
    /// openraft's `hard_ttl` shares a socket with control RPCs, so neither may
    /// be judged by the transport-wide default.
    #[tokio::test]
    async fn a_per_call_timeout_overrides_the_transport_default() {
        let ctx = zmq::Context::new();
        let addr = "inproc://dal-dealer-per-call-timeout";
        // A peer that accepts the connection but never replies.
        let router = ctx.socket(zmq::ROUTER).unwrap();
        router.bind(addr).unwrap();

        let transport = ZmqTransport::new(ctx.clone(), Duration::from_secs(30), Lane::Control);
        let request = Envelope::new(7, MsgType::ClientOp, GroupId::Data(0), 0, b"ping".to_vec());
        let started = Instant::now();
        let error = transport
            .call_with_timeout(addr, request, Some(Duration::from_millis(200)))
            .await
            .unwrap_err();

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the 200ms per-call deadline must win over the 30s transport default"
        );
        assert!(
            error.to_string().contains("timed out"),
            "unexpected error: {error}"
        );
    }

    /// The reply is decoded on the I/O thread and correlated by the id the
    /// transport assigned, not the one the caller set.
    #[tokio::test]
    async fn a_reply_is_decoded_and_correlated_on_the_io_thread() {
        let ctx = zmq::Context::new();
        let addr = "inproc://dal-dealer-round-trip";
        let router = ctx.socket(zmq::ROUTER).unwrap();
        router.bind(addr).unwrap();

        let echo = std::thread::spawn(move || {
            let parts = router.recv_multipart(0).unwrap();
            let request = Envelope::decode(&parts[1]).unwrap();
            let reply = Envelope::new(
                request.cluster_id,
                request.msg_type,
                request.group_id,
                request.request_id,
                b"pong".to_vec(),
            );
            router
                .send_multipart([parts[0].clone(), reply.encode().unwrap()], 0)
                .unwrap();
        });

        let transport = ZmqTransport::new(ctx.clone(), Duration::from_secs(10), Lane::Control);
        // The caller's request_id is deliberately bogus; the transport assigns
        // its own and the reply must still correlate.
        let request = Envelope::new(
            7,
            MsgType::ClientOp,
            GroupId::Data(0),
            999,
            b"ping".to_vec(),
        );
        let reply = transport.call(addr, request).await.unwrap();

        assert_eq!(reply.payload, b"pong");
        echo.join().unwrap();
    }
}
