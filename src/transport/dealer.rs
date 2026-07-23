//! Outbound ZeroMQ transport (DESIGN §10.1): a `DEALER`-based [`Transport`] that
//! sends a request frame to a peer `ROUTER` and awaits the correlated reply.
//!
//! Correctness never rides this path — every multi-node test uses the
//! in-process switch (ground rule 3). ZeroMQ is best-effort: application-level
//! timeout plus idempotent retry (DESIGN §10.3) live in the client and Raft
//! above, so here a lost reply simply surfaces as a timeout `Err` and the caller
//! retries another candidate. Each destination gets a small, bounded pool of
//! long-lived sockets: repeatedly creating a socket and a blocking task for
//! every heartbeat or AppendEntries RPC is prohibitively expensive for a
//! multi-Raft node.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::transport::Transport;
use crate::transport::codec::Envelope;

/// A small pool avoids head-of-line blocking for independent Raft groups while
/// putting a hard bound on sockets, threads, and queued requests per peer.
const CONNECTIONS_PER_PEER: usize = 2;
const QUEUE_DEPTH_PER_CONNECTION: usize = 256;

struct Call {
    frame: Vec<u8>,
    reply: oneshot::Sender<Result<Vec<u8>>>,
}

struct PeerPool {
    workers: Vec<mpsc::SyncSender<Call>>,
    next: AtomicUsize,
}

impl PeerPool {
    fn new(ctx: zmq::Context, addr: String, timeout: Duration) -> PeerPool {
        let workers = (0..CONNECTIONS_PER_PEER)
            .map(|slot| {
                let (tx, rx) = mpsc::sync_channel(QUEUE_DEPTH_PER_CONNECTION);
                let worker_ctx = ctx.clone();
                let worker_addr = addr.clone();
                std::thread::Builder::new()
                    .name(format!("zmq-dealer-{slot}"))
                    .spawn(move || worker_loop(worker_ctx, worker_addr, timeout, rx))
                    .expect("spawn ZeroMQ DEALER worker");
                tx
            })
            .collect();
        PeerPool {
            workers,
            next: AtomicUsize::new(0),
        }
    }

    fn submit(&self, call: Call) -> Result<()> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        match self.workers[index].try_send(call) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(_)) => Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "ZeroMQ peer queue is full",
            ))),
            Err(mpsc::TrySendError::Disconnected(_)) => Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "ZeroMQ peer worker stopped",
            ))),
        }
    }
}

struct TransportInner {
    ctx: zmq::Context,
    timeout: Duration,
    peers: Mutex<HashMap<String, Arc<PeerPool>>>,
}

impl TransportInner {
    fn peer(&self, addr: &str) -> Arc<PeerPool> {
        let mut peers = self.peers.lock().unwrap();
        peers
            .entry(addr.to_string())
            .or_insert_with(|| {
                Arc::new(PeerPool::new(
                    self.ctx.clone(),
                    addr.to_string(),
                    self.timeout,
                ))
            })
            .clone()
    }
}

/// A ZeroMQ outbound transport. Sockets are owned by dedicated worker threads
/// and reused for requests to the same peer; callers share this handle cheaply.
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
                peers: Mutex::new(HashMap::new()),
            }),
        }
    }
}

fn zmq_io(e: zmq::Error) -> Error {
    Error::Io(std::io::Error::other(format!("zmq: {e}")))
}

fn open_socket(ctx: &zmq::Context, addr: &str, timeout: Duration) -> Result<zmq::Socket> {
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let socket = ctx.socket(zmq::DEALER).map_err(zmq_io)?;
    socket.set_linger(0).map_err(zmq_io)?;
    socket.set_rcvtimeo(timeout_ms).map_err(zmq_io)?;
    socket.set_sndtimeo(timeout_ms).map_err(zmq_io)?;
    socket.connect(addr).map_err(zmq_io)?;
    Ok(socket)
}

fn exchange(socket: &zmq::Socket, frame: &[u8]) -> Result<Vec<u8>> {
    socket.send(frame, 0).map_err(zmq_io)?;
    // DEALER→ROUTER→DEALER: the identity frame is stripped, so the reply is a
    // single payload frame.
    let mut parts = socket.recv_multipart(0).map_err(zmq_io)?;
    parts.pop().ok_or_else(|| {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "empty reply from peer",
        ))
    })
}

fn worker_loop(ctx: zmq::Context, addr: String, timeout: Duration, rx: mpsc::Receiver<Call>) {
    let mut socket = open_socket(&ctx, &addr, timeout).ok();
    while let Ok(call) = rx.recv() {
        let result = match socket.as_ref() {
            Some(socket) => exchange(socket, &call.frame),
            None => Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "failed to create ZeroMQ DEALER socket",
            ))),
        };
        // A socket that has timed out or otherwise failed may have an unknown
        // request/reply state. Replace it before accepting another request.
        if result.is_err() {
            socket = open_socket(&ctx, &addr, timeout).ok();
        }
        let _ = call.reply.send(result);
    }
}

impl Transport for ZmqTransport {
    async fn call(&self, addr: &str, request: Envelope) -> Result<Envelope> {
        let (tx, rx) = oneshot::channel();
        self.inner.peer(addr).submit(Call {
            frame: request.encode(),
            reply: tx,
        })?;
        let reply = rx.await.map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "ZeroMQ peer worker stopped before replying",
            ))
        })??;

        Envelope::decode(&reply).map_err(Error::codec)
    }
}
