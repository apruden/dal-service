//! Inbound ZeroMQ transport (DESIGN §10.1): a `ROUTER` socket on one owning
//! poller thread, bridged to the async [`Server`] via the Tokio runtime.
//!
//! ZMQ sockets are not `Send`, so the socket lives on exactly one thread
//! (DESIGN §11). That thread receives `[identity, frame]`, hands the frame to an
//! async `serve` task on the runtime, and writes back `[identity, reply]` when
//! the task completes. A slow handler cannot block the poller because replies
//! are drained from a channel, not awaited inline.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::runtime::Handle;

use crate::error::{Error, Result};
use crate::transport::Server;
use crate::transport::codec::Envelope;

/// A bound inbound endpoint. Dropping it stops the poller thread and closes the
/// socket.
pub struct ZmqServer {
    running: Arc<AtomicBool>,
    poller: Option<JoinHandle<()>>,
}

fn zmq_io(e: zmq::Error) -> Error {
    Error::Io(std::io::Error::other(format!("zmq: {e}")))
}

impl ZmqServer {
    /// Bind `addr` on a `ROUTER` socket and serve inbound frames with `server`,
    /// spawning handler futures on the current Tokio runtime. Must be called
    /// from within a Tokio runtime (`Handle::current`).
    pub fn bind<S>(ctx: zmq::Context, addr: &str, server: Arc<S>) -> Result<ZmqServer>
    where
        S: Server + 'static,
    {
        let socket = ctx.socket(zmq::ROUTER).map_err(zmq_io)?;
        socket.set_linger(0).map_err(zmq_io)?;
        // The poller owns the socket, while handlers complete on Tokio. Bound
        // the wait so a completed reply is flushed promptly even when no new
        // request arrives. One millisecond avoids the former 50 ms tail while
        // keeping this single-owner design simple and portable.
        socket.set_rcvtimeo(1).map_err(zmq_io)?;
        socket.bind(addr).map_err(zmq_io)?;

        let handle = Handle::current();
        let running = Arc::new(AtomicBool::new(true));
        let running_thread = running.clone();
        let (reply_tx, reply_rx) = mpsc::channel::<(Vec<u8>, Vec<u8>)>();

        let poller = std::thread::Builder::new()
            .name(format!("zmq-router-{addr}"))
            .spawn(move || {
                Self::poll_loop(socket, server, handle, running_thread, reply_tx, reply_rx)
            })
            .map_err(Error::Io)?;

        Ok(ZmqServer {
            running,
            poller: Some(poller),
        })
    }

    fn poll_loop<S>(
        socket: zmq::Socket,
        server: Arc<S>,
        handle: Handle,
        running: Arc<AtomicBool>,
        reply_tx: mpsc::Sender<(Vec<u8>, Vec<u8>)>,
        reply_rx: mpsc::Receiver<(Vec<u8>, Vec<u8>)>,
    ) where
        S: Server + 'static,
    {
        while running.load(Ordering::Relaxed) {
            Self::flush_replies(&socket, &reply_rx);
            match socket.recv_multipart(0) {
                Ok(mut parts) if parts.len() >= 2 => {
                    // ROUTER delivers [identity, payload]; extra frames are
                    // ignored (we frame one payload per message).
                    let payload = parts.pop().unwrap();
                    let identity = parts.remove(0);
                    if let Ok(env) = Envelope::decode(&payload) {
                        let server = server.clone();
                        let tx = reply_tx.clone();
                        handle.spawn(async move {
                            let reply = server.serve(env).await;
                            let _ = tx.send((identity, reply.encode()));
                        });
                    }
                }
                // Timeout (EAGAIN) or a malformed short message: loop.
                _ => {}
            }
        }
    }

    fn flush_replies(socket: &zmq::Socket, reply_rx: &mpsc::Receiver<(Vec<u8>, Vec<u8>)>) {
        while let Ok((identity, bytes)) = reply_rx.try_recv() {
            let _ = socket.send_multipart([identity, bytes], 0);
        }
    }

    /// Stop the poller and wait for it to exit.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(h) = self.poller.take() {
            let _ = h.join();
        }
    }
}

impl Drop for ZmqServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A short delay helper for callers waiting on a freshly bound endpoint. ZMQ
/// `inproc`/`tcp` connect is asynchronous; a brief pause lets the bind settle in
/// tests before the first request.
pub fn settle() {
    std::thread::sleep(Duration::from_millis(50));
}
