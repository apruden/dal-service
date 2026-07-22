//! Outbound ZeroMQ transport (DESIGN §10.1): a `DEALER`-based [`Transport`] that
//! sends a request frame to a peer `ROUTER` and awaits the correlated reply.
//!
//! Correctness never rides this path — every multi-node test uses the
//! in-process switch (ground rule 3). ZeroMQ is best-effort: application-level
//! timeout plus idempotent retry (DESIGN §10.3) live in the client and Raft
//! above, so here a lost reply simply surfaces as a timeout `Err` and the caller
//! retries another candidate.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::transport::codec::Envelope;
use crate::transport::Transport;

/// A ZeroMQ outbound transport. The `zmq::Context` is `Send + Sync` and shared;
/// each `call` opens a short-lived `DEALER` inside a blocking task so the
/// non-`Send` socket never crosses a thread boundary. A production build would
/// pool these per peer (DESIGN §10.1); pooling is an efficiency refinement that
/// does not change the request/reply contract this trait defines.
#[derive(Clone)]
pub struct ZmqTransport {
    ctx: zmq::Context,
    timeout: Duration,
}

impl ZmqTransport {
    pub fn new(ctx: zmq::Context, timeout: Duration) -> ZmqTransport {
        ZmqTransport { ctx, timeout }
    }
}

fn zmq_io(e: zmq::Error) -> Error {
    Error::Io(std::io::Error::other(format!("zmq: {e}")))
}

impl Transport for ZmqTransport {
    async fn call(&self, addr: &str, request: Envelope) -> Result<Envelope> {
        let ctx = self.ctx.clone();
        let addr = addr.to_string();
        let timeout_ms = self.timeout.as_millis().min(i32::MAX as u128) as i32;
        let frame = request.encode();

        let reply = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let socket = ctx.socket(zmq::DEALER).map_err(zmq_io)?;
            socket.set_linger(0).map_err(zmq_io)?;
            socket.set_rcvtimeo(timeout_ms).map_err(zmq_io)?;
            socket.set_sndtimeo(timeout_ms).map_err(zmq_io)?;
            socket.connect(&addr).map_err(zmq_io)?;
            socket.send(frame, 0).map_err(zmq_io)?;
            // DEALER→ROUTER→DEALER: the identity frame is stripped, so the reply
            // is a single payload frame.
            let mut parts = socket.recv_multipart(0).map_err(zmq_io)?;
            parts.pop().ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "empty reply from peer",
                ))
            })
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(format!("join: {e}"))))??;

        Envelope::decode(&reply).map_err(Error::codec)
    }
}
