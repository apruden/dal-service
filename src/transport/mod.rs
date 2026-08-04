//! Transport layer: the envelope codec, an abstract [`Transport`] seam, and its
//! two implementations (DESIGN §10, ground rule 3 — "two networks behind one
//! trait", here applied to the client/RPC path).
//!
//! Correctness tests run over the in-process [`InProcess`] switch, which is
//! deterministic; the [`dealer::ZmqTransport`] transport is exercised
//! separately for ZeroMQ transport concerns only.

pub mod codec;
pub mod dealer;
pub mod raft_net;
pub mod raft_wire;
pub mod router;

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use codec::Envelope;

/// A request/reply RPC channel keyed by node control address. The client and
/// peer paths depend only on this trait; the concrete carrier (in-process or
/// ZeroMQ) is chosen at construction.
pub trait Transport: Send + Sync {
    /// Send `request` to `addr` and await the reply frame. An `Err` means the
    /// peer was unreachable or the exchange timed out — the caller retries
    /// another candidate with the same idempotency key (DESIGN §8.4).
    fn call(&self, addr: &str, request: Envelope) -> impl Future<Output = Result<Envelope>> + Send;
}

/// A node-side request handler: consumes an inbound frame and produces its
/// reply. Implemented by the client gateway and the peer-control dispatcher.
pub trait Server: Send + Sync {
    fn serve(&self, request: Envelope) -> impl Future<Output = Envelope> + Send;
}

/// A deterministic in-process transport: a shared directory of address →
/// server. Used by every M4 correctness test so routing, redirect, and retry
/// behaviour is exercised without real sockets or timing (ground rule 3).
pub struct InProcess<S> {
    servers: Arc<Mutex<HashMap<String, Arc<S>>>>,
}

impl<S> Clone for InProcess<S> {
    fn clone(&self) -> Self {
        InProcess {
            servers: self.servers.clone(),
        }
    }
}

impl<S> Default for InProcess<S> {
    fn default() -> Self {
        InProcess {
            servers: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<S> InProcess<S> {
    pub fn new() -> Self {
        InProcess::default()
    }

    /// Bind a server at an address so peers/clients can reach it.
    pub fn register(&self, addr: impl Into<String>, server: Arc<S>) {
        self.servers.lock().unwrap().insert(addr.into(), server);
    }

    /// Remove a server, modelling a node that has gone away.
    pub fn deregister(&self, addr: &str) {
        self.servers.lock().unwrap().remove(addr);
    }
}

impl<S: Server + 'static> Transport for InProcess<S> {
    async fn call(&self, addr: &str, request: Envelope) -> Result<Envelope> {
        let server = self.servers.lock().unwrap().get(addr).cloned();
        match server {
            Some(s) => Ok(s.serve(request).await),
            None => Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("no server bound at {addr}"),
            ))),
        }
    }
}
