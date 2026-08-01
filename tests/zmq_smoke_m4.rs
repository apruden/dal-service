//! M4: a transport-only smoke test for the real ZeroMQ carrier. Correctness
//! lives in `transport_m4.rs` over the in-process switch; here we only prove a
//! frame survives a genuine `DEALER → ROUTER → DEALER` round trip (DESIGN §10).
//!
//! Uses `inproc://` with a shared context so the test needs no TCP port and is
//! deterministic.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use dal::transport::codec::{Envelope, MsgType};
use dal::transport::dealer::ZmqTransport;
use dal::transport::router::{ZmqServer, settle};
use dal::transport::{Server, Transport};
use dal::types::{ClusterId, GroupId};
use tokio::sync::oneshot;

const CID: ClusterId = 0x0000_0000_0000_0000_0000_0000_0000_0DA1;

/// Echoes the request payload back in a reply envelope stamped with the same
/// cluster id.
struct Echo;

impl Server for Echo {
    async fn serve(&self, request: Envelope) -> Envelope {
        Envelope::new(
            request.cluster_id,
            MsgType::MetaQuery,
            GroupId::Meta,
            request.request_id,
            request.payload,
        )
    }
}

struct Blocking {
    started: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl Server for Blocking {
    async fn serve(&self, request: Envelope) -> Envelope {
        if let Some(started) = self.started.lock().unwrap().take() {
            let _ = started.send(());
        }
        let release = self.release.lock().unwrap().take();
        if let Some(release) = release {
            let _ = release.await;
        }
        request
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zmq_dealer_router_round_trip() {
    let ctx = zmq::Context::new();
    let addr = "inproc://m4-zmq-smoke";
    let server = ZmqServer::bind(ctx.clone(), addr, Arc::new(Echo)).unwrap();
    settle();

    let transport = ZmqTransport::new(ctx.clone(), Duration::from_secs(2));
    let payload = b"ping-through-zmq".to_vec();
    let env = Envelope::new(
        CID,
        MsgType::ClientOp,
        GroupId::Data(0),
        99,
        payload.clone(),
    );

    let reply = transport.call(addr, env).await.unwrap();
    assert_eq!(reply.cluster_id, CID);
    assert_eq!(reply.payload, payload);

    server.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_yields_while_an_inflight_handler_finishes() {
    let ctx = zmq::Context::new();
    let addr = "inproc://m4-zmq-async-shutdown";
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = ZmqServer::bind(
        ctx.clone(),
        addr,
        Arc::new(Blocking {
            started: Mutex::new(Some(started_tx)),
            release: Mutex::new(Some(release_rx)),
        }),
    )
    .unwrap();
    settle();

    let transport = ZmqTransport::new(ctx, Duration::from_secs(2));
    let request = tokio::spawn(async move {
        transport
            .call(
                addr,
                Envelope::new(CID, MsgType::MetaQuery, GroupId::Meta, 1, Vec::new()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), started_rx)
        .await
        .expect("handler was never dispatched")
        .expect("handler start signal dropped");

    let mut shutdown = tokio::spawn(server.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut shutdown)
            .await
            .is_err(),
        "shutdown returned before the active handler completed"
    );

    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), shutdown)
        .await
        .expect("shutdown blocked the current-thread runtime")
        .expect("shutdown task panicked");
    request.abort();
}
