//! M4: a transport-only smoke test for the real ZeroMQ carrier. Correctness
//! lives in `transport_m4.rs` over the in-process switch; here we only prove a
//! frame survives a genuine `DEALER → ROUTER → DEALER` round trip (DESIGN §10).
//!
//! Uses `inproc://` with a shared context so the test needs no TCP port and is
//! deterministic.

use std::sync::Arc;
use std::time::Duration;

use dal::transport::codec::{Envelope, MsgType};
use dal::transport::dealer::ZmqTransport;
use dal::transport::router::{ZmqServer, settle};
use dal::transport::{Server, Transport};
use dal::types::{ClusterId, GroupId};

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

    server.shutdown();
}
