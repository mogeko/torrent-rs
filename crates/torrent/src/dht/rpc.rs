//! Async DHT RPC client — UDP send/receive with transaction matching.
//!
//! [`DhtRpc`] binds a UDP socket, spawns a background receive loop, and
//! supports concurrent in-flight queries via a transaction ID → oneshot
//! channel map. Incoming queries are dispatched to an optional
//! [`QueryHandler`] callback.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::oneshot;

use crate::error::{Error, ErrorKind};

use super::krpc::{KrpcMessage, TransactionId};

/// Callback type for handling incoming DHT queries.
///
/// Receives the decoded [`KrpcMessage`] and the source address, returns
/// optional response bytes. Return `None` to silently ignore the query.
pub type QueryHandler = Arc<dyn Fn(&KrpcMessage, SocketAddr) -> Option<Vec<u8>> + Send + Sync>;

/// Internal state shared by [`DhtRpc`] clones.
struct DhtRpcInner {
    socket: UdpSocket,
    pending: Mutex<HashMap<TransactionId, oneshot::Sender<KrpcMessage>>>,
    query_handler: Mutex<Option<QueryHandler>>,
}

/// DHT RPC client for sending KRPC messages, matching responses, and
/// handling incoming queries.
///
/// Thin `Arc`-based handle — cloning is cheap.  Supports concurrent
/// in-flight queries via a background receive loop and a transaction
/// ID → oneshot channel map.
///
/// Timeout is not embedded — apply [`tokio::time::timeout`] at the
/// call site.  Incoming queries are dispatched to an optional
/// [`QueryHandler`] callback.
#[derive(Clone)]
pub struct DhtRpc {
    inner: Arc<DhtRpcInner>,
}

impl DhtRpc {
    /// Create a new DHT RPC client bound to a local address.
    ///
    /// Spawns a background receive loop that dispatches incoming KRPC
    /// messages to the corresponding in-flight query via transaction ID.
    pub async fn new(bind_addr: SocketAddr) -> Result<Self, Error> {
        let socket = bind_dht_socket(bind_addr)?;
        let inner = Arc::new(DhtRpcInner {
            socket,
            pending: Mutex::new(HashMap::new()),
            query_handler: Mutex::new(None),
        });
        start_recv_loop(inner.clone());
        Ok(DhtRpc { inner })
    }

    /// Set the handler for incoming DHT queries.
    ///
    /// When the background receive loop receives a [`KrpcMessage::Query`],
    /// it invokes this handler with the message and source address.
    /// The handler's return value (if any) is sent back to the source.
    pub fn set_query_handler(&self, handler: QueryHandler) {
        *self.inner.query_handler.lock().unwrap() = Some(handler);
    }

    /// Return the bound local address of the underlying UDP socket.
    pub fn local_addr(&self) -> Result<SocketAddr, Error> {
        self.inner.socket.local_addr().map_err(Error::protocol)
    }

    /// Send a query and wait for a response via the transaction table.
    ///
    /// Raw I/O only — no timeout is applied here.
    /// Wrap with [`tokio::time::timeout`] at the call site.
    pub async fn query(
        &self, addr: SocketAddr, tid: TransactionId, data: &[u8],
    ) -> Result<KrpcMessage, Error> {
        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().unwrap().insert(tid, tx);

        tracing::debug!("DHT query to {}", addr);
        if let Err(e) = self.inner.socket.send_to(data, addr).await {
            self.inner.pending.lock().unwrap().remove(&tid);
            return Err(Error::with_source(ErrorKind::Protocol, e));
        }

        rx.await.map_err(|_| {
            self.inner.pending.lock().unwrap().remove(&tid);
            Error::new(ErrorKind::Protocol)
        })
    }

    /// Ping a node to check if it's alive.
    pub async fn ping(
        &self, addr: SocketAddr, tid: TransactionId, node_id: &[u8; 20],
    ) -> Result<KrpcMessage, Error> {
        let data = super::krpc::build_ping(tid, node_id);
        self.query(addr, tid, &data).await
    }
}

/// Background receive loop — dispatches responses and handles queries.
fn start_recv_loop(inner: Arc<DhtRpcInner>) {
    tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        loop {
            let (len, src_addr) = match inner.socket.recv_from(&mut buf).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("DHT recv error: {e}");
                    continue;
                }
            };

            let msg = match KrpcMessage::from_bytes(&buf[..len]) {
                Ok(m) => m,
                Err(_) => continue,
            };

            match &msg {
                KrpcMessage::Response { transaction_id, .. }
                | KrpcMessage::Error { transaction_id, .. } => {
                    if let Some(tx) = inner.pending.lock().unwrap().remove(transaction_id) {
                        let _ = tx.send(msg);
                    }
                }
                KrpcMessage::Query { .. } => {
                    let handler = inner.query_handler.lock().unwrap().clone();
                    if let Some(handler) = handler {
                        if let Some(response_bytes) = handler(&msg, src_addr) {
                            let _ = inner.socket.send_to(&response_bytes, src_addr).await;
                        }
                    }
                }
            }
        }
    });
}

/// Bind a UDP socket with SO_REUSEADDR for DHT.
///
/// DHT binds known ports (e.g. 6882). Without SO_REUSEADDR, a session
/// restart would fail while the old socket is still in TIME_WAIT (up to
/// 120 s).
fn bind_dht_socket(bind_addr: SocketAddr) -> Result<UdpSocket, Error> {
    let domain = if bind_addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };

    let socket = match Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)) {
        Ok(s) => s,
        Err(e) => return Err(Error::protocol(e)),
    };

    socket
        .set_reuse_address(true)
        .map_err(|e| tracing::warn!("DHT: set_reuse_address failed: {e}"))
        .ok();

    if bind_addr.is_ipv6() {
        socket
            .set_only_v6(true)
            .map_err(|e| tracing::warn!("DHT: set_only_v6 failed: {e}"))
            .ok();
    }

    socket
        .bind(&bind_addr.into())
        .map_err(|e| Error::with_source(ErrorKind::Protocol, e))?;
    socket
        .set_nonblocking(true)
        .map_err(|e| Error::with_source(ErrorKind::Protocol, e))?;

    let std_socket: std::net::UdpSocket = socket.into();

    tokio::net::UdpSocket::from_std(std_socket).map_err(Error::protocol)
}
