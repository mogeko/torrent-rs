use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::oneshot;

use crate::error::{Error, ErrorKind};

use super::krpc::{KrpcMessage, TransactionId};

/// Callback type for handling incoming DHT queries.
///
/// Receives the decoded [`KrpcMessage`] and the source address, returns
/// optional response bytes. Return `None` to silently ignore the query.
pub type QueryHandler = Arc<dyn Fn(&KrpcMessage, SocketAddr) -> Option<Vec<u8>> + Send + Sync>;

/// DHT RPC client for sending KRPC messages, matching responses, and
/// handling incoming queries.
///
/// Supports concurrent in-flight queries via a background receive loop
/// and a transaction ID → oneshot channel map. Each [`query`](DhtRpc::query)
/// call inserts a oneshot sender into `pending`, sends the UDP datagram,
/// then awaits the receiver. The background loop dispatches matching
/// responses by transaction ID and delegates queries to the optional
/// [`query_handler`](QueryHandler).
pub struct DhtRpc {
    socket: UdpSocket,
    pending: Mutex<HashMap<TransactionId, oneshot::Sender<KrpcMessage>>>,
    query_handler: Mutex<Option<QueryHandler>>,
}

/// Default timeout for DHT RPC calls.
const RPC_TIMEOUT: Duration = Duration::from_secs(15);

impl DhtRpc {
    /// Create a new DHT RPC client bound to a local address.
    ///
    /// Spawns a background receive loop that dispatches incoming KRPC
    /// messages to the corresponding in-flight query via transaction ID.
    pub async fn new(bind_addr: SocketAddr) -> Result<Arc<Self>, Error> {
        let socket = UdpSocket::bind(bind_addr).await?;
        let rpc = Arc::new(DhtRpc {
            socket,
            pending: Mutex::new(HashMap::new()),
            query_handler: Mutex::new(None),
        });
        rpc.clone().start_recv_loop();
        Ok(rpc)
    }

    /// Set the handler for incoming DHT queries.
    ///
    /// When the background receive loop receives a [`KrpcMessage::Query`],
    /// it invokes this handler with the message and source address.
    /// The handler's return value (if any) is sent back to the source.
    pub fn set_query_handler(&self, handler: QueryHandler) {
        *self.query_handler.lock().unwrap() = Some(handler);
    }

    /// Return the bound local address of the underlying UDP socket.
    pub fn local_addr(&self) -> Result<SocketAddr, Error> {
        self.socket.local_addr().map_err(Error::protocol)
    }

    /// Send a query and wait for a response via the transaction table.
    pub async fn query(
        &self,
        addr: SocketAddr,
        tid: TransactionId,
        data: &[u8],
    ) -> Result<KrpcMessage, Error> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(tid, tx);

        tracing::debug!("DHT query to {}", addr);
        if let Err(e) = self.socket.send_to(data, addr).await {
            self.pending.lock().unwrap().remove(&tid);
            return Err(Error::with_source(ErrorKind::Protocol, e));
        }

        tokio::time::timeout(RPC_TIMEOUT, rx)
            .await
            .map_err(|_| {
                self.pending.lock().unwrap().remove(&tid);
                Error::new(ErrorKind::Protocol)
            })?
            .map_err(|_| Error::new(ErrorKind::Protocol))
    }

    /// Ping a node to check if it's alive.
    pub async fn ping(
        &self,
        addr: SocketAddr,
        tid: TransactionId,
        node_id: &[u8; 20],
    ) -> Result<KrpcMessage, Error> {
        let data = super::krpc::build_ping(tid, node_id);
        self.query(addr, tid, &data).await
    }

    /// Background receive loop — dispatches responses and handles queries.
    fn start_recv_loop(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut buf = [0u8; 8192];
            loop {
                let (len, src_addr) = match self.socket.recv_from(&mut buf).await {
                    Ok(r) => r,
                    Err(_) => break,
                };

                let msg = match KrpcMessage::from_bytes(&buf[..len]) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                match &msg {
                    KrpcMessage::Response { transaction_id, .. }
                    | KrpcMessage::Error { transaction_id, .. } => {
                        if let Some(tx) = self.pending.lock().unwrap().remove(transaction_id) {
                            let _ = tx.send(msg);
                        }
                    }
                    KrpcMessage::Query { .. } => {
                        let handler = self.query_handler.lock().unwrap().clone();
                        if let Some(handler) = handler {
                            if let Some(response_bytes) = handler(&msg, src_addr) {
                                let _ = self.socket.send_to(&response_bytes, src_addr).await;
                            }
                        }
                    }
                }
            }
        });
    }
}
