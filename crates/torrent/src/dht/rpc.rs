use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::oneshot;

use crate::error::{Error, ErrorKind};

use super::krpc::{KrpcMessage, TransactionId};

/// DHT RPC client for sending KRPC messages and matching responses.
///
/// Supports concurrent in-flight queries via a background receive loop
/// and a transaction ID → oneshot channel map. Each [`query`](DhtRpc::query)
/// call inserts a oneshot sender into [`pending`], sends the UDP datagram,
/// then awaits the receiver. The background loop dispatches matching
/// responses by transaction ID.
pub struct DhtRpc {
    socket: UdpSocket,
    pending: Mutex<HashMap<TransactionId, oneshot::Sender<KrpcMessage>>>,
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
        });
        rpc.clone().start_recv_loop();
        Ok(rpc)
    }

    /// Send a query and wait for a response via the transaction table.
    ///
    /// Registers a oneshot sender under `tid`, sends the datagram, then
    /// awaits the response from the background receive loop. On timeout
    /// the pending entry is cleaned up automatically.
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

    /// Background receive loop — dispatches responses by transaction ID.
    fn start_recv_loop(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut buf = [0u8; 8192];
            loop {
                let (len, _src) = match self.socket.recv_from(&mut buf).await {
                    Ok(r) => r,
                    Err(_) => break,
                };

                let msg = match KrpcMessage::from_bytes(&buf[..len]) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                // Extract transaction ID from response/error messages
                let tid = match &msg {
                    KrpcMessage::Response { transaction_id, .. }
                    | KrpcMessage::Error { transaction_id, .. } => *transaction_id,
                    KrpcMessage::Query { .. } => {
                        // Incoming queries are not handled yet (Phase 4).
                        continue;
                    }
                };

                // Dispatch to the waiting query (if any).
                if let Some(tx) = self.pending.lock().unwrap().remove(&tid) {
                    let _ = tx.send(msg);
                }
            }
        });
    }
}
