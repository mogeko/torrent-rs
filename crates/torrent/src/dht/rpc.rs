use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::UdpSocket;

use crate::dht::krpc::{KrpcMessage, TransactionId};
use crate::error::{Error, ErrorKind};

/// DHT RPC client for sending KRPC messages and matching responses.
pub struct DhtRpc {
    socket: UdpSocket,
}

/// Default timeout for DHT RPC calls.
const RPC_TIMEOUT: Duration = Duration::from_secs(15);

impl DhtRpc {
    /// Create a new DHT RPC client bound to a local address.
    pub async fn new(bind_addr: SocketAddr) -> Result<Self, Error> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(DhtRpc { socket })
    }

    /// Send a query and wait for a response.
    ///
    /// This is a simple send-and-receive with timeout and transaction ID matching.
    /// In a full implementation, multiple in-flight queries with proper matching
    /// would be supported via a pending call map.
    pub async fn query(
        &self,
        addr: SocketAddr,
        expected_tid: TransactionId,
        data: &[u8],
    ) -> Result<KrpcMessage, Error> {
        tracing::debug!("DHT query to {}", addr);
        if let Err(e) = self.socket.send_to(data, addr).await {
            return Err(Error::with_source(ErrorKind::Protocol, e));
        }

        let mut buf = [0u8; 2048];
        let (len, _src) = tokio::time::timeout(RPC_TIMEOUT, self.socket.recv_from(&mut buf))
            .await
            .map_err(|_| Error::new(ErrorKind::Protocol))?
            .map_err(Error::protocol)?;

        let response = KrpcMessage::from_bytes(&buf[..len])?;

        // Verify transaction ID matches
        match &response {
            KrpcMessage::Response { transaction_id, .. }
            | KrpcMessage::Error { transaction_id, .. } => {
                if *transaction_id != expected_tid {
                    return Err(Error::new(ErrorKind::Protocol));
                }
            }
            KrpcMessage::Query { .. } => {
                return Err(Error::new(ErrorKind::Protocol));
            }
        }

        Ok(response)
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
}
