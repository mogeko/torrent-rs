//! uTP socket manager (BEP 29).
//!
//! Manages a single UDP socket for all uTP connections, dispatching
//! incoming packets by `connection_id` to the correct [`UtpConnection`].
//!
//! # Architecture
//!
//! ```text
//! UDP socket (recv loop)
//!   │
//!   ├── conn_id=0x1234 → UtpConnection A
//!   ├── conn_id=0x5678 → UtpConnection B
//!   └── unknown conn_id → ST_RESET (or new connection if SYN)
//! ```
//!
//! The socket runs a background receive task. Incoming packets are
//! dispatched to the appropriate connection via mpsc channels.
//! Connections send outgoing packets directly via the shared UDP socket.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::sync::{Mutex, mpsc};

use super::connection::{ConnState, UtpConnection, UtpIncoming};
use super::{UtpHeader, UtpType};

/// Channel buffer size for the main recv loop.
const SOCKET_RECV_BUF: usize = 4096;

/// A handle to a uTP connection, providing send/recv access.
#[allow(dead_code)]
pub struct UtpConnectionHandle {
    /// The remote peer address.
    pub remote_addr: SocketAddr,
    /// Channel to send incoming packets to the connection task.
    packet_tx: mpsc::UnboundedSender<UtpIncoming>,
    /// Channel to receive application data from the connection.
    data_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    /// Channel to send application data to the connection.
    data_tx: mpsc::UnboundedSender<Vec<u8>>,
    /// Channel to receive connection state updates.
    state_rx: mpsc::UnboundedReceiver<ConnState>,
}

impl UtpConnectionHandle {
    /// Send data to the remote peer through this connection.
    pub fn send(&mut self, data: Vec<u8>) -> Result<(), String> {
        self.data_tx
            .send(data)
            .map_err(|_| "uTP: connection closed".to_string())
    }

    /// Receive available data from this connection (non-blocking).
    pub fn try_recv(&mut self) -> Option<Vec<u8>> {
        self.data_rx.try_recv().ok()
    }

    /// Receive data from this connection (async).
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.data_rx.recv().await
    }

    /// Check current connection state (non-blocking).
    pub fn state(&mut self) -> Option<ConnState> {
        self.state_rx.try_recv().ok()
    }
}

/// Manages a shared UDP socket for all uTP connections.
///
/// Spawns a background receive task that dispatches incoming
/// packets by `connection_id`.
pub struct UtpSocket {
    /// The shared UDP socket.
    socket: Arc<UdpSocket>,
    /// Bound address of the socket.
    local_addr: SocketAddr,
    /// Active connections, keyed by their `conn_id_recv`.
    connections: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<UtpIncoming>>>>,
    /// Outgoing packet channel from connections to the send path.
    _outgoing_tx: mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
    /// Shutdown signal sender.
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl UtpSocket {
    /// Bind a UDP socket for uTP communication.
    ///
    /// Spawns a background receive loop that dispatches packets
    /// to registered connections.
    pub async fn bind(addr: SocketAddr) -> Result<Self, String> {
        let socket = UdpSocket::bind(addr)
            .await
            .map_err(|e| format!("uTP: bind failed: {e}"))?;

        let local_addr = socket
            .local_addr()
            .map_err(|e| format!("uTP: local_addr failed: {e}"))?;

        let socket = Arc::new(socket);
        let connections: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<UtpIncoming>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (outgoing_tx, _outgoing_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Spawn the receive loop
        let recv_socket = socket.clone();
        let recv_connections = connections.clone();
        tokio::spawn(Self::recv_loop(
            recv_socket,
            recv_connections,
            outgoing_tx.clone(),
            shutdown_rx,
        ));

        tracing::info!("uTP socket bound to {}", local_addr);

        Ok(UtpSocket {
            socket,
            local_addr,
            connections,
            _outgoing_tx: outgoing_tx,
            _shutdown_tx: shutdown_tx,
        })
    }

    /// Get the local address this socket is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Initiate an outbound uTP connection.
    ///
    /// Sends ST_SYN and returns a handle for send/recv operations.
    /// The connection runs as a background task.
    pub async fn connect(&self, remote_addr: SocketAddr) -> Result<UtpConnectionHandle, String> {
        let (packet_tx, packet_rx) = mpsc::unbounded_channel();
        let (_data_tx, conn_data_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (conn_data_tx, data_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (state_tx, state_rx) = mpsc::unbounded_channel::<ConnState>();

        // Create the connection
        let conn =
            UtpConnection::connect(self.socket.clone(), remote_addr, self._outgoing_tx.clone())
                .await?;

        let conn_id_recv = conn.conn_id_recv();

        // Register the connection
        {
            let mut guard = self.connections.lock().await;
            guard.insert(conn_id_recv, packet_tx.clone());
        }

        // Spawn the connection task
        let socket = self.socket.clone();
        let connections = self.connections.clone();
        tokio::spawn(async move {
            Self::connection_task(
                conn,
                packet_rx,
                conn_data_rx,
                state_tx,
                socket,
                connections,
                conn_id_recv,
            )
            .await;
        });

        tracing::info!(
            "uTP: initiated connection to {} (conn_id={})",
            remote_addr,
            conn_id_recv
        );

        Ok(UtpConnectionHandle {
            remote_addr,
            packet_tx,
            data_rx,
            data_tx: conn_data_tx,
            state_rx,
        })
    }

    /// Accept an incoming uTP connection from a SYN packet.
    async fn accept_incoming(
        socket: Arc<UdpSocket>, syn: &UtpHeader, src: SocketAddr,
        connections: &Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<UtpIncoming>>>>,
        outgoing_tx: &mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
    ) {
        let (packet_tx, packet_rx) = mpsc::unbounded_channel();
        let (_data_tx, conn_data_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (_conn_data_tx, _data_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (_state_tx, _state_rx) = mpsc::unbounded_channel::<ConnState>();

        let conn = UtpConnection::accept(syn, socket.clone(), src, outgoing_tx.clone());

        let conn_id_recv = conn.conn_id_recv();

        // Register
        {
            let mut guard = connections.lock().await;
            guard.insert(conn_id_recv, packet_tx.clone());
        }

        tracing::info!(
            "uTP: accepted connection from {} (conn_id={})",
            src,
            conn_id_recv
        );

        // Spawn connection task
        let conn_socket = socket.clone();
        let conn_connections = connections.clone();
        tokio::spawn(async move {
            Self::connection_task(
                conn,
                packet_rx,
                conn_data_rx,
                _state_tx,
                conn_socket,
                conn_connections,
                conn_id_recv,
            )
            .await;
        });
    }

    /// Background receive loop: reads UDP datagrams and dispatches them.
    async fn recv_loop(
        socket: Arc<UdpSocket>,
        connections: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<UtpIncoming>>>>,
        outgoing_tx: mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut buf = vec![0u8; SOCKET_RECV_BUF];

        loop {
            tokio::select! {
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((n, src)) => {
                            let data = &buf[..n];
                            if data.len() < 20 {
                                continue; // Too short for a uTP header
                            }

                            let header_data: [u8; 20] =
                                data[..20].try_into().unwrap();
                            match UtpHeader::from_bytes(&header_data) {
                                Ok(header) => {
                                    let conn_id = header.connection_id;
                                    let payload = data[20..].to_vec();

                                    // Try to dispatch to existing connection
                                    let guard = connections.lock().await;
                                    if let Some(tx) = guard.get(&conn_id) {
                                        let incoming = UtpIncoming {
                                            header,
                                            payload,
                                            src,
                                        };
                                        let _ = tx.send(incoming);
                                    } else if header.is_syn() {
                                        // New incoming connection
                                        drop(guard);
                                        Self::accept_incoming(
                                            socket.clone(),
                                            &header,
                                            src,
                                            &connections,
                                            &outgoing_tx,
                                        ).await;
                                    } else if header.is_reset() {
                                        // RST for unknown connection — ignore
                                        tracing::debug!(
                                            "uTP: RST for unknown conn_id={}",
                                            conn_id
                                        );
                                    } else {
                                        // Unknown connection — send RST
                                        tracing::debug!(
                                            "uTP: unknown conn_id={}, sending RST",
                                            conn_id
                                        );
                                        Self::send_reset(
                                            &socket, &header, src,
                                        ).await;
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!(
                                        "uTP: invalid header from {}: {}",
                                        src, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("uTP: recv error: {}", e);
                            break;
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    tracing::info!("uTP: socket recv loop shutting down");
                    break;
                }
            }
        }
    }

    /// Background connection task: processes packets and manages the connection.
    async fn connection_task(
        mut conn: UtpConnection, mut packet_rx: mpsc::UnboundedReceiver<UtpIncoming>,
        mut data_rx: mpsc::UnboundedReceiver<Vec<u8>>, state_tx: mpsc::UnboundedSender<ConnState>,
        _socket: Arc<UdpSocket>,
        connections: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<UtpIncoming>>>>,
        conn_id_recv: u16,
    ) {
        let mut retransmit_timer =
            tokio::time::interval(super::connection::RETRANSMIT_CHECK_INTERVAL);

        loop {
            tokio::select! {
                // Incoming uTP packets from the socket
                Some(incoming) = packet_rx.recv() => {
                    if let Err(e) = conn.handle_packet(
                        &incoming.header,
                        &incoming.payload,
                    ).await {
                        tracing::warn!(
                            "uTP: connection error for {}: {}",
                            conn.remote_addr(), e
                        );
                        break;
                    }

                    // Notify state change
                    let current_state = if conn.is_connected() {
                        ConnState::Connected
                    } else if conn.is_closed() {
                        ConnState::Closed
                    } else {
                        ConnState::SynSent
                    };
                    let _ = state_tx.send(current_state);
                }

                // Outgoing application data
                Some(data) = data_rx.recv() => {
                    if let Err(e) = conn.send(&data).await {
                        tracing::warn!(
                            "uTP: send error for {}: {}",
                            conn.remote_addr(), e
                        );
                        break;
                    }
                }

                // Retransmission timer
                _ = retransmit_timer.tick() => {
                    if let Err(e) = conn.check_retransmit().await {
                        tracing::warn!(
                            "uTP: retransmit error for {}: {}",
                            conn.remote_addr(), e
                        );
                        break;
                    }
                }
            }

            // Check if connection is closed
            if conn.is_closed() {
                break;
            }
        }

        // Cleanup: remove connection from registry
        {
            let mut guard = connections.lock().await;
            guard.remove(&conn_id_recv);
        }
        tracing::info!("uTP: connection {} cleaned up", conn_id_recv);
    }

    /// Send a ST_RESET to an unknown connection.
    async fn send_reset(socket: &UdpSocket, header: &UtpHeader, dst: SocketAddr) {
        let reset = UtpHeader {
            utp_type: UtpType::StReset,
            version: 1,
            extension: 0,
            connection_id: header.connection_id,
            timestamp_microseconds: 0,
            timestamp_difference_microseconds: 0,
            wnd_size: 0,
            seq_nr: header.ack_nr,
            ack_nr: header.seq_nr,
        };
        let _ = socket.send_to(&reset.to_bytes(), dst).await;
    }
}
