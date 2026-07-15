use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpSocket;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

use crate::error::{Error, ErrorKind};

use super::utp::socket::UtpSocket;
use super::utp::stream::UtpStream;
use super::{Handshake, PeerId, PeerMessage, PeerState, decode, encode};

/// Timeout for TCP connect + handshake exchange.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum peer message payload size (2 MiB). Prevents OOM from malicious peers.
const MAX_MESSAGE_SIZE: u32 = 2 * 1024 * 1024;
/// Timeout for reading a single message body from a peer.
const MESSAGE_READ_TIMEOUT: Duration = Duration::from_secs(60);
/// Timeout for flushing data to a peer.
const MESSAGE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Bidirectional I/O trait for peer connections.
///
/// Implemented by TCP streams and uTP streams.
pub trait PeerIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> PeerIo for T {}

/// Inner stream variant for PeerConnection.
enum PeerStreamInner {
    /// TCP: split into independent read/write halves for concurrent access.
    Tcp {
        reader: Mutex<BufReader<OwnedReadHalf>>,
        writer: Mutex<BufWriter<OwnedWriteHalf>>,
    },
    /// Generic (uTP or other): single buffered stream behind a Mutex.
    Generic {
        stream: Mutex<tokio::io::BufStream<Pin<Box<dyn PeerIo>>>>,
    },
}

/// A managed peer connection over TCP or uTP.
///
/// For TCP connections, read/write halves are independent so that
/// recv() and send() never contend for the same lock. For uTP
/// connections, a single buffered stream is used.
pub struct PeerConnection {
    /// The underlying I/O stream (TCP halves or generic stream).
    inner: PeerStreamInner,
    /// Current protocol state.
    state: PeerState,
    /// The remote peer's ID (set after handshake).
    remote_peer_id: Option<PeerId>,
    /// Remote peer's reserved bytes from the BEP 3 handshake
    /// (for extension negotiation, BEP 10).
    remote_reserved: [u8; 8],
}

impl PeerConnection {
    /// Connect to a peer over TCP, perform the BEP 3 handshake.
    ///
    /// Performs TCP connect with TCP_NODELAY, then BEP 3 handshake,
    /// followed by BEP 10 LTEP extension negotiation if the remote
    /// peer supports extensions (bit 63 set).
    pub async fn connect(
        addr: SocketAddr, info_hash: [u8; 20], our_peer_id: PeerId,
    ) -> Result<Self, Error> {
        tracing::debug!("connecting to peer {}", addr);

        // TCP connect with timeout and TCP_NODELAY (critical for BitTorrent's
        // small control messages: Have, Request, Cancel — Nagle would add up
        // to 200ms of extra latency on each).
        let mut raw_stream = {
            let socket = if addr.is_ipv4() {
                TcpSocket::new_v4()
            } else {
                TcpSocket::new_v6()
            }
            .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?;

            socket
                .set_nodelay(true)
                .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?;

            tokio::time::timeout(HANDSHAKE_TIMEOUT, socket.connect(addr))
                .await
                .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
                .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
        };

        let (remote_peer_id, remote_reserved) =
            Self::perform_handshake(&mut raw_stream, info_hash, our_peer_id).await?;

        // Split into independent read/write halves for concurrent access
        let (read_half, write_half) = raw_stream.into_split();

        tracing::info!("TCP handshake complete with {}", addr);

        Ok(PeerConnection {
            inner: PeerStreamInner::Tcp {
                reader: Mutex::new(BufReader::new(read_half)),
                writer: Mutex::new(BufWriter::new(write_half)),
            },
            state: PeerState::Init,
            remote_peer_id: Some(remote_peer_id),
            remote_reserved,
        })
    }

    /// Connect to a peer over uTP (BEP 29), perform the BEP 3 handshake.
    ///
    /// The handshake runs over the uTP stream after the uTP connection
    /// is established. Returns a `PeerConnection` that uses uTP transport.
    pub(crate) async fn connect_utp(
        addr: SocketAddr, info_hash: [u8; 20], our_peer_id: PeerId, utp_socket: &UtpSocket,
    ) -> Result<Self, Error> {
        tracing::debug!("connecting to peer {} via uTP", addr);

        // Establish uTP connection
        let handle = utp_socket
            .connect(addr)
            .await
            .map_err(|e| Error::with_source(ErrorKind::PeerUtpConnectionFailed, e))?;

        let mut utp_stream = UtpStream::new(handle);

        // Perform BEP 3 handshake over uTP stream
        let (remote_peer_id, remote_reserved) =
            Self::perform_handshake(&mut utp_stream, info_hash, our_peer_id).await?;

        tracing::info!("uTP handshake complete with {}", addr);

        Ok(PeerConnection {
            inner: PeerStreamInner::Generic {
                stream: Mutex::new(tokio::io::BufStream::new(Box::pin(utp_stream))),
            },
            state: PeerState::Init,
            remote_peer_id: Some(remote_peer_id),
            remote_reserved,
        })
    }

    /// Perform the BEP 3 handshake on an already-connected stream.
    ///
    /// Returns the remote peer ID and reserved bytes.
    async fn perform_handshake(
        stream: &mut (impl AsyncRead + AsyncWrite + Unpin), info_hash: [u8; 20],
        our_peer_id: PeerId,
    ) -> Result<(PeerId, [u8; 8]), Error> {
        let mut handshake = Handshake::with_extensions(info_hash, our_peer_id.0, &[63]);
        // BEP 10 convention: byte 5 bit 4 = 0x10 signals LTEP support
        handshake.set_reserved_byte(5, handshake.reserved[5] | 0x10);
        // BEP 6: set bit 44 (byte 5, bit 3 = 0x08) for Fast Extension support.
        handshake.set_reserved_byte(5, handshake.reserved[5] | 0x08);
        let handshake_bytes = handshake.to_bytes();

        if let Err(e) =
            tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.write_all(&handshake_bytes)).await
        {
            return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
        }
        if let Err(e) = tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.flush()).await {
            return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
        }

        // Read remote handshake with timeout
        let mut buf = [0u8; 68];
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.read_exact(&mut buf)).await {
            Ok(Ok(_n)) => {}
            _ => return Err(Error::new(ErrorKind::PeerConnectionClosed)),
        };
        let remote_handshake = Handshake::from_bytes(&buf)?;

        // Verify info_hash
        if remote_handshake.info_hash != info_hash {
            return Err(Error::new(ErrorKind::PeerInvalidHandshake));
        }

        Ok((PeerId(remote_handshake.peer_id), remote_handshake.reserved))
    }

    /// Send a message to the peer.
    ///
    /// For TCP: locks the write half only — does not block concurrent reads.
    /// For uTP: locks the shared stream.
    pub async fn send(&self, msg: &PeerMessage) -> Result<(), Error> {
        tracing::trace!("sending {:?} to peer", msg);
        let data = encode(msg);

        match &self.inner {
            PeerStreamInner::Tcp { writer, .. } => {
                let mut writer = writer.lock().await;

                tokio::time::timeout(MESSAGE_WRITE_TIMEOUT, writer.write_all(&data))
                    .await
                    .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
                    .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;

                tokio::time::timeout(MESSAGE_WRITE_TIMEOUT, writer.flush())
                    .await
                    .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
                    .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;
            }
            PeerStreamInner::Generic { stream } => {
                let mut stream = stream.lock().await;

                tokio::time::timeout(MESSAGE_WRITE_TIMEOUT, stream.write_all(&data))
                    .await
                    .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
                    .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;

                tokio::time::timeout(MESSAGE_WRITE_TIMEOUT, stream.flush())
                    .await
                    .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
                    .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;
            }
        }

        Ok(())
    }

    /// Receive the next message from the peer.
    ///
    /// For TCP: locks the read half only — does not block concurrent writes.
    /// For uTP: locks the shared stream.
    pub async fn recv(&self) -> Result<PeerMessage, Error> {
        // Read 4-byte length prefix with timeout
        let len = {
            let mut len_buf = [0u8; 4];
            match &self.inner {
                PeerStreamInner::Tcp { reader, .. } => {
                    let mut reader = reader.lock().await;
                    tokio::time::timeout(MESSAGE_READ_TIMEOUT, reader.read_exact(&mut len_buf))
                        .await
                        .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
                        .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;
                }
                PeerStreamInner::Generic { stream } => {
                    let mut stream = stream.lock().await;
                    tokio::time::timeout(MESSAGE_READ_TIMEOUT, stream.read_exact(&mut len_buf))
                        .await
                        .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
                        .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;
                }
            }
            u32::from_be_bytes(len_buf)
        };

        // Keep-alive
        if len == 0 {
            tracing::trace!("received KeepAlive from peer");
            return Ok(PeerMessage::KeepAlive);
        }

        // Enforce maximum message size to prevent OOM from malicious peers
        if len > MAX_MESSAGE_SIZE {
            return Err(Error::new(ErrorKind::PeerConnectionClosed));
        }

        // Read the rest: message id + payload with timeout
        let mut msg_buf = vec![0u8; len as usize];
        match &self.inner {
            PeerStreamInner::Tcp { reader, .. } => {
                let mut reader = reader.lock().await;
                tokio::time::timeout(MESSAGE_READ_TIMEOUT, reader.read_exact(&mut msg_buf))
                    .await
                    .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
                    .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;
            }
            PeerStreamInner::Generic { stream } => {
                let mut stream = stream.lock().await;
                tokio::time::timeout(MESSAGE_READ_TIMEOUT, stream.read_exact(&mut msg_buf))
                    .await
                    .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
                    .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;
            }
        }

        // Build full wire format for decode: length prefix + msg_buf
        let mut full_msg = len.to_be_bytes().to_vec();
        full_msg.extend_from_slice(&msg_buf);

        decode(&full_msg)
    }

    /// Return the current connection state.
    pub fn state(&self) -> PeerState {
        self.state
    }

    /// Set the connection state.
    pub fn set_state(&mut self, state: PeerState) {
        self.state = state;
    }

    /// Return the remote peer's ID.
    pub fn remote_peer_id(&self) -> Option<PeerId> {
        self.remote_peer_id
    }

    /// Check if the remote peer advertised a specific extension bit
    /// in its BEP 3 handshake reserved bytes.
    ///
    /// Bit numbering follows BEP 3 conventions: bit 0 = MSB of byte 0.
    pub fn remote_has_extension(&self, bit: usize) -> bool {
        if bit >= 64 {
            return false;
        }
        let byte = bit / 8;
        let bit_in_byte = 7 - (bit % 8);
        (self.remote_reserved[byte] >> bit_in_byte) & 1 == 1
    }

    /// Return the remote peer's reserved bytes from the BEP 3 handshake.
    pub fn remote_reserved(&self) -> &[u8; 8] {
        &self.remote_reserved
    }
}
