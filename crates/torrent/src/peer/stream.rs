use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpSocket;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

use crate::error::{Error, ErrorKind};

use super::{Handshake, PeerId, PeerMessage, PeerState, decode, encode};

/// Timeout for TCP connect + handshake exchange.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum peer message payload size (2 MiB). Prevents OOM from malicious peers.
const MAX_MESSAGE_SIZE: u32 = 2 * 1024 * 1024;
/// Timeout for reading a single message body from a peer.
const MESSAGE_READ_TIMEOUT: Duration = Duration::from_secs(60);
/// Timeout for flushing data to a peer.
const MESSAGE_WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// A managed peer connection with independent read/write halves.
///
/// Uses [`OwnedReadHalf`] + [`OwnedWriteHalf`] behind separate [`Mutex`] guards
/// so that the reader task (recv) and the download loop (send) never contend
/// for the same lock. This is essential for BitTorrent's full-duplex wire protocol
/// where requests and piece data flow in both directions concurrently.
pub struct PeerConnection {
    /// Buffered read half (owned, behind Mutex for concurrent access).
    reader: Mutex<BufReader<OwnedReadHalf>>,
    /// Buffered write half (owned, behind Mutex for concurrent access).
    writer: Mutex<BufWriter<OwnedWriteHalf>>,
    /// Current protocol state.
    state: PeerState,
    /// The remote peer's ID (set after handshake).
    remote_peer_id: Option<PeerId>,
    /// Remote peer's reserved bytes from the BEP 3 handshake
    /// (for extension negotiation, BEP 10).
    remote_reserved: [u8; 8],
}

impl PeerConnection {
    /// Connect to a peer, perform the handshake, and return a connection.
    ///
    /// Performs BEP 3 TCP handshake followed by BEP 10 LTEP extension
    /// negotiation if the remote peer supports extensions (bit 63 set).
    pub async fn connect(
        addr: SocketAddr, info_hash: [u8; 20], our_peer_id: PeerId,
    ) -> Result<Self, Error> {
        tracing::debug!("connecting to peer {}", addr);

        // TCP connect with timeout and TCP_NODELAY (critical for BitTorrent's
        // small control messages: Have, Request, Cancel — Nagle would add up
        // to 200ms of extra latency on each).
        let raw_stream = {
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

        let mut raw_stream = raw_stream;

        // Perform BEP 3 handshake directly on the raw TcpStream so that no
        // BufReader read-ahead can steal bytes from subsequent wire
        // messages (Bitfield, Unchoke, etc.) that the peer may send
        // immediately after its handshake.
        let mut handshake = Handshake::with_extensions(info_hash, our_peer_id.0, &[63]);
        // BEP 10 convention: byte 5 bit 4 = 0x10 signals LTEP support
        // (alongside bit 63 which is shared with DHT)
        handshake.set_reserved_byte(5, handshake.reserved[5] | 0x10);
        // BEP 6: set bit 44 (byte 5, bit 3 = 0x08) for Fast Extension support.
        handshake.set_reserved_byte(5, handshake.reserved[5] | 0x08);
        let handshake_bytes = handshake.to_bytes();

        if let Err(e) =
            tokio::time::timeout(HANDSHAKE_TIMEOUT, raw_stream.write_all(&handshake_bytes)).await
        {
            return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
        }
        if let Err(e) = tokio::time::timeout(HANDSHAKE_TIMEOUT, raw_stream.flush()).await {
            return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
        }

        // Read remote handshake with timeout
        let mut buf = [0u8; 68];
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, raw_stream.read_exact(&mut buf)).await {
            Ok(Ok(_n)) => {}
            _ => return Err(Error::new(ErrorKind::PeerConnectionClosed)),
        };
        let remote_handshake = Handshake::from_bytes(&buf)?;

        // Verify info_hash
        if remote_handshake.info_hash != info_hash {
            return Err(Error::new(ErrorKind::PeerInvalidHandshake));
        }

        let remote_reserved = remote_handshake.reserved;

        // Now split into independent read/write halves so that recv and
        // send can proceed concurrently without lock contention.
        // BufReader/BufWriter are applied AFTER the split so no handshake
        // bytes are ever lost to read-ahead buffering.
        let (read_half, write_half) = raw_stream.into_split();

        tracing::info!("handshake complete with {}", addr);

        Ok(PeerConnection {
            reader: Mutex::new(BufReader::new(read_half)),
            writer: Mutex::new(BufWriter::new(write_half)),
            state: PeerState::Init,
            remote_peer_id: Some(PeerId(remote_handshake.peer_id)),
            remote_reserved,
        })
    }

    /// Send a message to the peer.
    ///
    /// Locks the write half only — does not block concurrent reads.
    pub async fn send(&self, msg: &PeerMessage) -> Result<(), Error> {
        tracing::trace!("sending {:?} to peer", msg);
        let data = encode(msg);
        let mut writer = self.writer.lock().await;

        tokio::time::timeout(MESSAGE_WRITE_TIMEOUT, writer.write_all(&data))
            .await
            .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
            .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;

        tokio::time::timeout(MESSAGE_WRITE_TIMEOUT, writer.flush())
            .await
            .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
            .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;

        Ok(())
    }

    /// Receive the next message from the peer.
    ///
    /// Locks the read half only — does not block concurrent writes.
    pub async fn recv(&self) -> Result<PeerMessage, Error> {
        let mut reader = self.reader.lock().await;

        // Read 4-byte length prefix with timeout
        let mut len_buf = [0u8; 4];
        tokio::time::timeout(MESSAGE_READ_TIMEOUT, reader.read_exact(&mut len_buf))
            .await
            .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
            .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;

        let len = u32::from_be_bytes(len_buf);

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
        tokio::time::timeout(MESSAGE_READ_TIMEOUT, reader.read_exact(&mut msg_buf))
            .await
            .map_err(|_| Error::new(ErrorKind::PeerConnectionClosed))?
            .map_err(|e| Error::with_source(ErrorKind::PeerConnectionClosed, e))?;

        // Build full wire format for decode: length prefix + msg_buf
        let mut full_msg = len_buf.to_vec();
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
