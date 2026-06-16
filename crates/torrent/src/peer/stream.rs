use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

use crate::error::{Error, ErrorKind};

use super::{Handshake, PeerId, PeerMessage, PeerState, decode, encode};

/// Timeout for TCP connect + handshake exchange.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// A managed peer connection with buffered message I/O.
pub struct PeerConnection {
    stream: BufReader<BufWriter<TcpStream>>,
    state: PeerState,
    /// Info hash expected for this connection.
    #[allow(dead_code)]
    info_hash: [u8; 20],
    /// Our peer ID.
    #[allow(dead_code)]
    our_peer_id: PeerId,
    /// The remote peer's ID (set after handshake).
    remote_peer_id: Option<PeerId>,
}

impl PeerConnection {
    /// Connect to a peer, perform the handshake, and return a connection.
    pub async fn connect(
        addr: SocketAddr, info_hash: [u8; 20], our_peer_id: PeerId,
    ) -> Result<Self, Error> {
        tracing::debug!("connecting to peer {}", addr);

        // TCP connect with timeout
        let raw_stream =
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(addr)).await {
                Ok(Ok(s)) => s,
                _ => return Err(Error::new(ErrorKind::PeerConnectionClosed)),
            };

        let stream = BufReader::new(BufWriter::new(raw_stream));

        let mut conn = PeerConnection {
            stream,
            state: PeerState::Handshake,
            info_hash,
            our_peer_id,
            remote_peer_id: None,
        };

        // Send our handshake
        let handshake = Handshake::new(info_hash, our_peer_id.0);
        let handshake_bytes = handshake.to_bytes();

        if let Err(e) = conn.stream.get_mut().write_all(&handshake_bytes).await {
            return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
        }

        if let Err(e) = conn.stream.get_mut().flush().await {
            return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
        }

        // Read remote handshake with timeout
        let mut buf = [0u8; 68];
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, read_exact(&mut conn, &mut buf)).await {
            Ok(Ok(())) => {}
            _ => return Err(Error::new(ErrorKind::PeerConnectionClosed)),
        };
        let remote_handshake = Handshake::from_bytes(&buf)?;

        // Verify info_hash
        if remote_handshake.info_hash != info_hash {
            return Err(Error::new(ErrorKind::PeerInvalidHandshake));
        }

        conn.remote_peer_id = Some(PeerId(remote_handshake.peer_id));
        conn.state = PeerState::Init;

        tracing::info!("handshake complete with {}", addr);

        Ok(conn)
    }

    /// Send a message to the peer.
    pub async fn send(&mut self, msg: &PeerMessage) -> Result<(), Error> {
        tracing::trace!("sending {:?} to peer", msg);
        let data = encode(msg);

        if let Err(e) = self.stream.get_mut().write_all(&data).await {
            return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
        }

        if let Err(e) = self.stream.get_mut().flush().await {
            return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
        }

        Ok(())
    }

    /// Receive the next message from the peer.
    pub async fn recv(&mut self) -> Result<PeerMessage, Error> {
        // Read 4-byte length prefix
        let mut len_buf = [0u8; 4];
        read_exact(self, &mut len_buf).await?;

        let len = u32::from_be_bytes(len_buf);

        // Keep-alive
        if len == 0 {
            tracing::trace!("received KeepAlive from peer");
            return Ok(PeerMessage::KeepAlive);
        }

        // Read the rest: message id + payload
        let mut msg_buf = vec![0u8; len as usize];
        read_exact(self, &mut msg_buf).await?;

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
}

/// Read exactly `n` bytes from the buffered stream.
async fn read_exact(conn: &mut PeerConnection, buf: &mut [u8]) -> Result<(), Error> {
    if let Err(e) = conn.stream.read_exact(buf).await {
        return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
    }

    Ok(())
}
