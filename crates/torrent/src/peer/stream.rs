use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

use crate::bencode;
use crate::error::{Error, ErrorKind};

use super::{ExtensionNegotiation, Handshake, PeerId, PeerMessage, PeerState, decode, encode};

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
    /// Extension name → message ID mapping negotiated via LTEP (BEP 10).
    /// Empty if the remote peer does not support extensions.
    extension_ids: HashMap<String, u8>,
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

        // TCP connect with timeout
        let mut raw_stream =
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, TcpStream::connect(addr)).await {
                Ok(Ok(s)) => s,
                _ => return Err(Error::new(ErrorKind::PeerConnectionClosed)),
            };

        // Perform BEP 3 handshake directly on the raw TcpStream so that no
        // BufReader read-ahead can steal bytes from subsequent wire
        // messages (Bitfield, Unchoke, etc.) that the peer may send
        // immediately after its handshake.
        let mut handshake = Handshake::with_extensions(info_hash, our_peer_id.0, &[63]);
        // BEP 10 convention: byte 5 bit 4 = 0x10 signals LTEP support
        // (alongside bit 63 which is shared with DHT)
        handshake.set_reserved_byte(5, handshake.reserved[5] | 0x10);
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

        // BEP 10 LTEP handshake: negotiate extension IDs if both peers support it
        let extension_ids = if remote_handshake.has_extension(63) {
            tracing::debug!(
                "remote peer {} supports extensions, performing LTEP handshake",
                addr
            );
            perform_ltep_handshake(&mut raw_stream, addr).await?
        } else {
            HashMap::new()
        };

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
            extension_ids,
        })
    }

    /// Send an extended message by extension name (BEP 10).
    ///
    /// Looks up the extension's message ID from the negotiated
    /// `extension_ids` map. Returns an error if the extension
    /// was not negotiated with this peer.
    pub async fn send_extended(&self, ext_name: &str, data: Vec<u8>) -> Result<(), Error> {
        let ext_id = self
            .extension_ids
            .get(ext_name)
            .copied()
            .ok_or_else(|| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
        self.send(&PeerMessage::Extended { ext_id, data }).await
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

    /// Look up the message ID for a named extension (BEP 10).
    ///
    /// Returns `None` if the extension was not negotiated with this peer.
    pub fn extension_id(&self, name: &str) -> Option<u8> {
        self.extension_ids.get(name).copied()
    }

    /// Return a reference to the full extension ID map.
    pub fn extension_ids(&self) -> &HashMap<String, u8> {
        &self.extension_ids
    }
}

/// Perform the BEP 10 LTEP extension negotiation handshake on a raw TCP stream.
///
/// Sends our supported extensions (`ut_pex`) then reads and parses the
/// remote peer's extension handshake response. Returns the remote peer's
/// extension name → message ID mapping.
///
/// This must be called BEFORE [`TcpStream::into_split`] so that no
/// buffered I/O steals bytes from subsequent wire messages.
async fn perform_ltep_handshake(
    raw_stream: &mut TcpStream, addr: SocketAddr,
) -> Result<HashMap<String, u8>, Error> {
    // Build our LTEP handshake offering ut_pex
    let mut neg = ExtensionNegotiation::new();
    neg.add_extension("ut_pex", 1);
    let payload = bencode::encode(&neg.to_bencode());

    // Send as extended message (ext_id = 0 = handshake)
    let ext_msg = PeerMessage::Extended {
        ext_id: 0,
        data: payload,
    };
    let wire = encode(&ext_msg);

    if let Err(e) = tokio::time::timeout(HANDSHAKE_TIMEOUT, raw_stream.write_all(&wire)).await {
        return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
    }
    if let Err(e) = tokio::time::timeout(HANDSHAKE_TIMEOUT, raw_stream.flush()).await {
        return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
    }

    // Read remote's LTEP handshake response (should be Extended { ext_id: 0 })
    let mut len_buf = [0u8; 4];
    if let Err(e) =
        tokio::time::timeout(HANDSHAKE_TIMEOUT, raw_stream.read_exact(&mut len_buf)).await
    {
        return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
    }
    let msg_len = u32::from_be_bytes(len_buf);
    if msg_len > MAX_MESSAGE_SIZE {
        return Err(Error::new(ErrorKind::PeerConnectionClosed));
    }
    let mut msg_buf = vec![0u8; msg_len as usize];
    if let Err(e) =
        tokio::time::timeout(HANDSHAKE_TIMEOUT, raw_stream.read_exact(&mut msg_buf)).await
    {
        return Err(Error::with_source(ErrorKind::PeerConnectionClosed, e));
    }

    let mut full_msg = len_buf.to_vec();
    full_msg.extend_from_slice(&msg_buf);
    let resp = decode(&full_msg)?;

    match resp {
        PeerMessage::Extended { ext_id: 0, data } => {
            let (val, _) = match bencode::decode(&data) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("invalid LTEP bencode from {}: {}", addr, e);
                    return Ok(HashMap::new());
                }
            };
            let remote_neg = match ExtensionNegotiation::from_bencode(&val) {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!("invalid LTEP dict from {}: {}", addr, e);
                    return Ok(HashMap::new());
                }
            };
            tracing::debug!("LTEP handshake complete with {}: {:?}", addr, remote_neg.m);
            Ok(remote_neg.m)
        }
        PeerMessage::Extended { ext_id, .. } => {
            tracing::warn!("LTEP from {}: expected ext_id=0, got {}", addr, ext_id);
            Ok(HashMap::new())
        }
        _ => {
            tracing::warn!("LTEP from {}: got {:?}", addr, resp);
            Ok(HashMap::new())
        }
    }
}
