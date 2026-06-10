mod handshake;
mod message;
mod stream;

use std::fmt;

use rand::Rng;

/// A 20-byte peer identifier (BEP 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId(pub [u8; 20]);

impl PeerId {
    /// Generate a random Azureus-style peer ID.
    ///
    /// Format: `-TR1000-<12 random alphanumeric chars>`.
    pub fn random() -> Self {
        let mut rng = rand::thread_rng();
        let mut bytes = [0u8; 20];
        let prefix = b"-TR1000-";
        bytes[..8].copy_from_slice(prefix);
        const CHARSET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        for byte in bytes.iter_mut().skip(8) {
            let idx = rng.gen_range(0..CHARSET.len());
            *byte = CHARSET[idx];
        }
        PeerId(bytes)
    }
}

impl From<[u8; 20]> for PeerId {
    fn from(bytes: [u8; 20]) -> Self {
        PeerId(bytes)
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

/// Peer connection state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    /// Waiting for or sending handshake.
    Handshake,
    /// Handshake complete, connection initialized.
    Init,
    /// Peer has been unchoked (can request pieces).
    Unchoked,
    /// Peer has choked us (cannot request pieces).
    Choked,
    /// Connection ended.
    Closed,
}

pub use handshake::Handshake;
pub use message::{PeerMessage, decode, encode};
pub use stream::PeerConnection;
