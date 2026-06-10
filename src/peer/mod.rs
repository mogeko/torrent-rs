//! BitTorrent peer wire protocol (BEP 3).
//!
//! This module provides all the primitives needed to communicate with
//! other peers in a BitTorrent swarm:
//! - [`PeerId`]: 20-byte client identifier
//! - [`Handshake`]: 68-byte protocol handshake
//! - [`PeerMessage`]: 11 wire protocol messages
//! - [`PeerConnection`]: async TCP connection with buffered I/O
//!
//! The handshake and message types are purely data (no I/O), making them
//! usable in both sync and async contexts.

mod handshake;
mod message;
mod stream;

use std::fmt;

use rand::Rng;

/// A 20-byte peer identifier (BEP 3).
///
/// Peer IDs are used to uniquely identify BitTorrent clients on a swarm.
/// The [`PeerId::random`] method generates an Azureus-style ID with the
/// format `-TR1000-<12 random alphanumeric chars>`.
///
/// # Examples
///
/// ```
/// use torrent::peer::PeerId;
///
/// let peer_id = PeerId::random();
/// assert_eq!(peer_id.0.len(), 20);
/// assert_eq!(&peer_id.0[..8], b"-TR1000-");
/// ```
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
