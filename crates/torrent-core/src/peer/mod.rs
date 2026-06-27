//! BitTorrent peer wire protocol (BEP 3, BEP 6, BEP 10).
//!
//! This module provides sync primitives for peer communication:
//! - [`PeerId`]: 20-byte client identifier
//! - [`Handshake`]: 68-byte protocol handshake
//! - [`PeerMessage`]: 17 wire protocol message types (BEP 3 + BEP 6 + BEP 10)
//! - [`ExtensionNegotiation`]: LTEP extension negotiation (BEP 10)
//! - [`compute_allowed_fast_set`]: Fast Extension piece set computation (BEP 6)
//! - [`PeerState`]: connection state machine
//!
//! All types are purely data with no I/O, usable in both sync and async
//! contexts. The async `PeerConnection` type lives in the `torrent` crate.

mod extension;
mod handshake;
pub mod lsd;
mod message;
pub mod metadata;
pub mod pex;

pub use self::extension::ExtensionNegotiation;
pub use self::handshake::Handshake;
pub use self::message::{PeerMessage, compute_allowed_fast_set, decode, encode};

use std::fmt;

use rand::RngExt;

/// A 20-byte peer identifier (BEP 3).
///
/// Peer IDs are used to uniquely identify BitTorrent clients on a swarm.
/// The [`PeerId::random`] method generates an Azureus-style ID with the
/// format `-TR1000-<12 random alphanumeric chars>`.
///
/// # Examples
///
/// ```
/// use torrent_core::peer::PeerId;
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
        let mut rng = rand::rng();
        let mut bytes = [0u8; 20];
        let prefix = b"-TR1000-";
        bytes[..8].copy_from_slice(prefix);
        const CHARSET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        for byte in bytes.iter_mut().skip(8) {
            let idx = rng.random_range(0..CHARSET.len());
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
