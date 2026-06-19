//! Async peer communication.
//!
//! Re-exports sync types from `torrent_core::peer` and provides
//! the async [`PeerConnection`] over TCP.
//!
//! # Key Types
//!
//! - [`PeerId`], [`Handshake`], [`PeerMessage`], [`PeerState`], [`ExtensionNegotiation`] — re-exported from `torrent_core`
//! - [`PeerConnection`] — async TCP connection with buffered I/O

mod stream;

pub use torrent_core::peer::{
    ExtensionNegotiation, Handshake, PeerId, PeerMessage, PeerState, PexMessage, decode, encode,
};

pub use self::stream::PeerConnection;
