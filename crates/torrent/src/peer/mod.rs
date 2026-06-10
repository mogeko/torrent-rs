//! Async peer communication.
//!
//! Re-exports sync types from `torrent_core::peer` and provides
//! the async [`PeerConnection`] over TCP.
//!
//! # Key Types
//!
//! - [`PeerId`], [`Handshake`], [`PeerMessage`], [`PeerState`] — re-exported from `torrent_core`
//! - [`PeerConnection`] — async TCP connection with buffered I/O

pub use torrent_core::peer::{Handshake, PeerId, PeerMessage, PeerState, decode, encode};

mod stream;

pub use stream::PeerConnection;
