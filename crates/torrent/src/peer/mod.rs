//! Async peer communication.
//!
//! Re-exports sync types from `torrent_core::peer` and provides
//! the async [`PeerConnection`] over TCP and uTP (BEP 29).
//!
//! # Key Types
//!
//! - [`PeerId`], [`Handshake`], [`PeerMessage`], [`PeerState`], [`ExtensionNegotiation`] — re-exported from `torrent_core`
//! - [`PeerConnection`] — async TCP connection with buffered I/O
//! - [`utp`] — uTP UDP-based transport (BEP 29)

mod stream;
pub mod utp;

pub use torrent_core::peer::lsd;
pub use torrent_core::peer::metadata;
pub use torrent_core::peer::pex;
pub use torrent_core::peer::{
    ExtensionNegotiation, Handshake, PeerId, PeerMessage, PeerState, compute_allowed_fast_set,
    decode, encode,
};

pub use self::stream::PeerConnection;
