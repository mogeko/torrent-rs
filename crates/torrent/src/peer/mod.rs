//! Async peer communication.
//!
//! Re-exports sync types from `torrent_core::peer` and provides
//! the async [`PeerConnection`] over TCP and uTP (BEP 29).
//!
//! uTP is managed transparently via [`SessionConfig::enable_utp`]
//! and requires no direct type imports from this module.
//!
//! # Key Types
//!
//! - [`PeerId`], [`Handshake`], [`PeerMessage`], [`PeerState`], [`ExtensionNegotiation`] — re-exported from `torrent_core`
//! - [`PeerConnection`] — async TCP connection with buffered I/O
//!
//! [`SessionConfig::enable_utp`]: crate::session::SessionConfig#structfield.enable_utp

mod stream;
pub(crate) mod utp;

pub use torrent_core::peer::lsd;
pub use torrent_core::peer::metadata;
pub use torrent_core::peer::pex;
pub use torrent_core::peer::{
    ExtensionNegotiation, Handshake, PeerId, PeerMessage, PeerState, compute_allowed_fast_set,
    decode, encode,
};

pub use self::stream::PeerConnection;
