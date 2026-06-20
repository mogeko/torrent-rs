//! High-level BitTorrent library.
//!
//! This crate provides the user-facing API for building BitTorrent clients.
//! It depends on [`torrent_core`] for all low-level data types and adds
//! async I/O via tokio for networking, file storage, and session management.
//!
//! # Re-exports
//!
//! Commonly used core types are re-exported for convenience:
//! - [`bencode`], [`error`], [`magnet`], [`metainfo`], [`piece`] from `torrent_core`
//!
//! # Quick Start
//!
//! ```no_run
//! use torrent::session::{Session, SessionConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = SessionConfig::default();
//! let session = Session::new(config).await.unwrap();
//!
//! let data = std::fs::read("torrent.torrent").unwrap();
//! let info_hash = session
//!     .add_torrent_bytes(&data).unwrap()
//!     .download_dir("./downloads")
//!     .start().await.unwrap();
//! # Ok(())
//! # }
//! ```

// Re-export key core types so users only need `torrent` as a dependency.
pub use torrent_core::{bencode, error, magnet, metainfo, piece, spec};

pub mod dht;
pub mod peer;
pub mod session;
pub mod storage;
pub mod tracker;

// Re-export commonly-used types at the crate root for convenience.
pub use peer::PeerId;

/// Client identifier sent in BEP 10 LTEP handshakes.
///
/// Defaults to `"torrent-rs <version>"`.  Library consumers building a
/// custom client should override via the `TORRENT_CLIENT_VERSION`
/// environment variable at compile time:
///
/// ```bash
/// TORRENT_CLIENT_VERSION="MyApp/2.0" cargo build
/// ```
pub const CLIENT_VERSION: &str = match option_env!("TORRENT_CLIENT_VERSION") {
    Some(v) => v,
    None => concat!("torrent-rs ", env!("CARGO_PKG_VERSION")),
};
