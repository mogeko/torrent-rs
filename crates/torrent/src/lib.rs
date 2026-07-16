//! High-level async BitTorrent library — the only dependency most users need.
//!
//! Built on [`torrent_core`] for protocol primitives, this crate adds
//! tokio-based async I/O: networking, file storage, DHT, tracker
//! communication, and session orchestration.
//!
//! # Primary API: [`session`]
//!
//! **[`session`] is the main entry point.** [`Session`] orchestrates all
//! BitTorrent activity — downloading, seeding, peer management, tracker
//! communication, DHT, and LSD — through a single high-level API.
//! Start here for almost all use cases.
//!
//! [`Session`]: session::Session
//!
//! ## Download a torrent
//!
//! ```no_run
//! use torrent::session::{Session, SessionConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let session = Session::new(SessionConfig::default()).await?;
//!
//! let data = std::fs::read("my.torrent")?;
//! session
//!     .add_torrent_bytes(&data)?
//!     .download_dir("./downloads")
//!     .start()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Seed a torrent
//!
//! ```no_run
//! use torrent::session::{Session, SessionConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let session = Session::new(SessionConfig::default()).await?;
//!
//! session
//!     .seed_from(std::path::PathBuf::from("./video.mp4"))
//!     .announce("http://tracker.example.com/announce")
//!     .start()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Module Map
//!
//! | Module          | Role                                                | Audience       |
//! |-----------------|-----------------------------------------------------|----------------|
//! | **[`session`]** | Orchestration — download, seed, status, lifecycle   | **All users**  |
//! | [`tracker`]     | HTTP/UDP tracker announce (BEP 3, 15, 23)           | Most users     |
//! | [`storage`]     | File I/O backends — trait + default async disk impl | Custom storage |
//! | [`peer`]        | Raw peer wire protocol (BEP 3, 6, 10, 29)           | Advanced       |
//! | [`dht`]         | DHT node + RPC + queries (BEP 5, 32)                | Advanced       |
//! | [`bencode`]     | Bencode encode/decode (BEP 3)                       | Low-level      |
//! | [`metainfo`]    | `.torrent` file parsing (BEP 3, 12, 52)             | Low-level      |
//! | [`magnet`]      | Magnet URI parsing (BEP 9)                          | Low-level      |
//! | [`error`]       | [`Error`] and [`ErrorKind`] types                   | All users      |
//!
//! The [`peer`], [`dht`], and [`tracker`] modules provide direct access to
//! low-level protocol primitives for users who need fine-grained control
//! outside of [`Session`].
//!
//! # Protocol Coverage
//!
//! **BEP 3** wire protocol · **BEP 5** DHT · **BEP 6** Fast Extension ·
//! **BEP 9** magnet + metadata · **BEP 10** LTEP · **BEP 11** PEX ·
//! **BEP 14** LSD · **BEP 15** UDP tracker · **BEP 19** web seed ·
//! **BEP 23** compact tracker · **BEP 29** uTP · **BEP 32** IPv6 DHT ·
//! **BEP 52** bt-v2 metainfo
//!
//! [`Error`]: error::Error
//! [`ErrorKind`]: error::ErrorKind

pub mod dht;
pub(crate) mod net;
pub mod peer;
pub mod session;
pub mod storage;
pub mod tracker;

// Re-export key core types so users only need `torrent` as a dependency.
pub use torrent_core::{bencode, error, magnet, metainfo};
// General-purpose URL handling.
pub use net::{IntoUrl, Url};

// Internal-only core types used by session/tracker internals.
pub(crate) use torrent_core::{piece, spec};

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
