//! High-level BitTorrent library.
//!
//! This crate provides the user-facing API for building BitTorrent clients.
//! It depends on [`torrent_core`] for all low-level data types and adds
//! async I/O via tokio for networking, file storage, and session management.
//!
//! # Re-exports
//!
//! Commonly used core types are re-exported for convenience:
//! - [`bencode`], [`error`], [`metainfo`], [`magnet`] from `torrent_core`
//!
//! # Quick Start
//!
//! ```no_run
//! use std::path::PathBuf;
//! use torrent::session::{Session, SessionConfig};
//!
//! # async fn example() {
//! let config = SessionConfig {
//!     download_dir: PathBuf::from("./downloads"),
//!     ..Default::default()
//! };
//! let session = Session::new(config).await.unwrap();
//!
//! let data = std::fs::read("torrent.torrent").unwrap();
//! let info_hash = session.add_torrent_bytes(&data).await.unwrap();
//! # }
//! ```

// Re-export key core types so users only need `torrent` as a dependency.
pub use torrent_core::{bencode, error, magnet, metainfo};

pub mod dht;
pub mod peer;
pub mod session;
pub mod storage;
pub mod tracker;
