//! A pure Rust BitTorrent library comparable in scope to libtorrent.
//!
//! # Architecture
//!
//! ```text
//! bencode ─── metainfo ─── peer ─── session
//!                 │           │         │
//!                 └── magnet   ├── tracker
//!                              │
//!                              └── dht
//!               storage ───────────────┘
//! ```
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

pub mod bencode;
pub mod dht;
pub mod error;
pub mod magnet;
pub mod metainfo;
pub mod peer;
pub mod session;
pub mod storage;
pub mod tracker;
