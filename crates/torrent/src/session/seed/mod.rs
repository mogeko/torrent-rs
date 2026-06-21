//! Seeding — create torrents from local data and serve them to peers.
//!
//! This module is the counterpart to the download module. While the
//! download module pulls data from peers and writes it to disk, the
//! seed module reads existing data from disk, computes piece hashes,
//! generates a [`Metainfo`](crate::metainfo::Metainfo), and serves data
//! to requesting peers.
//!
//! # Key Types
//!
//! - [`DataSource`] — trait for reading raw bytes from any backend
//! - `SeedBuilder` — configures and creates a torrent from a data source
//!   (to be implemented in Phase 4)
//! - `SeededTorrent` — the result of hashing (to be implemented in Phase 4)

pub mod source;

pub use self::source::DataSource;
