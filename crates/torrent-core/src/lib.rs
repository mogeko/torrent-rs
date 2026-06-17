//! Low-level core abstractions for the BitTorrent protocol.
//!
//! This crate provides the fundamental data types and algorithms needed
//! for BitTorrent communication, without any async I/O. All types are
//! fully synchronous and have no runtime dependencies.
//!
//! # Modules
//!
//! - [`bencode`] — BEP 3 encode/decode
//! - [`error`] — Error + ErrorKind
//! - [`metainfo`] — .torrent parsing (BEP 3/12/52)
//! - [`magnet`] — Magnet URI (BEP 9)
//! - [`peer`] — handshake, message types, PeerId
//! - [`piece`] — PieceManager, piece selection strategies (BEP 3)
//! - [`spec`] — Unified torrent specification (metainfo or magnet)
//! - [`storage`] — Storage trait
//! - [`tracker`] — Announce data types and parsing (sync)

pub mod bencode;
pub mod dht;
pub mod error;
pub mod magnet;
pub mod metainfo;
pub mod peer;
pub mod piece;
pub mod spec;
pub mod storage;
pub mod tracker;
