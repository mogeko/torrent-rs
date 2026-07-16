//! uTP (Micro Transport Protocol) — BEP 29 async I/O layer.
//!
//! This module provides the async UDP-based transport layer for uTP,
//! built on top of the sync protocol types from `torrent_core::peer::utp`.
//!
//! # Architecture
//!
//! A single UDP socket (managed by the internal `UtpSocket`) dispatches
//! incoming packets by `connection_id` to per-connection state machines
//! (`UtpConnection`). The async internals are `pub(crate)` and managed
//! transparently by [`Session`].
//!
//! For protocol data types (`UtpHeader`, `UtpType`, `UtpCongestionControl`,
//! `SelectiveAck`), depend on `torrent-core` directly.
//!
//! [`Session`]: crate::session::Session

mod connection;
mod socket;
mod stream;

pub(crate) use self::socket::UtpSocket;
pub(crate) use self::stream::UtpStream;

use torrent_core::peer::utp::{UtpCongestionControl, UtpHeader, UtpType};
