//! uTP (Micro Transport Protocol) — BEP 29 async I/O layer.
//!
//! This module provides the async UDP-based transport layer for uTP,
//! built on top of the sync protocol types from `torrent_core::peer::utp`.
//!
//! # Architecture
//!
//! - [`UtpSocket`] manages a single UDP socket for all uTP connections,
//!   dispatching incoming packets by `connection_id`.
//! - [`UtpConnection`] handles per-connection state: sequence numbers,
//!   congestion control, retransmission, and packet reassembly.

pub mod connection;
pub mod socket;

pub use torrent_core::peer::utp::congestion;
pub use torrent_core::peer::utp::header;
pub use torrent_core::peer::utp::selective_ack;
pub use torrent_core::peer::utp::{SelectiveAck, UtpCongestionControl, UtpHeader, UtpType};

pub use self::connection::UtpConnection;
pub use self::socket::UtpSocket;
