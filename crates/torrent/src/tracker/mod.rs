//! Async tracker implementations — HTTP and UDP announce.
//!
//! Re-exports data types from `torrent_core::tracker` and provides
//! async HTTP and UDP tracker clients.
//!
//! # Key Types
//!
//! - [`AnnounceRequest`], [`AnnounceResponse`], [`AnnounceEvent`] — re-exported from `torrent_core`
//! - [`HttpTracker`] — HTTP GET announce (BEP 3/23)
//! - [`UdpTracker`] — UDP announce (BEP 15)

pub use torrent_core::tracker::{
    AnnounceEvent, AnnounceRequest, AnnounceResponse, parse_compact_peers_ipv4,
};

mod http;
mod udp;

pub use http::HttpTracker;
pub use udp::UdpTracker;
