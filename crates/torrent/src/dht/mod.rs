//! DHT networking — async RPC and query helpers.
//!
//! Re-exports sync types from `torrent_core::dht` and provides
//! async UDP RPC and high-level query functions.
//!
//! # Key Types
//!
//! - [`Node`], [`RoutingTable`], [`krpc::KrpcMessage`] — re-exported from `torrent_core`
//! - [`rpc::DhtRpc`] — async UDP send/receive with transaction matching
//! - [`query::find_node`], [`query::get_peers`], [`query::announce_peer`] — high-level query helpers

pub use torrent_core::dht::{Node, RoutingTable, krpc};

pub mod query;
pub mod rpc;
