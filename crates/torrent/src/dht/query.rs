//! High-level DHT query wrappers — BEP 5.
//!
//! Each function accepts a [`DhtRpc`] client, builds the appropriate
//! KRPC message, sends it to the target address, and parses the response.

use std::net::SocketAddr;

use crate::error::{Error, ErrorKind};

use super::{Node, krpc, rpc::DhtRpc};

/// Find nodes close to a target ID (BEP 5 find_node).
///
/// Sends a find_node query and returns the list of closer nodes.
/// Used during DHT bootstrapping and recursive node lookup.
///
/// # Errors
///
/// Returns an error if the RPC call fails or the response is malformed.
pub async fn find_node(
    rpc: &DhtRpc, addr: SocketAddr, tid: krpc::TransactionId, node_id: &[u8; 20], target: &[u8; 20],
) -> Result<Vec<Node>, Error> {
    tracing::debug!("DHT find_node to {}", addr);
    let data = krpc::build_find_node(tid, node_id, target);
    let response = rpc.query(addr, tid, &data).await?;

    match &response {
        krpc::KrpcMessage::Response { result, .. } => {
            if let Some(nodes_bytes) = krpc::dict_get_bytes(result, b"nodes") {
                Ok(krpc::parse_compact_nodes(nodes_bytes))
            } else {
                Err(Error::new(ErrorKind::Protocol))
            }
        }
        _ => Err(Error::new(ErrorKind::Protocol)),
    }
}

/// Get peers for an info_hash from the DHT (BEP 5 get_peers).
///
/// Queries a DHT node for peers sharing the torrent identified by `info_hash`.
/// The response may contain peer addresses (`Values` variant) or
/// closer DHT nodes for continued recursive lookup (`Nodes` variant).
///
/// # Errors
///
/// Returns an error if the RPC call fails or the response is malformed.
pub async fn get_peers(
    rpc: &DhtRpc, addr: SocketAddr, tid: krpc::TransactionId, node_id: &[u8; 20],
    info_hash: &[u8; 20],
) -> Result<krpc::GetPeersResult, Error> {
    tracing::debug!("DHT get_peers to {}", addr);
    let data = krpc::build_get_peers(tid, node_id, info_hash);
    let response = rpc.query(addr, tid, &data).await?;

    krpc::parse_get_peers_response(&response)
}

/// Announce that we are a peer for an info_hash (BEP 5 announce_peer).
///
/// Tells a DHT node that we are downloading the torrent identified by
/// `info_hash` on the given `port`. Requires a `token` obtained from
/// a previous `get_peers` response.
///
/// # Errors
///
/// Returns an error if the RPC call fails or the response is malformed.
pub async fn announce_peer(
    rpc: &DhtRpc, addr: SocketAddr, tid: krpc::TransactionId, node_id: &[u8; 20],
    info_hash: &[u8; 20], port: u16, token: &[u8],
) -> Result<(), Error> {
    tracing::debug!("DHT announce_peer to {} (port {})", addr, port);
    let data = krpc::build_announce_peer(tid, node_id, info_hash, port, token);
    let _response = rpc.query(addr, tid, &data).await?;
    Ok(())
}
