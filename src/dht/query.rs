use std::net::SocketAddr;

use crate::dht::Node;
use crate::dht::krpc;
use crate::dht::rpc::DhtRpc;
use crate::error::{Error, ErrorKind};

/// Find nodes close to a target ID (BEP 5 find_node).
///
/// Sends a find_node query and returns the list of closer nodes.
pub async fn find_node(
    rpc: &DhtRpc,
    addr: SocketAddr,
    tid: krpc::TransactionId,
    node_id: &[u8; 20],
    target: &[u8; 20],
) -> Result<Vec<Node>, Error> {
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
pub async fn get_peers(
    rpc: &DhtRpc,
    addr: SocketAddr,
    tid: krpc::TransactionId,
    node_id: &[u8; 20],
    info_hash: &[u8; 20],
) -> Result<krpc::GetPeersResult, Error> {
    let data = krpc::build_get_peers(tid, node_id, info_hash);
    let response = rpc.query(addr, tid, &data).await?;

    krpc::parse_get_peers_response(&response)
}

/// Announce that we are a peer for an info_hash (BEP 5 announce_peer).
pub async fn announce_peer(
    rpc: &DhtRpc,
    addr: SocketAddr,
    tid: krpc::TransactionId,
    node_id: &[u8; 20],
    info_hash: &[u8; 20],
    port: u16,
    token: &[u8],
) -> Result<(), Error> {
    let data = krpc::build_announce_peer(tid, node_id, info_hash, port, token);
    let _response = rpc.query(addr, tid, &data).await?;
    Ok(())
}
