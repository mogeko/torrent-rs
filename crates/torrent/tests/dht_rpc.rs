//! Integration tests for async DHT RPC and query functions.
//!
//! Routing table unit tests live in crates/torrent-core/src/dht/mod.rs.

use torrent::dht::{DhtRpc, krpc};

#[test]
fn krpc_ping_message_builds() {
    let tid: krpc::TransactionId = [0xAB, 0xCD];
    let node_id = [0x42u8; 20];
    let data = krpc::build_ping(tid, &node_id);
    assert!(!data.is_empty());
    // Verify it starts with a bencode dict
    assert_eq!(data[0], b'd');
}

#[test]
fn krpc_find_node_message_builds() {
    let tid: krpc::TransactionId = [0x01, 0x02];
    let node_id = [0xAA; 20];
    let target = [0xBB; 20];
    let data = krpc::build_find_node(tid, &node_id, &target);
    assert_eq!(data[0], b'd');
}

#[test]
fn krpc_get_peers_message_builds() {
    let tid: krpc::TransactionId = [0x01, 0x02];
    let node_id = [0xAA; 20];
    let info_hash = [0xBB; 20];
    let data = krpc::build_get_peers(tid, &node_id, &info_hash);
    assert_eq!(data[0], b'd');
}

#[test]
fn krpc_announce_peer_message_builds() {
    let tid: krpc::TransactionId = [0x01, 0x02];
    let node_id = [0xAA; 20];
    let info_hash = [0xBB; 20];
    let token = b"test_token";
    let data = krpc::build_announce_peer(tid, &node_id, &info_hash, 6881, token);
    assert_eq!(data[0], b'd');
}

#[test]
fn krpc_message_encode_decode_roundtrip() {
    let tid: krpc::TransactionId = [0xAB, 0xCD];
    let node_id = [0x42u8; 20];

    let ping_data = krpc::build_ping(tid, &node_id);
    let decoded = krpc::KrpcMessage::from_bytes(&ping_data).unwrap();

    match decoded {
        krpc::KrpcMessage::Query {
            transaction_id,
            method,
            ..
        } => {
            assert_eq!(transaction_id, tid);
            assert_eq!(&method, "ping");
        }
        _ => panic!("expected Query variant"),
    }
}

#[test]
fn parse_compact_nodes() {
    let mut data = Vec::new();
    data.extend_from_slice(&[1u8; 20]);
    data.extend_from_slice(&[127, 0, 0, 1]);
    data.extend_from_slice(&0x1AE1u16.to_be_bytes());

    let nodes = krpc::parse_compact_nodes(&data);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id, [1u8; 20]);
    assert_eq!(nodes[0].addr, "127.0.0.1:6881".parse().unwrap());
}

#[test]
fn parse_compact_nodes_empty() {
    let nodes = krpc::parse_compact_nodes(&[]);
    assert!(nodes.is_empty());
}

#[tokio::test]
async fn dht_rpc_creation() {
    let rpc = DhtRpc::new("127.0.0.1:0".parse().unwrap()).await;
    assert!(rpc.is_ok());
}
