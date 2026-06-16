//! Integration tests for async DHT RPC and query functions.
//!
//! Some tests use loopback UDP — run with `--test-threads=1` to avoid
//! port conflicts.
//!
//! Routing table unit tests live in crates/torrent-core/src/dht/mod.rs.

use std::sync::Arc;

use torrent::dht::krpc::{KrpcMessage, TransactionId};
use torrent::dht::{DhtRpc, Node, generate_node_id, krpc};

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

// ── B.3: Server-side query handlers (loopback UDP) ────────────

#[tokio::test]
async fn handle_ping_via_loopback() {
    let server = DhtRpc::new("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let node_id = generate_node_id();
    let node_for_handler = node_id;
    let tid: TransactionId = [0x01, 0x02];

    server.set_query_handler(Arc::new(move |msg: &KrpcMessage, _src| {
        if let KrpcMessage::Query {
            transaction_id,
            method,
            ..
        } = msg
        {
            if method == "ping" {
                return Some(krpc::build_ping_response(
                    *transaction_id,
                    &node_for_handler,
                ));
            }
        }
        None
    }));

    let server_addr = server.local_addr().unwrap();
    let client = DhtRpc::new("127.0.0.1:0".parse().unwrap()).await.unwrap();

    let response = client.ping(server_addr, tid, &node_id).await.unwrap();
    match response {
        KrpcMessage::Response { .. } => {} // success
        _ => panic!("expected Response"),
    }
}

#[tokio::test]
async fn handle_find_node_via_loopback() {
    let server = DhtRpc::new("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let node_id = generate_node_id();
    let target = [0xABu8; 20];
    let tid: TransactionId = [0x03, 0x04];
    let node_for_handler = node_id;
    let node_for_query = node_id;

    server.set_query_handler(Arc::new(move |msg: &KrpcMessage, _src| {
        if let KrpcMessage::Query {
            transaction_id,
            method,
            ..
        } = msg
        {
            if method == "find_node" {
                let n = vec![Node {
                    id: node_for_handler,
                    addr: "127.0.0.1:6881".parse().unwrap(),
                }];
                return Some(krpc::build_find_node_response(
                    *transaction_id,
                    &node_for_handler,
                    &n,
                ));
            }
        }
        None
    }));

    let server_addr = server.local_addr().unwrap();
    let client = DhtRpc::new("127.0.0.1:0".parse().unwrap()).await.unwrap();

    let response = client
        .query(
            server_addr,
            tid,
            &krpc::build_find_node(tid, &node_for_query, &target),
        )
        .await
        .unwrap();
    match response {
        KrpcMessage::Response { .. } => {} // success
        _ => panic!("expected Response"),
    }
}

#[tokio::test]
async fn dht_rpc_concurrent_queries() {
    let server = DhtRpc::new("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let node_id = generate_node_id();
    let node = node_id;
    server.set_query_handler(Arc::new(move |msg: &KrpcMessage, _src| {
        if let KrpcMessage::Query {
            transaction_id,
            method,
            ..
        } = msg
        {
            if method == "ping" {
                return Some(krpc::build_ping_response(*transaction_id, &node));
            }
        }
        None
    }));

    let server_addr = server.local_addr().unwrap();
    let client = DhtRpc::new("127.0.0.1:0".parse().unwrap()).await.unwrap();

    // Fire 3 concurrent pings
    let mut handles = Vec::new();
    for i in 0u8..3 {
        let tid: TransactionId = [0x10 + i, 0x20];
        let nid = node_id;
        handles.push(tokio::spawn({
            let client = client.clone();
            async move { client.ping(server_addr, tid, &nid).await }
        }));
    }

    for handle in handles {
        let result = handle.await.unwrap().unwrap();
        match result {
            KrpcMessage::Response { .. } => {} // OK
            _ => panic!("expected Response"),
        }
    }
}

#[tokio::test]
async fn dht_rpc_query_timeout() {
    use std::time::Duration;

    let timeout = Duration::from_secs(1); // 1s
    let client = DhtRpc::with_timeout("127.0.0.1:0".parse().unwrap(), timeout)
        .await
        .unwrap();
    let tid: TransactionId = [0xFF, 0xFF];
    let node_id = [0u8; 20];
    // Port 1 is privileged — no DHT node responds there
    let unreachable = "127.0.0.1:1".parse().unwrap();

    let result = client.ping(unreachable, tid, &node_id).await;
    assert!(result.is_err());
}
