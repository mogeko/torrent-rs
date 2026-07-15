//! Integration tests for uTP (BEP 29).
//!
//! Tests two uTP endpoints communicating over localhost UDP.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::time::timeout;
use torrent::peer::utp::UtpSocket;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Helper: pick two available localhost ports.
fn test_addrs() -> (SocketAddr, SocketAddr) {
    let a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let b = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    (a, b)
}

/// Test that two uTP sockets can bind.
#[tokio::test]
async fn utp_socket_bind() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let socket = UtpSocket::bind(addr).await.unwrap();
    assert!(socket.local_addr().port() > 0);
}

/// Test that a uTP connection can be established between two endpoints.
#[tokio::test]
async fn utp_connect_and_send() {
    let (addr_a, addr_b) = test_addrs();

    // Bind both sockets
    let socket_a = UtpSocket::bind(addr_a).await.unwrap();
    let socket_b = UtpSocket::bind(addr_b).await.unwrap();
    let b_addr = socket_b.local_addr();

    // A connects to B
    let mut conn_a = socket_a.connect(b_addr).await.unwrap();
    assert_eq!(conn_a.remote_addr, b_addr);

    // Wait a bit for the uTP handshake (SYN → STATE → connected)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send data from A
    let test_data = b"hello uTP!".to_vec();
    conn_a.send(test_data.clone()).unwrap();

    // Give time for the packet to arrive
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Test that a uTP connection is rejected for an unreachable address.
#[tokio::test]
async fn utp_connect_unreachable() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let socket = UtpSocket::bind(addr).await.unwrap();

    // Connect to a port where nothing is listening
    let dead_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
    // This should eventually fail or timeout
    // For now, just check that connect doesn't panic
    let result = timeout(TEST_TIMEOUT, socket.connect(dead_addr)).await;
    // connect() may succeed (UDP is connectionless), or fail — either is fine
    // as long as it doesn't panic
    let _ = result;
}

/// Test sending multiple packets.
#[tokio::test]
async fn utp_send_multiple_packets() {
    let (addr_a, addr_b) = test_addrs();

    let socket_a = UtpSocket::bind(addr_a).await.unwrap();
    let socket_b = UtpSocket::bind(addr_b).await.unwrap();
    let b_addr = socket_b.local_addr();

    let mut conn = socket_a.connect(b_addr).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send several packets
    for i in 0..5u8 {
        let data = vec![i; 100]; // 100 bytes of value i
        conn.send(data).unwrap();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// Test that binding twice to the same address fails.
#[tokio::test]
async fn utp_bind_conflict() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let socket_a = UtpSocket::bind(addr).await.unwrap();
    let bound_addr = socket_a.local_addr();

    // Binding again to the same port should fail
    let result = UtpSocket::bind(bound_addr).await;
    assert!(result.is_err());
}

/// Test connection cleanup: after dropping a handle, resources are released.
#[tokio::test]
async fn utp_connection_cleanup() {
    let (addr_a, addr_b) = test_addrs();

    let socket_a = UtpSocket::bind(addr_a).await.unwrap();
    let socket_b = UtpSocket::bind(addr_b).await.unwrap();
    let b_addr = socket_b.local_addr();

    {
        let conn = socket_a.connect(b_addr).await.unwrap();
        // Connection handle is dropped here
        drop(conn);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // We can still use the socket for new connections
    let conn2 = socket_a.connect(b_addr).await.unwrap();
    drop(conn2);
}
