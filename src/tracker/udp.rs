use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::UdpSocket;

use crate::error::{Error, ErrorKind};
use crate::tracker::{AnnounceEvent, AnnounceRequest, AnnounceResponse};

/// Magic connection ID constant used during the connection phase.
const INITIAL_CONNECTION_ID: u64 = 0x41727101980;

/// UDP datagram buffer size.
const UDP_BUF_SIZE: usize = 2048;

/// Connection timeout.
const TIMEOUT: Duration = Duration::from_secs(15);

/// Max retries for connection.
const MAX_RETRIES: u32 = 3;

/// UDP tracker client (BEP 15).
pub struct UdpTracker {
    addr: SocketAddr,
}

impl UdpTracker {
    /// Create a new UDP tracker client for a given tracker URL.
    pub fn new(url: &str) -> Result<Self, Error> {
        // Strip udp:// prefix
        let addr_str = url
            .strip_prefix("udp://")
            .ok_or(Error::new(ErrorKind::InvalidInput))?;

        let addr: SocketAddr = addr_str
            .parse()
            .map_err(|_| Error::new(ErrorKind::InvalidInput))?;

        Ok(UdpTracker { addr })
    }

    /// Announce to the UDP tracker.
    pub async fn announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponse, Error> {
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

        // Phase 1: Connect to get connection ID
        let connection_id = connect(&socket, self.addr).await?;

        // Phase 2: Announce
        let event = match req.event {
            AnnounceEvent::None => 0u32,
            AnnounceEvent::Completed => 1u32,
            AnnounceEvent::Started => 2u32,
            AnnounceEvent::Stopped => 3u32,
        };

        let transaction_id = rand::random::<u32>();

        // Build announce request packet (BEP 15)
        let mut announce_packet = Vec::with_capacity(98);
        announce_packet.extend_from_slice(&connection_id.to_be_bytes()); // 8
        announce_packet.extend_from_slice(&1u32.to_be_bytes()); // action = 1 (announce), 4
        announce_packet.extend_from_slice(&transaction_id.to_be_bytes()); // 4
        announce_packet.extend_from_slice(&req.info_hash); // 20
        announce_packet.extend_from_slice(&req.peer_id.0); // 20
        announce_packet.extend_from_slice(&req.downloaded.to_be_bytes()); // 8
        announce_packet.extend_from_slice(&req.left.to_be_bytes()); // 8
        announce_packet.extend_from_slice(&req.uploaded.to_be_bytes()); // 8
        announce_packet.extend_from_slice(&event.to_be_bytes()); // 4
        announce_packet.extend_from_slice(&0u32.to_be_bytes()); // IP (0 = auto), 4
        announce_packet.extend_from_slice(&req.key.unwrap_or(0).to_be_bytes()); // 4
        announce_packet.extend_from_slice(&(req.numwant.unwrap_or(50) as i32).to_be_bytes()); // 4
        announce_packet.extend_from_slice(&req.port.to_be_bytes()); // 2

        socket
            .send_to(&announce_packet, self.addr)
            .await
            .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

        let mut buf = [0u8; UDP_BUF_SIZE];
        let (len, _src) = tokio::time::timeout(TIMEOUT, socket.recv_from(&mut buf))
            .await
            .map_err(|_| Error::new(ErrorKind::TrackerRequestFailed))?
            .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

        parse_announce_response(&buf[..len], transaction_id)
    }
}

/// Connect phase: obtain a connection ID from the tracker.
async fn connect(socket: &UdpSocket, addr: SocketAddr) -> Result<u64, Error> {
    let transaction_id = rand::random::<u32>();

    let connect_packet = build_connect_packet(transaction_id);

    let mut buf = [0u8; 16];
    let mut retries = 0;

    loop {
        socket
            .send_to(&connect_packet, addr)
            .await
            .map_err(|e| Error::with_source(ErrorKind::TrackerRequestFailed, e))?;

        match tokio::time::timeout(TIMEOUT, socket.recv_from(&mut buf)).await {
            Ok(Ok((len, _src))) => {
                return parse_connect_response(&buf[..len], transaction_id);
            }
            _ => {
                retries += 1;
                if retries >= MAX_RETRIES {
                    return Err(Error::new(ErrorKind::TrackerRequestFailed));
                }
            }
        }
    }
}

/// Build a UDP connect request packet.
fn build_connect_packet(transaction_id: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&INITIAL_CONNECTION_ID.to_be_bytes()); // connection_id (magic)
    buf.extend_from_slice(&0u32.to_be_bytes()); // action = 0 (connect)
    buf.extend_from_slice(&transaction_id.to_be_bytes());
    buf
}

/// Parse a UDP connect response.
fn parse_connect_response(data: &[u8], expected_transaction_id: u32) -> Result<u64, Error> {
    if data.len() < 16 {
        return Err(Error::new(ErrorKind::TrackerProtocolError));
    }
    let action = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if action != 0 {
        return Err(Error::new(ErrorKind::TrackerProtocolError));
    }
    let transaction_id = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    if transaction_id != expected_transaction_id {
        return Err(Error::new(ErrorKind::TrackerProtocolError));
    }
    let connection_id = u64::from_be_bytes([
        data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
    ]);
    Ok(connection_id)
}

/// Parse a UDP announce response.
fn parse_announce_response(
    data: &[u8],
    expected_transaction_id: u32,
) -> Result<AnnounceResponse, Error> {
    if data.len() < 20 {
        return Err(Error::new(ErrorKind::TrackerProtocolError));
    }
    let action = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if action != 1 {
        return Err(Error::new(ErrorKind::TrackerProtocolError));
    }
    let transaction_id = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    if transaction_id != expected_transaction_id {
        return Err(Error::new(ErrorKind::TrackerProtocolError));
    }
    let interval = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
    let leechers = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);
    let seeders = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);

    let peer_data = &data[20..];

    let peers = super::parse_compact_peers_ipv4(peer_data)?;

    Ok(AnnounceResponse {
        interval,
        complete: seeders,
        incomplete: leechers,
        peers,
        warning_message: None,
        tracker_id: None,
        min_interval: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_connect_packet() {
        let packet = build_connect_packet(0x12345678);
        assert_eq!(packet.len(), 16);
        // Action = 0
        assert_eq!(&packet[8..12], &0u32.to_be_bytes());
        // Transaction ID
        assert_eq!(&packet[12..16], &0x12345678u32.to_be_bytes());
    }

    #[test]
    fn test_parse_connect_response() {
        let conn_id = 0xABCDEF0123456789u64;
        let mut data = Vec::new();
        data.extend_from_slice(&0u32.to_be_bytes()); // action = 0
        data.extend_from_slice(&0x42u32.to_be_bytes()); // transaction_id
        data.extend_from_slice(&conn_id.to_be_bytes());

        let result = parse_connect_response(&data, 0x42).unwrap();
        assert_eq!(result, conn_id);
    }

    #[test]
    fn test_parse_connect_response_wrong_action() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_be_bytes()); // wrong action
        data.extend_from_slice(&0x42u32.to_be_bytes());
        data.extend_from_slice(&0u64.to_be_bytes());
        assert!(parse_connect_response(&data, 0x42).is_err());
    }

    #[test]
    fn test_parse_connect_response_wrong_transaction() {
        let mut data = Vec::new();
        data.extend_from_slice(&0u32.to_be_bytes());
        data.extend_from_slice(&0x99u32.to_be_bytes()); // wrong transaction
        data.extend_from_slice(&0u64.to_be_bytes());
        assert!(parse_connect_response(&data, 0x42).is_err());
    }

    #[test]
    fn test_parse_announce_response() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_be_bytes()); // action = announce
        data.extend_from_slice(&0x42u32.to_be_bytes()); // transaction_id
        data.extend_from_slice(&1800u32.to_be_bytes()); // interval
        data.extend_from_slice(&10u32.to_be_bytes()); // leechers
        data.extend_from_slice(&5u32.to_be_bytes()); // seeders
        // One compact peer: 127.0.0.1:6881
        data.extend_from_slice(&[127, 0, 0, 1, 0x1A, 0xE1]);

        let response = parse_announce_response(&data, 0x42).unwrap();
        assert_eq!(response.interval, 1800);
        assert_eq!(response.incomplete, 10);
        assert_eq!(response.complete, 5);
        assert_eq!(response.peers.len(), 1);
        assert_eq!(
            response.peers[0],
            "127.0.0.1:6881".parse::<SocketAddr>().unwrap()
        );
    }
}
