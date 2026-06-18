use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{UdpSocket, lookup_host};

use crate::error::{Error, ErrorKind};

use super::{AnnounceEvent, AnnounceRequest, AnnounceResponse, IntoUrl, Url};

/// Per-request timeout (connect + announce).
use super::DEFAULT_TIMEOUT;

/// Magic connection ID constant used during the connection phase.
const INITIAL_CONNECTION_ID: u64 = 0x41727101980;

/// Max retries for connect and announce phases (BEP 15: up to 4 retries).
const MAX_RETRIES: u32 = 4;

/// Receive buffer size — enough for compact peer lists with ~200 peers.
const RECV_BUF_SIZE: usize = 2048;

/// UDP tracker client (BEP 15).
#[derive(Debug, Clone)]
pub struct UdpTracker {
    url: Url,
    /// Cached connection ID per BEP 15 (reuse across announces until failure).
    connection_id: Arc<Mutex<Option<u64>>>,
    /// Cached resolved address to avoid repeated DNS lookups.
    cached_addr: Arc<Mutex<Option<SocketAddr>>>,
    /// Per-request timeout.
    timeout: Duration,
}

impl UdpTracker {
    /// Create a new UDP tracker client with the default 15 s timeout.
    ///
    /// `url` must be a `udp://` URL (e.g. `udp://tracker.example.com:6969`).
    /// Accepts `&str`, `String`, `&String`, or `Url`.
    pub fn new(url: impl IntoUrl) -> Result<Self, Error> {
        Self::with_timeout(url, DEFAULT_TIMEOUT)
    }

    /// Create a new UDP tracker client with a custom timeout.
    pub fn with_timeout(url: impl IntoUrl, timeout: Duration) -> Result<Self, Error> {
        let url = url.into_url()?;

        if url.scheme() != "udp" {
            return Err(Error::new(ErrorKind::InvalidInput));
        }

        Ok(UdpTracker {
            url,
            connection_id: Arc::new(Mutex::new(None)),
            cached_addr: Arc::new(Mutex::new(None)),
            timeout,
        })
    }

    /// Returns the tracker's URL.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Announce to the UDP tracker.
    ///
    /// Resolves all addresses and tries each one in sequence (multi-homed
    /// tracker support).  Uses the BEP 15 two-phase connect+announce protocol
    /// with retries on both phases. Connection ID is cached across announces
    /// per BEP 15 and re-fetched only on timeout.
    pub async fn announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponse, Error> {
        tracing::info!("UDP announce to {}", self.url);
        let Some(host) = self.url.host_str() else {
            return Err(Error::new(ErrorKind::InvalidInput));
        };
        let port = self.url.port().unwrap_or(6969);

        let event = match req.event {
            AnnounceEvent::None => 0u32,
            AnnounceEvent::Completed => 1u32,
            AnnounceEvent::Started => 2u32,
            AnnounceEvent::Stopped => 3u32,
            _ => 0u32,
        };

        let mut last_err = None;

        // Use cached address if available, otherwise resolve hostname.
        let target_addrs: Vec<SocketAddr> = {
            // Check cache without holding lock across await
            let cached = *self.cached_addr.lock().unwrap();
            if let Some(addr) = cached {
                // Cache hit: drop lock before any await
                vec![addr]
            } else {
                // Cache miss: drop lock, then resolve
                let addrs: Vec<SocketAddr> = lookup_host((host, port))
                    .await
                    .map_err(Error::tracker_failed)?
                    .collect();
                if let Some(first) = addrs.first() {
                    *self.cached_addr.lock().unwrap() = Some(*first);
                }
                addrs
            }
        };

        for addr in target_addrs {
            // Bind a socket matching this address family.
            let bind_addr = SocketAddr::new(
                if addr.is_ipv4() {
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
                } else {
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED)
                },
                0,
            );
            let socket = match UdpSocket::bind(bind_addr).await {
                Ok(s) => s,
                Err(e) => {
                    last_err = Some(Error::tracker_failed(e));
                    continue;
                }
            };

            // Phase 1: Connect (use cached connection_id if available)
            let connection_id = if let Some(cached) = *self.connection_id.lock().unwrap() {
                cached
            } else {
                match connect(&socket, addr, self.timeout).await {
                    Ok(id) => {
                        *self.connection_id.lock().unwrap() = Some(id);
                        id
                    }
                    Err(e) => {
                        last_err = Some(e);
                        continue;
                    }
                }
            };

            // Phase 2: Announce (with retries — UDP is unreliable)
            let transaction_id = rand::random::<u32>();
            let announce_packet = build_announce_packet(connection_id, transaction_id, req, event);

            for _ in 0..MAX_RETRIES {
                if let Err(e) = socket.send_to(&announce_packet, addr).await {
                    last_err = Some(Error::tracker_failed(e));
                    break;
                }

                let mut buf = vec![0u8; RECV_BUF_SIZE];
                match tokio::time::timeout(self.timeout, socket.recv_from(&mut buf)).await {
                    Ok(Ok((len, src))) => {
                        if src != addr {
                            continue;
                        }
                        match parse_announce_response(&buf[..len], transaction_id) {
                            Ok(response) => return Ok(response),
                            Err(e) => {
                                // Connection may have expired; clear cache for next attempt
                                *self.connection_id.lock().unwrap() = None;
                                last_err = Some(e);
                                break;
                            }
                        }
                    }
                    _ => continue,
                }
            }
        }

        Err(last_err.unwrap_or_else(|| Error::new(ErrorKind::TrackerRequestFailed)))
    }
}

/// Connect phase: obtain a connection ID from the tracker.
async fn connect(socket: &UdpSocket, addr: SocketAddr, timeout: Duration) -> Result<u64, Error> {
    let transaction_id = rand::random::<u32>();

    let connect_packet = build_connect_packet(transaction_id);

    for _ in 0..MAX_RETRIES {
        socket
            .send_to(&connect_packet, addr)
            .await
            .map_err(Error::tracker_failed)?;

        let mut buf = vec![0u8; 16];
        match tokio::time::timeout(timeout, socket.recv_from(&mut buf)).await {
            Ok(Ok((len, src))) => {
                if src != addr {
                    continue;
                }
                return parse_connect_response(&buf[..len], transaction_id);
            }
            _ => continue,
        }
    }

    Err(Error::new(ErrorKind::TrackerRequestFailed))
}

/// Build a UDP connect request packet.
fn build_connect_packet(transaction_id: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    buf.extend_from_slice(&INITIAL_CONNECTION_ID.to_be_bytes()); // connection_id (magic)
    buf.extend_from_slice(&0u32.to_be_bytes()); // action = 0 (connect)
    buf.extend_from_slice(&transaction_id.to_be_bytes());
    buf
}

/// Build a UDP announce request packet (BEP 15).
fn build_announce_packet(
    connection_id: u64, transaction_id: u32, req: &AnnounceRequest, event: u32,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(98);
    buf.extend_from_slice(&connection_id.to_be_bytes()); // 8
    buf.extend_from_slice(&1u32.to_be_bytes()); // action = 1 (announce), 4
    buf.extend_from_slice(&transaction_id.to_be_bytes()); // 4
    buf.extend_from_slice(&req.info_hash); // 20
    buf.extend_from_slice(&req.peer_id.0); // 20
    buf.extend_from_slice(&req.downloaded.to_be_bytes()); // 8
    buf.extend_from_slice(&req.left.to_be_bytes()); // 8
    buf.extend_from_slice(&req.uploaded.to_be_bytes()); // 8
    buf.extend_from_slice(&event.to_be_bytes()); // 4
    buf.extend_from_slice(&0u32.to_be_bytes()); // IP (0 = auto), 4
    buf.extend_from_slice(&req.key.unwrap_or(0).to_be_bytes()); // 4
    buf.extend_from_slice(&(req.numwant.unwrap_or(50) as i32).to_be_bytes()); // 4
    buf.extend_from_slice(&req.port.to_be_bytes()); // 2
    buf
}

/// Parse a UDP connect response.
fn parse_connect_response(data: &[u8], expected_transaction_id: u32) -> Result<u64, Error> {
    if data.len() < 16 {
        return Err(Error::new(ErrorKind::TrackerProtocolError));
    }
    let action = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if action == 3 {
        // BEP 15 error response: [action:32][transaction_id:32][message:...]
        let _msg = String::from_utf8_lossy(&data[8..]);
        return Err(Error::new(ErrorKind::TrackerProtocolError));
    }
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
    data: &[u8], expected_transaction_id: u32,
) -> Result<AnnounceResponse, Error> {
    if data.len() < 20 {
        return Err(Error::new(ErrorKind::TrackerProtocolError));
    }
    let action = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if action == 3 {
        // BEP 15 error response: [action:32][transaction_id:32][message:...]
        let _msg = String::from_utf8_lossy(&data[8..]);
        return Err(Error::new(ErrorKind::TrackerProtocolError));
    }
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

    Ok(AnnounceResponse::from_udp_fields(
        interval, seeders, leechers, peers,
    ))
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

    #[test]
    fn test_new_invalid_scheme() {
        assert!(UdpTracker::new("http://tracker.example.com:6969").is_err());
    }

    #[test]
    fn test_new_invalid_url() {
        assert!(UdpTracker::new("not-a-url").is_err());
    }
}
