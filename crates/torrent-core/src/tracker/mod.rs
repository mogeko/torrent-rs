//! Tracker announce data types (BEP 3, 15, 23).
//!
//! This module provides the shared data types for tracker communication:
//! - [`AnnounceRequest`] — parameters for tracker announce
//! - [`AnnounceResponse`] — parsed tracker response
//! - [`AnnounceEvent`] — started/stopped/completed event
//!
//! Async HTTP and UDP tracker implementations live in the `torrent` crate.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::bencode::{self, Bencode, dict_get, dict_get_bytes, dict_get_int};
use crate::error::{Error, ErrorKind};
use crate::peer::PeerId;

/// Event sent to the tracker during an announce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AnnounceEvent {
    Started,
    Stopped,
    Completed,
    None,
}

/// Parameters for a tracker announce request.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AnnounceRequest {
    /// The info_hash identifying the torrent.
    pub info_hash: [u8; 20],
    /// Our peer ID.
    pub peer_id: PeerId,
    /// The port we are listening on.
    pub port: u16,
    /// Total bytes uploaded so far.
    pub uploaded: u64,
    /// Total bytes downloaded so far.
    pub downloaded: u64,
    /// Bytes remaining to download.
    pub left: u64,
    /// The current event.
    pub event: AnnounceEvent,
    /// Request compact peer list format (recommended).
    pub compact: bool,
    /// Maximum number of peers to return.
    pub numwant: Option<u32>,
    /// Random key for tracker identification.
    pub key: Option<u32>,
    /// Tracker ID from a previous announce.
    pub trackerid: Option<String>,
}

/// Response from a tracker announce.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AnnounceResponse {
    /// Interval in seconds between announces.
    pub interval: u32,
    /// Number of seeders.
    pub complete: u32,
    /// Number of leechers.
    pub incomplete: u32,
    /// List of peer addresses.
    pub peers: Vec<SocketAddr>,
    /// Warning message from the tracker (optional).
    pub warning_message: Option<String>,
    /// Tracker ID (optional).
    pub tracker_id: Option<String>,
    /// Minimum announce interval (optional).
    pub min_interval: Option<u32>,
}

impl AnnounceRequest {
    /// Create a new `AnnounceRequest` with sensible defaults.
    ///
    /// Defaults: `uploaded = 0`, `downloaded = 0`, `left = 0`,
    /// `event = AnnounceEvent::None`, `compact = true`, `numwant = Some(50)`.
    pub fn new(info_hash: [u8; 20], peer_id: PeerId, port: u16) -> Self {
        AnnounceRequest {
            info_hash,
            peer_id,
            port,
            uploaded: 0,
            downloaded: 0,
            left: 0,
            event: AnnounceEvent::None,
            compact: true,
            numwant: Some(50),
            key: None,
            trackerid: None,
        }
    }
}

impl AnnounceResponse {
    /// Parse an `AnnounceResponse` from a bencoded tracker response body.
    pub fn from_bencode(data: &[u8]) -> Result<Self, Error> {
        tracing::debug!("parsing tracker response");
        let (val, _rest) = bencode::decode(data)?;

        let interval_i64 =
            dict_get_int(&val, b"interval").ok_or(Error::new(ErrorKind::TrackerInvalidResponse))?;
        let interval = u32::try_from(interval_i64)
            .map_err(|_| Error::new(ErrorKind::TrackerInvalidResponse))?;

        let complete = dict_get_int(&val, b"complete")
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(0);

        let incomplete = dict_get_int(&val, b"incomplete")
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(0);

        let warning_message = dict_get(&val, b"warning message").and_then(|v| match v {
            Bencode::Bytes(b) => String::from_utf8(b.to_vec()).ok(),
            _ => None,
        });

        let tracker_id = dict_get(&val, b"tracker id").and_then(|v| match v {
            Bencode::Bytes(b) => String::from_utf8(b.to_vec()).ok(),
            _ => None,
        });

        let min_interval = dict_get_int(&val, b"min interval").and_then(|v| u32::try_from(v).ok());

        let peers = parse_peers(&val)?;

        Ok(AnnounceResponse {
            interval,
            complete,
            incomplete,
            peers,
            warning_message,
            tracker_id,
            min_interval,
        })
    }

    /// Construct an `AnnounceResponse` from raw UDP announce fields.
    ///
    /// Used by the UDP tracker parser in the `torrent` crate.
    pub fn from_udp_fields(
        interval: u32, complete: u32, incomplete: u32, peers: Vec<SocketAddr>,
    ) -> Self {
        AnnounceResponse {
            interval,
            complete,
            incomplete,
            peers,
            warning_message: None,
            tracker_id: None,
            min_interval: None,
        }
    }
}

/// Parse the `peers` field from a tracker response.
///
/// Supports both compact format (binary blob: 6 bytes per IPv4 peer)
/// and list-of-dict format.
///
/// Also parses `peers6` (BEP 7) for compact IPv6 peer lists
/// (18 bytes per peer: 16 IPv6 + 2 port).
fn parse_peers(val: &Bencode) -> Result<Vec<SocketAddr>, Error> {
    // Try compact IPv4 format first (binary string of peer data)
    if let Some(bytes) = dict_get_bytes(val, b"peers")
        && !bytes.is_empty()
    {
        return parse_compact_peers_ipv4(bytes);
    }

    // Try compact IPv6 format (BEP 7) — 18 bytes per peer: 16 IPv6 + 2 port
    if let Some(bytes) = dict_get_bytes(val, b"peers6")
        && !bytes.is_empty()
    {
        let mut peers = Vec::with_capacity(bytes.len() / 18);
        for chunk in bytes.chunks_exact(18) {
            let mut ip_bytes = [0u8; 16];
            ip_bytes.copy_from_slice(&chunk[..16]);
            let ip = Ipv6Addr::from(ip_bytes);
            let port = u16::from_be_bytes([chunk[16], chunk[17]]);
            peers.push(SocketAddr::new(IpAddr::V6(ip), port));
        }
        // Handle trailing incomplete bytes
        if !bytes.chunks_exact(18).remainder().is_empty() {
            return Err(Error::new(ErrorKind::TrackerInvalidResponse));
        }
        return Ok(peers);
    }

    // Try list-of-dict format
    if let Some(Bencode::List(peer_list)) = dict_get(val, b"peers") {
        let mut peers = Vec::with_capacity(peer_list.len());
        for peer in peer_list {
            let ip_str = dict_get(peer, b"ip")
                .and_then(|v| match v {
                    Bencode::Bytes(b) => String::from_utf8(b.to_vec()).ok(),
                    _ => None,
                })
                .unwrap_or_default();
            let port = dict_get_int(peer, b"port").unwrap_or(0) as u16;

            if let Ok(ip) = ip_str.parse::<IpAddr>() {
                peers.push(SocketAddr::new(ip, port));
            } else if let Some(Bencode::Bytes(b)) = dict_get(peer, b"ip")
                && b.len() == 4
            {
                let ip = Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                peers.push(SocketAddr::new(IpAddr::V4(ip), port));
            }
        }
        return Ok(peers);
    }

    // No peers field found, return empty
    Ok(Vec::new())
}

/// Parse compact peer list (6 bytes per peer: 4 IPv4 + 2 port).
///
/// Implements BEP 23: Tracker Returns Compact Peer Lists.
///
/// # Errors
///
/// Returns an error if the data length is not a multiple of 6.
pub fn parse_compact_peers_ipv4(data: &[u8]) -> Result<Vec<SocketAddr>, Error> {
    if !data.len().is_multiple_of(6) {
        return Err(Error::new(ErrorKind::TrackerInvalidResponse));
    }
    data.chunks_exact(6)
        .map(|chunk| {
            let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            Ok(SocketAddr::new(IpAddr::V4(ip), port))
        })
        .collect()
}
