//! Local Service Discovery (LSD) message types (BEP 14).
//!
//! LSD provides a mechanism to announce a peer's presence in specific
//! swarms to local neighbors via UDP multicast. It can be used either as
//! a primary peer source for local transfers or to complement other
//! sources which only operate on global unicast addresses.
//!
//! # Key Types
//!
//! - [`LsdHost`] — the multicast group (IPv4 org-local or IPv6 site-local)
//! - [`LsdAnnounce`] — the wire-format announce message
//!
//! # Wire format (HTTP-style headers)
//!
//! ```text
//! BT-SEARCH * HTTP/1.1\r\n
//! Host: <multicast_addr>:<port>\r\n
//! Port: <tcp_port>\r\n
//! Infohash: <40-char-hex>\r\n
//! cookie: <opaque (optional)>\r\n
//! \r\n
//! \r\n
//! ```
//!
//! Multiple `Infohash` headers may be present (up to 1400 bytes total).
//! Unknown headers must be ignored for forward compatibility.

use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr};

use crate::error::{Error, ErrorKind};

// ── Constants ────────────────────────────────────────────────────────────

/// IPv4 org-local LSD multicast address (BEP 14).
pub const LSD_IPV4_MULTICAST: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);

/// IPv6 site-local LSD multicast address (BEP 14).
pub const LSD_IPV6_MULTICAST: Ipv6Addr = Ipv6Addr::new(0xff15, 0, 0, 0, 0, 0, 0xefc0, 0x988f);

/// UDP port used for LSD (BEP 14).
pub const LSD_PORT: u16 = 6771;

/// Maximum announce packet size before truncation (BEP 14 recommends 1400).
const MAX_ANNOUNCE_SIZE: usize = 1400;

// ── LsdHost ──────────────────────────────────────────────────────────────

/// The multicast group to which an LSD announce is sent (BEP 14).
///
/// Two groups are defined:
/// - `V4`: IPv4 org-local (`239.192.152.143:6771`)
/// - `V6`: IPv6 site-local (`[ff15::efc0:988f]:6771`)
///
/// # Examples
///
/// ```
/// use torrent_core::peer::lsd::LsdHost;
///
/// let v4 = LsdHost::V4;
/// assert_eq!(v4.multicast_addr(), "239.192.152.143:6771");
///
/// let v6 = LsdHost::V6;
/// assert_eq!(v6.multicast_addr(), "[ff15::efc0:988f]:6771");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LsdHost {
    /// IPv4 org-local: `239.192.152.143:6771`
    V4,
    /// IPv6 site-local: `[ff15::efc0:988f]:6771`
    V6,
}

impl LsdHost {
    /// The `<host>:<port>` string used in the `Host` header (BEP 14 §Protocol).
    pub fn multicast_addr(self) -> &'static str {
        match self {
            LsdHost::V4 => "239.192.152.143:6771",
            LsdHost::V6 => "[ff15::efc0:988f]:6771",
        }
    }

    /// Parse from a `Host` header value.
    ///
    /// Accepts the canonical forms `239.192.152.143:6771` (IPv4)
    /// and `[ff15::efc0:988f]:6771` (IPv6).
    fn from_host_header(s: &str) -> Option<Self> {
        match s {
            "239.192.152.143:6771" => Some(LsdHost::V4),
            "[ff15::efc0:988f]:6771" => Some(LsdHost::V6),
            _ => None,
        }
    }
}

impl fmt::Display for LsdHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.multicast_addr())
    }
}

// ── LsdAnnounce ──────────────────────────────────────────────────────────

/// An LSD announce message (BEP 14).
///
/// Sent via UDP multicast to announce this peer's participation in one
/// or more torrents on the local network. Received announces are used
/// to discover LAN peers without a tracker or DHT.
///
/// # Wire format
///
/// ```text
/// BT-SEARCH * HTTP/1.1\r\n
/// Host: 239.192.152.143:6771\r\n
/// Port: 6881\r\n
/// Infohash: aabbccdd...\r\n
/// cookie: my-opaque-id\r\n
/// \r\n
/// \r\n
/// ```
///
/// # Examples
///
/// ```
/// use torrent_core::peer::lsd::{LsdAnnounce, LsdHost};
///
/// let announce = LsdAnnounce::new(LsdHost::V4, 6881)
///     .info_hashes(vec![[0u8; 20]]);
///
/// let bytes = announce.to_bytes().unwrap();
/// let parsed = LsdAnnounce::from_bytes(&bytes).unwrap();
/// assert_eq!(announce, parsed);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LsdAnnounce {
    /// The multicast group this announce targets.
    pub host: LsdHost,
    /// TCP port on which the client is listening.
    pub port: u16,
    /// Info hashes of torrents the client participates in.
    pub info_hashes: Vec<[u8; 20]>,
    /// Optional opaque cookie for filtering loopback announces.
    pub cookie: Option<String>,
}

impl LsdAnnounce {
    /// Create an LSD announce with no info hashes.
    pub fn new(host: LsdHost, port: u16) -> Self {
        LsdAnnounce {
            host,
            port,
            info_hashes: Vec::new(),
            cookie: None,
        }
    }

    /// Set info hashes (replaces any existing).
    pub fn info_hashes(mut self, hashes: Vec<[u8; 20]>) -> Self {
        self.info_hashes = hashes;
        self
    }

    /// Set an optional cookie.
    pub fn cookie(mut self, cookie: Option<String>) -> Self {
        self.cookie = cookie;
        self
    }

    /// Serialize to wire format bytes (HTTP-style headers).
    ///
    /// Truncates `info_hashes` to fit within 1400 bytes per BEP 14.
    /// Returns `None` if `info_hashes` is empty.
    pub fn to_bytes(&self) -> Option<Vec<u8>> {
        if self.info_hashes.is_empty() {
            return None;
        }

        // First line: BT-SEARCH * HTTP/1.1
        let first_line = b"BT-SEARCH * HTTP/1.1\r\n";

        // Host header
        let host_line = format!("Host: {}\r\n", self.host.multicast_addr());

        // Port header
        let port_line = format!("Port: {}\r\n", self.port);

        // Cookie header (optional)
        let cookie_line = self
            .cookie
            .as_ref()
            .map(|c| format!("cookie: {c}\r\n"))
            .unwrap_or_default();

        // Build Infohash headers, respecting the 1400-byte limit
        let mut body = Vec::with_capacity(MAX_ANNOUNCE_SIZE);
        body.extend_from_slice(first_line);
        body.extend_from_slice(host_line.as_bytes());
        body.extend_from_slice(port_line.as_bytes());
        body.extend_from_slice(cookie_line.as_bytes());

        let mut added = 0usize;
        for ih in &self.info_hashes {
            // 40 hex chars + "Infohash: " + "\r\n" = 10 + 40 + 2 = 52 bytes
            let infohash_line = format!(
                "Infohash: {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}\r\n",
                ih[0],
                ih[1],
                ih[2],
                ih[3],
                ih[4],
                ih[5],
                ih[6],
                ih[7],
                ih[8],
                ih[9],
                ih[10],
                ih[11],
                ih[12],
                ih[13],
                ih[14],
                ih[15],
                ih[16],
                ih[17],
                ih[18],
                ih[19],
            );
            let line_bytes = infohash_line.as_bytes();

            if body.len() + line_bytes.len() + 2 > MAX_ANNOUNCE_SIZE {
                // Would exceed the limit — stop adding infohashes
                break;
            }
            body.extend_from_slice(line_bytes);
            added += 1;
        }

        // Final \r\n (empty line separating headers from body)
        body.extend_from_slice(b"\r\n");

        if added == 0 {
            return None;
        }

        Some(body)
    }

    /// Deserialize from wire format bytes.
    ///
    /// Parses HTTP-style headers. Unknown headers are silently ignored
    /// for forward compatibility (BEP 14). Handles both `\r\n` and `\n`
    /// line endings.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::PeerInvalidLsdAnnounce` if:
    /// - The first line is not `BT-SEARCH * HTTP/1.1`
    /// - The `Host` header is missing or unrecognized
    /// - The `Port` header is missing or not a valid u16
    /// - No valid `Infohash` headers are found
    pub fn from_bytes(data: &[u8]) -> Result<Self, Error> {
        let Ok(text) = str::from_utf8(data) else {
            return Err(Error::new(ErrorKind::PeerInvalidLsdAnnounce));
        };

        let mut lines = text.lines();

        // First line: BT-SEARCH * HTTP/1.1
        let Some(first_line) = lines.next() else {
            return Err(Error::new(ErrorKind::PeerInvalidLsdAnnounce));
        };

        if first_line != "BT-SEARCH * HTTP/1.1" {
            return Err(Error::new(ErrorKind::PeerInvalidLsdAnnounce));
        }

        let mut host: Option<LsdHost> = None;
        let mut port: Option<u16> = None;
        let mut info_hashes: Vec<[u8; 20]> = Vec::new();
        let mut cookie: Option<String> = None;

        for line in lines {
            // Empty line signals end of headers
            if line.is_empty() {
                break;
            }

            // Split on ": " — but handle the case where no colon+space exists
            let Some((key, value)) = line.split_once(": ") else {
                // Ignore malformed header lines (forward compatibility)
                continue;
            };

            match key.to_lowercase().as_str() {
                "host" => {
                    host = LsdHost::from_host_header(value);
                }
                "port" => {
                    port = value.parse::<u16>().ok();
                }
                "infohash" => {
                    if let Ok(ih) = hex_to_info_hash(value) {
                        info_hashes.push(ih);
                    }
                }
                "cookie" => {
                    cookie = Some(value.to_string());
                }
                // Unknown headers are silently ignored (forward compatibility)
                _ => {}
            }
        }

        let host = host.ok_or_else(|| Error::new(ErrorKind::PeerInvalidLsdAnnounce))?;
        let port = port.ok_or_else(|| Error::new(ErrorKind::PeerInvalidLsdAnnounce))?;

        if info_hashes.is_empty() {
            return Err(Error::new(ErrorKind::PeerInvalidLsdAnnounce));
        }

        Ok(LsdAnnounce {
            host,
            port,
            info_hashes,
            cookie,
        })
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Decode a 40-character hex string into a 20-byte info hash.
fn hex_to_info_hash(s: &str) -> Result<[u8; 20], ()> {
    if s.len() != 40 {
        return Err(());
    }
    let mut hash = [0u8; 20];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        if chunk.len() == 2
            && let Ok(b) = str::from_utf8(chunk)
            && let Ok(hex_byte) = u8::from_str_radix(b, 16)
        {
            hash[i] = hex_byte;
        } else {
            return Err(());
        }
    }
    Ok(hash)
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_info_hash(b: u8) -> [u8; 20] {
        let mut ih = [0u8; 20];
        ih[0] = b;
        ih
    }

    // ── LsdHost ──────────────────────────────────────────────────────

    #[test]
    fn host_multicast_addr_v4() {
        assert_eq!(LsdHost::V4.multicast_addr(), "239.192.152.143:6771");
    }

    #[test]
    fn host_multicast_addr_v6() {
        assert_eq!(LsdHost::V6.multicast_addr(), "[ff15::efc0:988f]:6771");
    }

    #[test]
    fn host_from_header_v4() {
        assert_eq!(
            LsdHost::from_host_header("239.192.152.143:6771"),
            Some(LsdHost::V4)
        );
    }

    #[test]
    fn host_from_header_v6() {
        assert_eq!(
            LsdHost::from_host_header("[ff15::efc0:988f]:6771"),
            Some(LsdHost::V6)
        );
    }

    #[test]
    fn host_from_header_unknown() {
        assert_eq!(LsdHost::from_host_header("224.0.0.1:1234"), None);
    }

    // ── LsdAnnounce ──────────────────────────────────────────────────

    #[test]
    fn round_trip_single_infohash() {
        let announce = LsdAnnounce {
            host: LsdHost::V4,
            port: 6881,
            info_hashes: vec![make_info_hash(0xab)],
            cookie: None,
        };

        let bytes = announce.to_bytes().expect("should produce bytes");
        let parsed = LsdAnnounce::from_bytes(&bytes).expect("should parse");
        assert_eq!(announce, parsed);
    }

    #[test]
    fn round_trip_multiple_infohashes() {
        let announce = LsdAnnounce {
            host: LsdHost::V4,
            port: 12345,
            info_hashes: vec![make_info_hash(1), make_info_hash(2), make_info_hash(3)],
            cookie: None,
        };

        let bytes = announce.to_bytes().expect("should produce bytes");
        let parsed = LsdAnnounce::from_bytes(&bytes).expect("should parse");
        assert_eq!(announce, parsed);
    }

    #[test]
    fn round_trip_with_cookie() {
        let announce = LsdAnnounce {
            host: LsdHost::V6,
            port: 9999,
            info_hashes: vec![make_info_hash(0xff)],
            cookie: Some("my-cookie-123".to_string()),
        };

        let bytes = announce.to_bytes().expect("should produce bytes");
        let parsed = LsdAnnounce::from_bytes(&bytes).expect("should parse");
        assert_eq!(announce, parsed);
    }

    #[test]
    fn empty_info_hashes_produces_none() {
        let announce = LsdAnnounce {
            host: LsdHost::V4,
            port: 6881,
            info_hashes: vec![],
            cookie: None,
        };
        assert!(announce.to_bytes().is_none());
    }

    #[test]
    fn parse_ignores_unknown_headers() {
        let data = b"BT-SEARCH * HTTP/1.1\r\n\
                     Host: 239.192.152.143:6771\r\n\
                     Port: 6881\r\n\
                     Infohash: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n\
                     X-Future-Ext: some-value\r\n\
                     \r\n";

        let parsed = LsdAnnounce::from_bytes(data).expect("should parse with unknown header");
        assert_eq!(parsed.host, LsdHost::V4);
        assert_eq!(parsed.port, 6881);
        assert_eq!(parsed.info_hashes.len(), 1);
    }

    #[test]
    fn parse_missing_host_is_error() {
        let data = b"BT-SEARCH * HTTP/1.1\r\nPort: 6881\r\nInfohash: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n\r\n";
        assert!(LsdAnnounce::from_bytes(data).is_err());
    }

    #[test]
    fn parse_missing_port_is_error() {
        let data = b"BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nInfohash: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n\r\n";
        assert!(LsdAnnounce::from_bytes(data).is_err());
    }

    #[test]
    fn parse_missing_infohash_is_error() {
        let data = b"BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 6881\r\n\r\n";
        assert!(LsdAnnounce::from_bytes(data).is_err());
    }

    #[test]
    fn parse_bad_first_line() {
        let data = b"NOT-A-LSD-MESSAGE\r\nHost: 239.192.152.143:6771\r\nPort: 6881\r\n\r\n";
        assert!(LsdAnnounce::from_bytes(data).is_err());
    }

    #[test]
    fn parse_empty_bytes() {
        assert!(LsdAnnounce::from_bytes(b"").is_err());
    }

    #[test]
    fn parse_invalid_infohash_hex() {
        let mut data = Vec::new();
        data.extend_from_slice(b"BT-SEARCH * HTTP/1.1\r\n");
        data.extend_from_slice(b"Host: 239.192.152.143:6771\r\n");
        data.extend_from_slice(b"Port: 6881\r\n");
        data.extend_from_slice(b"Infohash: zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz\r\n");
        data.extend_from_slice(b"\r\n");
        // Invalid hex infohash → no infohashes parsed → error
        assert!(LsdAnnounce::from_bytes(&data).is_err());
    }

    #[test]
    fn parse_short_infohash_hex() {
        let data = b"BT-SEARCH * HTTP/1.1\r\n\
                     Host: 239.192.152.143:6771\r\n\
                     Port: 6881\r\n\
                     Infohash: abc\r\n\
                     Infohash: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n\
                     \r\n";
        let parsed = LsdAnnounce::from_bytes(data).expect("should skip short infohash");
        assert_eq!(parsed.info_hashes.len(), 1); // only the valid 40-char one
    }
}
