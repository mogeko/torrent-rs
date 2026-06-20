use std::net::SocketAddr;

use crate::bencode::{Bencode, Bytes, dict_get_bytes};
use crate::error::{Error, ErrorKind};
use crate::tracker::{
    encode_compact_peers_ipv4, encode_compact_peers_ipv6, parse_compact_peers_ipv4,
    parse_compact_peers_ipv6,
};

/// A Peer Exchange (PEX) message (BEP 11).
///
/// PEX allows peers to exchange their lists of known peers without relying
/// on a tracker or DHT. The message is sent as a `PeerMessage::Extended`
/// with the `ut_pex` extension ID negotiated via LTEP (BEP 10).
///
/// # Wire format (bencoded)
///
/// ```text
/// d
///   5:added  <compact IPv4 list (6 bytes each)>
///   6:added6 <compact IPv6 list (18 bytes each, optional)>
///   7:dropped <compact IPv4 list>
///   8:dropped6 <compact IPv6 list (optional)>
/// e
/// ```
///
/// All four fields are optional. An empty dict `de` is valid.
/// Compact peer format: 4 bytes IP + 2 bytes port BE (IPv4),
/// 16 bytes IP + 2 bytes port BE (IPv6).
///
/// # Examples
///
/// ```
/// use std::net::{Ipv4Addr, SocketAddr};
/// use torrent_core::peer::PexMessage;
///
/// let mut msg = PexMessage::new();
/// msg.added.push(SocketAddr::new(
///     std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
///     6881,
/// ));
/// // IPv6 peers use the added6 / dropped6 fields (BEP 7 compact format).
/// msg.added6.push(SocketAddr::new(
///     std::net::IpAddr::V6(std::net::Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
///     6881,
/// ));
/// let ben = msg.to_bencode();
/// let parsed = PexMessage::from_bencode(&ben).unwrap();
/// assert_eq!(msg, parsed);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PexMessage {
    /// Peers added since the last PEX message (compact IPv4 format).
    pub added: Vec<SocketAddr>,
    /// Peers dropped since the last PEX message (compact IPv4 format).
    pub dropped: Vec<SocketAddr>,
    /// Peers added since the last PEX message (compact IPv6 format, optional).
    pub added6: Vec<SocketAddr>,
    /// Peers dropped since the last PEX message (compact IPv6 format, optional).
    pub dropped6: Vec<SocketAddr>,
}

impl PexMessage {
    /// Create an empty PEX message with no peers.
    pub fn new() -> Self {
        PexMessage {
            added: Vec::new(),
            dropped: Vec::new(),
            added6: Vec::new(),
            dropped6: Vec::new(),
        }
    }

    /// Serialize to a bencoded dictionary.
    ///
    /// Only non-empty fields are included. Keys are sorted lexicographically
    /// per BEP 3.
    pub fn to_bencode(&self) -> Bencode {
        let mut entries: Vec<(Bytes, Bencode)> = Vec::with_capacity(4);

        if !self.added.is_empty() {
            entries.push((
                Bytes::from("added"),
                Bencode::Bytes(Bytes::copy_from_slice(&encode_compact_peers_ipv4(
                    &self.added,
                ))),
            ));
        }

        if !self.added6.is_empty() {
            entries.push((
                Bytes::from("added6"),
                Bencode::Bytes(Bytes::copy_from_slice(&encode_compact_peers_ipv6(
                    &self.added6,
                ))),
            ));
        }

        if !self.dropped.is_empty() {
            entries.push((
                Bytes::from("dropped"),
                Bencode::Bytes(Bytes::copy_from_slice(&encode_compact_peers_ipv4(
                    &self.dropped,
                ))),
            ));
        }

        if !self.dropped6.is_empty() {
            entries.push((
                Bytes::from("dropped6"),
                Bencode::Bytes(Bytes::copy_from_slice(&encode_compact_peers_ipv6(
                    &self.dropped6,
                ))),
            ));
        }

        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        Bencode::Dict(entries)
    }

    /// Deserialize from a bencoded dictionary.
    ///
    /// Returns an error if `val` is not a dictionary. Missing fields
    /// default to empty vectors.
    pub fn from_bencode(val: &Bencode) -> Result<Self, Error> {
        if !matches!(val, Bencode::Dict(_)) {
            return Err(Error::new(ErrorKind::PeerInvalidPexMessage));
        }

        let added = dict_get_bytes(val, b"added")
            .and_then(|b| {
                parse_compact_peers_ipv4(b)
                    .inspect_err(|e| tracing::debug!("ignoring malformed PEX added field: {}", e))
                    .ok()
            })
            .unwrap_or_default();

        let dropped = dict_get_bytes(val, b"dropped")
            .and_then(|b| {
                parse_compact_peers_ipv4(b)
                    .inspect_err(|e| tracing::debug!("ignoring malformed PEX dropped field: {}", e))
                    .ok()
            })
            .unwrap_or_default();

        let added6 = dict_get_bytes(val, b"added6")
            .and_then(|b| {
                parse_compact_peers_ipv6(b)
                    .inspect_err(|e| tracing::debug!("ignoring malformed PEX added6 field: {}", e))
                    .ok()
            })
            .unwrap_or_default();

        let dropped6 = dict_get_bytes(val, b"dropped6")
            .and_then(|b| {
                parse_compact_peers_ipv6(b)
                    .inspect_err(|e| {
                        tracing::debug!("ignoring malformed PEX dropped6 field: {}", e)
                    })
                    .ok()
            })
            .unwrap_or_default();

        Ok(PexMessage {
            added,
            dropped,
            added6,
            dropped6,
        })
    }
}

impl Default for PexMessage {
    fn default() -> Self {
        PexMessage::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode::encode;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn to_bencode_empty() {
        let msg = PexMessage::new();
        let ben = msg.to_bencode();
        let encoded = encode(&ben);
        // Empty dict
        assert_eq!(encoded, b"de");
    }

    #[test]
    fn to_bencode_added_only() {
        let mut msg = PexMessage::new();
        msg.added.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            6881,
        ));
        let ben = msg.to_bencode();
        // Roundtrip to verify
        let parsed = PexMessage::from_bencode(&ben).unwrap();
        assert_eq!(parsed.added, msg.added);
        assert!(parsed.dropped.is_empty());
    }

    #[test]
    fn to_bencode_dropped_only() {
        let mut msg = PexMessage::new();
        msg.dropped.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            8080,
        ));
        let ben = msg.to_bencode();
        let parsed = PexMessage::from_bencode(&ben).unwrap();
        assert_eq!(parsed.dropped, msg.dropped);
        assert!(parsed.added.is_empty());
    }

    #[test]
    fn to_bencode_all_fields() {
        let mut msg = PexMessage::new();
        msg.added.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            6881,
        ));
        msg.dropped.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            6889,
        ));
        msg.added6.push(SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
            6881,
        ));
        msg.dropped6.push(SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
            8080,
        ));
        let ben = msg.to_bencode();
        let parsed = PexMessage::from_bencode(&ben).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn from_bencode_empty() {
        let (val, _) = crate::bencode::decode(b"de").unwrap();
        let msg = PexMessage::from_bencode(&val).unwrap();
        assert!(msg.added.is_empty());
        assert!(msg.dropped.is_empty());
        assert!(msg.added6.is_empty());
        assert!(msg.dropped6.is_empty());
    }

    #[test]
    fn from_bencode_added() {
        let mut msg = PexMessage::new();
        msg.added.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            6881,
        ));
        let ben = msg.to_bencode();
        let encoded = encode(&ben);
        let (val, _) = crate::bencode::decode(&encoded).unwrap();
        let parsed = PexMessage::from_bencode(&val).unwrap();
        assert_eq!(parsed.added, msg.added);
    }

    #[test]
    fn from_bencode_dropped() {
        let mut msg = PexMessage::new();
        msg.dropped.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            6889,
        ));
        let ben = msg.to_bencode();
        let encoded = encode(&ben);
        let (val, _) = crate::bencode::decode(&encoded).unwrap();
        let parsed = PexMessage::from_bencode(&val).unwrap();
        assert_eq!(parsed.dropped, msg.dropped);
    }

    #[test]
    fn from_bencode_not_a_dict() {
        let val = Bencode::Integer(42);
        assert!(PexMessage::from_bencode(&val).is_err());
    }

    #[test]
    fn from_bencode_invalid_compact() {
        // "added" field with odd number of bytes (not a multiple of 6)
        let (val, _) = crate::bencode::decode(b"d5:added1:xe").unwrap();
        let msg = PexMessage::from_bencode(&val).unwrap();
        // Invalid compact data is silently ignored; added stays empty
        assert!(msg.added.is_empty());
    }

    #[test]
    fn roundtrip() {
        let mut msg = PexMessage::new();
        msg.added.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            6881,
        ));
        msg.added.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            6889,
        ));
        msg.dropped.push(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            8080,
        ));
        msg.added6.push(SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
            6881,
        ));
        msg.dropped6.push(SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
            8080,
        ));
        let ben = msg.to_bencode();
        let parsed = PexMessage::from_bencode(&ben).unwrap();
        assert_eq!(msg, parsed);
    }
}
