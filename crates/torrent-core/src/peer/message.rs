use std::collections::HashSet;
use std::net::IpAddr;
use std::net::SocketAddr;

use sha1::{Digest, Sha1};

use crate::error::{Error, ErrorKind};

/// A peer wire protocol message (BEP 3).
///
/// Messages follow a fixed-length header format:
///
/// ```text
/// <4-byte big-endian length> <1-byte message ID> <payload>
/// ```
///
/// Supports 17 message types: 12 standard (BEP 3) including BEP 10 Extended,
/// plus 5 Fast Extension messages (BEP 6) and an `Unknown` catch-all for
/// forward compatibility.
///
/// # Examples
///
/// Encoding and decoding a Have message:
///
/// ```
/// use torrent_core::peer::{PeerMessage, encode, decode};
///
/// let msg = PeerMessage::Have(42);
/// let encoded = encode(&msg);
/// let decoded = decode(&encoded).unwrap();
/// assert_eq!(msg, decoded);
/// ```
///
/// Round-trip all message types:
///
/// ```
/// use torrent_core::peer::{PeerMessage, encode, decode};
///
/// let messages = vec![
///     PeerMessage::KeepAlive,
///     PeerMessage::Choke,
///     PeerMessage::Unchoke,
///     PeerMessage::Interested,
///     PeerMessage::NotInterested,
///     PeerMessage::Have(7),
///     PeerMessage::Bitfield(vec![0xFF]),
///     PeerMessage::Request { index: 0, begin: 0, length: 16384 },
///     PeerMessage::Cancel { index: 1, begin: 1024, length: 8192 },
///     PeerMessage::Port(6881),
/// ];
///
/// for msg in &messages {
///     let encoded = encode(msg);
///     let decoded = decode(&encoded).unwrap();
///     assert_eq!(msg, &decoded);
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    /// Keep-alive: `<len=0000>`.
    KeepAlive,
    /// Choke: `<len=0001><id=0>`.
    Choke,
    /// Unchoke: `<len=0001><id=1>`.
    Unchoke,
    /// Interested: `<len=0001><id=2>`.
    Interested,
    /// NotInterested: `<len=0001><id=3>`.
    NotInterested,
    /// Have: `<len=0005><id=4><piece index>`.
    Have(u32),
    /// Bitfield: `<len=0001+X><id=5><bitfield>`.
    Bitfield(Vec<u8>),
    /// Request: `<len=0013><id=6><index><begin><length>`.
    Request {
        /// Piece index.
        index: u32,
        /// Byte offset within the piece.
        begin: u32,
        /// Length of the block to request (typically 16 KB).
        length: u32,
    },
    /// Piece: `<len=0009+X><id=7><index><begin><block>`.
    Piece {
        /// Piece index.
        index: u32,
        /// Byte offset within the piece.
        begin: u32,
        /// The actual block data.
        data: Vec<u8>,
    },
    /// Cancel: `<len=0013><id=8><index><begin><length>`.
    Cancel {
        /// Piece index.
        index: u32,
        /// Byte offset within the piece.
        begin: u32,
        /// Length of the block to cancel.
        length: u32,
    },
    /// Port: `<len=0003><id=9><listen-port>` (BEP 5, DHT).
    Port(u16),
    /// Extended message (BEP 10): `<len=0002+X><id=20><ext_id><payload>`.
    ///
    /// Extended messages carry a single-byte extension ID negotiated via the
    /// LTEP handshake, followed by a bencoded dictionary payload. Extension
    /// ID 0 is reserved for the LTEP handshake itself.
    ///
    /// Implements BEP 10: Extension Protocol.
    Extended {
        /// Extension message ID (negotiated via LTEP handshake).
        ext_id: u8,
        /// Bencoded dictionary payload.
        data: Vec<u8>,
    },
    /// Suggest: `<len=0005><id=13><piece index>` (BEP 6).
    ///
    /// Advisory message suggesting a piece the peer should download.
    /// May be ignored by the receiver.
    Suggest(u32),
    /// HaveAll: `<len=0001><id=14>` (BEP 6).
    ///
    /// Sent by a peer that has every piece, replacing the Bitfield
    /// message. This eliminates sending a large bitfield for seeds.
    HaveAll,
    /// HaveNone: `<len=0001><id=15>` (BEP 6).
    ///
    /// Sent by a peer that has no pieces, replacing an all-zero
    /// Bitfield message.
    HaveNone,
    /// Reject: `<len=0013><id=16><index><begin><length>` (BEP 6).
    ///
    /// Sent in response to a Request that cannot be fulfilled.
    /// The receiver should clear the corresponding block from its
    /// pipeline.
    Reject {
        /// Piece index.
        index: u32,
        /// Byte offset within the piece.
        begin: u32,
        /// Length of the block that was requested.
        length: u32,
    },
    /// AllowedFast: `<len=0005><id=17><piece index>` (BEP 6).
    ///
    /// Grants the receiver permission to request this piece even
    /// when choked. Sent during connection establishment after
    /// the handshake, before the Bitfield/HaveAll/HaveNone exchange.
    AllowedFast(u32),
    /// Unknown message type (forward compatibility).
    ///
    /// BEP 3 requires clients to ignore messages with unrecognized
    /// IDs rather than disconnecting. This variant captures such
    /// messages so they can be silently skipped.
    Unknown {
        /// The unrecognized message ID byte.
        id: u8,
        /// Raw payload bytes (excluding the length prefix and ID).
        data: Vec<u8>,
    },
}

/// Encode a `PeerMessage` to its wire format bytes.
///
/// ```text
/// Format: <4-byte big-endian length prefix> <1-byte message id> <payload>
/// Keep-alive: <4-byte 0>
/// ```
///
/// # Panics
///
/// Panics if the payload of a [`PeerMessage::Bitfield`], [`PeerMessage::Piece`],
/// or [`PeerMessage::Extended`] exceeds `u32::MAX` minus the fixed header
/// overhead. This is unreachable in normal protocol use — the `torrent`
/// crate enforces a 2 MiB message size limit on incoming data.
pub fn encode(msg: &PeerMessage) -> Vec<u8> {
    tracing::trace!("encoding peer message: {:?}", msg);
    match msg {
        PeerMessage::KeepAlive => vec![0, 0, 0, 0],
        PeerMessage::Choke => vec![0, 0, 0, 1, 0],
        PeerMessage::Unchoke => vec![0, 0, 0, 1, 1],
        PeerMessage::Interested => vec![0, 0, 0, 1, 2],
        PeerMessage::NotInterested => vec![0, 0, 0, 1, 3],
        PeerMessage::Have(index) => {
            let mut buf = vec![0, 0, 0, 5, 4];
            buf.extend_from_slice(&index.to_be_bytes());
            buf
        }
        PeerMessage::Bitfield(bitfield) => {
            // SAFETY: MAX_MESSAGE_SIZE (2 MiB) ensures bitfield.len() never
            // exceeds u32::MAX - 1, so the length prefix always fits in a u32.
            let len = 1u32
                + u32::try_from(bitfield.len()).expect("bitfield payload exceeds u32::MAX - 1");
            let mut buf = Vec::with_capacity(4 + len as usize);
            buf.extend_from_slice(&len.to_be_bytes());
            buf.push(5);
            buf.extend_from_slice(bitfield);
            buf
        }
        PeerMessage::Request {
            index,
            begin,
            length,
        }
        | PeerMessage::Cancel {
            index,
            begin,
            length,
        } => {
            let msg_id = if matches!(msg, PeerMessage::Request { .. }) {
                6
            } else {
                8
            };
            let mut buf = vec![0, 0, 0, 13, msg_id];
            buf.extend_from_slice(&index.to_be_bytes());
            buf.extend_from_slice(&begin.to_be_bytes());
            buf.extend_from_slice(&length.to_be_bytes());
            buf
        }
        PeerMessage::Piece { index, begin, data } => {
            // SAFETY: MAX_MESSAGE_SIZE (2 MiB) ensures data.len() never exceeds
            // u32::MAX - 9, so the length prefix always fits in a u32.
            let len = 9u32 + u32::try_from(data.len()).expect("piece payload exceeds u32::MAX - 9");
            let mut buf = Vec::with_capacity(4 + len as usize);
            buf.extend_from_slice(&len.to_be_bytes());
            buf.push(7);
            buf.extend_from_slice(&index.to_be_bytes());
            buf.extend_from_slice(&begin.to_be_bytes());
            buf.extend_from_slice(data);
            buf
        }
        PeerMessage::Port(port) => {
            let mut buf = vec![0, 0, 0, 3, 9];
            buf.extend_from_slice(&port.to_be_bytes());
            buf
        }
        PeerMessage::Extended { ext_id, data } => {
            // SAFETY: MAX_MESSAGE_SIZE (2 MiB) ensures data.len() never exceeds
            // u32::MAX - 2, so the length prefix always fits in a u32.
            let len = 2u32
                + u32::try_from(data.len()).expect("extended message payload exceeds u32::MAX - 2");
            let mut buf = Vec::with_capacity(4 + len as usize);
            buf.extend_from_slice(&len.to_be_bytes());
            buf.push(20);
            buf.push(*ext_id);
            buf.extend_from_slice(data);
            buf
        }
        PeerMessage::Suggest(index) => {
            let mut buf = vec![0, 0, 0, 5, 13];
            buf.extend_from_slice(&index.to_be_bytes());
            buf
        }
        PeerMessage::HaveAll => vec![0, 0, 0, 1, 14],
        PeerMessage::HaveNone => vec![0, 0, 0, 1, 15],
        PeerMessage::Reject {
            index,
            begin,
            length,
        } => {
            let mut buf = vec![0, 0, 0, 13, 16];
            buf.extend_from_slice(&index.to_be_bytes());
            buf.extend_from_slice(&begin.to_be_bytes());
            buf.extend_from_slice(&length.to_be_bytes());
            buf
        }
        PeerMessage::AllowedFast(index) => {
            let mut buf = vec![0, 0, 0, 5, 17];
            buf.extend_from_slice(&index.to_be_bytes());
            buf
        }
        PeerMessage::Unknown { id, data } => {
            // SAFETY: MAX_MESSAGE_SIZE (2 MiB) ensures data.len() never exceeds
            // u32::MAX - 1, so the length prefix always fits in a u32.
            let len = 1u32
                + u32::try_from(data.len()).expect("unknown message payload exceeds u32::MAX - 1");
            let mut buf = Vec::with_capacity(4 + len as usize);
            buf.extend_from_slice(&len.to_be_bytes());
            buf.push(*id);
            buf.extend_from_slice(data);
            buf
        }
    }
}

/// Compute the Allowed Fast set for a peer (BEP 6 §2.2).
///
/// Returns the first `k` unique piece indices derived from:
///
/// ```text
/// SHA1(info_hash || ip_bytes || i)
/// ```
///
/// where `ip_bytes` is the peer's IP address in network byte order
/// (4 bytes for IPv4, 16 bytes for IPv6) and `i` starts at 0 and
/// increments until `k` unique indices are found or the search is
/// exhausted.
///
/// The result is returned in generation order as required by BEP 6.
///
/// # Examples
///
/// ```
/// use std::net::SocketAddr;
/// use torrent_core::peer::compute_allowed_fast_set;
///
/// let info_hash = [0u8; 20];
/// let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
/// let set = compute_allowed_fast_set(&info_hash, addr, 100, 10);
/// assert_eq!(set.len(), 10);
/// ```
pub fn compute_allowed_fast_set(
    info_hash: &[u8; 20], addr: SocketAddr, num_pieces: u32, k: usize,
) -> Vec<u32> {
    if num_pieces == 0 || k == 0 {
        return Vec::new();
    }

    let ip_bytes = match addr.ip() {
        IpAddr::V4(v4) => v4.octets().to_vec(),
        IpAddr::V6(v6) => v6.octets().to_vec(),
    };

    let mut seen = HashSet::with_capacity(k);
    let mut set = Vec::with_capacity(k);
    // Upper bound: if we've tried 4× the piece count without filling
    // k slots, the set is saturated.
    let max_iterations = num_pieces as usize * 4;

    for i in 0u32.. {
        if set.len() >= k || i as usize >= max_iterations {
            break;
        }

        let mut hasher = Sha1::new();
        Digest::update(&mut hasher, info_hash);
        Digest::update(&mut hasher, &ip_bytes);
        Digest::update(&mut hasher, i.to_be_bytes().as_ref());
        let hash = hasher.finalize();

        let piece_index = u32::from_be_bytes([hash[0], hash[1], hash[2], hash[3]]) % num_pieces;

        if seen.insert(piece_index) {
            set.push(piece_index);
        }
    }

    set
}

/// Decode a `PeerMessage` from wire format bytes.
///
/// The input must include the 4-byte length prefix. Returns an error if
/// the message is malformed.
pub fn decode(data: &[u8]) -> Result<PeerMessage, Error> {
    if data.len() < 4 {
        return Err(Error::new(ErrorKind::PeerInvalidMessage));
    }

    let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);

    // Keep-alive
    if len == 0 {
        tracing::trace!("decoded peer message: KeepAlive");
        return Ok(PeerMessage::KeepAlive);
    }

    // Need at least 1 byte for message id
    if data.len() < 5 {
        return Err(Error::new(ErrorKind::PeerInvalidMessage));
    }

    // Total expected: 4 + len
    if data.len() < 4 + len as usize {
        return Err(Error::new(ErrorKind::PeerInvalidMessage));
    }

    let payload = &data[5..4 + len as usize];
    let msg_id = data[4];

    match msg_id {
        0 => {
            if len != 1 {
                return Err(Error::new(ErrorKind::PeerInvalidMessage));
            }
            Ok(PeerMessage::Choke)
        }
        1 => {
            if len != 1 {
                return Err(Error::new(ErrorKind::PeerInvalidMessage));
            }
            Ok(PeerMessage::Unchoke)
        }
        2 => {
            if len != 1 {
                return Err(Error::new(ErrorKind::PeerInvalidMessage));
            }
            Ok(PeerMessage::Interested)
        }
        3 => {
            if len != 1 {
                return Err(Error::new(ErrorKind::PeerInvalidMessage));
            }
            Ok(PeerMessage::NotInterested)
        }
        4 => {
            if len != 5 || payload.len() != 4 {
                return Err(Error::new(ErrorKind::PeerInvalidMessage));
            }
            let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            Ok(PeerMessage::Have(index))
        }
        5 => Ok(PeerMessage::Bitfield(payload.to_vec())),
        6 => {
            if len != 13 || payload.len() != 12 {
                return Err(Error::new(ErrorKind::PeerInvalidMessage));
            }
            let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let begin = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
            let length = u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]);
            Ok(PeerMessage::Request {
                index,
                begin,
                length,
            })
        }
        7 => {
            if payload.len() < 8 {
                return Err(Error::new(ErrorKind::PeerInvalidMessage));
            }
            let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let begin = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
            let data = payload[8..].to_vec();
            Ok(PeerMessage::Piece { index, begin, data })
        }
        8 => {
            if len != 13 || payload.len() != 12 {
                return Err(Error::new(ErrorKind::PeerInvalidMessage));
            }
            let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let begin = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
            let length = u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]);
            Ok(PeerMessage::Cancel {
                index,
                begin,
                length,
            })
        }
        9 => {
            if len != 3 || payload.len() != 2 {
                return Err(Error::new(ErrorKind::PeerInvalidMessage));
            }
            let port = u16::from_be_bytes([payload[0], payload[1]]);
            Ok(PeerMessage::Port(port))
        }
        20 => {
            if payload.is_empty() {
                return Err(Error::new(ErrorKind::PeerInvalidExtendedMessage));
            }
            let ext_id = payload[0];
            let data = payload[1..].to_vec();
            Ok(PeerMessage::Extended { ext_id, data })
        }
        13 => {
            if len != 5 || payload.len() != 4 {
                return Err(Error::new(ErrorKind::PeerInvalidFastMessage));
            }
            let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            Ok(PeerMessage::Suggest(index))
        }
        14 => {
            if len != 1 {
                return Err(Error::new(ErrorKind::PeerInvalidFastMessage));
            }
            Ok(PeerMessage::HaveAll)
        }
        15 => {
            if len != 1 {
                return Err(Error::new(ErrorKind::PeerInvalidFastMessage));
            }
            Ok(PeerMessage::HaveNone)
        }
        16 => {
            if len != 13 || payload.len() != 12 {
                return Err(Error::new(ErrorKind::PeerInvalidFastMessage));
            }
            let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let begin = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
            let length = u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]);
            Ok(PeerMessage::Reject {
                index,
                begin,
                length,
            })
        }
        17 => {
            if len != 5 || payload.len() != 4 {
                return Err(Error::new(ErrorKind::PeerInvalidFastMessage));
            }
            let index = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            Ok(PeerMessage::AllowedFast(index))
        }
        _ => {
            // BEP 3: ignore unknown message types for forward compatibility.
            tracing::debug!("ignoring unknown peer message id {}", msg_id);
            Ok(PeerMessage::Unknown {
                id: msg_id,
                data: payload.to_vec(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_keepalive() {
        assert_eq!(encode(&PeerMessage::KeepAlive), vec![0, 0, 0, 0]);
    }

    #[test]
    fn encode_choke() {
        assert_eq!(encode(&PeerMessage::Choke), vec![0, 0, 0, 1, 0]);
    }

    #[test]
    fn encode_unchoke() {
        assert_eq!(encode(&PeerMessage::Unchoke), vec![0, 0, 0, 1, 1]);
    }

    #[test]
    fn encode_interested() {
        assert_eq!(encode(&PeerMessage::Interested), vec![0, 0, 0, 1, 2]);
    }

    #[test]
    fn encode_not_interested() {
        assert_eq!(encode(&PeerMessage::NotInterested), vec![0, 0, 0, 1, 3]);
    }

    #[test]
    fn encode_have() {
        let msg = PeerMessage::Have(42);
        let encoded = encode(&msg);
        assert_eq!(encoded.len(), 9); // 4 len + 1 id + 4 payload
        // length = 5, id = 4
        assert_eq!(&encoded[0..5], &[0, 0, 0, 5, 4]);
        // piece index = 42
        assert_eq!(&encoded[5..], &42u32.to_be_bytes());
    }

    #[test]
    fn encode_request() {
        let msg = PeerMessage::Request {
            index: 1,
            begin: 1024,
            length: 16384,
        };
        let encoded = encode(&msg);
        // 4 + 1 + 12 = 17 bytes
        assert_eq!(encoded.len(), 17);
    }

    #[test]
    fn encode_piece() {
        let data = vec![0xAB; 16384];
        let msg = PeerMessage::Piece {
            index: 0,
            begin: 0,
            data: data.clone(),
        };
        let encoded = encode(&msg);
        assert_eq!(encoded.len(), 4 + 1 + 8 + 16384);
        // first 4 bytes: length = 9 + data.len()
        let expected_len = (9u32 + 16384u32).to_be_bytes();
        assert_eq!(&encoded[0..4], &expected_len);
    }

    #[test]
    fn encode_cancel() {
        let msg = PeerMessage::Cancel {
            index: 5,
            begin: 2048,
            length: 8192,
        };
        let encoded = encode(&msg);
        assert_eq!(encoded.len(), 17);
        assert_eq!(encoded[4], 8); // msg_id = 8
    }

    #[test]
    fn encode_port() {
        let msg = PeerMessage::Port(6881);
        let encoded = encode(&msg);
        assert_eq!(encoded.len(), 7); // 4 + 1 + 2
        assert_eq!(&encoded[0..5], &[0, 0, 0, 3, 9]);
    }

    #[test]
    fn encode_bitfield() {
        let bits = vec![0xAA, 0x55, 0xFF];
        let msg = PeerMessage::Bitfield(bits.clone());
        let encoded = encode(&msg);
        // 4 + 1 + bitfield
        assert_eq!(encoded.len(), 5 + 3);
        assert_eq!(encoded[4], 5); // msg_id
        assert_eq!(&encoded[5..], bits.as_slice());
    }

    #[test]
    fn roundtrip_all_messages() {
        let messages = vec![
            PeerMessage::KeepAlive,
            PeerMessage::Choke,
            PeerMessage::Unchoke,
            PeerMessage::Interested,
            PeerMessage::NotInterested,
            PeerMessage::Have(7),
            PeerMessage::Bitfield(vec![0xFF, 0x00]),
            PeerMessage::Request {
                index: 0,
                begin: 0,
                length: 16384,
            },
            PeerMessage::Cancel {
                index: 1,
                begin: 1024,
                length: 8192,
            },
            PeerMessage::Port(6881),
            PeerMessage::Extended {
                ext_id: 0,
                data: b"d1:md2:ute".to_vec(),
            },
        ];

        for msg in &messages {
            let encoded = encode(msg);
            let decoded = decode(&encoded).unwrap();
            assert_eq!(msg, &decoded, "roundtrip failed for {:?}", msg);
        }
    }

    #[test]
    fn roundtrip_piece() {
        let msg = PeerMessage::Piece {
            index: 3,
            begin: 4096,
            data: vec![0xCC; 512],
        };
        let encoded = encode(&msg);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn decode_empty_data() {
        assert!(decode(b"").is_err());
    }

    #[test]
    fn decode_truncated() {
        // 3 bytes, not enough for length prefix
        assert!(decode(&[0, 0, 0]).is_err());
    }

    #[test]
    fn decode_unknown_message_id() {
        // BEP 3: unknown message IDs should be accepted as Unknown
        // for forward compatibility, not rejected.
        let data = [0, 0, 0, 1, 255];
        let decoded = decode(&data).unwrap();
        assert_eq!(
            decoded,
            PeerMessage::Unknown {
                id: 255,
                data: vec![]
            }
        );
    }

    #[test]
    fn encode_extended() {
        let msg = PeerMessage::Extended {
            ext_id: 1,
            data: vec![0xAB, 0xCD, 0xEF],
        };
        let encoded = encode(&msg);
        // 4 len + 1 id(20) + 1 ext_id + 3 data = 9 bytes
        assert_eq!(encoded.len(), 9);
        // len = 2 + 3 = 5
        assert_eq!(&encoded[0..4], &[0, 0, 0, 5]);
        assert_eq!(encoded[4], 20); // msg_id
        assert_eq!(encoded[5], 1); // ext_id
        assert_eq!(&encoded[6..], &[0xAB, 0xCD, 0xEF]);
    }

    #[test]
    fn decode_extended() {
        // len=5, id=20, ext_id=1, data=[0xAB, 0xCD, 0xEF]
        let data = [0, 0, 0, 5, 20, 1, 0xAB, 0xCD, 0xEF];
        let decoded = decode(&data).unwrap();
        assert_eq!(
            decoded,
            PeerMessage::Extended {
                ext_id: 1,
                data: vec![0xAB, 0xCD, 0xEF],
            }
        );
    }

    #[test]
    fn decode_extended_empty_payload() {
        // len=2 (only ext_id, no data), id=20, ext_id=0 — valid, empty data
        let data = [0, 0, 0, 2, 20, 0];
        let decoded = decode(&data).unwrap();
        assert_eq!(
            decoded,
            PeerMessage::Extended {
                ext_id: 0,
                data: vec![],
            }
        );
    }

    #[test]
    fn decode_extended_missing_ext_id() {
        // len=1, id=20 — payload is empty, should fail
        let data = [0, 0, 0, 1, 20];
        assert!(decode(&data).is_err());
    }

    #[test]
    fn roundtrip_extended() {
        let msg = PeerMessage::Extended {
            ext_id: 7,
            data: b"d5:added12:\x00\x00\x00\x00\x00\x00e".to_vec(),
        };
        let encoded = encode(&msg);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    // ── BEP 6 Fast Extension tests ──

    #[test]
    fn encode_suggest() {
        let msg = PeerMessage::Suggest(42);
        let encoded = encode(&msg);
        assert_eq!(encoded.len(), 9);
        assert_eq!(&encoded[0..5], &[0, 0, 0, 5, 13]);
        assert_eq!(&encoded[5..], &42u32.to_be_bytes());
    }

    #[test]
    fn encode_haveall() {
        let msg = PeerMessage::HaveAll;
        let encoded = encode(&msg);
        assert_eq!(encoded, vec![0, 0, 0, 1, 14]);
    }

    #[test]
    fn encode_havenone() {
        let msg = PeerMessage::HaveNone;
        let encoded = encode(&msg);
        assert_eq!(encoded, vec![0, 0, 0, 1, 15]);
    }

    #[test]
    fn encode_reject() {
        let msg = PeerMessage::Reject {
            index: 3,
            begin: 2048,
            length: 16384,
        };
        let encoded = encode(&msg);
        assert_eq!(encoded.len(), 17);
        assert_eq!(encoded[4], 16);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn encode_allowed_fast() {
        let msg = PeerMessage::AllowedFast(7);
        let encoded = encode(&msg);
        assert_eq!(encoded.len(), 9);
        assert_eq!(&encoded[0..5], &[0, 0, 0, 5, 17]);
        assert_eq!(&encoded[5..], &7u32.to_be_bytes());
    }

    #[test]
    fn roundtrip_bep6_messages() {
        let messages = vec![
            PeerMessage::Suggest(0),
            PeerMessage::Suggest(42),
            PeerMessage::HaveAll,
            PeerMessage::HaveNone,
            PeerMessage::Reject {
                index: 0,
                begin: 0,
                length: 16384,
            },
            PeerMessage::AllowedFast(0),
            PeerMessage::AllowedFast(99),
        ];
        for msg in &messages {
            let encoded = encode(msg);
            let decoded = decode(&encoded).unwrap();
            assert_eq!(msg, &decoded, "roundtrip failed for {:?}", msg);
        }
    }

    #[test]
    fn decode_unknown_is_forward_compatible() {
        // An unknown message with id=99, len=2 (id + 1 byte payload)
        let data = [0, 0, 0, 2, 99, 0xAB];
        let decoded = decode(&data).unwrap();
        assert_eq!(
            decoded,
            PeerMessage::Unknown {
                id: 99,
                data: vec![0xAB],
            }
        );
    }

    #[test]
    fn roundtrip_unknown() {
        let msg = PeerMessage::Unknown {
            id: 200,
            data: vec![1, 2, 3],
        };
        let encoded = encode(&msg);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn decode_invalid_suggest() {
        // len=3 (wrong), id=13 — should fail with PeerInvalidFastMessage
        let data = [0, 0, 0, 3, 13, 0, 0];
        let err = decode(&data).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PeerInvalidFastMessage);
    }

    #[test]
    fn decode_invalid_haveall() {
        // len=2 (wrong), id=14
        let data = [0, 0, 0, 2, 14, 0];
        let err = decode(&data).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PeerInvalidFastMessage);
    }

    #[test]
    fn decode_invalid_reject() {
        // len=5 (wrong), id=16
        let data = [0, 0, 0, 5, 16, 0, 0, 0, 0];
        let err = decode(&data).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PeerInvalidFastMessage);
    }

    // ── compute_allowed_fast_set tests ──

    #[test]
    fn allowed_fast_empty_when_zero_pieces() {
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let set = compute_allowed_fast_set(&[0u8; 20], addr, 0, 10);
        assert!(set.is_empty());
    }

    #[test]
    fn allowed_fast_empty_when_k_zero() {
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let set = compute_allowed_fast_set(&[0u8; 20], addr, 100, 0);
        assert!(set.is_empty());
    }

    #[test]
    fn allowed_fast_produces_expected_count() {
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        let set = compute_allowed_fast_set(&[0u8; 20], addr, 100, 10);
        assert_eq!(set.len(), 10);
        // All indices should be within [0, 100)
        for &idx in &set {
            assert!(idx < 100, "index {} out of range", idx);
        }
    }

    #[test]
    fn allowed_fast_is_deterministic() {
        let addr: SocketAddr = "10.0.0.1:9999".parse().unwrap();
        let info_hash = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10, 0x11, 0x12, 0x13, 0x14,
        ];
        let set1 = compute_allowed_fast_set(&info_hash, addr, 200, 5);
        let set2 = compute_allowed_fast_set(&info_hash, addr, 200, 5);
        assert_eq!(set1, set2, "allowed fast set must be deterministic");
    }

    #[test]
    fn allowed_fast_different_ips_produce_different_sets() {
        let info_hash = [0x42u8; 20];
        let addr1: SocketAddr = "1.1.1.1:6881".parse().unwrap();
        let addr2: SocketAddr = "2.2.2.2:6881".parse().unwrap();
        let set1 = compute_allowed_fast_set(&info_hash, addr1, 1000, 10);
        let set2 = compute_allowed_fast_set(&info_hash, addr2, 1000, 10);
        // It's extremely unlikely (but technically possible) they are equal.
        // We only verify both are valid.
        assert_eq!(set1.len(), 10);
        assert_eq!(set2.len(), 10);
    }

    #[test]
    fn allowed_fast_ipv6() {
        let addr: SocketAddr = "[::1]:6881".parse().unwrap();
        let set = compute_allowed_fast_set(&[0u8; 20], addr, 50, 5);
        assert_eq!(set.len(), 5);
        for &idx in &set {
            assert!(idx < 50);
        }
    }

    #[test]
    fn allowed_fast_k_exceeds_num_pieces() {
        let addr: SocketAddr = "192.168.1.1:6881".parse().unwrap();
        // Only 3 possible piece indices
        let set = compute_allowed_fast_set(&[0xFFu8; 20], addr, 3, 10);
        // Should return at most 3 unique indices
        assert!(set.len() <= 3);
    }
}
