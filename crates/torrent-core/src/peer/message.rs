use crate::error::{Error, ErrorKind};

/// A peer wire protocol message (BEP 3).
///
/// Messages follow a fixed-length header format:
///
/// ```text
/// <4-byte big-endian length> <1-byte message ID> <payload>
/// ```
///
/// Currently 11 message types are supported, from `KeepAlive` (length = 0)
/// to `Port` (length = 3).
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
}

/// Encode a `PeerMessage` to its wire format bytes.
///
/// ```text
/// Format: <4-byte big-endian length prefix> <1-byte message id> <payload>
/// Keep-alive: <4-byte 0>
/// ```
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
            let len = 1u32 + u32::try_from(bitfield.len()).unwrap_or(u32::MAX);
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
            let len = 9u32 + u32::try_from(data.len()).unwrap_or(u32::MAX);
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
    }
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
        _ => Err(Error::new(ErrorKind::PeerInvalidMessage)),
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
        // length=1, id=255 (invalid)
        let data = [0, 0, 0, 1, 255];
        assert!(decode(&data).is_err());
    }
}
