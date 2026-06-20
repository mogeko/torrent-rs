//! Magnet link metadata exchange (BEP 9).
//!
//! When a torrent is added via a magnet link, the full [`Metainfo`]
//! must be downloaded from peers. BEP 9 defines two messages exchanged
//! via the LTEP extension protocol (BEP 10) under the `ut_metadata`
//! extension:
//!
//! - [`MetadataRequest`] — request a 16 KB piece of the info dictionary
//! - [`MetadataData`] — deliver a metadata piece (or reject)
//!
//! # Wire format
//!
//! Both messages are bencoded dictionaries wrapped in
//! [`PeerMessage::Extended`]. The extension ID is negotiated during
//! the LTEP handshake (see [`crate::peer::ExtensionNegotiation::metadata_size`]).
//!
//! Request (msg_type 0):
//! ```text
//! d8:msg_type i0e5:piece i0ee
//! ```
//!
//! Data (msg_type 1):
//! ```text
//! d8:msg_type i1e5:piece i0e10:total_size i12345e...data...e
//! ```
//!
//! Reject (msg_type 2, piece not available):
//! ```text
//! d8:msg_type i2e5:piece i0ee
//! ```
//!
//! [`Metainfo`]: crate::metainfo::Metainfo
//! [`PeerMessage::Extended`]: crate::peer::PeerMessage::Extended
//! [`ExtensionNegotiation::metadata_size`]: crate::peer::ExtensionNegotiation::metadata_size

use crate::bencode::{Bencode, Bytes, dict_get_int};
use crate::error::{Error, ErrorKind};

/// Extension name registered during LTEP handshake for BEP 9 metadata exchange.
pub const UT_METADATA_EXT: &str = "ut_metadata";

/// Recommended extension ID for `ut_metadata` (BEP 10).
pub const UT_METADATA_ID: u8 = 2;

/// Size of each metadata piece in bytes (BEP 9).
pub const METADATA_PIECE_SIZE: u64 = 16 * 1024; // 16 KB

/// Request a metadata piece from a peer (BEP 9, msg_type 0).
///
/// Sent after the LTEP handshake when the peer's
/// [`crate::peer::ExtensionNegotiation::metadata_size`] is known.
///
/// # Examples
///
/// ```
/// use torrent_core::peer::metadata::MetadataRequest;
///
/// let req = MetadataRequest { piece: 3 };
/// let ben = req.to_bencode();
/// let parsed = MetadataRequest::from_bencode(&ben).unwrap();
/// assert_eq!(parsed.piece, 3);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequest {
    /// Index of the 16 KB piece to request.
    pub piece: u32,
}

impl MetadataRequest {
    /// Serialize to a bencoded dictionary.
    pub fn to_bencode(&self) -> Bencode {
        Bencode::Dict(vec![
            (Bytes::from("msg_type"), Bencode::Integer(0)),
            (Bytes::from("piece"), Bencode::Integer(self.piece as i64)),
        ])
    }

    /// Deserialize from a bencoded dictionary.
    pub fn from_bencode(val: &Bencode) -> Result<Self, Error> {
        let msg_type = dict_get_int(val, b"msg_type")
            .ok_or_else(|| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
        if msg_type != 0 {
            return Err(Error::new(ErrorKind::PeerInvalidExtendedMessage));
        }
        let piece = dict_get_int(val, b"piece")
            .ok_or_else(|| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
        Ok(MetadataRequest {
            piece: piece as u32,
        })
    }
}

/// Metadata piece delivered by a peer (BEP 9, msg_type 1).
///
/// Contains one 16 KB piece of the bencoded info dictionary.
/// The `total_size` field gives the full metadata size so the
/// receiver knows how many pieces to request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataData {
    /// Piece index (corresponds to the request).
    pub piece: u32,
    /// Total metadata size in bytes.
    pub total_size: u64,
    /// Raw piece data (≤ 16 KB). Empty if the piece was rejected.
    pub data: Vec<u8>,
}

impl MetadataData {
    /// Serialize the bencoded dictionary prefix (without raw data).
    pub fn to_bencode_with_data(&self) -> Bencode {
        Bencode::Dict(vec![
            (Bytes::from("msg_type"), Bencode::Integer(1)),
            (Bytes::from("piece"), Bencode::Integer(self.piece as i64)),
            (
                Bytes::from("total_size"),
                Bencode::Integer(self.total_size as i64),
            ),
        ])
    }

    /// Deserialize from a bencoded dictionary with raw piece data.
    pub fn from_bencode(val: &Bencode, raw_data: Vec<u8>) -> Result<Self, Error> {
        let msg_type = dict_get_int(val, b"msg_type")
            .ok_or_else(|| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
        if msg_type != 1 {
            return Err(Error::new(ErrorKind::PeerInvalidExtendedMessage));
        }
        let piece = dict_get_int(val, b"piece")
            .ok_or_else(|| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
        let total_size = dict_get_int(val, b"total_size")
            .ok_or_else(|| Error::new(ErrorKind::PeerInvalidExtendedMessage))?;
        Ok(MetadataData {
            piece: piece as u32,
            total_size: total_size as u64,
            data: raw_data,
        })
    }

    /// Check if a bencoded dict is a metadata reject (msg_type 2).
    pub fn is_reject(val: &Bencode) -> bool {
        dict_get_int(val, b"msg_type") == Some(2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_request_roundtrip() {
        let req = MetadataRequest { piece: 5 };
        let ben = req.to_bencode();
        let parsed = MetadataRequest::from_bencode(&ben).unwrap();
        assert_eq!(parsed.piece, 5);
    }

    #[test]
    fn metadata_data_roundtrip() {
        let data = MetadataData {
            piece: 2,
            total_size: 32768,
            data: vec![0x41; 100],
        };
        let ben = data.to_bencode_with_data();
        let parsed = MetadataData::from_bencode(&ben, data.data.clone()).unwrap();
        assert_eq!(parsed.piece, 2);
        assert_eq!(parsed.total_size, 32768);
        assert_eq!(parsed.data.len(), 100);
    }

    #[test]
    fn metadata_reject_detected() {
        let ben = Bencode::Dict(vec![
            (Bytes::from("msg_type"), Bencode::Integer(2)),
            (Bytes::from("piece"), Bencode::Integer(0)),
        ]);
        assert!(MetadataData::is_reject(&ben));
    }

    #[test]
    fn metadata_request_wrong_msg_type() {
        let ben = Bencode::Dict(vec![
            (Bytes::from("msg_type"), Bencode::Integer(1)),
            (Bytes::from("piece"), Bencode::Integer(0)),
        ]);
        assert!(MetadataRequest::from_bencode(&ben).is_err());
    }

    #[test]
    fn metadata_piece_size_constant() {
        assert_eq!(METADATA_PIECE_SIZE, 16384);
    }
}
