//! uTP packet header (BEP 29).
//!
//! The uTP header is a fixed 20-byte structure sent at the beginning of every
//! UDP datagram. All multi-byte fields are in network byte order (big-endian).
//!
//! # Header Layout (version 1)
//!
//! ```text
//!  0       4       8               16              24              32
//! +-------+-------+---------------+---------------+---------------+
//! | type  | ver   | extension     | connection_id                 |
//! +-------+-------+---------------+---------------+---------------+
//! | timestamp_microseconds                                        |
//! +---------------+---------------+---------------+---------------+
//! | timestamp_difference_microseconds                             |
//! +---------------+---------------+---------------+---------------+
//! | wnd_size                                                      |
//! +---------------+---------------+---------------+---------------+
//! | seq_nr                        | ack_nr                        |
//! +---------------+---------------+---------------+---------------+
//! ```
//!
//! # Key Differences from TCP
//!
//! - Sequence numbers refer to **packets**, not bytes.
//! - `ST_STATE` packets (pure ACK) do **not** increment `seq_nr`.
//! - `connection_id` has a +1 relationship: initiator picks a random ID,
//!   responder uses ID+1 for packets sent back.

use crate::error::{Error, ErrorKind};

/// Total size of a uTP header in bytes (BEP 29 version 1).
pub const UTP_HEADER_SIZE: usize = 20;

/// Current uTP protocol version.
pub const UTP_VERSION: u8 = 1;

/// uTP packet type (BEP 29 §header format).
///
/// The type occupies the high 4 bits of byte 0 in the header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UtpType {
    /// Regular data packet. The socket is connected and has data to send.
    /// Always carries a payload.
    StData = 0,
    /// Finalize the connection. Last packet — seq_nr will never go higher.
    /// The socket records this as `eof_pkt` and continues receiving
    /// out-of-order packets with lower sequence numbers.
    StFin = 1,
    /// State packet. Pure ACK with no data payload.
    /// **Does not increment seq_nr.**
    StState = 2,
    /// Terminate connection forcefully (like TCP RST).
    /// The remote host has no state for this connection.
    StReset = 3,
    /// Connect SYN (like TCP SYN). Initiates a connection.
    /// seq_nr is initialized to 1. connection_id is random.
    StSyn = 4,
}

impl UtpType {
    /// Parse a uTP packet type from a raw byte value.
    ///
    /// Returns `None` if the value does not correspond to a known type.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(UtpType::StData),
            1 => Some(UtpType::StFin),
            2 => Some(UtpType::StState),
            3 => Some(UtpType::StReset),
            4 => Some(UtpType::StSyn),
            _ => None,
        }
    }

    /// Returns true if this packet type increments the sequence number.
    ///
    /// `ST_STATE` and `ST_RESET` do not increment `seq_nr`.
    pub fn increments_seq_nr(self) -> bool {
        matches!(self, UtpType::StData | UtpType::StFin | UtpType::StSyn)
    }
}

/// A uTP packet header (BEP 29 §header format).
///
/// All multi-byte fields use network byte order (big-endian).
///
/// # Examples
///
/// ```
/// use torrent_core::peer::utp::{UtpHeader, UtpType};
///
/// let hdr = UtpHeader {
///     utp_type: UtpType::StSyn,
///     version: 1,
///     extension: 0,
///     connection_id: 0x1234,
///     timestamp_microseconds: 0,
///     timestamp_difference_microseconds: 0,
///     wnd_size: 65536,
///     seq_nr: 1,
///     ack_nr: 0,
/// };
///
/// let bytes = hdr.to_bytes();
/// let parsed = UtpHeader::from_bytes(&bytes).unwrap();
/// assert_eq!(hdr, parsed);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UtpHeader {
    /// Packet type (ST_DATA, ST_FIN, ST_STATE, ST_RESET, ST_SYN).
    pub utp_type: UtpType,
    /// Protocol version (currently 1).
    pub version: u8,
    /// Next extension type in linked list, or 0 for none.
    pub extension: u8,
    /// Random identifier for this connection.
    /// Initiator picks; responder uses `connection_id + 1`.
    pub connection_id: u16,
    /// Microsecond timestamp when this packet was sent.
    /// 32-bit value wraps approximately every 71 minutes.
    pub timestamp_microseconds: u32,
    /// Difference between local time and the timestamp in the last
    /// received packet, at the time that packet was received.
    /// This is the latest one-way delay measurement.
    /// Set to 0 when no delay samples exist yet.
    pub timestamp_difference_microseconds: u32,
    /// Advertised receive window in **bytes** (not packets).
    /// The number of bytes left in the socket's receive buffer.
    pub wnd_size: u32,
    /// Sequence number of this **packet** (not byte offset).
    /// ST_STATE packets do not increment this.
    pub seq_nr: u16,
    /// Sequence number last received from the other direction.
    pub ack_nr: u16,
}

impl UtpHeader {
    /// Create a new uTP header with minimal defaults.
    ///
    /// `timestamp_difference_microseconds` is set to 0 (no delay sample yet).
    pub fn new(utp_type: UtpType, connection_id: u16, seq_nr: u16, ack_nr: u16) -> Self {
        UtpHeader {
            utp_type,
            version: UTP_VERSION,
            extension: 0,
            connection_id,
            timestamp_microseconds: 0,
            timestamp_difference_microseconds: 0,
            wnd_size: 0,
            seq_nr,
            ack_nr,
        }
    }

    /// Serialize the header to a 20-byte array in network byte order.
    pub fn to_bytes(&self) -> [u8; UTP_HEADER_SIZE] {
        let mut buf = [0u8; UTP_HEADER_SIZE];

        // Byte 0: type (high 4 bits) | version (low 4 bits)
        buf[0] = ((self.utp_type as u8) << 4) | (self.version & 0x0F);

        // Byte 1: extension
        buf[1] = self.extension;

        // Bytes 2-3: connection_id (u16 big-endian)
        buf[2..4].copy_from_slice(&self.connection_id.to_be_bytes());

        // Bytes 4-7: timestamp_microseconds (u32 big-endian)
        buf[4..8].copy_from_slice(&self.timestamp_microseconds.to_be_bytes());

        // Bytes 8-11: timestamp_difference_microseconds (u32 big-endian)
        buf[8..12].copy_from_slice(&self.timestamp_difference_microseconds.to_be_bytes());

        // Bytes 12-15: wnd_size (u32 big-endian)
        buf[12..16].copy_from_slice(&self.wnd_size.to_be_bytes());

        // Bytes 16-17: seq_nr (u16 big-endian)
        buf[16..18].copy_from_slice(&self.seq_nr.to_be_bytes());

        // Bytes 18-19: ack_nr (u16 big-endian)
        buf[18..20].copy_from_slice(&self.ack_nr.to_be_bytes());

        buf
    }

    /// Deserialize a uTP header from exactly 20 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::PeerUtpInvalidHeader`] if:
    /// - The version is not 1
    /// - The packet type is unknown
    pub fn from_bytes(data: &[u8; UTP_HEADER_SIZE]) -> Result<Self, Error> {
        // Byte 0: type (high 4 bits) | version (low 4 bits)
        let type_raw = data[0] >> 4;
        let version = data[0] & 0x0F;

        if version != UTP_VERSION {
            tracing::warn!("uTP: unsupported version {}", version);
            return Err(Error::new(ErrorKind::PeerUtpInvalidHeader));
        }

        let utp_type = UtpType::from_u8(type_raw).ok_or_else(|| {
            tracing::warn!("uTP: unknown packet type {}", type_raw);
            Error::new(ErrorKind::PeerUtpInvalidHeader)
        })?;

        // Byte 1: extension
        let extension = data[1];

        // Bytes 2-3: connection_id
        let connection_id = u16::from_be_bytes([data[2], data[3]]);

        // Bytes 4-7: timestamp_microseconds
        let timestamp_microseconds = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);

        // Bytes 8-11: timestamp_difference_microseconds
        let timestamp_difference_microseconds =
            u32::from_be_bytes([data[8], data[9], data[10], data[11]]);

        // Bytes 12-15: wnd_size
        let wnd_size = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);

        // Bytes 16-17: seq_nr
        let seq_nr = u16::from_be_bytes([data[16], data[17]]);

        // Bytes 18-19: ack_nr
        let ack_nr = u16::from_be_bytes([data[18], data[19]]);

        Ok(UtpHeader {
            utp_type,
            version,
            extension,
            connection_id,
            timestamp_microseconds,
            timestamp_difference_microseconds,
            wnd_size,
            seq_nr,
            ack_nr,
        })
    }

    /// Returns true if this packet type increments the sequence number.
    pub fn increments_seq_nr(&self) -> bool {
        self.utp_type.increments_seq_nr()
    }

    /// Returns true if this is a data packet (ST_DATA) that carries payload.
    pub fn is_data(&self) -> bool {
        self.utp_type == UtpType::StData
    }

    /// Returns true if this is a SYN packet (connection initiation).
    pub fn is_syn(&self) -> bool {
        self.utp_type == UtpType::StSyn
    }

    /// Returns true if this is a FIN packet (connection close).
    pub fn is_fin(&self) -> bool {
        self.utp_type == UtpType::StFin
    }

    /// Returns true if this is a RESET packet (force close).
    pub fn is_reset(&self) -> bool {
        self.utp_type == UtpType::StReset
    }

    /// Returns true if this is a state packet (pure ACK, no payload).
    pub fn is_state(&self) -> bool {
        self.utp_type == UtpType::StState
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- UtpType tests ---

    #[test]
    fn utp_type_from_u8_valid() {
        assert_eq!(UtpType::from_u8(0), Some(UtpType::StData));
        assert_eq!(UtpType::from_u8(1), Some(UtpType::StFin));
        assert_eq!(UtpType::from_u8(2), Some(UtpType::StState));
        assert_eq!(UtpType::from_u8(3), Some(UtpType::StReset));
        assert_eq!(UtpType::from_u8(4), Some(UtpType::StSyn));
    }

    #[test]
    fn utp_type_from_u8_invalid() {
        assert_eq!(UtpType::from_u8(5), None);
        assert_eq!(UtpType::from_u8(255), None);
    }

    #[test]
    fn utp_type_increments_seq_nr() {
        assert!(UtpType::StData.increments_seq_nr());
        assert!(UtpType::StFin.increments_seq_nr());
        assert!(UtpType::StSyn.increments_seq_nr());
        assert!(!UtpType::StState.increments_seq_nr());
        assert!(!UtpType::StReset.increments_seq_nr());
    }

    // --- UtpHeader tests ---

    #[test]
    fn header_roundtrip_st_data() {
        let hdr = UtpHeader {
            utp_type: UtpType::StData,
            version: 1,
            extension: 0,
            connection_id: 0xABCD,
            timestamp_microseconds: 0x12345678,
            timestamp_difference_microseconds: 0x00001111,
            wnd_size: 65536,
            seq_nr: 42,
            ack_nr: 41,
        };
        let bytes = hdr.to_bytes();
        let parsed = UtpHeader::from_bytes(&bytes).unwrap();
        assert_eq!(hdr, parsed);
    }

    #[test]
    fn header_roundtrip_st_syn() {
        let hdr = UtpHeader {
            utp_type: UtpType::StSyn,
            version: 1,
            extension: 0,
            connection_id: 0xBEEF,
            timestamp_microseconds: 0,
            timestamp_difference_microseconds: 0,
            wnd_size: 1_000_000,
            seq_nr: 1,
            ack_nr: 0,
        };
        let bytes = hdr.to_bytes();
        let parsed = UtpHeader::from_bytes(&bytes).unwrap();
        assert_eq!(hdr, parsed);
    }

    #[test]
    fn header_roundtrip_st_state() {
        // ST_STATE: pure ACK, seq_nr not incremented by sender
        let hdr = UtpHeader {
            utp_type: UtpType::StState,
            version: 1,
            extension: 0,
            connection_id: 0x1111,
            timestamp_microseconds: 0xDEADBEEF,
            timestamp_difference_microseconds: 500,
            wnd_size: 0,
            seq_nr: 99,
            ack_nr: 5,
        };
        let bytes = hdr.to_bytes();
        let parsed = UtpHeader::from_bytes(&bytes).unwrap();
        assert_eq!(hdr, parsed);
    }

    #[test]
    fn header_roundtrip_st_fin() {
        let hdr = UtpHeader {
            utp_type: UtpType::StFin,
            version: 1,
            extension: 0,
            connection_id: 0x2222,
            timestamp_microseconds: 0,
            timestamp_difference_microseconds: 0,
            wnd_size: 0,
            seq_nr: 100,
            ack_nr: 99,
        };
        let bytes = hdr.to_bytes();
        let parsed = UtpHeader::from_bytes(&bytes).unwrap();
        assert_eq!(hdr, parsed);
    }

    #[test]
    fn header_roundtrip_st_reset() {
        let hdr = UtpHeader {
            utp_type: UtpType::StReset,
            version: 1,
            extension: 0,
            connection_id: 0x3333,
            timestamp_microseconds: 0,
            timestamp_difference_microseconds: 0,
            wnd_size: 0,
            seq_nr: 0,
            ack_nr: 0,
        };
        let bytes = hdr.to_bytes();
        let parsed = UtpHeader::from_bytes(&bytes).unwrap();
        assert_eq!(hdr, parsed);
    }

    #[test]
    fn header_with_extension_field() {
        let hdr = UtpHeader {
            utp_type: UtpType::StData,
            version: 1,
            extension: 1, // Selective ACK
            connection_id: 0x4444,
            timestamp_microseconds: 0,
            timestamp_difference_microseconds: 0,
            wnd_size: 0,
            seq_nr: 10,
            ack_nr: 9,
        };
        let bytes = hdr.to_bytes();
        let parsed = UtpHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.extension, 1);
        assert_eq!(hdr, parsed);
    }

    #[test]
    fn header_invalid_version() {
        let mut bytes = [0u8; 20];
        // Set version to 2 in low 4 bits of byte 0
        bytes[0] = (UtpType::StData as u8) << 4 | 2;
        let result = UtpHeader::from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn header_unknown_type() {
        let mut bytes = [0u8; 20];
        // Set type to 7 (unknown) in high 4 bits of byte 0
        bytes[0] = 7 << 4 | 1;
        let result = UtpHeader::from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn header_size_is_20() {
        assert_eq!(UTP_HEADER_SIZE, 20);
        let hdr = UtpHeader::new(UtpType::StData, 1, 1, 0);
        assert_eq!(hdr.to_bytes().len(), 20);
    }

    #[test]
    fn header_seq_nr_wraparound() {
        // u16::MAX wraps to 0 when incremented in uTP
        let hdr = UtpHeader {
            utp_type: UtpType::StData,
            version: 1,
            extension: 0,
            connection_id: 1,
            timestamp_microseconds: 0,
            timestamp_difference_microseconds: 0,
            wnd_size: 0,
            seq_nr: u16::MAX,
            ack_nr: u16::MAX - 1,
        };
        let bytes = hdr.to_bytes();
        let parsed = UtpHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.seq_nr, u16::MAX);
        assert_eq!(parsed.ack_nr, u16::MAX - 1);
    }

    #[test]
    fn header_max_wnd_size() {
        let hdr = UtpHeader {
            utp_type: UtpType::StData,
            version: 1,
            extension: 0,
            connection_id: 1,
            timestamp_microseconds: 0,
            timestamp_difference_microseconds: 0,
            wnd_size: u32::MAX,
            seq_nr: 0,
            ack_nr: 0,
        };
        let bytes = hdr.to_bytes();
        let parsed = UtpHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.wnd_size, u32::MAX);
    }

    // --- Convenience method tests ---

    #[test]
    fn is_methods() {
        let data = UtpHeader::new(UtpType::StData, 1, 1, 0);
        assert!(data.is_data());
        assert!(!data.is_syn());
        assert!(!data.is_fin());
        assert!(!data.is_reset());
        assert!(!data.is_state());

        let syn = UtpHeader::new(UtpType::StSyn, 1, 1, 0);
        assert!(syn.is_syn());
        assert!(!syn.is_data());

        let fin = UtpHeader::new(UtpType::StFin, 1, 1, 0);
        assert!(fin.is_fin());

        let reset = UtpHeader::new(UtpType::StReset, 1, 1, 0);
        assert!(reset.is_reset());

        let state = UtpHeader::new(UtpType::StState, 1, 1, 0);
        assert!(state.is_state());
    }
}
