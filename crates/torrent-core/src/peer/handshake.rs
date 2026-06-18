use crate::error::{Error, ErrorKind};

/// BitTorrent protocol string (always "BitTorrent protocol").
const PROTOCOL_STR: &[u8; 19] = b"BitTorrent protocol";

/// Total size of a handshake message: 1 + 19 + 8 + 20 + 20 = 68.
const HANDSHAKE_SIZE: usize = 68;

/// BitTorrent protocol handshake (BEP 3).
///
/// The handshake is a 68-byte message sent when a TCP connection is established:
///
/// ```text
/// | pstrlen (1) | pstr (19) | reserved (8) | info_hash (20) | peer_id (20) |
/// ```
///
/// # Examples
///
/// ```
/// use torrent_core::peer::Handshake;
///
/// let hs = Handshake::new([1u8; 20], [2u8; 20]);
/// let bytes = hs.to_bytes();
/// let parsed = Handshake::from_bytes(&bytes).unwrap();
/// assert_eq!(hs, parsed);
/// ```
///
/// Checking extension bits:
///
/// ```
/// use torrent_core::peer::Handshake;
///
/// let mut hs = Handshake::new([0u8; 20], [0u8; 20]);
/// // Set the DHT bit (bit 63: byte 7, LSB — BEP 5)
/// hs.reserved[7] = 0x01;
/// assert!(hs.has_extension(63));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    /// The info_hash of the torrent we want to download.
    pub info_hash: [u8; 20],
    /// The sender's peer ID.
    pub peer_id: [u8; 20],
    /// Reserved bytes for extension negotiation.
    pub reserved: [u8; 8],
}

impl Handshake {
    /// Create a new handshake with all extension bits cleared.
    pub fn new(info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        Handshake {
            info_hash,
            peer_id,
            reserved: [0u8; 8],
        }
    }

    /// Create a new handshake with the given extension bits set.
    ///
    /// `extensions` should be a list of bit indices (BEP 3 numbering: bit 0 = MSB of byte 0).
    /// Common extension bits:
    /// - Bit 44: Fast Extension (BEP 6)
    /// - Bit 63: DHT (BEP 5)
    /// - Bit 63: Extension Protocol / LTEP (BEP 10) — note: reuses same bit as DHT
    pub fn with_extensions(info_hash: [u8; 20], peer_id: [u8; 20], extensions: &[usize]) -> Self {
        let mut hs = Handshake::new(info_hash, peer_id);
        for &bit in extensions {
            if bit < 64 {
                let byte = bit / 8;
                let bit_in_byte = 7 - (bit % 8);
                hs.reserved[byte] |= 1 << bit_in_byte;
            }
        }
        hs
    }

    /// Serialize the handshake to a 68-byte array.
    pub fn to_bytes(&self) -> [u8; 68] {
        let mut buf = [0u8; HANDSHAKE_SIZE];
        buf[0] = 19; // pstrlen
        buf[1..20].copy_from_slice(PROTOCOL_STR.as_slice());
        buf[20..28].copy_from_slice(&self.reserved);
        buf[28..48].copy_from_slice(&self.info_hash);
        buf[48..68].copy_from_slice(&self.peer_id);
        buf
    }

    /// Deserialize a handshake from exactly 68 bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, Error> {
        tracing::debug!("parsing handshake");
        if data.len() != HANDSHAKE_SIZE {
            tracing::warn!(
                "handshake: wrong size (expected {} got {})",
                HANDSHAKE_SIZE,
                data.len()
            );
            return Err(Error::new(ErrorKind::PeerInvalidHandshake));
        }
        if data[0] != 19 {
            return Err(Error::new(ErrorKind::PeerInvalidHandshake));
        }
        if &data[1..20] != PROTOCOL_STR.as_slice() {
            return Err(Error::new(ErrorKind::PeerInvalidHandshake));
        }

        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&data[20..28]);

        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&data[28..48]);

        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&data[48..68]);

        Ok(Handshake {
            info_hash,
            peer_id,
            reserved,
        })
    }

    /// Check if a specific extension bit is set in the reserved bytes.
    ///
    /// Bit numbering follows BEP 3 conventions where bit 0 is the most significant
    /// bit of byte 0. Common extensions:
    ///
    /// - Bit 44 (byte 5, bit 4): Fast Extension (BEP 6)
    /// - Bit 63 (byte 7, bit 7): DHT (BEP 5) and Extension Protocol / LTEP (BEP 10)
    pub fn has_extension(&self, bit: usize) -> bool {
        if bit >= 64 {
            return false;
        }
        let byte = bit / 8;
        let bit_in_byte = 7 - (bit % 8); // MSB first
        (self.reserved[byte] >> bit_in_byte) & 1 == 1
    }

    /// Set a reserved byte directly (e.g., for BEP 10 extension protocol).
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `index >= 8`.
    pub fn set_reserved_byte(&mut self, index: usize, value: u8) {
        debug_assert!(index < 8, "reserved byte index must be < 8, got {index}");
        if index < 8 {
            self.reserved[index] = value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_to_bytes() {
        let hs = Handshake::new([1u8; 20], [2u8; 20]);
        let bytes = hs.to_bytes();
        assert_eq!(bytes.len(), 68);
        assert_eq!(bytes[0], 19);
        assert_eq!(&bytes[1..20], b"BitTorrent protocol");
    }

    #[test]
    fn handshake_from_bytes() {
        let original = Handshake::new([1u8; 20], [2u8; 20]);
        let bytes = original.to_bytes();
        let parsed = Handshake::from_bytes(&bytes).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn handshake_roundtrip() {
        let mut hs = Handshake::new([0xAB; 20], [0xCD; 20]);
        hs.reserved = [1, 2, 3, 4, 5, 6, 7, 8];
        let bytes = hs.to_bytes();
        let parsed = Handshake::from_bytes(&bytes).unwrap();
        assert_eq!(hs, parsed);
    }

    #[test]
    fn handshake_reject_invalid_pstrlen() {
        let mut bytes = Handshake::new([1u8; 20], [2u8; 20]).to_bytes();
        bytes[0] = 18; // wrong pstrlen
        assert!(Handshake::from_bytes(&bytes).is_err());
    }

    #[test]
    fn handshake_reject_invalid_pstr() {
        let mut bytes = Handshake::new([1u8; 20], [2u8; 20]).to_bytes();
        bytes[1] = b'X'; // corrupt protocol string
        assert!(Handshake::from_bytes(&bytes).is_err());
    }

    #[test]
    fn handshake_reject_wrong_size() {
        let bytes = vec![0u8; 67];
        assert!(Handshake::from_bytes(&bytes).is_err());
        let bytes = vec![0u8; 69];
        assert!(Handshake::from_bytes(&bytes).is_err());
    }

    #[test]
    fn handshake_has_extension() {
        let mut hs = Handshake::new([1u8; 20], [2u8; 20]);
        // Set bit 44 (byte 5, bit 4 from MSB = LSB bit 3) — BEP 6 Fast Extension
        // bit 44 → byte 5, shift = 7 - (44%8) = 3
        hs.reserved[5] = 0x08;
        assert!(hs.has_extension(44));
        // DHT/LTEP bit (63) should not be set
        assert!(!hs.has_extension(63));
    }

    #[test]
    fn handshake_extension_out_of_range() {
        let hs = Handshake::new([1u8; 20], [2u8; 20]);
        assert!(!hs.has_extension(64));
    }

    #[test]
    fn handshake_with_extensions() {
        let hs = Handshake::with_extensions([0u8; 20], [0u8; 20], &[44, 63]);
        assert!(hs.has_extension(44));
        assert!(hs.has_extension(63));
        assert!(!hs.has_extension(43));
    }
}
