//! Selective ACK extension for uTP (BEP 29 §extension).
//!
//! Selective ACK allows acknowledging packets non-sequentially. It is sent
//! when at least one sequence number has been skipped in the received stream.
//!
//! # Extension Format
//!
//! ```text
//!  0                8               16
//! +----------------+---------------+---------------+---------------+
//! | next_extension | len           | bitmask (len bytes)           |
//! +----------------+---------------+---------------+---------------+
//!                                  |                               |
//! +----------------+---------------+---------------+---------------+
//! ```
//!
//! - `next_extension`: type of the next extension in the linked list (0 = end).
//! - `len`: number of bytes of **this** extension's data (bitmask).
//!   Must be at least 4 and a multiple of 4.
//!   Does NOT include `next_extension` and `len` themselves.
//!
//! # Bitmask Layout
//!
//! The first bit in the bitmask represents `ack_nr + 2` (the packet
//! immediately after the missing one — `ack_nr + 1` is assumed lost).
//!
//! Bits within each byte are in **reverse order**: LSB represents the
//! smallest sequence number, MSB the largest.
//!
//! ```text
//! Byte 0: [ack_nr+9, ack_nr+8, ..., ack_nr+2]  (LSB = ack_nr+2)
//! Byte 1: [ack_nr+17, ack_nr+16, ..., ack_nr+10]
//! ...
//! ```
//!
//! A set bit (1) means the packet was received; a cleared bit (0) means
//! it has not been received.

use crate::error::{Error, ErrorKind};

/// Minimum size of the Selective ACK bitmask in bytes (must be multiple of 4).
pub const SELECTIVE_ACK_MIN_BYTES: usize = 4;

/// Selective ACK extension (BEP 29 §extension — SELECTIVE ACK).
///
/// Carries a bitmask acknowledging packets beyond the cumulative `ack_nr`.
/// Each set bit represents a received packet; each cleared bit represents
/// a missing packet.
///
/// # Examples
///
/// ```
/// use torrent_core::peer::utp::SelectiveAck;
///
/// // Create a Selective ACK acknowledging ack_nr+2 and ack_nr+5
/// let mut sack = SelectiveAck::new(0);
/// sack.set(0);  // ack_nr + 2
/// sack.set(3);  // ack_nr + 5
///
/// assert!(sack.acknowledges(0));
/// assert!(!sack.acknowledges(1)); // ack_nr+3 — not received
/// assert!(sack.acknowledges(3));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectiveAck {
    /// Type of the next extension in the linked list (0 = end of list).
    pub next_extension: u8,
    /// Bitmask where each bit represents a packet.
    /// Bits are indexed from `ack_nr + 2` upward.
    bitmask: Vec<u8>,
}

impl SelectiveAck {
    /// Create a new empty Selective ACK with a bitmask of at least
    /// `SELECTIVE_ACK_MIN_BYTES` (4 bytes, i.e., 32 bits).
    ///
    /// `next_extension` is the next extension type (0 = end of list).
    pub fn new(next_extension: u8) -> Self {
        SelectiveAck {
            next_extension,
            bitmask: vec![0u8; SELECTIVE_ACK_MIN_BYTES],
        }
    }

    /// Create a Selective ACK from raw bitmask bytes.
    ///
    /// `bitmask` length must be at least 4 and a multiple of 4.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::PeerUtpInvalidHeader`] if the bitmask length
    /// is invalid.
    pub fn from_bytes(next_extension: u8, bitmask: Vec<u8>) -> Result<Self, Error> {
        let len = bitmask.len();
        if len < SELECTIVE_ACK_MIN_BYTES || !len.is_multiple_of(4) {
            tracing::warn!(
                "uTP: invalid Selective ACK bitmask length {} (must be >= 4, multiple of 4)",
                len
            );
            return Err(Error::new(ErrorKind::PeerUtpInvalidHeader));
        }
        Ok(SelectiveAck {
            next_extension,
            bitmask,
        })
    }

    /// Set the bit for the packet at the given offset from `ack_nr + 2`.
    ///
    /// `offset` 0 corresponds to `ack_nr + 2`, offset 1 to `ack_nr + 3`, etc.
    /// If `offset` is beyond the current bitmask size, the bitmask is
    /// automatically extended (rounded up to the next multiple of 4 bytes).
    pub fn set(&mut self, offset: usize) {
        let byte_idx = offset / 8;
        let bit_in_byte = offset % 8;
        self.ensure_capacity(byte_idx + 1);
        // LSB = smallest seq_nr (offset 0 = ack_nr+2)
        self.bitmask[byte_idx] |= 1 << bit_in_byte;
    }

    /// Clear the bit for the packet at the given offset.
    pub fn clear(&mut self, offset: usize) {
        let byte_idx = offset / 8;
        if byte_idx < self.bitmask.len() {
            let bit_in_byte = offset % 8;
            self.bitmask[byte_idx] &= !(1u8 << bit_in_byte);
        }
    }

    /// Check if the packet at the given offset is acknowledged.
    ///
    /// Returns `false` if the offset is beyond the bitmask.
    pub fn acknowledges(&self, offset: usize) -> bool {
        let byte_idx = offset / 8;
        if byte_idx >= self.bitmask.len() {
            return false;
        }
        let bit_in_byte = offset % 8;
        (self.bitmask[byte_idx] >> bit_in_byte) & 1 == 1
    }

    /// Count how many packets are acknowledged in this Selective ACK.
    pub fn count_acked(&self) -> usize {
        self.bitmask.iter().map(|b| b.count_ones() as usize).sum()
    }

    /// Count how many packets in the given range `[0, max_offset)` are
    /// acknowledged. This is the number of duplicate ACKs that can be
    /// counted for loss detection.
    pub fn count_acked_in_range(&self, max_offset: usize) -> usize {
        let max_byte = max_offset.div_ceil(8);
        let limit = max_byte.min(self.bitmask.len());
        let mut count = 0usize;
        for (byte_idx, &byte) in self.bitmask[..limit].iter().enumerate() {
            let bits_in_this_byte = if byte_idx == limit - 1 {
                let remaining = max_offset - byte_idx * 8;
                remaining.min(8)
            } else {
                8
            };
            // Mask to only count bits within range
            let mask = if bits_in_this_byte == 8 {
                0xFF
            } else {
                (1u8 << bits_in_this_byte) - 1
            };
            count += (byte & mask).count_ones() as usize;
        }
        count
    }

    /// Serialize the Selective ACK extension to bytes.
    ///
    /// Returns `(next_extension, len, bitmask)` as a flat byte vector
    /// suitable for appending after the uTP header.
    pub fn to_extension_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + self.bitmask.len());
        buf.push(self.next_extension);
        buf.push(self.bitmask.len() as u8);
        buf.extend_from_slice(&self.bitmask);
        buf
    }

    /// Returns the total number of bits in this bitmask.
    pub fn bit_count(&self) -> usize {
        self.bitmask.len() * 8
    }

    /// Returns the raw bitmask bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bitmask
    }

    /// Returns the length of the bitmask in bytes.
    pub fn len_bytes(&self) -> usize {
        self.bitmask.len()
    }

    /// Returns true if the bitmask is empty (should not happen; minimum is 4 bytes).
    pub fn is_empty(&self) -> bool {
        self.bitmask.is_empty()
    }

    /// Ensure the bitmask has at least `min_bytes` capacity, rounding up
    /// to the next multiple of 4.
    fn ensure_capacity(&mut self, min_bytes: usize) {
        if min_bytes > self.bitmask.len() {
            let new_len = min_bytes.div_ceil(4) * 4; // round up to multiple of 4
            self.bitmask.resize(new_len, 0);
        }
    }

    /// Returns an iterator over the offsets of all acknowledged packets.
    pub fn iter_acked(&self) -> impl Iterator<Item = usize> + '_ {
        self.bitmask
            .iter()
            .enumerate()
            .flat_map(|(byte_idx, &byte)| {
                (0..8).filter_map(move |bit| {
                    if (byte >> bit) & 1 == 1 {
                        Some(byte_idx * 8 + bit)
                    } else {
                        None
                    }
                })
            })
    }
}

/// Parse a linked list of uTP extensions from the bytes following a uTP header.
///
/// Returns a list of `(extension_type, extension_data)` pairs.
/// Unknown extensions are skipped by reading their `len` field.
///
/// `data` should be the bytes immediately after the 20-byte uTP header.
/// `first_extension_type` is the `extension` field from the uTP header
/// (0 means no extensions).
pub fn parse_extensions(
    first_extension_type: u8, data: &[u8],
) -> Result<Vec<(u8, Vec<u8>)>, Error> {
    let mut extensions = Vec::new();
    let mut ext_type = first_extension_type;
    let mut offset = 0usize;

    while ext_type != 0 {
        if offset + 2 > data.len() {
            tracing::warn!("uTP: extension header truncated at offset {}", offset);
            return Err(Error::new(ErrorKind::PeerUtpInvalidHeader));
        }

        let next_ext = data[offset];
        let len = data[offset + 1] as usize;
        offset += 2;

        if offset + len > data.len() {
            tracing::warn!(
                "uTP: extension data truncated (need {} bytes, have {})",
                offset + len,
                data.len()
            );
            return Err(Error::new(ErrorKind::PeerUtpInvalidHeader));
        }

        let ext_data = data[offset..offset + len].to_vec();
        extensions.push((ext_type, ext_data));
        offset += len;
        ext_type = next_ext;
    }

    Ok(extensions)
}

/// Parse a Selective ACK from extension data bytes.
///
/// The extension data should be the raw bitmask bytes (without the
/// `next_extension` and `len` fields, which are handled by `parse_extensions`).
pub fn parse_selective_ack(next_extension: u8, data: &[u8]) -> Result<SelectiveAck, Error> {
    SelectiveAck::from_bytes(next_extension, data.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- SelectiveAck bit indexing tests ---

    #[test]
    fn selective_ack_bit_ordering() {
        // BEP 29: LSB of byte 0 = ack_nr+2, MSB = ack_nr+9
        let mut sack = SelectiveAck::new(0);

        // Set ack_nr+2 (offset 0) → LSB of byte 0
        sack.set(0);
        assert_eq!(sack.bitmask[0], 0b0000_0001);
        assert!(sack.acknowledges(0));
        assert!(!sack.acknowledges(1));

        // Set ack_nr+9 (offset 7) → MSB of byte 0
        sack.set(7);
        assert_eq!(sack.bitmask[0], 0b1000_0001);
        assert!(sack.acknowledges(7));

        // Set ack_nr+10 (offset 8) → LSB of byte 1
        sack.set(8);
        assert_eq!(sack.bitmask[1], 0b0000_0001);
        assert!(sack.acknowledges(8));
    }

    #[test]
    fn selective_ack_roundtrip_bytes() {
        let mut sack = SelectiveAck::new(0);
        sack.set(0);
        sack.set(3);
        sack.set(15);

        let ext_bytes = sack.to_extension_bytes();
        // [next_extension=0, len=4, 4 bytes bitmask]
        assert_eq!(ext_bytes.len(), 2 + 4);
        assert_eq!(ext_bytes[0], 0); // next_extension
        assert_eq!(ext_bytes[1], 4); // len (4 bytes)

        let parsed = SelectiveAck::from_bytes(0, ext_bytes[2..].to_vec()).unwrap();
        assert_eq!(sack, parsed);
    }

    #[test]
    fn selective_ack_auto_grow() {
        let mut sack = SelectiveAck::new(0);
        // Set a bit far beyond 32 bits — should auto-expand
        sack.set(50);
        // bitmask should now be at least ceil(51/8)=7, rounded to 8 bytes
        assert!(sack.bitmask.len() >= 8);
        assert!(sack.acknowledges(50));
        assert!(!sack.acknowledges(49));
        // Length must be multiple of 4
        assert_eq!(sack.bitmask.len() % 4, 0);
    }

    #[test]
    fn selective_ack_clear() {
        let mut sack = SelectiveAck::new(0);
        sack.set(5);
        assert!(sack.acknowledges(5));
        sack.clear(5);
        assert!(!sack.acknowledges(5));
    }

    #[test]
    fn selective_ack_clear_out_of_bounds() {
        let mut sack = SelectiveAck::new(0);
        // Clearing beyond bitmask should not panic
        sack.clear(100);
    }

    #[test]
    fn selective_ack_count() {
        let mut sack = SelectiveAck::new(0);
        assert_eq!(sack.count_acked(), 0);
        sack.set(0);
        sack.set(3);
        sack.set(7);
        sack.set(31);
        assert_eq!(sack.count_acked(), 4);
    }

    #[test]
    fn selective_ack_count_in_range() {
        let mut sack = SelectiveAck::new(0);
        sack.set(0); // offset 0
        sack.set(5); // offset 5
        sack.set(10); // offset 10 — beyond range

        // Count offsets 0..8 (first 8 bits)
        assert_eq!(sack.count_acked_in_range(8), 2);
        // Count offsets 0..6
        assert_eq!(sack.count_acked_in_range(6), 2);
        // Count offsets 0..1
        assert_eq!(sack.count_acked_in_range(1), 1);
    }

    #[test]
    fn selective_ack_iter_acked() {
        let mut sack = SelectiveAck::new(0);
        sack.set(0);
        sack.set(3);
        sack.set(7);
        let acked: Vec<usize> = sack.iter_acked().collect();
        assert_eq!(acked, vec![0, 3, 7]);
    }

    #[test]
    fn selective_ack_invalid_length() {
        let result = SelectiveAck::from_bytes(0, vec![0u8; 3]); // too short
        assert!(result.is_err());
        let result = SelectiveAck::from_bytes(0, vec![0u8; 5]); // not multiple of 4
        assert!(result.is_err());
    }

    #[test]
    fn selective_ack_next_extension_preserved() {
        let sack = SelectiveAck::new(2); // next_extension = 2
        let bytes = sack.to_extension_bytes();
        assert_eq!(bytes[0], 2); // next_extension in serialized form
    }

    // --- parse_extensions tests ---

    #[test]
    fn parse_extensions_single() {
        // Extension type 1 (Selective ACK), len=4, bitmask of 4 zero bytes
        let data = [0u8, 4, 0, 0, 0, 0]; // next=0, len=4, bitmask
        let result = parse_extensions(1, &data).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 1); // extension type
        assert_eq!(result[0].1.len(), 4); // bitmask data
    }

    #[test]
    fn parse_extensions_linked_list() {
        // Extension 1: len=4, next=2 → Extension 2: len=4, next=0
        let data = [
            2u8, 4, 0, 0, 0, 0, // ext 1: next=2, len=4, 4 bytes
            0u8, 4, 1, 1, 1, 1, // ext 2: next=0, len=4, 4 bytes
        ];
        let result = parse_extensions(1, &data).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, 1);
        assert_eq!(result[0].1, vec![0, 0, 0, 0]);
        assert_eq!(result[1].0, 2);
        assert_eq!(result[1].1, vec![1, 1, 1, 1]);
    }

    #[test]
    fn parse_extensions_no_extensions() {
        let result = parse_extensions(0, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_extensions_truncated_header() {
        // Only 1 byte where we need 2 (next + len)
        let result = parse_extensions(1, &[0u8]);
        assert!(result.is_err());
    }

    #[test]
    fn parse_extensions_truncated_data() {
        // next=0, len=8, but only 4 bytes follow
        let data = [0u8, 8, 0, 0, 0, 0];
        let result = parse_extensions(1, &data);
        assert!(result.is_err());
    }

    #[test]
    fn parse_extensions_skip_unknown() {
        // Extension type 99 (unknown), len=8, next=0
        let data = [0u8, 8, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = parse_extensions(99, &data).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, 99);
        assert_eq!(result[0].1.len(), 8);
    }

    // --- parse_selective_ack tests ---

    #[test]
    fn parse_selective_ack_valid() {
        let bitmask = vec![0b0000_0101, 0, 0, 0]; // ack_nr+2 and ack_nr+4
        let sack = parse_selective_ack(0, &bitmask).unwrap();
        assert!(sack.acknowledges(0)); // ack_nr+2
        assert!(!sack.acknowledges(1)); // ack_nr+3
        assert!(sack.acknowledges(2)); // ack_nr+4 (bit 2 = LSB+2)
    }
}
