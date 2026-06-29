/// Tracks which pieces have been downloaded and manages the bitfield.
///
/// Implements BEP 3: The BitTorrent Protocol Specification.
///
/// The [`PieceManager`] maintains a bitfield indicating which pieces
/// have been successfully downloaded and verified. It is used by the
/// download loop to track progress and decide which pieces to request next.
///
/// # Examples
///
/// ```
/// use torrent_core::piece::PieceManager;
///
/// let mut pm = PieceManager::new(10);
/// pm.set_piece(0);
/// pm.set_piece(5);
/// assert_eq!(pm.progress(), 0.2);
/// assert_eq!(pm.completed_pieces(), vec![0, 5]);
/// ```
pub struct PieceManager {
    pub num_pieces: usize,
    /// Bitfield: true = have the piece, false = missing.
    bitfield: Vec<bool>,
}

impl PieceManager {
    /// Create a new PieceManager with all pieces marked as missing.
    pub fn new(num_pieces: usize) -> Self {
        PieceManager {
            num_pieces,
            bitfield: vec![false; num_pieces],
        }
    }

    /// Mark a piece as completed.
    ///
    /// Does nothing if the index is out of range.
    pub fn set_piece(&mut self, index: u32) {
        let i = index as usize;
        if i < self.num_pieces {
            self.bitfield[i] = true;
            tracing::debug!(
                "piece {} completed ({}/{}, {:.1}%)",
                index,
                self.completed_pieces().len(),
                self.num_pieces,
                self.progress() * 100.0
            );
        }
    }

    /// Check if a piece is completed.
    ///
    /// Returns `false` if the index is out of range.
    pub fn has_piece(&self, index: u32) -> bool {
        let i = index as usize;
        i < self.num_pieces && self.bitfield[i]
    }

    /// Return a reference to the raw bitfield (for piece selection).
    pub fn bitfield(&self) -> &[bool] {
        &self.bitfield
    }

    /// Return all completed piece indices, sorted ascending.
    pub fn completed_pieces(&self) -> Vec<u32> {
        self.bitfield
            .iter()
            .enumerate()
            .filter(|&(_, have)| *have)
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// Return all missing piece indices, sorted ascending.
    pub fn missing_pieces(&self) -> Vec<u32> {
        self.bitfield
            .iter()
            .enumerate()
            .filter(|&(_, have)| !*have)
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// Progress as a float 0.0..=1.0.
    ///
    /// Returns 1.0 if there are no pieces.
    pub fn progress(&self) -> f64 {
        if self.num_pieces == 0 {
            return 1.0;
        }
        let have = self.bitfield.iter().filter(|&&b| b).count();
        have as f64 / self.num_pieces as f64
    }

    /// Export bitfield as bytes (for Bitfield message).
    ///
    /// Each bit represents one piece: 1 = have, 0 = missing.
    /// Bits are packed MSB-first per byte.
    pub fn to_bitfield(&self) -> Vec<u8> {
        let byte_count = self.num_pieces.div_ceil(8);
        let mut bytes = vec![0u8; byte_count];
        for (i, &have) in self.bitfield.iter().enumerate() {
            if have {
                let byte = i / 8;
                let bit = 7 - (i % 8);
                bytes[byte] |= 1 << bit;
            }
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_empty_manager() {
        let pm = PieceManager::new(5);
        assert_eq!(pm.num_pieces, 5);
        assert_eq!(pm.missing_pieces().len(), 5);
        assert!(pm.completed_pieces().is_empty());
    }

    #[test]
    fn new_zero_pieces() {
        let pm = PieceManager::new(0);
        assert_eq!(pm.num_pieces, 0);
        assert_eq!(pm.progress(), 1.0);
    }

    #[test]
    fn set_piece_and_check() {
        let mut pm = PieceManager::new(10);
        pm.set_piece(0);
        pm.set_piece(3);
        assert!(pm.has_piece(0));
        assert!(pm.has_piece(3));
        assert!(!pm.has_piece(1));
        assert!(!pm.has_piece(9));
    }

    #[test]
    fn set_piece_out_of_range() {
        let mut pm = PieceManager::new(5);
        pm.set_piece(100); // should not panic
        assert!(pm.missing_pieces().len() == 5);
    }

    #[test]
    fn completed_and_missing_pieces() {
        let mut pm = PieceManager::new(5);
        pm.set_piece(0);
        pm.set_piece(2);
        pm.set_piece(4);
        let completed = pm.completed_pieces();
        assert_eq!(completed.len(), 3);
        assert!(completed.contains(&0));
        assert!(completed.contains(&2));
        assert!(completed.contains(&4));
        let missing = pm.missing_pieces();
        assert_eq!(missing.len(), 2);
        assert!(missing.contains(&1));
        assert!(missing.contains(&3));
    }

    #[test]
    fn progress_calculation() {
        let mut pm = PieceManager::new(10);
        for i in 0..5 {
            pm.set_piece(i);
        }
        assert!((pm.progress() - 0.5).abs() < 1e-10);
    }

    #[test]
    fn to_bitfield_bytes() {
        let mut pm = PieceManager::new(16);
        pm.set_piece(0); // 10000000 00000000
        let bf = pm.to_bitfield();
        assert_eq!(bf.len(), 2);
        assert_eq!(bf[0], 0x80);
        assert_eq!(bf[1], 0x00);

        pm.set_piece(7); // 10000001 00000000
        let bf = pm.to_bitfield();
        assert_eq!(bf[0], 0x81);
    }

    #[test]
    fn bitfield_reflects_set_piece() {
        let mut pm = PieceManager::new(3);
        pm.set_piece(1);
        let bf = pm.bitfield();
        assert_eq!(bf.len(), 3);
        assert!(!bf[0]);
        assert!(bf[1]);
        assert!(!bf[2]);
    }

    #[test]
    fn progress_zero_completed() {
        let pm = PieceManager::new(10);
        assert!((pm.progress() - 0.0).abs() < 1e-10);
    }

    #[test]
    fn progress_all_completed() {
        let mut pm = PieceManager::new(5);
        for i in 0..5 {
            pm.set_piece(i);
        }
        assert!((pm.progress() - 1.0).abs() < 1e-10);
    }
}
