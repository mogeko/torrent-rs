//! Torrent data storage and piece management.
//!
//! This module handles all disk I/O and download scheduling decisions:
//! - [`Storage`] trait — abstraction for reading/writing pieces
//! - [`FileStorage`] — file-based implementation (single & multi-file)
//! - [`PieceManager`] — bitfield tracking, progress calculation
//! - [`PieceSelector`] trait + 4 strategies for picking which piece to download next
//!
//! # Selection Strategies
//!
//! - [`RarestFirst`] — picks the piece available from the fewest peers (BEP 3 recommended)
//! - [`RandomFirst`] — picks a random available piece
//! - [`Sequential`] — picks the lowest-indexed missing piece
//! - [`EndGame`] — picks any remaining piece (for duplicate requests in final phase)

mod file_backend;
mod piece_selector;

use std::future::Future;

use crate::error::Error;

/// Storage abstraction for torrent data.
///
/// This trait encapsulates piece-level read/write operations
/// without exposing filesystem details.
pub trait Storage: Send + Sync {
    /// Read an entire piece into `buf`. The buffer must be exactly the piece length.
    fn read_piece(
        &self,
        index: u32,
        buf: &mut [u8],
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Write a block (a portion of a piece) to storage.
    fn write_block(
        &self,
        piece: u32,
        offset: u32,
        data: &[u8],
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Check if a piece is fully downloaded and verified.
    fn has_piece(&self, index: u32) -> bool;

    /// Total number of pieces.
    fn num_pieces(&self) -> usize;

    /// Total size in bytes.
    fn total_size(&self) -> u64;
}

/// Tracks which pieces have been downloaded and manages the bitfield.
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
    pub fn set_piece(&mut self, index: u32) {
        let i = index as usize;
        if i < self.num_pieces {
            self.bitfield[i] = true;
        }
    }

    /// Check if a piece is completed.
    pub fn has_piece(&self, index: u32) -> bool {
        let i = index as usize;
        i < self.num_pieces && self.bitfield[i]
    }

    /// Return all completed piece indices.
    pub fn completed_pieces(&self) -> Vec<u32> {
        self.bitfield
            .iter()
            .enumerate()
            .filter(|&(_, have)| *have)
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// Return all missing piece indices.
    pub fn missing_pieces(&self) -> Vec<u32> {
        self.bitfield
            .iter()
            .enumerate()
            .filter(|&(_, have)| !*have)
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// Progress as a float 0.0..=1.0.
    pub fn progress(&self) -> f64 {
        if self.num_pieces == 0 {
            return 1.0;
        }
        let have = self.bitfield.iter().filter(|&&b| b).count();
        have as f64 / self.num_pieces as f64
    }

    /// Export bitfield as bytes (for Bitfield message).
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

pub use file_backend::FileStorage;
pub use piece_selector::*;
