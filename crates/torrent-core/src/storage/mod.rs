//! Torrent storage abstraction.
//!
//! This module provides the sync primitives for storage:
//! - [`Storage`] trait — abstraction for reading/writing pieces to disk
//!
//! The async `FileStorage` implementation lives in the `torrent` crate.

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

    /// Total number of pieces.
    fn num_pieces(&self) -> usize;

    /// Total size in bytes.
    fn total_size(&self) -> u64;
}
