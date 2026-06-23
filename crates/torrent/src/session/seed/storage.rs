//! [`DataSourceStorage`] — adapts a [`DataSource`] into a [`Storage`] backend.
//!
//! This is the bridge between the seeding API (which uses [`DataSource`])
//! and the download loop (which expects [`Storage`]). Piece-level read
//! requests are translated into byte-offset reads on the underlying source.

use std::fmt;

use crate::metainfo::Info;
use crate::storage::{BoxFuture, Storage};

use super::DataSource;

/// A [`Storage`] backend backed by a [`DataSource`].
///
/// Translates piece-index reads from the download loop into byte-offset
/// reads on the underlying data source. Used during seeding to serve
/// upload requests via the [`Storage`] trait.
///
/// `write_block` and `prepare` are no-ops — the data is read-only.
pub(crate) struct DataSourceStorage {
    source: Box<dyn DataSource>,
    piece_length: u64,
    num_pieces: usize,
    total_size: u64,
}

impl DataSourceStorage {
    /// Create a new adapter.
    ///
    /// `info` provides the piece layout used to translate piece indices
    /// into byte offsets. `source` must provide read access to the same
    /// data that was hashed to produce `info`.
    pub fn new(source: Box<dyn DataSource>, info: &Info) -> Self {
        Self {
            source,
            piece_length: info.piece_length,
            num_pieces: info.num_pieces(),
            total_size: info.total_size(),
        }
    }

    fn piece_offset(&self, index: u32) -> u64 {
        index as u64 * self.piece_length
    }

    fn piece_len_for_index(&self, index: u32) -> u64 {
        let idx = index as u64;
        if idx >= self.num_pieces as u64 {
            return 0;
        }
        let start = idx * self.piece_length;
        if idx == self.num_pieces as u64 - 1 {
            self.total_size - start
        } else {
            self.piece_length
        }
    }
}

impl fmt::Debug for DataSourceStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataSourceStorage")
            .field("source", &self.source)
            .field("piece_length", &self.piece_length)
            .finish()
    }
}

impl Storage for DataSourceStorage {
    fn read_piece<'a>(&'a self, index: u32, buf: &'a mut [u8]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let offset = self.piece_offset(index);
            let len = self.piece_len_for_index(index) as usize;
            let n = self.source.read_at(offset, &mut buf[..len]).await?;
            if n < len {
                // Fill remainder with zeros (partial last piece, or EOF)
                buf[n..len].fill(0);
            }
            Ok(())
        })
    }

    fn write_block<'a>(&'a self, _piece: u32, _offset: u32, _data: &'a [u8]) -> BoxFuture<'a, ()> {
        // Seeding is read-only — writes are no-ops
        Box::pin(async { Ok(()) })
    }

    fn read_block<'a>(&'a self, piece: u32, offset: u32, buf: &'a mut [u8]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let global_offset = self.piece_offset(piece) + offset as u64;
            let n = self.source.read_at(global_offset, buf).await?;
            if n < buf.len() {
                buf[n..].fill(0);
            }
            Ok(())
        })
    }

    fn num_pieces(&self) -> usize {
        self.num_pieces
    }

    fn total_size(&self) -> u64 {
        self.total_size
    }
}
