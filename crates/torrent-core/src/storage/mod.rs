//! Torrent storage abstraction.
//!
//! This module provides the sync primitives for storage:
//! - [`Storage`] trait — abstraction for reading/writing pieces to disk
//! - [`StorageFactory`] trait — creates [`Storage`] instances from metainfo
//!
//! The async `FileStorage` implementation lives in the `torrent` crate.

use std::fmt::Debug;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Arc;

use crate::error::Error;
use crate::metainfo::Info;

/// A pinned, boxed, [`Send`]-safe future.
///
/// This alias keeps async trait method signatures readable while
/// preserving dyn-compatibility. It is re-exported by the `torrent`
/// crate. Most trait methods using this alias return
/// `Result<T, Error>`, but the alias itself is fully generic.
pub type IoFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Factory trait for creating [`Storage`] backends.
///
/// This allows users to inject custom storage implementations
/// (in-memory, remote, processing pipeline, etc.) without
/// modifying the library.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use torrent_core::storage::{IoFuture, Storage, StorageFactory};
/// use torrent_core::metainfo::Info;
/// use torrent_core::error::Error;
///
/// #[derive(Debug)]
/// struct MyFactory;
///
/// impl StorageFactory for MyFactory {
///     fn create<'a>(&'a self, _info: &'a Info) -> IoFuture<'a, Result<Arc<dyn Storage>, Error>> {
///         Box::pin(async move {
///             // Create a custom storage backend here
///             todo!()
///         })
///     }
///     // `prepare` is optional — inherits the default no-op
/// }
/// ```
#[allow(clippy::type_complexity)]
pub trait StorageFactory: Debug + Send + Sync {
    /// Create a new [`Storage`] backend for a torrent.
    ///
    /// `info` contains the torrent's file layout metadata.
    /// The returned [`Storage`] may not yet be ready for I/O —
    /// call [`Storage::prepare`] for resource allocation before
    /// starting the download loop.
    ///
    /// For magnet-link torrents (BEP 9), `info` is a stub with
    /// `piece_length = 0` and no pieces. Implementations should
    /// handle this gracefully, e.g. by deferring allocation until
    /// metadata arrives from peers.
    fn create<'a>(&'a self, info: &'a Info) -> IoFuture<'a, Result<Arc<dyn Storage>, Error>>;
}

/// Storage abstraction for torrent data.
///
/// This trait encapsulates piece-level and block-level read/write
/// operations without exposing filesystem details.
///
/// # Implementation Notes
///
/// Implementations must be `Send + Sync` so they can be shared
/// across tokio tasks via `Arc<dyn Storage>`. Methods return
/// `Pin<Box<dyn Future>>` instead of `impl Future` so the trait
/// remains dyn-compatible.
#[allow(clippy::type_complexity)]
pub trait Storage: Send + Sync {
    /// Read a block (partial piece) without reading the entire piece.
    ///
    /// Used for serving upload requests. Significantly reduces I/O
    /// compared to [`read_piece`](Storage::read_piece) when only a
    /// single 16 KB block is needed rather than the full piece
    /// (which can be 4 MB or more).
    fn read_block<'a>(
        &'a self, piece: u32, offset: u32, buf: &'a mut [u8],
    ) -> IoFuture<'a, Result<(), Error>>;

    /// Read an entire piece into `buf`.
    ///
    /// The buffer must be at least the piece length for all pieces except
    /// the last, which may be shorter (BEP 3 allows the final piece to be
    /// truncated). Callers can use [`Info::num_pieces`] and
    /// [`Info::total_size`] to compute the actual length of the last
    /// piece.
    fn read_piece<'a>(&'a self, index: u32, buf: &'a mut [u8]) -> IoFuture<'a, Result<(), Error>>;

    /// Write a block (a portion of a piece) to storage.
    ///
    /// Implements BEP 0003: The BitTorrent Protocol Specification.
    fn write_block<'a>(
        &'a self, piece: u32, offset: u32, data: &'a [u8],
    ) -> IoFuture<'a, Result<(), Error>>;

    /// Write an entire verified piece to storage in a single operation.
    ///
    /// Called after SHA-1 verification succeeds. Implementations may
    /// override this to use a single I/O operation per piece instead of
    /// writing block-by-block via [`write_block`](Storage::write_block),
    /// which can reduce write amplification by up to 256× for a 4 MB
    /// piece.
    ///
    /// The default implementation splits `data` into 16 KB blocks (BEP 3
    /// default block size) and delegates to [`write_block`](Storage::write_block).
    /// Custom backends that can write an entire piece at once should
    /// override this method.
    fn write_piece<'a>(&'a self, index: u32, data: &'a [u8]) -> IoFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let block_size = 16 * 1024; // BEP 3 default block size
            for (i, chunk) in data.chunks(block_size).enumerate() {
                self.write_block(index, (i * block_size) as u32, chunk)
                    .await?;
            }
            Ok(())
        })
    }

    /// Prepare storage for I/O. Called once before the download loop starts.
    ///
    /// Override for resource allocation: disk file creation, remote bucket
    /// provisioning, connection verification, etc. The default is a no-op.
    fn prepare(&self) -> IoFuture<'_, Result<(), Error>> {
        Box::pin(ready(Ok(())))
    }

    /// Total number of pieces.
    fn num_pieces(&self) -> usize;

    /// Total size in bytes.
    fn total_size(&self) -> u64;
}
