//! [`DataSource`] trait and built-in implementations.
//!
//! A data source provides raw byte access to the content that will
//! become a torrent. Implementations include local files ([`PathBuf`]),
//! in-memory buffers ([`Vec<u8>`]), and can be extended for remote
//! storage (S3, HTTP, etc.).

use std::fmt;
use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _, SeekFrom};

use crate::storage::BoxFuture;

/// A source of raw file data for torrent creation.
///
/// Implementations provide sequential byte access to the data that
/// will be hashed into pieces. The trait is object-safe and can be
/// used with custom backends (local files, S3 objects, memory
/// buffers, etc.).
///
/// # Built-in Implementations
///
/// - [`PathBuf`] — a single file on the local filesystem
/// - [`Vec<u8>`] — an in-memory buffer (useful for testing)
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
/// use torrent::session::seed::DataSource;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let source = PathBuf::from("./my_release/video.mp4");
/// println!("name: {}, size: {}", source.name(), source.total_size().await?);
///
/// let mut buf = vec![0u8; 16];
/// let n = source.read_at(0, &mut buf).await?;
/// assert_eq!(n, 16);
/// # Ok(())
/// # }
/// ```
pub trait DataSource: Send + Sync + fmt::Debug {
    /// A human-readable name for this data source.
    ///
    /// This becomes the default torrent name if not overridden in
    /// `SeedBuilder`.
    fn name(&self) -> &str;

    /// Total size of the data in bytes.
    ///
    /// For local files this queries filesystem metadata; for remote
    /// sources this may involve a network request (e.g. HTTP HEAD).
    fn total_size(&self) -> BoxFuture<'_, u64>;

    /// Read up to `buf.len()` bytes starting at `offset`.
    ///
    /// Returns the number of bytes read. Fewer bytes than `buf.len()`
    /// indicates EOF was reached. Returns an error on I/O failure.
    fn read_at<'a>(&'a self, offset: u64, buf: &'a mut [u8]) -> BoxFuture<'a, usize>;
}

// ── PathBuf implementation ──

impl DataSource for PathBuf {
    fn name(&self) -> &str {
        self.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("<unknown>")
    }

    fn total_size(&self) -> BoxFuture<'_, u64> {
        Box::pin(async move {
            let meta = fs::metadata(self).await?;
            Ok(meta.len())
        })
    }

    fn read_at<'a>(&'a self, offset: u64, buf: &'a mut [u8]) -> BoxFuture<'a, usize> {
        Box::pin(async move {
            let mut file = fs::File::open(self).await?;

            file.seek(SeekFrom::Start(offset)).await?;

            let n = file.read(buf).await?;

            Ok(n)
        })
    }
}

// ── Vec<u8> implementation (in-memory, for testing) ──

impl DataSource for Vec<u8> {
    fn name(&self) -> &str {
        "memory"
    }

    fn total_size(&self) -> BoxFuture<'_, u64> {
        Box::pin(async move { Ok(self.len() as u64) })
    }

    fn read_at<'a>(&'a self, offset: u64, buf: &'a mut [u8]) -> BoxFuture<'a, usize> {
        Box::pin(async move {
            let start = offset as usize;
            if start >= self.len() {
                return Ok(0);
            }
            let end = (start + buf.len()).min(self.len());
            let n = end - start;
            buf[..n].copy_from_slice(&self[start..end]);
            Ok(n)
        })
    }
}

// ── Also accept &Path for convenience ──

impl DataSource for &Path {
    fn name(&self) -> &str {
        self.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("<unknown>")
    }

    fn total_size(&self) -> BoxFuture<'_, u64> {
        let path = self.to_path_buf();
        Box::pin(async move { path.total_size().await })
    }

    fn read_at<'a>(&'a self, offset: u64, buf: &'a mut [u8]) -> BoxFuture<'a, usize> {
        let path = self.to_path_buf();
        Box::pin(async move { path.read_at(offset, buf).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_source_read_full() {
        let data = b"hello world".to_vec();
        let mut buf = vec![0u8; 11];
        let n = data.read_at(0, &mut buf).await.unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf, b"hello world");
    }

    #[tokio::test]
    async fn memory_source_read_partial() {
        let data = b"hello".to_vec();
        let mut buf = vec![0u8; 10]; // bigger than source
        let n = data.read_at(0, &mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..5], b"hello");
    }

    #[tokio::test]
    async fn memory_source_read_at_offset() {
        let data = b"abcdefghij".to_vec();
        let mut buf = vec![0u8; 4];
        let n = data.read_at(3, &mut buf).await.unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"defg");
    }

    #[tokio::test]
    async fn memory_source_read_beyond_eof() {
        let data = b"abc".to_vec();
        let mut buf = vec![0u8; 5];
        let n = data.read_at(2, &mut buf).await.unwrap();
        assert_eq!(n, 1);
        assert_eq!(buf[0], b'c');
    }

    #[tokio::test]
    async fn memory_source_read_past_end() {
        let data = b"abc".to_vec();
        let mut buf = vec![0u8; 3];
        let n = data.read_at(5, &mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn memory_source_total_size() {
        let data = vec![0u8; 42];
        assert_eq!(data.total_size().await.unwrap(), 42);
    }

    #[tokio::test]
    async fn memory_source_name() {
        let data = vec![0u8; 10];
        assert_eq!(data.name(), "memory");
    }
}
