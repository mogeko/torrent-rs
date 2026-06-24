use std::fs::{File, OpenOptions};
use std::io::{Error as IoError, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::fs;

use crate::error::Error;
use crate::metainfo::{Info, Mode};

use super::{BoxFuture, Storage, StorageFactory};

/// [`StorageFactory`] that creates file-backed storage.
///
/// Construct with [`FileStorageFactory::new`], passing the download
/// directory. Use with [`DownloadBuilder::download_dir`].
///
/// [`DownloadBuilder::download_dir`]: crate::session::DownloadBuilder::download_dir
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use torrent::storage::{FileStorageFactory, StorageFactory};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let factory = FileStorageFactory::new("./downloads");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct FileStorageFactory {
    download_dir: PathBuf,
}

impl FileStorageFactory {
    /// Create a factory that stores files under `download_dir`.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            download_dir: dir.into(),
        }
    }
}

impl StorageFactory for FileStorageFactory {
    fn create<'a>(&'a self, info: &'a Info) -> BoxFuture<'a, Arc<dyn Storage>> {
        let dir = self.download_dir.clone();
        Box::pin(async move {
            let fs = FileStorage::new(info, &dir);
            Ok(Arc::new(fs) as Arc<dyn Storage>)
        })
    }
}

/// File-based storage backend.
pub struct FileStorage {
    /// Number of pieces.
    num_pieces: usize,
    /// Size of each piece in bytes.
    piece_length: u64,
    /// Total file size.
    total_size: u64,
    /// File layout mode (single or multi-file).
    mode: StorageMode,
}

enum StorageMode {
    SingleFile { path: PathBuf },
    MultiFile { files: Vec<StorageFile> },
}

struct StorageFile {
    path: PathBuf,
    length: u64,
}

impl FileStorage {
    /// Build a [`FileStorage`] layout from metainfo. No disk I/O.
    ///
    /// Call [`Storage::prepare`] afterwards to create directories
    /// and pre-allocate files on disk.
    pub fn new(info: &Info, download_dir: &Path) -> Self {
        let root = download_dir.to_path_buf();

        let num_pieces = info.num_pieces();
        let piece_length = info.piece_length;
        let total_size = info.total_size();

        let mode = match &info.mode {
            Mode::Single { name, length: _ } => {
                let path = root.join(name);
                StorageMode::SingleFile { path }
            }
            Mode::Multiple { name, files } => {
                let dir = root.join(name);
                let mut storage_files = Vec::with_capacity(files.len());
                for file_info in files {
                    let mut file_path = dir.clone();
                    for component in &file_info.path {
                        file_path.push(component);
                    }
                    storage_files.push(StorageFile {
                        path: file_path,
                        length: file_info.length,
                    });
                }
                StorageMode::MultiFile {
                    files: storage_files,
                }
            }
        };

        tracing::info!(
            "storage layout computed: {} pieces, {} total bytes",
            num_pieces,
            total_size
        );

        FileStorage {
            num_pieces,
            piece_length,
            total_size,
            mode,
        }
    }

    /// Map a piece to byte range [offset, offset+piece_len).
    fn piece_offset(&self, index: u32) -> u64 {
        index as u64 * self.piece_length
    }

    /// Length of the last piece may be shorter.
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

    /// Read a byte range from the file(s).
    ///
    /// Uses `tokio::task::spawn_blocking` to keep blocking I/O off the
    /// tokio worker threads. Opens the file, seeks to the offset, then
    /// reads the data. Data is first read into a heap-allocated buffer
    /// inside the blocking pool, then copied into the caller's `buf`.
    async fn read_range(&self, offset: u64, len: usize, buf: &mut [u8]) -> Result<(), Error> {
        match &self.mode {
            StorageMode::SingleFile { path } => {
                let path = path.clone();
                let data = tokio::task::spawn_blocking(move || {
                    let mut f = File::open(&path)?;
                    let mut local = vec![0u8; len];

                    f.seek(SeekFrom::Start(offset))?;
                    f.read_exact(&mut local)?;

                    Ok::<Vec<u8>, IoError>(local)
                })
                .await
                .map_err(|e| Error::io(format!("storage read task panicked: {}", e)))??;
                buf[..data.len()].copy_from_slice(&data);
                Ok(())
            }
            StorageMode::MultiFile { files } => {
                let ranges = map_byte_range(offset, len as u64, files);
                let mut buf_offset = 0;
                for (path, file_offset, read_len) in ranges {
                    let end = std::cmp::min(buf_offset + read_len as usize, buf.len());
                    let actual_len = end - buf_offset;
                    if actual_len == 0 {
                        break;
                    }
                    let path = path.clone();
                    let data = tokio::task::spawn_blocking(move || {
                        let mut f = File::open(&path)?;
                        let mut local = vec![0u8; actual_len];

                        f.seek(SeekFrom::Start(file_offset))?;
                        f.read_exact(&mut local)?;

                        Ok::<Vec<u8>, IoError>(local)
                    })
                    .await
                    .map_err(|e| Error::io(format!("storage read task panicked: {}", e)))??;
                    buf[buf_offset..end].copy_from_slice(&data);
                    buf_offset += read_len as usize;
                }
                Ok(())
            }
        }
    }

    /// Write a byte range to the file(s).
    ///
    /// Uses `tokio::task::spawn_blocking` to keep blocking I/O off the
    /// tokio worker threads. Opens the file, seeks to the offset, then
    /// writes the data. Data is first copied into a heap-allocated buffer,
    /// then written inside the blocking pool.
    async fn write_range(&self, offset: u64, data: &[u8]) -> Result<(), Error> {
        match &self.mode {
            StorageMode::SingleFile { path } => {
                let path = path.clone();
                let data = data.to_vec();
                tokio::task::spawn_blocking(move || {
                    let mut f = OpenOptions::new().write(true).open(&path)?;

                    f.seek(SeekFrom::Start(offset))?;
                    f.write_all(&data)?;

                    Ok::<(), IoError>(())
                })
                .await
                .map_err(|e| Error::io(format!("storage write task panicked: {}", e)))??;
                Ok(())
            }
            StorageMode::MultiFile { files } => {
                let ranges = map_byte_range(offset, data.len() as u64, files);
                let mut data_offset = 0;
                for (path, file_offset, write_len) in ranges {
                    let end = std::cmp::min(data_offset + write_len as usize, data.len());
                    if data_offset >= end {
                        break;
                    }
                    let path = path.clone();
                    let chunk = data[data_offset..end].to_vec();
                    tokio::task::spawn_blocking(move || {
                        let mut f = OpenOptions::new().write(true).open(&path)?;

                        f.seek(SeekFrom::Start(file_offset))?;
                        f.write_all(&chunk)?;

                        Ok::<(), IoError>(())
                    })
                    .await
                    .map_err(|e| Error::io(format!("storage write task panicked: {}", e)))??;
                    data_offset += write_len as usize;
                }
                Ok(())
            }
        }
    }
}

impl Storage for FileStorage {
    fn read_block<'a>(&'a self, piece: u32, offset: u32, buf: &'a mut [u8]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            tracing::trace!(
                "reading block: piece {} offset {} ({} bytes)",
                piece,
                offset,
                buf.len()
            );
            let global_offset = self.piece_offset(piece) + offset as u64;
            self.read_range(global_offset, buf.len(), buf).await
        })
    }

    fn read_piece<'a>(&'a self, index: u32, buf: &'a mut [u8]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            tracing::trace!("reading piece {}", index);
            let offset = self.piece_offset(index);
            let read_len = self.piece_len_for_index(index);
            self.read_range(offset, read_len as usize, buf).await
        })
    }

    fn write_block<'a>(&'a self, piece: u32, offset: u32, data: &'a [u8]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            tracing::trace!(
                "writing block: piece {} offset {} ({} bytes)",
                piece,
                offset,
                data.len()
            );
            let global_offset = self.piece_offset(piece) + offset as u64;
            self.write_range(global_offset, data).await
        })
    }

    fn write_piece<'a>(&'a self, index: u32, data: &'a [u8]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            tracing::trace!("writing piece {} ({} bytes)", index, data.len());
            self.write_range(self.piece_offset(index), data).await
        })
    }

    fn prepare(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            match &self.mode {
                StorageMode::SingleFile { path } => {
                    if let Some(parent) = path.parent() {
                        fs::create_dir_all(parent).await?;
                    }
                    let f = fs::File::create_new(path).await?;
                    f.set_len(self.total_size).await?;
                }
                StorageMode::MultiFile { files } => {
                    for file in files {
                        if let Some(parent) = file.path.parent() {
                            fs::create_dir_all(parent).await?;
                        }
                        let f = fs::File::create_new(&file.path).await?;
                        f.set_len(file.length).await?;
                    }
                }
            }
            tracing::info!(
                "storage prepared: {} pieces, {} total bytes",
                self.num_pieces,
                self.total_size
            );
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

/// Map a byte range [offset, offset+length) to file paths and positions.
fn map_byte_range(offset: u64, length: u64, files: &[StorageFile]) -> Vec<(PathBuf, u64, u64)> {
    let end = offset + length;
    let mut current_offset = 0u64;
    let mut result = Vec::new();

    for file in files {
        let file_start = current_offset;
        let file_end = current_offset + file.length;

        if file_end > offset && file_start < end {
            let read_start = std::cmp::max(file_start, offset) - file_start;
            let read_end = std::cmp::min(file_end, end) - file_start;
            result.push((file.path.clone(), read_start, read_end - read_start));
        }
        current_offset = file_end;
        if current_offset >= end {
            break;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::{Bytes, FileInfo, RawInfo};

    #[test]
    fn test_map_byte_range_single_file() {
        let files = vec![StorageFile {
            path: PathBuf::from("a.txt"),
            length: 100,
        }];
        let ranges = map_byte_range(0, 50, &files);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].1, 0); // file offset
        assert_eq!(ranges[0].2, 50); // length
    }

    #[test]
    fn test_map_byte_range_across_files() {
        let files = vec![
            StorageFile {
                path: PathBuf::from("a.txt"),
                length: 100,
            },
            StorageFile {
                path: PathBuf::from("b.txt"),
                length: 200,
            },
        ];
        // Read from byte 80 to byte 120 (spanning both files)
        let ranges = map_byte_range(80, 40, &files);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].0, PathBuf::from("a.txt"));
        assert_eq!(ranges[0].1, 80);
        assert_eq!(ranges[0].2, 20); // 80..100
        assert_eq!(ranges[1].0, PathBuf::from("b.txt"));
        assert_eq!(ranges[1].1, 0);
        assert_eq!(ranges[1].2, 20); // 0..20 in second file
    }

    #[tokio::test]
    async fn test_file_storage_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let info = Info {
            piece_length: 32,
            pieces: vec![[0u8; 20]; 2],
            mode: Mode::Single {
                name: "test.bin".into(),
                length: 64,
            },
            raw_info: RawInfo::Bytes(Bytes::new()),
        };
        let storage = FileStorage::new(&info, dir.path());
        // Prepare storage (pre-allocate files on disk)
        storage.prepare().await.unwrap();

        // Write a block
        let data = vec![0x42u8; 16];
        storage.write_block(0, 0, &data).await.unwrap();

        // Read the piece back
        let mut buf = vec![0u8; 32];
        storage.read_piece(0, &mut buf).await.unwrap();
        assert_eq!(&buf[..16], &data[..]);
    }

    #[tokio::test]
    async fn test_file_storage_multi_file() {
        let dir = tempfile::tempdir().unwrap();
        let info = Info {
            piece_length: 64,
            pieces: vec![[0u8; 20]; 1],
            mode: Mode::Multiple {
                name: "multi".into(),
                files: vec![
                    FileInfo {
                        length: 32,
                        path: vec!["a.bin".into()],
                    },
                    FileInfo {
                        length: 32,
                        path: vec!["b.bin".into()],
                    },
                ],
            },
            raw_info: RawInfo::Bytes(Bytes::new()),
        };
        let storage = FileStorage::new(&info, dir.path());
        storage.prepare().await.unwrap();

        let data = vec![0xFFu8; 64];
        storage.write_block(0, 0, &data).await.unwrap();

        let mut buf = vec![0u8; 64];
        storage.read_piece(0, &mut buf).await.unwrap();
        assert_eq!(buf, data);
    }
}
