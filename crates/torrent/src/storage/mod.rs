//! Async file-based storage.
//!
//! Re-exports the [`Storage`] trait and [`StorageFactory`] trait from
//! `torrent_core::storage`, and provides [`FileStorage`] which implements
//! the trait using tokio's async file I/O, along with [`FileStorageFactory`]
//! which is the default factory for creating file-backed storage.
//!
//! # Key Types
//!
//! - [`Storage`] — re-exported from `torrent_core::storage`
//! - [`StorageFactory`] — re-exported from `torrent_core::storage`
//! - [`FileStorage`] — async file-based storage backend
//! - [`FileStorageFactory`] — default factory for [`FileStorage`]

mod file_backend;

pub use torrent_core::storage::{Storage, StorageFactory};

pub use self::file_backend::{FileStorage, FileStorageFactory};
