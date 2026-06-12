//! Async file-based storage.
//!
//! Re-exports the [`Storage`] trait from `torrent_core::storage`,
//! piece management and selection types from `torrent_core::piece`,
//! and provides [`FileStorage`] which implements the trait using
//! tokio's async file I/O.
//!
//! # Key Types
//!
//! - [`Storage`] — re-exported from `torrent_core::storage`
//! - [`FileStorage`] — async file-based storage backend

mod file_backend;

pub use torrent_core::storage::Storage;

pub use self::file_backend::FileStorage;
