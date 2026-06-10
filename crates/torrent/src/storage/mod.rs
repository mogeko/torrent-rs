//! Async file-based storage.
//!
//! Re-exports the [`Storage`] trait and piece management types from
//! `torrent_core::storage`, and provides [`FileStorage`] which
//! implements the trait using tokio's async file I/O.
//!
//! # Key Types
//!
//! - [`Storage`], [`PieceManager`], [`PieceSelector`], etc. — re-exported from `torrent_core`
//! - [`FileStorage`] — async file-based storage backend

pub use torrent_core::storage::{
    EndGame, PieceManager, PieceSelector, RandomFirst, RarestFirst, Sequential, Storage,
};

mod file_backend;

pub use file_backend::FileStorage;
