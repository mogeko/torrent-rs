mod download;
mod peer_manager;
mod torrent;
mod upload;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::error::Error;
use crate::metainfo::{Metainfo, from_bytes as parse_metainfo};
use crate::peer::PeerId;
use crate::storage::FileStorage;

/// Unique identifier for a torrent (SHA-1 info hash).
pub type InfoHash = [u8; 20];

/// High-level session managing all torrent downloads/uploads.
pub struct Session {
    /// Our peer ID.
    #[allow(dead_code)]
    peer_id: PeerId,
    /// Session configuration.
    config: SessionConfig,
    /// Active torrents, keyed by info_hash.
    torrents: RwLock<HashMap<InfoHash, torrent::TorrentHandle>>,
}

/// Session configuration.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// TCP listen port for incoming peer connections.
    pub listen_port: u16,
    /// Maximum number of peer connections per torrent.
    pub max_connections: u32,
    /// Maximum upload slots.
    pub max_uploads: u32,
    /// Download directory.
    pub download_dir: PathBuf,
    /// Enable DHT.
    pub enable_dht: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        SessionConfig {
            listen_port: 6881,
            max_connections: 50,
            max_uploads: 8,
            download_dir: PathBuf::from("downloads"),
            enable_dht: true,
        }
    }
}

/// Status of a torrent, exposed via the public API.
#[derive(Debug, Clone)]
pub struct TorrentStatus {
    /// The 20-byte info hash.
    pub info_hash: InfoHash,
    /// Display name of the torrent.
    pub name: String,
    /// Download progress (0.0 to 1.0).
    pub progress: f64,
    /// Download rate in bytes per second.
    pub download_rate: f64,
    /// Upload rate in bytes per second.
    pub upload_rate: f64,
    /// Number of connected peers.
    pub num_peers: usize,
    /// Number of seeders (peers with 100% completion).
    pub num_seeds: usize,
    /// Current state of the torrent.
    pub state: TorrentState,
}

/// Possible states of a torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentState {
    /// Waiting to start.
    Queued,
    /// Actively downloading.
    Downloading,
    /// All pieces downloaded, uploading only.
    Seeding,
    /// Paused by user.
    Paused,
    /// An error occurred.
    Error,
}

impl Session {
    /// Create a new session with the given configuration.
    pub async fn new(config: SessionConfig) -> Result<Self, Error> {
        Ok(Session {
            peer_id: PeerId::random(),
            config,
            torrents: RwLock::new(HashMap::new()),
        })
    }

    /// Add a torrent by its `Metainfo`.
    pub async fn add_torrent(&self, meta: Metainfo) -> Result<InfoHash, Error> {
        let info_hash = meta.info_hash();
        let _name = match &meta.info.mode {
            crate::metainfo::Mode::Single { name, .. }
            | crate::metainfo::Mode::Multiple { name, .. } => name.clone(),
        };

        // Create FileStorage
        let storage = Arc::new(FileStorage::new(&meta.info, &self.config.download_dir).await?);

        let handle = torrent::TorrentHandle::new(meta, info_hash, storage, &self.config);
        self.torrents.write().await.insert(info_hash, handle);

        Ok(info_hash)
    }

    /// Add a torrent from raw bencoded bytes (the contents of a .torrent file).
    pub async fn add_torrent_bytes(&self, data: &[u8]) -> Result<InfoHash, Error> {
        let meta = parse_metainfo(data)?;
        self.add_torrent(meta).await
    }

    /// Remove a torrent by info_hash.
    pub async fn remove_torrent(&self, info_hash: &InfoHash) -> Result<(), Error> {
        let handle = self.torrents.write().await.remove(info_hash);
        if let Some(mut h) = handle {
            h.cancel().await;
        }
        Ok(())
    }

    /// Get the status of a torrent.
    pub async fn torrent_status(&self, info_hash: &InfoHash) -> Result<TorrentStatus, Error> {
        let torrents = self.torrents.read().await;
        let handle = torrents
            .get(info_hash)
            .ok_or(Error::new(crate::error::ErrorKind::InvalidInput))?;
        Ok(handle.status().await)
    }

    /// List all active info_hashes.
    pub async fn active_torrents(&self) -> Vec<InfoHash> {
        self.torrents.read().await.keys().copied().collect()
    }
}
