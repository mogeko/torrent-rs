//! Torrent builder — configures and activates a registered torrent.
//!
//! Created by [`Session::add_torrent`] (or its convenience wrappers).
//! The torrent is registered immediately; call [`start`](TorrentBuilder::start)
//! to create storage and begin downloading.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::{Error, ErrorKind};
use crate::storage::{FileStorageFactory, StorageFactory};

use super::{InfoHash, Session};

/// Builder for configuring and activating a torrent.
///
/// Holds a reference to the [`Session`] — cannot outlive it.
pub struct TorrentBuilder<'s> {
    session: &'s Session,
    pub(crate) info_hash: InfoHash,
    storage_factory: Option<Arc<dyn StorageFactory>>,
    metadata_resolved: bool,
    /// Peers extracted from magnet URI x.pe (BEP 9). Injected in [`start`](Self::start).
    magnet_peers: Vec<SocketAddr>,
}

impl<'s> TorrentBuilder<'s> {
    /// Create a new builder. Called by [`Session::add_torrent`].
    pub(crate) fn new(
        session: &'s Session, info_hash: InfoHash, metadata_resolved: bool,
        magnet_peers: Vec<SocketAddr>,
    ) -> Self {
        TorrentBuilder {
            session,
            info_hash,
            storage_factory: None,
            metadata_resolved,
            magnet_peers,
        }
    }

    /// The 20-byte info hash of this torrent.
    pub fn info_hash(&self) -> InfoHash {
        self.info_hash
    }

    // ── Metadata resolution ──

    /// Ensure full metadata is available.
    ///
    /// For [`Metainfo`](crate::metainfo::Metainfo) torrents this is a no-op.
    /// For magnet links (BEP 9), downloads metadata from peers via
    /// the LTEP extension protocol (BEP 10).
    ///
    /// Idempotent: safe to call multiple times.
    pub async fn resolve_metadata(mut self) -> Result<Self, Error> {
        if self.metadata_resolved {
            return Ok(self);
        }

        let needs_resolve = {
            let torrents = self.session.torrents().read().unwrap();
            let Some(handle) = torrents.get(&self.info_hash) else {
                return Err(Error::new(ErrorKind::InvalidInput));
            };
            // Metainfo torrents have non-zero piece_length and non-empty pieces
            handle.metainfo.info.piece_length == 0
        };

        if needs_resolve {
            // TODO: BEP 9/10 — download metainfo from peers via LTEP handshake.
            // Steps:
            // 1. Connect to peers (use handle.peer_mgr)
            // 2. Send LTEP handshake with ut_metadata extension
            // 3. Request metadata pieces
            // 4. Reconstruct full metainfo
            // 5. Update handle.metainfo and handle.piece_mgr
        }

        self.metadata_resolved = true;
        Ok(self)
    }

    // ── Storage configuration ──

    /// Bind a download directory. Internally creates
    /// [`FileStorageFactory::new(dir)`](FileStorageFactory::new).
    pub fn download_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.storage_factory = Some(Arc::new(FileStorageFactory::new(dir)));
        self
    }

    /// Inject a custom storage factory. Overrides any previous
    /// [`download_dir`](Self::download_dir) or [`storage`](Self::storage) call.
    pub fn storage(mut self, factory: Arc<dyn StorageFactory>) -> Self {
        self.storage_factory = Some(factory);
        self
    }

    // ── Activation ──

    /// Create storage and start the download/upload loop.
    pub async fn start(mut self) -> Result<InfoHash, Error> {
        // 0. Auto-resolve metadata if not already done
        if !self.metadata_resolved {
            self = self.resolve_metadata().await?;
        }

        // 0b. Inject magnet x.pe addresses into peer_mgr
        if !self.magnet_peers.is_empty() {
            let peer_mgr = {
                let torrents = self.session.torrents().read().unwrap();
                torrents.get(&self.info_hash).map(|h| h.peer_mgr.clone())
            };
            if let Some(peer_mgr) = peer_mgr {
                peer_mgr
                    .write()
                    .await
                    .add_peers(std::mem::take(&mut self.magnet_peers));
            }
        }

        // 1. Check active capacity (only counts torrents with running download loop)
        {
            let torrents = self.session.torrents().read().unwrap();
            let active_count = torrents.values().filter(|h| h.task.is_some()).count();
            let limit = self.session.config().max_active_torrents;
            if limit > 0 && active_count >= limit {
                return Err(Error::new(ErrorKind::InvalidInput));
            }
        }

        // 2. Resolve factory
        let factory = match &self.storage_factory {
            Some(f) => f.clone(),
            None => return Ok(self.info_hash), // Stay Registered
        };

        // 3. Get Info from registered handle
        let info = {
            let torrents = self.session.torrents().read().unwrap();

            match torrents.get(&self.info_hash) {
                Some(handle) => handle.metainfo.info.clone(),
                None => {
                    return Err(Error::new(ErrorKind::InvalidInput));
                }
            }
        };

        // 4. Create storage
        let storage = factory.create(&info).await?;

        // 5. Prepare (factory-defined resource allocation)
        storage.prepare().await?;

        // 6. Activate download loop
        {
            let mut torrents = self.session.torrents().write().unwrap();

            match torrents.get_mut(&self.info_hash) {
                Some(handle) => handle.activate(storage, self.session.config()),
                None => {
                    return Err(Error::new(ErrorKind::InvalidInput));
                }
            }
        }

        Ok(self.info_hash)
    }
}
