use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::{RwLock, mpsc};

use crate::error::{Error, ErrorKind};
use crate::magnet::hex_encode;
use crate::metainfo::{Metainfo, Mode};
use crate::peer::PeerId;
use crate::piece::{PieceManager, RarestFirst};
use crate::spec::TorrentSpec;
use crate::storage::Storage;
use crate::tracker::Tracker;

use super::download::{DownloadLoop, PeerEvent};
use super::peer_manager::PeerManager;
use super::upload::UploadManager;
use super::{SessionConfig, TorrentState, TorrentStatus};

/// Commands sent to the download loop.
pub(crate) enum TorrentCommand {
    Pause,
    Resume,
    Cancel,
}

/// Internal handle for a single torrent.
pub(crate) struct TorrentHandle {
    pub info_hash: [u8; 20],
    /// Full torrent metadata — `None` for magnet links until
    /// [`TorrentBuilder::resolve_metadata`] downloads it from peers (BEP 9/10).
    pub metainfo: Option<Metainfo>,
    pub peer_mgr: Arc<RwLock<PeerManager>>,
    pub piece_mgr: Arc<RwLock<PieceManager>>,
    pub status: Arc<RwLock<TorrentStatus>>,
    /// Set by [`activate`](TorrentHandle::activate).
    pub storage: Option<Arc<dyn Storage>>,
    /// Set by [`activate`](TorrentHandle::activate).
    pub control_tx: Option<mpsc::Sender<TorrentCommand>>,
    /// Set by [`activate`](TorrentHandle::activate).
    pub task: Option<tokio::task::JoinHandle<()>>,
}

impl TorrentHandle {
    /// Register a torrent without storage or download loop.
    /// State = [`TorrentState::Registered`].
    ///
    /// For magnet links, `metainfo` is `None` and `num_pieces` is 0 —
    /// metainfo must be downloaded from peers before activation.
    pub(crate) fn register(spec: TorrentSpec, config: &SessionConfig) -> Self {
        let (metainfo, info_hash, name, num_pieces) = match spec {
            TorrentSpec::Metainfo(meta) => {
                let ih = meta.info_hash();
                let num = meta.info.num_pieces();
                let name = match &meta.info.mode {
                    Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
                };
                (Some(meta), ih, name, num)
            }
            TorrentSpec::Magnet(uri) => {
                let ih = *uri.primary_info_hash();
                let name = uri.display_name.unwrap_or_else(|| hex_encode(ih));
                (None, ih, name, 0)
            }
        };

        let piece_mgr = Arc::new(RwLock::new(PieceManager::new(num_pieces)));
        let peer_id = PeerId::random();
        let peer_mgr = Arc::new(RwLock::new(PeerManager::new(
            info_hash,
            peer_id,
            config.max_connections,
            config.peer_connect_timeout,
            config.peer_max_retries,
            config.peer_cooldown,
        )));

        let status = Arc::new(RwLock::new(TorrentStatus {
            info_hash,
            name,
            progress: 0.0,
            download_rate: 0.0,
            upload_rate: 0.0,
            num_peers: 0,
            num_seeds: 0,
            state: TorrentState::Registered,
        }));

        TorrentHandle {
            info_hash,
            metainfo,
            peer_mgr,
            piece_mgr,
            status,
            storage: None,
            control_tx: None,
            task: None,
        }
    }

    /// Attach storage and spawn the download loop.
    /// Transitions from [`TorrentState::Registered`] to downloading.
    ///
    /// # Panics
    ///
    /// Panics if metainfo has not been resolved (i.e. `self.metainfo` is `None`).
    pub(crate) fn activate(&mut self, storage: Arc<dyn Storage>, config: &SessionConfig) {
        let metainfo = self.metainfo.as_ref();
        let metainfo = metainfo.expect("metainfo must be resolved before activate");
        let name = match &metainfo.info.mode {
            Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
        };
        tracing::info!(
            "torrent activated: {} ({} pieces)",
            name,
            metainfo.info.num_pieces()
        );

        let (control_tx, control_rx) = mpsc::channel::<TorrentCommand>(16);
        let (peer_msg_tx, peer_msg_rx) =
            mpsc::channel::<(SocketAddr, PeerEvent)>(config.peer_msg_buffer_size);

        let peer_id = PeerId::random();
        let tracker = Tracker::from_torrent_with_timeout(metainfo.clone(), config.tracker_timeout);
        let upload_mgr = Arc::new(RwLock::new(UploadManager::new(config.max_uploads)));

        let mut download_loop = DownloadLoop {
            info_hash: self.info_hash,
            metainfo: metainfo.clone(),
            storage: storage.clone(),
            piece_mgr: self.piece_mgr.clone(),
            peer_mgr: self.peer_mgr.clone(),
            status: self.status.clone(),
            control_rx,
            peer_id,
            listen_port: config.listen_port,
            announce_ip: config.announce_ip,
            announce_ipv6: config.announce_ipv6,
            request_timeout: config.request_timeout,
            max_concurrent_pieces: config.max_concurrent_pieces,
            piece_cache_size: config.piece_cache_size,
            endgame_threshold: config.endgame_threshold,
            choke_interval: config.choke_interval,
            snub_timeout: config.snub_timeout,
            corrupt_ban_threshold: config.corrupt_ban_threshold,
            announce_fallback_interval: config.announce_fallback_interval,
            pex_enabled: config.pex_enabled,
            pex_interval: config.pex_interval,
            tracker,
            next_announce: None,
            has_announced: false,
            announced_completed: false,
            peers: HashMap::new(),
            active_downloads: HashMap::new(),
            selector: Box::new(RarestFirst),
            peer_msg_rx,
            peer_msg_tx,
            upload_mgr,
            total_downloaded: 0,
            total_uploaded: 0,
            last_downloaded: 0,
            last_uploaded: 0,
            piece_cache: Vec::new(),
            recently_dropped: Vec::new(),
        };

        let task = tokio::spawn(async move { download_loop.run().await });

        self.storage = Some(storage);
        self.control_tx = Some(control_tx);
        self.task = Some(task);
    }

    /// Pause this torrent. No-op if not yet activated.
    #[allow(dead_code)]
    pub async fn pause(&self) -> Result<(), Error> {
        if let Some(tx) = &self.control_tx {
            tx.send(TorrentCommand::Pause)
                .await
                .map_err(|_| Error::new(ErrorKind::Protocol))
        } else {
            Ok(())
        }
    }

    /// Resume this torrent. No-op if not yet activated.
    #[allow(dead_code)]
    pub async fn resume(&self) -> Result<(), Error> {
        if let Some(tx) = &self.control_tx {
            tx.send(TorrentCommand::Resume)
                .await
                .map_err(|_| Error::new(ErrorKind::Protocol))
        } else {
            Ok(())
        }
    }

    /// Cancel this torrent (shuts down the download loop).
    pub async fn cancel(&mut self) {
        if let Some(tx) = &self.control_tx {
            let _ = tx.send(TorrentCommand::Cancel).await;
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
    /// Get the current status.
    #[allow(dead_code)]
    pub async fn status(&self) -> TorrentStatus {
        self.status.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn torrent_command_variants() {
        // Verify all enum variants are constructible
        let pause = TorrentCommand::Pause;
        let resume = TorrentCommand::Resume;
        let cancel = TorrentCommand::Cancel;
        match pause {
            TorrentCommand::Pause | TorrentCommand::Resume | TorrentCommand::Cancel => {}
        }
        let _ = (pause, resume, cancel);
    }
}
