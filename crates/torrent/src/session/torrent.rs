use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{RwLock, mpsc};

use crate::error::{Error, ErrorKind};
use crate::metainfo::{Metainfo, Mode};
use crate::peer::PeerId;
use crate::piece::{PieceManager, RarestFirst};
use crate::storage::FileStorage;

use super::download::DownloadLoop;
use super::peer_manager::PeerManager;
use super::{SessionConfig, TorrentState, TorrentStatus};

/// Commands sent to the download loop.
#[allow(dead_code)]
pub(crate) enum TorrentCommand {
    Pause,
    Resume,
    Cancel,
}

/// Internal handle for a single torrent.
#[allow(dead_code)]
pub(crate) struct TorrentHandle {
    pub info_hash: [u8; 20],
    pub metainfo: Metainfo,
    pub storage: Arc<FileStorage>,
    pub peer_mgr: Arc<RwLock<PeerManager>>,
    pub piece_mgr: Arc<RwLock<PieceManager>>,
    pub status: Arc<RwLock<TorrentStatus>>,
    pub control_tx: mpsc::Sender<TorrentCommand>,
    /// Download task join handle.
    pub task: tokio::task::JoinHandle<()>,
}

#[allow(dead_code)]
impl TorrentHandle {
    /// Create a new TorrentHandle and spawn its download loop.
    pub fn new(
        metainfo: Metainfo,
        info_hash: [u8; 20],
        storage: Arc<FileStorage>,
        config: &SessionConfig,
    ) -> Self {
        let num_pieces = metainfo.info.num_pieces();
        let name = match &metainfo.info.mode {
            Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
        };

        let piece_mgr = Arc::new(RwLock::new(PieceManager::new(num_pieces)));
        let peer_mgr = Arc::new(RwLock::new(PeerManager::new(
            info_hash,
            PeerId::random(),
            config.max_connections,
        )));

        let status = Arc::new(RwLock::new(TorrentStatus {
            info_hash,
            name: name.clone(),
            progress: 0.0,
            download_rate: 0.0,
            upload_rate: 0.0,
            num_peers: 0,
            num_seeds: 0,
            state: TorrentState::Queued,
        }));

        let (control_tx, control_rx) = mpsc::channel::<TorrentCommand>(16);
        let (peer_msg_tx, peer_msg_rx) = mpsc::unbounded_channel();

        let mut download_loop = DownloadLoop {
            info_hash,
            metainfo: metainfo.clone(),
            storage: storage.clone(),
            piece_mgr: piece_mgr.clone(),
            peer_mgr: peer_mgr.clone(),
            status: status.clone(),
            control_rx,
            peers: HashMap::new(),
            active_downloads: HashMap::new(),
            selector: Box::new(RarestFirst),
            peer_msg_rx,
            peer_msg_tx,
        };

        let task = tokio::spawn(async move {
            download_loop.run().await;
        });

        TorrentHandle {
            info_hash,
            metainfo,
            storage,
            peer_mgr,
            piece_mgr,
            status,
            control_tx,
            task,
        }
    }

    /// Pause this torrent.
    pub async fn pause(&self) -> Result<(), Error> {
        self.control_tx
            .send(TorrentCommand::Pause)
            .await
            .map_err(|_| Error::new(ErrorKind::Protocol))
    }

    /// Resume this torrent.
    pub async fn resume(&self) -> Result<(), Error> {
        self.control_tx
            .send(TorrentCommand::Resume)
            .await
            .map_err(|_| Error::new(ErrorKind::Protocol))
    }

    /// Cancel this torrent (shuts down the download loop).
    pub async fn cancel(&mut self) {
        let _ = self.control_tx.send(TorrentCommand::Cancel).await;
    }

    /// Get the current status.
    pub async fn status(&self) -> TorrentStatus {
        self.status.read().await.clone()
    }
}
