use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{RwLock, mpsc};

use crate::error::Error;
use crate::metainfo::Metainfo;
use crate::piece::PieceManager;
use crate::storage::FileStorage;

use super::peer_manager::PeerManager;
use super::torrent::TorrentCommand;
use super::{TorrentState, TorrentStatus};

/// The core download engine for a single torrent.
#[allow(dead_code)]
pub(crate) struct DownloadLoop {
    pub info_hash: [u8; 20],
    pub metainfo: Metainfo,
    pub storage: Arc<FileStorage>,
    pub piece_mgr: Arc<RwLock<PieceManager>>,
    pub peer_mgr: Arc<RwLock<PeerManager>>,
    pub status: Arc<RwLock<TorrentStatus>>,
    pub control_rx: mpsc::Receiver<TorrentCommand>,
}

impl DownloadLoop {
    /// Run the main download loop.
    pub async fn run(&mut self) {
        // Mark as downloading
        {
            let mut status = self.status.write().await;
            status.state = TorrentState::Downloading;
        }

        let tick_interval = Duration::from_secs(1);

        loop {
            tokio::select! {
                cmd = self.control_rx.recv() => {
                    match cmd {
                        Some(TorrentCommand::Pause) => {
                            let mut status = self.status.write().await;
                            status.state = TorrentState::Paused;
                        }
                        Some(TorrentCommand::Resume) => {
                            let mut status = self.status.write().await;
                            status.state = TorrentState::Downloading;
                        }
                        Some(TorrentCommand::Cancel) | None => break,
                    }
                }
                _ = tokio::time::sleep(tick_interval) => {
                    if let Err(e) = self.tick().await {
                        let mut status = self.status.write().await;
                        status.state = TorrentState::Error;
                        // Log error — in a real client we'd track it
                        let _ = e;
                    }
                }
            }
        }
    }

    /// Process one tick: connect to peers and update status.
    async fn tick(&mut self) -> Result<(), Error> {
        // Connect to pending peers
        {
            let mut pm = self.peer_mgr.write().await;
            pm.connect_pending().await;
        }

        // Update status
        {
            let mut status = self.status.write().await;
            let pm = self.piece_mgr.read().await;
            status.progress = pm.progress();
            status.num_peers = self.peer_mgr.read().await.num_connections();

            // Check if all pieces are complete → seeding
            if pm.missing_pieces().is_empty() {
                status.state = TorrentState::Seeding;
            }
        }

        Ok(())
    }
}
