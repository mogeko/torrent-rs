mod announce;
mod choke;
mod peer;
mod pex;
mod pieces;
pub(super) mod types;

pub(crate) use types::{ActiveDownload, PeerEvent, PeerInfo};

use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{RwLock, mpsc};

use crate::bencode::encode as bencode_encode;
use crate::error::Error;
use crate::metainfo::Metainfo;
use crate::peer::{ExtensionNegotiation, PeerId, PeerMessage};
use crate::piece::{PieceManager, PieceSelector};
use crate::storage::Storage;
use crate::tracker::{AnnounceEvent, Tracker};

use super::peer_manager::PeerManager;
use super::torrent::TorrentCommand;
use super::upload::UploadManager;
use super::{TorrentState, TorrentStatus};

use self::types::{UT_PEX, UT_PEX_ID};

/// The core download engine for a single torrent.
pub(crate) struct DownloadLoop {
    pub info_hash: [u8; 20],
    pub metainfo: Metainfo,
    pub storage: Arc<dyn Storage>,
    pub piece_mgr: Arc<RwLock<PieceManager>>,
    pub peer_mgr: Arc<RwLock<PeerManager>>,
    pub status: Arc<RwLock<TorrentStatus>>,
    pub control_rx: mpsc::Receiver<TorrentCommand>,
    /// Our peer ID.
    pub(crate) peer_id: PeerId,
    /// TCP listen port.
    pub(crate) listen_port: u16,
    /// Explicit IPv4 address to announce (BEP 7).
    pub(crate) announce_ip: Option<Ipv4Addr>,
    /// Explicit IPv6 address to announce (BEP 7).
    pub(crate) announce_ipv6: Option<Ipv6Addr>,
    /// Timeout for a single block request.
    pub(crate) request_timeout: Duration,
    /// Maximum concurrent piece downloads.
    pub(crate) max_concurrent_pieces: usize,
    /// How many completed pieces to cache for upload serving.
    pub(crate) piece_cache_size: usize,
    /// EndGame threshold (switch when fewer pieces remain).
    pub(crate) endgame_threshold: usize,
    /// Choke/unchoke interval.
    pub(crate) choke_interval: Duration,
    /// Snub timeout for idle peers.
    pub(crate) snub_timeout: Duration,
    /// Corrupt block ban threshold.
    pub(crate) corrupt_ban_threshold: u32,
    /// Re-announce fallback interval on tracker error.
    pub(crate) announce_fallback_interval: Duration,
    /// Tracker client for peer discovery.
    pub(crate) tracker: Option<Tracker>,
    /// Next announce time.
    pub(crate) next_announce: Option<Instant>,
    /// Have we sent the first announce?
    pub(crate) has_announced: bool,
    /// Have we sent the Completed event?
    pub(crate) announced_completed: bool,
    /// Per-peer protocol state.
    pub(crate) peers: HashMap<SocketAddr, PeerInfo>,
    /// Currently active piece downloads.
    pub(crate) active_downloads: HashMap<u32, ActiveDownload>,
    /// Piece selection strategy (default: rarest-first).
    pub(crate) selector: Box<dyn PieceSelector>,
    /// Receive peer messages from reader tasks.
    pub(crate) peer_msg_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
    /// Clone for spawning new reader tasks.
    pub(crate) peer_msg_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    /// Upload slot manager.
    pub(crate) upload_mgr: Arc<RwLock<UploadManager>>,
    /// Total bytes downloaded.
    pub(crate) total_downloaded: u64,
    /// Total bytes uploaded.
    pub(crate) total_uploaded: u64,
    /// Previous downloaded count for rate calc.
    pub(crate) last_downloaded: u64,
    /// Previous uploaded count for rate calc.
    pub(crate) last_uploaded: u64,
    /// Cached completed pieces for upload serving (avoid repeated disk reads).
    /// Ordered by insertion time — oldest first for LRU eviction.
    pub(crate) piece_cache: Vec<(u32, Arc<Vec<u8>>)>,
    /// Recently disconnected peers to announce in PEX dropped field.
    pub(crate) recently_dropped: Vec<SocketAddr>,
    /// Enable Peer Exchange (BEP 11).
    pub(crate) pex_enabled: bool,
    /// PEX broadcast interval.
    pub(crate) pex_interval: Duration,
}

impl DownloadLoop {
    /// Run the main download loop — event-driven with periodic maintenance.
    pub async fn run(&mut self) {
        {
            let mut status = self.status.write().await;
            status.state = TorrentState::Downloading;
        }

        let mut status_tick = tokio::time::interval(Duration::from_secs(1));
        let mut choke_tick = tokio::time::interval(self.choke_interval);
        let mut stale_tick = tokio::time::interval(Duration::from_secs(30));
        let mut pex_tick = tokio::time::interval(self.pex_interval);

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
                        Some(TorrentCommand::Cancel) | None => {
                            let _ = self.announce_to_tracker(AnnounceEvent::Stopped).await;
                            break;
                        }
                    }
                }
                Some((addr, event)) = self.peer_msg_rx.recv() => {
                    self.handle_peer_event(addr, event).await;
                    if let Err(e) = self.fill_pipelines().await {
                        tracing::warn!("failed to fill pipelines: {}", e);
                    }
                }
                _ = status_tick.tick() => {
                    self.update_status().await;
                    self.announce_if_needed().await;
                    if let Err(e) = self.connect_pending().await {
                        tracing::warn!("failed to connect pending peers: {}", e);
                    }
                }
                _ = choke_tick.tick() => {
                    if let Err(e) = self.run_choke_unchoke().await {
                        tracing::warn!("failed to run choke/unchoke: {}", e);
                    }
                }
                _ = stale_tick.tick() => {
                    self.expire_stale_requests().await;
                }
                _ = pex_tick.tick() => {
                    if self.pex_enabled {
                        if let Err(e) = self.broadcast_pex().await {
                            tracing::warn!("failed to broadcast PEX: {}", e);
                        }
                    }
                }
            }
        }
    }

    /// Update TorrentStatus with rate, progress, peers, seeding state.
    async fn update_status(&mut self) {
        let (progress, num_peers, download_rate, upload_rate) = {
            let pm = self.piece_mgr.read().await;
            let progress = pm.progress();
            let num_peers = self.peer_mgr.read().await.num_connections();
            let download_rate = (self.total_downloaded - self.last_downloaded) as f64;
            let upload_rate = (self.total_uploaded - self.last_uploaded) as f64;
            self.last_downloaded = self.total_downloaded;
            self.last_uploaded = self.total_uploaded;
            (progress, num_peers, download_rate, upload_rate)
        };

        let num_seeds = {
            let num_pieces = self.metainfo.info.num_pieces();
            self.peers
                .values()
                .filter(|p| {
                    !p.bitfield.is_empty()
                        && p.bitfield.len() >= num_pieces
                        && p.bitfield.iter().all(|&b| b)
                })
                .count()
        };

        let is_complete = {
            let pm = self.piece_mgr.read().await;
            pm.missing_pieces().is_empty()
        };

        {
            let mut status = self.status.write().await;
            status.progress = progress;
            status.num_peers = num_peers;
            status.num_seeds = num_seeds;
            status.download_rate = download_rate;
            status.upload_rate = upload_rate;
            if is_complete {
                status.state = TorrentState::Seeding;
            }
        }

        if is_complete && !self.announced_completed {
            let _ = self.announce_to_tracker(AnnounceEvent::Completed).await;
            self.announced_completed = true;
        }
    }

    /// Connect to pending peers (called from status tick).
    async fn connect_pending(&mut self) -> Result<(), Error> {
        let newly_connected = {
            let mut pm = self.peer_mgr.write().await;
            pm.connect_pending().await
        };

        for addr in &newly_connected {
            let conn_arc = {
                let pm = self.peer_mgr.read().await;
                pm.connection(addr)
            };
            if let Some(conn_arc) = conn_arc {
                let mut pi = PeerInfo::new();

                // BEP 10: register our enabled extensions.
                if self.pex_enabled {
                    pi.our_extension_ids.insert(UT_PEX.to_string(), UT_PEX_ID);
                }

                // Send the LTEP handshake if we have any extensions to
                // offer and the remote peer supports the protocol.
                if !pi.our_extension_ids.is_empty() {
                    let remote_ltep = conn_arc.remote_reserved()[5] & 0x10 != 0
                        || conn_arc.remote_has_extension(63);
                    if remote_ltep {
                        self.send_extended_handshake(*addr, &pi.our_extension_ids)
                            .await;
                    }
                }

                self.spawn_peer_reader(*addr, conn_arc);
                self.peers.insert(*addr, pi);
                self.send_bitfield(*addr).await?;

                // PEX is deferred: remote_extension_ids are not known yet.
                // They will be populated when the remote's LTEP handshake
                // arrives via handle_ltep_handshake, which then sends the
                // initial PEX message.
            }
        }
        Ok(())
    }

    /// Send our BEP 10 LTEP extension negotiation handshake.
    async fn send_extended_handshake(&self, addr: SocketAddr, our_ids: &HashMap<String, u8>) {
        let mut neg = ExtensionNegotiation::new();
        for (name, &id) in our_ids {
            neg.add_extension(name, id);
        }
        neg.v = Some(crate::CLIENT_VERSION.to_string());
        let payload = bencode_encode(&neg.to_bencode());
        let peer_mgr = self.peer_mgr.read().await;

        if let Err(e) = peer_mgr
            .send_to(
                &addr,
                &PeerMessage::Extended {
                    ext_id: 0,
                    data: payload,
                },
            )
            .await
        {
            tracing::warn!("failed to send LTEP handshake to {}: {}", addr, e);
        }
    }
}
