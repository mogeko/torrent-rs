mod announce;
mod choke;
mod peer;
mod pex;
mod pieces;
mod types;

pub(crate) use types::{ActiveDownload, PeerEvent, PeerInfo};

use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinSet;

use crate::bencode::encode as bencode_encode;
use crate::error::Error;
use crate::magnet::hex_encode;
use crate::metainfo::{Metainfo, Mode};
use crate::peer::{ExtensionNegotiation, PeerConnection, PeerId, PeerMessage};
use crate::piece::{PieceManager, PieceSelector, RarestFirst};
use crate::spec::TorrentSpec;
use crate::storage::Storage;
use crate::tracker::{AnnounceEvent, Tracker};

use super::peer_mgr::PeerManager;
use super::upload_mgr::UploadManager;
use super::{InfoHash, SessionConfig, TorrentState, TorrentStatus};

use self::types::{UT_PEX, UT_PEX_ID};

/// Commands sent to the download loop.
pub(crate) enum TorrentCommand {
    Pause,
    Resume,
    Cancel,
}

/// Internal handle for a single torrent.
pub(crate) struct TorrentHandle {
    pub info_hash: InfoHash,
    /// Full torrent metadata — `None` for magnet links until
    /// [`DownloadBuilder::resolve_metadata`] downloads it from peers (BEP 9/10).
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

        self.spawn_swarm_loop(metainfo.clone(), storage, config);
    }

    /// Activate for seeding — accept pre-verified piece state.
    ///
    /// Unlike [`activate`](Self::activate), this does NOT call
    /// [`Storage::prepare`] — the files must already exist on disk.
    /// The caller must have already verified on-disk data and populated
    /// `piece_mgr`.
    pub(crate) fn activate_seed(
        &mut self, metainfo: Metainfo, storage: Arc<dyn Storage>, piece_mgr: PieceManager,
        config: &SessionConfig,
    ) {
        let name = match &metainfo.info.mode {
            Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
        };

        assert_eq!(
            metainfo.info_hash(),
            self.info_hash,
            "metainfo info_hash mismatch"
        );

        self.metainfo = Some(metainfo.clone());
        self.piece_mgr = Arc::new(RwLock::new(piece_mgr));

        tracing::info!(
            "torrent activated for seeding: {} ({} pieces)",
            name,
            metainfo.info.num_pieces()
        );

        self.spawn_swarm_loop(metainfo, storage, config);
    }

    /// Build a [`SwarmLoop`], spawn its event loop, and store the
    /// channel + join handle in [`TorrentHandle`].
    fn spawn_swarm_loop(
        &mut self, metainfo: Metainfo, storage: Arc<dyn Storage>, config: &SessionConfig,
    ) {
        let (control_tx, control_rx) = mpsc::channel::<TorrentCommand>(16);
        let (peer_msg_tx, peer_msg_rx) =
            mpsc::channel::<(SocketAddr, PeerEvent)>(config.peer_msg_buffer_size);

        let peer_id = PeerId::random();
        let tracker = Tracker::from_torrent_with_timeout(metainfo.clone(), config.tracker_timeout);

        let mut swarm_loop = SwarmLoop {
            info_hash: self.info_hash,
            metainfo,
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
            upload_mgr: UploadManager::new(config.max_uploads),
            total_downloaded: 0,
            total_uploaded: 0,
            last_downloaded: 0,
            last_uploaded: 0,
            piece_cache: Vec::new(),
            recently_dropped: Vec::new(),
        };

        let task = tokio::spawn(async move { swarm_loop.run().await });

        self.storage = Some(storage);
        self.control_tx = Some(control_tx);
        self.task = Some(task);
    }
}

/// The core swarm engine for a single torrent — manages peer connections,
/// piece downloads/uploads, choke/unchoke, tracker announces, and PEX.
///
/// # Lock Ordering
///
/// When acquiring multiple locks, follow this order to prevent deadlocks:
///
/// 1. `piece_mgr` (if needed)
/// 2. `peer_mgr`
/// 3. `status`
///
/// `peer_mgr` is the most contended lock.  To minimize contention:
/// - `connect_pending()` releases the write lock between draining the
///   pending queue and awaiting connection results (three-phase approach).
/// - `send_to()` takes only a read lock on the hot path.
pub(crate) struct SwarmLoop {
    pub info_hash: InfoHash,
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
    pub(crate) upload_mgr: UploadManager,
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

impl SwarmLoop {
    /// Run the main swarm event loop — event-driven with periodic maintenance.
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
                    if !self.is_seeding().await && let Err(e) = self.fill_pipelines().await {
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
            if is_complete && status.state != TorrentState::Seeding {
                tracing::info!(
                    "download complete, transitioning to seeding ({} pieces)",
                    self.metainfo.info.num_pieces(),
                );
                status.state = TorrentState::Seeding;
            }
        }

        if is_complete && !self.announced_completed {
            let _ = self.announce_to_tracker(AnnounceEvent::Completed).await;
            self.announced_completed = true;
        }
    }

    /// Connect to pending peers (called from status tick).
    ///
    /// Uses a three-phase approach to avoid holding the write lock during
    /// network I/O:
    /// 1. Drain pending batch under write lock
    /// 2. Await connection results WITHOUT the lock
    /// 3. Apply results under write lock
    async fn connect_pending(&mut self) -> Result<(), Error> {
        // Phase 1: Drain pending batch under write lock
        let (batch, connect_timeout) = {
            let mut pm = self.peer_mgr.write().await;
            (pm.drain_pending_batch(), pm.connect_timeout())
        };
        // Write lock RELEASED here

        if batch.is_empty() {
            return Ok(());
        }

        // Phase 2: Spawn connection tasks and collect results WITHOUT lock
        let mut joinset = JoinSet::new();
        for &addr in &batch {
            let info_hash = self.info_hash;
            let peer_id = self.peer_id;
            joinset.spawn(async move {
                let result = PeerConnection::connect(addr, info_hash, peer_id).await;
                (addr, result)
            });
        }

        let mut outcomes: Vec<(SocketAddr, Result<PeerConnection, Error>)> = Vec::new();
        loop {
            match tokio::time::timeout(connect_timeout, joinset.join_next()).await {
                Ok(Some(Ok(result))) => outcomes.push(result),
                Ok(Some(Err(e))) => {
                    tracing::error!("peer connection task panicked: {}", e);
                }
                Ok(None) => break, // all tasks completed
                Err(_) => break,   // per-call timeout — remaining still running
            }
        }

        // Phase 3: Apply results under write lock
        let newly_connected = {
            let mut pm = self.peer_mgr.write().await;
            pm.apply_connect_results(outcomes, &batch)
        };
        // Write lock RELEASED here

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
