mod announce;
mod choke;
mod extension;
mod peer;
mod pex;
mod piece_pipeline;
mod pieces;
mod super_seed;
mod types;

#[allow(unused_imports)]
pub(crate) use extension::{SwarmBuilder, SwarmContext, SwarmExtension};
pub(crate) use types::{PeerEvent, PeerInfo};

use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::task::{JoinHandle, JoinSet};

use crate::bencode::encode as bencode_encode;
use crate::error::Error;
use crate::magnet::hex_encode;
use crate::metainfo::{Metainfo, Mode};
use crate::peer::utp::UtpSocket;
use crate::peer::{
    ExtensionNegotiation, PeerConnection, PeerId, PeerMessage, compute_allowed_fast_set,
};
use crate::piece::{PieceManager, RarestFirst};
use crate::spec::TorrentSpec;
use crate::storage::Storage;
use crate::tracker::{AnnounceEvent, Tracker};

use self::choke::ChokeManager;
use self::pex::PexExtension;
use self::piece_pipeline::PiecePipeline;
use self::super_seed::SuperSeedExtension;
use super::peer_mgr::PeerManager;
use super::upload_mgr::UploadManager;
use super::webseed::{WebSeedConfig, WebSeedExtension};
use super::{
    InfoHash, PeerStatus, SessionConfig, TorrentEvent, TorrentState, TorrentStatus, TrackerStatus,
};

/// Commands sent to the download loop.
pub(crate) enum TorrentCommand {
    Pause,
    Resume,
    Cancel,
    /// An inbound peer connection (TCP accept or uTP SYN).
    #[allow(dead_code)]
    InboundPeer(SocketAddr, Arc<PeerConnection>),
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
    /// Broadcast sender for [`TorrentEvent`]s — each receiver gets
    /// its own copy via [`broadcast::Sender::subscribe`].
    pub event_tx: broadcast::Sender<TorrentEvent>,
    /// Per-peer status snapshots, updated each status tick.
    pub peer_statuses: Arc<RwLock<Vec<PeerStatus>>>,
    /// Tracker communication status, updated after each announce.
    pub tracker_status: Arc<RwLock<TrackerStatus>>,
    /// Web seed URLs collected from the torrent spec — passed to
    /// [`WebSeedExtension`] during activation.
    web_seeds: Vec<String>,
    /// Set by [`activate`](TorrentHandle::activate).
    pub storage: Option<Arc<dyn Storage>>,
    /// Set by [`activate`](TorrentHandle::activate).
    pub control_tx: Option<mpsc::Sender<TorrentCommand>>,
    /// Set by [`activate`](TorrentHandle::activate).
    pub task: Option<JoinHandle<()>>,
    /// Shared uTP socket from the session (BEP 29).
    pub(crate) utp_socket: Option<Arc<UtpSocket>>,
}

impl TorrentHandle {
    /// Register a torrent without storage or download loop.
    /// State = [`TorrentState::Registered`].
    ///
    /// For magnet links, `metainfo` is `None` and `num_pieces` is 0 —
    /// metainfo must be downloaded from peers before activation.
    pub(crate) fn register(spec: TorrentSpec, config: &SessionConfig) -> Self {
        let web_seeds: Vec<String> = spec.web_seeds().into_iter().map(|s| s.to_owned()).collect();

        let (metainfo, info_hash, name, num_pieces, total_size) = match spec {
            TorrentSpec::Metainfo(meta) => {
                let ih = meta.info_hash();
                let num = meta.info.num_pieces();
                let name = match &meta.info.mode {
                    Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
                };
                let ts = meta.info.total_size();
                (Some(meta), ih, name, num, ts)
            }
            TorrentSpec::Magnet(uri) => {
                let ih = *uri.primary_info_hash();
                let name = uri.display_name.unwrap_or_else(|| hex_encode(ih));
                (None, ih, name, 0, 0)
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
            total_size,
            total_downloaded: 0,
            total_uploaded: 0,
            num_pieces: num_pieces as u32,
            pieces_completed: 0,
            bitfield: vec![false; num_pieces],
            elapsed: Duration::ZERO,
            total_wasted: 0,
            super_seed_active: false,
            super_seed_remaining: 0,
            error_message: None,
        }));

        let (event_tx, _) = broadcast::channel(64);
        let peer_statuses = Arc::new(RwLock::new(Vec::new()));
        let tracker_url = metainfo
            .as_ref()
            .map(|m| m.announce.clone())
            .unwrap_or_default();
        let tracker_status = Arc::new(RwLock::new(TrackerStatus {
            url: tracker_url,
            next_announce_in: None,
            announce_interval: Duration::ZERO,
            seeds_reported: 0,
            leechers_reported: 0,
            last_error: None,
        }));

        TorrentHandle {
            info_hash,
            metainfo,
            peer_mgr,
            piece_mgr,
            status,
            event_tx,
            peer_statuses,
            tracker_status,
            web_seeds,
            storage: None,
            control_tx: None,
            task: None,
            utp_socket: None,
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

        let total_size = metainfo.info.total_size();
        let num_pieces = metainfo.info.num_pieces() as u32;

        // Update the status snapshot now that metadata is available.
        {
            let mut status = self
                .status
                .try_write()
                .expect("status lock should be uncontended at activation");
            status.total_size = total_size;
            status.num_pieces = num_pieces;
        }

        tracing::info!(
            "torrent activated: {} ({} pieces)",
            name,
            metainfo.info.num_pieces()
        );

        self.spawn_swarm_loop(metainfo.clone(), storage, config, false);
    }

    /// Activate for seeding — accept pre-verified piece state.
    ///
    /// Unlike [`activate`](Self::activate), this does NOT call
    /// [`Storage::prepare`] — the files must already exist on disk.
    /// The caller must have already verified on-disk data and populated
    /// `piece_mgr`.
    pub(crate) fn activate_seed(
        &mut self, metainfo: Metainfo, storage: Arc<dyn Storage>, piece_mgr: PieceManager,
        config: &SessionConfig, super_seed: bool,
    ) {
        let name = match &metainfo.info.mode {
            Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
        };

        assert_eq!(
            metainfo.info_hash(),
            self.info_hash,
            "metainfo info_hash mismatch"
        );

        let num_pieces = metainfo.info.num_pieces() as u32;
        let total_size = metainfo.info.total_size();
        let pieces_completed = piece_mgr.completed_pieces().len() as u32;
        let progress = piece_mgr.progress();
        let bitfield = piece_mgr.bitfield().to_vec();
        let is_seeding = piece_mgr.missing_pieces().is_empty();

        self.metainfo = Some(metainfo.clone());
        self.piece_mgr = Arc::new(RwLock::new(piece_mgr));

        // Update the status snapshot with verified seed state.
        {
            let mut status = self
                .status
                .try_write()
                .expect("status lock should be uncontended at registration");
            status.total_size = total_size;
            status.num_pieces = num_pieces;
            status.pieces_completed = pieces_completed;
            status.progress = progress;
            status.bitfield = bitfield;
            status.state = if is_seeding {
                TorrentState::Seeding
            } else {
                TorrentState::Downloading
            };
        }

        tracing::info!(
            "torrent activated for seeding: {} ({} pieces)",
            name,
            metainfo.info.num_pieces()
        );

        self.spawn_swarm_loop(metainfo, storage, config, super_seed);
    }

    /// Build a [`SwarmLoop`], spawn its event loop, and store the
    /// channel + join handle in [`TorrentHandle`].
    fn spawn_swarm_loop(
        &mut self, metainfo: Metainfo, storage: Arc<dyn Storage>, config: &SessionConfig,
        super_seed: bool,
    ) {
        let (control_tx, control_rx) = mpsc::channel::<TorrentCommand>(16);
        let (peer_msg_tx, peer_msg_rx) =
            mpsc::channel::<(SocketAddr, PeerEvent)>(config.peer_msg_buffer_size);

        let peer_id = PeerId::random();
        let tracker = Tracker::from_torrent(metainfo.clone());

        let utp_socket = self.utp_socket.clone();

        let mut swarm_loop = SwarmLoop {
            info_hash: self.info_hash,
            metainfo,
            storage: storage.clone(),
            piece_mgr: self.piece_mgr.clone(),
            peer_mgr: self.peer_mgr.clone(),
            status: self.status.clone(),
            event_tx: self.event_tx.clone(),
            peer_statuses: self.peer_statuses.clone(),
            tracker_status: self.tracker_status.clone(),
            started_at: Instant::now(),
            control_rx,
            peer_id,
            listen_port: config.listen_port,
            announce_ip: config.announce_ip,
            announce_ipv6: config.announce_ipv6,
            tracker_timeout: config.tracker_timeout,
            piece_cache_size: config.piece_cache_size,
            choke_manager: ChokeManager::new(
                config.max_uploads,
                config.choke_interval,
                config.snub_timeout,
            ),
            corrupt_ban_threshold: config.corrupt_ban_threshold,
            announce_fallback_interval: config.announce_fallback_interval,
            tracker,
            next_announce: None,
            has_announced: false,
            announced_completed: false,
            peers: HashMap::new(),
            piece_pipeline: PiecePipeline::new(
                Box::new(RarestFirst),
                config.max_concurrent_pieces,
                config.endgame_threshold,
                config.request_timeout,
            ),
            peer_msg_rx,
            peer_msg_tx,
            total_downloaded: 0,
            total_uploaded: 0,
            total_wasted: 0,
            last_downloaded: 0,
            last_uploaded: 0,
            piece_cache: Vec::new(),
            extensions: {
                let mut exts: Vec<Box<dyn SwarmExtension>> = Vec::new();
                if config.pex_enabled {
                    exts.push(Box::new(PexExtension::new(config.pex_interval)));
                }
                if super_seed {
                    exts.push(Box::new(SuperSeedExtension::new()));
                }
                if config.webseed_enabled && !self.web_seeds.is_empty() {
                    exts.push(Box::new(WebSeedExtension::new(
                        self.web_seeds.clone(),
                        WebSeedConfig {
                            min_gap_pieces: config.webseed_min_gap_pieces,
                            max_range_bytes: config.webseed_max_range_bytes,
                            ..Default::default()
                        },
                        config.webseed_concurrency,
                    )));
                }
                exts
            },
            blocked_requests: HashSet::new(),
            confirmed_haves: Vec::new(),
            completed_files: HashSet::new(),
            utp_socket,
        };

        let task = tokio::spawn(async move { swarm_loop.run().await });

        self.storage = Some(storage);
        self.control_tx = Some(control_tx);
        self.task = Some(task);
    }
}

/// The core swarm engine for a single torrent — manages peer connections,
/// piece downloads/uploads, choke/unchoke, tracker announces, PEX, and
/// super seeding (BEP 16).
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
    /// Broadcast sender for [`TorrentEvent`]s to external consumers.
    pub event_tx: broadcast::Sender<TorrentEvent>,
    /// Per-peer status snapshots shared with [`TorrentHandle`].
    pub peer_statuses: Arc<RwLock<Vec<PeerStatus>>>,
    /// Tracker communication status shared with [`TorrentHandle`].
    pub tracker_status: Arc<RwLock<TrackerStatus>>,
    /// Instant when the swarm loop was spawned — used to compute
    /// [`TorrentStatus::elapsed`] each status tick.
    pub started_at: Instant,
    pub control_rx: mpsc::Receiver<TorrentCommand>,
    /// Our peer ID.
    pub(crate) peer_id: PeerId,
    /// TCP listen port.
    pub(crate) listen_port: u16,
    /// Explicit IPv4 address to announce (BEP 7).
    pub(crate) announce_ip: Option<Ipv4Addr>,
    /// Explicit IPv6 address to announce (BEP 7).
    pub(crate) announce_ipv6: Option<Ipv6Addr>,
    /// Timeout for tracker announce calls.
    pub(crate) tracker_timeout: Duration,
    /// How many completed pieces to cache for upload serving.
    pub(crate) piece_cache_size: usize,
    /// Choke/unchoke manager (BEP 3 tit-for-tat).
    pub(crate) choke_manager: ChokeManager,
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
    /// Piece download pipeline (selection, assignment, expiry).
    pub(crate) piece_pipeline: PiecePipeline,
    /// Receive peer messages from reader tasks.
    pub(crate) peer_msg_rx: mpsc::Receiver<(SocketAddr, PeerEvent)>,
    /// Clone for spawning new reader tasks.
    pub(crate) peer_msg_tx: mpsc::Sender<(SocketAddr, PeerEvent)>,
    /// Total bytes downloaded.
    pub(crate) total_downloaded: u64,
    /// Total bytes uploaded.
    pub(crate) total_uploaded: u64,
    /// Total bytes wasted (corrupt + duplicate).
    pub(crate) total_wasted: u64,
    /// Previous downloaded count for rate calc.
    pub(crate) last_downloaded: u64,
    /// Previous uploaded count for rate calc.
    pub(crate) last_uploaded: u64,
    /// Cached completed pieces for upload serving (avoid repeated disk reads).
    /// Ordered by insertion time — oldest first for LRU eviction.
    pub(crate) piece_cache: Vec<(u32, Arc<Vec<u8>>)>,
    /// Registered extensions (PEX, super seed, web seed, …).  Dispatched
    /// sequentially in insertion order for tick and peer events.
    pub(crate) extensions: Vec<Box<dyn SwarmExtension>>,
    /// Requests blocked by extensions (e.g. super seed gating).
    /// Cleared before each peer event dispatch, populated by extensions,
    /// checked by the core Request handler.
    pub(crate) blocked_requests: HashSet<(u32, SocketAddr)>,
    /// Pieces whose HAVE was confirmed by an extension (e.g. super seed
    /// reveal).  The core calls [`broadcast_have`](Self::broadcast_have)
    /// for each entry after extensions process a peer event.
    pub(crate) confirmed_haves: Vec<u32>,
    /// Paths of files that already reached 100% — prevents duplicate
    /// [`TorrentEvent::FileCompleted`] emissions.
    pub(crate) completed_files: HashSet<Vec<String>>,
    /// Shared uTP socket from the session (BEP 29).
    pub(crate) utp_socket: Option<Arc<UtpSocket>>,
}

impl SwarmLoop {
    /// Run the main swarm event loop — event-driven with periodic maintenance.
    pub async fn run(&mut self) {
        {
            let mut status = self.status.write().await;
            status.state = TorrentState::Downloading;
        }

        let mut status_tick = tokio::time::interval(Duration::from_secs(1));
        let mut choke_tick = tokio::time::interval(self.choke_manager.interval());
        let mut stale_tick = tokio::time::interval(Duration::from_secs(30));

        // Run extension on_start hooks (e.g. web seed service init).
        {
            let mut exts = std::mem::take(&mut self.extensions);
            let mut ctx = self.build_context();
            for ext in &mut exts {
                if let Err(e) = ext.on_start(&mut ctx).await {
                    tracing::warn!("extension on_start failed: {}", e);
                }
            }
            self.extensions = exts;
        }

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
                        Some(TorrentCommand::InboundPeer(addr, conn)) => {
                            if let Err(e) = self.register_new_peer(addr, conn).await {
                                tracing::warn!("failed to register inbound peer {}: {}", addr, e);
                            }
                        }
                        Some(TorrentCommand::Cancel) | None => {
                            let _ = self.announce_to_tracker(AnnounceEvent::Stopped).await;
                            break;
                        }
                    }
                }
                Some((addr, event)) = self.peer_msg_rx.recv() => {
                    // Clone before the core handler takes ownership,
                    // so extensions can inspect the event afterwards.
                    let ext_event = event.clone();

                    // Clear pre-event state used for extension ↔ core communication.
                    self.blocked_requests.clear();
                    self.confirmed_haves.clear();

                    // Dispatch to extensions FIRST so they can gate
                    // requests (super seed) before the core sends data.
                    {
                        let mut exts = std::mem::take(&mut self.extensions);
                        let mut ctx = self.build_context();
                        for ext in &mut exts {
                            if let Err(e) = ext.on_peer_event(addr, &ext_event, &mut ctx).await {
                                tracing::warn!("extension on_peer_event failed: {}", e);
                            }
                        }
                        self.extensions = exts;
                    }

                    // Process confirmed_haves set by extensions.
                    for index in std::mem::take(&mut self.confirmed_haves) {
                        if let Err(e) = self.broadcast_have(index).await {
                            tracing::warn!("broadcast_have failed for piece {}: {}", index, e);
                        }
                    }

                    // Core message handling.
                    self.handle_peer_event(addr, event).await;
                    if !self.piece_mgr.read().await.missing_pieces().is_empty()
                        && let Err(e) = self.piece_pipeline.fill(
                            &mut self.peers, &self.piece_mgr, &self.peer_mgr, &self.metainfo,
                        ).await
                    {
                            tracing::warn!("failed to fill pipelines: {}", e);
                        }
                }
                _ = status_tick.tick() => {
                    self.update_status().await;
                    self.announce_if_needed().await;
                    if let Err(e) = self.connect_pending().await {
                        tracing::warn!("failed to connect pending peers: {}", e);
                    }
                    // Dispatch tick to extensions (PEX broadcast, …).
                    {
                        let mut exts = std::mem::take(&mut self.extensions);
                        let mut ctx = self.build_context();
                        for ext in &mut exts {
                            if let Err(e) = ext.on_tick(&mut ctx).await {
                                tracing::warn!("extension on_tick failed: {}", e);
                            }
                        }
                        self.extensions = exts;
                    }
                }
                _ = choke_tick.tick() => {
                    if let Err(e) = self.choke_manager.run_round(
                        &mut self.peers,
                        self.piece_pipeline.active_downloads_mut(),
                        &self.piece_mgr,
                        &self.peer_mgr,
                    ).await {
                        tracing::warn!("failed to run choke/unchoke: {}", e);
                    }
                }
                _ = stale_tick.tick() => {
                    self.piece_pipeline.expire_stale(&mut self.peers, &self.peer_mgr).await;
                }
            }
        }
    }

    /// Build a [`SwarmContext`] borrowing the core loop's shared state.
    ///
    /// Extensions receive this context in `on_start`, `on_tick`, and
    /// `on_peer_event`, giving them read/write access to peers, locks,
    /// and immutable metadata.
    ///
    /// Callers must use [`std::mem::take`] on `self.extensions` before
    /// calling this method to avoid a borrow conflict (this method
    /// borrows `self.peers` mutably while the caller needs to iterate
    /// `self.extensions` mutably).
    fn build_context(&mut self) -> SwarmContext<'_> {
        SwarmContext {
            info_hash: self.info_hash,
            metainfo: &self.metainfo,
            storage: self.storage.clone(),
            event_tx: &self.event_tx,
            listen_port: self.listen_port,
            peer_id: self.peer_id,
            piece_mgr: self.piece_mgr.clone(),
            peer_mgr: &self.peer_mgr,
            status: &self.status,
            peers: &mut self.peers,
            blocked_requests: &mut self.blocked_requests,
            confirmed_haves: &mut self.confirmed_haves,
        }
    }

    /// Update TorrentStatus with rate, progress, peers, seeding state, and bitfield.
    async fn update_status(&mut self) {
        let (
            progress,
            num_peers,
            num_pieces,
            pieces_completed,
            is_complete,
            bitfield,
            total_downloaded,
            download_rate,
            upload_rate,
        ) = {
            let pm = self.piece_mgr.read().await;
            let progress = pm.progress();
            let num_pieces = pm.num_pieces as u32;
            let pieces_completed = pm.completed_pieces().len() as u32;
            let is_complete = pm.missing_pieces().is_empty();
            let bitfield = pm.bitfield().to_vec();
            let num_peers = self.peer_mgr.read().await.num_connections();

            // Derive download stats from verified pieces (BEP 3).
            // Uses piece completion rather than raw block-receive counters,
            // so it covers P2P + web seed uniformly and excludes corrupt data.
            let piece_len = self.metainfo.info.piece_length;
            let total_size = self.metainfo.info.total_size();
            let total_downloaded = (pieces_completed as u64 * piece_len).min(total_size);
            let download_rate = (total_downloaded - self.last_downloaded) as f64;
            self.last_downloaded = total_downloaded;
            self.total_downloaded = total_downloaded;

            let upload_rate = (self.total_uploaded - self.last_uploaded) as f64;
            self.last_uploaded = self.total_uploaded;
            (
                progress,
                num_peers,
                num_pieces,
                pieces_completed,
                is_complete,
                bitfield,
                total_downloaded,
                download_rate,
                upload_rate,
            )
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

        {
            let mut status = self.status.write().await;
            let old_state = status.state;
            status.progress = progress;
            status.num_peers = num_peers;
            status.num_seeds = num_seeds;
            status.download_rate = download_rate;
            status.upload_rate = upload_rate;
            status.num_pieces = num_pieces;
            status.pieces_completed = pieces_completed;
            status.total_downloaded = total_downloaded;
            status.total_uploaded = self.total_uploaded;
            status.bitfield = bitfield;
            status.elapsed = self.started_at.elapsed();
            status.total_wasted = self.total_wasted;

            if is_complete && status.state != TorrentState::Seeding {
                tracing::info!(
                    "download complete, transitioning to seeding ({} pieces)",
                    self.metainfo.info.num_pieces(),
                );
                status.state = TorrentState::Seeding;
            }

            if status.state != old_state {
                let _ = self.event_tx.send(TorrentEvent::StateChanged {
                    from: old_state,
                    to: status.state,
                });
                if status.state == TorrentState::Seeding {
                    let _ = self.event_tx.send(TorrentEvent::TorrentFinished);
                }
            }
        }

        // Populate per-peer status snapshots.
        {
            let mut pss = self.peer_statuses.write().await;
            pss.clear();
            let total_pieces = self.metainfo.info.num_pieces();
            for (addr, pi) in &self.peers {
                let progress = if pi.bitfield.len() >= total_pieces && total_pieces > 0 {
                    pi.bitfield.iter().filter(|&&b| b).count() as f64 / total_pieces as f64
                } else {
                    0.0
                };
                pss.push(PeerStatus {
                    addr: *addr,
                    client_name: pi.client_version.clone(),
                    progress,
                    download_rate: pi.downloaded_this_round as f64,
                    upload_rate: pi.uploaded_this_round as f64,
                    am_choked: pi.am_choked,
                    peer_interested: pi.peer_interested,
                });
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

        // Phase 2: Spawn connection tasks and collect results WITHOUT lock.
        // Race TCP and uTP in parallel; prefer uTP, fall back to TCP.
        // TCP is spawned as a separate task so it runs concurrently with uTP —
        // if uTP times out, TCP may already be connected by then.
        let mut joinset = JoinSet::new();
        for &addr in &batch {
            let (ih, pid, utp_socket) = (self.info_hash, self.peer_id, self.utp_socket.clone());
            joinset.spawn(async move {
                let result = if let Some(utp) = &utp_socket {
                    let tcp_task = tokio::spawn(PeerConnection::connect(addr, ih, pid));
                    match PeerConnection::connect_utp(addr, ih, pid, utp).await {
                        Ok(conn) => {
                            tcp_task.abort(); // cancel the TCP task if uTP succeeded
                            Ok(conn)
                        }
                        Err(_utp_err) => match tcp_task.await {
                            Ok(tcp_result) => tcp_result,
                            Err(join_err) => Err(Error::peer_closed(join_err)),
                        },
                    }
                } else {
                    PeerConnection::connect(addr, ih, pid).await
                };
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
                self.register_new_peer(*addr, conn_arc).await?;
            }
        }
        Ok(())
    }

    /// Register a newly connected peer: LTEP, AllowedFast, reader, Bitfield.
    ///
    /// Shared between outbound (`connect_pending`) and inbound paths.
    async fn register_new_peer(
        &mut self, addr: SocketAddr, conn: Arc<PeerConnection>,
    ) -> Result<(), Error> {
        // Dedup: reject if this peer is already connected (TCP+uTP race)
        {
            let pm = self.peer_mgr.read().await;
            if pm.connection(&addr).is_some() {
                tracing::debug!("peer {} already connected, rejecting duplicate", addr);
                return Ok(());
            }
            // Capacity: reject if at max connections
            if pm.num_connections() >= pm.max_connections() as usize {
                tracing::debug!("max connections reached, rejecting inbound {}", addr);
                return Ok(());
            }
        }

        let mut pi = PeerInfo::new();

        // BEP 10: collect our enabled LTEP extensions from all registered
        // extensions (PEX, future: metadata exchange, …).
        for ext in &self.extensions {
            for (name, id) in ext.ltep_extensions() {
                pi.our_extension_ids.insert(name, id);
            }
        }

        // Send the LTEP handshake if we have any extensions to
        // offer and the remote peer supports the protocol.
        if !pi.our_extension_ids.is_empty() {
            let remote_ltep =
                conn.remote_reserved()[5] & 0x10 != 0 || conn.remote_has_extension(63);
            if remote_ltep {
                self.send_extended_handshake(addr, &pi.our_extension_ids)
                    .await;
            }
        }

        // BEP 6: compute and send our Allowed Fast set to the peer
        // before the Bitfield/HaveAll/HaveNone exchange.
        if conn.remote_has_extension(44) {
            let num_pieces = self.metainfo.info.num_pieces() as u32;
            let fast_set = compute_allowed_fast_set(
                &self.info_hash,
                addr,
                num_pieces,
                10, // k=10 per BEP 6 recommendation
            );
            if !fast_set.is_empty() {
                let pm = self.peer_mgr.read().await;
                for &piece_idx in &fast_set {
                    let _ = pm
                        .send_to(&addr, &PeerMessage::AllowedFast(piece_idx))
                        .await;
                }
            }
            pi.our_allowed_fast = fast_set;
        }

        self.spawn_peer_reader(addr, conn);
        let client_name = pi.client_version.clone();
        self.peers.insert(addr, pi);
        let _ = self
            .event_tx
            .send(TorrentEvent::PeerConnected { addr, client_name });
        self.send_bitfield(addr).await?;

        // PEX is deferred: remote_extension_ids are not known yet.
        // They will be populated when the remote's LTEP handshake
        // arrives via handle_ltep_handshake, which then sends the
        // initial PEX message.
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
            TorrentCommand::Pause
            | TorrentCommand::Resume
            | TorrentCommand::Cancel
            | TorrentCommand::InboundPeer(_, _) => {}
        }
        let _ = (pause, resume, cancel);
    }
}
