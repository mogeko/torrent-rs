use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngExt;
use sha1::{Digest, Sha1};
use tokio::sync::{Mutex, RwLock, mpsc};

use crate::error::Error;
use crate::metainfo::Metainfo;
use crate::peer::{PeerConnection, PeerId, PeerMessage};
use crate::piece::{EndGame, PieceManager, PieceSelector};
use crate::storage::{FileStorage, Storage};
use crate::tracker::{AnnounceEvent, AnnounceRequest, Tracker};

use super::peer_manager::PeerManager;
use super::torrent::TorrentCommand;
use super::upload::UploadManager;
use super::{TorrentState, TorrentStatus};

/// Maximum number of pieces to download concurrently.
const MAX_CONCURRENT_DOWNLOADS: usize = 5;

/// How many blocks to keep in-flight per peer (BEP 3 pipelining).
const PIPELINE_SIZE: usize = 5;

/// How many pieces to cache for upload serving (LRU eviction).
const PIECE_CACHE_SIZE: usize = 256;

/// When fewer than this many pieces remain, switch to EndGame mode.
const ENDGAME_THRESHOLD: usize = 10;

/// Timeout for an individual block request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Default block size (BEP 3: 2^14 = 16 KB).
const BLOCK_SIZE: u32 = 16 * 1024;

/// Event from a peer reader task.
pub(crate) enum PeerEvent {
    /// A valid protocol message.
    Message(PeerMessage),
    /// Peer disconnected (recv error).
    Disconnected,
}

/// Per-peer protocol state tracked by the download loop.
pub(crate) struct PeerInfo {
    /// Which pieces this peer has (from bitfield + have messages).
    bitfield: Vec<bool>,
    /// Outstanding block requests sent to this peer.
    /// Fixed-size stack-allocated array: `None` slot = free, `Some` = in-flight.
    pipeline: [Option<(u32, u32, Instant)>; PIPELINE_SIZE],
    /// We are choked by this peer.
    am_choked: bool,
    /// We've sent Interested to this peer.
    #[allow(dead_code)]
    am_interested: bool,
    /// This peer has sent Interested to us.
    peer_interested: bool,
    /// Bytes uploaded to this peer.
    uploaded_bytes: u64,
    /// Bytes downloaded from this peer.
    downloaded_bytes: u64,
}

impl PeerInfo {
    fn new() -> Self {
        PeerInfo {
            bitfield: Vec::new(),
            pipeline: [None; PIPELINE_SIZE],
            am_choked: true,
            am_interested: false,
            peer_interested: false,
            uploaded_bytes: 0,
            downloaded_bytes: 0,
        }
    }

    /// Whether this peer can accept a new request.
    fn can_request(&self) -> bool {
        !self.am_choked && self.pipeline.iter().any(Option::is_none)
    }

    /// Record a new outstanding request.
    fn push_request(&mut self, index: u32, begin: u32) {
        if let Some(slot) = self.pipeline.iter_mut().find(|s| s.is_none()) {
            *slot = Some((index, begin, Instant::now()));
        }
    }

    /// Remove a specific request (piece arrived or cancelled).
    fn remove_request(&mut self, index: u32, begin: u32) {
        for slot in &mut self.pipeline {
            if let Some((i, b, _)) = *slot {
                if i == index && b == begin {
                    *slot = None;
                    return;
                }
            }
        }
    }
}

/// An in-progress piece download, assembling blocks from peers.
pub(crate) struct ActiveDownload {
    /// Piece index being downloaded.
    #[allow(dead_code)]
    index: u32,
    /// Full piece data buffer (allocated upfront, piece_length bytes).
    data: Vec<u8>,
    /// Which peer is currently assigned to each block. `None` = unrequested.
    requested: Vec<Option<SocketAddr>>,
    /// Which blocks have been received (one bool per block).
    received: Vec<bool>,
    /// Block size in bytes (default 16 KB).
    block_size: u32,
    /// Number of blocks per piece.
    #[allow(dead_code)]
    num_blocks: usize,
}

impl ActiveDownload {
    fn new(index: u32, piece_len: u64, block_size: u32) -> Self {
        let num_blocks = piece_len.div_ceil(block_size as u64) as usize;
        ActiveDownload {
            index,
            data: vec![0u8; piece_len as usize],
            received: vec![false; num_blocks],
            requested: vec![None; num_blocks],
            block_size,
            num_blocks,
        }
    }

    /// Find the first unrequested block.
    fn next_unrequested(&self) -> Option<u32> {
        self.requested
            .iter()
            .position(Option::is_none)
            .map(|i| i as u32 * self.block_size)
    }

    /// Length of the block at `begin` offset.
    fn block_len(&self, begin: u32) -> u32 {
        let piece_len = self.data.len() as u64;
        let remaining = piece_len.saturating_sub(begin as u64);
        remaining.min(self.block_size as u64) as u32
    }

    /// Mark a block as requested by a peer.
    ///
    /// In normal mode this is called with blocks that were confirmed
    /// unrequested. In EndGame mode it may overwrite a previous assignment
    /// (duplicate requests to multiple peers).
    fn mark_requested(&mut self, begin: u32, addr: SocketAddr) {
        let block_idx = (begin / self.block_size) as usize;
        if block_idx < self.requested.len() {
            self.requested[block_idx] = Some(addr);
        }
    }

    /// Mark a block as received. Returns `true` if this completes the piece.
    fn mark_received(&mut self, begin: u32, data: &[u8]) -> bool {
        let block_idx = (begin / self.block_size) as usize;
        if block_idx < self.received.len() && !self.received[block_idx] {
            let start = begin as usize;
            let end = start + data.len();
            if end <= self.data.len() {
                self.data[start..end].copy_from_slice(data);
            }
            self.received[block_idx] = true;
        }
        self.received.iter().all(|&r| r)
    }
}

/// The core download engine for a single torrent.
pub(crate) struct DownloadLoop {
    pub info_hash: [u8; 20],
    pub metainfo: Metainfo,
    pub storage: Arc<FileStorage>,
    pub piece_mgr: Arc<RwLock<PieceManager>>,
    pub peer_mgr: Arc<RwLock<PeerManager>>,
    pub status: Arc<RwLock<TorrentStatus>>,
    pub control_rx: mpsc::Receiver<TorrentCommand>,
    /// Our peer ID.
    pub(crate) peer_id: PeerId,
    /// TCP listen port.
    pub(crate) listen_port: u16,
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
    pub(crate) piece_cache: HashMap<u32, Arc<Vec<u8>>>,
}

impl DownloadLoop {
    /// Run the main download loop — event-driven with periodic maintenance.
    pub async fn run(&mut self) {
        {
            let mut status = self.status.write().await;
            status.state = TorrentState::Downloading;
        }

        let mut status_tick = tokio::time::interval(Duration::from_secs(1));
        let mut choke_tick = tokio::time::interval(Duration::from_secs(10));
        let mut stale_tick = tokio::time::interval(Duration::from_secs(30));

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
                        tracing::warn!("fill_pipelines failed: {}", e);
                    }
                }
                _ = status_tick.tick() => {
                    self.update_status().await;
                    self.announce_if_needed().await;
                    if let Err(e) = self.connect_pending().await {
                        tracing::warn!("connect_pending failed: {}", e);
                    }
                }
                _ = choke_tick.tick() => {
                    if let Err(e) = self.run_choke_unchoke().await {
                        tracing::warn!("choke_unchoke failed: {}", e);
                    }
                }
                _ = stale_tick.tick() => {
                    self.expire_stale_requests().await;
                }
            }
        }
    }

    // ── Periodic tasks ────────────────────────────────────────────

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
                self.spawn_peer_reader(*addr, conn_arc);
                self.peers.insert(*addr, PeerInfo::new());
                self.send_bitfield(*addr).await?;
            }
        }
        Ok(())
    }

    // ── Peer event handling ───────────────────────────────────────

    /// Handle an event from a peer reader task.
    async fn handle_peer_event(&mut self, addr: SocketAddr, event: PeerEvent) {
        match event {
            PeerEvent::Disconnected => {
                // Clear this peer's pipeline, mark its blocks as unrequested
                if let Some(peer) = self.peers.get(&addr) {
                    for slot in &peer.pipeline {
                        if let Some((index, begin)) = slot.map(|(i, b, _)| (i, b)) {
                            if let Some(dl) = self.active_downloads.get_mut(&index) {
                                let block_idx = (begin / dl.block_size) as usize;
                                if block_idx < dl.requested.len() {
                                    dl.requested[block_idx] = None;
                                }
                            }
                        }
                    }
                }
                self.peers.remove(&addr);
                self.peer_mgr.write().await.remove_peer(&addr);
            }
            PeerEvent::Message(msg) => {
                if let Err(_e) = self.handle_peer_message(addr, msg).await {
                    // Clear pipeline for dead peer
                    if let Some(peer) = self.peers.get(&addr) {
                        for slot in &peer.pipeline {
                            if let Some((index, begin)) = slot.map(|(i, b, _)| (i, b)) {
                                if let Some(dl) = self.active_downloads.get_mut(&index) {
                                    let block_idx = (begin / dl.block_size) as usize;
                                    if block_idx < dl.requested.len() {
                                        dl.requested[block_idx] = None;
                                    }
                                }
                            }
                        }
                    }
                    self.peers.remove(&addr);
                    self.peer_mgr.write().await.remove_peer(&addr);
                }
            }
        }
    }

    /// Process a single peer wire protocol message.
    async fn handle_peer_message(
        &mut self, addr: SocketAddr, msg: PeerMessage,
    ) -> Result<(), Error> {
        let peer = match self.peers.get_mut(&addr) {
            Some(p) => p,
            None => return Ok(()),
        };

        match msg {
            PeerMessage::KeepAlive => {}
            PeerMessage::Choke => {
                peer.am_choked = true;
                // Clear this peer's pipeline — they won't send the data
                for slot in &mut peer.pipeline {
                    if let Some((index, begin, _)) = *slot {
                        if let Some(dl) = self.active_downloads.get_mut(&index) {
                            let block_idx = (begin / dl.block_size) as usize;
                            if block_idx < dl.requested.len() {
                                dl.requested[block_idx] = None;
                            }
                        }
                    }
                    *slot = None;
                }
            }
            PeerMessage::Unchoke => {
                peer.am_choked = false;
                // fill_pipelines() called after handle_peer_event returns
            }
            PeerMessage::Interested => {
                peer.peer_interested = true;
            }
            PeerMessage::NotInterested => {
                peer.peer_interested = false;
            }
            PeerMessage::Have(index) => {
                let idx = index as usize;
                if idx < peer.bitfield.len() {
                    peer.bitfield[idx] = true;
                }
            }
            PeerMessage::Bitfield(bytes) => {
                let num_pieces = self.metainfo.info.num_pieces();
                peer.bitfield = parse_bitfield(&bytes, num_pieces);
            }
            PeerMessage::Piece { index, begin, data } => {
                // Write to storage
                self.storage.write_block(index, begin, &data).await?;
                self.total_downloaded += data.len() as u64;
                if let Some(p) = self.peers.get_mut(&addr) {
                    p.downloaded_bytes += data.len() as u64;
                }

                // Remove from pipeline
                if let Some(p) = self.peers.get_mut(&addr) {
                    p.remove_request(index, begin);
                }

                // Update active download
                let piece_complete = if let Some(dl) = self.active_downloads.get_mut(&index) {
                    dl.mark_received(begin, &data)
                } else {
                    false
                };

                if piece_complete && self.verify_and_complete_piece(index).await? {
                    self.broadcast_have(index).await?;
                }
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                let is_unchoked = {
                    let um = self.upload_mgr.read().await;
                    um.is_unchoked(&addr)
                };
                if !is_unchoked {
                    return Ok(());
                }

                let piece_data = if let Some(cached) = self.piece_cache.get(&index) {
                    Arc::clone(cached)
                } else {
                    let piece_len = self.piece_len_for_index(index) as usize;
                    let mut piece_buf = vec![0u8; piece_len];
                    self.storage.read_piece(index, &mut piece_buf).await?;
                    Arc::new(piece_buf)
                };

                let start = begin as usize;
                let end = (start + length as usize).min(piece_data.len());
                if start < end {
                    let block_data = piece_data[start..end].to_vec();
                    let msg = PeerMessage::Piece {
                        index,
                        begin,
                        data: block_data,
                    };
                    self.peer_mgr.read().await.send_to(&addr, &msg).await?;
                    self.total_uploaded += (end - start) as u64;
                    if let Some(p) = self.peers.get_mut(&addr) {
                        p.uploaded_bytes += (end - start) as u64;
                    }
                }
            }
            PeerMessage::Cancel { index, begin, .. } => {
                if let Some(p) = self.peers.get_mut(&addr) {
                    p.remove_request(index, begin);
                }
                // Mark block as unrequested in ActiveDownload
                if let Some(dl) = self.active_downloads.get_mut(&index) {
                    let block_idx = (begin / dl.block_size) as usize;
                    if block_idx < dl.requested.len() {
                        dl.requested[block_idx] = None;
                    }
                }
            }
            PeerMessage::Port(_) => {
                // DHT port message — handled at the DHT layer, ignore here
            }
        }

        Ok(())
    }

    // ── Pipeline / piece requesting ───────────────────────────────

    /// Fill request pipelines for all peers that can accept more requests.
    ///
    /// Called after every peer event. Computes piece availability across
    /// the swarm, then for each peer with free pipeline slots, finds the
    /// next block to request.
    async fn fill_pipelines(&mut self) -> Result<(), Error> {
        // Compute availability: how many unchoked peers have each piece
        let num_pieces = self.metainfo.info.num_pieces();
        let mut availability = vec![0usize; num_pieces];
        for peer in self.peers.values() {
            if peer.am_choked || peer.bitfield.is_empty() {
                continue;
            }
            for (i, &has) in peer.bitfield.iter().enumerate() {
                if i >= num_pieces {
                    break;
                }
                if has {
                    availability[i] += 1;
                }
            }
        }

        let our_bf = {
            let pm = self.piece_mgr.read().await;
            pm.bitfield().to_vec()
        };

        // Check EndGame mode
        let missing_count = our_bf.iter().filter(|&&b| !b).count();
        if missing_count < ENDGAME_THRESHOLD {
            self.selector = Box::new(EndGame);
        }

        let in_endgame = missing_count < ENDGAME_THRESHOLD;

        // For each peer with free pipeline slots, find the next block
        let peer_addrs: Vec<SocketAddr> = self.peers.keys().copied().collect();
        for addr in peer_addrs {
            // Re-borrow: we need mutable access per iteration
            let can_req = self.peers.get(&addr).is_some_and(|p| p.can_request());
            if !can_req {
                continue;
            }

            // Try to find a block from an existing ActiveDownload
            let block_opt = self.find_block_for_peer(&addr, in_endgame);

            let (index, begin) = if let Some(blk) = block_opt {
                blk
            } else if self.active_downloads.len() < MAX_CONCURRENT_DOWNLOADS {
                // Start a new piece
                let selected = self.selector.select(&our_bf, &availability);
                if let Some(idx) = selected {
                    let piece_len = self.piece_len_for_index(idx);
                    if piece_len == 0 {
                        continue;
                    }
                    let dl = ActiveDownload::new(idx, piece_len, BLOCK_SIZE);
                    // next_unrequested always returns Some for a fresh ActiveDownload
                    // (all blocks start as None).
                    let blk_begin = dl.next_unrequested().unwrap();
                    self.active_downloads.insert(idx, dl);
                    (idx, blk_begin)
                } else {
                    continue;
                }
            } else {
                continue;
            };

            // Send the request
            let dl = match self.active_downloads.get(&index) {
                Some(d) => d,
                None => continue,
            };
            let len = dl.block_len(begin);
            if len == 0 {
                continue;
            }

            let msg = PeerMessage::Request {
                index,
                begin,
                length: len,
            };
            self.peer_mgr.read().await.send_to(&addr, &msg).await?;

            // Record in pipeline and ActiveDownload
            if let Some(peer) = self.peers.get_mut(&addr) {
                peer.push_request(index, begin);
            }
            if let Some(dl) = self.active_downloads.get_mut(&index) {
                dl.mark_requested(begin, addr);
            }
        }

        Ok(())
    }

    /// Find the next block to request from a specific peer.
    ///
    /// Scans existing `ActiveDownload`s for unrequested blocks that this
    /// peer has. In EndGame mode, also considers blocks already requested
    /// from other peers.
    fn find_block_for_peer(&self, addr: &SocketAddr, in_endgame: bool) -> Option<(u32, u32)> {
        let peer = self.peers.get(addr)?;
        if peer.bitfield.is_empty() {
            return None;
        }

        for (idx, dl) in &self.active_downloads {
            let idx_usize = *idx as usize;
            if idx_usize >= peer.bitfield.len() || !peer.bitfield[idx_usize] {
                continue;
            }

            if let Some(begin) = dl.next_unrequested() {
                return Some((*idx, begin));
            }

            // EndGame: also consider blocks requested from other peers
            if in_endgame {
                for (block_i, assigned) in dl.requested.iter().enumerate() {
                    if assigned.as_ref() == Some(addr) {
                        continue; // already requested from this peer
                    }
                    if assigned.is_some() {
                        // Block is requested from another peer — duplicate request
                        return Some((*idx, block_i as u32 * dl.block_size));
                    }
                }
            }
        }

        None
    }

    /// Expire stale block requests (timeout > REQUEST_TIMEOUT).
    ///
    /// Peers whose *existing* requests all timed out are disconnected.
    /// Peers with no outstanding requests (pipeline all `None`) are
    /// explicitly NOT affected — they may simply not have been assigned
    /// any blocks yet.
    async fn expire_stale_requests(&mut self) {
        let now = Instant::now();
        let mut dead_peers = Vec::new();

        for (addr, peer) in &mut self.peers {
            let had_requests = peer.pipeline.iter().any(Option::is_some);
            if !had_requests {
                continue; // nothing to expire
            }
            let mut all_expired = true;
            for slot in &mut peer.pipeline {
                if let Some((index, begin, sent_at)) = *slot {
                    if now.duration_since(sent_at) > REQUEST_TIMEOUT {
                        if let Some(dl) = self.active_downloads.get_mut(&index) {
                            let block_idx = (begin / dl.block_size) as usize;
                            if block_idx < dl.requested.len() {
                                dl.requested[block_idx] = None;
                            }
                        }
                        *slot = None;
                    } else {
                        all_expired = false;
                    }
                }
            }
            if all_expired {
                dead_peers.push(*addr);
            }
        }

        for addr in &dead_peers {
            // Clear any blocks this dead peer had in ActiveDownloads
            for dl in self.active_downloads.values_mut() {
                for assigned in &mut dl.requested {
                    if *assigned == Some(*addr) {
                        *assigned = None;
                    }
                }
            }
            self.peers.remove(addr);
            self.peer_mgr.write().await.remove_peer(addr);
        }
    }

    // ── Piece verification ───────────────────────────────────────

    /// Verify SHA-1 hash of a completed piece and mark it as done.
    ///
    /// Returns `true` if the piece passed verification and was marked complete.
    /// Returns `false` if verification failed (the download will be discarded).
    async fn verify_and_complete_piece(&mut self, index: u32) -> Result<bool, Error> {
        let piece_len = self.piece_len_for_index(index) as usize;

        let data = match self.active_downloads.get(&index) {
            Some(dl) => dl.data[..piece_len].to_vec(),
            None => return Ok(false),
        };

        let expected = match self.metainfo.info.pieces.get(index as usize) {
            Some(h) => *h,
            None => return Ok(false),
        };

        if verify_piece_hash(&data, expected) {
            {
                let mut pm = self.piece_mgr.write().await;
                pm.set_piece(index);
            }
            // LRU-like eviction: if cache exceeds limit, remove oldest (simplified: remove first)
            if self.piece_cache.len() >= PIECE_CACHE_SIZE {
                let oldest = self.piece_cache.keys().next().copied();
                if let Some(old) = oldest {
                    self.piece_cache.remove(&old);
                }
            }
            self.piece_cache.insert(index, Arc::new(data));
            self.active_downloads.remove(&index);
            Ok(true)
        } else {
            self.active_downloads.remove(&index);
            Ok(false)
        }
    }

    /// Send a Have message to all connected peers.
    async fn broadcast_have(&self, index: u32) -> Result<(), Error> {
        let msg = PeerMessage::Have(index);
        let pm = self.peer_mgr.read().await;
        for addr in pm.connection_addrs() {
            let _ = pm.send_to(&addr, &msg).await;
        }
        Ok(())
    }

    // ── Tracker announce ─────────────────────────────────────────

    /// Announce to the tracker if it's time.
    async fn announce_if_needed(&mut self) {
        if self.tracker.is_none() {
            return;
        }

        let should_announce = match self.next_announce {
            None => true, // First announce
            Some(t) => Instant::now() >= t,
        };

        if !should_announce {
            return;
        }

        let event = if !self.has_announced {
            AnnounceEvent::Started
        } else {
            AnnounceEvent::None
        };

        match self.announce_to_tracker(event).await {
            Ok(()) => {
                self.has_announced = true;
            }
            Err(e) => {
                // Log error; retry after backoff (set by announce_to_tracker)
                let _ = e;
            }
        }
    }

    /// Announce to the tracker with a specific event.
    async fn announce_to_tracker(&mut self, event: AnnounceEvent) -> Result<(), Error> {
        tracing::debug!("announcing to tracker (event: {:?})", event);
        let tracker = match self.tracker.as_ref() {
            Some(t) => t,
            None => return Ok(()),
        };

        // Calculate downloaded/left bytes (approximate)
        let (downloaded, left) = {
            let pm = self.piece_mgr.read().await;
            let have = pm.completed_pieces().len() as u64;
            let piece_len = self.metainfo.info.piece_length;
            let total_size = self.metainfo.info.total_size();
            let d = have * piece_len;
            let l = total_size.saturating_sub(d);
            (d, l)
        };

        let mut req = AnnounceRequest::new(self.info_hash, self.peer_id, self.listen_port);
        req.downloaded = downloaded;
        req.uploaded = self.total_uploaded;
        req.left = left;
        req.event = event;

        match tracker.announce(&req).await {
            Ok(resp) => {
                tracing::debug!("tracker announce: {} peers", resp.peers.len());
                let interval = resp.min_interval.unwrap_or(resp.interval);
                self.next_announce = Some(Instant::now() + Duration::from_secs(interval as u64));

                if !resp.peers.is_empty() {
                    let mut pm = self.peer_mgr.write().await;
                    pm.add_peers(resp.peers);
                }

                Ok(())
            }
            Err(e) => {
                // Backoff on failure
                self.next_announce = Some(Instant::now() + Duration::from_secs(30));
                tracing::warn!("tracker announce failed: {}", e);
                Err(e)
            }
        }
    }

    // ── Upload / choke-unchoke ────────────────────────────────────

    /// Run a choke/unchoke round: select top uploaders + optimistic unchoke.
    async fn run_choke_unchoke(&mut self) -> Result<(), Error> {
        let max_uploads = {
            let um = self.upload_mgr.read().await;
            um.max_uploads()
        };
        if max_uploads == 0 {
            return Ok(());
        }

        // Collect peer addresses and upload stats
        let mut peer_stats: Vec<(SocketAddr, u64)> = self
            .peers
            .iter()
            .map(|(addr, info)| (*addr, info.uploaded_bytes))
            .collect();

        // Sort by uploaded bytes descending
        peer_stats.sort_by_key(|(_, u)| std::cmp::Reverse(*u));

        // Select top (max_uploads - 1), plus one random optimistic unchoke
        let top_count = ((max_uploads - 1) as usize).min(peer_stats.len());
        let mut to_unchoke: HashSet<SocketAddr> =
            peer_stats.iter().take(top_count).map(|(a, _)| *a).collect();

        // Optimistic unchoke: pick a random peer not in the top set
        let candidates: Vec<SocketAddr> =
            peer_stats.iter().skip(top_count).map(|(a, _)| *a).collect();
        if !candidates.is_empty() {
            let idx = rand::rng().random_range(0..candidates.len());
            to_unchoke.insert(candidates[idx]);
        }

        // Also unchoke any newly connected peer (with zero uploaded)
        for addr in self.peers.keys() {
            if to_unchoke.len() >= max_uploads as usize {
                break;
            }
            to_unchoke.insert(*addr);
        }

        // Apply changes
        let mut um = self.upload_mgr.write().await;
        let pm = self.peer_mgr.read().await;

        // Unchoke selected peers
        for addr in &to_unchoke {
            if !um.is_unchoked(addr) {
                um.unchoke(*addr);
                let _ = pm.send_to(addr, &PeerMessage::Unchoke).await;
            }
        }

        // Choke previously unchoked peers that aren't in the new set
        let previously_unchoked: Vec<SocketAddr> = um.unchoked_peers().copied().collect();
        for addr in previously_unchoked {
            if !to_unchoke.contains(&addr) {
                um.choke(&addr);
                let _ = pm.send_to(&addr, &PeerMessage::Choke).await;
            }
        }

        Ok(())
    }

    // ── Helpers ───────────────────────────────────────────────────

    /// Spawn a tokio task that loops `recv()` on a peer connection
    /// and sends messages to the download loop via the channel.
    fn spawn_peer_reader(&self, addr: SocketAddr, conn_arc: Arc<Mutex<PeerConnection>>) {
        let tx = self.peer_msg_tx.clone();
        tokio::spawn(async move {
            loop {
                let msg_result = {
                    let mut conn = conn_arc.lock().await;
                    conn.recv().await
                };
                match msg_result {
                    Ok(msg) => {
                        if tx.send((addr, PeerEvent::Message(msg))).await.is_err() {
                            break; // DownloadLoop dropped
                        }
                    }
                    Err(_) => {
                        let _ = tx.send((addr, PeerEvent::Disconnected)).await;
                        break;
                    }
                }
            }
        });
    }

    /// Send our bitfield and Interested to a newly connected peer.
    async fn send_bitfield(&self, addr: SocketAddr) -> Result<(), Error> {
        let piece_mgr = self.piece_mgr.clone();
        let peer_mgr = self.peer_mgr.clone();

        let bf_bytes = {
            let pm = piece_mgr.read().await;
            pm.to_bitfield()
        };
        let pm = peer_mgr.read().await;
        if !bf_bytes.is_empty() {
            pm.send_to(&addr, &PeerMessage::Bitfield(bf_bytes)).await?;
        }
        pm.send_to(&addr, &PeerMessage::Interested).await?;
        Ok(())
    }

    /// Length of the piece at `index` (last piece may be shorter).
    fn piece_len_for_index(&self, index: u32) -> u64 {
        let idx = index as u64;
        let num_pieces = self.metainfo.info.num_pieces() as u64;
        let piece_length = self.metainfo.info.piece_length;
        if idx >= num_pieces {
            return 0;
        }
        let start = idx * piece_length;
        if idx == num_pieces - 1 {
            self.metainfo.info.total_size() - start
        } else {
            piece_length
        }
    }
}

/// Compute SHA-1 of `data` and compare with `expected`.
fn verify_piece_hash(data: &[u8], expected: [u8; 20]) -> bool {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let computed: [u8; 20] = hasher.finalize().into();
    computed == expected
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn peer_info_default_state() {
        let pi = PeerInfo::new();
        assert!(pi.am_choked);
        assert!(!pi.am_interested);
        assert!(!pi.peer_interested);
        assert!(pi.bitfield.is_empty());
        assert_eq!(pi.uploaded_bytes, 0);
        assert_eq!(pi.downloaded_bytes, 0);
    }

    #[test]
    fn active_download_has_expected_fields() {
        let dl = ActiveDownload {
            index: 42,
            data: vec![0u8; 16000],
            received: vec![false; 1],
            requested: vec![None; 1],
            block_size: 16384,
            num_blocks: 1,
        };
        assert_eq!(dl.index, 42);
        assert_eq!(dl.num_blocks, 1);
        assert_eq!(dl.block_size, 16384);
        assert_eq!(dl.data.len(), 16000);
        assert_eq!(dl.received.len(), 1);
        assert_eq!(dl.requested.len(), 1);
        assert!(dl.requested[0].is_none());
    }
}

/// Parse bitfield bytes into a `Vec<bool>`.
fn parse_bitfield(bytes: &[u8], num_pieces: usize) -> Vec<bool> {
    let mut bf = vec![false; num_pieces];
    for (i, have) in bf.iter_mut().enumerate() {
        let byte = i / 8;
        let bit = 7 - (i % 8);
        if byte < bytes.len() {
            *have = (bytes[byte] & (1 << bit)) != 0;
        }
    }
    bf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_piece_hash_match() {
        let data = b"hello world test piece data";
        let expected = {
            let mut h = Sha1::new();
            h.update(data);
            h.finalize().into()
        };
        assert!(verify_piece_hash(data, expected));
    }

    #[test]
    fn verify_piece_hash_mismatch() {
        let data = b"hello world";
        let expected = [0xFFu8; 20];
        assert!(!verify_piece_hash(data, expected));
    }

    #[test]
    fn verify_piece_hash_empty() {
        let data = b"";
        let expected = {
            let mut h = Sha1::new();
            h.update(b"");
            h.finalize().into()
        };
        assert!(verify_piece_hash(data, expected));
    }

    #[test]
    fn verify_piece_hash_binary_data() {
        let data = [0x00u8, 0xFF, 0x42, 0x7F, 0x80];
        let expected = {
            let mut h = Sha1::new();
            h.update(&data);
            h.finalize().into()
        };
        assert!(verify_piece_hash(&data, expected));
    }

    #[test]
    fn verify_piece_hash_wrong_hash() {
        let data = b"correct data";
        let wrong_data = b"wrong data";
        let wrong_hash = {
            let mut h = Sha1::new();
            h.update(wrong_data);
            h.finalize().into()
        };
        assert!(!verify_piece_hash(data, wrong_hash));
    }

    #[test]
    fn parse_bitfield_all_set() {
        let bytes = vec![0xFF, 0xFF];
        let bf = parse_bitfield(&bytes, 16);
        assert_eq!(bf.len(), 16);
        assert!(bf.iter().all(|&b| b));
    }

    #[test]
    fn parse_bitfield_none_set() {
        let bytes = vec![0x00, 0x00];
        let bf = parse_bitfield(&bytes, 16);
        assert_eq!(bf.len(), 16);
        assert!(bf.iter().all(|&b| !b));
    }

    #[test]
    fn parse_bitfield_partial() {
        // 0x80 = 10000000 → only first piece set
        let bytes = vec![0x80, 0x00];
        let bf = parse_bitfield(&bytes, 16);
        assert_eq!(bf.len(), 16);
        assert!(bf[0]);
        assert!(!bf[1]);
        assert!(!bf[7]);
        assert!(!bf[8]);
    }

    #[test]
    fn parse_bitfield_shorter_than_requested() {
        // Only 1 byte provided, but asking for 16 pieces
        let bytes = vec![0xFF];
        let bf = parse_bitfield(&bytes, 16);
        assert_eq!(bf.len(), 16);
        // First 8 pieces should be set (from 0xFF)
        assert!(bf[0..8].iter().all(|&b| b));
        // Last 8 pieces should be false (no data)
        assert!(bf[8..16].iter().all(|&b| !b));
    }
}
