use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngExt;
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
            am_choked: true,
            am_interested: false,
            peer_interested: false,
            uploaded_bytes: 0,
            downloaded_bytes: 0,
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
    /// Which blocks have been received (one bool per block).
    received: Vec<bool>,
    /// Block size in bytes (default 16 KB).
    block_size: u32,
    /// Number of blocks per piece.
    #[allow(dead_code)]
    num_blocks: usize,
    /// Peers we've sent requests to for this piece.
    requested_from: HashSet<SocketAddr>,
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
    pub(crate) peer_msg_rx: mpsc::UnboundedReceiver<(SocketAddr, PeerEvent)>,
    /// Clone for spawning new reader tasks.
    pub(crate) peer_msg_tx: mpsc::UnboundedSender<(SocketAddr, PeerEvent)>,
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
    /// Tick counter for periodic tasks.
    pub(crate) tick_count: usize,
    /// Cached completed pieces for upload serving (avoid repeated disk reads).
    pub(crate) piece_cache: HashMap<u32, Arc<Vec<u8>>>,
}

/// When fewer than this many pieces remain, switch to EndGame mode
/// (send duplicate requests to multiple peers simultaneously).
const ENDGAME_THRESHOLD: usize = 10;

impl DownloadLoop {
    /// Run the main download loop.
    pub async fn run(&mut self) {
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
                        Some(TorrentCommand::Cancel) | None => {
                            // Best-effort: send Stopped event to tracker
                            let _ = self.announce_to_tracker(AnnounceEvent::Stopped).await;
                            break;
                        }
                    }
                }
                Some((addr, event)) = self.peer_msg_rx.recv() => {
                    self.handle_peer_event(addr, event).await;
                }
                _ = tokio::time::sleep(tick_interval) => {
                    if let Err(e) = self.tick().await {
                        tracing::warn!("download tick failed: {}", e);
                        let mut status = self.status.write().await;
                        status.state = TorrentState::Error;
                    }
                }
            }
        }
    }

    /// Process one tick: announce to tracker, connect peers, request pieces, update status.
    async fn tick(&mut self) -> Result<(), Error> {
        tracing::debug!("download tick");
        // 0. Announce to tracker if needed
        self.announce_if_needed().await;

        // 1. Connect to pending peers
        let newly_connected = {
            let mut pm = self.peer_mgr.write().await;
            pm.connect_pending().await
        };

        // 2. For each new connection: spawn reader + send bitfield
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

        // 3. If idle, request a new piece
        if self.active_downloads.is_empty() {
            self.maybe_request_piece().await?;
        }

        // 4. Update status with rate stats
        self.tick_count += 1;

        {
            let mut status = self.status.write().await;
            let pm = self.piece_mgr.read().await;
            status.progress = pm.progress();
            status.num_peers = self.peer_mgr.read().await.num_connections();

            // Rate calculation (bytes per second, tick runs every 1s)
            status.download_rate = (self.total_downloaded - self.last_downloaded) as f64;
            status.upload_rate = (self.total_uploaded - self.last_uploaded) as f64;
            self.last_downloaded = self.total_downloaded;
            self.last_uploaded = self.total_uploaded;
        }

        // Check seeding state + announce
        let is_seeding = {
            let pm = self.piece_mgr.read().await;
            let complete = pm.missing_pieces().is_empty();
            if complete {
                let mut status = self.status.write().await;
                status.state = TorrentState::Seeding;
            }
            complete
        };

        if is_seeding && !self.announced_completed {
            let _ = self.announce_to_tracker(AnnounceEvent::Completed).await;
            self.announced_completed = true;
        }

        // 5. Periodic choke/unchoke round
        if self.tick_count.is_multiple_of(10) {
            self.run_choke_unchoke().await?;
        }

        Ok(())
    }

    // ── Peer event handling ───────────────────────────────────────

    /// Handle an event from a peer reader task.
    async fn handle_peer_event(&mut self, addr: SocketAddr, event: PeerEvent) {
        match event {
            PeerEvent::Disconnected => {
                self.peers.remove(&addr);
                self.peer_mgr.write().await.remove_peer(&addr);
                // Reassign any active downloads that were assigned to this peer
                let affected: Vec<u32> = self
                    .active_downloads
                    .iter()
                    .filter(|(_, d)| d.requested_from.contains(&addr))
                    .map(|(i, _)| *i)
                    .collect();
                for idx in affected {
                    self.active_downloads.remove(&idx);
                }
            }
            PeerEvent::Message(msg) => {
                if let Err(_e) = self.handle_peer_message(addr, msg).await {
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
            }
            PeerMessage::Unchoke => {
                peer.am_choked = false;
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
                // If peer has pieces we need, we already send Interested on connect
            }
            PeerMessage::Piece { index, begin, data } => {
                // Write to storage
                self.storage.write_block(index, begin, &data).await?;
                self.total_downloaded += data.len() as u64;
                if let Some(peer) = self.peers.get_mut(&addr) {
                    peer.downloaded_bytes += data.len() as u64;
                }

                // Update active download
                let piece_complete = if let Some(dl) = self.active_downloads.get_mut(&index) {
                    let block_idx = (begin / dl.block_size) as usize;
                    if block_idx < dl.received.len() {
                        let start = begin as usize;
                        let end = start + data.len();
                        if end <= dl.data.len() {
                            dl.data[start..end].copy_from_slice(&data);
                        }
                        dl.received[block_idx] = true;
                    }
                    dl.received.iter().all(|&r| r)
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
                // Only serve data if the peer is unchoked
                let is_unchoked = {
                    let um = self.upload_mgr.read().await;
                    um.is_unchoked(&addr)
                };
                if !is_unchoked {
                    return Ok(());
                }

                // Read the piece data (from cache if available, otherwise disk)
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

                    // Track per-peer upload stats
                    if let Some(peer) = self.peers.get_mut(&addr) {
                        peer.uploaded_bytes += (end - start) as u64;
                    }
                }
            }
            PeerMessage::Cancel { .. } | PeerMessage::Port(_) => {
                // Cancel and Port messages are not handled yet
            }
        }

        Ok(())
    }

    // ── Piece requesting ──────────────────────────────────────────

    /// If no active download and not complete, select the next piece
    /// and request its blocks from suitable peers.
    ///
    /// In EndGame mode (fewer than `ENDGAME_THRESHOLD` pieces remaining),
    /// requests are sent to multiple peers simultaneously.
    async fn maybe_request_piece(&mut self) -> Result<(), Error> {
        let missing = {
            let pm = self.piece_mgr.read().await;
            pm.missing_pieces()
        };
        if missing.is_empty() {
            return Ok(());
        }

        // Check EndGame threshold
        let remaining = missing.len();
        let in_endgame = remaining < ENDGAME_THRESHOLD;
        if in_endgame {
            self.selector = Box::new(EndGame);
        }

        let local_bf = {
            let pm = self.piece_mgr.read().await;
            pm.bitfield().to_vec()
        };

        // Find a suitable piece
        let mut piece_idx: Option<u32> = None;
        for peer in self.peers.values() {
            if peer.am_choked || peer.bitfield.is_empty() {
                continue;
            }
            if let Some(idx) = self.selector.select(&peer.bitfield, &local_bf) {
                piece_idx = Some(idx);
                break;
            }
        }

        if let Some(idx) = piece_idx {
            if in_endgame {
                // EndGame: request from ALL peers that have this piece
                let request_addrs: Vec<SocketAddr> = self
                    .peers
                    .iter()
                    .filter(|(_, p)| {
                        !p.am_choked
                            && !p.bitfield.is_empty()
                            && (idx as usize) < p.bitfield.len()
                            && p.bitfield[idx as usize]
                    })
                    .map(|(a, _)| *a)
                    .collect();
                for addr in &request_addrs {
                    self.request_piece_from(addr, idx).await?;
                }
            } else if let Some((addr, _)) = self.peers.iter().find(|(_, p)| {
                !p.am_choked
                    && !p.bitfield.is_empty()
                    && (idx as usize) < p.bitfield.len()
                    && p.bitfield[idx as usize]
            }) {
                let addr = *addr;
                self.request_piece_from(&addr, idx).await?;
            }
        }

        Ok(())
    }

    /// Request all blocks of a piece from a specific peer.
    async fn request_piece_from(&mut self, addr: &SocketAddr, index: u32) -> Result<(), Error> {
        let piece_len = self.piece_len_for_index(index);
        let block_size: u32 = 16 * 1024;
        let block_size_u64 = block_size as u64;
        let num_blocks = piece_len.div_ceil(block_size_u64) as usize;

        let mut dl = ActiveDownload {
            index,
            data: vec![0u8; piece_len as usize],
            received: vec![false; num_blocks],
            block_size,
            num_blocks,
            requested_from: HashSet::new(),
        };
        dl.requested_from.insert(*addr);

        let pm = self.peer_mgr.read().await;
        for block_idx in 0..num_blocks {
            let begin = block_idx as u32 * block_size;
            let len = std::cmp::min(block_size_u64, piece_len - begin as u64) as u32;
            if len == 0 {
                break;
            }
            let msg = PeerMessage::Request {
                index,
                begin,
                length: len,
            };
            pm.send_to(addr, &msg).await?;
        }

        self.active_downloads.insert(index, dl);
        Ok(())
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

        let expected = self.metainfo.info.pieces[index as usize];

        if verify_piece_hash(&data, expected) {
            {
                let mut pm = self.piece_mgr.write().await;
                pm.set_piece(index);
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
                        if tx.send((addr, PeerEvent::Message(msg))).is_err() {
                            break; // DownloadLoop dropped
                        }
                    }
                    Err(_) => {
                        let _ = tx.send((addr, PeerEvent::Disconnected));
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
    use sha1::{Digest, Sha1};
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
            block_size: 16384,
            num_blocks: 1,
            requested_from: HashSet::new(),
        };
        // Verify the index and block metadata are set correctly
        assert_eq!(dl.index, 42);
        assert_eq!(dl.num_blocks, 1);
        assert_eq!(dl.block_size, 16384);
        assert_eq!(dl.data.len(), 16000);
        assert_eq!(dl.received.len(), 1);
        assert_eq!(dl.requested_from.len(), 0);
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
            use sha1::{Digest, Sha1};
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
            use sha1::{Digest, Sha1};
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
            use sha1::{Digest, Sha1};
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
            use sha1::{Digest, Sha1};
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
