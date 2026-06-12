use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock, mpsc};

use crate::error::Error;
use crate::metainfo::Metainfo;
use crate::peer::{PeerConnection, PeerMessage};
use crate::piece::{PieceManager, PieceSelector};
use crate::storage::FileStorage;
use crate::storage::Storage;

use super::peer_manager::PeerManager;
use super::torrent::TorrentCommand;
use super::{TorrentState, TorrentStatus};

/// Event from a peer reader task.
pub(crate) enum PeerEvent {
    /// A valid protocol message.
    Message(PeerMessage),
    /// Peer disconnected (recv error).
    Disconnected,
}

/// Per-peer protocol state tracked by the download loop.
#[allow(dead_code)]
pub(crate) struct PeerInfo {
    /// Which pieces this peer has (from bitfield + have messages).
    bitfield: Vec<bool>,
    /// We are choked by this peer.
    am_choked: bool,
    /// We've sent Interested to this peer.
    am_interested: bool,
    /// This peer has sent Interested to us.
    peer_interested: bool,
}

impl PeerInfo {
    fn new() -> Self {
        PeerInfo {
            bitfield: Vec::new(),
            am_choked: true,
            am_interested: false,
            peer_interested: false,
        }
    }
}

/// An in-progress piece download, assembling blocks from peers.
#[allow(dead_code)]
pub(crate) struct ActiveDownload {
    /// Piece index being downloaded.
    index: u32,
    /// Full piece data buffer (allocated upfront, piece_length bytes).
    data: Vec<u8>,
    /// Which blocks have been received (one bool per block).
    received: Vec<bool>,
    /// Block size in bytes (default 16 KB).
    block_size: u32,
    /// Number of blocks per piece.
    num_blocks: usize,
    /// Peers we've sent requests to for this piece.
    requested_from: HashSet<SocketAddr>,
}

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
}

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
                        Some(TorrentCommand::Cancel) | None => break,
                    }
                }
                Some((addr, event)) = self.peer_msg_rx.recv() => {
                    self.handle_peer_event(addr, event).await;
                }
                _ = tokio::time::sleep(tick_interval) => {
                    if let Err(e) = self.tick().await {
                        let mut status = self.status.write().await;
                        status.state = TorrentState::Error;
                        let _ = e;
                    }
                }
            }
        }
    }

    /// Process one tick: connect peers, request pieces, update status.
    async fn tick(&mut self) -> Result<(), Error> {
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

        // 4. Update status
        {
            let mut status = self.status.write().await;
            let pm = self.piece_mgr.read().await;
            status.progress = pm.progress();
            status.num_peers = self.peer_mgr.read().await.num_connections();

            if pm.missing_pieces().is_empty() {
                status.state = TorrentState::Seeding;
            }
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
        &mut self,
        addr: SocketAddr,
        msg: PeerMessage,
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

                // Update active download
                if let Some(dl) = self.active_downloads.get_mut(&index) {
                    let block_idx = (begin / dl.block_size) as usize;
                    if block_idx < dl.received.len() {
                        let start = begin as usize;
                        let end = start + data.len();
                        if end <= dl.data.len() {
                            dl.data[start..end].copy_from_slice(&data);
                        }
                        dl.received[block_idx] = true;

                        // Check if all blocks received
                        if dl.received.iter().all(|&r| r) {
                            self.active_downloads.remove(&index);
                            // TODO Phase 2: SHA-1 verification and set_piece()
                        }
                    }
                }
            }
            PeerMessage::Request { .. } | PeerMessage::Cancel { .. } | PeerMessage::Port(_) => {
                // Ignored for now (upload in Phase 4)
            }
        }

        Ok(())
    }

    // ── Piece requesting ──────────────────────────────────────────

    /// If no active download and not complete, select the next piece
    /// and request its blocks from a suitable peer.
    async fn maybe_request_piece(&mut self) -> Result<(), Error> {
        let missing = {
            let pm = self.piece_mgr.read().await;
            pm.missing_pieces()
        };
        if missing.is_empty() {
            return Ok(());
        }

        let local_bf = {
            let pm = self.piece_mgr.read().await;
            pm.bitfield().to_vec()
        };

        // Find a suitable piece-peer pair
        let mut selected: Option<(SocketAddr, u32)> = None;
        for (addr, peer) in &self.peers {
            if peer.am_choked || peer.bitfield.is_empty() {
                continue;
            }
            if let Some(idx) = self.selector.select(&peer.bitfield, &local_bf) {
                selected = Some((*addr, idx));
                break;
            }
        }

        if let Some((addr, idx)) = selected {
            self.request_piece_from(&addr, idx).await?;
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
