use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::error::Error;
use crate::metainfo::Metainfo;
use crate::peer::PeerMessage;
use crate::piece::{PieceManager, PieceSelector};

use super::PeerManager;
use super::types::{ActiveDownload, BLOCK_SIZE, PeerInfo};

/// Manages piece download pipelines across peers.
///
/// Extracted from [`SwarmLoop`] — handles piece selection, block
/// assignment, request dispatching, and stale request expiry.
/// The caller drives the pipeline via [`fill`] and [`expire_stale`].
pub(crate) struct PiecePipeline {
    /// Currently active piece downloads.
    active_downloads: HashMap<u32, ActiveDownload>,
    /// Piece selection strategy (default: rarest-first).
    selector: Box<dyn PieceSelector>,
    /// Maximum concurrent piece downloads.
    max_concurrent_pieces: usize,
    /// EndGame threshold (switch when fewer pieces remain).
    endgame_threshold: usize,
    /// Timeout for a single block request.
    request_timeout: Duration,
}

impl PiecePipeline {
    pub fn new(
        selector: Box<dyn PieceSelector>, max_concurrent_pieces: usize, endgame_threshold: usize,
        request_timeout: Duration,
    ) -> Self {
        PiecePipeline {
            active_downloads: HashMap::new(),
            selector,
            max_concurrent_pieces,
            endgame_threshold,
            request_timeout,
        }
    }

    /// Fill request pipelines for all peers that can accept more requests.
    pub async fn fill(
        &mut self, peers: &mut HashMap<SocketAddr, PeerInfo>, piece_mgr: &RwLock<PieceManager>,
        peer_mgr: &RwLock<PeerManager>, metainfo: &Metainfo,
    ) -> Result<(), Error> {
        let num_pieces = metainfo.info.num_pieces();
        let mut availability = vec![0usize; num_pieces];
        for peer in peers.values() {
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
            let pm = piece_mgr.read().await;
            pm.bitfield().to_vec()
        };

        let missing_count = our_bf.iter().filter(|&&b| !b).count();
        if missing_count > 0 && missing_count < self.endgame_threshold {
            self.selector = Box::new(crate::piece::EndGame);
        }

        let peer_addrs: Vec<SocketAddr> = peers.keys().copied().collect();
        for addr in peer_addrs {
            let can_req_normal = peers.get(&addr).is_some_and(|p| p.can_request());
            let can_req_fast = peers
                .get(&addr)
                .is_some_and(|p| p.am_choked && !p.peer_allowed_fast.is_empty());
            if !can_req_normal && !can_req_fast {
                continue;
            }
            let only_allowed_fast = !can_req_normal && can_req_fast;

            let block_opt =
                Self::find_block_for_peer(peers, &self.active_downloads, &addr, only_allowed_fast);

            let (index, begin) = if let Some(blk) = block_opt {
                blk
            } else if self.active_downloads.len() < self.max_concurrent_pieces {
                let peer_has_bitfield = peers.get(&addr).is_some_and(|p| !p.bitfield.is_empty());
                if !peer_has_bitfield {
                    continue;
                }

                let selected = self.selector.select(&our_bf, &availability);
                if let Some(idx) = selected {
                    if self.active_downloads.contains_key(&idx) {
                        continue;
                    }
                    let piece_len = piece_len_for_index(idx, metainfo);
                    if piece_len == 0 {
                        continue;
                    }
                    let dl = ActiveDownload::new(idx, piece_len, BLOCK_SIZE);
                    #[allow(clippy::unwrap_used)]
                    let blk_begin = dl.next_unrequested().unwrap();
                    self.active_downloads.insert(idx, dl);
                    (idx, blk_begin)
                } else {
                    continue;
                }
            } else {
                continue;
            };

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
            peer_mgr.read().await.send_to(&addr, &msg).await?;

            if let Some(peer) = peers.get_mut(&addr) {
                peer.push_request(index, begin);
            }
            if let Some(dl) = self.active_downloads.get_mut(&index) {
                dl.mark_requested(begin, addr);
            }
        }

        Ok(())
    }

    /// Find the next block to request from a specific peer.
    fn find_block_for_peer(
        peers: &HashMap<SocketAddr, PeerInfo>, active_downloads: &HashMap<u32, ActiveDownload>,
        addr: &SocketAddr, only_allowed_fast: bool,
    ) -> Option<(u32, u32)> {
        let peer = peers.get(addr)?;
        let fast_set = &peer.peer_allowed_fast;

        for index in peer
            .bitfield
            .iter()
            .enumerate()
            .filter_map(|(i, &h)| h.then_some(i as u32))
        {
            if only_allowed_fast && !fast_set.contains(&index) {
                continue;
            }
            if let Some(dl) = active_downloads.get(&index) {
                if let Some(begin) = dl.next_unrequested() {
                    return Some((index, begin));
                }
            }
        }

        None
    }

    /// Expire timed-out block requests and drop dead peers.
    pub async fn expire_stale(
        &mut self, peers: &mut HashMap<SocketAddr, PeerInfo>, peer_mgr: &RwLock<PeerManager>,
    ) {
        let now = Instant::now();
        let timeout = self.request_timeout;
        let mut dead_peers = Vec::new();

        for (addr, peer) in peers.iter_mut() {
            let had_requests = peer.pipeline.iter().any(Option::is_some);
            if !had_requests {
                continue;
            }
            let mut all_expired = true;
            for slot in &mut peer.pipeline {
                if let Some((index, begin, sent_at)) = *slot {
                    if now.duration_since(sent_at) > timeout {
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
            for dl in self.active_downloads.values_mut() {
                for assigned in &mut dl.requested {
                    if *assigned == Some(*addr) {
                        *assigned = None;
                    }
                }
            }
            peers.remove(addr);
            peer_mgr.write().await.remove_peer(addr);
        }
    }

    /// Access active downloads (for piece completion and cancel handling).
    pub fn active_downloads_mut(&mut self) -> &mut HashMap<u32, ActiveDownload> {
        &mut self.active_downloads
    }
}

/// Length of a piece in bytes.
pub(super) fn piece_len_for_index(index: u32, metainfo: &Metainfo) -> u64 {
    let idx = index as u64;
    let num_pieces = metainfo.info.num_pieces() as u64;
    let piece_length = metainfo.info.piece_length;
    if idx >= num_pieces {
        return 0;
    }
    let start = idx * piece_length;
    if idx == num_pieces - 1 {
        metainfo.info.total_size() - start
    } else {
        piece_length
    }
}
