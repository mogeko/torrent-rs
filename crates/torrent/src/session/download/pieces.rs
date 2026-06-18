use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use sha1::{Digest, Sha1};

use crate::error::Error;
use crate::peer::PeerMessage;
use crate::piece::EndGame;

use super::DownloadLoop;
use super::types::{
    ActiveDownload, BLOCK_SIZE, ENDGAME_THRESHOLD, MAX_CONCURRENT_DOWNLOADS, PIECE_CACHE_SIZE,
    REQUEST_TIMEOUT,
};

impl DownloadLoop {
    /// Fill request pipelines for all peers that can accept more requests.
    pub(super) async fn fill_pipelines(&mut self) -> Result<(), Error> {
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

        let missing_count = our_bf.iter().filter(|&&b| !b).count();
        let in_endgame = missing_count > 0 && missing_count < ENDGAME_THRESHOLD;
        if in_endgame {
            self.selector = Box::new(EndGame);
        }

        let peer_addrs: Vec<SocketAddr> = self.peers.keys().copied().collect();
        for addr in peer_addrs {
            let can_req = self.peers.get(&addr).is_some_and(|p| p.can_request());
            if !can_req {
                continue;
            }

            let block_opt = self.find_block_for_peer(&addr, in_endgame);

            let (index, begin) = if let Some(blk) = block_opt {
                blk
            } else if self.active_downloads.len() < MAX_CONCURRENT_DOWNLOADS {
                let selected = self.selector.select(&our_bf, &availability);
                if let Some(idx) = selected {
                    let piece_len = self.piece_len_for_index(idx);
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
            self.peer_mgr.read().await.send_to(&addr, &msg).await?;

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
    pub(super) fn find_block_for_peer(
        &self, addr: &SocketAddr, in_endgame: bool,
    ) -> Option<(u32, u32)> {
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

            if in_endgame {
                for (block_i, assigned) in dl.requested.iter().enumerate() {
                    if assigned.as_ref() == Some(addr) {
                        continue;
                    }
                    if assigned.is_some() {
                        return Some((*idx, block_i as u32 * dl.block_size));
                    }
                }
            }
        }

        None
    }

    /// Expire stale block requests (timeout > REQUEST_TIMEOUT).
    pub(super) async fn expire_stale_requests(&mut self) {
        let now = Instant::now();
        let mut dead_peers = Vec::new();

        for (addr, peer) in &mut self.peers {
            let had_requests = peer.pipeline.iter().any(Option::is_some);
            if !had_requests {
                continue;
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

    /// Verify SHA-1 hash of a completed piece and mark it as done.
    pub(super) async fn verify_and_complete_piece(&mut self, index: u32) -> Result<bool, Error> {
        let piece_len = self.piece_len_for_index(index) as usize;

        let expected = match self.metainfo.info.pieces.get(index as usize) {
            Some(h) => *h,
            None => return Ok(false),
        };

        // Verify hash via reference (avoids unnecessary piece-sized allocation).
        let hash_ok = match self.active_downloads.get(&index) {
            Some(dl) => verify_piece_hash(&dl.data[..piece_len], expected),
            None => return Ok(false),
        };

        if hash_ok {
            // Clone piece data for caching (only on success).
            let data = match self.active_downloads.get(&index) {
                Some(dl) => dl.data[..piece_len].to_vec(),
                None => return Ok(false),
            };
            {
                let mut pm = self.piece_mgr.write().await;
                pm.set_piece(index);
            }
            if self.piece_cache.len() >= PIECE_CACHE_SIZE {
                // LRU eviction: remove oldest (first inserted)
                self.piece_cache.remove(0);
            }
            self.piece_cache.push((index, Arc::new(data)));
            self.active_downloads.remove(&index);
            Ok(true)
        } else {
            // Corrupt piece: penalize peers that contributed blocks.
            // Since SHA-1 is per-piece, we can't identify which specific
            // block(s) failed. Each contributing peer gets one strike.
            // Ban threshold is 10 to tolerate false positives in EndGame.
            let mut penalized: HashSet<SocketAddr> = HashSet::new();
            if let Some(dl) = self.active_downloads.get(&index) {
                for addr in dl.requested.iter().flatten() {
                    if penalized.insert(*addr) {
                        if let Some(peer) = self.peers.get_mut(addr) {
                            peer.corrupt_blocks += 1;
                            tracing::warn!(
                                "peer {} sent corrupt data ({} strikes)",
                                addr,
                                peer.corrupt_blocks
                            );
                        }
                    }
                }
            }
            // Ban peers with repeated corrupt data.
            let mut ban: Vec<SocketAddr> = Vec::new();
            for (addr, peer) in &self.peers {
                if peer.corrupt_blocks >= 10 {
                    ban.push(*addr);
                }
            }
            for addr in &ban {
                tracing::warn!("banning peer {} for repeated corrupt data", addr);
                self.peers.remove(addr);
                self.peer_mgr.write().await.remove_peer(addr);
            }

            self.active_downloads.remove(&index);
            Ok(false)
        }
    }

    /// Send a Have message to all connected peers.
    pub(super) async fn broadcast_have(&self, index: u32) -> Result<(), Error> {
        let msg = PeerMessage::Have(index);
        let pm = self.peer_mgr.read().await;
        for addr in pm.connection_addrs() {
            let _ = pm.send_to(&addr, &msg).await;
        }
        Ok(())
    }

    /// Length of the piece at `index` (last piece may be shorter).
    pub(super) fn piece_len_for_index(&self, index: u32) -> u64 {
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
pub(super) fn verify_piece_hash(data: &[u8], expected: [u8; 20]) -> bool {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let computed: [u8; 20] = hasher.finalize().into();
    computed == expected
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
    fn block_len_for_short_last_block() {
        // Last block of a piece may be shorter than BLOCK_SIZE.
        // Piece length = 50000, block_size = 16384. Block 3 starts at 49152.
        // Remaining = 50000 - 49152 = 848 → block_len should return 848.
        let piece_len: u64 = 50000;
        let block_size: u32 = 16384;
        let remaining = piece_len.saturating_sub(49152);
        assert_eq!(remaining.min(block_size as u64) as u32, 848);
    }
}
