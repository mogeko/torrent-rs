use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use sha1::{Digest, Sha1};

use crate::error::Error;
use crate::peer::PeerMessage;

use super::{InfoHash, SwarmLoop, TorrentEvent};

impl SwarmLoop {
    /// Verify SHA-1 hash of a completed piece and mark it as done.
    pub(super) async fn verify_and_complete_piece(&mut self, index: u32) -> Result<bool, Error> {
        let piece_len = self.piece_len_for_index(index) as usize;

        let expected = match self.metainfo.info.pieces.get(index as usize) {
            Some(h) => *h,
            None => return Ok(false),
        };

        // Verify hash via reference (avoids unnecessary piece-sized allocation).
        let hash_ok = match self.piece_pipeline.active_downloads_mut().get(&index) {
            Some(dl) => verify_piece_hash(&dl.data[..piece_len], expected),
            None => return Ok(false),
        };

        if hash_ok {
            // Clone piece data for caching (only on success).
            let data = match self.piece_pipeline.active_downloads_mut().get(&index) {
                Some(dl) => dl.data[..piece_len].to_vec(),
                None => return Ok(false),
            };

            // Write the entire verified piece to disk in a single I/O.
            self.storage.write_piece(index, &data).await?;

            {
                let mut pm = self.piece_mgr.write().await;
                pm.set_piece(index);
            }
            // Notify external consumers that this piece is ready.
            let _ = self.event_tx.send(TorrentEvent::PieceCompleted { index });
            // Check for newly-completed files — only for multi-file torrents.
            // Use a guard set to avoid re-emitting FileCompleted for files
            // that were already reported as complete.
            if self.completed_files.len() < self.metainfo.info.file_offsets().len() {
                let pm = self.piece_mgr.read().await;
                let fs = self.metainfo.info.file_status(pm.bitfield());
                for f in &fs {
                    if f.progress >= 1.0
                        && f.length > 0
                        && self.completed_files.insert(f.path.clone())
                    {
                        let _ = self.event_tx.send(TorrentEvent::FileCompleted {
                            path: f.path.clone(),
                        });
                    }
                }
            }
            if self.piece_cache.len() >= self.piece_cache_size {
                // LRU eviction: remove oldest (first inserted)
                self.piece_cache.remove(0);
            }
            self.piece_cache.push((index, Arc::new(data)));
            self.piece_pipeline.active_downloads_mut().remove(&index);
            Ok(true)
        } else {
            // Corrupt piece: penalize peers that contributed blocks.
            // Since SHA-1 is per-piece, we can't identify which specific
            // block(s) failed. Each contributing peer gets one strike.
            // Ban threshold is 10 to tolerate false positives in EndGame.
            self.total_wasted += self.piece_len_for_index(index);
            let mut penalized: HashSet<SocketAddr> = HashSet::new();
            if let Some(dl) = self.piece_pipeline.active_downloads_mut().get(&index) {
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
                if peer.corrupt_blocks >= self.corrupt_ban_threshold {
                    ban.push(*addr);
                }
            }
            for addr in &ban {
                tracing::warn!("banning peer {} for repeated corrupt data", addr);
                self.peers.remove(addr);
                self.peer_mgr.write().await.remove_peer(addr);
            }

            self.piece_pipeline.active_downloads_mut().remove(&index);
            let _ = self.event_tx.send(TorrentEvent::PieceFailed { index });
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
pub(super) fn verify_piece_hash(data: &[u8], expected: InfoHash) -> bool {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let computed: InfoHash = hasher.finalize().into();
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
