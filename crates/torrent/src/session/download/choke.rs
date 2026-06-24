use std::collections::HashSet;
use std::net::SocketAddr;

use rand::RngExt;

use crate::error::Error;
use crate::peer::PeerMessage;

use super::DownloadLoop;
use super::types::BLOCK_SIZE;

impl DownloadLoop {
    /// Returns `true` when we hold every piece — we are in seeding mode.
    pub(super) async fn is_seeding(&self) -> bool {
        let pm = self.piece_mgr.read().await;
        pm.missing_pieces().is_empty()
    }

    /// Run a choke/unchoke round: select top uploaders + optimistic unchoke.
    ///
    /// In download mode (BEP 3 tit-for-tat): unchoke peers that upload the
    /// most TO us, using `downloaded_this_round` (peer → us).  Peers that
    /// haven't sent data for >`snub_timeout` are snubbed.
    ///
    /// In seeding mode: unchoke peers that download the most FROM us, using
    /// `uploaded_this_round` (us → peer).  Only peers that have sent
    /// `Interested` are considered; the snub check is replaced by the
    /// `peer_interested` flag.
    pub(super) async fn run_choke_unchoke(&mut self) -> Result<(), Error> {
        let max_uploads = {
            let um = self.upload_mgr.read().await;
            um.max_uploads()
        };
        if max_uploads == 0 {
            return Ok(());
        }

        let seeding = self.is_seeding().await;

        // Build a sorted peer list.
        //  - Downloading: rank by downloaded_this_round (peer → us, BEP 3 tit-for-tat).
        //  - Seeding:     rank by uploaded_this_round   (us → peer), only interested peers.
        let mut peer_stats: Vec<(SocketAddr, u64)> = if seeding {
            self.peers
                .iter()
                .filter(|(_, info)| info.peer_interested)
                .map(|(addr, info)| (*addr, info.uploaded_this_round))
                .collect()
        } else {
            self.peers
                .iter()
                .map(|(addr, info)| (*addr, info.downloaded_this_round))
                .collect()
        };

        peer_stats.sort_by_key(|(_, u)| std::cmp::Reverse(*u));

        let top_count = ((max_uploads - 1) as usize).min(peer_stats.len());
        let mut to_unchoke: HashSet<SocketAddr> =
            peer_stats.iter().take(top_count).map(|(a, _)| *a).collect();

        let candidates: Vec<SocketAddr> =
            peer_stats.iter().skip(top_count).map(|(a, _)| *a).collect();
        if !candidates.is_empty() {
            let idx = rand::rng().random_range(0..candidates.len());
            to_unchoke.insert(candidates[idx]);
        }

        if seeding {
            // Seeding snub: retain only peers that are still interested.
            to_unchoke.retain(|addr| self.peers.get(addr).is_some_and(|p| p.peer_interested));

            // Refill from the sorted list (respect the same filter).
            for (addr, _) in &peer_stats {
                if to_unchoke.len() >= max_uploads as usize {
                    break;
                }
                let interested = self.peers.get(addr).is_some_and(|p| p.peer_interested);
                if interested {
                    to_unchoke.insert(*addr);
                }
            }
        } else {
            // Download-mode snubbing: remove peers idle for >snub_timeout (BEP 3).
            let snub_timeout = self.snub_timeout;
            to_unchoke.retain(|addr| {
                self.peers.get(addr).is_none_or(|p| {
                    p.last_data_received
                        .is_some_and(|t| t.elapsed() < snub_timeout)
                })
            });

            // Fill remaining slots from rate-sorted list, respecting snub filter.
            for (addr, _) in &peer_stats {
                if to_unchoke.len() >= max_uploads as usize {
                    break;
                }
                let is_active = self.peers.get(addr).is_some_and(|p| {
                    p.last_data_received
                        .is_some_and(|t| t.elapsed() < snub_timeout)
                });
                if is_active {
                    to_unchoke.insert(*addr);
                }
            }
        }

        let mut um = self.upload_mgr.write().await;
        let pm = self.peer_mgr.read().await;

        for addr in &to_unchoke {
            if !um.is_unchoked(addr) {
                um.unchoke(*addr);
                let _ = pm.send_to(addr, &PeerMessage::Unchoke).await;
            }
        }

        let previously_unchoked: Vec<SocketAddr> = um.unchoked_peers().copied().collect();
        let choked_count = previously_unchoked.len();
        for addr in previously_unchoked {
            if !to_unchoke.contains(&addr) {
                // Cancel outstanding requests before choking
                if let Some(peer) = self.peers.get(&addr) {
                    for (index, begin, _) in peer.pipeline.iter().flatten() {
                        let cancel_len = self
                            .active_downloads
                            .get(index)
                            .map(|dl| dl.block_len(*begin))
                            .unwrap_or(BLOCK_SIZE);
                        let msg = PeerMessage::Cancel {
                            index: *index,
                            begin: *begin,
                            length: cancel_len,
                        };
                        let _ = pm.send_to(&addr, &msg).await;
                    }
                }
                // Clear pipeline for this peer
                if let Some(peer) = self.peers.get_mut(&addr) {
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
                um.choke(&addr);
                let _ = pm.send_to(&addr, &PeerMessage::Choke).await;
            }
        }

        // Reset per-round counters
        for info in self.peers.values_mut() {
            info.uploaded_this_round = 0;
            info.downloaded_this_round = 0;
        }

        tracing::debug!(
            "choke round: {} unchoked, {} choked ({} peers total)",
            to_unchoke.len(),
            choked_count.saturating_sub(to_unchoke.len()),
            self.peers.len(),
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    /// Simulate the snub check: returns true if the peer should be kept.
    fn snub_check(last_data_secs_ago: Option<u64>, timeout_secs: u64) -> bool {
        last_data_secs_ago.is_some_and(|s| s < timeout_secs)
    }

    #[test]
    fn snub_filters_idle_peer() {
        assert!(!snub_check(Some(70), 60));
    }

    #[test]
    fn snub_keeps_active_peer() {
        assert!(snub_check(Some(30), 60));
    }

    #[test]
    fn snub_filters_never_sent_peer() {
        assert!(!snub_check(None, 60));
    }

    #[test]
    fn refill_cannot_re_add_snubbed_peer() {
        // Scenario from audit: 3 peers, max_uploads=3, peer B is snubbed.
        // After retain: to_unchoke={A,C}. Refill must NOT add B back.
        let peer_stats: Vec<(u32, u64)> = vec![(1, 100), (2, 50), (3, 10)];
        let snubbed: HashSet<u32> = HashSet::from([2]);

        let mut to_unchoke: HashSet<u32> = HashSet::from([1, 3]);
        let max_uploads = 3;

        for (addr, _) in &peer_stats {
            if to_unchoke.len() >= max_uploads {
                break;
            }
            if !snubbed.contains(addr) {
                to_unchoke.insert(*addr);
            }
        }

        assert!(to_unchoke.contains(&1));
        assert!(to_unchoke.contains(&3));
        assert!(!to_unchoke.contains(&2)); // snubbed peer stays out
    }

    // ── Seeding-mode choke/unchoke helpers ──

    /// Simulate seeding-mode sort: rank by `uploaded_this_round` (us → peer),
    /// only peers with `peer_interested = true` appear in the list.
    fn seeding_peer_stats(
        peers: &[(u32, bool, u64)], interested: &HashSet<u32>,
    ) -> Vec<(u32, u64)> {
        let mut stats: Vec<(u32, u64)> = peers
            .iter()
            .filter(|(addr, _, _)| interested.contains(addr))
            .map(|(addr, _, uploaded)| (*addr, *uploaded))
            .collect();
        stats.sort_by_key(|(_, u)| std::cmp::Reverse(*u));
        stats
    }

    /// Seeding snub: retain only peers that are still interested.
    fn seeding_retain_interested(to_unchoke: &mut HashSet<u32>, interested: &HashSet<u32>) {
        to_unchoke.retain(|addr| interested.contains(addr));
    }

    #[test]
    fn seeding_sorts_by_uploaded_to_peer() {
        // (addr, peer_interested, uploaded_this_round)
        let peers = vec![(1, true, 100), (2, true, 500), (3, true, 50)];
        let interested: HashSet<u32> = HashSet::from([1, 2, 3]);

        let stats = seeding_peer_stats(&peers, &interested);
        // Should be sorted by uploaded (us→peer): 500, 100, 50
        assert_eq!(stats, vec![(2, 500), (1, 100), (3, 50)]);
    }

    #[test]
    fn seeding_excludes_disinterested_peers() {
        let peers = vec![(1, true, 100), (2, false, 500), (3, true, 50)];
        let interested: HashSet<u32> = HashSet::from([1, 3]);

        let stats = seeding_peer_stats(&peers, &interested);
        // Peer 2 (disinterested) should not appear even with high upload.
        assert_eq!(stats, vec![(1, 100), (3, 50)]);
        assert!(!stats.iter().any(|(a, _)| *a == 2));
    }

    #[test]
    fn seeding_retain_filters_disinterested() {
        let mut to_unchoke: HashSet<u32> = HashSet::from([1, 2, 3]);
        let interested: HashSet<u32> = HashSet::from([1, 3]);

        seeding_retain_interested(&mut to_unchoke, &interested);
        assert!(to_unchoke.contains(&1));
        assert!(!to_unchoke.contains(&2));
        assert!(to_unchoke.contains(&3));
    }

    #[test]
    fn seeding_empty_peer_stats_when_none_interested() {
        let peers = vec![(1, false, 100), (2, false, 500)];
        let interested: HashSet<u32> = HashSet::new();

        let stats = seeding_peer_stats(&peers, &interested);
        assert!(stats.is_empty());
    }
}
