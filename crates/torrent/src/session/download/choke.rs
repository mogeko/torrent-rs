use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use rand::RngExt;

use crate::error::Error;
use crate::peer::PeerMessage;

use super::DownloadLoop;
use super::types::BLOCK_SIZE;

impl DownloadLoop {
    /// Run a choke/unchoke round: select top uploaders + optimistic unchoke.
    pub(super) async fn run_choke_unchoke(&mut self) -> Result<(), Error> {
        let max_uploads = {
            let um = self.upload_mgr.read().await;
            um.max_uploads()
        };
        if max_uploads == 0 {
            return Ok(());
        }

        // Tit-for-tat (BEP 3): unchoke peers that upload the most TO us.
        // Use downloaded_this_round (bytes peer → us) in this round.
        let mut peer_stats: Vec<(SocketAddr, u64)> = self
            .peers
            .iter()
            .map(|(addr, info)| (*addr, info.downloaded_this_round))
            .collect();

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

        // Snubbing: remove peers idle for >60s (BEP 3)
        let snub_timeout = Duration::from_secs(60);
        to_unchoke.retain(|addr| {
            self.peers.get(addr).is_none_or(|p| {
                // Snub peers that haven't sent data in >60s.
                // Never-sent peers (last_data_received = None) are also snubbed.
                p.last_data_received
                    .is_some_and(|t| t.elapsed() < snub_timeout)
            })
        });

        // Fill remaining slots from rate-sorted list, respecting snub filter.
        // Re-applies the snub check — otherwise a snubbed peer from peer_stats
        // would be re-added here, undoing the retain above.
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

        let mut um = self.upload_mgr.write().await;
        let pm = self.peer_mgr.read().await;

        for addr in &to_unchoke {
            if !um.is_unchoked(addr) {
                um.unchoke(*addr);
                let _ = pm.send_to(addr, &PeerMessage::Unchoke).await;
            }
        }

        let previously_unchoked: Vec<SocketAddr> = um.unchoked_peers().copied().collect();
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
}
