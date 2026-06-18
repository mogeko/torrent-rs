use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use rand::RngExt;

use crate::error::Error;
use crate::peer::PeerMessage;

use super::DownloadLoop;

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

        // Fill remaining slots with next-best uploaders (not yet in to_unchoke).
        // This preserves the snubbing filter above by only adding peers from
        // the rate-sorted list, not all connected peers unconditionally.
        for (addr, _) in &peer_stats {
            if to_unchoke.len() >= max_uploads as usize {
                break;
            }
            to_unchoke.insert(*addr);
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
                        let msg = PeerMessage::Cancel {
                            index: *index,
                            begin: *begin,
                            length: 0,
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
