use std::time::{Duration, Instant};

use crate::error::Error;
use crate::tracker::{AnnounceEvent, AnnounceRequest};

use super::DownloadLoop;

impl DownloadLoop {
    /// Announce to the tracker if it's time.
    pub(super) async fn announce_if_needed(&mut self) {
        if self.tracker.is_none() {
            return;
        }

        let should_announce = match self.next_announce {
            None => true,
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
                let _ = e;
            }
        }
    }

    /// Announce to the tracker with a specific event.
    pub(super) async fn announce_to_tracker(&mut self, event: AnnounceEvent) -> Result<(), Error> {
        tracing::debug!("announcing to tracker (event: {:?})", event);
        let tracker = match self.tracker.as_ref() {
            Some(t) => t,
            None => return Ok(()),
        };

        let downloaded = self.total_downloaded;
        let left = {
            let total_size = self.metainfo.info.total_size();
            total_size.saturating_sub(self.total_downloaded)
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
                self.next_announce = Some(Instant::now() + self.announce_fallback_interval);
                tracing::warn!("tracker announce failed: {}", e);
                Err(e)
            }
        }
    }
}
