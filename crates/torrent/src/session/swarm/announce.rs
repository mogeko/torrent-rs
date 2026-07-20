use std::time::{Duration, Instant};

use tower::{Service, ServiceBuilder};

use crate::error::{Error, ErrorKind};
use crate::tracker::{AnnounceEvent, AnnounceRequest};

use super::{SwarmLoop, TorrentEvent};

impl SwarmLoop {
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
            Some(t) => t.clone(),
            None => return Ok(()),
        };

        let downloaded = self.total_downloaded;
        let left = {
            let total_size = self.metainfo.info.total_size();
            let pm = self.piece_mgr.read().await;
            let complete_bytes = (pm.progress() * total_size as f64) as u64;
            total_size.saturating_sub(complete_bytes)
        };

        let mut req = AnnounceRequest::new(self.info_hash, self.peer_id, self.listen_port);
        req.downloaded = downloaded;
        req.uploaded = self.total_uploaded;
        req.left = left;
        req.event = event;
        req.ip = self.announce_ip;
        req.ipv6 = self.announce_ipv6;

        // Apply timeout at the call site via tower middleware.
        // Each inner tracker (HTTP/UDP) is a raw Service — timeout
        // is a cross-cutting concern applied here through the stack.
        let mut svc = ServiceBuilder::new()
            .timeout(self.tracker_timeout)
            .service(tracker);

        match svc.call(req).await {
            Ok(resp) => {
                tracing::debug!("tracker announce: {} peers", resp.peers.len());
                let interval = resp.min_interval.unwrap_or(resp.interval);
                self.next_announce = Some(Instant::now() + Duration::from_secs(interval as u64));
                let peers_found = resp.peers.len();

                {
                    let mut ts = self.tracker_status.write().await;
                    ts.announce_interval = Duration::from_secs(interval as u64);
                    ts.next_announce_in = Some(Duration::from_secs(interval as u64));
                    ts.seeds_reported = resp.complete;
                    ts.leechers_reported = resp.incomplete;
                    ts.last_error = None;
                }

                let _ = self
                    .event_tx
                    .send(TorrentEvent::TrackerAnnounced { peers_found });

                if !resp.peers.is_empty() {
                    let mut pm = self.peer_mgr.write().await;
                    pm.add_peers(resp.peers);
                }

                Ok(())
            }
            Err(e) => {
                // e is Box<dyn Error + Send + Sync> from tower middleware.
                // The underlying cause may be a timeout (tower::timeout::error::Elapsed)
                // or a tracker protocol error (our Error type).
                let message = e.to_string();
                self.next_announce = Some(Instant::now() + self.announce_fallback_interval);
                {
                    let mut ts = self.tracker_status.write().await;
                    ts.next_announce_in = Some(self.announce_fallback_interval);
                    ts.last_error = Some(message.clone());
                }
                let _ = self.event_tx.send(TorrentEvent::TrackerError {
                    message: message.clone(),
                });
                tracing::warn!("failed to announce to tracker: {}", message);
                Err(Error::new(ErrorKind::TrackerRequestFailed))
            }
        }
    }
}
