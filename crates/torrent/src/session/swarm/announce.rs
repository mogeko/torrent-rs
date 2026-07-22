use std::time::{Duration, Instant};

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

        // Apply timeout at the call site via tokio::time::timeout.
        // Each inner tracker (HTTP/UDP) is raw — timeout is a
        // cross-cutting concern applied here.
        match tokio::time::timeout(self.tracker_timeout, tracker.announce(req)).await {
            Ok(Ok(resp)) => {
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
            Ok(Err(e)) => {
                // Tracker returned a protocol error — our typed Error.
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
            Err(_elapsed) => {
                // Timeout — tokio::time::timeout elapsed.
                let message = format!(
                    "tracker announce timed out after {:.0}s",
                    self.tracker_timeout.as_secs_f64()
                );
                self.next_announce = Some(Instant::now() + self.announce_fallback_interval);
                {
                    let mut ts = self.tracker_status.write().await;
                    ts.next_announce_in = Some(self.announce_fallback_interval);
                    ts.last_error = Some(message.clone());
                }
                let _ = self.event_tx.send(TorrentEvent::TrackerError {
                    message: message.clone(),
                });
                tracing::warn!("{}", message);
                Err(Error::new(ErrorKind::TrackerRequestFailed))
            }
        }
    }
}
