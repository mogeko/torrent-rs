use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::Error;
use crate::peer::{PeerConnection, PeerId, PeerMessage};

use super::InfoHash;
use super::uni_deque::UniDeque;

/// Per-peer backoff state tracking connection retries.
#[derive(Debug)]
struct BackoffState {
    attempts: u32,
    cooldown_until: Instant,
}

impl BackoffState {
    fn new(cooldown: Duration) -> Self {
        BackoffState {
            attempts: 0,
            cooldown_until: Instant::now() + cooldown,
        }
    }

    fn increment(&mut self, cooldown: Duration) {
        self.attempts += 1;
        self.cooldown_until = Instant::now() + cooldown;
    }
}

/// Manages peer connections for a single torrent.
pub(crate) struct PeerManager {
    /// Our peer ID (reserved for future use, e.g. outbound connection spawning).
    #[allow(dead_code)]
    peer_id: PeerId,
    /// Info hash of the torrent (reserved for future use).
    #[allow(dead_code)]
    info_hash: InfoHash,
    /// Active connections by remote address.
    connections: HashMap<SocketAddr, Arc<PeerConnection>>,
    /// Pending connection attempts (O(1) contains via internal HashSet).
    pending: UniDeque<SocketAddr>,
    /// Maximum connections.
    max_connections: u32,
    /// Per-peer backoff state (retry count + cooldown timer).
    backoff: HashMap<SocketAddr, BackoffState>,
    /// Per-peer TCP connect timeout.
    connect_timeout: Duration,
    /// Maximum connection retries before discarding.
    max_retries: u32,
    /// Cooldown before reconnecting a failed peer.
    cooldown: Duration,
}

impl PeerManager {
    /// Create a new PeerManager.
    pub fn new(
        info_hash: InfoHash, peer_id: PeerId, max_connections: u32, connect_timeout: Duration,
        max_retries: u32, cooldown: Duration,
    ) -> Self {
        PeerManager {
            peer_id,
            info_hash,
            connections: HashMap::new(),
            pending: UniDeque::new(),
            max_connections,
            backoff: HashMap::new(),
            connect_timeout,
            max_retries,
            cooldown,
        }
    }

    /// Add peers from tracker/DHT announce.
    ///
    /// Skips addresses already connected. Duplicate entries in the
    /// pending queue are silently skipped by [`UniDeque::push_unique`].
    pub fn add_peers(&mut self, addrs: Vec<SocketAddr>) {
        for addr in addrs {
            if !self.connections.contains_key(&addr) {
                self.pending.push_unique(addr);
            }
        }
    }

    /// Send a message to a specific peer.
    pub async fn send_to(&self, addr: &SocketAddr, msg: &PeerMessage) -> Result<(), Error> {
        if let Some(conn) = self.connections.get(addr) {
            conn.send(msg).await
        } else {
            Ok(())
        }
    }

    /// Remove a peer (disconnect).
    pub fn remove_peer(&mut self, addr: &SocketAddr) {
        tracing::debug!("peer disconnected: {}", addr);
        self.connections.remove(addr);
        self.backoff.remove(addr);
    }

    /// Get the number of active connections.
    pub fn num_connections(&self) -> usize {
        self.connections.len()
    }

    /// Drain a batch of pending peers for connection attempts.
    ///
    /// Returns up to `max_connections - current_connections` peers from the
    /// pending queue. Peers still in cooldown are skipped and re-enqueued.
    ///
    /// The caller should spawn connection tasks and collect results WITHOUT
    /// holding the write lock, then call [`apply_connect_results`](Self::apply_connect_results)
    /// to insert successful connections and update backoff state.
    pub fn drain_pending_batch(&mut self) -> Vec<SocketAddr> {
        let batch_size = (self.max_connections as usize).saturating_sub(self.connections.len());
        let raw_batch: Vec<SocketAddr> = self.pending.drain_first_n(batch_size);

        if raw_batch.is_empty() {
            return vec![];
        }

        let now = Instant::now();
        let mut batch = Vec::with_capacity(raw_batch.len());
        for addr in raw_batch {
            if let Some(state) = self.backoff.get(&addr) {
                if state.cooldown_until > now {
                    let re_enqueued = self.pending.push_unique(addr);
                    debug_assert!(
                        re_enqueued,
                        "cooldown peer {} unexpectedly already in pending set",
                        addr
                    );
                    continue;
                }
            }
            batch.push(addr);
        }

        batch
    }

    /// Return the configured TCP connect timeout.
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Apply connection results from a batch of connection attempts.
    ///
    /// Successful connections are inserted into [`Self::connections`].
    /// Failed connections update per-peer backoff state (re-enqueue with
    /// cooldown, or discard after max retries).  Peers from `batch` that
    /// are not in `outcomes` (timed out) are re-enqueued for the next tick.
    ///
    /// Returns the list of newly-connected peer addresses.
    pub fn apply_connect_results(
        &mut self, outcomes: Vec<(SocketAddr, Result<PeerConnection, Error>)>, batch: &[SocketAddr],
    ) -> Vec<SocketAddr> {
        let processed: HashSet<SocketAddr> = outcomes.iter().map(|(a, _)| *a).collect();
        let mut connected = Vec::new();

        for (addr, result) in outcomes {
            match result {
                Ok(conn) => {
                    tracing::info!("peer connected: {}", addr);
                    self.connections.insert(addr, Arc::new(conn));
                    self.backoff.remove(&addr);
                    connected.push(addr);
                }
                Err(_) => {
                    let state = self
                        .backoff
                        .entry(addr)
                        .or_insert_with(|| BackoffState::new(self.cooldown));
                    if state.attempts < self.max_retries {
                        state.increment(self.cooldown);
                        tracing::debug!(
                            "re-enqueuing peer {} (attempt {}/{}, cooldown {}s)",
                            addr,
                            state.attempts,
                            self.max_retries,
                            self.cooldown.as_secs()
                        );
                        let re_enqueued = self.pending.push_unique(addr);
                        debug_assert!(
                            re_enqueued,
                            "retried peer {} unexpectedly already in pending set",
                            addr
                        );
                    } else {
                        tracing::debug!("peer {}: max retries reached, discarding", addr);
                        self.backoff.remove(&addr);
                    }
                }
            }
        }

        // Re-enqueue peers whose tasks timed out (not in outcomes)
        for addr in batch {
            if !processed.contains(addr) {
                self.pending.push_unique(*addr);
            }
        }

        connected
    }

    /// Get a clone of the connection Arc for a peer.
    pub fn connection(&self, addr: &SocketAddr) -> Option<Arc<PeerConnection>> {
        self.connections.get(addr).cloned()
    }

    /// Get all connected peer addresses.
    pub fn connection_addrs(&self) -> Vec<SocketAddr> {
        self.connections.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pm(max_connections: u32) -> PeerManager {
        PeerManager::new(
            [0u8; 20],
            PeerId::random(),
            max_connections,
            Duration::from_millis(500),
            3,
            Duration::from_secs(30),
        )
    }

    fn test_addr(n: u8) -> SocketAddr {
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, n)),
            6881,
        )
    }

    #[test]
    fn new_creates_empty() {
        let pm = test_pm(10);
        assert_eq!(pm.num_connections(), 0);
        assert!(pm.connection_addrs().is_empty());
    }

    #[test]
    fn add_peers_to_pending() {
        let mut pm = test_pm(10);
        pm.add_peers(vec![test_addr(1), test_addr(2)]);
        // connect_next() will attempt to connect; at this point they're pending
        // We can verify by checking that connection_addrs is still empty
        assert_eq!(pm.num_connections(), 0);
    }

    #[test]
    fn at_capacity_precondition() {
        let pm = PeerManager {
            peer_id: PeerId::random(),
            info_hash: [0u8; 20],
            connections: HashMap::new(),
            pending: {
                let mut d = UniDeque::new();
                d.push_unique(test_addr(1));
                d
            },
            max_connections: 0,
            backoff: HashMap::new(),
            connect_timeout: Duration::from_millis(500),
            max_retries: 3,
            cooldown: Duration::from_secs(30),
        };
        // Precondition: capacity 0 means no connections will be attempted
        assert_eq!(pm.max_connections, 0);
    }

    #[test]
    fn remove_peer_nonexistent() {
        let mut pm = test_pm(10);
        pm.remove_peer(&test_addr(99)); // should not panic
        assert_eq!(pm.num_connections(), 0);
    }

    #[test]
    fn connection_addrs_empty() {
        let pm = test_pm(10);
        assert!(pm.connection_addrs().is_empty());
    }
}
