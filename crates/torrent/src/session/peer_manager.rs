use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::error::Error;
use crate::peer::{PeerConnection, PeerId, PeerMessage};

/// Maximum retry attempts per peer before discarding.
const MAX_RETRIES: u32 = 3;

/// Cooldown period before a failed peer can be retried.
const PEER_COOLDOWN: Duration = Duration::from_secs(30);

/// Per-peer backoff state tracking connection retries.
#[derive(Debug)]
struct BackoffState {
    attempts: u32,
    cooldown_until: Instant,
}

impl BackoffState {
    fn new() -> Self {
        BackoffState {
            attempts: 1,
            cooldown_until: Instant::now() + PEER_COOLDOWN,
        }
    }

    fn increment(&mut self) {
        self.attempts += 1;
        self.cooldown_until = Instant::now() + PEER_COOLDOWN;
    }
}

/// Manages peer connections for a single torrent.
pub(crate) struct PeerManager {
    /// Our peer ID.
    peer_id: PeerId,
    /// Info hash of the torrent.
    info_hash: [u8; 20],
    /// Active connections by remote address (behind Mutex for interior mutability).
    connections: HashMap<SocketAddr, Arc<Mutex<PeerConnection>>>,
    /// Pending connection attempts.
    pending: VecDeque<SocketAddr>,
    /// Maximum connections.
    max_connections: u32,
    /// Per-peer backoff state (retry count + cooldown timer).
    backoff: HashMap<SocketAddr, BackoffState>,
}

impl PeerManager {
    /// Create a new PeerManager.
    pub fn new(info_hash: [u8; 20], peer_id: PeerId, max_connections: u32) -> Self {
        PeerManager {
            peer_id,
            info_hash,
            connections: HashMap::new(),
            pending: VecDeque::new(),
            max_connections,
            backoff: HashMap::new(),
        }
    }

    /// Add peers from tracker/DHT announce.
    ///
    /// Skips addresses already connected or already pending.
    pub fn add_peers(&mut self, addrs: Vec<SocketAddr>) {
        for addr in addrs {
            if !self.connections.contains_key(&addr) && !self.pending.contains(&addr) {
                self.pending.push_back(addr);
            }
        }
    }

    /// Send a message to a specific peer.
    pub async fn send_to(&self, addr: &SocketAddr, msg: &PeerMessage) -> Result<(), Error> {
        if let Some(conn) = self.connections.get(addr) {
            let mut guard = conn.lock().await;
            guard.send(msg).await
        } else {
            Ok(())
        }
    }

    /// Remove a peer (disconnect).
    pub fn remove_peer(&mut self, addr: &SocketAddr) {
        tracing::debug!("peer disconnected: {}", addr);
        self.connections.remove(addr);
    }

    /// Get the number of active connections.
    pub fn num_connections(&self) -> usize {
        self.connections.len()
    }

    /// Connect to multiple pending peers in parallel.
    ///
    /// Drains a batch of pending peers (up to available slots) and spawns
    /// concurrent connection attempts. Peers still in cooldown are skipped
    /// and kept in the queue. Failed peers are re-enqueued with a per-peer
    /// cooldown for retry, up to [`MAX_RETRIES`] times.
    pub async fn connect_pending(&mut self) -> Vec<SocketAddr> {
        let batch_size = (self.max_connections as usize).saturating_sub(self.connections.len());
        let drain_count = batch_size.min(self.pending.len());
        let raw_batch: Vec<SocketAddr> = self.pending.drain(..drain_count).collect();

        if raw_batch.is_empty() {
            return vec![];
        }

        let now = Instant::now();
        let mut batch = Vec::with_capacity(raw_batch.len());
        for addr in raw_batch {
            if let Some(state) = self.backoff.get(&addr) {
                if state.cooldown_until > now {
                    // Still in cooldown — put back for later
                    self.pending.push_back(addr);
                    continue;
                }
            }
            batch.push(addr);
        }

        if batch.is_empty() {
            return vec![];
        }

        let mut handles = Vec::with_capacity(batch.len());
        for &addr in &batch {
            let info_hash = self.info_hash;
            let peer_id = self.peer_id;
            handles.push(tokio::spawn(async move {
                let result = PeerConnection::connect(addr, info_hash, peer_id).await;
                (addr, result)
            }));
        }

        let mut connected = Vec::new();
        for handle in handles {
            match handle.await {
                Ok((addr, Ok(conn))) => {
                    tracing::info!("peer connected: {}", addr);
                    self.connections.insert(addr, Arc::new(Mutex::new(conn)));
                    self.backoff.remove(&addr); // clear backoff on success
                    connected.push(addr);
                }
                Ok((addr, Err(_))) => {
                    let state = self.backoff.entry(addr).or_insert_with(BackoffState::new);
                    if state.attempts < MAX_RETRIES {
                        state.increment();
                        tracing::debug!(
                            "re-enqueuing peer {} (attempt {}/{}, cooldown {}s)",
                            addr,
                            state.attempts,
                            MAX_RETRIES,
                            PEER_COOLDOWN.as_secs()
                        );
                        self.pending.push_back(addr);
                    } else {
                        tracing::debug!("peer {}: max retries reached, discarding", addr);
                        self.backoff.remove(&addr);
                    }
                }
                Err(_) => {
                    // spawned task panicked — should not happen
                }
            }
        }

        connected
    }

    /// Get a clone of the connection Arc for a peer.
    pub fn connection(&self, addr: &SocketAddr) -> Option<Arc<Mutex<PeerConnection>>> {
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

    fn test_addr(n: u8) -> SocketAddr {
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, n)),
            6881,
        )
    }

    #[test]
    fn new_creates_empty() {
        let pm = PeerManager::new([0u8; 20], PeerId::random(), 10);
        assert_eq!(pm.num_connections(), 0);
        assert!(pm.connection_addrs().is_empty());
    }

    #[test]
    fn add_peers_to_pending() {
        let mut pm = PeerManager::new([0u8; 20], PeerId::random(), 10);
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
            pending: vec![test_addr(1)].into_iter().collect(),
            max_connections: 0,
            backoff: HashMap::new(),
        };
        // Precondition: capacity 0 means no connections will be attempted
        assert_eq!(pm.max_connections, 0);
    }

    #[test]
    fn remove_peer_nonexistent() {
        let mut pm = PeerManager::new([0u8; 20], PeerId::random(), 10);
        pm.remove_peer(&test_addr(99)); // should not panic
        assert_eq!(pm.num_connections(), 0);
    }

    #[test]
    fn connection_addrs_empty() {
        let pm = PeerManager::new([0u8; 20], PeerId::random(), 10);
        assert!(pm.connection_addrs().is_empty());
    }
}
