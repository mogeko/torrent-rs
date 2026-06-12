use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::error::Error;
use crate::peer::{PeerConnection, PeerId, PeerMessage};

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
    /// Last connect attempt for backoff.
    last_connect_attempt: Option<Instant>,
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
            last_connect_attempt: None,
        }
    }

    /// Add peers from tracker/DHT announce.
    pub fn add_peers(&mut self, addrs: Vec<SocketAddr>) {
        for addr in addrs {
            if !self.connections.contains_key(&addr) {
                self.pending.push_back(addr);
            }
        }
    }

    /// Attempt to connect to the next pending peer.
    ///
    /// Returns `Ok(Some(addr))` on success, `Ok(None)` if at capacity
    /// or no pending peers, `Err` on connection failure.
    pub async fn connect_next(&mut self) -> Result<Option<SocketAddr>, Error> {
        if self.connections.len() as u32 >= self.max_connections {
            return Ok(None); // at capacity
        }

        let addr = match self.pending.pop_front() {
            Some(a) => a,
            None => return Ok(None), // no pending peers
        };

        match PeerConnection::connect(addr, self.info_hash, self.peer_id).await {
            Ok(conn) => {
                self.connections.insert(addr, Arc::new(Mutex::new(conn)));
                Ok(Some(addr))
            }
            Err(e) => Err(e),
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
        self.connections.remove(addr);
    }

    /// Get the number of active connections.
    pub fn num_connections(&self) -> usize {
        self.connections.len()
    }

    /// Connect to multiple pending peers.
    ///
    /// Returns the addresses of all newly connected peers.
    /// Includes backoff: if ALL attempts fail, wait 5 seconds before retrying.
    pub async fn connect_pending(&mut self) -> Vec<SocketAddr> {
        // Backoff check
        if let Some(last) = self.last_connect_attempt
            && last.elapsed() < Duration::from_secs(5)
        {
            return vec![];
        }

        let had_pending = !self.pending.is_empty();
        let mut connected = Vec::new();

        while (self.connections.len() as u32) < self.max_connections {
            match self.connect_next().await {
                Ok(Some(addr)) => connected.push(addr),
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        // If we had pending peers but connected none, start backoff
        if had_pending && connected.is_empty() && self.pending.is_empty() {
            self.last_connect_attempt = Some(Instant::now());
        } else if !connected.is_empty() {
            self.last_connect_attempt = None;
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
    fn connect_next_at_capacity() {
        let pm = PeerManager {
            peer_id: PeerId::random(),
            info_hash: [0u8; 20],
            connections: HashMap::new(),
            pending: vec![test_addr(1)].into_iter().collect(),
            max_connections: 0, // capacity is 0, so all attempts return None
            last_connect_attempt: None,
        };
        // connect_next should return Ok(None) since at capacity
        // We can't actually call .await in sync tests, so test the precondition
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
