use std::collections::HashSet;
use std::net::SocketAddr;

/// Manages uploads: choke/unchoke logic, responding to piece requests.
pub(crate) struct UploadManager {
    max_uploads: u32,
    /// Peers we have unchoked.
    unchoked: HashSet<SocketAddr>,
}

impl UploadManager {
    /// Create a new UploadManager.
    pub fn new(max_uploads: u32) -> Self {
        UploadManager {
            max_uploads,
            unchoked: HashSet::new(),
        }
    }

    /// Check if a peer is unchoked.
    pub fn is_unchoked(&self, addr: &SocketAddr) -> bool {
        self.unchoked.contains(addr)
    }

    /// Choke a peer (stop sending data).
    pub fn choke(&mut self, addr: &SocketAddr) {
        tracing::debug!("choke peer {}", addr);
        self.unchoked.remove(addr);
    }

    /// Unchoke a peer (allow sending data).
    pub fn unchoke(&mut self, addr: SocketAddr) {
        tracing::debug!("unchoke peer {}", addr);
        if self.unchoked.len() < self.max_uploads as usize {
            self.unchoked.insert(addr);
        }
    }

    /// Get the number of unchoked peers.
    #[allow(dead_code)]
    pub fn num_unchoked(&self) -> usize {
        self.unchoked.len()
    }

    /// Get an iterator over all unchoked peer addresses.
    pub fn unchoked_peers(&self) -> impl Iterator<Item = &SocketAddr> {
        self.unchoked.iter()
    }

    /// Get the configured maximum upload slots.
    pub fn max_uploads(&self) -> u32 {
        self.max_uploads
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_addr(n: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, n)), 6881)
    }

    #[test]
    fn new_has_no_unchoked() {
        let um = UploadManager::new(5);
        assert_eq!(um.num_unchoked(), 0);
    }

    #[test]
    fn unchoke_peer() {
        let mut um = UploadManager::new(5);
        let addr = test_addr(1);
        assert!(!um.is_unchoked(&addr));
        um.unchoke(addr);
        assert!(um.is_unchoked(&addr));
    }

    #[test]
    fn choke_peer() {
        let mut um = UploadManager::new(5);
        let addr = test_addr(1);
        um.unchoke(addr);
        assert!(um.is_unchoked(&addr));
        um.choke(&addr);
        assert!(!um.is_unchoked(&addr));
    }

    #[test]
    fn unchoke_respects_max_uploads() {
        let mut um = UploadManager::new(2);
        let a1 = test_addr(1);
        let a2 = test_addr(2);
        let a3 = test_addr(3);
        um.unchoke(a1);
        um.unchoke(a2);
        um.unchoke(a3); // should be ignored
        assert_eq!(um.num_unchoked(), 2);
        assert!(um.is_unchoked(&a1));
        assert!(um.is_unchoked(&a2));
        assert!(!um.is_unchoked(&a3));
    }

    #[test]
    fn unchoked_peers_iterator() {
        let mut um = UploadManager::new(5);
        let a1 = test_addr(1);
        let a2 = test_addr(2);
        um.unchoke(a1);
        um.unchoke(a2);
        let peers: Vec<&SocketAddr> = um.unchoked_peers().collect();
        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&&a1));
        assert!(peers.contains(&&a2));
    }

    #[test]
    fn max_uploads_accessor() {
        let um = UploadManager::new(8);
        assert_eq!(um.max_uploads(), 8);
    }

    #[test]
    fn choke_nonexistent_peer() {
        let mut um = UploadManager::new(5);
        let addr = test_addr(99);
        um.choke(&addr); // should not panic
        assert_eq!(um.num_unchoked(), 0);
    }
}
