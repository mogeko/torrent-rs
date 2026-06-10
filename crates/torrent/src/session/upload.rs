use std::collections::HashSet;
use std::net::SocketAddr;

/// Manages uploads: choke/unchoke logic, responding to piece requests.
#[allow(dead_code)]
pub(crate) struct UploadManager {
    max_uploads: u32,
    /// Peers we have unchoked.
    unchoked: HashSet<SocketAddr>,
    /// Current optimistic unchoke peer.
    optimistic_unchoke: Option<SocketAddr>,
}

#[allow(dead_code)]
impl UploadManager {
    /// Create a new UploadManager.
    pub fn new(max_uploads: u32) -> Self {
        UploadManager {
            max_uploads,
            unchoked: HashSet::new(),
            optimistic_unchoke: None,
        }
    }

    /// Check if a peer is unchoked.
    pub fn is_unchoked(&self, addr: &SocketAddr) -> bool {
        self.unchoked.contains(addr)
    }

    /// Choke a peer (stop sending data).
    pub fn choke(&mut self, addr: &SocketAddr) {
        self.unchoked.remove(addr);
    }

    /// Unchoke a peer (allow sending data).
    pub fn unchoke(&mut self, addr: SocketAddr) {
        if self.unchoked.len() < self.max_uploads as usize {
            self.unchoked.insert(addr);
        }
    }

    /// Get the number of unchoked peers.
    pub fn num_unchoked(&self) -> usize {
        self.unchoked.len()
    }
}
