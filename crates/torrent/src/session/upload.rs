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
        self.unchoked.remove(addr);
    }

    /// Unchoke a peer (allow sending data).
    pub fn unchoke(&mut self, addr: SocketAddr) {
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
