use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use crate::peer::PeerMessage;

/// How many blocks to keep in-flight per peer (BEP 3 pipelining).
pub(super) const PIPELINE_SIZE: usize = 5;

/// Default block size (BEP 3: 2^14 = 16 KB).
pub(super) const BLOCK_SIZE: u32 = 16 * 1024;

/// BEP 10 extension name: Peer Exchange (BEP 11).  µTorrent prefix.
pub(super) const UT_PEX: &str = "ut_pex";
/// Our assigned extension message ID for `ut_pex` in outgoing LTEP
/// handshakes.
pub(super) const UT_PEX_ID: u8 = 1;

/// Event from a peer reader task.
pub(crate) enum PeerEvent {
    /// A valid protocol message.
    Message(PeerMessage),
    /// Peer disconnected (recv error).
    Disconnected,
}

/// Per-peer protocol state tracked by the download loop.
pub(crate) struct PeerInfo {
    /// Which pieces this peer has (from bitfield + have messages).
    pub(super) bitfield: Vec<bool>,
    /// Outstanding block requests sent to this peer.
    /// Fixed-size stack-allocated array: `None` slot = free, `Some` = in-flight.
    pub(super) pipeline: [Option<(u32, u32, Instant)>; PIPELINE_SIZE],
    /// We are choked by this peer.
    pub(super) am_choked: bool,
    /// We've sent Interested to this peer.
    #[allow(dead_code)]
    pub(super) am_interested: bool,
    /// This peer has sent Interested to us.
    pub(super) peer_interested: bool,
    /// Bytes uploaded to this peer (cumulative).
    pub(super) uploaded_bytes: u64,
    /// Bytes uploaded during the current choke round.
    pub(super) uploaded_this_round: u64,
    /// Bytes downloaded from this peer (cumulative).
    pub(super) downloaded_bytes: u64,
    /// Bytes downloaded during the current choke round (tit-for-tat metric).
    pub(super) downloaded_this_round: u64,
    /// Number of corrupt blocks received from this peer.
    pub(super) corrupt_blocks: u32,
    /// Last time we received a Piece message from this peer.
    pub(super) last_data_received: Option<Instant>,
    /// Extension name → message ID mapping from the remote peer (BEP 10).
    /// Used when sending extended messages TO this peer.
    pub(super) remote_extension_ids: HashMap<String, u8>,
    /// Extension name → message ID mapping we offered to the remote (BEP 10).
    /// Used when decoding extended messages FROM this peer.
    pub(super) our_extension_ids: HashMap<String, u8>,
    /// Last time we sent a PEX message to this peer.
    pub(super) last_pex_sent: Option<Instant>,
    /// Last time we received a PEX message from this peer.
    pub(super) last_pex_received: Option<Instant>,
    /// Maximum number of outstanding block requests this peer accepts.
    /// Derived from the remote's BEP 10 `reqq` handshake field, capped
    /// at [`PIPELINE_SIZE`]. Defaults to [`PIPELINE_SIZE`] if the remote
    /// did not advertise a value.
    pub(super) max_requests: usize,
    /// Client name and version from remote's LTEP handshake (BEP 10 `v`).
    pub(super) client_version: Option<String>,
    /// Metadata size from remote's LTEP handshake (BEP 10 `metadata_size`,
    /// for BEP 9 metadata exchange).
    pub(super) metadata_size: Option<i64>,
}

impl PeerInfo {
    pub(super) fn new() -> Self {
        PeerInfo {
            bitfield: Vec::new(),
            pipeline: [None; PIPELINE_SIZE],
            am_choked: true,
            am_interested: false,
            peer_interested: false,
            uploaded_bytes: 0,
            uploaded_this_round: 0,
            downloaded_bytes: 0,
            downloaded_this_round: 0,
            corrupt_blocks: 0,
            last_data_received: None,
            remote_extension_ids: HashMap::new(),
            our_extension_ids: HashMap::new(),
            last_pex_sent: None,
            last_pex_received: None,
            max_requests: PIPELINE_SIZE,
            client_version: None,
            metadata_size: None,
        }
    }

    /// Whether this peer can accept a new request.
    pub(super) fn can_request(&self) -> bool {
        if self.am_choked {
            return false;
        }
        let active = self.pipeline.iter().filter(|s| s.is_some()).count();
        active < self.max_requests
    }

    /// Record a new outstanding request.
    pub(super) fn push_request(&mut self, index: u32, begin: u32) {
        if let Some(slot) = self.pipeline.iter_mut().find(|s| s.is_none()) {
            *slot = Some((index, begin, Instant::now()));
        }
    }

    /// Remove a specific request (piece arrived or cancelled).
    pub(super) fn remove_request(&mut self, index: u32, begin: u32) {
        for slot in &mut self.pipeline {
            if let Some((i, b, _)) = *slot {
                if i == index && b == begin {
                    *slot = None;
                    return;
                }
            }
        }
    }
}

/// An in-progress piece download, assembling blocks from peers.
pub(crate) struct ActiveDownload {
    /// Piece index being downloaded.
    #[allow(dead_code)]
    pub(super) index: u32,
    /// Full piece data buffer (allocated upfront, piece_length bytes).
    pub(super) data: Vec<u8>,
    /// Which peer is currently assigned to each block. `None` = unrequested.
    pub(super) requested: Vec<Option<SocketAddr>>,
    /// Which blocks have been received (one bool per block).
    pub(super) received: Vec<bool>,
    /// Block size in bytes (default 16 KB).
    pub(super) block_size: u32,
    /// Number of blocks per piece.
    #[allow(dead_code)]
    pub(super) num_blocks: usize,
}

impl ActiveDownload {
    pub(super) fn new(index: u32, piece_len: u64, block_size: u32) -> Self {
        let num_blocks = piece_len.div_ceil(block_size as u64) as usize;
        ActiveDownload {
            index,
            data: vec![0u8; piece_len as usize],
            received: vec![false; num_blocks],
            requested: vec![None; num_blocks],
            block_size,
            num_blocks,
        }
    }

    /// Find the first unrequested block.
    pub(super) fn next_unrequested(&self) -> Option<u32> {
        self.requested
            .iter()
            .position(Option::is_none)
            .map(|i| i as u32 * self.block_size)
    }

    /// Length of the block at `begin` offset.
    pub(super) fn block_len(&self, begin: u32) -> u32 {
        let piece_len = self.data.len() as u64;
        let remaining = piece_len.saturating_sub(begin as u64);
        remaining.min(self.block_size as u64) as u32
    }

    /// Mark a block as requested by a peer.
    ///
    /// In normal mode this is called with blocks that were confirmed
    /// unrequested. In EndGame mode it may overwrite a previous assignment
    /// (duplicate requests to multiple peers).
    pub(super) fn mark_requested(&mut self, begin: u32, addr: SocketAddr) {
        let block_idx = (begin / self.block_size) as usize;
        if block_idx < self.requested.len() {
            self.requested[block_idx] = Some(addr);
        }
    }

    /// Mark a block as received. Returns `true` if this completes the piece.
    pub(super) fn mark_received(&mut self, begin: u32, data: &[u8]) -> bool {
        let block_idx = (begin / self.block_size) as usize;
        if block_idx < self.received.len() && !self.received[block_idx] {
            let start = begin as usize;
            let end = start + data.len();
            if end <= self.data.len() {
                self.data[start..end].copy_from_slice(data);
            }
            self.received[block_idx] = true;
        }
        self.received.iter().all(|&r| r)
    }
}

/// Parse bitfield bytes into a `Vec<bool>`.
pub(super) fn parse_bitfield(bytes: &[u8], num_pieces: usize) -> Vec<bool> {
    let mut bf = vec![false; num_pieces];
    for (i, have) in bf.iter_mut().enumerate() {
        let byte = i / 8;
        let bit = 7 - (i % 8);
        if byte < bytes.len() {
            *have = (bytes[byte] & (1 << bit)) != 0;
        }
    }
    bf
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn peer_info_default_state() {
        let pi = PeerInfo::new();
        assert!(pi.am_choked);
        assert!(!pi.am_interested);
        assert!(!pi.peer_interested);
        assert!(pi.bitfield.is_empty());
        assert_eq!(pi.uploaded_bytes, 0);
        assert_eq!(pi.downloaded_bytes, 0);
    }

    #[test]
    fn active_download_has_expected_fields() {
        let dl = ActiveDownload {
            index: 42,
            data: vec![0u8; 16000],
            received: vec![false; 1],
            requested: vec![None; 1],
            block_size: 16384,
            num_blocks: 1,
        };
        assert_eq!(dl.index, 42);
        assert_eq!(dl.num_blocks, 1);
        assert_eq!(dl.block_size, 16384);
        assert_eq!(dl.data.len(), 16000);
        assert_eq!(dl.received.len(), 1);
        assert_eq!(dl.requested.len(), 1);
        assert!(dl.requested[0].is_none());
    }

    #[test]
    fn peer_pipeline_push_and_remove() {
        let mut pi = PeerInfo::new();
        pi.am_choked = false;
        assert!(pi.can_request());

        pi.push_request(0, 0);
        pi.push_request(0, 16384);
        pi.push_request(1, 0);
        assert_eq!(pi.pipeline.iter().filter(|s| s.is_some()).count(), 3);

        pi.remove_request(0, 16384);
        assert_eq!(pi.pipeline.iter().filter(|s| s.is_some()).count(), 2);

        // Removing non-existent request is a no-op
        pi.remove_request(99, 999);
        assert_eq!(pi.pipeline.iter().filter(|s| s.is_some()).count(), 2);
    }

    #[test]
    fn peer_pipeline_full_cant_request() {
        let mut pi = PeerInfo::new();
        pi.am_choked = false;

        // Fill all 5 slots
        for i in 0..5 {
            pi.push_request(i, 0);
        }
        assert!(!pi.can_request());

        // 6th push should be silently ignored (no free slot)
        pi.push_request(5, 0);
        assert!(!pi.can_request());
    }

    #[test]
    fn peer_pipeline_choked_cant_request() {
        let pi = PeerInfo::new();
        // am_choked defaults to true
        assert!(!pi.can_request());
    }

    #[test]
    fn active_download_new_has_all_unrequested() {
        let dl = ActiveDownload::new(0, 65536, 16384); // 4 blocks
        assert_eq!(dl.num_blocks, 4);
        assert_eq!(dl.next_unrequested(), Some(0));
        assert!(dl.received.iter().all(|&r| !r));
        assert!(dl.requested.iter().all(Option::is_none));
    }

    #[test]
    fn active_download_mark_requested_skips() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 6881));
        let mut dl = ActiveDownload::new(0, 65536, 16384); // 4 blocks

        dl.mark_requested(0, addr);
        assert_eq!(dl.next_unrequested(), Some(16384)); // skips block 0

        dl.mark_requested(16384, addr);
        assert_eq!(dl.next_unrequested(), Some(32768)); // skips block 0,1
    }

    #[test]
    fn active_download_mark_received_complete() {
        let _ = SocketAddr::from(([127, 0, 0, 1], 6881));
        let mut dl = ActiveDownload::new(0, 32768, 16384); // 2 blocks

        assert!(!dl.mark_received(0, &[0x42u8; 16384]));
        assert!(!dl.received[1]);
        assert!(dl.mark_received(16384, &[0xFFu8; 16384]));
        assert!(dl.received.iter().all(|&r| r));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bitfield_all_set() {
        let bytes = vec![0xFF, 0xFF];
        let bf = parse_bitfield(&bytes, 16);
        assert_eq!(bf.len(), 16);
        assert!(bf.iter().all(|&b| b));
    }

    #[test]
    fn parse_bitfield_none_set() {
        let bytes = vec![0x00, 0x00];
        let bf = parse_bitfield(&bytes, 16);
        assert_eq!(bf.len(), 16);
        assert!(bf.iter().all(|&b| !b));
    }

    #[test]
    fn parse_bitfield_partial() {
        let bytes = vec![0x80, 0x00];
        let bf = parse_bitfield(&bytes, 16);
        assert_eq!(bf.len(), 16);
        assert!(bf[0]);
        assert!(!bf[1]);
        assert!(!bf[7]);
        assert!(!bf[8]);
    }

    #[test]
    fn parse_bitfield_shorter_than_requested() {
        let bytes = vec![0xFF];
        let bf = parse_bitfield(&bytes, 16);
        assert_eq!(bf.len(), 16);
        assert!(bf[0..8].iter().all(|&b| b));
        assert!(bf[8..16].iter().all(|&b| !b));
    }
}
