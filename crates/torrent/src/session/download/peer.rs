use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use crate::error::Error;
use crate::peer::{PeerConnection, PeerMessage};

use super::DownloadLoop;
use super::types::{PeerEvent, parse_bitfield};

impl DownloadLoop {
    /// Handle an event from a peer reader task.
    pub(super) async fn handle_peer_event(&mut self, addr: SocketAddr, event: PeerEvent) {
        match event {
            PeerEvent::Disconnected => {
                // Clear this peer's pipeline, mark its blocks as unrequested
                if let Some(peer) = self.peers.get(&addr) {
                    for slot in &peer.pipeline {
                        if let Some((index, begin)) = slot.map(|(i, b, _)| (i, b)) {
                            if let Some(dl) = self.active_downloads.get_mut(&index) {
                                let block_idx = (begin / dl.block_size) as usize;
                                if block_idx < dl.requested.len() {
                                    dl.requested[block_idx] = None;
                                }
                            }
                        }
                    }
                }
                self.peers.remove(&addr);
                self.peer_mgr.write().await.remove_peer(&addr);
            }
            PeerEvent::Message(msg) => {
                if let Err(_e) = self.handle_peer_message(addr, msg).await {
                    // Clear pipeline for dead peer
                    if let Some(peer) = self.peers.get(&addr) {
                        for slot in &peer.pipeline {
                            if let Some((index, begin)) = slot.map(|(i, b, _)| (i, b)) {
                                if let Some(dl) = self.active_downloads.get_mut(&index) {
                                    let block_idx = (begin / dl.block_size) as usize;
                                    if block_idx < dl.requested.len() {
                                        dl.requested[block_idx] = None;
                                    }
                                }
                            }
                        }
                    }
                    self.peers.remove(&addr);
                    self.peer_mgr.write().await.remove_peer(&addr);
                }
            }
        }
    }

    /// Process a single peer wire protocol message.
    pub(super) async fn handle_peer_message(
        &mut self, addr: SocketAddr, msg: PeerMessage,
    ) -> Result<(), Error> {
        let peer = match self.peers.get_mut(&addr) {
            Some(p) => p,
            None => return Ok(()),
        };

        match msg {
            PeerMessage::KeepAlive => {}
            PeerMessage::Choke => {
                peer.am_choked = true;
                for slot in &mut peer.pipeline {
                    if let Some((index, begin, _)) = *slot {
                        if let Some(dl) = self.active_downloads.get_mut(&index) {
                            let block_idx = (begin / dl.block_size) as usize;
                            if block_idx < dl.requested.len() {
                                dl.requested[block_idx] = None;
                            }
                        }
                    }
                    *slot = None;
                }
            }
            PeerMessage::Unchoke => {
                peer.am_choked = false;
            }
            PeerMessage::Interested => {
                peer.peer_interested = true;
            }
            PeerMessage::NotInterested => {
                peer.peer_interested = false;
            }
            PeerMessage::Have(index) => {
                let idx = index as usize;
                if idx < peer.bitfield.len() {
                    peer.bitfield[idx] = true;
                }
            }
            PeerMessage::Bitfield(bytes) => {
                let num_pieces = self.metainfo.info.num_pieces();
                peer.bitfield = parse_bitfield(&bytes, num_pieces);
            }
            PeerMessage::Piece { index, begin, data } => {
                self.storage.write_block(index, begin, &data).await?;
                self.total_downloaded += data.len() as u64;
                if let Some(p) = self.peers.get_mut(&addr) {
                    let len = data.len() as u64;
                    p.downloaded_bytes += len;
                    p.downloaded_this_round += len;
                    p.last_data_received = Some(Instant::now());
                }

                if let Some(p) = self.peers.get_mut(&addr) {
                    p.remove_request(index, begin);
                }

                let piece_complete = if let Some(dl) = self.active_downloads.get_mut(&index) {
                    dl.mark_received(begin, &data)
                } else {
                    false
                };

                if piece_complete && self.verify_and_complete_piece(index).await? {
                    self.broadcast_have(index).await?;
                }
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                let is_unchoked = {
                    let um = self.upload_mgr.read().await;
                    um.is_unchoked(&addr)
                };
                if !is_unchoked {
                    return Ok(());
                }

                let block_data =
                    if let Some(cached) = self.piece_cache.iter().find(|(i, _)| *i == index) {
                        let start = begin as usize;
                        let end = (start + length as usize).min(cached.1.len());
                        cached.1[start..end].to_vec()
                    } else {
                        let mut block_buf = vec![0u8; length as usize];
                        self.storage
                            .read_block(index, begin, &mut block_buf)
                            .await?;
                        block_buf
                    };

                if !block_data.is_empty() {
                    let uploaded = block_data.len() as u64;
                    let msg = PeerMessage::Piece {
                        index,
                        begin,
                        data: block_data,
                    };
                    self.peer_mgr.read().await.send_to(&addr, &msg).await?;
                    self.total_uploaded += uploaded;
                    if let Some(p) = self.peers.get_mut(&addr) {
                        p.uploaded_bytes += uploaded;
                        p.uploaded_this_round += uploaded;
                    }
                }
            }
            PeerMessage::Cancel { index, begin, .. } => {
                if let Some(p) = self.peers.get_mut(&addr) {
                    p.remove_request(index, begin);
                }
                if let Some(dl) = self.active_downloads.get_mut(&index) {
                    let block_idx = (begin / dl.block_size) as usize;
                    if block_idx < dl.requested.len() {
                        dl.requested[block_idx] = None;
                    }
                }
            }
            PeerMessage::Port(_) => {}
            PeerMessage::Extended { ext_id, data } => {
                self.handle_extended_message(addr, ext_id, data).await?;
            }
        }

        Ok(())
    }

    /// Spawn a tokio task that loops `recv()` on a peer connection
    /// and sends messages to the download loop via the channel.
    pub(super) fn spawn_peer_reader(&self, addr: SocketAddr, conn_arc: Arc<PeerConnection>) {
        let tx = self.peer_msg_tx.clone();
        tokio::spawn(async move {
            loop {
                let msg_result = conn_arc.recv().await;
                match msg_result {
                    Ok(msg) => {
                        if tx.send((addr, PeerEvent::Message(msg))).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => {
                        let _ = tx.send((addr, PeerEvent::Disconnected)).await;
                        break;
                    }
                }
            }
        });
    }

    /// Send our bitfield and Interested to a newly connected peer.
    pub(super) async fn send_bitfield(&self, addr: SocketAddr) -> Result<(), Error> {
        let piece_mgr = self.piece_mgr.clone();
        let peer_mgr = self.peer_mgr.clone();

        let bf_bytes = {
            let pm = piece_mgr.read().await;
            pm.to_bitfield()
        };
        let pm = peer_mgr.read().await;
        // BEP 3: the bitfield message is optional and SHOULD NOT be sent
        // if the client has no pieces (all bits are zero).
        if bf_bytes.iter().any(|&b| b != 0) {
            pm.send_to(&addr, &PeerMessage::Bitfield(bf_bytes)).await?;
        }
        pm.send_to(&addr, &PeerMessage::Interested).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::PeerInfo;

    #[test]
    fn cancel_removes_from_pipeline() {
        let mut pi = PeerInfo::new();
        pi.am_choked = false;
        pi.push_request(7, 16384);
        assert!(pi.pipeline[0].is_some());

        pi.remove_request(7, 16384);
        assert!(pi.pipeline[0].is_none());
    }

    #[test]
    fn cancel_non_existent_is_noop() {
        let mut pi = PeerInfo::new();
        pi.am_choked = false;
        pi.push_request(7, 0);
        pi.remove_request(99, 999);
        // Still has the original request
        assert!(pi.pipeline[0].is_some());
    }

    #[test]
    fn peer_starts_choked() {
        let pi = PeerInfo::new();
        assert!(pi.am_choked);
        assert!(!pi.can_request());
    }
}
