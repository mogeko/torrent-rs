use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::Error;
use crate::peer::{PeerConnection, PeerMessage};
use crate::storage::Storage;

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
                    p.downloaded_bytes += data.len() as u64;
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

                let piece_data = if let Some(cached) = self.piece_cache.get(&index) {
                    Arc::clone(cached)
                } else {
                    let piece_len = self.piece_len_for_index(index) as usize;
                    let mut piece_buf = vec![0u8; piece_len];
                    self.storage.read_piece(index, &mut piece_buf).await?;
                    Arc::new(piece_buf)
                };

                let start = begin as usize;
                let end = (start + length as usize).min(piece_data.len());
                if start < end {
                    let block_data = piece_data[start..end].to_vec();
                    let msg = PeerMessage::Piece {
                        index,
                        begin,
                        data: block_data,
                    };
                    self.peer_mgr.read().await.send_to(&addr, &msg).await?;
                    self.total_uploaded += (end - start) as u64;
                    if let Some(p) = self.peers.get_mut(&addr) {
                        p.uploaded_bytes += (end - start) as u64;
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
        }

        Ok(())
    }

    /// Spawn a tokio task that loops `recv()` on a peer connection
    /// and sends messages to the download loop via the channel.
    pub(super) fn spawn_peer_reader(&self, addr: SocketAddr, conn_arc: Arc<Mutex<PeerConnection>>) {
        let tx = self.peer_msg_tx.clone();
        tokio::spawn(async move {
            loop {
                let msg_result = {
                    let mut conn = conn_arc.lock().await;
                    conn.recv().await
                };
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
        if !bf_bytes.is_empty() {
            pm.send_to(&addr, &PeerMessage::Bitfield(bf_bytes)).await?;
        }
        pm.send_to(&addr, &PeerMessage::Interested).await?;
        Ok(())
    }
}
