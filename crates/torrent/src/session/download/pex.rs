use std::net::SocketAddr;
use std::time::Instant;

use crate::error::{Error, ErrorKind};
use crate::peer::{PeerMessage, PexMessage};

use super::DownloadLoop;

impl DownloadLoop {
    /// Dispatch an incoming extended message (BEP 10) to the appropriate handler.
    pub(super) async fn handle_extended_message(
        &mut self, addr: SocketAddr, ext_id: u8, data: Vec<u8>,
    ) -> Result<(), Error> {
        let peer = match self.peers.get(&addr) {
            Some(p) => p,
            None => return Ok(()),
        };

        // Check if this ext_id maps to ut_pex
        let is_pex = peer
            .extension_ids
            .iter()
            .any(|(name, &id)| name == "ut_pex" && id == ext_id);

        if is_pex {
            let (val, _) = crate::bencode::decode(&data)
                .map_err(|_| Error::new(ErrorKind::PeerInvalidPexMessage))?;
            let pex_msg = PexMessage::from_bencode(&val)?;

            let added_count = pex_msg.added.len();
            let dropped_count = pex_msg.dropped.len();
            let added6_count = pex_msg.added6.len();
            let dropped6_count = pex_msg.dropped6.len();

            // Add newly discovered peers
            if !pex_msg.added.is_empty() {
                self.peer_mgr.write().await.add_peers(pex_msg.added);
            }

            // Update peer state
            if let Some(peer) = self.peers.get_mut(&addr) {
                peer.last_pex_received = Some(Instant::now());
            }

            tracing::debug!(
                "received PEX from {}: +{}/-{} (IPv4), +{}/-{} (IPv6)",
                addr,
                added_count,
                dropped_count,
                added6_count,
                dropped6_count,
            );
        }

        Ok(())
    }

    /// Send a PEX message to a specific peer with our currently known peers.
    pub(super) async fn send_pex_message(&mut self, addr: SocketAddr) -> Result<(), Error> {
        let peer = match self.peers.get(&addr) {
            Some(p) => p,
            None => return Ok(()),
        };

        // Find the ut_pex extension ID
        let pex_id = match peer.extension_ids.get("ut_pex") {
            Some(&id) => id,
            None => return Ok(()), // Peer doesn't support PEX
        };

        // Gather all currently connected peers (excluding this peer itself)
        let connected = self.peer_mgr.read().await.connection_addrs();
        let added: Vec<SocketAddr> = connected.into_iter().filter(|a| *a != addr).collect();

        // Build PEX message
        let pex_msg = PexMessage {
            added,
            dropped: Vec::new(),
            added6: Vec::new(),
            dropped6: Vec::new(),
        };

        let payload = crate::bencode::encode(&pex_msg.to_bencode());
        self.peer_mgr
            .read()
            .await
            .send_to(
                &addr,
                &PeerMessage::Extended {
                    ext_id: pex_id,
                    data: payload,
                },
            )
            .await?;

        if let Some(peer) = self.peers.get_mut(&addr) {
            peer.last_pex_sent = Some(Instant::now());
        }

        Ok(())
    }

    /// Broadcast PEX messages to all connected peers.
    pub(super) async fn broadcast_pex(&mut self) -> Result<(), Error> {
        let addresses: Vec<SocketAddr> = self.peers.keys().copied().collect();
        for addr in addresses {
            if let Err(e) = self.send_pex_message(addr).await {
                tracing::debug!("failed to send PEX to {}: {}", addr, e);
            }
        }
        Ok(())
    }
}
