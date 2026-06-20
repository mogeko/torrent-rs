use std::net::SocketAddr;
use std::time::Instant;

use crate::bencode::{decode as bencode_decode, encode as bencode_encode};
use crate::error::{Error, ErrorKind};
use crate::peer::{ExtensionNegotiation, PeerMessage, PexMessage};

use super::DownloadLoop;
use super::types::{PIPELINE_SIZE, UT_PEX};

impl DownloadLoop {
    /// Handle the remote peer's BEP 10 LTEP extension negotiation handshake.
    ///
    /// Parses the remote's [`ExtensionNegotiation`] dictionary, stores the
    /// extension name → message ID mapping, and sends an initial PEX message
    /// now that we know the remote's extension IDs.
    pub(super) async fn handle_ltep_handshake(
        &mut self, addr: SocketAddr, data: &[u8],
    ) -> Result<(), Error> {
        let peer = match self.peers.get_mut(&addr) {
            Some(p) => p,
            None => return Ok(()),
        };

        let (val, _) = bencode_decode(data).map_err(|e| {
            tracing::warn!("invalid LTEP bencode from {}: {}", addr, e);
            Error::new(ErrorKind::PeerInvalidExtendedMessage)
        })?;
        let neg = ExtensionNegotiation::from_bencode(&val).map_err(|e| {
            tracing::warn!("invalid LTEP dict from {}: {}", addr, e);
            Error::new(ErrorKind::PeerInvalidExtendedMessage)
        })?;

        // ID=0 entries are already filtered by from_bencode (BEP 10).
        peer.remote_extension_ids = neg.m;

        // Persist remote metadata for diagnostics / future BEP 9 support.
        peer.client_version = neg.v;
        peer.metadata_size = neg.metadata_size;

        // Respect the remote's request queue limit (BEP 10 reqq).
        if let Some(reqq) = neg.reqq {
            let limit = usize::try_from(reqq).unwrap_or(PIPELINE_SIZE);
            peer.max_requests = limit.min(PIPELINE_SIZE);
        }

        tracing::debug!(
            "LTEP handshake from {}: {:?}",
            addr,
            peer.remote_extension_ids
        );

        // Now that we know the remote's extension IDs, send an initial PEX.
        if self.pex_enabled {
            if let Err(e) = self.send_pex_message(addr, &[]).await {
                tracing::warn!("failed to send initial PEX to {}: {}", addr, e);
            }
        }

        Ok(())
    }

    /// Dispatch an incoming extended message (BEP 10) to the appropriate handler.
    pub(super) async fn handle_extended_message(
        &mut self, addr: SocketAddr, ext_id: u8, data: Vec<u8>,
    ) -> Result<(), Error> {
        let peer = match self.peers.get(&addr) {
            Some(p) => p,
            None => return Ok(()),
        };

        // Check if this ext_id maps to ut_pex in our offered mapping.
        // The remote peer sends extended messages using the IDs we
        // advertised in our LTEP handshake (BEP 10).
        let is_pex = peer.our_extension_ids.get(UT_PEX) == Some(&ext_id);

        if is_pex {
            let (val, _) =
                bencode_decode(&data).map_err(|_| Error::new(ErrorKind::PeerInvalidPexMessage))?;
            let pex_msg = PexMessage::from_bencode(&val)?;

            let added_count = pex_msg.added.len();
            let dropped_count = pex_msg.dropped.len();
            let added6_count = pex_msg.added6.len();
            let dropped6_count = pex_msg.dropped6.len();

            // Add newly discovered peers (IPv4 and IPv6)
            let mut pm = self.peer_mgr.write().await;
            if !pex_msg.added.is_empty() {
                pm.add_peers(pex_msg.added);
            }
            if !pex_msg.added6.is_empty() {
                pm.add_peers(pex_msg.added6);
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
    pub(super) async fn send_pex_message(
        &mut self, addr: SocketAddr, dropped: &[SocketAddr],
    ) -> Result<(), Error> {
        let peer = match self.peers.get(&addr) {
            Some(p) => p,
            None => return Ok(()),
        };

        // Find the ut_pex extension ID
        let pex_id = match peer.remote_extension_ids.get(UT_PEX) {
            Some(&id) => id,
            None => return Ok(()), // Peer doesn't support PEX
        };

        // Gather currently connected peers (excluding this peer itself).
        // BEP 11: limit to 50 added peers per message.
        let connected = self.peer_mgr.read().await.connection_addrs();
        let added: Vec<SocketAddr> = connected
            .into_iter()
            .filter(|a| *a != addr)
            .take(50)
            .collect();

        let pex_msg = PexMessage {
            added,
            dropped: dropped.to_vec(),
            added6: Vec::new(),
            dropped6: Vec::new(),
        };

        let payload = bencode_encode(&pex_msg.to_bencode());
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
        let dropped_snapshot: Vec<SocketAddr> = self.recently_dropped.drain(..).collect();
        let addresses: Vec<SocketAddr> = self.peers.keys().copied().collect();
        for addr in addresses {
            let dropped = dropped_snapshot.iter().filter(|a| **a != addr).copied();
            let dropped: Vec<SocketAddr> = dropped.collect();
            if let Err(e) = self.send_pex_message(addr, &dropped).await {
                tracing::warn!("failed to send PEX to {}: {}", addr, e);
            }
        }
        Ok(())
    }
}
