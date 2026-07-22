use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{Duration, Instant};

use crate::bencode::{decode as bencode_decode, encode as bencode_encode};
use crate::error::{Error, ErrorKind};
use crate::peer::pex::PexMessage;
use crate::peer::{ExtensionNegotiation, PeerMessage};

use super::extension::{SwarmContext, SwarmExtension};
use super::types::{PIPELINE_SIZE, UT_PEX, UT_PEX_ID};

/// Peer Exchange extension (BEP 11).
///
/// Implements [`SwarmExtension`] so it hooks into the swarm event loop
/// without modifying the core `select!` branches.  Owns all PEX-specific
/// state (broadcast interval, recently-disconnected peer list).
pub(crate) struct PexExtension {
    interval: Duration,
    recently_dropped: Vec<SocketAddr>,
}

impl PexExtension {
    /// Create a PEX extension that broadcasts every `interval`.
    pub fn new(interval: Duration) -> Self {
        PexExtension {
            interval,
            recently_dropped: Vec::new(),
        }
    }

    /// Broadcast PEX messages to all PEX-capable connected peers.
    async fn broadcast(&mut self, ctx: &mut SwarmContext<'_>) -> Result<(), Error> {
        let dropped_snapshot: Vec<SocketAddr> = std::mem::take(&mut self.recently_dropped);
        let addresses: Vec<SocketAddr> = ctx
            .peers
            .iter()
            .filter(|(_, info)| info.remote_extension_ids.contains_key(UT_PEX))
            .map(|(addr, _)| *addr)
            .collect();
        for addr in addresses {
            let dropped: Vec<SocketAddr> = dropped_snapshot
                .iter()
                .filter(|a| **a != addr)
                .copied()
                .collect();
            if let Err(e) = self.send_one(&addr, &dropped, ctx).await {
                tracing::warn!("failed to send PEX to {}: {}", addr, e);
            }
        }
        Ok(())
    }

    /// Send a PEX message to a single peer.
    async fn send_one(
        &self, addr: &SocketAddr, dropped: &[SocketAddr], ctx: &mut SwarmContext<'_>,
    ) -> Result<(), Error> {
        let peer = match ctx.peers.get(addr) {
            Some(p) => p,
            None => return Ok(()),
        };
        let pex_id = match peer.remote_extension_ids.get(UT_PEX) {
            Some(&id) => id,
            None => return Ok(()),
        };
        if peer
            .last_pex_sent
            .is_some_and(|t| t.elapsed() < self.interval)
        {
            return Ok(());
        }

        let connected = ctx.peer_mgr.read().await.connection_addrs();
        let (added, added6): (Vec<_>, Vec<_>) = connected
            .into_iter()
            .filter(|a| a != addr)
            .partition(|a| a.is_ipv4());
        let added: Vec<SocketAddr> = added.into_iter().take(50).collect();
        let added6: Vec<SocketAddr> = added6.into_iter().take(50).collect();

        let (dropped_v4, dropped_v6): (Vec<_>, Vec<_>) = dropped.iter().partition(|a| a.is_ipv4());
        let dropped: Vec<SocketAddr> = dropped_v4.into_iter().copied().collect();
        let dropped6: Vec<SocketAddr> = dropped_v6.into_iter().copied().collect();

        let mut pex_msg = PexMessage::new();
        pex_msg.added = added;
        pex_msg.added6 = added6;
        pex_msg.dropped = dropped;
        pex_msg.dropped6 = dropped6;

        let payload = bencode_encode(&pex_msg.to_bencode());
        ctx.peer_mgr
            .read()
            .await
            .send_to(
                addr,
                &PeerMessage::Extended {
                    ext_id: pex_id,
                    data: payload,
                },
            )
            .await?;

        if let Some(peer) = ctx.peers.get_mut(addr) {
            peer.last_pex_sent = Some(Instant::now());
        }

        Ok(())
    }

    /// Handle an incoming LTEP extension negotiation handshake from a peer.
    /// If the remote supports PEX, send an initial PEX message now that
    /// we know their extension IDs.
    async fn handle_ltep_handshake(
        &self, addr: SocketAddr, data: &[u8], ctx: &mut SwarmContext<'_>,
    ) -> Result<(), Error> {
        let (val, _) = bencode_decode(data).map_err(|e| {
            tracing::warn!("invalid LTEP bencode from {}: {}", addr, e);
            Error::new(ErrorKind::PeerInvalidExtendedMessage)
        })?;
        let neg = ExtensionNegotiation::from_bencode(&val).map_err(|e| {
            tracing::warn!("invalid LTEP dict from {}: {}", addr, e);
            Error::new(ErrorKind::PeerInvalidExtendedMessage)
        })?;

        let peer = match ctx.peers.get_mut(&addr) {
            Some(p) => p,
            None => return Ok(()),
        };

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
        self.send_one(&addr, &[], ctx).await
    }

    /// Handle an incoming PEX message from a peer.
    async fn handle_incoming_pex(
        addr: SocketAddr, data: &[u8], ctx: &mut SwarmContext<'_>,
    ) -> Result<(), Error> {
        let (val, _) =
            bencode_decode(data).map_err(|_| Error::new(ErrorKind::PeerInvalidPexMessage))?;
        let pex_msg = PexMessage::from_bencode(&val)?;

        let added_count = pex_msg.added.len();
        let dropped_count = pex_msg.dropped.len();
        let added6_count = pex_msg.added6.len();
        let dropped6_count = pex_msg.dropped6.len();

        // Add newly discovered peers (IPv4 and IPv6).
        let mut pm = ctx.peer_mgr.write().await;
        if !pex_msg.added.is_empty() {
            pm.add_peers(pex_msg.added);
        }
        if !pex_msg.added6.is_empty() {
            pm.add_peers(pex_msg.added6);
        }

        if let Some(peer) = ctx.peers.get_mut(&addr) {
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

        Ok(())
    }
}

impl SwarmExtension for PexExtension {
    fn on_tick<'a>(
        &'a mut self, ctx: &'a mut SwarmContext<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move { self.broadcast(ctx).await })
    }

    fn on_peer_event<'a>(
        &'a mut self, addr: SocketAddr, event: &'a super::types::PeerEvent,
        ctx: &'a mut SwarmContext<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            match event {
                super::types::PeerEvent::Disconnected => {
                    self.recently_dropped.push(addr);
                }
                super::types::PeerEvent::Message(msg) => match msg {
                    // BEP 10: LTEP handshake — parse remote extension IDs,
                    // then send initial PEX if the remote supports it.
                    PeerMessage::Extended { ext_id: 0, data } => {
                        if let Err(e) = self.handle_ltep_handshake(addr, data, ctx).await {
                            tracing::warn!("failed to send initial PEX to {}: {}", addr, e);
                        }
                    }
                    // Incoming PEX message from a peer.
                    PeerMessage::Extended { ext_id, data }
                        if ctx
                            .peers
                            .get(&addr)
                            .is_some_and(|p| p.our_extension_ids.get(UT_PEX) == Some(ext_id)) =>
                    {
                        if let Err(e) = Self::handle_incoming_pex(addr, data, ctx).await {
                            tracing::warn!("failed to handle PEX from {}: {}", addr, e);
                        }
                    }
                    _ => {}
                },
            }
            Ok(())
        })
    }

    fn ltep_extensions(&self) -> HashMap<String, u8> {
        let mut m = HashMap::new();
        m.insert(UT_PEX.to_string(), UT_PEX_ID);
        m
    }
}
