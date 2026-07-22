use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use crate::error::Error;
use crate::peer::PeerMessage;

use super::extension::{SwarmContext, SwarmExtension};

/// Super seeding extension (BEP 16).
///
/// When enabled, pieces are uploaded to one peer at a time to minimize
/// redundant uploads during initial seeding.  Implements [`SwarmExtension`]
/// so the core loop does not need to know about super seed internals.
pub(crate) struct SuperSeedExtension {
    /// Piece → peer assignments.  Each key is a piece index being
    /// exclusively uploaded to the given peer.
    assignments: HashMap<u32, SocketAddr>,
    /// Unrevealed piece indices.  These pieces have been uploaded to
    /// the assigned peer but not yet confirmed (no HAVE received).
    unrevealed: HashSet<u32>,
}

impl SuperSeedExtension {
    pub fn new() -> Self {
        SuperSeedExtension {
            assignments: HashMap::new(),
            unrevealed: HashSet::new(),
        }
    }

    /// Select a piece for super seeding and assign it to a peer (BEP 16).
    ///
    /// Picks a piece that no peer in the swarm has (`availability == 0`),
    /// then assigns it to a single unchoked, interested peer for exclusive
    /// upload.  The piece remains unrevealed until that peer sends HAVE.
    async fn select_piece(&mut self, ctx: &mut SwarmContext<'_>) {
        let num_pieces = ctx.metainfo.info.num_pieces();

        // Compute per-piece availability across the swarm.
        let mut availability = vec![0usize; num_pieces];
        for peer in ctx.peers.values() {
            if peer.bitfield.is_empty() {
                continue;
            }
            for (i, &has) in peer.bitfield.iter().enumerate() {
                if i >= num_pieces {
                    break;
                }
                if has {
                    availability[i] += 1;
                }
            }
        }

        let our_bf = {
            let pm = ctx.piece_mgr.read().await;
            pm.bitfield().to_vec()
        };

        // Find a piece that: we have, no peer has, and not already assigned.
        for (i, &has) in our_bf.iter().enumerate() {
            let idx = i as u32;
            if !has {
                continue;
            }
            if availability[i] > 0 {
                continue; // some peer already has it
            }
            if self.assignments.contains_key(&idx) || self.unrevealed.contains(&idx) {
                continue; // already assigned or in progress
            }

            // Pick a target: an unchoked, interested peer not already assigned.
            let assigned_addrs: HashSet<SocketAddr> = self.assignments.values().copied().collect();
            let target = ctx
                .peers
                .iter()
                .find(|(addr, p)| {
                    p.peer_interested && !p.am_choked && !assigned_addrs.contains(addr)
                })
                .map(|(addr, _)| *addr);

            if let Some(addr) = target {
                self.assignments.insert(idx, addr);
                self.unrevealed.insert(idx);
                tracing::debug!("super seed: assigned piece {} to peer {}", idx, addr);
            }
            // Only assign one piece per call to avoid flooding.
            break;
        }
    }
}

impl SwarmExtension for SuperSeedExtension {
    fn on_tick<'a>(
        &'a mut self, ctx: &'a mut SwarmContext<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            // Only act when we are seeding.
            let is_seeding = ctx.piece_mgr.read().await.missing_pieces().is_empty();
            if is_seeding {
                self.select_piece(ctx).await;
            }
            // Update status fields.
            {
                let mut status = ctx.status.write().await;
                status.super_seed_active = true;
                status.super_seed_remaining = self.unrevealed.len() as u32;
            }
            Ok(())
        })
    }

    fn on_peer_event<'a>(
        &'a mut self, addr: SocketAddr, event: &'a super::types::PeerEvent,
        ctx: &'a mut SwarmContext<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            match event {
                super::types::PeerEvent::Disconnected => {
                    // Release super seed assignments for the disconnected peer.
                    let orphaned: Vec<u32> = self
                        .assignments
                        .iter()
                        .filter(|(_, a)| **a == addr)
                        .map(|(&i, _)| i)
                        .collect();
                    for idx in orphaned {
                        self.unrevealed.remove(&idx);
                        self.assignments.remove(&idx);
                    }
                }
                super::types::PeerEvent::Message(msg) => match msg {
                    // BEP 16: only serve unrevealed pieces to the assigned peer.
                    PeerMessage::Request { index, .. } => {
                        if self.unrevealed.contains(index)
                            && self.assignments.get(index) != Some(&addr)
                        {
                            ctx.blocked_requests.insert((*index, addr));
                        }
                    }
                    // BEP 16: if the assigned peer confirms they have
                    // the piece, reveal it to the entire swarm.
                    PeerMessage::Have(index) if self.assignments.get(index) == Some(&addr) => {
                        self.unrevealed.remove(index);
                        self.assignments.remove(index);
                        tracing::debug!(
                            "super seed: peer {} confirmed piece {}, revealing",
                            addr,
                            index,
                        );
                        ctx.confirmed_haves.push(*index);
                    }
                    _ => {}
                },
            }
            Ok(())
        })
    }

    fn mask_bitfield(&self, bf: &mut Vec<u8>) {
        for &idx in &self.unrevealed {
            let byte = idx as usize / 8;
            let bit = 7 - (idx as usize % 8);
            if byte < bf.len() {
                bf[byte] &= !(1 << bit);
            }
        }
    }
}
