//! Extension trait and shared context for composable swarm modules.
//!
//! Each optional BitTorrent protocol extension (PEX, super seed, web seed,
//! uTP) implements [`SwarmExtension`] and is registered into a
//! [`SwarmBuilder`].  The core event loop dispatches tick and peer events
//! to all registered extensions sequentially — extensions access shared
//! swarm state through [`SwarmContext`].
//!
//! # Design
//!
//! The trait uses manual `Pin<Box<dyn Future>>` return types so it
//! remains object-safe for `dyn SwarmExtension` dispatch without
//! requiring the `async-trait` crate.  Each method has a default
//! implementation that returns `Ready(Ok(()))`, so extensions only
//! override the hooks they need.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use tokio::sync::{RwLock, broadcast};

use crate::error::Error;
use crate::metainfo::Metainfo;
use crate::peer::PeerId;

use crate::piece::PieceManager;
use crate::session::peer_mgr::PeerManager;
use crate::session::{InfoHash, TorrentEvent, TorrentStatus};
use crate::storage::Storage;

use super::types::{PeerEvent, PeerInfo};

/// Shared swarm state accessible to extensions.
///
/// Fields behind [`RwLock`] use internal mutability — extensions call
/// `.read().await` / `.write().await` directly.  The `peers` map is
/// read-only for extensions because peer state mutations (insertion,
/// pipeline changes) are handled by the core loop.
#[allow(dead_code)] // wired into SwarmLoop in Phase 2+
pub(crate) struct SwarmContext<'a> {
    /// Torrent info hash.
    pub info_hash: InfoHash,
    /// Parsed torrent metadata.
    pub metainfo: &'a Metainfo,
    /// Storage backend for reading/writing pieces.
    pub storage: &'a dyn Storage,
    /// Sender for broadcasting [`TorrentEvent`]s to external consumers.
    pub event_tx: &'a broadcast::Sender<TorrentEvent>,
    /// TCP listen port announced to trackers.
    pub listen_port: u16,
    /// Our randomly-generated peer ID.
    pub peer_id: PeerId,
    /// Piece manager (bitfield, progress tracking).
    pub piece_mgr: &'a RwLock<PieceManager>,
    /// Peer connection pool and message dispatch.
    pub peer_mgr: &'a RwLock<PeerManager>,
    /// Torrent status snapshot (updated each status tick).
    pub status: &'a RwLock<TorrentStatus>,
    /// Map of connected peers → protocol state (read-only for extensions).
    pub peers: &'a HashMap<SocketAddr, PeerInfo>,
}

/// An optional module that hooks into the swarm event loop.
///
/// Implement this trait to add a composable protocol extension
/// (e.g. PEX, super seed, web seed).  Register implementations via
/// [`SwarmBuilder::extension`] — the core loop dispatches tick and
/// peer events to all registered extensions.
///
/// Each method returns a boxed future so the trait is object-safe
/// for `dyn SwarmExtension`.  Use `Box::pin(async move { ... })`
/// in your implementation.
///
/// # Defaults
///
/// All three hooks default to `Ready(Ok(()))`.  Override only the
/// hooks your extension needs.
#[allow(dead_code)] // wired into SwarmLoop in Phase 2+
pub(crate) trait SwarmExtension: Send {
    /// Called once when the swarm loop starts (before the first tick).
    fn on_start<'a>(
        &'a mut self, _ctx: &'a mut SwarmContext<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(())))
    }

    /// Called every status tick (~1 Hz).
    fn on_tick<'a>(
        &'a mut self, _ctx: &'a mut SwarmContext<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(())))
    }

    /// Called when a peer event arrives (message or disconnect).
    fn on_peer_event<'a>(
        &'a mut self, _addr: SocketAddr, _event: &'a PeerEvent, _ctx: &'a mut SwarmContext<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

/// Builder for assembling a [`SwarmLoop`] with optional extensions.
///
/// Add extensions via [`extension`](Self::extension), then call `build`
/// to construct the loop.  Convenience methods (e.g. `with_pex`) will
/// be added as extensions are extracted from `SwarmLoop`.
#[allow(dead_code)] // wired into SwarmLoop in Phase 2+
pub(crate) struct SwarmBuilder {
    extensions: Vec<Box<dyn SwarmExtension>>,
}

#[allow(dead_code)] // used in Phase 2+
impl SwarmBuilder {
    /// Create a new builder with no extensions.
    pub fn new() -> Self {
        SwarmBuilder {
            extensions: Vec::new(),
        }
    }

    /// Register an extension.  Extensions are dispatched in insertion order.
    pub fn extension(mut self, ext: impl SwarmExtension + 'static) -> Self {
        self.extensions.push(Box::new(ext));
        self
    }
}

impl Default for SwarmBuilder {
    fn default() -> Self {
        Self::new()
    }
}
