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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

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
pub(crate) struct SwarmContext<'a> {
    /// Torrent info hash.
    #[allow(dead_code)] // used by future extensions (web seed, super seed)
    pub info_hash: InfoHash,
    /// Parsed torrent metadata.
    #[allow(dead_code)] // used by future extensions (web seed, super seed)
    pub metainfo: &'a Metainfo,
    /// Storage backend for reading/writing pieces.
    pub storage: Arc<dyn Storage>,
    /// Sender for broadcasting [`TorrentEvent`]s to external consumers.
    #[allow(dead_code)] // used by future extensions (super seed)
    pub event_tx: &'a broadcast::Sender<TorrentEvent>,
    /// TCP listen port announced to trackers.
    #[allow(dead_code)] // used by future extensions
    pub listen_port: u16,
    /// Our randomly-generated peer ID.
    #[allow(dead_code)] // used by future extensions
    pub peer_id: PeerId,
    /// Piece manager (bitfield, progress tracking).
    pub piece_mgr: Arc<RwLock<PieceManager>>,
    /// Peer connection pool and message dispatch.
    pub peer_mgr: &'a RwLock<PeerManager>,
    /// Torrent status snapshot (updated each status tick).
    #[allow(dead_code)] // used by future extensions (super seed)
    pub status: &'a RwLock<TorrentStatus>,
    /// Map of connected peers → protocol state.
    ///
    /// Extensions may mutate peer state (e.g. updating `last_pex_sent` or
    /// `remote_extension_ids`).  Mutations are safe because extensions run
    /// sequentially within the core loop.
    pub peers: &'a mut HashMap<SocketAddr, PeerInfo>,
    /// Requests blocked by extensions (e.g. super seed gating).
    /// Extensions insert `(piece_index, peer_addr)` tuples; the core
    /// skips sending data for blocked requests.
    pub blocked_requests: &'a mut HashSet<(u32, SocketAddr)>,
    /// Pieces whose HAVE was confirmed by an extension (e.g. super
    /// seed reveal).  The core calls `broadcast_have` for each entry
    /// after all extensions have processed the event.
    pub confirmed_haves: &'a mut Vec<u32>,
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
pub(crate) trait SwarmExtension: Send + Sync {
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

    /// Return the set of BEP 10 LTEP extension name → message ID mappings
    /// to advertise in our extension negotiation handshake.
    ///
    /// Called by the core loop during `register_new_peer` to populate
    /// `PeerInfo::our_extension_ids`.  The default returns an empty map.
    fn ltep_extensions(&self) -> HashMap<String, u8> {
        HashMap::new()
    }

    /// Mask the bitfield before it is sent to a new peer.
    ///
    /// Extensions may clear bits to hide pieces from the remote peer
    /// (e.g. super seed hides unrevealed pieces per BEP 16).  The
    /// default implementation is a no-op.
    fn mask_bitfield(&self, _bf: &mut Vec<u8>) {}
}

/// Builder for assembling a [`SwarmLoop`] with optional extensions.
///
/// Use convenience methods to add protocol extensions, or call
/// [`extension`](Self::extension) directly for custom extensions.
///
/// # Example
///
/// ```ignore
/// let exts = SwarmBuilder::new()
///     .with_pex(config.pex_interval)
///     .maybe_super_seed(true)
///     .with_webseed(urls, config, concurrency)
///     .build();
/// ```
pub(crate) struct SwarmBuilder {
    extensions: Vec<Box<dyn SwarmExtension>>,
}

impl SwarmBuilder {
    /// Create a new builder with no extensions.
    pub fn new() -> Self {
        SwarmBuilder {
            extensions: Vec::new(),
        }
    }

    /// Register an arbitrary extension.  Extensions are dispatched
    /// in insertion order.
    #[allow(dead_code)] // public API for custom extensions
    pub fn extension(mut self, ext: impl SwarmExtension + 'static) -> Self {
        self.extensions.push(Box::new(ext));
        self
    }

    /// Add the PEX (Peer Exchange, BEP 11) extension conditionally.
    ///
    /// When `enabled` is false this is a no-op.
    pub fn maybe_pex(mut self, enabled: bool, interval: std::time::Duration) -> Self {
        if enabled {
            self.extensions
                .push(Box::new(super::pex::PexExtension::new(interval)));
        }
        self
    }

    /// Add the super seed extension (BEP 16) conditionally.
    ///
    /// When `enabled` is false this is a no-op so callers don't need
    /// an `if` at the call site.
    pub fn maybe_super_seed(mut self, enabled: bool) -> Self {
        if enabled {
            self.extensions
                .push(Box::new(super::super_seed::SuperSeedExtension::new()));
        }
        self
    }

    /// Add the web seed download extension (BEP 19) conditionally.
    ///
    /// When `enabled` is false or `urls` is empty this is a no-op.
    pub fn maybe_webseed(
        mut self, enabled: bool, urls: Vec<String>, config: crate::session::webseed::WebSeedConfig,
        concurrency: usize,
    ) -> Self {
        if enabled && !urls.is_empty() {
            self.extensions
                .push(Box::new(crate::session::webseed::WebSeedExtension::new(
                    urls,
                    config,
                    concurrency,
                )));
        }
        self
    }

    /// Consume the builder and return the registered extensions.
    pub fn build(self) -> Vec<Box<dyn SwarmExtension>> {
        self.extensions
    }
}
