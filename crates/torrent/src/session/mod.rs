//! High-level session management — orchestrates all BitTorrent modules.
//!
//! The [`Session`] is the main entry point. Use [`SessionConfig`] to configure
//! it, then add torrents with [`Session::add_torrent_bytes`] or
//! [`Session::add_torrent`]. Track progress via [`Session::torrent_status`].
//!
//! # Architecture
//!
//! Each added torrent spawns a [`tokio`] task that runs a download loop.
//! The loop periodically connects to peers, requests blocks, verifies
//! pieces using SHA-1, and updates the torrent status.

mod builder;
mod config;
mod download;
mod peer_manager;
mod torrent;
mod uni_deque;
mod upload;

pub use self::builder::TorrentBuilder;
pub use self::config::{InfoHash, SessionConfig, TorrentState, TorrentStatus};

use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::str::FromStr as _;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::dht::{DhtNode, generate_node_id};
use crate::error::{Error, ErrorKind};
use crate::magnet::{MagnetUri, hex_encode};
use crate::metainfo::{Metainfo, Mode};
use crate::spec::TorrentSpec;

use self::torrent::{TorrentCommand, TorrentHandle};

/// High-level session managing all torrent downloads/uploads.
///
/// This is the main entry point for the library. Create a [`Session`]
/// with [`SessionConfig`], then add torrents via
/// [`add_torrent`](Session::add_torrent) or
/// [`add_torrent_bytes`](Session::add_torrent_bytes).
///
/// # Examples
///
/// ```no_run
/// use torrent::session::{Session, SessionConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = SessionConfig::default();
/// let session = Session::new(config).await.unwrap();
///
/// // Add a torrent from a .torrent file, specifying a download directory
/// let data = std::fs::read("torrent.torrent").unwrap();
/// let info_hash = session.add_torrent_bytes(&data).unwrap()
///     .download_dir("./downloads")
///     .start().await.unwrap();
///
/// // Check its status
/// let status = session.torrent_status(&info_hash).await.unwrap();
/// println!("Progress: {:.1}%", status.progress * 100.0);
/// # Ok(())
/// # }
/// ```
pub struct Session {
    /// Session configuration.
    config: SessionConfig,
    /// Active torrents, keyed by info_hash.
    torrents: Arc<RwLock<HashMap<InfoHash, TorrentHandle>>>,
    /// Shared dual-stack DHT node (if DHT is enabled).
    #[expect(dead_code)]
    dht_node: Option<Arc<DhtNode>>,
}

impl Session {
    /// Create a new session with the given configuration.
    pub async fn new(config: SessionConfig) -> Result<Self, Error> {
        let torrents: Arc<RwLock<HashMap<InfoHash, TorrentHandle>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let node_id = config.node_id.unwrap_or_else(generate_node_id);

        // Initialize dual-stack DHT if any bootstrap nodes are configured
        let dht_node = {
            let bootstrap_nodes = config.bootstrap_nodes.as_deref().unwrap_or(&[]);
            let v4 = bootstrap_nodes
                .iter()
                .map(|n| (n.host.as_str(), n.port))
                .collect::<Vec<_>>();
            let bootstrap_nodes_v6 = config.bootstrap_nodes_v6.as_deref().unwrap_or(&[]);
            let v6 = bootstrap_nodes_v6
                .iter()
                .map(|n| (n.host.as_str(), n.port))
                .collect::<Vec<_>>();

            if v4.is_empty() && v6.is_empty() {
                None
            } else {
                let bind_v4 = SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::UNSPECIFIED,
                    config.listen_port + 1,
                ));
                let bind_v6 = SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::UNSPECIFIED,
                    config.listen_port + 2,
                    0,
                    0,
                ));
                match DhtNode::new(node_id, bind_v4, bind_v6, &v4, &v6).await {
                    Ok(node) => {
                        spawn_dht_poll(node.clone(), torrents.clone(), config.dht_poll_interval);
                        Some(node)
                    }
                    Err(e) => {
                        tracing::warn!("DHT init failed, DHT disabled: {e}");
                        None
                    }
                }
            }
        };

        Ok(Session {
            config,
            torrents,
            dht_node,
        })
    }

    // ── Torrent registration (sync) ──

    /// Register a torrent. Returns a [`TorrentBuilder`] for optional configuration.
    ///
    /// The torrent is inserted into the session immediately (state = [`TorrentState::Registered`]).
    /// Call [`.start()`](TorrentBuilder::start) on the builder to activate download.
    ///
    /// For magnet links, call [`.resolve_metadata()`](TorrentBuilder::resolve_metadata)
    /// before `.start()` to inspect metadata, or let `.start()` resolve automatically.
    pub fn add_torrent(&self, spec: impl Into<TorrentSpec>) -> Result<TorrentBuilder<'_>, Error> {
        let spec = spec.into();

        // Extract magnet peers (BEP 9 x.pe) before consuming spec
        let magnet_peers: Vec<SocketAddr> = if let TorrentSpec::Magnet(ref uri) = spec {
            let peers = uri.peers.iter();
            peers.filter_map(|p| SocketAddr::from_str(p).ok()).collect()
        } else {
            vec![]
        };

        let metadata_resolved = matches!(spec, TorrentSpec::Metainfo(_));
        let info_hash = spec.info_hash();

        // Register handle (consumes spec)
        let handle = TorrentHandle::register(spec, &self.config);
        let metainfo = handle.metainfo.as_ref();
        let (name, num_pieces) = metainfo
            .map(|m| {
                (
                    match &m.info.mode {
                        Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
                    },
                    m.info.num_pieces().to_string(),
                )
            })
            .unwrap_or_else(|| ("<unknown>".into(), "?".into()));

        self.torrents.write().unwrap().insert(info_hash, handle);

        tracing::info!("torrent registered: {name} ({num_pieces} pieces)");
        tracing::debug!(
            "torrent registered: {} ({num_pieces} pieces)",
            hex_encode(info_hash)
        );

        Ok(TorrentBuilder::new(
            self,
            info_hash,
            metadata_resolved,
            magnet_peers,
        ))
    }

    /// Register a torrent from raw bencoded bytes (a `.torrent` file).
    pub fn add_torrent_bytes(&self, data: &[u8]) -> Result<TorrentBuilder<'_>, Error> {
        self.add_torrent(Metainfo::try_from(data)?)
    }

    /// Register a torrent from a magnet URI string (BEP 9).
    pub fn add_magnet_str(&self, uri: impl AsRef<str>) -> Result<TorrentBuilder<'_>, Error> {
        let magnet: MagnetUri = uri.as_ref().parse()?;
        self.add_torrent(magnet)
    }

    // ── Accessors (for TorrentBuilder) ──

    pub(crate) fn config(&self) -> &SessionConfig {
        &self.config
    }

    pub(crate) fn torrents(&self) -> &Arc<RwLock<HashMap<InfoHash, TorrentHandle>>> {
        &self.torrents
    }

    // ── Lifecycle ──

    /// Pause an active torrent. No-op if not yet activated or already paused.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`] if the torrent is not found.
    /// Returns [`ErrorKind::Protocol`] if the download loop has stopped
    /// unexpectedly.
    pub async fn pause_torrent(&self, info_hash: &InfoHash) -> Result<(), Error> {
        let (control_tx, name) = {
            let torrents = self.torrents.read().unwrap();
            match torrents.get(info_hash) {
                Some(h) => (h.control_tx.clone(), torrent_name(h)),
                None => return Err(Error::new(ErrorKind::InvalidInput)),
            }
        };

        if let Some(tx) = control_tx {
            tracing::info!("pausing torrent: {name}");
            tracing::debug!("pausing torrent {} ({name})", hex_encode(*info_hash));
            tx.send(TorrentCommand::Pause)
                .await
                .map_err(|_| Error::new(ErrorKind::Protocol))
        } else {
            Ok(())
        }
    }

    /// Resume a paused torrent. No-op if not yet activated or already running.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`] if the torrent is not found.
    /// Returns [`ErrorKind::Protocol`] if the download loop has stopped
    /// unexpectedly.
    pub async fn resume_torrent(&self, info_hash: &InfoHash) -> Result<(), Error> {
        let (control_tx, name) = {
            let torrents = self.torrents.read().unwrap();
            match torrents.get(info_hash) {
                Some(h) => (h.control_tx.clone(), torrent_name(h)),
                None => return Err(Error::new(ErrorKind::InvalidInput)),
            }
        };
        if let Some(tx) = control_tx {
            tracing::info!("resuming torrent: {name}");
            tracing::debug!("resuming torrent {} ({name})", hex_encode(*info_hash));
            tx.send(TorrentCommand::Resume)
                .await
                .map_err(|_| Error::new(ErrorKind::Protocol))
        } else {
            Ok(())
        }
    }

    /// Remove a torrent and cancel its download loop.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`] if the torrent is not found.
    pub async fn remove_torrent(&self, info_hash: &InfoHash) -> Result<(), Error> {
        let handle = self.torrents.write().unwrap().remove(info_hash);

        let mut handle = handle.ok_or_else(|| Error::new(ErrorKind::InvalidInput))?;
        let name = torrent_name(&handle);

        tracing::info!("removing torrent: {name}");
        tracing::debug!("removing torrent {} ({name})", hex_encode(*info_hash));

        if let Some(tx) = &handle.control_tx {
            // Channel may already be closed if the download loop stopped;
            // still need to await the task below.
            let _ = tx.send(TorrentCommand::Cancel).await.inspect_err(|_| {
                tracing::debug!("cancel send failed (loop already stopped): {name}")
            });
        }

        if let Some(task) = handle.task.take() {
            if let Err(e) = task.await {
                tracing::warn!("download loop panicked for torrent: {name} ({e})");
            }
        }

        Ok(())
    }

    /// Get the status of a torrent.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`] if the torrent is not found.
    pub async fn torrent_status(&self, info_hash: &InfoHash) -> Result<TorrentStatus, Error> {
        let status = {
            let torrents = self.torrents.read().unwrap();

            match torrents.get(info_hash) {
                Some(handle) => handle.status.clone(), // Clone Arc, drop read guard before await
                None => {
                    return Err(Error::new(ErrorKind::InvalidInput));
                }
            }
        };

        Ok(status.read().await.clone())
    }

    /// List all active info_hashes.
    pub fn active_torrents(&self) -> Vec<InfoHash> {
        self.torrents.read().unwrap().keys().copied().collect()
    }
}

/// Extract a human-readable name from a [`TorrentHandle`].
fn torrent_name(handle: &TorrentHandle) -> String {
    let metainfo = handle.metainfo.as_ref();

    metainfo
        .map(|m| match &m.info.mode {
            Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
        })
        .unwrap_or_else(|| "<unknown>".into())
}

/// Spawn a background task that periodically queries the DHT for
/// peers of all active torrents.
fn spawn_dht_poll(
    dht: Arc<DhtNode>, torrents: Arc<RwLock<HashMap<InfoHash, TorrentHandle>>>,
    poll_interval: Duration,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(poll_interval).await;
            let info_hashes: Vec<InfoHash> = torrents.read().unwrap().keys().copied().collect();
            for ih in info_hashes {
                let peers = dht.get_peers(&ih).await;
                if !peers.is_empty() {
                    // Drop the read guard before awaiting on peer_mgr
                    let peer_mgr = torrents
                        .read()
                        .unwrap()
                        .get(&ih)
                        .map(|h| h.peer_mgr.clone());
                    if let Some(peer_mgr) = peer_mgr {
                        peer_mgr.write().await.add_peers(peers);
                    }
                }
            }
        }
    });
}
