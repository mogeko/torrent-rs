//! High-level session management — orchestrates all BitTorrent modules.
//!
//! The [`Session`] is the main entry point. Use [`SessionConfig`] to configure
//! it, then add torrents with [`Session::add_torrent_bytes`] or
//! [`Session::add_torrent`]. Track progress via [`Session::torrent_status`].
//!
//! # Architecture
//!
//! Each added torrent spawns a [`tokio`] task that runs a swarm loop.
//! The loop periodically connects to peers, requests blocks, verifies
//! pieces using SHA-1, and updates the torrent status.

mod config;
mod download;
mod inbound;
mod lsd;
mod peer_mgr;
mod seed;
mod swarm;
mod uni_deque;
mod upload_mgr;
mod webseed;

pub use self::config::{
    InfoHash, PeerStatus, SessionConfig, SessionStatus, TorrentEvent, TorrentState, TorrentStatus,
    TrackerStatus,
};
pub use self::download::DownloadBuilder;
pub use self::seed::{DataSource, PreparedTorrent, SeedBuilder};

use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::str::FromStr as _;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::dht::{DhtNode, generate_node_id};
use crate::error::{Error, ErrorKind};
use crate::magnet::{MagnetUri, hex_encode};
use crate::metainfo::{FileStatus, Metainfo, Mode};
use crate::peer::utp::UtpSocket;
use crate::piece::PieceManager;
use crate::spec::TorrentSpec;
use crate::storage::Storage;

use self::inbound::accept_loop;
use self::lsd::LsdService;
use self::seed::{DataSourceStorage, verify_existing};
use self::swarm::{TorrentCommand, TorrentHandle};

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
    dht_node: Option<Arc<DhtNode>>,
    /// LSD background task handle (keeps the task alive).
    #[expect(dead_code)]
    lsd_task: Option<JoinHandle<()>>,
    /// Shared uTP socket for all torrents (BEP 29).
    /// Created when [`SessionConfig::enable_utp`] is true.
    utp_socket: Option<Arc<UtpSocket>>,
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

        // Initialize LSD (Local Service Discovery, BEP 14)
        let lsd_task = if config.lsd_enabled {
            match LsdService::new(&config, torrents.clone()) {
                Ok(mut lsd) => Some(tokio::spawn(async move { lsd.run().await })),
                Err(e) => {
                    tracing::warn!("LSD init failed, LSD disabled: {e}");
                    None
                }
            }
        } else {
            None
        };

        // Initialize uTP socket (BEP 29)
        let utp_socket = if config.enable_utp {
            let bind_addr =
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, config.listen_port));
            match UtpSocket::bind(bind_addr).await {
                Ok(socket) => {
                    tracing::info!("uTP socket bound to {}", socket.local_addr());
                    Some(Arc::new(socket))
                }
                Err(e) => {
                    tracing::warn!("uTP init failed, uTP disabled: {e}");
                    None
                }
            }
        } else {
            None
        };

        // Initialize TCP listener for inbound peer connections
        let listen_addr =
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, config.listen_port));
        match TcpListener::bind(listen_addr).await {
            Ok(listener) => {
                tracing::info!("TCP listener bound to {}", listen_addr);
                let accept_torrents = torrents.clone();
                tokio::spawn(async move {
                    accept_loop(listener, accept_torrents).await;
                });
            }
            Err(e) => {
                tracing::warn!("TCP listen failed on {}: {}", listen_addr, e);
            }
        }

        Ok(Session {
            config,
            torrents,
            dht_node,
            lsd_task,
            utp_socket,
        })
    }

    // ── Torrent registration (sync) ──

    /// Register a torrent handle directly without a builder.
    ///
    /// Used by the seed path (which doesn't need `DownloadBuilder`)
    /// and internally by [`add_torrent`](Self::add_torrent).
    pub(crate) fn register_spec(&self, spec: impl Into<TorrentSpec>) -> InfoHash {
        let spec = spec.into();
        let info_hash = spec.info_hash();
        let mut handle = TorrentHandle::register(spec, &self.config);
        handle.utp_socket = self.utp_socket.clone();
        self.torrents.write().unwrap().insert(info_hash, handle);
        info_hash
    }

    /// Register a torrent. Returns a [`DownloadBuilder`] for optional configuration.
    ///
    /// The torrent is inserted into the session immediately (state = [`TorrentState::Registered`]).
    /// Call [`.start()`](DownloadBuilder::start) on the builder to activate download.
    ///
    /// For magnet links, call [`.resolve_metadata()`](DownloadBuilder::resolve_metadata)
    /// before `.start()` to inspect metadata, or let `.start()` resolve automatically.
    pub fn add_torrent(&self, spec: impl Into<TorrentSpec>) -> Result<DownloadBuilder<'_>, Error> {
        let spec = spec.into();

        // Extract magnet peers (BEP 9 x.pe) before consuming spec
        let magnet_peers: Vec<SocketAddr> = if let TorrentSpec::Magnet(ref uri) = spec {
            let peers = uri.peers.iter();
            peers.filter_map(|p| SocketAddr::from_str(p).ok()).collect()
        } else {
            vec![]
        };

        let metadata_resolved = matches!(spec, TorrentSpec::Metainfo(_));

        // Register (consumes spec)
        let info_hash = self.register_spec(spec);

        let (name, num_pieces) = {
            let torrents = self.torrents.read().unwrap();
            torrents
                .get(&info_hash)
                .and_then(|h| h.metainfo.as_ref())
                .map(|m| {
                    (
                        match &m.info.mode {
                            Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
                        },
                        m.info.num_pieces().to_string(),
                    )
                })
                .unwrap_or_else(|| ("<unknown>".into(), "?".into()))
        };

        tracing::info!("torrent registered: {name} ({num_pieces} pieces)");
        tracing::debug!(
            "torrent registered: {} ({num_pieces} pieces)",
            hex_encode(info_hash)
        );

        Ok(DownloadBuilder::new(
            self,
            info_hash,
            metadata_resolved,
            magnet_peers,
        ))
    }

    /// Register a torrent from raw bencoded bytes (a `.torrent` file).
    pub fn add_torrent_bytes(&self, data: &[u8]) -> Result<DownloadBuilder<'_>, Error> {
        self.add_torrent(Metainfo::try_from(data)?)
    }

    /// Register a torrent from a magnet URI string (BEP 9).
    pub fn add_magnet_str(&self, uri: impl AsRef<str>) -> Result<DownloadBuilder<'_>, Error> {
        let magnet: MagnetUri = uri.as_ref().parse()?;
        self.add_torrent(magnet)
    }

    // ── Seeding ──

    /// Prepare to seed from a data source.
    ///
    /// Returns a [`SeedBuilder`] that configures metadata parameters
    /// (piece length, tracker URL, etc.) and can either produce
    /// a `.torrent` file via [`.hash()`](SeedBuilder::hash) or begin seeding
    /// immediately via [`.start()`](SeedBuilder::start).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # use torrent::session::{Session, SessionConfig};
    /// let session = Session::new(SessionConfig::default()).await?;
    ///
    /// let info_hash = session
    ///     .seed_from(std::path::PathBuf::from("./my_release/video.mp4"))
    ///     .announce("http://tracker.example.com/announce")
    ///     .start()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn seed_from(&self, source: impl DataSource + 'static) -> SeedBuilder<'_> {
        SeedBuilder::new(self, source)
    }

    /// Register and activate a prepared torrent for seeding.
    ///
    /// `prepared` must have been produced by [`SeedBuilder::hash`].
    /// Verifies the on-disk data against the torrent's piece hashes,
    /// registers the torrent with the session, and activates the
    /// download loop.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if reading the data source fails during
    /// piece verification against the torrent's expected hashes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # use torrent::session::{Session, SessionConfig};
    /// let session = Session::new(SessionConfig::default()).await?;
    ///
    /// let prepared = session
    ///     .seed_from(std::path::PathBuf::from("./video.mp4"))
    ///     .announce("http://tracker.example.com/announce")
    ///     .hash()
    ///     .await?;
    ///
    /// // Export .torrent before seeding
    /// std::fs::write("video.torrent", prepared.torrent_bytes())?;
    ///
    /// // Start seeding
    /// let info_hash = session.start_seeding(prepared).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start_seeding(&self, prepared: PreparedTorrent) -> Result<InfoHash, Error> {
        let info = prepared.metainfo().info.clone();
        let metainfo = prepared.metainfo().clone();
        let super_seed = prepared.super_seed();

        // Verify existing data via the stored source
        let storage = DataSourceStorage::new(prepared.into_source(), &info);
        let mut piece_mgr = PieceManager::new(info.num_pieces());

        verify_existing(&storage, &info, &mut piece_mgr).await?;

        let info_hash = self.register_spec(TorrentSpec::Metainfo(metainfo.clone()));

        let mut torrents = self.torrents.write().unwrap();
        let handle = torrents
            .get_mut(&info_hash)
            .expect("torrent must be present: inserted by register_spec");

        handle.activate_seed(
            metainfo,
            Arc::new(storage) as Arc<dyn Storage>,
            piece_mgr,
            self.config(),
            super_seed,
        );

        Ok(info_hash)
    }

    // ── Accessors (for DownloadBuilder) ──

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

    /// Subscribe to a live event stream for a torrent.
    ///
    /// Returns a [`broadcast::Receiver`] that yields [`TorrentEvent`]s
    /// as they occur — piece completions, state transitions, and
    /// torrent completion.
    ///
    /// The broadcast channel has a bounded buffer (64 events); slow
    /// consumers may miss events.  Always use
    /// [`torrent_status`](Self::torrent_status) as the authoritative
    /// source of truth.  This stream is best-effort notification.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`] if the torrent is not found.
    pub fn torrent_events(
        &self, info_hash: &InfoHash,
    ) -> Result<broadcast::Receiver<TorrentEvent>, Error> {
        let torrents = self.torrents.read().unwrap();
        match torrents.get(info_hash) {
            Some(handle) => Ok(handle.event_tx.subscribe()),
            None => Err(Error::new(ErrorKind::InvalidInput)),
        }
    }

    /// Clear the error state of a torrent, transitioning it back to
    /// [`TorrentState::Downloading`] (or [`TorrentState::Seeding`] if
    /// all pieces are already complete).
    ///
    /// No-op if the torrent is not in [`TorrentState::Error`].
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`] if the torrent is not found.
    pub async fn clear_error(&self, info_hash: &InfoHash) -> Result<(), Error> {
        let (status, event_tx) = {
            let torrents = self.torrents.read().unwrap();
            match torrents.get(info_hash) {
                Some(handle) => (handle.status.clone(), handle.event_tx.clone()),
                None => return Err(Error::new(ErrorKind::InvalidInput)),
            }
        };

        let mut s = status.write().await;
        if s.state == TorrentState::Error {
            s.error_message = None;
            // Transition to Downloading — the next status tick will promote
            // to Seeding if all pieces are complete.
            s.state = TorrentState::Downloading;
            drop(s);
            let _ = event_tx.send(TorrentEvent::StateChanged {
                from: TorrentState::Error,
                to: TorrentState::Downloading,
            });
        }
        Ok(())
    }

    /// Get aggregated status across all active torrents.
    ///
    /// Computes sums of download/upload rates and bytes, plus
    /// torrent and connection counts.
    pub async fn session_status(&self) -> SessionStatus {
        // Collect status Arcs first, then await them without holding the
        // torrents read lock.
        let statuses: Vec<_> = {
            let torrents = self.torrents.read().unwrap();
            torrents.values().map(|h| h.status.clone()).collect()
        };

        let num_torrents = statuses.len();
        let mut total_download_rate = 0.0_f64;
        let mut total_upload_rate = 0.0_f64;
        let mut total_downloaded = 0_u64;
        let mut total_uploaded = 0_u64;
        let mut num_connections = 0_usize;

        for status in &statuses {
            let s = status.read().await;
            total_download_rate += s.download_rate;
            total_upload_rate += s.upload_rate;
            total_downloaded += s.total_downloaded;
            total_uploaded += s.total_uploaded;
            num_connections += s.num_peers;
        }

        SessionStatus {
            total_download_rate,
            total_upload_rate,
            total_downloaded,
            total_uploaded,
            num_torrents,
            num_connections,
            dht_nodes: match &self.dht_node {
                Some(n) => n.num_nodes().await,
                None => 0,
            },
        }
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

    /// Get per-file download progress for a torrent.
    ///
    /// For single-file torrents, returns a single-element `Vec`.
    /// For multi-file torrents, computes each file's progress by
    /// intersecting completed piece byte ranges with file boundaries.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`] if the torrent is not found,
    /// or if metadata has not yet been resolved (magnet link without
    /// downloaded metadata).
    pub async fn file_status(&self, info_hash: &InfoHash) -> Result<Vec<FileStatus>, Error> {
        let (piece_mgr, metainfo) = {
            let torrents = self.torrents.read().unwrap();
            match torrents.get(info_hash) {
                Some(handle) => (handle.piece_mgr.clone(), handle.metainfo.clone()),
                None => return Err(Error::new(ErrorKind::InvalidInput)),
            }
        };

        let metainfo = metainfo.ok_or_else(|| Error::new(ErrorKind::InvalidInput))?;
        let pm = piece_mgr.read().await;
        Ok(metainfo.info.file_status(pm.bitfield()))
    }

    /// Get per-peer status snapshots for all connected peers.
    ///
    /// Updated every status tick (~1 s).  Returns an empty `Vec` if
    /// no peers are connected or the torrent has not been activated.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`] if the torrent is not found.
    pub fn peer_status(&self, info_hash: &InfoHash) -> Result<Vec<PeerStatus>, Error> {
        let peer_statuses = {
            let torrents = self.torrents.read().unwrap();
            match torrents.get(info_hash) {
                Some(handle) => handle.peer_statuses.clone(),
                None => return Err(Error::new(ErrorKind::InvalidInput)),
            }
        };
        Ok(peer_statuses
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default())
    }

    /// Get the tracker communication status for a torrent.
    ///
    /// Updated after each tracker announce (success or failure).
    /// Before the first announce, most fields are at their defaults.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidInput`] if the torrent is not found.
    pub async fn tracker_status(&self, info_hash: &InfoHash) -> Result<TrackerStatus, Error> {
        let ts = {
            let torrents = self.torrents.read().unwrap();
            match torrents.get(info_hash) {
                Some(handle) => handle.tracker_status.clone(),
                None => return Err(Error::new(ErrorKind::InvalidInput)),
            }
        };
        Ok(ts.read().await.clone())
    }

    /// List all active info_hashes.
    pub fn active_torrents(&self) -> Vec<InfoHash> {
        self.torrents.read().unwrap().keys().copied().collect()
    }

    // ── Metadata export ──

    /// Get a clone of the torrent's full [`Metainfo`].
    ///
    /// Works for both downloaded and seeded torrents.
    ///
    /// Returns `None` if the info_hash is not found or if metadata
    /// has not yet been resolved (e.g. magnet link without downloaded
    /// metadata).
    pub fn metainfo(&self, info_hash: &InfoHash) -> Option<Metainfo> {
        let torrents = self.torrents.read().unwrap();
        torrents.get(info_hash).and_then(|h| h.metainfo.clone())
    }

    /// Get the serialized `.torrent` bytes for a torrent.
    ///
    /// Convenience wrapper around [`metainfo`](Self::metainfo) that
    /// calls [`Metainfo::to_bytes`].
    ///
    /// Returns `None` if the info_hash is not found or if metadata
    /// has not yet been resolved.
    pub fn torrent_bytes(&self, info_hash: &InfoHash) -> Option<Vec<u8>> {
        self.metainfo(info_hash)?.to_bytes()
    }

    /// Generate a magnet URI for a torrent (BEP 9).
    ///
    /// Convenience wrapper around [`metainfo`](Self::metainfo) that
    /// formats a `magnet:?xt=urn:btih:...` URI.
    ///
    /// Returns `None` if the info_hash is not found or if metadata
    /// has not yet been resolved.
    pub fn magnet_uri(&self, info_hash: &InfoHash) -> Option<String> {
        let meta = self.metainfo(info_hash)?;
        let name = match &meta.info.mode {
            Mode::Single { name, .. } | Mode::Multiple { name, .. } => name,
        };
        let ih = hex_encode(meta.info_hash());
        Some(format!("magnet:?xt=urn:btih:{ih}&dn={name}"))
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
