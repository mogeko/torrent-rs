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

mod download;
mod peer_manager;
mod torrent;
mod uni_deque;
mod upload;

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use crate::dht::{BootstrapNode, DhtNode, generate_node_id};
use crate::error::{Error, ErrorKind};
use crate::magnet::{MagnetUri, hex_encode};
use crate::metainfo::{Info, Metainfo, Mode, RawInfo};
use crate::spec::TorrentSpec;
use crate::storage::FileStorage;

use self::torrent::TorrentHandle;

/// Unique identifier for a torrent (SHA-1 info hash).
///
/// This is the 20-byte hash used throughout the BitTorrent protocol
/// to identify torrents. It is computed as `SHA-1(bencoded_info_dict)`.
pub type InfoHash = [u8; 20];

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
/// let config = SessionConfig {
///     download_dir: std::path::PathBuf::from("./downloads"),
///     ..Default::default()
/// };
/// let session = Session::new(config).await.unwrap();
///
/// // Add a torrent from a .torrent file
/// let data = std::fs::read("torrent.torrent").unwrap();
/// let info_hash = session.add_torrent_bytes(&data).await.unwrap();
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
    /// Shared DHT node (if DHT is enabled).
    #[expect(dead_code)]
    dht_node: Option<Arc<DhtNode>>,
}

/// Session configuration.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// TCP listen port for incoming peer connections.
    pub listen_port: u16,
    /// Maximum number of peer connections per torrent.
    pub max_connections: u32,
    /// Maximum upload slots.
    pub max_uploads: u32,
    /// Download directory.
    pub download_dir: PathBuf,
    /// DHT bootstrap nodes. Set to `None` to disable DHT entirely.
    /// When `Some`, the session initializes a DHT node and uses these
    /// addresses to join the DHT network (BEP 5).
    ///
    /// Default: `Some(vec![...])` with well-known public bootstrap nodes.
    pub bootstrap_nodes: Option<Vec<BootstrapNode>>,
    /// Optional DHT node ID (20 bytes). If `None`, a random one is generated
    /// each session. Set this to a persisted value to keep a stable identity
    /// across restarts (BEP 5 recommends persisting the node ID).
    pub node_id: Option<[u8; 20]>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        SessionConfig {
            listen_port: 6881,
            max_connections: 50,
            max_uploads: 8,
            download_dir: PathBuf::from("downloads"),
            bootstrap_nodes: Some(vec![
                BootstrapNode::from(("router.bittorrent.com", 6881)),
                BootstrapNode::from(("dht.transmissionbt.com", 6881)),
            ]),
            node_id: None,
        }
    }
}

/// Status of a torrent, exposed via the public API.
#[derive(Debug, Clone)]
pub struct TorrentStatus {
    /// The 20-byte info hash.
    pub info_hash: InfoHash,
    /// Display name of the torrent.
    pub name: String,
    /// Download progress (0.0 to 1.0).
    pub progress: f64,
    /// Download rate in bytes per second.
    pub download_rate: f64,
    /// Upload rate in bytes per second.
    pub upload_rate: f64,
    /// Number of connected peers.
    pub num_peers: usize,
    /// Number of seeders (peers with 100% completion).
    pub num_seeds: usize,
    /// Current state of the torrent.
    pub state: TorrentState,
}

/// Possible states of a torrent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentState {
    /// Waiting to start.
    Queued,
    /// Actively downloading.
    Downloading,
    /// All pieces downloaded, uploading only.
    Seeding,
    /// Paused by user.
    Paused,
    /// An error occurred.
    Error,
}

impl Session {
    /// Create a new session with the given configuration.
    pub async fn new(config: SessionConfig) -> Result<Self, Error> {
        let torrents: Arc<RwLock<HashMap<InfoHash, TorrentHandle>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Initialize DHT if bootstrap nodes are configured
        let dht_node = if let Some(ref bootstrap) = config.bootstrap_nodes {
            if bootstrap.is_empty() {
                None
            } else {
                let bind_addr = SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::UNSPECIFIED,
                    config.listen_port + 1,
                ));
                let bootstrap_refs: Vec<(&str, u16)> = bootstrap
                    .iter()
                    .map(|n| (n.host.as_str(), n.port))
                    .collect();
                let node_id = config.node_id.unwrap_or_else(generate_node_id);
                let node = DhtNode::new(node_id, bind_addr, &bootstrap_refs).await?;

                // Spawn background feeder: poll each torrent's info_hash every 30s
                let (dht, t) = (node.clone(), torrents.clone());
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        let info_hashes: Vec<InfoHash> = t.read().await.keys().copied().collect();
                        for ih in info_hashes {
                            let peers = dht.get_peers(&ih).await;
                            if !peers.is_empty() {
                                if let Some(handle) = t.read().await.get(&ih) {
                                    handle.peer_mgr.write().await.add_peers(peers);
                                }
                            }
                        }
                    }
                });

                Some(node)
            }
        } else {
            None
        };

        Ok(Session {
            config,
            torrents,
            dht_node,
        })
    }

    /// Add a torrent from a [`TorrentSpec`].
    ///
    /// Accepts both full metadata ([`Metainfo`]) and magnet links
    /// ([`MagnetUri`]). For magnet links, file download cannot start
    /// until metadata is obtained from peers.
    ///
    /// [`add_torrent_bytes`](Session::add_torrent_bytes) and
    /// [`add_magnet_str`](Session::add_magnet_str) are convenience
    /// wrappers.
    pub async fn add_torrent(&self, spec: impl Into<TorrentSpec>) -> Result<InfoHash, Error> {
        match spec.into() {
            TorrentSpec::Metainfo(meta) => self.add_metainfo(meta).await,
            TorrentSpec::Magnet(uri) => self.add_magnet(uri).await,
        }
    }

    /// Add a torrent from a magnet URI string (BEP 9).
    pub async fn add_magnet_str(&self, uri: impl AsRef<str>) -> Result<InfoHash, Error> {
        let magnet: MagnetUri = uri.as_ref().parse()?;
        self.add_torrent(magnet).await
    }

    /// Add a torrent from raw bencoded bytes (a `.torrent` file).
    pub async fn add_torrent_bytes(&self, data: &[u8]) -> Result<InfoHash, Error> {
        self.add_torrent(Metainfo::try_from(data)?).await
    }

    /// Core: bootstrap a torrent from full metadata.
    async fn add_metainfo(&self, meta: Metainfo) -> Result<InfoHash, Error> {
        let info_hash = meta.info_hash();
        let _name = match &meta.info.mode {
            Mode::Single { name, .. } | Mode::Multiple { name, .. } => name.clone(),
        };

        // Create FileStorage
        let storage = Arc::new(FileStorage::new(&meta.info, &self.config.download_dir).await?);

        let handle = TorrentHandle::new(meta, info_hash, storage, &self.config);
        self.torrents.write().await.insert(info_hash, handle);

        Ok(info_hash)
    }

    /// Core: bootstrap a torrent from a magnet URI.
    async fn add_magnet(&self, uri: MagnetUri) -> Result<InfoHash, Error> {
        let info_hash = *uri.primary_info_hash();
        let name = uri
            .display_name
            .clone()
            .unwrap_or_else(|| hex_encode(info_hash));
        let announce = uri.trackers.first().cloned().unwrap_or_default();
        let announce_list = if uri.trackers.len() > 1 {
            vec![uri.trackers[1..].to_vec()]
        } else {
            vec![]
        };

        // Build a minimal Metainfo stub: no pieces, no raw_info.
        // The download loop will be idle until metadata is discovered.
        let meta = Metainfo {
            announce,
            announce_list,
            info: Info {
                piece_length: 0,
                pieces: vec![],
                mode: Mode::Single {
                    name: name.clone(),
                    length: uri.exact_length.unwrap_or(0),
                },
                raw_info: RawInfo::Hash(info_hash),
            },
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        };

        let storage = Arc::new(FileStorage::new(&meta.info, &self.config.download_dir).await?);
        let handle = TorrentHandle::new(meta, info_hash, storage, &self.config);

        // Inject x.pe addresses directly into the connection pool (BEP 9).
        if !uri.peers.is_empty() {
            let mut peer_addrs = Vec::with_capacity(uri.peers.len());
            for peer_str in &uri.peers {
                if let Ok(addr) = SocketAddr::from_str(peer_str) {
                    peer_addrs.push(addr);
                }
            }
            if !peer_addrs.is_empty() {
                handle.peer_mgr.write().await.add_peers(peer_addrs);
            }
        }

        self.torrents.write().await.insert(info_hash, handle);

        Ok(info_hash)
    }

    /// Remove a torrent by info_hash.
    pub async fn remove_torrent(&self, info_hash: &InfoHash) -> Result<(), Error> {
        let handle = self.torrents.write().await.remove(info_hash);
        if let Some(mut h) = handle {
            h.cancel().await;
            // Await the task to ensure clean shutdown
            let _ = h.task.await;
        }
        Ok(())
    }

    /// Get the status of a torrent.
    pub async fn torrent_status(&self, info_hash: &InfoHash) -> Result<TorrentStatus, Error> {
        let torrents = self.torrents.read().await;
        let handle = torrents
            .get(info_hash)
            .ok_or(Error::new(ErrorKind::InvalidInput))?;
        Ok(handle.status().await)
    }

    /// List all active info_hashes.
    pub async fn active_torrents(&self) -> Vec<InfoHash> {
        self.torrents.read().await.keys().copied().collect()
    }
}
