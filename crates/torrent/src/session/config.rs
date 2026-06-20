//! Session configuration and status types.
//!
//! This module contains the public types used to configure a
//! [`Session`](super::Session) and query its state:
//!
//! - [`SessionConfig`] — all configuration knobs
//! - [`TorrentStatus`] — per-torrent progress and statistics
//! - [`TorrentState`] — lifecycle state of a torrent
//! - [`InfoHash`] — SHA-1 identifier for a torrent

use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::Duration;

use crate::dht::BootstrapNode;
#[cfg(test)]
use crate::storage::FileStorageFactory;
use crate::storage::StorageFactory;

/// Unique identifier for a torrent (SHA-1 info hash).
///
/// This is the 20-byte hash used throughout the BitTorrent protocol
/// to identify torrents. It is computed as `SHA-1(bencoded_info_dict)`.
pub type InfoHash = [u8; 20];

/// Session configuration.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SessionConfig {
    // ── Network ──
    /// TCP listen port for incoming peer connections.
    ///
    /// Default: `6881`.
    pub listen_port: u16,
    /// Explicit IPv4 address to announce to trackers (BEP 7).
    ///
    /// When `None`, the tracker auto-detects the address from the
    /// connection. Set this when behind NAT with a known external address.
    ///
    /// Default: `None`.
    pub announce_ip: Option<Ipv4Addr>,
    /// Explicit IPv6 address to announce to trackers (BEP 7).
    ///
    /// When `None`, the tracker auto-detects from the connection.
    ///
    /// Default: `None`.
    pub announce_ipv6: Option<Ipv6Addr>,
    /// Maximum number of peer connections per torrent.
    ///
    /// Default: `50`.
    pub max_connections: u32,
    /// Maximum upload slots (unchoke limit, BEP 3).
    ///
    /// Default: `8`.
    pub max_uploads: u32,
    // ── Rate Limiting ──
    /// Global download rate limit in bytes/s. `None` = unlimited.
    ///
    /// Applies across all torrents. Use `0` to pause downloads while
    /// keeping connections open. Per-torrent limits are not yet supported.
    ///
    /// Default: `None`.
    pub download_rate_limit: Option<u64>,
    /// Global upload rate limit in bytes/s. `None` = unlimited.
    ///
    /// Default: `None`.
    pub upload_rate_limit: Option<u64>,

    // ── Queue & Concurrency ──
    /// Maximum number of simultaneously active torrents.
    ///
    /// `0` means unlimited. When the limit is reached,
    /// [`Session::add_torrent`](super::Session::add_torrent) returns an error.
    ///
    /// Default: `0` (unlimited).
    pub max_active_torrents: usize,
    /// Maximum number of pieces to download concurrently.
    ///
    /// Default: `5`.
    pub max_concurrent_pieces: usize,
    /// How many completed pieces to cache for upload serving (LRU eviction).
    ///
    /// Default: `256`.
    pub piece_cache_size: usize,
    /// When fewer than this many pieces remain, switch to EndGame mode.
    ///
    /// Default: `10`.
    pub endgame_threshold: usize,

    // ── Timers & Retries ──
    /// Timeout for a single block request (BEP 3).
    ///
    /// If a peer does not deliver the requested block within this
    /// duration, the request is cancelled and re-assigned.
    ///
    /// Default: `60` s.
    pub request_timeout: Duration,
    /// Per-peer TCP connection timeout.
    ///
    /// Default: `500` ms.
    pub peer_connect_timeout: Duration,
    /// Maximum connection retries per peer before discarding.
    ///
    /// Default: `3`.
    pub peer_max_retries: u32,
    /// Cooldown before reconnecting a failed peer.
    ///
    /// Default: `30` s.
    pub peer_cooldown: Duration,
    /// How often to run the choke/unchoke algorithm.
    ///
    /// Default: `10` s.
    pub choke_interval: Duration,
    /// Idle duration before a peer is snubbed (BEP 3).
    ///
    /// Default: `60` s.
    pub snub_timeout: Duration,
    /// How many corrupt blocks before banning a peer.
    ///
    /// Default: `10`.
    pub corrupt_ban_threshold: u32,
    /// Re-announce interval after a tracker request fails.
    ///
    /// Default: `30` s.
    pub announce_fallback_interval: Duration,
    /// Timeout for HTTP and UDP tracker requests.
    ///
    /// Default: `15` s.
    pub tracker_timeout: Duration,
    /// How often the DHT background task polls for new peers.
    ///
    /// Default: `30` s.
    pub dht_poll_interval: Duration,
    /// Enable Peer Exchange (PEX, BEP 11). When enabled, the session
    /// exchanges peer lists with connected peers that support it.
    ///
    /// Default: `true`.
    pub pex_enabled: bool,
    /// How often to broadcast PEX messages to connected peers.
    ///
    /// Default: `60` s.
    pub pex_interval: Duration,
    /// Buffer size for the peer message channel (per torrent).
    ///
    /// Default: `256`.
    pub peer_msg_buffer_size: usize,

    // ── Storage ──
    /// Default storage factory. Overrideable per-torrent via
    /// [`TorrentBuilder::download_dir`] or [`TorrentBuilder::storage`].
    ///
    /// When `None`, each torrent must provide a factory through the builder.
    ///
    /// Default: `None`.
    ///
    /// [`TorrentBuilder::download_dir`]: crate::session::TorrentBuilder::download_dir
    /// [`TorrentBuilder::storage`]: crate::session::TorrentBuilder::storage
    #[cfg_attr(feature = "serde", serde(skip))]
    pub default_storage: Option<Arc<dyn StorageFactory>>,

    // ── DHT ──
    /// DHT bootstrap nodes. Set to `None` to disable DHT entirely.
    /// When `Some`, the session initializes a DHT node and uses these
    /// addresses to join the DHT network (BEP 5).
    ///
    /// Default: `Some(vec![router.bittorrent.com:6881, dht.transmissionbt.com:6881])`.
    pub bootstrap_nodes: Option<Vec<BootstrapNode>>,
    /// IPv6 DHT bootstrap nodes (BEP 32). Set to `None` to disable
    /// the IPv6 DHT. When `Some` with at least one node, the session
    /// initializes a second DHT node for IPv6.
    ///
    /// Default: `None`.
    pub bootstrap_nodes_v6: Option<Vec<BootstrapNode>>,
    /// Optional DHT node ID (20 bytes). If `None`, a random one is generated
    /// each session. Set this to a persisted value to keep a stable identity
    /// across restarts (BEP 5 recommends persisting the node ID).
    ///
    /// Default: `None`.
    pub node_id: Option<[u8; 20]>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        SessionConfig {
            listen_port: 6881,
            announce_ip: None,
            announce_ipv6: None,
            max_connections: 50,
            max_uploads: 8,
            download_rate_limit: None,
            upload_rate_limit: None,
            max_active_torrents: 0,
            max_concurrent_pieces: 5,
            piece_cache_size: 256,
            endgame_threshold: 10,
            request_timeout: Duration::from_secs(60),
            peer_connect_timeout: Duration::from_millis(500),
            peer_max_retries: 3,
            peer_cooldown: Duration::from_secs(30),
            choke_interval: Duration::from_secs(10),
            snub_timeout: Duration::from_secs(60),
            corrupt_ban_threshold: 10,
            announce_fallback_interval: Duration::from_secs(30),
            tracker_timeout: Duration::from_secs(15),
            dht_poll_interval: Duration::from_secs(30),
            pex_enabled: true,
            pex_interval: Duration::from_secs(60),
            peer_msg_buffer_size: 256,
            bootstrap_nodes: Some(vec![
                BootstrapNode::from(("router.bittorrent.com", 6881)),
                BootstrapNode::from(("dht.transmissionbt.com", 6881)),
            ]),
            bootstrap_nodes_v6: None,
            node_id: None,
            default_storage: None,
        }
    }
}

/// Status of a torrent, exposed via the public API.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
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
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TorrentState {
    /// Metadata registered, no storage/download started yet.
    Registered,
    /// Actively downloading.
    Downloading,
    /// All pieces downloaded, uploading only.
    Seeding,
    /// Paused by user.
    Paused,
    /// An error occurred.
    Error,
}

#[cfg(all(test, feature = "serde"))]
mod serde_tests {
    use std::time::Duration;

    use super::*;

    // ── SessionConfig ─────────────────────────────────────────

    #[test]
    fn session_config_roundtrip_default() {
        let config = SessionConfig::default();
        let json = serde_json::to_string_pretty(&config).unwrap();
        let back: SessionConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(back.listen_port, config.listen_port);
        assert_eq!(back.announce_ip, config.announce_ip);
        assert_eq!(back.announce_ipv6, config.announce_ipv6);
        assert_eq!(back.max_uploads, config.max_uploads);
        assert_eq!(back.download_rate_limit, config.download_rate_limit);
        assert_eq!(back.upload_rate_limit, config.upload_rate_limit);
        assert_eq!(back.max_active_torrents, config.max_active_torrents);
        assert_eq!(back.max_concurrent_pieces, config.max_concurrent_pieces);
        assert_eq!(back.piece_cache_size, config.piece_cache_size);
        assert_eq!(back.endgame_threshold, config.endgame_threshold);
        assert_eq!(back.request_timeout, config.request_timeout);
        assert_eq!(back.peer_connect_timeout, config.peer_connect_timeout);
        assert_eq!(back.peer_max_retries, config.peer_max_retries);
        assert_eq!(back.peer_cooldown, config.peer_cooldown);
        assert_eq!(back.choke_interval, config.choke_interval);
        assert_eq!(back.snub_timeout, config.snub_timeout);
        assert_eq!(back.corrupt_ban_threshold, config.corrupt_ban_threshold);
        assert_eq!(
            back.announce_fallback_interval,
            config.announce_fallback_interval
        );
        assert_eq!(back.tracker_timeout, config.tracker_timeout);
        assert_eq!(back.node_id, config.node_id);
        assert_eq!(back.dht_poll_interval, config.dht_poll_interval);
        assert_eq!(back.pex_enabled, config.pex_enabled);
        assert_eq!(back.pex_interval, config.pex_interval);
        assert_eq!(back.peer_msg_buffer_size, config.peer_msg_buffer_size);
    }

    #[test]
    fn session_config_roundtrip_custom() {
        let config = SessionConfig {
            listen_port: 12345,
            announce_ip: Some(Ipv4Addr::new(1, 2, 3, 4)),
            announce_ipv6: Some(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
            max_connections: 200,
            max_uploads: 16,
            download_rate_limit: Some(1_048_576),
            upload_rate_limit: Some(524_288),
            max_active_torrents: 5,
            max_concurrent_pieces: 10,
            piece_cache_size: 128,
            endgame_threshold: 5,
            request_timeout: Duration::from_secs(120),
            peer_connect_timeout: Duration::from_secs(2),
            peer_max_retries: 5,
            peer_cooldown: Duration::from_secs(60),
            choke_interval: Duration::from_secs(20),
            snub_timeout: Duration::from_secs(120),
            corrupt_ban_threshold: 5,
            announce_fallback_interval: Duration::from_secs(60),
            tracker_timeout: Duration::from_secs(30),
            bootstrap_nodes: None,
            bootstrap_nodes_v6: None,
            node_id: Some([0xAB; 20]),
            dht_poll_interval: Duration::from_secs(60),
            pex_enabled: false,
            pex_interval: Duration::from_secs(120),
            peer_msg_buffer_size: 512,
            default_storage: Some(Arc::new(FileStorageFactory::new("."))),
        };

        let json = serde_json::to_string(&config).unwrap();
        let back: SessionConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(back.listen_port, 12345);
        assert_eq!(back.announce_ip, Some(Ipv4Addr::new(1, 2, 3, 4)));
        assert_eq!(
            back.announce_ipv6,
            Some(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1))
        );
        assert_eq!(back.max_connections, 200);
        assert_eq!(back.max_uploads, 16);
        assert_eq!(back.download_rate_limit, Some(1_048_576));
        assert_eq!(back.upload_rate_limit, Some(524_288));
        assert_eq!(back.max_active_torrents, 5);
        assert_eq!(back.max_concurrent_pieces, 10);
        assert_eq!(back.piece_cache_size, 128);
        assert_eq!(back.endgame_threshold, 5);
        assert_eq!(back.request_timeout, Duration::from_secs(120));
        assert_eq!(back.peer_connect_timeout, Duration::from_secs(2));
        assert_eq!(back.peer_max_retries, 5);
        assert_eq!(back.peer_cooldown, Duration::from_secs(60));
        assert_eq!(back.choke_interval, Duration::from_secs(20));
        assert_eq!(back.snub_timeout, Duration::from_secs(120));
        assert_eq!(back.corrupt_ban_threshold, 5);
        assert_eq!(back.announce_fallback_interval, Duration::from_secs(60));
        assert_eq!(back.tracker_timeout, Duration::from_secs(30));
        assert!(back.bootstrap_nodes.is_none());
        assert!(back.bootstrap_nodes_v6.is_none());
        assert_eq!(back.node_id, Some([0xAB; 20]));
        assert_eq!(back.dht_poll_interval, Duration::from_secs(60));
        assert_eq!(back.pex_enabled, false);
        assert_eq!(back.pex_interval, Duration::from_secs(120));
        assert_eq!(back.peer_msg_buffer_size, 512);
    }

    #[test]
    fn session_config_duration_fields_use_default_serde() {
        let config = SessionConfig::default();
        let json = serde_json::to_value(&config).unwrap();
        // serde's default Duration format: {"secs": N, "nanos": N}
        assert!(json["request_timeout"].is_object());
        assert_eq!(json["request_timeout"]["secs"], 60);
        assert_eq!(json["peer_connect_timeout"]["nanos"], 500_000_000);
    }

    // ── TorrentStatus / TorrentState ───────────────────────────

    #[test]
    fn torrent_status_serialize() {
        let status = TorrentStatus {
            info_hash: [0x42; 20],
            name: "test.iso".into(),
            progress: 0.75,
            download_rate: 1_048_576.0,
            upload_rate: 512_000.0,
            num_peers: 12,
            num_seeds: 3,
            state: TorrentState::Downloading,
        };
        let json = serde_json::to_string(&status).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["name"], "test.iso");
        assert!((v["progress"].as_f64().unwrap() - 0.75).abs() < 0.001);
        assert_eq!(v["num_peers"], 12);
        assert_eq!(v["num_seeds"], 3);
        assert_eq!(v["state"], "Downloading");
    }

    #[test]
    fn torrent_state_roundtrip() {
        let states = [
            TorrentState::Registered,
            TorrentState::Downloading,
            TorrentState::Seeding,
            TorrentState::Paused,
            TorrentState::Error,
        ];
        for &state in &states {
            let json = serde_json::to_string(&state).unwrap();
            let back: TorrentState = serde_json::from_str(&json).unwrap();
            assert_eq!(back, state);
        }
    }
}
