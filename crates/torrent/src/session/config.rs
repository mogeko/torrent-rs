//! Session configuration and status types.
//!
//! This module contains the public types used to configure a
//! [`Session`](super::Session) and query its state:
//!
//! - [`SessionConfig`] — all configuration knobs
//! - [`TorrentStatus`] — per-torrent progress and statistics
//! - [`TorrentState`] — lifecycle state of a torrent
//! - [`InfoHash`] — SHA-1 identifier for a torrent

use std::path::PathBuf;
use std::time::Duration;

use crate::dht::BootstrapNode;

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
    pub listen_port: u16,
    /// Maximum number of peer connections per torrent.
    pub max_connections: u32,
    /// Maximum upload slots (unchoke limit, BEP 3).
    pub max_uploads: u32,
    /// Download directory for completed files.
    pub download_dir: PathBuf,

    // ── Rate Limiting ──
    /// Global download rate limit in bytes/s. `None` = unlimited.
    ///
    /// Applies across all torrents. Use `0` to pause downloads while
    /// keeping connections open. Per-torrent limits are not yet supported.
    pub download_rate_limit: Option<u64>,
    /// Global upload rate limit in bytes/s. `None` = unlimited.
    pub upload_rate_limit: Option<u64>,

    // ── Queue & Concurrency ──
    /// Maximum number of simultaneously active torrents.
    ///
    /// `0` means unlimited. When the limit is reached,
    /// [`Session::add_torrent`](super::Session::add_torrent) returns an error.
    pub max_active_torrents: usize,

    // ── Timers & Retries ──
    /// Timeout for a single block request (BEP 3).
    ///
    /// If a peer does not deliver the requested block within this
    /// duration, the request is cancelled and re-assigned.
    pub request_timeout: Duration,
    /// Per-peer TCP connection timeout.
    pub peer_connect_timeout: Duration,
    /// Maximum connection retries per peer before discarding.
    pub peer_max_retries: u32,
    /// Cooldown before reconnecting a failed peer.
    pub peer_cooldown: Duration,

    // ── DHT ──
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
            download_rate_limit: None,
            upload_rate_limit: None,
            max_active_torrents: 0,
            request_timeout: Duration::from_secs(60),
            peer_connect_timeout: Duration::from_millis(500),
            peer_max_retries: 3,
            peer_cooldown: Duration::from_secs(30),
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
        assert_eq!(back.max_connections, config.max_connections);
        assert_eq!(back.max_uploads, config.max_uploads);
        assert_eq!(back.download_dir, config.download_dir);
        assert_eq!(back.download_rate_limit, config.download_rate_limit);
        assert_eq!(back.upload_rate_limit, config.upload_rate_limit);
        assert_eq!(back.max_active_torrents, config.max_active_torrents);
        assert_eq!(back.request_timeout, config.request_timeout);
        assert_eq!(back.peer_connect_timeout, config.peer_connect_timeout);
        assert_eq!(back.peer_max_retries, config.peer_max_retries);
        assert_eq!(back.peer_cooldown, config.peer_cooldown);
        assert_eq!(back.node_id, config.node_id);
    }

    #[test]
    fn session_config_roundtrip_custom() {
        let config = SessionConfig {
            listen_port: 12345,
            max_connections: 200,
            max_uploads: 16,
            download_dir: PathBuf::from("/tmp/dl"),
            download_rate_limit: Some(1_048_576),
            upload_rate_limit: Some(524_288),
            max_active_torrents: 5,
            request_timeout: Duration::from_secs(120),
            peer_connect_timeout: Duration::from_secs(2),
            peer_max_retries: 5,
            peer_cooldown: Duration::from_secs(60),
            bootstrap_nodes: None,
            node_id: Some([0xAB; 20]),
        };

        let json = serde_json::to_string(&config).unwrap();
        let back: SessionConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(back.listen_port, 12345);
        assert_eq!(back.max_connections, 200);
        assert_eq!(back.max_uploads, 16);
        assert_eq!(back.download_dir, PathBuf::from("/tmp/dl"));
        assert_eq!(back.download_rate_limit, Some(1_048_576));
        assert_eq!(back.upload_rate_limit, Some(524_288));
        assert_eq!(back.max_active_torrents, 5);
        assert_eq!(back.request_timeout, Duration::from_secs(120));
        assert_eq!(back.peer_connect_timeout, Duration::from_secs(2));
        assert_eq!(back.peer_max_retries, 5);
        assert_eq!(back.peer_cooldown, Duration::from_secs(60));
        assert!(back.bootstrap_nodes.is_none());
        assert_eq!(back.node_id, Some([0xAB; 20]));
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
            TorrentState::Queued,
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
