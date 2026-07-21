//! Session configuration and status types.
//!
//! This module contains the public types used to configure a
//! [`Session`](super::Session) and query its state:
//!
//! - [`SessionConfig`] — all configuration knobs
//! - [`TorrentStatus`] — per-torrent progress and statistics
//! - [`TorrentState`] — lifecycle state of a torrent
//! - [`InfoHash`] — SHA-1 identifier for a torrent

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
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
    /// Enable Local Service Discovery (LSD, BEP 14). When enabled, the
    /// session announces its presence on the local network via UDP
    /// multicast and discovers LAN peers automatically.
    ///
    /// Default: `true`.
    pub lsd_enabled: bool,
    /// How often to send LSD announce messages (BEP 14).
    ///
    /// Default: `300` s (5 minutes).
    pub lsd_interval: Duration,
    /// Buffer size for the peer message channel (per torrent).
    ///
    /// Default: `256`.
    pub peer_msg_buffer_size: usize,

    /// Enable uTP (Micro Transport Protocol, BEP 29). When enabled,
    /// the session binds a UDP socket and prefers uTP over TCP for
    /// peer connections. uTP provides delay-based congestion control
    /// that avoids saturating home network upload buffers.
    ///
    /// Disable this to force TCP-only connections.
    ///
    /// Default: `true`.
    pub enable_utp: bool,

    // ── Web Seed ──
    /// Enable web seed downloads (BEP 19). When enabled, the session
    /// downloads from HTTP/FTP web seed URLs found in the torrent
    /// metadata (`url-list`) or magnet link (`ws` parameter).
    ///
    /// Default: `true`.
    pub webseed_enabled: bool,
    /// Minimum contiguous gap in pieces to trigger a web seed HTTP
    /// download. Smaller gaps are left for P2P peers.
    ///
    /// Default: `4`.
    pub webseed_min_gap_pieces: u32,
    /// Maximum bytes per web seed HTTP Range request.
    /// BEP 19 suggests ~5% of total file size.
    ///
    /// Default: `5 * 1024 * 1024` (5 MB).
    pub webseed_max_range_bytes: u64,
    /// Maximum concurrent web seed HTTP Range requests per torrent.
    /// Limits memory usage (each request buffers up to
    /// `webseed_max_range_bytes`) and prevents overwhelming web seed
    /// servers. UCB multi-armed bandit selection saturates the
    /// available slots with the best-performing URLs.
    ///
    /// Default: `8`. Range: `1..=16`.
    pub webseed_concurrency: usize,

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
    pub node_id: Option<InfoHash>,
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
            lsd_enabled: true,
            lsd_interval: Duration::from_secs(300),
            peer_msg_buffer_size: 256,
            enable_utp: true,
            webseed_enabled: true,
            webseed_min_gap_pieces: 4,
            webseed_max_range_bytes: 5 * 1024 * 1024,
            webseed_concurrency: 8,
            bootstrap_nodes: Some(vec![
                BootstrapNode::from(("router.bittorrent.com", 6881)),
                BootstrapNode::from(("dht.transmissionbt.com", 6881)),
            ]),
            bootstrap_nodes_v6: None,
            node_id: None,
        }
    }
}

/// Status of a torrent, exposed via the public API.
///
/// A snapshot of the torrent's current state.  Updated internally by
/// the swarm loop at ~1-second intervals; consumers should poll
/// [`Session::torrent_status`](super::Session::torrent_status) or
/// subscribe to [`TorrentEvent`]s for state transitions.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TorrentStatus {
    /// The 20-byte info hash.
    pub info_hash: InfoHash,
    /// Display name of the torrent.
    pub name: String,
    /// Download progress (0.0 to 1.0).
    pub progress: f64,
    /// Download rate in bytes per second (instantaneous, ≈1 s window).
    pub download_rate: f64,
    /// Upload rate in bytes per second (instantaneous, ≈1 s window).
    pub upload_rate: f64,
    /// Number of connected peers.
    pub num_peers: usize,
    /// Number of seeders (peers with 100% completion).
    pub num_seeds: usize,
    /// Current state of the torrent.
    pub state: TorrentState,
    /// Total size of the torrent content in bytes.
    pub total_size: u64,
    /// Cumulative bytes downloaded and verified (SHA-1 passed).
    pub total_downloaded: u64,
    /// Cumulative bytes uploaded to peers.
    pub total_uploaded: u64,
    /// Total number of pieces in this torrent.
    pub num_pieces: u32,
    /// Number of pieces that have been downloaded and verified.
    pub pieces_completed: u32,
    /// Per-piece completion bitmap: `true` at index `i` means piece `i`
    /// has been downloaded and verified.
    pub bitfield: Vec<bool>,
    /// Elapsed time since the torrent was registered with the session.
    ///
    /// Updated each status tick (~1 s).  Use for UI display
    /// (e.g. "Downloading for 5m 32s").
    pub elapsed: Duration,
    /// Human-readable error description when `state` is [`TorrentState::Error`].
    ///
    /// `None` in all other states.  Reserved for future error propagation;
    /// currently never populated by the swarm loop.
    pub error_message: Option<String>,
    /// Cumulative bytes received that failed SHA-1 verification (corrupt data)
    /// or were duplicate.  Includes both P2P and web seed sources.
    pub total_wasted: u64,
    /// Whether super seeding mode (BEP 16) is active for this torrent.
    pub super_seed_active: bool,
    /// Number of pieces not yet revealed to the swarm in super seeding mode.
    pub super_seed_remaining: u32,
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
    /// Verifying existing files on disk (e.g. after restart).
    Checking,
    /// An unrecoverable error occurred — see [`TorrentStatus::error_message`].
    Error,
}

/// Discrete state-transition events emitted by a torrent's swarm loop.
///
/// Subscribe via [`Session::torrent_events`](super::Session::torrent_events).
///
/// # Event semantics
///
/// These events are *hints* — they notify consumers that something
/// changed so they can react immediately rather than waiting for the
/// next poll of [`TorrentStatus`].  The [`broadcast`] channel has a
/// bounded buffer; slow consumers may miss events and should always
/// use [`Session::torrent_status`](super::Session::torrent_status) as
/// the authoritative source of truth.
///
/// [`broadcast`]: tokio::sync::broadcast
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TorrentEvent {
    /// The torrent's lifecycle state changed.
    StateChanged {
        /// Previous state.
        from: TorrentState,
        /// New state.
        to: TorrentState,
    },
    /// A single piece was downloaded and verified (SHA-1 passed).
    PieceCompleted {
        /// Zero-based piece index.
        index: u32,
    },
    /// A piece failed SHA-1 verification (corrupt data).
    PieceFailed {
        /// Zero-based piece index.
        index: u32,
    },
    /// All pieces are complete — the torrent has finished downloading.
    TorrentFinished,
    /// A new peer connection was established.
    PeerConnected {
        /// IP address and port of the peer.
        addr: SocketAddr,
        /// Client name from the peer's LTEP handshake, if available.
        client_name: Option<String>,
    },
    /// A peer disconnected or was removed.
    PeerDisconnected {
        /// IP address and port of the peer.
        addr: SocketAddr,
    },
    /// A tracker announce succeeded.
    TrackerAnnounced {
        /// Number of new peers returned by the tracker.
        peers_found: usize,
    },
    /// A tracker announce failed.
    TrackerError {
        /// Human-readable error message.
        message: String,
    },
    /// A single file within a multi-file torrent reached 100% completion.
    FileCompleted {
        /// Path components of the completed file.
        path: Vec<String>,
    },
}

/// Aggregated status across all torrents in a [`Session`](super::Session).
///
/// Obtain via [`Session::session_status`](super::Session::session_status).
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SessionStatus {
    /// Sum of all torrents' instantaneous download rates (bytes/s).
    pub total_download_rate: f64,
    /// Sum of all torrents' instantaneous upload rates (bytes/s).
    pub total_upload_rate: f64,
    /// Sum of all torrents' cumulative downloaded bytes.
    pub total_downloaded: u64,
    /// Sum of all torrents' cumulative uploaded bytes.
    pub total_uploaded: u64,
    /// Number of torrents currently managed by the session.
    pub num_torrents: usize,
    /// Total number of connected peers across all torrents.
    pub num_connections: usize,
    /// Total number of nodes in the DHT routing table (0 if DHT is disabled).
    pub dht_nodes: usize,
}

/// Tracker communication status for a torrent.
///
/// Updated after each successful or failed tracker announce.
/// Before the first announce, most fields are at their defaults.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TrackerStatus {
    /// URL of the currently active tracker.
    pub url: String,
    /// Time remaining until the next scheduled announce, if known.
    pub next_announce_in: Option<Duration>,
    /// The announce interval requested by the tracker (seconds).
    pub announce_interval: Duration,
    /// Number of seeders reported in the last announce response.
    pub seeds_reported: u32,
    /// Number of leechers reported in the last announce response.
    pub leechers_reported: u32,
    /// Error message from the last failed announce, if any.
    pub last_error: Option<String>,
}

/// Per-peer status snapshot.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct PeerStatus {
    /// IP address and port of the peer.
    pub addr: SocketAddr,
    /// Client name and version from the peer's LTEP handshake (BEP 10 `v`).
    pub client_name: Option<String>,
    /// The peer's download progress (0.0 to 1.0), derived from its bitfield.
    pub progress: f64,
    /// Download rate from this peer in bytes/s (current ≈1 s window).
    pub download_rate: f64,
    /// Upload rate to this peer in bytes/s (current ≈1 s window).
    pub upload_rate: f64,
    /// Whether we are choked by this peer.
    pub am_choked: bool,
    /// Whether the peer is interested in downloading from us.
    pub peer_interested: bool,
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
        assert_eq!(back.lsd_enabled, config.lsd_enabled);
        assert_eq!(back.lsd_interval, config.lsd_interval);
        assert_eq!(back.pex_enabled, config.pex_enabled);
        assert_eq!(back.pex_interval, config.pex_interval);
        assert_eq!(back.peer_msg_buffer_size, config.peer_msg_buffer_size);
        assert_eq!(back.enable_utp, config.enable_utp);
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
            lsd_enabled: false,
            lsd_interval: Duration::from_secs(600),
            peer_msg_buffer_size: 512,
            webseed_enabled: true,
            webseed_min_gap_pieces: 8,
            webseed_max_range_bytes: 10 * 1024 * 1024,
            webseed_concurrency: 10,
            enable_utp: false,
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
        assert_eq!(back.webseed_enabled, true);
        assert_eq!(back.webseed_min_gap_pieces, 8);
        assert_eq!(back.webseed_max_range_bytes, 10 * 1024 * 1024);
        assert_eq!(back.webseed_concurrency, 10);
        assert_eq!(back.enable_utp, false);
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
            total_size: 10_485_760,
            total_downloaded: 7_864_320,
            total_uploaded: 512_000,
            num_pieces: 40,
            pieces_completed: 30,
            bitfield: vec![true; 30]
                .into_iter()
                .chain(std::iter::repeat(false).take(10))
                .collect(),
            elapsed: Duration::from_secs(120),
            total_wasted: 0,
            super_seed_active: false,
            super_seed_remaining: 0,
            error_message: None,
        };
        let json = serde_json::to_string(&status).unwrap();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["name"], "test.iso");
        assert!((v["progress"].as_f64().unwrap() - 0.75).abs() < 0.001);
        assert_eq!(v["num_peers"], 12);
        assert_eq!(v["num_seeds"], 3);
        assert_eq!(v["state"], "Downloading");
        assert_eq!(v["total_size"], 10_485_760);
        assert_eq!(v["total_downloaded"], 7_864_320);
        assert_eq!(v["num_pieces"], 40);
        assert_eq!(v["pieces_completed"], 30);
        assert_eq!(v["bitfield"].as_array().unwrap().len(), 40);
        assert_eq!(v["elapsed"]["secs"], 120);
        assert_eq!(v["total_wasted"], 0);
        assert_eq!(v["super_seed_active"], false);
        assert_eq!(v["super_seed_remaining"], 0);
    }

    #[test]
    fn torrent_state_roundtrip() {
        let states = [
            TorrentState::Registered,
            TorrentState::Downloading,
            TorrentState::Seeding,
            TorrentState::Paused,
            TorrentState::Checking,
            TorrentState::Error,
        ];
        for &state in &states {
            let json = serde_json::to_string(&state).unwrap();
            let back: TorrentState = serde_json::from_str(&json).unwrap();
            assert_eq!(back, state);
        }
    }
}
