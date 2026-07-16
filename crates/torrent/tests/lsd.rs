//! Integration tests for Local Service Discovery (LSD, BEP 14).
//!
//! Low-level LSD protocol type round-trip tests live in
//! `torrent-core` (`crates/torrent-core/src/peer/lsd.rs`).

use std::time::Duration;

use torrent::error::Error;
use torrent::session::{Session, SessionConfig};

// ── Session-level tests ─────────────────────────────────────────────────

#[tokio::test]
async fn session_with_lsd_enabled_does_not_panic() -> Result<(), Error> {
    let config = SessionConfig {
        bootstrap_nodes: None,
        lsd_enabled: true,
        lsd_interval: Duration::from_secs(300),
        ..Default::default()
    };
    let session = Session::new(config).await?;
    assert!(session.active_torrents().is_empty());
    Ok(())
}

#[tokio::test]
async fn session_with_lsd_disabled_does_not_panic() -> Result<(), Error> {
    let config = SessionConfig {
        bootstrap_nodes: None,
        lsd_enabled: false,
        ..Default::default()
    };
    let session = Session::new(config).await?;
    assert!(session.active_torrents().is_empty());
    Ok(())
}

#[tokio::test]
async fn lsd_defaults_are_bep14_compliant() {
    let cfg = SessionConfig::default();
    assert!(cfg.lsd_enabled, "LSD should be enabled by default");
    assert_eq!(
        cfg.lsd_interval,
        Duration::from_secs(300),
        "LSD announce interval should be 5 minutes per BEP 14"
    );
}

#[tokio::test]
async fn lsd_with_dht_disabled_does_not_panic() -> Result<(), Error> {
    // LSD should work independently — no tracker, no DHT, just LAN multicast.
    let config = SessionConfig {
        bootstrap_nodes: None,
        bootstrap_nodes_v6: None,
        lsd_enabled: true,
        ..Default::default()
    };
    let session = Session::new(config).await?;
    assert!(session.active_torrents().is_empty());
    Ok(())
}
