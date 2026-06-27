//! Integration tests for Local Service Discovery (LSD, BEP 14).

use std::net::Ipv4Addr;
use std::time::Duration;

use torrent::error::Error;
use torrent::peer::lsd::{LSD_IPV4_MULTICAST, LSD_PORT, LsdAnnounce, LsdHost};
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

// ── LsdAnnounce serialization tests (cross-crate round-trip) ─────────────

#[test]
fn lsd_announce_multi_infohash_roundtrip() {
    let mut announce = LsdAnnounce::new(LsdHost::V4, 6881);
    announce.info_hashes.push([0xAA; 20]);
    announce.info_hashes.push([0xBB; 20]);
    announce.info_hashes.push([0xCC; 20]);

    let bytes = announce
        .to_bytes()
        .expect("multi-infohash should produce bytes");
    let parsed = LsdAnnounce::from_bytes(&bytes).expect("should parse multi-infohash");
    assert_eq!(announce, parsed);
    assert_eq!(parsed.info_hashes.len(), 3);
}

#[test]
fn lsd_announce_v6_roundtrip() {
    let mut announce = LsdAnnounce::new(LsdHost::V6, 9999);
    announce.info_hashes.push([0x42; 20]);
    announce.cookie = Some("test-cookie".to_string());

    let bytes = announce
        .to_bytes()
        .expect("v6 announce should produce bytes");
    let parsed = LsdAnnounce::from_bytes(&bytes).expect("should parse v6 announce");
    assert_eq!(announce, parsed);
    assert_eq!(parsed.host, LsdHost::V6);
    assert_eq!(parsed.port, 9999);
    assert_eq!(parsed.cookie.as_deref(), Some("test-cookie"));
}

#[test]
fn lsd_announce_truncation() {
    // Create an announce with many info_hashes to trigger 1400-byte limit
    let mut announce = LsdAnnounce::new(LsdHost::V4, 6881);
    // Each infohash line adds ~52 bytes. With ~30 infohashes we should exceed 1400.
    for i in 0u8..50 {
        let mut ih = [0u8; 20];
        ih[0] = i;
        announce.info_hashes.push(ih);
    }

    let bytes = announce.to_bytes().expect("should produce bytes");
    assert!(
        bytes.len() <= 1400,
        "announce should not exceed 1400 bytes (got {})",
        bytes.len()
    );

    // Parsing back should yield fewer infohashes than original
    let parsed = LsdAnnounce::from_bytes(&bytes).expect("should parse truncated announce");
    assert!(
        parsed.info_hashes.len() < announce.info_hashes.len(),
        "truncation should reduce infohash count"
    );
}

#[test]
fn lsd_announce_malformed_does_not_panic() {
    // Random garbage should not panic — should just return Err.
    let garbage = b"not an lsd message at all just some random bytes";
    assert!(LsdAnnounce::from_bytes(garbage).is_err());

    // Empty bytes
    assert!(LsdAnnounce::from_bytes(b"").is_err());

    // Valid prefix but garbage after
    let prefix_only = b"BT-SEARCH * HTTP/1.1\r\n\r\n";
    assert!(LsdAnnounce::from_bytes(prefix_only).is_err());
}

#[test]
fn lsd_announce_forward_compat_unknown_headers() {
    // BEP 14: unknown headers must be ignored for forward compatibility
    let mut data = Vec::new();
    data.extend_from_slice(b"BT-SEARCH * HTTP/1.1\r\n");
    data.extend_from_slice(b"Host: 239.192.152.143:6771\r\n");
    data.extend_from_slice(b"Port: 6881\r\n");
    data.extend_from_slice(b"Infohash: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n");
    data.extend_from_slice(b"X-Future-Extension: some-future-value\r\n");
    data.extend_from_slice(b"Another-Unknown: 12345\r\n");
    data.extend_from_slice(b"\r\n");

    let parsed = LsdAnnounce::from_bytes(&data).expect("should ignore unknown headers");
    assert_eq!(parsed.host, LsdHost::V4);
    assert_eq!(parsed.port, 6881);
    assert_eq!(parsed.info_hashes.len(), 1);
}

// ── Constant verification ───────────────────────────────────────────────

#[test]
fn lsd_multicast_constants_match_bep14() {
    // BEP 14: 239.192.152.143:6771 (org-local)
    assert_eq!(LSD_IPV4_MULTICAST, Ipv4Addr::new(239, 192, 152, 143));
    assert_eq!(LSD_PORT, 6771);
}
