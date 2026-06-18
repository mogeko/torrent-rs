//! Integration tests for the Session API.

use torrent::bencode::{Bencode, Bytes, encode};
use torrent::error::{Error, ErrorKind};
use torrent::session::{Session, SessionConfig, TorrentState};

/// Helper: build synthetic single-file .torrent data.
fn make_single_file_torrent() -> Vec<u8> {
    let info_dict = Bencode::Dict(vec![
        (Bytes::from("name"), Bencode::Bytes(Bytes::from("test.txt"))),
        (Bytes::from("piece length"), Bencode::Integer(16384)),
        (Bytes::from("length"), Bencode::Integer(1024)),
        (
            Bytes::from("pieces"),
            Bencode::Bytes(Bytes::from(vec![0u8; 20])),
        ),
    ]);
    let root = Bencode::Dict(vec![
        (
            Bytes::from("announce"),
            Bencode::Bytes(Bytes::from("http://tracker.example.com/announce")),
        ),
        (Bytes::from("info"), info_dict),
    ]);
    encode(&root)
}

#[tokio::test]
async fn session_config_defaults() {
    let cfg = SessionConfig::default();
    assert_eq!(cfg.listen_port, 6881);
    assert_eq!(cfg.max_connections, 50);
    assert_eq!(cfg.max_uploads, 8);
    assert_eq!(cfg.download_dir, std::path::PathBuf::from("."));
    assert!(cfg.bootstrap_nodes.is_some());
}

#[tokio::test]
async fn session_new_with_temp_dir() -> Result<(), Error> {
    let tmp = tempfile::tempdir().unwrap();
    let config = SessionConfig {
        download_dir: tmp.path().to_path_buf(),
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;
    assert!(session.active_torrents().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn add_torrent_bytes_and_query_status() -> Result<(), Error> {
    let tmp = tempfile::tempdir().unwrap();
    let config = SessionConfig {
        download_dir: tmp.path().to_path_buf(),
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let data = make_single_file_torrent();
    let info_hash = session.add_torrent_bytes(&data).await?;

    // Verify the torrent appears in active list
    let active = session.active_torrents().await;
    assert_eq!(active.len(), 1);
    assert_eq!(active[0], info_hash);

    // Query its status
    let status = session.torrent_status(&info_hash).await?;
    assert_eq!(status.info_hash, info_hash);
    assert_eq!(status.name, "test.txt");
    assert_eq!(status.state, TorrentState::Queued);
    assert_eq!(status.progress, 0.0);

    Ok(())
}

#[tokio::test]
async fn remove_torrent() -> Result<(), Error> {
    let tmp = tempfile::tempdir().unwrap();
    let config = SessionConfig {
        download_dir: tmp.path().to_path_buf(),
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let data = make_single_file_torrent();
    let info_hash = session.add_torrent_bytes(&data).await?;
    assert_eq!(session.active_torrents().await.len(), 1);

    session.remove_torrent(&info_hash).await?;
    assert!(session.active_torrents().await.is_empty());

    // Querying a removed torrent should fail
    let err = session.torrent_status(&info_hash).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);

    Ok(())
}

#[tokio::test]
async fn add_and_query_multiple_times() -> Result<(), Error> {
    let tmp = tempfile::tempdir().unwrap();
    let config = SessionConfig {
        download_dir: tmp.path().to_path_buf(),
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let data = make_single_file_torrent();
    let info_hash = session.add_torrent_bytes(&data).await?;

    // Query status multiple times — should be consistent
    for _ in 0..3 {
        let status = session.torrent_status(&info_hash).await?;
        assert_eq!(status.info_hash, info_hash);
        assert_eq!(status.state, TorrentState::Queued);
    }

    Ok(())
}

// --- Magnet URI tests ---

#[tokio::test]
async fn add_magnet_str_minimal() -> Result<(), Error> {
    let tmp = tempfile::tempdir().unwrap();
    let config = SessionConfig {
        download_dir: tmp.path().to_path_buf(),
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let info_hash = session
        .add_magnet_str("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567")
        .await?;

    let active = session.active_torrents().await;
    assert_eq!(active.len(), 1);
    assert_eq!(active[0], info_hash);

    let status = session.torrent_status(&info_hash).await?;
    assert_eq!(status.info_hash, info_hash);
    assert_eq!(status.state, TorrentState::Queued);
    assert_eq!(status.progress, 0.0);

    Ok(())
}

#[tokio::test]
async fn add_magnet_str_with_trackers() -> Result<(), Error> {
    let tmp = tempfile::tempdir().unwrap();
    let config = SessionConfig {
        download_dir: tmp.path().to_path_buf(),
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let info_hash = session
        .add_magnet_str(
            "magnet:?xt=urn:btih:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\
             &tr=http://t1.com/ann&tr=http://t2.com/ann",
        )
        .await?;

    assert!(session.active_torrents().await.contains(&info_hash));
    let status = session.torrent_status(&info_hash).await?;
    assert_eq!(status.state, TorrentState::Queued);

    Ok(())
}

#[tokio::test]
async fn add_magnet_str_invalid() -> Result<(), Error> {
    let tmp = tempfile::tempdir().unwrap();
    let config = SessionConfig {
        download_dir: tmp.path().to_path_buf(),
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    // No xt parameter — should fail
    let result = session.add_magnet_str("magnet:?dn=no_hash").await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn add_magnet_str_with_display_name() -> Result<(), Error> {
    let tmp = tempfile::tempdir().unwrap();
    let config = SessionConfig {
        download_dir: tmp.path().to_path_buf(),
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let info_hash = session
        .add_magnet_str("magnet:?xt=urn:btih:cccccccccccccccccccccccccccccccccccccccc&dn=Test+File")
        .await?;

    let status = session.torrent_status(&info_hash).await?;
    assert_eq!(status.name, "Test+File");

    Ok(())
}

#[tokio::test]
async fn magnet_via_add_torrent() -> Result<(), Error> {
    use std::str::FromStr;
    use torrent::magnet::MagnetUri;

    let tmp = tempfile::tempdir().unwrap();
    let config = SessionConfig {
        download_dir: tmp.path().to_path_buf(),
        bootstrap_nodes: None,
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let magnet = MagnetUri::from_str(
        "magnet:?xt=urn:btih:dddddddddddddddddddddddddddddddddddddddd&dn=Direct",
    )
    .unwrap();
    let info_hash = *magnet.primary_info_hash();

    session.add_torrent(magnet).await?;

    let status = session.torrent_status(&info_hash).await?;
    assert_eq!(status.name, "Direct");

    Ok(())
}
