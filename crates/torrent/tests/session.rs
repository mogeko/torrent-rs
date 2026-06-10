//! Integration tests for the Session API.

use bytes::Bytes;
use torrent::bencode::{Bencode, encode};
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
    assert!(cfg.enable_dht);
}

#[tokio::test]
async fn session_new_with_temp_dir() -> Result<(), Error> {
    let tmp = tempfile::tempdir().unwrap();
    let config = SessionConfig {
        download_dir: tmp.path().to_path_buf(),
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
