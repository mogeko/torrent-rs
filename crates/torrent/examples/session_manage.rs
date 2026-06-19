//! Manage torrent downloads via the Session API.
//!
//! Demonstrates the full lifecycle: configure a [`Session`], add a torrent
//! from a `.torrent` file, query status, list active torrents, and remove.
//! Uses the bundled Debian 13.5 torrent file.
//!
//! Run with: `cargo run -p torrent --example session_manage`

use torrent::session::{Session, SessionConfig};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // 1. Create a temporary download directory.
    let download_dir = tempfile::tempdir()?;
    let config = SessionConfig::default();

    // 2. Create the session — this is the main entry point.
    let session = Session::new(config).await?;
    println!(
        "Session created (port: {})",
        SessionConfig::default().listen_port
    );

    // 3. Add a torrent from a real .torrent file (bundled at compile time).
    let data = include_bytes!("data/debian-13.5.0-amd64-netinst.iso.torrent");
    let info_hash = session.add_torrent_bytes(data, download_dir.path()).await?;
    println!("\nTorrent added:");
    println!("  info_hash: {:02x?}", info_hash);

    // 4. Query the torrent's status.
    let status = session.torrent_status(&info_hash).await?;
    println!("  name:      {}", status.name);
    println!("  progress:  {:.1}%", status.progress * 100.0);
    println!("  state:     {:?}", status.state);
    println!("  peers:     {}", status.num_peers);

    // 5. List all active torrents.
    let active = session.active_torrents().await;
    println!("\nActive torrents: {}", active.len());
    for ih in &active {
        let s = session.torrent_status(ih).await?;
        println!("  - {} ({:.1}%)", s.name, s.progress * 100.0);
    }

    // 6. Remove the torrent when done.
    session.remove_torrent(&info_hash).await?;
    println!("\nTorrent removed.");
    println!("Active torrents: {}", session.active_torrents().await.len());

    Ok(())
}
