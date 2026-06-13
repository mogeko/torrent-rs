//! End-to-end torrent download using the full Session API.
//!
//! Downloads a real torrent (bundled Debian 13.5 server ISO) using tracker
//! announce, peer connections, piece selection, SHA-1 verification, and
//! disk I/O. Requires internet to reach trackers and peers.
//!
//! Run with: `cargo run -p torrent --example download_torrent`

use std::fs::canonicalize;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use torrent::session::{Session, SessionConfig, TorrentState};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // 1. Create a session. Files are saved to a `downloads` directory
    //    beside the torrent crate (created automatically if missing).
    let download_dir = PathBuf::from("crates/torrent/examples/data");
    std::fs::create_dir_all(&download_dir)?;

    // 2. Clean up any residual ISO from a previous run.
    let iso_path = download_dir.join("debian-13.5.0-amd64-netinst.iso");
    if iso_path.exists() {
        std::fs::remove_file(&iso_path)?;
        println!("Cleaned up residual file: {}", iso_path.display());
    }

    let config = SessionConfig {
        download_dir: download_dir.clone(),
        ..Default::default()
    };
    let session = Session::new(config).await?;
    println!(
        "Session created (port: {})",
        SessionConfig::default().listen_port
    );
    let abs_dir = canonicalize(&download_dir).unwrap_or_else(|_| download_dir.clone());
    println!("Download dir: {}", abs_dir.display());

    // 2. Add the bundled Debian 13.5 torrent.
    let data = include_bytes!("data/debian-13.5.0-amd64-netinst.iso.torrent");
    let info_hash = session.add_torrent_bytes(data).await?;
    let status = session.torrent_status(&info_hash).await?;
    let total_bytes = {
        let meta = torrent::metainfo::from_bytes(data)?;
        meta.info.total_size()
    };
    println!("\nTorrent: {}", status.name);
    println!("Size: {} MB ({} pieces)", total_bytes / (1024 * 1024), {
        let meta = torrent::metainfo::from_bytes(data)?;
        meta.info.num_pieces()
    });
    println!("Info hash: {:02x?}", &info_hash[..4]);

    // 3. Monitor progress until complete or error.
    println!("\nDownloading... (Ctrl+C to stop)\n");
    let start = Instant::now();
    let mut last_bytes = 0u64;
    let poll_interval = Duration::from_secs(2);

    loop {
        tokio::time::sleep(poll_interval).await;

        let status = match session.torrent_status(&info_hash).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Status error: {}", e);
                continue;
            }
        };

        let pct = status.progress * 100.0;
        let downloaded = (total_bytes as f64 * status.progress) as u64;
        let rate = downloaded.saturating_sub(last_bytes) / poll_interval.as_secs();
        last_bytes = downloaded;

        let elapsed = start.elapsed().as_secs();

        println!(
            "  {:5.1}% | {:>3} peers | ↓ {:>6} KB/s | ↓ {:>5} / {:>5} MB | {:>5}s elapsed",
            pct,
            status.num_peers,
            rate / 1024,
            downloaded / (1024 * 1024),
            total_bytes / (1024 * 1024),
            elapsed,
        );

        match status.state {
            TorrentState::Seeding => {
                let elapsed = start.elapsed().as_secs();
                println!("\n✓ Download complete! Took {}s", elapsed);
                println!("  File saved to: {}", abs_dir.display());
                break;
            }
            TorrentState::Error => {
                eprintln!("\n✗ Torrent entered error state");
                break;
            }
            _ => {}
        }
    }

    // 4. Cleanup.
    session.remove_torrent(&info_hash).await?;
    println!("Torrent removed.");

    Ok(())
}
