//! Download a torrent using web seeds (BEP 19).
//!
//! Uses the bundled Arch Linux 2026.06.01 torrent which includes
//! `url-list` — HTTP mirrors that act as permanent seeds. The session
//! automatically downloads from both P2P peers and web seed mirrors.
//!
//! Requires internet to reach web seed mirrors and trackers.
//!
//! Run with: `cargo run -p torrent --example webseed_download`

use std::path::PathBuf;
use std::time::{Duration, Instant};

use torrent::metainfo::Metainfo;
use torrent::session::{Session, SessionConfig, TorrentState};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // ── Setup ────────────────────────────────────────────────────

    let download_dir = PathBuf::from("crates/torrent/examples/data");
    std::fs::create_dir_all(&download_dir)?;

    // Clean up residual file from a previous run
    let iso_path = download_dir.join("archlinux-2026.06.01-x86_64.iso");
    if iso_path.exists() {
        std::fs::remove_file(&iso_path)?;
        println!("Cleaned up residual file: {}", iso_path.display());
    }

    let session = Session::new(SessionConfig::default()).await?;
    println!(
        "Session created (port: {})",
        SessionConfig::default().listen_port
    );

    // ── Parse and display torrent info ────────────────────────────

    let data = include_bytes!("data/archlinux-2026.06.01-x86_64.iso.torrent");
    let meta = Metainfo::try_from(data)?;

    let total_mb = meta.info.total_size() / 1024 / 1024;
    let num_pieces = meta.info.num_pieces();
    let piece_kb = meta.info.piece_length / 1024;

    let name = match &meta.info.mode {
        torrent::metainfo::Mode::Single { name, .. }
        | torrent::metainfo::Mode::Multiple { name, .. } => name.as_str(),
    };

    println!("\n=== Torrent ===");
    println!("  Name:    {}", name);
    println!("  Size:    {} MB", total_mb);
    println!("  Pieces:  {} ({} KB each)", num_pieces, piece_kb);
    println!("  Tracker: {}", meta.announce);
    println!("  Web seeds: {} (BEP 19 url-list)", meta.url_list.len());

    // Show a few web seed URLs
    for (i, url) in meta.url_list.iter().enumerate().take(3) {
        println!("    [{}] {}", i, url);
    }
    if meta.url_list.len() > 3 {
        println!("    ... and {} more", meta.url_list.len() - 3);
    }

    // ── Start download ────────────────────────────────────────────

    let info_hash = session
        .add_torrent_bytes(data)?
        .download_dir(&download_dir)
        .start()
        .await?;

    println!("\nDownloading from P2P peers + web seeds...");
    println!("(Ctrl+C to stop)\n");

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
        let downloaded = (meta.info.total_size() as f64 * status.progress) as u64;
        let rate = downloaded.saturating_sub(last_bytes) / poll_interval.as_secs();
        last_bytes = downloaded;

        let elapsed = start.elapsed().as_secs();

        println!(
            "  {:5.1}% | {:>3} peers | ↓ {:>6} KB/s | {:>5} / {:>5} MB | {:>5}s",
            pct,
            status.num_peers,
            rate / 1024,
            downloaded / (1024 * 1024),
            total_mb,
            elapsed,
        );

        match status.state {
            TorrentState::Seeding => {
                let elapsed = start.elapsed().as_secs();
                println!("\n✓ Download complete! Took {}s", elapsed);
                println!("  File: {}", iso_path.display());
                break;
            }
            TorrentState::Error => {
                eprintln!("\n✗ Torrent entered error state");
                break;
            }
            _ => {}
        }
    }

    // ── Cleanup ───────────────────────────────────────────────────

    session.remove_torrent(&info_hash).await?;
    println!("Torrent removed.");

    Ok(())
}
