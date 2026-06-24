//! Create a torrent from a file and seed it.
//!
//! Demonstrates the full seeding workflow: hash a file → produce
//! `.torrent` bytes and a magnet URI → start seeding to the swarm.
//! Run with: `cargo run -p torrent --example seed_torrent`

use std::io::Write as _;
use std::time::Duration;

use torrent::session::{Session, SessionConfig, TorrentState};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // ── Step 1: Create a temporary file to seed ──

    let dir = tempfile::tempdir()?;
    let file_path = dir.path().join("hello.txt");
    let data = b"Hello, BitTorrent! This is a test file for seeding.\n";

    {
        let mut f = std::fs::File::create(&file_path)?;
        f.write_all(data)?;
    }

    println!(
        "Created test file: {} ({} bytes)",
        file_path.display(),
        data.len()
    );

    // ── Step 2: Create a Session ──

    let config = SessionConfig::default();
    let session = Session::new(config).await?;

    // ── Step 3: Hash the file and produce a PreparedTorrent ──

    let prepared = session
        .seed_from(file_path.clone())
        .piece_length(32) // Small pieces for demo
        .announce("http://tracker.example.com:6969/announce")
        .comment("A test torrent created by torrent-rs")
        .hash()
        .await?;

    // ── Step 4: Export .torrent bytes and magnet URI ──

    let torrent_path = dir.path().join("hello.torrent");
    std::fs::write(&torrent_path, prepared.torrent_bytes())?;
    println!("\nWritten .torrent file: {}", torrent_path.display());
    println!("Magnet URI: {}", prepared.magnet_uri());
    println!("Info hash:  {:02x?}", prepared.info_hash());

    // ── Step 5: Start seeding ──

    let info_hash = session.start_seeding(prepared).await?;

    // ── Step 6: Verify it's registered ──

    let status = session.torrent_status(&info_hash).await?;
    println!("\nTorrent registered: {}", status.name);
    println!("State: {:?}", status.state);

    // ── Step 7: Retrieve metadata via Session API ──

    let meta = session.metainfo(&info_hash).expect("metadata unavailable");
    println!("Retrieved from session: {} pieces", meta.info.num_pieces());

    let magnet = session
        .magnet_uri(&info_hash)
        .expect("metadata unavailable");
    println!("Magnet from session: {magnet}");

    // ── Step 8: Keep seeding until Ctrl+C ──

    println!("\nSeeding... (Ctrl+C to stop)\n");
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        let status = match session.torrent_status(&info_hash).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Status error: {}", e);
                continue;
            }
        };

        let state = if status.state == TorrentState::Seeding {
            "Seeding"
        } else {
            "Waiting"
        };
        println!(
            "  {state} | peers: {} | up: {:.1} KB/s",
            status.num_peers,
            status.upload_rate / 1024.0
        );
    }
}
