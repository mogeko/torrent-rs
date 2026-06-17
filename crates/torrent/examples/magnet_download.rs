//! Parse a magnet URI and add it to a Session.
//!
//! Demonstrates BEP 9 magnet link support. The session will
//! attempt to discover metadata from peers (BEP 10).
//! Run with: `cargo run -p torrent --example magnet_download`

use std::path::PathBuf;
use std::str::FromStr;

use torrent::magnet::MagnetUri;
use torrent::session::{Session, SessionConfig};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Example magnet URI — replace with a real one to test
    let uri_str = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
        &dn=Example+File\
        &tr=http://tracker.example.com/announce\
        &xl=1024";

    // 1. Parse the magnet URI
    let magnet = MagnetUri::from_str(uri_str)?;
    println!("=== Magnet URI ===");
    println!("Info hash:  {:02x?}", magnet.primary_info_hash());
    if let Some(ref name) = magnet.display_name {
        println!("Name:       {}", name);
    }
    if let Some(len) = magnet.exact_length {
        println!("Size:       {} bytes", len);
    }
    println!("Trackers:   {}", magnet.trackers.len());
    for tr in &magnet.trackers {
        println!("  - {}", tr);
    }

    // 2. Re-serialize to verify round-trip
    println!("\n=== Round-trip ===");
    println!("{}", magnet);

    // 3. Create a session and add the torrent
    println!("\n=== Session ===");
    let config = SessionConfig {
        download_dir: PathBuf::from("/tmp/torrent-downloads"),
        enable_dht: false, // Disable DHT for this example
        ..Default::default()
    };

    let session = Session::new(config).await?;

    // Add via magnet URI string (convenience)
    let info_hash = session.add_magnet_str(uri_str).await?;
    println!("Added torrent: {:02x?}", info_hash);

    // Query status
    let status = session.torrent_status(&info_hash).await?;
    println!("Name:     {}", status.name);
    println!("State:    {:?}", status.state);

    // Clean up
    session.remove_torrent(&info_hash).await?;
    println!("Removed.");

    Ok(())
}
