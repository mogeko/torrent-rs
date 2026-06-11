//! Perform a real tracker announce and print the peer list.
//!
//! Uses the bundled Ubuntu 26.04 torrent's built-in tracker list.
//! Demonstrates single and multi-tracker announce via the unified [`Tracker`] API.
//!
//! **Note**: The Ubuntu torrent only has HTTPS trackers, but the current
//! `HttpTracker` supports TLS (`tokio-rustls`). Falls back to a public UDP tracker
//! to demonstrate actual network activity.
//!
//! Run with: `cargo run -p torrent --example tracker_announce`

use torrent::metainfo::from_bytes;
use torrent::peer::PeerId;
use torrent::tracker::{AnnounceEvent, AnnounceRequest, Tracker};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse the real torrent to get info_hash and tracker URLs
    let data = include_bytes!("data/ubuntu-26.04-live-server-amd64.iso.torrent");
    let meta = from_bytes(data)?;
    let info_hash = meta.info_hash();

    println!("Torrent:  ubuntu-26.04-live-server-amd64.iso");
    println!("Info hash: {:02x?}", info_hash);

    // Build an announce request
    let mut req = AnnounceRequest::new(info_hash, PeerId::random(), 6881);
    req.event = AnnounceEvent::Started;

    // --- Fallback: public UDP tracker ---
    // The Ubuntu torrent only has HTTPS trackers, so we add a public UDP
    // tracker to demonstrate actual announce activity.
    let public_udp = "udp://tracker.opentrackr.org:1337";
    // --- Public UDP tracker ---
    println!("\n=== Public UDP Tracker (Tracker::single) ===");
    println!("URL: {}", public_udp);
    match Tracker::single(public_udp)?.announce(&req).await {
        Ok(resp) => print_response(&resp),
        Err(e) => {
            eprintln!("Public UDP tracker failed: {}", e);
            eprintln!("(Public trackers may be offline or reject unknown info_hashes)");
        }
    }

    Ok(())
}

fn print_response(resp: &torrent::tracker::AnnounceResponse) {
    println!("Interval:     {}s", resp.interval);
    if let Some(min) = resp.min_interval {
        println!("Min interval: {}s", min);
    }
    println!("Seeders:      {}", resp.complete);
    println!("Leechers:     {}", resp.incomplete);
    println!("Peers found:  {}", resp.peers.len());

    if let Some(ref warning) = resp.warning_message {
        println!("Warning:      {}", warning);
    }

    for (i, peer) in resp.peers.iter().take(10).enumerate() {
        println!("  peer {}: {}", i + 1, peer);
    }
    if resp.peers.len() > 10 {
        println!("  ... and {} more", resp.peers.len() - 10);
    }
}
