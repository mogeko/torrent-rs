//! Perform a real tracker announce and print the peer list.
//!
//! Uses the bundled Debian 13.5 torrent's built-in tracker list.
//! Demonstrates single and multi-tracker announce via the unified [`Tracker`] API.
//!
//! Run with: `cargo run -p torrent --example tracker_announce`

use torrent::metainfo::from_bytes;
use torrent::peer::PeerId;
use torrent::tracker::{AnnounceEvent, AnnounceRequest, Tracker};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse the real torrent to get info_hash and tracker URLs
    let data = include_bytes!("data/debian-13.5.0-amd64-netinst.iso.torrent");
    let meta = from_bytes(data)?;
    let info_hash = meta.info_hash();

    println!("Torrent:  debian-13.5.0-amd64-netinst.iso.torrent");
    println!("Info hash: {:02x?}", info_hash);

    // Build an announce request
    let mut req = AnnounceRequest::new(info_hash, PeerId::random(), 6881);
    req.event = AnnounceEvent::Started;

    // --- Tracker from Metainfo (collects announce + announce_list URLs) ---
    let Some(tracker) = Tracker::from_metainfo(&meta) else {
        eprintln!("no valid trackers found in metainfo");
        return Ok(());
    };
    println!("\n=== Built-in Trackers (Tracker::from_metainfo) ===");
    match tracker.announce(&req).await {
        Ok(resp) => print_response(&resp),
        Err(e) => eprintln!("Tracker failed: {}", e),
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
