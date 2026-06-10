//! Perform a real tracker announce and print the peer list.
//!
//! Uses the bundled Ubuntu 26.04 torrent for the info_hash.
//! Tries UDP first (no TLS needed).
//!
//! Run with: `cargo run -p torrent --example tracker_announce`

use torrent::metainfo::from_bytes;
use torrent::peer::PeerId;
use torrent::tracker::{AnnounceEvent, AnnounceRequest, UdpTracker};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse the real torrent to get info_hash
    let data = include_bytes!("data/ubuntu-26.04-live-server-amd64.iso.torrent");
    let meta = from_bytes(data)?;
    let info_hash = meta.info_hash();

    println!("Torrent:  ubuntu-26.04-live-server-amd64.iso");
    println!("Info hash: {:02x?}", info_hash);

    // Build an announce request
    let req = AnnounceRequest {
        info_hash,
        peer_id: PeerId::random(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: 0,
        event: AnnounceEvent::Started,
        compact: true,
        numwant: Some(50),
        key: None,
        trackerid: None,
    };

    // --- UDP Tracker (BEP 15) ---
    // Plain UDP — no TLS needed. Uses a public tracker.
    println!("\n=== UDP Tracker Announce ===");
    let tracker = UdpTracker::new("udp://93.158.213.92:1337")?;

    match tracker.announce(&req).await {
        Ok(resp) => {
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
        Err(e) => {
            eprintln!("UDP announce failed: {}", e);
            eprintln!("(Public trackers may be offline or reject unknown info_hashes)");
        }
    }

    Ok(())
}
