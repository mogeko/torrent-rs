//! Parse a .torrent file and inspect its metadata.
//!
//! Uses a real Ubuntu 26.04 torrent file bundled in `examples/data/`.
//! Run with: `cargo run -p torrent --example parse_metainfo`

use torrent::metainfo::from_bytes;

fn main() {
    // Load a real .torrent file (embedded at compile time)
    let data = include_bytes!("data/ubuntu-26.04-live-server-amd64.iso.torrent");

    // Parse it
    let meta = from_bytes(data).expect("failed to parse .torrent file");

    println!("=== Torrent Metadata ===");
    println!("Tracker URL:    {}", meta.announce);
    println!("Info hash:      {:02x?}", meta.info_hash());
    println!("Piece length:   {} bytes", meta.info.piece_length);
    println!("Total size:     {} bytes", meta.info.total_size());
    println!("Number of pieces: {}", meta.info.num_pieces());

    // Optional fields
    if let Some(date) = meta.creation_date {
        println!("Created:        {}", date);
    }
    if let Some(ref comment) = meta.comment {
        println!("Comment:        {}", comment);
    }
    if let Some(ref created_by) = meta.created_by {
        println!("Created by:     {}", created_by);
    }

    // Announce tiers (BEP 12 multi-tracker)
    if !meta.announce_list.is_empty() {
        println!();
        println!("=== Tracker Tiers ===");
        for (i, tier) in meta.announce_list.iter().enumerate() {
            println!("  Tier {}:", i);
            for url in tier {
                println!("    {}", url);
            }
        }
    }

    // File layout
    match &meta.info.mode {
        torrent::metainfo::Mode::Single { name, length } => {
            println!();
            println!("=== File Layout (single-file) ===");
            println!("  {} ({} bytes)", name, length);
        }
        torrent::metainfo::Mode::Multiple { name, files } => {
            println!();
            println!("=== File Layout (multi-file) ===");
            println!("  Root: {}", name);
            for f in files {
                println!("  {} ({} bytes)", f.path.join("/"), f.length);
            }
        }
    }
}
