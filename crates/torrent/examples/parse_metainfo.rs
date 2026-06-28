//! Parse a .torrent file and inspect its metadata.
//!
//! Uses a real Arch Linux 2026.06.01 torrent file bundled in `examples/data/`.
//! This torrent includes BEP 19 `url-list` (web seed) URLs.
//! Run with: `cargo run -p torrent --example parse_metainfo`

use torrent::metainfo::{Metainfo, Mode};
use tracing_subscriber::EnvFilter;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Load a real .torrent file (embedded at compile time)
    let data = include_bytes!("data/archlinux-2026.06.01-x86_64.iso.torrent");

    // Parse it
    let meta = Metainfo::try_from(data).expect("failed to parse .torrent file");

    println!("=== Torrent Metadata ===");
    println!("Tracker URL:    {}", meta.announce);
    println!("Info hash:      {:02x?}", meta.info_hash());
    println!("Piece length:   {} bytes", meta.info.piece_length);
    println!(
        "Total size:     {} MB",
        meta.info.total_size() / 1024 / 1024
    );
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

    // Web seed URLs (BEP 19)
    if !meta.url_list.is_empty() {
        println!();
        println!("=== Web Seeds (BEP 19 url-list) ===");
        println!("  Count: {}", meta.url_list.len());
        for (i, url) in meta.url_list.iter().enumerate() {
            if i < 5 || i >= meta.url_list.len().saturating_sub(3) {
                println!("  [{}] {}", i, url);
            } else if i == 5 {
                println!("  ... ({} more)", meta.url_list.len() - 8);
            }
        }
    }

    // HTTP seeds (BEP 17)
    if !meta.httpseeds.is_empty() {
        println!();
        println!("=== HTTP Seeds (BEP 17 httpseeds) ===");
        for url in &meta.httpseeds {
            println!("  {}", url);
        }
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
        Mode::Single { name, length } => {
            println!();
            println!("=== File Layout (single-file) ===");
            println!("  {} ({} MB)", name, length / 1024 / 1024);
        }
        Mode::Multiple { name, files } => {
            println!();
            println!("=== File Layout (multi-file) ===");
            println!("  Root: {}", name);
            for f in files {
                println!("  {} ({} bytes)", f.path.join("/"), f.length);
            }
        }
    }
}
