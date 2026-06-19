# torrent-rs

[![CI](https://github.com/mogeko/torrent.rs/actions/workflows/build+test.yml/badge.svg)](https://github.com/mogeko/torrent.rs/actions/workflows/build+test.yml)
[![MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A pure Rust BitTorrent library covering bencode, metainfo, peer wire protocol,
tracker communication, DHT, magnet links, piece management, and session
orchestration.

## Quick Start

```rust
use torrent::session::{Session, SessionConfig};

#[tokio::main]
async fn main() -> Result<(), torrent::error::Error> {
    let config = SessionConfig::default();
    let session = Session::new(config).await?;

    let data = std::fs::read("ubuntu-24.04.torrent")?;
    let info_hash = session.add_torrent_bytes(&data, "./downloads").await?;

    loop {
        let status = session.torrent_status(&info_hash).await?;
        println!("{:.1}% — {} peers", status.progress * 100.0, status.num_peers);
        if status.progress >= 1.0 {
            println!("Download complete!");
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }

    session.remove_torrent(&info_hash).await?;
    Ok(())
}
```

## Repository

This is a Cargo workspace with two crates:

| Crate                                   | Description                                           | Runtime       |
| --------------------------------------- | ----------------------------------------------------- | ------------- |
| [`torrent`](./crates/torrent)           | High-level API — add this to your `Cargo.toml`        | async (tokio) |
| [`torrent-core`](./crates/torrent-core) | Low-level abstractions — benext, metainfo, peer types | sync          |

See each crate's README for module-level documentation and examples.

## Build & Test

```bash
cargo build                 # Build all crates
cargo test                  # Run all 162 tests
cargo clippy -- -D warnings # Lint
```

## License

MIT — see [LICENSE](LICENSE).
