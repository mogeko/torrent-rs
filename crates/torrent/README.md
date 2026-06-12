# torrent

[![MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../../LICENSE)

**High-level user-facing BitTorrent library.** Provides async I/O, session
management, tracker communication, and file storage — built on top of
[`torrent-core`](../torrent-core).

## Modules

| Module                     | Description                                      | BEP           |
| -------------------------- | ------------------------------------------------ | ------------- |
| [`session`](./src/session) | Session, download/upload loops, peer manager     | —             |
| [`peer`](./src/peer)       | Async `PeerConnection`, re-exports core types    | BEP 3         |
| [`tracker`](./src/tracker) | HTTP (manual) + UDP tracker announce             | BEP 3, 15, 23 |
| [`dht`](./src/dht)         | Async DHT RPC, query helpers (`find_node`, etc.) | BEP 5         |
| [`storage`](./src/storage) | `FileStorage` (async file I/O)                   | —             |

## Quick Start

```rust
use std::path::PathBuf;
use torrent::session::{Session, SessionConfig};

#[tokio::main]
async fn main() -> Result<(), torrent::error::Error> {
    let config = SessionConfig {
        download_dir: PathBuf::from("./downloads"),
        ..Default::default()
    };
    let session = Session::new(config).await?;

    let data = std::fs::read("ubuntu-24.04.torrent")?;
    let info_hash = session.add_torrent_bytes(&data).await?;

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

## Re-exports from `torrent-core`

All commonly used core types are re-exported so you only need `torrent` as a
dependency:

```rust
use torrent::bencode::{decode, encode, Bencode};
use torrent::metainfo::from_bytes;
use torrent::magnet::MagnetUri;
use torrent::error::{Error, ErrorKind};
use torrent::peer::{Handshake, PeerId, PeerMessage};
use torrent::storage::PieceManager;
```

## Examples

See the [`examples/`](./examples) directory for runnable scenario guides:

| Example                                                 | Scenario                                            |
| ------------------------------------------------------- | --------------------------------------------------- |
| [`parse_metainfo.rs`](./examples/parse_metainfo.rs)     | Parse a .torrent file and inspect metadata          |
| [`tracker_announce.rs`](./examples/tracker_announce.rs) | Query HTTP/UDP trackers for peer lists              |
| [`dht_discovery.rs`](./examples/dht_discovery.rs)       | Discover peers via the DHT (Kademlia)               |
| [`peer_connect.rs`](./examples/peer_connect.rs)         | Low-level peer wire protocol (handshake + messages) |

Run with:

```bash
cargo run -p torrent --example parse_metainfo
```

## Relationship with `torrent-core`

```
┌─────────────────────────────────────────────────────┐
│                   torrent (async)                    │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │ session  │ │ tracker  │ │ dht/rpc  │ │storage/ │ │
│  │          │ │ http/udp │ │ query    │ │fs       │ │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └───┬────┘ │
│       │            │            │            │      │
│       └────────────┴────────────┴────────────┘      │
│                        │                            │
│              depends on torrent_core                 │
│                        │                            │
├────────────────────────┼────────────────────────────┤
│              torrent-core (sync, no tokio)           │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │ bencode  │ │ metainfo │ │   peer   │ │ dht/   │ │
│  │ error    │ │ magnet   │ │ handshake│ │ krpc   │ │
│  │          │ │          │ │ message  │ │ routing│ │
│  └──────────┘ └──────────┘ └──────────┘ └────────┘ │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────────┐ │
│  │ tracker  │ │  piece   │ │ storage              │ │
│  │ data     │ │ manager  │ │ trait (read/write)   │ │
│  │ types    │ │ selector │ │                      │ │
│  └──────────┘ └──────────┘ └──────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

## Testing

```bash
cargo test -p torrent                    # Run all tests
cargo clippy -p torrent -- -D warnings   # Lint
```

## License

MIT — see [LICENSE](../../LICENSE).
