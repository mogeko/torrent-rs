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

### HTTP Tracker

```rust
use torrent::tracker::{AnnounceRequest, AnnounceEvent, HttpTracker};
use torrent::peer::PeerId;

let tracker = HttpTracker::new("http://tracker.example.com:6969/announce");
let req = AnnounceRequest {
    info_hash: [0x01; 20],
    peer_id: PeerId::random(),
    port: 6881,
    uploaded: 0,
    downloaded: 0,
    left: 1024,
    event: AnnounceEvent::Started,
    compact: true,
    numwant: Some(50),
    key: None,
    trackerid: None,
};
// let resp = tracker.announce(&req).await?;
```

### UDP Tracker

```rust
use torrent::tracker::UdpTracker;

let tracker = UdpTracker::new("udp://tracker.opentrackr.org:1337")?;
// let resp = tracker.announce(&req).await?;
```

### Async Peer Stream

```rust
use torrent::peer::PeerConnection;
use torrent::peer::PeerId;

let info_hash = [0u8; 20]; // target torrent
let peer_id = PeerId::random();
let addr = "192.168.1.42:6881".parse()?;

// let conn = PeerConnection::connect(addr, info_hash, peer_id).await?;
// conn.send(&PeerMessage::Interested).await?;
// let msg = conn.recv().await?;
```

### DHT RPC

```rust
use torrent::dht::DhtRpc;
use torrent::dht::krpc;

let rpc = DhtRpc::new("0.0.0.0:0".parse()?).await?;
let tid = rand::random();
let node_id = [0u8; 20];

// let response = rpc.ping(addr, tid, &node_id).await?;
// let nodes = torrent::dht::find_node(&rpc, addr, tid, &node_id, &target).await?;
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
│  ┌──────────┐ ┌──────────────────────────────────┐  │
│  │ tracker  │ │ storage                          │  │
│  │ data     │ │ trait, PieceManager, selectors   │  │
│  │ types    │ │                                  │  │
│  └──────────┘ └──────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

## Dependencies

| Crate          | Version | Purpose                                                  |
| -------------- | ------- | -------------------------------------------------------- |
| `torrent-core` | 0.1     | Core data types and algorithms                           |
| `tokio`        | 1       | Async runtime (net, rt, macros, time, io-util, fs, sync) |
| `bytes`        | 1       | Zero-copy byte buffers                                   |
| `sha1`         | 0.10    | SHA-1 hashing                                            |
| `rand`         | 0.8     | Random ID generation                                     |

## Testing

```bash
cargo test -p torrent                    # Run all tests
cargo clippy -p torrent -- -D warnings   # Lint
```

## License

MIT — see [LICENSE](../../LICENSE).
