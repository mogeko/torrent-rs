# torrent.rs

[![MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A pure Rust BitTorrent library comparable in scope to libtorrent. Built from the ground up with correctness, performance, and exhaustive testing.

```
142 tests · 10 modules · 4 runtime dependencies
```

## Features

| Feature                    | Tests | Standard      |
| -------------------------- | ----- | ------------- |
| Bencode encoding/decoding  | 63    | BEP 3         |
| .torrent file parsing      | 7     | BEP 3, 12, 52 |
| Magnet URI (hex + base32)  | 9     | BEP 9         |
| Peer wire protocol         | 25    | BEP 3         |
| HTTP tracker announce      | async | BEP 3, 23     |
| UDP tracker announce       | async | BEP 15        |
| File storage (multi-file)  | async | —             |
| Piece selection (4 strats) | 7     | BEP 3         |
| DHT Kademlia routing table | 9     | BEP 5         |
| DHT KRPC (4 query types)   | 6     | BEP 5         |
| Session API                | API   | —             |

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

## Architecture

```
bencode  ─── metainfo ─── peer ─── session
                │           │         │
                └── magnet   ├── tracker
                             │
                             └── dht
              storage ───────────────┘
```

| Module     | Responsibility                                           | Sync/Async |
| ---------- | -------------------------------------------------------- | ---------- |
| `bencode`  | Bencode encoding/decoding (BEP 3)                        | sync       |
| `metainfo` | `.torrent` file parsing, `info_hash()`                   | sync       |
| `magnet`   | Magnet URI parsing (BEP 9)                               | sync       |
| `peer`     | Handshake, wire messages, `PeerConnection` (async TCP)   | sync+async |
| `tracker`  | HTTP (manual) and UDP tracker announce                   | async      |
| `storage`  | File I/O (single/multi-file), piece selection strategies | async      |
| `dht`      | Kademlia routing table, KRPC protocol, DHT queries       | sync+async |
| `session`  | High-level API: add/remove torrents, download management | async      |

## Module Previews

### `bencode`

```rust
use torrent::bencode::{decode, encode, Bencode};

let (val, _) = decode(b"d3:fooi42e3:bar4:spame").unwrap();
let encoded = encode(&val);
assert_eq!(encoded, b"d3:bar4:spam3:fooi42ee"); // sorted keys
```

### `metainfo`

```rust
use torrent::metainfo::from_bytes;

let meta = from_bytes(&std::fs::read("debian.torrent")?)?;
let hash = meta.info_hash();
println!("{:x?}", hash);
```

### `magnet`

```rust
use std::str::FromStr;
use torrent::magnet::MagnetUri;

let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
let magnet = MagnetUri::from_str(uri).unwrap();
assert_eq!(magnet.info_hashes.len(), 1);
```

### `peer`

```rust
use torrent::peer::{Handshake, PeerId, PeerMessage};

let peer_id = PeerId::random();
let hs = Handshake::new([0u8; 20], peer_id.0);
let parsed = Handshake::from_bytes(&hs.to_bytes()).unwrap();
assert_eq!(hs, parsed);

let msg = PeerMessage::Have(42);
let decoded = torrent::peer::decode(&torrent::peer::encode(&msg)).unwrap();
assert_eq!(msg, decoded);
```

### `tracker`

```rust
use torrent::tracker::parse_compact_peers_ipv4;

let data = [127, 0, 0, 1, 0x1A, 0xE1]; // 127.0.0.1:6881
let peers = parse_compact_peers_ipv4(&data).unwrap();
assert_eq!(peers[0].to_string(), "127.0.0.1:6881");
```

### `storage`

```rust
use torrent::storage::PieceManager;

let mut pm = PieceManager::new(10);
pm.set_piece(0);
assert_eq!(pm.missing_pieces().len(), 9);
assert_eq!(pm.progress(), 0.1);
```

### `dht`

```rust
use torrent::dht::{Node, RoutingTable};

let mut rt = RoutingTable::new();
rt.insert(Node {
    id: [1u8; 20],
    addr: "127.0.0.1:6881".parse().unwrap(),
});
assert_eq!(rt.num_nodes(), 1);
```

## Dependencies

| Crate   | Version | Purpose                                |
| ------- | ------- | -------------------------------------- |
| `bytes` | 1       | Zero-copy byte buffers                 |
| `sha1`  | 0.10    | SHA-1 hashing (info hash, DHT, pieces) |
| `tokio` | 1       | Async TCP, UDP, filesystem, timers     |
| `rand`  | 0.8     | Random ID generation                   |

## License

MIT — see [LICENSE](LICENSE).
