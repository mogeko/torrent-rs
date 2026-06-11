# torrent-core

[![MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../../LICENSE)

**Low-level core abstractions for the BitTorrent protocol.** Zero async runtime
dependency — all types are fully synchronous.

This crate provides the fundamental data types and algorithms needed for
BitTorrent communication. It is a dependency of [`torrent`](../torrent) and can
also be used standalone when only low-level parsing or encoding is needed.

## Modules

| Module                       | Description                                         | BEP           |
| ---------------------------- | --------------------------------------------------- | ------------- |
| [`bencode`](./src/bencode)   | Bencode encoding/decoding with strict validation    | BEP 3         |
| [`error`](./src/error.rs)    | Error + ErrorKind (kind + source pattern)           | —             |
| [`metainfo`](./src/metainfo) | `.torrent` file parsing, `info_hash()`              | BEP 3, 12, 52 |
| [`magnet`](./src/magnet)     | Magnet URI parsing (hex + base32)                   | BEP 9         |
| [`peer`](./src/peer)         | Handshake, 11 wire message types, PeerId            | BEP 3         |
| [`dht`](./src/dht)           | KRPC message format, Kademlia RoutingTable          | BEP 5         |
| [`tracker`](./src/tracker)   | Announce request/response data types                | BEP 3, 15, 23 |
| [`storage`](./src/storage)   | Storage trait, PieceManager, 4 selection strategies | BEP 3         |

## Quick Start

```rust
use torrent_core::bencode::{decode, encode, Bencode};

let (val, rest) = decode(b"4:spam").unwrap();
assert!(rest.is_empty());

let encoded = encode(&val);
assert_eq!(encoded, b"4:spam");
```

## Examples

### Bencode

```rust
use torrent_core::bencode::{decode, encode, Bencode};
use bytes::Bytes;

// Decode a bencoded dictionary
let (val, _) = decode(b"d3:fooi42e5:hello5:worlde").unwrap();
let encoded = encode(&val);
// Keys are sorted lexicographically
assert_eq!(encoded, b"d3:fooi42e5:hello5:worlde");
```

### Metainfo

```rust
use torrent_core::metainfo::from_bytes;

let meta = from_bytes(&std::fs::read("debian.torrent").unwrap()).unwrap();
println!("Info hash: {:x?}", meta.info_hash());
println!("Pieces: {}", meta.info.num_pieces());
```

### Magnet

```rust
use std::str::FromStr;
use torrent_core::magnet::MagnetUri;

let uri = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567\
           &dn=ubuntu-24.04&tr=http://tracker.example.com/announce";
let magnet = MagnetUri::from_str(uri).unwrap();
assert_eq!(magnet.info_hashes.len(), 1);
assert_eq!(magnet.display_name.as_deref(), Some("ubuntu-24.04"));
```

### Peer

```rust
use torrent_core::peer::{Handshake, PeerId, PeerMessage};

// PeerId generation
let peer_id = PeerId::random();
assert_eq!(&peer_id.0[..8], b"-TR1000-");

// Handshake round-trip
let hs = Handshake::new([1u8; 20], peer_id.0);
let bytes = hs.to_bytes();
let parsed = Handshake::from_bytes(&bytes).unwrap();
assert_eq!(hs, parsed);

// Message round-trip
let msg = PeerMessage::Request { index: 0, begin: 0, length: 16384 };
let encoded = torrent_core::peer::encode(&msg);
let decoded = torrent_core::peer::decode(&encoded).unwrap();
assert_eq!(msg, decoded);
```

### DHT / KRPC

```rust
use torrent_core::dht::{Node, RoutingTable};

let mut rt = RoutingTable::new();
rt.insert(Node {
    id: [1u8; 20],
    addr: "127.0.0.1:6881".parse().unwrap(),
});
assert_eq!(rt.num_nodes(), 1);
```

### Storage

```rust
use torrent_core::storage::PieceManager;

let mut pm = PieceManager::new(10);
pm.set_piece(0);
assert_eq!(pm.missing_pieces().len(), 9);
assert_eq!(pm.progress(), 0.1);
```

### Tracker data types

```rust
use torrent_core::tracker::parse_compact_peers_ipv4;

let data = [127, 0, 0, 1, 0x1A, 0xE1]; // 127.0.0.1:6881
let peers = parse_compact_peers_ipv4(&data).unwrap();
assert_eq!(peers[0].to_string(), "127.0.0.1:6881");
```

**No async runtime required** — this crate has zero dependency on tokio or any
other async runtime.

## Testing

```bash
cargo test -p torrent-core                          # Run all tests
cargo test -p torrent-core -- --test-threads=1      # Sequential (if needed)
cargo clippy -p torrent-core -- -D warnings         # Lint
```

All tests are synchronous — no `#[tokio::test]` is used anywhere.

## Features Not Here

The following require async I/O and live in the [`torrent`](../torrent) crate:

- **Peer stream**: async TCP `PeerConnection`
- **Tracker**: HTTP and UDP announce clients
- **DHT RPC**: async UDP send/receive with transaction matching
- **DHT queries**: `find_node`, `get_peers`, `announce_peer`
- **File storage**: `FileStorage` (tokio `fs`)
- **Session**: high-level download/upload orchestration

## License

MIT — see [LICENSE](../../LICENSE).
