# Project Guidelines

## Vision

A pure Rust BitTorrent library comparable in scope to libtorrent — covering bencode, metainfo parsing, peer wire protocol, tracker communication (HTTP/UDP), DHT, magnet links, piece management, and session orchestration. Prioritize correctness, performance, and exhaustive testing.

## Language

- All documentation, comments, and git commits must be written in **English**.
- Public API doc comments (`///`) are mandatory for all `pub` items.

## Build and Test

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo test                     # Run all 142 unit/integration/property tests
cargo test -- --test-threads=1 # Run tests sequentially (for network tests)
cargo clippy -- -D warnings    # Lint strictly (treat warnings as errors)
cargo fmt -- --check           # Verify formatting
```

Always run `cargo test` and `cargo clippy -- -D warnings` after making changes.

## Project Status

All 5 implementation phases are complete (142 tests, 0 failures):

| Phase | Modules              | Status                                      |
| ----- | -------------------- | ------------------------------------------- |
| 1     | `bencode`, `error`   | ✅ Bencode AST + recursive-descent parser   |
| 2     | `metainfo`, `magnet` | ✅ .torrent parsing, Magnet URI (BEP 9)     |
| 3     | `peer`, `tracker`    | ✅ Wire protocol, HTTP/UDP tracker          |
| 4     | `storage`, `dht`     | ✅ File I/O, Kademlia DHT (BEP 5)           |
| 5     | `session`            | ✅ High-level API orchestrating all modules |

## Code Style

- Rust 2024 edition.
- Follow standard Rust naming conventions.
- Prefer `From` trait implementations over custom constructors.
- Keep `unsafe` blocks minimal, well-documented, behind safe abstractions.
- Use `#[non_exhaustive]` on `pub` enums/structs that may gain variants/fields.

## Dependencies

| Crate            | Feature                                  | Purpose                                |
| ---------------- | ---------------------------------------- | -------------------------------------- |
| `bytes`          | —                                        | Zero-copy byte buffers                 |
| `sha1`           | —                                        | Info hash, node ID, piece verification |
| `tokio`          | net, rt, macros, time, io-util, fs, sync | Async I/O                              |
| `rand`           | —                                        | Peer/transaction/node ID generation    |
| `proptest` (dev) | —                                        | Property-based testing                 |
| `tempfile` (dev) | —                                        | Temp dirs for storage tests            |

## Module Architecture

```
bencode ─── metainfo ─── peer ─── session
                │           │         │
                └── magnet   ├── tracker
                             │
                             └── dht
              storage ───────────────┘
```

- `bencode`, `peer::handshake`, `peer::message`, `dht::krpc`, `dht::RoutingTable` are **sync** (no runtime dependency).
- `peer::stream`, `tracker::{http,udp}`, `storage::FileStorage`, `dht::rpc` use **tokio async I/O**.

## Key Implementation Details

- **Bencode**: Recursive-descent parser with strict validation. Dict keys sorted lexicographically during both decode and encode for idempotent round-trips. Uses `Vec<(Bytes, Bencode)>` for dicts.
- **Metainfo**: `info_hash()` computes SHA-1 of the raw bencoded `info` dict. Supports single-file, multi-file (BEP 52), and announce-list (BEP 12).
- **Magnet**: Parses `magnet:?xt=urn:btih:<hex\|base32>`. Hex and base32 decoding implemented manually.
- **Peer**: 11 message types (`KeepAlive`–`Port`). 68-byte handshake with reserved extension bits. Async `PeerConnection` with buffered I/O.
- **Tracker**: `HttpTracker` uses manual HTTP/1.1 (no `reqwest`). `UdpTracker` implements BEP 15 connection protocol + announce + retry.
- **Storage**: `FileStorage` with single/multi-file sparse pre-allocation. `PieceManager` for bitfield tracking. 4 selection strategies: `RarestFirst`, `RandomFirst`, `Sequential`, `EndGame`.
- **DHT**: 160 K-buckets (K=8), XOR distance, KRPC bencode-based messages. 4 query types: `ping`, `find_node`, `get_peers`, `announce_peer`.
- **Session**: `Session::new(config)` → `add_torrent()` / `remove_torrent()` / `torrent_status()`. Per-torrent `DownloadLoop` (tokio::spawn). `PeerManager` connection pool. `UploadManager` choke/unchoke logic.

## Conventions

- All `pub` types implement `Debug`. Derive `Clone`, `PartialEq`, `Eq` when appropriate.
- Error types implement `std::error::Error` + `Send + Sync`.
- Document protocol references with BEP numbers: `/// Implements BEP 0003: The BitTorrent Protocol Specification`.
- Tests never require network access; use `tokio::test` for async tests.
- Test vector files live in `tests/data/`.

## Testing

- **Unit tests**: inline `#[cfg(test)] mod tests` in each source file.
- **Integration tests**: `tests/*.rs` for cross-module scenarios.
- **Property-based tests**: `tests/*_proptests.rs` using proptest.
- **Test vectors**: binary files in `tests/data/` for bencode and .torrent files.
