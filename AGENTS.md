# Project Guidelines

## Vision

A pure Rust BitTorrent library comparable in scope to libtorrent — covering bencode, metainfo parsing, peer wire protocol, tracker communication (HTTP/UDP), DHT, magnet links, piece management, and session orchestration. Prioritize correctness, performance, and exhaustive testing.

## Monorepo Structure (workspace)

The project is organized as a Cargo workspace with two crates:

```
torrent.rs/                  ← workspace root
├── Cargo.toml               ← [workspace] manifest
├── crates/
│   ├── torrent-core/        ← low-level core abstractions (sync, no tokio)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── bencode/     ← BEP 3 encode/decode
│   │       ├── error.rs     ← Error + ErrorKind
│   │       ├── metainfo/    ← .torrent parsing (BEP 3/12/52)
│   │       ├── magnet/      ← Magnet URI (BEP 9)
│   │       ├── peer/        ← handshake, message types, PeerId (sync only)
│   │       ├── dht/         ← krpc, RoutingTable (sync only)
│   │       └── storage/     ← Storage trait, PieceManager, piece_selector
│   │
│   └── torrent/             ← high-level user-facing API (async, tokio)
│       ├── Cargo.toml       ← depends on torrent-core
│       └── src/
│           ├── session/     ← Session, download/upload loop, peer_manager
│           ├── peer/        ← stream (async PeerConnection)
│           ├── tracker/     ← HTTP + UDP tracker (async)
│           ├── dht/         ← rpc, query helpers (async)
│           └── storage/     ← file_backend (FileStorage impl)
│
├── tests/                   ← integration + property tests
└── tests/data/              ← test vectors (.torrent, bencode)
```

### Crate Responsibilities

| Crate          | Role              | Runtime       | Key contents                                                            |
| -------------- | ----------------- | ------------- | ----------------------------------------------------------------------- |
| `torrent-core` | Core abstractions | sync          | bencode, error, metainfo, magnet, peer types, dht types, storage traits |
| `torrent`      | High-level API    | async (tokio) | session, tracker, peer stream, dht rpc, FileStorage                     |

**Rule**: `torrent-core` must NOT depend on tokio. All async I/O lives in `torrent`.

`torrent` re-exports key `torrent-core` types for convenience — users should only need `torrent` as a dependency.

## Language

- All documentation, comments, and git commits must be written in **English**.
- Public API doc comments (`///`) are mandatory for all `pub` items.

## Build and Test

```bash
cargo build                    # Debug build (all workspace crates)
cargo build --release          # Release build
cargo test                     # Run all tests across workspace
cargo test -p torrent-core     # Run only torrent-core tests
cargo test -p torrent          # Run only torrent tests
cargo test -- --test-threads=1 # Run tests sequentially (for network tests)
cargo clippy -- -D warnings    # Lint strictly (treat warnings as errors)
cargo clippy -p torrent-core -- -D warnings
cargo clippy -p torrent -- -D warnings
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

### torrent-core

| Crate   | Purpose                                |
| ------- | -------------------------------------- |
| `bytes` | Zero-copy byte buffers                 |
| `sha1`  | Info hash, node ID, piece verification |
| `rand`  | Peer/transaction/node ID generation    |

### torrent

| Crate          | Feature                                  | Purpose    |
| -------------- | ---------------------------------------- | ---------- |
| `torrent-core` | —                                        | Core types |
| `tokio`        | net, rt, macros, time, io-util, fs, sync | Async I/O  |

### dev-dependencies (workspace)

| Crate      | Purpose                     |
| ---------- | --------------------------- |
| `proptest` | Property-based testing      |
| `tempfile` | Temp dirs for storage tests |

## Module Architecture

```
torrent-core (sync)              torrent (async)
─────────────────────            ─────────────────
bencode ─── metainfo             session ───────────────
    │           │                    │
    │           ├── magnet           ├── tracker (http, udp)
    │           │                    │
    └── error   ├── peer/types       ├── peer/stream
                │                    │
                ├── dht/types        ├── dht/rpc
                │                    │
                └── storage/trait    └── storage/file_backend
                     storage/piece_selector
```

- `torrent-core`: All sync — no tokio dependency. Contains data types, parsing, encoding, traits.
- `torrent`: Async I/O via tokio. Depends on `torrent-core` for all data types.

## Key Implementation Details

- **Bencode**: Recursive-descent parser with strict validation. Dict keys sorted lexicographically during both decode and encode for idempotent round-trips. Uses `Vec<(Bytes, Bencode)>` for dicts.
- **Metainfo**: `info_hash()` computes SHA-1 of the raw bencoded `info` dict. Supports single-file, multi-file (BEP 52), and announce-list (BEP 12).
- **Magnet**: Parses `magnet:?xt=urn:btih:<hex\|base32>`. Hex and base32 decoding implemented manually.
- **Peer**: 11 message types (`KeepAlive`–`Port`). 68-byte handshake with reserved extension bits. Types in `torrent-core`, async `PeerConnection` in `torrent`.
- **Tracker**: `HttpTracker` uses manual HTTP/1.1 (no `reqwest`). `UdpTracker` implements BEP 15 connection protocol + announce + retry. Both in `torrent`.
- **Storage**: `Storage` trait + `PieceManager` + 4 selection strategies (`RarestFirst`, `RandomFirst`, `Sequential`, `EndGame`) in `torrent-core`. `FileStorage` implementation in `torrent`.
- **DHT**: 160 K-buckets (K=8), XOR distance, KRPC bencode-based messages in `torrent-core`. Async RPC + 4 query types (`ping`, `find_node`, `get_peers`, `announce_peer`) in `torrent`.
- **Session**: `Session::new(config)` → `add_torrent()` / `remove_torrent()` / `torrent_status()`. Per-torrent `DownloadLoop` (tokio::spawn). `PeerManager` connection pool. `UploadManager` choke/unchoke logic. All in `torrent`.

## Conventions

- All `pub` types implement `Debug`. Derive `Clone`, `PartialEq`, `Eq` when appropriate.
- Error types implement `std::error::Error` + `Send + Sync`.
- Document protocol references with BEP numbers: `/// Implements BEP 0003: The BitTorrent Protocol Specification`.
- Tests never require network access; use `tokio::test` for async tests (in `torrent` crate only).
- Test vector files live in `tests/data/`.
- When adding a new type, decide: does it need tokio? → `torrent`. Is it pure data/parsing? → `torrent-core`.
- `torrent` should re-export commonly-used `torrent-core` types so downstream users only need one dependency.

## Testing

- **Unit tests**: inline `#[cfg(test)] mod tests` in each source file.
- **Integration tests**: `tests/*.rs` for cross-module scenarios.
- **Property-based tests**: `tests/*_proptests.rs` using proptest.
- **Test vectors**: binary files in `tests/data/` for bencode and .torrent files.
- `torrent-core` tests must not use `#[tokio::test]`.
