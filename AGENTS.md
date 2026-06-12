# Project Guidelines

## Vision

A pure Rust BitTorrent library covering bencode, metainfo parsing, peer wire protocol, tracker communication (HTTP/UDP), DHT, magnet links, piece management, and session orchestration. Prioritize correctness, performance, and exhaustive testing.

## Hard Rules

These MUST be followed. Apply them before consulting the reference architecture below.

1.  **Language**. All documentation, comments, and git commits must be written in English. Public API doc comments (`///`) are mandatory for every `pub` item.

2.  **Crate Boundary**. `torrent-core` must NOT depend on tokio. All async I/O lives in `torrent`. Never add tokio as a dependency to `torrent-core`.

3.  **New Type Placement**. When adding a new public type, place it in `torrent-core` if it is pure data/parsing with no I/O; place it in `torrent` if it requires tokio or async behavior. Do not place sync-only types in the `torrent` crate.

4.  **Re-export Rule**. Re-export every public `torrent-core` type that is required by the `torrent` crate's public API. Do not re-export internal helper types or implementation-only types from `torrent-core`. Users should need only the `torrent` dependency for the public API surface.

5.  **Testing**. Always run `cargo test` and `cargo clippy -- -D warnings` after making changes:

    ```bash
    cargo test                     # Run all tests across workspace
    cargo test -p torrent-core
    cargo test -p torrent
    cargo test -- --test-threads=1 # Sequential (for network-sensitive tests)
    cargo clippy -- -D warnings
    cargo clippy -p torrent-core -- -D warnings
    cargo clippy -p torrent -- -D warnings
    cargo fmt -- --check
    RUSTDOCFLAGS="-D warnings" cargo doc
    ```

    - Tests must never require network access.
    - `torrent-core` tests must NOT use `#[tokio::test]` — they are fully synchronous.
    - Use `#[tokio::test]` only in the `torrent` crate.
    - Unit tests: inline `#[cfg(test)] mod tests` in each source file.
    - Integration tests: `crates/torrent-core/tests/*.rs` (sync); `crates/torrent/tests/*.rs` (async).
    - Property-based tests: `crates/torrent-core/tests/*_proptests.rs` using proptest.
    - Test vector files: `crates/torrent-core/tests/data/`.

6.  **Code Style**.
    - Rust 2024 edition. Follow standard Rust naming conventions.
    - Prefer `From` trait implementations over custom constructors.
    - Keep `unsafe` blocks minimal, well-documented, behind safe abstractions.
    - Use `#[non_exhaustive]` on `pub` enums/structs that may gain variants/fields.
    - Document protocol references with BEP numbers: `/// Implements BEP 0003: The BitTorrent Protocol Specification`.

7.  **Trait Derivation**.
    - Derive `Debug`, `Clone`, `PartialEq`, and `Eq` for all public value types that have value semantics and no hidden side effects.
    - Do NOT derive `Clone` or `Eq` for types that own resources, handles, or non-deterministic state (e.g., network connections, file handles, RNG state).
    - Error types must implement `std::error::Error` + `Send + Sync`.

## Reference Architecture

The sections below describe the workspace layout, module relationships, and implementation details. They are informative — rely on the Hard Rules above for normative constraints.

### Monorepo Structure (workspace)

```
torrent.rs/                  ← workspace root
├── Cargo.toml               ← [workspace] manifest
├── crates/
│   ├── torrent-core/        ← low-level core abstractions (sync, no tokio)
│   │   ├── Cargo.toml
│   │   ├── src/
│   │   │   ├── bencode/     ← BEP 3 encode/decode
│   │   │   ├── error.rs     ← Error + ErrorKind
│   │   │   ├── metainfo/    ← .torrent parsing (BEP 3/12/52)
│   │   │   ├── magnet/      ← Magnet URI (BEP 9)
│   │   │   ├── peer/        ← handshake, message types, PeerId (sync only)
│   │   │   ├── dht/         ← krpc, RoutingTable (sync only)
│   │   │   ├── tracker/     ← Announce data types (sync only)
│   │   │   ├── piece/       ← PieceManager, piece selection strategies
│   │   │   └── storage/     ← Storage trait
│   │   └── tests/           ← integration + property tests + test vectors
│   │
│   └── torrent/             ← high-level user-facing API (async, tokio)
│       ├── Cargo.toml       ← depends on torrent-core
│       └── src/
│           ├── session/     ← Session, download/upload loop, peer_manager
│           ├── peer/        ← stream (async PeerConnection)
│           ├── tracker/     ← HTTP + UDP tracker (async)
│           ├── dht/         ← rpc, query helpers (async)
│           └── storage/     ← file_backend (FileStorage impl)
```

### Crate Responsibilities

| Crate          | Role              | Runtime       | Key contents                                                                                                           |
| -------------- | ----------------- | ------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `torrent-core` | Core abstractions | sync          | bencode, error, metainfo, magnet, peer types, dht types, tracker data types, piece (manager + selector), storage trait |
| `torrent`      | High-level API    | async (tokio) | session, tracker, peer stream, dht rpc, FileStorage                                                                    |

### Dependencies

**torrent-core**

| Crate   | Purpose                                |
| ------- | -------------------------------------- |
| `bytes` | Zero-copy byte buffers                 |
| `sha1`  | Info hash, node ID, piece verification |
| `rand`  | Peer/transaction/node ID generation    |

**torrent**

| Crate          | Feature                                  | Purpose    |
| -------------- | ---------------------------------------- | ---------- |
| `torrent-core` | —                                        | Core types |
| `tokio`        | net, rt, macros, time, io-util, fs, sync | Async I/O  |

**dev-dependencies (workspace)**

| Crate      | Purpose                     |
| ---------- | --------------------------- |
| `proptest` | Property-based testing      |
| `tempfile` | Temp dirs for storage tests |

### Module Architecture

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
                ┌── piece (manager + selector)

      tracker/data
```

- `torrent-core`: All sync — no tokio dependency. Contains data types, parsing, encoding, traits.
- `torrent`: Async I/O via tokio. Depends on `torrent-core` for all data types.

### Key Implementation Details

- **Bencode**: Recursive-descent parser with strict validation. Dict keys sorted lexicographically during both decode and encode for idempotent round-trips. Uses `Vec<(Bytes, Bencode)>` for dicts.
- **Metainfo**: `info_hash()` computes SHA-1 of the raw bencoded `info` dict. Supports single-file, multi-file (BEP 52), and announce-list (BEP 12).
- **Magnet**: Parses `magnet:?xt=urn:btih:<hex\|base32>`. Hex and base32 decoding implemented manually.
- **Peer**: 11 message types (`KeepAlive`–`Port`). 68-byte handshake with reserved extension bits. Types in `torrent-core`, async `PeerConnection` in `torrent`.
- **Tracker**: `HttpTracker` uses manual HTTP/1.1 (no `reqwest`). `UdpTracker` implements BEP 15 connection protocol + announce + retry. Both in `torrent`.
  - `HttpTracker` supports both `http://` (plain TCP) and `https://` (TLS via `tokio-rustls`).
- **Piece**: `PieceManager` (bitfield, progress tracking) + 4 selection strategies (`RarestFirst`, `RandomFirst`, `Sequential`, `EndGame`) in `torrent-core`.
- **Storage**: `Storage` trait in `torrent-core`. `FileStorage` implementation in `torrent`.
- **DHT**: 160 K-buckets (K=8), XOR distance, KRPC bencode-based messages in `torrent-core`. Async RPC + 4 query types (`ping`, `find_node`, `get_peers`, `announce_peer`) in `torrent`.
- **Session**: `Session::new(config)` → `add_torrent()` / `remove_torrent()` / `torrent_status()`. Per-torrent `DownloadLoop` (tokio::spawn). `PeerManager` connection pool. `UploadManager` choke/unchoke logic. All in `torrent`.

### Project Status

All 5 implementation phases are complete (202 tests, 0 failures):

| Phase | Modules              | Status                                      |
| ----- | -------------------- | ------------------------------------------- |
| 1     | `bencode`, `error`   | ✅ Bencode AST + recursive-descent parser   |
| 2     | `metainfo`, `magnet` | ✅ .torrent parsing, Magnet URI (BEP 9)     |
| 3     | `peer`, `tracker`    | ✅ Wire protocol, HTTP/UDP tracker          |
| 4     | `storage`, `dht`     | ✅ File I/O, Kademlia DHT (BEP 5)           |
| 5     | `session`            | ✅ High-level API orchestrating all modules |
