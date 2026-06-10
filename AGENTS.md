# Project Guidelines

## Vision

A pure Rust BitTorrent library comparable in scope to libtorrent — covering bencode, metainfo parsing, peer wire protocol, tracker communication (HTTP/UDP), DHT, magnet links, and piece management. Prioritize correctness, performance, and exhaustive testing.

## Language

- All documentation, comments, and git commits must be written in **English**.
- Public API doc comments (`///`) are mandatory for all `pub` items.

## Build and Test

```bash
cargo build                    # Debug build
cargo build --release          # Release build
cargo test                     # Run all tests
cargo test -- --test-threads=1 # Run tests sequentially (useful for network-related tests)
cargo clippy -- -D warnings    # Lint strictly (treat warnings as errors)
cargo fmt -- --check           # Verify formatting
```

Always run `cargo test` and `cargo clippy -- -D warnings` after making changes.

## Code Style

- Rust 2024 edition.
- Follow standard Rust naming conventions: `snake_case` for functions/variables, `CamelCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Prefer `::from()` / `From` trait implementations over custom constructor methods.
- Use `thiserror` for library error types, `anyhow` only in binaries/examples.
- Keep `unsafe` blocks minimal, well-documented, and behind safe abstractions.

## Architecture

The library should be organized into focused modules reflecting the BitTorrent protocol layers:

| Module     | Responsibility                                                  |
| ---------- | --------------------------------------------------------------- |
| `bencode`  | Bencode encoding/decoding                                       |
| `metainfo` | `.torrent` file parsing and creation                            |
| `peer`     | Peer wire protocol (handshake, message framing, block requests) |
| `tracker`  | HTTP and UDP tracker announce/scrape                            |
| `dht`      | Distributed Hash Table (BEP 5)                                  |
| `magnet`   | Magnet URI parsing (BEP 9)                                      |
| `storage`  | Piece storage, file allocation, and disk I/O                    |
| `session`  | High-level session management, orchestrating downloads/uploads  |

Each module should be independently usable where practical.

## Testing

- **Unit tests**: Every public function and method. Use `#[cfg(test)] mod tests { ... }`.
- **Integration tests**: Place in `tests/` directory for cross-module scenarios.
- **Property-based tests**: Use `proptest` for parsers (bencode, metainfo, magnet).
- **Fuzz testing**: Set up with `cargo-fuzz` for network-facing parsers.
- **Test vectors**: Maintain known-good `.torrent` files and bencode blobs in a `tests/data/` directory.
- Tests should not require network access; mock or replay network interactions.

## Dependencies

- Prefer well-established crates: `tokio` for async I/O, `bytes` for buffer management, `thiserror` for errors.
- Minimize dependency count; justify each addition.
- Keep the library runtime-agnostic where possible (avoid hard-coding a specific async runtime in core modules).

## Conventions

- All `pub` types must implement `Debug`. Prefer deriving `Clone`, `PartialEq`, `Eq` where appropriate.
- Error types should implement `std::error::Error` + `Send + Sync`.
- Use `#[non_exhaustive]` on public enums and structs that may gain variants/fields in the future.
- Document protocol references with BEP numbers in doc comments (e.g., `/// Implements BEP 0003: The BitTorrent Protocol Specification`).
