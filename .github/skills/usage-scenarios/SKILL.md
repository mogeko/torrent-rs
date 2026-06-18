---
name: usage-scenarios
description: "Comprehensive usage scenario catalog for torrent-rs. Use when: designing API, evaluating feature proposals, clarifying project boundaries, or making design trade-offs. Maps real-world use cases to API requirements."
argument-hint: "[scenario category or specific use case to analyze]"
---

# Usage Scenarios for torrent-rs

## When to Use

- Designing new public API or changing existing API
- Deciding whether a feature belongs in `torrent-core` vs `torrent`
- Evaluating whether a use case is in scope for this library
- Making trade-offs between flexibility, simplicity, and performance
- Reviewing PRs that introduce new public types or configuration options
- Writing examples or documentation — pick the most representative scenarios

## How to Use This Skill

1. Identify which scenario category the feature/change targets
2. Read the API implications for that category
3. Cross-reference with project boundaries to confirm it's in scope
4. Apply the derived API design principles

## Current Priority

**Data Processing & Streaming** (Section 6) is the top-priority scenario for the next API design iteration. Key focuses:

- Streaming piece delivery (callback/stream-based, not just polling)
- Custom `Storage` backends (in-memory, remote, processing pipeline)
- Download without disk I/O (process-and-discard)
- Block-level streaming for lower latency than piece-level
- Selective file download from multi-file torrents

---

---

## Taxonomy of Usage Scenarios

```
1. Classic BT Client (end-user application)
   1.1 Desktop GUI Client
   1.2 Headless Daemon / Seedbox
   1.3 CLI Tool
   1.4 Mobile Client (via FFI)
   1.5 Web-Based Client (WASM)

2. Content Distribution
   2.1 Software Distribution (CDN alternative)
   2.2 Large Dataset Distribution
   2.3 Media Distribution & Streaming
   2.4 Internal Enterprise Distribution

3. Library Embedding
   3.1 Dependency of Another Rust Application
   3.2 Framework / Plugin Integration
   3.3 Embedded Systems / IoT

4. Analysis & Tooling
   4.1 Torrent File Inspector
   4.2 Magnet Link Tool
   4.3 Tracker Analysis & Health Checking
   4.4 Swarm / Peer Analysis
   4.5 DHT Crawler & Network Explorer

5. Protocol-Level Development
   5.1 Implementing BEP Extensions
   5.2 Protocol Testing & Fuzzing
   5.3 Academic Research & Benchmarking

6. Data Processing
   6.1 Streaming Download & On-the-Fly Processing
   6.2 Selective Download (file-level / piece-level)
   6.3 BenCode Processing (encode/decode/validate/convert)

7. Hybrid & Advanced
   7.1 BT + WebSeed (hybrid HTTP/BT)
   7.2 BT + IPFS / Content-Addressable Storage
   7.3 BT + Blockchain / Incentivized Seeding
   7.4 BT + WebRTC / WebTorrent Interop
   7.5 BT + Proxy / VPN / Tor / I2P

8. DevOps & Automation
   8.1 CI/CD Artifact Distribution
   8.2 Infrastructure as Code Integration
   8.3 Monitoring & Observability (metrics, health checks)

9. Specialized Clients
   9.1 Private Tracker Client
   9.2 Live Streaming Client
   9.3 RSS + BT Automation (Sonarr/Radarr-like)
```

---

## 1. Classic BT Client

### 1.1 Desktop GUI Client

**User wants to**: Build a full BitTorrent client with a graphical interface (e.g., Transmission-like).

**Key operations**:

- Add torrents from files / magnet links / URLs
- View download progress, speed, ETA, peer list per torrent
- Pause / resume / remove / recheck torrents
- Configure bandwidth limits globally and per torrent
- Set file priorities, skip unwanted files
- Sequential download for media preview (stream a video while downloading)
- Queue management (max active downloads)
- Watch directory (auto-add .torrent files from a folder)
- Schedule bandwidth (e.g., unlimited at night)
- Associate with `magnet:` and `.torrent` in the OS

**Library usage**: `Session` (full lifecycle), `SessionConfig`, `TorrentStatus`, piece selection strategies, `metainfo` for inspection, `magnet` for link parsing.

**API implications**:

- `Session` needs pause/resume/stop/recheck per torrent
- `SessionConfig` needs bandwidth limit fields, max active downloads, queue settings
- Need a callback/event stream for progress updates (not just polling)
- Need file-level priority API (`set_file_priority(info_hash, file_index, priority)`)
- Need sequential download mode (already have `Sequential` selector)
- Need session persistence (save/resume state across restarts)
- Need ability to observe peer connections (per-torrent peer list)
- Need `add_torrent_from_url()` for remote .torrent files

### 1.2 Headless Daemon / Seedbox

**User wants to**: Run torrent-rs as a headless service (daemon) on a server, managed via Web UI or RPC API.

**Key operations**:

- Start as a background service (systemd, Docker)
- Remote management via HTTP/gRPC API
- Multi-user support
- Ratio enforcement per torrent
- Automatic RSS feed monitoring and download
- Move completed downloads to specific directories
- Execute scripts on download completion
- Label/categorize torrents

**Library usage**: `Session`, all modules, plus RPC layer (not provided by the library).

**API implications**:

- Session must be `Send + Sync` and sharable across tasks
- All session operations must be async and non-blocking
- Need serializable status types (`TorrentStatus` should impl `serde::Serialize`)
- Need labels/tags on torrents
- Need completion hooks (callback on torrent finished)
- Configuration should be serializable (load/save config as JSON/TOML)

### 1.3 CLI Tool

**User wants to**: A simple CLI for quick downloads or torrent inspection.

**Key operations**:

- `torrent download <magnet/torrent>` — download a single torrent
- `torrent info <file.torrent>` — display metadata
- `torrent magnet <file.torrent>` — convert .torrent to magnet URI
- `torrent peers <magnet>` — show peer list from tracker/DHT

**Library usage**: `Session`, `metainfo`, `magnet`, `tracker`, `dht`.

**API implications**:

- Need synchronous convenience for simple operations (e.g., `Metainfo::try_from(path)`)
- Need one-shot tracker announce without full session
- Need one-shot DHT lookup without full session
- Progress reporting should work well with terminal output (percentage, bar)

### 1.4 Mobile Client (via FFI)

**User wants to**: Use torrent-rs on iOS/Android via C FFI bindings.

**Key operations**:

- Call Rust from Swift/Kotlin via C ABI
- Battery-aware downloading (pause when battery low)
- WiFi-only mode
- Background download (OS constraints)

**Library implications**: Not a Rust API concern but influences design:

- Avoid complex generic types in public API (FFI-unfriendly)
- Prefer concrete types over trait objects in key interfaces
- Consider `#[repr(C)]` for types that cross FFI boundary
- Session should be usable as opaque pointer (`Box<Session>` → `*mut c_void`)

### 1.5 Web-Based Client (WASM)

**User wants to**: Compile torrent-rs to WASM for browser-based BT client.

**Key operations**:

- Run in browser (no TCP/UDP sockets directly)
- Use WebRTC data channels for peer connections (WebTorrent)
- Use browser storage (IndexedDB) for file data

**Library implications**:

- `torrent-core` should be WASM-compatible (no tokio, no platform-specific deps)
- `torrent` likely needs WASM-specific networking layer
- Storage trait needs WASM-compatible implementation

---

## 2. Content Distribution

### 2.1 Software Distribution (CDN Alternative)

**User wants to**: Distribute software updates via BitTorrent to reduce CDN costs.

**Key operations**:

- Create .torrent files for release artifacts
- Seed from origin servers, let users help distribute
- Verify downloads via info_hash
- Bandwidth throttling on origin servers
- WebSeed fallback for reliability

**Library usage**: `metainfo` (creation, not just parsing), `storage` (seeding), `session` (managed seeding).

**API implications**:

- **Need `Metainfo` creation API**: `Metainfo::builder().add_file(...).build()` or similar
- Need WebSeed support (BEP 19) for hybrid HTTP/BT distribution
- Need seeding-only mode (share existing files without downloading)
- Need file verification / recheck (verify existing files match info_hash)
- Need upload-only bandwidth management

### 2.2 Large Dataset Distribution

**User wants to**: Distribute ML training datasets (100GB+) or scientific data.

**Key operations**:

- Multi-file torrents with directory structure preservation
- Resume interrupted downloads
- Verify data integrity after download
- Bandwidth scheduling for large transfers
- Partial download (only specific files from dataset)

**Library usage**: `metainfo` (multi-file), `storage` (file backend), `piece` (selection), `session`.

**API implications**:

- Need robust resume (check existing files, skip completed pieces)
- Need file-level filtering (download only specific paths from multi-file torrent)
- Need large file support (>4GB, already supported via `u64` lengths)
- Need checksum verification beyond SHA-1 pieces (BEP 52 v2 hashes)

### 2.3 Media Distribution & Streaming

**User wants to**: Distribute video/audio and potentially stream while downloading.

**Key operations**:

- Sequential download (first piece first, last piece last)
- Stream video while download is in progress
- Adaptive quality (choose files based on bandwidth)

**Library usage**: `piece::Sequential`, `storage`, `session`.

**API implications**:

- `Sequential` selector exists, but needs streaming read API: "read piece 0 as soon as it's verified"
- Need `read_piece(index) -> Vec<u8>` or streaming interface on `Session`
- Need to prioritize pieces near playback position

### 2.4 Internal Enterprise Distribution

**User wants to**: Distribute build artifacts, logs, or configuration within a corporate network.

**Key operations**:

- Private tracker (internal tracker server)
- LAN-only peer discovery (LSD, BEP 14)
- No external network access
- Authentication on tracker

**Library usage**: `tracker` (HTTP/UDP), `dht` (optional), `peer`, `session`.

**API implications**:

- Need private tracker support (passkey in announce URL)
- Need LSD (Local Service Discovery) — not yet implemented
- Need ability to disable DHT, PEX, etc. for private torrents
- `private=1` flag in metainfo should be respected

---

## 3. Library Embedding

### 3.1 Dependency of Another Rust Application

**User wants to**: Add BT download capability to their Rust application (game launcher, data sync tool, etc.).

**Key operations**:

- Programmatic download control from host application
- Integrate with host app's event loop / async runtime
- Report progress to host app's UI
- Minimal configuration, sensible defaults

**Library usage**: `Session`, `TorrentSpec`, `SessionConfig`.

**API implications**:

- `Session` should be easy to create with minimal config
- Progress reporting via channels/streams, not just polling
- Need `add_torrent_with_callback(spec, on_progress: Fn)` pattern
- Don't force specific logging/tracing configuration
- `Default` impl on `SessionConfig` should be production-usable

### 3.2 Framework / Plugin Integration

**User wants to**: Use torrent-rs inside a web framework (Actix, Axum) or actor system.

**Key operations**:

- Share `Session` across HTTP request handlers
- Download torrents in response to API requests
- Stream download progress via WebSocket/SSE

**Library usage**: `Session` (shared via `Arc`), `TorrentStatus`.

**API implications**:

- `Session` already uses `Arc` internally — ensure it's `Clone` or easily wrappable
- Need event stream: `session.events() -> impl Stream<Item = SessionEvent>`
- `TorrentStatus` should be cheap to clone and serialize

### 3.3 Embedded Systems / IoT

**User wants to**: Use BT for firmware updates on routers, set-top boxes, or IoT devices.

**Key operations**:

- Minimal resource usage (memory, CPU, storage)
- Download single file (firmware image)
- Verify checksum, apply update, reboot
- No persistent session needed

**Library usage**: `metainfo`, `peer` (minimal), `piece`, maybe tracker.

**API implications**:

- Allow minimal builds (feature flags to exclude DHT, magnet, etc.)
- `torrent-core` already sync — usable in `no_std` if dependencies permit
- Consider `no_std` compatibility for `torrent-core`
- Single-file download without full `Session` overhead

---

## 4. Analysis & Tooling

### 4.1 Torrent File Inspector

**User wants to**: Parse and display .torrent file contents for debugging or auditing.

**Key operations**:

- Parse .torrent file → display all fields
- Validate file structure and integrity
- Compute info_hash
- Compare two .torrent files
- Batch process a directory of .torrent files

**Library usage**: `metainfo` (only), `bencode`.

**API implications**:

- `Metainfo` already has `Debug`, `Display` — good
- Need `Metainfo::validate()` for deep validation beyond parsing
- Need `info_hash()` — already exists
- Need `Metainfo::to_bytes()` — already exists
- Consider `serde` support for serialization to JSON

### 4.2 Magnet Link Tool

**User wants to**: Convert between .torrent and magnet links, or inspect magnet URIs.

**Key operations**:

- Parse magnet URI → extract info_hash, trackers, display name
- Convert .torrent → magnet URI
- Validate magnet URI format
- Batch generate magnet links for a set of .torrent files

**Library usage**: `magnet`, `metainfo`.

**API implications**:

- `MagnetUri` from `FromStr` — exists
- `From<&Metainfo> for MagnetUri` — exists
- Need `MagnetUri::to_torrent_spec()` for feeding into session
- Need clear error messages for invalid magnet URIs

### 4.3 Tracker Analysis & Health Checking

**User wants to**: Check tracker availability and scrape swarm statistics.

**Key operations**:

- Announce to tracker, report response
- Scrape tracker for seed/leech counts
- Test tracker response time
- Monitor tracker uptime
- Compare multiple trackers for the same torrent

**Library usage**: `tracker` (HTTP + UDP), `metainfo`.

**API implications**:

- Need scrape support (BEP 48) — `Tracker::scrape(req) -> ScrapeResponse`
- One-shot announce without session (already have `HttpTracker` and `UdpTracker` standalone)
- Need timeout configuration per tracker
- Need tracker response metadata (response time, protocol used)

### 4.4 Swarm / Peer Analysis

**User wants to**: Monitor swarm health, peer distribution, and network topology.

**Key operations**:

- Discover all peers for a torrent (tracker + DHT + PEX)
- Geo-locate peers by IP
- Track peer churn over time
- Measure peer upload/download ratios
- Identify malicious/bad peers

**Library usage**: `tracker`, `dht`, `peer`.

**API implications**:

- Need raw peer address access without full connection
- Need peer metadata (client version from PeerId, connect time, etc.)
- Need PEX (Peer Exchange, BEP 11) support for full peer discovery
- Need ability to connect to peer just for handshake (collect metadata, then disconnect)

### 4.5 DHT Crawler & Network Explorer

**User wants to**: Map the DHT network, discover infohashes, or collect DHT statistics.

**Key operations**:

- Bootstrap into DHT with known nodes
- Recursively `find_node` to discover all nodes
- `get_peers` for target infohashes
- `sample_infohashes` (BEP 51) for crawling
- Collect DHT network statistics (node count, geography, uptime)

**Library usage**: `dht` (full DHT stack), `krpc`.

**API implications**:

- Need full control over DHT queries (already have `DhtRpc` + helpers)
- Need `sample_infohashes` support (BEP 51)
- Need ability to run DHT independently of session (already possible with `DhtRpc`)
- Need node blacklisting / rate limiting
- Need DHT statistics collection API

---

## 5. Protocol-Level Development

### 5.1 Implementing BEP Extensions

**User wants to**: Build new protocol extensions on top of torrent-rs.

**Key operations**:

- Add custom peer messages via extension protocol (BEP 10)
- Add custom DHT queries
- Extend metainfo with custom fields
- Add new magnet URI parameters

**Library usage**: All modules, especially `peer`, `dht`, `bencode`.

**API implications**:

- Need extension protocol handshake (BEP 10) — `extended_handshake` message type
- Need hook system for custom message types
- `Metainfo` should preserve unknown fields (already has `raw_info`)
- DHT KRPC should support custom query types
- Reserved bits API should support setting arbitrary bits

### 5.2 Protocol Testing & Fuzzing

**User wants to**: Test protocol implementations for correctness and security.

**Key operations**:

- Craft malformed peer messages and test parser
- Fuzz bencode parser with random input
- Simulate network conditions (latency, packet loss, reordering)
- Conformance testing against spec
- Regression testing

**Library usage**: `bencode`, `peer::message`, `metainfo`, etc. (all `torrent-core`).

**API implications**:

- All parsers should handle arbitrary input without panicking
- Error types should be granular enough for test assertions
- Round-trip test helpers (already well-covered in existing tests)
- Consider exposing raw byte-level APIs for testability
- `proptest` strategies for generating valid protocol messages

### 5.3 Academic Research & Benchmarking

**User wants to**: Research P2P network behavior, compare strategies, or benchmark performance.

**Key operations**:

- Implement custom piece selection strategies
- Implement custom choke algorithms
- Compare DHT routing strategies
- Benchmark throughput under various conditions
- Simulate large swarms

**Library usage**: `piece` (selector trait), `dht` (routing table), `session`, `peer`.

**API implications**:

- `PieceSelector` trait already public and extensible
- Need `ChokeAlgorithm` trait for custom choke/unchoke logic
- Need pluggable peer selection (which peers to connect to)
- Need metrics export for benchmarking (throughput, latency, message counts)
- All strategies should be behind traits for easy replacement

---

## 6. Data Processing

### 6.1 Streaming Download & On-the-Fly Processing

**User wants to**: Process data as it arrives, without waiting for full download.

**Key operations**:

- Receive piece data via callback/stream as each piece is verified
- Process pieces in order or out of order
- Pipe piece data to another system (database, message queue, etc.)
- Discard pieces after processing (no disk storage)

**Library usage**: `session`, `piece`, `storage`.

**API implications**:

- Need piece-completion callback: `on_piece_verified(info_hash, index, data)`
- Need streaming storage backend that doesn't require disk
- Need `Storage` trait implementable by users for custom backends
- Need ability to download without writing to disk at all
- Need block-level (not just piece-level) streaming for lower latency

### 6.2 Selective Download (file-level / piece-level)

**User wants to**: Download only specific files from a multi-file torrent, or specific pieces.

**Key operations**:

- List files in a torrent
- Toggle which files to download
- Download only a byte range of a file
- Set file priorities (high, normal, low, skip)

**Library usage**: `metainfo` (file list), `piece` (selection), `storage`, `session`.

**API implications**:

- Need `TorrentSpec::files() -> Vec<FileInfo>` (file listing)
- Need `set_file_priority(info_hash, file_index, Priority)` on session
- Need `skip_file` / `unskip_file` API
- Need partial piece handling (a piece spanning a wanted and unwanted file)
- Need `Session::add_torrent_with_file_filter(spec, filter)`

### 6.3 BenCode Processing

**User wants to**: Use the bencode parser standalone for non-BT purposes.

**Key operations**:

- Parse/encode arbitrary bencoded data (not just .torrent files)
- Validate bencode format
- Convert bencode ↔ JSON
- Pretty-print bencode
- Query nested bencode dicts

**Library usage**: `bencode` only.

**API implications**:

- `bencode` module is already public and usable standalone
- Need `Bencode` → JSON serialization (`impl Serialize for Bencode`)
- Need JSON → bencode conversion
- Need pretty-printer: `Bencode::to_string_pretty()`
- Need path-based query: `bencode_query(data, "info.pieces")`
- Consider separating `bencode` into its own crate for wider reuse

---

## 7. Hybrid & Advanced

### 7.1 BT + WebSeed (HTTP Fallback)

**User wants to**: Download via HTTP when no peers available, then switch to BT.

**Key operations**:

- Request pieces via HTTP Range requests from web seeds
- Fall back to HTTP if BT is too slow
- Verify HTTP-downloaded pieces against info_hash
- Prioritize rare pieces from peers, common pieces from HTTP

**Library usage**: `metainfo` (url-list), `piece`, `storage`, `session`.

**API implications**:

- Need WebSeed download backend implementing `Storage` or similar
- Need BEP 19 URL list parsing from metainfo
- Need hybrid piece source (peer or HTTP, whichever is faster)
- Need `PieceSource` abstraction

### 7.2 BT + IPFS / Content-Addressable Storage

**User wants to**: Bridge between BitTorrent and IPFS ecosystems.

**Key operations**:

- Convert BT infohash ↔ IPFS CID
- Serve BT content via IPFS gateway
- Use IPFS as a piece storage backend
- Dual-protocol content distribution

**Library usage**: `storage`, `metainfo`, `piece`.

**API implications**:

- `Storage` trait should be flexible enough for IPFS backend
- Info hash ↔ CID conversion utility (BEP 53?)
- Content-addressed piece access (retrieve by hash, not by index)

### 7.3 BT + Blockchain / Incentivized Seeding

**User wants to**: Build a system where seeders earn tokens for uploading.

**Key operations**:

- Prove upload bandwidth to a smart contract
- Track per-peer upload/download for accounting
- Generate verifiable upload proofs
- On-chain torrent registry

**Library usage**: `session`, `peer`, `storage`.

**API implications**:

- Need per-peer upload/download byte counters
- Need upload proof mechanism (Merkle proofs for pieces sent?)
- Need event hooks for upload/download events
- `SessionConfig` might need crypto wallet / identity fields

### 7.4 BT + WebRTC / WebTorrent Interop

**User wants to**: Enable browser-based peers to participate in BT swarms.

**Key operations**:

- Connect to WebTorrent peers via WebRTC
- Translate between TCP peer protocol and WebRTC data channels
- Serve as bridge between TCP and WebRTC swarms

**Library usage**: `peer`, `metainfo`, `session`.

**API implications**:

- `PeerConnection` is currently TCP-only; need transport abstraction
- `PeerTransport` trait: `TcpTransport`, `WebRtcTransport`
- WebTorrent uses a slightly different handshake; need compatibility mode

### 7.5 BT + Proxy / VPN / Tor / I2P

**User wants to**: Route BT traffic through proxy, VPN, or anonymous network.

**Key operations**:

- Connect to peers via SOCKS5 proxy
- Bind to specific network interface (VPN)
- Route tracker requests through proxy
- Anonymize DHT traffic via Tor/I2P

**Library usage**: `peer` (connection), `tracker`, `dht`.

**API implications**:

- Need proxy configuration in `SessionConfig`: `socks5_proxy: Option<SocketAddr>`
- Need per-connection proxy support in `PeerConnection`
- Need `bind_address` configuration (bind to VPN interface)
- Tracker HTTP client should support proxy
- DHT should support proxy or separate network interface

---

## 8. DevOps & Automation

### 8.1 CI/CD Artifact Distribution

**User wants to**: Distribute build artifacts among CI runners via BT.

**Key operations**:

- Create torrent from build output
- Seed from build server
- Download on test runners
- Verify integrity via info_hash
- Short-lived torrents (hours, not days)

**Library usage**: `metainfo` (creation), `session`, `tracker` (embedded).

**API implications**:

- Need `Metainfo::builder()` for programmatic torrent creation
- Need embedded tracker support (in-process tracker for LAN distribution)
- Need LAN-only mode (no external connections)
- Need quick session setup/teardown

### 8.2 Infrastructure as Code Integration

**User wants to**: Use torrent-rs in Terraform providers or Ansible modules.

**Key operations**:

- Download files via BT as part of provisioning
- Verify downloads via info_hash
- Report download status to IaC tool

**Library usage**: `metainfo`, `session` (or lower-level).

**API implications**:

- Need synchronous download API for non-async contexts
- Need simple blocking API: `download_torrent(torrent_path, dest_dir) -> Result<()>`
- Need progress reporting callbacks

### 8.3 Monitoring & Observability

**User wants to**: Export metrics, health status, and logs from a running session.

**Key operations**:

- Export Prometheus metrics (download rate, peer count, etc.)
- Health check endpoint (is session running? any stalled torrents?)
- Structured logging for log aggregation
- Alert on download failures or stalled torrents

**Library usage**: `session` (status), all modules (metrics).

**API implications**:

- Need metrics accessor: `session.metrics() -> SessionMetrics`
- `SessionMetrics` should include: total_downloaded, total_uploaded, num_peers, num_torrents, etc.
- Need per-torrent metrics
- Need event hooks for log integration
- `tracing` already used internally — good

---

## 9. Specialized Clients

### 9.1 Private Tracker Client

**User wants to**: Build a client optimized for private tracker rules.

**Key operations**:

- Authenticate with passkey in announce URL
- Enforce ratio requirements
- Respect `private=1` flag (disable DHT, PEX, LSD)
- Report accurate upload/download stats
- Unique PeerId per private tracker policy

**Library usage**: `session`, `tracker`, `metainfo`, `dht`.

**API implications**:

- Need `private` flag in metainfo to auto-disable DHT/PEX/LSD
- Need accurate upload/download byte counting for ratio
- Need announce URL manipulation (inject passkey)
- Need per-tracker announce intervals (private trackers have shorter intervals)

### 9.2 Live Streaming Client

**User wants to**: Stream live video/audio via BitTorrent (BEP 39/40).

**Key operations**:

- Subscribe to live stream torrent
- Download pieces in near-real-time
- Buffer management (sliding window)
- Low-latency piece delivery

**Library usage**: `session`, `piece`, `peer`.

**API implications**:

- Need streaming piece source: poll for new pieces as they're published
- Need low-latency peer connection management
- Need buffer size configuration
- May need different piece selection strategy for live content

### 9.3 RSS + BT Automation

**User wants to**: Build a Sonarr/Radarr-like tool: monitor RSS feeds, auto-download matching torrents.

**Key operations**:

- Parse RSS/Atom feeds for torrent links
- Match against user-defined filters (quality, language, etc.)
- Auto-add matching torrents to session
- Move completed downloads to media library
- Notify on completion

**Library usage**: `session`, `magnet`, `metainfo`.

**API implications**:

- Need completion callback/hook on session
- Need post-download move/rename API
- Session should support many torrents efficiently (hundreds)
- Queue management with priority

---

## Project Boundaries

### In Scope (library responsibility)

| Area                        | Scope                               |
| --------------------------- | ----------------------------------- |
| BitTorrent protocol (BEP 3) | Full implementation                 |
| BenCode encoding/decoding   | Standalone, reusable                |
| .torrent file parsing       | Complete (BEP 3/52)                 |
| Magnet URI parsing          | Complete (BEP 9)                    |
| Peer wire protocol          | All 11 message types                |
| Extension protocol (BEP 10) | Handshake + message framing         |
| Peer Exchange PEX (BEP 11)  | In scope                            |
| Multi-tracker (BEP 12)      | announce-list support               |
| Local Service Discovery     | In scope (BEP 14)                   |
| UDP tracker (BEP 15)        | Client implementation               |
| WebSeed download (BEP 19)   | In scope, as optional feature       |
| Compact peer lists (BEP 23) | Parsing                             |
| DHT (BEP 5)                 | Client + server node                |
| Piece management            | Selection strategies + verification |
| Storage abstraction         | Trait + file backend                |
| Session orchestration       | High-level download/upload loop     |
| HTTP/HTTPS tracker          | Client implementation               |
| SOCKS5 proxy                | In scope, as `SessionConfig` option |
| UPnP/NAT-PMP port mapping   | Optional feature, off by default    |
| WebTorrent interop          | In scope as extension               |
| Embedded tracker            | In scope for LAN/CI distribution    |
| Serde support for types     | Optional feature gate               |
| Metrics/Prometheus export   | In scope, minimal implementation    |

### Out of Scope (NOT library responsibility)

| Area                                  | Rationale                                                            |
| ------------------------------------- | -------------------------------------------------------------------- |
| GUI / TUI / Web UI                    | Applications, not library                                            |
| RSS feed parsing                      | Separate concern                                                     |
| Media playback                        | Separate concern                                                     |
| Video transcoding                     | Separate concern                                                     |
| File compression/decompression        | Separate concern                                                     |
| HTTP server / RPC server              | Could be a separate crate                                            |
| System tray integration               | Platform-specific                                                    |
| Desktop notifications                 | Platform-specific                                                    |
| File association (OS integration)     | Platform-specific                                                    |
| VPN client / Tor / I2P integration    | Separate concern (SOCKS5 proxy IS in scope as transport)             |
| File synchronization (like Syncthing) | Different protocol                                                   |
| C FFI bindings                        | Separate `torrent-ffi` crate — different API surface                 |
| bencode as standalone crate           | Keep in `torrent-core`; re-evaluate if external demand emerges       |
| WASM full support                     | `torrent-core` should compile to WASM; `torrent` WASM is future work |

---

## Derived API Design Principles

From the scenarios above, these principles should guide API design:

### 1. Modularity — Pay for What You Use

- `torrent-core` alone for parsing/inspection tools (no async runtime)
- `torrent` for full download/upload
- Feature flags for optional protocol extensions (DHT, PEX, LSD, WebSeed)
- Individual tracker/DHT operations without full `Session`

### 2. Observable — Don't Make Users Poll

- Event streams or callbacks for: piece completion, torrent completion, errors, peer connects/disconnects
- Built-in `tracing` spans and events
- Metrics accessor for structured monitoring
- All status types should be cheap to clone and poll

### 3. Configurable — Sensible Defaults, Full Override

- `Default` on `SessionConfig` should work for basic use
- Every knob configurable: timeouts, limits, ports, algorithms
- Serialization support for configuration (save/restore)
- Per-torrent overrides for global settings

### 4. Extensible — Traits, Not Final Types

- `PieceSelector` trait for custom strategies
- `Storage` trait for custom backends
- Extensible peer message handling (extension protocol)
- Pluggable choke/unchoke algorithm

### 5. Robust — Never Panic on Malformed Input

- All parsers return `Result`, never `panic!`
- Error types are granular and actionable
- Session survives individual torrent failures
- Fuzz-tested parsers

### 6. Async-First, Sync-Possible

- Primary API is async (tokio)
- `torrent-core` fully synchronous
- Consider blocking convenience wrappers for simple use cases
- No blocking inside async code

### 7. Zero-Copy Where Possible

- `Bytes` for buffer management
- Avoid unnecessary allocations in hot paths
- Piece data passed by reference when possible

### 8. Serde-Ready

- Configuration types: `Serialize + Deserialize`
- Status types: `Serialize` (for API responses)
- Behind optional feature gate to avoid bloat

---

## Scenario-to-Module Mapping

| Scenario Category        | Primary Modules Used                                                          |
| ------------------------ | ----------------------------------------------------------------------------- |
| Desktop/Mobile Client    | `session`, `metainfo`, `magnet`, `storage`, `tracker`, `dht`, `peer`, `piece` |
| Headless/Seedbox         | `session`, all modules                                                        |
| CLI Tool                 | `metainfo`, `magnet`, `tracker`, `session` (optional)                         |
| Software Distribution    | `metainfo` (creation), `session`, `storage`                                   |
| Dataset Distribution     | `metainfo`, `storage`, `piece`, `session`                                     |
| Media Streaming          | `piece` (Sequential), `storage`, `session`                                    |
| Enterprise Distribution  | `tracker`, `metainfo`, `session`                                              |
| Library Embedding        | `session` (minimal config)                                                    |
| IoT / Embedded           | `metainfo`, `piece`, `peer` (minimal, maybe no `session`)                     |
| Torrent Inspector        | `metainfo`, `bencode`                                                         |
| Magnet Tool              | `magnet`, `metainfo`                                                          |
| Tracker Analysis         | `tracker`                                                                     |
| Swarm Analysis           | `tracker`, `dht`, `peer`                                                      |
| DHT Crawler              | `dht`                                                                         |
| Protocol Extension Dev   | `peer`, `dht`, `bencode`                                                      |
| Protocol Testing/Fuzzing | `bencode`, `peer`, `metainfo` (core types)                                    |
| Academic Research        | `piece`, `dht`, `peer`, `session`                                             |
| Streaming Processing     | `storage` (custom), `piece`, `session`                                        |
| Selective Download       | `metainfo`, `piece`, `session`                                                |
| BenCode Processing       | `bencode`                                                                     |
| Hybrid BT+WebSeed        | `metainfo`, `piece`, `session`                                                |
| BT+IPFS                  | `storage`, `metainfo`                                                         |
| BT+Blockchain            | `session`, `peer` (metrics)                                                   |
| BT+WebRTC                | `peer` (transport abstraction)                                                |
| BT+Proxy                 | `peer`, `tracker`, `session` (config)                                         |
| CI/CD Distribution       | `metainfo` (creation), `session`                                              |
| IaC Integration          | `metainfo`, sync download API                                                 |
| Monitoring               | `session` (metrics), `tracing`                                                |
| Private Tracker          | `session`, `tracker`, `metainfo`                                              |
| Live Streaming           | `session`, `piece`, `peer`                                                    |
| RSS Automation           | `session` (hooks)                                                             |
