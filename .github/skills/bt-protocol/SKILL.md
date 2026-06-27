---
name: bt-protocol
description: "BitTorrent protocol development guide. Use when: implementing BEP standards, adding peer wire messages, tracker/DHT communication, magnet URI handling, metainfo parsing, or any protocol-level feature in the torrent.rs codebase."
argument-hint: "[BEP number or protocol feature to implement]"
---

# BitTorrent Protocol Development

## When to Use

- Implementing a new BEP (BitTorrent Enhancement Proposal)
- Adding or modifying peer wire protocol messages
- Working on HTTP/UDP tracker communication
- DHT Kademlia routing table or KRPC changes
- Magnet URI parsing or new URI parameters
- `.torrent` file (metainfo) parsing extensions

## Implemented BEPs

| BEP    | Title                                    | Module(s)                                                      |
| ------ | ---------------------------------------- | -------------------------------------------------------------- |
| BEP 3  | The BitTorrent Protocol Specification    | `bencode`, `metainfo`, `peer`, `tracker`, `storage`, `session` |
| BEP 5  | DHT Protocol                             | `dht` (krpc, routing table, rpc, query)                        |
| BEP 6  | Fast Extension                           | `peer::message` (Suggest, HaveAll/None, Reject, AllowedFast),  |
|        |                                          | `peer::handshake` (bit 44), `session::swarm`                   |
| BEP 7  | IPv6 Tracker Extension                   | `tracker` (AnnounceRequest ip/ipv6, compact peers6, UDP IPv6)  |
| BEP 9  | Extension for Peers to Send Metadata     | `magnet`                                                       |
| BEP 10 | Extension Protocol (LTEP)                | `peer::extension`, `peer::message` (Extended), `peer::stream`  |
| BEP 11 | Peer Exchange (PEX)                      | `peer::pex`, `session::swarm::pex`                             |
| BEP 12 | Multitracker Metadata Extension          | `metainfo` (announce-list)                                     |
| BEP 15 | UDP Tracker Protocol                     | `tracker::udp`                                                 |
| BEP 16 | Superseeding                             | `session` (seed builder, swarm super seed state)               |
| BEP 19 | WebSeed — HTTP/FTP Seeding               | `magnet` (ws parameter parsing only; download not implemented) |
| BEP 23 | Tracker Returns Compact Peer Lists       | `tracker` (compact response)                                   |
| BEP 32 | DHT Extensions for IPv6                  | `dht` (nodes6, want, dual routing tables, dual DHT nodes)      |
| BEP 52 | The BitTorrent Protocol Specification v2 | `metainfo` (bt2 metainfo)                                      |

## Architecture: Where Protocol Lives

```
torrent-core (sync, no tokio)     torrent (async, tokio)
────────────────────────────       ─────────────────────
bencode (BEP 3 wire format)       tracker::http (BEP 3/23)
metainfo (BEP 3/12/52 parsing)    tracker::udp (BEP 15)
magnet (BEP 9 URI parsing)        peer::stream (BEP 3 TCP)
peer::handshake (68-byte)         dht::rpc (BEP 5 UDP)
peer::message (17 types, BEP 6)   session (orchestration)
peer::extension (BEP 10 LTEP)
peer::pex (BEP 11 PEX)
dht::krpc (BEP 5 message format)
dht::RoutingTable (Kademlia)
storage (BEP 3 piece mgmt)
error (Error + ErrorKind)
```

**Rule**: Protocol data types and parsing → `torrent-core`. Protocol I/O (TCP/UDP) → `torrent`.

## Protocol Implementation Checklist

When adding a new protocol feature, follow this order:

1. **Read the BEP spec** — understand the wire format, message types, and edge cases
2. **Add ErrorKind variants** — in `crates/torrent-core/src/error.rs`, add protocol-specific error variants
3. **Define data types** — in `torrent-core`, create the structs/enums for the protocol messages
4. **Implement encode/decode** — `to_bytes()` / `from_bytes()` or `Display` / `FromStr`
5. **Document with BEP references** — use `/// Implements BEP XXXX: Title` on all public types
6. **Write unit tests** — round-trip encode/decode, edge cases, error conditions
7. **Add I/O layer (if needed)** — async send/receive in `torrent`
8. **Integration tests** — in `crates/torrent-core/tests/` (sync) or `crates/torrent/tests/` (async) for cross-module scenarios
9. **Run full suite** — `cargo test && cargo clippy -- -D warnings`

## Peer Wire Protocol (BEP 3)

### Message Format

```text
<4-byte big-endian length> <1-byte message ID> <payload>
```

### 17 Message Types (BEP 3 + BEP 6 + BEP 10)

**Standard (BEP 3)**

| ID  | Name          | Length | Payload                        |
| --- | ------------- | ------ | ------------------------------ |
| —   | KeepAlive     | 0      | —                              |
| 0   | Choke         | 1      | —                              |
| 1   | Unchoke       | 1      | —                              |
| 2   | Interested    | 1      | —                              |
| 3   | NotInterested | 1      | —                              |
| 4   | Have          | 5      | piece index (u32)              |
| 5   | Bitfield      | 1+X    | bitfield bytes                 |
| 6   | Request       | 13     | index + begin + length (u32×3) |
| 7   | Piece         | 9+X    | index + begin + block data     |
| 8   | Cancel        | 13     | index + begin + length (u32×3) |
| 9   | Port          | 3      | listen port (u16)              |

**Fast Extension (BEP 6)**

| ID  | Name        | Length | Payload                        |
| --- | ----------- | ------ | ------------------------------ |
| 13  | Suggest     | 5      | piece index (u32)              |
| 14  | HaveAll     | 1      | —                              |
| 15  | HaveNone    | 1      | —                              |
| 16  | Reject      | 13     | index + begin + length (u32×3) |
| 17  | AllowedFast | 5      | piece index (u32)              |

**Extension Protocol (BEP 10)**

| ID  | Name     | Length | Payload                |
| --- | -------- | ------ | ---------------------- |
| 20  | Extended | 2+X    | ext_id + bencoded data |

Defined in [`crates/torrent-core/src/peer/message.rs`](../../../crates/torrent-core/src/peer/message.rs).

### Handshake

68 bytes: `pstrlen(1) + pstr(19) + reserved(8) + info_hash(20) + peer_id(20)`.

Extension bits are numbered per BEP conventions: bit 0 = MSB of byte 0. Common extensions:

- Bit 44 (byte 5, `0x08`): Fast Extension (BEP 6)
- Bit 63 (byte 7, `0x01`): DHT (BEP 5) / Extension Protocol / LTEP (BEP 10)

Defined in [`crates/torrent-core/src/peer/handshake.rs`](../../../crates/torrent-core/src/peer/handshake.rs).

## Superseeding (BEP 16)

Super seeding minimizes upload bandwidth during initial seeding by
uploading each piece to only one peer at a time. Instead of responding
to any peer's request, the super seeder:

1. **Hides** unrevealed pieces from its advertised bitfield
2. **Assigns** each piece exclusively to one unchoked, interested peer
3. **Reveals** the piece to the swarm after the assigned peer confirms
   receipt via a `HAVE` message

The algorithm uses only standard BEP 3 wire messages — no new protocol
types are needed.

### Per-torrent Configuration

| Method                          | Crate     | Purpose                                  |
| ------------------------------- | --------- | ---------------------------------------- |
| `SeedBuilder::super_seed(bool)` | `torrent` | Enable/disable super seeding per torrent |
| `PreparedTorrent::super_seed()` | `torrent` | Read the super seed flag                 |

### Core State (SwarmLoop)

| Field                    | Type                       | Purpose                              |
| ------------------------ | -------------------------- | ------------------------------------ |
| `super_seed`             | `bool`                     | Whether super seeding is active      |
| `super_seed_assignments` | `HashMap<u32, SocketAddr>` | Piece index → assigned peer          |
| `super_seed_unrevealed`  | `HashSet<u32>`             | Pieces not yet revealed to the swarm |

Defined in [`crates/torrent/src/session/swarm/mod.rs`](../../../crates/torrent/src/session/swarm/mod.rs).
Implementation in [`crates/torrent/src/session/swarm/peer.rs`](../../../crates/torrent/src/session/swarm/peer.rs)
and [`crates/torrent/src/session/swarm/pieces.rs`](../../../crates/torrent/src/session/swarm/pieces.rs).

## DHT / KRPC (BEP 5)

### Message Format (bencoded)

```text
Query:    {"t": "<2-byte id>", "y": "q", "q": "<method>", "a": <args>}
Response: {"t": "<2-byte id>", "y": "r", "r": <result>}
Error:    {"t": "<2-byte id>", "y": "e", "e": [<code>, <msg>]}
```

### Query Types

| Method          | Module       | Purpose                                 |
| --------------- | ------------ | --------------------------------------- |
| `ping`          | `dht::rpc`   | Check if a node is alive                |
| `find_node`     | `dht::query` | Find nodes close to a target ID         |
| `get_peers`     | `dht::query` | Get peers for an info_hash              |
| `announce_peer` | `dht::query` | Announce we are a peer for an info_hash |

### Routing Table

- 160 K-buckets (K = 8)
- XOR distance metric
  Defined in [`crates/torrent-core/src/dht/mod.rs`](../../../crates/torrent-core/src/dht/mod.rs).

## Tracker Protocol (BEP 3, 15, 23)

### Announce Request Parameters

| Param        | Type | Required | Description                |
| ------------ | ---- | -------- | -------------------------- |
| `info_hash`  | 20B  | Yes      | URL-encoded SHA-1          |
| `peer_id`    | 20B  | Yes      | Our peer ID                |
| `port`       | u16  | Yes      | Listening port             |
| `uploaded`   | u64  | Yes      | Total bytes uploaded       |
| `downloaded` | u64  | Yes      | Total bytes downloaded     |
| `left`       | u64  | Yes      | Bytes remaining            |
| `event`      | enum | No       | started/stopped/completed  |
| `compact`    | 0/1  | No       | Compact peer list (BEP 23) |

- HTTP: Manual HTTP/1.1 client in [`crates/torrent/src/tracker/http.rs`](../../../crates/torrent/src/tracker/http.rs) — no `reqwest` dependency
- UDP: Connection protocol (BEP 15) in [`crates/torrent/src/tracker/udp.rs`](../../../crates/torrent/src/tracker/udp.rs)

## Magnet URI (BEP 9)

```
magnet:?xt=urn:btih:<info_hash>&dn=<name>&tr=<tracker>
```

- Info hash: hex (40 chars) or base32 (32 chars)
- `dn`: display name
- `tr`: tracker URL (repeatable)
- `ws`: web seed (BEP 19)

Defined in [`crates/torrent-core/src/magnet/mod.rs`](../../../crates/torrent-core/src/magnet/mod.rs).

## Metainfo (.torrent files, BEP 3/12/52)

- `info_hash()` = SHA-1 of the raw bencoded `info` dictionary
- Single-file: `info.length`, `info.name`
- Multi-file (BEP 52): `info.files[]` with `path`, `length`
- Announce tiers (BEP 12): `announce-list` with nested lists

Defined in [`crates/torrent-core/src/metainfo/`](../../../crates/torrent-core/src/metainfo/).

## Bencode (BEP 3)

The wire format for all BT protocols. Strict recursive-descent parser with:

- Dict keys sorted lexicographically for idempotent round-trips
- Integer validation: no leading zeros, no negative zero, `i64` range
- Uses `Vec<(Bytes, Bencode)>` for dicts

Defined in [`crates/torrent-core/src/bencode/`](../../../crates/torrent-core/src/bencode/).

## Notable Unimplemented BEPs

These BEPs are reserved or partially referenced in the codebase but not yet fully implemented:

| BEP | Title                           | Notes                                                               |
| --- | ------------------------------- | ------------------------------------------------------------------- |
| 14  | Local Service Discovery (LSD)   | LAN peer discovery via multicast                                    |
| 27  | Private Torrents                | Single `private` flag in metainfo                                   |
| 29  | uTP (Micro Transport Protocol)  | UDP-based congestion-controlled transport; required by most clients |
| 41  | UDP Tracker Protocol Extensions | Extended UDP tracker features                                       |

## Testing Protocol Features

- **Unit tests**: inline `#[cfg(test)] mod tests` in the same file (torrent-core: sync `#[test]`, torrent: `#[tokio::test]`)
- **Property-based tests**: `crates/torrent-core/tests/*_proptests.rs` using `proptest`
- **Test vectors**: binary `.bin` files in `crates/torrent-core/tests/data/` for bencode and `.torrent` files
- **No network**: tests must never require actual network access
- **Round-trip**: always test encode→decode and decode→encode idempotency

## Common Pitfalls

- **Big-endian everywhere**: peer messages, handshake lengths, tracker params all use network byte order
- **Dict key order**: bencode dict keys must be sorted lexicographically; the decoder validates this, the encoder produces sorted output
- **URL encoding**: tracker announce uses URL-encoded 20-byte info_hash (not hex)
- **Compact peer list**: 6 bytes per peer (4 IP + 2 port), not bencoded
- **KRPC transaction IDs**: random 2-byte values, matched by the RPC layer for request/response pairing
- **tokio in torrent-core is forbidden**: if a new type needs async I/O, it belongs in `torrent`, not `torrent-core`
