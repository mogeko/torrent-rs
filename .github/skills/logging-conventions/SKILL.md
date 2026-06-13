---
name: logging-conventions
description: "Logging/instrumentation conventions for the torrent-rs codebase. Use when: adding log statements (tracing::debug!/info!/trace!/warn!), adding instrumentation to new code, choosing log levels, reviewing log output, setting up tracing-subscriber in examples, or troubleshooting with RUST_LOG."
argument-hint: "[file or module to instrument]"
---

# Logging Conventions

## When to Use

- Adding or reviewing log statements in any crate
- Instrumenting a new function or module
- Choosing the right log level for a message
- Setting up `tracing-subscriber` in examples
- Troubleshooting with `RUST_LOG` environment variable
- Reviewing PRs that add/modify logging
- Deciding between `tracing` and `log` (always choose `tracing`)

## Core Rules

1. **Facade**: Use `tracing` as the only logging facade. Never use the `log` crate directly.
2. **No features on our side**: `tracing = "0.1"` with no features. The `log` bridge is a global decision made by the binary, not the library.
3. **Fully qualified macros**: Use `tracing::debug!(...)`, not `use tracing::debug; debug!(...)`. This makes the macro origin self-documenting at every call site.
4. **Language**: All log messages must be in English.
5. **No subscriber in library code**: `tracing` is a facade — the subscriber (output format, filtering) is the user's responsibility.

## Dependency Setup

### Root `Cargo.toml`

```toml
[workspace.dependencies]
tracing = "0.1"
```

### `torrent-core/Cargo.toml`

```toml
[dependencies]
tracing.workspace = true
```

### `torrent/Cargo.toml`

```toml
[dependencies]
tracing.workspace = true

[dev-dependencies]
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

> **Rule**: `tracing-subscriber` lives only in `torrent`'s `[dev-dependencies]`. It is never a workspace dependency — only one crate uses it, and only for examples.

## Log Level Conventions

| Level    | When to Use                              | Example                                                | Rationale                                                        |
| -------- | ---------------------------------------- | ------------------------------------------------------ | ---------------------------------------------------------------- |
| `error!` | Reserved — not currently used            | —                                                      | Library code returns `Result`; the caller decides error severity |
| `warn!`  | Recoverable failures, degraded operation | `warn!("tracker announce failed: {}", e)`              | User may want to investigate but operation continues             |
| `info!`  | State transitions, lifecycle events      | `info!("torrent added: {} ({} pieces)", name, pieces)` | High-level view of what the library is doing                     |
| `debug!` | Operational details, intermediate steps  | `debug!("parsing info dict")`                          | Useful for diagnosing issues, not overwhelming at default level  |
| `trace!` | High-frequency, per-message/byte events  | `trace!("encoding peer message: {:?}", msg)`           | Only enabled for deep debugging; noisy                           |

### Decision Flow

```
Is this a state transition the user should know about?
  → Yes: info!
  → No: Is this a recoverable failure?
      → Yes: warn!
      → No: Is this routine operational detail?
          → Yes: debug!
          → No: Is this per-message or per-byte?
              → Yes: trace!
```

### What NOT to log

- **Don't log successful returns** — `trace!("function returned Ok")` adds noise
- **Don't log internal implementation details** — `debug!("using BTreeMap internally")` leaks abstraction
- **Don't log sensitive data** — no info hashes in `info!` (use `debug!` or `trace!`)
- **Don't use `error!` in library code** — return `Err(...)` instead; let the user's subscriber decide severity

## Message Format

```rust
// ✅ Good: describes what happened, includes relevant context
tracing::info!("connecting to peer {}", addr);
tracing::debug!("parsing .torrent file ({} bytes)", data.len());
tracing::warn!("handshake: wrong size (expected {} got {})", expected, got);

// ❌ Bad: vague, no context
tracing::info!("ok");
tracing::debug!("done");
tracing::warn!("error");
```

Use `{:?}` for debug-printing complex types, `{}` for Display-printing simple values.

## Crate Placement

| Context                        | Crate          | Constraint                                   |
| ------------------------------ | -------------- | -------------------------------------------- |
| Parsing, encoding, data types  | `torrent-core` | `tracing` only (no tokio, no subscriber)     |
| Async I/O, session, networking | `torrent`      | `tracing` + `tracing-subscriber` in dev-deps |

## Example Initialization

Every example file in `crates/torrent/examples/` must initialize a subscriber:

```rust
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // ... example code
}
```

This lets users control log output with `RUST_LOG=torrent=debug cargo run --example download_torrent`.

## User-Facing Guide

Users have two options for consuming our logs:

### Option A: `tracing-subscriber` (structured, recommended)

```toml
[dependencies]
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

```rust
tracing_subscriber::fmt()
    .with_env_filter("torrent=info")
    .init();
```

### Option B: `env_logger` (simple, traditional)

```toml
[dependencies]
tracing = { version = "0.1", features = ["log"] }
env_logger = "0.11"
```

```rust
env_logger::init(); // RUST_LOG=torrent=debug
```

The `tracing/log` bridge is a **global** feature — enabling it once in the binary makes ALL dependency `tracing` events flow to `log`.

### Option C: Nothing (silent, zero overhead)

No subscriber → no logging. `tracing` macro calls are zero-cost when no subscriber is registered.

## Validation

After adding or changing instrumentation:

```bash
cargo check                       # Compiles?
cargo test                        # Tests still pass? (tracing is inert)
cargo clippy -- -D warnings       # No new warnings?
RUST_LOG=debug cargo run -p torrent --example parse_metainfo  # Output looks right?
```

## Anti-patterns

### ❌ `log` crate

```rust
use log::info;  // Don't — use tracing instead
```

### ❌ `use`-imported macros

```rust
use tracing::debug;  // Don't — use tracing::debug!(...) inline
debug!("...");
```

### ❌ `error!` in library code

```rust
tracing::error!("download failed");  // Don't — return Err(...) instead
```

### ❌ Logging sensitive identifiers at `info!`

```rust
tracing::info!("info_hash = {:02x?}", info_hash);  // Don't — use debug!
```

### ❌ `tracing-subscriber` as a workspace dependency

```toml
# Cargo.toml (root) — Don't
[workspace.dependencies]
tracing-subscriber = "0.3"
```

Keep it in `torrent`'s `[dev-dependencies]` only.

## Related Skills

- [bt-protocol](../bt-protocol/SKILL.md) — Protocol implementation conventions
- [documentation-writing](../documentation-writing/SKILL.md) — Doc comment and Markdown conventions
