---
name: documentation-writing
description: "Standardized documentation writing for the torrent-rs codebase. Use when: writing or updating Rust doc comments (///, //!), adding module-level docs, creating README files, documenting public API, adding code examples in docs, writing architecture decision records, or updating AGENTS.md. Covers both Rustdoc and project Markdown conventions."
argument-hint: "[file path or module to document]"
---

# Documentation Writing Standards

## When to Use

- Adding or updating `///` doc comments on public API items
- Writing `//!` module-level documentation
- Creating or updating `README.md` files in crates
- Documenting protocol implementations with BEP references
- Adding code examples (````rust` blocks) in documentation
- Writing architecture decision records or design docs
- Updating `AGENTS.md` or project-level guidelines
- Reviewing documentation for completeness and correctness

## Core Rules (from AGENTS.md)

These are non-negotiable. Always apply them first.

1. **Language**: All documentation, comments, and git commits must be written in **English**.
2. **Public API**: `///` doc comments are **mandatory** for every `pub` item (struct, enum, fn, trait, type alias, module).
3. **BEP References**: Use the format `/// Implements BEP XXXX: Title` on all protocol-related public types.
4. **Examples**: Every public function that has non-trivial behavior should include a `/// # Examples` section with a runnable code block.
5. **Panics**: Document all panic conditions with `/// # Panics`.
6. **Errors**: Document all error conditions with `/// # Errors` on fallible functions.

## Quick Reference: Documentation Tags

| Tag              | When to Use                                        | Example                                          |
| ---------------- | -------------------------------------------------- | ------------------------------------------------ |
| `/// `           | Documenting a public item (struct, fn, enum, etc.) | `/// Computes the SHA-1 hash of the info dict.`  |
| `//! `           | Module-level documentation                         | `//! Bencode encoding and decoding (BEP 3).`     |
| `# Examples`     | Runnable code example                              | `/// # Examples\n/// `rust\n/// ...\n/// `       |
| `# Panics`       | When the function can panic                        | `/// # Panics\n/// Panics if index > 7.`         |
| `# Errors`       | When the function returns `Result`                 | `/// # Errors\n/// Returns `Err` if ...`         |
| `# Safety`       | When the function is `unsafe`                      | `/// # Safety\n/// The caller must ensure ...`   |
| `#[doc(hidden)]` | Hide from public docs but keep public API          | `#[doc(hidden)] pub fn internal_helper()`        |
| `#[deprecated]`  | Mark API as deprecated                             | `#[deprecated(since = "0.2.0", note = "use X")]` |

## Rust Doc Comment Workflow

### Step 1: Module-Level Documentation (`//!`)

Every `mod.rs` or module root file should start with `//!` that describes:

- The module's purpose
- Relevant BEP numbers
- Architecture notes (sync vs async, crate placement)

> Use the [module doc template](./references/module-doc-template.md) for new modules.

````rust
//! Peer wire protocol message types (BEP 3).
//!
//! This module defines the 17 message types (BEP 3 + BEP 6 + BEP 10)
//! used in peer-to-peer communication after the handshake. All types are sync-only
//! and belong in `torrent-core`.
//!
//! # Message Format
//!
//! ```text
//! <4-byte big-endian length> <1-byte message ID> <payload>
//! ```
````

### Step 2: Public Type Documentation (`///`)

For every `pub` struct, enum, or trait, write a doc comment that answers:

1. **What** is this type?
2. **Why** does it exist?
3. **How** is it used? (with a code example if non-trivial)
4. **Which BEP** does it implement? (if protocol-related)

````rust
/// A bencoded dictionary with lexicographically sorted keys.
///
/// Implements BEP 3: The BitTorrent Protocol Specification.
///
/// # Examples
///
/// ```
/// use torrent_core::bencode::Bencode;
///
/// let dict = Bencode::Dict(vec![
///     ("announce".into(), Bencode::Bytes("http://tracker:6969/announce".into())),
/// ]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bencode { /* ... */ }
````

### Step 3: Function Documentation

Public functions must document:

- What the function does
- All parameters (implicitly via description, or explicitly with `/// * `param`-`)
- Return value meaning
- Error conditions (`# Errors`)
- Panic conditions (`# Panics`)

````rust
/// Parses a magnet URI and extracts the info hash, display name, and trackers.
///
/// Implements BEP 9: Extension for Peers to Send Metadata.
///
/// Supports both hex (40 chars) and base32 (32 chars) info hash formats.
///
/// # Errors
///
/// Returns [`ErrorKind::Magnet`] if the URI is malformed, missing required
/// parameters, or contains an invalid info hash encoding.
///
/// # Examples
///
/// ```
/// use torrent_core::magnet::MagnetLink;
///
/// let magnet = "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&dn=test";
/// let link = MagnetLink::parse(magnet)?;
/// assert_eq!(link.display_name(), Some("test"));
/// # Ok::<(), torrent_core::Error>(())
/// ```
pub fn parse(uri: &str) -> Result<MagnetLink, Error> { /* ... */ }
````

### Step 4: Code Examples (`# Examples`)

Rules for examples in doc comments:

- Use ````rust` fenced code blocks
- Examples should compile and run (they are tested with `cargo test`)
- Use `# ` to hide setup/teardown lines from rendered output
- For fallible examples, use `# Ok::<(), Box<dyn std::error::Error>>(())` or the crate-specific error type
- Prefer concrete, realistic examples over abstract ones

````rust
/// ```
/// # use torrent_core::metainfo::Metainfo;
/// # let bytes = include_bytes!("../tests/data/ubuntu-24.04.iso.torrent");
/// let metainfo = Metainfo::from_bytes(bytes)?;
/// assert_eq!(metainfo.info.name, "ubuntu-24.04.iso");
/// # Ok::<(), torrent_core::Error>(())
/// ```
````

### Step 5: Trait Derivation Documentation

When deriving traits, the standard derives should be listed in the struct doc. Non-obvious trait impls should have their own `///`:

```rust
/// Piece selection strategy that picks the rarest available piece first.
///
/// Uses a global piece frequency counter to determine rarity across
/// all connected peers. Falls back to random selection on ties.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RarestFirst;

impl PieceSelection for RarestFirst {
    /// Selects the next piece using rarest-first strategy.
    ///
    /// Returns `None` if all pieces are already downloaded or
    /// no peers have any pieces available.
    fn select(&self, state: &PieceState) -> Option<usize> { /* ... */ }
}
```

## Project Markdown Documentation Workflow

### README.md Structure

Each crate should have a README.md following this structure:

> Use the [README template](./references/README-template.md) for new crates.

```markdown
# crate-name

[Brief one-line description]

## Overview

[2-3 paragraphs about what the crate does, its role in the workspace]

## Features

- **Feature A**: Brief description
- **Feature B**: Brief description

## Usage

[Code example showing the most common use case]

## License

MIT OR Apache-2.0
```

### Architecture / Design Docs

When documenting architecture decisions:

- Start with the **problem** being solved
- Describe the **chosen solution** and **alternatives considered**
- Include a **diagram** (ASCII art or Mermaid) if it helps
- Reference relevant **BEP numbers**

### AGENTS.md Updates

When updating `AGENTS.md`:

- Add new hard rules if they are non-negotiable constraints
- Update the reference architecture if module structure changes
- Update the project status table after completing phases
- Keep the file focused — it's loaded into every agent context

## Documentation Checklist

Before considering documentation complete, verify:

- [ ] Every `pub` item has a `///` doc comment
- [ ] Module root files have `//! ` documentation
- [ ] BEP references use the format `/// Implements BEP XXXX: Title`
- [ ] Public functions with `Result` return have `# Errors` section
- [ ] Public functions that can panic have `# Panics` section
- [ ] `unsafe` functions have `# Safety` section
- [ ] Non-trivial public API has `# Examples` with runnable code
- [ ] All documentation is in English
- [ ] `cargo doc` produces no warnings (`RUSTDOCFLAGS="-D warnings" cargo doc`)
- [ ] Examples in doc comments compile (`cargo test --doc`)
- [ ] README.md is present for each crate with usage example
- [ ] Deprecated items use `#[deprecated]` with migration notes

## Validation Commands

```bash
# Check for doc warnings
RUSTDOCFLAGS="-D warnings" cargo doc

# Run doc tests (examples in /// blocks)
cargo test --doc

# Check for missing docs on public items (requires nightly)
# cargo +nightly doc -- -W missing_docs
```

## Anti-patterns

### ❌ Stub Comments

```rust
/// The Metainfo struct.
pub struct Metainfo { /* ... */ }
```

### ❌ Non-English Documentation

```rust
/// 解析 .torrent 文件
pub fn parse(data: &[u8]) -> Result<Metainfo, Error> { /* ... */ }
```

### ❌ Missing BEP Reference on Protocol Types

```rust
// Missing: /// Implements BEP 3: ...
pub struct Handshake { /* ... */ }
```

### ❌ Example Without Error Handling

````rust
/// ```
/// let x = fallible_fn(); // won't compile without ?
/// ```
````

### ❌ Documenting Internal Implementation

```rust
/// Uses a HashMap internally to store peers.
/// TODO: optimize with BTreeMap later.
pub struct PeerManager { /* ... */ }
```

Focus on **what** and **why**, not internal implementation details.

### ❌ Mixed Languages

```markdown
# torrent-core

核心抽象层 (Core abstractions) — pick one language and stick to it.
```

## Related Skills

- [bt-protocol](../bt-protocol/SKILL.md) — Protocol implementation conventions
