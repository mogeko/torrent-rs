# Module Documentation Template

> Copy this template into the root of any Rust module (`mod.rs` or `<module>.rs`).
> Replace `{{PLACEHOLDER}}` values with actual content.

````rust
//! {{MODULE_PURPOSE}} ({{BEP_NUMBER_AND_TITLE}}).
//!
//! {{1-2 sentences about what this module provides and why it exists.}}
//!
//! # Architecture
//!
//! {{Where this module lives and why:}}
//! - Crate: `{{torrent_core | torrent}}`
//! - Runtime: `{{sync | async (tokio)}}`
//! - Depends on: `{{list of sibling modules}}`
//!
//! # Protocol Reference
//!
//! Implements [BEP XXXX: Title](https://www.bittorrent.org/beps/bep_XXXX.html).
//!
//! # Key Types
//!
//! | Type          | Purpose                                   |
//! | ------------- | ----------------------------------------- |
//! | [`{{TypeA}}`] | {{Purpose}}                               |
//! | [`{{TypeB}}`] | {{Purpose}}                               |
//!
//! # Examples
//!
//! ```
//! use {{crate_name}}::{{module_name}}::{{MainType}};
//!
//! let result = {{MainType}}::new();
//! ```

// Public re-exports (if any)
// pub use self::submodule::PublicType;
````

## Fill-in Guide

| Placeholder            | Example                                                  |
| ---------------------- | -------------------------------------------------------- |
| `MODULE_PURPOSE`       | `Peer wire protocol message types`                       |
| `BEP_NUMBER_AND_TITLE` | `BEP 3: The BitTorrent Protocol Specification`           |
| `torrent_core/torrent` | `torrent-core` (sync) or `torrent` (async)               |
| `sync/async`           | `sync` for `torrent-core`, `async (tokio)` for `torrent` |
