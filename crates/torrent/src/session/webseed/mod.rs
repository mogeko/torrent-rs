//! Web seed download engine (BEP 19).
//!
//! Downloads pieces from web seed URLs using HTTP Range requests via
//! [`WebSeedService::download`].  Gap-finding utilities in [`gap`]
//! locate missing piece ranges in the bitfield; URL health tracking
//! and UCB multi-armed bandit selection are handled internally by
//! the service.
//!
//! # Architecture
//!
//! ```text
//! SwarmLoop::status_tick()
//!   ├── find_largest_gap() / gap_within_file()
//!   └── webseed_service.download(PieceRange)
//!         ├── UCB select best URL
//!         ├── HTTP Range download
//!         └── SHA-1 verify + storage write
//! ```

mod extension;
mod gap;
mod service;
mod types;

pub(crate) use self::extension::WebSeedExtension;
pub(crate) use self::types::WebSeedConfig;
