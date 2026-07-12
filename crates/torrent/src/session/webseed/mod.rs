//! Web seed download engine (BEP 19).
//!
//! Web seeds are standard HTTP/FTP servers that host torrent files.
//! This module downloads pieces from web seed URLs using HTTP Range
//! requests, filling gaps left by P2P peer downloads.
//!
//! # Module Layout
//!
//! - [`types`] — configuration, health scoring, scheduler↔fetcher messages
//! - [`fetcher`] — passive HTTP download worker (FetchTask)
//! - [`scheduler`] — centralized work dispatch (WebSeedScheduler)
//!
//! # Architecture
//!
//! The scheduler reads the piece bitfield, selects the largest gap,
//! picks the fastest available URL by throughput, and dispatches
//! [`WorkItem`]s to fetcher tasks via mpsc channels.  Fetchers handle
//! HTTP Range requests, SHA-1 verification, and storage writes.

mod fetcher;
mod scheduler;
mod types;

pub(crate) use self::fetcher::FetchTask;
pub(crate) use self::scheduler::{WebSeedScheduler, deduplicate_urls};
pub(crate) use self::types::{
    UrlActivity, UrlHealth, UrlKind, UrlState, WebSeedConfig, WorkItem, WorkResult,
};
