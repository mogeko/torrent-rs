//! Async tracker implementations — HTTP (plain + TLS) and UDP announce.
//!
//! Re-exports data types from `torrent_core::tracker` and provides
//! async HTTP and UDP tracker clients.
//!
//! # Key Types
//!
//! - [`Tracker`] — unified announce client (single or multi-tracker)
//! - [`HttpTracker`] — HTTP/HTTPS GET announce (BEP 3/23)
//! - [`UdpTracker`] — UDP announce (BEP 15)
//!
//! # Constructors
//!
//! - [`Tracker::single`] — create from a single URL
//! - [`Tracker::multi`] — create from multiple URLs
//! - [`Tracker::from_torrent`] — create from a [`TorrentSpec`] (metainfo or magnet)
//!
//! # Tracker Management
//!
//! - [`Tracker::add`] / [`Tracker::add_all`] — add trackers at runtime
//! - [`Tracker::remove`] — remove a tracker by URL
//! - [`Tracker::clear`] — remove all trackers
//! - [`Tracker::len`] / [`Tracker::is_empty`] / [`Tracker::urls`] — introspection
//!
//! # Announce Methods
//!
//! | Method                         | Behavior                                             |
//! | ------------------------------ | ---------------------------------------------------- |
//! | [`Tracker::announce`]          | single-tracker; for multi acts like `announce_first` |
//! | [`Tracker::announce_first`]    | race all trackers, return first success              |
//! | [`Tracker::announce_all`]      | collect all successful responses                     |
//! | [`Tracker::announce_into_set`] | return a [`JoinSet`] for caller to drive             |

mod http;
mod into_url;
mod udp;

pub use torrent_core::tracker::{
    AnnounceEvent, AnnounceRequest, AnnounceResponse, parse_compact_peers_ipv4,
};
pub use url::Url;

pub use self::http::HttpTracker;
pub use self::into_url::IntoUrl;
pub use self::udp::UdpTracker;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;

use crate::error::{Error, ErrorKind};
use crate::spec::TorrentSpec;

/// Default per-request timeout for tracker announces (15 s).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);

/// Unified tracker client that auto-detects HTTP vs UDP from the URL scheme.
///
/// Can represent a single tracker or a collection of trackers.
///
/// # Examples
///
/// ```no_run
/// # use torrent::tracker::{Tracker, AnnounceRequest, AnnounceEvent};
/// # use torrent::peer::PeerId;
/// # async fn example() {
/// let Some(tracker) = Tracker::single("http://tracker.example.com:6969/announce") else {
///     return;
/// };
/// let mut req = AnnounceRequest::new([0u8; 20], PeerId::random(), 6881);
/// req.event = AnnounceEvent::Started;
/// let _resp = tracker.announce(&req).await;
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Tracker {
    trackers: Vec<Inner>,
    /// Default timeout for dynamically-added trackers via [`add`](Self::add).
    default_timeout: Duration,
}

impl Tracker {
    /// Create a single `Tracker` from a URL.
    ///
    /// The scheme determines which backend is used:
    /// - `http://` → [`HttpTracker`] (plain TCP)
    /// - `https://` → [`HttpTracker`] with TLS
    /// - `udp://` → [`UdpTracker`]
    ///
    /// Accepts `&str`, `String`, `&String`, or [`Url`].
    ///
    /// Returns `None` if the URL is invalid or uses an unsupported scheme.
    pub fn single(url: impl IntoUrl) -> Option<Self> {
        Self::single_with_timeout(url, DEFAULT_TIMEOUT)
    }

    /// Create a single `Tracker` from a URL with a custom timeout.
    pub fn single_with_timeout(url: impl IntoUrl, timeout: Duration) -> Option<Self> {
        let inner = Inner::from_url(url.into_url().ok()?, timeout).ok()?;
        Some(Tracker {
            trackers: vec![inner],
            default_timeout: timeout,
        })
    }

    /// Create a `Tracker` from multiple tracker URLs.
    ///
    /// Invalid or unsupported URLs are silently skipped.
    /// Duplicate URLs are silently skipped (the first occurrence is kept).
    ///
    /// Returns `None` if **all** URLs are invalid.
    pub fn multi<I: IntoIterator>(urls: I) -> Option<Self>
    where
        I::Item: IntoUrl,
    {
        Self::multi_with_timeout(urls, DEFAULT_TIMEOUT)
    }

    /// Create a `Tracker` from multiple tracker URLs with a custom timeout.
    pub fn multi_with_timeout<I: IntoIterator>(urls: I, timeout: Duration) -> Option<Self>
    where
        I::Item: IntoUrl,
    {
        let mut seen: HashSet<String> = HashSet::new();
        let mut trackers: Vec<Inner> = Vec::new();

        for url in urls {
            if let Ok(url) = url.into_url()
                && seen.insert(url.as_str().into())
                && let Ok(inner) = Inner::from_url(url, timeout)
            {
                trackers.push(inner);
            }
        }

        if trackers.is_empty() {
            None
        } else {
            Some(Tracker {
                trackers,
                default_timeout: timeout,
            })
        }
    }

    /// Create a `Tracker` from anything that converts into a [`TorrentSpec`].
    ///
    /// Accepts [`Metainfo`](crate::metainfo::Metainfo),
    /// [`MagnetUri`](crate::magnet::MagnetUri), or [`TorrentSpec`] directly.
    /// Collects all tracker URLs from the spec (both `Metainfo` and `Magnet`
    /// variants), deduplicates them, and returns a single-tracker or
    /// multi-tracker as appropriate. Invalid or unsupported URLs are silently
    /// skipped.
    ///
    /// Returns `None` if no valid tracker URLs were found.
    pub fn from_torrent(spec: impl Into<TorrentSpec>) -> Option<Self> {
        Self::from_torrent_with_timeout(spec, DEFAULT_TIMEOUT)
    }

    /// Create a `Tracker` from anything that converts into a [`TorrentSpec`]
    /// with a custom timeout.
    pub fn from_torrent_with_timeout(
        spec: impl Into<TorrentSpec>, timeout: Duration,
    ) -> Option<Self> {
        Self::multi_with_timeout(spec.into().trackers(), timeout)
    }

    /// Returns the number of trackers.
    pub fn len(&self) -> usize {
        self.trackers.len()
    }

    /// Returns `true` if there are no trackers.
    pub fn is_empty(&self) -> bool {
        self.trackers.is_empty()
    }

    /// Add a single tracker URL.
    ///
    /// If the URL is already registered, it is silently skipped.
    /// Returns an error if the URL is invalid or has an unsupported scheme.
    pub fn add(&mut self, url: impl IntoUrl) -> Result<(), Error> {
        let url = url.into_url()?;
        // Silently skip if the URL is already registered
        if self.trackers.iter().any(|t| t.url() == url.as_str()) {
            return Ok(());
        }
        self.trackers
            .push(Inner::from_url(url, self.default_timeout)?);
        Ok(())
    }

    /// Add multiple tracker URLs.
    ///
    /// All-or-nothing: if any URL is invalid, none are added
    /// (the operation is atomic with respect to `self.trackers`).
    /// Duplicate and already-registered URLs are silently skipped.
    pub fn add_all<I: IntoIterator>(&mut self, urls: I) -> Result<(), Error>
    where
        I::Item: IntoUrl,
    {
        let mut seen: HashSet<String> = self.trackers.iter().map(|t| t.url().into()).collect();
        let mut new_trackers: Vec<Inner> = Vec::new();

        for url in urls {
            let url = url.into_url()?;
            if seen.insert(url.as_str().into()) {
                new_trackers.push(Inner::from_url(url, self.default_timeout)?);
            }
        }

        self.trackers.extend(new_trackers);
        Ok(())
    }

    /// Remove a tracker by its URL.
    ///
    /// Returns `true` if a tracker was found and removed.
    pub fn remove(&mut self, url: &str) -> bool {
        let len_before = self.trackers.len();
        self.trackers.retain(|inner| inner.url() != url);
        self.trackers.len() < len_before
    }

    /// Remove all trackers.
    pub fn clear(&mut self) {
        self.trackers.clear();
    }

    /// Return the URLs of all trackers (for logging / debugging).
    pub fn urls(&self) -> Vec<&str> {
        self.trackers.iter().map(|inner| inner.url()).collect()
    }

    /// Announce to the tracker(s).
    ///
    /// For a single tracker, delegates directly.
    /// For multiple trackers, acts as [`announce_first`](Self::announce_first).
    pub async fn announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponse, Error> {
        self.announce_first(req).await
    }

    /// Race all trackers and return the first successful response.
    ///
    /// If all trackers fail, the last error is returned.
    pub async fn announce_first(&self, req: &AnnounceRequest) -> Result<AnnounceResponse, Error> {
        let mut set = self.announce_into_set(req);

        let mut last_err = None;
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok(resp)) => return Ok(resp),
                Ok(Err(e)) => last_err = Some(e),
                Err(_) => last_err = Some(Error::new(ErrorKind::TrackerRequestFailed)),
            }
        }

        Err(last_err.unwrap_or_else(|| Error::new(ErrorKind::TrackerRequestFailed)))
    }

    /// Announce to all trackers and collect all successful responses.
    ///
    /// Errors are silently ignored.
    pub async fn announce_all(&self, req: &AnnounceRequest) -> Vec<AnnounceResponse> {
        let mut set = self.announce_into_set(req);

        let mut results = Vec::new();
        while let Some(result) = set.join_next().await {
            if let Ok(Ok(resp)) = result {
                results.push(resp);
            }
        }
        results
    }

    /// Spawn all trackers into a new [`JoinSet`] for the caller to drive.
    ///
    /// Each task returns `Ok(AnnounceResponse)` on success or `Err(Error)` on failure.
    pub fn announce_into_set(
        &self, req: &AnnounceRequest,
    ) -> JoinSet<Result<AnnounceResponse, Error>> {
        let mut set = JoinSet::new();
        let req = Arc::new(req.clone());
        for inner in &self.trackers {
            let inner = inner.clone();
            let req = Arc::clone(&req);
            set.spawn(async move { inner.announce(&req).await });
        }
        set
    }
}

/// Internal tracker variant: HTTP or UDP.
#[derive(Debug, Clone)]
enum Inner {
    Http(HttpTracker),
    Udp(UdpTracker),
}

impl Inner {
    fn url(&self) -> &str {
        match self {
            Inner::Http(t) => t.url().as_str(),
            Inner::Udp(t) => t.url().as_str(),
        }
    }

    fn from_url(url: Url, timeout: Duration) -> Result<Self, Error> {
        match url.scheme() {
            "http" | "https" => Ok(Inner::Http(HttpTracker::with_timeout(url, timeout)?)),
            "udp" => Ok(Inner::Udp(UdpTracker::with_timeout(url, timeout)?)),
            _ => Err(Error::new(ErrorKind::InvalidInput)),
        }
    }

    async fn announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponse, Error> {
        match self {
            Inner::Http(t) => t.announce(req).await,
            Inner::Udp(t) => t.announce(req).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerId;

    #[test]
    fn test_tracker_single_http() {
        let t = Tracker::single("http://tracker.example.com:6969/announce").unwrap();
        assert_eq!(t.trackers.len(), 1);
    }

    #[test]
    fn test_tracker_single_udp() {
        let t = Tracker::single("udp://tracker.example.com:6969").unwrap();
        assert_eq!(t.trackers.len(), 1);
    }

    #[test]
    fn test_tracker_single_https() {
        // HTTPS is now supported via TLS wrapping
        let t = Tracker::single("https://tracker.example.com/announce").unwrap();
        assert_eq!(t.trackers.len(), 1);
    }

    #[test]
    fn test_tracker_multi_mixed_http_https() {
        let result = Tracker::multi([
            "http://tracker.a.com/announce",
            "https://tracker.b.com/announce",
        ]);
        let t = result.unwrap();
        assert_eq!(t.trackers.len(), 2);
    }

    #[test]
    fn test_tracker_single_invalid_scheme() {
        assert!(Tracker::single("ftp://tracker.example.com").is_none());
    }

    #[test]
    fn test_tracker_single_invalid_url() {
        assert!(Tracker::single("not a url").is_none());
    }

    #[test]
    fn test_tracker_multi_valid() {
        let t =
            Tracker::multi(["http://tracker.a.com/announce", "udp://tracker.b.com:6969"]).unwrap();
        assert_eq!(t.trackers.len(), 2);
    }

    #[test]
    fn test_tracker_multi_skips_invalid() {
        // Invalid URLs are silently skipped; valid ones remain.
        let t = Tracker::multi(["http://tracker.a.com/announce", "not a url"]).unwrap();
        assert_eq!(t.trackers.len(), 1);
    }

    #[test]
    fn test_tracker_multi_all_invalid_scheme() {
        assert!(Tracker::multi(["ftp://tracker.example.com"]).is_none());
    }

    #[tokio::test]
    async fn test_tracker_multi_empty() {
        let urls: Vec<&str> = Vec::new();
        assert!(Tracker::multi(urls).is_none());
    }

    #[tokio::test]
    async fn test_tracker_announce_into_set_type() {
        // Verify the method compiles and returns the right type
        let t = Tracker::single("http://tracker.example.com/announce").unwrap();
        let mut req = AnnounceRequest::new([0u8; 20], PeerId::random(), 6881);
        req.compact = false;
        req.numwant = None;
        let set: JoinSet<Result<AnnounceResponse, Error>> = t.announce_into_set(&req);
        // Just verify it's not empty when there are trackers
        assert!(!set.is_empty());
    }

    #[test]
    fn test_tracker_multi_from_vec_string() {
        let urls = vec![
            "http://tracker.a.com/announce".to_string(),
            "udp://tracker.b.com:6969".to_string(),
        ];
        let t = Tracker::multi(urls).unwrap();
        assert_eq!(t.trackers.len(), 2);
    }

    #[test]
    fn test_tracker_multi_from_urls() {
        let url1 = Url::parse("http://tracker.a.com/announce").unwrap();
        let url2 = Url::parse("udp://tracker.b.com:6969").unwrap();
        let t = Tracker::multi([url1, url2]).unwrap();
        assert_eq!(t.trackers.len(), 2);
    }

    #[test]
    fn test_tracker_from_torrent() {
        use crate::metainfo::{Bytes, Info, Metainfo, Mode, RawInfo};

        let info = Info {
            piece_length: 262144,
            pieces: vec![[0u8; 20]],
            mode: Mode::Single {
                name: "test.txt".into(),
                length: 1024,
            },
            raw_info: RawInfo::Bytes(Bytes::new()),
        };
        let meta = Metainfo {
            announce: "http://tracker.a.com/announce".into(),
            announce_list: vec![vec!["udp://tracker.b.com:6969".into()]],
            info,
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        };

        let t = Tracker::from_torrent(meta).unwrap();
        assert_eq!(t.trackers.len(), 2);
    }

    #[test]
    fn test_tracker_from_torrent_skip_invalid() {
        use crate::metainfo::{Bytes, Info, Metainfo, Mode, RawInfo};

        let info = Info {
            piece_length: 262144,
            pieces: vec![[0u8; 20]],
            mode: Mode::Single {
                name: "test.txt".into(),
                length: 1024,
            },
            raw_info: RawInfo::Bytes(Bytes::new()),
        };
        // announce has invalid scheme; announce_list is valid
        let meta = Metainfo {
            announce: "ftp://tracker.a.com/announce".into(),
            announce_list: vec![vec!["http://tracker.b.com/announce".into()]],
            info,
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        };

        let t = Tracker::from_torrent(meta).unwrap();
        assert_eq!(t.trackers.len(), 1);
        assert_eq!(t.urls(), &["http://tracker.b.com/announce"]);
    }

    #[test]
    fn test_tracker_add_and_remove() {
        let mut t = Tracker::single("http://tracker.a.com/announce").unwrap();
        assert_eq!(t.len(), 1);
        assert!(!t.is_empty());
        assert_eq!(t.urls(), &["http://tracker.a.com/announce"]);

        t.add("udp://tracker.b.com:6969").unwrap();
        assert_eq!(t.len(), 2);
        assert_eq!(
            t.urls(),
            &["http://tracker.a.com/announce", "udp://tracker.b.com:6969"]
        );

        assert!(t.remove("http://tracker.a.com/announce"));
        assert_eq!(t.len(), 1);
        assert!(!t.remove("http://tracker.a.com/announce")); // already gone

        t.clear();
        assert!(t.is_empty());
        assert!(t.urls().is_empty());
    }

    #[test]
    fn test_tracker_multi_dedup() {
        // Duplicate URLs are deduplicated (keep first occurrence)
        let t = Tracker::multi([
            "http://tracker.a.com/announce",
            "udp://tracker.b.com:6969",
            "http://tracker.a.com/announce", // duplicate
        ])
        .unwrap();
        assert_eq!(t.trackers.len(), 2);
        assert_eq!(
            t.urls(),
            &["http://tracker.a.com/announce", "udp://tracker.b.com:6969"]
        );
    }

    #[test]
    fn test_tracker_multi_dedup_all_same() {
        // All URLs identical → single tracker
        let t = Tracker::multi([
            "http://tracker.a.com/announce",
            "http://tracker.a.com/announce",
            "http://tracker.a.com/announce",
        ])
        .unwrap();
        assert_eq!(t.trackers.len(), 1);
    }

    #[test]
    fn test_tracker_add_dedup() {
        let mut t = Tracker::single("http://tracker.a.com/announce").unwrap();
        assert_eq!(t.len(), 1);

        // Adding the same URL again should silently succeed without increasing count
        t.add("http://tracker.a.com/announce").unwrap();
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn test_tracker_from_torrent_dedup_across_tiers() {
        use crate::metainfo::{Bytes, Info, Metainfo, Mode, RawInfo};

        let info = Info {
            piece_length: 262144,
            pieces: vec![[0u8; 20]],
            mode: Mode::Single {
                name: "test.txt".into(),
                length: 1024,
            },
            raw_info: RawInfo::Bytes(Bytes::new()),
        };
        // announce and announce_list[0] share the same URL
        let meta = Metainfo {
            announce: "http://tracker.a.com/announce".into(),
            announce_list: vec![
                vec!["http://tracker.a.com/announce".into()], // duplicate
                vec!["udp://tracker.b.com:6969".into()],
            ],
            info,
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        };

        let t = Tracker::from_torrent(meta).unwrap();
        assert_eq!(t.trackers.len(), 2);
        // announce (top-level) is kept first; duplicate from announce_list is skipped
        assert_eq!(
            t.urls(),
            &["http://tracker.a.com/announce", "udp://tracker.b.com:6969"]
        );
    }

    #[test]
    fn test_tracker_add_all_dedup() {
        let mut t = Tracker::single("http://tracker.a.com/announce").unwrap();

        t.add_all([
            "udp://tracker.b.com:6969",
            "http://tracker.c.com/announce",
            "udp://tracker.b.com:6969", // duplicate in the same batch
        ])
        .unwrap();

        assert_eq!(t.trackers.len(), 3);
        assert_eq!(
            t.urls(),
            &[
                "http://tracker.a.com/announce",
                "udp://tracker.b.com:6969",
                "http://tracker.c.com/announce"
            ]
        );
    }

    #[test]
    fn test_tracker_add_all_dedup_with_existing() {
        let mut t = Tracker::single("http://tracker.a.com/announce").unwrap();

        // Some URLs already exist, some are new, some are duplicates within the batch
        t.add_all([
            "http://tracker.a.com/announce", // already exists
            "udp://tracker.b.com:6969",      // new
            "http://tracker.a.com/announce", // duplicate (existing + same batch)
            "udp://tracker.b.com:6969",      // duplicate
            "http://tracker.c.com/announce", // new
        ])
        .unwrap();

        assert_eq!(t.trackers.len(), 3);
    }

    #[test]
    fn test_tracker_add_all_invalid_url() {
        let mut t = Tracker::single("http://tracker.a.com/announce").unwrap();

        // Invalid URL should cause early Err (short-circuit), and no URLs
        // should be added — the operation is all-or-nothing.
        let result = t.add_all([
            "udp://tracker.b.com:6969",
            "not a url",
            "http://tracker.c.com/announce",
        ]);

        assert!(result.is_err());
        // State must be unchanged (atomic semantics)
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn test_tracker_add_all_empty() {
        let mut t = Tracker::single("http://tracker.a.com/announce").unwrap();
        let urls: Vec<&str> = Vec::new();

        t.add_all(urls).unwrap();
        assert_eq!(t.trackers.len(), 1); // unchanged
    }

    #[test]
    fn test_tracker_from_torrent_preserves_order() {
        use crate::metainfo::{Bytes, Info, Metainfo, Mode, RawInfo};

        let info = Info {
            piece_length: 262144,
            pieces: vec![[0u8; 20]],
            mode: Mode::Single {
                name: "test.txt".into(),
                length: 1024,
            },
            raw_info: RawInfo::Bytes(Bytes::new()),
        };
        // Multiple tiers with unique URLs; order should be preserved
        let meta = Metainfo {
            announce: "http://tracker.a.com/announce".into(),
            announce_list: vec![
                vec!["udp://tracker.b.com:6969".into()],
                vec!["http://tracker.c.com/announce".into()],
            ],
            info,
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        };

        let t = Tracker::from_torrent(meta).unwrap();
        assert_eq!(t.trackers.len(), 3);
        assert_eq!(
            t.urls(),
            &[
                "http://tracker.a.com/announce",
                "udp://tracker.b.com:6969",
                "http://tracker.c.com/announce"
            ]
        );
    }

    #[test]
    fn test_tracker_remove_nonexistent() {
        let mut t = Tracker::single("http://tracker.a.com/announce").unwrap();

        // Remove existing
        assert!(t.remove("http://tracker.a.com/announce"));
        assert!(t.is_empty());

        // Remove already-removed URL — should be idempotent
        assert!(!t.remove("http://tracker.a.com/announce"));
        assert!(t.is_empty());

        // Remove URL that was never added
        assert!(!t.remove("udp://tracker.b.com:6969"));
        assert!(t.is_empty());
    }
}
