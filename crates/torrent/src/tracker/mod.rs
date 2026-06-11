//! Async tracker implementations — HTTP and UDP announce.
//!
//! Re-exports data types from `torrent_core::tracker` and provides
//! async HTTP and UDP tracker clients.
//!
//! # Key Types
//!
//! - [`Tracker`] — unified announce client (single or multi-tracker)
//! - [`HttpTracker`] — HTTP GET announce (BEP 3/23)
//! - [`UdpTracker`] — UDP announce (BEP 15)
//!
//! # Constructors
//!
//! - [`Tracker::single`] — create from a single URL
//! - [`Tracker::multi`] — create from multiple URLs
//!
//! # Announce Methods
//!
//！ | Method                         | Behavior                                             |
//！ | ------------------------------ | ---------------------------------------------------- |
//！ | [`Tracker::announce`]          | single-tracker; for multi acts like `announce_first` |
//！ | [`Tracker::announce_first`]    | race all trackers, return first success              |
//！ | [`Tracker::announce_all`]      | collect all successful responses                     |
//！ | [`Tracker::announce_into_set`] | return a [`JoinSet`] for caller to drive             |

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

use std::collections::BTreeSet;
use std::sync::Arc;

use tokio::task::JoinSet;
use torrent_core::metainfo::Metainfo;

use crate::error::{Error, ErrorKind};

/// Unified tracker client that auto-detects HTTP vs UDP from the URL scheme.
///
/// Can represent a single tracker or a collection of trackers.
///
/// # Examples
///
/// ```no_run
/// # use torrent::tracker::{Tracker, AnnounceRequest, AnnounceEvent};
/// # use torrent::peer::PeerId;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let tracker = Tracker::single("http://tracker.example.com:6969/announce")?;
/// let mut req = AnnounceRequest::new([0u8; 20], PeerId::random(), 6881);
/// req.event = AnnounceEvent::Started;
/// let resp = tracker.announce(&req).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Tracker {
    trackers: Vec<Inner>,
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
    pub fn single(url: impl IntoUrl) -> Result<Self, Error> {
        let url = url.into_url()?;
        let inner = Inner::from_url(url)?;
        Ok(Tracker {
            trackers: vec![inner],
        })
    }

    /// Create a `Tracker` from multiple tracker URLs.
    ///
    /// Each URL is parsed and validated. Returns an error if any URL is invalid.
    pub fn multi<I: IntoIterator>(urls: I) -> Result<Self, Error>
    where
        I::Item: IntoUrl,
    {
        let trackers = urls
            .into_iter()
            .map(|u| Inner::from_url(u.into_url()?))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Tracker { trackers })
    }

    /// Create a `Tracker` from a parsed [`Metainfo`].
    ///
    /// Collects all tracker URLs from `announce` and `announce_list` (BEP 12),
    /// deduplicates them, and returns a single-tracker or multi-tracker as
    /// appropriate.  Accepts `&Metainfo` or `Metainfo`.
    ///
    /// Returns an error if any embedded URL is invalid.
    pub fn from_metainfo(meta: &Metainfo) -> Result<Self, Error> {
        let mut urls: BTreeSet<&str> = BTreeSet::new();

        urls.insert(&meta.announce);
        for tier in &meta.announce_list {
            for url in tier {
                urls.insert(url);
            }
        }

        if urls.len() == 1 {
            Self::single(*(urls.first().unwrap()))
        } else {
            Self::multi(urls)
        }
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
    /// Returns an error if the URL is invalid or has an unsupported scheme.
    pub fn add(&mut self, url: impl IntoUrl) -> Result<(), Error> {
        let url = url.into_url()?;
        self.trackers.push(Inner::from_url(url)?);
        Ok(())
    }

    /// Add multiple tracker URLs.
    ///
    /// Returns an error if any URL is invalid.
    pub fn add_all<I: IntoIterator>(&mut self, urls: I) -> Result<(), Error>
    where
        I::Item: IntoUrl,
    {
        for url in urls {
            self.add(url)?;
        }
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
        &self,
        req: &AnnounceRequest,
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

    fn from_url(url: Url) -> Result<Self, Error> {
        match url.scheme() {
            "http" | "https" => Ok(Inner::Http(HttpTracker::new(url)?)),
            "udp" => Ok(Inner::Udp(UdpTracker::new(url)?)),
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
        assert!(result.is_ok());
        assert_eq!(result.unwrap().trackers.len(), 2);
    }

    #[test]
    fn test_tracker_single_invalid_scheme() {
        assert!(Tracker::single("ftp://tracker.example.com").is_err());
    }

    #[test]
    fn test_tracker_single_invalid_url() {
        assert!(Tracker::single("not a url").is_err());
    }

    #[test]
    fn test_tracker_multi_valid() {
        let t =
            Tracker::multi(["http://tracker.a.com/announce", "udp://tracker.b.com:6969"]).unwrap();
        assert_eq!(t.trackers.len(), 2);
    }

    #[test]
    fn test_tracker_multi_invalid() {
        let result = Tracker::multi(["http://tracker.a.com/announce", "not a url"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_tracker_multi_all_invalid_scheme() {
        let result = Tracker::multi(["ftp://tracker.example.com"]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tracker_multi_empty() {
        let urls: Vec<&str> = Vec::new();
        let t = Tracker::multi(urls).unwrap();
        assert_eq!(t.trackers.len(), 0);
        let mut req = AnnounceRequest::new([0u8; 20], PeerId::random(), 6881);
        req.compact = false;
        req.numwant = None;
        let result = t.announce_first(&req).await;
        assert!(result.is_err());
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
    fn test_tracker_from_metainfo() {
        use torrent_core::metainfo::{Info, Metainfo, Mode};

        let info = Info {
            piece_length: 262144,
            pieces: vec![[0u8; 20]],
            mode: Mode::Single {
                name: "test.txt".into(),
                length: 1024,
            },
            raw_info: bytes::Bytes::new(),
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

        let t = Tracker::from_metainfo(&meta).unwrap();
        assert_eq!(t.trackers.len(), 2);
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
}
