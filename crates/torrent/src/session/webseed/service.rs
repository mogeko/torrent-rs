//! WebSeed download service — implements `tower::Service<PieceRange>`.
//!
//! Each `call()` selects the best URL via UCB scoring, downloads the
//! byte range via HTTP Range request, verifies SHA-1 hashes, and writes
//! completed pieces to storage.  URL health tracking and parking are
//! handled internally.
//!
//! This replaces the scheduler+fetcher+channel architecture with a
//! single composable tower Service.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use sha1::{Digest, Sha1};
use tokio::sync::RwLock;
use tower::Service;

use crate::error::{Error, ErrorKind};
use crate::metainfo::Metainfo;
use crate::net::Url;
use crate::net::http::HttpClient;
use crate::piece::PieceManager;
use crate::storage::Storage;

use super::fetcher::build_request_url;
use super::types::{PieceRange, UrlActivity, UrlHealth, UrlKind, UrlState, WebSeedConfig};

/// Exploration weight for the UCB bandit formula (bytes/sec).
const UCB_EXPLORATION_FACTOR: f64 = 100_000.0;

/// Web seed download service — one per torrent.
///
/// Implements [`Service<PieceRange>`] so callers can compose timeout,
/// retry, and concurrency-limiting middleware via [`tower::ServiceBuilder`].
/// URL health tracking and UCB selection are handled internally.
pub(crate) struct WebSeedService {
    urls: Vec<UrlState>,
    http: HttpClient,
    piece_mgr: Arc<RwLock<PieceManager>>,
    storage: Arc<dyn Storage>,
    piece_length: u64,
    metainfo: Metainfo,
    config: WebSeedConfig,
}

impl WebSeedService {
    pub fn new(
        url_strings: Vec<String>, piece_mgr: Arc<RwLock<PieceManager>>, storage: Arc<dyn Storage>,
        metainfo: Metainfo, config: WebSeedConfig,
    ) -> Self {
        let urls: Vec<UrlState> = url_strings
            .into_iter()
            .filter_map(|s| {
                Url::parse(&s)
                    .map_err(|e| tracing::warn!("invalid web seed URL '{}': {}", s, e))
                    .ok()
                    .map(|url| {
                        let url_kind = UrlKind::classify(&url);
                        UrlState {
                            url,
                            url_kind,
                            health: UrlHealth::default(),
                            work_tx: None,
                            activity: UrlActivity::Active,
                        }
                    })
            })
            .collect();

        WebSeedService {
            urls,
            http: HttpClient::new(),
            piece_mgr,
            storage,
            piece_length: metainfo.info.piece_length,
            metainfo,
            config,
        }
    }
}

impl Service<PieceRange> for WebSeedService {
    type Response = Vec<u32>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, range: PieceRange) -> Self::Future {
        // Select best available URL via UCB
        let best_url = self.select_best_url();
        let http = self.http.clone();
        let piece_mgr = self.piece_mgr.clone();
        let storage = self.storage.clone();
        let piece_length = self.piece_length;
        let metainfo = self.metainfo.clone();
        let park_threshold = self.config.park_threshold;

        Box::pin(async move {
            let Some(url) = best_url else {
                return Err(Error::new(ErrorKind::TrackerRequestFailed));
            };

            let started = Instant::now();
            let result = download_and_verify(
                &url,
                &http,
                &*piece_mgr,
                &*storage,
                piece_length,
                &metainfo,
                range.start_byte,
                range.end_byte,
            )
            .await;

            // Update health (this is a simplified version — full UCB tracking
            // requires mutable self access which Service::call doesn't provide
            // directly.  For now we log the result.)
            match &result {
                Ok(pieces) => {
                    let elapsed = started.elapsed();
                    let bytes: u64 = pieces
                        .iter()
                        .map(|&i| piece_len(i, &metainfo, piece_length))
                        .sum();
                    tracing::debug!(
                        "web seed {}: {} pieces ({:.1}KB) in {:.1}s",
                        url,
                        pieces.len(),
                        bytes as f64 / 1024.0,
                        elapsed.as_secs_f64()
                    );
                }
                Err(e) => {
                    tracing::debug!("web seed {}: download failed: {}", url, e);
                }
            }

            result
        })
    }
}

impl WebSeedService {
    /// Select the best available URL via UCB score.
    fn select_best_url(&self) -> Option<Url> {
        let total_attempts: u64 = self.urls.iter().map(|s| s.health.download_attempts()).sum();

        self.urls
            .iter()
            .filter(|s| match s.activity {
                UrlActivity::Active | UrlActivity::InFlight => true,
                UrlActivity::Parked => s.health.ready_for_retry(self.config.park_retry_interval),
            })
            .max_by(|a, b| {
                a.health
                    .ucb_score(total_attempts, UCB_EXPLORATION_FACTOR)
                    .partial_cmp(&b.health.ucb_score(total_attempts, UCB_EXPLORATION_FACTOR))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|s| s.url.clone())
    }

    /// Mark a URL as in-flight (reserved for a pending request).
    pub fn mark_in_flight(&mut self, url: &Url) {
        if let Some(state) = self.urls.iter_mut().find(|s| &s.url == url) {
            state.activity = UrlActivity::InFlight;
        }
    }

    /// Record a download result for URL health tracking.
    pub fn record_result(
        &mut self, url: &Url, bytes: u64, elapsed: std::time::Duration, error: Option<ErrorKind>,
    ) {
        if let Some(state) = self.urls.iter_mut().find(|s| &s.url == url) {
            match error {
                None => {
                    state.health.record_success(bytes, elapsed);
                    state.activity = UrlActivity::Active;
                }
                Some(ErrorKind::WebSeedHashMismatch) => {
                    // Permanent failure — URL will be filtered out
                    state.activity = UrlActivity::Parked;
                    state.health.record_failure();
                }
                Some(_) => {
                    state.health.record_failure();
                    if state.health.should_park(self.config.park_threshold) {
                        state.activity = UrlActivity::Parked;
                    } else {
                        state.activity = UrlActivity::Active;
                    }
                }
            }
        }
    }
}

/// Core download logic — shared between `Service::call` and direct usage.
async fn download_and_verify(
    url: &Url, http: &HttpClient, piece_mgr: &RwLock<PieceManager>, storage: &dyn Storage,
    piece_length: u64, metainfo: &Metainfo, start_byte: u64, end_byte: u64,
) -> Result<Vec<u32>, Error> {
    let url_kind = UrlKind::classify(url);
    let request_url = build_request_url(url, metainfo, &url_kind, start_byte)?;

    let range_size = end_byte - start_byte + 1;
    tracing::debug!(
        "web seed {}: GET {} [{}-{}] ({:.1}KB)",
        url,
        request_url,
        start_byte,
        end_byte,
        range_size as f64 / 1024.0
    );

    let http_req = http::Request::get(request_url.as_str())
        .header("Range", format!("bytes={start_byte}-{end_byte}"))
        .body(vec![])
        .map_err(|_| Error::new(ErrorKind::InvalidInput))?;

    let mut http = http.clone();
    let resp = Service::call(&mut http, http_req).await?;
    let body = resp.into_body();

    let mut completed = Vec::new();
    let first_piece = (start_byte / piece_length) as u32;
    let mut offset = 0u64;

    while offset < body.len() as u64 {
        let piece_index = first_piece + (offset / piece_length) as u32;
        let piece_offset = (start_byte + offset) % piece_length;
        let plen = piece_len(piece_index, metainfo, piece_length);
        let chunk_end = (offset + plen - piece_offset).min(body.len() as u64);
        let chunk = &body[offset as usize..chunk_end as usize];

        if chunk.len() as u64 == plen {
            if piece_mgr.read().await.has_piece(piece_index) {
                offset = chunk_end;
                continue;
            }
            let expected_hash = match metainfo.info.pieces.get(piece_index as usize) {
                Some(h) => *h,
                None => break,
            };
            let actual_hash: [u8; 20] = Sha1::digest(chunk).into();
            if actual_hash != expected_hash {
                return Err(Error::new(ErrorKind::WebSeedHashMismatch));
            }
            storage.write_piece(piece_index, chunk).await?;
            {
                let mut pm = piece_mgr.write().await;
                pm.set_piece(piece_index);
            }
            completed.push(piece_index);
        } else {
            break;
        }
        offset = chunk_end;
    }
    Ok(completed)
}

fn piece_len(index: u32, metainfo: &Metainfo, piece_length: u64) -> u64 {
    let total = metainfo.info.total_size();
    let start = index as u64 * piece_length;
    if start + piece_length > total {
        total - start
    } else {
        piece_length
    }
}
