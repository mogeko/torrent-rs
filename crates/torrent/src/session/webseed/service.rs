//! WebSeed download service — implements `tower::Service<PieceRange>`.
//!
//! Each `call()` selects the best URL via UCB scoring, downloads the
//! byte range via HTTP Range request, verifies SHA-1 hashes, writes
//! completed pieces to storage, and updates per-URL health state.
//! Uses the `Arc<Inner>` pattern (same as [`DhtRpc`]) so that
//! [`Service::call`] can update shared mutable state from within
//! the returned Future.
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

use super::types::{PieceRange, UrlActivity, UrlHealth, UrlKind, UrlState, WebSeedConfig};

/// Exploration weight for the UCB bandit formula (bytes/sec).
const UCB_EXPLORATION_FACTOR: f64 = 100_000.0;

/// Shared mutable state for [`WebSeedService`].
///
/// URL health and activity are behind a [`RwLock`] so that clone-
/// based [`Service::call`] futures can update them after each
/// HTTP Range download.
struct WebSeedServiceInner {
    urls: RwLock<Vec<UrlState>>,
    http: HttpClient,
    piece_mgr: Arc<RwLock<PieceManager>>,
    storage: Arc<dyn Storage>,
    piece_length: u64,
    metainfo: Metainfo,
    config: WebSeedConfig,
}

/// Web seed download service — one per torrent.
///
/// Thin `Arc`-based handle — cloning is cheap (same pattern as
/// [`DhtRpc`]).  Implements [`Service<PieceRange>`] so callers
/// can compose timeout, retry, and concurrency-limiting middleware
/// via [`tower::ServiceBuilder`].  URL health tracking and UCB
/// selection are handled internally via the shared [`WebSeedServiceInner`].
#[derive(Clone)]
pub(crate) struct WebSeedService {
    inner: Arc<WebSeedServiceInner>,
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
                    .map(|url| UrlState {
                        url,
                        health: UrlHealth::default(),
                        activity: UrlActivity::Active,
                    })
            })
            .collect();

        let count = urls.len();
        tracing::info!("web seed: initialized with {} URL(s)", count);

        WebSeedService {
            inner: Arc::new(WebSeedServiceInner {
                urls: RwLock::new(urls),
                http: HttpClient::new(),
                piece_mgr,
                storage,
                piece_length: metainfo.info.piece_length,
                metainfo,
                config,
            }),
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
        let this = self.inner.clone();
        Box::pin(async move {
            let best_url = select_best_url(&this).await;

            let Some(url) = best_url else {
                return Err(Error::new(ErrorKind::TrackerRequestFailed));
            };

            let started = Instant::now();
            let result = download_and_verify(
                &url,
                &this.http,
                &this.piece_mgr,
                &*this.storage,
                this.piece_length,
                &this.metainfo,
                range.start_byte,
                range.end_byte,
            )
            .await;

            // --- URL health update (now live, not dead code) ---
            let bytes: u64 = result
                .as_ref()
                .ok()
                .map(|pieces| {
                    pieces
                        .iter()
                        .map(|&i| piece_len(i, &this.metainfo, this.piece_length))
                        .sum()
                })
                .unwrap_or(0);

            let mut urls = this.urls.write().await;
            if let Some(state) = urls.iter_mut().find(|s| s.url == url) {
                match &result {
                    Ok(_) => {
                        state.health.record_success(bytes, started.elapsed());
                        state.activity = UrlActivity::Active;
                        tracing::debug!(
                            "web seed {}: {} pieces ({:.1}KB) in {:.1}s (ema={:.0} B/s)",
                            url,
                            result.as_ref().unwrap().len(),
                            bytes as f64 / 1024.0,
                            started.elapsed().as_secs_f64(),
                            state.health.ema_throughput(),
                        );
                    }
                    Err(e) => {
                        state.health.record_failure();
                        let was_active = matches!(state.activity, UrlActivity::Active);
                        let park = state.health.should_park(this.config.park_threshold);
                        state.activity = if park {
                            UrlActivity::Parked
                        } else {
                            UrlActivity::Active
                        };
                        if park && was_active {
                            tracing::info!(
                                "web seed {}: parked after {} consecutive failures",
                                url,
                                state.health.consecutive_failures(),
                            );
                        } else {
                            tracing::debug!(
                                "web seed {}: download failed ({} consecutive): {}",
                                url,
                                state.health.consecutive_failures(),
                                e,
                            );
                        }
                    }
                }
            }

            result
        })
    }
}

// ── Internal helpers ───────────────────────────────────────────────

/// Select the best available URL via UCB multi-armed bandit.
async fn select_best_url(inner: &WebSeedServiceInner) -> Option<Url> {
    let urls = inner.urls.read().await;
    let total_attempts: u64 = urls.iter().map(|s| s.health.download_attempts()).sum();

    let config = &inner.config;
    let candidates: Vec<&UrlState> = urls
        .iter()
        .filter(|s| match s.activity {
            UrlActivity::Active => true,
            UrlActivity::Parked => s.health.ready_for_retry(config.park_retry_interval),
        })
        .collect();

    if candidates.is_empty() {
        tracing::debug!(
            "web seed: no available URLs ({} total, {} parked)",
            urls.len(),
            urls.iter()
                .filter(|s| matches!(s.activity, UrlActivity::Parked))
                .count(),
        );
        return None;
    }

    let best = candidates.iter().max_by(|a, b| {
        a.health
            .ucb_score(total_attempts, UCB_EXPLORATION_FACTOR)
            .partial_cmp(&b.health.ucb_score(total_attempts, UCB_EXPLORATION_FACTOR))
            .unwrap_or(std::cmp::Ordering::Equal)
    })?;

    // Log UCB selection details and notify if a parked URL was unparked.
    let score = best
        .health
        .ucb_score(total_attempts, UCB_EXPLORATION_FACTOR);
    tracing::debug!(
        "web seed: selected {} (UCB={:.0}, ema={:.0} B/s, attempts={}, candidates={})",
        best.url,
        score,
        best.health.ema_throughput(),
        best.health.download_attempts(),
        candidates.len(),
    );
    if matches!(best.activity, UrlActivity::Parked) {
        tracing::info!(
            "web seed {}: unparked (retry interval elapsed, {}s since last attempt)",
            best.url,
            best.health.ready_for_retry_elapsed(),
        );
    }

    Some(best.url.clone())
}

/// Core download logic — HTTP Range request + SHA-1 verify + storage write.
#[allow(clippy::too_many_arguments)]
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
                tracing::trace!(
                    "web seed {}: piece {} already held, skipping",
                    url,
                    piece_index,
                );
                offset = chunk_end;
                continue;
            }
            let expected_hash = match metainfo.info.pieces.get(piece_index as usize) {
                Some(h) => *h,
                None => break,
            };
            tracing::trace!(
                "web seed {}: verifying piece {} ({} bytes)",
                url,
                piece_index,
                chunk.len(),
            );
            let actual_hash: [u8; 20] = Sha1::digest(chunk).into();
            if actual_hash != expected_hash {
                tracing::warn!(
                    "web seed {}: SHA-1 mismatch for piece {} (expected {:02x?})",
                    url,
                    piece_index,
                    &expected_hash[..4],
                );
                return Err(Error::new(ErrorKind::WebSeedHashMismatch));
            }
            storage.write_piece(piece_index, chunk).await?;
            {
                let mut pm = piece_mgr.write().await;
                pm.set_piece(piece_index);
            }
            completed.push(piece_index);
        } else {
            tracing::warn!(
                "web seed {}: short chunk at end ({} bytes, expected {} for piece {})",
                url,
                chunk.len(),
                plen,
                piece_index,
            );
            break;
        }
        offset = chunk_end;
    }
    Ok(completed)
}

/// Build the full file URL for an HTTP Range request.
fn build_request_url(
    url: &Url, metainfo: &Metainfo, url_kind: &UrlKind, start_byte: u64,
) -> Result<Url, Error> {
    match url_kind {
        UrlKind::Directory => {
            let offsets = metainfo.info.file_offsets();
            let file = offsets
                .iter()
                .find(|fo| start_byte >= fo.offset && start_byte < fo.offset + fo.length)
                .ok_or_else(|| {
                    tracing::warn!(
                        "web seed {}: no file found for byte offset {}",
                        url,
                        start_byte,
                    );
                    Error::new(ErrorKind::InvalidInput)
                })?;
            url.join(&file.path.join("/")).map_err(|_| {
                tracing::warn!(
                    "web seed {}: failed to join URL path for file '{}'",
                    url,
                    file.path.join("/"),
                );
                Error::new(ErrorKind::InvalidInput)
            })
        }
        UrlKind::Script => Ok(url.clone()),
    }
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
