//! Web seed download engine (BEP 19).
//!
//! Web seeds are standard HTTP/FTP servers that host torrent files.
//! This module downloads pieces from web seed URLs using HTTP Range
//! requests, filling gaps left by P2P peer downloads.
//!
//! # Algorithm
//!
//! 1. Read the piece bitfield to find the largest contiguous gap
//!    of missing pieces
//! 2. Compute the byte range for that gap
//! 3. Issue an HTTP GET with `Range: bytes=start-end`
//! 4. Split the response into piece-sized chunks
//! 5. SHA-1 verify each piece, write to storage, update bitfield
//! 6. Repeat until the torrent is complete

use std::sync::Arc;
use std::time::{Duration, Instant};

use sha1::{Digest, Sha1};
use tokio::sync::{Notify, RwLock, Semaphore, mpsc};
use url::Url;

use crate::error::{Error, ErrorKind};
use crate::metainfo::{Metainfo, Mode};
use crate::net::http::HttpClient;
use crate::piece::PieceManager;
use crate::storage::Storage;

/// Configuration for web seed downloads (BEP 19).
#[derive(Debug, Clone)]
pub(crate) struct WebSeedConfig {
    /// Minimum contiguous gap (in pieces) to trigger an HTTP download.
    /// Prevents tiny range requests. Default: 4 pieces.
    #[allow(dead_code, reason = "used by find_largest_gap; will add filtering")]
    pub min_gap_pieces: u32,
    /// Maximum bytes per Range request.
    /// BEP 19 suggests ~5% of total file size. Default: 5 MB.
    pub max_range_bytes: u64,
    /// Timeout for HTTP connect + download.
    /// Default: 30 s.
    pub timeout: Duration,
    /// Delay before retrying after a transient error (503, connection
    /// refused). Doubles on each consecutive failure up to 60 s.
    /// Default: 2 s.
    #[allow(dead_code, reason = "reserved for per-URL backoff in scheduler")]
    pub retry_delay: Duration,
    /// Maximum concurrent in-flight HTTP Range requests across all
    /// web seed tasks.  Serves two purposes:
    ///
    /// 1. Prevents TLS-handshake CPU spikes from starving a
    ///    [`current_thread`](https://docs.rs/tokio/latest/tokio/runtime/index.html#current-thread-scheduler)
    ///    runtime when many web seed URLs are present (BEP 19 torrents
    ///    can list hundreds of mirrors).
    /// 2. Caps connections to any single origin server, mitigating
    ///    the risk of being rate-limited or blacklisted (similar to
    ///    libtorrent's per-URL limit of 5 and Transmission's serial
    ///    web seed downloads).
    ///
    /// On a [`multi_thread`](https://docs.rs/tokio/latest/tokio/runtime/index.html#multi-thread-scheduler)
    /// runtime users can raise this value for higher throughput —
    /// a good starting point is `num_workers * 8`.  The library
    /// deliberately keeps a conservative default because it cannot
    /// detect the caller's runtime flavour.
    ///
    /// Default: 16.
    pub max_concurrent: usize,
    /// Consecutive HTTP failures before parking a URL (stop
    /// downloading, only probe periodically).
    ///
    /// Default: `5`.
    pub park_threshold: u32,
    /// How long to wait before re-probing a parked URL.
    ///
    /// Default: `60` s.
    pub park_retry_interval: Duration,
}

impl Default for WebSeedConfig {
    fn default() -> Self {
        WebSeedConfig {
            min_gap_pieces: 4,
            max_range_bytes: 5 * 1024 * 1024, // 5 MB
            timeout: Duration::from_secs(30),
            retry_delay: Duration::from_secs(2),
            max_concurrent: 16,
            park_threshold: 5,
            park_retry_interval: Duration::from_secs(60),
        }
    }
}

// ── URL Health Tracking ────────────────────────────────────────────

/// Per-URL health and performance tracking for runtime scoring.
///
/// Updated after each HTTP Range request. Used to decide whether
/// to keep or park a URL (Phase 1) and, in Phase 2, to weight
/// work distribution across URLs.
#[derive(Debug, Clone)]
pub(crate) struct UrlHealth {
    /// Exponential moving average of throughput (bytes/sec).
    /// Decay factor α = 0.3: recent speed contributes 30%, history 70%.
    ema_throughput: f64,
    /// Consecutive failures since the last successful download.
    consecutive_failures: u32,
    /// Total number of successful download requests.
    success_count: u64,
    /// When the last successful download completed.
    last_success: Option<Instant>,
}

impl Default for UrlHealth {
    fn default() -> Self {
        UrlHealth {
            ema_throughput: 0.0,
            consecutive_failures: 0,
            success_count: 0,
            last_success: None,
        }
    }
}

impl UrlHealth {
    /// Decay factor for the exponential moving average.
    const ALPHA: f64 = 0.3;

    /// Record a successful download of `bytes` in `elapsed` time.
    fn record_success(&mut self, bytes: u64, elapsed: Duration) {
        let throughput = bytes as f64 / elapsed.as_secs_f64().max(0.001);
        if self.success_count == 0 {
            self.ema_throughput = throughput;
        } else {
            self.ema_throughput =
                Self::ALPHA * throughput + (1.0 - Self::ALPHA) * self.ema_throughput;
        }
        self.consecutive_failures = 0;
        self.success_count += 1;
        self.last_success = Some(Instant::now());
    }

    /// Record a failed download attempt.
    fn record_failure(&mut self) {
        self.consecutive_failures += 1;
    }

    /// Whether this URL should be parked (too many consecutive failures).
    fn should_park(&self, threshold: u32) -> bool {
        self.consecutive_failures >= threshold
    }

    /// Whether enough time has passed to retry a parked URL.
    fn ready_for_retry(&self, interval: Duration) -> bool {
        match self.last_success {
            Some(t) => t.elapsed() >= interval,
            None => true, // never succeeded — always ready to retry
        }
    }
}

// ── Scheduler ↔ Fetcher message types ─────────────────────────────

/// A unit of work dispatched by the scheduler to a fetcher.
#[derive(Debug, Clone)]
pub(crate) struct WorkItem {
    /// Byte offset to start downloading from (inclusive).
    start_byte: u64,
    /// Byte offset to end downloading at (inclusive).
    end_byte: u64,
}

/// Result of a [`WorkItem`] reported back to the scheduler.
pub(crate) struct WorkResult {
    /// Indices of pieces successfully verified and written.
    completed: Vec<u32>,
    /// Total bytes downloaded (for throughput scoring).
    bytes: u64,
    /// Wall-clock time spent on the HTTP request.
    elapsed: Duration,
    /// `None` on success, `Some(WebSeedHashMismatch)` for permanent
    /// failure, `Some(_)` for transient errors.
    error: Option<ErrorKind>,
}

/// Whether a URL is actively downloading, parked, or currently busy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UrlActivity {
    /// Available for work dispatch.
    Active,
    /// Too many consecutive failures; only periodic re-probing.
    Parked,
    /// Currently has an in-flight download (work_tx has been sent to).
    InFlight,
}

/// A web seed URL with its health score and work channel.
pub(crate) struct UrlState {
    pub(crate) url: Url,
    pub(crate) health: UrlHealth,
    pub(crate) work_tx: mpsc::Sender<WorkItem>,
    pub(crate) activity: UrlActivity,
}

// ── Free helper functions (shared by FetchTask and WebSeedScheduler) ──

/// Build the full file URL for an HTTP Range request.
///
/// For single-file torrents: appends the filename if `url` ends with `/`.
/// For multi-file torrents: returns the URL as-is (future: append file path).
fn build_request_url(url: &Url, metainfo: &Metainfo) -> Result<Url, Error> {
    match &metainfo.info.mode {
        Mode::Single { name, .. } => {
            if url.path().ends_with('/') {
                url.join(name)
                    .map_err(|_| Error::new(ErrorKind::InvalidInput))
            } else {
                Ok(url.clone())
            }
        }
        Mode::Multiple { .. } => Ok(url.clone()),
    }
}

/// Length of the piece at `index` (last piece may be shorter).
fn piece_len(index: u32, metainfo: &Metainfo, piece_length: u64) -> u64 {
    let total_size = metainfo.info.total_size();
    let full_piece_count = total_size / piece_length;
    let last_piece_size = total_size % piece_length;

    if (index as u64) < full_piece_count {
        piece_length
    } else if (index as u64) == full_piece_count && last_piece_size > 0 {
        last_piece_size
    } else {
        0
    }
}

/// Probe a web seed URL with a three-level fallback.
///
/// Returns `true` if any probe succeeds.
async fn probe_url(url: &Url, metainfo: &Metainfo, probe_timeout: Duration) -> bool {
    let request_url = match build_request_url(url, metainfo) {
        Ok(u) => u,
        Err(_) => return false,
    };
    let path = request_url.path().to_string();

    if try_head(&request_url, &path, probe_timeout).await {
        tracing::debug!("web seed {url}: HEAD probe OK");
        return true;
    }
    if try_tiny_range(&request_url, &path, probe_timeout).await {
        tracing::debug!("web seed {url}: tiny Range probe OK");
        return true;
    }
    if try_short_get(
        &request_url,
        &path,
        metainfo.info.total_size(),
        probe_timeout,
    )
    .await
    {
        tracing::debug!("web seed {url}: short GET probe OK");
        return true;
    }
    tracing::debug!("web seed {url}: all probes failed");
    false
}

async fn try_head(url: &Url, path: &str, timeout: Duration) -> bool {
    HttpClient::new(timeout).head(url, path).await.is_ok()
}

async fn try_tiny_range(url: &Url, path: &str, timeout: Duration) -> bool {
    HttpClient::new(timeout)
        .get_with_range(url, path, 0, 0)
        .await
        .is_ok()
}

async fn try_short_get(url: &Url, path: &str, total_size: u64, timeout: Duration) -> bool {
    let end = 4095u64.min(total_size.saturating_sub(1));
    HttpClient::new(timeout)
        .get_with_range(url, path, 0, end)
        .await
        .is_ok()
}

// ── FetchTask: passive HTTP download worker ───────────────────────

/// A passive web seed download worker.
///
/// Waits for [`WorkItem`] messages on `work_rx`, downloads the
/// requested byte range, verifies SHA-1 hashes, writes pieces to
/// storage, and reports the result back via `result_tx`.
///
/// Unlike Phase 1's `WebSeedTask`, the fetcher does NOT scan the
/// bitfield or decide what to download — that is the scheduler's job.
pub(crate) struct FetchTask {
    /// Human-readable URL for logging.
    url: Url,
    /// HTTP client for this fetcher.
    http: HttpClient,
    /// Shared piece manager to skip already-completed pieces.
    piece_mgr: Arc<RwLock<PieceManager>>,
    /// Storage backend for writing verified pieces.
    storage: Arc<dyn Storage>,
    /// Piece length in bytes.
    piece_length: u64,
    /// Torrent metadata for URL construction and SHA-1 verification.
    metainfo: Metainfo,
    /// Receives work from the scheduler.
    work_rx: mpsc::Receiver<WorkItem>,
    /// Reports results back to the scheduler.
    result_tx: mpsc::Sender<WorkResult>,
    /// Concurrency limiter shared across all fetchers.
    semaphore: Arc<Semaphore>,
}

impl FetchTask {
    /// Create a new fetcher.  `work_rx`/`result_tx` are the channels
    /// that connect this fetcher to the [`WebSeedScheduler`].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: Url, piece_mgr: Arc<RwLock<PieceManager>>, storage: Arc<dyn Storage>,
        metainfo: Metainfo, work_rx: mpsc::Receiver<WorkItem>, result_tx: mpsc::Sender<WorkResult>,
        semaphore: Arc<Semaphore>, timeout: Duration,
    ) -> Self {
        let piece_length = metainfo.info.piece_length;
        let max_response = timeout.as_secs() * 1024 * 1024; // rough cap
        let http = HttpClient::with_max_response(timeout, max_response);

        FetchTask {
            url,
            http,
            piece_mgr,
            storage,
            piece_length,
            metainfo,
            work_rx,
            result_tx,
            semaphore,
        }
    }

    /// Run the fetcher loop.
    ///
    /// Waits for work, acquires a semaphore permit, downloads,
    /// reports the result. Exits when the channel is closed
    /// (scheduler dropped).
    pub async fn run(mut self) {
        tracing::debug!("web seed {}: fetcher started", self.url);
        while let Some(work) = self.work_rx.recv().await {
            let _permit = self.semaphore.clone().acquire_owned().await;
            let started = Instant::now();
            let result = match self.download_range(work.start_byte, work.end_byte).await {
                Ok(completed) => {
                    let bytes: u64 = completed
                        .iter()
                        .map(|&i| piece_len(i, &self.metainfo, self.piece_length))
                        .sum();
                    WorkResult {
                        completed,
                        bytes,
                        elapsed: started.elapsed(),
                        error: None,
                    }
                }
                Err(e) => {
                    let kind = e.kind();
                    if kind == ErrorKind::WebSeedHashMismatch {
                        // warn! already emitted in download_range
                    }
                    WorkResult {
                        completed: Vec::new(),
                        bytes: 0,
                        elapsed: started.elapsed(),
                        error: Some(kind),
                    }
                }
            };
            let _ = self.result_tx.send(result).await;
        }
        tracing::debug!("web seed {}: fetcher exiting (channel closed)", self.url);
    }

    /// Download a byte range, split into pieces, verify SHA-1, write to storage.
    async fn download_range(&self, start_byte: u64, end_byte: u64) -> Result<Vec<u32>, Error> {
        let request_url = build_request_url(&self.url, &self.metainfo)?;
        let path_and_query = request_url.path().to_string();

        tracing::debug!(
            "web seed {}: GET {} (bytes {}-{})",
            self.url,
            request_url,
            start_byte,
            end_byte,
        );

        let body = self
            .http
            .get_with_range(&request_url, &path_and_query, start_byte, end_byte)
            .await?;

        let mut completed = Vec::new();
        let first_piece = (start_byte / self.piece_length) as u32;
        let mut offset = 0u64;

        while offset < body.len() as u64 {
            let piece_index = first_piece + (offset / self.piece_length) as u32;
            let piece_offset = (start_byte + offset) % self.piece_length;
            let plen = piece_len(piece_index, &self.metainfo, self.piece_length);
            let chunk_end = (offset + plen - piece_offset).min(body.len() as u64);
            let chunk = &body[offset as usize..chunk_end as usize];

            if chunk.len() as u64 == plen {
                if self.piece_mgr.read().await.has_piece(piece_index) {
                    offset = chunk_end;
                    continue;
                }

                let expected_hash = match self.metainfo.info.pieces.get(piece_index as usize) {
                    Some(h) => *h,
                    None => {
                        tracing::warn!("web seed {}: piece {} out of range", self.url, piece_index);
                        break;
                    }
                };

                let actual_hash: [u8; 20] = Sha1::digest(chunk).into();
                if actual_hash != expected_hash {
                    tracing::warn!(
                        "web seed {}: SHA-1 mismatch for piece {} — discarding URL",
                        self.url,
                        piece_index,
                    );
                    return Err(Error::new(ErrorKind::WebSeedHashMismatch));
                }

                self.storage.write_piece(piece_index, chunk).await?;
                {
                    let mut pm = self.piece_mgr.write().await;
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
}

// ── WebSeedScheduler: centralized work dispatch ───────────────────

/// Centralized scheduler for web seed downloads (Phase 2).
///
/// Reads the piece bitfield, selects the largest gap, picks the
/// fastest available URL (by [`UrlHealth::ema_throughput`]), and
/// dispatches [`WorkItem`]s to [`FetchTask`]s via mpsc channels.
///
/// Parks URLs after too many consecutive failures and periodically
/// re-probes them.
pub(crate) struct WebSeedScheduler {
    /// All known URLs with health scores and work channels.
    urls: Vec<UrlState>,
    /// Shared piece manager to read bitfield.
    piece_mgr: Arc<RwLock<PieceManager>>,
    /// Torrent metadata for gap calculation.
    metainfo: Metainfo,
    /// Configuration knobs.
    config: WebSeedConfig,
    /// Receives results from all fetchers (fan-in).
    result_rx: mpsc::Receiver<WorkResult>,
    /// Woken by SwarmLoop when a P2P peer completes a piece.
    notify: Arc<Notify>,
}

impl WebSeedScheduler {
    /// Create a new scheduler.
    pub fn new(
        urls: Vec<UrlState>, piece_mgr: Arc<RwLock<PieceManager>>, metainfo: Metainfo,
        config: WebSeedConfig, result_rx: mpsc::Receiver<WorkResult>, notify: Arc<Notify>,
    ) -> Self {
        WebSeedScheduler {
            urls,
            piece_mgr,
            metainfo,
            config,
            result_rx,
            notify,
        }
    }

    /// Run the scheduler loop.
    ///
    /// 1. Probe all URLs; park failures.
    /// 2. Enter the dispatch loop, interleaving result handling
    ///    with periodic work dispatch and parked-URL revival.
    pub async fn run(mut self) {
        tracing::debug!("web seed scheduler: starting with {} URLs", self.urls.len());

        // ── Initial probe ────────────────────────────────────────
        let probe_timeout = Duration::from_secs(5);
        for state in &mut self.urls {
            if probe_url(&state.url, &self.metainfo, probe_timeout).await {
                state.activity = UrlActivity::Active;
            } else {
                state.activity = UrlActivity::Parked;
                tracing::info!("web seed {}: initial probe failed, parking", state.url);
            }
        }

        // ── Main dispatch loop ───────────────────────────────────
        let mut dispatch_tick = tokio::time::interval(Duration::from_secs(1));
        let mut revive_tick = tokio::time::interval(self.config.park_retry_interval);

        loop {
            // Exit when all pieces are complete.
            {
                let pm = self.piece_mgr.read().await;
                if pm.bitfield().iter().all(|&b| b) {
                    tracing::info!("web seed scheduler: torrent complete, exiting");
                    return;
                }
            }

            tokio::select! {
                Some(result) = self.result_rx.recv() => {
                    self.handle_result(result).await;
                }
                _ = dispatch_tick.tick() => {
                    self.dispatch_work().await;
                }
                _ = revive_tick.tick() => {
                    self.revive_parked(&probe_timeout).await;
                }
                _ = self.notify.notified() => {
                    // A P2P peer completed a piece — gaps may have changed.
                    self.dispatch_work().await;
                }
            }
        }
    }

    /// Handle a [`WorkResult`] from a fetcher.
    async fn handle_result(&mut self, result: WorkResult) {
        match result.error {
            None => {
                tracing::debug!(
                    "web seed scheduler: completed {} pieces",
                    result.completed.len(),
                );
                // Success: update health, mark URL as Active again.
                // Find which URL this result came from (we don't track
                // in-flight per-URL yet — in Phase 2 we update ALL
                // Active URLs' health optimistically, which is a
                // reasonable approximation.)
                for state in &mut self.urls {
                    if state.activity == UrlActivity::InFlight {
                        state.health.record_success(result.bytes, result.elapsed);
                        state.activity = UrlActivity::Active;
                        // Only handle the first InFlight (one result = one URL)
                        break;
                    }
                }
            }
            Some(ErrorKind::WebSeedHashMismatch) => {
                // Permanent failure — remove the URL entirely.
                self.urls.retain(|s| s.activity != UrlActivity::InFlight);
            }
            Some(_) => {
                // Transient error — record failure, maybe park.
                for state in &mut self.urls {
                    if state.activity == UrlActivity::InFlight {
                        state.health.record_failure();
                        if state.health.should_park(self.config.park_threshold) {
                            tracing::warn!(
                                "web seed {}: {} consecutive failures, parking",
                                state.url,
                                state.health.consecutive_failures,
                            );
                            state.activity = UrlActivity::Parked;
                        } else {
                            state.activity = UrlActivity::Active;
                        }
                        break;
                    }
                }
            }
        }
    }

    /// Select a gap and dispatch work to the best available URL.
    async fn dispatch_work(&mut self) {
        let bitfield = {
            let pm = self.piece_mgr.read().await;
            pm.bitfield().to_vec()
        };

        let Some((gap_start, _gap_size)) = find_largest_gap(&bitfield) else {
            return;
        };

        // Pick the best available URL: Active, work_tx not full,
        // highest throughput.
        let best_idx = self
            .urls
            .iter()
            .enumerate()
            .filter(|(_, s)| s.activity == UrlActivity::Active && !s.work_tx.is_closed())
            .max_by(|(_, a), (_, b)| {
                a.health
                    .ema_throughput
                    .partial_cmp(&b.health.ema_throughput)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

        let Some((idx, _state)) = best_idx else {
            return;
        };

        let start_byte = gap_start as u64 * self.metainfo.info.piece_length;
        let total_size = self.metainfo.info.total_size();
        let end_byte = (start_byte + self.config.max_range_bytes)
            .min(total_size)
            .saturating_sub(1);

        let work = WorkItem {
            start_byte,
            end_byte,
        };

        // try_send: if the fetcher is busy (channel full), we skip
        // and will try again on the next tick.
        if self.urls[idx].work_tx.try_send(work).is_ok() {
            self.urls[idx].activity = UrlActivity::InFlight;
        }
    }

    /// Re-probe parked URLs and revive them if they respond.
    async fn revive_parked(&mut self, probe_timeout: &Duration) {
        for state in &mut self.urls {
            if state.activity == UrlActivity::Parked
                && state
                    .health
                    .ready_for_retry(self.config.park_retry_interval)
                && probe_url(&state.url, &self.metainfo, *probe_timeout).await
            {
                tracing::info!("web seed {}: re-probe succeeded, reviving", state.url);
                state.activity = UrlActivity::Active;
                state.health = UrlHealth::default();
            }
        }
    }
}

/// Find the largest contiguous gap of missing pieces in a bitfield.
///
/// Returns `(gap_start_index, gap_size_in_pieces)`, or `None` if
/// no gaps exist (all pieces present).
///
/// Used by [`WebSeedScheduler`] to select the optimal download target.
fn find_largest_gap(bitfield: &[bool]) -> Option<(u32, u32)> {
    let (mut best_start, mut best_size) = (None, 0u32);
    let (mut gap_start, mut gap_size) = (None, 0u32);

    for (i, &has) in bitfield.iter().enumerate() {
        if !has {
            if gap_start.is_none() {
                gap_start = Some(i as u32);
            }
            gap_size += 1;
        } else {
            if let Some(start) = gap_start {
                if gap_size > best_size {
                    best_start = Some(start);
                    best_size = gap_size;
                }
                gap_start = None;
                gap_size = 0;
            }
        }
    }

    // Check trailing gap
    if let Some(start) = gap_start {
        if gap_size > best_size {
            best_start = Some(start);
            best_size = gap_size;
        }
    }

    best_start.map(|s| (s, best_size))
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use torrent_core::storage::StorageFactory;

    use crate::metainfo::MetainfoBuilder;
    use crate::storage::FileStorageFactory;

    use super::*;

    // ── Helper: mock HTTP server ─────────────────────────────────

    /// Start a mock HTTP server that serves `body` bytes on a random
    /// local port. Handles one GET request (with optional Range),
    /// then shuts down. Returns `(url, join_handle)`.
    async fn mock_http_server(body: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]);

            // Parse Range header
            let range = if request.contains("Range: bytes=") {
                let line = request
                    .lines()
                    .find(|l| l.starts_with("Range: bytes="))
                    .unwrap();
                let range_str = line.strip_prefix("Range: bytes=").unwrap();
                let parts: Vec<&str> = range_str.split('-').collect();
                let start: usize = parts[0].parse().unwrap();
                let end: usize = parts[1].parse().unwrap();
                Some((start, end))
            } else {
                None
            };

            let (response_body, status) = if let Some((start, end)) = range {
                let slice = &body[start..=end.min(body.len().saturating_sub(1))];
                (slice.to_vec(), "206 Partial Content")
            } else {
                (body.clone(), "200 OK")
            };

            let response = format!(
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                status,
                response_body.len(),
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&response_body).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        (addr, handle)
    }

    /// Build a single-file Metainfo from known data.
    fn build_test_metainfo(data: &[u8], piece_length: u32) -> crate::metainfo::Metainfo {
        let mut builder = MetainfoBuilder::new(piece_length);
        builder.add_data(data);
        builder.finish(
            "http://tracker.example.com/announce".into(),
            Mode::Single {
                name: "test.bin".into(),
                length: data.len() as u64,
            },
            Vec::new(),
            Vec::new(),
        )
    }

    // ── find_largest_gap tests ───────────────────────────────────

    #[test]
    fn find_largest_gap_empty() {
        let bf = vec![true; 10];
        assert_eq!(find_largest_gap(&bf), None);
    }

    #[test]
    fn find_largest_gap_full() {
        let bf = vec![false; 10];
        assert_eq!(find_largest_gap(&bf), Some((0, 10)));
    }

    #[test]
    fn find_largest_gap_multiple() {
        let bf = vec![
            true, true, false, false, false, false, true, false, false, true,
        ];
        assert_eq!(find_largest_gap(&bf), Some((2, 4)));
    }

    #[test]
    fn find_largest_gap_trailing() {
        let bf = vec![true, false, false, true, false, false, false];
        assert_eq!(find_largest_gap(&bf), Some((4, 3)));
    }

    #[test]
    fn find_largest_gap_leading() {
        let bf = vec![false, false, false, true, true, false];
        assert_eq!(find_largest_gap(&bf), Some((0, 3)));
    }

    #[test]
    fn find_largest_gap_single_piece() {
        let bf = vec![false];
        assert_eq!(find_largest_gap(&bf), Some((0, 1)));
    }

    #[test]
    fn find_largest_gap_zero_length() {
        let bf: Vec<bool> = vec![];
        assert_eq!(find_largest_gap(&bf), None);
    }

    // ── UrlHealth tests ──────────────────────────────────────────

    #[test]
    fn url_health_defaults() {
        let h = UrlHealth::default();
        assert_eq!(h.consecutive_failures, 0);
        assert_eq!(h.success_count, 0);
        assert_eq!(h.ema_throughput, 0.0);
        assert!(h.last_success.is_none());
        assert!(!h.should_park(5));
        assert!(h.ready_for_retry(Duration::from_secs(60)));
    }

    #[test]
    fn url_health_record_success_resets_failures() {
        let mut h = UrlHealth::default();
        h.record_failure();
        h.record_failure();
        assert_eq!(h.consecutive_failures, 2);
        h.record_success(1024, Duration::from_millis(100));
        assert_eq!(h.consecutive_failures, 0);
        assert_eq!(h.success_count, 1);
        assert!(h.ema_throughput > 0.0);
    }

    #[test]
    fn url_health_should_park() {
        let mut h = UrlHealth::default();
        for _ in 0..4 {
            h.record_failure();
        }
        assert!(!h.should_park(5));
        h.record_failure();
        assert!(h.should_park(5));
    }

    #[test]
    fn url_health_ema_converges() {
        let mut h = UrlHealth::default();
        // First measurement: 1000 bytes in 1s = 1000 B/s
        h.record_success(1000, Duration::from_secs(1));
        assert!((h.ema_throughput - 1000.0).abs() < 1.0);
        // Second: 2000 B/s — EMA = 0.3*2000 + 0.7*1000 = 1300
        h.record_success(2000, Duration::from_secs(1));
        assert!((h.ema_throughput - 1300.0).abs() < 1.0);
    }

    // ── download_range async tests (mock HTTP server) ────────────

    #[tokio::test]
    async fn downloads_full_file_single_piece() {
        let piece_length = 256u32;
        let data = vec![0xABu8; piece_length as usize];

        let metainfo = build_test_metainfo(&data, piece_length);

        // Test HttpClient direct connectivity
        let (server_url, _server) = mock_http_server(data.clone()).await;
        let url = Url::parse(&server_url).unwrap();
        let client = HttpClient::new(Duration::from_secs(5));
        let body = client.get_with_range(&url, "/", 0, 255).await.unwrap();
        assert_eq!(body.len(), 256);

        // Test FetchTask via download_range directly
        let (server_url2, _server2) = mock_http_server(data.clone()).await;
        let url2 = Url::parse(&server_url2).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let factory = FileStorageFactory::new(tmp.path().to_path_buf());
        let storage = factory.create(&metainfo.info).await.unwrap();
        storage.prepare().await.unwrap();
        let piece_mgr = Arc::new(RwLock::new(PieceManager::new(metainfo.info.num_pieces())));

        let (_work_tx, work_rx) = mpsc::channel::<WorkItem>(1);
        let (result_tx, _result_rx) = mpsc::channel::<WorkResult>(1);
        let timeout = Duration::from_secs(5);

        let task = FetchTask::new(
            url2,
            piece_mgr.clone(),
            storage.clone(),
            metainfo.clone(),
            work_rx,
            result_tx,
            Arc::new(Semaphore::new(1)),
            timeout,
        );

        let completed = task.download_range(0, 255).await.unwrap();
        assert_eq!(completed, vec![0]);

        let pm = piece_mgr.read().await;
        assert!(pm.has_piece(0));
    }

    #[tokio::test]
    async fn downloads_multiple_pieces() {
        let piece_length = 128u32;
        let data: Vec<u8> = (0u32..(piece_length * 3) as u32).map(|v| v as u8).collect();
        let metainfo = build_test_metainfo(&data, piece_length);

        let (server_url, _server) = mock_http_server(data.clone()).await;
        let url = Url::parse(&server_url).unwrap();

        // Verify HttpClient returns correct body
        let client = HttpClient::new(Duration::from_secs(5));
        let body = client.get_with_range(&url, "/", 0, 383).await.unwrap();
        assert_eq!(body.len(), 384, "body should be 384 bytes");
        assert_eq!(&body[..128], &data[0..128]);
        assert_eq!(&body[128..256], &data[128..256]);
        assert_eq!(&body[256..384], &data[256..384]);

        // Now test download_range with a fresh mock server
        let (server_url2, _server2) = mock_http_server(data.clone()).await;
        let url2 = Url::parse(&server_url2).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let factory = FileStorageFactory::new(tmp.path().to_path_buf());
        let storage = factory.create(&metainfo.info).await.unwrap();
        storage.prepare().await.unwrap();

        let piece_mgr = Arc::new(RwLock::new(PieceManager::new(metainfo.info.num_pieces())));

        let (_work_tx, work_rx) = mpsc::channel::<WorkItem>(1);
        let (result_tx, _result_rx) = mpsc::channel::<WorkResult>(1);
        let timeout = Duration::from_secs(5);

        let task = FetchTask::new(
            url2,
            piece_mgr.clone(),
            storage.clone(),
            metainfo.clone(),
            work_rx,
            result_tx,
            Arc::new(Semaphore::new(1)),
            timeout,
        );

        let completed = task.download_range(0, 383).await.unwrap();
        assert_eq!(completed, vec![0, 1, 2]);

        let pm = piece_mgr.read().await;
        assert!(pm.has_piece(0));
        assert!(pm.has_piece(1));
        assert!(pm.has_piece(2));
    }

    #[tokio::test]
    async fn sha1_mismatch_does_not_mark_complete() {
        let piece_length = 256u32;
        let correct_data = vec![0xABu8; piece_length as usize];
        let wrong_data = vec![0xCDu8; piece_length as usize];

        let metainfo = build_test_metainfo(&correct_data, piece_length);

        let (server_url, _server) = mock_http_server(wrong_data).await;

        let tmp = tempfile::tempdir().unwrap();
        let factory = FileStorageFactory::new(tmp.path().to_path_buf());
        let storage = factory.create(&metainfo.info).await.unwrap();
        storage.prepare().await.unwrap();

        let piece_mgr = Arc::new(RwLock::new(PieceManager::new(metainfo.info.num_pieces())));

        let url = Url::parse(&server_url).unwrap();
        let (work_tx, work_rx) = mpsc::channel::<WorkItem>(1);
        let (result_tx, mut result_rx) = mpsc::channel::<WorkResult>(1);
        let timeout = Duration::from_secs(5);
        let semaphore = Arc::new(Semaphore::new(1));

        let fetcher = FetchTask::new(
            url,
            piece_mgr.clone(),
            storage.clone(),
            metainfo.clone(),
            work_rx,
            result_tx,
            semaphore.clone(),
            timeout,
        );

        // Spawn the fetcher and send it work
        let handle = tokio::spawn(async move { fetcher.run().await });
        let _ = work_tx
            .send(WorkItem {
                start_byte: 0,
                end_byte: 255,
            })
            .await;

        // Wait for the result (SHA-1 will fail, fetcher exits)
        let result = tokio::time::timeout(Duration::from_secs(3), result_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.error, Some(ErrorKind::WebSeedHashMismatch));
        assert!(result.completed.is_empty());

        let pm = piece_mgr.read().await;
        assert!(
            !pm.has_piece(0),
            "piece should NOT be marked complete with wrong data"
        );
        drop(pm);

        handle.abort();
    }
}
