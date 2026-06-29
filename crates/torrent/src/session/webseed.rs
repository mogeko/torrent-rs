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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use sha1::{Digest, Sha1};
use tokio::sync::{Notify, RwLock, Semaphore};
use url::Url;

use crate::error::{Error, ErrorKind};
use crate::metainfo::{Metainfo, Mode};
use crate::net::http::HttpClient;
use crate::piece::PieceManager;
use crate::storage::Storage;

/// Shared cursor for round-robin gap scanning.
///
/// Each task atomically advances by [`SCAN_STEP`] pieces, then scans
/// forward from that position for the first missing piece.  This
/// distributes scan starting points across tasks instead of having
/// every task converge on the same "largest gap."
static NEXT_SCAN_START: AtomicU64 = AtomicU64::new(0);

/// Number of pieces to advance the scan cursor per call to [`claim_gap`].
const SCAN_STEP: u64 = 16;

/// Configuration for web seed downloads (BEP 19).
#[derive(Debug, Clone)]
pub(crate) struct WebSeedConfig {
    /// Minimum contiguous gap (in pieces) to trigger an HTTP download.
    /// Prevents tiny range requests. Default: 4 pieces.
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
struct UrlHealth {
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
    #[allow(dead_code, reason = "used by Phase 2 centralized scheduler")]
    fn ready_for_retry(&self, interval: Duration) -> bool {
        match self.last_success {
            Some(t) => t.elapsed() >= interval,
            None => true, // never succeeded — always ready to retry
        }
    }
}

/// A single web seed download task.
///
/// Runs in the background for one web seed URL. Reads the piece
/// bitfield to find gaps, downloads them via HTTP Range requests,
/// verifies SHA-1 hashes, and writes completed pieces to storage.
pub(crate) struct WebSeedTask {
    /// Base URL from the torrent metadata or magnet link.
    url: Url,
    /// HTTP client for this seed.
    http: HttpClient,
    /// Shared piece manager to read progress / mark pieces.
    piece_mgr: Arc<RwLock<PieceManager>>,
    /// Storage backend for writing verified pieces.
    storage: Arc<dyn Storage>,
    /// Piece length in bytes (constant for all non-final pieces).
    piece_length: u64,
    /// Number of pieces in the torrent.
    #[expect(
        dead_code,
        reason = "set during construction, used by future multi-file range mapping"
    )]
    num_pieces: u32,
    /// Torrent metadata for URL construction and SHA-1 verification.
    metainfo: Metainfo,
    /// Configuration knobs.
    config: WebSeedConfig,
    /// Notification channel — woken when a peer completes a piece
    /// (so we can re-evaluate gaps).
    notify: Arc<Notify>,
    /// Concurrency limiter shared across all web seed tasks.
    /// Acquired before each HTTP Range request and released after,
    /// preventing TLS handshake starvation on single-threaded runtimes.
    semaphore: Arc<Semaphore>,
}

impl WebSeedTask {
    /// Create a new web seed download task.
    ///
    /// `url` is the base web seed URL from `url-list` or `ws` parameter.
    /// If it ends with `/`, the file path is appended (multi-file).
    pub fn new(
        url: Url, piece_mgr: Arc<RwLock<PieceManager>>, storage: Arc<dyn Storage>,
        metainfo: Metainfo, config: WebSeedConfig, notify: Arc<Notify>, semaphore: Arc<Semaphore>,
    ) -> Self {
        let num_pieces = metainfo.info.num_pieces() as u32;
        let piece_length = metainfo.info.piece_length;
        let max_range_bytes = config.max_range_bytes + 1024 * 1024;
        let http = HttpClient::with_max_response(config.timeout, max_range_bytes);

        WebSeedTask {
            url,
            http,
            piece_mgr,
            storage,
            piece_length,
            num_pieces,
            metainfo,
            config,
            notify,
            semaphore,
        }
    }

    /// Probe the web seed URL with a three-level fallback.
    ///
    /// Level 1: HTTP HEAD (fastest — no body transfer).
    /// Level 2: Range `bytes=0-0` GET (1-byte body, best compatibility).
    /// Level 3: Short-timeout GET on the full URL.
    ///
    /// Returns `true` if any probe succeeds.
    async fn probe(&self) -> bool {
        let request_url = match self.build_request_url() {
            Ok(url) => url,
            Err(_) => return false,
        };
        let path = request_url.path().to_string();
        let probe_timeout = Duration::from_secs(5);

        // Level 1: HEAD
        if self.try_head(&request_url, &path, probe_timeout).await {
            tracing::debug!("web seed {}: HEAD probe OK", self.url);
            return true;
        }

        // Level 2: Range bytes=0-0
        if self
            .try_tiny_range(&request_url, &path, probe_timeout)
            .await
        {
            tracing::debug!("web seed {}: tiny Range probe OK", self.url);
            return true;
        }

        // Level 3: Short GET
        if self.try_short_get(&request_url, &path, probe_timeout).await {
            tracing::debug!("web seed {}: short GET probe OK", self.url);
            return true;
        }

        tracing::debug!("web seed {}: all probes failed", self.url);
        false
    }

    /// Level 1 probe: HTTP HEAD request.
    async fn try_head(&self, url: &Url, path: &str, timeout: Duration) -> bool {
        let client = HttpClient::new(timeout);
        client.head(url, path).await.is_ok()
    }

    /// Level 2 probe: Range bytes=0-0 GET (1 byte).
    async fn try_tiny_range(&self, url: &Url, path: &str, timeout: Duration) -> bool {
        let client = HttpClient::new(timeout);
        client.get_with_range(url, path, 0, 0).await.is_ok()
    }

    /// Level 3 probe: short-timeout GET on the first few KB.
    async fn try_short_get(&self, url: &Url, path: &str, timeout: Duration) -> bool {
        let client = HttpClient::new(timeout);
        // Request just the first 4 KB — enough to verify the server
        // serves the file without transferring a large body.
        let end = 4095u64.min(self.metainfo.info.total_size().saturating_sub(1));
        client.get_with_range(url, path, 0, end).await.is_ok()
    }

    /// Parked loop: periodically re-probe the URL.
    ///
    /// Returns when a probe succeeds (caller should then enter the
    /// main download loop).  Never returns if the task is aborted.
    async fn parked_loop(&self) {
        let interval = self.config.park_retry_interval;
        tracing::info!(
            "web seed {}: parked, will re-probe every {:?}",
            self.url,
            interval,
        );
        loop {
            tokio::time::sleep(interval).await;
            if self.probe().await {
                tracing::info!("web seed {}: re-probe succeeded, resuming", self.url);
                return;
            }
        }
    }

    /// Run the web seed download loop.
    ///
    /// Probes connectivity first, then fills gaps via HTTP Range
    /// requests with health tracking. Parks the URL after too many
    /// consecutive failures.
    pub async fn run(self) {
        tracing::debug!("web seed {}: starting", self.url);

        // ── Connectivity probe ──────────────────────────────────

        if !self.probe().await {
            self.parked_loop().await;
        }

        // ── Main download loop ───────────────────────────────────

        let mut health = UrlHealth::default();
        let mut retry_delay = self.config.retry_delay;

        loop {
            // Read current bitfield
            let bitfield = {
                let pm = self.piece_mgr.read().await;
                pm.bitfield().to_vec()
            };

            // Exit when download is complete
            if bitfield.iter().all(|&b| b) {
                tracing::info!("web seed {}: torrent complete, exiting", self.url);
                return;
            }

            // Find a gap using round-robin scanning (avoids all
            // tasks converging on the same "largest gap").
            let Some((gap_start, _gap_size)) = claim_gap(&bitfield, self.config.min_gap_pieces)
            else {
                tracing::debug!("web seed {}: no qualifying gap, sleeping", self.url);
                self.notify.notified().await;
                continue;
            };

            // Calculate byte range
            let start_byte = gap_start as u64 * self.piece_length;
            let total_size = self.metainfo.info.total_size();
            let end_byte = (start_byte + self.config.max_range_bytes)
                .min(total_size)
                .saturating_sub(1);

            // Acquire a concurrency permit before the HTTP call.
            let _permit = self.semaphore.clone().acquire_owned().await;

            let download_start = Instant::now();
            match self.download_range(start_byte, end_byte).await {
                Ok(downloaded_pieces) => {
                    let elapsed = download_start.elapsed();
                    let bytes: u64 = downloaded_pieces.iter().map(|&i| self.piece_len(i)).sum();
                    health.record_success(bytes, elapsed);
                    retry_delay = self.config.retry_delay; // reset backoff

                    for index in &downloaded_pieces {
                        tracing::debug!("web seed {}: completed piece {}", self.url, index);
                    }
                    if !downloaded_pieces.is_empty() {
                        self.notify.notify_one();
                    }
                }
                Err(ref e) if e.kind() == ErrorKind::WebSeedHashMismatch => {
                    // SHA-1 mismatch — BEP 19 says discard the URL permanently.
                    return;
                }
                Err(e) => {
                    health.record_failure();
                    tracing::warn!(
                        "web seed {}: download failed ({}) [failures: {}], retrying in {:?}",
                        self.url,
                        e,
                        health.consecutive_failures,
                        retry_delay,
                    );

                    // Park the URL after too many consecutive failures.
                    if health.should_park(self.config.park_threshold) {
                        tracing::warn!(
                            "web seed {}: {} consecutive failures, parking",
                            self.url,
                            health.consecutive_failures,
                        );
                        self.parked_loop().await;
                        // Reset health after successful re-probe.
                        health = UrlHealth::default();
                        retry_delay = self.config.retry_delay;
                        continue;
                    }

                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(60));
                }
            }
        }
    }

    /// Download a byte range from the web seed, split into pieces,
    /// verify SHA-1, and write to storage.
    ///
    /// Returns the list of piece indices that were successfully
    /// completed.
    async fn download_range(&self, start_byte: u64, end_byte: u64) -> Result<Vec<u32>, Error> {
        let request_url = self.build_request_url()?;
        let path_and_query = request_url.path().to_string();

        tracing::debug!(
            "web seed {}: GET {} (bytes {}-{})",
            self.url,
            request_url,
            start_byte,
            end_byte,
        );

        let http_client = &self.http;
        let body = http_client
            .get_with_range(&request_url, &path_and_query, start_byte, end_byte)
            .await?;

        // Split the response into piece-sized chunks and verify
        let mut completed = Vec::new();
        let first_piece = (start_byte / self.piece_length) as u32;
        let mut offset = 0u64;

        while offset < body.len() as u64 {
            let piece_index = first_piece + (offset / self.piece_length) as u32;
            let piece_offset = (start_byte + offset) % self.piece_length;
            let piece_len = self.piece_len(piece_index);
            let chunk_end = (offset + piece_len - piece_offset).min(body.len() as u64);
            let chunk = &body[offset as usize..chunk_end as usize];

            // Only verify if we have the full piece
            if chunk.len() as u64 == piece_len {
                // Skip if a P2P peer already completed this piece
                // while we were downloading.
                if self.piece_mgr.read().await.has_piece(piece_index) {
                    offset = chunk_end;
                    continue;
                }

                let expected_hash = match self.metainfo.info.pieces.get(piece_index as usize) {
                    Some(h) => *h,
                    None => {
                        tracing::warn!("web seed {}: piece {} out of range", self.url, piece_index,);
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

                // Write the verified piece
                self.storage.write_piece(piece_index, chunk).await?;

                // Mark as complete
                {
                    let mut pm = self.piece_mgr.write().await;
                    pm.set_piece(piece_index);
                }
                completed.push(piece_index);
            } else {
                // Partial piece at the end of the range —
                // don't verify yet; it'll be completed by peers
                // or the next web seed range download.
                break;
            }

            offset = chunk_end;
        }

        Ok(completed)
    }

    /// Build the full file URL for a byte range request.
    ///
    /// For single-file torrents:
    /// - If the URL ends with `/` (BEP 19 directory URL), appends the
    ///   torrent file name: `http://mirror.com/pub/` → `http://mirror.com/pub/file.iso`
    /// - If the URL is an explicit file URL, returns it as-is.
    fn build_request_url(&self) -> Result<Url, Error> {
        match &self.metainfo.info.mode {
            Mode::Single { name, .. } => {
                if self.url.path().ends_with('/') {
                    match self.url.join(name) {
                        Ok(url) => Ok(url),
                        Err(_) => Err(Error::new(ErrorKind::InvalidInput)),
                    }
                } else {
                    Ok(self.url.clone())
                }
            }
            Mode::Multiple { .. } => Ok(self.url.clone()),
        }
    }

    /// Length of the piece at `index` (last piece may be shorter).
    fn piece_len(&self, index: u32) -> u64 {
        let total_size = self.metainfo.info.total_size();
        let full_piece_count = total_size / self.piece_length;
        let last_piece_size = total_size % self.piece_length;

        if (index as u64) < full_piece_count {
            self.piece_length
        } else if (index as u64) == full_piece_count && last_piece_size > 0 {
            last_piece_size
        } else {
            0 // beyond the last piece (or empty torrent)
        }
    }
}

/// Claim a gap of missing pieces using round-robin scanning.
///
/// Each call atomically advances a shared cursor by [`SCAN_STEP`]
/// pieces, then scans forward from that position (wrapping around)
/// for the first gap of at least `min_gap_pieces`.
///
/// Returns `(gap_start_index, gap_size)` or `None` if no qualifying
/// gap exists.
fn claim_gap(bitfield: &[bool], min_gap_pieces: u32) -> Option<(u32, u32)> {
    let num_pieces = bitfield.len() as u64;
    if num_pieces == 0 {
        return None;
    }

    let start = NEXT_SCAN_START.fetch_add(SCAN_STEP, Ordering::Relaxed) % num_pieces;
    let start = start as usize;

    // Scan forward from `start`, wrapping around
    for offset in 0..num_pieces as usize {
        let idx = (start + offset) % num_pieces as usize;
        if !bitfield[idx] {
            // Found start of a gap — measure its size
            let gap_start = idx as u32;
            let mut gap_size = 0u32;
            for j in 0..num_pieces as usize {
                let check = (idx + j) % num_pieces as usize;
                if !bitfield[check] {
                    gap_size += 1;
                } else {
                    break;
                }
            }
            if gap_size >= min_gap_pieces {
                return Some((gap_start, gap_size));
            }
            // Gap too small, skip it — the outer loop advances past it.
            // Note: we don't advance `offset` explicitly here because
            // `offset` is incremented by the for-loop; we just let the
            // scan continue naturally past the small gap.
        }
    }
    None
}

/// Find the largest contiguous gap of missing pieces in a bitfield.
///
/// Returns `(gap_start_index, gap_size_in_pieces)`, or `None` if
/// no gaps exist (all pieces present).
///
/// Kept for backward compatibility and for potential use in
/// Phase 2's centralized coordinator.
#[allow(dead_code, reason = "kept for Phase 2 centralized scheduler")]
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

    // ── claim_gap tests ──────────────────────────────────────────

    #[test]
    fn claim_gap_finds_first_qualifying() {
        let bf = vec![false; 100];
        // All missing — should find a gap starting somewhere
        let result = claim_gap(&bf, 4);
        assert!(result.is_some());
        let (start, size) = result.unwrap();
        assert!(size >= 4);
        assert!((start as usize) < 100);
    }

    #[test]
    fn claim_gap_respects_min_size() {
        // [have, have, have, MISS×2, have, have, had, MISS×10, have]
        let mut bf = vec![true; 20];
        bf[3] = false;
        bf[4] = false; // gap of 2 — too small
        bf[9] = false;
        bf[10] = false;
        bf[11] = false;
        bf[12] = false;
        bf[13] = false; // gap of 5 — qualifies
        // claim_gap should skip the 2-piece gap and find the 5-piece one
        let result = claim_gap(&bf, 4);
        assert!(result.is_some());
        let (start, size) = result.unwrap();
        assert!(size >= 4);
        assert!(
            start >= 9,
            "should skip the small gap at 3-4, got start={start}"
        );
    }

    #[test]
    fn claim_gap_none_when_no_qualifying_gap() {
        let mut bf = vec![true; 10];
        bf[5] = false; // single missing piece
        assert_eq!(claim_gap(&bf, 4), None);
    }

    #[test]
    fn claim_gap_empty_bitfield() {
        assert_eq!(claim_gap(&[], 4), None);
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

        // Test WebSeedTask via download_range directly
        let (server_url2, _server2) = mock_http_server(data.clone()).await;
        let url2 = Url::parse(&server_url2).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let factory = FileStorageFactory::new(tmp.path().to_path_buf());
        let storage = factory.create(&metainfo.info).await.unwrap();
        storage.prepare().await.unwrap();
        let piece_mgr = Arc::new(RwLock::new(PieceManager::new(metainfo.info.num_pieces())));
        let notify = Arc::new(Notify::new());
        let config = WebSeedConfig {
            min_gap_pieces: 1,
            max_range_bytes: 5 * 1024 * 1024,
            timeout: Duration::from_secs(5),
            retry_delay: Duration::from_millis(100),
            max_concurrent: 1,
            park_threshold: 5,
            park_retry_interval: Duration::from_secs(60),
        };

        let task = WebSeedTask {
            url: url2,
            http: HttpClient::new(config.timeout),
            piece_mgr: piece_mgr.clone(),
            storage: storage.clone(),
            piece_length: metainfo.info.piece_length,
            num_pieces: metainfo.info.num_pieces() as u32,
            metainfo: metainfo.clone(),
            config: config.clone(),
            notify,
            semaphore: Arc::new(Semaphore::new(1)),
        };

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
        let notify = Arc::new(Notify::new());
        let config = WebSeedConfig {
            min_gap_pieces: 1,
            max_range_bytes: piece_length as u64 * 3,
            timeout: Duration::from_secs(5),
            retry_delay: Duration::from_millis(100),
            max_concurrent: 1,
            park_threshold: 5,
            park_retry_interval: Duration::from_secs(60),
        };

        let task = WebSeedTask {
            url: url2,
            http: HttpClient::new(config.timeout),
            piece_mgr: piece_mgr.clone(),
            storage: storage.clone(),
            piece_length: metainfo.info.piece_length,
            num_pieces: metainfo.info.num_pieces() as u32,
            metainfo: metainfo.clone(),
            config,
            notify,
            semaphore: Arc::new(Semaphore::new(1)),
        };

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
        let notify = Arc::new(Notify::new());

        let config = WebSeedConfig {
            min_gap_pieces: 1,
            max_range_bytes: 5 * 1024 * 1024,
            timeout: Duration::from_secs(5),
            retry_delay: Duration::from_millis(100),
            max_concurrent: 1,
            park_threshold: 5,
            park_retry_interval: Duration::from_secs(60),
        };

        let task = WebSeedTask::new(
            url,
            piece_mgr.clone(),
            storage.clone(),
            metainfo.clone(),
            config,
            notify,
            Arc::new(Semaphore::new(1)),
        );

        let task_handle = tokio::spawn(async move { task.run().await });

        // Wait for the task to try downloading (SHA-1 will fail)
        tokio::time::sleep(Duration::from_secs(1)).await;

        let pm = piece_mgr.read().await;
        assert!(
            !pm.has_piece(0),
            "piece should NOT be marked complete with wrong data"
        );
        drop(pm);

        task_handle.abort();
    }
}
