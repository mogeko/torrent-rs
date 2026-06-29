//! Configuration, health scoring, and scheduler↔fetcher message types
//! for the web seed download engine (BEP 19).

use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use url::Url;

use crate::error::ErrorKind;

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
    #[expect(dead_code, reason = "reserved for per-URL backoff in scheduler")]
    pub retry_delay: Duration,
    /// Maximum concurrent in-flight HTTP Range requests across all
    /// web seed tasks.
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
    pub(crate) consecutive_failures: u32,
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
    pub(crate) fn record_success(&mut self, bytes: u64, elapsed: Duration) {
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
    pub(crate) fn record_failure(&mut self) {
        self.consecutive_failures += 1;
    }

    /// Whether this URL should be parked (too many consecutive failures).
    pub(crate) fn should_park(&self, threshold: u32) -> bool {
        self.consecutive_failures >= threshold
    }

    /// Whether enough time has passed to retry a parked URL.
    pub(crate) fn ready_for_retry(&self, interval: Duration) -> bool {
        match self.last_success {
            Some(t) => t.elapsed() >= interval,
            None => true,
        }
    }

    /// Current EMA throughput (bytes/sec).  Used by the scheduler
    /// to rank URLs by speed.
    pub(crate) fn ema_throughput(&self) -> f64 {
        self.ema_throughput
    }
}

// ── Scheduler ↔ Fetcher message types ─────────────────────────────

/// A unit of work dispatched by the scheduler to a fetcher.
#[derive(Debug, Clone)]
pub(crate) struct WorkItem {
    /// Byte offset to start downloading from (inclusive).
    pub(crate) start_byte: u64,
    /// Byte offset to end downloading at (inclusive).
    pub(crate) end_byte: u64,
}

/// Result of a [`WorkItem`] reported back to the scheduler.
pub(crate) struct WorkResult {
    /// Indices of pieces successfully verified and written.
    pub(crate) completed: Vec<u32>,
    /// Total bytes downloaded (for throughput scoring).
    pub(crate) bytes: u64,
    /// Wall-clock time spent on the HTTP request.
    pub(crate) elapsed: Duration,
    /// `None` on success, `Some(WebSeedHashMismatch)` for permanent
    /// failure, `Some(_)` for transient errors.
    pub(crate) error: Option<ErrorKind>,
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

/// Whether a web seed URL is a directory (append file path) or
/// a script/explicit URL (serves the whole torrent as one file).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UrlKind {
    /// Directory URL (BEP 19 §2): ends with `/`, client appends file path.
    Directory,
    /// Script or explicit file URL: serves the entire torrent content.
    Script,
}

impl UrlKind {
    pub(crate) fn classify(url: &Url) -> Self {
        if url.path().ends_with('/') {
            UrlKind::Directory
        } else {
            UrlKind::Script
        }
    }
}

/// A web seed URL with its health score and work channel.
pub(crate) struct UrlState {
    pub(crate) url: Url,
    pub(crate) url_kind: UrlKind,
    pub(crate) health: UrlHealth,
    pub(crate) work_tx: mpsc::Sender<WorkItem>,
    pub(crate) activity: UrlActivity,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── UrlHealth tests ──────────────────────────────────────────

    #[test]
    fn url_health_defaults() {
        let h = UrlHealth::default();
        assert_eq!(h.consecutive_failures, 0);
        assert_eq!(h.success_count, 0);
        assert_eq!(h.ema_throughput(), 0.0);
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
        assert!(h.ema_throughput() > 0.0);
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
        assert!((h.ema_throughput() - 1000.0).abs() < 1.0);
        // Second: 2000 B/s — EMA = 0.3*2000 + 0.7*1000 = 1300
        h.record_success(2000, Duration::from_secs(1));
        assert!((h.ema_throughput() - 1300.0).abs() < 1.0);
    }
}
