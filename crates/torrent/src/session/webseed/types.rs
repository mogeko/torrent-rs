//! Configuration, health scoring, and URL types for web seed downloads (BEP 19).

use std::time::{Duration, Instant};

use crate::net::Url;

/// Configuration for web seed downloads (BEP 19).
#[derive(Debug, Clone)]
pub(crate) struct WebSeedConfig {
    /// Minimum contiguous gap (in pieces) to trigger an HTTP download.
    pub min_gap_pieces: u32,
    /// Upper bound for adaptive Range request size. Default: 5 MB.
    pub max_range_bytes: u64,
    /// Consecutive HTTP failures before parking a URL.
    pub park_threshold: u32,
    /// How long to wait before retrying a parked URL.
    pub park_retry_interval: Duration,
}

impl Default for WebSeedConfig {
    fn default() -> Self {
        WebSeedConfig {
            min_gap_pieces: 4,
            max_range_bytes: 5 * 1024 * 1024,
            park_threshold: 5,
            park_retry_interval: Duration::from_secs(60),
        }
    }
}

// ── URL Health Tracking ────────────────────────────────────────────

/// Per-URL health and performance tracking for UCB multi-armed bandit
/// URL selection (BEP 19).
///
/// Updated after each HTTP Range request from within the
/// [`Service::call`] Future via shared [`RwLock`] state.
#[derive(Debug, Clone)]
pub(crate) struct UrlHealth {
    /// Exponential moving average of throughput (bytes/sec).
    ema_throughput: f64,
    /// Consecutive failures since last success.
    consecutive_failures: u32,
    /// Total download attempts (success + failure).
    download_attempts: u64,
    /// When the last attempt completed (for park retry timing).
    last_attempt: Option<Instant>,
}

impl Default for UrlHealth {
    fn default() -> Self {
        UrlHealth {
            ema_throughput: 0.0,
            consecutive_failures: 0,
            download_attempts: 0,
            last_attempt: None,
        }
    }
}

impl UrlHealth {
    /// Decay factor for the exponential moving average.
    const ALPHA: f64 = 0.3;

    /// Record a successful download.
    pub(crate) fn record_success(&mut self, bytes: u64, elapsed: Duration) {
        let throughput = bytes as f64 / elapsed.as_secs_f64().max(0.001);
        self.ema_throughput = if self.download_attempts == 0 {
            throughput
        } else {
            Self::ALPHA * throughput + (1.0 - Self::ALPHA) * self.ema_throughput
        };
        self.consecutive_failures = 0;
        self.download_attempts += 1;
        self.last_attempt = Some(Instant::now());
    }

    /// Record a failed download attempt.
    pub(crate) fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.download_attempts += 1;
        self.last_attempt = Some(Instant::now());
    }

    /// Whether this URL should be parked (too many consecutive failures).
    pub(crate) fn should_park(&self, threshold: u32) -> bool {
        self.consecutive_failures >= threshold
    }

    /// Number of consecutive failures.
    pub(crate) fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    pub(crate) fn ready_for_retry(&self, interval: Duration) -> bool {
        self.ready_for_retry_elapsed() >= interval.as_secs_f64()
    }

    pub(crate) fn ready_for_retry_elapsed(&self) -> f64 {
        self.last_attempt
            .map(|t| t.elapsed().as_secs_f64())
            .unwrap_or(0.0)
    }

    pub(crate) fn ema_throughput(&self) -> f64 {
        self.ema_throughput
    }
    pub(crate) fn download_attempts(&self) -> u64 {
        self.download_attempts
    }

    /// UCB-weighted score for multi-armed bandit URL selection.
    pub(crate) fn ucb_score(&self, total_attempts: u64, c: f64) -> f64 {
        let n = (self.download_attempts + 1) as f64;
        let bonus = c * ((total_attempts.max(1) + 1) as f64).ln().sqrt() / n.sqrt();
        self.ema_throughput() + bonus
    }
}

// ── URL and Range types ────────────────────────────────────────────

/// A byte range to download from a web seed.
#[derive(Debug, Clone)]
pub(crate) struct PieceRange {
    pub start_byte: u64,
    pub end_byte: u64,
}

/// Whether a URL is active or parked (too many consecutive failures).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UrlActivity {
    Active,
    Parked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UrlKind {
    Directory,
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

/// A web seed URL with its health score and activity state.
pub(crate) struct UrlState {
    pub(crate) url: Url,
    pub(crate) health: UrlHealth,
    pub(crate) activity: UrlActivity,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_health_defaults() {
        let h = UrlHealth::default();
        assert!(h.ema_throughput() == 0.0);
        assert_eq!(h.download_attempts(), 0);
        assert_eq!(h.consecutive_failures(), 0);
        assert!(!h.should_park(5));
    }

    #[test]
    fn url_health_record_success() {
        let mut h = UrlHealth::default();
        h.record_success(1000, Duration::from_secs(1));
        assert!(h.ema_throughput() > 0.0);
        assert_eq!(h.download_attempts(), 1);
        assert_eq!(h.consecutive_failures(), 0);
    }

    #[test]
    fn url_health_record_failure() {
        let mut h = UrlHealth::default();
        h.record_failure();
        assert_eq!(h.download_attempts(), 1);
        assert_eq!(h.consecutive_failures(), 1);
        assert!(h.last_attempt.is_some());
    }

    #[test]
    fn url_health_should_park() {
        let mut h = UrlHealth::default();
        for _ in 0..5 {
            h.record_failure();
        }
        assert!(h.should_park(5));
    }
}
