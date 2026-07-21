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
    /// How long to wait before retrying a parked URL.
    pub park_retry_interval: Duration,
}

impl Default for WebSeedConfig {
    fn default() -> Self {
        WebSeedConfig {
            min_gap_pieces: 4,
            max_range_bytes: 5 * 1024 * 1024,
            park_retry_interval: Duration::from_secs(60),
        }
    }
}

// ── URL Health Tracking ────────────────────────────────────────────

/// Per-URL health and performance tracking for runtime UCB scoring.
///
/// Updated after each HTTP Range request via [`record_success`] /
/// [`record_failure`].  Used by [`super::WebSeedService::select_best_url`]
/// for UCB-weighted multi-armed bandit URL selection.
///
/// [`record_success`]: UrlHealth::record_success
/// [`record_failure`]: UrlHealth::record_failure
#[derive(Debug, Clone)]
pub(crate) struct UrlHealth {
    ema_throughput: f64,
    download_attempts: u64,
    last_attempt: Option<Instant>,
}

impl Default for UrlHealth {
    fn default() -> Self {
        UrlHealth {
            ema_throughput: 0.0,
            download_attempts: 0,
            last_attempt: None,
        }
    }
}

impl UrlHealth {
    /// Decay factor for the exponential moving average of throughput.
    #[allow(dead_code)]
    const ALPHA: f64 = 0.3;

    /// Record a successful download (called from the Service::call path
    /// once mutable self access is wired in).
    #[allow(dead_code)]
    pub(crate) fn record_success(&mut self, bytes: u64, elapsed: Duration) {
        let throughput = bytes as f64 / elapsed.as_secs_f64().max(0.001);
        self.ema_throughput = if self.download_attempts == 0 {
            throughput
        } else {
            Self::ALPHA * throughput + (1.0 - Self::ALPHA) * self.ema_throughput
        };
        self.download_attempts += 1;
        self.last_attempt = Some(Instant::now());
    }

    /// Record a failed download attempt.
    #[allow(dead_code)]
    pub(crate) fn record_failure(&mut self) {
        self.download_attempts += 1;
        self.last_attempt = Some(Instant::now());
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
#[allow(dead_code)]
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
    }

    #[test]
    fn url_health_record_success() {
        let mut h = UrlHealth::default();
        h.record_success(1000, Duration::from_secs(1));
        assert!(h.ema_throughput() > 0.0);
        assert_eq!(h.download_attempts(), 1);
    }

    #[test]
    fn url_health_record_failure() {
        let mut h = UrlHealth::default();
        h.record_failure();
        assert_eq!(h.download_attempts(), 1);
        assert!(h.last_attempt.is_some());
    }
}
