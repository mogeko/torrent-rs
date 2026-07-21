//! Centralized work dispatch scheduler and gap-finding utilities.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, RwLock, mpsc};

use crate::error::ErrorKind;
use crate::metainfo::Metainfo;
use crate::net::Url;
use crate::piece::PieceManager;

use super::fetcher::file_path_at_byte;
use super::types::{UrlActivity, UrlKind, UrlState, WebSeedConfig, WorkItem, WorkResult};

// ── WebSeedScheduler ──────────────────────────────────────────────

/// Exploration weight for the UCB bandit formula (bytes/sec).
///
/// Controls how aggressively untested URLs compete against
/// proven-fast URLs.  At 100 KB/s, an untested URL with 0
/// attempts scores ~214 KB/s against a pool of 100 total
/// attempts — enough to beat marginal URLs but not to
/// displace a proven 1 MB/s URL.
const UCB_EXPLORATION_FACTOR: f64 = 100_000.0;

/// Centralized scheduler for web seed downloads (Phase 2).
///
/// Reads the piece bitfield, selects the largest gap, picks the
/// best URL by UCB-weighted throughput score, and dispatches
/// [`WorkItem`]s to [`FetchTask`]s via mpsc channels.
///
/// # Known limitation: index shift after hash-mismatch removal
///
/// When a URL is permanently discarded via
/// [`handle_result`], its [`WorkResult::url_index`]-based
/// identification may become stale for in-flight fetchers of
/// URLs at higher indices.  This is rare (multiple hash
/// mismatches) and non-catastrophic (the guard in
/// [`handle_result`] bounds-checks the index).
pub(crate) struct WebSeedScheduler {
    urls: Vec<UrlState>,
    piece_mgr: Arc<RwLock<PieceManager>>,
    metainfo: Metainfo,
    config: WebSeedConfig,
    result_rx: mpsc::Receiver<WorkResult>,
    notify: Arc<Notify>,
}

impl WebSeedScheduler {
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

    /// Run the scheduler loop: optimistic dispatch → handle → repeat.
    ///
    /// All URLs start [`UrlActivity::Active`] — the scheduler
    /// dispatches download work immediately.  Dead URLs are caught
    /// by [`handle_result`] and parked after `park_threshold`
    /// consecutive failures.  Parked URLs are automatically
    /// reconsidered by [`dispatch_work`] once
    /// [`UrlHealth::ready_for_retry`] returns `true`.
    pub async fn run(mut self) {
        tracing::info!(
            "web seed: {} URLs, max {} concurrent downloads",
            self.urls.len(),
            self.config.max_concurrent,
        );

        let mut dispatch_tick = tokio::time::interval(Duration::from_secs(1));

        loop {
            {
                let pm = self.piece_mgr.read().await;
                if pm.bitfield().iter().all(|&b| b) {
                    tracing::info!("web seed: download complete, exiting");
                    return;
                }
            }

            tokio::select! {
                Some(result) = self.result_rx.recv() => {
                    self.handle_result(result).await;
                    self.dispatch_work().await;
                }
                _ = dispatch_tick.tick() => {
                    self.dispatch_work().await;
                }
                _ = self.notify.notified() => {
                    self.dispatch_work().await;
                }
            }
        }
    }

    async fn handle_result(&mut self, result: WorkResult) {
        let idx = result.url_index;
        let Some(state) = self.urls.get_mut(idx) else {
            return;
        };

        match result.error {
            None => {
                let throughput = result.bytes as f64 / result.elapsed.as_secs_f64().max(0.001);
                tracing::debug!(
                    "web seed {}: {} pieces ({:.1} KB) in {:.1}s ({:.1} KB/s)",
                    state.url,
                    result.completed.len(),
                    result.bytes as f64 / 1024.0,
                    result.elapsed.as_secs_f64(),
                    throughput / 1024.0,
                );
                state.health.record_success(result.bytes, result.elapsed);
                state.activity = UrlActivity::Active;
            }
            Some(ErrorKind::WebSeedHashMismatch) => {
                tracing::warn!(
                    "web seed {}: SHA-1 mismatch, discarding permanently",
                    state.url,
                );
                self.urls.remove(idx);
            }
            Some(_) => {
                state.health.record_failure();
                if state.health.should_park(self.config.park_threshold) {
                    tracing::warn!(
                        "web seed {}: {} consecutive failures, parking for {}s",
                        state.url,
                        state.health.consecutive_failures,
                        self.config.park_retry_interval.as_secs(),
                    );
                    state.activity = UrlActivity::Parked;
                } else {
                    tracing::debug!(
                        "web seed {}: download failed ({}/{} consecutive)",
                        state.url,
                        state.health.consecutive_failures,
                        self.config.park_threshold,
                    );
                    state.activity = UrlActivity::Active;
                }
            }
        }
    }

    /// Dispatch work to fill all available download slots.
    ///
    /// Reads the bitfield once, then loops: find a gap, pick the best
    /// available URL, send a [`WorkItem`], and mark the gap as taken
    /// in a local copy of the bitfield to avoid re-dispatching the
    /// same range to another URL.  Stops when no more slots, gaps, or
    /// eligible URLs are available.
    async fn dispatch_work(&mut self) {
        let mut bitfield = {
            let pm = self.piece_mgr.read().await;
            pm.bitfield().to_vec()
        };

        let piece_length = self.metainfo.info.piece_length;
        let min_gap = self.config.min_gap_pieces;
        let total_size = self.metainfo.info.total_size();
        let max_range = self.config.max_range_bytes;

        loop {
            let in_flight = self
                .urls
                .iter()
                .filter(|s| s.activity == UrlActivity::InFlight)
                .count();
            if in_flight >= self.config.max_concurrent {
                return;
            }

            let gap = find_largest_gap(&bitfield)
                .or_else(|| gap_within_file(&bitfield, &self.metainfo, piece_length, min_gap));

            let Some((gap_start, _gap_size)) = gap else {
                return;
            };

            let start_byte = gap_start as u64 * piece_length;

            // Compute total download attempts across all URLs for the
            // UCB exploration bonus.  Cached once per dispatch batch
            // to avoid repeated summation.
            let total_attempts: u64 = self.urls.iter().map(|s| s.health.download_attempts()).sum();

            let best_idx = self
                .urls
                .iter()
                .enumerate()
                .filter(|(_, s)| {
                    let eligible = match s.activity {
                        UrlActivity::Active => true,
                        UrlActivity::Parked => {
                            s.health.ready_for_retry(self.config.park_retry_interval)
                        }
                        UrlActivity::InFlight => false,
                    };
                    if !eligible || s.work_tx.as_ref().is_none_or(|tx| tx.is_closed()) {
                        return false;
                    }
                    if s.url_kind == UrlKind::Directory {
                        file_path_at_byte(&self.metainfo, start_byte).is_some()
                    } else {
                        true
                    }
                })
                .max_by(|(_, a), (_, b)| {
                    let a_prio = u8::from(a.url_kind == UrlKind::Script);
                    let b_prio = u8::from(b.url_kind == UrlKind::Script);
                    a_prio
                        .cmp(&b_prio)
                        .then_with(|| {
                            // UCB-weighted score: EMA throughput + exploration bonus.
                            // Untested URLs (download_attempts=0) get a large bonus,
                            // ensuring they compete against proven URLs.  As attempts
                            // accumulate, the bonus shrinks and EMA dominates.
                            a.health
                                .ucb_score(total_attempts, UCB_EXPLORATION_FACTOR)
                                .partial_cmp(
                                    &b.health.ucb_score(total_attempts, UCB_EXPLORATION_FACTOR),
                                )
                                .unwrap_or(std::cmp::Ordering::Equal)
                        })
                        .then_with(|| {
                            b.health
                                .consecutive_failures
                                .cmp(&a.health.consecutive_failures)
                        })
                });

            let Some((idx, _state)) = best_idx else {
                return;
            };
            // Adaptive range: use the full max_range_bytes.  Slow URLs
            // naturally finish fewer ranges per dispatch cycle.
            let floor_range = min_gap as u64 * piece_length;
            let adaptive_max = floor_range.max(max_range);
            let end_byte = (start_byte + adaptive_max)
                .min(total_size)
                .saturating_sub(1);
            let range_mb = (end_byte - start_byte + 1) as f64 / (1024.0 * 1024.0);
            let ema_kbs = self.urls[idx].health.ema_throughput() / 1024.0;
            let ucb_kbs = self.urls[idx]
                .health
                .ucb_score(total_attempts, UCB_EXPLORATION_FACTOR)
                / 1024.0;

            let revival = matches!(self.urls[idx].activity, UrlActivity::Parked);
            if revival {
                let elapsed = self.urls[idx].health.ready_for_retry_elapsed();
                tracing::info!(
                    "web seed {}: revived after {:.0}s parked",
                    self.urls[idx].url,
                    elapsed,
                );
            }

            tracing::debug!(
                "web seed dispatch → {} [{:.1}MB] (EMA={:.0}KB/s, UCB={:.0}KB/s)",
                self.urls[idx].url,
                range_mb,
                ema_kbs,
                ucb_kbs,
            );

            if self.urls[idx]
                .work_tx
                .as_ref()
                .and_then(|tx| {
                    tx.try_send(WorkItem {
                        start_byte,
                        end_byte,
                    })
                    .ok()
                })
                .is_some()
            {
                self.urls[idx].activity = UrlActivity::InFlight;

                // Mark the dispatched range as "taken" in our local
                // bitfield copy so the next loop iteration finds a
                // different gap.
                let first_piece = gap_start;
                let last_piece =
                    ((end_byte / piece_length) as u32).min(bitfield.len().saturating_sub(1) as u32);
                for p in first_piece..=last_piece {
                    if (p as usize) < bitfield.len() {
                        bitfield[p as usize] = true;
                    }
                }
            }
            // If try_send failed, the URL's channel is full (cap=1).
            // Continue the loop to try the next-best URL.
        }
    }
}

// ── URL deduplication ─────────────────────────────────────────────

/// Deduplicate URLs that resolve to the same origin (host:port).
///
/// For torrents with hundreds of mirror URLs, many point to the same
/// physical server. URLs with the same `(host, port)` are collapsed
/// to the first occurrence.
///
/// Returns `(deduplicated_urls, removed_count)`.
pub(crate) fn deduplicate_urls(urls: Vec<Url>) -> (Vec<Url>, usize) {
    let total = urls.len();
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::with_capacity(total);

    for url in urls {
        let key = (
            url.host_str().map(|h| h.to_ascii_lowercase()),
            url.port_or_known_default(),
        );
        if seen.insert(key) {
            result.push(url);
        }
    }
    let removed = total - result.len();
    (result, removed)
}

// ── Gap-finding utilities ─────────────────────────────────────────

/// Find the largest contiguous gap of missing pieces in a bitfield.
///
/// Returns `(gap_start_index, gap_size_in_pieces)`, or `None` if
/// no gaps exist (all pieces present).
fn find_largest_gap(bitfield: &[bool]) -> Option<(u32, u32)> {
    let (mut best_start, mut best_size) = (None, 0u32);
    let (mut gap_start, mut gap_size) = (None, 0u32);

    for (i, &has) in bitfield.iter().enumerate() {
        if !has {
            if gap_start.is_none() {
                gap_start = Some(i as u32);
            }
            gap_size += 1;
        } else if let Some(start) = gap_start {
            if gap_size > best_size {
                best_start = Some(start);
                best_size = gap_size;
            }
            gap_start = None;
            gap_size = 0;
        }
    }

    if let Some(start) = gap_start {
        if gap_size > best_size {
            best_start = Some(start);
            best_size = gap_size;
        }
    }

    best_start.map(|s| (s, best_size))
}

/// Find the largest contiguous gap within a single file's piece range.
///
/// Unlike [`find_largest_gap`], this restricts the gap to pieces that
/// fall entirely within one file. Used for directory-style web seed
/// URLs where each HTTP request targets a single file.
fn gap_within_file(
    bitfield: &[bool], metainfo: &Metainfo, piece_length: u64, min_gap_pieces: u32,
) -> Option<(u32, u32)> {
    let offsets = metainfo.info.file_offsets();
    let mut best: Option<(u32, u32)> = None;

    for fo in &offsets {
        let first_piece = (fo.offset / piece_length) as u32;
        let last_piece = ((fo.offset + fo.length).saturating_sub(1) / piece_length) as u32;
        let last_piece = last_piece.min(bitfield.len().saturating_sub(1) as u32);

        let (mut gap_start, mut gap_size) = (None, 0u32);
        for idx in first_piece..=last_piece {
            if !bitfield[idx as usize] {
                if gap_start.is_none() {
                    gap_start = Some(idx);
                }
                gap_size += 1;
            } else if let Some(start) = gap_start {
                if gap_size > best.map(|(_, s)| s).unwrap_or(0) && gap_size >= min_gap_pieces {
                    best = Some((start, gap_size));
                }
                gap_start = None;
                gap_size = 0;
            }
        }
        if let Some(start) = gap_start {
            if gap_size > best.map(|(_, s)| s).unwrap_or(0) && gap_size >= min_gap_pieces {
                best = Some((start, gap_size));
            }
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use crate::metainfo::{FileInfo, Mode, RawInfo};

    use super::*;

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

    // ── gap_within_file tests ────────────────────────────────────

    fn make_multi_file_info() -> crate::metainfo::Info {
        crate::metainfo::Info {
            piece_length: 100,
            pieces: vec![[0u8; 20]; 10],
            mode: Mode::Multiple {
                name: "root".into(),
                files: vec![
                    FileInfo {
                        length: 300,
                        path: vec!["a.txt".into()],
                    },
                    FileInfo {
                        length: 400,
                        path: vec!["b.txt".into()],
                    },
                    FileInfo {
                        length: 300,
                        path: vec!["c.txt".into()],
                    },
                ],
            },
            raw_info: RawInfo::Hash([0u8; 20]),
        }
    }

    fn metainfo_from_info(info: crate::metainfo::Info) -> Metainfo {
        Metainfo {
            announce: String::new(),
            announce_list: vec![],
            info,
            url_list: vec![],
            httpseeds: vec![],
            creation_date: None,
            comment: None,
            created_by: None,
            encoding: None,
        }
    }

    #[test]
    fn gap_within_file_finds_file_local_gap() {
        let metainfo = metainfo_from_info(make_multi_file_info());
        let bf = vec![false; 10];
        let gap = gap_within_file(&bf, &metainfo, 100, 1);
        assert!(gap.is_some());
        let (_, size) = gap.unwrap();
        assert!(size >= 3);
    }

    #[test]
    fn gap_within_file_respects_have() {
        let metainfo = metainfo_from_info(make_multi_file_info());
        let bf = vec![
            true, true, true, true, false, false, true, false, false, false,
        ];
        let gap = gap_within_file(&bf, &metainfo, 100, 2);
        assert!(gap.is_some());
        let (start, size) = gap.unwrap();
        assert_eq!(start, 7);
        assert_eq!(size, 3);
    }

    // ── deduplicate_urls tests ───────────────────────────────────

    #[test]
    fn dedup_removes_same_origin() {
        let urls = vec![
            Url::parse("http://mirror1.example.com/file.iso").unwrap(),
            Url::parse("http://mirror1.example.com/file.iso").unwrap(),
            Url::parse("http://mirror2.example.com/file.iso").unwrap(),
        ];
        let (result, removed) = deduplicate_urls(urls);
        assert_eq!(removed, 1);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn dedup_preserves_different_ports() {
        let urls = vec![
            Url::parse("http://example.com:8080/file").unwrap(),
            Url::parse("http://example.com:9090/file").unwrap(),
        ];
        let (result, removed) = deduplicate_urls(urls);
        assert_eq!(removed, 0);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn dedup_preserves_different_schemes() {
        let urls = vec![
            Url::parse("http://example.com/file").unwrap(),
            Url::parse("https://example.com/file").unwrap(),
        ];
        let (result, removed) = deduplicate_urls(urls);
        assert_eq!(removed, 0);
        assert_eq!(result.len(), 2);
    }
}
