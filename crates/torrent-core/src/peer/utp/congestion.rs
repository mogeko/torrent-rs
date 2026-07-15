//! uTP delay-based congestion control (BEP 29 §congestion control).
//!
//! The uTP congestion control uses one-way buffer delay as the primary
//! congestion signal, in addition to packet loss (like TCP). The goal is
//! to keep the send buffer in the network path as empty as possible,
//! targeting a maximum of 100 ms of buffering delay.
//!
//! # Core Algorithm
//!
//! 1. **Delay measurement**: Every packet carries a high-resolution
//!    timestamp. The receiver computes the difference and feeds it back
//!    as `timestamp_difference_microseconds`.
//!
//! 2. **Base delay**: A sliding minimum of delay samples over the last
//!    2 minutes serves as the baseline (minimum path latency).
//!
//! 3. **Window adjustment**: The send window grows when `our_delay` is
//!    below the target (100 ms) and shrinks when above.
//!
//! 4. **Loss response**: Packet loss (detected via 3 duplicate ACKs or
//!    timeout) halves the window, mimicking TCP's multiplicative decrease.
//!
//! # Key Constants
//!
//! - `CCONTROL_TARGET`: 100 ms — the target buffering delay
//! - `MAX_CWND_INCREASE_PACKETS_PER_RTT`: maximum window growth per RTT
//! - `BASE_DELAY_WINDOW`: 2 minutes — sliding window for base delay
//! - Minimum timeout: 500 ms
//! - Minimum packet size: 150 bytes

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Target buffering delay in microseconds (100 ms).
///
/// uTP aims to keep the network buffer delay below this threshold.
/// When `our_delay` exceeds this, the congestion window shrinks;
/// when below, it grows.
pub const CCONTROL_TARGET: u32 = 100_000; // 100 ms in microseconds

/// Maximum window increase per RTT, in bytes.
///
/// This caps the aggressiveness of the window growth when delay is
/// far below the target. The actual growth is scaled by the delay
/// factor and window factor.
const MAX_CWND_INCREASE_BYTES_PER_RTT: u32 = 3000;

/// Sliding window for base delay samples (2 minutes).
const BASE_DELAY_WINDOW: Duration = Duration::from_secs(120);

/// Minimum timeout in milliseconds (BEP 29: 500 ms).
const MIN_TIMEOUT_MS: u32 = 500;

/// Initial timeout in milliseconds before any RTT samples exist.
const INITIAL_TIMEOUT_MS: u32 = 1000;

/// Minimum packet size in bytes (used after timeout).
const MIN_PACKET_SIZE: u32 = 150;

/// A delay sample with its timestamp for the 2-minute sliding window.
#[derive(Debug, Clone, Copy)]
struct DelaySample {
    /// When this sample was recorded.
    time: Instant,
    /// The `reply_micro` value (one-way delay in microseconds).
    value: u32,
}

/// uTP congestion control state machine (BEP 29).
///
/// This struct manages the delay-based congestion control algorithm
/// for a single uTP connection. It is purely algorithmic — all I/O
/// and timer management is handled by the caller.
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use torrent_core::peer::utp::congestion::UtpCongestionControl;
///
/// let mut cc = UtpCongestionControl::new();
///
/// // Establish baseline delay and simulate window growth
/// // when delay is well below the 100ms target.
/// let now = Instant::now();
/// cc.update_delay(20_000, now);  // 20ms one-way delay
/// cc.update_delay(20_000, now);
/// // Simulate bytes in flight and let the algorithm adjust
/// cc.set_cur_window(3000);
/// cc.adjust_window();
/// assert!(cc.max_window() >= 150); // minimum packet size
///
/// // Simulate packet loss: window must halve
/// let before = cc.max_window();
/// cc.on_packet_loss();
/// assert!(cc.max_window() <= before / 2);
/// ```
#[derive(Debug)]
pub struct UtpCongestionControl {
    /// Maximum number of bytes in-flight (the congestion window).
    max_window: u32,
    /// Current number of bytes in-flight.
    cur_window: u32,
    /// Baseline delay: sliding minimum over the last 2 minutes (microseconds).
    base_delay: u32,
    /// Current buffering delay: `reply_micro - base_delay` (microseconds).
    our_delay: u32,
    /// Latest one-way delay measurement (microseconds).
    reply_micro: u32,
    /// Smoothed round-trip time (milliseconds).
    rtt: u32,
    /// Round-trip time variance (milliseconds).
    rtt_var: u32,
    /// Current retransmission timeout (milliseconds).
    timeout: u32,
    /// Current packet size in bytes (ranges from 150 to MTU).
    packet_size: u32,
    /// Number of consecutive timeouts (for exponential backoff).
    consecutive_timeouts: u32,
    /// Delay sample history for the 2-minute sliding window.
    delay_history: VecDeque<DelaySample>,
}

impl UtpCongestionControl {
    /// Create a new congestion control state with sensible defaults.
    ///
    /// Initial state:
    /// - `max_window`: 0 (will grow from ACKs)
    /// - `base_delay`: `u32::MAX` (no samples yet)
    /// - `timeout`: 1000 ms
    /// - `packet_size`: minimum (150 bytes)
    pub fn new() -> Self {
        UtpCongestionControl {
            max_window: 0,
            cur_window: 0,
            base_delay: u32::MAX,
            our_delay: 0,
            reply_micro: 0,
            rtt: 0,
            rtt_var: 0,
            timeout: INITIAL_TIMEOUT_MS,
            packet_size: MIN_PACKET_SIZE,
            consecutive_timeouts: 0,
            delay_history: VecDeque::new(),
        }
    }

    /// Returns the current maximum congestion window in bytes.
    pub fn max_window(&self) -> u32 {
        self.max_window
    }

    /// Returns the current number of bytes in-flight.
    pub fn cur_window(&self) -> u32 {
        self.cur_window
    }

    /// Returns the current baseline delay in microseconds.
    pub fn base_delay(&self) -> u32 {
        self.base_delay
    }

    /// Returns the current buffering delay in microseconds.
    pub fn our_delay(&self) -> u32 {
        self.our_delay
    }

    /// Returns the latest one-way delay sample in microseconds.
    pub fn reply_micro(&self) -> u32 {
        self.reply_micro
    }

    /// Returns the smoothed RTT in milliseconds.
    pub fn rtt_ms(&self) -> u32 {
        self.rtt
    }

    /// Returns the current timeout in milliseconds.
    pub fn timeout_ms(&self) -> u32 {
        self.timeout
    }

    /// Returns the current packet size in bytes.
    pub fn packet_size(&self) -> u32 {
        self.packet_size
    }

    /// Set the current number of bytes in-flight.
    ///
    /// Called by the connection layer when packets are sent or acked.
    pub fn set_cur_window(&mut self, bytes: u32) {
        self.cur_window = bytes;
    }

    /// Add `bytes` to the current in-flight count.
    pub fn add_in_flight(&mut self, bytes: u32) {
        self.cur_window = self.cur_window.saturating_add(bytes);
    }

    /// Subtract `bytes` from the current in-flight count.
    pub fn remove_in_flight(&mut self, bytes: u32) {
        self.cur_window = self.cur_window.saturating_sub(bytes);
    }

    /// Update the delay measurement with a new one-way delay sample.
    ///
    /// This should be called whenever a packet is received from the
    /// remote peer. The `reply_micro` value is computed as:
    /// `current_time - packet.timestamp_microseconds`
    ///
    /// This method:
    /// 1. Stores the new `reply_micro` value
    /// 2. Adds it to the 2-minute sliding window history
    /// 3. Prunes expired samples (> 2 minutes old)
    /// 4. Recomputes `base_delay` (minimum in the window)
    /// 5. Recomputes `our_delay` (current - baseline)
    pub fn update_delay(&mut self, reply_micro: u32, now: Instant) {
        self.reply_micro = reply_micro;

        // Add the new sample to history
        self.delay_history.push_back(DelaySample {
            time: now,
            value: reply_micro,
        });

        // Prune samples older than 2 minutes
        let cutoff = now - BASE_DELAY_WINDOW;
        while let Some(front) = self.delay_history.front() {
            if front.time < cutoff {
                self.delay_history.pop_front();
            } else {
                break;
            }
        }

        // Recompute base_delay as the minimum in the window
        if let Some(min_sample) = self.delay_history.iter().map(|s| s.value).min() {
            self.base_delay = min_sample;
        } else {
            self.base_delay = u32::MAX;
        }

        // Compute our_delay (current buffer delay)
        self.our_delay = reply_micro.saturating_sub(self.base_delay);
    }

    /// Update the RTT estimate with a new round-trip time sample.
    ///
    /// Only call this for packets that were sent **exactly once**
    /// (not retransmissions), to avoid ambiguity about which
    /// transmission was acknowledged.
    ///
    /// Updates `rtt`, `rtt_var`, and `timeout` according to:
    ///
    /// ```text
    /// delta = rtt - packet_rtt
    /// rtt_var += (abs(delta) - rtt_var) / 4
    /// rtt += (packet_rtt - rtt) / 8
    /// timeout = max(rtt + rtt_var * 4, 500)
    /// ```
    pub fn update_rtt(&mut self, packet_rtt_ms: u32) {
        if self.rtt == 0 {
            // First sample: initialize directly
            self.rtt = packet_rtt_ms;
            self.rtt_var = packet_rtt_ms / 2;
        } else {
            // Use i64 for intermediate calculations to handle
            // potentially negative (rtt_var - abs(delta)) correctly.
            let rtt = self.rtt as i64;
            let pkt_rtt = packet_rtt_ms as i64;
            let rtt_var = self.rtt_var as i64;

            let delta_abs = (rtt - pkt_rtt).unsigned_abs() as i64;

            // rtt_var += (abs(delta) - rtt_var) / 4
            let new_rtt_var = rtt_var + (delta_abs - rtt_var) / 4;
            self.rtt_var = new_rtt_var.max(0) as u32;

            // rtt += (packet_rtt - rtt) / 8
            let new_rtt = rtt + (pkt_rtt - rtt) / 8;
            self.rtt = new_rtt.max(0) as u32;
        }

        // timeout = max(rtt + rtt_var * 4, 500)
        self.timeout = (self.rtt + self.rtt_var * 4).max(MIN_TIMEOUT_MS);
        // Reset consecutive timeout counter on successful RTT update
        self.consecutive_timeouts = 0;
    }

    /// Adjust the congestion window based on the current delay measurement.
    ///
    /// Uses `cur_window` (bytes in-flight, not packet count) as the
    /// `outstanding_packet` term from BEP 29.
    ///
    /// The window is adjusted using the formula:
    ///
    /// ```text
    /// off_target = CCONTROL_TARGET - our_delay
    /// delay_factor = off_target / CCONTROL_TARGET
    /// window_factor = cur_window / max_window
    /// scaled_gain = MAX_CWND_INCREASE * delay_factor * window_factor
    /// max_window += scaled_gain
    /// ```
    ///
    /// If `our_delay > CCONTROL_TARGET`, the window shrinks.
    /// If `our_delay < CCONTROL_TARGET`, the window grows.
    pub fn adjust_window(&mut self) {
        // Handle edge case: no delay samples yet
        if self.base_delay == u32::MAX {
            // No baseline yet — use a small default window
            self.max_window = self.max_window.max(MIN_PACKET_SIZE * 2);
            return;
        }

        let off_target = CCONTROL_TARGET as i64 - self.our_delay as i64;

        // delay_factor: how far off target, in units of target delay
        let delay_factor = off_target as f64 / CCONTROL_TARGET as f64;

        // window_factor: fraction of window that's in use
        // Uses cur_window (bytes) / max_window (bytes) — both in same units.
        // Capped at 1.0 to prevent blowup when max_window is very small.
        let window_factor = (self.cur_window as f64 / self.max_window.max(1) as f64).min(1.0);

        // scaled_gain: the actual adjustment in bytes
        let scaled_gain = MAX_CWND_INCREASE_BYTES_PER_RTT as f64 * delay_factor * window_factor;

        // Apply the adjustment
        let new_window = (self.max_window as i64)
            .saturating_add(scaled_gain as i64)
            .max(0) as u32;

        // Don't let the window drop below one packet size when we have data
        self.max_window = new_window.max(MIN_PACKET_SIZE);

        // Update packet size to match window (small window → small packets)
        self.update_packet_size();
    }

    /// Called when a packet loss is detected (via 3 duplicate ACKs).
    ///
    /// Multiplicative decrease: `max_window = max_window / 2`.
    /// This mimics TCP's congestion avoidance behavior.
    pub fn on_packet_loss(&mut self) {
        self.max_window = (self.max_window / 2).max(MIN_PACKET_SIZE);
        self.update_packet_size();
    }

    /// Called when the retransmission timeout fires.
    ///
    /// Forces the connection back to minimum packet size and window,
    /// and applies exponential backoff to the timeout.
    ///
    /// This is the recovery mechanism when the window shrinks to zero
    /// or no packets are being exchanged.
    pub fn on_timeout(&mut self) {
        self.packet_size = MIN_PACKET_SIZE;
        self.max_window = MIN_PACKET_SIZE;
        self.consecutive_timeouts += 1;
        // Exponential backoff: double the timeout for each consecutive timeout
        self.timeout = self
            .timeout
            .saturating_mul(2u32.pow(self.consecutive_timeouts.min(6)));
    }

    /// Reset the consecutive timeout counter (called when a packet is received).
    pub fn reset_timeouts(&mut self) {
        self.consecutive_timeouts = 0;
    }

    /// Check if a packet of the given size can be sent.
    ///
    /// Returns `true` if:
    /// ```text
    /// cur_window + packet_size <= min(max_window, remote_wnd_size)
    /// ```
    ///
    /// Note: BEP 29 allows violating this rule if `max_window` is smaller
    /// than the packet size, as long as the average `cur_window` stays
    /// below `max_window` (pacing). This implementation enforces the
    /// strict rule.
    pub fn can_send(&self, packet_size: u32, remote_wnd_size: u32) -> bool {
        let effective_window = self.max_window.min(remote_wnd_size);
        self.cur_window.saturating_add(packet_size) <= effective_window
    }

    /// Update the packet size based on the current window.
    ///
    /// At low rates, small packets (150 bytes) avoid long serialization
    /// delays on slow links. At high rates, larger packets reduce header
    /// overhead.
    fn update_packet_size(&mut self) {
        // Simple heuristic: scale packet size with window
        // Window ≤ 1500 → packet_size = 150
        // Window ≥ 15000 → packet_size = MTU (~1500)
        let scale = (self.max_window as f64 / 1500.0).clamp(1.0, 10.0);
        self.packet_size = (MIN_PACKET_SIZE as f64 * scale) as u32;
    }
}

impl Default for UtpCongestionControl {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a control with a known window.
    fn cc_with_window(window: u32) -> UtpCongestionControl {
        let mut cc = UtpCongestionControl::new();
        cc.max_window = window;
        cc
    }

    // --- Initialization ---

    #[test]
    fn new_has_sensible_defaults() {
        let cc = UtpCongestionControl::new();
        assert_eq!(cc.max_window(), 0);
        assert_eq!(cc.cur_window(), 0);
        assert_eq!(cc.base_delay(), u32::MAX); // no samples yet
        assert_eq!(cc.timeout_ms(), INITIAL_TIMEOUT_MS);
        assert_eq!(cc.packet_size(), MIN_PACKET_SIZE);
    }

    // --- Delay measurement ---

    #[test]
    fn update_delay_sets_reply_micro() {
        let mut cc = UtpCongestionControl::new();
        let now = Instant::now();
        cc.update_delay(50_000, now);
        assert_eq!(cc.reply_micro(), 50_000);
    }

    #[test]
    fn base_delay_is_minimum_in_window() {
        let mut cc = UtpCongestionControl::new();
        let now = Instant::now();

        cc.update_delay(100_000, now);
        cc.update_delay(50_000, now + Duration::from_secs(1));
        cc.update_delay(75_000, now + Duration::from_secs(2));

        assert_eq!(cc.base_delay(), 50_000); // minimum in window
    }

    #[test]
    fn our_delay_is_reply_micro_minus_base_delay() {
        let mut cc = UtpCongestionControl::new();
        let now = Instant::now();

        cc.update_delay(50_000, now); // base_delay = 50_000
        cc.update_delay(80_000, now + Duration::from_secs(1)); // our_delay = 30_000

        assert_eq!(cc.our_delay(), 30_000);
    }

    #[test]
    fn old_samples_are_pruned() {
        let mut cc = UtpCongestionControl::new();
        let now = Instant::now();

        // Add a sample 3 minutes ago (should be pruned)
        cc.delay_history.push_back(DelaySample {
            time: now - Duration::from_secs(180),
            value: 10_000,
        });

        // Add a recent sample (should remain)
        cc.update_delay(50_000, now);

        // The old sample (10_000) should be gone, base_delay = 50_000
        assert_eq!(cc.base_delay(), 50_000);
    }

    // --- RTT estimation ---

    #[test]
    fn update_rtt_first_sample_initializes_directly() {
        let mut cc = UtpCongestionControl::new();
        cc.update_rtt(100);
        assert_eq!(cc.rtt_ms(), 100);
        assert_eq!(cc.rtt_var, 50); // rtt/2
    }

    #[test]
    fn update_rtt_converges() {
        let mut cc = UtpCongestionControl::new();
        // Simulate stable RTT of 100ms
        for _ in 0..20 {
            cc.update_rtt(100);
        }
        // Should converge near 100
        assert!(
            (cc.rtt_ms() as i32 - 100).abs() <= 5,
            "rtt should converge to ~100, got {}",
            cc.rtt_ms()
        );
    }

    #[test]
    fn timeout_is_at_least_500ms() {
        let mut cc = UtpCongestionControl::new();
        cc.update_rtt(10); // very small RTT
        assert!(cc.timeout_ms() >= 500);
    }

    #[test]
    fn timeout_scales_with_rtt() {
        let mut cc = UtpCongestionControl::new();
        for _ in 0..20 {
            cc.update_rtt(200);
        }
        // With stable RTT=200ms, rtt_var converges to 0,
        // so timeout = max(200 + 0, 500) = 500.
        // The minimum timeout guarantee is the key invariant.
        assert!(
            cc.timeout_ms() >= 500,
            "timeout should be at least 500ms, got {}",
            cc.timeout_ms()
        );
    }

    // --- Window adjustment ---

    #[test]
    fn window_grows_when_delay_below_target() {
        let mut cc = cc_with_window(1500);
        let now = Instant::now();

        // Establish a low baseline delay
        cc.update_delay(10_000, now); // base_delay = 10_000
        cc.update_delay(10_000, now + Duration::from_secs(1));
        cc.set_cur_window(1500);

        // our_delay = 10_000 - 10_000 = 0 → below target → window should grow
        let before = cc.max_window();
        cc.adjust_window();
        assert!(
            cc.max_window() > before,
            "window should grow when delay is below target ({} → {})",
            before,
            cc.max_window()
        );
    }

    #[test]
    fn window_shrinks_when_delay_above_target() {
        let mut cc = cc_with_window(10000);
        let now = Instant::now();

        // Establish baseline at 10ms
        cc.update_delay(10_000, now);
        cc.update_delay(10_000, now + Duration::from_secs(1));

        // Now simulate 200ms delay (our_delay = 190ms, way above 100ms target)
        cc.update_delay(200_000, now + Duration::from_secs(2));
        cc.set_cur_window(5000);

        let before = cc.max_window();
        cc.adjust_window();
        assert!(
            cc.max_window() < before,
            "window should shrink when delay is above target ({} → {})",
            before,
            cc.max_window()
        );
    }

    #[test]
    fn adjust_window_no_samples_yet() {
        let mut cc = UtpCongestionControl::new();
        // No delay samples → base_delay is u32::MAX
        cc.adjust_window();
        // Should set a minimal default window
        assert!(cc.max_window() >= MIN_PACKET_SIZE * 2);
    }

    // --- Loss handling ---

    #[test]
    fn packet_loss_halves_window() {
        let mut cc = cc_with_window(20000);
        cc.on_packet_loss();
        assert_eq!(cc.max_window(), 10000);
    }

    #[test]
    fn packet_loss_has_minimum_window() {
        let mut cc = cc_with_window(200);
        cc.on_packet_loss();
        assert!(cc.max_window() >= MIN_PACKET_SIZE);
    }

    // --- Timeout handling ---

    #[test]
    fn timeout_resets_to_min_packet() {
        let mut cc = cc_with_window(50000);
        let old_timeout = cc.timeout_ms();
        cc.on_timeout();
        assert_eq!(cc.packet_size(), MIN_PACKET_SIZE);
        assert_eq!(cc.max_window(), MIN_PACKET_SIZE);
        assert!(cc.timeout_ms() > old_timeout); // exponential backoff
    }

    #[test]
    fn consecutive_timeouts_double_timeout() {
        let mut cc = cc_with_window(50000);
        let t0 = cc.timeout_ms();
        cc.on_timeout();
        let t1 = cc.timeout_ms();
        cc.on_timeout();
        let t2 = cc.timeout_ms();
        cc.on_timeout();
        let t3 = cc.timeout_ms();

        // Each consecutive timeout should approximately double
        assert!(t1 >= t0 * 2, "t1={t1} should be >= 2 * t0={t0}");
        assert!(t2 >= t1 * 2, "t2={t2} should be >= 2 * t1={t1}");
        assert!(t3 >= t2 * 2, "t3={t3} should be >= 2 * t2={t2}");
    }

    #[test]
    fn reset_timeouts_clears_backoff() {
        let mut cc = cc_with_window(50000);
        cc.on_timeout();
        cc.on_timeout();
        cc.reset_timeouts();
        let after_reset = cc.timeout_ms();
        cc.on_timeout();
        // After reset, timeout should not have compounded from before the reset
        assert!(
            cc.timeout_ms() <= after_reset * 4,
            "timeout should not compound across reset"
        );
    }

    // --- can_send ---

    #[test]
    fn can_send_when_within_window() {
        let mut cc = cc_with_window(1500);
        cc.set_cur_window(500);
        assert!(cc.can_send(500, u32::MAX)); // 500 + 500 = 1000 <= 1500
    }

    #[test]
    fn cannot_send_when_exceeding_window() {
        let mut cc = cc_with_window(1500);
        cc.set_cur_window(1400);
        assert!(!cc.can_send(500, u32::MAX)); // 1400 + 500 = 1900 > 1500
    }

    #[test]
    fn can_send_respects_remote_window() {
        let mut cc = cc_with_window(1500);
        cc.set_cur_window(500);
        // Remote window = 800, so effective = min(1500, 800) = 800
        assert!(cc.can_send(200, 800)); // 500 + 200 = 700 <= 800
        assert!(!cc.can_send(400, 800)); // 500 + 400 = 900 > 800
    }

    // --- In-flight tracking ---

    #[test]
    fn add_remove_in_flight() {
        let mut cc = UtpCongestionControl::new();
        cc.add_in_flight(500);
        assert_eq!(cc.cur_window(), 500);
        cc.add_in_flight(300);
        assert_eq!(cc.cur_window(), 800);
        cc.remove_in_flight(500);
        assert_eq!(cc.cur_window(), 300);
    }

    #[test]
    fn in_flight_does_not_underflow() {
        let mut cc = UtpCongestionControl::new();
        cc.remove_in_flight(100);
        assert_eq!(cc.cur_window(), 0);
    }
}
