//! A small rolling-window throughput + ETA estimator, shared by the GUI
//! progress bar and the CLI's progress lines.
//!
//! Drop one next to a progress counter, call [`RateTracker::record`] each tick
//! with the running byte total and a stage label (any string that changes when
//! the work moves to a new phase), then read [`RateTracker::rate_bytes_per_sec`]
//! / [`RateTracker::eta_secs`] / [`RateTracker::suffix`].

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Rolling-window rate + ETA tracker. The window is ~10s so the ETA stays
/// responsive to stalls without flapping each tick; a stage change or a
/// regressing byte counter clears it so a new phase starts with a fresh
/// estimate.
#[derive(Default)]
pub struct RateTracker {
    /// (timestamp, bytes_so_far) samples. Front = oldest.
    samples: VecDeque<(Instant, u64)>,
    /// Stage label seen on the previous tick, to detect phase transitions.
    last_stage: String,
}

impl RateTracker {
    /// Record one sample. `stage` should be the operation label or any other
    /// string that changes when the work moves to a new phase. When the stage
    /// changes (or the counter regresses) the rolling window is cleared.
    pub fn record(&mut self, current_bytes: u64, stage: &str) {
        const WINDOW: Duration = Duration::from_secs(10);
        if stage != self.last_stage {
            self.last_stage = stage.to_string();
            self.samples.clear();
        }
        // Bytes regressing (e.g. a checksum phase restarts the counter) also
        // resets the window — we'd otherwise compute a negative rate.
        if let Some(&(_, last_bytes)) = self.samples.back() {
            if current_bytes < last_bytes {
                self.samples.clear();
            }
        }
        let now = Instant::now();
        self.samples.push_back((now, current_bytes));
        while let Some(&(t, _)) = self.samples.front() {
            if now.duration_since(t) > WINDOW {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Compute bytes/sec over the rolling window. Returns `None` when there
    /// isn't enough data yet (≤ 1 sample or < 250ms elapsed) so callers can hide
    /// the rate text until it's meaningful.
    pub fn rate_bytes_per_sec(&self) -> Option<f64> {
        if self.samples.len() < 2 {
            return None;
        }
        let (t_first, b_first) = *self.samples.front()?;
        let (t_last, b_last) = *self.samples.back()?;
        let dt = t_last.duration_since(t_first).as_secs_f64();
        if dt < 0.25 {
            return None;
        }
        let db = b_last.saturating_sub(b_first) as f64;
        if db == 0.0 {
            return Some(0.0);
        }
        Some(db / dt)
    }

    /// Estimated seconds remaining at the current rate. Returns `None` when the
    /// rate is unknown, zero, or `total_bytes` is missing.
    pub fn eta_secs(&self, current_bytes: u64, total_bytes: u64) -> Option<u64> {
        let rate = self.rate_bytes_per_sec()?;
        if rate <= 0.0 || total_bytes == 0 {
            return None;
        }
        let remaining = total_bytes.saturating_sub(current_bytes) as f64;
        Some((remaining / rate) as u64)
    }

    /// Format the trailing ` - <rate>/s, ETA <eta>` suffix that progress
    /// displays append to their `bytes / total` label. Empty when neither rate
    /// nor ETA is available, so callers can splice it in unconditionally.
    pub fn suffix(&self, current_bytes: u64, total_bytes: u64) -> String {
        match (
            self.rate_bytes_per_sec(),
            self.eta_secs(current_bytes, total_bytes),
        ) {
            (Some(r), Some(eta)) if r > 0.0 => {
                format!(" - {}/s, ETA {}", format_rate(r), format_eta(eta))
            }
            (Some(r), _) if r > 0.0 => format!(" - {}/s", format_rate(r)),
            _ => String::new(),
        }
    }

    /// Reset all sampling state. Useful when a worker is torn down and a fresh
    /// one starts in the same slot.
    pub fn reset(&mut self) {
        self.samples.clear();
        self.last_stage.clear();
    }
}

/// Format a bytes-per-second rate as KiB/s / MiB/s / GiB/s.
fn format_rate(bytes_per_sec: f64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    if bytes_per_sec >= GIB {
        format!("{:.2} GiB", bytes_per_sec / GIB)
    } else if bytes_per_sec >= MIB {
        format!("{:.1} MiB", bytes_per_sec / MIB)
    } else if bytes_per_sec >= KIB {
        format!("{:.0} KiB", bytes_per_sec / KIB)
    } else {
        format!("{:.0} B", bytes_per_sec)
    }
}

/// Format an ETA in seconds as `Hh Mm` / `Mm Ss` / `Ss`.
fn format_eta(total_secs: u64) -> String {
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    if h > 0 {
        format!("{h}h {m:02}m")
    } else if m > 0 {
        format!("{m}m {s:02}s")
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn rate_tracker_returns_none_before_two_samples() {
        let mut t = RateTracker::default();
        t.record(0, "stage");
        assert!(t.rate_bytes_per_sec().is_none());
        assert!(t.eta_secs(0, 1024).is_none());
        assert_eq!(t.suffix(0, 1024), "");
    }

    #[test]
    fn rate_tracker_computes_rate_after_window_opens() {
        let mut t = RateTracker::default();
        t.record(0, "stage");
        sleep(Duration::from_millis(300));
        t.record(1_000_000, "stage");
        let rate = t.rate_bytes_per_sec().expect("rate available");
        // ~300ms + 1 MB => ~3 MB/s; allow a generous range for CI noise.
        assert!(rate > 1_000_000.0 && rate < 20_000_000.0, "rate = {rate}");
        let suffix = t.suffix(1_000_000, 10_000_000);
        assert!(suffix.starts_with(" - "));
        assert!(suffix.contains("/s"));
        assert!(suffix.contains("ETA"));
    }

    #[test]
    fn rate_tracker_resets_on_stage_change() {
        let mut t = RateTracker::default();
        t.record(0, "stage-A");
        sleep(Duration::from_millis(300));
        t.record(1_000_000, "stage-A");
        assert!(t.rate_bytes_per_sec().is_some());
        t.record(0, "stage-B");
        assert!(t.rate_bytes_per_sec().is_none());
    }

    #[test]
    fn rate_tracker_resets_on_byte_regression() {
        let mut t = RateTracker::default();
        t.record(500, "stage");
        sleep(Duration::from_millis(300));
        t.record(1_000_000, "stage");
        assert!(t.rate_bytes_per_sec().is_some());
        t.record(0, "stage");
        assert!(t.rate_bytes_per_sec().is_none());
    }
}
