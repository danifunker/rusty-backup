use egui::{Color32, RichText};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogLevel {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone)]
struct LogEntry {
    level: LogLevel,
    message: String,
    timestamp: String,
}

/// Scrollable log panel displayed at the bottom of the window.
pub struct LogPanel {
    entries: Vec<LogEntry>,
    auto_scroll: bool,
}

impl Default for LogPanel {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            auto_scroll: true,
        }
    }
}

impl LogPanel {
    pub fn add(&mut self, level: LogLevel, message: impl Into<String>) {
        let now = chrono_now();
        self.entries.push(LogEntry {
            level,
            message: message.into(),
            timestamp: now,
        });
    }

    pub fn info(&mut self, message: impl Into<String>) {
        self.add(LogLevel::Info, message);
    }

    pub fn warn(&mut self, message: impl Into<String>) {
        self.add(LogLevel::Warning, message);
    }

    pub fn error(&mut self, message: impl Into<String>) {
        self.add(LogLevel::Error, message);
    }

    /// Copy all log entries to clipboard
    fn copy_to_clipboard(&self, ctx: &egui::Context) {
        let log_text: String = self
            .entries
            .iter()
            .map(|entry| {
                let prefix = match entry.level {
                    LogLevel::Info => "INFO",
                    LogLevel::Warning => "WARN",
                    LogLevel::Error => "ERR ",
                };
                format!("{} [{}] {}", entry.timestamp, prefix, entry.message)
            })
            .collect::<Vec<_>>()
            .join("\n");
        ctx.copy_text(log_text);
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(RichText::new("Log").strong());
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Copy Log").clicked() {
                    self.copy_to_clipboard(ui.ctx());
                }
                if ui.button("Clear").clicked() {
                    self.entries.clear();
                }
                ui.checkbox(&mut self.auto_scroll, "Auto-scroll");
            });
        });

        ui.separator();

        // Use a simple ScrollArea without show_rows to avoid bouncing
        egui::ScrollArea::vertical()
            .auto_shrink([false; 2])
            .stick_to_bottom(self.auto_scroll)
            .show(ui, |ui| {
                ui.set_width(ui.available_width());
                for entry in &self.entries {
                    let color = match entry.level {
                        LogLevel::Info => Color32::GRAY,
                        LogLevel::Warning => Color32::YELLOW,
                        LogLevel::Error => Color32::from_rgb(255, 100, 100),
                    };
                    let prefix = match entry.level {
                        LogLevel::Info => "INFO",
                        LogLevel::Warning => "WARN",
                        LogLevel::Error => "ERR ",
                    };

                    let text = format!("{} [{}] {}", entry.timestamp, prefix, entry.message);

                    // Use horizontal layout with word wrap to handle long lines
                    ui.horizontal_wrapped(|ui| {
                        ui.label(RichText::new(text).color(color).monospace());
                    });
                }
            });
    }
}

/// Rolling-window transfer-rate + ETA estimator. Shared by progress
/// bars in different tabs (backup, restore, VHD export, physical-disk
/// export) so the visible rate/ETA semantics stay consistent across
/// the app.
///
/// Drop one in next to your view's progress state, call
/// [`RateTracker::record`] each frame with the current byte counter and
/// stage label (any string that changes when the work shifts to a new
/// phase), then ask for [`RateTracker::rate_bytes_per_sec`] /
/// [`RateTracker::eta_secs`] for display.
#[derive(Default)]
pub struct RateTracker {
    /// (timestamp, bytes_so_far) samples. Front = oldest. Window is
    /// trimmed to ~10s so the ETA stays responsive to stalls without
    /// flapping each frame.
    samples: std::collections::VecDeque<(std::time::Instant, u64)>,
    /// Stage label observed on the previous frame, used to detect
    /// stage transitions. When the label changes the rate window is
    /// cleared so a new stage starts with a fresh ETA estimate
    /// instead of dragging the previous stage's numbers along.
    last_stage: String,
}

impl RateTracker {
    /// Record one sample. `stage` should be the operation label or any
    /// other string that changes when the work moves to a new phase
    /// (e.g. "Backing up partition 1" → "Checksum verify"). When the
    /// stage changes the rolling window is cleared.
    pub fn record(&mut self, current_bytes: u64, stage: &str) {
        const WINDOW: std::time::Duration = std::time::Duration::from_secs(10);
        if stage != self.last_stage {
            self.last_stage = stage.to_string();
            self.samples.clear();
        }
        // Bytes regressing (e.g. a checksum phase restarts the counter)
        // also resets the window — we'd otherwise compute a negative
        // rate.
        if let Some(&(_, last_bytes)) = self.samples.back() {
            if current_bytes < last_bytes {
                self.samples.clear();
            }
        }
        let now = std::time::Instant::now();
        self.samples.push_back((now, current_bytes));
        while let Some(&(t, _)) = self.samples.front() {
            if now.duration_since(t) > WINDOW {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    /// Compute bytes/sec over the rolling window. Returns `None` when
    /// there isn't enough data yet (≤ 1 sample or < 250ms elapsed) so
    /// the view can hide the rate text until it's meaningful.
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

    /// Estimated seconds remaining at the current rate. Returns `None`
    /// when rate is unknown, zero, or `total_bytes` is missing.
    pub fn eta_secs(&self, current_bytes: u64, total_bytes: u64) -> Option<u64> {
        let rate = self.rate_bytes_per_sec()?;
        if rate <= 0.0 || total_bytes == 0 {
            return None;
        }
        let remaining = total_bytes.saturating_sub(current_bytes) as f64;
        Some((remaining / rate) as u64)
    }

    /// Convenience: format the trailing " - <rate>/s, ETA <eta>"
    /// suffix that progress bars append to their `bytes / total`
    /// label. Returns an empty string when neither rate nor ETA is
    /// available, so callers can splice this in unconditionally.
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

    /// Reset all sampling state. Useful when a worker is torn down and
    /// a fresh one starts in the same view slot.
    pub fn reset(&mut self) {
        self.samples.clear();
        self.last_stage.clear();
    }
}

/// Progress state for long-running operations.
#[derive(Default)]
pub struct ProgressState {
    pub current_bytes: u64,
    pub total_bytes: u64,
    /// Full untrimmed partition sizes (for displaying savings).
    /// When this is larger than `total_bytes`, smart trimming is active.
    pub full_size_bytes: u64,
    pub operation: String,
    pub active: bool,
    tracker: RateTracker,
}

impl ProgressState {
    /// Record one sample for the rate window. Called by the view each
    /// frame after copying fields from the backup-thread snapshot.
    pub fn record_sample(&mut self) {
        let op = self.operation.clone();
        self.tracker.record(self.current_bytes, &op);
    }

    pub fn show(&self, ui: &mut egui::Ui) {
        if !self.active {
            return;
        }

        ui.horizontal(|ui| {
            ui.label(&self.operation);
            if self.total_bytes > 0 {
                let fraction = self.current_bytes as f32 / self.total_bytes as f32;
                let rate_eta = self.tracker.suffix(self.current_bytes, self.total_bytes);
                let text = if self.full_size_bytes > self.total_bytes {
                    format!(
                        "{} / {} to image of {} total ({:.0}%){}",
                        rusty_backup::partition::format_size(self.current_bytes),
                        rusty_backup::partition::format_size(self.total_bytes),
                        rusty_backup::partition::format_size(self.full_size_bytes),
                        fraction * 100.0,
                        rate_eta,
                    )
                } else {
                    format!(
                        "{} / {} ({:.0}%){}",
                        rusty_backup::partition::format_size(self.current_bytes),
                        rusty_backup::partition::format_size(self.total_bytes),
                        fraction * 100.0,
                        rate_eta,
                    )
                };
                let bar = egui::ProgressBar::new(fraction).text(text).animate(true);
                ui.add(bar);
            } else {
                ui.spinner();
            }
        });
    }
}

/// Format a bytes-per-second rate as KiB/s / MiB/s / GiB/s. Mirrors
/// `partition::format_size` style for visual consistency in the
/// progress bar.
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

/// Format an ETA in seconds as `Hh Mm` / `Mm Ss` / `Ss`. Stays
/// compact so it fits inline in the progress-bar text.
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

/// Get current timestamp in local time.
fn chrono_now() -> String {
    chrono::Local::now().format("%H:%M:%S").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

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
        // We slept ~300ms and added 1 MB so rate should be in the
        // ballpark of 3 MB/s. Allow a generous range for CI noise.
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
        // Window cleared; only one sample in the new stage.
        assert!(t.rate_bytes_per_sec().is_none());
    }

    #[test]
    fn rate_tracker_resets_on_byte_regression() {
        let mut t = RateTracker::default();
        t.record(500, "stage");
        sleep(Duration::from_millis(300));
        t.record(1_000_000, "stage");
        assert!(t.rate_bytes_per_sec().is_some());
        // A regression (counter restart) clears the window.
        t.record(0, "stage");
        assert!(t.rate_bytes_per_sec().is_none());
    }
}
