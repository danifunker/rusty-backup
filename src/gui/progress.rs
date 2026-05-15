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
    /// Rolling sample buffer of (instant, current_bytes) for rate/ETA
    /// estimation. Updated by the view each frame via
    /// [`ProgressState::record_sample`]; older entries are pruned to
    /// keep the window roughly 10s wide so ETA stays responsive to
    /// stalls and phase changes without flapping on each frame.
    samples: std::collections::VecDeque<(std::time::Instant, u64)>,
    /// Operation label observed on the previous frame, used to detect
    /// stage transitions. When the label changes the rate window is
    /// cleared so a new stage starts with a fresh ETA estimate
    /// instead of dragging the previous stage's numbers along.
    last_operation: String,
}

impl ProgressState {
    /// Record one sample for the rate window. Called by the view each
    /// frame after copying fields from the backup-thread snapshot.
    /// Idempotent if called more than once per frame (the latest
    /// sample overwrites previous samples within the same millisecond).
    pub fn record_sample(&mut self) {
        const WINDOW: std::time::Duration = std::time::Duration::from_secs(10);
        // Phase change resets the window so ETAs don't carry across
        // stages with different byte-rate profiles.
        if self.operation != self.last_operation {
            self.last_operation = self.operation.clone();
            self.samples.clear();
        }
        // Bytes regressing (e.g. checksum phase restarts the counter)
        // also resets the window — we'd otherwise compute a negative
        // rate.
        if let Some(&(_, last_bytes)) = self.samples.back() {
            if self.current_bytes < last_bytes {
                self.samples.clear();
            }
        }
        let now = std::time::Instant::now();
        self.samples.push_back((now, self.current_bytes));
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

    /// Estimated seconds remaining for this stage at the current rate.
    /// Returns `None` when rate is unknown, zero, or the totals are
    /// missing.
    pub fn eta_secs(&self) -> Option<u64> {
        let rate = self.rate_bytes_per_sec()?;
        if rate <= 0.0 || self.total_bytes == 0 {
            return None;
        }
        let remaining = self.total_bytes.saturating_sub(self.current_bytes) as f64;
        Some((remaining / rate) as u64)
    }

    pub fn show(&self, ui: &mut egui::Ui) {
        if !self.active {
            return;
        }

        ui.horizontal(|ui| {
            ui.label(&self.operation);
            if self.total_bytes > 0 {
                let fraction = self.current_bytes as f32 / self.total_bytes as f32;
                let rate_eta = match (self.rate_bytes_per_sec(), self.eta_secs()) {
                    (Some(r), Some(eta)) if r > 0.0 => {
                        format!(" - {}/s, ETA {}", format_rate(r), format_eta(eta))
                    }
                    (Some(r), _) if r > 0.0 => format!(" - {}/s", format_rate(r)),
                    _ => String::new(),
                };
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
