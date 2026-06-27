use egui::{Color32, RichText};

// The rate/ETA estimator lives in the (non-GUI) model layer so the CLI shares
// it; re-exported here for the tabs that refer to `progress::RateTracker`.
pub use rusty_backup::model::rate_tracker::RateTracker;

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

/// Get current timestamp in local time.
fn chrono_now() -> String {
    chrono::Local::now().format("%H:%M:%S").to_string()
}
