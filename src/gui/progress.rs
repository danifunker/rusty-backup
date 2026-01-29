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

    pub fn show(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(RichText::new("Log").strong());
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Clear").clicked() {
                    self.entries.clear();
                }
                ui.checkbox(&mut self.auto_scroll, "Auto-scroll");
            });
        });

        ui.separator();

        let text_style = egui::TextStyle::Monospace;
        let row_height = ui.text_style_height(&text_style) + 2.0;
        let num_rows = self.entries.len();

        egui::ScrollArea::vertical()
            .auto_shrink([false; 2])
            .stick_to_bottom(self.auto_scroll)
            .show_rows(ui, row_height, num_rows, |ui, row_range| {
                for i in row_range {
                    let entry = &self.entries[i];
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
                    ui.horizontal(|ui| {
                        ui.label(
                            RichText::new(format!("{} [{}] {}", entry.timestamp, prefix, entry.message))
                                .color(color)
                                .monospace(),
                        );
                    });
                }
            });
    }
}

/// Progress state for long-running operations.
pub struct ProgressState {
    pub current_bytes: u64,
    pub total_bytes: u64,
    pub operation: String,
    pub active: bool,
}

impl Default for ProgressState {
    fn default() -> Self {
        Self {
            current_bytes: 0,
            total_bytes: 0,
            operation: String::new(),
            active: false,
        }
    }
}

impl ProgressState {
    pub fn show(&self, ui: &mut egui::Ui) {
        if !self.active {
            return;
        }

        ui.horizontal(|ui| {
            ui.label(&self.operation);
            if self.total_bytes > 0 {
                let fraction = self.current_bytes as f32 / self.total_bytes as f32;
                let bar = egui::ProgressBar::new(fraction)
                    .text(format!(
                        "{} / {} ({:.0}%)",
                        rusty_backup::partition::format_size(self.current_bytes),
                        rusty_backup::partition::format_size(self.total_bytes),
                        fraction * 100.0,
                    ))
                    .animate(true);
                ui.add(bar);
            } else {
                ui.spinner();
            }
        });
    }
}

/// Simple timestamp without pulling in chrono crate.
fn chrono_now() -> String {
    use std::time::SystemTime;
    let duration = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    let hours = (secs / 3600) % 24;
    let minutes = (secs / 60) % 60;
    let seconds = secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}
