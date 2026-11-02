use egui::RichText;
use rusty_backup::controller::app::{format_size, AppController, RateTracker, Snapshot};
use rusty_backup::controller::log::{LogLevel, LogStore};
use rusty_backup::controller::Controller;

/// Draw the log panel at the bottom of the window from the app view-model.
pub fn show_log(ui: &mut egui::Ui, app: &mut AppController) {
    use rusty_backup::controller::app::AppAction;
    let mut actions = Vec::new();
    ui.horizontal(|ui| {
        ui.label(RichText::new("Log").strong());
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            if ui.button("Copy Log").clicked() {
                actions.push(AppAction::CopyLog);
            }
            if ui.button("Clear").clicked() {
                actions.push(AppAction::ClearLog);
            }
            let mut auto_scroll = app.log_auto_scroll();
            if ui.checkbox(&mut auto_scroll, "Auto-scroll").changed() {
                actions.push(AppAction::SetLogAutoScroll(auto_scroll));
            }
        });
    });

    ui.separator();

    // Use a simple ScrollArea without show_rows to avoid bouncing
    egui::ScrollArea::vertical()
        .auto_shrink([false; 2])
        .stick_to_bottom(app.log_auto_scroll())
        .show(ui, |ui| {
            ui.set_width(ui.available_width());
            for entry in app.log.entries() {
                let color = match entry.level {
                    LogLevel::Info => super::theme::muted(ui.visuals()),
                    LogLevel::Warning => super::theme::warning(ui.visuals()),
                    LogLevel::Error => super::theme::danger(ui.visuals()),
                };
                // Use horizontal layout with word wrap to handle long lines
                ui.horizontal_wrapped(|ui| {
                    ui.label(RichText::new(entry.line()).color(color).monospace());
                });
            }
        });

    for action in actions {
        let effects = app.apply(action);
        super::run_effects(ui.ctx(), effects);
    }
}

/// Move a controller's log lines into the log panel.
pub fn drain_log_lines(lines: Vec<(LogLevel, String)>, log: &mut LogStore) {
    for (level, msg) in lines {
        match level {
            LogLevel::Info => log.info(msg),
            LogLevel::Warning => log.warn(msg),
            LogLevel::Error => log.error(msg),
        }
    }
}

/// Draw a running job: its step, a bar (an activity bar when the total is unknown),
/// speed and time, and Cancel when offered; returns true when Cancel is clicked.
pub fn show_job(ui: &mut egui::Ui, snap: &Snapshot) -> bool {
    let mut cancel = false;
    let phase = if snap.phase.is_empty() {
        snap.label.as_str()
    } else {
        snap.phase.as_str()
    };
    ui.vertical(|ui| {
        ui.horizontal(|ui| {
            ui.label(phase);
            if snap.cancel_requested {
                ui.label(RichText::new("(cancelling)").weak());
            } else if snap.can_cancel && ui.small_button("Cancel").clicked() {
                cancel = true;
            }
        });
        let bar = match snap.progress.fraction() {
            Some(f) => egui::ProgressBar::new(f).show_percentage(),
            // No total to measure against: a sweeping fill says it is alive without implying a percentage.
            None => {
                let t = snap.elapsed.as_secs_f32();
                let sweep = (t * 0.6).fract();
                egui::ProgressBar::new(sweep).animate(true)
            }
        };
        ui.add(bar.desired_width(260.0));
        ui.label(RichText::new(snap.detail()).small().weak());
    });
    ui.ctx()
        .request_repaint_after(std::time::Duration::from_millis(100));
    cancel
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
                        format_size(self.current_bytes),
                        format_size(self.total_bytes),
                        format_size(self.full_size_bytes),
                        fraction * 100.0,
                        rate_eta,
                    )
                } else {
                    format!(
                        "{} / {} ({:.0}%){}",
                        format_size(self.current_bytes),
                        format_size(self.total_bytes),
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
