//! Progress modal for Commander Mode's long-running operations.
//!
//! One widget, one shape: a small centered window with a title, "N of M
//! files" line, the file currently being written, a byte counter, and a
//! `ProgressBar` labelled with percent + rate + ETA. Optional Cancel button.
//!
//! The rate/ETA comes from the same [`RateTracker`] the Backup / Restore /
//! Physical-Disk-Export flows already use, so a Commander copy reads its
//! progress the same way the rest of the app does.

use egui::Align2;

use super::super::progress::RateTracker;

/// One frame's snapshot of a long-running operation. The caller assembles
/// this from whatever `Arc<Mutex<...>>` its worker owns; the modal doesn't
/// care which backend produced it.
pub struct ProgressSnapshot {
    /// Window title (e.g. `"Copying"`, `"Applying edits"`).
    pub title: String,
    /// The current file / edit path, shown as a subtitle.
    pub current: String,
    /// Files (or edits) completed so far.
    pub items_done: usize,
    /// Total files (or edits) — 0 hides the "N of M" counter.
    pub items_total: usize,
    /// Bytes processed so far.
    pub bytes_done: u64,
    /// Total bytes — 0 shows a spinner instead of a percentage.
    pub bytes_total: u64,
    /// True once the worker set `finished = true`.
    pub finished: bool,
    /// If set, an error message. Displayed in red for a beat before the
    /// window closes.
    pub error: Option<String>,
    /// Whether to render a Cancel button.
    pub can_cancel: bool,
    /// If the GUI has already flipped `cancel_requested`, we replace the
    /// Cancel button with a "Cancelling..." label.
    pub cancel_requested: bool,
}

/// Result of one `show` call. `Cancel` fires exactly once, in the frame the
/// user clicks the button.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressAction {
    /// Nothing to do — the modal is still visible, still running.
    Idle,
    /// The user clicked Cancel. Caller sets its worker's cancel flag.
    Cancel,
}

/// Small stateful helper: owns a [`RateTracker`] so its rate + ETA persist
/// across frames. One instance per pending op (or per `CommanderMode`, since
/// only one op runs at a time).
#[derive(Default)]
pub struct ProgressWindow {
    tracker: RateTracker,
    /// Set to the operation name last recorded, so a fresh op resets the
    /// tracker window automatically (RateTracker's own record() does this
    /// when the stage changes).
    last_stage: String,
}

impl ProgressWindow {
    /// Reset between operations. Called by the caller when a modal closes so
    /// the next op starts from a clean slate.
    pub fn reset(&mut self) {
        self.tracker = RateTracker::default();
        self.last_stage.clear();
    }

    /// Render one frame. `snapshot` is built by the caller from its worker
    /// status. Returns `ProgressAction::Cancel` in the frame the user clicks
    /// the Cancel button.
    pub fn show(&mut self, ctx: &egui::Context, snapshot: &ProgressSnapshot) -> ProgressAction {
        // Only feed the tracker while the worker is still going; a finished
        // op shouldn't keep advancing the ETA/rate.
        if !snapshot.finished {
            self.tracker.record(snapshot.bytes_done, &snapshot.title);
            self.last_stage = snapshot.title.clone();
        }

        let mut action = ProgressAction::Idle;

        // Modal: fixed-anchor, non-resizable, no collapse. Repaint each frame
        // so the bar + rate stay live.
        ctx.request_repaint();
        egui::Window::new(&snapshot.title)
            .anchor(Align2::CENTER_TOP, egui::vec2(0.0, 80.0))
            .collapsible(false)
            .resizable(false)
            .movable(false)
            .default_width(420.0)
            .show(ctx, |ui| {
                ui.set_min_width(420.0);
                if !snapshot.current.is_empty() {
                    ui.label(egui::RichText::new(&snapshot.current).monospace().weak());
                }
                if snapshot.items_total > 0 {
                    ui.label(format!(
                        "{} of {} item(s)",
                        snapshot.items_done, snapshot.items_total
                    ));
                }

                let frac = if snapshot.bytes_total > 0 {
                    (snapshot.bytes_done as f32 / snapshot.bytes_total as f32).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                let text = if snapshot.bytes_total > 0 {
                    let suffix = self
                        .tracker
                        .suffix(snapshot.bytes_done, snapshot.bytes_total);
                    format!(
                        "{} / {} ({:.0}%){}",
                        rusty_backup::partition::format_size(snapshot.bytes_done),
                        rusty_backup::partition::format_size(snapshot.bytes_total),
                        frac * 100.0,
                        suffix,
                    )
                } else if snapshot.bytes_done > 0 {
                    format!(
                        "{} processed",
                        rusty_backup::partition::format_size(snapshot.bytes_done)
                    )
                } else {
                    "Preparing...".to_string()
                };

                if snapshot.bytes_total > 0 {
                    ui.add(egui::ProgressBar::new(frac).text(text).animate(true));
                } else {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(text);
                    });
                }

                if let Some(err) = &snapshot.error {
                    ui.colored_label(
                        super::super::theme::danger(ui.visuals()),
                        format!("Error: {err}"),
                    );
                }

                if snapshot.can_cancel && !snapshot.finished {
                    ui.horizontal(|ui| {
                        if snapshot.cancel_requested {
                            ui.label("Cancelling...");
                        } else if ui.button("Cancel").clicked() {
                            action = ProgressAction::Cancel;
                        }
                    });
                }
            });

        action
    }
}
