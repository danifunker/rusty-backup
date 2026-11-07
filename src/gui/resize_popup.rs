//! The Resize Partitions window: a shell over `controller::resize`.
//!
//! Draws the device warning, the Current / After bars, the size grid, the preview and the
//! write's job, and maps widgets to [`ResizeAction`]s. The rules and the write are the
//! controller's (docs/phase2_commander_plan.md § "Slice 15").

use rusty_backup::controller::log::LogStore;
use rusty_backup::controller::partition_edit::LayoutBar;
use rusty_backup::controller::resize::{
    format_size, ResizeAction, ResizeController, ResizePlan, ResizeRow, SizeMode,
};
use rusty_backup::controller::Controller;

pub struct ResizePopup {
    ctl: ResizeController,
}

impl ResizePopup {
    pub fn new(plan: ResizePlan) -> Self {
        let mut ctl = ResizeController::new(std::sync::Arc::new(super::ui_logger::EguiWake));
        ctl.apply(ResizeAction::Open(Box::new(plan)));
        ResizePopup { ctl }
    }

    /// Drain the controller: log lines, and whether a write just ended (the host re-reads the table).
    pub fn poll(&mut self, log: &mut LogStore) -> bool {
        self.ctl.tick();
        super::progress::drain_log_lines(self.ctl.take_log(), log);
        self.ctl.take_disk_changed()
    }

    /// A resize is running.
    pub fn is_running(&self) -> bool {
        self.ctl.is_running()
    }

    /// Show the window. Returns false once it is closed.
    pub fn show(&mut self, ui: &mut egui::Ui, log: &mut LogStore) -> bool {
        let mut actions = Vec::new();
        egui::Window::new("Resize Partitions")
            .collapsible(false)
            .resizable(true)
            .default_width(600.0)
            .show(ui.ctx(), |ui| {
                let running = self.ctl.is_running();

                if self.ctl.is_device() {
                    ui.group(|ui| {
                        ui.colored_label(
                            super::theme::danger(ui.visuals()),
                            "WARNING: This will directly modify the physical device. Data loss is permanent and irreversible!",
                        );
                        let mut accepted = self.ctl.device_risk_accepted();
                        if ui
                            .checkbox(&mut accepted, "I understand the risks and want to proceed")
                            .changed()
                        {
                            actions.push(ResizeAction::AcceptDeviceRisk(accepted));
                        }
                    });
                    ui.add_space(4.0);
                }

                // Growing the container is Inspect's "Expand Image...", not this dialog.
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new(format!(
                            "Disk size: {}",
                            format_size(self.ctl.disk_size())
                        ))
                        .weak(),
                    );
                    if self.ctl.can_grow_container() {
                        ui.label(
                            egui::RichText::new(
                                "- to make more room, close this and use \"Expand Image...\"",
                            )
                            .weak(),
                        );
                    }
                });
                ui.add_space(4.0);

                let (current, after, scale) = self.ctl.layout();
                show_scaled_bar(ui, &current, scale);
                ui.add_space(4.0);
                show_scaled_bar(ui, &after, scale);
                ui.add_space(8.0);

                ui.label(egui::RichText::new("Partition Sizes:").strong());
                egui::Grid::new("resize_partition_grid")
                    .striped(true)
                    .min_col_width(60.0)
                    .show(ui, |ui| {
                        for head in [
                            "#",
                            "Type",
                            "Current Size",
                            "Min Size",
                            "Size Mode",
                            "New Size (MiB)",
                        ] {
                            ui.label(egui::RichText::new(head).strong());
                        }
                        ui.end_row();
                        for row in self.ctl.rows() {
                            size_row(ui, row, running, &mut actions);
                        }
                    });

                ui.add_space(8.0);

                if let Some(err) = self.ctl.plan_error() {
                    ui.colored_label(super::theme::danger(ui.visuals()), format!("Error: {err}"));
                }

                if let Some(preview) = self.ctl.preview() {
                    ui.label(egui::RichText::new("Preview:").strong());
                    egui::Grid::new("resize_preview_grid")
                        .striped(true)
                        .min_col_width(60.0)
                        .show(ui, |ui| {
                            for head in
                                ["#", "Old Start", "Old Size", "New Start", "New Size", "Action"]
                            {
                                ui.label(egui::RichText::new(head).strong());
                            }
                            ui.end_row();
                            for row in preview {
                                ui.label(format!("{}", row.index));
                                ui.label(row.old_start.as_str());
                                ui.label(row.old_size.as_str());
                                ui.label(row.new_start.as_str());
                                ui.label(row.new_size.as_str());
                                ui.label(row.action.as_str());
                                ui.end_row();
                            }
                        });
                    ui.add_space(4.0);
                }

                if let Some(snap) = self.ctl.progress() {
                    if super::progress::show_job(ui, &snap) {
                        actions.push(ResizeAction::Cancel);
                    }
                }

                ui.add_space(8.0);

                ui.horizontal(|ui| {
                    if ui.add_enabled(!running, egui::Button::new("Preview")).clicked() {
                        actions.push(ResizeAction::Preview);
                    }
                    if ui
                        .add_enabled(self.ctl.can_apply(), egui::Button::new("Apply"))
                        .clicked()
                    {
                        actions.push(ResizeAction::Apply);
                    }
                    if ui.add_enabled(!running, egui::Button::new("Close")).clicked() {
                        actions.push(ResizeAction::Close);
                    }
                });
            });
        for a in actions {
            self.ctl.apply(a);
        }
        super::progress::drain_log_lines(self.ctl.take_log(), log);
        self.ctl.is_open()
    }
}

/// One row of the size grid: an extended container greyed out, any other with its mode radios
/// and the New Size field (editable only in Custom).
fn size_row(ui: &mut egui::Ui, row: &ResizeRow, running: bool, actions: &mut Vec<ResizeAction>) {
    if row.extended {
        let muted = super::theme::muted(ui.visuals());
        ui.colored_label(muted, format!("{}", row.index));
        ui.colored_label(muted, format!("{} (extended)", row.type_name));
        ui.colored_label(muted, format_size(row.original_size));
        for _ in 0..3 {
            ui.colored_label(muted, "-");
        }
        ui.end_row();
        return;
    }
    ui.label(format!("{}", row.index));
    ui.label(row.type_name.as_str());
    ui.label(format_size(row.original_size));
    ui.label(if row.minimum_size > 0 {
        format_size(row.minimum_size)
    } else {
        "-".to_string()
    });
    ui.add_enabled_ui(!running, |ui| {
        ui.horizontal(|ui| {
            for mode in row.mode_choices() {
                if ui.radio(row.mode == mode, mode.label()).clicked() && row.mode != mode {
                    actions.push(ResizeAction::SetMode {
                        index: row.index,
                        mode,
                    });
                }
            }
        });
    });
    let mut text = row.text.clone();
    let field = egui::TextEdit::singleline(&mut text).desired_width(80.0);
    if ui
        .add_enabled(!running && row.mode == SizeMode::Custom, field)
        .changed()
    {
        actions.push(ResizeAction::SetSizeText {
            index: row.index,
            text,
        });
    }
    ui.end_row();
}

/// A layout bar whose width is its total against `scale`, so growth and shrink show.
fn show_scaled_bar(ui: &mut egui::Ui, bar: &LayoutBar, scale: u64) {
    use super::partition_bar::{PartitionBar, Segment, SegmentKind};
    ui.label(bar.caption.as_str());
    let total: u64 = bar.segments.iter().map(|s| s.size_bytes).sum();
    let segments = bar
        .segments
        .iter()
        .map(|s| Segment {
            label: s.label.clone(),
            fs: s.fs.clone(),
            size_bytes: s.size_bytes,
            kind: match s.color {
                Some(color_index) => SegmentKind::Partition { color_index },
                None => SegmentKind::Free,
            },
            partition: None,
        })
        .collect();
    let available = ui.available_width().max(120.0);
    let width = available * (total as f64 / scale as f64) as f32;
    ui.scope(|ui| {
        ui.set_width(width.max(60.0));
        PartitionBar {
            segments,
            show_inline_labels: true,
            show_legend: bar.legend,
        }
        .show(ui);
    });
}
