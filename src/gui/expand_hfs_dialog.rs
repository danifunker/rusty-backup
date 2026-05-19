//! "Expand HFS Volume…" dialog for the Inspect tab.
//!
//! Step 7 of `docs/hfs_expand_block_size.md`. The user picks a target volume
//! size + allocation block size; a worker thread captures the source HFS
//! volume, builds a fresh blank HFS image of the chosen size, copies every
//! file/dir/metadata across, and wraps the result in a new APM disk image
//! at the user-chosen output path.
//!
//! Source disk is opened read-only; the output is always a brand-new file.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::model::hfs_expand_runner::{
    max_volume_for_block_size, start_hfs_expand, suggest_block_size, ExpandSource,
    BLOCK_SIZE_CHOICES,
};
use rusty_backup::model::status::ExpandStatus;

use super::progress::LogPanel;

pub use rusty_backup::model::hfs_expand_runner::summarize_source;

pub struct ExpandHfsDialog {
    pub open: bool,
    source: ExpandSource,
    /// Target size in MiB.
    target_size_mib: u32,
    /// Target allocation block size (one of `BLOCK_SIZE_CHOICES`).
    target_block_size: u32,
    /// User-picked output file path.
    output_path: Option<PathBuf>,
    /// Background work status; `Some` while running and after completion.
    status: Option<Arc<Mutex<ExpandStatus>>>,
    /// Set once the user has acknowledged a finished run.
    completed: bool,
}

impl ExpandHfsDialog {
    pub fn new(source: ExpandSource) -> Self {
        // Default target = current partition size, but never below the source's
        // used space. Round up to nearest MiB.
        let suggested_bytes = source.partition_size.max(source.used_bytes);
        let suggested_mib =
            (suggested_bytes.div_ceil(1024 * 1024)).clamp(1, u32::MAX as u64) as u32;
        let suggested_bs = suggest_block_size(suggested_bytes);
        Self {
            open: true,
            source,
            target_size_mib: suggested_mib,
            target_block_size: suggested_bs,
            output_path: None,
            status: None,
            completed: false,
        }
    }

    /// Render the dialog. Returns `false` once the user closes it for good.
    pub fn show(&mut self, ctx: &egui::Context, log: &mut LogPanel) -> bool {
        if !self.open {
            return false;
        }
        // Snapshot status state so the closure doesn't have to lock twice.
        let status_snapshot: Option<(bool, String, Vec<String>, Option<String>)> =
            self.status.as_ref().and_then(|s| {
                s.lock().ok().map(|g| {
                    (
                        g.finished,
                        g.current_step.clone(),
                        g.log_messages.clone(),
                        g.error.clone(),
                    )
                })
            });

        let mut start_clicked = false;
        let mut close_clicked = false;
        let mut pick_output_clicked = false;

        let mut window_open = self.open;
        egui::Window::new("Expand HFS Volume…")
            .open(&mut window_open)
            .resizable(true)
            .default_width(520.0)
            .show(ctx, |ui| {
                ui.label(egui::RichText::new("Source").strong());
                ui.label(format!("Volume: {}", self.source.volume_name));
                ui.label(format!(
                    "Size: {} MiB ({} bytes)",
                    self.source.partition_size / (1024 * 1024),
                    self.source.partition_size
                ));
                ui.label(format!(
                    "Allocation block size: {} KiB",
                    self.source.source_block_size / 1024
                ));
                ui.label(format!(
                    "Used: {} MiB across {} file(s) in {} folder(s)",
                    self.source.used_bytes / (1024 * 1024),
                    self.source.file_count,
                    self.source.dir_count
                ));

                ui.separator();
                ui.label(egui::RichText::new("Target").strong());

                let busy = status_snapshot
                    .as_ref()
                    .map(|(finished, _, _, _)| !*finished)
                    .unwrap_or(false);

                ui.add_enabled_ui(!busy && !self.completed, |ui| {
                    ui.horizontal(|ui| {
                        ui.label("Allocation block size:");
                        egui::ComboBox::new("expand_hfs_bs", "")
                            .selected_text(format!(
                                "{} KiB (max {} MiB)",
                                self.target_block_size / 1024,
                                max_volume_for_block_size(self.target_block_size) / (1024 * 1024)
                            ))
                            .show_ui(ui, |ui| {
                                for &bs in BLOCK_SIZE_CHOICES {
                                    ui.selectable_value(
                                        &mut self.target_block_size,
                                        bs,
                                        format!(
                                            "{} KiB (max {} MiB)",
                                            bs / 1024,
                                            max_volume_for_block_size(bs) / (1024 * 1024)
                                        ),
                                    );
                                }
                            });
                    });

                    let max_mib_for_bs =
                        (max_volume_for_block_size(self.target_block_size) / (1024 * 1024)) as u32;
                    let min_mib = (self.source.used_bytes.div_ceil(1024 * 1024)).max(1) as u32;
                    if self.target_size_mib > max_mib_for_bs {
                        self.target_size_mib = max_mib_for_bs;
                    }
                    if self.target_size_mib < min_mib {
                        self.target_size_mib = min_mib;
                    }

                    ui.horizontal(|ui| {
                        ui.label("Target size (MiB):");
                        ui.add(egui::Slider::new(
                            &mut self.target_size_mib,
                            min_mib..=max_mib_for_bs,
                        ));
                    });

                    ui.horizontal(|ui| {
                        ui.label("Output file:");
                        let path_text = self
                            .output_path
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|| "(not chosen)".to_string());
                        ui.label(path_text);
                        if ui.button("Save As…").clicked() {
                            pick_output_clicked = true;
                        }
                    });
                });

                ui.separator();
                if let Some((finished, step, msgs, error)) = &status_snapshot {
                    if !*finished {
                        ui.horizontal(|ui| {
                            ui.spinner();
                            ui.label(step);
                        });
                    } else if let Some(err) = error {
                        ui.colored_label(
                            egui::Color32::from_rgb(255, 100, 100),
                            format!("Failed: {err}"),
                        );
                    } else {
                        ui.colored_label(
                            egui::Color32::from_rgb(100, 200, 100),
                            "Expand complete.",
                        );
                    }
                    if !msgs.is_empty() {
                        egui::ScrollArea::vertical()
                            .max_height(140.0)
                            .id_salt("expand_hfs_log")
                            .show(ui, |ui| {
                                for m in msgs {
                                    ui.label(m);
                                }
                            });
                    }
                }

                ui.horizontal(|ui| {
                    let can_start = !busy && !self.completed && self.output_path.is_some();
                    if ui
                        .add_enabled(can_start, egui::Button::new("Expand"))
                        .clicked()
                    {
                        start_clicked = true;
                    }
                    let close_label = if self.completed { "Close" } else { "Cancel" };
                    if ui
                        .add_enabled(!busy, egui::Button::new(close_label))
                        .clicked()
                    {
                        close_clicked = true;
                    }
                });
            });
        self.open = window_open;

        if pick_output_clicked {
            let default_name = format!(
                "{}-expanded.hda",
                sanitize_filename(&self.source.volume_name)
            );
            if let Some(path) = super::file_dialog()
                .set_file_name(&default_name)
                .add_filter("Disk image", &["hda", "img", "dsk"])
                .save_file()
            {
                self.output_path = Some(path);
            }
        }
        if start_clicked {
            if let Some(out) = self.output_path.clone() {
                self.spawn_worker(out, log);
            }
        }
        if close_clicked {
            self.open = false;
        }

        // Promote a finished run into the "completed" state so the Expand
        // button stays disabled and the close button reads "Close".
        if let Some((finished, _, _, _)) = &status_snapshot {
            if *finished {
                self.completed = true;
            }
        }

        self.open
    }

    fn spawn_worker(&mut self, output: PathBuf, log: &mut LogPanel) {
        let source = self.source.clone();
        let target_size = self.target_size_mib as u64 * 1024 * 1024;
        let target_bs = self.target_block_size;
        log.info(format!(
            "Expanding HFS volume '{}' to {} MiB at {} KiB blocks -> {}",
            source.volume_name,
            self.target_size_mib,
            target_bs / 1024,
            output.display()
        ));
        self.status = Some(start_hfs_expand(source, target_size, target_bs, output));
    }
}

/// Strip filesystem-unfriendly characters from a volume name for use as a
/// default output filename.
fn sanitize_filename(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ' ') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim().to_string();
    if trimmed.is_empty() {
        "Expanded".to_string()
    } else {
        trimmed
    }
}
