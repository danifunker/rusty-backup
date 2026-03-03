//! Popup window for in-place partition resizing.

use std::collections::HashMap;
use std::io::Seek;
use std::sync::{Arc, Mutex};

use rusty_backup::device::DiskDevice;
use rusty_backup::partition::resize::{apply_resize, compute_resize_plan, detect_vhd};
use rusty_backup::partition::{self, PartitionInfo, PartitionTable};

use super::progress::LogPanel;

/// Background thread status for the resize operation.
struct ResizeStatus {
    finished: bool,
    error: Option<String>,
    log_messages: Vec<String>,
    current_bytes: u64,
    total_bytes: u64,
    cancel_requested: bool,
}

/// Per-partition entry in the resize grid.
struct ResizeEntry {
    index: usize,
    type_name: String,
    original_size: u64,
    minimum_size: u64,
    is_extended_container: bool,
    /// User-editable new size text in MiB.
    new_size_text: String,
}

/// Preview row for showing planned changes.
struct PreviewRow {
    index: usize,
    old_start: String,
    old_size: String,
    new_start: String,
    new_size: String,
    action: String,
}

/// Self-contained resize popup window.
pub struct ResizePopup {
    entries: Vec<ResizeEntry>,
    /// Computed plan for preview.
    preview: Option<Vec<PreviewRow>>,
    /// Validation/plan error message.
    plan_error: Option<String>,
    /// Background resize thread status.
    resize_status: Option<Arc<Mutex<ResizeStatus>>>,
    /// Alignment in sectors (0 = no alignment).
    alignment_sectors: u64,
    /// Total disk size in bytes.
    disk_size_bytes: u64,
    /// Whether the source is a physical device.
    is_device: bool,
    /// Whether the user has acknowledged the device risk warning.
    device_warning_accepted: bool,
    /// Partition table (needed for apply).
    partition_table: PartitionTable,
    /// All partition infos.
    partitions: Vec<PartitionInfo>,
    /// Source path for file I/O.
    source_path: std::path::PathBuf,
    /// Whether the popup should remain open.
    pub open: bool,
}

impl ResizePopup {
    pub fn new(
        partitions: &[PartitionInfo],
        partition_table: PartitionTable,
        partition_min_sizes: &HashMap<usize, u64>,
        alignment_sectors: u64,
        disk_size_bytes: u64,
        is_device: bool,
        source_path: std::path::PathBuf,
    ) -> Self {
        let entries = partitions
            .iter()
            .filter(|p| !p.is_logical)
            .map(|p| {
                let min_size = partition_min_sizes
                    .get(&p.index)
                    .copied()
                    .unwrap_or(0)
                    .max(512); // at least one sector
                let size_mib = p.size_bytes / (1024 * 1024);
                ResizeEntry {
                    index: p.index,
                    type_name: p.type_name.clone(),
                    original_size: p.size_bytes,
                    minimum_size: min_size,
                    is_extended_container: p.is_extended_container,
                    new_size_text: format!("{}", size_mib),
                }
            })
            .collect();

        Self {
            entries,
            preview: None,
            plan_error: None,
            resize_status: None,
            alignment_sectors,
            disk_size_bytes,
            is_device,
            device_warning_accepted: false,
            partition_table,
            partitions: partitions.to_vec(),
            source_path,
            open: true,
        }
    }

    /// Poll the background resize thread and drain log messages.
    pub fn poll_status(&mut self, log: &mut LogPanel) {
        let status_arc = match &self.resize_status {
            Some(s) => Arc::clone(s),
            None => return,
        };

        let Ok(mut status) = status_arc.lock() else {
            return;
        };

        for msg in status.log_messages.drain(..) {
            log.info(msg);
        }

        if status.finished {
            if let Some(err) = &status.error {
                log.error(format!("Resize failed: {err}"));
            } else {
                log.info(
                    "Partition resize completed successfully. Close and re-inspect to see updates.",
                );
            }
            drop(status);
            self.resize_status = None;
        }
    }

    /// Returns true if a resize is currently running.
    pub fn is_running(&self) -> bool {
        self.resize_status.is_some()
    }

    /// Show the resize popup window. Returns false if the popup should close.
    pub fn show(&mut self, ui: &mut egui::Ui, _devices: &[DiskDevice], log: &mut LogPanel) -> bool {
        let mut keep_open = self.open;

        egui::Window::new("Resize Partitions")
            .collapsible(false)
            .resizable(true)
            .default_width(600.0)
            .show(ui.ctx(), |ui| {
                let running = self.is_running();

                // Device risk warning
                if self.is_device {
                    ui.group(|ui| {
                        ui.colored_label(
                            egui::Color32::RED,
                            "WARNING: This will directly modify the physical device. Data loss is permanent and irreversible!",
                        );
                        ui.checkbox(
                            &mut self.device_warning_accepted,
                            "I understand the risks and want to proceed",
                        );
                    });
                    ui.add_space(4.0);
                }

                // Partition grid
                ui.label(egui::RichText::new("Partition Sizes:").strong());
                egui::Grid::new("resize_partition_grid")
                    .striped(true)
                    .min_col_width(60.0)
                    .show(ui, |ui| {
                        ui.label(egui::RichText::new("#").strong());
                        ui.label(egui::RichText::new("Type").strong());
                        ui.label(egui::RichText::new("Current Size").strong());
                        ui.label(egui::RichText::new("Min Size").strong());
                        ui.label(egui::RichText::new("New Size (MiB)").strong());
                        ui.end_row();

                        for entry in &mut self.entries {
                            if entry.is_extended_container {
                                // Show grayed out, not editable
                                ui.colored_label(egui::Color32::GRAY, format!("{}", entry.index));
                                ui.colored_label(
                                    egui::Color32::GRAY,
                                    format!("{} (extended)", entry.type_name),
                                );
                                ui.colored_label(
                                    egui::Color32::GRAY,
                                    partition::format_size(entry.original_size),
                                );
                                ui.colored_label(egui::Color32::GRAY, "—");
                                ui.colored_label(egui::Color32::GRAY, "—");
                                ui.end_row();
                                continue;
                            }

                            ui.label(format!("{}", entry.index));
                            ui.label(&entry.type_name);
                            ui.label(partition::format_size(entry.original_size));
                            ui.label(if entry.minimum_size > 0 {
                                partition::format_size(entry.minimum_size)
                            } else {
                                "—".to_string()
                            });

                            ui.add_enabled(
                                !running,
                                egui::TextEdit::singleline(&mut entry.new_size_text)
                                    .desired_width(80.0),
                            );
                            ui.end_row();
                        }
                    });

                ui.add_space(8.0);

                // Preview / error
                if let Some(err) = &self.plan_error {
                    ui.colored_label(egui::Color32::RED, format!("Error: {err}"));
                }

                if let Some(preview) = &self.preview {
                    ui.label(egui::RichText::new("Preview:").strong());
                    egui::Grid::new("resize_preview_grid")
                        .striped(true)
                        .min_col_width(60.0)
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("#").strong());
                            ui.label(egui::RichText::new("Old Start").strong());
                            ui.label(egui::RichText::new("Old Size").strong());
                            ui.label(egui::RichText::new("→ New Start").strong());
                            ui.label(egui::RichText::new("New Size").strong());
                            ui.label(egui::RichText::new("Action").strong());
                            ui.end_row();

                            for row in preview {
                                ui.label(format!("{}", row.index));
                                ui.label(&row.old_start);
                                ui.label(&row.old_size);
                                ui.label(&row.new_start);
                                ui.label(&row.new_size);
                                ui.label(&row.action);
                                ui.end_row();
                            }
                        });
                    ui.add_space(4.0);
                }

                // Progress bar
                if let Some(ref status_arc) = self.resize_status {
                    if let Ok(s) = status_arc.lock() {
                        if !s.finished && s.total_bytes > 0 {
                            let fraction = s.current_bytes as f32 / s.total_bytes as f32;
                            let text = format!(
                                "Resizing: {} / {} ({:.0}%)",
                                partition::format_size(s.current_bytes),
                                partition::format_size(s.total_bytes),
                                fraction * 100.0,
                            );
                            ui.add(egui::ProgressBar::new(fraction).text(text).animate(true));
                        } else if !s.finished {
                            ui.horizontal(|ui| {
                                ui.spinner();
                                ui.label("Resizing...");
                            });
                        }
                    }
                }

                ui.add_space(8.0);

                // Buttons
                ui.horizontal(|ui| {
                    if ui.add_enabled(!running, egui::Button::new("Preview")).clicked() {
                        self.compute_preview();
                    }

                    let can_apply = !running
                        && self.preview.is_some()
                        && self.plan_error.is_none()
                        && (!self.is_device || self.device_warning_accepted);

                    if ui.add_enabled(can_apply, egui::Button::new("Apply")).clicked() {
                        self.start_resize(log);
                    }

                    if running && ui.button("Cancel").clicked() {
                        if let Some(ref status_arc) = self.resize_status {
                            if let Ok(mut s) = status_arc.lock() {
                                s.cancel_requested = true;
                            }
                        }
                        log.warn("Resize cancellation requested...");
                    }

                    if ui.add_enabled(!running, egui::Button::new("Close")).clicked() {
                        keep_open = false;
                    }
                });
            });

        self.open = keep_open;
        keep_open
    }

    /// Parse entries and compute the resize plan for preview.
    fn compute_preview(&mut self) {
        self.preview = None;
        self.plan_error = None;

        // Parse desired sizes from text inputs
        let mut desired_sizes = Vec::new();
        for entry in &self.entries {
            if entry.is_extended_container {
                continue;
            }
            let new_mib: u64 = match entry.new_size_text.trim().parse() {
                Ok(v) => v,
                Err(_) => {
                    self.plan_error = Some(format!(
                        "Invalid size for partition {}: '{}'",
                        entry.index, entry.new_size_text
                    ));
                    return;
                }
            };
            let new_bytes = new_mib * 1024 * 1024;

            // Validate against minimum
            if entry.minimum_size > 0 && new_bytes < entry.minimum_size {
                self.plan_error = Some(format!(
                    "Partition {} new size ({}) is below minimum ({})",
                    entry.index,
                    partition::format_size(new_bytes),
                    partition::format_size(entry.minimum_size),
                ));
                return;
            }

            if new_bytes != entry.original_size {
                desired_sizes.push((entry.index, new_bytes));
            }
        }

        if desired_sizes.is_empty() {
            self.plan_error = Some("No size changes specified.".to_string());
            return;
        }

        match compute_resize_plan(
            &self.partitions,
            &desired_sizes,
            self.alignment_sectors,
            self.disk_size_bytes,
        ) {
            Ok(plans) => {
                let preview: Vec<PreviewRow> = plans
                    .iter()
                    .map(|p| {
                        let action = if p.new_size_bytes != p.old_size_bytes && p.needs_data_move {
                            "Resize + Move"
                        } else if p.new_size_bytes != p.old_size_bytes {
                            "Resize"
                        } else if p.needs_data_move {
                            "Move"
                        } else {
                            "No change"
                        };
                        PreviewRow {
                            index: p.index,
                            old_start: format!("LBA {}", p.old_start_lba),
                            old_size: partition::format_size(p.old_size_bytes),
                            new_start: format!("LBA {}", p.new_start_lba),
                            new_size: partition::format_size(p.new_size_bytes),
                            action: action.to_string(),
                        }
                    })
                    .collect();
                self.preview = Some(preview);
            }
            Err(e) => {
                self.plan_error = Some(format!("{:#}", e));
            }
        }
    }

    /// Start the background resize thread.
    fn start_resize(&mut self, log: &mut LogPanel) {
        // Re-compute the plan to get the actual PartitionResizePlan structs
        let mut desired_sizes = Vec::new();
        for entry in &self.entries {
            if entry.is_extended_container {
                continue;
            }
            let new_mib: u64 = match entry.new_size_text.trim().parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let new_bytes = new_mib * 1024 * 1024;
            if new_bytes != entry.original_size {
                desired_sizes.push((entry.index, new_bytes));
            }
        }

        let plans = match compute_resize_plan(
            &self.partitions,
            &desired_sizes,
            self.alignment_sectors,
            self.disk_size_bytes,
        ) {
            Ok(p) => p,
            Err(e) => {
                log.error(format!("Failed to compute resize plan: {:#}", e));
                return;
            }
        };

        // Calculate total bytes that need to move for progress
        let total_bytes: u64 = plans
            .iter()
            .filter(|p| p.needs_data_move)
            .map(|p| p.old_size_bytes)
            .sum();

        let status = Arc::new(Mutex::new(ResizeStatus {
            finished: false,
            error: None,
            log_messages: Vec::new(),
            current_bytes: 0,
            total_bytes,
            cancel_requested: false,
        }));
        self.resize_status = Some(Arc::clone(&status));

        let path = self.source_path.clone();
        let table = self.partition_table.clone();
        let is_device = self.is_device;
        let disk_size = self.disk_size_bytes;

        log.info(format!(
            "Starting partition resize on {}...",
            path.display()
        ));

        std::thread::spawn(move || {
            let status2 = Arc::clone(&status);
            let status3 = Arc::clone(&status);

            let result = (|| -> anyhow::Result<()> {
                // Open the file/device for read+write
                let mut file = if is_device {
                    let handle = rusty_backup::os::open_target_for_writing(&path)?;
                    handle.file
                } else {
                    std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&path)?
                };

                // Detect VHD
                let file_size = file.seek(std::io::SeekFrom::End(0))?;
                file.seek(std::io::SeekFrom::Start(0))?;
                let is_vhd = detect_vhd(&mut file, file_size);

                apply_resize(
                    &mut file,
                    &plans,
                    &table,
                    is_device,
                    is_vhd,
                    disk_size,
                    &mut |current, total| {
                        if let Ok(mut s) = status2.lock() {
                            s.current_bytes = current;
                            s.total_bytes = total;
                        }
                    },
                    &mut |msg| {
                        if let Ok(mut s) = status3.lock() {
                            s.log_messages.push(msg.to_string());
                        }
                    },
                )?;

                // Truncate file if needed (we couldn't do this inside apply_resize
                // with a generic impl, but here we have a real File)
                if !is_device {
                    let new_data_end = plans
                        .iter()
                        .map(|p| p.new_start_lba * 512 + p.new_size_bytes)
                        .max()
                        .unwrap_or(disk_size);

                    let old_data_size = if is_vhd {
                        disk_size.saturating_sub(512)
                    } else {
                        disk_size
                    };

                    if new_data_end < old_data_size {
                        let target = if is_vhd {
                            new_data_end + 512
                        } else {
                            new_data_end
                        };
                        file.set_len(target)?;
                    }
                }

                Ok(())
            })();

            if let Ok(mut s) = status.lock() {
                s.finished = true;
                if let Err(e) = result {
                    s.error = Some(format!("{:#}", e));
                }
            }
        });
    }
}
