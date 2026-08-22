//! Popup window for in-place partition resizing.

use std::collections::HashMap;
use std::io::Seek;
use std::sync::{Arc, Mutex};

use rusty_backup::device::DiskDevice;
use rusty_backup::model::status::ResizeStatus;
use rusty_backup::partition::resize::{apply_resize, compute_resize_plan, detect_vhd};
use rusty_backup::partition::{self, PartitionInfo, PartitionTable};

use super::progress::LogPanel;

/// Per-partition entry in the resize grid.
struct ResizeEntry {
    index: usize,
    type_name: String,
    original_size: u64,
    minimum_size: u64,
    is_extended_container: bool,
    /// User-editable new size text in MiB.
    new_size_text: String,
    /// Quick-pick radio (Original / Minimum / Custom). Selecting a non-Custom
    /// option re-stamps `new_size_text`; selecting Custom seeds it from the
    /// current text and otherwise leaves it editable.
    choice: rusty_backup::model::size_mode::SizeMode,
}

/// The size the user asked for, in bytes, or `None` when the field cannot
/// be read as a number.
///
/// **One parser for the preview and the apply.** They used to have their
/// own: the preview read the field as `f64` and sector-aligned it, while
/// `start_resize` read it as `u64` and `continue`d past anything that
/// failed. Since the field is seeded — and re-stamped by the Original /
/// Minimum radios — with two decimals, every entry whose size is not a
/// whole number of MiB was silently dropped from the apply's
/// `desired_sizes`. The plan then contained no changes, `apply_resize`
/// wrote nothing, and the run reported success: the preview showed the
/// resize and Apply did nothing.
///
/// That is every SGI / EFS partition, whose sizes are cylinder-rounded
/// (197.90 MiB, 91.34 MiB), which is why this survived on FAT volumes sized
/// to a clean 100 MiB.
fn parsed_new_size(entry: &ResizeEntry) -> Option<u64> {
    let text = entry.new_size_text.trim();
    // A field still showing the original's formatted size means "no
    // change" — the "{:.2}" round-trip otherwise shifts the size by up to
    // ~5 KiB on a partition whose byte count isn't a clean MiB multiple.
    let orig_text = format!("{:.2}", entry.original_size as f64 / (1024.0 * 1024.0));
    if text == orig_text {
        return Some(entry.original_size);
    }
    let mib: f64 = text.parse().ok()?;
    if !mib.is_finite() || mib < 0.0 {
        return None;
    }
    let bytes = (mib * 1024.0 * 1024.0) as u64;
    Some((bytes / 512) * 512)
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

/// What the dialog needs to know about the thing it is resizing, as opposed
/// to the partitions inside it.
pub struct ContainerInfo {
    /// Current capacity in bytes — an image file's length or a device's
    /// size. **Not** the end of the last partition: an image with slack at
    /// the end has room the dialog should be able to hand out.
    pub size_bytes: u64,
    /// A physical device: fixed size, and gated behind the risk checkbox.
    pub is_device: bool,
    /// Can be enlarged in place. A plain image file can; a device cannot,
    /// and neither can a compressed container like a CHD.
    pub can_grow: bool,
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
    /// Rate / ETA estimator for the in-progress resize bar.
    resize_rate: super::progress::RateTracker,
    /// Alignment in sectors (0 = no alignment).
    alignment_sectors: u64,
    /// The container's capacity in bytes — an image file's length or a
    /// device's size, and the bound the new layout must fit inside.
    ///
    /// **Not** the end of the last partition, which is what this used to be:
    /// an image with slack at the end reported itself full, so a partition
    /// could not be grown into space that already existed, and enlarging the
    /// file beforehand did not help either because reopening recomputed the
    /// same bound from the same partition table.
    disk_size_bytes: u64,
    /// Whether the container could be enlarged (by "Expand Image..." on the
    /// Inspect tab — this dialog only lays out what already exists). Drives
    /// the hint pointing there.
    can_grow_container: bool,
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
        container: ContainerInfo,
        source_path: std::path::PathBuf,
    ) -> Self {
        let ContainerInfo {
            size_bytes: disk_size_bytes,
            is_device,
            can_grow: can_grow_container,
        } = container;
        let entries = partitions
            .iter()
            .filter(|p| !p.is_logical)
            .map(|p| {
                let min_size = partition_min_sizes
                    .get(&p.index)
                    .copied()
                    .unwrap_or(0)
                    .max(512); // at least one sector
                let size_mib = p.size_bytes as f64 / (1024.0 * 1024.0);
                ResizeEntry {
                    index: p.index,
                    type_name: p.type_name.clone(),
                    original_size: p.size_bytes,
                    minimum_size: min_size,
                    is_extended_container: p.is_extended_container,
                    // Two-decimal MiB matches the partition editor's display
                    // and avoids the "new size is silently lower than current"
                    // bug for partitions whose byte count isn't a clean MiB
                    // multiple (anything not aligned to 1 MiB).
                    new_size_text: format!("{:.2}", size_mib),
                    choice: rusty_backup::model::size_mode::SizeMode::Original,
                }
            })
            .collect();

        Self {
            entries,
            preview: None,
            plan_error: None,
            resize_status: None,
            resize_rate: super::progress::RateTracker::default(),
            alignment_sectors,
            disk_size_bytes,
            can_grow_container,
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

    /// The entry's planned size for *display* (the live layout bar), falling
    /// back to `original_size` on a malformed value — a half-typed number
    /// shouldn't make the bar jump. Shares [`parsed_new_size`] so the bar
    /// can't disagree with the preview and the apply.
    fn planned_size_bytes(&self, entry: &ResizeEntry) -> u64 {
        parsed_new_size(entry).unwrap_or(entry.original_size)
    }

    /// Render the Current vs After PartitionBar pair using the working
    /// edits. Both bars share the byte-per-pixel scale so growth/shrink is
    /// visible.
    fn show_disk_layout_bars(&self, ui: &mut egui::Ui) {
        use super::partition_bar::{PartitionBar, Segment, SegmentKind};

        // "Current" — same logic as the inspect-tab disk-layout builder,
        // ignoring extended-container entries and assigning sequential color
        // indices.
        let mut color_index = 0usize;
        let mut current: Vec<Segment> = Vec::new();
        for p in &self.partitions {
            if p.is_extended_container || p.is_logical {
                continue;
            }
            let kind = SegmentKind::Partition { color_index };
            color_index += 1;
            current.push(Segment {
                label: format!("Partition {}", p.index + 1),
                fs: p.type_name.clone(),
                size_bytes: p.size_bytes,
                kind,
            });
        }

        // "After" — same partitions in the same order, but `size_bytes` reads
        // from `planned_size_bytes`. Color indices match Current.
        let mut after: Vec<Segment> = Vec::new();
        color_index = 0;
        for p in &self.partitions {
            if p.is_extended_container || p.is_logical {
                continue;
            }
            let new_size = self
                .entries
                .iter()
                .find(|e| e.index == p.index)
                .map(|e| self.planned_size_bytes(e))
                .unwrap_or(p.size_bytes);
            let kind = SegmentKind::Partition { color_index };
            color_index += 1;
            after.push(Segment {
                label: format!("Partition {}", p.index + 1),
                fs: p.type_name.clone(),
                size_bytes: new_size,
                kind,
            });
        }

        let current_total: u64 = current.iter().map(|s| s.size_bytes).sum();
        let after_total: u64 = after.iter().map(|s| s.size_bytes).sum();
        let max_total = current_total.max(after_total).max(1);

        ui.label("Current:");
        let available_width = ui.available_width().max(120.0);
        let current_w = available_width * (current_total as f64 / max_total as f64) as f32;
        ui.scope(|ui| {
            ui.set_width(current_w.max(60.0));
            PartitionBar {
                segments: current,
                show_inline_labels: true,
                show_legend: false,
            }
            .show(ui);
        });

        ui.add_space(4.0);
        ui.label(format!(
            "After  ({} -> {}):",
            partition::format_size(current_total),
            partition::format_size(after_total),
        ));
        let after_w = available_width * (after_total as f64 / max_total as f64) as f32;
        ui.scope(|ui| {
            ui.set_width(after_w.max(60.0));
            PartitionBar {
                segments: after,
                show_inline_labels: true,
                show_legend: true,
            }
            .show(ui);
        });
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
                            super::theme::danger(ui.visuals()),
                            "WARNING: This will directly modify the physical device. Data loss is permanent and irreversible!",
                        );
                        ui.checkbox(
                            &mut self.device_warning_accepted,
                            "I understand the risks and want to proceed",
                        );
                    });
                    ui.add_space(4.0);
                }

                // The disk's capacity, and where to get more of it. Growing
                // the container is "Expand Image..." on the Inspect tab —
                // it handles raw, VHD and CHD, so this dialog does not
                // duplicate it with a lesser version.
                ui.horizontal(|ui| {
                    ui.label(
                        egui::RichText::new(format!(
                            "Disk size: {}",
                            partition::format_size(self.disk_size_bytes)
                        ))
                        .weak(),
                    );
                    if self.can_grow_container {
                        ui.label(
                            egui::RichText::new(
                                "- to make more room, close this and use \"Expand Image...\"",
                            )
                            .weak(),
                        );
                    }
                });
                ui.add_space(4.0);

                // Before / After disk layout visualization. "Current" reads
                // sizes from `partitions`; "After" applies the working
                // `new_size_text` per entry. Both bars share a byte-per-pixel
                // scale, so the After bar grows/shrinks visibly with the
                // total of edits.
                self.show_disk_layout_bars(ui);
                ui.add_space(8.0);

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
                        ui.label(egui::RichText::new("Size Mode").strong());
                        ui.label(egui::RichText::new("New Size (MiB)").strong());
                        ui.end_row();

                        for entry in &mut self.entries {
                            if entry.is_extended_container {
                                // Show grayed out, not editable
                                ui.colored_label(super::theme::muted(ui.visuals()), format!("{}", entry.index));
                                ui.colored_label(
                                    super::theme::muted(ui.visuals()),
                                    format!("{} (extended)", entry.type_name),
                                );
                                ui.colored_label(
                                    super::theme::muted(ui.visuals()),
                                    partition::format_size(entry.original_size),
                                );
                                ui.colored_label(super::theme::muted(ui.visuals()), "—");
                                ui.colored_label(super::theme::muted(ui.visuals()), "—");
                                ui.colored_label(super::theme::muted(ui.visuals()), "—");
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

                            // Size-mode radios. Selecting Original/Minimum
                            // stamps `new_size_text` to the canonical MiB
                            // string for that target; Custom keeps the user's
                            // edits alive and lets the text field stay
                            // editable for free-form entry.
                            use rusty_backup::model::size_mode::SizeMode;
                            let prev = entry.choice;
                            ui.add_enabled_ui(!running, |ui| {
                                ui.horizontal(|ui| {
                                    ui.radio_value(&mut entry.choice, SizeMode::Original, "Original");
                                    if entry.minimum_size > 0
                                        && entry.minimum_size < entry.original_size
                                    {
                                        ui.radio_value(
                                            &mut entry.choice,
                                            SizeMode::Minimum,
                                            "Minimum",
                                        );
                                    }
                                    ui.radio_value(&mut entry.choice, SizeMode::Custom, "Custom");
                                });
                            });
                            if entry.choice != prev {
                                match entry.choice {
                                    SizeMode::Original => {
                                        entry.new_size_text = format!(
                                            "{:.2}",
                                            entry.original_size as f64 / (1024.0 * 1024.0),
                                        );
                                    }
                                    SizeMode::Minimum => {
                                        entry.new_size_text = format!(
                                            "{:.2}",
                                            entry.minimum_size as f64 / (1024.0 * 1024.0),
                                        );
                                    }
                                    _ => {}
                                }
                            }

                            ui.add_enabled(
                                !running && entry.choice == SizeMode::Custom,
                                egui::TextEdit::singleline(&mut entry.new_size_text)
                                    .desired_width(80.0),
                            );
                            ui.end_row();
                        }
                    });

                ui.add_space(8.0);

                // Preview / error
                if let Some(err) = &self.plan_error {
                    ui.colored_label(super::theme::danger(ui.visuals()), format!("Error: {err}"));
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
                            ui.label(egui::RichText::new("New Start").strong());
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
                            self.resize_rate.record(s.current_bytes, "Resizing");
                            let fraction = s.current_bytes as f32 / s.total_bytes as f32;
                            let suffix = self.resize_rate.suffix(s.current_bytes, s.total_bytes);
                            let text = format!(
                                "Resizing: {} / {} ({:.0}%){}",
                                partition::format_size(s.current_bytes),
                                partition::format_size(s.total_bytes),
                                fraction * 100.0,
                                suffix,
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
            let new_bytes = match parsed_new_size(entry) {
                Some(b) => b,
                None => {
                    self.plan_error = Some(format!(
                        "Invalid size for partition {}: '{}'",
                        entry.index, entry.new_size_text
                    ));
                    return;
                }
            };

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
        // Re-compute the plan to get the actual PartitionResizePlan structs.
        // Parses through the same helper the preview uses — see
        // `parsed_new_size` for why they must not diverge.
        let mut desired_sizes = Vec::new();
        for entry in &self.entries {
            if entry.is_extended_container {
                continue;
            }
            let new_bytes = match parsed_new_size(entry) {
                Some(b) => b,
                None => {
                    log.error(format!(
                        "Invalid size for partition {}: '{}' — nothing was written.",
                        entry.index, entry.new_size_text
                    ));
                    return;
                }
            };
            if new_bytes != entry.original_size {
                desired_sizes.push((entry.index, new_bytes));
            }
        }
        // Refuse rather than "succeed" having written nothing — the old code
        // reached `apply_resize` with an empty plan and reported success.
        if desired_sizes.is_empty() {
            log.error("No size changes to apply.".to_string());
            return;
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
        self.resize_rate.reset();

        let path = self.source_path.clone();
        let table = self.partition_table.clone();
        let is_device = self.is_device;
        let disk_size = self.disk_size_bytes;

        log.info(format!(
            "Starting partition resize on {}...",
            path.display()
        ));

        std::thread::spawn(move || {
            let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: partition resize");
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_backup::model::size_mode::SizeMode;

    fn entry(original_size: u64, text: &str) -> ResizeEntry {
        ResizeEntry {
            index: 0,
            type_name: "SGI EFS".into(),
            original_size,
            minimum_size: 512,
            is_extended_container: false,
            new_size_text: text.into(),
            choice: SizeMode::Custom,
        }
    }

    /// The size the popup seeds and re-stamps has two decimals, and the
    /// apply path used to parse it as `u64` — so every partition whose size
    /// is not a whole number of MiB was dropped, the plan came out empty,
    /// and the resize "succeeded" without writing anything. An SGI/EFS
    /// partition is cylinder-rounded, so that was all of them.
    #[test]
    fn a_fractional_mib_size_is_parsed_not_dropped() {
        // 100 MiB shrink target typed against a 197.90 MiB EFS partition.
        let e = entry(207_470_592, "91.34");
        let got = parsed_new_size(&e).expect("a two-decimal size must parse");
        assert_eq!(got, (((91.34 * 1024.0 * 1024.0) as u64) / 512) * 512);
        assert_ne!(got, e.original_size, "must register as a change");
    }

    /// The Original / Minimum radios stamp `{:.2}`; picking Original must
    /// read back as exactly the original size, not a value ~5 KiB off that
    /// the plan would treat as a shrink.
    #[test]
    fn the_original_stamp_round_trips_to_the_exact_original_size() {
        let original = 207_470_592u64;
        let stamped = format!("{:.2}", original as f64 / (1024.0 * 1024.0));
        assert_eq!(parsed_new_size(&entry(original, &stamped)), Some(original));
    }

    #[test]
    fn sizes_are_sector_aligned_and_junk_is_rejected() {
        // 1.0009765625 MiB = 1 MiB + 1 KiB; already sector-aligned.
        assert_eq!(
            parsed_new_size(&entry(999, "1.0009765625")),
            Some(1_049_600)
        );
        assert_eq!(parsed_new_size(&entry(999, "")), None);
        assert_eq!(parsed_new_size(&entry(999, "big")), None);
        assert_eq!(parsed_new_size(&entry(999, "-5")), None);
    }
}
