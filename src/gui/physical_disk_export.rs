//! "Physical Disk Export" sub-window launched from the Inspect tab's
//! Export Disk Image popup.
//!
//! Lets the user stream the currently-loaded raw image directly to a physical
//! disk. When the source is a superfloppy (no partition table), surfaces a
//! "Synthesize partition table" group with table kind / alignment /
//! fs-type-byte / partition size controls so emulators and BIOS-era machines
//! can recognize the result. For sources that already have an MBR/GPT/APM/SGI
//! table, the synthesize group is grayed and the action becomes a byte-for-byte
//! direct write.
//!
//! Engine code lives in `restore::superfloppy_wrap`; threading and Status
//! lives in `model::physical_write_runner`. This file is the view only — it
//! holds form state, polls the runner's Status, and renders.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusty_backup::device::DiskDevice;
use rusty_backup::model::physical_write_runner::{
    self, PhysicalWriteRequest, PhysicalWriteSource, PhysicalWriteStatus,
};
use rusty_backup::model::size_mode::SizeMode;
use rusty_backup::partition::PartitionTable;
use rusty_backup::restore::superfloppy_wrap::{self, WrapAlignment, WrapParams, WrapTable};

use super::context::TabContext;
use super::size_mode_row::{size_mode_row, SizeModeRowOptions};

/// Information about the currently-loaded source needed to drive the
/// sub-window. The Inspect tab assembles this on demand.
#[derive(Clone)]
pub struct PhysicalDiskExportSource {
    pub path: PathBuf,
    pub size_bytes: u64,
    pub fs_hint: String,
    pub has_partition_table: bool,
}

/// Table-kind choice in the UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableKind {
    Mbr,
    Gpt,
}

/// Alignment choice in the UI. Matches `WrapAlignment` but is a plain enum
/// for radio binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AlignmentChoice {
    DosTraditional,
    Modern1MB,
    Custom,
}

/// Sub-window state. Held by `InspectTab`.
pub struct PhysicalDiskExport {
    pub open: bool,
    /// Index into `ctx.devices` for the chosen target.
    selected_device: Option<usize>,
    confirm_text: String,
    synthesize: bool,
    table_kind: TableKind,
    alignment: AlignmentChoice,
    custom_alignment_sectors: u64,
    type_byte: u8,
    size_mode: SizeMode,
    custom_size_mib: u32,
    write_status: Option<Arc<Mutex<PhysicalWriteStatus>>>,
    /// Rate / ETA estimator for the disk-write progress bar.
    write_rate: super::progress::RateTracker,
    /// Source snapshot taken when the sub-window was opened.
    source: Option<PhysicalDiskExportSource>,
    last_error: Option<String>,
}

impl Default for PhysicalDiskExport {
    fn default() -> Self {
        Self {
            open: false,
            selected_device: None,
            confirm_text: String::new(),
            synthesize: false,
            table_kind: TableKind::Mbr,
            alignment: AlignmentChoice::Modern1MB,
            custom_alignment_sectors: 2048,
            type_byte: 0x07,
            size_mode: SizeMode::Original,
            custom_size_mib: 0,
            write_status: None,
            write_rate: super::progress::RateTracker::default(),
            source: None,
            last_error: None,
        }
    }
}

/// Action returned to the parent each frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Stay,
    Close,
}

impl PhysicalDiskExport {
    /// Open the sub-window for a given source. Seeds defaults from the
    /// heuristic when the source is a superfloppy.
    pub fn open_for(
        &mut self,
        source: PhysicalDiskExportSource,
        partition_table: Option<&PartitionTable>,
    ) {
        let hidden_sectors = partition_table.and_then(|pt| match pt {
            PartitionTable::None { .. } => detect_hidden_sectors_from_source(&source),
            _ => None,
        });

        let suggested = superfloppy_wrap::suggest_wrap_params(
            &source.fs_hint,
            source.size_bytes,
            // We don't know the target size yet — pass source size so the
            // heuristic doesn't force GPT prematurely. UI re-runs the
            // forced-GPT check once the user picks a device.
            source.size_bytes.max(1),
            hidden_sectors,
        );
        let (kind, type_byte, align) = match suggested.table {
            WrapTable::Mbr {
                type_byte,
                alignment,
                ..
            } => (TableKind::Mbr, type_byte, alignment),
            WrapTable::Gpt { alignment, .. } => (
                TableKind::Gpt,
                superfloppy_wrap::mbr_type_byte_for_fs(&source.fs_hint),
                alignment,
            ),
        };
        self.synthesize = !source.has_partition_table;
        self.table_kind = kind;
        self.alignment = match align {
            WrapAlignment::DosTraditional => AlignmentChoice::DosTraditional,
            WrapAlignment::Modern1MB => AlignmentChoice::Modern1MB,
            WrapAlignment::Custom(n) => {
                self.custom_alignment_sectors = n;
                AlignmentChoice::Custom
            }
        };
        self.type_byte = type_byte;
        self.size_mode = SizeMode::Original;
        self.custom_size_mib = (source.size_bytes / (1024 * 1024)).max(1) as u32;
        self.source = Some(source);
        self.confirm_text.clear();
        self.write_status = None;
        self.last_error = None;
        self.open = true;
    }

    pub fn show(&mut self, egui_ctx: &egui::Context, tab_ctx: &TabContext) -> Action {
        if !self.open {
            return Action::Close;
        }
        // Snapshot the source so we don't borrow self mutably during render.
        let source = match self.source.clone() {
            Some(s) => s,
            None => {
                self.open = false;
                return Action::Close;
            }
        };

        let mut close_requested = false;
        egui::Window::new("Physical Disk Export")
            .collapsible(false)
            .resizable(true)
            .default_width(560.0)
            .show(egui_ctx, |ui| {
                self.render_source_block(ui, &source);
                ui.separator();

                if self.write_status.is_some() {
                    self.render_progress(ui);
                    if ui.button("Close").clicked() && self.write_finished() {
                        close_requested = true;
                    }
                    return;
                }

                self.render_device_picker(ui, tab_ctx.devices);
                ui.add_space(4.0);

                ui.add_enabled_ui(!source.has_partition_table, |ui| {
                    let group_label = if source.has_partition_table {
                        "Synthesize partition table (source already has one — disabled)"
                    } else {
                        "Synthesize partition table"
                    };
                    ui.checkbox(&mut self.synthesize, group_label);
                    ui.add_enabled_ui(self.synthesize, |ui| {
                        self.render_synthesize_controls(ui, &source);
                    });
                });

                if let Some(err) = &self.last_error {
                    ui.add_space(4.0);
                    ui.colored_label(egui::Color32::from_rgb(220, 70, 70), err);
                }

                ui.separator();
                self.render_action_row(ui, tab_ctx.devices, &source, &mut close_requested);
            });

        if close_requested {
            self.open = false;
            return Action::Close;
        }
        Action::Stay
    }

    fn render_source_block(&self, ui: &mut egui::Ui, source: &PhysicalDiskExportSource) {
        ui.label(egui::RichText::new("Source").strong());
        ui.label(format!("Path: {}", source.path.display()));
        let table_desc = if source.has_partition_table {
            "has partition table"
        } else {
            "superfloppy (no partition table)"
        };
        ui.label(format!(
            "Size: {} bytes  -  Filesystem: {}  -  {}",
            source.size_bytes, source.fs_hint, table_desc,
        ));
    }

    fn render_device_picker(&mut self, ui: &mut egui::Ui, devices: &[DiskDevice]) {
        ui.horizontal(|ui| {
            ui.label("Target disk:");
            let current_label = self
                .selected_device
                .and_then(|i| devices.get(i))
                .map(|d| d.display_name())
                .unwrap_or_else(|| "Select a device...".to_string());
            egui::ComboBox::from_id_salt("physical_disk_export_target")
                .selected_text(current_label)
                .width(380.0)
                .show_ui(ui, |ui| {
                    for (i, device) in devices.iter().enumerate() {
                        ui.selectable_value(
                            &mut self.selected_device,
                            Some(i),
                            device.display_name(),
                        );
                    }
                });
        });
    }

    fn render_synthesize_controls(&mut self, ui: &mut egui::Ui, source: &PhysicalDiskExportSource) {
        ui.horizontal(|ui| {
            ui.label("Table:");
            ui.radio_value(&mut self.table_kind, TableKind::Mbr, "MBR");
            ui.radio_value(&mut self.table_kind, TableKind::Gpt, "GPT");
        });

        ui.horizontal(|ui| {
            ui.label("Alignment:");
            ui.radio_value(
                &mut self.alignment,
                AlignmentChoice::DosTraditional,
                "DOS legacy (LBA 63)",
            );
            ui.radio_value(
                &mut self.alignment,
                AlignmentChoice::Modern1MB,
                "Modern (LBA 2048)",
            );
            ui.radio_value(&mut self.alignment, AlignmentChoice::Custom, "Custom");
            if self.alignment == AlignmentChoice::Custom {
                ui.add(
                    egui::DragValue::new(&mut self.custom_alignment_sectors)
                        .speed(1.0)
                        .range(1..=u64::MAX)
                        .suffix(" sectors"),
                );
            }
        });

        ui.horizontal(|ui| {
            ui.label("Partition type:");
            type_byte_picker(ui, &mut self.type_byte, &source.fs_hint);
        });

        ui.horizontal(|ui| {
            ui.label("Partition size:");
            // Treat source size as both "original" and "minimum" — there's no
            // compaction in this path; the user can grow but can't shrink past
            // the source data.
            size_mode_row(
                ui,
                &mut self.size_mode,
                &mut self.custom_size_mib,
                source.size_bytes,
                source.size_bytes,
                SizeModeRowOptions {
                    allow_fill: true,
                    ..Default::default()
                },
            );
        });
    }

    fn render_action_row(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[DiskDevice],
        source: &PhysicalDiskExportSource,
        close_requested: &mut bool,
    ) {
        let device = self.selected_device.and_then(|i| devices.get(i));
        let ready = device.is_some();
        let device_name = device
            .map(|d| d.display_name())
            .unwrap_or_else(|| "(no device)".to_string());

        ui.label(egui::RichText::new(format!(
            "Confirm: type the device name `{}` to enable Write",
            device_name
        )))
        .on_hover_text("Writing erases all data on the selected device.");
        ui.add_enabled(
            ready,
            egui::TextEdit::singleline(&mut self.confirm_text).hint_text(&device_name),
        );

        let confirmed = ready && self.confirm_text == device_name;
        ui.horizontal(|ui| {
            let write_btn = ui.add_enabled(confirmed, egui::Button::new("Write to disk"));
            if write_btn.clicked() {
                if let Some(dev) = device {
                    self.start_write(source, dev);
                }
            }
            if ui.button("Cancel").clicked() {
                *close_requested = true;
            }
        });
    }

    fn render_progress(&mut self, ui: &mut egui::Ui) {
        let snapshot = self.write_status.as_ref().and_then(|s| {
            s.lock().ok().map(|g| {
                (
                    g.finished,
                    g.error.clone(),
                    g.current_bytes,
                    g.total_bytes,
                    g.cancel_requested,
                )
            })
        });
        if let Some((finished, error, current, total, cancel_requested)) = snapshot {
            let frac = if total > 0 {
                (current as f32 / total as f32).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let text = if total > 0 {
                self.write_rate.record(current, "Writing to disk");
                let suffix = self.write_rate.suffix(current, total);
                format!(
                    "{} / {} ({:.0}%){}",
                    rusty_backup::partition::format_size(current),
                    rusty_backup::partition::format_size(total),
                    frac * 100.0,
                    suffix,
                )
            } else {
                "preparing...".to_string()
            };
            ui.add(egui::ProgressBar::new(frac).text(text));
            if finished {
                if let Some(err) = error {
                    ui.colored_label(
                        egui::Color32::from_rgb(220, 70, 70),
                        format!("Error: {err}"),
                    );
                } else {
                    ui.label("Write complete.");
                }
            } else if !cancel_requested {
                if ui.button("Cancel write").clicked() {
                    if let Some(s) = &self.write_status {
                        if let Ok(mut g) = s.lock() {
                            g.cancel_requested = true;
                        }
                    }
                }
            } else {
                ui.label("Cancelling...");
            }
        }
    }

    fn write_finished(&self) -> bool {
        self.write_status
            .as_ref()
            .and_then(|s| s.lock().ok().map(|g| g.finished))
            .unwrap_or(false)
    }

    fn start_write(&mut self, source: &PhysicalDiskExportSource, device: &DiskDevice) {
        let target_size = device.size_bytes;
        let first_lba = self.resolved_alignment().first_partition_lba();
        let first_byte = first_lba * 512;

        // Validate partition size fits.
        let partition_size =
            self.resolved_partition_size(source.size_bytes, target_size, first_byte);
        if partition_size < source.size_bytes {
            self.last_error = Some(format!(
                "Partition size ({} bytes) is smaller than source filesystem ({} bytes)",
                partition_size, source.size_bytes,
            ));
            return;
        }
        if first_byte + partition_size > target_size {
            self.last_error = Some(format!(
                "Partition end ({} bytes) exceeds target disk size ({} bytes)",
                first_byte + partition_size,
                target_size,
            ));
            return;
        }

        let wrap = if self.synthesize {
            Some(self.build_wrap_params(source, partition_size, target_size))
        } else {
            None
        };
        let req = PhysicalWriteRequest {
            source: PhysicalWriteSource::RawFile(source.path.clone()),
            target_device_path: device.path.clone(),
            target_size_bytes: target_size,
            wrap,
        };
        self.last_error = None;
        self.write_status = Some(physical_write_runner::start_physical_write(req));
        self.write_rate.reset();
    }

    fn resolved_alignment(&self) -> WrapAlignment {
        match self.alignment {
            AlignmentChoice::DosTraditional => WrapAlignment::DosTraditional,
            AlignmentChoice::Modern1MB => WrapAlignment::Modern1MB,
            AlignmentChoice::Custom => WrapAlignment::Custom(self.custom_alignment_sectors),
        }
    }

    fn resolved_partition_size(&self, source_size: u64, target_size: u64, first_byte: u64) -> u64 {
        let chosen = self
            .size_mode
            .effective_size(source_size, source_size, self.custom_size_mib);
        let raw = if self.size_mode == SizeMode::FillRemaining {
            target_size.saturating_sub(first_byte)
        } else {
            chosen
        };
        // Round down to sector boundary.
        (raw / 512) * 512
    }

    fn build_wrap_params(
        &self,
        source: &PhysicalDiskExportSource,
        partition_size: u64,
        target_size: u64,
    ) -> WrapParams {
        let alignment = self.resolved_alignment();
        let table = match self.table_kind {
            TableKind::Mbr => WrapTable::Mbr {
                type_byte: self.type_byte,
                bootable: false,
                alignment,
            },
            TableKind::Gpt => {
                let suggested = superfloppy_wrap::suggest_wrap_params(
                    &source.fs_hint,
                    source.size_bytes,
                    target_size,
                    None,
                );
                match suggested.table {
                    WrapTable::Gpt {
                        type_guid, name, ..
                    } => WrapTable::Gpt {
                        type_guid,
                        name,
                        alignment,
                    },
                    // Suggest fell back to MBR; force GPT with a default GUID
                    // by rebuilding from the heuristic with a forced-large
                    // target. Should not happen in practice.
                    WrapTable::Mbr { .. } => {
                        let forced = superfloppy_wrap::suggest_wrap_params(
                            &source.fs_hint,
                            source.size_bytes,
                            u64::MAX,
                            None,
                        );
                        match forced.table {
                            WrapTable::Gpt {
                                type_guid, name, ..
                            } => WrapTable::Gpt {
                                type_guid,
                                name,
                                alignment,
                            },
                            _ => unreachable!(),
                        }
                    }
                }
            }
        };
        WrapParams {
            table,
            partition_size_bytes: partition_size,
            target_size_bytes: target_size,
            source_fs_hint: source.fs_hint.clone(),
        }
    }
}

/// Render a partition-type-byte combo: a dropdown of common types plus a raw
/// hex DragValue for override.
fn type_byte_picker(ui: &mut egui::Ui, type_byte: &mut u8, fs_hint: &str) {
    let presets: &[(u8, &str)] = &[
        (0x01, "0x01 FAT12"),
        (0x06, "0x06 FAT16"),
        (0x07, "0x07 NTFS/exFAT"),
        (0x0B, "0x0B FAT32 (CHS)"),
        (0x0C, "0x0C FAT32 (LBA)"),
        (0x83, "0x83 Linux"),
        (0xAF, "0xAF HFS/HFS+"),
    ];
    let label = presets
        .iter()
        .find(|(b, _)| b == type_byte)
        .map(|(_, n)| (*n).to_string())
        .unwrap_or_else(|| format!("0x{:02X} (custom)", type_byte));
    egui::ComboBox::from_id_salt("physical_disk_export_typebyte")
        .selected_text(label)
        .show_ui(ui, |ui| {
            for (byte, name) in presets {
                ui.selectable_value(type_byte, *byte, *name);
            }
        });
    ui.label("or");
    let mut hex_val = *type_byte;
    if ui
        .add(
            egui::DragValue::new(&mut hex_val)
                .speed(1.0)
                .range(0u8..=255u8)
                .hexadecimal(2, false, true),
        )
        .changed()
    {
        *type_byte = hex_val;
    }
    let _ = fs_hint; // hint used only by `open_for`; kept here for future tooltip
}

/// Probe the source's VBR for the `hidden_sectors` field at offset 0x1C.
/// Reads only the first 512 bytes; returns `None` on any error.
fn detect_hidden_sectors_from_source(source: &PhysicalDiskExportSource) -> Option<u32> {
    use std::fs::File;
    use std::io::Read;
    let mut f = File::open(&source.path).ok()?;
    let mut sector = [0u8; 512];
    f.read_exact(&mut sector).ok()?;
    Some(u32::from_le_bytes(sector[0x1C..0x20].try_into().ok()?))
}
