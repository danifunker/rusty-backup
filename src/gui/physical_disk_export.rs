//! The "Physical Disk Export" window: a shell over `controller::disk_write`.
//!
//! The Inspect tab's Export popup and the Restore tab's Write Image File hand it an image; it
//! draws the controller's source, target, partition list and synthesize form, maps widgets to
//! [`DiskWriteAction`]s, answers the device-name prompt through an [`EffectHost`] and shows the
//! write's job. Rules and the write live in the controller (docs/phase2_commander_plan.md
//! § "Slice 14").

use rusty_backup::controller::disk_write::{
    format_size, DiskWriteAction, DiskWriteController, DiskWriteServices, JobRegistry, SizeMode,
    TableKind, WrapAlign, WriteOpen, TYPE_BYTE_PRESETS,
};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{Controller, Effect};

use super::context::TabContext;
use super::effects::{EffectHost, Reply};
use super::size_mode_row::{size_mode_row, SizeModeRowOptions};

pub struct PhysicalDiskExport {
    ctl: DiskWriteController,
    effects: EffectHost,
    pending_fx: Vec<Effect>,
}

impl Default for PhysicalDiskExport {
    fn default() -> Self {
        Self {
            ctl: DiskWriteController::new(DiskWriteServices {
                jobs: JobRegistry::new(),
                notify: std::sync::Arc::new(super::ui_logger::EguiWake),
            }),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
        }
    }
}

impl PhysicalDiskExport {
    /// A device write is in flight; the hosting tab's controls stay locked.
    pub fn is_writing(&self) -> bool {
        self.ctl.is_writing()
    }

    /// Open the window for an image another screen hands over.
    pub fn open_for(&mut self, open: WriteOpen) {
        self.act(DiskWriteAction::Open(open));
    }

    fn act(&mut self, action: DiskWriteAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
    }

    /// Drain the controller (also while the window is hidden), so a write's end is seen.
    pub fn poll(&mut self, tab_ctx: &mut TabContext) {
        self.ctl.reconcile_devices(tab_ctx.devices);
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        for (level, msg) in self.ctl.take_log() {
            match level {
                LogLevel::Info => tab_ctx.log.info(msg),
                LogLevel::Warning => tab_ctx.log.warn(msg),
                LogLevel::Error => tab_ctx.log.error(msg),
            }
        }
    }

    pub fn show(&mut self, egui_ctx: &egui::Context, tab_ctx: &mut TabContext) {
        self.poll(tab_ctx);
        if !self.ctl.is_open() {
            return;
        }
        egui::Window::new("Physical Disk Export")
            .collapsible(false)
            .resizable(true)
            .default_width(560.0)
            .show(egui_ctx, |ui| {
                if let Some(snap) = self.ctl.opening() {
                    super::progress::show_job(ui, &snap);
                    return;
                }
                self.render_source_block(ui);
                ui.separator();

                if self.ctl.has_run() {
                    self.render_progress(ui);
                    return;
                }

                self.render_device_picker(ui, tab_ctx.devices);
                ui.add_space(4.0);

                let can_synth = self.ctl.can_synthesize();
                ui.add_enabled_ui(can_synth, |ui| {
                    let group_label = if can_synth {
                        "Synthesize partition table"
                    } else {
                        "Synthesize partition table (source already has one - disabled)"
                    };
                    let mut on = self.ctl.form().synthesize;
                    if ui.checkbox(&mut on, group_label).changed() {
                        self.act(DiskWriteAction::SetSynthesize(on));
                    }
                    ui.add_enabled_ui(on, |ui| self.render_synthesize_controls(ui));
                });

                if let Some(err) = self.ctl.error() {
                    ui.add_space(4.0);
                    ui.colored_label(super::theme::danger(ui.visuals()), err);
                }

                ui.separator();
                self.render_action_row(ui);
            });
        self.run_effects(egui_ctx, tab_ctx);
    }

    fn run_effects(&mut self, egui_ctx: &egui::Context, tab_ctx: &mut TabContext) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(egui_ctx, fx, &mut messages);
        replies.extend(self.effects.show(egui_ctx, "disk_write"));
        for msg in messages {
            tab_ctx.log.info(msg);
        }
        for reply in replies {
            let action = match reply {
                Reply::Picked { .. } | Reply::CloseScreen => continue,
                Reply::Confirmed { purpose, yes } => DiskWriteAction::Confirmed { purpose, yes },
                Reply::Answered { purpose, text } => DiskWriteAction::Answered { purpose, text },
            };
            self.act(action);
        }
        if !self.pending_fx.is_empty() {
            egui_ctx.request_repaint();
        }
    }

    fn render_source_block(&self, ui: &mut egui::Ui) {
        let Some(source) = self.ctl.source() else {
            return;
        };
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

    fn render_device_picker(
        &mut self,
        ui: &mut egui::Ui,
        devices: &[rusty_backup::device::DiskDevice],
    ) {
        let selected = self.ctl.target_row();
        let mut picked = None;
        ui.horizontal(|ui| {
            ui.label("Target disk:");
            let current_label = selected
                .and_then(|i| devices.get(i))
                .map(|d| d.display_name())
                .unwrap_or_else(|| "Select a device...".to_string());
            egui::ComboBox::from_id_salt("physical_disk_export_target")
                .selected_text(current_label)
                .width(380.0)
                .show_ui(ui, |ui| {
                    for (i, device) in devices.iter().enumerate() {
                        if ui
                            .selectable_label(selected == Some(i), device.display_name())
                            .clicked()
                        {
                            picked = Some(i);
                        }
                    }
                });
        });
        if let Some(row) = picked {
            if let Some(device) = devices.get(row) {
                self.act(DiskWriteAction::SelectDevice {
                    row,
                    device: device.clone(),
                });
            }
        }
    }

    fn render_synthesize_controls(&mut self, ui: &mut egui::Ui) {
        let form = *self.ctl.form();
        let size = self.ctl.source().map_or(0, |s| s.size_bytes);
        let mut actions = Vec::new();
        ui.horizontal(|ui| {
            ui.label("Table:");
            for (kind, label) in [(TableKind::Mbr, "MBR"), (TableKind::Gpt, "GPT")] {
                if ui.radio(form.table_kind == kind, label).clicked() {
                    actions.push(DiskWriteAction::SetTableKind(kind));
                }
            }
        });

        ui.horizontal(|ui| {
            ui.label("Alignment:");
            for (align, label) in [
                (WrapAlign::DosTraditional, "DOS legacy (LBA 63)"),
                (WrapAlign::Modern1MB, "Modern (LBA 2048)"),
                (WrapAlign::Custom, "Custom"),
            ] {
                if ui.radio(form.alignment == align, label).clicked() {
                    actions.push(DiskWriteAction::SetAlignment(align));
                }
            }
            if form.alignment == WrapAlign::Custom {
                let mut n = form.custom_alignment_sectors;
                if ui
                    .add(
                        egui::DragValue::new(&mut n)
                            .speed(1.0)
                            .range(1..=u64::MAX)
                            .suffix(" sectors"),
                    )
                    .changed()
                {
                    actions.push(DiskWriteAction::SetCustomAlignment(n));
                }
            }
        });

        ui.horizontal(|ui| {
            ui.label("Partition type:");
            let mut byte = form.type_byte;
            type_byte_picker(ui, &mut byte);
            if byte != form.type_byte {
                actions.push(DiskWriteAction::SetTypeByte(byte));
            }
        });

        ui.horizontal(|ui| {
            ui.label("Partition size:");
            // The source size is both "original" and "minimum": nothing is compacted here.
            let mut mode: SizeMode = form.size_mode;
            let mut custom = form.custom_size_mib;
            size_mode_row(
                ui,
                &mut mode,
                &mut custom,
                size,
                size,
                SizeModeRowOptions {
                    allow_fill: true,
                    ..Default::default()
                },
            );
            if mode != form.size_mode || custom != form.custom_size_mib {
                actions.push(DiskWriteAction::SetSize {
                    mode,
                    custom_mib: custom,
                });
            }
        });
        for a in actions {
            self.act(a);
        }
    }

    fn render_action_row(&mut self, ui: &mut egui::Ui) {
        self.render_target_region(ui);
        let hint = if self.ctl.target_partition().is_some() {
            "Writing replaces the contents of the selected partition."
        } else {
            "Writing erases all data on the selected device."
        };
        ui.horizontal(|ui| {
            let write = ui
                .add_enabled(self.ctl.can_write(), egui::Button::new("Write to disk..."))
                .on_hover_text(hint);
            if write.clicked() {
                self.act(DiskWriteAction::Write);
            }
            if ui.button("Cancel").clicked() {
                self.act(DiskWriteAction::Close);
            }
        });
    }

    /// Whole-disk vs single-partition target, with the partition list read off the device.
    fn render_target_region(&mut self, ui: &mut egui::Ui) {
        let Some(size) = self.ctl.target_device().map(|d| d.size_bytes) else {
            return;
        };
        if let Some(snap) = self.ctl.target_scan() {
            super::progress::show_job(ui, &snap);
            return;
        }
        let current = self.ctl.target_partition();
        let mut chosen = None;
        ui.separator();
        ui.label(egui::RichText::new("Write to").strong());
        let whole = format!(
            "Entire disk ({}) - replaces the partition table",
            format_size(size)
        );
        if ui.radio(current.is_none(), whole).clicked() {
            chosen = Some(None);
        }
        let parts = self.ctl.target_partitions();
        if parts.is_empty() {
            ui.label(
                egui::RichText::new(
                    "No partition table detected on the target, so only a \
                     whole-disk write is available.",
                )
                .weak(),
            );
        } else {
            for p in &parts {
                if ui
                    .radio(current == Some(p.index), p.label.as_str())
                    .clicked()
                {
                    chosen = Some(Some(p.index));
                }
            }
            if let Some(warning) = self.ctl.partition_warning() {
                ui.colored_label(super::theme::danger(ui.visuals()), warning);
            }
            ui.separator();
        }
        if let Some(idx) = chosen {
            self.act(DiskWriteAction::SetTargetPartition(idx));
        }
    }

    fn render_progress(&mut self, ui: &mut egui::Ui) {
        if let Some(snap) = self.ctl.progress() {
            if super::progress::show_job(ui, &snap) {
                self.act(DiskWriteAction::Cancel);
            }
        }
        match self.ctl.result() {
            Some(Ok(_)) => {
                ui.label("Write complete.");
            }
            Some(Err(e)) => {
                ui.colored_label(super::theme::danger(ui.visuals()), format!("Error: {e}"));
            }
            None => {}
        }
        if ui
            .add_enabled(!self.ctl.is_writing(), egui::Button::new("Close"))
            .clicked()
        {
            self.act(DiskWriteAction::Close);
        }
    }
}

/// A partition-type-byte combo: the common types plus a raw hex field for any other byte.
fn type_byte_picker(ui: &mut egui::Ui, type_byte: &mut u8) {
    let label = TYPE_BYTE_PRESETS
        .iter()
        .find(|(b, _)| b == type_byte)
        .map(|(_, n)| (*n).to_string())
        .unwrap_or_else(|| format!("0x{:02X} (custom)", type_byte));
    egui::ComboBox::from_id_salt("physical_disk_export_typebyte")
        .selected_text(label)
        .show_ui(ui, |ui| {
            for (byte, name) in TYPE_BYTE_PRESETS {
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
}
