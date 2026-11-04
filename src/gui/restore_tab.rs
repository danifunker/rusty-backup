//! The Restore tab: the egui shell of `controller::restore::RestoreController`.
//!
//! The tab draws each mode's form from the controller's accessors, maps clicks to
//! `RestoreAction`s, answers the controller's pickers, confirmations and the Build Disk name
//! prompt through an `EffectHost`, moves its log lines into the log panel and mirrors a running
//! restore onto the shared progress bar. It hosts two dialogs of its own: the Physical Disk Export
//! window Write Image File hands its image to, and the partition editor Build Disk fills.

use std::path::PathBuf;
use std::sync::Arc;

use rusty_backup::controller::disk_write::WriteOpen;
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::restore::{
    format_size, AlignmentChoice, HostRequest, LayoutFit, NewTableType, RestoreAction,
    RestoreController, RestoreMode, RestoreServices, TargetSlot,
};
use rusty_backup::controller::{Controller as _, Effect};

use super::context::TabContext;
use super::effects::{EffectHost, Reply};
use super::partition_bar::{PartitionBar, Segment, SegmentKind};
use super::physical_disk_export::PhysicalDiskExport;
use super::progress::ProgressState;
use super::size_mode_row::{size_mode_row, SizeModeRowOptions};

fn new_controller() -> RestoreController {
    RestoreController::new(RestoreServices {
        jobs: rusty_backup::controller::restore::JobRegistry::new(),
        notify: Arc::new(super::ui_logger::EguiWake),
    })
}

pub struct RestoreTab {
    ctl: RestoreController,
    /// Pickers, confirmations and the Build Disk prompt the controller asks for.
    effects: EffectHost,
    /// Effects from a call made outside `show`, run on the next frame.
    pending_fx: Vec<Effect>,
    /// MRU of opened backup-folder paths (newest first), mirrored to `config.json`.
    recent_files: Vec<String>,
    /// Write Image File's target picker and progress, shared with the Inspect tab's export.
    image_write: PhysicalDiskExport,
    /// The shared progress bar shows this tab's restore.
    showing_bar: bool,
}

impl Default for RestoreTab {
    fn default() -> Self {
        Self {
            ctl: new_controller(),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
            recent_files: super::load_recent(rusty_backup::update::RecentMode::Restore),
            image_write: PhysicalDiskExport::default(),
            showing_bar: false,
        }
    }
}

/// One device combo's label in the list.
fn device_label(d: &rusty_backup::device::DiskDevice) -> String {
    format!(
        "{} ({}){}",
        d.display_name(),
        format_size(d.size_bytes),
        if d.is_system { " [SYSTEM]" } else { "" },
    )
}

impl RestoreTab {
    pub fn is_running(&self) -> bool {
        self.ctl.is_running() || self.image_write.is_writing()
    }

    pub fn get_loaded_backup(&self) -> Option<PathBuf> {
        self.ctl.backup_folder().map(|p| p.to_path_buf())
    }

    pub fn has_backup(&self) -> bool {
        self.ctl.has_backup()
    }

    /// A source the user picked instead of a backup folder, or Write Image / Build Disk.
    pub fn has_other_source(&self) -> bool {
        self.ctl.has_other_source()
    }

    pub fn load_backup(&mut self, path: &std::path::Path) {
        self.act(RestoreAction::OpenBackup(path.to_path_buf()));
    }

    /// Open a dropped path: a folder restores in full, a file is a partition image.
    pub fn open_dropped(&mut self, path: PathBuf) {
        self.act(RestoreAction::OpenDropped(path));
    }

    /// Drop the in-memory recent list after Settings cleared the persisted one.
    pub(crate) fn clear_recent_files(&mut self) {
        self.recent_files.clear();
    }

    pub fn clear_backup(&mut self) {
        self.act(RestoreAction::ClearBackup);
    }

    /// Drain the controller while another tab shows, so a job keeps reporting and its end is seen.
    pub fn poll_background(&mut self, ctx: &mut TabContext, progress: &mut ProgressState) {
        self.poll(ctx, progress);
        self.image_write.poll(ctx);
    }

    /// Send an action; its effects run on the next frame.
    fn act(&mut self, action: RestoreAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
        self.carry_out_requests();
    }

    fn carry_out_requests(&mut self) {
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::RememberRecent(path) => {
                    self.recent_files =
                        super::push_recent(rusty_backup::update::RecentMode::Restore, &path)
                }
                HostRequest::WriteImage { path, size } => {
                    self.image_write.open_for(WriteOpen::image(path, size));
                }
            }
        }
    }

    fn poll(&mut self, ctx: &mut TabContext, progress: &mut ProgressState) {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        self.carry_out_requests();
        for (level, msg) in self.ctl.take_log() {
            match level {
                LogLevel::Info => ctx.log.info(msg),
                LogLevel::Warning => ctx.log.warn(msg),
                LogLevel::Error => ctx.log.error(msg),
            }
        }
        match self.ctl.progress() {
            Some(bar) => {
                progress.active = true;
                progress.operation = bar.operation;
                progress.current_bytes = bar.current_bytes;
                progress.total_bytes = bar.total_bytes;
                progress.full_size_bytes = 0;
                progress.record_sample();
                self.showing_bar = true;
            }
            None if self.showing_bar => {
                progress.active = false;
                self.showing_bar = false;
            }
            None => {}
        }
    }

    /// Run the effects waiting; answers go back to the controller.
    fn run_effects(&mut self, ui: &egui::Ui, ctx: &mut TabContext) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(ui.ctx(), fx, &mut messages);
        replies.extend(self.effects.show(ui.ctx(), "restore"));
        for msg in messages {
            ctx.log.info(msg);
        }
        for reply in replies {
            let action = match reply {
                Reply::Picked { purpose, path } => RestoreAction::Picked { purpose, path },
                Reply::Confirmed { purpose, yes } => RestoreAction::Confirmed { purpose, yes },
                Reply::Answered { purpose, text } => RestoreAction::Answered { purpose, text },
                Reply::Chose { .. } | Reply::CloseScreen => continue,
            };
            self.act(action);
        }
        if !self.pending_fx.is_empty() {
            ui.ctx().request_repaint();
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext, progress: &mut ProgressState) {
        self.ctl.reconcile_devices(ctx.devices);
        self.poll(ctx, progress);
        if let Some(snap) = self.ctl.decoding() {
            super::progress::show_job(ui, &snap);
        }

        ui.heading("Restore Backup");
        ui.add_space(8.0);

        if let Some(tab) = ctx.busy_elsewhere {
            ui.colored_label(
                super::theme::warning(ui.visuals()),
                super::context::busy_elsewhere_notice(tab),
            );
        }
        let controls_enabled = !self.is_running() && ctx.busy_elsewhere.is_none();

        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Mode:").strong());
                let mode = self.ctl.mode();
                let modes = [
                    (RestoreMode::FullDisk, "Full Disk Restore", ""),
                    (RestoreMode::SinglePartition, "Restore Single Partition", ""),
                    (RestoreMode::NewDisk, "Restore to New Disk", ""),
                    (
                        RestoreMode::WriteImage,
                        "Write Image File",
                        "Write a disk image straight to a device or one of its \
                         partitions. Any format Rusty Backup can read works, \
                         including DMG, CHD, VHD and compressed images.",
                    ),
                    (
                        RestoreMode::BuildDisk,
                        "Build Disk",
                        "Lay a fresh MBR / GPT / APM / SGI / X68000 partition \
                         table on a blank disk or image file, and optionally \
                         pour an image into each partition in one pass.",
                    ),
                ];
                for (m, label, hover) in modes {
                    let mut resp = ui.radio(mode == m, label);
                    if !hover.is_empty() {
                        resp = resp.on_hover_text(hover);
                    }
                    if resp.clicked() {
                        self.act(RestoreAction::SetMode(m));
                    }
                }
            });
        });
        ui.add_space(4.0);

        match self.ctl.mode() {
            RestoreMode::FullDisk => self.show_full_disk_mode(ui, ctx, controls_enabled),
            RestoreMode::SinglePartition => {
                self.show_single_partition_mode(ui, ctx, controls_enabled)
            }
            RestoreMode::NewDisk => self.show_new_disk_mode(ui, ctx, controls_enabled),
            RestoreMode::WriteImage => self.show_write_image_mode(ui, controls_enabled),
            RestoreMode::BuildDisk => self.show_build_disk_mode(ui, ctx, controls_enabled),
        }

        self.image_write.show(ui.ctx(), ctx);
        if self.ctl.builder_open() {
            self.show_build_disk_modal(ui);
        }
        self.run_effects(ui, ctx);
    }

    /// A device combo for `slot`, plus Close Drive when `closable`.
    fn device_combo(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &TabContext,
        slot: TargetSlot,
        salt: &str,
        closable: bool,
    ) {
        let row = self.ctl.device_row(slot);
        let current = self
            .ctl
            .device(slot)
            .map(|d| d.display_name())
            .unwrap_or_else(|| "Select a target device...".into());
        let mut picked = None;
        egui::ComboBox::from_id_salt(salt)
            .selected_text(&current)
            .width(400.0)
            .height(400.0)
            .show_ui(ui, |ui| {
                for (i, device) in ctx.devices.iter().enumerate() {
                    if ui
                        .selectable_label(row == Some(i), device_label(device))
                        .clicked()
                    {
                        picked = Some((i, device.clone()));
                    }
                }
                if ctx.devices.is_empty() {
                    ui.label(
                        egui::RichText::new(super::no_devices_hint())
                            .italics()
                            .weak(),
                    );
                }
            });
        if let Some((row, device)) = picked {
            self.act(RestoreAction::SelectDevice { slot, row, device });
        }
        if closable
            && row.is_some()
            && ui
                .button("Close Drive")
                .on_hover_text(
                    "Clear the selected target device so the drive can be safely removed",
                )
                .clicked()
        {
            self.act(RestoreAction::ClearDevice(slot));
        }
    }

    /// "Write to device" / "Save as image file" radios for `slot`.
    fn target_kind_radios(&mut self, ui: &mut egui::Ui, slot: TargetSlot) {
        let is_device = self.ctl.target_is_device(slot);
        ui.horizontal(|ui| {
            if ui.radio(is_device, "Write to device").clicked() {
                self.act(RestoreAction::SetTargetIsDevice(slot, true));
            }
            if ui.radio(!is_device, "Save as image file").clicked() {
                self.act(RestoreAction::SetTargetIsDevice(slot, false));
            }
        });
    }

    /// The image target's path and its "Save As..." button.
    fn target_image_row(&mut self, ui: &mut egui::Ui, slot: TargetSlot, caption: &str) {
        ui.horizontal(|ui| {
            ui.label(caption);
            let label = self
                .ctl
                .target_image(slot)
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "No file selected".into());
            ui.label(&label);
            if ui.button("Save As...").clicked() {
                self.act(RestoreAction::PickTargetImage(slot));
            }
        });
    }

    fn backup_folder_row(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Backup Folder:");
            let label = self
                .ctl
                .backup_folder()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "No folder selected".into());
            ui.label(&label);
            if ui.button("Browse...").clicked() {
                self.act(RestoreAction::PickBackupFolder);
            }
        });
    }

    fn load_status(&mut self, ui: &mut egui::Ui) {
        if let Some(snap) = self.ctl.loading() {
            super::progress::show_job(ui, &snap);
        }
    }

    fn show_full_disk_mode(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &mut TabContext,
        controls_enabled: bool,
    ) {
        ui.add_enabled_ui(controls_enabled, |ui| self.backup_folder_row(ui));
        if controls_enabled {
            if let Some(path) = super::recent_combo(ui, "restore_recent", &self.recent_files, 0) {
                self.act(RestoreAction::OpenDropped(path));
            }
        }
        self.load_status(ui);

        if let Some((info, source)) = self.ctl.backup_summary() {
            ui.add_space(4.0);
            ui.horizontal_wrapped(|ui| {
                ui.label(egui::RichText::new("Backup Info:").strong());
                ui.label(info);
            });
            ui.horizontal_wrapped(|ui| {
                ui.label(egui::RichText::new("Source:").strong());
                ui.label(source);
            });
        }
        if let Some(err) = self.ctl.load_error() {
            ui.colored_label(super::theme::warning(ui.visuals()), err);
        }

        ui.add_space(8.0);
        ui.label(egui::RichText::new("Target").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            #[cfg(feature = "remote")]
            {
                let mut on = self.ctl.remote_active();
                if ui
                    .checkbox(&mut on, "Restore to a remote machine (rb-cli serve)")
                    .on_hover_text(
                        "Push the restored image to a drive or image file on a remote \
                         daemon over the network",
                    )
                    .changed()
                {
                    self.act(RestoreAction::SetRemote(on));
                }
            }
            if !self.ctl.remote_active() {
                self.target_kind_radios(ui, TargetSlot::FullDisk);
                if self.ctl.target_is_device(TargetSlot::FullDisk) {
                    ui.horizontal(|ui| {
                        ui.label("Device:");
                        self.device_combo(
                            ui,
                            ctx,
                            TargetSlot::FullDisk,
                            "restore_target_device",
                            true,
                        );
                    });
                    if self
                        .ctl
                        .device(TargetSlot::FullDisk)
                        .is_some_and(|d| d.is_system)
                    {
                        ui.colored_label(
                            super::theme::danger(ui.visuals()),
                            "WARNING: This is a system disk! Restoring will destroy your OS.",
                        );
                    }
                } else {
                    self.target_image_row(ui, TargetSlot::FullDisk, "Image File:");
                }
            }
            #[cfg(feature = "remote")]
            if self.ctl.remote_active() {
                self.remote_target_ui(ui);
            }

            if let Some(source_size) = self.ctl.source_size() {
                let target_size = self.ctl.target_size();
                if target_size > 0 {
                    let color = if target_size >= source_size {
                        super::theme::muted(ui.visuals())
                    } else {
                        super::theme::warning(ui.visuals())
                    };
                    ui.colored_label(
                        color,
                        format!(
                            "Target: {} | Source was: {}{}",
                            format_size(target_size),
                            format_size(source_size),
                            if target_size < source_size {
                                " (target is smaller!)"
                            } else {
                                ""
                            },
                        ),
                    );
                }
            }
        });

        if self.ctl.expand_shown() {
            self.show_expand_section(ui, controls_enabled);
        }

        if self.ctl.has_backup() {
            ui.add_space(8.0);
            ui.label(egui::RichText::new("Alignment").strong());
            ui.separator();
            ui.add_enabled_ui(controls_enabled, |ui| {
                let (choice, custom) = self.ctl.alignment();
                if ui
                    .radio(
                        choice == AlignmentChoice::Original,
                        self.ctl.original_alignment_label(),
                    )
                    .clicked()
                {
                    self.act(RestoreAction::SetAlignment(AlignmentChoice::Original));
                }
                if ui
                    .radio(
                        choice == AlignmentChoice::Modern1MB,
                        "1 MB boundaries (modern, LBA 2048)",
                    )
                    .clicked()
                {
                    self.act(RestoreAction::SetAlignment(AlignmentChoice::Modern1MB));
                }
                ui.horizontal(|ui| {
                    if ui
                        .radio(choice == AlignmentChoice::Custom, "Custom:")
                        .clicked()
                    {
                        self.act(RestoreAction::SetAlignment(AlignmentChoice::Custom));
                    }
                    if choice == AlignmentChoice::Custom {
                        let mut n = custom;
                        if ui
                            .add(egui::DragValue::new(&mut n).range(1..=65535))
                            .changed()
                        {
                            self.act(RestoreAction::SetCustomAlignment(n));
                        }
                        ui.label("sectors");
                    }
                });
            });
        }

        if !self.ctl.size_rows().is_empty() {
            ui.add_space(8.0);
            ui.label(egui::RichText::new("Partition Sizes").strong());
            ui.separator();
            ui.add_enabled_ui(controls_enabled, |ui| self.show_size_grid(ui));
        }

        ui.add_space(16.0);
        self.show_action_buttons(ui, ctx);
    }

    fn show_size_grid(&mut self, ui: &mut egui::Ui) {
        let rows = self.ctl.size_rows().to_vec();
        let last_index = rows.last().map(|r| r.index);
        let mut actions = Vec::new();
        egui::Grid::new("restore_partition_sizes")
            .striped(true)
            .min_col_width(50.0)
            .show(ui, |ui| {
                for head in [
                    "#",
                    "Type",
                    "Original",
                    "Minimum",
                    "Size Mode",
                    "Size (MiB)",
                ] {
                    ui.label(egui::RichText::new(head).strong());
                }
                ui.end_row();
                for (pos, row) in rows.iter().enumerate() {
                    // Position, matching `inspect`'s `#`; `row.index` is the table slot.
                    ui.label(format!("{}", pos + 1));
                    ui.label(&row.type_name);
                    ui.label(format_size(row.original_size));
                    ui.label(format_size(row.minimum_size));
                    let mut mode = row.mode;
                    let mut custom = row.custom_mib;
                    size_mode_row(
                        ui,
                        &mut mode,
                        &mut custom,
                        row.original_size,
                        row.minimum_size,
                        SizeModeRowOptions {
                            allow_fill: Some(row.index) == last_index,
                            custom_seed: super::size_mode_row::CustomSeed::Original,
                            ..Default::default()
                        },
                    );
                    if mode != row.mode || custom != row.custom_mib {
                        actions.push(RestoreAction::SetPartitionSize {
                            index: row.index,
                            mode,
                            custom_mib: custom,
                        });
                    }
                    ui.end_row();
                }
            });
        for a in actions {
            self.act(a);
        }

        let target_size = self.ctl.target_size();
        if target_size == 0 {
            return;
        }
        let total = self.ctl.fixed_total();
        let color = if total <= target_size {
            super::theme::muted(ui.visuals())
        } else {
            super::theme::danger(ui.visuals())
        };
        ui.colored_label(
            color,
            format!(
                "Total: {} / {} available",
                format_size(total),
                format_size(target_size),
            ),
        );
        // Sizes adding up is not the layout fitting: Original alignment keeps every start.
        match self.ctl.layout_fit() {
            Some(LayoutFit::Refused { message, original }) => {
                ui.colored_label(
                    super::theme::danger(ui.visuals()),
                    format!("This layout will not fit: {message}"),
                );
                if original {
                    ui.colored_label(
                        super::theme::warning(ui.visuals()),
                        "Original alignment keeps every partition at its \
                         original position, so shrinking only leaves gaps. \
                         Choose 1 MB boundaries or Custom to repack them \
                         from the start of the disk.",
                    );
                }
            }
            Some(LayoutFit::Overrun { end, target }) => {
                ui.colored_label(
                    super::theme::danger(ui.visuals()),
                    format!(
                        "Layout ends at {} but the target holds {}",
                        format_size(end),
                        format_size(target),
                    ),
                );
            }
            Some(LayoutFit::Fits) | None => {}
        }
    }

    /// "Add free space for in-OS expansion": image targets only (docs/disk_expansion.md).
    fn show_expand_section(&mut self, ui: &mut egui::Ui, controls_enabled: bool) {
        let source_size = self.ctl.source_size().unwrap_or(0);
        ui.add_space(8.0);
        ui.label(egui::RichText::new("Add free space for in-OS expansion").strong());
        ui.separator();
        let plan = self.ctl.expand();
        let mut actions = Vec::new();
        ui.add_enabled_ui(controls_enabled, |ui| {
            let mut on = plan.enabled;
            if ui
                .checkbox(&mut on, "Pad the target image with unallocated free space")
                .on_hover_text(
                    "Useful for restoring a backup onto a larger virtual disk and \
                     letting the guest OS extend or add a partition there. After \
                     restore, run your OS's partitioner (parted, fdisk, IRIX fx, \
                     Disk Management) followed by your filesystem's own grow \
                     tool. Works for any filesystem.",
                )
                .changed()
            {
                actions.push(RestoreAction::SetExpand(on));
            }
            if !plan.enabled {
                return;
            }
            ui.horizontal(|ui| {
                ui.label("Free space:");
                let mut mib = plan.free_mib;
                if ui
                    .add(
                        egui::DragValue::new(&mut mib)
                            .range(0..=self.ctl.expand_cap_mib())
                            .suffix(" MiB"),
                    )
                    .changed()
                {
                    actions.push(RestoreAction::SetExpandMib(mib));
                }
                let new_total = source_size.saturating_add(self.ctl.expand_free_bytes());
                ui.label(format!(
                    "(target size {} -> {})",
                    format_size(source_size),
                    format_size(new_total),
                ));
            });
            ui.add_space(4.0);
            if ui
                .radio(
                    !plan.extend_last,
                    "Leave as unpartitioned free space (recommended)",
                )
                .on_hover_text(
                    "Mode A: partition table stays unchanged; the trailing region is \
                     unallocated. After restore, run your OS's partitioner to extend or \
                     add a partition, then your filesystem's grow tool. Works for any \
                     filesystem on any OS.",
                )
                .clicked()
            {
                actions.push(RestoreAction::SetExtendLast(false));
            }
            if ui
                .radio(plan.extend_last, "Extend last partition automatically")
                .on_hover_text(
                    "Mode B: the last partition is sized to absorb the new free space \
                     during restore (equivalent to picking 'Fill' for the last partition \
                     in the size table below). After restore, only the filesystem's \
                     own grow tool is needed in the guest OS.",
                )
                .clicked()
            {
                actions.push(RestoreAction::SetExtendLast(true));
            }
            self.show_expand_preview_bars(ui, source_size);
        });
        for a in actions {
            self.act(a);
        }
    }

    /// The current / after bars at one byte-per-pixel scale, so the after bar is wider.
    fn show_expand_preview_bars(&self, ui: &mut egui::Ui, source_size: u64) {
        let segments: Vec<Segment> = self
            .ctl
            .plan_segments()
            .into_iter()
            .enumerate()
            .map(|(color_index, s)| Segment {
                label: s.label,
                fs: s.fs,
                size_bytes: s.size_bytes,
                kind: SegmentKind::Partition { color_index },
            })
            .collect();
        let added = self.ctl.expand_free_bytes();
        let new_total = source_size.saturating_add(added);

        ui.add_space(6.0);
        ui.label("Current:");
        let available_width = ui.available_width().max(120.0);
        let ratio = (source_size as f64 / new_total.max(1) as f64) as f32;
        let current_segments = segments.clone();
        ui.scope(|ui| {
            ui.set_width((available_width * ratio).max(60.0));
            PartitionBar {
                segments: current_segments,
                show_inline_labels: true,
                show_legend: false,
            }
            .show(ui);
        });

        ui.add_space(6.0);
        ui.label(format!("After  (+ {} free):", format_size(added)));
        let mut after = segments;
        if added > 0 {
            if self.ctl.expand().extend_last {
                // Mode B: the last partition absorbs the new free space.
                if let Some(last) = after.last_mut() {
                    last.size_bytes = last.size_bytes.saturating_add(added);
                }
            } else {
                after.push(Segment {
                    label: String::new(),
                    fs: String::new(),
                    size_bytes: added,
                    kind: SegmentKind::Free,
                });
            }
        }
        PartitionBar {
            segments: after,
            show_inline_labels: true,
            show_legend: true,
        }
        .show(ui);
    }

    fn show_action_buttons(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        ui.add_space(16.0);
        ui.horizontal(|ui| {
            if !self.ctl.restore_running() && ctx.busy_elsewhere.is_none() {
                if ui
                    .add_enabled(self.ctl.can_start(), egui::Button::new("Restore"))
                    .clicked()
                {
                    self.act(RestoreAction::Start);
                }
            } else if self.ctl.restore_running() && ui.button("Cancel").clicked() {
                self.act(RestoreAction::Cancel);
            }
        });
    }

    /// The source rows of the single-partition and new-disk modes.
    fn show_partition_source(
        &mut self,
        ui: &mut egui::Ui,
        controls_enabled: bool,
        salt: &str,
        whole_image_note: bool,
    ) {
        ui.label(egui::RichText::new("Source").strong());
        ui.separator();
        ui.add_enabled_ui(controls_enabled, |ui| {
            self.backup_folder_row(ui);
            ui.horizontal(|ui| {
                ui.label("Or Image File:");
                let label = self
                    .ctl
                    .source_image()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "No file selected".into());
                ui.label(&label);
                if ui.button("Browse...").clicked() {
                    self.act(RestoreAction::PickSourceImage);
                }
            });
        });
        self.load_status(ui);

        if self.ctl.has_native_backup() {
            ui.add_space(4.0);
            ui.add_enabled_ui(controls_enabled, |ui| {
                ui.horizontal(|ui| {
                    ui.label("Source Partition:");
                    let parts = self.ctl.source_partitions();
                    let current = self.ctl.source_partition();
                    let label = current
                        .and_then(|idx| parts.iter().find(|p| p.index == idx))
                        .map(|p| p.label.clone())
                        .unwrap_or_else(|| "Select a partition...".into());
                    let mut picked = None;
                    egui::ComboBox::from_id_salt(salt)
                        .selected_text(&label)
                        .width(350.0)
                        .show_ui(ui, |ui| {
                            for p in &parts {
                                if ui
                                    .selectable_label(current == Some(p.index), p.label.as_str())
                                    .clicked()
                                {
                                    picked = Some(p.index);
                                }
                            }
                        });
                    if let Some(idx) = picked {
                        self.act(RestoreAction::SetSourcePartition(Some(idx)));
                    }
                });
            });
        } else if whole_image_note && self.ctl.source_image().is_some() {
            ui.add_space(4.0);
            ui.label("Source: entire image file");
        }
        if let Some(err) = self.ctl.load_error() {
            ui.colored_label(super::theme::warning(ui.visuals()), err);
        }
    }

    fn show_single_partition_mode(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &mut TabContext,
        controls_enabled: bool,
    ) {
        self.show_partition_source(ui, controls_enabled, "sp_source_partition", true);
        ui.add_space(8.0);
        ui.label(egui::RichText::new("Target").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label("Target Device:");
                self.device_combo(
                    ui,
                    ctx,
                    TargetSlot::SinglePartition,
                    "sp_target_device",
                    false,
                );
                if let Some(snap) = self.ctl.target_scan() {
                    super::progress::show_job(ui, &snap);
                } else if self.ctl.device(TargetSlot::SinglePartition).is_some()
                    && ui.button("Scan Target").clicked()
                {
                    self.act(RestoreAction::ScanTarget);
                }
            });
            if self
                .ctl
                .device(TargetSlot::SinglePartition)
                .is_some_and(|d| d.is_system)
            {
                ui.colored_label(
                    super::theme::danger(ui.visuals()),
                    "WARNING: This is a system disk!",
                );
            }

            let parts = self.ctl.target_partitions();
            if !parts.is_empty() {
                ui.add_space(4.0);
                ui.label("Target Partition:");
                let selected = self.ctl.target_partition();
                let mut picked = None;
                egui::Grid::new("sp_target_partitions")
                    .striped(true)
                    .min_col_width(30.0)
                    .show(ui, |ui| {
                        for head in ["", "#", "Type", "Size", ""] {
                            ui.label(egui::RichText::new(head).strong());
                        }
                        ui.end_row();
                        for part in &parts {
                            if ui.radio(selected == Some(part.index), "").clicked() {
                                picked = Some(part.index);
                            }
                            ui.label(format!("{}", part.position));
                            ui.label(&part.type_name);
                            ui.label(format_size(part.size_bytes));
                            match part.fits {
                                Some(false) => {
                                    ui.colored_label(
                                        super::theme::danger(ui.visuals()),
                                        "too small",
                                    );
                                }
                                Some(true) => {
                                    ui.label("OK");
                                }
                                None => {
                                    ui.label("");
                                }
                            }
                            ui.end_row();
                        }
                    });
                if let Some(idx) = picked {
                    self.act(RestoreAction::SetTargetPartition(idx));
                }
            }
            if let Some(err) = self.ctl.target_scan_error() {
                ui.colored_label(super::theme::warning(ui.visuals()), err);
            }
        });
        self.show_action_buttons(ui, ctx);
    }

    fn show_new_disk_mode(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &mut TabContext,
        controls_enabled: bool,
    ) {
        self.show_partition_source(ui, controls_enabled, "nd_source_partition", false);
        ui.add_space(8.0);
        ui.label(egui::RichText::new("Target").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            self.target_kind_radios(ui, TargetSlot::NewDisk);
            if self.ctl.target_is_device(TargetSlot::NewDisk) {
                ui.horizontal(|ui| {
                    ui.label("Device:");
                    self.device_combo(ui, ctx, TargetSlot::NewDisk, "nd_target_device", false);
                });
            } else {
                self.target_image_row(ui, TargetSlot::NewDisk, "Image File:");
            }
        });

        ui.add_space(8.0);
        ui.label(egui::RichText::new("New Partition Table").strong());
        ui.separator();

        let mut choice = self.ctl.new_disk();
        let before = choice;
        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label("Table Type:");
                ui.radio_value(&mut choice.table, NewTableType::Mbr, "MBR");
                ui.radio_value(&mut choice.table, NewTableType::Gpt, "GPT");
                ui.radio_value(&mut choice.table, NewTableType::Apm, "APM");
            });
            ui.horizontal(|ui| {
                ui.label("Alignment:");
                ui.radio_value(
                    &mut choice.alignment,
                    AlignmentChoice::Modern1MB,
                    "1 MB (LBA 2048)",
                );
                ui.radio_value(
                    &mut choice.alignment,
                    AlignmentChoice::Original,
                    "DOS (LBA 63)",
                );
                ui.radio_value(&mut choice.alignment, AlignmentChoice::Custom, "Custom");
                if choice.alignment == AlignmentChoice::Custom {
                    ui.add(egui::DragValue::new(&mut choice.custom_alignment).range(1..=65535));
                    ui.label("sectors");
                }
            });
            if choice.table == NewTableType::Mbr {
                ui.checkbox(&mut choice.bootable, "Mark partition as bootable");
            }
            ui.colored_label(
                super::theme::muted(ui.visuals()),
                format!(
                    "Partition will start at LBA {}",
                    self.ctl.new_disk_start_lba()
                ),
            );
        });
        if choice != before {
            self.act(RestoreAction::SetNewDisk(choice));
        }
        self.show_action_buttons(ui, ctx);
    }

    /// Write Image File: the source here; target, confirmation and progress are the shared
    /// Physical Disk Export window.
    fn show_write_image_mode(&mut self, ui: &mut egui::Ui, controls_enabled: bool) {
        ui.label(egui::RichText::new("Source Image").strong());
        ui.separator();
        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label("Image file:");
                let label = self
                    .ctl
                    .write_image()
                    .map(|(p, _)| p.display().to_string())
                    .unwrap_or_else(|| "(none selected)".to_string());
                ui.label(label);
                if ui.button("Choose...").clicked() {
                    self.act(RestoreAction::PickWriteImage);
                }
            });
        });
        if let Some(snap) = self.ctl.probing() {
            super::progress::show_job(ui, &snap);
        }

        match self.ctl.write_image() {
            Some((_, size)) if size > 0 => {
                ui.label(format!(
                    "Decoded size: {} (the file on disk may be smaller if it is compressed)",
                    format_size(size),
                ));
                ui.add_space(6.0);
                if ui
                    .add_enabled(
                        controls_enabled,
                        egui::Button::new("Choose target and write..."),
                    )
                    .clicked()
                {
                    self.act(RestoreAction::WriteImageTarget);
                }
            }
            Some(_) => {
                ui.colored_label(
                    super::theme::danger(ui.visuals()),
                    "That file could not be opened as a disk image.",
                );
            }
            None => {
                ui.label(
                    egui::RichText::new(
                        "Pick an image to write. Any format Rusty Backup can read works -- \
                         raw, DMG, CHD, VHD, 2MG, GHO, IMZ, and gzip- or zip-wrapped images \
                         are decoded on the way to the disk.",
                    )
                    .weak(),
                );
            }
        }
    }

    /// Build Disk: pick a blank target, then define its partitions in the shared modal.
    fn show_build_disk_mode(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &mut TabContext,
        controls_enabled: bool,
    ) {
        ui.label(egui::RichText::new("Target").strong());
        ui.separator();

        ui.add_enabled_ui(controls_enabled, |ui| {
            let is_device = self.ctl.target_is_device(TargetSlot::BuildDisk);
            ui.horizontal(|ui| {
                if ui.radio(!is_device, "New image file").clicked() {
                    self.act(RestoreAction::SetTargetIsDevice(
                        TargetSlot::BuildDisk,
                        false,
                    ));
                }
                if ui.radio(is_device, "Physical device").clicked() {
                    self.act(RestoreAction::SetTargetIsDevice(
                        TargetSlot::BuildDisk,
                        true,
                    ));
                }
            });
            if is_device {
                ui.horizontal(|ui| {
                    ui.label("Device:");
                    self.device_combo(ui, ctx, TargetSlot::BuildDisk, "bd_target_device", false);
                });
                ui.label(
                    egui::RichText::new("Building erases everything on the selected device.")
                        .color(super::theme::warning(ui.visuals())),
                );
            } else {
                self.target_image_row(ui, TargetSlot::BuildDisk, "Image file:");
                ui.horizontal(|ui| {
                    ui.label("Disk size:");
                    let mut text = self.ctl.build_size_text().to_string();
                    if ui
                        .add(egui::TextEdit::singleline(&mut text).desired_width(90.0))
                        .on_hover_text("Accepts K / M / G suffixes, e.g. 512M or 2G.")
                        .changed()
                    {
                        self.act(RestoreAction::SetBuildSize(text));
                    }
                    match self.ctl.build_size() {
                        Ok(n) => {
                            ui.label(egui::RichText::new(format_size(n)).weak());
                        }
                        Err(e) => {
                            ui.colored_label(super::theme::danger(ui.visuals()), e);
                        }
                    }
                });
            }
        });

        ui.add_space(8.0);
        let has_target = self.ctl.build_target().is_some();
        ui.add_enabled_ui(controls_enabled && has_target, |ui| {
            if ui
                .button("Define Partitions...")
                .on_hover_text("Choose the table type and lay out the partitions")
                .clicked()
            {
                self.act(RestoreAction::BuildDefine);
            }
        });
        if !has_target {
            ui.label(egui::RichText::new("Pick a target first.").weak());
        }
        self.show_build_progress(ui);
    }

    fn show_build_disk_modal(&mut self, ui: &mut egui::Ui) {
        let Some(view) = self.ctl.build_view() else {
            return;
        };
        let probing = self.ctl.build_source_probing().is_some();
        let (edits, action) = super::partition_editor_modal::show_build(ui, &view, probing);
        for edit in edits {
            self.act(RestoreAction::Build(edit));
        }
        match action {
            super::partition_editor_modal::Action::Stay => {}
            super::partition_editor_modal::Action::Close => self.act(RestoreAction::BuildClose),
            super::partition_editor_modal::Action::Apply => self.act(RestoreAction::BuildApply),
        }
    }

    fn show_build_progress(&mut self, ui: &mut egui::Ui) {
        let Some(snap) = self.ctl.build_progress() else {
            return;
        };
        ui.add_space(8.0);
        ui.separator();
        if snap.is_running() {
            if super::progress::show_job(ui, &snap) {
                self.act(RestoreAction::BuildCancel);
            }
            return;
        }
        match self.ctl.build_result() {
            Some(Err(err)) => {
                ui.colored_label(super::theme::danger(ui.visuals()), format!("Error: {err}"));
            }
            Some(Ok(())) => {
                ui.label("Disk build complete.");
            }
            None => {}
        }
    }

    /// Inline remote-target picker: connect to a daemon, then pick a drive or name an image file.
    #[cfg(feature = "remote")]
    fn remote_target_ui(&mut self, ui: &mut egui::Ui) {
        let busy = self.ctl.remote_busy();
        let connected = self.ctl.remote_connected();
        let mut actions = Vec::new();
        ui.horizontal(|ui| {
            ui.label("Host:");
            let mut addr = self.ctl.remote_address().to_string();
            if ui
                .add(
                    egui::TextEdit::singleline(&mut addr)
                        .hint_text("192.168.1.50:7341")
                        .desired_width(200.0),
                )
                .changed()
            {
                actions.push(RestoreAction::RemoteSetAddress(addr));
            }
            if ui
                .add_enabled(
                    !busy,
                    egui::Button::new(if connected { "Reconnect" } else { "Connect" }),
                )
                .clicked()
            {
                actions.push(RestoreAction::RemoteConnect);
            }
            if busy {
                ui.add(egui::Spinner::new());
            }
        });
        if let Some(err) = self.ctl.remote_error() {
            ui.colored_label(super::theme::danger(ui.visuals()), err);
        }

        if connected {
            ui.separator();
            ui.label("Restore target on the remote machine:");
            let drives = self.ctl.remote_drives();
            if drives.is_empty() {
                ui.label(
                    egui::RichText::new(
                        "No physical drives reported (the daemon may need to run as root). \
                         You can still restore to an image file below.",
                    )
                    .italics()
                    .weak(),
                );
            } else {
                let selected = self.ctl.remote_selected_device().map(|s| s.to_string());
                egui::ScrollArea::vertical()
                    .max_height(160.0)
                    .show(ui, |ui| {
                        for d in drives {
                            let is_sel = selected.as_deref() == Some(d.path.as_str());
                            if ui.selectable_label(is_sel, d.label.as_str()).clicked() {
                                actions.push(RestoreAction::RemotePickDevice {
                                    path: d.path,
                                    size: d.size_bytes,
                                });
                            }
                        }
                    });
            }
            ui.horizontal(|ui| {
                ui.label("or image file:");
                let mut name = self.ctl.remote_image_name().to_string();
                if ui
                    .add(
                        egui::TextEdit::singleline(&mut name)
                            .hint_text("restored.img")
                            .desired_width(200.0),
                    )
                    .changed()
                {
                    actions.push(RestoreAction::RemoteSetImageName(name.clone()));
                }
                if ui
                    .add_enabled(!name.trim().is_empty(), egui::Button::new("Use file"))
                    .clicked()
                {
                    actions.push(RestoreAction::RemoteUseImage);
                }
            });
            if let Some(label) = self.ctl.remote_target_label() {
                ui.colored_label(
                    super::theme::muted(ui.visuals()),
                    format!("Selected target: {label}"),
                );
            }
        }
        for a in actions {
            self.act(a);
        }
    }
}
