//! The Backup tab: the egui shell of `controller::backup::BackupController`.
//!
//! The tab draws the source picker, the partition grid, the options form, the VHD options window
//! and the Remote Drive window from the controller's accessors, maps clicks to `BackupAction`s,
//! answers the controller's pickers through an `EffectHost`, moves its log lines into the log
//! panel and mirrors a running backup onto the shared progress bar. The CHD options control is
//! widget state; its choice travels with Start.

use std::path::PathBuf;
use std::sync::Arc;

use rusty_backup::controller::app::{RecentHandle, RecentMode};
use rusty_backup::controller::backup::{
    format_size, BackupAction, BackupController, BackupServices, ChdChoice, ChdProfile,
    ChecksumType, CompactCell, CompressionType, HostRequest, MinCell,
};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{Controller as _, Effect};

use super::chd_options_ui::{ChdOptionsControl, ChdOptionsMode};
use super::context::TabContext;
use super::effects::{EffectHost, Reply};
use super::progress::ProgressState;

fn new_controller() -> BackupController {
    BackupController::new(BackupServices {
        jobs: rusty_backup::controller::backup::JobRegistry::new(),
        notify: Arc::new(super::ui_logger::EguiWake),
    })
}

pub struct BackupTab {
    ctl: BackupController,
    /// Pickers the controller asks for.
    effects: EffectHost,
    /// Effects from a call made outside `show`, run on the next frame.
    pending_fx: Vec<Effect>,
    /// In-memory mirror of the Backup recent-image list (config.json), for the Recent dropdown.
    recent_files: Vec<String>,
    /// The app's recent-files store; opens are recorded through it.
    recent: RecentHandle,
    chd_hd_control: ChdOptionsControl,
    /// The shared progress bar shows this tab's job.
    showing_bar: bool,
}

impl BackupTab {
    pub fn new(recent: RecentHandle) -> Self {
        let mut chd = ChdOptionsControl::new(ChdProfile::Hd);
        // The last Custom codecs and hunk size wait in the Custom slot.
        chd.custom = BackupController::saved_chd_custom(chd.custom.clone());
        Self {
            ctl: new_controller(),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
            recent_files: recent.list(RecentMode::Backup),
            recent,
            chd_hd_control: chd,
            showing_bar: false,
        }
    }
}

impl BackupTab {
    /// A backup or whole-disk VHD export is in flight.
    pub fn is_running(&self) -> bool {
        self.ctl.is_running()
    }

    /// Drain the controller while another tab shows, so a job keeps reporting and its end is seen.
    pub fn poll_background(&mut self, ctx: &mut TabContext, progress: &mut ProgressState) {
        self.poll(ctx, progress);
    }

    /// Open `path` as the source (drag-and-drop); a wrapped image is decoded first.
    pub fn open_image(&mut self, path: PathBuf) {
        self.act(BackupAction::OpenImage {
            path,
            rename: false,
        });
    }

    /// Drop the in-memory recent list after Settings cleared the persisted one.
    pub(crate) fn clear_recent_files(&mut self) {
        self.recent_files.clear();
    }

    /// Send an action; its effects run on the next frame.
    fn act(&mut self, action: BackupAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
        self.carry_out_requests();
    }

    fn carry_out_requests(&mut self) {
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::RememberRecent(path) => {
                    self.recent_files = super::push_recent(&self.recent, RecentMode::Backup, &path)
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
                progress.full_size_bytes = bar.full_size_bytes;
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

    /// Run the effects waiting; picker answers go back to the controller.
    fn run_effects(&mut self, ui: &egui::Ui, ctx: &mut TabContext) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(ui.ctx(), fx, &mut messages);
        replies.extend(self.effects.show(ui.ctx(), "backup"));
        for msg in messages {
            ctx.log.info(msg);
        }
        for reply in replies {
            let action = match reply {
                Reply::Picked { purpose, path } => BackupAction::Picked { purpose, path },
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => continue,
            };
            self.act(action);
        }
        if !self.pending_fx.is_empty() {
            ui.ctx().request_repaint();
        }
    }

    fn source_label(&self, ctx: &TabContext) -> String {
        if let Some(d) = self.ctl.remote_label() {
            format!("Remote: {d}")
        } else if let Some(path) = self.ctl.image_path() {
            format!("Image: {}", path.display())
        } else if let Some(idx) = self.ctl.device_slot() {
            ctx.devices
                .get(idx)
                .map(|d| d.display_name())
                .unwrap_or_else(|| "Unknown".into())
        } else {
            "Select a device or image...".into()
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext, progress: &mut ProgressState) {
        self.ctl.reconcile_devices(ctx.devices);
        self.poll(ctx, progress);

        ui.heading("Backup Disk");
        ui.add_space(8.0);

        if let Some(tab) = ctx.busy_elsewhere {
            ui.colored_label(
                super::theme::warning(ui.visuals()),
                super::context::busy_elsewhere_notice(tab),
            );
        }
        let running = self.ctl.is_running();
        let controls_enabled = !running && ctx.busy_elsewhere.is_none();

        self.show_source(ui, ctx, controls_enabled);
        #[cfg(feature = "remote")]
        self.show_remote_window(ui);
        self.show_partitions(ui, controls_enabled);
        self.show_options(ui, controls_enabled);

        ui.add_space(16.0);
        self.show_buttons(ui, ctx);
        if self.ctl.vhd_open() {
            self.show_vhd_window(ui, ctx);
        }
        self.run_effects(ui, ctx);
    }

    fn show_source(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext, controls_enabled: bool) {
        let mut actions = Vec::new();
        ui.horizontal(|ui| {
            ui.label("Source:");
            ui.add_enabled_ui(controls_enabled, |ui| {
                let current_label = self.source_label(ctx);
                let slot = self.ctl.device_slot();
                egui::ComboBox::from_id_salt("backup_source")
                    .selected_text(&current_label)
                    .width(400.0)
                    .height(400.0)
                    .show_ui(ui, |ui| {
                        for (i, device) in ctx.devices.iter().enumerate() {
                            if ui
                                .selectable_label(slot == Some(i), device.display_name())
                                .clicked()
                            {
                                actions.push(BackupAction::SelectDevice {
                                    slot: i,
                                    device: device.clone(),
                                });
                            }
                        }
                        if ctx.devices.is_empty() {
                            ui.label(
                                egui::RichText::new(super::no_devices_hint())
                                    .italics()
                                    .weak(),
                            );
                        }
                        ui.separator();
                        if ui
                            .selectable_label(self.ctl.image_path().is_some(), "Open Image File...")
                            .clicked()
                        {
                            actions.push(BackupAction::PickImage);
                        }
                        #[cfg(feature = "remote")]
                        {
                            ui.separator();
                            if ui
                                .selectable_label(self.ctl.remote_active(), "Remote Drive...")
                                .on_hover_text(
                                    "Back up a physical drive on a remote machine running \
                                     `rb-cli serve` (pulled over the network)",
                                )
                                .clicked()
                            {
                                actions.push(BackupAction::RemoteOpen);
                            }
                        }
                    });
                if let Some(path) = super::recent_combo(ui, "backup_recent", &self.recent_files, 5)
                {
                    actions.push(BackupAction::OpenImage { path, rename: true });
                }
            });
            if self.ctl.remote_active()
                && controls_enabled
                && ui
                    .button("X")
                    .on_hover_text("Disconnect / clear the remote source")
                    .clicked()
            {
                actions.push(BackupAction::RemoteClear);
            }
            if self.ctl.has_local_source()
                && controls_enabled
                && ui
                    .button("Close Drive")
                    .on_hover_text(
                        "Release the selected disk / image and clear the partition \
                         selection so the drive can be safely removed",
                    )
                    .clicked()
            {
                actions.push(BackupAction::CloseSource);
            }
        });

        // Devices are not auto-scanned (that would prompt for elevation on every pick).
        if self.ctl.can_scan_device() {
            let (label, hint) = if self.ctl.device_scanned() {
                (
                    "Reload partition info",
                    "Read the partition table again, e.g. after swapping the card in this reader",
                )
            } else {
                (
                    "Load partition info",
                    "Read the partition table from the selected device (may prompt for administrator credentials)",
                )
            };
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if ui.button(label).on_hover_text(hint).clicked() {
                    actions.push(BackupAction::ScanPartitions);
                }
            });
        }
        if let Some(snap) = self.ctl.decoding() {
            ui.add_space(4.0);
            super::progress::show_job(ui, &snap);
        }
        if let Some(snap) = self.ctl.scanning() {
            ui.add_space(4.0);
            if super::progress::show_job(ui, &snap) {
                actions.push(BackupAction::CancelScan);
            }
        }
        for a in actions {
            self.act(a);
        }
    }

    fn show_partitions(&mut self, ui: &mut egui::Ui, controls_enabled: bool) {
        let mut actions = Vec::new();
        if self.ctl.has_partitions() {
            ui.add_space(4.0);
            if let Some(desc) = self.ctl.table_desc() {
                let n = self.ctl.partition_count();
                ui.label(format!(
                    "Partition table: {} ({} partition{})",
                    desc,
                    n,
                    if n == 1 { "" } else { "s" },
                ));
            }
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Partitions:").strong());
                ui.add_enabled_ui(controls_enabled, |ui| {
                    if ui.small_button("All").clicked() {
                        actions.push(BackupAction::SelectAll);
                    }
                    if ui.small_button("None").clicked() {
                        actions.push(BackupAction::SelectNone);
                    }
                });
            });
            let rows = self.ctl.rows();
            ui.add_enabled_ui(controls_enabled, |ui| {
                egui::Grid::new("backup_partitions_grid")
                    .num_columns(7)
                    .spacing([12.0, 4.0])
                    .show(ui, |ui| {
                        ui.label(egui::RichText::new("Include").strong());
                        ui.label(egui::RichText::new("#").strong());
                        ui.label(egui::RichText::new("Type").strong());
                        ui.label(egui::RichText::new("Size").strong());
                        ui.label(egui::RichText::new("Min Size").strong());
                        ui.label(egui::RichText::new("Frag.").strong())
                            .on_hover_text("Percent of files with data in more than one extent");
                        ui.label(egui::RichText::new("Compact").strong())
                            .on_hover_text(
                                "Compact Space re-emits every allocated fork back-to-back, \
                                 closing the holes between files and producing a smaller backup. \
                                 Drastically increases backup time - recommended only when the \
                                 Min Size column shows meaningful savings vs. the in-place trim. \
                                 Available for HFS+/HFSX today; other layout-preserving \
                                 filesystems show the toggle as not implemented.",
                            );
                        ui.end_row();

                        for row in &rows {
                            let mut selected = row.selected;
                            if ui.checkbox(&mut selected, "").changed() {
                                actions.push(BackupAction::SetPartition(row.index, selected));
                            }
                            ui.label(format!("{}", row.position));
                            ui.label(&row.type_name);
                            ui.label(format_size(row.size_bytes));
                            match &row.min {
                                MinCell::Size(min) => {
                                    ui.label(format_size(*min));
                                }
                                MinCell::Pending(phase) => {
                                    ui.add(egui::Spinner::new()).on_hover_text(phase.as_str());
                                }
                                MinCell::CanCalc => {
                                    if ui
                                        .small_button("Calc min")
                                        .on_hover_text(
                                            "Compute minimum size (requires a filesystem walk)",
                                        )
                                        .clicked()
                                    {
                                        actions.push(BackupAction::CalcMin(row.index));
                                    }
                                }
                                MinCell::Empty => {
                                    ui.label("");
                                }
                            }
                            match row.fragmentation {
                                Some(pct) => ui.label(format!("{:.0}%", pct)),
                                None => ui.label(""),
                            };
                            match row.compact {
                                CompactCell::Hidden => {
                                    ui.label("");
                                }
                                CompactCell::NotImplemented => {
                                    let mut off = false;
                                    ui.add_enabled(false, egui::Checkbox::new(&mut off, ""))
                                        .on_hover_text(
                                            "Compact Space not yet implemented for this filesystem",
                                        );
                                }
                                CompactCell::Toggle(on) => {
                                    let mut on = on;
                                    if ui.checkbox(&mut on, "").changed() {
                                        actions.push(BackupAction::SetCompact(row.index, on));
                                    }
                                }
                            }
                            ui.end_row();
                        }
                    });
            });
            ui.add_space(4.0);
        }
        if let Some(err) = self.ctl.load_error() {
            ui.colored_label(super::theme::warning(ui.visuals()), err);
        }
        // Phase changes of a walk do not wake the UI, so keep the spinner moving.
        if self.ctl.min_jobs_running() {
            ui.ctx()
                .request_repaint_after(std::time::Duration::from_millis(80));
        }
        for a in actions {
            self.act(a);
        }
    }

    fn show_options(&mut self, ui: &mut egui::Ui, controls_enabled: bool) {
        let mut actions = Vec::new();
        ui.horizontal(|ui| {
            ui.label("Destination:");
            ui.add_enabled_ui(controls_enabled, |ui| {
                let label = self
                    .ctl
                    .destination()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "No folder selected".into());
                ui.label(&label);
                if ui.button("Browse...").clicked() {
                    actions.push(BackupAction::PickDestination);
                }
            });
        });
        ui.horizontal(|ui| {
            ui.label("Backup Name:");
            ui.add_enabled_ui(controls_enabled, |ui| {
                let mut name = self.ctl.name().to_string();
                if ui.text_edit_singleline(&mut name).changed() {
                    actions.push(BackupAction::SetName(name));
                }
            });
        });

        ui.add_space(12.0);
        ui.label(egui::RichText::new("Options").strong());
        ui.separator();

        let opts = self.ctl.options().clone();
        ui.add_enabled_ui(controls_enabled, |ui| {
            ui.horizontal(|ui| {
                ui.label("Output Format:");
                // DVD CHD is the Optical tab's: this tab backs up hard disks.
                let formats = [
                    (CompressionType::Chd, "CHD (Hard Disk)"),
                    (CompressionType::Vhd, "VHD"),
                    (CompressionType::Zstd, "zstd"),
                    (CompressionType::None, "None"),
                ];
                for (kind, label) in formats {
                    if ui.radio(opts.compression == kind, label).clicked() {
                        actions.push(BackupAction::SetFormat(kind));
                    }
                }
            });
            if opts.compression == CompressionType::Chd {
                ui.label(
                    egui::RichText::new("Output: single CHD file (chdman/MAME compatible).")
                        .small()
                        .weak(),
                );
                super::chd_options_ui::show(
                    ui,
                    "backup_chd",
                    ChdProfile::Hd,
                    &mut self.chd_hd_control,
                );
            }

            let mut sbs = opts.sector_by_sector;
            if ui
                .checkbox(&mut sbs, "Sector-by-sector copy (includes all blank space)")
                .changed()
            {
                actions.push(BackupAction::SetSectorBySector(sbs));
            }
            ui.add_enabled_ui(!opts.sector_by_sector, |ui| {
                ui.horizontal(|ui| {
                    let mut resize = opts.resize;
                    if ui
                        .checkbox(&mut resize, "Resize partitions to minimum size")
                        .changed()
                    {
                        actions.push(BackupAction::SetResize(resize));
                    }
                    if opts.sector_by_sector {
                        ui.label("(not available with sector-by-sector copy)");
                    }
                });
            });
            ui.add_enabled_ui(!opts.sector_by_sector, |ui| {
                ui.horizontal(|ui| {
                    let mut keep = opts.keep_swap;
                    if ui
                        .checkbox(&mut keep, "Keep swap/page-file content (don't exclude)")
                        .on_hover_text(
                            "By default a FAT volume's swap/page files (WIN386.SWP, \
                             386SPART.PAR, PAGEFILE.SYS, HIBERFIL.SYS, SWAPPER.DAT) are \
                             imaged full-size but zeroed -- they reinitialize on boot, \
                             and the codec crushes the zeros. Check to image them as-is.",
                        )
                        .changed()
                    {
                        actions.push(BackupAction::SetKeepSwap(keep));
                    }
                    if opts.sector_by_sector {
                        ui.label("(not available with sector-by-sector copy)");
                    }
                });
            });
            if opts.split_shown() {
                let editable = opts.split_editable();
                ui.horizontal(|ui| {
                    ui.add_enabled_ui(editable, |ui| {
                        let mut split = opts.split;
                        if ui.checkbox(&mut split, "Split archives").changed() {
                            actions.push(BackupAction::SetSplit(split));
                        }
                        if opts.split && editable {
                            ui.label("Split size (MiB):");
                            let mut mib = opts.split_mib;
                            if ui
                                .add(egui::DragValue::new(&mut mib).range(100..=64000))
                                .changed()
                            {
                                actions.push(BackupAction::SetSplitMib(mib));
                            }
                        }
                    });
                    if !editable {
                        ui.label("(not available for VHD)");
                    }
                });
            }
            ui.horizontal(|ui| {
                ui.label("Checksum:");
                for (kind, label) in [
                    (ChecksumType::Sha256, "SHA-256"),
                    (ChecksumType::Crc32, "CRC32"),
                ] {
                    if ui.radio(opts.checksum == kind, label).clicked() {
                        actions.push(BackupAction::SetChecksum(kind));
                    }
                }
            });
        });
        for a in actions {
            self.act(a);
        }
    }

    /// The CHD options Start carries, from the options control.
    fn chd_choice(&self) -> Option<ChdChoice> {
        (self.ctl.options().compression == CompressionType::Chd).then(|| ChdChoice {
            options: self.chd_hd_control.effective(ChdProfile::Hd),
            custom: self.chd_hd_control.mode == ChdOptionsMode::Custom,
        })
    }

    fn show_buttons(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        let mut actions = Vec::new();
        ui.horizontal(|ui| {
            if !self.ctl.is_running() {
                let chd_ok = self.ctl.options().compression != CompressionType::Chd
                    || self.chd_hd_control.is_valid(ChdProfile::Hd);
                let waiting = self.ctl.start_pending();
                let label = if waiting {
                    "Computing minimum sizes..."
                } else {
                    "Start Backup"
                };
                let enabled = self.ctl.can_start() && chd_ok && ctx.busy_elsewhere.is_none();
                if ui.add_enabled(enabled, egui::Button::new(label)).clicked() {
                    actions.push(BackupAction::Start(self.chd_choice()));
                }
                if waiting {
                    ui.add(egui::Spinner::new())
                        .on_hover_text("Waiting for minimum-size calculations to finish");
                    if ui.button("Cancel").clicked() {
                        actions.push(BackupAction::CancelPendingStart);
                    }
                }
            } else if ui.button("Cancel").clicked() {
                actions.push(BackupAction::Cancel);
            }
        });
        for a in actions {
            self.act(a);
        }
    }

    fn show_vhd_window(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        let mut actions = Vec::new();
        let whole = self.ctl.vhd_whole_disk();
        let rows = self.ctl.vhd_rows().to_vec();
        egui::Window::new("VHD Backup Options")
            .collapsible(false)
            .resizable(true)
            .default_width(500.0)
            .show(ui.ctx(), |ui| {
                ui.label("Configure VHD output.");
                ui.add_space(4.0);
                if ui.radio(whole, "Whole Disk (single .vhd file)").clicked() {
                    actions.push(BackupAction::VhdWholeDisk(true));
                }
                if ui
                    .radio(!whole, "Per Partition (one .vhd per partition)")
                    .clicked()
                {
                    actions.push(BackupAction::VhdWholeDisk(false));
                }
                ui.add_space(8.0);
                if !rows.is_empty() {
                    ui.label(egui::RichText::new("Partition Sizes:").strong());
                    egui::Grid::new("vhd_backup_partition_sizes")
                        .striped(true)
                        .min_col_width(50.0)
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("#").strong());
                            ui.label(egui::RichText::new("Type").strong());
                            ui.label(egui::RichText::new("Size Mode").strong());
                            ui.label(egui::RichText::new("Size (MiB)").strong());
                            ui.end_row();
                            for row in &rows {
                                ui.label(format!("{}", row.index));
                                ui.label(&row.type_name);
                                let phase = self.ctl.min_phase(row.index);
                                let deferred = if let Some(phase) = &phase {
                                    Some(super::size_mode_row::DeferredMin::Pending {
                                        phase: phase.as_str(),
                                    })
                                } else {
                                    row.deferred_fs.map(|fs_name| {
                                        super::size_mode_row::DeferredMin::Available { fs_name }
                                    })
                                };
                                let mut mode = row.mode;
                                let mut custom = row.custom_mib;
                                let action = super::size_mode_row::size_mode_row(
                                    ui,
                                    &mut mode,
                                    &mut custom,
                                    row.original_size,
                                    row.minimum_size,
                                    super::size_mode_row::SizeModeRowOptions {
                                        max_size: Some(row.original_size),
                                        deferred,
                                        ..Default::default()
                                    },
                                );
                                if mode != row.mode || custom != row.custom_mib {
                                    actions.push(BackupAction::VhdSize {
                                        index: row.index,
                                        mode,
                                        custom_mib: custom,
                                    });
                                }
                                if action
                                    == super::size_mode_row::SizeModeRowAction::CalcMinRequested
                                {
                                    actions.push(BackupAction::CalcMin(row.index));
                                }
                                ui.end_row();
                            }
                        });
                    ui.add_space(4.0);
                }
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(
                            ctx.busy_elsewhere.is_none(),
                            egui::Button::new("Start VHD Backup"),
                        )
                        .clicked()
                    {
                        actions.push(BackupAction::VhdStart);
                    }
                    if ui.button("Cancel").clicked() {
                        actions.push(BackupAction::VhdClose);
                    }
                });
            });
        for a in actions {
            self.act(a);
        }
    }

    /// The "Remote Drive" window: connect lists the daemon's drives; a pick becomes the source.
    #[cfg(feature = "remote")]
    fn show_remote_window(&mut self, ui: &mut egui::Ui) {
        if !self.ctl.remote_dialog_open() {
            return;
        }
        let mut actions = Vec::new();
        let busy = self.ctl.remote_busy();
        egui::Window::new("Remote Drive")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.horizontal(|ui| {
                    ui.label("Host:");
                    let mut addr = self.ctl.remote_address().to_string();
                    if ui
                        .add(
                            egui::TextEdit::singleline(&mut addr)
                                .hint_text("192.168.1.50:7341")
                                .desired_width(220.0),
                        )
                        .changed()
                    {
                        actions.push(BackupAction::RemoteSetAddress(addr));
                    }
                });
                ui.horizontal(|ui| {
                    ui.add_enabled_ui(!busy, |ui| {
                        if ui.button("Connect").clicked() {
                            actions.push(BackupAction::RemoteConnect);
                        }
                    });
                    if busy {
                        ui.add(egui::Spinner::new());
                    }
                    if ui.button("Close").clicked() {
                        actions.push(BackupAction::RemoteClose);
                    }
                });
                if let Some(err) = self.ctl.remote_error() {
                    ui.colored_label(super::theme::danger(ui.visuals()), err);
                }
                if self.ctl.remote_listed() {
                    ui.separator();
                    let drives = self.ctl.remote_drives();
                    if drives.is_empty() {
                        ui.label(
                            "No physical drives reported. The daemon may need to run as root.",
                        );
                    } else {
                        ui.label("Pick a drive to back up:");
                        egui::ScrollArea::vertical()
                            .max_height(220.0)
                            .show(ui, |ui| {
                                for d in drives {
                                    if ui.selectable_label(false, d.label.as_str()).clicked() {
                                        actions.push(BackupAction::RemotePick {
                                            path: d.path,
                                            size: d.size_bytes,
                                        });
                                    }
                                }
                            });
                    }
                }
            });
        for a in actions {
            self.act(a);
        }
    }
}
