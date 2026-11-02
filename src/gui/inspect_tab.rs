//! The Inspect tab: the egui shell of `controller::inspect::InspectController`.
//!
//! The tab draws the source picker, the partition grid and the tool windows, maps clicks to
//! `InspectAction`s, answers the controller's pickers and confirmations through an `EffectHost`,
//! moves its log lines into the log panel and carries out its `HostRequest`s (the browse view,
//! the Optical and Archives hand-offs, Expand HFS). Dialogs with their own widgets (resize,
//! Expand HFS, Alto resize, floppy convert, physical export) take their inputs from the controller.

#[cfg(feature = "rust173-polyfill")]
use rusty_backup::rust173_compat::IntIsMultipleOf as _;
use std::path::PathBuf;
use std::sync::Arc;

use rusty_backup::controller::inspect::{
    format_size, BackupSummary, ChdProfile, ClonezillaSummary, ExportFormat, HostRequest,
    InspectAction, InspectController, InspectServices, InspectSource, LayoutBar, LayoutSegment,
    MinCell, SegmentKind,
};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{Controller as _, Effect};

use super::alto_resize_dialog::AltoResizeDialog;
use super::browse_view::BrowseView;
use super::chd_options_ui::ChdOptionsControl;
use super::context::TabContext;
use super::effects::{EffectHost, Reply};
use super::expand_hfs_dialog::ExpandHfsDialog;
use super::floppy_convert_dialog::FloppyConvertDialog;
use super::physical_disk_export::{PhysicalDiskExport, PhysicalDiskExportSource};

fn new_controller() -> InspectController {
    InspectController::new(InspectServices {
        jobs: rusty_backup::controller::inspect::JobRegistry::new(),
        notify: Arc::new(super::ui_logger::EguiWake),
    })
}

pub struct InspectTab {
    ctl: InspectController,
    /// Pickers and confirmations the controller asks for.
    effects: EffectHost,
    /// Effects from a call made outside `show`, run on the next frame.
    pending_fx: Vec<Effect>,
    /// In-memory mirror of the Inspect recent-image list (config.json), for the picker.
    recent_files: Vec<String>,
    /// The user closed a backup; the app clears the shared backup folder.
    close_backup_requested: bool,
    pending_open_archive: Option<PathBuf>,
    pending_open_optical: Option<PathBuf>,
    /// The partition list's text widened; the next frame repaints so the grid re-measures.
    layout_seen: u64,
    browse_view: BrowseView,
    /// "Connect to Remote...": an inline panel browsing an `rb-cli serve` daemon.
    remote_browser: super::remote_browser::RemoteBrowsePanel,
    export_chd_hd_control: ChdOptionsControl,
    expand_image_dialog_open: bool,
    expand_image_add_mib: u32,
    resize_popup: Option<Box<super::resize_popup::ResizePopup>>,
    expand_hfs_dialog: Option<ExpandHfsDialog>,
    alto_resize_dialog: Option<AltoResizeDialog>,
    floppy_convert_dialog: Option<FloppyConvertDialog>,
    physical_disk_export: PhysicalDiskExport,
}

impl Default for InspectTab {
    fn default() -> Self {
        Self {
            ctl: new_controller(),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
            recent_files: super::load_recent(rusty_backup::update::RecentMode::Inspect),
            close_backup_requested: false,
            pending_open_archive: None,
            pending_open_optical: None,
            layout_seen: 0,
            browse_view: BrowseView::default(),
            remote_browser: super::remote_browser::RemoteBrowsePanel::default(),
            export_chd_hd_control: ChdOptionsControl::new(ChdProfile::Hd),
            expand_image_dialog_open: false,
            expand_image_add_mib: 0,
            resize_popup: None,
            expand_hfs_dialog: None,
            alto_resize_dialog: None,
            floppy_convert_dialog: None,
            physical_disk_export: PhysicalDiskExport::default(),
        }
    }
}

impl InspectTab {
    pub fn get_loaded_backup(&self) -> Option<PathBuf> {
        self.ctl.backup_folder().map(|p| p.to_path_buf())
    }

    pub fn has_backup(&self) -> bool {
        self.ctl.backup_folder().is_some()
    }

    /// A device, image or backup is open; a dropped file then belongs to the browse view, not a new open.
    pub fn has_loaded_source(&self) -> bool {
        self.ctl.has_local_source()
    }

    /// True while a remote inspect session owns the tab.
    pub fn is_remote_session(&self) -> bool {
        self.ctl.is_remote()
    }

    /// The user closed a backup here; the app drops the shared folder so it is not reopened.
    pub fn take_close_backup_request(&mut self) -> bool {
        std::mem::take(&mut self.close_backup_requested)
    }

    /// A Mac archive picked in the source dropdown, for the Archives tab.
    pub fn take_open_archive_request(&mut self) -> Option<PathBuf> {
        self.pending_open_archive.take()
    }

    /// A browsable optical disc the open found, for the Optical tab.
    pub fn take_open_optical_request(&mut self) -> Option<PathBuf> {
        self.pending_open_optical.take()
    }

    pub fn load_backup(&mut self, path: &std::path::Path) {
        if self.ctl.backup_folder() != Some(path) {
            self.act(InspectAction::Load(InspectSource::Backup {
                folder: path.to_path_buf(),
            }));
        }
    }

    /// Open an image handed over by another tab, keeping `guard` (its temp folder) alive.
    pub fn load_image_with_tempdir(&mut self, path: PathBuf, guard: Option<tempfile::TempDir>) {
        self.recent_files = super::push_recent(rusty_backup::update::RecentMode::Inspect, &path);
        self.act(InspectAction::Load(InspectSource::Image {
            path,
            temp: guard,
        }));
    }

    /// Drop the in-memory recent list after Settings cleared the persisted one.
    pub(crate) fn clear_recent_files(&mut self) {
        self.recent_files.clear();
    }

    /// The top bar's Close Backup: close the backup if one is open here.
    pub fn clear_backup(&mut self) {
        if self.ctl.backup_folder().is_some() {
            self.act(InspectAction::Unload { quiet: true });
            self.close_backup_requested = false;
        }
    }

    /// Send an action; its effects run on the next frame.
    fn act(&mut self, action: InspectAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
        self.carry_out_requests();
    }

    /// Drain the controller while another tab shows, so a finished job reports and frees the busy gate.
    pub fn poll_background(&mut self, ctx: &mut TabContext) {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        self.carry_out_requests();
        self.drain_log(ctx);
    }

    fn drain_log(&mut self, ctx: &mut TabContext) {
        for (level, msg) in self.ctl.take_log() {
            match level {
                LogLevel::Info => ctx.log.info(msg),
                LogLevel::Warning => ctx.log.warn(msg),
                LogLevel::Error => ctx.log.error(msg),
            }
        }
    }

    fn carry_out_requests(&mut self) {
        for req in self.ctl.take_requests() {
            match req {
                HostRequest::CloseBrowse => self.browse_view.close(),
                HostRequest::OpenBrowse(open) => self.browse_view.open_request(*open),
                HostRequest::UpgradeSeekable(path) => {
                    self.browse_view.upgrade_to_seekable_cache(path)
                }
                HostRequest::OpenOptical(path) => self.pending_open_optical = Some(path),
                HostRequest::OpenArchive(path) => self.pending_open_archive = Some(path),
                HostRequest::BackupClosed => self.close_backup_requested = true,
                HostRequest::ExpandHfs(source) => {
                    self.expand_hfs_dialog = Some(ExpandHfsDialog::new(*source))
                }
            }
        }
    }

    /// Run the effects waiting and the modal on screen; answers go back to the controller.
    fn run_effects(&mut self, ui: &egui::Ui, ctx: &mut TabContext) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(ui.ctx(), fx, &mut messages);
        replies.extend(self.effects.show(ui.ctx(), "inspect"));
        for msg in messages {
            ctx.log.info(msg);
        }
        for reply in replies {
            let action = match reply {
                Reply::Picked { purpose, path } => InspectAction::Picked { purpose, path },
                Reply::Confirmed { purpose, yes } => InspectAction::Confirmed { purpose, yes },
                Reply::Answered { .. } | Reply::CloseScreen => continue,
            };
            self.act(action);
        }
        if !self.pending_fx.is_empty() {
            ui.ctx().request_repaint();
        }
    }

    /// The running Inspect job other tabs must wait for (export, expand, repack, resize, ...), if any.
    pub fn running_job(&self) -> Option<&'static str> {
        if let Some(job) = self.ctl.running_job() {
            Some(job)
        } else if self.resize_popup.as_ref().is_some_and(|r| r.is_running()) {
            Some("Inspect: Resize")
        } else if self
            .expand_hfs_dialog
            .as_ref()
            .is_some_and(|d| d.is_running())
        {
            Some("Inspect: Expand HFS")
        } else if self
            .floppy_convert_dialog
            .as_ref()
            .is_some_and(|d| d.is_running())
        {
            Some("Inspect: Floppy convert")
        } else {
            self.browse_view.running_job().map(|_| "Inspect: Browse")
        }
    }

    fn source_label(&self, ctx: &TabContext) -> String {
        if let Some(rpath) = self.ctl.remote_path() {
            let base = rpath.rsplit('/').find(|s| !s.is_empty()).unwrap_or(rpath);
            format!("Remote image: {base}")
        } else if let Some(path) = self.ctl.backup_folder() {
            format!("Backup: {}", path.display())
        } else if let Some(path) = self.ctl.image_path() {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            match self.ctl.format_label() {
                Some(label) => format!("{label}: {name}"),
                None => format!("Image: {name}"),
            }
        } else if let Some(idx) = self.ctl.device_slot() {
            ctx.devices
                .get(idx)
                .map(|d| d.display_name())
                .unwrap_or_else(|| "Unknown".into())
        } else {
            "Select a device or image...".into()
        }
    }

    /// The source picker's event as a source to open; `None` for one Inspect does not take.
    fn source_for(
        &mut self,
        ev: super::source_picker::SourceEvent,
        ctx: &TabContext,
    ) -> Option<InspectSource> {
        use super::source_picker::SourceEvent;
        match ev {
            SourceEvent::Device(i) => ctx.devices.get(i).map(|d| InspectSource::Device {
                slot: i,
                identity: d.identity(),
            }),
            SourceEvent::Image { path, tempdir } => Some(InspectSource::Image {
                path,
                temp: tempdir,
            }),
            // A recent pick is a raw path; materialize it like re-picking the file.
            SourceEvent::Recent(path) => match super::prepare_disk_image_path(&path, false) {
                Ok((materialized, guard)) => Some(InspectSource::Image {
                    path: materialized,
                    temp: guard,
                }),
                Err(e) => {
                    log::error!("Failed to decompress {}: {}", path.display(), e);
                    Some(InspectSource::Image { path, temp: None })
                }
            },
            SourceEvent::BackupFolder(folder) => Some(InspectSource::Backup { folder }),
            // Inspect offers no host folder, and runs its own dialogs.
            SourceEvent::HostFolder(_)
            | SourceEvent::Remote
            | SourceEvent::PickImage
            | SourceEvent::PickHostFolder
            | SourceEvent::PickBackupFolder => None,
        }
    }

    fn show_source_bar(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        ui.horizontal(|ui| {
            ui.label("Source:");
            let current_label = self.source_label(ctx);
            // The shared source picker, as the Commander panes use; recents sit at its top.
            let cfg = super::source_picker::PickerConfig {
                show_devices: true,
                show_image: true,
                show_host_folder: false,
                show_backup_folder: true,
                show_remote: true,
                materialize_image: true,
                include_mac_archives: true,
                width: 400.0,
                // A fixed 5-row Recent group keeps egui's cached popup size right.
                recent_slots: 5,
                run_dialogs: true,
            };
            let state = super::source_picker::PickerState {
                selected_device_idx: self.ctl.device_slot(),
                image_active: self.ctl.image_path().is_some(),
                host_active: false,
                backup_active: self.ctl.backup_folder().is_some(),
                devices: ctx.devices,
                recent: &self.recent_files,
            };
            if let Some(ev) =
                super::source_picker::show(ui, "inspect_source", &cfg, &current_label, &state)
            {
                if matches!(ev, super::source_picker::SourceEvent::Remote) {
                    self.remote_browser.open();
                } else {
                    self.remote_browser.disconnect();
                    if let Some(source) = self.source_for(ev, ctx) {
                        self.act(InspectAction::Open(source));
                    }
                }
            }

            self.remote_browser.source_bar_controls(ui);

            // A remote image: switch images on the same connection, back it up, or close it.
            #[cfg(feature = "remote")]
            if let Some((conn, _)) = self.ctl.remote().cloned() {
                if ui
                    .button("Pick Another Image")
                    .on_hover_text("Browse the daemon for another image (no reconnect)")
                    .clicked()
                {
                    self.remote_browser.browse_on(conn);
                }
                let backing_up = self.ctl.remote_backup_running();
                if ui
                    .add_enabled(
                        !self.ctl.partitions().is_empty() && !backing_up,
                        egui::Button::new("Back Up Image..."),
                    )
                    .on_hover_text("Pull a full backup of this remote image to a local folder")
                    .clicked()
                {
                    self.act(InspectAction::BackupRemote);
                }
                if backing_up {
                    ui.add(egui::Spinner::new());
                    ui.label("Backing up...");
                }
                if ui
                    .add_enabled(!backing_up, egui::Button::new("Close Remote"))
                    .on_hover_text("Disconnect from the remote daemon")
                    .clicked()
                {
                    self.act(InspectAction::Unload { quiet: true });
                }
            }
        });
    }

    fn show_warnings(&mut self, ui: &mut egui::Ui) {
        // Raw disks belong to root: say so beside the picker, before Edit Mode fails obscurely.
        if InspectController::needs_elevation_note() {
            ui.add_space(2.0);
            ui.label(
                egui::RichText::new(super::unelevated_device_note())
                    .color(super::theme::warning(ui.visuals())),
            );
        }
        // A flux image decodes unreadable sectors as zeros; name them next to the picker.
        if let Some(report) = self.ctl.flux_faults() {
            ui.add_space(2.0);
            ui.label(
                egui::RichText::new(format!(
                    "Warning: flux decode: {}. Files using these sectors are damaged.",
                    report.summary
                ))
                .color(super::theme::warning(ui.visuals())),
            );
            egui::CollapsingHeader::new(format!("Unreadable sectors ({})", report.sectors.len()))
                .id_salt("flux_faults")
                .show(ui, |ui| {
                    egui::ScrollArea::vertical()
                        .max_height(160.0)
                        .show(ui, |ui| {
                            for line in &report.sectors {
                                ui.label(line);
                            }
                        });
                });
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        ui.heading("Inspect Disk / Image");
        ui.add_space(8.0);

        self.ctl
            .set_browse_dirty(self.browse_view.has_unsaved_edits());
        self.ctl.reconcile_devices(ctx.devices);
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        self.carry_out_requests();

        self.show_source_bar(ui, ctx);
        self.show_warnings(ui);
        ui.add_space(4.0);

        // The remote picker takes over the main area until it hands an image over.
        let remote_active = self.remote_browser.show(ui);
        for line in self.remote_browser.take_log() {
            ctx.log.info(format!("Remote: {line}"));
        }
        #[cfg(feature = "remote")]
        if let Some(conn) = self.remote_browser.take_inspect_request() {
            self.act(InspectAction::Load(InspectSource::Remote { conn }));
        }
        self.run_effects(ui, ctx);
        self.drain_log(ctx);
        if remote_active {
            return;
        }

        if let Some(snap) = self.ctl.loading() {
            super::progress::show_job(ui, &snap);
        }
        self.show_tool_windows(ui);

        if self.ctl.probing() || self.ctl.swabbing() {
            ui.ctx()
                .request_repaint_after(std::time::Duration::from_millis(80));
        }
        if self.layout_seen != self.ctl.layout_generation() {
            self.layout_seen = self.ctl.layout_generation();
            ui.ctx().request_repaint();
        }

        self.show_button_bar(ui);

        if let Some(snap) = self.ctl.export_job() {
            if super::progress::show_job(ui, &snap) {
                self.act(InspectAction::CancelExport);
            }
        }

        if self.ctl.export_open() {
            self.show_export_popup(ui);
        }
        if self.ctl.editor_open() {
            self.show_editor_popup(ui);
        }
        if self.expand_image_dialog_open {
            self.show_expand_image_dialog(ui);
        }

        let mut resize_finished = false;
        if let Some(ref mut popup) = self.resize_popup {
            let was_running = popup.is_running();
            popup.poll_status(ctx.log);
            resize_finished = was_running && !popup.is_running();
            if !popup.show(ui, ctx.devices, ctx.log) {
                self.resize_popup = None;
            }
        }
        // The table on disk changed under every later edit; read it again.
        if resize_finished {
            self.act(InspectAction::DiskChanged);
        }

        if self.physical_disk_export.open {
            let _ = self.physical_disk_export.show(ui.ctx(), ctx);
        }

        ui.add_space(12.0);

        if let Some(err) = self.ctl.last_error() {
            ui.colored_label(super::theme::danger(ui.visuals()), format!("Error: {err}"));
            ui.add_space(8.0);
        }

        for snap in self.ctl.browse_jobs() {
            super::progress::show_job(ui, &snap);
        }

        if let Some(meta) = self.ctl.backup_summary() {
            show_backup_metadata(ui, &meta);
        }
        if let Some(cz) = self.ctl.clonezilla_summary() {
            show_clonezilla_metadata(ui, &cz);
        }

        if self.ctl.table().is_some() {
            self.show_results(ui, ctx);
        } else if !self.ctl.partitions().is_empty() {
            self.show_partition_list(ui, ctx);
        }

        if self.browse_view.is_active() {
            self.browse_view.show(ui);
        }
        if let Some((path, guard)) = self.browse_view.take_nested_open() {
            ctx.log
                .info(format!("Inspecting {} as a disk image", path.display()));
            // A temp copy out of the browsed volume: kept out of the recents.
            self.act(InspectAction::Load(InspectSource::Image {
                path,
                temp: Some(guard),
            }));
        }
        self.run_effects(ui, ctx);
        self.drain_log(ctx);
    }

    fn show_button_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            let loading = self.ctl.loading().is_some();
            if ui
                .add_enabled(
                    self.ctl.has_source() && !loading,
                    egui::Button::new("Re-inspect"),
                )
                .clicked()
            {
                self.act(InspectAction::Reinspect);
            }

            if ui
                .add_enabled(
                    self.ctl.can_export(),
                    egui::Button::new("Export Disk Image..."),
                )
                .clicked()
            {
                self.act(InspectAction::OpenExport);
            }

            // Only shown for a floppy container, so the long button does not crowd an HDD's bar.
            if self.ctl.source_is_floppy() {
                if let Some(src) = self.ctl.image_path().map(|p| p.to_path_buf()) {
                    if ui
                        .button("Convert Floppy Container...")
                        .on_hover_text(
                            "Convert between XDF, HDM, DIM, and D88 floppy container \
                             formats (X68000 / PC-98 / FM-7).",
                        )
                        .clicked()
                    {
                        self.floppy_convert_dialog = Some(FloppyConvertDialog::with_source(src));
                    }
                }
            }

            let editable = self.ctl.can_edit_table();
            if ui
                .add_enabled(editable, egui::Button::new("Edit Partition Table..."))
                .clicked()
            {
                self.act(InspectAction::EditTable);
            }

            // Pre-fills the editor for the largest gap, between partitions or at the tail.
            let gap = self.ctl.largest_gap();
            let gap_bytes = gap.as_ref().map(|g| g.size_bytes).unwrap_or(0);
            if ui
                .add_enabled(
                    editable && gap_bytes >= 1024 * 1024,
                    egui::Button::new("Add Partition..."),
                )
                .on_hover_text(match &gap {
                    Some(gap) if gap.size_bytes >= 1024 * 1024 => format!(
                        "Add a new partition into the {} gap at LBA {}",
                        format_size(gap.size_bytes),
                        gap.start_lba,
                    ),
                    _ => "No free space available".to_string(),
                })
                .clicked()
            {
                self.act(InspectAction::AddPartition);
            }

            // Grows the image with zeros so a partition can be added or extended into it.
            let is_image = self.ctl.is_image_file();
            let expanding = self.ctl.expanding();
            if ui
                .add_enabled(
                    self.ctl.can_expand_image(),
                    egui::Button::new("Expand Image..."),
                )
                .on_hover_text(if !is_image {
                    "Only image files can be expanded - devices have fixed size.".to_string()
                } else if expanding {
                    "Expansion already running.".to_string()
                } else {
                    "Grow the image file with zero-padding so you can add or extend \
                     partitions in the new free space."
                        .to_string()
                })
                .clicked()
            {
                self.expand_image_dialog_open = true;
                self.expand_image_add_mib = 0;
            }

            if ui
                .add_enabled(
                    self.ctl.can_make_bootable(),
                    egui::Button::new("Make Bootable..."),
                )
                .on_hover_text(
                    "Auto-detect what this Mac disk needs to boot (SCSI driver + DDR on a \
                     full APM disk, boot blocks, blessed System Folder) and apply only the \
                     missing pieces. A flat HFV is kept flat; you'll be asked for a donor \
                     disk only if boot blocks are missing.",
                )
                .clicked()
            {
                self.act(InspectAction::MakeBootable);
            }

            let resize_running = self.resize_popup.as_ref().is_some_and(|p| p.is_running());
            if ui
                .add_enabled(
                    editable && !resize_running,
                    egui::Button::new("Resize Partitions..."),
                )
                .clicked()
            {
                self.open_resize_popup();
            }

            if self.ctl.chd_path().is_some() && ui.button("CHD Info").clicked() {
                self.act(InspectAction::ChdInfo);
            }

            let exporting = self.ctl.export_running();
            if self.ctl.has_local_source() && !loading && !exporting && ui.button("Close").clicked()
            {
                self.act(InspectAction::Close);
            }
            if exporting && ui.button("Cancel Export").clicked() {
                self.act(InspectAction::CancelExport);
            }
        });
    }

    fn open_resize_popup(&mut self) {
        let Some(plan) = self.ctl.resize_plan() else {
            return;
        };
        self.resize_popup = Some(Box::new(super::resize_popup::ResizePopup::new(
            plan.partitions.as_slice(),
            plan.table,
            &plan.min_sizes,
            plan.alignment_sectors,
            super::resize_popup::ContainerInfo {
                size_bytes: plan.size_bytes,
                is_device: plan.is_device,
                can_grow: plan.can_grow,
            },
            plan.path,
        )));
    }

    /// Small windows for running tools (check, table write, Make Bootable, expand, defragment).
    fn show_tool_windows(&mut self, ui: &mut egui::Ui) {
        for snap in self.ctl.tool_jobs() {
            egui::Window::new(snap.label.clone())
                .collapsible(false)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    super::progress::show_job(ui, &snap);
                });
        }
    }

    /// Grow the image by N MiB of zeros; re-inspect afterwards to see the new free space.
    fn show_expand_image_dialog(&mut self, ui: &mut egui::Ui) {
        let mut open = self.expand_image_dialog_open;
        let mut do_expand = false;
        let mut cancel = false;
        let current_size = self.ctl.disk_size().unwrap_or(0);

        egui::Window::new("Expand Image")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .default_width(420.0)
            .show(ui.ctx(), |ui| {
                ui.label(format!("Current size: {}", format_size(current_size)));
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    ui.label("Add free space:");
                    ui.add(
                        egui::DragValue::new(&mut self.expand_image_add_mib)
                            .range(0..=4_194_304u32) // 4 TiB upper bound
                            .suffix(" MiB"),
                    );
                });
                let add_bytes = (self.expand_image_add_mib as u64) * 1024 * 1024;
                ui.label(format!(
                    "New size: {}",
                    format_size(current_size.saturating_add(add_bytes)),
                ));
                ui.add_space(6.0);
                ui.label(
                    egui::RichText::new(
                        "After expansion, click Re-inspect to refresh the partition \
                         list, then use 'Add Partition...' or 'Edit Partition Table...' \
                         to allocate the new space.",
                    )
                    .weak(),
                );
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(self.expand_image_add_mib > 0, egui::Button::new("Expand"))
                        .clicked()
                    {
                        do_expand = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                });
            });

        if !open || cancel {
            self.expand_image_dialog_open = false;
            return;
        }
        if do_expand {
            let add_bytes = (self.expand_image_add_mib as u64) * 1024 * 1024;
            self.act(InspectAction::ExpandImage(add_bytes));
            self.expand_image_dialog_open = false;
        }
    }

    fn show_editor_popup(&mut self, ui: &mut egui::Ui) {
        let allow_apply = self.ctl.backup_folder().is_none();
        let (editor, partitions, table) = self.ctl.editor_parts();
        let action = super::partition_editor_modal::show(
            ui,
            super::partition_editor_modal::Mode::EditExisting {
                editor,
                partitions,
                table,
            },
            allow_apply,
        );
        match action {
            super::partition_editor_modal::Action::Stay => {}
            super::partition_editor_modal::Action::Close => self.act(InspectAction::CloseEditor),
            super::partition_editor_modal::Action::Apply => {
                self.act(InspectAction::ApplyTableEdits)
            }
        }
    }

    fn show_export_popup(&mut self, ui: &mut egui::Ui) {
        let chd_allowed = self.ctl.chd_export_allowed();
        let can_physical = self.ctl.can_export_physical();
        let layout = self.ctl.export_layout();
        let notes = self.ctl.export_notes();
        let mut start = false;
        let mut physical = false;
        let mut cancel = false;
        let chd_control = &mut self.export_chd_hd_control;
        let s = self.ctl.export_settings_mut();
        let whole_disk_only = s.whole_disk_only();
        let is_hfv = s.per_partition_only();
        let format = s.format;

        egui::Window::new("Export Disk Image")
            .collapsible(false)
            .resizable(true)
            .default_width(500.0)
            .show(ui.ctx(), |ui| {
                ui.label("Export partitions as disk image files.");
                ui.add_space(4.0);
                // Current vs After at one byte-per-pixel scale, so shrinks and grows show.
                show_export_disk_layout_bars(ui, &layout);
                ui.add_space(8.0);

                let whole = ui.add_enabled(
                    !is_hfv,
                    egui::RadioButton::new(s.whole_disk, "Whole Disk (single file)"),
                );
                if whole.clicked() && !is_hfv {
                    s.whole_disk = true;
                }
                if is_hfv {
                    whole.on_hover_text(
                        "HFV is a single classic-HFS volume with no partition table, so it is \
                         exported per-partition (one .hfv per HFS partition).",
                    );
                }
                let per_part = ui.add_enabled(
                    !whole_disk_only,
                    egui::RadioButton::new(
                        !s.whole_disk && !whole_disk_only,
                        "Per Partition (one file per partition)",
                    ),
                );
                if per_part.clicked() && !whole_disk_only {
                    s.whole_disk = false;
                }
                if let Some(why) = per_partition_refusal(format) {
                    per_part.on_hover_text(why);
                }

                ui.add_space(4.0);
                ui.label(egui::RichText::new("Format:").strong());
                ui.horizontal_wrapped(|ui| {
                    format_radios(ui, &mut s.format, chd_allowed);
                });

                if s.format == ExportFormat::Chd {
                    ui.add_space(4.0);
                    super::chd_options_ui::show(ui, "inspect_chd", ChdProfile::Hd, chd_control);
                }

                ui.add_space(8.0);
                if !s.configs.is_empty() {
                    ui.label(egui::RichText::new("Partition Sizes:").strong())
                        .on_hover_text(
                            "Minimum is the trim point: the imaged size of a compacted backup \
                             partition, or the last used byte of a live volume. The exported \
                             volume is resized in place to match. Anything smaller needs a \
                             Backup with shrink-to-minimum, which clones the volume.",
                        );
                    egui::Grid::new("export_partition_sizes")
                        .striped(true)
                        .min_col_width(50.0)
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("#").strong());
                            ui.label(egui::RichText::new("Type").strong());
                            ui.label(egui::RichText::new("Size Mode").strong());
                            ui.label(egui::RichText::new("Size (MiB)").strong());
                            ui.end_row();
                            for cfg in &mut s.configs {
                                ui.label(format!("{}", cfg.index));
                                ui.label(&cfg.type_name);
                                let max_size_hover = cfg.max_size.map(|b| {
                                    let max_mib = (b / (1024 * 1024)).max(1) as u32;
                                    format!(
                                        "HFS volumes are capped at 65535 allocation blocks. \
                                         For this volume's block size, the maximum is {max_mib} MiB. \
                                         Growing further requires reformatting.",
                                    )
                                });
                                super::size_mode_row::size_mode_row(
                                    ui,
                                    &mut cfg.choice,
                                    &mut cfg.custom_size_mib,
                                    cfg.original_size,
                                    cfg.minimum_size,
                                    super::size_mode_row::SizeModeRowOptions {
                                        max_size: cfg.max_size,
                                        max_size_hover: max_size_hover.as_deref(),
                                        ..Default::default()
                                    },
                                );
                                ui.end_row();
                            }
                        });
                    ui.add_space(4.0);
                }

                if !notes.pfs3_shrink.is_empty() {
                    show_pfs3_banner(ui, &notes.pfs3_shrink);
                }
                ui.add_space(8.0);

                if let Some(workflow) = notes.chd_workflow {
                    ui.label(egui::RichText::new(workflow).small().italics());
                    if let Some((total, scratch, output)) = notes.chd_estimate {
                        ui.label(format!(
                            "Estimated peak new disk usage: {} MiB \
                             (scratch {} MiB + output {} MiB). \
                             Available space is checked when you pick a destination.",
                            total / (1024 * 1024),
                            scratch / (1024 * 1024),
                            output / (1024 * 1024),
                        ));
                    }
                    if notes.sector_by_sector_warning {
                        ui.add_space(4.0);
                        ui.label(
                            egui::RichText::new(
                                "Warning: the source is a sector-by-sector backup. \
                                 Re-exporting with new partition sizes drops the \
                                 byte-for-byte preservation of free space and \
                                 unrecognized filesystem regions - only smart-compact \
                                 areas of recognized partitions carry over.",
                            )
                            .color(super::theme::warning(ui.visuals())),
                        );
                    }
                    ui.add_space(8.0);
                }

                ui.horizontal(|ui| {
                    if ui.button("Export...").clicked() {
                        start = true;
                    }
                    let pde = ui.add_enabled(
                        can_physical,
                        egui::Button::new("Physical Disk Export..."),
                    );
                    if !can_physical {
                        pde.clone().on_hover_text(
                            "Physical Disk Export currently supports raw image file \
                             sources only. Backup folders, devices, and Clonezilla \
                             images aren't wired in yet.",
                        );
                    }
                    if pde.clicked() {
                        physical = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                });
            });

        if start {
            let opts = self.export_chd_hd_control.effective(ChdProfile::Hd);
            self.act(InspectAction::StartExport(opts));
        } else if physical {
            if let Some(src) = self.ctl.physical_source() {
                self.act(InspectAction::CloseExport);
                self.physical_disk_export.open_for(
                    PhysicalDiskExportSource {
                        path: src.path,
                        size_bytes: src.size_bytes,
                        fs_hint: src.fs_hint,
                        has_partition_table: src.has_partition_table,
                    },
                    self.ctl.table(),
                );
            }
        } else if cancel {
            self.act(InspectAction::CloseExport);
        }
    }

    fn show_results(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        let Some(table) = self.ctl.table_summary() else {
            return;
        };
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Partition Table:").strong());
            if table.superfloppy {
                ui.label("None (superfloppy)");
            } else {
                ui.label(&table.type_name);
                ui.label(format!("(disk signature: 0x{:08X})", table.disk_signature));
            }
        });

        // Only tables readable in either word order show this row (the SGI disk label).
        if let Some(order) = table.byte_order {
            let mut want_swab = false;
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Word order:").strong());
                ui.label(format!("{order} (16-bit words)"));
                if self.ctl.swabbing() {
                    ui.add(egui::Spinner::new());
                    ui.label("Swapping...");
                } else if ui
                    .button("Swap Word Order...")
                    .on_hover_text(
                        "Write a copy of this image with the two bytes of every 16-bit word \
                         exchanged, fixing a capture taken through a controller that reverses \
                         words. The operation is its own inverse.",
                    )
                    .clicked()
                {
                    want_swab = true;
                }
            });
            if want_swab {
                self.act(InspectAction::SwapWordOrder);
            }
        }

        if let Some(a) = self.ctl.alignment() {
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Alignment:").strong());
                ui.label(a.kind.as_str());
            });
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("First LBA:").strong());
                ui.label(format!("{}", a.first_lba));
                if a.heads > 0 {
                    ui.label(format!(
                        "  CHS Geometry: {} heads x {} sectors/track",
                        a.heads, a.sectors_per_track
                    ));
                }
            });
        }

        ui.add_space(8.0);
        if !self.ctl.partitions().is_empty() {
            let segments = self.ctl.layout();
            egui::CollapsingHeader::new("Disk layout")
                .default_open(true)
                .show(ui, |ui| {
                    super::partition_bar::PartitionBar::new(bar_segments(&segments)).show(ui);
                });
            ui.add_space(4.0);
        }
        self.show_partition_list(ui, ctx);
    }

    fn show_partition_list(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext) {
        if self.ctl.partitions().is_empty() {
            ui.label("No partitions found.");
            return;
        }
        let mut action: Option<InspectAction> = None;
        let mut resize_alto = false;
        let locked = self.ctl.rows_locked();
        let show_min_col = self.ctl.shows_min_columns();
        let has_slots = self.ctl.table_summary().is_some_and(|t| t.has_slots);
        let parts = self.ctl.partitions().to_vec();
        let muted = super::theme::muted(ui.visuals());

        egui::Grid::new("partition_table")
            .striped(true)
            .min_col_width(60.0)
            .show(ui, |ui| {
                ui.label(egui::RichText::new("#").strong())
                    .on_hover_text("Position in this list - the number `IMG@N` takes");
                if has_slots {
                    ui.label(egui::RichText::new("Slot").strong())
                        .on_hover_text(
                            "The partition table's own slot, as the platform names it \
                         (diskutil's disk4s6, IRIX slot 0) - the number `IMG@sN` takes",
                        );
                }
                ui.label(egui::RichText::new("Type").strong());
                ui.label(egui::RichText::new("Volume").strong());
                ui.label(egui::RichText::new("Start LBA").strong());
                ui.label(egui::RichText::new("Size").strong());
                if show_min_col {
                    ui.label(egui::RichText::new("Min Size").strong())
                        .on_hover_text("Smallest size achievable by trim alone (no data movement)");
                    ui.label(egui::RichText::new("Defrag Min").strong())
                        .on_hover_text(
                            "Smallest size achievable by cloning into a packed target. \
                             Used when restoring with \"Resize partitions to minimum size.\"",
                        );
                }
                ui.label(egui::RichText::new("Block Size").strong());
                ui.label(egui::RichText::new("Boot").strong());
                ui.label(egui::RichText::new("").strong());
                ui.end_row();

                for (pos, part) in parts.iter().enumerate() {
                    // The position is the number the user types back as `@N`; the slot has its column.
                    let index_label = if part.is_logical {
                        format!("  {} (logical)", pos + 1)
                    } else if part.is_extended_container {
                        format!("{} (container)", pos + 1)
                    } else {
                        format!("{}", pos + 1)
                    };
                    let slot = self.ctl.slot(part);
                    if part.is_extended_container {
                        ui.label(egui::RichText::new(index_label).color(muted));
                        if has_slots {
                            ui.label(egui::RichText::new(slot).color(muted));
                        }
                        ui.label(egui::RichText::new(&part.type_name).color(muted));
                        ui.label("");
                        ui.label(egui::RichText::new(format!("{}", part.start_lba)).color(muted));
                        ui.label(egui::RichText::new(format_size(part.size_bytes)).color(muted));
                        if show_min_col {
                            match self.ctl.min_cell(part) {
                                MinCell::Size(sz) => {
                                    ui.label(egui::RichText::new(format_size(sz)).color(muted));
                                }
                                _ => {
                                    ui.label("");
                                }
                            }
                            ui.label("");
                        }
                        ui.label("");
                        ui.label("");
                        ui.label("");
                        ui.end_row();
                        continue;
                    }
                    ui.label(index_label);
                    if has_slots {
                        ui.label(slot);
                    }
                    ui.label(&part.type_name);
                    // Probed live from the partition, never from backup metadata.
                    ui.label(self.ctl.volume_label(part.index).unwrap_or(""));
                    ui.label(format!("{}", part.start_lba));
                    ui.label(format_size(part.size_bytes));
                    if show_min_col {
                        match self.ctl.min_cell(part) {
                            MinCell::Size(sz) => {
                                ui.label(format_size(sz));
                            }
                            MinCell::Pending(phase) => {
                                ui.add(egui::Spinner::new()).on_hover_text(phase);
                            }
                            MinCell::CanCalc => {
                                if ui
                                    .small_button("Calc min")
                                    .on_hover_text(
                                        "Compute minimum size (requires a filesystem walk)",
                                    )
                                    .clicked()
                                {
                                    action = Some(InspectAction::CalcMin(part.index));
                                }
                            }
                            MinCell::Empty => {
                                ui.label("");
                            }
                        }
                        match self.ctl.defrag_min(part) {
                            Some(d) => ui.label(format_size(d)),
                            None => ui.label(""),
                        };
                    }
                    match part.hfs_block_size {
                        Some(bs) => ui.label(format_block_size(bs)),
                        None => ui.label(""),
                    };
                    ui.label(if part.bootable { "Yes" } else { "" });
                    let acts = self.ctl.row_actions(part);
                    ui.add_enabled_ui(!locked, |ui| {
                        if !acts.browse {
                            ui.label("");
                            return;
                        }
                        if ui.small_button("Browse").clicked() {
                            action = Some(InspectAction::Browse(part.index));
                        }
                        if acts.check && ui.small_button("Check").clicked() {
                            action = Some(InspectAction::Check(part.index));
                        }
                        if acts.expand_hfs
                            && ui
                                .small_button("Expand/Export...")
                                .on_hover_text(
                                    "Re-floor this classic-HFS volume to a new size / block \
                                     size (APM .hda) or export it as a flat BasiliskII HFV.",
                                )
                                .clicked()
                        {
                            action = Some(InspectAction::ExpandHfs(part.index));
                        }
                        if acts.defragment
                            && ui
                                .small_button("Defragment...")
                                .on_hover_text(
                                    "Repack this Human68k volume so its files are stored \
                                     contiguously, reclaiming holes left by deleted files. \
                                     Rewrites the partition in place - back up first.",
                                )
                                .clicked()
                        {
                            action = Some(InspectAction::Defragment(part.index));
                        }
                        if acts.resize_alto
                            && ui
                                .small_button("Resize...")
                                .on_hover_text(
                                    "Re-lay this Alto volume onto a different disk geometry \
                                     (Diablo 31 / 44, grow or shrink) and save it as a PDI.",
                                )
                                .clicked()
                        {
                            resize_alto = true;
                        }
                    });
                    ui.end_row();
                }
            });

        if let Some(action) = action {
            self.act(action);
        }
        if resize_alto {
            match self.ctl.image_path().map(|p| p.to_path_buf()) {
                Some(path) => match AltoResizeDialog::new(path) {
                    Ok(dlg) => self.alto_resize_dialog = Some(dlg),
                    Err(e) => ctx.log.error(format!("Cannot open resize dialog: {e}")),
                },
                None => ctx.log.error("Resize: no image file loaded"),
            }
        }

        self.show_fsck_popup(ui);
        self.show_chd_info_popup(ui);

        if let Some(dlg) = &mut self.expand_hfs_dialog {
            if !dlg.show(ui.ctx(), ctx.log) {
                self.expand_hfs_dialog = None;
            }
        }
        if let Some(dlg) = &mut self.alto_resize_dialog {
            if !dlg.show(ui.ctx(), ctx.log) {
                self.alto_resize_dialog = None;
            }
        }
        if let Some(dlg) = &mut self.floppy_convert_dialog {
            if !dlg.show(ui.ctx(), ctx.log) {
                self.floppy_convert_dialog = None;
            }
        }
    }

    fn show_chd_info_popup(&mut self, ui: &mut egui::Ui) {
        let Some(text) = self.ctl.chd_info_text() else {
            return;
        };
        let mut open = true;
        let mut buf = text.to_string();
        egui::Window::new("CHD Info")
            .open(&mut open)
            .resizable(true)
            .default_width(560.0)
            .default_height(420.0)
            .show(ui.ctx(), |ui| {
                egui::ScrollArea::both()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.add(
                            egui::TextEdit::multiline(&mut buf)
                                .font(egui::TextStyle::Monospace)
                                .desired_width(f32::INFINITY)
                                .desired_rows(20)
                                .interactive(true),
                        );
                    });
            });
        if !open {
            self.act(InspectAction::CloseChdInfo);
        }
    }

    fn show_fsck_popup(&mut self, ui: &mut egui::Ui) {
        let Some(view) = self.ctl.fsck_view() else {
            return;
        };
        let result = view.result;
        let mut show_debug = view.show_debug;
        let mut open = true;
        let mut do_repair = false;
        egui::Window::new("Filesystem Check Results")
            .open(&mut open)
            .resizable(true)
            .default_width(500.0)
            .show(ui.ctx(), |ui| {
                if result.is_clean() {
                    ui.colored_label(super::theme::success(ui.visuals()), "Filesystem is clean.");
                } else {
                    ui.colored_label(
                        super::theme::danger(ui.visuals()),
                        format!("{} error(s) found.", result.errors.len()),
                    );
                }
                ui.separator();
                let mut stats_line = format!(
                    "Files: {}  Directories: {}",
                    result.stats.files_checked, result.stats.directories_checked,
                );
                for (label, value) in &result.stats.extra {
                    stats_line.push_str(&format!("  {}: {}", label, value));
                }
                ui.label(stats_line);

                if !result.errors.is_empty() {
                    ui.separator();
                    ui.label(egui::RichText::new("Errors:").strong());
                    egui::ScrollArea::vertical()
                        .id_salt("fsck_errors_inspect")
                        .max_height(200.0)
                        .show(ui, |ui| {
                            for issue in &result.errors {
                                ui.colored_label(
                                    super::theme::danger(ui.visuals()),
                                    format!("[{}] {}", issue.code, issue.message),
                                );
                            }
                        });
                }

                let has_debug = result.warnings.iter().any(|w| w.debug);
                let visible: Vec<_> = result
                    .warnings
                    .iter()
                    .filter(|w| !w.debug || show_debug)
                    .collect();
                if !visible.is_empty() {
                    ui.separator();
                    ui.label(egui::RichText::new("Warnings:").strong());
                    egui::ScrollArea::vertical()
                        .id_salt("fsck_warnings_inspect")
                        .max_height(200.0)
                        .show(ui, |ui| {
                            for issue in &visible {
                                if issue.debug {
                                    ui.colored_label(
                                        super::theme::muted(ui.visuals()),
                                        format!("[DEBUG] {}", issue.message),
                                    );
                                } else {
                                    ui.colored_label(
                                        super::theme::warning(ui.visuals()),
                                        format!("[{}] {}", issue.code, issue.message),
                                    );
                                }
                            }
                        });
                }
                if has_debug {
                    ui.checkbox(&mut show_debug, "Show debug messages");
                }

                if result.repairable {
                    ui.separator();
                    if ui.button("Repair").clicked() {
                        do_repair = true;
                    }
                }

                if let Some(report) = view.repair {
                    ui.separator();
                    ui.label(egui::RichText::new("Repair Report:").strong());
                    for fix in &report.fixes_applied {
                        ui.colored_label(super::theme::success(ui.visuals()), format!("  {}", fix));
                    }
                    for fail in &report.fixes_failed {
                        ui.colored_label(super::theme::danger(ui.visuals()), format!("  {}", fail));
                    }
                }
            });
        let debug_changed = show_debug != view.show_debug;
        if debug_changed {
            self.act(InspectAction::ShowCheckDebug(show_debug));
        }
        if !open {
            self.act(InspectAction::CloseCheck);
        }
        if do_repair {
            self.act(InspectAction::Repair);
        }
    }
}

/// Why per-partition export is off for a whole-disk-only format, for the radio's hover.
fn per_partition_refusal(format: ExportFormat) -> Option<&'static str> {
    Some(match format {
        ExportFormat::Chd => {
            "Per-partition export is not supported for CHD - each partition would be a \
             headless slice with no MBR/GPT, which no emulator can consume. Use whole-disk \
             export instead."
        }
        ExportFormat::VhdDynamic => {
            "Per-partition export is not supported for dynamic VHD - the sparse layout wraps \
             a whole disk and buys nothing on a single mostly-used partition. Use fixed VHD \
             for per-partition export."
        }
        ExportFormat::Qcow2 => {
            "Per-partition export is not supported for QCOW2 - the sparse layout wraps a \
             whole-disk geometry that QEMU/UTM expect to find an MBR/GPT/APM at sector 0."
        }
        ExportFormat::VmdkFlat => {
            "Per-partition export is not supported for VMDK flat - the descriptor wraps a \
             whole-disk geometry that VMware/qemu-img/VirtualBox expect to find a partition \
             table at sector 0 of."
        }
        ExportFormat::VmdkSparse => {
            "Per-partition export is not supported for VMDK sparse - the grain directory \
             wraps a whole-disk geometry that VMware/qemu-img/VirtualBox expect to find a \
             partition table at sector 0 of."
        }
        _ => return None,
    })
}

fn format_radios(ui: &mut egui::Ui, format: &mut ExportFormat, chd_allowed: bool) {
    ui.radio_value(format, ExportFormat::Vhd, "VHD");
    ui.radio_value(format, ExportFormat::VhdDynamic, "VHD (Dynamic)")
        .on_hover_text(
            "Sparse VHD - all-zero blocks are omitted. Same .vhd extension; \
             readable by Hyper-V, qemu-img, Disk Management.",
        );
    ui.radio_value(format, ExportFormat::Qcow2, "QCOW2")
        .on_hover_text(
            "QCOW2 v3 - sparse, uncompressed. The container UTM uses \
         for classic-Mac PPC guests; opens in QEMU, virt-manager.",
        );
    ui.radio_value(format, ExportFormat::VmdkFlat, "VMDK (Flat)")
        .on_hover_text(
            "monolithicFlat VMDK - emits <name>.vmdk descriptor + \
             <name>-flat.vmdk raw extent. Opens in VMware Workstation/\
             Fusion, VirtualBox, qemu-img.",
        );
    ui.radio_value(format, ExportFormat::VmdkSparse, "VMDK (Sparse)")
        .on_hover_text(
            "monolithicSparse VMDK - single self-contained .vmdk; \
             zero grains omitted. Opens in VMware, VirtualBox, qemu-img.",
        );
    ui.radio_value(format, ExportFormat::Raw, "Raw (.img)");
    ui.radio_value(format, ExportFormat::TwoMg, "2MG (.2mg)");
    ui.radio_value(format, ExportFormat::Woz, "WOZ (.woz)")
        .on_hover_text("Apple II WOZ 2.0 (floppy only: 140K / 400K / 800K sources)");
    ui.radio_value(format, ExportFormat::Moof, "MOOF (.moof)")
        .on_hover_text(
            "Applesauce Macintosh 3.5\" GCR bitstream (floppy only: 400K / 800K). \
             Loadable by Snow / MAME / Applesauce.",
        );
    ui.radio_value(format, ExportFormat::Dc42, "DiskCopy 4.2 (.dsk)")
        .on_hover_text("Mac / Apple IIgs DiskCopy 4.2 (floppy only: 400K / 720K / 800K / 1440K)");
    ui.radio_value(format, ExportFormat::Hfv, "HFV (BasiliskII)")
        .on_hover_text(
            "Flat classic-HFS volume for BasiliskII / SheepShaver (no partition \
             table, classic-HFS-only, max 2047 MB). Exported per-partition: one \
             .hfv per HFS partition, cloned and re-floored to the chosen size.",
        );
    let hd = ui.add_enabled(
        chd_allowed,
        egui::RadioButton::new(*format == ExportFormat::Chd, "CHD (Hard Disk)"),
    );
    if hd.clicked() {
        *format = ExportFormat::Chd;
    }
    if chd_allowed {
        hd.on_hover_text(
            "MAME hard-disk CHD (512-byte unit). DVD / CD CHD and BIN/CUE \
             extraction live on the Optical tab.",
        );
    } else {
        hd.on_hover_text(
            "CHD export needs a raw image or device source - backup \
             folders and Clonezilla images aren't supported.",
        );
    }
}

/// PFS3 export rebuilds a shrunk volume; say what that costs before the export.
fn show_pfs3_banner(ui: &mut egui::Ui, names: &[String]) {
    egui::Frame::group(ui.style())
        .fill(ui.visuals().widgets.noninteractive.bg_fill)
        .show(ui, |ui| {
            ui.label(egui::RichText::new("PFS3 shrink: volume will be rebuilt").strong());
            ui.label(format!("Affected partitions: {}", names.join(", ")));
            ui.label(
                "PFS3's allocator scatters blocks throughout the volume, so \
                 in-place trim can't free up space. The export pipeline will \
                 instead walk the source and rebuild a packed copy at the new \
                 size - limitations to be aware of:",
            );
            ui.label("  - PFS3 trashcan (deldir) contents are NOT preserved.");
            ui.label(
                "  - File data, directory tree, protection bits, comments, \
                 dates, softlinks, and hardlinks (both directions) ARE preserved.",
            );
            ui.label(
                "  - Operation is slower than a verbatim copy: every file is \
                 read from the source and re-written to the destination.",
            );
            ui.label(
                "  - Uses a tempfile for the rebuilt image; ensure your \
                 temp directory has enough free space for the target partition size.",
            );
        });
    ui.add_space(4.0);
}

fn show_clonezilla_metadata(ui: &mut egui::Ui, cz: &ClonezillaSummary) {
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new("Clonezilla Image:").strong());
        ui.label(format!("Disk: {}", cz.disk_name));
    });
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new("Source Size:").strong());
        ui.label(format!(
            "{} ({} bytes)",
            format_size_decimal(cz.source_size_bytes),
            format_bytes_grouped(cz.source_size_bytes),
        ));
        if let Some((c, h, s)) = cz.chs {
            ui.label(format!("  CHS: {c} / {h} / {s}"));
        }
    });
    if let Some(created) = &cz.created {
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Created:").strong());
            ui.label(created);
        });
    }
    ui.add_space(8.0);
}

fn show_backup_metadata(ui: &mut egui::Ui, meta: &BackupSummary) {
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new("Backup Info:").strong());
        ui.label(format!("Source: {}", meta.source));
    });
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new("Created:").strong());
        ui.label(&meta.created);
        ui.label(egui::RichText::new("Table:").strong());
        ui.label(&meta.table);
    });
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new("Compression:").strong());
        ui.label(&meta.compression);
        ui.label(egui::RichText::new("Checksum:").strong());
        ui.label(&meta.checksum);
        if meta.sector_by_sector {
            ui.label("(sector-by-sector)");
        }
    });
    ui.horizontal(|ui| {
        if meta.min_restore_bytes > 0 {
            ui.label(egui::RichText::new("Minimum Size:").strong());
            ui.label(format!(
                "{} ({} bytes)",
                format_size_decimal(meta.min_restore_bytes),
                format_bytes_grouped(meta.min_restore_bytes),
            ));
        } else if meta.source_size_bytes > 0 {
            ui.label(egui::RichText::new("Source Size:").strong());
            ui.label(format_size(meta.source_size_bytes));
        }
        ui.label(egui::RichText::new("Alignment:").strong());
        ui.label(&meta.alignment);
    });
    ui.add_space(8.0);
}

/// Base-1000 units, the way storage media is marketed ("8 GB" = 8,000,000,000 bytes).
fn format_size_decimal(bytes: u64) -> String {
    const GB: f64 = 1_000_000_000.0;
    const MB: f64 = 1_000_000.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else {
        format!("{:.1} MB", b / MB)
    }
}

/// An HFS allocation block size: "N KiB" when a whole number of KiB, else "N B".
fn format_block_size(bytes: u32) -> String {
    if bytes >= 1024 && bytes.is_multiple_of(1024) {
        format!("{} KiB", bytes / 1024)
    } else {
        format!("{} B", bytes)
    }
}

/// A byte count with digit grouping (1,048,576).
fn format_bytes_grouped(bytes: u64) -> String {
    let s = bytes.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

/// The controller's layout as bar segments, the palette cycling over real volumes.
fn bar_segments(layout: &[LayoutSegment]) -> Vec<super::partition_bar::Segment> {
    use super::partition_bar::{Segment, SegmentKind as Kind};
    let mut color_index = 0usize;
    layout
        .iter()
        .map(|s| Segment {
            label: s.label.clone(),
            fs: s.fs.clone(),
            size_bytes: s.size_bytes,
            kind: match s.kind {
                SegmentKind::Partition => {
                    color_index += 1;
                    Kind::Partition {
                        color_index: color_index - 1,
                    }
                }
                SegmentKind::Dimmed => Kind::Dimmed,
                SegmentKind::Free => Kind::Free,
            },
        })
        .collect()
}

/// The Export window's Current and After bars, at one shared byte-per-pixel scale.
fn show_export_disk_layout_bars(ui: &mut egui::Ui, layout: &LayoutBar) {
    use super::partition_bar::PartitionBar;
    let current = bar_segments(&layout.current);
    let after = bar_segments(&layout.after);
    let current_total: u64 = current.iter().map(|s| s.size_bytes).sum();
    let after_total: u64 = after.iter().map(|s| s.size_bytes).sum();
    let max_total = current_total.max(after_total).max(1);
    let available_width = ui.available_width().max(120.0);

    ui.label("Current:");
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
        format_size(current_total),
        format_size(after_total),
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
