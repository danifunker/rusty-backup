//! The Build Disk tab: the egui shell of `controller::build_disk::BuildDiskController`.
//!
//! A wizard: a step strip, one panel per step drawn from the controller's accessors, and Back /
//! Next under it. Every click is a `BuildDiskAction`; pickers come back through an
//! `EffectHost`. The step order, what blocks Next and the build itself are the controller's.

use std::path::PathBuf;
use std::sync::Arc;

use rusty_backup::controller::build_disk::{
    containers, cpm_presets, format_size, geometries, hints, ids, parse_size_field,
    BuildDiskAction, BuildDiskController, CdLayout, CdSidecars, DiskKind, FormatOptions,
    FormatTarget, FsChoice, FsOption, MacDriver, OptionEdit, PresetKind, RowContent, Step,
    AFFS_VARIANTS, CD_AUTO_SIZE, NTFS_VERSIONS, SECTOR_SIZES,
};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::partition_edit::BuildAction;
use rusty_backup::controller::{Controller as _, Effect};

use super::context::TabContext;
use super::effects::{EffectHost, Reply};
use super::progress::ProgressState;

pub struct BuildDiskTab {
    ctl: BuildDiskController,
    effects: EffectHost,
    pending_fx: Vec<Effect>,
    /// "Open in Inspect" was clicked: the app hands this image to the Inspect tab.
    open_inspect: Option<PathBuf>,
}

impl Default for BuildDiskTab {
    fn default() -> Self {
        Self {
            ctl: BuildDiskController::new(Arc::new(super::ui_logger::EguiWake)),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
            open_inspect: None,
        }
    }
}

impl BuildDiskTab {
    pub fn is_running(&self) -> bool {
        self.ctl.is_running()
    }

    /// The image to open in Inspect, once.
    pub fn take_open_inspect(&mut self) -> Option<PathBuf> {
        self.open_inspect.take()
    }

    pub fn poll_background(&mut self, ctx: &mut TabContext, progress: &mut ProgressState) {
        self.poll(ctx, progress);
    }

    fn act(&mut self, action: BuildDiskAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
    }

    fn poll(&mut self, ctx: &mut TabContext, _progress: &mut ProgressState) {
        self.ctl.reconcile_devices(ctx.devices);
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        for (level, msg) in self.ctl.take_log() {
            match level {
                LogLevel::Info => ctx.log.info(msg),
                LogLevel::Warning => ctx.log.warn(msg),
                LogLevel::Error => ctx.log.error(msg),
            }
        }
    }

    fn run_effects(&mut self, ui: &egui::Ui, ctx: &mut TabContext) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(ui.ctx(), fx, &mut messages);
        replies.extend(self.effects.show(ui.ctx(), "build_disk"));
        for msg in messages {
            ctx.log.info(msg);
        }
        for reply in replies {
            match reply {
                Reply::Picked { purpose, path } => {
                    self.act(BuildDiskAction::Picked { purpose, path })
                }
                Reply::Chose { purpose, choice } => {
                    self.act(BuildDiskAction::Chose { purpose, choice })
                }
                Reply::Confirmed { .. } | Reply::Answered { .. } | Reply::CloseScreen => {}
            }
        }
        if !self.pending_fx.is_empty() {
            ui.ctx().request_repaint();
        }
    }

    pub fn show(&mut self, ui: &mut egui::Ui, ctx: &mut TabContext, progress: &mut ProgressState) {
        self.poll(ctx, progress);
        ui.heading("Build Disk");
        ui.add_space(4.0);
        if let Some(tab) = ctx.busy_elsewhere {
            ui.colored_label(
                super::theme::warning(ui.visuals()),
                super::context::busy_elsewhere_notice(tab),
            );
        }
        self.step_strip(ui);
        ui.separator();
        let enabled = ctx.busy_elsewhere.is_none() || self.ctl.step() == Step::Build;
        egui::ScrollArea::vertical()
            .id_salt("build_disk_scroll")
            .max_height((ui.available_height() - 48.0).max(120.0))
            .show(ui, |ui| {
                ui.add_enabled_ui(enabled, |ui| match self.ctl.step() {
                    Step::Kind => self.kind_step(ui),
                    Step::Output => self.output_step(ui, ctx),
                    Step::Disk => self.disk_step(ui),
                    Step::Partitions => self.partitions_step(ui),
                    Step::Options => self.options_step(ui),
                    Step::Review => self.review_step(ui),
                    Step::Build => self.build_step(ui),
                });
            });
        ui.separator();
        self.nav(ui, enabled);
        self.run_effects(ui, ctx);
    }

    fn step_strip(&mut self, ui: &mut egui::Ui) {
        let steps = self.ctl.steps();
        let here = steps
            .iter()
            .position(|s| *s == self.ctl.step())
            .unwrap_or(0);
        ui.horizontal(|ui| {
            for (i, step) in steps.iter().enumerate() {
                if i > 0 {
                    ui.label(egui::RichText::new("->").weak());
                }
                let text = format!("{}. {}", i + 1, step.label());
                if i == here {
                    ui.label(egui::RichText::new(text).strong());
                } else if i < here && !self.ctl.is_running() && *step != Step::Build {
                    if ui
                        .link(text)
                        .on_hover_text("Go back to this step")
                        .clicked()
                    {
                        self.act(BuildDiskAction::GoTo(*step));
                    }
                } else {
                    ui.label(egui::RichText::new(text).weak());
                }
            }
        });
    }

    fn nav(&mut self, ui: &mut egui::Ui, enabled: bool) {
        let step = self.ctl.step();
        if step == Step::Build {
            return;
        }
        let blocked = self.ctl.next_blocked();
        ui.horizontal(|ui| {
            if ui
                .add_enabled(enabled && step != Step::Kind, egui::Button::new("Back"))
                .clicked()
            {
                self.act(BuildDiskAction::Back);
            }
            let label = if step == Step::Review {
                "Create Disk"
            } else {
                "Next"
            };
            let resp = ui.add_enabled(enabled && blocked.is_none(), egui::Button::new(label));
            if resp.clicked() {
                self.act(BuildDiskAction::Next);
            }
            if let Some(why) = blocked {
                ui.colored_label(super::theme::warning(ui.visuals()), why);
            }
        });
    }

    // --- steps ---------------------------------------------------------------------------

    fn kind_step(&mut self, ui: &mut egui::Ui) {
        ui.label("What are you building?");
        ui.add_space(4.0);
        for kind in DiskKind::ALL {
            let hint = kind.hint();
            if ui
                .radio(self.ctl.kind() == kind, kind.label())
                .on_hover_text(hint)
                .clicked()
            {
                self.act(BuildDiskAction::SetKind(kind));
            }
            ui.label(egui::RichText::new(hint).weak());
            ui.add_space(4.0);
        }
    }

    fn output_step(&mut self, ui: &mut egui::Ui, ctx: &TabContext) {
        let is_device = self.ctl.output_is_device();
        let cd = self.ctl.kind() == DiskKind::Cd;
        ui.horizontal(|ui| {
            if ui.radio(!is_device, "Image file").clicked() {
                self.act(BuildDiskAction::SetOutputIsDevice(false));
            }
            // A CD-ROM image is burned from the file, never written to a disk.
            if !cd
                && ui
                    .radio(is_device, "Physical disk")
                    .on_hover_text(hints::DEVICE)
                    .clicked()
            {
                self.act(BuildDiskAction::SetOutputIsDevice(true));
            }
        });
        ui.add_space(6.0);
        if !is_device {
            ui.horizontal(|ui| {
                ui.label("Save as:");
                match self.ctl.image() {
                    Some(p) => ui.label(p.display().to_string()),
                    None => ui.label(egui::RichText::new("(not chosen)").weak()),
                };
                if ui.button("Save As...").clicked() {
                    self.act(BuildDiskAction::PickOutputImage);
                }
            });
            if cd {
                return;
            }
            let cur = self.ctl.container();
            let mut picked = None;
            ui.horizontal(|ui| {
                ui.label("Format:");
                egui::ComboBox::from_id_salt("build_disk_container")
                    .selected_text(cur.description())
                    .show_ui(ui, |ui| {
                        for c in containers() {
                            if ui.selectable_label(c == cur, c.description()).clicked() {
                                picked = Some(c);
                            }
                        }
                    })
                    .response
                    .on_hover_text(hints::CONTAINER);
            });
            if let Some(c) = picked {
                self.act(BuildDiskAction::SetContainer(c));
            }
            return;
        }
        let current = self
            .ctl
            .device()
            .map(|d| d.display_name())
            .unwrap_or_else(|| "Select a device...".into());
        let row = self.ctl.device_row();
        let mut picked = None;
        ui.horizontal(|ui| {
            ui.label("Device:");
            egui::ComboBox::from_id_salt(ids::OUTPUT_DEVICE)
                .selected_text(current)
                .width(420.0)
                .show_ui(ui, |ui| {
                    for (i, d) in ctx.devices.iter().enumerate() {
                        let mut label = d.display_name();
                        if d.is_system {
                            label.push_str(" [SYSTEM]");
                        }
                        if ui.selectable_label(row == Some(i), label).clicked() {
                            picked = Some((i, d.clone()));
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
        });
        if let Some((row, device)) = picked {
            self.act(BuildDiskAction::SelectDevice { row, device });
        }
        ui.label(
            egui::RichText::new(hints::DEVICE_WARNING).color(super::theme::warning(ui.visuals())),
        );
    }

    /// The CD-ROM's layout, name, size and the folder it is filled from.
    fn cd_step(&mut self, ui: &mut egui::Ui) {
        let mut cd = self.ctl.cd().clone();
        let before = cd.clone();
        for l in CdLayout::ALL {
            ui.radio_value(&mut cd.layout, l, l.label())
                .on_hover_text(l.hint());
        }
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            ui.label("Volume name:");
            ui.add(egui::TextEdit::singleline(&mut cd.name).desired_width(200.0));
        });
        let mut auto = self.ctl.cd_auto_size();
        if ui
            .checkbox(&mut auto, "Size to the folder")
            .on_hover_text(hints::CD_AUTO_SIZE)
            .changed()
        {
            let text = if auto { CD_AUTO_SIZE } else { "600M" };
            self.act(BuildDiskAction::SetSize(text.to_string()));
        }
        if !auto {
            self.size_row(ui, Some(hints::CD_SIZE.to_string()));
        }
        ui.horizontal(|ui| {
            ui.label("Fill from:");
            match &cd.from_dir {
                Some(d) => ui.label(d.display().to_string()),
                None => ui.label(egui::RichText::new("(a blank disc)").weak()),
            };
            if ui
                .button("Choose Folder...")
                .on_hover_text(hints::CD_FOLDER)
                .clicked()
            {
                self.act(BuildDiskAction::PickCdFolder);
            }
            if cd.from_dir.is_some() && ui.small_button("Clear").clicked() {
                cd.from_dir = None;
            }
        });
        let mut toggled = Vec::new();
        for row in self.ctl.cd_options() {
            let mut on = row.on;
            let enabled = row.unavailable.is_none();
            let resp = ui.add_enabled(enabled, egui::Checkbox::new(&mut on, row.option.label()));
            if let Some(why) = row.unavailable {
                resp.on_disabled_hover_text(why);
            } else if on != row.on {
                toggled.push((row.option, on));
            }
            help(ui, row.option.hint());
        }
        let sidecars = self.ctl.cd_sidecars();
        let mut picked = sidecars;
        ui.horizontal(|ui| {
            ui.label("macOS ._ files:");
            ui.add_enabled_ui(self.ctl.cd_sidecars_unavailable().is_none(), |ui| {
                egui::ComboBox::from_id_salt(ids::CD_SIDECARS)
                    .selected_text(sidecars.label())
                    .show_ui(ui, |ui| {
                        for &c in CdSidecars::choices(cd.layout) {
                            ui.selectable_value(&mut picked, c, c.label())
                                .on_hover_text(c.hint());
                        }
                    })
            })
            .response
            .on_disabled_hover_text(self.ctl.cd_sidecars_unavailable().unwrap_or_default());
        });
        help(ui, &format!("{} {}", hints::CD_SIDECARS, sidecars.hint()));
        if cd != before {
            self.act(BuildDiskAction::SetCd(cd));
        }
        for (option, on) in toggled {
            self.act(BuildDiskAction::SetCdOption(option, on));
        }
        if picked != sidecars {
            self.act(BuildDiskAction::SetCdSidecars(picked));
        }
    }

    fn size_row(&mut self, ui: &mut egui::Ui, hint: Option<String>) {
        if self.ctl.output_is_device() {
            ui.horizontal(|ui| {
                ui.label("Size:");
                match self.ctl.disk_size() {
                    Ok(n) => ui.label(format!("{} (the device's size)", format_size(n))),
                    Err(e) => ui.label(e),
                };
            });
            return;
        }
        ui.horizontal(|ui| {
            ui.label("Size:");
            let mut text = self.ctl.size_text().to_string();
            if ui
                .add(
                    egui::TextEdit::singleline(&mut text)
                        .desired_width(90.0)
                        .id(egui::Id::unique(ids::SIZE)),
                )
                .on_hover_text(hints::SIZE)
                .changed()
            {
                self.act(BuildDiskAction::SetSize(text));
            }
            match self.ctl.disk_size() {
                Ok(n) => {
                    ui.label(egui::RichText::new(format_size(n)).weak());
                }
                Err(e) => {
                    ui.colored_label(super::theme::danger(ui.visuals()), e);
                }
            }
            if let Some(h) = hint {
                ui.label(egui::RichText::new(h).weak());
            }
        });
    }

    fn disk_step(&mut self, ui: &mut egui::Ui) {
        match self.ctl.kind() {
            DiskKind::Cd => self.cd_step(ui),
            DiskKind::Preset => {
                let mut p = self.ctl.preset().clone();
                let before = p.clone();
                for k in PresetKind::ALL {
                    ui.radio_value(&mut p.kind, k, k.label());
                }
                ui.add_space(4.0);
                match p.kind {
                    PresetKind::X68k => {
                        ui.horizontal(|ui| {
                            ui.label("Controller:");
                            ui.radio_value(&mut p.x68k_scsi, false, "SASI (256-byte sectors)");
                            ui.radio_value(&mut p.x68k_scsi, true, "SCSI (1024-byte sectors)");
                        });
                        ui.horizontal(|ui| {
                            ui.label("Human68k partitions:");
                            ui.add(egui::DragValue::new(&mut p.x68k_partitions).range(1..=8));
                        });
                        ui.horizontal(|ui| {
                            ui.label("System disk:");
                            match &p.x68k_system_disk {
                                Some(d) => ui.label(d.display().to_string()),
                                None => ui.label(egui::RichText::new(hints::NO_SYSTEM_DISK).weak()),
                            };
                            if ui
                                .button("Choose...")
                                .on_hover_text(hints::SYSTEM_DISK)
                                .clicked()
                            {
                                self.act(BuildDiskAction::PickSystemDisk);
                            }
                            if p.x68k_system_disk.is_some() && ui.small_button("Clear").clicked() {
                                p.x68k_system_disk = None;
                            }
                        });
                    }
                    PresetKind::SgiEfs => {
                        ui.horizontal(|ui| {
                            ui.label("EFS label:");
                            ui.add(
                                egui::TextEdit::singleline(&mut p.sgi_label).desired_width(80.0),
                            );
                        });
                    }
                }
                if p != before {
                    self.act(BuildDiskAction::SetPreset(p));
                }
                self.size_row(ui, None);
            }
            DiskKind::Volume => {
                let fs = self.ctl.volume_fs_choice();
                ui.horizontal(|ui| {
                    ui.label("Filesystem:");
                    if let Some(k) = fs_combo(ui, ids::VOLUME_FS, Some(fs), false) {
                        self.act(BuildDiskAction::SetVolumeFs(k.unwrap_or(fs)));
                    }
                });
                match self.ctl.volume_size_range() {
                    Some(hint) => self.size_row(ui, Some(hint)),
                    None => {
                        ui.label(egui::RichText::new(hints::FIXED_SIZE).weak());
                        if self.ctl.output_is_device() {
                            self.size_row(ui, None);
                        }
                    }
                }
            }
            DiskKind::Partitioned => {
                let view = self.ctl.table_view();
                ui.horizontal(|ui| {
                    ui.label("Start from:");
                    let donor = self
                        .ctl
                        .donor()
                        .map(|(p, t)| (p.to_path_buf(), t.to_string()));
                    if ui.radio(donor.is_none(), "A blank table").clicked() && donor.is_some() {
                        self.act(BuildDiskAction::ClearDonor);
                    }
                    if ui
                        .radio(donor.is_some(), "An existing disk's layout...")
                        .on_hover_text(hints::DONOR)
                        .clicked()
                    {
                        self.act(BuildDiskAction::PickDonor);
                    }
                    if let Some((p, t)) = donor {
                        ui.label(egui::RichText::new(format!("{t}: {}", p.display())).weak());
                    }
                    if self.ctl.donor_reading() {
                        ui.spinner();
                    }
                });
                if let Some(e) = self.ctl.donor_error() {
                    ui.colored_label(super::theme::danger(ui.visuals()), e.to_string());
                }
                ui.add_enabled_ui(!view.fixed, |ui| {
                    ui.horizontal_wrapped(|ui| {
                        ui.label("Table type:");
                        for &k in view.kinds {
                            if ui.radio(view.kind == k, k.label()).clicked() && k != view.kind {
                                self.act(BuildDiskAction::Table(BuildAction::SetKind(k)));
                            }
                        }
                    });
                });
                self.size_row(ui, None);
                ui.horizontal(|ui| {
                    ui.label("Alignment:");
                    let mut align = view.align_text.clone();
                    ui.add(
                        egui::TextEdit::singleline(&mut align)
                            .desired_width(80.0)
                            .hint_text(view.align_hint.as_str()),
                    )
                    .on_hover_text(hints::ALIGN);
                    if align != view.align_text {
                        self.act(BuildDiskAction::Table(BuildAction::SetAlign(align)));
                    }
                    if let Some((heads, sectors)) = view.geometry {
                        ui.label("Heads:");
                        let mut h = heads;
                        if ui
                            .add(egui::DragValue::new(&mut h).range(1..=255))
                            .changed()
                        {
                            self.act(BuildDiskAction::Table(BuildAction::SetHeads(h)));
                        }
                        ui.label("Sectors/track:");
                        let mut s = sectors;
                        if ui
                            .add(egui::DragValue::new(&mut s).range(1..=1024))
                            .changed()
                        {
                            self.act(BuildDiskAction::Table(BuildAction::SetSectorsPerTrack(s)));
                        }
                    }
                });
            }
        }
    }

    fn partitions_step(&mut self, ui: &mut egui::Ui) {
        let view = self.ctl.table_view();
        let probing = self.ctl.probing();
        match &view.layout {
            Ok(bar) => super::partition_editor_modal::show_bar(ui, bar),
            Err(e) => {
                ui.colored_label(super::theme::danger(ui.visuals()), e);
            }
        }
        ui.add_space(6.0);
        let mut acts = Vec::new();
        let n = view.rows.len();
        egui::Grid::new("build_disk_rows")
            .striped(true)
            .min_col_width(40.0)
            .show(ui, |ui| {
                for h in ["#", "Size", "Type"] {
                    ui.label(egui::RichText::new(h).strong());
                }
                if view.names {
                    ui.label(egui::RichText::new("Name").strong());
                }
                if view.boot.is_some() {
                    ui.label(egui::RichText::new("Boot").strong());
                }
                for h in ["Start LBA", "Content", ""] {
                    ui.label(egui::RichText::new(h).strong());
                }
                ui.end_row();
                for (i, row) in view.rows.iter().enumerate() {
                    ui.label(format!("{}", i + 1));
                    let mut size = row.size_text.clone();
                    ui.add(
                        egui::TextEdit::singleline(&mut size)
                            .desired_width(70.0)
                            .id(egui::Id::unique(("bd_size", i))),
                    )
                    .on_hover_text(hints::ROW_SIZE);
                    if size != row.size_text {
                        acts.push(BuildDiskAction::Table(BuildAction::SetRowSize {
                            row: i,
                            text: size,
                        }));
                    }
                    let mut ty = row.type_text.clone();
                    if view.fixed {
                        ui.label(ty.as_str()).on_hover_text(hints::ROW_FIXED_TYPE);
                    } else {
                        super::partition_editor_modal::type_field(
                            ui,
                            view.kind,
                            &format!("bd_type_{i}"),
                            &mut ty,
                        );
                    }
                    if ty != row.type_text {
                        acts.push(BuildDiskAction::Table(BuildAction::SetRowType {
                            row: i,
                            text: ty,
                        }));
                    }
                    if view.names {
                        let mut name = row.name.clone();
                        ui.add(
                            egui::TextEdit::singleline(&mut name)
                                .desired_width(90.0)
                                .id(egui::Id::unique(("bd_name", i))),
                        );
                        if name != row.name {
                            acts.push(BuildDiskAction::Table(BuildAction::SetRowName {
                                row: i,
                                text: name,
                            }));
                        }
                    }
                    if let Some(only_one) = view.boot {
                        let mut on = row.bootable;
                        let hint = if only_one {
                            hints::BOOT_MBR
                        } else {
                            hints::BOOT_RDB
                        };
                        if ui.checkbox(&mut on, "").on_hover_text(hint).changed() {
                            acts.push(BuildDiskAction::Table(BuildAction::SetRowBootable {
                                row: i,
                                on,
                            }));
                        }
                    }
                    match (row.start_lba, row.planned_size) {
                        (Some(lba), Some(size)) => {
                            ui.label(lba.to_string()).on_hover_text(format_size(size));
                        }
                        _ => {
                            ui.label(egui::RichText::new("-").weak());
                        }
                    }
                    ui.horizontal(|ui| {
                        let content = self.ctl.row_content(i);
                        let fs = self.ctl.row_fs_choice(i);
                        let id = format!("{}_{i}", ids::ROW_CONTENT);
                        if self.ctl.row_can_copy(i) {
                            let copying = content == RowContent::Copy;
                            if ui
                                .selectable_label(copying, "Copy")
                                .on_hover_text(hints::ROW_COPY)
                                .clicked()
                                && !copying
                            {
                                acts.push(BuildDiskAction::SetRowCopy(i));
                            }
                        }
                        let shown = if content == RowContent::Copy {
                            None
                        } else {
                            fs
                        };
                        if let Some(choice) = fs_combo(ui, &id, shown, true) {
                            acts.push(BuildDiskAction::SetRowFs { row: i, fs: choice });
                        }
                        if content == RowContent::Image {
                            if let Some(path) = &row.source {
                                let name = path
                                    .file_name()
                                    .map(|n| n.to_string_lossy().to_string())
                                    .unwrap_or_default();
                                ui.label(format!("{name} ({})", format_size(row.source_size)))
                                    .on_hover_text(path.display().to_string());
                                if ui.small_button("Clear").clicked() {
                                    acts.push(BuildDiskAction::Table(BuildAction::ClearRowSource(
                                        i,
                                    )));
                                }
                            }
                        }
                        if ui
                            .add_enabled(!probing, egui::Button::new("Image...").small())
                            .on_hover_text(hints::ROW_IMAGE)
                            .clicked()
                        {
                            acts.push(BuildDiskAction::Table(BuildAction::PickRowSource(i)));
                        }
                    });
                    ui.add_enabled_ui(!view.fixed, |ui| {
                        ui.horizontal(|ui| {
                            if ui
                                .add_enabled(i > 0, egui::Button::new("Up").small())
                                .clicked()
                            {
                                acts.push(BuildDiskAction::Table(BuildAction::MoveRow {
                                    row: i,
                                    delta: -1,
                                }));
                            }
                            if ui
                                .add_enabled(i + 1 < n, egui::Button::new("Down").small())
                                .clicked()
                            {
                                acts.push(BuildDiskAction::Table(BuildAction::MoveRow {
                                    row: i,
                                    delta: 1,
                                }));
                            }
                            if ui
                                .add_enabled(n > 1, egui::Button::new("Remove").small())
                                .clicked()
                            {
                                acts.push(BuildDiskAction::Table(BuildAction::RemoveRow(i)));
                            }
                        })
                    });
                    ui.end_row();
                }
            });
        ui.horizontal(|ui| {
            if ui
                .add_enabled(view.can_add_row, egui::Button::new("Add Partition"))
                .clicked()
            {
                acts.push(BuildDiskAction::Table(BuildAction::AddRow));
            }
            if let Some((used, total)) = view.slots {
                ui.label(egui::RichText::new(format!("{used} of {total} slots used")).weak());
            }
            if probing {
                ui.label(egui::RichText::new("Measuring the source image...").weak());
            }
        });
        if self.ctl.takes_handlers() {
            ui.add_space(8.0);
            ui.label(egui::RichText::new("Filesystem handlers in the RDB").strong());
            for (i, (t, p)) in self.ctl.handlers().iter().enumerate() {
                ui.horizontal(|ui| {
                    ui.label(format!("{t}: {}", p.display()));
                    if ui.small_button("Remove").clicked() {
                        acts.push(BuildDiskAction::RemoveHandler(i));
                    }
                });
            }
            ui.horizontal(|ui| {
                for t in self.ctl.handlers_missing() {
                    if ui
                        .button(format!("Add {t} handler..."))
                        .on_hover_text(hints::HANDLER)
                        .clicked()
                    {
                        acts.push(BuildDiskAction::AddHandler(t));
                    }
                }
            });
        }
        if self.ctl.takes_mac_driver() {
            ui.add_space(8.0);
            ui.label(egui::RichText::new("Mac driver").strong());
            let cur = self.ctl.mac_driver().clone();
            ui.horizontal(|ui| {
                for d in [MacDriver::Bundled, MacDriver::Off] {
                    let label = d.label();
                    if ui
                        .radio(cur == d, label)
                        .on_hover_text(hints::MAC_DRIVER)
                        .clicked()
                    {
                        acts.push(BuildDiskAction::SetMacDriver(d));
                    }
                }
                if ui.button("From a disk...").clicked() {
                    acts.push(BuildDiskAction::PickMacDriver);
                }
            });
            if matches!(cur, MacDriver::FromDisk(_)) {
                ui.label(cur.label());
            }
        }
        for a in acts {
            self.act(a);
        }
    }

    fn options_step(&mut self, ui: &mut egui::Ui) {
        for t in self.ctl.format_targets() {
            ui.label(egui::RichText::new(t.title.as_str()).strong());
            if let Some(opts) = options_form(ui, &t) {
                match t.row {
                    Some(row) => self.act(BuildDiskAction::SetRowOptions { row, opts }),
                    None => self.act(BuildDiskAction::SetVolumeOptions(opts)),
                }
            }
            if let Some(e) = &t.error {
                ui.colored_label(super::theme::danger(ui.visuals()), e);
            }
            ui.add_space(8.0);
        }
    }

    fn review_step(&mut self, ui: &mut egui::Ui) {
        for line in self.ctl.review_lines() {
            ui.label(line);
        }
        ui.add_space(6.0);
        for w in self.ctl.warnings() {
            let color = if w.starts_with("Warning:") {
                super::theme::warning(ui.visuals())
            } else {
                ui.visuals().text_color()
            };
            ui.colored_label(color, w);
        }
        if let Some(want) = self.ctl.confirm_expected() {
            ui.add_space(6.0);
            ui.colored_label(
                super::theme::danger(ui.visuals()),
                format!("Everything on {want} will be erased."),
            );
            ui.horizontal(|ui| {
                ui.label(format!("Type `{want}` to confirm:"));
                let mut text = self.ctl.confirm_text().to_string();
                if ui
                    .add(
                        egui::TextEdit::singleline(&mut text)
                            .desired_width(260.0)
                            .id(egui::Id::unique(ids::CONFIRM)),
                    )
                    .changed()
                {
                    self.act(BuildDiskAction::SetConfirmText(text));
                }
            });
        }
    }

    fn build_step(&mut self, ui: &mut egui::Ui) {
        if let Some(snap) = self.ctl.progress() {
            if snap.is_running() {
                if super::progress::show_job(ui, &snap) {
                    self.act(BuildDiskAction::Cancel);
                }
                return;
            }
        }
        match self.ctl.outcome() {
            Some(Ok(())) => {
                ui.colored_label(super::theme::success(ui.visuals()), "Disk build complete.");
            }
            Some(Err(e)) => {
                ui.colored_label(super::theme::danger(ui.visuals()), format!("Error: {e}"));
            }
            None => {}
        }
        ui.add_space(6.0);
        ui.horizontal(|ui| {
            if let Some(img) = self.ctl.built_image() {
                if ui.button("Open in Inspect").clicked() {
                    self.open_inspect = Some(img.to_path_buf());
                }
            }
            if ui.button("Build Another").clicked() {
                self.act(BuildDiskAction::BuildAnother);
            }
        });
    }
}

/// A filesystem picker; with `empty` it also offers "Empty". Returns the new choice.
fn fs_combo(
    ui: &mut egui::Ui,
    id: &str,
    current: Option<FsChoice>,
    empty: bool,
) -> Option<Option<FsChoice>> {
    let text = match current {
        Some(k) => k.label(),
        None => "Empty".to_string(),
    };
    let mut out = None;
    egui::ComboBox::from_id_salt(id)
        .selected_text(text)
        .width(220.0)
        .height(400.0)
        .show_ui(ui, |ui| {
            if empty && ui.selectable_label(current.is_none(), "Empty").clicked() {
                out = Some(None);
            }
            for k in FsChoice::all() {
                if ui.selectable_label(current == Some(k), k.label()).clicked() {
                    out = Some(Some(k));
                }
            }
        });
    out
}

/// A size field that may be blank (auto); its text lives in egui memory while it is edited.
fn size_option(ui: &mut egui::Ui, id: egui::Id, value: Option<u64>) -> Option<Option<u64>> {
    let mut text: String = ui
        .data_mut(|d| d.get_temp::<String>(id))
        .unwrap_or_else(|| value.map(|v| v.to_string()).unwrap_or_default());
    let resp = ui.add(
        egui::TextEdit::singleline(&mut text)
            .desired_width(80.0)
            .hint_text("auto")
            .id(id),
    );
    let mut out = None;
    if resp.changed() {
        if let Ok(v) = parse_size_field(text.as_str()) {
            if v != value {
                out = Some(v);
            }
        }
        ui.data_mut(|d| d.insert_temp(id, text));
    }
    if resp.lost_focus() {
        ui.data_mut(|d| d.remove::<String>(id));
    }
    out
}

/// The label and per-filesystem options of one formatted target; the edited copy when changed.
fn options_form(ui: &mut egui::Ui, t: &FormatTarget) -> Option<FormatOptions> {
    let mut o = t.opts.clone();
    let key = (t.row, t.fs.id());
    egui::Grid::new(("bd_opts", key)).show(ui, |ui| {
        if t.caps.label_max > 0 {
            ui.label("Label:");
            ui.add(
                egui::TextEdit::singleline(&mut o.label)
                    .desired_width(220.0)
                    .id(egui::Id::unique((ids::LABEL, key))),
            )
            .on_hover_text(format!("Up to {} bytes", t.caps.label_max));
            ui.end_row();
        }
        for opt in t.fields() {
            match opt {
                FsOption::BlockSize => {
                    ui.label("Block size:");
                    if let Some(v) = size_option(
                        ui,
                        egui::Id::unique(("bd_bs", key)),
                        o.block_size.map(u64::from),
                    ) {
                        OptionEdit::BlockSize(v).apply(&mut o);
                    }
                }
                FsOption::BigEndian => {
                    ui.label("Byte order:");
                    ui.checkbox(&mut o.big_endian, "Big-endian");
                }
                FsOption::Fat32 => {
                    ui.label("FAT type:");
                    ui.checkbox(&mut o.fat32, "FAT32 whatever the size")
                        .on_hover_text(hints::FAT32);
                }
                FsOption::CaseSensitive => {
                    ui.label("Case:");
                    ui.checkbox(&mut o.case_sensitive, "Case-sensitive (HFSX)");
                }
                FsOption::AffsVariant => {
                    ui.label("Variant:");
                    let cur = AFFS_VARIANTS.iter().find(|v| v.0 == o.affs_variant);
                    egui::ComboBox::from_id_salt(("bd_affs", key))
                        .selected_text(cur.map_or("?", |v| v.1))
                        .show_ui(ui, |ui| {
                            for (v, name) in AFFS_VARIANTS {
                                ui.selectable_value(&mut o.affs_variant, v, name);
                            }
                        });
                }
                FsOption::CpmPreset => {
                    ui.label("Disk format:");
                    let cur = o.cpm_preset.clone().unwrap_or_default();
                    egui::ComboBox::from_id_salt(("bd_cpm", key))
                        .selected_text(cur.as_str())
                        .show_ui(ui, |ui| {
                            for p in cpm_presets() {
                                if ui.selectable_label(cur == p, p).clicked() {
                                    o.cpm_preset = Some(p.to_string());
                                }
                            }
                        });
                }
                FsOption::Geometry => {
                    ui.label("Geometry:");
                    let all = geometries(t.fs);
                    let cur = o.geometry.clone().unwrap_or_else(|| all[0].0.to_string());
                    let shown = all.iter().find(|g| g.0 == cur).map_or("?", |g| g.1);
                    egui::ComboBox::from_id_salt(("bd_geo", key))
                        .selected_text(shown)
                        .show_ui(ui, |ui| {
                            for (id, label) in all {
                                if ui.selectable_label(cur == *id, *label).clicked() {
                                    o.geometry = Some(id.to_string());
                                }
                            }
                        });
                }
                FsOption::Inodes => {
                    ui.label("Inodes:");
                    if let Some(v) = size_option(ui, egui::Id::unique(("bd_ino", key)), o.inodes) {
                        OptionEdit::Inodes(v).apply(&mut o);
                    }
                }
                FsOption::BytesPerInode => {
                    ui.label("Bytes per inode:");
                    if let Some(v) =
                        size_option(ui, egui::Id::unique(("bd_bpi", key)), o.bytes_per_inode)
                    {
                        OptionEdit::BytesPerInode(v).apply(&mut o);
                    }
                }
                FsOption::ClusterSize => {
                    ui.label("Cluster size:");
                    if let Some(v) = size_option(
                        ui,
                        egui::Id::unique(("bd_cl", key)),
                        o.cluster_bytes.map(u64::from),
                    ) {
                        OptionEdit::ClusterSize(v).apply(&mut o);
                    }
                }
                FsOption::SectorSize => {
                    ui.label("Sector size:");
                    egui::ComboBox::from_id_salt(("bd_ss", key))
                        .selected_text(o.sector_size.unwrap_or(512).to_string())
                        .show_ui(ui, |ui| {
                            for s in SECTOR_SIZES {
                                if ui
                                    .selectable_label(
                                        o.sector_size.unwrap_or(512) == s,
                                        s.to_string(),
                                    )
                                    .clicked()
                                {
                                    OptionEdit::SectorSize(s).apply(&mut o);
                                }
                            }
                        });
                }
                FsOption::NtfsVersion => {
                    ui.label("NTFS version:");
                    let now = o.ntfs_version.unwrap_or_default();
                    let shown = NTFS_VERSIONS
                        .iter()
                        .find(|v| v.0 == now)
                        .map_or("?", |v| v.1);
                    egui::ComboBox::from_id_salt(("bd_ntfsv", key))
                        .selected_text(shown)
                        .show_ui(ui, |ui| {
                            for (v, label) in NTFS_VERSIONS {
                                if ui.selectable_label(now == v, label).clicked() {
                                    OptionEdit::NtfsVersion(v).apply(&mut o);
                                }
                            }
                        });
                }
                FsOption::BtreeSizes | FsOption::MinCatalog => continue,
            }
            ui.end_row();
        }
    });
    (o != t.opts).then_some(o)
}

/// An option's plain-language help, indented under it.
fn help(ui: &mut egui::Ui, text: &str) {
    ui.indent(text, |ui| {
        ui.label(egui::RichText::new(text).small().weak());
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_backup::controller::build_disk::{purpose, FsKind};
    use rusty_backup::controller::log::LogStore;

    /// Renders every step headlessly: egui panics on a Grid whose rows disagree on cell count or
    /// a clashing id, which no compile catches.
    #[test]
    fn every_step_renders() {
        for kind in DiskKind::ALL {
            let mut tab = BuildDiskTab::default();
            tab.act(BuildDiskAction::SetKind(kind));
            tab.act(BuildDiskAction::Picked {
                purpose: purpose::OUTPUT_IMAGE,
                path: Some(PathBuf::from("/tmp/never-written.img")),
            });
            tab.act(BuildDiskAction::SetSize("512M".into()));
            tab.act(BuildDiskAction::Table(BuildAction::AddRow));
            tab.act(BuildDiskAction::SetRowFs {
                row: 0,
                fs: Some(FsKind::Ufs.into()),
            });
            tab.act(BuildDiskAction::SetRowFs {
                row: 1,
                fs: Some(FsKind::Ntfs.into()),
            });
            tab.act(BuildDiskAction::SetVolumeFs(FsKind::Cpm.into()));
            let mut log = LogStore::default();
            let mut progress = ProgressState::default();
            for _ in 0..8 {
                egui::__run_test_ui(|ui| {
                    let mut ctx = TabContext::new(&[], &mut log);
                    tab.show(ui, &mut ctx, &mut progress);
                });
                if tab.ctl.step() == Step::Review {
                    break;
                }
                tab.act(BuildDiskAction::Next);
            }
            assert_eq!(tab.ctl.step(), Step::Review, "{:?}", tab.ctl.next_blocked());
        }
    }
}
