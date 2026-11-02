//! Bulk Convert dialog: a shell over `controller::bulk_convert`.
//!
//! Setup picks the folders and the format, Review ticks files and sets the output extension, and
//! a modal asks about outputs that already exist. The controller owns the form, the scan, the
//! conflict check and the conversion; this view keeps only the per-profile CHD option controls.

use std::sync::Arc;

use rusty_backup::controller::bulk_convert::{
    chd_profile, fmt_bytes, BulkConvertAction, BulkConvertController, BulkPhase, ChdOptions,
    ChdProfile, Conflict, ConflictChoice, ExportFormat,
};
use rusty_backup::controller::log::LogStore;
use rusty_backup::controller::{Controller, Effect};

use super::chd_options_ui::ChdOptionsControl;
use super::effects::{EffectHost, Reply};

/// The dialog's widget state around the controller.
pub struct BulkConvertDialog {
    ctl: BulkConvertController,
    effects: EffectHost,
    pending_fx: Vec<Effect>,
    /// One CHD options control per profile, so switching formats keeps each one's choices.
    chd_hd_control: ChdOptionsControl,
    chd_dvd_control: ChdOptionsControl,
    chd_cd_control: ChdOptionsControl,
}

impl Default for BulkConvertDialog {
    fn default() -> Self {
        Self {
            ctl: BulkConvertController::new(Arc::new(super::ui_logger::EguiWake)),
            effects: EffectHost::default(),
            pending_fx: Vec::new(),
            chd_hd_control: ChdOptionsControl::new(ChdProfile::Hd),
            chd_dvd_control: ChdOptionsControl::new(ChdProfile::Dvd),
            chd_cd_control: ChdOptionsControl::new(ChdProfile::Cd),
        }
    }
}

impl BulkConvertDialog {
    /// Show a fresh form (the CHD controls keep their last choices only while the app runs).
    pub fn open(&mut self) {
        self.chd_hd_control = ChdOptionsControl::new(ChdProfile::Hd);
        self.chd_dvd_control = ChdOptionsControl::new(ChdProfile::Dvd);
        self.chd_cd_control = ChdOptionsControl::new(ChdProfile::Cd);
        self.act(BulkConvertAction::Open);
    }

    pub fn is_running(&self) -> bool {
        self.ctl.is_running()
    }

    pub fn cancel(&mut self) {
        self.act(BulkConvertAction::Cancel);
    }

    /// The progress strip's fraction and text while a conversion runs.
    pub fn strip(&self) -> Option<(f32, String)> {
        let p = self.ctl.progress()?;
        let frac = if p.total_bytes > 0 {
            (p.bytes as f32 / p.total_bytes as f32).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let text = if p.total_bytes > 0 {
            format!(
                "Bulk Convert: [{}/{}] {} - {} / {} ({:.0}%)",
                p.index,
                p.total_files,
                p.file,
                fmt_bytes(p.bytes),
                fmt_bytes(p.total_bytes),
                frac * 100.0,
            )
        } else {
            format!(
                "Bulk Convert: [{}/{}] {} - preparing...",
                p.index, p.total_files, p.file,
            )
        };
        Some((frac, text))
    }

    fn act(&mut self, action: BulkConvertAction) {
        let fx = self.ctl.apply(action);
        self.pending_fx.extend(fx);
    }

    /// Poll the controller and drain its log; call every frame, open or not.
    pub fn poll(&mut self, log: &mut LogStore) {
        let fx = self.ctl.tick();
        self.pending_fx.extend(fx);
        super::progress::drain_log_lines(self.ctl.take_log(), log);
    }

    /// Render the dialog when open.
    pub fn show(&mut self, ctx: &egui::Context, log: &mut LogStore) {
        let fx = std::mem::take(&mut self.pending_fx);
        let mut messages = Vec::new();
        let mut replies = self.effects.run(ctx, fx, &mut messages);
        replies.extend(self.effects.show(ctx, "Bulk convert"));
        for m in messages {
            log.info(m);
        }
        for reply in replies {
            match reply {
                Reply::Picked { purpose, path } => {
                    self.act(BulkConvertAction::Picked { purpose, path })
                }
                Reply::Confirmed { .. } | Reply::Answered { .. } | Reply::CloseScreen => {}
            }
        }
        if !self.ctl.is_open() {
            return;
        }
        let actions = match self.ctl.phase() {
            BulkPhase::Setup => self.show_setup(ctx),
            BulkPhase::Review => self.show_review(ctx),
        };
        for a in actions {
            self.act(a);
        }
    }

    fn chd_options(&self) -> Option<ChdOptions> {
        match chd_profile(self.ctl.format())? {
            ChdProfile::Hd => Some(self.chd_hd_control.effective(ChdProfile::Hd)),
            ChdProfile::Dvd => Some(self.chd_dvd_control.effective(ChdProfile::Dvd)),
            ChdProfile::Cd => Some(self.chd_cd_control.effective(ChdProfile::Cd)),
        }
    }

    fn show_setup(&mut self, ctx: &egui::Context) -> Vec<BulkConvertAction> {
        let mut actions = Vec::new();
        let mut format = self.ctl.format();
        let mut multi_bin = self.ctl.multi_bin();

        egui::Window::new("Bulk Convert")
            .collapsible(false)
            .resizable(true)
            .default_width(560.0)
            .show(ctx, |ui| {
                ui.label(
                    "Convert every disk image in a folder to a single output format. \
                     All files use the same parameters; original size is preserved.",
                );
                ui.add_space(8.0);

                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Source folder:").strong());
                    let label = self
                        .ctl
                        .source()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(not selected)".to_string());
                    ui.label(label);
                });
                if ui.button("Choose Source Folder...").clicked() {
                    actions.push(BulkConvertAction::PickSource);
                }

                ui.add_space(6.0);

                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Output folder:").strong());
                    let label = self
                        .ctl
                        .output()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(not selected)".to_string());
                    ui.label(label);
                });
                if ui.button("Choose Output Folder...").clicked() {
                    actions.push(BulkConvertAction::PickOutput);
                }

                ui.add_space(8.0);

                ui.label(egui::RichText::new("Output format:").strong());
                ui.horizontal_wrapped(|ui| {
                    ui.radio_value(&mut format, ExportFormat::Vhd, "VHD");
                    ui.radio_value(&mut format, ExportFormat::VhdDynamic, "VHD (Dynamic)")
                        .on_hover_text(
                            "Sparse VHD - all-zero blocks are omitted. Same .vhd extension; \
                             readable by Hyper-V, qemu-img, Disk Management.",
                        );
                    ui.radio_value(&mut format, ExportFormat::Qcow2, "QCOW2")
                        .on_hover_text(
                            "QCOW2 v3 - sparse, uncompressed. The format UTM uses for \
                             classic-Mac PPC guests; opens in QEMU, qemu-img, virt-manager.",
                        );
                    ui.radio_value(&mut format, ExportFormat::VmdkFlat, "VMDK (Flat)")
                        .on_hover_text(
                            "monolithicFlat VMDK - descriptor + <name>-flat.vmdk pair. \
                             Opens in VMware, VirtualBox, qemu-img.",
                        );
                    ui.radio_value(&mut format, ExportFormat::VmdkSparse, "VMDK (Sparse)")
                        .on_hover_text(
                            "monolithicSparse VMDK - single self-contained .vmdk; \
                             zero grains omitted. Opens in VMware, VirtualBox, qemu-img.",
                        );
                    ui.radio_value(&mut format, ExportFormat::Raw, "Raw (.img)");
                    ui.radio_value(&mut format, ExportFormat::TwoMg, "2MG (.2mg)");
                    ui.radio_value(&mut format, ExportFormat::Woz, "WOZ (.woz)")
                        .on_hover_text("Floppy only: 140K / 400K / 800K sources");
                    ui.radio_value(&mut format, ExportFormat::Moof, "MOOF (.moof)")
                        .on_hover_text("Macintosh 3.5\" bitstream, floppy only: 400K / 800K (Snow / MAME)");
                    ui.radio_value(&mut format, ExportFormat::Dc42, "DiskCopy 4.2 (.dsk)")
                        .on_hover_text("Floppy only: 400K / 720K / 800K / 1440K sources");
                    ui.radio_value(&mut format, ExportFormat::Chd, "CHD (Hard Disk)")
                        .on_hover_text("MAME hard-disk CHD (512-byte unit)");
                    ui.radio_value(&mut format, ExportFormat::ChdDvd, "DVD CHD")
                        .on_hover_text("MAME DVD CHD (2048-byte unit, MAME 0.287+)");
                    ui.radio_value(&mut format, ExportFormat::ChdCd, "CD CHD")
                        .on_hover_text("MAME CD CHD - input must be .iso or .cue");
                    ui.radio_value(&mut format, ExportFormat::BinCue, "BIN/CUE")
                        .on_hover_text("Extract a CD CHD into a BIN/CUE pair");
                    ui.radio_value(&mut format, ExportFormat::Xdf, "XDF (.xdf)")
                        .on_hover_text(
                            "X68000 raw headerless floppy dump. \
                             Floppy only (.xdf/.hdm/.dim/.d88 sources).",
                        );
                    ui.radio_value(&mut format, ExportFormat::Hdm, "HDM (.hdm)")
                        .on_hover_text(
                            "PC-98 / DiskExplorer raw floppy dump. \
                             Byte-identical layout to XDF.",
                        );
                    ui.radio_value(&mut format, ExportFormat::Dim, "DIM (.dim)")
                        .on_hover_text(
                            "DiskExplorer DIFC DIM (256-byte header + payload). \
                             Floppy only; 640 KB 2DD not representable here.",
                        );
                    ui.radio_value(&mut format, ExportFormat::D88, "D88 (.d88)")
                        .on_hover_text(
                            "Sharp D88 sparse track-table container (X68000 / PC-88 / PC-98 / FM-7).",
                        );
                });

                match format {
                    ExportFormat::Chd => {
                        ui.add_space(4.0);
                        super::chd_options_ui::show(
                            ui,
                            "bulk_chd",
                            ChdProfile::Hd,
                            &mut self.chd_hd_control,
                        );
                    }
                    ExportFormat::ChdDvd => {
                        ui.add_space(4.0);
                        super::chd_options_ui::show(
                            ui,
                            "bulk_chd",
                            ChdProfile::Dvd,
                            &mut self.chd_dvd_control,
                        );
                    }
                    ExportFormat::ChdCd => {
                        ui.add_space(4.0);
                        super::chd_options_ui::show(
                            ui,
                            "bulk_chd",
                            ChdProfile::Cd,
                            &mut self.chd_cd_control,
                        );
                    }
                    ExportFormat::BinCue => {
                        ui.add_space(4.0);
                        ui.checkbox(
                            &mut multi_bin,
                            "Multi-bin (one .bin per track)",
                        )
                        .on_hover_text(
                            "Beyond chdman: writes <name> (Track NN).bin per track plus a \
                             multi-FILE cue. Falls back to single-bin if track sizes don't match.",
                        );
                    }
                    _ => {}
                }


                if self.ctl.same_folders() {
                    ui.add_space(6.0);
                    ui.colored_label(
                        super::theme::warning(ui.visuals()),
                        "Warning: source and output folders are the same - converted files may overwrite originals.",
                    );
                }

                if let Some(err) = self.ctl.scan_error() {
                    ui.add_space(6.0);
                    ui.colored_label(super::theme::danger(ui.visuals()), err);
                }

                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(self.ctl.can_scan(), egui::Button::new("Scan..."))
                        .on_hover_text("Read the source folder and review the file list before converting.")
                        .clicked()
                    {
                        actions.push(BulkConvertAction::Scan);
                    }
                    if ui.button("Cancel").clicked() {
                        actions.push(BulkConvertAction::Close);
                    }
                });
                if let Some(snap) = self.ctl.scan_snapshot() {
                    super::progress::show_job(ui, &snap);
                }
            });

        if format != self.ctl.format() {
            actions.insert(0, BulkConvertAction::SetFormat(format));
        }
        if multi_bin != self.ctl.multi_bin() {
            actions.insert(0, BulkConvertAction::SetMultiBin(multi_bin));
        }
        actions
    }

    fn show_review(&mut self, ctx: &egui::Context) -> Vec<BulkConvertAction> {
        let mut actions = Vec::new();
        let recommended_ext = self.ctl.recommended_extension();
        let mut extension = self.ctl.extension().to_string();

        egui::Window::new("Bulk Convert - Review")
            .collapsible(false)
            .resizable(true)
            .default_width(640.0)
            .default_height(500.0)
            .show(ctx, |ui| {
                let total = self.ctl.files().len();
                let selected_count = self.ctl.selected_count();

                ui.label(format!(
                    "Found {} file(s) in source folder. {} selected for conversion.",
                    total, selected_count,
                ));
                ui.label(
                    "Files that fail to open or aren't recognized as disk images will be \
                     reported as warnings in the log after conversion finishes.",
                );

                ui.add_space(6.0);

                // Pre-filled with the format's extension; the user may override it (".hda").
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Output extension:").strong());
                    ui.label(".");
                    if ui
                        .add(
                            egui::TextEdit::singleline(&mut extension)
                                .desired_width(80.0)
                                .hint_text(recommended_ext),
                        )
                        .changed()
                    {
                        actions.push(BulkConvertAction::SetExtension(extension.clone()));
                    }
                    if ui
                        .add_enabled(
                            extension.as_str() != recommended_ext,
                            egui::Button::new("Reset"),
                        )
                        .on_hover_text(format!("Reset to recommended: .{recommended_ext}"))
                        .clicked()
                    {
                        actions.push(BulkConvertAction::ResetExtension);
                    }
                    ui.label(
                        egui::RichText::new(format!("(recommended: .{recommended_ext})")).weak(),
                    );
                });

                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Select All").clicked() {
                        actions.push(BulkConvertAction::SelectAll);
                    }
                    if ui.button("Select None").clicked() {
                        actions.push(BulkConvertAction::SelectNone);
                    }
                });

                ui.add_space(6.0);
                ui.separator();

                egui::ScrollArea::vertical()
                    .auto_shrink([false; 2])
                    .max_height(340.0)
                    .show(ui, |ui| {
                        egui::Grid::new("bulk_convert_files")
                            .striped(true)
                            .num_columns(3)
                            .min_col_width(40.0)
                            .show(ui, |ui| {
                                ui.label(egui::RichText::new("").strong());
                                ui.label(egui::RichText::new("File").strong());
                                ui.label(egui::RichText::new("Size").strong());
                                ui.end_row();

                                for (i, f) in self.ctl.files().iter().enumerate() {
                                    let mut on = f.selected;
                                    if ui.checkbox(&mut on, "").changed() {
                                        actions.push(BulkConvertAction::SetSelected(i, on));
                                    }
                                    let name =
                                        f.path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                                    ui.label(name);
                                    ui.label(fmt_bytes(f.size));
                                    ui.end_row();
                                }
                            });
                    });

                ui.separator();
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Back").clicked() {
                        actions.push(BulkConvertAction::Back);
                    }
                    let label = format!("Start Conversion ({selected_count})");
                    if ui
                        .add_enabled(self.ctl.can_start(), egui::Button::new(label))
                        .clicked()
                    {
                        actions.push(BulkConvertAction::Start(self.chd_options()));
                    }
                    if ui.button("Cancel").clicked() {
                        actions.push(BulkConvertAction::Close);
                    }
                });
            });

        if let Some(conflicts) = self.ctl.conflicts() {
            if let Some(choice) = show_conflict_modal(ctx, conflicts) {
                actions.push(BulkConvertAction::ResolveConflicts(choice));
            }
        }
        actions
    }
}

fn show_conflict_modal(ctx: &egui::Context, conflicts: &[Conflict]) -> Option<ConflictChoice> {
    let mut resolution = None;
    egui::Window::new("Output Files Already Exist")
        .collapsible(false)
        .resizable(true)
        .default_width(560.0)
        .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
        .show(ctx, |ui| {
            ui.label(format!(
                "{} file(s) in the output folder would be overwritten by this conversion:",
                conflicts.len(),
            ));
            ui.add_space(6.0);

            egui::ScrollArea::vertical()
                .auto_shrink([false; 2])
                .max_height(200.0)
                .show(ui, |ui| {
                    for c in conflicts {
                        let name = c.dest.file_name().and_then(|n| n.to_str()).unwrap_or("");
                        ui.label(format!("- {name}"));
                    }
                });

            ui.add_space(8.0);
            ui.label(
                "Skip: leave existing files alone, convert only the non-conflicting ones.\n\
                 Overwrite: replace the existing output files.",
            );
            ui.add_space(8.0);

            ui.horizontal(|ui| {
                if ui.button("Skip Conflicting").clicked() {
                    resolution = Some(ConflictChoice::SkipConflicting);
                }
                if ui.button("Overwrite").clicked() {
                    resolution = Some(ConflictChoice::Overwrite);
                }
                if ui.button("Back").clicked() {
                    resolution = Some(ConflictChoice::Back);
                }
            });
        });

    resolution
}
