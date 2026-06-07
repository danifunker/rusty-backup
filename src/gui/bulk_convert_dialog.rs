//! Bulk Convert dialog: convert every disk image in a folder to one chosen
//! output format, dropping results into a separate output folder.
//!
//! Reuses [`rusty_backup::rbformats::export::export_whole_disk`] (the same
//! single-file path the Inspect tab uses) — there is no per-partition sizing
//! in bulk mode, every disk exports at original size.
//!
//! Flow:
//! 1. **Setup** — pick source folder, output folder, output format.
//! 2. **Review** — scan source folder (no filtering); user un-checks any files
//!    they want to skip. Files that fail to open / detect during conversion
//!    surface as warnings in the log panel afterward.

use std::path::PathBuf;

use rusty_backup::model::bulk_convert_runner::{fmt_bytes, scan_source_folder, ScannedFile};
use rusty_backup::rbformats::chd_options::{ChdOptions, ChdProfile};
use rusty_backup::rbformats::export::ExportFormat;

use super::chd_options_ui::ChdOptionsControl;

// Re-export so `gui::mod` can keep its `bulk_convert_dialog::start_bulk_convert(...)` call.
pub use rusty_backup::model::bulk_convert_runner::start_bulk_convert;

/// One pending output filename collision.
struct Conflict {
    source: PathBuf,
    /// Existing destination file in the output folder.
    dest: PathBuf,
}

/// Which view of the dialog is active.
enum Phase {
    Setup,
    Review {
        files: Vec<ScannedFile>,
        /// User-editable output extension (no leading dot). Defaults to the
        /// recommended extension for the chosen format.
        extension: String,
        /// When the user clicks Start and one or more output files already
        /// exist in the destination, we stash the collisions here and render
        /// a confirmation modal instead of immediately emitting Start.
        pending_conflicts: Option<Vec<Conflict>>,
    },
}

/// Modal dialog state.
pub struct BulkConvertDialog {
    pub source_folder: Option<PathBuf>,
    pub output_folder: Option<PathBuf>,
    pub format: ExportFormat,
    /// CHD options state — one per profile so toggling between HD/DVD/CD
    /// keeps each profile's last custom selection.
    chd_hd_control: ChdOptionsControl,
    chd_dvd_control: ChdOptionsControl,
    chd_cd_control: ChdOptionsControl,
    /// When the BIN/CUE output format is selected, write one .bin per track
    /// instead of a single concatenated .bin. Multi-bin is a feature beyond
    /// chdman, built on top of libchdman-rs's track metadata.
    bincue_multi_bin: bool,
    phase: Phase,
    /// Set when scan returns no usable entries; surfaced inline in the dialog.
    scan_error: Option<String>,
}

impl Default for BulkConvertDialog {
    fn default() -> Self {
        Self {
            source_folder: None,
            output_folder: None,
            format: ExportFormat::Vhd,
            chd_hd_control: ChdOptionsControl::new(ChdProfile::Hd),
            chd_dvd_control: ChdOptionsControl::new(ChdProfile::Dvd),
            chd_cd_control: ChdOptionsControl::new(ChdProfile::Cd),
            bincue_multi_bin: false,
            phase: Phase::Setup,
            scan_error: None,
        }
    }
}

/// Outcome of `show()` — caller acts on this to start the worker or close.
pub enum DialogAction {
    None,
    /// User confirmed Start in the Review phase. Carries the list of selected
    /// files so the worker doesn't have to re-scan.
    Start {
        files: Vec<PathBuf>,
        extension: String,
        /// Resolved CHD options when `format` is HD/DVD/CD CHD; `None` otherwise.
        chd_options: Option<ChdOptions>,
        /// True only for `ExportFormat::BinCue` when the user wants per-track .bin files.
        bincue_multi_bin: bool,
    },
    Cancel,
}

impl BulkConvertDialog {
    /// Render the dialog. Returns the user's intent for this frame.
    pub fn show(&mut self, ctx: &egui::Context) -> DialogAction {
        match self.phase {
            Phase::Setup => self.show_setup(ctx),
            Phase::Review { .. } => self.show_review(ctx),
        }
    }

    fn show_setup(&mut self, ctx: &egui::Context) -> DialogAction {
        let mut action = DialogAction::None;
        let mut go_to_review = false;

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
                        .source_folder
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(not selected)".to_string());
                    ui.label(label);
                });
                if ui.button("Choose Source Folder…").clicked() {
                    if let Some(p) = super::file_dialog().pick_folder() {
                        self.source_folder = Some(p);
                    }
                }

                ui.add_space(6.0);

                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Output folder:").strong());
                    let label = self
                        .output_folder
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "(not selected)".to_string());
                    ui.label(label);
                });
                if ui.button("Choose Output Folder…").clicked() {
                    if let Some(p) = super::file_dialog().pick_folder() {
                        self.output_folder = Some(p);
                    }
                }

                ui.add_space(8.0);

                ui.label(egui::RichText::new("Output format:").strong());
                ui.horizontal_wrapped(|ui| {
                    ui.radio_value(&mut self.format, ExportFormat::Vhd, "VHD");
                    ui.radio_value(&mut self.format, ExportFormat::VhdDynamic, "VHD (Dynamic)")
                        .on_hover_text(
                            "Sparse VHD — all-zero blocks are omitted. Same .vhd extension; \
                             readable by Hyper-V, qemu-img, Disk Management.",
                        );
                    ui.radio_value(&mut self.format, ExportFormat::Qcow2, "QCOW2")
                        .on_hover_text(
                            "QCOW2 v3 — sparse, uncompressed. The format UTM uses for \
                             classic-Mac PPC guests; opens in QEMU, qemu-img, virt-manager.",
                        );
                    ui.radio_value(&mut self.format, ExportFormat::VmdkFlat, "VMDK (Flat)")
                        .on_hover_text(
                            "monolithicFlat VMDK — descriptor + <name>-flat.vmdk pair. \
                             Opens in VMware, VirtualBox, qemu-img.",
                        );
                    ui.radio_value(&mut self.format, ExportFormat::VmdkSparse, "VMDK (Sparse)")
                        .on_hover_text(
                            "monolithicSparse VMDK — single self-contained .vmdk; \
                             zero grains omitted. Opens in VMware, VirtualBox, qemu-img.",
                        );
                    ui.radio_value(&mut self.format, ExportFormat::Raw, "Raw (.img)");
                    ui.radio_value(&mut self.format, ExportFormat::TwoMg, "2MG (.2mg)");
                    ui.radio_value(&mut self.format, ExportFormat::Woz, "WOZ (.woz)")
                        .on_hover_text("Floppy only: 140K / 400K / 800K sources");
                    ui.radio_value(&mut self.format, ExportFormat::Dc42, "DiskCopy 4.2 (.dsk)")
                        .on_hover_text("Floppy only: 400K / 720K / 800K / 1440K sources");
                    ui.radio_value(&mut self.format, ExportFormat::Chd, "CHD (Hard Disk)")
                        .on_hover_text("MAME hard-disk CHD (512-byte unit)");
                    ui.radio_value(&mut self.format, ExportFormat::ChdDvd, "DVD CHD")
                        .on_hover_text("MAME DVD CHD (2048-byte unit, MAME 0.287+)");
                    ui.radio_value(&mut self.format, ExportFormat::ChdCd, "CD CHD")
                        .on_hover_text("MAME CD CHD — input must be .iso or .cue");
                    ui.radio_value(&mut self.format, ExportFormat::BinCue, "BIN/CUE")
                        .on_hover_text("Extract a CD CHD into a BIN/CUE pair");
                    ui.radio_value(&mut self.format, ExportFormat::Xdf, "XDF (.xdf)")
                        .on_hover_text(
                            "X68000 raw headerless floppy dump. \
                             Floppy only (.xdf/.hdm/.dim/.d88 sources).",
                        );
                    ui.radio_value(&mut self.format, ExportFormat::Hdm, "HDM (.hdm)")
                        .on_hover_text(
                            "PC-98 / DiskExplorer raw floppy dump. \
                             Byte-identical layout to XDF.",
                        );
                    ui.radio_value(&mut self.format, ExportFormat::Dim, "DIM (.dim)")
                        .on_hover_text(
                            "DiskExplorer DIFC DIM (256-byte header + payload). \
                             Floppy only; 640 KB 2DD not representable here.",
                        );
                    ui.radio_value(&mut self.format, ExportFormat::D88, "D88 (.d88)")
                        .on_hover_text(
                            "Sharp D88 sparse track-table container (X68000 / PC-88 / PC-98 / FM-7).",
                        );
                });

                match self.format {
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
                            &mut self.bincue_multi_bin,
                            "Multi-bin (one .bin per track)",
                        )
                        .on_hover_text(
                            "Beyond chdman: writes <name> (Track NN).bin per track plus a \
                             multi-FILE cue. Falls back to single-bin if track sizes don't match.",
                        );
                    }
                    _ => {}
                }

                if let (Some(src), Some(out)) = (&self.source_folder, &self.output_folder) {
                    if src == out {
                        ui.add_space(6.0);
                        ui.colored_label(
                            egui::Color32::YELLOW,
                            "Warning: source and output folders are the same — converted files may overwrite originals.",
                        );
                    }
                }

                if let Some(err) = &self.scan_error {
                    ui.add_space(6.0);
                    ui.colored_label(egui::Color32::from_rgb(255, 100, 100), err);
                }

                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    let ready =
                        self.source_folder.is_some() && self.output_folder.is_some();
                    if ui
                        .add_enabled(ready, egui::Button::new("Scan…"))
                        .on_hover_text("Read the source folder and review the file list before converting.")
                        .clicked()
                    {
                        go_to_review = true;
                    }
                    if ui.button("Cancel").clicked() {
                        action = DialogAction::Cancel;
                    }
                });
            });

        if go_to_review {
            match scan_source_folder(self.source_folder.as_ref().unwrap(), self.format) {
                Ok(files) if files.is_empty() => {
                    self.scan_error = Some("No files found in source folder.".to_string());
                }
                Ok(files) => {
                    self.scan_error = None;
                    self.phase = Phase::Review {
                        files,
                        extension: self.format.extension().to_string(),
                        pending_conflicts: None,
                    };
                }
                Err(e) => {
                    self.scan_error = Some(format!("Scan failed: {e}"));
                }
            }
        }

        action
    }

    fn show_review(&mut self, ctx: &egui::Context) -> DialogAction {
        let mut action = DialogAction::None;
        let mut go_back = false;
        let mut start = false;
        let mut select_all = false;
        let mut select_none = false;

        let recommended_ext = self.format.extension();
        let output_folder = self.output_folder.clone();
        let chd_options: Option<ChdOptions> = match self.format {
            ExportFormat::Chd => Some(self.chd_hd_control.effective(ChdProfile::Hd)),
            ExportFormat::ChdDvd => Some(self.chd_dvd_control.effective(ChdProfile::Dvd)),
            ExportFormat::ChdCd => Some(self.chd_cd_control.effective(ChdProfile::Cd)),
            _ => None,
        };
        let bincue_multi_bin = self.format == ExportFormat::BinCue && self.bincue_multi_bin;
        let Phase::Review {
            files,
            extension,
            pending_conflicts,
        } = &mut self.phase
        else {
            return action;
        };

        egui::Window::new("Bulk Convert — Review")
            .collapsible(false)
            .resizable(true)
            .default_width(640.0)
            .default_height(500.0)
            .show(ctx, |ui| {
                let total = files.len();
                let selected_count = files.iter().filter(|f| f.selected).count();

                ui.label(format!(
                    "Found {} file(s) in source folder. {} selected for conversion.",
                    total, selected_count,
                ));
                ui.label(
                    "Files that fail to open or aren't recognized as disk images will be \
                     reported as warnings in the log after conversion finishes.",
                );

                ui.add_space(6.0);

                // Extension field — pre-filled with the recommended extension
                // for the chosen format. The user can override (e.g. ".hda"
                // instead of ".vhd") and reset back to the recommendation.
                ui.horizontal(|ui| {
                    ui.label(egui::RichText::new("Output extension:").strong());
                    ui.label(".");
                    ui.add(
                        egui::TextEdit::singleline(extension)
                            .desired_width(80.0)
                            .hint_text(recommended_ext),
                    );
                    if ui
                        .add_enabled(
                            extension.as_str() != recommended_ext,
                            egui::Button::new("Reset"),
                        )
                        .on_hover_text(format!("Reset to recommended: .{recommended_ext}"))
                        .clicked()
                    {
                        *extension = recommended_ext.to_string();
                    }
                    ui.label(
                        egui::RichText::new(format!("(recommended: .{recommended_ext})")).weak(),
                    );
                });

                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Select All").clicked() {
                        select_all = true;
                    }
                    if ui.button("Select None").clicked() {
                        select_none = true;
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

                                for f in files.iter_mut() {
                                    ui.checkbox(&mut f.selected, "");
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
                        go_back = true;
                    }
                    let ext_ok =
                        !extension.trim().is_empty() && !extension.contains(['/', '\\', '.', ' ']);
                    let can_start = selected_count > 0 && ext_ok;
                    let label = format!("Start Conversion ({selected_count})");
                    if ui
                        .add_enabled(can_start, egui::Button::new(label))
                        .clicked()
                    {
                        start = true;
                    }
                    if ui.button("Cancel").clicked() {
                        action = DialogAction::Cancel;
                    }
                });
            });

        if select_all {
            for f in files.iter_mut() {
                f.selected = true;
            }
        }
        if select_none {
            for f in files.iter_mut() {
                f.selected = false;
            }
        }
        // If the user clicked Start, detect output-folder collisions before
        // emitting the action. If any are found, populate `pending_conflicts`
        // so the modal renders next frame; otherwise emit Start immediately.
        let mut emit_start: Option<(Vec<PathBuf>, String, Option<ChdOptions>, bool)> = None;
        if start {
            let ext = extension.trim().to_string();
            let chosen_paths: Vec<PathBuf> = files
                .iter()
                .filter(|f| f.selected)
                .map(|f| f.path.clone())
                .collect();

            let conflicts = if let Some(out) = &output_folder {
                detect_conflicts(&chosen_paths, out, &ext)
            } else {
                Vec::new()
            };

            if conflicts.is_empty() {
                emit_start = Some((chosen_paths, ext, chd_options.clone(), bincue_multi_bin));
            } else {
                *pending_conflicts = Some(conflicts);
            }
        }

        // Render the conflict modal on top of the Review window if collisions
        // are pending. Returns the user's resolution, if any.
        let resolution = if let Some(conflicts) = pending_conflicts.as_ref() {
            show_conflict_modal(ctx, conflicts)
        } else {
            ConflictResolution::None
        };

        match resolution {
            ConflictResolution::SkipConflicting => {
                let conflict_set: std::collections::HashSet<PathBuf> = pending_conflicts
                    .as_ref()
                    .map(|cs| cs.iter().map(|c| c.source.clone()).collect())
                    .unwrap_or_default();
                let chosen: Vec<PathBuf> = files
                    .iter()
                    .filter(|f| f.selected && !conflict_set.contains(&f.path))
                    .map(|f| f.path.clone())
                    .collect();
                let ext = extension.trim().to_string();
                *pending_conflicts = None;
                if !chosen.is_empty() {
                    emit_start = Some((chosen, ext, chd_options.clone(), bincue_multi_bin));
                }
            }
            ConflictResolution::Overwrite => {
                let chosen: Vec<PathBuf> = files
                    .iter()
                    .filter(|f| f.selected)
                    .map(|f| f.path.clone())
                    .collect();
                let ext = extension.trim().to_string();
                *pending_conflicts = None;
                emit_start = Some((chosen, ext, chd_options.clone(), bincue_multi_bin));
            }
            ConflictResolution::Back => {
                *pending_conflicts = None;
            }
            ConflictResolution::None => {}
        }

        // Drop the &mut borrow on self.phase before mutating it.
        if go_back {
            self.phase = Phase::Setup;
        }
        if let Some((files, extension, chd_options, bincue_multi_bin)) = emit_start {
            action = DialogAction::Start {
                files,
                extension,
                chd_options,
                bincue_multi_bin,
            };
        }

        action
    }
}

#[derive(Clone, Copy)]
enum ConflictResolution {
    None,
    SkipConflicting,
    Overwrite,
    Back,
}

fn detect_conflicts(
    sources: &[PathBuf],
    output: &std::path::Path,
    extension: &str,
) -> Vec<Conflict> {
    let mut out = Vec::new();
    for src in sources {
        let stem = src.file_stem().and_then(|s| s.to_str()).unwrap_or("disk");
        let dest = output.join(format!("{stem}.{extension}"));
        if dest.exists() {
            out.push(Conflict {
                source: src.clone(),
                dest,
            });
        }
    }
    out
}

fn show_conflict_modal(ctx: &egui::Context, conflicts: &[Conflict]) -> ConflictResolution {
    let mut resolution = ConflictResolution::None;

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
                    resolution = ConflictResolution::SkipConflicting;
                }
                if ui.button("Overwrite").clicked() {
                    resolution = ConflictResolution::Overwrite;
                }
                if ui.button("Back").clicked() {
                    resolution = ConflictResolution::Back;
                }
            });
        });

    resolution
}
