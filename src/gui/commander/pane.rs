//! One Commander pane drawn from its controller: a source bar (open image / folder + partition
//! picker + Apply / Discard), a path line, and the listing grid with sortable columns, selection,
//! `..` / double-click navigation and the staged-edit overlay.
//!
//! Everything it shows comes from [`CommanderController::pane`]; every click becomes a
//! [`CommanderAction`] collected into the frame's list, which [`super::CommanderMode`] runs once
//! drawing is done. The only state here is the view's own: the review's radio choice, the
//! password field's text and the listing height last sent for Page Up / Page Down.

use eframe::egui;

use rusty_backup::controller::browser::rows::{DisplayRow, RowKind};
use rusty_backup::controller::commander::{
    format_size, CommanderAction, CommanderController, BrowserPaneAction,
    BrowserPaneController, ExportFormat, OnConflict, Side, SortColumn, MAX_CONSECUTIVE_FAILURES,
};

const ROW_H: f32 = 20.0;
/// Per-depth indent for tree rows, and the width reserved for the `+`/`-`
/// disclosure toggle (names align whether or not a row has one).
const TREE_INDENT: f32 = 14.0;
const TOGGLE_W: f32 = 16.0;
// These were consts, which is why they could not follow the theme: a const is
// evaluated once with no `Visuals` to consult. They are functions now, taking
// the visuals like every other colour in the app.
fn add_color(v: &egui::Visuals) -> egui::Color32 {
    super::super::theme::success(v)
}
fn del_color(v: &egui::Visuals) -> egui::Color32 {
    super::super::theme::muted(v)
}
/// Tint for an existing entry with staged metadata edits (type/dates/perms).
fn meta_color(v: &egui::Visuals) -> egui::Color32 {
    super::super::theme::info(v)
}
/// A file in a remote-host listing that looks like a disk image we can open
/// (a teal, distinct from the folder blue and the gray of plain files).
fn image_color(v: &egui::Visuals) -> egui::Color32 {
    super::super::theme::accent(v)
}
/// The "Close Image" affordance — amber, to read as "step back out".
fn close_image_color(v: &egui::Visuals) -> egui::Color32 {
    super::super::theme::warning(v)
}
/// The "Read-only" media badge in the volume readout — amber, "look but touch
/// carefully".
fn read_only_badge_color(v: &egui::Visuals) -> egui::Color32 {
    super::super::theme::warning(v)
}

type A = CommanderAction;
type P = BrowserPaneAction;

pub(crate) struct PaneView {
    side: Side,
    /// The review window's conflict choice until the user presses Apply.
    review_choice: OnConflict,
    /// The user's in-progress password / passphrase entry for the unlock prompt.
    password_input: String,
    /// Rows that fit in the listing, as last sent to the controller.
    page_rows: i32,
}

impl PaneView {
    pub(crate) fn new(side: Side) -> Self {
        Self {
            side,
            review_choice: OnConflict::Skip,
            password_input: String::new(),
            page_rows: 0,
        }
    }

    fn pane_action(&self, out: &mut Vec<CommanderAction>, action: BrowserPaneAction) {
        out.push(A::Pane(self.side, action));
    }

    /// Draw the pane; `active` draws the keyboard cursor and `scroll` brings it into view.
    pub(crate) fn show(
        &mut self,
        ui: &mut egui::Ui,
        ctl: &CommanderController,
        active: bool,
        scroll: bool,
        out: &mut Vec<CommanderAction>,
    ) {
        let pane = ctl.pane(self.side);
        self.source_bar(ui, pane, out);
        self.path_line(ui, pane, out);
        ui.separator();

        // A wrapper expanding as a job: a spinner and its phase above the still-visible listing.
        if let Some((name, snap)) = pane.wrapper_opening() {
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label(if snap.phase.is_empty() {
                    format!("Opening {name}...")
                } else {
                    format!("{name}: {}", snap.phase)
                });
            });
            ui.ctx().request_repaint();
        }
        if pane.checking().is_some() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label("Checking staged edits...");
            });
            ui.ctx().request_repaint();
        }
        if let Some(snap) = pane.host_writing() {
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                super::super::progress::show_job(ui, &snap);
            });
        }
        if pane.applying().is_some() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label("Applying staged edits...");
            });
        } else if let Some(snap) = pane.opening() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                if super::super::progress::show_job(ui, &snap) {
                    self.pane_action(out, P::CancelOpen);
                }
            });
        } else if pane.is_opening() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label("Opening...");
            });
        } else if pane.needs_password() {
            self.unlock_prompt(ui, pane, out);
        } else if let Some(err) = pane.source_error() {
            ui.add_space(12.0);
            ui.colored_label(super::super::theme::danger_muted(ui.visuals()), err);
        } else if pane.listing.is_loaded() {
            self.render_header(ui, pane, out);
            self.render_rows(ui, ctl, active, scroll, out);
        } else {
            ui.centered_and_justified(|ui| {
                ui.weak("Open a disk image or container to browse it here.");
            });
        }

        self.render_edits_popup(ui.ctx(), pane, out);
        self.render_apply_review(ui.ctx(), pane, out);
        self.render_apply_report(ui.ctx(), pane, out);
    }

    /// Unlock prompt for an encrypted source; its copy differs for a locked APFS FileVault volume.
    fn unlock_prompt(
        &mut self,
        ui: &mut egui::Ui,
        pane: &BrowserPaneController,
        out: &mut Vec<CommanderAction>,
    ) {
        let is_apfs = pane.volume().fs_type == "APFS";
        let mut submit = false;
        ui.add_space(20.0);
        ui.vertical_centered(|ui| {
            if is_apfs {
                ui.label("This APFS volume is encrypted with FileVault.");
                ui.add_space(4.0);
                ui.label("Enter the volume password or personal recovery key.");
            } else {
                ui.label("This source is password-protected.");
            }
            ui.add_space(8.0);
            ui.horizontal(|ui| {
                ui.label(if is_apfs { "Passphrase:" } else { "Password:" });
                let resp = ui.add(
                    egui::TextEdit::singleline(&mut self.password_input)
                        .password(true)
                        .desired_width(220.0),
                );
                let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                if ui.button("Unlock").clicked() || enter {
                    submit = true;
                }
            });
        });
        if submit {
            let secret = std::mem::take(&mut self.password_input);
            self.pane_action(out, P::SubmitPassword(secret));
        }
    }

    // --- source bar --------------------------------------------------------

    fn source_bar(
        &self,
        ui: &mut egui::Ui,
        pane: &BrowserPaneController,
        out: &mut Vec<CommanderAction>,
    ) {
        let mut source_action = None;

        // Row 1: source selection + pane lifecycle (Close / Apply / Discard).
        // Wrapped so the picker + lifecycle buttons flow onto a second line on a
        // narrow pane instead of overflowing past the pane edge.
        ui.horizontal_wrapped(|ui| {
            // Shared source picker (R1): the same ComboBox widget the Inspect
            // tab uses, here offering an image file or a host folder. Image
            // opens are not materialized (BrowseSession peels the container).
            let is_backup = pane.is_backup();
            let source = pane.source_path().map(|p| p.to_path_buf());
            let current_label = if let Some(label) = pane.remote_label() {
                label
            } else if pane.listing.is_host() {
                "Local folder".to_string()
            } else if is_backup {
                match &source {
                    Some(p) => format!(
                        "Backup: {}",
                        p.file_name().unwrap_or_default().to_string_lossy()
                    ),
                    None => "Backup folder".to_string(),
                }
            } else if let Some(p) = &source {
                let base = p.file_name().unwrap_or_default().to_string_lossy();
                if pane.is_archive() {
                    format!("Archive: {base}")
                } else {
                    format!("Image: {base}")
                }
            } else {
                "Open image, backup, or folder...".to_string()
            };
            let cfg = super::super::source_picker::PickerConfig {
                show_devices: false,
                show_image: true,
                show_host_folder: true,
                show_backup_folder: true,
                // "Connect to Remote..." lives in this dropdown now (no
                // separate button).
                show_remote: true,
                materialize_image: false,
                // Surface .sit/.hqx/... in the "Open File..." dialog; a Mac
                // archive opens read-only in-pane (ArchiveFilesystem).
                include_mac_archives: true,
                width: 200.0,
                // Variable-length recent group: the merged list is already short
                // (newest 3 per mode) and rarely changes mid-session.
                recent_slots: 0,
                run_dialogs: false,
            };
            let state = super::super::source_picker::PickerState {
                selected_device_idx: None,
                image_active: source.is_some() && !pane.listing.is_host() && !is_backup,
                host_active: pane.listing.is_host(),
                backup_active: is_backup,
                devices: &[],
                // Merged recent list (newest 3 per mode) shown as a group inside
                // this same dropdown — no extra combo crowding the source bar.
                recent: pane.recent(),
            };
            let id = format!("commander_source_{}", self.side.idx());
            if let Some(ev) =
                super::super::source_picker::show(ui, &id, &cfg, &current_label, &state)
            {
                use super::super::source_picker::SourceEvent;
                match ev {
                    // A recent pick opens like a drag-and-drop: a folder as a host folder, a file as an image.
                    SourceEvent::Recent(path) => source_action = Some(P::OpenPath(path)),
                    SourceEvent::Image { path, .. } => source_action = Some(P::OpenImage(path)),
                    SourceEvent::HostFolder(dir) => source_action = Some(P::OpenHostFolder(dir)),
                    SourceEvent::BackupFolder(dir) => {
                        source_action = Some(P::OpenBackupFolder(dir))
                    }
                    SourceEvent::PickImage => source_action = Some(P::PickImage),
                    SourceEvent::PickHostFolder => source_action = Some(P::PickHostFolder),
                    SourceEvent::PickBackupFolder => source_action = Some(P::PickBackupFolder),
                    SourceEvent::Device(_) => {}
                    SourceEvent::Remote => source_action = Some(P::Connect),
                }
            }

            // When inside a remote image, "Close Image" steps back to the host
            // browser (the folder it was opened from) — distinct from "Close",
            // which unloads the whole connection.
            if pane.is_remote_image() {
                let btn = egui::Button::new(
                    egui::RichText::new("Close Image").color(close_image_color(ui.visuals())),
                );
                if ui
                    .add_enabled(pane.queue.is_empty(), btn)
                    .on_hover_text("Back to browsing the remote host filesystem")
                    .clicked()
                {
                    source_action = Some(P::CloseRemoteImage);
                }
            }

            // Close (unload) this pane — guarded by unsaved staged edits.
            if (source.is_some() || pane.listing.is_loaded())
                && ui
                    .button("Close")
                    .on_hover_text("Unload this pane / disconnect")
                    .clicked()
            {
                source_action = Some(P::CloseSource);
            }

            // Per-pane staging controls (writable image panes only; host writes
            // are immediate and never staged, and backups / archives are
            // read-only).
            if pane.stages_edits() {
                let n = pane.queue.len();
                let busy = pane.is_busy();
                ui.add_enabled_ui(n > 0 && !busy, |ui| {
                    if ui.button(format!("Apply ({n})")).clicked() {
                        source_action = Some(P::Apply);
                    }
                });
                ui.add_enabled_ui(n > 0 && !busy, |ui| {
                    if ui.button("Discard").clicked() {
                        source_action = Some(P::Discard);
                    }
                });
                if n > 0
                    && ui
                        .button(format!("Edits ({n})"))
                        .on_hover_text("Review the pending staged edits")
                        .clicked()
                {
                    source_action = Some(P::ShowEdits(!pane.edits_shown()));
                }
            }
        });

        // Row 2: partition switcher + New Folder + volume readout. Wrapped so on
        // a narrow pane the volume readout flows onto a second line instead of
        // overlapping the New Folder button.
        if pane.listing.is_loaded() {
            ui.horizontal_wrapped(|ui| {
                // Filesystem / partition dropdown (image panes only; host
                // folders have none). An optical disc lists its filesystems
                // (ISO 9660 + any hybrid Mac side); every other image lists its
                // partition-table entries.
                if !pane.listing.is_host() {
                    let labels = pane.partition_labels();
                    let empty_text = if pane.is_optical() {
                        "(no filesystems)"
                    } else {
                        "(no partitions)"
                    };
                    let selected = pane.selected_partition();
                    let current = selected
                        .and_then(|i| labels.get(i).cloned())
                        .unwrap_or_else(|| empty_text.to_string());
                    let mut chosen = selected;
                    // Capped so a long label (a raw-carve "Amiga NDOS (no filesystem)") can't push the readout.
                    egui::ComboBox::from_id_salt(("commander_part", self.side.idx()))
                        .selected_text(current)
                        .width(210.0)
                        .show_ui(ui, |ui| {
                            for (i, label) in labels.iter().enumerate() {
                                ui.selectable_value(&mut chosen, Some(i), label);
                            }
                        });
                    if chosen != selected {
                        if let Some(i) = chosen {
                            source_action = Some(P::SelectPartition(i));
                        }
                    }
                }

                // New Folder: stages a CreateDirectory on an image pane;
                // creates it immediately on a host pane. Not offered on a
                // read-only backup pane.
                if pane.can_new_folder()
                    && ui
                        .button("New Folder")
                        .on_hover_text("Create a folder in the current directory")
                        .clicked()
                {
                    source_action = Some(P::RequestNewFolder);
                }

                // Volume label + free space. Rendered inline (not right-aligned)
                // so on a narrow pane it wraps to a second line rather than
                // overflowing left into the New Folder button.
                ui.separator();
                if pane.listing.is_host() {
                    ui.strong("local folder");
                } else {
                    let vol = pane.volume();
                    let label = if vol.label.is_empty() {
                        vol.fs_type.clone()
                    } else {
                        format!("{} ({})", vol.label, vol.fs_type)
                    };
                    ui.strong(label);
                    ui.separator();
                    let free = vol.total_size.saturating_sub(vol.used_size);
                    ui.label(format!("free: {}", format_size(free)));
                    // Read-only media (archive / backup / remote host): flag it so
                    // the greyed copy-into button reads as "by design", and edits
                    // aren't attempted.
                    if pane.is_read_only() {
                        ui.separator();
                        ui.colored_label(read_only_badge_color(ui.visuals()), "Read-only")
                            .on_hover_text(
                                "This source can be browsed and copied out of, but not \
                                 written to - copies into this pane are disabled.",
                            );
                    }
                }
            });
        }
        if let Some(action) = source_action {
            self.pane_action(out, action);
        }
    }

    /// The "Pending edits" list: every staged edit in plain words, in apply order, each removable.
    fn render_edits_popup(
        &self,
        ctx: &egui::Context,
        pane: &BrowserPaneController,
        out: &mut Vec<CommanderAction>,
    ) {
        if !pane.edits_shown() {
            return;
        }
        let mut open = true;
        let busy = pane.is_busy();
        let lines = pane.edit_lines();
        egui::Window::new(format!("Pending edits ({})", self.side.label()))
            .open(&mut open)
            .resizable(true)
            .default_width(440.0)
            .show(ctx, |ui| {
                ui.label("Staged for this pane (applied in order on Apply):");
                ui.add_space(4.0);
                egui::ScrollArea::vertical()
                    .auto_shrink([false, true])
                    .max_height(320.0)
                    .show(ui, |ui| {
                        for (i, line) in lines.iter().enumerate() {
                            ui.horizontal(|ui| {
                                let remove = egui::Button::new("X").small();
                                if ui
                                    .add_enabled(!busy, remove)
                                    .on_hover_text("Remove this edit from the queue")
                                    .clicked()
                                {
                                    self.pane_action(out, P::Unstage(i));
                                }
                                ui.label(format!("{}. {line}", i + 1));
                            });
                        }
                    });
            });
        if !open {
            self.pane_action(out, P::ShowEdits(false));
        }
    }

    /// The partition tag and the cwd as crumbs; clicking a crumb navigates there.
    fn path_line(
        &self,
        ui: &mut egui::Ui,
        pane: &BrowserPaneController,
        out: &mut Vec<CommanderAction>,
    ) {
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 0.0;
            let prefix = if pane.listing.is_host() {
                "host ".to_string()
            } else {
                match pane.selected_partition() {
                    Some(i) => format!("@{} ", i + 1),
                    None => "- ".to_string(),
                }
            };
            ui.monospace(prefix);
            let crumbs = pane.path_crumbs();
            if crumbs.is_empty() {
                ui.monospace("/");
            }
            let last = crumbs.len().saturating_sub(1);
            for (i, (label, target)) in crumbs.into_iter().enumerate() {
                let text = egui::RichText::new(label).monospace();
                match target {
                    Some(t) if i < last => {
                        if ui.link(text).clicked() {
                            self.pane_action(out, P::NavigateTo(t));
                        }
                    }
                    _ => {
                        ui.label(text);
                    }
                }
            }
        });
    }

    // --- apply: the review and report are views over controller state ------

    fn render_apply_review(
        &mut self,
        ctx: &egui::Context,
        pane: &BrowserPaneController,
        out: &mut Vec<CommanderAction>,
    ) {
        let Some(p) = pane.apply_review() else {
            self.review_choice = OnConflict::Skip;
            return;
        };
        let danger = super::super::theme::danger_muted(&ctx.global_style().visuals);
        let choice = &mut self.review_choice;
        let mut go = None;
        egui::Window::new(format!("Before applying ({})", self.side.label()))
            .collapsible(false)
            .resizable(true)
            .default_width(480.0)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                if p.shortfall() > 0 {
                    ui.colored_label(
                        danger,
                        format!(
                            "Not enough space: needs about {}, {} free on {}.",
                            format_size(p.bytes_needed),
                            format_size(p.bytes_free),
                            p.fs_type
                        ),
                    );
                    ui.add_space(4.0);
                }
                if !p.scan.conflicts.is_empty() {
                    ui.label(format!(
                        "{} item(s) already exist on {}:",
                        p.scan.conflicts.len(),
                        p.fs_type
                    ));
                    path_list(ui, "review_conflicts", p.scan.conflicts.iter().map(|c| &c.0));
                    ui.horizontal(|ui| {
                        ui.radio_value(choice, OnConflict::Skip, "Skip existing");
                        ui.radio_value(choice, OnConflict::Replace, "Overwrite");
                    });
                    ui.add_space(4.0);
                }
                if !p.scan.blocked_folders.is_empty() {
                    ui.label(format!(
                        "{} folder(s) can't be created because a file has the same name; \
                         their contents will fail:",
                        p.scan.blocked_folders.len()
                    ));
                    path_list(ui, "review_blocked", p.scan.blocked_folders.iter());
                    ui.add_space(4.0);
                }
                if !p.scan.missing_folders.is_empty() {
                    ui.label(format!(
                        "{} destination folder(s) no longer exist; items going into them will fail:",
                        p.scan.missing_folders.len()
                    ));
                    path_list(ui, "review_missing", p.scan.missing_folders.iter());
                    ui.add_space(4.0);
                }
                ui.label("Items that fail are listed at the end; everything else is still written.");
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    let label = if p.shortfall() > 0 { "Apply anyway" } else { "Apply" };
                    if ui.button(label).clicked() {
                        go = Some(true);
                    }
                    if ui.button("Cancel").clicked() {
                        go = Some(false);
                    }
                });
            });
        match go {
            Some(true) => self.pane_action(out, P::ApplyReviewed(self.review_choice)),
            Some(false) => self.pane_action(out, P::DismissReview),
            None => {}
        }
    }

    fn render_apply_report(
        &self,
        ctx: &egui::Context,
        pane: &BrowserPaneController,
        out: &mut Vec<CommanderAction>,
    ) {
        let Some(report) = pane.apply_report() else {
            return;
        };
        let danger = super::super::theme::danger_muted(&ctx.global_style().visuals);
        let still = pane.queue.len();
        let mut then = None;
        let mut dismiss = false;
        egui::Window::new(format!("Apply report ({})", self.side.label()))
            .collapsible(false)
            .resizable(true)
            .default_width(560.0)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                if let Some(fatal) = &report.fatal {
                    ui.colored_label(danger, fatal);
                    ui.add_space(4.0);
                }
                ui.label(format!(
                    "Applied {} of {} edit(s). {still} still staged.",
                    report.applied, report.total
                ));
                if !report.failed.is_empty() {
                    ui.add_space(4.0);
                    ui.label(format!("{} item(s) failed:", report.failed.len()));
                    egui::ScrollArea::vertical()
                        .id_salt("apply_report_failed")
                        .max_height(260.0)
                        .show(ui, |ui| {
                            for f in report.failed.iter().take(500) {
                                ui.monospace(format!("{}: {}", f.label, f.error));
                            }
                            if report.failed.len() > 500 {
                                ui.label(format!(
                                    "... and {} more (see the log)",
                                    report.failed.len() - 500
                                ));
                            }
                        });
                }
                if report.not_tried > 0 {
                    ui.add_space(4.0);
                    ui.label(format!(
                        "Stopped after {} failures in a row; {} edit(s) were not tried.",
                        MAX_CONSECUTIVE_FAILURES, report.not_tried
                    ));
                }
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if still > 0 && ui.button("Retry").clicked() {
                        dismiss = true;
                        then = Some(P::Apply);
                    }
                    if still > 0
                        && ui
                            .button("Discard remaining")
                            .on_hover_text("Drop the edits that did not apply")
                            .clicked()
                    {
                        dismiss = true;
                        then = Some(P::Discard);
                    }
                    if ui.button("Close").clicked() {
                        dismiss = true;
                    }
                });
            });
        if dismiss {
            self.pane_action(out, P::DismissReport);
        }
        if let Some(action) = then {
            self.pane_action(out, action);
        }
    }

    // --- listing grid ------------------------------------------------------

    fn render_header(
        &self,
        ui: &mut egui::Ui,
        pane: &BrowserPaneController,
        out: &mut Vec<CommanderAction>,
    ) {
        let (rect, resp) = ui.allocate_exact_size(
            egui::vec2(ui.available_width(), ROW_H),
            egui::Sense::click(),
        );
        let c = cols(rect);
        let mid = rect.center().y;
        let font = egui::FontId::proportional(13.0);
        let color = ui.visuals().strong_text_color();
        let active = pane.listing.sort_column();
        let desc = pane.listing.is_descending();
        let caret = |col: SortColumn| -> &'static str {
            if col != active {
                ""
            } else if desc {
                " v"
            } else {
                " ^"
            }
        };
        let pt = ui.painter();
        pt.text(
            egui::pos2(c.name_l, mid),
            egui::Align2::LEFT_CENTER,
            format!("Name{}", caret(SortColumn::Name)),
            font.clone(),
            color,
        );
        pt.text(
            egui::pos2(c.size_r, mid),
            egui::Align2::RIGHT_CENTER,
            format!("Data{}", caret(SortColumn::Size)),
            font.clone(),
            color,
        );
        pt.text(
            egui::pos2(c.rsrc_r, mid),
            egui::Align2::RIGHT_CENTER,
            "Rsrc",
            font.clone(),
            color,
        );
        pt.text(
            egui::pos2(c.mod_l, mid),
            egui::Align2::LEFT_CENTER,
            format!("Modified{}", caret(SortColumn::Modified)),
            font.clone(),
            color,
        );
        pt.text(
            egui::pos2(c.type_l, mid),
            egui::Align2::LEFT_CENTER,
            format!("Type{}", caret(SortColumn::Type)),
            font,
            color,
        );
        pt.line_segment(
            [
                egui::pos2(rect.left(), rect.bottom()),
                egui::pos2(rect.right(), rect.bottom()),
            ],
            egui::Stroke::new(1.0_f32, ui.visuals().window_stroke.color),
        );

        if resp.clicked() {
            if let Some(pos) = resp.interact_pointer_pos() {
                let clicked = if pos.x < c.size_l {
                    SortColumn::Name
                } else if pos.x < c.mod_l {
                    SortColumn::Size
                } else if pos.x < c.type_l {
                    SortColumn::Modified
                } else {
                    SortColumn::Type
                };
                self.pane_action(out, P::SortBy(clicked));
            }
        }
    }

    fn render_rows(
        &mut self,
        ui: &mut egui::Ui,
        ctl: &CommanderController,
        active: bool,
        scroll: bool,
        out: &mut Vec<CommanderAction>,
    ) {
        let side = self.side;
        let pane = ctl.pane(side);
        let rows = pane.rows();
        let cursor = if active { pane.cursor_index() } else { None };
        let page_rows = ((ui.available_height() / ROW_H) as i32).max(1);
        if page_rows != self.page_rows {
            self.page_rows = page_rows;
            out.push(A::SetPageRows(side, page_rows));
        }
        let busy = pane.applying().is_some();
        // Host panes write immediately (Delete removes now); image panes stage
        // (Delete / Undelete / Remove-from-staging go on the queue).
        let host_pane = pane.listing.is_host();
        // A read-only backup pane offers browse / copy-out actions only — no
        // delete or rename (both would mutate the backup).
        let read_only = pane.is_backup();
        // On a remote *host* pane, a file can be opened as a disk image.
        let remote_host = pane.is_remote_host();

        // Row clicks this frame, run through the controller in order once drawing is done.
        let mut acts: Vec<BrowserPaneAction> = Vec::new();
        let mut after: Vec<CommanderAction> = Vec::new();

        egui::ScrollArea::vertical()
            .id_salt(("commander_rows", side.idx()))
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let mods = ui.input(|i| i.modifiers);
                for (i, row) in rows.iter().enumerate() {
                    let (rect, resp) = ui.allocate_exact_size(
                        egui::vec2(ui.available_width(), ROW_H),
                        egui::Sense::click(),
                    );
                    let selected = if row.is_parent() {
                        false
                    } else if row.is_tree {
                        pane.tree_selected.contains(&row.node_id)
                    } else {
                        pane.listing.is_selected(&row.name)
                    };
                    if selected {
                        ui.painter().rect_filled(
                            rect,
                            egui::CornerRadius::ZERO,
                            ui.visuals().selection.bg_fill,
                        );
                    } else if resp.hovered() {
                        ui.painter().rect_filled(
                            rect,
                            egui::CornerRadius::ZERO,
                            ui.visuals().widgets.hovered.bg_fill,
                        );
                    }
                    if cursor == Some(i) {
                        ui.painter().rect_stroke(
                            rect,
                            egui::CornerRadius::ZERO,
                            egui::Stroke::new(1.0_f32, ui.visuals().selection.stroke.color),
                            egui::StrokeKind::Inside,
                        );
                        if scroll {
                            ui.scroll_to_rect(rect, None);
                        }
                    }
                    paint_row(ui, rect, row);

                    // The `+`/`-` disclosure occupies a fixed slot at the row's
                    // indent; a click there toggles expansion instead of
                    // selecting / navigating.
                    let toggle_x0 = cols(rect).name_l + (row.depth as f32) * TREE_INDENT;
                    let hit_toggle = |resp: &egui::Response| -> bool {
                        row.expandable
                            && resp
                                .interact_pointer_pos()
                                .is_some_and(|p| p.x >= toggle_x0 && p.x < toggle_x0 + TOGGLE_W)
                    };
                    if resp.double_clicked() {
                        acts.push(if hit_toggle(&resp) {
                            P::ToggleWrapper(row.node_id.clone())
                        } else {
                            P::Activate(row.node_id.clone())
                        });
                    } else if resp.clicked() {
                        if hit_toggle(&resp) {
                            acts.push(P::ToggleWrapper(row.node_id.clone()));
                        } else if row.is_tree {
                            acts.push(P::TreeClick {
                                node_id: row.node_id.clone(),
                                ctrl: mods.command,
                                shift: mods.shift,
                            });
                        } else if !row.is_parent() {
                            acts.push(P::Click {
                                name: row.name.clone(),
                                ctrl: mods.command,
                                shift: mods.shift,
                            });
                        }
                    }

                    // Right-click a data row for its actions.
                    if !row.is_parent() && !busy {
                        let menu = resp.context_menu(|ui| {
                            row_menu(ui, row, side, remote_host, host_pane, read_only, &mut after)
                        });
                        if menu.is_some() {
                            acts.push(if row.is_tree {
                                P::TreeContextSelect(row.node_id.clone())
                            } else {
                                P::ContextSelect(row.name.clone())
                            });
                        }
                    }
                }

                // Click empty space to clear the selection.
                let remaining = ui.available_size();
                if remaining.y > 4.0 {
                    let (_r, bgr) = ui.allocate_exact_size(remaining, egui::Sense::click());
                    if bgr.clicked() {
                        acts.push(P::ClearSelection);
                    }
                }
            });

        // Any row interaction makes this the active pane, before what the clicks asked for.
        if !acts.is_empty() {
            out.push(A::Focus(side));
        }
        out.extend(acts.into_iter().map(|a| A::Pane(side, a)));
        out.extend(after);
    }
}

/// A data row's right-click menu; its choices land in `out` after the row's own select.
fn row_menu(
    ui: &mut egui::Ui,
    row: &DisplayRow,
    side: Side,
    remote_host: bool,
    host_pane: bool,
    read_only: bool,
    out: &mut Vec<CommanderAction>,
) {
    // A not-yet-applied staged add has no data to copy, export, hash or describe.
    let real = !matches!(row.kind, RowKind::PendingAdd);
    // On a remote host pane, a file opens as an image (browse inside it).
    if remote_host && !row.is_dir && !row.is_tree && ui.button("Open Image").clicked() {
        out.push(A::Pane(side, P::OpenRemoteImage(row.name.clone())));
        ui.close();
    }
    if real && ui.button("Copy to other pane").clicked() {
        out.push(A::Copy(side));
        ui.close();
    }
    if real {
        if ui.button("Export to hard drive...").clicked() {
            out.push(A::Export {
                from: side,
                format: None,
            });
            ui.close();
        }
        ui.menu_button("Export as", |ui| {
            for fmt in ExportFormat::ALL {
                if ui.button(fmt.label()).clicked() {
                    out.push(A::Export {
                        from: side,
                        format: Some(fmt),
                    });
                    ui.close();
                }
            }
        });
    }
    if real && ui.button("Calculate checksums...").clicked() {
        out.push(A::Checksums(side));
        ui.close();
    }
    // File Info works for wrapper-tree rows too, read through the mounted wrapper.
    if real && ui.button("File Info / Details...").clicked() {
        out.push(A::OpenDetail(side, row.name.clone()));
        ui.close();
    }
    // A wrapper's contents are read-only (copy out, don't edit), and so is a backup.
    if row.is_tree || read_only {
        return;
    }
    if matches!(row.kind, RowKind::PendingRename) && ui.button("Cancel rename").clicked() {
        out.push(A::Pane(side, P::CancelRename(row.name.clone())));
        ui.close();
    } else if matches!(row.kind, RowKind::Normal) && ui.button("Rename...").clicked() {
        out.push(A::Pane(side, P::RequestRename(row.name.clone())));
        ui.close();
    }
    let label = match row.kind {
        _ if host_pane => "Delete (immediate)",
        RowKind::PendingDelete => "Undelete",
        RowKind::PendingAdd => "Remove from staging",
        _ => "Delete",
    };
    if ui.button(label).clicked() {
        out.push(A::Pane(side, P::Delete));
        ui.close();
    }
}

// --- column geometry + painting (adapted from the layout mock) -------------

struct Cols {
    name_l: f32,
    name_r: f32,
    size_l: f32,
    size_r: f32,
    /// Right edge of the resource-fork size column (right-aligned values).
    rsrc_r: f32,
    mod_l: f32,
    type_l: f32,
}

fn cols(rect: egui::Rect) -> Cols {
    let pad = 6.0;
    let gap = 8.0;
    // Trimmed fixed columns so the Name column isn't starved at the default
    // pane width (~500 px): footprint here is ~334 px vs the old ~392 px.
    let type_w = 50.0;
    let mod_w = 112.0;
    let size_w = 66.0;
    let rsrc_w = 66.0;
    let name_l = rect.left() + pad;
    let name_w = (rect.width() - type_w - mod_w - size_w - rsrc_w - 5.0 * gap).max(90.0);
    let name_r = name_l + name_w;
    let size_l = name_r + gap;
    let size_r = size_l + size_w;
    let rsrc_r = size_r + gap + rsrc_w;
    let mod_l = rsrc_r + gap;
    let type_l = mod_l + mod_w + gap;
    Cols {
        name_l,
        name_r,
        size_l,
        size_r,
        rsrc_r,
        mod_l,
        type_l,
    }
}

fn paint_row(ui: &egui::Ui, rect: egui::Rect, row: &DisplayRow) {
    let c = cols(rect);
    let mid = rect.center().y;
    let font = egui::FontId::proportional(13.0);
    let base = ui.visuals().text_color();
    let color = match row.kind {
        RowKind::Parent => ui.visuals().weak_text_color(),
        RowKind::PendingAdd => add_color(ui.visuals()),
        RowKind::PendingRename => add_color(ui.visuals()),
        RowKind::PendingDelete => del_color(ui.visuals()),
        RowKind::Normal if row.meta_changed => meta_color(ui.visuals()),
        RowKind::Normal if row.is_image => image_color(ui.visuals()),
        RowKind::Normal if row.is_dir => super::super::theme::info(ui.visuals()),
        RowKind::Normal => base,
    };

    // ASCII overlay markers (no Unicode glyphs): "+ " pending add, "- " pending
    // delete, "old -> new" pending rename, trailing "/" for directories, and a
    // leading "* " for an entry with staged metadata edits (in addition to the
    // blue tint, since blue alone reads too close to the folder color).
    let display_name = match row.kind {
        RowKind::PendingAdd => format!("+ {}", row.name),
        RowKind::PendingDelete => format!("- {}", row.name),
        RowKind::PendingRename => match &row.rename_to {
            Some(new) => format!("{} -> {}", row.name, new),
            None => row.name.clone(),
        },
        _ => {
            let base = if row.is_dir && !row.is_parent() {
                format!("{}/", row.name)
            } else {
                row.name.clone()
            };
            if row.meta_changed {
                format!("* {base}")
            } else {
                base
            }
        }
    };

    let name_cell = egui::Rect::from_min_max(
        egui::pos2(c.name_l, rect.top()),
        egui::pos2(c.name_r, rect.bottom()),
    );
    // Indent by tree depth and reserve a fixed toggle slot so names align
    // whether or not a row has a `+`/`-`. Draw the disclosure marker (ASCII —
    // no Unicode glyphs) for expandable rows.
    let indent_x = c.name_l + (row.depth as f32) * TREE_INDENT;
    let name_x = indent_x + TOGGLE_W;
    let painter = ui.painter_at(name_cell);
    if row.expandable {
        painter.text(
            egui::pos2(indent_x, mid),
            egui::Align2::LEFT_CENTER,
            if row.expanded { "-" } else { "+" },
            font.clone(),
            ui.visuals().weak_text_color(),
        );
    }
    painter.text(
        egui::pos2(name_x, mid),
        egui::Align2::LEFT_CENTER,
        display_name,
        font.clone(),
        color,
    );

    if !row.is_dir {
        ui.painter().text(
            egui::pos2(c.size_r, mid),
            egui::Align2::RIGHT_CENTER,
            format_size(row.size),
            font.clone(),
            color,
        );
        // Resource-fork size, only when the file actually has one (Mac
        // filesystems / archives); blank elsewhere so the column stays quiet.
        if row.rsrc_size > 0 {
            ui.painter().text(
                egui::pos2(c.rsrc_r, mid),
                egui::Align2::RIGHT_CENTER,
                format_size(row.rsrc_size),
                font.clone(),
                ui.visuals().weak_text_color(),
            );
        }
    }
    ui.painter().text(
        egui::pos2(c.mod_l, mid),
        egui::Align2::LEFT_CENTER,
        row.modified.clone(),
        font.clone(),
        ui.visuals().weak_text_color(),
    );
    ui.painter().text(
        egui::pos2(c.type_l, mid),
        egui::Align2::LEFT_CENTER,
        row.type_tag.clone(),
        font,
        ui.visuals().weak_text_color(),
    );

    // Strike through a pending delete's name.
    if row.kind == RowKind::PendingDelete {
        ui.painter().line_segment(
            [egui::pos2(c.name_l, mid), egui::pos2(c.name_r, mid)],
            egui::Stroke::new(1.0_f32, color),
        );
    }
}

/// A capped, scrollable list of destination paths for the apply review.
fn path_list<'a>(ui: &mut egui::Ui, id: &str, paths: impl Iterator<Item = &'a String>) {
    egui::ScrollArea::vertical()
        .id_salt(id)
        .max_height(120.0)
        .show(ui, |ui| {
            for (i, p) in paths.enumerate() {
                if i == 200 {
                    ui.label("...");
                    break;
                }
                ui.monospace(p);
            }
        });
}
