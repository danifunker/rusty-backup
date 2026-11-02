//! Commander Mode -- full-page two-pane file explorer overlay.
//!
//! This is wired into [`crate::gui::RustyBackupApp`] as `Option<CommanderMode>`:
//! `Some` means the overlay is open and takes over the whole frame (the tab
//! strip is not drawn). Everything Commander does lives in
//! [`CommanderController`] (both panes, copy, export, checksums, File Info data,
//! the session log, the key map); this file and `pane.rs` draw it, turn clicks and
//! keys into its actions, and carry out its effects through one [`EffectHost`],
//! whose answers go back as `Picked` / `Confirmed` / `Answered`. The metadata
//! editor widgets stay here and send their changes back as `StageMetadata`.
//!
//! NOTE: this crate uses a patched eframe whose panels are
//! `egui::Panel::*::show_inside` rather than the stock `TopBottomPanel`. The
//! main app's `ui()` method hands us its `&mut egui::Ui`, so we build the
//! overlay with `show_inside` against it, exactly like the rest of the GUI.

use std::path::PathBuf;
use std::sync::Arc;

use eframe::egui;

use rusty_backup::controller::app::{CommanderRecent, UserConfig};
use rusty_backup::controller::commander::{
    CommanderAction, CommanderController, ExportFormat, FileDetail, HashAlgo, MetadataEdit,
    OnConflict, PaneServices, ResourceForkMode, Side,
};
use rusty_backup::controller::keys::{Key, KeyMap, Mods};
use rusty_backup::controller::Controller;
use rusty_backup::model::edit_queue::EditQueue;

use super::effects::{EffectHost, Reply};
use super::file_detail::{self, FileContent};
use super::metadata_editor::{
    self, ExtPermsEditorState, HfsDatesEditorState, HfsTypeEditorState, MetadataSink,
    OwnerEditorState, ProdosTypeEditorState, XattrEditorState,
};

mod pane;

use pane::PaneView;

type A = CommanderAction;

/// The File Info window's widget state for one entry; rebuilt when the controller's serial changes.
#[derive(Default)]
struct DetailEditors {
    serial: u64,
    content: Option<FileContent>,
    hfs: Option<HfsTypeEditorState>,
    prodos: Option<ProdosTypeEditorState>,
    dates: Option<HfsDatesEditorState>,
    perms: Option<ExtPermsEditorState>,
    owner: Option<OwnerEditorState>,
    xattr: Option<XattrEditorState>,
}

/// The editors read the pane's queue and hand their changes back for the controller to stage.
struct ToController<'a> {
    queue: &'a EditQueue,
    staged: &'a mut Vec<MetadataEdit>,
}

impl MetadataSink for ToController<'_> {
    fn queue(&self) -> &EditQueue {
        self.queue
    }

    fn stage(&mut self, edit: MetadataEdit) -> Option<String> {
        self.staged.push(edit);
        None
    }
}

pub struct CommanderMode {
    ctl: CommanderController,
    /// Pickers, confirmations and prompts the controller asks for, answered back to it.
    effects: EffectHost,
    left: PaneView,
    right: PaneView,
    detail: DetailEditors,
    /// A key moved the cursor, so the active listing scrolls it into view.
    scroll_cursor: bool,
    close: bool,
}

impl CommanderMode {
    /// Both panes' jobs register in `jobs`, the app's registry.
    pub fn new(jobs: rusty_backup::model::job::JobRegistry) -> Self {
        let services = || PaneServices {
            jobs: jobs.clone(),
            notify: Arc::new(super::ui_logger::EguiWake),
            recent: Box::new(CommanderRecent(UserConfig)),
        };
        Self {
            ctl: CommanderController::new(services(), services(), load_keymap()),
            effects: EffectHost::default(),
            left: PaneView::new(Side::Left),
            right: PaneView::new(Side::Right),
            detail: DetailEditors::default(),
            scroll_cursor: false,
            close: false,
        }
    }

    /// Open a drag-and-dropped path in the active pane: a folder as a host folder, a file as an image.
    pub fn open_dropped_path(&mut self, ctx: &egui::Context, path: PathBuf) {
        self.dispatch(ctx, A::OpenDropped(path));
    }

    /// The Settings dialog cleared the persisted recent lists.
    pub(crate) fn clear_recent_files(&mut self) {
        self.ctl.apply(A::RecentCleared);
    }

    fn dispatch(&mut self, ctx: &egui::Context, action: CommanderAction) {
        let fx = self.ctl.apply(action);
        self.run_effects(ctx, fx);
    }

    fn run_effects(&mut self, ctx: &egui::Context, fx: Vec<rusty_backup::controller::Effect>) {
        let mut messages = Vec::new();
        let replies = self.effects.run(ctx, fx, &mut messages);
        for m in messages {
            log::info!("{m}");
        }
        for reply in replies {
            self.answer(ctx, reply);
        }
    }

    fn answer(&mut self, ctx: &egui::Context, reply: Reply) {
        let action = match reply {
            Reply::Picked { purpose, path } => A::Picked { purpose, path },
            Reply::Confirmed { purpose, yes } => A::Confirmed { purpose, yes },
            Reply::Answered { purpose, text } => A::Answered { purpose, text },
            Reply::CloseScreen => {
                self.close = true;
                return;
            }
        };
        self.dispatch(ctx, action);
    }

    /// Render the overlay into the app's root `Ui`. Returns `true` when the
    /// user asks to close it (the caller then drops the `CommanderMode`).
    pub fn show(&mut self, ui: &mut egui::Ui) -> bool {
        let ctx = ui.ctx().clone();
        let fx = self.ctl.tick();
        self.run_effects(&ctx, fx);
        self.handle_keys(&ctx);
        let mut acts: Vec<CommanderAction> = Vec::new();

        self.render_progress_modal(&ctx, &mut acts);
        self.render_host_conflict_modal(&ctx, &mut acts);
        self.render_rename_notice(&ctx, &mut acts);

        egui::Panel::top("commander_top").show(ui, |ui| {
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.heading("Commander Mode");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Close").clicked() {
                        acts.push(A::RequestClose);
                    }
                    if ui
                        .button(format!("Log ({})", self.ctl.log().len()))
                        .on_hover_text("Operations completed this Commander session")
                        .clicked()
                    {
                        acts.push(A::ShowLog(!self.ctl.log_shown()));
                    }
                });
            });
            ui.add_space(2.0);
        });

        egui::Panel::bottom("commander_bottom").show(ui, |ui| {
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.label("Status:");
                ui.label(self.ctl.status());
            });
            ui.add_space(2.0);
        });

        let active = self.ctl.active();
        let scroll = std::mem::take(&mut self.scroll_cursor);
        egui::CentralPanel::default().show(ui, |ui| {
            let full_h = ui.available_height();
            let full_w = ui.available_width();
            // Wide enough for the middle column's widest control (the "Export
            // forks as:" combo) without overflowing into the panes.
            let mid_w = 168.0;
            // Reserve room for the two separators + the item spacing between the
            // five horizontal items, so the right pane isn't clipped off-edge.
            let gaps = ui.spacing().item_spacing.x * 4.0 + 16.0;
            let pane_w = ((full_w - mid_w - gaps) / 2.0).max(220.0);
            ui.horizontal_top(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(pane_w, full_h),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        let mine = active == Side::Left;
                        self.left
                            .show(ui, &self.ctl, mine, mine && scroll, &mut acts);
                    },
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(mid_w, full_h),
                    egui::Layout::top_down(egui::Align::Center),
                    |ui| render_middle(ui, &self.ctl, &mut acts),
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(pane_w, full_h),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        let mine = active == Side::Right;
                        self.right
                            .show(ui, &self.ctl, mine, mine && scroll, &mut acts);
                    },
                );
            });
        });

        self.render_checksum_window(&ctx, &mut acts);
        self.render_detail_window(&ctx, &mut acts);
        self.render_log_window(&ctx, &mut acts);
        self.render_help(&ctx, &mut acts);

        let whose = self
            .effects
            .pending_purpose()
            .and_then(|p| self.ctl.asker(p))
            .map_or("", Side::label);
        if let Some(reply) = self.effects.show(&ctx, whose) {
            self.answer(&ctx, reply);
        }
        for action in acts {
            self.dispatch(&ctx, action);
        }
        std::mem::take(&mut self.close)
    }

    /// Hands this frame's bound key presses to the controller; the key map decides what they mean.
    fn handle_keys(&mut self, ctx: &egui::Context) {
        let modal = self.effects.modal_open() || self.ctl.modal_open();
        if modal || ctx.text_edit_focused() || egui::Popup::is_any_open(ctx) {
            return;
        }
        let ctl = &self.ctl;
        let keys: Vec<(Key, Mods)> = ctx.input_mut(|i| {
            let mut found = Vec::new();
            i.events.retain(|ev| {
                let egui::Event::Key {
                    key,
                    pressed: true,
                    modifiers,
                    ..
                } = ev
                else {
                    return true;
                };
                let mods = egui_mods(*modifiers);
                let bound = egui_key(*key).filter(|k| ctl.binds(*k, mods));
                found.extend(bound.map(|k| (k, mods)));
                bound.is_none()
            });
            found
        });
        if keys.is_empty() {
            return;
        }
        // egui already read Tab and the arrows as focus moves this frame; a shortcut cancels that.
        ctx.memory_mut(|m| {
            m.move_focus(egui::FocusDirection::None);
            m.stop_text_input();
        });
        self.scroll_cursor = true;
        for (key, mods) in keys {
            self.dispatch(ctx, A::Key(key, mods));
        }
    }

    /// The running copy or export, else a pane's apply, as a progress window.
    fn render_progress_modal(&self, ctx: &egui::Context, acts: &mut Vec<CommanderAction>) {
        if let Some((title, snap)) = self.ctl.copy_job() {
            egui::Window::new(title)
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    if super::progress::show_job(ui, &snap) {
                        acts.push(A::CancelCopy);
                    }
                });
            return;
        }
        for side in [Side::Left, Side::Right] {
            let Some(snap) = self.ctl.pane(side).applying() else {
                continue;
            };
            egui::Window::new(format!("Applying {} pane edits", side.label()))
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| super::progress::show_job(ui, &snap));
            return;
        }
    }

    fn render_rename_notice(&self, ctx: &egui::Context, acts: &mut Vec<CommanderAction>) {
        let Some(notice) = self.ctl.rename_notice() else {
            return;
        };
        let out = &notice.outcome;
        egui::Window::new(format!("Names changed for {}", out.fs_type))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                if !out.renamed.is_empty() {
                    ui.label(format!(
                        "{} item(s) have names {} can't store and were renamed:",
                        out.renamed.len(),
                        out.fs_type
                    ));
                    egui::ScrollArea::vertical()
                        .id_salt("rename_notice_renamed")
                        .max_height(180.0)
                        .show(ui, |ui| {
                            for c in out.renamed.iter().take(200) {
                                ui.monospace(format!("{} -> {}", printable(&c.from), c.to));
                            }
                            if out.renamed.len() > 200 {
                                ui.label(format!("... and {} more", out.renamed.len() - 200));
                            }
                        });
                }
                if !out.dropped.is_empty() {
                    ui.add_space(4.0);
                    ui.label(format!(
                        "{} item(s) have no usable name on {} and were left out:",
                        out.dropped.len(),
                        out.fs_type
                    ));
                    egui::ScrollArea::vertical()
                        .id_salt("rename_notice_dropped")
                        .max_height(120.0)
                        .show(ui, |ui| {
                            for (path, why) in out.dropped.iter().take(100) {
                                ui.monospace(format!("{} ({why})", printable(path)));
                            }
                        });
                }
                ui.add_space(6.0);
                ui.label(
                    "Tip: to keep every original name and all Mac metadata, export as \
                     Mac Archive (.mar) instead.",
                );
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("OK").clicked() {
                        acts.push(A::RenameNoticeOk);
                    }
                    if ui
                        .button("Undo copy")
                        .on_hover_text("Remove this copy from the staged edits")
                        .clicked()
                    {
                        acts.push(A::RenameNoticeUndo);
                    }
                });
            });
    }

    fn render_host_conflict_modal(&self, ctx: &egui::Context, acts: &mut Vec<CommanderAction>) {
        let Some(names) = self.ctl.host_conflict() else {
            return;
        };
        egui::Window::new("Files already exist")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label(format!(
                    "{} item(s) already exist in the destination folder:",
                    names.len()
                ));
                egui::ScrollArea::vertical()
                    .max_height(160.0)
                    .show(ui, |ui| {
                        for name in names.iter().take(50) {
                            ui.monospace(name);
                        }
                        if names.len() > 50 {
                            ui.label(format!("... and {} more", names.len() - 50));
                        }
                    });
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Overwrite").clicked() {
                        acts.push(A::HostConflict(OnConflict::Replace));
                    }
                    if ui.button("Skip existing").clicked() {
                        acts.push(A::HostConflict(OnConflict::Skip));
                    }
                    if ui.button("Cancel").clicked() {
                        acts.push(A::HostConflictCancel);
                    }
                });
            });
    }

    /// The checksum window: the hashing job while it runs, then a CRC32 / MD5 / SHA1 / SHA256 grid.
    fn render_checksum_window(&self, ctx: &egui::Context, acts: &mut Vec<CommanderAction>) {
        let Some(c) = self.ctl.checksums() else {
            return;
        };
        let mut open = true;
        egui::Window::new(c.title.as_str())
            .open(&mut open)
            .resizable(true)
            .default_width(560.0)
            .show(ctx, |ui| {
                if let Some(err) = &c.error {
                    ui.colored_label(super::theme::danger_muted(ui.visuals()), err);
                    return;
                }
                if let Some(snap) = c.running() {
                    super::progress::show_job(ui, &snap);
                    return;
                }
                if ui.small_button("Copy all").clicked() {
                    acts.push(A::CopyChecksums);
                }
                egui::ScrollArea::vertical()
                    .auto_shrink([false, true])
                    .show(ui, |ui| {
                        for (i, row) in c.rows.iter().enumerate() {
                            if i > 0 {
                                ui.add_space(6.0);
                                ui.separator();
                            }
                            ui.strong(format!(
                                "{}  ({})",
                                row.name,
                                disk_core::display::format_size(row.size)
                            ));
                            if let Some(err) = &row.error {
                                ui.colored_label(
                                    super::theme::danger_muted(ui.visuals()),
                                    format!("failed: {err}"),
                                );
                                continue;
                            }
                            let Some(values) = &row.values else { continue };
                            egui::Grid::new(("cksum_grid", i))
                                .num_columns(3)
                                .spacing([12.0, 4.0])
                                .show(ui, |ui| {
                                    for (algo, value) in HashAlgo::ALL.iter().zip(values) {
                                        ui.label(algo.label());
                                        ui.add(
                                            egui::Label::new(
                                                egui::RichText::new(value).monospace(),
                                            )
                                            .wrap(),
                                        );
                                        if ui.small_button("Copy").clicked() {
                                            acts.push(A::CopyChecksum {
                                                file: i,
                                                algo: *algo,
                                            });
                                        }
                                        ui.end_row();
                                    }
                                });
                        }
                    });
            });
        if !open {
            acts.push(A::CloseChecksums);
        }
    }

    /// The File Info window: metadata rows, the editors on a writable image pane, and a preview.
    fn render_detail_window(&mut self, ctx: &egui::Context, acts: &mut Vec<CommanderAction>) {
        let Some(d) = self.ctl.detail() else {
            return;
        };
        let ed = &mut self.detail;
        if ed.serial != d.serial {
            *ed = DetailEditors {
                serial: d.serial,
                content: d
                    .preview
                    .as_ref()
                    .map(|data| file_detail::detect_content_type(&d.entry, data)),
                ..DetailEditors::default()
            };
        }
        let mut staged = Vec::new();
        let mut open = true;
        let queue = &self.ctl.pane(d.side).queue;
        egui::Window::new(format!("File Info: {}", d.entry.name))
            .open(&mut open)
            .resizable(true)
            .default_width(620.0)
            .show(ctx, |ui| detail_body(ui, d, ed, queue, &mut staged));
        acts.extend(staged.into_iter().map(|e| A::StageMetadata(Box::new(e))));
        if !open {
            acts.push(A::CloseDetail);
        }
    }

    /// The session log: completed operations, newest at the bottom, with Copy all / Clear.
    fn render_log_window(&self, ctx: &egui::Context, acts: &mut Vec<CommanderAction>) {
        if !self.ctl.log_shown() {
            return;
        }
        let log = self.ctl.log();
        let mut open = true;
        egui::Window::new("Session log")
            .open(&mut open)
            .resizable(true)
            .default_width(560.0)
            .default_height(360.0)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label(format!("{} operation(s) this session.", log.len()));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.button("Clear").clicked() {
                            acts.push(A::ClearLog);
                        }
                        let copy = egui::Button::new("Copy all");
                        if ui.add_enabled(!log.is_empty(), copy).clicked() {
                            acts.push(A::CopyLog);
                        }
                    });
                });
                ui.separator();
                if log.is_empty() {
                    ui.weak(
                        "No operations yet. Applied edits, host copies / exports, and \
                         immediate host deletes / renames / new folders are recorded here.",
                    );
                    return;
                }
                egui::ScrollArea::vertical()
                    .id_salt("commander_session_log")
                    .auto_shrink([false, false])
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for line in log {
                            ui.monospace(line);
                        }
                    });
            });
        if !open {
            acts.push(A::ShowLog(false));
        }
    }

    /// Every shortcut with its first chord on this platform, from the controller's key map.
    fn render_help(&self, ctx: &egui::Context, acts: &mut Vec<CommanderAction>) {
        if !self.ctl.help_shown() {
            return;
        }
        let mut open = true;
        egui::Window::new("Keyboard shortcuts")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                egui::Grid::new("commander_keys")
                    .striped(true)
                    .show(ui, |ui| {
                        for (label, chord) in self.ctl.key_help() {
                            ui.label(label);
                            ui.monospace(chord);
                            ui.end_row();
                        }
                    });
                ui.add_space(4.0);
                ui.weak(self.ctl.key_help_footer());
            });
        if !open {
            acts.push(A::ShowHelp(false));
        }
    }
}

/// The File Info window's contents; editor changes land in `staged`.
fn detail_body(
    ui: &mut egui::Ui,
    d: &FileDetail,
    ed: &mut DetailEditors,
    queue: &EditQueue,
    staged: &mut Vec<MetadataEdit>,
) {
    let fs = d.fs_type.as_str();
    let is_hfs = matches!(fs, "HFS" | "HFS+" | "HFSX");
    let is_prodos = fs == "ProDOS";
    let edit = d.editable;
    let entry = &d.entry;
    // The controller reports staging results in `d.result`; the editors' own line is unused.
    let mut unused = None;
    let mut sink = ToController { queue, staged };
    file_detail::render_metadata_rows(ui, entry, is_hfs || is_prodos, None);
    if is_hfs && entry.is_file() {
        ui.separator();
        metadata_editor::render_hfs_type_row(ui, entry, edit, &mut sink, &mut ed.hfs, &mut unused);
    }
    if fs == "HFS" {
        metadata_editor::render_hfs_dates_row(
            ui,
            entry,
            edit,
            &mut sink,
            &mut ed.dates,
            &mut unused,
        );
    }
    if is_prodos && entry.is_file() {
        ui.separator();
        metadata_editor::render_prodos_type_row(
            ui,
            entry,
            edit,
            &mut sink,
            &mut ed.prodos,
            &mut unused,
        );
    }
    // Unix rows follow the entry carrying a POSIX mode, not the filesystem's name.
    if entry.mode.is_some() {
        ui.separator();
        metadata_editor::render_ext_permissions_row(
            ui,
            entry,
            edit,
            &mut sink,
            &mut ed.perms,
            &mut unused,
        );
        metadata_editor::render_owner_row(
            ui,
            entry,
            edit,
            None,
            &mut sink,
            &mut ed.owner,
            &mut unused,
        );
        metadata_editor::render_xattr_section(
            ui,
            entry,
            edit,
            &d.xattrs,
            &mut sink,
            &mut ed.xattr,
            &mut unused,
        );
    }
    if let Some(msg) = &d.result {
        ui.add_space(4.0);
        ui.colored_label(super::theme::success(ui.visuals()), msg);
    }
    ui.separator();
    match &ed.content {
        Some(FileContent::Text(text)) => {
            egui::ScrollArea::vertical()
                .id_salt("commander_detail_preview")
                .max_height(280.0)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.add(
                        egui::TextEdit::multiline(&mut text.as_str())
                            .desired_width(f32::INFINITY)
                            .font(egui::TextStyle::Monospace),
                    );
                });
        }
        Some(FileContent::Binary(data)) => {
            egui::ScrollArea::vertical()
                .id_salt("commander_detail_preview")
                .max_height(280.0)
                .auto_shrink([false, false])
                .show(ui, |ui| file_detail::render_hex_view(ui, data));
        }
        None => {}
    }
}

/// The middle column: copy either way, delete on the active pane, and the copy / export settings.
fn render_middle(ui: &mut egui::Ui, ctl: &CommanderController, acts: &mut Vec<CommanderAction>) {
    ui.add_space(60.0);
    let sz = egui::vec2(100.0, 48.0);
    // A greyed copy button says why in its hover, so it reads as "by design" rather than broken.
    let copy_hover = |into: Side| match ctl.pane(into).readonly_reason() {
        Some(reason) => {
            let side = if into == Side::Right { "right" } else { "left" };
            format!("Can't copy into the {side} pane - {reason}")
        }
        None if into == Side::Right => "Copy the left pane's selection into the right pane".into(),
        None => "Copy the right pane's selection into the left pane".into(),
    };
    // Procedurally painted buttons: no font glyphs, no image assets.
    let l_hover = copy_hover(Side::Right);
    let l_copy = |p: &egui::Painter, r, c| draw_copy_icon(p, r, c, true);
    if icon_button(ui, sz, ctl.can_copy(Side::Left), &l_hover, l_copy).clicked() {
        acts.push(A::Copy(Side::Left));
    }
    ui.add_space(6.0);
    let r_hover = copy_hover(Side::Left);
    let r_copy = |p: &egui::Painter, r, c| draw_copy_icon(p, r, c, false);
    if icon_button(ui, sz, ctl.can_copy(Side::Right), &r_hover, r_copy).clicked() {
        acts.push(A::Copy(Side::Right));
    }
    ui.add_space(12.0);
    let del_hover = format!(
        "Delete the selected item(s) in the {} pane",
        ctl.active().label()
    );
    if icon_button(ui, sz, ctl.can_delete(), &del_hover, draw_delete_icon).clicked() {
        acts.push(A::Delete);
    }
    ui.add_space(12.0);
    icon_button(
        ui,
        sz,
        false,
        "Compare - not implemented yet",
        draw_compare_icon,
    );
    ui.add_space(12.0);
    let mut keep_dates = ctl.keep_dates();
    if ui
        .checkbox(&mut keep_dates, "Keep original dates")
        .on_hover_text(
            "Reproduce each copied file's original timestamps on the \
             destination (HFS catalog dates / Amiga datestamp) instead of \
             stamping the current time. Image-to-image copies only.",
        )
        .changed()
    {
        acts.push(A::SetKeepDates(keep_dates));
    }
    ui.add_space(8.0);
    ui.label("Export as:");
    let mut format = ctl.export_format();
    egui::ComboBox::from_id_salt("commander_export_format")
        .selected_text(format.label())
        .width(150.0)
        .show_ui(ui, |ui| {
            for fmt in ExportFormat::ALL {
                ui.selectable_value(&mut format, fmt, fmt.label());
            }
        })
        .response
        .on_hover_text(
            "How the row menu's \"Export to hard drive...\" writes the selection \
             out. The row menu's \"Export as\" submenu can pick a format inline.",
        );
    if format != ctl.export_format() {
        acts.push(A::SetExportFormat(format));
    }
    ui.add_space(4.0);
    // The resource-fork container applies to loose files only.
    ui.add_enabled_ui(ctl.export_format() == ExportFormat::LooseFiles, |ui| {
        ui.label("Forks as:");
        let mut mode = ctl.export_fork_mode();
        egui::ComboBox::from_id_salt("commander_fork_mode")
            .selected_text(mode.label())
            .width(150.0)
            .show_ui(ui, |ui| {
                for m in ResourceForkMode::ALL {
                    ui.selectable_value(&mut mode, m, m.label());
                }
            })
            .response
            .on_hover_text(
                "How to preserve a Mac file's resource fork when exporting loose \
                 files. Tar keeps data forks only; Zip and StuffIt carry forks \
                 natively.",
            );
        if mode != ctl.export_fork_mode() {
            acts.push(A::SetForkMode(mode));
        }
    });
}

// --- procedural button icons -----------------------------------------------
// Painted with the egui Painter (rects/lines/text) rather than a font glyph
// (the default font has no symbols) or bundled image assets (no image-loader
// pipeline is wired). A first cut — easy to refine the proportions later.

/// A clickable, button-shaped rect with a custom-painted icon instead of a text
/// label. Mimics an egui button's fill + hover/disabled styling. `draw` paints
/// the icon into the inset rect with the given foreground color.
fn icon_button(
    ui: &mut egui::Ui,
    size: egui::Vec2,
    enabled: bool,
    hover: &str,
    draw: impl FnOnce(&egui::Painter, egui::Rect, egui::Color32),
) -> egui::Response {
    let sense = if enabled {
        egui::Sense::click()
    } else {
        egui::Sense::hover()
    };
    let (rect, resp) = ui.allocate_exact_size(size, sense);
    let wv = if enabled {
        *ui.style().interact(&resp)
    } else {
        ui.visuals().widgets.noninteractive
    };
    let icon_color = if enabled {
        wv.fg_stroke.color
    } else {
        ui.visuals().weak_text_color()
    };
    let bg = wv.weak_bg_fill;
    let cr = wv.corner_radius;
    let painter = ui.painter();
    painter.rect_filled(rect, cr, bg);
    draw(painter, rect.shrink(8.0), icon_color);
    resp.on_hover_text(hover)
}

/// A classic 3.5" floppy: a square body with a darker shutter near the top and
/// a lighter label across the bottom.
fn draw_floppy(p: &egui::Painter, center: egui::Pos2, side: f32, color: egui::Color32) {
    let body = egui::Rect::from_center_size(center, egui::vec2(side, side));
    p.rect_filled(body, 1.0, color);
    let shutter = egui::Rect::from_min_size(
        egui::pos2(center.x - side * 0.18, body.top() + side * 0.12),
        egui::vec2(side * 0.5, side * 0.28),
    );
    p.rect_filled(shutter, 0.0, egui::Color32::from_black_alpha(130));
    let label = egui::Rect::from_min_size(
        egui::pos2(body.left() + side * 0.16, center.y + side * 0.04),
        egui::vec2(side * 0.68, side * 0.34),
    );
    p.rect_filled(label, 0.0, egui::Color32::from_white_alpha(120));
}

/// Two stacked floppies on the left and an arrow on the right (pointing right
/// for L->R, left for R->L).
fn draw_copy_icon(p: &egui::Painter, r: egui::Rect, color: egui::Color32, rightward: bool) {
    let side = (r.height() * 0.7).min(22.0);
    let cy = r.center().y;
    let fx = r.left() + side * 0.7;
    draw_floppy(p, egui::pos2(fx - 3.0, cy - 3.0), side, color); // back
    draw_floppy(p, egui::pos2(fx + 3.0, cy + 3.0), side, color); // front
    let stroke = egui::Stroke::new(2.5_f32, color);
    let ax0 = r.center().x + 8.0;
    let ax1 = r.right() - 2.0;
    let (tail, head) = if rightward { (ax0, ax1) } else { (ax1, ax0) };
    p.line_segment([egui::pos2(tail, cy), egui::pos2(head, cy)], stroke);
    let d = if rightward { -1.0 } else { 1.0 };
    p.line_segment(
        [egui::pos2(head, cy), egui::pos2(head + d * 5.0, cy - 4.0)],
        stroke,
    );
    p.line_segment(
        [egui::pos2(head, cy), egui::pos2(head + d * 5.0, cy + 4.0)],
        stroke,
    );
}

/// A floppy with a bold red X over it.
fn draw_delete_icon(p: &egui::Painter, r: egui::Rect, color: egui::Color32) {
    let side = (r.height() * 0.8).min(26.0);
    draw_floppy(p, egui::pos2(r.center().x - 5.0, r.center().y), side, color);
    // Fixed rather than themed: this is handed to `icon_button` as a function
    // pointer, so it has no `Visuals` to consult. A mid-red clears the 3:1
    // WCAG floor for non-text graphics against both backgrounds.
    let red = egui::Color32::from_rgb(200, 60, 60);
    let stroke = egui::Stroke::new(3.0_f32, red);
    let h = side * 0.55;
    let xc = egui::pos2(r.center().x + 8.0, r.center().y);
    p.line_segment([xc + egui::vec2(-h, -h), xc + egui::vec2(h, h)], stroke);
    p.line_segment([xc + egui::vec2(-h, h), xc + egui::vec2(h, -h)], stroke);
}

/// "101=011?" — two binary numbers compared, with a question mark.
fn draw_compare_icon(p: &egui::Painter, r: egui::Rect, color: egui::Color32) {
    p.text(
        r.center(),
        egui::Align2::CENTER_CENTER,
        "101=011?",
        egui::FontId::monospace(12.0),
        color,
    );
}

/// Show control characters in a name (the CR in `Icon\r`) instead of a blank box.
fn printable(name: &str) -> String {
    name.chars()
        .map(|c| {
            if (c as u32) < 0x20 {
                format!("\\x{:02X}", c as u32)
            } else {
                c.to_string()
            }
        })
        .collect()
}

/// The shortcut map, logging any lines of the user's file it could not use.
fn load_keymap() -> KeyMap {
    let (map, problems) = KeyMap::load(KeyMap::user_file().as_deref());
    for p in problems {
        log::warn!("keybindings: {p}");
    }
    map
}

/// An egui key as the key map names it; keys no binding can use are `None`.
fn egui_key(key: egui::Key) -> Option<Key> {
    use egui::Key as K;
    Some(match key {
        K::Tab => Key::Tab,
        K::Enter => Key::Enter,
        K::Escape => Key::Escape,
        K::Backspace => Key::Backspace,
        K::Delete => Key::Delete,
        K::Insert => Key::Insert,
        K::Space => Key::Space,
        K::ArrowUp => Key::Up,
        K::ArrowDown => Key::Down,
        K::ArrowLeft => Key::Left,
        K::ArrowRight => Key::Right,
        K::PageUp => Key::PageUp,
        K::PageDown => Key::PageDown,
        K::Home => Key::Home,
        K::End => Key::End,
        K::F1 => Key::F(1),
        K::F2 => Key::F(2),
        K::F3 => Key::F(3),
        K::F4 => Key::F(4),
        K::F5 => Key::F(5),
        K::F6 => Key::F(6),
        K::F7 => Key::F(7),
        K::F8 => Key::F(8),
        K::F9 => Key::F(9),
        K::F10 => Key::F(10),
        K::F11 => Key::F(11),
        K::F12 => Key::F(12),
        other => {
            let name = other.name();
            let mut chars = name.chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) if c.is_ascii_alphanumeric() => Key::Char(c.to_ascii_lowercase()),
                _ => return None,
            }
        }
    })
}

/// egui's modifiers; `mac_cmd` is Cmd on macOS and never set elsewhere.
fn egui_mods(m: egui::Modifiers) -> Mods {
    Mods {
        ctrl: m.ctrl,
        alt: m.alt,
        shift: m.shift,
        cmd: m.mac_cmd,
    }
}
