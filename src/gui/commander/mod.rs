//! Commander Mode -- full-page two-pane file explorer overlay.
//!
//! This is wired into [`crate::gui::RustyBackupApp`] as `Option<CommanderMode>`:
//! `Some` means the overlay is open and takes over the whole frame (the tab
//! strip is not drawn). Each pane opens a disk image / container *or* a host-OS
//! folder and browses it (listing, sort, multi-select, `..` / double-click
//! navigation, delete), backed by the
//! [`DirListing`](rusty_backup::model::dir_listing::DirListing) model. The middle
//! column copies one pane's selection onto the other in any combination:
//! image->image and host->image are staged onto the destination's queue, while
//! image->host and host->host are immediate threaded host writes
//! (`commander_ops::{stage_copy, stage_host_to_image, spawn_host_copy}`).
//!
//! NOTE: this crate uses a patched eframe whose panels are
//! `egui::Panel::*::show_inside` rather than the stock `TopBottomPanel`. The
//! main app's `ui()` method hands us its `&mut egui::Ui`, so we build the
//! overlay with `show_inside` against it, exactly like the rest of the GUI.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use eframe::egui;

use rusty_backup::fs::entry::FileEntry;
use rusty_backup::model::checksum::{self, ChecksumJob, ChecksumStatus};
use rusty_backup::model::commander_ops::{self, HostCopyJob, HostCopyStatus};
use rusty_backup::partition::format_size;

use super::file_detail::{self, FileContent};
use super::metadata_editor::{
    self, ExtPermsEditorState, HfsDatesEditorState, HfsTypeEditorState, ProdosTypeEditorState,
};

mod pane;

use pane::CommanderPane;

/// Upper bound on bytes read off a volume to preview the selected file in the
/// File Info window (matches the classic browse view's cap).
const MAX_PREVIEW_SIZE: usize = 1024 * 1024;

/// An open "Calculate Checksums" window: its title and the worker status it
/// polls each frame.
struct ChecksumWindow {
    title: String,
    status: Arc<Mutex<ChecksumStatus>>,
}

/// An open "File Info" window: the entry it describes, its decoded preview, and
/// the per-editor scratch state for the editable-metadata subset. Edits stage
/// onto the owning pane's queue (resolved by `side`); host panes are read-only.
struct DetailWindow {
    /// Which pane owns the entry (and the queue edits stage onto).
    side: Side,
    entry: FileEntry,
    /// True when the owning pane lists a host folder (read-only display).
    is_host: bool,
    /// The owning pane's filesystem type, gating which editors appear.
    fs_type: String,
    /// Decoded preview content (text or binary), or `None` for a directory /
    /// unreadable file.
    content: Option<FileContent>,
    hfs_editor: Option<HfsTypeEditorState>,
    prodos_editor: Option<ProdosTypeEditorState>,
    dates_editor: Option<HfsDatesEditorState>,
    perms_editor: Option<ExtPermsEditorState>,
    /// Last staging status, shown in the window.
    result: Option<String>,
}

/// Which pane is which. Kept tiny and `Copy` so it can key per-pane widget ids
/// (Commander draws two near-identical panes in one `Ui` tree, so every
/// stateful widget must take a side-keyed `id_salt` or egui raises an ID clash).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Left,
    Right,
}

impl Side {
    pub(crate) fn idx(self) -> usize {
        match self {
            Side::Left => 0,
            Side::Right => 1,
        }
    }
    pub(crate) fn label(self) -> &'static str {
        match self {
            Side::Left => "L",
            Side::Right => "R",
        }
    }
    pub(crate) fn other(self) -> Side {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        }
    }
}

/// Full-page Commander Mode overlay.
pub struct CommanderMode {
    left: CommanderPane,
    right: CommanderPane,
    status: String,
    /// Scratch dir holding files extracted for image -> image copies, kept
    /// alive until the destination queue is applied (or the overlay closes).
    /// Created lazily on the first copy.
    temp: Option<tempfile::TempDir>,
    /// Whether the unsaved-edits confirmation is showing (Close was clicked
    /// while a pane had staged edits).
    unsaved_close: bool,
    /// In-flight immediate host-write copy (image->host / host->host) and the
    /// destination side to re-list when it finishes. The side is `None` for an
    /// "Export to hard drive" write, whose destination is an external folder
    /// not shown in either pane (nothing to re-list).
    pending_host_copy: Option<(Option<Side>, Arc<Mutex<HostCopyStatus>>)>,
    /// The open "Calculate Checksums" window, if any (one at a time).
    checksums: Option<ChecksumWindow>,
    /// The open "File Info" window, if any (one at a time).
    detail: Option<DetailWindow>,
    /// When set, an image->image copy reproduces each file's original
    /// timestamps on the destination (HFS catalog dates / Amiga datestamp)
    /// instead of stamping the current time. Defaults on.
    keep_dates: bool,
}

impl Default for CommanderMode {
    fn default() -> Self {
        Self::new()
    }
}

impl CommanderMode {
    pub fn new() -> Self {
        Self {
            left: CommanderPane::new(Side::Left),
            right: CommanderPane::new(Side::Right),
            status: "Commander Mode -- open a disk image in each pane; select files and \
                     use the middle Copy buttons or right-click to stage a copy."
                .into(),
            temp: None,
            unsaved_close: false,
            pending_host_copy: None,
            checksums: None,
            detail: None,
            keep_dates: true,
        }
    }

    /// Render the overlay into the app's root `Ui`. Returns `true` when the
    /// user asks to close it (the caller then drops the `CommanderMode`).
    pub fn show(&mut self, ui: &mut egui::Ui) -> bool {
        let mut close = false;

        self.poll_host_copy(ui.ctx());

        egui::Panel::top("commander_top").show_inside(ui, |ui| {
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.heading("Commander Mode");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Close").clicked() {
                        if self.left.staged_count() + self.right.staged_count() > 0 {
                            self.unsaved_close = true;
                        } else {
                            close = true;
                        }
                    }
                });
            });
            ui.add_space(2.0);
        });

        egui::Panel::bottom("commander_bottom").show_inside(ui, |ui| {
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                ui.label("Status:");
                ui.label(&self.status);
            });
            ui.add_space(2.0);
        });

        egui::CentralPanel::default().show_inside(ui, |ui| {
            let full_h = ui.available_height();
            let full_w = ui.available_width();
            let mid_w = 132.0;
            let pane_w = ((full_w - mid_w) / 2.0 - 8.0).max(200.0);
            ui.horizontal_top(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(pane_w, full_h),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        let resp = self.left.show(ui);
                        if let Some(msg) = resp.status {
                            self.status = msg;
                        }
                        if resp.copy_to_other {
                            self.status = self.copy(Side::Left);
                        }
                        if resp.export_to_host {
                            self.status = self.export(Side::Left);
                        }
                        if resp.checksums {
                            self.status = self.start_checksums(Side::Left);
                        }
                        if let Some(name) = resp.detail {
                            self.status = self.open_detail(Side::Left, name);
                        }
                    },
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(mid_w, full_h),
                    egui::Layout::top_down(egui::Align::Center),
                    |ui| {
                        if let Some(msg) = self.render_middle(ui) {
                            self.status = msg;
                        }
                    },
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(pane_w, full_h),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        let resp = self.right.show(ui);
                        if let Some(msg) = resp.status {
                            self.status = msg;
                        }
                        if resp.copy_to_other {
                            self.status = self.copy(Side::Right);
                        }
                        if resp.export_to_host {
                            self.status = self.export(Side::Right);
                        }
                        if resp.checksums {
                            self.status = self.start_checksums(Side::Right);
                        }
                        if let Some(name) = resp.detail {
                            self.status = self.open_detail(Side::Right, name);
                        }
                    },
                );
            });
        });

        self.render_checksum_window(ui.ctx());
        self.render_detail_window(ui.ctx());

        if self.unsaved_close {
            let n = self.left.staged_count() + self.right.staged_count();
            egui::Window::new("Unsaved staged edits")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ui.ctx(), |ui| {
                    ui.label(format!(
                        "{n} staged edit(s) across the panes have not been applied."
                    ));
                    ui.label("Apply them per-pane first, or discard.");
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui.button("Discard & Close").clicked() {
                            self.left.discard_edits();
                            self.right.discard_edits();
                            self.unsaved_close = false;
                            close = true;
                        }
                        if ui.button("Cancel").clicked() {
                            self.unsaved_close = false;
                        }
                    });
                });
        }

        close
    }

    /// Middle action column: copy one pane's selection onto the other (any
    /// image/host combination). Delete is reachable per-pane (right-click);
    /// Compare is a later milestone.
    fn render_middle(&mut self, ui: &mut egui::Ui) -> Option<String> {
        let mut status = None;
        ui.add_space(60.0);
        let sz = egui::vec2(100.0, 48.0);

        // A copy needs a selection on the source and a ready destination, and no
        // host-write already in flight.
        let idle = self.pending_host_copy.is_none();
        let l_can = idle && self.left.has_selection() && self.right.can_receive();
        let r_can = idle && self.right.has_selection() && self.left.can_receive();

        // Pictured buttons (procedurally painted — no font glyphs, no assets):
        // stacked floppies + arrow for copy, a floppy with a red X for delete,
        // and "101=011?" for the (disabled) compare.
        if icon_button(
            ui,
            sz,
            l_can,
            "Copy the left pane's selection into the right pane",
            |p, r, c| draw_copy_icon(p, r, c, true),
        )
        .clicked()
        {
            status = Some(self.copy(Side::Left));
        }
        ui.add_space(6.0);
        if icon_button(
            ui,
            sz,
            r_can,
            "Copy the right pane's selection into the left pane",
            |p, r, c| draw_copy_icon(p, r, c, false),
        )
        .clicked()
        {
            status = Some(self.copy(Side::Right));
        }
        ui.add_space(12.0);
        // Delete is staged per-pane via right-click; Compare is deferred.
        icon_button(
            ui,
            sz,
            false,
            "Use the row right-click menu to delete",
            draw_delete_icon,
        );
        ui.add_space(12.0);
        icon_button(
            ui,
            sz,
            false,
            "Compare — not implemented yet",
            draw_compare_icon,
        );
        ui.add_space(12.0);
        ui.checkbox(&mut self.keep_dates, "Keep original dates")
            .on_hover_text(
                "Reproduce each copied file's original timestamps on the \
                 destination (HFS catalog dates / Amiga datestamp) instead of \
                 stamping the current time. Image-to-image copies only.",
            );
        status
    }

    /// Copy the `from` pane's selection into the other pane's current directory.
    /// Dispatches by source/destination kind:
    /// - image -> image / host -> image: staged onto the destination's queue
    ///   (Apply writes through);
    /// - image -> host / host -> host: an immediate threaded host write.
    fn copy(&mut self, from: Side) -> String {
        let keep_dates = self.keep_dates;
        let (src, dest) = match from {
            Side::Left => (&mut self.left, &mut self.right),
            Side::Right => (&mut self.right, &mut self.left),
        };

        let entries = src.selected_entries();
        if entries.is_empty() {
            return format!("Nothing selected in the {} pane to copy.", from.label());
        }
        if !dest.can_receive() {
            return format!(
                "The {} pane can't receive a copy (open a volume or folder there first).",
                from.other().label()
            );
        }
        let Some(dest_parent) = dest.cwd_entry() else {
            return "Open a destination first.".to_string();
        };
        let src_host = src.is_host_pane();
        let dest_host = dest.is_host_pane();
        let other = from.other().label();

        match (src_host, dest_host) {
            // host -> image: stage real host paths (no temp extraction).
            (true, false) => {
                let edits = commander_ops::stage_host_to_image(&entries, &dest_parent);
                let n = dest.stage_edits(edits);
                format!("Staged copy of {n} host item(s) into the {other} pane. Apply to write.")
            }
            // image -> image: extract to temp, stage onto the destination queue.
            (false, false) => {
                if self.temp.is_none() {
                    self.temp = tempfile::tempdir().ok();
                }
                let Some(temp_dir) = self.temp.as_ref().map(|t| t.path().to_path_buf()) else {
                    return "Could not create a temp directory for the copy.".to_string();
                };
                let Some(src_fs) = src.fs_mut() else {
                    return "Source volume is not open.".to_string();
                };
                match commander_ops::stage_copy(
                    src_fs,
                    &entries,
                    &dest_parent,
                    &temp_dir,
                    keep_dates,
                ) {
                    Ok(edits) => {
                        let n = dest.stage_edits(edits);
                        format!("Staged copy of {n} item(s) into the {other} pane. Apply to write.")
                    }
                    Err(e) => format!("Copy failed: {e:#}"),
                }
            }
            // image -> host: immediate extraction on a worker thread.
            (false, true) => {
                let Some(session) = src.session() else {
                    return "Source volume is not open.".to_string();
                };
                let dest_dir = PathBuf::from(&dest_parent.path);
                let job = HostCopyJob::ImageToHost {
                    session,
                    entries,
                    dest_dir,
                };
                self.pending_host_copy =
                    Some((Some(from.other()), commander_ops::spawn_host_copy(job)));
                format!("Copying to the {other} folder...")
            }
            // host -> host: immediate filesystem copy on a worker thread.
            (true, true) => {
                let dest_dir = PathBuf::from(&dest_parent.path);
                let job = HostCopyJob::HostToHost { entries, dest_dir };
                self.pending_host_copy =
                    Some((Some(from.other()), commander_ops::spawn_host_copy(job)));
                format!("Copying to the {other} folder...")
            }
        }
    }

    /// Export the `from` pane's selection to a host folder the user picks
    /// (loose files / folders, not an archive — §15.3). The destination is
    /// independent of what the other pane shows; this is the immediate host
    /// write engine ([`commander_ops::spawn_host_copy`]) with no re-list on
    /// completion (the picked folder isn't a pane).
    fn export(&mut self, from: Side) -> String {
        if self.pending_host_copy.is_some() {
            return "A copy is already in progress; wait for it to finish.".to_string();
        }
        let src = match from {
            Side::Left => &self.left,
            Side::Right => &self.right,
        };
        let entries = src.selected_entries();
        if entries.is_empty() {
            return format!("Nothing selected in the {} pane to export.", from.label());
        }
        let Some(dest_dir) = super::file_dialog().pick_folder() else {
            return "Export cancelled.".to_string();
        };

        let job = if src.is_host_pane() {
            HostCopyJob::HostToHost { entries, dest_dir }
        } else {
            let Some(session) = src.session() else {
                return "Source volume is not open.".to_string();
            };
            HostCopyJob::ImageToHost {
                session,
                entries,
                dest_dir,
            }
        };
        self.pending_host_copy = Some((None, commander_ops::spawn_host_copy(job)));
        format!(
            "Exporting the {} pane selection to the host folder...",
            from.label()
        )
    }

    /// Poll an in-flight immediate host copy; on completion, re-list the
    /// destination pane (when the destination is a pane, not an export target)
    /// and surface the result.
    fn poll_host_copy(&mut self, ctx: &egui::Context) {
        let Some((dest_side, arc)) = self.pending_host_copy.clone() else {
            return;
        };
        ctx.request_repaint();
        let Ok(mut guard) = arc.lock() else {
            return;
        };
        if !guard.finished {
            return;
        }
        self.pending_host_copy = None;
        let err = guard.error.take();
        let copied = guard.copied;
        drop(guard);

        // Re-list the destination pane only for a cross-pane copy; an export
        // writes to an external folder that isn't shown in either pane.
        let where_to = match dest_side {
            Some(Side::Left) => {
                self.left.reload_listing();
                "the L folder".to_string()
            }
            Some(Side::Right) => {
                self.right.reload_listing();
                "the R folder".to_string()
            }
            None => "the host folder".to_string(),
        };
        self.status = match err {
            Some(e) => format!("Export to {where_to} failed: {e}"),
            None => format!("Copied {copied} file(s) to {where_to}."),
        };
    }

    /// Open a "Calculate Checksums" window over the `from` pane's selected files
    /// (§15.2). Directories are skipped; an image source is re-opened on the
    /// worker thread (same as export). Replaces any window already open.
    fn start_checksums(&mut self, from: Side) -> String {
        let src = match from {
            Side::Left => &self.left,
            Side::Right => &self.right,
        };
        let entries = src.selected_entries();
        let file_count = entries.iter().filter(|e| e.is_file()).count();
        if file_count == 0 {
            return format!(
                "Select one or more files in the {} pane to checksum (directories are skipped).",
                from.label()
            );
        }

        let job = if src.is_host_pane() {
            ChecksumJob::Host { entries }
        } else {
            let Some(session) = src.session() else {
                return "Source volume is not open.".to_string();
            };
            ChecksumJob::Image { session, entries }
        };
        let title = if file_count == 1 {
            "Checksums".to_string()
        } else {
            format!("Checksums ({file_count} files)")
        };
        self.checksums = Some(ChecksumWindow {
            title,
            status: checksum::spawn(job),
        });
        format!("Calculating checksums for {file_count} file(s)...")
    }

    /// Render the open checksum window (if any): a spinner + progress while the
    /// worker runs, then a CRC32 / MD5 / SHA1 / SHA256 grid per file, each value
    /// with a Copy button.
    fn render_checksum_window(&mut self, ctx: &egui::Context) {
        let Some(win) = &self.checksums else {
            return;
        };
        let mut open = true;
        let mut to_copy: Option<String> = None;
        let mut running = false;
        egui::Window::new(&win.title)
            .open(&mut open)
            .resizable(true)
            .default_width(560.0)
            .show(ctx, |ui| {
                let Ok(st) = win.status.lock() else {
                    return;
                };
                running = !st.finished;
                if let Some(err) = &st.error {
                    ui.colored_label(egui::Color32::from_rgb(220, 120, 120), err);
                    return;
                }
                if running {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label(format!(
                            "Hashing {} ({}/{})",
                            st.current_file,
                            st.done_files + 1,
                            st.total_files
                        ));
                    });
                    if st.current_total > 0 {
                        let frac = st.current_bytes as f32 / st.current_total as f32;
                        ui.add(egui::ProgressBar::new(frac.clamp(0.0, 1.0)).show_percentage());
                    }
                    ui.add_space(4.0);
                }
                egui::ScrollArea::vertical()
                    .auto_shrink([false, true])
                    .show(ui, |ui| {
                        for (i, fc) in st.results.iter().enumerate() {
                            if i > 0 {
                                ui.add_space(6.0);
                                ui.separator();
                            }
                            ui.strong(format!("{}  ({})", fc.name, format_size(fc.size)));
                            if let Some(err) = &fc.error {
                                ui.colored_label(
                                    egui::Color32::from_rgb(220, 120, 120),
                                    format!("failed: {err}"),
                                );
                                continue;
                            }
                            let Some(set) = &fc.set else { continue };
                            egui::Grid::new(("cksum_grid", i))
                                .num_columns(3)
                                .spacing([12.0, 4.0])
                                .show(ui, |ui| {
                                    for (algo, value) in [
                                        ("CRC32", set.crc32_hex()),
                                        ("MD5", set.md5_hex()),
                                        ("SHA1", set.sha1_hex()),
                                        ("SHA256", set.sha256_hex()),
                                    ] {
                                        ui.label(algo);
                                        ui.add(
                                            egui::Label::new(
                                                egui::RichText::new(&value).monospace(),
                                            )
                                            .wrap(),
                                        );
                                        if ui.small_button("Copy").clicked() {
                                            to_copy = Some(value);
                                        }
                                        ui.end_row();
                                    }
                                });
                        }
                    });
            });

        if let Some(text) = to_copy {
            ctx.copy_text(text);
        }
        if running {
            ctx.request_repaint();
        }
        if !open {
            self.checksums = None;
        }
    }

    /// Open the File Info window over the named entry in the `from` pane (§9).
    /// Reads up to 1 MiB for the preview and snapshots the pane's fs type so the
    /// window can offer the right editable-metadata subset. Replaces any window
    /// already open.
    fn open_detail(&mut self, from: Side, name: String) -> String {
        let pane = match from {
            Side::Left => &mut self.left,
            Side::Right => &mut self.right,
        };
        let is_host = pane.is_host_pane();
        let fs_type = pane.fs_type().to_string();
        let Some((entry, bytes)) = pane.detail_payload(&name, MAX_PREVIEW_SIZE) else {
            return format!("[{}] could not open File Info for '{name}'.", from.label());
        };
        let content = bytes.map(|data| file_detail::detect_content_type(&entry, &data));
        let title = entry.name.clone();
        self.detail = Some(DetailWindow {
            side: from,
            entry,
            is_host,
            fs_type,
            content,
            hfs_editor: None,
            prodos_editor: None,
            dates_editor: None,
            perms_editor: None,
            result: None,
        });
        format!("File Info: {title}")
    }

    /// Render the open File Info window (if any): read-only metadata rows + a
    /// text/hex preview, plus the editable-metadata subset (HFS type/creator +
    /// dates, ProDOS type, ext permissions) on image panes. Edits stage onto the
    /// owning pane's queue; host panes are read-only.
    fn render_detail_window(&mut self, ctx: &egui::Context) {
        // Disjoint borrows: the window scratch and the owning pane's queue.
        let CommanderMode {
            detail,
            left,
            right,
            ..
        } = self;
        let Some(win) = detail.as_mut() else {
            return;
        };
        let pane = match win.side {
            Side::Left => left,
            Side::Right => right,
        };

        let fs = win.fs_type.as_str();
        let is_hfs = matches!(fs, "HFS" | "HFS+" | "HFSX");
        let is_classic_hfs = fs == "HFS";
        let is_prodos = fs == "ProDOS";
        let is_ext = fs.starts_with("ext");
        // Image panes get the editors; host panes are read-only display.
        let edit_mode = !win.is_host;

        let mut open = true;
        egui::Window::new(format!("File Info: {}", win.entry.name))
            .open(&mut open)
            .resizable(true)
            .default_width(620.0)
            .show(ctx, |ui| {
                // Read-only metadata rows (HFS/ProDOS suppress inline type/creator
                // — they render it in a dedicated editor row below).
                file_detail::render_metadata_rows(ui, &win.entry, is_hfs || is_prodos);

                let queue = pane.queue_mut();
                if is_hfs && win.entry.is_file() {
                    ui.separator();
                    metadata_editor::render_hfs_type_row(
                        ui,
                        &win.entry,
                        edit_mode,
                        queue,
                        &mut win.hfs_editor,
                        &mut win.result,
                    );
                }
                if is_classic_hfs {
                    metadata_editor::render_hfs_dates_row(
                        ui,
                        &win.entry,
                        edit_mode,
                        queue,
                        &mut win.dates_editor,
                        &mut win.result,
                    );
                }
                if is_prodos && win.entry.is_file() {
                    ui.separator();
                    metadata_editor::render_prodos_type_row(
                        ui,
                        &win.entry,
                        edit_mode,
                        queue,
                        &mut win.prodos_editor,
                        &mut win.result,
                    );
                }
                if is_ext {
                    ui.separator();
                    metadata_editor::render_ext_permissions_row(
                        ui,
                        &win.entry,
                        edit_mode,
                        queue,
                        &mut win.perms_editor,
                        &mut win.result,
                    );
                }

                if let Some(msg) = &win.result {
                    ui.add_space(4.0);
                    ui.colored_label(egui::Color32::from_rgb(120, 200, 120), msg);
                }

                // Preview (read-only).
                ui.separator();
                match &win.content {
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
                            .show(ui, |ui| {
                                file_detail::render_hex_view(ui, data);
                            });
                    }
                    None => {}
                }
            });

        if !open {
            self.detail = None;
        }
    }
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
    let stroke = egui::Stroke::new(2.5, color);
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
    let red = egui::Color32::from_rgb(220, 70, 70);
    let stroke = egui::Stroke::new(3.0, red);
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
