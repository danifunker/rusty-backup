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
use rusty_backup::fs::export_selection::ExportFormat;
#[allow(unused_imports)] // referenced from a doc comment below
use rusty_backup::fs::fork_export::export_file_with_fork;
use rusty_backup::fs::replace::OnConflict;
use rusty_backup::fs::resource_fork::ResourceForkMode;
use rusty_backup::model::checksum::{self, ChecksumJob, ChecksumStatus};
use rusty_backup::model::commander_ops::{
    self, ExportJob, HostCopyJob, HostCopyStatus, StageCopyStatus,
};
use rusty_backup::partition::format_size;

use super::file_detail::{self, FileContent};
use super::metadata_editor::{
    self, ExtPermsEditorState, HfsDatesEditorState, HfsTypeEditorState, OwnerEditorState,
    ProdosTypeEditorState, XattrEditorState,
};

mod pane;
mod progress;

use pane::CommanderPane;
use progress::{ProgressAction, ProgressSnapshot, ProgressWindow};

/// Upper bound on bytes read off a volume to preview the selected file in the
/// File Info window (matches the classic browse view's cap).
const MAX_PREVIEW_SIZE: usize = 1024 * 1024;

/// An open "Calculate Checksums" window: its title, the worker status it
/// polls each frame, and a rate tracker so the progress line can show the
/// same rate + ETA suffix the rest of the app uses.
struct ChecksumWindow {
    title: String,
    status: Arc<Mutex<ChecksumStatus>>,
    /// Sum of every selected file's size, precomputed when the window opens
    /// so the progress bar spans the whole batch (not just the current file).
    total_bytes: u64,
    tracker: super::progress::RateTracker,
}

/// An open "File Info" window: the entry it describes, its decoded preview, and
/// the per-editor scratch state for the editable-metadata subset. Edits stage
/// onto the owning pane's queue (resolved by `side`); host folders and read-only
/// backups show the metadata without the editors.
struct DetailWindow {
    /// Which pane owns the entry (and the queue edits stage onto).
    side: Side,
    entry: FileEntry,
    /// True when the owning pane accepts metadata edits (a writable image
    /// volume). False for host folders and read-only backups, which display the
    /// metadata without the editor rows.
    editable: bool,
    /// The owning pane's filesystem type, gating which editors appear.
    fs_type: String,
    /// Decoded preview content (text or binary), or `None` for a directory /
    /// unreadable file.
    content: Option<FileContent>,
    hfs_editor: Option<HfsTypeEditorState>,
    prodos_editor: Option<ProdosTypeEditorState>,
    dates_editor: Option<HfsDatesEditorState>,
    perms_editor: Option<ExtPermsEditorState>,
    owner_editor: Option<OwnerEditorState>,
    xattr_editor: Option<XattrEditorState>,
    /// The entry's on-disk extended attributes, read when the window opened.
    /// Empty on filesystems without xattr support.
    xattrs: Vec<rusty_backup::fs::xattr::Xattr>,
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
/// A host copy waiting on the "Files already exist" modal.
struct PendingHostConflict {
    job: HostCopyJob,
    dest_side: Option<Side>,
    names: Vec<String>,
}

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
    /// A host copy held back because files it would write already exist;
    /// the modal lets the user overwrite, skip them, or cancel.
    pending_host_conflict: Option<PendingHostConflict>,
    /// In-flight off-thread image->image staging copy (see
    /// [`commander_ops::spawn_stage_copy`]), plus the destination side its
    /// finished edits push onto.
    pending_stage_copy: Option<(Side, Arc<Mutex<StageCopyStatus>>)>,
    /// One-shared progress modal for whichever long-running op is in flight
    /// (host copy / stage copy / apply). Only one op runs at a time, so this
    /// is a single [`ProgressWindow`] that reads the appropriate status.
    progress_window: ProgressWindow,
    /// The open "Calculate Checksums" window, if any (one at a time).
    checksums: Option<ChecksumWindow>,
    /// The open "File Info" window, if any (one at a time).
    detail: Option<DetailWindow>,
    /// When set, an image->image copy reproduces each file's original
    /// timestamps on the destination (HFS catalog dates / Amiga datestamp)
    /// instead of stamping the current time. Defaults on.
    keep_dates: bool,
    /// How resource forks are preserved when copying/exporting an image file to
    /// a host folder (MacBinary / BinHex / AppleDouble / … via
    /// [`export_file_with_fork`]). Ignored for host->host copies.
    export_fork_mode: ResourceForkMode,
    /// The default output shape for "Export to hard drive..." — loose files, a
    /// per-file-compressed tree, or a single tar/zip/sit archive. The row menu's
    /// "Export as" submenu can override it per action.
    export_format: ExportFormat,
    /// The pane the user last interacted with — the middle-column Delete acts
    /// on it. Updated from each pane's `focused` response.
    active: Side,
    /// Rolling history of data-changing operations completed this Commander
    /// session (applies, host copies / exports, immediate host deletes /
    /// renames / new folders), each timestamped. Capped at the most recent
    /// [`Self::MAX_LOG`] entries. Shown in the "Log" window.
    session_log: Vec<String>,
    /// Whether the session-log window is open.
    show_log: bool,
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
            pending_host_conflict: None,
            pending_stage_copy: None,
            progress_window: ProgressWindow::default(),
            checksums: None,
            detail: None,
            keep_dates: true,
            export_fork_mode: ResourceForkMode::MacBinary,
            export_format: ExportFormat::LooseFiles,
            active: Side::Left,
            session_log: Vec::new(),
            show_log: false,
        }
    }

    /// Open a drag-and-dropped path in the active pane (the drag-and-drop router
    /// in `RustyBackupApp::update` calls this while the Commander overlay is up).
    /// A directory opens as a host folder, a file as an image source.
    pub fn open_dropped_path(&mut self, path: PathBuf) {
        let msg = match self.active {
            Side::Left => self.left.open_dropped(path),
            Side::Right => self.right.open_dropped(path),
        };
        if !msg.is_empty() {
            self.status = msg;
        }
    }

    /// Drop both panes' in-memory recent-files mirrors (the Settings dialog
    /// cleared the persisted lists).
    pub(crate) fn clear_recent_files(&mut self) {
        self.left.clear_recent_files();
        self.right.clear_recent_files();
    }

    /// Upper bound on retained session-log entries (rolling; oldest drop first).
    const MAX_LOG: usize = 200;

    /// Timestamp a completed-operation message and append it to the rolling
    /// session log, dropping the oldest entries past [`Self::MAX_LOG`].
    fn record_log(&mut self, msg: String) {
        self.session_log
            .push(format!("[{}] {msg}", log_timestamp()));
        if self.session_log.len() > Self::MAX_LOG {
            let excess = self.session_log.len() - Self::MAX_LOG;
            self.session_log.drain(0..excess);
        }
    }

    /// Render the overlay into the app's root `Ui`. Returns `true` when the
    /// user asks to close it (the caller then drops the `CommanderMode`).
    pub fn show(&mut self, ui: &mut egui::Ui) -> bool {
        let mut close = false;

        self.poll_host_copy(ui.ctx());
        self.poll_stage_copy(ui.ctx());
        self.render_progress_modal(ui.ctx());
        self.render_host_conflict_modal(ui.ctx());

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
                    if ui
                        .button(format!("Log ({})", self.session_log.len()))
                        .on_hover_text("Operations completed this Commander session")
                        .clicked()
                    {
                        self.show_log = !self.show_log;
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
                        let resp = self.left.show(ui);
                        for ev in resp.log_events {
                            self.record_log(ev);
                        }
                        if let Some(msg) = resp.status {
                            self.status = msg;
                        }
                        if resp.copy_to_other {
                            self.status = self.copy(Side::Left);
                        }
                        if resp.export_to_host {
                            self.status = self.export(Side::Left, None);
                        }
                        if let Some(fmt) = resp.export_as {
                            self.status = self.export(Side::Left, Some(fmt));
                        }
                        if resp.checksums {
                            self.status = self.start_checksums(Side::Left);
                        }
                        if let Some(name) = resp.detail {
                            self.status = self.open_detail(Side::Left, name);
                        }
                        if resp.focused {
                            self.active = Side::Left;
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
                        for ev in resp.log_events {
                            self.record_log(ev);
                        }
                        if let Some(msg) = resp.status {
                            self.status = msg;
                        }
                        if resp.copy_to_other {
                            self.status = self.copy(Side::Right);
                        }
                        if resp.export_to_host {
                            self.status = self.export(Side::Right, None);
                        }
                        if let Some(fmt) = resp.export_as {
                            self.status = self.export(Side::Right, Some(fmt));
                        }
                        if resp.checksums {
                            self.status = self.start_checksums(Side::Right);
                        }
                        if let Some(name) = resp.detail {
                            self.status = self.open_detail(Side::Right, name);
                        }
                        if resp.focused {
                            self.active = Side::Right;
                        }
                    },
                );
            });
        });

        self.render_checksum_window(ui.ctx());
        self.render_detail_window(ui.ctx());
        self.render_log_window(ui.ctx());

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

        // When a destination can't receive a copy, say why in the hover so the
        // greyed button reads as "by design" rather than "broken": read-only
        // media (archive / backup / remote host folder) each give a reason.
        let l_copy_hover = match self.right.readonly_reason() {
            Some(reason) => format!("Can't copy into the right pane - {reason}"),
            None => "Copy the left pane's selection into the right pane".to_string(),
        };
        let r_copy_hover = match self.left.readonly_reason() {
            Some(reason) => format!("Can't copy into the left pane - {reason}"),
            None => "Copy the right pane's selection into the left pane".to_string(),
        };

        // Pictured buttons (procedurally painted — no font glyphs, no assets):
        // stacked floppies + arrow for copy, a floppy with a red X for delete,
        // and "101=011?" for the (disabled) compare.
        if icon_button(ui, sz, l_can, &l_copy_hover, |p, r, c| {
            draw_copy_icon(p, r, c, true)
        })
        .clicked()
        {
            status = Some(self.copy(Side::Left));
        }
        ui.add_space(6.0);
        if icon_button(ui, sz, r_can, &r_copy_hover, |p, r, c| {
            draw_copy_icon(p, r, c, false)
        })
        .clicked()
        {
            status = Some(self.copy(Side::Right));
        }
        ui.add_space(12.0);
        // Delete acts on the active pane (the one last clicked in). Disabled when
        // that pane is read-only (backup or archive) or a remote image (the
        // remote write path carries copies only for now, not deletes).
        let active = self.active;
        let (active_has_sel, active_readonly, active_remote) = match active {
            Side::Left => (
                self.left.has_selection(),
                self.left.is_backup_pane() || self.left.is_archive_pane(),
                self.left.is_remote(),
            ),
            Side::Right => (
                self.right.has_selection(),
                self.right.is_backup_pane() || self.right.is_archive_pane(),
                self.right.is_remote(),
            ),
        };
        let del_enabled = idle && active_has_sel && !active_readonly && !active_remote;
        let del_hover = format!("Delete the selected item(s) in the {} pane", active.label());
        if icon_button(ui, sz, del_enabled, &del_hover, draw_delete_icon).clicked() {
            status = Some(match active {
                Side::Left => self.left.delete_selection(),
                Side::Right => self.right.delete_selection(),
            });
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
        ui.checkbox(&mut self.keep_dates, "Keep original dates")
            .on_hover_text(
                "Reproduce each copied file's original timestamps on the \
                 destination (HFS catalog dates / Amiga datestamp) instead of \
                 stamping the current time. Image-to-image copies only.",
            );
        ui.add_space(8.0);
        // Output shape for "Export to hard drive..." — loose files, a per-file
        // compressed tree, or a single tar/zip/sit archive.
        ui.label("Export as:");
        egui::ComboBox::from_id_salt("commander_export_format")
            .selected_text(self.export_format.label())
            .width(150.0)
            .show_ui(ui, |ui| {
                for fmt in ExportFormat::ALL {
                    ui.selectable_value(&mut self.export_format, fmt, fmt.label());
                }
            })
            .response
            .on_hover_text(
                "How the row menu's \"Export to hard drive...\" writes the selection \
                 out. The row menu's \"Export as\" submenu can pick a format inline.",
            );
        ui.add_space(4.0);
        // Resource-fork container, used only by the "Loose files" format. When
        // exporting a Mac file out, its resource fork is preserved in the chosen
        // container (MacBinary / BinHex / AppleDouble / …).
        ui.add_enabled_ui(self.export_format == ExportFormat::LooseFiles, |ui| {
            ui.label("Forks as:");
            egui::ComboBox::from_id_salt("commander_fork_mode")
                .selected_text(self.export_fork_mode.label())
                .width(150.0)
                .show_ui(ui, |ui| {
                    for mode in ResourceForkMode::ALL {
                        ui.selectable_value(&mut self.export_fork_mode, mode, mode.label());
                    }
                })
                .response
                .on_hover_text(
                    "How to preserve a Mac file's resource fork when exporting loose \
                     files. Tar keeps data forks only; Zip and StuffIt carry forks \
                     natively.",
                );
        });
        status
    }

    /// Copy the `from` pane's selection into the other pane's current directory.
    /// Dispatches by source/destination kind:
    /// - image -> image / host -> image: staged onto the destination's queue
    ///   (Apply writes through);
    /// - image -> host / host -> host: an immediate threaded host write.
    fn copy(&mut self, from: Side) -> String {
        let keep_dates = self.keep_dates;
        let fork_mode = self.export_fork_mode;
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

        // Remote host destination: upload over the wire (immediate, like a local
        // host copy). A remote host pane is a `ListingSource::Image`, so
        // `is_host_pane()` is false — intercept it here, before the local
        // `(src_host, dest_host)` match would misroute it to the image path.
        if dest.is_remote_host() {
            let Some(dest_addr) = dest.remote_addr() else {
                return "The remote connection is unavailable; reconnect first.".to_string();
            };
            let dest_parent_path = dest_parent.path.clone();
            let job = if src_host {
                HostCopyJob::HostToRemoteHost {
                    entries,
                    dest_addr,
                    dest_parent: dest_parent_path,
                }
            } else if let Some(source) = src.copy_stage_source() {
                HostCopyJob::ImageToRemoteHost {
                    source,
                    entries,
                    dest_addr,
                    dest_parent: dest_parent_path,
                    fork_mode,
                }
            } else {
                // A '+'-expanded wrapper mount has no reopenable handle; uploading
                // it over the wire isn't wired yet.
                return "This source can't be uploaded to a remote folder yet - \
                        copy it to a local folder first."
                    .to_string();
            };
            self.pending_host_copy =
                Some((Some(from.other()), commander_ops::spawn_host_copy(job)));
            return format!("Uploading to the {other} (remote) folder...");
        }

        match (src_host, dest_host) {
            // host -> image: stage real host paths (no temp extraction).
            (true, false) => {
                let edits = commander_ops::stage_host_to_image(&entries, &dest_parent);
                let n = dest.stage_edits(edits);
                format!("Staged copy of {n} host item(s) into the {other} pane. Apply to write.")
            }
            // image -> image: extract to temp, stage onto the destination queue.
            // Runs on a worker thread so a large multi-file copy shows the same
            // progress modal (percent + rate + ETA) the host-copy path does, and
            // doesn't freeze the UI while extracting.
            (false, false) => {
                if self.temp.is_none() {
                    self.temp = tempfile::tempdir().ok();
                }
                let Some(temp_dir) = self.temp.as_ref().map(|t| t.path().to_path_buf()) else {
                    return "Could not create a temp directory for the copy.".to_string();
                };
                // A reopenable source (local session or remote image) runs on a
                // worker with the same progress modal (percent + rate + ETA) —
                // the worker reopens it off the UI thread.
                if let Some(stage_source) = src.copy_stage_source() {
                    self.pending_stage_copy = Some((
                        from.other(),
                        commander_ops::spawn_stage_copy(
                            stage_source,
                            entries,
                            dest_parent,
                            temp_dir,
                            keep_dates,
                        ),
                    ));
                    self.progress_window.reset();
                    return format!("Copying to the {other} pane...");
                }
                // No reopenable handle: a wrapper-tree mount (a '+'-expanded
                // container, e.g. a floppy opened inline). It's a small, local
                // source, so extract synchronously through its live fs and stage
                // the result — mirroring the session-less image->host branch below.
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
                    Err(e) => format!("Copy to the {other} pane failed: {e:#}"),
                }
            }
            // image -> host: immediate extraction on a worker thread.
            (false, true) => {
                let dest_dir = PathBuf::from(&dest_parent.path);
                // A reopenable source (local session or remote image) extracts on
                // a worker with the same progress modal as any host copy. The
                // completion poll re-lists the destination pane.
                let conflicts =
                    commander_ops::host_copy_conflicts(&entries, &dest_dir, Some(fork_mode));
                if let Some(source) = src.copy_stage_source() {
                    let job = HostCopyJob::ImageToHost {
                        source,
                        entries,
                        dest_dir,
                        fork_mode,
                        on_conflict: OnConflict::Fail,
                    };
                    return self.start_host_copy(job, Some(from.other()), conflicts, other);
                }
                if !conflicts.is_empty() {
                    return format!(
                        "{} item(s) already exist in the {other} folder; this source copies \
                         synchronously, so remove them or pick an empty folder first.",
                        conflicts.len()
                    );
                }
                // No reopenable handle: a wrapper-tree mount (small, local) —
                // extract synchronously over its live fs.
                let Some(src_fs) = src.fs_mut() else {
                    return "Source volume is not open.".to_string();
                };
                match commander_ops::export_fs_entries_to_host(
                    src_fs, &entries, &dest_dir, fork_mode,
                ) {
                    Ok(n) => {
                        dest.reload_listing();
                        format!("Copied {n} item(s) to the {other} folder.")
                    }
                    Err(e) => format!("Copy failed: {e:#}"),
                }
            }
            // host -> host: immediate filesystem copy on a worker thread.
            (true, true) => {
                let dest_dir = PathBuf::from(&dest_parent.path);
                let conflicts = commander_ops::host_copy_conflicts(&entries, &dest_dir, None);
                let job = HostCopyJob::HostToHost {
                    entries,
                    dest_dir,
                    on_conflict: OnConflict::Fail,
                };
                self.start_host_copy(job, Some(from.other()), conflicts, other)
            }
        }
    }

    /// Export the `from` pane's selection to a host folder the user picks
    /// (loose files / folders, not an archive — §15.3). The destination is
    /// independent of what the other pane shows; this is the immediate host
    /// write engine ([`commander_ops::spawn_host_copy`]) with no re-list on
    /// completion (the picked folder isn't a pane).
    fn export(&mut self, from: Side, format_override: Option<ExportFormat>) -> String {
        if self.pending_host_copy.is_some() {
            return "A copy is already in progress; wait for it to finish.".to_string();
        }
        let fork_mode = self.export_fork_mode;
        let format = format_override.unwrap_or(self.export_format);
        let src = match from {
            Side::Left => &mut self.left,
            Side::Right => &mut self.right,
        };
        let entries = src.selected_entries();
        if entries.is_empty() {
            return format!("Nothing selected in the {} pane to export.", from.label());
        }
        // A host source has no `Filesystem` to read archive members from, so only
        // loose files are supported there — reject before prompting for a dest.
        if src.is_host_pane() && format != ExportFormat::LooseFiles {
            return format!(
                "'{}' export runs from an image/volume source; a host folder can only \
                 export as loose files.",
                format.label()
            );
        }

        // Single-file formats prompt for an archive filename; folder formats
        // (loose / per-file) prompt for a destination directory.
        let dest = if format.is_single_file() {
            let ext = format.file_extension().unwrap_or("out");
            let name = export_archive_name(&entries, ext);
            let Some(p) = super::file_dialog()
                .set_file_name(&name)
                .add_filter(format.label(), &[ext])
                .save_file()
            else {
                return "Export cancelled.".to_string();
            };
            p
        } else {
            let Some(d) = super::file_dialog().pick_folder() else {
                return "Export cancelled.".to_string();
            };
            d
        };

        // Host source + loose files: an immediate host-to-host copy.
        if src.is_host_pane() {
            let conflicts = commander_ops::host_copy_conflicts(&entries, &dest, None);
            let job = HostCopyJob::HostToHost {
                entries,
                dest_dir: dest,
                on_conflict: OnConflict::Fail,
            };
            let where_to = format!("export folder ({} pane)", from.label());
            return self.start_host_copy(job, None, conflicts, &where_to);
        }
        // A reopenable image source (local session, remote image, archive, or an
        // inline disk-image/optical wrapper) exports on a worker with progress.
        if let Some(source) = src.copy_stage_source() {
            let job = HostCopyJob::ExportSelection(Box::new(ExportJob {
                source,
                entries,
                dest,
                format,
                fork_mode,
            }));
            self.pending_host_copy = Some((None, commander_ops::spawn_host_copy(job)));
            return format!(
                "Exporting the {} pane selection as {}...",
                from.label(),
                format.label()
            );
        }
        // A non-reopenable wrapper mount (small, local): export synchronously.
        let Some(src_fs) = src.fs_mut() else {
            return "Source volume is not open.".to_string();
        };
        match commander_ops::export_now(src_fs, &entries, &dest, format, fork_mode) {
            Ok(s) => format!("Exported {} file(s) to {}.", s.files, dest.display()),
            Err(e) => format!("Export failed: {e:#}"),
        }
    }

    /// Spawn `job` now, or hold it behind the conflict modal when `conflicts`
    /// names host files it would write over. Returns the status line.
    fn start_host_copy(
        &mut self,
        job: HostCopyJob,
        dest_side: Option<Side>,
        conflicts: Vec<String>,
        where_to: &str,
    ) -> String {
        if conflicts.is_empty() {
            self.pending_host_copy = Some((dest_side, commander_ops::spawn_host_copy(job)));
            return format!("Copying to the {where_to} folder...");
        }
        let n = conflicts.len();
        self.pending_host_conflict = Some(PendingHostConflict {
            job,
            dest_side,
            names: conflicts,
        });
        format!("{n} item(s) already exist in the {where_to} folder; choose what to do.")
    }

    fn render_host_conflict_modal(&mut self, ctx: &egui::Context) {
        let Some(pending) = self.pending_host_conflict.as_ref() else {
            return;
        };
        let mut decision: Option<Option<OnConflict>> = None;
        egui::Window::new("Files already exist")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label(format!(
                    "{} item(s) already exist in the destination folder:",
                    pending.names.len()
                ));
                egui::ScrollArea::vertical()
                    .max_height(160.0)
                    .show(ui, |ui| {
                        for name in pending.names.iter().take(50) {
                            ui.monospace(name);
                        }
                        if pending.names.len() > 50 {
                            ui.label(format!("... and {} more", pending.names.len() - 50));
                        }
                    });
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Overwrite").clicked() {
                        decision = Some(Some(OnConflict::Replace));
                    }
                    if ui.button("Skip existing").clicked() {
                        decision = Some(Some(OnConflict::Skip));
                    }
                    if ui.button("Cancel").clicked() {
                        decision = Some(None);
                    }
                });
            });
        let Some(decision) = decision else {
            return;
        };
        let Some(pending) = self.pending_host_conflict.take() else {
            return;
        };
        match decision {
            Some(on) => {
                let job = pending.job.with_conflict(on);
                self.pending_host_copy =
                    Some((pending.dest_side, commander_ops::spawn_host_copy(job)));
                self.status = "Copying...".to_string();
            }
            None => {
                self.status = "Copy cancelled; nothing was written.".to_string();
            }
        }
        self.record_log(self.status.clone());
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
        let skipped = std::mem::take(&mut guard.skipped);
        drop(guard);
        self.progress_window.reset();

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
            None if skipped.is_empty() => format!("Copied {copied} file(s) to {where_to}."),
            None => format!(
                "Copied {copied} file(s) to {where_to}; skipped {} (see log).",
                skipped.len()
            ),
        };
        self.record_log(self.status.clone());
        for s in skipped {
            self.record_log(format!("Skipped {s}"));
        }
    }

    /// Poll an in-flight image->image stage copy; on completion, push its
    /// edits onto the destination pane's queue and surface the result. Errors
    /// discard the staged edits (the queue is left untouched).
    fn poll_stage_copy(&mut self, ctx: &egui::Context) {
        let Some((dest_side, arc)) = self.pending_stage_copy.clone() else {
            return;
        };
        ctx.request_repaint();
        let Ok(mut guard) = arc.lock() else {
            return;
        };
        if !guard.finished {
            return;
        }
        self.pending_stage_copy = None;
        let err = guard.error.take();
        let edits = std::mem::take(&mut guard.edits);
        drop(guard);
        self.progress_window.reset();

        let dest = match dest_side {
            Side::Left => &mut self.left,
            Side::Right => &mut self.right,
        };
        self.status = match err {
            Some(e) => format!("Copy to the {} pane failed: {e}", dest_side.label()),
            None => {
                let n = dest.stage_edits(edits);
                format!(
                    "Staged copy of {n} item(s) into the {} pane. Apply to write.",
                    dest_side.label()
                )
            }
        };
        self.record_log(self.status.clone());
    }

    /// Render the progress modal for whichever long-running operation is in
    /// flight, if any. Only one runs at a time — image->host copy, image->image
    /// stage copy, or a pane's apply — and each maps to the same
    /// [`ProgressSnapshot`] the widget consumes. Returning early keeps the
    /// modal out of the way when everything is idle.
    fn render_progress_modal(&mut self, ctx: &egui::Context) {
        // 1) Host copy (image->host / host->host / Export to hard drive).
        if let Some((_, arc)) = self.pending_host_copy.clone() {
            let snap = arc.lock().ok().map(|g| ProgressSnapshot {
                title: "Copying".into(),
                current: g.current_file.clone(),
                items_done: g.copied,
                items_total: g.files_total,
                bytes_done: g.bytes_done,
                bytes_total: g.bytes_total,
                finished: g.finished,
                error: g.error.clone(),
                can_cancel: true,
                cancel_requested: g.cancel_requested,
            });
            if let Some(snap) = snap {
                if self.progress_window.show(ctx, &snap) == ProgressAction::Cancel {
                    if let Ok(mut g) = arc.lock() {
                        g.cancel_requested = true;
                    }
                }
            }
            return;
        }
        // 2) Image->image staging copy.
        if let Some((_, arc)) = self.pending_stage_copy.clone() {
            let snap = arc.lock().ok().map(|g| ProgressSnapshot {
                title: "Copying to other pane".into(),
                current: g.current_file.clone(),
                items_done: g.files_done,
                items_total: g.files_total,
                bytes_done: g.bytes_done,
                bytes_total: g.bytes_total,
                finished: g.finished,
                error: g.error.clone(),
                can_cancel: true,
                cancel_requested: g.cancel_requested,
            });
            if let Some(snap) = snap {
                if self.progress_window.show(ctx, &snap) == ProgressAction::Cancel {
                    if let Ok(mut g) = arc.lock() {
                        g.cancel_requested = true;
                    }
                }
            }
            return;
        }
        // 3) Apply queue on either pane (only one is in flight at a time — the
        // pane refuses staging until the previous apply finishes).
        for (side, arc_opt) in [
            (Side::Left, self.left.pending_apply_status()),
            (Side::Right, self.right.pending_apply_status()),
        ] {
            let Some(arc) = arc_opt else { continue };
            let snap = arc.lock().ok().map(|g| ProgressSnapshot {
                title: format!("Applying {} pane edits", side.label()),
                current: g.current_edit.clone(),
                items_done: g.edits_done,
                items_total: g.edits_total,
                bytes_done: g.bytes_done,
                bytes_total: g.bytes_total,
                finished: g.finished,
                error: g.error.clone(),
                can_cancel: false,
                cancel_requested: false,
            });
            if let Some(snap) = snap {
                let _ = self.progress_window.show(ctx, &snap);
            }
            return;
        }
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

        // Precompute the batch total before entries is consumed by the job.
        let total_bytes: u64 = entries.iter().filter(|e| e.is_file()).map(|e| e.size).sum();

        let job = if src.is_host_pane() {
            ChecksumJob::Host { entries }
        } else if let Some(source) = src.copy_stage_source() {
            ChecksumJob::Image { source, entries }
        } else {
            return "Source volume is not open.".to_string();
        };
        let title = if file_count == 1 {
            "Checksums".to_string()
        } else {
            format!("Checksums ({file_count} files)")
        };
        self.checksums = Some(ChecksumWindow {
            title,
            status: checksum::spawn(job),
            total_bytes,
            tracker: super::progress::RateTracker::default(),
        });
        format!("Calculating checksums for {file_count} file(s)...")
    }

    /// Render the open checksum window (if any): a spinner + progress while the
    /// worker runs, then a CRC32 / MD5 / SHA1 / SHA256 grid per file, each value
    /// with a Copy button.
    fn render_checksum_window(&mut self, ctx: &egui::Context) {
        let Some(win) = self.checksums.as_mut() else {
            return;
        };
        let mut open = true;
        let mut to_copy: Option<String> = None;
        let mut running = false;
        let title = win.title.clone();
        let total_bytes = win.total_bytes;
        egui::Window::new(&title)
            .open(&mut open)
            .resizable(true)
            .default_width(560.0)
            .show(ctx, |ui| {
                let Ok(st) = win.status.lock() else {
                    return;
                };
                running = !st.finished;
                if let Some(err) = &st.error {
                    ui.colored_label(super::theme::danger_muted(ui.visuals()), err);
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
                    // Batch-wide byte progress: everything hashed on prior
                    // files (their sizes are recorded in `results`) plus the
                    // in-flight byte counter for the current file. Fed to the
                    // shared RateTracker so the label carries rate + ETA.
                    let done_bytes: u64 =
                        st.results.iter().map(|r| r.size).sum::<u64>() + st.current_bytes;
                    if total_bytes > 0 {
                        win.tracker.record(done_bytes, "Hashing");
                        let frac = (done_bytes as f32 / total_bytes as f32).clamp(0.0, 1.0);
                        let suffix = win.tracker.suffix(done_bytes, total_bytes);
                        let text = format!(
                            "{} / {} ({:.0}%){}",
                            format_size(done_bytes),
                            format_size(total_bytes),
                            frac * 100.0,
                            suffix,
                        );
                        ui.add(egui::ProgressBar::new(frac).text(text).animate(true));
                    } else if st.current_total > 0 {
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
                                    super::theme::danger_muted(ui.visuals()),
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
        // A writable image volume gets the metadata editors; host folders,
        // read-only backups / archives, and wrapper contents (copy out, don't
        // edit) show the metadata without them.
        let editable = !pane.is_host_pane()
            && !pane.is_backup_pane()
            && !pane.is_archive_pane()
            && !pane.wrapper_selection_active();
        let fs_type = pane.detail_fs_type();
        let Some((entry, bytes)) = pane.detail_payload(&name, MAX_PREVIEW_SIZE) else {
            return format!("[{}] could not open File Info for '{name}'.", from.label());
        };
        let content = bytes.map(|data| file_detail::detect_content_type(&entry, &data));
        let xattrs = pane.detail_xattrs(&entry);
        let title = entry.name.clone();
        self.detail = Some(DetailWindow {
            side: from,
            entry,
            editable,
            fs_type,
            content,
            hfs_editor: None,
            prodos_editor: None,
            dates_editor: None,
            perms_editor: None,
            owner_editor: None,
            xattr_editor: None,
            xattrs,
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
        // (Unix permission / owner / xattr rows gate on the entry carrying a
        // POSIX mode, not on the filesystem name — see below.)
        // Writable image panes get the editors; host folders and backups are
        // read-only display.
        let edit_mode = win.editable;

        let mut open = true;
        egui::Window::new(format!("File Info: {}", win.entry.name))
            .open(&mut open)
            .resizable(true)
            .default_width(620.0)
            .show(ctx, |ui| {
                // Read-only metadata rows (HFS/ProDOS suppress inline type/creator
                // — they render it in a dedicated editor row below). Owner-name
                // resolution (id_names) is Inspect-only for now; Commander shows
                // raw uid:gid.
                file_detail::render_metadata_rows(ui, &win.entry, is_hfs || is_prodos, None);

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
                // Unix permissions / owner / xattrs. Gated on the entry actually
                // carrying a POSIX mode rather than on the filesystem name, so
                // every Unix volume gets them (this used to be ext-only).
                if win.entry.mode.is_some() {
                    ui.separator();
                    metadata_editor::render_ext_permissions_row(
                        ui,
                        &win.entry,
                        edit_mode,
                        queue,
                        &mut win.perms_editor,
                        &mut win.result,
                    );
                    metadata_editor::render_owner_row(
                        ui,
                        &win.entry,
                        edit_mode,
                        None,
                        queue,
                        &mut win.owner_editor,
                        &mut win.result,
                    );
                    let xattrs = std::mem::take(&mut win.xattrs);
                    metadata_editor::render_xattr_section(
                        ui,
                        &win.entry,
                        edit_mode,
                        &xattrs,
                        queue,
                        &mut win.xattr_editor,
                        &mut win.result,
                    );
                    win.xattrs = xattrs;
                }

                if let Some(msg) = &win.result {
                    ui.add_space(4.0);
                    ui.colored_label(super::theme::success(ui.visuals()), msg);
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

    /// Render the session-log window (if open): the rolling list of completed
    /// operations, newest at the bottom, with Copy-all / Clear controls.
    fn render_log_window(&mut self, ctx: &egui::Context) {
        if !self.show_log {
            return;
        }
        let mut open = true;
        egui::Window::new("Session log")
            .open(&mut open)
            .resizable(true)
            .default_width(560.0)
            .default_height(360.0)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label(format!(
                        "{} operation(s) this session.",
                        self.session_log.len()
                    ));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.button("Clear").clicked() {
                            self.session_log.clear();
                        }
                        if ui
                            .add_enabled(
                                !self.session_log.is_empty(),
                                egui::Button::new("Copy all"),
                            )
                            .clicked()
                        {
                            ctx.copy_text(self.session_log.join("\n"));
                        }
                    });
                });
                ui.separator();
                if self.session_log.is_empty() {
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
                        for line in &self.session_log {
                            ui.monospace(line);
                        }
                    });
            });
        if !open {
            self.show_log = false;
        }
    }
}

fn log_timestamp() -> String {
    chrono::Local::now().format("%H:%M:%S").to_string()
}

/// Default filename for a single-file export: `<stem>.<ext>` when exactly one
/// entry is selected (its own name, minus any extension), else `export.<ext>`.
fn export_archive_name(entries: &[FileEntry], ext: &str) -> String {
    let stem = match entries {
        [only] => only
            .name
            .rsplit_once('.')
            .map(|(s, _)| s)
            .filter(|s| !s.is_empty())
            .unwrap_or(&only.name),
        _ => "export",
    };
    format!("{stem}.{ext}")
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
