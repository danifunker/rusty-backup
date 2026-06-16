//! One Commander pane: a source bar (open image / folder + partition picker +
//! Apply/Discard), a path line, and a flat single-directory listing grid with
//! sortable columns, multi-selection, `..` / double-click navigation, and a
//! per-pane staged-edit queue (delete + copy-in, shown in the virtual overlay).
//!
//! A pane lists either a **disk-image volume** (partition probe + `BrowseSession`,
//! opened off-thread via [`BrowseSession::spawn_open`]) or a **host-OS folder**
//! (`std::fs`). All listing state lives in the [`DirListing`] model
//! (`rusty_backup::model::dir_listing`); the pane is the thin egui renderer over
//! it. Applying staged edits runs off-thread via
//! [`rusty_backup::model::commander_ops::spawn_apply`].
//!
//! Image panes stage edits (Apply writes through); host panes take immediate
//! writes and never stage. The cross-pane copy itself lives in
//! [`super::CommanderMode`] (it spans both panes).
//!
//! [`BrowseSession::spawn_open`]: rusty_backup::model::browse_session::BrowseSession::spawn_open

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use eframe::egui;

use rusty_backup::fs::entry::FileEntry;
use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::model::browse_session::{BrowseOpenStatus, BrowseSession};
use rusty_backup::model::commander_ops::{self, ApplyStatus};
use rusty_backup::model::commander_source;
use rusty_backup::model::dir_listing::{type_tag, DirListing, Row, SortColumn};
use rusty_backup::model::edit_queue::{EditQueue, StagedEdit};
use rusty_backup::partition::{format_size, PartitionInfo};

use super::Side;

const ROW_H: f32 = 20.0;
const ADD_COLOR: egui::Color32 = egui::Color32::from_rgb(90, 180, 90);
const DEL_COLOR: egui::Color32 = egui::Color32::from_rgb(150, 150, 150);

/// What a pane reports back to [`super::CommanderMode`] after a frame.
#[derive(Default)]
pub(crate) struct PaneResponse {
    /// A status line for the overlay's bottom bar, if anything happened.
    pub status: Option<String>,
    /// The user asked (via the row menu) to copy this pane's selection to the
    /// other pane; `CommanderMode` performs the cross-pane copy.
    pub copy_to_other: bool,
    /// The user asked (via the row menu) to export this pane's selection to a
    /// host folder they pick; `CommanderMode` runs the threaded host write.
    pub export_to_host: bool,
}

pub(crate) struct CommanderPane {
    side: Side,
    /// Loaded source path (image / container). `None` until the user opens one.
    source: Option<PathBuf>,
    /// Partitions parsed from the source; drives the partition dropdown.
    partitions: Vec<PartitionInfo>,
    /// Index into `partitions` currently being browsed.
    selected_part: Option<usize>,
    /// The directory-listing model this pane renders.
    listing: DirListing,
    /// The session that opened the current partition, kept so Apply can
    /// re-open it read-write. `None` until a partition is opened.
    session: Option<BrowseSession>,
    /// Staged edits (delete + copy-in) for this image pane; empty for host
    /// panes, which write immediately.
    queue: EditQueue,
    /// In-flight async open (spinner) from `BrowseSession::spawn_open`.
    pending_open: Option<Arc<Mutex<BrowseOpenStatus>>>,
    /// In-flight async apply (spinner) from `commander_ops::spawn_apply`.
    pending_apply: Option<Arc<Mutex<ApplyStatus>>>,
    /// Phase text shown next to the spinner while `pending_open` is live.
    open_phase: String,
    /// Volume metadata captured on open, for the source-bar readout.
    volume_label: String,
    fs_type: String,
    total_size: u64,
    used_size: u64,
    /// Last open / navigation / apply error, shown in the pane body.
    error: Option<String>,
    /// A source/partition switch the user requested while the queue was
    /// non-empty; held until they confirm discarding the staged edits.
    pending_switch: Option<PendingSwitch>,
    /// Host-pane only: entry names the user asked to delete, held until they
    /// confirm the (immediate, irreversible) host removal.
    pending_host_delete: Option<Vec<String>>,
}

/// A deferred source change awaiting the unsaved-edits confirmation.
enum PendingSwitch {
    Source(PathBuf),
    HostRoot(PathBuf),
    Partition(usize),
}

impl CommanderPane {
    pub(crate) fn new(side: Side) -> Self {
        Self {
            side,
            source: None,
            partitions: Vec::new(),
            selected_part: None,
            listing: DirListing::new(),
            session: None,
            queue: EditQueue::new(),
            pending_open: None,
            pending_apply: None,
            open_phase: String::new(),
            volume_label: String::new(),
            fs_type: String::new(),
            total_size: 0,
            used_size: 0,
            error: None,
            pending_switch: None,
            pending_host_delete: None,
        }
    }

    /// Number of staged (unapplied) edits on this pane.
    pub(crate) fn staged_count(&self) -> usize {
        self.queue.len()
    }

    /// Discard all staged edits (used by the overlay's Close guard).
    pub(crate) fn discard_edits(&mut self) {
        self.queue.clear();
    }

    /// Render the pane. Returns status + any cross-pane request for the overlay.
    pub(crate) fn show(&mut self, ui: &mut egui::Ui) -> PaneResponse {
        let mut status = self.poll_open(ui.ctx());
        if let Some(s) = self.poll_apply(ui.ctx()) {
            status = Some(s);
        }
        let mut copy_to_other = false;
        let mut export_to_host = false;

        if let Some(s) = self.source_bar(ui) {
            status = Some(s);
        }
        self.path_line(ui);
        ui.separator();

        if self.pending_apply.is_some() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label("Applying staged edits...");
            });
        } else if self.pending_open.is_some() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label(if self.open_phase.is_empty() {
                    "Opening...".to_string()
                } else {
                    self.open_phase.clone()
                });
            });
        } else if let Some(err) = &self.error {
            ui.add_space(12.0);
            ui.colored_label(egui::Color32::from_rgb(220, 120, 120), err);
        } else if self.listing.is_loaded() {
            self.render_header(ui);
            let (s, copy, export) = self.render_rows(ui);
            if s.is_some() {
                status = s;
            }
            copy_to_other = copy;
            export_to_host = export;
        } else {
            ui.centered_and_justified(|ui| {
                ui.weak("Open a disk image or container to browse it here.");
            });
        }

        if let Some(s) = self.render_switch_guard(ui.ctx()) {
            status = Some(s);
        }
        if let Some(s) = self.render_host_delete_guard(ui.ctx()) {
            status = Some(s);
        }

        PaneResponse {
            status,
            copy_to_other,
            export_to_host,
        }
    }

    /// Confirm (and perform) an immediate host delete. No-op when nothing is
    /// pending. Host writes can't be staged/undone, so this is a hard confirm.
    fn render_host_delete_guard(&mut self, ctx: &egui::Context) -> Option<String> {
        let count = self.pending_host_delete.as_ref()?.len();
        let mut confirm = false;
        let mut cancel = false;
        egui::Window::new(format!("Delete from host folder? ({})", self.side.label()))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label(format!(
                    "Permanently delete {count} item(s) from the host folder?"
                ));
                ui.label("This is immediate and cannot be undone.");
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Delete").clicked() {
                        confirm = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                });
            });
        if confirm {
            let names = self.pending_host_delete.take().unwrap_or_default();
            return Some(self.delete_host_entries(&names));
        }
        if cancel {
            self.pending_host_delete = None;
        }
        None
    }

    /// Immediately remove `names` from the host folder, then re-list.
    fn delete_host_entries(&mut self, names: &[String]) -> String {
        let targets: Vec<FileEntry> = self
            .listing
            .entries()
            .iter()
            .filter(|e| names.iter().any(|n| n == &e.name))
            .cloned()
            .collect();
        let (mut removed, mut failed) = (0, 0);
        for e in targets {
            let res = if e.is_directory() {
                std::fs::remove_dir_all(&e.path)
            } else {
                std::fs::remove_file(&e.path)
            };
            match res {
                Ok(()) => removed += 1,
                Err(_) => failed += 1,
            }
        }
        self.reload_listing();
        if failed > 0 {
            format!(
                "[{}] deleted {removed} item(s); {failed} could not be removed.",
                self.side.label()
            )
        } else {
            format!(
                "[{}] deleted {removed} item(s) from the host folder.",
                self.side.label()
            )
        }
    }

    /// Confirm discarding staged edits before honoring a deferred source /
    /// partition switch. No-op when nothing is pending.
    fn render_switch_guard(&mut self, ctx: &egui::Context) -> Option<String> {
        // Short-circuit when nothing is pending.
        self.pending_switch.as_ref()?;
        let n = self.queue.len();
        let mut confirm = false;
        let mut cancel = false;
        egui::Window::new(format!("Discard staged edits? ({})", self.side.label()))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label(format!(
                    "The {} pane has {n} staged edit(s) that have not been applied.",
                    self.side.label()
                ));
                ui.label("Switching the source will discard them.");
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui.button("Discard & switch").clicked() {
                        confirm = true;
                    }
                    if ui.button("Cancel").clicked() {
                        cancel = true;
                    }
                });
            });
        if confirm {
            // `load_source` / `open_partition` reset the queue themselves.
            return self.pending_switch.take().map(|req| match req {
                PendingSwitch::Source(path) => self.load_source(path),
                PendingSwitch::HostRoot(path) => self.load_host(path),
                PendingSwitch::Partition(idx) => self.open_partition(idx),
            });
        }
        if cancel {
            self.pending_switch = None;
        }
        None
    }

    // --- accessors used by CommanderMode for cross-pane copy ---------------

    /// True when this pane can receive a copy: a volume/folder is open and the
    /// pane isn't mid-operation. (A host destination takes an immediate write;
    /// an image destination takes a staged copy.)
    pub(crate) fn can_receive(&self) -> bool {
        self.listing.is_loaded() && self.pending_apply.is_none() && self.pending_open.is_none()
    }

    /// True when this pane lists a host-OS folder rather than a disk image.
    pub(crate) fn is_host_pane(&self) -> bool {
        self.listing.is_host()
    }

    /// A clone of the session that opened this image pane (to re-open the source
    /// read-only on a worker thread for an image->host extraction). `None` for a
    /// host pane or before a source is opened.
    pub(crate) fn session(&self) -> Option<BrowseSession> {
        self.session.clone()
    }

    /// Re-read the current directory listing (after an immediate host write).
    pub(crate) fn reload_listing(&mut self) {
        let _ = self.listing.reload();
    }

    /// True when at least one row is selected.
    pub(crate) fn has_selection(&self) -> bool {
        !self.listing.selection().is_empty()
    }

    /// The selected entries (owned clones) in the current directory.
    pub(crate) fn selected_entries(&self) -> Vec<FileEntry> {
        self.listing
            .selected_entries()
            .into_iter()
            .cloned()
            .collect()
    }

    /// The current directory entry (copy destination parent).
    pub(crate) fn cwd_entry(&self) -> Option<FileEntry> {
        self.listing.cwd().cloned()
    }

    /// Mutable access to the open filesystem (to extract files for a copy).
    pub(crate) fn fs_mut(&mut self) -> Option<&mut (dyn Filesystem + 'static)> {
        self.listing.fs_mut()
    }

    /// Push staged edits onto this pane's queue; returns how many.
    pub(crate) fn stage_edits(&mut self, edits: Vec<StagedEdit>) -> usize {
        let n = edits.len();
        for e in edits {
            self.queue.push(e);
        }
        n
    }

    // --- source bar --------------------------------------------------------

    fn source_bar(&mut self, ui: &mut egui::Ui) -> Option<String> {
        let mut status = None;
        ui.horizontal_wrapped(|ui| {
            if ui.button("Open...").clicked() {
                if let Some(path) = super::super::file_dialog()
                    .add_filter(
                        "Disk Images",
                        rusty_backup::model::file_types::DISK_IMAGE_EXTS,
                    )
                    .add_filter("All Files", &["*"])
                    .pick_file()
                {
                    if self.queue.is_empty() {
                        status = Some(self.load_source(path));
                    } else {
                        self.pending_switch = Some(PendingSwitch::Source(path));
                    }
                }
            }
            if ui.button("Open Folder...").clicked() {
                if let Some(dir) = super::super::file_dialog().pick_folder() {
                    if self.queue.is_empty() {
                        status = Some(self.load_host(dir));
                    } else {
                        self.pending_switch = Some(PendingSwitch::HostRoot(dir));
                    }
                }
            }

            // Partition dropdown (image panes only; host folders have none).
            if !self.listing.is_host() {
                let current = self
                    .selected_part
                    .and_then(|i| self.partitions.get(i))
                    .map(partition_label)
                    .unwrap_or_else(|| "(no partitions)".to_string());
                let mut chosen = self.selected_part;
                egui::ComboBox::from_id_salt(("commander_part", self.side.idx()))
                    .selected_text(current)
                    .show_ui(ui, |ui| {
                        for (i, p) in self.partitions.iter().enumerate() {
                            ui.selectable_value(&mut chosen, Some(i), partition_label(p));
                        }
                    });
                if chosen != self.selected_part {
                    if let Some(i) = chosen {
                        if self.queue.is_empty() {
                            status = Some(self.open_partition(i));
                        } else {
                            self.pending_switch = Some(PendingSwitch::Partition(i));
                        }
                    }
                }
            }

            // Per-pane staging controls (image panes only; host writes are
            // immediate and never staged).
            if !self.listing.is_host() {
                let n = self.queue.len();
                let busy = self.pending_apply.is_some() || self.pending_open.is_some();
                ui.add_enabled_ui(n > 0 && !busy, |ui| {
                    if ui.button(format!("Apply ({n})")).clicked() {
                        status = Some(self.apply());
                    }
                });
                ui.add_enabled_ui(n > 0 && !busy, |ui| {
                    if ui.button("Discard").clicked() {
                        self.queue.clear();
                        status = Some(format!("[{}] discarded staged edits.", self.side.label()));
                    }
                });
            }

            // Right-aligned volume label + free space.
            if self.listing.is_loaded() {
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if self.listing.is_host() {
                        ui.strong("host folder");
                    } else {
                        let free = self.total_size.saturating_sub(self.used_size);
                        ui.label(format!("free: {}", format_size(free)));
                        ui.separator();
                        let label = if self.volume_label.is_empty() {
                            self.fs_type.clone()
                        } else {
                            format!("{} ({})", self.volume_label, self.fs_type)
                        };
                        ui.strong(label);
                    }
                });
            }
        });
        status
    }

    fn path_line(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            let prefix = if self.listing.is_host() {
                "host".to_string()
            } else {
                match self.selected_part {
                    Some(i) => format!("@{}", i + 1),
                    None => "-".to_string(),
                }
            };
            ui.monospace(prefix);
            let path = self.listing.cwd_path();
            ui.monospace(if path.is_empty() { "/" } else { path });
        });
    }

    // --- opening -----------------------------------------------------------

    /// Probe a freshly-picked file and start browsing its first real partition.
    fn load_source(&mut self, path: PathBuf) -> String {
        self.source = Some(path.clone());
        self.listing = DirListing::new();
        self.pending_open = None;
        self.error = None;
        self.selected_part = None;
        self.volume_label.clear();
        self.fs_type.clear();

        match commander_source::probe_partitions(&path) {
            Ok(parts) => {
                self.partitions = parts;
                // Auto-open the first non-extended-container partition.
                let first = self
                    .partitions
                    .iter()
                    .position(|p| !p.is_extended_container);
                match first {
                    Some(i) => self.open_partition(i),
                    None => format!(
                        "[{}] {} has no browsable partitions.",
                        self.side.label(),
                        path.display()
                    ),
                }
            }
            Err(e) => {
                self.partitions.clear();
                self.error = Some(format!("Could not read partitions: {e:#}"));
                format!("[{}] failed to open {}", self.side.label(), path.display())
            }
        }
    }

    /// Open a host-OS folder as this pane's source (synchronous `std::fs` read;
    /// writes to a host pane are immediate, never staged).
    fn load_host(&mut self, dir: PathBuf) -> String {
        self.source = Some(dir.clone());
        self.partitions.clear();
        self.selected_part = None;
        self.session = None;
        self.pending_open = None;
        self.pending_apply = None;
        self.queue.clear();
        self.error = None;
        self.volume_label.clear();
        self.fs_type.clear();

        match self.listing.load_host_root(dir.clone()) {
            Ok(()) => format!(
                "[{}] opened host folder {} ({} item(s)).",
                self.side.label(),
                dir.display(),
                self.listing.entries().len()
            ),
            Err(e) => {
                self.listing = DirListing::new();
                self.error = Some(format!("Could not read folder: {e}"));
                format!(
                    "[{}] failed to open folder {}",
                    self.side.label(),
                    dir.display()
                )
            }
        }
    }

    /// Begin an async open of partition `idx`.
    fn open_partition(&mut self, idx: usize) -> String {
        let Some(path) = self.source.clone() else {
            return String::new();
        };
        let Some(part) = self.partitions.get(idx) else {
            return String::new();
        };
        self.selected_part = Some(idx);
        self.error = None;
        self.listing = DirListing::new();
        self.queue.clear();
        let session = commander_source::session_for(&path, part);
        self.pending_open = Some(session.spawn_open());
        self.session = Some(session);
        self.open_phase = "Opening...".to_string();
        format!(
            "[{}] opening {} ...",
            self.side.label(),
            partition_label(part)
        )
    }

    /// Poll an in-flight open; on completion, hand the filesystem + root listing
    /// to the model or record the error. Returns a status line on completion.
    fn poll_open(&mut self, ctx: &egui::Context) -> Option<String> {
        let arc = self.pending_open.clone()?;
        ctx.request_repaint(); // keep polling until the worker finishes
        let mut guard = arc.lock().ok()?;
        if !guard.finished {
            self.open_phase = guard.phase.clone();
            return None;
        }
        // Finished — detach the pending handle either way.
        self.pending_open = None;

        if let Some(err) = guard.error.take() {
            self.error = Some(err);
            return Some(format!("[{}] open failed.", self.side.label()));
        }

        let fs = guard.fs.take();
        let root = guard.root.take();
        let entries = guard.root_entries.take().unwrap_or_default();
        self.volume_label = guard.volume_label.clone();
        self.fs_type = guard.fs_type.clone();
        self.total_size = guard.total_size;
        self.used_size = guard.used_size;
        drop(guard);

        match (fs, root) {
            (Some(fs), Some(root)) => {
                self.listing.load_root(fs, root, entries, false);
                Some(format!(
                    "[{}] opened {} ({} item(s)).",
                    self.side.label(),
                    if self.volume_label.is_empty() {
                        self.fs_type.clone()
                    } else {
                        self.volume_label.clone()
                    },
                    self.listing.entries().len()
                ))
            }
            _ => {
                self.error = Some("Filesystem opened but no root directory was returned.".into());
                None
            }
        }
    }

    // --- staging -----------------------------------------------------------

    /// Spawn an async apply of the staged queue against this pane's source.
    fn apply(&mut self) -> String {
        if self.queue.is_empty() {
            return String::new();
        }
        let Some(session) = self.session.clone() else {
            return format!("[{}] no source to apply to.", self.side.label());
        };
        let n = self.queue.len();
        let edits: Vec<StagedEdit> = self.queue.iter().cloned().collect();
        self.pending_apply = Some(commander_ops::spawn_apply(session, edits));
        self.error = None;
        format!("[{}] applying {n} edit(s)...", self.side.label())
    }

    /// Poll an in-flight apply; on success, re-open the source so the listing
    /// reflects the write. Returns a status line on completion.
    fn poll_apply(&mut self, ctx: &egui::Context) -> Option<String> {
        let arc = self.pending_apply.clone()?;
        ctx.request_repaint();
        let mut guard = arc.lock().ok()?;
        if !guard.finished {
            return None;
        }
        self.pending_apply = None;
        if let Some(err) = guard.error.take() {
            drop(guard);
            self.error = Some(format!("Apply failed: {err}"));
            return Some(format!("[{}] apply failed.", self.side.label()));
        }
        drop(guard);
        let n = self.queue.len();
        self.queue.clear();
        // Re-open the source: the cached read-only filesystem snapshotted its
        // catalog before the write, so a plain reload would show stale data.
        if let Some(i) = self.selected_part {
            self.open_partition(i);
        }
        Some(format!("[{}] applied {n} edit(s).", self.side.label()))
    }

    /// Toggle the staged-delete state of `names` in the current directory:
    /// stage a delete on a normal entry, undelete a pending delete, or un-stage
    /// a pending copy / new folder.
    fn toggle_delete(&mut self, names: &[String]) {
        let Some(cwd) = self.listing.cwd().cloned() else {
            return;
        };
        let pending_adds = self.queue.pending_adds_for(&cwd.path);
        for name in names {
            if let Some(add) = pending_adds.iter().find(|e| &e.name == name) {
                if add.is_directory() {
                    self.queue.remove_pending_subtree(&add.path);
                } else {
                    self.queue.remove_pending_add(&add.path);
                }
                continue;
            }
            let Some(entry) = self
                .listing
                .entries()
                .iter()
                .find(|e| &e.name == name)
                .cloned()
            else {
                continue;
            };
            if self.queue.is_pending_delete(&entry.path) {
                self.queue.remove_pending_delete(&entry.path);
            } else if entry.is_directory() {
                self.queue.push(StagedEdit::DeleteRecursive {
                    parent: cwd.clone(),
                    entry,
                });
            } else {
                self.queue.push(StagedEdit::DeleteEntry {
                    parent: cwd.clone(),
                    entry,
                });
            }
        }
    }

    // --- listing grid ------------------------------------------------------

    fn render_header(&mut self, ui: &mut egui::Ui) {
        let (rect, resp) = ui.allocate_exact_size(
            egui::vec2(ui.available_width(), ROW_H),
            egui::Sense::click(),
        );
        let c = cols(rect);
        let mid = rect.center().y;
        let font = egui::FontId::proportional(13.0);
        let color = ui.visuals().strong_text_color();
        let active = self.listing.sort_column();
        let desc = self.listing.is_descending();
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
            format!("Size{}", caret(SortColumn::Size)),
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
            egui::Stroke::new(1.0, ui.visuals().window_stroke.color),
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
                self.listing.resort(clicked);
            }
        }
    }

    fn render_rows(&mut self, ui: &mut egui::Ui) -> (Option<String>, bool, bool) {
        let rows = self.build_display_rows();
        let busy = self.pending_apply.is_some();
        // Host panes write immediately (Delete removes now); image panes stage
        // (Delete / Undelete / Remove-from-staging go on the queue).
        let host_pane = self.listing.is_host();

        let mut to_enter: Option<String> = None;
        let mut to_up = false;
        let mut click: Option<(String, bool, bool)> = None;
        let mut bg_deselect = false;
        let mut ctx_rclick: Option<String> = None;
        let mut m_delete = false;
        let mut m_host_delete = false;
        let mut m_copy = false;
        let mut m_export = false;

        egui::ScrollArea::vertical()
            .id_salt(("commander_rows", self.side.idx()))
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let mods = ui.input(|i| i.modifiers);
                for row in &rows {
                    let (rect, resp) = ui.allocate_exact_size(
                        egui::vec2(ui.available_width(), ROW_H),
                        egui::Sense::click(),
                    );
                    let selected = !row.is_parent() && self.listing.is_selected(&row.name);
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
                    paint_row(ui, rect, row);

                    if resp.double_clicked() {
                        if row.is_parent() {
                            to_up = true;
                        } else if row.is_dir {
                            to_enter = Some(row.name.clone());
                        }
                    } else if resp.clicked() {
                        if row.is_parent() {
                            to_up = true;
                        } else {
                            click = Some((row.name.clone(), mods.command, mods.shift));
                        }
                    }

                    // Right-click a data row for its actions.
                    if !row.is_parent() && !busy {
                        let menu = resp.context_menu(|ui| {
                            // Copy applies to real / pending-delete rows, not a
                            // not-yet-applied staged add.
                            if !matches!(row.kind, RowKind::PendingAdd)
                                && ui.button("Copy to other pane").clicked()
                            {
                                m_copy = true;
                                ui.close();
                            }
                            // Export the selection to a host folder the user
                            // picks (loose files, not an archive). Like Copy,
                            // it needs real data, so a not-yet-applied staged
                            // add is excluded.
                            if !matches!(row.kind, RowKind::PendingAdd)
                                && ui.button("Export to hard drive...").clicked()
                            {
                                m_export = true;
                                ui.close();
                            }
                            if host_pane {
                                if ui.button("Delete (immediate)").clicked() {
                                    m_host_delete = true;
                                    ui.close();
                                }
                            } else {
                                let label = match row.kind {
                                    RowKind::PendingDelete => "Undelete",
                                    RowKind::PendingAdd => "Remove from staging",
                                    _ => "Delete",
                                };
                                if ui.button(label).clicked() {
                                    m_delete = true;
                                    ui.close();
                                }
                            }
                        });
                        if menu.is_some() {
                            ctx_rclick = Some(row.name.clone());
                        }
                    }
                }

                // Click empty space to clear the selection.
                let remaining = ui.available_size();
                if remaining.y > 4.0 {
                    let (_r, bgr) = ui.allocate_exact_size(remaining, egui::Sense::click());
                    if bgr.clicked() {
                        bg_deselect = true;
                    }
                }
            });

        let mut status = None;
        if to_up {
            self.listing.up();
        }
        if let Some(name) = to_enter {
            if let Err(e) = self.listing.enter(&name) {
                status = Some(format!("[{}] cannot open '{name}': {e}", self.side.label()));
            }
        }
        if bg_deselect {
            self.listing.clear_selection();
        }
        if let Some((name, command, shift)) = click {
            if shift {
                self.listing.shift_click(&name);
            } else if command {
                self.listing.ctrl_click(&name);
            } else {
                self.listing.click(&name);
            }
        }
        // A right-click on an unselected row acts on just that row.
        if let Some(name) = &ctx_rclick {
            if !self.listing.is_selected(name) {
                self.listing.click(name);
            }
        }
        if m_delete {
            let names: Vec<String> = self.listing.selection().to_vec();
            self.toggle_delete(&names);
            status = Some(format!(
                "[{}] toggled delete on {} item(s).",
                self.side.label(),
                names.len()
            ));
        }
        if m_host_delete {
            let names: Vec<String> = self.listing.selection().to_vec();
            if !names.is_empty() {
                self.pending_host_delete = Some(names);
            }
        }
        (status, m_copy, m_export)
    }
}

/// How a row participates in the staged-edit overlay.
#[derive(Clone, Copy, PartialEq)]
enum RowKind {
    Parent,
    Normal,
    PendingDelete,
    PendingAdd,
}

/// Owned per-frame row snapshot, so the row loop can mutate the listing / queue
/// freely after rendering without holding a borrow of them.
struct DisplayRow {
    name: String,
    is_dir: bool,
    size: u64,
    modified: String,
    type_tag: String,
    kind: RowKind,
}

impl DisplayRow {
    fn is_parent(&self) -> bool {
        self.kind == RowKind::Parent
    }
}

impl CommanderPane {
    /// Build the rendered rows, merging the staged-edit overlay: real entries
    /// flagged pending-delete where the queue has a delete for them, and the
    /// queue's pending adds for this directory appended as green rows.
    fn build_display_rows(&self) -> Vec<DisplayRow> {
        let cwd_path = self.listing.cwd_path().to_string();
        let mut rows: Vec<DisplayRow> = self
            .listing
            .current_rows()
            .into_iter()
            .map(|r| match r {
                Row::Parent => DisplayRow {
                    name: "..".to_string(),
                    is_dir: true,
                    size: 0,
                    modified: String::new(),
                    type_tag: String::new(),
                    kind: RowKind::Parent,
                },
                Row::Entry(e) => {
                    let kind = if self.queue.is_pending_delete(&e.path) {
                        RowKind::PendingDelete
                    } else {
                        RowKind::Normal
                    };
                    DisplayRow {
                        name: e.name.clone(),
                        is_dir: e.is_directory(),
                        size: e.size,
                        modified: e.modified.clone().unwrap_or_default(),
                        type_tag: type_tag(e),
                        kind,
                    }
                }
            })
            .collect();

        for e in self.queue.pending_adds_for(&cwd_path) {
            rows.push(DisplayRow {
                name: e.name.clone(),
                is_dir: e.is_directory(),
                size: e.size,
                modified: String::new(),
                type_tag: type_tag(&e),
                kind: RowKind::PendingAdd,
            });
        }
        rows
    }
}

/// Short label for a partition in the dropdown: `1: FAT16 (510.0 MiB)`.
fn partition_label(p: &PartitionInfo) -> String {
    format!(
        "{}: {} ({})",
        p.index + 1,
        p.type_name,
        format_size(p.size_bytes)
    )
}

// --- column geometry + painting (adapted from the layout mock) -------------

struct Cols {
    name_l: f32,
    name_r: f32,
    size_l: f32,
    size_r: f32,
    mod_l: f32,
    type_l: f32,
}

fn cols(rect: egui::Rect) -> Cols {
    let pad = 6.0;
    let gap = 10.0;
    let type_w = 56.0;
    let mod_w = 134.0;
    let size_w = 80.0;
    let name_l = rect.left() + pad;
    let name_w = (rect.width() - type_w - mod_w - size_w - 4.0 * gap).max(60.0);
    let name_r = name_l + name_w;
    let size_l = name_r + gap;
    let size_r = size_l + size_w;
    let mod_l = size_r + gap;
    let type_l = mod_l + mod_w + gap;
    Cols {
        name_l,
        name_r,
        size_l,
        size_r,
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
        RowKind::PendingAdd => ADD_COLOR,
        RowKind::PendingDelete => DEL_COLOR,
        RowKind::Normal if row.is_dir => egui::Color32::from_rgb(120, 160, 255),
        RowKind::Normal => base,
    };

    // ASCII overlay markers (no Unicode glyphs): "+ " pending add, "- " pending
    // delete, trailing "/" for directories.
    let display_name = match row.kind {
        RowKind::PendingAdd => format!("+ {}", row.name),
        RowKind::PendingDelete => format!("- {}", row.name),
        _ if row.is_dir && !row.is_parent() => format!("{}/", row.name),
        _ => row.name.clone(),
    };

    let name_cell = egui::Rect::from_min_max(
        egui::pos2(c.name_l, rect.top()),
        egui::pos2(c.name_r, rect.bottom()),
    );
    ui.painter_at(name_cell).text(
        egui::pos2(c.name_l, mid),
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
            egui::Stroke::new(1.0, color),
        );
    }
}
