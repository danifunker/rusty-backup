//! One Commander pane: a source bar (open image / folder + partition picker +
//! Apply/Discard), a path line, and a flat single-directory listing grid with
//! sortable columns, multi-selection, `..` / double-click navigation, and a
//! per-pane staged-edit queue (delete + copy-in, shown in the virtual overlay).
//!
//! A pane lists one of three sources: a **disk-image volume** (partition probe +
//! `BrowseSession`, opened off-thread via [`BrowseSession::spawn_open`]), a
//! **host-OS folder** (`std::fs`), or a **backup folder** — native rusty-backup
//! or Clonezilla (read-only; partitions + opener come from the shared
//! [`commander_source::resolve_backup`] resolver). All listing state lives in
//! the [`DirListing`] model
//! (`rusty_backup::model::dir_listing`); the pane is the thin egui renderer over
//! it. Applying staged edits runs off-thread via
//! [`rusty_backup::model::commander_ops::spawn_apply`].
//!
//! Image panes stage edits (Apply writes through); host panes take immediate
//! writes and never stage; backup panes are read-only (browse + copy-out only).
//! The cross-pane copy itself lives in [`super::CommanderMode`] (it spans both
//! panes).
//!
//! [`BrowseSession::spawn_open`]: rusty_backup::model::browse_session::BrowseSession::spawn_open

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use eframe::egui;

use rusty_backup::fs::entry::FileEntry;
use rusty_backup::fs::filesystem::Filesystem;
use rusty_backup::fs::partition_is_browsable;
use rusty_backup::model::browse_session::{BrowseOpenStatus, BrowseSession};
use rusty_backup::model::cache_runner;
use rusty_backup::model::commander_ops::{self, ApplyStatus};
use rusty_backup::model::commander_source;
use rusty_backup::model::dir_listing::{type_tag, DirListing, Row, SortColumn};
use rusty_backup::model::edit_queue::{EditQueue, StagedEdit};
use rusty_backup::model::status::BlockCacheScan;
use rusty_backup::partition::{format_size, PartitionInfo};

use super::Side;

const ROW_H: f32 = 20.0;
const ADD_COLOR: egui::Color32 = egui::Color32::from_rgb(90, 180, 90);
const DEL_COLOR: egui::Color32 = egui::Color32::from_rgb(150, 150, 150);
/// Tint for an existing entry with staged metadata edits (type/dates/perms).
const META_COLOR: egui::Color32 = egui::Color32::from_rgb(150, 190, 255);

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
    /// The user asked (via the row menu) to calculate checksums for this pane's
    /// selection; `CommanderMode` opens the threaded checksum window.
    pub checksums: bool,
    /// The user double-clicked a file or chose "File Info..."; carries the
    /// entry name. `CommanderMode` opens the floating File Info window over it.
    pub detail: Option<String>,
    /// The user interacted with (clicked in) this pane this frame, so it becomes
    /// the "active" pane the middle-column Delete acts on.
    pub focused: bool,
    /// Data-changing operations that completed on this pane this frame (a staged
    /// queue applied, or an immediate host delete / rename / new folder), in the
    /// order they happened. `CommanderMode` timestamps each and appends it to the
    /// rolling session log.
    pub log_events: Vec<String>,
}

/// Per-frame outcome of the listing grid (which row action, if any, fired).
#[derive(Default)]
struct RowActions {
    status: Option<String>,
    /// Copy the selection to the other pane.
    copy: bool,
    /// Export the selection to a picked host folder.
    export: bool,
    /// Calculate checksums for the selection.
    checksums: bool,
    /// Open the File Info window for this entry (by name).
    detail: Option<String>,
    /// Open this entry (a file on a remote-host pane) as a disk image (by name).
    open_image: Option<String>,
    /// A row in this pane was clicked/navigated this frame (focus signal).
    focused: bool,
}

pub(crate) struct CommanderPane {
    side: Side,
    /// Loaded source path (image / container). `None` until the user opens one.
    source: Option<PathBuf>,
    /// Partitions parsed from the source; drives the partition dropdown.
    partitions: Vec<PartitionInfo>,
    /// When the source is a backup folder (native rusty-backup *or* Clonezilla),
    /// the resolved backup — `open_partition` routes through it to build the
    /// right session. `None` for an image / host source.
    resolved_backup: Option<commander_source::ResolvedBackup>,
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
    /// In-flight Clonezilla metadata scan (spinner) from
    /// `cache_runner::spawn_partclone_scan`. On completion `poll_scan` builds a
    /// partclone-cache session and hands off to `pending_open`.
    pending_scan: Option<Arc<Mutex<BlockCacheScan>>>,
    /// Scanned Clonezilla block caches reused across opens this session. The
    /// shared store also holds the in-memory/on-disk/scan decision tree so this
    /// pane and the Inspect tab agree (see `commander_source::PartcloneCacheStore`).
    cache_store: commander_source::PartcloneCacheStore,
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
    /// The open "Rename..." dialog, if any (single entry at a time).
    rename_dialog: Option<RenameDialog>,
    /// The open "New Folder..." dialog, if any.
    new_folder_dialog: Option<NewFolderDialog>,
    /// Whether the "Pending edits" review popup is open for this pane.
    show_edits_popup: bool,
    /// Data-changing operations completed this frame, drained into the
    /// `PaneResponse` at the end of `show()` for the session log.
    log_events: Vec<String>,
    /// When this pane browses a disk image on a remote daemon, the connection
    /// info. Also the marker that this is a remote pane. `None` for local.
    remote: Option<RemoteConn>,
    /// In-flight async remote connect + open (spinner).
    pending_remote: Option<Arc<Mutex<RemoteOpenStatus>>>,
    /// The open "Connect to remote..." dialog, if any.
    connect_dialog: Option<ConnectDialog>,
}

/// A live remote-pane connection (and the marker that the pane is remote).
/// `mode` is what the pane currently shows: the daemon's host filesystem (a
/// file browser), or inside an image opened from it.
struct RemoteConn {
    addr: String,
    mode: RemoteMode,
}

/// What a remote pane is currently browsing.
enum RemoteMode {
    /// The daemon's host filesystem — a file browser. Files can be opened as
    /// images (double-click / right-click -> Open Image).
    Host,
    /// Inside a disk image opened on the daemon.
    Image {
        path: String,
        partition: Option<u32>,
    },
}

/// Async status for a remote connect + open running on a worker thread. The
/// opened filesystem is `Send`, so it moves back to the UI thread here.
#[derive(Default)]
struct RemoteOpenStatus {
    done: bool,
    addr: String,
    result: Option<Result<RemoteOpened, String>>,
}

/// The successful payload of a remote open — the host file browser, or an image
/// opened from it — handed back to the UI thread.
enum RemoteOpened {
    Host {
        fs: rusty_backup::remote::RemoteHostFilesystem,
        root: FileEntry,
        entries: Vec<FileEntry>,
    },
    Image {
        fs: rusty_backup::remote::RemoteFilesystem,
        root: FileEntry,
        entries: Vec<FileEntry>,
        fs_type: String,
        volume_label: String,
        total_size: u64,
        used_size: u64,
        path: String,
        partition: Option<u32>,
    },
}

/// State for the modal "Connect to remote..." dialog (host only — you browse the
/// remote machine and open an image you find, rather than naming it up front).
struct ConnectDialog {
    /// `host` or `host:port` of the daemon.
    host: String,
    /// Last validation / connect error, shown inline.
    error: Option<String>,
}

/// State for the modal "Rename..." dialog.
struct RenameDialog {
    /// The live entry being renamed.
    entry: FileEntry,
    /// The editable new-name buffer (seeded with the current name).
    input: String,
    /// Last validation error, shown inline; cleared as the user types.
    error: Option<String>,
}

/// State for the modal "New Folder..." dialog.
struct NewFolderDialog {
    /// The editable folder-name buffer.
    input: String,
    /// Last validation error, shown inline; cleared as the user types.
    error: Option<String>,
}

/// A deferred source change awaiting the unsaved-edits confirmation.
enum PendingSwitch {
    Source(PathBuf),
    HostRoot(PathBuf),
    /// Open a native rusty-backup folder as the source.
    Backup(PathBuf),
    Partition(usize),
    /// Close (unload) the pane entirely.
    Close,
}

impl CommanderPane {
    pub(crate) fn new(side: Side) -> Self {
        Self {
            side,
            source: None,
            partitions: Vec::new(),
            resolved_backup: None,
            selected_part: None,
            listing: DirListing::new(),
            session: None,
            queue: EditQueue::new(),
            pending_open: None,
            pending_apply: None,
            pending_scan: None,
            cache_store: commander_source::PartcloneCacheStore::new(),
            open_phase: String::new(),
            volume_label: String::new(),
            fs_type: String::new(),
            total_size: 0,
            used_size: 0,
            error: None,
            pending_switch: None,
            pending_host_delete: None,
            rename_dialog: None,
            new_folder_dialog: None,
            show_edits_popup: false,
            log_events: Vec::new(),
            remote: None,
            pending_remote: None,
            connect_dialog: None,
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
        if let Some(s) = self.poll_scan(ui.ctx()) {
            status = Some(s);
        }
        if let Some(s) = self.poll_remote(ui.ctx()) {
            status = Some(s);
        }
        let mut copy_to_other = false;
        let mut export_to_host = false;
        let mut checksums = false;
        let mut detail = None;
        let mut focused = false;

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
        } else if self.pending_scan.is_some() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label("Scanning Clonezilla metadata (first open of this partition)...");
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
        } else if self.pending_remote.is_some() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label(if self.open_phase.is_empty() {
                    "Connecting...".to_string()
                } else {
                    self.open_phase.clone()
                });
            });
        } else if let Some(err) = &self.error {
            ui.add_space(12.0);
            ui.colored_label(egui::Color32::from_rgb(220, 120, 120), err);
        } else if self.listing.is_loaded() {
            self.render_header(ui);
            let actions = self.render_rows(ui);
            if actions.status.is_some() {
                status = actions.status;
            }
            copy_to_other = actions.copy;
            export_to_host = actions.export;
            checksums = actions.checksums;
            detail = actions.detail;
            focused = actions.focused;
            // Open a file on a remote-host pane as a disk image.
            if let Some(name) = actions.open_image {
                let path = self
                    .listing
                    .entries()
                    .iter()
                    .find(|e| e.name == name)
                    .map(|e| e.path.clone());
                let addr = self.remote.as_ref().map(|r| r.addr.clone());
                if let (Some(path), Some(addr)) = (path, addr) {
                    status = Some(self.spawn_open_image(addr, path, None));
                }
            }
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
        if let Some(s) = self.render_connect_dialog(ui.ctx()) {
            status = Some(s);
        }
        if let Some(s) = self.render_new_folder_dialog(ui.ctx()) {
            status = Some(s);
        }
        if let Some(s) = self.render_rename_dialog(ui.ctx()) {
            status = Some(s);
        }
        self.render_edits_popup(ui.ctx());

        PaneResponse {
            status,
            copy_to_other,
            export_to_host,
            checksums,
            detail,
            focused,
            log_events: std::mem::take(&mut self.log_events),
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
        let msg = if failed > 0 {
            format!(
                "[{}] deleted {removed} item(s); {failed} could not be removed.",
                self.side.label()
            )
        } else {
            format!(
                "[{}] deleted {removed} item(s) from the host folder.",
                self.side.label()
            )
        };
        if removed > 0 {
            self.log_events.push(msg.clone());
        }
        msg
    }

    /// The modal "Rename..." dialog. Image panes stage a `Rename` edit (applied
    /// on Apply); host panes rename immediately via `std::fs::rename`. No-op
    /// when nothing is pending.
    fn render_rename_dialog(&mut self, ctx: &egui::Context) -> Option<String> {
        self.rename_dialog.as_ref()?;
        let side = self.side;
        let mut do_rename = false;
        let mut cancel = false;
        {
            let d = self.rename_dialog.as_mut().unwrap();
            let old_name = d.entry.name.clone();
            let input = &mut d.input;
            let err = d.error.clone();
            egui::Window::new(format!("Rename ({})", side.label()))
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label(format!("Rename \"{old_name}\" to:"));
                    let resp = ui.text_edit_singleline(input);
                    if let Some(e) = &err {
                        ui.colored_label(egui::Color32::from_rgb(220, 120, 120), e);
                    }
                    let valid = !input.trim().is_empty() && *input != old_name;
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui.add_enabled(valid, egui::Button::new("Rename")).clicked() {
                            do_rename = true;
                        }
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                    });
                    // Enter confirms when the name is valid.
                    if valid && resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        do_rename = true;
                    }
                });
        }

        if cancel {
            self.rename_dialog = None;
            return None;
        }
        if do_rename {
            let new_name = self
                .rename_dialog
                .as_ref()
                .unwrap()
                .input
                .trim()
                .to_string();
            // Validate against the filesystem's name rules (image panes only;
            // host names are validated by the OS at rename time).
            if let Some(Err(e)) = self.listing.fs_mut().map(|fs| fs.validate_name(&new_name)) {
                if let Some(d) = self.rename_dialog.as_mut() {
                    d.error = Some(format!("{e}"));
                }
                return None;
            }
            let entry = self.rename_dialog.take().unwrap().entry;
            return Some(self.perform_rename(entry, new_name));
        }
        None
    }

    /// Carry out a confirmed rename: stage it (image pane) or apply it now
    /// (host pane), then refresh.
    fn perform_rename(&mut self, entry: FileEntry, new_name: String) -> String {
        let side = self.side.label();
        if self.listing.is_host() {
            let old_path = PathBuf::from(&entry.path);
            let new_path = old_path.with_file_name(&new_name);
            if new_path.exists() {
                return format!("[{side}] '{new_name}' already exists.");
            }
            match std::fs::rename(&old_path, &new_path) {
                Ok(()) => {
                    self.reload_listing();
                    let msg = format!("[{side}] renamed '{}' to '{new_name}'.", entry.name);
                    self.log_events.push(msg.clone());
                    msg
                }
                Err(e) => format!("[{side}] rename failed: {e}"),
            }
        } else {
            let Some(cwd) = self.listing.cwd().cloned() else {
                return format!("[{side}] no directory to rename in.");
            };
            // Replace any prior staged rename of the same entry.
            self.queue.remove_pending_rename(&entry.path);
            self.queue.push(StagedEdit::Rename {
                parent: cwd,
                entry,
                new_name: new_name.clone(),
            });
            format!("[{side}] staged rename to '{new_name}'. Apply to write.")
        }
    }

    /// The "New Folder..." modal: a name field staged as a CreateDirectory on
    /// an image pane, or created immediately on a host pane.
    fn render_new_folder_dialog(&mut self, ctx: &egui::Context) -> Option<String> {
        self.new_folder_dialog.as_ref()?;
        let side = self.side;
        let mut do_create = false;
        let mut cancel = false;
        {
            let d = self.new_folder_dialog.as_mut().unwrap();
            let input = &mut d.input;
            let err = d.error.clone();
            egui::Window::new(format!("New Folder ({})", side.label()))
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("Folder name:");
                    let resp = ui.text_edit_singleline(input);
                    if let Some(e) = &err {
                        ui.colored_label(egui::Color32::from_rgb(220, 120, 120), e);
                    }
                    let valid = !input.trim().is_empty();
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui.add_enabled(valid, egui::Button::new("Create")).clicked() {
                            do_create = true;
                        }
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                    });
                    if valid && resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        do_create = true;
                    }
                });
        }

        if cancel {
            self.new_folder_dialog = None;
            return None;
        }
        if do_create {
            let name = self
                .new_folder_dialog
                .as_ref()
                .unwrap()
                .input
                .trim()
                .to_string();
            // Name validity (image panes; host names are validated by the OS).
            if let Some(Err(e)) = self.listing.fs_mut().map(|fs| fs.validate_name(&name)) {
                if let Some(d) = self.new_folder_dialog.as_mut() {
                    d.error = Some(format!("{e}"));
                }
                return None;
            }
            // Reject a name already present (on disk or staged here).
            let cwd_path = self.listing.cwd_path().to_string();
            let dup = self.listing.entries().iter().any(|e| e.name == name)
                || self
                    .queue
                    .pending_adds_for(&cwd_path)
                    .iter()
                    .any(|e| e.name == name);
            if dup {
                if let Some(d) = self.new_folder_dialog.as_mut() {
                    d.error = Some(format!("'{name}' already exists here"));
                }
                return None;
            }
            self.new_folder_dialog = None;
            return Some(self.perform_new_folder(name));
        }
        None
    }

    /// Create the folder: immediate `std::fs::create_dir` on a host pane, or a
    /// staged `CreateDirectory` on an image pane.
    fn perform_new_folder(&mut self, name: String) -> String {
        let side = self.side.label();
        if self.listing.is_host() {
            let dir = PathBuf::from(self.listing.cwd_path()).join(&name);
            match std::fs::create_dir(&dir) {
                Ok(()) => {
                    self.reload_listing();
                    let msg = format!("[{side}] created folder '{name}'.");
                    self.log_events.push(msg.clone());
                    msg
                }
                Err(e) => format!("[{side}] create folder failed: {e}"),
            }
        } else {
            let Some(cwd) = self.listing.cwd().cloned() else {
                return format!("[{side}] no directory open.");
            };
            self.queue.push(StagedEdit::CreateDirectory {
                parent: cwd,
                name: name.clone(),
            });
            format!("[{side}] staged new folder '{name}'. Apply to write.")
        }
    }

    /// Render the modal "Connect to remote..." dialog (host:port only). On
    /// Connect it opens the daemon's host filesystem (the file browser); the
    /// listing swaps in when `poll_remote` sees it finish. You then open an
    /// image you find by double-clicking it (or right-click -> Open Image).
    fn render_connect_dialog(&mut self, ctx: &egui::Context) -> Option<String> {
        self.connect_dialog.as_ref()?;
        let side = self.side;
        let mut do_connect = false;
        let mut cancel = false;
        {
            let d = self.connect_dialog.as_mut().unwrap();
            egui::Window::new(format!("Connect to remote ({})", side.label()))
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label("Host (rb-cli serve daemon):");
                    let resp = ui.text_edit_singleline(&mut d.host);
                    ui.label(
                        egui::RichText::new("e.g. 192.168.1.42:7341  (port 7341 if omitted)")
                            .weak()
                            .small(),
                    );
                    if let Some(e) = &d.error {
                        ui.colored_label(egui::Color32::from_rgb(220, 120, 120), e);
                    }
                    let valid = !d.host.trim().is_empty();
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui
                            .add_enabled(valid, egui::Button::new("Connect"))
                            .clicked()
                        {
                            do_connect = true;
                        }
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                    });
                    if valid && resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                        do_connect = true;
                    }
                });
        }

        if cancel {
            self.connect_dialog = None;
            return None;
        }
        if do_connect {
            let host_raw = self
                .connect_dialog
                .as_ref()
                .unwrap()
                .host
                .trim()
                .to_string();
            let addr = if host_raw.contains(':') {
                host_raw
            } else {
                format!(
                    "{host_raw}:{}",
                    rusty_backup::remote::protocol::DEFAULT_PORT
                )
            };
            self.connect_dialog = None;
            return Some(self.spawn_connect_host(addr));
        }
        None
    }

    /// Connect and open the daemon's host filesystem (the file browser) on a
    /// worker thread — a connect can block on an unreachable host.
    fn spawn_connect_host(&mut self, addr: String) -> String {
        let status = Arc::new(Mutex::new(RemoteOpenStatus {
            addr: addr.clone(),
            ..Default::default()
        }));
        self.pending_remote = Some(status.clone());
        self.open_phase = format!("Connecting to {addr}...");
        let msg = format!("[{}] connecting to {addr}...", self.side.label());
        std::thread::spawn(move || {
            let result = rusty_backup::remote::RemoteHostFilesystem::open(&addr, "/")
                .map(|(fs, root, entries)| RemoteOpened::Host { fs, root, entries })
                .map_err(|e| format!("{e:#}"));
            if let Ok(mut s) = status.lock() {
                s.result = Some(result);
                s.done = true;
            }
        });
        msg
    }

    /// Open an image found while browsing the remote host FS, on a worker thread.
    fn spawn_open_image(&mut self, addr: String, path: String, partition: Option<u32>) -> String {
        let status = Arc::new(Mutex::new(RemoteOpenStatus {
            addr: addr.clone(),
            ..Default::default()
        }));
        self.pending_remote = Some(status.clone());
        self.open_phase = format!("Opening {path}...");
        let msg = format!("[{}] opening {path}...", self.side.label());
        std::thread::spawn(move || {
            let result = rusty_backup::remote::RemoteFilesystem::open(&addr, &path, partition)
                .map(|(fs, root, entries)| RemoteOpened::Image {
                    fs_type: fs.fs_type().to_string(),
                    volume_label: fs.volume_label().unwrap_or_default().to_string(),
                    total_size: fs.total_size(),
                    used_size: fs.used_size(),
                    fs,
                    root,
                    entries,
                    path,
                    partition,
                })
                .map_err(|e| format!("{e:#}"));
            if let Ok(mut s) = status.lock() {
                s.result = Some(result);
                s.done = true;
            }
        });
        msg
    }

    /// Reset local-source state when this pane becomes (or re-targets) remote.
    fn reset_for_remote(&mut self) {
        self.source = None;
        self.partitions = Vec::new();
        self.resolved_backup = None;
        self.selected_part = None;
        self.session = None;
        self.queue.clear();
        self.error = None;
    }

    /// Poll an in-flight remote open; when done, swap the result (the host file
    /// browser, or an opened image) into the listing.
    fn poll_remote(&mut self, ctx: &egui::Context) -> Option<String> {
        let done = match self.pending_remote.as_ref() {
            Some(s) => s.lock().ok().map(|g| g.done).unwrap_or(false),
            None => return None,
        };
        if !done {
            ctx.request_repaint();
            return None;
        }
        let (addr, result) = {
            let arc = self.pending_remote.take()?;
            let mut g = arc.lock().ok()?;
            (g.addr.clone(), g.result.take()?)
        };
        self.open_phase.clear();

        match result {
            Ok(RemoteOpened::Host { fs, root, entries }) => {
                self.reset_for_remote();
                self.fs_type = "remote-host".to_string();
                self.volume_label = String::new();
                self.total_size = 0;
                self.used_size = 0;
                self.remote = Some(RemoteConn {
                    addr: addr.clone(),
                    mode: RemoteMode::Host,
                });
                self.listing.load_root(Box::new(fs), root, entries, false);
                Some(format!(
                    "[{}] connected to {addr} (browsing host)",
                    self.side.label()
                ))
            }
            Ok(RemoteOpened::Image {
                fs,
                root,
                entries,
                fs_type,
                volume_label,
                total_size,
                used_size,
                path,
                partition,
            }) => {
                self.reset_for_remote();
                self.fs_type = fs_type;
                self.volume_label = volume_label;
                self.total_size = total_size;
                self.used_size = used_size;
                let shown = path.clone();
                self.remote = Some(RemoteConn {
                    addr: addr.clone(),
                    mode: RemoteMode::Image { path, partition },
                });
                self.listing.load_root(Box::new(fs), root, entries, false);
                Some(format!("[{}] opened rb://{addr}{shown}", self.side.label()))
            }
            Err(e) => {
                self.error = Some(e.clone());
                Some(format!(
                    "[{}] remote connect failed: {e}",
                    self.side.label()
                ))
            }
        }
    }

    /// True when this pane is browsing a remote daemon's *host* filesystem (the
    /// file browser), where a file can be opened as an image.
    fn is_remote_host(&self) -> bool {
        matches!(
            self.remote,
            Some(RemoteConn {
                mode: RemoteMode::Host,
                ..
            })
        )
    }

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
                PendingSwitch::Backup(path) => self.load_backup_source(path),
                PendingSwitch::Partition(idx) => self.open_partition(idx),
                PendingSwitch::Close => self.close_source(),
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
    /// an image destination takes a staged copy.) A read-only backup pane can
    /// never receive a copy.
    pub(crate) fn can_receive(&self) -> bool {
        self.listing.is_loaded()
            && self.pending_apply.is_none()
            && self.pending_open.is_none()
            && self.resolved_backup.is_none()
            // A remote pane can be browsed and copied *out of*, but not yet
            // copied *into* (the local→remote write path is a later increment).
            && self.remote.is_none()
    }

    /// True when this pane lists a host-OS folder rather than a disk image.
    pub(crate) fn is_host_pane(&self) -> bool {
        self.listing.is_host()
    }

    /// True when this pane lists a (read-only) backup folder's contents — native
    /// rusty-backup or Clonezilla. Backups can be browsed and copied *out of*,
    /// but not edited: writing to a raw backup's partition file would silently
    /// desync its `metadata.json` checksum, zstd backups have no in-place
    /// editable session, and Clonezilla images are read-through a block cache.
    pub(crate) fn is_backup_pane(&self) -> bool {
        self.resolved_backup.is_some()
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

    /// Delete this pane's current selection — the middle-column Delete acting on
    /// the active pane. Mirrors the row right-click delete: an image pane toggles
    /// a staged delete (Apply writes through); a host pane queues a confirm.
    pub(crate) fn delete_selection(&mut self) -> String {
        if self.is_backup_pane() {
            return format!("[{}] backup panes are read-only.", self.side.label());
        }
        let names: Vec<String> = self.listing.selection().to_vec();
        if names.is_empty() {
            return format!("[{}] nothing selected to delete.", self.side.label());
        }
        if self.listing.is_host() {
            self.pending_host_delete = Some(names);
            format!("[{}] confirm host delete...", self.side.label())
        } else {
            let n = names.len();
            self.toggle_delete(&names);
            format!("[{}] toggled delete on {n} item(s).", self.side.label())
        }
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

    /// Mutable access to this pane's staged-edit queue (so the File Info window
    /// can stage metadata edits onto it).
    pub(crate) fn queue_mut(&mut self) -> &mut EditQueue {
        &mut self.queue
    }

    /// The current filesystem type string (e.g. "HFS", "ProDOS", "ext4"); empty
    /// for a host pane or before a source is opened.
    pub(crate) fn fs_type(&self) -> &str {
        &self.fs_type
    }

    /// Look up an entry by name in the current directory and read up to `max`
    /// bytes of its data for the File Info preview. Returns the entry plus its
    /// bytes (`None` for a directory or on read error). Drives `open_detail`.
    pub(crate) fn detail_payload(
        &mut self,
        name: &str,
        max: usize,
    ) -> Option<(FileEntry, Option<Vec<u8>>)> {
        let entry = self
            .listing
            .entries()
            .iter()
            .find(|e| e.name == name)
            .cloned()?;
        if !entry.is_file() {
            return Some((entry, None));
        }
        let data = if self.listing.is_host() {
            read_host_file_capped(&entry.path, max)
        } else {
            self.listing
                .fs_mut()
                .and_then(|fs| fs.read_file(&entry, max).ok())
        };
        Some((entry, data))
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

        // Row 1: source selection + pane lifecycle (Close / Apply / Discard).
        // Kept on its own line so the partition switcher (row 2) never gets
        // squeezed into per-character wrapping on a narrow pane.
        ui.horizontal(|ui| {
            // Shared source picker (R1): the same ComboBox widget the Inspect
            // tab uses, here offering an image file or a host folder. Image
            // opens are not materialized (BrowseSession peels the container).
            let is_backup = self.resolved_backup.is_some();
            let current_label = if let Some(r) = &self.remote {
                match &r.mode {
                    RemoteMode::Host => format!("Remote {} (host)", r.addr),
                    RemoteMode::Image { path, partition } => {
                        let part = partition.map(|n| format!("@{n}")).unwrap_or_default();
                        format!("Remote {}{}{}", r.addr, path, part)
                    }
                }
            } else if self.listing.is_host() {
                "Local folder".to_string()
            } else if is_backup {
                match &self.source {
                    Some(p) => format!(
                        "Backup: {}",
                        p.file_name().unwrap_or_default().to_string_lossy()
                    ),
                    None => "Backup folder".to_string(),
                }
            } else if let Some(p) = &self.source {
                format!(
                    "Image: {}",
                    p.file_name().unwrap_or_default().to_string_lossy()
                )
            } else {
                "Open image, backup, or folder...".to_string()
            };
            let cfg = super::super::source_picker::PickerConfig {
                show_devices: false,
                show_image: true,
                show_host_folder: true,
                show_backup_folder: true,
                materialize_image: false,
                include_mac_archives: false,
                width: 200.0,
            };
            let state = super::super::source_picker::PickerState {
                selected_device_idx: None,
                image_active: self.source.is_some() && !self.listing.is_host() && !is_backup,
                host_active: self.listing.is_host(),
                backup_active: is_backup,
                devices: &[],
            };
            let id = format!("commander_source_{}", self.side.idx());
            if let Some(ev) =
                super::super::source_picker::show(ui, &id, &cfg, &current_label, &state)
            {
                use super::super::source_picker::SourceEvent;
                match ev {
                    SourceEvent::Image { path, .. } => {
                        if self.queue.is_empty() {
                            status = Some(self.load_source(path));
                        } else {
                            self.pending_switch = Some(PendingSwitch::Source(path));
                        }
                    }
                    SourceEvent::HostFolder(dir) => {
                        if self.queue.is_empty() {
                            status = Some(self.load_host(dir));
                        } else {
                            self.pending_switch = Some(PendingSwitch::HostRoot(dir));
                        }
                    }
                    SourceEvent::BackupFolder(folder) => {
                        if self.queue.is_empty() {
                            status = Some(self.load_backup_source(folder));
                        } else {
                            self.pending_switch = Some(PendingSwitch::Backup(folder));
                        }
                    }
                    SourceEvent::Device(_) => {}
                }
            }

            // Browse a disk image on a remote rb-cli serve daemon. Disabled
            // while edits are staged (connecting replaces the listing).
            if ui
                .add_enabled(self.queue.is_empty(), egui::Button::new("Remote..."))
                .on_hover_text("Browse a remote rb-cli serve daemon's filesystem")
                .clicked()
            {
                self.connect_dialog = Some(ConnectDialog {
                    host: self
                        .remote
                        .as_ref()
                        .map(|r| r.addr.clone())
                        .unwrap_or_default(),
                    error: None,
                });
            }

            // Close (unload) this pane — guarded by unsaved staged edits.
            if (self.source.is_some() || self.listing.is_loaded())
                && ui
                    .button("Close")
                    .on_hover_text("Unload this pane")
                    .clicked()
            {
                if self.queue.is_empty() {
                    status = Some(self.close_source());
                } else {
                    self.pending_switch = Some(PendingSwitch::Close);
                }
            }

            // Per-pane staging controls (writable image panes only; host writes
            // are immediate and never staged, and backups are read-only).
            if !self.listing.is_host() && self.resolved_backup.is_none() {
                let n = self.queue.len();
                let busy = self.pending_apply.is_some()
                    || self.pending_open.is_some()
                    || self.pending_scan.is_some();
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
                if n > 0
                    && ui
                        .button(format!("Edits ({n})"))
                        .on_hover_text("Review the pending staged edits")
                        .clicked()
                {
                    self.show_edits_popup = !self.show_edits_popup;
                }
            }
        });

        // Row 2: partition switcher + view toggle + volume readout.
        if self.listing.is_loaded() {
            ui.horizontal(|ui| {
                // Partition dropdown (image panes only; host folders have none).
                if !self.listing.is_host() {
                    let current = self
                        .selected_part
                        .and_then(|i| self.partitions.get(i))
                        .map(partition_label)
                        .unwrap_or_else(|| "(no partitions)".to_string());
                    let mut chosen = self.selected_part;
                    // Cap the width so a long partition label (e.g. a raw-carve
                    // "Amiga NDOS (no filesystem)") can't overflow into the
                    // volume readout on the right.
                    egui::ComboBox::from_id_salt(("commander_part", self.side.idx()))
                        .selected_text(current)
                        .width(210.0)
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

                // New Folder: stages a CreateDirectory on an image pane;
                // creates it immediately on a host pane. Not offered on a
                // read-only backup pane.
                let busy = self.pending_apply.is_some()
                    || self.pending_open.is_some()
                    || self.pending_scan.is_some();
                if !busy
                    && self.resolved_backup.is_none()
                    && ui
                        .button("New Folder")
                        .on_hover_text("Create a folder in the current directory")
                        .clicked()
                {
                    self.new_folder_dialog = Some(NewFolderDialog {
                        input: String::new(),
                        error: None,
                    });
                }

                // Right-aligned volume label + free space.
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if self.listing.is_host() {
                        ui.strong("local folder");
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
            });
        }
        status
    }

    /// Unload the pane back to the empty state (the "Close" button). The caller
    /// guards this behind the unsaved-edits check; here we just reset.
    fn close_source(&mut self) -> String {
        self.source = None;
        self.partitions.clear();
        self.resolved_backup = None;
        self.cache_store.clear();
        self.selected_part = None;
        self.listing = DirListing::new();
        self.session = None;
        self.queue.clear();
        self.pending_open = None;
        self.pending_apply = None;
        self.pending_scan = None;
        self.open_phase.clear();
        self.volume_label.clear();
        self.fs_type.clear();
        self.total_size = 0;
        self.used_size = 0;
        self.error = None;
        self.pending_host_delete = None;
        self.rename_dialog = None;
        format!("[{}] closed.", self.side.label())
    }

    /// The "Pending edits" review popup: a plain-language list of every staged
    /// edit on this pane, in apply order. Opened by the "Edits (N)" button.
    fn render_edits_popup(&mut self, ctx: &egui::Context) {
        if !self.show_edits_popup {
            return;
        }
        // Auto-close once the queue empties (Apply / Discard).
        if self.queue.is_empty() {
            self.show_edits_popup = false;
            return;
        }
        let mut open = true;
        let lines = self.queue.describe();
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
                            ui.label(format!("{}. {line}", i + 1));
                        }
                    });
            });
        if !open {
            self.show_edits_popup = false;
        }
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

    /// Index of the first partition the browser can actually open — skips
    /// extended containers *and* non-filesystem partitions (APM driver /
    /// partition-map entries, EFI system partitions, ...). Uses the shared
    /// engine gate so Commander, the Inspect grid, and the CLI agree.
    fn first_browsable_partition(&self) -> Option<usize> {
        self.partitions.iter().position(|p| {
            !p.is_extended_container
                && partition_is_browsable(
                    p.partition_type_byte,
                    p.partition_type_string.as_deref(),
                    &p.type_name,
                )
        })
    }

    /// Probe a freshly-picked file and start browsing its first real partition.
    fn load_source(&mut self, path: PathBuf) -> String {
        self.source = Some(path.clone());
        self.listing = DirListing::new();
        self.resolved_backup = None;
        self.cache_store.clear();
        self.pending_open = None;
        self.error = None;
        self.selected_part = None;
        self.volume_label.clear();
        self.fs_type.clear();

        match commander_source::probe_partitions(&path) {
            Ok(parts) => {
                self.partitions = parts;
                // Auto-open the first *browsable* partition (skips APM driver /
                // partition-map entries and other non-filesystem partitions).
                match self.first_browsable_partition() {
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

    /// Open a backup folder — native rusty-backup *or* Clonezilla (the shared
    /// resolver detects which) — and start browsing its first browsable
    /// partition. The partition dropdown lists the backed-up partitions;
    /// `open_partition` routes through the resolver to build the right session.
    fn load_backup_source(&mut self, folder: PathBuf) -> String {
        self.source = Some(folder.clone());
        self.listing = DirListing::new();
        self.resolved_backup = None;
        self.cache_store.clear();
        self.partitions.clear();
        self.session = None;
        self.pending_open = None;
        self.pending_apply = None;
        self.queue.clear();
        self.error = None;
        self.selected_part = None;
        self.volume_label.clear();
        self.fs_type.clear();

        match commander_source::resolve_backup(&folder) {
            Ok(resolved) => {
                self.partitions = resolved.partitions().to_vec();
                self.resolved_backup = Some(resolved);
                match self.first_browsable_partition() {
                    Some(i) => self.open_partition(i),
                    None => format!(
                        "[{}] backup has no browsable partitions.",
                        self.side.label()
                    ),
                }
            }
            Err(e) => {
                self.error = Some(format!("Could not read backup: {e:#}"));
                format!(
                    "[{}] failed to open backup {}",
                    self.side.label(),
                    folder.display()
                )
            }
        }
    }

    /// Open a host-OS folder as this pane's source (synchronous `std::fs` read;
    /// writes to a host pane are immediate, never staged).
    fn load_host(&mut self, dir: PathBuf) -> String {
        self.source = Some(dir.clone());
        self.resolved_backup = None;
        self.cache_store.clear();
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

    /// Begin an async open of partition `idx`. Builds a backup-data session
    /// (per compression) when the source is a backup folder, otherwise a
    /// raw-image session.
    fn open_partition(&mut self, idx: usize) -> String {
        let Some(path) = self.source.clone() else {
            return String::new();
        };
        let Some(part) = self.partitions.get(idx).cloned() else {
            return String::new();
        };
        self.selected_part = Some(idx);
        self.error = None;
        self.listing = DirListing::new();
        self.queue.clear();
        self.pending_scan = None;

        // Non-filesystem partitions (APM driver / partition-map entries, EFI,
        // ...) can't be browsed — surface a clear message instead of letting
        // `open_filesystem` fail with a raw "unsupported partition type" error.
        if !partition_is_browsable(
            part.partition_type_byte,
            part.partition_type_string.as_deref(),
            &part.type_name,
        ) {
            let msg = format!("{} has no browsable filesystem.", part.type_name);
            self.error = Some(msg.clone());
            return format!("[{}] {msg}", self.side.label());
        }

        // Resolve the session. A backup folder routes through the shared
        // resolver (native -> per-compression session; Clonezilla -> a partclone
        // block cache from a prior scan); a plain image builds a raw session.
        // `open_partition` returns owned data, so the `resolved_backup` borrow is
        // released before we mutate the pane on the error paths below.
        use commander_source::BackupPartitionOpen;
        let open_result = self
            .resolved_backup
            .as_ref()
            .map(|r| r.open_partition(part.index));
        let session = match open_result {
            Some(Ok(BackupPartitionOpen::Session(s))) => s,
            Some(Ok(BackupPartitionOpen::Clonezilla(open))) => {
                // Shared decision tree: a cache scanned earlier this session is
                // reused in memory; else a prior on-disk scan loads; else a
                // background scan builds one (`poll_scan` memoizes + opens it).
                match self.cache_store.resolve(part.index, &open) {
                    commander_source::PartcloneLookup::Ready(cache) => {
                        commander_source::session_for_partclone_cache(cache, open.partition_type)
                    }
                    commander_source::PartcloneLookup::NeedsScan { cache, cache_path } => {
                        self.pending_scan = Some(cache_runner::spawn_partclone_scan(
                            cache,
                            part.index,
                            open.partition_type,
                            cache_path,
                        ));
                        self.open_phase = "Scanning Clonezilla metadata...".to_string();
                        return format!(
                            "[{}] scanning Clonezilla metadata for {} ...",
                            self.side.label(),
                            partition_label(&part)
                        );
                    }
                }
            }
            Some(Err(e)) => {
                self.error = Some(format!("{e:#}"));
                return format!(
                    "[{}] cannot browse {}: {e}",
                    self.side.label(),
                    partition_label(&part)
                );
            }
            None => commander_source::session_for(&path, &part),
        };
        self.pending_open = Some(session.spawn_open());
        self.session = Some(session);
        self.open_phase = "Opening...".to_string();
        format!(
            "[{}] opening {} ...",
            self.side.label(),
            partition_label(&part)
        )
    }

    /// Poll an in-flight Clonezilla metadata scan; on completion, build a
    /// partclone-cache session and hand off to the normal async-open path (so
    /// `poll_open` loads the root listing). Returns a status line on completion.
    fn poll_scan(&mut self, ctx: &egui::Context) -> Option<String> {
        let arc = self.pending_scan.clone()?;
        ctx.request_repaint();
        let mut guard = arc.lock().ok()?;
        if !guard.finished {
            return None;
        }
        self.pending_scan = None;
        if let Some(err) = guard.error.take() {
            drop(guard);
            self.error = Some(format!("Clonezilla metadata scan failed: {err}"));
            return Some(format!("[{}] Clonezilla scan failed.", self.side.label()));
        }
        let cache = Arc::clone(&guard.cache);
        let partition_type = guard.partition_type;
        let part_index = guard.partition_index;
        drop(guard);
        // Memoize the freshly-scanned cache so re-opening this partition reuses
        // it in memory instead of re-loading the persisted scan from disk.
        self.cache_store.insert(part_index, Arc::clone(&cache));
        let session = commander_source::session_for_partclone_cache(cache, partition_type);
        self.open_phase = "Opening...".to_string();
        self.pending_open = Some(session.spawn_open());
        self.session = Some(session);
        Some(format!(
            "[{}] metadata ready; opening...",
            self.side.label()
        ))
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
        let msg = format!("[{}] applied {n} edit(s).", self.side.label());
        self.log_events.push(msg.clone());
        Some(msg)
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
            } else {
                // Deleting supersedes a staged rename of the same entry (else
                // apply would rename it, then fail to delete the old name).
                self.queue.remove_pending_rename(&entry.path);
                if entry.is_directory() {
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

    fn render_rows(&mut self, ui: &mut egui::Ui) -> RowActions {
        let rows = self.build_display_rows();
        let busy = self.pending_apply.is_some();
        // Host panes write immediately (Delete removes now); image panes stage
        // (Delete / Undelete / Remove-from-staging go on the queue).
        let host_pane = self.listing.is_host();
        // A read-only backup pane offers browse / copy-out actions only — no
        // delete or rename (both would mutate the backup).
        let read_only = self.resolved_backup.is_some();
        // On a remote *host* pane, a file can be opened as a disk image.
        let remote_host = self.is_remote_host();

        let mut to_enter: Option<String> = None;
        let mut to_up = false;
        let mut click: Option<(String, bool, bool)> = None;
        let mut bg_deselect = false;
        let mut ctx_rclick: Option<String> = None;
        let mut m_delete = false;
        let mut m_host_delete = false;
        let mut m_copy = false;
        let mut m_export = false;
        let mut m_checksums = false;
        let mut m_rename: Option<String> = None;
        let mut m_cancel_rename: Option<String> = None;
        let mut m_info: Option<String> = None;
        let mut m_open_image: Option<String> = None;

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
                        } else if remote_host {
                            // On a remote host pane, double-click a file opens it
                            // as a disk image (browse inside it).
                            m_open_image = Some(row.name.clone());
                        } else {
                            // Double-click a file -> open its File Info window.
                            m_info = Some(row.name.clone());
                        }
                    } else if resp.clicked() {
                        // A single click on ".." does nothing — navigate up on
                        // double-click, consistent with folders (which enter on
                        // double-click). A single click navigating immediately
                        // also pre-empted any double-click on the row.
                        if !row.is_parent() {
                            click = Some((row.name.clone(), mods.command, mods.shift));
                        }
                    }

                    // Right-click a data row for its actions.
                    if !row.is_parent() && !busy {
                        let menu = resp.context_menu(|ui| {
                            // On a remote host pane, offer to open a file as an
                            // image (browse inside it).
                            if remote_host && !row.is_dir && ui.button("Open Image").clicked() {
                                m_open_image = Some(row.name.clone());
                                ui.close();
                            }
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
                            // Checksums need real data, so a not-yet-applied
                            // staged add is excluded (same as Copy / Export).
                            if !matches!(row.kind, RowKind::PendingAdd)
                                && ui.button("Calculate checksums...").clicked()
                            {
                                m_checksums = true;
                                ui.close();
                            }
                            // File Info / Details: metadata + preview, with the
                            // editable-metadata subset on image panes. Needs
                            // real data, so a not-yet-applied add is excluded.
                            if !matches!(row.kind, RowKind::PendingAdd)
                                && ui.button("File Info / Details...").clicked()
                            {
                                m_info = Some(row.name.clone());
                                ui.close();
                            }
                            // Rename a single entry in place. A row with a
                            // rename already staged offers to cancel it instead.
                            // Suppressed on a read-only backup pane.
                            if !read_only {
                                if matches!(row.kind, RowKind::PendingRename)
                                    && ui.button("Cancel rename").clicked()
                                {
                                    m_cancel_rename = Some(row.name.clone());
                                    ui.close();
                                } else if matches!(row.kind, RowKind::Normal)
                                    && ui.button("Rename...").clicked()
                                {
                                    m_rename = Some(row.name.clone());
                                    ui.close();
                                }
                            }
                            // Delete (immediate on host, staged on image).
                            // Suppressed on a read-only backup pane.
                            if read_only {
                                // browse / copy-out only
                            } else if host_pane {
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

        // Any row interaction marks this pane "active" for the middle Delete.
        let focused =
            click.is_some() || ctx_rclick.is_some() || to_enter.is_some() || to_up || bg_deselect;

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
        if let Some(name) = m_rename {
            if let Some(entry) = self
                .listing
                .entries()
                .iter()
                .find(|e| e.name == name)
                .cloned()
            {
                self.rename_dialog = Some(RenameDialog {
                    input: entry.name.clone(),
                    entry,
                    error: None,
                });
            }
        }
        if let Some(name) = m_cancel_rename {
            if let Some(path) = self
                .listing
                .entries()
                .iter()
                .find(|e| e.name == name)
                .map(|e| e.path.clone())
            {
                self.queue.remove_pending_rename(&path);
                status = Some(format!("[{}] cancelled staged rename.", self.side.label()));
            }
        }
        RowActions {
            status,
            copy: m_copy,
            export: m_export,
            checksums: m_checksums,
            detail: m_info,
            open_image: m_open_image,
            focused,
        }
    }
}

/// Read up to `max` bytes from a host file for the File Info preview.
/// Returns `None` on any I/O error (the window then shows metadata only).
fn read_host_file_capped(path: &str, max: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    let file = std::fs::File::open(path).ok()?;
    let mut buf = Vec::new();
    file.take(max as u64).read_to_end(&mut buf).ok()?;
    Some(buf)
}

/// How a row participates in the staged-edit overlay.
#[derive(Clone, Copy, PartialEq)]
enum RowKind {
    Parent,
    Normal,
    PendingDelete,
    PendingAdd,
    PendingRename,
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
    /// For a `PendingRename` row, the staged new name (shown as `old -> new`).
    rename_to: Option<String>,
    /// A metadata edit (type/creator, dates, permissions, ...) is staged for
    /// this existing entry — tints the row blue.
    meta_changed: bool,
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
                    rename_to: None,
                    meta_changed: false,
                },
                Row::Entry(e) => {
                    let rename_to = self.queue.pending_rename_for(&e.path).map(str::to_string);
                    let kind = if self.queue.is_pending_delete(&e.path) {
                        RowKind::PendingDelete
                    } else if rename_to.is_some() {
                        RowKind::PendingRename
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
                        rename_to,
                        meta_changed: self.queue.has_pending_metadata(&e.path),
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
                rename_to: None,
                meta_changed: false,
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
        RowKind::PendingRename => ADD_COLOR,
        RowKind::PendingDelete => DEL_COLOR,
        RowKind::Normal if row.meta_changed => META_COLOR,
        RowKind::Normal if row.is_dir => egui::Color32::from_rgb(120, 160, 255),
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
