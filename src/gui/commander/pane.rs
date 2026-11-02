//! One Commander pane: a source bar (open image / folder + partition picker +
//! Apply/Discard), a path line, and a flat single-directory listing grid with
//! sortable columns, multi-selection, `..` / double-click navigation, and a
//! per-pane staged-edit queue (delete + copy-in, shown in the virtual overlay).
//!
//! What the pane shows and how it opens (host folder, disk image, backup folder,
//! Mac archive, optical disc, as jobs) lives in [`CommanderPaneController`]; this
//! file draws it, maps input to its actions, and answers its pickers and
//! confirmations and text prompts through an [`EffectHost`]. Staging, Apply, the
//! review and report, host writes, wrapper expansion and the remote connection are
//! controller state this file draws.
//!
//! Image panes stage edits (Apply writes through); host panes take immediate
//! writes and never stage; backup panes are read-only (browse + copy-out only).
//! The cross-pane copy itself lives in [`super::CommanderMode`] (it spans both
//! panes).

use std::path::PathBuf;
use std::sync::Arc;

use eframe::egui;

use disk_core::display::format_size;
use disk_core::entry::FileEntry;
use disk_core::filesystem::Filesystem;
use rusty_backup::fs::export_selection::ExportFormat;
use rusty_backup::model::commander_ops;
use rusty_backup::model::edit_queue::{EditQueue, StagedEdit};
use rusty_backup::model::job::{JobRegistry, Snapshot};

use rusty_backup::controller::app::{CommanderRecent, UserConfig};
use rusty_backup::controller::commander::rows::{DisplayRow, RowKind};
use rusty_backup::controller::commander::{
    CommanderPaneAction, CommanderPaneController, OnConflict, PaneRequest, PaneServices,
    SortColumn, StageOutcome, MAX_CONSECUTIVE_FAILURES,
};
use rusty_backup::controller::Controller;

use super::super::effects::{EffectHost, Reply};
use super::Side;

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

/// What a pane reports back to [`super::CommanderMode`] after a frame.
#[derive(Default)]
pub(crate) struct PaneResponse {
    /// A status line for the overlay's bottom bar, if anything happened.
    pub status: Option<String>,
    /// The user asked (via the row menu) to copy this pane's selection to the
    /// other pane; `CommanderMode` performs the cross-pane copy.
    pub copy_to_other: bool,
    /// The user asked (via the row menu) to export this pane's selection to the
    /// host using the middle-column "Export as" format; `CommanderMode` runs the
    /// threaded write.
    pub export_to_host: bool,
    /// The user picked a specific format from the row menu's "Export" submenu,
    /// overriding the dropdown for this one export.
    pub export_as: Option<ExportFormat>,
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
    /// Export the selection to the host using the dropdown's "Export as" format.
    export: bool,
    /// Export the selection in a format picked from the "Export" submenu.
    export_as: Option<ExportFormat>,
    /// Calculate checksums for the selection.
    checksums: bool,
    /// Open the File Info window for this entry (by name).
    detail: Option<String>,
    /// A row in this pane was clicked/navigated this frame (focus signal).
    focused: bool,
}

pub(crate) struct CommanderPane {
    /// Source, listing, staged edits and wrapper tree, shared with every shell.
    ctl: CommanderPaneController,
    /// Pickers and confirmations the controller asks for, answered back to it.
    effects: EffectHost,
    /// Status lines from the controller's messages, shown at the end of the frame.
    statuses: Vec<String>,
    side: Side,
    /// The review window's conflict choice until the user presses Apply.
    review_choice: OnConflict,
    /// The user's in-progress password / passphrase entry for the unlock prompt.
    password_input: String,
    /// Data-changing operations completed this frame, drained into the
    /// `PaneResponse` at the end of `show()` for the session log.
    log_events: Vec<String>,
    /// Keyboard actions for this pane, run at the start of the next row render.
    key_actions: Vec<CommanderPaneAction>,
    /// This is the active pane, so it draws the keyboard cursor.
    has_focus: bool,
    /// Rows that fit in the listing, for Page Up / Page Down.
    page_rows: i32,
}

impl CommanderPane {
    /// The rows to draw, from the controller.
    fn display_rows(&mut self) -> Vec<DisplayRow> {
        self.ctl.display_rows()
    }

    pub(crate) fn new(side: Side, jobs: JobRegistry) -> Self {
        let services = PaneServices {
            jobs,
            notify: Arc::new(super::super::ui_logger::EguiWake),
            recent: Box::new(CommanderRecent(UserConfig)),
        };
        Self {
            side,
            ctl: CommanderPaneController::with_services(services),
            effects: EffectHost::default(),
            statuses: Vec::new(),
            review_choice: OnConflict::Skip,
            password_input: String::new(),
            log_events: Vec::new(),
            key_actions: Vec::new(),
            has_focus: false,
            page_rows: 1,
        }
    }

    /// Run a source action and its effects, answering pickers straight back to the controller.
    fn act(&mut self, ctx: &egui::Context, action: CommanderPaneAction) {
        let fx = self.ctl.apply(action);
        self.run_effects(ctx, fx);
    }

    fn run_effects(&mut self, ctx: &egui::Context, fx: Vec<rusty_backup::controller::Effect>) {
        let mut messages = Vec::new();
        let replies = self.effects.run(ctx, fx, &mut messages);
        let side = self.side.label();
        self.statuses
            .extend(messages.into_iter().map(|m| format!("[{side}] {m}")));
        self.handle_requests();
        for reply in replies {
            self.answer(ctx, reply);
        }
    }

    fn answer(&mut self, ctx: &egui::Context, reply: Reply) {
        let action = match reply {
            Reply::Picked { purpose, path } => CommanderPaneAction::Picked { purpose, path },
            Reply::Confirmed { purpose, yes } => CommanderPaneAction::Confirmed { purpose, yes },
            Reply::Answered { purpose, text } => CommanderPaneAction::Answered { purpose, text },
        };
        self.act(ctx, action);
    }

    /// Requests the controller leaves to the view (File Info).
    fn handle_requests(&mut self) {
        let rest = self.ctl.take_requests();
        if !rest.is_empty() {
            let mut out = RowActions::default();
            self.handle_row_requests(rest, &mut out);
            if let Some(s) = out.status {
                self.statuses.push(s);
            }
        }
    }

    /// True when the active selection is inside an expanded wrapper. In that
    /// state the pane's copy source is the mounted wrapper filesystem rather
    /// than the base listing — `selected_entries` / `fs_mut` / `is_host_pane` /
    /// `session` all switch to the wrapper context so the existing copy
    /// dispatch pulls files straight out.
    fn tree_active(&self) -> bool {
        self.ctl.tree_active()
    }

    fn tree_selection(&self) -> Option<(String, Vec<FileEntry>)> {
        self.ctl.tree_selection()
    }

    /// Drop this pane's in-memory recent-files mirror (the Settings dialog
    /// cleared the persisted lists).
    pub(crate) fn clear_recent_files(&mut self) {
        self.ctl.apply(CommanderPaneAction::RecentCleared);
    }

    /// Number of staged (unapplied) edits on this pane.
    pub(crate) fn staged_count(&self) -> usize {
        self.ctl.queue.len()
    }

    /// Discard all staged edits (used by the overlay's Close guard).
    pub(crate) fn discard_edits(&mut self) {
        self.ctl.queue.clear();
    }

    /// Render the pane. Returns status + any cross-pane request for the overlay.
    pub(crate) fn show(&mut self, ui: &mut egui::Ui, active: bool) -> PaneResponse {
        self.has_focus = active;
        let fx = self.ctl.tick();
        self.run_effects(ui.ctx(), fx);
        let mut status = None;
        let mut cancel_open = false;
        let mut copy_to_other = false;
        let mut export_to_host = false;
        let mut export_as: Option<ExportFormat> = None;
        let mut checksums = false;
        let mut detail = None;
        let mut focused = false;

        if let Some(s) = self.source_bar(ui) {
            status = Some(s);
        }
        if let Some(s) = self.path_line(ui) {
            status = Some(s);
        }
        ui.separator();

        // A wrapper expanding as a job: a spinner and its phase above the still-visible listing.
        if let Some((name, snap)) = self.ctl.wrapper_opening() {
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

        if self.ctl.checking().is_some() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label("Checking staged edits...");
            });
            ui.ctx().request_repaint();
        }
        if let Some(snap) = self.ctl.host_writing() {
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                super::super::progress::show_job(ui, &snap);
            });
        }
        if self.ctl.applying().is_some() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label("Applying staged edits...");
            });
        } else if let Some(snap) = self.ctl.opening() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                if super::super::progress::show_job(ui, &snap) {
                    cancel_open = true;
                }
            });
        } else if self.ctl.is_opening() {
            ui.add_space(20.0);
            ui.horizontal(|ui| {
                ui.add_space(8.0);
                ui.spinner();
                ui.label("Opening...");
            });
        } else if self.ctl.needs_password() {
            // Unlock prompt for an encrypted source; its copy differs for a locked APFS FileVault volume.
            let is_apfs = self.ctl.volume().fs_type == "APFS";
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
                self.act(ui.ctx(), CommanderPaneAction::SubmitPassword(secret));
            }
        } else if let Some(err) = self.ctl.source_error().map(str::to_string) {
            ui.add_space(12.0);
            ui.colored_label(super::super::theme::danger_muted(ui.visuals()), err);
        } else if self.ctl.listing.is_loaded() {
            self.render_header(ui);
            let actions = self.render_rows(ui);
            if actions.status.is_some() {
                status = actions.status;
            }
            copy_to_other = actions.copy;
            export_to_host = actions.export;
            export_as = actions.export_as;
            checksums = actions.checksums;
            detail = actions.detail;
            focused = actions.focused;
        } else {
            ui.centered_and_justified(|ui| {
                ui.weak("Open a disk image or container to browse it here.");
            });
        }

        if cancel_open {
            self.act(ui.ctx(), CommanderPaneAction::CancelOpen);
        }
        let side = self.side.label();
        if let Some(reply) = self.effects.show(ui.ctx(), side) {
            self.answer(ui.ctx(), reply);
        }
        self.render_edits_popup(ui.ctx());
        self.render_apply_review(ui.ctx());
        if let Some(s) = self.render_apply_report(ui.ctx()) {
            status = Some(s);
        }

        self.key_actions.clear();
        if let Some(s) = self.take_status() {
            status = Some(s);
        }
        for line in self.ctl.take_log() {
            self.log_events.push(format!("[{side}] {line}"));
        }
        PaneResponse {
            status,
            copy_to_other,
            export_to_host,
            export_as,
            checksums,
            detail,
            focused,
            log_events: std::mem::take(&mut self.log_events),
        }
    }

    /// True when this pane is browsing a remote daemon's *host* filesystem (the
    /// file browser), where a file can be opened as an image.
    pub(crate) fn is_remote_host(&self) -> bool {
        self.ctl.is_remote_host()
    }

    /// The daemon address (`host:port`) this pane is connected to, if remote.
    /// Used to open a fresh connection for an upload worker.
    pub(crate) fn remote_addr(&self) -> Option<String> {
        self.ctl.remote_addr()
    }

    // --- accessors used by CommanderMode for cross-pane copy ---------------

    /// True when this pane can receive a copy: something is open, nothing runs, and it is writable.
    pub(crate) fn can_receive(&self) -> bool {
        self.ctl.can_receive()
    }

    /// Read-only media (archive, optical disc, backup), for the header's "Read-only" badge.
    pub(crate) fn is_read_only(&self) -> bool {
        self.ctl.is_read_only()
    }

    /// Why a copy into this pane is refused, for the copy button's hover.
    pub(crate) fn readonly_reason(&self) -> Option<&'static str> {
        self.ctl.readonly_reason()
    }

    /// True when this pane lists a host-OS folder rather than a disk image.
    /// When a wrapper selection is active the effective source is the mounted
    /// wrapper filesystem (not the host), so report `false`.
    pub(crate) fn is_host_pane(&self) -> bool {
        !self.tree_active() && self.ctl.listing.is_host()
    }

    /// True when this pane lists a (read-only) backup folder's contents — native
    /// rusty-backup or Clonezilla. Backups can be browsed and copied *out of*,
    /// but not edited: writing to a raw backup's partition file would silently
    /// desync its `metadata.json` checksum, zstd backups have no in-place
    /// editable session, and Clonezilla images are read-through a block cache.
    pub(crate) fn is_backup_pane(&self) -> bool {
        self.ctl.is_backup()
    }

    /// True when this pane's source is a read-only Mac archive
    /// (`ArchiveFilesystem`). Like a backup pane, it can be browsed and copied
    /// out of, but not edited or copied into.
    pub(crate) fn is_archive_pane(&self) -> bool {
        self.ctl.is_archive()
    }

    /// How a copy or checksum worker reopens this pane's selection source.
    pub(crate) fn copy_stage_source(&self) -> Option<commander_ops::StageSource> {
        self.ctl.copy_stage_source()
    }

    /// The local apply job, for the progress modal.
    pub(crate) fn apply_job(&self) -> Option<Snapshot> {
        self.ctl.applying()
    }

    /// Re-read the current directory listing (after an immediate host write).
    pub(crate) fn reload_listing(&mut self) {
        self.ctl.apply(CommanderPaneAction::Reload);
        self.ctl.take_error();
    }

    /// Delete the selection (the middle-column Delete): staged on an image, confirmed then immediate on a host.
    pub(crate) fn delete_selection(&mut self, ctx: &egui::Context) -> String {
        self.act(ctx, CommanderPaneAction::Delete);
        self.take_status().unwrap_or_default()
    }

    /// Queues a keyboard action for this pane; it runs when the listing next renders.
    pub(crate) fn key_action(&mut self, action: CommanderPaneAction) {
        self.key_actions.push(action);
    }

    /// Rows that fit in the listing, the step for Page Up / Page Down.
    pub(crate) fn page_rows(&self) -> i32 {
        self.page_rows
    }

    /// True while one of this pane's dialogs waits for an answer, so shortcuts stand down.
    pub(crate) fn modal_open(&self) -> bool {
        self.effects.modal_open()
            || self.ctl.apply_review().is_some()
            || self.ctl.apply_report().is_some()
    }

    /// The Apply button and F10.
    pub(crate) fn apply_now(&mut self, ctx: &egui::Context) -> String {
        if !self.ctl.can_apply() {
            return String::new();
        }
        self.act(ctx, CommanderPaneAction::Apply);
        self.take_status().unwrap_or_default()
    }

    /// A pane action from the keyboard or a button whose effects must run now (prompts, confirms).
    pub(crate) fn command(&mut self, ctx: &egui::Context, action: CommanderPaneAction) -> String {
        self.act(ctx, action);
        self.take_status().unwrap_or_default()
    }

    /// True when at least one row is selected (base listing or a wrapper).
    pub(crate) fn has_selection(&self) -> bool {
        self.tree_active() || !self.ctl.listing.selection().is_empty()
    }

    /// The selected entries (owned clones) — from the active wrapper mount when
    /// a tree selection is live, otherwise the base current directory.
    pub(crate) fn selected_entries(&self) -> Vec<FileEntry> {
        if let Some((_, entries)) = self.tree_selection() {
            return entries;
        }
        self.ctl
            .listing
            .selected_entries()
            .into_iter()
            .cloned()
            .collect()
    }

    /// The current directory entry (copy destination parent).
    pub(crate) fn cwd_entry(&self) -> Option<FileEntry> {
        self.ctl.listing.cwd().cloned()
    }

    /// Mutable access to the open filesystem (to extract files for a copy).
    /// Returns the active wrapper mount when a tree selection is live.
    pub(crate) fn fs_mut(&mut self) -> Option<&mut (dyn Filesystem + 'static)> {
        if let Some((mount, _)) = self.tree_selection() {
            return self.ctl.wrapper_tree.mount_fs(&mount);
        }
        self.ctl.listing.fs_mut()
    }

    /// Mutable access to this pane's staged-edit queue (so the File Info window
    /// can stage metadata edits onto it).
    pub(crate) fn queue_mut(&mut self) -> &mut EditQueue {
        &mut self.ctl.queue
    }

    /// The current filesystem type string (e.g. "HFS", "ProDOS", "ext4"); empty
    /// for a host pane or before a source is opened.
    pub(crate) fn fs_type(&self) -> &str {
        &self.ctl.volume().fs_type
    }

    /// Look up an entry by name in the current directory and read up to `max`
    /// bytes of its data for the File Info preview. Returns the entry plus its
    /// bytes (`None` for a directory or on read error). Drives `open_detail`.
    pub(crate) fn detail_payload(
        &mut self,
        name: &str,
        max: usize,
    ) -> Option<(FileEntry, Option<Vec<u8>>)> {
        // A wrapper-tree row (a file inside an expanded .sit / .toast / disk-image
        // wrapper): resolve against the active tree selection — a right-click
        // selects the row first — and read through the mounted wrapper fs.
        if self.tree_active() {
            let (_, entries) = self.tree_selection()?;
            let entry = entries.into_iter().find(|e| e.name == name)?;
            if !entry.is_file() {
                return Some((entry, None));
            }
            let data = self.fs_mut().and_then(|fs| fs.read_file(&entry, max).ok());
            return Some((entry, data));
        }
        let entry = self
            .ctl
            .listing
            .entries()
            .iter()
            .find(|e| e.name == name)
            .cloned()?;
        if !entry.is_file() {
            return Some((entry, None));
        }
        let data = if self.ctl.listing.is_host() {
            read_host_file_capped(&entry.path, max)
        } else {
            self.ctl
                .listing
                .fs_mut()
                .and_then(|fs| fs.read_file(&entry, max).ok())
        };
        Some((entry, data))
    }

    /// True when an expanded-wrapper row is the active selection. Its contents
    /// are read-only (copy out, don't edit), so File Info shows no editors.
    pub(crate) fn wrapper_selection_active(&self) -> bool {
        self.tree_active()
    }

    /// The filesystem type to show in File Info: the mounted wrapper's type
    /// when a tree selection is active (e.g. "HFS" for a file inside a `.toast`
    /// opened through a `.sit`), else the base pane's type.
    /// The on-disk extended attributes of `entry`, for the File Info window.
    /// Empty on a filesystem without xattr support (or when unreadable) — this
    /// is display metadata, never a hard failure.
    pub(crate) fn detail_xattrs(&mut self, entry: &FileEntry) -> Vec<disk_core::xattr::Xattr> {
        self.fs_mut()
            .filter(|fs| fs.supports_xattrs())
            .and_then(|fs| fs.list_xattrs(entry).ok())
            .unwrap_or_default()
    }

    pub(crate) fn detail_fs_type(&mut self) -> String {
        if let Some((mount, _)) = self.tree_selection() {
            if let Some(fs) = self.ctl.wrapper_tree.mount_fs(&mount) {
                return fs.fs_type().to_string();
            }
        }
        self.fs_type().to_string()
    }

    /// True when this pane is browsing a remote daemon (host FS or an image on
    /// it). Cross-pane callers use this to gate in-place edits that the remote
    /// write path doesn't carry yet (delete / rename over the wire).
    pub(crate) fn is_remote(&self) -> bool {
        self.ctl.is_remote()
    }

    /// Open a drag-and-dropped path as this pane's source (drag-and-drop router).
    /// A directory opens as a host folder, a file as an image; staged edits defer
    /// the switch behind the discard-confirmation, mirroring the source picker.
    pub(crate) fn open_dropped(&mut self, ctx: &egui::Context, path: PathBuf) -> String {
        self.act(ctx, CommanderPaneAction::OpenPath(path));
        if self.effects.modal_open() {
            return format!(
                "[{}] discard staged edits to open a new source.",
                self.side.label()
            );
        }
        self.take_status().unwrap_or_default()
    }

    /// The newest controller message, dropping older ones from the same frame.
    fn take_status(&mut self) -> Option<String> {
        let last = self.statuses.pop();
        self.statuses.clear();
        last
    }

    /// Push a copy batch onto this pane's queue, renaming names this volume can't hold.
    pub(crate) fn stage_edits(&mut self, edits: Vec<StagedEdit>) -> StageOutcome {
        self.ctl.stage_edits(edits)
    }

    /// Drop everything staged since `queue_start` (the rename notice's "Undo copy").
    pub(crate) fn unstage_since(&mut self, queue_start: usize) -> bool {
        self.ctl.unstage_since(queue_start)
    }

    // --- source bar --------------------------------------------------------

    fn source_bar(&mut self, ui: &mut egui::Ui) -> Option<String> {
        use CommanderPaneAction as A;
        let mut status = None;
        let mut source_action = None;
        let mut apply = false;

        // Row 1: source selection + pane lifecycle (Close / Apply / Discard).
        // Wrapped so the picker + lifecycle buttons flow onto a second line on a
        // narrow pane instead of overflowing past the pane edge.
        ui.horizontal_wrapped(|ui| {
            // Shared source picker (R1): the same ComboBox widget the Inspect
            // tab uses, here offering an image file or a host folder. Image
            // opens are not materialized (BrowseSession peels the container).
            let is_backup = self.ctl.is_backup();
            let source = self.ctl.source_path().map(|p| p.to_path_buf());
            let current_label = if let Some(label) = self.ctl.remote_label() {
                label
            } else if self.ctl.listing.is_host() {
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
                if self.ctl.is_archive() {
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
                image_active: source.is_some() && !self.ctl.listing.is_host() && !is_backup,
                host_active: self.ctl.listing.is_host(),
                backup_active: is_backup,
                devices: &[],
                // Merged recent list (newest 3 per mode) shown as a group inside
                // this same dropdown — no extra combo crowding the source bar.
                recent: self.ctl.recent(),
            };
            let id = format!("commander_source_{}", self.side.idx());
            if let Some(ev) =
                super::super::source_picker::show(ui, &id, &cfg, &current_label, &state)
            {
                use super::super::source_picker::SourceEvent;
                match ev {
                    // A recent pick opens like a drag-and-drop: a folder as a host folder, a file as an image.
                    SourceEvent::Recent(path) => source_action = Some(A::OpenPath(path)),
                    SourceEvent::Image { path, .. } => source_action = Some(A::OpenImage(path)),
                    SourceEvent::HostFolder(dir) => source_action = Some(A::OpenHostFolder(dir)),
                    SourceEvent::BackupFolder(dir) => {
                        source_action = Some(A::OpenBackupFolder(dir))
                    }
                    SourceEvent::PickImage => source_action = Some(A::PickImage),
                    SourceEvent::PickHostFolder => source_action = Some(A::PickHostFolder),
                    SourceEvent::PickBackupFolder => source_action = Some(A::PickBackupFolder),
                    SourceEvent::Device(_) => {}
                    SourceEvent::Remote => source_action = Some(A::Connect),
                }
            }

            // When inside a remote image, "Close Image" steps back to the host
            // browser (the folder it was opened from) — distinct from "Close",
            // which unloads the whole connection.
            if self.ctl.is_remote_image() {
                let btn = egui::Button::new(
                    egui::RichText::new("Close Image").color(close_image_color(ui.visuals())),
                );
                if ui
                    .add_enabled(self.ctl.queue.is_empty(), btn)
                    .on_hover_text("Back to browsing the remote host filesystem")
                    .clicked()
                {
                    source_action = Some(A::CloseRemoteImage);
                }
            }

            // Close (unload) this pane — guarded by unsaved staged edits.
            if (source.is_some() || self.ctl.listing.is_loaded())
                && ui
                    .button("Close")
                    .on_hover_text("Unload this pane / disconnect")
                    .clicked()
            {
                source_action = Some(A::CloseSource);
            }

            // Per-pane staging controls (writable image panes only; host writes
            // are immediate and never staged, and backups / archives are
            // read-only).
            if self.ctl.stages_edits() {
                let n = self.ctl.queue.len();
                let busy = self.ctl.is_busy();
                ui.add_enabled_ui(n > 0 && !busy, |ui| {
                    if ui.button(format!("Apply ({n})")).clicked() {
                        apply = true;
                    }
                });
                ui.add_enabled_ui(n > 0 && !busy, |ui| {
                    if ui.button("Discard").clicked() {
                        source_action = Some(A::Discard);
                    }
                });
                if n > 0
                    && ui
                        .button(format!("Edits ({n})"))
                        .on_hover_text("Review the pending staged edits")
                        .clicked()
                {
                    source_action = Some(A::ShowEdits(!self.ctl.edits_shown()));
                }
            }
        });

        // Row 2: partition switcher + New Folder + volume readout. Wrapped so on
        // a narrow pane the volume readout flows onto a second line instead of
        // overlapping the New Folder button.
        if self.ctl.listing.is_loaded() {
            ui.horizontal_wrapped(|ui| {
                // Filesystem / partition dropdown (image panes only; host
                // folders have none). An optical disc lists its filesystems
                // (ISO 9660 + any hybrid Mac side); every other image lists its
                // partition-table entries.
                if !self.ctl.listing.is_host() {
                    let labels = self.ctl.partition_labels();
                    let empty_text = if self.ctl.is_optical() {
                        "(no filesystems)"
                    } else {
                        "(no partitions)"
                    };
                    let selected = self.ctl.selected_partition();
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
                            source_action = Some(A::SelectPartition(i));
                        }
                    }
                }

                // New Folder: stages a CreateDirectory on an image pane;
                // creates it immediately on a host pane. Not offered on a
                // read-only backup pane.
                if self.ctl.can_new_folder()
                    && ui
                        .button("New Folder")
                        .on_hover_text("Create a folder in the current directory")
                        .clicked()
                {
                    source_action = Some(A::RequestNewFolder);
                }

                // Volume label + free space. Rendered inline (not right-aligned)
                // so on a narrow pane it wraps to a second line rather than
                // overflowing left into the New Folder button.
                ui.separator();
                if self.ctl.listing.is_host() {
                    ui.strong("local folder");
                } else {
                    let vol = self.ctl.volume().clone();
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
                    if self.is_read_only() {
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
            self.act(ui.ctx(), action);
        }
        if apply {
            status = Some(self.apply_now(ui.ctx()));
        }
        status
    }

    /// The "Pending edits" list: every staged edit in plain words, in apply order, each removable.
    fn render_edits_popup(&mut self, ctx: &egui::Context) {
        if !self.ctl.edits_shown() {
            return;
        }
        let mut open = true;
        let mut unstage = None;
        let busy = self.ctl.is_busy();
        let lines = self.ctl.edit_lines();
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
                                    unstage = Some(i);
                                }
                                ui.label(format!("{}. {line}", i + 1));
                            });
                        }
                    });
            });
        if let Some(i) = unstage {
            self.act(ctx, CommanderPaneAction::Unstage(i));
        }
        if !open {
            self.act(ctx, CommanderPaneAction::ShowEdits(false));
        }
    }

    /// The partition tag and the cwd as crumbs; clicking a crumb navigates there.
    fn path_line(&mut self, ui: &mut egui::Ui) -> Option<String> {
        let mut go: Option<String> = None;
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 0.0;
            let prefix = if self.ctl.listing.is_host() {
                "host ".to_string()
            } else {
                match self.ctl.selected_partition() {
                    Some(i) => format!("@{} ", i + 1),
                    None => "- ".to_string(),
                }
            };
            ui.monospace(prefix);
            let crumbs = self.ctl.path_crumbs();
            if crumbs.is_empty() {
                ui.monospace("/");
            }
            let last = crumbs.len().saturating_sub(1);
            for (i, (label, target)) in crumbs.into_iter().enumerate() {
                let text = egui::RichText::new(label).monospace();
                match target {
                    Some(t) if i < last => {
                        if ui.link(text).clicked() {
                            go = Some(t);
                        }
                    }
                    _ => {
                        ui.label(text);
                    }
                }
            }
        });
        let mut out = RowActions::default();
        let ctx = ui.ctx().clone();
        self.dispatch(&ctx, CommanderPaneAction::NavigateTo(go?), &mut out);
        out.status
    }

    // --- apply: the review and report are views over controller state ------

    fn render_apply_review(&mut self, ctx: &egui::Context) {
        let Some(p) = self.ctl.apply_review() else {
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
        let action = match go {
            Some(true) => CommanderPaneAction::ApplyReviewed(self.review_choice),
            Some(false) => CommanderPaneAction::DismissReview,
            None => return,
        };
        self.act(ctx, action);
    }

    fn render_apply_report(&mut self, ctx: &egui::Context) -> Option<String> {
        let report = self.ctl.apply_report()?;
        let danger = super::super::theme::danger_muted(&ctx.global_style().visuals);
        let still = self.ctl.queue.len();
        let mut action = None;
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
                        action = Some(0);
                    }
                    if still > 0
                        && ui
                            .button("Discard remaining")
                            .on_hover_text("Drop the edits that did not apply")
                            .clicked()
                    {
                        action = Some(1);
                    }
                    if ui.button("Close").clicked() {
                        action = Some(2);
                    }
                });
            });
        let action = action?;
        self.act(ctx, CommanderPaneAction::DismissReport);
        match action {
            0 => Some(self.apply_now(ctx)),
            1 => Some(self.command(ctx, CommanderPaneAction::Discard)),
            _ => None,
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
        let active = self.ctl.listing.sort_column();
        let desc = self.ctl.listing.is_descending();
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
                self.ctl.apply(CommanderPaneAction::SortBy(clicked));
            }
        }
    }

    fn render_rows(&mut self, ui: &mut egui::Ui) -> RowActions {
        let mut out = RowActions::default();
        let ctx = ui.ctx().clone();
        let keyed = !self.key_actions.is_empty();
        for action in std::mem::take(&mut self.key_actions) {
            self.dispatch(&ctx, action, &mut out);
        }
        let rows = self.display_rows();
        let cursor = if self.has_focus {
            self.ctl.cursor_index()
        } else {
            None
        };
        self.page_rows = ((ui.available_height() / ROW_H) as i32).max(1);
        let busy = self.ctl.applying().is_some();
        // Host panes write immediately (Delete removes now); image panes stage
        // (Delete / Undelete / Remove-from-staging go on the queue).
        let host_pane = self.ctl.listing.is_host();
        // A read-only backup pane offers browse / copy-out actions only — no
        // delete or rename (both would mutate the backup).
        let read_only = self.ctl.is_backup();
        // On a remote *host* pane, a file can be opened as a disk image.
        let remote_host = self.is_remote_host();

        // Row clicks this frame, run through the controller in order once drawing is done.
        let mut acts: Vec<CommanderPaneAction> = Vec::new();
        let mut m_delete = false;
        let mut m_copy = false;
        let mut m_export = false;
        let mut m_export_as: Option<ExportFormat> = None;
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
                for (i, row) in rows.iter().enumerate() {
                    let (rect, resp) = ui.allocate_exact_size(
                        egui::vec2(ui.available_width(), ROW_H),
                        egui::Sense::click(),
                    );
                    let selected = if row.is_parent() {
                        false
                    } else if row.is_tree {
                        self.ctl.tree_selected.contains(&row.node_id)
                    } else {
                        self.ctl.listing.is_selected(&row.name)
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
                        if keyed {
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
                            CommanderPaneAction::ToggleWrapper(row.node_id.clone())
                        } else {
                            CommanderPaneAction::Activate(row.node_id.clone())
                        });
                    } else if resp.clicked() {
                        if hit_toggle(&resp) {
                            acts.push(CommanderPaneAction::ToggleWrapper(row.node_id.clone()));
                        } else if row.is_tree {
                            acts.push(CommanderPaneAction::TreeClick {
                                node_id: row.node_id.clone(),
                                ctrl: mods.command,
                                shift: mods.shift,
                            });
                        } else if !row.is_parent() {
                            acts.push(CommanderPaneAction::Click {
                                name: row.name.clone(),
                                ctrl: mods.command,
                                shift: mods.shift,
                            });
                        }
                    }

                    // Right-click a data row for its actions.
                    if !row.is_parent() && !busy {
                        let menu = resp.context_menu(|ui| {
                            // On a remote host pane, offer to open a file as an
                            // image (browse inside it).
                            if remote_host
                                && !row.is_dir
                                && !row.is_tree
                                && ui.button("Open Image").clicked()
                            {
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
                            // Export the selection to the host. "Export to hard
                            // drive..." uses the middle-column "Export as" format;
                            // the "Export" submenu picks a format inline. Needs
                            // real data, so a not-yet-applied staged add is out.
                            if !matches!(row.kind, RowKind::PendingAdd) {
                                if ui.button("Export to hard drive...").clicked() {
                                    m_export = true;
                                    ui.close();
                                }
                                ui.menu_button("Export as", |ui| {
                                    for fmt in ExportFormat::ALL {
                                        if ui.button(fmt.label()).clicked() {
                                            m_export_as = Some(fmt);
                                            ui.close();
                                        }
                                    }
                                });
                            }
                            // Checksums need real data, so a not-yet-applied
                            // staged add is excluded (same as Copy / Export).
                            if !matches!(row.kind, RowKind::PendingAdd)
                                && ui.button("Calculate checksums...").clicked()
                            {
                                m_checksums = true;
                                ui.close();
                            }
                            // File Info / Details: metadata + preview. Available
                            // for base-listing rows AND wrapper-tree rows (files
                            // inside a .sit / .toast / disk-image wrapper), which
                            // resolve through the mounted wrapper filesystem.
                            // Needs real data, so a not-yet-applied add is excluded.
                            if !matches!(row.kind, RowKind::PendingAdd)
                                && ui.button("File Info / Details...").clicked()
                            {
                                m_info = Some(row.name.clone());
                                ui.close();
                            }
                            // Edit actions are base-listing only — a wrapper's
                            // contents are read-only (copy out, don't mutate).
                            if !row.is_tree {
                                // Rename a single entry in place. A row with a
                                // rename already staged offers to cancel it
                                // instead. Suppressed on a read-only backup pane.
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
                                        m_delete = true;
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
                            }
                        });
                        if menu.is_some() {
                            acts.push(if row.is_tree {
                                CommanderPaneAction::TreeContextSelect(row.node_id.clone())
                            } else {
                                CommanderPaneAction::ContextSelect(row.name.clone())
                            });
                        }
                    }
                }

                // Click empty space to clear the selection.
                let remaining = ui.available_size();
                if remaining.y > 4.0 {
                    let (_r, bgr) = ui.allocate_exact_size(remaining, egui::Sense::click());
                    if bgr.clicked() {
                        acts.push(CommanderPaneAction::ClearSelection);
                    }
                }
            });

        // Any row interaction marks this pane "active" for the middle Delete.
        out.focused = !acts.is_empty();
        if m_delete {
            acts.push(CommanderPaneAction::Delete);
        }
        if let Some(name) = m_rename {
            acts.push(CommanderPaneAction::RequestRename(name));
        }
        if let Some(name) = m_cancel_rename {
            acts.push(CommanderPaneAction::CancelRename(name));
        }
        if let Some(name) = m_open_image {
            acts.push(CommanderPaneAction::OpenRemoteImage(name));
        }
        for action in acts {
            self.dispatch(&ctx, action, &mut out);
        }
        let status = out.status.take();
        RowActions {
            status,
            copy: m_copy,
            export: m_export,
            export_as: m_export_as,
            checksums: m_checksums,
            detail: m_info.or(out.detail),
            focused: out.focused,
        }
    }

    /// Runs a pane action, turning its error and requests into this frame's row actions.
    fn dispatch(&mut self, ctx: &egui::Context, action: CommanderPaneAction, out: &mut RowActions) {
        let fx = self.ctl.apply(action);
        if let Some(e) = self.ctl.take_error() {
            out.status = Some(format!("[{}] {e}", self.side.label()));
        }
        let reqs = self.ctl.take_requests();
        self.handle_row_requests(reqs, out);
        self.run_effects(ctx, fx);
    }

    fn handle_row_requests(&mut self, reqs: Vec<PaneRequest>, out: &mut RowActions) {
        for req in reqs {
            match req {
                PaneRequest::ShowInfo(name) => out.detail = Some(name),
            }
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
