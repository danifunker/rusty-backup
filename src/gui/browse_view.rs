// Browse-view filesystem-open helpers thread filesystem type, partition
// info, reader, ctx, logging, etc. — multi-arg by design.
#![allow(clippy::too_many_arguments)]

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rusty_backup::controller::app::{CommanderRecent, UserConfig};
use rusty_backup::controller::commander::{
    format_size, is_known_prodos_type, preview_bytes, prodos_type_description, ArchiveEditContext,
    ArchiveUse, BrowseSession, BrowserPaneAction, CommanderAction, CommanderController, DisplayRow,
    EditQueue, EntryType, ExportFormat, ExtractOptions, FileEntry, FileEntryTypeDisplay as _,
    ForkFormat, ImportChoice, JobRegistry, LineEnding, ListingStyle, MacArchiveKind, ModeAction,
    PaneProfile, PaneServices, PartcloneCache, ResourceForkMode, RowKind, Side, Snapshot,
    TarChoice, TextShape, UnsavedChoice, MIN_NESTED_IMAGE_BYTES, PRODOS_UNKNOWN_TYPE_NOTE,
};
use rusty_backup::controller::{Controller as _, Effect};

use super::commander::pane::PaneView;
use super::commander::DetailEditors;
use super::effects::{EffectHost, Reply};
use super::file_detail::{self, FileContent};
use super::metadata_editor;

const MAX_PREVIEW_SIZE: usize = 1024 * 1024; // 1 MB max file preview

/// Past this the tree popup's multiline widget stalls (egui lays the whole text out every
/// frame), so a larger listing offers Save first.
const TREE_INLINE_RENDER_LIMIT: usize = 1024 * 1024;

/// The browser pane behind the view: the Inspect profile, its listing style remembered as `inspect`.
fn new_controller() -> CommanderController {
    let services = PaneServices {
        jobs: JobRegistry::new(),
        notify: Arc::new(super::ui_logger::EguiWake),
        recent: Box::new(CommanderRecent(UserConfig)),
    };
    let keymap = super::commander::load_keymap();
    CommanderController::single(services, keymap, PaneProfile::inspect())
}

/// Filesystem browser view for inspecting partition contents.
pub struct BrowseView {
    /// Effects from a host call made outside `show`, run on the next frame.
    pending_fx: Vec<Effect>,
    /// The modes' volume generation the panel last followed.
    generation_seen: u64,
    /// The browser pane (profile `inspect`): source, listing, tree, cursor, selection, queue.
    ctl: CommanderController,
    /// Pickers, confirmations and prompts the controller asks for.
    effects: EffectHost,
    /// Draws the flat list and folder-tree styles, as Commander does.
    pane_view: PaneView,
    /// The tree was clicked last, so bound keys go to the pane.
    keys_active: bool,
    /// A key moved the cursor, so the tree scrolls it into view.
    scroll_cursor: bool,
    /// The cursor row `selected_entry` was last taken from.
    cursor_seen: Option<String>,
    /// The user moved the cursor since the volume (re)opened; until then it selects nothing.
    cursor_armed: bool,
    /// The entry the content panel shows: the cursor row, or the root when its row is clicked.
    selected_entry: Option<FileEntry>,
    /// The side panel's preview and editor widgets, rebuilt when its detail changes.
    side_ed: DetailEditors,
    /// View mode for file content.
    #[allow(dead_code)]
    view_mode: ViewMode,
    /// Error message to display.
    error: Option<String>,
    /// Filesystem info.
    fs_type: String,
    /// Volume total / used bytes (cached at open) for the header free-space
    /// line. `volume_total == 0` means "unknown / not loaded".
    volume_total: u64,
    volume_used: u64,
    volume_label: String,
    /// Optional prefix prepended to the volume label in the header (e.g. the
    /// Amiga drive name "DH0"). Set by the caller via `set_label_prefix`
    /// right after `open` so the header reads "Label: DH0 - Workbench". When
    /// the volume label is empty the prefix is shown on its own.
    label_prefix: String,
    /// Whether the browser is active (filesystem loaded).
    active: bool,
    /// Resource fork handling mode (HFS/HFS+ only).
    resource_fork_mode: ResourceForkMode,
    /// How to encode ProDOS type/aux on extract (ProDOS only).
    prodos_export_mode: ProdosExportMode,
    /// Convert RS line separators in text files to LF on extract (QNX 2).
    rs_to_lf: bool,
    /// An extraction runs as the screen's copy job; its status line becomes the result.
    extracting: bool,
    /// Message to show after extraction completes.
    extraction_result: Option<String>,
    /// Status message from last edit operation.
    edit_result: Option<String>,
    /// Blessed (bootable) system folder info (HFS/HFS+ only).
    blessed_folder: Option<(u64, String)>,
    /// Whether the open HFS volume has boot blocks at sector 0 (`'LK'`).
    /// `None` until computed (or when not file-backed); computed lazily in
    /// edit mode where the backing bytes are raw. Drives the "Boot blocks:"
    /// status line and the "Boot Blocks…" button's hint.
    boot_blocks_present: Option<bool>,
    /// HFS+ journal history viewer. `Some` while the window is open.
    journal_view: Option<super::journal_view::JournalView>,
    /// Whether to show debug-level fsck messages.
    show_fsck_debug: bool,
    /// The screen's export of the marked rows is running; its status line is the result.
    exporting: bool,
    /// Cached tree-view text output.
    tree_text: Option<String>,
    /// Whether to show the tree-view popup.
    show_tree_popup: bool,
    /// Whether the tree popup shows filesystem IDs.
    tree_show_ids: bool,
    /// When the generated tree text exceeds `TREE_INLINE_RENDER_LIMIT`, the
    /// poll routine routes through a "save first" dialog instead of opening
    /// the multiline TextEdit popup — egui's text widget repaints the entire
    /// laid-out text every frame and grinds to a halt at multi-MB sizes
    /// (originally hit on a 128 GB Amiga RDB partition).
    show_tree_large_dialog: bool,
    /// The SquashFS size-budget dialog, open only between clicking Edit Mode on
    /// a SquashFS volume and choosing a ceiling. See
    /// [`super::squashfs_budget_dialog`].
    squashfs_budget_dialog: Option<Box<super::squashfs_budget_dialog::SquashfsBudgetDialog>>,
    /// The text editor window, when open.
    text_editor: Option<GuiTextEditor>,

    /// User's password input text for the password prompt.
    password_input: String,
}

/// How to encode ProDOS file type/aux when extracting files to a host
/// directory. Only meaningful for ProDOS filesystems.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ProdosExportMode {
    /// Append a CiderPress `#TTAAAA` suffix to each extracted filename so
    /// type and aux survive a round-trip back into ProDOS.
    WithTypeSuffix,
    /// Write the bare ProDOS name (lossy — type/aux are dropped).
    Plain,
}

impl ProdosExportMode {
    const ALL: [ProdosExportMode; 2] = [ProdosExportMode::WithTypeSuffix, ProdosExportMode::Plain];

    fn label(&self) -> &'static str {
        match self {
            ProdosExportMode::WithTypeSuffix => "Name + #TTAAAA suffix",
            ProdosExportMode::Plain => "Bare name (lossy)",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum ViewMode {
    Auto,
    Hex,
    Text,
}

impl Default for BrowseView {
    fn default() -> Self {
        Self {
            pending_fx: Vec::new(),
            generation_seen: 0,
            ctl: new_controller(),
            effects: EffectHost::default(),
            pane_view: PaneView::new(Side::Left),
            keys_active: false,
            scroll_cursor: false,
            cursor_seen: None,
            cursor_armed: false,
            selected_entry: None,
            side_ed: DetailEditors::default(),
            view_mode: ViewMode::Auto,
            error: None,
            fs_type: String::new(),
            volume_total: 0,
            volume_used: 0,
            volume_label: String::new(),
            label_prefix: String::new(),
            active: false,
            resource_fork_mode: ResourceForkMode::AppleDouble,
            prodos_export_mode: ProdosExportMode::WithTypeSuffix,
            rs_to_lf: false,
            extracting: false,
            extraction_result: None,
            edit_result: None,
            blessed_folder: None,
            boot_blocks_present: None,
            journal_view: None,
            show_fsck_debug: false,
            exporting: false,
            tree_text: None,
            show_tree_popup: false,
            tree_show_ids: false,
            show_tree_large_dialog: false,
            text_editor: None,
            squashfs_budget_dialog: None,
            password_input: String::new(),
        }
    }
}

/// A text file open for editing in the GUI.
///
/// egui supplies the text widget, so this only has to hold the conversion
/// state, which is the part that matters: the file is decoded to UTF-8/LF on
/// open and re-encoded to its own encoding and endings on save, exactly as the
/// CLI and TUI editors do, via the shared `model::text_edit`.
struct GuiTextEditor {
    name: String,
    /// The folder holding it, where the save is staged.
    folder: String,
    /// The editable buffer: UTF-8 with LF endings.
    text: String,
    /// What the file was, and what it will be written back as.
    shape: TextShape,
    /// The endings it was found with, so a conversion can be shown as one.
    original_ending: LineEnding,
    dirty: bool,
    status: Option<String>,
}

impl BrowseView {
    /// The label of a running extract, source-level job, export, apply or volume tool, if any.
    pub fn running_job(&self) -> Option<&'static str> {
        let pane = self.ctl.pane(Side::Left);
        if self.extracting && self.ctl.copy_job().is_some() {
            Some("Extract")
        } else if self.ctl.mode_working().is_some() {
            Some("Edit Mode")
        } else if self.exporting && self.ctl.copy_job().is_some() {
            Some("Export")
        } else if pane.checking().is_some() || pane.applying().is_some() {
            Some("Apply edits")
        } else if pane.archive_working().is_some() {
            Some("Archive")
        } else if pane.tool_working().is_some() {
            Some("Volume tool")
        } else {
            None
        }
    }

    /// Open the browser on a session the Inspect tab built; it starts read-only.
    pub fn open_with_session(&mut self, session: BrowseSession) {
        self.close();
        let label = session
            .source_path
            .as_ref()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "partition".into());
        self.active = true;
        let fx = self.ctl.open_inspect(session, &label);
        self.pending_fx.extend(fx);
    }

    /// The pane's staged-edit queue.
    fn queue(&self) -> &EditQueue {
        &self.ctl.pane(Side::Left).queue
    }

    /// Open a partition of an image or device (`preopen_file`: an already elevated handle).
    pub fn open(
        &mut self,
        source_path: PathBuf,
        partition_offset: u64,
        partition_type: u8,
        partition_type_string: Option<String>,
        preopen_file: Option<File>,
    ) {
        let session = BrowseSession {
            source_path: Some(source_path),
            partition_offset,
            partition_type,
            partition_type_string,
            preopen_file: preopen_file.map(std::sync::Arc::new),
            ..Default::default()
        };
        self.open_with_session(session);
        self.ctl.inspect_edit_in_place();
    }

    /// Open a Clonezilla partition through its partclone block cache.
    pub fn open_partclone(&mut self, cache: PartcloneCache, partition_type: u8) {
        self.open_with_session(BrowseSession::partclone(cache, partition_type));
    }

    /// Open a native zstd partition image as a stream until its seekable cache is ready.
    pub fn open_streaming(&mut self, path: PathBuf, ptype: u8, ptype_str: Option<String>) {
        match BrowseSession::streaming(&path, ptype, ptype_str) {
            Ok(session) => self.open_with_session(session),
            Err(e) => {
                self.close();
                self.error = Some(e);
                self.active = true;
            }
        }
    }

    /// A prefix for the header's volume label, such as the Amiga drive name ("DH0"); empty clears it.
    pub fn set_label_prefix(&mut self, prefix: String) {
        self.label_prefix = prefix;
    }

    /// The open source may be edited in place (raw or CHD backup partitions, remote images).
    pub fn mark_edit_supported(&mut self) {
        self.ctl.inspect_edit_in_place();
    }

    /// The open backup partition is edited by decompressing, editing, then recompressing it.
    pub fn set_archive_edit_context(
        &mut self,
        archive_path: PathBuf,
        compression_type: String,
        original_size: u64,
        compacted: bool,
        metadata_path: PathBuf,
        partition_index: usize,
        checksum_type: String,
    ) {
        self.ctl.inspect_edit_archive(ArchiveEditContext {
            archive_path,
            compression_type,
            original_size,
            compacted,
            metadata_path,
            partition_index,
            checksum_type,
        });
    }

    /// The open CHD is the body of this single-file-chd backup; a save refreshes its metadata.
    pub fn set_single_file_chd_backup_folder(&mut self, backup_folder: PathBuf) {
        self.ctl.inspect_backup_folder(backup_folder);
    }

    /// The streamed backup's seekable cache is ready; the tree stays where it is.
    pub fn upgrade_to_seekable_cache(&mut self, cache_path: PathBuf) {
        if self.active {
            let act = CommanderAction::Mode(ModeAction::UpgradeSeekable(cache_path));
            let fx = self.ctl.apply(act);
            self.pending_fx.extend(fx);
        }
    }

    pub fn close(&mut self) {
        // The old pane's readers go before its CHD diff or archive temp is removed.
        std::mem::replace(&mut self.ctl, new_controller()).close_inspect();
        self.pending_fx.clear();
        self.effects = EffectHost::default();
        self.keys_active = false;
        self.cursor_seen = None;
        self.selected_entry = None;
        self.side_ed = DetailEditors::default();
        self.error = None;
        self.active = false;
        self.fs_type.clear();
        self.volume_total = 0;
        self.volume_used = 0;
        self.volume_label.clear();
        self.label_prefix.clear();
        self.extracting = false;
        self.extraction_result = None;
        self.edit_result = None;
        self.blessed_folder = None;
        self.journal_view = None;
        self.tree_text = None;
        self.show_tree_popup = false;
        self.show_tree_large_dialog = false;
        self.tree_show_ids = false;
        self.password_input.clear();
        // Dialogs and editors that name a file on the volume being closed;
        // left open, their Save/Overwrite acted on the next volume.
        self.text_editor = None;
        self.boot_blocks_present = None;
        self.squashfs_budget_dialog = None;
        self.generation_seen = 0;
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    /// True when the view is open in edit mode with unapplied staged edits —
    /// i.e. closing / switching the source would lose work. Lets a caller gate
    /// a source switch behind its own confirm dialog (see Inspect's deferred
    /// source-switch guard) before calling [`close`](Self::close).
    pub fn has_unsaved_edits(&self) -> bool {
        self.active && self.ctl.has_unsaved_edits()
    }

    /// Returns true if the current filesystem is HFS or HFS+.
    fn is_hfs_type(&self) -> bool {
        let ft = self.fs_type.as_str();
        ft == "HFS" || ft == "HFS+" || ft == "HFSX"
    }

    /// Returns true if the current filesystem is ProDOS.
    fn is_prodos_type(&self) -> bool {
        self.fs_type == "ProDOS"
    }

    /// Returns true if the current filesystem is a Mac-native one (HFS,
    /// HFS+, HFSX, MFS, or APFS). Gates the Mac-archive workflows — "Save
    /// as Mac archive...", "Browse archive...", and the "Export Mac
    /// archive" bundling row — so they only appear when the source volume
    /// is one whose files actually carry forks / type / creator codes.
    /// (APFS is browse-readable; MFS isn't fully readable yet, but both are
    /// listed so the gate is correct the day a driver reports them.)
    fn is_mac_filesystem(&self) -> bool {
        matches!(
            self.fs_type.as_str(),
            "HFS" | "HFS+" | "HFSX" | "MFS" | "APFS"
        )
    }

    /// The selected file's Mac-archive kind, once the pane's sniff of it has landed.
    fn ensure_archive_sniff(&mut self, entry: &FileEntry) -> Option<MacArchiveKind> {
        if !entry.is_file() || !self.is_mac_filesystem() {
            return None;
        }
        let pane = self.ctl.pane(Side::Left);
        if let Some(kind) = pane.archive_sniff(&entry.path) {
            return kind;
        }
        if !pane.is_sniffing() {
            let act = BrowserPaneAction::SniffArchive(entry.path.clone());
            self.ctl.apply(CommanderAction::Pane(Side::Left, act));
        }
        None
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        if !self.active {
            return;
        }
        self.poll_editor();
        if let Some(snap) = self.ctl.pane(Side::Left).tool_working() {
            egui::Window::new(snap.label.clone())
                .collapsible(false)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    super::progress::show_job(ui, &snap);
                });
        }
        if let Some(said) = self.ctl.host_pane().take_archive_notice() {
            self.edit_result = Some(said);
        }
        self.render_apply_job(ui);
        if let Some(snap) = self.ctl.pane(Side::Left).archive_working() {
            egui::Window::new(snap.label.clone())
                .collapsible(false)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    super::progress::show_job(ui, &snap);
                });
        }
        self.poll_export(ui);
        if let Some(snap) = self.ctl.mode_working() {
            egui::Window::new(snap.label.clone())
                .collapsible(false)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    super::progress::show_job(ui, &snap);
                });
        }

        let ctx = ui.ctx().clone();
        let mut fx = std::mem::take(&mut self.pending_fx);
        fx.extend(self.ctl.tick());
        self.run_effects(&ctx, fx);
        self.poll_modes();
        if !self.active {
            return;
        }
        if self.ctl.is_working() {
            ctx.request_repaint_after(std::time::Duration::from_millis(50));
        }
        // The first open takes the whole view; a reopen after a write shows in the tree's place.
        if self.fs_type.is_empty() && self.pane_opening(ui) {
            return;
        }
        self.sync_volume();

        // Poll background tree-view generation. Only renders a spinner here;
        // the rest of the UI stays interactive while the walk runs.
        if let Some(tree) = self.ctl.host_pane().take_tree_text() {
            self.tree_show_ids = tree.ids;
            let oversized = tree.text.len() > TREE_INLINE_RENDER_LIMIT;
            self.tree_text = Some(tree.text);
            if oversized {
                self.show_tree_large_dialog = true;
            } else {
                self.show_tree_popup = true;
            }
        }
        if let Some(detail) = self.ctl.host_pane().take_journal() {
            self.journal_view = Some(super::journal_view::JournalView::new(detail));
        }
        // Poll extraction progress
        self.poll_extraction();

        // Unlock prompt for an encrypted container (e.g. IMZ with ZipCrypto)
        // or an encrypted filesystem (APFS FileVault). The retry loop is shared:
        // stash the secret on the session and re-spawn the open.
        if self.ctl.pane(Side::Left).needs_password() {
            let is_apfs = self.ctl.pane(Side::Left).volume().fs_type == "APFS";
            ui.vertical_centered(|ui| {
                ui.add_space(20.0);
                if is_apfs {
                    ui.label("This APFS volume is encrypted with FileVault.");
                    ui.add_space(4.0);
                    ui.label("Enter the volume password or personal recovery key.");
                } else {
                    ui.label("This image is password-protected.");
                }
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    ui.label(if is_apfs { "Passphrase:" } else { "Password:" });
                    let resp = ui.add(
                        egui::TextEdit::singleline(&mut self.password_input)
                            .password(true)
                            .desired_width(240.0),
                    );
                    let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                    if ui.button("Unlock").clicked() || enter {
                        let secret = self.password_input.clone();
                        self.error = None;
                        let act = CommanderAction::Mode(ModeAction::Password(secret));
                        self.dispatch(&ctx, act);
                    }
                });
            });
            return;
        }

        // Handle drag-and-drop from host OS
        self.handle_dropped_files(ui);

        self.boot_blocks_present = self.ctl.pane(Side::Left).boot_blocks_present();

        // Header
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Filesystem Browser").strong());
            ui.label(format!("[{}]", self.fs_type));
            if self.volume_total > 0 {
                let free = self.volume_total.saturating_sub(self.volume_used);
                ui.label(format!(
                    "{} used / {} ({} free)",
                    format_size(self.volume_used),
                    format_size(self.volume_total),
                    format_size(free),
                ))
                .on_hover_text("Volume space: used / total (free)");
            }
            let display_label = match (self.label_prefix.is_empty(), self.volume_label.is_empty()) {
                (false, false) => format!("{} - {}", self.label_prefix, self.volume_label),
                (false, true) => self.label_prefix.clone(),
                (true, false) => self.volume_label.clone(),
                (true, true) => String::new(),
            };
            if !display_label.is_empty() {
                ui.label(format!("Label: {}", display_label));
            }
            if let Some((_, ref name)) = self.blessed_folder {
                ui.label(format!("Blessed: {}", name));
            }
            // Boot-block status (edit mode, HFS volumes). A bootable volume
            // needs both a blessed System Folder and boot blocks at sector 0.
            if self.ctl.edit_mode() && self.is_hfs_type() {
                match self.boot_blocks_present {
                    Some(true) => {
                        ui.label("Boot blocks: present")
                            .on_hover_text("The volume's first sector has the 'LK' boot loader.");
                    }
                    Some(false) => {
                        ui.label(
                            egui::RichText::new("Boot blocks: absent")
                                .color(super::theme::warning(ui.visuals())),
                        )
                        .on_hover_text(
                            "This volume has no boot loader. Use 'Boot Blocks...' to copy \
                             them from a bootable donor disk (e.g. a matching stock System \
                             disk), then bless the System Folder. Works on a flat HFV and on \
                             the HFS partition of a full (APM) disk alike.",
                        );
                    }
                    None => {}
                }
            }

            // Carve view: "Full scan" toggle. By default the synthetic carve
            // view only scans the first 10 MB of an image for recoverable
            // text (fast on large devices); turning this on scans the whole
            // image. Flipping it re-opens the volume so the new policy applies.
            if self.fs_type == "Raw carve (no filesystem)" {
                ui.add_space(8.0);
                let mut full = self.ctl.carve_full_scan();
                let resp = ui.checkbox(&mut full, "Full scan");
                let limit_mb = self.ctl.carve_scan_limit_mb();
                resp.clone().on_hover_text(format!(
                    "Off: scan only the first {limit_mb} MB for recoverable text (fast).\n\
                     On: scan the entire image (slower on large disks).\n\
                     'whole-disk.img' is always the full image regardless."
                ));
                if resp.changed() {
                    self.error = None;
                    let act = CommanderAction::Mode(ModeAction::CarveFullScan(full));
                    self.dispatch(&ctx, act);
                }
            }

            if self.fs_type == "QNX 2" {
                ui.add_space(8.0);
                let mut deleted = self.ctl.show_deleted();
                let resp = ui.checkbox(&mut deleted, "Show deleted");
                resp.clone().on_hover_text(
                    "List deleted entries whose data can still be read, named 'NAME [deleted]'.",
                );
                if resp.changed() {
                    let act = CommanderAction::Mode(ModeAction::ShowDeleted(deleted));
                    self.dispatch(&ctx, act);
                }
            }

            // Edit mode toggle (Pilot/Cedar volumes are read-only for now)
            if self.ctl.edit_supported() && self.fs_type != "Pilot/Cedar" {
                let busy = self.extracting || self.ctl.mode_working().is_some();
                ui.add_space(8.0);
                let edit_btn = if self.ctl.edit_mode() {
                    egui::Button::new("Edit Mode ON")
                } else {
                    egui::Button::new("Edit Mode")
                };
                let btn = ui.add_enabled(!busy, edit_btn);
                if btn.clicked() {
                    self.dispatch(&ctx, CommanderAction::Mode(ModeAction::ToggleEditMode));
                }
                if !self.ctl.edit_mode() && btn.hovered() {
                    btn.on_hover_text("Enable editing to add or delete files on this image");
                }
            }

            // Export the whole volume to a tar archive (.tar.gz / .tar.zst /
            // .tar). Preserves case-sensitive names + real symlinks (data
            // fork only) so a case-insensitive host won't clobber files that
            // differ only in case. Runs on a worker thread.
            if !self.ctl.edit_mode() && self.ctl.copy_job().is_none() {
                let btn = ui.button("Export to .tar.gz...");
                if btn.hovered() {
                    btn.clone().on_hover_text(
                        "Archive the whole volume to .tar.gz / .tar.zst / .tar \
                         (preserves case + symlinks; choose the extension in the save dialog)",
                    );
                }
                if btn.clicked() {
                    self.export_entry(ui.ctx(), &FileEntry::root(), ExportFormat::TarGz);
                }
            }

            // HFS and Alto BFS packs check here; the Inspect grid's Check covers the rest.
            let tool_free = self.ctl.pane(Side::Left).tool_working().is_none();
            if matches!(self.fs_type.as_str(), "HFS" | "Alto BFS")
                && tool_free
                && ui.button("Check").clicked()
            {
                self.pane_act(&ui.ctx().clone(), BrowserPaneAction::Fsck);
            }
            if (self.fs_type == "HFS+" || self.fs_type == "HFSX") && ui.button("Journal").clicked()
            {
                self.pane_act(&ui.ctx().clone(), BrowserPaneAction::ShowJournal);
            }

            if self.ctl.is_chd_source() && ui.button("CHD Info").clicked() {
                self.dispatch(&ctx, CommanderAction::Mode(ModeAction::ChdInfo));
            }

            if ui.button("Tree").clicked() {
                let ids = self.tree_show_ids;
                self.pane_act(&ui.ctx().clone(), BrowserPaneAction::ListTree { ids });
            }

            ui.menu_button("View", |ui| {
                let items = self.ctl.view_menu();
                if let Some(action) = super::commander::view_menu_body(ui, &items) {
                    self.ctl.apply(action);
                    ui.close();
                }
            });

            if ui.button("Close").clicked() {
                self.request_close();
            }
        });

        if let Some(err) = &self.error {
            ui.colored_label(super::theme::danger(ui.visuals()), err);
        }

        // Edit result message
        if let Some(result) = &self.edit_result.clone() {
            ui.horizontal(|ui| {
                let color = if result.starts_with("Error") {
                    super::theme::danger(ui.visuals())
                } else {
                    super::theme::success(ui.visuals())
                };
                ui.colored_label(color, result);
                if ui.button("OK").clicked() {
                    self.edit_result = None;
                }
            });
        }

        // Extraction result message
        if let Some(result) = &self.extraction_result.clone() {
            ui.horizontal(|ui| {
                ui.label(result);
                if ui.button("OK").clicked() {
                    self.extraction_result = None;
                }
            });
        }

        // The SquashFS size-budget dialog, and the edit-mode entry it gates.
        self.poll_squashfs_budget_dialog(ui);

        // Info banner while editing an archive
        if self.ctl.editing_archive() {
            ui.colored_label(
                super::theme::info(ui.visuals()),
                "Editing temporary copy. Click 'Apply Edits' to write changes.",
            );
        }
        if self.ctl.editing_chd() {
            ui.colored_label(
                super::theme::info(ui.visuals()),
                "Editing CHD via diff. Click 'Apply Edits' to flush, or exit Edit Mode to merge into the CHD.",
            );
        }

        // Edit mode toolbar
        if self.ctl.edit_mode() {
            self.render_edit_toolbar(ui);
        }

        ui.separator();

        // Show drop target overlay when files are being dragged over
        if self.ctl.edit_mode() {
            let hovered = ui.ctx().input(|i| !i.raw.hovered_files.is_empty());
            if hovered {
                ui.colored_label(
                    super::theme::info(ui.visuals()),
                    "Drop files here to add them to the current directory",
                );
                ui.separator();
            }
        }

        // Delete confirmation dialog

        // Unsaved changes dialog
        self.render_unsaved_dialog(ui);

        // The pane's apply review (names already taken) and its report
        let mut review_acts = Vec::new();
        let pane = self.ctl.pane(Side::Left);
        self.pane_view
            .render_apply_review(ui.ctx(), pane, &mut review_acts);
        self.pane_view
            .render_apply_report(ui.ctx(), pane, &mut review_acts);
        for a in review_acts {
            self.dispatch(&ctx, a);
        }

        // Text editor window
        self.render_text_editor(ui);

        // Tar archive added/dropped: import contents vs add as file
        self.render_tar_add_dialog(ui);

        // Tar import confirmation (preflight found unrepresentable content)
        self.render_tar_import_dialog(ui);

        // ProDOS "Set Type…" dialog

        // Staging errors dialog
        self.render_staging_errors_dialog(ui);

        // Extraction overwrite-confirm dialog

        // Workflow A: Mac archive import modal (Convert / Expand / Add as-is).
        self.render_archive_import_dialog(ui);

        // Workflow C: Mac archive extract-out modal (Save as-is / Decode-and-save).
        self.render_archive_extract_dialog(ui);

        // Workflow E: floating Mac archive viewer window (read-only browse + extract).
        self.render_archive_browse_window(ui);

        // Two-panel layout: tree | content
        let available = ui.available_size();
        let tree_width = (available.x * 0.4).clamp(200.0, 400.0);
        let panel_height = available.y;

        ui.horizontal(|ui| {
            ui.set_min_height(panel_height);

            // Left panel: file tree
            ui.vertical(|ui| {
                ui.set_width(tree_width);
                ui.set_min_height(panel_height);
                self.render_selection_bar(ui);
                self.render_listing(ui, panel_height);
            });

            ui.separator();

            // Right panel: file content / details
            ui.vertical(|ui| {
                ui.set_min_width(available.x - tree_width - 20.0);
                ui.set_min_height(panel_height);
                self.render_content_panel(ui, panel_height);
            });
        });

        // Fsck results popup window
        self.render_fsck_popup(ui);
        self.render_chd_info_popup(ui);
        self.render_tree_popup(ui);
        self.render_tree_large_dialog(ui);

        // Journal history viewer window
        if let Some(view) = &mut self.journal_view {
            if !view.show(ui.ctx()) {
                self.journal_view = None;
            }
        }

        // Commander's windows for what the shared keys and the View menu open.
        let mut acts = Vec::new();
        type Cm = super::commander::CommanderMode;
        Cm::render_checksum_window(&self.ctl, &ctx, &mut acts);
        Cm::render_host_conflict_modal(&self.ctl, &ctx, &mut acts);
        Cm::render_log_window(&self.ctl, &ctx, &mut acts);
        Cm::render_help(&self.ctl, &ctx, &mut acts);
        Cm::render_view_window(&self.ctl, &ctx, &mut acts);
        // The content panel already shows what File Info would.
        if self.ctl.detail().is_some() {
            acts.push(CommanderAction::CloseDetail);
        }
        for a in acts {
            self.dispatch(&ctx, a);
        }
        if let Some(reply) = self.effects.show(&ctx, "") {
            self.answer(&ctx, reply);
        }
        self.handle_keys(&ctx);
        self.sync_selected();
    }

    /// The pane's open in progress, or a failed one: drawn instead of the browser.
    fn pane_opening(&mut self, ui: &mut egui::Ui) -> bool {
        let pane = self.ctl.pane(Side::Left);
        if let Some(snap) = pane.opening() {
            ui.horizontal(|ui| {
                ui.add(egui::Spinner::new());
                ui.label("Loading filesystem:");
            });
            if super::progress::show_job(ui, &snap) {
                let act = CommanderAction::Pane(Side::Left, BrowserPaneAction::CancelOpen);
                self.ctl.apply(act);
            }
            ui.label(
                "Large volumes (e.g. HFS+ catalogs with hundreds of \
                 thousands of files) can take several seconds.",
            );
            return true;
        }
        if pane.is_opening() {
            ui.spinner();
            return true;
        }
        if let Some(err) = pane.source_error() {
            if !pane.listing.is_loaded() && !pane.needs_password() {
                ui.colored_label(super::theme::danger(ui.visuals()), err);
                if ui.button("Close").clicked() {
                    self.close();
                }
                return true;
            }
        }
        false
    }

    /// The header's volume readout, from the volume the pane opened.
    fn sync_volume(&mut self) {
        let pane = self.ctl.pane(Side::Left);
        if !pane.listing.is_loaded() {
            return;
        }
        let v = pane.volume();
        if self.fs_type != v.fs_type {
            self.fs_type = v.fs_type.clone();
        }
        if self.volume_label != v.label {
            self.volume_label = v.label.clone();
        }
        self.volume_total = v.total_size;
        self.volume_used = v.used_size;
        if self.blessed_folder != v.blessed {
            self.blessed_folder = v.blessed.clone();
        }
    }

    /// Carry out the pane's effects; answers go straight back to it.
    fn run_effects(&mut self, ctx: &egui::Context, fx: Vec<Effect>) {
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
            Reply::Picked { purpose, path } => CommanderAction::Picked { purpose, path },
            Reply::Confirmed { purpose, yes } => CommanderAction::Confirmed { purpose, yes },
            Reply::Answered { purpose, text } => CommanderAction::Answered { purpose, text },
            Reply::CloseScreen => {
                self.request_close();
                return;
            }
        };
        self.dispatch(ctx, action);
    }

    fn dispatch(&mut self, ctx: &egui::Context, action: CommanderAction) {
        let fx = self.ctl.apply(action);
        self.run_effects(ctx, fx);
    }

    fn pane_act(&mut self, ctx: &egui::Context, action: BrowserPaneAction) {
        self.dispatch(ctx, CommanderAction::Pane(Side::Left, action));
    }

    /// Close, after the unsaved-edits dialog when edits wait.
    fn request_close(&mut self) {
        self.ctl
            .apply(CommanderAction::Mode(ModeAction::RequestClose));
        if self.ctl.take_close_request() {
            self.close();
        }
    }

    /// What the modes ended with: notices, errors, a close, a size budget to ask for, a write.
    fn poll_modes(&mut self) {
        if let Some(said) = self.ctl.take_mode_notice() {
            self.edit_result = Some(said);
        }
        if let Some(e) = self.ctl.take_mode_error() {
            self.error = Some(e);
        }
        if let Some(plan) = self.ctl.take_size_plan() {
            self.squashfs_budget_dialog = Some(Box::new(
                super::squashfs_budget_dialog::SquashfsBudgetDialog::new(plan),
            ));
        }
        let generation = self.ctl.volume_generation();
        if generation != self.generation_seen {
            // The volume was reopened or written: what the panel showed may be gone.
            self.generation_seen = generation;
            self.selected_entry = None;
            self.side_ed = DetailEditors::default();
            self.cursor_seen = None;
            self.cursor_armed = false;
        }
        if self.ctl.take_close_request() {
            self.close();
        }
    }

    /// Bound keys go to the pane while the tree was clicked last and no text field has focus.
    fn handle_keys(&mut self, ctx: &egui::Context) {
        let modal = self.effects.modal_open() || self.ctl.modal_open();
        if !self.keys_active || modal || ctx.text_edit_focused() || egui::Popup::is_any_open(ctx) {
            return;
        }
        let ctl = &self.ctl;
        let keys: Vec<_> = ctx.input_mut(|i| {
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
                let mods = super::commander::egui_mods(*modifiers);
                let bound = super::commander::egui_key(*key).filter(|k| ctl.binds(*k, mods));
                found.extend(bound.map(|k| (k, mods)));
                bound.is_none()
            });
            found
        });
        if keys.is_empty() {
            return;
        }
        ctx.memory_mut(|m| m.move_focus(egui::FocusDirection::None));
        for (key, mods) in keys {
            self.dispatch(ctx, CommanderAction::Key(key, mods));
        }
        self.cursor_armed = true;
        self.scroll_cursor = true;
        ctx.request_repaint();
    }

    /// Point the content panel at the cursor row once it moved (a click or a key).
    fn sync_selected(&mut self) {
        let pane = self.ctl.pane(Side::Left);
        let Some(row) = pane.cursor_index().map(|i| pane.rows()[i].clone()) else {
            return;
        };
        if self.cursor_seen.as_deref() == Some(row.node_id.as_str()) {
            return;
        }
        self.cursor_seen = Some(row.node_id.clone());
        if !self.cursor_armed {
            return;
        }
        // Rows inside an expanded archive or image belong to another filesystem.
        let entry = if row.is_tree {
            None
        } else {
            pane.row_entry(&row)
        };
        let Some(entry) = entry else {
            self.selected_entry = None;
            self.side_ed = DetailEditors::default();
            return;
        };
        if self.selected_entry.as_ref().map(|e| &e.path) == Some(&entry.path) {
            return;
        }
        self.selected_entry = Some(entry);
        self.error = None;
    }

    /// The listing in the pane's style: the combined tree as the browser always drew it, or Commander's.
    fn render_listing(&mut self, ui: &mut egui::Ui, height: f32) {
        let mut acts: Vec<CommanderAction> = Vec::new();
        let pane = self.ctl.pane(Side::Left);
        if !pane.listing.is_loaded() {
            if pane.is_opening() {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Reading the volume...");
                });
            } else if let Some(err) = pane.source_error() {
                ui.colored_label(super::theme::danger(ui.visuals()), err);
            }
            return;
        }
        if pane.listing_style() == ListingStyle::CombinedTree {
            egui::ScrollArea::vertical()
                .id_salt("browse_tree")
                .max_height(height)
                .auto_shrink([false, false])
                .show(ui, |ui| self.render_tree_rows(ui, &mut acts));
            self.scroll_cursor = false;
        } else {
            ui.allocate_ui_with_layout(
                egui::vec2(ui.available_width(), height),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    let scroll = self.scroll_cursor;
                    self.pane_view
                        .show_listing(ui, &self.ctl, scroll, &mut acts)
                },
            );
            self.scroll_cursor = false;
        }
        if !acts.is_empty() {
            self.keys_active = true;
            self.cursor_armed = true;
            self.cursor_seen = None;
            let ctx = ui.ctx().clone();
            for a in acts {
                self.dispatch(&ctx, a);
            }
        }
    }

    /// One row per entry of the combined tree, indented by depth, with the mark checkbox.
    fn render_tree_rows(&mut self, ui: &mut egui::Ui, acts: &mut Vec<CommanderAction>) {
        let pane = self.ctl.pane(Side::Left);
        let rows = pane.rows().to_vec();
        let cursor = pane.cursor_index();
        let root_selected = self.selected_entry.as_ref().is_some_and(|e| e.path == "/");
        let p = |a| CommanderAction::Pane(Side::Left, a);
        // The volume's root: always open; clicking it shows the volume in the content panel.
        ui.horizontal(|ui| {
            if ui.selectable_label(root_selected, "/").clicked() {
                self.selected_entry = pane.entry_at("/").or_else(|| Some(FileEntry::root()));
                self.side_ed = DetailEditors::default();
                self.error = None;
                self.keys_active = true;
            }
        });
        let indent = ui.spacing().indent;
        for (i, row) in rows.iter().enumerate() {
            if row.is_parent() {
                continue;
            }
            let entry = pane.row_entry(row);
            let is_cursor = cursor == Some(i) && !root_selected;
            ui.horizontal(|ui| {
                ui.add_space(indent * (row.depth as f32 + 1.0));
                if row.expandable {
                    let size = egui::vec2(indent, ui.spacing().interact_size.y);
                    let (_, resp) = ui.allocate_exact_size(size, egui::Sense::click());
                    let openness = if row.expanded { 1.0 } else { 0.0 };
                    egui::collapsing_header::paint_default_icon(ui, openness, &resp);
                    if resp.clicked() {
                        acts.push(p(BrowserPaneAction::ToggleExpand(row.node_id.clone())));
                    }
                } else {
                    ui.add_space(indent);
                }
                let markable = !matches!(row.kind, RowKind::PendingAdd | RowKind::PendingDelete);
                if markable {
                    let mut marked = pane.row_selected(row);
                    let resp = ui
                        .checkbox(&mut marked, "")
                        .on_hover_text("Mark for multi-select export");
                    if resp.changed() {
                        acts.push(p(BrowserPaneAction::Mark(row.node_id.clone())));
                    }
                }
                let resp = self.row_label(ui, row, entry.as_ref(), is_cursor);
                if resp.double_clicked() {
                    acts.push(p(BrowserPaneAction::Activate(row.node_id.clone())));
                } else if resp.clicked() {
                    acts.push(p(BrowserPaneAction::CursorTo(row.node_id.clone())));
                }
                if is_cursor && self.scroll_cursor {
                    resp.scroll_to_me(None);
                }
            });
            if row.loading {
                ui.horizontal(|ui| {
                    ui.add_space(indent * (row.depth as f32 + 3.0));
                    ui.label("Loading...");
                });
            }
        }
    }

    /// A row's label as the tree always showed it: sizes, ProDOS badges, links, staged edits.
    fn row_label(
        &self,
        ui: &mut egui::Ui,
        row: &DisplayRow,
        entry: Option<&FileEntry>,
        selected: bool,
    ) -> egui::Response {
        let dimmed = super::theme::muted(ui.visuals());
        if row.kind == RowKind::PendingAdd {
            let text = match entry {
                Some(e) => self.pending_add_text(ui, e),
                None => egui::RichText::new(format!("+ {}", row.name)),
            };
            return ui.selectable_label(selected, text);
        }
        let Some(entry) = entry else {
            return ui.selectable_label(selected, &row.name);
        };
        let mut hover = None;
        let mut label = match entry.entry_type {
            EntryType::Directory => {
                let blessed = self
                    .blessed_folder
                    .as_ref()
                    .is_some_and(|(cnid, _)| *cnid == entry.location);
                if blessed {
                    format!("{} [System]", entry.name)
                } else {
                    entry.name.clone()
                }
            }
            EntryType::File => {
                let size_str = entry.size_string();
                let badge = if self.is_prodos_type() {
                    entry.type_code_display()
                } else {
                    None
                };
                hover = badge.as_ref().map(|tc| {
                    let tt = entry.prodos_file_type.unwrap_or(0);
                    let desc = prodos_type_description(tt);
                    let aux = entry.aux_type.unwrap_or(0);
                    if desc.is_empty() {
                        format!("{tc} ${:04X}", aux)
                    } else {
                        format!("{desc} ${:04X}", aux)
                    }
                });
                match badge {
                    Some(b) => format!("{}  [{}]  ({})", entry.name, b, size_str),
                    None => format!("{}  ({})", entry.name, size_str),
                }
            }
            EntryType::Symlink => {
                let target = entry.symlink_target.as_deref().unwrap_or("?");
                format!("{} -> {}", entry.name, target)
            }
            EntryType::Special => {
                let stype = entry.special_type.as_deref().unwrap_or("special");
                format!("{}  ({})", entry.name, stype)
            }
        };
        if row.is_wrapper {
            label = format!("{label}  {}", row.type_tag);
        }
        if let Some(to) = &row.rename_to {
            label = format!("{label} -> {to}");
        }
        let text = match row.kind {
            RowKind::PendingDelete => egui::RichText::new(label).color(dimmed).strikethrough(),
            _ if row.meta_changed || row.rename_to.is_some() => {
                egui::RichText::new(label).color(super::theme::info(ui.visuals()))
            }
            _ => egui::RichText::new(label),
        };
        let resp = ui.selectable_label(selected, text);
        match hover {
            Some(h) => resp.on_hover_text(h),
            None => resp,
        }
    }

    /// Compact bar shown above the tree when entries are checkbox-marked: the
    /// count, an "Export selected as..." format menu (the shared `export_selection`
    /// engine, so `.mar` and the other formats come for free), and Clear.
    fn render_selection_bar(&mut self, ui: &mut egui::Ui) {
        let count = self.ctl.pane(Side::Left).selected_entries().len();
        if count == 0 {
            return;
        }
        let picked = self.ctl.export_format();
        let mut fmt = picked;
        let mut do_export = false;
        let mut clear = false;
        ui.horizontal_wrapped(|ui| {
            ui.label(format!("{count} selected"));
            ui.label("Export as:");
            egui::ComboBox::from_id_salt("browse_export_fmt")
                .selected_text(fmt.label())
                .show_ui(ui, |ui| {
                    for f in ExportFormat::ALL {
                        ui.selectable_value(&mut fmt, f, f.label());
                    }
                });
            if ui.button("Export...").clicked() {
                do_export = true;
            }
            if ui.button("Clear").clicked() {
                clear = true;
            }
        });
        ui.separator();
        if fmt != picked {
            self.ctl.apply(CommanderAction::SetExportFormat(fmt));
        }
        if clear {
            let act = CommanderAction::Pane(Side::Left, BrowserPaneAction::ClearSelection);
            self.ctl.apply(act);
        }
        if do_export {
            self.export_marked(&ui.ctx().clone(), fmt);
        }
    }

    /// Export the marked rows as one `format` output through the screen's export (picker, job).
    fn export_marked(&mut self, ctx: &egui::Context, format: ExportFormat) {
        if self.ctl.copy_job().is_some() {
            return;
        }
        let fork = CommanderAction::SetForkMode(self.resource_fork_mode);
        self.ctl.apply(fork);
        let export = CommanderAction::Export {
            from: Side::Left,
            format: Some(format),
        };
        self.exporting = true;
        self.dispatch(ctx, export);
    }

    /// Export one entry (the root: the whole volume) as `format` through the screen's export.
    fn export_entry(&mut self, ctx: &egui::Context, entry: &FileEntry, format: ExportFormat) {
        if self.ctl.copy_job().is_some() {
            return;
        }
        self.ctl
            .apply(CommanderAction::SetForkMode(self.resource_fork_mode));
        let act = CommanderAction::ExportEntry {
            from: Side::Left,
            path: entry.path.clone(),
            format: Some(format),
        };
        self.exporting = true;
        let ctx = ctx.clone();
        self.dispatch(&ctx, act);
    }

    /// The export's progress window, then its status line as the result.
    fn poll_export(&mut self, ui: &mut egui::Ui) {
        if self.extracting {
            if let Some((_, snap)) = self.ctl.copy_job() {
                self.show_copy_job(ui, "Extracting", &snap);
            }
            return;
        }
        if !self.exporting {
            return;
        }
        let Some((title, snap)) = self.ctl.copy_job() else {
            // Not started (cancelled picker, nothing marked) or finished.
            if !self.effects.modal_open() {
                self.exporting = false;
                let said = self.ctl.status();
                if !said.is_empty() {
                    self.edit_result = Some(said.to_string());
                }
            }
            return;
        };
        self.show_copy_job(ui, title, &snap);
    }

    /// The screen's copy job as a window with Cancel.
    fn show_copy_job(&mut self, ui: &mut egui::Ui, title: &str, snap: &Snapshot) {
        let mut cancel = false;
        egui::Window::new(title)
            .collapsible(false)
            .resizable(false)
            .show(ui.ctx(), |ui| {
                cancel = super::progress::show_job(ui, snap);
            });
        if cancel {
            self.ctl.apply(CommanderAction::CancelCopy);
        }
    }

    /// A staged add's label: green, blue when its type is unresolved, with its fork badge.
    fn pending_add_text(&self, ui: &egui::Ui, entry: &FileEntry) -> egui::RichText {
        let green = super::theme::success(ui.visuals());
        let blue = super::theme::info(ui.visuals());
        let queue = self.queue();
        // HFS: no FInfo, no dictionary match, no override. ProDOS: a type byte not in the registry.
        let color = if self.is_hfs_type() && entry.is_file() {
            let (t, c) = queue.resolved_hfs_type_creator(entry);
            if t == [0; 4] && c == [0; 4] {
                blue
            } else {
                green
            }
        } else if self.is_prodos_type() && entry.is_file() {
            let (t, _) = metadata_editor::resolved_prodos_type(queue, entry);
            if !is_known_prodos_type(t) {
                blue
            } else {
                green
            }
        } else {
            green
        };
        let rsrc_badge: Option<String> = queue.pending_resource_fork_for(&entry.path).map(|imp| {
            let tc = imp
                .type_code
                .map(|c| String::from_utf8_lossy(&c).to_string());
            let cc = imp
                .creator_code
                .map(|c| String::from_utf8_lossy(&c).to_string());
            let codes = match (tc, cc) {
                (Some(t), Some(c)) => format!("{t}/{c} "),
                (Some(t), None) => format!("{t} "),
                (None, Some(c)) => format!("?/{c} "),
                (None, None) => String::new(),
            };
            format!("[{}+rsrc {}]", codes, format_size(imp.data.len() as u64))
        });
        let label = if entry.is_directory() {
            format!("+ {}", entry.name)
        } else if let Some(ref badge) = rsrc_badge {
            format!("+ {}  {}  ({})", entry.name, badge, entry.size_string())
        } else {
            format!("+ {}  ({})", entry.name, entry.size_string())
        };
        egui::RichText::new(label).color(color)
    }

    fn render_content_panel(&mut self, ui: &mut egui::Ui, panel_height: f32) {
        let entry = self.selected_entry.clone();
        let shown = self.render_detail(ui, entry.as_ref());
        // A row inside an expanded archive or image: details only, its actions act on this volume.
        let Some(entry) = entry else {
            if !shown {
                ui.colored_label(
                    super::theme::muted(ui.visuals()),
                    "Select a file to view its contents.",
                );
            }
            return;
        };
        // ProDOS/GS-OS leaves $CB..$EE unassigned; vintage apps used them, so say it is not corruption.
        if self.is_prodos_type() {
            let tt = entry.prodos_file_type;
            if let Some(tt) = tt {
                if !is_known_prodos_type(tt) {
                    let aux = entry.aux_type.unwrap_or(0);
                    ui.colored_label(
                        super::theme::warning(ui.visuals()),
                        format!(
                            "Note: ${:02X} is not in the ProDOS type registry - {}. The type and aux (${:04X}) will be preserved on export via the CiderPress #{:02X}{:04X} filename suffix.",
                            tt,
                            PRODOS_UNKNOWN_TYPE_NOTE,
                            aux,
                            tt,
                            aux,
                        ),
                    );
                }
            }
        }

        // Extract controls row
        let extraction_running = self.extracting;
        let is_extractable = entry.is_file() || entry.is_directory() || entry.is_symlink();

        // The Mac-archive workflows only make sense on Mac-native
        // volumes. Sniff the selection once per change so the
        // "Save as / Browse archive" buttons appear strictly for
        // files that are actually archives (preflight, not
        // error-on-click). The bundling row needs only the
        // filesystem gate — you can wrap any file/folder.
        let is_mac_fs = self.is_mac_filesystem();
        let show_archive_actions = is_mac_fs && self.ensure_archive_sniff(&entry).is_some();

        if is_extractable && !extraction_running {
            ui.horizontal(|ui| {
                // Resource fork mode dropdown (HFS/HFS+ only)
                if self.is_hfs_type() {
                    ui.label("Resource forks:");
                    let current_label = self.resource_fork_mode.label();
                    egui::ComboBox::from_id_salt("rsrc_mode")
                        .selected_text(current_label)
                        .show_ui(ui, |ui| {
                            for mode in &ResourceForkMode::ALL {
                                ui.selectable_value(
                                    &mut self.resource_fork_mode,
                                    *mode,
                                    mode.label(),
                                );
                            }
                        });
                    ui.add_space(8.0);
                }

                if self.fs_type == "QNX 2" {
                    ui.checkbox(&mut self.rs_to_lf, "Text: RS to LF").on_hover_text(
                        "Convert QNX line separators (RS, 0x1E) to LF in files that look like text.",
                    );
                    ui.add_space(8.0);
                }

                // ProDOS export mode dropdown (ProDOS only)
                if self.is_prodos_type() {
                    ui.label("Export name:");
                    let current_label = self.prodos_export_mode.label();
                    egui::ComboBox::from_id_salt("prodos_export_mode")
                        .selected_text(current_label)
                        .show_ui(ui, |ui| {
                            for mode in &ProdosExportMode::ALL {
                                ui.selectable_value(
                                    &mut self.prodos_export_mode,
                                    *mode,
                                    mode.label(),
                                );
                            }
                        });
                    ui.add_space(8.0);
                }

                let btn_label = if entry.is_directory() {
                    "Extract Folder..."
                } else {
                    "Extract File..."
                };
                if ui.button(btn_label).clicked() {
                    let ctx = ui.ctx().clone();
                    self.start_extraction(&ctx, &entry, None);
                }

                // Archive the selected folder/file to a tar.gz instead
                // of laying it out on the host — preserves case +
                // symlinks (handy for case-sensitive volumes).
                let tgz_label = if entry.is_directory() {
                    "Export Folder to .tgz..."
                } else {
                    "Export File to .tgz..."
                };
                if ui
                    .add_enabled(
                        self.ctl.copy_job().is_none(),
                        egui::Button::new(tgz_label),
                    )
                    .on_hover_text(
                        "Save the selection as a single .tar.gz / .tar.zst / .tar \
                         (preserves exact case + symlinks; pick the extension in \
                         the save dialog)",
                    )
                    .clicked()
                {
                    self.export_entry(ui.ctx(), &entry, ExportFormat::TarGz);
                }

                if entry.is_file()
                    && entry.size >= MIN_NESTED_IMAGE_BYTES
                    && ui
                        .button("Open as Disk Image")
                        .on_hover_text(
                            "Copy this file to a temporary folder and inspect it as \
                             its own disk image, e.g. a ShapeShifter hardfile or a \
                             floppy image stored on this volume.",
                        )
                        .clicked()
                {
                    let ctx = ui.ctx().clone();
                    self.extracting = true;
                    self.extraction_result = None;
                    let act = CommanderAction::Mode(ModeAction::OpenNested(entry.path.clone()));
                    self.dispatch(&ctx, act);
                    if self.ctl.copy_job().is_none() {
                        self.extracting = false;
                        self.extraction_result = Some(self.ctl.status().to_string());
                    }
                }

                // Workflow C: the selection already sniffed as a
                // Mac archive (see show_archive_actions), so pop
                // the C modal (Save as-is vs Decode-and-save).
                if show_archive_actions
                    && ui
                        .button("Save as Mac archive...")
                        .on_hover_text(
                            "This file is a Mac archive (BinHex / StuffIt / \
                             SEA): open a dialog to either save it raw or \
                             decode and save its contents in a fork-preserving \
                             container.",
                        )
                        .clicked()
                {
                    self.open_archive_extract_modal(&ui.ctx().clone(), &entry);
                }

                // Workflow E: open a floating viewer that lets
                // the user browse the archive's entries and
                // extract any subset.
                if show_archive_actions
                    && ui
                        .button("Browse archive...")
                        .on_hover_text(
                            "Open a viewer to browse this Mac archive's entries \
                             and extract any subset (per-entry selection, \
                             fork-preserving containers).",
                        )
                        .clicked()
                {
                    self.open_archive_browse_window(&ui.ctx().clone(), &entry);
                }
            });

            // Workflow B: bundle the current selection as a Mac
            // archive on the host. BinHex is single-file only;
            // SIT / SIT.HQX accept any single entry (folders
            // walk recursively). Only offered on Mac volumes,
            // where forks / type / creator codes exist to bundle.
            if is_mac_fs {
                ui.horizontal(|ui| {
                    ui.label("Export Mac archive:");
                    let is_single_file = entry.is_file();
                    if ui
                        .add_enabled(is_single_file, egui::Button::new("BinHex (.hqx)"))
                        .on_hover_text(
                            "Encode the selected file's data fork, resource fork, \
                         and type/creator codes as printable ASCII (BinHex 4.0). \
                         The only flat format that survives a non-HFS roundtrip.",
                        )
                        .clicked()
                    {
                        self.export_entry(ui.ctx(), &entry, ExportFormat::BinHex);
                    }
                    if ui
                        .button("StuffIt (.sit)")
                        .on_hover_text(
                            "Multi-file compressed archive in classic StuffIt format. \
                         Preserves forks on HFS or inside a .hqx wrapper. \
                         Folders walk recursively.",
                        )
                        .clicked()
                    {
                        self.export_entry(ui.ctx(), &entry, ExportFormat::StuffIt);
                    }
                    if ui
                        .button("StuffIt-over-BinHex (.sit.hqx)")
                        .on_hover_text(
                            "StuffIt bundle wrapped in BinHex for transport-safe ASCII. \
                         The classic \"emailed me a Mac app\" format.",
                        )
                        .clicked()
                    {
                        self.export_entry(ui.ctx(), &entry, ExportFormat::StuffItHqx);
                    }
                    if ui
                        .button("MAR (.mar)")
                        .on_hover_text(
                            "Hampa Hug's MAR format: a single file or folder tree with \
                         both forks, stored uncompressed. Folders walk recursively; \
                         multiple items are wrapped in a folder.",
                        )
                        .clicked()
                    {
                        self.export_entry(ui.ctx(), &entry, ExportFormat::MacArchive);
                    }
                });
            }
        }

        ui.separator();

        if entry.size > MAX_PREVIEW_SIZE as u64 && entry.is_file() {
            ui.label(format!(
                "File too large to preview ({}).",
                format_size(entry.size)
            ));
            return;
        }

        if entry.is_directory() {
            return;
        }

        let content_height = ui.available_height().min(panel_height);
        if entry.is_symlink() {
            let target = entry
                .symlink_target
                .as_deref()
                .unwrap_or("(unknown target)");
            ui.monospace(format!("Symlink target: {target}"));
            return;
        }
        if entry.is_special() {
            let stype = entry.special_type.as_deref().unwrap_or("special file");
            ui.label(format!("{stype} -- no preview available"));
            return;
        }
        // Editing is offered only where it is safe: a text preview means the content decoded cleanly.
        if matches!(self.side_ed.content, Some(FileContent::Text(_)))
            && ui
                .add_enabled(self.ctl.edit_mode(), egui::Button::new("Edit as text..."))
                .on_disabled_hover_text("Turn on Edit Mode to edit this file.")
                .clicked()
        {
            self.open_text_editor();
        }
        super::commander::detail_preview(ui, &self.side_ed, "browse", content_height);
    }

    /// The side panel's File Info for the cursor row: metadata, editors (Edit Mode) and the preview.
    fn render_detail(&mut self, ui: &mut egui::Ui, entry: Option<&FileEntry>) -> bool {
        let detail = self.ctl.side_detail(Side::Left);
        // The root has no row; anything else must be the cursor row the detail follows.
        let detail = match entry {
            Some(e) if e.path == "/" => None,
            Some(e) => detail.filter(|d| d.entry.path == e.path),
            None => detail.filter(|_| self.cursor_armed),
        };
        let Some(d) = detail else {
            if let Some(e) = entry {
                file_detail::render_metadata_rows(ui, e, false, None);
            }
            return false;
        };
        let ed = &mut self.side_ed;
        if ed.serial != d.serial {
            *ed = DetailEditors::fresh(d.serial);
        }
        if ed.content.is_none() {
            ed.content = d.preview.as_ref().map(|data| {
                let shown = preview_bytes(&d.fs_type, data);
                file_detail::detect_content_type(&d.entry, &shown)
            });
        }
        let mut staged = Vec::new();
        let queue = &self.ctl.pane(Side::Left).queue;
        super::commander::detail_meta(ui, d, ed, queue, &mut staged);
        if entry.is_none() {
            ui.separator();
            super::commander::detail_preview(ui, ed, "browse_wrapped", ui.available_height());
        }
        for e in staged {
            self.ctl.apply(CommanderAction::StageMetadata(Box::new(e)));
        }
        true
    }

    /// Start extracting the selected entry to a user-chosen folder.
    /// A finished "Open as Disk Image" copy: the image path and the temp dir that owns it.
    pub fn take_nested_open(&mut self) -> Option<(PathBuf, tempfile::TempDir)> {
        self.ctl.take_nested_open()
    }

    /// Extract `entry` through the screen (a picker unless `dest` is given; existing files are reviewed).
    fn start_extraction(&mut self, ctx: &egui::Context, entry: &FileEntry, dest: Option<PathBuf>) {
        if self.ctl.copy_job().is_some() {
            return;
        }
        // An image opened from inside the volume wants its bytes, not its forks.
        let fork_mode = match dest {
            Some(_) => ResourceForkMode::DataForkOnly,
            None => self.resource_fork_mode,
        };
        let options = ExtractOptions {
            fork_mode,
            prodos_suffix: self.prodos_export_mode == ProdosExportMode::WithTypeSuffix,
            rs_to_lf: self.rs_to_lf,
        };
        let act = CommanderAction::Extract {
            from: Side::Left,
            path: entry.path.clone(),
            options,
            dest,
        };
        self.extracting = true;
        self.extraction_result = None;
        self.dispatch(ctx, act);
    }

    /// Get the parent entry for the currently selected entry, or root if nothing selected.
    fn current_parent_entry(&self) -> FileEntry {
        // If a directory is selected, use it as the parent
        if let Some(ref sel) = self.selected_entry {
            if sel.is_directory() {
                return sel.clone();
            }
            if let Some(parent) = self.parent_entry_of_path(&sel.path) {
                return parent;
            }
        }
        self.root_entry()
    }

    /// The volume's root entry as the pane listed it.
    fn root_entry(&self) -> FileEntry {
        let pane = self.ctl.pane(Side::Left);
        pane.entry_at("/")
            .or_else(|| pane.listing.root_entry())
            .unwrap_or_else(FileEntry::root)
    }

    /// The directory entry holding `path`, from the expanded-directory cache.
    fn parent_entry_of_path(&self, path: &str) -> Option<FileEntry> {
        let parent_path = path
            .rsplit_once('/')
            .map(|(p, _)| if p.is_empty() { "/" } else { p })?;
        if parent_path == "/" {
            return Some(self.root_entry());
        }
        let pane = self.ctl.pane(Side::Left);
        pane.entry_at(parent_path).filter(|e| e.is_directory())
    }

    /// Draw the budget dialog and act on its outcome: a chosen budget is
    /// installed on the session and Edit Mode is entered; cancelling just
    /// closes, leaving the volume read-only.
    fn poll_squashfs_budget_dialog(&mut self, ui: &mut egui::Ui) {
        let Some(dialog) = self.squashfs_budget_dialog.as_mut() else {
            return;
        };
        dialog.show(ui.ctx());
        let outcome = dialog.take_outcome();
        let closed = dialog.is_closed();
        if let Some(budget) = outcome {
            self.squashfs_budget_dialog = None;
            let act = CommanderAction::Mode(ModeAction::SetBudget(Some(budget)));
            self.dispatch(&ui.ctx().clone(), act);
        } else if closed {
            self.squashfs_budget_dialog = None;
        }
    }

    /// Render the edit mode toolbar with action buttons and free space.
    fn render_edit_toolbar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            let parent = self.current_parent_entry();
            let parent_name = if parent.path == "/" {
                "root".to_string()
            } else {
                parent.name.clone()
            };
            ui.label(
                egui::RichText::new(format!("Editing: /{parent_name}"))
                    .color(super::theme::info(ui.visuals())),
            );

            ui.add_space(8.0);

            if ui.button("Add File...").clicked() {
                self.add_file_dialog(&ui.ctx().clone());
            }

            // Import a host .tar.gz / .tar.zst / .tar into the current
            // directory. Runs a preflight first and prompts if the target
            // filesystem can't represent some of the archive's content.
            if ui.button("Import Archive...").clicked() {
                self.pane_act(&ui.ctx().clone(), BrowserPaneAction::ImportTar);
            }

            // Alto BFS has a flat namespace — no subdirectories to create.
            if self.fs_type != "Alto BFS" && ui.button("New Folder...").clicked() {
                let act = CommanderAction::Pane(Side::Left, BrowserPaneAction::RequestNewFolder);
                let fx = self.ctl.apply(act);
                self.run_effects(&ui.ctx().clone(), fx);
            }

            // Delete acts on the marks, else the cursor row; a full folder asks first.
            let has_selection = self.selected_entry.as_ref().is_some_and(|e| e.path != "/");
            let deletable = has_selection || self.ctl.pane(Side::Left).has_selection();
            if ui
                .add_enabled(deletable, egui::Button::new("Delete"))
                .clicked()
            {
                let act = CommanderAction::Pane(Side::Left, BrowserPaneAction::Delete);
                self.dispatch(&ui.ctx().clone(), act);
            }

            // Set ProDOS Type… button (ProDOS only, file selected)
            // Bless Folder button (HFS/HFS+ only)
            if self.is_hfs_type() {
                let can_bless = has_selection
                    && self
                        .selected_entry
                        .as_ref()
                        .map(|e| e.is_directory())
                        .unwrap_or(false);
                if ui
                    .add_enabled(can_bless, egui::Button::new("Bless Folder"))
                    .on_hover_text(
                        "Mark the selected folder as the bootable System Folder. For the \
                         volume to actually boot it also needs boot blocks (see 'Boot \
                         Blocks...').",
                    )
                    .clicked()
                {
                    self.pane_act(&ui.ctx().clone(), BrowserPaneAction::Bless);
                }

                // Boot Blocks... — copy the 1024-byte boot loader from a
                // bootable donor disk into the volume's first sector
                // (partition-scoped, so it works on the HFS partition of a
                // full APM disk too, not just a flat HFV). Needed to make a
                // bare HFS volume (e.g. an edited infinite-mac disk) boot.
                let boot_hint = match self.boot_blocks_present {
                    Some(true) => {
                        "This volume already has boot blocks; copy a donor's to replace them."
                    }
                    _ => {
                        "Copy boot blocks from a bootable donor disk (e.g. a matching stock \
                          System disk) into this volume's first sector. Works on a flat HFV \
                          and on the HFS partition of a full (APM) disk."
                    }
                };
                if ui
                    .button("Boot Blocks...")
                    .on_hover_text(boot_hint)
                    .clicked()
                {
                    self.pane_act(&ui.ctx().clone(), BrowserPaneAction::BootBlocks);
                }
            }

            // ProDOS Lock / Unlock buttons (ProDOS only, file or folder
            // selected). The access byte conventions are:
            //   $C3 = unlocked: read + write + destroy + rename + backup
            //   $21 = locked:   read + backup-required
            // We stage canonical values; advanced callers can drive
            // `EditableFilesystem::set_prodos_access` directly via the CLI
            // when they need bit-level control.
            if self.is_prodos_type() {
                let can_lock = has_selection;
                if ui
                    .add_enabled(can_lock, egui::Button::new("Lock"))
                    .on_hover_text("Set the ProDOS access byte to $21 (read + backup only).")
                    .clicked()
                {
                    self.prodos_lock(&ui.ctx().clone(), true);
                }
                if ui
                    .add_enabled(can_lock, egui::Button::new("Unlock"))
                    .on_hover_text("Set the ProDOS access byte to $C3 (read/write/destroy/rename).")
                    .clicked()
                {
                    self.prodos_lock(&ui.ctx().clone(), false);
                }
            }

            ui.add_space(8.0);
            ui.separator();
            ui.add_space(8.0);

            // Apply button: only with staged edits, and not while an extraction
            // or tar export is still reading the volume the apply would rewrite.
            if !self.ctl.pane(Side::Left).queue.is_empty() {
                let label = format!("Apply Edits ({})", self.ctl.pane(Side::Left).queue.len());
                let reading = self.extracting || self.exporting;
                if ui
                    .add_enabled(!reading, egui::Button::new(label))
                    .on_disabled_hover_text("Wait for the running extraction or export to finish")
                    .clicked()
                {
                    self.dispatch(&ui.ctx().clone(), CommanderAction::Mode(ModeAction::Apply));
                }
            }

            ui.add_space(8.0);

            // Free space, probed in a job once per edit session and again after each write.
            if let Some(space) = self.ctl.space() {
                ui.label(format!("Free: {}", format_size(space.free)));
                if let Some(after) = space.after {
                    let color = if space.overflows {
                        super::theme::danger(ui.visuals())
                    } else if space.tight {
                        super::theme::warning(ui.visuals())
                    } else {
                        super::theme::success(ui.visuals())
                    };
                    ui.colored_label(color, format!("After: {}", format_size(after)));
                }
            }
        });
    }

    /// Stage the cursor entry's ProDOS lock through the pane; its status line is the result.
    fn prodos_lock(&mut self, ctx: &egui::Context, locked: bool) {
        self.pane_act(ctx, BrowserPaneAction::SetProdosLock(locked));
        self.edit_result = Some(self.ctl.status().to_string());
    }

    /// Show the "Add File" file picker dialog and add files.
    fn add_file_dialog(&mut self, ctx: &egui::Context) {
        let files = rfd::FileDialog::new()
            .set_title("Select files to add")
            .pick_files();

        if let Some(paths) = files {
            self.add_host_paths(ctx, &paths);
        }
    }

    /// Stage host picks and drops through the pane; a tar or (on HFS) a Mac archive asks first.
    fn add_host_paths(&mut self, ctx: &egui::Context, paths: &[PathBuf]) {
        self.pane_act(ctx, BrowserPaneAction::AddPicked(paths.to_vec()));
    }

    /// Workflow E trigger: read the selected file's bytes off the disk
    /// image, sniff + decode, and open a floating viewer window with
    /// the archive's entries. Read-only — the user can extract any
    /// subset but can't modify the archive itself.
    fn open_archive_browse_window(&mut self, ctx: &egui::Context, entry: &FileEntry) {
        let path = entry.path.clone();
        let purpose = ArchiveUse::Browse;
        self.pane_act(ctx, BrowserPaneAction::ReadArchive { path, purpose });
    }

    /// Workflow C trigger: read the selected file's bytes off the disk
    /// image, sniff them via [`detect_mac_archive`], and on a hit open
    /// the Save-as-is / Decode-and-save modal. On a non-archive (or a
    /// read failure), surface a one-line error and don't open the modal.
    fn open_archive_extract_modal(&mut self, ctx: &egui::Context, entry: &FileEntry) {
        let path = entry.path.clone();
        let purpose = ArchiveUse::Extract;
        self.pane_act(ctx, BrowserPaneAction::ReadArchive { path, purpose });
    }

    /// A small window with the pane's running preflight or write.
    fn render_apply_job(&self, ui: &mut egui::Ui) {
        let pane = self.ctl.pane(Side::Left);
        let Some(snap) = pane.checking().or_else(|| pane.applying()) else {
            return;
        };
        egui::Window::new(snap.label.clone())
            .collapsible(false)
            .resizable(false)
            .show(ui.ctx(), |ui| {
                super::progress::show_job(ui, &snap);
            });
    }

    /// Render the delete confirmation dialog.
    /// Render the unsaved changes confirmation dialog.
    /// Open a text file in the editor window, decoding it the way the CLI and
    /// TUI editors do.
    /// Open a file in the text editor as a job (the editable open decodes a whole container).
    /// Read the cursor's file for the editor through the pane (Edit Mode only).
    fn open_text_editor(&mut self) {
        let act = CommanderAction::Pane(Side::Left, BrowserPaneAction::EditText);
        self.ctl.apply(act);
        let said = self.ctl.status().to_string();
        if !said.is_empty() && self.ctl.pane(Side::Left).tool_working().is_none() {
            self.edit_result = Some(said);
        }
    }

    /// Take the file the pane read for the editor.
    fn poll_editor(&mut self) {
        if let Some(doc) = self.ctl.take_text_doc() {
            self.text_editor = Some(GuiTextEditor {
                name: doc.name,
                folder: doc.folder,
                text: doc.text,
                original_ending: doc.shape.ending,
                shape: doc.shape,
                dirty: false,
                status: None,
            });
        }
    }

    /// The text editor window.
    fn render_text_editor(&mut self, ui: &mut egui::Ui) {
        let Some(mut ed) = self.text_editor.take() else {
            return;
        };
        let mut keep_open = true;
        let mut save = false;

        egui::Window::new(format!("Edit  {}", ed.name))
            .collapsible(false)
            .resizable(true)
            .default_size([720.0, 480.0])
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.horizontal(|ui| {
                    ui.label(format!("Encoding: {}", ed.shape.encoding.label()));
                    ui.separator();
                    ui.label("Line endings:");
                    // Changeable, for a file whose endings are simply wrong.
                    // They are *read* from the content on open, never assumed
                    // from the filesystem.
                    for (v, label) in [
                        (LineEnding::Lf, "LF (Unix)"),
                        (LineEnding::CrLf, "CRLF (DOS)"),
                        (LineEnding::Cr, "CR (classic Mac)"),
                    ] {
                        if ui.selectable_label(ed.shape.ending == v, label).clicked()
                            && ed.shape.ending != v
                        {
                            ed.shape.ending = v;
                            // A conversion is a change even with nothing typed.
                            ed.dirty = true;
                        }
                    }
                });
                if ed.shape.mixed_endings {
                    ui.colored_label(
                        super::theme::warning(ui.visuals()),
                        "This file mixes line-ending styles; saving makes them all the same.",
                    );
                }
                if ed.shape.ending != ed.original_ending {
                    ui.colored_label(
                        super::theme::warning(ui.visuals()),
                        format!(
                            "Will convert {} -> {}",
                            ed.original_ending.label(),
                            ed.shape.ending.label()
                        ),
                    );
                }
                ui.separator();

                egui::ScrollArea::vertical()
                    .max_height(ui.available_height() - 40.0)
                    .show(ui, |ui| {
                        let r = ui.add(
                            egui::TextEdit::multiline(&mut ed.text)
                                .desired_width(f32::INFINITY)
                                .desired_rows(24)
                                .font(egui::TextStyle::Monospace),
                        );
                        if r.changed() {
                            ed.dirty = true;
                        }
                    });

                if let Some(msg) = &ed.status {
                    ui.colored_label(super::theme::danger(ui.visuals()), msg);
                }
                ui.horizontal(|ui| {
                    if ui
                        .button("Save")
                        .on_hover_text("Stage the new text; Apply Edits writes it.")
                        .clicked()
                    {
                        save = true;
                    }
                    if ui.button("Cancel").clicked() {
                        keep_open = false;
                    }
                    if ed.dirty {
                        ui.label("modified");
                    }
                });
            });

        if save {
            // Staged like any edit; a refusal (an unencodable character) keeps the window and the text.
            let before = self.ctl.pane(Side::Left).queue.len();
            let act = BrowserPaneAction::SaveText {
                name: ed.name.clone(),
                folder: ed.folder.clone(),
                text: ed.text.clone(),
                shape: ed.shape,
            };
            self.ctl.apply(CommanderAction::Pane(Side::Left, act));
            if self.ctl.pane(Side::Left).queue.len() > before {
                self.edit_result = Some(format!(
                    "Staged the new {}; Apply Edits writes it.",
                    ed.name
                ));
                return;
            }
            ed.status = Some(self.ctl.status().to_string());
        }
        if keep_open {
            self.text_editor = Some(ed);
        }
    }

    /// Leaving Edit Mode or closing with edits staged: discard, apply or stay.
    fn render_unsaved_dialog(&mut self, ui: &mut egui::Ui) {
        if !self.ctl.unsaved_shown() {
            return;
        }
        let count = self.ctl.pane(Side::Left).queue.len();
        let mut choice = None;
        egui::Window::new("Unsaved Changes")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.label(format!("You have {count} unsaved edit(s)."));
                ui.add_space(4.0);
                for line in self.ctl.pane(Side::Left).staged_summary() {
                    ui.label(line);
                }
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Discard Edits").clicked() {
                        choice = Some(UnsavedChoice::Discard);
                    }
                    if ui.button("Apply Edits").clicked() {
                        choice = Some(UnsavedChoice::Apply);
                    }
                    if ui.button("Cancel").clicked() {
                        choice = Some(UnsavedChoice::Cancel);
                    }
                });
            });
        if let Some(c) = choice {
            let ctx = ui.ctx().clone();
            self.dispatch(&ctx, CommanderAction::Mode(ModeAction::Unsaved(c)));
            self.poll_modes();
        }
    }

    /// The pane's Mac archive waiting for its import choice; the buttons depend on its kind.
    fn render_archive_import_dialog(&mut self, ui: &mut egui::Ui) {
        let Some((filename, kind)) = self.ctl.pane(Side::Left).pending_import() else {
            return;
        };
        let kind_label = kind.label();
        let mut choice: Option<ImportChoice> = None;
        // A single BinHex converts; an archive expands; BinHex over an archive does either.
        let (title, body, expand) = match kind {
            MacArchiveKind::BinHexSingleFile => (
                "Convert HQX file?",
                format!("Would you like to convert {filename} to binary?"),
                "Convert",
            ),
            MacArchiveKind::BinHexOverSit
            | MacArchiveKind::BinHexOverSea
            | MacArchiveKind::BinHexOverCompactPro => (
                "Convert and/or expand?",
                format!(
                    "Would you like to convert {filename} to binary and/or expand the contents?"
                ),
                "Convert and expand",
            ),
            MacArchiveKind::Sit
            | MacArchiveKind::Sit5
            | MacArchiveKind::Sea
            | MacArchiveKind::CompactPro
            | MacArchiveKind::Mar
            | MacArchiveKind::MacBinary
            | MacArchiveKind::MacZip => (
                "Expand archive?",
                format!("Would you like to expand the contents of {filename}?"),
                "Expand",
            ),
        };
        let convert_only = match kind {
            MacArchiveKind::BinHexOverSea => Some("Convert only (.sea lands here)"),
            MacArchiveKind::BinHexOverCompactPro => Some("Convert only (inner archive lands here)"),
            MacArchiveKind::BinHexOverSit => Some("Convert only (.sit lands here)"),
            _ => None,
        };
        egui::Window::new(title)
            .id(egui::Id::unique("archive_import_dialog"))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.label(format!("{filename} is a {kind_label} archive."));
                ui.add_space(4.0);
                ui.label(body);
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui
                        .button(expand)
                        .on_hover_text(
                            "Decode the archive and add each enclosed file to this directory \
                             (resource forks preserved on HFS/HFS+).",
                        )
                        .clicked()
                    {
                        choice = Some(ImportChoice::Expand);
                    }
                    if let Some(label) = convert_only {
                        if ui
                            .button(label)
                            .on_hover_text(
                                "Strip the BinHex wrapper and add the inner archive file. \
                                 Useful when the target Mac has StuffIt Expander but no \
                                 BinHex tool.",
                            )
                            .clicked()
                        {
                            choice = Some(ImportChoice::ConvertOnly);
                        }
                    }
                    if ui
                        .button("Add as-is")
                        .on_hover_text("Add the file unchanged.")
                        .clicked()
                    {
                        choice = Some(ImportChoice::AsIs);
                    }
                    if ui.button("Cancel").clicked() {
                        choice = Some(ImportChoice::Cancel);
                    }
                });
            });
        if let Some(c) = choice {
            self.pane_act(&ui.ctx().clone(), BrowserPaneAction::ChooseImport(c));
        }
    }

    /// The pane's tar waiting for its choice: import its contents, or add it as a file.
    fn render_tar_add_dialog(&mut self, ui: &mut egui::Ui) {
        let Some(name) = self.ctl.pane(Side::Left).pending_tar() else {
            return;
        };
        let mut open = true;
        let mut choice: Option<TarChoice> = None;
        egui::Window::new("Tar archive")
            .open(&mut open)
            .resizable(false)
            .collapsible(false)
            .default_width(460.0)
            .show(ui.ctx(), |ui| {
                ui.label(format!("\"{name}\" is a tar archive."));
                ui.add_space(4.0);
                ui.label(
                    "Import its contents into the current folder, or add the \
                     archive itself as a file?",
                );
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Import contents").clicked() {
                        choice = Some(TarChoice::Import);
                    }
                    if ui.button("Add as file").clicked() {
                        choice = Some(TarChoice::AsFile);
                    }
                    if ui.button("Cancel").clicked() {
                        choice = Some(TarChoice::Cancel);
                    }
                });
            });
        if !open {
            choice = Some(TarChoice::Cancel);
        }
        if let Some(c) = choice {
            self.pane_act(&ui.ctx().clone(), BrowserPaneAction::ChooseTar(c));
        }
    }

    /// The pane's tar preflight that found content this volume cannot keep: go on or cancel.
    fn render_tar_import_dialog(&mut self, ui: &mut egui::Ui) {
        let Some(pf) = self.ctl.pane(Side::Left).tar_preview().cloned() else {
            return;
        };
        let mut open = true;
        let mut decision: Option<bool> = None;
        egui::Window::new("Import archive - some content can't be kept")
            .open(&mut open)
            .resizable(false)
            .collapsible(false)
            .default_width(520.0)
            .show(ui.ctx(), |ui| {
                ui.colored_label(
                    super::theme::warning(ui.visuals()),
                    format!(
                        "This [{}] volume can't store everything in the archive:",
                        self.fs_type
                    ),
                );
                ui.add_space(4.0);
                for w in &pf.warnings {
                    ui.label(format!("- {w}"));
                }
                if pf.sidecars > 0 {
                    ui.label(format!(
                        "- {} macOS ._ sidecar(s) will be skipped.",
                        pf.sidecars
                    ));
                }
                ui.add_space(6.0);
                ui.label(format!(
                    "{} file(s) and {} dir(s) will be imported. Existing names are skipped.",
                    pf.files, pf.dirs
                ));
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Continue").clicked() {
                        decision = Some(true);
                    }
                    if ui.button("Cancel").clicked() {
                        decision = Some(false);
                    }
                });
            });
        if !open {
            decision = Some(false);
        }
        if let Some(yes) = decision {
            self.pane_act(&ui.ctx().clone(), BrowserPaneAction::ConfirmTarImport(yes));
        }
    }

    /// The pane's read-only archive viewer: entries with marks, the fork container, extract.
    fn render_archive_browse_window(&mut self, ui: &mut egui::Ui) {
        let Some(w) = self.ctl.pane(Side::Left).archive_viewer().cloned() else {
            return;
        };
        let mut acts: Vec<BrowserPaneAction> = Vec::new();
        let marked = w.marked_files();
        egui::Window::new(format!("Archive: {} ({})", w.entry_name, w.kind.label()))
            .id(egui::Id::unique("mac_archive_window"))
            .collapsible(false)
            .resizable(true)
            .default_size([640.0, 480.0])
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.label(format!(
                    "{} file(s) in archive (read-only view).",
                    w.file_count()
                ));
                ui.separator();
                egui::ScrollArea::vertical()
                    .max_height(320.0)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        egui::Grid::new("mac_archive_window_entries")
                            .striped(true)
                            .num_columns(5)
                            .spacing([12.0, 2.0])
                            .show(ui, |ui| {
                                for h in ["Extract", "Name", "Type/Creator", "Size", "Method"] {
                                    ui.strong(h);
                                }
                                ui.end_row();
                                for (i, e) in w.rows.iter().enumerate() {
                                    if e.is_dir {
                                        ui.label("");
                                        ui.label(format!("[ {} ]", e.path));
                                        for _ in 0..3 {
                                            ui.label("");
                                        }
                                        ui.end_row();
                                        continue;
                                    }
                                    let mut checked = w.marked[i];
                                    if ui.checkbox(&mut checked, "").changed() {
                                        acts.push(BrowserPaneAction::ArchiveMark(i));
                                    }
                                    ui.label(&e.path);
                                    ui.label(&e.type_creator);
                                    ui.label(human_size_b(e.size));
                                    ui.label(e.method);
                                    ui.end_row();
                                }
                            });
                    });
                ui.separator();
                ui.horizontal(|ui| {
                    ui.label("Extract as:");
                    let mut format = w.format;
                    egui::ComboBox::from_id_salt("mac_archive_window_fork_format")
                        .selected_text(format.label())
                        .show_ui(ui, |ui| {
                            for f in ForkFormat::ALL {
                                ui.selectable_value(&mut format, f, f.label());
                            }
                        });
                    if format != w.format {
                        let viewer = true;
                        acts.push(BrowserPaneAction::ArchiveFormat { viewer, format });
                    }
                    if ui.button("Extract All...").clicked() {
                        acts.push(BrowserPaneAction::ArchiveExtract { all: true });
                    }
                    let label = format!("Extract Selected... ({marked})");
                    if ui
                        .add_enabled(marked > 0, egui::Button::new(label))
                        .clicked()
                    {
                        acts.push(BrowserPaneAction::ArchiveExtract { all: false });
                    }
                    if ui.button("Close").clicked() {
                        acts.push(BrowserPaneAction::CloseArchive);
                    }
                });
            });
        let ctx = ui.ctx().clone();
        for a in acts {
            self.pane_act(&ctx, a);
        }
    }

    /// The pane's save dialog for a Mac archive on the volume: as stored, or decoded.
    fn render_archive_extract_dialog(&mut self, ui: &mut egui::Ui) {
        let Some(p) = self.ctl.pane(Side::Left).archive_save().cloned() else {
            return;
        };
        let mut acts: Vec<BrowserPaneAction> = Vec::new();
        egui::Window::new("Save Mac archive")
            .id(egui::Id::unique("archive_extract_dialog"))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.label(format!("{} is a {} archive.", p.entry_name, p.kind.label()));
                ui.add_space(4.0);
                ui.label("How would you like to save it?");
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    ui.label("Decoded fork container:");
                    let mut format = p.format;
                    egui::ComboBox::from_id_salt("archive_extract_fork_format")
                        .selected_text(format.label())
                        .show_ui(ui, |ui| {
                            for f in ForkFormat::ALL {
                                ui.selectable_value(&mut format, f, f.label());
                            }
                        });
                    if format != p.format {
                        let viewer = false;
                        acts.push(BrowserPaneAction::ArchiveFormat { viewer, format });
                    }
                });
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui
                        .button("Save as-is")
                        .on_hover_text(
                            "Write the raw archive bytes to the host. Useful when \
                             you're passing the archive along without unpacking.",
                        )
                        .clicked()
                    {
                        acts.push(BrowserPaneAction::SaveArchive { raw: true });
                    }
                    if ui
                        .button("Decode and save contents to a folder...")
                        .on_hover_text("Decompress on the host into the chosen fork container.")
                        .clicked()
                    {
                        acts.push(BrowserPaneAction::SaveArchive { raw: false });
                    }
                    if ui.button("Cancel").clicked() {
                        acts.push(BrowserPaneAction::CancelSaveArchive);
                    }
                });
            });
        let ctx = ui.ctx().clone();
        for a in acts {
            self.pane_act(&ctx, a);
        }
    }

    /// The items that could not be staged: the pane's host staging, then an archive import's failure.
    fn render_staging_errors_dialog(&mut self, ui: &mut egui::Ui) {
        let rows: Vec<(String, String)> = self.ctl.pane(Side::Left).staging_errors().to_vec();
        if rows.is_empty() {
            return;
        }
        let mut open = true;
        let mut dismiss = false;
        egui::Window::new("Could not add some files")
            .open(&mut open)
            .resizable(true)
            .collapsible(false)
            .default_width(520.0)
            .default_height(320.0)
            .show(ui.ctx(), |ui| {
                ui.colored_label(
                    super::theme::warning(ui.visuals()),
                    format!(
                        "{} item(s) could not be staged for the [{}] filesystem:",
                        rows.len(),
                        self.fs_type
                    ),
                );
                ui.add_space(4.0);
                ui.separator();
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .max_height(220.0)
                    .show(ui, |ui| {
                        for (path, reason) in &rows {
                            let label = Path::new(path)
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or(path);
                            ui.horizontal_wrapped(|ui| {
                                ui.label(egui::RichText::new(label).strong());
                                ui.label("-");
                                ui.colored_label(super::theme::danger(ui.visuals()), reason);
                            });
                            ui.label(
                                egui::RichText::new(path)
                                    .small()
                                    .color(super::theme::muted(ui.visuals())),
                            );
                            ui.add_space(4.0);
                        }
                    });
                ui.separator();
                ui.horizontal(|ui| {
                    if ui.button("OK").clicked() {
                        dismiss = true;
                    }
                });
            });
        if !open || dismiss {
            let act = BrowserPaneAction::DismissStagingErrors;
            self.ctl.apply(CommanderAction::Pane(Side::Left, act));
        }
    }

    /// Render the filesystem check results popup.
    fn render_chd_info_popup(&mut self, ui: &mut egui::Ui) {
        let Some(text) = self.ctl.chd_info().map(str::to_string) else {
            return;
        };
        let mut open = true;
        let mut buf = text;
        egui::Window::new("CHD Info")
            .open(&mut open)
            .resizable(true)
            .default_width(560.0)
            .default_height(420.0)
            .show(ui.ctx(), |ui| {
                egui::ScrollArea::both()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.add(
                            egui::TextEdit::multiline(&mut buf)
                                .font(egui::TextStyle::Monospace)
                                .desired_width(f32::INFINITY)
                                .desired_rows(20),
                        );
                    });
            });
        if !open {
            self.ctl
                .apply(CommanderAction::Mode(ModeAction::CloseChdInfo));
        }
    }

    /// The pane's check or repair report as the browser always showed it.
    fn render_fsck_popup(&mut self, ui: &mut egui::Ui) {
        let Some(report) = self.ctl.pane(Side::Left).tool_report().cloned() else {
            return;
        };
        let mut open = true;
        let mut do_repair = false;
        egui::Window::new("Filesystem Check Results")
            .open(&mut open)
            .resizable(true)
            .default_width(500.0)
            .show(ui.ctx(), |ui| {
                let Some(check) = &report.check else {
                    for line in &report.lines {
                        ui.label(line);
                    }
                    return;
                };
                if check.clean {
                    ui.colored_label(super::theme::success(ui.visuals()), "Filesystem is clean.");
                } else {
                    ui.colored_label(
                        super::theme::danger(ui.visuals()),
                        format!("{} error(s) found.", check.errors.len()),
                    );
                }
                ui.separator();
                ui.label(&check.stats_line);
                if !check.errors.is_empty() {
                    ui.separator();
                    ui.label(egui::RichText::new("Errors:").strong());
                    egui::ScrollArea::vertical()
                        .id_salt("fsck_errors")
                        .max_height(200.0)
                        .show(ui, |ui| {
                            for e in &check.errors {
                                ui.colored_label(super::theme::danger(ui.visuals()), e);
                            }
                        });
                }
                if !check.orphans.is_empty() {
                    ui.separator();
                    let total: usize = check.orphans.iter().map(|(_, v)| v.len()).sum();
                    let parents = check.orphans.len();
                    ui.colored_label(
                        super::theme::danger(ui.visuals()),
                        format!(
                            "{total} file(s)/folder(s) reference {parents} missing parent director{} - not repairable.",
                            if parents == 1 { "y" } else { "ies" },
                        ),
                    );
                    ui.label(
                        "These entries exist in the catalog but their parent directory is gone. \
                         This typically indicates severe directory corruption.",
                    );
                    egui::ScrollArea::vertical()
                        .id_salt("fsck_orphans")
                        .max_height(200.0)
                        .show(ui, |ui| {
                            for (parent, entries) in &check.orphans {
                                ui.colored_label(
                                    super::theme::warning(ui.visuals()),
                                    format!("Missing parent ID {parent}:"),
                                );
                                for e in entries {
                                    ui.label(format!("    {e}"));
                                }
                            }
                        });
                }
                let shown: Vec<_> = check
                    .warnings
                    .iter()
                    .filter(|(_, debug)| !debug || self.show_fsck_debug)
                    .collect();
                let has_debug = check.warnings.iter().any(|(_, debug)| *debug);
                if !shown.is_empty() {
                    ui.separator();
                    ui.label(egui::RichText::new("Warnings:").strong());
                    egui::ScrollArea::vertical()
                        .id_salt("fsck_warnings")
                        .max_height(200.0)
                        .show(ui, |ui| {
                            for (text, debug) in shown {
                                let color = if *debug {
                                    super::theme::muted(ui.visuals())
                                } else {
                                    super::theme::warning(ui.visuals())
                                };
                                ui.colored_label(color, text);
                            }
                        });
                }
                if has_debug {
                    ui.checkbox(&mut self.show_fsck_debug, "Show debug messages");
                }
                if check.repairable && !self.ctl.has_archive_flow() {
                    ui.separator();
                    let resp = ui
                        .add_enabled(self.ctl.edit_mode(), egui::Button::new("Repair"))
                        .on_disabled_hover_text("Turn on Edit Mode to repair the volume.");
                    if resp.clicked() {
                        do_repair = true;
                    }
                }
                if let Some(repair) = &report.repair {
                    ui.separator();
                    ui.label(egui::RichText::new("Repair Report:").strong());
                    for fix in &repair.applied {
                        ui.colored_label(super::theme::success(ui.visuals()), format!("  {fix}"));
                    }
                    for fail in &repair.failed {
                        ui.colored_label(super::theme::danger(ui.visuals()), format!("  {fail}"));
                    }
                    if repair.unrepairable > 0 {
                        ui.colored_label(
                            super::theme::warning(ui.visuals()),
                            format!(
                                "{} error(s) could not be repaired (missing parent directories).",
                                repair.unrepairable
                            ),
                        );
                    }
                }
            });
        let ctx = ui.ctx().clone();
        if !open {
            self.pane_act(&ctx, BrowserPaneAction::DismissToolReport);
        }
        if do_repair {
            self.pane_act(&ctx, BrowserPaneAction::Repair);
        }
    }

    fn render_tree_popup(&mut self, ui: &mut egui::Ui) {
        if !self.show_tree_popup {
            return;
        }
        let mut open = true;
        let mut save_requested = false;
        let mut regenerate = false;
        egui::Window::new("Tree View")
            .open(&mut open)
            .resizable(true)
            .default_width(600.0)
            .default_height(500.0)
            .show(ui.ctx(), |ui| {
                ui.horizontal(|ui| {
                    if ui.button("Copy to Clipboard").clicked() {
                        if let Some(text) = &self.tree_text {
                            ui.ctx().copy_text(text.clone());
                        }
                    }
                    if ui.button("Save to File...").clicked() {
                        save_requested = true;
                    }
                    ui.add_space(16.0);
                    if ui.checkbox(&mut self.tree_show_ids, "Show IDs").changed() {
                        regenerate = true;
                    }
                });
                ui.separator();
                if let Some(text) = &self.tree_text {
                    egui::ScrollArea::both()
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            ui.add(
                                egui::TextEdit::multiline(&mut text.as_str())
                                    .font(egui::TextStyle::Monospace)
                                    .desired_width(f32::INFINITY),
                            );
                        });
                }
            });
        if !open {
            self.show_tree_popup = false;
        }
        if regenerate {
            let ids = self.tree_show_ids;
            self.pane_act(&ui.ctx().clone(), BrowserPaneAction::ListTree { ids });
        }
        if save_requested {
            self.pane_act(&ui.ctx().clone(), BrowserPaneAction::SaveTree);
        }
    }

    /// Confirmation dialog shown when the generated tree text would choke the
    /// in-app multiline TextEdit. Offers a Save-to-File button (the primary
    /// path) and a smaller "Show in app anyway" escape hatch.
    fn render_tree_large_dialog(&mut self, ui: &mut egui::Ui) {
        if !self.show_tree_large_dialog {
            return;
        }
        let size = self.tree_text.as_deref().map(|t| t.len()).unwrap_or(0);
        let lines = self
            .tree_text
            .as_deref()
            .map(|t| t.bytes().filter(|b| *b == b'\n').count())
            .unwrap_or(0);
        let mut open = true;
        let mut save_clicked = false;
        let mut show_anyway = false;
        egui::Window::new("Tree view is large")
            .open(&mut open)
            .resizable(false)
            .collapsible(false)
            .show(ui.ctx(), |ui| {
                ui.label(format!(
                    "The generated tree is {:.1} MB (~{} entries).",
                    size as f64 / (1024.0 * 1024.0),
                    lines,
                ));
                ui.label(
                    "Rendering this many lines in the app would freeze the UI. \
                     Save it to a file instead.",
                );
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Save to File...").clicked() {
                        save_clicked = true;
                    }
                    if ui.small_button("Show in app anyway (slow)").clicked() {
                        show_anyway = true;
                    }
                });
            });
        if !open {
            self.show_tree_large_dialog = false;
        }
        if save_clicked {
            self.show_tree_large_dialog = false;
            self.pane_act(&ui.ctx().clone(), BrowserPaneAction::SaveTree);
        }
        if show_anyway {
            self.show_tree_large_dialog = false;
            self.show_tree_popup = true;
        }
    }

    /// Handle files dropped from the host OS onto the window.
    fn handle_dropped_files(&mut self, ui: &mut egui::Ui) {
        if !self.ctl.edit_mode() {
            return;
        }

        let dropped: Vec<PathBuf> = ui.ctx().input(|i| {
            i.raw
                .dropped_files
                .iter()
                .filter_map(|f| super::non_empty_path(f.path()))
                .collect()
        });

        if !dropped.is_empty() {
            self.add_host_paths(&ui.ctx().clone(), &dropped);
        }
    }

    /// The extraction's status line as the result (an "Open as Disk Image" copy reports through the modes).
    fn poll_extraction(&mut self) {
        if !self.extracting || self.ctl.copy_job().is_some() || self.effects.modal_open() {
            return;
        }
        if self.ctl.host_conflict().is_some() {
            return;
        }
        self.extracting = false;
        let said = self.ctl.status().to_string();
        if !said.is_empty() {
            self.extraction_result = Some(said);
        }
    }
}

/// Compact byte-count formatter for the Workflow E archive viewer
/// window. Mirrors `archives_tab::human_size` rather than
/// `format_size` (which uses base-1000 GB/MB and is meant
/// for storage-media advertising units) so the viewer matches the
/// Archives tab's display.
fn human_size_b(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}
