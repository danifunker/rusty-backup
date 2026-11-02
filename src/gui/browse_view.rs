// Browse-view filesystem-open helpers thread filesystem type, partition
// info, reader, ctx, logging, etc. — multi-arg by design.
#![allow(clippy::too_many_arguments)]

use rusty_backup::fs::entry::FileEntryTypeDisplay as _;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arc_mac::detect::{detect_mac_archive, MacArchiveKind};
use arc_mac::extract as mac_extract;
use arc_mac::stuffit::{build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod};
use disk_core::entry::{EntryType, FileEntry};
use disk_core::filesystem::Filesystem;
use dsk_clonezilla::block_cache::PartcloneBlockCache;
use rusty_backup::controller::app::{CommanderRecent, UserConfig};
use rusty_backup::controller::commander::{
    BrowserPaneAction, CommanderAction, CommanderController, DisplayRow, EditQueue, ExtractOptions,
    JobRegistry, ListingStyle, PaneProfile, PaneServices, ResourceForkMode, RowKind, Side,
    Snapshot,
};
use rusty_backup::controller::Controller as _;
use rusty_backup::fs::zstd_stream::ZstdStreamCache;
use rusty_backup::model::archive_edit::{self, ArchiveEditContext, ArchiveEditProgress};
use rusty_backup::model::browse_session::BrowseSession;
use rusty_backup::model::edit_queue::StagedEdit;
use rusty_backup::rbformats::chd_edit;

use super::commander::pane::PaneView;
use super::commander::DetailEditors;
use super::effects::{EffectHost, Reply};
use super::file_detail::{self, FileContent};
use super::metadata_editor;

/// Smallest file offered "Open as Disk Image": below this nothing holds a partitioned volume.
const MIN_NESTED_IMAGE_BYTES: u64 = 16 * 1024;
/// Free space kept spare beyond the image size when copying it out for "Open as Disk Image".
const NESTED_FREE_MARGIN: u64 = 64 * 1024 * 1024;

const MAX_PREVIEW_SIZE: usize = 1024 * 1024; // 1 MB max file preview

/// Upper bound on bytes read off the disk image to sniff whether the
/// selected file is a Mac archive (drives the "Save as Mac archive..." /
/// "Browse archive..." button visibility). The magics for SIT / SIT5 /
/// SEA sit at or near the start, and BinHex's banner is at byte 0, so a
/// generous prefix is enough to classify every archive we'd practically
/// meet on a vintage Mac volume while keeping the per-selection read
/// bounded. The click handlers still read the full file authoritatively.
const ARCHIVE_SNIFF_LIMIT: usize = 16 * 1024 * 1024; // 16 MB

/// Beyond this many bytes, the tree-view popup's `TextEdit::multiline` widget
/// becomes unresponsive (egui re-lays the entire text every frame). We route
/// past the threshold straight to a save-to-file dialog with a "show anyway"
/// escape hatch. 1 MiB ~= 30–50k lines depending on indentation.
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
    /// The session changed or the volume was written: the pane reopens on the next frame.
    pane_stale: bool,
    /// The entry the content panel shows: the cursor row, or the root when its row is clicked.
    selected_entry: Option<FileEntry>,
    /// Edits applied into a compressed CHD's diff that no flatten has
    /// committed yet; closing on an empty queue deleted that diff silently.
    chd_diff_dirty: bool,
    /// Chosen output format for the "Export selected" pulldown.
    export_format: rusty_backup::fs::export_selection::ExportFormat,
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
    /// "Full scan" toggle for the synthetic carve view: when off (default)
    /// only the first `carve::DEFAULT_SCAN_LIMIT` bytes are scanned for
    /// recoverable text; when on the whole image is scanned. Only shown for
    /// carve filesystems. Flipping it re-opens the volume.
    carve_full_scan: bool,
    volume_label: String,
    /// Optional prefix prepended to the volume label in the header (e.g. the
    /// Amiga drive name "DH0"). Set by the caller via `set_label_prefix`
    /// right after `open` so the header reads "Label: DH0 - Workbench". When
    /// the volume label is empty the prefix is shown on its own.
    label_prefix: String,
    /// Where to open the filesystem from (path / preopen handle / caches).
    /// `Clone` so background workers can be handed a session value.
    session: BrowseSession,
    /// Whether the browser is active (filesystem loaded).
    active: bool,
    /// Resource fork handling mode (HFS/HFS+ only).
    resource_fork_mode: ResourceForkMode,
    /// How to encode ProDOS type/aux on extract (ProDOS only).
    prodos_export_mode: ProdosExportMode,
    /// List deleted entries the filesystem can still read (QNX 2).
    show_deleted: bool,
    /// Convert RS line separators in text files to LF on extract (QNX 2).
    rs_to_lf: bool,
    /// An extraction runs as the screen's copy job; its status line becomes the result.
    extracting: bool,
    /// Message to show after extraction completes.
    extraction_result: Option<String>,
    /// Temp dir an "Open as Disk Image" extraction is writing into, until it finishes.
    nested_open: Option<tempfile::TempDir>,
    /// A finished "Open as Disk Image" extraction, for the Inspect tab to load.
    nested_ready: Option<(PathBuf, tempfile::TempDir)>,
    /// Whether edit mode is enabled (allows adding/deleting files).
    edit_mode: bool,
    /// Whether editing is possible for this source (not Clonezilla/streaming).
    edit_supported: bool,
    /// Status message from last edit operation.
    edit_result: Option<String>,
    /// Context for backup archive editing (decompress → edit → recompress).
    archive_edit_ctx: Option<ArchiveEditContext>,
    /// Progress for archive edit background operations.
    archive_edit_progress: Option<Arc<Mutex<ArchiveEditProgress>>>,
    /// Temp file path while editing an archive (cleaned up on close/save).
    archive_temp_path: Option<PathBuf>,
    /// Blessed (bootable) system folder info (HFS/HFS+ only).
    blessed_folder: Option<(u64, String)>,
    /// Whether the open HFS volume has boot blocks at sector 0 (`'LK'`).
    /// `None` until computed (or when not file-backed); computed lazily in
    /// edit mode where the backing bytes are raw. Drives the "Boot blocks:"
    /// status line and the "Boot Blocks…" button's hint.
    boot_blocks_present: Option<bool>,
    /// CHD info popup text. `Some` while the popup is open.
    chd_info_text: Option<String>,
    /// HFS+ journal history viewer. `Some` while the window is open.
    journal_view: Option<super::journal_view::JournalView>,
    /// Whether to show debug-level fsck messages.
    show_fsck_debug: bool,
    /// The repair report on screen was seen, so its follow-ups ran once.
    repair_seen: bool,
    /// Folder listings and previews waiting for the reader, one at a time.
    read_queue: std::collections::VecDeque<FsRead>,
    /// The read in flight, and the filesystem generation it borrowed from.
    read_job: Option<(rusty_backup::model::job::Job<FsReadOut>, u64)>,
    /// The screen's export of the marked rows is running; its status line is the result.
    exporting: bool,
    /// A running tar import (its preflight, or the import itself).
    tar_job: Option<rusty_backup::model::job::Job<TarStep>>,
    /// Backing up a CHD and opening its edit session, before edit mode starts.
    chd_open_job: Option<
        rusty_backup::model::job::Job<(
            PathBuf,
            Result<rusty_backup::model::chd_edit_open::ChdEditOpen, String>,
        )>,
    >,
    /// The pane's Apply in flight: what to do once it lands, the queue size, and whether it wrote.
    apply_follow: Option<(ApplyFollowUp, usize, bool)>,
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
    /// Free space of the editable volume: None = not probed, Some(None) = unavailable.
    edit_free_space: Option<Option<u64>>,
    /// The free-space probe in flight (an editable open decodes a whole container).
    free_space_job: Option<rusty_backup::model::job::Job<Option<u64>>>,
    /// The SquashFS size-budget dialog, open only between clicking Edit Mode on
    /// a SquashFS volume and choosing a ceiling. See
    /// [`super::squashfs_budget_dialog`].
    squashfs_budget_dialog: Option<Box<super::squashfs_budget_dialog::SquashfsBudgetDialog>>,
    /// Measuring a SquashFS for its budget dialog (walks the whole tree).
    squashfs_measure_job:
        Option<rusty_backup::model::job::Job<Result<fs_squashfs::squashfs_edit::SizePlan, String>>>,
    /// Whether to show the "unsaved changes" confirmation dialog.
    show_unsaved_dialog: bool,
    /// The text editor window, when open.
    text_editor: Option<GuiTextEditor>,
    /// When true, a successful Discard/Apply from the unsaved dialog should
    /// fully close the browse view rather than just leaving edit mode. Set by
    /// the in-view Close intercept when staged edits force the dialog first.
    pending_close: bool,
    /// Files that failed to stage (bad name, IO error, etc.). When non-empty,
    /// `show_staging_errors` drives a modal dialog listing each failure.
    staging_errors: Vec<(PathBuf, String)>,
    /// Whether the staging-errors modal dialog is open.
    show_staging_errors: bool,
    /// Live CHD edit-mode state. Present while editing a CHD via
    /// [`chd_edit::ChdEditSession`] (diff-against-parent for compressed,
    /// in-place for uncompressed). The `Arc<Mutex<ChdEditSession>>` itself
    /// lives on `self.session.chd_edit_session`; this struct just holds the
    /// paths needed to flatten / clean up on exit.
    chd_edit: Option<ChdEditState>,
    /// Background flatten progress (compressed CHD apply): merges the diff
    /// into a fresh compressed CHD that overwrites the parent.
    chd_flatten_progress: Option<Arc<Mutex<ChdFlattenProgress>>>,
    /// Background tar export progress ("Export to .tar.gz...").
    tar_export_progress: Option<Arc<Mutex<TarExportProgress>>>,
    /// A tar import awaiting the user's confirmation (preflight found
    /// content this filesystem can't represent).
    pending_tar_import: Option<PendingTarImport>,
    /// A tar archive dropped/added as a host file, awaiting the user's
    /// choice: import its contents, or add the archive as a plain file.
    pending_tar_add: Option<PathBuf>,
    /// When the CHD being edited is the body of a single-file-chd backup,
    /// this carries the backup folder path so that on flatten-success we
    /// can refresh `metadata.json` (per-partition checksums + container
    /// SHA-1). `None` for plain CHD images that aren't part of a backup.
    single_file_chd_backup_folder: Option<PathBuf>,

    /// User's password input text for the password prompt.
    password_input: String,

    /// One open `Filesystem` instance reused across read-only operations
    /// (directory listings, file previews, fsck, tree dumps). Re-opening for
    /// every operation forces a re-read of the entire catalog — fine for
    /// FAT, slow for HFS+ on a heavily-used volume. Read-only paths borrow
    /// this via `take_or_open_fs` and return it via `return_fs`. Any code
    /// that mutates the volume (sync_metadata, archive recompress, edit
    /// apply) calls `invalidate_cached_fs` so the next read sees disk truth.
    cached_fs: Option<Box<dyn Filesystem>>,
    /// Bumped on every invalidation; a worker's handle is only cached if it matches.
    fs_generation: u64,

    /// Queue of Mac archives (`.hqx` / `.sit` / `.sea` / `.sit.hqx` /
    /// `.sea.hqx`) that the user picked via Add File... — each waits on a
    /// user choice in the Workflow A modal (Convert / Expand / Add as-is).
    /// Drained by [`render_archive_import_dialog`] one item at a time.
    pending_archive_imports: Vec<PendingArchiveImport>,
    /// Holds extracted archive payloads on the host so the staged
    /// `AddFile` paths still resolve at Apply time. Created lazily on the
    /// first Convert/Expand action; dropped on Discard / browse close.
    archive_import_tempdir: Option<tempfile::TempDir>,
    /// Workflow C state — the user clicked "Save as Mac archive..." on a
    /// file inside the disk image that sniffed as a Mac archive, and the
    /// modal is open asking what to do with it.
    pending_archive_extract: Option<PendingArchiveExtract>,
    /// Workflow E state — a floating "Archive viewer" window the user
    /// opened with "Browse archive..." on a Mac archive file inside the
    /// disk image. Read-only: shows entries + a per-entry checkbox + the
    /// fork-format dropdown + Extract All / Extract Selected, mirroring
    /// the standalone Archives tab shape.
    mac_archive_window: Option<MacArchiveWindow>,
    /// Cached Mac-archive sniff for the current selection, keyed by path.
    /// `Some((path, Some(kind)))` means the file at `path` is a Mac
    /// archive of `kind`; `Some((path, None))` means it was sniffed and
    /// is not one. Gates the "Save as Mac archive..." / "Browse
    /// archive..." buttons so they only appear for actual archives rather
    /// than showing always and erroring on click. Recomputed only when
    /// the selected path changes — see [`Self::ensure_archive_sniff`].
    archive_sniff: Option<(String, Option<MacArchiveKind>)>,
    /// The sniff of the selected path, reading on a worker with the cached filesystem.
    archive_sniff_job: Option<(String, u64, rusty_backup::model::job::Job<ArchiveSniff>)>,
}

/// Workflow C state: the selected entry on the disk image is a Mac
/// archive and the user clicked "Save as Mac archive...". The raw
/// bytes are kept around so "Save as-is" doesn't need to re-read; the
/// parsed archive + ready-for-extract bytes drive the "Decode and
/// save" path. `fork_format` is the user-editable container choice for
/// the decode path (BinHex / MacBinary / AppleDouble / Raw).
struct PendingArchiveExtract {
    entry_name: String,
    kind: MacArchiveKind,
    raw_bytes: Vec<u8>,
    extract_bytes: Vec<u8>,
    archive: arc_mac::stuffit::StuffItArchive,
    fork_format: mac_extract::ForkFormat,
}

/// Workflow E state — the floating "Archive viewer" window opened on a
/// Mac archive that lives inside the disk image. Read-only browse +
/// extract; mirrors the standalone Archives tab shape so the UX is
/// consistent. Per-entry `selected` checkboxes drive "Extract
/// Selected".
struct MacArchiveWindow {
    entry_name: String,
    kind: MacArchiveKind,
    bytes: Vec<u8>,
    archive: arc_mac::stuffit::StuffItArchive,
    selected: Vec<bool>,
    fork_format: mac_extract::ForkFormat,
}

/// One Mac archive the user picked via Add File..., awaiting their
/// Convert/Expand/Add-as-is choice in the import modal (Workflow A).
/// The archive's bytes are read up-front so the modal can show the
/// correct three-option set (which depends on whether the file is
/// HQX-wrapping-a-SIT vs a plain SIT, etc.).
#[derive(Clone)]
struct PendingArchiveImport {
    /// The original host path the user picked.
    host_path: PathBuf,
    /// Where on the disk image to land the result.
    parent: FileEntry,
    /// Sniffed archive kind — drives modal copy + which buttons appear.
    kind: MacArchiveKind,
    /// Raw bytes of the picked file, kept around so the action handler
    /// doesn't have to re-read after the user clicks.
    raw: Vec<u8>,
}

/// State carried for the duration of a CHD edit-mode session.
struct ChdEditState {
    /// Original CHD file the user is editing (and which `flatten_to_parent`
    /// will overwrite on exit).
    parent_path: PathBuf,
    /// Companion diff file. `Some` for compressed CHDs; `None` when editing
    /// an uncompressed CHD in place.
    diff_path: Option<PathBuf>,
}

/// Background flatten progress for compressed CHD edit-apply. Mirrors
/// [`ArchiveEditProgress`] but lives in the GUI crate so the worker thread
/// can update it without pulling chd_edit into model/.
struct ChdFlattenProgress {
    current: u64,
    total: u64,
    finished: bool,
    error: Option<String>,
    cancel_requested: bool,
}

/// Background progress for "Export to .tar.gz..." — archiving the whole
/// volume to a tar stream on a worker thread.
struct TarExportProgress {
    files: u64,
    dirs: u64,
    symlinks: u64,
    bytes: u64,
    finished: bool,
    error: Option<String>,
    out_path: std::path::PathBuf,
}

/// A tar import awaiting user confirmation because the preflight found
/// content the target filesystem can't represent (dropped symlinks, invalid
/// names, …). Cleared when the user confirms or cancels the modal.
#[derive(Clone)]
struct PendingTarImport {
    archive_path: std::path::PathBuf,
    preflight: fs_tar::tar_import::TarImportPreflight,
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
            ctl: new_controller(),
            effects: EffectHost::default(),
            pane_view: PaneView::new(Side::Left),
            keys_active: false,
            scroll_cursor: false,
            cursor_seen: None,
            cursor_armed: false,
            pane_stale: false,
            selected_entry: None,
            chd_diff_dirty: false,
            export_format: rusty_backup::fs::export_selection::ExportFormat::MacArchive,
            side_ed: DetailEditors::default(),
            view_mode: ViewMode::Auto,
            error: None,
            fs_type: String::new(),
            volume_total: 0,
            volume_used: 0,
            carve_full_scan: rusty_backup::fs::carve::full_scan_enabled(),
            volume_label: String::new(),
            label_prefix: String::new(),
            session: BrowseSession::new(),
            active: false,
            resource_fork_mode: ResourceForkMode::AppleDouble,
            prodos_export_mode: ProdosExportMode::WithTypeSuffix,
            show_deleted: false,
            rs_to_lf: false,
            extracting: false,
            extraction_result: None,
            nested_open: None,
            nested_ready: None,
            edit_mode: false,
            edit_supported: false,
            edit_result: None,
            archive_edit_ctx: None,
            archive_edit_progress: None,
            archive_temp_path: None,
            blessed_folder: None,
            boot_blocks_present: None,
            chd_info_text: None,
            journal_view: None,
            show_fsck_debug: false,
            repair_seen: false,
            read_queue: std::collections::VecDeque::new(),
            read_job: None,
            exporting: false,
            tar_job: None,
            chd_open_job: None,
            apply_follow: None,
            tree_text: None,
            show_tree_popup: false,
            tree_show_ids: false,
            show_tree_large_dialog: false,
            edit_free_space: None,
            free_space_job: None,
            show_unsaved_dialog: false,
            text_editor: None,
            pending_close: false,
            staging_errors: Vec::new(),
            show_staging_errors: false,
            chd_edit: None,
            chd_flatten_progress: None,
            squashfs_budget_dialog: None,
            squashfs_measure_job: None,
            tar_export_progress: None,
            pending_tar_import: None,
            pending_tar_add: None,
            single_file_chd_backup_folder: None,
            password_input: String::new(),
            cached_fs: None,
            fs_generation: 0,
            pending_archive_imports: Vec::new(),
            archive_import_tempdir: None,
            pending_archive_extract: None,
            mac_archive_window: None,
            archive_sniff: None,
            archive_sniff_job: None,
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
    shape: rusty_backup::model::text_edit::TextShape,
    /// The endings it was found with, so a conversion can be shown as one.
    original_ending: rusty_backup::model::text_edit::LineEnding,
    dirty: bool,
    status: Option<String>,
}

impl BrowseView {
    /// The label of a running extract, archive edit, CHD flatten or tar export, if any.
    pub fn running_job(&self) -> Option<&'static str> {
        use rusty_backup::model::worker::lock_status;
        if self.extracting && self.ctl.copy_job().is_some() {
            Some("Extract")
        } else if self
            .archive_edit_progress
            .as_ref()
            .is_some_and(|p| !lock_status(p).finished)
        {
            Some("Archive edit")
        } else if self
            .chd_flatten_progress
            .as_ref()
            .is_some_and(|p| !lock_status(p).finished)
        {
            Some("CHD flatten")
        } else if self
            .tar_export_progress
            .as_ref()
            .is_some_and(|p| !lock_status(p).finished)
        {
            Some("Tar export")
        } else if self.apply_follow.is_some() {
            Some("Apply edits")
        } else if self.tar_job.is_some() {
            Some("Tar import")
        } else if self.ctl.pane(Side::Left).tool_working().is_some() {
            Some("Volume tool")
        } else {
            None
        }
    }

    /// Initialize the browser for a partition within a source image/device.
    ///
    /// `preopen_file` — if provided, this already-elevated file handle is used
    /// for all filesystem reads instead of re-opening `source_path`.  Pass
    /// `Some(file)` when browsing a raw device on macOS to avoid a second auth
    /// prompt; pass `None` for backup files / image files.
    /// Open the browser from a fully-built [`BrowseSession`]. This is the
    /// low-level seam every other opener funnels through: it clears prior
    /// browse state, installs the session, and spawns the async open + initial
    /// root listing on a worker thread (`show()` drains the result each frame).
    ///
    /// It deliberately does **not** decide edit policy — `edit_supported`, the
    /// WOZ/zstd `archive_edit_ctx`, the CHD-edit flow, and the single-file-chd
    /// backup folder are layered by the caller after this returns (see `open`,
    /// and the Inspect tab's resolver-driven routing).
    pub fn open_with_session(&mut self, session: BrowseSession) {
        self.close();
        self.session = session;
        self.active = true;
        self.reopen_pane();
    }

    /// Hand the pane the current session; it lists the volume in a job.
    fn reopen_pane(&mut self) {
        self.pane_stale = false;
        let label = self
            .session
            .source_path
            .as_ref()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "partition".into());
        self.ctl.open_fixed_session(self.session.clone(), &label);
        self.cursor_seen = None;
        self.cursor_armed = false;
    }

    /// The pane's staged-edit queue.
    fn queue(&self) -> &EditQueue {
        &self.ctl.pane(Side::Left).queue
    }

    pub fn open(
        &mut self,
        source_path: PathBuf,
        partition_offset: u64,
        partition_type: u8,
        partition_type_string: Option<String>,
        preopen_file: Option<File>,
    ) {
        let session = BrowseSession {
            source_path: Some(source_path.clone()),
            partition_offset,
            partition_type,
            partition_type_string,
            preopen_file: preopen_file.map(std::sync::Arc::new),
            ..Default::default()
        };
        self.open_with_session(session);

        // Editing is supported for regular files (not Clonezilla/streaming).
        self.edit_supported = true;

        // WOZ images still go through the decompress→edit→recompress
        // archive flow (no in-place / diff support). CHDs use the lighter
        // chd_edit path: nothing to set up here — the edit-mode toggle
        // calls `enter_chd_edit_mode` which opens a `ChdEditSession`
        // (diff-against-parent for compressed, in-place for uncompressed).
        let ext = source_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if ext == "woz" {
            let original_size = dsk_apple::woz::WozReader::open(&source_path)
                .map(|r| r.len())
                .unwrap_or(0);
            log::info!(
                "Container edit context set for {} ({} decoded bytes)",
                source_path.display(),
                original_size
            );
            self.archive_edit_ctx = Some(ArchiveEditContext {
                archive_path: source_path.clone(),
                compression_type: "woz".to_string(),
                original_size,
                compacted: false,
                metadata_path: PathBuf::new(),
                partition_index: 0,
                checksum_type: String::new(),
            });
        }
    }

    /// Open the browser using a partclone block cache (for Clonezilla images).
    pub fn open_partclone(&mut self, cache: Arc<Mutex<PartcloneBlockCache>>, partition_type: u8) {
        self.open_with_session(BrowseSession {
            partclone_cache: Some(cache),
            partition_type,
            partition_offset: 0,
            ..Default::default()
        });
    }

    /// Open the browser by streaming a native zstd-compressed partition image.
    ///
    /// The filesystem opens immediately via a `ZstdStreamReader` backed by a
    /// 256 MB in-memory buffer.  Call `upgrade_to_seekable_cache` once the
    /// background seekable cache is ready to enable full random access.
    pub fn open_streaming(&mut self, path: PathBuf, ptype: u8, ptype_str: Option<String>) {
        let cache = match ZstdStreamCache::new(&path) {
            Ok(c) => Arc::new(Mutex::new(c)),
            Err(e) => {
                self.close();
                self.error = Some(format!("Cannot open zstd stream: {e}"));
                self.active = true;
                return;
            }
        };
        // Open + initial root listing on a worker thread so the UI can paint
        // a spinner while a slow first read (NAS, large HFS+ catalog) runs.
        self.open_with_session(BrowseSession {
            partition_type: ptype,
            partition_type_string: ptype_str,
            partition_offset: 0,
            zstd_cache: Some(cache),
            ..Default::default()
        });
    }

    /// Set a label prefix shown alongside the volume label in the header.
    /// Used to surface the Amiga drive name (e.g. "DH0") so users don't lose
    /// track of which RDB partition they're browsing when several share the
    /// same volume name (or none have one). Pass an empty string to clear.
    pub fn set_label_prefix(&mut self, prefix: String) {
        self.label_prefix = prefix;
    }

    /// Mark the open source as editable in place (no archive decompress flow).
    /// `open` sets this for image/device sources; a resolver-built session
    /// opened via [`open_with_session`](Self::open_with_session) starts
    /// read-only, so the caller flips it on for raw / CHD backup partitions
    /// (the edit-mode toggle then picks direct vs `chd_edit` by sniffing the
    /// source). Compressed (zstd / woz) backups use
    /// [`set_archive_edit_context`](Self::set_archive_edit_context) instead.
    pub fn mark_edit_supported(&mut self) {
        self.edit_supported = true;
    }

    /// Set up archive edit context so that toggling edit mode triggers
    /// decompress → edit → recompress flow instead of direct editing.
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
        self.archive_edit_ctx = Some(ArchiveEditContext {
            archive_path,
            compression_type,
            original_size,
            compacted,
            metadata_path,
            partition_index,
            checksum_type,
        });
        self.edit_supported = true;
    }

    /// Mark the currently-open CHD as the body of a single-file-chd backup.
    /// On a successful chd_edit flatten we'll refresh the backup's
    /// `metadata.json` (per-partition SHA-256 + container SHA-1).
    pub fn set_single_file_chd_backup_folder(&mut self, backup_folder: PathBuf) {
        self.single_file_chd_backup_folder = Some(backup_folder);
    }

    /// (`zstd_cache` is `Some`).  The directory cache is preserved so the
    /// user stays in the same place in the tree.
    pub fn upgrade_to_seekable_cache(&mut self, cache_path: PathBuf) {
        if !self.active || self.session.zstd_cache.is_none() {
            return;
        }
        self.session.source_path = Some(cache_path);
        self.session.zstd_cache = None;
        self.pane_stale = true;
    }

    pub fn close(&mut self) {
        // A fresh pane drops the old one's readers before any CHD diff below is removed.
        self.ctl = new_controller();
        self.effects = EffectHost::default();
        self.keys_active = false;
        self.cursor_seen = None;
        self.edit_free_space = None;
        self.free_space_job = None;
        self.selected_entry = None;
        self.side_ed = DetailEditors::default();
        self.error = None;
        self.active = false;
        self.fs_type.clear();
        self.volume_total = 0;
        self.volume_used = 0;
        self.volume_label.clear();
        self.label_prefix.clear();
        self.session = BrowseSession::new();
        self.extracting = false;
        self.extraction_result = None;
        self.edit_mode = false;
        self.edit_supported = false;
        self.edit_result = None;
        self.blessed_folder = None;
        self.journal_view = None;
        self.tree_text = None;
        self.show_tree_popup = false;
        self.show_tree_large_dialog = false;
        self.tree_show_ids = false;
        self.show_unsaved_dialog = false;
        self.pending_close = false;
        self.pending_archive_imports.clear();
        self.archive_import_tempdir = None;
        self.pending_archive_extract = None;
        self.mac_archive_window = None;
        self.archive_sniff = None;
        self.archive_sniff_job = None;
        self.read_queue.clear();
        self.read_job = None;
        self.password_input.clear();
        self.cached_fs = None;
        // Clean up archive temp file if present
        if let Some(temp) = self.archive_temp_path.take() {
            let _ = std::fs::remove_file(&temp);
        }
        self.archive_edit_ctx = None;
        self.archive_edit_progress = None;
        // Drop any live CHD edit session and remove its diff. The parent
        // CHD is left untouched; user's `.chd_backup` is preserved.
        self.discard_chd_edit_session();
        self.chd_flatten_progress = None;
        self.tar_export_progress = None;
        self.pending_tar_import = None;
        self.pending_tar_add = None;
        self.single_file_chd_backup_folder = None;
        // Dialogs and editors that name a file on the volume being closed;
        // left open, their Save/Overwrite acted on the next volume.
        self.text_editor = None;
        self.boot_blocks_present = None;
        self.pane_stale = false;
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    /// True when the view is open in edit mode with unapplied staged edits —
    /// i.e. closing / switching the source would lose work. Lets a caller gate
    /// a source switch behind its own confirm dialog (see Inspect's deferred
    /// source-switch guard) before calling [`close`](Self::close).
    pub fn has_unsaved_edits(&self) -> bool {
        self.active && self.edit_mode && (!self.queue().is_empty() || self.chd_diff_dirty)
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

    /// Sniff the selected file for Mac-archive content, caching the result
    /// by path so the disk read + [`detect_mac_archive`] runs once per
    /// selection rather than every frame. Returns the detected kind, if
    /// any. No-op (returns `None`) for non-files, non-Mac filesystems, or
    /// when the bytes can't be read. The read is bounded by
    /// [`ARCHIVE_SNIFF_LIMIT`]; the click handlers re-detect on the full
    /// file, so a >16 MB archive (vanishingly rare on a vintage volume)
    /// simply won't surface the buttons rather than mis-detecting.
    fn ensure_archive_sniff(&mut self, entry: &FileEntry) -> Option<MacArchiveKind> {
        if !entry.is_file() || !self.is_mac_filesystem() {
            self.archive_sniff = None;
            return None;
        }
        self.poll_archive_sniff();
        if let Some((path, kind)) = &self.archive_sniff {
            if path == &entry.path {
                return *kind;
            }
        }
        if self.archive_sniff_job.is_some() {
            // One sniff at a time; a newer selection starts when this one lands.
            return None;
        }
        // The cached filesystem goes to the worker and comes back; with none cached, the worker opens one.
        let cached = self.cached_fs.take();
        let session = self.session.clone();
        let show_deleted = self.show_deleted;
        let target = entry.clone();
        let job = rusty_backup::model::job::Job::spawn(
            "Checking for an archive",
            Arc::new(super::ui_logger::EguiWake),
            move |_| {
                let mut fs = match cached {
                    Some(fs) => fs,
                    None => match session.open() {
                        Ok(mut fs) => {
                            if show_deleted {
                                fs.set_show_deleted(true);
                            }
                            fs
                        }
                        Err(_) => return Ok((None, None)),
                    },
                };
                let kind = fs
                    .read_file(&target, ARCHIVE_SNIFF_LIMIT)
                    .ok()
                    .and_then(|bytes| detect_mac_archive(&bytes));
                Ok((Some(fs), kind))
            },
        );
        self.archive_sniff_job = Some((entry.path.clone(), self.fs_generation, job));
        None
    }

    /// Take a finished sniff: cache its verdict and put the filesystem back.
    fn poll_archive_sniff(&mut self) {
        let Some((_, _, job)) = &self.archive_sniff_job else {
            return;
        };
        if job.is_running() {
            return;
        }
        let out = job.take_result();
        let Some((path, generation, _)) = self.archive_sniff_job.take() else {
            return;
        };
        let (fs, kind) = out.unwrap_or((None, None));
        // A filesystem from before an invalidation is stale; drop it.
        if let Some(fs) = fs {
            if self.cached_fs.is_none() && generation == self.fs_generation {
                self.return_fs(fs);
            }
        }
        // A failed open or read caches "not an archive" for this path instead of retrying every frame.
        self.archive_sniff = Some((path, kind));
    }

    pub fn show(&mut self, ui: &mut egui::Ui) {
        if !self.active {
            return;
        }
        self.render_tool_job(ui);
        self.poll_editor();
        if let Some(snap) = self.ctl.pane(Side::Left).tool_working() {
            egui::Window::new(snap.label.clone())
                .collapsible(false)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    super::progress::show_job(ui, &snap);
                });
        }
        self.poll_reads();
        if let Some((job, _)) = &self.read_job {
            let snap = job.snapshot();
            if snap.label == ARCHIVE_READ_LABEL {
                egui::Window::new(snap.label.clone())
                    .collapsible(false)
                    .resizable(false)
                    .show(ui.ctx(), |ui| {
                        super::progress::show_job(ui, &snap);
                    });
            }
        }
        self.poll_archive_sniff();
        self.render_apply_job(ui);
        self.poll_tar_job();
        if let Some(snap) = self.tar_job.as_ref().map(|j| j.snapshot()) {
            egui::Window::new(snap.label.clone())
                .collapsible(false)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    super::progress::show_job(ui, &snap);
                });
        }
        self.poll_export(ui);
        self.poll_chd_open_job();
        if let Some(job) = &self.chd_open_job {
            let snap = job.snapshot();
            egui::Window::new(snap.label.clone())
                .collapsible(false)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    super::progress::show_job(ui, &snap);
                });
        }

        if self.pane_stale {
            self.reopen_pane();
        }
        self.ctl.host_pane().set_read_only(!self.edit_mode);
        let ctx = ui.ctx().clone();
        let fx = self.ctl.tick();
        self.run_effects(&ctx, fx);
        self.poll_apply();
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
                        self.session.password = Some(secret.clone());
                        self.error = None;
                        let act = BrowserPaneAction::SubmitPassword(secret);
                        self.ctl.apply(CommanderAction::Pane(Side::Left, act));
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
                    disk_core::display::format_size(self.volume_used),
                    disk_core::display::format_size(self.volume_total),
                    disk_core::display::format_size(free),
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
            if self.edit_mode && self.is_hfs_type() {
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
                let resp = ui.checkbox(&mut self.carve_full_scan, "Full scan");
                let limit_mb = rusty_backup::fs::carve::DEFAULT_SCAN_LIMIT / (1024 * 1024);
                resp.clone().on_hover_text(format!(
                    "Off: scan only the first {limit_mb} MB for recoverable text (fast).\n\
                     On: scan the entire image (slower on large disks).\n\
                     'whole-disk.img' is always the full image regardless."
                ));
                if resp.changed() {
                    rusty_backup::fs::carve::set_full_scan(self.carve_full_scan);
                    self.error = None;
                    self.invalidate_cached_fs();
                }
            }

            if self.fs_type == "QNX 2" {
                ui.add_space(8.0);
                let resp = ui.checkbox(&mut self.show_deleted, "Show deleted");
                resp.clone().on_hover_text(
                    "List deleted entries whose data can still be read, named 'NAME [deleted]'.",
                );
                if resp.changed() {
                    self.session.show_deleted = self.show_deleted;
                    self.invalidate_all_caches();
                }
            }

            // Edit mode toggle (Pilot/Cedar volumes are read-only for now)
            if self.edit_supported && self.fs_type != "Pilot/Cedar" {
                let busy = self.extracting
                    || self.archive_edit_progress.is_some()
                    || self.chd_flatten_progress.is_some();
                ui.add_space(8.0);
                let edit_btn = if self.edit_mode {
                    egui::Button::new("Edit Mode ON")
                } else {
                    egui::Button::new("Edit Mode")
                };
                let btn = ui.add_enabled(!busy, edit_btn);
                if btn.clicked() {
                    log::info!(
                        "Edit Mode clicked: archive_edit_ctx={}, chd_edit={}, edit_mode={}",
                        self.archive_edit_ctx.is_some(),
                        self.chd_edit.is_some(),
                        self.edit_mode
                    );
                    if self.is_chd_source() {
                        // CHD editing: open ChdEditSession on entry; flatten
                        // the diff back into the parent on exit.
                        if !self.edit_mode {
                            self.enter_chd_edit_mode();
                        } else if !self.ctl.pane(Side::Left).queue.is_empty() {
                            self.show_unsaved_dialog = true;
                        } else {
                            self.exit_edit_mode();
                        }
                    } else if self.archive_edit_ctx.is_some() {
                        // Archive editing: toggle triggers decompress/recompress
                        if !self.edit_mode {
                            log::info!("Starting archive extract for editing");
                            self.start_archive_extract();
                        } else if !self.ctl.pane(Side::Left).queue.is_empty() {
                            self.show_unsaved_dialog = true;
                        } else {
                            self.start_archive_compress();
                        }
                    } else {
                        // Direct editing (raw image / device)
                        if self.edit_mode
                            && (!self.ctl.pane(Side::Left).queue.is_empty() || self.chd_diff_dirty)
                        {
                            self.show_unsaved_dialog = true;
                        } else if !self.edit_mode {
                            // SquashFS has to declare a size budget before any
                            // edit is made — it has no free space to consult and
                            // no in-place write, so "will this still fit?" can
                            // only be answered as a ceiling chosen up front.
                            // The dialog answers it and re-runs this entry when
                            // the user commits. See docs/squashfs_edit.md §2.
                            if !self.open_squashfs_budget_dialog() {
                                self.enter_direct_edit_mode();
                            }
                        } else {
                            self.edit_mode = false;
                            self.edit_result = None;
                            self.ctl.host_pane().queue.clear();
                            self.pending_archive_imports.clear();
                            self.archive_import_tempdir = None;
                        }
                    }
                }
                if !self.edit_mode && btn.hovered() {
                    btn.on_hover_text("Enable editing to add or delete files on this image");
                }
            }

            // Export the whole volume to a tar archive (.tar.gz / .tar.zst /
            // .tar). Preserves case-sensitive names + real symlinks (data
            // fork only) so a case-insensitive host won't clobber files that
            // differ only in case. Runs on a worker thread.
            if !self.edit_mode && self.tar_export_progress.is_none() {
                let btn = ui.button("Export to .tar.gz...");
                if btn.hovered() {
                    btn.clone().on_hover_text(
                        "Archive the whole volume to .tar.gz / .tar.zst / .tar \
                         (preserves case + symlinks; choose the extension in the save dialog)",
                    );
                }
                if btn.clicked() {
                    self.start_tar_export(&FileEntry::root());
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

            if self.is_chd_source() {
                if let Some(chd_path) = self.session.source_path.clone() {
                    if ui.button("CHD Info").clicked() {
                        match rusty_backup::rbformats::chd::format_chd_info(&chd_path) {
                            Ok(text) => self.chd_info_text = Some(text),
                            Err(e) => {
                                self.error = Some(format!("CHD Info failed: {}", e));
                            }
                        }
                    }
                }
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
                if self.edit_mode
                    && (!self.ctl.pane(Side::Left).queue.is_empty() || self.chd_diff_dirty)
                {
                    self.show_unsaved_dialog = true;
                } else {
                    self.close();
                }
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

        // Archive edit progress bar
        self.poll_archive_edit(ui);
        if let Some(progress) = &self.archive_edit_progress {
            if let Ok(p) = progress.lock() {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label(format!("{}...", p.phase));
                    if p.total > 0 {
                        let frac = p.current as f32 / p.total as f32;
                        ui.add(egui::ProgressBar::new(frac).show_percentage());
                    }
                });
                ui.ctx().request_repaint();
            }
        }

        // The SquashFS size-budget dialog, and the edit-mode entry it gates.
        self.poll_squashfs_measure(ui);
        self.poll_squashfs_budget_dialog(ui);

        // Background tar-export progress (Export to .tar.gz...).
        self.poll_tar_export(ui);

        // CHD flatten progress bar (compressed CHD edit-apply)
        self.poll_chd_flatten(ui);
        if let Some(progress) = &self.chd_flatten_progress {
            if let Ok(p) = progress.lock() {
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Flattening CHD...");
                    if p.total > 0 {
                        let frac = p.current as f32 / p.total as f32;
                        ui.add(egui::ProgressBar::new(frac).show_percentage());
                    }
                });
                ui.ctx().request_repaint();
            }
        }

        // Info banner while editing an archive
        if self.edit_mode && self.archive_edit_ctx.is_some() {
            ui.colored_label(
                super::theme::info(ui.visuals()),
                "Editing temporary copy. Click 'Apply Edits' to write changes.",
            );
        }
        if self.edit_mode && self.chd_edit.is_some() {
            ui.colored_label(
                super::theme::info(ui.visuals()),
                "Editing CHD via diff. Click 'Apply Edits' to flush, or exit Edit Mode to merge into the CHD.",
            );
        }

        // Edit mode toolbar
        if self.edit_mode {
            self.render_edit_toolbar(ui);
        }

        ui.separator();

        // Show drop target overlay when files are being dragged over
        if self.edit_mode {
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
        if self.edit_mode && (!self.queue().is_empty() || self.chd_diff_dirty) {
            self.pending_close = true;
            self.show_unsaved_dialog = true;
        } else {
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
                    let desc = fs_apple2::prodos_types::type_description(tt);
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
        use rusty_backup::fs::export_selection::ExportFormat;
        let mut fmt = self.export_format;
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
        self.export_format = fmt;
        if clear {
            let act = CommanderAction::Pane(Side::Left, BrowserPaneAction::ClearSelection);
            self.ctl.apply(act);
        }
        if do_export {
            self.export_marked(&ui.ctx().clone(), fmt);
        }
    }

    /// Export the marked rows as one `format` output through the screen's export (picker, job).
    fn export_marked(
        &mut self,
        ctx: &egui::Context,
        format: rusty_backup::fs::export_selection::ExportFormat,
    ) {
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
            if !fs_apple2::prodos_types::is_known_type(t) {
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
            format!(
                "[{}+rsrc {}]",
                codes,
                disk_core::display::format_size(imp.data.len() as u64)
            )
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

    /// Start the next queued read: it borrows the cached filesystem, or opens one off the UI thread.
    fn pump_reads(&mut self) {
        if self.read_job.is_some() {
            return;
        }
        let Some(read) = self.read_queue.pop_front() else {
            return;
        };
        let cached = self.cached_fs.take();
        let session = self.session.clone();
        let show_deleted = self.show_deleted;
        let label = ARCHIVE_READ_LABEL;
        let job = rusty_backup::model::job::Job::spawn(
            label,
            Arc::new(super::ui_logger::EguiWake),
            move |_| {
                let mut fs = match cached {
                    Some(fs) => fs,
                    None => match session.open() {
                        Ok(mut fs) => {
                            if show_deleted {
                                fs.set_show_deleted(true);
                            }
                            fs
                        }
                        Err(e) => {
                            return Ok((
                                None,
                                read.failed(format!("Failed to open filesystem: {e}")),
                            ))
                        }
                    },
                };
                let done = match read {
                    FsRead::Archive(entry, purpose) => {
                        let result = read_mac_archive(&mut *fs, &entry, purpose);
                        FsReadDone::Archive(entry, purpose, result)
                    }
                };
                Ok((Some(fs), done))
            },
        );
        self.read_job = Some((job, self.fs_generation));
    }

    /// Apply a finished read, put the filesystem back, and start the next.
    fn poll_reads(&mut self) {
        let Some((job, _)) = &self.read_job else {
            self.pump_reads();
            return;
        };
        if job.is_running() {
            return;
        }
        let out = job.take_result();
        let Some((_, generation)) = self.read_job.take() else {
            return;
        };
        if let Some((fs, done)) = out {
            // A filesystem from before an invalidation is stale; so is what it read.
            let fresh = generation == self.fs_generation;
            if let Some(fs) = fs {
                if fresh && self.cached_fs.is_none() {
                    self.return_fs(fs);
                }
            }
            if fresh {
                self.apply_read(done);
            }
        }
        self.pump_reads();
    }

    fn apply_read(&mut self, done: FsReadDone) {
        match done {
            FsReadDone::Archive(entry, purpose, result) => {
                self.apply_archive_read(entry, purpose, result);
            }
        }
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
                if !fs_apple2::prodos_types::is_known_type(tt) {
                    let aux = entry.aux_type.unwrap_or(0);
                    ui.colored_label(
                        super::theme::warning(ui.visuals()),
                        format!(
                            "Note: ${:02X} is not in the ProDOS type registry - {}. The type and aux (${:04X}) will be preserved on export via the CiderPress #{:02X}{:04X} filename suffix.",
                            tt,
                            fs_apple2::prodos_types::UNKNOWN_TYPE_NOTE,
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
                        self.tar_export_progress.is_none(),
                        egui::Button::new(tgz_label),
                    )
                    .on_hover_text(
                        "Save the selection as a single .tar.gz / .tar.zst / .tar \
                         (preserves exact case + symlinks; pick the extension in \
                         the save dialog)",
                    )
                    .clicked()
                {
                    self.start_tar_export(&entry);
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
                    self.start_nested_open(&ctx, &entry);
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
                    self.open_archive_extract_modal(&entry);
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
                    self.open_archive_browse_window(&entry);
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
                        self.export_selection_as_archive(&entry, ArchiveExportFormat::Hqx);
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
                        self.export_selection_as_archive(&entry, ArchiveExportFormat::Sit);
                    }
                    if ui
                        .button("StuffIt-over-BinHex (.sit.hqx)")
                        .on_hover_text(
                            "StuffIt bundle wrapped in BinHex for transport-safe ASCII. \
                         The classic \"emailed me a Mac app\" format.",
                        )
                        .clicked()
                    {
                        self.export_selection_as_archive(&entry, ArchiveExportFormat::SitHqx);
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
                        self.export_selection_as_archive(&entry, ArchiveExportFormat::Mar);
                    }
                });
            }
        }

        ui.separator();

        if entry.size > MAX_PREVIEW_SIZE as u64 && entry.is_file() {
            ui.label(format!(
                "File too large to preview ({}).",
                disk_core::display::format_size(entry.size)
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
                .add_enabled(self.edit_mode, egui::Button::new("Edit as text..."))
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
                if d.fs_type == "QNX 2" && rusty_backup::fs::qnx2::looks_like_rs_text(data) {
                    let text = rusty_backup::fs::qnx2::rs_to_lf(data);
                    file_detail::detect_content_type(&d.entry, &text)
                } else {
                    file_detail::detect_content_type(&d.entry, data)
                }
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
        self.nested_ready.take()
    }

    /// Stream `entry` into a fresh temp dir, then hand it to Inspect as a disk image.
    fn start_nested_open(&mut self, ctx: &egui::Context, entry: &FileEntry) {
        if !self.ctl.pane(Side::Left).queue.is_empty() {
            self.extraction_result = Some(
                "Apply or discard the staged edits first: opening a disk image replaces this view."
                    .to_string(),
            );
            return;
        }
        let temp = match tempfile::Builder::new().prefix("rb-nested-").tempdir() {
            Ok(t) => t,
            Err(e) => {
                self.extraction_result = Some(format!("Open as disk image failed: {e}"));
                return;
            }
        };
        let needed = entry.size.saturating_add(NESTED_FREE_MARGIN);
        if let Some(free) = rusty_backup::os::available_space(temp.path()) {
            if free < needed {
                self.extraction_result = Some(format!(
                    "Not enough free space to open {} as a disk image: it needs {} in {}, \
                     which has {} free. Point TMPDIR at a larger drive, or extract it instead.",
                    entry.name,
                    super::bytes_human(needed),
                    temp.path().parent().unwrap_or(temp.path()).display(),
                    super::bytes_human(free),
                ));
                return;
            }
        }
        let dest = temp.path().to_path_buf();
        self.nested_open = Some(temp);
        self.start_extraction(ctx, entry, Some(dest));
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

    /// Start background extraction of an archive to a temp file for editing.
    fn start_archive_extract(&mut self) {
        let Some(ctx) = self.archive_edit_ctx.clone() else {
            return;
        };

        // Create temp file next to the archive
        let parent = ctx
            .archive_path
            .parent()
            .unwrap_or(std::path::Path::new("."));
        let temp_path = parent.join(format!(
            "{}-edit.img",
            ctx.archive_path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
        ));

        self.archive_edit_progress = Some(archive_edit::start_extract(&ctx, temp_path));
    }

    /// Start background recompression of the edited temp file back to the archive.
    fn start_archive_compress(&mut self) {
        let Some(ctx) = self.archive_edit_ctx.clone() else {
            return;
        };
        let Some(temp_path) = self.archive_temp_path.clone() else {
            return;
        };

        // Disable edit mode immediately
        self.edit_mode = false;
        self.edit_free_space = None;
        self.free_space_job = None;
        self.edit_result = None;

        // Close the browser while compressing to release the temp file.
        self.ctl = new_controller();
        self.selected_entry = None;
        self.side_ed = DetailEditors::default();

        self.archive_edit_progress = Some(archive_edit::start_compress(&ctx, temp_path));
    }

    /// Point the session at the extracted temp and enter edit mode; also used
    /// after a failed recompress so the edits there are saved, not re-extracted over.
    fn adopt_archive_temp(&mut self, temp: PathBuf) {
        self.archive_temp_path = Some(temp.clone());
        self.session.source_path = Some(temp);
        self.session.zstd_cache = None;
        self.edit_mode = true;
        self.edit_free_space = None;
        self.free_space_job = None;

        // The source bytes changed: the pane reopens on the temp file.
        self.invalidate_cached_fs();
        self.selected_entry = None;
        self.side_ed = DetailEditors::default();
    }

    /// Poll archive edit background operations for completion.
    fn poll_archive_edit(&mut self, ui: &egui::Ui) {
        let progress_arc = match &self.archive_edit_progress {
            Some(p) => Arc::clone(p),
            None => return,
        };

        let Ok(p) = progress_arc.lock() else {
            return;
        };

        if !p.finished {
            ui.ctx().request_repaint();
            return;
        }

        let phase = p.phase.clone();
        let error = p.error.clone();
        let temp_path = p.temp_path.clone();
        drop(p);

        self.archive_edit_progress = None;

        if let Some(err) = error {
            if phase == "Compressing" {
                // The temp still holds the applied edits: re-adopt it so Close
                // or Apply Edits can try the save again, not re-extract over it.
                if let Some(temp) = self.archive_temp_path.clone() {
                    self.adopt_archive_temp(temp);
                }
                self.edit_result = Some(format!(
                    "Error saving the archive: {err}. Your edits are still in the temporary \
                     volume; use Close or Apply Edits to try saving again."
                ));
            } else {
                self.edit_result = Some(format!("Error: {err}"));
            }
            return;
        }

        if phase == "Extracting" {
            // Extraction done — switch source to temp file, enable editing.
            // `partition_offset` is preserved: the temp file is byte-identical
            // to the archive's decompressed content, so a partition that lived
            // at offset N inside the original (e.g. an HFS volume inside a
            // whole-disk APM CHD) is at offset N in the temp too. Resetting
            // it to 0 here used to break edits of any non-single-partition
            // container.
            if let Some(temp) = temp_path {
                self.adopt_archive_temp(temp);
            }
        } else {
            // Compression done — re-open original archive for browsing.
            // Preserve `partition_offset` for the same reason as above: the
            // recompressed archive's logical bytes match the temp file, so
            // the partition is still at the original offset.
            if let Some(ctx) = &self.archive_edit_ctx {
                self.session.source_path = Some(ctx.archive_path.clone());
                self.archive_temp_path = None;
                self.invalidate_cached_fs();
                self.selected_entry = None;
                self.side_ed = DetailEditors::default();
                self.edit_result = Some("Changes saved successfully.".to_string());
            }
        }
    }

    /// True if the current session is editing a CHD file (extension match —
    /// the chd_edit path covers both compressed and uncompressed CHDs).
    fn is_chd_source(&self) -> bool {
        self.session
            .source_path
            .as_ref()
            .and_then(|p| p.extension())
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("chd"))
            .unwrap_or(false)
    }

    /// Open a [`ChdEditSession`] for the current source CHD and install it
    /// on `self.session.chd_edit_session` so subsequent reads / writes flow
    /// through the diff (or in-place for uncompressed). Reloads the
    /// filesystem from the live session so the browser reflects the
    /// current state. On failure leaves edit_mode false and reports the
    /// error via `edit_result`.
    /// Back up the CHD and open an edit session as a job; the poll enters edit mode.
    fn enter_chd_edit_mode(&mut self) {
        if self.chd_open_job.is_some() {
            return;
        }
        let Some(parent_path) = self.session.source_path.clone() else {
            self.edit_result = Some("Error: no source path".to_string());
            return;
        };
        self.chd_open_job = Some(rusty_backup::model::job::Job::spawn(
            "Preparing CHD for editing...",
            Arc::new(super::ui_logger::EguiWake),
            move |job| {
                let opened =
                    rusty_backup::model::chd_edit_open::open_for_editing(&parent_path, job);
                Ok((parent_path, opened))
            },
        ));
    }

    fn poll_chd_open_job(&mut self) {
        use rusty_backup::model::job::Status;
        let Some(job) = &self.chd_open_job else {
            return;
        };
        let snap = job.snapshot();
        if snap.is_running() {
            return;
        }
        let out = job.take_result();
        self.chd_open_job = None;
        let opened = match (snap.status, out) {
            (Status::Done, Some((parent_path, Ok(opened)))) => (parent_path, opened),
            (Status::Done, Some((_, Err(message)))) => {
                self.edit_result = Some(message);
                return;
            }
            (Status::Failed(e), _) => {
                self.edit_result = Some(format!("Error opening CHD: {e}"));
                return;
            }
            _ => return,
        };
        let (parent_path, opened) = opened;
        let compressed = opened.compressed;
        let arc = Arc::new(Mutex::new(opened.session));
        self.session.chd_edit_session = Some(Arc::clone(&arc));
        self.chd_edit = Some(ChdEditState {
            parent_path,
            diff_path: opened.diff_path,
        });
        self.edit_mode = true;
        self.edit_free_space = None;
        self.free_space_job = None;

        // Reload the filesystem through the session so any pre-existing
        // diff content is visible.
        self.invalidate_all_caches();
        log::info!(
            "Entered CHD edit mode (compressed={compressed}, diff={})",
            self.chd_edit
                .as_ref()
                .and_then(|s| s.diff_path.as_ref())
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<none>".into())
        );
    }

    /// Discard the live CHD edit session, deleting the diff (if any). The
    /// parent CHD is left untouched. Caller is responsible for any UI state
    /// transitions (clearing `edit_mode`, etc.).
    fn discard_chd_edit_session(&mut self) {
        self.chd_diff_dirty = false;
        self.session.chd_edit_session = None;
        self.pane_stale = true;
        if let Some(state) = self.chd_edit.take() {
            if let Some(diff) = state.diff_path {
                if diff.exists() {
                    let _ = std::fs::remove_file(&diff);
                }
            }
        }
    }

    /// Apply the current CHD edit session by flattening the diff back into
    /// the parent CHD. For uncompressed CHDs there's no diff — writes were
    /// already in place — so this just drops the session and reloads.
    /// Compressed CHDs spawn a background worker that calls
    /// `flatten_to_parent`; progress is polled by `poll_chd_flatten`.
    fn start_chd_flatten(&mut self) {
        // The diff is being committed to the parent; nothing is unsaved now.
        self.chd_diff_dirty = false;
        let Some(state) = self.chd_edit.take() else {
            return;
        };

        // Drop the session so the diff handle is released before flatten
        // re-opens it via `reopen_with_diff`.
        self.session.chd_edit_session = None;

        let Some(diff_path) = state.diff_path else {
            // Uncompressed: writes already landed in the parent. Just reload.
            self.invalidate_all_caches();
            self.edit_result = Some("Changes saved successfully.".to_string());
            return;
        };

        let parent_path = state.parent_path;
        let total = std::fs::metadata(&diff_path).map(|m| m.len()).unwrap_or(0);

        let progress = Arc::new(Mutex::new(ChdFlattenProgress {
            current: 0,
            total,
            finished: false,
            error: None,
            cancel_requested: false,
        }));
        let progress_thread = Arc::clone(&progress);
        rusty_backup::model::worker::spawn_guarded(
            Arc::clone(&progress),
            "CHD flatten",
            move || {
                let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: CHD diff flatten");
                let cancel = {
                    let p = Arc::clone(&progress_thread);
                    move || p.lock().map(|g| g.cancel_requested).unwrap_or(false)
                };
                let result = chd_edit::flatten_to_parent(
                    &parent_path,
                    &diff_path,
                    None,
                    &mut |bytes| {
                        if let Ok(mut p) = progress_thread.lock() {
                            p.current = bytes;
                        }
                    },
                    &cancel,
                    &mut |msg| log::info!("{msg}"),
                );
                if let Ok(mut p) = progress_thread.lock() {
                    p.finished = true;
                    if let Err(e) = result {
                        p.error = Some(format!("{e:#}"));
                    }
                }
                Ok(())
            },
        );
        self.chd_flatten_progress = Some(progress);
    }

    /// Poll the background CHD flatten worker. On completion, reload the
    /// filesystem from the freshly-merged parent CHD.
    fn poll_chd_flatten(&mut self, ui: &egui::Ui) {
        let progress_arc = match &self.chd_flatten_progress {
            Some(p) => Arc::clone(p),
            None => return,
        };
        let Ok(p) = progress_arc.lock() else { return };
        if !p.finished {
            ui.ctx().request_repaint();
            return;
        }
        let error = p.error.clone();
        drop(p);
        self.chd_flatten_progress = None;

        if let Some(err) = error {
            self.edit_result = Some(format!("Error flattening CHD: {err}"));
            return;
        }
        // Re-open the now-replaced parent CHD for browsing.
        self.invalidate_all_caches();

        // If the CHD is the body of a single-file-chd backup, refresh
        // metadata.json so per-partition checksums + container SHA-1 stay
        // in sync with the new container contents. Best-effort: failure
        // surfaces in the edit result but doesn't roll back the save —
        // the bytes on disk are already correct.
        let mut result_msg = String::from("Changes saved successfully.");
        if let Some(folder) = self.single_file_chd_backup_folder.clone() {
            let mut log_lines: Vec<String> = Vec::new();
            let mut log_cb = |s: &str| log_lines.push(s.to_string());
            match rusty_backup::backup::single_file_chd::refresh_metadata_after_edit(
                &folder,
                &mut log_cb,
            ) {
                Ok(()) => {
                    result_msg.push_str(" Backup metadata.json refreshed.");
                }
                Err(e) => {
                    result_msg.push_str(&format!(
                        " (Warning: failed to refresh backup metadata.json: {e:#})"
                    ));
                }
            }
        }
        self.edit_result = Some(result_msg);
    }

    /// Spawn a background job that archives the whole volume to a
    /// `.tar.gz` / `.tar.zst` / `.tar` chosen via a save dialog. Compression
    /// is inferred from the chosen extension. Real symlinks and
    /// case-sensitive names are preserved (data fork only). Progress is
    /// polled by [`Self::poll_tar_export`].
    /// Archive `target` (the whole volume when its path is `/`, otherwise the
    /// selected folder or file) to a `.tar.gz` / `.tar.zst` / `.tar` chosen
    /// via a save dialog. A subtree is rooted under its own basename in the
    /// archive (like `tar`); a single file becomes a one-entry archive.
    fn start_tar_export(&mut self, target: &FileEntry) {
        let whole_volume = target.path == "/";
        let default_name = if whole_volume {
            let base = self
                .session
                .source_path
                .as_ref()
                .and_then(|p| p.file_stem())
                .and_then(|s| s.to_str())
                .unwrap_or("volume");
            format!("{base}.tar.gz")
        } else {
            format!("{}.tar.gz", target.name)
        };
        let title = if target.is_directory() {
            "Export folder to tar archive"
        } else {
            "Export file to tar archive"
        };
        let path = match super::file_dialog()
            .set_title(title)
            .set_file_name(&default_name)
            .save_file()
        {
            Some(p) => p,
            None => return,
        };
        // A subtree roots its entries under its own name; the whole volume
        // and single files sit at the archive top level.
        let archive_prefix = if target.is_directory() && !whole_volume {
            target.name.clone()
        } else {
            String::new()
        };
        let target = target.clone();
        let compression = fs_tar::tar_export::TarCompression::infer_from_path(&path);
        let session = self.session.clone();
        let progress = Arc::new(Mutex::new(TarExportProgress {
            files: 0,
            dirs: 0,
            symlinks: 0,
            bytes: 0,
            finished: false,
            error: None,
            out_path: path.clone(),
        }));
        let progress_thread = Arc::clone(&progress);
        rusty_backup::model::worker::spawn_guarded(
            Arc::clone(&progress),
            "tar export",
            move || {
                let _wake = rusty_backup::os::wakelock::acquire("Rusty Backup: tar export");
                let result = (|| -> anyhow::Result<()> {
                    let mut fs = session
                        .open()
                        .map_err(|e| anyhow::anyhow!("opening filesystem: {e}"))?;
                    // Re-fetch the root fresh on the worker's own fs instance; a
                    // subtree/file uses the selected entry directly (its on-disk
                    // identity is stable across fs instances of the same image).
                    let root = if whole_volume {
                        fs.root()
                            .map_err(|e| anyhow::anyhow!("reading root: {e}"))?
                    } else {
                        target
                    };
                    let file = std::fs::File::create(&path)
                        .map_err(|e| anyhow::anyhow!("creating {}: {e}", path.display()))?;
                    let out = std::io::BufWriter::new(file);
                    let opts = fs_tar::tar_export::TarExportOptions::default();
                    let cb = |s: &fs_tar::tar_export::TarExportStats| {
                        if let Ok(mut p) = progress_thread.lock() {
                            p.files = s.files;
                            p.dirs = s.dirs;
                            p.symlinks = s.symlinks;
                            p.bytes = s.total_bytes;
                        }
                    };
                    fs_tar::tar_export::export_tar(
                        &mut *fs,
                        &root,
                        &archive_prefix,
                        out,
                        compression,
                        &opts,
                        &cb,
                    )?;
                    Ok(())
                })();
                if let Ok(mut p) = progress_thread.lock() {
                    p.finished = true;
                    if let Err(e) = result {
                        p.error = Some(format!("{e:#}"));
                    }
                }
                Ok(())
            },
        );
        self.tar_export_progress = Some(progress);
    }

    /// Poll the background tar-export worker: show a spinner while running,
    /// and surface the result when it finishes.
    /// Enter Edit Mode on a raw image / device, probing the editable open
    /// up-front so guards like the journaled-HFS+ refusal surface here rather
    /// than being deferred to the first edit.
    fn enter_direct_edit_mode(&mut self) {
        match self.session.open_editable() {
            // Probe only: discard the fs + commit guard (no mutation, so a
            // container is left intact).
            Ok((_efs, _commit)) => {
                self.edit_mode = true;
                self.edit_free_space = None;
                self.free_space_job = None;
            }
            Err(e) => {
                self.error = Some(format!("Cannot enter edit mode: {e}"));
            }
        }
    }

    /// Open the size-budget dialog when this volume is a SquashFS that hasn't
    /// been given a budget yet. Returns true when the dialog took over, meaning
    /// the caller must not enter Edit Mode — [`Self::poll_squashfs_budget_dialog`]
    /// does that once the user has chosen.
    ///
    /// Measuring walks the directory tree for file sizes only (no content is
    /// decompressed), so this stays cheap even on a distro rootfs.
    fn open_squashfs_budget_dialog(&mut self) -> bool {
        if self.fs_type != "SquashFS" || self.session.rebuild_budget.is_some() {
            return false;
        }
        if self.squashfs_measure_job.is_some() {
            return true;
        }
        let session = self.session.clone();
        // A partition bounds the image; a bare file at byte 0 is the image.
        let capacity = (self.session.partition_offset != 0)
            .then_some(self.session.partition_size)
            .flatten();
        self.squashfs_measure_job = Some(rusty_backup::model::job::Job::spawn(
            "Measuring the image...",
            Arc::new(super::ui_logger::EguiWake),
            move |job| {
                job.set_can_cancel(false);
                let mut fs = match session.open() {
                    Ok(fs) => fs,
                    Err(e) => return Ok(Err(format!("Cannot measure this image: {e}"))),
                };
                let image_len = fs_squashfs::squashfs_write::image_footprint(fs.total_size());
                Ok(
                    fs_squashfs::squashfs_edit::plan_size(fs.as_mut(), image_len, capacity)
                        .map_err(|e| format!("Cannot measure this image: {e}")),
                )
            },
        ));
        true
    }

    /// Open the budget dialog once the measurement lands.
    fn poll_squashfs_measure(&mut self, ui: &mut egui::Ui) {
        let Some(job) = &self.squashfs_measure_job else {
            return;
        };
        let snap = job.snapshot();
        if snap.is_running() {
            egui::Window::new(snap.label.clone())
                .collapsible(false)
                .resizable(false)
                .show(ui.ctx(), |ui| {
                    super::progress::show_job(ui, &snap);
                });
            return;
        }
        let out = job.take_result();
        self.squashfs_measure_job = None;
        match out {
            Some(Ok(plan)) => {
                self.squashfs_budget_dialog = Some(Box::new(
                    super::squashfs_budget_dialog::SquashfsBudgetDialog::new(plan),
                ));
            }
            Some(Err(e)) => self.error = Some(e),
            None => {
                if let rusty_backup::model::job::Status::Failed(e) = snap.status {
                    self.error = Some(format!("Cannot measure this image: {e}"));
                }
            }
        }
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
            self.session.rebuild_budget = budget;
            self.squashfs_budget_dialog = None;
            self.enter_direct_edit_mode();
        } else if closed {
            self.squashfs_budget_dialog = None;
        }
    }

    fn poll_tar_export(&mut self, ui: &mut egui::Ui) {
        let arc = match &self.tar_export_progress {
            Some(p) => Arc::clone(p),
            None => return,
        };
        let Ok(p) = arc.lock() else { return };
        if !p.finished {
            ui.ctx().request_repaint();
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label(format!(
                    "Exporting archive... {} files, {} dirs, {} symlinks",
                    p.files, p.dirs, p.symlinks
                ));
            });
            return;
        }
        let error = p.error.clone();
        let (files, dirs, symlinks, bytes) = (p.files, p.dirs, p.symlinks, p.bytes);
        let out = p.out_path.clone();
        drop(p);
        self.tar_export_progress = None;
        match error {
            Some(e) => self.error = Some(format!("Tar export failed: {e}")),
            None => {
                self.edit_result = Some(format!(
                    "Wrote {} ({} files, {} dirs, {} symlinks, {})",
                    out.display(),
                    files,
                    dirs,
                    symlinks,
                    disk_core::display::format_size(bytes),
                ));
            }
        }
    }

    /// Pick a host `.tar.gz` / `.tar.zst` / `.tar` and preflight it against
    /// the current volume. If the import would lose content (dropped
    /// symlinks, names this FS can't store), stash it for a confirmation
    /// modal; otherwise import straight away. Imports into the directory the
    /// user is currently editing.
    fn start_tar_import_dialog(&mut self) {
        let path = match rfd::FileDialog::new()
            .set_title("Import a tar archive into this volume")
            .add_filter("tar archive", &["gz", "tgz", "tar", "zst"])
            .pick_file()
        {
            Some(p) => p,
            None => return,
        };
        self.begin_tar_import_from_path(&path);
    }

    /// Preflight `path` against the current volume and either import it
    /// straight away (lossless) or stash it for the confirmation modal
    /// (`render_tar_import_dialog`) when content would be skipped/dropped.
    /// Shared by the "Import Archive..." button and the add/drop auto-route.
    fn begin_tar_import_from_path(&mut self, path: &std::path::Path) {
        self.spawn_tar_job(path.to_path_buf(), true);
    }

    /// Import `archive` into the current directory after its preflight was confirmed.
    fn run_tar_import(&mut self, archive: &std::path::Path) {
        self.spawn_tar_job(archive.to_path_buf(), false);
    }

    /// Preflight (when `check`) and import a tar as a job; a preflight with warnings stops for the dialog.
    /// Existing names are skipped, macOS `._*` sidecars dropped; it writes at once, not staged.
    fn spawn_tar_job(&mut self, archive: PathBuf, check: bool) {
        if self.tar_job.is_some() {
            return;
        }
        let session = self.session.clone();
        let dest = self.current_parent_entry();
        self.tar_job = Some(rusty_backup::model::job::Job::spawn(
            "Importing the archive...",
            Arc::new(super::ui_logger::EguiWake),
            move |job| {
                job.set_can_cancel(false);
                Ok(tar_import_step(&session, &dest, archive, check, job))
            },
        ));
    }

    fn poll_tar_job(&mut self) {
        use rusty_backup::model::job::Status;
        let Some(job) = &self.tar_job else {
            return;
        };
        let snap = job.snapshot();
        if snap.is_running() {
            return;
        }
        let out = job.take_result();
        self.tar_job = None;
        match (snap.status, out) {
            (Status::Done, Some(TarStep::Confirm(archive_path, preflight))) => {
                self.pending_tar_import = Some(PendingTarImport {
                    archive_path,
                    preflight,
                });
            }
            (Status::Done, Some(TarStep::Finished(msg, wrote))) => {
                self.edit_result = Some(msg);
                if wrote {
                    self.invalidate_all_caches();
                    self.invalidate_cached_fs();
                }
            }
            (Status::Failed(e), _) => self.edit_result = Some(format!("Import failed: {e}")),
            _ => {}
        }
    }

    /// Modal shown when a tar archive is added/dropped as a host file: import
    /// its contents into the current folder, or add the archive as a plain
    /// file. Mirrors the Mac-archive add flow.
    fn render_tar_add_dialog(&mut self, ui: &mut egui::Ui) {
        let Some(path) = self.pending_tar_add.clone() else {
            return;
        };
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("archive")
            .to_string();
        let mut open = true;
        let mut decision: Option<u8> = None; // 0=import, 1=add-as-file, 2=cancel
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
                        decision = Some(0);
                    }
                    if ui.button("Add as file").clicked() {
                        decision = Some(1);
                    }
                    if ui.button("Cancel").clicked() {
                        decision = Some(2);
                    }
                });
            });
        if !open {
            decision = Some(2);
        }
        match decision {
            Some(0) => {
                self.pending_tar_add = None;
                self.begin_tar_import_from_path(&path);
            }
            Some(1) => {
                self.pending_tar_add = None;
                let folder = Some(self.current_parent_entry().path);
                self.stage_paths(vec![path.clone()], folder);
            }
            Some(_) => self.pending_tar_add = None, // cancel / window closed
            None => {}
        }
    }

    /// Modal shown when a tar import's preflight found content the target
    /// filesystem can't represent. Continue runs the (lossy) import; Cancel
    /// abandons it.
    fn render_tar_import_dialog(&mut self, ui: &mut egui::Ui) {
        let Some(pending) = self.pending_tar_import.clone() else {
            return;
        };
        let mut open = true;
        let mut decision: Option<bool> = None; // Some(true)=continue, Some(false)=cancel
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
                for w in pending.preflight.warnings() {
                    ui.label(format!("- {w}"));
                }
                if pending.preflight.appledouble > 0 {
                    ui.label(format!(
                        "- {} macOS ._ sidecar(s) will be skipped.",
                        pending.preflight.appledouble
                    ));
                }
                ui.add_space(6.0);
                ui.label(format!(
                    "{} file(s) and {} dir(s) will be imported. Existing names are skipped.",
                    pending.preflight.files, pending.preflight.dirs
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
        match decision {
            Some(true) => {
                self.pending_tar_import = None;
                self.run_tar_import(&pending.archive_path);
            }
            Some(false) => {
                self.pending_tar_import = None;
                self.edit_result = Some("Archive import cancelled.".to_string());
            }
            None => {}
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
                self.add_file_dialog();
            }

            // Import a host .tar.gz / .tar.zst / .tar into the current
            // directory. Runs a preflight first and prompts if the target
            // filesystem can't represent some of the archive's content.
            if ui.button("Import Archive...").clicked() {
                self.start_tar_import_dialog();
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
                    if let Some(ref sel) = self.selected_entry.clone() {
                        self.ctl
                            .host_pane()
                            .queue
                            .push(StagedEdit::BlessFolder { entry: sel.clone() });
                        self.edit_result = Some(format!("Staged bless folder '{}'", sel.name));
                    }
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
                    if let Some(ref sel) = self.selected_entry.clone() {
                        self.ctl
                            .host_pane()
                            .queue
                            .replace_set_prodos_access(sel, 0x21);
                        self.edit_result = Some(format!("Staged lock '{}'", sel.name));
                    }
                }
                if ui
                    .add_enabled(can_lock, egui::Button::new("Unlock"))
                    .on_hover_text("Set the ProDOS access byte to $C3 (read/write/destroy/rename).")
                    .clicked()
                {
                    if let Some(ref sel) = self.selected_entry.clone() {
                        self.ctl
                            .host_pane()
                            .queue
                            .replace_set_prodos_access(sel, 0xC3);
                        self.edit_result = Some(format!("Staged unlock '{}'", sel.name));
                    }
                }
            }

            ui.add_space(8.0);
            ui.separator();
            ui.add_space(8.0);

            // Apply button: only with staged edits, and not while an extraction
            // or tar export is still reading the volume the apply would rewrite.
            if !self.ctl.pane(Side::Left).queue.is_empty() {
                let label = format!("Apply Edits ({})", self.ctl.pane(Side::Left).queue.len());
                let reading = self.extracting || self.tar_export_progress.is_some();
                if ui
                    .add_enabled(!reading, egui::Button::new(label))
                    .on_disabled_hover_text("Wait for the running extraction or export to finish")
                    .clicked()
                {
                    self.start_apply(ApplyFollowUp::RecompressArchive);
                }
            }

            ui.add_space(8.0);

            // Free space, probed once per edit session and again after each write:
            // opening the editable volume decodes a whole container.
            if self.edit_free_space.is_none() {
                match &self.free_space_job {
                    Some(job) if !job.is_running() => {
                        self.edit_free_space = Some(job.take_result().flatten());
                        self.free_space_job = None;
                    }
                    Some(_) => {}
                    // Never alongside one of this view's writes.
                    None if self.running_job().is_none() => {
                        let session = self.session.clone();
                        self.free_space_job = Some(rusty_backup::model::job::Job::spawn(
                            "Free space",
                            Arc::new(super::ui_logger::EguiWake),
                            move |_| {
                                Ok(session
                                    .open_editable()
                                    .ok()
                                    .and_then(|(mut efs, _commit)| efs.free_space().ok()))
                            },
                        ));
                    }
                    None => {}
                }
            }
            if let Some(Some(free)) = self.edit_free_space {
                ui.label(format!("Free: {}", disk_core::display::format_size(free)));

                if !self.ctl.pane(Side::Left).queue.is_empty() {
                    let delta = self.ctl.pane(Side::Left).queue.space_delta();
                    let projected = free.saturating_add(delta.freed).saturating_sub(delta.added);
                    let color = if delta.added > free + delta.freed {
                        super::theme::danger(ui.visuals()) // red — won't fit
                    } else if projected < free / 10 {
                        super::theme::warning(ui.visuals()) // yellow — tight
                    } else {
                        super::theme::success(ui.visuals()) // green — ok
                    };
                    ui.colored_label(
                        color,
                        format!("After: {}", disk_core::display::format_size(projected)),
                    );
                }
            }
        });
    }

    /// Show the "Add File" file picker dialog and add files.
    fn add_file_dialog(&mut self) {
        let files = rfd::FileDialog::new()
            .set_title("Select files to add")
            .pick_files();

        if let Some(paths) = files {
            self.add_host_paths(&paths);
        }
    }

    /// Stage host paths through the pane; a tar or (on HFS) a Mac archive asks how to import it first.
    fn add_host_paths(&mut self, paths: &[PathBuf]) {
        let parent = self.current_parent_entry();
        let mut plain = Vec::new();
        for path in paths {
            if path.is_file() && fs_tar::tar_import::looks_like_tar_archive(path) {
                self.pending_tar_add = Some(path.clone());
            } else if !(path.is_file()
                && self.is_hfs_type()
                && self.queue_archive_import_if_applicable(path, &parent))
            {
                plain.push(path.clone());
            }
        }
        self.stage_paths(plain, None);
    }

    /// Stage host files and folders into `folder` (the pane's target folder when `None`).
    fn stage_paths(&mut self, paths: Vec<PathBuf>, folder: Option<String>) {
        if paths.is_empty() {
            return;
        }
        let act = BrowserPaneAction::AddHostPaths { paths, folder };
        // A staging pane answers with status lines only; nothing here asks the user.
        for fx in self.ctl.apply(CommanderAction::Pane(Side::Left, act)) {
            log::debug!("staging: {fx:?}");
        }
        self.edit_result = Some(self.ctl.status().to_string());
    }

    /// Workflow A: sniff the picked host file for a Mac archive kind.
    /// On a hit, push a [`PendingArchiveImport`] so
    /// [`render_archive_import_dialog`] asks the user how to handle it
    /// and return `true`. Returning `true` short-circuits the normal
    /// staging path. Returning `false` means "not a Mac archive — keep
    /// going with the as-binary staging path."
    ///
    /// Sniff failures (unreadable file, opaque bytes) return `false`
    /// silently — the regular staging path handles its own errors.
    fn queue_archive_import_if_applicable(
        &mut self,
        host_path: &std::path::Path,
        parent: &FileEntry,
    ) -> bool {
        // Detection needs the whole file; past 64 MiB only a Mac-archive extension earns that read.
        let size = std::fs::metadata(host_path).map(|m| m.len()).unwrap_or(0);
        let named_archive = host_path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| {
                rusty_backup::model::file_types::MAC_ARCHIVE_EXTS
                    .iter()
                    .any(|a| a.eq_ignore_ascii_case(e))
            });
        if size > fs_tar::mem_archive::INLINE_MAX && !named_archive {
            return false;
        }
        let raw = match std::fs::read(host_path) {
            Ok(bytes) => bytes,
            Err(_) => return false,
        };
        let kind = match detect_mac_archive(&raw) {
            Some(k) => k,
            None => return false,
        };
        self.pending_archive_imports.push(PendingArchiveImport {
            host_path: host_path.to_path_buf(),
            parent: parent.clone(),
            kind,
            raw,
        });
        true
    }

    /// Get-or-create the BrowseView's archive-import tempdir. Held until
    /// Discard/Apply/close so the staged AddFile host_path references
    /// stay valid.
    fn ensure_archive_import_tempdir(&mut self) -> Result<&Path, String> {
        if self.archive_import_tempdir.is_none() {
            let dir = tempfile::Builder::new()
                .prefix("rusty-archive-import-")
                .tempdir()
                .map_err(|e| format!("create tempdir: {e}"))?;
            self.archive_import_tempdir = Some(dir);
        }
        Ok(self.archive_import_tempdir.as_ref().unwrap().path())
    }

    /// Workflow A "Convert" / "Expand" / "Convert and expand": decode the
    /// pending archive's contents into an AppleDouble dump inside the
    /// import tempdir, then re-stage its children as if the user had
    /// picked them via the regular Add File flow (so resource forks are
    /// detected via the `._<name>` sidecars).
    fn apply_archive_import_extract(
        &mut self,
        pending: &PendingArchiveImport,
    ) -> Result<(), String> {
        // Decode the picked file. For HQX wrappers, peel them; for SIT/SEA
        // standalone the bytes go straight to the parser.
        let (bytes, archive) = mac_extract::open_bytes(pending.raw.clone())
            .map_err(|e| format!("decode {}: {e}", pending.host_path.display()))?;

        // A unique subdir per import so multiple Convert clicks don't
        // collide on filename.
        let root = self.ensure_archive_import_tempdir()?.to_path_buf();
        let subdir = root.join(format!("import-{}", self.pending_archive_imports.len()));
        std::fs::create_dir_all(&subdir).map_err(|e| format!("create subdir: {e}"))?;

        // AppleDouble preserves data fork + resource fork + type/creator
        // via a `._<name>` sidecar that the existing
        // `resource_fork::detect_resource_fork` picks up at staging time.
        mac_extract::extract_all(
            &bytes,
            &archive,
            &subdir,
            mac_extract::ForkFormat::AppleDouble,
            |_, _, _| {},
            |_| {},
        )
        .map_err(|e| format!("extract: {e}"))?;

        // Now stage every direct child of the subdir under `parent`.
        // (stage_host_file silently skips `._<name>` sidecars and the
        // primary file's resource fork is detected via the sidecar.)
        let children: Vec<PathBuf> = std::fs::read_dir(&subdir)
            .map_err(|e| format!("read subdir: {e}"))?
            .filter_map(|r| r.ok())
            .map(|e| e.path())
            .collect();
        self.stage_paths(children, Some(pending.parent.path.clone()));
        Ok(())
    }

    /// Workflow A "Convert only" (HQX-over-SIT / HQX-over-SEA): strip
    /// the BinHex wrapper and stage the resulting .sit / .sea as a
    /// single AddFile under the import tempdir.
    fn apply_archive_import_convert_only(
        &mut self,
        pending: &PendingArchiveImport,
    ) -> Result<(), String> {
        let bh = fs_mac::binhex::parse_binhex(&pending.raw)
            .map_err(|e| format!("decode BinHex: {e}"))?;
        let root = self.ensure_archive_import_tempdir()?.to_path_buf();
        let subdir = root.join(format!("import-{}", self.pending_archive_imports.len()));
        std::fs::create_dir_all(&subdir).map_err(|e| format!("create subdir: {e}"))?;
        let inner_name = if bh.name.is_empty() {
            "archive.sit".to_string()
        } else {
            bh.name.replace(['/', '\\'], "_")
        };
        let inner_path = subdir.join(&inner_name);
        std::fs::write(&inner_path, &bh.data_fork)
            .map_err(|e| format!("write inner archive: {e}"))?;
        self.stage_paths(vec![inner_path], Some(pending.parent.path.clone()));
        Ok(())
    }

    /// Workflow A "Add as-is": skip the archive intercept and stage the
    /// picked file unchanged through the regular add-host-file path.
    /// Direct call (bypasses queue_archive_import_if_applicable).
    fn apply_archive_import_as_is(&mut self, pending: &PendingArchiveImport) -> Result<(), String> {
        let folder = Some(pending.parent.path.clone());
        self.stage_paths(vec![pending.host_path.clone()], folder);
        Ok(())
    }

    /// Workflow E trigger: read the selected file's bytes off the disk
    /// image, sniff + decode, and open a floating viewer window with
    /// the archive's entries. Read-only — the user can extract any
    /// subset but can't modify the archive itself.
    fn open_archive_browse_window(&mut self, entry: &FileEntry) {
        self.read_queue
            .push_back(FsRead::Archive(entry.clone(), ArchiveUse::Browse));
        self.pump_reads();
    }

    /// Workflow C trigger: read the selected file's bytes off the disk
    /// image, sniff them via [`detect_mac_archive`], and on a hit open
    /// the Save-as-is / Decode-and-save modal. On a non-archive (or a
    /// read failure), surface a one-line error and don't open the modal.
    fn open_archive_extract_modal(&mut self, entry: &FileEntry) {
        self.read_queue
            .push_back(FsRead::Archive(entry.clone(), ArchiveUse::Extract));
        self.pump_reads();
    }

    /// Open the archive window or the extract modal for a decoded archive.
    fn apply_archive_read(
        &mut self,
        entry: FileEntry,
        purpose: ArchiveUse,
        result: Result<ArchiveRead, String>,
    ) {
        let read = match result {
            Ok(read) => read,
            Err(msg) => {
                self.edit_result = Some(msg);
                return;
            }
        };
        match purpose {
            ArchiveUse::Browse => {
                let selected = vec![true; read.archive.entries.len()];
                self.mac_archive_window = Some(MacArchiveWindow {
                    entry_name: entry.name.clone(),
                    kind: read.kind,
                    bytes: read.bytes,
                    archive: read.archive,
                    selected,
                    fork_format: mac_extract::ForkFormat::BinHex,
                });
            }
            ArchiveUse::Extract => {
                self.pending_archive_extract = Some(PendingArchiveExtract {
                    entry_name: entry.name.clone(),
                    kind: read.kind,
                    raw_bytes: read.raw,
                    extract_bytes: read.bytes,
                    archive: read.archive,
                    fork_format: mac_extract::ForkFormat::BinHex,
                });
            }
        }
    }

    /// Walk `entry` (and its children if a directory) on the open
    /// filesystem, collecting every reachable file into a [`StuffItInputNode`]
    /// tree. Symlinks and special files are skipped. Used by
    /// [`export_selection_as_archive`] for Workflow B. Holds the
    /// filesystem only for the duration of the walk.
    fn collect_archive_inputs(
        &mut self,
        entry: &FileEntry,
    ) -> Result<Vec<StuffItInputNode>, String> {
        let mut fs = self
            .take_or_open_fs()
            .ok_or_else(|| "filesystem not open".to_string())?;
        let result = walk_fs_to_input_nodes(&mut *fs, entry);
        self.return_fs(fs);
        result
    }

    /// Workflow B: bundle the selected entry as a Mac archive on the
    /// host. Single-file selections can be wrapped as BinHex (which is
    /// always single-file); file-or-folder selections can be SIT or
    /// SIT-over-BinHex (a `.sit.hqx`). Foreground execution — typical
    /// classic-Mac payloads are small (Hypercard stacks, dev tools).
    fn export_selection_as_archive(&mut self, entry: &FileEntry, format: ArchiveExportFormat) {
        let default_name = format!("{}{}", entry.name, format.extension());
        let path = match super::file_dialog()
            .set_title("Save Mac archive")
            .set_file_name(&default_name)
            .save_file()
        {
            Some(p) => p,
            None => return,
        };

        let nodes = match self.collect_archive_inputs(entry) {
            Ok(n) => n,
            Err(e) => {
                self.edit_result = Some(format!("Archive collect failed: {e}"));
                return;
            }
        };
        if nodes.is_empty() {
            self.edit_result = Some("Nothing to archive (empty selection).".into());
            return;
        }

        let bytes = match format {
            ArchiveExportFormat::Sit => match build_archive_tree(&nodes, WriteMethod::Store) {
                Ok(b) => b,
                Err(e) => {
                    self.edit_result = Some(format!("SIT build failed: {e}"));
                    return;
                }
            },
            ArchiveExportFormat::Mar => match arc_mac::mar::build_archive(&entry.name, &nodes) {
                Ok(b) => b,
                Err(e) => {
                    self.edit_result = Some(format!("MAR build failed: {e}"));
                    return;
                }
            },
            ArchiveExportFormat::SitHqx => {
                let sit = match build_archive_tree(&nodes, WriteMethod::Store) {
                    Ok(b) => b,
                    Err(e) => {
                        self.edit_result = Some(format!("SIT build failed: {e}"));
                        return;
                    }
                };
                let bh = fs_mac::binhex::BinHexFile {
                    name: format!("{}.sit", entry.name),
                    type_code: *b"SITD", // StuffIt Deluxe archive type
                    creator_code: *b"SIT!",
                    flags: 0,
                    data_fork: sit,
                    resource_fork: Vec::new(),
                };
                fs_mac::binhex::build_binhex(&bh).into_bytes()
            }
            ArchiveExportFormat::Hqx => {
                // BinHex is fundamentally single-file. The button is gated
                // on a single-file selection, so the input tree should be
                // a one-element Vec<File>; defend against the alternative.
                let file = match nodes.first() {
                    Some(StuffItInputNode::File(f)) if nodes.len() == 1 => f.clone(),
                    _ => {
                        self.edit_result =
                            Some("BinHex export requires a single file selection.".into());
                        return;
                    }
                };
                let bh = fs_mac::binhex::BinHexFile {
                    name: file.name,
                    type_code: file.type_code,
                    creator_code: file.creator_code,
                    flags: file.finder_flags,
                    data_fork: file.data_fork,
                    resource_fork: file.resource_fork,
                };
                fs_mac::binhex::build_binhex(&bh).into_bytes()
            }
        };

        match std::fs::write(&path, &bytes) {
            Ok(()) => {
                self.edit_result = Some(format!(
                    "Wrote {} ({}, {})",
                    path.display(),
                    format.label(),
                    disk_core::display::format_size(bytes.len() as u64),
                ));
            }
            Err(e) => {
                self.edit_result = Some(format!("Write failed: {e}"));
            }
        }
    }

    /// Apply all staged edits to the filesystem in a single batch.
    /// Returns false when the batch was deferred to the conflict review; the
    /// callers used to read that as success and dropped the queue.
    /// Apply the staged edits as a job; `follow_up` runs once they land.
    /// Apply through the pane (preflight, review, write, reopen); `follow_up` runs once it landed.
    fn start_apply(&mut self, follow_up: ApplyFollowUp) {
        if self.apply_follow.is_some() {
            return;
        }
        self.edit_free_space = None;
        self.free_space_job = None;
        let total = self.ctl.pane(Side::Left).queue.len();
        log::info!("Applying {total} staged edit(s)");
        self.apply_follow = Some((follow_up, total, false));
        self.ctl
            .apply(CommanderAction::Pane(Side::Left, BrowserPaneAction::Apply));
    }

    /// Watch the pane's Apply; once it wrote and reopened, report it and run the follow-up.
    fn poll_apply(&mut self) {
        let Some((follow_up, total, wrote)) = self.apply_follow else {
            return;
        };
        let pane = self.ctl.pane(Side::Left);
        let wrote = wrote || pane.applying().is_some();
        if pane.is_busy() || pane.apply_review().is_some() {
            self.apply_follow = Some((follow_up, total, wrote));
            return;
        }
        self.apply_follow = None;
        if !wrote {
            // The review was dismissed, or the preflight failed and left a report.
            self.pending_close = false;
            return;
        }
        // The pane reopened the volume itself; only this view's reader is stale.
        self.forget_cached_fs();
        self.selected_entry = None;
        self.cursor_armed = false;
        let left = self.ctl.pane(Side::Left).queue.len();
        let applied = total.saturating_sub(left);
        if applied > 0 && self.chd_edit.is_some() {
            self.chd_diff_dirty = true;
        }
        match self.ctl.pane(Side::Left).apply_report() {
            Some(r) => {
                let why = r
                    .fatal
                    .clone()
                    .or_else(|| r.failed.first().map(|f| f.error.clone()))
                    .unwrap_or_default();
                self.edit_result = Some(format!(
                    "Error: {why}. Applied {} of {}; {left} edit(s) remain staged",
                    r.applied, r.total
                ));
            }
            None => {
                self.edit_result = Some(format!("Applied {total} edit(s) successfully"));
                // The archive-import tempdir backed the AddFile host paths; the bytes are on the image now.
                self.archive_import_tempdir = None;
            }
        }
        self.run_apply_follow_up(follow_up);
    }

    fn run_apply_follow_up(&mut self, follow_up: ApplyFollowUp) {
        match follow_up {
            ApplyFollowUp::RecompressArchive => {
                if self.archive_edit_ctx.is_some() && !self.has_edit_error() {
                    self.start_archive_compress();
                }
            }
            ApplyFollowUp::LeaveEditing => {
                if !self.has_edit_error() {
                    if self.pending_close {
                        self.close();
                    } else {
                        self.exit_edit_mode();
                    }
                }
            }
        }
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

    fn has_edit_error(&self) -> bool {
        self.edit_result
            .as_ref()
            .map(|r| r.starts_with("Error"))
            .unwrap_or(false)
    }

    /// Invalidate all cached directory listings and reload root.
    ///
    /// Called after edit-apply / sync_metadata writes. The cached read-only
    /// filesystem has stale in-memory tables (catalog, bitmap), so drop it
    /// and re-open against the freshly written disk bytes.
    fn invalidate_all_caches(&mut self) {
        self.selected_entry = None;
        self.side_ed = DetailEditors::default();
        // The pane's reopen drops marks too: after a write they may name renamed or deleted files.
        self.invalidate_cached_fs();
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
        use rusty_backup::model::text_edit::LineEnding;
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
        if !self.show_unsaved_dialog {
            return;
        }

        let count = self.ctl.pane(Side::Left).queue.len();

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
                        // The dialog is the confirmation, so answer the pane's own discard question.
                        let yes = BrowserPaneAction::Confirmed {
                            purpose: rusty_backup::controller::commander::purpose::DISCARD_STAGED,
                            yes: true,
                        };
                        self.ctl.apply(CommanderAction::Pane(Side::Left, yes));
                        self.pending_archive_imports.clear();
                        self.archive_import_tempdir = None;
                        self.show_unsaved_dialog = false;
                        // For CHD edit sessions, "Discard" must also drop
                        // the diff so prior Apply Edits aren't flattened
                        // into the parent. Without this, a discard followed
                        // by exit_edit_mode would still flatten any work
                        // that was previously written to the diff.
                        if self.chd_edit.is_some() {
                            self.discard_chd_edit_session();
                        }
                        if self.pending_close {
                            self.close();
                        } else {
                            self.exit_edit_mode();
                        }
                    }
                    if ui.button("Apply Edits").clicked() {
                        self.show_unsaved_dialog = false;
                        self.start_apply(ApplyFollowUp::LeaveEditing);
                    }
                    if ui.button("Cancel").clicked() {
                        self.show_unsaved_dialog = false;
                        self.pending_close = false;
                    }
                });
            });
    }

    /// Render the Workflow A "Mac archive picked" modal. Shows one
    /// modal per pending import in queue order; each user click pops
    /// the front and applies the chosen action. The modal copy varies
    /// by sniffed archive kind:
    ///   * BinHexSingleFile -> Convert / Add as-is / Cancel
    ///   * Sit / Sit5 / Sea -> Expand / Add as-is / Cancel
    ///   * BinHexOverSit / BinHexOverSea -> Convert and expand / Convert only / Add as-is / Cancel
    fn render_archive_import_dialog(&mut self, ui: &mut egui::Ui) {
        if self.pending_archive_imports.is_empty() {
            return;
        }
        // Snapshot the front item to render the modal body. The action
        // handlers each remove the front entry; this clone keeps the
        // borrow short and lets the click handlers mutate self.
        let pending = self.pending_archive_imports[0].clone();
        let filename = pending
            .host_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("(no name)")
            .to_string();
        let kind_label = pending.kind.label();

        #[derive(Clone, Copy)]
        enum ImportAction {
            Extract,     // Convert (HQX) / Expand (SIT/SEA) / Convert+Expand (HQX-over)
            ConvertOnly, // Only valid for BinHexOverSit / BinHexOverSea
            AsIs,
            Cancel,
        }
        let mut action: Option<ImportAction> = None;

        let title = match pending.kind {
            MacArchiveKind::BinHexSingleFile => "Convert HQX file?",
            MacArchiveKind::Sit
            | MacArchiveKind::Sit5
            | MacArchiveKind::Sea
            | MacArchiveKind::CompactPro
            | MacArchiveKind::Mar
            | MacArchiveKind::MacBinary
            | MacArchiveKind::MacZip => "Expand archive?",
            MacArchiveKind::BinHexOverSit
            | MacArchiveKind::BinHexOverSea
            | MacArchiveKind::BinHexOverCompactPro => "Convert and/or expand?",
        };

        egui::Window::new(title)
            .id(egui::Id::unique("archive_import_dialog"))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.label(format!("{filename} is a {kind_label} archive."));
                ui.add_space(4.0);
                let body = match pending.kind {
                    MacArchiveKind::BinHexSingleFile => {
                        format!("Would you like to convert {filename} to binary?")
                    }
                    MacArchiveKind::Sit
                    | MacArchiveKind::Sit5
                    | MacArchiveKind::Sea
                    | MacArchiveKind::CompactPro
                    | MacArchiveKind::Mar
                    | MacArchiveKind::MacBinary
                    | MacArchiveKind::MacZip => {
                        format!("Would you like to expand the contents of {filename}?")
                    }
                    MacArchiveKind::BinHexOverSit
                    | MacArchiveKind::BinHexOverSea
                    | MacArchiveKind::BinHexOverCompactPro => {
                        format!(
                            "Would you like to convert {filename} to binary and/or expand the contents?"
                        )
                    }
                };
                ui.label(body);
                ui.add_space(8.0);
                ui.horizontal(|ui| match pending.kind {
                    MacArchiveKind::BinHexSingleFile => {
                        if ui
                            .button("Convert")
                            .on_hover_text(
                                "Decode the BinHex envelope and add the enclosed file \
                                 to this directory.",
                            )
                            .clicked()
                        {
                            action = Some(ImportAction::Extract);
                        }
                        if ui
                            .button("Add as-is")
                            .on_hover_text("Add the .hqx file unchanged.")
                            .clicked()
                        {
                            action = Some(ImportAction::AsIs);
                        }
                        if ui.button("Cancel").clicked() {
                            action = Some(ImportAction::Cancel);
                        }
                    }
                    MacArchiveKind::Sit
                    | MacArchiveKind::Sit5
                    | MacArchiveKind::Sea
                    | MacArchiveKind::CompactPro
                    | MacArchiveKind::Mar
                    | MacArchiveKind::MacBinary
                    | MacArchiveKind::MacZip => {
                        if ui
                            .button("Expand")
                            .on_hover_text(
                                "Decompress the archive and add each enclosed file to this \
                                 directory (resource forks preserved on HFS/HFS+).",
                            )
                            .clicked()
                        {
                            action = Some(ImportAction::Extract);
                        }
                        if ui
                            .button("Add as-is")
                            .on_hover_text("Add the archive file unchanged.")
                            .clicked()
                        {
                            action = Some(ImportAction::AsIs);
                        }
                        if ui.button("Cancel").clicked() {
                            action = Some(ImportAction::Cancel);
                        }
                    }
                    MacArchiveKind::BinHexOverSit
                    | MacArchiveKind::BinHexOverSea
                    | MacArchiveKind::BinHexOverCompactPro => {
                        if ui
                            .button("Convert and expand")
                            .on_hover_text(
                                "Decode BinHex, decompress the inner archive, and add \
                                 each enclosed file to this directory.",
                            )
                            .clicked()
                        {
                            action = Some(ImportAction::Extract);
                        }
                        let convert_only_label = match pending.kind {
                            MacArchiveKind::BinHexOverSea => "Convert only (.sea lands here)",
                            MacArchiveKind::BinHexOverCompactPro => {
                                "Convert only (inner archive lands here)"
                            }
                            _ => "Convert only (.sit lands here)",
                        };
                        if ui
                            .button(convert_only_label)
                            .on_hover_text(
                                "Strip the BinHex wrapper and add the inner archive file. \
                                 Useful when the target Mac has StuffIt Expander but no \
                                 BinHex tool.",
                            )
                            .clicked()
                        {
                            action = Some(ImportAction::ConvertOnly);
                        }
                        if ui
                            .button("Add as-is")
                            .on_hover_text("Add the wrapped archive unchanged.")
                            .clicked()
                        {
                            action = Some(ImportAction::AsIs);
                        }
                        if ui.button("Cancel").clicked() {
                            action = Some(ImportAction::Cancel);
                        }
                    }
                });
            });

        let Some(action) = action else { return };
        // Pop the queue front (matches the snapshot above).
        let pending = self.pending_archive_imports.remove(0);
        let outcome = match action {
            ImportAction::Extract => self.apply_archive_import_extract(&pending),
            ImportAction::ConvertOnly => self.apply_archive_import_convert_only(&pending),
            ImportAction::AsIs => self.apply_archive_import_as_is(&pending),
            ImportAction::Cancel => return,
        };
        match outcome {
            Ok(()) => {
                self.edit_result = Some(format!(
                    "{} {filename}",
                    match action {
                        ImportAction::Extract => "Imported contents of",
                        ImportAction::ConvertOnly => "Imported inner archive from",
                        ImportAction::AsIs => "Imported (as-is)",
                        ImportAction::Cancel => "Cancelled",
                    }
                ));
            }
            Err(e) => {
                self.staging_errors
                    .push((pending.host_path.clone(), e.clone()));
                self.show_staging_errors = true;
                self.edit_result = Some(format!("Failed to import {filename}: {e}"));
            }
        }
    }

    /// Render the Workflow E floating "Archive viewer" window. Mirrors
    /// the standalone Archives tab shape: entry list with per-entry
    /// checkboxes, fork-format dropdown, Extract All / Extract Selected
    /// / Close buttons. Read-only — no edit operations inside the
    /// archive (would need archive repacking, deferred follow-up).
    fn render_archive_browse_window(&mut self, ui: &mut egui::Ui) {
        if self.mac_archive_window.is_none() {
            return;
        }
        let mut close = false;
        let mut extract_action: Option<bool> = None; // Some(true)=all, Some(false)=selected

        // Snapshot read-only labels so the closure below doesn't have to
        // re-borrow self.mac_archive_window through more than the
        // mutable handle it already takes for checkboxes / format.
        let (title, file_count, selected_file_count) = {
            let w = self.mac_archive_window.as_ref().unwrap();
            let fc = w.archive.entries.iter().filter(|e| !e.is_dir).count();
            let sfc = w
                .archive
                .entries
                .iter()
                .enumerate()
                .filter(|(i, e)| !e.is_dir && w.selected.get(*i).copied().unwrap_or(true))
                .count();
            (
                format!("Archive: {} ({})", w.entry_name, w.kind.label()),
                fc,
                sfc,
            )
        };

        egui::Window::new(title)
            .id(egui::Id::unique("mac_archive_window"))
            .collapsible(false)
            .resizable(true)
            .default_size([640.0, 480.0])
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.label(format!("{file_count} file(s) in archive (read-only view)."));
                ui.separator();

                egui::ScrollArea::vertical()
                    .max_height(320.0)
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        let Some(w) = self.mac_archive_window.as_mut() else {
                            return;
                        };
                        egui::Grid::new("mac_archive_window_entries")
                            .striped(true)
                            .num_columns(5)
                            .spacing([12.0, 2.0])
                            .show(ui, |ui| {
                                ui.strong("Extract");
                                ui.strong("Name");
                                ui.strong("Type/Creator");
                                ui.strong("Size");
                                ui.strong("Method");
                                ui.end_row();

                                for i in 0..w.archive.entries.len() {
                                    let e = &w.archive.entries[i];
                                    if e.is_dir {
                                        ui.label("");
                                        ui.label(format!("[ {} ]", e.display_path()));
                                        ui.label("");
                                        ui.label("");
                                        ui.label("");
                                        ui.end_row();
                                        continue;
                                    }
                                    let checked = w.selected.get(i).copied().unwrap_or(true);
                                    let mut new_checked = checked;
                                    ui.checkbox(&mut new_checked, "");
                                    if new_checked != checked {
                                        if let Some(slot) = w.selected.get_mut(i) {
                                            *slot = new_checked;
                                        }
                                    }
                                    ui.label(e.display_path());
                                    let tc = String::from_utf8_lossy(&e.type_code);
                                    let cc = String::from_utf8_lossy(&e.creator_code);
                                    ui.label(format!("{tc} / {cc}"));
                                    let (size, method) = e
                                        .data
                                        .as_ref()
                                        .filter(|f| f.uncompressed_len > 0)
                                        .or(e.rsrc.as_ref())
                                        .map(|f| (f.uncompressed_len, f.method_name()))
                                        .unwrap_or((0, "None"));
                                    ui.label(human_size_b(size as u64));
                                    ui.label(method);
                                    ui.end_row();
                                }
                            });
                    });

                ui.separator();
                ui.horizontal(|ui| {
                    ui.label("Extract as:");
                    if let Some(w) = self.mac_archive_window.as_mut() {
                        egui::ComboBox::from_id_salt("mac_archive_window_fork_format")
                            .selected_text(w.fork_format.label())
                            .show_ui(ui, |ui| {
                                for f in mac_extract::ForkFormat::ALL {
                                    ui.selectable_value(&mut w.fork_format, f, f.label());
                                }
                            });
                    }

                    if ui.button("Extract All...").clicked() {
                        extract_action = Some(true);
                    }
                    if ui
                        .add_enabled(
                            selected_file_count > 0,
                            egui::Button::new(format!(
                                "Extract Selected... ({selected_file_count})"
                            )),
                        )
                        .clicked()
                    {
                        extract_action = Some(false);
                    }
                    if ui.button("Close").clicked() {
                        close = true;
                    }
                });
            });

        if let Some(extract_all) = extract_action {
            self.run_archive_window_extract(extract_all);
        }
        if close {
            self.mac_archive_window = None;
        }
    }

    /// Drives the Workflow E extract action: picks an output folder and
    /// runs the same extract pipeline the standalone Archives tab uses.
    fn run_archive_window_extract(&mut self, all: bool) {
        let dest = match super::file_dialog()
            .set_title("Extract archive contents to folder")
            .pick_folder()
        {
            Some(d) => d,
            None => return,
        };
        let Some(w) = self.mac_archive_window.as_ref() else {
            return;
        };
        let bytes = w.bytes.clone();
        let archive = w.archive.clone();
        let selected = w.selected.clone();
        let format = w.fork_format;
        let result = if all {
            mac_extract::extract_all(&bytes, &archive, &dest, format, |_, _, _| {}, |_| {})
        } else {
            mac_extract::extract_filtered(
                &bytes,
                &archive,
                &dest,
                format,
                |i| selected.get(i).copied().unwrap_or(true),
                |_, _, _| {},
                |_| {},
            )
        };
        match result {
            Ok(stats) => {
                self.edit_result = Some(format!(
                    "Extracted {} file(s) to {} as {}{}",
                    stats.files,
                    dest.display(),
                    format.label(),
                    if stats.skipped > 0 {
                        format!(" ({} skipped)", stats.skipped)
                    } else {
                        String::new()
                    },
                ));
            }
            Err(e) => {
                self.edit_result = Some(format!("Extract failed: {e}"));
            }
        }
    }

    /// Render the Workflow C "Save Mac archive" modal: the selected
    /// file inside the disk image sniffed as a Mac archive; the user
    /// can either dump the raw bytes (Save as-is) or decode into a
    /// folder using a fork-preserving container (Decode and save).
    fn render_archive_extract_dialog(&mut self, ui: &mut egui::Ui) {
        if self.pending_archive_extract.is_none() {
            return;
        }

        #[derive(Clone, Copy)]
        enum CAction {
            SaveAsIs,
            Decode,
            Cancel,
        }
        let mut action: Option<CAction> = None;

        // Snapshot fields for rendering; the mutable handle is reborrowed
        // below for the fork-format dropdown.
        let (entry_name, kind_label) = {
            let p = self.pending_archive_extract.as_ref().unwrap();
            (p.entry_name.clone(), p.kind.label())
        };

        egui::Window::new("Save Mac archive")
            .id(egui::Id::unique("archive_extract_dialog"))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ui.ctx(), |ui| {
                ui.label(format!("{entry_name} is a {kind_label} archive."));
                ui.add_space(4.0);
                ui.label("How would you like to save it?");
                ui.add_space(8.0);

                // Fork-format dropdown for the Decode path (no effect
                // when the user picks Save as-is).
                ui.horizontal(|ui| {
                    ui.label("Decoded fork container:");
                    if let Some(p) = self.pending_archive_extract.as_mut() {
                        egui::ComboBox::from_id_salt("archive_extract_fork_format")
                            .selected_text(p.fork_format.label())
                            .show_ui(ui, |ui| {
                                for f in mac_extract::ForkFormat::ALL {
                                    ui.selectable_value(&mut p.fork_format, f, f.label());
                                }
                            });
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
                        action = Some(CAction::SaveAsIs);
                    }
                    if ui
                        .button("Decode and save contents to a folder...")
                        .on_hover_text("Decompress on the host into the chosen fork container.")
                        .clicked()
                    {
                        action = Some(CAction::Decode);
                    }
                    if ui.button("Cancel").clicked() {
                        action = Some(CAction::Cancel);
                    }
                });
            });

        let Some(action) = action else { return };
        // Take the pending state; subsequent actions own it.
        let pending = self.pending_archive_extract.take().unwrap();
        match action {
            CAction::SaveAsIs => {
                let default = pending.entry_name.clone();
                if let Some(dest) = super::file_dialog()
                    .set_title("Save archive as-is")
                    .set_file_name(&default)
                    .save_file()
                {
                    match std::fs::write(&dest, &pending.raw_bytes) {
                        Ok(()) => {
                            self.edit_result = Some(format!(
                                "Wrote {} ({})",
                                dest.display(),
                                disk_core::display::format_size(pending.raw_bytes.len() as u64),
                            ));
                        }
                        Err(e) => {
                            self.edit_result = Some(format!("Write failed: {e}"));
                        }
                    }
                }
            }
            CAction::Decode => {
                if let Some(dest_dir) = super::file_dialog()
                    .set_title("Save decoded contents to folder")
                    .pick_folder()
                {
                    let result = mac_extract::extract_all(
                        &pending.extract_bytes,
                        &pending.archive,
                        &dest_dir,
                        pending.fork_format,
                        |_, _, _| {},
                        |_| {},
                    );
                    match result {
                        Ok(stats) => {
                            self.edit_result = Some(format!(
                                "Extracted {} files to {} as {}{}",
                                stats.files,
                                dest_dir.display(),
                                pending.fork_format.label(),
                                if stats.skipped > 0 {
                                    format!(" ({} skipped)", stats.skipped)
                                } else {
                                    String::new()
                                },
                            ));
                        }
                        Err(e) => {
                            self.edit_result = Some(format!("Extract failed: {e}"));
                        }
                    }
                }
            }
            CAction::Cancel => {}
        }
    }

    fn exit_edit_mode(&mut self) {
        self.edit_mode = false;
        self.edit_free_space = None;
        self.free_space_job = None;
        self.edit_result = None;
        self.ctl.host_pane().queue.clear();
        self.pending_archive_imports.clear();
        self.archive_import_tempdir = None;

        if self.chd_edit.is_some() {
            self.start_chd_flatten();
        } else if self.archive_edit_ctx.is_some() {
            self.start_archive_compress();
        }
    }

    /// The items that could not be staged: the pane's host staging, then an archive import's failure.
    fn render_staging_errors_dialog(&mut self, ui: &mut egui::Ui) {
        let mut rows: Vec<(String, String)> = self.ctl.pane(Side::Left).staging_errors().to_vec();
        if self.show_staging_errors {
            let own = self.staging_errors.iter();
            rows.extend(own.map(|(p, why)| (p.display().to_string(), why.clone())));
        }
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
            self.show_staging_errors = false;
            self.staging_errors.clear();
            let act = BrowserPaneAction::DismissStagingErrors;
            self.ctl.apply(CommanderAction::Pane(Side::Left, act));
        }
    }

    /// Render the filesystem check results popup.
    fn render_chd_info_popup(&mut self, ui: &mut egui::Ui) {
        let Some(text) = self.chd_info_text.clone() else {
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
            self.chd_info_text = None;
        }
    }

    /// The pane's check or repair report as the browser always showed it.
    fn render_fsck_popup(&mut self, ui: &mut egui::Ui) {
        let Some(report) = self.ctl.pane(Side::Left).tool_report().cloned() else {
            self.repair_seen = false;
            return;
        };
        if report.repair.is_some() && !self.repair_seen {
            // The repair wrote the volume: probes and the legacy reader are stale.
            self.repair_seen = true;
            self.edit_free_space = None;
            self.free_space_job = None;
            self.invalidate_cached_fs();
            if self.chd_edit.is_some() {
                self.chd_diff_dirty = true;
            }
        }
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
                if check.repairable && self.archive_edit_ctx.is_none() {
                    ui.separator();
                    let resp = ui
                        .add_enabled(self.edit_mode, egui::Button::new("Repair"))
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

    /// The running check, repair, tree listing or journal read.
    fn render_tool_job(&self, ui: &mut egui::Ui) {
        let Some(snap) = self.ctl.pane(Side::Left).tool_working() else {
            return;
        };
        egui::Window::new(snap.label.clone())
            .collapsible(false)
            .resizable(false)
            .show(ui.ctx(), |ui| {
                super::progress::show_job(ui, &snap);
            });
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
            if let Some(text) = &self.tree_text {
                let text = text.clone();
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Save tree view")
                    .set_file_name("tree.txt")
                    .add_filter("Text files", &["txt"])
                    .save_file()
                {
                    if let Err(e) = std::fs::write(&path, &text) {
                        self.error = Some(format!("Failed to save tree: {}", e));
                    }
                }
            }
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
            if let Some(text) = self.tree_text.clone() {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Save tree view")
                    .set_file_name("tree.txt")
                    .add_filter("Text files", &["txt"])
                    .save_file()
                {
                    match std::fs::write(&path, &text) {
                        Ok(()) => {
                            self.show_tree_large_dialog = false;
                        }
                        Err(e) => {
                            self.error = Some(format!("Failed to save tree: {}", e));
                        }
                    }
                }
            } else {
                self.show_tree_large_dialog = false;
            }
        }
        if show_anyway {
            self.show_tree_large_dialog = false;
            self.show_tree_popup = true;
        }
    }

    /// Handle files dropped from the host OS onto the window.
    fn handle_dropped_files(&mut self, ui: &mut egui::Ui) {
        if !self.edit_mode {
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
            self.add_host_paths(&dropped);
        }
    }

    /// Borrow the cached read-only filesystem, opening fresh if the cache is
    /// empty. The caller MUST return the fs via [`return_fs`](Self::return_fs)
    /// after use so subsequent calls can reuse it. Returning an `Option` (vs.
    /// a `&mut`) keeps the borrow off `self` so callers can also mutate other
    /// `BrowseView` fields in the same scope.
    fn take_or_open_fs(&mut self) -> Option<Box<dyn Filesystem>> {
        if let Some(fs) = self.cached_fs.take() {
            return Some(fs);
        }
        match self.session.open() {
            Ok(mut fs) => {
                if self.show_deleted {
                    fs.set_show_deleted(true);
                }
                Some(fs)
            }
            Err(e) => {
                log::debug!("session.open() failed: {e}");
                None
            }
        }
    }

    /// Return a previously taken filesystem to the cache.
    fn return_fs(&mut self, fs: Box<dyn Filesystem>) {
        self.cached_fs = Some(fs);
    }

    /// Drop the cached filesystem so the next read re-opens from disk.
    /// Call this whenever the underlying volume bytes may have changed
    /// (after sync_metadata, archive recompress, etc.).
    fn invalidate_cached_fs(&mut self) {
        self.pane_stale = true;
        self.forget_cached_fs();
    }

    /// Drop this view's own reader (the pane reopened by itself after its Apply).
    fn forget_cached_fs(&mut self) {
        self.cached_fs = None;
        self.fs_generation = self.fs_generation.wrapping_add(1);
        // Boot-block status is recomputed lazily from the (now possibly
        // changed) backing bytes the next time the header renders.
        self.boot_blocks_present = None;
    }

    /// Best-effort: does the open volume have HFS boot blocks at sector 0?
    /// Reads two bytes from the backing file at the partition offset. Returns
    /// `None` when unknown — no file-backed source (e.g. a CHD edit session),
    /// or a read error. Only trustworthy in edit mode, where the backing file
    /// holds the raw, decompressed image bytes.
    /// The extraction's progress window, then its status line (or the nested image) as the result.
    fn poll_extraction(&mut self) {
        if !self.extracting || self.ctl.copy_job().is_some() || self.effects.modal_open() {
            return;
        }
        if self.ctl.host_conflict().is_some() {
            return;
        }
        self.extracting = false;
        let said = self.ctl.status().to_string();
        let Some(temp) = self.nested_open.take() else {
            if !said.is_empty() {
                self.extraction_result = Some(said);
            }
            return;
        };
        let extracted = std::fs::read_dir(temp.path())
            .ok()
            .and_then(|mut d| d.find_map(|e| e.ok()))
            .map(|e| e.path());
        match extracted {
            Some(path) => {
                self.extraction_result =
                    Some(format!("Opening {} as a disk image...", path.display()));
                self.nested_ready = Some((path, temp));
            }
            None => self.extraction_result = Some(format!("Open as disk image: {said}")),
        }
    }
}

/// Compact byte-count formatter for the Workflow E archive viewer
/// window. Mirrors `archives_tab::human_size` rather than
/// `disk_core::display::format_size` (which uses base-1000 GB/MB and is meant
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

/// Workflow B: which on-host archive format to write when the user
/// clicks one of the "Export Mac archive" buttons under the browse-view
/// extract row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchiveExportFormat {
    /// `.hqx` — BinHex 4.0. Single file only.
    Hqx,
    /// `.sit` — classic StuffIt. Any selection (file or folder).
    Sit,
    /// `.sit.hqx` — StuffIt-over-BinHex. Any selection.
    SitHqx,
    /// `.mar` — Hampa Hug's MAR (stored). A single file or folder selection
    /// (multiple top-level items are wrapped in a folder).
    Mar,
}

impl ArchiveExportFormat {
    fn extension(self) -> &'static str {
        match self {
            ArchiveExportFormat::Hqx => ".hqx",
            ArchiveExportFormat::Sit => ".sit",
            ArchiveExportFormat::SitHqx => ".sit.hqx",
            ArchiveExportFormat::Mar => ".mar",
        }
    }
    fn label(self) -> &'static str {
        match self {
            ArchiveExportFormat::Hqx => "BinHex 4.0 (.hqx)",
            ArchiveExportFormat::Sit => "StuffIt (.sit)",
            ArchiveExportFormat::SitHqx => "StuffIt-over-BinHex (.sit.hqx)",
            ArchiveExportFormat::Mar => "MAR (.mar)",
        }
    }
}

/// Recursive helper for [`BrowseView::collect_archive_inputs`]: walks
/// `entry` on the open `fs` and produces zero or more
/// [`StuffItInputNode`]s. Files become `File` nodes carrying both forks
/// plus type/creator; directories become `Folder` nodes containing the
/// recursive walk of their children. Symlinks and special files are
/// dropped silently — there's no Mac-archive representation for them.
fn walk_fs_to_input_nodes(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
) -> Result<Vec<StuffItInputNode>, String> {
    if entry.is_directory() {
        let children = fs.list_directory(entry).map_err(|e| e.to_string())?;
        let mut inner: Vec<StuffItInputNode> = Vec::new();
        for child in children {
            inner.extend(walk_fs_to_input_nodes(fs, &child)?);
        }
        // A directory at the volume root (path "/") expands to its
        // children directly — bundling the root would yield a strangely-
        // named outer folder ("/" or ""), and the user almost always
        // means "bundle these children" when they pick root.
        if entry.path == "/" {
            return Ok(inner);
        }
        Ok(vec![StuffItInputNode::Folder {
            name: entry.name.clone(),
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            children: inner,
        }])
    } else if entry.is_file() {
        let data = fs.read_file(entry, usize::MAX).map_err(|e| e.to_string())?;
        let mut rsrc: Vec<u8> = Vec::new();
        // Best-effort: missing resource fork support / empty fork =>
        // zero bytes (the StuffIt writer drops it on the floor).
        let _ = fs.write_resource_fork_to(entry, &mut rsrc);

        let type_code = entry.type_code.unwrap_or([0; 4]);
        let creator_code = entry.creator_code.unwrap_or([0; 4]);

        Ok(vec![StuffItInputNode::File(StuffItInput {
            name: entry.name.clone(),
            type_code,
            creator_code,
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: data,
            resource_fork: rsrc,
        })])
    } else {
        Ok(Vec::new())
    }
}

rusty_backup::model::worker::finished_with_error!(ChdFlattenProgress, TarExportProgress);

/// What to do once a staged-edit Apply lands without an error.
#[derive(Clone, Copy)]
enum ApplyFollowUp {
    /// Re-encode the edited archive (the Apply button and the conflict review).
    RecompressArchive,
    /// Close the view or leave edit mode (the unsaved-changes dialog).
    LeaveEditing,
}

/// A sniff's result: the filesystem to put back (when there was one), and the archive kind found.
type ArchiveSniff = (Option<Box<dyn Filesystem>>, Option<MacArchiveKind>);

/// What a tar-import job ended with.
enum TarStep {
    /// The preflight found content that would be skipped or dropped; ask before importing.
    Confirm(PathBuf, fs_tar::tar_import::TarImportPreflight),
    /// The status-line message, and whether anything was written.
    Finished(String, bool),
}

/// The tar import, off the UI thread: preflight on a throwaway editable open, then import, sync and commit.
fn tar_import_step(
    session: &BrowseSession,
    dest: &FileEntry,
    archive: PathBuf,
    check: bool,
    job: &rusty_backup::model::job::JobCtx,
) -> TarStep {
    let opts = fs_tar::tar_import::TarImportOptions {
        conflict: fs_tar::tar_import::ImportConflict::Skip,
        ..Default::default()
    };
    if check {
        job.set_phase("Checking the archive...");
        let preflight = {
            let (efs, _commit) = match session.open_editable() {
                Ok(pair) => pair,
                Err(e) => {
                    return TarStep::Finished(format!("Error opening filesystem: {e}"), false)
                }
            };
            match fs_tar::tar_import::preflight_tar_from_path(&*efs, &archive, &opts) {
                Ok(pf) => pf,
                Err(e) => {
                    return TarStep::Finished(format!("Could not read archive: {e:#}"), false)
                }
            }
        };
        if preflight.has_warnings() {
            return TarStep::Confirm(archive, preflight);
        }
    }
    job.set_phase("Importing...");
    let (mut efs, commit) = match session.open_editable() {
        Ok(pair) => pair,
        Err(e) => return TarStep::Finished(format!("Error opening filesystem: {e}"), false),
    };
    let report = |st: &fs_tar::tar_import::TarImportStats| {
        job.set_phase(format!(
            "Importing... {} file(s), {} folder(s)",
            st.files, st.dirs_created
        ));
    };
    let stats =
        match fs_tar::tar_import::import_tar_from_path(&mut *efs, dest, &archive, &opts, &report) {
            Ok(s) => s,
            Err(e) => return TarStep::Finished(format!("Import failed: {e:#}"), false),
        };
    if let Err(e) = efs.sync_metadata() {
        return TarStep::Finished(format!("Error saving to disk: {e}"), false);
    }
    drop(efs);
    if let Err(e) = commit.commit() {
        return TarStep::Finished(format!("Error writing container: {e}"), false);
    }
    let mut msg = format!(
        "Imported {} files, {} dirs, {} symlinks into /{}",
        stats.files,
        stats.dirs_created,
        stats.symlinks,
        if dest.path == "/" {
            String::new()
        } else {
            dest.name.clone()
        }
    );
    if stats.symlinks_skipped > 0 {
        msg.push_str(&format!("; {} symlink(s) skipped", stats.symlinks_skipped));
    }
    if stats.appledouble_skipped > 0 {
        msg.push_str(&format!(
            "; {} ._ sidecar(s) skipped",
            stats.appledouble_skipped
        ));
    }
    if stats.appledouble_paired > 0 {
        msg.push_str(&format!(
            "; {} ._ sidecar(s) joined to their file",
            stats.appledouble_paired
        ));
    }
    if stats.invalid_names_skipped > 0 {
        msg.push_str(&format!(
            "; {} bad-name entr(ies) skipped",
            stats.invalid_names_skipped
        ));
    }
    if stats.skipped_existing > 0 {
        msg.push_str(&format!("; {} already existed", stats.skipped_existing));
    }
    TarStep::Finished(msg, true)
}

/// A read the browse view queues off the UI thread.
enum FsRead {
    /// Read a whole file and decode it as a Mac archive.
    Archive(FileEntry, ArchiveUse),
}

impl FsRead {
    /// The same read, failed with `message` (when the filesystem would not open).
    fn failed(self, message: String) -> FsReadDone {
        match self {
            FsRead::Archive(entry, purpose) => FsReadDone::Archive(entry, purpose, Err(message)),
        }
    }
}

/// What a queued read produced.
enum FsReadDone {
    Archive(FileEntry, ArchiveUse, Result<ArchiveRead, String>),
}

/// The label of a Mac-archive read, which gets its own progress window.
const ARCHIVE_READ_LABEL: &str = "Reading the archive...";

/// Which window a decoded archive opens.
#[derive(Clone, Copy)]
enum ArchiveUse {
    /// The read-only archive viewer (Workflow E).
    Browse,
    /// The Save-as-is / Decode-and-save modal (Workflow C).
    Extract,
}

/// A Mac archive read off the volume and decoded.
struct ArchiveRead {
    kind: MacArchiveKind,
    /// The file as stored (Extract saves it as-is).
    raw: Vec<u8>,
    bytes: Vec<u8>,
    archive: arc_mac::stuffit::StuffItArchive,
}

/// Read `entry` whole, check it is a Mac archive, and decode it; the error is the status-line message.
fn read_mac_archive(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    purpose: ArchiveUse,
) -> Result<ArchiveRead, String> {
    let raw = fs
        .read_file(entry, usize::MAX)
        .map_err(|e| format!("Read failed: {e}"))?;
    let kind = detect_mac_archive(&raw).ok_or_else(|| match purpose {
        ArchiveUse::Browse => format!("{} is not a recognized Mac archive.", entry.name),
        ArchiveUse::Extract => format!(
            "{} is not a recognized Mac archive (use Extract File to save raw bytes).",
            entry.name
        ),
    })?;
    let keep_raw = match purpose {
        ArchiveUse::Browse => Vec::new(),
        ArchiveUse::Extract => raw.clone(),
    };
    let (bytes, archive) =
        mac_extract::open_bytes(raw).map_err(|e| format!("{} decode failed: {e}", entry.name))?;
    Ok(ArchiveRead {
        kind,
        raw: keep_raw,
        bytes,
        archive,
    })
}

/// A finished read, and the filesystem it borrowed (when it had one to give back).
type FsReadOut = (Option<Box<dyn Filesystem>>, FsReadDone);
