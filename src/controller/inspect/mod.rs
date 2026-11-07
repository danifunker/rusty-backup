//! Inspect: what a disk, image, backup folder or remote image holds, and the disk-level tools on it.
//!
//! [`InspectController`] owns the inspected source and what was found in it (partition table,
//! alignment, partitions, backup or Clonezilla metadata, minimum sizes, volume labels), the jobs
//! that find it, and the per-partition and disk-level tools: Check / Repair, Calc min, Make
//! Bootable, the partition-table editor, Expand Image, Defragment, the word-order swap, Expand HFS,
//! CHD Info, Export and the remote "Back Up Image". Browsing a partition is handed to the host as a
//! [`BrowseOpen`] the browse view opens on its own controller.
//!
//! The egui Inspect tab (`src/gui/inspect_tab.rs`) is the shell: it maps the source picker,
//! buttons and dialogs onto [`InspectAction`]s, fulfils the [`Effect`]s (pickers, confirmations),
//! drains [`InspectController::take_log`] into the log panel and carries out each
//! [`HostRequest`]. Dialogs with their own controllers start from accessors here: resize
//! (`controller::resize` from [`InspectController::resize_plan`]), Expand HFS
//! (`controller::expand_hfs`), Alto resize, floppy convert, the remote picker, and the disk writer.
//! See docs/phase2_commander_plan.md § "Slice 11".

mod browse;
mod export;
mod layout;
mod open;
mod tools;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::backup::metadata::BackupMetadata;
use crate::controller::log::LogLevel;
use crate::controller::{ConfirmSpec, Controller, Effect, PickSpec};
use crate::model::job::{Job, Notify, Snapshot};
use crate::partition::{PartitionAlignment, PartitionInfo, PartitionTable};
use dsk_clonezilla::metadata::ClonezillaImage;

pub use crate::controller::browser::{CheckSummary, RepairSummary};
pub use export::{
    per_partition_refusal, ExportFormatChoice, ExportNotes, ExportSettings, LayoutBar, CHD_REFUSAL,
    EXPORT_FORMATS,
};
pub use layout::{
    format_block_size, format_bytes_grouped, format_size_decimal, format_size_exact, hints,
    segment_at, segment_widths, LayoutSegment, PartitionRow, SegmentKind, BAR_HEIGHT,
    LABEL_THRESHOLD_PX, MIN_SEGMENT_PX,
};
pub use tools::{CheckReport, FsckView, ResizePlan};

// Plain data the views need, so they never name the model (CONTRIBUTING.md MVC).
pub use crate::model::browse_session::BrowseSession;
pub use crate::model::export_runner::PartitionExportConfig;
/// A device's table and size read without elevating, for a shell's quick device view.
pub use crate::model::inspect_runner::{
    read_table_unelevated, size_unelevated, PlainPartition, PlainTable,
};
pub use crate::model::job::{JobRegistry, NoNotify};
pub use crate::model::partition_editor::PartitionEditor;
pub use crate::model::size_mode::SizeMode;
pub use crate::rbformats::chd_options::{ChdOptions, ChdProfile};
pub use crate::rbformats::export::ExportFormat;
pub use disk_core::display::format_size;
pub use dsk_flux::flux::FaultReport;

/// A remote image on an `rb-cli serve` daemon: the shared connection and the daemon-side path.
#[cfg(feature = "remote")]
pub type RemoteImage = crate::model::inspect_runner::RemoteImage;

/// The largest Expand Image step, in MiB (4 TiB).
pub const EXPAND_IMAGE_MIB_MAX: u32 = 4_194_304;

/// The `purpose` of each picker and confirmation this controller asks for.
pub mod purpose {
    pub const DISCARD_EDITS: &str = "inspect.discard_edits";
    pub const REPAIR: &str = "inspect.repair";
    pub const DEFRAGMENT: &str = "inspect.defragment";
    /// Apply Changes in the partition-table editor.
    pub const APPLY_TABLE: &str = "inspect.apply_table";
    pub const BOOT_DONOR: &str = "inspect.boot_donor";
    pub const SWAB_DEST: &str = "inspect.swab_dest";
    pub const EXPORT_DISK: &str = "inspect.export_disk";
    pub const EXPORT_FOLDER: &str = "inspect.export_folder";
    pub const EXPORT_HFV: &str = "inspect.export_hfv";
    pub const REMOTE_BACKUP: &str = "inspect.remote_backup";
    pub const OPEN_IMAGE: &str = "inspect.open_image";
    pub const OPEN_BACKUP: &str = "inspect.open_backup";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const REINSPECT: &str = "inspect.reinspect";
    pub const EXPORT: &str = "inspect.export";
    pub const EDIT_TABLE: &str = "inspect.edit_table";
    pub const ADD_PARTITION: &str = "inspect.add_partition";
    pub const EXPAND_IMAGE: &str = "inspect.expand_image";
    pub const EXPAND_IMAGE_ADD: &str = "inspect.expand_image.add_mib";
    pub const EXPAND_IMAGE_START: &str = "inspect.expand_image.start";
    pub const CHECK_REPAIR: &str = "inspect.check.repair";
    pub const CHECK_DEBUG: &str = "inspect.check.debug";
    pub const MAKE_BOOTABLE: &str = "inspect.make_bootable";
    pub const RESIZE: &str = "inspect.resize";
    pub const CHD_INFO: &str = "inspect.chd_info";
    pub const CLOSE: &str = "inspect.close";
    pub const SOURCE: &str = "inspect.source";
    pub const SOURCE_RECENT: &str = "inspect.source.recent";
    pub const SOURCE_DEVICE: &str = "inspect.source.device";
    pub const OPEN_FILE: &str = "inspect.source.open_file";
    pub const OPEN_BACKUP: &str = "inspect.source.open_backup";
    pub const CONNECT_REMOTE: &str = "inspect.source.connect_remote";
    pub const CONVERT_FLOPPY: &str = "inspect.convert_floppy";
    pub const CANCEL_EXPORT: &str = "inspect.cancel_export";
    pub const SWAP_WORD_ORDER: &str = "inspect.swap_word_order";
    pub const PARTITION_BAR: &str = "inspect.partition_bar";
    pub const PARTITION_TABLE: &str = "inspect.partition_table";
    pub const ROW_CALC_MIN: &str = "inspect.row.calc_min";
    pub const ROW_BROWSE: &str = "inspect.row.browse";
    pub const ROW_CHECK: &str = "inspect.row.check";
    pub const ROW_EXPAND_HFS: &str = "inspect.row.expand_hfs";
    pub const ROW_DEFRAGMENT: &str = "inspect.row.defragment";
    pub const ROW_RESIZE_ALTO: &str = "inspect.row.resize_alto";
}

/// What the user picked to inspect.
pub enum InspectSource {
    /// A physical disk: its row in the device list and what identifies it there.
    Device {
        slot: usize,
        identity: crate::device::DeviceIdentity,
    },
    /// An image file; `temp` keeps a decompressed `.adz` / `.hdz` (or a nested copy) alive.
    Image {
        path: PathBuf,
        temp: Option<tempfile::TempDir>,
    },
    /// An image file as the user named it; a wrapper is decoded to a temporary file first, in a job.
    ImageFile(PathBuf),
    /// A rusty-backup or Clonezilla backup folder.
    Backup { folder: PathBuf },
    /// An image on a daemon, read over the block tier.
    #[cfg(feature = "remote")]
    Remote { conn: RemoteImage },
}

/// The source as it stays open.
enum Source {
    Device {
        slot: usize,
        identity: crate::device::DeviceIdentity,
    },
    Image {
        path: PathBuf,
        _temp: Option<tempfile::TempDir>,
    },
    Backup {
        folder: PathBuf,
    },
    #[cfg(feature = "remote")]
    Remote {
        conn: RemoteImage,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsavedGate {
    Open,
    Close,
}

/// User intents.
pub enum InspectAction {
    /// Switch to a source the user picked (asks first while the browse view holds edits).
    Open(InspectSource),
    /// Open File: ask for an image, then open it as [`InspectAction::Open`] does.
    ChooseImage,
    /// Open Backup Folder: ask for a folder, then open it as [`InspectAction::Open`] does.
    ChooseBackupFolder,
    /// The Close button (asks first while the browse view holds edits).
    Close,
    /// Switch without asking: a hand-off from another tab, a nested image, a device list change.
    Load(InspectSource),
    /// Close without asking; `quiet` logs nothing (a remote session closing).
    Unload {
        quiet: bool,
    },
    Reinspect,
    /// Select the partition with this index in the bar and the list, or clear the selection.
    SelectPartition(Option<usize>),
    /// Compute the deferred minimum size of the partition with this index.
    CalcMin(usize),
    /// Browse the partition with this index.
    Browse(usize),
    /// Check the filesystem of the partition with this index.
    Check(usize),
    /// Repair the checked filesystem (asks first).
    Repair,
    CloseCheck,
    ShowCheckDebug(bool),
    /// Defragment the Human68k partition with this index (asks first).
    Defragment(usize),
    /// Open Expand HFS on the partition with this index (summarised in a job first).
    ExpandHfs(usize),
    SwapWordOrder,
    MakeBootable,
    /// Grow the image file by this many bytes of zeros.
    ExpandImage(u64),
    ChdInfo,
    CloseChdInfo,
    /// Open the partition-table editor on the current table.
    EditTable,
    /// Open the editor with a new partition filling the largest free gap.
    AddPartition,
    CloseEditor,
    /// An edit in the partition-table window.
    Table(crate::controller::partition_edit::TableEditAction),
    /// Apply Changes: validates, asks, then writes the edits.
    ApplyTableEdits,
    /// Open the Export Disk Image window.
    OpenExport,
    CloseExport,
    /// The window's Export button, with the CHD options its control holds.
    StartExport(crate::rbformats::chd_options::ChdOptions),
    CancelExport,
    /// Pull a backup of the remote image to a local folder.
    BackupRemote,
    /// A dialog of the shell changed the disk (Resize): read the table again.
    DiskChanged,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Confirmed {
        purpose: &'static str,
        yes: bool,
    },
}

/// What the host screen must do; drained by [`InspectController::take_requests`].
pub enum HostRequest {
    /// Close the browse view (the source changed or its disk is being rewritten).
    CloseBrowse,
    /// Open the browse view on a partition.
    OpenBrowse(Box<BrowseOpen>),
    /// The streamed backup partition's seekable cache is ready at this path.
    UpgradeSeekable(PathBuf),
    /// A browsable optical disc, for the Optical tab.
    OpenOptical(PathBuf),
    /// A classic Mac archive, for the Archives tab.
    OpenArchive(PathBuf),
    /// The user closed a backup folder; other tabs drop the shared folder too.
    BackupClosed,
    /// Open the Expand HFS dialog on this summary.
    ExpandHfs(Box<crate::model::hfs_expand_runner::ExpandSource>),
}

/// How the browse view may edit what it opens.
pub enum BrowseEdit {
    ReadOnly,
    InPlace,
    Archive(crate::model::archive_edit::ArchiveEditContext),
}

/// A partition to browse: the session, how it may be edited, and the header's extras.
pub struct BrowseOpen {
    pub session: crate::model::browse_session::BrowseSession,
    pub edit: BrowseEdit,
    /// The Amiga drive name ("DH0") the header puts before the volume label.
    pub label_prefix: Option<String>,
    /// The single-file-CHD backup folder whose metadata a CHD save refreshes.
    pub backup_folder: Option<PathBuf>,
}

impl BrowseOpen {
    /// Edited through Edit Mode's copy (a compressed backup decompressed, a CHD over a diff),
    /// not written in place at once.
    pub fn edits_through_modes(&self) -> bool {
        let chd = self
            .session
            .source_path
            .as_ref()
            .and_then(|p| p.extension())
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("chd"));
        match &self.edit {
            BrowseEdit::Archive(_) => true,
            BrowseEdit::InPlace => chd,
            BrowseEdit::ReadOnly => false,
        }
    }
}

/// The partition-table row's header facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSummary {
    pub type_name: String,
    /// Set for tables readable in either 16-bit word order (the SGI label).
    pub byte_order: Option<&'static str>,
    pub disk_signature: u32,
    pub superfloppy: bool,
    /// The table names its own slots (diskutil's disk4s6, IRIX slot 0): show a Slot column.
    pub has_slots: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlignmentSummary {
    pub kind: String,
    pub first_lba: u64,
    pub heads: u32,
    pub sectors_per_track: u32,
}

/// A loaded backup folder's header lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupSummary {
    pub source: String,
    pub created: String,
    pub table: String,
    pub compression: String,
    pub checksum: String,
    pub sector_by_sector: bool,
    /// Imaged sizes plus the space before the first partition; 0 when unknown.
    pub min_restore_bytes: u64,
    pub source_size_bytes: u64,
    pub alignment: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClonezillaSummary {
    pub disk_name: String,
    pub source_size_bytes: u64,
    pub chs: Option<(u32, u32, u32)>,
    /// The image info's first line, its creation date.
    pub created: Option<String>,
}

/// The Min Size cell of a partition row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MinCell {
    Size(u64),
    /// A calculation runs; its last phase.
    Pending(String),
    /// The minimum needs a filesystem walk: offer "Calc min".
    CanCalc,
    Empty,
}

/// What a partition row may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RowActions {
    pub browse: bool,
    pub check: bool,
    pub expand_hfs: bool,
    pub defragment: bool,
    pub resize_alto: bool,
}

/// Jobs and the shell's wake handle.
pub struct InspectServices {
    pub jobs: JobRegistry,
    pub notify: Arc<dyn Notify>,
}

/// An image file ready to open, and the temporary folder that holds it when it was decoded.
type Prepared = (PathBuf, Option<tempfile::TempDir>);

/// Lines for the log panel, shared with running jobs.
type LogSink = Arc<Mutex<Vec<(LogLevel, String)>>>;

pub(crate) fn push_log(sink: &LogSink, level: LogLevel, msg: impl Into<String>) {
    crate::model::worker::lock_status(&**sink).push((level, msg.into()));
}

/// What a source was found to hold; dropped whenever the source changes.
#[derive(Default)]
struct Found {
    table: Option<PartitionTable>,
    alignment: Option<PartitionAlignment>,
    partitions: Vec<PartitionInfo>,
    backup: Option<BackupMetadata>,
    clonezilla: Option<ClonezillaImage>,
    min_sizes: HashMap<usize, u64>,
    defrag_min_sizes: HashMap<usize, u64>,
    volume_labels: HashMap<usize, String>,
    deferred: HashMap<usize, &'static str>,
    format_label: Option<String>,
    error: Option<String>,
    /// The CHD every partition probe reads through, when the source is one.
    chd_path: Option<PathBuf>,
    /// The single-file-CHD backup this CHD is the body of, and whether it was sector-by-sector.
    chd_backup: Option<(PathBuf, bool)>,
    /// macOS: the elevated device handle and claim, reused by browse and Calc min.
    device_file: Option<Arc<std::fs::File>>,
    device_guard: Option<crate::os::TempFileGuard>,
    disk_size: Option<Option<u64>>,
    /// The partition selected in the view, by index.
    selected: Option<usize>,
}

pub struct InspectController {
    services: InspectServices,
    source: Option<Source>,
    found: Found,
    log: LogSink,
    requests: Vec<HostRequest>,
    /// The browse view holds unapplied edits; set by the shell every frame.
    browse_dirty: bool,
    pending_gate: Option<(UnsavedGate, Option<InspectSource>)>,
    /// The device path this controller escalated, so closing releases exactly that one.
    escalated: Option<PathBuf>,
    /// Bumped when the partition list's text changes width, so the shell re-measures.
    layout_generation: u64,
    /// An `ImageFile` whose wrapper is being decoded before it opens.
    prepare: Option<(PathBuf, Job<Prepared>)>,
    open: open::OpenState,
    tools: tools::ToolState,
    export: export::ExportState,
    browse: browse::BrowseState,
}

impl InspectController {
    pub fn new(services: InspectServices) -> Self {
        InspectController {
            services,
            source: None,
            found: Found::default(),
            log: Arc::new(Mutex::new(Vec::new())),
            requests: Vec::new(),
            browse_dirty: false,
            pending_gate: None,
            escalated: None,
            layout_generation: 0,
            prepare: None,
            open: open::OpenState::default(),
            tools: tools::ToolState::default(),
            export: export::ExportState::default(),
            browse: browse::BrowseState::default(),
        }
    }

    fn info(&self, msg: impl Into<String>) {
        push_log(&self.log, LogLevel::Info, msg);
    }

    fn warn(&self, msg: impl Into<String>) {
        push_log(&self.log, LogLevel::Warning, msg);
    }

    fn error(&self, msg: impl Into<String>) {
        push_log(&self.log, LogLevel::Error, msg);
    }

    fn spawn<R: Send + 'static>(
        &self,
        label: &str,
        body: impl FnOnce(&crate::model::job::JobCtx) -> anyhow::Result<R> + Send + 'static,
    ) -> Job<R> {
        Job::spawn(label, Arc::clone(&self.services.notify), body)
    }

    /// Lines for the log panel since the last call.
    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        std::mem::take(&mut *crate::model::worker::lock_status(&*self.log))
    }

    /// What the host must do since the last call.
    pub fn take_requests(&mut self) -> Vec<HostRequest> {
        std::mem::take(&mut self.requests)
    }

    /// Tell the controller whether the browse view holds unapplied edits (each frame).
    pub fn set_browse_dirty(&mut self, dirty: bool) {
        self.browse_dirty = dirty;
    }

    // --- the source ---------------------------------------------------------------------

    pub fn image_path(&self) -> Option<&Path> {
        match &self.source {
            Some(Source::Image { path, .. }) => Some(path.as_path()),
            _ => None,
        }
    }

    pub fn backup_folder(&self) -> Option<&Path> {
        match &self.source {
            Some(Source::Backup { folder }) => Some(folder.as_path()),
            _ => None,
        }
    }

    pub fn device_path(&self) -> Option<&Path> {
        match &self.source {
            Some(Source::Device { identity, .. }) => Some(identity.path.as_path()),
            _ => None,
        }
    }

    /// The device list row the open device was picked from.
    pub fn device_slot(&self) -> Option<usize> {
        match &self.source {
            Some(Source::Device { slot, .. }) => Some(*slot),
            _ => None,
        }
    }

    /// "Refresh Devices" replaced the list: a different disk (or none) may now sit at our row.
    pub fn reconcile_devices(&mut self, devices: &[crate::device::DiskDevice]) {
        let Some(Source::Device { slot, identity }) = &self.source else {
            return;
        };
        let slot = *slot;
        let now = crate::device::identity_at(devices, Some(slot));
        if now.as_ref() == Some(identity) {
            return;
        }
        if self.browse_dirty {
            self.warn(
                "The device list changed under the open volume; staged edits were discarded.",
            );
        }
        match now {
            None => {
                self.warn("The inspected device is no longer listed; closing it.");
                self.unload(false);
            }
            Some(identity) => {
                self.warn("A different device now sits at the selected slot; re-inspecting it.");
                self.load(InspectSource::Device { slot, identity });
            }
        }
    }

    pub fn is_remote(&self) -> bool {
        #[cfg(feature = "remote")]
        {
            matches!(self.source, Some(Source::Remote { .. }))
        }
        #[cfg(not(feature = "remote"))]
        {
            false
        }
    }

    /// The remote image's connection and path, while one is inspected.
    #[cfg(feature = "remote")]
    pub fn remote(&self) -> Option<&RemoteImage> {
        match &self.source {
            Some(Source::Remote { conn }) => Some(conn),
            _ => None,
        }
    }

    /// The remote image's daemon-side path, for the source label.
    pub fn remote_path(&self) -> Option<&str> {
        #[cfg(feature = "remote")]
        if let Some(Source::Remote { conn }) = &self.source {
            return Some(conn.1.as_str());
        }
        None
    }

    /// The source as the picker names it: remote image, backup, image (with its format) or device.
    pub fn source_label(&self, devices: &[crate::device::DiskDevice]) -> String {
        if let Some(rpath) = self.remote_path() {
            let base = rpath.rsplit('/').find(|s| !s.is_empty()).unwrap_or(rpath);
            format!("Remote image: {base}")
        } else if let Some(path) = self.backup_folder() {
            format!("Backup: {}", path.display())
        } else if let Some(path) = self.image_path() {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            match self.format_label() {
                Some(label) => format!("{label}: {name}"),
                None => format!("Image: {name}"),
            }
        } else if let Some(idx) = self.device_slot() {
            devices
                .get(idx)
                .map(|d| d.display_name())
                .unwrap_or_else(|| "Unknown".into())
        } else {
            "Select a device or image...".into()
        }
    }

    /// This build can open a remote image (the `remote` feature).
    pub fn remote_available() -> bool {
        cfg!(feature = "remote")
    }

    /// Re-inspect applies: a source is open and not still loading.
    pub fn can_reinspect(&self) -> bool {
        self.has_source() && self.loading().is_none()
    }

    /// Close applies: a local source, not loading and not exporting (a remote one closes apart).
    pub fn can_close(&self) -> bool {
        self.has_local_source() && self.loading().is_none() && !self.export_running()
    }

    /// Expand Image's sizes for `add_mib` more MiB: (current, after); the step is capped at 4 TiB.
    pub fn expand_image_preview(&mut self, add_mib: u32) -> (u64, u64) {
        let current = self.disk_size().unwrap_or(0);
        (current, current.saturating_add(expand_image_bytes(add_mib)))
    }

    /// A device, image or backup folder is open (a remote session is not counted).
    pub fn has_local_source(&self) -> bool {
        self.source.is_some() && !self.is_remote()
    }

    pub fn has_source(&self) -> bool {
        self.source.is_some()
    }

    /// The device or image path a disk-level tool reads and writes.
    fn disk_path(&self) -> Option<PathBuf> {
        match &self.source {
            Some(Source::Device { identity, .. }) => Some(identity.path.clone()),
            Some(Source::Image { path, .. }) => Some(path.clone()),
            _ => None,
        }
    }

    /// The image is a plain file (not a device, backup or remote image) a tool may rewrite.
    pub fn is_image_file(&self) -> bool {
        matches!(self.source, Some(Source::Image { .. })) && self.found.clonezilla.is_none()
    }

    // --- what was found -----------------------------------------------------------------

    pub fn partitions(&self) -> &[PartitionInfo] {
        self.found.partitions.as_slice()
    }

    pub fn table(&self) -> Option<&PartitionTable> {
        self.found.table.as_ref()
    }

    pub fn last_error(&self) -> Option<&str> {
        self.found.error.as_deref()
    }

    pub fn format_label(&self) -> Option<&str> {
        self.found.format_label.as_deref()
    }

    pub fn chd_path(&self) -> Option<&Path> {
        self.found.chd_path.as_deref()
    }

    pub fn has_clonezilla(&self) -> bool {
        self.found.clonezilla.is_some()
    }

    pub fn has_backup_metadata(&self) -> bool {
        self.found.backup.is_some()
    }

    /// Changes whenever a probe widened a row's text.
    pub fn layout_generation(&self) -> u64 {
        self.layout_generation
    }

    pub fn volume_label(&self, index: usize) -> Option<&str> {
        self.found.volume_labels.get(&index).map(String::as_str)
    }

    pub fn table_summary(&self) -> Option<TableSummary> {
        let t = self.found.table.as_ref()?;
        Some(TableSummary {
            type_name: t.type_name().to_string(),
            byte_order: t.byte_order_name(),
            disk_signature: t.disk_signature(),
            superfloppy: matches!(t, PartitionTable::None { .. }),
            has_slots: t.has_native_slots(),
        })
    }

    /// The table's own slot name for a partition ("disk4s6"), or empty.
    pub fn slot(&self, part: &PartitionInfo) -> String {
        self.found
            .table
            .as_ref()
            .and_then(|t| t.native_slot(part))
            .map(|s| s.to_string())
            .unwrap_or_default()
    }

    pub fn alignment(&self) -> Option<AlignmentSummary> {
        let a = self.found.alignment.as_ref()?;
        Some(AlignmentSummary {
            kind: a.alignment_type.to_string(),
            first_lba: a.first_lba,
            heads: u32::from(a.heads),
            sectors_per_track: u32::from(a.sectors_per_track),
        })
    }

    pub fn backup_summary(&self) -> Option<BackupSummary> {
        let m = self.found.backup.as_ref()?;
        let data: u64 = m.partitions.iter().map(|p| p.imaged_size_bytes).sum();
        let min_restore = if data > 0 {
            data + m.alignment.first_partition_lba * 512
        } else {
            0
        };
        Some(BackupSummary {
            source: m.source_device.clone(),
            created: m.created.clone(),
            table: m.partition_table_type.clone(),
            compression: m.compression_type.clone(),
            checksum: m.checksum_type.clone(),
            sector_by_sector: m.sector_by_sector,
            min_restore_bytes: min_restore,
            source_size_bytes: m.source_size_bytes,
            alignment: m.alignment.detected_type.clone(),
        })
    }

    pub fn clonezilla_summary(&self) -> Option<ClonezillaSummary> {
        let cz = self.found.clonezilla.as_ref()?;
        Some(ClonezillaSummary {
            disk_name: cz.disk_name.clone(),
            source_size_bytes: cz.source_size_bytes,
            chs: (cz.heads > 0).then_some((cz.cylinders, cz.heads, cz.sectors_per_track)),
            created: cz.image_info.lines().next().map(str::to_string),
        })
    }

    /// The grid shows Min Size and Defrag Min columns.
    pub fn shows_min_columns(&self) -> bool {
        self.found.backup.is_some()
            || !self.found.min_sizes.is_empty()
            || !self.found.deferred.is_empty()
            || self.open.min_jobs_running()
    }

    /// The in-place trim point, from the backup's record or the computed minimum, when below the size.
    fn in_place_min(&self, part: &PartitionInfo) -> Option<u64> {
        self.found
            .backup
            .as_ref()
            .and_then(|m| m.partitions.iter().find(|pm| pm.index == part.index))
            .and_then(|pm| {
                // Older backups lack minimum_size_bytes; their imaged size is the trim point.
                pm.minimum_size_bytes.or({
                    if pm.imaged_size_bytes > 0 && pm.imaged_size_bytes < pm.original_size_bytes {
                        Some(pm.imaged_size_bytes)
                    } else {
                        None
                    }
                })
            })
            .filter(|&sz| sz > 0 && sz < part.size_bytes)
            .or_else(|| {
                self.found
                    .min_sizes
                    .get(&part.index)
                    .copied()
                    .filter(|&sz| sz < part.size_bytes)
            })
    }

    pub fn min_cell(&self, part: &PartitionInfo) -> MinCell {
        if part.is_extended_container {
            // A container's row sums its logical partitions' imaged sizes.
            let sum: u64 = self.found.backup.as_ref().map_or(0, |m| {
                m.partitions
                    .iter()
                    .filter(|pm| pm.index >= 4)
                    .map(|pm| pm.imaged_size_bytes)
                    .sum()
            });
            return if sum > 0 {
                MinCell::Size(sum)
            } else {
                MinCell::Empty
            };
        }
        if let Some(sz) = self.in_place_min(part) {
            MinCell::Size(sz)
        } else if let Some(&computed) = self.found.min_sizes.get(&part.index) {
            // Computed at the full size: the volume cannot shrink in place.
            MinCell::Size(computed)
        } else if let Some(phase) = self.open.min_phase(part.index) {
            MinCell::Pending(phase)
        } else if self.found.deferred.contains_key(&part.index) {
            MinCell::CanCalc
        } else {
            MinCell::Empty
        }
    }

    /// The defragmented minimum, when smaller than the in-place one.
    pub fn defrag_min(&self, part: &PartitionInfo) -> Option<u64> {
        if part.is_extended_container {
            return None;
        }
        let defrag = self
            .found
            .backup
            .as_ref()
            .and_then(|m| m.partitions.iter().find(|pm| pm.index == part.index))
            .and_then(|pm| pm.defragmented_min_size_bytes)
            .filter(|&sz| sz > 0 && sz < part.size_bytes)
            .or_else(|| {
                self.found
                    .defrag_min_sizes
                    .get(&part.index)
                    .copied()
                    .filter(|&sz| sz < part.size_bytes)
            });
        match (defrag, self.in_place_min(part)) {
            (Some(d), Some(m)) if d < m => Some(d),
            (Some(d), None) => Some(d),
            _ => None,
        }
    }

    /// The type byte a browse or check opens the partition with (a backup row may lack one).
    fn effective_type(part: &PartitionInfo) -> u8 {
        if part.partition_type_byte != 0 {
            part.partition_type_byte
        } else {
            crate::model::backup_loader::infer_fat_type_byte(part.type_name.as_str())
        }
    }

    pub fn row_actions(&self, part: &PartitionInfo) -> RowActions {
        let ts = part.partition_type_string.as_deref();
        let name = part.type_name.as_str();
        if part.is_extended_container
            || !crate::fs::partition_is_browsable(part.partition_type_byte, ts, name)
        {
            return RowActions::default();
        }
        let ptype = Self::effective_type(part);
        let image = self.is_image_file();
        RowActions {
            browse: true,
            check: crate::fs::is_checkable_type(ptype, ts)
                || crate::fs::is_checkable_fs_name(name)
                || crate::fs::is_superfloppy_hfs(part.partition_type_byte, name)
                || crate::fs::is_checkable_retro_fs(part.partition_type_byte, ts, name),
            expand_hfs: crate::fs::is_classic_hfs(part.partition_type_byte, ts, name)
                && !self.is_remote(),
            defragment: ts == Some("human68k") && image && !self.tools.repacking(),
            resize_alto: name == "Alto BFS" && image,
        }
    }

    /// Partition rows lock while a defragment rewrites the image under them.
    pub fn rows_locked(&self) -> bool {
        self.tools.repacking()
    }

    /// The real disk size: the backup's record, a CHD's logical size, else the file's length.
    pub fn disk_size(&mut self) -> Option<u64> {
        if let Some(cached) = self.found.disk_size {
            return cached;
        }
        let size = self.compute_disk_size();
        self.found.disk_size = Some(size);
        size
    }

    fn compute_disk_size(&self) -> Option<u64> {
        if let Some(m) = &self.found.backup {
            if m.source_size_bytes > 0 {
                return Some(m.source_size_bytes);
            }
        }
        if let Some(cz) = &self.found.clonezilla {
            if cz.source_size_bytes > 0 {
                return Some(cz.source_size_bytes);
            }
        }
        // A .chd's file size is its compressed size; the disk is the header's logical size.
        if let Some(chd) = &self.found.chd_path {
            if let Ok(reader) = crate::rbformats::chd::ChdReader::open(chd.as_path()) {
                let logical = reader.logical_size();
                if logical > 0 {
                    return Some(logical);
                }
            }
        }
        let len = std::fs::metadata(self.image_path()?).ok()?.len();
        (len > 0).then_some(len)
    }

    /// The disk-layout bar: each volume, then trailing free space of 1 MiB or more.
    pub fn layout(&mut self) -> Vec<LayoutSegment> {
        let disk_size = self.disk_size();
        let mut segments = Vec::new();
        let mut end = 0u64;
        let mut volumes = 0usize;
        for part in &self.found.partitions {
            if part.is_extended_container {
                continue;
            }
            end = end.max(
                part.start_lba
                    .saturating_mul(512)
                    .saturating_add(part.size_bytes),
            );
            let label = self
                .found
                .volume_labels
                .get(&part.index)
                .cloned()
                .or_else(|| part.drv_name.clone())
                .unwrap_or_else(|| format!("Partition {}", part.index + 1));
            let dimmed = matches!(
                part.partition_type_string.as_deref(),
                Some("Apple_partition_map") | Some("SGI_volhdr") | Some("SGI_volume"),
            );
            let kind = if dimmed {
                SegmentKind::Dimmed
            } else {
                volumes += 1;
                SegmentKind::Partition {
                    color_index: volumes - 1,
                }
            };
            segments.push(LayoutSegment {
                label,
                fs: part.type_name.clone(),
                size_bytes: part.size_bytes,
                kind,
                partition: Some(part.index),
            });
        }
        // Alignment and GPT backup headers leave a MiB or two that is not user-actionable space.
        if let Some(free) = disk_size.and_then(|d| d.checked_sub(end)) {
            if free >= 1024 * 1024 {
                segments.push(LayoutSegment {
                    label: String::new(),
                    fs: String::new(),
                    size_bytes: free,
                    kind: SegmentKind::Free,
                    partition: None,
                });
            }
        }
        segments
    }

    /// Unreadable sectors a flux image decoded as zeros.
    pub fn flux_faults(&self) -> Option<FaultReport> {
        dsk_flux::flux::recorded_faults(self.image_path()?)
    }

    /// Physical disks need elevation and this process has none: say so beside the picker.
    pub fn needs_elevation_note() -> bool {
        crate::os::PHYSICAL_DEVICES_ENABLED && !crate::os::is_elevated()
    }

    /// The loaded image is a floppy container the converter takes.
    pub fn source_is_floppy(&self) -> bool {
        self.open.floppy
    }

    /// The running inspect or backup load, for the shell's spinner.
    pub fn loading(&self) -> Option<Snapshot> {
        let preparing = self.prepare.as_ref().map(|(_, job)| job.snapshot());
        preparing.or_else(|| self.open.loading())
    }

    /// The label of a running job other tabs must wait for, if any.
    pub fn running_job(&self) -> Option<&'static str> {
        self.export.running().or_else(|| self.tools.running())
    }

    /// Block until the open and its probes end; for a headless shell's worker, never a UI thread.
    pub fn wait_opened(&mut self) {
        while self.prepare.is_some() {
            self.poll_prepare();
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        loop {
            self.poll_open();
            if self.open.loading().is_none() && !self.open.probing() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    // --- source changes -----------------------------------------------------------------

    fn gate(&mut self, gate: UnsavedGate, source: Option<InspectSource>) -> Vec<Effect> {
        if !self.browse_dirty {
            self.commit_gate(gate, source);
            return Vec::new();
        }
        self.pending_gate = Some((gate, source));
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::DISCARD_EDITS,
            title: "Discard unsaved edits?".into(),
            message: "The open volume has unapplied staged edits. Switching or closing the \
                      source will discard them."
                .into(),
            confirm_label: "Discard & continue".into(),
            destructive: true,
        })]
    }

    fn commit_gate(&mut self, gate: UnsavedGate, source: Option<InspectSource>) {
        match (gate, source) {
            (UnsavedGate::Open, Some(source)) => self.load(source),
            (UnsavedGate::Open, None) => {}
            (UnsavedGate::Close, _) => self.unload(false),
        }
    }

    /// Mac archives go to the Archives tab and change nothing here.
    fn is_mac_archive(path: &Path) -> bool {
        path.extension()
            .and_then(|e| e.to_str())
            .is_some_and(|ext| {
                crate::model::file_types::MAC_ARCHIVE_EXTS
                    .iter()
                    .any(|m| m.eq_ignore_ascii_case(ext))
            })
    }

    fn load(&mut self, source: InspectSource) {
        if let InspectSource::Image { path, .. } | InspectSource::ImageFile(path) = &source {
            if Self::is_mac_archive(path.as_path()) {
                self.requests.push(HostRequest::OpenArchive(path.clone()));
                return;
            }
        }
        self.prepare = None;
        if let InspectSource::ImageFile(path) = source {
            // Floppy containers stay as they are, so edits persist back into the container.
            let file = path.clone();
            let job = Job::spawn(
                "Preparing the image...",
                Arc::clone(&self.services.notify),
                move |job| {
                    job.set_can_cancel(false);
                    Ok(crate::model::image_prep::prepare_disk_image_path(
                        file.as_path(),
                        false,
                    )?)
                },
            );
            self.prepare = Some((path, job));
            return;
        }
        self.requests.push(HostRequest::CloseBrowse);
        self.reset();
        let source = match source {
            InspectSource::Device { slot, identity } => Source::Device { slot, identity },
            InspectSource::Image { path, temp } => Source::Image { path, _temp: temp },
            InspectSource::ImageFile(_) => return,
            InspectSource::Backup { folder } => Source::Backup { folder },
            #[cfg(feature = "remote")]
            InspectSource::Remote { conn } => Source::Remote { conn },
        };
        self.release_device();
        self.source = Some(source);
        self.open.floppy = self
            .image_path()
            .map(crate::model::inspect_runner::is_floppy_container_file)
            .unwrap_or(false);
        self.reinspect();
    }

    fn unload(&mut self, quiet: bool) {
        let was_backup = self.backup_folder().is_some();
        self.requests.push(HostRequest::CloseBrowse);
        self.reset();
        self.release_device();
        self.source = None;
        self.open.floppy = false;
        if was_backup {
            self.requests.push(HostRequest::BackupClosed);
        }
        if !quiet {
            self.info("Source closed.");
        }
    }

    /// Drop everything found in the source and every job reading it.
    fn reset(&mut self) {
        self.found = Found::default();
        self.open.restart();
        self.tools.reset();
        self.browse.reset();
        self.export.close();
    }

    /// Give up the elevated descriptor this controller opened, so the device can eject.
    fn release_device(&mut self) {
        self.found.device_file = None;
        self.found.device_guard = None;
        if let Some(path) = self.escalated.take() {
            crate::os::release_elevated_devices(Some(&*path.to_string_lossy()));
        }
    }

    fn picker(purpose: &'static str, title: &str, save: bool, file_name: String) -> PickSpec {
        PickSpec {
            purpose,
            title: title.to_string(),
            filters: Vec::new(),
            save,
            file_name,
        }
    }
}

/// The bytes Expand Image adds for `add_mib`, capped at [`EXPAND_IMAGE_MIB_MAX`].
pub fn expand_image_bytes(add_mib: u32) -> u64 {
    u64::from(add_mib.min(EXPAND_IMAGE_MIB_MAX)) * 1024 * 1024
}

impl Controller for InspectController {
    type Action = InspectAction;

    fn apply(&mut self, action: InspectAction) -> Vec<Effect> {
        match action {
            InspectAction::Open(source) => self.gate(UnsavedGate::Open, Some(source)),
            InspectAction::ChooseImage => vec![Effect::PickFile(Self::image_picker())],
            InspectAction::ChooseBackupFolder => {
                let title = "Open Backup Folder";
                let spec = Self::picker(purpose::OPEN_BACKUP, title, false, String::new());
                vec![Effect::PickFolder(spec)]
            }
            InspectAction::Close => self.gate(UnsavedGate::Close, None),
            InspectAction::Load(source) => {
                self.load(source);
                Vec::new()
            }
            InspectAction::Unload { quiet } => {
                self.unload(quiet);
                Vec::new()
            }
            InspectAction::Reinspect | InspectAction::DiskChanged => {
                self.reinspect();
                Vec::new()
            }
            InspectAction::SelectPartition(index) => {
                self.select_partition(index);
                Vec::new()
            }
            InspectAction::CalcMin(index) => {
                self.calc_min(index);
                Vec::new()
            }
            InspectAction::Browse(index) => {
                self.browse_partition(index);
                Vec::new()
            }
            InspectAction::Check(index) => {
                self.check(index);
                Vec::new()
            }
            InspectAction::Repair => self.ask_repair(),
            InspectAction::CloseCheck => {
                self.tools.close_check();
                Vec::new()
            }
            InspectAction::ShowCheckDebug(on) => {
                self.tools.show_debug(on);
                Vec::new()
            }
            InspectAction::Defragment(index) => self.ask_defragment(index),
            InspectAction::ExpandHfs(index) => {
                self.expand_hfs(index);
                Vec::new()
            }
            InspectAction::SwapWordOrder => self.ask_swab(),
            InspectAction::MakeBootable => {
                self.make_bootable();
                Vec::new()
            }
            InspectAction::ExpandImage(bytes) => {
                self.expand_image(bytes);
                Vec::new()
            }
            InspectAction::ChdInfo => {
                self.chd_info();
                Vec::new()
            }
            InspectAction::CloseChdInfo => {
                self.tools.close_chd_info();
                Vec::new()
            }
            InspectAction::EditTable => {
                self.edit_table();
                Vec::new()
            }
            InspectAction::AddPartition => {
                self.add_partition();
                Vec::new()
            }
            InspectAction::CloseEditor => {
                self.tools.close_editor();
                Vec::new()
            }
            InspectAction::Table(edit) => {
                self.table_edit(edit);
                Vec::new()
            }
            InspectAction::ApplyTableEdits => self.ask_apply_table(),
            InspectAction::OpenExport => {
                self.open_export();
                Vec::new()
            }
            InspectAction::CloseExport => {
                self.export.close();
                Vec::new()
            }
            InspectAction::StartExport(chd) => self.start_export(chd),
            InspectAction::CancelExport => {
                self.cancel_export();
                Vec::new()
            }
            InspectAction::BackupRemote => self.ask_remote_backup(),
            InspectAction::Picked { purpose, path } => self.picked(purpose, path),
            InspectAction::Confirmed { purpose, yes } => self.confirmed(purpose, yes),
        }
    }

    fn tick(&mut self) -> Vec<Effect> {
        let mut fx = Vec::new();
        self.poll_prepare();
        self.poll_open();
        fx.extend(self.poll_tools());
        self.poll_export();
        self.poll_browse();
        fx
    }
}

impl InspectController {
    /// An `ImageFile` was decoded (or could not be): open what there is.
    fn poll_prepare(&mut self) {
        let Some(done) = self
            .prepare
            .as_ref()
            .and_then(|(_, j)| crate::controller::job_outcome(j))
        else {
            return;
        };
        let (path, _) = self.prepare.take().expect("prepare job");
        let source = match done {
            Ok((path, temp)) => InspectSource::Image { path, temp },
            Err(e) => {
                self.warn(format!(
                    "Failed to materialize {}: {} - opening raw bytes",
                    path.display(),
                    e
                ));
                InspectSource::Image { path, temp: None }
            }
        };
        self.load(source);
    }

    /// An image file is being prepared to open.
    pub fn is_preparing(&self) -> bool {
        self.prepare.is_some()
    }

    fn picked(&mut self, purpose: &'static str, path: Option<PathBuf>) -> Vec<Effect> {
        match purpose {
            // A wrapper (`.adz`, ...) is decoded to a temporary file in the open's job.
            purpose::OPEN_IMAGE => {
                let source = path.map(InspectSource::ImageFile);
                return self.open_picked(source);
            }
            purpose::OPEN_BACKUP => {
                let source = path.map(|folder| InspectSource::Backup { folder });
                return self.open_picked(source);
            }
            purpose::BOOT_DONOR => self.boot_donor_picked(path),
            purpose::SWAB_DEST => self.swab_picked(path),
            purpose::EXPORT_DISK | purpose::EXPORT_FOLDER | purpose::EXPORT_HFV => {
                self.export_picked(purpose, path)
            }
            purpose::REMOTE_BACKUP => self.remote_backup_picked(path),
            _ => {}
        }
        Vec::new()
    }

    /// A source the user's picker chose; a cancel changes nothing.
    fn open_picked(&mut self, source: Option<InspectSource>) -> Vec<Effect> {
        match source {
            Some(source) => self.gate(UnsavedGate::Open, Some(source)),
            None => Vec::new(),
        }
    }

    /// Open File's picker: disk images, Mac archives, or anything.
    fn image_picker() -> PickSpec {
        let exts = |list: &[&str]| list.iter().map(|e| e.to_string()).collect();
        let mut spec = Self::picker(purpose::OPEN_IMAGE, "Open Disk Image", false, String::new());
        spec.filters = vec![
            (
                "Disk Images".to_string(),
                exts(crate::model::file_types::DISK_IMAGE_EXTS),
            ),
            (
                "Mac archives".to_string(),
                exts(crate::model::file_types::MAC_ARCHIVE_EXTS),
            ),
            ("All Files".to_string(), vec!["*".to_string()]),
        ];
        spec
    }

    fn confirmed(&mut self, purpose: &'static str, yes: bool) -> Vec<Effect> {
        match purpose {
            purpose::DISCARD_EDITS => {
                if let Some((gate, source)) = self.pending_gate.take() {
                    if yes {
                        self.commit_gate(gate, source);
                    }
                }
            }
            purpose::REPAIR if yes => self.repair(),
            purpose::DEFRAGMENT => self.defragment_answered(yes),
            purpose::APPLY_TABLE => self.apply_table_answered(yes),
            _ => {}
        }
        Vec::new()
    }
}
