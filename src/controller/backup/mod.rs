//! Backup: a disk, image or remote drive into a backup folder.
//!
//! [`BackupController`] owns the source (a device row, an image file being decoded or ready, a
//! drive on an `rb-cli serve` daemon), what the partition scan and the minimum-size walks found,
//! the per-partition selection and Compact toggles, the options form, the VHD options window, and
//! every job: the image decode, the scan, Calc min, the remote connect and load, the backup and
//! the whole-disk VHD export. The sizing rules live in `model::backup_plan`.
//!
//! The egui Backup tab (`src/gui/backup_tab.rs`) and the TUI Backup screen are shells over it:
//! they map widgets and keys to [`BackupAction`]s, fulfil the [`Effect`]s (the image and
//! destination pickers), drain [`BackupController::take_log`] and carry out each
//! [`HostRequest`]. See docs/phase2_commander_plan.md § "Slice 12".

mod remote;
mod run;
mod source;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::controller::log::LogLevel;
use crate::controller::{Controller, Effect, PickSpec};
use crate::device::DiskDevice;
use crate::model::backup_plan::{BackupOptions, MinSizes};
use crate::model::job::{Job, Notify, Snapshot};
use crate::partition::PartitionInfo;

// Plain data the views need, so they never name the model (CONTRIBUTING.md MVC).
pub use crate::backup::{ChecksumType, CompressionType};
pub use crate::model::job::{JobRegistry, NoNotify};
pub use crate::model::size_mode::SizeMode;
pub use crate::rbformats::chd_options::{ChdOptions, ChdProfile};
pub use disk_core::display::format_size;

/// The `purpose` of each picker this controller asks for.
pub mod purpose {
    pub const SOURCE_IMAGE: &str = "backup.source_image";
    pub const DESTINATION: &str = "backup.destination";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const SOURCE: &str = "backup.source";
    pub const LOAD_PARTITIONS: &str = "backup.load_partitions";
    pub const CLOSE_SOURCE: &str = "backup.close_source";
    pub const CLEAR_REMOTE: &str = "backup.clear_remote";
    pub const SELECT_ALL: &str = "backup.select_all";
    pub const SELECT_NONE: &str = "backup.select_none";
    pub const DESTINATION: &str = "backup.destination";
    pub const NAME: &str = "backup.name";
    pub const START: &str = "backup.start";
    pub const CANCEL: &str = "backup.cancel";
    pub const VHD_START: &str = "backup.vhd_start";
    pub const VHD_CANCEL: &str = "backup.vhd_cancel";
    pub const REMOTE_CONNECT: &str = "backup.remote_connect";
    pub const REMOTE_CLOSE: &str = "backup.remote_close";
}

/// The CHD codec and hunk choice the shell's options control holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChdChoice {
    pub options: ChdOptions,
    /// Hand-tuned (Custom mode): remembered in config.json for the next launch.
    pub custom: bool,
}

/// User intents.
pub enum BackupAction {
    /// A row of the device list.
    SelectDevice {
        slot: usize,
        device: DiskDevice,
    },
    /// An image file; `rename` refreshes the backup name once it is ready (a picker or recent pick).
    OpenImage {
        path: PathBuf,
        rename: bool,
    },
    /// "Open Image File...": asks for the file.
    PickImage,
    /// Close Drive: release the device or image and forget its partitions.
    CloseSource,
    /// "Load partition info" on a device (may prompt for credentials).
    ScanPartitions,
    CancelScan,
    SelectAll,
    SelectNone,
    SetPartition(usize, bool),
    /// Compute the deferred minimum size of the partition with this index.
    CalcMin(usize),
    SetCompact(usize, bool),
    /// "Browse..." beside the destination.
    PickDestination,
    SetDestination(Option<PathBuf>),
    SetName(String),
    SetFormat(CompressionType),
    SetChecksum(ChecksumType),
    SetSectorBySector(bool),
    SetResize(bool),
    SetKeepSwap(bool),
    SetSplit(bool),
    SetSplitMib(u32),
    /// Start Backup, with the CHD options the shell's control holds (`None` for chdman's defaults).
    Start(Option<ChdChoice>),
    /// Stop waiting for minimum sizes before starting (the walks run on).
    CancelPendingStart,
    /// Cancel the running backup or VHD export.
    Cancel,
    /// The VHD window's layout: one disk image, or one .vhd per partition.
    VhdWholeDisk(bool),
    VhdSize {
        index: usize,
        mode: SizeMode,
        custom_mib: u32,
    },
    VhdStart,
    VhdClose,
    /// "Remote Drive...": open the connect window.
    RemoteOpen,
    RemoteSetAddress(String),
    RemoteConnect,
    /// A drive the daemon listed, by its path and size.
    RemotePick {
        path: String,
        size: u64,
    },
    RemoteClose,
    /// Disconnect and drop the remote source.
    RemoteClear,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
}

/// What the host must do; drained by [`BackupController::take_requests`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostRequest {
    /// An image was opened: put it at the top of the Backup recent list.
    RememberRecent(PathBuf),
}

/// The Min Size cell of a partition row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MinCell {
    Size(u64),
    /// A walk runs; its last phase.
    Pending(String),
    /// The minimum needs a filesystem walk: offer "Calc min".
    CanCalc,
    Empty,
}

/// The Compact Space cell of a partition row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactCell {
    /// Not a layout-preserving filesystem: nothing to show.
    Hidden,
    /// Shown off, with "not yet implemented" on hover.
    NotImplemented,
    Toggle(bool),
}

/// One row of the partition grid.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionRow {
    pub index: usize,
    /// 1-based position, matching `inspect`'s `#` and `--partitions N`.
    pub position: usize,
    pub type_name: String,
    pub size_bytes: u64,
    pub selected: bool,
    pub min: MinCell,
    pub fragmentation: Option<f32>,
    pub compact: CompactCell,
}

/// A row of the VHD options window.
#[derive(Debug, Clone, PartialEq)]
pub struct VhdRow {
    pub index: usize,
    pub type_name: String,
    pub start_lba: u64,
    pub original_size: u64,
    pub minimum_size: u64,
    pub mode: SizeMode,
    pub custom_mib: u32,
    /// The filesystem whose minimum still needs a walk ("Calc min").
    pub deferred_fs: Option<&'static str>,
}

impl VhdRow {
    pub fn effective_size(&self) -> u64 {
        self.mode
            .effective_size(self.original_size, self.minimum_size, self.custom_mib)
    }
}

/// The shared progress bar's content while a backup or VHD export runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupBar {
    pub operation: String,
    pub current_bytes: u64,
    pub total_bytes: u64,
    /// The untrimmed size; larger than `total_bytes` while compaction saves space.
    pub full_size_bytes: u64,
}

/// A drive a remote daemon listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDrive {
    pub path: String,
    pub size_bytes: u64,
    /// "path (size) - media [removable]".
    pub label: String,
}

/// Jobs and the shell's wake handle.
pub struct BackupServices {
    pub jobs: JobRegistry,
    pub notify: Arc<dyn Notify>,
}

/// Lines for the log panel, shared with running jobs.
type LogSink = Arc<Mutex<Vec<(LogLevel, String)>>>;

pub(crate) fn push_log(sink: &LogSink, level: LogLevel, msg: impl Into<String>) {
    crate::model::worker::lock_status(&**sink).push((level, msg.into()));
}

/// The local source.
enum Source {
    Device {
        slot: usize,
        device: DiskDevice,
    },
    Image {
        path: PathBuf,
        _temp: Option<tempfile::TempDir>,
    },
}

/// What the scan of the current source found; dropped whenever the source changes.
#[derive(Default)]
struct Found {
    partitions: Vec<PartitionInfo>,
    selected: HashSet<usize>,
    mins: MinSizes,
    deferred: HashMap<usize, &'static str>,
    table_desc: Option<String>,
    load_error: Option<String>,
    /// The scanned source, kept open so the minimum-size walks can clone it.
    source_file: Option<Arc<std::fs::File>>,
}

pub struct BackupController {
    services: BackupServices,
    source: Option<Source>,
    found: Found,
    opts: BackupOptions,
    /// Custom CHD options to remember once a backup starts.
    chd_custom: bool,
    destination: Option<PathBuf>,
    name: String,
    log: LogSink,
    requests: Vec<HostRequest>,
    /// The device path the scan escalated, so closing releases exactly that one.
    escalated: Option<PathBuf>,
    src: source::SourceState,
    remote: remote::RemoteState,
    run: run::RunState,
}

impl BackupController {
    pub fn new(services: BackupServices) -> Self {
        BackupController {
            services,
            source: None,
            found: Found::default(),
            opts: BackupOptions::default(),
            chd_custom: false,
            destination: None,
            name: String::new(),
            log: Arc::new(Mutex::new(Vec::new())),
            requests: Vec::new(),
            escalated: None,
            src: source::SourceState::default(),
            remote: remote::RemoteState::default(),
            run: run::RunState::default(),
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

    // --- the source ---------------------------------------------------------------------

    pub fn image_path(&self) -> Option<&Path> {
        match &self.source {
            Some(Source::Image { path, .. }) => Some(path.as_path()),
            _ => None,
        }
    }

    /// The device list row the source was picked from.
    pub fn device_slot(&self) -> Option<usize> {
        match &self.source {
            Some(Source::Device { slot, .. }) => Some(*slot),
            _ => None,
        }
    }

    /// The path the backup reads: the image, or the device node.
    pub fn source_path(&self) -> Option<PathBuf> {
        match &self.source {
            Some(Source::Device { device, .. }) => Some(device.path.clone()),
            Some(Source::Image { path, .. }) => Some(path.clone()),
            None => None,
        }
    }

    /// A device or image is selected (a remote drive is not counted).
    pub fn has_local_source(&self) -> bool {
        self.source.is_some()
    }

    pub fn has_source(&self) -> bool {
        self.source.is_some() || self.remote_active()
    }

    /// The image's folder, the TUI's default destination.
    pub fn default_destination(&self) -> Option<PathBuf> {
        let parent = self.image_path()?.parent()?;
        (!parent.as_os_str().is_empty()).then(|| parent.to_path_buf())
    }

    /// "Refresh Devices" replaced the list: a different disk (or none) may now sit at our row.
    pub fn reconcile_devices(&mut self, devices: &[DiskDevice]) {
        if self.run.running() || self.remote_active() {
            return;
        }
        let Some(Source::Device { slot, device }) = &self.source else {
            return;
        };
        let slot = *slot;
        let now = devices.get(slot);
        if now.map(DiskDevice::identity) == Some(device.identity()) {
            return;
        }
        match now {
            None => {
                self.warn("The selected source device is no longer listed; clearing it.");
                self.source = None;
                self.source_changed();
            }
            Some(dev) => {
                self.source = Some(Source::Device {
                    slot,
                    device: dev.clone(),
                });
                self.source_changed();
            }
        }
    }

    // --- what was found -----------------------------------------------------------------

    pub fn has_partitions(&self) -> bool {
        !self.found.partitions.is_empty()
    }

    pub fn partition_count(&self) -> usize {
        self.found.partitions.len()
    }

    pub fn table_desc(&self) -> Option<&str> {
        self.found.table_desc.as_deref()
    }

    pub fn load_error(&self) -> Option<&str> {
        self.found.load_error.as_deref()
    }

    pub fn selected_count(&self) -> usize {
        self.found.selected.len()
    }

    pub fn rows(&self) -> Vec<PartitionRow> {
        let mins = &self.found.mins;
        self.found
            .partitions
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.is_extended_container)
            .map(|(pos, p)| {
                let ts = p.partition_type_string.as_deref();
                let gate = p.gate_type_byte();
                let min = match mins.effective(p.index, gate, ts) {
                    Some(m) if m < p.size_bytes => MinCell::Size(m),
                    _ => match self.src.min_phase(p.index) {
                        Some(phase) => MinCell::Pending(phase),
                        None if self.found.deferred.contains_key(&p.index) => MinCell::CanCalc,
                        None => MinCell::Empty,
                    },
                };
                let compact = if !crate::model::backup_plan::shows_compact(mins, p) {
                    CompactCell::Hidden
                } else if mins.compact_available(p.index, gate, ts) {
                    CompactCell::Toggle(mins.compact_enabled(p.index))
                } else {
                    CompactCell::NotImplemented
                };
                PartitionRow {
                    index: p.index,
                    position: pos + 1,
                    type_name: p.type_name.clone(),
                    size_bytes: p.size_bytes,
                    selected: self.found.selected.contains(&p.index),
                    min,
                    fragmentation: mins.fragmentation.get(&p.index).copied(),
                    compact,
                }
            })
            .collect()
    }

    // --- options ------------------------------------------------------------------------

    pub fn options(&self) -> &BackupOptions {
        &self.opts
    }

    pub fn destination(&self) -> Option<&Path> {
        self.destination.as_deref()
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// The CHD options remembered from the last Custom backup, laid over `base`.
    pub fn saved_chd_custom(base: ChdOptions) -> ChdOptions {
        run::saved_chd_custom(base)
    }

    // --- running ------------------------------------------------------------------------

    /// A backup or whole-disk VHD export is in flight.
    pub fn is_running(&self) -> bool {
        self.run.running()
    }

    /// The shared progress bar's content, while a backup or VHD export runs.
    pub fn progress(&self) -> Option<BackupBar> {
        self.run.bar()
    }

    /// How the last backup or export ended, until the next one starts.
    pub fn last_result(&self) -> Option<&Result<(), String>> {
        self.run.last.as_ref()
    }

    /// Start Backup waits for minimum sizes (or the scan) before it starts.
    pub fn start_pending(&self) -> bool {
        self.run.pending_start
    }

    /// The image decode in progress, for the shell's bar.
    pub fn decoding(&self) -> Option<Snapshot> {
        self.src.decoding()
    }

    /// The partition scan or image preview in progress.
    pub fn scanning(&self) -> Option<Snapshot> {
        self.src.scanning()
    }

    /// Minimum-size walks run, so the shell keeps redrawing.
    pub fn min_jobs_running(&self) -> bool {
        self.src.min_running()
    }

    /// The "Load partition info" button: shown for a device, labelled by whether it was read.
    pub fn can_scan_device(&self) -> bool {
        !self.run.running() && self.device_slot().is_some()
    }

    pub fn device_scanned(&self) -> bool {
        self.has_partitions() || self.found.load_error.is_some()
    }

    /// Start Backup is enabled (the shell adds its own checks, such as valid CHD options).
    pub fn can_start(&self) -> bool {
        !self.run.running()
            && !self.run.pending_start
            && self.src.idle()
            && self.validate().is_ok()
    }

    /// Why Start Backup cannot run now, or `Ok`.
    pub fn validate(&self) -> Result<(), String> {
        if !self.has_source() {
            return Err("Select a source first.".into());
        }
        if let Some(path) = self.image_path() {
            if !path.exists() {
                return Err("Source not found.".into());
            }
        }
        if self.destination.is_none() {
            return Err("Choose a destination folder.".into());
        }
        if self.name.trim().is_empty() {
            return Err("Enter a backup name.".into());
        }
        if self.has_partitions() && self.found.selected.is_empty() {
            return Err("Select at least one partition.".into());
        }
        Ok(())
    }

    // --- the VHD window -----------------------------------------------------------------

    pub fn vhd_open(&self) -> bool {
        self.run.vhd.is_some()
    }

    pub fn vhd_whole_disk(&self) -> bool {
        self.run.vhd_whole_disk
    }

    pub fn vhd_rows(&self) -> &[VhdRow] {
        self.run.vhd.as_deref().unwrap_or(&[])
    }

    /// A Calc min walk's last phase, for a VHD row.
    pub fn min_phase(&self, index: usize) -> Option<String> {
        self.src.min_phase(index)
    }

    fn picker(purpose: &'static str, title: &str, filters: Vec<(String, Vec<String>)>) -> PickSpec {
        PickSpec {
            purpose,
            title: title.to_string(),
            filters,
            save: false,
            file_name: String::new(),
        }
    }

    fn set_option(&mut self, f: impl FnOnce(&mut BackupOptions)) {
        f(&mut self.opts);
        // Sector-by-sector keeps every byte, so resizing would defeat it.
        if self.opts.sector_by_sector {
            self.opts.resize = false;
        }
    }
}

impl Controller for BackupController {
    type Action = BackupAction;

    fn apply(&mut self, action: BackupAction) -> Vec<Effect> {
        match action {
            BackupAction::SelectDevice { slot, device } => self.select_device(slot, device),
            BackupAction::OpenImage { path, rename } => self.open_image(path, rename),
            BackupAction::PickImage => {
                let ext = |list: &[&str]| list.iter().map(|e| e.to_string()).collect();
                return vec![Effect::PickFile(Self::picker(
                    purpose::SOURCE_IMAGE,
                    "Open Image File",
                    vec![
                        (
                            "Disk Images".into(),
                            ext(crate::model::file_types::DISK_IMAGE_EXTS),
                        ),
                        (
                            "Mac archives".into(),
                            ext(crate::model::file_types::MAC_ARCHIVE_EXTS),
                        ),
                        ("All Files".into(), vec!["*".to_string()]),
                    ],
                ))];
            }
            BackupAction::CloseSource => self.close_source(),
            BackupAction::ScanPartitions => self.scan_partitions(),
            BackupAction::CancelScan => self.src.cancel_scan(),
            BackupAction::SelectAll => self.select_all(),
            BackupAction::SelectNone => self.found.selected.clear(),
            BackupAction::SetPartition(index, on) => {
                if on {
                    self.found.selected.insert(index);
                } else {
                    self.found.selected.remove(&index);
                }
            }
            BackupAction::CalcMin(index) => self.calc_min(index),
            BackupAction::SetCompact(index, on) => self.found.mins.set_compact(index, on),
            BackupAction::PickDestination => {
                return vec![Effect::PickFolder(Self::picker(
                    purpose::DESTINATION,
                    "Choose the destination folder",
                    Vec::new(),
                ))];
            }
            BackupAction::SetDestination(path) => self.destination = path,
            BackupAction::SetName(name) => self.name = name,
            BackupAction::SetFormat(c) => self.set_option(|o| o.compression = c),
            BackupAction::SetChecksum(c) => self.set_option(|o| o.checksum = c),
            BackupAction::SetSectorBySector(on) => self.set_option(|o| o.sector_by_sector = on),
            BackupAction::SetResize(on) => self.set_option(|o| o.resize = on),
            BackupAction::SetKeepSwap(on) => self.set_option(|o| o.keep_swap = on),
            BackupAction::SetSplit(on) => self.set_option(|o| o.split = on),
            BackupAction::SetSplitMib(mib) => self.set_option(|o| o.split_mib = mib),
            BackupAction::Start(chd) => self.start(chd),
            BackupAction::CancelPendingStart => self.cancel_pending_start(),
            BackupAction::Cancel => self.cancel(),
            BackupAction::VhdWholeDisk(on) => self.run.vhd_whole_disk = on,
            BackupAction::VhdSize {
                index,
                mode,
                custom_mib,
            } => self.run.set_vhd_size(index, mode, custom_mib),
            BackupAction::VhdStart => self.vhd_start(),
            BackupAction::VhdClose => self.run.vhd = None,
            BackupAction::RemoteOpen => self.remote_open(),
            BackupAction::RemoteSetAddress(addr) => self.remote.address = addr,
            BackupAction::RemoteConnect => self.remote_connect(),
            BackupAction::RemotePick { path, size } => self.remote_pick(path, size),
            BackupAction::RemoteClose => self.remote.dialog = false,
            BackupAction::RemoteClear => self.remote_clear(),
            BackupAction::Picked { purpose, path } => self.picked(purpose, path),
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll_decode();
        self.poll_scan();
        self.poll_remote();
        self.poll_min();
        self.poll_run();
        self.poll_pending_start();
        Vec::new()
    }
}

impl BackupController {
    fn picked(&mut self, purpose: &'static str, path: Option<PathBuf>) {
        let Some(path) = path else {
            return;
        };
        match purpose {
            purpose::SOURCE_IMAGE => self.open_image(path, true),
            purpose::DESTINATION => self.destination = Some(path),
            _ => {}
        }
    }

    /// Block until the decode, scan and minimum walks end; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        loop {
            self.tick();
            if self.src.idle() && !self.src.min_running() && !self.remote.busy() {
                self.tick();
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Block until the running backup or export ends; for a headless shell or a test.
    pub fn wait_finished(&mut self) {
        loop {
            self.tick();
            if !self.run.running() && !self.run.pending_start {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
