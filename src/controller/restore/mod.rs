//! Restore: a backup folder (or one image) onto a disk, an image file or a remote drive.
//!
//! [`RestoreController`] owns the five modes of the Restore tab and everything they hold: the
//! backup folder and what loading it found (per-partition size rows, alignment, trailing free
//! space), the single-partition / new-disk source (a partition of the backup or an image file,
//! decoded first when wrapped), each mode's target (a device row, an image path, a drive or file
//! on an `rb-cli serve` daemon), the single-partition target scan, Write Image File's source, and
//! Build Disk's target and partition plan. Every slow step is a job: the backup load, the image
//! decode, the target scan, the decoded-size probe, the remote connect, the restore and the build.
//! A destructive start is a `Confirm` effect (Build Disk on a device asks for the device's name).
//! The sizing rules live in `model::restore_plan`; the layouts in `crate::restore`.
//!
//! The egui Restore tab (`src/gui/restore_tab.rs`) and the TUI Restore screen are shells over it:
//! they map widgets and keys to [`RestoreAction`]s, fulfil the [`Effect`]s, drain
//! [`RestoreController::take_log`] and carry out each [`HostRequest`]. See
//! docs/phase2_commander_plan.md § "Slice 13".

mod remote;
mod run;
mod source;
mod target;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::controller::log::LogLevel;
use crate::controller::{Controller, Effect, PickSpec};
use crate::device::DiskDevice;
use crate::model::job::{Job, Notify};
use crate::model::restore_plan::LoadedBackup;

// Plain data the views need, so they never name the model (CONTRIBUTING.md MVC).
pub use crate::model::disk_builder::DiskBuilder;
pub use crate::model::job::{JobRegistry, NoNotify, Snapshot};
pub use crate::model::restore_plan::{
    AlignmentChoice, ExpandPlan, LayoutFit, NewDiskChoice, PlanSegment, SizeRow,
};
pub use crate::model::size_mode::SizeMode;
pub use crate::restore::single::NewTableType;
pub use disk_core::display::format_size;

/// The `purpose` of each picker, confirmation and prompt this controller asks for.
pub mod purpose {
    pub const BACKUP_FOLDER: &str = "restore.backup_folder";
    pub const SOURCE_IMAGE: &str = "restore.source_image";
    pub const TARGET_IMAGE: &str = "restore.target_image";
    pub const NEW_DISK_IMAGE: &str = "restore.new_disk_image";
    pub const BUILD_IMAGE: &str = "restore.build_image";
    pub const WRITE_IMAGE: &str = "restore.write_image";
    pub const RESTORE: &str = "restore.confirm";
    pub const BUILD: &str = "restore.build_confirm";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const MODE: &str = "restore.mode";
    pub const BACKUP_FOLDER: &str = "restore.backup_folder";
    pub const SOURCE_IMAGE: &str = "restore.source_image";
    pub const SOURCE_PARTITION: &str = "restore.source_partition";
    pub const TARGET_DEVICE: &str = "restore.target_device";
    pub const TARGET_IMAGE: &str = "restore.target_image";
    pub const CLOSE_TARGET: &str = "restore.close_target";
    pub const REMOTE_TARGET: &str = "restore.remote_target";
    pub const REMOTE_CONNECT: &str = "restore.remote_connect";
    pub const REMOTE_USE_IMAGE: &str = "restore.remote_use_image";
    pub const EXPAND: &str = "restore.expand";
    pub const ALIGNMENT: &str = "restore.alignment";
    pub const SCAN_TARGET: &str = "restore.scan_target";
    pub const WRITE_IMAGE: &str = "restore.write_image";
    pub const WRITE_TARGET: &str = "restore.write_target";
    pub const BUILD_SIZE: &str = "restore.build_size";
    pub const BUILD_DEFINE: &str = "restore.build_define";
    pub const BUILD_CANCEL: &str = "restore.build_cancel";
    pub const START: &str = "restore.start";
    pub const CANCEL: &str = "restore.cancel";
}

/// Which restore the tab is set up for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreMode {
    /// The whole backup onto a disk or image.
    FullDisk,
    /// One partition onto an existing partition of a device.
    SinglePartition,
    /// One partition onto a blank disk under a fresh table.
    NewDisk,
    /// A plain image file onto a device or one of its partitions.
    WriteImage,
    /// A fresh table on a blank disk, partitions optionally filled.
    BuildDisk,
}

/// The target each device-picking mode holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetSlot {
    FullDisk,
    SinglePartition,
    NewDisk,
    BuildDisk,
}

/// User intents.
pub enum RestoreAction {
    SetMode(RestoreMode),
    /// "Browse..." beside the backup folder.
    PickBackupFolder,
    /// A backup folder (or `.cbk`) from a picker, another tab or the recent list.
    OpenBackup(PathBuf),
    /// "Browse..." beside the single-partition / new-disk source image.
    PickSourceImage,
    /// An image file as the single-partition / new-disk source; a wrapped image is decoded first.
    OpenSourceImage(PathBuf),
    /// A dropped or recent path: a folder restores in full, a file is a partition image.
    OpenDropped(PathBuf),
    /// Forget the backup (the top bar's Close Backup).
    ClearBackup,
    SetSourcePartition(Option<usize>),
    /// A row of the device list as a mode's target.
    SelectDevice {
        slot: TargetSlot,
        row: usize,
        device: DiskDevice,
    },
    /// Close Drive: clear a mode's target device.
    ClearDevice(TargetSlot),
    /// A mode's "Write to device" / "Save as image file" choice.
    SetTargetIsDevice(TargetSlot, bool),
    /// "Save As..." for a mode's image target.
    PickTargetImage(TargetSlot),
    SetTargetImage(TargetSlot, Option<PathBuf>),
    SetAlignment(AlignmentChoice),
    SetCustomAlignment(u32),
    SetPartitionSize {
        index: usize,
        mode: SizeMode,
        custom_mib: u32,
    },
    SetExpand(bool),
    SetExpandMib(u32),
    SetExtendLast(bool),
    /// Read the single-partition target's table.
    ScanTarget,
    SetTargetPartition(usize),
    SetNewDisk(NewDiskChoice),
    /// "Choose..." for Write Image File's source.
    PickWriteImage,
    OpenWriteImage(PathBuf),
    /// "Choose target and write...": hands the image to the shell's disk writer.
    WriteImageTarget,
    SetBuildSize(String),
    /// "Define Partitions...": open the partition plan.
    BuildDefine,
    BuildClose,
    /// The plan's Apply: an image target builds at once, a device asks for its name first.
    BuildApply,
    BuildCancel,
    /// The Restore button: asks for confirmation.
    Start,
    /// Cancel the running restore.
    Cancel,
    /// "Restore to a remote machine".
    SetRemote(bool),
    RemoteSetAddress(String),
    RemoteConnect,
    /// A drive the daemon listed, by its path and size.
    RemotePickDevice {
        path: String,
        size: u64,
    },
    RemoteSetImageName(String),
    RemoteUseImage,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Confirmed {
        purpose: &'static str,
        yes: bool,
    },
    Answered {
        purpose: &'static str,
        text: Option<String>,
    },
}

/// What the host must do; drained by [`RestoreController::take_requests`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostRequest {
    /// A backup folder was opened: put it at the top of the Restore recent list.
    RememberRecent(PathBuf),
    /// Open the shell's disk writer for this image and its decoded size.
    WriteImage { path: PathBuf, size: u64 },
}

/// A partition of the loaded backup, for the source-partition picker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePartition {
    pub index: usize,
    /// "#index: type (size)".
    pub label: String,
}

/// A partition found on the single-partition target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPartition {
    pub index: usize,
    /// 1-based position, matching `inspect`'s `#`.
    pub position: usize,
    pub type_name: String,
    pub size_bytes: u64,
    /// Whether the source fits, once the source size is known.
    pub fits: Option<bool>,
}

/// A drive a remote daemon listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDrive {
    pub path: String,
    pub size_bytes: u64,
    /// "path (size) - media [removable]".
    pub label: String,
}

/// The shared progress bar's content while a restore runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreBar {
    pub operation: String,
    pub current_bytes: u64,
    pub total_bytes: u64,
}

/// Jobs and the shell's wake handle.
pub struct RestoreServices {
    pub jobs: JobRegistry,
    pub notify: Arc<dyn Notify>,
}

/// Lines for the log panel, shared with running jobs.
type LogSink = Arc<Mutex<Vec<(LogLevel, String)>>>;

pub(crate) fn push_log(sink: &LogSink, level: LogLevel, msg: impl Into<String>) {
    crate::model::worker::lock_status(&**sink).push((level, msg.into()));
}

/// A device picked from the list: its row and what was there.
#[derive(Clone)]
struct Picked {
    row: usize,
    device: DiskDevice,
}

/// A mode's target: a device, or an image file.
struct Target {
    is_device: bool,
    device: Option<Picked>,
    image: Option<PathBuf>,
}

impl Target {
    fn new(is_device: bool) -> Self {
        Target {
            is_device,
            device: None,
            image: None,
        }
    }
}

pub struct RestoreController {
    services: RestoreServices,
    mode: RestoreMode,
    log: LogSink,
    requests: Vec<HostRequest>,
    src: source::SourceState,
    targets: target::TargetState,
    remote: remote::RemoteState,
    run: run::RunState,
}

impl RestoreController {
    pub fn new(services: RestoreServices) -> Self {
        RestoreController {
            services,
            mode: RestoreMode::FullDisk,
            log: Arc::new(Mutex::new(Vec::new())),
            requests: Vec::new(),
            src: source::SourceState::default(),
            targets: target::TargetState::default(),
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

    pub fn mode(&self) -> RestoreMode {
        self.mode
    }

    fn picker(
        purpose: &'static str,
        title: &str,
        filters: Vec<(String, Vec<String>)>,
        save: bool,
    ) -> PickSpec {
        PickSpec {
            purpose,
            title: title.to_string(),
            filters,
            save,
            file_name: String::new(),
        }
    }

    fn ext(list: &[&str]) -> Vec<String> {
        list.iter().map(|e| e.to_string()).collect()
    }

    /// The disk-image filters of the source pickers.
    fn image_filters(archives: bool) -> Vec<(String, Vec<String>)> {
        let mut f = vec![(
            "Disk Images".to_string(),
            Self::ext(crate::model::file_types::DISK_IMAGE_EXTS),
        )];
        if archives {
            f.push((
                "Mac archives".to_string(),
                Self::ext(crate::model::file_types::MAC_ARCHIVE_EXTS),
            ));
            f.push(("All Files".to_string(), vec!["*".to_string()]));
        }
        f
    }

    // --- running ------------------------------------------------------------------------

    /// A restore or a disk build is in flight.
    pub fn is_running(&self) -> bool {
        self.run.running() || self.run.building()
    }

    /// A restore (not a build) is in flight: the Restore button becomes Cancel.
    pub fn restore_running(&self) -> bool {
        self.run.running()
    }

    /// The shared progress bar's content, while a restore runs.
    pub fn progress(&self) -> Option<RestoreBar> {
        self.run.bar()
    }

    /// How the last restore ended, until the next one starts.
    pub fn last_result(&self) -> Option<&Result<(), String>> {
        self.run.last.as_ref()
    }

    /// The disk build in progress or finished, for its inline bar.
    pub fn build_progress(&self) -> Option<Snapshot> {
        self.run.build_snapshot()
    }

    /// How the last disk build ended.
    pub fn build_result(&self) -> Option<&Result<(), String>> {
        self.run.build_last.as_ref()
    }

    /// Why the Restore button is disabled, or `Ok` (each mode's own rule).
    pub fn validate(&self) -> Result<(), String> {
        match self.mode {
            RestoreMode::FullDisk => {
                if !self.has_backup() {
                    return Err("Choose a backup folder.".into());
                }
                self.full_disk_target_ready()?;
            }
            RestoreMode::SinglePartition | RestoreMode::NewDisk => {
                let has_source = (self.src.metadata().is_some()
                    && self.src.source_partition.is_some())
                    || self.src.has_image();
                if !has_source {
                    return Err("Choose a source partition or image.".into());
                }
                let has_target = if self.mode == RestoreMode::SinglePartition {
                    self.targets.single.device.is_some() && self.targets.sp_part.is_some()
                } else {
                    let t = &self.targets.new_disk;
                    if t.is_device {
                        t.device.is_some()
                    } else {
                        t.image.is_some()
                    }
                };
                if !has_target {
                    return Err("Choose a target.".into());
                }
            }
            RestoreMode::WriteImage | RestoreMode::BuildDisk => {
                return Err("This mode starts from its own window.".into());
            }
        }
        Ok(())
    }

    /// The Restore button is enabled.
    pub fn can_start(&self) -> bool {
        !self.run.running() && self.validate().is_ok()
    }
}

impl Controller for RestoreController {
    type Action = RestoreAction;

    fn apply(&mut self, action: RestoreAction) -> Vec<Effect> {
        match action {
            RestoreAction::SetMode(mode) => self.mode = mode,
            RestoreAction::PickBackupFolder => {
                return vec![Effect::PickFolder(Self::picker(
                    purpose::BACKUP_FOLDER,
                    "Choose the backup folder",
                    Vec::new(),
                    false,
                ))];
            }
            RestoreAction::OpenBackup(path) => self.open_backup(path),
            RestoreAction::PickSourceImage => {
                return vec![Effect::PickFile(Self::picker(
                    purpose::SOURCE_IMAGE,
                    "Open Image File",
                    Self::image_filters(true),
                    false,
                ))];
            }
            RestoreAction::OpenSourceImage(path) => self.open_source_image(path),
            RestoreAction::OpenDropped(path) => self.open_dropped(path),
            RestoreAction::ClearBackup => self.clear_backup(),
            RestoreAction::SetSourcePartition(idx) => self.src.source_partition = idx,
            RestoreAction::SelectDevice { slot, row, device } => {
                self.select_device(slot, row, device)
            }
            RestoreAction::ClearDevice(slot) => self.clear_device(slot),
            RestoreAction::SetTargetIsDevice(slot, on) => self.set_target_is_device(slot, on),
            RestoreAction::PickTargetImage(slot) => {
                let purpose = match slot {
                    TargetSlot::NewDisk => purpose::NEW_DISK_IMAGE,
                    TargetSlot::BuildDisk => purpose::BUILD_IMAGE,
                    TargetSlot::FullDisk | TargetSlot::SinglePartition => purpose::TARGET_IMAGE,
                };
                return vec![Effect::PickFile(Self::picker(
                    purpose,
                    "Save the restored image as",
                    vec![("Disk Images".to_string(), Self::ext(&["img", "raw", "bin"]))],
                    true,
                ))];
            }
            RestoreAction::SetTargetImage(slot, path) => self.targets.target_mut(slot).image = path,
            RestoreAction::SetAlignment(a) => self.targets.alignment = a,
            RestoreAction::SetCustomAlignment(n) => self.targets.custom_alignment = n,
            RestoreAction::SetPartitionSize {
                index,
                mode,
                custom_mib,
            } => self.src.set_size(index, mode, custom_mib),
            RestoreAction::SetExpand(on) => self.targets.expand.enabled = on,
            RestoreAction::SetExpandMib(n) => self.targets.expand.free_mib = n,
            RestoreAction::SetExtendLast(on) => self.targets.expand.extend_last = on,
            RestoreAction::ScanTarget => self.scan_target(),
            RestoreAction::SetTargetPartition(idx) => self.targets.sp_part = Some(idx),
            RestoreAction::SetNewDisk(choice) => self.targets.new_disk_choice = choice,
            RestoreAction::PickWriteImage => {
                return vec![Effect::PickFile(Self::picker(
                    purpose::WRITE_IMAGE,
                    "Choose the image to write",
                    Self::image_filters(false),
                    false,
                ))];
            }
            RestoreAction::OpenWriteImage(path) => self.open_write_image(path),
            RestoreAction::WriteImageTarget => self.write_image_target(),
            RestoreAction::SetBuildSize(text) => self.targets.build_size_text = text,
            RestoreAction::BuildDefine => self.build_define(),
            RestoreAction::BuildClose => self.targets.builder = None,
            RestoreAction::BuildApply => return self.build_apply(),
            RestoreAction::BuildCancel => self.run.cancel_build(),
            RestoreAction::Start => return self.start(),
            RestoreAction::Cancel => self.cancel(),
            RestoreAction::SetRemote(on) => self.remote.active = on,
            RestoreAction::RemoteSetAddress(addr) => self.remote.address = addr,
            RestoreAction::RemoteConnect => self.remote_connect(),
            RestoreAction::RemotePickDevice { path, size } => self.remote_pick_device(path, size),
            RestoreAction::RemoteSetImageName(name) => self.remote.image_name = name,
            RestoreAction::RemoteUseImage => self.remote_use_image(),
            RestoreAction::Picked { purpose, path } => self.picked(purpose, path),
            RestoreAction::Confirmed { purpose, yes } => {
                if yes && purpose == purpose::RESTORE {
                    self.start_confirmed();
                }
            }
            RestoreAction::Answered { purpose, text } => {
                if let (purpose::BUILD, Some(text)) = (purpose, text) {
                    return self.build_answered(text.as_str());
                }
            }
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll_load();
        self.poll_decode();
        self.poll_write_probe();
        self.poll_scan();
        self.poll_remote();
        self.poll_run();
        Vec::new()
    }
}

impl RestoreController {
    fn picked(&mut self, purpose: &'static str, path: Option<PathBuf>) {
        let Some(path) = path else {
            return;
        };
        match purpose {
            purpose::BACKUP_FOLDER => self.open_backup(path),
            purpose::SOURCE_IMAGE => self.open_source_image(path),
            purpose::TARGET_IMAGE => self.targets.full.image = Some(path),
            purpose::NEW_DISK_IMAGE => self.targets.new_disk.image = Some(path),
            purpose::BUILD_IMAGE => self.targets.build.image = Some(path),
            purpose::WRITE_IMAGE => self.open_write_image(path),
            _ => {}
        }
    }

    /// The source a single-partition or new-disk restore reads, as the model's plan input.
    fn partition_source(&self) -> Option<crate::model::restore_plan::PartitionSource<'_>> {
        use crate::model::restore_plan::PartitionSource;
        if let Some(path) = self.src.image_path() {
            return Some(PartitionSource::Image(path));
        }
        let folder = self.src.folder.as_deref()?;
        Some(PartitionSource::Backup {
            folder,
            metadata: self.src.metadata(),
            index: self.src.source_partition?,
        })
    }

    fn loaded(&self) -> Option<&LoadedBackup> {
        self.src.loaded.as_ref()
    }

    /// Block until the load, decode, probes, scan and connect end; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        loop {
            self.tick();
            if !self.src.busy() && !self.targets.scanning() && !self.remote.busy() {
                self.tick();
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Block until the running restore or build ends; for a headless shell or a test.
    pub fn wait_finished(&mut self) {
        loop {
            self.tick();
            if !self.is_running() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

/// The path a mode's image target would be written to, for the confirmation's wording.
fn path_label(p: Option<&Path>) -> String {
    p.map(|p| p.display().to_string())
        .unwrap_or_else(|| "Unknown".into())
}
