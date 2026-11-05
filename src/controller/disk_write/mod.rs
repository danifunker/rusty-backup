//! Writing one image to a disk: the Physical Disk Export window and Write Image File.
//!
//! [`DiskWriteController`] owns the source handed over by another screen (Inspect's export, the
//! Restore tab's Write Image File, the TUI's image source), the target (a device row or, for the
//! TUI, an image file), the partitions read off a device target, the optional partition table
//! synthesized around a superfloppy, and the write. The source probe, the target scan and the
//! write are jobs. A write to a device is an `AskText` whose validator wants the device's name;
//! a write to an image file is a `Confirm`. The rules live in `model::disk_write_plan`, the write
//! in `model::physical_write_runner`.
//!
//! Shells: the egui window (`src/gui/physical_disk_export.rs`), hosted by the Inspect and Restore
//! tabs, and the TUI Restore screen's image source (headless). See
//! docs/phase2_commander_plan.md § "Slice 14".

mod run;
mod target;

#[cfg(test)]
mod tests;

use std::sync::{Arc, Mutex};

use crate::controller::log::LogLevel;
use crate::controller::{Controller, Effect};
use crate::device::DiskDevice;
use crate::model::job::{Job, Notify, Status};

// Plain data the views need, so they never name the model (CONTRIBUTING.md MVC).
pub use crate::model::disk_write_plan::{
    TableKind, WrapAlign, WrapForm, WriteOpen, WriteSource, TYPE_BYTE_PRESETS,
};
pub use crate::model::job::{JobRegistry, NoNotify, Progress, Snapshot};
pub use crate::model::size_mode::SizeMode;
pub use disk_core::display::format_size;

/// The `purpose` of each confirmation and prompt this controller asks for.
pub mod purpose {
    /// The typed device name before a device write.
    pub const DEVICE_NAME: &str = "disk_write.device_name";
    /// Overwriting an image file.
    pub const OVERWRITE_FILE: &str = "disk_write.overwrite_file";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const TARGET_DEVICE: &str = "disk_write.target_device";
    pub const TARGET_REGION: &str = "disk_write.target_region";
    pub const SYNTHESIZE: &str = "disk_write.synthesize";
    pub const TABLE_KIND: &str = "disk_write.table_kind";
    pub const ALIGNMENT: &str = "disk_write.alignment";
    pub const TYPE_BYTE: &str = "disk_write.type_byte";
    pub const PARTITION_SIZE: &str = "disk_write.partition_size";
    pub const WRITE: &str = "disk_write.write";
    pub const CANCEL_WRITE: &str = "disk_write.cancel_write";
    pub const CLOSE: &str = "disk_write.close";
}

/// User intents.
pub enum DiskWriteAction {
    /// Another screen hands over its image; the window opens.
    Open(WriteOpen),
    /// Close the window (refused while a write runs).
    Close,
    /// A row of the device list as the target.
    SelectDevice {
        row: usize,
        device: DiskDevice,
    },
    /// An image file as the target (the TUI's "Save as image file").
    SetTargetImage(std::path::PathBuf),
    /// `None` writes the whole target; `Some(index)` writes into that partition.
    SetTargetPartition(Option<usize>),
    /// Step the choice: whole disk, each partition, then the whole disk again.
    CycleTargetPartition,
    SetSynthesize(bool),
    SetTableKind(TableKind),
    SetAlignment(WrapAlign),
    SetCustomAlignment(u64),
    SetTypeByte(u8),
    SetSize {
        mode: SizeMode,
        custom_mib: u32,
    },
    /// "Write to disk": asks first.
    Write,
    /// Cancel the running write.
    Cancel,
    Confirmed {
        purpose: &'static str,
        yes: bool,
    },
    Answered {
        purpose: &'static str,
        text: Option<String>,
    },
}

/// A partition found on the device target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetPartition {
    pub index: usize,
    /// 1-based position, matching `inspect`'s `#`.
    pub position: usize,
    pub type_name: String,
    /// "Partition N - type (size) at LBA n", N from the table slot.
    pub label: String,
    pub size_bytes: u64,
}

/// Jobs and the shell's wake handle.
pub struct DiskWriteServices {
    pub jobs: JobRegistry,
    pub notify: Arc<dyn Notify>,
}

/// Lines for the log panel, shared with running jobs.
type LogSink = Arc<Mutex<Vec<(LogLevel, String)>>>;

fn push_log(sink: &LogSink, level: LogLevel, msg: impl Into<String>) {
    crate::model::worker::lock_status(&**sink).push((level, msg.into()));
}

fn finished<R>(job: &Job<R>) -> Option<Result<R, String>> {
    match job.snapshot().status {
        Status::Running => None,
        Status::Done => Some(job.take_result().ok_or_else(|| "no result".to_string())),
        Status::Failed(e) => Some(Err(e)),
        Status::Cancelled => Some(Err("cancelled".to_string())),
    }
}

pub struct DiskWriteController {
    services: DiskWriteServices,
    log: LogSink,
    /// The window is up (a source was handed over).
    open: bool,
    /// Measuring the source and seeding the form.
    opening: Option<Job<(WriteSource, WrapForm)>>,
    source: Option<WriteSource>,
    form: WrapForm,
    error: Option<String>,
    target: target::TargetState,
    run: run::RunState,
}

impl DiskWriteController {
    pub fn new(services: DiskWriteServices) -> Self {
        DiskWriteController {
            services,
            log: Arc::new(Mutex::new(Vec::new())),
            open: false,
            opening: None,
            source: None,
            form: WrapForm::default(),
            error: None,
            target: target::TargetState::default(),
            run: run::RunState::default(),
        }
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

    /// The window is up.
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// The source is still being measured.
    pub fn opening(&self) -> Option<Snapshot> {
        self.opening.as_ref().map(|j| j.snapshot())
    }

    pub fn source(&self) -> Option<&WriteSource> {
        self.source.as_ref()
    }

    pub fn form(&self) -> &WrapForm {
        &self.form
    }

    /// The synthesize group applies (the source has no table and the whole disk is written).
    pub fn can_synthesize(&self) -> bool {
        self.source.as_ref().is_some_and(|s| !s.has_partition_table)
    }

    /// Why the last Write was refused, until the next one.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// The size modes the synthesized partition offers.
    pub fn size_choices(&self) -> [SizeMode; 3] {
        WrapForm::size_choices()
    }

    /// The synthesized partition's Custom range in MiB; Write refuses a size below it.
    pub fn custom_bounds_mib(&self) -> (u32, u32) {
        WrapForm::custom_bounds_mib(self.source.as_ref().map_or(0, |s| s.size_bytes))
    }

    /// A switch into Custom starts at the source's size, as the GUI's size cell did.
    fn set_size(&mut self, mode: SizeMode, custom_mib: u32) {
        let seed = mode == SizeMode::Custom && self.form.size_mode != SizeMode::Custom;
        self.form.size_mode = mode;
        self.form.custom_size_mib = if seed {
            self.custom_bounds_mib().0
        } else {
            custom_mib
        };
    }

    fn open_source(&mut self, open: WriteOpen) {
        if self.run.running() {
            return;
        }
        if let Some(job) = self.opening.take() {
            job.cancel();
        }
        // A new source starts with no target: the last one may name another disk by now.
        self.target = target::TargetState::default();
        self.run = run::RunState::default();
        self.source = None;
        self.form = WrapForm::default();
        self.error = None;
        self.open = true;
        self.opening = Some(self.spawn("Reading the source...", move |_| {
            Ok(crate::model::disk_write_plan::prepare(open))
        }));
    }

    fn poll_open(&mut self) {
        let Some(done) = self.opening.as_ref().and_then(finished) else {
            return;
        };
        self.opening = None;
        match done {
            Ok((source, form)) => {
                self.source = Some(source);
                self.form = form;
            }
            Err(e) => {
                push_log(
                    &self.log,
                    LogLevel::Error,
                    format!("Cannot open the source: {e}"),
                );
                self.open = false;
            }
        }
    }

    fn close(&mut self) {
        if self.run.running() {
            return;
        }
        if let Some(job) = self.opening.take() {
            job.cancel();
        }
        self.target.cancel_scan();
        self.open = false;
    }

    /// Block until the source probe and the target scan end; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        loop {
            self.tick();
            if self.opening.is_none() && !self.target.scanning() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Block until the running write ends; for a headless shell or a test.
    pub fn wait_finished(&mut self) {
        loop {
            self.tick();
            if !self.run.running() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

impl Controller for DiskWriteController {
    type Action = DiskWriteAction;

    fn apply(&mut self, action: DiskWriteAction) -> Vec<Effect> {
        // The form is locked while the write runs.
        let edits_form = !matches!(
            action,
            DiskWriteAction::Open(_)
                | DiskWriteAction::Close
                | DiskWriteAction::Cancel
                | DiskWriteAction::Confirmed { .. }
                | DiskWriteAction::Answered { .. }
        );
        if edits_form && self.run.running() {
            return Vec::new();
        }
        match action {
            DiskWriteAction::Open(open) => self.open_source(open),
            DiskWriteAction::Close => self.close(),
            DiskWriteAction::Cancel => self.run.cancel(),
            DiskWriteAction::Confirmed { purpose, yes } => {
                if yes && purpose == purpose::OVERWRITE_FILE {
                    self.start_write();
                }
            }
            DiskWriteAction::Answered { purpose, text } => {
                if let (purpose::DEVICE_NAME, Some(text)) = (purpose, text) {
                    self.device_name_answered(text.as_str());
                }
            }
            DiskWriteAction::SelectDevice { row, device } => self.select_device(row, device),
            DiskWriteAction::SetTargetImage(path) => self.set_target_image(path),
            DiskWriteAction::SetTargetPartition(idx) => self.target.partition = idx,
            DiskWriteAction::CycleTargetPartition => self.cycle_target_partition(),
            DiskWriteAction::SetSynthesize(on) => self.form.synthesize = on,
            DiskWriteAction::SetTableKind(k) => self.form.table_kind = k,
            DiskWriteAction::SetAlignment(a) => self.form.alignment = a,
            DiskWriteAction::SetCustomAlignment(n) => self.form.custom_alignment_sectors = n.max(1),
            DiskWriteAction::SetTypeByte(b) => self.form.type_byte = b,
            DiskWriteAction::SetSize { mode, custom_mib } => self.set_size(mode, custom_mib),
            DiskWriteAction::Write => return self.write(),
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll_open();
        self.poll_scan();
        self.poll_run();
        Vec::new()
    }
}
