//! Build Disk: make a new disk, partitioned or a single bare volume, as an image file or onto a
//! physical disk (an SD / CF card, a USB stick, any disk the device list shows).
//!
//! [`BuildDiskController`] is a wizard. Its steps are controller state ([`Step`],
//! [`BuildDiskController::steps`], [`BuildDiskController::next_blocked`]), so the GUI tab and the
//! TUI screen walk the same steps and one scenario test drives both. The plan lives in
//! `model::disk_builder::DiskBuilder` (rows, layout, validation), the formatting in
//! `model::format`, and the build is one job over `model::provision_runner`. The CLI's `rb-cli
//! build` fills the same request without the steps. See docs/build_disk_plan.md § 7.

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::controller::log::LogLevel;
use crate::controller::partition_edit::{self, BuildAction, BuildView};
use crate::controller::{job_outcome, Controller, Effect, PickSpec};
use crate::model::disk_builder::{DiskBuilder, RowFormat};
use crate::model::job::{Job, Notify, Snapshot};

pub use crate::device::DiskDevice;
pub use crate::model::format::{FormatOptions, FsCaps, FsKind, FsOption};
pub use crate::partition::type_catalog::TableKind;
pub use disk_core::display::format_size;

/// A label the user typed on `old`, kept when switching to `fs` if it fits there.
fn typed_label(old: &RowFormat, fs: FsKind) -> Option<String> {
    let label = old.opts.label.as_str();
    let max = fs.caps().label_max;
    let typed = label != RowFormat::new(old.kind).opts.label;
    (typed && max > 0 && label.len() <= max).then(|| label.to_string())
}

/// The CP/M disk formats a CP/M volume can take, by preset name.
pub fn cpm_presets() -> Vec<&'static str> {
    fs_cpm::cpm_diskdefs::ALL_PRESETS
        .iter()
        .map(|d| d.name)
        .collect()
}

/// A blank (auto) or sized field in the `4K` / `64M` grammar.
pub fn parse_size_field(text: &str) -> Result<Option<u64>, String> {
    let t = text.trim();
    if t.is_empty() {
        return Ok(None);
    }
    crate::partition::parse_size(t)
        .map(Some)
        .map_err(|e| format!("{e:#}"))
}

/// The `purpose` of each picker this controller asks for.
pub mod purpose {
    pub const OUTPUT_IMAGE: &str = "build_disk.output_image";
    pub const ROW_SOURCE: &str = "build_disk.row_source";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const STEP: &str = "build_disk.step";
    pub const KIND: &str = "build_disk.kind";
    pub const OUTPUT_DEVICE: &str = "build_disk.output_device";
    pub const OUTPUT_IMAGE: &str = "build_disk.output_image";
    pub const SIZE: &str = "build_disk.size";
    pub const TABLE: &str = "build_disk.table";
    pub const VOLUME_FS: &str = "build_disk.volume_fs";
    pub const ROW_CONTENT: &str = "build_disk.row_content";
    pub const LABEL: &str = "build_disk.label";
    pub const CONFIRM: &str = "build_disk.confirm";
    pub const BACK: &str = "build_disk.back";
    pub const NEXT: &str = "build_disk.next";
    pub const CREATE: &str = "build_disk.create";
    pub const CANCEL: &str = "build_disk.cancel";
    pub const BUILD_ANOTHER: &str = "build_disk.build_another";
}

/// The wizard's steps, in the order they are walked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Partitioned disk or a single volume.
    Kind,
    /// Image file or physical disk; a disk fixes the size.
    Output,
    /// Partitioned: table, size, alignment. Volume: filesystem and size.
    Disk,
    /// Rows: size, type, name, content.
    Partitions,
    /// Label and filesystem options for every formatted row or the volume.
    Options,
    /// Everything chosen, the warnings, and for a device its typed path.
    Review,
    /// The job's progress and how it ended.
    Build,
}

impl Step {
    pub fn label(self) -> &'static str {
        match self {
            Step::Kind => "Kind",
            Step::Output => "Output",
            Step::Disk => "Disk",
            Step::Partitions => "Partitions",
            Step::Options => "Options",
            Step::Review => "Review",
            Step::Build => "Build",
        }
    }
}

/// What is being built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskKind {
    /// A partition table with rows.
    Partitioned,
    /// One filesystem filling the whole disk, no table (a superfloppy or a floppy).
    Volume,
}

impl DiskKind {
    pub const ALL: [DiskKind; 2] = [DiskKind::Partitioned, DiskKind::Volume];

    pub fn label(self) -> &'static str {
        match self {
            DiskKind::Partitioned => "Partitioned disk",
            DiskKind::Volume => "Single volume (no partition table)",
        }
    }
}

/// What a row holds after the build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowContent {
    Empty,
    /// Formatted blank with a filesystem.
    Format,
    /// A source image poured in.
    Image,
}

/// User intents.
#[derive(Debug, Clone)]
pub enum BuildDiskAction {
    Next,
    Back,
    /// Jump back to a step already passed.
    GoTo(Step),
    SetKind(DiskKind),
    SetOutputIsDevice(bool),
    SelectDevice {
        row: usize,
        device: DiskDevice,
    },
    ClearDevice,
    /// "Save As..." for the image file.
    PickOutputImage,
    /// The image-file size (or a volume's size), in the `20M` / `1G` grammar.
    SetSize(String),
    /// An edit of the table and its rows (table type, alignment, geometry, row size / type /
    /// name / source, add, remove, move).
    Table(BuildAction),
    /// A row formatted with `fs`, or `None` for empty.
    SetRowFs {
        row: usize,
        fs: Option<FsKind>,
    },
    SetRowOptions {
        row: usize,
        opts: FormatOptions,
    },
    SetVolumeFs(FsKind),
    SetVolumeOptions(FormatOptions),
    SetConfirmText(String),
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Create,
    Cancel,
    /// Back to the start, keeping the choices.
    BuildAnother,
}

/// One formatted target (a row or the volume), for the Options step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatTarget {
    /// The row index, or `None` for the volume.
    pub row: Option<usize>,
    pub title: String,
    pub fs: FsKind,
    pub caps: FsCaps,
    pub opts: FormatOptions,
    /// Why its label or options are refused.
    pub error: Option<String>,
}

pub struct BuildDiskController {
    notify: Arc<dyn Notify>,
    log: Arc<Mutex<Vec<(LogLevel, String)>>>,
    step: Step,
    kind: DiskKind,
    builder: DiskBuilder,
    volume: RowFormat,
    size_text: String,
    output_is_device: bool,
    device: Option<(usize, DiskDevice)>,
    image: Option<PathBuf>,
    confirm_text: String,
    source_row: Option<usize>,
    probe: Option<(usize, Job<(PathBuf, u64)>)>,
    job: Option<Job<Result<(), String>>>,
    outcome: Option<Result<(), String>>,
    finished: Option<Snapshot>,
}

impl BuildDiskController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        let mut c = BuildDiskController {
            notify,
            log: Arc::new(Mutex::new(Vec::new())),
            step: Step::Kind,
            kind: DiskKind::Partitioned,
            builder: DiskBuilder::new(TableKind::Mbr, 0),
            volume: RowFormat::new(FsKind::Fat),
            size_text: "512M".to_string(),
            output_is_device: false,
            device: None,
            image: None,
            confirm_text: String::new(),
            source_row: None,
            probe: None,
            job: None,
            outcome: None,
            finished: None,
        };
        c.sync_size();
        c
    }

    /// Lines for the log panel since the last call.
    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        self.log
            .lock()
            .map(|mut l| std::mem::take(&mut *l))
            .unwrap_or_default()
    }

    fn push(&self, level: LogLevel, msg: impl Into<String>) {
        if let Ok(mut l) = self.log.lock() {
            l.push((level, msg.into()));
        }
    }

    // --- reading -------------------------------------------------------------------------

    pub fn step(&self) -> Step {
        self.step
    }

    /// The steps for the current kind, in order.
    pub fn steps(&self) -> Vec<Step> {
        let mut v = vec![Step::Kind, Step::Output, Step::Disk];
        if self.kind == DiskKind::Partitioned {
            v.push(Step::Partitions);
            if self.builder.rows.iter().any(|r| r.format.is_some()) {
                v.push(Step::Options);
            }
        } else {
            v.push(Step::Options);
        }
        v.push(Step::Review);
        v.push(Step::Build);
        v
    }

    pub fn kind(&self) -> DiskKind {
        self.kind
    }

    pub fn output_is_device(&self) -> bool {
        self.output_is_device
    }

    pub fn device(&self) -> Option<&DiskDevice> {
        self.device.as_ref().map(|(_, d)| d)
    }

    pub fn device_row(&self) -> Option<usize> {
        self.device.as_ref().map(|(r, _)| *r)
    }

    pub fn image(&self) -> Option<&Path> {
        self.image.as_deref()
    }

    pub fn size_text(&self) -> &str {
        self.size_text.as_str()
    }

    /// The size of the disk or volume being built, or why it is unknown.
    pub fn disk_size(&self) -> Result<u64, String> {
        if self.output_is_device {
            return match &self.device {
                Some((_, d)) => Ok(d.size_bytes),
                None => Err("Choose a device first.".to_string()),
            };
        }
        if self.kind == DiskKind::Volume && self.volume.kind.caps().fixed_size {
            return self.fixed_volume_size();
        }
        match crate::partition::parse_size(self.size_text.as_str()) {
            Ok(0) => Err("The size must be more than zero.".to_string()),
            Ok(n) => Ok(n),
            Err(e) => Err(format!("{e:#}")),
        }
    }

    /// A fixed-geometry volume's own size (CP/M's comes from its preset).
    fn fixed_volume_size(&self) -> Result<u64, String> {
        let caps = self.volume.kind.caps();
        if caps.min_size > 0 {
            return Ok(caps.min_size);
        }
        let mut cur = std::io::Cursor::new(Vec::new());
        crate::model::format::format_into(&mut cur, 0, self.volume.kind, &self.volume.opts)
            .map(|d| d.len)
            .map_err(|e| format!("{e:#}"))
    }

    /// The table and rows, as the partition editor draws them.
    pub fn table_view(&self) -> BuildView {
        partition_edit::build_view(&self.builder)
    }

    /// Each row's content choice.
    pub fn row_content(&self, row: usize) -> RowContent {
        match self.builder.rows.get(row) {
            Some(r) if r.source.is_some() => RowContent::Image,
            Some(r) if r.format.is_some() => RowContent::Format,
            _ => RowContent::Empty,
        }
    }

    /// The filesystem a row is formatted with.
    pub fn row_fs(&self, row: usize) -> Option<FsKind> {
        self.builder
            .rows
            .get(row)
            .and_then(|r| r.format.as_ref())
            .map(|f| f.kind)
    }

    pub fn volume_fs(&self) -> FsKind {
        self.volume.kind
    }

    /// The filesystems a volume or row can be formatted with, in a stable order.
    pub fn filesystems(&self) -> &'static [FsKind] {
        &FsKind::ALL
    }

    /// The source image of a row is being measured.
    pub fn probing(&self) -> bool {
        self.probe.is_some()
    }

    /// Every formatted row (or the volume), with its options, for the Options step.
    pub fn format_targets(&self) -> Vec<FormatTarget> {
        let target = |row: Option<usize>, title: String, f: &RowFormat| FormatTarget {
            row,
            title,
            fs: f.kind,
            caps: f.kind.caps(),
            opts: f.opts.clone(),
            error: f.check().err(),
        };
        match self.kind {
            DiskKind::Volume => vec![target(
                None,
                format!("Volume ({})", self.volume.kind.display_name()),
                &self.volume,
            )],
            DiskKind::Partitioned => self
                .builder
                .rows
                .iter()
                .enumerate()
                .filter_map(|(i, r)| {
                    r.format.as_ref().map(|f| {
                        target(
                            Some(i),
                            format!("Partition {} ({})", i + 1, f.kind.display_name()),
                            f,
                        )
                    })
                })
                .collect(),
        }
    }

    pub fn confirm_text(&self) -> &str {
        self.confirm_text.as_str()
    }

    /// What a device build asks the user to type.
    pub fn confirm_expected(&self) -> Option<String> {
        if !self.output_is_device {
            return None;
        }
        self.device().map(|d| d.path.display().to_string())
    }

    pub fn is_running(&self) -> bool {
        self.job.is_some()
    }

    /// The build in progress, or the last one once it ended.
    pub fn progress(&self) -> Option<Snapshot> {
        self.job
            .as_ref()
            .map(|j| j.snapshot())
            .or_else(|| self.finished.clone())
    }

    pub fn outcome(&self) -> Option<&Result<(), String>> {
        self.outcome.as_ref()
    }

    /// The image file the last successful build wrote, for "Open in Inspect".
    pub fn built_image(&self) -> Option<&Path> {
        match (&self.outcome, self.output_is_device) {
            (Some(Ok(())), false) => self.image.as_deref(),
            _ => None,
        }
    }

    /// Why Next (or Create on Review) is refused on `step`, or `None`.
    pub fn next_blocked(&self) -> Option<String> {
        self.blocked_at(self.step)
    }

    fn blocked_at(&self, step: Step) -> Option<String> {
        match step {
            Step::Kind => None,
            Step::Output => self.output_blocked(),
            Step::Disk => self.disk_blocked(),
            Step::Partitions => self.partitions_blocked(),
            Step::Options => self
                .format_targets()
                .into_iter()
                .find_map(|t| t.error.map(|e| format!("{}: {e}", t.title))),
            Step::Review => self.review_blocked(),
            Step::Build => Some("The build is running.".to_string()),
        }
    }

    fn output_blocked(&self) -> Option<String> {
        if !self.output_is_device {
            return match self.image {
                Some(_) => None,
                None => Some("Choose where to save the image file.".to_string()),
            };
        }
        match &self.device {
            None => Some("Choose a device.".to_string()),
            Some((_, d)) if d.is_system => {
                Some("That is the system disk; choose another device.".to_string())
            }
            Some((_, d)) if d.is_read_only => Some("That device is read-only.".to_string()),
            Some((_, d)) if d.size_bytes == 0 => {
                Some("That device reports no size; refresh the device list.".to_string())
            }
            Some(_) => None,
        }
    }

    fn disk_blocked(&self) -> Option<String> {
        let size = match self.disk_size() {
            Ok(s) => s,
            Err(e) => return Some(e),
        };
        match self.kind {
            DiskKind::Partitioned => None,
            DiskKind::Volume => self.volume.fits(size).err(),
        }
    }

    fn partitions_blocked(&self) -> Option<String> {
        let mut b = self.builder.clone();
        b.validate();
        b.errors.into_iter().find(|e| !e.starts_with("Warning:"))
    }

    fn review_blocked(&self) -> Option<String> {
        if self.job.is_some() {
            return Some("A build is running.".to_string());
        }
        for step in self.steps() {
            if matches!(step, Step::Review | Step::Build) {
                break;
            }
            if let Some(e) = self.blocked_at(step) {
                return Some(format!("{}: {e}", step.label()));
            }
        }
        match self.confirm_expected() {
            Some(want) if self.confirm_text != want => {
                Some(format!("Type the device path `{want}` to confirm."))
            }
            _ => None,
        }
    }

    /// The Review step's summary lines.
    pub fn review_lines(&self) -> Vec<String> {
        let mut v = Vec::new();
        let size = self.disk_size().map(format_size).unwrap_or_default();
        v.push(match (&self.device, self.output_is_device, &self.image) {
            (Some((_, d)), true, _) => {
                format!("Write to {} ({size}), erasing it", d.display_name())
            }
            (_, false, Some(p)) => format!("Save as {} ({size})", p.display()),
            _ => "No output chosen".to_string(),
        });
        match self.kind {
            DiskKind::Volume => v.push(format!(
                "One {} volume, label {:?}",
                self.volume.kind.display_name(),
                self.volume.opts.label
            )),
            DiskKind::Partitioned => {
                v.push(format!("{} partition table", self.builder.kind.label()));
                if let Ok(placed) = self.builder.plan() {
                    for (i, (row, p)) in self.builder.rows.iter().zip(placed.iter()).enumerate() {
                        let content = match (&row.source, &row.format) {
                            (Some(path), _) => format!("from {}", path.display()),
                            (None, Some(f)) => {
                                format!("{} {:?}", f.kind.display_name(), f.opts.label)
                            }
                            (None, None) => "empty".to_string(),
                        };
                        v.push(format!(
                            "Partition {}: {} at LBA {}, type {}, {content}",
                            i + 1,
                            format_size(p.size_bytes),
                            p.start_lba,
                            p.type_text,
                        ));
                    }
                }
            }
        }
        v
    }

    /// Warnings shown on Review that do not block the build.
    pub fn warnings(&self) -> Vec<String> {
        let mut v = Vec::new();
        if self.kind == DiskKind::Partitioned {
            let mut b = self.builder.clone();
            b.validate();
            v.extend(b.errors.into_iter().filter(|e| e.starts_with("Warning:")));
        }
        if self.output_is_device
            && (self.kind == DiskKind::Volume
                || self.builder.rows.iter().any(|r| r.format.is_some()))
        {
            v.push(
                "Info: each formatted partition is written in full, so old data on the device \
                 cannot show through."
                    .to_string(),
            );
        }
        v
    }

    // --- acting --------------------------------------------------------------------------

    fn editable(&self) -> bool {
        self.job.is_none()
    }

    /// The disk size the plan lays out on follows the output.
    fn sync_size(&mut self) {
        self.builder.disk_size = self.disk_size().unwrap_or(0);
    }

    fn go(&mut self, forward: bool) {
        let steps = self.steps();
        let Some(at) = steps.iter().position(|s| *s == self.step) else {
            self.step = Step::Kind;
            return;
        };
        let next = if forward {
            at + 1
        } else {
            at.saturating_sub(1)
        };
        if let Some(s) = steps.get(next) {
            if *s != Step::Build {
                self.step = *s;
            }
        }
    }

    fn pick_output(&self) -> Vec<Effect> {
        let name = match self.kind {
            DiskKind::Volume => format!("{}.img", self.volume.kind.id()),
            DiskKind::Partitioned => "disk.img".to_string(),
        };
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::OUTPUT_IMAGE,
            title: "Save the new disk image as".to_string(),
            filters: vec![
                ("Raw disk images".to_string(), vec!["img".to_string()]),
                ("All Files".to_string(), vec!["*".to_string()]),
            ],
            save: true,
            file_name: name,
        })]
    }

    fn table_edit(&mut self, action: BuildAction) -> Vec<Effect> {
        if let BuildAction::PickRowSource(row) = action {
            if row >= self.builder.rows.len() || self.probe.is_some() {
                return Vec::new();
            }
            self.source_row = Some(row);
            let mut exts: Vec<String> = crate::model::file_types::DISK_IMAGE_EXTS
                .iter()
                .map(|e| e.to_string())
                .collect();
            exts.dedup();
            return vec![Effect::PickFile(PickSpec {
                purpose: purpose::ROW_SOURCE,
                title: format!("Source image for partition {}", row + 1),
                filters: vec![
                    ("Disk images".to_string(), exts),
                    ("All Files".to_string(), vec!["*".to_string()]),
                ],
                save: false,
                file_name: String::new(),
            })];
        }
        if let BuildAction::SetKind(kind) = action {
            self.builder.set_kind(kind);
            return Vec::new();
        }
        partition_edit::apply_build(&mut self.builder, action);
        Vec::new()
    }

    fn picked(&mut self, which: &'static str, path: Option<PathBuf>) {
        let Some(path) = path else {
            self.source_row = None;
            return;
        };
        match which {
            purpose::OUTPUT_IMAGE => self.image = Some(path),
            purpose::ROW_SOURCE => {
                let Some(row) = self.source_row.take() else {
                    return;
                };
                let job = Job::spawn(
                    "Measuring the source image...",
                    Arc::clone(&self.notify),
                    move |_| {
                        let size = crate::model::source_reader::decoded_image_size(path.as_path())
                            .unwrap_or(0);
                        Ok((path, size))
                    },
                );
                self.probe = Some((row, job));
            }
            _ => {}
        }
    }

    fn set_row_fs(&mut self, row: usize, fs: Option<FsKind>) {
        let kind = self.builder.kind;
        let size = self
            .builder
            .plan()
            .ok()
            .and_then(|p| p.get(row).map(|p| p.size_bytes))
            .unwrap_or(0);
        let Some(r) = self.builder.rows.get_mut(row) else {
            return;
        };
        match fs {
            None => r.format = None,
            Some(fs) => {
                r.source = None;
                r.source_size = 0;
                let mut f = RowFormat::new(fs);
                if let Some(label) = r.format.as_ref().and_then(|old| typed_label(old, fs)) {
                    f.opts.label = label;
                }
                // A type still at the table's default follows the filesystem.
                if r.type_text.trim().is_empty()
                    || r.type_text.trim() == crate::partition::provision::default_type(kind)
                {
                    if let Some(t) = crate::model::disk_builder::suggested_type(kind, fs, size) {
                        r.type_text = t;
                    }
                }
                r.format = Some(f);
            }
        }
    }

    fn create(&mut self) {
        if self.step != Step::Review || self.job.is_some() {
            return;
        }
        if let Some(e) = self.review_blocked() {
            self.push(LogLevel::Warning, e);
            return;
        }
        let target = match (self.output_is_device, &self.device, &self.image) {
            (true, Some((_, d)), _) => d.path.clone(),
            (false, _, Some(p)) => p.clone(),
            _ => return,
        };
        let Ok(size) = self.disk_size() else {
            return;
        };
        let request = match self.kind {
            DiskKind::Volume => crate::model::provision_runner::BuildRequest::Volume {
                target_path: target.clone(),
                size,
                format: self.volume.clone(),
                full_write: self.output_is_device,
            },
            DiskKind::Partitioned => {
                let Some(placed) = self.builder.validate() else {
                    return;
                };
                crate::model::provision_runner::BuildRequest::Partitioned(
                    crate::model::provision_runner::ProvisionRequest {
                        target_path: target.clone(),
                        target_size_bytes: size,
                        kind: self.builder.kind,
                        geometry: self.builder.geometry,
                        partitions: placed,
                        sources: self.builder.sources(),
                        formats: self.builder.formats(),
                        full_write: self.output_is_device,
                    },
                )
            }
        };
        self.push(
            LogLevel::Info,
            format!("Building on {} ({})", target.display(), format_size(size)),
        );
        let sink = Arc::clone(&self.log);
        self.outcome = None;
        self.finished = None;
        self.step = Step::Build;
        self.job = Some(Job::spawn(
            "Building disk",
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_phase("Writing the disk...");
                let status = crate::model::provision_runner::start_build(request);
                let mut logged = 0;
                crate::model::worker::wait_spawned(ctx, &status, |s, ctx| {
                    if let Ok(mut l) = sink.lock() {
                        for msg in s.log_messages.iter().skip(logged) {
                            l.push((LogLevel::Info, msg.clone()));
                        }
                    }
                    logged = s.log_messages.len();
                    if s.total_bytes > 0 {
                        ctx.set_phase("Building disk");
                        ctx.set_bytes(s.current_bytes, s.total_bytes);
                    }
                    if ctx.is_cancelled() {
                        s.cancel_requested = true;
                    }
                    s.finished
                });
                let err = crate::model::worker::lock_status(&*status).error.clone();
                Ok(match err {
                    Some(e) => Err(e),
                    None => Ok(()),
                })
            },
        ));
    }

    fn poll(&mut self) {
        if let Some(done) = self.probe.as_ref().and_then(|(_, j)| job_outcome(j)) {
            if let Some((row, _)) = self.probe.take() {
                if let Ok((path, size)) = done {
                    partition_edit::set_row_source(&mut self.builder, row, path, size);
                    if let Some(r) = self.builder.rows.get_mut(row) {
                        r.format = None;
                    }
                }
            }
        }
        if let Some(done) = self.job.as_ref().and_then(job_outcome) {
            self.finished = self.job.take().map(|j| j.snapshot());
            let outcome = done.and_then(|r| r);
            match &outcome {
                Ok(()) => self.push(LogLevel::Info, "Disk build complete"),
                Err(e) => self.push(LogLevel::Error, format!("Disk build failed: {e}")),
            }
            self.outcome = Some(outcome);
        }
    }

    /// Block until the running build ends; for a headless shell or a test.
    pub fn wait(&mut self) {
        if let Some(j) = self.probe.as_ref() {
            j.1.wait();
        }
        if let Some(j) = self.job.as_ref() {
            j.wait();
        }
        self.poll();
    }
}

impl Controller for BuildDiskController {
    type Action = BuildDiskAction;

    fn apply(&mut self, action: BuildDiskAction) -> Vec<Effect> {
        let editable = self.editable();
        match action {
            BuildDiskAction::Next => {
                if self.step == Step::Review {
                    self.create();
                } else if self.next_blocked().is_none() {
                    self.go(true);
                }
            }
            BuildDiskAction::Back => {
                if editable {
                    self.go(false);
                }
            }
            BuildDiskAction::GoTo(step) => {
                let steps = self.steps();
                let here = steps.iter().position(|s| *s == self.step);
                let there = steps.iter().position(|s| *s == step);
                if editable && step != Step::Build && there < here {
                    self.step = step;
                }
            }
            BuildDiskAction::Cancel => {
                if let Some(j) = &self.job {
                    j.cancel();
                }
            }
            BuildDiskAction::BuildAnother => {
                if editable {
                    self.outcome = None;
                    self.finished = None;
                    self.confirm_text.clear();
                    self.step = Step::Kind;
                }
            }
            BuildDiskAction::Picked { purpose, path } => {
                if editable {
                    self.picked(purpose, path);
                    self.sync_size();
                }
            }
            _ if !editable => {}
            BuildDiskAction::SetKind(k) => self.kind = k,
            BuildDiskAction::SetOutputIsDevice(on) => self.output_is_device = on,
            BuildDiskAction::SelectDevice { row, device } => {
                self.device = Some((row, device));
                self.confirm_text.clear();
            }
            BuildDiskAction::ClearDevice => {
                self.device = None;
                self.confirm_text.clear();
            }
            BuildDiskAction::PickOutputImage => return self.pick_output(),
            BuildDiskAction::SetSize(t) => self.size_text = t,
            BuildDiskAction::Table(a) => return self.table_edit(a),
            BuildDiskAction::SetRowFs { row, fs } => self.set_row_fs(row, fs),
            BuildDiskAction::SetRowOptions { row, opts } => {
                if let Some(f) = self
                    .builder
                    .rows
                    .get_mut(row)
                    .and_then(|r| r.format.as_mut())
                {
                    f.opts = opts;
                }
            }
            BuildDiskAction::SetVolumeFs(fs) => {
                let label = typed_label(&self.volume, fs);
                self.volume = RowFormat::new(fs);
                if let Some(label) = label {
                    self.volume.opts.label = label;
                }
            }
            BuildDiskAction::SetVolumeOptions(opts) => self.volume.opts = opts,
            BuildDiskAction::SetConfirmText(t) => self.confirm_text = t,
            BuildDiskAction::Create => self.create(),
        }
        self.sync_size();
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll();
        Vec::new()
    }
}
