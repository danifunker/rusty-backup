//! Resize Image: the guided flow that gives an image file a new disk size and resizes its
//! partitions with their filesystems, checking the volumes before and after.
//!
//! [`ResizeImageController`] owns the pages (Size, Check, Partitions, Review, Work, Done), the
//! typed sizes, the checks, the plan check and the write, each slow part a `Job` over
//! `model::resize_image`. Inspect hands it a [`ResizeImageSource`] (its `ResizePlan` plus each
//! partition's number, name and layout colour). See docs/resize_image_wizard_plan.md.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::controller::log::LogLevel;
use crate::controller::{job_outcome, ConfirmSpec, Controller, Effect, PickSpec};
use crate::model::job::{Job, Notify, Snapshot};
use crate::model::partition_resize::{parsed_new_size, stamp_mib};
use crate::model::resize_image::{
    self as engine, Layout, PartProbe, ResizeImageRequest, SharedProgress,
};
use crate::partition::resize::compute_resize_plan;
use crate::partition::PartitionInfo;

pub use crate::controller::inspect::{LayoutSegment, ResizePlan, SegmentKind};
pub use crate::model::hfs_expand_runner::PartitionPlace;
pub use crate::model::resize_image::{Step, StepState, Verdict};
pub use disk_core::display::format_size;

const MIB: u64 = 1024 * 1024;

/// The `purpose` of each picker and confirmation this controller asks for.
pub mod purpose {
    pub const OUTPUT: &str = "resize_image.output";
    pub const IN_PLACE: &str = "resize_image.in_place";
    pub const REPAIR: &str = "resize_image.repair";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const DISK_SIZE: &str = "resize_image.disk_size";
    pub const CHECK: &str = "resize_image.check";
    pub const CONTINUE_ANYWAY: &str = "resize_image.continue_anyway";
    pub const REPAIR: &str = "resize_image.repair";
    pub const ROW_MODE: &str = "resize_image.row_mode";
    pub const ROW_SIZE: &str = "resize_image.row_size";
    pub const IN_PLACE: &str = "resize_image.in_place";
    pub const OUTPUT: &str = "resize_image.output";
    pub const BACK: &str = "resize_image.back";
    pub const NEXT: &str = "resize_image.next";
    pub const START: &str = "resize_image.start";
    pub const CANCEL: &str = "resize_image.cancel";
    pub const CLOSE: &str = "resize_image.close";
    pub const OPEN_RESULT: &str = "resize_image.open_result";
    pub const BAR: &str = "resize_image.bar";
}

/// What every shell shows on the same controls.
pub mod labels {
    pub const TITLE: &str = "Resize Image";
    pub const DISK_SIZE: &str = "New disk size";
    pub const CHECK: &str = "Check the volumes before and after resizing";
    pub const CONTINUE_ANYWAY: &str = "Resize anyway";
    pub const IN_PLACE: &str = "Change this file in place (no copy)";
    pub const START: &str = "Resize";
    pub const OPEN_RESULT: &str = "Open in Inspect";
    pub const SIZE_HINT: &str = "Accepts K / M / G suffixes, e.g. 512M or 2G.";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    Size,
    Check,
    Partitions,
    Review,
    Work,
    Done,
}

impl Page {
    pub const ALL: [Page; 6] = [
        Page::Size,
        Page::Check,
        Page::Partitions,
        Page::Review,
        Page::Work,
        Page::Done,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Page::Size => "Disk size",
            Page::Check => "Check",
            Page::Partitions => "Partitions",
            Page::Review => "Review",
            Page::Work => "Resize",
            Page::Done => "Done",
        }
    }

    /// "Step 2 of 6: Check   (Disk size > Check > ...)".
    pub fn heading(self) -> String {
        let n = Page::ALL.iter().position(|p| *p == self).unwrap_or(0) + 1;
        let trail: Vec<&str> = Page::ALL.iter().map(|p| p.label()).collect();
        format!("Step {n} of 6: {}   ({})", self.label(), trail.join(" > "))
    }
}

/// How a partition row is sized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartMode {
    Keep,
    /// Takes all the space the other rows leave; one row at most.
    Fill,
    Minimum,
    Custom,
}

impl PartMode {
    pub fn label(self) -> &'static str {
        match self {
            PartMode::Keep => "Keep",
            PartMode::Fill => "Fill free space",
            PartMode::Minimum => "Minimum",
            PartMode::Custom => "Custom",
        }
    }
}

/// What Inspect hands over: its plan, and how its grid and layout bar name each partition.
pub struct ResizeImageSource {
    pub plan: ResizePlan,
    pub places: HashMap<usize, PartitionPlace>,
}

/// One partition as the wizard shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct PartRow {
    pub index: usize,
    /// "Partition 2 of 3: Macintosh HD (HFS)".
    pub title: String,
    pub color_index: Option<usize>,
    pub size: u64,
    pub minimum: Option<u64>,
    /// Why the partition keeps its size (a filesystem with no in-place resizer).
    pub fixed: Option<String>,
    /// Classic HFS: its allocation block size, which caps how far it grows in place.
    pub hfs_block: Option<u32>,
    /// A bare volume: it is the whole disk, so it always fills it.
    pub whole: bool,
    pub mode: PartMode,
    /// The size field, MiB.
    pub text: String,
}

impl PartRow {
    pub fn modes(&self) -> Vec<PartMode> {
        if self.whole {
            return vec![PartMode::Fill];
        }
        if self.fixed.is_some() {
            return vec![PartMode::Keep];
        }
        let mut modes = vec![PartMode::Keep, PartMode::Fill];
        if self.minimum.is_some_and(|m| m < self.size) {
            modes.push(PartMode::Minimum);
        }
        modes.push(PartMode::Custom);
        modes
    }

    /// "Used 12.0 MiB of 32.0 MiB" or why it cannot change.
    pub fn note(&self) -> String {
        if let Some(fs) = &self.fixed {
            let how = match self.whole {
                true => "cannot be resized, so the disk keeps its size",
                false => "cannot be resized, keeps its size (may still move)",
            };
            return format!("{fs}: {how}");
        }
        if self.whole {
            let smallest = self.minimum.map_or("unknown".to_string(), format_size);
            return format!("A bare volume: it is the whole disk. Smallest: {smallest}");
        }
        match self.minimum {
            Some(m) => format!("Smallest: {}", format_size(m)),
            None => "Smallest: unknown".to_string(),
        }
    }
}

/// One row of the Check page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckRow {
    pub index: usize,
    pub title: String,
    /// `None` while it is being checked.
    pub verdict: Option<Verdict>,
}

pub enum ResizeImageAction {
    Open(Box<ResizeImageSource>),
    Close,
    Back,
    Next,
    SetDiskSize(String),
    SetCheck(bool),
    ContinueAnyway(bool),
    /// Repair partition `index` of the original image; asks first.
    Repair(usize),
    SetMode {
        index: usize,
        mode: PartMode,
    },
    SetSizeText {
        index: usize,
        text: String,
    },
    SetInPlace(bool),
    PickOutput,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Confirmed {
        purpose: &'static str,
        yes: bool,
    },
    Start,
    Cancel,
    OpenResult,
}

type Checks = Vec<(usize, Verdict)>;

pub struct ResizeImageController {
    notify: Arc<dyn Notify>,
    log: Vec<(LogLevel, String)>,
    page: Page,
    source: Option<ResizeImageSource>,
    old_disk: u64,
    disk_text: String,
    check: bool,
    checks: Vec<CheckRow>,
    continue_anyway: bool,
    check_job: Option<Job<Checks>>,
    probe_job: Option<Job<Vec<PartProbe>>>,
    rows: Vec<PartRow>,
    plan_job: Option<Job<Result<Layout, String>>>,
    plan_error: Option<String>,
    layout: Option<Layout>,
    in_place: bool,
    output: Option<PathBuf>,
    asked: Option<&'static str>,
    pending_repair: Option<usize>,
    work: Option<Job<Result<(), String>>>,
    progress: SharedProgress,
    outcome: Option<Result<(), String>>,
    open_request: Option<PathBuf>,
    disk_changed: bool,
}

impl ResizeImageController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        ResizeImageController {
            notify,
            log: Vec::new(),
            page: Page::Size,
            source: None,
            old_disk: 0,
            disk_text: String::new(),
            check: true,
            checks: Vec::new(),
            continue_anyway: false,
            check_job: None,
            probe_job: None,
            rows: Vec::new(),
            plan_job: None,
            plan_error: None,
            layout: None,
            in_place: false,
            output: None,
            asked: None,
            pending_repair: None,
            work: None,
            progress: SharedProgress::default(),
            outcome: None,
            open_request: None,
            disk_changed: false,
        }
    }

    // --- what the views read -----------------------------------------------------------

    pub fn is_open(&self) -> bool {
        self.source.is_some()
    }

    pub fn page(&self) -> Page {
        self.page
    }

    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        std::mem::take(&mut self.log)
    }

    /// The file to open in Inspect once the user asks (the copy, or the changed original).
    pub fn take_open_request(&mut self) -> Option<PathBuf> {
        self.open_request.take()
    }

    /// The original was changed in place: Inspect reads it again.
    pub fn take_disk_changed(&mut self) -> bool {
        std::mem::take(&mut self.disk_changed)
    }

    pub fn source_path(&self) -> Option<&Path> {
        self.source.as_ref().map(|s| s.plan.path.as_path())
    }

    pub fn current_size(&self) -> u64 {
        self.old_disk
    }

    pub fn disk_text(&self) -> &str {
        self.disk_text.as_str()
    }

    /// The typed disk size, or why it is not one.
    pub fn disk_size(&self) -> Result<u64, String> {
        let size = crate::controller::build_disk::parse_size_field(self.disk_text.as_str())?
            .ok_or_else(|| "Type the new disk size.".to_string())?;
        let size = size / 512 * 512;
        match size < MIB {
            true => Err("The disk must be at least 1 MiB.".to_string()),
            false => Ok(size),
        }
    }

    /// "Grows by 32.0 MiB" or "Shrinks by 8.0 MiB" or "Keeps its size".
    pub fn disk_change(&self) -> String {
        match self.disk_size() {
            Ok(n) if n > self.old_disk => format!("Grows by {}", format_size(n - self.old_disk)),
            Ok(n) if n < self.old_disk => {
                format!("Shrinks by {}", format_size(self.old_disk - n))
            }
            Ok(_) => "Keeps its size".to_string(),
            Err(e) => e,
        }
    }

    pub fn check_enabled(&self) -> bool {
        self.check
    }

    pub fn checks(&self) -> &[CheckRow] {
        self.checks.as_slice()
    }

    pub fn checking(&self) -> bool {
        self.check_job.is_some()
    }

    /// Some volume reports a problem the user has to decide on.
    pub fn check_trouble(&self) -> bool {
        self.checks
            .iter()
            .any(|r| r.verdict.as_ref().is_some_and(|v| !v.passes()))
    }

    pub fn continue_anyway(&self) -> bool {
        self.continue_anyway
    }

    pub fn rows(&self) -> &[PartRow] {
        self.rows.as_slice()
    }

    /// Still reading each partition's minimum size and resizability.
    pub fn probing(&self) -> bool {
        self.probe_job.is_some()
    }

    pub fn planning(&self) -> bool {
        self.plan_job.is_some()
    }

    pub fn plan_error(&self) -> Option<&str> {
        self.plan_error.as_deref()
    }

    pub fn in_place(&self) -> bool {
        self.in_place
    }

    /// Where the resized image goes: the copy's path, or the original when in place.
    pub fn output(&self) -> Option<PathBuf> {
        match self.in_place {
            true => self.source_path().map(Path::to_path_buf),
            false => self.output.clone(),
        }
    }

    pub fn progress(&self) -> Option<Snapshot> {
        self.work.as_ref().map(|j| j.snapshot())
    }

    pub fn is_running(&self) -> bool {
        self.work.is_some()
    }

    /// The steps of the write with their state, and the check after it.
    pub fn steps(&self) -> Vec<(Step, StepState)> {
        crate::model::worker::lock_status(&*self.progress)
            .steps
            .clone()
    }

    pub fn after_checks(&self) -> Vec<CheckRow> {
        let p = crate::model::worker::lock_status(&*self.progress);
        p.checks
            .iter()
            .map(|(i, v)| CheckRow {
                index: *i,
                title: self.title(*i),
                verdict: Some(v.clone()),
            })
            .collect()
    }

    pub fn outcome(&self) -> Option<&Result<(), String>> {
        self.outcome.as_ref()
    }

    pub fn can_back(&self) -> bool {
        !matches!(self.page, Page::Size | Page::Work | Page::Done) && !self.busy()
    }

    pub fn can_next(&self) -> bool {
        if self.busy() {
            return false;
        }
        match self.page {
            Page::Size => self.disk_size().is_ok(),
            Page::Check => {
                !self.check
                    || (self.checks.iter().all(|r| r.verdict.is_some())
                        && (!self.check_trouble() || self.continue_anyway))
            }
            Page::Partitions => self.live_error().is_none() && !self.probing(),
            Page::Review | Page::Work | Page::Done => false,
        }
    }

    pub fn can_start(&self) -> bool {
        self.page == Page::Review && self.layout.is_some() && self.output().is_some()
    }

    fn busy(&self) -> bool {
        self.check_job.is_some() || self.plan_job.is_some() || self.work.is_some()
    }

    // --- the layout, recomputed from the typed sizes -----------------------------------

    fn partitions(&self) -> &[PartitionInfo] {
        self.source
            .as_ref()
            .map_or(&[][..], |s| s.plan.partitions.as_slice())
    }

    fn title(&self, index: usize) -> String {
        let place = self.source.as_ref().and_then(|s| s.places.get(&index));
        match place {
            Some(p) => format!(
                "Partition {} of {}: {} ({})",
                p.number, p.of, p.label, p.kind
            ),
            None => format!("Partition {}", index + 1),
        }
    }

    /// "2: Data", as a bar segment names a partition.
    fn short_title(&self, index: usize) -> String {
        match self.source.as_ref().and_then(|s| s.places.get(&index)) {
            Some(p) => format!("{}: {}", p.number, p.label),
            None => format!("{}", index + 1),
        }
    }

    /// Each row's new size from its mode and field; a Fill row is left at its size here.
    fn typed_sizes(&self) -> Result<Vec<(usize, u64)>, String> {
        let mut out = Vec::new();
        for r in &self.rows {
            let size = match r.mode {
                PartMode::Keep | PartMode::Fill => r.size,
                PartMode::Minimum => r.minimum.unwrap_or(r.size),
                PartMode::Custom => parsed_new_size(r.text.as_str(), r.size).ok_or_else(|| {
                    format!(
                        "{}: '{}' is not a size in MiB.",
                        self.title(r.index),
                        r.text
                    )
                })?,
            };
            if let Some(m) = r.minimum.filter(|m| size < *m) {
                return Err(format!(
                    "{} can shrink to {} at the smallest.",
                    self.title(r.index),
                    format_size(m)
                ));
            }
            out.push((r.index, size));
        }
        Ok(out)
    }

    /// Where the partitions end for `sizes`, through the same planner the write uses.
    fn data_end(&self, sizes: &[(usize, u64)], room: u64) -> Result<u64, String> {
        if self.is_bare() {
            return Ok(sizes.first().map_or(0, |s| s.1));
        }
        let changed: Vec<(usize, u64)> = sizes
            .iter()
            .filter(|(i, s)| self.rows.iter().any(|r| r.index == *i && r.size != *s))
            .copied()
            .collect();
        let align = self
            .source
            .as_ref()
            .map_or(2048, |s| s.plan.alignment_sectors);
        let plans = match changed.is_empty() {
            true => Vec::new(),
            false => compute_resize_plan(self.partitions(), changed.as_slice(), align, room)
                .map_err(|e| format!("{e:#}"))?,
        };
        Ok(self
            .partitions()
            .iter()
            .filter(|p| !p.is_logical)
            .map(|p| match plans.iter().find(|r| r.index == p.index) {
                Some(r) => r.new_start_lba * 512 + r.new_size_bytes,
                None => p.byte_offset() + p.size_bytes,
            })
            .max()
            .unwrap_or(0))
    }

    /// The image is one bare volume: no table, the volume is the disk.
    fn is_bare(&self) -> bool {
        self.source
            .as_ref()
            .is_some_and(|s| engine::is_bare(&s.plan.table))
    }

    fn tail(&self) -> u64 {
        self.source
            .as_ref()
            .map_or(0, |s| engine::table_tail(&s.plan.table))
    }

    /// The final sizes, with a Fill row given the space the others leave.
    pub fn final_sizes(&self) -> Result<Vec<(usize, u64)>, String> {
        let disk = self.disk_size()?;
        if self.is_bare() {
            let Some(row) = self.rows.first() else {
                return Ok(Vec::new());
            };
            if let Some(m) = row.minimum.filter(|m| disk < *m) {
                return Err(format!(
                    "The volume can shrink to {} at the smallest.",
                    format_size(m)
                ));
            }
            if row.fixed.is_some() && disk != row.size {
                return Err(row.note());
            }
            return Ok(vec![(row.index, disk)]);
        }
        let mut sizes = self.typed_sizes()?;
        let room = disk.max(self.old_disk);
        let end = self.data_end(sizes.as_slice(), room)?;
        let free = (disk - self.tail()).saturating_sub(end);
        if let Some(fill) = self.rows.iter().find(|r| r.mode == PartMode::Fill) {
            let align = self
                .source
                .as_ref()
                .map_or(2048, |s| s.plan.alignment_sectors.max(1))
                * 512;
            if let Some(s) = sizes.iter_mut().find(|(i, _)| *i == fill.index) {
                s.1 = (fill.size + free) / align * align;
                s.1 = s.1.max(fill.size);
            }
        }
        Ok(sizes)
    }

    /// The block size partition `index` is rebuilt at: classic HFS asked past its in-place limit.
    pub fn rebuild_block(&self, index: usize) -> Option<u32> {
        let row = self.rows.iter().find(|r| r.index == index)?;
        let block = row.hfs_block?;
        let sizes = self.final_sizes().ok()?;
        let size = sizes.iter().find(|(i, _)| *i == index)?.1;
        (size > engine::hfs_ceiling(block)).then(|| fs_mac::hfv::suggest_block_size(size))
    }

    /// What a row's size means beyond the number: an HFS rebuild, said plainly.
    pub fn row_warning(&self, index: usize) -> Option<String> {
        let block = self.rebuild_block(index)?;
        let old = self.rows.iter().find(|r| r.index == index)?.hfs_block?;
        Some(format!(
            "Past classic HFS's {} limit at {} blocks: rebuilt with {} KiB blocks (every file is \
             copied into a new volume).",
            format_size(engine::hfs_ceiling(old)),
            block_text(old),
            block / 1024
        ))
    }

    /// Why the typed layout cannot be written, found without touching the disk.
    pub fn live_error(&self) -> Option<String> {
        let disk = match self.disk_size() {
            Ok(d) => d,
            Err(e) => return Some(e),
        };
        let sizes = match self.final_sizes() {
            Ok(s) => s,
            Err(e) => return Some(e),
        };
        match self.data_end(sizes.as_slice(), disk.max(self.old_disk)) {
            Ok(end) if end + self.tail() > disk => Some(format!(
                "The partitions need {}; the disk is {}.",
                format_size(end + self.tail()),
                format_size(disk)
            )),
            Ok(_) => None,
            Err(e) => Some(e),
        }
    }

    /// Unallocated space left at the end of the disk.
    pub fn free_at_end(&self) -> Option<u64> {
        let disk = self.disk_size().ok()?;
        let sizes = self.final_sizes().ok()?;
        let end = self
            .data_end(sizes.as_slice(), disk.max(self.old_disk))
            .ok()?;
        (disk - self.tail()).checked_sub(end)
    }

    /// The disk before and after, in each partition's layout colour, then free space.
    pub fn bars(&self) -> (Vec<LayoutSegment>, Vec<LayoutSegment>) {
        let seg = |r: &PartRow, size| LayoutSegment {
            label: self.short_title(r.index),
            fs: String::new(),
            size_bytes: size,
            kind: match r.color_index {
                Some(color_index) => SegmentKind::Partition { color_index },
                None => SegmentKind::Dimmed,
            },
            partition: Some(r.index),
        };
        let free = |size| LayoutSegment {
            label: "Free".to_string(),
            fs: String::new(),
            size_bytes: size,
            kind: SegmentKind::Free,
            partition: None,
        };
        let mut before: Vec<LayoutSegment> = self.rows.iter().map(|r| seg(r, r.size)).collect();
        let used: u64 = self.rows.iter().map(|r| r.size).sum();
        if self.old_disk > used + MIB {
            before.push(free(self.old_disk - used));
        }
        let sizes = self.final_sizes().unwrap_or_default();
        let mut after: Vec<LayoutSegment> = self
            .rows
            .iter()
            .map(|r| {
                let size = sizes
                    .iter()
                    .find(|(i, _)| *i == r.index)
                    .map_or(r.size, |s| s.1);
                seg(r, size)
            })
            .collect();
        if let Some(f) = self.free_at_end().filter(|f| *f >= MIB) {
            after.push(free(f));
        }
        (before, after)
    }

    /// The Review page: everything the write will do, in order.
    pub fn review(&self) -> Vec<String> {
        let mut lines = Vec::new();
        let Ok(disk) = self.disk_size() else {
            return lines;
        };
        match self.output() {
            Some(out) if !self.in_place => {
                lines.push(format!("Copy the image to {}", out.display()))
            }
            _ => lines.push("Change the image in place (no copy)".to_string()),
        }
        if disk != self.old_disk {
            lines.push(format!(
                "Disk: {} -> {}",
                format_size(self.old_disk),
                format_size(disk)
            ));
        }
        let sizes = self.final_sizes().unwrap_or_default();
        if let Some(layout) = &self.layout {
            for p in &layout.plans {
                let row = self.rows.iter().find(|r| r.index == p.index);
                let title = self.title(p.index);
                let size = sizes.iter().find(|(i, _)| *i == p.index).map_or(0, |s| s.1);
                let resized = p.new_size_bytes != p.old_size_bytes;
                let mut parts = Vec::new();
                let rebuilt = self.rebuild_block(p.index);
                if resized {
                    let how = match rebuilt {
                        Some(block) => format!("rebuilt with {} KiB blocks", block / 1024),
                        None => "with its filesystem".to_string(),
                    };
                    parts.push(format!(
                        "{} -> {} {how}",
                        format_size(p.old_size_bytes),
                        format_size(size.max(p.new_size_bytes))
                    ));
                }
                if p.needs_data_move {
                    parts.push(format!("moves to LBA {}", p.new_start_lba));
                }
                if parts.is_empty() || row.is_none() {
                    continue;
                }
                lines.push(format!("{title}: {}", parts.join(", ")));
            }
        }
        if let Some(f) = self.free_at_end().filter(|f| *f > 0) {
            lines.push(format!("Free space left at the end: {}", format_size(f)));
        }
        if self.check {
            lines.push("Then check every resized or moved volume again".to_string());
        }
        if let Some(layout) = &self.layout {
            if layout.chd.is_some() {
                lines.push(
                    "The CHD is unpacked to a raw file, resized, then packed again with its \
                     codecs and hunk size"
                        .to_string(),
                );
            }
            let free = match layout.space.free {
                Some(f) => format!(" ({} free)", format_size(f)),
                None => String::new(),
            };
            lines.push(format!(
                "Needs about {} free in {}{free}",
                format_size(layout.space.needed),
                layout.space.dir.display()
            ));
        }
        lines
    }

    // --- actions ------------------------------------------------------------------------

    fn open(&mut self, source: ResizeImageSource) {
        if self.busy() {
            return;
        }
        let path = source.plan.path.clone();
        let (old, _) = match engine::disk_size_of(path.as_path()) {
            Ok(s) => s,
            Err(e) => {
                self.log
                    .push((LogLevel::Error, format!("Resize Image: {e:#}")));
                return;
            }
        };
        let mut fresh = ResizeImageController::new(Arc::clone(&self.notify));
        fresh.log = std::mem::take(&mut self.log);
        fresh.old_disk = old;
        fresh.disk_text = stamp_size(old);
        fresh.output = Some(default_copy(path.as_path()));
        let parts: Vec<PartitionInfo> = source
            .plan
            .partitions
            .iter()
            .filter(|p| !p.is_extended_container && !p.is_logical)
            .cloned()
            .collect();
        fresh.rows = parts
            .iter()
            .map(|p| PartRow {
                index: p.index,
                title: String::new(),
                color_index: source.places.get(&p.index).map(|pl| pl.color_index),
                size: p.size_bytes,
                minimum: source.plan.min_sizes.get(&p.index).copied(),
                fixed: None,
                hfs_block: None,
                whole: engine::is_bare(&source.plan.table),
                mode: match engine::is_bare(&source.plan.table) {
                    true => PartMode::Fill,
                    false => PartMode::Keep,
                },
                text: stamp_mib(p.size_bytes),
            })
            .collect();
        fresh.source = Some(source);
        for i in 0..fresh.rows.len() {
            fresh.rows[i].title = fresh.title(fresh.rows[i].index);
        }
        let all = fresh.partitions().to_vec();
        fresh.probe_job = Some(Job::spawn(
            "Reading the partitions",
            Arc::clone(&fresh.notify),
            move |ctx| engine::probe(path.as_path(), all.as_slice(), ctx),
        ));
        *self = fresh;
    }

    fn start_checks(&mut self) {
        let Some(path) = self.source_path().map(Path::to_path_buf) else {
            return;
        };
        let parts: Vec<PartitionInfo> = self
            .partitions()
            .iter()
            .filter(|p| !p.is_extended_container)
            .cloned()
            .collect();
        self.checks = parts
            .iter()
            .map(|p| CheckRow {
                index: p.index,
                title: self.title(p.index),
                verdict: None,
            })
            .collect();
        self.continue_anyway = false;
        self.check_job = Some(Job::spawn(
            "Checking the volumes",
            Arc::clone(&self.notify),
            move |ctx| {
                let mut out = Vec::new();
                for p in &parts {
                    ctx.set_phase(format!("Checking partition {}", p.index + 1));
                    out.push((
                        p.index,
                        engine::check_partition(path.as_path(), p.byte_offset(), p),
                    ));
                }
                Ok(out)
            },
        ));
    }

    fn repair(&mut self, index: usize) {
        let Some(path) = self.source_path().map(Path::to_path_buf) else {
            return;
        };
        let Some(part) = self.partitions().iter().find(|p| p.index == index).cloned() else {
            return;
        };
        if let Some(r) = self.checks.iter_mut().find(|r| r.index == index) {
            r.verdict = None;
        }
        let checked = self
            .checks
            .iter()
            .filter(|r| r.index != index)
            .filter_map(|r| r.verdict.clone().map(|v| (r.index, v)))
            .collect::<Vec<_>>();
        self.check_job = Some(Job::spawn(
            "Repairing the volume",
            Arc::clone(&self.notify),
            move |_| {
                let mut out = checked;
                out.push((
                    index,
                    engine::repair_partition(path.as_path(), part.byte_offset(), &part),
                ));
                Ok(out)
            },
        ));
    }

    fn set_mode(&mut self, index: usize, mode: PartMode) {
        let Some(row) = self.rows.iter().find(|r| r.index == index) else {
            return;
        };
        if !row.modes().contains(&mode) {
            return;
        }
        if mode == PartMode::Fill {
            for r in self.rows.iter_mut().filter(|r| r.mode == PartMode::Fill) {
                r.mode = PartMode::Keep;
                r.text = stamp_mib(r.size);
            }
        }
        if let Some(row) = self.rows.iter_mut().find(|r| r.index == index) {
            row.mode = mode;
            match mode {
                PartMode::Keep | PartMode::Fill => row.text = stamp_mib(row.size),
                PartMode::Minimum => row.text = stamp_mib(row.minimum.unwrap_or(row.size)),
                PartMode::Custom => {}
            }
        }
        self.stale();
    }

    fn set_text(&mut self, index: usize, text: String) {
        if let Some(row) = self.rows.iter_mut().find(|r| r.index == index) {
            if row.fixed.is_none() && !row.whole && row.text != text {
                row.text = text;
                row.mode = PartMode::Custom;
                self.stale();
            }
        }
    }

    /// A size changed: the checked plan no longer describes it.
    fn stale(&mut self) {
        self.layout = None;
        self.plan_error = None;
    }

    fn request(&self) -> Option<ResizeImageRequest> {
        let source = self.source.as_ref()?;
        let sizes = self.final_sizes().ok()?;
        let sizes = sizes
            .into_iter()
            .filter(|(i, s)| self.rows.iter().any(|r| r.index == *i && r.size != *s))
            .collect();
        Some(ResizeImageRequest {
            source: source.plan.path.clone(),
            copy_to: (!self.in_place).then(|| self.output.clone()).flatten(),
            table: source.plan.table.clone(),
            partitions: source.plan.partitions.clone(),
            alignment_sectors: source.plan.alignment_sectors,
            disk_size: self.disk_size().ok()?,
            sizes,
            check_after: self.check,
            rebuild: self
                .rows
                .iter()
                .filter_map(|r| self.rebuild_block(r.index).map(|b| (r.index, b)))
                .collect(),
        })
    }

    /// Partitions -> Review: the engine plans against the image, rehearsing every shrink.
    fn start_plan(&mut self) {
        let Some(req) = self.request() else {
            return;
        };
        self.plan_error = None;
        self.plan_job = Some(Job::spawn(
            "Checking the plan",
            Arc::clone(&self.notify),
            move |_| Ok(engine::plan(&req).map_err(|e| format!("{e:#}"))),
        ));
    }

    fn next(&mut self) {
        if !self.can_next() {
            return;
        }
        match self.page {
            Page::Size => {
                self.page = Page::Check;
                if self.check && self.checks.is_empty() {
                    self.start_checks();
                }
            }
            Page::Check => self.page = Page::Partitions,
            Page::Partitions => self.start_plan(),
            _ => {}
        }
    }

    fn back(&mut self) {
        if !self.can_back() {
            return;
        }
        self.page = match self.page {
            Page::Check => Page::Size,
            Page::Partitions => Page::Check,
            Page::Review => Page::Partitions,
            p => p,
        };
    }

    fn pick_output(&self) -> Vec<Effect> {
        let Some(path) = self.source_path() else {
            return Vec::new();
        };
        let name = default_copy(path)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::OUTPUT,
            title: "Save Resized Image".to_string(),
            filters: Vec::new(),
            save: true,
            file_name: name,
        })]
    }

    /// Start: in place asks first, since the original is changed.
    fn ask_start(&mut self) -> Vec<Effect> {
        if !self.can_start() {
            return Vec::new();
        }
        if !self.in_place {
            self.start();
            return Vec::new();
        }
        let name = self.source_path().map(|p| p.display().to_string());
        self.asked = Some(purpose::IN_PLACE);
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::IN_PLACE,
            title: labels::TITLE.to_string(),
            message: format!(
                "Resize {} in place? There is no copy to go back to if it fails part way.",
                name.unwrap_or_default()
            ),
            confirm_label: "Resize in Place".to_string(),
            destructive: true,
        })]
    }

    fn ask_repair(&mut self, index: usize) -> Vec<Effect> {
        if self.busy() {
            return Vec::new();
        }
        self.asked = Some(purpose::REPAIR);
        self.pending_repair = Some(index);
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::REPAIR,
            title: "Repair Filesystem?".to_string(),
            message: format!(
                "Repair {} in the original image? This changes the original even when the \
                 resize writes a copy.",
                self.title(index)
            ),
            confirm_label: "Repair".to_string(),
            destructive: true,
        })]
    }

    fn start(&mut self) {
        let Some(req) = self.request() else {
            return;
        };
        if let Some(out) = &req.copy_to {
            if out.exists() {
                self.plan_error = Some(format!(
                    "{} already exists: choose another name.",
                    out.display()
                ));
                return;
            }
        }
        self.progress = SharedProgress::default();
        let shared = Arc::clone(&self.progress);
        self.log.push((
            LogLevel::Info,
            format!("Resize Image: {}", req.target().display()),
        ));
        self.page = Page::Work;
        self.work = Some(Job::spawn(
            "Resizing the image",
            Arc::clone(&self.notify),
            move |ctx| {
                let _wake = crate::os::wakelock::acquire("Rusty Backup: resize image");
                Ok(engine::run(&req, ctx, &shared).map_err(|e| format!("{e:#}")))
            },
        ));
    }

    // --- polling ------------------------------------------------------------------------

    fn poll(&mut self) {
        if let Some(done) = self.probe_job.as_ref().and_then(job_outcome) {
            self.probe_job = None;
            match done {
                Ok(probes) => {
                    for p in probes {
                        if let Some(r) = self.rows.iter_mut().find(|r| r.index == p.index) {
                            r.fixed = p.fixed;
                            r.minimum = p.minimum.or(r.minimum);
                            r.hfs_block = p.hfs_block;
                        }
                    }
                }
                Err(e) => self
                    .log
                    .push((LogLevel::Warning, format!("Resize Image: {e}"))),
            }
        }
        if let Some(done) = self.check_job.as_ref().and_then(job_outcome) {
            self.check_job = None;
            match done {
                Ok(results) => {
                    for (i, v) in results {
                        if let Some(r) = self.checks.iter_mut().find(|r| r.index == i) {
                            r.verdict = Some(v);
                        }
                    }
                }
                Err(e) => {
                    for r in self.checks.iter_mut().filter(|r| r.verdict.is_none()) {
                        r.verdict = Some(Verdict::Failed(e.clone()));
                    }
                }
            }
        }
        if let Some(done) = self.plan_job.as_ref().and_then(job_outcome) {
            self.plan_job = None;
            match done.and_then(|r| r) {
                Ok(layout) => {
                    self.layout = Some(layout);
                    self.page = Page::Review;
                }
                Err(e) => self.plan_error = Some(e),
            }
        }
        self.drain_lines();
        if let Some(done) = self.work.as_ref().and_then(job_outcome) {
            self.work = None;
            self.drain_lines();
            let done = done.and_then(|r| r);
            match &done {
                Ok(()) => self
                    .log
                    .push((LogLevel::Info, "Resize Image finished.".to_string())),
                Err(e) => self
                    .log
                    .push((LogLevel::Error, format!("Resize Image failed: {e}"))),
            }
            // In place, even a failed write may have changed the original.
            self.disk_changed = self.in_place;
            self.outcome = Some(done);
            self.page = Page::Done;
        }
    }

    fn drain_lines(&mut self) {
        let lines = std::mem::take(&mut crate::model::worker::lock_status(&*self.progress).lines);
        self.log
            .extend(lines.into_iter().map(|l| (LogLevel::Info, l)));
    }

    /// Block until every job ends; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        while self.busy() || self.probe_job.is_some() {
            self.poll();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        self.poll();
    }
}

impl Controller for ResizeImageController {
    type Action = ResizeImageAction;

    fn apply(&mut self, action: ResizeImageAction) -> Vec<Effect> {
        use ResizeImageAction as A;
        let edits = !matches!(
            action,
            A::Open(_) | A::Close | A::Cancel | A::Confirmed { .. } | A::OpenResult
        );
        if edits && self.work.is_some() {
            return Vec::new();
        }
        match action {
            A::Open(source) => self.open(*source),
            A::Close => {
                if self.work.is_none() {
                    self.source = None;
                }
            }
            A::Back => self.back(),
            A::Next => self.next(),
            A::SetDiskSize(text) => {
                if self.disk_text != text {
                    self.disk_text = text;
                    self.stale();
                }
            }
            A::SetCheck(on) => {
                if self.check_job.is_none() {
                    self.check = on;
                    if on && self.page == Page::Check && self.checks.is_empty() {
                        self.start_checks();
                    }
                }
            }
            A::ContinueAnyway(on) => self.continue_anyway = on,
            A::Repair(index) => return self.ask_repair(index),
            A::SetMode { index, mode } => self.set_mode(index, mode),
            A::SetSizeText { index, text } => self.set_text(index, text),
            A::SetInPlace(on) => self.in_place = on,
            A::PickOutput => return self.pick_output(),
            A::Picked { purpose, path } => {
                if let (purpose::OUTPUT, Some(path)) = (purpose, path) {
                    self.output = Some(path);
                    self.plan_error = None;
                }
            }
            A::Confirmed { purpose, yes } => {
                if self.asked.take() != Some(purpose) || !yes {
                    self.pending_repair = None;
                    return Vec::new();
                }
                match purpose {
                    purpose::IN_PLACE => self.start(),
                    purpose::REPAIR => {
                        if let Some(i) = self.pending_repair.take() {
                            self.repair(i);
                        }
                    }
                    _ => {}
                }
            }
            A::Start => return self.ask_start(),
            A::Cancel => {
                if let Some(job) = &self.work {
                    job.cancel();
                }
            }
            A::OpenResult => {
                if self.outcome.as_ref().is_some_and(|o| o.is_ok()) {
                    self.open_request = self.output();
                }
            }
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll();
        Vec::new()
    }
}

/// "512-byte" or "4 KiB".
fn block_text(block: u32) -> String {
    match block % 1024 {
        0 => format!("{} KiB", block / 1024),
        _ => format!("{block}-byte"),
    }
}

/// A size field's starting text: whole MiB as "64M", anything else in bytes.
fn stamp_size(bytes: u64) -> String {
    match bytes % MIB {
        0 => format!("{}M", bytes / MIB),
        _ => format!("{bytes}"),
    }
}

/// `disk.img` -> `disk-resized.img` beside it, numbered when that name is taken.
pub fn default_copy(path: &Path) -> PathBuf {
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "image".to_string());
    let ext = path
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let dir = path.parent().unwrap_or(Path::new("."));
    (1..1000)
        .map(|n| match n {
            1 => dir.join(format!("{stem}-resized{ext}")),
            n => dir.join(format!("{stem}-resized-{n}{ext}")),
        })
        .find(|p| !p.exists())
        .unwrap_or_else(|| dir.join(format!("{stem}-resized{ext}")))
}

#[cfg(test)]
mod tests;
