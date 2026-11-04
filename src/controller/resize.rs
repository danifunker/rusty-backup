//! Resize Partitions: grow, shrink and move the partitions of an image or device in place.
//!
//! [`ResizeController`] starts from the [`ResizePlan`] Inspect builds (table, partitions, minimum
//! sizes, alignment, the container's capacity), owns each row's size mode and typed size, the
//! Current / After layout bars, the preview (`compute_resize_plan`), the device-risk
//! acknowledgement, and the write as a job over `model::partition_resize::run_resize`. When a
//! write ends it tells the host to read the table again (`take_disk_changed`). Shell:
//! `src/gui/resize_popup.rs`. See docs/phase2_commander_plan.md § "Slice 15".

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::controller::log::LogLevel;
use crate::controller::partition_edit::{BarSegment, LayoutBar};
use crate::controller::{job_outcome, Controller, Effect};
use crate::model::job::{Job, Notify, Snapshot};
use crate::model::partition_resize::{self, parsed_new_size, stamp_mib};
use crate::model::status::ResizeStatus;
use crate::partition::resize::compute_resize_plan;
use crate::partition::{PartitionInfo, PartitionTable};

pub use crate::controller::inspect::ResizePlan;
pub use crate::model::size_mode::SizeMode;
pub use disk_core::display::format_size;

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const DEVICE_RISK: &str = "resize.device_risk";
    pub const SIZE_MODE: &str = "resize.size_mode";
    pub const NEW_SIZE: &str = "resize.new_size";
    pub const PREVIEW: &str = "resize.preview";
    pub const APPLY: &str = "resize.apply";
    pub const CANCEL: &str = "resize.cancel";
    pub const CLOSE: &str = "resize.close";
}

/// User intents.
pub enum ResizeAction {
    Open(Box<ResizePlan>),
    Close,
    /// Original / Minimum stamp the row's size field; Custom leaves it editable.
    SetMode {
        index: usize,
        mode: SizeMode,
    },
    SetSizeText {
        index: usize,
        text: String,
    },
    /// "I understand the risks" for a physical device.
    AcceptDeviceRisk(bool),
    Preview,
    Apply,
    Cancel,
}

/// One row of the size grid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResizeRow {
    pub index: usize,
    pub type_name: String,
    pub original_size: u64,
    pub minimum_size: u64,
    /// An extended container: shown, not editable.
    pub extended: bool,
    pub mode: SizeMode,
    /// The New Size field, MiB.
    pub text: String,
    /// The Minimum choice applies (a known minimum below the current size).
    pub offers_minimum: bool,
}

impl ResizeRow {
    /// The size modes the row offers: Minimum only when a known minimum is below the size.
    pub fn mode_choices(&self) -> Vec<SizeMode> {
        let mut modes = vec![SizeMode::Original];
        if self.offers_minimum {
            modes.push(SizeMode::Minimum);
        }
        modes.push(SizeMode::Custom);
        modes
    }
}

/// One row of the preview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewRow {
    pub index: usize,
    pub old_start: String,
    pub old_size: String,
    pub new_start: String,
    pub new_size: String,
    pub action: String,
}

pub struct ResizeController {
    notify: Arc<dyn Notify>,
    log: Vec<(LogLevel, String)>,
    plan: Option<ResizePlan>,
    rows: Vec<ResizeRow>,
    preview: Option<Vec<PreviewRow>>,
    plan_error: Option<String>,
    device_risk_accepted: bool,
    job: Option<Job<Result<(), String>>>,
    /// The engine's lines, shared with the running write.
    lines: Arc<Mutex<Vec<String>>>,
    disk_changed: bool,
}

impl ResizeController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        ResizeController {
            notify,
            log: Vec::new(),
            plan: None,
            rows: Vec::new(),
            preview: None,
            plan_error: None,
            device_risk_accepted: false,
            job: None,
            lines: Arc::new(Mutex::new(Vec::new())),
            disk_changed: false,
        }
    }

    /// Lines for the log panel since the last call.
    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        std::mem::take(&mut self.log)
    }

    /// A write ended: the host reads the table again. Handed over once.
    pub fn take_disk_changed(&mut self) -> bool {
        std::mem::take(&mut self.disk_changed)
    }

    pub fn is_open(&self) -> bool {
        self.plan.is_some()
    }

    pub fn is_running(&self) -> bool {
        self.job.is_some()
    }

    pub fn progress(&self) -> Option<Snapshot> {
        self.job.as_ref().map(|j| j.snapshot())
    }

    pub fn is_device(&self) -> bool {
        self.plan.as_ref().is_some_and(|p| p.is_device)
    }

    pub fn device_risk_accepted(&self) -> bool {
        self.device_risk_accepted
    }

    /// The container's capacity, the bound the new layout must fit.
    pub fn disk_size(&self) -> u64 {
        self.plan.as_ref().map_or(0, |p| p.size_bytes)
    }

    /// The container could be enlarged first (Inspect's "Expand Image...").
    pub fn can_grow_container(&self) -> bool {
        self.plan.as_ref().is_some_and(|p| p.can_grow)
    }

    pub fn rows(&self) -> &[ResizeRow] {
        self.rows.as_slice()
    }

    pub fn preview(&self) -> Option<&[PreviewRow]> {
        self.preview.as_deref()
    }

    pub fn plan_error(&self) -> Option<&str> {
        self.plan_error.as_deref()
    }

    pub fn can_apply(&self) -> bool {
        self.job.is_none()
            && self.preview.is_some()
            && self.plan_error.is_none()
            && (!self.is_device() || self.device_risk_accepted)
    }

    /// The Current and After bars (the After bar reads the typed sizes; a half-typed number
    /// keeps the original), and the larger total both share as their scale.
    pub fn layout(&self) -> (LayoutBar, LayoutBar, u64) {
        let parts = self
            .plan
            .as_ref()
            .map_or(&[][..], |p| p.partitions.as_slice());
        let mut current = Vec::new();
        let mut after = Vec::new();
        for (color, p) in parts
            .iter()
            .filter(|p| !p.is_extended_container && !p.is_logical)
            .enumerate()
        {
            let planned = self
                .rows
                .iter()
                .find(|r| r.index == p.index)
                .map(|r| {
                    parsed_new_size(r.text.as_str(), r.original_size).unwrap_or(r.original_size)
                })
                .unwrap_or(p.size_bytes);
            let seg = |size_bytes| BarSegment {
                label: format!("Partition {}", p.index + 1),
                fs: p.type_name.clone(),
                size_bytes,
                color: Some(color),
            };
            current.push(seg(p.size_bytes));
            after.push(seg(planned));
        }
        let cur_total: u64 = current.iter().map(|s| s.size_bytes).sum();
        let after_total: u64 = after.iter().map(|s| s.size_bytes).sum();
        let fmt = disk_core::display::format_size;
        (
            LayoutBar {
                caption: "Current:".to_string(),
                segments: current,
                legend: false,
            },
            LayoutBar {
                caption: format!("After  ({} -> {}):", fmt(cur_total), fmt(after_total)),
                segments: after,
                legend: true,
            },
            cur_total.max(after_total).max(1),
        )
    }

    fn open(&mut self, plan: ResizePlan) {
        if self.job.is_some() {
            return;
        }
        let rows = plan
            .partitions
            .iter()
            .filter(|p| !p.is_logical)
            .map(|p| {
                let min = plan.min_sizes.get(&p.index).copied().unwrap_or(0).max(512);
                ResizeRow {
                    index: p.index,
                    type_name: p.type_name.clone(),
                    original_size: p.size_bytes,
                    minimum_size: min,
                    extended: p.is_extended_container,
                    mode: SizeMode::Original,
                    text: stamp_mib(p.size_bytes),
                    offers_minimum: min > 0 && min < p.size_bytes,
                }
            })
            .collect();
        *self = ResizeController {
            notify: Arc::clone(&self.notify),
            log: std::mem::take(&mut self.log),
            plan: Some(plan),
            rows,
            ..ResizeController::new(Arc::clone(&self.notify))
        };
    }

    fn row_mut(&mut self, index: usize) -> Option<&mut ResizeRow> {
        self.rows
            .iter_mut()
            .find(|r| r.index == index && !r.extended)
    }

    fn set_mode(&mut self, index: usize, mode: SizeMode) {
        let Some(row) = self.row_mut(index) else {
            return;
        };
        if mode == SizeMode::Minimum && !row.offers_minimum {
            return;
        }
        match mode {
            SizeMode::Original => row.text = stamp_mib(row.original_size),
            SizeMode::Minimum => row.text = stamp_mib(row.minimum_size),
            _ => {}
        }
        row.mode = mode;
    }

    fn set_text(&mut self, index: usize, text: String) {
        if let Some(row) = self.row_mut(index) {
            if row.mode == SizeMode::Custom {
                row.text = text;
            }
        }
    }

    fn partitions(&self) -> &[PartitionInfo] {
        self.plan
            .as_ref()
            .map_or(&[][..], |p| p.partitions.as_slice())
    }

    fn compute_preview(&mut self) {
        self.preview = None;
        self.plan_error = None;
        let mut desired = Vec::new();
        for row in self.rows.iter().filter(|r| !r.extended) {
            let Some(new_bytes) = parsed_new_size(row.text.as_str(), row.original_size) else {
                self.plan_error = Some(format!(
                    "Invalid size for partition {}: '{}'",
                    row.index, row.text
                ));
                return;
            };
            if row.minimum_size > 0 && new_bytes < row.minimum_size {
                self.plan_error = Some(format!(
                    "Partition {} new size ({}) is below minimum ({})",
                    row.index,
                    disk_core::display::format_size(new_bytes),
                    disk_core::display::format_size(row.minimum_size),
                ));
                return;
            }
            if new_bytes != row.original_size {
                desired.push((row.index, new_bytes));
            }
        }
        if desired.is_empty() {
            self.plan_error = Some("No size changes specified.".to_string());
            return;
        }
        let (align, disk) = self
            .plan
            .as_ref()
            .map_or((0, 0), |p| (p.alignment_sectors, p.size_bytes));
        match compute_resize_plan(self.partitions(), desired.as_slice(), align, disk) {
            Ok(plans) => {
                let fmt = disk_core::display::format_size;
                self.preview = Some(
                    plans
                        .iter()
                        .map(|p| {
                            let resized = p.new_size_bytes != p.old_size_bytes;
                            let action = match (resized, p.needs_data_move) {
                                (true, true) => "Resize + Move",
                                (true, false) => "Resize",
                                (false, true) => "Move",
                                (false, false) => "No change",
                            };
                            PreviewRow {
                                index: p.index,
                                old_start: format!("LBA {}", p.old_start_lba),
                                old_size: fmt(p.old_size_bytes),
                                new_start: format!("LBA {}", p.new_start_lba),
                                new_size: fmt(p.new_size_bytes),
                                action: action.to_string(),
                            }
                        })
                        .collect(),
                );
            }
            Err(e) => self.plan_error = Some(format!("{e:#}")),
        }
    }

    fn error(&mut self, msg: impl Into<String>) {
        self.log.push((LogLevel::Error, msg.into()));
    }

    /// Re-plan from the fields (the same parser as the preview) and start the write.
    fn start(&mut self) {
        if !self.can_apply() {
            return;
        }
        let mut desired = Vec::new();
        for row in self.rows.iter().filter(|r| !r.extended) {
            let Some(new_bytes) = parsed_new_size(row.text.as_str(), row.original_size) else {
                let msg = format!(
                    "Invalid size for partition {}: '{}' - nothing was written.",
                    row.index, row.text
                );
                self.error(msg);
                return;
            };
            if new_bytes != row.original_size {
                desired.push((row.index, new_bytes));
            }
        }
        // Refuse rather than "succeed" having written nothing.
        if desired.is_empty() {
            self.error("No size changes to apply.");
            return;
        }
        let Some(plan) = self.plan.as_ref() else {
            return;
        };
        let plans = match compute_resize_plan(
            plan.partitions.as_slice(),
            desired.as_slice(),
            plan.alignment_sectors,
            plan.size_bytes,
        ) {
            Ok(p) => p,
            Err(e) => {
                self.error(format!("Failed to compute resize plan: {e:#}"));
                return;
            }
        };
        let path: PathBuf = plan.path.clone();
        let table: PartitionTable = plan.table.clone();
        let is_device = plan.is_device;
        let disk_size = plan.size_bytes;
        let total = partition_resize::bytes_to_move(plans.as_slice());
        let sink = Arc::clone(&self.lines);
        self.log.push((
            LogLevel::Info,
            format!("Starting partition resize on {}...", path.display()),
        ));
        self.job = Some(Job::spawn(
            "Resizing",
            Arc::clone(&self.notify),
            move |ctx| {
                let _wake = crate::os::wakelock::acquire("Rusty Backup: partition resize");
                let status = Arc::new(Mutex::new(ResizeStatus {
                    finished: false,
                    error: None,
                    log_messages: Vec::new(),
                    current_bytes: 0,
                    total_bytes: total,
                    cancel_requested: false,
                }));
                let result = crate::model::worker::run_mirrored(
                    ctx,
                    &status,
                    |s, ctx| {
                        crate::model::worker::lock_status(&*sink).extend(s.log_messages.drain(..));
                        if s.total_bytes > 0 {
                            ctx.set_bytes(s.current_bytes, s.total_bytes);
                        }
                        if ctx.is_cancelled() {
                            s.cancel_requested = true;
                        }
                    },
                    || {
                        partition_resize::run_resize(
                            path.as_path(),
                            is_device,
                            plans.as_slice(),
                            &table,
                            disk_size,
                            &status,
                        )
                    },
                );
                Ok(result.map_err(|e| format!("{e:#}")))
            },
        ));
    }

    /// Cancel is asked for; the resize engine has no stop point, so it runs to the end.
    fn cancel(&mut self) {
        if let Some(job) = &self.job {
            job.cancel();
            self.log.push((
                LogLevel::Warning,
                "Resize cancellation requested...".to_string(),
            ));
        }
    }

    fn drain_lines(&mut self) {
        let lines = std::mem::take(&mut *crate::model::worker::lock_status(&*self.lines));
        self.log
            .extend(lines.into_iter().map(|l| (LogLevel::Info, l)));
    }

    fn poll(&mut self) {
        self.drain_lines();
        let Some(done) = self.job.as_ref().and_then(job_outcome) else {
            return;
        };
        self.job = None;
        self.drain_lines();
        match done.and_then(|r| r) {
            Ok(()) => self.log.push((
                LogLevel::Info,
                "Partition resize completed successfully. Close and re-inspect to see updates."
                    .to_string(),
            )),
            Err(e) => self.error(format!("Resize failed: {e}")),
        }
        self.disk_changed = true;
    }

    /// Block until the write ends; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        while self.job.is_some() {
            self.poll();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

impl Controller for ResizeController {
    type Action = ResizeAction;

    fn apply(&mut self, action: ResizeAction) -> Vec<Effect> {
        let edits = !matches!(
            action,
            ResizeAction::Open(_) | ResizeAction::Close | ResizeAction::Cancel
        );
        if edits && self.job.is_some() {
            return Vec::new();
        }
        match action {
            ResizeAction::Open(plan) => self.open(*plan),
            ResizeAction::Close => {
                if self.job.is_none() {
                    self.plan = None;
                }
            }
            ResizeAction::SetMode { index, mode } => self.set_mode(index, mode),
            ResizeAction::SetSizeText { index, text } => self.set_text(index, text),
            ResizeAction::AcceptDeviceRisk(on) => self.device_risk_accepted = on,
            ResizeAction::Preview => self.compute_preview(),
            ResizeAction::Apply => self.start(),
            ResizeAction::Cancel => self.cancel(),
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll();
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_size_row_offers_minimum_only_below_its_size() {
        let mut row = ResizeRow {
            index: 0,
            type_name: "FAT16".into(),
            original_size: 8 << 20,
            minimum_size: 512,
            extended: false,
            mode: SizeMode::Original,
            text: stamp_mib(8 << 20),
            offers_minimum: false,
        };
        assert_eq!(
            row.mode_choices(),
            vec![SizeMode::Original, SizeMode::Custom]
        );
        row.offers_minimum = true;
        assert_eq!(
            row.mode_choices(),
            vec![SizeMode::Original, SizeMode::Minimum, SizeMode::Custom]
        );
    }
}
