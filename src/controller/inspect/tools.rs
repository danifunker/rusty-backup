//! Disk and partition tools: Check / Repair, Make Bootable, the table editor, Expand Image,
//! Defragment, the word-order swap, Expand HFS's summary and CHD Info. Each slow step is a job.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use super::{purpose, push_log, HostRequest, InspectController, LogSink, Source};
use crate::controller::log::LogLevel;
use crate::controller::{ConfirmSpec, Effect};
use crate::fs::make_bootable::{
    Assessment, BootDiskKind, DriverSource, MakeBootableOptions, MakeBootableReport,
};
use crate::fs::{FsckResult, RepairReport};
use crate::model::fsck_runner::{FsckRun, FsckSource, FsckTarget};
use crate::model::job::{Job, JobCtx, Snapshot, Status};
use crate::model::partition_editor::PartitionEditor;
use crate::partition::{PartitionInfo, PartitionTable};
use disk_core::display::format_size;

/// A Make Bootable step: the assessment (for the image it read), or the write's report.
enum BootStep {
    Assessed(PathBuf, Result<Assessment, String>),
    Written(Result<MakeBootableReport, String>),
}

/// A partition-table write: the engine's messages and its error text.
type TableApply = (Vec<String>, Result<(), String>);

#[derive(Default)]
pub(super) struct ToolState {
    fsck: Option<Job<FsckRun>>,
    fsck_result: Option<FsckResult>,
    repair_report: Option<RepairReport>,
    fsck_open: bool,
    show_debug: bool,
    /// What a Repair rewrites: the checked partition's (offset, type byte, type string).
    repair_target: Option<(u64, u8, Option<String>)>,
    bootable: Option<Job<BootStep>>,
    /// The assessed image waiting for a boot-block donor.
    boot_pending: Option<PathBuf>,
    table_apply: Option<Job<TableApply>>,
    editor: PartitionEditor,
    editor_open: bool,
    expand: Option<Job<u64>>,
    expand_chd: bool,
    repack: Option<Job<()>>,
    pending_repack: Option<(u64, String)>,
    swab: Option<Job<()>>,
    hfs_summary: Option<Job<Result<crate::model::hfs_expand_runner::ExpandSource, String>>>,
    chd_info: Option<Job<Result<String, String>>>,
    chd_info_text: Option<String>,
}

impl ToolState {
    /// The source changed: results and questions about the old one go; running writes finish.
    pub fn reset(&mut self) {
        self.fsck_result = None;
        self.repair_report = None;
        self.fsck_open = false;
        self.repair_target = None;
        self.pending_repack = None;
        self.boot_pending = None;
    }

    pub fn repacking(&self) -> bool {
        self.repack.is_some()
    }

    pub fn running(&self) -> Option<&'static str> {
        if self.expand.is_some() && self.expand_chd {
            Some("Inspect: Expand CHD")
        } else if self.repack.is_some() {
            Some("Inspect: Repack")
        } else if self.swab.is_some() {
            Some("Inspect: Swab16")
        } else {
            None
        }
    }

    pub fn close_check(&mut self) {
        self.fsck_open = false;
    }

    pub fn show_debug(&mut self, on: bool) {
        self.show_debug = on;
    }

    pub fn close_chd_info(&mut self) {
        self.chd_info_text = None;
    }

    pub fn close_editor(&mut self) {
        self.editor_open = false;
    }
}

/// The Check results window.
pub struct FsckView<'a> {
    pub result: &'a FsckResult,
    pub repair: Option<&'a RepairReport>,
    pub show_debug: bool,
}

/// What the Resize Partitions dialog starts from.
pub struct ResizePlan {
    pub table: PartitionTable,
    pub partitions: Vec<PartitionInfo>,
    pub min_sizes: HashMap<usize, u64>,
    pub alignment_sectors: u64,
    /// The container's real capacity, never less than the table describes.
    pub size_bytes: u64,
    pub is_device: bool,
    /// A plain image file can be grown; a device or CHD cannot.
    pub can_grow: bool,
    pub path: PathBuf,
}

/// The Physical Disk Export window's source.
pub struct PhysicalSource {
    pub path: PathBuf,
    /// The decoded disk size (a container's file size is not its disk size).
    pub size_bytes: u64,
    pub fs_hint: String,
    pub has_partition_table: bool,
}

fn finished<R>(job: &Job<R>) -> Option<Result<R, String>> {
    match job.snapshot().status {
        Status::Running => None,
        Status::Done => Some(job.take_result().ok_or_else(|| "no result".to_string())),
        Status::Failed(e) => Some(Err(e)),
        Status::Cancelled => Some(Err("cancelled".to_string())),
    }
}

impl InspectController {
    // --- enablement ---------------------------------------------------------------------

    /// A local device or image with a real table: the editor, Add Partition and Resize work on it.
    pub fn can_edit_table(&self) -> bool {
        matches!(
            self.source,
            Some(Source::Device { .. }) | Some(Source::Image { .. })
        ) && self.found.clonezilla.is_none()
            && !matches!(self.found.table, None | Some(PartitionTable::None { .. }))
            && !self.export_running()
    }

    pub fn can_expand_image(&self) -> bool {
        self.is_image_file() && !self.export_running() && self.tools.expand.is_none()
    }

    pub fn expanding(&self) -> bool {
        self.tools.expand.is_some()
    }

    pub fn can_make_bootable(&self) -> bool {
        let mac = self.found.partitions.iter().any(|p| {
            crate::fs::is_classic_hfs(
                p.partition_type_byte,
                p.partition_type_string.as_deref(),
                p.type_name.as_str(),
            )
        });
        self.can_expand_image() && mac
    }

    /// The largest gap a new partition could take, never below the first partition (or LBA 2048).
    pub fn largest_gap(&mut self) -> Option<crate::partition::FreeRegion> {
        let disk_size = self.disk_size()?;
        let min_start = self
            .found
            .partitions
            .iter()
            .filter(|p| !p.is_extended_container)
            .map(|p| p.start_lba)
            .min()
            .unwrap_or(2048);
        crate::partition::largest_free_region(
            self.found.partitions.as_slice(),
            disk_size,
            min_start,
        )
    }

    /// Running tools a shell shows in small windows.
    pub fn tool_jobs(&self) -> Vec<Snapshot> {
        let fsck = self.tools.fsck.as_ref().map(|j| j.snapshot());
        let table = self.tools.table_apply.as_ref().map(|j| j.snapshot());
        let boot = self.tools.bootable.as_ref().map(|j| j.snapshot());
        let expand = self.tools.expand.as_ref().map(|j| j.snapshot());
        let repack = self.tools.repack.as_ref().map(|j| j.snapshot());
        let chd = self.tools.chd_info.as_ref().map(|j| j.snapshot());
        [fsck, table, boot, expand, repack, chd]
            .into_iter()
            .flatten()
            .collect()
    }

    pub fn swabbing(&self) -> bool {
        self.tools.swab.is_some()
    }

    pub fn fsck_view(&self) -> Option<FsckView<'_>> {
        if !self.tools.fsck_open {
            return None;
        }
        Some(FsckView {
            result: self.tools.fsck_result.as_ref()?,
            repair: self.tools.repair_report.as_ref(),
            show_debug: self.tools.show_debug,
        })
    }

    pub fn chd_info_text(&self) -> Option<&str> {
        self.tools.chd_info_text.as_deref()
    }

    pub fn editor_open(&self) -> bool {
        self.tools.editor_open
    }

    /// The editor, with the partitions and table it edits, for the editor window.
    pub fn editor_parts(
        &mut self,
    ) -> (
        &mut PartitionEditor,
        &[PartitionInfo],
        Option<&PartitionTable>,
    ) {
        (
            &mut self.tools.editor,
            self.found.partitions.as_slice(),
            self.found.table.as_ref(),
        )
    }

    // --- Check / Repair -----------------------------------------------------------------

    fn fsck_source(&self) -> Option<FsckSource> {
        #[cfg(feature = "remote")]
        if let Some(conn) = self.remote() {
            return Some(FsckSource::Remote(Arc::clone(&conn.0), conn.1.clone()));
        }
        self.disk_path().map(FsckSource::Path)
    }

    pub(super) fn check(&mut self, index: usize) {
        let Some(part) = self.found.partitions.iter().find(|p| p.index == index) else {
            return;
        };
        let target = (
            part.byte_offset(),
            Self::effective_type(part),
            part.partition_type_string.clone(),
        );
        if self.tools.fsck.is_some() {
            return;
        }
        self.tools.repair_target = Some(target.clone());
        self.tools.repair_report = None;
        let Some(source) = self.fsck_source() else {
            self.error("No source available for filesystem check");
            return;
        };
        let (offset, ptype, type_string) = target;
        let target = FsckTarget {
            source,
            offset,
            ptype,
            type_string,
        };
        self.tools.fsck = Some(self.spawn("Checking the filesystem...", move |job| {
            job.set_can_cancel(false);
            Ok(FsckRun {
                repair: None,
                check: crate::model::fsck_runner::check_target(&target)
                    .map_err(|e| format!("{e:#}")),
            })
        }));
    }

    pub(super) fn ask_repair(&mut self) -> Vec<Effect> {
        let count = self
            .tools
            .fsck_result
            .as_ref()
            .map(|r| r.errors.iter().filter(|e| e.repairable).count())
            .unwrap_or(0);
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::REPAIR,
            title: "Repair Filesystem?".into(),
            message: format!(
                "This will attempt to fix {count} repairable error(s). Unrepairable issues \
                 (B-tree structural damage) will be skipped.\n\nContinue?"
            ),
            confirm_label: "OK".into(),
            destructive: true,
        })]
    }

    pub(super) fn repair(&mut self) {
        let Some((offset, ptype, type_string)) = self.tools.repair_target.clone() else {
            self.error("No repair context available");
            return;
        };
        if self.tools.fsck.is_some() {
            return;
        }
        let Some(source) = self.fsck_source() else {
            self.error("No source available for repair");
            return;
        };
        let target = FsckTarget {
            source,
            offset,
            ptype,
            type_string,
        };
        self.tools.fsck = Some(self.spawn("Repairing the filesystem...", move |job| {
            job.set_can_cancel(false);
            crate::model::fsck_runner::repair_and_recheck(&target)
        }));
    }

    fn fsck_done(&mut self, run: Result<FsckRun, String>) {
        let run = match run {
            Ok(run) => run,
            Err(e) => {
                self.error(e);
                return;
            }
        };
        if let Some(report) = run.repair {
            self.info(format!(
                "Repair complete: {} fix(es) applied, {} failed",
                report.fixes_applied.len(),
                report.fixes_failed.len(),
            ));
            self.tools.repair_report = Some(report);
        }
        match run.check {
            Ok(Some(result)) => {
                if result.is_clean() {
                    self.info(format!(
                        "Filesystem check: clean ({} files, {} dirs)",
                        result.stats.files_checked, result.stats.directories_checked,
                    ));
                } else {
                    let warns = result.warnings.iter().filter(|w| !w.debug).count();
                    self.warn(format!(
                        "Filesystem check: {} error(s), {} warning(s)",
                        result.errors.len(),
                        warns,
                    ));
                }
                self.tools.fsck_result = Some(result);
                self.tools.fsck_open = true;
            }
            Ok(None) => self.info("Filesystem check not supported for this filesystem type"),
            Err(e) => self.error(e),
        }
    }

    // --- Make Bootable ------------------------------------------------------------------

    pub(super) fn make_bootable(&mut self) {
        let Some(path) = self.image_path().map(|p| p.to_path_buf()) else {
            return;
        };
        if self.tools.bootable.is_some() {
            return;
        }
        self.tools.bootable = Some(self.spawn(
            "Checking what the disk needs to boot...",
            move |job| {
                job.set_can_cancel(false);
                let assessed = crate::fs::make_bootable::assess_bootability(path.as_path())
                    .map_err(|e| e.to_string());
                Ok(BootStep::Assessed(path, assessed))
            },
        ));
    }

    fn boot_step(&mut self, step: Result<BootStep, String>) -> Vec<Effect> {
        match step {
            Err(e) | Ok(BootStep::Written(Err(e))) => {
                self.error(format!("Make Bootable failed: {e}"))
            }
            Ok(BootStep::Assessed(_, Err(e))) => self.error(format!("Make Bootable: {e}")),
            Ok(BootStep::Assessed(path, Ok(a))) => {
                let kind = match a.kind {
                    BootDiskKind::FlatHfs => "flat HFS image (kept flat)",
                    BootDiskKind::ApmHfs => "full APM disk",
                };
                self.info(format!(
                    "Make Bootable: detected {kind} - boot blocks {}, System Folder {}",
                    if a.has_boot_blocks {
                        "present"
                    } else {
                        "absent"
                    },
                    match &a.blessed_folder {
                        Some(n) => format!("blessed ({n})"),
                        None => "not blessed".to_string(),
                    }
                ));
                if a.is_bootable() {
                    self.info("Make Bootable: disk is already bootable; nothing to do.");
                } else if a.has_boot_blocks {
                    self.write_bootable(path, None);
                } else {
                    // Boot blocks cannot be synthesised; they come from a donor disk.
                    self.tools.boot_pending = Some(path);
                    return vec![Effect::PickFile(crate::controller::PickSpec {
                        purpose: purpose::BOOT_DONOR,
                        title: "Boot blocks needed - pick a bootable donor disk".into(),
                        filters: vec![(
                            "Disk images".into(),
                            ["dsk", "hfv", "img", "hda", "raw"]
                                .iter()
                                .map(|s| s.to_string())
                                .collect(),
                        )],
                        save: false,
                        file_name: String::new(),
                    })];
                }
            }
            Ok(BootStep::Written(Ok(report))) => {
                for s in &report.applied {
                    self.info(format!("Make Bootable: {s}"));
                }
                for s in &report.skipped {
                    self.info(format!("Make Bootable: skip - {s}"));
                }
                for s in &report.still_missing {
                    self.warn(format!("Make Bootable: MISSING - {s}"));
                }
                if report.now_bootable {
                    self.info("Make Bootable: disk is now bootable.");
                } else {
                    self.warn("Make Bootable: disk is not yet bootable (see MISSING above).");
                }
                self.reinspect();
            }
        }
        Vec::new()
    }

    pub(super) fn boot_donor_picked(&mut self, donor: Option<PathBuf>) {
        let Some(path) = self.tools.boot_pending.take() else {
            return;
        };
        match donor {
            Some(d) => self.write_bootable(path, Some(d)),
            None => self.info("Make Bootable: cancelled (boot blocks need a donor disk)."),
        }
    }

    fn write_bootable(&mut self, path: PathBuf, donor: Option<PathBuf>) {
        let opts = MakeBootableOptions {
            donor,
            driver: DriverSource::Builtin,
            bless_path: None,
            dry_run: false,
        };
        self.tools.bootable = Some(self.spawn("Making the disk bootable...", move |job| {
            job.set_can_cancel(false);
            Ok(BootStep::Written(
                crate::fs::make_bootable::make_bootable(path.as_path(), &opts)
                    .map_err(|e| e.to_string()),
            ))
        }));
    }

    // --- the partition-table editor -----------------------------------------------------

    pub(super) fn edit_table(&mut self) {
        let disk_size = self.disk_size();
        self.tools.editor.seed_from_with_minimums(
            self.found.partitions.as_slice(),
            &self.found.min_sizes,
            disk_size,
        );
        self.tools.editor_open = true;
    }

    /// The editor, pre-filled to add one partition filling the largest gap.
    pub(super) fn add_partition(&mut self) {
        let Some(gap) = self.largest_gap() else {
            return;
        };
        self.edit_table();
        let default_type = match self.found.table.as_ref() {
            Some(PartitionTable::Sgi(_)) => "XFS",
            Some(PartitionTable::Apm(_)) => "Apple_HFS",
            Some(PartitionTable::Gpt { .. }) => "0FC63DAF-8483-4772-8E79-3D69D8477DE4",
            _ => "83",
        };
        let ed = &mut self.tools.editor;
        ed.add_start_lba = gap.start_lba.to_string();
        ed.add_size_mb = format!("{}", (gap.size_bytes / (1024 * 1024)).max(1));
        ed.add_type = default_type.to_string();
        ed.add_bootable = false;
    }

    pub(super) fn apply_table_edits(&mut self) {
        let Some(table) = self.found.table.clone() else {
            self.error("No partition table loaded");
            return;
        };
        let Some(path) = self.disk_path() else {
            self.error("No source path for editing");
            return;
        };
        if self.tools.table_apply.is_some() {
            return;
        }
        let edits = self.tools.editor.edits.clone();
        let disk_size = self.tools.editor.disk_size;
        let is_device = self.device_path().is_some();
        self.info(format!(
            "Applying {} partition table edit(s) to {}...",
            edits.len(),
            path.display()
        ));
        self.tools.table_apply = Some(self.spawn("Writing the partition table...", move |job| {
            job.set_can_cancel(false);
            Ok(crate::model::partition_editor::apply_to_source(
                path.as_path(),
                is_device,
                &table,
                edits.as_slice(),
                disk_size,
            ))
        }));
    }

    fn table_applied(&mut self, out: Result<TableApply, String>) {
        let result = match out {
            Ok((messages, result)) => {
                for msg in messages {
                    self.info(msg);
                }
                result
            }
            Err(e) => Err(e),
        };
        match result {
            Ok(()) => {
                self.info("Partition table updated successfully.");
                self.tools.editor.status = Some(
                    "Changes applied successfully! Close and re-inspect to see updates."
                        .to_string(),
                );
                self.tools.editor.edits.clear();
                // Add Partition and the editors would otherwise work from the old table.
                self.reinspect();
            }
            Err(e) => {
                self.error(format!("Failed to apply edits: {e}"));
                self.tools.editor.status = Some(format!("Error: {e}"));
            }
        }
    }

    pub fn resize_plan(&self) -> Option<ResizePlan> {
        let table = self.found.table.clone()?;
        let path = self.disk_path()?;
        let is_device = self.device_path().is_some();
        let partition_end = self
            .found
            .partitions
            .iter()
            .map(|p| p.byte_offset() + p.size_bytes)
            .max()
            .unwrap_or(0);
        // The container's capacity, not the last partition's end, so slack can be grown into.
        let size = if is_device {
            self.device_size().unwrap_or(partition_end)
        } else {
            std::fs::metadata(path.as_path())
                .map(|m| m.len())
                .unwrap_or(partition_end)
        }
        .max(partition_end);
        Some(ResizePlan {
            table,
            partitions: self.found.partitions.clone(),
            min_sizes: self.found.min_sizes.clone(),
            alignment_sectors: self
                .found
                .alignment
                .as_ref()
                .map(|a| a.alignment_sectors)
                .unwrap_or(0),
            size_bytes: size,
            is_device,
            can_grow: !is_device && self.found.chd_path.is_none(),
            path,
        })
    }

    /// The device size the shell knows for the open device.
    pub fn device_size(&self) -> Option<u64> {
        match &self.source {
            Some(Source::Device { identity, .. }) => Some(identity.size_bytes),
            _ => None,
        }
    }

    pub fn physical_source(&self) -> Option<PhysicalSource> {
        let path = self.image_path()?.to_path_buf();
        let size_bytes = crate::model::source_reader::decoded_image_size(path.as_path())
            .unwrap_or_else(|| {
                std::fs::metadata(path.as_path())
                    .map(|m| m.len())
                    .unwrap_or(0)
            });
        let (fs_hint, has_partition_table) = match &self.found.table {
            Some(PartitionTable::None { fs_hint, .. }) => (fs_hint.clone(), false),
            Some(_) => ("partitioned".to_string(), true),
            None => ("Unknown".to_string(), false),
        };
        Some(PhysicalSource {
            path,
            size_bytes,
            fs_hint,
            has_partition_table,
        })
    }

    // --- Expand Image -------------------------------------------------------------------

    pub(super) fn expand_image(&mut self, add_bytes: u64) {
        let Some(path) = self.image_path().map(|p| p.to_path_buf()) else {
            self.error("Expand Image: no image file loaded");
            return;
        };
        if add_bytes == 0 || self.tools.expand.is_some() {
            return;
        }
        let is_chd = self.found.chd_path.is_some();
        self.info(format!(
            "Expanding image '{}' by {}...",
            path.display(),
            format_size(add_bytes),
        ));
        let sink = Arc::clone(&self.log);
        self.tools.expand_chd = is_chd;
        self.tools.expand = Some(self.spawn("Expanding the image...", move |job| {
            if is_chd {
                expand_chd_in_job(path, add_bytes, &sink, job)
            } else {
                job.set_can_cancel(false);
                let mut log = |m: &str| push_log(&sink, LogLevel::Info, m);
                Ok(crate::partition::resize::expand_image_file(
                    path.as_path(),
                    add_bytes,
                    &mut log,
                )?)
            }
        }));
    }

    fn expanded(&mut self, out: Result<u64, String>) {
        let chd = self.tools.expand_chd;
        match out {
            Ok(_) if chd => self.info("CHD expansion complete. Click Re-inspect to refresh."),
            Ok(total) => self.info(format!(
                "Image expanded to {}. Click Re-inspect to refresh.",
                format_size(total),
            )),
            Err(e) if chd => self.error(format!("Expand Image (CHD) failed: {e}")),
            Err(e) => self.error(format!("Expand Image failed: {e}")),
        }
        self.found.disk_size = None;
    }

    // --- Defragment (Human68k) ----------------------------------------------------------

    pub(super) fn ask_defragment(&mut self, index: usize) -> Vec<Effect> {
        let Some(part) = self.found.partitions.iter().find(|p| p.index == index) else {
            return Vec::new();
        };
        if !self.row_actions(part).defragment {
            return Vec::new();
        }
        let label = part.type_name.clone();
        self.tools.pending_repack = Some((part.byte_offset(), label.clone()));
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::DEFRAGMENT,
            title: "Defragment Human68k partition".into(),
            message: format!(
                "Defragment \"{label}\"?\n\nRepacks the volume so files are stored contiguously, \
                 reclaiming holes left by deleted files.\n\nThe partition is rewritten in place. \
                 Back up the image first."
            ),
            confirm_label: "Defragment".into(),
            destructive: true,
        })]
    }

    pub(super) fn defragment_answered(&mut self, yes: bool) {
        let Some((offset, _)) = self.tools.pending_repack.take() else {
            return;
        };
        if !yes {
            return;
        }
        let Some(path) = self.image_path().map(|p| p.to_path_buf()) else {
            self.error("No image file loaded to defragment.");
            return;
        };
        // The browse view's handles go first so the repack can rewrite the file.
        self.requests.push(HostRequest::CloseBrowse);
        self.info("Starting Human68k defragment (repack)...");
        let sink = Arc::clone(&self.log);
        self.tools.repack = Some(self.spawn("Defragmenting...", move |job| {
            job.set_can_cancel(false);
            let status = crate::model::repack_runner::spawn(path, offset);
            let mut err = None;
            crate::model::worker::wait_spawned(job, &status, |s, ctx| {
                drain(&sink, &mut s.log_messages);
                ctx.set_bytes(s.current_bytes, s.total_bytes);
                if s.finished {
                    err = s.error.take();
                }
                s.finished
            });
            match err {
                Some(e) => Err(anyhow::anyhow!(e)),
                None => Ok(()),
            }
        }));
    }

    // --- word-order swap ----------------------------------------------------------------

    pub(super) fn ask_swab(&mut self) -> Vec<Effect> {
        let Some(path) = self.image_path() else {
            self.error("Word-order swap needs a loaded image file (not a device).");
            return Vec::new();
        };
        let name = path
            .file_stem()
            .map(|s| format!("{}-swab16.img", s.to_string_lossy()))
            .unwrap_or_else(|| "swab16.img".to_string());
        let mut spec = Self::picker(
            purpose::SWAB_DEST,
            "Save the word-swapped image",
            true,
            name,
        );
        spec.filters = vec![(
            "Disk image".into(),
            vec!["img".into(), "raw".into(), "hd".into()],
        )];
        vec![Effect::PickFile(spec)]
    }

    pub(super) fn swab_picked(&mut self, dest: Option<PathBuf>) {
        let (Some(dest), Some(path)) = (dest, self.image_path().map(|p| p.to_path_buf())) else {
            return;
        };
        if dest == path {
            self.error("Pick a different destination; `rb-cli swab16 --in-place` rewrites a file.");
            return;
        }
        if self.tools.swab.is_some() {
            return;
        }
        let sink = Arc::clone(&self.log);
        self.tools.swab = Some(self.spawn("Swapping word order...", move |job| {
            job.set_can_cancel(false);
            let status = crate::model::swab_runner::spawn(path, Some(dest));
            let mut err = None;
            crate::model::worker::wait_spawned(job, &status, |s, ctx| {
                drain(&sink, &mut s.log_messages);
                ctx.set_bytes(s.current_bytes, s.total_bytes);
                if s.finished {
                    err = s.error.take();
                }
                s.finished
            });
            match err {
                Some(e) => Err(anyhow::anyhow!(e)),
                None => Ok(()),
            }
        }));
    }

    // --- Expand HFS and CHD Info --------------------------------------------------------

    pub(super) fn expand_hfs(&mut self, index: usize) {
        let Some(part) = self.found.partitions.iter().find(|p| p.index == index) else {
            return;
        };
        let (offset, size) = (part.byte_offset(), part.size_bytes);
        let Some(path) = self.disk_path() else {
            self.error("No source available for HFS expand");
            return;
        };
        if self.tools.hfs_summary.is_some() {
            return;
        }
        self.tools.hfs_summary = Some(self.spawn("Reading the HFS volume...", move |_| {
            Ok(
                crate::model::hfs_expand_runner::summarize_source(path.as_path(), offset, size)
                    .map_err(|e| e.to_string()),
            )
        }));
    }

    pub(super) fn chd_info(&mut self) {
        let Some(path) = self.found.chd_path.clone() else {
            return;
        };
        if self.tools.chd_info.is_some() {
            return;
        }
        self.tools.chd_info = Some(self.spawn("Reading the CHD header...", move |job| {
            job.set_can_cancel(false);
            Ok(crate::rbformats::chd::format_chd_info(path.as_path()).map_err(|e| format!("{e}")))
        }));
    }

    // --- polling ------------------------------------------------------------------------

    pub(super) fn poll_tools(&mut self) -> Vec<Effect> {
        let mut fx = Vec::new();
        if let Some(done) = self.tools.fsck.as_ref().and_then(finished) {
            self.tools.fsck = None;
            self.fsck_done(done);
        }
        if let Some(done) = self.tools.bootable.as_ref().and_then(finished) {
            self.tools.bootable = None;
            fx.extend(self.boot_step(done));
        }
        if let Some(done) = self.tools.table_apply.as_ref().and_then(finished) {
            self.tools.table_apply = None;
            self.table_applied(done);
        }
        if let Some(done) = self.tools.expand.as_ref().and_then(finished) {
            self.tools.expand = None;
            self.expanded(done);
        }
        if let Some(done) = self.tools.repack.as_ref().and_then(finished) {
            self.tools.repack = None;
            match done {
                Ok(()) => {
                    self.info("Defragment complete.");
                    // The layout inside the partition changed.
                    self.reinspect();
                }
                Err(e) => self.error(format!("Defragment failed: {e}")),
            }
        }
        if let Some(done) = self.tools.swab.as_ref().and_then(finished) {
            self.tools.swab = None;
            match done {
                Ok(()) => self.info("Word-order swap complete."),
                Err(e) => self.error(format!("Word-order swap failed: {e}")),
            }
        }
        if let Some(done) = self.tools.hfs_summary.as_ref().and_then(finished) {
            self.tools.hfs_summary = None;
            match done.and_then(|r| r) {
                Ok(source) => self.requests.push(HostRequest::ExpandHfs(Box::new(source))),
                Err(e) => self.error(format!("Cannot read source HFS volume: {e}")),
            }
        }
        if let Some(done) = self.tools.chd_info.as_ref().and_then(finished) {
            self.tools.chd_info = None;
            match done.and_then(|r| r) {
                Ok(text) => self.tools.chd_info_text = Some(text),
                Err(e) => self.error(format!("CHD Info failed: {e}")),
            }
        }
        fx
    }
}

/// Move a runner's log lines into the log panel.
pub(super) fn drain(sink: &LogSink, lines: &mut Vec<String>) {
    if lines.is_empty() {
        return;
    }
    let mut out = crate::model::worker::lock_status(&**sink);
    out.extend(lines.drain(..).map(|m| (LogLevel::Info, m)));
}

#[cfg(feature = "chd")]
fn expand_chd_in_job(
    path: PathBuf,
    add_bytes: u64,
    sink: &LogSink,
    job: &JobCtx,
) -> anyhow::Result<u64> {
    let status = crate::model::chd_expand_runner::spawn(path, add_bytes);
    let mut err = None;
    crate::model::worker::wait_spawned(job, &status, |s, ctx| {
        drain(sink, &mut s.log_messages);
        ctx.set_bytes(s.current_bytes, s.total_bytes);
        if ctx.is_cancelled() {
            s.cancel_requested = true;
        }
        if s.finished {
            err = s.error.take();
        }
        s.finished
    });
    match err {
        Some(e) => Err(anyhow::anyhow!(e)),
        None => Ok(0),
    }
}

#[cfg(not(feature = "chd"))]
fn expand_chd_in_job(_: PathBuf, _: u64, _: &LogSink, _: &JobCtx) -> anyhow::Result<u64> {
    anyhow::bail!("CHD support is not built in")
}
