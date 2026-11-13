//! Resize Image: one job that copies an image file (or works on it in place), sets its disk size,
//! resizes its partitions and their filesystems, and checks them again.
//!
//! The steps are the existing pieces in order: [`crate::partition::resize::expand_image_file`]
//! grows the file, [`crate::partition::resize::apply_resize`] moves and resizes the entries and
//! patches each filesystem, and the table is written once more at the final disk size so a GPT's
//! backup lands at the new end. `controller::resize_image` drives it; `rb-cli resize-image` runs
//! it headless. See docs/resize_image_wizard_plan.md.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::model::job::JobCtx;
use crate::partition::resize::{apply_resize, compute_resize_plan, detect_vhd};
use crate::partition::{PartitionInfo, PartitionTable};
use disk_core::display::format_size;

const SECTOR: u64 = 512;
const COPY_CHUNK: usize = 1024 * 1024;

/// Bytes a table keeps after its last partition: GPT's backup entries and header.
pub fn table_tail(table: &PartitionTable) -> u64 {
    match table {
        PartitionTable::Gpt { .. } => 33 * SECTOR,
        _ => 0,
    }
}

/// The tables whose own records hold the disk size, so a new size means writing them again.
fn records_disk_size(table: &PartitionTable) -> bool {
    matches!(
        table,
        PartitionTable::Gpt { .. } | PartitionTable::Apm(_) | PartitionTable::Next(_)
    )
}

/// One partition's filesystem check, as both checks (before and after) report it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Clean,
    /// Errors found; `repairable` when Repair can fix at least one.
    Errors {
        count: usize,
        repairable: bool,
    },
    /// This filesystem has no checker.
    NoChecker,
    /// The check itself could not run.
    Failed(String),
}

impl Verdict {
    pub fn text(&self) -> String {
        match self {
            Verdict::Clean => "Clean".to_string(),
            Verdict::Errors { count, repairable } => match repairable {
                true => format!("{count} error(s), repairable"),
                false => format!("{count} error(s)"),
            },
            Verdict::NoChecker => "No checker for this filesystem".to_string(),
            Verdict::Failed(e) => format!("Check failed: {e}"),
        }
    }

    /// Nothing found that should stop a resize.
    pub fn passes(&self) -> bool {
        matches!(self, Verdict::Clean | Verdict::NoChecker)
    }
}

/// The type byte a check opens a partition with (a FAT entry may carry none).
pub fn check_type(part: &PartitionInfo) -> u8 {
    match part.partition_type_byte {
        0 => crate::model::backup_loader::infer_fat_type_byte(part.type_name.as_str()),
        b => b,
    }
}

/// Check the partition at `offset` of `path`, read-only.
pub fn check_partition(path: &Path, offset: u64, part: &PartitionInfo) -> Verdict {
    let ts = part.partition_type_string.as_deref();
    match crate::model::fsck_runner::run_fsck(path, offset, check_type(part), ts) {
        Ok(None) => Verdict::NoChecker,
        Ok(Some(r)) if r.errors.is_empty() => Verdict::Clean,
        Ok(Some(r)) => Verdict::Errors {
            count: r.errors.len(),
            repairable: r.repairable,
        },
        Err(e) => Verdict::Failed(format!("{e:#}")),
    }
}

/// Repair the partition at `offset` of `path` in place, then check it again.
pub fn repair_partition(path: &Path, offset: u64, part: &PartitionInfo) -> Verdict {
    let ts = part.partition_type_string.as_deref();
    if let Err(e) = crate::model::fsck_runner::run_repair(path, offset, check_type(part), ts) {
        return Verdict::Failed(format!("repair: {e:#}"));
    }
    check_partition(path, offset, part)
}

/// What one resize writes.
#[derive(Debug, Clone)]
pub struct ResizeImageRequest {
    pub source: PathBuf,
    /// Write a resized copy here; `None` changes `source` itself.
    pub copy_to: Option<PathBuf>,
    pub table: PartitionTable,
    pub partitions: Vec<PartitionInfo>,
    pub alignment_sectors: u64,
    /// The disk's new size in bytes (a VHD's footer is not counted).
    pub disk_size: u64,
    /// `(partition index, new size in bytes)` for each partition that changes.
    pub sizes: Vec<(usize, u64)>,
    /// Check every resized or moved filesystem once the write is done.
    pub check_after: bool,
}

impl ResizeImageRequest {
    /// The file the steps write to.
    pub fn target(&self) -> &Path {
        self.copy_to.as_deref().unwrap_or(self.source.as_path())
    }
}

/// The steps of one resize, in the order they run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Copy,
    Grow,
    Partitions,
    Finish,
    Check,
}

impl Step {
    pub fn label(self) -> &'static str {
        match self {
            Step::Copy => "Copy the image",
            Step::Grow => "Grow the file",
            Step::Partitions => "Move and resize the partitions",
            Step::Finish => "Set the disk size",
            Step::Check => "Check the volumes again",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepState {
    Pending,
    Running,
    Done(String),
    Failed(String),
}

/// What a running resize has done so far, shared with the controller.
#[derive(Debug, Default)]
pub struct Progress {
    pub steps: Vec<(Step, StepState)>,
    pub lines: Vec<String>,
    /// The check after the write: `(partition index, verdict)`.
    pub checks: Vec<(usize, Verdict)>,
}

pub type SharedProgress = Arc<Mutex<Progress>>;

/// Where everything stands before the write: the plan the steps carry out.
#[derive(Debug, Clone)]
pub struct Layout {
    pub old_disk_size: u64,
    pub is_vhd: bool,
    pub plans: Vec<crate::partition::resize::PartitionResizePlan>,
    /// Where the last partition ends once resized.
    pub data_end: u64,
}

/// Plan `req` against the image as it is now; every refusal is found here, before a write.
pub fn plan(req: &ResizeImageRequest) -> Result<Layout> {
    let mut file = std::fs::File::open(&req.source)
        .with_context(|| format!("open {}", req.source.display()))?;
    let len = file.seek(SeekFrom::End(0))?;
    let is_vhd = detect_vhd(&mut file, len);
    let old_disk_size = match is_vhd {
        true => len - SECTOR,
        false => len,
    };
    if req.disk_size % SECTOR != 0 {
        bail!("the disk size must be a whole number of 512-byte sectors");
    }
    let room = old_disk_size.max(req.disk_size);
    let plans = match req.sizes.is_empty() {
        true => Vec::new(),
        false => compute_resize_plan(
            req.partitions.as_slice(),
            req.sizes.as_slice(),
            req.alignment_sectors,
            room,
        )?,
    };
    let data_end = req
        .partitions
        .iter()
        .filter(|p| !p.is_logical)
        .map(|p| match plans.iter().find(|r| r.index == p.index) {
            Some(r) => r.new_start_lba * SECTOR + r.new_size_bytes,
            None => p.byte_offset() + p.size_bytes,
        })
        .max()
        .unwrap_or(0);
    let needed = data_end + table_tail(&req.table);
    if req.disk_size < needed {
        bail!(
            "the partitions need {} but the disk would be {}",
            format_size(needed),
            format_size(req.disk_size)
        );
    }
    if plans.is_empty() && req.disk_size == old_disk_size {
        bail!("nothing to change: the disk and every partition keep their size");
    }
    rehearse_shrinks(&mut file, &plans)?;
    Ok(Layout {
        old_disk_size,
        is_vhd,
        plans,
        data_end,
    })
}

/// Shrink each shrinking filesystem on an overlay first: the engine writes the table before it
/// patches a filesystem, so a refusal found there would leave the disk half-written.
fn rehearse_shrinks(
    file: &mut std::fs::File,
    plans: &[crate::partition::resize::PartitionResizePlan],
) -> Result<()> {
    for p in plans.iter().filter(|p| p.new_size_bytes < p.old_size_bytes) {
        let mut overlay = Overlay::new(&mut *file);
        let mut quiet = |_: &str| {};
        let offset = p.old_start_lba * SECTOR;
        crate::fs::resize_filesystem_for(&mut overlay, offset, p.new_size_bytes, &mut quiet)
            .with_context(|| {
                format!(
                    "partition {} cannot shrink to {}",
                    p.index + 1,
                    format_size(p.new_size_bytes)
                )
            })?;
    }
    Ok(())
}

/// Reads fall through to the file; writes stay in memory, a sector at a time.
struct Overlay<'a> {
    file: &'a mut std::fs::File,
    pos: u64,
    sectors: std::collections::HashMap<u64, Vec<u8>>,
}

impl<'a> Overlay<'a> {
    fn new(file: &'a mut std::fs::File) -> Self {
        Overlay {
            file,
            pos: 0,
            sectors: std::collections::HashMap::new(),
        }
    }

    /// Sector `n` as the rehearsal sees it (past the file's end reads as zeros).
    fn sector(&mut self, n: u64) -> std::io::Result<Vec<u8>> {
        if let Some(s) = self.sectors.get(&n) {
            return Ok(s.clone());
        }
        let mut buf = vec![0u8; SECTOR as usize];
        self.file.seek(SeekFrom::Start(n * SECTOR))?;
        let mut got = 0;
        while got < buf.len() {
            match self.file.read(&mut buf[got..])? {
                0 => break,
                k => got += k,
            }
        }
        Ok(buf)
    }
}

impl Read for Overlay<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let (n, at) = (self.pos / SECTOR, (self.pos % SECTOR) as usize);
        let sector = self.sector(n)?;
        let k = out.len().min(sector.len() - at);
        out[..k].copy_from_slice(&sector[at..at + k]);
        self.pos += k as u64;
        Ok(k)
    }
}

impl Write for Overlay<'_> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let (n, at) = (self.pos / SECTOR, (self.pos % SECTOR) as usize);
        let mut sector = self.sector(n)?;
        let k = data.len().min(sector.len() - at);
        sector[at..at + k].copy_from_slice(&data[..k]);
        self.sectors.insert(n, sector);
        self.pos += k as u64;
        Ok(k)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for Overlay<'_> {
    fn seek(&mut self, to: SeekFrom) -> std::io::Result<u64> {
        let end = self.file.metadata()?.len();
        self.pos = match to {
            SeekFrom::Start(n) => n,
            SeekFrom::End(d) => (end as i64 + d).max(0) as u64,
            SeekFrom::Current(d) => (self.pos as i64 + d).max(0) as u64,
        };
        Ok(self.pos)
    }
}

/// The steps `req` runs, in order.
pub fn steps(req: &ResizeImageRequest, layout: &Layout) -> Vec<Step> {
    let mut steps = Vec::new();
    if req.copy_to.is_some() {
        steps.push(Step::Copy);
    }
    if req.disk_size > layout.old_disk_size {
        steps.push(Step::Grow);
    }
    if !layout.plans.is_empty() {
        steps.push(Step::Partitions);
    }
    // A VHD resize writes a footer at the last partition's end, which Finish clears.
    if req.disk_size != layout.old_disk_size || (layout.is_vhd && !layout.plans.is_empty()) {
        steps.push(Step::Finish);
    }
    if req.check_after {
        steps.push(Step::Check);
    }
    steps
}

fn set_state(shared: &SharedProgress, step: Step, state: StepState) {
    let mut p = crate::model::worker::lock_status(&**shared);
    if let Some(s) = p.steps.iter_mut().find(|(s, _)| *s == step) {
        s.1 = state;
    }
}

fn line(shared: &SharedProgress, text: &str) {
    crate::model::worker::lock_status(&**shared)
        .lines
        .push(text.to_string());
}

/// Run every step of `req`; a failed step stops the rest, and a failed copy is removed.
pub fn run(req: &ResizeImageRequest, ctx: &JobCtx, shared: &SharedProgress) -> Result<()> {
    let layout = plan(req)?;
    let steps = steps(req, &layout);
    crate::model::worker::lock_status(&**shared).steps =
        steps.iter().map(|s| (*s, StepState::Pending)).collect();
    for step in steps {
        if ctx.is_cancelled() {
            bail!("cancelled before \"{}\"", step.label());
        }
        set_state(shared, step, StepState::Running);
        ctx.set_phase(step.label());
        match run_step(step, req, &layout, ctx, shared) {
            Ok(note) => set_state(shared, step, StepState::Done(note)),
            Err(e) => {
                let msg = format!("{e:#}");
                set_state(shared, step, StepState::Failed(msg.clone()));
                if step == Step::Copy {
                    let _ = std::fs::remove_file(req.target());
                }
                bail!("{}: {msg}", step.label());
            }
        }
    }
    Ok(())
}

fn run_step(
    step: Step,
    req: &ResizeImageRequest,
    layout: &Layout,
    ctx: &JobCtx,
    shared: &SharedProgress,
) -> Result<String> {
    let target = req.target();
    let mut log = |m: &str| line(shared, m);
    match step {
        Step::Copy => {
            copy_file(req.source.as_path(), target, ctx)?;
            Ok(format!("to {}", target.display()))
        }
        Step::Grow => {
            let add = req.disk_size - layout.old_disk_size;
            crate::partition::resize::expand_image_file(target, add, &mut log)?;
            Ok(format!("{} added", format_size(add)))
        }
        Step::Partitions => {
            let mut file = open_rw(target)?;
            let len = file.seek(SeekFrom::End(0))?;
            let is_vhd = detect_vhd(&mut file, len);
            ctx.set_can_cancel(false);
            apply_resize(
                &mut file,
                layout.plans.as_slice(),
                &req.table,
                false,
                is_vhd,
                len,
                &mut |done, total| ctx.set_bytes(done, total),
                &mut log,
            )?;
            ctx.set_can_cancel(true);
            let changed = layout
                .plans
                .iter()
                .filter(|p| p.new_size_bytes != p.old_size_bytes || p.needs_data_move)
                .count();
            Ok(format!("{changed} partition(s) changed"))
        }
        Step::Finish => {
            finish(target, req, layout, &mut log)?;
            Ok(format_size(req.disk_size))
        }
        Step::Check => {
            let checks = check_after(target, req, layout);
            let bad = checks.iter().filter(|(_, v)| !v.passes()).count();
            crate::model::worker::lock_status(&**shared).checks = checks;
            match bad {
                0 => Ok("all clean".to_string()),
                n => bail!("{n} volume(s) report problems"),
            }
        }
    }
}

fn open_rw(path: &Path) -> Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open {} for writing", path.display()))
}

/// Stream `from` into a new file at `to`, never replacing one that is there.
fn copy_file(from: &Path, to: &Path, ctx: &JobCtx) -> Result<()> {
    let mut src = std::fs::File::open(from)?;
    let total = src.metadata()?.len();
    let mut dst = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(to)
        .with_context(|| format!("create {}", to.display()))?;
    let mut buf = vec![0u8; COPY_CHUNK];
    let mut done = 0u64;
    loop {
        if ctx.is_cancelled() {
            bail!("cancelled");
        }
        let n = src.read(&mut buf)?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n])?;
        done += n as u64;
        ctx.set_bytes(done, total);
    }
    dst.flush()?;
    Ok(())
}

/// Bring the file to the new disk size and write the table for it (a GPT's backup at the end).
fn finish(
    path: &Path,
    req: &ResizeImageRequest,
    layout: &Layout,
    log: &mut impl FnMut(&str),
) -> Result<()> {
    let mut file = open_rw(path)?;
    let is_vhd = layout.is_vhd;
    let new = req.disk_size;
    let old_tail = layout.old_disk_size.saturating_sub(table_tail(&req.table));
    // A shrink cuts the file; a VHD's footer goes back at its new end.
    if new < layout.old_disk_size {
        file.set_len(new)?;
    }
    if is_vhd {
        file.set_len(new)?;
        file.seek(SeekFrom::Start(new))?;
        file.write_all(&dsk_vm::vhd::build_vhd_footer(new))?;
    }
    // Old records now in free space (a backup table, a VHD footer at the old end or the last
    // partition's end): clear them so no reader mistakes them for live ones.
    let footer = if is_vhd { SECTOR } else { 0 };
    for (at, bytes) in [
        (old_tail, table_tail(&req.table)),
        (layout.data_end, footer),
        (layout.old_disk_size, footer),
    ] {
        if bytes > 0 && at >= layout.data_end && at + bytes <= new {
            file.seek(SeekFrom::Start(at))?;
            file.write_all(&vec![0u8; bytes as usize])?;
        }
    }
    if records_disk_size(&req.table) {
        file.seek(SeekFrom::Start(0))?;
        let table = PartitionTable::detect(&mut file).context("read the resized table")?;
        crate::partition::editor::apply_edits(&mut file, &table, &[], new, log)?;
    }
    file.flush()?;
    log(&format!("Disk size set to {}", format_size(new)));
    Ok(())
}

/// Check every partition the resize touched, where it now starts.
fn check_after(path: &Path, req: &ResizeImageRequest, layout: &Layout) -> Vec<(usize, Verdict)> {
    req.partitions
        .iter()
        .filter(|p| !p.is_extended_container)
        .filter_map(|p| {
            let plan = layout.plans.iter().find(|r| r.index == p.index)?;
            let touched = plan.new_size_bytes != plan.old_size_bytes || plan.needs_data_move;
            let offset = plan.new_start_lba * SECTOR;
            touched.then(|| (p.index, check_partition(path, offset, p)))
        })
        .collect()
}

#[cfg(test)]
mod tests;
