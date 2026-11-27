//! The source: the backup folder and its load, the partition-image decode, Write Image's probe.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{HostRequest, RestoreController, RestoreMode, SizeMode, SourcePartition, WriteOpen};
use crate::backup::metadata::BackupMetadata;
use crate::model::fs_limits::VolumeFacts;
use crate::model::job::{Job, Snapshot, Status};
use crate::model::restore_plan::LoadedBackup;
use disk_core::display::format_size;

/// A flat image path, and the folder that must outlive it when it was decoded.
type Prepared = (PathBuf, Option<tempfile::TempDir>);

type LoadOutcome = Result<LoadedBackup, String>;

#[derive(Default)]
pub(super) struct SourceState {
    /// The backup folder picked (loaded, loading, or waiting for a restore to end).
    pub folder: Option<PathBuf>,
    pub loaded: Option<LoadedBackup>,
    pub load_error: Option<String>,
    load: Option<Job<LoadOutcome>>,
    /// Dropping the image's swap files, then reading the backup again.
    drop_swap: Option<Job<LoadOutcome>>,
    /// Load `folder` once the running restore ends.
    pub load_deferred: bool,
    /// Keeps a `.cbk`'s unpacked folder alive, shared with a restore reading it.
    pub guard: Option<Arc<tempfile::TempDir>>,
    /// The partition image for a single-partition or new-disk restore.
    image: Option<Prepared>,
    decode: Option<(PathBuf, Job<Prepared>)>,
    /// What the source image holds, read by `facts_probe`.
    pub image_facts: Option<VolumeFacts>,
    facts_probe: Option<Job<Option<VolumeFacts>>>,
    pub source_partition: Option<usize>,
    /// Write Image File's source as the disk writer takes it (size 0 when unreadable).
    pub write: Option<WriteOpen>,
    probe: Option<(PathBuf, Job<WriteOpen>)>,
}

/// A job's result, or the failure message once it ended without one.
pub(super) fn finished<R>(job: &Job<R>) -> Option<Result<R, String>> {
    let snap = job.snapshot();
    match snap.status {
        Status::Running => None,
        Status::Done => Some(job.take_result().ok_or_else(|| "no result".to_string())),
        Status::Failed(e) => Some(Err(e)),
        Status::Cancelled => Some(Err("cancelled".to_string())),
    }
}

impl SourceState {
    pub fn metadata(&self) -> Option<&BackupMetadata> {
        self.loaded.as_ref().and_then(|l| l.metadata.as_ref())
    }

    pub fn has_image(&self) -> bool {
        self.image.is_some()
    }

    pub fn image_path(&self) -> Option<&Path> {
        self.image.as_ref().map(|(p, _)| p.as_path())
    }

    /// A load, decode or probe runs.
    pub fn busy(&self) -> bool {
        self.load.is_some()
            || self.drop_swap.is_some()
            || self.decode.is_some()
            || self.probe.is_some()
            || self.facts_probe.is_some()
    }

    /// A switch into Custom starts at the original size, as the GUI's size cell did.
    pub fn set_size(&mut self, index: usize, mode: SizeMode, custom_mib: u32) {
        if let Some(row) = self
            .loaded
            .as_mut()
            .and_then(|l| l.rows.iter_mut().find(|r| r.index == index))
        {
            let seed = mode == SizeMode::Custom && row.mode != SizeMode::Custom;
            row.mode = mode;
            row.custom_mib = if seed {
                (row.original_size / (1024 * 1024)).max(1) as u32
            } else {
                custom_mib
            };
        }
    }

    fn forget_backup(&mut self) {
        self.folder = None;
        self.loaded = None;
        self.load_error = None;
        self.load = None;
        self.load_deferred = false;
        self.guard = None;
        self.source_partition = None;
        // A drop-swap landing later reloaded the old backup over the new one (CWR-003).
        self.drop_swap = None;
    }
}

impl RestoreController {
    // --- accessors ----------------------------------------------------------------------

    pub fn backup_folder(&self) -> Option<&Path> {
        self.src.folder.as_deref()
    }

    /// A backup (native or Clonezilla) is loaded.
    pub fn has_backup(&self) -> bool {
        self.src.loaded.is_some()
    }

    /// A native backup (with `metadata.json`) is loaded: its partitions can be restored singly.
    pub fn has_native_backup(&self) -> bool {
        self.src.metadata().is_some()
    }

    /// A source the user picked instead of a backup folder, or a mode that needs none.
    pub fn has_other_source(&self) -> bool {
        self.src.image.is_some()
            || self.src.decode.is_some()
            || matches!(self.mode, RestoreMode::WriteImage)
    }

    pub fn load_error(&self) -> Option<&str> {
        self.src.load_error.as_deref()
    }

    /// The backup load in progress.
    pub fn loading(&self) -> Option<Snapshot> {
        self.src.load.as_ref().map(|j| j.snapshot())
    }

    /// The backup's two summary lines: its contents and where it came from.
    pub fn backup_summary(&self) -> Option<(String, String)> {
        crate::model::restore_plan::summary(self.loaded()?)
    }

    /// The backup's source disk size, the default size of an image target.
    pub fn source_size(&self) -> Option<u64> {
        self.loaded().map(|l| l.source_size()).filter(|&n| n > 0)
    }

    /// The partitions a single-partition or new-disk restore can take from the backup.
    pub fn source_partitions(&self) -> Vec<SourcePartition> {
        self.src
            .metadata()
            .map(|m| {
                m.partitions
                    .iter()
                    .map(|p| SourcePartition {
                        index: p.index,
                        label: format!(
                            "#{}: {} ({})",
                            p.index,
                            p.type_name,
                            format_size(p.original_size_bytes),
                        ),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn source_partition(&self) -> Option<usize> {
        self.src.source_partition
    }

    /// The single-partition / new-disk source image, once ready.
    pub fn source_image(&self) -> Option<&Path> {
        self.src.image_path()
    }

    /// The source image decode in progress.
    pub fn decoding(&self) -> Option<Snapshot> {
        self.src.decode.as_ref().map(|(_, j)| j.snapshot())
    }

    /// Write Image File's source and its decoded size (0 when it could not be read).
    pub fn write_image(&self) -> Option<(&Path, u64)> {
        self.src
            .write
            .as_ref()
            .map(|w| (w.path.as_path(), w.size_bytes.unwrap_or(0)))
    }

    /// The decoded size of Write Image File's source, in words; `None` when unreadable or unset.
    pub fn write_image_line(&self) -> Option<String> {
        let (_, size) = self.write_image().filter(|(_, size)| *size > 0)?;
        Some(format!(
            "Decoded size: {} (the file on disk may be smaller if it is compressed)",
            format_size(size),
        ))
    }

    /// Write Image File's source is a table-less volume: its filesystem, for the writer's table.
    pub fn write_image_superfloppy(&self) -> Option<&str> {
        self.src
            .write
            .as_ref()
            .filter(|w| w.superfloppy)
            .map(|w| w.fs_hint.as_str())
    }

    /// Where a full-disk image target goes by default: beside the backup, `<name>_restored.img`.
    pub fn default_target_image(&self) -> Option<PathBuf> {
        let folder = self.src.folder.as_deref()?;
        let stem = folder.file_name()?.to_string_lossy().into_owned();
        Some(folder.parent()?.join(format!("{stem}_restored.img")))
    }

    /// The size modes a row offers; Fill only on the last.
    pub fn size_mode_choices(&self, index: usize) -> Vec<SizeMode> {
        let rows = self.size_rows();
        let is_last = rows.last().map(|r| r.index) == Some(index);
        rows.iter()
            .find(|r| r.index == index)
            .map(|r| r.mode_choices(is_last))
            .unwrap_or_default()
    }

    /// The decoded-size probe of Write Image File's source in progress.
    pub fn probing(&self) -> Option<Snapshot> {
        self.src.probe.as_ref().map(|(_, j)| j.snapshot())
    }

    // --- actions ------------------------------------------------------------------------

    pub(super) fn open_backup(&mut self, path: PathBuf) {
        self.src.image = None;
        self.src.image_facts = None;
        self.src.facts_probe = None;
        self.src.decode = None;
        if self.src.folder.as_deref() == Some(path.as_path())
            // A failed load is retried: the card may be mounted now (CWR-014).
            && (self.src.loaded.is_some() || self.src.load.is_some() || self.src.load_deferred)
        {
            return;
        }
        self.src.forget_backup();
        self.src.folder = Some(path);
        if self.run.running() {
            // The running restore reads the old folder; load the new one once it ends.
            self.src.load_deferred = true;
            return;
        }
        self.start_load();
    }

    pub(super) fn start_load(&mut self) {
        let Some(folder) = self.src.folder.clone() else {
            return;
        };
        self.src.load_deferred = false;
        self.requests
            .push(HostRequest::RememberRecent(folder.clone()));
        self.src.load = Some(self.spawn("Loading the backup", move |ctx| {
            ctx.set_can_cancel(false);
            ctx.set_phase("Reading the backup...");
            Ok(crate::model::restore_plan::load_backup(folder.as_path()))
        }));
    }

    pub(super) fn poll_load(&mut self) {
        let Some(done) = self.src.load.as_ref().and_then(finished) else {
            return;
        };
        self.src.load = None;
        match done {
            Ok(Ok(mut loaded)) => {
                self.info(loaded.loaded_line.as_str());
                self.src.guard = loaded.guard.take().map(Arc::new);
                self.src.loaded = Some(loaded);
            }
            Ok(Err(msg)) | Err(msg) => self.src.load_error = Some(msg),
        }
    }

    /// Windows swap files in the loaded disk image, which "Drop Swap Files..." deletes.
    pub fn swap_files(&self) -> &[super::SwapFile] {
        self.src
            .loaded
            .as_ref()
            .map_or(&[], |l| l.swap_files.as_slice())
    }

    /// The line under the size table naming them, or `None` when there are none.
    pub fn swap_files_line(&self) -> Option<String> {
        let files = self.swap_files();
        if files.is_empty() {
            return None;
        }
        let names: Vec<String> = files
            .iter()
            .map(|f| {
                format!(
                    "{} ({}) on partition {}",
                    f.name,
                    format_size(f.size),
                    f.partition + 1
                )
            })
            .collect();
        Some(format!(
            "{} {} rebuilt by Windows at boot; dropping {} may let a partition shrink further.",
            names.join(", "),
            if files.len() == 1 { "is" } else { "are" },
            if files.len() == 1 { "it" } else { "them" },
        ))
    }

    /// The swap-file drop in progress.
    pub fn dropping_swap(&self) -> Option<Snapshot> {
        self.src.drop_swap.as_ref().map(|j| j.snapshot())
    }

    pub(super) fn ask_drop_swap(&mut self) -> Vec<crate::controller::Effect> {
        if self.swap_files().is_empty() || self.src.busy() || self.run.running() {
            return Vec::new();
        }
        let names: Vec<&str> = self.swap_files().iter().map(|f| f.name.as_str()).collect();
        let in_place = self
            .src
            .metadata()
            .and_then(|m| m.container.as_deref())
            .is_some_and(|c| !c.ends_with(crate::model::image_backup::DECODED_NAME));
        let copy = if in_place {
            format!(
                " The image is copied to a temporary file first ({}); your file is not changed.",
                format_size(self.source_size().unwrap_or(0))
            )
        } else {
            " This edits Restore's temporary copy; your file is not changed.".to_string()
        };
        vec![crate::controller::Effect::Confirm(
            crate::controller::ConfirmSpec {
                purpose: super::purpose::DROP_SWAP,
                title: "Drop Swap Files".to_string(),
                message: format!(
                "Delete {} from the disk being restored? Windows recreates swap files at the next \
                 boot.{copy}",
                names.join(", ")
            ),
                confirm_label: "Drop them".to_string(),
                destructive: false,
            },
        )]
    }

    pub(super) fn drop_swap(&mut self) {
        if self.is_running() || self.src.drop_swap.is_some() {
            return;
        }
        let Some(folder) = self.src.loaded.as_ref().map(|l| l.folder.clone()) else {
            return;
        };
        // The unpacked .cbk folder lives while the job reads it.
        let guard = self.src.guard.clone();
        self.src.drop_swap = Some(self.spawn("Dropping swap files", move |ctx| {
            let _guard = guard;
            ctx.set_can_cancel(false);
            ctx.set_phase("Dropping swap files...");
            let mut log = |m: String| ctx.set_phase(m);
            Ok(
                crate::model::image_backup::drop_swap_files(folder.as_path(), &mut log)
                    .map_err(|e| format!("{e:#}"))
                    .and_then(|()| crate::model::restore_plan::load_backup(folder.as_path())),
            )
        }));
    }

    pub(super) fn poll_drop_swap(&mut self) {
        let Some(done) = self.src.drop_swap.as_ref().and_then(finished) else {
            return;
        };
        self.src.drop_swap = None;
        match done {
            // The folder stays the one the first load made, so its guard is kept.
            Ok(Ok(loaded)) => {
                self.info("Swap files dropped; the minimum sizes are recomputed.");
                self.src.loaded = Some(loaded);
            }
            Ok(Err(msg)) | Err(msg) => self.error(format!("Could not drop the swap files: {msg}")),
        }
    }

    pub(super) fn clear_backup(&mut self) {
        self.src.forget_backup();
        self.targets.expand.enabled = false;
    }

    pub(super) fn open_source_image(&mut self, path: PathBuf) {
        // A running partition restore reads the decoded image; replacing it dropped it (CWR-009).
        if self.is_running() {
            self.warn("Wait for the restore to finish before changing the source.");
            return;
        }
        self.src.forget_backup();
        // A partition image is written as a flat sector stream, so wrapped images are decoded.
        if !crate::model::image_prep::needs_decoding(path.as_path(), true) {
            self.src.decode = None;
            self.probe_image_facts(path.clone());
            self.src.image = Some((path, None));
            return;
        }
        self.src.image = None;
        self.src.image_facts = None;
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let worker_path = path.clone();
        let job = self.spawn("Decompress image", move |ctx| {
            ctx.set_phase(format!("Decompressing {name}..."));
            Ok(crate::model::image_prep::prepare_disk_image_path_reporting(
                worker_path.as_path(),
                true,
                &|done, total| ctx.set_bytes(done, total),
                &|| ctx.is_cancelled(),
            )?)
        });
        self.src.decode = Some((path, job));
    }

    pub(super) fn poll_decode(&mut self) {
        let Some(done) = self.src.decode.as_ref().and_then(|(_, j)| finished(j)) else {
            return;
        };
        let Some((original, _)) = self.src.decode.take() else {
            return;
        };
        // A failed decode sets no source: the container's raw bytes are not a partition (CWR-002).
        self.src.image = match done {
            Ok(prepared) => Some(prepared),
            Err(e) => {
                self.error(format!("Failed to decompress {}: {e}", original.display()));
                None
            }
        };
        if let Some(path) = self.src.image_path().map(Path::to_path_buf) {
            self.probe_image_facts(path);
        }
    }

    fn probe_image_facts(&mut self, path: PathBuf) {
        self.src.image_facts = None;
        let job = self.spawn("Reading the image", move |_| {
            Ok(crate::model::restore_plan::image_facts(path.as_path()))
        });
        self.src.facts_probe = Some(job);
    }

    pub(super) fn poll_image_facts(&mut self) {
        let Some(done) = self.src.facts_probe.as_ref().and_then(finished) else {
            return;
        };
        self.src.facts_probe = None;
        self.src.image_facts = done.ok().flatten();
    }

    pub(super) fn open_dropped(&mut self, path: PathBuf) {
        if path.is_dir() {
            self.mode = RestoreMode::FullDisk;
            self.open_backup(path);
        } else {
            // Refused before the mode flips under a running restore (a folder waits instead).
            if self.is_running() {
                self.warn("Wait for the restore to finish before changing the source.");
                return;
            }
            self.mode = RestoreMode::SinglePartition;
            self.open_source_image(path);
        }
    }

    pub(super) fn open_write_image(&mut self, path: PathBuf) {
        let worker_path = path.clone();
        let job = self.spawn("Reading the image", move |ctx| {
            ctx.set_can_cancel(false);
            ctx.set_phase("Measuring the decoded image...");
            Ok(WriteOpen::probe_image(worker_path))
        });
        self.src.write = None;
        self.src.probe = Some((path, job));
    }

    pub(super) fn poll_write_probe(&mut self) {
        let Some(done) = self.src.probe.as_ref().and_then(|(_, j)| finished(j)) else {
            return;
        };
        let Some((path, _)) = self.src.probe.take() else {
            return;
        };
        let open = done.unwrap_or_else(|_| WriteOpen::image(path.clone(), 0));
        let size = open.size_bytes.unwrap_or(0);
        if size == 0 {
            self.error(format!("Could not read {} as a disk image", path.display()));
        } else {
            self.info(format!(
                "Source image: {} ({})",
                path.display(),
                format_size(size)
            ));
        }
        self.src.write = Some(open);
    }

    pub(super) fn write_image_target(&mut self) {
        if let Some(open) = self.src.write.clone() {
            if open.size_bytes.unwrap_or(0) > 0 {
                self.requests.push(HostRequest::WriteImage(open));
            }
        }
    }
}
