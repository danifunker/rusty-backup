//! The source: the backup folder and its load, the partition-image decode, Write Image's probe.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{HostRequest, RestoreController, RestoreMode, SizeMode, SourcePartition};
use crate::backup::metadata::BackupMetadata;
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
    /// Load `folder` once the running restore ends.
    pub load_deferred: bool,
    /// Keeps a `.cbk`'s unpacked folder alive, shared with a restore reading it.
    pub guard: Option<Arc<tempfile::TempDir>>,
    /// The partition image for a single-partition or new-disk restore.
    image: Option<Prepared>,
    decode: Option<(PathBuf, Job<Prepared>)>,
    pub source_partition: Option<usize>,
    /// Write Image File's source and its decoded size (0 when unreadable).
    pub write: Option<(PathBuf, u64)>,
    probe: Option<(PathBuf, Job<Option<u64>>)>,
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
        self.load.is_some() || self.decode.is_some() || self.probe.is_some()
    }

    pub fn set_size(&mut self, index: usize, mode: SizeMode, custom_mib: u32) {
        if let Some(row) = self
            .loaded
            .as_mut()
            .and_then(|l| l.rows.iter_mut().find(|r| r.index == index))
        {
            row.mode = mode;
            row.custom_mib = custom_mib;
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
            || matches!(self.mode, RestoreMode::WriteImage | RestoreMode::BuildDisk)
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
        self.src.write.as_ref().map(|(p, n)| (p.as_path(), *n))
    }

    /// The decoded-size probe of Write Image File's source in progress.
    pub fn probing(&self) -> Option<Snapshot> {
        self.src.probe.as_ref().map(|(_, j)| j.snapshot())
    }

    // --- actions ------------------------------------------------------------------------

    pub(super) fn open_backup(&mut self, path: PathBuf) {
        self.src.image = None;
        self.src.decode = None;
        if self.src.folder.as_deref() == Some(path.as_path())
            && (self.src.loaded.is_some()
                || self.src.load.is_some()
                || self.src.load_error.is_some()
                || self.src.load_deferred)
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

    pub(super) fn clear_backup(&mut self) {
        self.src.forget_backup();
        self.targets.expand.enabled = false;
    }

    pub(super) fn open_source_image(&mut self, path: PathBuf) {
        self.src.forget_backup();
        // A partition image is written as a flat sector stream, so wrapped images are decoded.
        if !crate::model::image_prep::needs_decoding(path.as_path(), true) {
            self.src.decode = None;
            self.src.image = Some((path, None));
            return;
        }
        self.src.image = None;
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let worker_path = path.clone();
        let job = self.spawn("Decompress image", move |ctx| {
            ctx.set_can_cancel(false);
            ctx.set_phase(format!("Decompressing {name}..."));
            Ok(crate::model::image_prep::prepare_disk_image_path_reporting(
                worker_path.as_path(),
                true,
                &|done, total| ctx.set_bytes(done, total),
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
        // A failed decode falls back to the file as picked.
        self.src.image = Some(match done {
            Ok(prepared) => prepared,
            Err(e) => {
                self.error(format!("Failed to decompress {}: {e}", original.display()));
                (original, None)
            }
        });
    }

    pub(super) fn open_dropped(&mut self, path: PathBuf) {
        if path.is_dir() {
            self.mode = RestoreMode::FullDisk;
            self.open_backup(path);
        } else {
            self.mode = RestoreMode::SinglePartition;
            self.open_source_image(path);
        }
    }

    pub(super) fn open_write_image(&mut self, path: PathBuf) {
        let worker_path = path.clone();
        let job = self.spawn("Reading the image", move |ctx| {
            ctx.set_can_cancel(false);
            ctx.set_phase("Measuring the decoded image...");
            Ok(crate::model::source_reader::decoded_image_size(
                worker_path.as_path(),
            ))
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
        let size = done.ok().flatten().unwrap_or(0);
        if size == 0 {
            self.error(format!("Could not read {} as a disk image", path.display()));
        } else {
            self.info(format!(
                "Source image: {} ({})",
                path.display(),
                format_size(size)
            ));
        }
        self.src.write = Some((path, size));
    }

    pub(super) fn write_image_target(&mut self) {
        if let Some((path, size)) = self.src.write.clone() {
            if size > 0 {
                self.requests.push(HostRequest::WriteImage { path, size });
            }
        }
    }
}
