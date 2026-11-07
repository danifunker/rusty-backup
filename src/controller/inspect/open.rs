//! Opening a source: the inspect job, the backup-folder load, Calc min and the backup label probes.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use super::{push_log, InspectController, LogSink, Source};
use crate::controller::log::LogLevel;
use crate::model::backup_loader::{BackupLoadOutcome, ClonezillaLoadOutcome, LoadOutcome};
use crate::model::inspect_runner::{inspect_source, InspectRequest, Inspected};
use crate::model::job::{Job, JobCtx, Snapshot, Status};
use crate::model::min_size_runner::{MinSizeRequest, MinSizeSource};
use crate::model::volume_label_runner::{VolumeLabelRequest, VolumeLabelSource};
use disk_core::display::format_size;

/// A finished minimum-size walk: in-place, defragmented, and the last phase it reported.
type MinDone = (Option<u64>, Option<u64>, String);

#[derive(Default)]
pub(super) struct OpenState {
    pub floppy: bool,
    inspect: Option<Job<Result<Inspected, String>>>,
    backup: Option<Job<Result<LoadOutcome, String>>>,
    min: HashMap<usize, Job<MinDone>>,
    labels: HashMap<usize, Job<Option<String>>>,
}

impl OpenState {
    pub fn loading(&self) -> Option<Snapshot> {
        let inspect = self.inspect.as_ref().map(|j| j.snapshot());
        inspect.or_else(|| self.backup.as_ref().map(|j| j.snapshot()))
    }

    pub fn min_jobs_running(&self) -> bool {
        !self.min.is_empty()
    }

    pub fn min_phase(&self, index: usize) -> Option<String> {
        self.min.get(&index).map(|j| {
            let phase = j.snapshot().phase;
            if phase.is_empty() {
                "...".to_string()
            } else {
                phase
            }
        })
    }

    /// Stop the walks reading the old source (the inspect and load jobs cannot be stopped).
    pub fn cancel_all(&self) {
        for job in self.min.values() {
            job.cancel();
        }
    }

    pub fn probing(&self) -> bool {
        !self.min.is_empty() || !self.labels.is_empty()
    }

    /// Drop every job reading the old source; the floppy flag belongs to the source and stays.
    pub fn restart(&mut self) {
        self.cancel_all();
        *self = OpenState {
            floppy: self.floppy,
            ..Default::default()
        };
    }
}

/// A job's result, or the failure message once it ended without one.
fn finished<R>(job: &Job<R>) -> Option<Result<R, String>> {
    let snap = job.snapshot();
    match snap.status {
        Status::Running => None,
        Status::Done => Some(job.take_result().ok_or_else(|| "no result".to_string())),
        Status::Failed(e) => Some(Err(e)),
        Status::Cancelled => Some(Err("cancelled".to_string())),
    }
}

impl InspectController {
    /// Read the source again: a backup folder's metadata, or a disk's partition table.
    pub(super) fn reinspect(&mut self) {
        match &self.source {
            None => {}
            Some(Source::Backup { folder }) => {
                let folder = folder.clone();
                self.load_backup(folder);
            }
            Some(_) => self.run_inspect(),
        }
    }

    fn clear_found(&mut self) {
        let device_file = self.found.device_file.take();
        let device_guard = self.found.device_guard.take();
        self.open.restart();
        self.found = super::Found::default();
        // A reinspect of the same device keeps its descriptor; dropping it would re-prompt on macOS.
        self.found.device_file = device_file;
        self.found.device_guard = device_guard;
        self.tools.reset();
        self.browse.reset();
    }

    fn run_inspect(&mut self) {
        self.clear_found();
        let remote = self.is_remote();
        let (path, req) = match &self.source {
            Some(Source::Device { identity, .. }) => (
                identity.path.clone(),
                InspectRequest::local(identity.path.clone()),
            ),
            Some(Source::Image { path, .. }) => (path.clone(), InspectRequest::local(path.clone())),
            #[cfg(feature = "remote")]
            Some(Source::Remote { conn }) => {
                let path = PathBuf::from(conn.1.as_str());
                let req = InspectRequest {
                    path: path.clone(),
                    remote: Some((Arc::clone(&conn.0), conn.1.clone())),
                };
                (path, req)
            }
            _ => return,
        };
        // Remember the escalated device so Close releases exactly it; a reinspect keeps it.
        if !remote && crate::os::is_device_path(path.as_path()) {
            if self.escalated.as_deref() != Some(path.as_path()) {
                self.release_device();
            }
            self.escalated = Some(path.clone());
        } else {
            self.release_device();
        }
        if remote {
            self.info(format!("Inspecting remote image {}...", path.display()));
        } else {
            self.info(format!("Inspecting {}...", path.display()));
        }
        // A remote image is never a local CHD.
        self.found.chd_path = if !remote && crate::model::source_reader::is_chd_path(path.as_path())
        {
            Some(path.clone())
        } else {
            None
        };
        let sink = Arc::clone(&self.log);
        self.open.inspect = Some(self.spawn("Inspecting", move |job| {
            job.set_can_cancel(false);
            let log = |m: String| push_log(&sink, LogLevel::Info, m);
            Ok(inspect_source(&req, job, &log))
        }));
    }

    fn load_backup(&mut self, folder: PathBuf) {
        self.clear_found();
        self.info(format!("Loading backup folder {}...", folder.display()));
        // Metadata and partclone headers can be slow on a share or a card.
        self.open.backup = Some(self.spawn("Loading the backup...", move |job| {
            job.set_can_cancel(false);
            Ok(crate::model::backup_loader::load_backup(folder.as_path())
                .map_err(|e| format!("{e:#}")))
        }));
    }

    pub(super) fn poll_open(&mut self) {
        if let Some(done) = self.open.inspect.as_ref().and_then(finished) {
            self.open.inspect = None;
            match done.and_then(|r| r) {
                Ok(Inspected::Disk(out)) => self.inspected(*out),
                Ok(Inspected::Optical(path)) => {
                    self.info(format!(
                        "{} is an optical disc - opening the Optical tab.",
                        path.display()
                    ));
                    self.requests.push(super::HostRequest::OpenOptical(path));
                }
                Err(err) => {
                    let msg = format!("Inspect failed: {err}");
                    self.error(msg.as_str());
                    self.found.error = Some(msg);
                }
            }
        }
        if let Some(done) = self.open.backup.as_ref().and_then(finished) {
            self.open.backup = None;
            match done.and_then(|r| r) {
                Ok(LoadOutcome::Backup(out)) => self.backup_loaded(out),
                Ok(LoadOutcome::Clonezilla(out)) => self.clonezilla_loaded(out),
                Err(msg) => {
                    self.error(msg.as_str());
                    self.found.error = Some(msg);
                }
            }
        }
        self.poll_min();
        self.poll_labels();
    }

    fn inspected(&mut self, out: crate::model::inspect_runner::InspectOutcome) {
        self.found.table = Some(out.table);
        self.found.alignment = Some(out.alignment);
        self.found.partitions = out.partitions;
        self.found.min_sizes = out.min_sizes;
        self.found.defrag_min_sizes = out.defrag_min_sizes;
        self.found.volume_labels = out.volume_labels;
        self.found.deferred = out.deferred_min_sizes;
        self.found.format_label = out.format_label;
        self.found.disk_size = None;
        // Browse and Calc min share one handle, so a device is not opened (or prompted for) twice.
        if let Some(f) = out.device_file {
            match f.into_file() {
                Ok(file) => self.found.device_file = Some(Arc::new(file)),
                Err(e) => self.warn(format!(
                    "Could not retain the device handle for browsing: {e}"
                )),
            }
        }
        if out.device_guard.is_some() {
            self.found.device_guard = out.device_guard;
        }
        self.layout_generation += 1;
    }

    fn backup_loaded(&mut self, out: BackupLoadOutcome) {
        for msg in &out.info {
            self.info(msg.as_str());
        }
        for msg in &out.warnings {
            self.warn(msg.as_str());
        }
        // A single-file-CHD backup is one disk image; inspect the CHD itself, remembering the folder.
        if matches!(
            out.metadata.layout,
            crate::backup::metadata::BackupLayout::SingleFileChd
        ) {
            if let Some(folder) = self.backup_folder().map(|f| f.to_path_buf()) {
                let container = out.metadata.container.as_deref().unwrap_or("disk.chd");
                let chd_path = folder.join(container);
                self.info(format!(
                    "Single-file-CHD backup detected; opening {} as a disk image \
                     for inspection (instead of the backup folder).",
                    chd_path.display(),
                ));
                self.source = Some(Source::Image {
                    path: chd_path,
                    _temp: None,
                });
                self.run_inspect();
                self.found.chd_backup = Some((folder, out.metadata.sector_by_sector));
                return;
            }
        }
        self.found.backup = Some(out.metadata);
        self.found.table = out.partition_table;
        self.found.alignment = out.alignment;
        self.found.partitions = out.partitions;
        self.found.disk_size = None;
        self.spawn_label_probes();
    }

    fn clonezilla_loaded(&mut self, out: ClonezillaLoadOutcome) {
        for msg in &out.info {
            self.info(msg.as_str());
        }
        for msg in &out.warnings {
            self.warn(msg.as_str());
        }
        if out.cached_metadata_count > 0 {
            self.info(format!(
                "Found {} cached metadata file(s) for browsing",
                out.cached_metadata_count,
            ));
        }
        self.found.table = out.partition_table;
        self.found.alignment = out.alignment;
        self.found.partitions = out.partitions;
        self.found.min_sizes = out.partition_min_sizes;
        self.found.clonezilla = Some(out.image);
        self.found.disk_size = None;
    }

    // --- Calc min -----------------------------------------------------------------------

    fn source_is_device(&self) -> bool {
        self.device_path().is_some_and(|p| {
            let s = p.to_string_lossy();
            s.starts_with("/dev/") || s.starts_with("\\\\.\\")
        })
    }

    fn min_size_source(&self, index: usize) -> Result<MinSizeSource, String> {
        #[cfg(feature = "remote")]
        if let Some(conn) = self.remote() {
            return Ok(MinSizeSource::Remote {
                conn: Arc::clone(&conn.0),
                path: conn.1.clone(),
                is_device: false,
            });
        }
        let is_device = self.source_is_device();
        // A CHD or GHO read at the partition offset would hit compressed bytes; decode them.
        if let Some(chd) = &self.found.chd_path {
            return Ok(MinSizeSource::Chd(chd.clone()));
        }
        if let Some(gho) = self
            .image_path()
            .filter(|&p| crate::model::source_reader::is_gho_path(p))
        {
            return Ok(MinSizeSource::Gho(gho.to_path_buf()));
        }
        if let Some(file) = &self.found.device_file {
            return Ok(MinSizeSource::File {
                file: Arc::clone(file),
                use_sector_aligned: is_device,
            });
        }
        match self.image_path().filter(|_| !is_device) {
            // Only macOS keeps the inspect's handle; elsewhere an image is reopened.
            Some(path) => match std::fs::File::open(path) {
                Ok(file) => Ok(MinSizeSource::File {
                    file: Arc::new(file),
                    use_sector_aligned: false,
                }),
                Err(e) => Err(format!(
                    "Cannot calculate minimum size for partition {}: {e}",
                    index + 1
                )),
            },
            None => Err(format!(
                "Cannot calculate minimum size for partition {}: no open source handle",
                index + 1
            )),
        }
    }

    pub(super) fn calc_min(&mut self, index: usize) {
        if self.open.min.contains_key(&index) {
            return;
        }
        let Some(part) = self
            .found
            .partitions
            .iter()
            .find(|p| p.index == index)
            .cloned()
        else {
            return;
        };
        let source = match self.min_size_source(index) {
            Ok(s) => s,
            Err(e) => {
                self.error(e);
                return;
            }
        };
        let fs_name = self
            .found
            .deferred
            .get(&index)
            .copied()
            .unwrap_or("unknown");
        self.info(format!(
            "Calculating minimum size for partition {} ({fs_name})...",
            index + 1
        ));
        let req = MinSizeRequest {
            source,
            partition_offset: part.byte_offset(),
            partition_type: part.partition_type_byte,
            partition_type_string: part.partition_type_string.clone(),
            partition_size: part.size_bytes,
            partition_index: index,
        };
        let sink = Arc::clone(&self.log);
        let job = self.spawn("Calculating the minimum size", move |job| {
            min_size_in_job(req, index, &sink, job)
        });
        self.open.min.insert(index, job);
    }

    fn poll_min(&mut self) {
        let done: Vec<usize> = self
            .open
            .min
            .iter()
            .filter(|(_, j)| !j.is_running())
            .map(|(i, _)| *i)
            .collect();
        for idx in done {
            let Some(job) = self.open.min.remove(&idx) else {
                continue;
            };
            self.found.deferred.remove(&idx);
            let no = idx + 1;
            match finished(&job) {
                Some(Ok((Some(min), defrag, _))) => self.min_found(idx, min, defrag),
                Some(Ok((None, _, phase))) => self.info(format!(
                    "Partition {no}: filesystem could not be opened for minimum-size calculation (last phase: {phase})",
                )),
                Some(Err(err)) => self.error(format!(
                    "Minimum size calculation failed for partition {no}: {err}",
                )),
                None => {}
            }
        }
    }

    fn min_found(&mut self, idx: usize, min: u64, defrag: Option<u64>) {
        let no = idx + 1;
        let part_size = self
            .found
            .partitions
            .iter()
            .find(|p| p.index == idx)
            .map(|p| p.size_bytes)
            .unwrap_or(min);
        let clamped = min.min(part_size);
        self.found.min_sizes.insert(idx, clamped);
        match defrag.map(|d| d.min(part_size)).filter(|&d| d < clamped) {
            Some(d) => {
                self.found.defrag_min_sizes.insert(idx, d);
                self.info(format!(
                    "Partition {no}: minimum size {} (computed); defragmented {}",
                    format_size(clamped),
                    format_size(d),
                ));
            }
            None => self.info(format!(
                "Partition {no}: minimum size {} (computed)",
                format_size(clamped),
            )),
        }
    }

    // --- backup volume labels -----------------------------------------------------------

    /// One label probe per HFS+ partition whose data is a single raw, zstd or CHD file.
    fn spawn_label_probes(&mut self) {
        let (Some(folder), Some(meta)) = (
            self.backup_folder().map(|f| f.to_path_buf()),
            self.found.backup.clone(),
        ) else {
            return;
        };
        for part in &self.found.partitions {
            let ts = part.partition_type_string.as_deref();
            let is_hfs_plus = part.partition_type_byte == 0xAF
                || matches!(
                    ts,
                    Some("Apple_HFS") | Some("Apple_HFS+") | Some("Apple_HFSX")
                );
            if !is_hfs_plus {
                continue;
            }
            let Some(pm) = meta.partitions.iter().find(|p| p.index == part.index) else {
                continue;
            };
            // The probe needs random access; a split data file is out of scope.
            if pm.compressed_files.len() != 1 {
                continue;
            }
            let data_path = folder.join(pm.compressed_files[0].as_str());
            if !data_path.exists() {
                continue;
            }
            let source = match meta.compression_type.as_str() {
                "none" => VolumeLabelSource::Raw {
                    path: data_path,
                    partition_offset: 0,
                },
                "zstd" => VolumeLabelSource::Zstd { path: data_path },
                "chd" => VolumeLabelSource::Chd {
                    path: data_path,
                    partition_offset: 0,
                },
                _ => continue,
            };
            let req = VolumeLabelRequest {
                source,
                partition_index: part.index,
            };
            let job = Job::spawn(
                "Probing a volume label",
                Arc::clone(&self.services.notify),
                move |job| label_in_job(req, job),
            );
            self.open.labels.insert(part.index, job);
        }
        let count = self.open.labels.len();
        if count > 0 {
            self.info(format!(
                "Probing {count} HFS+ partition label(s) from backup data..."
            ));
        }
    }

    fn poll_labels(&mut self) {
        let done: Vec<usize> = self
            .open
            .labels
            .iter()
            .filter(|(_, j)| !j.is_running())
            .map(|(i, _)| *i)
            .collect();
        for idx in done {
            let Some(job) = self.open.labels.remove(&idx) else {
                continue;
            };
            match finished(&job) {
                Some(Ok(Some(label))) => {
                    if let Some(part) = self.found.partitions.iter_mut().find(|p| p.index == idx) {
                        let new_name = crate::model::inspect_runner::apply_volume_label(
                            part.type_name.as_str(),
                            label.as_str(),
                        );
                        if part.type_name != new_name {
                            part.type_name = new_name;
                            self.layout_generation += 1;
                        }
                        self.info(format!("Partition {} label: {label}", idx + 1));
                    }
                }
                Some(Ok(None)) | None => {}
                Some(Err(err)) => self.warn(format!(
                    "Volume label probe failed for partition {}: {err}",
                    idx + 1
                )),
            }
        }
    }

    /// Something is still being probed, so the shell keeps redrawing.
    pub fn probing(&self) -> bool {
        self.open.probing()
    }
}

fn min_size_in_job(
    req: MinSizeRequest,
    index: usize,
    sink: &LogSink,
    job: &JobCtx,
) -> anyhow::Result<MinDone> {
    let status = crate::model::min_size_runner::spawn(req);
    let mut out = None;
    crate::model::worker::wait_spawned(job, &status, |s, ctx| {
        for phase in s.phase_log.drain(..) {
            if !phase.is_empty() {
                push_log(
                    sink,
                    LogLevel::Info,
                    format!("Partition {}: {phase}", index + 1),
                );
            }
        }
        ctx.set_phase(s.phase.as_str());
        if ctx.is_cancelled() {
            s.cancel_requested = true;
        }
        if s.finished {
            out = Some(match s.error.take() {
                Some(e) => Err(anyhow::anyhow!(e)),
                None => Ok((s.result, s.defragmented_min, s.phase.clone())),
            });
        }
        s.finished
    });
    out.unwrap_or_else(|| Err(anyhow::anyhow!("no result")))
}

fn label_in_job(req: VolumeLabelRequest, job: &JobCtx) -> anyhow::Result<Option<String>> {
    job.set_can_cancel(false);
    let status = crate::model::volume_label_runner::spawn(req);
    let mut out = None;
    crate::model::worker::wait_spawned(job, &status, |s, _| {
        if s.finished {
            out = Some(match s.error.take() {
                Some(e) => Err(anyhow::anyhow!(e)),
                None => Ok(s.label.take()),
            });
        }
        s.finished
    });
    out.unwrap_or_else(|| Err(anyhow::anyhow!("no result")))
}
