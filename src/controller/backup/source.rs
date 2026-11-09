//! The source: picking a device or image, decoding a wrapped image, the scan and Calc min.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use super::{push_log, BackupController, HostRequest, LogSink, Source};
use crate::controller::log::LogLevel;
use crate::device::DiskDevice;
use crate::model::backup_plan::is_device_path;
use crate::model::job::{Job, JobCtx, Snapshot, Status};
use crate::model::min_size_runner::{MinSizeRequest, MinSizeSource};
use crate::model::source_scan::{ScanFailure, SourceScan};
use disk_core::display::format_size;

/// A scan's result: the partitions, or why it stopped (shown under the source).
type ScanOutcome = Result<SourceScan, ScanFailure>;

/// A finished walk: in-place, defragmented, fragmentation, and the last phase it reported.
type MinDone = (Option<u64>, Option<u64>, Option<f32>, String);

/// A flat image path, and the folder that must outlive it when it was decoded.
type Prepared = (PathBuf, Option<tempfile::TempDir>);

struct Decode {
    original: PathBuf,
    rename: bool,
    job: Job<Prepared>,
}

#[derive(Default)]
pub(super) struct SourceState {
    decode: Option<Decode>,
    /// The scan, and whether it is the automatic image preview.
    scan: Option<(Job<ScanOutcome>, bool)>,
    /// Open the VHD window once the scan ends.
    pub vhd_after_scan: bool,
    min: HashMap<usize, Job<MinDone>>,
}

impl SourceState {
    pub fn decoding(&self) -> Option<Snapshot> {
        self.decode.as_ref().map(|d| d.job.snapshot())
    }

    pub fn scanning(&self) -> Option<Snapshot> {
        self.scan.as_ref().map(|(j, _)| j.snapshot())
    }

    /// No decode and no scan runs.
    pub fn idle(&self) -> bool {
        self.decode.is_none() && self.scan.is_none()
    }

    #[cfg(feature = "remote")]
    pub fn drop_decode(&mut self) {
        self.decode = None;
    }

    pub fn cancel_scan(&self) {
        if let Some((job, _)) = &self.scan {
            job.cancel();
        }
    }

    pub fn min_running(&self) -> bool {
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

    /// Stop the walks and the scan reading the old source.
    pub fn stop_reading(&mut self) {
        for job in self.min.values() {
            job.cancel();
        }
        self.min.clear();
        if let Some((job, _)) = self.scan.take() {
            job.cancel();
        }
        self.vhd_after_scan = false;
    }
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

impl BackupController {
    pub(super) fn select_device(&mut self, slot: usize, device: DiskDevice) {
        self.drop_remote_source();
        self.src.decode = None;
        let same = matches!(&self.source, Some(Source::Device { slot: s, device: d })
            if *s == slot && d.identity() == device.identity());
        self.source = Some(Source::Device { slot, device });
        self.update_name();
        if !same {
            self.source_changed();
        }
    }

    pub(super) fn open_image(&mut self, path: PathBuf, rename: bool) {
        self.drop_remote_source();
        self.requests
            .push(HostRequest::RememberRecent(path.clone()));
        // Backup needs a flat sector stream, so floppy containers are decoded too.
        if !crate::model::image_prep::needs_decoding(path.as_path(), true) {
            self.src.decode = None;
            self.set_image(path, None, rename);
            return;
        }
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
        self.src.decode = Some(Decode {
            original: path,
            rename,
            job,
        });
        if self.source.is_some() {
            self.source = None;
            self.source_changed();
        }
    }

    fn set_image(&mut self, path: PathBuf, temp: Option<tempfile::TempDir>, rename: bool) {
        let changed = self.image_path() != Some(path.as_path());
        self.source = Some(Source::Image { path, _temp: temp });
        if rename {
            self.update_name();
        }
        if changed {
            self.source_changed();
        }
    }

    pub(super) fn poll_decode(&mut self) {
        let Some(done) = self.src.decode.as_ref().and_then(|d| finished(&d.job)) else {
            return;
        };
        let Some(decode) = self.src.decode.take() else {
            return;
        };
        let (path, temp) = match done {
            Ok(prepared) => prepared,
            Err(e) => {
                self.error(format!(
                    "Failed to decompress {}: {e}",
                    decode.original.display()
                ));
                (decode.original, None)
            }
        };
        self.set_image(path, temp, decode.rename);
    }

    /// The backup name from the source: its size, first volume label and the date.
    fn update_name(&mut self) {
        let Some(path) = self.source_path() else {
            return;
        };
        let device = match &self.source {
            Some(Source::Device { device, .. }) => Some(device),
            _ => None,
        };
        let size = device
            .map(|d| d.size_bytes)
            .or_else(|| std::fs::metadata(path.as_path()).ok().map(|m| m.len()));
        let label = device
            .and_then(|d| d.partitions.first())
            .map(|p| p.name.as_str())
            .filter(|n| !n.is_empty());
        self.name = crate::backup::format::generate_backup_name(path.as_path(), size, label);
    }

    /// The source changed: forget what was found, and preview an image (devices wait for a scan).
    pub(super) fn source_changed(&mut self) {
        // A Start waiting on the old source's minimum sizes must not fire on the new one.
        self.run.pending_start = false;
        self.clear_found();
        let Some(path) = self.image_path().map(|p| p.to_path_buf()) else {
            return;
        };
        if is_device_path(path.as_path()) {
            return;
        }
        self.src.scan = Some((
            spawn_scan(self, "Reading the image", move |job| {
                crate::model::source_scan::scan_image(path.as_path(), job)
            }),
            true,
        ));
    }

    pub(super) fn clear_found(&mut self) {
        self.src.stop_reading();
        self.found = super::Found::default();
    }

    /// "Load partition info": read the table off the UI thread (a device may prompt for credentials).
    pub(super) fn scan_partitions(&mut self) {
        self.clear_found();
        let Some(path) = self.source_path() else {
            return;
        };
        self.info(format!("Scanning partitions on {}...", path.display()));
        let is_device = is_device_path(path.as_path());
        // The elevated descriptor is cached process-wide; Close releases it so the drive can eject.
        self.escalated = if is_device { Some(path.clone()) } else { None };
        self.src.scan = Some((
            spawn_scan(self, "Scanning partitions", move |job| {
                crate::model::source_scan::scan_source(path.as_path(), is_device, job)
            }),
            false,
        ));
    }

    pub(super) fn poll_scan(&mut self) {
        let Some((job, preview)) = &self.src.scan else {
            return;
        };
        let snap = job.snapshot();
        if snap.is_running() {
            return;
        }
        let preview = *preview;
        let outcome = job.take_result();
        self.src.scan = None;
        let after_vhd = std::mem::take(&mut self.src.vhd_after_scan);
        match (snap.status, outcome) {
            (Status::Done, Some(Ok(scan))) => {
                self.apply_scan(scan, preview);
                if after_vhd {
                    self.open_vhd();
                }
            }
            (Status::Done, Some(Err(f))) => {
                if let Some(msg) = f.log_error {
                    self.error(msg);
                }
                self.found.load_error = Some(f.load_error);
            }
            (Status::Cancelled, _) => self.info("Partition scan cancelled"),
            (Status::Failed(msg), _) => {
                self.error(format!("Partition scan failed: {msg}"));
                self.found.load_error = Some(msg);
            }
            (Status::Running, _) | (Status::Done, None) => {}
        }
    }

    fn apply_scan(&mut self, scan: SourceScan, preview: bool) {
        for (warn, note) in scan.notes {
            if warn {
                self.warn(note);
            } else {
                self.info(note);
            }
        }
        self.found.partitions = scan.partitions;
        self.select_all();
        self.info(format!(
            "{} {} partition(s) ({})",
            if preview { "Source has" } else { "Found" },
            self.found.partitions.len(),
            scan.table_desc,
        ));
        self.found.table_desc = Some(scan.table_desc);
        for probe in scan.probes {
            let idx = probe.index;
            let type_str = probe.type_string.as_deref();
            match probe.result {
                crate::fs::MinimumResult::Computed {
                    in_place,
                    defragmented,
                    fragmentation_percent,
                } => {
                    let mins = &mut self.found.mins;
                    mins.record(
                        idx,
                        in_place,
                        defragmented,
                        fragmentation_percent,
                        probe.size_bytes,
                        probe.type_byte,
                        type_str,
                    );
                    if let Some(min) = mins.effective(idx, probe.type_byte, type_str) {
                        let suffix = mins.result_suffix(idx, fragmentation_percent);
                        self.info(format!(
                            "Partition {idx}: minimum size {} (original {}){suffix}",
                            format_size(min),
                            format_size(probe.size_bytes),
                        ));
                    }
                }
                crate::fs::MinimumResult::Deferred { fs_name } => {
                    self.info(format!(
                        "Partition {idx}: need to calculate minimum size due to filesystem {fs_name} (click \"Calc min\" to compute)",
                    ));
                    self.found.deferred.insert(idx, fs_name);
                }
            }
        }
        if let Some(f) = scan.source_file {
            self.found.source_file = Some(Arc::new(f));
        }
    }

    /// Select every volume (the extended container is not backed up on its own).
    pub(super) fn select_all(&mut self) {
        self.found.selected = self
            .found
            .partitions
            .iter()
            .filter(|p| !p.is_extended_container)
            .map(|p| p.index)
            .collect();
    }

    /// Close Drive: drop the handle so the OS releases the drive, and every derived state.
    pub(super) fn close_source(&mut self) {
        self.source = None;
        self.src.decode = None;
        self.clear_found();
        if let Some(path) = self.escalated.take() {
            crate::os::release_elevated_devices(Some(&*path.to_string_lossy()));
        }
        self.run.pending_start = false;
    }

    // --- Calc min -----------------------------------------------------------------------

    fn min_source(&self, index: usize) -> Result<MinSizeSource, String> {
        #[cfg(feature = "remote")]
        if let Some((conn, path)) = self.remote.source() {
            // A Backup-tab remote source is a drive, so the daemon opens it as a device.
            return Ok(MinSizeSource::Remote {
                conn,
                path,
                is_device: true,
            });
        }
        let path = self.source_path();
        match path.as_deref() {
            Some(p) if crate::model::source_reader::is_chd_path(p) => {
                Ok(MinSizeSource::Chd(p.to_path_buf()))
            }
            Some(p) if crate::model::source_reader::is_gho_path(p) => {
                Ok(MinSizeSource::Gho(p.to_path_buf()))
            }
            _ => match self.found.source_file.clone() {
                Some(file) => Ok(MinSizeSource::File {
                    file,
                    use_sector_aligned: path.as_deref().map(is_device_path).unwrap_or(false),
                }),
                None => Err(format!(
                    "Cannot calculate minimum size for partition {index}: no open source handle",
                )),
            },
        }
    }

    pub(super) fn calc_min(&mut self, index: usize) {
        if self.src.min.contains_key(&index) {
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
        let remote = self.remote_active();
        if remote && !self.remote_connected() {
            self.error(format!(
                "Cannot calculate minimum size for partition {index}: remote source not connected",
            ));
            return;
        }
        let source = match self.min_source(index) {
            Ok(s) => s,
            Err(e) => {
                self.error(e);
                return;
            }
        };
        let deferred = self.found.deferred.get(&index).copied();
        let offset = if remote {
            let fs_name = deferred.unwrap_or_else(|| {
                crate::fs::fs_name_for(part.gate_type_byte(), part.partition_type_string.as_deref())
            });
            self.info(format!(
                "Calculating minimum size for partition {index} ({fs_name}) over the wire...",
            ));
            part.byte_offset()
        } else {
            let fs_name = deferred.unwrap_or("unknown");
            self.info(format!(
                "Calculating minimum size for partition {index} ({fs_name})...",
            ));
            part.start_lba * 512
        };
        let req = MinSizeRequest {
            source,
            partition_offset: offset,
            partition_type: part.partition_type_byte,
            partition_type_string: part.partition_type_string.clone(),
            partition_size: part.size_bytes,
            partition_index: index,
        };
        let sink = Arc::clone(&self.log);
        let job = self.spawn("Calculating the minimum size", move |job| {
            min_size_in_job(req, index, &sink, job)
        });
        self.src.min.insert(index, job);
    }

    /// Start Calc min on every selected partition whose minimum is still deferred.
    pub(super) fn calc_deferred_selected(&mut self) {
        let list = crate::model::backup_plan::deferred_selected(
            self.found.partitions.as_slice(),
            &self.found.selected,
            &self.found.mins,
            &self.found.deferred,
        );
        for idx in list {
            self.calc_min(idx);
        }
    }

    pub(super) fn has_deferred_selected(&self) -> bool {
        !crate::model::backup_plan::deferred_selected(
            self.found.partitions.as_slice(),
            &self.found.selected,
            &self.found.mins,
            &self.found.deferred,
        )
        .is_empty()
    }

    pub(super) fn poll_min(&mut self) {
        let done: Vec<usize> = self
            .src
            .min
            .iter()
            .filter(|(_, j)| !j.is_running())
            .map(|(i, _)| *i)
            .collect();
        for idx in done {
            let Some(job) = self.src.min.remove(&idx) else {
                continue;
            };
            match finished(&job) {
                Some(Ok((in_place, defrag, frag, phase))) => {
                    self.min_found(idx, in_place, defrag, frag, phase)
                }
                Some(Err(err)) => self.error(format!(
                    "Minimum size calculation failed for partition {idx}: {err}",
                )),
                None => {}
            }
            self.found.deferred.remove(&idx);
        }
    }

    fn min_found(
        &mut self,
        idx: usize,
        in_place: Option<u64>,
        defrag: Option<u64>,
        frag: Option<f32>,
        phase: String,
    ) {
        let part = self.found.partitions.iter().find(|p| p.index == idx);
        let (size, ptype, pstring) = part
            .map(|p| {
                (
                    p.size_bytes,
                    p.partition_type_byte,
                    p.partition_type_string.clone(),
                )
            })
            .unwrap_or((u64::MAX, 0, None));
        let mins = &mut self.found.mins;
        mins.record(idx, in_place, defrag, frag, size, ptype, pstring.as_deref());
        match mins.effective(idx, ptype, pstring.as_deref()) {
            Some(min) => {
                let suffix = mins.result_suffix(idx, frag);
                self.info(format!(
                    "Partition {idx}: minimum size {} (computed){suffix}",
                    format_size(min),
                ));
                self.run.min_found(idx, min);
            }
            None => self.info(format!(
                "Partition {idx}: filesystem could not be opened for minimum-size calculation (last phase: {phase})",
            )),
        }
    }
}

/// Run a scan as a job, handing a [`ScanFailure`] back as the result so the view can place it.
fn spawn_scan(
    ctl: &BackupController,
    label: &str,
    scan: impl FnOnce(&JobCtx) -> anyhow::Result<SourceScan> + Send + 'static,
) -> Job<ScanOutcome> {
    ctl.spawn(label, move |job| match scan(job) {
        Ok(found) => Ok(Ok(found)),
        Err(e) => match e.downcast::<ScanFailure>() {
            Ok(f) => Ok(Err(f)),
            Err(e) => Err(e),
        },
    })
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
        // Each phase is logged so a long HFS+ catalog walk does not look like a hang.
        for phase in s.phase_log.drain(..) {
            if !phase.is_empty() {
                push_log(sink, LogLevel::Info, format!("Partition {index}: {phase}"));
            }
        }
        ctx.set_phase(s.phase.as_str());
        if ctx.is_cancelled() {
            s.cancel_requested = true;
        }
        if s.finished {
            out = Some(match s.error.take() {
                Some(e) => Err(anyhow::anyhow!(e)),
                None => Ok((
                    s.result,
                    s.defragmented_min,
                    s.fragmentation_percent,
                    s.phase.clone(),
                )),
            });
        }
        s.finished
    });
    out.unwrap_or_else(|| Err(anyhow::anyhow!("no result")))
}
