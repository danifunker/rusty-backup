//! Starting and running a backup: the start rules, the VHD window, the backup and VHD export jobs.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::source::finished;
use super::{push_log, BackupBar, BackupController, ChdChoice, ChdOptions, SizeMode, VhdRow};
use crate::backup::{BackupConfig, BackupProgress};
use crate::controller::log::LogLevel;
use crate::model::job::{Job, Progress};

/// What a finished backup or export returns: `Err` carries the engine's message.
type RunOutcome = Result<(), String>;

enum Running {
    Backup {
        job: Job<RunOutcome>,
        progress: Arc<Mutex<BackupProgress>>,
    },
    Vhd {
        job: Job<RunOutcome>,
    },
}

/// Why Start waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pending {
    /// The image decode or scan: then the start rules run again.
    Scan,
    /// The selected partitions' minimum sizes: then the backup starts.
    Minimums,
}

pub(super) struct RunState {
    running: Option<Running>,
    pub last: Option<RunOutcome>,
    pub pending_start: bool,
    pending: Option<Pending>,
    pub vhd: Option<Vec<VhdRow>>,
    pub vhd_whole_disk: bool,
}

impl Default for RunState {
    fn default() -> Self {
        RunState {
            running: None,
            last: None,
            pending_start: false,
            pending: None,
            vhd: None,
            vhd_whole_disk: true,
        }
    }
}

impl RunState {
    pub fn running(&self) -> bool {
        self.running.is_some()
    }

    pub fn bar(&self) -> Option<BackupBar> {
        match self.running.as_ref()? {
            Running::Backup { progress, .. } => {
                let p = crate::model::worker::lock_status(&**progress);
                Some(BackupBar {
                    operation: p.operation.clone(),
                    current_bytes: p.current_bytes,
                    total_bytes: p.total_bytes,
                    full_size_bytes: p.full_size_bytes,
                })
            }
            Running::Vhd { job } => {
                let (current, total) = match job.snapshot().progress {
                    Progress::Determinate { done, total, .. } => (done, total),
                    _ => (0, 0),
                };
                Some(BackupBar {
                    operation: "Exporting VHD...".into(),
                    current_bytes: current,
                    total_bytes: total,
                    full_size_bytes: 0,
                })
            }
        }
    }

    pub fn set_vhd_size(&mut self, index: usize, mode: SizeMode, custom_mib: u32) {
        if let Some(row) = self
            .vhd
            .as_mut()
            .and_then(|rows| rows.iter_mut().find(|r| r.index == index))
        {
            // Switching into Custom starts from the minimum, as the size cell shows it.
            row.custom_mib = if mode == SizeMode::Custom && row.mode != SizeMode::Custom {
                row.custom_bounds_mib().0
            } else {
                custom_mib
            };
            row.mode = mode;
        }
    }

    /// A Calc min finished: the VHD row takes the new minimum.
    pub fn min_found(&mut self, index: usize, min: u64) {
        for row in self.vhd.iter_mut().flatten() {
            if row.index == index {
                row.minimum_size = min;
                row.deferred_fs = None;
            }
        }
    }

    fn set_pending(&mut self, why: Option<Pending>) {
        self.pending = why;
        self.pending_start = why.is_some();
    }
}

impl BackupController {
    /// Start Backup: VHD asks for its layout, a resize-to-minimum CHD computes minimums first.
    pub(super) fn start(&mut self, chd: Option<ChdChoice>) {
        if self.run.running() {
            return;
        }
        match chd {
            Some(c) => {
                self.opts.chd = Some(c.options);
                self.chd_custom = c.custom;
            }
            None => {
                self.opts.chd = None;
                self.chd_custom = false;
            }
        }
        if let Err(e) = self.validate() {
            self.warn(e.as_str());
            self.run.last = Some(Err(e));
            return;
        }
        self.run.last = None;
        if !self.src.idle() {
            self.info("Waiting for the source to be read before starting...");
            self.run.set_pending(Some(Pending::Scan));
            return;
        }
        let wait_for_minimums = self.opts.needs_minimums() && self.has_deferred_selected();
        if self.remote_active() {
            // The VHD window is local-only; a remote drive's minimums are walked over the wire.
            if wait_for_minimums {
                self.calc_deferred_selected();
                self.run.set_pending(Some(Pending::Minimums));
            } else {
                self.begin();
            }
        } else if self.opts.compression == super::CompressionType::Vhd {
            if self.has_partitions() {
                self.open_vhd();
            } else {
                self.scan_partitions();
                self.src.vhd_after_scan = true;
            }
        } else if wait_for_minimums {
            self.calc_deferred_selected();
            self.run.set_pending(Some(Pending::Minimums));
        } else {
            self.begin();
        }
    }

    pub(super) fn cancel_pending_start(&mut self) {
        if self.run.pending_start {
            self.run.set_pending(None);
            self.info(
                "Auto-backup canceled; minimum-size calculations will still complete in the background",
            );
        }
    }

    pub(super) fn poll_pending_start(&mut self) {
        let Some(why) = self.run.pending else {
            return;
        };
        if self.run.running() || !self.src.idle() || self.src.min_running() {
            return;
        }
        self.run.set_pending(None);
        match why {
            Pending::Scan => {
                let chd = self.opts.chd.clone().map(|options| ChdChoice {
                    options,
                    custom: self.chd_custom,
                });
                self.start(chd);
            }
            Pending::Minimums => self.begin(),
        }
    }

    pub(super) fn cancel(&mut self) {
        match &self.run.running {
            Some(Running::Backup { job, .. }) | Some(Running::Vhd { job }) => job.cancel(),
            None => return,
        }
        self.warn("Cancellation requested...");
    }

    /// The backup itself, from the remote drive or the local source.
    fn begin(&mut self) {
        if self.remote_active() {
            self.start_remote_backup();
        } else {
            self.start_local_backup();
        }
    }

    pub(super) fn open_vhd(&mut self) {
        let rows = self
            .found
            .partitions
            .iter()
            .filter(|p| !p.is_extended_container)
            .map(|p| VhdRow {
                index: p.index,
                type_name: p.type_name.clone(),
                start_lba: p.start_lba,
                original_size: p.size_bytes,
                minimum_size: self
                    .found
                    .mins
                    .in_place
                    .get(&p.index)
                    .copied()
                    .unwrap_or(p.size_bytes),
                mode: SizeMode::Original,
                custom_mib: (p.size_bytes / (1024 * 1024)) as u32,
                deferred_fs: self.found.deferred.get(&p.index).copied(),
            })
            .collect();
        self.run.vhd = Some(rows);
    }

    pub(super) fn vhd_start(&mut self) {
        if self.run.vhd.is_none() || self.run.running() {
            return;
        }
        if self.run.vhd_whole_disk {
            self.start_vhd_whole_disk();
        } else {
            // One .vhd per partition is the ordinary backup flow.
            self.start_local_backup();
        }
        self.run.vhd = None;
    }

    fn remember_chd(&self) {
        if let (true, Some(opts)) = (self.chd_custom, self.opts.chd.as_ref()) {
            if self.opts.compression == super::CompressionType::Chd {
                persist_chd(opts);
            }
        }
    }

    fn config_for(&self, source: PathBuf) -> Option<BackupConfig> {
        let Some(destination) = self.destination.clone() else {
            self.error("No destination folder selected");
            return None;
        };
        Some(crate::model::backup_plan::build_config(
            source,
            destination,
            self.name.clone(),
            &self.opts,
            self.found.partitions.as_slice(),
            &self.found.selected,
            &self.found.mins,
        ))
    }

    fn start_local_backup(&mut self) {
        let Some(source) = self.source_path() else {
            self.error("No source selected");
            return;
        };
        let Some(config) = self.config_for(source) else {
            return;
        };
        self.remember_chd();
        self.info(format!(
            "Starting backup: {} -> {}",
            config.source_path.display(),
            config.destination_dir.join(config.backup_name.as_str()).display()
        ));
        self.spawn_backup(
            "Rusty Backup: disk backup",
            Box::new(move |progress| crate::backup::run_backup(config, progress)),
        );
    }

    #[cfg(feature = "remote")]
    fn start_remote_backup(&mut self) {
        let Some((conn, path, display)) = self.remote.selection() else {
            return;
        };
        let Some(config) = self.config_for(PathBuf::from(path.as_str())) else {
            return;
        };
        self.remember_chd();
        self.info(format!(
            "Starting remote backup: {display} -> {}",
            config.destination_dir.join(config.backup_name.as_str()).display()
        ));
        let source = crate::backup::BackupSource::Remote {
            conn,
            path,
            display,
            is_device: true,
        };
        self.spawn_backup(
            "Rusty Backup: remote drive backup",
            Box::new(move |progress| crate::backup::run_backup_from(source, config, progress)),
        );
    }

    #[cfg(not(feature = "remote"))]
    fn start_remote_backup(&mut self) {}

    /// Run `body` as the backup job, mirroring its progress and log into the controller.
    #[allow(clippy::type_complexity)]
    fn spawn_backup(
        &mut self,
        wake_reason: &'static str,
        body: Box<dyn FnOnce(Arc<Mutex<BackupProgress>>) -> anyhow::Result<()> + Send>,
    ) {
        let progress = Arc::new(Mutex::new(BackupProgress::new()));
        let shared = Arc::clone(&progress);
        let sink = Arc::clone(&self.log);
        let job = self.spawn("Backup", move |job| {
            let _wake = crate::os::wakelock::acquire(wake_reason);
            let mut err = None;
            crate::model::worker::run_mirrored(
                job,
                &shared,
                |p, ctx| {
                    while let Some(msg) = p.log_messages.pop_front() {
                        let level = match msg.level {
                            crate::backup::LogLevel::Info => LogLevel::Info,
                            crate::backup::LogLevel::Warning => LogLevel::Warning,
                            crate::backup::LogLevel::Error => LogLevel::Error,
                        };
                        push_log(&sink, level, msg.message);
                    }
                    ctx.set_phase(p.operation.as_str());
                    ctx.set_bytes(p.current_bytes, p.total_bytes);
                    if ctx.is_cancelled() {
                        p.cancel_requested = true;
                    }
                },
                || {
                    if let Err(e) = body(Arc::clone(&shared)) {
                        err = Some(format!("{e:#}"));
                    }
                },
            );
            let err = err.or_else(|| crate::model::worker::lock_status(&*shared).error.take());
            Ok(match err {
                Some(e) => Err(e),
                None => Ok(()),
            })
        });
        self.run.running = Some(Running::Backup { job, progress });
    }

    fn start_vhd_whole_disk(&mut self) {
        let Some(source) = self.source_path() else {
            self.error("No source selected");
            return;
        };
        let Some(dest_dir) = self.destination.clone() else {
            self.error("No destination folder selected");
            return;
        };
        let dest = dest_dir.join(format!("{}.vhd", self.name));
        let overrides: Vec<part_tables::PartitionSizeOverride> = self
            .run
            .vhd
            .iter()
            .flatten()
            .map(|row| {
                part_tables::PartitionSizeOverride::size_only(
                    row.index,
                    row.start_lba,
                    row.original_size,
                    row.effective_size(),
                )
            })
            .collect();
        let total: u64 = overrides.iter().map(|o| o.export_size).sum();
        self.info(format!(
            "Starting whole-disk VHD backup: {} -> {}",
            source.display(),
            dest.display()
        ));
        let sink = Arc::clone(&self.log);
        let job = self.spawn("Exporting VHD", move |job| {
            let _wake = crate::os::wakelock::acquire("Rusty Backup: whole-disk VHD backup");
            job.set_phase("Exporting VHD...");
            job.set_bytes(0, total);
            let res = crate::rbformats::vhd::export_whole_disk_vhd(
                source.as_path(),
                None,
                None,
                overrides.as_slice(),
                dest.as_path(),
                |bytes| job.set_bytes(bytes, total),
                || job.is_cancelled(),
                |msg| push_log(&sink, LogLevel::Info, msg),
            );
            Ok(res.map_err(|e| format!("{e:#}")))
        });
        self.run.running = Some(Running::Vhd { job });
    }

    pub(super) fn poll_run(&mut self) {
        let (done, vhd) = match &self.run.running {
            Some(Running::Backup { job, .. }) => (finished(job), false),
            Some(Running::Vhd { job }) => (finished(job), true),
            None => return,
        };
        let Some(done) = done else {
            return;
        };
        self.run.running = None;
        let outcome = match done {
            Ok(inner) => inner,
            Err(e) => Err(e),
        };
        let what = if vhd { "VHD backup" } else { "Backup" };
        match outcome {
            Ok(()) => {
                if vhd {
                    self.info("VHD backup completed successfully.");
                }
                self.run.last = Some(Ok(()));
            }
            Err(e) => {
                let msg = format!("{what} failed: {e}");
                self.error(msg.as_str());
                self.run.last = Some(Err(msg));
            }
        }
    }
}

/// Remember a Custom CHD choice in config.json for the next launch.
#[cfg(all(
    feature = "chd",
    any(feature = "gui", feature = "tui", feature = "tui-update")
))]
fn persist_chd(opts: &ChdOptions) {
    let mut cfg = crate::update::UpdateConfig::load();
    cfg.last_chd_codecs = Some(
        opts.codecs
            .iter()
            .map(|c| crate::rbformats::chd_options::codec_label(*c))
            .collect::<Vec<_>>()
            .join(","),
    );
    cfg.last_chd_hunk_size = Some(opts.hunk_size);
    let _ = cfg.save();
}

#[cfg(not(all(
    feature = "chd",
    any(feature = "gui", feature = "tui", feature = "tui-update")
)))]
fn persist_chd(_: &ChdOptions) {}

/// The last Custom CHD choice from config.json, laid over `base`.
#[cfg(all(
    feature = "chd",
    any(feature = "gui", feature = "tui", feature = "tui-update")
))]
pub(super) fn saved_chd_custom(mut base: ChdOptions) -> ChdOptions {
    let cfg = crate::update::UpdateConfig::load();
    if let Some(spec) = cfg.last_chd_codecs.as_deref() {
        if let Ok(codecs) = crate::rbformats::chd_options::parse_codec_string(spec) {
            base.codecs = codecs;
        }
    }
    if let Some(hs) = cfg.last_chd_hunk_size {
        if hs % 512 == 0 {
            base.hunk_size = hs;
        }
    }
    base
}

#[cfg(not(all(
    feature = "chd",
    any(feature = "gui", feature = "tui", feature = "tui-update")
)))]
pub(super) fn saved_chd_custom(base: ChdOptions) -> ChdOptions {
    base
}
