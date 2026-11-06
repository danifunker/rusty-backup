//! Starting and running: the confirmations and the restore jobs of each mode.

use std::sync::{Arc, Mutex};

use super::source::finished;
use super::{
    path_label, purpose, push_log, RestoreBar, RestoreController, RestoreMode, SizeMode, TargetSlot,
};
use crate::controller::log::LogLevel;
use crate::controller::{ConfirmSpec, Effect};
use crate::model::job::Job;
use crate::restore::RestoreProgress;
use disk_core::display::format_size;

/// What a finished restore returns: `Err` carries the engine's message.
type RunOutcome = Result<(), String>;

/// The engine call a restore job makes with its shared progress.
type RestoreBody = Box<dyn FnOnce(Arc<Mutex<RestoreProgress>>) -> anyhow::Result<()> + Send>;

#[derive(Default)]
pub(super) struct RunState {
    running: Option<(Job<RunOutcome>, Arc<Mutex<RestoreProgress>>)>,
    pub last: Option<RunOutcome>,
}

impl RunState {
    pub fn running(&self) -> bool {
        self.running.is_some()
    }

    pub fn bar(&self) -> Option<RestoreBar> {
        let (_, progress) = self.running.as_ref()?;
        let p = crate::model::worker::lock_status(&**progress);
        Some(RestoreBar {
            operation: p.operation.clone(),
            current_bytes: p.current_bytes,
            total_bytes: p.total_bytes,
        })
    }
}

impl RestoreController {
    /// The Restore button: ask before overwriting the target.
    pub(super) fn start(&mut self) -> Vec<Effect> {
        if self.run.running() {
            return Vec::new();
        }
        if let Err(e) = self.validate() {
            self.warn(e.as_str());
            self.run.last = Some(Err(e));
            return Vec::new();
        }
        self.run.last = None;
        let (message, destructive_title) = match self.mode {
            RestoreMode::FullDisk => (self.full_disk_question(), "Confirm Restore"),
            RestoreMode::SinglePartition => (self.single_question(), "Confirm Restore"),
            RestoreMode::NewDisk => (self.new_disk_question(), "Confirm Restore"),
            RestoreMode::WriteImage => return Vec::new(),
        };
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::RESTORE,
            title: destructive_title.to_string(),
            message,
            confirm_label: "I understand, proceed".to_string(),
            destructive: true,
        })]
    }

    /// The full-disk target's name, as the confirmation and the log say it.
    fn full_disk_target_name(&self) -> String {
        if let Some(d) = self
            .remote_active()
            .then(|| self.remote_target_label())
            .flatten()
        {
            return d.to_string();
        }
        let t = &self.targets.full;
        if t.is_device {
            t.device
                .as_ref()
                .map(|p| p.device.display_name())
                .unwrap_or_else(|| "Unknown".into())
        } else {
            path_label(t.image.as_deref())
        }
    }

    fn full_disk_question(&self) -> String {
        let mut msg = format!(
            "All data on {} will be permanently overwritten!\n",
            self.full_disk_target_name()
        );
        for row in self.size_rows() {
            let size = if row.mode == SizeMode::FillRemaining {
                "Fill remaining".to_string()
            } else {
                format_size(row.effective_size())
            };
            msg.push_str(
                format!("\n  Partition {}: {} -> {}", row.index, row.type_name, size).as_str(),
            );
        }
        msg
    }

    fn single_question(&self) -> String {
        let target = self
            .device(TargetSlot::SinglePartition)
            .map(|d| d.display_name())
            .unwrap_or_else(|| "Unknown".into());
        let part = self
            .targets
            .sp_part
            .and_then(|idx| self.targets.sp_parts.iter().find(|p| p.index == idx))
            .map(|p| {
                format!(
                    "Partition {} ({}, {})",
                    p.index,
                    p.type_name,
                    format_size(p.size_bytes)
                )
            })
            .unwrap_or_else(|| "Unknown".into());
        format!(
            "{part} on {target} will be overwritten.\nThe partition table will NOT be modified."
        )
    }

    fn new_disk_question(&self) -> String {
        let t = &self.targets.new_disk;
        let target = if t.is_device {
            t.device
                .as_ref()
                .map(|p| p.device.display_name())
                .unwrap_or_else(|| "Unknown".into())
        } else {
            path_label(t.image.as_deref())
        };
        let table = match self.targets.new_disk_choice.table {
            super::NewTableType::Mbr => "MBR",
            super::NewTableType::Gpt => "GPT",
            super::NewTableType::Apm => "APM",
        };
        format!(
            "A new {table} partition table will be written to {target}!\nAll existing data will be destroyed."
        )
    }

    pub(super) fn start_confirmed(&mut self) {
        if self.run.running() || self.validate().is_err() {
            return;
        }
        match self.mode {
            RestoreMode::FullDisk => self.start_full_disk(),
            RestoreMode::SinglePartition => self.start_single(),
            RestoreMode::NewDisk => self.start_new_disk(),
            RestoreMode::WriteImage => {}
        }
    }

    fn start_full_disk(&mut self) {
        let Some(folder) = self.loaded().map(|l| l.folder.clone()) else {
            self.error("No backup folder selected");
            return;
        };
        #[cfg(feature = "remote")]
        if self.remote_active() {
            self.start_remote(folder);
            return;
        }
        let t = &self.targets.full;
        let (target_path, target_is_device, target_size) = if t.is_device {
            let Some(p) = t.device.as_ref() else {
                self.error("No target device selected");
                return;
            };
            (p.device.path.clone(), true, p.device.size_bytes)
        } else {
            let Some(path) = t.image.clone() else {
                self.error("No image file selected");
                return;
            };
            (path, false, self.target_size())
        };
        let config = crate::restore::RestoreConfig {
            backup_folder: folder,
            target_path: target_path.clone(),
            target_is_device,
            target_size,
            alignment: self.resolved_alignment(),
            partition_sizes: self.partition_sizes(target_is_device),
            write_zeros_to_unused: false,
            install_mac_driver: self.targets.install_mac_driver,
        };
        self.info(format!("Starting restore to {}", target_path.display()));
        self.spawn_restore(
            "disk restore",
            "Rusty Backup: disk restore",
            Box::new(move |p| crate::restore::run_restore(config, p)),
        );
    }

    /// Restore into a local staging image and push it to the daemon (`model::restore_remote`).
    #[cfg(feature = "remote")]
    fn start_remote(&mut self, folder: std::path::PathBuf) {
        let Some((conn, path, display, is_device)) = self.remote.selection() else {
            self.error("No remote target selected");
            return;
        };
        let base = self.source_size().unwrap_or(0);
        // An image honours the trailing free space; the daemon checks the image fits a device.
        let target_size = if is_device {
            base
        } else {
            base.saturating_add(self.targets.expand.free_bytes(false))
        };
        let config = crate::restore::RestoreConfig {
            backup_folder: folder,
            target_path: std::path::PathBuf::new(),
            target_is_device: false,
            target_size,
            alignment: self.resolved_alignment(),
            partition_sizes: self.partition_sizes(is_device),
            write_zeros_to_unused: false,
            install_mac_driver: self.targets.install_mac_driver,
        };
        self.info(format!("Starting restore to {display}"));
        self.spawn_restore(
            "remote restore",
            "Rusty Backup: remote restore",
            Box::new(move |p| {
                // The picker is the confirmation, as for a local image target.
                crate::model::restore_remote::restore_to_remote(
                    config,
                    conn,
                    path.as_str(),
                    is_device,
                    p,
                    None,
                    true,
                )
            }),
        );
    }

    fn start_single(&mut self) {
        let Some(source) = self.partition_source() else {
            self.error("No source selected");
            return;
        };
        let Some(device) = self.device(TargetSlot::SinglePartition).cloned() else {
            self.error("No target device selected");
            return;
        };
        if self.targets.sp_identity.as_ref() != Some(&device.identity()) {
            self.error("The target device changed since it was scanned; scan it again");
            return;
        }
        let Some(part) = self
            .targets
            .sp_part
            .and_then(|idx| self.targets.sp_parts.iter().find(|p| p.index == idx))
            .cloned()
        else {
            self.error("No target partition selected");
            return;
        };
        let config = crate::model::restore_plan::single_partition_config(
            &source,
            device.path.as_path(),
            part.start_lba,
            part.size_bytes,
        );
        self.info(format!(
            "Starting single-partition restore to partition {} on {}",
            part.index,
            device.path.display(),
        ));
        self.spawn_restore(
            "partition restore",
            "Rusty Backup: partition restore",
            Box::new(move |p| crate::restore::single::run_single_partition_restore(config, p)),
        );
    }

    fn start_new_disk(&mut self) {
        let Some(source) = self.partition_source() else {
            self.error("No source selected");
            return;
        };
        let t = &self.targets.new_disk;
        let (target, is_device, device_size) = if t.is_device {
            let Some(p) = t.device.as_ref() else {
                self.error("No target device selected");
                return;
            };
            (p.device.path.clone(), true, Some(p.device.size_bytes))
        } else {
            let Some(path) = t.image.clone() else {
                self.error("No target file selected");
                return;
            };
            (path, false, None)
        };
        let config = crate::model::restore_plan::new_disk_config(
            &source,
            target.as_path(),
            is_device,
            device_size,
            &self.targets.new_disk_choice,
        );
        self.info(format!("Starting new-disk restore to {}", target.display()));
        self.spawn_restore(
            "new-disk restore",
            "Rusty Backup: new-disk restore",
            Box::new(move |p| crate::restore::single::run_single_partition_restore(config, p)),
        );
    }

    /// Run `body` as the restore job, mirroring its progress and log into the controller.
    fn spawn_restore(&mut self, what: &str, wake_reason: &'static str, body: RestoreBody) {
        let progress = Arc::new(Mutex::new(RestoreProgress::new()));
        let shared = Arc::clone(&progress);
        let sink = Arc::clone(&self.log);
        // A `.cbk`'s unpacked folder outlives the restore reading it.
        let guard = self.src.guard.clone();
        let job = self.spawn(what, move |job| {
            let _guard = guard;
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
        self.run.running = Some((job, progress));
    }

    pub(super) fn cancel(&mut self) {
        let Some((job, progress)) = &self.run.running else {
            return;
        };
        job.cancel();
        let mut p = crate::model::worker::lock_status(&**progress);
        p.cancel_requested = true;
        p.operation = "Cancelling - waiting for current write to complete...".to_string();
        drop(p);
        self.warn("Cancellation requested - waiting for current disk write to complete...");
    }

    pub(super) fn poll_run(&mut self) {
        let Some(done) = self.run.running.as_ref().and_then(|(j, _)| finished(j)) else {
            return;
        };
        self.run.running = None;
        match done.and_then(|inner| inner) {
            Ok(()) => {
                self.info("Restore completed successfully.");
                self.run.last = Some(Ok(()));
            }
            Err(e) => {
                let msg = format!("Restore failed: {e}");
                self.error(msg.as_str());
                self.run.last = Some(Err(msg));
            }
        }
        if self.src.load_deferred {
            self.start_load();
        }
    }
}
