//! Write: the device-name prompt or the overwrite question, then the write job.

use std::sync::{Arc, Mutex};

use super::{finished, purpose, push_log, DiskWriteController};
use crate::controller::log::LogLevel;
use crate::controller::{ConfirmSpec, Effect, PickSpec, TextSpec};
use crate::model::job::{Job, Snapshot};
use crate::model::physical_write_runner::{self, PhysicalWriteStatus};

/// A finished write: the bytes written, or the engine's message.
type Outcome = Result<u64, String>;

#[derive(Default)]
pub(super) struct RunState {
    job: Option<Job<Outcome>>,
    /// The finished write's last snapshot, for its bar.
    done: Option<Snapshot>,
    pub last: Option<Outcome>,
    /// The question on screen: a yes only counts for exactly what it named.
    pub asked: Option<String>,
}

impl RunState {
    pub fn running(&self) -> bool {
        self.job.is_some()
    }

    pub fn cancel(&self) {
        if let Some(job) = &self.job {
            job.cancel();
        }
    }
}

/// The device prompt's rule: the answer must be the device's name.
fn device_name_typed(spec: &TextSpec, text: &str) -> Result<(), String> {
    if spec.taken.first().map(|s| s.as_str()) == Some(text) {
        Ok(())
    } else {
        Err(String::new())
    }
}

impl DiskWriteController {
    /// A write is in flight; the hosting tab's controls stay locked.
    pub fn is_writing(&self) -> bool {
        self.run.running()
    }

    /// The write's progress, running or finished.
    pub fn progress(&self) -> Option<Snapshot> {
        match &self.run.job {
            Some(job) => Some(job.snapshot()),
            None => self.run.done.clone(),
        }
    }

    /// How the last write ended: the bytes written, or why it failed.
    pub fn result(&self) -> Option<&Result<u64, String>> {
        self.run.last.as_ref()
    }

    /// A write started here (running or finished): the window shows its progress, not the form.
    pub fn has_run(&self) -> bool {
        self.run.job.is_some() || self.run.done.is_some()
    }

    /// Write is enabled: a source, a target, nothing being read or written.
    pub fn can_write(&self) -> bool {
        self.source.is_some()
            && (self.target_device().is_some() || self.target_image().is_some())
            && !self.target.scanning()
            && !self.run.running()
    }

    /// The target as the questions and the window name it.
    pub fn write_target_name(&self) -> Option<String> {
        if let Some(d) = self.target_device() {
            return Some(d.display_name());
        }
        self.target_image().map(|p| p.display().to_string())
    }

    /// What Write asks now: the target, what part of it, and what goes there.
    fn question(&self) -> Option<String> {
        let target = self.write_target_name()?;
        let source = self.source_label();
        let what = match self.chosen_partition() {
            Some(p) => format!(
                "Writing replaces the contents of partition {} ({}) on {target}.",
                p.index + 1,
                p.type_name
            ),
            None if self.target_device().is_some() => {
                format!("Writing erases all data on {target}.")
            }
            None => format!("Write the image to {target}? Its contents are replaced."),
        };
        Some(format!("{what}\nSource: {source}"))
    }

    /// Check the form, then ask: a device wants its name typed, an image file a yes.
    pub(super) fn write(&mut self) -> Vec<Effect> {
        if !self.can_write() {
            return Vec::new();
        }
        if let Err(e) = self.request() {
            self.error = Some(e);
            return Vec::new();
        }
        self.error = None;
        let Some(message) = self.question() else {
            return Vec::new();
        };
        self.run.asked = Some(message.clone());
        if let Some(name) = self.device_name() {
            return vec![Effect::AskText(TextSpec {
                purpose: purpose::DEVICE_NAME,
                title: "Confirm Write".to_string(),
                prompt: format!(
                    "{message}\nConfirm: type the device name `{name}` to enable Write"
                ),
                initial: String::new(),
                ok_label: "Write to disk".to_string(),
                unchanged: String::new(),
                taken: vec![name],
                fold_case: false,
                forbidden: String::new(),
                error: None,
                validator: device_name_typed,
            })];
        }
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::OVERWRITE_FILE,
            title: "Confirm Write".to_string(),
            message,
            confirm_label: "Write".to_string(),
            destructive: true,
        })]
    }

    /// The asked question, if it still names what a write would do now.
    fn still_asked(&mut self) -> bool {
        let Some(asked) = self.run.asked.take() else {
            return false;
        };
        if self.question().as_deref() == Some(asked.as_str()) {
            return true;
        }
        push_log(
            &self.log,
            LogLevel::Warning,
            "The write target changed while the question was open; nothing was written.",
        );
        false
    }

    /// The image-file question's answer.
    pub(super) fn file_answered(&mut self, yes: bool) {
        if !yes {
            self.run.asked = None;
            return;
        }
        if self.target_device().is_none() && self.still_asked() {
            self.start_write();
        }
    }

    /// The typed device name came back: check it and the question again, then write.
    pub(super) fn device_name_answered(&mut self, text: Option<String>) {
        let Some(text) = text else {
            self.run.asked = None;
            return;
        };
        let name = self.device_name().unwrap_or_default();
        if name.is_empty() || text != name {
            self.run.asked = None;
            push_log(
                &self.log,
                LogLevel::Warning,
                "The typed name does not match the device; nothing was written.",
            );
            return;
        }
        if self.still_asked() {
            self.start_write();
        }
    }

    /// Ask where to write the image file.
    pub(super) fn pick_target_image(&self) -> Vec<Effect> {
        let name = self
            .source
            .as_ref()
            .and_then(|s| s.path.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::TARGET_IMAGE,
            title: "Write to an image file".to_string(),
            filters: Vec::new(),
            save: true,
            file_name: name,
        })]
    }

    fn request(&self) -> Result<physical_write_runner::PhysicalWriteRequest, String> {
        let source = self.source.as_ref().ok_or("No source")?;
        let target = self.write_target().ok_or("No target selected")?;
        let partition = match self.target.partition {
            Some(_) => Some(
                self.chosen_partition()
                    .ok_or("Selected partition is no longer present")?,
            ),
            None => None,
        };
        let mut form = self.form;
        form.synthesize = form.synthesize && !source.has_partition_table;
        crate::model::disk_write_plan::build_request(source, &form, &target, partition)
    }

    pub(super) fn start_write(&mut self) {
        if self.run.running() {
            return;
        }
        let req = match self.request() {
            Ok(r) => r,
            Err(e) => {
                self.error = Some(e);
                return;
            }
        };
        let total = match &req.wrap {
            Some(p) => p.partition_size_bytes,
            None => self.source.as_ref().map_or(0, |s| s.size_bytes),
        };
        push_log(
            &self.log,
            LogLevel::Info,
            format!(
                "Writing {} to {}",
                self.source_label(),
                req.target_device_path.display()
            ),
        );
        let sink = Arc::clone(&self.log);
        self.run.done = None;
        self.run.last = None;
        self.run.job = Some(self.spawn("Writing to disk", move |ctx| {
            let _wake = crate::os::wakelock::acquire("Rusty Backup: physical disk export");
            let status = Arc::new(Mutex::new(PhysicalWriteStatus {
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
                    for msg in s.log_messages.drain(..) {
                        push_log(&sink, LogLevel::Info, msg);
                    }
                    ctx.set_bytes(s.current_bytes, s.total_bytes);
                    if ctx.is_cancelled() {
                        s.cancel_requested = true;
                    }
                },
                || physical_write_runner::run_worker(&req, Arc::clone(&status)),
            );
            let written = crate::model::worker::lock_status(&*status).current_bytes;
            Ok(match result {
                Ok(()) => Ok(written),
                Err(e) => Err(format!("{e:#}")),
            })
        }));
    }

    fn source_label(&self) -> String {
        self.source
            .as_ref()
            .map(|s| s.path.display().to_string())
            .unwrap_or_default()
    }

    pub(super) fn poll_run(&mut self) {
        let Some(done) = self.run.job.as_ref().and_then(finished) else {
            return;
        };
        self.run.done = self.run.job.take().map(|j| j.snapshot());
        let outcome = done.and_then(|inner| inner);
        match &outcome {
            Ok(n) => push_log(
                &self.log,
                LogLevel::Info,
                format!("Wrote {} to the target", super::format_size(*n)),
            ),
            Err(e) => push_log(
                &self.log,
                LogLevel::Error,
                format!("Disk write failed: {e}"),
            ),
        }
        self.run.last = Some(outcome);
    }
}
