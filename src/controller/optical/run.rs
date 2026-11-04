//! Start, Cancel and the rip / conversion job, with its log lines and the shared bar.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::{ChdChoice, ChdOptions, ChdProfile, OpticalBar, OpticalController, OutputFormat, Task};
use crate::controller::job_outcome;
use crate::controller::log::LogLevel;
use crate::model::job::Job;
use crate::model::optical_ops;
use crate::model::worker::lock_status;
use crate::optical::convert::ConvertProgress;
use crate::optical::rip::RipProgress;

/// Which engine status a run reports through, and the words its result uses.
enum Progress {
    Rip(Arc<Mutex<RipProgress>>),
    /// A conversion, or a rip to CHD (which reports as one).
    Convert(Arc<Mutex<ConvertProgress>>),
}

struct Running {
    job: Job<()>,
    progress: Progress,
}

#[derive(Default)]
pub(super) struct RunState {
    running: Option<Running>,
    pub last: Option<Result<(), String>>,
}

impl RunState {
    pub fn running(&self) -> bool {
        self.running.is_some()
    }
}

/// Record a worker's error the way the tab's threads did: a rip overwrites, a conversion keeps the first.
fn fail_rip(p: &Arc<Mutex<RipProgress>>, e: anyhow::Error) {
    let mut p = lock_status(&**p);
    p.error = Some(format!("{e:#}"));
    p.finished = true;
}

fn fail_convert(p: &Arc<Mutex<ConvertProgress>>, e: anyhow::Error) {
    let mut p = lock_status(&**p);
    if !p.finished {
        p.error = Some(format!("{e:#}"));
        p.finished = true;
    }
}

impl OpticalController {
    /// A rip or conversion is in flight.
    pub fn is_running(&self) -> bool {
        self.run.running()
    }

    /// The shared bar's numbers while a run is in flight.
    pub fn progress(&self) -> Option<OpticalBar> {
        let running = self.run.running.as_ref()?;
        Some(match &running.progress {
            Progress::Rip(p) => {
                let p = lock_status(&**p);
                OpticalBar {
                    operation: p.operation.clone(),
                    current_bytes: p.current_bytes,
                    total_bytes: p.total_bytes,
                }
            }
            Progress::Convert(p) => {
                let p = lock_status(&**p);
                OpticalBar {
                    operation: p.operation.clone(),
                    current_bytes: p.current_bytes,
                    total_bytes: p.total_bytes,
                }
            }
        })
    }

    /// How the last rip or conversion ended, until the next one starts.
    pub fn last_result(&self) -> Option<&Result<(), String>> {
        self.run.last.as_ref()
    }

    fn spawn_run(&mut self, label: &str, progress: Progress, body: impl FnOnce() + Send + 'static) {
        let job = Job::spawn(label.to_string(), Arc::clone(&self.notify), move |_| {
            body();
            Ok(())
        });
        self.run.last = None;
        self.run.running = Some(Running { job, progress });
    }

    pub(super) fn start(&mut self, chd: Option<ChdChoice>) {
        if let Some(why) = self.start_refusal() {
            if !self.is_running() {
                self.error(format!("Cannot start: {why}"));
            }
            return;
        }
        match self.task() {
            Task::Rip => self.start_rip(chd),
            Task::Convert => self.start_convert(chd),
        }
    }

    fn output_or_log(&mut self) -> Option<PathBuf> {
        let out = self.output.clone();
        if out.is_none() {
            self.error("No output path selected");
        }
        out
    }

    fn chd_options(&self, chd: Option<ChdChoice>) -> ChdOptions {
        match chd {
            Some(choice) => {
                if choice.custom {
                    persist_chd(&choice.options);
                }
                choice.options
            }
            None => ChdOptions::defaults_for(ChdProfile::Cd),
        }
    }

    fn start_rip(&mut self, chd: Option<ChdChoice>) {
        let Some(target) = self.selected_device().map(|d| d.to_target()) else {
            self.error("No drive selected");
            return;
        };
        let Some(output) = self.output_or_log() else {
            return;
        };
        let eject_after = self.eject_after;
        if self.format == OutputFormat::Chd {
            let options = self.chd_options(chd);
            self.info(format!(
                "Starting rip to CHD: {} -> {}",
                target.display(),
                output.display()
            ));
            let progress = Arc::new(Mutex::new(ConvertProgress::new()));
            let worker = Arc::clone(&progress);
            self.spawn_run("Ripping to CHD", Progress::Convert(progress), move || {
                let _wake = crate::os::wakelock::acquire("Rusty Backup: optical rip to CHD");
                let result = optical_ops::rip_to_chd(
                    target,
                    output.as_path(),
                    eject_after,
                    options,
                    &worker,
                );
                if let Err(e) = result {
                    fail_convert(&worker, e);
                }
            });
            return;
        }
        self.info(format!(
            "Starting rip: {} -> {}",
            target.display(),
            output.display()
        ));
        let format = self.format;
        let progress = Arc::new(Mutex::new(RipProgress::new()));
        let worker = Arc::clone(&progress);
        self.spawn_run("Ripping", Progress::Rip(progress), move || {
            let _wake = crate::os::wakelock::acquire("Rusty Backup: optical rip");
            let result = optical_ops::rip(target, output, format, eject_after, Arc::clone(&worker));
            if let Err(e) = result {
                fail_rip(&worker, e);
            }
        });
    }

    fn start_convert(&mut self, chd: Option<ChdChoice>) {
        let Some(input) = self.image.clone() else {
            self.error("No input file selected");
            return;
        };
        let Some(output) = self.output_or_log() else {
            return;
        };
        let source_format = self.disc.as_ref().map(|i| i.format);
        self.info(format!(
            "Starting conversion: {} -> {}",
            input.display(),
            output.display()
        ));
        let format = self.format;
        let options = (format == OutputFormat::Chd).then(|| self.chd_options(chd));
        let progress = Arc::new(Mutex::new(ConvertProgress::new()));
        let worker = Arc::clone(&progress);
        self.spawn_run("Converting", Progress::Convert(progress), move || {
            let _wake = crate::os::wakelock::acquire("Rusty Backup: optical convert");
            let result = optical_ops::run_conversion(
                input.as_path(),
                output.as_path(),
                source_format,
                format,
                options,
                Arc::clone(&worker),
            );
            if let Err(e) = result {
                fail_convert(&worker, e);
            }
        });
    }

    pub(super) fn cancel(&mut self) {
        let Some(running) = &self.run.running else {
            return;
        };
        match &running.progress {
            Progress::Rip(p) => lock_status(&**p).cancel_requested = true,
            Progress::Convert(p) => lock_status(&**p).cancel_requested = true,
        }
        running.job.cancel();
        self.warn("Cancellation requested...");
    }

    /// Drain the run's log lines; once its job ends, log and keep how it ended.
    pub(super) fn poll_run(&mut self) {
        let Some(running) = &self.run.running else {
            return;
        };
        // Read the end first, so the lines drained below include the run's last ones.
        let done = job_outcome(&running.job);
        let (lines, error) = match &running.progress {
            Progress::Rip(p) => {
                let mut p = lock_status(&**p);
                (
                    p.log_messages.drain(..).collect::<Vec<_>>(),
                    p.error.clone(),
                )
            }
            Progress::Convert(p) => {
                let mut p = lock_status(&**p);
                (
                    p.log_messages.drain(..).collect::<Vec<_>>(),
                    p.error.clone(),
                )
            }
        };
        for msg in lines {
            let level = match msg.level {
                crate::backup::LogLevel::Info => LogLevel::Info,
                crate::backup::LogLevel::Warning => LogLevel::Warning,
                crate::backup::LogLevel::Error => LogLevel::Error,
            };
            self.log.push((level, msg.message));
        }
        let Some(done) = done else {
            return;
        };
        let Some(running) = &self.run.running else {
            return;
        };
        let what = match running.progress {
            Progress::Rip(_) => "Rip",
            Progress::Convert(_) => "Conversion",
        };
        self.run.running = None;
        let outcome = match (done, error) {
            (Ok(()), None) => Ok(()),
            (Ok(()), Some(e)) | (Err(e), _) => Err(e),
        };
        match &outcome {
            Ok(()) if what == "Rip" => self.info("Rip completed successfully"),
            Ok(()) => self.info("Conversion completed successfully"),
            Err(e) => self.error(format!("{what} failed: {e}")),
        }
        self.run.last = Some(outcome);
    }
}

/// Remember a Custom CD codec / hunk choice in config.json for the next launch.
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

/// The saved choice, kept only when it is a CD choice: CD codecs and a hunk of whole 2448-byte frames.
#[cfg(all(
    feature = "chd",
    any(feature = "gui", feature = "tui", feature = "tui-update")
))]
pub(super) fn saved_chd_custom(mut base: ChdOptions) -> ChdOptions {
    use crate::rbformats::chd_options::{codec_label, parse_codec_string};
    let cfg = crate::update::UpdateConfig::load();
    if let Some(spec) = cfg.last_chd_codecs.as_deref() {
        if let Ok(codecs) = parse_codec_string(spec) {
            if codecs
                .iter()
                .all(|c| codec_label(*c).starts_with("cd") || *c == 0)
            {
                base.codecs = codecs;
            }
        }
    }
    if let Some(hs) = cfg.last_chd_hunk_size {
        if hs % 2448 == 0 {
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
