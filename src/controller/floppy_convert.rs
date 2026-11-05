//! Convert Floppy Container: one XDF / HDM / DIM / D88 image to another of the four.
//!
//! [`FloppyConvertController`] owns the source (handed over by the Inspect tab or picked in the
//! dialog, sniffed in a job), the target format, the output file and the conversion job. The
//! rules are `model::floppy_convert`; `rb-cli floppy convert` keeps its own verb over the same
//! engine call. Shell: `src/gui/floppy_convert_dialog.rs`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::controller::log::LogLevel;
use crate::controller::{job_outcome, Controller, Effect, PickSpec};
use crate::model::floppy_convert::{self, FloppyConverted, FloppySource};
use crate::model::job::{Job, Notify, Snapshot};

pub use crate::model::floppy_convert::FloppyTarget;

/// The `purpose` of each picker this controller asks for.
pub mod purpose {
    pub const SOURCE: &str = "floppy_convert.source";
    pub const OUTPUT: &str = "floppy_convert.output";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const CHOOSE_SOURCE: &str = "floppy_convert.choose_source";
    pub const TARGET: &str = "floppy_convert.target";
    pub const SAVE_AS: &str = "floppy_convert.save_as";
    pub const CONVERT: &str = "floppy_convert.convert";
    pub const CLOSE: &str = "floppy_convert.close";
}

/// User intents.
pub enum FloppyConvertAction {
    /// Open the dialog, with a source to sniff or empty.
    Open(Option<PathBuf>),
    Close,
    /// "Choose File...": asks for a source.
    PickSource,
    SetTarget(FloppyTarget),
    /// "Save As...": asks for the output file.
    PickOutput,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Start,
}

pub struct FloppyConvertController {
    notify: Arc<dyn Notify>,
    log: Vec<(LogLevel, String)>,
    open: bool,
    source: Option<(PathBuf, FloppySource)>,
    source_error: Option<String>,
    probing: Option<Job<(PathBuf, Result<FloppySource, String>)>>,
    target: FloppyTarget,
    output: Option<PathBuf>,
    job: Option<Job<Result<FloppyConverted, String>>>,
    outcome: Option<Result<FloppyConverted, String>>,
}

impl FloppyConvertController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        FloppyConvertController {
            notify,
            log: Vec::new(),
            open: false,
            source: None,
            source_error: None,
            probing: None,
            target: FloppyTarget::D88,
            output: None,
            job: None,
            outcome: None,
        }
    }

    /// Lines for the log panel since the last call.
    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        std::mem::take(&mut self.log)
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    pub fn source(&self) -> Option<&Path> {
        self.source.as_ref().map(|(p, _)| p.as_path())
    }

    /// "Detected: ..." for the accepted source.
    pub fn source_kind(&self) -> Option<&'static str> {
        self.source.as_ref().map(|(_, s)| s.kind_name)
    }

    /// Why the last source was refused.
    pub fn source_error(&self) -> Option<&str> {
        self.source_error.as_deref()
    }

    /// The source is being sniffed.
    pub fn probing(&self) -> bool {
        self.probing.is_some()
    }

    pub fn target(&self) -> FloppyTarget {
        self.target
    }

    /// Source and target are the same format: the output is a byte-identical copy.
    pub fn same_format(&self) -> bool {
        self.source
            .as_ref()
            .is_some_and(|(_, s)| s.format == self.target)
    }

    pub fn output(&self) -> Option<&Path> {
        self.output.as_deref()
    }

    pub fn is_running(&self) -> bool {
        self.job.is_some()
    }

    pub fn progress(&self) -> Option<Snapshot> {
        self.job.as_ref().map(|j| j.snapshot())
    }

    /// How the conversion ended, once it has.
    pub fn outcome(&self) -> Option<&Result<FloppyConverted, String>> {
        self.outcome.as_ref()
    }

    /// The form is editable: nothing has run on this source yet.
    pub fn editable(&self) -> bool {
        self.job.is_none() && self.outcome.is_none()
    }

    pub fn can_start(&self) -> bool {
        self.editable() && self.source.is_some() && self.output.is_some() && !self.probing()
    }

    fn open_with(&mut self, path: Option<PathBuf>) {
        if self.job.is_some() {
            return;
        }
        *self = FloppyConvertController::new(Arc::clone(&self.notify));
        self.open = true;
        if let Some(path) = path {
            self.probe(path);
        }
    }

    fn probe(&mut self, path: PathBuf) {
        if let Some(job) = self.probing.take() {
            job.cancel();
        }
        self.probing = Some(Job::spawn(
            "Reading the source...",
            Arc::clone(&self.notify),
            move |_| {
                let found = floppy_convert::probe_source(path.as_path());
                Ok((path, found))
            },
        ));
    }

    fn poll(&mut self) {
        if let Some(done) = self.probing.as_ref().and_then(job_outcome) {
            self.probing = None;
            match done {
                Ok((path, Ok(found))) => {
                    self.target = found.suggested;
                    self.source = Some((path, found));
                    self.source_error = None;
                    self.output = None;
                    self.outcome = None;
                }
                Ok((_, Err(e))) | Err(e) => self.source_error = Some(e),
            }
        }
        if let Some(done) = self.job.as_ref().and_then(job_outcome) {
            self.job = None;
            self.outcome = Some(done.and_then(|r| r));
        }
    }

    fn pick_source(&self) -> Vec<Effect> {
        if !self.editable_or_done() {
            return Vec::new();
        }
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::SOURCE,
            title: "Choose Floppy Container".to_string(),
            filters: vec![
                (
                    "Floppy Containers".to_string(),
                    ["xdf", "hdm", "dim", "d88"]
                        .iter()
                        .map(|e| e.to_string())
                        .collect(),
                ),
                ("All Files".to_string(), vec!["*".to_string()]),
            ],
            save: false,
            file_name: String::new(),
        })]
    }

    /// A new source may be chosen after a run, which resets the form.
    fn editable_or_done(&self) -> bool {
        self.job.is_none()
    }

    fn pick_output(&self) -> Vec<Effect> {
        if !self.editable() {
            return Vec::new();
        }
        let stem = self
            .source()
            .and_then(|p| p.file_stem())
            .and_then(|s| s.to_str())
            .unwrap_or("converted")
            .to_string();
        let ext = self.target.extension();
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::OUTPUT,
            title: "Save Converted Floppy".to_string(),
            filters: vec![(self.target.label().to_string(), vec![ext.to_string()])],
            save: true,
            file_name: format!("{stem}.{ext}"),
        })]
    }

    fn start(&mut self) {
        if !self.can_start() {
            return;
        }
        let (Some(source), Some(output)) =
            (self.source().map(Path::to_path_buf), self.output.clone())
        else {
            return;
        };
        let target = self.target;
        self.log.push((
            LogLevel::Info,
            format!(
                "Converting floppy container: {} -> {} ({})",
                source.display(),
                output.display(),
                target.label(),
            ),
        ));
        self.job = Some(Job::spawn(
            "Converting the floppy",
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_can_cancel(false);
                ctx.set_phase("Decoding source...");
                Ok(floppy_convert::convert(
                    source.as_path(),
                    output.as_path(),
                    target,
                ))
            },
        ));
    }

    /// Block until the sniff and any conversion end; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        loop {
            self.poll();
            if self.probing.is_none() && self.job.is_none() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

impl Controller for FloppyConvertController {
    type Action = FloppyConvertAction;

    fn apply(&mut self, action: FloppyConvertAction) -> Vec<Effect> {
        match action {
            FloppyConvertAction::Open(path) => self.open_with(path),
            FloppyConvertAction::Close => {
                if self.job.is_none() {
                    if let Some(job) = self.probing.take() {
                        job.cancel();
                    }
                    self.open = false;
                }
            }
            FloppyConvertAction::PickSource => return self.pick_source(),
            FloppyConvertAction::SetTarget(t) => {
                if self.editable() {
                    self.target = t;
                }
            }
            FloppyConvertAction::PickOutput => return self.pick_output(),
            FloppyConvertAction::Picked { purpose, path } => match (purpose, path) {
                (purpose::SOURCE, Some(path)) if self.editable_or_done() => self.probe(path),
                (purpose::OUTPUT, Some(path)) if self.editable() => self.output = Some(path),
                _ => {}
            },
            FloppyConvertAction::Start => self.start(),
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll();
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::job::NoNotify;

    #[test]
    fn a_non_floppy_source_is_refused_and_keeps_the_form_empty() {
        let dir = tempfile::tempdir().unwrap();
        let junk = dir.path().join("junk.bin");
        std::fs::write(&junk, vec![0xE5u8; 4096]).unwrap();
        let mut ctl = FloppyConvertController::new(Arc::new(NoNotify));
        ctl.apply(FloppyConvertAction::Open(Some(junk)));
        ctl.wait_idle();
        assert!(ctl.is_open() && ctl.source().is_none());
        assert!(ctl
            .source_error()
            .is_some_and(|e| e.contains("not a recognised floppy container")));
        assert!(!ctl.can_start());
        let fx = ctl.apply(FloppyConvertAction::PickSource);
        assert!(
            matches!(&fx[..], [Effect::PickFile(s)] if s.purpose == purpose::SOURCE && !s.save)
        );
    }

    #[test]
    fn an_xdf_converts_to_d88_and_the_form_locks() {
        let dir = tempfile::tempdir().unwrap();
        let xdf = dir.path().join("disk.xdf");
        std::fs::write(&xdf, vec![0xE5u8; 1_261_568]).unwrap();
        let mut ctl = FloppyConvertController::new(Arc::new(NoNotify));
        ctl.apply(FloppyConvertAction::Open(Some(xdf)));
        ctl.wait_idle();
        assert_eq!(ctl.target(), FloppyTarget::D88, "{:?}", ctl.source_error());
        assert!(ctl.source_kind().is_some() && !ctl.same_format());
        let fx = ctl.apply(FloppyConvertAction::PickOutput);
        assert!(matches!(&fx[..], [Effect::PickFile(s)] if s.file_name == "disk.d88" && s.save));
        let out = dir.path().join("disk.d88");
        ctl.apply(FloppyConvertAction::Picked {
            purpose: purpose::OUTPUT,
            path: Some(out.clone()),
        });
        ctl.apply(FloppyConvertAction::Start);
        ctl.wait_idle();
        let done = ctl.outcome().unwrap().as_ref().unwrap();
        assert!(
            done.summary.starts_with("Converted to Sharp D88 (.d88)"),
            "{}",
            done.summary
        );
        assert!(std::fs::metadata(&out).unwrap().len() > 1_261_568);
        assert!(!ctl.can_start());
        ctl.apply(FloppyConvertAction::SetTarget(FloppyTarget::Hdm));
        assert_eq!(ctl.target(), FloppyTarget::D88, "locked after the run");
    }
}
