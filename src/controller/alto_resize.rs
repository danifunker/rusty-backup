//! Resize Alto Disk: re-lay an Alto BFS pack onto a Diablo 31 or 44 geometry and save it as a PDI.
//!
//! [`AltoResizeController`] reads the source's summary in a job when the Inspect tab opens it,
//! holds the chosen target, asks for the output file (`Effect::PickFile`), and writes it in a
//! job. The rules and the write are `model::alto_resize`. Shell: `src/gui/alto_resize_dialog.rs`.
//! See docs/phase2_commander_plan.md § "Slice 15".

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::controller::log::LogLevel;
use crate::controller::{job_outcome, Controller, Effect, PickSpec};
use crate::model::alto_resize::{self, AltoSummary};
use crate::model::job::{Job, Notify, Snapshot};

pub use crate::model::alto_resize::AltoTarget;

/// The `purpose` of each picker this controller asks for.
pub mod purpose {
    pub const OUTPUT: &str = "alto_resize.output";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const TARGET: &str = "alto_resize.target";
    pub const SAVE_AS: &str = "alto_resize.save_as";
    pub const CLOSE: &str = "alto_resize.close";
}

/// User intents.
pub enum AltoResizeAction {
    /// Open the dialog on an Alto pack (its summary is read in a job).
    Open(PathBuf),
    Close,
    SetTarget(AltoTarget),
    /// "Resize and Save As...": asks for the output file.
    SaveAs,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
}

pub struct AltoResizeController {
    notify: Arc<dyn Notify>,
    log: Vec<(LogLevel, String)>,
    source: Option<PathBuf>,
    opening: Option<Job<Result<AltoSummary, String>>>,
    summary: Option<AltoSummary>,
    target: AltoTarget,
    writing: Option<Job<Result<String, String>>>,
    result: Option<Result<String, String>>,
}

impl AltoResizeController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        AltoResizeController {
            notify,
            log: Vec::new(),
            source: None,
            opening: None,
            summary: None,
            target: AltoTarget::Diablo44,
            writing: None,
            result: None,
        }
    }

    /// Lines for the log panel since the last call.
    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        std::mem::take(&mut self.log)
    }

    /// The dialog is up: a source is opening or open.
    pub fn is_open(&self) -> bool {
        self.source.is_some()
    }

    pub fn source(&self) -> Option<&Path> {
        self.source.as_deref()
    }

    /// The summary is still being read.
    pub fn opening(&self) -> Option<Snapshot> {
        self.opening.as_ref().map(|j| j.snapshot())
    }

    /// "Current: Diablo N - ... (U used)", once read.
    pub fn summary_line(&self) -> Option<&str> {
        self.summary.as_ref().map(|s| s.line.as_str())
    }

    pub fn target(&self) -> AltoTarget {
        self.target
    }

    /// Why the target may be too small for the source's used pages.
    pub fn fit_warning(&self) -> Option<String> {
        let used = self.summary.as_ref()?.used_pages;
        (used > self.target.pages()).then(|| {
            format!(
                "Warning: {} used pages may not fit the target's {} pages.",
                used,
                self.target.pages()
            )
        })
    }

    /// The last write: the saved line, or why it failed.
    pub fn result(&self) -> Option<&Result<String, String>> {
        self.result.as_ref()
    }

    pub fn is_writing(&self) -> bool {
        self.writing.is_some()
    }

    pub fn can_save(&self) -> bool {
        self.summary.is_some() && self.writing.is_none()
    }

    fn open(&mut self, path: PathBuf) {
        if self.writing.is_some() {
            return;
        }
        *self = AltoResizeController::new(Arc::clone(&self.notify));
        let read = path.clone();
        self.source = Some(path);
        self.opening = Some(Job::spawn(
            "Reading the Alto pack...",
            Arc::clone(&self.notify),
            move |_| Ok(alto_resize::summarize(read.as_path())),
        ));
    }

    fn close(&mut self) {
        if self.writing.is_some() {
            return;
        }
        if let Some(job) = self.opening.take() {
            job.cancel();
        }
        self.source = None;
        self.summary = None;
        self.result = None;
    }

    fn save_as(&self) -> Vec<Effect> {
        if !self.can_save() {
            return Vec::new();
        }
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::OUTPUT,
            title: "Save Resized Alto Disk".to_string(),
            filters: vec![("PARC Disk Image".to_string(), vec!["pdi".to_string()])],
            save: true,
            file_name: "resized.pdi".to_string(),
        })]
    }

    fn start_write(&mut self, out: PathBuf) {
        let Some(source) = self.source.clone() else {
            return;
        };
        if !self.can_save() {
            return;
        }
        let target = self.target;
        self.writing = Some(Job::spawn(
            "Resizing the Alto disk",
            Arc::clone(&self.notify),
            move |_| {
                Ok(alto_resize::resize_to(
                    source.as_path(),
                    target,
                    out.as_path(),
                ))
            },
        ));
    }

    fn poll(&mut self) {
        if let Some(done) = self.opening.as_ref().and_then(job_outcome) {
            self.opening = None;
            match done.and_then(|r| r) {
                Ok(summary) => {
                    self.target = summary.suggested;
                    self.summary = Some(summary);
                }
                Err(e) => {
                    self.log
                        .push((LogLevel::Error, format!("Cannot open resize dialog: {e}")));
                    self.source = None;
                }
            }
        }
        if let Some(done) = self.writing.as_ref().and_then(job_outcome) {
            self.writing = None;
            let result = done.and_then(|r| r);
            match &result {
                Ok(msg) => self
                    .log
                    .push((LogLevel::Info, format!("Alto resize: {msg}"))),
                Err(e) => self
                    .log
                    .push((LogLevel::Error, format!("Alto resize failed: {e}"))),
            }
            self.result = Some(result);
        }
    }

    /// Block until the summary and any write end; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        loop {
            self.poll();
            if self.opening.is_none() && self.writing.is_none() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

impl Controller for AltoResizeController {
    type Action = AltoResizeAction;

    fn apply(&mut self, action: AltoResizeAction) -> Vec<Effect> {
        match action {
            AltoResizeAction::Open(path) => self.open(path),
            AltoResizeAction::Close => self.close(),
            AltoResizeAction::SetTarget(t) => {
                if self.writing.is_none() {
                    self.target = t;
                }
            }
            AltoResizeAction::SaveAs => return self.save_as(),
            AltoResizeAction::Picked { purpose, path } => {
                if let (purpose::OUTPUT, Some(path)) = (purpose, path) {
                    self.start_write(path);
                }
            }
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

    fn blank_pack(dir: &Path, target: AltoTarget) -> PathBuf {
        let disk = fs_xerox::alto::write::create_blank(target.geometry()).expect("blank pack");
        let path = dir.join("src.pdi");
        std::fs::write(&path, fs_xerox::alto::pdi::write(&disk)).expect("write pack");
        path
    }

    #[test]
    fn a_diablo31_pack_is_relaid_onto_a_diablo44() {
        let dir = tempfile::tempdir().unwrap();
        let src = blank_pack(dir.path(), AltoTarget::Diablo31);
        let mut ctl = AltoResizeController::new(Arc::new(NoNotify));
        ctl.apply(AltoResizeAction::Open(src));
        ctl.wait_idle();
        assert!(ctl
            .summary_line()
            .unwrap()
            .starts_with("Current: Diablo 31"));
        assert_eq!(
            ctl.target(),
            AltoTarget::Diablo44,
            "suggests the other geometry"
        );
        assert!(ctl.fit_warning().is_none());
        let fx = ctl.apply(AltoResizeAction::SaveAs);
        assert!(matches!(&fx[..], [Effect::PickFile(s)] if s.save && s.purpose == purpose::OUTPUT));
        let out = dir.path().join("out.pdi");
        ctl.apply(AltoResizeAction::Picked {
            purpose: purpose::OUTPUT,
            path: Some(out.clone()),
        });
        ctl.wait_idle();
        assert!(matches!(ctl.result(), Some(Ok(m)) if m.contains("9744 pages")));
        let bytes = std::fs::read(&out).unwrap();
        let disk = fs_xerox::alto::open_pack(&bytes).unwrap();
        assert_eq!(disk.geometry.disk_model, 44);
        assert!(ctl.take_log()[0].1.starts_with("Alto resize: Saved"));
    }

    #[test]
    fn a_file_that_is_not_an_alto_pack_closes_the_dialog_with_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("junk.pdi");
        std::fs::write(&src, b"not a pack").unwrap();
        let mut ctl = AltoResizeController::new(Arc::new(NoNotify));
        ctl.apply(AltoResizeAction::Open(src));
        ctl.wait_idle();
        assert!(!ctl.is_open());
        assert!(ctl.take_log()[0].1.starts_with("Cannot open resize dialog"));
    }
}
