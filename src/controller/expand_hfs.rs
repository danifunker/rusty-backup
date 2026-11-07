//! Expand HFS Volume: clone a classic-HFS volume into a new APM disk or flat HFV of another size.
//!
//! [`ExpandHfsController`] takes the source summary the Inspect controller measured
//! (`HostRequest::ExpandHfs`), owns the output format, block size, target size (kept inside what
//! the source's used space and the block size allow) and output file (`Effect::PickFile`), and
//! runs `model::hfs_expand_runner` in a job, keeping the worker's lines for the dialog. `rb-cli
//! expand` keeps its own flags over the same runner. Shell: `src/gui/expand_hfs_dialog.rs`.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::controller::log::LogLevel;
use crate::controller::{job_outcome, Controller, Effect, PickSpec};
use crate::model::hfs_expand_runner::{
    max_volume_for_block_size, start_hfs_expand, suggest_block_size, ExpandOutput,
    BLOCK_SIZE_CHOICES,
};
use crate::model::job::{Job, Notify, Snapshot};

pub use crate::model::hfs_expand_runner::ExpandSource;

const MIB: u64 = 1024 * 1024;
/// HFV's 4063 MB ceiling in whole MiB.
const HFV_MAX_MIB: u32 = (fs_mac::hfv::HFV_MAX_BYTES / MIB) as u32;

/// The `purpose` of each picker this controller asks for.
pub mod purpose {
    pub const OUTPUT: &str = "expand_hfs.output";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const OUTPUT_FORMAT: &str = "expand_hfs.output_format";
    pub const BLOCK_SIZE: &str = "expand_hfs.block_size";
    pub const TARGET_SIZE: &str = "expand_hfs.target_size";
    pub const SAVE_AS: &str = "expand_hfs.save_as";
    pub const EXPAND: &str = "expand_hfs.expand";
    pub const CLOSE: &str = "expand_hfs.close";
}

/// User intents.
pub enum ExpandHfsAction {
    Open(ExpandSource),
    Close,
    /// Write a bare HFS volume (`.hfv` / `.img`) instead of an APM disk.
    SetFlatHfv(bool),
    SetBlockSize(u32),
    SetSizeMib(u32),
    /// "Save As...": asks for the output file.
    PickOutput,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Start,
}

/// An allocation block size the dialog offers, with its label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockChoice {
    pub bytes: u32,
    /// "4 KiB (max 255 MiB)".
    pub label: String,
}

fn block_label(bs: u32) -> String {
    format!(
        "{} KiB (max {} MiB)",
        bs / 1024,
        max_volume_for_block_size(bs) / MIB
    )
}

pub struct ExpandHfsController {
    notify: Arc<dyn Notify>,
    log: Vec<(LogLevel, String)>,
    source: Option<ExpandSource>,
    size_mib: u32,
    block_size: u32,
    flat_hfv: bool,
    output: Option<PathBuf>,
    job: Option<Job<Result<(), String>>>,
    /// The worker's lines so far, shared with the running job.
    lines: Arc<Mutex<Vec<String>>>,
    /// How the run ended; Expand stays off afterwards.
    outcome: Option<Result<(), String>>,
}

impl ExpandHfsController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        ExpandHfsController {
            notify,
            log: Vec::new(),
            source: None,
            size_mib: 1,
            block_size: BLOCK_SIZE_CHOICES[0],
            flat_hfv: false,
            output: None,
            job: None,
            lines: Arc::new(Mutex::new(Vec::new())),
            outcome: None,
        }
    }

    /// Lines for the log panel since the last call.
    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        std::mem::take(&mut self.log)
    }

    pub fn is_open(&self) -> bool {
        self.source.is_some()
    }

    pub fn source(&self) -> Option<&ExpandSource> {
        self.source.as_ref()
    }

    pub fn flat_hfv(&self) -> bool {
        self.flat_hfv
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub fn block_size_label(&self) -> String {
        block_label(self.block_size)
    }

    pub fn block_choices(&self) -> Vec<BlockChoice> {
        BLOCK_SIZE_CHOICES
            .iter()
            .map(|&bytes| BlockChoice {
                bytes,
                label: block_label(bytes),
            })
            .collect()
    }

    pub fn size_mib(&self) -> u32 {
        self.size_mib
    }

    /// The target size's slider range in MiB: the source's used space up to the block size's
    /// ceiling (and the HFV cap for a flat HFV).
    pub fn size_bounds(&self) -> (u32, u32) {
        let mut max = (max_volume_for_block_size(self.block_size) / MIB) as u32;
        if self.flat_hfv {
            max = max.min(HFV_MAX_MIB);
        }
        let used = self.source.as_ref().map_or(0, |s| s.used_bytes);
        (used.div_ceil(MIB).max(1) as u32, max)
    }

    pub fn output(&self) -> Option<&std::path::Path> {
        self.output.as_deref()
    }

    /// The expand is running.
    pub fn is_running(&self) -> bool {
        self.job.is_some()
    }

    /// The running expand's step.
    pub fn progress(&self) -> Option<Snapshot> {
        self.job.as_ref().map(|j| j.snapshot())
    }

    /// The worker's lines so far.
    pub fn lines(&self) -> Vec<String> {
        crate::model::worker::lock_status(&*self.lines).clone()
    }

    /// How the run ended, once it has.
    pub fn outcome(&self) -> Option<&Result<(), String>> {
        self.outcome.as_ref()
    }

    /// The form is editable: nothing has run yet.
    pub fn editable(&self) -> bool {
        self.job.is_none() && self.outcome.is_none()
    }

    pub fn can_start(&self) -> bool {
        self.editable() && self.output.is_some() && self.source.is_some()
    }

    fn clamp_size(&mut self) {
        let (min, max) = self.size_bounds();
        if self.size_mib > max {
            self.size_mib = max;
        }
        if self.size_mib < min {
            self.size_mib = min;
        }
    }

    fn open(&mut self, source: ExpandSource) {
        if self.job.is_some() {
            return;
        }
        // Default to the current size, never below the used space, rounded up to a MiB.
        let bytes = source.partition_size.max(source.used_bytes);
        let mut fresh = ExpandHfsController::new(Arc::clone(&self.notify));
        fresh.size_mib = bytes.div_ceil(MIB).clamp(1, u32::MAX as u64) as u32;
        fresh.block_size = suggest_block_size(bytes);
        // A source at offset 0 (an .hfv or a bare partition image) has no APM to keep.
        fresh.flat_hfv = source.partition_offset == 0;
        fresh.source = Some(source);
        fresh.clamp_size();
        *self = fresh;
    }

    fn pick_output(&self) -> Vec<Effect> {
        let Some(src) = &self.source else {
            return Vec::new();
        };
        if !self.editable() {
            return Vec::new();
        }
        let stem = sanitize_filename(src.volume_name.as_str());
        let (file_name, filter) = if self.flat_hfv {
            // An .hfv source keeps .hfv; anything else (a raw partition-0.img) stays .img.
            let hfv = src
                .source_path
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e.eq_ignore_ascii_case("hfv"));
            let ext = if hfv { "hfv" } else { "img" };
            (
                format!("{stem}-expanded.{ext}"),
                ("Bare HFS image", ["hfv", "img", "dsk"]),
            )
        } else {
            (
                format!("{stem}-expanded.hda"),
                ("Disk image", ["hda", "img", "dsk"]),
            )
        };
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::OUTPUT,
            title: "Save Expanded Volume".to_string(),
            filters: vec![(
                filter.0.to_string(),
                filter.1.iter().map(|e| e.to_string()).collect(),
            )],
            save: true,
            file_name,
        })]
    }

    fn start(&mut self) {
        if !self.can_start() {
            return;
        }
        let (Some(source), Some(output)) = (self.source.clone(), self.output.clone()) else {
            return;
        };
        let target_size = self.size_mib as u64 * MIB;
        let block_size = self.block_size;
        let mode = if self.flat_hfv {
            ExpandOutput::FlatHfv
        } else {
            ExpandOutput::ApmDisk
        };
        self.log.push((
            LogLevel::Info,
            format!(
                "Expanding HFS volume '{}' to {} MiB at {} KiB blocks -> {} ({})",
                source.volume_name,
                self.size_mib,
                block_size / 1024,
                output.display(),
                if self.flat_hfv {
                    "bare HFS image"
                } else {
                    "APM disk"
                }
            ),
        ));
        let lines = Arc::clone(&self.lines);
        self.job = Some(Job::spawn(
            "Expanding the HFS volume",
            Arc::clone(&self.notify),
            move |ctx| {
                ctx.set_can_cancel(false);
                let status = start_hfs_expand(source, target_size, block_size, output, mode);
                let mut seen = 0;
                crate::model::worker::wait_spawned(ctx, &status, |s, ctx| {
                    ctx.set_phase(s.current_step.clone());
                    let fresh = s.log_messages.get(seen..).unwrap_or(&[]).to_vec();
                    seen = s.log_messages.len();
                    crate::model::worker::lock_status(&*lines).extend(fresh);
                    s.finished
                });
                let error = crate::model::worker::lock_status(&*status).error.clone();
                Ok(match error {
                    Some(e) => Err(e),
                    None => Ok(()),
                })
            },
        ));
    }

    fn poll(&mut self) {
        if let Some(done) = self.job.as_ref().and_then(job_outcome) {
            self.job = None;
            self.outcome = Some(done.and_then(|r| r));
        }
    }

    /// Block until the expand ends; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        while self.job.is_some() {
            self.poll();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

impl Controller for ExpandHfsController {
    type Action = ExpandHfsAction;

    fn apply(&mut self, action: ExpandHfsAction) -> Vec<Effect> {
        let edits = !matches!(
            action,
            ExpandHfsAction::Open(_) | ExpandHfsAction::Close | ExpandHfsAction::PickOutput
        );
        if edits && !self.editable() {
            return Vec::new();
        }
        match action {
            ExpandHfsAction::Open(source) => self.open(source),
            ExpandHfsAction::Close => {
                if self.job.is_none() {
                    self.source = None;
                }
            }
            ExpandHfsAction::SetFlatHfv(on) => {
                self.flat_hfv = on;
                self.clamp_size();
            }
            ExpandHfsAction::SetBlockSize(bs) => {
                if BLOCK_SIZE_CHOICES.contains(&bs) {
                    self.block_size = bs;
                    self.clamp_size();
                }
            }
            ExpandHfsAction::SetSizeMib(n) => {
                self.size_mib = n;
                self.clamp_size();
            }
            ExpandHfsAction::PickOutput => return self.pick_output(),
            ExpandHfsAction::Picked { purpose, path } => {
                if let (purpose::OUTPUT, Some(path)) = (purpose, path) {
                    self.output = Some(path);
                }
            }
            ExpandHfsAction::Start => self.start(),
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll();
        Vec::new()
    }
}

/// A volume name made safe for a default output file name.
fn sanitize_filename(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ' ') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim().to_string();
    if trimmed.is_empty() {
        "Expanded".to_string()
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::job::NoNotify;

    fn hfs_image(dir: &std::path::Path, mib: u64) -> PathBuf {
        let bytes = fs_mac::hfs::create_blank_hfs(mib * MIB, 4096, "Work/Disk").unwrap();
        let path = dir.join("vol.hfv");
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn a_flat_hfv_grows_into_a_bigger_flat_hfv() {
        let dir = tempfile::tempdir().unwrap();
        let src = hfs_image(dir.path(), 4);
        let source = crate::model::hfs_expand_runner::summarize_source(&src, 0, 4 * MIB).unwrap();
        let mut ctl = ExpandHfsController::new(Arc::new(NoNotify));
        ctl.apply(ExpandHfsAction::Open(source));
        assert!(ctl.flat_hfv(), "an offset-0 source defaults to a flat HFV");
        assert_eq!(ctl.size_mib(), 4);
        ctl.apply(ExpandHfsAction::SetSizeMib(u32::MAX));
        assert_eq!(
            ctl.size_mib(),
            ctl.size_bounds().1,
            "clamped to the ceiling"
        );
        ctl.apply(ExpandHfsAction::SetSizeMib(8));
        let fx = ctl.apply(ExpandHfsAction::PickOutput);
        let Some(Effect::PickFile(spec)) = fx.first() else {
            panic!("{fx:?}");
        };
        assert_eq!(spec.file_name, "Work_Disk-expanded.hfv");
        assert!(!ctl.can_start(), "no output yet");
        let out = dir.path().join("big.hfv");
        ctl.apply(ExpandHfsAction::Picked {
            purpose: purpose::OUTPUT,
            path: Some(out.clone()),
        });
        ctl.apply(ExpandHfsAction::Start);
        assert!(ctl.is_running());
        ctl.wait_idle();
        assert_eq!(ctl.outcome(), Some(&Ok(())), "{:?}", ctl.lines());
        let len = std::fs::metadata(&out).unwrap().len();
        assert!(len > 7 * MIB && len <= 8 * MIB, "{len}");
        assert!(!ctl.editable() && !ctl.can_start());
        assert!(ctl.take_log()[0]
            .1
            .starts_with("Expanding HFS volume 'Work/Disk' to 8 MiB"));
    }
}
