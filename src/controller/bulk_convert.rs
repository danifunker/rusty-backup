//! Bulk Convert: every disk image in a folder to one output format, into an output folder.
//!
//! Setup picks the folders and the format; Scan lists the source folder in a job and moves to
//! Review, where files are ticked and the output extension can be changed. Start first looks for
//! outputs that already exist and, if any do, waits for Skip / Overwrite / Back. The conversion is
//! a job waiting on `model::bulk_convert_runner::start_bulk_convert` (unchanged, so it writes the
//! same files), mirroring its per-file progress and log lines.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::log::LogLevel;
use super::{job_outcome, Controller, Effect, PickSpec};
use crate::model::bulk_convert_runner::{scan_source_folder, start_bulk_convert};
use crate::model::job::{Job, Notify, Snapshot};
use crate::model::status::{BulkConvertLogLevel, BulkConvertStatus};

pub use crate::model::bulk_convert_runner::{fmt_bytes, ScannedFile};
pub use crate::model::job::NoNotify;
pub use crate::rbformats::chd_options::{ChdOptions, ChdProfile};
pub use crate::rbformats::export::ExportFormat;

/// The `purpose` of each picker this controller asks for.
pub mod purpose {
    pub const SOURCE: &str = "bulk_convert.source";
    pub const OUTPUT: &str = "bulk_convert.output";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const SOURCE: &str = "bulk_convert.source";
    pub const OUTPUT: &str = "bulk_convert.output";
    pub const FORMAT: &str = "bulk_convert.format";
    pub const MULTI_BIN: &str = "bulk_convert.multi_bin";
    pub const SCAN: &str = "bulk_convert.scan";
    pub const EXTENSION: &str = "bulk_convert.extension";
    pub const FILES: &str = "bulk_convert.files";
    pub const START: &str = "bulk_convert.start";
    pub const BACK: &str = "bulk_convert.back";
    pub const CANCEL: &str = "bulk_convert.cancel";
}

/// The formats the dialog offers, in its order.
pub const FORMATS: &[ExportFormat] = &[
    ExportFormat::Vhd,
    ExportFormat::VhdDynamic,
    ExportFormat::Qcow2,
    ExportFormat::VmdkFlat,
    ExportFormat::VmdkSparse,
    ExportFormat::Raw,
    ExportFormat::TwoMg,
    ExportFormat::Woz,
    ExportFormat::Moof,
    ExportFormat::Dc42,
    ExportFormat::Chd,
    ExportFormat::ChdDvd,
    ExportFormat::ChdCd,
    ExportFormat::BinCue,
    ExportFormat::Xdf,
    ExportFormat::Hdm,
    ExportFormat::Dim,
    ExportFormat::D88,
];

/// A format's name in the format list.
pub fn format_label(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Vhd => "VHD",
        ExportFormat::VhdDynamic => "VHD (Dynamic)",
        ExportFormat::Qcow2 => "QCOW2",
        ExportFormat::VmdkFlat => "VMDK (Flat)",
        ExportFormat::VmdkSparse => "VMDK (Sparse)",
        ExportFormat::Raw => "Raw (.img)",
        ExportFormat::TwoMg => "2MG (.2mg)",
        ExportFormat::Woz => "WOZ (.woz)",
        ExportFormat::Moof => "MOOF (.moof)",
        ExportFormat::Dc42 => "DiskCopy 4.2 (.dsk)",
        ExportFormat::Chd => "CHD (Hard Disk)",
        ExportFormat::ChdDvd => "DVD CHD",
        ExportFormat::ChdCd => "CD CHD",
        ExportFormat::BinCue => "BIN/CUE",
        ExportFormat::Xdf => "XDF (.xdf)",
        ExportFormat::Hdm => "HDM (.hdm)",
        ExportFormat::Dim => "DIM (.dim)",
        ExportFormat::D88 => "D88 (.d88)",
        other => other.description(),
    }
}

/// What a format needs from its inputs or how it writes, when there is more to say than its name.
pub fn format_note(format: ExportFormat) -> Option<&'static str> {
    Some(match format {
        ExportFormat::VhdDynamic => {
            "Sparse VHD - all-zero blocks are omitted. Same .vhd extension; \
             readable by Hyper-V, qemu-img, Disk Management."
        }
        ExportFormat::Qcow2 => {
            "QCOW2 v3 - sparse, uncompressed. The format UTM uses for \
             classic-Mac PPC guests; opens in QEMU, qemu-img, virt-manager."
        }
        ExportFormat::VmdkFlat => {
            "monolithicFlat VMDK - descriptor + <name>-flat.vmdk pair. \
             Opens in VMware, VirtualBox, qemu-img."
        }
        ExportFormat::VmdkSparse => {
            "monolithicSparse VMDK - single self-contained .vmdk; \
             zero grains omitted. Opens in VMware, VirtualBox, qemu-img."
        }
        ExportFormat::Woz => "Floppy only: 140K / 400K / 800K sources",
        ExportFormat::Moof => "Macintosh 3.5\" bitstream, floppy only: 400K / 800K (Snow / MAME)",
        ExportFormat::Dc42 => "Floppy only: 400K / 720K / 800K / 1440K sources",
        ExportFormat::Chd => "MAME hard-disk CHD (512-byte unit)",
        ExportFormat::ChdDvd => "MAME DVD CHD (2048-byte unit, MAME 0.287+)",
        ExportFormat::ChdCd => "MAME CD CHD - input must be .iso or .cue",
        ExportFormat::BinCue => "Extract a CD CHD into a BIN/CUE pair",
        ExportFormat::Xdf => {
            "X68000 raw headerless floppy dump. Floppy only (.xdf/.hdm/.dim/.d88 sources)."
        }
        ExportFormat::Hdm => "PC-98 / DiskExplorer raw floppy dump. Byte-identical layout to XDF.",
        ExportFormat::Dim => {
            "DiskExplorer DIFC DIM (256-byte header + payload). \
             Floppy only; 640 KB 2DD not representable here."
        }
        ExportFormat::D88 => {
            "Sharp D88 sparse track-table container (X68000 / PC-88 / PC-98 / FM-7)."
        }
        _ => return None,
    })
}

/// The multi-bin option's label and what it does.
pub const MULTI_BIN_LABEL: &str = "Multi-bin (one .bin per track)";
pub const MULTI_BIN_NOTE: &str = "Beyond chdman: writes <name> (Track NN).bin per track plus a \
     multi-FILE cue. Falls back to single-bin if track sizes don't match.";

/// The CHD profile a format writes with, if it is a CHD format.
pub fn chd_profile(format: ExportFormat) -> Option<ChdProfile> {
    match format {
        ExportFormat::Chd => Some(ChdProfile::Hd),
        ExportFormat::ChdDvd => Some(ChdProfile::Dvd),
        ExportFormat::ChdCd => Some(ChdProfile::Cd),
        _ => None,
    }
}

/// An output file that already exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub source: PathBuf,
    pub dest: PathBuf,
}

/// The answer to the "output files already exist" question.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictChoice {
    SkipConflicting,
    Overwrite,
    Back,
}

/// Which page of the dialog is up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BulkPhase {
    Setup,
    Review,
}

/// A running conversion's place, for a progress strip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkProgress {
    /// 1-based.
    pub index: usize,
    pub total_files: usize,
    pub file: String,
    pub bytes: u64,
    pub total_bytes: u64,
}

/// How a conversion ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BulkResult {
    pub succeeded: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BulkConvertAction {
    /// Show a fresh form.
    Open,
    Close,
    PickSource,
    PickOutput,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    /// In Review this reads the source folder again for the new format.
    SetFormat(ExportFormat),
    /// BIN/CUE only: one .bin per track.
    SetMultiBin(bool),
    Scan,
    /// Review back to Setup.
    Back,
    SetExtension(String),
    ResetExtension,
    SetSelected(usize, bool),
    SelectAll,
    SelectNone,
    /// The resolved CHD options for a CHD format (the shell's CHD control), else `None`.
    Start(Option<ChdOptions>),
    ResolveConflicts(ConflictChoice),
    /// Stop a running conversion after the current file.
    Cancel,
}

pub struct BulkConvertController {
    notify: Arc<dyn Notify>,
    open: bool,
    source: Option<PathBuf>,
    output: Option<PathBuf>,
    format: ExportFormat,
    multi_bin: bool,
    phase: BulkPhase,
    files: Vec<ScannedFile>,
    extension: String,
    conflicts: Option<Vec<Conflict>>,
    /// The CHD options a Start carried while its conflicts are asked about.
    pending_chd: Option<ChdOptions>,
    scan_error: Option<String>,
    scan: Option<Job<std::io::Result<Vec<ScannedFile>>>>,
    /// The format the running (or last) scan filtered for.
    scan_format: ExportFormat,
    run: Option<Job<BulkResult>>,
    status: Arc<Mutex<Option<Arc<Mutex<BulkConvertStatus>>>>>,
    lines: Arc<Mutex<Vec<(LogLevel, String)>>>,
    log: Vec<(LogLevel, String)>,
    result: Option<Result<BulkResult, String>>,
}

impl BulkConvertController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        BulkConvertController {
            notify,
            open: false,
            source: None,
            output: None,
            format: ExportFormat::Vhd,
            multi_bin: false,
            phase: BulkPhase::Setup,
            files: Vec::new(),
            extension: String::new(),
            conflicts: None,
            pending_chd: None,
            scan_error: None,
            scan: None,
            scan_format: ExportFormat::Vhd,
            run: None,
            status: Arc::new(Mutex::new(None)),
            lines: Arc::new(Mutex::new(Vec::new())),
            log: Vec::new(),
            result: None,
        }
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    pub fn phase(&self) -> BulkPhase {
        self.phase
    }

    pub fn source(&self) -> Option<&Path> {
        self.source.as_deref()
    }

    pub fn output(&self) -> Option<&Path> {
        self.output.as_deref()
    }

    pub fn format(&self) -> ExportFormat {
        self.format
    }

    pub fn multi_bin(&self) -> bool {
        self.multi_bin
    }

    /// Source and output are the same folder, so outputs may overwrite originals.
    pub fn same_folders(&self) -> bool {
        matches!((&self.source, &self.output), (Some(s), Some(o)) if s == o)
    }

    pub fn files(&self) -> &[ScannedFile] {
        self.files.as_slice()
    }

    pub fn selected_count(&self) -> usize {
        self.files.iter().filter(|f| f.selected).count()
    }

    pub fn extension(&self) -> &str {
        self.extension.as_str()
    }

    pub fn recommended_extension(&self) -> &'static str {
        self.format.extension()
    }

    pub fn scan_error(&self) -> Option<&str> {
        self.scan_error.as_deref()
    }

    pub fn scan_snapshot(&self) -> Option<Snapshot> {
        self.scan.as_ref().map(Job::snapshot)
    }

    pub fn is_scanning(&self) -> bool {
        self.scan.is_some()
    }

    /// Both folders are chosen and no scan is running.
    pub fn can_scan(&self) -> bool {
        self.source.is_some() && self.output.is_some() && self.scan.is_none()
    }

    /// Why Start would be refused, checked in this order; `None` when it can start.
    pub fn start_problem(&self) -> Option<&'static str> {
        if self.selected_count() == 0 {
            return Some("No files selected to convert.");
        }
        let no_output = match &self.output {
            Some(p) => p.as_os_str().is_empty(),
            None => true,
        };
        if no_output {
            return Some("Enter an output folder.");
        }
        let ext = self.extension.as_str();
        if ext.trim().is_empty() || ext.contains(['/', '\\', '.', ' ']) {
            return Some("The output extension may not be empty or hold a dot, slash or space.");
        }
        None
    }

    pub fn can_start(&self) -> bool {
        self.phase == BulkPhase::Review && !self.is_running() && self.start_problem().is_none()
    }

    /// Outputs that already exist, while the shell asks what to do about them.
    pub fn conflicts(&self) -> Option<&[Conflict]> {
        self.conflicts.as_deref()
    }

    pub fn is_running(&self) -> bool {
        self.run.is_some()
    }

    /// The running conversion's place; `None` before the first file and once it ends.
    pub fn progress(&self) -> Option<BulkProgress> {
        let slot = crate::model::worker::lock_status(&*self.status);
        let s = crate::model::worker::lock_status(&**slot.as_ref()?);
        (!s.finished).then(|| BulkProgress {
            index: s.current_index,
            total_files: s.total_files,
            file: s.current_file.clone(),
            bytes: s.current_bytes,
            total_bytes: s.current_total_bytes,
        })
    }

    /// The last conversion's counts, or why it could not run.
    pub fn last_result(&self) -> Option<&Result<BulkResult, String>> {
        self.result.as_ref()
    }

    /// Log lines since the last call, for the shell's log panel.
    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        std::mem::take(&mut self.log)
    }

    /// A fresh form; a conversion still running keeps going.
    fn reset_form(&mut self) {
        self.source = None;
        self.output = None;
        self.format = ExportFormat::Vhd;
        self.multi_bin = false;
        self.phase = BulkPhase::Setup;
        self.files.clear();
        self.extension.clear();
        self.conflicts = None;
        self.pending_chd = None;
        self.scan_error = None;
        self.scan = None;
    }

    fn start_scan(&mut self) {
        if self.scan.is_some() {
            return;
        }
        let Some(folder) = self.source.clone() else {
            return;
        };
        let format = self.format;
        self.scan_format = format;
        self.scan = Some(Job::spawn(
            "Scanning the source folder...",
            Arc::clone(&self.notify),
            move |job| {
                job.set_can_cancel(false);
                Ok(scan_source_folder(folder.as_path(), format))
            },
        ));
    }

    fn poll_scan(&mut self) {
        let Some(done) = self.scan.as_ref().and_then(job_outcome) else {
            return;
        };
        self.scan = None;
        let result = done.unwrap_or_else(|e| Err(crate::compat::io_other(e)));
        match result {
            Ok(files) if files.is_empty() && self.phase == BulkPhase::Setup => {
                self.scan_error = Some("No files found in source folder.".to_string());
            }
            Ok(files) => {
                self.scan_error = None;
                if self.phase == BulkPhase::Setup {
                    self.extension = self.format.extension().to_string();
                }
                self.files = files;
                self.phase = BulkPhase::Review;
                // The format changed while it ran: list the folder again for the new one.
                if self.scan_format != self.format {
                    self.extension = self.format.extension().to_string();
                    self.start_scan();
                }
            }
            Err(e) => self.scan_error = Some(format!("Scan failed: {e}")),
        }
    }

    fn chosen(&self, skip: &[Conflict]) -> Vec<PathBuf> {
        self.files
            .iter()
            .filter(|f| f.selected && !skip.iter().any(|c| c.source == f.path))
            .map(|f| f.path.clone())
            .collect()
    }

    fn start(&mut self, chd: Option<ChdOptions>) {
        if !self.can_start() {
            return;
        }
        let ext = self.extension.trim().to_string();
        let chosen = self.chosen(&[]);
        let conflicts = match &self.output {
            Some(out) => detect_conflicts(chosen.as_slice(), out.as_path(), ext.as_str()),
            None => Vec::new(),
        };
        if conflicts.is_empty() {
            self.launch(chosen, chd);
        } else {
            self.conflicts = Some(conflicts);
            self.pending_chd = chd;
        }
    }

    fn resolve(&mut self, choice: ConflictChoice) {
        let Some(conflicts) = self.conflicts.take() else {
            return;
        };
        let chd = self.pending_chd.take();
        match choice {
            ConflictChoice::SkipConflicting => {
                let chosen = self.chosen(conflicts.as_slice());
                if !chosen.is_empty() {
                    self.launch(chosen, chd);
                }
            }
            ConflictChoice::Overwrite => {
                let chosen = self.chosen(&[]);
                self.launch(chosen, chd);
            }
            ConflictChoice::Back => {}
        }
    }

    fn launch(&mut self, files: Vec<PathBuf>, chd: Option<ChdOptions>) {
        let Some(out) = self.output.clone() else {
            return;
        };
        let format = self.format;
        let extension = self.extension.trim().to_string();
        let chd = chd_profile(format).and(chd);
        let multi_bin = format == ExportFormat::BinCue && self.multi_bin;
        self.log.push((
            LogLevel::Info,
            format!(
                "Starting bulk convert: {} file(s) -> {} ({}, .{extension})",
                files.len(),
                out.display(),
                format.description(),
            ),
        ));
        self.open = false;
        self.result = None;
        let slot = Arc::clone(&self.status);
        let lines = Arc::clone(&self.lines);
        self.run = Some(Job::spawn(
            "Bulk convert",
            Arc::clone(&self.notify),
            move |ctx| {
                let status = start_bulk_convert(files, out, format, extension, chd, multi_bin);
                *crate::model::worker::lock_status(&*slot) = Some(Arc::clone(&status));
                crate::model::worker::wait_spawned(ctx, &status, |s, ctx| {
                    if ctx.is_cancelled() {
                        s.cancel_requested = true;
                    }
                    ctx.set_phase(format!(
                        "{}/{} {}",
                        s.current_index, s.total_files, s.current_file
                    ));
                    ctx.set_bytes(s.current_bytes, s.current_total_bytes);
                    let mut out = crate::model::worker::lock_status(&*lines);
                    for (level, msg) in s.log_messages.drain(..) {
                        out.push((level_of(level), msg));
                    }
                    s.finished
                });
                let s = crate::model::worker::lock_status(&*status);
                Ok(BulkResult {
                    succeeded: s.succeeded,
                    failed: s.failed,
                })
            },
        ));
    }

    fn poll_run(&mut self) {
        let fresh = std::mem::take(&mut *crate::model::worker::lock_status(&*self.lines));
        self.log.extend(fresh);
        if let Some(done) = self.run.as_ref().and_then(job_outcome) {
            self.run = None;
            *crate::model::worker::lock_status(&*self.status) = None;
            if let Err(e) = &done {
                if e != "cancelled" {
                    self.log
                        .push((LogLevel::Error, format!("Bulk convert failed: {e}")));
                }
            }
            self.result = Some(done);
        }
    }

    /// Block until the scan and any conversion end; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        while self.scan.is_some() || self.run.is_some() {
            self.poll_scan();
            self.poll_run();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

fn level_of(level: BulkConvertLogLevel) -> LogLevel {
    match level {
        BulkConvertLogLevel::Info => LogLevel::Info,
        BulkConvertLogLevel::Warn => LogLevel::Warning,
        BulkConvertLogLevel::Error => LogLevel::Error,
    }
}

/// The selected sources whose `<stem>.<extension>` already exists in `output`.
fn detect_conflicts(sources: &[PathBuf], output: &Path, extension: &str) -> Vec<Conflict> {
    let mut out = Vec::new();
    for src in sources {
        let stem = src.file_stem().and_then(|s| s.to_str()).unwrap_or("disk");
        let dest = output.join(format!("{stem}.{extension}"));
        if dest.exists() {
            out.push(Conflict {
                source: src.clone(),
                dest,
            });
        }
    }
    out
}

fn folder_picker(purpose: &'static str, title: &str) -> Vec<Effect> {
    vec![Effect::PickFolder(PickSpec {
        purpose,
        title: title.to_string(),
        filters: Vec::new(),
        save: false,
        file_name: String::new(),
    })]
}

impl Controller for BulkConvertController {
    type Action = BulkConvertAction;

    fn apply(&mut self, action: BulkConvertAction) -> Vec<Effect> {
        match action {
            BulkConvertAction::Open => {
                self.reset_form();
                self.open = true;
            }
            BulkConvertAction::Close => self.open = false,
            BulkConvertAction::PickSource => {
                return folder_picker(purpose::SOURCE, "Choose Source Folder")
            }
            BulkConvertAction::PickOutput => {
                return folder_picker(purpose::OUTPUT, "Choose Output Folder")
            }
            BulkConvertAction::Picked { purpose, path } => match (purpose, path) {
                (purpose::SOURCE, Some(p)) => self.source = Some(p),
                (purpose::OUTPUT, Some(p)) => self.output = Some(p),
                _ => {}
            },
            BulkConvertAction::SetFormat(format) => {
                if format != self.format {
                    self.format = format;
                    if self.phase == BulkPhase::Review {
                        self.extension = format.extension().to_string();
                        self.start_scan();
                    }
                }
            }
            BulkConvertAction::SetMultiBin(on) => self.multi_bin = on,
            BulkConvertAction::Scan => {
                if self.can_scan() {
                    self.start_scan();
                }
            }
            BulkConvertAction::Back => {
                self.phase = BulkPhase::Setup;
                self.conflicts = None;
            }
            BulkConvertAction::SetExtension(ext) => self.extension = ext,
            BulkConvertAction::ResetExtension => {
                self.extension = self.format.extension().to_string()
            }
            BulkConvertAction::SetSelected(i, on) => {
                if let Some(f) = self.files.get_mut(i) {
                    f.selected = on;
                }
            }
            BulkConvertAction::SelectAll => self.files.iter_mut().for_each(|f| f.selected = true),
            BulkConvertAction::SelectNone => self.files.iter_mut().for_each(|f| f.selected = false),
            BulkConvertAction::Start(chd) => self.start(chd),
            BulkConvertAction::ResolveConflicts(choice) => self.resolve(choice),
            BulkConvertAction::Cancel => {
                if let Some(job) = &self.run {
                    job.cancel();
                    self.log.push((
                        LogLevel::Warning,
                        "Bulk convert cancellation requested...".to_string(),
                    ));
                }
            }
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll_scan();
        self.poll_run();
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::job::NoNotify;

    fn folder_with(names: &[&str]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for n in names {
            std::fs::write(dir.path().join(n), vec![0u8; 4096]).unwrap();
        }
        dir
    }

    #[test]
    fn scan_review_and_conflicts() {
        let src = folder_with(&["a.img", "b.img"]);
        let out = folder_with(&["a.img"]);
        let mut c = BulkConvertController::new(Arc::new(NoNotify));
        c.apply(BulkConvertAction::Open);
        assert!(!c.can_scan());
        assert_eq!(
            c.apply(BulkConvertAction::PickSource).len(),
            1,
            "a folder picker"
        );
        for (purpose, dir) in [(purpose::SOURCE, &src), (purpose::OUTPUT, &out)] {
            c.apply(BulkConvertAction::Picked {
                purpose,
                path: Some(dir.path().to_path_buf()),
            });
        }
        c.apply(BulkConvertAction::SetFormat(ExportFormat::Raw));
        c.apply(BulkConvertAction::Scan);
        c.wait_idle();
        c.tick();
        assert_eq!(c.phase(), BulkPhase::Review);
        assert_eq!(c.files().len(), 2);
        assert_eq!(c.extension(), "img");
        c.apply(BulkConvertAction::SetExtension("r.w".into()));
        assert!(!c.can_start());
        c.apply(BulkConvertAction::ResetExtension);
        c.apply(BulkConvertAction::SelectNone);
        assert_eq!(c.start_problem(), Some("No files selected to convert."));
        c.apply(BulkConvertAction::SelectAll);
        c.apply(BulkConvertAction::Start(None));
        assert_eq!(c.conflicts().map(|c| c.len()), Some(1));
        c.apply(BulkConvertAction::ResolveConflicts(ConflictChoice::Back));
        assert!(c.conflicts().is_none() && !c.is_running());
        c.apply(BulkConvertAction::Start(None));
        c.apply(BulkConvertAction::ResolveConflicts(
            ConflictChoice::SkipConflicting,
        ));
        assert!(c.is_running() && !c.is_open());
        c.wait_idle();
        c.tick();
        let log: Vec<String> = c.take_log().into_iter().map(|(_, m)| m).collect();
        assert!(
            log[0].starts_with("Starting bulk convert: 1 file(s) -> "),
            "{log:?}"
        );
        assert!(out.path().join("b.img").exists());
        assert_eq!(
            std::fs::read(out.path().join("a.img")).unwrap().len(),
            4096,
            "the conflicting output was left alone"
        );
    }

    #[test]
    fn an_empty_folder_stays_on_setup() {
        let src = folder_with(&[]);
        let mut c = BulkConvertController::new(Arc::new(NoNotify));
        c.apply(BulkConvertAction::Open);
        for purpose in [purpose::SOURCE, purpose::OUTPUT] {
            c.apply(BulkConvertAction::Picked {
                purpose,
                path: Some(src.path().to_path_buf()),
            });
        }
        assert!(c.same_folders());
        c.apply(BulkConvertAction::Scan);
        c.wait_idle();
        c.tick();
        assert_eq!(c.phase(), BulkPhase::Setup);
        assert_eq!(c.scan_error(), Some("No files found in source folder."));
    }
}
