//! The Convert tab: one image, or every image in a folder, to another format (docs/convert_tab.md).
//!
//! Picking a source starts a scan job that lists and classifies the files; the list shows each
//! file's kind, what it becomes and why it would be skipped. Each kind (hard disk, floppy, disc)
//! has its own format and extension, hard disks a size choice and floppies a refit size. Start
//! asks Skip / Overwrite / Back when outputs exist, then converts in a job, one file after
//! another, through `model::convert::run::convert_one`. Inspect and Optical hand their image
//! over with [`ConvertAction::SetSource`].

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::log::LogLevel;
use super::{job_outcome, Choice, ChoiceSpec, Controller, Effect, PickSpec};
use crate::model::convert::run::{convert_one, Hooks, Level};
use crate::model::convert::{self as model, classify, extension_problem, output_path, Classified};
use crate::model::job::{Job, Notify, Snapshot};

pub use crate::model::convert::classify::size_text;
pub use crate::model::convert::refit::{RefitSize, ONLY_FILES, SIZES as FLOPPY_SIZES};
pub use crate::model::convert::ChdChoices;
pub use crate::model::convert::{
    formats_for, Extra, FloppySize, Format, ImageKind, Sizing, DISC_FORMATS, FLOPPY_FORMATS,
    HARD_DISK_FORMATS,
};
pub use crate::rbformats::chd_options::{ChdOptions, ChdProfile};

/// The `purpose` of each picker and question this controller asks.
pub mod purpose {
    pub const SOURCE_FILE: &str = "convert.source_file";
    pub const SOURCE_FOLDER: &str = "convert.source_folder";
    pub const OUTPUT_FILE: &str = "convert.output_file";
    pub const OUTPUT_FOLDER: &str = "convert.output_folder";
    pub const CONFLICTS: &str = "convert.conflicts";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const SOURCE_FILE: &str = "convert.source_file";
    pub const SOURCE_FOLDER: &str = "convert.source_folder";
    pub const RECURSIVE: &str = "convert.recursive";
    pub const OUTPUT: &str = "convert.output";
    pub const HARD_DISK_FORMAT: &str = "convert.hard_disk.format";
    pub const HARD_DISK_EXTENSION: &str = "convert.hard_disk.extension";
    pub const FLOPPY_FORMAT: &str = "convert.floppy.format";
    pub const FLOPPY_EXTENSION: &str = "convert.floppy.extension";
    pub const DISC_FORMAT: &str = "convert.disc.format";
    pub const DISC_EXTENSION: &str = "convert.disc.extension";
    pub const SIZING: &str = "convert.sizing";
    pub const EXTRA_AMOUNT: &str = "convert.extra_amount";
    pub const EXTRA_UNIT: &str = "convert.extra_unit";
    pub const MEASURE_SLOW: &str = "convert.measure_slow";
    pub const FLOPPY_SIZE: &str = "convert.floppy_size";
    pub const VERIFY: &str = "convert.verify";
    pub const MULTI_BIN: &str = "convert.multi_bin";
    pub const FILES: &str = "convert.files";
    pub const SELECT_ALL: &str = "convert.select_all";
    pub const SELECT_NONE: &str = "convert.select_none";
    pub const START: &str = "convert.start";
    pub const CANCEL: &str = "convert.cancel";
    pub const CLEAR: &str = "convert.clear";
    pub const CHD_MODE: &str = "convert.chd_mode";
    pub const CHD_HUNK: &str = "convert.chd_hunk";
    pub const CHD_CODEC: &str = "convert.chd_codec";
}

/// The widget ids of a kind's format and extension rows.
pub fn kind_ids(kind: ImageKind) -> (&'static str, &'static str) {
    match kind {
        ImageKind::HardDisk => (ids::HARD_DISK_FORMAT, ids::HARD_DISK_EXTENSION),
        ImageKind::Floppy => (ids::FLOPPY_FORMAT, ids::FLOPPY_EXTENSION),
        ImageKind::Disc => (ids::DISC_FORMAT, ids::DISC_EXTENSION),
    }
}

/// Plain-language help for each choice.
pub mod hints {
    pub const INTRO: &str = "Change the format of one disk image, or of every image in a \
        folder. Hard disks, floppies and discs each get their own format.";
    pub const RECURSIVE: &str = "Also convert the images in every folder inside this one. The \
        output folder gets the same folders.";
    pub const SIZING: &str = "Keep size makes an exact copy in the new format. Minimum shrinks \
        each partition to the space its files use, and the disk with it. Minimum plus extra \
        leaves room to add files later.";
    pub const EXTRA: &str = "Added to every partition: a fixed amount, or a share of what the \
        partition's files use. A partition never ends up larger than it was.";
    pub const VERIFY: &str = "After each file, read the new image back and compare it with the \
        original. Takes about as long again.";
    pub const FLOPPY_SIZE: &str = "Same size changes only the format. A new size formats a \
        blank disk of that size and copies every file onto it; a disk whose files don't fit is \
        skipped.";
    pub const MULTI_BIN: &str = "Write one .bin per track, as most CD rippers do, instead of \
        one .bin for the whole disc.";
    pub const OUTPUT_FILE: &str = "Where the converted image is saved.";
    pub const OUTPUT_FOLDER: &str = "Where the converted images go. Each keeps its name with \
        the new extension.";
    pub const HANDOFF: &str = "Change this image's format, shrink it, or refit a floppy in the \
        Convert tab.";
    pub const EXTENSION: &str = "The converted files' extension. Some emulators want a \
        particular one, such as .hdf or .hda.";

    /// "Measure slow filesystems too": the slow ones come from the engine's own list.
    pub fn measure_slow() -> String {
        format!(
            "Finding the smallest size of some filesystems means reading the whole partition, \
             which can take minutes each: {}. When this is off, those partitions keep their size.",
            crate::fs::expensive_minimum_names().join(", ")
        )
    }
}

/// The text a format shows when this build cannot write it, or `None`.
pub fn format_unavailable(format: Format) -> Option<&'static str> {
    format.unavailable()
}

/// The size choice's three modes; the extra amount and unit are separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeChoice {
    Keep,
    Minimum,
    Extra,
}

impl SizeChoice {
    pub const ALL: [SizeChoice; 3] = [SizeChoice::Keep, SizeChoice::Minimum, SizeChoice::Extra];

    pub const fn label(self) -> &'static str {
        match self {
            SizeChoice::Keep => "Keep size",
            SizeChoice::Minimum => "Minimum",
            SizeChoice::Extra => "Minimum plus extra",
        }
    }
}

/// The extra amount's unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtraUnit {
    Mb,
    Percent,
}

impl ExtraUnit {
    pub const ALL: [ExtraUnit; 2] = [ExtraUnit::Mb, ExtraUnit::Percent];

    pub const fn label(self) -> &'static str {
        match self {
            ExtraUnit::Mb => "MB",
            ExtraUnit::Percent => "%",
        }
    }
}

/// The CHD profile a format writes with, if it is a CHD format.
pub fn chd_profile(format: Format) -> Option<ChdProfile> {
    match format {
        Format::Chd => Some(ChdProfile::Hd),
        Format::ChdCd => Some(ChdProfile::Cd),
        Format::ChdDvd => Some(ChdProfile::Dvd),
        _ => None,
    }
}

/// The floppy size choices in list order: Same, then each refit size.
pub fn floppy_size_choices() -> Vec<FloppySize> {
    std::iter::once(FloppySize::Same)
        .chain(FLOPPY_SIZES.iter().map(|s| FloppySize::Refit(s.bytes)))
        .collect()
}

/// One file the scan found.
#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub path: PathBuf,
    /// The path below the source folder, or the file's name.
    pub name: String,
    /// `Err` when the file could not be read.
    pub found: Result<Classified, String>,
    pub selected: bool,
}

/// A file list row's text, for every shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub name: String,
    pub kind: String,
    pub detail: String,
    /// What it becomes ("game.chd") or why it is skipped.
    pub outcome: String,
    pub skipped: bool,
    pub selected: bool,
    /// The contents and the extension disagree.
    pub note: Option<String>,
}

/// A running conversion's place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    /// 1-based.
    pub index: usize,
    pub total: usize,
    pub file: String,
}

/// How a conversion ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunResult {
    pub succeeded: usize,
    pub failed: usize,
    pub skipped: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConvertAction {
    PickSourceFile,
    PickSourceFolder,
    PickOutput,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    /// Load a file or folder handed over by another tab or a drop.
    SetSource(PathBuf),
    SetRecursive(bool),
    SetFormat(ImageKind, Format),
    SetExtension(ImageKind, String),
    ResetExtension(ImageKind),
    SetSizing(SizeChoice),
    SetExtraAmount(u32),
    SetExtraUnit(ExtraUnit),
    SetMeasureSlow(bool),
    SetFloppySize(FloppySize),
    SetVerify(bool),
    SetMultiBin(bool),
    SetSelected(usize, bool),
    SelectAll,
    SelectNone,
    /// The CHD options from the shell's CHD controls, used where a CHD format is chosen.
    Start(ChdChoices),
    /// The answer to a question this controller asked.
    Chose {
        purpose: &'static str,
        index: usize,
    },
    /// Stop after the current file.
    Cancel,
    /// Forget the source and the list.
    Clear,
}

type Lines = Arc<Mutex<Vec<(LogLevel, String)>>>;

pub struct ConvertController {
    notify: Arc<dyn Notify>,
    source: Option<PathBuf>,
    source_is_folder: bool,
    recursive: bool,
    output: Option<PathBuf>,
    options: model::Options,
    size_choice: SizeChoice,
    extra_amount: u32,
    extra_unit: ExtraUnit,
    items: Vec<Item>,
    scan: Option<Job<Vec<Item>>>,
    scan_error: Option<String>,
    /// The items Start would write over, while the question is up.
    conflicts: Vec<usize>,
    pending_chd: ChdChoices,
    run: Option<Job<RunResult>>,
    place: Arc<Mutex<Option<Progress>>>,
    lines: Lines,
    log: Vec<(LogLevel, String)>,
    result: Option<Result<RunResult, String>>,
}

impl ConvertController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        ConvertController {
            notify,
            source: None,
            source_is_folder: false,
            recursive: false,
            output: None,
            options: model::Options::default(),
            size_choice: SizeChoice::Keep,
            extra_amount: 64,
            extra_unit: ExtraUnit::Mb,
            items: Vec::new(),
            scan: None,
            scan_error: None,
            conflicts: Vec::new(),
            pending_chd: ChdChoices::default(),
            run: None,
            place: Arc::new(Mutex::new(None)),
            lines: Arc::new(Mutex::new(Vec::new())),
            log: Vec::new(),
            result: None,
        }
    }

    // ---- What the shells draw ------------------------------------------------------------

    pub fn source(&self) -> Option<&Path> {
        self.source.as_deref()
    }

    pub fn source_is_folder(&self) -> bool {
        self.source_is_folder
    }

    pub fn recursive(&self) -> bool {
        self.recursive
    }

    pub fn output(&self) -> Option<&Path> {
        self.output.as_deref()
    }

    /// The output row's label: a folder for a folder source, a file otherwise.
    pub fn output_label(&self) -> &'static str {
        if self.source_is_folder {
            "Output folder"
        } else {
            "Save as"
        }
    }

    pub fn output_hint(&self) -> &'static str {
        if self.source_is_folder {
            hints::OUTPUT_FOLDER
        } else {
            hints::OUTPUT_FILE
        }
    }

    /// A path row's text.
    pub fn path_text(path: Option<&Path>) -> String {
        path.map(|p| p.display().to_string())
            .unwrap_or_else(|| "(not chosen)".to_string())
    }

    pub fn format(&self, kind: ImageKind) -> Format {
        self.options.target(kind).0
    }

    pub fn extension(&self, kind: ImageKind) -> &str {
        self.options.target(kind).1
    }

    /// The kinds whose format rows are shown: the one file's kind, or every kind the folder has.
    pub fn shown_kinds(&self) -> Vec<ImageKind> {
        let found: Vec<ImageKind> = ImageKind::ALL
            .iter()
            .copied()
            .filter(|k| {
                self.items
                    .iter()
                    .any(|i| i.found.as_ref().is_ok_and(|c| c.kind == *k))
            })
            .collect();
        if found.is_empty() && self.source_is_folder {
            ImageKind::ALL.to_vec()
        } else {
            found
        }
    }

    pub fn size_choice(&self) -> SizeChoice {
        self.size_choice
    }

    pub fn extra_amount(&self) -> u32 {
        self.extra_amount
    }

    pub fn extra_unit(&self) -> ExtraUnit {
        self.extra_unit
    }

    pub fn measure_slow(&self) -> bool {
        self.options.measure_slow
    }

    /// The slow-filesystems switch only matters when a hard disk shrinks.
    pub fn measure_slow_enabled(&self) -> bool {
        self.size_choice != SizeChoice::Keep
    }

    pub fn floppy_size(&self) -> FloppySize {
        self.options.floppy_size
    }

    pub fn verify(&self) -> bool {
        self.options.verify
    }

    pub fn multi_bin(&self) -> bool {
        self.options.multi_bin
    }

    /// The multi-bin switch only matters for BIN/CUE.
    pub fn multi_bin_shown(&self) -> bool {
        self.format(ImageKind::Disc) == Format::BinCue
            && self.shown_kinds().contains(&ImageKind::Disc)
    }

    /// The CHD profiles the chosen formats write, so the shell shows a CHD control for each.
    pub fn chd_profiles(&self) -> Vec<(ImageKind, ChdProfile)> {
        self.shown_kinds()
            .into_iter()
            .filter_map(|k| chd_profile(self.format(k)).map(|p| (k, p)))
            .collect()
    }

    pub fn items(&self) -> &[Item] {
        self.items.as_slice()
    }

    /// The file list's rows.
    pub fn rows(&self) -> Vec<Row> {
        self.items.iter().map(|i| self.row(i)).collect()
    }

    fn row(&self, item: &Item) -> Row {
        match &item.found {
            Err(e) => Row {
                name: item.name.clone(),
                kind: "-".to_string(),
                detail: String::new(),
                outcome: format!("Skipped: {e}"),
                skipped: true,
                selected: item.selected,
                note: None,
            },
            Ok(c) => {
                let refusal = self.options.refusal(c);
                let outcome = match &refusal {
                    Some(why) => format!("Skipped: {why}"),
                    None => self
                        .dest_for(c)
                        .and_then(|d| d.file_name().map(|n| n.to_string_lossy().into_owned()))
                        .map(|n| format!("-> {n}"))
                        .unwrap_or_default(),
                };
                Row {
                    name: item.name.clone(),
                    kind: c.kind.label().to_string(),
                    detail: c.detail.clone(),
                    outcome,
                    skipped: refusal.is_some(),
                    selected: item.selected,
                    note: c.mismatch_note(),
                }
            }
        }
    }

    /// The list's count line.
    pub fn summary(&self) -> String {
        if self.items.is_empty() {
            return String::new();
        }
        let ready = self.ready().len();
        format!(
            "{} file(s) found; {} will be converted.",
            self.items.len(),
            ready
        )
    }

    pub fn is_scanning(&self) -> bool {
        self.scan.is_some()
    }

    pub fn scan_snapshot(&self) -> Option<Snapshot> {
        self.scan.as_ref().map(Job::snapshot)
    }

    pub fn scan_error(&self) -> Option<&str> {
        self.scan_error.as_deref()
    }

    pub fn is_running(&self) -> bool {
        self.run.is_some()
    }

    pub fn run_snapshot(&self) -> Option<Snapshot> {
        self.run.as_ref().map(Job::snapshot)
    }

    /// The running conversion's place.
    pub fn progress(&self) -> Option<Progress> {
        crate::model::worker::lock_status(&*self.place).clone()
    }

    /// The progress strip's fraction and text while a conversion runs.
    pub fn strip(&self) -> Option<(f32, String)> {
        let snap = self.run_snapshot()?;
        let p = self.progress()?;
        let frac = snap.progress.fraction().unwrap_or(0.0);
        Some((
            frac,
            format!(
                "Convert: [{}/{}] {} - {}",
                p.index,
                p.total,
                p.file,
                snap.detail()
            ),
        ))
    }

    pub fn last_result(&self) -> Option<&Result<RunResult, String>> {
        self.result.as_ref()
    }

    /// Log lines since the last call, for the shell's log panel.
    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        std::mem::take(&mut self.log)
    }

    /// Why Start would be refused, checked in this order; `None` when it can start.
    pub fn start_problem(&self) -> Option<String> {
        if self.source.is_none() {
            return Some("Choose an image or a folder to convert.".to_string());
        }
        if self.scan.is_some() {
            return Some("Still reading the files...".to_string());
        }
        if self.output.is_none() {
            return Some(if self.source_is_folder {
                "Choose an output folder.".to_string()
            } else {
                "Choose where to save the converted image.".to_string()
            });
        }
        for kind in self.shown_kinds() {
            if let Some(p) = extension_problem(self.extension(kind)) {
                return Some(p.to_string());
            }
        }
        if self.ready().is_empty() {
            return Some("No selected file can be converted with these choices.".to_string());
        }
        None
    }

    pub fn can_start(&self) -> bool {
        !self.is_running() && self.start_problem().is_none()
    }

    pub fn start_label(&self) -> String {
        match self.ready().len() {
            1 => "Convert".to_string(),
            n => format!("Convert {n} files"),
        }
    }

    // ---- Rules ---------------------------------------------------------------------------

    /// Where a classified file goes.
    fn dest_for(&self, c: &Classified) -> Option<PathBuf> {
        let out = self.output.as_deref()?;
        if !self.source_is_folder {
            return Some(out.to_path_buf());
        }
        let root = self.source.as_deref()?;
        Some(output_path(
            c.path.as_path(),
            root,
            out,
            self.extension(c.kind),
        ))
    }

    /// The selected items that will convert, with their outputs.
    fn ready(&self) -> Vec<(usize, PathBuf)> {
        self.items
            .iter()
            .enumerate()
            .filter(|(_, i)| i.selected)
            .filter_map(|(n, i)| {
                let c = i.found.as_ref().ok()?;
                if self.options.refusal(c).is_some() {
                    return None;
                }
                Some((n, self.dest_for(c)?))
            })
            .collect()
    }

    fn sizing(&self) -> Sizing {
        match self.size_choice {
            SizeChoice::Keep => Sizing::Keep,
            SizeChoice::Minimum => Sizing::Minimum,
            SizeChoice::Extra => Sizing::Extra(match self.extra_unit {
                ExtraUnit::Mb => Extra::Mib(self.extra_amount),
                ExtraUnit::Percent => Extra::Percent(self.extra_amount),
            }),
        }
    }

    fn set_source(&mut self, path: PathBuf) {
        if self.is_running() {
            return;
        }
        self.source_is_folder = path.is_dir();
        self.source = Some(path);
        self.output = None;
        self.items.clear();
        self.scan_error = None;
        self.start_scan();
    }

    fn start_scan(&mut self) {
        let Some(source) = self.source.clone() else {
            return;
        };
        let (folder, recursive) = (self.source_is_folder, self.recursive);
        self.scan = Some(Job::spawn(
            "Reading the files...",
            Arc::clone(&self.notify),
            move |ctx| {
                let paths = if folder {
                    model::scan(source.as_path(), recursive)?
                } else {
                    vec![source.clone()]
                };
                let total = paths.len();
                let mut items = Vec::with_capacity(total);
                for (n, path) in paths.into_iter().enumerate() {
                    ctx.check_cancel()?;
                    ctx.set_phase(format!("{}/{total}", n + 1));
                    let name = if folder {
                        path.strip_prefix(source.as_path())
                            .map(|p| p.display().to_string())
                            .unwrap_or_else(|_| path.display().to_string())
                    } else {
                        path.file_name()
                            .map(|n| n.to_string_lossy().into_owned())
                            .unwrap_or_default()
                    };
                    let found = classify(path.as_path()).map_err(|e| e.to_string());
                    items.push(Item {
                        path,
                        name,
                        found,
                        selected: true,
                    });
                }
                Ok(items)
            },
        ));
    }

    fn poll_scan(&mut self) {
        let Some(done) = self.scan.as_ref().and_then(job_outcome) else {
            return;
        };
        self.scan = None;
        match done {
            Ok(items) if items.is_empty() => {
                self.scan_error = Some("No files found in that folder.".to_string());
            }
            Ok(items) => {
                self.items = items;
                // One file: start each kind's choices on that file's kind.
                if !self.source_is_folder {
                    self.suggest_output_name();
                }
            }
            Err(e) if e == "cancelled" => {}
            Err(e) => self.scan_error = Some(format!("Could not read the files: {e}")),
        }
    }

    /// A one-file source's save name: beside it, with the format's extension.
    fn suggest_output_name(&mut self) {
        let Some(c) = self.items.first().and_then(|i| i.found.as_ref().ok()) else {
            return;
        };
        let ext = self.extension(c.kind).to_string();
        self.output = Some(c.path.with_extension(ext));
        if self.output.as_deref() == Some(c.path.as_path()) {
            self.output = None;
        }
    }

    /// A one-file output follows its kind's extension, and is never the source itself.
    fn follow_extension(&mut self, kind: ImageKind) {
        if self.source_is_folder {
            return;
        }
        let Some(src) = self
            .items
            .first()
            .and_then(|i| i.found.as_ref().ok())
            .filter(|c| c.kind == kind)
            .map(|c| c.path.clone())
        else {
            return;
        };
        let base = self.output.clone().unwrap_or_else(|| src.clone());
        let next = base.with_extension(self.extension(kind));
        self.output = (next != src).then_some(next);
    }

    fn set_target(&mut self, kind: ImageKind, format: Option<Format>, ext: Option<String>) {
        let slot = match kind {
            ImageKind::HardDisk => &mut self.options.hard_disk,
            ImageKind::Floppy => &mut self.options.floppy,
            ImageKind::Disc => &mut self.options.disc,
        };
        if let Some(f) = format {
            if !formats_for(kind).contains(&f) || f.unavailable().is_some() {
                return;
            }
            slot.0 = f;
            slot.1 = f.extension().to_string();
        }
        if let Some(e) = ext {
            slot.1 = e;
        }
        self.follow_extension(kind);
    }

    fn start(&mut self, chd: ChdChoices) {
        if !self.can_start() {
            return;
        }
        let conflicts: Vec<usize> = self
            .ready()
            .into_iter()
            .filter(|(_, dest)| dest.exists())
            .map(|(n, _)| n)
            .collect();
        if conflicts.is_empty() {
            let all = self.ready();
            self.launch(all, chd);
            return;
        }
        let count = conflicts.len();
        self.conflicts = conflicts;
        self.pending_chd = chd;
        self.log.push((
            LogLevel::Info,
            format!("{count} output file(s) already exist."),
        ));
    }

    /// The question Start asks when outputs exist; the shell shows it from [`Self::tick`].
    fn conflict_question(&self) -> Effect {
        let n = self.conflicts.len();
        Effect::Choose(ChoiceSpec {
            purpose: purpose::CONFLICTS,
            title: "Output files already exist".to_string(),
            message: format!(
                "{n} converted file(s) would replace files that are already there.\n\
                 Skip: leave those alone and convert the rest.\n\
                 Overwrite: replace them."
            ),
            choices: vec![
                Choice {
                    label: "Skip".to_string(),
                    key: 's',
                },
                Choice {
                    label: "Overwrite".to_string(),
                    key: 'o',
                },
                Choice {
                    label: "Back".to_string(),
                    key: 'b',
                },
            ],
            dismiss: 2,
        })
    }

    fn resolve(&mut self, index: usize) {
        let conflicts = std::mem::take(&mut self.conflicts);
        let chd = std::mem::take(&mut self.pending_chd);
        let ready = self.ready();
        match index {
            0 => {
                let keep: Vec<(usize, PathBuf)> = ready
                    .into_iter()
                    .filter(|(n, _)| !conflicts.contains(n))
                    .collect();
                if !keep.is_empty() {
                    self.launch(keep, chd);
                }
            }
            1 => self.launch(ready, chd),
            _ => {}
        }
    }

    fn launch(&mut self, chosen: Vec<(usize, PathBuf)>, chd: ChdChoices) {
        let jobs: Vec<(Classified, PathBuf, String)> = chosen
            .into_iter()
            .filter_map(|(n, dest)| {
                let item = self.items.get(n)?;
                Some((item.found.clone().ok()?, dest, item.name.clone()))
            })
            .collect();
        let mut options = self.options.clone();
        options.sizing = self.sizing();
        options.chd = chd;
        let total = jobs.len();
        let skipped = self.items.len() - total;
        self.log.push((
            LogLevel::Info,
            format!(
                "Converting {total} file(s); hard disks: {}.",
                options.sizing.describe()
            ),
        ));
        self.result = None;
        let place = Arc::clone(&self.place);
        let lines = Arc::clone(&self.lines);
        self.run = Some(Job::spawn(
            "Convert",
            Arc::clone(&self.notify),
            move |ctx| {
                let say = |level: LogLevel, msg: String| {
                    crate::model::worker::lock_status(&*lines).push((level, msg));
                };
                let mut result = RunResult {
                    skipped,
                    ..RunResult::default()
                };
                for (n, (found, dest, name)) in jobs.iter().enumerate() {
                    if ctx.is_cancelled() {
                        say(LogLevel::Warning, "Convert cancelled.".to_string());
                        break;
                    }
                    *crate::model::worker::lock_status(&*place) = Some(Progress {
                        index: n + 1,
                        total,
                        file: name.clone(),
                    });
                    ctx.set_phase(format!("{}/{total} {name}", n + 1));
                    say(
                        LogLevel::Info,
                        format!("[{}/{total}] {name} -> {}", n + 1, dest.display()),
                    );
                    let progress = |done: u64, all: u64| {
                        if all > 0 {
                            ctx.set_bytes(done.min(all), all);
                        }
                    };
                    let cancelled = || ctx.is_cancelled();
                    let log = |level: Level, msg: &str| {
                        let level = match level {
                            Level::Info => LogLevel::Info,
                            Level::Warning => LogLevel::Warning,
                        };
                        say(level, msg.to_string());
                    };
                    let hooks = Hooks {
                        progress: &progress,
                        cancelled: &cancelled,
                        log: &log,
                    };
                    match convert_one(found, dest.as_path(), &options, &hooks) {
                        Ok(done) => {
                            result.succeeded += 1;
                            if let Some(line) = done.verified {
                                say(LogLevel::Info, format!("  {line}"));
                            }
                            say(LogLevel::Info, format!("  OK: {}", dest.display()));
                        }
                        Err(e) => {
                            result.failed += 1;
                            remove_partial(dest.as_path());
                            say(LogLevel::Warning, format!("  Failed: {name}: {e:#}"));
                        }
                    }
                }
                *crate::model::worker::lock_status(&*place) = None;
                say(
                    LogLevel::Info,
                    format!(
                        "Convert finished: {} converted, {} failed, {} skipped.",
                        result.succeeded, result.failed, result.skipped
                    ),
                );
                Ok(result)
            },
        ));
    }

    fn poll_run(&mut self) {
        let fresh = std::mem::take(&mut *crate::model::worker::lock_status(&*self.lines));
        self.log.extend(fresh);
        if let Some(done) = self.run.as_ref().and_then(job_outcome) {
            self.run = None;
            *crate::model::worker::lock_status(&*self.place) = None;
            if let Err(e) = &done {
                if e != "cancelled" {
                    self.log
                        .push((LogLevel::Error, format!("Convert failed: {e}")));
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

/// A failed output is removed, with a BIN/CUE's .bin.
fn remove_partial(dest: &Path) {
    let _ = std::fs::remove_file(dest);
    if dest
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("cue"))
    {
        let _ = std::fs::remove_file(dest.with_extension("bin"));
    }
}

fn picker(purpose: &'static str, title: &str, save: bool, file_name: String) -> PickSpec {
    PickSpec {
        purpose,
        title: title.to_string(),
        filters: Vec::new(),
        save,
        file_name,
    }
}

impl Controller for ConvertController {
    type Action = ConvertAction;

    fn apply(&mut self, action: ConvertAction) -> Vec<Effect> {
        let busy = self.is_running();
        match action {
            ConvertAction::PickSourceFile if !busy => {
                return vec![Effect::PickFile(picker(
                    purpose::SOURCE_FILE,
                    "Choose an image to convert",
                    false,
                    String::new(),
                ))]
            }
            ConvertAction::PickSourceFolder if !busy => {
                return vec![Effect::PickFolder(picker(
                    purpose::SOURCE_FOLDER,
                    "Choose a folder of images",
                    false,
                    String::new(),
                ))]
            }
            ConvertAction::PickOutput if !busy && self.source.is_some() => {
                return if self.source_is_folder {
                    vec![Effect::PickFolder(picker(
                        purpose::OUTPUT_FOLDER,
                        "Choose the output folder",
                        false,
                        String::new(),
                    ))]
                } else {
                    let name = self
                        .output
                        .as_ref()
                        .and_then(|p| p.file_name())
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    vec![Effect::PickFile(picker(
                        purpose::OUTPUT_FILE,
                        "Save the converted image as",
                        true,
                        name,
                    ))]
                };
            }
            ConvertAction::PickSourceFile
            | ConvertAction::PickSourceFolder
            | ConvertAction::PickOutput => {}
            ConvertAction::Picked { purpose, path } => match (purpose, path) {
                (purpose::SOURCE_FILE | purpose::SOURCE_FOLDER, Some(p)) => self.set_source(p),
                (purpose::OUTPUT_FILE | purpose::OUTPUT_FOLDER, Some(p)) if !busy => {
                    self.output = Some(p)
                }
                _ => {}
            },
            ConvertAction::SetSource(p) => self.set_source(p),
            ConvertAction::SetRecursive(on) => {
                if on != self.recursive && !busy {
                    self.recursive = on;
                    if self.source_is_folder {
                        self.items.clear();
                        self.start_scan();
                    }
                }
            }
            ConvertAction::SetFormat(kind, format) => self.set_target(kind, Some(format), None),
            ConvertAction::SetExtension(kind, ext) => self.set_target(kind, None, Some(ext)),
            ConvertAction::ResetExtension(kind) => {
                let ext = self.format(kind).extension().to_string();
                self.set_target(kind, None, Some(ext));
            }
            ConvertAction::SetSizing(c) => self.size_choice = c,
            ConvertAction::SetExtraAmount(n) => self.extra_amount = n.min(1_000_000),
            ConvertAction::SetExtraUnit(u) => self.extra_unit = u,
            ConvertAction::SetMeasureSlow(on) => self.options.measure_slow = on,
            ConvertAction::SetFloppySize(s) => self.options.floppy_size = s,
            ConvertAction::SetVerify(on) => self.options.verify = on,
            ConvertAction::SetMultiBin(on) => self.options.multi_bin = on,
            ConvertAction::SetSelected(n, on) => {
                if let Some(i) = self.items.get_mut(n) {
                    i.selected = on;
                }
            }
            ConvertAction::SelectAll => self.items.iter_mut().for_each(|i| i.selected = true),
            ConvertAction::SelectNone => self.items.iter_mut().for_each(|i| i.selected = false),
            ConvertAction::Start(chd) => {
                self.start(chd);
                if !self.conflicts.is_empty() {
                    return vec![self.conflict_question()];
                }
            }
            ConvertAction::Chose { purpose, index } => {
                if purpose == purpose::CONFLICTS {
                    self.resolve(index);
                }
            }
            ConvertAction::Cancel => {
                if let Some(job) = &self.run {
                    job.cancel();
                    self.log.push((
                        LogLevel::Warning,
                        "Convert will stop after the current file...".to_string(),
                    ));
                }
                if let Some(job) = &self.scan {
                    job.cancel();
                }
            }
            ConvertAction::Clear => {
                if !busy {
                    self.source = None;
                    self.output = None;
                    self.items.clear();
                    self.scan = None;
                    self.scan_error = None;
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
mod tests;
