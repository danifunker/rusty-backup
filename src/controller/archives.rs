//! The Archives screen: open a classic Mac archive, tick entries, extract them in a
//! fork-preserving format, or hand a single-entry disk-image payload to Inspect.
//!
//! Opening and extracting are jobs over `model::mac_archive_extract`; the host carries out
//! [`HostRequest`]s (the recent list, the Inspect hand-off).

use std::path::PathBuf;
use std::sync::Arc;

use super::log::LogLevel;
use super::{job_outcome, Controller, Effect, PickSpec};
use crate::model::job::{Job, Notify, Progress, Snapshot, Unit};
use crate::model::mac_archive_extract::{
    extract, files_to_extract, open_archive, write_payload_to_tempdir, OpenedArchive,
};

pub use crate::model::job::NoNotify;
pub use crate::model::mac_archive_extract::ArchiveEntryRow;
pub use arc_mac::detect::MountableImageKind;
pub use arc_mac::extract::{ExtractStats, ForkFormat};

/// The `purpose` of each picker this controller asks for.
pub mod purpose {
    pub const ARCHIVE: &str = "archives.archive";
    pub const EXTRACT_ALL: &str = "archives.extract_all";
    pub const EXTRACT_SELECTED: &str = "archives.extract_selected";
}

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const SOURCE: &str = "archives.source";
    pub const BROWSE: &str = "archives.browse";
    pub const CLOSE: &str = "archives.close";
    pub const RECENT: &str = "archives.recent";
    pub const ENTRIES: &str = "archives.entries";
    pub const FORMAT: &str = "archives.format";
    pub const EXTRACT_ALL: &str = "archives.extract_all";
    pub const EXTRACT_SELECTED: &str = "archives.extract_selected";
    pub const MOUNT: &str = "archives.mount";
}

/// The text every shell shows on the same controls.
pub mod hints {
    pub const INTRO: &str = "Browse and extract classic Mac archives. Detection is magic-driven, \
         so the picker accepts any file: StuffIt (.sit / .sea, classic + StuffIt \
         5) and BinHex 4.0 (loose .hqx or .sit.hqx / .sea.hqx wrappers).";
    pub const CLOSE: &str = "Unload the current archive";
    pub const EXTRACT_SELECTED: &str = "Extract only the entries with their checkbox ticked. \
         Folder markers are always extracted so the rebuilt \
         tree stays rooted.";
}

/// The Mount button's label for a payload of this kind.
pub fn mount_label(kind: MountableImageKind) -> String {
    format!("Mount in new Inspect tab ({})", kind.label())
}

/// The Mount button's tooltip.
pub fn mount_hint(kind: MountableImageKind) -> String {
    format!(
        "The archive's contents are themselves a {} image. \
         Open the decoded payload in a new Inspect tab \
         instead of extracting it as a file.",
        kind.label(),
    )
}

/// An entry's size in binary units, as the entry lists show it.
pub fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

/// What only the host can do.
pub enum HostRequest {
    /// An archive was opened: put it at the top of the Archives recent list.
    RememberRecent(PathBuf),
    /// Open this decoded payload in Inspect; the guard keeps its temporary folder alive.
    OpenInInspect {
        path: PathBuf,
        guard: tempfile::TempDir,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchivesAction {
    /// "Browse...": asks for an archive file.
    Pick,
    Open(PathBuf),
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    /// Unload the archive (the recent list stays).
    Close,
    SetSelected(usize, bool),
    SetFormat(ForkFormat),
    /// Ask for a folder, then extract every entry into it.
    ExtractAll,
    /// Ask for a folder, then extract the ticked entries (folders always).
    ExtractSelected,
    /// Extract every entry into this folder at once (a shell with its own folder prompt).
    ExtractAllTo(PathBuf),
    /// Write the mountable payload to a temporary file and hand it to Inspect.
    Mount,
}

/// Why an open or an extraction failed: the error alone, and with its causes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub message: String,
    pub detail: String,
}

impl Failure {
    fn of(e: &anyhow::Error) -> Self {
        Failure {
            message: format!("{e}"),
            detail: format!("{e:#}"),
        }
    }
}

/// The finished extraction: its counts and where it wrote, or why it failed.
pub type ExtractOutcome = Result<(ExtractStats, PathBuf), Failure>;

/// An extraction's result and the lines it logged along the way.
type Extracted = (Result<ExtractStats, Failure>, Vec<String>);

pub struct ArchivesController {
    notify: Arc<dyn Notify>,
    path: Option<PathBuf>,
    loaded: Option<OpenedArchive>,
    selected: Vec<bool>,
    load: Option<Job<Result<OpenedArchive, Failure>>>,
    load_error: Option<Failure>,
    format: ForkFormat,
    extract: Option<(PathBuf, Job<Extracted>)>,
    last_extract: Option<ExtractOutcome>,
    log: Vec<(LogLevel, String)>,
    requests: Vec<HostRequest>,
}

impl ArchivesController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        ArchivesController {
            notify,
            path: None,
            loaded: None,
            selected: Vec::new(),
            load: None,
            load_error: None,
            format: ForkFormat::BinHex,
            extract: None,
            last_extract: None,
            log: Vec::new(),
            requests: Vec::new(),
        }
    }

    pub fn path(&self) -> Option<&std::path::Path> {
        self.path.as_deref()
    }

    pub fn is_loaded(&self) -> bool {
        self.loaded.is_some()
    }

    pub fn rows(&self) -> &[ArchiveEntryRow] {
        match &self.loaded {
            Some(l) => l.rows.as_slice(),
            None => &[],
        }
    }

    pub fn file_count(&self) -> usize {
        self.loaded.as_ref().map_or(0, |l| l.file_count)
    }

    /// Whether entry `i` is ticked for Extract Selected.
    pub fn is_selected(&self, i: usize) -> bool {
        self.selected.get(i).copied().unwrap_or(true)
    }

    /// Ticked files (folders do not count).
    pub fn selected_file_count(&self) -> usize {
        self.loaded.as_ref().map_or(0, |l| {
            files_to_extract(l.archive.as_ref(), Some(self.selected.as_slice()))
        })
    }

    pub fn format(&self) -> ForkFormat {
        self.format
    }

    pub fn load_error(&self) -> Option<&Failure> {
        self.load_error.as_ref()
    }

    pub fn load_snapshot(&self) -> Option<Snapshot> {
        self.load.as_ref().map(Job::snapshot)
    }

    pub fn is_loading(&self) -> bool {
        self.load.is_some()
    }

    /// The archive is one disk image (its kind), offered as Mount in Inspect.
    pub fn mountable(&self) -> Option<MountableImageKind> {
        self.loaded.as_ref()?.mountable.as_ref().map(|m| m.kind)
    }

    pub fn is_extracting(&self) -> bool {
        self.extract.is_some()
    }

    /// A running extraction's (done, total, current entry).
    pub fn extract_progress(&self) -> Option<(usize, usize, String)> {
        let snap = self.extract.as_ref()?.1.snapshot();
        let (done, total) = match snap.progress {
            Progress::Determinate { done, total, .. } => (done as usize, total as usize),
            _ => (0, 0),
        };
        Some((done, total, snap.phase))
    }

    pub fn last_extract(&self) -> Option<&ExtractOutcome> {
        self.last_extract.as_ref()
    }

    pub fn take_log(&mut self) -> Vec<(LogLevel, String)> {
        std::mem::take(&mut self.log)
    }

    pub fn take_requests(&mut self) -> Vec<HostRequest> {
        std::mem::take(&mut self.requests)
    }

    fn open(&mut self, path: PathBuf) {
        self.requests
            .push(HostRequest::RememberRecent(path.clone()));
        self.path = Some(path.clone());
        self.loaded = None;
        self.selected.clear();
        self.load_error = None;
        // The last extraction's line belongs to the archive it came from.
        if self.extract.is_none() {
            self.last_extract = None;
        }
        self.load = Some(Job::spawn(
            "Reading the archive...",
            Arc::clone(&self.notify),
            move |job| {
                job.set_can_cancel(false);
                Ok(open_archive(path.as_path()).map_err(|e| Failure::of(&e)))
            },
        ));
    }

    fn close(&mut self) {
        self.path = None;
        self.load = None;
        self.loaded = None;
        self.selected.clear();
        self.load_error = None;
    }

    fn start_extract(&mut self, dest: PathBuf, selected_only: bool) {
        if self.extract.is_some() {
            return;
        }
        let Some(loaded) = self.loaded.as_ref() else {
            return;
        };
        let bytes = Arc::clone(&loaded.bytes);
        let archive = Arc::clone(&loaded.archive);
        let keep = selected_only.then(|| self.selected.clone());
        let total = match &keep {
            Some(k) => files_to_extract(archive.as_ref(), Some(k.as_slice())),
            None => loaded.file_count,
        };
        let format = self.format;
        self.log.push((
            LogLevel::Info,
            format!(
                "Extracting {} archive entries to {} as {}",
                if selected_only {
                    format!("selected ({total})")
                } else {
                    "all".into()
                },
                dest.display(),
                format.label(),
            ),
        ));
        self.last_extract = None;
        let to = dest.clone();
        let job = Job::spawn("Extracting", Arc::clone(&self.notify), move |ctx| {
            ctx.set_can_cancel(false);
            let items = |done: usize, total: usize| Progress::Determinate {
                done: done as u64,
                total: total as u64,
                unit: Unit::Items,
            };
            ctx.set_progress(items(0, total));
            let progress = |done: usize, total: usize, name: &str| {
                ctx.set_phase(name.to_string());
                ctx.set_progress(items(done, total));
            };
            let mut lines = Vec::new();
            let stats = extract(
                bytes.as_slice(),
                archive.as_ref(),
                to.as_path(),
                format,
                keep,
                progress,
                |l| lines.push(l),
            );
            Ok((stats.map_err(|e| Failure::of(&e)), lines))
        });
        self.extract = Some((dest, job));
    }

    fn poll(&mut self) {
        if let Some(done) = self.load.as_ref().and_then(job_outcome) {
            self.load = None;
            match done.unwrap_or_else(|e| {
                Err(Failure {
                    message: e.clone(),
                    detail: e,
                })
            }) {
                Ok(opened) => {
                    self.selected = vec![true; opened.archive.entries.len()];
                    self.loaded = Some(opened);
                }
                Err(e) => self.load_error = Some(e),
            }
        }
        if let Some(done) = self.extract.as_ref().and_then(|(_, j)| job_outcome(j)) {
            let (dest, _) = self.extract.take().expect("extract job");
            let (done, lines) = done.unwrap_or_else(|e| {
                (
                    Err(Failure {
                        message: e.clone(),
                        detail: e,
                    }),
                    Vec::new(),
                )
            });
            for line in lines {
                self.log.push((LogLevel::Warning, line));
            }
            match &done {
                Ok(stats) => {
                    self.log.push((
                        LogLevel::Info,
                        format!(
                            "Extraction complete: {} files{}",
                            stats.files,
                            if stats.skipped > 0 {
                                format!(", {} skipped", stats.skipped)
                            } else {
                                String::new()
                            }
                        ),
                    ));
                }
                Err(e) => self
                    .log
                    .push((LogLevel::Error, format!("Extraction failed: {}", e.message))),
            }
            self.last_extract = Some(done.map(|s| (s, dest)));
        }
    }

    fn mount(&mut self) {
        let Some(payload) = self.loaded.as_ref().and_then(|l| l.mountable.as_ref()) else {
            return;
        };
        match write_payload_to_tempdir(payload.name.as_str(), payload.decoded.as_slice()) {
            Ok((path, guard)) => {
                self.log.push((
                    LogLevel::Info,
                    format!(
                        "Routing {} payload to Inspect tab: {}",
                        payload.kind.label(),
                        path.display()
                    ),
                ));
                self.requests
                    .push(HostRequest::OpenInInspect { path, guard });
            }
            Err(e) => self.log.push((
                LogLevel::Error,
                format!("Failed to materialize archive payload: {e}"),
            )),
        }
    }

    /// Block until the open and any extraction end; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        while self.load.is_some() || self.extract.is_some() {
            self.poll();
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
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

impl Controller for ArchivesController {
    type Action = ArchivesAction;

    fn apply(&mut self, action: ArchivesAction) -> Vec<Effect> {
        let busy = self.extract.is_some();
        match action {
            ArchivesAction::Pick if !busy => {
                let exts = crate::model::file_types::archives_picker_exts()
                    .into_iter()
                    .map(str::to_string)
                    .collect();
                return vec![Effect::PickFile(PickSpec {
                    purpose: purpose::ARCHIVE,
                    title: "Open Mac Archive".to_string(),
                    filters: vec![
                        ("Mac Archives".to_string(), exts),
                        ("All Files".to_string(), vec!["*".to_string()]),
                    ],
                    save: false,
                    file_name: String::new(),
                })];
            }
            // A running extraction keeps its own copy of the archive, so a new one may open.
            ArchivesAction::Open(path) => self.open(path),
            ArchivesAction::Picked { purpose, path } => match (purpose, path) {
                (purpose::ARCHIVE, Some(p)) => self.open(p),
                (purpose::EXTRACT_ALL, Some(p)) => self.start_extract(p, false),
                (purpose::EXTRACT_SELECTED, Some(p)) => self.start_extract(p, true),
                _ => {}
            },
            ArchivesAction::Close if !busy => self.close(),
            ArchivesAction::SetSelected(i, on) => {
                if let Some(slot) = self.selected.get_mut(i) {
                    *slot = on;
                }
            }
            ArchivesAction::SetFormat(f) => self.format = f,
            ArchivesAction::ExtractAll if !busy && self.loaded.is_some() => {
                return folder_picker(purpose::EXTRACT_ALL, "Extract All To")
            }
            ArchivesAction::ExtractSelected if !busy && self.selected_file_count() > 0 => {
                return folder_picker(purpose::EXTRACT_SELECTED, "Extract Selected To")
            }
            ArchivesAction::ExtractAllTo(dest) => self.start_extract(dest, false),
            ArchivesAction::Mount if !busy => self.mount(),
            ArchivesAction::Pick
            | ArchivesAction::Close
            | ArchivesAction::ExtractAll
            | ArchivesAction::ExtractSelected
            | ArchivesAction::Mount => {}
        }
        Vec::new()
    }

    fn tick(&mut self) -> Vec<Effect> {
        self.poll();
        Vec::new()
    }
}

/// A classic StuffIt archive of TEXT files at `dir/a.sit`, for tests.
#[cfg(test)]
pub(crate) fn sample_archive(dir: &std::path::Path, files: &[(&str, &[u8])]) -> PathBuf {
    use arc_mac::stuffit::{build_archive, StuffItInput, WriteMethod};
    let inputs: Vec<StuffItInput> = files
        .iter()
        .map(|(name, data)| StuffItInput {
            name: name.to_string(),
            type_code: *b"TEXT",
            creator_code: *b"ttxt",
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: data.to_vec(),
            resource_fork: Vec::new(),
        })
        .collect();
    let path = dir.join("a.sit");
    std::fs::write(
        &path,
        build_archive(inputs.as_slice(), WriteMethod::Store).unwrap(),
    )
    .unwrap();
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sit(dir: &std::path::Path) -> PathBuf {
        sample_archive(dir, &[("one", b"first"), ("two", b"second")])
    }

    #[test]
    fn open_tick_and_extract_the_selected_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = sit(dir.path());
        let mut c = ArchivesController::new(Arc::new(NoNotify));
        c.apply(ArchivesAction::Open(path.clone()));
        assert!(matches!(
            c.take_requests().as_slice(),
            [HostRequest::RememberRecent(p)] if *p == path
        ));
        c.wait_idle();
        assert_eq!(c.file_count(), 2);
        assert_eq!(c.rows()[0].type_code, "TEXT");
        assert!(c.mountable().is_none());
        c.apply(ArchivesAction::SetSelected(0, false));
        assert_eq!(c.selected_file_count(), 1);
        let fx = c.apply(ArchivesAction::ExtractSelected);
        assert!(matches!(fx.as_slice(), [Effect::PickFolder(_)]));
        let out = dir.path().join("out");
        c.apply(ArchivesAction::SetFormat(ForkFormat::Raw));
        c.apply(ArchivesAction::Picked {
            purpose: purpose::EXTRACT_SELECTED,
            path: Some(out.clone()),
        });
        c.wait_idle();
        let (stats, dest) = c.last_extract().unwrap().as_ref().unwrap();
        assert_eq!((stats.files, dest), (1, &out));
        assert_eq!(std::fs::read(out.join("two")).unwrap(), b"second");
        assert!(!out.join("one").exists());
        let log: Vec<String> = c.take_log().into_iter().map(|(_, l)| l).collect();
        assert!(log[0].starts_with("Extracting selected (1) archive entries to "));
        assert_eq!(log.last().unwrap(), "Extraction complete: 1 files");
    }

    #[test]
    fn a_file_that_is_no_archive_reports_why() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.sit");
        std::fs::write(&path, b"not an archive").unwrap();
        let mut c = ArchivesController::new(Arc::new(NoNotify));
        c.apply(ArchivesAction::Open(path));
        c.wait_idle();
        assert!(!c.is_loaded());
        assert!(!c.load_error().unwrap().message.is_empty());
    }
}
