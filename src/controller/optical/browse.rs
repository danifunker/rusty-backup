//! The Optical tab's disc browser: one expandable tree of the disc, a preview, extraction, an
//! export of the marked entries and the tree listing.
//!
//! [`DiscBrowseController`] owns what is open (the disc, the hybrid side or boot area chosen),
//! the folders listed so far, the selection and marks, and every job; the bodies are
//! `model::optical_browse`. The Optical tab's controller hosts it and forwards
//! `OpticalAction::Browse`. Why this is not the shared browser pane: docs/phase2_commander_plan.md
//! § "Slice 16".

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::controller::{job_outcome, Effect, PickSpec};
use crate::model::job::{Job, Notify, Snapshot};
use crate::model::optical_browse::{self as ob, DiscChoice, OpenedDisc, RootRead};

pub use crate::model::optical_browse::{
    display_name, format_size, hex_dump, DiscSource, EntryType, ExportFormat, FileContent,
    FileEntry, ResourceForkMode, MAX_PREVIEW_SIZE,
};

/// The `purpose` of each picker the browser asks for.
pub mod purpose {
    pub const EXTRACT_TO: &str = "optical_browse.extract_to";
    pub const EXPORT_FILE: &str = "optical_browse.export_file";
    pub const EXPORT_FOLDER: &str = "optical_browse.export_folder";
    pub const SAVE_TREE: &str = "optical_browse.save_tree";
}

/// Stable widget ids.
pub mod ids {
    pub const FS_PICKER: &str = "optical_browse.fs_picker";
    pub const TREE: &str = "optical_browse.tree";
    pub const CLOSE: &str = "optical_browse.close";
    pub const EXPORT_FORMAT: &str = "optical_browse.export_format";
    pub const EXPORT: &str = "optical_browse.export";
    pub const CLEAR_MARKS: &str = "optical_browse.clear_marks";
    pub const FORK_MODE: &str = "optical_browse.fork_mode";
    pub const EXTRACT: &str = "optical_browse.extract";
    pub const TREE_COPY: &str = "optical_browse.tree_copy";
    pub const TREE_SAVE: &str = "optical_browse.tree_save";
}

/// Browser intents.
#[derive(Debug, Clone)]
pub enum DiscBrowseAction {
    /// Pick which filesystem of a hybrid disc (or which boot area) to browse.
    SelectFs(usize),
    /// A folder was expanded: list it unless it already is.
    Expand(FileEntry),
    /// A row was clicked: a file is previewed, a folder is selected for Extract Folder.
    Select(FileEntry),
    SetMarked {
        entry: FileEntry,
        marked: bool,
    },
    ClearMarks,
    SetExportFormat(ExportFormat),
    /// "Export...": asks where the marked entries go.
    Export,
    SetForkMode(ResourceForkMode),
    /// "Extract File..." / "Extract Folder...": asks for a folder.
    Extract,
    CancelExtract,
    /// "Tree": builds the listing for the tree window.
    Tree,
    CloseTree,
    CopyTree,
    /// "Save to File..." in the tree window.
    SaveTree,
    Picked {
        purpose: &'static str,
        path: Option<PathBuf>,
    },
    Close,
}

type Listing = Job<Result<Vec<FileEntry>, String>>;

pub struct DiscBrowseController {
    notify: Arc<dyn Notify>,
    active: bool,
    opening: Option<Job<(DiscSource, OpenedDisc)>>,
    disc: Option<DiscChoice>,
    labels: Vec<String>,
    game: Option<String>,
    switching: Option<Job<RootRead>>,
    root: Option<FileEntry>,
    dirs: HashMap<String, Vec<FileEntry>>,
    listing: Vec<(String, Listing)>,
    /// Folders whose read failed; not retried until the filesystem changes.
    failed: HashSet<String>,
    selected: Option<FileEntry>,
    content: Option<FileContent>,
    preview: Option<(String, Job<Result<FileContent, String>>)>,
    marked: BTreeMap<String, FileEntry>,
    export_format: ExportFormat,
    fork_mode: ResourceForkMode,
    error: Option<String>,
    result: Option<String>,
    extracting: Option<Job<Result<u32, String>>>,
    exporting: Option<Job<Result<(usize, PathBuf), String>>>,
    tree_job: Option<Job<Result<String, String>>>,
    tree_text: Option<String>,
    saving: Option<Job<Result<(), String>>>,
}

impl DiscBrowseController {
    pub fn new(notify: Arc<dyn Notify>) -> Self {
        DiscBrowseController {
            notify,
            active: false,
            opening: None,
            disc: None,
            labels: Vec::new(),
            game: None,
            switching: None,
            root: None,
            dirs: HashMap::new(),
            listing: Vec::new(),
            failed: HashSet::new(),
            selected: None,
            content: None,
            preview: None,
            marked: BTreeMap::new(),
            export_format: ExportFormat::MacArchive,
            fork_mode: ResourceForkMode::AppleDouble,
            error: None,
            result: None,
            extracting: None,
            exporting: None,
            tree_job: None,
            tree_text: None,
            saving: None,
        }
    }

    /// The browser is showing a disc (or why it could not open one).
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// The open is running.
    pub fn opening(&self) -> Option<Snapshot> {
        self.opening.as_ref().map(|j| j.snapshot())
    }

    /// Picker labels: the primary filesystem, the hybrid sides, the boot areas.
    pub fn fs_choices(&self) -> &[String] {
        self.labels.as_slice()
    }

    pub fn selected_fs(&self) -> usize {
        self.disc.as_ref().map_or(0, |d| d.selected)
    }

    /// "Game: ..." for a recognised console disc.
    pub fn game(&self) -> Option<&str> {
        self.game.as_deref()
    }

    pub fn root(&self) -> Option<&FileEntry> {
        self.root.as_ref()
    }

    /// A folder's entries once listed; `None` shows "Loading...".
    pub fn children(&self, path: &str) -> Option<&[FileEntry]> {
        self.dirs.get(path).map(|v| v.as_slice())
    }

    pub fn selected(&self) -> Option<&FileEntry> {
        self.selected.as_ref()
    }

    /// The selected file's preview, once read.
    pub fn content(&self) -> Option<&FileContent> {
        self.content.as_ref()
    }

    pub fn is_marked(&self, path: &str) -> bool {
        self.marked.contains_key(path)
    }

    pub fn marked_count(&self) -> usize {
        self.marked.len()
    }

    pub fn export_format(&self) -> ExportFormat {
        self.export_format
    }

    pub fn fork_mode(&self) -> ResourceForkMode {
        self.fork_mode
    }

    /// The browsed filesystem is HFS or HFS+: the resource-fork control applies.
    pub fn is_hfs(&self) -> bool {
        use opticaldiscs::formats::FilesystemType;
        matches!(
            self.disc.as_ref().and_then(|d| d.fs_type()),
            Some(FilesystemType::Hfs | FilesystemType::HfsPlus)
        )
    }

    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// The last extraction or export's line.
    pub fn result(&self) -> Option<&str> {
        self.result.as_deref()
    }

    /// The extraction, export, tree build or folder read running, for the job widget.
    pub fn working(&self) -> Option<Snapshot> {
        if let Some(job) = &self.extracting {
            return Some(job.snapshot());
        }
        if let Some(job) = &self.exporting {
            return Some(job.snapshot());
        }
        if let Some(job) = &self.tree_job {
            return Some(job.snapshot());
        }
        self.switching.as_ref().map(|j| j.snapshot())
    }

    /// An extraction or export runs: the extract controls are hidden.
    pub fn is_extracting(&self) -> bool {
        self.extracting.is_some() || self.exporting.is_some()
    }

    /// The tree window's text, while it is open.
    pub fn tree_text(&self) -> Option<&str> {
        self.tree_text.as_deref()
    }

    pub(super) fn open(&mut self, src: DiscSource) {
        self.close();
        self.active = true;
        let notify = Arc::clone(&self.notify);
        self.opening = Some(Job::spawn("Opening the disc...", notify, move |ctx| {
            ctx.set_can_cancel(false);
            let opened = ob::open_disc(&src);
            Ok((src, opened))
        }));
    }

    pub(super) fn close(&mut self) {
        if let Some(job) = &self.extracting {
            job.cancel();
        }
        let notify = Arc::clone(&self.notify);
        *self = DiscBrowseController::new(notify);
    }

    fn take_root(&mut self, read: RootRead) {
        if let (Some(root), Some(entries)) = (&read.root, read.entries) {
            self.dirs.insert(root.path.clone(), entries);
        }
        self.root = read.root;
        if read.error.is_some() {
            self.error = read.error;
        }
    }

    pub(super) fn tick(&mut self) {
        if let Some(done) = self.opening.as_ref().and_then(job_outcome) {
            self.opening = None;
            match done {
                Ok((src, opened)) => {
                    let OpenedDisc {
                        info,
                        boot_areas,
                        root,
                    } = opened;
                    if let Some(info) = info {
                        self.labels = ob::fs_choice_labels(&info, boot_areas.as_slice());
                        self.game = info.game.as_ref().map(crate::optical::format_game_identity);
                        self.disc = Some(DiscChoice {
                            src,
                            info: Arc::new(info),
                            selected: 0,
                            boot_areas,
                        });
                    }
                    self.take_root(root);
                }
                Err(e) => self.error = Some(e),
            }
        }
        if let Some(done) = self.switching.as_ref().and_then(job_outcome) {
            self.switching = None;
            match done {
                Ok(read) => self.take_root(read),
                Err(e) => self.error = Some(e),
            }
        }
        let mut i = 0;
        while i < self.listing.len() {
            match job_outcome(&self.listing[i].1) {
                None => i += 1,
                Some(done) => {
                    let (path, _) = self.listing.remove(i);
                    match done.and_then(|r| r) {
                        Ok(entries) => {
                            self.dirs.insert(path, entries);
                        }
                        Err(e) => {
                            self.failed.insert(path);
                            self.error = Some(e);
                        }
                    }
                }
            }
        }
        if let Some(done) = self.preview.as_ref().and_then(|(_, j)| job_outcome(j)) {
            self.preview = None;
            match done.and_then(|r| r) {
                Ok(content) => self.content = Some(content),
                Err(e) => self.error = Some(e),
            }
        }
        if let Some(done) = self.extracting.as_ref().and_then(job_outcome) {
            self.extracting = None;
            self.result = Some(match done.and_then(|r| r) {
                Ok(n) => format!("Extraction complete: {n} files extracted."),
                Err(e) => format!("Extraction failed: {e}"),
            });
        }
        if let Some(done) = self.exporting.as_ref().and_then(job_outcome) {
            self.exporting = None;
            match done.and_then(|r| r) {
                Ok((n, path)) => {
                    self.result = Some(format!("Exported {n} item(s) to {}", path.display()))
                }
                Err(e) => self.error = Some(format!("Export failed: {e}")),
            }
        }
        if let Some(done) = self.tree_job.as_ref().and_then(job_outcome) {
            self.tree_job = None;
            match done.and_then(|r| r) {
                Ok(text) => self.tree_text = Some(text),
                Err(e) => self.error = Some(e),
            }
        }
        if let Some(done) = self.saving.as_ref().and_then(job_outcome) {
            self.saving = None;
            if let Err(e) = done.and_then(|r| r) {
                self.error = Some(format!("Failed to save tree: {e}"));
            }
        }
    }

    fn spawn<R: Send + 'static>(
        &self,
        label: &str,
        body: impl FnOnce(&crate::model::job::JobCtx) -> R + Send + 'static,
    ) -> Job<R> {
        Job::spawn(label.to_string(), Arc::clone(&self.notify), move |ctx| {
            Ok(body(ctx))
        })
    }

    fn select_fs(&mut self, idx: usize) {
        let Some(disc) = self.disc.as_mut() else {
            return;
        };
        if idx == disc.selected || idx >= self.labels.len() {
            return;
        }
        disc.selected = idx;
        let choice = disc.clone();
        self.dirs.clear();
        self.listing.clear();
        self.failed.clear();
        self.root = None;
        self.selected = None;
        self.marked.clear();
        self.content = None;
        self.preview = None;
        self.error = None;
        self.switching = Some(self.spawn("Reading the filesystem...", move |ctx| {
            ctx.set_can_cancel(false);
            choice.read_root()
        }));
    }

    fn expand(&mut self, entry: FileEntry) {
        let Some(disc) = self.disc.clone() else {
            return;
        };
        let path = entry.path.clone();
        if self.dirs.contains_key(&path)
            || self.failed.contains(&path)
            || self.listing.iter().any(|(p, _)| *p == path)
        {
            return;
        }
        let job = self.spawn("Reading the folder...", move |ctx| {
            ctx.set_can_cancel(false);
            disc.list_dir(&entry)
        });
        self.listing.push((path, job));
    }

    fn select(&mut self, entry: FileEntry) {
        self.content = None;
        self.preview = None;
        self.error = None;
        let read = entry.is_file() && entry.size <= MAX_PREVIEW_SIZE as u64;
        self.selected = Some(entry.clone());
        if !read {
            return;
        }
        let Some(disc) = self.disc.clone() else {
            return;
        };
        let path = entry.path.clone();
        let job = self.spawn("Reading the file...", move |ctx| {
            ctx.set_can_cancel(false);
            disc.preview(&entry)
        });
        self.preview = Some((path, job));
    }

    fn export_pick(&mut self) -> Vec<Effect> {
        if self.disc.is_none() || self.is_extracting() {
            return Vec::new();
        }
        if ob::marked_export_entries(&self.marked).is_empty() {
            self.error = Some("Nothing selected to export.".into());
            return Vec::new();
        }
        let format = self.export_format;
        if format.is_single_file() {
            let ext = format
                .file_extension()
                .map(|e| format!(".{e}"))
                .unwrap_or_default();
            vec![Effect::PickFile(PickSpec {
                purpose: purpose::EXPORT_FILE,
                title: "Export selection".to_string(),
                filters: Vec::new(),
                save: true,
                file_name: format!("selection{ext}"),
            })]
        } else {
            vec![Effect::PickFolder(PickSpec {
                purpose: purpose::EXPORT_FOLDER,
                title: "Export selection to folder".to_string(),
                filters: Vec::new(),
                save: false,
                file_name: String::new(),
            })]
        }
    }

    fn start_export(&mut self, dest: PathBuf) {
        let Some(disc) = self.disc.clone() else {
            return;
        };
        let entries = ob::marked_export_entries(&self.marked);
        let (format, mode) = (self.export_format, self.fork_mode);
        self.error = None;
        self.exporting = Some(self.spawn("Exporting the selection...", move |ctx| {
            ctx.set_can_cancel(false);
            disc.export(entries.as_slice(), dest.as_path(), format, mode)
                .map(|n| (n, dest))
        }));
    }

    fn start_extract(&mut self, dest: PathBuf) {
        let (Some(disc), Some(entry)) = (self.disc.clone(), self.selected.clone()) else {
            return;
        };
        let mode = self.fork_mode;
        self.result = None;
        self.extracting = Some(self.spawn("Extracting", move |ctx| {
            let _wake = crate::os::wakelock::acquire("Rusty Backup: extract optical disc files");
            disc.extract(&entry, dest.as_path(), mode, ctx)
        }));
    }

    fn start_tree(&mut self) {
        let Some(disc) = self.disc.clone() else {
            return;
        };
        if self.tree_job.is_some() {
            return;
        }
        self.tree_job = Some(self.spawn("Building the tree listing...", move |ctx| {
            ctx.set_can_cancel(false);
            disc.tree_listing()
        }));
    }

    pub(super) fn apply(&mut self, action: DiscBrowseAction) -> Vec<Effect> {
        if !self.active {
            return Vec::new();
        }
        match action {
            DiscBrowseAction::SelectFs(idx) => self.select_fs(idx),
            DiscBrowseAction::Expand(entry) => self.expand(entry),
            DiscBrowseAction::Select(entry) => self.select(entry),
            DiscBrowseAction::SetMarked { entry, marked } => {
                if marked {
                    self.marked.insert(entry.path.clone(), entry);
                } else {
                    self.marked.remove(&entry.path);
                }
            }
            DiscBrowseAction::ClearMarks => self.marked.clear(),
            DiscBrowseAction::SetExportFormat(f) => self.export_format = f,
            DiscBrowseAction::Export => return self.export_pick(),
            DiscBrowseAction::SetForkMode(m) => self.fork_mode = m,
            DiscBrowseAction::Extract => {
                if self.selected.is_some() && !self.is_extracting() {
                    return vec![Effect::PickFolder(PickSpec {
                        purpose: purpose::EXTRACT_TO,
                        title: "Extract to folder".to_string(),
                        filters: Vec::new(),
                        save: false,
                        file_name: String::new(),
                    })];
                }
            }
            DiscBrowseAction::CancelExtract => {
                if let Some(job) = &self.extracting {
                    job.cancel();
                }
            }
            DiscBrowseAction::Tree => self.start_tree(),
            DiscBrowseAction::CloseTree => self.tree_text = None,
            DiscBrowseAction::CopyTree => {
                if let Some(text) = &self.tree_text {
                    return vec![Effect::CopyToClipboard(text.clone())];
                }
            }
            DiscBrowseAction::SaveTree => {
                if self.tree_text.is_some() {
                    return vec![Effect::PickFile(PickSpec {
                        purpose: purpose::SAVE_TREE,
                        title: "Save tree view".to_string(),
                        filters: vec![("Text files".to_string(), vec!["txt".to_string()])],
                        save: true,
                        file_name: "tree.txt".to_string(),
                    })];
                }
            }
            DiscBrowseAction::Picked { purpose, path } => {
                let Some(path) = path else {
                    return Vec::new();
                };
                match purpose {
                    purpose::EXTRACT_TO if !self.is_extracting() => self.start_extract(path),
                    purpose::EXPORT_FILE | purpose::EXPORT_FOLDER if !self.is_extracting() => {
                        self.start_export(path)
                    }
                    purpose::SAVE_TREE => {
                        if let Some(text) = self.tree_text.clone() {
                            self.saving =
                                Some(self.spawn("Saving the tree listing...", move |_| {
                                    std::fs::write(&path, text.as_bytes())
                                        .map_err(|e| e.to_string())
                                }));
                        }
                    }
                    _ => {}
                }
            }
            DiscBrowseAction::Close => self.close(),
        }
        Vec::new()
    }

    /// Block until every job ends; for a headless shell or a test.
    pub fn wait_idle(&mut self) {
        loop {
            self.tick();
            if self.opening.is_none()
                && self.switching.is_none()
                && self.listing.is_empty()
                && self.preview.is_none()
                && self.extracting.is_none()
                && self.exporting.is_none()
                && self.tree_job.is_none()
                && self.saving.is_none()
            {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}
