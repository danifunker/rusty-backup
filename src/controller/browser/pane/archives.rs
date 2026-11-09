//! Archives at the pane's edge: the Mac archive viewer and its extract, saving a Mac archive as
//! stored or decoded, and importing a picked or dropped archive.
//!
//! `ReadArchive` lands in [`ArchiveViewer`] (Browse) or [`ArchiveSave`] (Extract). `AddPicked`
//! classifies host picks in a job: plain items are staged, a tar waits for `ChooseTar`, and on a
//! Mac volume a Mac archive waits for `ChooseImport` (expanded or unwrapped into a temp folder in a
//! job, then staged; the folder lives while anything is staged). A tar's contents are imported
//! straight into the volume (not staged) after a preflight that asks when content would be lost.
//! What a job ended with waits in `take_archive_notice`.

use std::collections::VecDeque;
use std::path::PathBuf;

use super::source::purpose;
use super::volume::{ArchiveRead, ArchiveUse, MacArchiveKind};
use super::BrowserPaneController;
use crate::controller::{Effect, PickSpec};
use crate::model::archive_import::{self, ArchiveRow, HostPick, TarPreview, TarStep};
use crate::model::job::{Job, Resource, Snapshot, Status};
use arc_mac::stuffit::StuffItArchive;
use disk_core::entry::FileEntry;

pub use arc_mac::extract::ForkFormat;

/// How to bring in a picked Mac archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportChoice {
    /// Decode it (and anything it wraps) and stage what it holds.
    Expand,
    /// Strip only the BinHex wrapper and stage the inner archive.
    ConvertOnly,
    /// Stage the file unchanged.
    AsIs,
    Cancel,
}

/// The words of the "what to do with this archive" question; one per archive kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportPrompt {
    pub title: &'static str,
    /// "NAME is a KIND archive."
    pub intro: String,
    pub question: String,
    /// The `Expand` button's label.
    pub expand: &'static str,
    /// The `ConvertOnly` button's label, offered only for BinHex over an archive.
    pub convert_only: Option<&'static str>,
}

/// The import question for the archive `name` of `kind`; every shell shows these words.
pub fn import_prompt(name: &str, kind: MacArchiveKind) -> ImportPrompt {
    use MacArchiveKind as K;
    // A single BinHex converts; an archive expands; BinHex over an archive does either.
    let (title, question, expand) = match kind {
        K::BinHexSingleFile => (
            "Convert HQX file?",
            format!("Would you like to convert {name} to binary?"),
            "Convert",
        ),
        K::BinHexOverSit | K::BinHexOverSea | K::BinHexOverCompactPro => (
            "Convert and/or expand?",
            format!("Would you like to convert {name} to binary and/or expand the contents?"),
            "Convert and expand",
        ),
        K::Sit
        | K::Sit5
        | K::Sea
        | K::CompactPro
        | K::Mar
        | K::MacBinary
        | K::MacZip
        | K::InstallerTome => (
            "Expand archive?",
            format!("Would you like to expand the contents of {name}?"),
            "Expand",
        ),
    };
    let convert_only = match kind {
        K::BinHexOverSea => Some("Convert only (.sea lands here)"),
        K::BinHexOverCompactPro => Some("Convert only (inner archive lands here)"),
        K::BinHexOverSit => Some("Convert only (.sit lands here)"),
        _ => None,
    };
    ImportPrompt {
        title,
        intro: {
            let label = kind.label();
            let an = matches!(label.chars().next(), Some('A' | 'E' | 'I' | 'O' | 'U'));
            format!("{name} is {} {label} archive.", if an { "an" } else { "a" })
        },
        question,
        expand,
        convert_only,
    }
}

/// How to bring in a picked tar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TarChoice {
    /// Import its contents into the folder.
    Import,
    AsFile,
    Cancel,
}

/// The read-only archive viewer: entries, which are marked, and the fork container to extract into.
#[derive(Debug, Clone)]
pub struct ArchiveViewer {
    pub entry_name: String,
    pub kind: MacArchiveKind,
    pub rows: Vec<ArchiveRow>,
    pub marked: Vec<bool>,
    pub format: ForkFormat,
    bytes: Vec<u8>,
    archive: StuffItArchive,
}

impl ArchiveViewer {
    pub fn file_count(&self) -> usize {
        self.rows.iter().filter(|r| !r.is_dir).count()
    }

    pub fn marked_files(&self) -> usize {
        let marked = |(i, r): &(usize, &ArchiveRow)| !r.is_dir && self.marked[*i];
        self.rows.iter().enumerate().filter(marked).count()
    }
}

/// A Mac archive on the volume to save as it is stored, or decoded into a fork container.
#[derive(Debug, Clone)]
pub struct ArchiveSave {
    pub entry_name: String,
    pub kind: MacArchiveKind,
    pub format: ForkFormat,
    raw: Vec<u8>,
    bytes: Vec<u8>,
    archive: StuffItArchive,
}

/// A picked Mac archive waiting for its `ImportChoice`.
struct PendingImport {
    path: PathBuf,
    folder: String,
    kind: MacArchiveKind,
    raw: Vec<u8>,
}

/// Decoding a picked Mac archive into the temp folder: what to stage.
type Decode = Box<dyn FnOnce() -> Result<Vec<PathBuf>, String> + Send>;

enum ArchiveJob {
    Classify {
        folder: String,
        job: Job<Vec<HostPick>>,
    },
    /// Decoding an import into the temp folder; what to stage lands.
    Import {
        folder: String,
        name: String,
        said: String,
        job: Job<Result<Vec<PathBuf>, String>>,
    },
    /// A tar's preflight or import into this folder.
    Tar(String, Job<TarStep>),
    /// Writing to the host (an extract or a save): the status line.
    Host(Job<Result<String, String>>),
}

#[derive(Default)]
pub(super) struct ArchiveState {
    imports: VecDeque<PendingImport>,
    /// A picked tar and the folder it goes to, waiting for `ChooseTar`.
    tar: Option<(PathBuf, String)>,
    tar_preview: Option<(TarPreview, String)>,
    job: Option<ArchiveJob>,
    viewer: Option<ArchiveViewer>,
    save: Option<ArchiveSave>,
    /// The viewer's extract waiting for its folder: all entries, or the marked ones.
    extract_all: bool,
    /// Decoded imports, kept until nothing is staged.
    temp: Option<tempfile::TempDir>,
    imported: usize,
    notice: Option<String>,
}

fn is_mac_fs(fs_type: &str) -> bool {
    matches!(fs_type, "HFS" | "HFS+" | "HFSX")
}

fn file_name(path: &std::path::Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

fn msg(text: impl Into<String>) -> Vec<Effect> {
    vec![Effect::Message(text.into())]
}

impl BrowserPaneController {
    // --- read accessors ---------------------------------------------------

    /// The Mac archive waiting for `ChooseImport`: its file name and kind.
    pub fn pending_import(&self) -> Option<(String, MacArchiveKind)> {
        let p = self.arc.imports.front()?;
        Some((file_name(&p.path), p.kind))
    }

    /// The tar waiting for `ChooseTar`: its file name.
    pub fn pending_tar(&self) -> Option<String> {
        self.arc.tar.as_ref().map(|(p, _)| file_name(p))
    }

    /// A tar import whose preflight found content the volume cannot keep, for `ConfirmTarImport`.
    pub fn tar_preview(&self) -> Option<&TarPreview> {
        self.arc.tar_preview.as_ref().map(|(p, _)| p)
    }

    pub fn archive_viewer(&self) -> Option<&ArchiveViewer> {
        self.arc.viewer.as_ref()
    }

    pub fn archive_save(&self) -> Option<&ArchiveSave> {
        self.arc.save.as_ref()
    }

    /// The archive job in progress (classifying picks, decoding, a tar import, writing to the host).
    pub fn archive_working(&self) -> Option<Snapshot> {
        Some(match self.arc.job.as_ref()? {
            ArchiveJob::Classify { job, .. } => job.snapshot(),
            ArchiveJob::Import { job, .. } => job.snapshot(),
            ArchiveJob::Tar(_, job) => job.snapshot(),
            ArchiveJob::Host(job) => job.snapshot(),
        })
    }

    /// What the last archive job ended with, handed over once.
    pub fn take_archive_notice(&mut self) -> Option<String> {
        self.arc.notice.take()
    }

    pub(super) fn archive_busy(&self) -> bool {
        matches!(self.arc.job, Some(ArchiveJob::Tar(..)))
    }

    // --- actions ----------------------------------------------------------

    /// Handles an archive action, or hands it back.
    pub(super) fn apply_archive(
        &mut self,
        action: super::BrowserPaneAction,
    ) -> Result<Vec<Effect>, super::BrowserPaneAction> {
        use super::BrowserPaneAction as A;
        let fx = match action {
            A::AddPicked(paths) => self.add_picked(paths),
            A::ChooseImport(choice) => self.choose_import(choice),
            A::ChooseTar(choice) => self.choose_tar(choice),
            A::ImportTar => self.pick_tar(),
            A::Picked { purpose, path } if purpose == purpose::IMPORT_TAR => match path {
                Some(p) => self.start_tar(p, None, true),
                None => Vec::new(),
            },
            A::ConfirmTarImport(yes) => match self.arc.tar_preview.take() {
                Some((pf, folder)) if yes => self.start_tar(pf.archive, Some(folder), false),
                Some(_) => {
                    self.arc.notice = Some("Archive import cancelled.".into());
                    Vec::new()
                }
                None => Vec::new(),
            },
            A::ArchiveMark(i) => {
                if let Some(slot) = self.arc.viewer.as_mut().and_then(|v| v.marked.get_mut(i)) {
                    *slot = !*slot;
                }
                Vec::new()
            }
            A::ArchiveFormat { viewer, format } => {
                match viewer {
                    true => self.arc.viewer.iter_mut().for_each(|v| v.format = format),
                    false => self.arc.save.iter_mut().for_each(|s| s.format = format),
                }
                Vec::new()
            }
            A::ArchiveExtract { all } => self.ask_viewer_folder(all),
            A::Picked { purpose, path } if purpose == purpose::ARCHIVE_EXTRACT => match path {
                Some(dest) => self.extract_viewer(dest),
                None => Vec::new(),
            },
            A::CloseArchive => {
                self.arc.viewer = None;
                Vec::new()
            }
            A::SaveArchive { raw } => self.ask_save(raw),
            A::Picked { purpose, path } if purpose == purpose::ARCHIVE_SAVE_RAW => match path {
                Some(dest) => self.save_archive(dest, true),
                None => Vec::new(),
            },
            A::Picked { purpose, path } if purpose == purpose::ARCHIVE_SAVE_DECODED => match path {
                Some(dest) => self.save_archive(dest, false),
                None => Vec::new(),
            },
            A::CancelSaveArchive => {
                self.arc.save = None;
                Vec::new()
            }
            other => return Err(other),
        };
        Ok(fx)
    }

    /// A decoded archive from `ReadArchive`: the viewer or the save dialog opens on it.
    pub(super) fn found_archive(
        &mut self,
        entry: FileEntry,
        purpose: ArchiveUse,
        read: Result<ArchiveRead, String>,
    ) -> Vec<Effect> {
        let read = match read {
            Ok(r) => r,
            Err(e) => return msg(e),
        };
        match purpose {
            ArchiveUse::Browse => {
                let rows = archive_import::archive_rows(&read.archive);
                self.arc.viewer = Some(ArchiveViewer {
                    entry_name: entry.name,
                    kind: read.kind,
                    marked: vec![true; rows.len()],
                    rows,
                    format: ForkFormat::BinHex,
                    bytes: read.bytes,
                    archive: read.archive,
                });
            }
            ArchiveUse::Extract => {
                self.arc.save = Some(ArchiveSave {
                    entry_name: entry.name,
                    kind: read.kind,
                    format: ForkFormat::BinHex,
                    raw: read.raw,
                    bytes: read.bytes,
                    archive: read.archive,
                });
            }
        }
        Vec::new()
    }

    fn job_free(&self) -> Result<(), Vec<Effect>> {
        match self.arc.job.is_some() {
            true => Err(msg("wait for the archive job to finish.")),
            false => Ok(()),
        }
    }

    fn add_picked(&mut self, paths: Vec<PathBuf>) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume().and_then(|_| self.job_free()) {
            return fx;
        }
        let Some(folder) = self.target_folder().map(|e| e.path) else {
            return msg("no folder open.");
        };
        let mac = is_mac_fs(&self.volume().fs_type);
        let (jobs, notify) = self.job_services();
        let spawned = jobs.spawn("Reading the picked files", Vec::new(), notify, move |_| {
            Ok(paths
                .iter()
                .map(|p| archive_import::classify(p, mac))
                .collect())
        });
        match spawned {
            Ok(job) => {
                self.arc.job = Some(ArchiveJob::Classify { folder, job });
                Vec::new()
            }
            Err(busy) => msg(format!("cannot add: {busy}")),
        }
    }

    /// Stage the plain picks; tars and Mac archives wait for the user's choice.
    fn classified(&mut self, folder: String, picks: Vec<HostPick>) -> Vec<Effect> {
        let mut plain = Vec::new();
        for pick in picks {
            match pick {
                HostPick::Plain(p) => plain.push(p),
                HostPick::Tar(p) => self.arc.tar = Some((p, folder.clone())),
                HostPick::Mac { path, kind, raw } => self.arc.imports.push_back(PendingImport {
                    path,
                    folder: folder.clone(),
                    kind,
                    raw,
                }),
            }
        }
        match plain.is_empty() {
            true => Vec::new(),
            false => self.add_host_paths(&plain, Some(&folder)),
        }
    }

    fn choose_import(&mut self, choice: ImportChoice) -> Vec<Effect> {
        if choice != ImportChoice::Cancel {
            if let Err(fx) = self.writable_volume().and_then(|_| self.job_free()) {
                return fx;
            }
        }
        let Some(p) = self.arc.imports.pop_front() else {
            return Vec::new();
        };
        let name = file_name(&p.path);
        if choice == ImportChoice::AsIs {
            self.arc.notice = Some(format!("Imported (as-is) {name}"));
            return self.add_host_paths(&[p.path], Some(&p.folder));
        }
        if choice == ImportChoice::Cancel {
            return Vec::new();
        }
        if self.arc.temp.is_none() {
            self.arc.temp = tempfile::Builder::new()
                .prefix("rusty-archive-import-")
                .tempdir()
                .ok();
        }
        let Some(root) = self.arc.temp.as_ref().map(|t| t.path().to_path_buf()) else {
            return msg("cannot import: no temporary folder.");
        };
        self.arc.imported += 1;
        let dir = root.join(format!("import-{}", self.arc.imported));
        let raw = p.raw;
        let label = format!("Decoding {name}");
        let (said, body): (String, Decode) = match choice {
            ImportChoice::ConvertOnly => (
                format!("Imported inner archive from {name}"),
                Box::new(move || archive_import::unwrap_binhex(&raw, &dir).map(|p| vec![p])),
            ),
            _ => (
                format!("Imported contents of {name}"),
                Box::new(move || archive_import::expand_mac_archive(raw, &dir)),
            ),
        };
        let (jobs, notify) = self.job_services();
        let spawned = jobs.spawn(label, Vec::new(), notify, move |_| Ok(body()));
        match spawned {
            Ok(job) => {
                let folder = p.folder;
                self.arc.job = Some(ArchiveJob::Import {
                    folder,
                    name,
                    said,
                    job,
                });
                Vec::new()
            }
            Err(busy) => msg(format!("cannot import: {busy}")),
        }
    }

    fn choose_tar(&mut self, choice: TarChoice) -> Vec<Effect> {
        let Some((path, folder)) = self.arc.tar.take() else {
            return Vec::new();
        };
        match choice {
            TarChoice::Import => self.start_tar(path, Some(folder), true),
            TarChoice::AsFile => self.add_host_paths(&[path], Some(&folder)),
            TarChoice::Cancel => Vec::new(),
        }
    }

    fn pick_tar(&mut self) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        let exts = ["gz", "tgz", "tar", "zst"].iter().map(|e| e.to_string());
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::IMPORT_TAR,
            title: "Import a tar archive into this volume".into(),
            filters: vec![("tar archive".into(), exts.collect())],
            save: false,
            file_name: String::new(),
        })]
    }

    /// Import `archive` into `folder` (the target folder when `None`) in a job; `check` preflights first.
    fn start_tar(&mut self, archive: PathBuf, folder: Option<String>, check: bool) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume().and_then(|_| self.job_free()) {
            return fx;
        }
        let dest = match &folder {
            Some(path) => self.entry_at(path).filter(|e| e.is_directory()),
            None => self.target_folder(),
        };
        let (Some(dest), Some(session)) = (dest, self.session().cloned()) else {
            return msg("no folder open.");
        };
        let folder = dest.path.clone();
        let resources = self.apply_resources();
        let (jobs, notify) = self.job_services();
        let spawned = jobs.spawn("Importing the archive...", resources, notify, move |job| {
            job.set_can_cancel(false);
            Ok(archive_import::tar_import_step(
                &session, &dest, archive, check, job,
            ))
        });
        match spawned {
            Ok(job) => {
                self.arc.job = Some(ArchiveJob::Tar(folder, job));
                Vec::new()
            }
            Err(busy) => msg(format!("cannot import: {busy}")),
        }
    }

    fn ask_viewer_folder(&mut self, all: bool) -> Vec<Effect> {
        if self.arc.viewer.is_none() {
            return Vec::new();
        }
        self.arc.extract_all = all;
        vec![Effect::PickFolder(PickSpec {
            purpose: purpose::ARCHIVE_EXTRACT,
            title: "Extract archive contents to folder".into(),
            filters: Vec::new(),
            save: false,
            file_name: String::new(),
        })]
    }

    fn extract_viewer(&mut self, dest: PathBuf) -> Vec<Effect> {
        let Some(v) = self.arc.viewer.as_ref() else {
            return Vec::new();
        };
        let keep = (!self.arc.extract_all).then(|| v.marked.clone());
        let (bytes, archive, format) = (v.bytes.clone(), v.archive.clone(), v.format);
        self.spawn_host_write("Extracting the archive", dest.clone(), move || {
            archive_import::extract_to_host(&bytes, &archive, &dest, format, keep.as_deref())
        })
    }

    fn ask_save(&mut self, raw: bool) -> Vec<Effect> {
        let Some(s) = self.arc.save.as_ref() else {
            return Vec::new();
        };
        let spec = |purpose, title: &str, file_name: String| PickSpec {
            purpose,
            title: title.into(),
            filters: Vec::new(),
            save: raw,
            file_name,
        };
        match raw {
            true => vec![Effect::PickFile(spec(
                purpose::ARCHIVE_SAVE_RAW,
                "Save archive as-is",
                s.entry_name.clone(),
            ))],
            false => vec![Effect::PickFolder(spec(
                purpose::ARCHIVE_SAVE_DECODED,
                "Save decoded contents to folder",
                String::new(),
            ))],
        }
    }

    fn save_archive(&mut self, dest: PathBuf, raw: bool) -> Vec<Effect> {
        let Some(s) = self.arc.save.take() else {
            return Vec::new();
        };
        if raw {
            let folder = dest.parent().map(|p| p.to_path_buf()).unwrap_or_default();
            return self.spawn_host_write("Saving the archive", folder, move || {
                let n = s.raw.len() as u64;
                match std::fs::write(&dest, &s.raw) {
                    Ok(()) => Ok(format!(
                        "Wrote {} ({})",
                        dest.display(),
                        disk_core::display::format_size(n)
                    )),
                    Err(e) => Err(format!("Write failed: {e}")),
                }
            });
        }
        self.spawn_host_write("Extracting the archive", dest.clone(), move || {
            archive_import::extract_to_host(&s.bytes, &s.archive, &dest, s.format, None)
        })
    }

    pub(super) fn spawn_host_write(
        &mut self,
        label: &str,
        folder: PathBuf,
        body: impl FnOnce() -> Result<String, String> + Send + 'static,
    ) -> Vec<Effect> {
        if let Err(fx) = self.job_free() {
            return fx;
        }
        let (jobs, notify) = self.job_services();
        let resources = vec![Resource::Folder(folder)];
        match jobs.spawn(label, resources, notify, move |_| Ok(body())) {
            Ok(job) => {
                self.arc.job = Some(ArchiveJob::Host(job));
                Vec::new()
            }
            Err(busy) => msg(format!("cannot write there: {busy}")),
        }
    }

    /// Land a finished archive job; called from `tick`.
    pub(super) fn poll_archives(&mut self) -> Vec<Effect> {
        let done = match &self.arc.job {
            Some(ArchiveJob::Classify { job, .. }) => !job.is_running(),
            Some(ArchiveJob::Import { job, .. }) => !job.is_running(),
            Some(ArchiveJob::Tar(_, job)) => !job.is_running(),
            Some(ArchiveJob::Host(job)) => !job.is_running(),
            None => {
                self.drop_import_temp();
                return Vec::new();
            }
        };
        if !done {
            return Vec::new();
        }
        let failed = |status: Status| match status {
            Status::Failed(e) => Some(e),
            _ => None,
        };
        match self.arc.job.take() {
            Some(ArchiveJob::Classify { folder, job }) => {
                let status = job.snapshot().status;
                match job.take_result() {
                    Some(picks) => self.classified(folder, picks),
                    None => msg(failed(status).unwrap_or_else(|| "stopped.".into())),
                }
            }
            Some(ArchiveJob::Import {
                folder,
                name,
                said,
                job,
            }) => {
                let status = job.snapshot().status;
                let out = job.take_result();
                let out = out.unwrap_or_else(|| Err(failed(status).unwrap_or_default()));
                match out {
                    Ok(paths) => {
                        self.arc.notice = Some(said);
                        self.add_host_paths(&paths, Some(&folder))
                    }
                    Err(e) => {
                        self.arc.notice = Some(format!("Failed to import {name}: {e}"));
                        self.push_staging_error(name, e);
                        Vec::new()
                    }
                }
            }
            Some(ArchiveJob::Tar(folder, job)) => {
                let status = job.snapshot().status;
                match job.take_result() {
                    Some(TarStep::Confirm(pf)) => {
                        self.arc.tar_preview = Some((pf, folder));
                        Vec::new()
                    }
                    Some(TarStep::Finished(said, wrote)) => {
                        self.arc.notice = Some(said);
                        match wrote {
                            true => self.reopen_after_apply(),
                            false => Vec::new(),
                        }
                    }
                    None => {
                        let why = failed(status).unwrap_or_else(|| "stopped".into());
                        self.arc.notice = Some(format!("Import failed: {why}"));
                        Vec::new()
                    }
                }
            }
            Some(ArchiveJob::Host(job)) => {
                let status = job.snapshot().status;
                let out = job.take_result();
                let said = match out {
                    Some(Ok(s)) | Some(Err(s)) => s,
                    None => failed(status).unwrap_or_else(|| "stopped.".into()),
                };
                self.arc.notice = Some(said);
                Vec::new()
            }
            None => Vec::new(),
        }
    }

    /// The decoded imports were written or dropped once nothing is staged.
    fn drop_import_temp(&mut self) {
        if self.queue.is_empty() && self.arc.imports.is_empty() && !self.is_busy() {
            self.arc.temp = None;
        }
    }
}
