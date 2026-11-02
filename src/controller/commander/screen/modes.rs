//! The Inspect browse view's source-level modes, on the one-pane screen.
//!
//! The host (the Inspect tab) hands over a session with [`CommanderController::open_inspect`] and
//! says how it may be edited: in place (a raw image or device, or a CHD through `chd_edit`), or
//! through the archive flow (a zstd or WOZ backup decompressed to a temp file, edited, then
//! recompressed). Edit Mode decides how the session opens for writing and hands the pane the
//! resulting session: a CHD is backed up and opened over a diff, an archive is decompressed, a
//! SquashFS asks for a size budget first, anything else is probed with an editable open. Leaving
//! Edit Mode flattens the CHD diff or recompresses the archive. Each of these is a job.
//!
//! The modes also own the unsaved-edits question (Discard / Apply / Cancel) with the Apply that
//! leaves Edit Mode or closes, the free-space readout, CHD Info, the carve view's full scan, Show
//! deleted, the seekable-cache upgrade, and "Open as Disk Image" (an extract into a temp folder
//! with room checked first, handed to the host by `take_nested_open`).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::{CommanderController, Side};
use crate::controller::browser::purpose as pane_purpose;
use crate::controller::browser::BrowserPaneAction;
use crate::controller::Effect;
use crate::model::archive_edit::{self, ArchiveEditContext};
use crate::model::browse_session::BrowseSession;
use crate::model::extract::ExtractOptions;
use crate::model::job::{Job, JobCtx, Snapshot, Status};
use fs_mac::resource_fork::ResourceForkMode;
use fs_squashfs::squashfs_edit::{SizeBudget, SizePlan};

/// Smallest file offered "Open as Disk Image": below this nothing holds a partitioned volume.
pub const MIN_NESTED_IMAGE_BYTES: u64 = 16 * 1024;
/// Free space kept spare beyond the image size when copying it out for "Open as Disk Image".
const NESTED_FREE_MARGIN: u64 = 64 * 1024 * 1024;

/// What the Inspect browse view asks of its source.
#[derive(Debug, Clone)]
pub enum ModeAction {
    /// The Edit Mode button: enter, or leave (asking first while edits wait).
    ToggleEditMode,
    /// Write the staged edits; an archive being edited is recompressed after.
    Apply,
    /// The unsaved-edits question's answer.
    Unsaved(UnsavedChoice),
    /// Close the view, after the unsaved-edits question when edits wait.
    RequestClose,
    /// The SquashFS size budget chosen before Edit Mode (`None` inside: no ceiling); `None`: cancel.
    SetBudget(Option<Option<SizeBudget>>),
    ChdInfo,
    CloseChdInfo,
    /// Copy the file at this path out and hand it to the host as a disk image.
    OpenNested(String),
    ShowDeleted(bool),
    CarveFullScan(bool),
    /// The seekable cache of a streamed backup is ready at this path.
    UpgradeSeekable(PathBuf),
    /// The host's password for a locked source, kept for every later reopen.
    Password(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsavedChoice {
    Discard,
    Apply,
    Cancel,
}

/// What follows a write once it landed without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowUp {
    /// The Apply button: an archive being edited is recompressed.
    Recompress,
    /// The unsaved-edits question: leave Edit Mode, or close.
    Leave,
}

/// Free space for the header: now, and after the staged edits when any wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpaceReadout {
    pub free: u64,
    pub after: Option<u64>,
    /// The staged edits do not fit.
    pub overflows: bool,
    /// Under a tenth of the space would be left.
    pub tight: bool,
}

struct ChdEdit {
    parent: PathBuf,
    /// The diff the writes go to; `None` for an uncompressed CHD, written in place.
    diff: Option<PathBuf>,
}

type ChdSession = Arc<Mutex<crate::rbformats::chd_edit::ChdEditSession>>;
/// An open CHD edit session and the diff its writes go to.
type ChdOpened = (ChdSession, Option<PathBuf>);

#[cfg(feature = "chd")]
fn open_chd(parent: &std::path::Path, job: &JobCtx) -> Result<ChdOpened, String> {
    let opened = crate::model::chd_edit_open::open_for_editing(parent, job)?;
    Ok((Arc::new(Mutex::new(opened.session)), opened.diff_path))
}

#[cfg(feature = "chd")]
fn flatten_chd(
    parent: &std::path::Path,
    diff: &std::path::Path,
    folder: Option<&std::path::Path>,
    job: &JobCtx,
) -> Result<String, String> {
    crate::model::chd_edit_open::flatten_in_job(parent, diff, folder, job)
}

#[cfg(feature = "chd")]
fn chd_info_text(path: &std::path::Path) -> Result<String, String> {
    crate::rbformats::chd::format_chd_info(path).map_err(|e| format!("{e}"))
}

#[cfg(not(feature = "chd"))]
fn open_chd(_: &std::path::Path, _: &JobCtx) -> Result<ChdOpened, String> {
    Err("Error: CHD editing is not built in.".into())
}

#[cfg(not(feature = "chd"))]
fn flatten_chd(
    _: &std::path::Path,
    _: &std::path::Path,
    _: Option<&std::path::Path>,
    _: &JobCtx,
) -> Result<String, String> {
    Err("Error: CHD editing is not built in.".into())
}

#[cfg(not(feature = "chd"))]
fn chd_info_text(_: &std::path::Path) -> Result<String, String> {
    Err("CHD support is not built in.".into())
}

enum ModeJob {
    ChdOpen(Job<(PathBuf, Result<ChdOpened, String>)>),
    Flatten(Job<Result<String, String>>),
    Extract(Job<Result<PathBuf, String>>),
    Compress(Job<Result<(), String>>),
    Measure(Job<Result<SizePlan, String>>),
    Probe(Job<Result<(), String>>),
}

#[derive(Default)]
pub(super) struct Modes {
    /// The host opened the single pane through `open_inspect`.
    active: bool,
    session: BrowseSession,
    label: String,
    edit_supported: bool,
    edit_mode: bool,
    archive: Option<ArchiveEditContext>,
    archive_temp: Option<PathBuf>,
    chd: Option<ChdEdit>,
    /// Edits written into a CHD diff that no flatten has committed yet.
    chd_dirty: bool,
    /// The single-file-chd backup whose metadata a flatten refreshes.
    backup_folder: Option<PathBuf>,
    job: Option<ModeJob>,
    plan: Option<SizePlan>,
    free: Option<Option<u64>>,
    free_job: Option<Job<Option<u64>>>,
    info: Option<String>,
    info_job: Option<Job<Result<String, String>>>,
    notice: Option<String>,
    error: Option<String>,
    unsaved: bool,
    pending_close: bool,
    close: bool,
    /// The Apply in flight: what follows it, the queue size, and whether it wrote.
    follow: Option<(FollowUp, usize, bool)>,
    /// The pane's write count last seen.
    writes: u64,
    /// Bumped whenever the volume is reopened or written, so a view drops what it showed.
    generation: u64,
    nested: Option<tempfile::TempDir>,
    nested_ready: Option<(PathBuf, tempfile::TempDir)>,
}

fn has_ext(session: &BrowseSession, ext: &str) -> bool {
    let e = session.source_path.as_ref().and_then(|p| p.extension());
    e.and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case(ext))
}

impl CommanderController {
    // --- host calls -------------------------------------------------------

    /// The Inspect tab hands the single pane a session; it opens read-only until Edit Mode.
    pub fn open_inspect(&mut self, session: BrowseSession, label: &str) -> Vec<Effect> {
        self.modes = Modes {
            active: true,
            label: label.to_string(),
            session,
            ..Default::default()
        };
        self.modes.writes = self.left.write_count();
        self.left.set_read_only(true);
        self.reopen_modes()
    }

    /// The open source may be edited in place (a WOZ image goes through the archive flow).
    pub fn inspect_edit_in_place(&mut self) {
        self.modes.edit_supported = true;
        let path = self.modes.session.source_path.clone();
        if let Some(p) = path.filter(|_| has_ext(&self.modes.session, "woz")) {
            self.modes.archive = Some(archive_edit::woz_context(&p));
        }
    }

    /// The open backup partition is edited by decompressing it, then recompressing.
    pub fn inspect_edit_archive(&mut self, ctx: ArchiveEditContext) {
        self.modes.archive = Some(ctx);
        self.modes.edit_supported = true;
    }

    /// The open CHD is the body of this single-file-chd backup, whose metadata a save refreshes.
    pub fn inspect_backup_folder(&mut self, folder: PathBuf) {
        self.modes.backup_folder = Some(folder);
    }

    /// Release the source: the pane's readers go first, then a CHD diff and an archive temp.
    pub fn close_inspect(mut self) {
        let mut leftovers: Vec<PathBuf> = self.modes.archive_temp.take().into_iter().collect();
        if let Some(diff) = self.modes.chd.take().and_then(|c| c.diff) {
            leftovers.push(diff);
        }
        drop(self);
        for p in leftovers {
            let _ = std::fs::remove_file(p);
        }
    }

    // --- read accessors ---------------------------------------------------

    pub fn edit_mode(&self) -> bool {
        self.modes.edit_mode
    }

    pub fn edit_supported(&self) -> bool {
        self.modes.edit_supported
    }

    pub fn is_chd_source(&self) -> bool {
        has_ext(&self.modes.session, "chd")
    }

    /// Editing a decompressed temporary copy of an archive.
    pub fn editing_archive(&self) -> bool {
        self.modes.edit_mode && self.modes.archive.is_some()
    }

    /// Editing a CHD through its diff.
    pub fn editing_chd(&self) -> bool {
        self.modes.edit_mode && self.modes.chd.is_some()
    }

    /// True while an archive flow is set up (repair is not offered on the temp copy).
    pub fn has_archive_flow(&self) -> bool {
        self.modes.archive.is_some()
    }

    /// Closing or switching the source would lose work.
    pub fn has_unsaved_edits(&self) -> bool {
        self.modes.edit_mode && (!self.left.queue.is_empty() || self.modes.chd_dirty)
    }

    /// The source-level job in progress (CHD open or flatten, decompress, recompress, measure).
    pub fn mode_working(&self) -> Option<Snapshot> {
        Some(match self.modes.job.as_ref()? {
            ModeJob::ChdOpen(j) => j.snapshot(),
            ModeJob::Flatten(j) => j.snapshot(),
            ModeJob::Extract(j) => j.snapshot(),
            ModeJob::Compress(j) => j.snapshot(),
            ModeJob::Measure(j) => j.snapshot(),
            ModeJob::Probe(j) => j.snapshot(),
        })
    }

    /// What the last mode change or write ended with, handed over once.
    pub fn take_mode_notice(&mut self) -> Option<String> {
        self.modes.notice.take()
    }

    /// Why Edit Mode could not start, handed over once.
    pub fn take_mode_error(&mut self) -> Option<String> {
        self.modes.error.take()
    }

    pub fn unsaved_shown(&self) -> bool {
        self.modes.unsaved
    }

    /// The view should close (nothing was unsaved, or the question was answered).
    pub fn take_close_request(&mut self) -> bool {
        std::mem::take(&mut self.modes.close)
    }

    /// A SquashFS's measurement for the size-budget dialog, handed over once.
    pub fn take_size_plan(&mut self) -> Option<SizePlan> {
        self.modes.plan.take()
    }

    pub fn space(&self) -> Option<SpaceReadout> {
        let free = self.modes.free.flatten()?;
        if self.left.queue.is_empty() {
            return Some(SpaceReadout {
                free,
                after: None,
                overflows: false,
                tight: false,
            });
        }
        let delta = self.left.queue.space_delta();
        let after = free.saturating_add(delta.freed).saturating_sub(delta.added);
        Some(SpaceReadout {
            free,
            after: Some(after),
            overflows: delta.added > free + delta.freed,
            tight: after < free / 10,
        })
    }

    pub fn chd_info(&self) -> Option<&str> {
        self.modes.info.as_deref()
    }

    pub fn carve_full_scan(&self) -> bool {
        crate::fs::carve::full_scan_enabled()
    }

    /// The carve view's default scan, in MiB.
    pub fn carve_scan_limit_mb(&self) -> u64 {
        crate::fs::carve::DEFAULT_SCAN_LIMIT / (1024 * 1024)
    }

    pub fn show_deleted(&self) -> bool {
        self.modes.session.show_deleted
    }

    /// Changes whenever the volume was reopened or written.
    pub fn volume_generation(&self) -> u64 {
        self.modes.generation
    }

    /// A finished "Open as Disk Image" copy: the image and the temp folder that owns it.
    pub fn take_nested_open(&mut self) -> Option<(PathBuf, tempfile::TempDir)> {
        self.modes.nested_ready.take()
    }

    // --- actions ----------------------------------------------------------

    pub(super) fn apply_mode(&mut self, action: ModeAction) -> Vec<Effect> {
        use ModeAction as M;
        match action {
            M::ToggleEditMode => self.toggle_edit_mode(),
            M::Apply => self.start_apply(FollowUp::Recompress),
            M::Unsaved(choice) => self.unsaved(choice),
            M::RequestClose => {
                if self.has_unsaved_edits() {
                    self.modes.pending_close = true;
                    self.modes.unsaved = true;
                } else {
                    self.modes.close = true;
                }
                Vec::new()
            }
            M::SetBudget(Some(budget)) => {
                self.modes.session.rebuild_budget = budget;
                self.probe_editable();
                Vec::new()
            }
            M::SetBudget(None) => Vec::new(),
            M::ChdInfo => {
                self.start_chd_info();
                Vec::new()
            }
            M::CloseChdInfo => {
                self.modes.info = None;
                Vec::new()
            }
            M::OpenNested(path) => self.open_nested(&path),
            M::ShowDeleted(on) => {
                self.modes.session.show_deleted = on;
                self.reopen_modes()
            }
            M::CarveFullScan(on) => {
                crate::fs::carve::set_full_scan(on);
                self.reopen_modes()
            }
            M::UpgradeSeekable(path) => {
                if self.modes.session.zstd_cache.is_none() {
                    return Vec::new();
                }
                self.modes.session.source_path = Some(path);
                self.modes.session.zstd_cache = None;
                self.reopen_modes()
            }
            M::Password(secret) => {
                self.modes.session.password = Some(secret.clone());
                self.forward(Side::Left, BrowserPaneAction::SubmitPassword(secret))
            }
        }
    }

    /// Hand the pane the current session, keeping its queue, folder and expanded set.
    fn reopen_modes(&mut self) -> Vec<Effect> {
        self.modes.generation += 1;
        let session = self.modes.session.clone();
        let label = self.modes.label.clone();
        self.open_fixed_session(session, &label)
    }

    fn mode_busy(&self) -> bool {
        self.modes.job.is_some() || self.copy.is_some()
    }

    fn toggle_edit_mode(&mut self) -> Vec<Effect> {
        if !self.modes.edit_supported || self.mode_busy() {
            return Vec::new();
        }
        let editing = self.modes.edit_mode;
        if editing && self.has_unsaved_edits() {
            self.modes.unsaved = true;
            return Vec::new();
        }
        match (editing, self.is_chd_source(), self.modes.archive.is_some()) {
            (false, true, _) => self.start_chd_open(),
            (false, false, true) => self.start_archive_extract(),
            (false, false, false) => {
                let squash = self.left.volume().fs_type == "SquashFS";
                if squash && self.modes.session.rebuild_budget.is_none() {
                    self.start_measure();
                } else {
                    self.probe_editable();
                }
            }
            (true, ..) => self.exit_edit_mode(),
        }
        Vec::new()
    }

    fn spawn_mode<R: Send + 'static>(
        &mut self,
        label: &str,
        body: impl FnOnce(&crate::model::job::JobCtx) -> R + Send + 'static,
    ) -> Option<Job<R>> {
        let spawned = self
            .jobs
            .spawn(label, Vec::new(), Arc::clone(&self.notify), move |ctx| {
                Ok(body(ctx))
            });
        match spawned {
            Ok(job) => Some(job),
            Err(busy) => {
                self.modes.notice = Some(format!("Error: {busy}"));
                None
            }
        }
    }

    fn start_chd_open(&mut self) {
        let Some(parent) = self.modes.session.source_path.clone() else {
            self.modes.notice = Some("Error: no source path".into());
            return;
        };
        let job = self.spawn_mode("Preparing CHD for editing...", move |ctx| {
            let opened = open_chd(&parent, ctx);
            (parent, opened)
        });
        self.modes.job = job.map(ModeJob::ChdOpen);
    }

    fn start_archive_extract(&mut self) {
        let Some(ctx) = self.modes.archive.clone() else {
            return;
        };
        let temp = archive_edit::edit_temp_path(&ctx);
        let job = self.spawn_mode("Decompressing for editing", move |job| {
            archive_edit::extract_for_edit(&ctx, &temp, job).map(|()| temp)
        });
        self.modes.job = job.map(ModeJob::Extract);
    }

    fn start_archive_compress(&mut self) {
        let (Some(ctx), Some(temp)) = (self.modes.archive.clone(), self.modes.archive_temp.clone())
        else {
            return;
        };
        self.leave_edit_state();
        // The pane lets go of the temp file before it is recompressed and removed.
        self.left.release_source();
        let job = self.spawn_mode("Recompressing the archive", move |job| {
            archive_edit::compress_after_edit(&ctx, &temp, job)
        });
        self.modes.job = job.map(ModeJob::Compress);
    }

    fn start_measure(&mut self) {
        let session = self.modes.session.clone();
        // A partition bounds the image; a bare file at byte 0 is the image.
        let capacity = (session.partition_offset != 0)
            .then_some(session.partition_size)
            .flatten();
        let job = self.spawn_mode("Measuring the image...", move |job| {
            job.set_can_cancel(false);
            let mut fs = session
                .open()
                .map_err(|e| format!("Cannot measure this image: {e}"))?;
            let image_len = fs_squashfs::squashfs_write::image_footprint(fs.total_size());
            fs_squashfs::squashfs_edit::plan_size(fs.as_mut(), image_len, capacity)
                .map_err(|e| format!("Cannot measure this image: {e}"))
        });
        self.modes.job = job.map(ModeJob::Measure);
    }

    /// Enter Edit Mode once an editable open succeeds, so a refusal shows now, not at Apply.
    fn probe_editable(&mut self) {
        let session = self.modes.session.clone();
        let job = self.spawn_mode("Opening for editing", move |_| {
            session
                .open_editable()
                .map(|_| ())
                .map_err(|e| e.to_string())
        });
        self.modes.job = job.map(ModeJob::Probe);
    }

    fn enter_edit_state(&mut self) {
        self.modes.edit_mode = true;
        self.modes.free = None;
        self.left.set_read_only(false);
    }

    fn leave_edit_state(&mut self) {
        self.modes.edit_mode = false;
        self.modes.free = None;
        self.modes.free_job = None;
        self.modes.notice = None;
        self.left.set_read_only(true);
    }

    fn exit_edit_mode(&mut self) {
        self.left.queue.clear();
        if self.modes.chd.is_some() {
            self.leave_edit_state();
            self.start_chd_flatten();
        } else if self.modes.archive.is_some() {
            self.start_archive_compress();
        } else {
            self.leave_edit_state();
        }
    }

    fn start_chd_flatten(&mut self) {
        self.modes.chd_dirty = false;
        let Some(chd) = self.modes.chd.take() else {
            return;
        };
        self.modes.session.chd_edit_session = None;
        let Some(diff) = chd.diff else {
            // Uncompressed: the writes already landed in the parent.
            self.modes.notice = Some("Changes saved successfully.".into());
            self.reopen_modes();
            return;
        };
        let folder = self.modes.backup_folder.clone();
        let job = self.spawn_mode("Flattening CHD...", move |job| {
            flatten_chd(&chd.parent, &diff, folder.as_deref(), job)
        });
        self.modes.job = job.map(ModeJob::Flatten);
    }

    /// Drop the CHD session and its diff; the parent is untouched.
    fn discard_chd(&mut self) {
        self.modes.chd_dirty = false;
        self.modes.session.chd_edit_session = None;
        if let Some(diff) = self.modes.chd.take().and_then(|c| c.diff) {
            // The pane's readers hold the diff open until the reopen replaces them.
            self.reopen_modes();
            let _ = std::fs::remove_file(diff);
        }
    }

    fn start_apply(&mut self, follow: FollowUp) -> Vec<Effect> {
        if self.modes.follow.is_some() {
            return Vec::new();
        }
        let total = self.left.queue.len();
        self.modes.follow = Some((follow, total, false));
        self.forward(Side::Left, BrowserPaneAction::Apply)
    }

    fn unsaved(&mut self, choice: UnsavedChoice) -> Vec<Effect> {
        self.modes.unsaved = false;
        match choice {
            UnsavedChoice::Cancel => {
                self.modes.pending_close = false;
                Vec::new()
            }
            // Only a CHD diff waits: leaving Edit Mode flattens it.
            UnsavedChoice::Apply if self.left.queue.is_empty() => {
                self.leave_or_close();
                Vec::new()
            }
            UnsavedChoice::Apply => self.start_apply(FollowUp::Leave),
            UnsavedChoice::Discard => {
                // The question was the confirmation, so answer the pane's own discard.
                let yes = BrowserPaneAction::Confirmed {
                    purpose: pane_purpose::DISCARD_STAGED,
                    yes: true,
                };
                let fx = self.forward(Side::Left, yes);
                if self.modes.chd.is_some() {
                    self.discard_chd();
                }
                self.leave_or_close();
                fx
            }
        }
    }

    fn leave_or_close(&mut self) {
        if std::mem::take(&mut self.modes.pending_close) {
            self.modes.close = true;
        } else {
            self.exit_edit_mode();
        }
    }

    fn has_error(&self) -> bool {
        let n = self.modes.notice.as_deref();
        n.is_some_and(|n| n.starts_with("Error"))
    }

    fn start_chd_info(&mut self) {
        let Some(path) = self.modes.session.source_path.clone() else {
            return;
        };
        let job = self.spawn_mode("Reading CHD info", move |_| chd_info_text(&path));
        self.modes.info_job = job;
    }

    fn open_nested(&mut self, path: &str) -> Vec<Effect> {
        if !self.left.queue.is_empty() {
            self.status =
                "Apply or discard the staged edits first: opening a disk image replaces this view."
                    .into();
            return Vec::new();
        }
        let Some(entry) = self.left.entry_at(path) else {
            return Vec::new();
        };
        let temp = match tempfile::Builder::new().prefix("rb-nested-").tempdir() {
            Ok(t) => t,
            Err(e) => {
                self.status = format!("Open as disk image failed: {e}");
                return Vec::new();
            }
        };
        let needed = entry.size.saturating_add(NESTED_FREE_MARGIN);
        if let Some(free) = crate::os::available_space(temp.path()) {
            if free < needed {
                let size = disk_core::display::format_size;
                self.status = format!(
                    "Not enough free space to open {} as a disk image: it needs {} in {}, \
                     which has {} free. Point TMPDIR at a larger drive, or extract it instead.",
                    entry.name,
                    size(needed),
                    temp.path().parent().unwrap_or(temp.path()).display(),
                    size(free),
                );
                return Vec::new();
            }
        }
        let dest = temp.path().to_path_buf();
        self.modes.nested = Some(temp);
        // An image opened from inside the volume wants its bytes, not its forks.
        let options = ExtractOptions {
            fork_mode: ResourceForkMode::DataForkOnly,
            prodos_suffix: false,
            rs_to_lf: false,
        };
        self.request_extract(Side::Left, path, options, Some(dest))
    }

    // --- polling ----------------------------------------------------------

    /// Land finished mode jobs, watch the pane's writes and Apply; called from `tick`.
    pub(super) fn poll_modes(&mut self) {
        if !self.modes.active {
            return;
        }
        self.poll_mode_job();
        self.poll_writes();
        self.poll_apply_follow();
        self.poll_free_space();
        self.poll_nested();
        if self
            .modes
            .info_job
            .as_ref()
            .is_some_and(|j| !j.is_running())
        {
            let job = self.modes.info_job.take().expect("checked");
            match job.take_result() {
                Some(Ok(text)) => self.modes.info = Some(text),
                Some(Err(e)) => self.modes.error = Some(format!("CHD Info failed: {e}")),
                None => {}
            }
        }
    }

    fn poll_mode_job(&mut self) {
        let running = match &self.modes.job {
            Some(ModeJob::ChdOpen(j)) => j.is_running(),
            Some(ModeJob::Flatten(j)) => j.is_running(),
            Some(ModeJob::Extract(j)) => j.is_running(),
            Some(ModeJob::Compress(j)) => j.is_running(),
            Some(ModeJob::Measure(j)) => j.is_running(),
            Some(ModeJob::Probe(j)) => j.is_running(),
            None => return,
        };
        if running {
            return;
        }
        let failed = |s: Snapshot| match s.status {
            Status::Failed(e) => e,
            _ => "stopped".to_string(),
        };
        match self.modes.job.take() {
            Some(ModeJob::ChdOpen(job)) => {
                let snap = job.snapshot();
                match job.take_result() {
                    Some((parent, Ok((session, diff)))) => {
                        self.modes.session.chd_edit_session = Some(session);
                        self.modes.chd = Some(ChdEdit { parent, diff });
                        self.enter_edit_state();
                        self.reopen_modes();
                    }
                    Some((_, Err(message))) => self.modes.notice = Some(message),
                    None => {
                        self.modes.notice = Some(format!("Error opening CHD: {}", failed(snap)))
                    }
                }
            }
            Some(ModeJob::Flatten(job)) => {
                let snap = job.snapshot();
                let out = job.take_result();
                self.modes.notice = Some(match out {
                    Some(Ok(said)) | Some(Err(said)) => said,
                    None => format!("Error flattening CHD: {}", failed(snap)),
                });
                self.reopen_modes();
            }
            Some(ModeJob::Extract(job)) => {
                let snap = job.snapshot();
                match job.take_result() {
                    Some(Ok(temp)) => self.adopt_archive_temp(temp),
                    Some(Err(e)) => self.modes.notice = Some(format!("Error: {e}")),
                    None => self.modes.notice = Some(format!("Error: {}", failed(snap))),
                }
            }
            Some(ModeJob::Compress(job)) => {
                let snap = job.snapshot();
                let out = job.take_result();
                let out = out.unwrap_or_else(|| Err(failed(snap)));
                match (out, self.modes.archive.clone()) {
                    (Ok(()), Some(ctx)) => {
                        self.modes.session.source_path = Some(ctx.archive_path);
                        self.modes.archive_temp = None;
                        self.modes.notice = Some("Changes saved successfully.".into());
                        self.reopen_modes();
                    }
                    (Err(e), _) => {
                        // The temp still holds the edits: adopt it again so a retry saves, not re-extracts.
                        if let Some(temp) = self.modes.archive_temp.clone() {
                            self.adopt_archive_temp(temp);
                        }
                        self.modes.notice = Some(format!(
                            "Error saving the archive: {e}. Your edits are still in the temporary \
                             volume; use Close or Apply Edits to try saving again."
                        ));
                    }
                    (Ok(()), None) => {}
                }
            }
            Some(ModeJob::Measure(job)) => {
                let snap = job.snapshot();
                match job.take_result() {
                    Some(Ok(plan)) => self.modes.plan = Some(plan),
                    Some(Err(e)) => self.modes.error = Some(e),
                    None => {
                        let why = failed(snap);
                        self.modes.error = Some(format!("Cannot measure this image: {why}"));
                    }
                }
            }
            Some(ModeJob::Probe(job)) => {
                let snap = job.snapshot();
                match job.take_result() {
                    Some(Ok(())) => self.enter_edit_state(),
                    Some(Err(e)) => self.modes.error = Some(format!("Cannot enter edit mode: {e}")),
                    None => {
                        let why = failed(snap);
                        self.modes.error = Some(format!("Cannot enter edit mode: {why}"));
                    }
                }
            }
            None => {}
        }
    }

    /// Point the session at the decompressed temp and enter Edit Mode; the partition offset stays.
    fn adopt_archive_temp(&mut self, temp: PathBuf) {
        self.modes.archive_temp = Some(temp.clone());
        self.modes.session.source_path = Some(temp);
        self.modes.session.zstd_cache = None;
        self.enter_edit_state();
        self.reopen_modes();
    }

    /// An apply, repair or tar import wrote the volume: the readout is stale, a CHD diff unsaved.
    fn poll_writes(&mut self) {
        let writes = self.left.write_count();
        if writes == self.modes.writes {
            return;
        }
        self.modes.writes = writes;
        self.modes.generation += 1;
        self.modes.free = None;
        self.modes.free_job = None;
        if self.modes.chd.is_some() {
            self.modes.chd_dirty = true;
        }
    }

    fn poll_apply_follow(&mut self) {
        let Some((follow, total, wrote)) = self.modes.follow else {
            return;
        };
        let wrote = wrote || self.left.applying().is_some();
        if self.left.is_busy() || self.left.apply_review().is_some() {
            self.modes.follow = Some((follow, total, wrote));
            return;
        }
        self.modes.follow = None;
        if !wrote {
            // The review was dismissed, or the preflight failed and left a report.
            self.modes.pending_close = false;
            return;
        }
        let left = self.left.queue.len();
        self.modes.notice = Some(match self.left.apply_report() {
            Some(r) => {
                let why = r.fatal.clone();
                let why = why.or_else(|| r.failed.first().map(|f| f.error.clone()));
                format!(
                    "Error: {}. Applied {} of {}; {left} edit(s) remain staged",
                    why.unwrap_or_default(),
                    r.applied,
                    r.total
                )
            }
            None => format!("Applied {total} edit(s) successfully"),
        });
        if self.has_error() {
            return;
        }
        match follow {
            FollowUp::Recompress if self.modes.archive.is_some() => {
                self.left.queue.clear();
                self.start_archive_compress();
            }
            FollowUp::Recompress => {}
            FollowUp::Leave => self.leave_or_close(),
        }
    }

    /// Probe free space once per edit session and after each write (an editable open decodes a container).
    fn poll_free_space(&mut self) {
        if let Some(job) = &self.modes.free_job {
            if !job.is_running() {
                self.modes.free = Some(job.take_result().flatten());
                self.modes.free_job = None;
            }
            return;
        }
        let idle = !self.mode_busy() && !self.left.is_busy() && self.modes.follow.is_none();
        if !self.modes.edit_mode || self.modes.free.is_some() || !idle {
            return;
        }
        let session = self.modes.session.clone();
        let spawned = self.jobs.spawn(
            "Free space",
            Vec::new(),
            Arc::clone(&self.notify),
            move |_| {
                let opened = session.open_editable().ok();
                Ok(opened.and_then(|(mut efs, _commit)| efs.free_space().ok()))
            },
        );
        self.modes.free_job = spawned.ok();
    }

    /// Hand the host the image an "Open as Disk Image" extraction wrote.
    fn poll_nested(&mut self) {
        if self.modes.nested.is_none()
            || self.copy.is_some()
            || self.extract.is_some()
            || self.conflict.is_some()
        {
            return;
        }
        let Some(temp) = self.modes.nested.take() else {
            return;
        };
        let first = std::fs::read_dir(temp.path())
            .ok()
            .and_then(|mut d| d.find_map(|e| e.ok()))
            .map(|e| e.path());
        match first {
            Some(path) => {
                self.modes.notice = Some(format!("Opening {} as a disk image...", path.display()));
                self.modes.nested_ready = Some((path, temp));
            }
            None => self.modes.notice = Some(format!("Open as disk image: {}", self.status)),
        }
    }

    pub(super) fn modes_working(&self) -> bool {
        self.modes.job.is_some() || self.modes.free_job.is_some() || self.modes.info_job.is_some()
    }
}

#[cfg(test)]
#[path = "modes_tests.rs"]
mod tests;
