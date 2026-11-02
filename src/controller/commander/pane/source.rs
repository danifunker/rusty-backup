//! Opening a pane's source: a host folder, disk image, backup folder, Mac archive or optical disc.
//!
//! Every open is a [`Job`] holding its path as a [`Resource`]; `tick` installs the result. An
//! image or backup opens in two jobs: the source job finds the partitions, then a partition job
//! opens one (a Clonezilla partition first scans its partclone metadata). A new open abandons the
//! one in flight, and waits for it to let go of the path if they share one. Leaving a source
//! that has staged edits asks first with [`Effect::Confirm`]; a locked source waits for a
//! password through [`CommanderPaneController::needs_password`].

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context as _};
use disk_core::entry::FileEntry;
use disk_core::filesystem::Filesystem;

use super::{CommanderPaneAction, CommanderPaneController, PaneRequest};
use crate::controller::commander::rows::{is_mac_archive_path, partition_label};
use crate::controller::{ConfirmSpec, Effect, PickSpec};
use crate::fs::partition_is_browsable;
use crate::model::browse_session::{looks_like_password_error, BrowseOpenStatus, BrowseSession};
use crate::model::cache_runner;
use crate::model::commander_descend::{
    classify, open_archive, open_optical, open_optical_hybrid, optical_filesystems, DescendKind,
    OpticalFsChoice, ReopenRecipe,
};
use crate::model::commander_ops::StageSource;
use crate::model::commander_source::{
    self, BackupPartitionOpen, PartcloneCacheStore, PartcloneLookup, ResolvedBackup,
};
use crate::model::dir_listing::{read_host_root, DirListing, HostRoot};
use crate::model::edit_queue::StagedEdit;
use crate::model::job::{Job, JobCtx, JobRegistry, NoNotify, Notify, Resource, Snapshot, Status};
use crate::model::worker::lock_status;
use crate::partition::PartitionInfo;

/// The `purpose` of each picker and confirmation a pane asks for.
pub mod purpose {
    pub const OPEN_IMAGE: &str = "commander.open_image";
    pub const OPEN_HOST_FOLDER: &str = "commander.open_host_folder";
    pub const OPEN_BACKUP_FOLDER: &str = "commander.open_backup_folder";
    pub const DISCARD_EDITS: &str = "commander.discard_edits";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Image,
    Host,
    Backup,
    Archive,
    Optical,
}

/// The open volume's readout.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VolumeInfo {
    pub label: String,
    pub fs_type: String,
    pub total_size: u64,
    pub used_size: u64,
}

impl VolumeInfo {
    /// The volume label, or the filesystem type when it has none.
    pub fn name(&self) -> &str {
        if self.label.is_empty() {
            &self.fs_type
        } else {
            &self.label
        }
    }
}

/// Where a pane reads and records its recent files.
pub trait RecentStore: Send {
    /// Newest first.
    fn list(&self) -> Vec<String>;
    fn remember(&mut self, path: &str);
}

/// No recent files (tests, slim builds).
pub struct NoRecent;

impl RecentStore for NoRecent {
    fn list(&self) -> Vec<String> {
        Vec::new()
    }

    fn remember(&mut self, _path: &str) {}
}

/// What a pane needs from the app: the job registry, the shell's wake and the recent files.
pub struct PaneServices {
    pub jobs: JobRegistry,
    pub notify: Arc<dyn Notify>,
    pub recent: Box<dyn RecentStore>,
}

impl PaneServices {
    /// Its own registry, no wake and no recent files.
    pub fn headless() -> Self {
        PaneServices {
            jobs: JobRegistry::new(),
            notify: Arc::new(NoNotify),
            recent: Box::new(NoRecent),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OpenRequest {
    /// A folder opens as a host folder, anything else as an image.
    Path(PathBuf),
    Image(PathBuf),
    Host(PathBuf),
    Backup(PathBuf),
}

impl OpenRequest {
    fn path(&self) -> &Path {
        match self {
            OpenRequest::Path(p)
            | OpenRequest::Image(p)
            | OpenRequest::Host(p)
            | OpenRequest::Backup(p) => p,
        }
    }

    fn resource(&self) -> Resource {
        match self {
            OpenRequest::Host(p) | OpenRequest::Backup(p) => Resource::Folder(p.clone()),
            OpenRequest::Path(p) | OpenRequest::Image(p) => Resource::Image(p.clone()),
        }
    }
}

/// A source change waiting for the user to agree to drop the staged edits.
pub(super) enum Switch {
    Open(OpenRequest),
    Partition(usize),
    Close,
}

/// How a partition job gets its browse session.
#[derive(Clone)]
enum PartOpen {
    Session(BrowseSession),
    Backup {
        backup: Arc<ResolvedBackup>,
        part_index: usize,
        store: Arc<Mutex<PartcloneCacheStore>>,
    },
}

/// One open job's work, kept so an open that found its path busy can start again later.
#[derive(Clone)]
enum Plan {
    Source(OpenRequest),
    Partition {
        how: PartOpen,
        resource: Resource,
        label: String,
    },
    Optical {
        path: PathBuf,
        choices: Vec<OpticalFsChoice>,
        idx: usize,
    },
}

impl Plan {
    fn label_and_resource(&self) -> (String, Resource) {
        match self {
            Plan::Source(req) => (format!("Opening {}", file_name(req.path())), req.resource()),
            Plan::Partition {
                resource, label, ..
            } => (format!("Opening {label}"), resource.clone()),
            Plan::Optical { path, choices, idx } => (
                format!("Opening {}", choices[*idx].label),
                Resource::Image(path.clone()),
            ),
        }
    }

    fn source_level(&self) -> bool {
        matches!(self, Plan::Source(_))
    }
}

struct Loaded {
    fs: Box<dyn Filesystem>,
    root: FileEntry,
    entries: Vec<FileEntry>,
    volume: VolumeInfo,
}

/// What an open job hands back to `tick`.
enum Opened {
    Host(HostRoot),
    Archive(Loaded),
    Optical {
        choices: Vec<OpticalFsChoice>,
        idx: usize,
        loaded: Loaded,
    },
    Partitions(Vec<PartitionInfo>),
    Backup(ResolvedBackup),
    Volume {
        session: BrowseSession,
        loaded: Loaded,
    },
    Locked {
        session: BrowseSession,
        volume: VolumeInfo,
    },
}

struct Opening {
    job: Job<Opened>,
    source_level: bool,
}

/// The source half of a pane's state.
pub(super) struct SourceState {
    services: PaneServices,
    recent: Vec<String>,
    path: Option<PathBuf>,
    kind: Option<SourceKind>,
    partitions: Vec<PartitionInfo>,
    backup: Option<Arc<ResolvedBackup>>,
    optical: Vec<OpticalFsChoice>,
    selected: Option<usize>,
    session: Option<BrowseSession>,
    /// Scanned Clonezilla caches; replaced (not cleared) per source so an abandoned job cannot refill it.
    cache_store: Arc<Mutex<PartcloneCacheStore>>,
    volume: VolumeInfo,
    needs_password: bool,
    error: Option<String>,
    opening: Option<Opening>,
    /// A replaced open still winding down; a deferred open waits for it to free the path.
    abandoned: Option<Job<Opened>>,
    deferred: Option<Plan>,
    pending_switch: Option<Switch>,
    /// Edits still staged and the folder to return to once a post-apply reopen lands.
    reopen_restore: Option<(Vec<StagedEdit>, String)>,
}

impl SourceState {
    pub(super) fn new(services: PaneServices) -> Self {
        let recent = services.recent.list();
        SourceState {
            services,
            recent,
            path: None,
            kind: None,
            partitions: Vec::new(),
            backup: None,
            optical: Vec::new(),
            selected: None,
            session: None,
            cache_store: Arc::new(Mutex::new(PartcloneCacheStore::new())),
            volume: VolumeInfo::default(),
            needs_password: false,
            error: None,
            opening: None,
            abandoned: None,
            deferred: None,
            pending_switch: None,
            reopen_restore: None,
        }
    }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

fn file_label(path: &Path) -> Option<String> {
    path.file_name().map(|n| n.to_string_lossy().into_owned())
}

fn message(text: String) -> Effect {
    Effect::Message(text)
}

/// The image picker's filters: disk images, Mac archives, anything.
fn image_filters() -> Vec<(String, Vec<String>)> {
    let list = |exts: &[&str]| exts.iter().map(|e| e.to_string()).collect();
    vec![
        (
            "Disk Images".to_string(),
            list(crate::model::file_types::DISK_IMAGE_EXTS),
        ),
        (
            "Mac archives".to_string(),
            list(crate::model::file_types::MAC_ARCHIVE_EXTS),
        ),
        ("All Files".to_string(), vec!["*".to_string()]),
    ]
}

impl CommanderPaneController {
    // --- read accessors ---------------------------------------------------

    /// The open image, folder or backup; `None` before one opens or after a failed open.
    pub fn source_path(&self) -> Option<&Path> {
        self.src.path.as_deref()
    }

    /// What kind of source is open; `None` while an image or dropped path is still being probed.
    pub fn source_kind(&self) -> Option<SourceKind> {
        self.src.kind
    }

    /// True for a (read-only) native or Clonezilla backup folder.
    pub fn is_backup(&self) -> bool {
        self.src.backup.is_some()
    }

    pub fn is_archive(&self) -> bool {
        self.src.kind == Some(SourceKind::Archive)
    }

    pub fn is_optical(&self) -> bool {
        self.src.kind == Some(SourceKind::Optical)
    }

    /// The partition picker's entries: a disc's filesystems, or the partition table.
    pub fn partition_labels(&self) -> Vec<String> {
        if self.is_optical() {
            self.src.optical.iter().map(|c| c.label.clone()).collect()
        } else {
            let parts = self.src.partitions.iter().enumerate();
            parts.map(|(i, p)| partition_label(i, p)).collect()
        }
    }

    pub fn selected_partition(&self) -> Option<usize> {
        self.src.selected
    }

    /// The session that opened the current partition, for apply and copy workers.
    pub fn session(&self) -> Option<&BrowseSession> {
        self.src.session.as_ref()
    }

    pub fn volume(&self) -> &VolumeInfo {
        &self.src.volume
    }

    /// True when the source is locked; answer with `SubmitPassword`.
    pub fn needs_password(&self) -> bool {
        self.src.needs_password && self.src.session.is_some()
    }

    /// Why the source or partition did not open.
    pub fn source_error(&self) -> Option<&str> {
        self.src.error.as_deref()
    }

    /// The open in progress, for a progress display.
    pub fn opening(&self) -> Option<Snapshot> {
        self.src.opening.as_ref().map(|o| o.job.snapshot())
    }

    /// True while an open runs or waits to start.
    pub fn is_opening(&self) -> bool {
        self.src.opening.is_some() || self.src.deferred.is_some()
    }

    /// Recently opened paths, newest first.
    pub fn recent(&self) -> &[String] {
        &self.src.recent
    }

    /// How a worker reopens this pane's local source: its session, or an archive / disc by path.
    pub fn local_stage_source(&self) -> Option<StageSource> {
        if let Some(session) = &self.src.session {
            return Some(StageSource::Session(session.clone()));
        }
        let path = self.src.path.clone()?;
        let label = file_label(&path);
        let recipe = match self.src.kind? {
            SourceKind::Archive => ReopenRecipe::Archive { path, label },
            SourceKind::Optical => {
                let hybrid = self
                    .src
                    .selected
                    .and_then(|i| self.src.optical.get(i))
                    .and_then(|c| c.hybrid_index);
                match hybrid {
                    Some(index) => ReopenRecipe::OpticalHybrid { path, index, label },
                    None => ReopenRecipe::Optical { path, label },
                }
            }
            _ => return None,
        };
        Some(StageSource::Reopen(recipe))
    }

    // --- transitional entry points for the view (slices 4 and 5) ----------

    /// Reopen the partition after a write, keeping what is still staged and the current folder.
    pub fn reopen_after_apply(&mut self) -> Vec<Effect> {
        let Some(i) = self.src.selected else {
            return Vec::new();
        };
        let keep: Vec<StagedEdit> = self.queue.drain().collect();
        let cwd = self.listing.cwd_path().to_string();
        let fx = self.open_partition(i);
        self.src.reopen_restore = Some((keep, cwd));
        fx
    }

    /// Show a remote listing the view opened, dropping any local source.
    pub fn install_remote(
        &mut self,
        fs: Box<dyn Filesystem>,
        root: FileEntry,
        entries: Vec<FileEntry>,
        volume: VolumeInfo,
    ) {
        self.reset_source();
        self.src.volume = volume;
        self.listing.load_root(fs, root, entries, false);
        self.rebuild_rows();
    }

    // --- actions ----------------------------------------------------------

    pub(super) fn apply_source(&mut self, action: CommanderPaneAction) -> Vec<Effect> {
        use CommanderPaneAction as A;
        match action {
            A::OpenImage(p) => self.request_switch(Switch::Open(OpenRequest::Image(p))),
            A::OpenHostFolder(p) => self.request_switch(Switch::Open(OpenRequest::Host(p))),
            A::OpenBackupFolder(p) => self.request_switch(Switch::Open(OpenRequest::Backup(p))),
            A::OpenPath(p) => self.request_switch(Switch::Open(OpenRequest::Path(p))),
            A::PickImage => vec![Effect::PickFile(PickSpec {
                purpose: purpose::OPEN_IMAGE,
                title: "Open disk image".into(),
                filters: image_filters(),
                save: false,
            })],
            A::PickHostFolder => vec![Effect::PickFolder(PickSpec {
                purpose: purpose::OPEN_HOST_FOLDER,
                title: "Open local folder".into(),
                filters: Vec::new(),
                save: false,
            })],
            A::PickBackupFolder => vec![Effect::PickFolder(PickSpec {
                purpose: purpose::OPEN_BACKUP_FOLDER,
                title: "Open backup folder".into(),
                filters: Vec::new(),
                save: false,
            })],
            A::Picked {
                purpose,
                path: Some(p),
            } => match purpose {
                purpose::OPEN_IMAGE => self.apply_source(A::OpenImage(p)),
                purpose::OPEN_HOST_FOLDER => self.apply_source(A::OpenHostFolder(p)),
                purpose::OPEN_BACKUP_FOLDER => self.apply_source(A::OpenBackupFolder(p)),
                _ => Vec::new(),
            },
            A::Picked { path: None, .. } => Vec::new(),
            A::SelectPartition(i) => {
                if self.src.selected == Some(i) && self.listing.is_loaded() {
                    return Vec::new();
                }
                self.request_switch(Switch::Partition(i))
            }
            A::CloseSource => self.request_switch(Switch::Close),
            A::CancelOpen => {
                if let Some(o) = &self.src.opening {
                    o.job.cancel();
                }
                Vec::new()
            }
            A::SubmitPassword(secret) => self.submit_password(secret),
            A::Confirmed { purpose, yes } => {
                if purpose != purpose::DISCARD_EDITS {
                    return Vec::new();
                }
                match self.src.pending_switch.take() {
                    Some(sw) if yes => self.switch(sw),
                    _ => Vec::new(),
                }
            }
            A::RecentCleared => {
                self.src.recent.clear();
                Vec::new()
            }
            // Navigation and selection are handled in `apply`.
            _ => Vec::new(),
        }
    }

    /// A locked volume can open and then fail deeper; that error becomes the password prompt.
    pub(super) fn password_error_locks(&mut self) {
        let locked = self.src.session.is_some()
            && self.error.as_deref().is_some_and(looks_like_password_error);
        if locked {
            self.src.needs_password = true;
            self.error = None;
        }
    }

    fn request_switch(&mut self, sw: Switch) -> Vec<Effect> {
        if self.queue.is_empty() {
            return self.switch(sw);
        }
        let n = self.queue.len();
        self.src.pending_switch = Some(sw);
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::DISCARD_EDITS,
            title: "Discard staged edits?".into(),
            message: format!(
                "This pane has {n} staged edit(s) that have not been applied. \
                 Switching the source will discard them."
            ),
            confirm_label: "Discard & switch".into(),
            destructive: true,
        })]
    }

    fn switch(&mut self, sw: Switch) -> Vec<Effect> {
        match sw {
            Switch::Open(req) => self.start_source(req),
            Switch::Partition(i) => self.open_partition(i),
            Switch::Close => {
                self.reset_source();
                self.requests.push(PaneRequest::SourceReset);
                vec![message("closed.".into())]
            }
        }
    }

    fn abandon_open(&mut self) {
        if let Some(o) = self.src.opening.take() {
            o.job.cancel();
            self.src.abandoned = Some(o.job);
        }
        self.src.deferred = None;
    }

    fn reset_source(&mut self) {
        self.abandon_open();
        let s = &mut self.src;
        s.path = None;
        s.kind = None;
        s.partitions.clear();
        s.backup = None;
        s.optical.clear();
        s.selected = None;
        s.session = None;
        s.cache_store = Arc::new(Mutex::new(PartcloneCacheStore::new()));
        s.volume = VolumeInfo::default();
        s.needs_password = false;
        s.error = None;
        s.reopen_restore = None;
        self.listing = DirListing::new();
        self.source_changed();
        self.queue.clear();
    }

    fn start_source(&mut self, req: OpenRequest) -> Vec<Effect> {
        self.reset_source();
        self.requests.push(PaneRequest::SourceReset);
        let path = req.path().to_path_buf();
        self.src.kind = match req {
            OpenRequest::Host(_) => Some(SourceKind::Host),
            OpenRequest::Backup(_) => Some(SourceKind::Backup),
            OpenRequest::Image(_) | OpenRequest::Path(_) => None,
        };
        self.src.path = Some(path.clone());
        self.spawn(Plan::Source(req));
        vec![message(format!("opening {} ...", path.display()))]
    }

    /// Clears the listing for a partition or disc filesystem about to open.
    fn clear_for_partition(&mut self, idx: usize) {
        self.abandon_open();
        self.src.selected = Some(idx);
        self.src.error = None;
        self.src.needs_password = false;
        self.src.session = None;
        self.listing = DirListing::new();
        self.source_changed();
        self.queue.clear();
    }

    fn open_partition(&mut self, idx: usize) -> Vec<Effect> {
        let Some(path) = self.src.path.clone() else {
            return Vec::new();
        };
        if self.is_optical() {
            return self.open_optical_choice(path, idx);
        }
        let Some(part) = self.src.partitions.get(idx).cloned() else {
            return Vec::new();
        };
        self.clear_for_partition(idx);
        let browsable = partition_is_browsable(
            part.partition_type_byte,
            part.partition_type_string.as_deref(),
            &part.type_name,
        );
        if !browsable {
            let msg = format!("{} has no browsable filesystem.", part.type_name);
            self.src.error = Some(msg.clone());
            return vec![message(msg)];
        }
        let label = partition_label(idx, &part);
        let (how, resource) = match &self.src.backup {
            Some(backup) => (
                PartOpen::Backup {
                    backup: Arc::clone(backup),
                    part_index: part.index,
                    store: Arc::clone(&self.src.cache_store),
                },
                Resource::Folder(path),
            ),
            None => (
                PartOpen::Session(commander_source::session_for(&path, &part)),
                Resource::Image(path),
            ),
        };
        let msg = format!("opening {label} ...");
        self.spawn(Plan::Partition {
            how,
            resource,
            label,
        });
        vec![message(msg)]
    }

    fn open_optical_choice(&mut self, path: PathBuf, idx: usize) -> Vec<Effect> {
        let Some(choice) = self.src.optical.get(idx).cloned() else {
            return Vec::new();
        };
        self.clear_for_partition(idx);
        let choices = self.src.optical.clone();
        self.spawn(Plan::Optical { path, choices, idx });
        vec![message(format!("opening {} ...", choice.label))]
    }

    fn submit_password(&mut self, secret: String) -> Vec<Effect> {
        if !self.needs_password() {
            return Vec::new();
        }
        let (Some(mut session), Some(path)) = (self.src.session.take(), self.src.path.clone())
        else {
            return Vec::new();
        };
        session.password = Some(secret);
        self.src.needs_password = false;
        self.src.error = None;
        let resource = match self.src.backup {
            Some(_) => Resource::Folder(path.clone()),
            None => Resource::Image(path.clone()),
        };
        self.spawn(Plan::Partition {
            how: PartOpen::Session(session),
            resource,
            label: file_name(&path),
        });
        vec![message("unlocking ...".into())]
    }

    fn spawn(&mut self, plan: Plan) {
        let (label, resource) = plan.label_and_resource();
        let source_level = plan.source_level();
        let body = plan.clone();
        let notify = Arc::clone(&self.src.services.notify);
        let spawned = self
            .src
            .services
            .jobs
            .spawn(label, vec![resource], notify, move |ctx| {
                run_plan(body, ctx)
            });
        match spawned {
            Ok(job) => self.src.opening = Some(Opening { job, source_level }),
            // Our own replaced open still holds the path; start once it lets go.
            Err(_) if self.src.abandoned.as_ref().is_some_and(|j| j.is_running()) => {
                self.src.deferred = Some(plan);
            }
            Err(busy) => self.open_failed(busy.to_string(), source_level),
        }
    }

    /// Installs a finished open and starts a deferred one; called from `tick`.
    pub(super) fn poll_open(&mut self) -> Vec<Effect> {
        let mut fx = Vec::new();
        if self.src.abandoned.as_ref().is_some_and(|j| !j.is_running()) {
            self.src.abandoned = None;
        }
        if self.src.abandoned.is_none() {
            if let Some(plan) = self.src.deferred.take() {
                self.spawn(plan);
            }
        }
        let finished = self
            .src
            .opening
            .as_ref()
            .is_some_and(|o| !o.job.is_running());
        if !finished {
            return fx;
        }
        let Some(o) = self.src.opening.take() else {
            return fx;
        };
        match o.job.snapshot().status {
            Status::Done => {
                if let Some(opened) = o.job.take_result() {
                    fx.extend(self.install(opened));
                }
            }
            Status::Failed(msg) => {
                self.open_failed(msg, o.source_level);
                fx.push(message("open failed.".into()));
            }
            Status::Cancelled => {
                self.open_failed("Open cancelled.".into(), o.source_level);
                fx.push(message("open cancelled.".into()));
            }
            Status::Running => {}
        }
        fx
    }

    fn open_failed(&mut self, msg: String, source_level: bool) {
        // A failed post-apply reopen must not lose what is still staged.
        if let Some((edits, _)) = self.src.reopen_restore.take() {
            for e in edits {
                self.queue.push(e);
            }
        }
        self.src.error = Some(msg);
        if source_level {
            self.src.path = None;
            self.src.kind = None;
        }
    }

    fn remember_source(&mut self) {
        if let Some(path) = &self.src.path {
            let path = path.display().to_string();
            self.src.services.recent.remember(&path);
            self.src.recent = self.src.services.recent.list();
        }
    }

    fn show_loaded(&mut self, loaded: Loaded) -> usize {
        self.src.volume = loaded.volume;
        let n = loaded.entries.len();
        self.source_changed();
        self.listing
            .load_root(loaded.fs, loaded.root, loaded.entries, false);
        n
    }

    /// Index of the first partition the browser can open (skips extended containers, maps, EFI).
    fn first_browsable_partition(&self) -> Option<usize> {
        self.src.partitions.iter().position(|p| {
            !p.is_extended_container
                && partition_is_browsable(
                    p.partition_type_byte,
                    p.partition_type_string.as_deref(),
                    &p.type_name,
                )
        })
    }

    fn install(&mut self, opened: Opened) -> Vec<Effect> {
        let path = self
            .src
            .path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        match opened {
            Opened::Host(root) => {
                self.src.kind = Some(SourceKind::Host);
                let n = root.entries.len();
                self.source_changed();
                self.listing.load_host_prepared(root);
                vec![message(format!("opened host folder {path} ({n} item(s))."))]
            }
            Opened::Archive(loaded) => {
                self.src.kind = Some(SourceKind::Archive);
                self.remember_source();
                let n = self.show_loaded(loaded);
                vec![message(format!("opened archive {path} ({n} item(s))."))]
            }
            Opened::Optical {
                choices,
                idx,
                loaded,
            } => {
                if self.src.kind != Some(SourceKind::Optical) {
                    self.src.kind = Some(SourceKind::Optical);
                    self.remember_source();
                }
                let label = choices[idx].label.clone();
                self.src.optical = choices;
                self.src.selected = Some(idx);
                let n = self.show_loaded(loaded);
                vec![message(format!("opened {label} ({n} item(s))."))]
            }
            Opened::Partitions(parts) => {
                self.src.kind = Some(SourceKind::Image);
                self.remember_source();
                self.src.partitions = parts;
                match self.first_browsable_partition() {
                    Some(i) => self.open_partition(i),
                    None => vec![message(format!("{path} has no browsable partitions."))],
                }
            }
            Opened::Backup(resolved) => {
                self.src.kind = Some(SourceKind::Backup);
                self.src.partitions = resolved.partitions().to_vec();
                self.src.backup = Some(Arc::new(resolved));
                match self.first_browsable_partition() {
                    Some(i) => self.open_partition(i),
                    None => vec![message("backup has no browsable partitions.".into())],
                }
            }
            Opened::Volume { session, loaded } => {
                self.src.session = Some(session);
                self.src.needs_password = false;
                let n = self.show_loaded(loaded);
                if let Some((edits, cwd)) = self.src.reopen_restore.take() {
                    for e in edits {
                        self.queue.push(e);
                    }
                    if let Err(e) = self.listing.navigate_to(&cwd) {
                        log::warn!("could not return to {cwd} after reopening: {e}");
                    }
                }
                let name = self.src.volume.name().to_string();
                vec![message(format!("opened {name} ({n} item(s))."))]
            }
            Opened::Locked { session, volume } => {
                self.src.session = Some(session);
                self.src.volume = volume;
                self.src.needs_password = true;
                Vec::new()
            }
        }
    }
}

// --- job bodies: run on the job's thread ---------------------------------

fn run_plan(plan: Plan, ctx: &JobCtx) -> anyhow::Result<Opened> {
    match plan {
        Plan::Source(req) => run_source(req, ctx),
        Plan::Partition { how, .. } => {
            let session = match how {
                PartOpen::Session(s) => s,
                PartOpen::Backup {
                    backup,
                    part_index,
                    store,
                } => backup_session(ctx, &backup, part_index, &store)?,
            };
            ctx.set_phase("Opening...");
            let status = session.open_in_job(ctx)?;
            finish_open(session, status)
        }
        Plan::Optical { path, choices, idx } => {
            let loaded = load_optical(&path, &choices[idx])?;
            Ok(Opened::Optical {
                choices,
                idx,
                loaded,
            })
        }
    }
}

fn run_source(req: OpenRequest, ctx: &JobCtx) -> anyhow::Result<Opened> {
    match req {
        OpenRequest::Host(dir) => open_host(dir),
        OpenRequest::Backup(folder) => {
            ctx.set_phase("Reading backup...");
            let resolved = commander_source::resolve_backup(&folder)
                .map_err(|e| anyhow!("Could not read backup: {e:#}"))?;
            Ok(Opened::Backup(resolved))
        }
        OpenRequest::Image(path) => open_image(&path, ctx),
        OpenRequest::Path(path) if path.is_dir() => open_host(path),
        OpenRequest::Path(path) => open_image(&path, ctx),
    }
}

fn open_host(dir: PathBuf) -> anyhow::Result<Opened> {
    let root = read_host_root(dir).map_err(|e| anyhow!("Could not read folder: {e}"))?;
    Ok(Opened::Host(root))
}

fn open_image(path: &Path, ctx: &JobCtx) -> anyhow::Result<Opened> {
    // A Mac archive is no disk image: it opens read-only as an ArchiveFilesystem.
    if is_mac_archive_path(path) {
        ctx.set_phase("Reading archive...");
        let fs = open_archive(path, file_label(path))
            .map_err(|e| anyhow!("Could not open archive: {e:#}"))?;
        return Ok(Opened::Archive(load(fs, "archive")?));
    }
    // A hybrid disc hides its ISO side from the partition table, so optical images list filesystems.
    let name = path.file_name().and_then(|n| n.to_str());
    if name.and_then(classify) == Some(DescendKind::Optical) {
        ctx.set_phase("Reading disc...");
        let choices = optical_filesystems(path);
        if !choices.is_empty() {
            let loaded = load_optical(path, &choices[0])?;
            return Ok(Opened::Optical {
                choices,
                idx: 0,
                loaded,
            });
        }
    }
    ctx.set_phase("Reading partition table...");
    let parts = commander_source::probe_partitions(path)
        .map_err(|e| anyhow!("Could not read partitions: {e:#}"))?;
    Ok(Opened::Partitions(parts))
}

fn load(mut fs: Box<dyn Filesystem>, what: &str) -> anyhow::Result<Loaded> {
    let root = fs
        .root()
        .map_err(|e| anyhow!("Could not read {what} root: {e}"))?;
    let entries = fs
        .list_directory(&root)
        .map_err(|e| anyhow!("Could not list {what}: {e}"))?;
    let volume = VolumeInfo {
        label: fs.volume_label().unwrap_or_default().to_string(),
        fs_type: fs.fs_type().to_string(),
        total_size: fs.total_size(),
        used_size: fs.used_size(),
    };
    Ok(Loaded {
        fs,
        root,
        entries,
        volume,
    })
}

fn load_optical(path: &Path, choice: &OpticalFsChoice) -> anyhow::Result<Loaded> {
    let label = file_label(path);
    let fs = match choice.hybrid_index {
        None => open_optical(path, label),
        Some(h) => open_optical_hybrid(path, h, label),
    }
    .with_context(|| format!("cannot open {}", choice.label))?;
    let mut loaded = load(fs, "disc")?;
    // Optical discs report no usable free-space figure.
    loaded.volume.total_size = 0;
    loaded.volume.used_size = 0;
    Ok(loaded)
}

/// A backup partition's session; a Clonezilla one reuses or loads a scanned cache, or scans now.
fn backup_session(
    ctx: &JobCtx,
    backup: &ResolvedBackup,
    part_index: usize,
    store: &Mutex<PartcloneCacheStore>,
) -> anyhow::Result<BrowseSession> {
    let open = match backup.open_partition(part_index)? {
        BackupPartitionOpen::Session(s) => return Ok(s),
        BackupPartitionOpen::Clonezilla(open) => open,
    };
    let lookup = lock_status(store).resolve(part_index, &open);
    let cache = match lookup {
        PartcloneLookup::Ready(cache) => cache,
        PartcloneLookup::NeedsScan { cache, cache_path } => {
            ctx.set_phase("Scanning Clonezilla metadata (first open of this partition)...");
            let ptype = open.partition_type;
            cache_runner::scan_in_job(ctx, Arc::clone(&cache), part_index, ptype, cache_path)?;
            lock_status(store).insert(part_index, Arc::clone(&cache));
            cache
        }
    };
    Ok(commander_source::session_for_partclone_cache(
        cache,
        open.partition_type,
    ))
}

fn finish_open(session: BrowseSession, mut st: BrowseOpenStatus) -> anyhow::Result<Opened> {
    let volume = VolumeInfo {
        label: st.volume_label.clone(),
        fs_type: st.fs_type.clone(),
        total_size: st.total_size,
        used_size: st.used_size,
    };
    if st.needs_password {
        return Ok(Opened::Locked { session, volume });
    }
    if let Some(e) = st.error.take() {
        return Err(anyhow!(e));
    }
    match (st.fs.take(), st.root.take()) {
        (Some(fs), Some(root)) => Ok(Opened::Volume {
            session,
            loaded: Loaded {
                fs,
                root,
                entries: st.root_entries.take().unwrap_or_default(),
                volume,
            },
        }),
        _ => Err(anyhow!(
            "Filesystem opened but no root directory was returned."
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::Controller;
    use std::time::{Duration, Instant};

    type A = CommanderPaneAction;

    /// Ticks until no open runs, returning every effect the ticks produced.
    fn settle(vm: &mut CommanderPaneController) -> Vec<Effect> {
        let start = Instant::now();
        let mut fx = vm.tick();
        while vm.is_opening() {
            assert!(
                start.elapsed() < Duration::from_secs(60),
                "open never finished"
            );
            std::thread::sleep(Duration::from_millis(5));
            fx.extend(vm.tick());
        }
        fx
    }

    fn messages(fx: &[Effect]) -> Vec<String> {
        let text = |e: &Effect| match e {
            Effect::Message(m) => Some(m.clone()),
            _ => None,
        };
        fx.iter().filter_map(text).collect()
    }

    fn fixture(name: &str) -> Vec<u8> {
        use std::io::Read as _;
        let compressed = std::fs::read(format!("tests/fixtures/{name}")).unwrap();
        let mut raw = Vec::new();
        crate::rbformats::zstd_compat::decoder(std::io::Cursor::new(compressed))
            .and_then(|mut d| d.read_to_end(&mut raw))
            .unwrap();
        raw
    }

    /// The FAT16 fixture behind an MBR whose one partition starts at LBA 2048.
    fn mbr_fat16(dir: &Path) -> PathBuf {
        let fat = fixture("test_fat16.img.zst");
        let mut img = vec![0u8; 2048 * 512];
        let entry = &mut img[446..462];
        entry[4] = 0x06;
        entry[8..12].copy_from_slice(&2048u32.to_le_bytes());
        entry[12..16].copy_from_slice(&((fat.len() / 512) as u32).to_le_bytes());
        img[510] = 0x55;
        img[511] = 0xAA;
        img.extend_from_slice(&fat);
        let path = dir.join("disk.img");
        std::fs::write(&path, img).unwrap();
        path
    }

    fn names(vm: &CommanderPaneController) -> Vec<String> {
        vm.listing
            .entries()
            .iter()
            .map(|e| e.name.clone())
            .collect()
    }

    #[test]
    fn opening_a_host_folder_runs_as_a_job() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"x").unwrap();
        let mut vm = CommanderPaneController::new();
        let fx = vm.apply(A::OpenHostFolder(dir.path().to_path_buf()));
        assert_eq!(vm.take_requests(), [PaneRequest::SourceReset]);
        assert!(messages(&fx)[0].starts_with("opening "));
        assert!(vm.is_opening());
        let fx = settle(&mut vm);
        assert!(messages(&fx)[0].contains("(1 item(s))"), "{fx:?}");
        assert_eq!(vm.source_kind(), Some(SourceKind::Host));
        assert!(vm.listing.is_host());
        assert_eq!(names(&vm), ["a.txt"]);
        assert!(vm.opening().is_none());
    }

    #[test]
    fn opening_an_image_probes_partitions_and_selects_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = mbr_fat16(dir.path());
        let mut vm = CommanderPaneController::new();
        vm.apply(A::OpenImage(path.clone()));
        let fx = settle(&mut vm);
        assert!(vm.source_error().is_none(), "{:?}", vm.source_error());
        assert_eq!(vm.source_kind(), Some(SourceKind::Image));
        assert_eq!(vm.source_path(), Some(path.as_path()));
        assert_eq!(
            vm.partition_labels().len(),
            1,
            "{:?}",
            vm.partition_labels()
        );
        assert_eq!(vm.selected_partition(), Some(0));
        assert!(vm.session().is_some());
        assert!(vm.volume().fs_type.starts_with("FAT"), "{:?}", vm.volume());
        assert!(vm.listing.is_loaded() && !vm.listing.is_host());
        assert!(
            messages(&fx).iter().any(|m| m.starts_with("opened ")),
            "{fx:?}"
        );
        // Reselecting the partition already open does nothing.
        assert!(vm.apply(A::SelectPartition(0)).is_empty());
        assert!(!vm.is_opening());
    }

    #[test]
    fn switching_with_staged_edits_asks_first() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        std::fs::write(second.path().join("b.txt"), b"x").unwrap();
        let mut vm = CommanderPaneController::new();
        vm.apply(A::OpenHostFolder(first.path().to_path_buf()));
        settle(&mut vm);
        // An empty queue switches straight away.
        let fx = vm.apply(A::OpenHostFolder(first.path().to_path_buf()));
        assert!(!fx.iter().any(|e| matches!(e, Effect::Confirm(_))));
        settle(&mut vm);
        let root = vm.listing.cwd().unwrap().clone();
        vm.queue.push(StagedEdit::CreateDirectory {
            parent: root,
            name: "new".into(),
            qnx2_attrs: None,
        });
        let fx = vm.apply(A::OpenHostFolder(second.path().to_path_buf()));
        let [Effect::Confirm(spec)] = fx.as_slice() else {
            panic!("expected one Confirm, got {fx:?}");
        };
        assert_eq!(spec.purpose, purpose::DISCARD_EDITS);
        assert!(spec.destructive);
        vm.apply(A::Confirmed {
            purpose: purpose::DISCARD_EDITS,
            yes: false,
        });
        assert_eq!(vm.source_path(), Some(first.path()));
        assert_eq!(vm.queue.len(), 1);
        vm.apply(A::OpenHostFolder(second.path().to_path_buf()));
        vm.apply(A::Confirmed {
            purpose: purpose::DISCARD_EDITS,
            yes: true,
        });
        settle(&mut vm);
        assert_eq!(vm.source_path(), Some(second.path()));
        assert!(vm.queue.is_empty());
        assert_eq!(names(&vm), ["b.txt"]);
    }

    #[test]
    fn picker_effects_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut vm = CommanderPaneController::new();
        let fx = vm.apply(A::PickHostFolder);
        let [Effect::PickFolder(spec)] = fx.as_slice() else {
            panic!("expected PickFolder, got {fx:?}");
        };
        assert_eq!(spec.purpose, purpose::OPEN_HOST_FOLDER);
        let fx = vm.apply(A::PickImage);
        let [Effect::PickFile(spec)] = fx.as_slice() else {
            panic!("expected PickFile, got {fx:?}");
        };
        assert!(spec.filters.iter().any(|(n, _)| n == "Disk Images"));
        assert!(vm
            .apply(A::Picked {
                purpose: purpose::OPEN_IMAGE,
                path: None
            })
            .is_empty());
        assert!(!vm.is_opening());
        vm.apply(A::Picked {
            purpose: purpose::OPEN_HOST_FOLDER,
            path: Some(dir.path().to_path_buf()),
        });
        settle(&mut vm);
        assert_eq!(vm.source_kind(), Some(SourceKind::Host));
        assert_eq!(vm.source_path(), Some(dir.path()));
    }

    #[test]
    fn a_failed_open_reports_and_leaves_no_source() {
        let dir = tempfile::tempdir().unwrap();
        let mut vm = CommanderPaneController::new();
        vm.apply(A::OpenImage(dir.path().join("missing.img")));
        let fx = settle(&mut vm);
        assert_eq!(messages(&fx), ["open failed."]);
        assert!(vm.source_error().is_some());
        assert!(vm.source_path().is_none());
        assert!(vm.source_kind().is_none());
        assert!(!vm.listing.is_loaded());
    }

    #[test]
    fn an_open_waits_for_the_one_it_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let mut vm = CommanderPaneController::new();
        vm.apply(A::OpenHostFolder(dir.path().to_path_buf()));
        vm.apply(A::OpenHostFolder(dir.path().to_path_buf()));
        settle(&mut vm);
        assert!(vm.source_error().is_none(), "{:?}", vm.source_error());
        assert_eq!(vm.source_kind(), Some(SourceKind::Host));
    }

    #[test]
    fn close_drops_the_source() {
        let dir = tempfile::tempdir().unwrap();
        let mut vm = CommanderPaneController::new();
        vm.apply(A::OpenHostFolder(dir.path().to_path_buf()));
        settle(&mut vm);
        vm.take_requests();
        assert_eq!(messages(&vm.apply(A::CloseSource)), ["closed."]);
        assert_eq!(vm.take_requests(), [PaneRequest::SourceReset]);
        assert!(vm.source_path().is_none() && !vm.listing.is_loaded());
    }

    #[cfg(feature = "crypto")]
    #[test]
    fn a_locked_source_asks_for_its_password() {
        let doc = std::fs::read_to_string("tests/fixtures/apfs/ENCRYPTED_FIXTURE.md").unwrap();
        let pass = doc
            .lines()
            .find_map(|l| l.strip_prefix("- **Passphrase:** `"))
            .and_then(|r| r.split('`').next())
            .unwrap()
            .to_string();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vault.img");
        std::fs::write(&path, fixture("test_apfs_encrypted.img.zst")).unwrap();
        let mut vm = CommanderPaneController::new();
        vm.apply(A::OpenImage(path));
        settle(&mut vm);
        assert!(vm.needs_password(), "error: {:?}", vm.source_error());
        assert_eq!(vm.volume().label, "RBBASE");
        vm.apply(A::SubmitPassword("wrong".into()));
        settle(&mut vm);
        assert!(vm.needs_password(), "a wrong passphrase asks again");
        vm.apply(A::SubmitPassword(pass));
        settle(&mut vm);
        assert!(!vm.needs_password(), "error: {:?}", vm.source_error());
        assert!(
            names(&vm).iter().any(|n| n == "hello.txt"),
            "{:?}",
            names(&vm)
        );
    }
}
