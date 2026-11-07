//! The Inspect tab: a retained view over `controller::inspect::InspectController`.
//!
//! See docs/gtk4_shell_plan.md for the pattern. `src/gui/inspect_tab.rs` is the reference; the
//! dialogs are later slices, so their buttons stay greyed out with a tooltip saying so. The browse
//! view takes the whole tab while it is open.

pub mod bar;
mod details;
mod table;
mod tools;

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::{Rc, Weak};
use std::sync::Arc;

use adw::prelude::*;
use rusty_backup::controller::app::{
    unelevated_device_note, DiskDevice, Notify, RecentHandle, RecentMode, Snapshot,
};
use rusty_backup::controller::inspect::{
    ids, purpose, AlignmentSummary, BackupSummary, ClonezillaSummary, HostRequest, InspectAction,
    InspectController, InspectServices, InspectSource, JobRegistry, LayoutSegment, PartitionRow,
    TableSummary,
};
use rusty_backup::controller::keys::{Key, Mods};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{Controller, Effect};

use crate::browser::BrowseView;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::progress::JobList;
use crate::source::{Pick, PickerSpec, SourcePicker};
use crate::sync::SyncGuard;
use crate::widgets::set_text;

use bar::PartitionBar;
use details::{show_note, Details};
use table::PartitionTable;
use tools::{ToolBar, ToolState};

/// Sends an action to the controller; a change a sync made is dropped here, for every widget.
type ActionFn = Rc<dyn Fn(InspectAction)>;

pub struct InspectScreen {
    ctrl: RefCell<InspectController>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    devices: RefCell<Vec<DiskDevice>>,
    /// The user closed a backup here, so the app drops the shared folder.
    backup_closed: Cell<bool>,
    sync: SyncGuard,
    me: Weak<InspectScreen>,
    root: gtk::Stack,
    browse: Rc<BrowseView>,
    source: Rc<SourcePicker>,
    elevation: gtk::Label,
    flux: gtk::Label,
    flux_sectors: gtk::Expander,
    flux_list: gtk::Label,
    jobs: JobList,
    tools: ToolBar,
    error: gtk::Label,
    details: Details,
    layout: gtk::Expander,
    bar: PartitionBar,
    table: Rc<PartitionTable>,
    no_partitions: gtk::Label,
}

/// Everything a refresh shows, read while the controller is borrowed and shown after it is not.
struct Shown {
    source: String,
    flux: Option<(String, Vec<String>)>,
    jobs: Vec<(Snapshot, bool)>,
    tools: ToolState,
    error: Option<String>,
    backup: Option<BackupSummary>,
    clonezilla: Option<ClonezillaSummary>,
    table: Option<TableSummary>,
    alignment: Option<AlignmentSummary>,
    swabbing: bool,
    segments: Vec<LayoutSegment>,
    selected: Option<usize>,
    rows: Vec<PartitionRow>,
    shows_min: bool,
}

/// Carry out `request`; what this view cannot do yet says so.
/// No `_` arm: a new request fails to compile until this view decides what it means.
fn carry_out(screen: &InspectScreen, request: HostRequest) -> Option<String> {
    let browse = &screen.browse;
    match request {
        HostRequest::CloseBrowse => {
            browse.close();
            None
        }
        HostRequest::UpgradeSeekable(path) => {
            browse.upgrade_seekable(path);
            None
        }
        HostRequest::OpenBrowse(open) => {
            browse.open_request(*open);
            None
        }
        HostRequest::BackupClosed => {
            screen.backup_closed.set(true);
            // Nothing else wakes the app, and the backup closes on Restore too.
            gtk::glib::idle_add_local_once(crate::app::tick);
            None
        }
        HostRequest::ExpandHfs(_) => Some("The Expand HFS dialog is coming soon.".to_string()),
        HostRequest::OpenOptical(path) => {
            crate::app::open_in_optical(path);
            None
        }
        HostRequest::OpenArchive(path) => {
            crate::app::open_in_archives(path);
            None
        }
    }
}

fn warning_label() -> gtk::Label {
    let label = gtk::Label::builder().xalign(0.0).wrap(true).build();
    label.add_css_class("warning");
    label
}

impl InspectScreen {
    pub fn build(notify: Arc<dyn Notify>, recent: RecentHandle, host: EffectHost) -> Rc<Self> {
        Rc::new_cyclic(|me: &Weak<InspectScreen>| {
            let target = me.clone();
            let send: ActionFn = Rc::new(move |action| {
                let Some(screen) = target.upgrade() else {
                    return;
                };
                if !screen.sync.active() {
                    screen.apply(action);
                }
            });

            let source = SourcePicker::build(PickerSpec {
                source: ids::SOURCE,
                recent: ids::SOURCE_RECENT,
                device: ids::SOURCE_DEVICE,
                open_file: Some(ids::OPEN_FILE),
                open_folder: None,
                open_backup: Some(ids::OPEN_BACKUP),
                connect: Some(ids::CONNECT_REMOTE),
                connect_live: false,
                no_devices: None,
                width_chars: 44,
                caption: Some("Source:"),
            });
            let target = me.clone();
            source.connect_pick(move |pick| {
                if let Some(screen) = target.upgrade() {
                    screen.picked_source(pick);
                }
            });

            let elevation = warning_label();
            let flux = warning_label();
            let flux_list = gtk::Label::builder().xalign(0.0).selectable(true).build();
            let flux_scroll = gtk::ScrolledWindow::builder()
                .child(&flux_list)
                .max_content_height(160)
                .propagate_natural_height(true)
                .build();
            let flux_sectors = gtk::Expander::builder().child(&flux_scroll).build();

            let cancel = send.clone();
            let jobs = JobList::new(
                ids::CANCEL_EXPORT,
                Rc::new(move || cancel(InspectAction::CancelExport)),
            );
            let tools = ToolBar::build(&send);
            let error = gtk::Label::builder().xalign(0.0).wrap(true).build();
            error.add_css_class("error");
            let details = Details::build(&send);

            let select = send.clone();
            let bar = PartitionBar::build(move |p| select(InspectAction::SelectPartition(p)));
            let layout = gtk::Expander::builder()
                .label("Disk layout")
                .expanded(true)
                .child(bar.root())
                .build();
            let table = PartitionTable::build(&send);
            let no_partitions = gtk::Label::builder()
                .label("No partitions found.")
                .xalign(0.0)
                .build();

            let page = gtk::Box::builder()
                .orientation(gtk::Orientation::Vertical)
                .spacing(8)
                .margin_top(12)
                .margin_bottom(12)
                .margin_start(12)
                .margin_end(12)
                .build();
            page.append(source.root());
            page.append(&elevation);
            page.append(&flux);
            page.append(&flux_sectors);
            page.append(jobs.root());
            page.append(tools.root());
            page.append(&error);
            page.append(details.root());
            page.append(&layout);
            page.append(table.root());
            page.append(&no_partitions);
            let scroll = gtk::ScrolledWindow::builder()
                .child(&page)
                .hscrollbar_policy(gtk::PolicyType::Never)
                .build();
            let browse = BrowseView::build(notify.clone(), host.clone());
            let target = me.clone();
            browse.on_change(Rc::new(move || {
                if let Some(screen) = target.upgrade() {
                    screen.show_page();
                }
            }));
            let root = gtk::Stack::new();
            root.add_named(&scroll, Some("inspect"));
            // Clipped: the edit bar's extra buttons wrap rather than widen the window.
            let clip = gtk::ScrolledWindow::builder()
                .child(browse.root())
                .hscrollbar_policy(gtk::PolicyType::External)
                .vscrollbar_policy(gtk::PolicyType::Never)
                .build();
            root.add_named(&clip, Some("browse"));

            let recent_files = recent.list(RecentMode::Inspect);
            InspectScreen {
                ctrl: RefCell::new(InspectController::new(InspectServices {
                    jobs: JobRegistry::new(),
                    notify,
                })),
                host,
                recent,
                recent_files: RefCell::new(recent_files),
                devices: RefCell::new(Vec::new()),
                backup_closed: Cell::new(false),
                sync: SyncGuard::default(),
                me: me.clone(),
                root,
                browse,
                source,
                elevation,
                flux,
                flux_sectors,
                flux_list,
                jobs,
                tools,
                error,
                details,
                layout,
                bar,
                table,
                no_partitions,
            }
        })
    }

    pub fn root(&self) -> &gtk::Stack {
        &self.root
    }

    /// The browse view, while it is open, in the tab's place.
    fn show_page(&self) {
        let page = if self.browse.is_active() {
            "browse"
        } else {
            "inspect"
        };
        if self.root.visible_child_name().as_deref() != Some(page) {
            self.root.set_visible_child_name(page);
        }
    }

    /// A window-wide key: the browse view's, while it is open.
    pub fn key(&self, key: Key, mods: Mods) -> bool {
        self.browse.key(key, mods)
    }

    pub fn apply(&self, action: InspectAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        self.browse.tick();
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// Open an image named outside the picker (the command line); a wrapper is decoded first.
    pub fn open_file(&self, path: PathBuf) {
        self.remember(&path);
        self.apply(InspectAction::Load(InspectSource::ImageFile(path)));
    }

    /// The app's device list; a change under the open device closes or re-inspects it.
    pub fn set_devices(&self, devices: Vec<DiskDevice>) {
        self.ctrl.borrow_mut().reconcile_devices(&devices);
        *self.devices.borrow_mut() = devices;
    }

    /// The backup folder open here, for the share with Restore.
    pub fn loaded_backup(&self) -> Option<PathBuf> {
        self.ctrl.borrow().backup_folder().map(Path::to_path_buf)
    }

    /// Nothing is open, so a shared backup may be adopted.
    pub fn is_empty(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        ctrl.backup_folder().is_none() && !ctrl.has_local_source()
    }

    /// Open the shared backup folder.
    pub fn load_backup(&self, folder: &Path) {
        if self.ctrl.borrow().backup_folder() != Some(folder) {
            self.apply(InspectAction::Load(InspectSource::Backup {
                folder: folder.to_path_buf(),
            }));
        }
    }

    /// Close Backup from elsewhere: the app already dropped the shared folder.
    pub fn clear_backup(&self) {
        if self.ctrl.borrow().backup_folder().is_some() {
            self.apply(InspectAction::Unload { quiet: true });
            self.backup_closed.set(false);
        }
    }

    /// The user closed a backup here; asked once.
    pub fn take_backup_closed(&self) -> bool {
        self.backup_closed.replace(false)
    }

    /// The disk job running here, by name, for the other tabs' locks.
    pub fn running_job(&self) -> Option<&'static str> {
        self.ctrl.borrow().running_job()
    }

    fn remember(&self, path: &std::path::Path) {
        let list = self
            .recent
            .remember(RecentMode::Inspect, &path.display().to_string());
        *self.recent_files.borrow_mut() = list;
    }

    fn picked_source(&self, pick: Pick) {
        let action = match pick {
            Pick::Recent(path) => {
                self.remember(&path);
                InspectAction::Open(InspectSource::ImageFile(path))
            }
            Pick::Device(slot) => {
                let identity = self.devices.borrow().get(slot).map(DiskDevice::identity);
                let Some(identity) = identity else {
                    return;
                };
                InspectAction::Open(InspectSource::Device { slot, identity })
            }
            Pick::OpenFile => InspectAction::ChooseImage,
            Pick::OpenBackup => InspectAction::ChooseBackupFolder,
            // Inspect's picker offers neither: no folder source, and remote is slice 7.
            Pick::OpenFolder | Pick::Connect => return,
        };
        self.apply(action);
    }

    /// Answers to this controller's own questions.
    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            if let Some(screen) = me.upgrade() {
                screen.answered(reply);
            }
        })
    }

    fn answered(&self, reply: Reply) {
        let action = match reply {
            Reply::Picked { purpose, path } => {
                if let Some(image) = path.as_ref().filter(|_| purpose == purpose::OPEN_IMAGE) {
                    self.remember(image);
                }
                InspectAction::Picked { purpose, path }
            }
            Reply::Confirmed { purpose, yes } => InspectAction::Confirmed { purpose, yes },
            // Inspect asks for neither text nor a choice and has no screen to leave.
            Reply::Answered { .. } | Reply::Chose { .. } | Reply::CloseScreen => return,
        };
        self.apply(action);
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let dirty = self.browse.has_unsaved_edits();
        let (requests, mut log) = {
            let mut ctrl = self.ctrl.borrow_mut();
            ctrl.set_browse_dirty(dirty);
            (ctrl.take_requests(), ctrl.take_log())
        };
        let refused = requests.into_iter().filter_map(|r| carry_out(self, r));
        log.extend(refused.map(|line| (LogLevel::Warning, line)));
        crate::app::append_log(log);
        self.refresh();
        self.browse.set_host_jobs(self.ctrl.borrow().browse_jobs());
        if let Some(load) = self.browse.take_nested_load() {
            self.apply(load);
        }
    }

    fn read(&self) -> Shown {
        let devices = self.devices.borrow();
        let mut ctrl = self.ctrl.borrow_mut();
        let mut jobs = Vec::new();
        jobs.extend(ctrl.loading().map(|s| (s, false)));
        jobs.extend(ctrl.export_job().map(|s| (s, true)));
        jobs.extend(ctrl.tool_jobs().into_iter().map(|s| (s, false)));
        jobs.extend(ctrl.browse_jobs().into_iter().map(|s| (s, false)));
        let segments = if ctrl.partitions().is_empty() {
            Vec::new()
        } else {
            ctrl.layout()
        };
        Shown {
            source: ctrl.source_label(devices.as_slice()),
            flux: ctrl.flux_faults().map(|f| (f.summary, f.sectors)),
            jobs,
            tools: ToolState {
                can_reinspect: ctrl.can_reinspect(),
                can_make_bootable: ctrl.can_make_bootable(),
                can_close: ctrl.can_close(),
                has_chd: ctrl.chd_path().is_some(),
                is_floppy: ctrl.floppy_convert_source().is_some(),
            },
            error: ctrl.last_error().map(str::to_string),
            backup: ctrl.backup_summary(),
            clonezilla: ctrl.clonezilla_summary(),
            table: ctrl.table_summary(),
            alignment: ctrl.alignment(),
            swabbing: ctrl.swabbing(),
            segments,
            selected: ctrl.selected_partition(),
            rows: ctrl.rows(),
            shows_min: ctrl.shows_min_columns(),
        }
    }

    /// Read the controller first and release it: setting a widget can fire its change signal.
    fn refresh(&self) {
        let shown = self.read();
        self.sync.run(|| self.show(shown));
    }

    fn show(&self, s: Shown) {
        let devices: Vec<String> = self
            .devices
            .borrow()
            .iter()
            .map(|d| d.display_name())
            .collect();
        self.source
            .show(&s.source, &self.recent_files.borrow(), &devices);

        let note = InspectController::needs_elevation_note().then(unelevated_device_note);
        show_note(&self.elevation, note);
        let flux = s.flux.as_ref().map(|(summary, _)| {
            format!("Warning: flux decode: {summary}. Files using these sectors are damaged.")
        });
        show_note(&self.flux, flux.as_deref());
        self.flux_sectors.set_visible(s.flux.is_some());
        if let Some((_, sectors)) = &s.flux {
            let title = format!("Unreadable sectors ({})", sectors.len());
            if self.flux_sectors.label().as_deref() != Some(title.as_str()) {
                self.flux_sectors.set_label(Some(&title));
            }
            set_text(&self.flux_list, &sectors.join("\n"));
        }

        self.jobs.show(&s.jobs);
        self.tools.show(&s.tools);
        let error = s.error.as_ref().map(|e| format!("Error: {e}"));
        show_note(&self.error, error.as_deref());
        self.details.show(
            s.backup.as_ref(),
            s.clonezilla.as_ref(),
            s.table.as_ref(),
            s.alignment.as_ref(),
            s.swabbing,
        );

        let has_rows = !s.rows.is_empty();
        self.layout.set_visible(s.table.is_some() && has_rows);
        self.no_partitions
            .set_visible(s.table.is_some() && !has_rows);
        self.table.root().set_visible(has_rows);
        let slots = s.table.as_ref().is_some_and(|t| t.has_slots);
        self.bar.show(s.segments, s.selected);
        self.table.show(s.rows, s.selected, s.shows_min, slots);
    }
}
