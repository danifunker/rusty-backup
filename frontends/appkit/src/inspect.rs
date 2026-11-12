//! The Inspect screen over `controller::inspect::InspectController`; the Win32 shell's
//! `inspect.rs` is the reference, `src/gui/inspect_tab.rs` the original.
//!
//! Slice 1: the source (a pull-down of recent files and devices, beside Open File and Open Backup
//! Folder), the facts, the running job, the two rows of tools, the partition bar, the partition
//! list with the tools that act on its selected row, and the Check and CHD Info windows. The
//! tools whose windows later slices build show but stay off, saying which slice brings them.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use objc2::rc::Retained;
use objc2::{sel, MainThreadMarker, Message};
use objc2_app_kit::{
    NSButton, NSMenu, NSMenuItem, NSPopUpButton, NSProgressIndicator, NSTextField,
    NSUserInterfaceItemIdentification, NSView,
};
use objc2_foundation::NSRect;
use rusty_backup::controller::app::{
    unelevated_device_note, DiskDevice, Notify, Progress, RecentHandle, RecentMode, Snapshot,
};
use rusty_backup::controller::inspect::{
    format_size, ids, purpose, HostRequest, InspectAction, InspectController, InspectServices,
    InspectSource, JobRegistry, LayoutSegment, MinCell, PartitionRow,
};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::remote_pick::{
    hints as remote_hints, ids as remote_ids, RemotePickAction,
};
use rusty_backup::controller::{Controller, Effect};

use crate::alto::AltoWindow;
use crate::browse::BrowseView;
use crate::disk_write::DiskWriteWindow;
use crate::editor::EditorWindow;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::expand_hfs::ExpandHfsWindow;
use crate::expand_image::{self, ExpandImageWindow};
use crate::export::{self, ExportWindow};
use crate::layout::{self, Node, Size};
use crate::partition_bar::{self, Bar};
use crate::remote_pick::RemotePage;
use crate::report::{self, ReportWindow};
use crate::resize::ResizeWindow;
use crate::sync::SyncGuard;
use crate::table::Table;
use crate::ui::{self, ns};

/// The list's columns: title, width in points, right-aligned.
const COLUMNS: [(&str, f64, bool); 7] = [
    ("#", 50.0, false),
    ("Type", 200.0, false),
    ("Volume", 160.0, false),
    ("Start LBA", 100.0, true),
    ("Size", 100.0, true),
    ("Min Size", 100.0, true),
    ("Block", 70.0, true),
];

/// Recent files the source menu lists, as egui's picker does.
pub const RECENT_ROWS: usize = 5;

/// The source menu's item ids: a recent file or a device by its place in the list.
pub const RECENT_ITEM: &str = "inspect.source.recent.";
pub const DEVICE_ITEM: &str = "inspect.source.device.";

/// The Check and CHD Info windows' Close buttons.
const CHECK_CLOSE: &str = "inspect.check.close";
const CHD_CLOSE: &str = "inspect.chd_info.close";

/// Tools whose windows a later slice builds: they show, stay off and say when they come.
const LATER: [(&str, &str); 1] = [(ids::CONVERT, "Arrives in slice 7c (the Convert screen).")];

fn later(id: &str) -> Option<&'static str> {
    LATER.iter().find(|(i, _)| *i == id).map(|(_, why)| *why)
}

fn cell(row: &PartitionRow, column: usize) -> String {
    match column {
        0 => row.number.clone(),
        1 => row.type_name.clone(),
        2 => row.volume.clone(),
        3 => row.start_lba.to_string(),
        4 => format_size(row.size_bytes),
        5 => match &row.min {
            MinCell::Size(size) => format_size(*size),
            MinCell::Pending(phase) => phase.clone(),
            MinCell::CanCalc | MinCell::Empty => String::new(),
        },
        _ => row.block_size.clone(),
    }
}

/// A job's one-line summary (what runs, its phase, how far) and its percentage when known.
pub fn job_line(s: &Snapshot) -> (String, Option<u32>) {
    let pct = match &s.progress {
        Progress::Determinate { done, total, .. } if *total > 0 => {
            Some((done.saturating_mul(100) / total).min(100) as u32)
        }
        _ => None,
    };
    let mut line = s.label.clone();
    if !s.phase.is_empty() {
        line.push_str(": ");
        line.push_str(&s.phase);
    }
    if let Some(p) = pct {
        line.push_str(&format!(" ({p}%)"));
    }
    (line, pct)
}

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    /// An export is running, so Cancel Export shows.
    exporting: bool,
    /// A remote image is open, and whether its backup runs.
    remote_open: bool,
    remote_backing_up: bool,
    source: String,
    error: Option<String>,
    flux: Option<String>,
    jobs: Vec<Snapshot>,
    details: Vec<String>,
    has_table: bool,
    rows: Vec<PartitionRow>,
    segments: Vec<LayoutSegment>,
    selected: Option<usize>,
    can_reinspect: bool,
    can_bootable: bool,
    can_close: bool,
    has_chd: bool,
    can_convert: bool,
    check: Option<report::Shown>,
    chd: Option<String>,
}

/// What each view of the page is, for layout.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Part {
    SourceCaption,
    Source,
    OpenFile,
    OpenBackup,
    Connect,
    PickAnother,
    BackupRemote,
    BackingUp,
    CloseRemote,
    Note,
    Flux,
    Error,
    Job,
    Progress,
    CancelExport,
    Reinspect,
    Export,
    Convert,
    Chd,
    Close,
    EditTable,
    AddPartition,
    Resize,
    ExpandImage,
    Bootable,
    Details,
    Bar,
    Table,
    NoPartitions,
    Browse,
    CalcMin,
    Check,
    Expand,
    Defragment,
    ResizeAlto,
}

pub struct InspectScreen {
    mtm: MainThreadMarker,
    ctrl: RefCell<InspectController>,
    notify: Arc<dyn Notify>,
    /// Expand HFS Volume, while its controller has a source.
    hfs: RefCell<Option<Rc<ExpandHfsWindow>>>,
    /// Resize Partitions and Resize Alto Disk, while their controllers are open.
    resize: RefCell<Option<Rc<ResizeWindow>>>,
    alto: RefCell<Option<Rc<AltoWindow>>>,
    /// Expand Image's window, which the view holds.
    expand_window: RefCell<Option<Rc<ExpandImageWindow>>>,
    /// The table editor, open while the controller hands out its view.
    editor_window: RefCell<Option<Rc<EditorWindow>>>,
    /// The daemon's listing, standing in Inspect's place while it is active.
    pub remote: Rc<RemotePage>,
    page_changed: RefCell<Option<Rc<dyn Fn()>>>,
    /// Export, open while the controller says so, and its Physical Disk Export's writer.
    export_window: RefCell<Option<Rc<ExportWindow>>>,
    writer: RefCell<Option<Rc<DiskWriteWindow>>>,
    host: EffectHost,
    recent: RecentHandle,
    devices: RefCell<Vec<DiskDevice>>,
    /// The recent files and devices the source menu was last built from.
    menu_built: RefCell<(Vec<String>, Vec<String>)>,
    backup_closed: Cell<bool>,
    /// A disc or archive Inspect handed to the Optical or Archives screen, once.
    optical: RefCell<Option<PathBuf>>,
    archive: RefCell<Option<PathBuf>>,
    sync: SyncGuard,
    me: Weak<InspectScreen>,
    body: Retained<NSView>,
    parts: Vec<(Part, Retained<NSView>)>,
    source: Retained<NSPopUpButton>,
    note: Retained<NSTextField>,
    flux: Retained<NSTextField>,
    error: Retained<NSTextField>,
    job: Retained<NSTextField>,
    progress: Retained<NSProgressIndicator>,
    details: Retained<NSTextField>,
    bar: Retained<Bar>,
    table: Table,
    rows: RefCell<Vec<PartitionRow>>,
    /// The row the tools act on: the list's selection, else the first row (as the TUI's cursor).
    cursor: Cell<usize>,
    check_window: RefCell<Option<ReportWindow>>,
    chd_window: RefCell<Option<ReportWindow>>,
    /// The browse view, shown in the page's place while it is open.
    pub browse: Rc<BrowseView>,
}

fn view<T: AsRef<NSView>>(v: &T) -> Retained<NSView> {
    v.as_ref().retain()
}

impl InspectScreen {
    pub fn build(
        mtm: MainThreadMarker,
        body: Retained<NSView>,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<Self> {
        let button = |title: &str, id: &str| -> Retained<NSButton> {
            match later(id) {
                Some(why) => ui::later(mtm, title, id, why),
                None => ui::button(mtm, title, id),
            }
        };
        let source = NSPopUpButton::initWithFrame_pullsDown(mtm.alloc(), NSRect::ZERO, true);
        ui::set_id(&source, ids::SOURCE);
        let note = ui::label(mtm, "");
        let flux = ui::label(mtm, "");
        let error = ui::label(mtm, "");
        let job = ui::label(mtm, "");
        let progress = ui::progress(mtm);
        let details = ui::label(mtm, "");
        details.setSelectable(true);
        let bar = Bar::new(mtm, ids::PARTITION_BAR);
        let table = Table::new(mtm, ids::PARTITION_TABLE, &COLUMNS);
        let no_partitions = ui::label(mtm, "No partitions found.");
        let parts: Vec<(Part, Retained<NSView>)> = vec![
            (Part::SourceCaption, view(&ui::label(mtm, "Source:"))),
            (Part::Source, view(&source)),
            (
                Part::OpenFile,
                view(&button("Open File...", ids::OPEN_FILE)),
            ),
            (
                Part::OpenBackup,
                view(&button("Open Backup Folder...", ids::OPEN_BACKUP)),
            ),
            (
                Part::Connect,
                view(&button("Connect to Remote...", ids::CONNECT_REMOTE)),
            ),
            (
                Part::PickAnother,
                view(&button("Pick Another Image", remote_ids::PICK_ANOTHER)),
            ),
            (
                Part::BackupRemote,
                view(&button("Back Up Image...", remote_ids::BACKUP)),
            ),
            (
                Part::BackingUp,
                view(&ui::label(mtm, remote_hints::BACKING_UP)),
            ),
            (
                Part::CloseRemote,
                view(&button("Close Remote", remote_ids::CLOSE)),
            ),
            (Part::Note, view(&note)),
            (Part::Flux, view(&flux)),
            (Part::Error, view(&error)),
            (Part::Job, view(&job)),
            (Part::Progress, view(&progress)),
            (
                Part::CancelExport,
                view(&button("Cancel Export", ids::CANCEL_EXPORT)),
            ),
            (Part::Reinspect, view(&button("Re-inspect", ids::REINSPECT))),
            (
                Part::Export,
                view(&button("Export Disk Image...", ids::EXPORT)),
            ),
            (Part::Convert, view(&button("Convert...", ids::CONVERT))),
            (Part::Chd, view(&button("CHD Info", ids::CHD_INFO))),
            (Part::Close, view(&button("Close", ids::CLOSE))),
            (
                Part::EditTable,
                view(&button("Edit Partition Table...", ids::EDIT_TABLE)),
            ),
            (
                Part::AddPartition,
                view(&button("Add Partition...", ids::ADD_PARTITION)),
            ),
            (
                Part::Resize,
                view(&button("Resize Partitions...", ids::RESIZE)),
            ),
            (
                Part::ExpandImage,
                view(&button("Expand Image...", ids::EXPAND_IMAGE)),
            ),
            (
                Part::Bootable,
                view(&button("Make Bootable...", ids::MAKE_BOOTABLE)),
            ),
            (Part::Details, view(&details)),
            (Part::Bar, view(&bar)),
            (Part::Table, view(&table.scroll)),
            (Part::NoPartitions, view(&no_partitions)),
            (Part::Browse, view(&button("Browse", ids::ROW_BROWSE))),
            (Part::CalcMin, view(&button("Calc min", ids::ROW_CALC_MIN))),
            (Part::Check, view(&button("Check", ids::ROW_CHECK))),
            (
                Part::Expand,
                view(&button("Expand/Export...", ids::ROW_EXPAND_HFS)),
            ),
            (
                Part::Defragment,
                view(&button("Defragment...", ids::ROW_DEFRAGMENT)),
            ),
            (
                Part::ResizeAlto,
                view(&button("Resize...", ids::ROW_RESIZE_ALTO)),
            ),
        ];
        for (_, v) in &parts {
            body.addSubview(v);
        }
        let browse = BrowseView::build(mtm, notify.clone(), host.clone(), false);
        let remote = RemotePage::build(mtm, notify.clone(), host.clone());
        let screen = Rc::new_cyclic(|me: &Weak<InspectScreen>| InspectScreen {
            mtm,
            ctrl: RefCell::new(InspectController::new(InspectServices {
                jobs: JobRegistry::new(),
                notify: notify.clone(),
            })),
            notify,
            hfs: RefCell::default(),
            resize: RefCell::default(),
            alto: RefCell::default(),
            expand_window: RefCell::default(),
            editor_window: RefCell::default(),
            export_window: RefCell::default(),
            page_changed: RefCell::default(),
            writer: RefCell::default(),
            host,
            recent,
            devices: RefCell::default(),
            menu_built: RefCell::new((vec!["-".to_string()], Vec::new())),
            backup_closed: Cell::new(false),
            optical: RefCell::default(),
            archive: RefCell::default(),
            sync: SyncGuard::default(),
            me: me.clone(),
            body,
            parts,
            source,
            note,
            flux,
            error,
            job,
            progress,
            details,
            bar,
            table,
            rows: RefCell::default(),
            cursor: Cell::new(0),
            check_window: RefCell::default(),
            chd_window: RefCell::default(),
            browse,
            remote,
        });
        let me = screen.me.clone();
        screen.remote.on_change(Rc::new(move || {
            if let Some(s) = me.upgrade() {
                s.remote_changed();
            }
        }));
        let me = screen.me.clone();
        screen.bar.on_select(Rc::new(move |p| {
            if let Some(s) = me.upgrade() {
                s.apply(InspectAction::SelectPartition(p));
            }
        }));
        let me = screen.me.clone();
        screen.table.on_select(Rc::new(move |i| {
            if let Some(s) = me.upgrade() {
                s.row_selected(i);
            }
        }));
        screen.refresh();
        screen
    }

    fn part(&self, p: Part) -> &NSView {
        self.parts
            .iter()
            .find(|(q, _)| *q == p)
            .map(|(_, v)| &**v)
            .expect("every part is built")
    }

    fn button(&self, p: Part) -> Option<&NSButton> {
        self.part(p).downcast_ref::<NSButton>()
    }

    /// Enable a tool, unless a later slice brings its window.
    fn set_enabled(&self, p: Part, on: bool) {
        if let Some(b) = self.button(p) {
            let id =
                objc2_app_kit::NSAccessibility::accessibilityIdentifier(b).map(|i| i.to_string());
            if later(id.as_deref().unwrap_or("")).is_none() {
                ui::set_enabled(b, on);
            }
        }
    }

    pub fn apply(&self, action: InspectAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        self.browse.tick();
        self.remote.tick();
        let hfs = self.hfs.borrow().clone();
        if let Some(d) = hfs {
            d.tick();
            if !d.is_open() {
                self.hfs.replace(None);
            }
        }
        let resize = self.resize.borrow().clone();
        if let Some(d) = resize {
            d.tick();
            if d.take_changed() {
                self.apply(InspectAction::DiskChanged);
            }
            if !d.is_open() {
                self.resize.replace(None);
            }
        }
        let alto = self.alto.borrow().clone();
        if let Some(d) = alto {
            d.tick();
            if !d.is_open() {
                self.alto.replace(None);
            }
        }
        let writer = self.writer.borrow().clone();
        if let Some(d) = writer {
            d.tick();
            if !d.is_open() {
                self.writer.replace(None);
            }
        }
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// The app's device list; a change under the open device closes or re-inspects it.
    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        let resize = self.resize.borrow().clone();
        if let Some(d) = resize {
            d.set_devices(devices);
        }
        let writer = self.writer.borrow().clone();
        if let Some(d) = writer {
            d.set_devices(devices);
        }
        *self.devices.borrow_mut() = devices.to_vec();
    }

    /// The backup folder open here, for the share with Restore.
    pub fn loaded_backup(&self) -> Option<PathBuf> {
        self.ctrl.borrow().backup_folder().map(PathBuf::from)
    }

    /// The optical image Inspect asked the Optical screen to open, once.
    pub fn take_optical(&self) -> Option<PathBuf> {
        self.optical.borrow_mut().take()
    }

    /// The Mac archive Inspect asked the Archives screen to open, once.
    pub fn take_archive(&self) -> Option<PathBuf> {
        self.archive.borrow_mut().take()
    }

    pub fn take_backup_closed(&self) -> bool {
        self.backup_closed.replace(false)
    }

    /// The remote picker changed: log its lines and open a picked image.
    fn remote_changed(&self) {
        let (lines, picked) = self.remote.take();
        let log = lines
            .into_iter()
            .map(|l| (LogLevel::Info, format!("Remote: {l}")));
        crate::app::append_log(log.collect());
        if let Some(conn) = picked {
            self.apply(InspectAction::Load(InspectSource::Remote { conn }));
        }
        self.on_page_change();
    }

    /// The page on show may have changed (browse view, remote listing, screen): tell the app.
    fn on_page_change(&self) {
        if let Some(f) = self.page_changed.borrow().clone() {
            f();
        }
    }

    /// The app's hook for a change of which page stands in Inspect's place.
    pub fn on_page(&self, f: Rc<dyn Fn()>) {
        *self.page_changed.borrow_mut() = Some(f);
    }

    /// Resize Partitions: a dialog with its own controller, on the table read here.
    fn open_resize(&self) {
        let busy = self.resize.borrow().as_ref().is_some_and(|d| d.is_open());
        let plan = self.ctrl.borrow().resize_plan();
        if let (false, Some(plan)) = (busy, plan) {
            let d = ResizeWindow::open(self.mtm, self.notify.clone(), self.host.clone(), plan);
            d.set_devices(&self.devices.borrow());
            *self.resize.borrow_mut() = Some(d);
        }
    }

    /// Resize Alto Disk: a dialog with its own controller, on the open image file.
    fn open_alto(&self) {
        if self.alto.borrow().is_some() {
            return;
        }
        let source = self.ctrl.borrow().alto_resize_source();
        match source {
            Ok(path) => {
                let d = AltoWindow::open(self.mtm, self.notify.clone(), self.host.clone(), path);
                *self.alto.borrow_mut() = Some(d);
            }
            Err(e) => crate::app::append_log(vec![(LogLevel::Error, e)]),
        }
    }

    /// Expand Image's window: the view's own, previewing sizes through the controller.
    fn open_expand_image(&self) {
        if self.expand_window.borrow().is_some() {
            return;
        }
        let me = self.me.clone();
        let w = ExpandImageWindow::open(
            self.mtm,
            Rc::new(move |event| {
                let Some(s) = me.upgrade() else { return };
                match event {
                    expand_image::Event::Preview(mib) => s.preview_expand(mib),
                    expand_image::Event::Expand(bytes) => {
                        s.expand_window.replace(None);
                        s.apply(InspectAction::ExpandImage(bytes));
                    }
                    expand_image::Event::Close => {
                        s.expand_window.replace(None);
                    }
                }
            }),
        );
        *self.expand_window.borrow_mut() = Some(w);
        self.preview_expand(0);
    }

    fn preview_expand(&self, mib: u32) {
        let (current, new) = self.ctrl.borrow_mut().expand_image_preview(mib);
        if let Some(w) = self.expand_window.borrow().as_ref() {
            w.show(current, new);
        }
    }

    /// Open an image file, as Build Disk's Open in Inspect asks.
    pub fn open_image(&self, path: PathBuf) {
        self.apply(InspectAction::Open(InspectSource::ImageFile(path)));
    }

    /// Nothing open here, so a shared backup folder may be adopted.
    pub fn is_empty(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        ctrl.backup_folder().is_none() && !ctrl.has_local_source()
    }

    /// Open the shared backup folder.
    pub fn load_backup(&self, folder: &std::path::Path) {
        if self.ctrl.borrow().backup_folder() != Some(folder) {
            self.apply(InspectAction::Open(InspectSource::Backup {
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

    /// The disk job running here, by name, for the other screens' locks.
    pub fn running_job(&self) -> Option<&'static str> {
        if let Some(job) = self.ctrl.borrow().running_job() {
            Some(job)
        } else if self
            .resize
            .borrow()
            .as_ref()
            .is_some_and(|d| d.is_running())
        {
            Some("Inspect: Resize")
        } else if self.hfs.borrow().as_ref().is_some_and(|d| d.is_running()) {
            Some("Inspect: Expand HFS")
        } else if self
            .writer
            .borrow()
            .as_ref()
            .is_some_and(|d| d.is_writing())
        {
            Some("Inspect: Physical Disk Export")
        } else {
            None
        }
    }

    /// A control's action, by its controller id; the menu bar's items come here too.
    pub fn command(&self, id: &str) {
        if self.sync.active() {
            return;
        }
        if let Some(n) = id.strip_prefix(RECENT_ITEM).and_then(|n| n.parse().ok()) {
            self.open_recent(n);
            return;
        }
        if let Some(n) = id.strip_prefix(DEVICE_ITEM).and_then(|n| n.parse().ok()) {
            self.open_device(n);
            return;
        }
        let row = self.row_index();
        let action = match id {
            ids::OPEN_FILE => InspectAction::ChooseImage,
            ids::OPEN_BACKUP => InspectAction::ChooseBackupFolder,
            ids::REINSPECT => InspectAction::Reinspect,
            ids::CONNECT_REMOTE => {
                self.remote.apply(RemotePickAction::Connect);
                return self.on_page_change();
            }
            remote_ids::PICK_ANOTHER => {
                let image = self.ctrl.borrow().remote().cloned();
                if let Some(image) = image {
                    self.remote.apply(RemotePickAction::BrowseOn(image));
                }
                return self.on_page_change();
            }
            remote_ids::BACKUP => InspectAction::BackupRemote,
            remote_ids::CLOSE => InspectAction::Unload { quiet: true },
            ids::EDIT_TABLE => InspectAction::EditTable,
            ids::EXPORT => InspectAction::OpenExport,
            ids::CANCEL_EXPORT => InspectAction::CancelExport,
            ids::ADD_PARTITION => InspectAction::AddPartition,
            ids::EXPAND_IMAGE => return self.open_expand_image(),
            ids::RESIZE => return self.open_resize(),
            ids::ROW_RESIZE_ALTO => return self.open_alto(),
            ids::MAKE_BOOTABLE => InspectAction::MakeBootable,
            ids::CHD_INFO => InspectAction::ChdInfo,
            ids::CLOSE => InspectAction::Close,
            ids::CHECK_REPAIR => InspectAction::Repair,
            ids::CHECK_DEBUG => {
                let on = self
                    .check_window
                    .borrow()
                    .as_ref()
                    .is_some_and(|w| w.toggled());
                InspectAction::ShowCheckDebug(on)
            }
            CHECK_CLOSE => InspectAction::CloseCheck,
            CHD_CLOSE => InspectAction::CloseChdInfo,
            ids::ROW_CALC_MIN => match row {
                Some(i) => InspectAction::CalcMin(i),
                None => return,
            },
            ids::ROW_CHECK => match row {
                Some(i) => InspectAction::Check(i),
                None => return,
            },
            ids::ROW_BROWSE => match row {
                Some(i) => InspectAction::Browse(i),
                None => return,
            },
            ids::ROW_DEFRAGMENT => match row {
                Some(i) => InspectAction::Defragment(i),
                None => return,
            },
            ids::ROW_EXPAND_HFS => match row {
                Some(i) => InspectAction::ExpandHfs(i),
                None => return,
            },
            _ => return,
        };
        self.apply(action);
    }

    fn open_recent(&self, n: usize) {
        let recent = self.recent.list(RecentMode::Inspect);
        if let Some(path) = recent.get(n).map(PathBuf::from) {
            self.remember(&path);
            self.apply(InspectAction::Open(InspectSource::ImageFile(path)));
        }
    }

    fn open_device(&self, slot: usize) {
        let identity = self.devices.borrow().get(slot).map(|d| d.identity());
        if let Some(identity) = identity {
            self.apply(InspectAction::Open(InspectSource::Device {
                slot,
                identity,
            }));
        }
    }

    fn row_selected(&self, i: Option<usize>) {
        let Some(i) = i else { return };
        self.cursor.set(i);
        let index = self.rows.borrow().get(i).map(|r| r.index);
        if !self.sync.active() {
            self.apply(InspectAction::SelectPartition(index));
        }
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            if let Some(s) = me.upgrade() {
                s.answered(reply);
            }
        })
    }

    fn remember(&self, path: &std::path::Path) {
        self.recent
            .remember(RecentMode::Inspect, &path.display().to_string());
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
            let mut c = self.ctrl.borrow_mut();
            c.set_browse_dirty(dirty);
            (c.take_requests(), c.take_log())
        };
        for r in requests {
            if let Some(line) = self.carry_out(r) {
                log.push((LogLevel::Warning, line));
            }
        }
        crate::app::append_log(log);
        self.refresh();
        if let Some(load) = self.browse.take_nested_load() {
            self.apply(load);
        }
    }

    /// Carry out `request`; what this shell cannot do yet says so. No `_` arm.
    fn carry_out(&self, request: HostRequest) -> Option<String> {
        match request {
            HostRequest::CloseBrowse => {
                self.browse.close();
                None
            }
            HostRequest::UpgradeSeekable(path) => {
                self.browse.upgrade_seekable(path);
                None
            }
            HostRequest::OpenBrowse(open) => {
                self.browse.open_request(*open);
                None
            }
            HostRequest::OpenOptical(path) => {
                *self.optical.borrow_mut() = Some(path);
                None
            }
            HostRequest::OpenArchive(path) => {
                *self.archive.borrow_mut() = Some(path);
                None
            }
            HostRequest::BackupClosed => {
                self.backup_closed.set(true);
                None
            }
            HostRequest::ExpandHfs(source) => {
                let d = ExpandHfsWindow::open(
                    self.mtm,
                    self.notify.clone(),
                    self.host.clone(),
                    *source,
                );
                *self.hfs.borrow_mut() = Some(d);
                None
            }
        }
    }

    fn read(&self) -> Shown {
        let devices = self.devices.borrow();
        let mut c = self.ctrl.borrow_mut();
        let segments = if c.partitions().is_empty() {
            Vec::new()
        } else {
            c.layout()
        };
        let mut jobs = Vec::new();
        jobs.extend(c.loading());
        jobs.extend(c.export_job());
        jobs.extend(c.tool_jobs());
        Shown {
            source: c.source_label(devices.as_slice()),
            error: c.last_error().map(str::to_string),
            flux: c.flux_faults().map(|f| {
                format!(
                    "Warning: flux decode: {}. Files using these sectors are damaged.",
                    f.summary
                )
            }),
            jobs,
            details: c.detail_lines(),
            has_table: c.table_summary().is_some(),
            rows: c.rows(),
            segments,
            selected: c.selected_partition(),
            can_reinspect: c.can_reinspect(),
            can_bootable: c.can_make_bootable(),
            can_close: c.can_close(),
            has_chd: c.chd_path().is_some(),
            can_convert: c.convert_source().is_some(),
            check: c.check_report().map(|r| report::Shown {
                headline: r.headline(),
                stats: r.check.stats_line.clone(),
                lines: r.lines(),
                toggle: r.has_debug().then_some(r.show_debug),
                button: r.check.repairable,
            }),
            chd: c.chd_info_text().map(str::to_string),
            exporting: c.export_job().is_some(),
            remote_open: c.remote().is_some(),
            remote_backing_up: c.remote_backup_running(),
        }
    }

    /// Read the controller first and release it, then set the views inside the guard.
    fn refresh(&self) {
        let shown = self.read();
        self.sync.run(|| self.show(shown));
        let editor = {
            let c = self.ctrl.borrow();
            c.table_editor().map(|v| (v, c.table_write_target()))
        };
        self.show_editor(editor);
        let export = export::Shown::read(&mut self.ctrl.borrow_mut());
        self.show_export(export);
    }

    /// The Export window follows the controller: open while it says so, updated each refresh.
    fn show_export(&self, shown: Option<export::Shown>) {
        let Some(shown) = shown else {
            self.export_window.replace(None);
            return;
        };
        let existing = self.export_window.borrow().clone();
        let w = existing.unwrap_or_else(|| {
            let me = self.me.clone();
            let send: Rc<dyn Fn(InspectAction)> = Rc::new(move |a| {
                if let Some(s) = me.upgrade() {
                    s.apply(a);
                }
            });
            let me = self.me.clone();
            let physical: Rc<dyn Fn()> = Rc::new(move || {
                if let Some(s) = me.upgrade() {
                    s.open_physical();
                }
            });
            let w = ExportWindow::open(self.mtm, send, physical);
            *self.export_window.borrow_mut() = Some(w.clone());
            w
        });
        w.show(shown);
    }

    /// Export's Physical Disk Export: the image goes to the disk writer's window.
    fn open_physical(&self) {
        let busy = self.writer.borrow().as_ref().is_some_and(|d| d.is_open());
        let Some(open) = self.ctrl.borrow().physical_source() else {
            return;
        };
        if busy {
            return;
        }
        self.apply(InspectAction::CloseExport);
        let d = DiskWriteWindow::open(
            self.mtm,
            self.notify.clone(),
            self.host.clone(),
            &self.devices.borrow(),
            open,
        );
        *self.writer.borrow_mut() = Some(d);
    }

    /// The table editor follows the controller: open while it hands out a view.
    fn show_editor(
        &self,
        shown: Option<(
            rusty_backup::controller::partition_edit::TableEditView,
            Option<String>,
        )>,
    ) {
        let Some((view, target)) = shown else {
            self.editor_window.replace(None);
            return;
        };
        let existing = self.editor_window.borrow().clone();
        let w = existing.unwrap_or_else(|| {
            let me = self.me.clone();
            let send: Rc<dyn Fn(InspectAction)> = Rc::new(move |a| {
                if let Some(s) = me.upgrade() {
                    s.apply(a);
                }
            });
            let w = EditorWindow::open(self.mtm, send);
            *self.editor_window.borrow_mut() = Some(w.clone());
            w
        });
        w.show(&view, target.as_deref());
    }

    fn show(&self, s: Shown) {
        self.show_source(&s.source);
        let note = InspectController::needs_elevation_note().then(unelevated_device_note);
        ui::set_text(&self.note, note.unwrap_or(""));
        ui::set_text(&self.flux, s.flux.as_deref().unwrap_or(""));
        ui::set_text(
            &self.error,
            &s.error.map(|e| format!("Error: {e}")).unwrap_or_default(),
        );
        let job = s.jobs.first();
        let (line, pct) = job.map(job_line).unwrap_or_default();
        ui::set_text(&self.job, &line);
        ui::set_progress(&self.progress, job.is_some(), pct);
        self.set_enabled(Part::Reinspect, s.can_reinspect);
        self.set_enabled(Part::Bootable, s.can_bootable);
        ui::set_visible(self.part(Part::Convert), s.can_convert);
        ui::set_visible(self.part(Part::Chd), s.has_chd);
        ui::set_visible(self.part(Part::Close), s.can_close);
        ui::set_visible(self.part(Part::CancelExport), s.exporting);
        for p in [Part::PickAnother, Part::BackupRemote, Part::CloseRemote] {
            ui::set_visible(self.part(p), s.remote_open);
        }
        self.set_enabled(Part::BackupRemote, !s.remote_backing_up);
        self.set_enabled(Part::CloseRemote, !s.remote_backing_up);
        ui::set_visible(
            self.part(Part::BackingUp),
            s.remote_open && s.remote_backing_up,
        );
        ui::set_text(&self.details, &s.details.join("\n"));
        ui::set_visible(
            self.part(Part::NoPartitions),
            s.has_table && s.rows.is_empty(),
        );
        ui::set_visible(&self.bar, s.has_table && !s.rows.is_empty());
        self.bar.show(s.segments, s.selected);
        self.show_rows(s.rows);
        // A click on the bar selects the partition; the list follows it.
        let at = s
            .selected
            .and_then(|p| self.rows.borrow().iter().position(|r| r.index == p));
        if let Some(i) = at {
            self.cursor.set(i);
            self.table.select(i);
        }
        self.show_windows(s.check, s.chd);
        self.relayout();
    }

    /// The source pull-down: its title is the open source; its menu lists recent files and devices.
    fn show_source(&self, title: &str) {
        let recent: Vec<String> = self
            .recent
            .list(RecentMode::Inspect)
            .into_iter()
            .take(RECENT_ROWS)
            .collect();
        let devices: Vec<String> = self
            .devices
            .borrow()
            .iter()
            .map(|d| d.display_name())
            .collect();
        let title = if title.is_empty() {
            "Select a device or image..."
        } else {
            title
        };
        let want = (recent.clone(), devices.clone());
        let same = *self.menu_built.borrow() == want;
        // `itemAtIndex` raises on an empty menu, so ask for the count first.
        let shown = (self.source.numberOfItems() > 0)
            .then(|| self.source.itemAtIndex(0))
            .flatten()
            .map(|i| i.title().to_string());
        if same && shown.as_deref() == Some(&ui::plain(title)) {
            return;
        }
        *self.menu_built.borrow_mut() = want;
        let mtm = self.mtm;
        let menu = NSMenu::new(mtm);
        menu.setAutoenablesItems(false);
        // A pull-down's first item is its title; on, so the button does not look greyed out.
        menu.addItem(&item(mtm, &ui::plain(title), "", true));
        menu.addItem(&item(mtm, "Recent files", "", false));
        if recent.is_empty() {
            menu.addItem(&item(mtm, "    (none yet)", "", false));
        }
        for (i, path) in recent.iter().enumerate() {
            menu.addItem(&item(mtm, path, &format!("{RECENT_ITEM}{i}"), true));
        }
        menu.addItem(&NSMenuItem::separatorItem(mtm));
        menu.addItem(&item(mtm, "Devices", "", false));
        if devices.is_empty() {
            menu.addItem(&item(
                mtm,
                "    (none listed; Refresh Devices may find more)",
                "",
                false,
            ));
        }
        for (i, name) in devices.iter().enumerate() {
            menu.addItem(&item(mtm, name, &format!("{DEVICE_ITEM}{i}"), true));
        }
        self.source.setMenu(Some(&menu));
        crate::menu::set_sources(&recent, &devices);
    }

    fn show_rows(&self, rows: Vec<PartitionRow>) {
        let table: Vec<Vec<String>> = rows
            .iter()
            .map(|r| (0..COLUMNS.len()).map(|c| ui::plain(&cell(r, c))).collect())
            .collect();
        let count = rows.len();
        *self.rows.borrow_mut() = rows;
        self.table.set_rows(table);
        if self.cursor.get() >= count {
            self.cursor.set(0);
        }
        if count > 0 && self.table.selected().is_none() {
            self.table.select(self.cursor.get());
        }
        let rows = self.rows.borrow();
        let row = rows.get(self.cursor.get());
        let (acts, locked) = row.map(|r| (r.actions, r.locked)).unwrap_or_default();
        let can_calc = row.is_some_and(|r| matches!(r.min, MinCell::CanCalc));
        self.set_enabled(Part::Browse, acts.browse && !locked);
        self.set_enabled(Part::CalcMin, can_calc && !locked);
        self.set_enabled(Part::Check, acts.browse && acts.check && !locked);
        self.set_enabled(Part::Expand, acts.browse && acts.expand_hfs && !locked);
        ui::set_visible(self.part(Part::Defragment), acts.browse && acts.defragment);
        self.set_enabled(Part::Defragment, !locked);
        ui::set_visible(self.part(Part::ResizeAlto), acts.browse && acts.resize_alto);
    }

    /// Open, update or close the Check and CHD Info windows to match the controller.
    fn show_windows(&self, check: Option<report::Shown>, chd: Option<String>) {
        match check {
            Some(shown) => {
                if self.check_window.borrow().is_none() {
                    let w = self.window("Filesystem Check Results", true);
                    *self.check_window.borrow_mut() = Some(w);
                }
                if let Some(w) = self.check_window.borrow().as_ref() {
                    w.show(&shown);
                }
            }
            None => {
                self.check_window.replace(None);
            }
        }
        match chd {
            Some(text) => {
                if self.chd_window.borrow().is_none() {
                    *self.chd_window.borrow_mut() = Some(self.window("CHD Info", false));
                }
                if let Some(w) = self.chd_window.borrow().as_ref() {
                    w.show(&report::Shown {
                        headline: String::new(),
                        stats: String::new(),
                        lines: text.lines().map(str::to_string).collect(),
                        toggle: None,
                        button: false,
                    });
                }
            }
            None => {
                self.chd_window.replace(None);
            }
        }
    }

    fn window(&self, title: &str, check: bool) -> ReportWindow {
        let me = self.me.clone();
        let on_close = Box::new(move || {
            if let Some(s) = me.upgrade() {
                s.apply(if check {
                    InspectAction::CloseCheck
                } else {
                    InspectAction::CloseChdInfo
                });
            }
        });
        let (toggle, button, close) = if check {
            (
                Some(("Show debug messages", ids::CHECK_DEBUG)),
                Some(("Repair", ids::CHECK_REPAIR)),
                CHECK_CLOSE,
            )
        } else {
            (None, None, CHD_CLOSE)
        };
        ReportWindow::open(self.mtm, title, toggle, button, close, on_close)
    }

    fn row_index(&self) -> Option<usize> {
        self.rows.borrow().get(self.cursor.get()).map(|r| r.index)
    }

    /// Lay the page out at its width; the app's page scrolls when it is taller than the window.
    pub fn relayout(&self) {
        let p = |x: Part| x;
        let mut rows = vec![Node::row(vec![
            Node::leaf(
                p(Part::SourceCaption),
                Size::Fixed(55),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::leaf(
                p(Part::Source),
                Size::Stretch(1),
                Size::Fixed(layout::ROW + 2),
            ),
            Node::button(p(Part::OpenFile), 120),
            Node::button(p(Part::OpenBackup), 170),
            Node::button(p(Part::Connect), 170),
        ])];
        rows.push(Node::row(vec![
            Node::button(p(Part::PickAnother), 160),
            Node::button(p(Part::BackupRemote), 140),
            Node::button(p(Part::CloseRemote), 115),
            Node::leaf(
                p(Part::BackingUp),
                Size::Fixed(140),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::Space(Size::Stretch(1)),
        ]));
        let lines = |t: &NSTextField| {
            let text = t.stringValue().to_string();
            if text.is_empty() {
                0
            } else {
                text.lines().count() as i32
            }
        };
        let line =
            |x: Part, n: i32| Node::leaf(x, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n));
        if lines(&self.note) > 0 {
            rows.push(line(Part::Note, 2));
        }
        if lines(&self.flux) > 0 {
            rows.push(line(Part::Flux, 1));
        }
        if lines(&self.error) > 0 {
            rows.push(line(Part::Error, 1));
        }
        rows.push(Node::row(vec![
            Node::leaf(p(Part::Job), Size::Stretch(2), Size::Fixed(layout::ROW - 4)),
            Node::leaf(
                p(Part::Progress),
                Size::Stretch(1),
                Size::Fixed(layout::ROW - 4),
            ),
            Node::button(p(Part::CancelExport), 120),
        ]));
        rows.push(Node::row(vec![
            Node::button(p(Part::Reinspect), 100),
            Node::button(p(Part::Export), 160),
            Node::button(p(Part::Convert), 100),
            Node::button(p(Part::Chd), 90),
            Node::Space(Size::Stretch(1)),
            Node::button(p(Part::Close), 80),
        ]));
        rows.push(Node::row(vec![
            Node::button(p(Part::EditTable), 170),
            Node::button(p(Part::AddPartition), 130),
            Node::button(p(Part::Resize), 150),
            Node::button(p(Part::ExpandImage), 130),
            Node::button(p(Part::Bootable), 130),
            Node::Space(Size::Stretch(1)),
        ]));
        let n = lines(&self.details);
        if n > 0 {
            rows.push(line(Part::Details, n));
        }
        rows.push(Node::leaf(
            p(Part::Bar),
            Size::Stretch(1),
            Size::Fixed(partition_bar::HEIGHT),
        ));
        rows.push(Node::leaf(
            p(Part::Table),
            Size::Stretch(1),
            Size::Stretch(1),
        ));
        rows.push(line(Part::NoPartitions, 1));
        rows.push(Node::row(vec![
            Node::button(p(Part::Browse), 80),
            Node::button(p(Part::CalcMin), 90),
            Node::button(p(Part::Check), 80),
            Node::button(p(Part::Expand), 140),
            Node::button(p(Part::Defragment), 120),
            Node::button(p(Part::ResizeAlto), 90),
            Node::Space(Size::Stretch(1)),
        ]));
        let tree = Node::inset(Node::column(rows), layout::MARGIN);
        // Hidden views take no room (the Win32 shell's R-095 lesson).
        let keep = |x: &Part| match x {
            Part::Job => ui::shown(&self.progress),
            _ => ui::shown(self.part(*x)),
        };
        let Some(tree) = tree.prune(&keep) else {
            return;
        };
        crate::app::place_page(&self.body, &tree, &|x: Part, r| {
            ui::move_to(self.part(x), r)
        });
    }
}

/// A menu item that goes to the action target with `id`; an empty id makes a caption.
fn item(mtm: MainThreadMarker, title: &str, id: &str, on: bool) -> Retained<NSMenuItem> {
    // SAFETY: a plain menu item; its action goes to the target set below.
    let it = unsafe {
        NSMenuItem::initWithTitle_action_keyEquivalent(
            mtm.alloc(),
            &ns(&ui::plain(title)),
            (!id.is_empty()).then_some(sel!(act:)),
            &ns(""),
        )
    };
    if !id.is_empty() {
        let t = ui::target(mtm);
        // SAFETY: the target is never dropped.
        unsafe { it.setTarget(Some(&t)) };
        it.setIdentifier(Some(&ns(id)));
    }
    it.setEnabled(on);
    it
}
