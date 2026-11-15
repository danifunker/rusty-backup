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

use rusty_backup::controller::app::{
    unelevated_device_note, DiskDevice, Notify, RecentHandle, RecentMode, Snapshot,
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

use super::alto::AltoWindow;
use super::browse::BrowseView;
use super::disk_write::DiskWriteWindow;
use super::editor::EditorWindow;
use super::effects::{EffectHost, Reply, ReplyFn};
use super::expand_hfs::ExpandHfsWindow;
use super::export::{self, ExportWindow};
use super::form::Form;
use super::host::{Host, Surface};
use super::jobs::job_line;
use super::layout::{self, Node, Size};
use super::partition_bar::{self, Bar};
use super::remote_pick::RemotePage;
use super::report::{self, ReportWindow};
use super::resize::ResizeWindow;
use super::resize_image::ResizeImageWindow;
use super::sync::SyncGuard;

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

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    /// An export is running, so Cancel Export shows.
    exporting: bool,
    /// Which image and table tools apply to what is open.
    /// Expand Image opens Resize Image: an image file with a table.
    can_resize_image: bool,
    can_edit_table: bool,
    can_export: bool,
    can_add_partition: bool,
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
    /// A recent file or a device in the source pull-down.
    Recent(usize),
    Device(usize),
}

/// The controller id each control sends, as the menu bar's items do.
fn part_id(p: Part) -> Option<String> {
    Some(
        match p {
            Part::OpenFile => ids::OPEN_FILE,
            Part::OpenBackup => ids::OPEN_BACKUP,
            Part::Connect => ids::CONNECT_REMOTE,
            Part::PickAnother => remote_ids::PICK_ANOTHER,
            Part::BackupRemote => remote_ids::BACKUP,
            Part::CloseRemote => remote_ids::CLOSE,
            Part::CancelExport => ids::CANCEL_EXPORT,
            Part::Reinspect => ids::REINSPECT,
            Part::Export => ids::EXPORT,
            Part::Convert => ids::CONVERT,
            Part::Chd => ids::CHD_INFO,
            Part::Close => ids::CLOSE,
            Part::EditTable => ids::EDIT_TABLE,
            Part::AddPartition => ids::ADD_PARTITION,
            Part::Resize => ids::RESIZE,
            Part::ExpandImage => ids::EXPAND_IMAGE,
            Part::Bootable => ids::MAKE_BOOTABLE,
            Part::Browse => ids::ROW_BROWSE,
            Part::CalcMin => ids::ROW_CALC_MIN,
            Part::Check => ids::ROW_CHECK,
            Part::Expand => ids::ROW_EXPAND_HFS,
            Part::Defragment => ids::ROW_DEFRAGMENT,
            Part::ResizeAlto => ids::ROW_RESIZE_ALTO,
            Part::Recent(n) => return Some(format!("{}{}", RECENT_ITEM, n)),
            Part::Device(n) => return Some(format!("{}{}", DEVICE_ITEM, n)),
            _ => return None,
        }
        .to_string(),
    )
}

pub struct InspectScreen {
    ui: Rc<dyn Host>,
    ctrl: RefCell<InspectController>,
    notify: Arc<dyn Notify>,
    /// Expand HFS Volume, while its controller has a source.
    hfs: RefCell<Option<Rc<ExpandHfsWindow>>>,
    /// Resize Partitions and Resize Alto Disk, while their controllers are open.
    resize: RefCell<Option<Rc<ResizeWindow>>>,
    /// Resize Image, while its controller is open.
    resize_image: RefCell<Option<Rc<ResizeImageWindow>>>,
    alto: RefCell<Option<Rc<AltoWindow>>>,
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
    convert: RefCell<Option<PathBuf>>,
    sync: SyncGuard,
    me: Weak<InspectScreen>,
    page: Surface,
    form: Form<Part>,
    bar: Bar,
    rows: RefCell<Vec<PartitionRow>>,
    /// The row the tools act on: the list's selection, else the first row (as the TUI's cursor).
    cursor: Cell<usize>,
    check_window: RefCell<Option<ReportWindow>>,
    chd_window: RefCell<Option<ReportWindow>>,
    /// The browse view, shown in the page's place while it is open.
    pub browse: Rc<BrowseView>,
}

impl InspectScreen {
    /// The screen on `page`; the browse view and the remote picker get the pages the shell places
    /// over it while they are open.
    pub fn build(
        ui: Rc<dyn Host>,
        page: Surface,
        browse_page: Surface,
        remote_page: Surface,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<Self> {
        let browse =
            BrowseView::build(ui.clone(), browse_page, notify.clone(), host.clone(), false);
        let remote = RemotePage::build(ui.clone(), remote_page, notify.clone(), host.clone());
        let screen = Rc::new_cyclic(|me: &Weak<InspectScreen>| {
            let fire = me.clone();
            let form = Form::with_host(
                ui.clone(),
                page,
                Rc::new(move |p| {
                    if let Some(s) = fire.upgrade() {
                        s.fired(p);
                    }
                }),
            );
            let f = &form;
            f.label(Part::SourceCaption, "Source:");
            f.pulldown(Part::Source, ids::SOURCE);
            f.button(Part::OpenFile, "Open File...", ids::OPEN_FILE);
            f.button(Part::OpenBackup, "Open Backup Folder...", ids::OPEN_BACKUP);
            f.button(Part::Connect, "Connect to Remote...", ids::CONNECT_REMOTE);
            f.button(
                Part::PickAnother,
                "Pick Another Image",
                remote_ids::PICK_ANOTHER,
            );
            f.button(Part::BackupRemote, "Back Up Image...", remote_ids::BACKUP);
            f.label(Part::BackingUp, remote_hints::BACKING_UP);
            f.button(Part::CloseRemote, "Close Remote", remote_ids::CLOSE);
            f.wrap(Part::Note, "");
            f.label(Part::Flux, "");
            f.label(Part::Error, "");
            f.label(Part::Job, "");
            f.progress(Part::Progress);
            f.button(Part::CancelExport, "Cancel Export", ids::CANCEL_EXPORT);
            f.button(Part::Reinspect, "Re-inspect", ids::REINSPECT);
            f.button(Part::Export, "Export Disk Image...", ids::EXPORT);
            f.button(Part::Convert, "Convert...", ids::CONVERT);
            f.button(Part::Chd, "CHD Info", ids::CHD_INFO);
            f.button(Part::Close, "Close", ids::CLOSE);
            f.button(Part::EditTable, "Edit Partition Table...", ids::EDIT_TABLE);
            f.button(Part::AddPartition, "Add Partition...", ids::ADD_PARTITION);
            f.button(Part::Resize, "Resize Partitions...", ids::RESIZE);
            f.button(Part::ExpandImage, "Expand Image...", ids::EXPAND_IMAGE);
            f.button(Part::Bootable, "Make Bootable...", ids::MAKE_BOOTABLE);
            f.wrap(Part::Details, "");
            let bar = Bar::add(f, Part::Bar, ids::PARTITION_BAR);
            f.table(Part::Table, ids::PARTITION_TABLE, &COLUMNS, None, None);
            f.label(Part::NoPartitions, "No partitions found.");
            f.button(Part::Browse, "Browse", ids::ROW_BROWSE);
            f.button(Part::CalcMin, "Calc min", ids::ROW_CALC_MIN);
            f.button(Part::Check, "Check", ids::ROW_CHECK);
            f.button(Part::Expand, "Expand/Export...", ids::ROW_EXPAND_HFS);
            f.button(Part::Defragment, "Defragment...", ids::ROW_DEFRAGMENT);
            f.button(Part::ResizeAlto, "Resize...", ids::ROW_RESIZE_ALTO);
            InspectScreen {
                ui: ui.clone(),
                ctrl: RefCell::new(InspectController::new(InspectServices {
                    jobs: JobRegistry::new(),
                    notify: notify.clone(),
                })),
                notify,
                hfs: RefCell::default(),
                resize: RefCell::default(),
                resize_image: RefCell::default(),
                alto: RefCell::default(),
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
                convert: RefCell::default(),
                sync: SyncGuard::default(),
                me: me.clone(),
                page,
                form,
                bar,
                rows: RefCell::default(),
                cursor: Cell::new(0),
                check_window: RefCell::default(),
                chd_window: RefCell::default(),
                browse,
                remote,
            }
        });
        let me = screen.me.clone();
        screen.remote.on_change(Rc::new(move || {
            if let Some(s) = me.upgrade() {
                s.remote_changed();
            }
        }));
        screen.refresh();
        screen
    }

    /// A control reported: the bar and the list by their own events, the rest by controller id.
    fn fired(&self, p: Part) {
        match p {
            Part::Bar => {
                if let Some(picked) = self.bar.picked() {
                    if !self.sync.active() {
                        self.apply(InspectAction::SelectPartition(picked));
                    }
                }
            }
            Part::Table => {
                let i = self.form.selected_row(Part::Table);
                self.row_selected(i);
            }
            _ => {
                if let Some(id) = part_id(p) {
                    self.command(&id);
                }
            }
        }
    }

    fn set_enabled(&self, p: Part, on: bool) {
        self.form.enable(p, on);
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
        let wizard = self.resize_image.borrow().clone();
        if let Some(d) = wizard {
            d.tick();
            if d.take_disk_changed() {
                self.apply(InspectAction::DiskChanged);
            }
            if let Some(path) = d.take_open_request() {
                d.apply(rusty_backup::controller::resize_image::ResizeImageAction::Close);
                self.apply(InspectAction::Open(InspectSource::ImageFile(path)));
            }
            if !d.is_open() {
                self.resize_image.replace(None);
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

    /// The image Convert... asked the Convert screen to take, once.
    pub fn take_convert(&self) -> Option<PathBuf> {
        self.convert.borrow_mut().take()
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
        self.ui.log(log.collect());
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
            let d = ResizeWindow::open(
                self.ui.clone(),
                self.notify.clone(),
                self.host.clone(),
                plan,
            );
            d.set_devices(&self.devices.borrow());
            *self.resize.borrow_mut() = Some(d);
        }
    }

    /// Expand Image opens the Resize Image wizard, with its own controller, on what Inspect read.
    fn open_resize_image(&self) {
        if self
            .resize_image
            .borrow()
            .as_ref()
            .is_some_and(|d| d.is_open())
        {
            return;
        }
        let source = self.ctrl.borrow_mut().resize_image_source();
        match source {
            Ok(source) => {
                let d = ResizeImageWindow::open(
                    self.ui.clone(),
                    self.notify.clone(),
                    self.host.clone(),
                    source,
                );
                *self.resize_image.borrow_mut() = Some(d);
            }
            Err(e) => self.ui.log(vec![(LogLevel::Error, e)]),
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
                let d = AltoWindow::open(
                    self.ui.clone(),
                    self.notify.clone(),
                    self.host.clone(),
                    path,
                );
                *self.alto.borrow_mut() = Some(d);
            }
            Err(e) => self.ui.log(vec![(LogLevel::Error, e)]),
        }
    }

    /// Files dropped from Finder, staged into the browsed volume while it is in Edit Mode.
    pub fn drop_paths(&self, paths: Vec<PathBuf>) -> bool {
        self.browse.drop_paths(paths)
    }

    /// Whether a remote session is open, and whether a local source is: where a drop may go.
    pub fn drop_state(&self) -> (bool, bool) {
        let ctrl = self.ctrl.borrow();
        (ctrl.is_remote(), ctrl.has_local_source())
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
            ids::CONVERT => {
                *self.convert.borrow_mut() = self.ctrl.borrow().convert_source();
                // No action reaches the app loop from here, so wake it to take the request.
                return self.notify.wake();
            }
            ids::EDIT_TABLE => InspectAction::EditTable,
            ids::EXPORT => InspectAction::OpenExport,
            ids::CANCEL_EXPORT => InspectAction::CancelExport,
            ids::ADD_PARTITION => InspectAction::AddPartition,
            ids::EXPAND_IMAGE => return self.open_resize_image(),
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
        self.ui.log(log);
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
                    self.ui.clone(),
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
            can_resize_image: c.can_resize_image(),
            can_edit_table: c.can_edit_table(),
            can_export: c.can_export(),
            can_add_partition: c.can_add_partition(),
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
            let w = ExportWindow::open(self.ui.clone(), send, physical);
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
            self.ui.clone(),
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
            let w = EditorWindow::open(self.ui.clone(), send);
            *self.editor_window.borrow_mut() = Some(w.clone());
            w
        });
        w.show(&view, target.as_deref());
    }

    fn show(&self, s: Shown) {
        self.show_source(&s.source);
        let note = InspectController::needs_elevation_note().then(unelevated_device_note);
        let f = &self.form;
        f.set_text(Part::Note, note.unwrap_or(""));
        f.set_text(Part::Flux, s.flux.as_deref().unwrap_or(""));
        f.set_text(
            Part::Error,
            &s.error.map(|e| format!("Error: {}", e)).unwrap_or_default(),
        );
        let job = s.jobs.first();
        let (line, pct) = job.map(job_line).unwrap_or_default();
        f.set_text(Part::Job, &line);
        f.set_progress(Part::Progress, job.is_some(), pct);
        self.set_enabled(Part::Reinspect, s.can_reinspect);
        self.set_enabled(Part::Bootable, s.can_bootable);
        f.show(Part::Convert, s.can_convert);
        f.show(Part::Chd, s.has_chd);
        f.show(Part::Close, s.can_close);
        f.show(Part::CancelExport, s.exporting);
        self.set_enabled(Part::ExpandImage, s.can_resize_image);
        self.set_enabled(Part::Export, s.can_export);
        self.set_enabled(Part::EditTable, s.can_edit_table);
        self.set_enabled(Part::AddPartition, s.can_add_partition);
        let resizing = self
            .resize
            .borrow()
            .as_ref()
            .is_some_and(|d| d.is_running());
        self.set_enabled(Part::Resize, s.can_edit_table && !resizing);
        for p in [Part::PickAnother, Part::BackupRemote, Part::CloseRemote] {
            f.show(p, s.remote_open);
        }
        self.set_enabled(Part::BackupRemote, !s.remote_backing_up);
        self.set_enabled(Part::CloseRemote, !s.remote_backing_up);
        f.show(Part::BackingUp, s.remote_open && s.remote_backing_up);
        f.set_text(Part::Details, &s.details.join("\n"));
        f.show(Part::NoPartitions, s.has_table && s.rows.is_empty());
        f.show(Part::Bar, s.has_table && !s.rows.is_empty());
        self.bar.show(s.segments, s.selected);
        self.show_rows(s.rows);
        // A click on the bar selects the partition; the list follows it.
        let at = s
            .selected
            .and_then(|p| self.rows.borrow().iter().position(|r| r.index == p));
        if let Some(i) = at {
            self.cursor.set(i);
            f.select_row(Part::Table, i);
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
        let shown = self.form.items(Part::Source).into_iter().next();
        if same && shown.as_deref() == Some(title) {
            return;
        }
        *self.menu_built.borrow_mut() = want;
        // A pull-down's first item is its title; captions and placeholders are items with no key.
        let mut items: Vec<(String, Option<Part>, bool)> =
            vec![("Recent files".to_string(), None, false)];
        if recent.is_empty() {
            items.push(("    (none yet)".to_string(), None, false));
        }
        for (i, path) in recent.iter().enumerate() {
            items.push((path.clone(), Some(Part::Recent(i)), true));
        }
        items.push((String::new(), None, false));
        items.push(("Devices".to_string(), None, false));
        if devices.is_empty() {
            items.push((
                "    (none listed; Refresh Devices may find more)".to_string(),
                None,
                false,
            ));
        }
        for (i, name) in devices.iter().enumerate() {
            items.push((name.clone(), Some(Part::Device(i)), true));
        }
        self.form.set_pulldown(Part::Source, title, &items);
        self.ui.menu_sources(&recent, &devices);
    }

    fn show_rows(&self, rows: Vec<PartitionRow>) {
        let table: Vec<Vec<String>> = rows
            .iter()
            .map(|r| (0..COLUMNS.len()).map(|c| cell(r, c)).collect())
            .collect();
        let count = rows.len();
        *self.rows.borrow_mut() = rows;
        self.form.set_rows(Part::Table, &table);
        if self.cursor.get() >= count {
            self.cursor.set(0);
        }
        if count > 0 && self.form.selected_row(Part::Table).is_none() {
            self.form.select_row(Part::Table, self.cursor.get());
        }
        let rows = self.rows.borrow();
        let row = rows.get(self.cursor.get());
        let (acts, locked) = row.map(|r| (r.actions, r.locked)).unwrap_or_default();
        let can_calc = row.is_some_and(|r| matches!(r.min, MinCell::CanCalc));
        self.set_enabled(Part::Browse, acts.browse && !locked);
        self.set_enabled(Part::CalcMin, can_calc && !locked);
        self.set_enabled(Part::Check, acts.browse && acts.check && !locked);
        self.set_enabled(Part::Expand, acts.browse && acts.expand_hfs && !locked);
        self.form
            .show(Part::Defragment, acts.browse && acts.defragment);
        self.set_enabled(Part::Defragment, !locked);
        self.form
            .show(Part::ResizeAlto, acts.browse && acts.resize_alto);
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
        let me = self.me.clone();
        let command: Rc<dyn Fn(&str)> = Rc::new(move |id| {
            if let Some(s) = me.upgrade() {
                s.command(id);
            }
        });
        ReportWindow::open(
            self.ui.clone(),
            title,
            toggle,
            button,
            close,
            on_close,
            command,
        )
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
        let lines = |p: Part| {
            if self.form.text(p).is_empty() {
                0
            } else {
                self.form.wrapped_lines(p)
            }
        };
        let line =
            |x: Part, n: i32| Node::leaf(x, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n));
        if lines(Part::Note) > 0 {
            rows.push(line(Part::Note, lines(Part::Note)));
        }
        if lines(Part::Flux) > 0 {
            rows.push(line(Part::Flux, 1));
        }
        if lines(Part::Error) > 0 {
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
        let n = lines(Part::Details);
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
            Part::Job => self.form.shown(Part::Progress),
            _ => self.form.shown(*x),
        };
        if let Some(tree) = tree.prune(&keep) {
            self.form.place_page(tree);
        }
    }
}
