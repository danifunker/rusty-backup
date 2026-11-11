//! The Backup screen over `controller::backup::BackupController`; GTK's `backup/` is the reference.
//!
//! Slice 4, first pass: the source (a split button listing recent images and devices, with Open
//! Image, Close Drive and Load partition info), the partition table with Include, Calc min and
//! Compact, the destination, name, output format with the shared CHD control, the sizing and
//! split options, the checksum, and Start / Cancel with the progress line and the outcome.
//! The VHD options window is `backup_vhd.rs`, the Remote Drive window `backup_remote.rs`.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::chd::ChdOptionsControl;
use rusty_backup::controller::app::{
    busy_elsewhere_notice, no_devices_hint, DiskDevice, Notify, RecentHandle, RecentMode, Snapshot,
};
use rusty_backup::controller::backup::{
    format_size, hints, ids, labels, BackupAction, BackupController, BackupServices, ChdChoice,
    ChdProfile, CompactCell, CompressionType, HostRequest, JobRegistry, MinCell, PartitionRow,
    CHECKSUMS, FORMATS, SPLIT_MIB_MAX, SPLIT_MIB_MIN,
};
use rusty_backup::controller::{Controller, Effect};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{BCN_DROPDOWN, BS_SPLITBUTTON, NMHDR};
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, GetAncestor, GetWindowRect, SendMessageW, BM_GETCHECK, BM_SETCHECK, BN_CLICKED,
    CBN_SELCHANGE, CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL, CB_SETCURSEL, EN_CHANGE,
    ES_NUMBER, GA_ROOT, WINDOW_EX_STYLE, WINDOW_STYLE, WS_TABSTOP, WS_VSCROLL,
};

use crate::backup_remote::{RemoteShown, RemoteWindow};
use crate::backup_vhd::{VhdShown, VhdWindow};
use crate::chd::{ChdControl, ChdIds};
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

fn combo(parent: HWND, id: &str, items: &[&str]) -> HWND {
    let style = WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
    let h = ui::child(parent, w!("COMBOBOX"), "", style, WINDOW_EX_STYLE(0), id);
    for item in items {
        let t = ui::wide(item);
        // SAFETY: the combo copies the string.
        unsafe {
            SendMessageW(h, CB_ADDSTRING, None, Some(LPARAM(t.as_ptr() as isize)));
        }
    }
    h
}

fn select(h: HWND, i: usize) {
    // SAFETY: plain combo messages; CB_SETCURSEL sends no CBN_SELCHANGE.
    unsafe {
        if SendMessageW(h, CB_GETCURSEL, None, None).0 != i as isize {
            SendMessageW(h, CB_SETCURSEL, Some(WPARAM(i)), None);
        }
    }
}

fn picked(h: HWND) -> Option<usize> {
    // SAFETY: a plain combo query.
    usize::try_from(unsafe { SendMessageW(h, CB_GETCURSEL, None, None).0 }).ok()
}

fn checked(h: HWND) -> bool {
    // SAFETY: a plain button query.
    unsafe { SendMessageW(h, BM_GETCHECK, None, None).0 == 1 }
}

fn set_check(h: HWND, on: bool) {
    if checked(h) != on {
        // SAFETY: a plain button message; it sends no BN_CLICKED.
        unsafe {
            SendMessageW(h, BM_SETCHECK, Some(WPARAM(on as usize)), None);
        }
    }
}

/// One partition's controls.
struct Row {
    index: usize,
    include: HWND,
    position: HWND,
    kind: HWND,
    size: HWND,
    min: HWND,
    calc: HWND,
    frag: HWND,
    compact: HWND,
}

impl Row {
    fn build(page: HWND, row: &PartitionRow) -> Row {
        let index = row.index;
        Row {
            index,
            include: ui::checkbox(page, "", &format!("{}.{index}", ids::ROW_INCLUDE)),
            position: ui::cell_label(page, &row.position.to_string()),
            kind: ui::cell_label(page, &row.type_name),
            size: ui::cell_label(page, &format_size(row.size_bytes)),
            min: ui::cell_label(page, ""),
            calc: ui::button(page, "Calc min", &format!("{}.{index}", ids::ROW_CALC_MIN)),
            frag: ui::cell_label(page, ""),
            compact: ui::checkbox(page, "", &format!("{}.{index}", ids::ROW_COMPACT)),
        }
    }

    fn all(&self) -> [HWND; 8] {
        [
            self.include,
            self.position,
            self.kind,
            self.size,
            self.min,
            self.calc,
            self.frag,
            self.compact,
        ]
    }

    fn show(&self, row: &PartitionRow) {
        set_check(self.include, row.selected);
        let min = match &row.min {
            MinCell::Size(min) => format_size(*min),
            MinCell::Pending(phase) => phase.clone(),
            MinCell::CanCalc | MinCell::Empty => String::new(),
        };
        ui::set_text(self.min, &min);
        ui::set_visible(self.calc, row.min == MinCell::CanCalc);
        let frag = row.fragmentation.map(|p| format!("{p:.0}%"));
        ui::set_text(self.frag, frag.as_deref().unwrap_or(""));
        match row.compact {
            CompactCell::Hidden => ui::set_visible(self.compact, false),
            CompactCell::NotImplemented => {
                ui::set_visible(self.compact, true);
                ui::set_enabled(self.compact, false);
                set_check(self.compact, false);
            }
            CompactCell::Toggle(on) => {
                ui::set_visible(self.compact, true);
                ui::set_enabled(self.compact, true);
                set_check(self.compact, on);
            }
        }
    }

    fn command(&self, from: HWND) -> Option<BackupAction> {
        if from == self.include {
            Some(BackupAction::SetPartition(self.index, checked(from)))
        } else if from == self.calc {
            Some(BackupAction::CalcMin(self.index))
        } else if from == self.compact {
            Some(BackupAction::SetCompact(self.index, checked(from)))
        } else {
            None
        }
    }
}

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    source: String,
    has_local: bool,
    remote: bool,
    can_scan: Option<(&'static str, &'static str)>,
    job: Option<(Snapshot, bool)>,
    table: Option<String>,
    rows: Vec<PartitionRow>,
    error: Option<String>,
    destination: String,
    name: String,
    format: usize,
    chd: bool,
    checksum: usize,
    sector_by_sector: bool,
    size_editable: bool,
    resize: bool,
    keep_swap: bool,
    split_shown: bool,
    split_editable: bool,
    split: bool,
    split_mib: u32,
    running: bool,
    cancelling: bool,
    pending: bool,
    start_label: &'static str,
    can_start: bool,
    why: Option<String>,
    outcome: Option<Result<(), String>>,
    run: Option<String>,
    pct: Option<u32>,
}

pub struct BackupScreen {
    ctrl: RefCell<BackupController>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    devices: RefCell<Vec<DiskDevice>>,
    /// Another tab's disk job, which locks this one.
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<BackupScreen>,
    page: HWND,
    busy_note: HWND,
    source_caption: HWND,
    source: HWND,
    open_image: HWND,
    remote_open: HWND,
    close: HWND,
    clear_remote: HWND,
    scan: HWND,
    job: HWND,
    job_progress: HWND,
    cancel_scan: HWND,
    parts_caption: HWND,
    all: HWND,
    none: HWND,
    table: HWND,
    heads: [HWND; 7],
    rows: RefCell<Vec<Row>>,
    error: HWND,
    destination_caption: HWND,
    destination: HWND,
    browse: HWND,
    name_caption: HWND,
    name: HWND,
    format_caption: HWND,
    format: HWND,
    chd: ChdControl,
    sector_by_sector: HWND,
    resize: HWND,
    keep_swap: HWND,
    split: HWND,
    split_caption: HWND,
    split_mib: HWND,
    checksum_caption: HWND,
    checksum: HWND,
    outcome: HWND,
    run: HWND,
    progress: HWND,
    start: HWND,
    cancel_pending: HWND,
    cancel: HWND,
    why: HWND,
    shape: RefCell<Vec<bool>>,
    vhd: RefCell<Option<Rc<VhdWindow>>>,
    remote: RefCell<Option<Rc<RemoteWindow>>>,
}

impl BackupScreen {
    pub fn build(
        page: HWND,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<BackupScreen> {
        let split_style = WINDOW_STYLE(BS_SPLITBUTTON as u32 | WS_TABSTOP.0);
        let source = ui::child(
            page,
            w!("BUTTON"),
            "",
            split_style,
            WINDOW_EX_STYLE(0),
            ids::SOURCE,
        );
        let progress = || {
            ui::child(
                page,
                w!("msctls_progress32"),
                "",
                WINDOW_STYLE(0),
                WINDOW_EX_STYLE(0),
                "",
            )
        };
        let formats: Vec<&str> = FORMATS.iter().map(|f| f.1).collect();
        let checksums: Vec<&str> = CHECKSUMS.iter().map(|c| c.1).collect();
        let mut custom = ChdOptionsControl::new(ChdProfile::Hd);
        custom.custom = BackupController::saved_chd_custom(custom.custom.clone());
        let chd = ChdControl::build(
            page,
            ChdIds {
                mode: ids::CHD_MODE,
                hunk: ids::CHD_HUNK,
                codec: ids::CHD_CODEC,
            },
            custom,
            ChdProfile::Hd,
        );
        let number = WINDOW_STYLE(ES_NUMBER as u32 | WS_TABSTOP.0);
        let split_mib = ui::child(
            page,
            w!("EDIT"),
            "",
            number,
            windows::Win32::UI::WindowsAndMessaging::WS_EX_CLIENTEDGE,
            ids::SPLIT_MIB,
        );
        let heads =
            ["", "#", "Type", "Size", "Min", "Frag", "Compact"].map(|t| ui::cell_label(page, t));
        let screen = Rc::new_cyclic(|me: &Weak<BackupScreen>| BackupScreen {
            ctrl: RefCell::new(BackupController::new(BackupServices {
                jobs: JobRegistry::new(),
                notify,
            })),
            host,
            recent_files: RefCell::new(recent.list(RecentMode::Backup)),
            recent,
            devices: RefCell::default(),
            busy: Cell::new(None),
            sync: SyncGuard::default(),
            me: me.clone(),
            page,
            busy_note: ui::label(page, ""),
            source_caption: ui::cell_label(page, "Source:"),
            source,
            open_image: ui::button(page, "Open Image File...", ids::OPEN_IMAGE),
            remote_open: ui::button(page, "Remote Drive...", ids::REMOTE_OPEN),
            close: ui::button(page, "Close Drive", ids::CLOSE_SOURCE),
            clear_remote: ui::button(page, "Disconnect", ids::CLEAR_REMOTE),
            scan: ui::button(page, "Load partition info", ids::LOAD_PARTITIONS),
            job: ui::cell_label(page, ""),
            job_progress: progress(),
            cancel_scan: ui::button(page, "Cancel", ids::CANCEL_SCAN),
            parts_caption: ui::cell_label(page, "Partitions"),
            all: ui::button(page, "All", ids::SELECT_ALL),
            none: ui::button(page, "None", ids::SELECT_NONE),
            table: ui::label(page, ""),
            heads,
            rows: RefCell::default(),
            error: ui::label(page, ""),
            destination_caption: ui::cell_label(page, &format!("{}:", labels::DESTINATION)),
            destination: ui::edit(page, ids::DESTINATION),
            browse: ui::button(page, "Browse...", ids::PICK_DESTINATION),
            name_caption: ui::cell_label(page, &format!("{}:", labels::NAME)),
            name: ui::edit(page, ids::NAME),
            format_caption: ui::cell_label(page, &format!("{}:", labels::FORMAT)),
            format: combo(page, ids::FORMAT, &formats),
            chd,
            sector_by_sector: ui::checkbox(page, hints::SECTOR_BY_SECTOR, ids::SECTOR_BY_SECTOR),
            resize: ui::checkbox(page, hints::RESIZE, ids::RESIZE),
            keep_swap: ui::checkbox(page, hints::KEEP_SWAP, ids::KEEP_SWAP),
            split: ui::checkbox(page, "Split archives", ids::SPLIT),
            split_caption: ui::cell_label(page, "Split size (MiB):"),
            split_mib,
            checksum_caption: ui::cell_label(page, "Checksum:"),
            checksum: combo(page, ids::CHECKSUM, &checksums),
            outcome: ui::label(page, ""),
            run: ui::cell_label(page, ""),
            progress: progress(),
            start: ui::button(page, labels::START, ids::START),
            cancel_pending: ui::button(page, "Cancel", ids::CANCEL_PENDING),
            cancel: ui::button(page, "Cancel", ids::CANCEL),
            why: ui::cell_label(page, ""),
            shape: RefCell::default(),
            vhd: RefCell::default(),
            remote: RefCell::default(),
        });
        let weak: Weak<dyn Handler> = screen.me.clone();
        panel::register(page, weak);
        screen.refresh();
        screen
    }

    /// The app's device list; a change under the open device closes or re-reads it.
    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        *self.devices.borrow_mut() = devices.to_vec();
    }

    /// Another tab's job, if any, which locks this tab's start controls.
    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    /// Settings cleared the recent files: the picker forgets its copy too.
    pub fn forget_recents(&self) {
        self.recent_files.borrow_mut().clear();
    }

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    pub fn apply(&self, action: BackupAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(screen) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => BackupAction::Picked { purpose, path },
                // Backup only ever opens pickers.
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            screen.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let (requests, log) = {
            let mut ctrl = self.ctrl.borrow_mut();
            (ctrl.take_requests(), ctrl.take_log())
        };
        for request in requests {
            match request {
                HostRequest::RememberRecent(path) => self.remember(path),
            }
        }
        crate::app::append_log(log);
        self.show_vhd();
        self.show_remote();
        // Off screen nothing needs it, and a job wakes this many times a second.
        if ui::shown(self.page) {
            self.refresh();
        }
    }

    fn remember(&self, path: PathBuf) {
        let list = self
            .recent
            .remember(RecentMode::Backup, &path.display().to_string());
        *self.recent_files.borrow_mut() = list;
    }

    /// What Start carries from the CHD control.
    fn chd_choice(&self) -> Option<ChdChoice> {
        let chd = self.ctrl.borrow().options().compression == CompressionType::Chd;
        chd.then(|| {
            let (options, custom) = self.chd.choice();
            ChdChoice { options, custom }
        })
    }

    /// The Remote Drive window follows the controller: open while it says so.
    fn show_remote(&self) {
        let shown = {
            let ctrl = self.ctrl.borrow();
            ctrl.remote_dialog_open().then(|| RemoteShown {
                address: ctrl.remote_address().to_string(),
                busy: ctrl.remote_busy(),
                error: ctrl.remote_error().map(str::to_string),
                drives: ctrl.remote_listed().then(|| ctrl.remote_drives()),
            })
        };
        let Some(shown) = shown else {
            self.remote.replace(None);
            return;
        };
        let held = self.remote.borrow().clone();
        let w = held.unwrap_or_else(|| {
            let me = self.me.clone();
            let send: Rc<dyn Fn(BackupAction)> = Rc::new(move |a| {
                if let Some(s) = me.upgrade() {
                    s.apply(a);
                }
            });
            // SAFETY: a plain query of our own window.
            let owner = unsafe { GetAncestor(self.page, GA_ROOT) };
            let w = RemoteWindow::open(owner, send);
            *self.remote.borrow_mut() = Some(w.clone());
            w
        });
        w.show(shown);
    }

    /// The VHD options window follows the controller: open while it says so, rebuilt for new rows.
    fn show_vhd(&self) {
        let shown = {
            let ctrl = self.ctrl.borrow();
            ctrl.vhd_open().then(|| VhdShown {
                whole_disk: ctrl.vhd_whole_disk(),
                rows: ctrl
                    .vhd_rows()
                    .iter()
                    .map(|r| (r.clone(), ctrl.min_phase(r.index)))
                    .collect(),
                can_start: !ctrl.is_running() && self.busy.get().is_none(),
            })
        };
        let Some(shown) = shown else {
            self.vhd.replace(None);
            return;
        };
        let rows: Vec<usize> = shown.rows.iter().map(|(r, _)| r.index).collect();
        let held = self.vhd.borrow().clone().filter(|w| w.indices() == rows);
        let w = held.unwrap_or_else(|| {
            self.vhd.replace(None);
            let me = self.me.clone();
            let send: Rc<dyn Fn(BackupAction)> = Rc::new(move |a| {
                if let Some(s) = me.upgrade() {
                    s.apply(a);
                }
            });
            // SAFETY: a plain query of our own window.
            let owner = unsafe { GetAncestor(self.page, GA_ROOT) };
            let list: Vec<_> = shown.rows.iter().map(|(r, _)| r.clone()).collect();
            let w = VhdWindow::open(owner, send, &list);
            *self.vhd.borrow_mut() = Some(w.clone());
            w
        });
        w.show(shown);
    }

    /// Start's whole rule: asked again at the click, not only when the button was last drawn.
    fn may_start(&self) -> bool {
        let ctrl = self.ctrl.borrow();
        let chd_ok = ctrl.options().compression != CompressionType::Chd || self.chd.is_valid();
        ctrl.can_start() && chd_ok && self.busy.get().is_none()
    }

    fn start(&self) {
        if self.may_start() {
            let chd = self.chd_choice();
            self.apply(BackupAction::Start(chd));
        }
    }

    /// The source button's menu: recent images, devices, then the ways to open one.
    fn source_menu(&self) {
        let recent = self.recent_files.borrow().clone();
        let devices: Vec<DiskDevice> = self.devices.borrow().clone();
        let mut entries: Vec<(String, bool)> = recent.iter().map(|p| (p.clone(), true)).collect();
        if !recent.is_empty() {
            entries.push((String::new(), false));
        }
        if devices.is_empty() {
            entries.push((no_devices_hint().to_string(), false));
        }
        entries.extend(devices.iter().map(|d| (d.display_name(), true)));
        entries.push((String::new(), false));
        entries.push(("Open Image File...".into(), true));
        entries.push(("Remote Drive...".into(), true));
        let mut r = windows::Win32::Foundation::RECT::default();
        // SAFETY: plain queries of our own windows.
        let (owner, picked) = unsafe {
            let _ = GetWindowRect(self.source, &mut r);
            let owner = GetAncestor(self.page, GA_ROOT);
            (
                owner,
                crate::menu::choose(owner, &entries, r.left, r.bottom),
            )
        };
        let _ = owner;
        let Some(i) = picked else {
            return;
        };
        let devices_at =
            recent.len() + usize::from(!recent.is_empty()) + usize::from(devices.is_empty());
        let action = if i < recent.len() {
            BackupAction::OpenImage {
                path: PathBuf::from(&recent[i]),
                rename: true,
            }
        } else if let Some(device) = i.checked_sub(devices_at).and_then(|d| devices.get(d)) {
            BackupAction::SelectDevice {
                slot: i - devices_at,
                device: device.clone(),
            }
        } else if i == entries.len() - 2 {
            BackupAction::PickImage
        } else if i == entries.len() - 1 {
            BackupAction::RemoteOpen
        } else {
            return;
        };
        self.apply(action);
    }

    fn read(&self) -> Shown {
        let devices = self.devices.borrow();
        let ctrl = self.ctrl.borrow();
        let o = ctrl.options();
        let mut jobs = Vec::new();
        jobs.extend(ctrl.decoding().map(|s| (s, false)));
        jobs.extend(ctrl.scanning().map(|s| (s, true)));
        let (run, pct) = match ctrl.progress() {
            Some(b) => {
                let pct = (b.total_bytes > 0)
                    .then(|| (b.current_bytes.saturating_mul(100) / b.total_bytes).min(100) as u32);
                let line = format!(
                    "{}: {} of {}",
                    b.operation,
                    format_size(b.current_bytes),
                    format_size(b.total_bytes)
                );
                (Some(line), pct)
            }
            None => (None, None),
        };
        Shown {
            source: ctrl.source_label(devices.as_slice()),
            has_local: ctrl.has_local_source(),
            remote: ctrl.remote_active(),
            can_scan: ctrl.can_scan_device().then(|| ctrl.scan_button()),
            job: jobs.into_iter().next(),
            table: ctrl.table_line(),
            rows: ctrl.rows(),
            error: ctrl.load_error().map(str::to_string),
            destination: ctrl
                .destination()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            name: ctrl.name().to_string(),
            format: FORMATS
                .iter()
                .position(|f| f.0 == o.compression)
                .unwrap_or(0),
            chd: o.compression == CompressionType::Chd,
            checksum: CHECKSUMS
                .iter()
                .position(|c| c.0 == o.checksum)
                .unwrap_or(0),
            sector_by_sector: o.sector_by_sector,
            size_editable: o.size_options_editable(),
            resize: o.resize,
            keep_swap: o.keep_swap,
            split_shown: o.split_shown(),
            split_editable: o.split_editable(),
            split: o.split,
            split_mib: o.split_mib,
            running: ctrl.is_running(),
            cancelling: ctrl.cancelling(),
            pending: ctrl.start_pending(),
            start_label: ctrl.start_label(),
            can_start: false,
            why: ctrl
                .validate()
                .err()
                .or_else(|| self.busy.get().map(busy_elsewhere_notice)),
            outcome: ctrl.last_result().cloned(),
            run,
            pct,
        }
    }

    /// Read the controller first and release it, then set the controls inside the guard.
    fn refresh(&self) {
        let mut shown = self.read();
        shown.can_start = self.may_start();
        self.sync.run(|| self.show(shown));
        if *self.shape.borrow() != self.shape_now() {
            self.layout();
        }
    }

    fn show(&self, s: Shown) {
        ui::set_text(self.source, &s.source);
        let notice = self.busy.get().map(busy_elsewhere_notice);
        ui::set_text(self.busy_note, notice.as_deref().unwrap_or(""));
        ui::set_visible(self.busy_note, notice.is_some());
        ui::set_visible(self.close, s.has_local);
        ui::set_visible(self.clear_remote, s.remote);
        ui::set_visible(self.scan, s.can_scan.is_some());
        if let Some((label, _)) = s.can_scan {
            ui::set_text(self.scan, label);
        }
        let (line, pct) = s
            .job
            .as_ref()
            .map(|(j, _)| crate::inspect::job_line(j))
            .unwrap_or_default();
        ui::set_text(self.job, &line);
        ui::set_visible(self.job, s.job.is_some());
        crate::inspect::set_progress(self.job_progress, s.job.is_some(), pct);
        ui::set_visible(self.cancel_scan, s.job.as_ref().is_some_and(|(_, c)| *c));
        ui::set_text(self.table, s.table.as_deref().unwrap_or(""));
        self.show_rows(&s.rows);
        ui::set_text(self.error, s.error.as_deref().unwrap_or(""));
        ui::set_visible(self.error, s.error.is_some());
        if !self.typing(self.destination) {
            ui::set_text(self.destination, &s.destination);
        }
        if !self.typing(self.name) {
            ui::set_text(self.name, &s.name);
        }
        select(self.format, s.format);
        self.chd.set_visible(s.chd);
        set_check(self.sector_by_sector, s.sector_by_sector);
        set_check(self.resize, s.resize);
        set_check(self.keep_swap, s.keep_swap);
        ui::set_visible(self.split, s.split_shown);
        set_check(self.split, s.split);
        let mib = s.split_shown && s.split && s.split_editable;
        ui::set_visible(self.split_caption, mib);
        ui::set_visible(self.split_mib, mib);
        if !self.typing(self.split_mib) {
            ui::set_text(self.split_mib, &s.split_mib.to_string());
        }
        select(self.checksum, s.checksum);
        // The footer shows one line: progress, else why Start is off, else the outcome.
        ui::set_visible(self.outcome, s.run.is_none() && (s.running || s.can_start));
        let free = !s.running && self.busy.get().is_none();
        for h in self.form_controls() {
            ui::set_enabled(h, free);
        }
        ui::set_enabled(self.split, free && s.split_editable);
        ui::set_enabled(self.resize, free && s.size_editable);
        ui::set_enabled(self.keep_swap, free && s.size_editable);
        ui::set_visible(self.start, !s.running);
        ui::set_enabled(self.start, s.can_start);
        ui::set_text(self.start, s.start_label);
        ui::set_visible(self.cancel_pending, s.pending);
        ui::set_visible(self.cancel, s.running);
        ui::set_enabled(self.cancel, !s.cancelling);
        ui::set_text(
            self.cancel,
            if s.cancelling {
                "Cancelling..."
            } else {
                "Cancel"
            },
        );
        ui::set_text(self.run, s.run.as_deref().unwrap_or(""));
        ui::set_visible(self.run, s.run.is_some());
        crate::inspect::set_progress(
            self.progress,
            s.run.is_some(),
            s.pct.or(Some(0)).filter(|_| s.run.is_some()),
        );
        ui::set_text(self.why, s.why.as_deref().unwrap_or(""));
        ui::set_visible(self.why, !s.running && !s.can_start);
        let (outcome, tone) = match &s.outcome {
            Some(Ok(())) => (labels::DONE.to_string(), None),
            Some(Err(e)) => (
                e.clone(),
                Some(rusty_backup::controller::commander::Tone::Error),
            ),
            None => (String::new(), None),
        };
        ui::set_text(self.outcome, &outcome);
        crate::theme::set_tone(self.outcome, tone);
    }

    /// The field has the keyboard: what is typed there wins over the controller's copy.
    fn typing(&self, h: HWND) -> bool {
        // SAFETY: a plain query.
        unsafe { windows::Win32::UI::Input::KeyboardAndMouse::GetFocus() == h }
    }

    /// The controls a running backup or another tab's job locks.
    fn form_controls(&self) -> Vec<HWND> {
        let mut v = vec![
            self.source,
            self.open_image,
            self.remote_open,
            self.close,
            self.scan,
            self.all,
            self.none,
            self.destination,
            self.browse,
            self.name,
            self.format,
            self.sector_by_sector,
            self.split_mib,
            self.checksum,
        ];
        for r in self.rows.borrow().iter() {
            v.extend([r.include, r.calc]);
        }
        v.extend(self.chd.combos());
        v
    }

    /// Build the rows again when the partitions change, then show each.
    fn show_rows(&self, rows: &[PartitionRow]) {
        let same = {
            let held = self.rows.borrow();
            held.len() == rows.len() && held.iter().zip(rows).all(|(h, r)| h.index == r.index)
        };
        if !same {
            for r in self.rows.borrow_mut().drain(..) {
                for h in r.all() {
                    // SAFETY: our own controls, built again for the new partitions.
                    let _ = unsafe { DestroyWindow(h) };
                }
            }
            *self.rows.borrow_mut() = rows.iter().map(|r| Row::build(self.page, r)).collect();
        }
        for (view, row) in self.rows.borrow().iter().zip(rows) {
            view.show(row);
        }
        let any = !rows.is_empty();
        for h in [self.parts_caption, self.all, self.none] {
            ui::set_visible(h, any);
        }
        for h in self.heads {
            ui::set_visible(h, any);
        }
    }

    fn shape_now(&self) -> Vec<bool> {
        let mut all = vec![
            self.busy_note,
            self.close,
            self.clear_remote,
            self.scan,
            self.job,
            self.cancel_scan,
            self.all,
            self.split_mib,
            self.run,
            self.cancel,
            self.cancel_pending,
            self.why,
            self.outcome,
        ];
        all.extend(self.chd.combos());
        all.push(self.error);
        let mut shape: Vec<bool> = all.iter().map(|h| ui::shown(*h)).collect();
        shape.push(!self.rows.borrow().is_empty());
        shape.extend(self.rows.borrow().iter().map(|r| ui::shown(r.calc)));
        shape
    }

    /// A compact form: the screen shares the window with the log, so nothing scrolls off.
    fn layout(&self) {
        *self.shape.borrow_mut() = self.shape_now();
        let (area, dpi) = panel::client(self.page);
        let row_h = Size::Fixed(layout::ROW);
        let line = |h: HWND| Node::leaf(h, Size::Stretch(1), Size::Fixed(layout::ROW - 8));
        let fixed = |h: HWND, w: i32| Node::leaf(h, Size::Fixed(w), row_h);
        let shown = |h: HWND, w: i32| ui::shown(h).then(|| Node::button(h, w));
        let mut col = Vec::new();
        if ui::shown(self.busy_note) {
            col.push(line(self.busy_note));
        }
        let mut source = vec![
            fixed(self.source_caption, 60),
            Node::leaf(self.source, Size::Stretch(1), row_h),
            Node::button(self.open_image, 130),
            Node::button(self.remote_open, 115),
        ];
        source.extend(
            [(self.close, 95), (self.clear_remote, 90), (self.scan, 140)]
                .iter()
                .filter_map(|(h, w)| shown(*h, *w)),
        );
        col.push(Node::row(source));
        if ui::shown(self.job) {
            let mut r = vec![
                Node::leaf(self.job, Size::Stretch(1), row_h),
                Node::leaf(
                    self.job_progress,
                    Size::Fixed(160),
                    Size::Fixed(layout::ROW - 6),
                ),
            ];
            if ui::shown(self.cancel_scan) {
                r.push(Node::button(self.cancel_scan, 70));
            }
            col.push(Node::row(r));
        }
        let rows = self.rows.borrow();
        if !rows.is_empty() {
            col.push(Node::row(vec![
                fixed(self.parts_caption, 80),
                Node::button(self.all, 50),
                Node::button(self.none, 55),
                Node::leaf(self.table, Size::Stretch(1), row_h),
            ]));
            let widths = [24, 30, 220, 90, 160, 60, 60];
            col.push(Node::row(
                self.heads
                    .iter()
                    .zip(widths)
                    .map(|(h, w)| fixed(*h, w))
                    .collect(),
            ));
            for r in rows.iter() {
                let min = match ui::shown(r.calc) {
                    true => Node::button(r.calc, widths[4]),
                    false => fixed(r.min, widths[4]),
                };
                let mut row: Vec<Node<HWND>> = [r.include, r.position, r.kind, r.size]
                    .iter()
                    .zip(widths)
                    .map(|(h, w)| fixed(*h, w))
                    .collect();
                row.push(min);
                row.push(fixed(r.frag, widths[5]));
                row.push(fixed(r.compact, widths[6]));
                col.push(Node::row(row));
            }
        }
        drop(rows);
        if ui::shown(self.error) {
            col.push(line(self.error));
        }
        col.push(Node::row(vec![
            fixed(self.destination_caption, 110),
            Node::field(self.destination),
            Node::button(self.browse, 85),
        ]));
        col.push(Node::row(vec![
            fixed(self.name_caption, 110),
            Node::field(self.name),
            fixed(self.format_caption, 105),
            fixed(self.format, 160),
            fixed(self.checksum_caption, 75),
            fixed(self.checksum, 120),
        ]));
        col.extend(self.chd.rows());
        col.push(Node::row(vec![
            Node::leaf(self.sector_by_sector, Size::Stretch(1), row_h),
            Node::leaf(self.resize, Size::Stretch(1), row_h),
        ]));
        let mut split = vec![Node::leaf(self.keep_swap, Size::Stretch(1), row_h)];
        let mut right = Vec::new();
        if ui::shown(self.split) {
            right.push(fixed(self.split, 130));
            if ui::shown(self.split_mib) {
                right.push(fixed(self.split_caption, 115));
                right.push(fixed(self.split_mib, 70));
            }
        }
        right.push(Node::Space(Size::Stretch(1)));
        split.push(Node::Row {
            children: right,
            gap: layout::GAP,
        });
        col.push(Node::row(split));
        col.push(Node::Space(Size::Stretch(1)));
        // One footer row: the buttons, then the progress, or why Start is off, or how it went.
        let mut foot: Vec<Node<HWND>> = [
            (self.start, 120),
            (self.cancel_pending, 80),
            (self.cancel, 100),
        ]
        .iter()
        .filter_map(|(h, w)| shown(*h, *w))
        .collect();
        if ui::shown(self.run) {
            foot.push(fixed(self.progress, 200));
            foot.push(Node::leaf(self.run, Size::Stretch(1), row_h));
        } else if ui::shown(self.why) {
            foot.push(Node::leaf(self.why, Size::Stretch(1), row_h));
        } else {
            foot.push(Node::leaf(self.outcome, Size::Stretch(1), row_h));
        }
        col.push(Node::row(foot));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in panel::place(self.page, tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }
}

impl Handler for BackupScreen {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        if self.chd.command(from, code) {
            self.refresh();
            return;
        }
        if code == EN_CHANGE {
            let text = ui::text(from);
            let action = if from == self.destination {
                let path = (!text.trim().is_empty()).then(|| PathBuf::from(text.trim()));
                BackupAction::SetDestination(path)
            } else if from == self.name {
                BackupAction::SetName(text)
            } else if from == self.split_mib {
                match text.trim().parse::<u32>() {
                    Ok(n) if (SPLIT_MIB_MIN..=SPLIT_MIB_MAX).contains(&n) => {
                        BackupAction::SetSplitMib(n)
                    }
                    _ => return,
                }
            } else {
                return;
            };
            self.apply(action);
            return;
        }
        if code == CBN_SELCHANGE {
            let action = if from == self.format {
                picked(from)
                    .and_then(|i| FORMATS.get(i))
                    .map(|f| BackupAction::SetFormat(f.0))
            } else if from == self.checksum {
                picked(from)
                    .and_then(|i| CHECKSUMS.get(i))
                    .map(|c| BackupAction::SetChecksum(c.0))
            } else {
                None
            };
            if let Some(a) = action {
                self.apply(a);
            }
            return;
        }
        if code != BN_CLICKED {
            return;
        }
        let row = self.rows.borrow().iter().find_map(|r| r.command(from));
        let action = if let Some(a) = row {
            a
        } else if from == self.source {
            self.source_menu();
            return;
        } else if from == self.open_image {
            BackupAction::PickImage
        } else if from == self.remote_open {
            BackupAction::RemoteOpen
        } else if from == self.close {
            BackupAction::CloseSource
        } else if from == self.clear_remote {
            BackupAction::RemoteClear
        } else if from == self.scan {
            BackupAction::ScanPartitions
        } else if from == self.cancel_scan {
            BackupAction::CancelScan
        } else if from == self.all {
            BackupAction::SelectAll
        } else if from == self.none {
            BackupAction::SelectNone
        } else if from == self.browse {
            BackupAction::PickDestination
        } else if from == self.sector_by_sector {
            BackupAction::SetSectorBySector(checked(from))
        } else if from == self.resize {
            BackupAction::SetResize(checked(from))
        } else if from == self.keep_swap {
            BackupAction::SetKeepSwap(checked(from))
        } else if from == self.split {
            BackupAction::SetSplit(checked(from))
        } else if from == self.start {
            self.start();
            return;
        } else if from == self.cancel_pending {
            BackupAction::CancelPendingStart
        } else if from == self.cancel {
            BackupAction::Cancel
        } else {
            return;
        };
        self.apply(action);
    }

    fn notify(&self, hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        if hdr.hwndFrom == self.source && hdr.code == BCN_DROPDOWN {
            self.source_menu();
            return Some(LRESULT(0));
        }
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
