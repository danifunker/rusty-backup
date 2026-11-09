//! The Inspect screen over `controller::inspect::InspectController`; `src/gui/inspect_tab.rs` is the reference.
//!
//! Slice 1: the source (a split button whose menu lists recent files and devices, beside Open
//! File and Open Backup Folder), the facts, the running jobs, the tools, the partition list (a
//! virtual list view) with the tools that act on its selected row, and the Check and CHD Info
//! windows. Still to come: the partition bar, Export, the table editor, Expand Image, Resize,
//! Defragment, Alto resize, the remote listing (slice 7b) and browsing (slice 2).

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{
    unelevated_device_note, DiskDevice, Notify, Progress, RecentHandle, RecentMode, Snapshot,
};
use rusty_backup::controller::inspect::{
    format_size, ids, purpose, HostRequest, InspectAction, InspectController, InspectServices,
    InspectSource, JobRegistry, LayoutSegment, MinCell, PartitionRow,
};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{Controller, Effect};
use windows::core::{w, PCWSTR, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::UI::Controls::{
    BCN_DROPDOWN, BS_SPLITBUTTON, LIST_VIEW_ITEM_STATE_FLAGS, LVCFMT_RIGHT, LVCF_FMT, LVCF_TEXT,
    LVCF_WIDTH, LVCOLUMNW, LVIF_STATE, LVIF_TEXT, LVIS_FOCUSED, LVIS_SELECTED, LVITEMW,
    LVM_GETNEXTITEM, LVM_INSERTCOLUMNW, LVM_REDRAWITEMS, LVM_SETEXTENDEDLISTVIEWSTYLE,
    LVM_SETITEMCOUNT, LVM_SETITEMSTATE, LVNI_SELECTED, LVN_GETDISPINFOW, LVN_ITEMCHANGED,
    LVSICF_NOSCROLL, LVS_EX_DOUBLEBUFFER, LVS_EX_FULLROWSELECT, LVS_OWNERDATA, LVS_REPORT,
    LVS_SHOWSELALWAYS, LVS_SINGLESEL, NMHDR, NMLISTVIEW, NMLVDISPINFOW, PBM_SETMARQUEE, PBM_SETPOS,
    PBM_SETRANGE32, PBS_MARQUEE,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    AppendMenuW, CreatePopupMenu, DestroyMenu, GetWindowLongPtrW, GetWindowRect, SendMessageW,
    SetWindowLongPtrW, TrackPopupMenu, BN_CLICKED, GWL_STYLE, MF_GRAYED, MF_SEPARATOR, MF_STRING,
    TPM_RETURNCMD, WINDOW_EX_STYLE, WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::alto::AltoWindow;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::expand_hfs::ExpandHfsWindow;
use crate::expand_image::{self, ExpandImageWindow};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::report::{self, ReportWindow};
use crate::resize::ResizeWindow;
use crate::sync::SyncGuard;
use crate::ui;

/// The list's columns: title, width in DIPs, right-aligned.
const COLUMNS: [(&str, i32, bool); 7] = [
    ("#", 80, false),
    ("Type", 200, false),
    ("Volume", 160, false),
    ("Start LBA", 100, true),
    ("Size", 100, true),
    ("Min Size", 100, true),
    ("Block", 80, true),
];

/// Recent files the source menu lists, as egui's picker does.
const RECENT_ROWS: usize = 5;
const MENU_RECENT: usize = 1000;
const MENU_DEVICE: usize = 2000;

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
fn job_line(s: &Snapshot) -> (String, Option<u32>) {
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
    can_expand_image: bool,
    can_edit_table: bool,
    can_close: bool,
    has_chd: bool,
    can_convert: bool,
    check: Option<report::Shown>,
    chd: Option<String>,
}

pub struct InspectScreen {
    ctrl: RefCell<InspectController>,
    notify: Arc<dyn Notify>,
    host: EffectHost,
    owner: HWND,
    recent: RecentHandle,
    devices: RefCell<Vec<DiskDevice>>,
    backup_closed: Cell<bool>,
    convert: RefCell<Option<PathBuf>>,
    sync: SyncGuard,
    me: Weak<InspectScreen>,
    page: HWND,
    source_caption: HWND,
    source: HWND,
    open_file: HWND,
    open_backup: HWND,
    note: HWND,
    flux: HWND,
    error: HWND,
    job: HWND,
    progress: HWND,
    reinspect: HWND,
    convert_btn: HWND,
    bootable: HWND,
    chd: HWND,
    expand_image: HWND,
    resize_btn: HWND,
    resize: RefCell<Option<Rc<ResizeWindow>>>,
    close: HWND,
    details: HWND,
    bar: HWND,
    table: HWND,
    rows: RefCell<Vec<PartitionRow>>,
    /// The row the tools act on: the list's selection, else the first row (as the TUI's cursor).
    cursor: Cell<usize>,
    /// Which optional lines the last layout gave room to, so a change lays the page out again.
    shape: Cell<u32>,
    no_partitions: HWND,
    calc_min: HWND,
    check: HWND,
    expand: HWND,
    defragment: HWND,
    resize_alto: HWND,
    alto: RefCell<Option<Rc<AltoWindow>>>,
    hfs: RefCell<Option<Rc<ExpandHfsWindow>>>,
    expand_window: RefCell<Option<Rc<ExpandImageWindow>>>,
    check_window: RefCell<Option<Rc<ReportWindow>>>,
    chd_window: RefCell<Option<Rc<ReportWindow>>>,
}

impl InspectScreen {
    pub fn build(
        owner: HWND,
        page: HWND,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<Self> {
        let source_caption = ui::label(page, "Source:");
        let split = WINDOW_STYLE(BS_SPLITBUTTON as u32 | WS_TABSTOP.0);
        let source = ui::child(
            page,
            w!("BUTTON"),
            "",
            split,
            WINDOW_EX_STYLE(0),
            ids::SOURCE,
        );
        let open_file = ui::button(page, "Open File...", ids::OPEN_FILE);
        let open_backup = ui::button(page, "Open Backup Folder...", ids::OPEN_BACKUP);
        let note = ui::label(page, "");
        let flux = ui::label(page, "");
        let error = ui::label(page, "");
        let job = ui::label(page, "");
        let progress = ui::child(
            page,
            w!("msctls_progress32"),
            "",
            WINDOW_STYLE(0),
            WINDOW_EX_STYLE(0),
            "",
        );
        let reinspect = ui::button(page, "Re-inspect", ids::REINSPECT);
        let convert_btn = ui::button(page, "Convert...", ids::CONVERT);
        let bootable = ui::button(page, "Make Bootable...", ids::MAKE_BOOTABLE);
        let chd = ui::button(page, "CHD Info", ids::CHD_INFO);
        let expand_image = ui::button(page, "Expand Image...", ids::EXPAND_IMAGE);
        let resize_btn = ui::button(page, "Resize Partitions...", ids::RESIZE);
        let close = ui::button(page, "Close", ids::CLOSE);
        let details = ui::label(page, "");
        let bar_target: Rc<RefCell<Weak<InspectScreen>>> = Rc::default();
        let target = bar_target.clone();
        let bar = crate::partition_bar::build(
            page,
            Box::new(move |p| {
                if let Some(s) = target.borrow().upgrade() {
                    s.apply(InspectAction::SelectPartition(p));
                }
            }),
        );
        let style = LVS_REPORT | LVS_OWNERDATA | LVS_SINGLESEL | LVS_SHOWSELALWAYS | WS_TABSTOP.0;
        let table = ui::child(
            page,
            w!("SysListView32"),
            "",
            WINDOW_STYLE(style),
            WS_EX_CLIENTEDGE,
            ids::PARTITION_TABLE,
        );
        // SAFETY: plain list-view setup; each column title outlives its insert.
        unsafe {
            let ex = LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER;
            SendMessageW(
                table,
                LVM_SETEXTENDEDLISTVIEWSTYLE,
                Some(WPARAM(ex as usize)),
                Some(LPARAM(ex as isize)),
            );
            let dpi = GetDpiForWindow(page).max(96);
            for (i, (title, width, right)) in COLUMNS.iter().enumerate() {
                let mut t = ui::wide(title);
                let col = LVCOLUMNW {
                    mask: LVCF_TEXT | LVCF_WIDTH | LVCF_FMT,
                    fmt: if *right {
                        LVCFMT_RIGHT
                    } else {
                        Default::default()
                    },
                    cx: layout::scale(*width, dpi),
                    pszText: PWSTR(t.as_mut_ptr()),
                    ..Default::default()
                };
                SendMessageW(
                    table,
                    LVM_INSERTCOLUMNW,
                    Some(WPARAM(i)),
                    Some(LPARAM(&col as *const _ as isize)),
                );
            }
        }
        let no_partitions = ui::label(page, "No partitions found.");
        let calc_min = ui::button(page, "Calc min", ids::ROW_CALC_MIN);
        let check = ui::button(page, "Check", ids::ROW_CHECK);
        let expand = ui::button(page, "Expand/Export...", ids::ROW_EXPAND_HFS);
        let defragment = ui::button(page, "Defragment...", ids::ROW_DEFRAGMENT);
        let resize_alto = ui::button(page, "Resize...", ids::ROW_RESIZE_ALTO);
        let screen = Rc::new_cyclic(|me: &Weak<InspectScreen>| InspectScreen {
            ctrl: RefCell::new(InspectController::new(InspectServices {
                jobs: JobRegistry::new(),
                notify: notify.clone(),
            })),
            notify,
            host,
            owner,
            recent,
            devices: RefCell::default(),
            backup_closed: Cell::new(false),
            convert: RefCell::default(),
            sync: SyncGuard::default(),
            me: me.clone(),
            page,
            source_caption,
            source,
            open_file,
            open_backup,
            note,
            flux,
            error,
            job,
            progress,
            reinspect,
            convert_btn,
            bootable,
            chd,
            expand_image,
            resize_btn,
            resize: RefCell::default(),
            close,
            details,
            bar,
            table,
            rows: RefCell::default(),
            cursor: Cell::new(0),
            shape: Cell::new(u32::MAX),
            no_partitions,
            calc_min,
            check,
            expand,
            defragment,
            resize_alto,
            alto: RefCell::default(),
            hfs: RefCell::default(),
            expand_window: RefCell::default(),
            check_window: RefCell::default(),
            chd_window: RefCell::default(),
        });
        *bar_target.borrow_mut() = screen.me.clone();
        let weak: Weak<dyn Handler> = screen.me.clone();
        panel::register(page, weak);
        screen.refresh();
        screen
    }

    pub fn apply(&self, action: InspectAction) {
        let effects = self.ctrl.borrow_mut().apply(action);
        self.after(effects);
    }

    pub fn tick(&self) {
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
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// Resize Partitions: a dialog with its own controller, on the table read here.
    fn open_resize(&self) {
        let busy = self.resize.borrow().as_ref().is_some_and(|d| d.is_open());
        let plan = self.ctrl.borrow().resize_plan();
        if let (false, Some(plan)) = (busy, plan) {
            let d = ResizeWindow::open(self.owner, self.notify.clone(), self.host.clone(), plan);
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
                let d = AltoWindow::open(self.owner, self.notify.clone(), self.host.clone(), path);
                *self.alto.borrow_mut() = Some(d);
            }
            Err(e) => crate::app::append_log(vec![(LogLevel::Error, e)]),
        }
    }

    /// The app's device list; a change under the open device closes or re-inspects it.
    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        let resize = self.resize.borrow().clone();
        if let Some(d) = resize {
            d.set_devices(devices);
        }
        *self.devices.borrow_mut() = devices.to_vec();
    }

    /// The disk job running here, by name, for the other tabs' locks.
    #[allow(dead_code)] // read by the app once a second tab runs jobs
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
        } else {
            None
        }
    }

    pub fn take_backup_closed(&self) -> bool {
        self.backup_closed.replace(false)
    }

    pub fn take_convert(&self) -> Option<PathBuf> {
        self.convert.borrow_mut().take()
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
        let (requests, mut log) = {
            let mut c = self.ctrl.borrow_mut();
            c.set_browse_dirty(false);
            (c.take_requests(), c.take_log())
        };
        for r in requests {
            if let Some(line) = self.carry_out(r) {
                log.push((LogLevel::Warning, line));
            }
        }
        crate::app::append_log(log);
        self.refresh();
    }

    /// Carry out `request`; what this shell cannot do yet says so. No `_` arm.
    fn carry_out(&self, request: HostRequest) -> Option<String> {
        match request {
            HostRequest::CloseBrowse | HostRequest::UpgradeSeekable(_) => None,
            HostRequest::OpenBrowse(_) => {
                Some("Browsing arrives in slice 2 of the Win32 shell.".into())
            }
            HostRequest::OpenOptical(_) => {
                Some("The Optical screen arrives in slice 6 of the Win32 shell.".into())
            }
            HostRequest::OpenArchive(_) => {
                Some("The Archives screen arrives in slice 6 of the Win32 shell.".into())
            }
            HostRequest::BackupClosed => {
                self.backup_closed.set(true);
                None
            }
            HostRequest::ExpandHfs(source) => {
                let d = ExpandHfsWindow::open(
                    self.owner,
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
            can_expand_image: c.can_expand_image(),
            can_edit_table: c.can_edit_table(),
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
        }
    }

    /// Read the controller first and release it, then set the controls inside the guard.
    fn refresh(&self) {
        let shown = self.read();
        self.sync.run(|| self.show(shown));
    }

    fn show(&self, s: Shown) {
        ui::set_text(self.source, &s.source);
        let note = InspectController::needs_elevation_note().then(unelevated_device_note);
        ui::set_text(self.note, note.unwrap_or(""));
        ui::set_text(self.flux, s.flux.as_deref().unwrap_or(""));
        ui::set_text(
            self.error,
            &s.error.map(|e| format!("Error: {e}")).unwrap_or_default(),
        );
        self.show_job(s.jobs.first());
        ui::set_enabled(self.reinspect, s.can_reinspect);
        ui::set_enabled(self.bootable, s.can_bootable);
        ui::set_enabled(self.expand_image, s.can_expand_image);
        let resizing = self
            .resize
            .borrow()
            .as_ref()
            .is_some_and(|d| d.is_running());
        ui::set_enabled(self.resize_btn, s.can_edit_table && !resizing);
        ui::set_visible(self.convert_btn, s.can_convert);
        ui::set_visible(self.chd, s.has_chd);
        ui::set_visible(self.close, s.can_close);
        ui::set_text(self.details, &s.details.join("\r\n"));
        ui::set_visible(self.no_partitions, s.has_table && s.rows.is_empty());
        ui::set_visible(self.bar, s.has_table && !s.rows.is_empty());
        crate::partition_bar::show(self.bar, s.segments, s.selected);
        self.show_rows(s.rows);
        if self.shape.get() != self.shape_now() {
            self.layout();
        }
        // A click on the bar selects the partition; the list follows it.
        let at = s
            .selected
            .and_then(|p| self.rows.borrow().iter().position(|r| r.index == p));
        if let Some(i) = at.filter(|i| Some(*i) != self.selected()) {
            self.cursor.set(i);
            self.select(i);
        }
        self.show_windows(s.check, s.chd);
    }

    fn show_job(&self, job: Option<&Snapshot>) {
        let (line, pct) = job.map(job_line).unwrap_or_default();
        ui::set_text(self.job, &line);
        ui::set_visible(self.progress, job.is_some());
        // SAFETY: plain progress-bar messages; a marquee needs its style bit, set here.
        unsafe {
            let style = GetWindowLongPtrW(self.progress, GWL_STYLE) as u32;
            let marquee = job.is_some() && pct.is_none();
            let want = if marquee {
                style | PBS_MARQUEE
            } else {
                style & !PBS_MARQUEE
            };
            if want != style {
                SetWindowLongPtrW(self.progress, GWL_STYLE, want as isize);
            }
            SendMessageW(
                self.progress,
                PBM_SETMARQUEE,
                Some(WPARAM(marquee as usize)),
                Some(LPARAM(30)),
            );
            SendMessageW(
                self.progress,
                PBM_SETRANGE32,
                Some(WPARAM(0)),
                Some(LPARAM(100)),
            );
            SendMessageW(
                self.progress,
                PBM_SETPOS,
                Some(WPARAM(pct.unwrap_or(0) as usize)),
                None,
            );
        }
    }

    fn show_rows(&self, rows: Vec<PartitionRow>) {
        if *self.rows.borrow() != rows {
            let count = rows.len();
            *self.rows.borrow_mut() = rows;
            // SAFETY: the virtual list reads its text back through LVN_GETDISPINFO.
            unsafe {
                SendMessageW(
                    self.table,
                    LVM_SETITEMCOUNT,
                    Some(WPARAM(count)),
                    Some(LPARAM(LVSICF_NOSCROLL as isize)),
                );
                SendMessageW(
                    self.table,
                    LVM_REDRAWITEMS,
                    Some(WPARAM(0)),
                    Some(LPARAM(count as isize)),
                );
            }
            if self.cursor.get() >= count {
                self.cursor.set(0);
            }
            if count > 0 && self.selected().is_none() {
                self.select(self.cursor.get());
            }
        }
        let rows = self.rows.borrow();
        let row = rows.get(self.cursor.get());
        let (acts, locked) = row.map(|r| (r.actions, r.locked)).unwrap_or_default();
        let can_calc = row.is_some_and(|r| matches!(r.min, MinCell::CanCalc));
        ui::set_enabled(self.calc_min, can_calc && !locked);
        ui::set_enabled(self.check, acts.browse && acts.check && !locked);
        ui::set_enabled(self.expand, acts.browse && acts.expand_hfs && !locked);
        ui::set_visible(self.defragment, acts.browse && acts.defragment);
        ui::set_enabled(self.defragment, !locked);
        ui::set_visible(self.resize_alto, acts.browse && acts.resize_alto);
        ui::set_enabled(self.resize_alto, !locked);
    }

    /// Open, update or close the Check and CHD Info windows to match the controller.
    fn show_windows(&self, check: Option<report::Shown>, chd: Option<String>) {
        match check {
            Some(shown) => self
                .window_for(&self.check_window, "Filesystem Check Results", true)
                .show(&shown),
            None => {
                self.check_window.replace(None);
            }
        }
        match chd {
            Some(text) => {
                self.window_for(&self.chd_window, "CHD Info", false)
                    .show(&report::Shown {
                        headline: String::new(),
                        stats: String::new(),
                        lines: text.lines().map(str::to_string).collect(),
                        toggle: None,
                        button: false,
                    })
            }
            None => {
                self.chd_window.replace(None);
            }
        }
    }

    fn window_for(
        &self,
        slot: &RefCell<Option<Rc<ReportWindow>>>,
        title: &str,
        check: bool,
    ) -> Rc<ReportWindow> {
        if let Some(w) = slot.borrow().clone() {
            return w;
        }
        let me = self.me.clone();
        let on_event = Box::new(move |event: report::Event| {
            let Some(s) = me.upgrade() else { return };
            let action = match (check, event) {
                (true, report::Event::Close) => InspectAction::CloseCheck,
                (true, report::Event::Toggle(on)) => InspectAction::ShowCheckDebug(on),
                (true, report::Event::Button) => InspectAction::Repair,
                (false, _) => InspectAction::CloseChdInfo,
            };
            s.apply(action);
        });
        let (toggle, button) = if check {
            (
                Some(("Show debug messages", ids::CHECK_DEBUG)),
                Some(("Repair", ids::CHECK_REPAIR)),
            )
        } else {
            (None, None)
        };
        let w = ReportWindow::open(self.owner, title, toggle, button, on_event);
        *slot.borrow_mut() = Some(w.clone());
        w
    }

    fn selected(&self) -> Option<usize> {
        // SAFETY: a plain list-view query.
        let i = unsafe {
            SendMessageW(
                self.table,
                LVM_GETNEXTITEM,
                Some(WPARAM(usize::MAX)),
                Some(LPARAM(LVNI_SELECTED as isize)),
            )
            .0
        };
        usize::try_from(i).ok()
    }

    /// Select list row `i` as the shell's own change: the guard keeps it from echoing back.
    fn select(&self, i: usize) {
        let both = LIST_VIEW_ITEM_STATE_FLAGS(LVIS_SELECTED.0 | LVIS_FOCUSED.0);
        let item = LVITEMW {
            stateMask: both,
            state: both,
            ..Default::default()
        };
        self.sync.run(|| {
            // SAFETY: LVM_SETITEMSTATE reads the item we pass.
            unsafe {
                SendMessageW(
                    self.table,
                    LVM_SETITEMSTATE,
                    Some(WPARAM(i)),
                    Some(LPARAM(&item as *const _ as isize)),
                );
            }
        });
    }

    /// The source menu: recent files and devices; `None` when nothing was picked.
    fn source_menu(&self) -> Option<InspectAction> {
        let recent: Vec<String> = self
            .recent
            .list(RecentMode::Inspect)
            .into_iter()
            .take(RECENT_ROWS)
            .collect();
        let devices = self.devices.borrow().clone();
        // SAFETY: a popup menu we create, track and destroy; each string outlives its append.
        let picked = unsafe {
            let menu = CreatePopupMenu().ok()?;
            let add = |flags, id: usize, text: &str| {
                let t = ui::wide(&ui::plain(text));
                let _ = AppendMenuW(menu, flags, id, PCWSTR(t.as_ptr()));
            };
            add(MF_STRING | MF_GRAYED, 0, "Recent files");
            if recent.is_empty() {
                add(MF_STRING | MF_GRAYED, 0, "    (none yet)");
            }
            for (i, path) in recent.iter().enumerate() {
                add(MF_STRING, MENU_RECENT + i, path);
            }
            let _ = AppendMenuW(menu, MF_SEPARATOR, 0, PCWSTR::null());
            add(MF_STRING | MF_GRAYED, 0, "Devices");
            if devices.is_empty() {
                add(
                    MF_STRING | MF_GRAYED,
                    0,
                    "    (none listed; Show Physical Devices may find more)",
                );
            }
            for (i, d) in devices.iter().enumerate() {
                add(MF_STRING, MENU_DEVICE + i, &d.display_name());
            }
            let mut r = RECT::default();
            let _ = GetWindowRect(self.source, &mut r);
            let id = TrackPopupMenu(menu, TPM_RETURNCMD, r.left, r.bottom, None, self.page, None).0
                as usize;
            let _ = DestroyMenu(menu);
            id
        };
        if (MENU_RECENT..MENU_RECENT + RECENT_ROWS).contains(&picked) {
            let path = PathBuf::from(recent.get(picked - MENU_RECENT)?);
            self.remember(&path);
            Some(InspectAction::Open(InspectSource::ImageFile(path)))
        } else if picked >= MENU_DEVICE {
            let slot = picked - MENU_DEVICE;
            let identity = devices.get(slot)?.identity();
            Some(InspectAction::Open(InspectSource::Device {
                slot,
                identity,
            }))
        } else {
            None
        }
    }

    /// The optional lines' state, one bit each, and the facts' line count.
    fn shape_now(&self) -> u32 {
        let has = |h: HWND| !ui::text(h).is_empty();
        let bits = [
            has(self.note),
            has(self.flux),
            has(self.error),
            ui::shown(self.progress),
            ui::shown(self.bar),
            ui::shown(self.no_partitions),
        ];
        let mut v = ui::text(self.details).lines().count() as u32;
        for (i, b) in bits.iter().enumerate() {
            v |= (*b as u32) << (8 + i);
        }
        v
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.page);
        // A line left out of the tree must not keep its old place and paint over its neighbours.
        let optional = [
            self.note,
            self.flux,
            self.error,
            self.job,
            self.progress,
            self.details,
            self.bar,
            self.no_partitions,
        ];
        for h in optional {
            ui::move_to(h, layout::Rect::default());
        }
        let line =
            |h: HWND, n: i32| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n));
        let mut tree = Node::inset(
            Node::column(vec![Node::row(vec![
                Node::leaf(
                    self.source_caption,
                    Size::Fixed(60),
                    Size::Fixed(layout::ROW - 8),
                ),
                Node::leaf(self.source, Size::Stretch(1), Size::Fixed(layout::ROW)),
                Node::button(self.open_file, 110),
                Node::button(self.open_backup, 160),
            ])]),
            layout::MARGIN,
        );
        // Only the lines with something to say take room; the rest of the page moves up.
        let Node::Inset { child, .. } = &mut tree else {
            unreachable!()
        };
        let Node::Column { children, .. } = child.as_mut() else {
            unreachable!()
        };
        let lines = |h: HWND| {
            let t = ui::text(h);
            if t.is_empty() {
                0
            } else {
                t.lines().count() as i32
            }
        };
        let note = (lines(self.note) > 0).then(|| line(self.note, 2));
        children.extend(note);
        children.extend((lines(self.flux) > 0).then(|| line(self.flux, 1)));
        children.extend((lines(self.error) > 0).then(|| line(self.error, 1)));
        if ui::shown(self.progress) {
            children.push(Node::row(vec![
                Node::leaf(self.job, Size::Stretch(2), Size::Fixed(layout::ROW - 8)),
                Node::leaf(
                    self.progress,
                    Size::Stretch(1),
                    Size::Fixed(layout::ROW - 8),
                ),
            ]));
        }
        children.extend([Node::row(vec![
            Node::button(self.reinspect, 90),
            Node::button(self.convert_btn, 90),
            Node::button(self.bootable, 120),
            Node::button(self.chd, 80),
            Node::button(self.expand_image, 120),
            Node::button(self.resize_btn, 140),
            Node::Space(Size::Stretch(1)),
            Node::button(self.close, 80),
        ])]);
        children.extend((lines(self.details) > 0).then(|| line(self.details, lines(self.details))));
        if ui::shown(self.bar) {
            children.push(Node::leaf(
                self.bar,
                Size::Stretch(1),
                Size::Fixed(crate::partition_bar::HEIGHT),
            ));
        }
        children.push(Node::leaf(self.table, Size::Stretch(1), Size::Stretch(1)));
        if ui::shown(self.no_partitions) {
            children.push(line(self.no_partitions, 1));
        }
        children.push(Node::row(vec![
            Node::button(self.calc_min, 80),
            Node::button(self.check, 80),
            Node::button(self.expand, 130),
            Node::button(self.defragment, 110),
            Node::button(self.resize_alto, 90),
            Node::Space(Size::Stretch(1)),
        ]));
        self.shape.set(self.shape_now());
        for (h, r) in layout::layout(&tree, area, dpi) {
            ui::move_to(h, r);
        }
    }

    /// Fill a cell the virtual list asks for, from the rows last read.
    fn display(&self, info: &mut NMLVDISPINFOW) {
        let item = &mut info.item;
        if item.mask.0 & LVIF_TEXT.0 == 0 || item.pszText.is_null() || item.cchTextMax <= 0 {
            return;
        }
        let rows = self.rows.borrow();
        let Some(row) = usize::try_from(item.iItem).ok().and_then(|i| rows.get(i)) else {
            return;
        };
        let text: Vec<u16> = ui::plain(&cell(row, item.iSubItem as usize))
            .encode_utf16()
            .collect();
        let n = text.len().min(item.cchTextMax as usize - 1);
        // SAFETY: the list gave us a buffer of cchTextMax characters.
        unsafe {
            std::ptr::copy_nonoverlapping(text.as_ptr(), item.pszText.0, n);
            *item.pszText.0.add(n) = 0;
        }
    }

    /// Expand Image's window: the view's own, previewing sizes through the controller.
    fn open_expand_image(&self) {
        if self.expand_window.borrow().is_some() {
            return;
        }
        let me = self.me.clone();
        let w = ExpandImageWindow::open(
            self.owner,
            Box::new(move |event| {
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
            debug_assert_eq!(w.added(), mib);
            w.show(current, new);
        }
    }

    fn row_index(&self) -> Option<usize> {
        self.rows.borrow().get(self.cursor.get()).map(|r| r.index)
    }
}

impl Handler for InspectScreen {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() || code != BN_CLICKED {
            return;
        }
        let action = if from == self.source {
            match self.source_menu() {
                Some(a) => a,
                None => return,
            }
        } else if from == self.open_file {
            InspectAction::ChooseImage
        } else if from == self.open_backup {
            InspectAction::ChooseBackupFolder
        } else if from == self.reinspect {
            InspectAction::Reinspect
        } else if from == self.bootable {
            InspectAction::MakeBootable
        } else if from == self.chd {
            InspectAction::ChdInfo
        } else if from == self.expand_image {
            self.open_expand_image();
            return;
        } else if from == self.resize_btn {
            self.open_resize();
            return;
        } else if from == self.close {
            InspectAction::Close
        } else if from == self.convert_btn {
            let src = self.ctrl.borrow().convert_source();
            *self.convert.borrow_mut() = src;
            crate::app::tick();
            return;
        } else if let Some(index) = self.row_index() {
            if from == self.calc_min {
                InspectAction::CalcMin(index)
            } else if from == self.check {
                InspectAction::Check(index)
            } else if from == self.expand {
                InspectAction::ExpandHfs(index)
            } else if from == self.defragment {
                InspectAction::Defragment(index)
            } else if from == self.resize_alto {
                self.open_alto();
                return;
            } else {
                return;
            }
        } else {
            return;
        };
        self.apply(action);
    }

    fn notify(&self, hdr: &NMHDR, lp: LPARAM) -> Option<LRESULT> {
        if hdr.hwndFrom == self.source && hdr.code == BCN_DROPDOWN {
            if let Some(action) = self.source_menu() {
                self.apply(action);
            }
            return Some(LRESULT(0));
        }
        if hdr.hwndFrom != self.table {
            return None;
        }
        match hdr.code {
            LVN_GETDISPINFOW => {
                // SAFETY: LVN_GETDISPINFOW comes with an NMLVDISPINFOW.
                let info = unsafe { &mut *(lp.0 as *mut NMLVDISPINFOW) };
                self.display(info);
                Some(LRESULT(0))
            }
            LVN_ITEMCHANGED => {
                // SAFETY: LVN_ITEMCHANGED comes with an NMLISTVIEW.
                let nm = unsafe { &*(lp.0 as *const NMLISTVIEW) };
                let now = nm.uNewState & LVIS_SELECTED.0 != 0;
                let was = nm.uOldState & LVIS_SELECTED.0 != 0;
                if nm.uChanged.0 & LVIF_STATE.0 != 0 && now && !was {
                    if let Ok(i) = usize::try_from(nm.iItem) {
                        self.cursor.set(i);
                        let index = self.rows.borrow().get(i).map(|r| r.index);
                        if !self.sync.active() {
                            self.apply(InspectAction::SelectPartition(index));
                        }
                    }
                }
                Some(LRESULT(0))
            }
            _ => None,
        }
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
