//! The Inspect tab over `controller::inspect::InspectController`; `src/gui/inspect_tab.rs` is the reference.
//!
//! Slice 1, first pass: the source row, the table summary, the partition list (a virtual list
//! view) and the row tools that act on the selected row, with Expand HFS Volume. The partition
//! bar, browse view, remote listing and the other tools and windows follow.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{
    unelevated_device_note, DiskDevice, Notify, RecentHandle, RecentMode,
};
use rusty_backup::controller::inspect::{
    format_size, ids, purpose, HostRequest, InspectAction, InspectController, InspectServices,
    JobRegistry, MinCell, PartitionRow,
};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{Controller, Effect};
use windows::core::{w, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{
    LIST_VIEW_ITEM_STATE_FLAGS, LVCFMT_RIGHT, LVCF_FMT, LVCF_TEXT, LVCF_WIDTH, LVCOLUMNW,
    LVIF_STATE, LVIF_TEXT, LVIS_FOCUSED, LVIS_SELECTED, LVITEMW, LVM_GETNEXTITEM,
    LVM_INSERTCOLUMNW, LVM_REDRAWITEMS, LVM_SETEXTENDEDLISTVIEWSTYLE, LVM_SETITEMCOUNT,
    LVM_SETITEMSTATE, LVNI_SELECTED, LVN_GETDISPINFOW, LVN_ITEMCHANGED, LVSICF_NOSCROLL,
    LVS_EX_DOUBLEBUFFER, LVS_EX_FULLROWSELECT, LVS_OWNERDATA, LVS_REPORT, LVS_SHOWSELALWAYS,
    LVS_SINGLESEL, NMHDR, NMLISTVIEW, NMLVDISPINFOW,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    SendMessageW, BN_CLICKED, WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::expand_hfs::ExpandHfsWindow;
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
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
    status: HWND,
    error: HWND,
    details: HWND,
    table: HWND,
    rows: RefCell<Vec<PartitionRow>>,
    /// The row the tools act on: the list's selection, else the first row (as the TUI's cursor).
    cursor: Cell<usize>,
    no_partitions: HWND,
    expand: HWND,
    hfs: RefCell<Option<Rc<ExpandHfsWindow>>>,
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
        let source = ui::label(page, "");
        ui::set_automation_id(source, ids::SOURCE);
        let open_file = ui::button(page, "Open File...", ids::OPEN_FILE);
        let open_backup = ui::button(page, "Open Backup Folder...", ids::OPEN_BACKUP);
        let note = ui::label(page, "");
        let status = ui::label(page, "");
        let error = ui::label(page, "");
        let details = ui::label(page, "");
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
        let expand = ui::button(page, "Expand/Export...", ids::ROW_EXPAND_HFS);
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
            status,
            error,
            details,
            table,
            rows: RefCell::default(),
            cursor: Cell::new(0),
            no_partitions,
            expand,
            hfs: RefCell::default(),
        });
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
        let effects = self.ctrl.borrow_mut().tick();
        self.after(effects);
    }

    /// The app's device list; a change under the open device closes or re-inspects it.
    pub fn set_devices(&self, devices: &[DiskDevice]) {
        self.ctrl.borrow_mut().reconcile_devices(devices);
        *self.devices.borrow_mut() = devices.to_vec();
    }

    /// The disk job running here, by name, for the other tabs' locks.
    #[allow(dead_code)] // read by the app once a second tab runs jobs
    pub fn running_job(&self) -> Option<&'static str> {
        if let Some(job) = self.ctrl.borrow().running_job() {
            Some(job)
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

    fn answered(&self, reply: Reply) {
        let action = match reply {
            Reply::Picked { purpose, path } => {
                if let Some(image) = path.as_ref().filter(|_| purpose == purpose::OPEN_IMAGE) {
                    self.recent
                        .remember(RecentMode::Inspect, &image.display().to_string());
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
        self.sync.run(|| self.refresh());
    }

    /// Carry out `request`; what this shell cannot do yet says so. No `_` arm.
    fn carry_out(&self, request: HostRequest) -> Option<String> {
        match request {
            HostRequest::CloseBrowse | HostRequest::UpgradeSeekable(_) => None,
            HostRequest::OpenBrowse(_) => {
                Some("Browsing arrives in slice 2 of the Win32 shell.".into())
            }
            HostRequest::OpenOptical(_) => {
                Some("The Optical tab arrives in slice 6 of the Win32 shell.".into())
            }
            HostRequest::OpenArchive(_) => {
                Some("The Archives tab arrives in slice 6 of the Win32 shell.".into())
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

    /// Read the controller first and release it, then set the controls.
    fn refresh(&self) {
        let (source, error, loading, table, alignment, rows) = {
            let devices = self.devices.borrow();
            let c = self.ctrl.borrow();
            (
                c.source_label(devices.as_slice()),
                c.last_error().map(str::to_string),
                c.loading().map(|s| s.phase),
                c.table_summary(),
                c.alignment(),
                c.rows(),
            )
        };
        ui::set_text(self.source, &source);
        let note = InspectController::needs_elevation_note().then(unelevated_device_note);
        ui::set_text(self.note, note.unwrap_or(""));
        ui::set_text(self.status, loading.as_deref().unwrap_or(""));
        ui::set_text(
            self.error,
            &error.map(|e| format!("Error: {e}")).unwrap_or_default(),
        );
        let mut details = String::new();
        if let Some(t) = &table {
            details.push_str(&format!("Partition table: {}", t.type_name));
            if t.superfloppy {
                details.push_str(" (superfloppy)");
            }
        }
        if let Some(a) = &alignment {
            details.push_str(&format!(
                "\r\nAlignment: {}, first LBA {}",
                a.kind, a.first_lba
            ));
        }
        ui::set_text(self.details, &details);
        ui::set_visible(self.no_partitions, table.is_some() && rows.is_empty());
        let changed = *self.rows.borrow() != rows;
        if changed {
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
        let acts = row.map(|r| (r.actions, r.locked));
        ui::set_enabled(
            self.expand,
            acts.is_some_and(|(a, locked)| a.browse && a.expand_hfs && !locked),
        );
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
        let item = LVITEMW {
            stateMask: LIST_VIEW_ITEM_STATE_FLAGS(LVIS_SELECTED.0 | LVIS_FOCUSED.0),
            state: LIST_VIEW_ITEM_STATE_FLAGS(LVIS_SELECTED.0 | LVIS_FOCUSED.0),
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

    fn layout(&self) {
        let (area, dpi) = panel::client(self.page);
        let line =
            |h: HWND, n: i32| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n));
        let tree = Node::inset(
            Node::column(vec![
                Node::row(vec![
                    Node::leaf(
                        self.source_caption,
                        Size::Fixed(60),
                        Size::Fixed(layout::ROW - 8),
                    ),
                    Node::leaf(self.source, Size::Stretch(1), Size::Fixed(layout::ROW - 8)),
                    Node::button(self.open_file, 110),
                    Node::button(self.open_backup, 160),
                ]),
                line(self.note, 1),
                line(self.status, 1),
                line(self.error, 1),
                line(self.details, 2),
                Node::leaf(self.table, Size::Stretch(1), Size::Stretch(1)),
                line(self.no_partitions, 1),
                Node::row(vec![
                    Node::button(self.expand, 130),
                    Node::Space(Size::Stretch(1)),
                ]),
            ]),
            layout::MARGIN,
        );
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
}

impl Handler for InspectScreen {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() || code != BN_CLICKED {
            return;
        }
        let action = if from == self.open_file {
            InspectAction::ChooseImage
        } else if from == self.open_backup {
            InspectAction::ChooseBackupFolder
        } else if from == self.expand {
            let Some(index) = self.rows.borrow().get(self.cursor.get()).map(|r| r.index) else {
                return;
            };
            InspectAction::ExpandHfs(index)
        } else {
            return;
        };
        self.apply(action);
    }

    fn notify(&self, hdr: &NMHDR, lp: LPARAM) -> Option<LRESULT> {
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
