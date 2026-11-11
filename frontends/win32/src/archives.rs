//! The Archives screen over `controller::archives`; GTK's `archives/` is the reference.
//!
//! The archive (a split button listing recent ones, and Open File...), its entries in a list with
//! a tick box each, the extraction format, Extract All / Extract Selected, and Mount for a payload
//! that is itself a disk image, which goes to Inspect. The entries are copied into the list once
//! per archive; a tick there is the only change that comes back.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{busy_elsewhere_notice, Notify, RecentHandle, RecentMode};
use rusty_backup::controller::archives::{
    hints, human_size, ids, mount_label, ArchiveEntryRow, ArchivesAction, ArchivesController,
    ExtractOutcome, ForkFormat, HostRequest,
};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::{Controller, Effect};
use windows::core::{w, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::UI::Controls::{
    BCN_DROPDOWN, BS_SPLITBUTTON, LIST_VIEW_ITEM_STATE_FLAGS, LVCF_TEXT, LVCF_WIDTH, LVCOLUMNW,
    LVIF_TEXT, LVIS_STATEIMAGEMASK, LVITEMW, LVM_DELETEALLITEMS, LVM_GETITEMSTATE,
    LVM_INSERTCOLUMNW, LVM_INSERTITEMW, LVM_SETEXTENDEDLISTVIEWSTYLE, LVM_SETITEMSTATE,
    LVM_SETITEMTEXTW, LVN_ITEMCHANGED, LVS_EX_CHECKBOXES, LVS_EX_DOUBLEBUFFER,
    LVS_EX_FULLROWSELECT, LVS_NOSORTHEADER, LVS_REPORT, LVS_SHOWSELALWAYS, NMHDR, NMLISTVIEW,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    GetAncestor, GetWindowRect, SendMessageW, BN_CLICKED, CBN_SELCHANGE, GA_ROOT, WINDOW_EX_STYLE,
    WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::build_disk::set_line;
use crate::combo::Combo;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

const COLUMNS: [(&str, i32); 4] = [
    ("Name", 360),
    ("Type/Creator", 120),
    ("Size", 90),
    ("Method", 120),
];

fn cells(r: &ArchiveEntryRow) -> [String; 4] {
    match r.is_dir {
        true => [
            format!("[ {} ]", r.path),
            String::new(),
            String::new(),
            String::new(),
        ],
        false => [
            r.path.clone(),
            format!("{} / {}", r.type_code, r.creator),
            human_size(r.size),
            r.method.to_string(),
        ],
    }
}

fn outcome_text(outcome: &ExtractOutcome) -> (String, bool) {
    match outcome {
        Ok((stats, dest)) => {
            let skipped = match stats.skipped {
                0 => String::new(),
                n => format!(", {n} skipped"),
            };
            let line = format!(
                "Extracted {} files to {}{skipped}",
                stats.files,
                dest.display()
            );
            (line, false)
        }
        Err(e) => (format!("Extraction failed: {}", e.message), true),
    }
}

pub struct ArchivesScreen {
    ctrl: RefCell<ArchivesController>,
    host: EffectHost,
    recent: RecentHandle,
    recent_files: RefCell<Vec<String>>,
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<ArchivesScreen>,
    page: HWND,
    /// A mounted payload's temporary folder lives as long as the screen.
    guards: RefCell<Vec<Box<dyn std::any::Any>>>,
    mount_asked: RefCell<Option<PathBuf>>,
    /// The archive the list was filled for, so its rows are copied once.
    listed: RefCell<Option<(PathBuf, usize)>>,
    busy_note: HWND,
    intro: HWND,
    source_caption: HWND,
    source: HWND,
    browse: HWND,
    close: HWND,
    job: HWND,
    job_progress: HWND,
    notes: HWND,
    count: HWND,
    list: HWND,
    format_caption: HWND,
    format: Combo,
    extract_all: HWND,
    extract_selected: HWND,
    mount: HWND,
    run: HWND,
    run_progress: HWND,
    shape: RefCell<Vec<usize>>,
}

impl ArchivesScreen {
    pub fn build(
        page: HWND,
        notify: Arc<dyn Notify>,
        recent: RecentHandle,
        host: EffectHost,
    ) -> Rc<ArchivesScreen> {
        let split = WINDOW_STYLE(BS_SPLITBUTTON as u32 | WS_TABSTOP.0);
        let source = ui::child(
            page,
            w!("BUTTON"),
            "",
            split,
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
        let style = WINDOW_STYLE(LVS_REPORT | LVS_NOSORTHEADER | LVS_SHOWSELALWAYS | WS_TABSTOP.0);
        let list = ui::child(
            page,
            w!("SysListView32"),
            "",
            style,
            WS_EX_CLIENTEDGE,
            ids::ENTRIES,
        );
        // SAFETY: plain list-view setup; each title outlives its insert.
        unsafe {
            let ex = LVS_EX_CHECKBOXES | LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER;
            SendMessageW(
                list,
                LVM_SETEXTENDEDLISTVIEWSTYLE,
                Some(WPARAM(ex as usize)),
                Some(LPARAM(ex as isize)),
            );
            let dpi = GetDpiForWindow(page).max(96);
            for (i, (title, width)) in COLUMNS.iter().enumerate() {
                let mut t = ui::wide(title);
                let col = LVCOLUMNW {
                    mask: LVCF_TEXT | LVCF_WIDTH,
                    cx: layout::scale(*width, dpi),
                    pszText: PWSTR(t.as_mut_ptr()),
                    ..Default::default()
                };
                SendMessageW(
                    list,
                    LVM_INSERTCOLUMNW,
                    Some(WPARAM(i)),
                    Some(LPARAM(&col as *const _ as isize)),
                );
            }
        }
        crate::theme::control(list);
        let formats: Vec<&str> = ForkFormat::ALL.iter().map(|f| f.label()).collect();
        let screen = Rc::new_cyclic(|me: &Weak<ArchivesScreen>| ArchivesScreen {
            ctrl: RefCell::new(ArchivesController::new(notify)),
            host,
            recent_files: RefCell::new(recent.list(RecentMode::Archives)),
            recent,
            busy: Cell::new(None),
            sync: SyncGuard::default(),
            me: me.clone(),
            page,
            guards: RefCell::default(),
            mount_asked: RefCell::default(),
            listed: RefCell::default(),
            busy_note: ui::label(page, ""),
            intro: ui::label(page, hints::INTRO),
            source_caption: ui::cell_label(page, "Archive:"),
            source,
            browse: ui::button(page, "Open File...", ids::BROWSE),
            close: ui::button(page, "Close", ids::CLOSE),
            job: ui::cell_label(page, ""),
            job_progress: progress(),
            notes: ui::label(page, ""),
            count: ui::cell_label(page, ""),
            list,
            format_caption: ui::cell_label(page, "Extract as:"),
            format: Combo::build(page, ids::FORMAT, &formats),
            extract_all: ui::button(page, "Extract All...", ids::EXTRACT_ALL),
            extract_selected: ui::button(page, "Extract Selected... (0)", ids::EXTRACT_SELECTED),
            mount: ui::button(page, "Mount in new Inspect tab", ids::MOUNT),
            run: ui::cell_label(page, ""),
            run_progress: progress(),
            shape: RefCell::default(),
        });
        let weak: Weak<dyn Handler> = screen.me.clone();
        panel::register(page, weak);
        screen.refresh();
        screen
    }

    pub fn set_busy(&self, busy: Option<&'static str>) {
        self.busy.set(busy);
    }

    /// Settings cleared the recent files: the picker forgets its copy too.
    pub fn forget_recents(&self) {
        self.recent_files.borrow_mut().clear();
    }

    /// Open `path` as the archive (Inspect's hand-off, or a drop).
    pub fn open_path(&self, path: PathBuf) {
        self.apply(ArchivesAction::Open(path));
    }

    /// The payload Mount decoded, for Inspect, once.
    pub fn take_mount(&self) -> Option<PathBuf> {
        self.mount_asked.borrow_mut().take()
    }

    pub fn apply(&self, action: ArchivesAction) {
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
            let Some(s) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => ArchivesAction::Picked { purpose, path },
                // Archives asks only for files and folders.
                Reply::Confirmed { .. }
                | Reply::Answered { .. }
                | Reply::Chose { .. }
                | Reply::CloseScreen => return,
            };
            s.apply(action);
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
                HostRequest::RememberRecent(path) => {
                    let list = self
                        .recent
                        .remember(RecentMode::Archives, &path.display().to_string());
                    *self.recent_files.borrow_mut() = list;
                }
                HostRequest::OpenInInspect { path, guard } => {
                    self.guards.borrow_mut().push(Box::new(guard));
                    *self.mount_asked.borrow_mut() = Some(path);
                }
            }
        }
        crate::app::append_log(log);
        // Off screen nothing needs it, and a job wakes this many times a second.
        if ui::shown(self.page) {
            self.refresh();
        }
    }

    /// The source button's menu: recent archives, then Open File.
    fn source_menu(&self) {
        let recent = self.recent_files.borrow().clone();
        let mut entries: Vec<(String, bool)> = recent.iter().map(|p| (p.clone(), true)).collect();
        if !recent.is_empty() {
            entries.push((String::new(), false));
        }
        entries.push(("Open File...".into(), true));
        let mut r = RECT::default();
        // SAFETY: plain queries of our own windows.
        let picked = unsafe {
            let _ = GetWindowRect(self.source, &mut r);
            crate::menu::choose(GetAncestor(self.page, GA_ROOT), &entries, r.left, r.bottom)
        };
        match picked {
            Some(i) if i < recent.len() => {
                self.apply(ArchivesAction::Open(PathBuf::from(&recent[i])))
            }
            Some(i) if i == entries.len() - 1 => self.apply(ArchivesAction::Pick),
            _ => {}
        }
    }

    /// Copy the archive's rows into the list, once per archive.
    fn fill_list(&self) {
        let ctrl = self.ctrl.borrow();
        let key = ctrl.path().map(|p| (p.to_path_buf(), ctrl.rows().len()));
        if *self.listed.borrow() == key {
            return;
        }
        // SAFETY: plain list-view messages; each text outlives its message.
        unsafe {
            SendMessageW(self.list, LVM_DELETEALLITEMS, None, None);
            for (n, row) in ctrl.rows().iter().enumerate() {
                for (col, text) in cells(row).iter().enumerate() {
                    let mut t = ui::wide(&ui::plain(text));
                    let item = LVITEMW {
                        mask: LVIF_TEXT,
                        iItem: n as i32,
                        iSubItem: col as i32,
                        pszText: PWSTR(t.as_mut_ptr()),
                        ..Default::default()
                    };
                    let (msg, wp) = match col {
                        0 => (LVM_INSERTITEMW, 0),
                        _ => (LVM_SETITEMTEXTW, n),
                    };
                    SendMessageW(
                        self.list,
                        msg,
                        Some(WPARAM(wp)),
                        Some(LPARAM(&item as *const _ as isize)),
                    );
                }
            }
        }
        *self.listed.borrow_mut() = key;
    }

    /// A row's tick box, as the list holds it.
    fn ticked(&self, i: usize) -> bool {
        // SAFETY: a plain list-view query.
        let state = unsafe {
            SendMessageW(
                self.list,
                LVM_GETITEMSTATE,
                Some(WPARAM(i)),
                Some(LPARAM(LVIS_STATEIMAGEMASK.0 as isize)),
            )
            .0 as u32
        };
        (state >> 12) == 2
    }

    fn set_ticked(&self, i: usize, on: bool) {
        if self.ticked(i) == on {
            return;
        }
        let item = LVITEMW {
            stateMask: LVIS_STATEIMAGEMASK,
            state: LIST_VIEW_ITEM_STATE_FLAGS(if on { 2 << 12 } else { 1 << 12 }),
            ..Default::default()
        };
        // SAFETY: a plain list-view message with a local item.
        unsafe {
            SendMessageW(
                self.list,
                LVM_SETITEMSTATE,
                Some(WPARAM(i)),
                Some(LPARAM(&item as *const _ as isize)),
            );
        }
    }

    pub fn refresh(&self) {
        self.sync.run(|| {
            self.fill_list();
            self.show();
        });
        if *self.shape.borrow() != self.shape_now() {
            self.layout();
        }
    }

    fn show(&self) {
        let ctrl = self.ctrl.borrow();
        let notice = self.busy.get().map(busy_elsewhere_notice);
        set_line(self.busy_note, notice.as_deref(), Some(Tone::Warning));
        let extracting = ctrl.is_extracting();
        let path = ctrl.path().map(|p| p.display().to_string());
        ui::set_text(
            self.source,
            path.as_deref().unwrap_or("No archive selected"),
        );
        ui::set_enabled(self.source, !extracting);
        ui::set_enabled(self.browse, !extracting);
        ui::set_visible(self.close, path.is_some());
        ui::set_enabled(self.close, !extracting);
        let job = ctrl.load_snapshot();
        let (line, pct) = job
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        ui::set_text(self.job, &line);
        ui::set_visible(self.job, job.is_some());
        crate::inspect::set_progress(self.job_progress, job.is_some(), pct);

        let mut notes = Vec::new();
        let mut tone = None;
        if let Some(e) = ctrl.load_error() {
            notes.push(format!("Error: {}", e.message));
            tone = Some(Tone::Error);
        }
        if let Some(outcome) = ctrl.last_extract() {
            let (line, failed) = outcome_text(outcome);
            notes.push(line);
            if failed {
                tone = Some(Tone::Error);
            }
        }
        let notes = notes.join("\r\n");
        set_line(
            self.notes,
            Some(notes.as_str()).filter(|n| !n.is_empty()),
            tone,
        );

        let loaded = ctrl.is_loaded();
        ui::set_text(
            self.count,
            &format!("{} files in archive", ctrl.file_count()),
        );
        for h in [
            self.count,
            self.list,
            self.format_caption,
            self.format.h,
            self.extract_all,
            self.extract_selected,
        ] {
            ui::set_visible(h, loaded);
        }
        for i in 0..ctrl.rows().len() {
            self.set_ticked(i, ctrl.is_selected(i));
        }
        let at = ForkFormat::ALL.iter().position(|f| *f == ctrl.format());
        self.format.select(at.unwrap_or(0));
        ui::set_enabled(self.extract_all, !extracting);
        let selected = ctrl.selected_file_count();
        ui::set_enabled(self.extract_selected, !extracting && selected > 0);
        ui::set_text(
            self.extract_selected,
            &format!("Extract Selected... ({selected})"),
        );
        let mountable = ctrl.mountable();
        ui::set_visible(self.mount, loaded && mountable.is_some());
        ui::set_enabled(self.mount, !extracting);
        if let Some(kind) = mountable {
            ui::set_text(self.mount, &mount_label(kind));
        }
        let run = ctrl.extract_progress();
        let text = run
            .as_ref()
            .map(|(done, total, current)| format!("{done} / {total}  {current}"));
        ui::set_text(self.run, &ui::plain(text.as_deref().unwrap_or("")));
        ui::set_visible(self.run, run.is_some());
        let pct = run
            .as_ref()
            .and_then(|(d, t, _)| (*t > 0).then(|| (d * 100 / t).min(100) as u32));
        crate::inspect::set_progress(self.run_progress, run.is_some(), pct);
    }

    fn shape_now(&self) -> Vec<usize> {
        let mut v: Vec<usize> = [
            self.busy_note,
            self.close,
            self.job,
            self.notes,
            self.list,
            self.mount,
            self.run,
        ]
        .iter()
        .map(|h| usize::from(ui::shown(*h)))
        .collect();
        v.push(ui::text(self.notes).lines().count());
        v
    }

    fn layout(&self) {
        *self.shape.borrow_mut() = self.shape_now();
        let (area, dpi) = panel::client(self.page);
        let row_h = Size::Fixed(layout::ROW);
        let fixed = |h: HWND, w: i32| Node::leaf(h, Size::Fixed(w), row_h);
        let stretch = |h: HWND| Node::leaf(h, Size::Stretch(1), row_h);
        let lines = |h: HWND, n: i32| {
            Node::leaf(
                h,
                Size::Stretch(1),
                Size::Fixed((layout::ROW - 8) * n.max(1)),
            )
        };
        let mut col = Vec::new();
        if ui::shown(self.busy_note) {
            col.push(lines(self.busy_note, 1));
        }
        col.push(lines(self.intro, 2));
        let mut source = vec![
            fixed(self.source_caption, 70),
            stretch(self.source),
            Node::button(self.browse, 110),
        ];
        if ui::shown(self.close) {
            source.push(Node::button(self.close, 70));
        }
        col.push(Node::row(source));
        if ui::shown(self.job) {
            col.push(Node::row(vec![
                stretch(self.job),
                Node::leaf(
                    self.job_progress,
                    Size::Fixed(180),
                    Size::Fixed(layout::ROW - 6),
                ),
            ]));
        }
        if ui::shown(self.notes) {
            let n = ui::text(self.notes).lines().count() as i32;
            col.push(lines(self.notes, n));
        }
        if ui::shown(self.list) {
            col.push(stretch(self.count));
            col.push(Node::leaf(self.list, Size::Stretch(1), Size::Stretch(1)));
            let mut row = vec![
                fixed(self.format_caption, 80),
                fixed(self.format.h, 200),
                Node::button(self.extract_all, 110),
                Node::button(self.extract_selected, 170),
            ];
            if ui::shown(self.mount) {
                row.push(Node::button(self.mount, 260));
            }
            row.push(Node::Space(Size::Stretch(1)));
            col.push(Node::row(row));
        } else {
            col.push(Node::Space(Size::Stretch(1)));
        }
        if ui::shown(self.run) {
            col.push(Node::row(vec![
                Node::leaf(
                    self.run_progress,
                    Size::Fixed(200),
                    Size::Fixed(layout::ROW - 6),
                ),
                stretch(self.run),
            ]));
        }
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, panel::scroll_area(self.page, &tree, area, dpi), dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }
}

impl Handler for ArchivesScreen {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        if code == CBN_SELCHANGE && from == self.format.h {
            if let Some(f) = self.format.picked().and_then(|i| ForkFormat::ALL.get(i)) {
                self.apply(ArchivesAction::SetFormat(*f));
            }
            return;
        }
        if code != BN_CLICKED {
            return;
        }
        let action = if from == self.source {
            self.source_menu();
            return;
        } else if from == self.browse {
            ArchivesAction::Pick
        } else if from == self.close {
            ArchivesAction::Close
        } else if from == self.extract_all {
            ArchivesAction::ExtractAll
        } else if from == self.extract_selected {
            ArchivesAction::ExtractSelected
        } else if from == self.mount {
            ArchivesAction::Mount
        } else {
            return;
        };
        self.apply(action);
    }

    fn notify(&self, hdr: &NMHDR, lp: LPARAM) -> Option<LRESULT> {
        if hdr.hwndFrom == self.source && hdr.code == BCN_DROPDOWN {
            self.source_menu();
            return Some(LRESULT(0));
        }
        if hdr.hwndFrom == self.list && hdr.code == LVN_ITEMCHANGED && !self.sync.active() {
            // SAFETY: LVN_ITEMCHANGED comes with an NMLISTVIEW.
            let nm = unsafe { &*(lp.0 as *const NMLISTVIEW) };
            let mask = LVIS_STATEIMAGEMASK.0;
            if (nm.uNewState ^ nm.uOldState) & mask != 0 && nm.uOldState & mask != 0 {
                if let Ok(i) = usize::try_from(nm.iItem) {
                    let on = (nm.uNewState & mask) >> 12 == 2;
                    if self.ctrl.borrow().is_selected(i) != on {
                        self.apply(ArchivesAction::SetSelected(i, on));
                    }
                }
            }
            return Some(LRESULT(0));
        }
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
