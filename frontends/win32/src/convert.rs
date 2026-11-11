//! The Convert screen over `controller::convert`; GTK's `convert.rs` is the reference.
//!
//! A source (one image or a folder of them), the output, a format and extension for each kind
//! the scan found, the CHD options of each CHD profile in use, the hard-disk size and floppy refit
//! choices, the file list with a tick box per file, and Convert / Cancel. Start asks Skip /
//! Overwrite / Back through the controller's own question when outputs exist.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::chd::ChdOptionsControl;
use rusty_backup::controller::app::{busy_elsewhere_notice, Notify};
use rusty_backup::controller::commander::Tone;
use rusty_backup::controller::convert::{
    floppy_size_choices, formats_for, hints, ids, kind_ids, ChdChoices, ChdProfile, ConvertAction,
    ConvertController, ExtraUnit, FloppySize, Format, ImageKind, Row, SizeChoice, ONLY_FILES,
};
use rusty_backup::controller::{Controller, Effect};
use windows::core::{w, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{
    LIST_VIEW_ITEM_STATE_FLAGS, LVCF_TEXT, LVCF_WIDTH, LVCOLUMNW, LVIF_TEXT, LVIS_STATEIMAGEMASK,
    LVITEMW, LVM_DELETEALLITEMS, LVM_GETITEMSTATE, LVM_INSERTCOLUMNW, LVM_INSERTITEMW,
    LVM_SETEXTENDEDLISTVIEWSTYLE, LVM_SETITEMSTATE, LVM_SETITEMTEXTW, LVN_ITEMCHANGED,
    LVS_EX_CHECKBOXES, LVS_EX_DOUBLEBUFFER, LVS_EX_FULLROWSELECT, LVS_NOSORTHEADER, LVS_REPORT,
    LVS_SHOWSELALWAYS, NMHDR, NMLISTVIEW,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    SendMessageW, BN_CLICKED, CBN_SELCHANGE, EN_CHANGE, WINDOW_EX_STYLE, WINDOW_STYLE,
    WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::build_disk::{checked, number, set_check, set_field, set_line};
use crate::chd::{ChdControl, ChdIds};
use crate::combo::Combo;
use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

const PROFILES: [ChdProfile; 3] = [ChdProfile::Hd, ChdProfile::Cd, ChdProfile::Dvd];
const COLUMNS: [(&str, i32); 4] = [
    ("File", 260),
    ("Kind", 90),
    ("Read as", 220),
    ("Result", 260),
];

fn profile_name(p: ChdProfile) -> &'static str {
    match p {
        ChdProfile::Hd => "Hard disk",
        ChdProfile::Cd => "CD",
        ChdProfile::Dvd => "DVD",
    }
}

/// A format's list label; one this build cannot write says so.
fn format_label(f: Format) -> String {
    match f.unavailable() {
        Some(_) => format!("{} (not in this version)", f.label()),
        None => f.label().to_string(),
    }
}

fn cells(r: &Row) -> [String; 4] {
    let read_as = match &r.note {
        Some(n) => format!("{} - {n}", r.detail),
        None => r.detail.clone(),
    };
    [r.name.clone(), r.kind.clone(), read_as, r.outcome.clone()]
}

/// One kind's format and extension controls.
struct KindRows {
    kind: ImageKind,
    title: HWND,
    format_caption: HWND,
    format: Combo,
    ext_caption: HWND,
    extension: HWND,
    caption: HWND,
}

pub struct ConvertScreen {
    ctrl: RefCell<ConvertController>,
    host: EffectHost,
    busy: Cell<Option<&'static str>>,
    sync: SyncGuard,
    me: Weak<ConvertScreen>,
    page: HWND,
    listed: RefCell<Vec<Row>>,
    busy_note: HWND,
    intro: HWND,
    source_caption: HWND,
    source: HWND,
    pick_file: HWND,
    pick_folder: HWND,
    clear: HWND,
    recursive: HWND,
    output_caption: HWND,
    output: HWND,
    output_hint: HWND,
    pick_output: HWND,
    scan: HWND,
    scan_progress: HWND,
    scan_error: HWND,
    kinds: Vec<KindRows>,
    chd: Vec<(ChdProfile, HWND, ChdControl)>,
    multi_bin: HWND,
    size_caption: HWND,
    sizing: Combo,
    extra: HWND,
    unit: Combo,
    measure_slow: HWND,
    floppy_caption: HWND,
    floppy_size: Combo,
    floppy_help: HWND,
    verify: HWND,
    summary: HWND,
    all: HWND,
    none: HWND,
    list: HWND,
    problem: HWND,
    run: HWND,
    run_progress: HWND,
    start: HWND,
    cancel: HWND,
    outcome: HWND,
    shape: RefCell<Vec<usize>>,
}

impl ConvertScreen {
    pub fn build(page: HWND, notify: Arc<dyn Notify>, host: EffectHost) -> Rc<ConvertScreen> {
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
        let kinds = ImageKind::ALL
            .iter()
            .map(|&kind| {
                let (format_id, ext_id) = kind_ids(kind);
                let labels: Vec<String> =
                    formats_for(kind).iter().map(|f| format_label(*f)).collect();
                let refs: Vec<&str> = labels.iter().map(String::as_str).collect();
                KindRows {
                    kind,
                    title: ui::cell_label(page, kind.plural()),
                    format_caption: ui::cell_label(page, "Format:"),
                    format: Combo::build(page, format_id, &refs),
                    ext_caption: ui::cell_label(page, "Extension:"),
                    extension: ui::edit(page, ext_id),
                    caption: ui::label(page, ""),
                }
            })
            .collect();
        let chd = PROFILES
            .iter()
            .map(|p| {
                let title = ui::cell_label(page, &format!("{} CHD options", profile_name(*p)));
                let control = ChdControl::build(
                    page,
                    ChdIds {
                        mode: ids::CHD_MODE,
                        hunk: ids::CHD_HUNK,
                        codec: ids::CHD_CODEC,
                    },
                    ChdOptionsControl::new(*p),
                    *p,
                );
                (*p, title, control)
            })
            .collect();
        let sizes: Vec<&str> = SizeChoice::ALL.iter().map(|c| c.label()).collect();
        let units: Vec<&str> = ExtraUnit::ALL.iter().map(|u| u.label()).collect();
        let floppy: Vec<String> = floppy_size_choices().iter().map(|f| f.label()).collect();
        let floppy: Vec<&str> = floppy.iter().map(String::as_str).collect();
        let style = WINDOW_STYLE(LVS_REPORT | LVS_NOSORTHEADER | LVS_SHOWSELALWAYS | WS_TABSTOP.0);
        let list = ui::child(
            page,
            w!("SysListView32"),
            "",
            style,
            WS_EX_CLIENTEDGE,
            ids::FILES,
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
        let screen = Rc::new_cyclic(|me: &Weak<ConvertScreen>| ConvertScreen {
            ctrl: RefCell::new(ConvertController::new(notify)),
            host,
            busy: Cell::new(None),
            sync: SyncGuard::default(),
            me: me.clone(),
            page,
            listed: RefCell::default(),
            busy_note: ui::label(page, ""),
            intro: ui::label(page, hints::INTRO),
            source_caption: ui::cell_label(page, "Source:"),
            source: ui::path_label(page, ""),
            pick_file: ui::button(page, "Choose Image...", ids::SOURCE_FILE),
            pick_folder: ui::button(page, "Choose Folder...", ids::SOURCE_FOLDER),
            clear: ui::button(page, "Clear", ids::CLEAR),
            recursive: ui::checkbox(page, "Include subfolders", ids::RECURSIVE),
            output_caption: ui::cell_label(page, ""),
            output: ui::path_label(page, ""),
            output_hint: ui::label(page, ""),
            pick_output: ui::button(page, "Choose...", ids::OUTPUT),
            scan: ui::cell_label(page, ""),
            scan_progress: progress(),
            scan_error: ui::label(page, ""),
            kinds,
            chd,
            multi_bin: ui::checkbox(page, "One .bin per track", ids::MULTI_BIN),
            size_caption: ui::cell_label(page, "Hard disk size:"),
            sizing: Combo::build(page, ids::SIZING, &sizes),
            extra: number(page, ids::EXTRA_AMOUNT),
            unit: Combo::build(page, ids::EXTRA_UNIT, &units),
            measure_slow: ui::checkbox(page, "Measure slow filesystems too", ids::MEASURE_SLOW),
            floppy_caption: ui::cell_label(page, "Floppy size:"),
            floppy_size: Combo::build(page, ids::FLOPPY_SIZE, &floppy),
            floppy_help: ui::label(page, ""),
            verify: ui::checkbox(page, "Verify after converting", ids::VERIFY),
            summary: ui::cell_label(page, ""),
            all: ui::button(page, "Select All", ids::SELECT_ALL),
            none: ui::button(page, "Select None", ids::SELECT_NONE),
            list,
            problem: ui::cell_label(page, ""),
            run: ui::cell_label(page, ""),
            run_progress: progress(),
            start: ui::button(page, "Convert", ids::START),
            cancel: ui::button(page, "Cancel", ids::CANCEL),
            outcome: ui::label(page, ""),
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

    pub fn is_running(&self) -> bool {
        self.ctrl.borrow().is_running()
    }

    /// A file or folder another tab hands over.
    pub fn open(&self, path: PathBuf) {
        self.apply(ConvertAction::SetSource(path));
    }

    pub fn apply(&self, action: ConvertAction) {
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
                Reply::Picked { purpose, path } => ConvertAction::Picked { purpose, path },
                Reply::Chose { purpose, choice } => ConvertAction::Chose {
                    purpose,
                    index: choice,
                },
                // Convert asks for files, folders and one choice; no text, no yes/no.
                Reply::Confirmed { .. } | Reply::Answered { .. } | Reply::CloseScreen => return,
            };
            s.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        let log = self.ctrl.borrow_mut().take_log();
        crate::app::append_log(log);
        // Off screen nothing needs it, and a job wakes this many times a second.
        if ui::shown(self.page) {
            self.refresh();
        }
    }

    fn chd_choices(&self) -> ChdChoices {
        let mut out = ChdChoices::default();
        let profiles: Vec<ChdProfile> = self
            .ctrl
            .borrow()
            .chd_profiles()
            .into_iter()
            .map(|(_, p)| p)
            .collect();
        for (p, _, control) in &self.chd {
            if !profiles.contains(p) {
                continue;
            }
            let opts = Some(control.choice().0);
            match p {
                ChdProfile::Hd => out.hd = opts,
                ChdProfile::Cd => out.cd = opts,
                ChdProfile::Dvd => out.dvd = opts,
            }
        }
        out
    }

    fn chd_ok(&self) -> bool {
        let used: Vec<ChdProfile> = self
            .ctrl
            .borrow()
            .chd_profiles()
            .into_iter()
            .map(|(_, p)| p)
            .collect();
        self.chd
            .iter()
            .all(|(p, _, c)| !used.contains(p) || c.is_valid())
    }

    /// Start's whole rule: asked again at the click, not only when the button was last drawn.
    fn may_start(&self) -> bool {
        self.ctrl.borrow().can_start() && self.chd_ok() && self.busy.get().is_none()
    }

    /// A new list replaces the rows; a changed row replaces only its text and tick.
    fn show_rows(&self, rows: &[Row]) {
        let mut listed = self.listed.borrow_mut();
        let same =
            listed.len() == rows.len() && listed.iter().zip(rows).all(|(a, b)| a.name == b.name);
        // SAFETY: plain list-view messages; each text outlives its message.
        unsafe {
            if !same {
                SendMessageW(self.list, LVM_DELETEALLITEMS, None, None);
            }
            for (n, row) in rows.iter().enumerate() {
                if same && listed[n] == *row {
                    continue;
                }
                for (col, text) in cells(row).iter().enumerate() {
                    let mut t = ui::wide(&ui::plain(text));
                    let item = LVITEMW {
                        mask: LVIF_TEXT,
                        iItem: n as i32,
                        iSubItem: col as i32,
                        pszText: PWSTR(t.as_mut_ptr()),
                        ..Default::default()
                    };
                    let (msg, wp) = match (col, same) {
                        (0, false) => (LVM_INSERTITEMW, 0),
                        _ => (LVM_SETITEMTEXTW, n),
                    };
                    SendMessageW(
                        self.list,
                        msg,
                        Some(WPARAM(wp)),
                        Some(LPARAM(&item as *const _ as isize)),
                    );
                }
                let on = row.selected && !row.skipped;
                let item = LVITEMW {
                    stateMask: LVIS_STATEIMAGEMASK,
                    state: LIST_VIEW_ITEM_STATE_FLAGS(if on { 2 << 12 } else { 1 << 12 }),
                    ..Default::default()
                };
                SendMessageW(
                    self.list,
                    LVM_SETITEMSTATE,
                    Some(WPARAM(n)),
                    Some(LPARAM(&item as *const _ as isize)),
                );
            }
        }
        *listed = rows.to_vec();
    }

    pub fn refresh(&self) {
        let can_start = self.may_start();
        self.sync.run(|| self.show(can_start));
        if *self.shape.borrow() != self.shape_now() {
            self.layout();
        }
    }

    fn show(&self, can_start: bool) {
        let c = self.ctrl.borrow();
        let notice = self.busy.get().map(busy_elsewhere_notice);
        set_line(self.busy_note, notice.as_deref(), Some(Tone::Warning));
        ui::set_text(self.source, &ConvertController::path_text(c.source()));
        ui::set_visible(self.recursive, c.source_is_folder());
        set_check(self.recursive, c.recursive());
        let has = c.source().is_some();
        ui::set_visible(self.clear, has);
        for h in [
            self.output_caption,
            self.output,
            self.output_hint,
            self.pick_output,
        ] {
            ui::set_visible(h, has);
        }
        ui::set_text(self.output_caption, &format!("{}:", c.output_label()));
        ui::set_text(self.output, &ConvertController::path_text(c.output()));
        ui::set_text(self.output_hint, c.output_hint());
        let scan = c.scan_snapshot();
        let (line, pct) = scan
            .as_ref()
            .map(crate::inspect::job_line)
            .unwrap_or_default();
        ui::set_text(self.scan, &line);
        ui::set_visible(self.scan, scan.is_some());
        crate::inspect::set_progress(self.scan_progress, scan.is_some(), pct);
        set_line(self.scan_error, c.scan_error(), Some(Tone::Error));

        let shown = c.shown_kinds();
        for k in &self.kinds {
            let on = shown.contains(&k.kind);
            for h in [
                k.title,
                k.format_caption,
                k.format.h,
                k.ext_caption,
                k.extension,
                k.caption,
            ] {
                ui::set_visible(h, on);
            }
            let format = c.format(k.kind);
            let at = formats_for(k.kind).iter().position(|f| *f == format);
            k.format.select(at.unwrap_or(0));
            set_field(k.extension, c.extension(k.kind));
            let help = match format.unavailable() {
                Some(why) => format!("Not in this version of Rusty Backup: {why}"),
                None => format.hint().to_string(),
            };
            ui::set_text(k.caption, &help);
        }
        let used: Vec<ChdProfile> = c.chd_profiles().into_iter().map(|(_, p)| p).collect();
        for (p, title, control) in &self.chd {
            let on = used.contains(p);
            ui::set_visible(*title, on);
            control.set_visible(on);
        }
        ui::set_visible(self.multi_bin, c.multi_bin_shown());
        set_check(self.multi_bin, c.multi_bin());

        let hd = shown.contains(&ImageKind::HardDisk);
        for h in [self.size_caption, self.sizing.h, self.measure_slow] {
            ui::set_visible(h, hd);
        }
        let at = SizeChoice::ALL.iter().position(|s| *s == c.size_choice());
        self.sizing.select(at.unwrap_or(0));
        let extra = hd && c.size_choice() == SizeChoice::Extra;
        ui::set_visible(self.extra, extra);
        ui::set_visible(self.unit.h, extra);
        set_field(self.extra, &c.extra_amount().to_string());
        let at = ExtraUnit::ALL.iter().position(|u| *u == c.extra_unit());
        self.unit.select(at.unwrap_or(0));
        ui::set_enabled(self.measure_slow, c.measure_slow_enabled());
        set_check(self.measure_slow, c.measure_slow());

        let floppy = shown.contains(&ImageKind::Floppy);
        for h in [self.floppy_caption, self.floppy_size.h, self.floppy_help] {
            ui::set_visible(h, floppy);
        }
        let sizes = floppy_size_choices();
        let at = sizes.iter().position(|f| *f == c.floppy_size());
        self.floppy_size.select(at.unwrap_or(0));
        let help = match c.floppy_size() {
            FloppySize::Same => hints::FLOPPY_SIZE.to_string(),
            _ => format!("{} {ONLY_FILES}", hints::FLOPPY_SIZE),
        };
        ui::set_text(self.floppy_help, &help);
        ui::set_visible(self.verify, !shown.is_empty());
        set_check(self.verify, c.verify());

        let rows = c.rows();
        for h in [self.summary, self.all, self.none, self.list] {
            ui::set_visible(h, !rows.is_empty());
        }
        ui::set_text(self.summary, &c.summary());
        drop(c);
        self.show_rows(&rows);
        let c = self.ctrl.borrow();

        let running = c.is_running();
        ui::set_visible(self.start, !running);
        ui::set_enabled(self.start, can_start);
        ui::set_text(self.start, &c.start_label());
        ui::set_visible(self.cancel, running);
        let problem = c.start_problem().filter(|_| !running);
        set_line(self.problem, problem.as_deref(), None);
        let run = c.strip();
        ui::set_text(
            self.run,
            &ui::plain(run.as_ref().map_or("", |(_, t)| t.as_str())),
        );
        ui::set_visible(self.run, run.is_some());
        let pct = run
            .as_ref()
            .map(|(f, _)| (f.clamp(0.0, 1.0) * 100.0) as u32);
        crate::inspect::set_progress(self.run_progress, run.is_some(), pct);
        let (text, error) = c.outcome().unzip();
        set_line(
            self.outcome,
            text.as_deref(),
            error.filter(|e| *e).map(|_| Tone::Error),
        );
        let free = !running && self.busy.get().is_none();
        for h in [
            self.pick_file,
            self.pick_folder,
            self.clear,
            self.recursive,
            self.pick_output,
        ] {
            ui::set_enabled(h, free);
        }
    }

    fn shape_now(&self) -> Vec<usize> {
        let mut hs = vec![
            self.busy_note,
            self.recursive,
            self.output,
            self.scan,
            self.scan_error,
            self.multi_bin,
            self.sizing.h,
            self.extra,
            self.floppy_size.h,
            self.verify,
            self.list,
            self.problem,
            self.run,
            self.start,
            self.cancel,
            self.outcome,
        ];
        hs.extend(self.kinds.iter().map(|k| k.format.h));
        for (_, t, c) in &self.chd {
            hs.push(*t);
            hs.extend(c.combos());
        }
        hs.iter().map(|h| usize::from(ui::shown(*h))).collect()
    }

    fn layout(&self) {
        *self.shape.borrow_mut() = self.shape_now();
        let (area, dpi) = panel::client(self.page);
        let row_h = Size::Fixed(layout::ROW);
        let fixed = |h: HWND, w: i32| Node::leaf(h, Size::Fixed(w), row_h);
        let stretch = |h: HWND| Node::leaf(h, Size::Stretch(1), row_h);
        let lines =
            |h: HWND, n: i32| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n));
        let shown = |h: HWND| ui::shown(h);
        let cap = 120;
        let mut col = Vec::new();
        if shown(self.busy_note) {
            col.push(lines(self.busy_note, 1));
        }
        col.push(lines(self.intro, 2));
        let mut source = vec![
            fixed(self.source_caption, cap),
            stretch(self.source),
            Node::button(self.pick_file, 120),
            Node::button(self.pick_folder, 120),
        ];
        if shown(self.clear) {
            source.push(Node::button(self.clear, 60));
        }
        col.push(Node::row(source));
        if shown(self.recursive) {
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                stretch(self.recursive),
            ]));
        }
        if shown(self.output) {
            col.push(Node::row(vec![
                fixed(self.output_caption, cap),
                stretch(self.output),
                Node::button(self.pick_output, 85),
            ]));
            col.push(Node::row(vec![
                Node::Space(Size::Fixed(cap)),
                stretch(self.output_hint),
            ]));
        }
        if shown(self.scan) {
            col.push(Node::row(vec![
                stretch(self.scan),
                Node::leaf(
                    self.scan_progress,
                    Size::Fixed(180),
                    Size::Fixed(layout::ROW - 6),
                ),
            ]));
        }
        if shown(self.scan_error) {
            col.push(lines(self.scan_error, 1));
        }
        for k in &self.kinds {
            if !shown(k.format.h) {
                continue;
            }
            col.push(Node::row(vec![
                fixed(k.title, cap),
                fixed(k.format_caption, 60),
                fixed(k.format.h, 260),
                fixed(k.ext_caption, 75),
                fixed(k.extension, 80),
                stretch(k.caption),
            ]));
        }
        for (_, title, c) in &self.chd {
            if shown(*title) {
                col.push(stretch(*title));
                col.extend(c.rows());
            }
        }
        if shown(self.multi_bin) {
            col.push(stretch(self.multi_bin));
        }
        if shown(self.sizing.h) {
            let mut row = vec![fixed(self.size_caption, cap), fixed(self.sizing.h, 220)];
            if shown(self.extra) {
                row.push(fixed(self.extra, 70));
                row.push(fixed(self.unit.h, 80));
            }
            row.push(stretch(self.measure_slow));
            col.push(Node::row(row));
        }
        if shown(self.floppy_size.h) {
            col.push(Node::row(vec![
                fixed(self.floppy_caption, cap),
                fixed(self.floppy_size.h, 220),
                stretch(self.floppy_help),
            ]));
        }
        if shown(self.verify) {
            col.push(stretch(self.verify));
        }
        if shown(self.list) {
            col.push(Node::row(vec![
                stretch(self.summary),
                Node::button(self.all, 90),
                Node::button(self.none, 95),
            ]));
            col.push(Node::leaf(self.list, Size::Stretch(1), Size::Stretch(1)));
        } else {
            col.push(Node::Space(Size::Stretch(1)));
        }
        if shown(self.outcome) {
            col.push(lines(self.outcome, 1));
        }
        let mut foot = Vec::new();
        for (h, w) in [(self.start, 140), (self.cancel, 90)] {
            if shown(h) {
                foot.push(Node::button(h, w));
            }
        }
        if shown(self.run) {
            foot.push(Node::leaf(
                self.run_progress,
                Size::Fixed(200),
                Size::Fixed(layout::ROW - 6),
            ));
            foot.push(stretch(self.run));
        } else {
            foot.push(stretch(self.problem));
        }
        col.push(Node::row(foot));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in panel::place(self.page, tree, area, dpi) {
            if ui::shown(h) {
                ui::move_to(h, r);
            }
        }
    }

    fn selected(&self, from: HWND) -> Option<ConvertAction> {
        if let Some(k) = self.kinds.iter().find(|k| k.format.h == from) {
            let f = *formats_for(k.kind).get(k.format.picked()?)?;
            return Some(ConvertAction::SetFormat(k.kind, f));
        }
        if from == self.sizing.h {
            return Some(ConvertAction::SetSizing(
                *SizeChoice::ALL.get(self.sizing.picked()?)?,
            ));
        }
        if from == self.unit.h {
            return Some(ConvertAction::SetExtraUnit(
                *ExtraUnit::ALL.get(self.unit.picked()?)?,
            ));
        }
        if from == self.floppy_size.h {
            let sizes = floppy_size_choices();
            return Some(ConvertAction::SetFloppySize(
                *sizes.get(self.floppy_size.picked()?)?,
            ));
        }
        None
    }

    fn clicked(&self, from: HWND) -> Option<ConvertAction> {
        Some(if from == self.pick_file {
            ConvertAction::PickSourceFile
        } else if from == self.pick_folder {
            ConvertAction::PickSourceFolder
        } else if from == self.clear {
            ConvertAction::Clear
        } else if from == self.recursive {
            ConvertAction::SetRecursive(checked(from))
        } else if from == self.pick_output {
            ConvertAction::PickOutput
        } else if from == self.multi_bin {
            ConvertAction::SetMultiBin(checked(from))
        } else if from == self.measure_slow {
            ConvertAction::SetMeasureSlow(checked(from))
        } else if from == self.verify {
            ConvertAction::SetVerify(checked(from))
        } else if from == self.all {
            ConvertAction::SelectAll
        } else if from == self.none {
            ConvertAction::SelectNone
        } else if from == self.start {
            if !self.may_start() {
                return None;
            }
            ConvertAction::Start(self.chd_choices())
        } else if from == self.cancel {
            ConvertAction::Cancel
        } else {
            return None;
        })
    }
}

impl Handler for ConvertScreen {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        if self.chd.iter().any(|(_, _, c)| c.command(from, code)) {
            self.refresh();
            return;
        }
        let action = match code {
            CBN_SELCHANGE => self.selected(from),
            BN_CLICKED => self.clicked(from),
            EN_CHANGE if from == self.extra => ui::text(from)
                .trim()
                .parse()
                .ok()
                .map(ConvertAction::SetExtraAmount),
            EN_CHANGE => self
                .kinds
                .iter()
                .find(|k| k.extension == from)
                .map(|k| ConvertAction::SetExtension(k.kind, ui::text(from))),
            _ => None,
        };
        if let Some(a) = action {
            self.apply(a);
        }
    }

    fn notify(&self, hdr: &NMHDR, lp: LPARAM) -> Option<LRESULT> {
        if hdr.hwndFrom != self.list || hdr.code != LVN_ITEMCHANGED || self.sync.active() {
            return None;
        }
        // SAFETY: LVN_ITEMCHANGED comes with an NMLISTVIEW.
        let nm = unsafe { &*(lp.0 as *const NMLISTVIEW) };
        let mask = LVIS_STATEIMAGEMASK.0;
        if (nm.uNewState ^ nm.uOldState) & mask != 0 && nm.uOldState & mask != 0 {
            if let Ok(i) = usize::try_from(nm.iItem) {
                // SAFETY: a plain list-view query.
                let state = unsafe {
                    SendMessageW(
                        self.list,
                        LVM_GETITEMSTATE,
                        Some(WPARAM(i)),
                        Some(LPARAM(mask as isize)),
                    )
                    .0 as u32
                };
                self.apply(ConvertAction::SetSelected(i, state >> 12 == 2));
            }
        }
        Some(LRESULT(0))
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
