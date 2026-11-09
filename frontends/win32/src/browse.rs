//! The Inspect browse view over a single-pane `CommanderController`; GTK's `browser/mod.rs` is
//! the reference.
//!
//! Slice 2, first pass: the volume line, Extract All, Export to .tar.gz and Close, the running
//! job with Cancel, the controller's notices, the folder tree, the listing (a virtual list view),
//! the side panel with Extract, Export and Open as Disk Image over a text or hex preview, and the
//! status line. Keys go through the controller's key map. Still to come: marks, Edit Mode and its
//! bar, the View menu, the row menu, Check / Journal / Tree / CHD Info, and the dialogs.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use rusty_backup::controller::app::{CommanderRecent, Notify, Snapshot, UserConfig};
use rusty_backup::controller::browser::ids;
use rusty_backup::controller::commander::{
    extract_fork_modes, format_size, BrowserPaneAction as P, CommanderAction as A,
    CommanderController, DisplayRow, ExportFormat, ExtractOptions, JobRegistry, ListingStyle,
    ModeAction, PaneProfile, PaneServices, PanelActions, ResourceForkMode, Side, SidePanel,
    SortColumn, VolumeLine, FOLLOW_DELAY,
};
use rusty_backup::controller::inspect::{BrowseEdit, BrowseOpen, InspectAction, InspectSource};
use rusty_backup::controller::keys::{Command, Key, KeyMap, Mods};
use rusty_backup::controller::log::LogLevel;
use rusty_backup::controller::{Controller, Effect};
use windows::core::{w, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{
    ImageList_Destroy, HIMAGELIST, HTREEITEM, LIST_VIEW_ITEM_STATE_FLAGS, LVCFMT_RIGHT, LVCF_FMT,
    LVCF_TEXT, LVCF_WIDTH, LVCOLUMNW, LVHITTESTINFO, LVHT_ONITEMICON, LVIF_IMAGE, LVIF_INDENT,
    LVIF_STATE, LVIF_TEXT, LVIS_FOCUSED, LVIS_SELECTED, LVITEMW, LVM_ENSUREVISIBLE,
    LVM_INSERTCOLUMNW, LVM_REDRAWITEMS, LVM_SETCOLUMNWIDTH, LVM_SETEXTENDEDLISTVIEWSTYLE,
    LVM_SETIMAGELIST, LVM_SETITEMCOUNT, LVM_SETITEMSTATE, LVM_SUBITEMHITTEST, LVN_COLUMNCLICK,
    LVN_GETDISPINFOW, LVN_ITEMCHANGED, LVN_KEYDOWN, LVSICF_NOSCROLL, LVSIL_SMALL,
    LVS_EX_DOUBLEBUFFER, LVS_EX_FULLROWSELECT, LVS_OWNERDATA, LVS_REPORT, LVS_SHOWSELALWAYS,
    LVS_SINGLESEL, NMHDR, NMITEMACTIVATE, NMLISTVIEW, NMLVDISPINFOW, NMLVKEYDOWN, NMTREEVIEWW,
    NM_CLICK, NM_DBLCLK, TVE_EXPAND, TVGN_CARET, TVIF_CHILDREN, TVIF_IMAGE, TVIF_PARAM,
    TVIF_SELECTEDIMAGE, TVIF_TEXT, TVINSERTSTRUCTW, TVINSERTSTRUCTW_0, TVITEMW, TVI_LAST, TVI_ROOT,
    TVM_DELETEITEM, TVM_ENSUREVISIBLE, TVM_EXPAND, TVM_INSERTITEMW, TVM_SELECTITEM,
    TVM_SETIMAGELIST, TVN_ITEMEXPANDINGW, TVN_SELCHANGEDW, TVSIL_NORMAL, TVS_HASBUTTONS,
    TVS_HASLINES, TVS_LINESATROOT, TVS_SHOWSELALWAYS,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::Input::KeyboardAndMouse::{
    GetKeyState, VIRTUAL_KEY, VK_BACK, VK_CONTROL, VK_DELETE, VK_DOWN, VK_END, VK_ESCAPE, VK_F1,
    VK_F24, VK_HOME, VK_INSERT, VK_LEFT, VK_MENU, VK_NEXT, VK_PRIOR, VK_RETURN, VK_RIGHT, VK_SHIFT,
    VK_SPACE, VK_TAB, VK_UP,
};
use windows::Win32::UI::Shell::{DefSubclassProc, SetWindowSubclass};
use windows::Win32::UI::WindowsAndMessaging::{
    KillTimer, PostMessageW, SendMessageW, SetTimer, CBN_SELCHANGE, CBS_DROPDOWNLIST, CB_ADDSTRING,
    CB_GETCURSEL, CB_SETCURSEL, DLGC_WANTMESSAGE, ES_AUTOVSCROLL, ES_MULTILINE, ES_READONLY, MSG,
    WINDOW_EX_STYLE, WINDOW_STYLE, WM_COMMAND, WM_GETDLGCODE, WM_KEYDOWN, WS_EX_CLIENTEDGE,
    WS_HSCROLL, WS_TABSTOP, WS_VSCROLL,
};

use crate::effects::{EffectHost, Reply, ReplyFn};
use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::row_icons;
use crate::sync::SyncGuard;
use crate::ui;

/// The listing's columns: title, width in DIPs, right-aligned, the sort they ask for.
const COLUMNS: [(&str, i32, bool, Option<SortColumn>); 5] = [
    ("Name", 260, false, Some(SortColumn::Name)),
    ("Data", 90, true, Some(SortColumn::Size)),
    ("Rsrc", 80, true, None),
    ("Modified", 140, false, Some(SortColumn::Modified)),
    ("Type", 90, false, Some(SortColumn::Type)),
];

/// The side panel's width and the folder tree's, in DIPs.
const DETAIL_W: i32 = 330;
const TREE_W: i32 = 200;

/// A `WM_COMMAND` code the view posts to itself: run the folder-tree action a notification held.
const DEFERRED: u32 = 0x7F00;

fn new_controller(notify: &Arc<dyn Notify>) -> CommanderController {
    let services = PaneServices {
        jobs: JobRegistry::new(),
        notify: Arc::clone(notify),
        recent: Box::new(CommanderRecent(UserConfig)),
    };
    let (keymap, problems) = KeyMap::load(KeyMap::user_file().as_deref());
    let mut ctl = CommanderController::single(services, keymap, PaneProfile::inspect());
    ctl.report_keymap_problems(problems);
    ctl
}

/// One listing row as the list view draws it.
#[derive(Clone, PartialEq)]
struct Line {
    cells: [String; 5],
    node_id: String,
    icon: i32,
    depth: i32,
}

/// Everything a refresh shows, read while the controller is borrowed.
struct Shown {
    loaded: bool,
    volume: VolumeLine,
    message: String,
    export_volume: bool,
    job: Option<(Snapshot, bool)>,
    rows: Option<Vec<DisplayRow>>,
    lines: Vec<Line>,
    rsrc: bool,
    cursor: Option<usize>,
    folders: Option<Vec<(usize, String, bool)>>,
    folder_tree: bool,
    folder_cursor: usize,
    panel_key: Option<(u64, bool)>,
    panel: Option<SidePanel>,
    result: Option<String>,
    actions: Option<PanelActions>,
    status: String,
    follow: bool,
}

pub struct BrowseView {
    ctrl: RefCell<CommanderController>,
    notify: Arc<dyn Notify>,
    host: EffectHost,
    sync: SyncGuard,
    me: Weak<BrowseView>,
    active: Cell<bool>,
    page: HWND,
    title: HWND,
    volume: HWND,
    extract_all: HWND,
    export_volume: HWND,
    close: HWND,
    job: HWND,
    progress: HWND,
    cancel: HWND,
    notice: HWND,
    notice_ok: HWND,
    message: HWND,
    tree: HWND,
    list: HWND,
    name: HWND,
    facts: HWND,
    note: HWND,
    budget: HWND,
    result: HWND,
    fork_caption: HWND,
    fork: HWND,
    extract: HWND,
    export: HWND,
    nested: HWND,
    preview: HWND,
    status: HWND,
    rows: RefCell<Vec<DisplayRow>>,
    lines: RefCell<Vec<Line>>,
    /// Listing positions shift by the `..` rows a tick-box listing leaves out.
    skip: Cell<usize>,
    folders: RefCell<Vec<(usize, String, bool)>>,
    folder_items: RefCell<Vec<isize>>,
    panel_key: Cell<Option<(u64, bool)>>,
    label_prefix: RefCell<String>,
    deferred: RefCell<Option<P>>,
    follow_timer: Cell<usize>,
    on_change: RefCell<Option<Rc<dyn Fn()>>>,
    /// Which optional lines the last layout gave room to.
    shape: Cell<u32>,
}

thread_local! {
    /// The view the follow timer wakes; a timer callback has no context of its own.
    static FOLLOW: RefCell<Weak<BrowseView>> = const { RefCell::new(Weak::new()) };
}

unsafe extern "system" fn follow_fired(_: HWND, _: u32, id: usize, _: u32) {
    // SAFETY: the timer is ours and one-shot.
    let _ = unsafe { KillTimer(None, id) };
    if let Some(view) = FOLLOW.with(|f| f.borrow().upgrade()) {
        view.follow_timer.set(0);
        view.tick();
    }
}

fn multiline(parent: HWND, id: &str) -> HWND {
    let style = ES_MULTILINE as u32
        | ES_READONLY as u32
        | ES_AUTOVSCROLL as u32
        | WS_VSCROLL.0
        | WS_HSCROLL.0
        | WS_TABSTOP.0;
    ui::child(
        parent,
        w!("EDIT"),
        "",
        WINDOW_STYLE(style),
        WS_EX_CLIENTEDGE,
        id,
    )
}

/// The controller's name for a key the list hands over; `None` for one it leaves to the list.
fn key_of(vk: u16) -> Option<Key> {
    let k = VIRTUAL_KEY(vk);
    Some(match k {
        VK_RETURN => Key::Enter,
        VK_ESCAPE => Key::Escape,
        VK_BACK => Key::Backspace,
        VK_DELETE => Key::Delete,
        VK_INSERT => Key::Insert,
        VK_TAB => Key::Tab,
        _ if (VK_F1.0..=VK_F24.0).contains(&vk) => Key::F((vk - VK_F1.0 + 1) as u8),
        // Moving the selection is the list's own: its change moves the controller's cursor.
        VK_LEFT => Key::Left,
        VK_RIGHT => Key::Right,
        VK_UP | VK_DOWN | VK_PRIOR | VK_NEXT | VK_HOME | VK_END | VK_SPACE => return None,
        _ => {
            let c = char::from_u32(u32::from(vk)).filter(|c| c.is_ascii_alphanumeric())?;
            Key::Char(c.to_ascii_lowercase())
        }
    })
}

/// The list asks the dialog loop for Enter and Escape, which `IsDialogMessage` would keep.
unsafe extern "system" fn list_proc(
    hwnd: HWND,
    msg: u32,
    wp: WPARAM,
    lp: LPARAM,
    _id: usize,
    _data: usize,
) -> LRESULT {
    if msg == WM_GETDLGCODE && lp.0 != 0 {
        // SAFETY: WM_GETDLGCODE's lParam, when set, is the MSG being dispatched.
        let m = unsafe { &*(lp.0 as *const MSG) };
        let key = VIRTUAL_KEY(m.wParam.0 as u16);
        if m.message == WM_KEYDOWN && (key == VK_RETURN || key == VK_ESCAPE) {
            return LRESULT(DLGC_WANTMESSAGE as isize);
        }
    }
    // SAFETY: everything else goes on to the list.
    unsafe { DefSubclassProc(hwnd, msg, wp, lp) }
}

fn held(vk: VIRTUAL_KEY) -> bool {
    // SAFETY: a plain query of the keyboard state.
    unsafe { GetKeyState(i32::from(vk.0)) < 0 }
}

fn mods() -> Mods {
    Mods {
        ctrl: held(VK_CONTROL),
        alt: held(VK_MENU),
        shift: held(VK_SHIFT),
        cmd: false,
    }
}

fn line_of(row: &DisplayRow, name: String) -> Line {
    Line {
        cells: [
            name,
            match row.is_dir {
                true => String::new(),
                false => format_size(row.size),
            },
            match row.rsrc_size {
                0 => String::new(),
                n => format_size(n),
            },
            row.modified.clone(),
            row.type_tag.clone(),
        ],
        node_id: row.node_id.clone(),
        icon: row_icons::for_row(row),
        depth: row.depth as i32,
    }
}

impl BrowseView {
    pub fn build(page: HWND, notify: Arc<dyn Notify>, host: EffectHost) -> Rc<BrowseView> {
        let title = ui::label(page, "Filesystem Browser");
        let volume = ui::label(page, "");
        let extract_all = ui::button(page, "Extract All...", ids::EXTRACT);
        let export_volume = ui::button(page, "Export to .tar.gz...", ids::EXPORT_VOLUME);
        let close = ui::button(page, "Close", ids::CLOSE);
        let job = ui::label(page, "");
        let progress = ui::child(
            page,
            w!("msctls_progress32"),
            "",
            WINDOW_STYLE(0),
            WINDOW_EX_STYLE(0),
            "",
        );
        let cancel = ui::button(page, "Cancel", ids::CANCEL_JOB);
        let notice = ui::label(page, "");
        let notice_ok = ui::button(page, "OK", "");
        let message = ui::label(page, "");
        let tree_style =
            TVS_HASBUTTONS | TVS_HASLINES | TVS_LINESATROOT | TVS_SHOWSELALWAYS | WS_TABSTOP.0;
        let tree = ui::child(
            page,
            w!("SysTreeView32"),
            "",
            WINDOW_STYLE(tree_style),
            WS_EX_CLIENTEDGE,
            ids::FOLDER_TREE,
        );
        let style = LVS_REPORT | LVS_OWNERDATA | LVS_SINGLESEL | LVS_SHOWSELALWAYS | WS_TABSTOP.0;
        let list = ui::child(
            page,
            w!("SysListView32"),
            "",
            WINDOW_STYLE(style),
            WS_EX_CLIENTEDGE,
            ids::LIST,
        );
        // SAFETY: plain list-view setup; each column title outlives its insert.
        unsafe {
            let _ = SetWindowSubclass(list, Some(list_proc), 1, 0);
            let ex = LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER;
            SendMessageW(
                list,
                LVM_SETEXTENDEDLISTVIEWSTYLE,
                Some(WPARAM(ex as usize)),
                Some(LPARAM(ex as isize)),
            );
            let dpi = GetDpiForWindow(page).max(96);
            for (i, (title, width, right, _)) in COLUMNS.iter().enumerate() {
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
                    list,
                    LVM_INSERTCOLUMNW,
                    Some(WPARAM(i)),
                    Some(LPARAM(&col as *const _ as isize)),
                );
            }
        }
        let name = ui::label(page, "");
        let facts = ui::label(page, "");
        let note = ui::label(page, "");
        let budget = ui::label(page, "");
        let result = ui::label(page, "");
        let fork_caption = ui::cell_label(page, "Resource forks:");
        let combo = WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
        let fork = ui::child(page, w!("COMBOBOX"), "", combo, WINDOW_EX_STYLE(0), "");
        let modes = extract_fork_modes();
        for m in &modes {
            let t = ui::wide(m.label());
            // SAFETY: the combo copies the string.
            unsafe {
                SendMessageW(fork, CB_ADDSTRING, None, Some(LPARAM(t.as_ptr() as isize)));
            }
        }
        let start = modes
            .iter()
            .position(|m| *m == ResourceForkMode::AppleDouble)
            .unwrap_or(0);
        // SAFETY: a plain combo message.
        unsafe {
            SendMessageW(fork, CB_SETCURSEL, Some(WPARAM(start)), None);
        }
        let extract = ui::button(page, "Extract...", ids::EXTRACT);
        let export = ui::button(page, "Export to .tgz...", ids::EXPORT_ENTRY);
        let nested = ui::button(page, "Open as Disk Image", ids::OPEN_NESTED);
        let preview = multiline(page, ids::DETAIL);
        // SAFETY: a plain query of our own page.
        let dpi = unsafe { GetDpiForWindow(page) }.max(96);
        ui::set_font(preview, ui::mono_font_at(dpi));
        let status = ui::label(page, "");
        let view = Rc::new_cyclic(|me: &Weak<BrowseView>| BrowseView {
            ctrl: RefCell::new(new_controller(&notify)),
            notify,
            host,
            sync: SyncGuard::default(),
            me: me.clone(),
            active: Cell::new(false),
            page,
            title,
            volume,
            extract_all,
            export_volume,
            close,
            job,
            progress,
            cancel,
            notice,
            notice_ok,
            message,
            tree,
            list,
            name,
            facts,
            note,
            budget,
            result,
            fork_caption,
            fork,
            extract,
            export,
            nested,
            preview,
            status,
            rows: RefCell::default(),
            lines: RefCell::default(),
            skip: Cell::new(0),
            folders: RefCell::default(),
            folder_items: RefCell::default(),
            panel_key: Cell::new(None),
            label_prefix: RefCell::default(),
            deferred: RefCell::default(),
            follow_timer: Cell::new(0),
            on_change: RefCell::default(),
            shape: Cell::new(u32::MAX),
        });
        FOLLOW.with(|f| *f.borrow_mut() = view.me.clone());
        view.set_icons();
        let weak: Weak<dyn Handler> = view.me.clone();
        panel::register(page, weak);
        ui::set_visible(notice, false);
        ui::set_visible(notice_ok, false);
        view.refresh();
        view
    }

    pub fn is_active(&self) -> bool {
        self.active.get()
    }

    /// Called when the view opens or closes, for the screen that shows it.
    pub fn on_change(&self, f: Rc<dyn Fn()>) {
        *self.on_change.borrow_mut() = Some(f);
    }

    fn changed(&self) {
        let f = self.on_change.borrow().clone();
        if let Some(f) = f {
            f();
        }
    }

    pub fn has_unsaved_edits(&self) -> bool {
        self.active.get() && self.ctrl.borrow().has_unsaved_edits()
    }

    /// Open the partition the Inspect controller chose, with its edit flow and label prefix.
    pub fn open_request(&self, req: BrowseOpen) {
        self.close();
        let BrowseOpen {
            session,
            edit,
            label_prefix,
            backup_folder,
        } = req;
        let label = session
            .source_path
            .as_ref()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "partition".into());
        let fx = {
            let mut ctl = self.ctrl.borrow_mut();
            let fx = ctl.open_inspect(session, &label);
            match edit {
                BrowseEdit::ReadOnly => {}
                BrowseEdit::InPlace => ctl.inspect_edit_in_place(),
                BrowseEdit::Archive(ctx) => ctl.inspect_edit_archive(ctx),
            }
            if let Some(folder) = backup_folder {
                ctl.inspect_backup_folder(folder);
            }
            fx
        };
        *self.label_prefix.borrow_mut() = label_prefix.unwrap_or_default();
        self.active.set(true);
        self.after(fx);
        self.changed();
        // SAFETY: focusing our own list.
        unsafe {
            let _ = windows::Win32::UI::Input::KeyboardAndMouse::SetFocus(Some(self.list));
        }
    }

    /// The streamed backup's seekable cache is ready; the listing stays where it is.
    pub fn upgrade_seekable(&self, path: PathBuf) {
        if self.active.get() {
            self.apply(A::Mode(ModeAction::UpgradeSeekable(path)));
        }
    }

    pub fn close(&self) {
        if !self.active.replace(false) {
            return;
        }
        let fresh = new_controller(&self.notify);
        // The old pane's readers go before its CHD diff or archive temp is removed.
        self.ctrl.replace(fresh).close_inspect();
        self.label_prefix.borrow_mut().clear();
        self.show_notice(None);
        self.refresh();
        self.changed();
    }

    /// A finished "Open as Disk Image" copy, as the load the Inspect controller takes.
    pub fn take_nested_load(&self) -> Option<InspectAction> {
        let (path, guard) = self.ctrl.borrow_mut().take_nested_open()?;
        Some(InspectAction::Load(InspectSource::Image {
            path,
            temp: Some(guard),
        }))
    }

    fn apply(&self, action: A) {
        let fx = self.ctrl.borrow_mut().apply(action);
        self.after(fx);
    }

    pub fn tick(&self) {
        if self.active.get() {
            let fx = self.ctrl.borrow_mut().tick();
            self.after(fx);
        }
    }

    fn reply(&self) -> ReplyFn {
        let me = self.me.clone();
        Rc::new(move |reply| {
            let Some(view) = me.upgrade() else {
                return;
            };
            let action = match reply {
                Reply::Picked { purpose, path } => A::Picked { purpose, path },
                Reply::Confirmed { purpose, yes } => A::Confirmed { purpose, yes },
                Reply::Answered { purpose, text } => A::Answered { purpose, text },
                Reply::Chose { purpose, choice } => A::Chose { purpose, choice },
                Reply::CloseScreen => A::Mode(ModeAction::RequestClose),
            };
            view.apply(action);
        })
    }

    fn after(&self, effects: Vec<Effect>) {
        self.host.run(effects, &self.reply());
        if self.ctrl.borrow_mut().take_close_request() {
            self.close();
            return;
        }
        self.take_results();
        // The side panel already shows File Info, so the window a double-click asks for is not opened.
        if self.ctrl.borrow().detail().is_some() {
            self.ctrl.borrow_mut().apply(A::CloseDetail);
        }
        self.refresh();
    }

    /// What jobs ended with: the notices; the tree listing, journal and text editor come later.
    fn take_results(&self) {
        let lines: Vec<String> = {
            let mut ctl = self.ctrl.borrow_mut();
            let pane = ctl.host_pane();
            let _ = pane.take_tree_text();
            let _ = pane.take_journal();
            let archive = pane.take_archive_notice();
            let _ = ctl.take_text_doc();
            let notice = ctl.take_mode_notice();
            let error = ctl.take_mode_error();
            [notice, archive, error].into_iter().flatten().collect()
        };
        if let Some(last) = lines.last() {
            self.show_notice(Some(last));
        }
        crate::app::append_log(lines.into_iter().map(|l| (LogLevel::Info, l)).collect());
    }

    fn show_notice(&self, text: Option<&str>) {
        ui::set_text(self.notice, text.unwrap_or(""));
        ui::set_visible(self.notice, text.is_some());
        ui::set_visible(self.notice_ok, text.is_some());
        if self.shape.get() != self.shape_now() {
            self.layout();
        }
    }

    fn extract_options(&self) -> ExtractOptions {
        // SAFETY: a plain combo query.
        let i = unsafe { SendMessageW(self.fork, CB_GETCURSEL, None, None).0 };
        let fork_mode = usize::try_from(i)
            .ok()
            .and_then(|i| extract_fork_modes().get(i).copied())
            .unwrap_or(ResourceForkMode::AppleDouble);
        ExtractOptions {
            fork_mode,
            ..ExtractOptions::default()
        }
    }

    fn extract(&self, path: String) {
        let options = self.extract_options();
        self.apply(A::Extract {
            from: Side::Left,
            path,
            options,
            dest: None,
        });
    }

    fn cursor_path(&self) -> Option<String> {
        let ctl = self.ctrl.borrow();
        ctl.side_detail(Side::Left).map(|d| d.entry.path.clone())
    }

    /// Run a key through the key map; true when it was bound.
    fn key(&self, key: Key, mods: Mods) -> bool {
        if !self.active.get() {
            return false;
        }
        let command = {
            let ctl = self.ctrl.borrow();
            if ctl.modal_open() || !ctl.binds(key, mods) {
                return false;
            }
            ctl.command_for(key, mods)
        };
        match command {
            Some(Command::Extract) => {
                if let Some(path) = self.cursor_path() {
                    self.extract(path);
                }
            }
            _ => self.apply(A::Key(key, mods)),
        }
        true
    }

    fn read(&self) -> Shown {
        let prefix = self.label_prefix.borrow();
        let ctl = self.ctrl.borrow();
        let pane = ctl.pane(Side::Left);
        let all = pane.rows();
        let rows = (*self.rows.borrow() != all).then(|| all.to_vec());
        let boxes = pane.marks_with_boxes();
        let skip = match boxes {
            true => all.iter().take_while(|r| r.is_parent()).count(),
            false => 0,
        };
        let lines = match &rows {
            Some(rows) => rows
                .iter()
                .skip(skip)
                .map(|r| line_of(r, pane.row_label(r, false).text))
                .collect(),
            None => Vec::new(),
        };
        let folders: Vec<(usize, String, bool)> = pane
            .folder_tree()
            .iter()
            .map(|n| {
                let name = match n.depth {
                    0 => "/".to_string(),
                    _ => n.entry.name.clone(),
                };
                (n.depth, name, n.expanded)
            })
            .collect();
        let folders = (*self.folders.borrow() != folders).then_some(folders);
        let message = match (pane.opening(), pane.source_error()) {
            (Some(_), _) => "Reading the volume...".to_string(),
            (None, Some(err)) => err.to_string(),
            (None, None) if pane.is_opening() => "Opening...".to_string(),
            (None, None) => String::new(),
        };
        let mut jobs: Vec<(Snapshot, bool)> = Vec::new();
        jobs.extend(pane.opening().map(|s| (s, true)));
        jobs.extend(ctl.copy_job().map(|(_, s)| (s, true)));
        jobs.extend(ctl.mode_working().map(|s| (s, false)));
        jobs.extend(pane.checking().map(|s| (s, false)));
        jobs.extend(pane.applying().map(|s| (s, false)));
        jobs.extend(pane.tool_working().map(|s| (s, false)));
        jobs.extend(pane.archive_working().map(|s| (s, false)));
        let caps = ctl.browse_caps();
        let detail = ctl.side_detail(Side::Left);
        let panel_key = detail.map(|d| (d.serial, d.loading));
        Shown {
            loaded: pane.listing.is_loaded(),
            volume: ctl.volume_line(Side::Left, &prefix),
            message,
            export_volume: !ctl.edit_mode() && ctl.copy_job().is_none(),
            job: jobs.into_iter().next(),
            rsrc: all.iter().any(|r| r.rsrc_size > 0),
            rows,
            lines,
            cursor: pane.cursor_index().filter(|i| *i >= skip).map(|i| i - skip),
            folders,
            folder_tree: pane.listing_style() == ListingStyle::TreeAndList,
            folder_cursor: pane.tree_cursor(),
            panel: detail
                .filter(|_| self.panel_key.get() != panel_key)
                .map(|d| d.side_panel(caps)),
            panel_key,
            result: detail.and_then(|d| d.result.clone()),
            actions: ctl.panel_actions(Side::Left, false),
            status: ctl.status().to_string(),
            follow: ctl.follow_pending(),
        }
    }

    /// Read the controller first and release it, then set the controls inside the guard.
    fn refresh(&self) {
        let shown = self.read();
        let follow = shown.follow;
        self.sync.run(|| self.show(shown));
        if self.shape.get() != self.shape_now() {
            self.layout();
        }
        if follow && self.follow_timer.get() == 0 {
            let ms = FOLLOW_DELAY.as_millis() as u32;
            // SAFETY: a one-shot thread timer; its callback finds the view through FOLLOW.
            let id = unsafe { SetTimer(None, 0, ms, Some(follow_fired)) };
            self.follow_timer.set(id);
        }
    }

    fn show(&self, s: Shown) {
        let v = &s.volume;
        let parts = [&v.fs, &v.usage, &v.label, &v.blessed];
        let line: Vec<&str> = parts
            .iter()
            .map(|p| p.as_str())
            .filter(|p| !p.is_empty())
            .collect();
        ui::set_text(self.volume, &line.join("    "));
        ui::set_visible(self.extract_all, s.export_volume && s.loaded);
        ui::set_visible(self.export_volume, s.export_volume && s.loaded);
        let (job_text, pct) = s
            .job
            .as_ref()
            .map(|(j, _)| crate::inspect::job_line(j))
            .unwrap_or_default();
        ui::set_text(self.job, &job_text);
        ui::set_visible(self.job, s.job.is_some());
        crate::inspect::set_progress(self.progress, s.job.is_some(), pct);
        ui::set_visible(self.cancel, s.job.as_ref().is_some_and(|(_, c)| *c));
        ui::set_text(self.message, &s.message);
        ui::set_visible(self.message, !s.message.is_empty());
        ui::set_visible(self.tree, s.folder_tree);
        if let Some(folders) = s.folders {
            self.show_folders(folders);
        }
        if s.folder_tree {
            let items = self.folder_items.borrow();
            if let Some(item) = items.get(s.folder_cursor) {
                // SAFETY: an item of our own tree.
                unsafe {
                    SendMessageW(
                        self.tree,
                        TVM_SELECTITEM,
                        Some(WPARAM(TVGN_CARET as usize)),
                        Some(LPARAM(*item)),
                    );
                }
            }
        }
        if let Some(rows) = s.rows {
            self.show_rows(rows, s.lines, s.rsrc);
        }
        if let Some(i) = s.cursor {
            self.select(i);
        }
        self.show_panel(s.panel_key, s.panel, s.result, s.actions);
        ui::set_text(self.status, &s.status);
    }

    fn show_rows(&self, rows: Vec<DisplayRow>, lines: Vec<Line>, rsrc: bool) {
        let skip = rows.len() - lines.len();
        self.skip.set(skip);
        *self.rows.borrow_mut() = rows;
        let count = lines.len();
        *self.lines.borrow_mut() = lines;
        // SAFETY: the virtual list reads its text back through LVN_GETDISPINFO.
        unsafe {
            let dpi = GetDpiForWindow(self.page).max(96);
            let width = if rsrc {
                layout::scale(COLUMNS[2].1, dpi)
            } else {
                0
            };
            SendMessageW(
                self.list,
                LVM_SETCOLUMNWIDTH,
                Some(WPARAM(2)),
                Some(LPARAM(width as isize)),
            );
            SendMessageW(
                self.list,
                LVM_SETITEMCOUNT,
                Some(WPARAM(count)),
                Some(LPARAM(LVSICF_NOSCROLL as isize)),
            );
            let last = count.saturating_sub(1);
            SendMessageW(
                self.list,
                LVM_REDRAWITEMS,
                Some(WPARAM(0)),
                Some(LPARAM(last as isize)),
            );
        }
    }

    /// Give the list and the folder tree the row icons for the page's DPI and the theme.
    pub fn set_icons(&self) {
        // SAFETY: each control takes its new image list; the one it gives back is freed.
        unsafe {
            let dpi = GetDpiForWindow(self.page).max(96);
            let old = SendMessageW(
                self.list,
                LVM_SETIMAGELIST,
                Some(WPARAM(LVSIL_SMALL as usize)),
                Some(LPARAM(row_icons::image_list(dpi).0 as isize)),
            );
            if old.0 != 0 {
                let _ = ImageList_Destroy(Some(HIMAGELIST(old.0 as _)));
            }
            let old = SendMessageW(
                self.tree,
                TVM_SETIMAGELIST,
                Some(WPARAM(TVSIL_NORMAL as usize)),
                Some(LPARAM(row_icons::image_list(dpi).0 as isize)),
            );
            if old.0 != 0 {
                let _ = ImageList_Destroy(Some(HIMAGELIST(old.0 as _)));
            }
        }
    }

    /// Select the cursor's row in the list, inside the sync guard so no click comes back.
    fn select(&self, i: usize) {
        let on = LIST_VIEW_ITEM_STATE_FLAGS(LVIS_SELECTED.0 | LVIS_FOCUSED.0);
        let item = LVITEMW {
            stateMask: on,
            state: on,
            ..Default::default()
        };
        // SAFETY: plain list-view messages with a local item.
        unsafe {
            SendMessageW(
                self.list,
                LVM_SETITEMSTATE,
                Some(WPARAM(i)),
                Some(LPARAM(&item as *const _ as isize)),
            );
            SendMessageW(
                self.list,
                LVM_ENSUREVISIBLE,
                Some(WPARAM(i)),
                Some(LPARAM(0)),
            );
        }
    }

    /// Build the folder tree again from the controller's flat list of visible folders.
    fn show_folders(&self, folders: Vec<(usize, String, bool)>) {
        let mut items: Vec<isize> = Vec::with_capacity(folders.len());
        let mut parents: Vec<isize> = Vec::new();
        // SAFETY: tree-view messages on our own tree; each text outlives its insert.
        unsafe {
            SendMessageW(self.tree, TVM_DELETEITEM, None, Some(LPARAM(TVI_ROOT.0)));
            for (i, (depth, name, open)) in folders.iter().enumerate() {
                let icon = match open {
                    true => row_icons::FOLDER_OPEN,
                    false => row_icons::FOLDER,
                };
                parents.truncate(*depth);
                let parent = parents.last().copied().unwrap_or(TVI_ROOT.0);
                let mut text = ui::wide(&ui::plain(name));
                let insert = TVINSERTSTRUCTW {
                    hParent: HTREEITEM(parent),
                    hInsertAfter: TVI_LAST,
                    Anonymous: TVINSERTSTRUCTW_0 {
                        item: TVITEMW {
                            mask: TVIF_TEXT
                                | TVIF_PARAM
                                | TVIF_CHILDREN
                                | TVIF_IMAGE
                                | TVIF_SELECTEDIMAGE,
                            iImage: icon,
                            iSelectedImage: icon,
                            pszText: PWSTR(text.as_mut_ptr()),
                            lParam: LPARAM(i as isize),
                            cChildren: windows::Win32::UI::Controls::TVITEMEXW_CHILDREN(1),
                            ..Default::default()
                        },
                    },
                };
                let item = SendMessageW(
                    self.tree,
                    TVM_INSERTITEMW,
                    None,
                    Some(LPARAM(&insert as *const _ as isize)),
                )
                .0;
                items.push(item);
                parents.push(item);
            }
            for (i, (_, _, expanded)) in folders.iter().enumerate() {
                if *expanded {
                    SendMessageW(
                        self.tree,
                        TVM_EXPAND,
                        Some(WPARAM(TVE_EXPAND.0 as usize)),
                        Some(LPARAM(items[i])),
                    );
                }
            }
            if let Some(first) = items.first() {
                SendMessageW(self.tree, TVM_ENSUREVISIBLE, None, Some(LPARAM(*first)));
            }
        }
        *self.folders.borrow_mut() = folders;
        *self.folder_items.borrow_mut() = items;
    }

    fn show_panel(
        &self,
        key: Option<(u64, bool)>,
        panel: Option<SidePanel>,
        result: Option<String>,
        actions: Option<PanelActions>,
    ) {
        if key.is_none() {
            for h in [self.name, self.facts, self.note, self.budget, self.preview] {
                ui::set_text(h, "");
            }
        }
        self.panel_key.set(key);
        if let Some(p) = panel {
            ui::set_text(self.name, &p.name);
            ui::set_text(self.facts, &p.facts.join("\r\n"));
            ui::set_text(self.note, p.note.as_deref().unwrap_or(""));
            ui::set_text(self.budget, &p.budget.join("\r\n"));
            ui::set_text(self.preview, &p.preview.text().replace('\n', "\r\n"));
        }
        ui::set_text(self.result, result.as_deref().unwrap_or(""));
        let a = actions.unwrap_or_default();
        let (extract, export) = match a.folder {
            true => ("Extract Folder...", "Export Folder to .tgz..."),
            false => ("Extract File...", "Export File to .tgz..."),
        };
        ui::set_text(self.extract, extract);
        ui::set_text(self.export, export);
        ui::set_visible(self.extract, a.extractable);
        ui::set_visible(self.export, a.extractable && a.export);
        ui::set_enabled(self.extract, !a.copying);
        ui::set_enabled(self.export, !a.copying);
        ui::set_visible(self.nested, a.extractable && a.nested);
        ui::set_visible(self.fork_caption, a.extractable && a.forks);
        ui::set_visible(self.fork, a.extractable && a.forks);
    }

    /// Which optional controls are up, so a change lays the page out again.
    fn shape_now(&self) -> u32 {
        [
            self.job,
            self.notice,
            self.message,
            self.tree,
            self.extract,
            self.export,
            self.nested,
            self.fork,
            self.extract_all,
        ]
        .iter()
        .enumerate()
        .map(|(i, h)| (ui::shown(*h) as u32) << i)
        .sum()
    }

    fn layout(&self) {
        self.shape.set(self.shape_now());
        let (area, dpi) = panel::client(self.page);
        let line = |h: HWND| Node::leaf(h, Size::Stretch(1), Size::Fixed(layout::ROW - 8));
        let fixed = |h: HWND, n: i32| Node::leaf(h, Size::Fixed(DETAIL_W), Size::Fixed(n));
        let mut col = vec![Node::row(vec![
            Node::leaf(self.title, Size::Fixed(130), Size::Fixed(layout::ROW - 8)),
            line(self.volume),
        ])];
        let mut tools = Vec::new();
        if ui::shown(self.extract_all) {
            tools.push(Node::button(self.extract_all, 110));
            tools.push(Node::button(self.export_volume, 150));
        }
        tools.push(Node::Space(Size::Stretch(1)));
        tools.push(Node::button(self.close, 80));
        col.push(Node::row(tools));
        if ui::shown(self.job) {
            let mut r = vec![
                line(self.job),
                Node::leaf(
                    self.progress,
                    Size::Fixed(160),
                    Size::Fixed(layout::ROW - 6),
                ),
            ];
            if ui::shown(self.cancel) {
                r.push(Node::button(self.cancel, 80));
            }
            col.push(Node::row(r));
        }
        if ui::shown(self.notice) {
            col.push(Node::row(vec![
                line(self.notice),
                Node::button(self.notice_ok, 60),
            ]));
        }
        if ui::shown(self.message) {
            col.push(line(self.message));
        }
        let mut side = vec![
            fixed(self.name, layout::ROW - 8),
            fixed(self.facts, (layout::ROW - 8) * 6),
            fixed(self.note, layout::ROW - 8),
            fixed(self.budget, layout::ROW - 8),
            fixed(self.result, layout::ROW - 8),
        ];
        if ui::shown(self.fork) {
            side.push(Node::row(vec![
                Node::leaf(
                    self.fork_caption,
                    Size::Fixed(110),
                    Size::Fixed(layout::ROW),
                ),
                Node::leaf(
                    self.fork,
                    Size::Fixed(DETAIL_W - 110 - layout::GAP),
                    Size::Fixed(layout::ROW),
                ),
            ]));
        }
        let mut buttons = Vec::new();
        if ui::shown(self.extract) {
            buttons.push(Node::button(self.extract, 130));
        }
        if ui::shown(self.export) {
            buttons.push(Node::button(self.export, DETAIL_W - 130 - layout::GAP));
        }
        if !buttons.is_empty() {
            side.push(Node::row(buttons));
        }
        if ui::shown(self.nested) {
            side.push(Node::button(self.nested, 150));
        }
        side.push(Node::leaf(
            self.preview,
            Size::Fixed(DETAIL_W),
            Size::Stretch(1),
        ));
        let mut body = Vec::new();
        if ui::shown(self.tree) {
            body.push(Node::leaf(self.tree, Size::Fixed(TREE_W), Size::Stretch(1)));
        }
        body.push(Node::leaf(self.list, Size::Stretch(1), Size::Stretch(1)));
        body.push(Node::column(side));
        col.push(Node::row(body));
        col.push(line(self.status));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        let hidden = [
            self.job,
            self.progress,
            self.cancel,
            self.notice,
            self.notice_ok,
            self.message,
            self.tree,
            self.fork_caption,
            self.fork,
            self.extract,
            self.export,
            self.nested,
            self.extract_all,
            self.export_volume,
        ];
        for (h, r) in layout::layout(&tree, area, dpi) {
            if hidden.contains(&h) && !ui::shown(h) {
                continue;
            }
            ui::move_to(h, r);
        }
    }

    /// Fill a cell the virtual list asks for, from the rows last read.
    fn display(&self, info: &mut NMLVDISPINFOW) {
        let item = &mut info.item;
        let lines = self.lines.borrow();
        let Some(line) = usize::try_from(item.iItem).ok().and_then(|i| lines.get(i)) else {
            return;
        };
        if item.iSubItem == 0 {
            if item.mask.0 & LVIF_IMAGE.0 != 0 {
                item.iImage = line.icon;
            }
            // An indent is counted in icon widths, so a row inside a folder sits one icon in.
            if item.mask.0 & LVIF_INDENT.0 != 0 {
                item.iIndent = line.depth;
            }
        }
        if item.mask.0 & LVIF_TEXT.0 == 0 || item.pszText.is_null() || item.cchTextMax <= 0 {
            return;
        }
        let cell = line
            .cells
            .get(item.iSubItem as usize)
            .map_or("", |c| c.as_str());
        let text: Vec<u16> = ui::plain(cell).encode_utf16().collect();
        let n = text.len().min(item.cchTextMax as usize - 1);
        // SAFETY: the list gave us a buffer of cchTextMax characters.
        unsafe {
            std::ptr::copy_nonoverlapping(text.as_ptr(), item.pszText.0, n);
            *item.pszText.0.add(n) = 0;
        }
    }

    /// The listing row at list position `i`.
    fn row_at(&self, i: usize) -> Option<DisplayRow> {
        self.rows.borrow().get(i + self.skip.get()).cloned()
    }

    fn list_notify(&self, code: u32, lp: LPARAM) -> Option<LRESULT> {
        match code {
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
                if self.sync.active() || nm.uChanged.0 & LVIF_STATE.0 == 0 || !now || was {
                    return Some(LRESULT(0));
                }
                let row = usize::try_from(nm.iItem).ok().and_then(|i| self.row_at(i));
                if let Some(row) = row {
                    let m = mods();
                    let click = {
                        let ctl = self.ctrl.borrow();
                        ctl.pane(Side::Left).row_click(&row, m.ctrl, m.shift)
                    };
                    self.apply(A::Focus(Side::Left));
                    if let Some(action) = click {
                        self.apply(A::Pane(Side::Left, action));
                    }
                }
                Some(LRESULT(0))
            }
            NM_CLICK => {
                // A click on an icon that is a +/- opens or shuts its row in place.
                // SAFETY: NM_CLICK from a list view comes with an NMITEMACTIVATE.
                let nm = unsafe { &*(lp.0 as *const NMITEMACTIVATE) };
                let mut hit = LVHITTESTINFO {
                    pt: nm.ptAction,
                    ..Default::default()
                };
                // SAFETY: a hit test with a local struct.
                unsafe {
                    SendMessageW(
                        self.list,
                        LVM_SUBITEMHITTEST,
                        None,
                        Some(LPARAM(&mut hit as *mut _ as isize)),
                    );
                }
                let on_icon = hit.flags.0 & LVHT_ONITEMICON.0 != 0 && hit.iSubItem == 0;
                let row = usize::try_from(hit.iItem).ok().and_then(|i| self.row_at(i));
                if let Some(row) = row.filter(|r| on_icon && r.expandable) {
                    self.apply(A::Pane(Side::Left, P::ToggleExpand(row.node_id)));
                }
                Some(LRESULT(0))
            }
            NM_DBLCLK => {
                let id = {
                    let ctl = self.ctrl.borrow();
                    let pane = ctl.pane(Side::Left);
                    pane.cursor_index().map(|i| pane.rows()[i].node_id.clone())
                };
                if let Some(id) = id {
                    self.apply(A::Pane(Side::Left, P::Activate(id)));
                }
                Some(LRESULT(0))
            }
            LVN_KEYDOWN => {
                // SAFETY: LVN_KEYDOWN comes with an NMLVKEYDOWN.
                let nm = unsafe { &*(lp.0 as *const NMLVKEYDOWN) };
                if let Some(key) = key_of(nm.wVKey) {
                    self.key(key, mods());
                }
                Some(LRESULT(0))
            }
            LVN_COLUMNCLICK => {
                // SAFETY: LVN_COLUMNCLICK comes with an NMLISTVIEW.
                let nm = unsafe { &*(lp.0 as *const NMLISTVIEW) };
                let sort = usize::try_from(nm.iSubItem)
                    .ok()
                    .and_then(|i| COLUMNS.get(i))
                    .and_then(|c| c.3);
                if let Some(col) = sort {
                    self.apply(A::Pane(Side::Left, P::SortBy(col)));
                }
                Some(LRESULT(0))
            }
            _ => None,
        }
    }

    fn tree_notify(&self, code: u32, lp: LPARAM) -> Option<LRESULT> {
        if self.sync.active() {
            return None;
        }
        // SAFETY: both notifications come with an NMTREEVIEWW.
        let nm = unsafe { &*(lp.0 as *const NMTREEVIEWW) };
        let action = match code {
            TVN_SELCHANGEDW => P::FolderTreeClick(nm.itemNew.lParam.0 as usize),
            TVN_ITEMEXPANDINGW => P::FolderTreeToggle(nm.itemNew.lParam.0 as usize),
            _ => return None,
        };
        // The tree is built again for the answer, which it must not be while it notifies.
        *self.deferred.borrow_mut() = Some(action);
        // SAFETY: posting to our own page.
        unsafe {
            let _ = PostMessageW(
                Some(self.page),
                WM_COMMAND,
                WPARAM((DEFERRED as usize) << 16),
                LPARAM(self.tree.0 as isize),
            );
        }
        Some(LRESULT((code == TVN_ITEMEXPANDINGW) as isize))
    }
}

impl Handler for BrowseView {
    fn command(&self, from: HWND, code: u32) {
        if code == DEFERRED {
            let action = self.deferred.borrow_mut().take();
            if let Some(action) = action {
                self.apply(A::Focus(Side::Left));
                self.apply(A::Pane(Side::Left, action));
            }
            return;
        }
        if self.sync.active() {
            return;
        }
        if from == self.fork {
            if code == CBN_SELCHANGE {
                let mode = self.extract_options().fork_mode;
                self.apply(A::SetForkMode(mode));
            }
            return;
        }
        if code != windows::Win32::UI::WindowsAndMessaging::BN_CLICKED {
            return;
        }
        if from == self.close {
            self.apply(A::Mode(ModeAction::RequestClose));
        } else if from == self.cancel {
            let copying = self.ctrl.borrow().copy_job().is_some();
            self.apply(match copying {
                true => A::CancelCopy,
                false => A::Pane(Side::Left, P::CancelOpen),
            });
        } else if from == self.notice_ok {
            self.show_notice(None);
        } else if from == self.extract_all {
            self.extract("/".to_string());
        } else if from == self.export_volume {
            self.apply(A::ExportEntry {
                from: Side::Left,
                path: "/".to_string(),
                format: Some(ExportFormat::TarGz),
            });
        } else if from == self.extract {
            if let Some(path) = self.cursor_path() {
                self.extract(path);
            }
        } else if from == self.export {
            if let Some(path) = self.cursor_path() {
                let mode = self.extract_options().fork_mode;
                self.apply(A::SetForkMode(mode));
                self.apply(A::ExportEntry {
                    from: Side::Left,
                    path,
                    format: Some(ExportFormat::TarGz),
                });
            }
        } else if from == self.nested {
            if let Some(path) = self.cursor_path() {
                self.apply(A::Mode(ModeAction::OpenNested(path)));
            }
        }
    }

    fn notify(&self, hdr: &NMHDR, lp: LPARAM) -> Option<LRESULT> {
        if hdr.hwndFrom == self.list {
            self.list_notify(hdr.code, lp)
        } else if hdr.hwndFrom == self.tree {
            self.tree_notify(hdr.code, lp)
        } else {
            None
        }
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {}
}
