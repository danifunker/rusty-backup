//! One browser pane's listing over a `CommanderController` side: the folder tree and the list
//! (a virtual list view with tick boxes for marks and the row icons). GTK's `browser/pane.rs`
//! is the reference; Inspect's browse view has one and Commander two.
//!
//! The view sends nothing itself: [`PaneView::notify`] says what the user did as a
//! [`PaneEvent`], and the screen that owns it turns that into actions on its controller.

use std::cell::{Cell, RefCell};

use rusty_backup::controller::commander::{
    format_size, BrowserPaneAction as P, CommanderController, DisplayRow, ListingStyle, Side,
    SortColumn,
};
use rusty_backup::controller::keys::{Key, Mods};
use windows::core::{w, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::{
    ImageList_Destroy, HIMAGELIST, HTREEITEM, LIST_VIEW_ITEM_STATE_FLAGS, LVCFMT_RIGHT, LVCF_FMT,
    LVCF_TEXT, LVCF_WIDTH, LVCOLUMNW, LVHITTESTINFO, LVHT_ONITEMICON, LVHT_ONITEMSTATEICON,
    LVIF_IMAGE, LVIF_INDENT, LVIF_STATE, LVIF_TEXT, LVIS_FOCUSED, LVIS_SELECTED,
    LVIS_STATEIMAGEMASK, LVITEMW, LVM_ENSUREVISIBLE, LVM_GETCOUNTPERPAGE, LVM_INSERTCOLUMNW,
    LVM_REDRAWITEMS, LVM_SETCALLBACKMASK, LVM_SETCOLUMNWIDTH, LVM_SETEXTENDEDLISTVIEWSTYLE,
    LVM_SETIMAGELIST, LVM_SETITEMCOUNT, LVM_SETITEMSTATE, LVM_SUBITEMHITTEST, LVN_BEGINDRAG,
    LVN_COLUMNCLICK, LVN_GETDISPINFOW, LVN_ITEMCHANGED, LVN_KEYDOWN, LVSICF_NOSCROLL, LVSIL_SMALL,
    LVS_EX_CHECKBOXES, LVS_EX_DOUBLEBUFFER, LVS_EX_FULLROWSELECT, LVS_OWNERDATA, LVS_REPORT,
    LVS_SHOWSELALWAYS, LVS_SINGLESEL, NMHDR, NMITEMACTIVATE, NMLISTVIEW, NMLVDISPINFOW,
    NMLVKEYDOWN, NMTREEVIEWW, NM_CLICK, NM_DBLCLK, NM_RCLICK, TVE_EXPAND, TVGN_CARET,
    TVIF_CHILDREN, TVIF_IMAGE, TVIF_PARAM, TVIF_SELECTEDIMAGE, TVIF_TEXT, TVINSERTSTRUCTW,
    TVINSERTSTRUCTW_0, TVITEMW, TVI_LAST, TVI_ROOT, TVM_DELETEITEM, TVM_ENSUREVISIBLE, TVM_EXPAND,
    TVM_INSERTITEMW, TVM_SELECTITEM, TVM_SETIMAGELIST, TVN_ITEMEXPANDINGW, TVN_SELCHANGEDW,
    TVSIL_NORMAL, TVS_HASBUTTONS, TVS_HASLINES, TVS_LINESATROOT, TVS_SHOWSELALWAYS,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::Input::KeyboardAndMouse::{
    GetKeyState, SetFocus, VIRTUAL_KEY, VK_BACK, VK_CONTROL, VK_DELETE, VK_DOWN, VK_END, VK_ESCAPE,
    VK_F1, VK_F24, VK_HOME, VK_INSERT, VK_LEFT, VK_MENU, VK_NEXT, VK_PRIOR, VK_RETURN, VK_RIGHT,
    VK_SHIFT, VK_SPACE, VK_TAB, VK_UP,
};
use windows::Win32::UI::Shell::{DefSubclassProc, SetWindowSubclass};
use windows::Win32::UI::WindowsAndMessaging::{
    PostMessageW, SendMessageW, DLGC_WANTMESSAGE, MSG, WINDOW_STYLE, WM_COMMAND, WM_GETDLGCODE,
    WM_KEYDOWN, WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::layout;
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

/// A `WM_COMMAND` code the pane posts to its parent: run the folder-tree action it holds.
pub const DEFERRED: u32 = 0x7F00;

/// What the user did in the pane, for its screen to act on.
pub enum PaneEvent {
    /// The selection moved to `row` (a click or the arrow keys).
    Click {
        row: DisplayRow,
        ctrl: bool,
        shift: bool,
    },
    /// The row's tick box.
    Mark(String),
    /// The row's +/- icon.
    Toggle(String),
    /// A double-click: open what is under the cursor.
    Activate,
    Key(Key, Mods),
    Sort(SortColumn),
    /// A right-click on `row`.
    Menu(DisplayRow),
    /// A row was dragged off the list.
    Drag,
}

/// One listing row as the list view draws it.
#[derive(Clone, PartialEq)]
struct Line {
    cells: [String; 5],
    marked: bool,
    node_id: String,
    icon: i32,
    depth: i32,
}

/// What a refresh shows, read while the controller is borrowed.
pub struct PaneShown {
    rows: Option<Vec<DisplayRow>>,
    lines: Vec<Line>,
    rsrc: bool,
    cursor: Option<usize>,
    folders: Option<Vec<(usize, String, bool)>>,
    folder_tree: bool,
    folder_cursor: usize,
    marked: Vec<bool>,
}

pub struct PaneView {
    side: Side,
    parent: HWND,
    pub list: HWND,
    pub tree: HWND,
    sync: SyncGuard,
    rows: RefCell<Vec<DisplayRow>>,
    lines: RefCell<Vec<Line>>,
    /// Listing positions shift by the `..` rows a tick-box listing leaves out.
    skip: Cell<usize>,
    folders: RefCell<Vec<(usize, String, bool)>>,
    folder_items: RefCell<Vec<isize>>,
    deferred: RefCell<Option<P>>,
}

/// The controller's name for a key the list hands over; `None` for one it leaves to the list.
pub fn key_of(vk: u16) -> Option<Key> {
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
        VK_SPACE => Key::Space,
        VK_UP | VK_DOWN | VK_PRIOR | VK_NEXT | VK_HOME | VK_END => return None,
        _ => {
            let c = char::from_u32(u32::from(vk)).filter(|c| c.is_ascii_alphanumeric())?;
            Key::Char(c.to_ascii_lowercase())
        }
    })
}

/// The list asks the dialog loop for Enter, Escape and Tab, which `IsDialogMessage` would keep.
unsafe extern "system" fn list_proc(
    hwnd: HWND,
    msg: u32,
    wp: WPARAM,
    lp: LPARAM,
    _id: usize,
    data: usize,
) -> LRESULT {
    if msg == WM_GETDLGCODE && lp.0 != 0 {
        // SAFETY: WM_GETDLGCODE's lParam, when set, is the MSG being dispatched.
        let m = unsafe { &*(lp.0 as *const MSG) };
        let key = VIRTUAL_KEY(m.wParam.0 as u16);
        let tab = key == VK_TAB && data != 0;
        if m.message == WM_KEYDOWN && (key == VK_RETURN || key == VK_ESCAPE || tab) {
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

pub fn mods() -> Mods {
    Mods {
        ctrl: held(VK_CONTROL),
        alt: held(VK_MENU),
        shift: held(VK_SHIFT),
        cmd: false,
    }
}

fn line_of(row: &DisplayRow, name: String) -> Line {
    Line {
        marked: false,
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

impl PaneView {
    /// The pane's tree and list on `parent`; `tab_keys` gives Tab to the key map (Commander's
    /// pane switch) rather than to the dialog loop.
    pub fn build(parent: HWND, side: Side, list_id: &str, tree_id: &str, tab_keys: bool) -> Self {
        let tree_style =
            TVS_HASBUTTONS | TVS_HASLINES | TVS_LINESATROOT | TVS_SHOWSELALWAYS | WS_TABSTOP.0;
        let tree = ui::child(
            parent,
            w!("SysTreeView32"),
            "",
            WINDOW_STYLE(tree_style),
            WS_EX_CLIENTEDGE,
            tree_id,
        );
        let style = LVS_REPORT | LVS_OWNERDATA | LVS_SINGLESEL | LVS_SHOWSELALWAYS | WS_TABSTOP.0;
        let list = ui::child(
            parent,
            w!("SysListView32"),
            "",
            WINDOW_STYLE(style),
            WS_EX_CLIENTEDGE,
            list_id,
        );
        // SAFETY: plain list-view setup; each column title outlives its insert.
        unsafe {
            let _ = SetWindowSubclass(list, Some(list_proc), 1, usize::from(tab_keys));
            let ex = LVS_EX_FULLROWSELECT | LVS_EX_DOUBLEBUFFER | LVS_EX_CHECKBOXES;
            // A virtual list asks for its tick boxes' state, which is the controller's marks.
            SendMessageW(
                list,
                LVM_SETCALLBACKMASK,
                Some(WPARAM(LVIS_STATEIMAGEMASK.0 as usize)),
                None,
            );
            SendMessageW(
                list,
                LVM_SETEXTENDEDLISTVIEWSTYLE,
                Some(WPARAM(ex as usize)),
                Some(LPARAM(ex as isize)),
            );
            let dpi = GetDpiForWindow(parent).max(96);
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
        let view = PaneView {
            side,
            parent,
            list,
            tree,
            sync: SyncGuard::default(),
            rows: RefCell::default(),
            lines: RefCell::default(),
            skip: Cell::new(0),
            folders: RefCell::default(),
            folder_items: RefCell::default(),
            deferred: RefCell::default(),
        };
        view.set_icons();
        view
    }

    pub fn read(&self, ctl: &CommanderController) -> PaneShown {
        let pane = ctl.pane(self.side);
        let all = pane.rows();
        let rows = (*self.rows.borrow() != all).then(|| all.to_vec());
        let skip = match pane.marks_with_boxes() {
            true => all.iter().take_while(|r| r.is_parent()).count(),
            false => 0,
        };
        let marked: Vec<bool> = all
            .iter()
            .skip(skip)
            .map(|r| pane.has_selection() && pane.row_selected(r))
            .collect();
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
        PaneShown {
            rsrc: all.iter().any(|r| r.rsrc_size > 0),
            rows,
            lines,
            cursor: pane.cursor_index().filter(|i| *i >= skip).map(|i| i - skip),
            folders,
            folder_tree: pane.listing_style() == ListingStyle::TreeAndList,
            folder_cursor: pane.tree_cursor(),
            marked,
        }
    }

    pub fn show(&self, s: PaneShown) {
        self.sync.run(|| {
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
            self.show_marks(s.marked);
        });
    }

    pub fn focus(&self) {
        // SAFETY: focusing our own list.
        unsafe {
            let _ = SetFocus(Some(self.list));
        }
    }

    /// The rows one page of the list shows, for Page Up and Page Down.
    pub fn page_rows(&self) -> i32 {
        // SAFETY: a plain list-view query.
        let n = unsafe { SendMessageW(self.list, LVM_GETCOUNTPERPAGE, None, None).0 };
        i32::try_from(n).unwrap_or(1).max(1)
    }

    /// The folder-tree action the pane posted to its parent, once.
    pub fn take_deferred(&self) -> Option<P> {
        self.deferred.borrow_mut().take()
    }

    fn show_rows(&self, rows: Vec<DisplayRow>, lines: Vec<Line>, rsrc: bool) {
        let skip = rows.len() - lines.len();
        self.skip.set(skip);
        *self.rows.borrow_mut() = rows;
        let count = lines.len();
        *self.lines.borrow_mut() = lines;
        // SAFETY: the virtual list reads its text back through LVN_GETDISPINFO.
        unsafe {
            let dpi = GetDpiForWindow(self.parent).max(96);
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

    /// Draw the tick boxes again where the marks changed.
    fn show_marks(&self, marked: Vec<bool>) {
        let (changed, count) = {
            let mut lines = self.lines.borrow_mut();
            let mut changed = false;
            for (line, m) in lines.iter_mut().zip(marked) {
                if line.marked != m {
                    line.marked = m;
                    changed = true;
                }
            }
            (changed, lines.len())
        };
        // The list may ask for a row's text while it redraws, so the rows are not held here.
        if changed {
            let last = count.saturating_sub(1);
            // SAFETY: the virtual list reads the marks back through LVN_GETDISPINFO.
            unsafe {
                SendMessageW(
                    self.list,
                    LVM_REDRAWITEMS,
                    Some(WPARAM(0)),
                    Some(LPARAM(last as isize)),
                );
            }
        }
    }

    /// Give the list and the folder tree the row icons for the window's DPI and the theme.
    pub fn set_icons(&self) {
        // SAFETY: each control takes its new image list; the one it gives back is freed.
        unsafe {
            let dpi = GetDpiForWindow(self.parent).max(96);
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

    /// Fill a cell the virtual list asks for, from the rows last read.
    fn display(&self, info: &mut NMLVDISPINFOW) {
        let item = &mut info.item;
        let lines = self.lines.borrow();
        let Some(line) = usize::try_from(item.iItem).ok().and_then(|i| lines.get(i)) else {
            return;
        };
        if item.iSubItem == 0 {
            if item.mask.0 & LVIF_STATE.0 != 0 {
                // State image 2 is a ticked box, 1 an empty one.
                let image = if line.marked { 2u32 } else { 1 };
                item.state = LIST_VIEW_ITEM_STATE_FLAGS(image << 12);
                item.stateMask = LVIS_STATEIMAGEMASK;
            }
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

    /// A notification from the pane's list or tree: its answer, and what the user did, if anything.
    /// `None` when it came from another control.
    pub fn notify(&self, hdr: &NMHDR, lp: LPARAM) -> Option<(Option<LRESULT>, Option<PaneEvent>)> {
        if hdr.hwndFrom == self.list {
            Some(self.list_notify(hdr.code, lp))
        } else if hdr.hwndFrom == self.tree {
            Some((self.tree_notify(hdr.code, lp), None))
        } else {
            None
        }
    }

    fn list_notify(&self, code: u32, lp: LPARAM) -> (Option<LRESULT>, Option<PaneEvent>) {
        let done = Some(LRESULT(0));
        match code {
            LVN_GETDISPINFOW => {
                // SAFETY: LVN_GETDISPINFOW comes with an NMLVDISPINFOW.
                let info = unsafe { &mut *(lp.0 as *mut NMLVDISPINFOW) };
                self.display(info);
                (done, None)
            }
            LVN_ITEMCHANGED => {
                // SAFETY: LVN_ITEMCHANGED comes with an NMLISTVIEW.
                let nm = unsafe { &*(lp.0 as *const NMLISTVIEW) };
                let now = nm.uNewState & LVIS_SELECTED.0 != 0;
                let was = nm.uOldState & LVIS_SELECTED.0 != 0;
                if self.sync.active() || nm.uChanged.0 & LVIF_STATE.0 == 0 || !now || was {
                    return (done, None);
                }
                let row = usize::try_from(nm.iItem).ok().and_then(|i| self.row_at(i));
                let m = mods();
                let event = row.map(|row| PaneEvent::Click {
                    row,
                    ctrl: m.ctrl,
                    shift: m.shift,
                });
                (done, event)
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
                let on_box = hit.flags.0 & LVHT_ONITEMSTATEICON.0 != 0;
                let row = usize::try_from(hit.iItem).ok().and_then(|i| self.row_at(i));
                let event = match row {
                    Some(row) if on_box => Some(PaneEvent::Mark(row.node_id)),
                    Some(row) if on_icon && row.expandable => Some(PaneEvent::Toggle(row.node_id)),
                    _ => None,
                };
                (done, event)
            }
            NM_RCLICK => {
                // SAFETY: NM_RCLICK from a list view comes with an NMITEMACTIVATE.
                let nm = unsafe { &*(lp.0 as *const NMITEMACTIVATE) };
                let row = usize::try_from(nm.iItem).ok().and_then(|i| self.row_at(i));
                (done, row.map(PaneEvent::Menu))
            }
            NM_DBLCLK => (done, Some(PaneEvent::Activate)),
            LVN_KEYDOWN => {
                // SAFETY: LVN_KEYDOWN comes with an NMLVKEYDOWN.
                let nm = unsafe { &*(lp.0 as *const NMLVKEYDOWN) };
                (done, key_of(nm.wVKey).map(|k| PaneEvent::Key(k, mods())))
            }
            LVN_COLUMNCLICK => {
                // SAFETY: LVN_COLUMNCLICK comes with an NMLISTVIEW.
                let nm = unsafe { &*(lp.0 as *const NMLISTVIEW) };
                let sort = usize::try_from(nm.iSubItem)
                    .ok()
                    .and_then(|i| COLUMNS.get(i))
                    .and_then(|c| c.3);
                (done, sort.map(PaneEvent::Sort))
            }
            LVN_BEGINDRAG => (done, Some(PaneEvent::Drag)),
            _ => (None, None),
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
        // SAFETY: posting to our own parent.
        unsafe {
            let _ = PostMessageW(
                Some(self.parent),
                WM_COMMAND,
                WPARAM((DEFERRED as usize) << 16),
                LPARAM(self.tree.0 as isize),
            );
        }
        Some(LRESULT((code == TVN_ITEMEXPANDINGW) as isize))
    }
}
