//! A window the controller describes (`commander::PaneWindow`): its parts top to bottom and its
//! buttons along the bottom, each sending what it carries.
//!
//! The owner keeps one per kind. While the controller's key for it stays the same the window
//! only takes new text, ticks and enabled states ([`ModelWindow::update`]), so what is typed in
//! a field survives; a new key builds it again in the same place. The window keeps the option a
//! choice has picked and what its fields hold, and hands those to `commander::submit`.

use std::cell::Cell;
use std::rc::{Rc, Weak};

use rusty_backup::controller::commander::{
    field_action, submit, CommanderAction as A, FieldId, PaneWindow, Part, Press, Tone, WindowKind,
};
use windows::core::{w, PCWSTR, PWSTR};
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::UI::Controls::{
    LIST_VIEW_ITEM_STATE_FLAGS, LVCF_WIDTH, LVCOLUMNW, LVIF_STATE, LVIF_TEXT, LVIS_STATEIMAGEMASK,
    LVITEMW, LVM_DELETEALLITEMS, LVM_INSERTCOLUMNW, LVM_INSERTITEMW, LVM_SETCOLUMNWIDTH,
    LVM_SETEXTENDEDLISTVIEWSTYLE, LVM_SETITEMSTATE, LVN_ITEMCHANGED, LVSCW_AUTOSIZE_USEHEADER,
    LVS_EX_CHECKBOXES, LVS_EX_FULLROWSELECT, LVS_NOCOLUMNHEADER, LVS_REPORT, LVS_SINGLESEL, NMHDR,
    NMLISTVIEW,
};
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    AppendMenuW, CreatePopupMenu, DestroyMenu, DestroyWindow, GetWindowRect, SendMessageW,
    SetWindowPos, ShowWindow, TrackPopupMenu, BM_GETCHECK, BM_SETCHECK, BN_CLICKED,
    BS_AUTORADIOBUTTON, CBN_SELCHANGE, CBS_DROPDOWNLIST, CB_ADDSTRING, CB_GETCURSEL, CB_SETCURSEL,
    EN_CHANGE, ES_AUTOHSCROLL, ES_AUTOVSCROLL, ES_MULTILINE, ES_PASSWORD, ES_READONLY,
    ES_WANTRETURN, LBN_SELCHANGE, LBS_NOINTEGRALHEIGHT, LBS_NOTIFY, LB_ADDSTRING, LB_GETCURSEL,
    MF_STRING, SWP_NOZORDER, SW_SHOW, TPM_RETURNCMD, WINDOW_EX_STYLE, WINDOW_STYLE,
    WS_EX_CLIENTEDGE, WS_GROUP, WS_HSCROLL, WS_TABSTOP, WS_VSCROLL,
};

use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

/// What the window hands its owner.
pub enum Event {
    Send(Vec<A>),
    Copy(String),
    /// A tick box the view keeps (no actions of its own) changed.
    Toggle(bool),
    /// Closed with nothing to send: the owner drops what the window showed.
    Closed,
}

/// The width of a field's label, in DIPs.
const LABEL_W: i32 = 120;

/// One built part: its controls and what they do.
enum Built {
    Text(HWND),
    List(HWND),
    Block(HWND),
    Choice {
        radios: Vec<HWND>,
        send: Option<Vec<Vec<A>>>,
        /// The line under the options, and its text for each one.
        note: Option<(HWND, Vec<String>)>,
    },
    Select {
        label: HWND,
        combo: HWND,
        send: Vec<Vec<A>>,
    },
    Field {
        label: HWND,
        edit: HWND,
        id: FieldId,
        multiline: bool,
        live: bool,
    },
    Picker {
        button: HWND,
        options: Vec<(String, Vec<(FieldId, String)>)>,
    },
    Checklist {
        list: HWND,
        send: Vec<Vec<A>>,
    },
    Browse {
        list: HWND,
        detail: HWND,
        details: Vec<String>,
    },
    Toggle(HWND, Option<(Vec<A>, Vec<A>)>),
    Items {
        list: HWND,
        button: HWND,
        actions: Vec<Vec<A>>,
    },
}

pub struct ModelWindow {
    window: HWND,
    pub kind: WindowKind,
    pub key: String,
    built: Vec<Built>,
    heights: Vec<i32>,
    buttons: Vec<(HWND, Press)>,
    dismiss: Option<Vec<A>>,
    picked: Cell<usize>,
    on_event: Rc<dyn Fn(Event)>,
    sync: SyncGuard,
}

fn edit_box(parent: HWND, mono: bool) -> HWND {
    let style = ES_MULTILINE as u32
        | ES_READONLY as u32
        | ES_AUTOVSCROLL as u32
        | WS_VSCROLL.0
        | WS_HSCROLL.0
        | WS_TABSTOP.0;
    let h = ui::child(
        parent,
        w!("EDIT"),
        "",
        WINDOW_STYLE(style),
        WS_EX_CLIENTEDGE,
        "",
    );
    if mono {
        mono_font(parent, h);
    }
    h
}

fn mono_font(parent: HWND, h: HWND) {
    // SAFETY: a plain query of our own window.
    let dpi = unsafe { GetDpiForWindow(parent) }.max(96);
    ui::set_font(h, ui::mono_font_at(dpi));
}

fn list_box(parent: HWND, lines: &[String]) -> HWND {
    let style =
        WINDOW_STYLE(LBS_NOTIFY as u32 | LBS_NOINTEGRALHEIGHT as u32 | WS_VSCROLL.0 | WS_TABSTOP.0);
    let list = ui::child(parent, w!("LISTBOX"), "", style, WS_EX_CLIENTEDGE, "");
    for l in lines {
        let t = ui::wide(&ui::plain(l));
        // SAFETY: the list box copies the string.
        unsafe {
            SendMessageW(list, LB_ADDSTRING, None, Some(LPARAM(t.as_ptr() as isize)));
        }
    }
    list
}

/// About how tall a wrapped line of `text` is at `width` DIPs, from the message font's average width.
fn text_height(text: &str, width: i32) -> i32 {
    let per_line = ((width - 2 * layout::MARGIN) / 7).max(20) as usize;
    let lines: usize = text
        .split('\n')
        .map(|l| l.chars().count().div_ceil(per_line).max(1))
        .sum();
    (layout::ROW - 8) * lines as i32
}

/// A window button's id for the flow drivers: `window.` and its label, e.g. `window.apply_anyway`.
fn button_id(label: &str) -> String {
    let words: Vec<String> = label
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    format!("window.{}", words.join("_"))
}

/// A field's id for the flow drivers, e.g. `window.field.password`.
fn field_id(id: FieldId) -> String {
    format!("window.field.{}", format!("{id:?}").to_ascii_lowercase())
}

fn set_check(h: HWND, on: bool) {
    // SAFETY: a plain button message.
    unsafe {
        SendMessageW(h, BM_SETCHECK, Some(WPARAM(on as usize)), None);
    }
}

fn checked(h: HWND) -> bool {
    // SAFETY: a plain button query.
    unsafe { SendMessageW(h, BM_GETCHECK, None, None).0 == 1 }
}

fn select(combo: HWND, i: usize) {
    // SAFETY: plain combo messages; CB_SETCURSEL sends no CBN_SELCHANGE.
    unsafe {
        if SendMessageW(combo, CB_GETCURSEL, None, None).0 != i as isize {
            SendMessageW(combo, CB_SETCURSEL, Some(WPARAM(i)), None);
        }
    }
}

/// Tick (`Some(true)`), untick or hide (`None`) the box of checklist row `i`.
fn set_tick(list: HWND, i: usize, tick: Option<bool>) {
    let image: u32 = match tick {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    };
    let item = LVITEMW {
        stateMask: LVIS_STATEIMAGEMASK,
        state: LIST_VIEW_ITEM_STATE_FLAGS(image << 12),
        ..Default::default()
    };
    // SAFETY: a plain list-view message with a local item.
    unsafe {
        SendMessageW(
            list,
            LVM_SETITEMSTATE,
            Some(WPARAM(i)),
            Some(LPARAM(&item as *const _ as isize)),
        );
    }
}

fn checklist(parent: HWND, lines: &[String]) -> HWND {
    let style = LVS_REPORT | LVS_NOCOLUMNHEADER | LVS_SINGLESEL | WS_TABSTOP.0;
    let list = ui::child(
        parent,
        w!("SysListView32"),
        "",
        WINDOW_STYLE(style),
        WS_EX_CLIENTEDGE,
        "",
    );
    // SAFETY: plain list-view setup; each text outlives its insert.
    unsafe {
        let ex = LVS_EX_CHECKBOXES | LVS_EX_FULLROWSELECT;
        SendMessageW(
            list,
            LVM_SETEXTENDEDLISTVIEWSTYLE,
            Some(WPARAM(ex as usize)),
            Some(LPARAM(ex as isize)),
        );
        let col = LVCOLUMNW {
            mask: LVCF_WIDTH,
            cx: 400,
            ..Default::default()
        };
        SendMessageW(
            list,
            LVM_INSERTCOLUMNW,
            Some(WPARAM(0)),
            Some(LPARAM(&col as *const _ as isize)),
        );
        SendMessageW(list, LVM_DELETEALLITEMS, None, None);
        for (i, line) in lines.iter().enumerate() {
            let mut t = ui::wide(&ui::plain(line));
            let item = LVITEMW {
                mask: LVIF_TEXT,
                iItem: i as i32,
                pszText: PWSTR(t.as_mut_ptr()),
                ..Default::default()
            };
            SendMessageW(
                list,
                LVM_INSERTITEMW,
                None,
                Some(LPARAM(&item as *const _ as isize)),
            );
        }
        SendMessageW(
            list,
            LVM_SETCOLUMNWIDTH,
            Some(WPARAM(0)),
            Some(LPARAM(LVSCW_AUTOSIZE_USEHEADER as isize)),
        );
    }
    list
}

impl ModelWindow {
    /// Open `w` over `owner`; `at` is where an earlier build of the same window stood.
    pub fn open(
        owner: HWND,
        w: &PaneWindow,
        at: Option<RECT>,
        on_event: Rc<dyn Fn(Event)>,
    ) -> Rc<Self> {
        let (width, height) = w.size;
        let window = panel::dialog(owner, &w.title, width, height.max(120));
        let mut built = Vec::new();
        let mut heights = Vec::new();
        let mut picked = 0;
        let mut ticks = Vec::new();
        for part in &w.parts {
            let (b, h) = match part {
                Part::Text(t, tone) => {
                    let h = ui::label(window, t);
                    crate::theme::set_tone(h, (*tone != Tone::Plain).then_some(*tone));
                    (Built::Text(h), text_height(t, width))
                }
                Part::List(lines) => {
                    let h = edit_box(window, false);
                    ui::set_text(h, &lines.join("\r\n"));
                    let rows = lines.len().clamp(2, 8) as i32;
                    (Built::List(h), rows * 16 + 10)
                }
                Part::Block(text) => {
                    let h = edit_box(window, true);
                    ui::set_text(h, &text.replace('\n', "\r\n"));
                    (Built::Block(h), -1)
                }
                Part::Choice {
                    options,
                    picked: p,
                    send,
                    notes,
                } => {
                    picked = *p;
                    let radios: Vec<HWND> = options
                        .iter()
                        .enumerate()
                        .map(|(i, label)| {
                            let group = if i == 0 { WS_GROUP.0 } else { 0 };
                            let style =
                                WINDOW_STYLE(BS_AUTORADIOBUTTON as u32 | WS_TABSTOP.0 | group);
                            ui::child(window, w!("BUTTON"), label, style, WINDOW_EX_STYLE(0), "")
                        })
                        .collect();
                    if let Some(r) = radios.get(*p) {
                        set_check(*r, true);
                    }
                    let rows = match send {
                        Some(_) => options.len() as i32 * layout::ROW,
                        None => layout::ROW,
                    };
                    let send = send.clone();
                    let note = (!notes.is_empty()).then(|| {
                        let h = ui::label(window, notes.get(*p).map_or("", |n| n.as_str()));
                        crate::theme::set_tone(h, Some(Tone::Warning));
                        (h, notes.clone())
                    });
                    let rows = rows + note.as_ref().map_or(0, |_| layout::ROW - 8);
                    (Built::Choice { radios, send, note }, rows)
                }
                Part::Select {
                    label,
                    options,
                    picked,
                    send,
                } => {
                    let caption = ui::cell_label(window, label);
                    let style =
                        WINDOW_STYLE((CBS_DROPDOWNLIST as u32) | WS_TABSTOP.0 | WS_VSCROLL.0);
                    let combo =
                        ui::child(window, w!("COMBOBOX"), "", style, WINDOW_EX_STYLE(0), "");
                    for o in options {
                        let t = ui::wide(o);
                        // SAFETY: the combo copies the string.
                        unsafe {
                            SendMessageW(
                                combo,
                                CB_ADDSTRING,
                                None,
                                Some(LPARAM(t.as_ptr() as isize)),
                            );
                        }
                    }
                    select(combo, *picked);
                    let built = Built::Select {
                        label: caption,
                        combo,
                        send: send.clone(),
                    };
                    (built, layout::ROW)
                }
                Part::Field {
                    id,
                    label,
                    value,
                    hint: _,
                    secret,
                    multiline,
                    live,
                } => {
                    let caption = ui::cell_label(window, label);
                    let mut style = WS_TABSTOP.0;
                    if *multiline {
                        style |= ES_MULTILINE as u32
                            | ES_WANTRETURN as u32
                            | ES_AUTOVSCROLL as u32
                            | WS_VSCROLL.0
                            | WS_HSCROLL.0;
                    } else {
                        style |= ES_AUTOHSCROLL as u32;
                    }
                    if *secret {
                        style |= ES_PASSWORD as u32;
                    }
                    let edit = ui::child(
                        window,
                        w!("EDIT"),
                        "",
                        WINDOW_STYLE(style),
                        WS_EX_CLIENTEDGE,
                        &field_id(*id),
                    );
                    if *multiline {
                        mono_font(window, edit);
                        // SAFETY: a plain edit message; the text grows past the 32K default.
                        unsafe {
                            SendMessageW(
                                edit,
                                windows::Win32::UI::Controls::EM_SETLIMITTEXT,
                                Some(WPARAM(0)),
                                None,
                            );
                        }
                    }
                    ui::set_text(edit, &value.replace('\n', "\r\n"));
                    let built = Built::Field {
                        label: caption,
                        edit,
                        id: *id,
                        multiline: *multiline,
                        live: *live,
                    };
                    (built, if *multiline { -1 } else { layout::ROW })
                }
                Part::Picker { label, options } => {
                    let button = ui::button(window, label, "window.picker");
                    let built = Built::Picker {
                        button,
                        options: options.clone(),
                    };
                    (built, layout::ROW)
                }
                Part::Checklist {
                    lines,
                    ticked,
                    send,
                } => {
                    let list = checklist(window, lines);
                    ticks.push((list, ticked.clone()));
                    let built = Built::Checklist {
                        list,
                        send: send.clone(),
                    };
                    (built, -1)
                }
                Part::Browse { lines, details } => {
                    let list = list_box(window, lines);
                    let detail = edit_box(window, true);
                    let built = Built::Browse {
                        list,
                        detail,
                        details: details.clone(),
                    };
                    (built, -1)
                }
                Part::Toggle { label, on, send } => {
                    let h = ui::checkbox(window, label, "");
                    set_check(h, *on);
                    (Built::Toggle(h, send.clone()), layout::ROW)
                }
                Part::Items {
                    lines,
                    button,
                    actions,
                    enabled,
                } => {
                    let list = list_box(window, lines);
                    let b = ui::button(window, button, "");
                    ui::set_enabled(b, false);
                    let built = Built::Items {
                        list,
                        button: b,
                        actions: if *enabled {
                            actions.clone()
                        } else {
                            Vec::new()
                        },
                    };
                    (built, -1)
                }
            };
            built.push(b);
            heights.push(h);
        }
        let mut buttons = Vec::new();
        for b in &w.buttons {
            let h = ui::button(window, &b.label, &button_id(&b.label));
            ui::set_enabled(h, b.enabled);
            buttons.push((h, b.press.clone()));
        }
        if w.buttons.is_empty() {
            let close = ui::button(window, "Close", "window.close");
            let press = match &w.dismiss {
                Some(a) => Press::Send(a.clone()),
                None => Press::Send(Vec::new()),
            };
            buttons.push((close, press));
        }
        let me = Rc::new(ModelWindow {
            window,
            kind: w.kind,
            key: w.key.clone(),
            built,
            heights,
            buttons,
            dismiss: w.dismiss.clone(),
            picked: Cell::new(picked),
            on_event,
            sync: SyncGuard::default(),
        });
        let weak: Weak<dyn Handler> = Rc::downgrade(&me) as Weak<dyn Handler>;
        panel::register(window, weak);
        me.sync.run(|| {
            for (list, ticked) in &ticks {
                for (i, t) in ticked.iter().enumerate() {
                    set_tick(*list, i, *t);
                }
            }
        });
        // SAFETY: placing and showing the window we created.
        unsafe {
            let dpi = GetDpiForWindow(window).max(96);
            if let Some(r) = at {
                let _ = SetWindowPos(
                    window,
                    None,
                    r.left,
                    r.top,
                    r.right - r.left,
                    r.bottom - r.top,
                    SWP_NOZORDER,
                );
            } else if height == 0 {
                let mut r = RECT::default();
                let _ = GetWindowRect(window, &mut r);
                let content: i32 = me
                    .heights
                    .iter()
                    .map(|h| if *h < 0 { 160 } else { *h })
                    .sum();
                let rows = me.heights.len() as i32 + 1;
                let dips = content + rows * layout::GAP + layout::ROW + 2 * layout::MARGIN + 40;
                let _ = SetWindowPos(
                    window,
                    None,
                    r.left,
                    r.top,
                    r.right - r.left,
                    layout::scale(dips, dpi),
                    SWP_NOZORDER,
                );
            }
            let _ = ShowWindow(window, SW_SHOW);
        }
        me.layout();
        me
    }

    /// Take `w`'s new lines, choices, ticks and button states; fields keep what is typed in them.
    pub fn update(&self, w: &PaneWindow) {
        self.sync.run(|| {
            for (b, part) in self.built.iter().zip(&w.parts) {
                match (b, part) {
                    (Built::Text(h), Part::Text(t, tone)) => {
                        ui::set_text(*h, t);
                        crate::theme::set_tone(*h, (*tone != Tone::Plain).then_some(*tone));
                    }
                    (Built::Choice { radios, note, .. }, Part::Choice { picked, .. }) => {
                        for (i, r) in radios.iter().enumerate() {
                            set_check(*r, i == *picked);
                        }
                        self.picked.set(*picked);
                        if let Some((h, notes)) = note {
                            ui::set_text(*h, notes.get(*picked).map_or("", |n| n.as_str()));
                        }
                    }
                    (Built::Select { combo, .. }, Part::Select { picked, .. }) => {
                        select(*combo, *picked);
                    }
                    (Built::Checklist { list, .. }, Part::Checklist { ticked, .. }) => {
                        for (i, t) in ticked.iter().enumerate() {
                            set_tick(*list, i, *t);
                        }
                    }
                    _ => {}
                }
            }
            for ((h, press), b) in self.buttons.iter().zip(&w.buttons) {
                let _ = press;
                ui::set_text(*h, &b.label);
                ui::set_enabled(*h, b.enabled);
            }
        });
    }

    pub fn hwnd(&self) -> HWND {
        self.window
    }

    /// Where the window stands, for a rebuild to open in the same place.
    pub fn rect(&self) -> RECT {
        let mut r = RECT::default();
        // SAFETY: a plain query of our own window.
        let _ = unsafe { GetWindowRect(self.window, &mut r) };
        r
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let mut col = Vec::new();
        let pair = |label: HWND, field: HWND, height: Size| {
            Node::row(vec![
                Node::leaf(label, Size::Fixed(LABEL_W), Size::Fixed(layout::ROW)),
                Node::leaf(field, Size::Stretch(1), height),
            ])
        };
        for (b, h) in self.built.iter().zip(&self.heights) {
            let height = match *h {
                -1 => Size::Stretch(1),
                h => Size::Fixed(h),
            };
            match b {
                Built::Text(c) | Built::List(c) | Built::Block(c) => {
                    col.push(Node::leaf(*c, Size::Stretch(1), height));
                }
                Built::Toggle(c, _) => col.push(Node::leaf(*c, Size::Stretch(1), height)),
                Built::Choice {
                    radios, send, note, ..
                } => {
                    let radio =
                        |r: &HWND| Node::leaf(*r, Size::Fixed(200), Size::Fixed(layout::ROW));
                    match send {
                        // Options that act at once stack, each on its own line.
                        Some(_) => col.extend(radios.iter().map(radio)),
                        None => col.push(Node::row(radios.iter().map(radio).collect())),
                    }
                    if let Some((h, _)) = note {
                        col.push(Node::leaf(
                            *h,
                            Size::Stretch(1),
                            Size::Fixed(layout::ROW - 8),
                        ));
                    }
                }
                Built::Select { label, combo, .. } => {
                    col.push(pair(*label, *combo, Size::Fixed(layout::ROW)));
                }
                Built::Field {
                    label,
                    edit,
                    multiline,
                    ..
                } => {
                    if *multiline && ui::text(*label).is_empty() {
                        col.push(Node::leaf(*edit, Size::Stretch(1), height));
                    } else {
                        col.push(pair(*label, *edit, Size::Fixed(layout::ROW)));
                    }
                }
                Built::Picker { button, .. } => col.push(Node::row(vec![
                    Node::button(*button, 160),
                    Node::Space(Size::Stretch(1)),
                ])),
                Built::Checklist { list, .. } => {
                    col.push(Node::leaf(*list, Size::Stretch(1), height));
                }
                Built::Browse { list, detail, .. } => {
                    col.push(Node::leaf(*list, Size::Stretch(1), Size::Stretch(1)));
                    col.push(Node::leaf(*detail, Size::Stretch(1), Size::Stretch(1)));
                }
                Built::Items { list, button, .. } => {
                    col.push(Node::leaf(*list, Size::Stretch(1), height));
                    col.push(Node::row(vec![
                        Node::Space(Size::Stretch(1)),
                        Node::button(*button, 90),
                    ]));
                }
            }
        }
        let mut row = vec![Node::Space(Size::Stretch(1))];
        for (h, _) in &self.buttons {
            let label = ui::text(*h);
            let w = (label.chars().count() as i32 * 7 + 30).max(80);
            row.push(Node::button(*h, w));
        }
        col.push(Node::row(row));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (h, r) in layout::layout(&tree, area, dpi) {
            ui::move_to(h, r);
        }
    }

    fn event(&self, e: Event) {
        (self.on_event)(e);
    }

    /// What the fields hold now, for `submit`.
    fn values(&self) -> Vec<(FieldId, String)> {
        self.built
            .iter()
            .filter_map(|b| match b {
                Built::Field { edit, id, .. } => Some((*id, ui::text(*edit).replace("\r\n", "\n"))),
                _ => None,
            })
            .collect()
    }

    /// The picker's menu under its button; the option picked fills its fields.
    fn pick(&self, button: HWND, options: &[(String, Vec<(FieldId, String)>)]) {
        let mut r = RECT::default();
        // SAFETY: a menu we create, track and destroy here; each label outlives its append.
        let picked = unsafe {
            let _ = GetWindowRect(button, &mut r);
            let Ok(menu) = CreatePopupMenu() else {
                return;
            };
            for (i, (label, _)) in options.iter().enumerate() {
                let t = ui::wide(&ui::plain(label));
                let _ = AppendMenuW(menu, MF_STRING, i + 1, PCWSTR(t.as_ptr()));
            }
            let picked = TrackPopupMenu(
                menu,
                TPM_RETURNCMD,
                r.left,
                r.bottom,
                None,
                self.window,
                None,
            );
            let _ = DestroyMenu(menu);
            picked.0
        };
        let Some((_, set)) = usize::try_from(picked)
            .ok()
            .and_then(|i| i.checked_sub(1))
            .and_then(|i| options.get(i))
        else {
            return;
        };
        for b in &self.built {
            if let Built::Field { edit, id, .. } = b {
                if let Some((_, v)) = set.iter().find(|(f, _)| f == id) {
                    ui::set_text(*edit, v);
                }
            }
        }
    }
}

impl Drop for ModelWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        for b in &self.built {
            match b {
                Built::Text(h) => crate::theme::set_tone(*h, None),
                Built::Choice {
                    note: Some((h, _)), ..
                } => crate::theme::set_tone(*h, None),
                _ => {}
            }
        }
        // SAFETY: the owner dropped the window; it goes with its handler.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}

impl Handler for ModelWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() {
            return;
        }
        for b in &self.built {
            match b {
                Built::Choice { radios, send, note } if code == BN_CLICKED => {
                    if let Some(i) = radios.iter().position(|r| *r == from) {
                        self.picked.set(i);
                        if let Some((h, notes)) = note {
                            ui::set_text(*h, notes.get(i).map_or("", |n| n.as_str()));
                        }
                        if let Some(a) = send.as_ref().and_then(|s| s.get(i)) {
                            self.event(Event::Send(a.clone()));
                        }
                        return;
                    }
                }
                Built::Select { combo, send, .. } if *combo == from && code == CBN_SELCHANGE => {
                    // SAFETY: a plain combo query.
                    let i = unsafe { SendMessageW(*combo, CB_GETCURSEL, None, None).0 };
                    if let Some(a) = usize::try_from(i).ok().and_then(|i| send.get(i)) {
                        self.event(Event::Send(a.clone()));
                    }
                    return;
                }
                Built::Field { edit, id, live, .. } if *edit == from && code == EN_CHANGE => {
                    if *live {
                        if let Some(a) = field_action(*id, &ui::text(*edit)) {
                            self.event(Event::Send(vec![a]));
                        }
                    }
                    return;
                }
                Built::Picker { button, options } if *button == from && code == BN_CLICKED => {
                    self.pick(*button, options);
                    return;
                }
                Built::Browse {
                    list,
                    detail,
                    details,
                } if *list == from && code == LBN_SELCHANGE => {
                    // SAFETY: a plain list-box query.
                    let i = unsafe { SendMessageW(*list, LB_GETCURSEL, None, None).0 };
                    let text = usize::try_from(i).ok().and_then(|i| details.get(i));
                    ui::set_text(
                        *detail,
                        &text.map_or(String::new(), |t| t.replace('\n', "\r\n")),
                    );
                    return;
                }
                Built::Toggle(h, send) if *h == from && code == BN_CLICKED => {
                    let on = checked(*h);
                    match send {
                        Some((yes, no)) => {
                            self.event(Event::Send(if on { yes } else { no }.clone()))
                        }
                        None => self.event(Event::Toggle(on)),
                    }
                    return;
                }
                Built::Items {
                    list,
                    button,
                    actions,
                } => {
                    // SAFETY: a plain list-box query.
                    let at = unsafe { SendMessageW(*list, LB_GETCURSEL, None, None).0 };
                    let chosen = usize::try_from(at).ok().and_then(|i| actions.get(i));
                    if from == *list && code == LBN_SELCHANGE {
                        ui::set_enabled(*button, chosen.is_some_and(|a| !a.is_empty()));
                        return;
                    }
                    if from == *button && code == BN_CLICKED {
                        if let Some(a) = chosen {
                            self.event(Event::Send(a.clone()));
                        }
                        return;
                    }
                }
                _ => {}
            }
        }
        if code != BN_CLICKED {
            return;
        }
        let press = self
            .buttons
            .iter()
            .find(|(h, _)| *h == from)
            .map(|(_, p)| p.clone());
        match press {
            Some(Press::Send(a)) if a.is_empty() => self.event(Event::Closed),
            Some(Press::Send(a)) => self.event(Event::Send(a)),
            Some(Press::PerChoice(each)) => {
                if let Some(a) = each.get(self.picked.get()) {
                    self.event(Event::Send(a.clone()));
                }
            }
            Some(Press::Copy(text)) => self.event(Event::Copy(text)),
            Some(Press::Submit(form)) => {
                let actions = submit(&form, &self.values(), self.picked.get());
                self.event(Event::Send(actions));
            }
            None => {}
        }
    }

    fn notify(&self, hdr: &NMHDR, lp: LPARAM) -> Option<LRESULT> {
        if hdr.code != LVN_ITEMCHANGED || self.sync.active() {
            return None;
        }
        // SAFETY: LVN_ITEMCHANGED comes with an NMLISTVIEW.
        let nm = unsafe { &*(lp.0 as *const NMLISTVIEW) };
        let image = |s: u32| (s & LVIS_STATEIMAGEMASK.0) >> 12;
        let flipped = nm.uChanged.0 & LVIF_STATE.0 != 0
            && image(nm.uOldState) != 0
            && image(nm.uNewState) != image(nm.uOldState);
        if !flipped {
            return None;
        }
        for b in &self.built {
            if let Built::Checklist { list, send } = b {
                if *list == hdr.hwndFrom {
                    if let Some(a) = usize::try_from(nm.iItem).ok().and_then(|i| send.get(i)) {
                        self.event(Event::Send(a.clone()));
                    }
                }
            }
        }
        Some(LRESULT(0))
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {
        match &self.dismiss {
            Some(a) => self.event(Event::Send(a.clone())),
            None => self.event(Event::Closed),
        }
    }
}
