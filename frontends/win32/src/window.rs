//! A window the controller describes (`commander::PaneWindow`): its parts top to bottom and its
//! buttons along the bottom, each sending what it carries.
//!
//! The owner keeps one per kind and builds it again when the controller's key for it changes;
//! the window remembers only which option of a choice is picked and the item a list has selected.

use std::cell::Cell;
use std::rc::{Rc, Weak};

use rusty_backup::controller::commander::{
    CommanderAction as A, PaneWindow, Part, Press, Tone, WindowKind,
};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::HiDpi::GetDpiForWindow;
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, GetWindowRect, SendMessageW, SetWindowPos, ShowWindow, BM_GETCHECK, BM_SETCHECK,
    BN_CLICKED, BS_AUTORADIOBUTTON, ES_AUTOVSCROLL, ES_MULTILINE, ES_READONLY, LBN_SELCHANGE,
    LBS_NOINTEGRALHEIGHT, LBS_NOTIFY, LB_ADDSTRING, LB_GETCURSEL, SWP_NOZORDER, SW_SHOW,
    WINDOW_EX_STYLE, WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_GROUP, WS_HSCROLL, WS_TABSTOP, WS_VSCROLL,
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

/// One built part: its controls and what they do.
enum Built {
    Text(HWND),
    List(HWND),
    Block(HWND),
    Choice(Vec<HWND>),
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
        // SAFETY: a plain query of our own window.
        let dpi = unsafe { GetDpiForWindow(parent) }.max(96);
        ui::set_font(h, ui::mono_font_at(dpi));
    }
    h
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
                Part::Choice { options, picked: p } => {
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
                    (Built::Choice(radios), layout::ROW)
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
                    let style = WINDOW_STYLE(
                        LBS_NOTIFY as u32
                            | LBS_NOINTEGRALHEIGHT as u32
                            | WS_VSCROLL.0
                            | WS_TABSTOP.0,
                    );
                    let list = ui::child(window, w!("LISTBOX"), "", style, WS_EX_CLIENTEDGE, "");
                    for l in lines {
                        let t = ui::wide(&ui::plain(l));
                        // SAFETY: the list box copies the string.
                        unsafe {
                            SendMessageW(
                                list,
                                LB_ADDSTRING,
                                None,
                                Some(LPARAM(t.as_ptr() as isize)),
                            );
                        }
                    }
                    let b = ui::button(window, button, "");
                    ui::set_enabled(b, false);
                    let _ = enabled;
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
                Built::Choice(radios) => col.push(Node::row(
                    radios
                        .iter()
                        .map(|r| Node::leaf(*r, Size::Fixed(150), Size::Fixed(layout::ROW)))
                        .collect(),
                )),
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
}

impl Drop for ModelWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        for b in &self.built {
            if let Built::Text(h) = b {
                crate::theme::set_tone(*h, None);
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
                Built::Choice(radios) if code == BN_CLICKED => {
                    if let Some(i) = radios.iter().position(|r| *r == from) {
                        self.picked.set(i);
                        return;
                    }
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
                        ui::set_enabled(*button, chosen.is_some());
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
            None => {}
        }
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
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
