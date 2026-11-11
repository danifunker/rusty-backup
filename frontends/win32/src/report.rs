//! A read-only text window a controller holds open: Check results, CHD Info.
//!
//! The owner rebuilds nothing: it hands the window its text on every refresh, and the window
//! sends the owner's dismiss when the user closes it. An optional tick box and button cover
//! "Show debug messages" and "Repair".

use std::rc::{Rc, Weak};

use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, SendMessageW, ShowWindow, BM_GETCHECK, BM_SETCHECK, BN_CLICKED, ES_AUTOVSCROLL,
    ES_MULTILINE, ES_READONLY, SW_SHOW, WINDOW_STYLE, WS_EX_CLIENTEDGE, WS_TABSTOP, WS_VSCROLL,
};

use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::sync::SyncGuard;
use crate::ui;

/// What the window's controls send back to its owner.
pub enum Event {
    Close,
    Toggle(bool),
    Button,
}

pub struct ReportWindow {
    window: HWND,
    headline: HWND,
    stats: HWND,
    text: HWND,
    toggle: Option<HWND>,
    button: Option<HWND>,
    close: HWND,
    on_event: Box<dyn Fn(Event)>,
    sync: SyncGuard,
    me: Weak<ReportWindow>,
}

/// What a report shows now.
pub struct Shown {
    pub headline: String,
    pub stats: String,
    pub lines: Vec<String>,
    pub toggle: Option<bool>,
    pub button: bool,
}

impl ReportWindow {
    /// `toggle` and `button` are `(label, id)` for the optional tick box and action button.
    pub fn open(
        owner: HWND,
        title: &str,
        toggle: Option<(&str, &str)>,
        button: Option<(&str, &str)>,
        on_event: Box<dyn Fn(Event)>,
    ) -> Rc<Self> {
        let window = panel::dialog(owner, title, 560, 440);
        let headline = ui::label(window, "");
        let stats = ui::label(window, "");
        let style = ES_MULTILINE as u32
            | ES_READONLY as u32
            | ES_AUTOVSCROLL as u32
            | WS_VSCROLL.0
            | WS_TABSTOP.0;
        let text = ui::child(
            window,
            w!("EDIT"),
            "",
            WINDOW_STYLE(style),
            WS_EX_CLIENTEDGE,
            "",
        );
        let toggle = toggle.map(|(label, id)| ui::checkbox(window, label, id));
        let button = button.map(|(label, id)| ui::button(window, label, id));
        let close = ui::button(window, "Close", "");
        let me = Rc::new_cyclic(|me: &Weak<ReportWindow>| ReportWindow {
            window,
            headline,
            stats,
            text,
            toggle,
            button,
            close,
            on_event,
            sync: SyncGuard::default(),
            me: me.clone(),
        });
        let weak: Weak<dyn Handler> = me.me.clone();
        panel::register(window, weak);
        me.layout();
        // SAFETY: showing the window we created.
        unsafe {
            let _ = ShowWindow(window, SW_SHOW);
        }
        me
    }

    pub fn show(&self, s: &Shown) {
        self.sync.run(|| {
            ui::set_text(self.headline, &s.headline);
            ui::set_text(self.stats, &s.stats);
            ui::set_text(self.text, &ui::plain(&s.lines.join("\r\n")));
            if let Some(t) = self.toggle {
                ui::set_visible(t, s.toggle.is_some());
                // SAFETY: a plain button message.
                unsafe {
                    SendMessageW(
                        t,
                        BM_SETCHECK,
                        Some(WPARAM((s.toggle == Some(true)) as usize)),
                        None,
                    );
                }
            }
            if let Some(b) = self.button {
                ui::set_visible(b, s.button);
            }
        });
        // The stats line's height follows its text.
        self.layout();
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let line = |h: HWND| Node::leaf(h, Size::Stretch(1), Size::Fixed(layout::ROW - 8));
        let mut bottom = Vec::new();
        if let Some(t) = self.toggle {
            bottom.push(Node::leaf(t, Size::Fixed(200), Size::Fixed(layout::ROW)));
        }
        if let Some(b) = self.button {
            bottom.push(Node::button(b, 90));
        }
        bottom.push(Node::Space(Size::Stretch(1)));
        bottom.push(Node::button(self.close, 90));
        // The stats wrap rather than run off the edge; a tenth extra allows for word breaks.
        let avail = (area.w * 96 / dpi as i32 - 2 * layout::MARGIN).max(1);
        let rows = ((ui::text_dips(self.stats) * 11 / 10 + avail - 1) / avail).max(1);
        let stats = Node::leaf(
            self.stats,
            Size::Stretch(1),
            Size::Fixed((layout::ROW - 8) * rows),
        );
        let tree = Node::inset(
            Node::column(vec![
                line(self.headline),
                stats,
                Node::leaf(self.text, Size::Stretch(1), Size::Stretch(1)),
                Node::row(bottom),
            ]),
            layout::MARGIN,
        );
        for (h, r) in layout::layout(&tree, area, dpi) {
            ui::move_to(h, r);
        }
    }
}

impl Drop for ReportWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        // SAFETY: the owner dropped the window; it goes with its handler.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}

impl Handler for ReportWindow {
    fn command(&self, from: HWND, code: u32) {
        if self.sync.active() || code != BN_CLICKED {
            return;
        }
        if from == self.close {
            (self.on_event)(Event::Close);
        } else if Some(from) == self.button {
            (self.on_event)(Event::Button);
        } else if Some(from) == self.toggle {
            // SAFETY: a plain button query.
            let on = unsafe { SendMessageW(from, BM_GETCHECK, None, None).0 } == 1;
            (self.on_event)(Event::Toggle(on));
        }
    }

    fn notify(&self, _hdr: &NMHDR, _lp: LPARAM) -> Option<LRESULT> {
        None
    }

    fn resized(&self) {
        self.layout();
    }

    fn close(&self) {
        (self.on_event)(Event::Close);
    }
}
