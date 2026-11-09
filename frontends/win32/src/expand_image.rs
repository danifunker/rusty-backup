//! Expand Image: grow an image file with zero-padding, so new partitions fit (`gui/inspect_tab.rs`).
//!
//! The view holds the window, as the egui and GTK shells do: the controller only previews the
//! sizes and runs `InspectAction::ExpandImage` once the user presses Expand.

use std::cell::Cell;
use std::rc::{Rc, Weak};

use rusty_backup::controller::inspect::{
    expand_image_bytes, format_size, ids, EXPAND_IMAGE_MIB_MAX,
};
use windows::core::w;
use windows::Win32::Foundation::{HWND, LPARAM, LRESULT};
use windows::Win32::UI::Controls::NMHDR;
use windows::Win32::UI::WindowsAndMessaging::{
    DestroyWindow, ShowWindow, BN_CLICKED, EN_CHANGE, ES_NUMBER, SW_SHOW, WINDOW_STYLE,
    WS_EX_CLIENTEDGE, WS_TABSTOP,
};

use crate::layout::{self, Node, Size};
use crate::panel::{self, Handler};
use crate::ui;

/// What the window asks of its owner.
pub enum Event {
    /// The MiB to add changed: show the sizes for it.
    Preview(u32),
    /// Expand by this many bytes, then close.
    Expand(u64),
    Close,
}

pub struct ExpandImageWindow {
    window: HWND,
    current: HWND,
    caption: HWND,
    mib: HWND,
    unit: HWND,
    new: HWND,
    note: HWND,
    start: HWND,
    cancel: HWND,
    added: Cell<u32>,
    on_event: Box<dyn Fn(Event)>,
    me: Weak<ExpandImageWindow>,
}

impl ExpandImageWindow {
    pub fn open(owner: HWND, on_event: Box<dyn Fn(Event)>) -> Rc<Self> {
        let window = panel::dialog(owner, "Expand Image", 440, 240);
        let current = ui::label(window, "");
        let caption = ui::label(window, "Add free space:");
        let style = WINDOW_STYLE((ES_NUMBER as u32) | WS_TABSTOP.0);
        let mib = ui::child(
            window,
            w!("EDIT"),
            "0",
            style,
            WS_EX_CLIENTEDGE,
            ids::EXPAND_IMAGE_ADD,
        );
        let unit = ui::label(window, "MiB");
        let new = ui::label(window, "");
        let note = ui::label(
            window,
            "After expansion, click Re-inspect to refresh the partition list, then use 'Add \
             Partition...' or 'Edit Partition Table...' to allocate the new space.",
        );
        let start = ui::button(window, "Expand", ids::EXPAND_IMAGE_START);
        let cancel = ui::button(window, "Cancel", "");
        let me = Rc::new_cyclic(|me: &Weak<ExpandImageWindow>| ExpandImageWindow {
            window,
            current,
            caption,
            mib,
            unit,
            new,
            note,
            start,
            cancel,
            added: Cell::new(0),
            on_event,
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

    /// The MiB the user asked to add.
    pub fn added(&self) -> u32 {
        self.added.get()
    }

    /// Show the controller's preview: the image's size now and after.
    pub fn show(&self, current: u64, new: u64) {
        ui::set_text(
            self.current,
            &format!("Current size: {}", format_size(current)),
        );
        ui::set_text(self.new, &format!("New size: {}", format_size(new)));
        ui::set_enabled(self.start, self.added.get() > 0);
    }

    fn layout(&self) {
        let (area, dpi) = panel::client(self.window);
        let line =
            |h: HWND, n: i32| Node::leaf(h, Size::Stretch(1), Size::Fixed((layout::ROW - 8) * n));
        let tree = Node::inset(
            Node::column(vec![
                line(self.current, 1),
                Node::row(vec![
                    Node::leaf(self.caption, Size::Fixed(110), Size::Fixed(layout::ROW - 8)),
                    Node::leaf(self.mib, Size::Fixed(90), Size::Fixed(layout::ROW)),
                    Node::leaf(self.unit, Size::Fixed(40), Size::Fixed(layout::ROW - 8)),
                ]),
                line(self.new, 1),
                line(self.note, 3),
                Node::row(vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(self.start, 90),
                    Node::button(self.cancel, 90),
                ]),
            ]),
            layout::MARGIN,
        );
        for (h, r) in layout::layout(&tree, area, dpi) {
            ui::move_to(h, r);
        }
    }
}

impl Drop for ExpandImageWindow {
    fn drop(&mut self) {
        panel::unregister(self.window);
        // SAFETY: the owner dropped the window; it goes with its handler.
        let _ = unsafe { DestroyWindow(self.window) };
    }
}

impl Handler for ExpandImageWindow {
    fn command(&self, from: HWND, code: u32) {
        if code == EN_CHANGE && from == self.mib {
            let mib = ui::text(self.mib)
                .trim()
                .parse::<u32>()
                .unwrap_or(0)
                .min(EXPAND_IMAGE_MIB_MAX);
            self.added.set(mib);
            (self.on_event)(Event::Preview(mib));
        } else if code == BN_CLICKED && from == self.start {
            let add = expand_image_bytes(self.added.get());
            if add > 0 {
                (self.on_event)(Event::Expand(add));
            }
        } else if code == BN_CLICKED && from == self.cancel {
            (self.on_event)(Event::Close);
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
