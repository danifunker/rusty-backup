//! Expand Image: grow an image file with zero-padding, so new partitions fit; the Win32 shell's
//! `expand_image.rs` is the reference. The view holds the window: the controller only previews
//! the sizes and runs `InspectAction::ExpandImage` once the user presses Expand.

use std::cell::Cell;
use std::rc::{Rc, Weak};

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::NSWindow;
use rusty_backup::controller::inspect::{
    expand_image_bytes, format_size, ids, EXPAND_IMAGE_MIB_MAX,
};

use crate::form::Form;
use crate::layout::{self, Node, Size};
use crate::ui::{self, CloseDelegate};

/// What the window asks of its owner.
pub enum Event {
    /// The MiB to add changed: show the sizes for it.
    Preview(u32),
    /// Expand by this many bytes, then close.
    Expand(u64),
    Close,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Current,
    Caption,
    Mib,
    Unit,
    New,
    Note,
    Start,
    Cancel,
}

pub struct ExpandImageWindow {
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    form: Form<K>,
    added: Cell<u32>,
    on_event: Rc<dyn Fn(Event)>,
}

impl ExpandImageWindow {
    pub fn open(mtm: MainThreadMarker, on_event: Rc<dyn Fn(Event)>) -> Rc<ExpandImageWindow> {
        let me = Rc::new_cyclic(|me: &Weak<ExpandImageWindow>| {
            let close = on_event.clone();
            let (window, delegate) = ui::window(
                mtm,
                "Expand Image",
                440.0,
                240.0,
                Box::new(move || {
                    let close = close.clone();
                    crate::app::later(Box::new(move || close(Event::Close)));
                }),
            );
            let body = window.contentView().expect("the window's content view");
            let fire = me.clone();
            let form = Form::new(
                mtm,
                body,
                Rc::new(move |k| {
                    if let Some(w) = fire.upgrade() {
                        w.fired(k);
                    }
                }),
            );
            form.label(K::Current, "");
            form.label(K::Caption, "Add free space:");
            form.field(K::Mib, ids::EXPAND_IMAGE_ADD, "Add free space (MiB)", false);
            form.set_text(K::Mib, "0");
            form.label(K::Unit, "MiB");
            form.label(K::New, "");
            form.wrap(
                K::Note,
                "After expansion, click Re-inspect to refresh the partition list, then use 'Add \
                 Partition...' or 'Edit Partition Table...' to allocate the new space.",
            );
            form.button(K::Start, "Expand", ids::EXPAND_IMAGE_START);
            form.button(K::Cancel, "Cancel", "");
            window.makeKeyAndOrderFront(None);
            ExpandImageWindow {
                window,
                _delegate: delegate,
                form,
                added: Cell::new(0),
                on_event,
            }
        });
        me.layout();
        me
    }

    /// Show the controller's preview: the image's size now and after.
    pub fn show(&self, current: u64, new: u64) {
        let f = &self.form;
        f.set_text(
            K::Current,
            &format!("Current size: {}", format_size(current)),
        );
        f.set_text(K::New, &format!("New size: {}", format_size(new)));
        f.enable(K::Start, self.added.get() > 0);
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let line =
            |k: K, n: i32| Node::leaf(k, Size::Stretch(1), Size::Fixed((layout::ROW - 6) * n + 4));
        let tree = Node::inset(
            Node::column(vec![
                line(K::Current, 1),
                Node::row(vec![
                    Node::leaf(K::Caption, Size::Fixed(110), Size::Fixed(layout::ROW - 4)),
                    Node::leaf(K::Mib, Size::Fixed(90), Size::Fixed(layout::ROW)),
                    Node::leaf(K::Unit, Size::Fixed(40), Size::Fixed(layout::ROW - 4)),
                ]),
                line(K::New, 1),
                line(K::Note, 3),
                Node::row(vec![
                    Node::Space(Size::Stretch(1)),
                    Node::button(K::Start, 90),
                    Node::button(K::Cancel, 90),
                ]),
            ]),
            layout::MARGIN,
        );
        self.form.place(&tree, ui::area(&body));
    }

    fn fired(&self, k: K) {
        let event = match k {
            K::Mib => {
                let mib = self
                    .form
                    .text(k)
                    .trim()
                    .parse::<u32>()
                    .unwrap_or(0)
                    .min(EXPAND_IMAGE_MIB_MAX);
                self.added.set(mib);
                Event::Preview(mib)
            }
            K::Start => match expand_image_bytes(self.added.get()) {
                0 => return,
                add => Event::Expand(add),
            },
            K::Cancel => Event::Close,
            _ => return,
        };
        // The owner may close this window: act once the handler returns.
        let on = self.on_event.clone();
        crate::app::later(Box::new(move || on(event)));
    }
}

impl Drop for ExpandImageWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
