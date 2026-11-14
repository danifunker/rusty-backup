//! A read-only text window a controller holds open: Check results, CHD Info.
//!
//! The owner rebuilds nothing: it hands the window its text on every refresh, and the window
//! sends the owner's dismiss when the user closes it. An optional tick box and button cover
//! "Show debug messages" and "Repair"; their actions go by controller id like any control.

use objc2::rc::Retained;
use objc2::MainThreadMarker;
use objc2_app_kit::{
    NSBorderType, NSButton, NSControlStateValueOff, NSControlStateValueOn, NSScrollView,
    NSTextField, NSTextView, NSWindow,
};
use objc2_foundation::{NSPoint, NSRect, NSSize};

use crate::layout::{self, Node, Size};
use crate::ui::{self, ns, CloseDelegate};

/// What a report shows now.
pub struct Shown {
    pub headline: String,
    pub stats: String,
    pub lines: Vec<String>,
    /// The tick box's state, when the report has one.
    pub toggle: Option<bool>,
    /// Whether the action button shows.
    pub button: bool,
}

/// What each view of the window is, for layout.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Part {
    Headline,
    Stats,
    Text,
    Toggle,
    Button,
    Close,
}

pub struct ReportWindow {
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    headline: Retained<NSTextField>,
    stats: Retained<NSTextField>,
    scroll: Retained<NSScrollView>,
    text: Retained<NSTextView>,
    toggle: Option<Retained<NSButton>>,
    button: Option<Retained<NSButton>>,
    close: Retained<NSButton>,
    shown: std::cell::RefCell<String>,
}

impl ReportWindow {
    /// `toggle` and `button` are `(label, id)`; `close_id` is what the Close button sends.
    pub fn open(
        mtm: MainThreadMarker,
        title: &str,
        toggle: Option<(&str, &str)>,
        button: Option<(&str, &str)>,
        close_id: &str,
        on_close: Box<dyn Fn()>,
    ) -> ReportWindow {
        let (window, delegate) = ui::window(mtm, title, 560.0, 440.0, on_close);
        let body = window.contentView().expect("the window's content view");
        let headline = ui::label(mtm, "");
        // Long stats (a filesystem's counters) wrap onto a second line.
        let stats = NSTextField::wrappingLabelWithString(&ns(""), mtm);
        let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(500.0, 300.0));
        let scroll = NSScrollView::initWithFrame(mtm.alloc(), frame);
        scroll.setHasVerticalScroller(true);
        scroll.setBorderType(NSBorderType::BezelBorder);
        let text = NSTextView::initWithFrame(mtm.alloc(), frame);
        text.setEditable(false);
        text.setVerticallyResizable(true);
        text.setAutoresizingMask(objc2_app_kit::NSAutoresizingMaskOptions::ViewWidthSizable);
        text.setFont(Some(&crate::app::log_font()));
        scroll.setDocumentView(Some(&text));
        let toggle = toggle.map(|(label, id)| ui::checkbox(mtm, label, id));
        let button = button.map(|(label, id)| ui::button(mtm, label, id));
        let close = ui::button(mtm, "Close", close_id);
        body.addSubview(&headline);
        body.addSubview(&stats);
        body.addSubview(&scroll);
        for b in toggle.iter().chain(button.iter()) {
            body.addSubview(b);
        }
        body.addSubview(&close);
        let w = ReportWindow {
            window,
            _delegate: delegate,
            headline,
            stats,
            scroll,
            text,
            toggle,
            button,
            close,
            shown: Default::default(),
        };
        w.layout();
        ui::present(&w.window);
        w
    }

    pub fn show(&self, s: &Shown) {
        ui::set_text(&self.headline, &s.headline);
        ui::set_text(&self.stats, &s.stats);
        let text = ui::plain(&s.lines.join("\n"));
        if *self.shown.borrow() != text {
            self.text.setString(&ns(&text));
            self.text
                .setTextColor(Some(&objc2_app_kit::NSColor::textColor()));
            *self.shown.borrow_mut() = text;
        }
        if let (Some(t), Some(on)) = (&self.toggle, s.toggle) {
            let want = if on {
                NSControlStateValueOn
            } else {
                NSControlStateValueOff
            };
            if t.state() != want {
                t.setState(want);
            }
        }
        if let Some(t) = &self.toggle {
            ui::set_visible(t, s.toggle.is_some());
        }
        if let Some(b) = &self.button {
            ui::set_visible(b, s.button);
        }
        self.layout();
    }

    /// The tick box's state, for the owner's toggle action.
    pub fn toggled(&self) -> bool {
        self.toggle
            .as_ref()
            .is_some_and(|t| t.state() == NSControlStateValueOn)
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        let lines = |t: &NSTextField| !t.stringValue().to_string().is_empty();
        let mut rows = Vec::new();
        if lines(&self.headline) {
            rows.push(Node::leaf(
                Part::Headline,
                Size::Stretch(1),
                Size::Fixed(layout::ROW - 4),
            ));
        }
        if lines(&self.stats) {
            rows.push(Node::leaf(
                Part::Stats,
                Size::Stretch(1),
                Size::Fixed(2 * (layout::ROW - 6) + 4),
            ));
        }
        rows.push(Node::leaf(Part::Text, Size::Stretch(1), Size::Stretch(1)));
        rows.push(Node::row(vec![
            Node::button(Part::Toggle, 200),
            Node::Space(Size::Stretch(1)),
            Node::button(Part::Button, 100),
            Node::button(Part::Close, 90),
        ]));
        let shown = |p: &Part| match p {
            Part::Toggle => self.toggle.as_ref().is_some_and(|t| ui::shown(t)),
            Part::Button => self.button.as_ref().is_some_and(|b| ui::shown(b)),
            _ => true,
        };
        let Some(tree) = Node::inset(Node::column(rows), layout::MARGIN).prune(&shown) else {
            return;
        };
        for (part, r) in layout::layout(&tree, ui::area(&body), 96) {
            match part {
                Part::Headline => ui::move_to(&self.headline, r),
                Part::Stats => ui::move_to(&self.stats, r),
                Part::Text => ui::move_to(&self.scroll, r),
                Part::Toggle => {
                    if let Some(t) = &self.toggle {
                        ui::move_to(t, r);
                    }
                }
                Part::Button => {
                    if let Some(b) = &self.button {
                        ui::move_to(b, r);
                    }
                }
                Part::Close => ui::move_to(&self.close, r),
            }
        }
    }
}

impl Drop for ReportWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}
