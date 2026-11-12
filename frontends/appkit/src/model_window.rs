//! A window the controller describes (`commander::PaneWindow`): its parts top to bottom and its
//! buttons along the bottom, each sending what it carries. The Win32 shell's `window.rs` is the
//! reference.
//!
//! The owner keeps one per kind. While the controller's key for it stays the same the window only
//! takes new text, ticks and enabled states ([`ModelWindow::update`]), so what is typed in a field
//! survives; a new key builds it again in the same place. Every control has a tag; one target
//! per window hands the tag to [`ModelWindow::fired`].

use std::cell::Cell;
use std::rc::{Rc, Weak};

use objc2::rc::Retained;
use objc2::runtime::{AnyObject, ProtocolObject};
use objc2::{MainThreadMarker, Message};
use objc2_app_kit::{
    NSAccessibility, NSBorderType, NSButton, NSColor, NSControl, NSControlStateValueOff,
    NSControlStateValueOn, NSMenuItem, NSPopUpButton, NSScrollView, NSSecureTextField, NSTextField,
    NSTextView, NSView, NSWindow,
};
use objc2_foundation::{NSPoint, NSRect, NSSize};
use rusty_backup::controller::commander::{
    field_action, submit, CommanderAction as A, FieldId, PaneWindow, Part, Press, Tone, WindowKind,
};

use crate::layout::{self, Node, Size};
use crate::sync::SyncGuard;
use crate::ui::{self, ns, Callback, CloseDelegate, TextChange};

/// What the window hands its owner.
pub enum Event {
    Send(Vec<A>),
    Copy(String),
    /// A tick box the view keeps (no actions of its own) changed.
    Toggle(bool),
    /// Closed with nothing to send: the owner drops what the window showed.
    Closed,
}

/// The width of a field's label, in points.
const LABEL_W: i32 = 120;

/// A field's text control: one line, or a scrolled text view.
enum Input {
    Line(Retained<NSTextField>),
    Lines(Retained<NSScrollView>, Retained<NSTextView>),
}

impl Input {
    fn text(&self) -> String {
        match self {
            Input::Line(f) => f.stringValue().to_string(),
            Input::Lines(_, v) => v.string().to_string(),
        }
    }

    fn set_text(&self, t: &str) {
        match self {
            Input::Line(f) => ui::set_text(f, t),
            Input::Lines(_, v) => v.setString(&ns(&ui::plain(t))),
        }
    }

    fn view(&self) -> &NSView {
        match self {
            Input::Line(f) => f,
            Input::Lines(s, _) => s,
        }
    }
}

/// One built part: its views and what they do.
enum Built {
    Text(Retained<NSTextField>),
    Scroll(Retained<NSScrollView>),
    Choice {
        radios: Vec<Retained<NSButton>>,
        send: Option<Vec<Vec<A>>>,
        note: Option<(Retained<NSTextField>, Vec<String>)>,
    },
    Select {
        label: Retained<NSTextField>,
        popup: Retained<NSPopUpButton>,
        send: Vec<Vec<A>>,
    },
    Field {
        label: Retained<NSTextField>,
        input: Input,
        id: FieldId,
        live: bool,
        _delegate: Retained<TextChange>,
    },
    Picker {
        popup: Retained<NSPopUpButton>,
        options: Vec<(String, Vec<(FieldId, String)>)>,
    },
    Checklist {
        scroll: Retained<NSScrollView>,
        boxes: Vec<Option<Retained<NSButton>>>,
        send: Vec<Vec<A>>,
    },
    Browse {
        list: crate::table::Table,
        detail: (Retained<NSScrollView>, Retained<NSTextView>),
    },
    Toggle(Retained<NSButton>, Option<(Vec<A>, Vec<A>)>),
    Items {
        list: crate::table::Table,
        button: Retained<NSButton>,
        actions: Vec<Vec<A>>,
    },
}

/// What a control's tag stands for.
#[derive(Clone, Copy)]
enum Tag {
    Part(usize, usize),
    Button(usize),
}

fn encode(t: Tag) -> isize {
    match t {
        Tag::Part(p, i) => (p as isize) << 16 | i as isize,
        Tag::Button(i) => -(i as isize) - 1,
    }
}

fn decode(n: isize) -> Tag {
    if n < 0 {
        Tag::Button((-n - 1) as usize)
    } else {
        Tag::Part((n >> 16) as usize, (n & 0xffff) as usize)
    }
}

/// A window button's id for the flow drivers: `window.` and its label, e.g. `window.apply_anyway`.
pub fn button_id(label: &str) -> String {
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

fn text_view(
    mtm: MainThreadMarker,
    mono: bool,
    editable: bool,
) -> (Retained<NSScrollView>, Retained<NSTextView>) {
    let frame = NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(400.0, 160.0));
    let scroll = NSScrollView::initWithFrame(mtm.alloc(), frame);
    scroll.setHasVerticalScroller(true);
    scroll.setBorderType(NSBorderType::BezelBorder);
    let view = NSTextView::initWithFrame(mtm.alloc(), frame);
    view.setEditable(editable);
    view.setRichText(false);
    view.setVerticallyResizable(true);
    view.setAutoresizingMask(objc2_app_kit::NSAutoresizingMaskOptions::ViewWidthSizable);
    if mono {
        view.setFont(Some(&crate::app::log_font()));
    }
    view.setTextColor(Some(&NSColor::textColor()));
    scroll.setDocumentView(Some(&view));
    (scroll, view)
}

fn tone_color(t: Tone) -> Option<Retained<NSColor>> {
    match t {
        Tone::Plain => None,
        Tone::Warning => Some(NSColor::systemOrangeColor()),
        Tone::Error => Some(NSColor::systemRedColor()),
        Tone::Success => Some(NSColor::systemGreenColor()),
    }
}

fn set_tone(label: &NSTextField, t: Tone) {
    label.setTextColor(Some(&tone_color(t).unwrap_or_else(NSColor::labelColor)));
}

fn set_on(b: &NSButton, on: bool) {
    let want = if on {
        NSControlStateValueOn
    } else {
        NSControlStateValueOff
    };
    if b.state() != want {
        b.setState(want);
    }
}

pub struct ModelWindow {
    window: Retained<NSWindow>,
    _delegate: Retained<CloseDelegate>,
    _target: Retained<Callback>,
    pub kind: WindowKind,
    pub key: String,
    built: Vec<Built>,
    heights: Vec<i32>,
    buttons: Vec<(Retained<NSButton>, Press)>,
    dismiss: Option<Vec<A>>,
    picked: Cell<usize>,
    on_event: Rc<dyn Fn(Event)>,
    sync: SyncGuard,
}

impl ModelWindow {
    /// Open `w`; `at` is where an earlier build of the same window stood.
    pub fn open(
        mtm: MainThreadMarker,
        w: &PaneWindow,
        at: Option<NSRect>,
        on_event: Rc<dyn Fn(Event)>,
    ) -> Rc<Self> {
        Rc::new_cyclic(|me: &Weak<ModelWindow>| {
            let close_me = me.clone();
            let (width, height) = w.size;
            let (window, delegate) = ui::window(
                mtm,
                &w.title,
                f64::from(width),
                f64::from(height.max(120)),
                Box::new(move || {
                    if let Some(m) = close_me.upgrade() {
                        m.closed();
                    }
                }),
            );
            let resize_me = me.clone();
            delegate.on_resize(Box::new(move || {
                if let Some(m) = resize_me.upgrade() {
                    m.layout();
                }
            }));
            let fire_me = me.clone();
            let target = ui::callback(
                mtm,
                Box::new(move |sender: &AnyObject| {
                    let tag = sender
                        .downcast_ref::<NSControl>()
                        .map(|c| c.tag())
                        .or_else(|| sender.downcast_ref::<NSMenuItem>().map(|m| m.tag()));
                    if let (Some(m), Some(tag)) = (fire_me.upgrade(), tag) {
                        m.fired(decode(tag), sender);
                    }
                }),
            );
            let body = window.contentView().expect("the window's content view");
            let hook = |c: &NSControl, tag: Tag| {
                c.setTag(encode(tag));
                ui::hook(c, &target);
            };
            let mut built = Vec::new();
            let mut heights = Vec::new();
            let mut picked = 0;
            for (pi, part) in w.parts.iter().enumerate() {
                let (b, h) = match part {
                    Part::Text(t, tone) => {
                        let l = NSTextField::wrappingLabelWithString(&ns(&ui::plain(t)), mtm);
                        set_tone(&l, *tone);
                        body.addSubview(&l);
                        let h = text_height(t, width);
                        (Built::Text(l), h)
                    }
                    Part::List(lines) => {
                        let (s, v) = text_view(mtm, false, false);
                        v.setString(&ns(&ui::plain(&lines.join("\n"))));
                        body.addSubview(&s);
                        let rows = lines.len().clamp(2, 8) as i32;
                        (Built::Scroll(s), rows * 16 + 10)
                    }
                    Part::Block(text) => {
                        let (s, v) = text_view(mtm, true, false);
                        v.setString(&ns(&ui::plain(text)));
                        body.addSubview(&s);
                        (Built::Scroll(s), -1)
                    }
                    Part::Choice {
                        options,
                        picked: p,
                        send,
                        notes,
                    } => {
                        picked = *p;
                        let radios: Vec<Retained<NSButton>> = options
                            .iter()
                            .enumerate()
                            .map(|(i, label)| {
                                // SAFETY: the target is hooked below and kept by the window.
                                let r = unsafe {
                                    NSButton::radioButtonWithTitle_target_action(
                                        &ns(&ui::plain(label)),
                                        None,
                                        None,
                                        mtm,
                                    )
                                };
                                hook(&r, Tag::Part(pi, i));
                                set_on(&r, i == *p);
                                body.addSubview(&r);
                                r
                            })
                            .collect();
                        let rows = match send {
                            Some(_) => options.len() as i32 * layout::ROW,
                            None => layout::ROW,
                        };
                        let note = (!notes.is_empty()).then(|| {
                            let l = ui::label(mtm, notes.get(*p).map_or("", |n| n.as_str()));
                            set_tone(&l, Tone::Warning);
                            body.addSubview(&l);
                            (l, notes.clone())
                        });
                        let rows = rows + note.as_ref().map_or(0, |_| layout::ROW - 6);
                        let b = Built::Choice {
                            radios,
                            send: send.clone(),
                            note,
                        };
                        (b, rows)
                    }
                    Part::Select {
                        label,
                        options,
                        picked,
                        send,
                    } => {
                        let caption = ui::label(mtm, label);
                        let popup = NSPopUpButton::initWithFrame_pullsDown(
                            mtm.alloc(),
                            NSRect::ZERO,
                            false,
                        );
                        for o in options {
                            popup.addItemWithTitle(&ns(&ui::plain(o)));
                        }
                        popup.selectItemAtIndex(*picked as isize);
                        hook(&popup, Tag::Part(pi, 0));
                        popup.setAccessibilityLabel(Some(&ns(label)));
                        body.addSubview(&caption);
                        body.addSubview(&popup);
                        let b = Built::Select {
                            label: caption,
                            popup,
                            send: send.clone(),
                        };
                        (b, layout::ROW)
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
                        let caption = ui::label(mtm, label);
                        let change_me = me.clone();
                        let pi_live = pi;
                        let delegate = ui::on_change(
                            mtm,
                            Box::new(move || {
                                if let Some(m) = change_me.upgrade() {
                                    m.field_changed(pi_live);
                                }
                            }),
                        );
                        let input = if *multiline {
                            let (s, v) = text_view(mtm, true, true);
                            v.setString(&ns(&ui::plain(value)));
                            ui::set_id(&v, &field_id(*id));
                            v.setAccessibilityLabel(Some(&ns(label)));
                            v.setDelegate(Some(ProtocolObject::from_ref(&*delegate)));
                            Input::Lines(s, v)
                        } else {
                            let frame =
                                NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(200.0, 22.0));
                            let f: Retained<NSTextField> = if *secret {
                                Retained::into_super(NSSecureTextField::initWithFrame(
                                    mtm.alloc(),
                                    frame,
                                ))
                            } else {
                                NSTextField::initWithFrame(mtm.alloc(), frame)
                            };
                            f.setStringValue(&ns(&ui::plain(value)));
                            ui::set_id(&f, &field_id(*id));
                            f.setAccessibilityLabel(Some(&ns(label)));
                            // SAFETY: as above.
                            unsafe { f.setDelegate(Some(ProtocolObject::from_ref(&*delegate))) };
                            Input::Line(f)
                        };
                        body.addSubview(&caption);
                        body.addSubview(input.view());
                        let h = if *multiline { -1 } else { layout::ROW };
                        let b = Built::Field {
                            label: caption,
                            input,
                            id: *id,
                            live: *live,
                            _delegate: delegate,
                        };
                        (b, h)
                    }
                    Part::Picker { label, options } => {
                        let popup =
                            NSPopUpButton::initWithFrame_pullsDown(mtm.alloc(), NSRect::ZERO, true);
                        popup.addItemWithTitle(&ns(label));
                        for (i, (o, _)) in options.iter().enumerate() {
                            popup.addItemWithTitle(&ns(&ui::plain(o)));
                            if let Some(item) = popup.lastItem() {
                                item.setTag(encode(Tag::Part(pi, i)));
                                // SAFETY: the target is kept by the window.
                                unsafe {
                                    item.setTarget(Some(&target));
                                    item.setAction(Some(objc2::sel!(fire:)));
                                }
                            }
                        }
                        ui::set_id(&popup, "window.picker");
                        body.addSubview(&popup);
                        let b = Built::Picker {
                            popup,
                            options: options.clone(),
                        };
                        (b, layout::ROW)
                    }
                    Part::Checklist {
                        lines,
                        ticked,
                        send,
                    } => {
                        let scroll = NSScrollView::initWithFrame(mtm.alloc(), NSRect::ZERO);
                        scroll.setHasVerticalScroller(true);
                        scroll.setBorderType(NSBorderType::BezelBorder);
                        let doc = ui::flipped(mtm);
                        let row_h = 20.0;
                        doc.setFrameSize(NSSize::new(400.0, row_h * lines.len() as f64 + 4.0));
                        let mut boxes = Vec::new();
                        for (i, (line, tick)) in lines.iter().zip(ticked).enumerate() {
                            let y = 2.0 + row_h * i as f64;
                            let frame =
                                NSRect::new(NSPoint::new(4.0, y), NSSize::new(1000.0, row_h));
                            match tick {
                                Some(on) => {
                                    // SAFETY: the target is hooked below and kept by the window.
                                    let b = unsafe {
                                        NSButton::checkboxWithTitle_target_action(
                                            &ns(&ui::plain(line)),
                                            None,
                                            None,
                                            mtm,
                                        )
                                    };
                                    hook(&b, Tag::Part(pi, i));
                                    set_on(&b, *on);
                                    b.setFrame(frame);
                                    doc.addSubview(&b);
                                    boxes.push(Some(b));
                                }
                                None => {
                                    let l = ui::label(mtm, line);
                                    l.setFrame(frame);
                                    doc.addSubview(&l);
                                    boxes.push(None);
                                }
                            }
                        }
                        scroll.setDocumentView(Some(&doc));
                        body.addSubview(&scroll);
                        let b = Built::Checklist {
                            scroll,
                            boxes,
                            send: send.clone(),
                        };
                        (b, -1)
                    }
                    Part::Browse { lines, details } => {
                        let list = crate::table::Table::new(mtm, "", &[("", 400.0, false)]);
                        list.view.setHeaderView(None);
                        list.set_rows(lines.iter().map(|l| vec![ui::plain(l)]).collect());
                        let detail = text_view(mtm, true, false);
                        let shown = detail.1.clone();
                        let details = details.clone();
                        list.on_select(Rc::new(move |i| {
                            let text = i.and_then(|i| details.get(i)).cloned().unwrap_or_default();
                            shown.setString(&ns(&ui::plain(&text)));
                        }));
                        body.addSubview(&list.scroll);
                        body.addSubview(&detail.0);
                        (Built::Browse { list, detail }, -1)
                    }
                    Part::Toggle { label, on, send } => {
                        let b = ui::checkbox(mtm, label, "");
                        hook(&b, Tag::Part(pi, 0));
                        set_on(&b, *on);
                        body.addSubview(&b);
                        (Built::Toggle(b, send.clone()), layout::ROW)
                    }
                    Part::Items {
                        lines,
                        button,
                        actions,
                        enabled,
                    } => {
                        let list = crate::table::Table::new(mtm, "", &[("", 400.0, false)]);
                        list.view.setHeaderView(None);
                        list.set_rows(lines.iter().map(|l| vec![ui::plain(l)]).collect());
                        let b = ui::button(mtm, button, "");
                        hook(&b, Tag::Part(pi, 0));
                        b.setEnabled(false);
                        let actions = if *enabled {
                            actions.clone()
                        } else {
                            Vec::new()
                        };
                        let chosen = b.clone();
                        let can = actions.iter().map(|a| !a.is_empty()).collect::<Vec<_>>();
                        list.on_select(Rc::new(move |i| {
                            let on = i.and_then(|i| can.get(i).copied()).unwrap_or(false);
                            chosen.setEnabled(on);
                        }));
                        body.addSubview(&list.scroll);
                        body.addSubview(&b);
                        (
                            Built::Items {
                                list,
                                button: b,
                                actions,
                            },
                            -1,
                        )
                    }
                };
                built.push(b);
                heights.push(h);
            }
            let mut buttons = Vec::new();
            for (i, b) in w.buttons.iter().enumerate() {
                let h = ui::button(mtm, &b.label, &button_id(&b.label));
                hook(&h, Tag::Button(i));
                h.setEnabled(b.enabled);
                body.addSubview(&h);
                buttons.push((h, b.press.clone()));
            }
            if w.buttons.is_empty() {
                let close = ui::button(mtm, "Close", "window.close");
                hook(&close, Tag::Button(0));
                body.addSubview(&close);
                let press = Press::Send(w.dismiss.clone().unwrap_or_default());
                buttons.push((close, press));
            }
            if let Some(r) = at {
                window.setFrame_display(r, false);
            } else if height == 0 {
                let content: i32 = heights.iter().map(|h| if *h < 0 { 160 } else { *h }).sum();
                let rows = heights.len() as i32 + 1;
                let h = content + rows * layout::GAP + layout::ROW + 2 * layout::MARGIN;
                window.setContentSize(NSSize::new(f64::from(width), f64::from(h)));
                window.center();
            }
            window.makeKeyAndOrderFront(None);
            ModelWindow {
                window,
                _delegate: delegate,
                _target: target,
                kind: w.kind,
                key: w.key.clone(),
                built,
                heights,
                buttons,
                dismiss: w.dismiss.clone(),
                picked: Cell::new(picked),
                on_event,
                sync: SyncGuard::default(),
            }
        })
        .tap_layout()
    }

    /// Take `w`'s new lines, choices, ticks and button states; fields keep what is typed in them.
    pub fn update(&self, w: &PaneWindow) {
        self.sync.run(|| {
            for (b, part) in self.built.iter().zip(&w.parts) {
                match (b, part) {
                    (Built::Text(l), Part::Text(t, tone)) => {
                        ui::set_text(l, t);
                        set_tone(l, *tone);
                    }
                    (Built::Choice { radios, note, .. }, Part::Choice { picked, .. }) => {
                        for (i, r) in radios.iter().enumerate() {
                            set_on(r, i == *picked);
                        }
                        self.picked.set(*picked);
                        if let Some((l, notes)) = note {
                            ui::set_text(l, notes.get(*picked).map_or("", |n| n.as_str()));
                        }
                    }
                    (Built::Select { popup, .. }, Part::Select { picked, .. }) => {
                        if popup.indexOfSelectedItem() != *picked as isize {
                            popup.selectItemAtIndex(*picked as isize);
                        }
                    }
                    (Built::Checklist { boxes, .. }, Part::Checklist { ticked, .. }) => {
                        for (b, t) in boxes.iter().zip(ticked) {
                            if let (Some(b), Some(on)) = (b, t) {
                                set_on(b, *on);
                            }
                        }
                    }
                    _ => {}
                }
            }
            for ((h, _), b) in self.buttons.iter().zip(&w.buttons) {
                if h.title().to_string() != b.label {
                    h.setTitle(&ns(&b.label));
                }
                ui::set_enabled(h, b.enabled);
            }
        });
    }

    /// Where the window stands, for a rebuild to open in the same place.
    pub fn frame(&self) -> NSRect {
        self.window.frame()
    }

    fn layout(&self) {
        let Some(body) = self.window.contentView() else {
            return;
        };
        // Views are named by their place: part `i` view `j`, or button `i`.
        let mut views: Vec<Retained<NSView>> = Vec::new();
        let mut leaf = |v: &NSView, w: Size, h: Size| -> Node<usize> {
            views.push(v.retain());
            Node::leaf(views.len() - 1, w, h)
        };
        let mut col = Vec::new();
        for (b, h) in self.built.iter().zip(&self.heights) {
            let height = match *h {
                -1 => Size::Stretch(1),
                h => Size::Fixed(h),
            };
            match b {
                Built::Text(l) => col.push(leaf(l, Size::Stretch(1), height)),
                Built::Scroll(s) => col.push(leaf(s, Size::Stretch(1), height)),
                Built::Toggle(c, _) => col.push(leaf(c, Size::Stretch(1), height)),
                Built::Choice { radios, send, note } => {
                    let row: Vec<Node<usize>> = radios
                        .iter()
                        .map(|r| leaf(r, Size::Fixed(200), Size::Fixed(layout::ROW)))
                        .collect();
                    match send {
                        // Options that act at once stack, each on its own line.
                        Some(_) => col.extend(row),
                        None => col.push(Node::row(row)),
                    }
                    if let Some((l, _)) = note {
                        col.push(leaf(l, Size::Stretch(1), Size::Fixed(layout::ROW - 6)));
                    }
                }
                Built::Select { label, popup, .. } => col.push(Node::row(vec![
                    leaf(label, Size::Fixed(LABEL_W), Size::Fixed(layout::ROW - 4)),
                    leaf(popup, Size::Stretch(1), Size::Fixed(layout::ROW + 2)),
                ])),
                Built::Field { label, input, .. } => match input {
                    Input::Lines(s, _) if label.stringValue().to_string().is_empty() => {
                        col.push(leaf(s, Size::Stretch(1), height))
                    }
                    _ => col.push(Node::row(vec![
                        leaf(label, Size::Fixed(LABEL_W), Size::Fixed(layout::ROW - 4)),
                        leaf(input.view(), Size::Stretch(1), height),
                    ])),
                },
                Built::Picker { popup, .. } => col.push(Node::row(vec![
                    leaf(popup, Size::Fixed(200), Size::Fixed(layout::ROW + 2)),
                    Node::Space(Size::Stretch(1)),
                ])),
                Built::Checklist { scroll, .. } => col.push(leaf(scroll, Size::Stretch(1), height)),
                Built::Browse { list, detail } => {
                    col.push(leaf(&list.scroll, Size::Stretch(1), Size::Stretch(1)));
                    col.push(leaf(&detail.0, Size::Stretch(1), Size::Stretch(1)));
                }
                Built::Items { list, button, .. } => {
                    col.push(leaf(&list.scroll, Size::Stretch(1), height));
                    col.push(Node::row(vec![
                        Node::Space(Size::Stretch(1)),
                        leaf(button, Size::Fixed(110), Size::Fixed(layout::ROW)),
                    ]));
                }
            }
        }
        let mut row = vec![Node::Space(Size::Stretch(1))];
        for (h, _) in &self.buttons {
            let w = (h.title().length() as i32 * 8 + 34).max(84);
            row.push(leaf(h, Size::Fixed(w), Size::Fixed(layout::ROW)));
        }
        col.push(Node::row(row));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        for (i, r) in layout::layout(&tree, ui::area(&body), 96) {
            ui::move_to(&views[i], r);
        }
    }

    fn event(&self, e: Event) {
        (self.on_event)(e);
    }

    fn closed(&self) {
        match &self.dismiss {
            Some(a) => self.event(Event::Send(a.clone())),
            None => self.event(Event::Closed),
        }
    }

    fn field_changed(&self, part: usize) {
        if self.sync.active() {
            return;
        }
        if let Some(Built::Field {
            input,
            id,
            live: true,
            ..
        }) = self.built.get(part)
        {
            if let Some(a) = field_action(*id, &input.text()) {
                self.event(Event::Send(vec![a]));
            }
        }
    }

    /// What the fields hold now, for `submit`.
    fn values(&self) -> Vec<(FieldId, String)> {
        self.built
            .iter()
            .filter_map(|b| match b {
                Built::Field { input, id, .. } => Some((*id, input.text())),
                _ => None,
            })
            .collect()
    }

    fn fired(&self, tag: Tag, sender: &AnyObject) {
        if self.sync.active() {
            return;
        }
        let on = sender
            .downcast_ref::<NSButton>()
            .is_some_and(|b| b.state() == NSControlStateValueOn);
        match tag {
            Tag::Part(p, i) => match self.built.get(p) {
                Some(Built::Choice { send, note, .. }) => {
                    self.picked.set(i);
                    if let Some((l, notes)) = note {
                        ui::set_text(l, notes.get(i).map_or("", |n| n.as_str()));
                    }
                    if let Some(a) = send.as_ref().and_then(|s| s.get(i)) {
                        self.event(Event::Send(a.clone()));
                    }
                }
                Some(Built::Select { popup, send, .. }) => {
                    let at = popup.indexOfSelectedItem();
                    if let Some(a) = usize::try_from(at).ok().and_then(|i| send.get(i)) {
                        self.event(Event::Send(a.clone()));
                    }
                }
                Some(Built::Picker { options, .. }) => {
                    let Some((_, set)) = options.get(i) else {
                        return;
                    };
                    for b in &self.built {
                        if let Built::Field { input, id, .. } = b {
                            if let Some((_, v)) = set.iter().find(|(f, _)| f == id) {
                                input.set_text(v);
                            }
                        }
                    }
                }
                Some(Built::Checklist { send, .. }) => {
                    if let Some(a) = send.get(i) {
                        self.event(Event::Send(a.clone()));
                    }
                }
                Some(Built::Toggle(_, send)) => match send {
                    Some((yes, no)) => self.event(Event::Send(if on { yes } else { no }.clone())),
                    None => self.event(Event::Toggle(on)),
                },
                Some(Built::Items { list, actions, .. }) => {
                    if let Some(a) = list.selected().and_then(|i| actions.get(i)) {
                        self.event(Event::Send(a.clone()));
                    }
                }
                _ => {}
            },
            Tag::Button(i) => match self.buttons.get(i).map(|(_, p)| p.clone()) {
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
            },
        }
    }
}

trait TapLayout {
    fn tap_layout(self) -> Self;
}

impl TapLayout for Rc<ModelWindow> {
    fn tap_layout(self) -> Self {
        self.layout();
        self
    }
}

impl Drop for ModelWindow {
    fn drop(&mut self) {
        self.window.setDelegate(None);
        self.window.orderOut(None);
    }
}

/// About how tall a wrapped `text` is at `width` points.
fn text_height(text: &str, width: i32) -> i32 {
    let per_line = ((width - 2 * layout::MARGIN) / 7).max(20) as usize;
    let lines: usize = text
        .split('\n')
        .map(|l| l.chars().count().div_ceil(per_line).max(1))
        .sum();
    (layout::ROW - 6) * lines as i32
}
