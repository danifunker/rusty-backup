//! A window the controller describes (`commander::PaneWindow`): its parts top to bottom and its
//! buttons along the bottom, each sending what it carries. The Win32 shell's `window.rs` is the
//! reference.
//!
//! The owner keeps one per kind. While the controller's key for it stays the same the window only
//! takes new text, ticks and enabled states ([`ModelWindow::update`]), so what is typed in a field
//! survives; a new key builds it again in the same place.

use std::cell::Cell;
use std::rc::{Rc, Weak};

use rusty_backup::controller::commander::{
    field_action, submit, CommanderAction as A, FieldId, PaneWindow, Part, Press, Tone, WindowKind,
};

use super::form::Form;
use super::host::{Host, RowIcon, RowLine, RowsSpec, Surface};
use super::layout::{self, Node, Size};
use super::sync::SyncGuard;

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

/// A control of the window, by its part's place.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum K {
    Text(usize),
    Caption(usize),
    Opt(usize, usize),
    Note(usize),
    Pick(usize),
    Input(usize),
    Picker(usize),
    PickerItem(usize, usize),
    List(usize),
    Detail(usize),
    Toggle(usize),
    ItemsButton(usize),
    Checklist(usize),
    Button(usize),
}

/// One built part: what its controls do.
enum Built {
    Text,
    Scroll,
    Choice {
        n: usize,
        send: Option<Vec<Vec<A>>>,
        notes: Option<Vec<String>>,
    },
    Select {
        send: Vec<Vec<A>>,
    },
    Field {
        id: FieldId,
        live: bool,
        multiline: bool,
        captioned: bool,
    },
    Picker {
        options: Vec<(String, Vec<(FieldId, String)>)>,
    },
    Checklist {
        send: Vec<Vec<A>>,
        lines: Vec<String>,
    },
    Browse {
        details: Vec<String>,
    },
    Toggle(Option<(Vec<A>, Vec<A>)>),
    Items {
        actions: Vec<Vec<A>>,
    },
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
    format!("window.field.{}", format!("{:?}", id).to_ascii_lowercase())
}

fn tone(t: Tone) -> Option<Tone> {
    match t {
        Tone::Plain => None,
        t => Some(t),
    }
}

/// A checklist's rows: a tick box where the line has one, a plain line where not.
fn checklist_rows(lines: &[String], ticked: &[Option<bool>]) -> Vec<RowLine> {
    lines
        .iter()
        .zip(ticked)
        .map(|(l, t)| RowLine {
            cells: vec![l.clone()],
            depth: 0,
            icon: RowIcon::None,
            tick: *t,
        })
        .collect()
}

pub struct ModelWindow {
    ui: Rc<dyn Host>,
    window: Surface,
    form: Form<K>,
    pub kind: WindowKind,
    pub key: String,
    built: Vec<Built>,
    heights: Vec<i32>,
    buttons: Vec<(String, Press)>,
    dismiss: Option<Vec<A>>,
    picked: Cell<usize>,
    on_event: Rc<dyn Fn(Event)>,
    sync: SyncGuard,
}

impl ModelWindow {
    /// Open `w`; `at` is where an earlier build of the same window stood.
    pub fn open(
        ui: Rc<dyn Host>,
        w: &PaneWindow,
        at: Option<(f64, f64, f64, f64)>,
        on_event: Rc<dyn Fn(Event)>,
    ) -> Rc<Self> {
        let me = Rc::new_cyclic(|me: &Weak<ModelWindow>| {
            let close_me = me.clone();
            let (width, height) = w.size;
            let window = ui.window(
                &w.title,
                width,
                height.max(120),
                Box::new(move || {
                    if let Some(m) = close_me.upgrade() {
                        m.closed();
                    }
                }),
            );
            let resize_me = me.clone();
            ui.on_resize(
                window,
                Box::new(move || {
                    if let Some(m) = resize_me.upgrade() {
                        m.layout();
                    }
                }),
            );
            let fire_me = me.clone();
            let form = Form::with_host(
                ui.clone(),
                window,
                Rc::new(move |k| {
                    if let Some(m) = fire_me.upgrade() {
                        m.fired(k);
                    }
                }),
            );
            let f = &form;
            let mut built = Vec::new();
            let mut heights = Vec::new();
            let mut picked = 0;
            for (pi, part) in w.parts.iter().enumerate() {
                let (b, h) = match part {
                    Part::Text(t, tn) => {
                        f.wrap(K::Text(pi), t);
                        f.set_tone(K::Text(pi), tone(*tn));
                        (Built::Text, text_height(t, width))
                    }
                    Part::List(lines) => {
                        f.text_view(K::Detail(pi), "");
                        f.set_text_view(K::Detail(pi), &lines.join("\n"));
                        let rows = lines.len().clamp(2, 8) as i32;
                        (Built::Scroll, rows * 16 + 10)
                    }
                    Part::Block(text) => {
                        f.text_view(K::Detail(pi), "");
                        f.set_text_view(K::Detail(pi), text);
                        (Built::Scroll, -1)
                    }
                    Part::Choice {
                        options,
                        picked: p,
                        send,
                        notes,
                    } => {
                        picked = *p;
                        for (i, label) in options.iter().enumerate() {
                            f.radio(K::Opt(pi, i), pi, label, "");
                            f.set_check(K::Opt(pi, i), i == *p);
                        }
                        let rows = match send {
                            Some(_) => options.len() as i32 * layout::ROW,
                            None => layout::ROW,
                        };
                        let notes = (!notes.is_empty()).then(|| {
                            f.label(K::Note(pi), notes.get(*p).map_or("", |n| n.as_str()));
                            f.set_tone(K::Note(pi), Some(Tone::Warning));
                            notes.clone()
                        });
                        let rows = rows + notes.as_ref().map_or(0, |_| layout::ROW - 6);
                        let b = Built::Choice {
                            n: options.len(),
                            send: send.clone(),
                            notes,
                        };
                        (b, rows)
                    }
                    Part::Select {
                        label,
                        options,
                        picked,
                        send,
                    } => {
                        f.label(K::Caption(pi), label);
                        f.popup(K::Pick(pi), options, "");
                        f.select(K::Pick(pi), *picked);
                        let b = Built::Select { send: send.clone() };
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
                        f.label(K::Caption(pi), label);
                        if *multiline {
                            f.text_area(K::Input(pi), &field_id(*id), label);
                            f.set_text_view(K::Input(pi), value);
                        } else {
                            f.field(K::Input(pi), &field_id(*id), label, *secret);
                            f.set_text(K::Input(pi), value);
                        }
                        let h = if *multiline { -1 } else { layout::ROW };
                        let b = Built::Field {
                            id: *id,
                            live: *live,
                            multiline: *multiline,
                            captioned: !label.is_empty(),
                        };
                        (b, h)
                    }
                    Part::Picker { label, options } => {
                        f.pulldown(K::Picker(pi), "window.picker");
                        let items: Vec<(String, Option<K>, bool)> = options
                            .iter()
                            .enumerate()
                            .map(|(i, (o, _))| (o.clone(), Some(K::PickerItem(pi, i)), true))
                            .collect();
                        f.set_pulldown(K::Picker(pi), label, &items);
                        let b = Built::Picker {
                            options: options.clone(),
                        };
                        (b, layout::ROW)
                    }
                    Part::Checklist {
                        lines,
                        ticked,
                        send,
                    } => {
                        let spec = RowsSpec {
                            columns: vec![(String::new(), 400.0, false)],
                            header: false,
                            multiple: false,
                            stripes: false,
                        };
                        let c = f.custom(K::Checklist(pi));
                        let tick_me = me.clone();
                        ui.rows_view(
                            window,
                            c,
                            "",
                            &spec,
                            Rc::new(move |e| {
                                if let super::host::RowsEvent::Tick(row) = e {
                                    if let Some(m) = tick_me.upgrade() {
                                        m.ticked(pi, row);
                                    }
                                }
                            }),
                        );
                        ui.set_row_lines(window, c, &checklist_rows(lines, ticked));
                        let b = Built::Checklist {
                            send: send.clone(),
                            lines: lines.clone(),
                        };
                        (b, -1)
                    }
                    Part::Browse { lines, details } => {
                        f.table(K::List(pi), "", &[("", 400.0, false)], None, None);
                        let rows: Vec<Vec<String>> =
                            lines.iter().map(|l| vec![l.clone()]).collect();
                        f.set_rows(K::List(pi), &rows);
                        f.text_view(K::Detail(pi), "");
                        let b = Built::Browse {
                            details: details.clone(),
                        };
                        (b, -1)
                    }
                    Part::Toggle { label, on, send } => {
                        f.check(K::Toggle(pi), label, "");
                        f.set_check(K::Toggle(pi), *on);
                        (Built::Toggle(send.clone()), layout::ROW)
                    }
                    Part::Items {
                        lines,
                        button,
                        actions,
                        enabled,
                    } => {
                        f.table(K::List(pi), "", &[("", 400.0, false)], None, None);
                        let rows: Vec<Vec<String>> =
                            lines.iter().map(|l| vec![l.clone()]).collect();
                        f.set_rows(K::List(pi), &rows);
                        f.button(K::ItemsButton(pi), button, "");
                        f.enable(K::ItemsButton(pi), false);
                        let actions = if *enabled {
                            actions.clone()
                        } else {
                            Vec::new()
                        };
                        (Built::Items { actions }, -1)
                    }
                };
                built.push(b);
                heights.push(h);
            }
            let mut buttons = Vec::new();
            for (i, b) in w.buttons.iter().enumerate() {
                f.button(K::Button(i), &b.label, &button_id(&b.label));
                f.enable(K::Button(i), b.enabled);
                buttons.push((b.label.clone(), b.press.clone()));
            }
            if w.buttons.is_empty() {
                f.button(K::Button(0), "Close", "window.close");
                let press = Press::Send(w.dismiss.clone().unwrap_or_default());
                buttons.push(("Close".to_string(), press));
            }
            if let Some(r) = at {
                ui.set_window_frame(window, r);
            } else if height == 0 {
                let content: i32 = heights.iter().map(|h| if *h < 0 { 160 } else { *h }).sum();
                let rows = heights.len() as i32 + 1;
                let h = content + rows * layout::GAP + layout::ROW + 2 * layout::MARGIN;
                ui.resize_window(window, width, h);
            }
            ui.present(window);
            ModelWindow {
                ui: ui.clone(),
                window,
                form,
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
        });
        me.layout();
        me
    }

    /// Take `w`'s new lines, choices, ticks and button states; fields keep what is typed in them.
    pub fn update(&self, w: &PaneWindow) {
        let f = &self.form;
        self.sync.run(|| {
            for (pi, (b, part)) in self.built.iter().zip(&w.parts).enumerate() {
                match (b, part) {
                    (Built::Text, Part::Text(t, tn)) => {
                        f.set_text(K::Text(pi), t);
                        f.set_tone(K::Text(pi), tone(*tn));
                    }
                    (Built::Choice { n, notes, .. }, Part::Choice { picked, .. }) => {
                        for i in 0..*n {
                            f.set_check(K::Opt(pi, i), i == *picked);
                        }
                        self.picked.set(*picked);
                        if let Some(notes) = notes {
                            f.set_text(K::Note(pi), notes.get(*picked).map_or("", |n| n.as_str()));
                        }
                    }
                    (Built::Select { .. }, Part::Select { picked, .. }) => {
                        f.select(K::Pick(pi), *picked);
                    }
                    (Built::Checklist { lines, .. }, Part::Checklist { ticked, .. }) => {
                        let c = f.ctl(K::Checklist(pi));
                        self.ui
                            .set_row_lines(self.window, c, &checklist_rows(lines, ticked));
                    }
                    _ => {}
                }
            }
            for (i, b) in w.buttons.iter().enumerate() {
                if f.has(K::Button(i)) {
                    f.set_title(K::Button(i), &b.label);
                    f.enable(K::Button(i), b.enabled);
                }
            }
        });
    }

    /// Where the window stands, for a rebuild to open in the same place.
    pub fn frame(&self) -> Option<(f64, f64, f64, f64)> {
        self.ui.window_frame(self.window)
    }

    fn layout(&self) {
        let mut col = Vec::new();
        let row_h = Size::Fixed(layout::ROW);
        for (pi, (b, h)) in self.built.iter().zip(&self.heights).enumerate() {
            let height = match *h {
                -1 => Size::Stretch(1),
                h => Size::Fixed(h),
            };
            match b {
                Built::Text => col.push(Node::leaf(K::Text(pi), Size::Stretch(1), height)),
                Built::Scroll => col.push(Node::leaf(K::Detail(pi), Size::Stretch(1), height)),
                Built::Toggle(_) => col.push(Node::leaf(K::Toggle(pi), Size::Stretch(1), height)),
                Built::Choice { n, send, notes } => {
                    let row: Vec<Node<K>> = (0..*n)
                        .map(|i| Node::leaf(K::Opt(pi, i), Size::Fixed(200), row_h))
                        .collect();
                    match send {
                        // Options that act at once stack, each on its own line.
                        Some(_) => col.extend(row),
                        None => col.push(Node::row(row)),
                    }
                    if notes.is_some() {
                        col.push(Node::leaf(
                            K::Note(pi),
                            Size::Stretch(1),
                            Size::Fixed(layout::ROW - 6),
                        ));
                    }
                }
                Built::Select { .. } => col.push(Node::row(vec![
                    Node::leaf(
                        K::Caption(pi),
                        Size::Fixed(LABEL_W),
                        Size::Fixed(layout::ROW - 4),
                    ),
                    Node::leaf(K::Pick(pi), Size::Stretch(1), Size::Fixed(layout::ROW + 2)),
                ])),
                Built::Field {
                    multiline,
                    captioned,
                    ..
                } => {
                    if *multiline && !*captioned {
                        col.push(Node::leaf(K::Input(pi), Size::Stretch(1), height));
                    } else {
                        col.push(Node::row(vec![
                            Node::leaf(
                                K::Caption(pi),
                                Size::Fixed(LABEL_W),
                                Size::Fixed(layout::ROW - 4),
                            ),
                            Node::leaf(K::Input(pi), Size::Stretch(1), height),
                        ]));
                    }
                }
                Built::Picker { .. } => col.push(Node::row(vec![
                    Node::leaf(
                        K::Picker(pi),
                        Size::Fixed(200),
                        Size::Fixed(layout::ROW + 2),
                    ),
                    Node::Space(Size::Stretch(1)),
                ])),
                Built::Checklist { .. } => {
                    col.push(Node::leaf(K::Checklist(pi), Size::Stretch(1), height))
                }
                Built::Browse { .. } => {
                    col.push(Node::leaf(K::List(pi), Size::Stretch(1), Size::Stretch(1)));
                    col.push(Node::leaf(
                        K::Detail(pi),
                        Size::Stretch(1),
                        Size::Stretch(1),
                    ));
                }
                Built::Items { .. } => {
                    col.push(Node::leaf(K::List(pi), Size::Stretch(1), height));
                    col.push(Node::row(vec![
                        Node::Space(Size::Stretch(1)),
                        Node::leaf(K::ItemsButton(pi), Size::Fixed(110), row_h),
                    ]));
                }
            }
        }
        let mut row = vec![Node::Space(Size::Stretch(1))];
        for (i, (label, _)) in self.buttons.iter().enumerate() {
            let w = (label.chars().count() as i32 * 8 + 34).max(84);
            row.push(Node::leaf(K::Button(i), Size::Fixed(w), row_h));
        }
        col.push(Node::row(row));
        let tree = Node::inset(Node::column(col), layout::MARGIN);
        self.form.place(&tree, self.ui.area(self.window));
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

    /// What field `pi` holds now.
    fn input_text(&self, pi: usize) -> String {
        match self.built.get(pi) {
            Some(Built::Field {
                multiline: true, ..
            }) => self.form.text_view_text(K::Input(pi)),
            _ => self.form.text(K::Input(pi)),
        }
    }

    /// What the fields hold now, for `submit`.
    fn values(&self) -> Vec<(FieldId, String)> {
        self.built
            .iter()
            .enumerate()
            .filter_map(|(pi, b)| match b {
                Built::Field { id, .. } => Some((*id, self.input_text(pi))),
                _ => None,
            })
            .collect()
    }

    fn ticked(&self, pi: usize, row: usize) {
        if self.sync.active() {
            return;
        }
        if let Some(Built::Checklist { send, .. }) = self.built.get(pi) {
            if let Some(a) = send.get(row) {
                self.event(Event::Send(a.clone()));
            }
        }
    }

    fn fired(&self, k: K) {
        if self.sync.active() {
            return;
        }
        let f = &self.form;
        match k {
            K::Input(pi) => {
                if let Some(Built::Field { id, live: true, .. }) = self.built.get(pi) {
                    if let Some(a) = field_action(*id, &self.input_text(pi)) {
                        self.event(Event::Send(vec![a]));
                    }
                }
            }
            K::Opt(pi, i) => {
                if let Some(Built::Choice { notes, send, .. }) = self.built.get(pi) {
                    self.picked.set(i);
                    if let Some(notes) = notes {
                        f.set_text(K::Note(pi), notes.get(i).map_or("", |n| n.as_str()));
                    }
                    if let Some(a) = send.as_ref().and_then(|s| s.get(i)) {
                        self.event(Event::Send(a.clone()));
                    }
                }
            }
            K::Pick(pi) => {
                if let Some(Built::Select { send }) = self.built.get(pi) {
                    if let Some(a) = f.picked(K::Pick(pi)).and_then(|i| send.get(i)) {
                        self.event(Event::Send(a.clone()));
                    }
                }
            }
            K::PickerItem(pi, i) => {
                if let Some(Built::Picker { options }) = self.built.get(pi) {
                    let set = match options.get(i) {
                        Some((_, set)) => set,
                        None => return,
                    };
                    for (fi, b) in self.built.iter().enumerate() {
                        if let Built::Field { id, multiline, .. } = b {
                            if let Some((_, v)) = set.iter().find(|(fid, _)| fid == id) {
                                if *multiline {
                                    f.set_text_view(K::Input(fi), v);
                                } else {
                                    f.set_text(K::Input(fi), v);
                                }
                            }
                        }
                    }
                }
            }
            K::Toggle(pi) => {
                if let Some(Built::Toggle(send)) = self.built.get(pi) {
                    let on = f.checked(K::Toggle(pi));
                    match send {
                        Some((yes, no)) => {
                            self.event(Event::Send(if on { yes } else { no }.clone()))
                        }
                        None => self.event(Event::Toggle(on)),
                    }
                }
            }
            K::List(pi) => match self.built.get(pi) {
                Some(Built::Browse { details }) => {
                    let text = f
                        .selected_row(K::List(pi))
                        .and_then(|i| details.get(i))
                        .cloned()
                        .unwrap_or_default();
                    f.set_text_view(K::Detail(pi), &text);
                }
                Some(Built::Items { actions }) => {
                    let can = f
                        .selected_row(K::List(pi))
                        .and_then(|i| actions.get(i))
                        .is_some_and(|a| !a.is_empty());
                    f.enable(K::ItemsButton(pi), can);
                }
                _ => {}
            },
            K::ItemsButton(pi) => {
                if let Some(Built::Items { actions }) = self.built.get(pi) {
                    if let Some(a) = f.selected_row(K::List(pi)).and_then(|i| actions.get(i)) {
                        self.event(Event::Send(a.clone()));
                    }
                }
            }
            K::Button(i) => match self.buttons.get(i).map(|(_, p)| p.clone()) {
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
            _ => {}
        }
    }
}

impl Drop for ModelWindow {
    fn drop(&mut self) {
        self.ui.release(self.window);
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
