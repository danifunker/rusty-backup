//! The form rows Backup and Restore share: libadwaita rows set only when their value changed.

use std::cell::RefCell;

use adw::prelude::*;
use gtk::glib;

use crate::widgets::{plain, set_text};

/// How much a line under a form matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    /// Body text, at full strength.
    Plain,
    Info,
    Warning,
    Danger,
}

impl Tone {
    fn class(self) -> Option<&'static str> {
        match self {
            Tone::Plain => None,
            Tone::Info => Some("dim-label"),
            Tone::Warning => Some("warning"),
            Tone::Danger => Some("error"),
        }
    }
}

/// Lines of text in their tones, rebuilt only when they change.
pub struct Notes {
    root: gtk::Box,
    shown: RefCell<Vec<(Tone, String)>>,
}

impl Notes {
    pub fn new() -> Notes {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 2);
        root.set_visible(false);
        Notes {
            root,
            shown: RefCell::default(),
        }
    }

    pub fn root(&self) -> &gtk::Box {
        &self.root
    }

    pub fn show(&self, notes: Vec<(Tone, String)>) {
        if *self.shown.borrow() == notes {
            return;
        }
        while let Some(child) = self.root.first_child() {
            self.root.remove(&child);
        }
        for (tone, text) in &notes {
            let label = gtk::Label::builder()
                .label(plain(text).as_ref())
                .xalign(0.0)
                .wrap(true)
                .wrap_mode(gtk::pango::WrapMode::WordChar)
                .selectable(true)
                .build();
            if let Some(class) = tone.class() {
                label.add_css_class(class);
            }
            self.root.append(&label);
        }
        self.root.set_visible(!notes.is_empty());
        *self.shown.borrow_mut() = notes;
    }
}

/// A titled group of rows.
pub fn group(title: &str) -> adw::PreferencesGroup {
    adw::PreferencesGroup::builder().title(title).build()
}

/// A `ComboRow` over `labels`.
pub fn combo(title: &str, id: &str, labels: &[&str]) -> adw::ComboRow {
    let row = adw::ComboRow::builder()
        .title(title)
        .model(&gtk::StringList::new(labels))
        .build();
    row.set_widget_name(id);
    row
}

pub fn switch(title: &str, id: &str) -> adw::SwitchRow {
    let row = adw::SwitchRow::builder().title(title).build();
    row.set_widget_name(id);
    row
}

pub fn spin(title: &str, id: &str, lo: f64, hi: f64) -> adw::SpinRow {
    let row = adw::SpinRow::with_range(lo, hi, 1.0);
    row.set_title(title);
    row.set_widget_name(id);
    row
}

/// An `ActionRow` showing a path, and a button at its end.
pub fn path_row(title: &str, button: &gtk::Button) -> adw::ActionRow {
    button.set_valign(gtk::Align::Center);
    let row = adw::ActionRow::builder()
        .title(title)
        .subtitle_lines(2)
        .build();
    row.add_suffix(button);
    row
}

pub fn button(label: &str, id: &str) -> gtk::Button {
    let button = gtk::Button::with_label(label);
    button.set_widget_name(id);
    button
}

pub fn set_combo(row: &adw::ComboRow, index: u32) {
    if row.selected() != index {
        row.set_selected(index);
    }
}

/// Replace a `ComboRow`'s choices, leaving it alone when they are the same.
pub fn set_labels(row: &adw::ComboRow, labels: &[String]) {
    let list = row.model().and_downcast::<gtk::StringList>();
    let same = list.as_ref().is_some_and(|l| {
        l.n_items() as usize == labels.len()
            && labels
                .iter()
                .enumerate()
                .all(|(i, s)| l.string(i as u32).as_deref() == Some(plain(s).as_ref()))
    });
    if !same {
        let plain: Vec<_> = labels.iter().map(|s| plain(s).into_owned()).collect();
        let refs: Vec<&str> = plain.iter().map(String::as_str).collect();
        row.set_model(Some(&gtk::StringList::new(&refs)));
    }
}

pub fn set_switch(row: &adw::SwitchRow, on: bool) {
    if row.is_active() != on {
        row.set_active(on);
    }
}

pub fn set_spin(row: &adw::SpinRow, value: f64) {
    if row.value() != value {
        row.set_value(value);
    }
}

pub fn set_entry(row: &adw::EntryRow, text: &str) {
    let text = plain(text);
    if row.text() != text.as_ref() {
        row.set_text(&text);
    }
}

pub fn set_subtitle(row: &impl IsA<adw::ActionRow>, text: &str) {
    // Pango markup in a subtitle: a path with an ampersand must stay a path.
    let text = glib::markup_escape_text(&plain(text));
    let row = row.as_ref();
    if row.subtitle().as_deref() != Some(text.as_str()) {
        row.set_subtitle(&text);
    }
}

/// A label under a group, left aligned and wrapping.
pub fn line() -> gtk::Label {
    let label = gtk::Label::builder()
        .xalign(0.0)
        .wrap(true)
        .wrap_mode(gtk::pango::WrapMode::WordChar)
        .build();
    label.set_visible(false);
    label
}

/// Show `text`, or hide the label.
pub fn set_line(label: &gtk::Label, text: Option<&str>) {
    label.set_visible(text.is_some());
    set_text(label, text.unwrap_or(""));
}

/// A grid of a partition table's rows: bounded by the table, and a cell here is never recycled.
pub fn grid() -> gtk::Grid {
    gtk::Grid::builder()
        .column_spacing(12)
        .row_spacing(4)
        .margin_top(4)
        .build()
}

pub fn clear_grid(grid: &gtk::Grid) {
    while let Some(child) = grid.first_child() {
        grid.remove(&child);
    }
}

/// A column heading.
pub fn head(grid: &gtk::Grid, column: i32, title: &str, hint: Option<&str>) {
    let label = gtk::Label::builder().label(title).xalign(0.0).build();
    label.add_css_class("heading");
    label.set_tooltip_text(hint);
    grid.attach(&label, column, 0, 1, 1);
}

/// A plain cell's label.
pub fn cell(xalign: f32) -> gtk::Label {
    gtk::Label::builder().xalign(xalign).build()
}

/// A page of groups, its scroll, and a width that reads well.
pub fn page(content: &gtk::Box) -> gtk::ScrolledWindow {
    content.set_margin_top(12);
    content.set_margin_bottom(12);
    content.set_margin_start(12);
    content.set_margin_end(12);
    let clamp = adw::Clamp::builder()
        .maximum_size(960)
        .tightening_threshold(720)
        .child(content)
        .build();
    gtk::ScrolledWindow::builder()
        .child(&clamp)
        .hscrollbar_policy(gtk::PolicyType::Never)
        .vexpand(true)
        .build()
}
