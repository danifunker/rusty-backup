//! Small helpers every screen uses to touch a widget only when its value changed.

use std::borrow::Cow;

use adw::prelude::*;
use gtk::glib;

/// `text` as GTK can take it: names read off a disk can hold NUL (HFS+'s private folder does).
pub fn plain(text: &str) -> Cow<'_, str> {
    match text.contains('\0') {
        true => Cow::Owned(text.replace('\0', "?")),
        false => Cow::Borrowed(text),
    }
}

/// Set a plain label, leaving it alone when it already says `text`.
pub fn set_text(label: &gtk::Label, text: &str) {
    let text = plain(text);
    if label.label() != text.as_ref() {
        label.set_label(&text);
    }
}

/// Set a label's Pango markup, leaving it alone when unchanged.
pub fn set_markup(label: &gtk::Label, markup: &str) {
    let markup = plain(markup);
    if label.label() != markup.as_ref() {
        label.set_markup(&markup);
    }
}

/// `<b>name</b> value`, with both escaped for markup.
pub fn fact(name: &str, value: &str) -> String {
    format!(
        "<b>{}</b> {}",
        glib::markup_escape_text(&plain(name)),
        glib::markup_escape_text(&plain(value))
    )
}

/// Text boxes and open menus keep their keys, wherever the window's shortcuts are read.
pub fn typing(widget: &impl IsA<gtk::Widget>) -> bool {
    let focus = widget.root().and_then(|r| r.focus());
    focus.is_some_and(|f| f.is::<gtk::Text>() || f.ancestor(gtk::Popover::static_type()).is_some())
}

/// Keep a button for a dialog that is not built yet: greyed out, with a tooltip saying so.
pub fn coming(button: &gtk::Button, what: &str) {
    button.set_sensitive(false);
    button.set_tooltip_text(Some(&format!("Coming soon: {what}")));
}

/// A row of controls that wraps when the window is narrow.
pub fn flow() -> gtk::FlowBox {
    gtk::FlowBox::builder()
        .selection_mode(gtk::SelectionMode::None)
        .homogeneous(false)
        .max_children_per_line(16)
        .column_spacing(4)
        .row_spacing(4)
        .build()
}

/// A hidden widget's `FlowBoxChild` stays behind, so hide that too.
pub fn flow_show(widget: &impl IsA<gtk::Widget>, on: bool) {
    widget.set_visible(on);
    if let Some(child) = widget.parent() {
        child.set_visible(on);
    }
}

/// Buttons under `root` leave the keyboard where it was when clicked, so a toolbar click keeps the listing's keys.
pub fn keep_focus(root: &impl IsA<gtk::Widget>) {
    let mut child = root.first_child();
    while let Some(widget) = child {
        if widget.is::<gtk::Button>() {
            widget.set_focus_on_click(false);
        }
        keep_focus(&widget);
        child = widget.next_sibling();
    }
}
