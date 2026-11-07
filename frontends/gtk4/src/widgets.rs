//! Small helpers every screen uses to touch a widget only when its value changed.

use adw::prelude::*;
use gtk::glib;

/// Set a plain label, leaving it alone when it already says `text`.
pub fn set_text(label: &gtk::Label, text: &str) {
    if label.label() != text {
        label.set_label(text);
    }
}

/// Set a label's Pango markup, leaving it alone when unchanged.
pub fn set_markup(label: &gtk::Label, markup: &str) {
    if label.label() != markup {
        label.set_markup(markup);
    }
}

/// `<b>name</b> value`, with both escaped for markup.
pub fn fact(name: &str, value: &str) -> String {
    format!(
        "<b>{}</b> {}",
        glib::markup_escape_text(name),
        glib::markup_escape_text(value)
    )
}

/// Keep a button for a dialog that is not built yet: greyed out, with a tooltip saying so.
pub fn coming(button: &gtk::Button, what: &str) {
    button.set_sensitive(false);
    button.set_tooltip_text(Some(&format!("Coming soon: {what}")));
}
