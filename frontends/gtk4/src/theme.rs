//! Colours the shell paints itself; everything else comes from the GTK theme.

/// An opaque colour, each channel from 0 to 255.
pub type Rgb = (u8, u8, u8);

const DARK_FILLS: [Rgb; 8] = [
    (62, 118, 166),
    (74, 129, 52),
    (151, 103, 56),
    (170, 87, 155),
    (52, 125, 125),
    (127, 116, 46),
    (129, 100, 184),
    (172, 94, 94),
];
const LIGHT_FILLS: [Rgb; 8] = [
    (76, 140, 196),
    (106, 182, 76),
    (196, 136, 76),
    (194, 99, 176),
    (76, 176, 176),
    (196, 180, 76),
    (148, 116, 209),
    (196, 108, 108),
];

pub fn is_dark() -> bool {
    adw::StyleManager::default().is_dark()
}

/// A partition's fill, wrapping around the palette; the label on it is [`on_partition`].
pub fn partition_fill(color_index: usize) -> Rgb {
    let table = if is_dark() { &DARK_FILLS } else { &LIGHT_FILLS };
    table[color_index % table.len()]
}

pub fn on_partition() -> Rgb {
    if is_dark() {
        (255, 255, 255)
    } else {
        (25, 25, 25)
    }
}

pub fn free_space() -> Rgb {
    if is_dark() {
        (60, 60, 60)
    } else {
        (225, 225, 228)
    }
}

pub fn dimmed() -> Rgb {
    if is_dark() {
        (45, 45, 45)
    } else {
        (238, 238, 240)
    }
}

/// Make `rgb` the cairo source colour.
pub fn set_source(cr: &gtk::cairo::Context, rgb: Rgb) {
    let channel = |c: u8| f64::from(c) / 255.0;
    cr.set_source_rgb(channel(rgb.0), channel(rgb.1), channel(rgb.2));
}

const CSS: &str = "
.rb-list > listview > row > cell { padding: 0; }
.rb-list row:focus, .rb-list row:focus-visible, .rb-list cell:focus-visible { outline: none; }
.rb-row { padding: 3px 6px; }
.rb-cursor { box-shadow: inset 0 1px @accent_color, inset 0 -1px @accent_color; }
.rb-added { color: @success_color; }
.rb-changed, .rb-unresolved { color: @accent_color; }
.rb-deleted { opacity: 0.6; }
.rb-tree-focus { outline: 1px solid @accent_color; outline-offset: -1px; }
.rb-image { color: @accent_color; }
.rb-crumb { padding: 0 2px; min-height: 0; min-width: 0; }
.rb-pane { border-top: 2px solid transparent; }
.rb-pane-active { border-top-color: @accent_color; }
.rb-drop { outline: 2px solid @accent_color; outline-offset: -2px; }
";

/// The shell's few style rules; colours stay the theme's named ones.
pub fn install_css() {
    // A selectable label takes a window's first focus, and a full selection reads as a highlight.
    if let Some(settings) = gtk::Settings::default() {
        settings.set_gtk_label_select_on_focus(false);
    }
    let Some(display) = gtk::gdk::Display::default() else {
        return;
    };
    let provider = gtk::CssProvider::new();
    provider.load_from_string(CSS);
    gtk::style_context_add_provider_for_display(
        &display,
        &provider,
        gtk::STYLE_PROVIDER_PRIORITY_APPLICATION,
    );
}
