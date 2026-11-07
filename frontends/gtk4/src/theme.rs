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
