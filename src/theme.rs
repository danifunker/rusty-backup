//! Semantic colours that work in both light and dark mode.
//!
//! ## Why this exists
//!
//! The GUI accumulated ~180 hard-coded `Color32` literals, and every one was
//! picked against the dark background egui shows by default:
//! `from_rgb(255, 100, 100)` for errors, `(100, 200, 100)` for success,
//! `(255, 200, 100)` and `YELLOW` for warnings, `GRAY` for secondary text.
//!
//! egui follows the OS theme, so on a light desktop the *chrome* turns light
//! while those literals stay where they are — pale red, pale green and yellow
//! on near-white. That is legible in the way a highlighter on paper is
//! legible, which is to say barely, and it is why the app was reported as
//! "almost unusable" in light mode.
//!
//! ## The rule
//!
//! Call these helpers instead of naming a colour. Each returns the variant for
//! the current theme, so a caller never has to ask which mode it is in:
//!
//! ```ignore
//! ui.colored_label(theme::danger(ui.visuals()), "cannot read partition");
//! ```
//!
//! Dark-mode values are the ones already in use, so dark mode looks exactly as
//! it did. Light-mode values are darker and more saturated — the same hue at a
//! contrast that survives a white background.
//!
//! For plain body text prefer egui's own `ui.visuals().text_color()`; these are
//! for text and marks that carry *meaning* through colour.

use egui::{Color32, Visuals};

/// Errors, destructive actions, "this will be overwritten" warnings.
///
/// Dark keeps the existing `(255, 100, 100)`. Light drops to a deep red that
/// stays red rather than turning pink against white.
pub fn danger(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::from_rgb(255, 100, 100)
    } else {
        Color32::from_rgb(170, 30, 30)
    }
}

/// A softer danger, for a row that is merely suspect rather than broken —
/// the old `(220, 120, 120)`.
pub fn danger_muted(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::from_rgb(220, 120, 120)
    } else {
        Color32::from_rgb(150, 60, 60)
    }
}

/// Cautions that are not failures: lossy conversions, skipped entries,
/// "this filesystem is read-only". Was `(255, 200, 100)` / `YELLOW`.
///
/// Yellow is the worst offender on white — pure `YELLOW` is effectively
/// invisible — so the light variant is a brown-amber, not a yellow.
pub fn warning(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::from_rgb(255, 200, 100)
    } else {
        Color32::from_rgb(140, 85, 0)
    }
}

/// Completion, verification passed, "checksum matched". Was `(100, 200, 100)`.
pub fn success(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::from_rgb(100, 200, 100)
    } else {
        Color32::from_rgb(20, 110, 40)
    }
}

/// Informational emphasis and in-app links. Was `(120, 160, 220)` /
/// `(150, 190, 255)`.
pub fn info(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::from_rgb(150, 190, 255)
    } else {
        Color32::from_rgb(30, 85, 175)
    }
}

/// Secondary text — hints, byte counts, the dimmer half of a two-tone row.
/// Was `Color32::GRAY`, which is mid-grey and therefore weak against *both*
/// backgrounds; this leans it the right way for each.
pub fn muted(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::from_rgb(160, 160, 160)
    } else {
        Color32::from_rgb(95, 95, 95)
    }
}

/// Accent for selected / active decoration that is not a widget state.
/// Was `(110, 210, 190)`.
pub fn accent(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::from_rgb(110, 210, 190)
    } else {
        Color32::from_rgb(15, 120, 105)
    }
}

/// Partition-bar segment fills, indexed by partition number.
///
/// These are block fills with a label drawn on top, so the requirement is
/// inverted from body text: the FILL must contrast with the LABEL. Pair with
/// [`on_partition`] for that label — never assume white.
pub fn partition_fill(visuals: &Visuals, color_index: usize) -> Color32 {
    // The eight hues are the ones the bar always used. The dark values are
    // darkened from those originals because the test below measures them and
    // the originals failed badly against their own white labels — gold at
    // 2.10:1, green 2.50, teal 2.58, blue 3.59. The old code asserted in a
    // comment that "white reads on all of them"; against half of them it did
    // not, in dark mode, before light mode entered into it.
    const DARK: [Color32; 8] = [
        Color32::from_rgb(62, 118, 166),  // blue
        Color32::from_rgb(74, 129, 52),   // green
        Color32::from_rgb(151, 103, 56),  // orange
        Color32::from_rgb(170, 87, 155),  // magenta
        Color32::from_rgb(52, 125, 125),  // teal
        Color32::from_rgb(127, 116, 46),  // gold
        Color32::from_rgb(129, 100, 184), // violet
        Color32::from_rgb(172, 94, 94),   // salmon
    ];
    // Light mode carries a near-black label instead, so the fills stay close to
    // the original hues — they only had to clear AA against ink, not white.
    const LIGHT: [Color32; 8] = [
        Color32::from_rgb(76, 140, 196),  // blue
        Color32::from_rgb(106, 182, 76),  // green
        Color32::from_rgb(196, 136, 76),  // orange
        Color32::from_rgb(194, 99, 176),  // magenta
        Color32::from_rgb(76, 176, 176),  // teal
        Color32::from_rgb(196, 180, 76),  // gold
        Color32::from_rgb(148, 116, 209), // violet
        Color32::from_rgb(196, 108, 108), // salmon
    ];
    let table = if visuals.dark_mode { &DARK } else { &LIGHT };
    table[color_index % table.len()]
}

/// Label colour for text drawn on top of a [`partition_fill`].
///
/// The old code returned `WHITE` unconditionally, with the comment "palette
/// colors are mid-tone; white reads on all of them". True of the dark
/// mid-tones, false of the light tints, where white on pale blue is unreadable.
pub fn on_partition(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::WHITE
    } else {
        Color32::from_rgb(25, 25, 25)
    }
}

/// Unallocated space in the partition bar.
pub fn free_space(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::from_rgb(60, 60, 60)
    } else {
        Color32::from_rgb(225, 225, 228)
    }
}

/// A segment shown greyed — out of scope for the current operation.
pub fn dimmed(visuals: &Visuals) -> Color32 {
    if visuals.dark_mode {
        Color32::from_rgb(45, 45, 45)
    } else {
        Color32::from_rgb(238, 238, 240)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Relative luminance per WCAG 2.1, used to check a colour is on the right
    /// side of its background rather than eyeballing hex values.
    fn luminance(c: Color32) -> f32 {
        let f = |v: u8| {
            let s = v as f32 / 255.0;
            if s <= 0.03928 {
                s / 12.92
            } else {
                ((s + 0.055) / 1.055).powf(2.4)
            }
        };
        0.2126 * f(c.r()) + 0.7152 * f(c.g()) + 0.0722 * f(c.b())
    }

    fn contrast(a: Color32, b: Color32) -> f32 {
        let (la, lb) = (luminance(a), luminance(b));
        let (hi, lo) = if la > lb { (la, lb) } else { (lb, la) };
        (hi + 0.05) / (lo + 0.05)
    }

    /// The bug this module exists for: every semantic colour must be readable
    /// against the background of its own theme. 4.5:1 is the WCAG AA threshold
    /// for body text.
    ///
    /// Run against the *old* hard-coded values this fails on light mode for
    /// warning (yellow on white is ~1.1:1), success and danger — which is
    /// precisely the reported symptom.
    #[test]
    fn every_semantic_colour_is_readable_in_both_themes() {
        let dark = Visuals::dark();
        let light = Visuals::light();
        let dark_bg = Color32::from_rgb(27, 27, 27);
        let light_bg = Color32::WHITE;

        type Accessor = fn(&Visuals) -> Color32;
        let cases: [(&str, Accessor); 7] = [
            ("danger", danger),
            ("danger_muted", danger_muted),
            ("warning", warning),
            ("success", success),
            ("info", info),
            ("muted", muted),
            ("accent", accent),
        ];
        for (name, f) in cases {
            let d = contrast(f(&dark), dark_bg);
            let l = contrast(f(&light), light_bg);
            assert!(d >= 4.5, "{name} on dark: {d:.2}:1 (need 4.5)");
            assert!(l >= 4.5, "{name} on light: {l:.2}:1 (need 4.5)");
        }
    }

    /// Partition labels are drawn over the fill, so that pairing has to hold
    /// too — this is the one the old `WHITE` constant got wrong.
    #[test]
    fn partition_labels_are_readable_on_their_own_fill() {
        for visuals in [Visuals::dark(), Visuals::light()] {
            for i in 0..8 {
                let c = contrast(on_partition(&visuals), partition_fill(&visuals, i));
                assert!(
                    c >= 4.5,
                    "label on fill {i} ({}): {c:.2}:1",
                    if visuals.dark_mode { "dark" } else { "light" }
                );
            }
        }
    }

    /// A caller indexing past the end wraps rather than panicking — partition
    /// counts are not bounded by the palette length.
    #[test]
    fn partition_fill_wraps_past_the_palette() {
        let v = Visuals::dark();
        assert_eq!(partition_fill(&v, 0), partition_fill(&v, 8));
    }
}
