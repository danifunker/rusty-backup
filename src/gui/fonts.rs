//! Bundled font setup for the egui GUI.
//!
//! egui's default font has no CJK glyphs, so Japanese (Shift-JIS) Human68k
//! filenames — and any other CJK text a filesystem hands us — render as
//! tofu boxes. We embed Noto Sans CJK JP (SIL Open Font License 1.1; the
//! license ships alongside the font at `assets/fonts/LICENSE-OFL.txt`) and
//! register it as a *fallback* on both the proportional and monospace
//! families: Latin keeps the default look, CJK code points resolve from
//! Noto.

use std::sync::Arc;

use egui::{FontData, FontDefinitions, FontFamily};

/// Full Noto Sans CJK JP (covers all of JIS X 0208/0213 plus shared Han),
/// embedded at compile time. ~16 MB — only linked into the GUI binary.
const NOTO_CJK_JP: &[u8] = include_bytes!("../../assets/fonts/NotoSansCJKjp-Regular.otf");

const NOTO_KEY: &str = "noto-sans-cjk-jp";

/// Register the bundled CJK font as a fallback so Japanese filenames render
/// in the browse view. Call once at startup with the eframe creation
/// context's `egui_ctx`.
pub fn install_cjk_font(ctx: &egui::Context) {
    let mut fonts = FontDefinitions::default();
    fonts.font_data.insert(
        NOTO_KEY.to_owned(),
        Arc::new(FontData::from_static(NOTO_CJK_JP)),
    );

    // Append (not prepend) so the default font is preferred for Latin and
    // Noto only fills in glyphs the default lacks (i.e. CJK).
    for family in [FontFamily::Proportional, FontFamily::Monospace] {
        fonts
            .families
            .entry(family)
            .or_default()
            .push(NOTO_KEY.to_owned());
    }

    ctx.set_fonts(fonts);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ab_glyph::{Font, FontRef};

    #[test]
    fn bundled_font_parses_and_covers_japanese() {
        let font = FontRef::try_from_slice(NOTO_CJK_JP).expect("bundled CJK font must parse");
        // hiragana, two kanji, half-width katakana — all must resolve to a
        // real (non-.notdef) glyph or Japanese filenames would still tofu.
        for ch in ['あ', '日', '本', 'ｼ'] {
            assert_ne!(
                font.glyph_id(ch).0,
                0,
                "bundled font is missing a glyph for U+{:04X} '{ch}'",
                ch as u32
            );
        }
    }
}
