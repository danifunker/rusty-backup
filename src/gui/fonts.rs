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

    /// The Mac Roman repertoire surfaces in egui two ways: filesystem/archive
    /// filenames (browse view, Archives tab) and the type/creator OSType codes
    /// the Archives tab renders via `fs::hfs::format_ostype`. egui's default
    /// font doesn't cover the symbol/punctuation half of Mac Roman, so the
    /// bundled Noto fallback must — or those names/codes tofu. Guards against a
    /// future font swap silently dropping coverage.
    ///
    /// The lone exception is the Apple-logo glyph (Mac Roman `0xF0` → U+F8FF),
    /// a private-use code point no standard font carries; it tofus regardless
    /// and is out of scope here.
    #[test]
    fn bundled_font_covers_mac_roman_symbols() {
        let font = FontRef::try_from_slice(NOTO_CJK_JP).expect("bundled CJK font must parse");
        // The non-ASCII Mac Roman glyphs that show up in real names / OSType
        // codes: bullet, trademark, registered, florin, accents, math symbols.
        for ch in [
            '\u{2022}', '\u{2122}', '\u{00AE}', '\u{0192}', '\u{00A9}', '\u{00B0}', '\u{00A2}',
            '\u{00A3}', '\u{00A7}', '\u{00B6}', '\u{2020}', '\u{2021}', '\u{2026}', '\u{00E9}',
            '\u{00FC}', '\u{00F1}', '\u{2206}', '\u{03A9}', '\u{220F}', '\u{222B}', '\u{25CA}',
            '\u{FB01}',
        ] {
            assert_ne!(
                font.glyph_id(ch).0,
                0,
                "bundled font is missing a glyph for U+{:04X} '{ch}' — OSType codes / Mac names would tofu",
                ch as u32
            );
        }
    }
}
