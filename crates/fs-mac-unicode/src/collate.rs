//! Catalog-name ordering for HFS (Mac Roman, Apple's `RelString` ranks) and HFS+ (Apple's
//! case-fold and decomposition tables in [`crate::hfs_unicode`]).

use std::cmp::Ordering;

use crate::hfs_charorder::HFS_RANK;

/// Compare two HFS+ catalog keys: parent CNID, then the names as Apple's `FastUnicodeCompare`
/// does (fold each unit, skip ignorables); names must already be decomposed (`decompose_str`).
pub fn compare_hfsplus_keys(
    parent_a: u32,
    name_a: &[u16],
    parent_b: u32,
    name_b: &[u16],
    case_sensitive: bool,
) -> Ordering {
    match parent_a.cmp(&parent_b) {
        Ordering::Equal => {}
        other => return other,
    }
    if case_sensitive {
        // HFSX binary comparison — every code point is significant,
        // including NULs. (Apple's HFSX uses straight UTF-16 binary
        // compare, no folding, no ignore set.)
        return name_a.cmp(name_b);
    }
    let fold = |u: &u16| crate::hfs_unicode::case_fold(*u);
    name_a
        .iter()
        .map(fold)
        .filter(|&f| f != 0)
        .cmp(name_b.iter().map(fold).filter(|&f| f != 0))
}

/// Classic HFS keys as the File Manager orders them: parent ID, then names rank by rank
/// ([`HFS_RANK`], Apple's `RelString`), the shorter first on a common prefix.
pub fn compare_hfs_keys(parent_a: u32, name_a: &[u8], parent_b: u32, name_b: &[u8]) -> Ordering {
    let rank = |&b: &u8| HFS_RANK[b as usize];
    parent_a
        .cmp(&parent_b)
        .then_with(|| name_a.iter().map(rank).cmp(name_b.iter().map(rank)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Key comparison --

    #[test]
    fn test_compare_hfsplus_keys_parent_ordering() {
        let name: Vec<u16> = "test".encode_utf16().collect();
        assert_eq!(
            compare_hfsplus_keys(1, &name, 2, &name, false),
            Ordering::Less
        );
        assert_eq!(
            compare_hfsplus_keys(2, &name, 1, &name, false),
            Ordering::Greater
        );
        assert_eq!(
            compare_hfsplus_keys(2, &name, 2, &name, false),
            Ordering::Equal
        );
    }

    #[test]
    fn test_compare_hfsplus_keys_case_insensitive() {
        let a: Vec<u16> = "Hello".encode_utf16().collect();
        let b: Vec<u16> = "hello".encode_utf16().collect();
        assert_eq!(compare_hfsplus_keys(2, &a, 2, &b, false), Ordering::Equal);
    }

    /// HFS+ case-folds to LOWERCASE (Apple `FastUnicodeCompare` / TN1150), so a
    /// character between the uppercase and lowercase ASCII blocks — notably the
    /// underscore `_` (0x5F) — sorts BEFORE letters, not after the uppercased
    /// ones. These exact pairs come from a real Mac OS 9.2.2 HFS+ volume that
    /// the previous uppercase fold flagged as `KeyOutOfOrder`.
    #[test]
    fn test_compare_hfsplus_keys_underscore_collation() {
        let k = |s: &str| -> Vec<u16> { s.encode_utf16().collect() };
        for (a, b) in [
            ("as_OpenURL", "asVers.htm"),
            ("wr_single_pixel.gif", "wrApp.gif"),
            ("high_priority.gif", "highest_priority.gif"),
            ("not_valid.gif", "note.gif"),
        ] {
            assert_eq!(
                compare_hfsplus_keys(2, &k(a), 2, &k(b), false),
                Ordering::Less,
                "{a:?} must sort before {b:?} (lowercase fold)"
            );
        }
        // Still case-insensitive.
        assert_eq!(
            compare_hfsplus_keys(2, &k("README"), 2, &k("readme"), false),
            Ordering::Equal
        );
    }

    /// Apple's TN1150 tables differ from Rust's `to_lowercase` + NFD on exotic
    /// code points; these cases pin the table-faithful behavior.
    #[test]
    fn test_hfsplus_unicode_tables_match_apple() {
        use crate::hfs_unicode::decompose_str;
        let k = |s: &str| -> Vec<u16> { s.encode_utf16().collect() };

        // ß (0xDF) folds 1:1 — it is NOT expanded to "ss" (as `to_uppercase`
        // would), so "straße" and "strasse" are different names.
        assert_ne!(
            compare_hfsplus_keys(2, &k("stra\u{df}e"), 2, &k("strasse"), false),
            Ordering::Equal
        );

        // Apple does NOT decompose the OHM sign U+2126 (TN1150 excludes
        // U+2000..U+2FFF), unlike Unicode NFD which maps it to Greek capital
        // Omega U+03A9.
        assert_eq!(decompose_str("\u{2126}"), vec![0x2126]);
        // It DOES canonically decompose precomposed Latin and Hangul.
        assert_eq!(decompose_str("\u{e9}"), vec![0x0065, 0x0301]); // é
        assert_eq!(decompose_str("\u{ac00}"), vec![0x1100, 0x1161]); // 가
        assert_eq!(decompose_str("\u{d4db}"), vec![0x1111, 0x1171, 0x11b6]); // 3-jamo Hangul
    }

    #[test]
    fn test_compare_hfsplus_keys_case_sensitive() {
        let a: Vec<u16> = "Hello".encode_utf16().collect();
        let b: Vec<u16> = "hello".encode_utf16().collect();
        // 'H' (0x0048) < 'h' (0x0068)
        assert_eq!(compare_hfsplus_keys(2, &a, 2, &b, true), Ordering::Less);
    }

    #[test]
    fn test_compare_hfsplus_keys_nfd() {
        // Keys are decomposed before they are compared, as on disk; the compare itself only folds.
        let a = crate::hfs_unicode::decompose_str("\u{E9}");
        let b: Vec<u16> = vec![0x0065, 0x0301];
        assert_eq!(compare_hfsplus_keys(2, &a, 2, &b, false), Ordering::Equal);
        let raw: Vec<u16> = vec![0x00E9];
        assert_ne!(compare_hfsplus_keys(2, &raw, 2, &b, false), Ordering::Equal);
    }

    /// Units an older writer stored precomposed (U+0218) sort by their own value, as
    /// `FastUnicodeCompare` sorts them, not by a decomposition (MU-002).
    #[test]
    fn test_compare_hfsplus_keys_does_not_decompose_stored_units() {
        let stored = [0x0218u16, b'a' as u16];
        assert_eq!(
            compare_hfsplus_keys(2, &stored, 2, &[b't' as u16], false),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_hfsplus_keys_private_data_dir_sorts_last() {
        // Apple's FastUnicodeCompare maps U+0000 to 0xFFFF, so a name
        // starting with NULs sorts AFTER all printable names with the
        // same parent CNID. This is how `\0\0\0\0HFS+ Private Data`
        // ends up at the END of root's children on a Mac-formatted
        // volume rather than the front. Empirically verified against
        // OS-formatted images (HFSTest6/TestHFS6-OSCopy.dmg, 2026-05).
        // Pre-fix, our compare folded NULs as ignorable and our inserts
        // sorted the dir before printable names — fsck_hfs flagged this
        // as "key for HFS+ Private Data is out of order."
        let private: Vec<u16> = "\u{0000}\u{0000}\u{0000}\u{0000}HFS+ Private Data"
            .encode_utf16()
            .collect();
        let alpha: Vec<u16> = "Apple".encode_utf16().collect();
        let dotted: Vec<u16> = ".HFS+ Private Directory Data\u{D}".encode_utf16().collect();
        let later: Vec<u16> = "tone.wav".encode_utf16().collect();
        let way_later: Vec<u16> = "zzz".encode_utf16().collect();

        // All printable parent=2 entries must sort BEFORE NUL+name.
        for (label, other) in [
            ("Apple", &alpha),
            (".HFS+ Private Directory Data\\r", &dotted),
            ("tone.wav", &later),
            ("zzz", &way_later),
        ] {
            assert_eq!(
                compare_hfsplus_keys(2, other, 2, &private, false),
                Ordering::Less,
                "'{label}' must sort before NUL-prefixed private dir"
            );
            assert_eq!(
                compare_hfsplus_keys(2, &private, 2, other, false),
                Ordering::Greater,
                "NUL-prefixed private dir must sort after '{label}'"
            );
        }
    }

    #[test]
    fn test_compare_hfsplus_keys_hfsx_binary_keeps_nuls() {
        // Case-sensitive HFSX uses straight UTF-16 binary compare —
        // NULs are preserved as 0x0000 there. A leading-NUL name MUST
        // sort before a printable name in HFSX (binary order).
        let nul_prefixed: Vec<u16> = "\u{0000}HFS".encode_utf16().collect();
        let plain: Vec<u16> = "HFS".encode_utf16().collect();
        assert_eq!(
            compare_hfsplus_keys(2, &nul_prefixed, 2, &plain, true),
            Ordering::Less
        );
    }

    /// Answers Apple's `RelString` gives (Mac Roman bytes): case-blind, accent-aware.
    #[test]
    fn test_compare_hfs_keys_follows_relstring() {
        let cmp = |a: &[u8], b: &[u8]| compare_hfs_keys(2, a, 2, b);
        assert_eq!(cmp(b"a", b"A"), Ordering::Equal);
        assert_eq!(cmp(b"\x83", b"\x8E"), Ordering::Equal, "E-acute == e-acute");
        assert_eq!(cmp(b"e", b"\x8E"), Ordering::Less, "e < e-acute");
        assert_eq!(cmp(b"B\x8Ezier", b"Bind"), Ordering::Less, "e-acute < i");
        assert_eq!(
            cmp(b"\xCA", b" "),
            Ordering::Equal,
            "non-breaking space == space"
        );
        assert_eq!(cmp(b"Z", b"_"), Ordering::Less);
        assert_eq!(cmp(b"ab", b"abc"), Ordering::Less);
        assert_eq!(compare_hfs_keys(1, b"z", 2, b"a"), Ordering::Less);
    }

    #[test]
    fn test_compare_hfs_keys_case_insensitive() {
        assert_eq!(compare_hfs_keys(2, b"Hello", 2, b"hello"), Ordering::Equal);
        assert_eq!(compare_hfs_keys(2, b"abc", 2, b"ABD"), Ordering::Less);
    }

    /// Mac Roman collation must match classic Mac OS (`_RelString`), not raw
    /// byte order — the cases below were lifted straight from a real
    /// Apple-formatted MacPack disk that rusty-backup's old (ASCII-only) table
    /// flagged as `KeysOutOfOrder`. é/â/ü etc. sort next to their base letter,
    /// curly quotes / dashes / the non-breaking space sort as punctuation, and
    /// the comparison stays case-insensitive but diacritical-sensitive.
    #[test]
    fn test_compare_hfs_keys_mac_roman_collation() {
        // Accented letter sorts right after its base letter (é after e),
        // so "Bézier" < "Bind" (é < i), not after 'Z' as a raw 0x8e would.
        let bezier = b"B\x8ezier Text"; // 0x8e = é
        assert_eq!(compare_hfs_keys(2, bezier, 2, b"Bind"), Ordering::Less);
        // Leading curly double-quote (0xD2) sorts as punctuation, before 'E'.
        let curly = b"\xd2Extension List\xd3 Docs";
        assert_eq!(
            compare_hfs_keys(2, curly, 2, b"Extension List 1.0.2"),
            Ordering::Less
        );
        // Non-breaking space (0xCA) collates like a normal space, before 'b'.
        let nbsp = b"\xcaRead Me\xca";
        assert_eq!(compare_hfs_keys(2, nbsp, 2, b"bas.rl"), Ordering::Less);
        // Diacritical-sensitive: base < accented < next base ('A' < 'Ä' < 'B').
        assert_eq!(compare_hfs_keys(2, b"A", 2, b"\x80"), Ordering::Less); // 0x80 = Ä
        assert_eq!(compare_hfs_keys(2, b"\x80", 2, b"B"), Ordering::Less);
        // Case-insensitive across the high range too: Ä (0x80) == ä (0x8a).
        assert_eq!(compare_hfs_keys(2, b"\x80", 2, b"\x8a"), Ordering::Equal);
    }

    #[test]
    fn test_compare_hfs_keys_parent_ordering() {
        assert_eq!(compare_hfs_keys(1, b"test", 2, b"test"), Ordering::Less);
    }
}
