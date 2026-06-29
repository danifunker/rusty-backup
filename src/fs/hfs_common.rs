//! Shared helpers for HFS and HFS+ filesystem editing.
//!
//! Provides big-endian bitmap operations, Mac epoch time utilities,
//! type/creator code lookup, B-tree key comparison, and B-tree node
//! manipulation (insert, remove, split, grow root).

use byteorder::{BigEndian, ByteOrder};
use std::cmp::Ordering;
use std::sync::OnceLock;

use super::filesystem::FilesystemError;

// ---------------------------------------------------------------------------
// Big-endian bitmap operations (HFS/HFS+ bitmaps are MSB-first)
// ---------------------------------------------------------------------------

/// Test whether the bit at `index` is set in a big-endian bitmap.
#[inline]
#[allow(dead_code)]
pub fn bitmap_test_bit_be(data: &[u8], index: u32) -> bool {
    let byte_idx = index as usize / 8;
    let bit_idx = 7 - (index % 8);
    byte_idx < data.len() && (data[byte_idx] >> bit_idx) & 1 == 1
}

/// Set the bit at `index` in a big-endian bitmap.
#[inline]
pub fn bitmap_set_bit_be(data: &mut [u8], index: u32) {
    let byte_idx = index as usize / 8;
    let bit_idx = 7 - (index % 8);
    data[byte_idx] |= 1u8 << bit_idx;
}

/// Clear the bit at `index` in a big-endian bitmap.
#[inline]
pub fn bitmap_clear_bit_be(data: &mut [u8], index: u32) {
    let byte_idx = index as usize / 8;
    let bit_idx = 7 - (index % 8);
    data[byte_idx] &= !(1u8 << bit_idx);
}

#[allow(dead_code)]
/// Find the first set (1) bit in a big-endian bitmap within `max_bits`.
/// ProDOS uses bit=1 for FREE blocks, so this finds the first free block.
pub fn bitmap_find_set_bit_be(data: &[u8], max_bits: u32) -> Option<u32> {
    (0..max_bits).find(|&bit| bitmap_test_bit_be(data, bit))
}

#[allow(dead_code)]
/// Find the first clear (0) bit in a big-endian bitmap within `max_bits`.
pub fn bitmap_find_clear_bit_be(data: &[u8], max_bits: u32) -> Option<u32> {
    (0..max_bits).find(|&bit| !bitmap_test_bit_be(data, bit))
}

/// Collect every contiguous run of clear bits in a big-endian bitmap as
/// `(start, length)` pairs. Used by the HFS+ multi-extent allocator
/// (largest-run-first) — see `allocate_extents` in `src/fs/hfsplus.rs`.
pub fn bitmap_collect_clear_runs_be(data: &[u8], max_bits: u32) -> Vec<(u32, u32)> {
    let mut runs = Vec::new();
    let mut run_start = 0u32;
    let mut run_len = 0u32;
    for bit in 0..max_bits {
        if bitmap_test_bit_be(data, bit) {
            if run_len > 0 {
                runs.push((run_start, run_len));
                run_len = 0;
            }
            run_start = bit + 1;
        } else {
            if run_len == 0 {
                run_start = bit;
            }
            run_len += 1;
        }
    }
    if run_len > 0 {
        runs.push((run_start, run_len));
    }
    runs
}

/// Find a contiguous run of `count` clear bits in a big-endian bitmap.
/// Returns the starting bit index, or None if no such run exists.
pub fn bitmap_find_clear_run_be(data: &[u8], max_bits: u32, count: u32) -> Option<u32> {
    if count == 0 {
        return Some(0);
    }
    let mut run_start = 0u32;
    let mut run_len = 0u32;
    for bit in 0..max_bits {
        if bitmap_test_bit_be(data, bit) {
            run_start = bit + 1;
            run_len = 0;
        } else {
            run_len += 1;
            if run_len >= count {
                return Some(run_start);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Mac epoch time
// ---------------------------------------------------------------------------

/// Seconds between 1904-01-01 00:00:00 UTC and 1970-01-01 00:00:00 UTC.
const MAC_EPOCH_DELTA: u64 = 2_082_844_800;

/// Returns the current time as seconds since the Mac epoch (1904-01-01).
pub fn hfs_now() -> u32 {
    let unix_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    (unix_secs + MAC_EPOCH_DELTA) as u32
}

/// Format an HFS/HFS+ Mac-epoch timestamp (seconds since 1904-01-01 UTC) as a
/// human-readable `YYYY-MM-DD HH:MM:SS` string. Returns `None` for a zero
/// timestamp ("no date set" on disk). Dates before the Unix epoch clamp to
/// 1970-01-01 — vintage Mac files are 1984+, so this is harmless in practice.
pub fn format_mac_date(mac_secs: u32) -> Option<String> {
    if mac_secs == 0 {
        return None;
    }
    let unix = mac_secs as i64 - MAC_EPOCH_DELTA as i64;
    Some(super::unix_common::inode::format_unix_timestamp(unix))
}

/// Parse a `YYYY-MM-DD HH:MM:SS` string (interpreted as UTC) into Mac-epoch
/// seconds. The inverse of [`format_mac_date`]; returns `None` when the string
/// doesn't parse or falls outside the representable range. An empty string (or
/// the literal `0`) maps to `0` ("no date set"), so a cleared field round-trips.
pub fn parse_mac_date(s: &str) -> Option<u32> {
    parse_mac_date_checked(s).ok()
}

/// Like [`parse_mac_date`] but returns a short, specific reason on failure
/// (e.g. "month 13 is out of range", "day 29 is invalid for 1997-02") so the
/// date editor can tell the user exactly what's wrong. Leap-year / days-per-month
/// validity is delegated to `chrono`'s `from_ymd_opt`.
pub fn parse_mac_date_checked(s: &str) -> Result<u32, String> {
    let s = s.trim();
    if s.is_empty() || s == "0" {
        return Ok(0);
    }
    let fmt = "expected YYYY-MM-DD HH:MM:SS";
    let (date, time) = s.split_once(' ').ok_or(fmt)?;
    let d: Vec<&str> = date.split('-').collect();
    let t: Vec<&str> = time.split(':').collect();
    if d.len() != 3 || t.len() != 3 {
        return Err(fmt.into());
    }
    let num = |x: &str, what: &str| -> Result<i64, String> {
        x.trim()
            .parse::<i64>()
            .map_err(|_| format!("'{}' is not a number ({what})", x.trim()))
    };
    let (y, mo, day) = (num(d[0], "year")?, num(d[1], "month")?, num(d[2], "day")?);
    let (h, mi, se) = (
        num(t[0], "hour")?,
        num(t[1], "minute")?,
        num(t[2], "second")?,
    );
    if !(1..=12).contains(&mo) {
        return Err(format!("month {mo} is out of range (1-12)"));
    }
    let nd = chrono::NaiveDate::from_ymd_opt(y as i32, mo as u32, day as u32)
        .ok_or_else(|| format!("day {day} is invalid for {y}-{mo:02}"))?;
    let nt = chrono::NaiveTime::from_hms_opt(h as u32, mi as u32, se as u32)
        .ok_or_else(|| format!("time {h:02}:{mi:02}:{se:02} is out of range"))?;
    let mac = chrono::NaiveDateTime::new(nd, nt).and_utc().timestamp() + MAC_EPOCH_DELTA as i64;
    if (0..=u32::MAX as i64).contains(&mac) {
        Ok(mac as u32)
    } else {
        Err("date is outside the representable range".into())
    }
}

// ---------------------------------------------------------------------------
// Type/creator lookup from hfs_file_types.json
// ---------------------------------------------------------------------------

/// Parsed extension-to-type/creator mapping.
struct ExtensionMap {
    entries: Vec<(String, [u8; 4], [u8; 4])>, // (ext, type_code, creator_code)
}

fn load_extension_map() -> &'static ExtensionMap {
    static MAP: OnceLock<ExtensionMap> = OnceLock::new();
    MAP.get_or_init(|| {
        let json_bytes = include_str!("../../assets/hfs_file_types.json");
        let parsed: serde_json::Value = serde_json::from_str(json_bytes).unwrap_or_default();
        let mut entries = Vec::new();
        if let Some(exts) = parsed.get("extensions").and_then(|v| v.as_object()) {
            for (ext, val) in exts {
                if ext.starts_with('_') {
                    continue;
                }
                if let (Some(tc), Some(cc)) = (
                    val.get("type").and_then(|v| v.as_str()),
                    val.get("creator").and_then(|v| v.as_str()),
                ) {
                    entries.push((ext.to_lowercase(), encode_fourcc(tc), encode_fourcc(cc)));
                }
            }
        }
        ExtensionMap { entries }
    })
}

/// A known (type, creator) pair with a friendly description, used to populate
/// the type/creator editor's pulldown.
#[derive(Debug, Clone)]
pub struct TypeCreatorEntry {
    pub type_code: [u8; 4],
    pub creator_code: [u8; 4],
    pub description: String,
}

impl TypeCreatorEntry {
    pub fn type_str(&self) -> String {
        String::from_utf8_lossy(&self.type_code).to_string()
    }
    pub fn creator_str(&self) -> String {
        String::from_utf8_lossy(&self.creator_code).to_string()
    }
}

fn load_type_creator_entries() -> &'static Vec<TypeCreatorEntry> {
    static ENTRIES: OnceLock<Vec<TypeCreatorEntry>> = OnceLock::new();
    ENTRIES.get_or_init(|| {
        let json_bytes = include_str!("../../assets/hfs_file_types.json");
        let parsed: serde_json::Value = serde_json::from_str(json_bytes).unwrap_or_default();

        // type code -> friendly description (from the "types" block)
        let mut type_desc: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        if let Some(types) = parsed.get("types").and_then(|v| v.as_object()) {
            for (tc, val) in types {
                if let Some(desc) = val.get("description").and_then(|v| v.as_str()) {
                    type_desc.insert(tc.clone(), desc.to_string());
                }
            }
        }

        // Curated (type, creator) catalog from the "entries" block — these
        // win on description because they're hand-written.
        let mut seen: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();
        let mut out: Vec<TypeCreatorEntry> = Vec::new();
        if let Some(arr) = parsed.get("entries").and_then(|v| v.as_array()) {
            for item in arr {
                let (Some(tc_str), Some(cc_str), Some(desc)) = (
                    item.get("type").and_then(|v| v.as_str()),
                    item.get("creator").and_then(|v| v.as_str()),
                    item.get("description").and_then(|v| v.as_str()),
                ) else {
                    continue;
                };
                if !seen.insert((tc_str.to_string(), cc_str.to_string())) {
                    continue;
                }
                out.push(TypeCreatorEntry {
                    type_code: encode_fourcc(tc_str),
                    creator_code: encode_fourcc(cc_str),
                    description: desc.to_string(),
                });
            }
        }
        // Fall back to extension-derived pairs for anything not already in
        // the curated catalog (legacy entries, late additions).
        if let Some(exts) = parsed.get("extensions").and_then(|v| v.as_object()) {
            for (ext, val) in exts {
                if ext.starts_with('_') {
                    continue;
                }
                let (Some(tc_str), Some(cc_str)) = (
                    val.get("type").and_then(|v| v.as_str()),
                    val.get("creator").and_then(|v| v.as_str()),
                ) else {
                    continue;
                };
                if !seen.insert((tc_str.to_string(), cc_str.to_string())) {
                    continue;
                }
                let desc = type_desc
                    .get(tc_str)
                    .cloned()
                    .unwrap_or_else(|| format!(".{ext}"));
                out.push(TypeCreatorEntry {
                    type_code: encode_fourcc(tc_str),
                    creator_code: encode_fourcc(cc_str),
                    description: desc,
                });
            }
        }
        out.sort_by(|a, b| {
            a.description
                .to_lowercase()
                .cmp(&b.description.to_lowercase())
                .then_with(|| a.type_code.cmp(&b.type_code))
                .then_with(|| a.creator_code.cmp(&b.creator_code))
        });
        out
    })
}

/// All known (type, creator) entries, sorted by friendly description.
pub fn known_type_creators() -> &'static [TypeCreatorEntry] {
    load_type_creator_entries().as_slice()
}

/// Friendly description for a (type, creator) pair, if known.
pub fn describe_type_creator(type_code: &[u8; 4], creator_code: &[u8; 4]) -> Option<&'static str> {
    load_type_creator_entries()
        .iter()
        .find(|e| &e.type_code == type_code && &e.creator_code == creator_code)
        .map(|e| e.description.as_str())
}

/// Look up the type and creator codes for a file extension.
/// Returns None for unknown extensions.
pub fn type_creator_for_extension(ext: &str) -> Option<([u8; 4], [u8; 4])> {
    let lower = ext.to_lowercase();
    let map = load_extension_map();
    for (e, tc, cc) in &map.entries {
        if e == &lower {
            return Some((*tc, *cc));
        }
    }
    None
}

/// Encode a 4-character string into a 4-byte array, space-padded on the right.
pub fn encode_fourcc(s: &str) -> [u8; 4] {
    let bytes = s.as_bytes();
    let mut out = [b' '; 4];
    for (i, &b) in bytes.iter().take(4).enumerate() {
        out[i] = b;
    }
    out
}

/// Render a raw 4-byte Mac `OSType` (type/creator code) as a printable string
/// for **display only** — non-printable / non-ASCII bytes become `.`.
///
/// This is intentionally lossy and must never be used to reconstruct the
/// bytes (that is what corrupted high-bit creators like Prince of Persia's
/// `PoƒP` = `50 6f C4 50`, mangling `0xC4` to `.`). For fidelity, carry the
/// raw `[u8; 4]` instead (see `FileEntry::type_code` / `os_type`).
pub fn decode_ostype(code: &[u8; 4]) -> String {
    code.iter()
        .map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// B-tree key comparison
// ---------------------------------------------------------------------------

/// Normalize an HFS+ catalog name into Apple's `FastUnicodeCompare` form:
/// canonically decompose each UTF-16 unit (Apple's TN1150 decomposition, NOT
/// Rust NFD — the two differ in the ranges Apple excludes), then case-fold each
/// resulting unit through Apple's table, dropping units that fold to 0
/// (Apple's "ignored" set: zero-width / format / variation selectors). NUL
/// folds to 0xFFFF, so a leading-NUL name like the `HFS+ Private Data`
/// directory sorts *last*. Comparing the folded `u16` sequences lexicographically
/// reproduces a real Mac OS volume's ordering exactly (see [`hfs_unicode`]).
fn fold_hfsplus_name_for_compare(name: &[u16]) -> Vec<u16> {
    let mut out = Vec::with_capacity(name.len());
    let mut buf = [0u16; 3];
    for &unit in name {
        for &d in super::hfs_unicode::decompose(unit, &mut buf) {
            let folded = super::hfs_unicode::case_fold(d);
            if folded != 0 {
                out.push(folded);
            }
        }
    }
    out
}

/// Compare two HFS+ catalog keys (case-insensitive by default).
///
/// Keys are: parent_cnid (u32) + name (UTF-16BE units).
/// Comparison: parent CNID first (numeric), then name.
/// Case-insensitive: drop Apple-ignored code points, NFD decompose, then
/// compare uppercased chars.
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
    let folded_a = fold_hfsplus_name_for_compare(name_a);
    let folded_b = fold_hfsplus_name_for_compare(name_b);
    folded_a.cmp(&folded_b)
}

/// Mac Roman catalog-name collation order (byte -> sort rank), as used by
/// classic Mac OS HFS.
///
/// Classic HFS compares catalog names with the File Manager's `_RelString`
/// trap (`CMKeyCmp` in Apple's source: `OS/HFS/CMMAINT.a`), i.e. the system
/// string comparison — **case-insensitive, diacritical-sensitive**, with a
/// specific Mac Roman ordering. This is NOT a plain ASCII uppercase: lowercase
/// folds onto its uppercase *rank*, accented letters sort right after their
/// base letter (so "Bézier" < "Bind"), and punctuation / quotes / spaces
/// (incl. high-byte curly quotes 0xD2-0xD5, en/em dashes, and the non-breaking
/// space 0xCA, which sorts as a space) get their own early ranks rather than
/// landing after 'Z' by raw byte value.
///
/// The table is the well-known `hfs_charorder` reproduction (hfsutils
/// `libhfs/data.c`); the Linux kernel's `caseorder` (`fs/hfs/string.c`) is a
/// second, independent copy — they use different absolute values but induce an
/// identical ordering, and this table reproduces real Apple-formatted volumes
/// fsck-clean (verified against a MacPack boot disk). The previous table only
/// folded ASCII and left high bytes raw, which mis-sorted any name with an
/// accent or curly quote.
static HFS_CHARORDER: [u8; 256] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x20, 0x22, 0x23, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
    0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
    0x47, 0x48, 0x58, 0x5a, 0x5e, 0x60, 0x67, 0x69, 0x6b, 0x6d, 0x73, 0x75, 0x77, 0x79, 0x7b, 0x7f,
    0x8d, 0x8f, 0x91, 0x93, 0x96, 0x98, 0x9f, 0xa1, 0xa3, 0xa5, 0xa8, 0xaa, 0xab, 0xac, 0xad, 0xae,
    0x54, 0x48, 0x58, 0x5a, 0x5e, 0x60, 0x67, 0x69, 0x6b, 0x6d, 0x73, 0x75, 0x77, 0x79, 0x7b, 0x7f,
    0x8d, 0x8f, 0x91, 0x93, 0x96, 0x98, 0x9f, 0xa1, 0xa3, 0xa5, 0xa8, 0xaf, 0xb0, 0xb1, 0xb2, 0xb3,
    0x4c, 0x50, 0x5c, 0x62, 0x7d, 0x81, 0x9a, 0x55, 0x4a, 0x56, 0x4c, 0x4e, 0x50, 0x5c, 0x62, 0x64,
    0x65, 0x66, 0x6f, 0x70, 0x71, 0x72, 0x7d, 0x89, 0x8a, 0x8b, 0x81, 0x83, 0x9c, 0x9d, 0x9e, 0x9a,
    0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0x95, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf, 0xc0, 0x52, 0x85,
    0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xcb, 0x57, 0x8c, 0xcc, 0x52, 0x85,
    0xcd, 0xce, 0xcf, 0xd0, 0xd1, 0xd2, 0xd3, 0x26, 0x27, 0xd4, 0x20, 0x4a, 0x4e, 0x83, 0x87, 0x87,
    0xd5, 0xd6, 0x24, 0x25, 0x2d, 0x2e, 0xd7, 0xd8, 0xa7, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde, 0xdf,
    0xe0, 0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xeb, 0xec, 0xed, 0xee, 0xef,
    0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff,
];

/// Compare two HFS (classic) catalog keys the way the Mac OS File Manager does.
///
/// Keys are: parent_cnid (u32) + name (Mac Roman bytes). Parent IDs compare
/// numerically; names compare through [`HFS_CHARORDER`] byte-by-byte, with the
/// shorter name sorting first on a common prefix (matching hfsutils
/// `d_relstring` / the Linux kernel `hfs_strcmp`).
pub fn compare_hfs_keys(parent_a: u32, name_a: &[u8], parent_b: u32, name_b: &[u8]) -> Ordering {
    match parent_a.cmp(&parent_b) {
        Ordering::Equal => {}
        other => return other,
    }
    let len = name_a.len().min(name_b.len());
    for i in 0..len {
        let a = HFS_CHARORDER[name_a[i] as usize];
        let b = HFS_CHARORDER[name_b[i] as usize];
        match a.cmp(&b) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    name_a.len().cmp(&name_b.len())
}

// ---------------------------------------------------------------------------
// B-tree node manipulation
// ---------------------------------------------------------------------------

/// B-tree node kinds.
pub const BTREE_LEAF_NODE: i8 = -1;
pub const BTREE_INDEX_NODE: i8 = 0;
#[allow(dead_code)]
pub const BTREE_HEADER_NODE: i8 = 1;
#[allow(dead_code)]
pub const BTREE_MAP_NODE: i8 = 2;

/// Mac OS forces all catalog B-tree index keys to this length (0x25 = 37 decimal).
/// Key structure: reserved(1) + parentID(4) + nameLen(1) + name(up to 31) = 37 max.
pub const HFS_CAT_MAX_KEY_LEN: u8 = 0x25;

/// Normalize a catalog key for use in an index record.
///
/// Mac OS expects all catalog index record keys to have key_len = 0x25,
/// zero-padded beyond the actual key data. This matches the `n_index`
/// function in hfsutils which forces `ckrKeyLen = 0x25`.
///
/// Returns: `[0x25][key_data zero-padded to 37 bytes]` = 38 bytes total.
pub fn normalize_catalog_index_key(key: &[u8]) -> Vec<u8> {
    let max = HFS_CAT_MAX_KEY_LEN as usize; // 37
    let mut result = vec![0u8; 1 + max]; // 38 bytes: key_len + 37 bytes of key data
    result[0] = HFS_CAT_MAX_KEY_LEN;
    // Copy actual key data (skip the key_len byte from source)
    if key.len() > 1 {
        let copy_len = (key.len() - 1).min(max);
        result[1..1 + copy_len].copy_from_slice(&key[1..1 + copy_len]);
    }
    result
}

/// `kBTBigKeysMask` — the B-tree's key-length prefix is a 2-byte (`u16`) field.
/// Set on every HFS+ tree (catalog, extents-overflow, attributes); clear on
/// classic HFS, whose key length is a single byte.
pub const KBT_BIG_KEYS_MASK: u32 = 0x0000_0002;
/// `kBTVariableIndexKeysMask` — index records carry the child's variable-length
/// key verbatim. Set on the HFS+ catalog and attributes trees; clear on the
/// extents-overflow tree and classic HFS, whose index keys are padded to a
/// fixed `max_key_len`.
pub const KBT_VARIABLE_INDEX_KEYS_MASK: u32 = 0x0000_0004;

/// Describes how a B-tree encodes its keys, so the shared split / grow / rotate
/// machinery below can serve both the classic-HFS 1-byte-key catalog and the
/// HFS+ 2-byte ("big key") trees without forking the algorithm.
///
/// The classic path threads [`BTreeKeyFormat::CLASSIC_CATALOG`] and is
/// byte-identical to the pre-descriptor code (the fixed 0x25 index key produced
/// by [`normalize_catalog_index_key`]); HFS+ threads the matching `HFSPLUS_*`
/// constant, which is what unlocks variable-length index keys so an HFS+ catalog
/// can split past a single leaf level without corrupting.
///
/// Derived from the BTHeaderRec `attributes` bitfield + `maxKeyLength`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BTreeKeyFormat {
    /// `kBTBigKeysMask`: key-length prefix is 2 bytes (HFS+) vs 1 byte (classic).
    pub big_keys: bool,
    /// `kBTVariableIndexKeysMask`: index records store the child's
    /// variable-length key verbatim. When clear, index keys are padded to
    /// `max_key_len`.
    pub variable_index_keys: bool,
    /// Maximum key length — the fixed index-key width when `variable_index_keys`
    /// is clear (classic catalog: 0x25; HFS+ extents-overflow: 10).
    pub max_key_len: u16,
}

impl BTreeKeyFormat {
    /// Classic HFS catalog: 1-byte key length, fixed 0x25-byte index keys.
    pub const CLASSIC_CATALOG: BTreeKeyFormat = BTreeKeyFormat {
        big_keys: false,
        variable_index_keys: false,
        max_key_len: HFS_CAT_MAX_KEY_LEN as u16,
    };

    /// HFS+/HFSX catalog: 2-byte key length, variable-length index keys
    /// (`kHFSPlusCatalogKeyMaximumLength` = 516).
    pub const HFSPLUS_CATALOG: BTreeKeyFormat = BTreeKeyFormat {
        big_keys: true,
        variable_index_keys: true,
        max_key_len: 516,
    };

    /// HFS+ extents-overflow: 2-byte key length, fixed 10-byte index keys
    /// (`kHFSPlusExtentKeyMaximumLength` = 10).
    pub const HFSPLUS_EXTENTS: BTreeKeyFormat = BTreeKeyFormat {
        big_keys: true,
        variable_index_keys: false,
        max_key_len: 10,
    };

    /// HFS+ attributes: 2-byte key length, variable-length index keys
    /// (`kHFSPlusAttrKeyMaximumLength` = 264).
    pub const HFSPLUS_ATTRIBUTES: BTreeKeyFormat = BTreeKeyFormat {
        big_keys: true,
        variable_index_keys: true,
        max_key_len: 264,
    };

    /// Build a descriptor from a BTHeaderRec `attributes` bitfield and
    /// `maxKeyLength`. The `attributes` field on hand-built volumes can be 0
    /// (no flags) even for HFS+ trees, so callers that know the tree's identity
    /// should prefer the `HFSPLUS_*` / `CLASSIC_CATALOG` constants; this helper
    /// is for code paths that only have the raw header bytes.
    pub fn from_header(attributes: u32, max_key_len: u16) -> Self {
        BTreeKeyFormat {
            big_keys: attributes & KBT_BIG_KEYS_MASK != 0,
            variable_index_keys: attributes & KBT_VARIABLE_INDEX_KEYS_MASK != 0,
            max_key_len,
        }
    }

    /// The BTHeaderRec `attributes` bits implied by this format.
    pub fn header_attributes(&self) -> u32 {
        let mut a = 0;
        if self.big_keys {
            a |= KBT_BIG_KEYS_MASK;
        }
        if self.variable_index_keys {
            a |= KBT_VARIABLE_INDEX_KEYS_MASK;
        }
        a
    }

    /// Bytes occupied by the key-length prefix (1 classic, 2 big-key).
    #[inline]
    pub fn len_prefix(&self) -> usize {
        if self.big_keys {
            2
        } else {
            1
        }
    }

    /// Read the key length stored at the front of `record`.
    #[inline]
    pub fn key_len(&self, record: &[u8]) -> usize {
        if self.big_keys {
            if record.len() < 2 {
                0
            } else {
                BigEndian::read_u16(&record[0..2]) as usize
            }
        } else if record.is_empty() {
            0
        } else {
            record[0] as usize
        }
    }

    /// The key portion of a record — the length prefix plus `key_len` key
    /// bytes. This is exactly the slice an index separator must carry, and what
    /// the split helpers extract from a leaf/child's first record.
    pub fn key_portion(&self, record: &[u8]) -> Vec<u8> {
        let end = (self.len_prefix() + self.key_len(record)).min(record.len());
        record[..end].to_vec()
    }

    /// Build the key bytes of an index record (length prefix + key) from a
    /// child node's first-key portion.
    ///
    /// - Classic HFS: the fixed 0x25 normalized key, identical to
    ///   [`normalize_catalog_index_key`].
    /// - HFS+ variable-index-key trees (catalog, attributes): the key portion
    ///   verbatim (2-byte length + key bytes).
    /// - HFS+ fixed-index-key trees (extents-overflow): the key bytes padded to
    ///   `max_key_len`, length field forced to `max_key_len`.
    pub fn make_index_key(&self, first_key: &[u8]) -> Vec<u8> {
        if !self.big_keys {
            return normalize_catalog_index_key(first_key);
        }
        if self.variable_index_keys {
            return first_key.to_vec();
        }
        let max = self.max_key_len as usize;
        let copy = self.key_len(first_key).min(max);
        let mut result = vec![0u8; 2 + max];
        BigEndian::write_u16(&mut result[0..2], max as u16);
        if first_key.len() >= 2 + copy {
            result[2..2 + copy].copy_from_slice(&first_key[2..2 + copy]);
        }
        result
    }
}

/// Read the B-tree header record from catalog_data (node 0, record 0 at offset 14).
/// Returns (depth, root_node, leaf_records, first_leaf, last_leaf, node_size,
///          max_key_len, total_nodes, free_nodes).
pub struct BTreeHeader {
    pub depth: u16,
    pub root_node: u32,
    pub leaf_records: u32,
    pub first_leaf_node: u32,
    pub last_leaf_node: u32,
    pub node_size: u16,
    pub total_nodes: u32,
    pub free_nodes: u32,
}

impl BTreeHeader {
    pub fn read(catalog_data: &[u8]) -> Self {
        // Header record starts at offset 14 in node 0
        let d = &catalog_data[14..];
        BTreeHeader {
            depth: BigEndian::read_u16(&d[0..2]),
            root_node: BigEndian::read_u32(&d[2..6]),
            leaf_records: BigEndian::read_u32(&d[6..10]),
            first_leaf_node: BigEndian::read_u32(&d[10..14]),
            last_leaf_node: BigEndian::read_u32(&d[14..18]),
            node_size: BigEndian::read_u16(&d[18..20]),
            total_nodes: BigEndian::read_u32(&d[22..26]),
            free_nodes: BigEndian::read_u32(&d[26..30]),
        }
    }

    pub fn write(&self, catalog_data: &mut [u8]) {
        let d = &mut catalog_data[14..];
        BigEndian::write_u16(&mut d[0..2], self.depth);
        BigEndian::write_u32(&mut d[2..6], self.root_node);
        BigEndian::write_u32(&mut d[6..10], self.leaf_records);
        BigEndian::write_u32(&mut d[10..14], self.first_leaf_node);
        BigEndian::write_u32(&mut d[14..18], self.last_leaf_node);
        BigEndian::write_u32(&mut d[22..26], self.total_nodes);
        BigEndian::write_u32(&mut d[26..30], self.free_nodes);
    }
}

/// Calculate free space in a B-tree node (space available for a new record).
///
/// Free space = gap between last record's end and first offset-table entry,
/// minus 2 bytes for the new offset slot that would be needed.
pub fn btree_node_free_space(node: &[u8], node_size: usize) -> usize {
    let num_records = BigEndian::read_u16(&node[10..12]) as usize;
    if num_records == 0 {
        // Empty node: data starts at 14 (after descriptor), offset table starts at end
        // Offset table has 1 entry (the "free space offset" at position 0)
        let data_end = 14;
        let table_start = node_size - 2 * (num_records + 1);
        if table_start <= data_end + 2 {
            return 0;
        }
        return table_start - data_end - 2;
    }
    // Last record ends at the offset stored in the (num_records) position of offset table
    let free_offset_pos = node_size - 2 * (num_records + 1);
    let last_rec_end = BigEndian::read_u16(&node[free_offset_pos..free_offset_pos + 2]) as usize;
    // Offset table currently occupies: 2*(num_records+1) bytes at end of node
    // Adding a record would need 2 more bytes for a new offset slot
    let table_start = free_offset_pos;
    if table_start <= last_rec_end + 2 {
        return 0;
    }
    table_start - last_rec_end - 2
}

/// Get the offset and data of record `rec_idx` in a node.
/// Returns (rec_offset, rec_end) — the byte range within the node.
pub fn btree_record_range(node: &[u8], node_size: usize, rec_idx: usize) -> (usize, usize) {
    let offset_pos = node_size - 2 * (rec_idx + 1);
    let start = BigEndian::read_u16(&node[offset_pos..offset_pos + 2]) as usize;
    let next_pos = node_size - 2 * (rec_idx + 2);
    let end = BigEndian::read_u16(&node[next_pos..next_pos + 2]) as usize;
    (start, end)
}

/// Visit every record in the catalog's leaf-node chain, skipping non-leaf
/// nodes and stopping if a cycle is detected. The visitor receives
/// `(node_idx, rec_idx, abs_rec_offset, rec_bytes)`; returning `Some(T)`
/// short-circuits the walk and yields that value.
///
/// Used by HFS and HFS+ for catalog scans where a B-tree descent isn't
/// appropriate (linear scans by CNID, valence updates, name lookups in
/// small catalogs).
pub fn walk_leaf_records<T, F>(
    catalog_data: &[u8],
    first_leaf: u32,
    node_size: usize,
    mut visit: F,
) -> Option<T>
where
    F: FnMut(u32, usize, usize, &[u8]) -> Option<T>,
{
    if node_size == 0 || catalog_data.len() < node_size {
        return None;
    }
    let mut node_idx = first_leaf;
    let mut visited = std::collections::HashSet::new();
    while node_idx != 0 {
        if !visited.insert(node_idx) {
            return None;
        }
        let offset = (node_idx as usize).checked_mul(node_size)?;
        if offset + node_size > catalog_data.len() {
            return None;
        }
        let node = &catalog_data[offset..offset + node_size];
        let next = BigEndian::read_u32(&node[0..4]);
        let kind = node[8] as i8;
        if kind != BTREE_LEAF_NODE {
            node_idx = next;
            continue;
        }
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;
        for i in 0..num_records {
            let (rec_start, rec_end) = btree_record_range(node, node_size, i);
            if rec_start >= rec_end || rec_end > node_size {
                continue;
            }
            if let Some(t) = visit(node_idx, i, offset + rec_start, &node[rec_start..rec_end]) {
                return Some(t);
            }
        }
        node_idx = next;
    }
    None
}

/// Insert a record (key_bytes ++ record_bytes) into a B-tree leaf node at the
/// correct sorted position.
///
/// `compare_fn` takes (existing_key_bytes, new_key_bytes) → Ordering.
/// Returns the index where the record was inserted.
/// Returns Err if the node doesn't have enough free space.
pub fn btree_insert_record<F>(
    node: &mut [u8],
    node_size: usize,
    key_record_bytes: &[u8],
    compare_fn: &F,
) -> Result<usize, FilesystemError>
where
    F: Fn(&[u8], &[u8]) -> Ordering,
{
    let num_records = BigEndian::read_u16(&node[10..12]) as usize;
    let total_needed = key_record_bytes.len() + 2; // +2 for offset table entry

    let free = btree_node_free_space(node, node_size);
    if free < total_needed {
        return Err(FilesystemError::InvalidData("B-tree node full".into()));
    }

    // Collect all existing records
    let mut records: Vec<Vec<u8>> = Vec::with_capacity(num_records + 1);
    for i in 0..num_records {
        let (rec_start, rec_end) = btree_record_range(node, node_size, i);
        if rec_start < rec_end && rec_end <= node_size {
            records.push(node[rec_start..rec_end].to_vec());
        }
    }

    // Find insertion position by comparing keys
    let mut insert_pos = records.len();
    for (i, rec) in records.iter().enumerate() {
        if compare_fn(rec, key_record_bytes) == Ordering::Greater {
            insert_pos = i;
            break;
        }
    }

    // Insert the new record at the sorted position
    records.insert(insert_pos, key_record_bytes.to_vec());

    // Rebuild the node: write all records sequentially starting at offset 14
    // This ensures physical data order matches offset table order (monotonically increasing)
    let mut write_pos = 14usize;
    for (i, rec) in records.iter().enumerate() {
        node[write_pos..write_pos + rec.len()].copy_from_slice(rec);
        let opos = node_size - 2 * (i + 1);
        BigEndian::write_u16(&mut node[opos..opos + 2], write_pos as u16);
        write_pos += rec.len();
    }

    // Write free-space offset
    let fpos = node_size - 2 * (records.len() + 1);
    BigEndian::write_u16(&mut node[fpos..fpos + 2], write_pos as u16);

    // Clear any leftover data between write_pos and the offset table
    let offset_table_start = node_size - 2 * (records.len() + 1);
    if write_pos < offset_table_start {
        node[write_pos..offset_table_start].fill(0);
    }

    // Update num_records in node descriptor
    BigEndian::write_u16(&mut node[10..12], records.len() as u16);

    Ok(insert_pos)
}

/// Remove record at `rec_idx` from a B-tree node and compact data.
///
/// Rebuilds the node data area sequentially (like `btree_insert_record`) so
/// that record boundaries in the offset table are contiguous. Without
/// compaction, `btree_record_range` would report inflated record lengths
/// because it uses consecutive offset table entries to determine boundaries.
pub fn btree_remove_record(node: &mut [u8], node_size: usize, rec_idx: usize) {
    let num_records = BigEndian::read_u16(&node[10..12]) as usize;
    if rec_idx >= num_records {
        return;
    }

    // Collect all records except the one being removed
    let mut records: Vec<Vec<u8>> = Vec::with_capacity(num_records - 1);
    for i in 0..num_records {
        if i == rec_idx {
            continue;
        }
        let (rec_start, rec_end) = btree_record_range(node, node_size, i);
        if rec_start < rec_end && rec_end <= node_size {
            records.push(node[rec_start..rec_end].to_vec());
        }
    }

    // Rebuild node: write all remaining records sequentially from offset 14
    let mut write_pos = 14usize;
    for (i, rec) in records.iter().enumerate() {
        node[write_pos..write_pos + rec.len()].copy_from_slice(rec);
        let opos = node_size - 2 * (i + 1);
        BigEndian::write_u16(&mut node[opos..opos + 2], write_pos as u16);
        write_pos += rec.len();
    }

    // Write free-space offset
    let fpos = node_size - 2 * (records.len() + 1);
    BigEndian::write_u16(&mut node[fpos..fpos + 2], write_pos as u16);

    // Clear leftover data between write_pos and the offset table
    let offset_table_start = node_size - 2 * (records.len() + 1);
    if write_pos < offset_table_start {
        node[write_pos..offset_table_start].fill(0);
    }

    // Clear the now-unused last offset slot
    let clear_pos = node_size - 2 * (num_records + 1);
    BigEndian::write_u16(&mut node[clear_pos..clear_pos + 2], 0);

    // Update num_records
    BigEndian::write_u16(&mut node[10..12], records.len() as u16);
}

// ---------------------------------------------------------------------------
// B-tree node bitmap (allocation bitmap for nodes)
// ---------------------------------------------------------------------------
//
// The first segment of the node bitmap lives in record 2 of node 0 (the
// header node). At node_size=512 it covers (512−256) bytes × 8 = 2048 bits,
// so a B-tree file larger than 2048 nodes needs MAP NODES (BTNodeKind=2)
// to hold the rest. Map nodes are chained through node descriptors:
//
//   header.fLink → mapNode₁ → mapNode₂ → … → 0
//
// Each map node carries a single record (record 0) of bitmap bytes covering
// (node_size − 14 − 4) × 8 = 3952 bits at node_size=512 — i.e. each map node
// extends the addressable range by 3952 nodes.

/// Header-node bitmap range — first segment of the bitmap chain.
/// Returns `(byte_offset_in_catalog_data, size_in_bytes)`.
pub fn btree_node_bitmap_range(catalog_data: &[u8], node_size: usize) -> (usize, usize) {
    // Node 0 has 3 records: [0]=header, [1]=user data, [2]=bitmap
    // The bitmap record starts at the offset stored for record 2
    let offset_pos = node_size - 2 * 3; // record 2 offset
    let bitmap_start = BigEndian::read_u16(&catalog_data[offset_pos..offset_pos + 2]) as usize;
    let next_pos = node_size - 2 * 4; // free space offset
    let bitmap_end = BigEndian::read_u16(&catalog_data[next_pos..next_pos + 2]) as usize;
    (bitmap_start, bitmap_end.saturating_sub(bitmap_start))
}

/// Bytes of bitmap stored in a single map node at the given node_size.
pub fn map_node_bitmap_bytes(node_size: usize) -> usize {
    // descriptor (14) + record-0 offset u16 + free-space offset u16 = 18.
    node_size.saturating_sub(18)
}

/// Number of map nodes (BTNodeKind=2) needed to extend the node-allocation
/// bitmap so it covers `total_nodes` indices, given a node size. The
/// header node carries the first `(node_size - 256) * 8` bits; map nodes
/// in the chain add `map_node_bitmap_bytes * 8` bits each. Used by both
/// classic HFS catalog/extents builders and the HFS+ defrag B-tree
/// allocator to decide how many map nodes to materialise up front.
pub fn map_nodes_required(total_nodes: u32, node_size: usize) -> u32 {
    let header_cap = ((node_size.saturating_sub(256)) * 8) as u32;
    if total_nodes <= header_cap {
        return 0;
    }
    let per_map = (map_node_bitmap_bytes(node_size) * 8) as u32;
    if per_map == 0 {
        return 0;
    }
    (total_nodes - header_cap).div_ceil(per_map)
}

/// One contiguous span of the node bitmap in `catalog_data`.
#[derive(Debug, Clone, Copy)]
pub struct BitmapSegment {
    /// Byte offset in `catalog_data`.
    pub byte_off: usize,
    /// Length in bytes.
    pub len: usize,
    /// Index of the first node bit in this segment (i.e. global bit
    /// `base_bit` lives at byte `byte_off`, bit 7 within that byte).
    pub base_bit: u32,
}

/// Walk the B-tree node bitmap from the header node through any map-node
/// chain and return the byte ranges that make up the full bitmap.
///
/// Defensive: bails out if the chain self-loops or points outside the file.
/// Always returns at least the header-node segment.
pub fn btree_bitmap_segments(catalog_data: &[u8], node_size: usize) -> Vec<BitmapSegment> {
    let mut segs = Vec::with_capacity(1);
    if catalog_data.len() < node_size {
        return segs;
    }

    let (hdr_off, hdr_len) = btree_node_bitmap_range(catalog_data, node_size);
    segs.push(BitmapSegment {
        byte_off: hdr_off,
        len: hdr_len,
        base_bit: 0,
    });
    let mut next_bit = (hdr_len as u32) * 8;

    // The header node's fLink is at byte 0..4 of node 0 — when non-zero, it
    // points to the first map node.
    let mut next_node = BigEndian::read_u32(&catalog_data[0..4]);
    let mut visited: std::collections::HashSet<u32> = std::collections::HashSet::with_capacity(8);

    while next_node != 0 {
        if !visited.insert(next_node) {
            // Cycle — stop.
            break;
        }
        let off = next_node as usize * node_size;
        if off + node_size > catalog_data.len() {
            break;
        }
        // Validate kind == 2 (map node) and numRecords >= 1.
        let kind = catalog_data[off + 8] as i8;
        let num_records = BigEndian::read_u16(&catalog_data[off + 10..off + 12]);
        if kind != 2 || num_records < 1 {
            break;
        }
        // Record 0 offset is at end of node minus 2 bytes.
        let rec0_pos = off + node_size - 2;
        let rec0_off = BigEndian::read_u16(&catalog_data[rec0_pos..rec0_pos + 2]) as usize;
        // Free-space offset (next 2 bytes back) gives the end of record 0.
        let free_pos = off + node_size - 4;
        let free_off = BigEndian::read_u16(&catalog_data[free_pos..free_pos + 2]) as usize;
        if rec0_off < 14 || free_off <= rec0_off || free_off > node_size {
            break;
        }
        let len = free_off - rec0_off;
        segs.push(BitmapSegment {
            byte_off: off + rec0_off,
            len,
            base_bit: next_bit,
        });
        next_bit += (len as u32) * 8;
        // fLink at byte 0..4 of this map node.
        next_node = BigEndian::read_u32(&catalog_data[off..off + 4]);
    }

    segs
}

fn locate_bit_in_segments(segs: &[BitmapSegment], global_bit: u32) -> Option<(usize, u32)> {
    for (i, seg) in segs.iter().enumerate() {
        let cap = (seg.len as u32) * 8;
        if global_bit >= seg.base_bit && global_bit < seg.base_bit + cap {
            return Some((i, global_bit - seg.base_bit));
        }
    }
    None
}

/// Test whether the given global node bit is set in the node bitmap chain.
pub fn btree_bitmap_test(catalog_data: &[u8], node_size: usize, bit: u32) -> bool {
    let segs = btree_bitmap_segments(catalog_data, node_size);
    let Some((idx, local)) = locate_bit_in_segments(&segs, bit) else {
        return false;
    };
    let seg = segs[idx];
    let slice = &catalog_data[seg.byte_off..seg.byte_off + seg.len];
    bitmap_test_bit_be(slice, local)
}

/// Allocate a free node from the B-tree node bitmap.
/// Sets the bit and returns the node index. Updates nothing else (caller must
/// update header's free_nodes).
pub fn btree_alloc_node(
    catalog_data: &mut [u8],
    node_size: usize,
    total_nodes: u32,
) -> Result<u32, FilesystemError> {
    let segs = btree_bitmap_segments(catalog_data, node_size);
    for seg in &segs {
        let bitmap = &catalog_data[seg.byte_off..seg.byte_off + seg.len];
        let cap = (seg.len as u32) * 8;
        let upper = total_nodes.saturating_sub(seg.base_bit).min(cap);
        for local in 0..upper {
            if !bitmap_test_bit_be(bitmap, local) {
                let global = seg.base_bit + local;
                bitmap_set_bit_be(
                    &mut catalog_data[seg.byte_off..seg.byte_off + seg.len],
                    local,
                );
                return Ok(global);
            }
        }
    }
    Err(FilesystemError::DiskFull("no free B-tree nodes".into()))
}

/// Free a node in the B-tree node bitmap. Clears the bit.
pub fn btree_free_node(catalog_data: &mut [u8], node_size: usize, node_idx: u32) {
    let segs = btree_bitmap_segments(catalog_data, node_size);
    let Some((idx, local)) = locate_bit_in_segments(&segs, node_idx) else {
        return;
    };
    let seg = segs[idx];
    bitmap_clear_bit_be(
        &mut catalog_data[seg.byte_off..seg.byte_off + seg.len],
        local,
    );
}

/// Clear a node's bit in the bitmap. Segment-aware (handles map nodes).
pub fn btree_bitmap_clear(catalog_data: &mut [u8], node_size: usize, node_idx: u32) {
    let segs = btree_bitmap_segments(catalog_data, node_size);
    let Some((idx, local)) = locate_bit_in_segments(&segs, node_idx) else {
        return;
    };
    let seg = segs[idx];
    bitmap_clear_bit_be(
        &mut catalog_data[seg.byte_off..seg.byte_off + seg.len],
        local,
    );
}

/// Set a node's bit in the bitmap (without doing any allocation). Used by
/// the blank-volume builder to reserve specific node indices for map nodes.
pub fn btree_bitmap_set(catalog_data: &mut [u8], node_size: usize, node_idx: u32) {
    let segs = btree_bitmap_segments(catalog_data, node_size);
    let Some((idx, local)) = locate_bit_in_segments(&segs, node_idx) else {
        return;
    };
    let seg = segs[idx];
    bitmap_set_bit_be(
        &mut catalog_data[seg.byte_off..seg.byte_off + seg.len],
        local,
    );
}

/// Initialize a node as a map node (BTNodeKind=2): descriptor + one record
/// holding `bitmap_bytes` of zeros + offset table.
pub fn init_map_node(
    catalog_data: &mut [u8],
    node_size: usize,
    node_idx: u32,
    prev_link: u32,
    next_link: u32,
) {
    let off = node_idx as usize * node_size;
    let node = &mut catalog_data[off..off + node_size];
    // Zero everything first.
    node.fill(0);
    // Descriptor: fLink, bLink, kind=2, height=0, numRecords=1, reserved.
    BigEndian::write_u32(&mut node[0..4], next_link);
    BigEndian::write_u32(&mut node[4..8], prev_link);
    node[8] = 2; // BTNodeKind::MapNode
    node[9] = 0;
    BigEndian::write_u16(&mut node[10..12], 1);
    // Record 0 starts at offset 14 and runs to (node_size - 4).
    let bitmap_bytes = map_node_bitmap_bytes(node_size) as u16;
    let free_off = 14u16 + bitmap_bytes;
    // Offset table at the end: [free_space_offset, record0_offset] (last entry first).
    BigEndian::write_u16(&mut node[node_size - 2..node_size], 14);
    BigEndian::write_u16(&mut node[node_size - 4..node_size - 2], free_off);
}

// ---------------------------------------------------------------------------
// B-tree splitting
// ---------------------------------------------------------------------------

/// Initialize a new empty node in catalog_data at the given node_idx.
/// Sets the node kind and height. Returns a mutable slice to the node.
pub(crate) fn init_node(
    catalog_data: &mut [u8],
    node_size: usize,
    node_idx: u32,
    kind: i8,
    height: u8,
) {
    let offset = node_idx as usize * node_size;
    let node = &mut catalog_data[offset..offset + node_size];
    // Zero the node
    node.fill(0);
    // Set descriptor fields
    node[8] = kind as u8; // kind
    node[9] = height; // height
    BigEndian::write_u16(&mut node[10..12], 0); // num_records = 0
                                                // Initialize offset table: record 0 starts at offset 14, free-space offset = 14
    BigEndian::write_u16(&mut node[node_size - 2..node_size], 14);
}

/// Split a full leaf node and insert `new_record` atomically. Distributes
/// existing records + `new_record` (sorted via `compare_fn`) across the old
/// leaf and one or more new leaves so each side fits within `node_size`.
/// Greedy byte-based partitioning handles arbitrary record sizes — including
/// HFS+ catalogs full of long-name records that exceed `2 × node_size` and
/// would defeat a strict 2-way split.
///
/// Returns a list of `(new_node_idx, first_key_of_new_node)` — one entry per
/// new leaf node allocated; each entry's `first_key_of_new_node` is the
/// separator key the caller must install into the parent index node, in
/// order. `header` is mutated: `free_nodes` decremented; caller writes back.
///
/// Most calls produce a single entry (classic 2-way split). N-way splits
/// trigger only when the merged record set genuinely exceeds two nodes —
/// rare outside HFS+ catalogs holding many max-length filenames.
pub fn btree_split_leaf_with_insert<F>(
    catalog_data: &mut [u8],
    node_size: usize,
    node_idx: u32,
    header: &mut BTreeHeader,
    new_record: &[u8],
    kf: &BTreeKeyFormat,
    compare_fn: &F,
) -> Result<Vec<(u32, Vec<u8>)>, FilesystemError>
where
    F: Fn(&[u8], &[u8]) -> Ordering,
{
    let old_offset = node_idx as usize * node_size;
    let old_num = BigEndian::read_u16(&catalog_data[old_offset + 10..old_offset + 12]) as usize;
    let old_height = catalog_data[old_offset + 9];

    // Gather existing records, then merge in new_record at the sorted spot.
    let mut records: Vec<Vec<u8>> = Vec::with_capacity(old_num + 1);
    for i in 0..old_num {
        let (s, e) = btree_record_range(
            &catalog_data[old_offset..old_offset + node_size],
            node_size,
            i,
        );
        records.push(catalog_data[old_offset + s..old_offset + e].to_vec());
    }
    let mut insert_pos = records.len();
    for (i, rec) in records.iter().enumerate() {
        if compare_fn(rec, new_record) == Ordering::Greater {
            insert_pos = i;
            break;
        }
    }
    records.insert(insert_pos, new_record.to_vec());

    // Per-node usage: 14 (descriptor) + sum(rec_lens) + 2 * count + 2 (free
    // pointer); `payload` accounts for the fixed overhead. A single record
    // larger than the payload is a structural impossibility (HFS/HFS+ key+record
    // max is well under node_size) — reject up front rather than loop forever.
    let payload = node_size.saturating_sub(16);
    for rec in &records {
        if rec.len() + 2 > payload {
            return Err(FilesystemError::InvalidData(format!(
                "btree_split_leaf_with_insert: record of {} bytes exceeds node payload {}",
                rec.len(),
                payload
            )));
        }
    }

    // Greedy "pack-left-full" partition by bytes. `cuts` holds the record index
    // at which each chunk after the first begins (chunk count = cuts.len() + 1).
    let mut cuts: Vec<usize> = Vec::new();
    let mut cur_bytes: usize = 0;
    for (i, rec) in records.iter().enumerate() {
        let cost = rec.len() + 2;
        if i > 0 && cur_bytes + cost > payload {
            cuts.push(i);
            cur_bytes = 0;
        }
        cur_bytes += cost;
    }
    // Caller's contract: "I tried inserting and it failed, allocate me a new
    // node." `btree_insert_record`'s free-space check is 2 bytes more
    // conservative than the actual packing limit, so the merged set can fit a
    // single node by 1–2 bytes. We still must produce ≥ 1 new node — force a
    // 2-way split at the midpoint when greedy yielded a single chunk.
    if cuts.is_empty() {
        cuts.push(
            (records.len() / 2)
                .max(1)
                .min(records.len().saturating_sub(1)),
        );
    }

    // Density tuning. Greedy pack-left is ideal for *sequential* (append)
    // inserts — the left node stays full — but for a *random* insert into the
    // middle of a full leaf it splits off only the overflow (often a single
    // record), leaving a trail of ~1-record leaves; a catalog built that way
    // runs ~2x larger than Mac OS writes. When the new record landed in the
    // middle and the whole set fits two nodes, re-split at the most balanced
    // *valid* point instead, so both leaves keep room to fill (random inserts
    // then converge to ~69% full, matching a real Mac volume). Sequential growth
    // keeps the dense greedy path.
    let is_append = insert_pos == old_num;
    if cuts.len() == 1 && !is_append {
        let total: usize = records.iter().map(|r| r.len() + 2).sum();
        let mut prefix = 0usize;
        let mut best_k = cuts[0];
        let mut best_diff = usize::MAX;
        for k in 1..records.len() {
            prefix += records[k - 1].len() + 2;
            let right = total - prefix;
            if prefix <= payload && right <= payload {
                let diff = prefix.abs_diff(total / 2);
                if diff < best_diff {
                    best_diff = diff;
                    best_k = k;
                }
            }
        }
        cuts[0] = best_k;
    }

    // Materialize chunks by draining `records` at the cut points (front to back).
    let mut chunks: Vec<Vec<Vec<u8>>> = Vec::with_capacity(cuts.len() + 1);
    let mut prev = 0usize;
    for &cut in &cuts {
        chunks.push(records.drain(0..cut - prev).collect());
        prev = cut;
    }
    chunks.push(records);

    // Allocate new nodes (one per extra chunk beyond the first, which reuses old_idx).
    let new_count = chunks.len() - 1;
    let mut new_indices: Vec<u32> = Vec::with_capacity(new_count);
    for _ in 0..new_count {
        let idx = btree_alloc_node(catalog_data, node_size, header.total_nodes)?;
        header.free_nodes -= 1;
        init_node(catalog_data, node_size, idx, BTREE_LEAF_NODE, old_height);
        new_indices.push(idx);
    }

    // Helper to write a chunk into a node at `target_idx`, preserving any
    // existing fwd/back links written by `init_node` (caller patches them
    // afterwards via the leaf-chain splice).
    let write_chunk = |data: &mut [u8], target_idx: u32, chunk: &[Vec<u8>]| {
        let off = target_idx as usize * node_size;
        let node = &mut data[off..off + node_size];
        BigEndian::write_u16(&mut node[10..12], 0);
        BigEndian::write_u16(&mut node[node_size - 2..node_size], 14);
        let mut write_pos = 14usize;
        for (i, rec) in chunk.iter().enumerate() {
            node[write_pos..write_pos + rec.len()].copy_from_slice(rec);
            let opos = node_size - 2 * (i + 1);
            BigEndian::write_u16(&mut node[opos..opos + 2], write_pos as u16);
            write_pos += rec.len();
        }
        let fpos = node_size - 2 * (chunk.len() + 1);
        BigEndian::write_u16(&mut node[fpos..fpos + 2], write_pos as u16);
        BigEndian::write_u16(&mut node[10..12], chunk.len() as u16);
    };

    // First chunk goes back into the old node.
    write_chunk(catalog_data, node_idx, &chunks[0]);
    for (i, idx) in new_indices.iter().enumerate() {
        write_chunk(catalog_data, *idx, &chunks[i + 1]);
    }

    // Splice the chain: old -> new_indices[0] -> new_indices[1] -> ... -> old_next.
    let old_next = BigEndian::read_u32(&catalog_data[old_offset..old_offset + 4]);
    let mut prev_idx = node_idx;
    for &idx in &new_indices {
        let prev_off = prev_idx as usize * node_size;
        let cur_off = idx as usize * node_size;
        BigEndian::write_u32(&mut catalog_data[prev_off..prev_off + 4], idx);
        BigEndian::write_u32(&mut catalog_data[cur_off + 4..cur_off + 8], prev_idx);
        prev_idx = idx;
    }
    // Last new node points to old_next, and old_next.back -> last new node.
    let last_idx = *new_indices.last().expect("at least one new node");
    let last_off = last_idx as usize * node_size;
    BigEndian::write_u32(&mut catalog_data[last_off..last_off + 4], old_next);
    if old_next != 0 {
        let next_offset = old_next as usize * node_size;
        BigEndian::write_u32(
            &mut catalog_data[next_offset + 4..next_offset + 8],
            last_idx,
        );
    }
    if header.last_leaf_node == node_idx {
        header.last_leaf_node = last_idx;
    }

    // Collect separator keys: first record of each new chunk. The key portion
    // (length prefix + key bytes) is format-dependent — 1-byte length for
    // classic HFS, 2-byte for HFS+ — so read it through `kf` rather than
    // assuming `first[0]` is the whole length.
    let mut results: Vec<(u32, Vec<u8>)> = Vec::with_capacity(new_count);
    for (i, idx) in new_indices.iter().enumerate() {
        let split_key = kf.key_portion(&chunks[i + 1][0]);
        results.push((*idx, split_key));
    }
    Ok(results)
}

/// Overwrite the record area of node `idx` with `recs` (in key order),
/// preserving the node descriptor (sibling links, kind, height). Mirrors the
/// rebuild step of [`btree_insert_record`] but takes a ready-made record list —
/// used by the sibling-rotation path, which rewrites both nodes of a pair at
/// once. The caller guarantees `recs` fits within the node payload.
fn btree_write_node_records(data: &mut [u8], node_size: usize, idx: u32, recs: &[Vec<u8>]) {
    let off = idx as usize * node_size;
    let node = &mut data[off..off + node_size];
    let mut write_pos = 14usize;
    for (i, rec) in recs.iter().enumerate() {
        node[write_pos..write_pos + rec.len()].copy_from_slice(rec);
        let opos = node_size - 2 * (i + 1);
        BigEndian::write_u16(&mut node[opos..opos + 2], write_pos as u16);
        write_pos += rec.len();
    }
    let fpos = node_size - 2 * (recs.len() + 1);
    BigEndian::write_u16(&mut node[fpos..fpos + 2], write_pos as u16);
    if write_pos < fpos {
        node[write_pos..fpos].fill(0);
    }
    BigEndian::write_u16(&mut node[10..12], recs.len() as u16);
}

/// True when index node `parent_idx` holds a record whose trailing 4-byte
/// child pointer equals `child` — i.e. `child` is a direct child of that index
/// node, so the two share `parent_idx` as a parent.
fn index_node_has_child(data: &[u8], node_size: usize, parent_idx: u32, child: u32) -> bool {
    if parent_idx == 0 {
        return false;
    }
    let off = parent_idx as usize * node_size;
    if off + node_size > data.len() {
        return false;
    }
    let num = BigEndian::read_u16(&data[off + 10..off + 12]) as usize;
    for i in 0..num {
        let (s, e) = btree_record_range(&data[off..off + node_size], node_size, i);
        if e < s + 4 || e > node_size {
            continue;
        }
        if BigEndian::read_u32(&data[off + e - 4..off + e]) == child {
            return true;
        }
    }
    false
}

/// In index node `parent_idx`, rewrite (in place) the separator key of the
/// record pointing at `child` so it equals `new_first_key` (a record's key
/// portion: the `key_len` byte plus key bytes). Catalog index keys are a fixed
/// 0x25-length normalized key, so the record length never changes and the
/// offset table / sibling records are untouched.
///
/// Returns `true` only when the separator was found *and* the in-place rewrite
/// was valid (the existing key portion is the same length as the normalized
/// key). Returns `false` — writing nothing — when `child` isn't a direct child
/// of `parent_idx`, or when the existing separator is a different length (a
/// real variable-length HFS+ index key, which can't be patched in place). The
/// caller uses the `false` result to abandon a rotation and split instead, so a
/// non-classic index is never left with a stale separator.
#[must_use]
fn btree_update_index_separator(
    data: &mut [u8],
    node_size: usize,
    parent_idx: u32,
    child: u32,
    new_first_key: &[u8],
    kf: &BTreeKeyFormat,
) -> bool {
    let off = parent_idx as usize * node_size;
    if off + node_size > data.len() {
        return false;
    }
    let num = BigEndian::read_u16(&data[off + 10..off + 12]) as usize;
    for i in 0..num {
        let (s, e) = btree_record_range(&data[off..off + node_size], node_size, i);
        if e < s + 4 || e > node_size {
            continue;
        }
        if BigEndian::read_u32(&data[off + e - 4..off + e]) == child {
            // The replacement separator must occupy the same number of bytes as
            // the existing one — only then is an in-place patch valid (the
            // offset table and sibling records stay put). Classic HFS index keys
            // are a fixed 0x25 length so this always holds; an HFS+ variable key
            // matches only when the new first key has the same length as the old
            // separator. When it differs we return `false` and the caller
            // abandons the rotation and splits instead, so a stale separator is
            // never left behind.
            let new_key = kf.make_index_key(new_first_key);
            let key_bytes = e - s - 4;
            if new_key.len() != key_bytes {
                return false;
            }
            data[off + s..off + s + key_bytes].copy_from_slice(&new_key);
            return true;
        }
    }
    false
}

/// Try to absorb `new_record` into a full leaf by redistributing records with
/// an adjacent sibling that shares the same parent index node and still has
/// room — a B*-tree rotation — instead of splitting.
///
/// A plain 50/50 leaf split leaves non-sequential inserts (the per-`put`
/// workload, where keys land mid-leaf rather than appending) at the classic
/// ~69% B-tree occupancy, so a catalog grown one record at a time exhausts its
/// node budget ~1.4-2.7x sooner than a bulk/sequential build and can hit the
/// file's node ceiling far below the volume's real capacity. Rotating into a
/// neighbour before splitting lifts random-insert occupancy to ~88%, so the
/// incremental path packs nearly as tightly as the sequential one.
///
/// Returns `true` when the record was absorbed: no node is allocated and the
/// tree shape is unchanged — only the two nodes' records and one parent
/// separator key move. Returns `false` when no sibling can take the overflow
/// and the caller must fall back to [`btree_split_leaf_with_insert`].
///
/// Safety of the in-place separator update: we only ever rewrite the separator
/// of the pair's *right* node, and only when the pair's *left* node keeps its
/// original first key (guarded below). The right node is never the parent's
/// first child (a right sibling sorts after `leaf_idx`; a left sibling sharing
/// the parent means `leaf_idx` isn't first), so the parent's minimum key — and
/// the grandparent invariant that mirrors it — is never disturbed.
fn btree_try_rotate_leaf<F>(
    data: &mut [u8],
    node_size: usize,
    leaf_idx: u32,
    parent_idx: u32,
    new_record: &[u8],
    kf: &BTreeKeyFormat,
    cmp: &F,
) -> bool
where
    F: Fn(&[u8], &[u8]) -> Ordering,
{
    let payload = node_size.saturating_sub(16);
    let cost = |r: &Vec<u8>| r.len() + 2;
    let first_key = |r: &[u8]| -> Vec<u8> { kf.key_portion(r) };
    let read_records = |data: &[u8], idx: u32| -> Vec<Vec<u8>> {
        let off = idx as usize * node_size;
        let num = BigEndian::read_u16(&data[off + 10..off + 12]) as usize;
        let mut v = Vec::with_capacity(num);
        for i in 0..num {
            let (s, e) = btree_record_range(&data[off..off + node_size], node_size, i);
            v.push(data[off + s..off + e].to_vec());
        }
        v
    };

    let loff = leaf_idx as usize * node_size;
    let rsib = BigEndian::read_u32(&data[loff..loff + 4]);
    let lsib = BigEndian::read_u32(&data[loff + 4..loff + 8]);
    let leaf_recs = read_records(data, leaf_idx);

    // Prefer the right sibling, then the left. For each candidate, pool the two
    // siblings' records plus `new_record` in key order, then redistribute them
    // balanced across the pair so both nodes keep headroom.
    for (sib_is_right, sib) in [(true, rsib), (false, lsib)] {
        if sib == 0 || !index_node_has_child(data, node_size, parent_idx, sib) {
            continue;
        }
        let sib_recs = read_records(data, sib);
        let (left_idx, right_idx, left_recs, right_recs) = if sib_is_right {
            (leaf_idx, sib, &leaf_recs, &sib_recs)
        } else {
            (sib, leaf_idx, &sib_recs, &leaf_recs)
        };
        let Some(left_first) = left_recs.first() else {
            continue;
        };
        // Build the merged, key-ordered record set across the pair.
        let mut pool: Vec<Vec<u8>> = left_recs.iter().chain(right_recs.iter()).cloned().collect();
        let mut p = pool.len();
        for (i, r) in pool.iter().enumerate() {
            if cmp(r, new_record) == Ordering::Greater {
                p = i;
                break;
            }
        }
        pool.insert(p, new_record.to_vec());
        // Skip if the left node's minimum would change — its parent separator
        // (which we don't touch) would then be stale. Happens only when the new
        // record sorts before the whole pair; rare, and a normal split handles
        // it correctly.
        if cmp(&pool[0], left_first) != Ordering::Equal {
            continue;
        }
        let total: usize = pool.iter().map(cost).sum();
        if total > 2 * payload {
            continue; // neighbour is too full to absorb the overflow
        }
        // Balanced split point: both halves within payload, closest to total/2.
        let mut prefix = 0usize;
        let mut best_p = 0usize;
        let mut best_diff = usize::MAX;
        for cut in 1..pool.len() {
            prefix += cost(&pool[cut - 1]);
            let right = total - prefix;
            if prefix <= payload && right <= payload {
                let diff = prefix.abs_diff(total / 2);
                if diff < best_diff {
                    best_diff = diff;
                    best_p = cut;
                }
            }
        }
        if best_p == 0 {
            continue;
        }
        let left: Vec<Vec<u8>> = pool[..best_p].to_vec();
        let right: Vec<Vec<u8>> = pool[best_p..].to_vec();
        let right_first = first_key(&right[0]);
        // Patch the parent separator first: if it can't be updated in place
        // (a variable-length, non-normalized index key — i.e. a real HFS+
        // catalog), abandon the rotation untouched and let the caller split,
        // rather than leave the moved records behind a stale separator.
        if !btree_update_index_separator(data, node_size, parent_idx, right_idx, &right_first, kf) {
            continue;
        }
        btree_write_node_records(data, node_size, left_idx, &left);
        btree_write_node_records(data, node_size, right_idx, &right);
        return true;
    }
    false
}

/// Split a full index node while inserting `new_record` (a normalized index
/// record: a 0x25-length key followed by a 4-byte child pointer), append-aware.
///
/// The previous index split was a fixed 50/50 of the existing records, then the
/// new record went into one half. For *sequential* separator inserts — which is
/// what every leaf split produces when files arrive in key order (a bulk
/// `untar`, or `put`ting a sorted batch) — that froze each non-rightmost index
/// node at ~50%, doubling the index node count and adding a whole tree level.
/// Packing the left node full on an append (and rebalancing only a genuine
/// middle insert), exactly as the leaf split does, keeps the index dense.
///
/// Returns `(new_node_idx, separator_key)` to install in the grandparent. Index
/// records are uniform and small, so a 2-way split always suffices (an overflow
/// is at most one record over a node).
fn btree_split_index_with_insert<F>(
    catalog_data: &mut [u8],
    node_size: usize,
    node_idx: u32,
    header: &mut BTreeHeader,
    new_record: &[u8],
    kf: &BTreeKeyFormat,
    compare_fn: &F,
) -> Result<(u32, Vec<u8>), FilesystemError>
where
    F: Fn(&[u8], &[u8]) -> Ordering,
{
    let old_offset = node_idx as usize * node_size;
    let old_num = BigEndian::read_u16(&catalog_data[old_offset + 10..old_offset + 12]) as usize;
    let old_height = catalog_data[old_offset + 9];

    // Existing records merged with new_record, in key order.
    let mut records: Vec<Vec<u8>> = Vec::with_capacity(old_num + 1);
    for i in 0..old_num {
        let (s, e) = btree_record_range(
            &catalog_data[old_offset..old_offset + node_size],
            node_size,
            i,
        );
        records.push(catalog_data[old_offset + s..old_offset + e].to_vec());
    }
    let mut insert_pos = records.len();
    for (i, rec) in records.iter().enumerate() {
        if compare_fn(rec, new_record) == Ordering::Greater {
            insert_pos = i;
            break;
        }
    }
    records.insert(insert_pos, new_record.to_vec());

    let payload = node_size.saturating_sub(16);
    let cost = |r: &Vec<u8>| r.len() + 2;
    let total: usize = records.iter().map(cost).sum();

    // Greedy pack-left (dense for appends). `cut` = count kept in the old node.
    let mut cut = {
        let mut acc = 0usize;
        let mut k = 1usize;
        for (i, r) in records.iter().enumerate() {
            let c = cost(r);
            if i > 0 && acc + c > payload {
                break;
            }
            acc += c;
            k = i + 1;
        }
        k.clamp(1, records.len() - 1)
    };
    // A genuine middle insert rebalances so both halves keep room (matches the
    // leaf split's density tuning); a tail append keeps the dense pack-left.
    let is_append = insert_pos == old_num;
    if !is_append {
        let mut prefix = 0usize;
        let mut best = cut;
        let mut best_diff = usize::MAX;
        for k in 1..records.len() {
            prefix += cost(&records[k - 1]);
            let right = total - prefix;
            if prefix <= payload && right <= payload {
                let diff = prefix.abs_diff(total / 2);
                if diff < best_diff {
                    best_diff = diff;
                    best = k;
                }
            }
        }
        cut = best;
    }

    let new_idx = btree_alloc_node(catalog_data, node_size, header.total_nodes)?;
    header.free_nodes -= 1;
    init_node(
        catalog_data,
        node_size,
        new_idx,
        BTREE_INDEX_NODE,
        old_height,
    );

    let left: Vec<Vec<u8>> = records[..cut].to_vec();
    let right: Vec<Vec<u8>> = records[cut..].to_vec();
    btree_write_node_records(catalog_data, node_size, node_idx, &left);
    btree_write_node_records(catalog_data, node_size, new_idx, &right);

    // Splice the index-level sibling chain: node_idx <-> new_idx <-> old_next.
    // `btree_write_node_records` preserved node_idx's old forward link.
    let old_next = BigEndian::read_u32(&catalog_data[old_offset..old_offset + 4]);
    let new_offset = new_idx as usize * node_size;
    BigEndian::write_u32(&mut catalog_data[old_offset..old_offset + 4], new_idx);
    BigEndian::write_u32(&mut catalog_data[new_offset + 4..new_offset + 8], node_idx);
    BigEndian::write_u32(&mut catalog_data[new_offset..new_offset + 4], old_next);
    if old_next != 0 {
        let next_off = old_next as usize * node_size;
        BigEndian::write_u32(&mut catalog_data[next_off + 4..next_off + 8], new_idx);
    }

    // Separator = first key portion (length prefix + key bytes) of the new node.
    let split_key = kf.key_portion(&right[0]);
    Ok((new_idx, split_key))
}

/// Insert a separator key into a parent index node, pointing to child_node.
///
/// For index nodes, each record is: key_bytes + child_node_ptr (4 bytes BE).
/// If the parent is full, recursively splits and inserts upward.
///
/// `parent_chain` is the chain of (node_idx, parent_of_node_idx) from root to the
/// index node. We need this to find the grandparent when the index node itself splits.
#[allow(clippy::too_many_arguments)] // B-tree index insert threads tree state + cmp + parent_chain
pub fn btree_insert_into_index<F>(
    catalog_data: &mut [u8],
    node_size: usize,
    index_node_idx: u32,
    child_node: u32,
    split_key: &[u8],
    header: &mut BTreeHeader,
    kf: &BTreeKeyFormat,
    compare_fn: &F,
    parent_chain: &[(u32, u32)], // [(node_idx, parent_idx), ...]
) -> Result<(), FilesystemError>
where
    F: Fn(&[u8], &[u8]) -> Ordering,
{
    // Build the index record: index key + child_pointer. For classic HFS the
    // index key is the fixed 0x25-length normalized key (record always 42
    // bytes); for an HFS+ variable-index-key tree it is the child's first key
    // verbatim (2-byte length + key bytes). `make_index_key` picks the right
    // shape per `kf`. Catalog/attributes keys are even-length, so the trailing
    // child pointer always lands on an even offset.
    let mut index_record = kf.make_index_key(split_key);
    let mut ptr = [0u8; 4];
    BigEndian::write_u32(&mut ptr, child_node);
    index_record.extend_from_slice(&ptr);

    let offset = index_node_idx as usize * node_size;
    let node = &mut catalog_data[offset..offset + node_size];

    // Try inserting
    match btree_insert_record(node, node_size, &index_record, compare_fn) {
        Ok(_) => Ok(()),
        Err(_) => {
            // Index node is full. Before allocating a node, try a B*-style
            // rotation into an index sibling that shares this node's parent and
            // has room — the same density win applied at the leaf level, so a
            // deep or sequential catalog doesn't accumulate half-full index
            // nodes (and the extra level they imply). The root has no parent and
            // grows instead; `btree_try_rotate_leaf` is record-agnostic and
            // updates the parent separator in place, so it serves index nodes
            // just as it does leaves.
            if let Some(&(_, parent_idx)) = parent_chain
                .iter()
                .find(|&&(nidx, _)| nidx == index_node_idx)
            {
                if parent_idx != 0
                    && btree_try_rotate_leaf(
                        catalog_data,
                        node_size,
                        index_node_idx,
                        parent_idx,
                        &index_record,
                        kf,
                        compare_fn,
                    )
                {
                    return Ok(());
                }
            }

            // No sibling could help — split (append-aware), placing the new
            // record in the appropriate half, then install the new node's
            // separator in the parent.
            let (new_idx, new_split_key) = btree_split_index_with_insert(
                catalog_data,
                node_size,
                index_node_idx,
                header,
                &index_record,
                kf,
                compare_fn,
            )?;

            // Now insert new_split_key into this index node's parent
            // Find our parent from the chain
            if let Some(&(_, parent_idx)) = parent_chain
                .iter()
                .find(|&&(nidx, _)| nidx == index_node_idx)
            {
                if parent_idx == 0 && index_node_idx == header.root_node {
                    // The root itself split — grow the tree
                    btree_grow_root(
                        catalog_data,
                        node_size,
                        header,
                        index_node_idx,
                        new_idx,
                        &new_split_key,
                        kf,
                    )?;
                } else {
                    // Recurse to parent index node
                    btree_insert_into_index(
                        catalog_data,
                        node_size,
                        parent_idx,
                        new_idx,
                        &new_split_key,
                        header,
                        kf,
                        compare_fn,
                        parent_chain,
                    )?;
                }
            } else {
                // No parent found — this must be root
                btree_grow_root(
                    catalog_data,
                    node_size,
                    header,
                    index_node_idx,
                    new_idx,
                    &new_split_key,
                    kf,
                )?;
            }
            Ok(())
        }
    }
}

/// Grow the B-tree root: create a new root node at height+1 with two children.
pub fn btree_grow_root(
    catalog_data: &mut [u8],
    node_size: usize,
    header: &mut BTreeHeader,
    old_root: u32,
    new_sibling: u32,
    split_key: &[u8],
    kf: &BTreeKeyFormat,
) -> Result<(), FilesystemError> {
    let new_root = btree_alloc_node(catalog_data, node_size, header.total_nodes)?;
    header.free_nodes -= 1;

    let old_root_offset = old_root as usize * node_size;
    let old_height = catalog_data[old_root_offset + 9];

    init_node(
        catalog_data,
        node_size,
        new_root,
        BTREE_INDEX_NODE,
        old_height + 1,
    );

    let new_root_offset = new_root as usize * node_size;

    // Build two index records:
    // 1. First child (old_root): use the first key of old_root + pointer to old_root
    // 2. Second child (new_sibling): split_key + pointer to new_sibling

    // Get the first key portion of old_root (length prefix + key bytes, read
    // through `kf` so the 1-byte vs 2-byte length is handled correctly).
    let (first_rec_start, first_rec_end) = btree_record_range(
        &catalog_data[old_root_offset..old_root_offset + node_size],
        node_size,
        0,
    );
    let first_key = kf.key_portion(
        &catalog_data[old_root_offset + first_rec_start..old_root_offset + first_rec_end],
    );

    // Record 1: index key for first_key + old_root pointer.
    let mut rec1 = kf.make_index_key(&first_key);
    let mut ptr1 = [0u8; 4];
    BigEndian::write_u32(&mut ptr1, old_root);
    rec1.extend_from_slice(&ptr1);

    // Record 2: index key for split_key + new_sibling pointer.
    let mut rec2 = kf.make_index_key(split_key);
    let mut ptr2 = [0u8; 4];
    BigEndian::write_u32(&mut ptr2, new_sibling);
    rec2.extend_from_slice(&ptr2);

    // Write records manually into the new root node (we know it's empty)
    let node = &mut catalog_data[new_root_offset..new_root_offset + node_size];
    let mut write_pos = 14usize;
    // Record 0
    node[write_pos..write_pos + rec1.len()].copy_from_slice(&rec1);
    BigEndian::write_u16(&mut node[node_size - 2..node_size], 14);
    write_pos += rec1.len();
    // Record 1
    node[write_pos..write_pos + rec2.len()].copy_from_slice(&rec2);
    BigEndian::write_u16(&mut node[node_size - 4..node_size - 2], write_pos as u16);
    write_pos += rec2.len();
    // Free-space offset
    BigEndian::write_u16(&mut node[node_size - 6..node_size - 4], write_pos as u16);
    BigEndian::write_u16(&mut node[10..12], 2); // num_records = 2

    header.root_node = new_root;
    header.depth += 1;

    Ok(())
}

// ---------------------------------------------------------------------------
// B-tree search helpers
// ---------------------------------------------------------------------------

/// Walk the B-tree from root to leaf to find a record matching a key.
///
/// `key_extract_fn` extracts the comparable key portion from a record.
/// `compare_fn` compares (extracted_key, search_key) → Ordering.
///
/// Returns Some((node_idx, rec_idx, rec_data_offset_in_catalog_data))
/// or None if not found.
///
/// Also returns the parent chain for use in insertion: Vec<(node_idx, parent_idx)>.
#[allow(dead_code)]
#[allow(clippy::type_complexity)] // returns (find_result, parent_chain) tuples — flattening would obscure intent
pub fn btree_find_record<K, C>(
    catalog_data: &[u8],
    header: &BTreeHeader,
    search_key: &[u8],
    key_extract_fn: &K,
    compare_fn: &C,
) -> (Option<(u32, usize, usize)>, Vec<(u32, u32)>)
where
    K: Fn(&[u8]) -> &[u8],
    C: Fn(&[u8], &[u8]) -> Ordering,
{
    let node_size = header.node_size as usize;
    let mut parent_chain: Vec<(u32, u32)> = Vec::new();
    let mut current_node = header.root_node;
    let mut parent_node = 0u32;
    let mut visited = std::collections::HashSet::new();

    loop {
        // Cycle detection: bail if we revisit a node
        if !visited.insert(current_node) {
            return (None, parent_chain);
        }
        let offset = current_node as usize * node_size;
        if offset + node_size > catalog_data.len() {
            return (None, parent_chain);
        }
        let node = &catalog_data[offset..offset + node_size];
        let kind = node[8] as i8;
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;

        parent_chain.push((current_node, parent_node));

        if kind == BTREE_LEAF_NODE {
            // Search records for exact match
            for i in 0..num_records {
                let (start, end) = btree_record_range(node, node_size, i);
                if start >= end || end > node_size {
                    continue;
                }
                let rec_bytes = &node[start..end];
                let key = key_extract_fn(rec_bytes);
                match compare_fn(key, search_key) {
                    Ordering::Equal => {
                        return (Some((current_node, i, offset + start)), parent_chain);
                    }
                    Ordering::Greater => return (None, parent_chain),
                    _ => {}
                }
            }
            return (None, parent_chain);
        } else if kind == BTREE_INDEX_NODE {
            // Find the child pointer to descend into
            // Index records: key + child_node_ptr(4 bytes at end)
            let mut next_child = 0u32;
            for i in 0..num_records {
                let (start, end) = btree_record_range(node, node_size, i);
                if end < start + 4 || end > node_size {
                    continue;
                }
                let rec_bytes = &node[start..end];
                let key = key_extract_fn(rec_bytes);
                // Child pointer is last 4 bytes of the record
                let child_ptr = BigEndian::read_u32(&node[end - 4..end]);

                match compare_fn(key, search_key) {
                    Ordering::Greater => {
                        // This key is beyond our search key; use the previous child
                        break;
                    }
                    _ => {
                        next_child = child_ptr;
                    }
                }
            }
            if next_child == 0 && num_records > 0 {
                // Use the last child
                let (_, end) = btree_record_range(node, node_size, num_records - 1);
                next_child = BigEndian::read_u32(&node[end - 4..end]);
            }
            parent_node = current_node;
            current_node = next_child;
        } else {
            return (None, parent_chain);
        }
    }
}

/// Find the leaf node and position where a key should be inserted.
/// Returns (leaf_node_idx, parent_chain).
pub fn btree_find_insert_leaf<C>(
    catalog_data: &[u8],
    header: &BTreeHeader,
    search_key: &[u8],
    compare_fn: &C,
) -> (u32, Vec<(u32, u32)>)
where
    C: Fn(&[u8], &[u8]) -> Ordering,
{
    let node_size = header.node_size as usize;
    let mut parent_chain: Vec<(u32, u32)> = Vec::new();
    let mut current_node = header.root_node;
    let mut parent_node = 0u32;
    let mut visited = std::collections::HashSet::new();

    loop {
        // Cycle detection: bail to first leaf if we revisit a node
        if !visited.insert(current_node) {
            return (header.first_leaf_node, parent_chain);
        }
        let offset = current_node as usize * node_size;
        if offset + node_size > catalog_data.len() {
            return (current_node, parent_chain);
        }
        let node = &catalog_data[offset..offset + node_size];
        let kind = node[8] as i8;
        let num_records = BigEndian::read_u16(&node[10..12]) as usize;

        parent_chain.push((current_node, parent_node));

        if kind == BTREE_LEAF_NODE {
            return (current_node, parent_chain);
        }

        // Index node: find the right child
        let mut next_child = 0u32;
        for i in 0..num_records {
            let (start, end) = btree_record_range(node, node_size, i);
            if end < start + 4 || end > node_size {
                continue;
            }
            // For HFS+ catalog keys, the key is everything up to the last 4 bytes
            let key_portion = &node[start..end - 4];
            let child_ptr = BigEndian::read_u32(&node[end - 4..end]);

            if compare_fn(key_portion, search_key) != Ordering::Greater {
                next_child = child_ptr;
            } else {
                break;
            }
        }
        if next_child == 0 && num_records > 0 {
            let (_, end) = btree_record_range(node, node_size, num_records - 1);
            next_child = BigEndian::read_u32(&node[end - 4..end]);
        }

        parent_node = current_node;
        current_node = next_child;
    }
}

/// Insert a fully-formed `key + record` pair into a B-tree backed by
/// `data` (header lives in node 0 at offset 14, leaves chained from
/// `first_leaf_node`). Drives `btree_find_insert_leaf`,
/// `btree_insert_record`, and the `btree_split_leaf_with_insert` /
/// `btree_grow_root` / `btree_insert_into_index` machinery so callers
/// don't have to repeat the (find leaf, try insert, split on full,
/// fix up parent or grow root) dance.
///
/// `cmp` compares two records by their key portion only — it must
/// match whatever ordering the tree was built with (case-folding NFD
/// for HFS+ catalog, binary for HFSX, case-insensitive Mac-Roman for
/// classic HFS, raw byte order for extents-overflow / attributes).
///
/// On success the header's `leaf_records` is bumped and (for splits)
/// `depth` / `root_node` / `last_leaf_node` are updated. Returns
/// `Err(DiskFull)` when the B-tree is out of free nodes.
///
/// Used by the streamed defrag builder (Step 22c) to populate target
/// catalog / extents-overflow / attributes B-trees from scratch, and
/// matches the algorithm `HfsPlusFilesystem::insert_catalog_record`
/// has been using.
pub fn btree_insert_full<F>(
    data: &mut [u8],
    key_record: &[u8],
    kf: &BTreeKeyFormat,
    cmp: &F,
) -> Result<(), super::filesystem::FilesystemError>
where
    F: Fn(&[u8], &[u8]) -> Ordering,
{
    let mut header = BTreeHeader::read(data);
    let node_size = header.node_size as usize;
    if node_size == 0 {
        return Err(super::filesystem::FilesystemError::InvalidData(
            "B-tree has zero node_size".into(),
        ));
    }

    // Bootstrap an empty (depth-0) tree: a fresh extents-overflow / attributes
    // B-tree carries only its header node until the first record arrives
    // (matching Apple's newfs_hfs, which lays out empty trees as
    // depth=0/root=0 with no leaf). Allocate and initialise the root leaf so
    // the normal insert path below has somewhere to land.
    if header.depth == 0 || header.root_node == 0 {
        let leaf = btree_alloc_node(data, node_size, header.total_nodes)?;
        let off = leaf as usize * node_size;
        let node = &mut data[off..off + node_size];
        node.fill(0);
        node[8] = 0xFF; // kind = -1 (leaf)
        node[9] = 1; // height = 1
        BigEndian::write_u16(&mut node[10..12], 0); // numRecords = 0
        BigEndian::write_u16(&mut node[node_size - 2..node_size], 14); // free-space offset
        header.depth = 1;
        header.root_node = leaf;
        header.first_leaf_node = leaf;
        header.last_leaf_node = leaf;
        header.free_nodes = header.free_nodes.saturating_sub(1);
        header.write(data);
    }

    let (leaf_idx, parent_chain) = btree_find_insert_leaf(data, &header, key_record, cmp);

    let off = leaf_idx as usize * node_size;
    let node = &mut data[off..off + node_size];
    match btree_insert_record(node, node_size, key_record, cmp) {
        Ok(_) => {
            let mut h = BTreeHeader::read(data);
            h.leaf_records += 1;
            h.write(data);
            Ok(())
        }
        Err(_) => {
            let mut h = BTreeHeader::read(data);
            // Before allocating a node, try a B*-style rotation into an adjacent
            // sibling that shares this leaf's parent and still has room. This
            // keeps the catalog densely packed for non-sequential (per-`put`)
            // inserts; only when no sibling can help do we split. Needs a parent
            // index node, so skip it while the root is still the lone leaf.
            if h.depth > 1 {
                if let Some(&(_, parent_idx)) =
                    parent_chain.iter().find(|&&(nidx, _)| nidx == leaf_idx)
                {
                    if parent_idx != 0
                        && btree_try_rotate_leaf(
                            data, node_size, leaf_idx, parent_idx, key_record, kf, cmp,
                        )
                    {
                        h.leaf_records += 1;
                        h.write(data);
                        return Ok(());
                    }
                }
            }
            // A single-record insert into a previously-valid leaf splits at
            // most 2-way (one new leaf), and the separator can cascade up,
            // splitting one node per level plus a possible new root — at most
            // `depth + 1` new nodes total. If the tree can't cover that exact
            // worst case, bail *before* touching anything (the
            // `btree_insert_record` above failed without mutating the full leaf,
            // and a failed rotation leaves the tree unchanged), so a caller that
            // grows the fork and retries can't end up with a half-applied split
            // — which would duplicate the record. This makes DiskFull from here
            // a clean no-op (see the grow-on-full retry in
            // `HfsPlusFilesystem::insert_catalog_record` et al.). The bound is
            // exact, not padded, so classic HFS (which has no grow path) still
            // fills its pre-sized catalog right up to genuine capacity.
            if h.free_nodes < h.depth as u32 + 1 {
                return Err(super::filesystem::FilesystemError::DiskFull(
                    "no free B-tree nodes".into(),
                ));
            }
            let splits = btree_split_leaf_with_insert(
                data, node_size, leaf_idx, &mut h, key_record, kf, cmp,
            )?;
            h.leaf_records += 1;
            // First new node's separator goes via either grow_root (depth==1)
            // or insert_into_index. Any additional separators (N-way splits
            // with N > 2) always go through insert_into_index — the tree's
            // root depth is already > 1 once the first separator installed.
            let first = &splits[0];
            if h.depth == 1 {
                btree_grow_root(data, node_size, &mut h, leaf_idx, first.0, &first.1, kf)?;
            } else if let Some(&(_, parent_idx)) =
                parent_chain.iter().find(|&&(nidx, _)| nidx == leaf_idx)
            {
                btree_insert_into_index(
                    data,
                    node_size,
                    parent_idx,
                    first.0,
                    &first.1,
                    &mut h,
                    kf,
                    cmp,
                    &parent_chain,
                )?;
            } else {
                btree_grow_root(data, node_size, &mut h, leaf_idx, first.0, &first.1, kf)?;
            }
            for split in &splits[1..] {
                // After the first separator installed, the leaf's parent is
                // either the new root (depth grew to ≥ 2) or whatever was in
                // `parent_chain`. Re-resolve by walking down from root via the
                // separator key.
                let header_now = BTreeHeader::read(data);
                let (_, fresh_chain) = btree_find_insert_leaf(data, &header_now, &split.1, cmp);
                // The leaf the separator key would land in shares a parent
                // with `leaf_idx` (the original split source). Use the last
                // entry of fresh_chain as the parent — that's the deepest
                // index node above any leaf along the search path.
                let parent_for_sep = fresh_chain
                    .last()
                    .map(|&(_, p)| p)
                    .unwrap_or(header_now.root_node);
                btree_insert_into_index(
                    data,
                    node_size,
                    parent_for_sep,
                    split.0,
                    &split.1,
                    &mut h,
                    kf,
                    cmp,
                    &fresh_chain,
                )?;
            }
            h.write(data);
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_mac_date() {
        // Zero == "no date set".
        assert_eq!(format_mac_date(0), None);
        // The Unix epoch in Mac-epoch seconds is MAC_EPOCH_DELTA.
        assert_eq!(
            format_mac_date(MAC_EPOCH_DELTA as u32).as_deref(),
            Some("1970-01-01 00:00:00")
        );
        // A real vintage Mac timestamp: 2000-01-01 00:00:00 UTC.
        assert_eq!(
            format_mac_date((MAC_EPOCH_DELTA + 946_684_800) as u32).as_deref(),
            Some("2000-01-01 00:00:00")
        );
    }

    #[test]
    fn test_parse_mac_date_round_trips() {
        // Empty / "0" map to the "no date" sentinel.
        assert_eq!(parse_mac_date(""), Some(0));
        assert_eq!(parse_mac_date("0"), Some(0));
        // Round-trips a real timestamp through format -> parse.
        let mac = (MAC_EPOCH_DELTA + 946_684_800) as u32; // 2000-01-01
        let s = format_mac_date(mac).unwrap();
        assert_eq!(parse_mac_date(&s), Some(mac));
        // Garbage is rejected.
        assert_eq!(parse_mac_date("not a date"), None);
    }

    #[test]
    fn test_parse_mac_date_checked_specific_errors() {
        assert!(parse_mac_date_checked("1997-13-01 11:09:34")
            .unwrap_err()
            .contains("month 13"));
        assert!(parse_mac_date_checked("1997-02-29 11:09:34")
            .unwrap_err()
            .contains("day 29 is invalid for 1997-02"));
        assert!(parse_mac_date_checked("2000-01-01 25:00:00")
            .unwrap_err()
            .contains("out of range"));
        assert!(parse_mac_date_checked("garbage")
            .unwrap_err()
            .contains("expected YYYY-MM-DD"));
        // A leap day in an actual leap year is fine.
        assert!(parse_mac_date_checked("2000-02-29 12:00:00").is_ok());
    }

    // -- Bitmap tests --

    #[test]
    fn test_bitmap_set_clear_test_be() {
        let mut data = [0u8; 2];
        // MSB-first: bit 0 is the leftmost bit of byte 0
        bitmap_set_bit_be(&mut data, 0);
        assert_eq!(data[0], 0b10000000);
        assert!(bitmap_test_bit_be(&data, 0));
        assert!(!bitmap_test_bit_be(&data, 1));

        bitmap_set_bit_be(&mut data, 7);
        assert_eq!(data[0], 0b10000001);

        bitmap_set_bit_be(&mut data, 8);
        assert_eq!(data[1], 0b10000000);

        bitmap_clear_bit_be(&mut data, 0);
        assert_eq!(data[0], 0b00000001);
        assert!(!bitmap_test_bit_be(&data, 0));
    }

    #[test]
    fn test_bitmap_find_clear_bit_be() {
        let data = [0b11111110, 0b11111111]; // bit 7 is clear
        assert_eq!(bitmap_find_clear_bit_be(&data, 16), Some(7));

        let full = [0xFF, 0xFF];
        assert_eq!(bitmap_find_clear_bit_be(&full, 16), None);

        let empty = [0x00, 0x00];
        assert_eq!(bitmap_find_clear_bit_be(&empty, 16), Some(0));
    }

    #[test]
    fn test_bitmap_find_clear_run_be() {
        // bit pattern: 11100011 00000000 (BE: bit 0=MSB)
        let data = [0b11100011, 0b00000000];
        // BE bit indices: 0=1,1=1,2=1,3=0,4=0,5=0,6=1,7=1, 8..15=0
        // Bits 3,4,5 are clear (run of 3), bits 6,7 set, bits 8-15 all clear (run of 8).
        assert_eq!(bitmap_find_clear_run_be(&data, 16, 3), Some(3));
        // Run of 4: bits 3-5 only 3 clear, so run of 4 starts at bit 8.
        assert_eq!(bitmap_find_clear_run_be(&data, 16, 4), Some(8));
        assert_eq!(bitmap_find_clear_run_be(&data, 16, 8), Some(8));
        assert_eq!(bitmap_find_clear_run_be(&data, 16, 9), None);
    }

    #[test]
    fn test_bitmap_be_roundtrip() {
        let mut data = [0u8; 4];
        for i in 0..32u32 {
            bitmap_set_bit_be(&mut data, i);
        }
        assert_eq!(data, [0xFF; 4]);
        for i in 0..32u32 {
            assert!(bitmap_test_bit_be(&data, i));
            bitmap_clear_bit_be(&mut data, i);
        }
        assert_eq!(data, [0x00; 4]);
    }

    #[test]
    fn test_bitmap_single_bit() {
        let mut data = [0u8; 1];
        bitmap_set_bit_be(&mut data, 3);
        assert_eq!(data[0], 0b00010000);
        assert!(bitmap_test_bit_be(&data, 3));
        assert!(!bitmap_test_bit_be(&data, 2));
        assert!(!bitmap_test_bit_be(&data, 4));
    }

    // -- Mac epoch --

    #[test]
    fn test_hfs_now_sanity() {
        let now = hfs_now();
        // Should be well past 2024-01-01 in Mac time
        // 2024-01-01 00:00:00 UTC = Unix 1704067200 + 2082844800 = 3786912000
        assert!(now > 3_786_912_000);
    }

    // -- Type/creator lookup --

    #[test]
    fn test_type_creator_for_extension_known() {
        let (tc, cc) = type_creator_for_extension("txt").unwrap();
        assert_eq!(&tc, b"TEXT");
        assert_eq!(&cc, b"ttxt");
    }

    #[test]
    fn test_type_creator_for_extension_case_insensitive() {
        let (tc, cc) = type_creator_for_extension("TXT").unwrap();
        assert_eq!(&tc, b"TEXT");
        assert_eq!(&cc, b"ttxt");
    }

    #[test]
    fn test_type_creator_for_extension_unknown() {
        assert!(type_creator_for_extension("xyz123").is_none());
    }

    #[test]
    fn test_encode_fourcc() {
        assert_eq!(encode_fourcc("TEXT"), *b"TEXT");
        assert_eq!(encode_fourcc("AB"), *b"AB  ");
        assert_eq!(encode_fourcc("SIT!"), *b"SIT!");
    }

    #[test]
    fn test_encode_fourcc_roundtrip() {
        let code = encode_fourcc("JPEG");
        assert_eq!(&code, b"JPEG");
    }

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
        use super::super::hfs_unicode::decompose_str;
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
        // é (U+00E9, precomposed) vs e + combining acute (U+0065 U+0301).
        // Apple's decomposition folds the precomposed form to the decomposed
        // one, so the two keys must compare equal.
        let a: Vec<u16> = "é".encode_utf16().collect();
        let b: Vec<u16> = vec![0x0065, 0x0301];
        assert_eq!(compare_hfsplus_keys(2, &a, 2, &b, false), Ordering::Equal);
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

    // -- B-tree node manipulation --

    /// Create a minimal B-tree structure for testing: header node + one leaf node.
    fn make_test_btree(node_size: usize) -> Vec<u8> {
        let mut data = vec![0u8; node_size * 4]; // 4 nodes

        // Node 0: header node
        data[8] = BTREE_HEADER_NODE as u8; // kind
        data[9] = 0; // height
        BigEndian::write_u16(&mut data[10..12], 3); // 3 records (header, user, bitmap)

        // Header record at offset 14
        BigEndian::write_u16(&mut data[14..16], 1); // depth
        BigEndian::write_u32(&mut data[16..20], 1); // root_node = 1
        BigEndian::write_u32(&mut data[20..24], 0); // leaf_records = 0
        BigEndian::write_u32(&mut data[24..28], 1); // first_leaf = 1
        BigEndian::write_u32(&mut data[28..32], 1); // last_leaf = 1
        BigEndian::write_u16(&mut data[32..34], node_size as u16); // node_size
        BigEndian::write_u16(&mut data[34..36], 256); // max_key_len
        BigEndian::write_u32(&mut data[36..40], 4); // total_nodes
        BigEndian::write_u32(&mut data[40..44], 2); // free_nodes (nodes 2,3 are free)

        // Offset table for node 0 (3 records + free-space)
        let header_rec_offset = 14u16;
        let user_rec_offset = 120u16; // after header record
        let bitmap_rec_offset = 128u16; // after user record
        let free_offset = 256u16; // after bitmap record

        BigEndian::write_u16(&mut data[node_size - 2..node_size], header_rec_offset);
        BigEndian::write_u16(&mut data[node_size - 4..node_size - 2], user_rec_offset);
        BigEndian::write_u16(&mut data[node_size - 6..node_size - 4], bitmap_rec_offset);
        BigEndian::write_u16(&mut data[node_size - 8..node_size - 6], free_offset);

        // Bitmap in node 0: nodes 0,1 allocated, 2,3 free
        // bit 0 = node 0 (header), bit 1 = node 1 (leaf)
        data[bitmap_rec_offset as usize] = 0b11000000; // BE: bits 0,1 set

        // Node 1: empty leaf node
        let n1 = node_size;
        data[n1 + 8] = BTREE_LEAF_NODE as u8; // kind = -1
        data[n1 + 9] = 1; // height
        BigEndian::write_u16(&mut data[n1 + 10..n1 + 12], 0); // num_records = 0
        BigEndian::write_u16(&mut data[n1 + node_size - 2..n1 + node_size], 14); // free-space offset

        data
    }

    #[test]
    fn test_btree_node_free_space_empty_leaf() {
        let data = make_test_btree(512);
        let node = &data[512..1024]; // node 1
        let free = btree_node_free_space(node, 512);
        // Empty node: data starts at 14, offset table has 1 entry (2 bytes) at end
        // Free = 512 - 14 - 2 - 2 = 494
        assert_eq!(free, 494);
    }

    #[test]
    fn test_btree_insert_and_remove() {
        let mut data = make_test_btree(512);
        let node_size = 512;

        // Insert a record into node 1 (leaf)
        let rec1 = vec![0u8; 20]; // 20-byte record
        let rec2 = vec![1u8; 20];
        let compare = |a: &[u8], b: &[u8]| a.cmp(b);

        let node = &mut data[node_size..node_size * 2];
        let pos = btree_insert_record(node, node_size, &rec1, &compare).unwrap();
        assert_eq!(pos, 0);
        assert_eq!(BigEndian::read_u16(&node[10..12]), 1); // num_records = 1

        let pos2 = btree_insert_record(node, node_size, &rec2, &compare).unwrap();
        assert_eq!(pos2, 1); // rec2 > rec1
        assert_eq!(BigEndian::read_u16(&node[10..12]), 2);

        // Remove first record
        btree_remove_record(node, node_size, 0);
        assert_eq!(BigEndian::read_u16(&node[10..12]), 1);
    }

    #[test]
    fn test_btree_alloc_free_node() {
        let mut data = make_test_btree(512);
        let node_size = 512;

        // Allocate: should get node 2 (first free)
        let idx = btree_alloc_node(&mut data, node_size, 4).unwrap();
        assert_eq!(idx, 2);

        // Allocate again: should get node 3
        let idx2 = btree_alloc_node(&mut data, node_size, 4).unwrap();
        assert_eq!(idx2, 3);

        // No more free nodes
        assert!(btree_alloc_node(&mut data, node_size, 4).is_err());

        // Free node 2
        btree_free_node(&mut data, node_size, 2);
        let idx3 = btree_alloc_node(&mut data, node_size, 4).unwrap();
        assert_eq!(idx3, 2);
    }

    #[test]
    fn test_btree_header_read_write() {
        let mut data = make_test_btree(512);
        let header = BTreeHeader::read(&data);
        assert_eq!(header.depth, 1);
        assert_eq!(header.root_node, 1);
        assert_eq!(header.total_nodes, 4);
        assert_eq!(header.free_nodes, 2);

        // Modify and write back
        let mut header = header;
        header.leaf_records = 42;
        header.write(&mut data);

        let header2 = BTreeHeader::read(&data);
        assert_eq!(header2.leaf_records, 42);
    }

    #[test]
    fn test_btree_split_leaf_with_insert() {
        let mut data = make_test_btree(512);
        let node_size = 512;

        // Insert 10 fixed-size records (keys 0..10, even indices) into node 1.
        // 45-byte records: 10 × 47 = 470 + 14 + 2 = 486, fits one 512-byte node.
        // An 11th 45-byte record would push usage to 533 > 512 → forces a split.
        let compare = |a: &[u8], b: &[u8]| a.cmp(b);
        for i in 0..10u8 {
            let mut rec = vec![0u8; 45];
            rec[0] = i * 2;
            let node = &mut data[node_size..node_size * 2];
            btree_insert_record(node, node_size, &rec, &compare).unwrap();
        }

        let node = &data[node_size..node_size * 2];
        assert_eq!(BigEndian::read_u16(&node[10..12]), 10);

        // Atomic split + insert a new 45-byte record with key=5 (lands in left half).
        let mut new_rec = vec![0u8; 45];
        new_rec[0] = 5;
        let mut header = BTreeHeader::read(&data);
        let splits = btree_split_leaf_with_insert(
            &mut data,
            node_size,
            1,
            &mut header,
            &new_rec,
            &BTreeKeyFormat::CLASSIC_CATALOG,
            &compare,
        )
        .unwrap();
        assert_eq!(splits.len(), 1, "30-byte records: 2-way split is enough");
        let new_idx = splits[0].0;
        assert_eq!(new_idx, 2);

        let old_node = &data[node_size..node_size * 2];
        let new_node = &data[new_idx as usize * node_size..(new_idx as usize + 1) * node_size];
        let old_count = BigEndian::read_u16(&old_node[10..12]);
        let new_count = BigEndian::read_u16(&new_node[10..12]);
        assert_eq!(old_count + new_count, 11); // 10 existing + 1 inserted
    }
}
