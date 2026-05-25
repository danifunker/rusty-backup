//! Shared helpers for AmigaDOS Fast/Original File System (AFFS).
//!
//! All multi-byte fields are big-endian. The on-disk block size is fixed at
//! 512 bytes for floppies (`.adf`) and matches the RDB DosEnv block size for
//! hard-disk partitions (also 512 in practice).
//!
//! Reference: ADFlib (`src/adf_blk.h`, `src/adf_raw.c`) and the AmigaDOS
//! reference manual.

/// AFFS logical block size in bytes. Both ADF floppies and AmigaDOS hard
/// disks use 512 bytes — block sizes other than 512 require routing
/// changes in the dispatcher and are rejected by `AffsFilesystem::open`.
pub const BSIZE: usize = 512;
pub const BSIZE_U64: u64 = BSIZE as u64;

/// Hash table size for root/dir/userdir blocks (slots, each holding a u32).
pub const HT_SIZE: usize = 72;
/// Number of bitmap-block pointers stored inline in the root block.
pub const BM_PAGES_ROOT: usize = 25;
/// Number of data-block pointers per file header / extension block.
pub const MAX_DATA_BLOCKS: usize = 72;
/// Maximum file/dir name length on AFFS (DOS\0..DOS\5).
pub const MAX_NAME_LEN: usize = 30;
/// Maximum comment length stored in a header/dir entry.
pub const MAX_COMMENT_LEN: usize = 79;

/// AFFS block type tags.
pub const T_HEADER: i32 = 2;
pub const T_LIST: i32 = 16;
pub const T_DATA: i32 = 8;

/// Secondary type tags (`secType` at offset 0x1FC of header blocks).
pub const ST_ROOT: i32 = 1;
pub const ST_DIR: i32 = 2;
pub const ST_FILE: i32 = -3;
pub const ST_LSOFT: i32 = -4;
pub const ST_LDIR: i32 = 4;
pub const ST_LFILE: i32 = -7;

/// AmigaDOS epoch offset: 1978-01-01 is 2922 days after 1970-01-01.
const AMIGA_EPOCH_DAYS: i64 = 2922;
const SECS_PER_DAY: i64 = 86_400;

/// True if the given DosType variant uses International (Intl) case folding
/// rules — DOS\2, DOS\3, DOS\6, DOS\7.
pub fn is_intl_variant(variant: u8) -> bool {
    matches!(variant, 2 | 3 | 6 | 7)
}

/// True if the variant uses FFS-style data blocks (raw 512 bytes per block).
/// DOS\1, DOS\3, DOS\5, DOS\7 are FFS; DOS\0, DOS\2, DOS\4, DOS\6 are OFS.
pub fn is_ffs_variant(variant: u8) -> bool {
    variant & 1 == 1
}

/// True if the variant uses Directory Cache blocks (DOS\4, DOS\5).
pub fn is_dircache_variant(variant: u8) -> bool {
    matches!(variant, 4 | 5)
}

/// True if the variant supports Long File Names (DOS\6, DOS\7).
pub fn is_lnfs_variant(variant: u8) -> bool {
    matches!(variant, 6 | 7)
}

/// Read an i32 from a 4-byte big-endian slice at byte offset `off`.
pub fn read_i32(buf: &[u8], off: usize) -> i32 {
    i32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

/// Read a u32 from a 4-byte big-endian slice at byte offset `off`.
pub fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

/// "Normal" AFFS checksum (root / dir / file header / dir entry / file ext).
/// Sum of all big-endian u32 longs in the block, with the checksum slot
/// (`checksum_offset_longs`) treated as zero, then negated.
///
/// `checksum_offset_longs` is the *long index* of the checksum field
/// (5 for header blocks, where the field sits at byte offset 0x14).
pub fn normal_checksum(buf: &[u8], checksum_offset_longs: usize) -> u32 {
    let mut sum: u32 = 0;
    let longs = buf.len() / 4;
    for i in 0..longs {
        if i == checksum_offset_longs {
            continue;
        }
        sum = sum.wrapping_add(read_u32(buf, i * 4));
    }
    sum.wrapping_neg()
}

/// True if the block's stored checksum at `checksum_offset_longs * 4` matches
/// the value computed by `normal_checksum`.
pub fn verify_normal_checksum(buf: &[u8], checksum_offset_longs: usize) -> bool {
    let stored = read_u32(buf, checksum_offset_longs * 4);
    let expected = normal_checksum(buf, checksum_offset_longs);
    stored == expected
}

/// Bitmap-block checksum: negated sum of `map[1..128]`, where `map[0]` is
/// the checksum slot itself at byte offset 0.
pub fn bitmap_checksum(buf: &[u8]) -> u32 {
    let mut sum: u32 = 0;
    for i in 1..128 {
        sum = sum.wrapping_sub(read_u32(buf, i * 4));
    }
    sum
}

/// Verify a bitmap block's checksum (checksum field at offset 0).
pub fn verify_bitmap_checksum(buf: &[u8]) -> bool {
    let stored = read_u32(buf, 0);
    stored == bitmap_checksum(buf)
}

/// AFFS hash function used to locate entries in `hashTable[HT_SIZE]`. The
/// standard variant uses ASCII case folding; Intl variants fold ISO-8859-1
/// 0xE0..0xFE range as well.
pub fn name_hash(name: &[u8], intl: bool) -> u32 {
    let mut hash = name.len() as u32 & 0x7FF;
    for &c in name {
        let upper = if intl { intl_upper(c) } else { ascii_upper(c) };
        hash = (hash.wrapping_mul(13).wrapping_add(upper as u32)) & 0x7FF;
    }
    hash % HT_SIZE as u32
}

fn ascii_upper(b: u8) -> u8 {
    if b.is_ascii_lowercase() {
        b - 0x20
    } else {
        b
    }
}

fn intl_upper(b: u8) -> u8 {
    if b.is_ascii_lowercase() {
        return b - 0x20;
    }
    // ISO-8859-1 letters 0xE0..=0xFE except multiplication sign 0xF7.
    if (0xE0..=0xFE).contains(&b) && b != 0xF7 {
        return b - 0x20;
    }
    b
}

/// Convert an AmigaDOS DateStamp `(days, mins, ticks)` to UNIX seconds.
/// Ticks run at 50 Hz (PAL). Returns 0 if the date is uninitialised.
pub fn datestamp_to_unix(days: i32, mins: i32, ticks: i32) -> i64 {
    if days == 0 && mins == 0 && ticks == 0 {
        return 0;
    }
    let total_days = days as i64 + AMIGA_EPOCH_DAYS;
    total_days * SECS_PER_DAY + mins as i64 * 60 + ticks as i64 / 50
}

/// Render `(days, mins, ticks)` as a human-readable UTC string.
pub fn datestamp_string(days: i32, mins: i32, ticks: i32) -> Option<String> {
    let unix = datestamp_to_unix(days, mins, ticks);
    if unix == 0 {
        return None;
    }
    // Avoid bringing in chrono just for this — format manually.
    let secs = unix as u64;
    let day = secs / SECS_PER_DAY as u64;
    let tod = secs % SECS_PER_DAY as u64;
    let (yy, mm, dd) = days_to_ymd(day as i64);
    let hh = tod / 3600;
    let mi = (tod % 3600) / 60;
    let ss = tod % 60;
    Some(format!("{yy:04}-{mm:02}-{dd:02} {hh:02}:{mi:02}:{ss:02}"))
}

/// Convert days-since-1970-01-01 to (year, month, day). Civil calendar
/// algorithm from Howard Hinnant (good for any positive day count we care
/// about here).
fn days_to_ymd(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719468;
    let era = z.div_euclid(146097);
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

/// Decode an AFFS BSTR (Pascal-style: 1 length byte + chars). Reads at most
/// `max` characters and returns the decoded UTF-8 string. Non-ASCII bytes
/// are mapped via ISO-8859-1 (the closest match for legacy AmigaDOS text).
pub fn read_bstr(buf: &[u8], offset: usize, max: usize) -> String {
    if offset >= buf.len() {
        return String::new();
    }
    let len = (buf[offset] as usize).min(max).min(buf.len() - offset - 1);
    let bytes = &buf[offset + 1..offset + 1 + len];
    let mut out = String::with_capacity(len);
    for &b in bytes {
        out.push(b as char); // ISO-8859-1 → char
    }
    out
}

/// Decode AFFS protection bits into a `HSPARWED` string (bit set = capability
/// present, except for the lower nibble RWED where set bit means *blocked*).
/// Matches the convention `list` uses on AmigaDOS.
///
/// `access` is the raw `access` field from the entry block (4 bytes BE).
pub fn protection_string(access: u32) -> String {
    // Bits per the AmigaDOS reference:
    //   bit 0 = D (delete)  — set = denied  (i.e. "protected")
    //   bit 1 = E (execute) — set = denied
    //   bit 2 = W (write)   — set = denied
    //   bit 3 = R (read)    — set = denied
    //   bit 4 = A (archive) — set = present
    //   bit 5 = P (pure)
    //   bit 6 = S (script)
    //   bit 7 = H (hold)
    //
    // We emit "hsparwed" with lower-case for cleared and upper for set,
    // using the AmigaDOS "list" convention: the RWED nibble is inverted
    // so a freshly-created file shows "----rwed" (everything allowed).
    let h = if access & 0x80 != 0 { 'h' } else { '-' };
    let s = if access & 0x40 != 0 { 's' } else { '-' };
    let p = if access & 0x20 != 0 { 'p' } else { '-' };
    let a = if access & 0x10 != 0 { 'a' } else { '-' };
    let r = if access & 0x08 == 0 { 'r' } else { '-' };
    let w = if access & 0x04 == 0 { 'w' } else { '-' };
    let e = if access & 0x02 == 0 { 'e' } else { '-' };
    let d = if access & 0x01 == 0 { 'd' } else { '-' };
    format!("{h}{s}{p}{a}{r}{w}{e}{d}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_classification() {
        assert!(is_ffs_variant(1));
        assert!(is_ffs_variant(3));
        assert!(!is_ffs_variant(0));
        assert!(!is_ffs_variant(2));
        assert!(is_intl_variant(2));
        assert!(is_intl_variant(3));
        assert!(!is_intl_variant(0));
        assert!(!is_intl_variant(1));
        assert!(is_dircache_variant(4));
        assert!(is_dircache_variant(5));
        assert!(is_lnfs_variant(6));
        assert!(is_lnfs_variant(7));
    }

    #[test]
    fn hash_structural_properties() {
        // Empty name → hash starts from length 0 and never advances → 0.
        assert_eq!(name_hash(b"", false), 0);
        // Standard ASCII case folding: "FOO" and "foo" must collide.
        assert_eq!(name_hash(b"foo", false), name_hash(b"FOO", false));
        // Intl folding picks up extended Latin chars (0xE0..=0xFE minus 0xF7).
        let a = name_hash(&[0xE9], true); // 'é'
        let b = name_hash(&[0xC9], true); // 'É'
        assert_eq!(a, b);
        // Standard folding does NOT touch the extended Latin range, so
        // lowercase 'é' and uppercase 'É' must hash differently there.
        assert_ne!(name_hash(&[0xE9], false), name_hash(&[0xC9], false));
        // Result is always within HT_SIZE.
        for name in [
            b"a".as_slice(),
            b"longername",
            b"with-dashes-and-numbers-123",
        ] {
            assert!(name_hash(name, false) < HT_SIZE as u32);
            assert!(name_hash(name, true) < HT_SIZE as u32);
        }
    }

    #[test]
    fn datestamp_zero_returns_zero() {
        assert_eq!(datestamp_to_unix(0, 0, 0), 0);
        assert!(datestamp_string(0, 0, 0).is_none());
    }

    #[test]
    fn datestamp_epoch() {
        // (0, 0, 0) — 1978-01-01 00:00:00 UTC, but treated as "unset".
        // First valid date is (1, 0, 0) — 1978-01-02 00:00:00 UTC.
        let unix = datestamp_to_unix(1, 0, 0);
        // 1978-01-02 in unix seconds:
        let expected = (AMIGA_EPOCH_DAYS + 1) * SECS_PER_DAY;
        assert_eq!(unix, expected);
        let s = datestamp_string(1, 0, 0).unwrap();
        assert_eq!(s, "1978-01-02 00:00:00");
    }

    #[test]
    fn protection_bits_default() {
        // access=0 → nothing protected, nothing flagged → "----rwed".
        assert_eq!(protection_string(0), "----rwed");
        // Set archive bit (0x10) → "---arwed".
        assert_eq!(protection_string(0x10), "---arwed");
        // Set delete-protected (bit 0) → "----rwe-".
        assert_eq!(protection_string(0x01), "----rwe-");
    }

    #[test]
    fn normal_checksum_roundtrip() {
        let mut buf = [0u8; 512];
        // Place some payload.
        buf[0..4].copy_from_slice(&T_HEADER.to_be_bytes());
        buf[4..8].copy_from_slice(&880u32.to_be_bytes());
        buf[508..512].copy_from_slice(&ST_ROOT.to_be_bytes());
        let sum = normal_checksum(&buf, 5);
        buf[20..24].copy_from_slice(&sum.to_be_bytes());
        assert!(verify_normal_checksum(&buf, 5));
        // Corrupt a byte → verification fails.
        buf[100] ^= 0xFF;
        assert!(!verify_normal_checksum(&buf, 5));
    }

    #[test]
    fn bitmap_checksum_roundtrip() {
        let mut buf = [0u8; 512];
        for i in 1..128 {
            buf[i * 4..i * 4 + 4].copy_from_slice(&(0xAAAA0000u32 ^ i as u32).to_be_bytes());
        }
        let sum = bitmap_checksum(&buf);
        buf[0..4].copy_from_slice(&sum.to_be_bytes());
        assert!(verify_bitmap_checksum(&buf));
        buf[200] ^= 0x01;
        assert!(!verify_bitmap_checksum(&buf));
    }
}
