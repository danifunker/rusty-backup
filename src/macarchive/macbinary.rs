//! MacBinary I / II / III — the canonical full-fidelity parser for the family.
//!
//! MacBinary is a *transport wrapper*: it serializes exactly one Mac file's
//! data fork + resource fork + Finder metadata into a single flat stream. It
//! is not a multi-file archive, so it joins the `macarchive` family as a
//! single-entry [`StuffItArchive`](super::stuffit::StuffItArchive) (see
//! [`super::extract::open_bytes`]).
//!
//! This module replaces the full-fidelity parser that used to live buried in
//! the `put-macbinary` CLI verb. The lean `resource_fork::parse_macbinary`
//! remains a separate, deliberately conservative host-sidecar *import*
//! detector (it returns an `ImportedResourceFork` and only accepts
//! CRC-validated II/III with a non-empty resource fork). The two have distinct
//! roles; this one is the format reader.
//!
//! Format reference: `docs/native_mac_archives.md` §2 (embedded spec).
//!
//! ## Two entry points
//! - [`is_macbinary`] — the confidence-scored §2.4 detection heuristic, used by
//!   the content classifier. Strict on purpose: MacBinary I has no magic and
//!   lazy II writers zero the CRC, so a false positive would hijack an
//!   unrelated `.bin`.
//! - [`parse`] — lenient structural parse for a buffer already believed to be
//!   MacBinary (e.g. a user-supplied `put-macbinary` input). Does not require a
//!   valid CRC, and **accepts an empty resource fork** (data-fork-only Mac
//!   files — a wrapped disk image, a text file — are legitimate MacBinary; the
//!   old `resource_fork` parser wrongly rejected them).

use anyhow::{bail, Result};
use byteorder::{BigEndian, ByteOrder};

use crate::fs::resource_fork::macbinary_crc16;

/// Which MacBinary revision a stream identifies as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MacBinaryVersion {
    /// MacBinary I — no signature, no CRC. Identified only by heuristics.
    One,
    /// MacBinary II — version byte `$7A == 0x81`, CRC-16/XMODEM at `$7C`.
    Two,
    /// MacBinary III — `'mBIN'` at `$66` and version byte `$7A == 0x82`.
    Three,
}

impl MacBinaryVersion {
    /// Human-readable label ("MacBinary I" / "II" / "III"). Plain ASCII.
    pub fn label(self) -> &'static str {
        match self {
            MacBinaryVersion::One => "MacBinary I",
            MacBinaryVersion::Two => "MacBinary II",
            MacBinaryVersion::Three => "MacBinary III",
        }
    }
}

/// One Mac file recovered from a MacBinary stream: both forks plus the full
/// Finder metadata the header carries.
#[derive(Debug, Clone)]
pub struct MacBinaryFile {
    /// Filename (Mac Roman → UTF-8).
    pub filename: String,
    /// 4-byte Finder type code (`fdType`); may be all-zero.
    pub type_code: [u8; 4],
    /// 4-byte Finder creator code (`fdCreator`); may be all-zero.
    pub creator_code: [u8; 4],
    /// Full 16-bit Finder flags: high byte from `$49`, low byte from `$65`
    /// (II/III) or `$4A` (I).
    pub finder_flags: u16,
    /// `fdLocation` `(v, h)` — Pascal Point convention (vertical first).
    pub finder_location: (i16, i16),
    /// `fdFldr` — the window/folder ID the Finder displays the file in.
    pub finder_folder: i16,
    /// Creation date, Mac epoch (seconds since 1904-01-01, local time).
    pub create_date: u32,
    /// Modification date, Mac epoch.
    pub modify_date: u32,
    /// Which revision this stream identifies as.
    pub version: MacBinaryVersion,
    /// Data fork bytes (may be empty).
    pub data_fork: Vec<u8>,
    /// Resource fork bytes (may be empty).
    pub resource_fork: Vec<u8>,
}

/// Round up to the next 128-byte boundary. MacBinary pads every section
/// (secondary header, data fork, resource fork, comment).
const fn pad128(n: usize) -> usize {
    n.div_ceil(128) * 128
}

/// Maximum legal fork length per the standard (`0..=0x7FFFFF`).
const MAX_FORK_LEN: u32 = 0x007F_FFFF;

/// Label the version from the "version the writer targets" byte at `$7A`
/// (`0x82` = III, `0x81` = II, else I). The `'mBIN'` signature at `$66` is an
/// *additional* III marker used by [`is_macbinary`] for high-confidence
/// detection, but our own `build_macbinary` (and some real writers) set `0x82`
/// without it, so the version byte is the authoritative label.
fn version_of(bytes: &[u8]) -> MacBinaryVersion {
    match bytes[122] {
        0x82 => MacBinaryVersion::Three,
        0x81 => MacBinaryVersion::Two,
        _ => MacBinaryVersion::One,
    }
}

/// Combine the split Finder-flags bytes into the full 16-bit `fdFlags`.
fn finder_flags_of(bytes: &[u8], version: MacBinaryVersion) -> u16 {
    let hi = bytes[73] as u16;
    let lo = match version {
        // II/III moved the low byte to $65 and made $4A reserved.
        MacBinaryVersion::Two | MacBinaryVersion::Three => bytes[101] as u16,
        // MacBinary I kept fdFlags as a 2-byte field at $49..$4B.
        MacBinaryVersion::One => bytes[74] as u16,
    };
    (hi << 8) | lo
}

/// Classify `bytes` as MacBinary using the confidence-scored §2.4 heuristic.
/// Returns the identified version, or `None`.
///
/// The floor (all required): byte `$00`/`$52`/the 8 reserved at `$6C` are zero;
/// filename length `1..=63` with printable Mac-Roman bytes; fork lengths each
/// `<= 0x7FFFFF` with at least one nonzero; type/creator each four printable
/// bytes or all-zero. Then escalate: `'mBIN'` → III (certain); valid CRC → II
/// (certain); otherwise accept as I only when the padded size matches exactly
/// (conservative, to avoid hijacking an unrelated `.bin`).
pub fn is_macbinary(bytes: &[u8]) -> Option<MacBinaryVersion> {
    if bytes.len() < 128 {
        return None;
    }
    // --- Floor checks -----------------------------------------------------
    // Reserved bytes that must be zero in every MacBinary revision.
    if bytes[0] != 0 || bytes[82] != 0 || bytes[108..116].iter().any(|&b| b != 0) {
        return None;
    }
    let name_len = bytes[1] as usize;
    if name_len == 0 || name_len > 63 {
        return None;
    }
    // Filename bytes must be printable Mac Roman (>= 0x20) and contain no
    // colon (the classic-Mac path separator).
    if bytes[2..2 + name_len]
        .iter()
        .any(|&b| b < 0x20 || b == b':')
    {
        return None;
    }
    let data_len = BigEndian::read_u32(&bytes[83..87]);
    let rsrc_len = BigEndian::read_u32(&bytes[87..91]);
    if data_len > MAX_FORK_LEN || rsrc_len > MAX_FORK_LEN || (data_len == 0 && rsrc_len == 0) {
        return None;
    }
    if !fourcc_plausible(&bytes[65..69]) || !fourcc_plausible(&bytes[69..73]) {
        return None;
    }

    // --- Confidence escalation -------------------------------------------
    // Strong MacBinary III signature → certain.
    if bytes[122] == 0x82 && &bytes[102..106] == b"mBIN" {
        return Some(MacBinaryVersion::Three);
    }
    // CRC-validated → certain (a II, or a III whose writer set $7A=0x82 but
    // omitted the 'mBIN' signature — our build_macbinary does exactly this).
    let stored_crc = BigEndian::read_u16(&bytes[124..126]);
    if stored_crc != 0 && stored_crc == macbinary_crc16(&bytes[0..124]) {
        return Some(version_of(bytes));
    }
    // MacBinary I (or a CRC-lazy II): require an exact size match — the file is
    // the header plus both 128-padded forks, allowing the final block to be
    // left unpadded by some writers. Be conservative to avoid hijacking an
    // unrelated `.bin`.
    let min = 128usize + data_len as usize + rsrc_len as usize;
    let max = 128usize + pad128(data_len as usize) + pad128(rsrc_len as usize);
    if (min..=max).contains(&bytes.len()) {
        Some(version_of(bytes))
    } else {
        None
    }
}

/// A 4-byte Finder code is plausible if it's all-zero or every byte is a
/// printable (`>= 0x20`) character.
fn fourcc_plausible(b: &[u8]) -> bool {
    b == [0u8; 4] || b.iter().all(|&c| c >= 0x20)
}

/// Parse a buffer believed to be MacBinary into a [`MacBinaryFile`]. Lenient:
/// no CRC enforcement (some legacy II archives carry garbage there) and an
/// empty resource fork is accepted. Use [`is_macbinary`] first for
/// content-driven detection.
pub fn parse(bytes: &[u8]) -> Result<MacBinaryFile> {
    if bytes.len() < 128 {
        bail!("MacBinary: file too short ({} bytes)", bytes.len());
    }
    // byte 0 ("old version") and byte 82 (reserved) must be 0 in all revisions.
    if bytes[0] != 0 || bytes[82] != 0 {
        bail!(
            "MacBinary: header sanity bytes wrong (b0={:#x}, b82={:#x})",
            bytes[0],
            bytes[82]
        );
    }
    let name_len = bytes[1] as usize;
    if name_len == 0 || name_len > 63 {
        bail!("MacBinary: bad filename length {name_len}");
    }
    let filename = crate::fs::hfs::mac_roman_to_utf8(&bytes[2..2 + name_len]);

    let version = version_of(bytes);

    let mut type_code = [0u8; 4];
    type_code.copy_from_slice(&bytes[65..69]);
    let mut creator_code = [0u8; 4];
    creator_code.copy_from_slice(&bytes[69..73]);

    let finder_flags = finder_flags_of(bytes, version);
    let finder_location = (
        BigEndian::read_i16(&bytes[75..77]),
        BigEndian::read_i16(&bytes[77..79]),
    );
    let finder_folder = BigEndian::read_i16(&bytes[79..81]);
    let data_len = BigEndian::read_u32(&bytes[83..87]) as usize;
    let rsrc_len = BigEndian::read_u32(&bytes[87..91]) as usize;
    let create_date = BigEndian::read_u32(&bytes[91..95]);
    let modify_date = BigEndian::read_u32(&bytes[95..99]);

    // Sections follow the header, each padded to 128: [secondary header]
    // [data fork] [resource fork] [optional comment].
    let sec_hdr_len = BigEndian::read_u16(&bytes[120..122]) as usize;
    let data_start = 128 + pad128(sec_hdr_len);
    let data_end = data_start + data_len;
    if data_end > bytes.len() {
        bail!(
            "MacBinary: data fork extends past file ({data_end} > {})",
            bytes.len()
        );
    }
    let data_fork = bytes[data_start..data_end].to_vec();

    let rsrc_start = data_start + pad128(data_len);
    let rsrc_end = rsrc_start + rsrc_len;
    if rsrc_end > bytes.len() {
        bail!(
            "MacBinary: resource fork extends past file ({rsrc_end} > {})",
            bytes.len()
        );
    }
    let resource_fork = bytes[rsrc_start..rsrc_end].to_vec();

    Ok(MacBinaryFile {
        filename,
        type_code,
        creator_code,
        finder_flags,
        finder_location,
        finder_folder,
        create_date,
        modify_date,
        version,
        data_fork,
        resource_fork,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a MacBinary II archive (valid CRC) for parser/detection tests.
    fn build_mb2(
        filename: &[u8],
        type_code: [u8; 4],
        creator: [u8; 4],
        finder_flags: u16,
        data: &[u8],
        rsrc: &[u8],
    ) -> Vec<u8> {
        let mut hdr = [0u8; 128];
        hdr[1] = filename.len() as u8;
        hdr[2..2 + filename.len()].copy_from_slice(filename);
        hdr[65..69].copy_from_slice(&type_code);
        hdr[69..73].copy_from_slice(&creator);
        hdr[73] = (finder_flags >> 8) as u8;
        hdr[101] = (finder_flags & 0xFF) as u8;
        BigEndian::write_i16(&mut hdr[75..77], 100); // fdLocation.v
        BigEndian::write_i16(&mut hdr[77..79], 200); // fdLocation.h
        BigEndian::write_i16(&mut hdr[79..81], -1); // fdFldr
        BigEndian::write_u32(&mut hdr[83..87], data.len() as u32);
        BigEndian::write_u32(&mut hdr[87..91], rsrc.len() as u32);
        BigEndian::write_u32(&mut hdr[91..95], 0xAAAA_AAAA); // create
        BigEndian::write_u32(&mut hdr[95..99], 0xBBBB_BBBB); // modify
        hdr[122] = 129; // MacBinary II
        hdr[123] = 129;
        let crc = macbinary_crc16(&hdr[0..124]);
        BigEndian::write_u16(&mut hdr[124..126], crc);

        let mut out = hdr.to_vec();
        out.extend_from_slice(data);
        while out.len() % 128 != 0 {
            out.push(0);
        }
        out.extend_from_slice(rsrc);
        while out.len() % 128 != 0 {
            out.push(0);
        }
        out
    }

    #[test]
    fn parse_extracts_all_fields() {
        let data = b"hello mac";
        let rsrc = b"RSRC";
        let mb = build_mb2(b"Greet", *b"TEXT", *b"ttxt", 0x2000, data, rsrc);
        let f = parse(&mb).unwrap();
        assert_eq!(f.filename, "Greet");
        assert_eq!(&f.type_code, b"TEXT");
        assert_eq!(&f.creator_code, b"ttxt");
        assert_eq!(f.finder_flags, 0x2000);
        assert_eq!(f.finder_location, (100, 200));
        assert_eq!(f.finder_folder, -1);
        assert_eq!(f.create_date, 0xAAAA_AAAA);
        assert_eq!(f.modify_date, 0xBBBB_BBBB);
        assert_eq!(f.version, MacBinaryVersion::Two);
        assert_eq!(f.data_fork, data);
        assert_eq!(f.resource_fork, rsrc);
    }

    #[test]
    fn is_macbinary_accepts_crc_validated_ii() {
        let mb = build_mb2(b"x", *b"TEXT", *b"ttxt", 0, b"data", b"rs");
        assert_eq!(is_macbinary(&mb), Some(MacBinaryVersion::Two));
    }

    #[test]
    fn is_macbinary_detects_iii_signature() {
        let mut mb = build_mb2(b"x", *b"APPL", *b"Po.P", 0x2000, b"data", b"");
        mb[122] = 0x82; // MacBinary III version byte
        mb[102..106].copy_from_slice(b"mBIN");
        // CRC is now stale, but the 'mBIN' signature is authoritative.
        assert_eq!(is_macbinary(&mb), Some(MacBinaryVersion::Three));
    }

    #[test]
    fn parse_accepts_empty_resource_fork() {
        // Data-fork-only Mac file (e.g. a wrapped disk image). The old
        // resource_fork parser wrongly rejected this.
        let mb = build_mb2(b"img", *b"????", *b"????", 0, b"disk-image-bytes", b"");
        let f = parse(&mb).unwrap();
        assert!(f.resource_fork.is_empty());
        assert_eq!(f.data_fork, b"disk-image-bytes");
    }

    #[test]
    fn is_macbinary_rejects_raw_disk_image() {
        // A raw HFS volume (MDB sig at byte 1024) must not be misread as
        // MacBinary — byte 0 is nonzero / reserved bytes dirty / no CRC.
        let mut buf = vec![0u8; 2048];
        buf[0] = 0x4C; // typical first byte of an HFS boot block ('LK')
        buf[1] = 0x4B;
        buf[1024] = 0x42;
        buf[1025] = 0x44;
        assert_eq!(is_macbinary(&buf), None);
    }

    #[test]
    fn is_macbinary_rejects_random_and_short() {
        assert_eq!(is_macbinary(&[0u8; 64]), None);
        let junk: Vec<u8> = (0..200u8).collect();
        assert_eq!(is_macbinary(&junk), None);
    }

    #[test]
    fn parse_rejects_bad_reserved_byte() {
        let mut mb = build_mb2(b"x", *b"TEXT", *b"ttxt", 0, b"d", b"");
        mb[82] = 0x55; // reserved byte must be 0
        assert!(parse(&mb).is_err());
    }
}
