//! Magic-sniff classifier for Mac archive bytes. Returns a single
//! [`MacArchiveKind`] regardless of filename extension, so the GUI
//! workflows in OPEN-WORK §6.1 can trigger off the actual file content
//! rather than a possibly-misleading name.
//!
//! Detection order:
//!  1. BinHex 4.0 (textual banner). If it decodes, re-sniff the inner
//!     data fork: a SIT/SIT5 payload yields [`BinHexOverSit`], a SEA
//!     payload yields [`BinHexOverSea`], anything else yields
//!     [`BinHexSingleFile`] (one Mac file's worth of forks).
//!  2. Standalone classic StuffIt `SIT!`.
//!  3. Standalone StuffIt 5.
//!  4. Standalone SEA (Mac app data fork with a SIT! signature inside).
//!
//! Anything else: `None`. The caller treats it as opaque bytes.
//!
//! [`BinHexOverSit`]: MacArchiveKind::BinHexOverSit
//! [`BinHexOverSea`]: MacArchiveKind::BinHexOverSea
//! [`BinHexSingleFile`]: MacArchiveKind::BinHexSingleFile

use std::io::Cursor;

use crate::fs::binhex;
use crate::rbformats::dc42;

use super::compactpro::is_compactpro;
use super::extract::is_stuffitx;
use super::stuffit::{find_sea_archive, is_stuffit};
use super::stuffit5::is_stuffit5;

/// What kind of Mac archive a byte buffer is, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MacArchiveKind {
    /// A `.hqx` whose decoded payload is one Mac file (not a SIT/SEA).
    /// Carries data fork + resource fork + Finder info (type, creator,
    /// flags). The Workflow A modal asks the user whether to "Convert
    /// to binary" (decode the HQX and add the inner file) or add as-is.
    BinHexSingleFile,
    /// A `.sit.hqx`: BinHex-wrapped classic StuffIt or StuffIt 5
    /// archive. The Workflow A modal offers three actions (convert and
    /// expand / convert only / add as-is).
    BinHexOverSit,
    /// A `.sea.hqx`: BinHex-wrapped self-extracting StuffIt archive.
    /// Same three-action shape as [`BinHexOverSit`].
    BinHexOverSea,
    /// A classic StuffIt archive (`SIT!` magic).
    Sit,
    /// A StuffIt 5 archive.
    Sit5,
    /// A self-extracting StuffIt archive: a Mac application carrying a
    /// SIT! signature inside its data fork.
    Sea,
    /// A Compact Pro archive (`.cpt`, or a `.sea` self-extracting one). The
    /// `0x01` marker plus a CRC-validated index identify it.
    CompactPro,
    /// A `.cpt.hqx` / `.sea.hqx`: BinHex-wrapped Compact Pro archive.
    BinHexOverCompactPro,
    /// A `mar` archive (`.mar`, `MAR\x80` magic) — a single Mac file or folder
    /// tree. Only the stored (unwrapped) form is recognized here.
    Mar,
    /// A MacBinary I/II/III file (`.bin`): a transport wrapper holding one Mac
    /// file's two forks plus Finder info. Content-detected (it has no reliable
    /// extension), so a raw `.bin` disk image stays a disk image.
    MacBinary,
    /// A MacZip archive (`.zip`): Info-ZIP's Macintosh port stores each Mac
    /// file's data fork as a normal ZIP entry and its resource fork under a
    /// `XtraStuf.mac/` component, with Finder info in a `Mac3` extra field.
    /// Content-detected (presence of a `Mac3` field), so a plain
    /// disk-image-in-a-zip stays a disk image.
    MacZip,
}

impl MacArchiveKind {
    /// Short label suitable for log messages and dialog titles
    /// ("BinHex 4.0", "StuffIt", "StuffIt 5", "SEA", "StuffIt-over-BinHex",
    /// "SEA-over-BinHex").
    pub fn label(self) -> &'static str {
        match self {
            MacArchiveKind::BinHexSingleFile => "BinHex 4.0",
            MacArchiveKind::BinHexOverSit => "StuffIt-over-BinHex",
            MacArchiveKind::BinHexOverSea => "SEA-over-BinHex",
            MacArchiveKind::Sit => "StuffIt",
            MacArchiveKind::Sit5 => "StuffIt 5",
            MacArchiveKind::Sea => "SEA",
            MacArchiveKind::CompactPro => "Compact Pro",
            MacArchiveKind::BinHexOverCompactPro => "Compact-Pro-over-BinHex",
            MacArchiveKind::Mar => "MAR",
            MacArchiveKind::MacBinary => "MacBinary",
            MacArchiveKind::MacZip => "MacZip",
        }
    }

    /// True iff the kind has a BinHex outer wrapper (i.e. would benefit
    /// from a "Convert" action that strips the wrapper).
    pub fn is_binhex_wrapped(self) -> bool {
        matches!(
            self,
            MacArchiveKind::BinHexSingleFile
                | MacArchiveKind::BinHexOverSit
                | MacArchiveKind::BinHexOverSea
                | MacArchiveKind::BinHexOverCompactPro
        )
    }

    /// True iff the kind has a multi-file archive payload (SIT / SIT5 /
    /// SEA, possibly wrapped in BinHex). The opposite case is a
    /// `BinHexSingleFile`, which only ever holds one file.
    pub fn is_multi_file(self) -> bool {
        !matches!(
            self,
            MacArchiveKind::BinHexSingleFile | MacArchiveKind::MacBinary
        )
    }
}

/// Classify `bytes` as a Mac archive if possible. Filename extension
/// is irrelevant — detection runs purely on content magic. Returns
/// `None` for anything that doesn't match a known archive shape.
///
/// Performance: when BinHex is suspected, the full
/// [`binhex::parse_binhex`] runs (it has to, because we need the
/// decoded payload to disambiguate single-file vs SIT-wrapped vs
/// SEA-wrapped). For a multi-MB HQX this is the bulk of the cost.
/// Non-BinHex inputs are O(small constant) — just magic comparisons.
pub fn detect_mac_archive(bytes: &[u8]) -> Option<MacArchiveKind> {
    // BinHex first. The banner is text so a quick header peek would
    // suffice as a gate, but parse_binhex already searches for it
    // with bounded work; calling it directly keeps one code path.
    if let Ok(bh) = binhex::parse_binhex(bytes) {
        let inner = bh.data_fork.as_slice();
        if is_stuffit(inner) || is_stuffit5(inner) {
            return Some(MacArchiveKind::BinHexOverSit);
        }
        if is_compactpro(inner) {
            return Some(MacArchiveKind::BinHexOverCompactPro);
        }
        if find_sea_archive(inner).is_some() {
            return Some(MacArchiveKind::BinHexOverSea);
        }
        return Some(MacArchiveKind::BinHexSingleFile);
    }
    // Standalone SIT / SIT5 / SEA. is_stuffitx is checked too so the
    // "looks like StuffIt but is actually StuffIt X" case doesn't
    // falsely match SEA's broader signature scan.
    if is_stuffit(bytes) {
        return Some(MacArchiveKind::Sit);
    }
    if is_stuffit5(bytes) {
        return Some(MacArchiveKind::Sit5);
    }
    if is_stuffitx(bytes) {
        // StuffIt X is a recognized container but the extractor doesn't
        // speak it yet; report None so the caller doesn't pop a "we can
        // do this" modal it can't honor. The user's still free to add
        // the file as-is; the existing extractor bails with a clearer
        // message when they try to expand it.
        return None;
    }
    if is_compactpro(bytes) {
        return Some(MacArchiveKind::CompactPro);
    }
    if super::mar::is_mar(bytes) {
        return Some(MacArchiveKind::Mar);
    }
    // MacBinary after the strong-magic formats (it has no magic of its own and
    // uses a confidence heuristic), but before the broad SEA signature scan so
    // a MacBinary wrapping a SEA app still classifies as MacBinary. The
    // heuristic is conservative enough that a raw `.bin` disk image is not
    // matched (see is_macbinary's floor checks).
    if super::macbinary::is_macbinary(bytes).is_some() {
        return Some(MacArchiveKind::MacBinary);
    }
    // MacZip is a ZIP carrying Mac3 extra fields. Cheap PK-magic gate inside
    // is_maczip means a non-zip input bails immediately; a plain
    // disk-image-in-a-zip has no Mac3 field and is not matched.
    if super::maczip::is_maczip(bytes) {
        return Some(MacArchiveKind::MacZip);
    }
    if find_sea_archive(bytes).is_some() {
        return Some(MacArchiveKind::Sea);
    }
    None
}

/// A disk-image payload an archive's single entry can resolve to.
/// Returned by [`detect_mountable_image`] — the Workflow A and D.2
/// auto-unwrap hint uses this to surface "Mount in new Inspect tab"
/// when the user picks an archive whose contents are themselves a
/// disk image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountableImageKind {
    /// DiskCopy 4.2 floppy image (`.dsk` style).
    DiskCopy42,
    /// A raw HFS volume — MDB signature `0x4244` at byte 1024.
    RawHfs,
    /// A raw HFS+ / HFSX volume — signature `0x482B` / `0x4858`.
    RawHfsPlus,
}

impl MountableImageKind {
    /// Short label for log messages / dialog buttons.
    pub fn label(self) -> &'static str {
        match self {
            MountableImageKind::DiskCopy42 => "DiskCopy 4.2",
            MountableImageKind::RawHfs => "raw HFS",
            MountableImageKind::RawHfsPlus => "raw HFS+",
        }
    }
}

/// Sniff a decompressed payload for "is this a disk image the Inspect
/// tab can mount and partition-walk?" Tries DiskCopy 4.2 first (header
/// at offset 0 plus size match), then raw HFS / HFS+ (MDB signature at
/// byte 1024 — the BasiliskII HFV layout and the Apple_HFS partition
/// layout share this offset).
pub fn detect_mountable_image(bytes: &[u8]) -> Option<MountableImageKind> {
    // DiskCopy 4.2 — header validates against the buffer length, so a
    // random buffer that happens to start with header-shaped bytes
    // gets rejected by the size match.
    if dc42::detect_dc42(&mut Cursor::new(bytes), bytes.len() as u64).is_some() {
        return Some(MountableImageKind::DiskCopy42);
    }
    // Raw HFS / HFS+ — signature at byte 1024 (HFS_SIGNATURE = 0x4244,
    // HFSPlus = 0x482B, HFSX = 0x4858). Matches the superfloppy probe
    // in partition::detect_superfloppy.
    if bytes.len() >= 1026 {
        let sig = u16::from_be_bytes([bytes[1024], bytes[1025]]);
        return match sig {
            0x4244 => Some(MountableImageKind::RawHfs),
            0x482B | 0x4858 => Some(MountableImageKind::RawHfsPlus),
            _ => None,
        };
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimum-viable SIT header that satisfies `is_stuffit` (just
    /// the magic — no real entries needed to drive the classifier).
    fn synth_sit_header() -> Vec<u8> {
        let mut out = vec![0u8; 14];
        out[0..4].copy_from_slice(b"SIT!");
        out[10..14].copy_from_slice(b"rLau");
        out
    }

    /// A minimum SIT5 header (just the magic string).
    fn synth_sit5_header() -> Vec<u8> {
        b"StuffIt (c)1997-2002 by Aladdin Systems, Inc.".to_vec()
    }

    /// A SEA-shaped buffer: arbitrary "Mac app code" prefix with a SIT
    /// signature buried inside. find_sea_archive scans for the magic.
    fn synth_sea_blob() -> Vec<u8> {
        let mut out = vec![0u8; 256];
        // Plant a SIT! / rLau signature at offset 100 — anywhere
        // non-zero counts as a SEA (offset 0 would classify as plain
        // Sit instead).
        out[100..104].copy_from_slice(b"SIT!");
        out[110..114].copy_from_slice(b"rLau");
        out
    }

    /// Wrap arbitrary bytes in a BinHex envelope so they round-trip
    /// through parse_binhex. Picks a benign Mac filename, zero
    /// type/creator, and the given bytes as the data fork.
    fn wrap_binhex(name: &str, data_fork: Vec<u8>) -> Vec<u8> {
        let bh = crate::fs::binhex::BinHexFile {
            name: name.to_string(),
            type_code: [0; 4],
            creator_code: [0; 4],
            flags: 0,
            data_fork,
            resource_fork: Vec::new(),
        };
        crate::fs::binhex::build_binhex(&bh).into_bytes()
    }

    #[test]
    fn detects_standalone_sit() {
        assert_eq!(
            detect_mac_archive(&synth_sit_header()),
            Some(MacArchiveKind::Sit)
        );
    }

    #[test]
    fn detects_standalone_sit5() {
        assert_eq!(
            detect_mac_archive(&synth_sit5_header()),
            Some(MacArchiveKind::Sit5)
        );
    }

    #[test]
    fn detects_standalone_sea() {
        // The SEA scan finds SIT! anywhere in the buffer; the synthetic
        // one plants it at offset 100. is_stuffit at offset 0 wouldn't
        // match (header at 0 would be plain Sit), so this routes
        // through the Sea branch.
        assert_eq!(
            detect_mac_archive(&synth_sea_blob()),
            Some(MacArchiveKind::Sea)
        );
    }

    #[test]
    fn detects_binhex_single_file() {
        // Plain data fork, no SIT/SEA inside.
        let hqx = wrap_binhex("Document.txt", b"hello world".to_vec());
        assert_eq!(
            detect_mac_archive(&hqx),
            Some(MacArchiveKind::BinHexSingleFile)
        );
    }

    #[test]
    fn detects_binhex_over_sit() {
        // BinHex wrapping a SIT payload.
        let hqx = wrap_binhex("Bundle.sit", synth_sit_header());
        assert_eq!(
            detect_mac_archive(&hqx),
            Some(MacArchiveKind::BinHexOverSit)
        );
    }

    #[test]
    fn detects_binhex_over_sit5() {
        // BinHex wrapping a SIT5 payload — still classified as
        // BinHexOverSit since the modal flow is the same regardless of
        // SIT vs SIT5 inside.
        let hqx = wrap_binhex("Bundle.sit", synth_sit5_header());
        assert_eq!(
            detect_mac_archive(&hqx),
            Some(MacArchiveKind::BinHexOverSit)
        );
    }

    #[test]
    fn detects_binhex_over_sea() {
        // BinHex wrapping a SEA payload.
        let hqx = wrap_binhex("Installer.sea", synth_sea_blob());
        assert_eq!(
            detect_mac_archive(&hqx),
            Some(MacArchiveKind::BinHexOverSea)
        );
    }

    #[test]
    fn returns_none_for_random_bytes() {
        let junk = (0..200u8).collect::<Vec<u8>>();
        assert_eq!(detect_mac_archive(&junk), None);
    }

    #[test]
    fn returns_none_for_empty() {
        assert_eq!(detect_mac_archive(&[]), None);
    }

    #[test]
    fn returns_none_for_stuffitx() {
        // StuffIt X is recognized but we don't support extraction, so
        // the classifier returns None — the caller falls through to the
        // existing add-as-binary path instead of popping a misleading
        // "we can expand this" modal.
        let mut sx = b"StuffIt!".to_vec();
        sx.extend_from_slice(&[0u8; 32]);
        assert_eq!(detect_mac_archive(&sx), None);
    }

    #[test]
    fn label_is_human_readable() {
        // Spot-check: a few labels must come back as plain ASCII so
        // they survive the egui font (CLAUDE.md rule).
        assert!(MacArchiveKind::Sit.label().is_ascii());
        assert!(MacArchiveKind::BinHexOverSit.label().is_ascii());
        assert!(MacArchiveKind::Sea.label().is_ascii());
    }

    #[test]
    fn is_binhex_wrapped_covers_the_three_hqx_kinds() {
        for k in [
            MacArchiveKind::BinHexSingleFile,
            MacArchiveKind::BinHexOverSit,
            MacArchiveKind::BinHexOverSea,
        ] {
            assert!(k.is_binhex_wrapped(), "{k:?}");
        }
        for k in [
            MacArchiveKind::Sit,
            MacArchiveKind::Sit5,
            MacArchiveKind::Sea,
        ] {
            assert!(!k.is_binhex_wrapped(), "{k:?}");
        }
    }

    #[test]
    fn mountable_image_detects_raw_hfs_at_byte_1024() {
        // Buffer big enough to host the MDB; signature 0x4244 = "BD" at
        // byte 1024 marks classic HFS.
        let mut buf = vec![0u8; 2048];
        buf[1024] = 0x42;
        buf[1025] = 0x44;
        assert_eq!(
            detect_mountable_image(&buf),
            Some(MountableImageKind::RawHfs)
        );
    }

    #[test]
    fn mountable_image_detects_hfs_plus_signatures() {
        for sig in [0x482Bu16, 0x4858u16] {
            let mut buf = vec![0u8; 2048];
            buf[1024..1026].copy_from_slice(&sig.to_be_bytes());
            assert_eq!(
                detect_mountable_image(&buf),
                Some(MountableImageKind::RawHfsPlus),
                "sig 0x{sig:04X}",
            );
        }
    }

    #[test]
    fn mountable_image_returns_none_for_random_short_buffer() {
        assert_eq!(detect_mountable_image(&[0u8; 64]), None);
        // Even big buffers with non-magic at byte 1024 return None.
        assert_eq!(detect_mountable_image(&vec![0u8; 4096]), None);
    }

    #[test]
    fn mountable_image_label_is_ascii() {
        // egui font (CLAUDE.md rule) — every label that appears in the
        // GUI must survive without unicode glyphs.
        for kind in [
            MountableImageKind::DiskCopy42,
            MountableImageKind::RawHfs,
            MountableImageKind::RawHfsPlus,
        ] {
            assert!(kind.label().is_ascii(), "{kind:?}");
        }
    }

    #[test]
    fn is_multi_file_excludes_binhex_single_file() {
        assert!(!MacArchiveKind::BinHexSingleFile.is_multi_file());
        for k in [
            MacArchiveKind::BinHexOverSit,
            MacArchiveKind::BinHexOverSea,
            MacArchiveKind::Sit,
            MacArchiveKind::Sit5,
            MacArchiveKind::Sea,
            MacArchiveKind::Mar,
        ] {
            assert!(k.is_multi_file(), "{k:?}");
        }
    }

    #[test]
    fn detects_macbinary_and_not_raw_disk_image() {
        use byteorder::{BigEndian, ByteOrder};
        // A CRC-valid MacBinary II classifies as MacBinary (single-file).
        let mut hdr = [0u8; 128];
        hdr[1] = 1;
        hdr[2] = b'x';
        hdr[65..69].copy_from_slice(b"TEXT");
        hdr[69..73].copy_from_slice(b"ttxt");
        BigEndian::write_u32(&mut hdr[83..87], 4); // data fork len
        hdr[122] = 129;
        hdr[123] = 129;
        let crc = crate::fs::resource_fork::macbinary_crc16(&hdr[0..124]);
        BigEndian::write_u16(&mut hdr[124..126], crc);
        let mut mb = hdr.to_vec();
        mb.extend_from_slice(b"DATA");
        while mb.len() % 128 != 0 {
            mb.push(0);
        }
        assert_eq!(detect_mac_archive(&mb), Some(MacArchiveKind::MacBinary));
        assert!(!MacArchiveKind::MacBinary.is_multi_file());

        // A raw HFS volume (MDB sig at byte 1024) must NOT be a Mac archive —
        // the .bin overload stays a disk image.
        let mut hfs = vec![0u8; 2048];
        hfs[0] = 0x4C;
        hfs[1] = 0x4B;
        hfs[1024] = 0x42;
        hfs[1025] = 0x44;
        assert_eq!(detect_mac_archive(&hfs), None);
    }

    #[test]
    fn detects_mar() {
        // A stored MAR built by our own writer classifies as Mar (magic +
        // header CRC), is not BinHex-wrapped, and counts as multi-file.
        let mar = crate::macarchive::mar::build_archive(
            "x",
            &[crate::macarchive::stuffit::StuffItInputNode::File(
                crate::macarchive::stuffit::StuffItInput {
                    name: "f".into(),
                    type_code: *b"TEXT",
                    creator_code: *b"ttxt",
                    finder_flags: 0,
                    create_date: 0,
                    mod_date: 0,
                    data_fork: b"hi".to_vec(),
                    resource_fork: Vec::new(),
                },
            )],
        )
        .unwrap();
        assert_eq!(detect_mac_archive(&mar), Some(MacArchiveKind::Mar));
        assert!(!MacArchiveKind::Mar.is_binhex_wrapped());
        assert_eq!(MacArchiveKind::Mar.label(), "MAR");
    }
}
