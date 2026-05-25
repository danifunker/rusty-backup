//! Norton Ghost `.GHO`/`.GHS` container header parser (session 5.5a).
//!
//! Phase 5 / session 5.5a scope is the **outer container header only** —
//! the 12-byte wrapper that every Ghost backup file (Ghost 7.5 + 11.5)
//! begins with, plus the optional password verifier and Pascal-style
//! description field. The *inner* record stream (Track0 / Partition /
//! Continuation / End records, `0x012F18D8` magic, 32 KiB blocks, Fast-LZ /
//! zlib decompression) is **deferred to session 5.5b**; SECTOR-mode
//! decoding is deferred to **session 5.6b**.
//!
//! This module is intentionally a thin parser + dispatch surface so the
//! API shape locks in early and 5.5b / 5.6 / 5.6b can flesh it out without
//! disturbing the callers.
//!
//! Layout reverse-engineered from our own fixture corpus (12 files spanning
//! Ghost 7.5 and 11.5, with every combination of compression / image-type
//! / password). See `docs/virtualization-formats.md` §5.5 for the
//! confirmation table.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};

/// Length of the fixed container header (excluding the optional password
/// verifier and description string).
pub const GHO_HEADER_PREFIX_LEN: usize = 12;

/// Length of the password verifier appended after the header when
/// `password_flag == 1`.
pub const GHO_PASSWORD_VERIFIER_LEN: usize = 16;

/// Container magic at file offset 0.
pub const GHO_MAGIC: [u8; 2] = [0xFE, 0xEF];

/// Offset where Ghost stores the Pascal-style description string
/// (`length_byte + ASCII`). Present in some 7.5 backups, absent in 11.5
/// ones. Capped to 1 sector so we never read past the in-file metadata
/// region.
pub const GHO_DESCRIPTION_OFFSET: u64 = 0xFF;

/// Maximum description string length we'll accept. Ghost's UI capped this
/// well below the sector boundary in practice (the longest string in our
/// corpus is "PartitionBackup no compression", 30 bytes).
pub const GHO_DESCRIPTION_MAX: u8 = 254;

/// Compression byte at header offset 0x03.
///
/// Cross-referenced against the inner record stream's per-block compression
/// indicator once 5.5b lands — the container byte is the "default" for the
/// whole image, but block-level overrides exist in the wild.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GhoCompression {
    /// `0x00` — no compression.
    None,
    /// `0x02` — Fast-LZ (custom LZ77 variant; spec in `nyarime/gho`).
    Fast,
    /// `0x03` — zlib (deflate).
    High,
    /// Anything else — Ghost shipped a handful of intermediate levels
    /// (`Z1`, `Z4`-`Z9`) that map to the same algorithms internally; we
    /// store the raw byte so 5.5b can decide whether to treat it as
    /// Fast or High.
    Other(u8),
}

impl GhoCompression {
    pub fn from_byte(b: u8) -> Self {
        match b {
            0x00 => Self::None,
            0x02 => Self::Fast,
            0x03 => Self::High,
            other => Self::Other(other),
        }
    }

    /// Raw byte value as it appears in the header.
    pub fn as_byte(self) -> u8 {
        match self {
            Self::None => 0x00,
            Self::Fast => 0x02,
            Self::High => 0x03,
            Self::Other(b) => b,
        }
    }
}

/// Image-type byte at header offset 0x0A.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GhoImageType {
    /// `0x00` — file-aware ("truncated full backup"). Ghost walks the
    /// filesystem and stores only used clusters. The inner record stream
    /// targets this mode; it's session 5.5b's focus.
    FileAware,
    /// `0x01` — SECTOR (raw sector-by-sector). Deferred to session 5.6b.
    Sector,
    /// Anything else — surface raw so a future Ghost version doesn't
    /// silently misparse.
    Other(u8),
}

impl GhoImageType {
    pub fn from_byte(b: u8) -> Self {
        match b {
            0x00 => Self::FileAware,
            0x01 => Self::Sector,
            other => Self::Other(other),
        }
    }

    pub fn as_byte(self) -> u8 {
        match self {
            Self::FileAware => 0x00,
            Self::Sector => 0x01,
            Self::Other(b) => b,
        }
    }
}

/// Parsed Norton Ghost container header.
#[derive(Debug, Clone)]
pub struct GhoContainerHeader {
    /// Container version byte at offset 0x02. Always `0x01` across every
    /// Ghost 7.5 + 11.5 fixture we've inspected — we surface it so we can
    /// reject unknown versions cleanly when they appear.
    pub container_version: u8,
    /// Compression scheme (offset 0x03).
    pub compression: GhoCompression,
    /// 32-bit serial / CRC (offset 0x04). Semantics not yet decoded; we
    /// expose it for diagnostics.
    pub serial: u32,
    /// Constant `[0x01, 0x01]` in our corpus (offset 0x08). Stored raw so
    /// future format variants don't silently misparse.
    pub flags: [u8; 2],
    /// Image type (offset 0x0A).
    pub image_type: GhoImageType,
    /// `true` if the backup is password-protected (offset 0x0B).
    pub password_protected: bool,
    /// 16-byte verifier following the prefix when `password_protected`,
    /// `None` otherwise. We never attempt to decrypt — the verifier is
    /// surfaced for diagnostics only.
    pub password_verifier: Option<[u8; GHO_PASSWORD_VERIFIER_LEN]>,
    /// Optional Pascal-style description string at offset 0xFF
    /// (length-prefixed ASCII). `None` if length byte is zero or the
    /// string would overflow the first sector.
    pub description: Option<String>,
}

impl GhoContainerHeader {
    /// Parse a container header from `reader`. Reader is left at an
    /// undefined position; callers should re-seek before continuing.
    pub fn parse<R: Read + Seek>(reader: &mut R) -> Result<Self> {
        reader
            .seek(SeekFrom::Start(0))
            .context("seeking to start of GHO container")?;

        let mut prefix = [0u8; GHO_HEADER_PREFIX_LEN];
        reader
            .read_exact(&mut prefix)
            .context("reading GHO header prefix")?;

        if prefix[0..2] != GHO_MAGIC {
            bail!(
                "not a Norton Ghost container: magic at offset 0 is {:02x} {:02x}, expected FE EF",
                prefix[0],
                prefix[1]
            );
        }

        let container_version = prefix[2];
        let compression = GhoCompression::from_byte(prefix[3]);
        let serial = u32::from_le_bytes([prefix[4], prefix[5], prefix[6], prefix[7]]);
        let flags = [prefix[8], prefix[9]];
        let image_type = GhoImageType::from_byte(prefix[10]);
        let password_protected = match prefix[11] {
            0x00 => false,
            0x01 => true,
            other => {
                bail!(
                    "GHO header password_flag byte at offset 0x0B is {:#04x}, expected 0x00 or 0x01",
                    other
                );
            }
        };

        let password_verifier = if password_protected {
            let mut verifier = [0u8; GHO_PASSWORD_VERIFIER_LEN];
            reader
                .read_exact(&mut verifier)
                .context("reading GHO password verifier")?;
            Some(verifier)
        } else {
            None
        };

        let description = read_description(reader).context("reading GHO description string")?;

        Ok(Self {
            container_version,
            compression,
            serial,
            flags,
            image_type,
            password_protected,
            password_verifier,
            description,
        })
    }
}

fn read_description<R: Read + Seek>(reader: &mut R) -> Result<Option<String>> {
    reader.seek(SeekFrom::Start(GHO_DESCRIPTION_OFFSET))?;
    let mut len_byte = [0u8; 1];
    if reader.read_exact(&mut len_byte).is_err() {
        // File too small to carry a description — that's fine.
        return Ok(None);
    }
    let len = len_byte[0];
    if len == 0 || len > GHO_DESCRIPTION_MAX {
        return Ok(None);
    }
    let max_remaining = 512u64.saturating_sub(GHO_DESCRIPTION_OFFSET + 1) as usize;
    let take = (len as usize).min(max_remaining);
    let mut buf = vec![0u8; take];
    if reader.read_exact(&mut buf).is_err() {
        return Ok(None);
    }
    // Strip NUL padding + trailing whitespace.
    while buf.last().map(|b| *b == 0 || *b == b' ').unwrap_or(false) {
        buf.pop();
    }
    if buf.is_empty() {
        return Ok(None);
    }
    match std::str::from_utf8(&buf) {
        Ok(s) => Ok(Some(s.to_string())),
        // Description is ASCII in practice; tolerate stray high bytes by
        // lossy-decoding rather than failing the whole header.
        Err(_) => Ok(Some(String::from_utf8_lossy(&buf).into_owned())),
    }
}

/// Materialize a Norton Ghost backup into a raw disk image temp file.
///
/// **Session 5.5a scope**: parses the container header and returns precise
/// errors for cases not yet supported (password-protected, SECTOR mode,
/// unknown container version). File-aware-mode + supported compression
/// hits a deliberate "inner record stream not yet implemented" error
/// stub; sessions 5.5b / 5.6 will replace that stub with the real decoder.
///
/// The error returned for the deferred cases mentions exactly which
/// follow-up session covers it so a user hitting an unsupported fixture
/// can read the doc.
pub fn materialize_gho_to_temp(path: &Path) -> Result<GhoMaterializedStub> {
    let mut file = File::open(path).with_context(|| format!("opening GHO {}", path.display()))?;
    let header = GhoContainerHeader::parse(&mut file)
        .with_context(|| format!("parsing GHO container header from {}", path.display()))?;

    if header.container_version != 0x01 {
        return Err(anyhow!(
            "GHO {} has unknown container version {:#04x}; this build supports 0x01 (Ghost 7.5 + 11.5)",
            path.display(),
            header.container_version
        ));
    }

    if header.password_protected {
        return Err(anyhow!(
            "GHO {} is password-protected; password-protected Ghost backups are not yet \
             supported (planned: session 5.5b / 5.6)",
            path.display()
        ));
    }

    match header.image_type {
        GhoImageType::Sector => {
            return Err(anyhow!(
                "GHO {} is a SECTOR-mode (raw sector-by-sector) backup; SECTOR-mode decoding \
                 is deferred to session 5.6b",
                path.display()
            ));
        }
        GhoImageType::Other(b) => {
            return Err(anyhow!(
                "GHO {} has unknown image_type byte {:#04x} at offset 0x0A; expected 0x00 \
                 (file-aware) or 0x01 (SECTOR)",
                path.display(),
                b
            ));
        }
        GhoImageType::FileAware => {}
    }

    if let GhoCompression::Other(b) = header.compression {
        return Err(anyhow!(
            "GHO {} has unknown compression byte {:#04x} at offset 0x03; expected 0x00 (none), \
             0x02 (fast/LZ), or 0x03 (high/zlib)",
            path.display(),
            b
        ));
    }

    // File-aware mode + supported compression: the container is valid,
    // but the inner record stream decoder isn't here yet.
    Err(anyhow!(
        "GHO {} is a valid file-aware Ghost backup ({:?} compression), but the inner record \
         stream decoder is not yet implemented (planned: session 5.5b for the parser + Fast-LZ, \
         session 5.6 for the full decode-to-temp + .GHS span chain)",
        path.display(),
        header.compression
    ))
}

/// Placeholder return type. Session 5.6 swaps this for the same
/// `(temp_path, TempDir guard, logical_size)` shape used by IMZ — until
/// then the materializer only returns errors so this struct stays empty.
#[derive(Debug)]
pub struct GhoMaterializedStub {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};

    /// Build a synthetic GHO header for testing. Returns the prefix-sector
    /// (512 bytes) so callers can drop in description bytes at 0xFF if they
    /// like.
    fn build_header(
        comp: u8,
        image_type: u8,
        password_flag: u8,
        description: Option<&str>,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; 512];
        buf[0] = 0xFE;
        buf[1] = 0xEF;
        buf[2] = 0x01; // container version
        buf[3] = comp;
        buf[4..8].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        buf[8] = 0x01;
        buf[9] = 0x01;
        buf[10] = image_type;
        buf[11] = password_flag;
        if password_flag == 0x01 {
            // 16-byte verifier follows the prefix at offset 12.
            buf[12..28].fill(0xAB);
        }
        if let Some(desc) = description {
            let bytes = desc.as_bytes();
            assert!(bytes.len() <= 0xFF);
            buf[0xFF] = bytes.len() as u8;
            let end = 0x100 + bytes.len();
            buf[0x100..end].copy_from_slice(bytes);
        }
        buf
    }

    #[test]
    fn parses_minimal_uncompressed_file_aware_header() {
        let buf = build_header(0x00, 0x00, 0x00, None);
        let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
        assert_eq!(header.container_version, 0x01);
        assert_eq!(header.compression, GhoCompression::None);
        assert_eq!(header.serial, 0xDEAD_BEEF);
        assert_eq!(header.flags, [0x01, 0x01]);
        assert_eq!(header.image_type, GhoImageType::FileAware);
        assert!(!header.password_protected);
        assert!(header.password_verifier.is_none());
        assert_eq!(header.description, None);
    }

    #[test]
    fn parses_high_compression_sector_header() {
        let buf = build_header(0x03, 0x01, 0x00, None);
        let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
        assert_eq!(header.compression, GhoCompression::High);
        assert_eq!(header.image_type, GhoImageType::Sector);
    }

    #[test]
    fn parses_password_verifier() {
        let buf = build_header(0x00, 0x00, 0x01, None);
        let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
        assert!(header.password_protected);
        assert_eq!(header.password_verifier, Some([0xAB; 16]));
    }

    #[test]
    fn parses_pascal_description() {
        let buf = build_header(0x00, 0x00, 0x00, Some("PartitionBackup no compression"));
        let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
        assert_eq!(
            header.description.as_deref(),
            Some("PartitionBackup no compression")
        );
    }

    #[test]
    fn rejects_wrong_magic() {
        let mut buf = build_header(0x00, 0x00, 0x00, None);
        buf[0] = 0xCA;
        buf[1] = 0xFE;
        let err = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap_err();
        assert!(format!("{err:#}").contains("not a Norton Ghost container"));
    }

    #[test]
    fn rejects_unknown_password_flag() {
        let mut buf = build_header(0x00, 0x00, 0x00, None);
        buf[11] = 0x42;
        let err = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap_err();
        assert!(format!("{err:#}").contains("password_flag"));
    }

    #[test]
    fn surfaces_unknown_compression_byte_via_other() {
        let buf = build_header(0x07, 0x00, 0x00, None);
        let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
        assert_eq!(header.compression, GhoCompression::Other(0x07));
    }

    #[test]
    fn surfaces_unknown_image_type_byte_via_other() {
        let buf = build_header(0x00, 0x05, 0x00, None);
        let header = GhoContainerHeader::parse(&mut Cursor::new(buf)).unwrap();
        assert_eq!(header.image_type, GhoImageType::Other(0x05));
    }

    // ---------- materializer-stub error-shape tests ----------

    fn write_to_temp(bytes: &[u8]) -> std::path::PathBuf {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("test.GHO");
        let mut f = File::create(&path).unwrap();
        f.write_all(bytes).unwrap();
        path
    }

    #[test]
    fn materialize_password_protected_errors_cleanly() {
        let buf = build_header(0x00, 0x00, 0x01, None);
        let path = write_to_temp(&buf);
        let err = materialize_gho_to_temp(&path).unwrap_err();
        let msg = format!("{err:#}").to_lowercase();
        assert!(
            msg.contains("password"),
            "error should mention password, got: {msg}"
        );
    }

    #[test]
    fn materialize_sector_mode_errors_cleanly() {
        let buf = build_header(0x03, 0x01, 0x00, None);
        let path = write_to_temp(&buf);
        let err = materialize_gho_to_temp(&path).unwrap_err();
        let msg = format!("{err:#}").to_lowercase();
        assert!(
            msg.contains("sector"),
            "error should mention SECTOR mode, got: {msg}"
        );
        assert!(
            msg.contains("5.6b"),
            "error should point at session 5.6b follow-up, got: {msg}"
        );
    }

    #[test]
    fn materialize_file_aware_uncompressed_returns_inner_stream_stub_error() {
        let buf = build_header(0x00, 0x00, 0x00, None);
        let path = write_to_temp(&buf);
        let err = materialize_gho_to_temp(&path).unwrap_err();
        let msg = format!("{err:#}").to_lowercase();
        assert!(
            msg.contains("inner record stream"),
            "error should explain the deferred inner-stream work, got: {msg}"
        );
    }

    #[test]
    fn materialize_unknown_compression_byte_errors_cleanly() {
        let buf = build_header(0x07, 0x00, 0x00, None);
        let path = write_to_temp(&buf);
        let err = materialize_gho_to_temp(&path).unwrap_err();
        let msg = format!("{err:#}").to_lowercase();
        assert!(msg.contains("compression"));
    }

    // ---------- fixture-corpus walk (gated on the user's local fixtures) ----------

    /// Expected (compression_byte, image_type_byte, password_flag) for
    /// each fixture, relative to `~/new-fixtures/gho/`. Mirrors the
    /// confirmation table in `docs/virtualization-formats.md`.
    const EXPECTED_FIXTURES: &[(&str, u8, u8, u8)] = &[
        ("HPVectra95C.gho", 0x02, 0x00, 0x00),
        ("XP_SP2FU.GHO", 0x03, 0x00, 0x00),
        (
            "ManualGhostBackups/7.5/FULLDISK/FULLDISK.GHO",
            0x00,
            0x00,
            0x00,
        ),
        ("ManualGhostBackups/7.5/PART/PART.GHO", 0x00, 0x00, 0x00),
        ("ManualGhostBackups/7.5/SECTOR/SECTOR.GHO", 0x00, 0x01, 0x00),
        ("ManualGhostBackups/7.5/FD-FAST/FAST.GHO", 0x02, 0x00, 0x00),
        (
            "ManualGhostBackups/7.5/FD-HIGH/FD-HIGH.GHO",
            0x03,
            0x00,
            0x00,
        ),
        (
            "ManualGhostBackups/11.5/GH11/fulldisk.GHS",
            0x00,
            0x00,
            0x00,
        ),
        (
            "ManualGhostBackups/11.5/GH11-hicompression/High.GHO",
            0x03,
            0x00,
            0x00,
        ),
        (
            "ManualGhostBackups/11.5/GH11-password/GH11PW.GHO",
            0x00,
            0x00,
            0x01,
        ),
        (
            "ManualGhostBackups/11.5/Gh11-sect compression high/secthigh.GHO",
            0x03,
            0x01,
            0x00,
        ),
    ];

    #[test]
    fn parses_every_fixture_in_corpus() {
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return,
        };
        let base = std::path::PathBuf::from(home).join("new-fixtures/gho");
        if !base.exists() {
            eprintln!(
                "skipping: {} not present (fixture-gated test)",
                base.display()
            );
            return;
        }
        let mut checked = 0;
        let mut missing = Vec::new();
        for (rel, comp, image_type, pwd) in EXPECTED_FIXTURES {
            let path = base.join(rel);
            if !path.exists() {
                missing.push(rel.to_string());
                continue;
            }
            let mut f = File::open(&path).expect("opening fixture");
            let header = GhoContainerHeader::parse(&mut f)
                .unwrap_or_else(|e| panic!("parsing {}: {e:#}", path.display()));
            assert_eq!(header.container_version, 0x01, "{}: container_version", rel);
            assert_eq!(header.flags, [0x01, 0x01], "{}: flags", rel);
            assert_eq!(
                header.compression.as_byte(),
                *comp,
                "{}: compression byte (got {:#04x}, want {:#04x})",
                rel,
                header.compression.as_byte(),
                comp
            );
            assert_eq!(
                header.image_type.as_byte(),
                *image_type,
                "{}: image_type byte (got {:#04x}, want {:#04x})",
                rel,
                header.image_type.as_byte(),
                image_type
            );
            assert_eq!(
                header.password_protected,
                *pwd == 0x01,
                "{}: password_protected",
                rel
            );
            if *pwd == 0x01 {
                assert!(
                    header.password_verifier.is_some(),
                    "{}: pwd flag set but no verifier captured",
                    rel
                );
            }
            checked += 1;
        }
        if !missing.is_empty() {
            eprintln!(
                "fixture corpus walk: {} fixtures parsed OK, {} missing: {:?}",
                checked,
                missing.len(),
                missing
            );
        }
        assert!(
            checked >= 6,
            "fewer than 6 fixtures present ({} found) — corpus may be out of date",
            checked
        );
    }
}
