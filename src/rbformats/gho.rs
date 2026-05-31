//! Norton Ghost `.GHO`/`.GHS` reader (sessions 5.5a + 5.5b + 5.5c + 5.6 + 5.6-span).
//!
//! Layered scope:
//! - **5.5a:** outer container header — 12-byte wrapper, optional 16-byte
//!   password verifier, optional Pascal-style description at offset 0xFF.
//! - **5.5b:** inner **record-stream parser** — `GhoRecordHeader`
//!   (10-byte: `u16 type | u16 marker | u32 magic | u16 body_len`),
//!   `find_inner_stream_start` (scan-forward for record magic, absorbs
//!   the per-version padding difference: 7.5 starts at sector 6, 11.5 at
//!   sector 11), `GhoRecordIter` walking the stream.
//! - **5.5c:** standalone Fast-LZ block decoder (`fast_lz_decompress` +
//!   `fast_lz_hash`) ported from the MIT-licensed clean-room reference
//!   `nyarime/gho`.
//! - **5.6:** **SECTOR-mode decode-to-temp** (`materialize_gho_to_temp`).
//!   SECTOR-mode (raw sector-by-sector) backups — uncompressed, zlib
//!   (`High`), or Fast-LZ (`Fast`) — decode to a raw disk image. Data
//!   starts at the sector after the last `FE EF` sub-header; compressed
//!   streams are `[u16 stored_len][block]` chunks (stored_len includes
//!   itself) terminated by a record header. Wired into the GUI via
//!   `prepare_disk_image_path`.
//!
//!   **File-aware mode is deferred.** Despite the plan ordering SECTOR
//!   after file-aware, the fixture corpus showed SECTOR mode is the path
//!   that yields a mountable disk image with modest effort, while
//!   file-aware mode interleaves boot-sector / FAT-entry / directory /
//!   file-extent metadata records (types 0x0017, 0x0004, 0x0102/3/4)
//!   with 0x0002 cluster-data records and needs a full filesystem
//!   rebuilder. The walker (`parse_gho_image`) + `decode_data_blocks_to`
//!   are kept as scaffolding for that future slice.
//! - **5.6-span (this commit):** **multi-file span sets.**
//!   `discover_gho_span_set(picked)` walks the picked file's directory
//!   and returns the ordered set (primary + numbered spans), handling
//!   three corpus naming patterns: stem-prefix `.GHS` siblings (incl.
//!   8.3 truncation like `SECTOR.GHO` + `SECTO00N.GHS`), hyphenated
//!   stem-prefix, and `.GHO.NNN` numeric suffix. `SpanReader: Read +
//!   Seek` virtualises the chain — primary verbatim, container-header
//!   skipped on every continuation — so SECTOR-mode decode reads one
//!   continuous stream. The user can pick the primary OR any span
//!   sibling and the whole set decodes.
//! - **next:** file-aware filesystem reconstruction; password decrypt.
//!
//! Layout reverse-engineered from our own fixture corpus (12+ files
//! spanning Ghost 7.5 and 11.5, with every combination of compression /
//! image-type / password). See `docs/virtualization-formats.md` §5.5 for
//! the container-header confirmation table.
//!
//! Key 5.5b finding: the record stream is **structurally identical**
//! between Ghost 7.5 and 11.5 — same record sequence, same `body_len`
//! values record-for-record over the first 128 records of the matching
//! fixtures. Only the 2-byte `marker` field differs: 7.5 is always
//! `0x0000`; 11.5 is mostly `0x0000` with ~10% of records carrying
//! `0x95FD` (purpose TBD — likely a per-record class flag or chained CRC;
//! 5.6 will catalogue).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{anyhow, bail, Context, Result};

use super::gho_crypto::{gho_decrypt_body, gho_seed_from_password, gho_verify_seed};

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
    // The description is a NUL-terminated ASCII string starting at
    // offset 0xFF, bounded by the end of the first sector. (Ghost's
    // UI caps descriptions well before the sector boundary.)
    let max_len = 512u64.saturating_sub(GHO_DESCRIPTION_OFFSET) as usize;
    let mut buf = vec![0u8; max_len];
    let n = match read_fully_or_eof(reader, &mut buf) {
        Ok(n) => n,
        Err(_) => return Ok(None),
    };
    buf.truncate(n);
    // Trim at NUL terminator.
    if let Some(nul) = buf.iter().position(|&b| b == 0) {
        buf.truncate(nul);
    }
    // Strip trailing whitespace.
    while buf.last().map(|&b| b == b' ').unwrap_or(false) {
        buf.pop();
    }
    if buf.is_empty() {
        return Ok(None);
    }
    match std::str::from_utf8(&buf) {
        Ok(s) => Ok(Some(s.to_string())),
        Err(_) => Ok(Some(String::from_utf8_lossy(&buf).into_owned())),
    }
}

/// Format a human-readable summary of a GHO container's metadata.
/// Similar to `format_chd_info` for CHD files.
pub fn format_gho_info(path: &Path) -> Result<String> {
    let span_set = discover_gho_span_set(path)?;
    let mut f = File::open(&span_set[0])?;
    let header = GhoContainerHeader::parse(&mut f)?;

    let mut lines = Vec::new();
    lines.push("Norton Ghost Image".to_string());
    lines.push(String::new());

    if let Some(desc) = &header.description {
        lines.push(format!("Description:  {}", desc));
    }

    let comp_str = match header.compression {
        GhoCompression::None => "None",
        GhoCompression::Fast => "Fast (LZ)",
        GhoCompression::High => "High (zlib)",
        GhoCompression::Other(b) => {
            lines.push(format!("Compression:  Unknown (0x{:02x})", b));
            ""
        }
    };
    if !comp_str.is_empty() {
        lines.push(format!("Compression:  {}", comp_str));
    }

    let type_str = match header.image_type {
        GhoImageType::FileAware => "File-aware (truncated)",
        GhoImageType::Sector => "Sector-by-sector",
        GhoImageType::Other(b) => {
            lines.push(format!("Image type:   Unknown (0x{:02x})", b));
            ""
        }
    };
    if !type_str.is_empty() {
        lines.push(format!("Image type:   {}", type_str));
    }

    if header.password_protected {
        lines.push("Password:     Yes (encrypted)".to_string());
    }

    if span_set.len() > 1 {
        lines.push(format!("Span files:   {} files", span_set.len()));
    }

    let total_bytes: u64 = span_set
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
        .sum();
    lines.push(format!(
        "Archive size: {:.1} MB",
        total_bytes as f64 / (1024.0 * 1024.0)
    ));

    Ok(lines.join("\n"))
}

/// Materialize a Norton Ghost backup into a raw disk image temp file.
///
/// **Session 5.5a scope**: parses the container header and returns precise
/// errors for cases not yet supported (password-protected, SECTOR mode,
/// multi-partition disk reconstruction, unknown container version).
///
/// Single-partition file-aware backups decode end-to-end here: the inner
/// record stream walker finds the lone partition, its block-stream is
/// decompressed (none / Fast-LZ / zlib) into a fresh tempdir, and the
/// resulting raw partition image is returned via [`GhoMaterialized`].
/// Multi-partition full-disk reconstruction (placing each decoded
/// partition at its MBR-declared LBA) is the next slice — until then we
/// return a precise error pointing at the deferred session.
pub fn materialize_gho_to_temp(path: &Path) -> Result<GhoMaterialized> {
    materialize_gho_to_temp_with_password(path, None)
}

/// Like [`materialize_gho_to_temp`] but accepts an optional password for
/// encrypted images. Password-protected GHOs are decoded through the streaming
/// [`GhoReader`], which derives + verifies the seed and applies the correct
/// decryption framing (per record body for file-aware, 32 KiB chunks for
/// uncompressed SECTOR), then the decoded disk is copied to a tempfile.
pub fn materialize_gho_to_temp_with_password(
    path: &Path,
    password: Option<&[u8]>,
) -> Result<GhoMaterialized> {
    let span_set = discover_gho_span_set(path)
        .with_context(|| format!("discovering span set for {}", path.display()))?;
    if span_set.len() > 1 {
        log::info!(
            "GHO span set: {} files discovered for {}",
            span_set.len(),
            path.display()
        );
        for (i, p) in span_set.iter().enumerate() {
            log::debug!("  span[{i}] -> {}", p.display());
        }
    }

    let primary = &span_set[0];
    let mut primary_file =
        File::open(primary).with_context(|| format!("opening GHO {}", primary.display()))?;
    let header = GhoContainerHeader::parse(&mut primary_file)
        .with_context(|| format!("parsing GHO container header from {}", primary.display()))?;
    drop(primary_file);

    if header.container_version != 0x01 {
        return Err(anyhow!(
            "GHO {} has unknown container version {:#04x}; this build supports 0x01 (Ghost 7.5 + 11.5)",
            path.display(),
            header.container_version
        ));
    }

    if header.password_protected {
        // Route encrypted images through the streaming GhoReader, which
        // derives + verifies the password seed and applies the right
        // decryption framing for both file-aware and SECTOR layouts, then
        // copy the decoded disk into a tempfile. (A missing/incorrect
        // password surfaces as a precise error from `open_with_password`.)
        let mut reader = GhoReader::open_with_password(path, password)
            .with_context(|| format!("opening password-protected GHO {}", path.display()))?;
        let logical = reader.logical_size();
        let partition_count = match &reader.mode {
            GhoReaderMode::FileAware { partitions, .. } => partitions.len(),
            _ => 1,
        };
        let guard =
            tempfile::tempdir().context("creating tempdir for encrypted GHO materialization")?;
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("gho");
        let out_path = guard.path().join(format!("{}.img", stem));
        let mut out =
            File::create(&out_path).with_context(|| format!("creating {}", out_path.display()))?;
        let bytes_written = std::io::copy(&mut reader, &mut out)
            .with_context(|| format!("materializing encrypted {} via GhoReader", path.display()))?;
        out.sync_all().ok();
        log::info!(
            "Materialized encrypted GHO {} -> {} ({} bytes via GhoReader)",
            path.display(),
            out_path.display(),
            bytes_written
        );
        return Ok(GhoMaterialized {
            temp_path: out_path,
            logical_size: logical,
            guard,
            partition_count,
        });
    }

    if let GhoCompression::Other(b) = header.compression {
        return Err(anyhow!(
            "GHO {} has unknown compression byte {:#04x} at offset 0x03; expected 0x00 (none), \
             0x02 (fast/LZ), or 0x03 (high/zlib)",
            path.display(),
            b
        ));
    }

    match header.image_type {
        GhoImageType::Sector => {
            let mut reader = SpanReader::open(&span_set)
                .with_context(|| format!("opening span set for {}", path.display()))?;
            decode_sector_mode_to_temp(&mut reader, path, &header)
        }
        GhoImageType::FileAware => {
            let mut reader = GhoReader::open(path)
                .with_context(|| format!("opening file-aware GHO {}", path.display()))?;
            let logical = reader.logical_size();
            let partition_count = match &reader.mode {
                GhoReaderMode::FileAware { partitions, .. } => partitions.len(),
                _ => 1,
            };
            let guard = tempfile::tempdir()
                .context("creating tempdir for GHO file-aware materialization")?;
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("gho");
            let out_path = guard.path().join(format!("{}.img", stem));
            let mut out = File::create(&out_path)
                .with_context(|| format!("creating {}", out_path.display()))?;
            let bytes_written = std::io::copy(&mut reader, &mut out).with_context(|| {
                format!("materializing file-aware {} via GhoReader", path.display())
            })?;
            out.sync_all().ok();
            log::info!(
                "Materialized GHO file-aware {} -> {} ({} bytes via streaming GhoReader, {} partitions)",
                path.display(),
                out_path.display(),
                bytes_written,
                partition_count
            );
            Ok(GhoMaterialized {
                temp_path: out_path,
                logical_size: logical,
                guard,
                partition_count,
            })
        }
        GhoImageType::Other(b) => Err(anyhow!(
            "GHO {} has unknown image_type byte {:#04x} at offset 0x0A; expected 0x00 \
             (file-aware) or 0x01 (SECTOR)",
            path.display(),
            b
        )),
    }
}

// SECTOR-mode decoder overview. Data starts at the sector after the last
// FE EF sub-header (sector 6 on Ghost 7.5, sector 11 on Ghost 11.5 in
// the corpus); from there it is either:
//   - compression == None → raw sectors, copied verbatim to `out`
//   - compression == Fast/High → stream of `[u16 stored_len][block_data]`
//     chunks where `stored_len` includes itself; each chunk decompresses
//     to one 32 KiB raw block.
// The decoder fn itself lives further down (`decode_sector_mode_to_temp`);
// the span-set discovery + multi-file reader machinery sits between.
// ---------------------------------------------------------------------------
// Span-set discovery + virtual multi-file reader
// ---------------------------------------------------------------------------
//
// Norton Ghost splits large backups across multiple files (CD/DVD media-size
// caps). We see three naming conventions in the corpus:
//
//   1. Stem-prefix `.GHS` siblings — primary `.GHO` + one or more `.GHS`
//      files in the same directory whose stems share a prefix with the
//      primary's stem. Examples: `SECTOR.GHO` + `SECTO00N.GHS` (8.3
//      filename truncation), `gh11-spl.GHO` + `gh11-00N.GHS`,
//      `gh11pwd.GHO` + `gh11p00N.GHS`, `hipwd.GHO` + `hipwd00N.GHS`.
//   2. Truncated `.GHS` siblings — `XP_SP2FU.GHO` + `XP_SP00N.GHS`. Same
//      family as #1 but the 8.3 truncation drops more characters.
//   3. Numeric `.GHO.NNN` suffix — every file in the set is named
//      `name.GHO.NNN` (e.g. `Win7_86xAMB.GHO.001` ... `.066`). No `.GHO`
//      or `.GHS` extension on its own; the lowest-numbered file is the
//      primary.
//
// Discovery rule (`discover_gho_span_set`): infer the picked file's
// "logical stem" + numeric suffix, walk the same directory for siblings
// that match the same logical stem + a 3-digit numeric suffix, return
// `[primary, span_001, span_002, …]` in order. If nothing else matches,
// return just `[picked]` (single-file case).

/// Discover all files in the same Norton Ghost span set as `picked`.
///
/// Returns the set in stream order: primary first, then numbered spans
/// in ascending order. The picked file is always included in the result;
/// if no siblings are found the result is `[picked]`.
///
/// The picked file does NOT need to be the primary — passing any one
/// file (primary OR any span) yields the same set.
pub fn discover_gho_span_set(picked: &Path) -> Result<Vec<std::path::PathBuf>> {
    let dir = picked.parent().unwrap_or_else(|| Path::new("."));
    let name = picked
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("picked path has no filename: {}", picked.display()))?;

    // Pattern 3: `<stem>.GHO.NNN` — numeric extension on .GHO. Detect via
    // case-insensitive `.GHO.<digits>` suffix.
    if let Some((logical_stem, _)) = split_gho_dot_numeric(name) {
        return gather_dot_numeric_siblings(dir, &logical_stem);
    }

    // Patterns 1 + 2: primary .GHO + .GHS siblings (or .GHS alone).
    // Strategy: take the picked file's stem, strip a trailing 3-digit
    // numeric suffix if present, then find every sibling whose stem
    // (compared case-insensitively, ignoring its own trailing 3-digit
    // suffix) starts with the same prefix AND whose extension is .GHO
    // or .GHS. Sort by extension (.GHO before .GHS) then by numeric tail.
    let stem = Path::new(name)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let ext = Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if ext != "gho" && ext != "ghs" {
        // Unknown extension — caller should have validated already.
        return Ok(vec![picked.to_path_buf()]);
    }
    let logical_prefix = strip_trailing_numeric(stem).to_ascii_lowercase();
    if logical_prefix.is_empty() {
        return Ok(vec![picked.to_path_buf()]);
    }
    gather_ghs_siblings(dir, &logical_prefix, picked)
}

/// Splits `name.GHO.NNN` → `(name, NNN)`. Case-insensitive on `.GHO`.
fn split_gho_dot_numeric(name: &str) -> Option<(String, u32)> {
    let lower = name.to_ascii_lowercase();
    let dot = lower.rfind('.')?;
    let tail = &lower[dot + 1..];
    if tail.is_empty() || !tail.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let n: u32 = tail.parse().ok()?;
    let stem_and_gho = &lower[..dot];
    if !stem_and_gho.ends_with(".gho") {
        return None;
    }
    let logical = &name[..dot - 4]; // strip `.GHO.NNN` from original (case-preserving)
    Some((logical.to_string(), n))
}

fn gather_dot_numeric_siblings(dir: &Path, logical_stem: &str) -> Result<Vec<std::path::PathBuf>> {
    let mut matches: Vec<(u32, std::path::PathBuf)> = Vec::new();
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("reading directory {}", dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let n = entry.file_name();
        let s = match n.to_str() {
            Some(s) => s,
            None => continue,
        };
        if let Some((stem, num)) = split_gho_dot_numeric(s) {
            if stem.eq_ignore_ascii_case(logical_stem) {
                matches.push((num, entry.path()));
            }
        }
    }
    matches.sort_by_key(|(n, _)| *n);
    if matches.is_empty() {
        bail!("no .GHO.NNN siblings found in {}", dir.display());
    }
    Ok(matches.into_iter().map(|(_, p)| p).collect())
}

/// Strip a trailing run of ASCII digits from `s` (max ~3 chars typical).
fn strip_trailing_numeric(s: &str) -> &str {
    let end = s
        .rfind(|c: char| !c.is_ascii_digit())
        .map(|i| i + 1)
        .unwrap_or(0);
    &s[..end]
}

fn gather_ghs_siblings(
    dir: &Path,
    logical_prefix: &str,
    picked: &Path,
) -> Result<Vec<std::path::PathBuf>> {
    // logical_prefix is the picked file's stem with any trailing digits
    // stripped, lowercased. Examples: "SECTOR" → "sector",
    // "SECTO" → "secto", "gh11-spl" → "gh11-spl", "gh11pwd" → "gh11pwd",
    // "gh11p" → "gh11p" (truncated 8.3 form), "XP_SP2FU" → "XP_SP2FU".
    // Some sets truncate the primary's stem when generating span names
    // (gh11pwd.GHO → gh11p001.GHS); to catch that, also try a shortened
    // 5-character prefix.
    let mut prefixes = vec![logical_prefix.to_string()];
    if logical_prefix.len() > 5 {
        prefixes.push(logical_prefix[..5].to_string());
    }

    let mut primary: Option<std::path::PathBuf> = None;
    let mut spans: Vec<(String, std::path::PathBuf)> = Vec::new();

    for entry in
        std::fs::read_dir(dir).with_context(|| format!("reading directory {}", dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name_os = entry.file_name();
        let name = match name_os.to_str() {
            Some(s) => s,
            None => continue,
        };
        let p = Path::new(name);
        let ext = p
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if ext != "gho" && ext != "ghs" {
            continue;
        }
        let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let stem_prefix = strip_trailing_numeric(stem).to_ascii_lowercase();
        let matches_any = prefixes
            .iter()
            .any(|p| stem_prefix == *p || (!p.is_empty() && stem_prefix.starts_with(p)));
        if !matches_any {
            continue;
        }
        if ext == "gho" {
            // Prefer the picked file as primary if it is the .GHO; otherwise
            // first .GHO wins. (Either way, a .GHO acts as primary; spans
            // are .GHS.)
            if primary.is_none() || entry.path() == picked {
                primary = Some(entry.path());
            }
        } else {
            // .GHS span. Key by name for deterministic sort.
            spans.push((name.to_ascii_lowercase(), entry.path()));
        }
    }
    spans.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = Vec::new();
    if let Some(p) = primary {
        out.push(p);
    } else {
        // No .GHO in the dir — the picked file is the primary (could be a
        // lone .GHS such as `fulldisk.GHS`).
        out.push(picked.to_path_buf());
    }
    for (_, p) in spans {
        // Don't double-include the picked file.
        if out.first().map(|x| x.as_path()) != Some(p.as_path()) {
            out.push(p);
        }
    }
    Ok(out)
}

/// Trait giving the total length of a `Read + Seek` source. `File` reads
/// it from `metadata`; `SpanReader` reads it from its precomputed map.
/// Used so `decode_sector_mode_to_temp` can drive both without an extra
/// per-file `metadata()` round-trip.
pub trait DataLen {
    fn total_len(&self) -> u64;
}

impl DataLen for File {
    fn total_len(&self) -> u64 {
        self.metadata().map(|m| m.len()).unwrap_or(0)
    }
}

/// Virtual `Read + Seek` over an ordered list of Ghost span files. The
/// first file is exposed verbatim; every file after that has its first
/// 512 bytes (the container header) skipped, so the SECTOR-mode decoder
/// sees one continuous data stream.
/// Layout of the encrypted region(s) inside a password-protected GHO.
///
/// Ghost encrypts independent units, each with the cipher reset to the
/// password seed. Two shapes occur in the wild:
enum GhoEncLayout {
    /// File-aware mode: the inner record stream is `[header][body]...` and
    /// only the **bodies** are encrypted (headers + container bytes are
    /// plaintext). Holds the record body ranges `(body_start, body_len)`,
    /// sorted by `body_start`.
    Bodies(Vec<(u64, u32)>),
    /// Uncompressed SECTOR mode: the contiguous disk image at `[data_start,
    /// end)` is encrypted in fixed `chunk`-byte pieces, each reset to the
    /// seed. (Compressed SECTOR images are stored *unencrypted* by Ghost
    /// 11.5, so they never use this path.)
    Chunked {
        data_start: u64,
        chunk: u64,
        end: u64,
    },
}

/// Transparent decryption layer for password-protected GHOs.
///
/// Holds the encrypted-region layout so [`SpanReader`] can decrypt the
/// relevant bytes on the fly while passing plaintext bytes (record headers,
/// container metadata, the pre-data sub-headers) through unchanged — making
/// decryption invisible to every decode path.
struct GhoBodyDecryptor {
    seed: u16,
    layout: GhoEncLayout,
    /// Single-unit decrypt cache (reads are largely sequential within a unit).
    cache_start: u64,
    cache: Vec<u8>,
    cache_valid: bool,
}

impl GhoBodyDecryptor {
    /// Return the encrypted unit `(start, len)` containing `pos`, if any.
    fn body_at(&self, pos: u64) -> Option<(u64, u32)> {
        match &self.layout {
            GhoEncLayout::Bodies(bodies) => {
                let idx = match bodies.binary_search_by(|&(s, _)| s.cmp(&pos)) {
                    Ok(i) => i,
                    Err(0) => return None,
                    Err(i) => i - 1,
                };
                let (s, l) = bodies[idx];
                if pos >= s && pos < s + l as u64 {
                    Some((s, l))
                } else {
                    None
                }
            }
            GhoEncLayout::Chunked {
                data_start,
                chunk,
                end,
            } => {
                if pos < *data_start || pos >= *end {
                    return None;
                }
                let idx = (pos - data_start) / chunk;
                let bstart = data_start + idx * chunk;
                let blen = (*end - bstart).min(*chunk) as u32;
                Some((bstart, blen))
            }
        }
    }

    /// Smallest encrypted-unit start strictly greater than `pos`, used to
    /// bound passthrough reads so they never spill into an encrypted unit.
    fn next_body_start(&self, pos: u64) -> Option<u64> {
        match &self.layout {
            GhoEncLayout::Bodies(bodies) => {
                let idx = bodies.partition_point(|&(s, _)| s <= pos);
                bodies.get(idx).map(|&(s, _)| s)
            }
            // Chunked: the only plaintext region is before `data_start`; once
            // inside `[data_start, end)` every byte is covered by `body_at`.
            GhoEncLayout::Chunked { data_start, .. } => (pos < *data_start).then_some(*data_start),
        }
    }
}

pub struct SpanReader {
    files: Vec<File>,
    /// Cumulative virtual offsets — `offsets[i]` is the byte position of
    /// `files[i]`'s exposed bytes in the virtual stream. `offsets[N]`
    /// equals the total virtual length.
    offsets: Vec<u64>,
    pos: u64,
    total: u64,
    /// `Some` for password-protected images — decrypts record bodies on read.
    crypt: Option<GhoBodyDecryptor>,
}

impl SpanReader {
    pub fn open(paths: &[std::path::PathBuf]) -> Result<Self> {
        let mut files = Vec::with_capacity(paths.len());
        let mut offsets = Vec::with_capacity(paths.len() + 1);
        let mut cum: u64 = 0;
        for (i, p) in paths.iter().enumerate() {
            let f = File::open(p).with_context(|| format!("opening span file {}", p.display()))?;
            let raw = f.metadata()?.len();
            let exposed = if i == 0 {
                raw
            } else {
                raw.saturating_sub(GHO_SECTOR_SIZE)
            };
            offsets.push(cum);
            cum = cum.saturating_add(exposed);
            files.push(f);
        }
        offsets.push(cum);
        Ok(Self {
            files,
            offsets,
            pos: 0,
            total: cum,
            crypt: None,
        })
    }

    /// Enable transparent body decryption for a password-protected image.
    ///
    /// `bodies` is the full list of `(body_start, body_len)` record-body
    /// ranges (sorted by `body_start`); `seed` is the password-derived cipher
    /// seed. After this call, reads of body bytes return plaintext while record
    /// headers and container bytes pass through unchanged.
    pub fn enable_decryption(&mut self, seed: u16, mut bodies: Vec<(u64, u32)>) {
        bodies.sort_unstable_by_key(|&(s, _)| s);
        self.crypt = Some(GhoBodyDecryptor {
            seed,
            layout: GhoEncLayout::Bodies(bodies),
            cache_start: u64::MAX,
            cache: Vec::new(),
            cache_valid: false,
        });
    }

    /// Enable transparent decryption for an uncompressed SECTOR-mode image.
    ///
    /// The contiguous disk image at `[data_start, end)` is decrypted in
    /// `chunk`-byte pieces (each reset to `seed`); bytes before `data_start`
    /// (container + sub-headers) pass through unchanged. Random access works
    /// because each chunk is independently keyed.
    pub fn enable_chunked_decryption(&mut self, seed: u16, data_start: u64, chunk: u64, end: u64) {
        self.crypt = Some(GhoBodyDecryptor {
            seed,
            layout: GhoEncLayout::Chunked {
                data_start,
                chunk,
                end,
            },
            cache_start: u64::MAX,
            cache: Vec::new(),
            cache_valid: false,
        });
    }

    /// Map a virtual offset to `(file_index, physical_offset, remaining_in_file)`.
    fn map(&self, virt: u64) -> Option<(usize, u64, u64)> {
        if virt >= self.total {
            return None;
        }
        let mut idx = 0;
        for i in 0..self.files.len() {
            if virt < self.offsets[i + 1] {
                idx = i;
                break;
            }
            idx = i + 1;
        }
        if idx >= self.files.len() {
            return None;
        }
        let virt_in_file = virt - self.offsets[idx];
        let physical = if idx == 0 {
            virt_in_file
        } else {
            GHO_SECTOR_SIZE + virt_in_file
        };
        let remaining = self.offsets[idx + 1] - virt;
        Some((idx, physical, remaining))
    }

    /// Read raw (undecrypted) bytes at the current `pos`, advancing it.
    /// Limited to a single span file per call (callers loop / partial-read).
    fn read_raw_advancing(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let (idx, physical, remaining) = match self.map(self.pos) {
            Some(m) => m,
            None => return Ok(0),
        };
        let to_read = (buf.len() as u64).min(remaining) as usize;
        self.files[idx].seek(SeekFrom::Start(physical))?;
        let n = self.files[idx].read(&mut buf[..to_read])?;
        self.pos += n as u64;
        Ok(n)
    }

    /// Read exactly `out.len()` raw bytes starting at virtual `off`, without
    /// touching `self.pos`. Crosses span boundaries as needed.
    fn read_exact_raw_at(&mut self, off: u64, out: &mut [u8]) -> std::io::Result<()> {
        let mut done = 0usize;
        let mut cur = off;
        while done < out.len() {
            let (idx, physical, remaining) = self.map(cur).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "GHO body read past EOF")
            })?;
            let want = ((out.len() - done) as u64).min(remaining) as usize;
            self.files[idx].seek(SeekFrom::Start(physical))?;
            let n = self.files[idx].read(&mut out[done..done + want])?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "GHO body read hit EOF",
                ));
            }
            done += n;
            cur += n as u64;
        }
        Ok(())
    }

    /// Ensure the decrypted body starting at `bstart` (length `blen`) is in
    /// the decryptor cache.
    fn ensure_body_cached(&mut self, bstart: u64, blen: u32) -> std::io::Result<()> {
        if let Some(c) = &self.crypt {
            if c.cache_valid && c.cache_start == bstart {
                return Ok(());
            }
        }
        let mut tmp = vec![0u8; blen as usize];
        self.read_exact_raw_at(bstart, &mut tmp)?;
        let seed = self.crypt.as_ref().expect("crypt set").seed;
        gho_decrypt_body(&mut tmp, seed);
        let c = self.crypt.as_mut().expect("crypt set");
        c.cache = tmp;
        c.cache_start = bstart;
        c.cache_valid = true;
        Ok(())
    }
}

impl DataLen for SpanReader {
    fn total_len(&self) -> u64 {
        self.total
    }
}

impl Read for SpanReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.total || buf.is_empty() {
            return Ok(0);
        }
        if self.crypt.is_none() {
            return self.read_raw_advancing(buf);
        }

        let pos = self.pos;
        let (body, next_start) = {
            let c = self.crypt.as_ref().expect("crypt set");
            (c.body_at(pos), c.next_body_start(pos))
        };

        match body {
            Some((bstart, blen)) => {
                // Encrypted body byte: serve from the decrypted cache.
                self.ensure_body_cached(bstart, blen)?;
                let c = self.crypt.as_ref().expect("crypt set");
                let within = (pos - bstart) as usize;
                let avail = (blen as usize - within).min(buf.len());
                buf[..avail].copy_from_slice(&c.cache[within..within + avail]);
                self.pos += avail as u64;
                Ok(avail)
            }
            None => {
                // Plaintext byte (record header / container): pass through,
                // but never read past the start of the next encrypted body.
                let mut limit = buf.len() as u64;
                if let Some(ns) = next_start {
                    limit = limit.min(ns.saturating_sub(pos));
                }
                let limit = limit.min(self.total - pos) as usize;
                if limit == 0 {
                    return Ok(0);
                }
                self.read_raw_advancing(&mut buf[..limit])
            }
        }
    }
}

impl Seek for SpanReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos: i128 = match pos {
            SeekFrom::Start(o) => o as i128,
            SeekFrom::Current(d) => self.pos as i128 + d as i128,
            SeekFrom::End(d) => self.total as i128 + d as i128,
        };
        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek before start",
            ));
        }
        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

fn decode_sector_mode_to_temp<R: Read + Seek + DataLen>(
    reader: &mut R,
    path: &Path,
    header: &GhoContainerHeader,
) -> Result<GhoMaterialized> {
    let file_size = reader.total_len();
    let data_start = find_sector_data_start(reader, file_size)?;

    let guard = tempfile::tempdir().context("creating tempdir for GHO materialization")?;
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("gho");
    let out_path = guard.path().join(format!("{}.img", stem));
    let mut out =
        File::create(&out_path).with_context(|| format!("creating {}", out_path.display()))?;

    let bytes_written = match header.compression {
        GhoCompression::None => {
            reader.seek(SeekFrom::Start(data_start))?;
            std::io::copy(reader, &mut out)
                .with_context(|| format!("copying raw sectors from {}", path.display()))?
        }
        GhoCompression::Fast | GhoCompression::High => {
            decode_sector_block_stream(reader, data_start, file_size, header.compression, &mut out)
                .with_context(|| {
                    format!(
                        "decompressing SECTOR-mode block stream from {}",
                        path.display()
                    )
                })?
        }
        GhoCompression::Other(b) => bail!("unsupported GHO compression byte {:#04x}", b),
    };
    out.sync_all().ok();

    log::info!(
        "Materialized GHO SECTOR mode {} -> {} ({} bytes, data start {:#x}, {:?})",
        path.display(),
        out_path.display(),
        bytes_written,
        data_start,
        header.compression,
    );

    Ok(GhoMaterialized {
        temp_path: out_path,
        logical_size: bytes_written,
        guard,
        partition_count: 1, // SECTOR mode = single contiguous image
    })
}

/// Find the file offset where SECTOR-mode disk data begins.
///
/// Both corpus dialects place a 512-byte `FE EF` sub-header immediately
/// before the data stream (7.5 SECTOR.GHO at sector 5 → data at sector 6
/// / 0x0C00; 11.5 secthigh.GHO at sector 10 → data at sector 11 /
/// 0x1600). We scan 512-aligned boundaries in the first 64 KiB for the
/// LAST `FE EF` magic and start the data immediately after it.
///
/// If no FEEF sub-header is found we fall back to the first non-zero
/// sector (degenerate / unknown dialects still produce *some* output).
fn find_sector_data_start<R: Read + Seek>(reader: &mut R, file_size: u64) -> Result<u64> {
    let mut buf = [0u8; 512];
    let probe_limit = file_size.min(64 * 1024);

    // Pass 1: locate the last FEEF sub-header. Start at sector 1 so the
    // container header's own FE EF at offset 0 is excluded.
    let mut last_feef: Option<u64> = None;
    let mut off: u64 = GHO_SECTOR_SIZE;
    while off + GHO_SECTOR_SIZE <= probe_limit {
        reader.seek(SeekFrom::Start(off))?;
        if reader.read_exact(&mut buf).is_err() {
            break;
        }
        if buf[0..2] == GHO_MAGIC {
            last_feef = Some(off);
        }
        off += GHO_SECTOR_SIZE;
    }
    if let Some(feef_off) = last_feef {
        return Ok(feef_off + GHO_SECTOR_SIZE);
    }

    // Pass 2 (fallback): first non-zero sector.
    let mut off: u64 = GHO_SECTOR_SIZE;
    while off + GHO_SECTOR_SIZE <= probe_limit {
        reader.seek(SeekFrom::Start(off))?;
        if reader.read_exact(&mut buf).is_err() {
            break;
        }
        if buf.iter().any(|&b| b != 0) {
            return Ok(off);
        }
        off += GHO_SECTOR_SIZE;
    }
    Ok(GHO_SECTOR_SIZE)
}

fn read_fully_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match reader.read(&mut buf[total..])? {
            0 => break,
            n => total += n,
        }
    }
    Ok(total)
}

fn decode_sector_block_stream<R: Read + Seek, W: Write>(
    reader: &mut R,
    data_start: u64,
    data_end: u64,
    compression: GhoCompression,
    writer: &mut W,
) -> Result<u64> {
    let mut total: u64 = 0;
    let mut decoded = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
    let mut block_buf = vec![0u8; u16::MAX as usize];

    let mut off = data_start;
    let record_magic = GHO_RECORD_MAGIC.to_le_bytes();
    while off + 2 <= data_end {
        reader.seek(SeekFrom::Start(off))?;
        // The block stream is terminated by a record header (e.g. a
        // 0x0703 Continuation or End record). Peek the 10-byte header
        // window: if the record magic sits at offset+4, we've reached the
        // end of this span's blocks. (Found empirically — the 11.5
        // compressed SECTOR fixture ends its block stream with a 0x0703
        // record, not a length sentinel.)
        let mut peek = [0u8; GHO_RECORD_HEADER_LEN];
        let peek_n = read_fully_or_eof(reader, &mut peek)?;
        if peek_n >= 8 && peek[4..8] == record_magic {
            break;
        }
        let len_buf = [peek[0], peek[1]];
        let stored_len = u16::from_le_bytes(len_buf) as usize;
        // `stored_len == 0` and `stored_len < 4` both mean "no usable chunk
        // here" — treat as end-of-stream.
        if stored_len < 4 {
            break;
        }
        let comp_len = stored_len.saturating_sub(2);
        if comp_len > block_buf.len() {
            bail!(
                "GHO SECTOR block at offset {:#x}: comp_len {} exceeds u16 sanity bound",
                off,
                comp_len
            );
        }
        let block = &mut block_buf[..comp_len];
        reader.seek(SeekFrom::Start(off + 2))?;
        reader
            .read_exact(block)
            .with_context(|| format!("reading block bytes at offset {:#x}", off + 2))?;

        let n = match compression {
            GhoCompression::Fast => fast_lz_decompress(block, comp_len, &mut decoded)
                .with_context(|| format!("Fast-LZ decode at offset {:#x}", off))?,
            GhoCompression::High => {
                // SECTOR-mode High blocks are pure zlib streams (no 4-byte
                // prefix), unlike the file-aware 0x0002 records.
                use flate2::read::ZlibDecoder;
                let mut dec = ZlibDecoder::new(&block[..]);
                let mut n = 0;
                loop {
                    if n >= decoded.len() {
                        bail!(
                            "GHO SECTOR zlib block at offset {:#x} decoded > {} bytes",
                            off,
                            decoded.len()
                        );
                    }
                    match dec.read(&mut decoded[n..]) {
                        Ok(0) => break,
                        Ok(k) => n += k,
                        Err(e) => {
                            return Err(anyhow::Error::from(e)
                                .context(format!("zlib decode at offset {:#x}", off)));
                        }
                    }
                }
                n
            }
            GhoCompression::None | GhoCompression::Other(_) => {
                bail!("decode_sector_block_stream called with non-block compression mode");
            }
        };
        writer
            .write_all(&decoded[..n])
            .with_context(|| format!("writing decoded block at output offset {}", total))?;
        total += n as u64;
        off += stored_len as u64;
    }
    Ok(total)
}

// ---------------------------------------------------------------------------
// GhoReader — streaming Read+Seek over a SECTOR-mode GHO (or span set)
// ---------------------------------------------------------------------------
//
// Eliminates the temp-file decode for browse / inspect / metadata reads.
// Modelled after ChdReader: open once (cheap index pass, no
// decompression), then random-access reads of the logical disk image
// pull-decompress one block at a time, cached.
//
// Only SECTOR mode is supported here. File-aware mode keeps the legacy
// decode-to-temp path (it has no LBA-addressable block layout). For
// uncompressed SECTOR backups the reader is a thin wrapper over
// SpanReader; for Fast / High compression the open scan walks the
// `[u16 stored_len][block_body]` chunks recording each block's file
// offset and stored_len, decompresses only the final block to learn its
// exact size, and assumes the standard 32 KiB decompressed length for
// every other block (Ghost's writer emits fixed-size blocks
// throughout). A debug assertion fires inside `decode_block_into` if a
// non-final block ever decompresses to a different size.

const GHO_BLOCK_DECOMPRESSED_SIZE: usize = FAST_LZ_BLOCK_SIZE; // 32 KiB

#[derive(Debug, Clone)]
struct GhoBlockEntry {
    /// Byte offset of the `[u16 stored_len]` prefix in the SpanReader.
    file_offset: u64,
    /// Total length on disk (includes the 2-byte stored_len prefix).
    stored_len: u16,
    /// Where this block's first decompressed byte sits in the logical image.
    output_offset: u64,
    /// Decompressed length. 32 KiB for all blocks except possibly the last.
    decompressed_len: u32,
}

/// Streaming `Read + Seek` over a Norton Ghost SECTOR-mode image. Opens
/// in milliseconds (header parse + index walk, no decompression) and
/// decompresses one block on each read miss, caching the last block.
///
/// Multi-file span sets are handled transparently via [`SpanReader`];
/// callers pass the primary `.gho` path and span discovery happens
/// inside [`GhoReader::open`].
///
/// Handles both SECTOR-mode (streaming the raw block stream) and
/// file-aware (in-memory virtual FAT image whose data clusters are
/// resolved on demand by decompressing `0x0002`/`0x0102` records).
/// Password-protected GHOs still return an error from `open`.
pub struct GhoReader {
    inner: SpanReader,
    logical_size: u64,
    position: u64,
    mode: GhoReaderMode,
}

/// One partition inside a multi-partition file-aware GHO.
struct FileAwarePartition {
    disk_offset: u64,
    image: VirtualFatImage,
    cluster_cache: Option<(u32, Vec<u8>)>,
}

// Exactly one GhoReaderMode exists per GhoReader, moved by value only once at
// open time. The size spread between variants (the NtfsFileAware index dominates)
// is therefore a single ~1 KB move, not a per-read cost — boxing the large
// variant would instead add heap indirection on the read hot path, so we keep
// the variants inline.
#[allow(clippy::large_enum_variant)]
enum GhoReaderMode {
    Uncompressed {
        data_start: u64,
    },
    Blocked {
        compression: GhoCompression,
        blocks: Vec<GhoBlockEntry>,
        cache_block_index: Option<usize>,
        cache_buf: Vec<u8>,
        /// Synthesized MBR + EBRs for multi-partition sector mode.
        /// Empty for single-partition images.
        synth_sectors: std::collections::HashMap<u64, [u8; 512]>,
    },
    FileAware {
        partitions: Vec<FileAwarePartition>,
        compression: GhoCompression,
        mbr: [u8; 512],
        ebrs: std::collections::HashMap<u64, [u8; 512]>,
    },
    NtfsFileAware {
        index: NtfsGhoIndex,
        last_run_hint: usize,
        compressed: Option<NtfsCompressedState>,
    },
}

impl GhoReader {
    /// Open a GHO (single file or span set) for streaming reads, without a
    /// password. Equivalent to [`GhoReader::open_with_password`] with `None`.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_password(path, None)
    }

    /// Open a GHO (single file or span set) for streaming reads, optionally
    /// supplying a password for encrypted images.
    ///
    /// For password-protected images the seed is derived from `password`,
    /// validated against the container verifier, and transparent body
    /// decryption is enabled on the underlying reader (see `gho_crypto`).
    /// Returns an error if the password is missing or incorrect.
    pub fn open_with_password(path: &Path, password: Option<&[u8]>) -> Result<Self> {
        let span_set = discover_gho_span_set(path)
            .with_context(|| format!("discovering span set for {}", path.display()))?;

        let mut primary = File::open(&span_set[0])
            .with_context(|| format!("opening {}", span_set[0].display()))?;
        let header = GhoContainerHeader::parse(&mut primary)
            .with_context(|| format!("parsing GHO header from {}", span_set[0].display()))?;
        drop(primary);

        if let Some(desc) = &header.description {
            log::info!("Ghost description: {}", desc);
        }
        let comp_label = match header.compression {
            GhoCompression::None => "none",
            GhoCompression::Fast => "Fast (LZ)",
            GhoCompression::High => "High (zlib)",
            GhoCompression::Other(_) => "unknown",
        };
        let type_label = match header.image_type {
            GhoImageType::FileAware => "file-aware",
            GhoImageType::Sector => "sector-by-sector",
            GhoImageType::Other(_) => "unknown",
        };
        log::info!(
            "Ghost image: {} compression, {} mode",
            comp_label,
            type_label
        );

        if header.container_version != 0x01 {
            bail!(
                "GHO {} container version {:#04x} not supported by GhoReader",
                path.display(),
                header.container_version
            );
        }
        // Derive + verify the password seed up front so a wrong/missing
        // password fails before we touch the data stream.
        let password_seed: Option<u16> = if header.password_protected {
            let pw = password.ok_or_else(|| {
                anyhow!(
                    "GHO {} is password-protected; a password is required to open it",
                    path.display()
                )
            })?;
            let seed = gho_seed_from_password(pw);
            match &header.password_verifier {
                Some(v) if gho_verify_seed(v, seed) => Some(seed),
                Some(_) => bail!(
                    "GHO {}: incorrect password (verifier mismatch)",
                    path.display()
                ),
                None => bail!(
                    "GHO {} is flagged password-protected but carries no verifier",
                    path.display()
                ),
            }
        } else {
            None
        };

        let mut inner = SpanReader::open(&span_set)
            .with_context(|| format!("opening span set for {}", path.display()))?;
        let file_size = inner.total_len();

        // Enable transparent decryption before any decode path reads the
        // stream. The encryption shape depends on the image mode:
        //   * file-aware       -> per record body (headers stay plaintext)
        //   * SECTOR + None     -> contiguous image in 32 KiB chunks
        //   * SECTOR + compress -> NOT encrypted (Ghost 11.5 stores the block
        //                          stream plaintext even with a password set)
        //
        // For SECTOR mode we must locate `data_start` on the *plaintext* sub-
        // headers before switching the reader into decrypting mode, so the
        // FEEF scan can't trip over decrypted disk bytes. The value is cached
        // and reused by the SECTOR dispatch below.
        let mut sector_data_start: Option<u64> = None;
        if let Some(seed) = password_seed {
            match header.image_type {
                GhoImageType::FileAware => {
                    let header_end = (GHO_HEADER_PREFIX_LEN + GHO_PASSWORD_VERIFIER_LEN) as u64;
                    let bodies = collect_gho_body_ranges(&mut inner, header_end, file_size)
                        .with_context(|| {
                            format!("mapping encrypted record bodies for {}", path.display())
                        })?;
                    let body_count = bodies.len();
                    inner.enable_decryption(seed, bodies);
                    log::info!(
                        "GHO {}: password accepted, decrypting {} record bodies",
                        path.display(),
                        body_count
                    );
                }
                GhoImageType::Sector => {
                    let ds = find_sector_data_start(&mut inner, file_size)?;
                    sector_data_start = Some(ds);
                    match header.compression {
                        GhoCompression::None => {
                            inner.enable_chunked_decryption(
                                seed,
                                ds,
                                FAST_LZ_BLOCK_SIZE as u64,
                                file_size,
                            );
                            log::info!(
                                "GHO {}: password accepted, decrypting SECTOR data in {}-byte \
                                 chunks from {:#x}",
                                path.display(),
                                FAST_LZ_BLOCK_SIZE,
                                ds
                            );
                        }
                        _ => {
                            log::info!(
                                "GHO {}: compressed SECTOR image — Ghost 11.5 stores the block \
                                 stream unencrypted; password verified, no decryption applied",
                                path.display()
                            );
                        }
                    }
                }
                GhoImageType::Other(_) => {}
            }
        }

        // ---- File-aware dispatch ----
        if matches!(header.image_type, GhoImageType::FileAware) {
            // Try to find the FAT record stream. NTFS file-aware backups
            // don't use it — they have GHPR metadata + packed cluster runs
            // instead. We detect this by checking whether the record stream
            // magic appears in the header region.
            let header_end = if header.password_protected {
                GHO_HEADER_PREFIX_LEN as u64 + GHO_PASSWORD_VERIFIER_LEN as u64
            } else {
                GHO_HEADER_PREFIX_LEN as u64
            };
            let has_record_stream = find_inner_stream_start(&mut inner, header_end).is_ok();

            if !has_record_stream {
                // No FAT record stream — try NTFS file-aware path.
                log::info!("No FAT record stream found, trying NTFS file-aware path...");
                let cache_key = NtfsScanKey::from_span_set(&span_set);
                match try_open_ntfs_file_aware(&mut inner, file_size, header.compression, cache_key)
                {
                    Ok((mode, volume_size)) => {
                        return Ok(Self {
                            inner,
                            logical_size: volume_size,
                            position: 0,
                            mode,
                        });
                    }
                    Err(e) => {
                        bail!(
                            "GHO {} is file-aware but has no FAT record stream and \
                             NTFS detection also failed: {:#}",
                            path.display(),
                            e
                        );
                    }
                }
            }

            log::info!("Scanning Ghost record stream...");
            let image = parse_gho_image(&mut inner, file_size, &header)
                .with_context(|| format!("parsing inner record stream for {}", path.display()))?;
            log::info!(
                "Found {} records ({} cached bodies)",
                image.records.len(),
                image.cached_bodies.len()
            );

            let mut slices = split_partitions(&mut inner, &image)
                .with_context(|| format!("splitting partitions for {}", path.display()))?;

            if slices.len() <= 1 {
                // Single-partition: use the original path (whole tree).
                let tree = walk_file_aware_tree(&mut inner, &image)
                    .with_context(|| format!("walking file-aware tree for {}", path.display()))?;
                log::info!(
                    "Reconstructing FAT partition from {} files, {} directories...",
                    tree.file_count(),
                    tree.dir_count()
                );
                let virtual_image =
                    build_virtual_fat_image(&mut inner, &image, &tree, header.compression)
                        .with_context(|| {
                            format!("building virtual FAT image for {}", path.display())
                        })?;
                let logical_size = virtual_image.total_size;
                return Ok(Self {
                    inner,
                    logical_size,
                    position: 0,
                    mode: GhoReaderMode::FileAware {
                        partitions: vec![FileAwarePartition {
                            disk_offset: 0,
                            image: virtual_image,
                            cluster_cache: None,
                        }],
                        compression: header.compression,
                        mbr: [0u8; 512],
                        ebrs: std::collections::HashMap::new(),
                    },
                });
            }

            // Multi-partition: resolve absolute offsets, then build
            // one VirtualFatImage per partition.
            resolve_absolute_offsets(&mut slices);
            let mbr = synthesize_mbr(&slices);
            let primary_end = slices[0].hidden_sectors + slices[0].partition_size / 512;
            let ext_slices: Vec<GhoPartitionSlice> = slices[1..].to_vec();
            let ebrs = if !ext_slices.is_empty() {
                synthesize_ebrs(&ext_slices, primary_end)
            } else {
                std::collections::HashMap::new()
            };

            log::info!(
                "Reconstructing {} partitions from sparsely populated Ghost image...",
                slices.len()
            );
            let mut fa_parts = Vec::with_capacity(slices.len());
            let mut max_end: u64 = 0;
            for (pi, slice) in slices.iter().enumerate() {
                let sub_records = image.records[slice.record_range.clone()].to_vec();
                let sub_bodies: std::collections::HashMap<u64, Vec<u8>> = sub_records
                    .iter()
                    .filter_map(|r| {
                        image
                            .cached_bodies
                            .get(&r.offset)
                            .map(|b| (r.offset, b.clone()))
                    })
                    .collect();
                let sub_image = GhoImage {
                    records: sub_records,
                    cached_bodies: sub_bodies,
                    partition_count: 1,
                };
                let tree = walk_file_aware_tree(&mut inner, &sub_image).with_context(|| {
                    format!(
                        "walking file-aware tree for partition at LBA {}",
                        slice.hidden_sectors
                    )
                })?;
                log::info!(
                    "  Partition {} of {}: {} ({} files, {} dirs)",
                    pi + 1,
                    slices.len(),
                    slice.fs_type,
                    tree.file_count(),
                    tree.dir_count()
                );
                let virtual_image =
                    build_virtual_fat_image(&mut inner, &sub_image, &tree, header.compression)
                        .with_context(|| {
                            format!(
                                "building virtual FAT image for partition at LBA {}",
                                slice.hidden_sectors
                            )
                        })?;
                let disk_offset = slice.hidden_sectors * 512;
                let part_end = disk_offset + virtual_image.total_size;
                if part_end > max_end {
                    max_end = part_end;
                }
                fa_parts.push(FileAwarePartition {
                    disk_offset,
                    image: virtual_image,
                    cluster_cache: None,
                });
            }

            log::info!(
                "Multi-partition file-aware GHO: {} partitions, logical disk size {}",
                fa_parts.len(),
                max_end
            );

            return Ok(Self {
                inner,
                logical_size: max_end,
                position: 0,
                mode: GhoReaderMode::FileAware {
                    partitions: fa_parts,
                    compression: header.compression,
                    mbr,
                    ebrs,
                },
            });
        }

        if !matches!(header.image_type, GhoImageType::Sector) {
            bail!(
                "GHO {} image_type {:?} not yet handled by GhoReader",
                path.display(),
                header.image_type
            );
        }

        // Reuse the `data_start` computed (on plaintext) when decryption was
        // enabled; otherwise scan for it now. Re-scanning after chunked
        // decryption is enabled would read decrypted disk bytes and could
        // mistake a stray `FE EF` for a sub-header.
        let data_start = match sector_data_start {
            Some(ds) => ds,
            None => find_sector_data_start(&mut inner, file_size)?,
        };

        match header.compression {
            GhoCompression::None => {
                // TODO: multi-partition uncompressed sector mode
                // (split at 0x0703 records) is not yet supported.
                let logical_size = file_size.saturating_sub(data_start);
                Ok(Self {
                    inner,
                    logical_size,
                    position: 0,
                    mode: GhoReaderMode::Uncompressed { data_start },
                })
            }
            GhoCompression::Fast | GhoCompression::High => {
                let index = index_sector_blocks(&mut inner, data_start, file_size)?;
                let mut blocks = index.blocks;
                let partition_boundaries = index.partition_boundaries;

                if partition_boundaries.is_empty() {
                    // Single partition — fix last block and return.
                    if let Some(last) = blocks.last_mut() {
                        let mut tmp = vec![0u8; GHO_BLOCK_DECOMPRESSED_SIZE + 1024];
                        let n = decode_block_into(&mut inner, last, header.compression, &mut tmp)
                            .with_context(|| {
                            format!("sizing final block of {}", path.display())
                        })?;
                        last.decompressed_len = n as u32;
                    }
                    let logical_size = blocks
                        .last()
                        .map(|b| b.output_offset + b.decompressed_len as u64)
                        .unwrap_or(0);
                    return Ok(Self {
                        inner,
                        logical_size,
                        position: 0,
                        mode: GhoReaderMode::Blocked {
                            compression: header.compression,
                            blocks,
                            cache_block_index: None,
                            cache_buf: Vec::with_capacity(GHO_BLOCK_DECOMPRESSED_SIZE + 1024),
                            synth_sectors: std::collections::HashMap::new(),
                        },
                    });
                }

                // Multi-partition sector mode. Each partition's blocks
                // are currently output_offset'd as if they're
                // contiguous. We need to:
                // 1. Fix the last block of each partition's range.
                // 2. Read VBR from each partition's first block to
                //    get hidden_sectors.
                // 3. Re-assign output_offsets to place each partition
                //    at hidden_sectors * 512.
                // 4. Synthesize MBR.

                // Partition ranges: [0..boundaries[0]),
                // [boundaries[0]..boundaries[1]), ...
                let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
                let mut prev = 0;
                for &b in &partition_boundaries {
                    ranges.push(prev..b);
                    prev = b;
                }
                ranges.push(prev..blocks.len());

                // Fix last block of each partition range. Trim trailing
                // blocks that fail to decompress (they're padding or
                // trailer data that the indexer picked up).
                let mut tmp = vec![0u8; GHO_BLOCK_DECOMPRESSED_SIZE + 1024];
                let mut adjusted_ranges = Vec::with_capacity(ranges.len());
                for range in &ranges {
                    if range.is_empty() {
                        adjusted_ranges.push(range.clone());
                        continue;
                    }
                    let mut end = range.end;
                    while end > range.start {
                        match decode_block_into(
                            &mut inner,
                            &blocks[end - 1],
                            header.compression,
                            &mut tmp,
                        ) {
                            Ok(n) => {
                                blocks[end - 1].decompressed_len = n as u32;
                                break;
                            }
                            Err(_) => {
                                end -= 1;
                            }
                        }
                    }
                    adjusted_ranges.push(range.start..end);
                }
                let ranges = adjusted_ranges;

                // Read each partition's VBR from its first block.
                let mut part_infos: Vec<(u64, u64, String)> = Vec::new();
                for range in &ranges {
                    if range.is_empty() {
                        part_infos.push((0, 0, "Unknown".to_string()));
                        continue;
                    }
                    let first = &blocks[range.start];
                    let n = decode_block_into(&mut inner, first, header.compression, &mut tmp)?;
                    if n >= 64 {
                        let bpb = &tmp[..n];
                        let bps = u16::from_le_bytes([bpb[11], bpb[12]]) as u64;
                        let hidden =
                            u32::from_le_bytes([bpb[28], bpb[29], bpb[30], bpb[31]]) as u64;
                        let tot16 = u16::from_le_bytes([bpb[19], bpb[20]]) as u64;
                        let tot32 = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]) as u64;
                        let total = if tot16 != 0 { tot16 } else { tot32 };
                        let fs = if n >= 90 && &bpb[82..87] == b"FAT32" {
                            "FAT32"
                        } else if n >= 62 && &bpb[54..59] == b"FAT16" {
                            "FAT16"
                        } else {
                            "Unknown"
                        };
                        let size = total * bps.max(512);
                        part_infos.push((hidden, size, fs.to_string()));
                    } else {
                        part_infos.push((0, 0, "Unknown".to_string()));
                    }
                }

                // Resolve absolute disk offsets via the shared helper.
                let mut tmp_slices: Vec<GhoPartitionSlice> = part_infos
                    .iter()
                    .map(|(hidden, size, fs)| GhoPartitionSlice {
                        boot_record_index: 0,
                        record_range: 0..0,
                        partition_size: *size,
                        hidden_sectors: *hidden,
                        fs_type: fs.clone(),
                        reserved_record_index: None,
                    })
                    .collect();
                resolve_absolute_offsets(&mut tmp_slices);
                let abs_offsets: Vec<u64> = tmp_slices.iter().map(|s| s.hidden_sectors).collect();

                // Re-assign output_offsets: each partition at its
                // absolute disk offset.
                let mut max_end: u64 = 0;
                for (ri, range) in ranges.iter().enumerate() {
                    let disk_offset = abs_offsets.get(ri).copied().unwrap_or(0) * 512;
                    let mut part_output: u64 = 0;
                    for idx in range.clone() {
                        blocks[idx].output_offset = disk_offset + part_output;
                        part_output += blocks[idx].decompressed_len as u64;
                    }
                    let end = disk_offset + part_output;
                    if end > max_end {
                        max_end = end;
                    }
                }

                // Update part_infos with resolved absolute offsets.
                for (i, abs) in abs_offsets.iter().enumerate() {
                    part_infos[i].0 = *abs;
                }

                // Synthesize MBR + EBRs. Filter out empty partitions.
                let slices: Vec<GhoPartitionSlice> = part_infos
                    .iter()
                    .filter(|(_, size, _)| *size > 0)
                    .map(|(hidden, size, fs)| GhoPartitionSlice {
                        boot_record_index: 0,
                        record_range: 0..0,
                        partition_size: *size,
                        hidden_sectors: *hidden,
                        fs_type: fs.clone(),
                        reserved_record_index: None,
                    })
                    .collect();
                let mbr = synthesize_mbr(&slices);
                let mut synth_sectors = std::collections::HashMap::new();
                synth_sectors.insert(0u64, mbr);

                if slices.len() > 1 {
                    let primary_end = slices[0].hidden_sectors + slices[0].partition_size / 512;
                    let ext_slices: Vec<GhoPartitionSlice> = slices[1..].to_vec();
                    for (lba, ebr) in synthesize_ebrs(&ext_slices, primary_end) {
                        synth_sectors.insert(lba, ebr);
                    }
                }

                let logical_size = max_end.max(512);

                log::info!(
                    "Multi-partition SECTOR-mode GHO: {} partitions, {} total blocks, logical size {}",
                    ranges.len(),
                    blocks.len(),
                    logical_size
                );

                Ok(Self {
                    inner,
                    logical_size,
                    position: 0,
                    mode: GhoReaderMode::Blocked {
                        compression: header.compression,
                        blocks,
                        cache_block_index: None,
                        cache_buf: Vec::with_capacity(GHO_BLOCK_DECOMPRESSED_SIZE + 1024),
                        synth_sectors,
                    },
                })
            }
            GhoCompression::Other(b) => bail!(
                "GHO {} has unknown compression byte {:#04x}",
                path.display(),
                b
            ),
        }
    }

    /// Logical (decompressed) byte length of the disk image.
    pub fn logical_size(&self) -> u64 {
        self.logical_size
    }

    /// Number of indexed compressed blocks. For uncompressed images this
    /// is 0 (the reader streams raw bytes). Exposed for diagnostics and
    /// tests.
    pub fn block_count(&self) -> usize {
        match &self.mode {
            GhoReaderMode::Uncompressed { .. } => 0,
            GhoReaderMode::Blocked { blocks, .. } => blocks.len(),
            GhoReaderMode::FileAware { partitions, .. } => partitions
                .iter()
                .map(|p| p.image.cluster_to_file.len())
                .sum(),
            GhoReaderMode::NtfsFileAware { index, .. } => index.runs.len(),
        }
    }

    fn ensure_block_cached(&mut self, idx: usize) -> std::io::Result<()> {
        let GhoReaderMode::Blocked {
            compression,
            blocks,
            cache_block_index,
            cache_buf,
            ..
        } = &mut self.mode
        else {
            return Ok(());
        };
        if *cache_block_index == Some(idx) {
            return Ok(());
        }
        let block = &blocks[idx];
        cache_buf.clear();
        cache_buf.resize(GHO_BLOCK_DECOMPRESSED_SIZE + 1024, 0);
        let n = decode_block_into(&mut self.inner, block, *compression, cache_buf)
            .map_err(|e| std::io::Error::other(format!("GHO block {}: {:#}", idx, e)))?;
        debug_assert_eq!(
            n as u32, block.decompressed_len,
            "block {} decompressed to {} bytes, index says {}",
            idx, n, block.decompressed_len
        );
        cache_buf.truncate(n);
        *cache_block_index = Some(idx);
        Ok(())
    }
}

unsafe impl Send for GhoReader {}

impl Read for GhoReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.position >= self.logical_size {
            return Ok(0);
        }
        match &self.mode {
            GhoReaderMode::Uncompressed { data_start } => {
                let phys = data_start + self.position;
                let remaining = self.logical_size - self.position;
                let to_read = (buf.len() as u64).min(remaining) as usize;
                self.inner.seek(SeekFrom::Start(phys))?;
                let n = self.inner.read(&mut buf[..to_read])?;
                self.position += n as u64;
                Ok(n)
            }
            GhoReaderMode::Blocked {
                blocks,
                synth_sectors,
                ..
            } => {
                // Check synthesized sectors (MBR, EBRs) first.
                let sector_lba = self.position / 512;
                if let Some(sector_data) = synth_sectors.get(&sector_lba) {
                    let off = (self.position % 512) as usize;
                    let avail = 512 - off;
                    let remaining = (self.logical_size - self.position) as usize;
                    let n = buf.len().min(avail).min(remaining);
                    buf[..n].copy_from_slice(&sector_data[off..off + n]);
                    self.position += n as u64;
                    return Ok(n);
                }

                // Binary search for the block containing `self.position`.
                let idx = match blocks.binary_search_by(|b| b.output_offset.cmp(&self.position)) {
                    Ok(i) => i,
                    Err(0) => {
                        // Position is before the first block (gap area).
                        let first_off = blocks.first().map(|b| b.output_offset).unwrap_or(0);
                        if self.position < first_off {
                            let gap = (first_off - self.position) as usize;
                            let n = buf.len().min(gap).min(512);
                            for b in &mut buf[..n] {
                                *b = 0;
                            }
                            self.position += n as u64;
                            return Ok(n);
                        }
                        0
                    }
                    Err(i) => i - 1,
                };

                // Check if position is actually in a gap between
                // partitions (multi-partition: blocks may not be
                // contiguous in output space).
                let block = &blocks[idx];
                let block_end = block.output_offset + block.decompressed_len as u64;
                if self.position >= block_end {
                    // In a gap. Return zeros up to the next block.
                    let next_off = if idx + 1 < blocks.len() {
                        blocks[idx + 1].output_offset
                    } else {
                        self.logical_size
                    };
                    let gap = (next_off - self.position) as usize;
                    let n = buf.len().min(gap).min(512);
                    for b in &mut buf[..n] {
                        *b = 0;
                    }
                    self.position += n as u64;
                    return Ok(n);
                }

                self.ensure_block_cached(idx)?;
                let GhoReaderMode::Blocked {
                    blocks, cache_buf, ..
                } = &self.mode
                else {
                    unreachable!()
                };
                let block = &blocks[idx];
                let off_in_block = (self.position - block.output_offset) as usize;
                let avail = cache_buf.len().saturating_sub(off_in_block);
                let n = avail.min(buf.len());
                buf[..n].copy_from_slice(&cache_buf[off_in_block..off_in_block + n]);
                self.position += n as u64;
                Ok(n)
            }
            GhoReaderMode::FileAware { .. } => {
                let remaining = self.logical_size - self.position;
                let want = (buf.len() as u64).min(remaining) as usize;
                let position = self.position;
                let n = self.read_file_aware_into(position, &mut buf[..want])?;
                self.position += n as u64;
                Ok(n)
            }
            GhoReaderMode::NtfsFileAware { .. } => {
                let remaining = self.logical_size - self.position;
                let want = (buf.len() as u64).min(remaining) as usize;
                let position = self.position;
                let GhoReaderMode::NtfsFileAware {
                    index,
                    last_run_hint,
                    compressed,
                } = &mut self.mode
                else {
                    unreachable!()
                };
                let n = read_ntfs_file_aware_into(
                    &mut self.inner,
                    index,
                    last_run_hint,
                    compressed,
                    position,
                    &mut buf[..want],
                )?;
                self.position += n as u64;
                Ok(n)
            }
        }
    }
}

impl GhoReader {
    /// Prepare for a faithful whole-disk export. For NTFS file-aware GHOs this
    /// forces the lazy run scan to completion, then builds the synthesized
    /// `$Bitmap` and recovers `$Boot`'s boot code so the exported image is both
    /// mountable and bootable. No-op for other GHO shapes.
    ///
    /// Export paths call this (via `open_source_reader`); inspect/browse don't,
    /// so they keep fast lazy opens (and serve VBR + zeros for `$Boot`, which is
    /// fine since they never need the boot code).
    pub fn prepare_full_image(&mut self) {
        let GhoReaderMode::NtfsFileAware {
            index, compressed, ..
        } = &mut self.mode
        else {
            return;
        };

        // Complete the run index so $Bitmap and $Boot recovery see every run.
        if let Some(cs) = compressed.as_mut() {
            if cs.lazy_scan.is_some() {
                extend_ntfs_compressed_index(&mut self.inner, index, cs, u64::MAX);
            }
        }

        // Post-scan content-aware fix-up: rewire runs the streaming matcher
        // misassigned among same-cc candidates by inspecting first-4-byte magic
        // (INDX / FILE / RSTR|RCRD). Resolves the cross-links that otherwise
        // cause chkdsk /f to orphan files. See fixup_ntfs_typed_misalignments.
        // Disable with RUSTYBACKUP_GHO_NTFS_NO_FIXUP=1 for diagnostic comparison.
        if std::env::var("RUSTYBACKUP_GHO_NTFS_NO_FIXUP")
            .map(|v| v == "1")
            .unwrap_or(false)
        {
            log::warn!("NTFS fixup disabled via RUSTYBACKUP_GHO_NTFS_NO_FIXUP");
        } else if let Ok(vbr_parsed) = crate::fs::ntfs::parse_vbr(&index.vbr) {
            fixup_ntfs_typed_misalignments(&mut self.inner, index, compressed, &vbr_parsed);
        }

        // Build $Bitmap (Ghost omits it).
        if index.synth_bitmap.is_empty() && !index.bitmap_runs.is_empty() {
            let total_clusters = index.volume_size / index.cluster_size;
            let mut allocation: Vec<(u64, u64)> = index
                .runs
                .iter()
                .map(|r| (r.lcn_start, r.cluster_count))
                .collect();
            allocation.extend_from_slice(&index.bitmap_runs);
            index.synth_bitmap = build_synth_ntfs_bitmap(&allocation, total_clusters);
            log::info!("Prepared $Bitmap: {} bytes", index.synth_bitmap.len());
        }

        // Recover $Boot's boot code (mislabeled by Ghost; matched via the VBR).
        if index.boot_code.is_empty() {
            match recover_ntfs_boot_code(&mut self.inner, index, compressed) {
                Some(bc) => {
                    log::info!("Recovered $Boot boot code: {} bytes", bc.len());
                    index.boot_code = bc;
                }
                None => log::warn!(
                    "Could not locate $Boot in the GHO stream; exported disk will \
                     mount but not boot"
                ),
            }
        }

        // Refresh the cache with the now-complete $Bitmap + $Boot so a later
        // export (or browse) of this archive reuses them too.
        if let Some(cs) = compressed.as_ref() {
            if let Some(key) = cs.cache_key.clone() {
                ntfs_scan_cache_store(&key, index, cs);
            }
        }
    }

    /// Mapped NTFS cluster runs as `(lcn_start, cluster_count)`, sorted by LCN.
    /// Only meaningful after `prepare_full_image` (so the run set is complete);
    /// `None` for non-NTFS-file-aware GHOs. Used by diagnostics to report which
    /// files have declared clusters not covered by any mapped run.
    pub fn ntfs_mapped_ranges(&self) -> Option<Vec<(u64, u64)>> {
        let GhoReaderMode::NtfsFileAware { index, .. } = &self.mode else {
            return None;
        };
        let mut ranges: Vec<(u64, u64)> = index
            .runs
            .iter()
            .map(|r| (r.lcn_start, r.cluster_count))
            .collect();
        ranges.sort_by_key(|r| r.0);
        Some(ranges)
    }

    /// Diagnostic / recovery: every "sentinel" run (lcn_start == u64::MAX —
    /// recorded by the lazy scan but never assigned a real LCN), returning
    /// `(cluster_count, seq, full_decoded_bytes)`. The bytes ARE the dropped
    /// data — captured in the stream, just unplaced — so callers can splice
    /// them back into a file with a hole at the right offset. `max_bytes` caps
    /// the per-run read (pass a cluster-size multiple).
    pub fn debug_sentinel_runs_full(&mut self, max_bytes: usize) -> Vec<(u64, u64, Vec<u8>)> {
        let GhoReaderMode::NtfsFileAware {
            index, compressed, ..
        } = &mut self.mode
        else {
            return Vec::new();
        };
        let sentinels: Vec<(u64, u64, u64)> = index
            .runs
            .iter()
            .filter(|r| r.lcn_start == u64::MAX)
            .map(|r| (r.cluster_count, r.seq, r.file_offset))
            .collect();
        let mut out = Vec::with_capacity(sentinels.len());
        for (cc, seq, fo) in sentinels {
            let want = ((cc * index.cluster_size) as usize).min(max_bytes);
            let mut buf = vec![0u8; want];
            let mut got = 0usize;
            if let Some(cs) = compressed.as_mut() {
                let mut rdr = NtfsDecompressingReader {
                    inner: &mut self.inner,
                    state: cs,
                    position: fo,
                };
                while got < want {
                    match rdr.read(&mut buf[got..]) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => got += n,
                    }
                }
            } else if self.inner.seek(SeekFrom::Start(fo)).is_ok() {
                while got < want {
                    match self.inner.read(&mut buf[got..]) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => got += n,
                    }
                }
            }
            buf.truncate(got);
            out.push((cc, seq, buf));
        }
        out
    }

    /// Serve bytes for FileAware mode at `position` into `out`.
    fn read_file_aware_into(&mut self, position: u64, out: &mut [u8]) -> std::io::Result<usize> {
        let GhoReaderMode::FileAware {
            partitions,
            compression,
            mbr,
            ebrs,
        } = &mut self.mode
        else {
            unreachable!()
        };

        let sector_lba = position / 512;
        let off_in_sector = (position % 512) as usize;
        let avail_in_sector = 512 - off_in_sector;
        let to_read = out.len().min(avail_in_sector);
        if to_read == 0 {
            return Ok(0);
        }

        // 1) MBR at sector 0 (only for multi-partition images that
        //    have a real synthesized MBR, not the zero placeholder).
        if sector_lba == 0 && mbr[510] == 0x55 && mbr[511] == 0xAA {
            out[..to_read].copy_from_slice(&mbr[off_in_sector..off_in_sector + to_read]);
            return Ok(to_read);
        }

        // 2) EBR sectors.
        if let Some(ebr) = ebrs.get(&sector_lba) {
            out[..to_read].copy_from_slice(&ebr[off_in_sector..off_in_sector + to_read]);
            return Ok(to_read);
        }

        // 3) Partition data: find the partition containing this position.
        for part in partitions.iter_mut() {
            let part_end = part.disk_offset + part.image.total_size;
            if position >= part.disk_offset && position < part_end {
                let pos_in_part = position - part.disk_offset;
                return read_partition_data_at(
                    &mut self.inner,
                    &part.image,
                    *compression,
                    &mut part.cluster_cache,
                    pos_in_part,
                    out,
                    to_read,
                );
            }
        }

        // 4) Default: zero (inter-partition gap).
        for b in &mut out[..to_read] {
            *b = 0;
        }
        Ok(to_read)
    }
}

/// Serve bytes from a single `VirtualFatImage` at `pos_in_part` (byte
/// offset within the partition).
fn read_partition_data_at(
    inner: &mut SpanReader,
    image: &VirtualFatImage,
    compression: GhoCompression,
    cluster_cache: &mut Option<(u32, Vec<u8>)>,
    pos_in_part: u64,
    out: &mut [u8],
    to_read: usize,
) -> std::io::Result<usize> {
    let bps = image.bytes_per_sector as u64;
    let sector_lba = pos_in_part / bps;
    let off_in_sector = (pos_in_part % bps) as usize;
    let avail_in_sector = (bps as usize) - off_in_sector;
    let to_read = to_read.min(avail_in_sector);

    // Sparse metadata hit (VBR, FAT tables, root dir, etc.)?
    if let Some(stored) = image.sparse.get(&sector_lba) {
        out[..to_read].copy_from_slice(&stored[off_in_sector..off_in_sector + to_read]);
        return Ok(to_read);
    }

    // File-owned cluster?
    let data_start_sector = image.data_start_sector as u64;
    if sector_lba >= data_start_sector {
        let spc = image.sectors_per_cluster as u64;
        let cluster_offset = sector_lba - data_start_sector;
        let cluster_id = (cluster_offset / spc + 2) as u32;
        if let Some(&(file_id, cluster_index_in_file)) = image.cluster_to_file.get(&cluster_id) {
            let cluster_bytes = ensure_cluster_decoded(
                inner,
                image,
                compression,
                file_id,
                cluster_index_in_file,
                cluster_id,
                cluster_cache,
            )?;
            let sector_in_cluster = (cluster_offset % spc) as usize;
            let byte_in_cluster = sector_in_cluster * bps as usize + off_in_sector;
            let end = (byte_in_cluster + to_read).min(cluster_bytes.len());
            if end > byte_in_cluster {
                let span = end - byte_in_cluster;
                out[..span].copy_from_slice(&cluster_bytes[byte_in_cluster..end]);
                for b in &mut out[span..to_read] {
                    *b = 0;
                }
            } else {
                for b in &mut out[..to_read] {
                    *b = 0;
                }
            }
            return Ok(to_read);
        }
    }

    for b in &mut out[..to_read] {
        *b = 0;
    }
    Ok(to_read)
}

/// Decode the requested cluster's bytes for a file-aware FAT cluster.
/// Caches the most recently decoded cluster to avoid re-decoding when
/// consecutive sectors target the same cluster.
fn ensure_cluster_decoded(
    inner: &mut SpanReader,
    image: &VirtualFatImage,
    compression: GhoCompression,
    file_id: u32,
    cluster_index_in_file: u32,
    cluster_id: u32,
    cache: &mut Option<(u32, Vec<u8>)>,
) -> std::io::Result<Vec<u8>> {
    if let Some((cached_id, cached_bytes)) = cache {
        if *cached_id == cluster_id {
            return Ok(cached_bytes.clone());
        }
    }

    let file = &image.files[file_id as usize];
    let cluster_size = image.cluster_size as usize;
    let byte_in_file = cluster_index_in_file as u64 * image.cluster_size;

    let mut content =
        GhoFileContentReader::new(inner, file.records.clone(), compression, file.file_size);

    // Skip to the start of this cluster's bytes within the file.
    let mut skip = byte_in_file;
    let mut sink = [0u8; 8192];
    while skip > 0 {
        let want = (skip as usize).min(sink.len());
        let n = content.read(&mut sink[..want])?;
        if n == 0 {
            break;
        }
        skip -= n as u64;
    }

    // Read up to cluster_size bytes from this cluster.
    let mut out = vec![0u8; cluster_size];
    let mut filled = 0usize;
    while filled < cluster_size {
        let n = content.read(&mut out[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    out.truncate(filled.max(cluster_size).min(cluster_size));
    out.resize(cluster_size, 0);

    *cache = Some((cluster_id, out.clone()));
    Ok(out)
}

impl Seek for GhoReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos: i128 = match pos {
            SeekFrom::Start(o) => o as i128,
            SeekFrom::Current(o) => self.position as i128 + o as i128,
            SeekFrom::End(o) => self.logical_size as i128 + o as i128,
        };
        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "GhoReader seek before start",
            ));
        }
        self.position = (new_pos as u64).min(self.logical_size);
        Ok(self.position)
    }
}

impl DataLen for GhoReader {
    fn total_len(&self) -> u64 {
        self.logical_size
    }
}

/// Walk the compressed block stream once without decompressing any
/// block except the last. Records each block's file offset, stored_len,
/// and computed logical output offset; decompresses only the final
/// block to learn its exact length so `logical_size` is correct down
/// to the byte.
///
/// Reads in 256 KiB chunks to avoid per-block seek+read syscalls —
/// highly-compressible data can produce millions of tiny blocks.
/// Result of indexing the sector-mode block stream, including
/// partition boundaries from `0x0703` continuation records.
struct SectorBlockIndex {
    blocks: Vec<GhoBlockEntry>,
    /// Block indices where new partitions start. The first partition
    /// implicitly starts at block 0; entries here mark subsequent ones.
    partition_boundaries: Vec<usize>,
}

fn index_sector_blocks<R: Read + Seek>(
    reader: &mut R,
    data_start: u64,
    data_end: u64,
) -> Result<SectorBlockIndex> {
    let mut blocks = Vec::new();
    let mut partition_boundaries = Vec::new();
    let mut off = data_start;
    let mut output_offset: u64 = 0;
    let record_magic = GHO_RECORD_MAGIC.to_le_bytes();

    const CHUNK: usize = 256 * 1024;
    let mut buf = vec![0u8; CHUNK];
    reader.seek(SeekFrom::Start(data_start))?;
    let mut buf_file_off: u64 = data_start;
    let mut buf_valid: usize = read_fully_or_eof(reader, &mut buf)?;

    while off + 2 <= data_end {
        let pos = (off - buf_file_off) as usize;
        if pos + GHO_RECORD_HEADER_LEN > buf_valid {
            reader.seek(SeekFrom::Start(off))?;
            buf_file_off = off;
            buf_valid = read_fully_or_eof(reader, &mut buf)?;
            if buf_valid < GHO_RECORD_HEADER_LEN {
                break;
            }
            continue;
        }

        let peek = &buf[pos..pos + GHO_RECORD_HEADER_LEN];
        if peek[4..8] == record_magic {
            // This is a record header. Check type to decide action.
            let type_code = u16::from_le_bytes([peek[0], peek[1]]);
            let body_len = u16::from_le_bytes([peek[8], peek[9]]);
            if type_code == GHO_REC_CONTINUATION {
                // Partition boundary — skip this record, mark boundary,
                // and continue indexing.
                partition_boundaries.push(blocks.len());
                off += GHO_RECORD_HEADER_LEN as u64 + body_len as u64;

                // After a 0x0703 continuation, Ghost inserts a 512-byte
                // FEEF sub-header before the next partition's block
                // stream. Skip it if present.
                if off + 2 <= data_end {
                    reader.seek(SeekFrom::Start(off))?;
                    let mut feef_check = [0u8; 2];
                    if reader.read_exact(&mut feef_check).is_ok() && feef_check == GHO_MAGIC {
                        off += GHO_SECTOR_SIZE;
                    }
                }

                // Re-fill buffer from new offset.
                reader.seek(SeekFrom::Start(off))?;
                buf_file_off = off;
                buf_valid = read_fully_or_eof(reader, &mut buf)?;
                continue;
            }
            // Any other record type (0x0023 end, etc.) = stop.
            break;
        }
        let stored_len = u16::from_le_bytes([peek[0], peek[1]]) as u64;
        if stored_len < 4 {
            break;
        }
        if off + stored_len > data_end {
            break;
        }
        blocks.push(GhoBlockEntry {
            file_offset: off,
            stored_len: stored_len as u16,
            output_offset,
            decompressed_len: GHO_BLOCK_DECOMPRESSED_SIZE as u32,
        });
        output_offset += GHO_BLOCK_DECOMPRESSED_SIZE as u64;
        off += stored_len;
    }
    Ok(SectorBlockIndex {
        blocks,
        partition_boundaries,
    })
}

/// Decompress one indexed block into `dst`. `dst` must be at least
/// `GHO_BLOCK_DECOMPRESSED_SIZE + 1024` bytes; the actual decompressed
/// length is returned.
fn decode_block_into<R: Read + Seek>(
    reader: &mut R,
    block: &GhoBlockEntry,
    compression: GhoCompression,
    dst: &mut [u8],
) -> Result<usize> {
    let comp_len = (block.stored_len as usize).saturating_sub(2);
    let mut buf = vec![0u8; comp_len];
    reader.seek(SeekFrom::Start(block.file_offset + 2))?;
    reader
        .read_exact(&mut buf)
        .with_context(|| format!("reading block body at {:#x}", block.file_offset + 2))?;
    match compression {
        GhoCompression::Fast => fast_lz_decompress(&buf, comp_len, dst)
            .with_context(|| format!("Fast-LZ decode at {:#x}", block.file_offset)),
        GhoCompression::High => {
            use flate2::read::ZlibDecoder;
            let mut dec = ZlibDecoder::new(&buf[..]);
            let mut n = 0;
            loop {
                if n >= dst.len() {
                    bail!(
                        "GHO SECTOR zlib block at {:#x} decoded > {} bytes",
                        block.file_offset,
                        dst.len()
                    );
                }
                match dec.read(&mut dst[n..]) {
                    Ok(0) => return Ok(n),
                    Ok(k) => n += k,
                    Err(e) => {
                        return Err(anyhow::Error::from(e)
                            .context(format!("zlib decode at {:#x}", block.file_offset)));
                    }
                }
            }
        }
        GhoCompression::None | GhoCompression::Other(_) => {
            bail!("decode_block_into called with non-block compression")
        }
    }
}

/// Result of materializing a GHO container to a temp file. Same shape as
/// [`crate::rbformats::imz::ImzMaterialized`] so the GUI's
/// `prepare_disk_image_path` can treat both formats identically.
#[derive(Debug)]
pub struct GhoMaterialized {
    /// Path to the raw decoded partition image inside the tempdir.
    pub temp_path: std::path::PathBuf,
    /// Bytes written to `temp_path` (== sum of decompressed block bytes).
    pub logical_size: u64,
    /// Tempdir guard — callers MUST hold this for the lifetime of the
    /// materialized file. Dropping it removes the file.
    pub guard: tempfile::TempDir,
    /// Number of partition records found in the container.
    pub partition_count: usize,
}

// ---------------------------------------------------------------------------
// Inner record stream (session 5.5b)
// ---------------------------------------------------------------------------

/// Magic that precedes every record's body, little-endian (`0x012F18D8`).
/// Bytes on disk: `D8 18 2F 01`.
pub const GHO_RECORD_MAGIC: u32 = 0x012F_18D8;

/// On-disk byte length of a record header (`u16 type | u16 marker | u32
/// magic | u16 body_len`).
pub const GHO_RECORD_HEADER_LEN: usize = 10;

/// Maximum bytes we'll scan past the container header looking for the first
/// inner-stream record magic. In our corpus the inner stream starts well
/// before 16 KiB (7.5 at 0xC00, 11.5 at 0x1600). Cap at 64 KiB to bound
/// pathological cases.
pub const GHO_INNER_STREAM_SCAN_LIMIT: u64 = 64 * 1024;

/// Single record header.
///
/// Layout reverse-engineered from our fixture corpus:
///
/// ```text
/// offset  size  field
/// 0       2     type        u16 LE — record kind (e.g. 0x0017 = first
///                            sector / BPB block, 0x0004 = FAT-entry-style
///                            payload). Exact taxonomy lives in 5.6.
/// 2       2     marker      u16 LE — 0x0000 on Ghost 7.5, 0x95FD on Ghost
///                            11.5 across every fixture in our corpus.
///                            Likely a per-version checksum/discriminator.
///                            Surfaced raw; not used for dispatch.
/// 4       4     magic       u32 LE — always 0x012F18D8 (sanity check).
/// 8       2     body_len    u16 LE — number of body bytes that follow.
/// ```
///
/// The 7.5-vs-11.5 record stream is identical at (type, body_len) level —
/// only `marker` differs. Confirmed by `record_streams_match_between_75_and_115`
/// against `7.5/PART/PART.GHO` and `11.5/GH11/fulldisk.GHS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GhoRecordHeader {
    /// Record kind. Raw u16; meanings catalogued by 5.6.
    pub type_code: u16,
    /// Per-version marker. `0x0000` on 7.5, `0x95FD` on 11.5.
    pub marker: u16,
    /// Body length in bytes (does NOT include this header).
    pub body_len: u16,
}

impl GhoRecordHeader {
    /// Parse a record header from a 10-byte slice. Returns `Err` if the
    /// magic doesn't match (caller should treat that as end-of-stream).
    pub fn parse_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < GHO_RECORD_HEADER_LEN {
            bail!(
                "GHO record header needs {} bytes, got {}",
                GHO_RECORD_HEADER_LEN,
                bytes.len()
            );
        }
        let type_code = u16::from_le_bytes([bytes[0], bytes[1]]);
        let marker = u16::from_le_bytes([bytes[2], bytes[3]]);
        let magic = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if magic != GHO_RECORD_MAGIC {
            bail!(
                "GHO record magic at expected offset is {:#010x}, expected {:#010x}",
                magic,
                GHO_RECORD_MAGIC
            );
        }
        let body_len = u16::from_le_bytes([bytes[8], bytes[9]]);
        Ok(Self {
            type_code,
            marker,
            body_len,
        })
    }

    /// Read a record header from `reader` at its current position. Returns
    /// `Ok(None)` on clean end-of-file, `Err` on malformed data.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Option<Self>> {
        let mut buf = [0u8; GHO_RECORD_HEADER_LEN];
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        Self::parse_bytes(&buf).map(Some)
    }
}

/// Locate the start of the inner record stream by scanning forward from
/// `header_end` for the record magic `0xD8182F01`.
///
/// Ghost pads the container metadata region to a different sector count
/// on 7.5 vs 11.5 (sector 6 vs sector 11 in our corpus), so we don't
/// hardcode the offset — the scan-forward approach absorbs the version
/// difference without per-version code.
///
/// `header_end` is the byte offset where parsing should resume — typically
/// `GHO_HEADER_PREFIX_LEN` (12) when the file has no password verifier,
/// `GHO_HEADER_PREFIX_LEN + GHO_PASSWORD_VERIFIER_LEN` (28) when it does.
pub fn find_inner_stream_start<R: Read + Seek>(reader: &mut R, header_end: u64) -> Result<u64> {
    let needle = GHO_RECORD_MAGIC.to_le_bytes();
    let mut buf = vec![0u8; 4096];
    let mut scanned: u64 = 0;
    let mut window_start = header_end;
    reader
        .seek(SeekFrom::Start(header_end))
        .context("seeking past container header")?;

    // Carry the last 3 bytes of the prior chunk so we don't miss a
    // magic that straddles a chunk boundary.
    let mut tail: Vec<u8> = Vec::new();
    while scanned < GHO_INNER_STREAM_SCAN_LIMIT {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        // Build a scan slice = tail + new chunk.
        let mut scan = Vec::with_capacity(tail.len() + n);
        scan.extend_from_slice(&tail);
        scan.extend_from_slice(&buf[..n]);

        if let Some(pos) = scan.windows(needle.len()).position(|w| w == needle) {
            // Magic starts at byte (window_start + pos - tail.len()),
            // but the record HEADER starts 4 bytes earlier (type+marker).
            let magic_offset = window_start + pos as u64 - tail.len() as u64;
            let header_offset = magic_offset.saturating_sub(4);
            return Ok(header_offset);
        }

        // Keep last (needle.len() - 1) bytes for the next iteration.
        let keep = needle.len() - 1;
        if scan.len() >= keep {
            tail = scan[scan.len() - keep..].to_vec();
        } else {
            tail = scan;
        }
        window_start += n as u64;
        scanned += n as u64;
    }
    Err(anyhow!(
        "GHO inner-stream magic not found within {} bytes of container header",
        GHO_INNER_STREAM_SCAN_LIMIT
    ))
}

/// Walk the inner record stream (reading only the plaintext 10-byte headers)
/// and collect every record's body range `(body_start, body_len)`.
///
/// Used to build the decryption map for password-protected images: each body
/// is independently encrypted, so we need the exact byte ranges to decrypt.
/// Records with a zero-length body are skipped (nothing to decrypt).
fn collect_gho_body_ranges<R: Read + Seek>(
    reader: &mut R,
    header_end: u64,
    file_size: u64,
) -> Result<Vec<(u64, u32)>> {
    // Mirror `parse_gho_image`'s walk so the body map covers exactly the
    // records the real (decrypted) decode pass will read: bound by file size,
    // resync on a magic mismatch, stop at a clean end.
    let mut offset = find_inner_stream_start(&mut *reader, header_end)
        .context("locating inner record stream for encrypted GHO")?;
    let mut bodies = Vec::new();
    while offset + GHO_RECORD_HEADER_LEN as u64 <= file_size {
        reader.seek(SeekFrom::Start(offset))?;
        let rec = match GhoRecordHeader::read_from(reader) {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(_) => match find_inner_stream_start(&mut *reader, offset) {
                Ok(o) if o > offset && o < file_size => {
                    offset = o;
                    continue;
                }
                _ => break,
            },
        };
        let body_start = offset + GHO_RECORD_HEADER_LEN as u64;
        if rec.body_len > 0 {
            bodies.push((body_start, rec.body_len as u32));
        }
        offset = body_start + rec.body_len as u64;
    }
    Ok(bodies)
}

/// Iterator that walks the records in a Ghost backup's inner stream.
///
/// Each `next()` reads a 10-byte header, validates the magic, then **skips
/// past the body** (using `Seek::seek` — bodies can be 512 bytes of FAT
/// data and we don't need to materialize them here in 5.5b). The body is
/// not returned; callers that need it should re-seek to
/// `current_offset()` and read `header.body_len` bytes themselves. The
/// 5.6 decoder will swap this iterator for a streaming variant that
/// returns body bytes.
pub struct GhoRecordIter<R: Read + Seek> {
    reader: R,
    /// Next-record byte offset within `reader`.
    next_offset: u64,
    /// Records read so far (diagnostics).
    count: usize,
}

impl<R: Read + Seek> GhoRecordIter<R> {
    /// Build an iterator that starts reading at `start_offset` in `reader`.
    pub fn new(mut reader: R, start_offset: u64) -> Result<Self> {
        reader.seek(SeekFrom::Start(start_offset))?;
        Ok(Self {
            reader,
            next_offset: start_offset,
            count: 0,
        })
    }

    /// Byte offset within the source reader where the next record starts.
    pub fn current_offset(&self) -> u64 {
        self.next_offset
    }

    /// Records yielded so far. Named `records_read` (not `count`) to
    /// avoid shadowing the `Iterator::count()` trait method, which
    /// consumes the iterator.
    pub fn records_read(&self) -> usize {
        self.count
    }
}

impl<R: Read + Seek> Iterator for GhoRecordIter<R> {
    type Item = Result<GhoRecordHeader>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(e) = self.reader.seek(SeekFrom::Start(self.next_offset)) {
            return Some(Err(e.into()));
        }
        let header = match GhoRecordHeader::read_from(&mut self.reader) {
            Ok(Some(h)) => h,
            Ok(None) => return None,
            Err(e) => return Some(Err(e)),
        };
        let advance = GHO_RECORD_HEADER_LEN as u64 + header.body_len as u64;
        self.next_offset += advance;
        self.count += 1;
        Some(Ok(header))
    }
}

// ============================================================================
// 5.5c — Fast-LZ block decoder
// ============================================================================
//
// Norton Ghost's "Z1" Fast-LZ is a custom LZ77 variant ported from the
// MIT-licensed clean-room reference `github.com/nyarime/gho/fastlz.go`
// (itself reverse-engineered from Ghost 11.5.1 `sub_4DDD70` via IDA — no
// Symantec source consulted, see the provenance rule in
// `docs/virtualization-formats.md` §5).
//
// Each on-disk block carries a 4-byte prefix; if the first byte is 1 the
// remaining `comp_len - 4` bytes are stored verbatim, otherwise the payload
// is compressed.
//
// Compressed stream uses 16-bit control words: each bit selects literal (0)
// or 2-byte match token (1). A match token packs a 12-bit hash-table index
// (b1 | ((b0 & 0xF0) << 4)) and a 4-bit extra-length (b0 & 0x0F); copy
// length is `3 + extra_len` bytes from the previous output at the hashed
// position. Hash function:
//
//     h = ((-24993 * (b2 ^ (16 * (b1 ^ (16 * b0))))) >> 4) & 0xFFF
//
// Token loop runs 16 tokens per control word, except within 32 bytes of
// `src_end` where it falls back to 1 token at a time (per the reference).
// Hash-table entries start as a "sentinel" pointing at the 18-byte literal
// `"123456789012345678"` — matches resolved against the sentinel copy from
// that string (zero-padded past byte 17). Decode-to-temp wiring lands in 5.6.

/// Fast-LZ hash-table size (12-bit hash → 4096 entries).
pub const FAST_LZ_HASH_SIZE: usize = 4096;

/// Sentinel literal pre-populated into hash slots Ghost has never resolved
/// against an actual position. Reads past the sentinel return zero.
pub const FAST_LZ_SENTINEL: &[u8; 18] = b"123456789012345678";

/// Decoded block size Ghost picks for partition data (32 KiB). Caller
/// supplies the destination buffer; the decoder never allocates.
pub const FAST_LZ_BLOCK_SIZE: usize = 32 * 1024;

/// Compute the 12-bit Fast-LZ hash for 3 consecutive bytes.
///
/// `h = ((-24993 * (b2 ^ (16 * (b1 ^ (16 * b0))))) >> 4) & 0xFFF`.
/// Multiplication wraps modulo 2^32 (matches Ghost's i32×i32→i32 cast +
/// reinterpret-as-u32 + unsigned shift).
#[inline]
pub fn fast_lz_hash(b0: u8, b1: u8, b2: u8) -> usize {
    let v = (b2 as i32) ^ (16 * ((b1 as i32) ^ (16 * (b0 as i32))));
    let prod = (-24993_i32).wrapping_mul(v) as u32;
    ((prod >> 4) & 0xFFF) as usize
}

/// Decompress one Fast-LZ block into `dst`. Returns the number of bytes
/// written.
///
/// `data` must contain at least `comp_len` valid bytes (the on-disk block
/// payload, excluding the 2-byte `stored_len` prefix from the outer
/// container framing). `dst` must be pre-allocated to at least
/// `FAST_LZ_BLOCK_SIZE` bytes; the decoder zero-fills `dst` before
/// writing, so the sentinel-fallback paths in the reference reproduce
/// faithfully (Ghost's reference also relies on its destination buffer
/// being pre-zeroed by `make`).
pub fn fast_lz_decompress(data: &[u8], comp_len: usize, dst: &mut [u8]) -> Result<usize> {
    if comp_len == 0 || data.len() < comp_len {
        bail!(
            "fast-lz: truncated input (comp_len={}, data_len={})",
            comp_len,
            data.len()
        );
    }

    // Mirror the reference: zero-fill the destination so out-of-range
    // hash-table-driven reads return 0 rather than uninitialized memory.
    for b in dst.iter_mut() {
        *b = 0;
    }

    // Uncompressed block: prefix byte == 1 → straight copy of bytes [4..comp_len).
    if data[0] == 1 {
        if comp_len < 4 {
            bail!("fast-lz: uncompressed block too short ({} < 4)", comp_len);
        }
        let n = comp_len - 4;
        if n > dst.len() {
            bail!(
                "fast-lz: uncompressed payload {} exceeds dst {}",
                n,
                dst.len()
            );
        }
        dst[..n].copy_from_slice(&data[4..4 + n]);
        return Ok(n);
    }

    // Compressed path.
    let mut hash_table: [i32; FAST_LZ_HASH_SIZE] = [-1; FAST_LZ_HASH_SIZE];

    let mut src: usize = 4;
    let src_end: usize = comp_len;
    let mut out_pos: usize = 0;

    // Sentinel control value forces an initial control-word reload.
    let mut control: u32 = 1;
    let mut literal_run: u16 = 0;
    let mut prev_literal_run: u16 = 0;

    'outer: while src < src_end {
        if control == 1 {
            if src + 1 >= src_end {
                break;
            }
            control = (data[src] as u32) | ((data[src + 1] as u32) << 8) | 0x10000;
            src += 2;
        }

        // Near-end safeguard from the reference: within 32 bytes of the
        // end of the compressed payload we decode one token at a time
        // (avoids over-reading the 2-byte match token at a boundary).
        let near_end = src_end < src + 32;
        let token_count = if near_end { 1 } else { 16 };

        for _ in 0..token_count {
            if src >= src_end {
                break;
            }

            if control & 1 != 0 {
                // Match reference: 2-byte token.
                if src + 1 >= src_end {
                    break 'outer;
                }
                let b0 = data[src];
                let b1 = data[src + 1];

                let hash_idx = (b1 as usize) | (((b0 as usize) & 0xF0) << 4);
                let extra_len = (b0 & 0x0F) as usize;

                let match_pos = hash_table[hash_idx];
                let match_start = out_pos;
                let total_copy = 3 + extra_len;

                // j indexes three independent sources (sentinel slice,
                // dst at match_pos + j, fallback zero) chosen by branch —
                // .enumerate() over any single source would lose clarity.
                #[allow(clippy::needless_range_loop)]
                for j in 0..total_copy {
                    if out_pos >= dst.len() {
                        bail!("fast-lz: output overflow at match copy");
                    }
                    let byte = if match_pos < 0 {
                        // Sentinel slot: copy from the literal "123…",
                        // zero past byte 17 (Ghost reads from a global buffer
                        // initialised that way).
                        if j < FAST_LZ_SENTINEL.len() {
                            FAST_LZ_SENTINEL[j]
                        } else {
                            0
                        }
                    } else {
                        let src_idx = match_pos as usize + j;
                        if src_idx < dst.len() {
                            dst[src_idx]
                        } else {
                            0
                        }
                    };
                    dst[out_pos] = byte;
                    out_pos += 1;
                }

                src += 2;

                // Register hash entries for the literal run that ended just
                // before this match (matches the reference's 1- or 2-entry
                // backfill).
                if literal_run > 0 {
                    let pos_isz = match_start as isize - literal_run as isize;
                    if pos_isz >= 0 {
                        let pos = pos_isz as usize;
                        if pos + 2 < out_pos {
                            let h = fast_lz_hash(dst[pos], dst[pos + 1], dst[pos + 2]);
                            hash_table[h] = pos as i32;
                            if prev_literal_run == 2 && pos + 3 < out_pos {
                                let h2 = fast_lz_hash(dst[pos + 1], dst[pos + 2], dst[pos + 3]);
                                hash_table[h2] = (pos + 1) as i32;
                            }
                        }
                    }
                    literal_run = 0;
                    prev_literal_run = 0;
                }

                hash_table[hash_idx] = match_start as i32;
            } else {
                // Literal byte.
                if out_pos >= dst.len() {
                    bail!("fast-lz: output overflow at literal copy");
                }
                literal_run += 1;
                dst[out_pos] = data[src];
                out_pos += 1;
                src += 1;
                prev_literal_run = literal_run;

                if literal_run == 3 {
                    let pos = out_pos - 3;
                    let h = fast_lz_hash(dst[pos], dst[pos + 1], dst[pos + 2]);
                    hash_table[h] = pos as i32;
                    literal_run = 2;
                    prev_literal_run = 2;
                }
            }

            control >>= 1;
            if control == 1 {
                break;
            }
        }
    }

    Ok(out_pos)
}

// ============================================================================
// 5.6 — Record-stream walker + block-stream decoder + decode-to-temp
// ============================================================================
//
// **Record taxonomy used by our fixture corpus** (verified against both
// Ghost 7.5 and 11.5 file-aware backups, 2026-05-26):
//
// | type   | body_len            | meaning                              |
// |--------|---------------------|--------------------------------------|
// | 0x0017 | 512                 | boot sector (start of a partition)   |
// | 0x0004 | 56                  | FAT entry / cluster-chain record     |
// | 0x0102 | varies (125, 720…)  | directory header                     |
// | 0x0103 | 20                  | directory descriptor                 |
// | 0x0104 | 56                  | file extent                          |
// | 0x0002 | block size (≤32768) | **data block** (1 chunk of cluster   |
// |        |                     | bytes; compressed per container.cmp) |
// | 0x0017 |                     | next partition's boot sector         |
//
// Decoding the partition image = concat the bodies of every `0x0002`
// record across the inner record stream, decompressing each per the
// container's compression byte:
//   - `0x00` (None) → body IS 32 KiB of raw cluster bytes
//   - `0x02` (Fast) → body is a Fast-LZ block (4-byte prefix + LZ token stream)
//   - `0x03` (High) → body is a zlib block (4-byte prefix + deflate stream)
//
// **Important:** the Go reference `nyarime/gho` describes a different
// hierarchy (Track0 0x0006 + Partition 0x0603 + Continuation 0x0703 +
// End 0x0023 + per-span `[u16 stored_len][data]` block frames). That
// taxonomy does **not** appear in any fixture in our corpus — neither
// 7.5 nor 11.5. The dialect documented here matches every fixture we
// have (PART.GHO, gh11part.GHO, High.GHO, FULLDISK.GHO, fulldisk.GHS,
// HPVectra95C.gho). 5.6's walker targets this dialect.
//
// Multi-partition (multiple `0x0017` boot-sector markers in one file)
// support is partial — we just concatenate every `0x0002` block we see,
// which yields the partitions stacked back-to-back rather than placed
// at MBR-declared LBAs. Full-disk reconstruction with MBR + LBA-positioned
// partitions is the next slice.

/// Inner record type carrying one data block (compressed per the
/// container's compression byte). Body_len is the on-disk block size.
pub const GHO_REC_DATA_BLOCK: u16 = 0x0002;

/// Boot-sector record on Ghost 7.5 (body = 512 bytes). Marks the start
/// of a partition.
pub const GHO_REC_BOOT_SECTOR: u16 = 0x0017;

/// Boot-sector record on Ghost 11.5 (body = 512 bytes). Same body shape
/// as [`GHO_REC_BOOT_SECTOR`]; the high bits seem to encode "header
/// section" framing — records in this section carry marker `0xC01E`.
pub const GHO_REC_BOOT_SECTOR_V2: u16 = 0x0717;

/// Boot-sector record on Ghost 7.5 **full-disk** mode (body = 512
/// bytes). Same body shape as [`GHO_REC_BOOT_SECTOR`]; the `0xae` high
/// byte tags the disk-level header section. Observed as the very first
/// record in `FULLDISK.GHO`, `HPVectra95C.gho`,
/// `fromdanilaptop.GHO`, and `XP_SP2FU.GHO`.
pub const GHO_REC_BOOT_SECTOR_FULLDISK: u16 = 0xae17;

/// Subsequent boot-sector record observed in `FULLDISK.GHO` after the
/// disk-level header section ends. Bytes are still a verbatim 512-byte
/// FAT BPB. Likely the partition boot sector (vs `0xae17` which carries
/// the same content as a disk-level header).
pub const GHO_REC_BOOT_SECTOR_PARTITION: u16 = 0x0117;

/// Root-directory entry record on Ghost 7.5 (body = 56 bytes). Each
/// record holds one 32-byte FAT directory entry (LFN segment, 8.3
/// entry, dot/dotdot, or empty slot) followed by a 24-byte trailer
/// (4-byte hash + 20 zero bytes). The root dir's entries are stored
/// as a contiguous run of these.
pub const GHO_REC_DIR_ENTRY_ROOT: u16 = 0x0004;

/// Root-directory entry record on Ghost 11.5 (body = 56 bytes). Same
/// 56-byte payload as [`GHO_REC_DIR_ENTRY_ROOT`]; appears once at the
/// start of the inner stream on 11.5 fixtures (instead of the
/// multi-entry run we see on 7.5).
pub const GHO_REC_DIR_ENTRY_ROOT_V2: u16 = 0x0704;

/// Subdirectory entry record (body = 56 bytes). Same on-disk layout as
/// [`GHO_REC_DIR_ENTRY_ROOT`]. Used for every directory below the root
/// on both Ghost 7.5 and 11.5.
pub const GHO_REC_DIR_ENTRY_SUB: u16 = 0x0104;

/// Directory-entry record in Ghost 7.5 **full-disk** mode, disk-level
/// header section (body = 56 bytes). Same body shape as
/// [`GHO_REC_DIR_ENTRY_ROOT`]; `0xae` high byte tags the header
/// section. Observed at the start of `FULLDISK.GHO` directly after the
/// `0xae17` boot sector.
pub const GHO_REC_DIR_ENTRY_FULLDISK: u16 = 0xae04;

/// File-content record (body = variable length). Carries either the
/// entire content of a small file (≤ 32 KiB) or the trailing fragment
/// of a larger file (`file_size mod 32768` bytes after N `0x0002`
/// blocks).
pub const GHO_REC_FILE_TAIL: u16 = 0x0102;

/// Per-file checksum record (body = 20 bytes). Layout
/// `[u32 cksum][u32 cksum_dup][12 zero bytes]` — the same 32-bit
/// value stored twice for integrity.
pub const GHO_REC_FILE_CHECKSUM: u16 = 0x0103;

/// Reserved-sectors blob for a FAT32 partition. Body = VBR + FSInfo +
/// backup VBR + padding. `body_len == reserved_sectors × bytes_per_sector`.
pub const GHO_REC_RESERVED_SECTORS: u16 = 0x0118;

/// Zlib-compressed per-partition file catalog. One record per partition
/// at the end of a multi-partition file-aware backup.
pub const GHO_REC_FILE_CATALOG: u16 = 0x0005;

/// End-of-image marker (body = 24 bytes). Last record in both
/// file-aware and sector-mode backups.
pub const GHO_REC_END: u16 = 0x0023;

/// Sector-mode continuation / partition-boundary marker (body = 20
/// bytes: `[u32 checksum][u32 checksum_dup][12 zero bytes]`). Separates
/// per-partition block streams in multi-partition SECTOR-mode backups.
pub const GHO_REC_CONTINUATION: u16 = 0x0703;

/// Returns true if `type_code` is a boot-sector marker.
///
/// Both Ghost 7.5 (`0x0017`) and 11.5 (`0x0717`) use a 512-byte body
/// that's a verbatim copy of the partition's first sector.
pub fn is_boot_sector_record(type_code: u16) -> bool {
    matches!(
        type_code,
        GHO_REC_BOOT_SECTOR
            | GHO_REC_BOOT_SECTOR_V2
            | GHO_REC_BOOT_SECTOR_FULLDISK
            | GHO_REC_BOOT_SECTOR_PARTITION
    )
}

/// Returns true if `type_code` carries one 32-byte FAT directory entry
/// (root or subdirectory variant, 7.5 or 11.5). All variants share the
/// same 56-byte body layout — see [`parse_fat_dir_entry_body`].
pub fn is_dir_entry_record(type_code: u16) -> bool {
    matches!(
        type_code,
        GHO_REC_DIR_ENTRY_ROOT
            | GHO_REC_DIR_ENTRY_ROOT_V2
            | GHO_REC_DIR_ENTRY_SUB
            | GHO_REC_DIR_ENTRY_FULLDISK
    )
}

/// Returns true if `type_code` carries cluster / file content bytes
/// (full 32 KiB block for `0x0002`, variable-length tail for `0x0102`).
pub fn is_data_block_record(type_code: u16) -> bool {
    type_code == GHO_REC_DATA_BLOCK || type_code == GHO_REC_FILE_TAIL
}

/// Returns true if `type_code` is the per-file checksum record.
pub fn is_checksum_record(type_code: u16) -> bool {
    type_code == GHO_REC_FILE_CHECKSUM
}

/// One parsed directory-entry record (`0x0004` / `0x0104` / `0x0704`).
///
/// The first 32 bytes are the verbatim FAT directory entry (LFN
/// segment, 8.3 entry, dot/dotdot, or empty slot). Whether it's an
/// LFN slot or an 8.3 entry is determined by the attribute byte at
/// offset 11 (`0x0F == LFN slot`). The 24-byte trailer carries a
/// per-entry 32-bit hash followed by 20 reserved bytes (all zero in
/// every fixture observed).
#[derive(Debug, Clone, Copy)]
pub struct GhoDirEntryRecord {
    /// The 32-byte FAT directory entry, ready to be written verbatim
    /// into a rebuilt directory cluster.
    pub fat_entry: [u8; 32],
    /// 32-bit per-entry hash (purpose not yet reverse-engineered;
    /// possibly CRC-32 of the entry). Preserved for diagnostics +
    /// future integrity checks.
    pub entry_hash: u32,
}

impl GhoDirEntryRecord {
    /// FAT attribute byte at offset 11.
    pub fn attr_byte(&self) -> u8 {
        self.fat_entry[11]
    }

    /// True when the entry is a long-filename slot (`attr == 0x0F`).
    pub fn is_lfn_slot(&self) -> bool {
        self.attr_byte() == 0x0F
    }

    /// True when the entry is a directory (`attr & 0x10`). Not valid
    /// for LFN slots.
    pub fn is_directory(&self) -> bool {
        !self.is_lfn_slot() && (self.attr_byte() & 0x10) != 0
    }

    /// True when the entry is a volume label (`attr & 0x08`). Not
    /// valid for LFN slots.
    pub fn is_volume_label(&self) -> bool {
        !self.is_lfn_slot() && (self.attr_byte() & 0x08) != 0
    }

    /// True when the entry is empty (first byte 0x00) — FAT directory
    /// "end of entries" sentinel.
    pub fn is_empty_slot(&self) -> bool {
        self.fat_entry[0] == 0x00
    }

    /// True when the entry is deleted (first byte 0xE5).
    pub fn is_deleted(&self) -> bool {
        self.fat_entry[0] == 0xE5
    }

    /// 8.3 entry's 32-bit starting cluster (high 16 + low 16 = full
    /// 32-bit cluster for FAT32; high is 0 for FAT12/16). Returns 0
    /// for LFN / empty / deleted slots.
    pub fn first_cluster(&self) -> u32 {
        if self.is_lfn_slot() || self.is_empty_slot() || self.is_deleted() {
            return 0;
        }
        let lo = u16::from_le_bytes([self.fat_entry[26], self.fat_entry[27]]) as u32;
        let hi = u16::from_le_bytes([self.fat_entry[20], self.fat_entry[21]]) as u32;
        (hi << 16) | lo
    }

    /// 8.3 entry's 32-bit file size in bytes. Returns 0 for LFN /
    /// empty / deleted slots and for directories.
    pub fn file_size(&self) -> u32 {
        if self.is_lfn_slot() || self.is_empty_slot() || self.is_deleted() || self.is_directory() {
            return 0;
        }
        u32::from_le_bytes([
            self.fat_entry[28],
            self.fat_entry[29],
            self.fat_entry[30],
            self.fat_entry[31],
        ])
    }
}

/// Parse a `0x0004` / `0x0104` / `0x0704` directory-entry record body.
///
/// `body.len()` must be at least 36 (32 FAT bytes + 4 hash bytes); the
/// remaining bytes are silently ignored (every fixture has the 24-byte
/// trailer with hash at offset 32, but the reserved tail isn't
/// required by the parser).
pub fn parse_fat_dir_entry_body(body: &[u8]) -> Result<GhoDirEntryRecord> {
    if body.len() < 36 {
        bail!(
            "GHO dir-entry record body must be >= 36 bytes, got {}",
            body.len()
        );
    }
    let mut fat_entry = [0u8; 32];
    fat_entry.copy_from_slice(&body[..32]);
    let entry_hash = u32::from_le_bytes([body[32], body[33], body[34], body[35]]);
    Ok(GhoDirEntryRecord {
        fat_entry,
        entry_hash,
    })
}

// ---------------------------------------------------------------------------
// File-aware directory tree walker (Slice B)
// ---------------------------------------------------------------------------
//
// Algorithm (derived from PART.GHO + cross-checked against records
// 60-200 of the same fixture — see docs/gho_file_aware.md for the
// trace):
//
//   1. Track `current_dir_cluster` (initialised from the boot sector's
//      BPB_RootClus on FAT32, or sentinel 0 for FAT12/16).
//   2. When a "." dir entry is seen, set `current_dir_cluster` to that
//      entry's `first_cluster`. The writer always emits "." right after
//      descending into a directory.
//   3. When a ".." dir entry is seen, it tells us the PARENT cluster of
//      the directory we just descended into. We back-fill
//      `parent_cluster` on the most-recently-emitted DIR entry.
//   4. For a regular file (8.3, attr != dir):
//      `parent_cluster` = `current_dir_cluster`. Subsequent `0x0002` /
//      `0x0102` content records attach to it; `0x0103` finalises it.
//   5. LFN slots (attr 0x0F) buffer up before each 8.3 record; they
//      decorate the next 8.3 entry as its `long_name`.
//   6. Empty slots (first byte 0x00) and deleted slots (0xE5) are
//      source-FAT padding artifacts — ignored.
//
// **Known limitation.** The walker uses `current_dir_cluster` (last "."
// cluster) as the parent for FILE entries. If Ghost's writer ascends
// through multiple levels between a "." entry and the next 8.3, that
// implicit ascent is invisible — files in the intervening levels
// would be attributed to the deeper dir. Subdir entries are NOT
// affected (they get their true parent via their own ".." entry).

/// One entry (file or directory) recovered from a file-aware GHO.
#[derive(Debug, Clone)]
pub struct GhoFileAwareEntry {
    pub source_cluster: u32,
    pub parent_cluster: u32,
    pub short_name: String,
    pub long_name: Option<String>,
    pub attr: u8,
    pub file_size: u32,
    /// Byte offsets within the GHO of this file's `0x0002` / `0x0102`
    /// content record bodies, in stream order. Empty for directories.
    pub content_record_offsets: Vec<u64>,
    /// `0x0103` checksum value (`None` for dirs or unfinalised files).
    pub checksum: Option<u32>,
}

impl GhoFileAwareEntry {
    pub fn is_directory(&self) -> bool {
        self.attr & 0x10 != 0 && self.attr != 0x0F
    }

    pub fn display_name(&self) -> &str {
        self.long_name.as_deref().unwrap_or(&self.short_name)
    }
}

/// Flat tree of file-aware entries; parent/child links are implicit
/// in [`GhoFileAwareEntry::parent_cluster`].
#[derive(Debug, Clone, Default)]
pub struct GhoFileAwareTree {
    pub root_cluster: u32,
    pub entries: Vec<GhoFileAwareEntry>,
}

impl GhoFileAwareTree {
    pub fn children_of(&self, dir_cluster: u32) -> impl Iterator<Item = &GhoFileAwareEntry> {
        self.entries
            .iter()
            .filter(move |e| e.parent_cluster == dir_cluster)
    }
    pub fn file_count(&self) -> usize {
        self.entries.iter().filter(|e| !e.is_directory()).count()
    }
    pub fn dir_count(&self) -> usize {
        self.entries.iter().filter(|e| e.is_directory()).count()
    }
}

fn format_8_3_name(raw: &[u8]) -> String {
    let name = String::from_utf8_lossy(&raw[..8]).trim_end().to_string();
    let ext = String::from_utf8_lossy(&raw[8..11]).trim_end().to_string();
    if ext.is_empty() {
        name
    } else {
        format!("{}.{}", name, ext)
    }
}

fn decode_lfn_fragment(fat_entry: &[u8; 32]) -> String {
    let mut chars: Vec<u16> = Vec::with_capacity(13);
    for &(s, e) in &[(1usize, 11usize), (14, 26), (28, 32)] {
        for chunk in fat_entry[s..e].chunks(2) {
            if chunk.len() != 2 {
                continue;
            }
            let c = u16::from_le_bytes([chunk[0], chunk[1]]);
            if c == 0 || c == 0xFFFF {
                return char::decode_utf16(chars).filter_map(|r| r.ok()).collect();
            }
            chars.push(c);
        }
    }
    char::decode_utf16(chars).filter_map(|r| r.ok()).collect()
}

/// Read a record's body, using the cache if available, otherwise
/// falling back to seeking in the file.
fn read_record_body<R: Read + Seek>(
    reader: &mut R,
    rec: &GhoInnerRecord,
    cache: &std::collections::HashMap<u64, Vec<u8>>,
) -> Result<Vec<u8>> {
    if let Some(body) = cache.get(&rec.offset) {
        return Ok(body.clone());
    }
    reader.seek(SeekFrom::Start(rec.body_start()))?;
    let mut body = vec![0u8; rec.body_len as usize];
    reader.read_exact(&mut body)?;
    Ok(body)
}

/// Walk an already-parsed [`GhoImage`] and produce the file-aware tree.
///
/// Uses `image.cached_bodies` to avoid re-seeking for dir entry and
/// checksum record bodies when available. Falls back to `reader` for
/// any record not in the cache.
pub fn walk_file_aware_tree<R: Read + Seek>(
    reader: &mut R,
    image: &GhoImage,
) -> Result<GhoFileAwareTree> {
    let cache = &image.cached_bodies;
    let mut root_cluster: u32 = 0;
    if let Some(bs) = image
        .records
        .iter()
        .find(|r| is_boot_sector_record(r.type_code))
    {
        if bs.body_len as usize >= 48 {
            let bpb = read_record_body(reader, bs, cache)?;
            if bpb.len() >= 87 && &bpb[82..87] == b"FAT32" {
                root_cluster = u32::from_le_bytes([bpb[44], bpb[45], bpb[46], bpb[47]]);
            }
        }
    }

    let mut tree = GhoFileAwareTree {
        root_cluster,
        entries: Vec::new(),
    };
    let mut current_dir = root_cluster;
    let mut lfn_buf: Vec<String> = Vec::new();
    let mut pending_dir: Option<usize> = None;
    let mut pending_file: Option<usize> = None;

    for rec in &image.records {
        if is_dir_entry_record(rec.type_code) && rec.body_len as usize >= 36 {
            let body = read_record_body(reader, rec, cache)?;
            let entry = parse_fat_dir_entry_body(&body)?;

            if entry.is_empty_slot() || entry.is_deleted() {
                lfn_buf.clear();
                continue;
            }
            if entry.is_lfn_slot() {
                let frag = decode_lfn_fragment(&entry.fat_entry);
                if entry.fat_entry[0] & 0x40 != 0 {
                    lfn_buf.clear();
                }
                lfn_buf.insert(0, frag);
                continue;
            }

            let short = format_8_3_name(&entry.fat_entry[..11]);
            if short == "." {
                current_dir = entry.first_cluster();
                lfn_buf.clear();
                continue;
            }
            if short == ".." {
                if let Some(idx) = pending_dir.take() {
                    let parent = entry.first_cluster();
                    tree.entries[idx].parent_cluster = if parent == 0 {
                        tree.root_cluster
                    } else {
                        parent
                    };
                }
                lfn_buf.clear();
                continue;
            }

            let long_name = if !lfn_buf.is_empty() {
                Some(lfn_buf.join(""))
            } else {
                None
            };
            lfn_buf.clear();

            let is_dir = entry.is_directory();
            tree.entries.push(GhoFileAwareEntry {
                source_cluster: entry.first_cluster(),
                parent_cluster: if is_dir { 0 } else { current_dir },
                short_name: short,
                long_name,
                attr: entry.attr_byte(),
                file_size: entry.file_size(),
                content_record_offsets: Vec::new(),
                checksum: None,
            });
            let idx = tree.entries.len() - 1;
            if is_dir {
                pending_dir = Some(idx);
                pending_file = None;
            } else {
                pending_file = Some(idx);
                pending_dir = None;
            }
        } else if is_data_block_record(rec.type_code) {
            if let Some(idx) = pending_file {
                tree.entries[idx]
                    .content_record_offsets
                    .push(rec.body_start());
            }
        } else if is_checksum_record(rec.type_code) && rec.body_len as usize >= 8 {
            if let Some(idx) = pending_file.take() {
                let body = read_record_body(reader, rec, cache)?;
                tree.entries[idx].checksum = parse_checksum_record_body(&body).ok();
            }
        }
    }

    Ok(tree)
}

/// Parse a `0x0103` per-file checksum record body.
///
/// The body is `[u32 cksum][u32 cksum_dup][12 zero bytes]`. We surface
/// the checksum once and assert internally that the duplicate matches
/// (mismatch → corruption or misidentified record).
pub fn parse_checksum_record_body(body: &[u8]) -> Result<u32> {
    if body.len() < 8 {
        bail!(
            "GHO checksum record body must be >= 8 bytes, got {}",
            body.len()
        );
    }
    let a = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
    let b = u32::from_le_bytes([body[4], body[5], body[6], body[7]]);
    if a != b {
        bail!(
            "GHO checksum record has mismatched duplicate: {:#x} vs {:#x}",
            a,
            b
        );
    }
    Ok(a)
}

// ---------------------------------------------------------------------------
// Slice C — file-aware FAT image emitter
// ---------------------------------------------------------------------------
//
// Given a parsed `GhoImage` + walked `GhoFileAwareTree`, reconstruct a
// fresh, mountable FAT partition image. The output is byte-equivalent to
// the source partition up to cluster allocation order: the original FAT
// chain is NOT preserved (file-aware GHOs don't store the source FAT at
// all), but file names, sizes, content, directory structure, and 8.3 LFN
// pairings ARE.
//
// Sizing: read BPB_TotSec from the source boot sector and pass that
// to `create_blank_fat`. The output FAT type may differ from the
// source's (e.g. our blank formatter picks FAT16 for 1 GiB, even if
// source was FAT32), but the result is mountable everywhere a current
// OS / Ghost Explorer would mount it.
//
// File content: for each file we already have `content_record_offsets`
// from slice B (a list of `0x0002` / `0x0102` body offsets in stream
// order). `GhoFileContentReader` streams decompressed bytes from those
// records on demand, so we never materialise an entire file in RAM
// just to call `create_file`.

/// Per-file content streamer over a file-aware GHO's record body
/// offsets. Yields the file's decompressed content in stream order,
/// truncated to `total_left` bytes (so the final `0x0102` tail doesn't
/// over-spill if the source happened to pad).
pub struct GhoFileContentReader<'a, R: Read + Seek> {
    reader: &'a mut R,
    record_meta: Vec<(u64, u16)>, // (body_start, body_len) — type code is irrelevant for decode
    compression: GhoCompression,
    next_idx: usize,
    decoded: Vec<u8>,
    pos: usize,
    end: usize,
    total_left: u64,
    body_buf: Vec<u8>,
}

impl<'a, R: Read + Seek> GhoFileContentReader<'a, R> {
    pub fn new(
        reader: &'a mut R,
        record_meta: Vec<(u64, u16)>,
        compression: GhoCompression,
        file_size: u64,
    ) -> Self {
        Self {
            reader,
            record_meta,
            compression,
            next_idx: 0,
            decoded: vec![0u8; FAST_LZ_BLOCK_SIZE + 1024],
            pos: 0,
            end: 0,
            total_left: file_size,
            body_buf: vec![0u8; u16::MAX as usize],
        }
    }

    fn fill_next(&mut self) -> std::io::Result<bool> {
        if self.next_idx >= self.record_meta.len() {
            return Ok(false);
        }
        let (body_start, body_len) = self.record_meta[self.next_idx];
        self.next_idx += 1;
        self.reader
            .seek(SeekFrom::Start(body_start))
            .map_err(std::io::Error::other)?;
        let body = &mut self.body_buf[..body_len as usize];
        self.reader.read_exact(body)?;
        let n = match self.compression {
            GhoCompression::None => {
                self.decoded[..body_len as usize].copy_from_slice(body);
                body_len as usize
            }
            GhoCompression::Fast => fast_lz_decompress(body, body_len as usize, &mut self.decoded)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            GhoCompression::High => zlib_decode_block(body, &mut self.decoded)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?,
            GhoCompression::Other(b) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsupported GHO compression byte {:#04x}", b),
                ))
            }
        };
        self.pos = 0;
        self.end = n;
        Ok(true)
    }
}

impl<'a, R: Read + Seek> Read for GhoFileContentReader<'a, R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.total_left == 0 || out.is_empty() {
            return Ok(0);
        }
        if self.pos >= self.end && !self.fill_next()? {
            return Ok(0);
        }
        let avail = (self.end - self.pos)
            .min(out.len())
            .min(self.total_left as usize);
        out[..avail].copy_from_slice(&self.decoded[self.pos..self.pos + avail]);
        self.pos += avail;
        self.total_left -= avail as u64;
        Ok(avail)
    }
}

/// Outcome of emitting a file-aware GHO to a fresh FAT image. The
/// `image` field is populated only by the Vec-returning variant
/// [`emit_file_aware_fat_image`]; the streaming variant
/// [`emit_file_aware_fat_image_to_sink`] leaves it empty since the
/// image is written directly to the caller's sink.
#[derive(Debug, Clone, Default)]
pub struct EmitFileAwareResult {
    pub image: Vec<u8>,
    pub bytes_written: u64,
    pub files_emitted: usize,
    pub dirs_emitted: usize,
    /// Entries we couldn't translate (e.g. name failed FAT validation).
    /// Includes the source short name + reason. Reconstruction continues
    /// past these; the resulting image is mountable but missing those
    /// entries.
    pub skipped: Vec<(String, String)>,
}

/// Read the source partition size in bytes from the boot sector record
/// (`0x0017` / `0x0717`). Returns `None` if the record is missing or the
/// BPB looks bogus.
fn source_partition_size_from_boot<R: Read + Seek>(
    reader: &mut R,
    image: &GhoImage,
) -> Result<Option<u64>> {
    let Some(bs) = image
        .records
        .iter()
        .find(|r| is_boot_sector_record(r.type_code))
    else {
        return Ok(None);
    };
    if (bs.body_len as usize) < 36 {
        return Ok(None);
    }
    let bpb = read_record_body(reader, bs, &image.cached_bodies)?;
    let n = bpb.len();
    if n < 36 {
        return Ok(None);
    }
    let byts_per_sec = u16::from_le_bytes([bpb[11], bpb[12]]) as u64;
    if !(byts_per_sec == 512
        || byts_per_sec == 1024
        || byts_per_sec == 2048
        || byts_per_sec == 4096)
    {
        return Ok(None);
    }
    let tot_sec_16 = u16::from_le_bytes([bpb[19], bpb[20]]) as u64;
    let tot_sec_32 = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]) as u64;
    let tot_sec = if tot_sec_16 != 0 {
        tot_sec_16
    } else {
        tot_sec_32
    };
    if tot_sec == 0 {
        return Ok(None);
    }
    Ok(Some(tot_sec * byts_per_sec))
}

/// Pick a name to feed to the FAT writer. Prefer the long filename, but
/// fall back to the 8.3 short name if the LFN fails validation (control
/// chars, illegal char, trailing space/dot, etc.).
fn choose_emit_name(entry: &GhoFileAwareEntry) -> Option<String> {
    use crate::fs::filesystem::FilesystemError;
    fn try_validate(s: &str) -> Result<(), FilesystemError> {
        // Cheap mirror of validate_fat_name. Kept here to avoid making
        // that function public from src/fs/fat.rs.
        if s.is_empty() {
            return Err(FilesystemError::InvalidData("empty".into()));
        }
        for c in s.chars() {
            if matches!(c, '"' | '*' | '/' | ':' | '<' | '>' | '?' | '\\' | '|') {
                return Err(FilesystemError::InvalidData(format!("bad char {c}")));
            }
            if (c as u32) < 0x20 {
                return Err(FilesystemError::InvalidData("control".into()));
            }
        }
        if s.ends_with(' ') || s.ends_with('.') {
            return Err(FilesystemError::InvalidData("trailing".into()));
        }
        Ok(())
    }
    if let Some(lfn) = entry.long_name.as_deref() {
        if try_validate(lfn).is_ok() {
            return Some(lfn.to_string());
        }
    }
    let short = &entry.short_name;
    if try_validate(short).is_ok() {
        return Some(short.clone());
    }
    None
}

// ---------------------------------------------------------------------------
// Streaming file-aware reader support (unifies with GhoReader::FileAware)
// ---------------------------------------------------------------------------

/// In-RAM representation of a reconstructed FAT image. Metadata
/// (BPB, FAT tables, directory clusters, FSInfo) is stored sector-
/// granularly in `sparse`; data-region clusters are resolved on demand
/// by looking up `cluster_to_file` and decompressing the appropriate
/// `0x0002` / `0x0102` records.
///
/// Peak memory for an 8 GB FAT32 reconstruction: ≈ tens of MB
/// (reserved + 2 × FAT + dir clusters + per-file metadata).
pub struct VirtualFatImage {
    pub bytes_per_sector: u32,
    pub sectors_per_cluster: u32,
    pub data_start_sector: u32,
    pub total_size: u64,
    pub cluster_size: u64,
    pub sparse: std::collections::HashMap<u64, Vec<u8>>,
    pub cluster_to_file: std::collections::HashMap<u32, (u32, u32)>,
    pub files: Vec<FileContentRef>,
}

#[derive(Debug, Clone)]
pub struct FileContentRef {
    pub records: Vec<(u64, u16)>,
    pub file_size: u64,
}

/// Sparse `Read + Write + Seek` over a virtual byte range. Storage is
/// sector-granular (one `Vec<u8>` per touched sector). Reads of
/// unwritten sectors return zero.
struct SparseSink {
    sectors: std::collections::HashMap<u64, Vec<u8>>,
    sector_size: u64,
    total_size: u64,
    pos: u64,
}

impl SparseSink {
    fn new(total_size: u64, sector_size: u64) -> Self {
        Self {
            sectors: std::collections::HashMap::new(),
            sector_size,
            total_size,
            pos: 0,
        }
    }
}

impl Read for SparseSink {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.pos >= self.total_size {
            return Ok(0);
        }
        let sector_lba = self.pos / self.sector_size;
        let off_in_sector = (self.pos % self.sector_size) as usize;
        let avail = (self.sector_size as usize) - off_in_sector;
        let remaining = (self.total_size - self.pos) as usize;
        let n = buf.len().min(avail).min(remaining);
        match self.sectors.get(&sector_lba) {
            Some(stored) => {
                buf[..n].copy_from_slice(&stored[off_in_sector..off_in_sector + n]);
            }
            None => {
                for b in &mut buf[..n] {
                    *b = 0;
                }
            }
        }
        self.pos += n as u64;
        Ok(n)
    }
}

impl Write for SparseSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let sector_lba = self.pos / self.sector_size;
        let off_in_sector = (self.pos % self.sector_size) as usize;
        let avail = (self.sector_size as usize) - off_in_sector;
        let n = buf.len().min(avail);
        let sector_size = self.sector_size as usize;
        let sector = self
            .sectors
            .entry(sector_lba)
            .or_insert_with(|| vec![0u8; sector_size]);
        sector[off_in_sector..off_in_sector + n].copy_from_slice(&buf[..n]);
        self.pos += n as u64;
        if self.pos > self.total_size {
            self.total_size = self.pos;
        }
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for SparseSink {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos: i128 = match pos {
            SeekFrom::Start(o) => o as i128,
            SeekFrom::Current(o) => self.pos as i128 + o as i128,
            SeekFrom::End(o) => self.total_size as i128 + o as i128,
        };
        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SparseSink seek before start",
            ));
        }
        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

/// Build the in-memory virtual FAT image used by
/// `GhoReaderMode::FileAware`.
fn build_virtual_fat_image(
    inner: &mut SpanReader,
    image: &GhoImage,
    tree: &GhoFileAwareTree,
    compression: GhoCompression,
) -> Result<VirtualFatImage> {
    use crate::fs::entry::FileEntry;
    use crate::fs::fat::{
        compute_fat_blank_layout, write_blank_fat_metadata_to_sink, FatFilesystem,
    };
    use crate::fs::filesystem::{
        CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
    };
    use std::collections::HashMap;

    let _ = compression;

    let size = match source_partition_size_from_boot(inner, image)? {
        Some(s) => s,
        None => bail!("file-aware GHO has no usable boot sector — can't size virtual image"),
    };
    if size < 64 * 1024 {
        bail!(
            "boot sector reports partition size {} bytes (< 64 KiB) — refusing to virtualise",
            size
        );
    }
    let layout = compute_fat_blank_layout(size)
        .with_context(|| format!("computing FAT layout for size {}", size))?;

    let bps = layout.bytes_per_sector as u64;
    let mut sink = SparseSink::new(size, bps);
    write_blank_fat_metadata_to_sink(&mut sink, &layout, None)
        .context("writing blank FAT metadata into sparse sink")?;

    let mut fs = FatFilesystem::open(&mut sink, 0).context("opening sparse FAT image for emit")?;

    let mut offset_to_len: HashMap<u64, u16> = HashMap::with_capacity(image.records.len());
    for rec in &image.records {
        if is_data_block_record(rec.type_code) {
            offset_to_len.insert(rec.body_start(), rec.body_len);
        }
    }

    let mut children_by_parent: HashMap<u32, Vec<usize>> = HashMap::new();
    for (idx, e) in tree.entries.iter().enumerate() {
        children_by_parent
            .entry(e.parent_cluster)
            .or_default()
            .push(idx);
    }

    let root = fs.root().map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut stack: Vec<(u32, FileEntry)> = vec![(tree.root_cluster, root)];

    let mut files: Vec<FileContentRef> = Vec::new();
    let mut cluster_to_file: HashMap<u32, (u32, u32)> = HashMap::new();

    while let Some((src_cluster, parent_entry)) = stack.pop() {
        let Some(child_idxs) = children_by_parent.get(&src_cluster) else {
            continue;
        };
        let child_idxs = child_idxs.clone();
        for idx in child_idxs {
            let entry = &tree.entries[idx];
            let Some(name) = choose_emit_name(entry) else {
                continue;
            };

            if entry.is_directory() {
                if let Ok(dir_entry) =
                    fs.create_directory(&parent_entry, &name, &CreateDirectoryOptions::default())
                {
                    stack.push((entry.source_cluster, dir_entry));
                }
            } else {
                let mut meta: Vec<(u64, u16)> =
                    Vec::with_capacity(entry.content_record_offsets.len());
                let mut missing = false;
                for &off in &entry.content_record_offsets {
                    match offset_to_len.get(&off) {
                        Some(&l) => meta.push((off, l)),
                        None => {
                            missing = true;
                            break;
                        }
                    }
                }
                if missing {
                    continue;
                }
                let file_size = entry.file_size as u64;
                let mut empty = std::io::empty();
                let opts = CreateFileOptions {
                    skip_data_write: true,
                    skip_name_checks: true,
                    skip_fsinfo_update: true,
                    ..Default::default()
                };
                let f = match fs.create_file(&parent_entry, &name, &mut empty, file_size, &opts) {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                let file_id = files.len() as u32;
                files.push(FileContentRef {
                    records: meta,
                    file_size,
                });

                let mut cluster_idx: u32 = 0;
                let mut c = f.location as u32;
                while (2..0x0FFF_FFF8).contains(&c) {
                    cluster_to_file.insert(c, (file_id, cluster_idx));
                    cluster_idx += 1;
                    match fs.next_cluster(c) {
                        Ok(Some(next)) => c = next,
                        _ => break,
                    }
                }
            }
        }
    }

    fs.sync_metadata().map_err(|e| anyhow::anyhow!("{e}"))?;
    drop(fs);

    Ok(VirtualFatImage {
        bytes_per_sector: layout.bytes_per_sector,
        sectors_per_cluster: layout.sectors_per_cluster,
        data_start_sector: (layout.data_start_byte() / bps) as u32,
        total_size: size,
        cluster_size: layout.cluster_size(),
        sparse: sink.sectors,
        cluster_to_file,
        files,
    })
}

/// Vec-returning convenience wrapper: useful for small images and
/// tests. For large partitions, prefer
/// [`emit_file_aware_fat_image_to_sink`] which streams the image to any
/// `Read + Write + Seek` (e.g. a tempfile) without holding the whole
/// thing in RAM.
pub fn emit_file_aware_fat_image<R: Read + Seek>(
    reader: &mut R,
    image: &GhoImage,
    tree: &GhoFileAwareTree,
    compression: GhoCompression,
) -> Result<EmitFileAwareResult> {
    let mut cur = std::io::Cursor::new(Vec::<u8>::new());
    let mut result = emit_file_aware_fat_image_to_sink(reader, image, tree, compression, &mut cur)?;
    result.image = cur.into_inner();
    Ok(result)
}

/// Reconstruct a mountable FAT image from a file-aware GHO and write it
/// to `sink`. The sink is rewound and re-used as the FAT backing store
/// (it must be a true `Read + Write + Seek`). Errors are fatal only for
/// I/O / format problems on the source; per-entry name or disk-full
/// failures are recorded in `EmitFileAwareResult::skipped` and
/// reconstruction continues.
pub fn emit_file_aware_fat_image_to_sink<R: Read + Seek, S: Read + Write + Seek + Send>(
    reader: &mut R,
    image: &GhoImage,
    tree: &GhoFileAwareTree,
    compression: GhoCompression,
    sink: &mut S,
) -> Result<EmitFileAwareResult> {
    use crate::fs::entry::FileEntry;
    use crate::fs::fat::{create_blank_fat, FatFilesystem};
    use crate::fs::filesystem::{
        CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
    };
    use std::collections::HashMap;

    // Decide output image size.
    let size = match source_partition_size_from_boot(reader, image)? {
        Some(s) => s,
        None => bail!("file-aware GHO has no usable boot sector — can't size output image"),
    };
    if size < 64 * 1024 {
        bail!(
            "boot sector reports partition size {} bytes (< 64 KiB) — refusing to format",
            size
        );
    }

    // Format a blank FAT in RAM (cheap: just BPB + zeroed FAT tables +
    // FSInfo, not the full partition), then stream it into the sink and
    // pad with zeros to the partition size.
    let blank = create_blank_fat(size, None).context("formatting blank FAT for emit")?;
    sink.seek(SeekFrom::Start(0))?;
    sink.write_all(&blank)?;
    if (blank.len() as u64) < size {
        // Pad with zeros to the full partition size so the FAT image is
        // sector-complete (some FAT writers won't tolerate a truncated
        // backing store when extending cluster chains).
        let mut remaining = size - blank.len() as u64;
        let chunk = vec![0u8; 1 << 20];
        while remaining > 0 {
            let n = remaining.min(chunk.len() as u64) as usize;
            sink.write_all(&chunk[..n])?;
            remaining -= n as u64;
        }
    }
    sink.flush()?;
    sink.seek(SeekFrom::Start(0))?;

    let mut fs = FatFilesystem::open(sink, 0).context("opening fresh FAT image for emit")?;

    // Build record-offset → body_len lookup. The tree only stores
    // body_start offsets; we need body_len to decode.
    let mut offset_to_len: HashMap<u64, u16> = HashMap::with_capacity(image.records.len());
    for rec in &image.records {
        if is_data_block_record(rec.type_code) {
            offset_to_len.insert(rec.body_start(), rec.body_len);
        }
    }

    // Index children by parent_cluster for DFS pre-order emission.
    let mut children_by_parent: HashMap<u32, Vec<usize>> = HashMap::new();
    for (idx, e) in tree.entries.iter().enumerate() {
        children_by_parent
            .entry(e.parent_cluster)
            .or_default()
            .push(idx);
    }

    let root = fs.root().map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut stack: Vec<(u32, FileEntry)> = Vec::new();
    stack.push((tree.root_cluster, root));

    let mut result = EmitFileAwareResult {
        image: Vec::new(),
        bytes_written: size,
        files_emitted: 0,
        dirs_emitted: 0,
        skipped: Vec::new(),
    };

    while let Some((src_cluster, parent_entry)) = stack.pop() {
        let Some(child_idxs) = children_by_parent.get(&src_cluster) else {
            continue;
        };
        let child_idxs = child_idxs.clone();
        for idx in child_idxs {
            let entry = &tree.entries[idx];
            let Some(name) = choose_emit_name(entry) else {
                result
                    .skipped
                    .push((entry.short_name.clone(), "name failed validation".into()));
                continue;
            };

            if entry.is_directory() {
                match fs.create_directory(&parent_entry, &name, &CreateDirectoryOptions::default())
                {
                    Ok(dir_entry) => {
                        result.dirs_emitted += 1;
                        stack.push((entry.source_cluster, dir_entry));
                    }
                    Err(e) => {
                        result
                            .skipped
                            .push((entry.short_name.clone(), format!("mkdir: {e}")));
                    }
                }
            } else {
                let mut meta: Vec<(u64, u16)> =
                    Vec::with_capacity(entry.content_record_offsets.len());
                let mut missing = false;
                for &off in &entry.content_record_offsets {
                    match offset_to_len.get(&off) {
                        Some(&l) => meta.push((off, l)),
                        None => {
                            missing = true;
                            break;
                        }
                    }
                }
                if missing {
                    result.skipped.push((
                        entry.short_name.clone(),
                        "content record offset not in image".into(),
                    ));
                    continue;
                }
                let file_size = entry.file_size as u64;
                let mut content = GhoFileContentReader::new(reader, meta, compression, file_size);
                match fs.create_file(
                    &parent_entry,
                    &name,
                    &mut content,
                    file_size,
                    &CreateFileOptions::default(),
                ) {
                    Ok(_) => result.files_emitted += 1,
                    Err(e) => result
                        .skipped
                        .push((entry.short_name.clone(), format!("create_file: {e}"))),
                }
            }
        }
    }

    fs.sync_metadata().map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(result)
}

/// Sector size used by the container header.
pub const GHO_SECTOR_SIZE: u64 = 512;

/// One inner record located by [`parse_gho_image`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GhoInnerRecord {
    pub offset: u64,
    pub type_code: u16,
    pub body_len: u16,
}

impl GhoInnerRecord {
    pub fn body_start(&self) -> u64 {
        self.offset + GHO_RECORD_HEADER_LEN as u64
    }
    pub fn body_end(&self) -> u64 {
        self.body_start() + self.body_len as u64
    }
}

/// Result of walking the inner record stream.
#[derive(Debug, Clone)]
pub struct GhoImage {
    pub records: Vec<GhoInnerRecord>,
    /// Pre-read record bodies, keyed by record offset. Populated
    /// during `parse_gho_image` for dir entries, boot sectors, and
    /// checksums so that `walk_file_aware_tree` and `split_partitions`
    /// can avoid re-seeking into the file.
    pub cached_bodies: std::collections::HashMap<u64, Vec<u8>>,
    /// Count of `0x0017` boot-sector records — proxy for partition count.
    pub partition_count: usize,
}

impl GhoImage {
    /// All data-block records (type `0x0002`) in stream order.
    pub fn data_blocks(&self) -> impl Iterator<Item = &GhoInnerRecord> {
        self.records
            .iter()
            .filter(|r| r.type_code == GHO_REC_DATA_BLOCK)
    }
}

/// Walk the inner record stream. Starts scanning past the container
/// header (sector 0); uses [`find_inner_stream_start`] to locate the
/// first record so 7.5 vs 11.5 padding differences are absorbed.
///
/// Each record advances the cursor by `HEADER + body_len`, since records
/// in this dialect are back-to-back (no scan-forward needed between them).
/// If a record's `body_len` walks us into garbage, we scan forward for
/// the next valid magic — defensive against minor corruption.
pub fn parse_gho_image<R: Read + Seek>(
    reader: &mut R,
    file_size: u64,
    header: &GhoContainerHeader,
) -> Result<GhoImage> {
    let header_end = if header.password_protected {
        (GHO_HEADER_PREFIX_LEN + GHO_PASSWORD_VERIFIER_LEN) as u64
    } else {
        GHO_HEADER_PREFIX_LEN as u64
    };
    let mut offset = match find_inner_stream_start(reader, header_end) {
        Ok(o) => o,
        Err(_) => {
            return Ok(GhoImage {
                records: Vec::new(),
                cached_bodies: std::collections::HashMap::new(),
                partition_count: 0,
            })
        }
    };

    let mut records = Vec::new();
    let mut cached_bodies = std::collections::HashMap::new();
    let mut partition_count = 0;
    while offset + GHO_RECORD_HEADER_LEN as u64 <= file_size {
        reader.seek(SeekFrom::Start(offset))?;
        let rec = match GhoRecordHeader::read_from(reader) {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(_) => {
                // Bad magic — try to recover by scanning forward to next
                // record magic. If none, we're done.
                match find_inner_stream_start(reader, offset + 1) {
                    Ok(o) if o > offset && o < file_size => {
                        offset = o;
                        continue;
                    }
                    _ => break,
                }
            }
        };
        let inner = GhoInnerRecord {
            offset,
            type_code: rec.type_code,
            body_len: rec.body_len,
        };
        if is_boot_sector_record(inner.type_code) && inner.body_len == 512 {
            partition_count += 1;
        }

        // Eagerly read small record bodies that walk_file_aware_tree
        // and split_partitions will need later. The reader is already
        // positioned at body_start (right after the 10-byte header),
        // so this is free sequential I/O — no extra seeks.
        let should_cache = is_boot_sector_record(inner.type_code)
            || is_dir_entry_record(inner.type_code)
            || is_checksum_record(inner.type_code);
        if should_cache && inner.body_len > 0 {
            let mut body = vec![0u8; inner.body_len as usize];
            if reader.read_exact(&mut body).is_ok() {
                cached_bodies.insert(inner.offset, body);
            }
        }

        records.push(inner);
        offset = inner.body_end();
    }
    Ok(GhoImage {
        records,
        cached_bodies,
        partition_count,
    })
}

/// One partition's slice of the inner record stream. Produced by
/// [`split_partitions`] for multi-partition file-aware GHOs.
#[derive(Debug, Clone)]
pub struct GhoPartitionSlice {
    /// Index into `GhoImage.records` of this partition's boot-sector record.
    pub boot_record_index: usize,
    /// Inclusive start and exclusive end into `GhoImage.records`.
    pub record_range: std::ops::Range<usize>,
    /// Partition size in bytes (from VBR `total_sectors × bytes_per_sector`).
    pub partition_size: u64,
    /// VBR `hidden_sectors` — the partition's LBA offset on the source disk.
    pub hidden_sectors: u64,
    /// FAT type string: `"FAT32"`, `"FAT16"`, `"FAT12"`, or `"Unknown"`.
    pub fs_type: String,
    /// Index of the `0x0118` reserved-sectors record, if present.
    pub reserved_record_index: Option<usize>,
}

/// Read a VBR from a boot-sector record body and extract geometry.
fn parse_vbr_geometry<R: Read + Seek>(
    reader: &mut R,
    rec: &GhoInnerRecord,
    cache: &std::collections::HashMap<u64, Vec<u8>>,
) -> Result<(u64, u64, String)> {
    if (rec.body_len as usize) < 64 {
        bail!("boot-sector body too short: {} bytes", rec.body_len);
    }
    let bpb_vec = read_record_body(reader, rec, cache)?;
    let bpb = &bpb_vec;
    let n = bpb.len();
    let byts_per_sec = u16::from_le_bytes([bpb[11], bpb[12]]) as u64;
    if byts_per_sec == 0 || byts_per_sec > 4096 {
        bail!("invalid bytes_per_sector: {}", byts_per_sec);
    }
    let hidden = u32::from_le_bytes([bpb[28], bpb[29], bpb[30], bpb[31]]) as u64;
    let tot16 = u16::from_le_bytes([bpb[19], bpb[20]]) as u64;
    let tot32 = u32::from_le_bytes([bpb[32], bpb[33], bpb[34], bpb[35]]) as u64;
    let total_sectors = if tot16 != 0 { tot16 } else { tot32 };
    let size = total_sectors * byts_per_sec;

    let fs_type = if n >= 90 && &bpb[82..87] == b"FAT32" {
        "FAT32".to_string()
    } else if n >= 62 && &bpb[54..59] == b"FAT16" {
        "FAT16".to_string()
    } else if n >= 62 && &bpb[54..59] == b"FAT12" {
        "FAT12".to_string()
    } else {
        "Unknown".to_string()
    };
    Ok((hidden, size, fs_type))
}

/// Split a multi-partition file-aware `GhoImage` into per-partition
/// slices. Each slice owns a range of records and carries the VBR
/// geometry needed for disk reconstruction.
///
/// The partition boundary pattern is a pair of `0x0117` records: the
/// first repeats the previous partition's VBR, the second carries the
/// new partition's VBR. We skip the "repeat" and use the second as the
/// new partition's boot record.
pub fn split_partitions<R: Read + Seek>(
    reader: &mut R,
    image: &GhoImage,
) -> Result<Vec<GhoPartitionSlice>> {
    let mut slices = Vec::new();
    let records = &image.records;
    if records.is_empty() {
        return Ok(slices);
    }

    // Identify boot-sector record indices (each marks a potential
    // partition start). We skip "repeat" VBRs by deduplicating
    // consecutive 0x0117 pairs: when two 0x0117 records are adjacent,
    // the first is a repeat and the second is the real new partition.
    let mut boot_indices: Vec<usize> = Vec::new();
    let mut i = 0;
    while i < records.len() {
        let rec = &records[i];
        if is_boot_sector_record(rec.type_code) && rec.body_len == 512 {
            // Check for 0x0117-pair: if next record is also a boot
            // sector, this one is a repeat — skip it.
            if i + 1 < records.len()
                && is_boot_sector_record(records[i + 1].type_code)
                && records[i + 1].body_len == 512
            {
                i += 1; // skip the repeat, loop will process the real one
                continue;
            }
            // Footer detection: a lone 0x0117 (not the first boot
            // record) that ISN'T followed by partition data (dir
            // entries, data blocks) is the "last VBR copy" footer.
            if rec.type_code == GHO_REC_BOOT_SECTOR_PARTITION {
                if i + 1 >= records.len() {
                    i += 1;
                    continue;
                }
                let next = records[i + 1].type_code;
                if !is_dir_entry_record(next) && !is_data_block_record(next) {
                    i += 1;
                    continue;
                }
            }
            boot_indices.push(i);
        }
        i += 1;
    }

    if boot_indices.is_empty() {
        return Ok(slices);
    }

    for (pi, &boot_idx) in boot_indices.iter().enumerate() {
        let rec = &records[boot_idx];
        let (hidden, size, fs_type) = match parse_vbr_geometry(reader, rec, &image.cached_bodies) {
            Ok(g) => g,
            Err(_) => continue,
        };

        // Record range: from boot_idx to the next partition's boot_idx
        // (exclusive), or to the first footer record (0x0005/0x0023),
        // or to end of records.
        let range_end = if pi + 1 < boot_indices.len() {
            // End just before the 0x0117 PAIR that starts the next
            // partition. The pair starts one record before the next
            // boot_idx (since we skipped the repeat).
            let next_boot = boot_indices[pi + 1];
            if next_boot > 0
                && is_boot_sector_record(records[next_boot - 1].type_code)
                && records[next_boot - 1].body_len == 512
            {
                next_boot - 1
            } else {
                next_boot
            }
        } else {
            // Last partition: end before footer records.
            let mut end = records.len();
            for j in boot_idx..records.len() {
                let t = records[j].type_code;
                if t == GHO_REC_FILE_CATALOG || t == GHO_REC_END {
                    end = j;
                    break;
                }
                // A lone 0x0117 after the last partition's data is
                // the "last VBR copy" footer — stop before it.
                if j > boot_idx
                    && records[j].type_code == GHO_REC_BOOT_SECTOR_PARTITION
                    && records[j].body_len == 512
                {
                    let after = if j + 1 < records.len() {
                        records[j + 1].type_code
                    } else {
                        0
                    };
                    if after == GHO_REC_FILE_CATALOG
                        || after == GHO_REC_END
                        || after == GHO_REC_BOOT_SECTOR_PARTITION
                    {
                        end = j;
                        break;
                    }
                }
            }
            end
        };

        // Look for a 0x0118 reserved-sectors record in this slice.
        let reserved_record_index =
            (boot_idx..range_end).find(|&j| records[j].type_code == GHO_REC_RESERVED_SECTORS);

        slices.push(GhoPartitionSlice {
            boot_record_index: boot_idx,
            record_range: boot_idx..range_end,
            partition_size: size,
            hidden_sectors: hidden,
            fs_type,
            reserved_record_index,
        });
    }

    Ok(slices)
}

/// Resolve absolute disk LBA offsets for a set of partition slices.
/// The first partition's `hidden_sectors` is treated as absolute.
/// Subsequent partitions that overlap with the first partition's
/// range are treated as relative (logical partitions inside an
/// extended container) and their absolute offset is computed as
/// `prev_partition_end + hidden_sectors`.
fn resolve_absolute_offsets(slices: &mut [GhoPartitionSlice]) {
    if slices.is_empty() {
        return;
    }
    let p0_end = slices[0].hidden_sectors + slices[0].partition_size / 512;
    let mut prev_end = p0_end;
    for slice in &mut slices[1..] {
        let hidden = slice.hidden_sectors;
        let sectors = slice.partition_size / 512;
        if hidden < p0_end && hidden + sectors <= p0_end {
            let abs = prev_end + hidden;
            slice.hidden_sectors = abs;
            prev_end = abs + sectors;
        } else {
            prev_end = hidden + sectors;
        }
    }
}

/// MBR partition type byte for FAT32 (LBA).
const MBR_TYPE_FAT32_LBA: u8 = 0x0C;
/// MBR partition type byte for FAT16 (LBA, > 32 MB).
const MBR_TYPE_FAT16_LBA: u8 = 0x0E;
/// MBR partition type byte for extended (LBA).
const MBR_TYPE_EXTENDED_LBA: u8 = 0x0F;

/// Synthesize an MBR sector from a list of partitions discovered in
/// a multi-partition GHO. Returns 512 bytes with a valid partition
/// table and `0x55AA` signature.
///
/// The function handles extended partitions: if any partition's
/// `hidden_sectors` indicates it lives inside another partition's
/// range, it builds an extended container with EBR chain.
fn synthesize_mbr(slices: &[GhoPartitionSlice]) -> [u8; 512] {
    let mut mbr = [0u8; 512];
    mbr[510] = 0x55;
    mbr[511] = 0xAA;

    if slices.is_empty() {
        return mbr;
    }

    // Sort partitions by hidden_sectors (disk LBA order).
    let mut parts: Vec<(u64, u64, u8)> = slices
        .iter()
        .map(|s| {
            let type_byte = match s.fs_type.as_str() {
                "FAT32" => MBR_TYPE_FAT32_LBA,
                "FAT16" => MBR_TYPE_FAT16_LBA,
                _ => MBR_TYPE_FAT32_LBA,
            };
            let sectors = s.partition_size / 512;
            (s.hidden_sectors, sectors, type_byte)
        })
        .collect();
    parts.sort_by_key(|p| p.0);

    // Detect extended partitions: a partition whose start_lba falls
    // inside the range of the gap between the first partition's end
    // and the disk's end, with hidden_sectors suggesting an EBR offset
    // (hidden == container_start + 63 or similar).
    //
    // Heuristic: the first partition is primary. If there are multiple
    // subsequent partitions, they're in an extended container. The
    // extended container starts right after the first partition.
    if parts.len() == 1 {
        write_mbr_entry(&mut mbr, 0, parts[0].0, parts[0].1, parts[0].2, true);
    } else {
        // First partition is primary.
        write_mbr_entry(&mut mbr, 0, parts[0].0, parts[0].1, parts[0].2, true);

        // Extended partition contains all remaining partitions.
        // Extended start = first partition end.
        let ext_start = parts[0].0 + parts[0].1;
        let ext_end = parts.last().map(|p| p.0 + p.1).unwrap_or(ext_start);
        let ext_sectors = ext_end - ext_start;

        write_mbr_entry(
            &mut mbr,
            1,
            ext_start,
            ext_sectors,
            MBR_TYPE_EXTENDED_LBA,
            false,
        );
    }

    mbr
}

/// Write one 16-byte MBR partition entry at slot `idx` (0..3).
fn write_mbr_entry(
    mbr: &mut [u8; 512],
    idx: usize,
    start_lba: u64,
    sectors: u64,
    ptype: u8,
    active: bool,
) {
    let off = 446 + idx * 16;
    mbr[off] = if active { 0x80 } else { 0x00 };
    // CHS fields: use LBA-only mode (FE FF FF).
    mbr[off + 1] = 0xFE;
    mbr[off + 2] = 0xFF;
    mbr[off + 3] = 0xFF;
    mbr[off + 4] = ptype;
    mbr[off + 5] = 0xFE;
    mbr[off + 6] = 0xFF;
    mbr[off + 7] = 0xFF;
    mbr[off + 8..off + 12].copy_from_slice(&(start_lba as u32).to_le_bytes());
    mbr[off + 12..off + 16].copy_from_slice(&(sectors as u32).to_le_bytes());
}

/// Synthesize Extended Boot Records (EBRs) for logical partitions
/// inside the extended container. Returns a map of `(disk_lba ->
/// [u8; 512])` for each EBR sector.
fn synthesize_ebrs(
    slices: &[GhoPartitionSlice],
    ext_start: u64,
) -> std::collections::HashMap<u64, [u8; 512]> {
    let mut ebrs = std::collections::HashMap::new();
    // Sort logical partitions by disk LBA.
    let mut logicals: Vec<&GhoPartitionSlice> = slices.iter().collect();
    logicals.sort_by_key(|s| s.hidden_sectors);

    for (i, part) in logicals.iter().enumerate() {
        // EBR lives 63 sectors before the partition's VBR.
        let ebr_lba = part.hidden_sectors - 63;
        let mut ebr = [0u8; 512];
        ebr[510] = 0x55;
        ebr[511] = 0xAA;

        // Entry 0: the logical partition (offset relative to this EBR).
        let type_byte = match part.fs_type.as_str() {
            "FAT32" => MBR_TYPE_FAT32_LBA,
            "FAT16" => MBR_TYPE_FAT16_LBA,
            _ => MBR_TYPE_FAT32_LBA,
        };
        let part_sectors = part.partition_size / 512;
        // Offset from EBR to partition start = 63 sectors.
        let entry0_off = 446;
        ebr[entry0_off + 4] = type_byte;
        ebr[entry0_off + 1] = 0xFE;
        ebr[entry0_off + 2] = 0xFF;
        ebr[entry0_off + 3] = 0xFF;
        ebr[entry0_off + 5] = 0xFE;
        ebr[entry0_off + 6] = 0xFF;
        ebr[entry0_off + 7] = 0xFF;
        ebr[entry0_off + 8..entry0_off + 12].copy_from_slice(&63u32.to_le_bytes());
        ebr[entry0_off + 12..entry0_off + 16].copy_from_slice(&(part_sectors as u32).to_le_bytes());

        // Entry 1: chain to next EBR (relative to ext_start).
        if i + 1 < logicals.len() {
            let next = logicals[i + 1];
            let next_ebr_lba = next.hidden_sectors - 63;
            let next_part_sectors = next.partition_size / 512;
            let entry1_off = 446 + 16;
            ebr[entry1_off + 4] = MBR_TYPE_EXTENDED_LBA;
            ebr[entry1_off + 1] = 0xFE;
            ebr[entry1_off + 2] = 0xFF;
            ebr[entry1_off + 3] = 0xFF;
            ebr[entry1_off + 5] = 0xFE;
            ebr[entry1_off + 6] = 0xFF;
            ebr[entry1_off + 7] = 0xFF;
            let offset_from_ext = (next_ebr_lba - ext_start) as u32;
            let chain_size = (63 + next_part_sectors) as u32;
            ebr[entry1_off + 8..entry1_off + 12].copy_from_slice(&offset_from_ext.to_le_bytes());
            ebr[entry1_off + 12..entry1_off + 16].copy_from_slice(&chain_size.to_le_bytes());
        }

        ebrs.insert(ebr_lba, ebr);
    }
    ebrs
}

/// Decode every `0x0002` data-block record in `image` into `writer`,
/// applying the container's compression. Returns total bytes written.
pub fn decode_data_blocks_to<R: Read + Seek, W: Write>(
    reader: &mut R,
    image: &GhoImage,
    compression: GhoCompression,
    writer: &mut W,
) -> Result<u64> {
    let mut total: u64 = 0;
    let mut decoded = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
    let mut body_buf = vec![0u8; u16::MAX as usize];

    for rec in image.data_blocks() {
        let body_len = rec.body_len as usize;
        if body_len == 0 {
            continue;
        }
        reader.seek(SeekFrom::Start(rec.body_start()))?;
        let body = &mut body_buf[..body_len];
        reader
            .read_exact(body)
            .with_context(|| format!("reading 0x0002 body at offset {:#x}", rec.offset))?;

        let n = match compression {
            GhoCompression::None => {
                // Uncompressed: body IS the raw 32 KiB block. Write verbatim.
                writer.write_all(body)?;
                total += body_len as u64;
                continue;
            }
            GhoCompression::Fast => fast_lz_decompress(body, body_len, &mut decoded)
                .with_context(|| format!("Fast-LZ decode at offset {:#x}", rec.offset))?,
            GhoCompression::High => zlib_decode_block(body, &mut decoded)
                .with_context(|| format!("zlib decode at offset {:#x}", rec.offset))?,
            GhoCompression::Other(b) => bail!("unsupported GHO compression byte {:#04x}", b),
        };
        writer
            .write_all(&decoded[..n])
            .with_context(|| format!("writing decoded block at output offset {}", total))?;
        total += n as u64;
    }
    Ok(total)
}

fn zlib_decode_block(block: &[u8], dst: &mut [u8]) -> Result<usize> {
    use flate2::read::ZlibDecoder;
    // Unlike Fast-LZ (which has a 4-byte prefix), zlib/High data blocks
    // contain raw zlib directly — no prefix to skip.  Confirmed on both
    // Ghost 7.5 and 11.5 corpora (all 0x0002 bodies start with 0x78).
    if block.is_empty() {
        return Ok(0);
    }
    let mut dec = ZlibDecoder::new(block);
    let mut n = 0;
    loop {
        if n >= dst.len() {
            bail!("zlib block decoded > {} bytes", dst.len());
        }
        match dec.read(&mut dst[n..]) {
            Ok(0) => return Ok(n),
            Ok(k) => n += k,
            Err(e) => return Err(anyhow::Error::from(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// NTFS file-aware GHO support
// ---------------------------------------------------------------------------
//
// NTFS file-aware Ghost backups use a completely different internal format
// from FAT file-aware ones. Instead of the 0x012F18D8 record stream, they
// store:
//   1. GHPR metadata blocks containing the NTFS VBR
//   2. Packed cluster runs — each preceded by a 42-byte header containing
//      the cluster count, with MFT FILE records inline between runs
//      providing the LCN mapping
//   3. A single 0x0023 End record at the very end
//
// Detection: if `find_inner_stream_start` fails for a FileAware GHO, we
// scan for the NTFS VBR signature in the header region. If found, we take
// the NTFS path.

/// One compressed zlib block in the NTFS file-aware compressed stream.
#[derive(Debug, Clone)]
struct NtfsCompressedBlock {
    file_offset: u64,
    comp_size: u32,
    decomp_offset: u64,
    decomp_size: u32,
}

/// State for on-demand decompression of compressed NTFS file-aware GHOs.
/// The decompressed stream is structurally identical to the uncompressed
/// format (same run headers, MFT records, cluster data) — each zlib block
/// decompresses to 10–32768 bytes. Blocks are separated by 2-byte gaps.
struct NtfsCompressedState {
    blocks: Vec<NtfsCompressedBlock>,
    compression: GhoCompression,
    total_decompressed: u64,
    cache_block: Option<usize>,
    cache_buf: Vec<u8>,
    /// Lazy scanning state: where the next forward scan should resume
    /// in the compressed file. `None` = fully scanned.
    lazy_scan: Option<NtfsLazyScanState>,
    /// Identity of the source archive for the process-wide scan cache. When
    /// set, the completed run index + block table are stored under this key so
    /// a later re-open of the same archive skips the rescan. `None` disables
    /// caching (e.g. file metadata couldn't be read at open time).
    cache_key: Option<NtfsScanKey>,
}

/// State preserved between incremental forward scans of a compressed
/// NTFS file-aware GHO. Allows `read_ntfs_file_aware_into` to extend
/// the run index on demand when an unmapped LCN is accessed.
struct NtfsLazyScanState {
    /// File offset in the SpanReader where the next compressed block
    /// should be read from.
    file_offset: u64,
    /// Decompressed stream offset corresponding to `file_offset`.
    decomp_offset: u64,
    /// Total file size (end of scan region).
    file_size: u64,
    /// VBR for inline MFT record parsing.
    vbr: crate::fs::ntfs::NtfsVbr,
    /// Cluster size (bytes).
    cluster_size: u64,
    /// Pending inline-MFT data runs not yet matched to run headers.
    pending_lcns: Vec<(u64, u64)>,
    /// Index into `pending_lcns` where the most recently parsed gap's entries
    /// begin. Stream runs are matched against this freshest group first (in
    /// forward order); see `match_inline_run`.
    pending_group_start: usize,
    /// Data-run queue from the MFT (Phase 1), consumed during matching.
    data_run_queue: Vec<(u64, u64)>,
    /// Absolute decompressed offset where the previous run's data ends.
    prev_data_end: u64,
    /// Rolling decompressed stream buffer for gap/header parsing.
    stream: Vec<u8>,
    /// Base offset of `stream[0]` in the decompressed address space.
    stream_base: u64,
    /// Remaining decompressed bytes of cluster data to skip before
    /// the next gap/run-header region.
    skip_data_bytes: u64,
    /// Decompressed offset up to which we have already searched for run
    /// headers and found none. Avoids re-scanning the same bytes on every
    /// subsequent block (which would be O(N^2)). Always >= `prev_data_end`.
    search_resume: u64,
    /// LCN starts already assigned to a run. Ghost re-declares some files'
    /// runs both inline and in the MFT queue, so a stream run can otherwise
    /// match an already-used `(lcn, cc)` and land two files on the same
    /// clusters -> cross-links in chkdsk. Used to skip already-assigned LCNs.
    assigned_lcns: std::collections::HashSet<u64>,
}

// ---------------------------------------------------------------------------
// NTFS file-aware lazy-scan cache
//
// Building the run index for a compressed NTFS file-aware GHO means scanning
// the whole compressed stream forward (decompressing every block) to find each
// file's cluster runs — `extend_ntfs_compressed_index`. The GUI opens a fresh
// `GhoReader` for nearly every browse/extract operation (see the MEMORY.md note
// about not caching state on the filesystem struct), so without a cache that
// full scan is paid again on each extract. This process-wide cache stores the
// completed scan keyed by the archive's file identity and reuses it across
// reader instances.
// ---------------------------------------------------------------------------

/// Identity of a GHO span set for the lazy-scan cache: each span file's path,
/// length, and modification time. A change to any span file (re-backup,
/// truncation) yields a different key, so a stale scan is never reused.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct NtfsScanKey(Vec<(PathBuf, u64, u128)>);

impl NtfsScanKey {
    /// Build a key from the span set, or `None` if any file's metadata
    /// (length / mtime) can't be read — in which case caching is skipped.
    fn from_span_set(span_set: &[PathBuf]) -> Option<Self> {
        let mut parts = Vec::with_capacity(span_set.len());
        for p in span_set {
            let meta = std::fs::metadata(p).ok()?;
            let mtime = meta
                .modified()
                .ok()?
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_nanos();
            parts.push((p.clone(), meta.len(), mtime));
        }
        Some(NtfsScanKey(parts))
    }
}

/// A completed NTFS file-aware lazy scan, cached in memory so re-opening the
/// same archive reuses the run index + decompressed-block table instead of
/// re-scanning the whole compressed stream.
struct CachedNtfsScan {
    runs: Vec<NtfsGhoClusterRun>,
    blocks: Vec<NtfsCompressedBlock>,
    compression: GhoCompression,
    total_decompressed: u64,
    volume_size: u64,
    cluster_size: u64,
    vbr: [u8; 512],
    bitmap_runs: Vec<(u64, u64)>,
    synth_bitmap: Vec<u8>,
    boot_code: Vec<u8>,
}

/// Max archives kept in the scan cache. Each entry can hold a few MB of run /
/// block tables, and users rarely juggle more than one or two GHOs at once;
/// eviction is FIFO.
const NTFS_SCAN_CACHE_CAP: usize = 4;

#[derive(Default)]
struct NtfsScanCacheInner {
    map: std::collections::HashMap<NtfsScanKey, Arc<CachedNtfsScan>>,
    /// FIFO insertion order for eviction.
    order: std::collections::VecDeque<NtfsScanKey>,
}

fn ntfs_scan_cache() -> &'static Mutex<NtfsScanCacheInner> {
    static CACHE: OnceLock<Mutex<NtfsScanCacheInner>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(NtfsScanCacheInner::default()))
}

fn ntfs_scan_cache_get(key: &NtfsScanKey) -> Option<Arc<CachedNtfsScan>> {
    let cache = ntfs_scan_cache().lock().ok()?;
    cache.map.get(key).cloned()
}

/// Snapshot the (now fully-scanned) index + compressed state and store it under
/// `key`, replacing any prior entry. Cheap to call repeatedly: later calls just
/// refresh the entry as `$Bitmap` / `$Boot` get filled in.
fn ntfs_scan_cache_store(key: &NtfsScanKey, index: &NtfsGhoIndex, comp: &NtfsCompressedState) {
    let snap = Arc::new(CachedNtfsScan {
        runs: index.runs.clone(),
        blocks: comp.blocks.clone(),
        compression: comp.compression,
        total_decompressed: comp.total_decompressed,
        volume_size: index.volume_size,
        cluster_size: index.cluster_size,
        vbr: index.vbr,
        bitmap_runs: index.bitmap_runs.clone(),
        synth_bitmap: index.synth_bitmap.clone(),
        boot_code: index.boot_code.clone(),
    });
    let Ok(mut cache) = ntfs_scan_cache().lock() else {
        return;
    };
    if cache.map.insert(key.clone(), snap).is_none() {
        // New key: record order and evict the oldest if over capacity.
        cache.order.push_back(key.clone());
        while cache.order.len() > NTFS_SCAN_CACHE_CAP {
            if let Some(old) = cache.order.pop_front() {
                cache.map.remove(&old);
            }
        }
    }
    log::info!(
        "Cached NTFS lazy-scan result: {} runs, {} blocks ({} archive(s) cached)",
        index.runs.len(),
        comp.blocks.len(),
        cache.map.len(),
    );
}

/// Reconstruct a fully-scanned `NtfsFileAware` reader mode from a cached
/// snapshot, skipping the MFT decompression and the forward scan entirely.
fn ntfs_mode_from_cache(snap: &CachedNtfsScan, key: NtfsScanKey) -> (GhoReaderMode, u64) {
    let index = NtfsGhoIndex {
        runs: snap.runs.clone(),
        volume_size: snap.volume_size,
        cluster_size: snap.cluster_size,
        vbr: snap.vbr,
        bitmap_runs: snap.bitmap_runs.clone(),
        synth_bitmap: snap.synth_bitmap.clone(),
        boot_code: snap.boot_code.clone(),
    };
    let comp = NtfsCompressedState {
        blocks: snap.blocks.clone(),
        compression: snap.compression,
        total_decompressed: snap.total_decompressed,
        cache_block: None,
        cache_buf: Vec::new(),
        lazy_scan: None,
        cache_key: Some(key),
    };
    let served_size = ntfs_served_size(&index);
    (
        GhoReaderMode::NtfsFileAware {
            index,
            last_run_hint: 0,
            compressed: Some(comp),
        },
        served_size,
    )
}

/// 16-byte constant prefix appearing at the start of an NTFS file-aware
/// MFT header (the first run only). Subsequent runs use [`NTFS_RUN_ENTRY_B`]
/// directly without this entry A prefix.
const NTFS_RUN_HEADER_ENTRY1: [u8; 16] = [
    0x0E, 0x20, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F,
];

/// Run-header entry B layout (16 bytes):
///   byte 0:      0x0E (entry marker)
///   bytes 1-6:   0x02 0x00 0x00 0x00 0x00 0x00 (fixed)
///   bytes 7-14:  u64-LE run sequence number (0 = $MFT, 1+ subsequent)
///   byte 15:     0x0F (terminator)
/// Only the fixed bytes (0-6, 15) are matched; the seq field is read out
/// separately by `parse_ntfs_run_b_header`.
fn match_run_entry_b(window: &[u8]) -> bool {
    if window.len() < 16 {
        return false;
    }
    window[0] == 0x0E
        && window[1] == 0x02
        && window[2] == 0x00
        && window[3] == 0x00
        && window[4] == 0x00
        && window[5] == 0x00
        && window[6] == 0x00
        && window[15] == 0x0F
}

/// Size of the 3-entry MFT header (entry A + entry B + entry C).
const NTFS_RUN_HEADER_SIZE: usize = 42;

/// Size of a non-MFT run header (entry B + entry C only).
const NTFS_RUN_B_HEADER_SIZE: usize = 26;

/// One cluster run in the NTFS file-aware index: maps a contiguous range
/// of LCNs to a file offset in the GHO.
#[derive(Debug, Clone)]
struct NtfsGhoClusterRun {
    lcn_start: u64,
    cluster_count: u64,
    file_offset: u64,
    /// Run header bytes 7-14 carry a u64 sequence number: the fragment index
    /// within an attribute (0 = first fragment of a new attribute; 1+ =
    /// continuation). Single-fragment attributes are always seq=0. Captured
    /// during the lazy scan and used by [`fixup_ntfs_typed_misalignments`] to
    /// deterministically map stream runs to MFT attributes — when an
    /// attribute has multiple fragments, the seq chain uniquely identifies
    /// the attribute (the cc sequence is unique among multi-fragment attrs).
    /// `u64::MAX` for entries that pre-date the seq capture (e.g. the
    /// explicitly-registered MFT first run added at scan start).
    seq: u64,
}

/// Index built during the open scan. Sorted by `lcn_start` for binary
/// search during reads.
struct NtfsGhoIndex {
    runs: Vec<NtfsGhoClusterRun>,
    volume_size: u64,
    cluster_size: u64,
    #[allow(dead_code)]
    vbr: [u8; 512],
    /// Synthesized `$Bitmap`. Ghost omits `$Bitmap` content from file-aware
    /// backups (it's derivable from the MFT allocation and Ghost's restorer
    /// regenerates it), so the whole-disk stream must serve a synthesized one
    /// or a strict NTFS driver sees an all-free/corrupt volume and won't mount.
    /// `bitmap_runs` are `$Bitmap`'s own `$DATA` LCN runs (where to serve it);
    /// `synth_bitmap` is one bit per cluster, LSB-first, set = allocated.
    bitmap_runs: Vec<(u64, u64)>,
    synth_bitmap: Vec<u8>,
    /// `$Boot`'s first 16 sectors (8 KiB), recovered from the stream by
    /// content-matching the VBR (the read path otherwise synthesizes only the
    /// VBR for the boot region and zeros sectors 1-15). Empty until
    /// `prepare_full_image` runs (export only) or if no match is found; the read
    /// path then falls back to VBR + zeros. Serving the real boot code (sectors
    /// 1-15) is what makes the exported disk bootable.
    boot_code: Vec<u8>,
}

/// Build a synthesized NTFS `$Bitmap` from the MFT-declared allocation.
///
/// `allocation` is every `(lcn, cluster_count)` the MFT marks in use (the
/// `data_run_queue` plus the `$MFT` run itself). Returns the bitmap bytes sized
/// to cover the whole volume (`ceil(total_clusters / 8)`), one bit per cluster,
/// LSB-first, set = allocated. Bits for clusters past the volume end (padding in
/// the final byte) are set to 1 per NTFS convention.
fn build_synth_ntfs_bitmap(allocation: &[(u64, u64)], total_clusters: u64) -> Vec<u8> {
    let nbytes = (total_clusters as usize).div_ceil(8);
    let mut bm = vec![0u8; nbytes];
    for &(lcn, len) in allocation {
        let end = lcn.saturating_add(len).min(total_clusters);
        for c in lcn..end {
            bm[(c / 8) as usize] |= 1 << (c % 8);
        }
    }
    // Clusters past the volume end occupy the slack bits of the final byte;
    // NTFS marks them allocated so nothing tries to use them.
    for c in total_clusters..(nbytes as u64 * 8) {
        bm[(c / 8) as usize] |= 1 << (c % 8);
    }
    bm
}
/// Parse inline MFT records from a buffer (gap between cluster runs).
/// Same logic as `parse_inline_mft_records` but works on an in-memory
/// slice instead of a reader.
fn parse_inline_mft_from_buf(
    buf: &[u8],
    vbr: &crate::fs::ntfs::NtfsVbr,
    out: &mut Vec<(u64, u64)>,
) {
    use crate::fs::ntfs::{apply_fixup, parse_mft_attributes};
    let rec_size = vbr.mft_record_size as usize;
    if buf.len() < rec_size + 4 {
        return;
    }
    let mut pos = 0;
    while pos + rec_size <= buf.len() {
        if let Some(idx) = buf[pos..].windows(4).position(|w| w == b"FILE") {
            let rec_off = pos + idx;
            if rec_off + rec_size > buf.len() {
                break;
            }
            let rec_num = u32::from_le_bytes([
                buf[rec_off + 44],
                buf[rec_off + 45],
                buf[rec_off + 46],
                buf[rec_off + 47],
            ]);
            if rec_num == 8 {
                pos = rec_off + rec_size;
                continue;
            }
            let mut rec = buf[rec_off..rec_off + rec_size].to_vec();
            let _ = apply_fixup(&mut rec, vbr.bytes_per_sector);
            let attrs = parse_mft_attributes(&rec, vbr.mft_record_size);
            for attr in &attrs {
                if !attr.resident && !attr.data_runs.is_empty() {
                    for dr in &attr.data_runs {
                        // Skip sparse runs. decode_data_runs returns
                        // cluster_offset == 0 for holes (offset_size == 0); they
                        // have no stored content and no real LCN. Queuing them as
                        // (lcn=0, cc) makes real stream runs match LCN 0 and pile
                        // up on $Boot's clusters -> cross-links in chkdsk.
                        if dr.cluster_offset > 0 && dr.length > 0 {
                            out.push((dr.cluster_offset as u64, dr.length));
                        }
                    }
                }
            }
            pos = rec_off + rec_size;
        } else {
            break;
        }
    }
}

/// A `Read + Seek` adapter over a compressed NTFS block stream. Used
/// for on-demand reads after the index has been built.
struct NtfsDecompressingReader<'a, R> {
    inner: &'a mut R,
    state: &'a mut NtfsCompressedState,
    position: u64,
}

impl<'a, R: Read + Seek> NtfsDecompressingReader<'a, R> {
    fn ensure_block_cached(&mut self, idx: usize) -> std::io::Result<()> {
        if self.state.cache_block == Some(idx) {
            return Ok(());
        }
        let block = &self.state.blocks[idx];
        let comp_size = block.comp_size as usize;
        let mut comp_buf = vec![0u8; comp_size];
        self.inner
            .seek(SeekFrom::Start(block.file_offset))
            .map_err(std::io::Error::other)?;
        self.inner
            .read_exact(&mut comp_buf)
            .map_err(std::io::Error::other)?;

        self.state.cache_buf.clear();
        self.state
            .cache_buf
            .resize(block.decomp_size as usize + 512, 0);

        let n = match self.state.compression {
            GhoCompression::High => {
                use flate2::read::ZlibDecoder;
                let mut dec = ZlibDecoder::new(&comp_buf[..]);
                let mut total = 0;
                loop {
                    match dec.read(&mut self.state.cache_buf[total..]) {
                        Ok(0) => break,
                        Ok(k) => total += k,
                        Err(e) => return Err(e),
                    }
                }
                total
            }
            GhoCompression::Fast => {
                fast_lz_decompress(&comp_buf, comp_size, &mut self.state.cache_buf)
                    .map_err(|e| std::io::Error::other(format!("{e:#}")))?
            }
            _ => {
                return Err(std::io::Error::other("unsupported compression"));
            }
        };
        self.state.cache_buf.truncate(n);
        self.state.cache_block = Some(idx);
        Ok(())
    }

    fn find_block(&self, pos: u64) -> Option<usize> {
        self.state
            .blocks
            .binary_search_by(|b| {
                if pos < b.decomp_offset {
                    std::cmp::Ordering::Greater
                } else if pos >= b.decomp_offset + b.decomp_size as u64 {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
    }
}

impl<R: Read + Seek> Read for NtfsDecompressingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || self.position >= self.state.total_decompressed {
            return Ok(0);
        }
        let Some(idx) = self.find_block(self.position) else {
            return Ok(0);
        };
        self.ensure_block_cached(idx)?;
        let block = &self.state.blocks[idx];
        let off_in_block = (self.position - block.decomp_offset) as usize;
        let avail = self.state.cache_buf.len().saturating_sub(off_in_block);
        let n = avail.min(buf.len());
        buf[..n].copy_from_slice(&self.state.cache_buf[off_in_block..off_in_block + n]);
        self.position += n as u64;
        Ok(n)
    }
}

impl<R: Read + Seek> Seek for NtfsDecompressingReader<'_, R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p,
            SeekFrom::Current(delta) => {
                if delta >= 0 {
                    self.position.saturating_add(delta as u64)
                } else {
                    self.position.saturating_sub((-delta) as u64)
                }
            }
            SeekFrom::End(delta) => {
                let total = self.state.total_decompressed;
                if delta >= 0 {
                    total.saturating_add(delta as u64)
                } else {
                    total.saturating_sub((-delta) as u64)
                }
            }
        };
        self.position = new_pos;
        Ok(new_pos)
    }
}

/// Locate the NTFS VBR embedded in the GHPR metadata region. Scans from
/// `start` to `end` for the 3-byte jump + `NTFS    ` OEM ID + valid
/// `0x55 0xAA` boot signature.
fn find_ntfs_vbr_in_header<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
) -> Result<(u64, [u8; 512])> {
    let mut buf = [0u8; 512];
    let mut off = start;
    while off + 512 <= end {
        reader.seek(SeekFrom::Start(off))?;
        if reader.read_exact(&mut buf).is_err() {
            break;
        }
        if &buf[3..11] == b"NTFS    " && buf[510] == 0x55 && buf[511] == 0xAA {
            return Ok((off, buf));
        }
        off += 1;
    }
    bail!("NTFS VBR not found in GHPR metadata region");
}

/// Parse the 42-byte MFT run header. Returns the cluster count, or `None`
/// if the header doesn't match the MFT pattern (entry A + entry B + entry C).
fn parse_ntfs_run_header(data: &[u8]) -> Option<u64> {
    if data.len() < NTFS_RUN_HEADER_SIZE {
        return None;
    }
    if data[0..16] != NTFS_RUN_HEADER_ENTRY1 {
        return None;
    }
    if data[32] != 0x0F || data[41] != 0x0E {
        return None;
    }
    let cluster_count = u64::from_le_bytes([
        data[33], data[34], data[35], data[36], data[37], data[38], data[39], data[40],
    ]);
    Some(cluster_count)
}

/// Map an absolute decompressed offset to a position within the rolling
/// `scan.stream` buffer (whose first byte is at `stream_base`). Returns
/// `None` if the offset is before the buffer or past its end. Free function
/// (not a closure) so it doesn't borrow `scan` across mutations.
fn stream_local_offset(abs: u64, stream_base: u64, stream_len: usize) -> Option<usize> {
    if abs >= stream_base {
        let l = (abs - stream_base) as usize;
        if l <= stream_len {
            return Some(l);
        }
    }
    None
}

/// Parse the 26-byte run header used for every non-MFT data run
/// (entry B + entry C). Returns `(sequence_number, cluster_count)`, or
/// `None` if the header doesn't match. The sequence number (entry B bytes
/// 7-14, u64-LE) is the run's index in MFT data-run order: 0 = $MFT, 1 =
/// first non-MFT run, etc. — used to match the run to its LCN exactly.
fn parse_ntfs_run_b_header(data: &[u8]) -> Option<(u64, u64)> {
    if data.len() < NTFS_RUN_B_HEADER_SIZE {
        return None;
    }
    if !match_run_entry_b(&data[0..16]) {
        return None;
    }
    if data[16] != 0x0F || data[25] != 0x0E {
        return None;
    }
    let seq = u64::from_le_bytes([
        data[7], data[8], data[9], data[10], data[11], data[12], data[13], data[14],
    ]);
    let cluster_count = u64::from_le_bytes([
        data[17], data[18], data[19], data[20], data[21], data[22], data[23], data[24],
    ]);
    Some((seq, cluster_count))
}

/// Parse inline MFT FILE records in the gap between cluster runs.
/// Appends `(lcn, cluster_count)` entries to `out` for every non-resident
/// attribute that has data runs (`$DATA`, `$INDEX_ALLOCATION`, `$BITMAP`,
/// `$SECURITY_DESCRIPTOR`, etc.). Ghost stores cluster data for ALL
/// non-resident attributes, not just `$DATA`.
fn parse_inline_mft_records<R: Read + Seek>(
    reader: &mut R,
    start: u64,
    end: u64,
    vbr: &crate::fs::ntfs::NtfsVbr,
    out: &mut Vec<(u64, u64)>,
) {
    use crate::fs::ntfs::{apply_fixup, parse_mft_attributes};
    let rec_size = vbr.mft_record_size as usize;
    let gap_size = (end - start) as usize;
    if gap_size < rec_size + 4 {
        return;
    }
    let mut buf = vec![0u8; gap_size];
    if reader.seek(SeekFrom::Start(start)).is_err() {
        return;
    }
    let n = match read_fully_or_eof(reader, &mut buf) {
        Ok(n) => n,
        Err(_) => return,
    };
    buf.truncate(n);

    let mut pos = 0;
    while pos + rec_size <= buf.len() {
        if let Some(idx) = buf[pos..].windows(4).position(|w| w == b"FILE") {
            let rec_off = pos + idx;
            if rec_off + rec_size > buf.len() {
                break;
            }
            let mut rec = buf[rec_off..rec_off + rec_size].to_vec();
            let rec_num = u32::from_le_bytes([rec[44], rec[45], rec[46], rec[47]]);
            // Skip $BadClus (record 8) — sparse run covering entire volume.
            if rec_num == 8 {
                pos = rec_off + rec_size;
                continue;
            }
            let _ = apply_fixup(&mut rec, vbr.bytes_per_sector);
            let attrs = parse_mft_attributes(&rec, vbr.mft_record_size);
            for attr in &attrs {
                if !attr.resident && !attr.data_runs.is_empty() {
                    for dr in &attr.data_runs {
                        // Skip sparse runs. decode_data_runs returns
                        // cluster_offset == 0 for holes (offset_size == 0); they
                        // have no stored content and no real LCN. Queuing them as
                        // (lcn=0, cc) makes real stream runs match LCN 0 and pile
                        // up on $Boot's clusters -> cross-links in chkdsk.
                        if dr.cluster_offset > 0 && dr.length > 0 {
                            out.push((dr.cluster_offset as u64, dr.length));
                        }
                    }
                }
            }
            pos = rec_off + rec_size;
        } else {
            break;
        }
    }
}

/// Build the NTFS cluster run index by walking the packed data stream.
///
/// Strategy: the first run is always the MFT. We read it, parse every
/// MFT record to collect all non-resident `$DATA` attribute data runs,
/// and build a queue of `(LCN, cluster_count)` entries sorted by MFT
/// record number. Each subsequent cluster run in the file consumes the
/// next entry from this queue (matched by cluster count).
fn index_ntfs_file_aware<R: Read + Seek>(
    reader: &mut R,
    file_size: u64,
    vbr: &crate::fs::ntfs::NtfsVbr,
    vbr_raw: &[u8; 512],
) -> Result<NtfsGhoIndex> {
    use crate::fs::ntfs::{apply_fixup, parse_mft_attributes};

    let cluster_size = vbr.bytes_per_sector * vbr.sectors_per_cluster;
    let volume_size = vbr.total_sectors * vbr.bytes_per_sector;
    let mft_record_size = vbr.mft_record_size as usize;

    // Scan for the first run header. For raw files the GHPR metadata
    // occupies the first ~8 KiB, so we skip it; for the decompressed
    // view of a compressed file the stream starts right at the data.
    let scan_start = 0u64;
    let scan_end = file_size.min(scan_start + 256 * 1024);
    let mut header_buf = [0u8; NTFS_RUN_HEADER_SIZE];
    let mut first_run_offset: Option<u64> = None;

    let mut off = scan_start;
    while off + NTFS_RUN_HEADER_SIZE as u64 <= scan_end {
        reader.seek(SeekFrom::Start(off))?;
        if reader.read_exact(&mut header_buf).is_err() {
            break;
        }
        if parse_ntfs_run_header(&header_buf).is_some() {
            first_run_offset = Some(off);
            break;
        }
        off += 1;
    }
    let first_run = first_run_offset
        .ok_or_else(|| anyhow!("NTFS file-aware: no cluster run header found in scan region"))?;

    // --- Phase 1: read the MFT (run 0) ---
    reader.seek(SeekFrom::Start(first_run))?;
    reader.read_exact(&mut header_buf)?;
    let mft_clusters = parse_ntfs_run_header(&header_buf)
        .ok_or_else(|| anyhow!("first run header is not valid"))?;
    let mft_data_start = first_run + NTFS_RUN_HEADER_SIZE as u64;
    let mft_size = mft_clusters * cluster_size;

    log::info!(
        "Reading MFT: {} clusters ({:.1} MB) from offset {:#x}",
        mft_clusters,
        mft_size as f64 / (1024.0 * 1024.0),
        mft_data_start
    );

    // Parse all MFT records to build the data-run queue.
    // Each entry: (mft_record_number, lcn, cluster_count)
    let mut data_run_queue: Vec<(u64, u64)> = Vec::new();
    // $Bitmap (record 6) $DATA runs — where the synthesized bitmap is served.
    let mut bitmap_runs: Vec<(u64, u64)> = Vec::new();
    // $MFTMirr (record 1) $DATA run — (lcn, cluster_count). Ghost omits the
    // mirror's content from the stream (it's a verbatim copy of $MFT's first
    // records, hence derivable), so we synthesize it by mapping these clusters
    // back to the start of $MFT's data. See the run injection below.
    let mut mftmirr_run: Option<(u64, u64)> = None;
    let num_records = mft_size / mft_record_size as u64;
    let mut rec_buf = vec![0u8; mft_record_size];

    for rec_idx in 0..num_records {
        let rec_off = mft_data_start + rec_idx * mft_record_size as u64;
        reader.seek(SeekFrom::Start(rec_off))?;
        if reader.read_exact(&mut rec_buf).is_err() {
            break;
        }
        if &rec_buf[0..4] != b"FILE" {
            continue;
        }
        let rec_num = u32::from_le_bytes([rec_buf[44], rec_buf[45], rec_buf[46], rec_buf[47]]);
        // Skip $BadClus (record 8) — it has a sparse data run covering
        // the entire volume but no actual data is stored.
        if rec_num == 8 {
            continue;
        }
        let _ = apply_fixup(&mut rec_buf, vbr.bytes_per_sector);
        let attrs = parse_mft_attributes(&rec_buf, vbr.mft_record_size);
        for attr in &attrs {
            if attr.resident || attr.data_runs.is_empty() {
                continue;
            }
            // Record 0 is $MFT itself; fragment 0 is already registered as
            // run 0, but its later $DATA fragments must be queued so they
            // get mapped — otherwise high-numbered MFT records read as zeros.
            let skip_first = rec_num == 0 && attr.attr_type == 0x80;
            for (i, dr) in attr.data_runs.iter().enumerate() {
                if skip_first && i == 0 {
                    continue;
                }
                // Skip sparse runs (cluster_offset == 0 = hole); see the inline
                // MFT parser for why queuing them as lcn=0 causes cross-links.
                if dr.cluster_offset > 0 && dr.length > 0 {
                    // $MFTMirr's $DATA: remember it for synthesis, don't queue
                    // it (no stream run will ever match — Ghost omits the
                    // mirror content). Queuing would just leave it unmapped.
                    if rec_num == 1 && attr.attr_type == 0x80 && mftmirr_run.is_none() {
                        mftmirr_run = Some((dr.cluster_offset as u64, dr.length));
                        continue;
                    }
                    data_run_queue.push((dr.cluster_offset as u64, dr.length));
                    if rec_num == 6 && attr.attr_type == 0x80 {
                        bitmap_runs.push((dr.cluster_offset as u64, dr.length));
                    }
                }
            }
        }
    }

    log::info!(
        "MFT parsed: {} data-run entries from {} MFT records",
        data_run_queue.len(),
        num_records
    );

    // --- Phase 2: walk all cluster runs using the run-header needle ---
    let mut runs: Vec<NtfsGhoClusterRun> = Vec::new();
    let mut total_stored_clusters: u64 = 0;

    // Add the MFT as run 0. seq=0 — it's the first fragment of $MFT's $DATA.
    runs.push(NtfsGhoClusterRun {
        lcn_start: vbr.mft_cluster,
        cluster_count: mft_clusters,
        file_offset: mft_data_start,
        seq: 0,
    });
    total_stored_clusters += mft_clusters;

    // Synthesize $MFTMirr. NTFS keeps $MFTMirr as a byte-verbatim copy of
    // $MFT's first records (the mirror cluster equals the MFT's first cluster,
    // confirmed against ground-truth disks), so map the mirror's clusters
    // straight back to the start of $MFT's stream data. Ghost stores no mirror
    // content of its own (file-aware backups omit derivable system streams the
    // way they omit $Bitmap / $Boot), so without this the mirror reads as zeros
    // and chkdsk reports "$MFTMirr ($DATA) is corrupt / cross-linked".
    if let Some((mirr_lcn, mirr_cc)) = mftmirr_run {
        let cc = mirr_cc.min(mft_clusters);
        if cc > 0 {
            runs.push(NtfsGhoClusterRun {
                lcn_start: mirr_lcn,
                cluster_count: cc,
                file_offset: mft_data_start,
                seq: 0,
            });
            log::info!(
                "Synthesized $MFTMirr: {} cluster(s) at lcn {} mapped to $MFT data",
                cc,
                mirr_lcn
            );
        }
    }

    // Pending data runs from inline MFT records. Matched to cluster runs in
    // MFT-walk order, freshest gap first (see match_inline_run).
    let mut pending_lcns: Vec<(u64, u64)> = Vec::new();
    let mut pending_group_start: usize = 0;

    // LCN starts already assigned — prevents two stream runs landing on the
    // same clusters when Ghost re-declares a run inline and in the MFT queue.
    let mut assigned: std::collections::HashSet<u64> = runs.iter().map(|r| r.lcn_start).collect();

    // Runs that couldn't be matched to an inline MFT record's data runs.
    // Resolved via $Bitmap after the main scan.
    let mut unmapped_runs: Vec<(u64, u64)> = Vec::new(); // (cluster_count, file_offset)

    // Scan the entire file for run headers using the 16-byte needle.
    let needle = &NTFS_RUN_HEADER_ENTRY1;
    let chunk_size: usize = 4 * 1024 * 1024;
    let mut search_pos = mft_data_start + mft_size;
    let mut prev_data_end = search_pos;

    while search_pos < file_size {
        let read_len = ((file_size - search_pos) as usize).min(chunk_size + 64);
        let mut chunk = vec![0u8; read_len];
        reader.seek(SeekFrom::Start(search_pos))?;
        let n = read_fully_or_eof(reader, &mut chunk)?;
        chunk.truncate(n);

        let mut scan_pos = 0;
        while scan_pos + NTFS_RUN_HEADER_SIZE <= chunk.len() {
            if let Some(idx) = chunk[scan_pos..].windows(16).position(|w| w == needle) {
                let abs_off = search_pos + (scan_pos + idx) as u64;
                let local_off = scan_pos + idx;
                if local_off + NTFS_RUN_HEADER_SIZE <= chunk.len() {
                    if let Some(cc) = parse_ntfs_run_header(&chunk[local_off..]) {
                        if cc > 0 {
                            let data_start = abs_off + NTFS_RUN_HEADER_SIZE as u64;
                            let data_end = data_start + cc * cluster_size;
                            if data_end > file_size + cluster_size {
                                scan_pos = chunk.len();
                                break;
                            }

                            // Parse inline MFT records in the gap since the
                            // last run. Each gap may contain multiple FILE
                            // records whose data runs describe subsequent runs.
                            // Append to (not replace) pending_lcns — a single
                            // gap's MFT records can provide LCNs for many
                            // subsequent runs.
                            let gap_start = prev_data_end;
                            let gap_end = abs_off;
                            if gap_end > gap_start && (gap_end - gap_start) < 1024 * 1024 {
                                let len_before = pending_lcns.len();
                                parse_inline_mft_records(
                                    reader,
                                    gap_start,
                                    gap_end,
                                    vbr,
                                    &mut pending_lcns,
                                );
                                // A non-empty gap starts a fresh group: the runs
                                // that follow consume these entries in MFT order.
                                if pending_lcns.len() > len_before {
                                    pending_group_start = len_before;
                                }
                            }

                            // Determine LCN from pending data runs in MFT-walk
                            // order (freshest gap first). Skip already-assigned
                            // LCNs so a re-declared run doesn't double-map onto
                            // another file's clusters.
                            let lcn = match_inline_run(
                                &mut pending_lcns,
                                &mut pending_group_start,
                                cc,
                                &assigned,
                            );

                            if let Some(lcn) = lcn {
                                assigned.insert(lcn);
                                runs.push(NtfsGhoClusterRun {
                                    lcn_start: lcn,
                                    cluster_count: cc,
                                    file_offset: data_start,
                                    // TODO: parse seq from entry B inside the 3-entry header
                                    // for the uncompressed path. Currently a sentinel — fixup
                                    // will fall back to non-seq matching for these.
                                    seq: u64::MAX,
                                });
                            } else {
                                unmapped_runs.push((cc, data_start));
                            }
                            total_stored_clusters += cc;
                            prev_data_end = data_end;

                            if data_end > search_pos + chunk.len() as u64 {
                                search_pos = data_end;
                                scan_pos = chunk.len();
                                break;
                            } else {
                                scan_pos = (data_end - search_pos) as usize;
                                continue;
                            }
                        }
                    }
                }
                scan_pos += idx + 16;
            } else {
                break;
            }
        }

        if scan_pos < chunk.len() {
            search_pos += chunk.len().saturating_sub(15) as u64;
        } else if search_pos + chunk.len() as u64 >= file_size {
            break;
        }
    }

    // --- Phase 3: resolve unmapped runs via $Bitmap ---
    if !unmapped_runs.is_empty() {
        log::info!(
            "{} unmapped runs ({} clusters), resolving via MFT data-run queue",
            unmapped_runs.len(),
            unmapped_runs.iter().map(|(cc, _)| cc).sum::<u64>()
        );

        // Build a set of already-mapped LCN ranges for fast lookup.
        let mut mapped_lcns: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for run in &runs {
            for c in 0..run.cluster_count {
                mapped_lcns.insert(run.lcn_start + c);
            }
        }

        // Collect unmapped data runs from the MFT queue (those not
        // consumed by inline matching). Match by cluster count.
        let mut remaining_queue: Vec<(u64, u64)> = data_run_queue
            .iter()
            .filter(|&&(lcn, len)| {
                // Exclude runs already mapped.
                !(0..len).any(|c| mapped_lcns.contains(&(lcn + c)))
            })
            .copied()
            .collect();

        let mut resolved = 0u64;
        for &(cc, file_offset) in &unmapped_runs {
            if let Some(lcn) = pop_unassigned_lcn(&mut remaining_queue, cc, &assigned) {
                assigned.insert(lcn);
                runs.push(NtfsGhoClusterRun {
                    lcn_start: lcn,
                    cluster_count: cc,
                    file_offset,
                    seq: u64::MAX,
                });
                resolved += 1;
            }
        }

        log::info!(
            "Resolved {} of {} unmapped runs via MFT queue fallback",
            resolved,
            unmapped_runs.len()
        );
    }

    // Sort by LCN for binary search during reads.
    runs.sort_by_key(|r| r.lcn_start);

    log::info!(
        "NTFS file-aware index: {} runs, {} stored clusters ({:.1} MB), volume {:.1} GB",
        runs.len(),
        total_stored_clusters,
        total_stored_clusters as f64 * cluster_size as f64 / (1024.0 * 1024.0),
        volume_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    // Synthesize $Bitmap from the complete run set (every allocated cluster
    // with stored content) plus $Bitmap's own clusters. The uncompressed
    // builder discovers all runs here, so the bitmap is complete now.
    let total_clusters = volume_size / cluster_size;
    let mut allocation: Vec<(u64, u64)> = runs
        .iter()
        .map(|r| (r.lcn_start, r.cluster_count))
        .collect();
    allocation.extend_from_slice(&bitmap_runs);
    let synth_bitmap = build_synth_ntfs_bitmap(&allocation, total_clusters);
    log::info!(
        "Synthesized $Bitmap: {} bytes over {} run(s) for {} clusters",
        synth_bitmap.len(),
        bitmap_runs.len(),
        total_clusters,
    );

    Ok(NtfsGhoIndex {
        runs,
        volume_size,
        cluster_size,
        vbr: *vbr_raw,
        bitmap_runs,
        synth_bitmap,
        boot_code: Vec::new(),
    })
}

/// MFT-only open for compressed NTFS file-aware GHOs. Decompresses
/// just the first run (MFT), parses all MFT records, and returns a
/// partial index + lazy-scan state. The full run index is built
/// incrementally by `extend_ntfs_compressed_index` as reads arrive.
fn open_ntfs_compressed_mft_only(
    reader: &mut SpanReader,
    data_start: u64,
    file_size: u64,
    compression: GhoCompression,
    vbr: &crate::fs::ntfs::NtfsVbr,
    vbr_raw: &[u8; 512],
) -> Result<(NtfsCompressedState, NtfsGhoIndex)> {
    use crate::fs::ntfs::{apply_fixup, parse_mft_attributes};

    let _ = compression;
    let cluster_size = vbr.bytes_per_sector * vbr.sectors_per_cluster;
    let volume_size = vbr.total_sectors * vbr.bytes_per_sector;
    let mft_record_size = vbr.mft_record_size as usize;

    let end = file_size;
    let io_chunk: usize = 4 * 1024 * 1024;
    let max_decomp_block: usize = FAST_LZ_BLOCK_SIZE + 1024;
    let mut decomp_buf = vec![0u8; max_decomp_block];
    let mut blocks = Vec::with_capacity(8_000);
    let mut decomp_offset: u64 = 0;
    let mut stream = Vec::with_capacity(128 * 1024 * 1024);

    // Find first zlib header.
    let mut off = data_start;
    {
        let scan_len = ((end - off) as usize).min(8192);
        let mut scan_buf = vec![0u8; scan_len];
        reader.seek(SeekFrom::Start(off))?;
        let n = read_fully_or_eof(reader, &mut scan_buf)?;
        let mut found = false;
        for i in 0..n.saturating_sub(1) {
            if scan_buf[i] == 0x78 && scan_buf[i + 1] == 0x01 {
                off += i as u64;
                found = true;
                break;
            }
        }
        if !found {
            bail!("no zlib header found in NTFS compressed data region");
        }
    }

    // Decompress blocks until we have enough for the MFT.
    // Phase 1: find the first run header → get MFT cluster count.
    // Phase 2: accumulate MFT data → parse MFT records.
    let mut chunk_buf = vec![0u8; io_chunk + 64 * 1024];
    let mut chunk_start = off;
    reader.seek(SeekFrom::Start(chunk_start))?;
    let mut chunk_len = read_fully_or_eof(reader, &mut chunk_buf)?;
    let mut local = 0usize;

    let mut mft_data_start: Option<u64> = None;
    let mut mft_clusters: u64 = 0;
    let mut data_run_queue: Vec<(u64, u64)> = Vec::new();
    let mut bitmap_runs: Vec<(u64, u64)> = Vec::new();
    let mut mftmirr_run: Option<(u64, u64)> = None;
    let mut runs: Vec<NtfsGhoClusterRun> = Vec::new();
    let mut mft_parsed = false;

    let mut consecutive_fails = 0u32;
    while chunk_start + local as u64 + 4 < end && !mft_parsed {
        // Refill
        if local + 64 * 1024 > chunk_len && chunk_start + (chunk_len as u64) < end {
            let remaining = chunk_len - local;
            chunk_buf.copy_within(local..chunk_len, 0);
            chunk_start += local as u64;
            local = 0;
            reader.seek(SeekFrom::Start(chunk_start + remaining as u64))?;
            let extra = read_fully_or_eof(reader, &mut chunk_buf[remaining..])?;
            chunk_len = remaining + extra;
        }
        if local + 4 > chunk_len {
            break;
        }
        if chunk_buf[local] != 0x78 || chunk_buf[local + 1] != 0x01 {
            local += 1;
            consecutive_fails += 1;
            if consecutive_fails > 1_000_000 {
                // Ghost interleaves structural records between zlib blocks; a
                // long non-zlib gap must not abort the scan (doing so abandons
                // the rest of the stream and silently drops data). Reset and
                // keep scanning forward to resync at the next block. The high
                // cap only guards against u32 overflow.
                consecutive_fails = 0;
            }
            continue;
        }

        let avail = chunk_len - local;
        let input = &chunk_buf[local..local + avail];
        let mut decompress = flate2::Decompress::new(true);
        let mut total_in = 0usize;
        let mut ok = true;
        loop {
            let in_before = decompress.total_in() as usize;
            let out_before = decompress.total_out();
            let out_off = (decompress.total_out() as usize) % decomp_buf.len();
            match decompress.decompress(
                &input[total_in..],
                &mut decomp_buf[out_off..],
                flate2::FlushDecompress::None,
            ) {
                Ok(flate2::Status::Ok) => {
                    total_in = decompress.total_in() as usize;
                    if decompress.total_in() as usize == in_before
                        && decompress.total_out() == out_before
                    {
                        break;
                    }
                }
                Ok(flate2::Status::StreamEnd) => {
                    total_in = decompress.total_in() as usize;
                    break;
                }
                Ok(flate2::Status::BufError) | Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        let total_out = decompress.total_out() as usize;

        if !ok || total_out == 0 {
            local += 2;
            consecutive_fails += 1;
            if consecutive_fails > 1_000_000 {
                // Ghost interleaves structural records between zlib blocks; a
                // long non-zlib gap must not abort the scan (doing so abandons
                // the rest of the stream and silently drops data). Reset and
                // keep scanning forward to resync at the next block. The high
                // cap only guards against u32 overflow.
                consecutive_fails = 0;
            }
            continue;
        }

        blocks.push(NtfsCompressedBlock {
            file_offset: chunk_start + local as u64,
            comp_size: total_in as u32,
            decomp_offset,
            decomp_size: total_out as u32,
        });

        if total_out <= decomp_buf.len() {
            stream.extend_from_slice(&decomp_buf[..total_out]);
        } else {
            let mut big = vec![0u8; total_out];
            let mut d2 = flate2::Decompress::new(true);
            let _ = d2.decompress(
                &input[..total_in],
                &mut big,
                flate2::FlushDecompress::Finish,
            );
            stream.extend_from_slice(&big[..d2.total_out() as usize]);
        }
        decomp_offset += total_out as u64;
        local += total_in + 2;
        consecutive_fails = 0;

        // Try to find the run header and MFT in the accumulated stream.
        if mft_data_start.is_none() {
            let needle = &NTFS_RUN_HEADER_ENTRY1;
            if let Some(pos) = stream
                .windows(NTFS_RUN_HEADER_SIZE)
                .position(|w| w[..16] == *needle)
            {
                let hex: String = stream[pos..(pos + NTFS_RUN_HEADER_SIZE).min(stream.len())]
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<_>>()
                    .join(" ");
                log::info!("MFT header at stream pos {:#x}: {}", pos, hex);
                if let Some(cc) = parse_ntfs_run_header(&stream[pos..]) {
                    let mds = pos as u64 + NTFS_RUN_HEADER_SIZE as u64;
                    mft_clusters = cc;
                    mft_data_start = Some(mds);
                    let mft_size = cc * cluster_size;
                    log::info!(
                        "Reading MFT: {} clusters ({:.1} MB) from decompressed offset {:#x}",
                        cc,
                        mft_size as f64 / (1024.0 * 1024.0),
                        mds
                    );
                    runs.push(NtfsGhoClusterRun {
                        lcn_start: vbr.mft_cluster,
                        cluster_count: cc,
                        file_offset: mds,
                        seq: 0,
                    });
                }
            }
        }

        if let Some(mds) = mft_data_start {
            let mft_size = mft_clusters * cluster_size;
            let mft_end = mds + mft_size;
            if stream.len() as u64 >= mft_end && !mft_parsed {
                let num_records = mft_size / mft_record_size as u64;
                for rec_idx in 0..num_records {
                    let rec_off = mds as usize + rec_idx as usize * mft_record_size;
                    if rec_off + mft_record_size > stream.len() {
                        break;
                    }
                    let rec = &stream[rec_off..rec_off + mft_record_size];
                    if &rec[0..4] != b"FILE" {
                        continue;
                    }
                    let rec_num = u32::from_le_bytes([rec[44], rec[45], rec[46], rec[47]]);
                    if rec_num == 8 {
                        continue;
                    }
                    let mut rec_buf = rec.to_vec();
                    let _ = apply_fixup(&mut rec_buf, vbr.bytes_per_sector);
                    let attrs = parse_mft_attributes(&rec_buf, vbr.mft_record_size);
                    for attr in &attrs {
                        if attr.resident || attr.data_runs.is_empty() {
                            continue;
                        }
                        // Record 0 is $MFT itself. Its $DATA runs describe
                        // every MFT fragment; fragment 0 is already registered
                        // as the MFT run above, so skip it here, but queue the
                        // later fragments so the lazy scan maps them. Without
                        // this, high-numbered MFT records (which live in those
                        // fragments) read back as zeros -> "invalid magic".
                        let skip_first = rec_num == 0 && attr.attr_type == 0x80;
                        for (i, dr) in attr.data_runs.iter().enumerate() {
                            if skip_first && i == 0 {
                                continue;
                            }
                            // Skip sparse runs (cluster_offset == 0 = hole);
                            // queuing them as lcn=0 causes $Boot cross-links.
                            if dr.cluster_offset > 0 && dr.length > 0 {
                                // $MFTMirr ($DATA, rec 1): synthesized from the
                                // $MFT data, not from a stream run (Ghost omits
                                // the mirror content). Capture, don't queue. See
                                // the uncompressed path for the full rationale.
                                if rec_num == 1 && attr.attr_type == 0x80 && mftmirr_run.is_none() {
                                    mftmirr_run = Some((dr.cluster_offset as u64, dr.length));
                                    continue;
                                }
                                data_run_queue.push((dr.cluster_offset as u64, dr.length));
                                if rec_num == 6 && attr.attr_type == 0x80 {
                                    bitmap_runs.push((dr.cluster_offset as u64, dr.length));
                                }
                            }
                        }
                    }
                }
                // Synthesize $MFTMirr: map its clusters back to the start of
                // $MFT's stream data (the mirror is a verbatim copy of $MFT's
                // first records). Without this the mirror reads as zeros and
                // chkdsk flags "$MFTMirr ($DATA) corrupt / cross-linked".
                if let Some((mirr_lcn, mirr_cc)) = mftmirr_run {
                    let cc = mirr_cc.min(mft_clusters);
                    if cc > 0 {
                        runs.push(NtfsGhoClusterRun {
                            lcn_start: mirr_lcn,
                            cluster_count: cc,
                            file_offset: mds,
                            seq: 0,
                        });
                        log::info!(
                            "Synthesized $MFTMirr: {} cluster(s) at lcn {} mapped to $MFT data",
                            cc,
                            mirr_lcn
                        );
                    }
                }
                log::info!(
                    "MFT parsed: {} data-run entries from {} MFT records",
                    data_run_queue.len(),
                    num_records
                );
                mft_parsed = true;
            }
        }
    }

    if !mft_parsed {
        bail!("failed to read MFT from compressed NTFS stream");
    }

    let mds = mft_data_start.unwrap();
    let mft_end = mds + mft_clusters * cluster_size;
    // The resume position: where we stopped in the compressed file.
    let resume_file_offset = chunk_start + local as u64;

    // Keep the stream tail from mft_end onward for gap parsing when
    // the lazy scan resumes.
    let stream_tail_start = mft_end as usize;
    let leftover_stream = if stream_tail_start < stream.len() {
        stream[stream_tail_start..].to_vec()
    } else {
        Vec::new()
    };

    runs.sort_by_key(|r| r.lcn_start);

    log::info!(
        "MFT-only open: {} blocks, {} run(s), resume scan at {:#x}",
        blocks.len(),
        runs.len(),
        resume_file_offset,
    );

    // $Bitmap is synthesized lazily on first read (see read_ntfs_file_aware_into):
    // the complete run set isn't known until the lazy scan finishes, and most
    // files' MFT records are stored inline in the stream, not in data_run_queue.
    // Leaving synth_bitmap empty signals "not built yet".

    let comp_state = NtfsCompressedState {
        blocks,
        compression,
        total_decompressed: decomp_offset,
        cache_block: None,
        cache_buf: Vec::new(),
        lazy_scan: Some(NtfsLazyScanState {
            file_offset: resume_file_offset,
            decomp_offset,
            file_size,
            vbr: vbr.clone(),
            cluster_size,
            pending_lcns: Vec::new(),
            pending_group_start: 0,
            data_run_queue,
            prev_data_end: mft_end,
            stream: leftover_stream,
            stream_base: mft_end,
            skip_data_bytes: 0,
            search_resume: mft_end,
            assigned_lcns: runs.iter().map(|r| r.lcn_start).collect(),
        }),
        cache_key: None,
    };
    let index = NtfsGhoIndex {
        runs,
        volume_size,
        cluster_size,
        vbr: *vbr_raw,
        bitmap_runs,
        synth_bitmap: Vec::new(), // built lazily on first $Bitmap read
        boot_code: Vec::new(),    // recovered by prepare_full_image (export)
    };
    Ok((comp_state, index))
}

/// Pop the FIRST `(lcn, cc)` entry whose cluster count matches `cc` and whose
/// LCN isn't already assigned to another run. Ghost emits cluster runs in
/// MFT-walk order — record order, then attribute order within a record, then
/// fragment order within an attribute — and the MFT data-run queue is built in
/// that same order, so the correct match for a stream run is the EARLIEST
/// unassigned entry of the right size. The assigned-LCN check prevents two
/// stream runs from landing on the same clusters (Ghost re-declares some runs
/// both inline and in the MFT queue). Returns the chosen LCN, or `None`.
///
/// Used for the global MFT data-run queue (the Phase-3 fallback and the
/// compressed lazy path's secondary lookup), where the whole list is one
/// MFT-ordered group. For the inline `pending_lcns` list, which interleaves a
/// freshly parsed gap's entries with stale orphans, use [`match_inline_run`].
fn pop_unassigned_lcn(
    list: &mut Vec<(u64, u64)>,
    cc: u64,
    assigned: &std::collections::HashSet<u64>,
) -> Option<u64> {
    let idx = list
        .iter()
        .position(|&(lcn, len)| len == cc && !assigned.contains(&lcn))?;
    Some(list.remove(idx).0)
}

/// Match a stream run of size `cc` to an inline-MFT `(lcn, cc)` entry.
///
/// `pending_lcns` accumulates the data runs parsed from inline FILE records as
/// gaps are scanned. Ghost writes each gap's records immediately before the
/// cluster data they describe, and emits that data in MFT-walk order, so the
/// correct entry for a stream run is the EARLIEST unassigned entry of size `cc`
/// within the most recently parsed gap — the suffix `list[group_start..]`.
/// Matching from the back instead (`rposition`) reverses every same-`cc` group:
/// e.g. `$Secure`'s `$SDH`/`$SII` index-allocation blocks get swapped and a
/// fragmented file's `$DATA` extents get reversed (verified against
/// `smallNTFS.GHO`, where it produced chkdsk `$SDH`/`$SII`/`$I30` index errors).
/// Matching from the front of the whole list instead grabs stale orphans: Ghost
/// re-declares some already-emitted runs in a later gap, leaving entries that
/// never get a stream run (e.g. a leftover `(1310297, 1)` ahead of the real
/// `$SDH`/`$SII` pair). So: search the freshest group forward first, and only
/// fall back to older orphaned entries (also forward) if it has no match.
///
/// On success removes the chosen entry, fixes up `*group_start`, and returns the
/// LCN. The assigned-LCN check prevents double-mapping onto another file's
/// clusters.
fn match_inline_run(
    list: &mut Vec<(u64, u64)>,
    group_start: &mut usize,
    cc: u64,
    assigned: &std::collections::HashSet<u64>,
) -> Option<u64> {
    let gs = (*group_start).min(list.len());
    let idx = list[gs..]
        .iter()
        .position(|&(lcn, len)| len == cc && !assigned.contains(&lcn))
        .map(|i| gs + i)
        .or_else(|| {
            list[..gs]
                .iter()
                .position(|&(lcn, len)| len == cc && !assigned.contains(&lcn))
        })?;
    if idx < *group_start {
        *group_start -= 1;
    }
    Some(list.remove(idx).0)
}

/// Extend the NTFS compressed run index by scanning forward in the
/// compressed stream until `target_lcn` is found or the file is exhausted.
/// Called lazily from `read_ntfs_file_aware_into` when a read hits an
/// unmapped LCN.
fn extend_ntfs_compressed_index(
    inner: &mut SpanReader,
    index: &mut NtfsGhoIndex,
    comp: &mut NtfsCompressedState,
    target_lcn: u64,
) -> bool {
    let Some(scan) = &mut comp.lazy_scan else {
        return false;
    };

    let io_chunk: usize = 4 * 1024 * 1024;
    let max_decomp_block: usize = FAST_LZ_BLOCK_SIZE + 1024;
    let mut decomp_buf = vec![0u8; max_decomp_block];
    let mut discard = vec![0u8; 256 * 1024];
    let end = scan.file_size;

    let mut chunk_buf = vec![0u8; io_chunk + 64 * 1024];
    let mut chunk_start = scan.file_offset;
    if inner.seek(SeekFrom::Start(chunk_start)).is_err() {
        return false;
    }
    let mut chunk_len = match read_fully_or_eof(inner, &mut chunk_buf) {
        Ok(n) => n,
        Err(_) => return false,
    };
    let mut local = 0usize;
    let mut found_target = false;
    let mut consecutive_fails = 0u32;
    let mut decompress = flate2::Decompress::new(true);
    let scan_start_offset = chunk_start;
    let scan_start_time = std::time::Instant::now();
    let mut last_progress_log = scan_start_time;
    log::info!(
        "Lazy scan starting: target_lcn={}, from offset {:#x}/{:#x} ({:.1}% of file)",
        target_lcn,
        scan_start_offset,
        end,
        (scan_start_offset as f64 / end as f64) * 100.0
    );

    // Note: we intentionally scan to EOF rather than stopping at the target
    // LCN. Stopping early would leave the target run's cluster-data blocks
    // un-decompressed (and thus absent from comp.blocks), so the read that
    // triggered the scan would get zero bytes back. A full pass records every
    // block once; the index + block table then persist for all later reads.
    while chunk_start + local as u64 + 4 < end {
        // Periodic progress report
        if last_progress_log.elapsed().as_secs() >= 2 {
            let cur = chunk_start + local as u64;
            let scanned = cur.saturating_sub(scan_start_offset);
            let elapsed = scan_start_time.elapsed().as_secs_f64();
            let mb_per_s = (scanned as f64 / 1_048_576.0) / elapsed.max(0.001);
            log::info!(
                "Lazy scan progress: at {:#x}/{:#x} ({:.1}%), {} runs, {} blocks, {:.1} MB/s",
                cur,
                end,
                (cur as f64 / end as f64) * 100.0,
                index.runs.len(),
                comp.blocks.len(),
                mb_per_s
            );
            last_progress_log = std::time::Instant::now();
        }

        // Refill
        if local + 64 * 1024 > chunk_len && chunk_start + (chunk_len as u64) < end {
            let remaining = chunk_len - local;
            chunk_buf.copy_within(local..chunk_len, 0);
            chunk_start += local as u64;
            local = 0;
            if inner
                .seek(SeekFrom::Start(chunk_start + remaining as u64))
                .is_err()
            {
                break;
            }
            let extra = match read_fully_or_eof(inner, &mut chunk_buf[remaining..]) {
                Ok(n) => n,
                Err(_) => break,
            };
            chunk_len = remaining + extra;
        }
        if local + 4 > chunk_len {
            break;
        }
        if chunk_buf[local] != 0x78 || chunk_buf[local + 1] != 0x01 {
            local += 1;
            consecutive_fails += 1;
            if consecutive_fails > 1_000_000 {
                // Ghost interleaves structural records between zlib blocks; a
                // long non-zlib gap must not abort the scan (doing so abandons
                // the rest of the stream and silently drops data). Reset and
                // keep scanning forward to resync at the next block. The high
                // cap only guards against u32 overflow.
                consecutive_fails = 0;
            }
            continue;
        }

        let avail = chunk_len - local;
        let input = &chunk_buf[local..local + avail];

        // If skipping cluster data, use discard buffer.
        if scan.skip_data_bytes > 0 {
            decompress.reset(true);
            let mut total_in = 0usize;
            let mut ok = true;
            loop {
                let in_before = decompress.total_in() as usize;
                let out_before = decompress.total_out();
                match decompress.decompress(
                    &input[total_in..],
                    &mut discard[..],
                    flate2::FlushDecompress::None,
                ) {
                    Ok(flate2::Status::Ok) => {
                        total_in = decompress.total_in() as usize;
                        if decompress.total_in() as usize == in_before
                            && decompress.total_out() == out_before
                        {
                            break;
                        }
                    }
                    Ok(flate2::Status::StreamEnd) => {
                        total_in = decompress.total_in() as usize;
                        break;
                    }
                    Ok(flate2::Status::BufError) | Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
            let total_out = decompress.total_out() as usize;
            if !ok || total_out == 0 {
                local += 2;
                consecutive_fails += 1;
                if consecutive_fails > 1_000_000 {
                    consecutive_fails = 0;
                }
                continue;
            }
            comp.blocks.push(NtfsCompressedBlock {
                file_offset: chunk_start + local as u64,
                comp_size: total_in as u32,
                decomp_offset: scan.decomp_offset,
                decomp_size: total_out as u32,
            });
            scan.decomp_offset += total_out as u64;
            comp.total_decompressed = scan.decomp_offset;
            // If this block straddles the end of the run's cluster data, keep
            // the metadata tail (decompressed into `discard`) so the gap's
            // inline MFT records aren't lost. stream_base was set to
            // run_data_end when the skip was triggered, so the tail's bytes
            // begin exactly at the (now empty) stream's base.
            if (total_out as u64) > scan.skip_data_bytes {
                let tail_start = scan.skip_data_bytes as usize;
                if tail_start < total_out && total_out <= discard.len() {
                    scan.stream
                        .extend_from_slice(&discard[tail_start..total_out]);
                }
                scan.skip_data_bytes = 0;
            } else {
                scan.skip_data_bytes -= total_out as u64;
            }
            local += total_in + 2;
            consecutive_fails = 0;
            continue;
        }

        // Metadata block — decompress fully.
        decompress.reset(true);
        let mut total_in = 0usize;
        let mut ok = true;
        loop {
            let in_before = decompress.total_in() as usize;
            let out_before = decompress.total_out();
            let out_off = (decompress.total_out() as usize) % decomp_buf.len();
            match decompress.decompress(
                &input[total_in..],
                &mut decomp_buf[out_off..],
                flate2::FlushDecompress::None,
            ) {
                Ok(flate2::Status::Ok) => {
                    total_in = decompress.total_in() as usize;
                    if decompress.total_in() as usize == in_before
                        && decompress.total_out() == out_before
                    {
                        break;
                    }
                }
                Ok(flate2::Status::StreamEnd) => {
                    total_in = decompress.total_in() as usize;
                    break;
                }
                Ok(flate2::Status::BufError) | Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        let total_out = decompress.total_out() as usize;
        if !ok || total_out == 0 {
            local += 2;
            consecutive_fails += 1;
            if consecutive_fails > 1_000_000 {
                // Ghost interleaves structural records between zlib blocks; a
                // long non-zlib gap must not abort the scan (doing so abandons
                // the rest of the stream and silently drops data). Reset and
                // keep scanning forward to resync at the next block. The high
                // cap only guards against u32 overflow.
                consecutive_fails = 0;
            }
            continue;
        }

        comp.blocks.push(NtfsCompressedBlock {
            file_offset: chunk_start + local as u64,
            comp_size: total_in as u32,
            decomp_offset: scan.decomp_offset,
            decomp_size: total_out as u32,
        });

        if total_out <= decomp_buf.len() {
            scan.stream.extend_from_slice(&decomp_buf[..total_out]);
        }
        scan.decomp_offset += total_out as u64;
        comp.total_decompressed = scan.decomp_offset;
        local += total_in + 2;
        consecutive_fails = 0;

        // Search for run headers in accumulated stream.
        let stream_abs_len = scan.stream_base + scan.stream.len() as u64;

        // Tracks whether the inner search loop reached EOH (end of haystack)
        // without finding a match. Only then is it safe to advance
        // `search_resume` past the current stream — otherwise we risk
        // skipping a needle that was found but couldn't be parsed because
        // entry C extended past the current stream.
        let mut full_scan_completed = false;
        loop {
            let search_start = scan.prev_data_end.max(scan.search_resume);
            let Some(search_local) =
                stream_local_offset(search_start, scan.stream_base, scan.stream.len())
            else {
                break;
            };
            if search_local + NTFS_RUN_B_HEADER_SIZE > scan.stream.len() {
                // Not enough bytes from search_start onward to even contain
                // a complete header — wait for the next block.
                break;
            }
            // Subsequent (non-MFT) runs use the 26-byte entry B + entry C
            // header. Byte 7 of entry B is a run sequence number, so we
            // match with a wildcard there.
            let haystack = &scan.stream[search_local..];
            let mut idx_opt: Option<usize> = None;
            for i in 0..haystack.len().saturating_sub(16) + 1 {
                if match_run_entry_b(&haystack[i..i + 16]) {
                    idx_opt = Some(i);
                    break;
                }
            }
            let Some(idx) = idx_opt else {
                // Scanned the entire haystack — no needle. Safe to advance
                // search_resume past this stream extent.
                full_scan_completed = true;
                break;
            };
            let hdr_local = search_local + idx;
            let hdr_abs = scan.stream_base + hdr_local as u64;
            if hdr_local + NTFS_RUN_B_HEADER_SIZE > scan.stream.len() {
                // Found a needle but entry C extends past current stream.
                // Resume right here next pass so we re-find and parse it.
                if scan.search_resume < hdr_abs {
                    scan.search_resume = hdr_abs;
                }
                break;
            }
            let Some((seq, cc)) = parse_ntfs_run_b_header(&scan.stream[hdr_local..]) else {
                scan.prev_data_end = hdr_abs + 16;
                continue;
            };
            if cc == 0 {
                scan.prev_data_end = hdr_abs + NTFS_RUN_B_HEADER_SIZE as u64;
                continue;
            }

            let run_data_start = hdr_abs + NTFS_RUN_B_HEADER_SIZE as u64;
            let run_data_end = run_data_start + cc * scan.cluster_size;

            // Parse inline MFT records in the gap since the last run. Each
            // gap holds the FILE records whose data runs describe the runs
            // that follow, so this populates pending_lcns just-in-time.
            if let Some(gap_local_start) =
                stream_local_offset(scan.prev_data_end, scan.stream_base, scan.stream.len())
            {
                if hdr_local > gap_local_start {
                    let gap = scan.stream[gap_local_start..hdr_local].to_vec();
                    let len_before = scan.pending_lcns.len();
                    parse_inline_mft_from_buf(&gap, &scan.vbr, &mut scan.pending_lcns);
                    // A non-empty gap starts a fresh group (see match_inline_run).
                    if scan.pending_lcns.len() > len_before {
                        scan.pending_group_start = len_before;
                    }
                }
            }

            // Match LCN in MFT-walk order: prefer the freshest inline group
            // (pending_lcns; see match_inline_run), then fall back to the global
            // MFT data-run queue. Skip candidates whose LCN is already assigned —
            // Ghost re-declares some files' runs both inline and in the queue, so
            // a naive match double-assigns and cross-links.
            let lcn = match_inline_run(
                &mut scan.pending_lcns,
                &mut scan.pending_group_start,
                cc,
                &scan.assigned_lcns,
            )
            .or_else(|| pop_unassigned_lcn(&mut scan.data_run_queue, cc, &scan.assigned_lcns));

            if let Some(lcn) = lcn {
                scan.assigned_lcns.insert(lcn);
                index.runs.push(NtfsGhoClusterRun {
                    lcn_start: lcn,
                    cluster_count: cc,
                    file_offset: run_data_start,
                    seq,
                });
                if target_lcn >= lcn && target_lcn < lcn + cc {
                    found_target = true;
                }
            } else {
                // Even when no LCN matches, record the stream run so the
                // fixup can reassign by seq later. lcn_start = u64::MAX is a
                // sentinel meaning "no original lcn known".
                index.runs.push(NtfsGhoClusterRun {
                    lcn_start: u64::MAX,
                    cluster_count: cc,
                    file_offset: run_data_start,
                    seq,
                });
            }

            scan.prev_data_end = run_data_end;
            if run_data_end > stream_abs_len {
                scan.skip_data_bytes = run_data_end - stream_abs_len;
                // The remaining run data isn't in the stream yet; the skip
                // path will decompress+discard it. Reset the rolling buffer
                // so it stays contiguous: after the skip completes, new
                // metadata bytes begin at run_data_end.
                scan.stream.clear();
                scan.stream_base = run_data_end;
                scan.search_resume = run_data_end;
                break;
            }
        }

        // Only advance search_resume if the inner loop scanned the entire
        // haystack without finding a needle. If it broke for "insufficient
        // bytes for entry C", search_resume was already pinned to that
        // needle position so we re-find it next pass.
        if full_scan_completed {
            let stream_end_abs = scan.stream_base + scan.stream.len() as u64;
            let new_resume = stream_end_abs.saturating_sub(15);
            if new_resume > scan.search_resume {
                scan.search_resume = new_resume;
            }
        }

        // Drain processed stream data.
        if let Some(drain_local) =
            stream_local_offset(scan.prev_data_end, scan.stream_base, scan.stream.len())
        {
            let drain_local = drain_local.min(scan.stream.len());
            if drain_local > 4 * 1024 * 1024 {
                let keep = scan.stream.len() - drain_local;
                scan.stream.copy_within(drain_local.., 0);
                scan.stream.truncate(keep);
                scan.stream_base += drain_local as u64;
            }
        }
    }

    // Save resume position.
    scan.file_offset = chunk_start + local as u64;
    let final_file_offset = scan.file_offset;
    let reached_eof = final_file_offset + 4 >= end;

    // Re-sort runs after adding new ones.
    index.runs.sort_by_key(|r| r.lcn_start);

    let total_decompressed = comp.total_decompressed;
    let run_count = index.runs.len();
    let block_count = comp.blocks.len();
    // If we reached EOF, mark scan as complete.
    if reached_eof {
        comp.lazy_scan = None;
        log::info!(
            "Lazy scan complete (EOF): {run_count} runs, {block_count} blocks, \
             file_offset={final_file_offset} end={end} total_decompressed={total_decompressed}",
        );
        // The index + block table are now complete; cache them so a later
        // re-open of this archive skips the rescan entirely.
        if let Some(key) = comp.cache_key.clone() {
            ntfs_scan_cache_store(&key, index, comp);
        }
    } else {
        log::warn!(
            "Lazy scan STOPPED EARLY at file_offset={final_file_offset} of end={end} \
             ({run_count} runs, {block_count} blocks, total_decompressed={total_decompressed}) \
             — remaining compressed data was not decoded",
        );
    }

    found_target
}

/// Try to open an NTFS file-aware GHO. Returns the reader mode and
/// volume size on success.
fn try_open_ntfs_file_aware(
    inner: &mut SpanReader,
    file_size: u64,
    compression: GhoCompression,
    cache_key: Option<NtfsScanKey>,
) -> Result<(GhoReaderMode, u64)> {
    use crate::fs::ntfs::parse_vbr;

    // Find the NTFS VBR in the GHPR metadata region.
    let (vbr_off, vbr_raw) = find_ntfs_vbr_in_header(inner, 0x200, file_size.min(0x4000))
        .context("locating NTFS VBR in GHPR metadata")?;
    log::info!("NTFS VBR found at file offset {:#x}", vbr_off);

    let vbr = parse_vbr(&vbr_raw).map_err(|e| anyhow!("parsing NTFS VBR: {e}"))?;
    log::info!(
        "NTFS volume: {} bytes/sector, {} sectors/cluster, {} total sectors, MFT at LCN {}",
        vbr.bytes_per_sector,
        vbr.sectors_per_cluster,
        vbr.total_sectors,
        vbr.mft_cluster
    );

    if matches!(compression, GhoCompression::None) {
        let index = index_ntfs_file_aware(inner, file_size, &vbr, &vbr_raw)
            .context("building NTFS cluster run index")?;
        let served_size = ntfs_served_size(&index);
        return Ok((
            GhoReaderMode::NtfsFileAware {
                index,
                last_run_hint: 0,
                compressed: None,
            },
            served_size,
        ));
    }

    // Compressed NTFS file-aware. A completed lazy scan for this exact archive
    // may already be cached from a prior open (the GUI re-opens a fresh reader
    // per browse/extract operation) — reuse it and skip both the MFT
    // decompression and the whole-stream forward scan.
    if let Some(key) = &cache_key {
        if let Some(snap) = ntfs_scan_cache_get(key) {
            log::info!(
                "Reusing cached NTFS lazy-scan ({} runs, {} blocks) — skipping rescan",
                snap.runs.len(),
                snap.blocks.len()
            );
            return Ok(ntfs_mode_from_cache(&snap, key.clone()));
        }
    }

    // Compressed NTFS file-aware: MFT-only open. Decompress just the
    // first run (MFT) and parse it. The rest of the index is built
    // lazily on demand when reads hit unmapped LCNs.
    log::info!("Opening compressed NTFS file-aware (MFT-only, lazy index)...");
    let data_start = vbr_off + 512;
    let (mut comp_state, index) =
        open_ntfs_compressed_mft_only(inner, data_start, file_size, compression, &vbr, &vbr_raw)
            .context("MFT-only open of compressed NTFS file-aware")?;
    // Remember the cache key so the completed scan gets stored for reuse.
    comp_state.cache_key = cache_key;
    let served_size = ntfs_served_size(&index);
    Ok((
        GhoReaderMode::NtfsFileAware {
            index,
            last_run_hint: 0,
            compressed: Some(comp_state),
        },
        served_size,
    ))
}

/// What first-4-byte magic the cluster content for an NTFS attribute is expected
/// to start with. Used by [`fixup_ntfs_typed_misalignments`] to detect cluster
/// runs the matcher placed at the wrong `lcn_start` when multiple MFT attributes
/// share the same `cluster_count` (the dominant ambiguity is `cc=2`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ExpectedMagic {
    /// `$INDEX_ALLOCATION` (attr type `0xa0`) blocks start with `INDX`.
    Indx,
    /// `$MFT` (rec 0) `$DATA` fragments are sequences of `FILE` records.
    File,
    /// `$LogFile` (rec 2) `$DATA` blocks start with `RSTR` (restart) or `RCRD`
    /// (log record).
    Logfile,
    /// Pre-Windows-2000 per-file `$SECURITY_DESCRIPTOR` (attr type `0x50`) and
    /// the entries in `$Secure $DATA "$SDS"`. The first byte is the SD revision
    /// (always 1), byte 1 is sbz1 (always 0), byte 3's high bit is
    /// `SE_SELF_RELATIVE` (almost always set for stored SDs). That's a 4-byte
    /// fingerprint stronger than a magic number even though it's not literal.
    SecurityDescriptor,
}

impl ExpectedMagic {
    /// Predict the magic for a non-resident attribute, given the MFT record that
    /// **owns** it (i.e. the base record number, chasing `$ATTRIBUTE_LIST` for
    /// extension records). `None` for attributes whose content is opaque (user
    /// `$DATA`, `$Secure $DATA $SDS`, etc.) — no useful prediction.
    fn from_attr(owning_rec: u64, attr_type: u32) -> Option<Self> {
        match attr_type {
            0xa0 => Some(Self::Indx),
            // $MFT (rec 0) and $MFTMirr (rec 1) both store FILE records.
            0x80 if owning_rec == 0 || owning_rec == 1 => Some(Self::File),
            0x80 if owning_rec == 2 => Some(Self::Logfile),
            // Per-file embedded security descriptor (old-style NTFS).
            0x50 => Some(Self::SecurityDescriptor),
            _ => None,
        }
    }

    /// Classify an observed magic against the predictable types.
    fn from_observed(magic: &[u8; 4]) -> Option<Self> {
        if magic == b"INDX" {
            Some(Self::Indx)
        } else if magic == b"FILE" {
            Some(Self::File)
        } else if magic == b"RSTR" || magic == b"RCRD" {
            Some(Self::Logfile)
        } else if magic[0] == 0x01 && magic[1] == 0x00 && (magic[3] & 0x80) != 0 {
            // SD revision 1 + sbz1=0 + SE_SELF_RELATIVE control bit. Byte 2
            // (control low) varies; byte 3 (control high) is 0x80 or
            // 0x80|other-bits.
            Some(Self::SecurityDescriptor)
        } else {
            None
        }
    }
}

/// Read up to `out.len()` bytes directly from `file_offset` — bypassing the
/// `lcn → run` index lookup so an overlapping-runs ambiguity doesn't corrupt
/// the result. Handles both uncompressed (read straight from the `SpanReader`)
/// and zlib-compressed GHOs (route through `NtfsDecompressingReader`). Returns
/// the number of bytes actually read; short reads (or 0) signal EOF / missing
/// decompressed coverage for that offset.
fn peek_run_bytes(
    inner: &mut SpanReader,
    compressed: Option<&mut NtfsCompressedState>,
    file_offset: u64,
    out: &mut [u8],
) -> usize {
    if let Some(cs) = compressed {
        if file_offset >= cs.total_decompressed {
            return 0;
        }
        let mut rdr = NtfsDecompressingReader {
            inner,
            state: cs,
            position: file_offset,
        };
        let mut got = 0;
        while got < out.len() {
            match rdr.read(&mut out[got..]) {
                Ok(0) => break,
                Ok(n) => got += n,
                Err(_) => break,
            }
        }
        return got;
    }
    if inner.seek(SeekFrom::Start(file_offset)).is_err() {
        return 0;
    }
    let mut got = 0;
    while got < out.len() {
        match inner.read(&mut out[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(_) => break,
        }
    }
    got
}

/// Per-run expected content, derived from the MFT.
#[derive(Debug, Clone, Copy)]
struct TypedExpect {
    /// Run length in clusters.
    cc: u64,
    /// Predicted content magic, or `None` for opaque attribute kinds.
    magic: Option<ExpectedMagic>,
    /// For `Indx`, the expected `self_vcn` of the FIRST INDX block in this run.
    /// (Each subsequent INDX block self_vcn is `+= idx_record_size / cluster_size`,
    /// which is `1` on all volumes we've seen since idx_record_size == cluster_size.)
    expected_vcn: u64,
}

/// Post-scan content-aware index rebuild.
///
/// The streaming matcher in [`index_ntfs_file_aware`] / [`extend_ntfs_compressed_index`]
/// disambiguates same-`cc` MFT entries by MFT-record order alone. When Ghost's
/// stored stream order doesn't match MFT-record order — the dominant ambiguity
/// is `cc=2` (the default initial `$INDEX_ALLOCATION` allocation) — runs land
/// at the wrong `lcn_start` and the reader serves other files' bytes at those
/// LCNs. chkdsk then sees garbage `INDX` blocks, flags directories as corrupt,
/// and (with `/f`) orphans thousands of files.
///
/// **Strategy.** Instead of trying to patch the matcher's mistakes via swaps
/// (which are hard to get right when same-`cc` entries are pervasive and the
/// matcher's index has *overlapping* LCN ranges), this pass treats the MFT as
/// ground truth and rebuilds `index.runs` from scratch:
///
/// 1. **MFT scan.** Walk every FILE record; for each non-resident attribute,
///    record `(lcn, cc, magic, expected_vcn)`. Magic is INDX for
///    `$INDEX_ALLOCATION`, FILE for `$MFT $DATA`, RSTR|RCRD for `$LogFile
///    $DATA`, `None` for opaque attrs (user `$DATA`, `$Secure $DATA $SDS`,
///    etc.). The base-record id (FILE header offset 0x20) resolves
///    `$ATTRIBUTE_LIST` extension records back to the owning base — that's how
///    `$Secure`'s `$SDH` index runs (which live in an extension record, not
///    rec 9 itself) get the correct INDX expectation.
/// 2. **Observe.** For every run in the existing `index.runs`, peek the first
///    24 bytes directly from its `file_offset` (bypassing the binary_search-on-
///    LCN lookup, which is what overlapping runs corrupt). Record kind + INDX
///    `self_vcn`. The (file_offset, kind, self_vcn, cluster_count) tuple is
///    what we have to *place*.
/// 3. **Assign.** For each MFT entry of cc=K, find the observed run with cc=K
///    whose content matches the entry's predicted magic + (for INDX) vcn. That
///    run's `file_offset` becomes the new entry's `file_offset` at `lcn = entry.lcn`.
///    For entries with no magic prediction (user data), match by cc alone in
///    MFT order — same heuristic as the original matcher but applied at the
///    end, after all magic-typed assignments have claimed their slots.
/// 4. **Replace.** Drop the old `index.runs` entirely; substitute the rebuilt
///    list. Re-sort by `lcn_start`. The new list has *no overlaps* because
///    every entry came from a distinct MFT data-run declaration, and NTFS
///    guarantees non-overlapping cluster claims within a well-formed volume.
///
/// The MFT run itself (rec 0 fragment 0 at `vbr.mft_cluster`) is preserved
/// verbatim — it was set explicitly at scan start, never via the ambiguous cc
/// queue, so it's already correct.
///
/// Conservative for opaque attrs: when MFT order disagrees with stream order,
/// the user-`$DATA` portion may still be cross-linked. But every metadata
/// stream (`$MFT`, `$LogFile`, every `$INDEX_ALLOCATION`) is placed by content,
/// which is what chkdsk reads.
fn fixup_ntfs_typed_misalignments(
    inner: &mut SpanReader,
    index: &mut NtfsGhoIndex,
    compressed: &mut Option<NtfsCompressedState>,
    vbr: &crate::fs::ntfs::NtfsVbr,
) {
    use crate::fs::ntfs::{apply_fixup, parse_mft_attributes};

    let cluster_size = index.cluster_size;
    let mft_record_size = vbr.mft_record_size as usize;

    let Some(mft_run) = index
        .runs
        .iter()
        .find(|r| r.lcn_start == vbr.mft_cluster)
        .cloned()
    else {
        log::warn!("NTFS fixup: MFT run not found in index, skipping");
        return;
    };

    // The MFT's first fragment is only part of the MFT — read $MFT (rec 0) to
    // discover the total size via its $DATA $DATA attribute. Without this we'd
    // miss every MFT record beyond the first fragment, which on this corpus is
    // ~3 % of the table.
    let mut rec0 = vec![0u8; mft_record_size];
    {
        let mft_pos = vbr.mft_cluster * cluster_size;
        let mut total = 0;
        let mut hint = 0usize;
        while total < mft_record_size {
            let n = match read_ntfs_file_aware_into(
                inner,
                index,
                &mut hint,
                compressed,
                mft_pos + total as u64,
                &mut rec0[total..],
            ) {
                Ok(n) => n,
                Err(_) => break,
            };
            if n == 0 {
                break;
            }
            total += n;
        }
    }
    let _ = apply_fixup(&mut rec0, vbr.bytes_per_sector);
    // $MFT's own $DATA runs, as (vcn_start, cluster_count, lcn_start) in order.
    // Used to place FILE-magic MFT-fragment runs deterministically by content:
    // an MFT fragment's first record carries its own record number, which fixes
    // the fragment's VCN, and these runs map that VCN to its LCN.
    let mut mft_data_runs: Vec<(u64, u64, u64)> = Vec::new();
    let mft_total_bytes = {
        let attrs = parse_mft_attributes(&rec0, vbr.mft_record_size);
        let mut total = 0u64;
        for a in &attrs {
            if !a.resident && a.attr_type == 0x80 {
                let mut vcn = a.starting_vcn;
                for dr in &a.data_runs {
                    if dr.cluster_offset > 0 && dr.length > 0 {
                        mft_data_runs.push((vcn, dr.length, dr.cluster_offset as u64));
                    }
                    vcn += dr.length;
                    total += dr.length * cluster_size;
                }
            }
        }
        if total == 0 {
            mft_run.cluster_count * cluster_size
        } else {
            total
        }
    };
    // Map an absolute $MFT VCN to its on-disk LCN via the $MFT $DATA runs.
    let mft_vcn_to_lcn = |vcn: u64| -> Option<u64> {
        for &(vstart, len, lcn) in &mft_data_runs {
            if vcn >= vstart && vcn < vstart + len {
                return Some(lcn + (vcn - vstart));
            }
        }
        None
    };
    let num_records = mft_total_bytes / mft_record_size as u64;
    log::info!(
        "NTFS fixup: scanning {num_records} MFT records ({} bytes total MFT)",
        mft_total_bytes
    );

    // Step 1: collect MFT entries — ALL non-resident attribute data runs.
    // Each entry will become an `index.runs` slot in the rebuilt index.
    //
    // We build TWO views:
    //   1. `mft_entries: Vec<(lcn, TypedExpect)>` — flat, sorted+deduped by
    //      `(lcn, cc)`. Used by the content-based opaque fallback.
    //   2. `mft_attrs: Vec<Vec<MftFragment>>` — grouped per non-resident
    //      attribute *in MFT-walk order*, preserving emission order. Each
    //      inner Vec is one attribute's fragments (one entry per data run).
    //      This is what gets matched to `stream_attrs` by position.
    struct MftFragment {
        lcn: u64,
        cc: u64,
        magic: Option<ExpectedMagic>,
        expected_vcn: u64,
    }
    let mut mft_entries: Vec<(u64, TypedExpect)> = Vec::new();
    let mut mft_attrs: Vec<Vec<MftFragment>> = Vec::new();
    // Deterministic INDX placement: map (owning_dir_rec, vcn) -> lcn for every
    // $INDEX_ALLOCATION ($I30) fragment. An INDX block in the stream is
    // self-identifying — its index entries carry the parent directory's MFT
    // reference — so we can place each one at exactly the right LCN regardless
    // of cluster-count collisions or stream emission order. Keyed only for the
    // $I30 (directory) index, which is what chkdsk's index checks cover.
    let mut indx_dest: std::collections::HashMap<(u64, u64), u64> =
        std::collections::HashMap::new();
    // Per-FILE $DATA runlist, merged across $ATTRIBUTE_LIST extension records:
    // owning_rec -> Vec<(vcn, lcn, cc)>. A fragmented file's $DATA runs are
    // described across several MFT records, but in the stream they're emitted as
    // one seq-numbered run sequence. Sorting these by vcn reconstructs the
    // file's fragment order, so a sentinel run's seq indexes straight into it.
    // This is what lets sentinel recovery place a hive's interior clusters.
    let mut file_data_frags: std::collections::HashMap<u64, Vec<(u64, u64, u64)>> =
        std::collections::HashMap::new();
    // base record -> file name, for the residual-hole report logged at the end.
    let mut rec_names: std::collections::HashMap<u64, String> = std::collections::HashMap::new();
    // $MFTMirr (rec 1) $DATA — pinned to map onto $MFT's data, never matched to
    // a stream run (Ghost omits the mirror's content; it's a verbatim copy of
    // $MFT's first records). Captured here, re-added at reassembly.
    let mut pinned_mftmirr: Option<(u64, u64)> = None;

    let mut rec_buf = vec![0u8; mft_record_size];
    let mut hint = 0usize;
    for rec_idx in 0..num_records {
        let mft_pos = vbr.mft_cluster * cluster_size + rec_idx * mft_record_size as u64;
        let mut total = 0;
        while total < mft_record_size {
            let n = match read_ntfs_file_aware_into(
                inner,
                index,
                &mut hint,
                compressed,
                mft_pos + total as u64,
                &mut rec_buf[total..],
            ) {
                Ok(n) => n,
                Err(_) => break,
            };
            if n == 0 {
                break;
            }
            total += n;
        }
        if total < mft_record_size || &rec_buf[0..4] != b"FILE" {
            continue;
        }
        let flags = u16::from_le_bytes([rec_buf[22], rec_buf[23]]);
        if flags & 0x01 == 0 {
            continue;
        }
        let base_ref = u64::from_le_bytes([
            rec_buf[0x20],
            rec_buf[0x21],
            rec_buf[0x22],
            rec_buf[0x23],
            rec_buf[0x24],
            rec_buf[0x25],
            rec_buf[0x26],
            rec_buf[0x27],
        ]);
        let base_rec = base_ref & 0x0000_FFFF_FFFF_FFFF;
        let self_rec =
            u32::from_le_bytes([rec_buf[44], rec_buf[45], rec_buf[46], rec_buf[47]]) as u64;
        let owning_rec = if base_rec != 0 { base_rec } else { self_rec };
        // Skip rec 8 ($BadClus, sparse all-volume run) and rec 0 fragment 0
        // (the MFT first run — added separately at the end).
        if self_rec == 8 {
            continue;
        }
        let _ = apply_fixup(&mut rec_buf, vbr.bytes_per_sector);
        let attrs = parse_mft_attributes(&rec_buf, vbr.mft_record_size);
        // Capture the file name from a base record's resident $FILE_NAME (0x30)
        // for the residual-hole report. Value layout: parent ref (8) ... name
        // length (1) @0x40, name (UTF-16LE) @0x42.
        if base_ref == 0 {
            for a in &attrs {
                if a.attr_type == 0x30 && a.resident && a.value.len() >= 0x42 {
                    let nl = a.value[0x40] as usize;
                    if 0x42 + nl * 2 <= a.value.len() {
                        let name: String = (0..nl)
                            .filter_map(|i| {
                                char::from_u32(u16::from_le_bytes([
                                    a.value[0x42 + i * 2],
                                    a.value[0x42 + i * 2 + 1],
                                ]) as u32)
                            })
                            .collect();
                        rec_names.entry(self_rec).or_insert(name);
                    }
                    break;
                }
            }
        }
        for attr in &attrs {
            if attr.resident || attr.data_runs.is_empty() {
                continue;
            }
            let magic = ExpectedMagic::from_attr(owning_rec, attr.attr_type);
            // $MFTMirr (rec 1) $DATA: pin it, don't match it to a stream run.
            // Its content is synthesized from $MFT's data (see below).
            if self_rec == 1 && attr.attr_type == 0x80 {
                if pinned_mftmirr.is_none() {
                    if let Some(dr) = attr
                        .data_runs
                        .iter()
                        .find(|dr| dr.cluster_offset > 0 && dr.length > 0)
                    {
                        pinned_mftmirr = Some((dr.cluster_offset as u64, dr.length));
                    }
                }
                continue;
            }
            // Rec 0's $DATA fragment 0 is the MFT first run, set explicitly at
            // scan time. Don't include it as an entry (we'll add it back below).
            let skip_first = self_rec == 0 && attr.attr_type == 0x80;
            let mut vcn = attr.starting_vcn;
            let mut attr_frags: Vec<MftFragment> = Vec::new();
            for (i, dr) in attr.data_runs.iter().enumerate() {
                let len = dr.length;
                if dr.cluster_offset > 0 && len > 0 && !(skip_first && i == 0) {
                    let lcn = dr.cluster_offset as u64;
                    mft_entries.push((
                        lcn,
                        TypedExpect {
                            cc: len,
                            magic,
                            expected_vcn: vcn,
                        },
                    ));
                    attr_frags.push(MftFragment {
                        lcn,
                        cc: len,
                        magic,
                        expected_vcn: vcn,
                    });
                    // Index every $INDEX_ALLOCATION (0xa0) fragment's destination
                    // per VCN. $Secure (rec 9) owns $SII/$SDH which aren't
                    // directory ($I30) indexes; their entries have no parent ref,
                    // so the content lookup just won't match them — excluding
                    // rec 9 keeps the map clean. Each VCN of the attribute maps to
                    // its own LCN (a cc>1 fragment covers several consecutive VCNs).
                    if attr.attr_type == 0xa0 && owning_rec != 9 {
                        for off in 0..len {
                            indx_dest.insert((owning_rec, vcn + off), lcn + off);
                        }
                    }
                    // Accumulate $DATA fragments per owning file (skip $MFT,
                    // rec 0, served separately) for sentinel recovery.
                    if attr.attr_type == 0x80 && owning_rec != 0 {
                        file_data_frags
                            .entry(owning_rec)
                            .or_default()
                            .push((vcn, lcn, len));
                    }
                }
                vcn += len;
            }
            if !attr_frags.is_empty() {
                mft_attrs.push(attr_frags);
            }
        }
    }

    // Dedupe MFT entries by (lcn, cc, magic, expected_vcn). $ATTRIBUTE_LIST can
    // cause the same attribute to surface from both the base record and an
    // extension; we only want one entry per (lcn, cc) pair.
    mft_entries.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(a.1.cc.cmp(&b.1.cc))
            .then(a.1.expected_vcn.cmp(&b.1.expected_vcn))
    });
    mft_entries.dedup_by(|a, b| a.0 == b.0 && a.1.cc == b.1.cc);
    log::info!(
        "NTFS fixup: {} unique MFT entries (after dedup)",
        mft_entries.len()
    );

    // Step 2: observe every existing run's actual content (peek at file_offset).
    // Skip the MFT run itself — it's preserved separately.
    #[derive(Debug, Clone, Copy)]
    struct ObservedRun {
        file_offset: u64,
        cc: u64,
        kind: Option<ExpectedMagic>,
        self_vcn: Option<u64>,
        /// Stream-side fragment index captured during the lazy scan. `u64::MAX`
        /// for entries whose seq wasn't recorded (uncompressed path TODO).
        seq: u64,
    }
    let mut observed_runs: Vec<ObservedRun> = Vec::with_capacity(index.runs.len());
    let mut order: Vec<usize> = (0..index.runs.len()).collect();
    order.sort_by_key(|&i| index.runs[i].file_offset);
    for run_i in order {
        let r = &index.runs[run_i];
        if r.lcn_start == vbr.mft_cluster {
            continue;
        }
        // Skip the synthesized $MFTMirr run: it shares the MFT's file_offset
        // (it maps onto $MFT's data) and would otherwise be observed as a
        // phantom FILE-magic run and mis-matched. It's re-added at reassembly.
        if r.file_offset == mft_run.file_offset {
            continue;
        }
        let mut buf = [0u8; 24];
        let got = peek_run_bytes(inner, compressed.as_mut(), r.file_offset, &mut buf);
        if got < 4 {
            continue;
        }
        let kind = ExpectedMagic::from_observed(&[buf[0], buf[1], buf[2], buf[3]]);
        let self_vcn = if kind == Some(ExpectedMagic::Indx) && got >= 0x18 {
            Some(u64::from_le_bytes([
                buf[0x10], buf[0x11], buf[0x12], buf[0x13], buf[0x14], buf[0x15], buf[0x16],
                buf[0x17],
            ]))
        } else {
            None
        };
        observed_runs.push(ObservedRun {
            file_offset: r.file_offset,
            cc: r.cluster_count,
            kind,
            self_vcn,
            seq: r.seq,
        });
    }
    // observed_runs is now in stream order (sorted by file_offset). Group it
    // into stream-attributes by seq=0 boundaries: a new attribute starts every
    // time seq drops back to 0. Each group is one MFT non-resident attribute's
    // fragment list in emission order.
    let mut stream_attrs: Vec<Vec<usize>> = Vec::new(); // groups of observed_runs indices
    {
        let mut current: Vec<usize> = Vec::new();
        for (i, o) in observed_runs.iter().enumerate() {
            if o.seq == 0 {
                if !current.is_empty() {
                    stream_attrs.push(std::mem::take(&mut current));
                }
                current.push(i);
            } else if o.seq == u64::MAX {
                // Uncompressed-path sentinel — flush as a standalone group so
                // it still gets matched by fragment-shape (which is just [cc]).
                if !current.is_empty() {
                    stream_attrs.push(std::mem::take(&mut current));
                }
                stream_attrs.push(vec![i]);
            } else {
                current.push(i);
            }
        }
        if !current.is_empty() {
            stream_attrs.push(current);
        }
    }
    log::info!(
        "NTFS fixup: grouped {} observed runs into {} stream-attributes",
        observed_runs.len(),
        stream_attrs.len()
    );
    let mut indx_obs = 0;
    let mut file_obs = 0;
    let mut log_obs = 0;
    let mut sd_obs = 0;
    let mut other_obs = 0;
    for o in &observed_runs {
        match o.kind {
            Some(ExpectedMagic::Indx) => indx_obs += 1,
            Some(ExpectedMagic::File) => file_obs += 1,
            Some(ExpectedMagic::Logfile) => log_obs += 1,
            Some(ExpectedMagic::SecurityDescriptor) => sd_obs += 1,
            None => other_obs += 1,
        }
    }
    log::info!(
        "NTFS fixup: {} runs observed (INDX={} FILE={} LOGFILE={} SD={} other={})",
        observed_runs.len(),
        indx_obs,
        file_obs,
        log_obs,
        sd_obs,
        other_obs
    );

    // Step 3: assign observed runs to MFT entries.
    //
    // Pass 0 (seq-based) — the deterministic path. `stream_attrs[k]` is the
    // k-th attribute Ghost emitted; `mft_attrs[k]` is the k-th non-resident
    // attribute we found walking the MFT. Their fragments line up
    // one-to-one IF the emission order matches MFT-walk order AND the
    // fragment counts agree. When a mismatch is detected (different fragment
    // count or magic disagreement on the first fragment) we BAIL on seq
    // matching for that attribute and let Pass 1 / Pass 2 handle its runs as
    // before — that's the fallback for `$ATTRIBUTE_LIST`-reordered streams
    // or any other emission-order surprise.
    //
    // Pass 1 — magic-typed entries (content-based fallback). For each MFT
    // entry with a magic prediction (INDX/FILE/LOGFILE), find an unused
    // observed run whose cc + kind + (for INDX) self_vcn match.
    //
    // Pass 2 — opaque entries (user $DATA, $Secure $DATA $SDS, etc.).
    // Preserve the matcher's original `(lcn, cc) → file_offset` choice.
    let mut used = vec![false; observed_runs.len()];
    let mut assignments: Vec<(u64, u64, u64, u64)> = Vec::new(); // (lcn, cc, file_offset, seq)
    let mut typed_matched = 0usize;
    let mut typed_unmatched = 0usize;
    let mut seq_matched = 0usize;
    let mut seq_bailouts = 0usize;
    // (lcn, cc) pairs already assigned, shared across all passes so none
    // double-assign. Maintained incrementally from the first (INDX-content) pass
    // onward.
    let mut claimed: std::collections::HashSet<(u64, u64)> = std::collections::HashSet::new();

    // Pass -1 (INDX content): deterministic directory-index placement, run
    // before everything else. Every $I30 INDX block names its parent directory
    // in its first index entry's $FILE_NAME key; combined with the block's
    // self-VCN that uniquely identifies the destination LCN (`indx_dest`),
    // independent of cluster-count collisions and stream emission order — the
    // failure mode that desynced positional seq-matching on large volumes and
    // produced chkdsk "index entry ... is incorrect" / "incorrectly sorted"
    // errors en masse. INDX blocks that can't be identified (empty nodes, or
    // non-$I30 indexes whose keys aren't $FILE_NAME) fall through to the later
    // passes unchanged.
    let mut indx_content_matched = 0usize;
    {
        let mut blk = vec![0u8; 8192];
        for i in 0..observed_runs.len() {
            if used[i] || observed_runs[i].kind != Some(ExpectedMagic::Indx) {
                continue;
            }
            let fo = observed_runs[i].file_offset;
            let got = peek_run_bytes(inner, compressed.as_mut(), fo, &mut blk);
            if got < 0x20 || &blk[0..4] != b"INDX" {
                continue;
            }
            let self_vcn = u64::from_le_bytes(blk[0x10..0x18].try_into().unwrap());
            // INDEX_NODE_HEADER sits at 0x18; its first-entry offset is relative
            // to the header start (0x18).
            let first_off = 0x18 + u32::from_le_bytes(blk[0x18..0x1C].try_into().unwrap()) as usize;
            if first_off + 0x18 > got {
                continue;
            }
            let entry_flags = u16::from_le_bytes([blk[first_off + 0x0C], blk[first_off + 0x0D]]);
            let key_len =
                u16::from_le_bytes([blk[first_off + 0x0A], blk[first_off + 0x0B]]) as usize;
            // 0x02 = last/end entry. With no key it's an empty node — unidentifiable.
            if entry_flags & 0x02 != 0 || key_len < 8 {
                continue;
            }
            // The key is a $FILE_NAME; its first 8 bytes are the parent dir ref.
            let pr_off = first_off + 0x10;
            if pr_off + 8 > got {
                continue;
            }
            let parent_ref = u64::from_le_bytes(blk[pr_off..pr_off + 8].try_into().unwrap())
                & 0x0000_FFFF_FFFF_FFFF;
            if let Some(&lcn) = indx_dest.get(&(parent_ref, self_vcn)) {
                let cc = observed_runs[i].cc;
                if !claimed.contains(&(lcn, cc)) {
                    used[i] = true;
                    assignments.push((lcn, cc, fo, observed_runs[i].seq));
                    claimed.insert((lcn, cc));
                    indx_content_matched += 1;
                }
            }
        }
    }
    log::info!(
        "NTFS fixup: Pass -1 (INDX content) — {indx_content_matched} directory-index blocks placed"
    );

    // Pass -1b (MFT-fragment content): place FILE-magic runs — the $MFT's own
    // later $DATA fragments — deterministically. Each fragment is a sequence of
    // FILE records, and every record stores its own record number at offset
    // 0x2C. The first record's number fixes the fragment's $MFT VCN, which the
    // $MFT $DATA runs map to an LCN. This avoids the cc-based ambiguity that
    // otherwise swaps same-size MFT fragments and leaves a few records reading
    // as zeros / foreign data ("Attribute record (80) is corrupt" in chkdsk).
    let mut mft_frag_matched = 0usize;
    {
        let mut hdr = [0u8; 0x30];
        for i in 0..observed_runs.len() {
            if used[i] || observed_runs[i].kind != Some(ExpectedMagic::File) {
                continue;
            }
            let fo = observed_runs[i].file_offset;
            let got = peek_run_bytes(inner, compressed.as_mut(), fo, &mut hdr);
            if got < 0x30 || &hdr[0..4] != b"FILE" {
                continue;
            }
            // Record number this fragment starts at (offset 0x2C).
            let first_rec = u32::from_le_bytes([hdr[0x2C], hdr[0x2D], hdr[0x2E], hdr[0x2F]]) as u64;
            // VCN of that record within $MFT, then its LCN via the $MFT runs.
            let vcn = first_rec * mft_record_size as u64 / cluster_size;
            if let Some(lcn) = mft_vcn_to_lcn(vcn) {
                let cc = observed_runs[i].cc;
                if !claimed.contains(&(lcn, cc)) {
                    used[i] = true;
                    assignments.push((lcn, cc, fo, observed_runs[i].seq));
                    claimed.insert((lcn, cc));
                    mft_frag_matched += 1;
                }
            }
        }
    }
    log::info!(
        "NTFS fixup: Pass -1b (MFT-fragment content) — {mft_frag_matched} $MFT fragments placed"
    );

    // Pass 0: seq-based attribute alignment.
    let n_pairs = stream_attrs.len().min(mft_attrs.len());
    for k in 0..n_pairs {
        let s_idxs = &stream_attrs[k];
        let mft_frags = &mft_attrs[k];
        // Skip groups whose runs or destinations the INDX-content pass already
        // claimed — re-committing them here would double-assign.
        if s_idxs.iter().any(|&si| used[si])
            || mft_frags.iter().any(|f| claimed.contains(&(f.lcn, f.cc)))
        {
            continue;
        }
        // Bail if the shapes don't agree — different fragment counts or
        // cc mismatch on a fragment means Ghost's emission order isn't what
        // we assumed for this attribute.
        if s_idxs.len() != mft_frags.len() {
            seq_bailouts += 1;
            continue;
        }
        let mut shape_ok = true;
        for (j, &si) in s_idxs.iter().enumerate() {
            if observed_runs[si].cc != mft_frags[j].cc {
                shape_ok = false;
                break;
            }
            // Magic disagreement on a fragment whose attribute predicts a
            // specific magic is a strong "this isn't the right attribute"
            // signal. Check fragment 0 (others may genuinely lack magic).
            if j == 0 {
                if let Some(want_m) = mft_frags[j].magic {
                    if observed_runs[si].kind != Some(want_m) {
                        shape_ok = false;
                        break;
                    }
                    if want_m == ExpectedMagic::Indx
                        && observed_runs[si].self_vcn != Some(mft_frags[j].expected_vcn)
                    {
                        shape_ok = false;
                        break;
                    }
                }
            }
        }
        if !shape_ok {
            seq_bailouts += 1;
            continue;
        }
        // Shapes agree — commit each fragment.
        for (j, &si) in s_idxs.iter().enumerate() {
            used[si] = true;
            assignments.push((
                mft_frags[j].lcn,
                mft_frags[j].cc,
                observed_runs[si].file_offset,
                observed_runs[si].seq,
            ));
            claimed.insert((mft_frags[j].lcn, mft_frags[j].cc));
            seq_matched += 1;
        }
    }
    log::info!(
        "NTFS fixup: Pass 0 (seq) — {} fragments matched, {} attrs bailed out",
        seq_matched,
        seq_bailouts
    );

    // Pass 1: typed entries that Pass 0 didn't already claim.
    for (lcn, t) in &mft_entries {
        if claimed.contains(&(*lcn, t.cc)) {
            continue;
        }
        let Some(want_magic) = t.magic else {
            continue;
        };
        let mut found: Option<usize> = None;
        for (i, o) in observed_runs.iter().enumerate() {
            if used[i] {
                continue;
            }
            if o.cc != t.cc {
                continue;
            }
            if o.kind != Some(want_magic) {
                continue;
            }
            if want_magic == ExpectedMagic::Indx && o.self_vcn != Some(t.expected_vcn) {
                continue;
            }
            found = Some(i);
            break;
        }
        if let Some(i) = found {
            used[i] = true;
            assignments.push((
                *lcn,
                t.cc,
                observed_runs[i].file_offset,
                observed_runs[i].seq,
            ));
            claimed.insert((*lcn, t.cc));
            typed_matched += 1;
        } else {
            typed_unmatched += 1;
        }
    }

    // Pass 2: opaque entries — preserve the matcher's original choice.
    let mut original_assignments: std::collections::HashMap<(u64, u64), u64> =
        std::collections::HashMap::with_capacity(index.runs.len());
    for r in &index.runs {
        original_assignments
            .entry((r.lcn_start, r.cluster_count))
            .or_insert(r.file_offset);
    }
    let mut opaque_matched = 0usize;
    let mut opaque_unmatched = 0usize;
    for (lcn, t) in &mft_entries {
        if t.magic.is_some() {
            continue;
        }
        if claimed.contains(&(*lcn, t.cc)) {
            continue;
        }
        if let Some(&fo) = original_assignments.get(&(*lcn, t.cc)) {
            assignments.push((*lcn, t.cc, fo, u64::MAX));
            claimed.insert((*lcn, t.cc));
            opaque_matched += 1;
        } else {
            opaque_unmatched += 1;
        }
    }

    // Also preserve original placement for any TYPED entries that Pass 1
    // couldn't match — better to keep the matcher's guess than to lose the run
    // entirely (would read as zeros and chkdsk would think the file is empty).
    for (lcn, t) in &mft_entries {
        if t.magic.is_none() {
            continue;
        }
        if claimed.contains(&(*lcn, t.cc)) {
            continue;
        }
        if let Some(&fo) = original_assignments.get(&(*lcn, t.cc)) {
            assignments.push((*lcn, t.cc, fo, u64::MAX));
            claimed.insert((*lcn, t.cc));
        }
    }

    // Sentinel recovery. The lazy scan records runs it can't assign an LCN with
    // lcn_start == u64::MAX (it found the run header + decoded the data, but had
    // no candidate (lcn,cc) at scan time — typical for a fragment of a file
    // whose runlist is described in an $ATTRIBUTE_LIST extension record). Their
    // data IS present; only the LCN is missing. Recover each via its
    // stream-attribute neighbours: within a seq-delimited stream attribute, an
    // already-placed (matched) run identifies the owning MFT attribute, and the
    // sentinel is just another fragment of it — its LCN is mft_attrs[k][seq].
    // This is what restores a registry hive's interior $DATA clusters that the
    // scan dropped, without the cc-collision risk of blind matching.
    let orig_lcn_by_fo: std::collections::HashMap<u64, u64> = index
        .runs
        .iter()
        .map(|r| (r.file_offset, r.lcn_start))
        .collect();
    // Finalize per-file runlists: sort each file's $DATA fragments by VCN (=
    // stream emission order) and index every fragment lcn back to its file +
    // position. Position is the fragment's index in the sorted runlist, which
    // equals its `seq` offset from any sibling fragment in the same stream run.
    let mut file_runlist: std::collections::HashMap<u64, Vec<(u64, u64)>> =
        std::collections::HashMap::with_capacity(file_data_frags.len()); // rec -> [(lcn, cc)]
    let mut lcn_to_file_pos: std::collections::HashMap<u64, (u64, usize)> =
        std::collections::HashMap::new(); // lcn -> (owning_rec, index)
    for (rec, mut frags) in file_data_frags.into_iter() {
        frags.sort_by_key(|&(vcn, _, _)| vcn);
        let runs: Vec<(u64, u64)> = frags.iter().map(|&(_, l, c)| (l, c)).collect();
        for (i, &(l, _)) in runs.iter().enumerate() {
            lcn_to_file_pos.entry(l).or_insert((rec, i));
        }
        file_runlist.insert(rec, runs);
    }
    // Walk observed runs in stream (file_offset) emission order. Ghost emits a
    // file's $DATA fragments consecutively, in VCN order, so a matched run
    // identifies the current file + its position in that file's runlist, and the
    // run immediately following is the next fragment. A sentinel sandwiched
    // between a file's matched runs is therefore that file's next runlist entry —
    // recovered with no dependence on the (irregular) seq field. The cc check +
    // anchor reset on any mismatch keep a wrong guess from landing.
    let mut sentinel_recovered = 0usize;
    let mut cur: Option<(u64, usize)> = None; // (owning_rec, next runlist index)
    for (i, o) in observed_runs.iter().enumerate() {
        let olcn = orig_lcn_by_fo
            .get(&o.file_offset)
            .copied()
            .unwrap_or(u64::MAX);
        if olcn != u64::MAX {
            // Matched run: (re)anchor to the file + fragment AFTER this one.
            cur = lcn_to_file_pos.get(&olcn).map(|&(rec, idx)| (rec, idx + 1));
            continue;
        }
        // Sentinel: place it at the current file's next expected fragment.
        if used[i] {
            continue;
        }
        let Some((rec, next_idx)) = cur else { continue };
        let placed = file_runlist
            .get(&rec)
            .and_then(|runs| runs.get(next_idx).copied())
            .filter(|&(lcn, cc)| cc == o.cc && !claimed.contains(&(lcn, cc)));
        match placed {
            Some((lcn, cc)) => {
                assignments.push((lcn, cc, o.file_offset, o.seq));
                claimed.insert((lcn, cc));
                used[i] = true;
                sentinel_recovered += 1;
                cur = Some((rec, next_idx + 1));
            }
            None => cur = None, // shape disagreed — stop trusting this anchor
        }
    }
    log::info!(
        "NTFS fixup: sentinel recovery placed {sentinel_recovered} previously-unmapped runs"
    );

    // Safety net: preserve every observed run the passes above couldn't match
    // to an MFT entry, at its ORIGINAL lazy-scan LCN. The rebuild must never
    // DROP a run the lazy scan found — dropping reads it back as zeros. This
    // happens when a file's $DATA-describing attribute lives in an extension
    // record we couldn't read (a zeroed MFT cluster), so the MFT walk never
    // collected that file's runs and nothing here matches its observed stream
    // run. Large registry hives hit exactly this — losing their first $DATA
    // cluster (the `regf` base block) made the hive unloadable. Keep the lazy
    // placement unless its LCN was already reassigned to a corrected run.
    let mut preserved_unmatched = 0usize;
    for (i, o) in observed_runs.iter().enumerate() {
        if used[i] {
            continue;
        }
        let Some(&orig_lcn) = orig_lcn_by_fo.get(&o.file_offset) else {
            continue;
        };
        if claimed.contains(&(orig_lcn, o.cc)) {
            continue;
        }
        assignments.push((orig_lcn, o.cc, o.file_offset, o.seq));
        claimed.insert((orig_lcn, o.cc));
        used[i] = true;
        preserved_unmatched += 1;
    }

    log::info!(
        "NTFS fixup: typed-matched={typed_matched} typed-unmatched={typed_unmatched} \
         opaque-matched={opaque_matched} opaque-unmatched={opaque_unmatched} \
         preserved-unmatched={preserved_unmatched} unused-observed={}",
        used.iter().filter(|&&u| !u).count()
    );

    // Step 4: rebuild index.runs. Preserve the MFT run unchanged; replace
    // everything else with the new assignments.
    let mut new_runs: Vec<NtfsGhoClusterRun> = Vec::with_capacity(assignments.len() + 2);
    let mft_file_offset = mft_run.file_offset;
    let mft_run_clusters = mft_run.cluster_count;
    new_runs.push(mft_run);
    // Re-add the synthesized $MFTMirr: clusters mapped onto the start of $MFT's
    // data so the mirror reads as a verbatim copy of $MFT's first records.
    if let Some((mirr_lcn, mirr_cc)) = pinned_mftmirr {
        let cc = mirr_cc.min(mft_run_clusters);
        if cc > 0 {
            new_runs.push(NtfsGhoClusterRun {
                lcn_start: mirr_lcn,
                cluster_count: cc,
                file_offset: mft_file_offset,
                seq: 0,
            });
            log::info!(
                "NTFS fixup: re-added synthesized $MFTMirr ({} cluster(s) at lcn {})",
                cc,
                mirr_lcn
            );
        }
    }
    for (lcn, cc, file_offset, seq) in assignments {
        new_runs.push(NtfsGhoClusterRun {
            lcn_start: lcn,
            cluster_count: cc,
            file_offset,
            seq,
        });
    }
    new_runs.sort_by_key(|r| r.lcn_start);
    log::info!(
        "NTFS fixup: rebuilt index, {} runs (was {})",
        new_runs.len(),
        index.runs.len()
    );
    index.runs = new_runs;

    // Residual-hole report. After recovery, any file $DATA cluster the final
    // run set still doesn't cover reads back as zeros in the exported image —
    // genuinely-uncaptured data (Ghost-skipped transient files like
    // hiberfil/pagefile, or runs whose headers the scan never found). These
    // are harmless to mount but represent incomplete file content, so list the
    // worst offenders in the log as a record of what couldn't be restored.
    {
        let mut covered: Vec<(u64, u64)> = index
            .runs
            .iter()
            .filter(|r| r.lcn_start != u64::MAX)
            .map(|r| (r.lcn_start, r.cluster_count))
            .collect();
        covered.sort_by_key(|r| r.0);
        let is_covered = |lcn: u64| -> bool {
            covered
                .binary_search_by(|&(s, l)| {
                    if lcn < s {
                        std::cmp::Ordering::Greater
                    } else if lcn >= s + l {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
                .is_ok()
        };
        let mut holes: Vec<(u64, u64)> = Vec::new(); // (owning_rec, uncovered clusters)
        let mut total_hole = 0u64;
        for (rec, runs) in &file_runlist {
            let mut miss = 0u64;
            for &(lcn, cc) in runs {
                for c in 0..cc {
                    if !is_covered(lcn + c) {
                        miss += 1;
                    }
                }
            }
            if miss > 0 {
                holes.push((*rec, miss));
                total_hole += miss;
            }
        }
        if !holes.is_empty() {
            holes.sort_by(|a, b| b.1.cmp(&a.1));
            log::warn!(
                "NTFS: {} file(s) have zero-filled $DATA holes (no captured data); \
                 {} clusters total. These files are incomplete in the exported image:",
                holes.len(),
                total_hole
            );
            for (rec, miss) in holes.iter().take(40) {
                let name = rec_names
                    .get(rec)
                    .map(|s| s.as_str())
                    .unwrap_or("<name in extension record>");
                log::warn!("  rec#{rec} ({miss} clusters zero-filled): {name}");
            }
            if holes.len() > 40 {
                log::warn!("  ... and {} more file(s)", holes.len() - 40);
            }
        }
    }
}

/// Recover `$Boot`'s first 16 sectors by content-matching the VBR.
///
/// The read path synthesizes the boot region (sector 0 = VBR, rest zeros) to
/// keep partition-detection probes cheap, so the real NTFS boot code (sectors
/// 1-15, the bootstrap that loads NTLDR) is otherwise dropped. Ghost does store
/// `$Boot`, and `$Boot`'s sector 0 is byte-identical to the volume VBR, so the
/// `$Boot` run is the one whose first 512 bytes equal the VBR. Matching by
/// content (rather than trusting the cluster-count LCN mapping) is robust to any
/// run-mapping ambiguity. Runs are checked in stream order (`$Boot` is stored
/// early) and the search stops at the first match. Returns up to 8 KiB (16
/// sectors) of boot code, or `None` if no match (caller falls back to zeros).
///
/// Requires a complete run index (call after the lazy scan finishes).
fn recover_ntfs_boot_code(
    inner: &mut SpanReader,
    index: &NtfsGhoIndex,
    compressed: &mut Option<NtfsCompressedState>,
) -> Option<Vec<u8>> {
    const BOOT_BYTES: usize = 16 * 512;
    let mut order: Vec<&NtfsGhoClusterRun> = index.runs.iter().collect();
    order.sort_by_key(|r| r.file_offset);

    let mut head = [0u8; 512];
    for run in order {
        let read_head = |inner: &mut SpanReader,
                         compressed: &mut Option<NtfsCompressedState>,
                         head: &mut [u8; 512]|
         -> bool {
            if let Some(cs) = compressed.as_mut() {
                let mut rdr = NtfsDecompressingReader {
                    inner,
                    state: cs,
                    position: run.file_offset,
                };
                rdr.read_exact(head).is_ok()
            } else {
                inner.seek(SeekFrom::Start(run.file_offset)).is_ok()
                    && inner.read_exact(head).is_ok()
            }
        };
        if !read_head(inner, compressed, &mut head) {
            continue;
        }
        if head == index.vbr {
            let avail = (run.cluster_count * index.cluster_size) as usize;
            let want = BOOT_BYTES.min(avail);
            let mut buf = vec![0u8; want];
            let ok = if let Some(cs) = compressed.as_mut() {
                let mut rdr = NtfsDecompressingReader {
                    inner,
                    state: cs,
                    position: run.file_offset,
                };
                rdr.read_exact(&mut buf).is_ok()
            } else {
                inner.seek(SeekFrom::Start(run.file_offset)).is_ok()
                    && inner.read_exact(&mut buf).is_ok()
            };
            if ok {
                return Some(buf);
            }
        }
    }
    None
}

/// Bytes per sector from a parsed NTFS VBR (offset 0x0B), defaulting to 512
/// when the field is zero/garbage.
fn ntfs_vbr_sector_size(vbr: &[u8; 512]) -> u64 {
    let s = u16::from_le_bytes([vbr[11], vbr[12]]) as u64;
    if s == 0 {
        512
    } else {
        s
    }
}

/// Total served length of the reconstructed NTFS volume in bytes.
///
/// `index.volume_size` is the NTFS-addressable volume (`total_sectors * bps`),
/// which deliberately EXCLUDES the final sector. NTFS places a copy of the VBR
/// (the backup boot sector) in that excluded final sector, so the real volume —
/// and therefore the partition we export it into — is one sector longer. The
/// read path serves that trailing sector from the VBR (see the backup-boot-sector
/// branch in `read_ntfs_file_aware_into`); without the `+1` the exported volume
/// is a sector short and the backup boot sector lands at the wrong offset, which
/// chkdsk and the Windows boot path both flag.
fn ntfs_served_size(index: &NtfsGhoIndex) -> u64 {
    index.volume_size + ntfs_vbr_sector_size(&index.vbr)
}

/// Read bytes from an NTFS file-aware GHO at `position` in the logical
/// volume. Uses binary search over the sorted cluster run index.
fn read_ntfs_file_aware_into(
    inner: &mut SpanReader,
    index: &mut NtfsGhoIndex,
    last_run_hint: &mut usize,
    compressed: &mut Option<NtfsCompressedState>,
    position: u64,
    out: &mut [u8],
) -> std::io::Result<usize> {
    if out.is_empty() {
        return Ok(0);
    }

    let cluster_size = index.cluster_size;
    let lcn = position / cluster_size;
    let offset_in_cluster = (position % cluster_size) as usize;
    let avail_in_cluster = cluster_size as usize - offset_in_cluster;
    let mut to_read = out.len().min(avail_in_cluster);

    // NTFS mirrors the VBR in the backup boot sector, the ONE sector that sits
    // immediately past the addressable volume (`total_sectors` excludes it; see
    // `ntfs_served_size`). It isn't part of any stored run. Clamp normal reads so
    // they stop at that boundary; the read that then starts there serves the VBR
    // copy below. Without the clamp a sequential read straddling the boundary
    // would serve the whole (unmapped) tail as zeros, blanking the backup sector.
    let sector_size = ntfs_vbr_sector_size(&index.vbr);
    let backup_start = index.volume_size;
    if position < backup_start && position + to_read as u64 > backup_start {
        to_read = (backup_start - position) as usize;
    }

    // $Boot region (first 16 sectors, LCN 0). Sector 0 is always the
    // authoritative GHPR-embedded VBR (this also keeps partition-detection
    // probes cheap). Sectors 1-15 are the NTFS boot code: served from
    // `index.boot_code` when an export prepared it (recover_ntfs_boot_code),
    // else zeros — the volume still mounts, just won't self-boot.
    const SYNTH_BOOT_BYTES: u64 = 16 * 512; // 16 sectors = RDB scan range
    if position < SYNTH_BOOT_BYTES {
        let pos = position as usize;
        let n = to_read.min(SYNTH_BOOT_BYTES as usize - pos);
        for (i, b) in out[..n].iter_mut().enumerate() {
            let fo = pos + i;
            *b = if fo < 512 {
                index.vbr[fo]
            } else {
                index.boot_code.get(fo).copied().unwrap_or(0)
            };
        }
        return Ok(n);
    }

    // Backup boot sector: serve the VBR copy for the trailing sector at
    // `volume_size` (synthesized from the GHPR VBR; see the clamp above). The
    // served volume runs to `volume_size + sector_size` (`ntfs_served_size`), so
    // this is the genuine last sector of the partition — where NTFS expects it.
    if position >= backup_start && position < index.volume_size + sector_size {
        let in_sector = (position - backup_start) as usize;
        let n = to_read.min(sector_size as usize - in_sector);
        for (i, b) in out[..n].iter_mut().enumerate() {
            *b = index.vbr.get(in_sector + i).copied().unwrap_or(0);
        }
        return Ok(n);
    }

    // Serve the synthesized $Bitmap. Ghost omits $Bitmap content from the
    // backup, so its LCNs have no mapped run and would otherwise read as zeros
    // (an all-free bitmap → strict NTFS drivers reject the volume). Serve the
    // bitmap we build from the complete run set instead.
    if index
        .bitmap_runs
        .iter()
        .any(|&(l, c)| lcn >= l && lcn < l + c)
    {
        // Build it on first access. For the compressed path the run set isn't
        // complete until the lazy scan finishes, so force it to EOF first.
        if index.synth_bitmap.is_empty() {
            if let Some(cs) = compressed.as_mut() {
                if cs.lazy_scan.is_some() {
                    extend_ntfs_compressed_index(inner, index, cs, u64::MAX);
                }
            }
            let total_clusters = index.volume_size / index.cluster_size;
            let mut allocation: Vec<(u64, u64)> = index
                .runs
                .iter()
                .map(|r| (r.lcn_start, r.cluster_count))
                .collect();
            allocation.extend_from_slice(&index.bitmap_runs);
            index.synth_bitmap = build_synth_ntfs_bitmap(&allocation, total_clusters);
            log::info!(
                "Synthesized $Bitmap on demand: {} bytes from {} runs for {} clusters",
                index.synth_bitmap.len(),
                index.runs.len(),
                total_clusters,
            );
            // The scan is now complete and the bitmap built; refresh the cache
            // so subsequent opens also reuse the synthesized $Bitmap.
            if let Some(cs) = compressed.as_ref() {
                if let Some(key) = cs.cache_key.clone() {
                    ntfs_scan_cache_store(&key, index, cs);
                }
            }
        }
        let mut base = 0u64; // cluster offset within the $Bitmap file
        for &(blcn, bcnt) in &index.bitmap_runs {
            if lcn >= blcn && lcn < blcn + bcnt {
                let file_off =
                    ((base + (lcn - blcn)) * cluster_size + offset_in_cluster as u64) as usize;
                for (i, b) in out[..to_read].iter_mut().enumerate() {
                    *b = index.synth_bitmap.get(file_off + i).copied().unwrap_or(0);
                }
                return Ok(to_read);
            }
            base += bcnt;
        }
    }

    let find_run = |runs: &[NtfsGhoClusterRun], hint: usize| -> Option<usize> {
        if hint < runs.len() {
            let h = &runs[hint];
            if lcn >= h.lcn_start && lcn < h.lcn_start + h.cluster_count {
                return Some(hint);
            }
        }
        runs.binary_search_by(|r| {
            if lcn < r.lcn_start {
                std::cmp::Ordering::Greater
            } else if lcn >= r.lcn_start + r.cluster_count {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .ok()
    };

    let mut found_idx = find_run(&index.runs, *last_run_hint);

    if found_idx.is_none() {
        if let Some(cs) = compressed.as_mut() {
            if let Some(scan) = &cs.lazy_scan {
                // Only trigger the expensive forward scan if this LCN is
                // actually in the MFT data-run queue (= allocated cluster).
                // Partition detection probes random sectors and should get
                // zeros, not trigger a full decompression pass.
                let is_known_lcn = scan
                    .data_run_queue
                    .iter()
                    .any(|&(l, c)| lcn >= l && lcn < l + c);
                if is_known_lcn {
                    extend_ntfs_compressed_index(inner, index, cs, lcn);
                    found_idx = find_run(&index.runs, *last_run_hint);
                }
            }
        }
    }

    let Some(idx) = found_idx else {
        for b in &mut out[..to_read] {
            *b = 0;
        }
        return Ok(to_read);
    };

    *last_run_hint = idx;
    let run = &index.runs[idx];
    let cluster_in_run = lcn - run.lcn_start;
    let file_offset = run.file_offset + cluster_in_run * cluster_size + offset_in_cluster as u64;

    if let Some(cs) = compressed {
        // A mapped run whose decompressed data wasn't recorded (file_offset at
        // or past the end of the decompressed stream) would make the reader
        // return 0, which sequential consumers (e.g. whole-disk export) treat
        // as EOF and stop early. Zero-fill the cluster instead so reads keep
        // advancing, and log it so the mismapping can be investigated.
        if file_offset >= cs.total_decompressed {
            log::warn!(
                "GHO NTFS: run for lcn {lcn} maps to decompressed offset {file_offset} \
                 >= total_decompressed {} (run lcn_start={} cluster_count={} file_offset={}); \
                 zero-filling cluster",
                cs.total_decompressed,
                run.lcn_start,
                run.cluster_count,
                run.file_offset,
            );
            for b in &mut out[..to_read] {
                *b = 0;
            }
            return Ok(to_read);
        }
        let mut reader = NtfsDecompressingReader {
            inner,
            state: cs,
            position: file_offset,
        };
        return reader.read(&mut out[..to_read]);
    }

    inner
        .seek(SeekFrom::Start(file_offset))
        .map_err(std::io::Error::other)?;
    let n = inner.read(&mut out[..to_read])?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};

    /// Helper: a minimal `CachedNtfsScan` for cache round-trip tests.
    fn dummy_scan(run_count: usize) -> CachedNtfsScan {
        CachedNtfsScan {
            runs: (0..run_count as u64)
                .map(|i| NtfsGhoClusterRun {
                    lcn_start: i * 10,
                    cluster_count: 10,
                    file_offset: i * 4096,
                    seq: 0,
                })
                .collect(),
            blocks: Vec::new(),
            compression: GhoCompression::High,
            total_decompressed: 1024,
            volume_size: 1 << 20,
            cluster_size: 4096,
            vbr: [0u8; 512],
            bitmap_runs: Vec::new(),
            synth_bitmap: Vec::new(),
            boot_code: Vec::new(),
        }
    }

    #[test]
    fn ntfs_backup_boot_sector_lives_in_the_trailing_sector() {
        // A reconstructed NTFS volume must run to total_sectors + 1 sectors, with
        // the VBR copy (backup boot sector) in that final trailing sector — where
        // NTFS, chkdsk, and the Windows boot path all look for it. total_sectors
        // (VBR 0x28) deliberately EXCLUDES that sector, so the served volume is
        // one sector longer than index.volume_size.
        let bps: u64 = 512;
        let total_sectors: u64 = 100;
        let cluster_size: u64 = 4096;

        let mut vbr = [0u8; 512];
        vbr[3..11].copy_from_slice(b"NTFS    ");
        vbr[0x0B..0x0D].copy_from_slice(&(bps as u16).to_le_bytes());
        vbr[0x28..0x30].copy_from_slice(&total_sectors.to_le_bytes());
        vbr[0xAB] = 0xAB; // a recognizable boot-code byte past the BPB
        vbr[510] = 0x55;
        vbr[511] = 0xAA;

        let mut index = NtfsGhoIndex {
            runs: Vec::new(), // no mapped runs -> normal reads return zeros
            volume_size: total_sectors * bps,
            cluster_size,
            vbr,
            bitmap_runs: Vec::new(),
            synth_bitmap: Vec::new(),
            boot_code: Vec::new(),
        };

        // The served length includes the trailing backup-boot sector.
        assert_eq!(ntfs_served_size(&index), total_sectors * bps + bps);

        // runs is empty, so `inner` is never actually read below; SpanReader still
        // needs a backing file to exist.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("dummy.bin");
        std::fs::write(&p, [0u8; 4096]).unwrap();
        let mut inner = SpanReader::open(std::slice::from_ref(&p)).unwrap();
        let mut hint = 0usize;
        let mut comp: Option<NtfsCompressedState> = None;

        let mut read_at = |inner: &mut SpanReader, index: &mut NtfsGhoIndex, pos: u64| -> Vec<u8> {
            let mut out = [0u8; 512];
            let n = read_ntfs_file_aware_into(inner, index, &mut hint, &mut comp, pos, &mut out)
                .unwrap();
            out[..n].to_vec()
        };

        // The backup boot sector at `volume_size` mirrors the VBR exactly.
        let backup = read_at(&mut inner, &mut index, total_sectors * bps);
        assert_eq!(backup.len(), bps as usize);
        assert_eq!(
            &backup[..],
            &vbr[..],
            "trailing sector must be the VBR copy"
        );

        // The OLD (buggy) location, one sector earlier, must now be ordinary
        // volume data (zeros here) — NOT a stray, misplaced VBR copy.
        let old_loc = read_at(&mut inner, &mut index, (total_sectors - 1) * bps);
        assert!(
            old_loc.iter().all(|&b| b == 0),
            "the sector before the end must not carry a misplaced backup VBR"
        );

        // Sector 0 remains the primary VBR.
        let sector0 = read_at(&mut inner, &mut index, 0);
        assert_eq!(&sector0[..16], &vbr[..16], "sector 0 must be the VBR");
    }

    #[test]
    fn match_inline_run_consumes_fresh_group_in_mft_order() {
        use std::collections::HashSet;
        // Reproduces the $Secure (file 9) scenario from smallNTFS.GHO: a stale
        // orphaned (1310297,1) entry from an earlier gap sits ahead of the
        // freshest gap's two same-size runs — $SDH (1310298) then $SII (1310299)
        // in MFT attribute order. The two cc=1 stream runs that follow must be
        // matched to 1310298 then 1310299, in that order. Matching from the back
        // would swap them; matching from the front of the whole list would grab
        // the stale 1310297.
        let mut list = vec![(1310297u64, 1u64), (1310298, 1), (1310299, 1)];
        let mut group_start = 1; // freshest gap appended entries 1..=2
        let assigned: HashSet<u64> = HashSet::new();

        let first = match_inline_run(&mut list, &mut group_start, 1, &assigned).unwrap();
        assert_eq!(first, 1310298, "$SDH must take the lower (first) LCN");
        let mut assigned = HashSet::new();
        assigned.insert(first);
        let second = match_inline_run(&mut list, &mut group_start, 1, &assigned).unwrap();
        assert_eq!(second, 1310299, "$SII must take the higher (second) LCN");
    }

    #[test]
    fn match_inline_run_falls_back_to_older_orphans() {
        use std::collections::HashSet;
        // Fresh group has no cc=3 match, so an older orphaned entry is used.
        let mut list = vec![(100u64, 3u64), (200, 1), (201, 1)];
        let mut group_start = 1;
        let assigned: HashSet<u64> = HashSet::new();
        let lcn = match_inline_run(&mut list, &mut group_start, 3, &assigned).unwrap();
        assert_eq!(lcn, 100);
        // Removing an entry before group_start shifts the boundary down.
        assert_eq!(group_start, 0);
    }

    #[test]
    fn match_inline_run_skips_assigned_and_preserves_fragment_order() {
        use std::collections::HashSet;
        // A fragmented file's $DATA: four same-size extents in MFT order. They
        // must be handed out in that order, not reversed.
        let mut list = vec![(50u64, 4u64), (10, 4), (90, 4), (30, 4)];
        let mut group_start = 0;
        let mut assigned: HashSet<u64> = HashSet::new();
        let mut got = Vec::new();
        for _ in 0..4 {
            let lcn = match_inline_run(&mut list, &mut group_start, 4, &assigned).unwrap();
            assigned.insert(lcn);
            got.push(lcn);
        }
        assert_eq!(got, vec![50, 10, 90, 30]);
    }

    #[test]
    fn ntfs_scan_key_tracks_len_and_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.gho");
        std::fs::write(&a, b"hello").unwrap();

        let k1 = NtfsScanKey::from_span_set(std::slice::from_ref(&a)).expect("key");
        // Same file, unchanged -> identical key.
        let k2 = NtfsScanKey::from_span_set(std::slice::from_ref(&a)).expect("key");
        assert_eq!(k1, k2);

        // Different length -> different key (so a re-backup is never reused).
        std::fs::write(&a, b"hello world").unwrap();
        let k3 = NtfsScanKey::from_span_set(std::slice::from_ref(&a)).expect("key");
        assert_ne!(k1, k3);

        // Missing file -> no key (caching disabled).
        let missing = dir.path().join("nope.gho");
        assert!(NtfsScanKey::from_span_set(std::slice::from_ref(&missing)).is_none());
    }

    #[test]
    fn ntfs_scan_cache_round_trips_and_evicts() {
        // Use unique synthetic keys so this test doesn't collide with others
        // sharing the process-wide cache.
        let key = |tag: &str| NtfsScanKey(vec![(PathBuf::from(tag), 1, 1)]);

        let k = key("round-trip-unique-archive");
        assert!(ntfs_scan_cache_get(&k).is_none());

        let index = NtfsGhoIndex {
            runs: dummy_scan(3).runs,
            volume_size: 1 << 20,
            cluster_size: 4096,
            vbr: [0u8; 512],
            bitmap_runs: Vec::new(),
            synth_bitmap: Vec::new(),
            boot_code: Vec::new(),
        };
        let comp = NtfsCompressedState {
            blocks: Vec::new(),
            compression: GhoCompression::High,
            total_decompressed: 1024,
            cache_block: None,
            cache_buf: Vec::new(),
            lazy_scan: None,
            cache_key: Some(k.clone()),
        };
        ntfs_scan_cache_store(&k, &index, &comp);

        let got = ntfs_scan_cache_get(&k).expect("cached");
        assert_eq!(got.runs.len(), 3);
        assert_eq!(got.volume_size, 1 << 20);

        // Insert more than the cap of distinct keys; the first inserted one
        // must be evicted (FIFO).
        for i in 0..NTFS_SCAN_CACHE_CAP + 2 {
            let kk = key(&format!("evict-unique-{i}"));
            let c = NtfsCompressedState {
                blocks: Vec::new(),
                compression: GhoCompression::High,
                total_decompressed: 0,
                cache_block: None,
                cache_buf: Vec::new(),
                lazy_scan: None,
                cache_key: Some(kk.clone()),
            };
            ntfs_scan_cache_store(&kk, &index, &c);
        }
        // The very first evict key is gone; the most recent survives.
        assert!(ntfs_scan_cache_get(&key("evict-unique-0")).is_none());
        assert!(
            ntfs_scan_cache_get(&key(&format!("evict-unique-{}", NTFS_SCAN_CACHE_CAP + 1)))
                .is_some()
        );
    }

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
            let max = 512 - 0xFF - 1; // leave room for NUL
            assert!(bytes.len() <= max);
            buf[0xFF..0xFF + bytes.len()].copy_from_slice(bytes);
            // NUL terminator (buf is already zeroed).
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
    fn parses_nul_terminated_description() {
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
    fn materialize_sector_mode_empty_returns_empty_image() {
        // SECTOR mode with no data sectors past the container header is
        // a valid (if degenerate) input; decoder produces a zero-length
        // output rather than erroring.
        let buf = build_header(0x00, 0x01, 0x00, None);
        let path = write_to_temp(&buf);
        let mat = materialize_gho_to_temp(&path).expect("SECTOR mode should decode");
        assert_eq!(mat.logical_size, 0);
        assert_eq!(mat.partition_count, 1);
    }

    #[test]
    fn materialize_file_aware_empty_stream_errors_on_missing_boot_sector() {
        // A bare file-aware header with no inner records has no boot
        // sector → the emitter can't size the output image. We expect a
        // clean error mentioning "boot sector", not a panic.
        let buf = build_header(0x00, 0x00, 0x00, None);
        let path = write_to_temp(&buf);
        let err = materialize_gho_to_temp(&path).unwrap_err();
        let msg = format!("{err:#}").to_lowercase();
        assert!(
            msg.contains("boot sector") || msg.contains("ntfs") || msg.contains("record stream"),
            "expected boot-sector / NTFS-fallback error, got: {msg}"
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

    // ---------- checked-in fixture parse guard ----------

    /// Small real Norton Ghost containers checked into tests/fixtures/
    /// (zstd-compressed). They run on every clone and in CI, so they are
    /// the minimum header-parse regression guard. Between them they cover
    /// both compression variants
    /// that exist at a checkable-in size (none + high) and both Ghost header
    /// generations (7.5 + 11.x). Tuple: (file, compression, image_type, pwd).
    const REPO_GHO_FIXTURES: &[(&str, u8, u8, u8)] = &[
        // 75MULTIU.GHO: Ghost 7.5, compression=none, fulldisk, no password.
        ("test_gho_75_none.gho.zst", 0x00, 0x00, 0x00),
        // 11comp.GHO: Ghost 11.x, compression=high, fulldisk, no password.
        ("test_gho_11_high.gho.zst", 0x03, 0x00, 0x00),
    ];

    fn load_zst_fixture(name: &str) -> Vec<u8> {
        let path = format!("tests/fixtures/{name}");
        let compressed =
            std::fs::read(&path).unwrap_or_else(|e| panic!("reading repo fixture {path}: {e}"));
        let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(compressed))
            .unwrap_or_else(|e| panic!("zstd decoder for {path}: {e}"));
        let mut out = Vec::new();
        decoder
            .read_to_end(&mut out)
            .unwrap_or_else(|e| panic!("decompressing repo fixture {path}: {e}"));
        out
    }

    /// Header-parse regression guard against the checked-in fixtures.
    #[test]
    fn parses_checked_in_gho_fixtures() {
        for (name, comp, image_type, pwd) in REPO_GHO_FIXTURES {
            let bytes = load_zst_fixture(name);
            let header = GhoContainerHeader::parse(&mut Cursor::new(bytes))
                .unwrap_or_else(|e| panic!("parsing {name}: {e:#}"));
            assert_eq!(header.container_version, 0x01, "{name}: container_version");
            assert_eq!(header.flags, [0x01, 0x01], "{name}: flags");
            assert_eq!(
                header.compression.as_byte(),
                *comp,
                "{name}: compression (got {:#04x}, want {:#04x})",
                header.compression.as_byte(),
                comp
            );
            assert_eq!(
                header.image_type.as_byte(),
                *image_type,
                "{name}: image_type (got {:#04x}, want {:#04x})",
                header.image_type.as_byte(),
                image_type
            );
            assert_eq!(
                header.password_protected,
                *pwd == 0x01,
                "{name}: password_protected"
            );
        }
    }

    // ---------- record-stream parser (5.5b) ----------

    fn build_record_header_bytes(type_code: u16, marker: u16, body_len: u16) -> [u8; 10] {
        let mut buf = [0u8; 10];
        buf[0..2].copy_from_slice(&type_code.to_le_bytes());
        buf[2..4].copy_from_slice(&marker.to_le_bytes());
        buf[4..8].copy_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
        buf[8..10].copy_from_slice(&body_len.to_le_bytes());
        buf
    }

    #[test]
    fn parses_75_style_record_header() {
        // Ghost 7.5: marker = 0x0000
        let bytes = build_record_header_bytes(0x0017, 0x0000, 0x0200);
        let header = GhoRecordHeader::parse_bytes(&bytes).unwrap();
        assert_eq!(header.type_code, 0x0017);
        assert_eq!(header.marker, 0x0000);
        assert_eq!(header.body_len, 0x0200);
    }

    #[test]
    fn parses_115_style_record_header() {
        // Ghost 11.5: marker = 0x95FD
        let bytes = build_record_header_bytes(0x0017, 0x95FD, 0x0200);
        let header = GhoRecordHeader::parse_bytes(&bytes).unwrap();
        assert_eq!(header.type_code, 0x0017);
        assert_eq!(header.marker, 0x95FD);
        assert_eq!(header.body_len, 0x0200);
    }

    #[test]
    fn rejects_record_header_with_wrong_magic() {
        let mut bytes = build_record_header_bytes(0x0017, 0x0000, 0x0200);
        bytes[4] = 0xFF; // clobber magic
        let err = GhoRecordHeader::parse_bytes(&bytes).unwrap_err();
        assert!(format!("{err:#}").contains("record magic"));
    }

    #[test]
    fn record_iter_walks_two_records_and_stops_at_eof() {
        // Two records back to back: type 0x0017 with 4-byte body 'b1b1',
        // type 0x0004 with 3-byte body 'b2b'.
        let mut buf = Vec::new();
        buf.extend_from_slice(&build_record_header_bytes(0x0017, 0x95FD, 4));
        buf.extend_from_slice(b"b1b1");
        buf.extend_from_slice(&build_record_header_bytes(0x0004, 0x95FD, 3));
        buf.extend_from_slice(b"b2b");

        let mut iter = GhoRecordIter::new(Cursor::new(buf), 0).unwrap();
        let r0 = iter.next().unwrap().unwrap();
        assert_eq!(r0.type_code, 0x0017);
        assert_eq!(r0.body_len, 4);
        let r1 = iter.next().unwrap().unwrap();
        assert_eq!(r1.type_code, 0x0004);
        assert_eq!(r1.body_len, 3);
        assert!(iter.next().is_none(), "iterator should stop cleanly at EOF");
        assert_eq!(iter.records_read(), 2);
    }

    #[test]
    fn record_iter_surfaces_error_on_corrupt_magic_mid_stream() {
        // First record OK; second record has bad magic.
        let mut buf = Vec::new();
        buf.extend_from_slice(&build_record_header_bytes(0x0017, 0x0000, 0));
        buf.extend_from_slice(&build_record_header_bytes(0x0004, 0x0000, 0));
        // Clobber the magic in the second header.
        let second = 10;
        buf[second + 4] = 0;

        let mut iter = GhoRecordIter::new(Cursor::new(buf), 0).unwrap();
        let _ = iter.next().unwrap().unwrap();
        let err = iter.next().unwrap().unwrap_err();
        assert!(format!("{err:#}").contains("record magic"));
    }

    #[test]
    fn find_inner_stream_start_locates_magic_in_synthetic_stream() {
        // Container header (12 bytes) + 100 bytes of pad + first record.
        let mut buf = vec![0u8; 12 + 100];
        buf[0] = 0xFE;
        buf[1] = 0xEF;
        buf.extend_from_slice(&build_record_header_bytes(0x0017, 0x0000, 0));
        let start = find_inner_stream_start(&mut Cursor::new(buf), 12).unwrap();
        assert_eq!(
            start,
            12 + 100,
            "header offset should land 4 bytes before magic"
        );
    }

    #[test]
    fn find_inner_stream_start_handles_magic_straddling_chunks() {
        // Place magic so the first 3 bytes land at the end of a 4096-byte
        // scan chunk and the 4th byte lands in the next chunk. This
        // exercises the tail-carry path.
        let header_end = 12;
        let pad_to_boundary = 4096 - 3 + 4; // ensure magic straddles boundary
        let mut buf = vec![0u8; header_end + pad_to_boundary];
        buf.extend_from_slice(&build_record_header_bytes(0x0017, 0x95FD, 0));
        let start = find_inner_stream_start(&mut Cursor::new(buf), header_end as u64).unwrap();
        assert_eq!(start, (header_end + pad_to_boundary) as u64);
    }

    #[test]
    fn find_inner_stream_start_errors_when_magic_absent() {
        let buf = vec![0u8; 32 * 1024]; // all zeros, no magic
        let err = find_inner_stream_start(&mut Cursor::new(buf), 12).unwrap_err();
        assert!(format!("{err:#}").contains("not found"));
    }

    // ---------- Fast-LZ block decoder (5.5c) ----------

    #[test]
    fn fast_lz_hash_matches_reference_values() {
        // h(0,0,0) trivially = 0.
        assert_eq!(fast_lz_hash(0, 0, 0), 0);
        // h(1,0,0): v = 0 ^ (16 * (0 ^ (16 * 1))) = 256.
        //   -24993 * 256 = -6398208 → as u32 0xFF9E5F00, >> 4 → 0x0FF9E5F0,
        //   & 0xFFF = 0x5F0.
        assert_eq!(fast_lz_hash(1, 0, 0), 0x5F0);
        // Different orderings of the same 3 bytes hash differently — the
        // formula is order-sensitive.
        assert_ne!(
            fast_lz_hash(0xAB, 0xCD, 0xEF),
            fast_lz_hash(0xEF, 0xCD, 0xAB)
        );
    }

    #[test]
    fn fast_lz_decompresses_uncompressed_block_via_prefix_byte_one() {
        // data[0] == 1 → stored verbatim past the 4-byte prefix.
        let payload = b"hello, ghost";
        let mut input = vec![1u8, 0, 0, 0];
        input.extend_from_slice(payload);
        let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
        let n = fast_lz_decompress(&input, input.len(), &mut dst).unwrap();
        assert_eq!(n, payload.len());
        assert_eq!(&dst[..n], payload);
    }

    #[test]
    fn fast_lz_uncompressed_rejects_too_short_input() {
        // comp_len < 4 → no room for the 4-byte prefix.
        let input = [1u8, 0, 0];
        let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
        let err = fast_lz_decompress(&input, 3, &mut dst).unwrap_err();
        assert!(format!("{err:#}").contains("too short"));
    }

    #[test]
    fn fast_lz_rejects_truncated_input_when_comp_len_exceeds_data() {
        let input = [2u8, 0, 0, 0];
        let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
        let err = fast_lz_decompress(&input, 8, &mut dst).unwrap_err();
        assert!(format!("{err:#}").contains("truncated"));
    }

    #[test]
    fn fast_lz_zero_complen_rejected() {
        let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
        let err = fast_lz_decompress(&[1u8, 0, 0, 0], 0, &mut dst).unwrap_err();
        assert!(format!("{err:#}").contains("truncated"));
    }

    #[test]
    fn fast_lz_decompresses_literal_only_payload() {
        // Construct: 4-byte non-1 prefix, then a 16-bit control word of
        // 0x0000 (all bits 0 → all 16 tokens literal), then 16 literal
        // bytes, then 32 bytes of zero padding so the near_end safeguard
        // does not kick in mid-batch. The decoder should emit exactly
        // the 16 literal bytes for the first control word; subsequent
        // padding-driven control words produce extra zero literals which
        // are also valid output but not asserted on.
        let mut input = vec![2u8, 0, 0, 0]; // prefix, type byte != 1
        input.extend_from_slice(&[0x00, 0x00]); // control word: all literals
        input.extend_from_slice(b"ABCDEFGHIJKLMNOP"); // 16 literals
        input.extend(std::iter::repeat_n(0u8, 64)); // pad past near_end window

        let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
        let n = fast_lz_decompress(&input, input.len(), &mut dst).unwrap();
        assert!(n >= 16, "decoder should emit at least the 16 literals");
        assert_eq!(&dst[..16], b"ABCDEFGHIJKLMNOP");
    }

    // ---------- 5.6 — walker + decode-to-temp ----------

    /// Build a synthetic file-aware container in our fixture-corpus
    /// dialect: container header + one boot-sector record (0x0017) +
    /// N data-block records (0x0002) each carrying `block_payload`.
    fn build_single_partition_corpus_dialect(
        compression_byte: u8,
        boot_sector: &[u8],
        data_blocks: &[Vec<u8>],
    ) -> Vec<u8> {
        let mut out = build_header(compression_byte, 0x00, 0x00, None);
        out.resize(GHO_SECTOR_SIZE as usize, 0);
        // Boot-sector record.
        assert_eq!(boot_sector.len(), 512);
        out.extend_from_slice(&build_record_header_bytes(GHO_REC_BOOT_SECTOR, 0, 512));
        out.extend_from_slice(boot_sector);
        // Data-block records.
        for body in data_blocks {
            assert!(body.len() <= u16::MAX as usize);
            out.extend_from_slice(&build_record_header_bytes(
                GHO_REC_DATA_BLOCK,
                0,
                body.len() as u16,
            ));
            out.extend_from_slice(body);
        }
        out
    }

    #[test]
    fn parse_gho_image_counts_partitions_and_data_blocks() {
        // The walker is kept as infrastructure for the deferred file-aware
        // reconstruction; today only the synthetic shape exercises it.
        let boot = vec![0u8; 512];
        let blocks = vec![vec![0xAA; 32768], vec![0xBB; 32768]];
        let buf = build_single_partition_corpus_dialect(0x00, &boot, &blocks);
        let mut cur = Cursor::new(&buf);
        let header = GhoContainerHeader::parse(&mut cur).unwrap();
        let img = parse_gho_image(&mut cur, buf.len() as u64, &header).unwrap();
        assert_eq!(img.partition_count, 1);
        assert_eq!(img.data_blocks().count(), 2);
        assert_eq!(img.records.len(), 3);
        assert_eq!(img.records[0].type_code, GHO_REC_BOOT_SECTOR);
        assert_eq!(img.records[1].type_code, GHO_REC_DATA_BLOCK);
        assert_eq!(img.records[1].body_len, 32768);
    }

    /// Build a synthetic SECTOR-mode UNCOMPRESSED GHO: container header,
    /// some zero padding, a FEEF sub-header (the data-start marker), then
    /// raw sectors. Mirrors the real corpus layout.
    fn build_sector_mode_uncompressed(sectors: &[[u8; 512]]) -> Vec<u8> {
        let mut out = build_header(0x00, 0x01, 0x00, None);
        // Pad to sector 4, then a FEEF sub-header at sector 4, data at 5.
        out.resize(4 * GHO_SECTOR_SIZE as usize, 0);
        let mut feef = [0u8; 512];
        feef[0] = 0xFE;
        feef[1] = 0xEF;
        out.extend_from_slice(&feef);
        for s in sectors {
            out.extend_from_slice(s);
        }
        out
    }

    #[test]
    fn materialize_sector_mode_uncompressed_round_trips_sectors() {
        let mut s0 = [0u8; 512];
        s0[0..3].copy_from_slice(b"\xeb\x58\x90"); // FAT BPB jump
        s0[510] = 0x55;
        s0[511] = 0xAA;
        let mut s1 = [0u8; 512];
        s1[..16].copy_from_slice(b"second sector...");
        let buf = build_sector_mode_uncompressed(&[s0, s1]);
        let path = write_to_temp(&buf);
        let mat = materialize_gho_to_temp(&path).expect("SECTOR mode should decode");
        assert_eq!(mat.partition_count, 1);
        let written = std::fs::read(&mat.temp_path).unwrap();
        assert_eq!(written.len(), 1024);
        assert_eq!(&written[510..512], &[0x55, 0xAA]);
        assert_eq!(&written[512..528], b"second sector...");
    }

    #[test]
    fn find_sector_data_start_lands_after_last_feef_subheader() {
        // FEEF at sector 3 → data should start at sector 4.
        let mut buf = vec![0u8; 6 * 512];
        buf[3 * 512] = 0xFE;
        buf[3 * 512 + 1] = 0xEF;
        let start = find_sector_data_start(&mut Cursor::new(&buf), buf.len() as u64).unwrap();
        assert_eq!(start, 4 * GHO_SECTOR_SIZE);
    }

    #[test]
    fn find_sector_data_start_falls_back_to_first_nonzero_when_no_feef() {
        let mut buf = vec![0u8; 5 * 512];
        buf[2 * 512] = 0xCD; // non-zero, but not FEEF
        let start = find_sector_data_start(&mut Cursor::new(&buf), buf.len() as u64).unwrap();
        assert_eq!(start, 2 * GHO_SECTOR_SIZE);
    }

    // ---------- 5.6-span — span-set discovery + multi-file decode ----------

    #[test]
    fn split_gho_dot_numeric_parses_win7_style_names() {
        assert_eq!(
            split_gho_dot_numeric("Win7_86xAMB.GHO.001"),
            Some(("Win7_86xAMB".to_string(), 1))
        );
        assert_eq!(
            split_gho_dot_numeric("Win7_86xAMB.GHO.066"),
            Some(("Win7_86xAMB".to_string(), 66))
        );
        // Case-insensitive on `.GHO`.
        assert_eq!(
            split_gho_dot_numeric("name.gho.042"),
            Some(("name".to_string(), 42))
        );
        // Doesn't trigger on plain `.gho`.
        assert_eq!(split_gho_dot_numeric("disk.gho"), None);
        // Doesn't trigger on `.ghs` extension.
        assert_eq!(split_gho_dot_numeric("disk.ghs"), None);
    }

    #[test]
    fn strip_trailing_numeric_handles_corpus_stems() {
        assert_eq!(strip_trailing_numeric("SECTO001"), "SECTO");
        assert_eq!(strip_trailing_numeric("gh11-001"), "gh11-");
        assert_eq!(strip_trailing_numeric("gh11pwd"), "gh11pwd");
        assert_eq!(strip_trailing_numeric("hipwd005"), "hipwd");
        assert_eq!(strip_trailing_numeric("SECTOR"), "SECTOR");
    }

    /// Synthetic directory with `disk.GHO` + `disk001.GHS` + `disk002.GHS`.
    /// Picking ANY one of the three should return the same ordered set.
    #[test]
    fn discover_span_set_finds_ghs_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let primary = dir.path().join("disk.GHO");
        let s1 = dir.path().join("disk001.GHS");
        let s2 = dir.path().join("disk002.GHS");
        std::fs::write(&primary, b"primary").unwrap();
        std::fs::write(&s1, b"span1").unwrap();
        std::fs::write(&s2, b"span2").unwrap();
        for pick in [&primary, &s1, &s2] {
            let set = discover_gho_span_set(pick).unwrap();
            assert_eq!(
                set.len(),
                3,
                "picking {} should yield 3 files",
                pick.display()
            );
            assert_eq!(set[0], primary, "primary must be first");
            assert_eq!(set[1], s1);
            assert_eq!(set[2], s2);
        }
    }

    /// Numeric-suffix layout: `name.GHO.001`, `.002`, `.003`. Picking any
    /// one yields the set sorted by numeric tail.
    #[test]
    fn discover_span_set_finds_dot_numeric_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("name.GHO.001");
        let p2 = dir.path().join("name.GHO.002");
        let p3 = dir.path().join("name.GHO.003");
        std::fs::write(&p1, b"a").unwrap();
        std::fs::write(&p2, b"b").unwrap();
        std::fs::write(&p3, b"c").unwrap();
        for pick in [&p1, &p2, &p3] {
            let set = discover_gho_span_set(pick).unwrap();
            assert_eq!(set, vec![p1.clone(), p2.clone(), p3.clone()]);
        }
    }

    /// Single-file case: no siblings → set is just the picked file.
    #[test]
    fn discover_span_set_singleton_when_no_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("lonely.GHO");
        std::fs::write(&f, b"alone").unwrap();
        let set = discover_gho_span_set(&f).unwrap();
        assert_eq!(set, vec![f]);
    }

    #[test]
    fn span_reader_concatenates_and_skips_container_headers() {
        // Build 3 synthetic span files:
        //   primary: 512-byte header (FE EF ...) + 100 bytes "A..."
        //   span 1:  512-byte header (FE EF ...) + 50 bytes "B..."
        //   span 2:  512-byte header (FE EF ...) + 25 bytes "C..."
        // SpanReader should expose: full primary (612 bytes) +
        // span1 minus its 512-byte header (50 bytes) +
        // span2 minus its 512-byte header (25 bytes) = 687 bytes total.
        let dir = tempfile::tempdir().unwrap();
        let make = |name: &str, fill: u8, body_len: usize| -> std::path::PathBuf {
            let path = dir.path().join(name);
            let mut data = vec![0u8; 512];
            data[0] = 0xFE;
            data[1] = 0xEF;
            data.extend_from_slice(&vec![fill; body_len]);
            std::fs::write(&path, &data).unwrap();
            path
        };
        let p0 = make("set.GHO", b'A', 100);
        let p1 = make("set001.GHS", b'B', 50);
        let p2 = make("set002.GHS", b'C', 25);

        let mut r = SpanReader::open(&[p0, p1, p2]).unwrap();
        assert_eq!(r.total_len(), 612 + 50 + 25);

        let mut all = Vec::new();
        r.read_to_end(&mut all).unwrap();
        assert_eq!(all.len() as u64, r.total_len());

        // Primary's header byte at offset 0 should be FE.
        assert_eq!(all[0], 0xFE);
        // Last byte of primary (offset 611) is 'A'.
        assert_eq!(all[611], b'A');
        // Bytes 612..662 are 'B' (50 from span 1, header skipped).
        assert!(all[612..662].iter().all(|&b| b == b'B'), "span 1 body");
        // Bytes 662..687 are 'C' (25 from span 2, header skipped).
        assert!(all[662..687].iter().all(|&b| b == b'C'), "span 2 body");

        // Seek round-trip.
        r.seek(SeekFrom::Start(612)).unwrap();
        let mut probe = [0u8; 1];
        r.read_exact(&mut probe).unwrap();
        assert_eq!(probe[0], b'B', "seek-then-read should land in span 1");
    }

    // ---------- file-aware record body parsers ----------

    #[test]
    fn record_type_predicates_classify_known_codes() {
        // Boot sector
        assert!(is_boot_sector_record(0x0017));
        assert!(is_boot_sector_record(0x0717));
        assert!(is_boot_sector_record(0xae17)); // 7.5 full-disk header copy
        assert!(is_boot_sector_record(0x0117)); // 7.5 full-disk partition copy
        assert!(!is_boot_sector_record(0x0004));

        // Dir entries
        assert!(is_dir_entry_record(0x0004));
        assert!(is_dir_entry_record(0x0104));
        assert!(is_dir_entry_record(0x0704));
        assert!(is_dir_entry_record(0xae04)); // 7.5 full-disk header section
        assert!(!is_dir_entry_record(0x0017));
        assert!(!is_dir_entry_record(0x0102));

        // Data blocks
        assert!(is_data_block_record(0x0002));
        assert!(is_data_block_record(0x0102));
        assert!(!is_data_block_record(0x0103));
        assert!(!is_data_block_record(0x0104));

        // Checksum
        assert!(is_checksum_record(0x0103));
        assert!(!is_checksum_record(0x0102));
    }

    #[test]
    fn parse_fat_dir_entry_body_decodes_lfn_slot() {
        // Sequence-1 LFN holding 'A','c','c' UTF-16LE. attr=0x0F at offset 11.
        let mut body = [0u8; 56];
        body[0] = 0x41; // sequence + LFN flag
        body[1] = b'A';
        body[2] = 0;
        body[3] = b'c';
        body[4] = 0;
        body[5] = b'c';
        body[6] = 0;
        body[11] = 0x0F; // LFN attr
        body[32..36].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());

        let parsed = parse_fat_dir_entry_body(&body).unwrap();
        assert!(parsed.is_lfn_slot());
        assert!(!parsed.is_directory());
        assert_eq!(parsed.first_cluster(), 0);
        assert_eq!(parsed.file_size(), 0);
        assert_eq!(parsed.entry_hash, 0xDEAD_BEEF);
    }

    #[test]
    fn parse_fat_dir_entry_body_decodes_8_3_directory() {
        // "MYDOCU~1" with attr=0x10 (dir), first_cluster=3 (FAT32 split:
        // lo at 26-27, hi at 20-21), file_size=0.
        let mut body = [0u8; 56];
        body[..11].copy_from_slice(b"MYDOCU~1   ");
        body[11] = 0x10; // directory
        body[26..28].copy_from_slice(&3u16.to_le_bytes()); // cluster lo
        body[20..22].copy_from_slice(&0u16.to_le_bytes()); // cluster hi
        body[32..36].copy_from_slice(&0xCAFEu32.to_le_bytes());

        let parsed = parse_fat_dir_entry_body(&body).unwrap();
        assert!(!parsed.is_lfn_slot());
        assert!(parsed.is_directory());
        assert!(!parsed.is_volume_label());
        assert_eq!(parsed.first_cluster(), 3);
        assert_eq!(parsed.file_size(), 0); // directory size always 0
        assert_eq!(parsed.entry_hash, 0xCAFE);
    }

    #[test]
    fn parse_fat_dir_entry_body_decodes_regular_file() {
        // "MSPAINT EXE" with size 0x54000 (344064), cluster 0x040000 (256K).
        let mut body = [0u8; 56];
        body[..11].copy_from_slice(b"MSPAINT EXE");
        body[11] = 0x20; // archive
        body[26..28].copy_from_slice(&0x0000u16.to_le_bytes()); // cluster lo
        body[20..22].copy_from_slice(&0x0004u16.to_le_bytes()); // cluster hi
        body[28..32].copy_from_slice(&0x00054000u32.to_le_bytes()); // size

        let parsed = parse_fat_dir_entry_body(&body).unwrap();
        assert!(!parsed.is_lfn_slot());
        assert!(!parsed.is_directory());
        assert_eq!(parsed.first_cluster(), 0x0004_0000);
        assert_eq!(parsed.file_size(), 344_064);
    }

    #[test]
    fn parse_fat_dir_entry_body_recognises_empty_and_deleted_slots() {
        let body_empty = [0u8; 56];
        let parsed = parse_fat_dir_entry_body(&body_empty).unwrap();
        assert!(parsed.is_empty_slot());
        assert!(!parsed.is_deleted());

        let mut body_del = [0u8; 56];
        body_del[0] = 0xE5;
        let parsed = parse_fat_dir_entry_body(&body_del).unwrap();
        assert!(parsed.is_deleted());
        assert!(!parsed.is_empty_slot());
    }

    #[test]
    fn parse_fat_dir_entry_body_rejects_too_short() {
        let err = parse_fat_dir_entry_body(&[0u8; 35]).unwrap_err();
        assert!(format!("{err}").contains(">= 36"));
    }

    #[test]
    fn parse_checksum_record_body_extracts_duplicated_value() {
        let mut body = [0u8; 20];
        body[..4].copy_from_slice(&0x12345678u32.to_le_bytes());
        body[4..8].copy_from_slice(&0x12345678u32.to_le_bytes());
        let v = parse_checksum_record_body(&body).unwrap();
        assert_eq!(v, 0x12345678);
    }

    #[test]
    fn parse_checksum_record_body_errors_on_mismatch() {
        let mut body = [0u8; 20];
        body[..4].copy_from_slice(&0xAAAAu32.to_le_bytes());
        body[4..8].copy_from_slice(&0xBBBBu32.to_le_bytes());
        let err = parse_checksum_record_body(&body).unwrap_err();
        assert!(format!("{err}").contains("mismatched"));
    }

    // ---------- file-aware directory walker ----------

    /// Unit-level: build a synthetic file-aware GHO with boot sector +
    /// one dir + one file, walk it, and confirm the tree shape.
    #[test]
    fn walk_file_aware_tree_assembles_synthetic_stream() {
        use std::io::Cursor;

        // Build a minimal byte stream: container header + boot sector
        // + dir entries + file content + checksum + EOF.
        let mut buf = Vec::new();
        // container prefix at sector 0
        let mut prefix = vec![0u8; 512];
        prefix[0] = 0xFE;
        prefix[1] = 0xEF;
        prefix[2] = 0x01; // version
        prefix[3] = 0x00; // compression=None
        prefix[10] = 0x00; // image_type=FileAware
        buf.extend_from_slice(&prefix);

        // Helper to write a record header.
        let write_record = |buf: &mut Vec<u8>, type_code: u16, body: &[u8]| {
            buf.extend_from_slice(&type_code.to_le_bytes());
            buf.extend_from_slice(&0u16.to_le_bytes()); // marker
            buf.extend_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
            buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
            buf.extend_from_slice(body);
        };
        // Build a 512-byte FAT32 boot sector: just enough for the
        // walker to extract root_cluster.
        let mut boot = [0u8; 512];
        boot[44..48].copy_from_slice(&2u32.to_le_bytes());
        boot[82..87].copy_from_slice(b"FAT32");
        write_record(&mut buf, GHO_REC_BOOT_SECTOR, &boot);

        // 8.3 entry for "HELLO.TXT" in root, cluster=5, size=11.
        let mut e = [0u8; 56];
        e[..11].copy_from_slice(b"HELLO   TXT");
        e[11] = 0x20; // archive
        e[26..28].copy_from_slice(&5u16.to_le_bytes()); // cluster lo
        e[28..32].copy_from_slice(&11u32.to_le_bytes()); // size
        write_record(&mut buf, GHO_REC_DIR_ENTRY_ROOT, &e);

        // File content for HELLO.TXT (11 bytes).
        write_record(&mut buf, GHO_REC_FILE_TAIL, b"hello world");

        // Checksum.
        let mut c = [0u8; 20];
        c[..4].copy_from_slice(&0x12345678u32.to_le_bytes());
        c[4..8].copy_from_slice(&0x12345678u32.to_le_bytes());
        write_record(&mut buf, GHO_REC_FILE_CHECKSUM, &c);

        let mut cur = Cursor::new(buf.clone());
        let header = GhoContainerHeader::parse(&mut cur).unwrap();
        let image = parse_gho_image(&mut cur, buf.len() as u64, &header).unwrap();
        let tree = walk_file_aware_tree(&mut cur, &image).unwrap();
        assert_eq!(tree.root_cluster, 2);
        assert_eq!(tree.entries.len(), 1);
        let hello = &tree.entries[0];
        assert_eq!(hello.short_name, "HELLO.TXT");
        assert!(!hello.is_directory());
        assert_eq!(hello.parent_cluster, 2);
        assert_eq!(hello.file_size, 11);
        assert_eq!(hello.content_record_offsets.len(), 1);
        assert_eq!(hello.checksum, Some(0x12345678));
    }

    // ---------- Slice C: file-aware FAT image emitter ----------

    /// Build a synthetic file-aware GHO (boot sector + one root file +
    /// one subdir with one file), run the emitter, then re-open the
    /// resulting FAT image and verify the tree.
    #[test]
    fn emit_file_aware_fat_image_roundtrips_synthetic_stream() {
        use crate::fs::fat::FatFilesystem;
        use crate::fs::filesystem::Filesystem;
        use std::io::Cursor;

        // ---------- build synthetic GHO bytes ----------
        let mut buf = Vec::new();
        let mut prefix = vec![0u8; 512];
        prefix[0] = 0xFE;
        prefix[1] = 0xEF;
        prefix[2] = 0x01;
        prefix[3] = 0x00; // compression=None
        prefix[10] = 0x00; // image_type=FileAware
        buf.extend_from_slice(&prefix);

        let write_record = |buf: &mut Vec<u8>, type_code: u16, body: &[u8]| {
            buf.extend_from_slice(&type_code.to_le_bytes());
            buf.extend_from_slice(&0u16.to_le_bytes());
            buf.extend_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
            buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
            buf.extend_from_slice(body);
        };

        // FAT16 boot sector sized for a ~4 MiB partition (so the
        // emitter's create_blank_fat picks the same regime).
        // 4 MiB = 8192 sectors of 512.
        let mut boot = [0u8; 512];
        boot[11..13].copy_from_slice(&512u16.to_le_bytes()); // BytsPerSec
        boot[13] = 1; // SecPerClus
        boot[14..16].copy_from_slice(&1u16.to_le_bytes()); // RsvdSecCnt
        boot[16] = 2; // NumFATs
        boot[19..21].copy_from_slice(&8192u16.to_le_bytes()); // TotSec16
                                                              // Mark as FAT16 explicitly via fs-type-string @ 54.
        boot[54..62].copy_from_slice(b"FAT16   ");
        write_record(&mut buf, GHO_REC_BOOT_SECTOR, &boot);

        // Root file: HELLO.TXT, 11 bytes, cluster 5.
        let mut e = [0u8; 56];
        e[..11].copy_from_slice(b"HELLO   TXT");
        e[11] = 0x20;
        e[26..28].copy_from_slice(&5u16.to_le_bytes());
        e[28..32].copy_from_slice(&11u32.to_le_bytes());
        write_record(&mut buf, GHO_REC_DIR_ENTRY_ROOT, &e);
        write_record(&mut buf, GHO_REC_FILE_TAIL, b"hello world");
        let mut c = [0u8; 20];
        c[..4].copy_from_slice(&0xdeadbeefu32.to_le_bytes());
        c[4..8].copy_from_slice(&0xdeadbeefu32.to_le_bytes());
        write_record(&mut buf, GHO_REC_FILE_CHECKSUM, &c);

        // Subdir SUB, cluster 7.
        let mut d = [0u8; 56];
        d[..11].copy_from_slice(b"SUB        ");
        d[11] = 0x10; // dir
        d[26..28].copy_from_slice(&7u16.to_le_bytes());
        write_record(&mut buf, GHO_REC_DIR_ENTRY_SUB, &d);

        // Descend into SUB: "." cluster=7, ".." cluster=0 (root).
        let mut dot = [0u8; 56];
        dot[..11].copy_from_slice(b".          ");
        dot[11] = 0x10;
        dot[26..28].copy_from_slice(&7u16.to_le_bytes());
        write_record(&mut buf, GHO_REC_DIR_ENTRY_SUB, &dot);
        let mut dotdot = [0u8; 56];
        dotdot[..11].copy_from_slice(b"..         ");
        dotdot[11] = 0x10;
        dotdot[26..28].copy_from_slice(&0u16.to_le_bytes());
        write_record(&mut buf, GHO_REC_DIR_ENTRY_SUB, &dotdot);

        // Inside SUB: NESTED.TXT, 6 bytes, cluster 9.
        let mut e2 = [0u8; 56];
        e2[..11].copy_from_slice(b"NESTED  TXT");
        e2[11] = 0x20;
        e2[26..28].copy_from_slice(&9u16.to_le_bytes());
        e2[28..32].copy_from_slice(&6u32.to_le_bytes());
        write_record(&mut buf, GHO_REC_DIR_ENTRY_SUB, &e2);
        write_record(&mut buf, GHO_REC_FILE_TAIL, b"hello!");
        let mut c2 = [0u8; 20];
        c2[..4].copy_from_slice(&0xcafef00du32.to_le_bytes());
        c2[4..8].copy_from_slice(&0xcafef00du32.to_le_bytes());
        write_record(&mut buf, GHO_REC_FILE_CHECKSUM, &c2);

        // ---------- parse + walk + emit ----------
        let mut cur = Cursor::new(buf.clone());
        let header = GhoContainerHeader::parse(&mut cur).unwrap();
        let image = parse_gho_image(&mut cur, buf.len() as u64, &header).unwrap();
        let tree = walk_file_aware_tree(&mut cur, &image).unwrap();
        assert_eq!(tree.entries.len(), 3); // HELLO, SUB, NESTED
        assert_eq!(tree.file_count(), 2);
        assert_eq!(tree.dir_count(), 1);

        let res = emit_file_aware_fat_image(&mut cur, &image, &tree, header.compression).unwrap();
        assert_eq!(res.files_emitted, 2);
        assert_eq!(res.dirs_emitted, 1);
        assert!(
            res.skipped.is_empty(),
            "no entries should have been skipped, got {:?}",
            res.skipped
        );
        assert!(!res.image.is_empty());

        // ---------- reopen the FAT image and verify contents ----------
        let mut img_cur = Cursor::new(res.image);
        let mut fs = FatFilesystem::open(&mut img_cur, 0).unwrap();
        let root = fs.root().unwrap();
        let root_entries = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = root_entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"HELLO.TXT"), "root listing: {:?}", names);
        assert!(names.contains(&"SUB"), "root listing: {:?}", names);

        let hello = root_entries.iter().find(|e| e.name == "HELLO.TXT").unwrap();
        let data = fs.read_file(hello, usize::MAX).unwrap();
        assert_eq!(&data, b"hello world");

        let sub = root_entries.iter().find(|e| e.name == "SUB").unwrap();
        let sub_entries = fs.list_directory(sub).unwrap();
        assert_eq!(sub_entries.len(), 1);
        assert_eq!(sub_entries[0].name, "NESTED.TXT");
        let nested = fs.read_file(&sub_entries[0], usize::MAX).unwrap();
        assert_eq!(&nested, b"hello!");
    }

    /// Open a synthetic file-aware GHO via the unified `GhoReader`,
    /// pipe its bytes into a Vec, and re-open as a FAT image. The
    /// reader must serve the same bytes as `emit_file_aware_fat_image`
    /// (the in-RAM Vec path) up to volume-label byte differences.
    #[test]
    fn gho_reader_file_aware_streams_same_tree_as_emit() {
        use crate::fs::fat::FatFilesystem;
        use crate::fs::filesystem::Filesystem;
        use std::io::Cursor;

        // Same synthetic stream the emit test builds.
        let mut buf = Vec::new();
        let mut prefix = vec![0u8; 512];
        prefix[0] = 0xFE;
        prefix[1] = 0xEF;
        prefix[2] = 0x01;
        prefix[3] = 0x00;
        prefix[10] = 0x00;
        buf.extend_from_slice(&prefix);
        let write_record = |buf: &mut Vec<u8>, type_code: u16, body: &[u8]| {
            buf.extend_from_slice(&type_code.to_le_bytes());
            buf.extend_from_slice(&0u16.to_le_bytes());
            buf.extend_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
            buf.extend_from_slice(&(body.len() as u16).to_le_bytes());
            buf.extend_from_slice(body);
        };
        let mut boot = [0u8; 512];
        boot[11..13].copy_from_slice(&512u16.to_le_bytes());
        boot[13] = 1;
        boot[14..16].copy_from_slice(&1u16.to_le_bytes());
        boot[16] = 2;
        boot[19..21].copy_from_slice(&8192u16.to_le_bytes()); // 4 MiB
        boot[54..62].copy_from_slice(b"FAT16   ");
        write_record(&mut buf, GHO_REC_BOOT_SECTOR, &boot);
        let mut e = [0u8; 56];
        e[..11].copy_from_slice(b"HELLO   TXT");
        e[11] = 0x20;
        e[26..28].copy_from_slice(&5u16.to_le_bytes());
        e[28..32].copy_from_slice(&11u32.to_le_bytes());
        write_record(&mut buf, GHO_REC_DIR_ENTRY_ROOT, &e);
        write_record(&mut buf, GHO_REC_FILE_TAIL, b"hello world");
        let mut c = [0u8; 20];
        c[..4].copy_from_slice(&0xdeadbeefu32.to_le_bytes());
        c[4..8].copy_from_slice(&0xdeadbeefu32.to_le_bytes());
        write_record(&mut buf, GHO_REC_FILE_CHECKSUM, &c);

        // Write to a tempfile so GhoReader::open can pull it in.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("syn.gho");
        std::fs::write(&path, &buf).unwrap();

        let mut reader = GhoReader::open(&path).expect("GhoReader::open file-aware");
        let logical = reader.logical_size();
        assert!(
            logical >= 4 * 1024 * 1024,
            "logical size at least source partition"
        );
        let mut streamed = Vec::with_capacity(logical as usize);
        reader.read_to_end(&mut streamed).unwrap();
        assert_eq!(streamed.len() as u64, logical);
        // First sector should look like a FAT boot sector.
        assert_eq!(streamed[0], 0xEB);
        assert_eq!(&streamed[510..512], &[0x55, 0xAA]);

        // Reopen the streamed bytes as a real FAT image.
        let mut img_cur = Cursor::new(streamed);
        let mut fs = FatFilesystem::open(&mut img_cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let hello = entries
            .iter()
            .find(|e| e.name == "HELLO.TXT")
            .expect("HELLO.TXT must round-trip through the streaming reader");
        let data = fs.read_file(hello, usize::MAX).unwrap();
        assert_eq!(&data, b"hello world");
    }

    // ---------- GhoReader (streaming Read+Seek) ----------

    #[test]
    fn gho_reader_uncompressed_sector_matches_decode_to_temp() {
        let mut s0 = [0u8; 512];
        s0[0..3].copy_from_slice(b"\xeb\x58\x90");
        s0[510] = 0x55;
        s0[511] = 0xAA;
        let mut s1 = [0u8; 512];
        s1[..16].copy_from_slice(b"second sector...");
        let buf = build_sector_mode_uncompressed(&[s0, s1]);
        let path = write_to_temp(&buf);

        // Reader path
        let mut reader = GhoReader::open(&path).unwrap();
        assert_eq!(reader.logical_size(), 1024);
        let mut via_reader = Vec::new();
        reader.read_to_end(&mut via_reader).unwrap();

        // Temp-file path
        let mat = materialize_gho_to_temp(&path).unwrap();
        let via_temp = std::fs::read(&mat.temp_path).unwrap();

        assert_eq!(via_reader, via_temp);

        // Random-access seek
        let mut buf2 = [0u8; 16];
        reader.seek(SeekFrom::Start(512)).unwrap();
        reader.read_exact(&mut buf2).unwrap();
        assert_eq!(&buf2, b"second sector...");
    }

    #[test]
    fn gho_reader_rejects_file_aware() {
        let buf = build_header(0x00, 0x00, 0x00, None);
        let path = write_to_temp(&buf);
        let err = GhoReader::open(&path).err().expect("must error");
        let msg = format!("{err:#}").to_lowercase();
        assert!(
            msg.contains("record stream") || msg.contains("ntfs") || msg.contains("sector"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn gho_reader_rejects_password_protected() {
        let buf = build_header(0x03, 0x01, 0x01, None);
        let path = write_to_temp(&buf);
        let err = GhoReader::open(&path).err().expect("must error");
        assert!(format!("{err:#}").to_lowercase().contains("password"));
    }

    #[test]
    fn gho_reader_requires_password_when_protected() {
        let buf = build_header(0x00, 0x00, 0x01, None);
        let path = write_to_temp(&buf);
        let err = GhoReader::open_with_password(&path, None)
            .err()
            .expect("must error");
        let m = format!("{err:#}").to_lowercase();
        assert!(m.contains("password is required"), "got: {m}");
    }

    #[test]
    fn gho_reader_rejects_wrong_password() {
        // build_header writes a 0xAB*16 verifier, which no real password
        // decrypts to "BinaryResearch\0", so any password is "wrong".
        let buf = build_header(0x00, 0x00, 0x01, None);
        let path = write_to_temp(&buf);
        let err = GhoReader::open_with_password(&path, Some(b"whatever"))
            .err()
            .expect("must error");
        let m = format!("{err:#}").to_lowercase();
        assert!(m.contains("incorrect password"), "got: {m}");
    }

    /// Definitive decryption check against real fixtures: every data-block
    /// body of an encrypted GHO must decrypt (through the real `SpanReader`
    /// path) bit-exact to the same body in its unencrypted twin. Ignored by
    /// default — set `RB_GHO_PLAIN` + `RB_GHO_CIPHER` (and optionally
    /// `RB_GHO_PASSWORD`, default "password") to run it.
    #[test]
    #[ignore = "needs external GHO twin fixtures via RB_GHO_PLAIN / RB_GHO_CIPHER"]
    fn decrypts_real_fixture_data_blocks_bit_exact() {
        let (plain, cipher) = match (
            std::env::var("RB_GHO_PLAIN"),
            std::env::var("RB_GHO_CIPHER"),
        ) {
            (Ok(p), Ok(c)) => (std::path::PathBuf::from(p), std::path::PathBuf::from(c)),
            _ => {
                eprintln!("skip: set RB_GHO_PLAIN and RB_GHO_CIPHER");
                return;
            }
        };
        let pw = std::env::var("RB_GHO_PASSWORD").unwrap_or_else(|_| "password".into());

        // Cipher reader with decryption enabled (mirrors open_with_password).
        let mut csr = SpanReader::open(std::slice::from_ref(&cipher)).unwrap();
        let file_size = csr.total_len();
        let seed = gho_seed_from_password(pw.as_bytes());
        let header_end = (GHO_HEADER_PREFIX_LEN + GHO_PASSWORD_VERIFIER_LEN) as u64;
        let bodies = collect_gho_body_ranges(&mut csr, header_end, file_size).unwrap();
        csr.enable_decryption(seed, bodies);

        // Two plain readers: one to walk records, one to read bodies.
        let mut psr = SpanReader::open(std::slice::from_ref(&plain)).unwrap();
        let mut walk = SpanReader::open(std::slice::from_ref(&plain)).unwrap();
        let pstart = find_inner_stream_start(&mut walk, GHO_HEADER_PREFIX_LEN as u64).unwrap();
        let mut iter = GhoRecordIter::new(&mut walk, pstart).unwrap();

        let mut checked = 0u64;
        loop {
            let off = iter.current_offset();
            match iter.next() {
                Some(Ok(h)) => {
                    if matches!(h.type_code, 0x0002 | 0x0102) && h.body_len > 0 {
                        let bs = off + GHO_RECORD_HEADER_LEN as u64;
                        let len = h.body_len as usize;
                        let mut pb = vec![0u8; len];
                        psr.seek(SeekFrom::Start(bs)).unwrap();
                        psr.read_exact(&mut pb).unwrap();
                        let mut cb = vec![0u8; len];
                        csr.seek(SeekFrom::Start(bs)).unwrap();
                        csr.read_exact(&mut cb).unwrap();
                        checked += 1;
                        assert_eq!(pb, cb, "data block at {bs} decrypted incorrectly");
                    }
                }
                Some(Err(_)) | None => break,
            }
        }
        eprintln!("checked {checked} data blocks, all bit-exact");
        assert!(checked > 0, "no data blocks walked");
    }

    /// Exercise the `SpanReader` body-decryption layer directly: a synthetic
    /// stream of `[container header][record header][encrypted body]...` must
    /// read back with bodies decrypted and headers untouched, under both
    /// sequential and random access.
    #[test]
    fn span_reader_decrypts_bodies_and_passes_headers_through() {
        use crate::rbformats::gho_crypto::gho_encrypt_body;
        use std::io::Read as _;

        let seed = gho_seed_from_password(b"secret");
        let plain1: Vec<u8> = b"The quick brown fox jumps over the lazy dog 0123456789".to_vec();
        let plain2: Vec<u8> = (0..500u32)
            .map(|i| (i.wrapping_mul(37) ^ 0xC3) as u8)
            .collect();

        let rec_hdr = |typ: u16, body_len: u16| -> [u8; 10] {
            let mut h = [0u8; 10];
            h[0..2].copy_from_slice(&typ.to_le_bytes());
            h[4..8].copy_from_slice(&GHO_RECORD_MAGIC.to_le_bytes());
            h[8..10].copy_from_slice(&body_len.to_le_bytes());
            h
        };

        // On-disk stream: 28-byte container header, then two records.
        let mut disk = vec![0u8; 28];
        disk[0] = 0xFE;
        disk[1] = 0xEF;

        disk.extend_from_slice(&rec_hdr(0x0002, plain1.len() as u16));
        let body1_off = disk.len() as u64;
        let mut enc1 = plain1.clone();
        gho_encrypt_body(&mut enc1, seed);
        disk.extend_from_slice(&enc1);

        disk.extend_from_slice(&rec_hdr(0x0002, plain2.len() as u16));
        let body2_off = disk.len() as u64;
        let mut enc2 = plain2.clone();
        gho_encrypt_body(&mut enc2, seed);
        disk.extend_from_slice(&enc2);

        // Expected decrypted view: headers identical, bodies plaintext.
        let mut expected = disk.clone();
        expected[body1_off as usize..body1_off as usize + plain1.len()].copy_from_slice(&plain1);
        expected[body2_off as usize..body2_off as usize + plain2.len()].copy_from_slice(&plain2);

        let path = write_to_temp(&disk);
        let mut sr = SpanReader::open(std::slice::from_ref(&path)).unwrap();
        sr.enable_decryption(
            seed,
            vec![
                (body1_off, plain1.len() as u32),
                (body2_off, plain2.len() as u32),
            ],
        );

        // Sequential read of the whole stream.
        let mut got = Vec::new();
        sr.read_to_end(&mut got).unwrap();
        assert_eq!(got, expected, "sequential decrypt mismatch");

        // Random access into the middle of body2 (cache fill).
        sr.seek(SeekFrom::Start(body2_off + 123)).unwrap();
        let mut b = [0u8; 32];
        sr.read_exact(&mut b).unwrap();
        assert_eq!(&b, &plain2[123..155]);

        // Random access back into body1 (cache eviction + re-decrypt).
        sr.seek(SeekFrom::Start(body1_off + 4)).unwrap();
        let mut b1 = [0u8; 10];
        sr.read_exact(&mut b1).unwrap();
        assert_eq!(&b1, &plain1[4..14]);

        // Header bytes pass through unchanged.
        sr.seek(SeekFrom::Start(0)).unwrap();
        let mut hdr = [0u8; 2];
        sr.read_exact(&mut hdr).unwrap();
        assert_eq!(&hdr, &[0xFE, 0xEF]);
    }

    /// Uncompressed SECTOR-mode framing: a contiguous image encrypted in fixed
    /// chunks (each reset to the seed), with a plaintext sub-header region
    /// before `data_start`. Mirrors what Ghost writes for `-ia` + password.
    #[test]
    fn span_reader_chunked_sector_decryption() {
        use crate::rbformats::gho_crypto::gho_encrypt_body;
        use std::io::Read as _;

        let seed = gho_seed_from_password(b"hunter2");
        let data_start = 0x600u64; // plaintext container + sub-header region
        let chunk = 64u64;

        // A plaintext "disk image" spanning several chunks (200 = 3*64 + 8, so
        // the final chunk is short — exercises the partial-tail path).
        let image: Vec<u8> = (0..200u32)
            .map(|i| (i.wrapping_mul(91) ^ 0x5A) as u8)
            .collect();

        // On disk: data_start plaintext bytes, then each `chunk`-sized slice of
        // the image encrypted independently (cipher reset per chunk).
        let mut disk = vec![0u8; data_start as usize];
        disk[0] = 0xFE;
        disk[1] = 0xEF;
        for off in (0..image.len()).step_by(chunk as usize) {
            let end = (off + chunk as usize).min(image.len());
            let mut seg = image[off..end].to_vec();
            gho_encrypt_body(&mut seg, seed);
            disk.extend_from_slice(&seg);
        }
        let end = disk.len() as u64;

        let path = write_to_temp(&disk);
        let mut sr = SpanReader::open(std::slice::from_ref(&path)).unwrap();
        sr.enable_chunked_decryption(seed, data_start, chunk, end);

        // Sequential read: plaintext sub-header passthrough + decrypted image.
        let mut got = Vec::new();
        sr.read_to_end(&mut got).unwrap();
        let mut expected = vec![0u8; data_start as usize];
        expected[0] = 0xFE;
        expected[1] = 0xEF;
        expected.extend_from_slice(&image);
        assert_eq!(got, expected, "sequential chunked decrypt mismatch");

        // Random access straddling a chunk boundary (image[60..70)).
        sr.seek(SeekFrom::Start(data_start + 60)).unwrap();
        let mut b = [0u8; 10];
        sr.read_exact(&mut b).unwrap();
        assert_eq!(&b, &image[60..70], "cross-chunk random read");

        // Into the short final chunk (image[195..200)).
        sr.seek(SeekFrom::Start(data_start + 195)).unwrap();
        let mut tail = [0u8; 5];
        sr.read_exact(&mut tail).unwrap();
        assert_eq!(&tail, &image[195..200], "final short chunk");

        // Sub-header bytes before data_start pass through unchanged.
        sr.seek(SeekFrom::Start(0)).unwrap();
        let mut hdr = [0u8; 2];
        sr.read_exact(&mut hdr).unwrap();
        assert_eq!(&hdr, &[0xFE, 0xEF]);
    }
}
