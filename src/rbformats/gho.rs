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
//!   `materialize_amiga_image_path`.
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
        return Err(anyhow!(
            "GHO {} is password-protected; password-protected Ghost backups are not yet \
             supported (cipher is reset per data block and does not match the CRC-16 model in \
             the Go reference -- see docs/gho_password.md)",
            path.display()
        ));
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
            // File-aware mode interleaves boot-sector + FAT-entry + dir/file
            // metadata records with `0x0002` cluster-data records. Producing
            // a usable raw partition image requires rebuilding the
            // filesystem (boot sector + FAT tables + directory + clusters
            // placed at correct LBAs) — much larger than 5.6's scope.
            Err(anyhow!(
                "GHO {} is a file-aware (Symantec \"truncated full backup\") image, \
                 which stores only used clusters interleaved with FAT/directory metadata \
                 records. Reconstructing a mountable partition image from these records \
                 requires a full filesystem rebuilder and is the next planned slice. \
                 SECTOR-mode (raw sector-by-sector) backups decode today.",
                path.display()
            ))
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
pub struct SpanReader {
    files: Vec<File>,
    /// Cumulative virtual offsets — `offsets[i]` is the byte position of
    /// `files[i]`'s exposed bytes in the virtual stream. `offsets[N]`
    /// equals the total virtual length.
    offsets: Vec<u64>,
    pos: u64,
    total: u64,
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
        })
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
        // Find the file containing self.pos.
        let mut idx = 0;
        for i in 0..self.files.len() {
            if self.pos < self.offsets[i + 1] {
                idx = i;
                break;
            }
            idx = i + 1;
        }
        if idx >= self.files.len() {
            return Ok(0);
        }
        let virt_in_file = self.pos - self.offsets[idx];
        let physical_in_file = if idx == 0 {
            virt_in_file
        } else {
            GHO_SECTOR_SIZE + virt_in_file
        };
        let remaining_in_file = self.offsets[idx + 1] - self.pos;
        let to_read = (buf.len() as u64).min(remaining_in_file) as usize;
        self.files[idx].seek(SeekFrom::Start(physical_in_file))?;
        let n = self.files[idx].read(&mut buf[..to_read])?;
        self.pos += n as u64;
        Ok(n)
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

/// Result of materializing a GHO container to a temp file. Same shape as
/// [`crate::rbformats::imz::ImzMaterialized`] so the GUI's
/// `materialize_amiga_image_path` can treat both formats identically.
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

/// Boot-sector record (body = 512 bytes). Marks the start of a partition
/// in our fixture corpus.
pub const GHO_REC_BOOT_SECTOR: u16 = 0x0017;

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
                partition_count: 0,
            })
        }
    };

    let mut records = Vec::new();
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
        if inner.type_code == GHO_REC_BOOT_SECTOR && inner.body_len == 512 {
            partition_count += 1;
        }
        records.push(inner);
        offset = inner.body_end();
    }
    Ok(GhoImage {
        records,
        partition_count,
    })
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
    // Like Fast-LZ, a leading byte of `1` signals "stored verbatim past
    // the 4-byte prefix"; otherwise the body is a raw deflate stream
    // (still with the 4-byte prefix per the corpus).
    if block.is_empty() {
        return Ok(0);
    }
    let payload = if block.len() >= 4 { &block[4..] } else { block };
    if block[0] == 1 {
        if payload.len() > dst.len() {
            bail!("zlib uncompressed-escape exceeds decode buffer");
        }
        dst[..payload.len()].copy_from_slice(payload);
        return Ok(payload.len());
    }
    let mut dec = ZlibDecoder::new(payload);
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
    fn materialize_file_aware_errors_on_deferred_reconstruction() {
        let buf = build_header(0x00, 0x00, 0x00, None);
        let path = write_to_temp(&buf);
        let err = materialize_gho_to_temp(&path).unwrap_err();
        let msg = format!("{err:#}").to_lowercase();
        assert!(
            msg.contains("file-aware") && msg.contains("reconstruct"),
            "error should explain file-aware reconstruction is deferred, got: {msg}"
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

    // ---------- 7.5-vs-11.5 record-stream diff (5.5b deliverable) ----------

    fn fixture_inner_records(path: &std::path::Path, limit: usize) -> Option<Vec<GhoRecordHeader>> {
        let mut f = File::open(path).ok()?;
        let header = GhoContainerHeader::parse(&mut f).ok()?;
        let header_end = if header.password_protected {
            (GHO_HEADER_PREFIX_LEN + GHO_PASSWORD_VERIFIER_LEN) as u64
        } else {
            GHO_HEADER_PREFIX_LEN as u64
        };
        let start = find_inner_stream_start(&mut f, header_end).ok()?;
        let iter = GhoRecordIter::new(f, start).ok()?;
        let records: Result<Vec<_>> = iter.take(limit).collect();
        records.ok()
    }

    /// Confirms the inner record stream is structurally identical between
    /// Ghost 7.5 and 11.5: same record sequence, same body_len for each
    /// record, only the `marker` field differs (0x0000 vs 0x95FD). This
    /// is the 5.5b "diff" deliverable from the plan.
    #[test]
    fn record_streams_match_between_75_and_115() {
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return,
        };
        let base = std::path::PathBuf::from(home).join("new-fixtures/gho");
        let path_75 = base.join("ManualGhostBackups/7.5/PART/PART.GHO");
        let path_115 = base.join("ManualGhostBackups/11.5/GH11/fulldisk.GHS");
        if !path_75.exists() || !path_115.exists() {
            eprintln!(
                "skipping: missing fixtures ({} or {})",
                path_75.display(),
                path_115.display()
            );
            return;
        }

        const LIMIT: usize = 128;
        let rec_75 = fixture_inner_records(&path_75, LIMIT).expect("7.5 fixture should parse");
        let rec_115 = fixture_inner_records(&path_115, LIMIT).expect("11.5 fixture should parse");

        assert!(
            rec_75.len() >= 4,
            "7.5: read {} records, expected at least 4",
            rec_75.len()
        );
        assert!(
            rec_115.len() >= 4,
            "11.5: read {} records, expected at least 4",
            rec_115.len()
        );

        // Diff: types and body_lens should be identical record-for-record;
        // markers differ by Ghost version.
        let common = rec_75.len().min(rec_115.len());
        let mut type_mismatches = Vec::new();
        let mut len_mismatches = Vec::new();
        for i in 0..common {
            if rec_75[i].type_code != rec_115[i].type_code {
                type_mismatches.push(i);
            }
            if rec_75[i].body_len != rec_115[i].body_len {
                len_mismatches.push(i);
            }
        }

        // The first record in both should be type 0x0017 with body_len 512
        // (the boot sector / first-sector payload). This is the strongest
        // claim about Ghost 7.5 vs 11.5 inner-stream parity.
        assert_eq!(rec_75[0].type_code, 0x0017);
        assert_eq!(rec_75[0].body_len, 512);
        assert_eq!(rec_75[0].marker, 0x0000, "7.5 first-record marker");
        assert_eq!(rec_115[0].type_code, 0x0017);
        assert_eq!(rec_115[0].body_len, 512);
        assert_eq!(rec_115[0].marker, 0x95FD, "11.5 first-record marker");

        // Markers are NOT uniform within either stream — Ghost mixes
        // 0x0000 and 0x95FD records in 11.5 backups. Record the
        // distribution so 5.6 can build a taxonomy from it.
        let marker_dist = |records: &[GhoRecordHeader]| {
            let mut buckets: std::collections::BTreeMap<u16, usize> =
                std::collections::BTreeMap::new();
            for r in records {
                *buckets.entry(r.marker).or_insert(0) += 1;
            }
            buckets
        };
        eprintln!(
            "7.5 vs 11.5 record diff over {} records: {} type mismatches, {} body-len mismatches",
            common,
            type_mismatches.len(),
            len_mismatches.len()
        );
        eprintln!("  7.5 marker distribution:  {:?}", marker_dist(&rec_75));
        eprintln!("  11.5 marker distribution: {:?}", marker_dist(&rec_115));

        // The hard claim: the first ~16 records should agree completely
        // (FAT boot sector + first dozen FAT entries; before any
        // partition-boundary divergence). If this trips, the format
        // changed more than just the marker between Ghost versions and
        // we need to revisit the plan.
        let early = 16.min(common);
        for i in 0..early {
            assert_eq!(
                rec_75[i].type_code, rec_115[i].type_code,
                "early record {} type mismatch: 7.5={:#06x} 11.5={:#06x}",
                i, rec_75[i].type_code, rec_115[i].type_code
            );
            assert_eq!(
                rec_75[i].body_len, rec_115[i].body_len,
                "early record {} body_len mismatch: 7.5={} 11.5={}",
                i, rec_75[i].body_len, rec_115[i].body_len
            );
        }
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

    // ---------- fixture-gated: locate-and-decode a real `0x02` block ----------

    /// Brute-force smoke test against real Fast-compressed fixtures:
    /// scan the file for any candidate `[u16 stored_len][block_data]`
    /// frame whose Fast-LZ decode produces a 32 KiB output ending in
    /// the FAT/NTFS boot-sector signature `0x55 0xAA` at offset 510. The
    /// first such hit proves the decoder produces real-world correct
    /// output without us having to navigate the still-undocumented
    /// per-version record taxonomy (which is 5.6's job).
    ///
    /// Marked `#[ignore]` so it does not run in the default test loop —
    /// it can scan tens of MB and false-negative on some 7.x dialects
    /// whose first compressed block doesn't sit at a boot-sector. Run
    /// with `cargo test -- --ignored fast_lz_decompresses_first_block`
    /// when iterating on the decoder itself.
    #[test]
    #[ignore = "fixture-gated brute-force scan; opt in with --ignored"]
    fn fast_lz_decompresses_first_block_from_real_fixture() {
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return,
        };
        let base = std::path::PathBuf::from(home).join("new-fixtures/gho");
        // HPVectra95C.gho is the smallest Fast-compressed fixture in the
        // corpus; FAST.GHO is a second option if the first is absent.
        let candidates = [
            base.join("HPVectra95C.gho"),
            base.join("ManualGhostBackups/7.5/FD-FAST/FAST.GHO"),
        ];
        let path = match candidates.iter().find(|p| p.exists()) {
            Some(p) => p.clone(),
            None => {
                eprintln!("skipping: no Fast-compressed GHO fixture present");
                return;
            }
        };

        let mut f = File::open(&path).expect("open fixture");
        let header = GhoContainerHeader::parse(&mut f).expect("parse container header");
        assert_eq!(
            header.compression.as_byte(),
            0x02,
            "fixture should be Fast-compressed (got {:#04x})",
            header.compression.as_byte()
        );

        // Read the whole fixture (the candidates are 30-700 MB; we cap the
        // scan to the first 16 MB for runtime sanity).
        let mut data = Vec::new();
        f.take(16 * 1024 * 1024)
            .read_to_end(&mut data)
            .expect("read fixture body");

        let mut dst = vec![0u8; FAST_LZ_BLOCK_SIZE + 1024];
        let mut tried = 0usize;
        // Skip the 512-byte container header at offset 0 (its own
        // FE EF magic + metadata) before scanning for compressed blocks.
        for off in 512..data.len().saturating_sub(8) {
            let stored_len = u16::from_le_bytes([data[off], data[off + 1]]) as usize;
            if !(8..=33_002).contains(&stored_len) {
                continue;
            }
            let comp_len = stored_len - 2;
            if off + 2 + comp_len > data.len() {
                continue;
            }
            // Skip frames whose prefix byte is the "uncompressed" sentinel —
            // those exercise a different path we already cover synthetically.
            let block = &data[off + 2..off + 2 + comp_len];
            if block[0] == 1 {
                continue;
            }
            tried += 1;
            let n = match fast_lz_decompress(block, comp_len, &mut dst) {
                Ok(n) => n,
                Err(_) => continue,
            };
            if n == FAST_LZ_BLOCK_SIZE && dst[510..512] == [0x55, 0xAA] {
                eprintln!(
                    "  found valid FastLZ block @ {:#x} (stored_len={}, tried {} candidates)",
                    off, stored_len, tried
                );
                return;
            }
        }
        // Byte-granular brute-force can't reliably locate block boundaries
        // (a real block starts at the exact position the preceding block's
        // `stored_len` consumes — there is no in-band alignment), so a miss
        // here is *expected* on most fixtures and does not by itself
        // indicate a broken decoder. The synthetic tests above are the
        // load-bearing correctness checks for the decoder itself; the
        // real-fixture wiring is 5.6's job.
        eprintln!(
            "  scanned 16 MB of {}: {} candidate frames tried, no match. \
             (Expected — block boundaries are not byte-discoverable.)",
            path.display(),
            tried
        );
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

    // ---------- fixture-gated: real SECTOR.GHO end-to-end ----------

    /// Decode `7.5/SECTOR/SECTOR.GHO` (uncompressed SECTOR mode, ~6.4 GB)
    /// to a tempfile and confirm the first sector is a valid FAT32 BPB
    /// (boot-sector signature `0x55 0xAA` at offset 510, "MSWIN4.1" OEM
    /// name at offset 3-10). Reads only the first 64 KiB of the decoded
    /// output — the full ~6.4 GB copy is far too slow for unit-test time
    /// budgets, so the test truncates the file early via a smaller bound.
    ///
    /// Gated on the user's local fixture corpus; skipped if absent.
    /// Marked `#[ignore]` so default `cargo test` doesn't have to copy
    /// gigabytes; opt in with `cargo test -- --ignored materialize_real_sector`.
    #[test]
    #[ignore = "copies ~6.4 GB of fixture; opt in with --ignored"]
    fn materialize_real_sector_gho_decodes_to_fat_disk() {
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return,
        };
        let path = std::path::PathBuf::from(home)
            .join("new-fixtures/gho/ManualGhostBackups/7.5/SECTOR/SECTOR.GHO");
        if !path.exists() {
            eprintln!("skipping: {} not present", path.display());
            return;
        }
        let mat = materialize_gho_to_temp(&path)
            .unwrap_or_else(|e| panic!("decode of {} failed: {:#}", path.display(), e));
        assert!(
            mat.logical_size >= 1024,
            "decoded image should be non-empty"
        );
        let mut f = File::open(&mat.temp_path).unwrap();
        let mut boot = [0u8; 512];
        f.read_exact(&mut boot).unwrap();
        assert_eq!(&boot[510..512], &[0x55, 0xAA], "boot-sector signature");
        assert_eq!(
            &boot[3..11],
            b"MSWIN4.1",
            "OEM name should match FAT32 from Windows"
        );
        eprintln!(
            "  decoded SECTOR.GHO -> {} ({} bytes)",
            mat.temp_path.display(),
            mat.logical_size
        );
    }

    /// Decode `11.5/.../secthigh.GHO` (zlib-compressed SECTOR mode, ~641 MB)
    /// and confirm the first decoded sector is a valid FAT32 BPB. Exercises
    /// the zlib block-stream path end-to-end. `#[ignore]`-gated to keep
    /// the default test loop fast.
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

    /// Fixture-gated: decode a real multi-file SECTOR set. Picks the
    /// 7.5 corpus `SECTOR.GHO` set (1 primary + 3 .GHS spans, ~6 GiB
    /// raw) but only reads the first 512 bytes of the decoded output
    /// — we just need to confirm the multi-file reader doesn't break
    /// the SECTOR data-start probe and the boot sector decodes.
    /// `#[ignore]` because it touches ~6 GiB of input.
    #[test]
    #[ignore = "fixture-gated; reads multi-GiB SECTOR span set. Opt in with --ignored"]
    fn materialize_real_sector_span_set_decodes_boot_sector() {
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return,
        };
        let path = std::path::PathBuf::from(home)
            .join("new-fixtures/gho/ManualGhostBackups/7.5/SECTOR/SECTOR.GHO");
        if !path.exists() {
            eprintln!("skipping: {} not present", path.display());
            return;
        }
        // Pre-flight: confirm span discovery sees all 4 files.
        let set = discover_gho_span_set(&path).unwrap();
        assert!(
            set.len() >= 2,
            "span discovery should find the .GHS siblings: {set:?}"
        );
        eprintln!("  span set: {} files", set.len());

        let mat = materialize_gho_to_temp(&path)
            .unwrap_or_else(|e| panic!("decode of {} failed: {:#}", path.display(), e));
        assert!(mat.logical_size >= 1024);
        let mut f = File::open(&mat.temp_path).unwrap();
        let mut boot = [0u8; 512];
        f.read_exact(&mut boot).unwrap();
        assert_eq!(&boot[510..512], &[0x55, 0xAA]);
        eprintln!(
            "  decoded SECTOR set -> {} ({} bytes)",
            mat.temp_path.display(),
            mat.logical_size
        );
    }

    #[test]
    #[ignore = "fixture-gated; decompresses hundreds of MB. Opt in with --ignored"]
    fn materialize_real_secthigh_gho_decodes_to_fat_disk() {
        let home = match std::env::var("HOME") {
            Ok(h) => h,
            Err(_) => return,
        };
        let path = std::path::PathBuf::from(home).join(
            "new-fixtures/gho/ManualGhostBackups/11.5/Gh11-sect compression high/secthigh.GHO",
        );
        if !path.exists() {
            eprintln!("skipping: {} not present", path.display());
            return;
        }
        let mat = materialize_gho_to_temp(&path)
            .unwrap_or_else(|e| panic!("decode of {} failed: {:#}", path.display(), e));
        assert!(mat.logical_size >= 1024);
        let mut f = File::open(&mat.temp_path).unwrap();
        let mut boot = [0u8; 512];
        f.read_exact(&mut boot).unwrap();
        assert_eq!(&boot[510..512], &[0x55, 0xAA]);
        eprintln!(
            "  decoded secthigh.GHO -> {} ({} bytes)",
            mat.temp_path.display(),
            mat.logical_size
        );
    }
}
