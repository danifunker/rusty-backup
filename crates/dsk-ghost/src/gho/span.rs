//! Span sets (`.GHS` / `.GHO.NNN` siblings), the spanned reader, body decryption and sector-mode decoding.

use super::*;

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
    let dir = picked
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
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
pub(super) fn split_gho_dot_numeric(name: &str) -> Option<(String, u32)> {
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

pub(super) fn gather_dot_numeric_siblings(
    dir: &Path,
    logical_stem: &str,
) -> Result<Vec<std::path::PathBuf>> {
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
pub(super) fn strip_trailing_numeric(s: &str) -> &str {
    let end = s
        .rfind(|c: char| !c.is_ascii_digit())
        .map(|i| i + 1)
        .unwrap_or(0);
    &s[..end]
}

pub(super) fn same_path(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(x), Ok(y)) => x == y,
        _ => a == b,
    }
}

pub(super) fn gather_ghs_siblings(
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
        // Exact match only: `backup` must not pull in `backupold001.ghs` from another image.
        if !prefixes.contains(&stem_prefix) {
            continue;
        }
        if ext == "gho" {
            // The picked .GHO is the primary; otherwise the first one found.
            if primary.is_none() || same_path(&entry.path(), picked) {
                primary = Some(entry.path());
            }
        } else {
            // .GHS span. Key by name for deterministic sort.
            spans.push((name.to_ascii_lowercase(), entry.path()));
        }
    }
    spans.sort_by(|a, b| a.0.cmp(&b.0));
    // Spans are numbered 001, 002, ...: a gap means a missing piece, and reading on would
    // serve the next span's bytes in its place.
    for (i, (name, _)) in spans.iter().enumerate() {
        let stem = Path::new(name)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        let digits: String = stem
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit())
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        if let Ok(n) = digits.parse::<usize>() {
            if n != i + 1 {
                bail!(
                    "Ghost span {} is missing from {} (found {name})",
                    i + 1,
                    dir.display()
                );
            }
        }
    }

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
        if !out.first().is_some_and(|x| same_path(x, &p)) {
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
pub(super) enum GhoEncLayout {
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
pub(super) struct GhoBodyDecryptor {
    pub(super) seed: u16,
    pub(super) layout: GhoEncLayout,
    /// Single-unit decrypt cache (reads are largely sequential within a unit).
    pub(super) cache_start: u64,
    pub(super) cache: Vec<u8>,
    pub(super) cache_valid: bool,
}

impl GhoBodyDecryptor {
    /// Return the encrypted unit `(start, len)` containing `pos`, if any.
    pub(super) fn body_at(&self, pos: u64) -> Option<(u64, u32)> {
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
    pub(super) fn next_body_start(&self, pos: u64) -> Option<u64> {
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
    pub(super) files: Vec<File>,
    /// Cumulative virtual offsets — `offsets[i]` is the byte position of
    /// `files[i]`'s exposed bytes in the virtual stream. `offsets[N]`
    /// equals the total virtual length.
    pub(super) offsets: Vec<u64>,
    pub(super) pos: u64,
    pub(super) total: u64,
    /// `Some` for password-protected images — decrypts record bodies on read.
    pub(super) crypt: Option<GhoBodyDecryptor>,
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
    pub(super) fn map(&self, virt: u64) -> Option<(usize, u64, u64)> {
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
    pub(super) fn read_raw_advancing(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
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
    pub(super) fn read_exact_raw_at(&mut self, off: u64, out: &mut [u8]) -> std::io::Result<()> {
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
    pub(super) fn ensure_body_cached(&mut self, bstart: u64, blen: u32) -> std::io::Result<()> {
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

pub(super) fn decode_sector_mode_to_temp<R: Read + Seek + DataLen>(
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
pub(super) fn find_sector_data_start<R: Read + Seek>(
    reader: &mut R,
    file_size: u64,
) -> Result<u64> {
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

pub(super) fn read_fully_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match reader.read(&mut buf[total..])? {
            0 => break,
            n => total += n,
        }
    }
    Ok(total)
}

pub(super) fn decode_sector_block_stream<R: Read + Seek, W: Write>(
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
            // SECTOR-mode High blocks are pure zlib streams (no 4-byte prefix), unlike the
            // file-aware 0x0002 records.
            GhoCompression::High => zlib_decode_block(block, &mut decoded)
                .with_context(|| format!("GHO SECTOR zlib block at offset {:#x}", off))?,
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
