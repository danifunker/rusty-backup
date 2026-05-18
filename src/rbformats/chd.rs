use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{Context, Result};
use libchdman_rs::{
    dvd::{create_from_reader as dvd_create_from_reader, DvdCreateOptions, DVD_SECTOR_SIZE},
    hd::{create_from_reader, HdCreateOptions},
    ChdError, CompressionProgress,
};

use super::chd_options::{ChdOptions, ChdProfile};
use super::{file_name, output_path, CHUNK_SIZE};

/// Read+Seek adapter over a CHD file backed by libchdman-rs. Enables
/// filesystem browsing without extracting to a temp file.
pub struct ChdReader {
    chd: libchdman_rs::Chd,
    logical_size: u64,
    position: u64,
}

// SAFETY: `libchdman_rs::Chd` holds a raw pointer to a heap-allocated CHD file
// handle. Operations are routed through &self, but the handle is only accessed
// from one thread at a time (we hand the reader off to a worker thread). The
// underlying C++ object is not shared across threads, so Send is sound.
unsafe impl Send for ChdReader {}

impl ChdReader {
    /// Open a CHD file for reading.
    /// Logical (uncompressed) byte length of the CHD's contents.
    pub fn logical_size(&self) -> u64 {
        self.logical_size
    }

    pub fn open(path: &Path) -> Result<Self> {
        let path_str = path
            .to_str()
            .with_context(|| format!("CHD path is not valid UTF-8: {}", path.display()))?;
        let chd = libchdman_rs::Chd::open(path_str, false, None)
            .map_err(|e| anyhow::anyhow!("failed to open CHD {}: {:?}", path.display(), e))?;
        let logical_size = chd.logical_bytes();
        Ok(Self {
            chd,
            logical_size,
            position: 0,
        })
    }
}

/// Render a human-readable summary of a CHD file, mimicking the layout of
/// `chdman info`. Used by the GUI's "CHD Info" button so users can inspect
/// version, codecs, hunk/unit size, SHA1s, and metadata tags without leaving
/// the app.
pub fn format_chd_info(path: &Path) -> Result<String> {
    use libchdman_rs::cd;
    use libchdman_rs::codec::codec_name;

    let path_str = path
        .to_str()
        .with_context(|| format!("CHD path is not valid UTF-8: {}", path.display()))?;
    let chd = libchdman_rs::Chd::open(path_str, false, None)
        .map_err(|e| anyhow::anyhow!("failed to open CHD {}: {:?}", path.display(), e))?;
    let info = chd
        .info()
        .map_err(|e| anyhow::anyhow!("failed to read CHD info: {:?}", e))?;

    let file_size = fs::metadata(path).map(|m| m.len()).ok();
    let total_units = if info.unit_bytes > 0 {
        info.logical_bytes / info.unit_bytes as u64
    } else {
        0
    };

    let mut out = String::new();
    out.push_str(&format!("Input file:   {}\n", path.display()));
    let kind = if info.is_cd {
        "CD"
    } else if info.is_dvd {
        "DVD"
    } else if info.is_gd {
        "GD"
    } else if info.is_hd {
        "HD"
    } else if info.is_av {
        "AV"
    } else {
        "Unknown"
    };
    out.push_str(&format!("Type:         {}\n", kind));
    out.push_str(&format!("File Version: {}\n", info.version));
    out.push_str(&format!(
        "Logical size: {} bytes ({})\n",
        format_with_commas(info.logical_bytes),
        format_size_human(info.logical_bytes),
    ));
    out.push_str(&format!(
        "Hunk Size:    {} bytes\n",
        format_with_commas(info.hunk_bytes as u64)
    ));
    out.push_str(&format!(
        "Total Hunks:  {}\n",
        format_with_commas(info.hunk_count as u64)
    ));
    out.push_str(&format!(
        "Unit Size:    {} bytes\n",
        format_with_commas(info.unit_bytes as u64)
    ));
    out.push_str(&format!(
        "Total Units:  {}\n",
        format_with_commas(total_units)
    ));

    out.push_str("Compression:  ");
    if info.compressed {
        let parts: Vec<String> = info
            .codecs
            .iter()
            .filter(|c| **c != 0)
            .map(|c| {
                let label = super::chd_options::codec_label(*c);
                let long = codec_name(*c).unwrap_or("");
                if long.is_empty() {
                    label
                } else {
                    format!("{} ({})", label, long)
                }
            })
            .collect();
        if parts.is_empty() {
            out.push_str("none");
        } else {
            out.push_str(&parts.join(", "));
        }
    } else {
        out.push_str("none");
    }
    out.push('\n');

    if let Some(fs) = file_size {
        out.push_str(&format!(
            "CHD size:     {} bytes ({})\n",
            format_with_commas(fs),
            format_size_human(fs),
        ));
        if info.logical_bytes > 0 {
            let ratio = (fs as f64 / info.logical_bytes as f64) * 100.0;
            out.push_str(&format!("Ratio:        {:.1}%\n", ratio));
        }
    }

    out.push_str(&format!("SHA1:         {}\n", hex_lower(&info.sha1)));
    if info.version >= 4 {
        out.push_str(&format!("Data SHA1:    {}\n", hex_lower(&info.raw_sha1)));
    }
    if info.has_parent {
        out.push_str(&format!("Parent SHA1:  {}\n", hex_lower(&info.parent_sha1)));
    }

    if !info.metadata_tags.is_empty() {
        out.push_str("Metadata:\n");
        for (tag, index) in &info.metadata_tags {
            out.push_str(&format!(
                "  Tag='{}'  Index={}\n",
                fourcc_to_string(*tag),
                index,
            ));
        }
    }

    if info.is_cd || info.is_gd {
        match cd::list_tracks(&chd) {
            Ok(tracks) if !tracks.is_empty() => {
                out.push_str("Tracks:\n");
                for t in &tracks {
                    out.push_str(&format!(
                        "  Track {:>2}: {:?}  frames={}  pregap={}  postgap={}  subcode={:?}\n",
                        t.track_num,
                        t.track_type,
                        format_with_commas(t.frames as u64),
                        t.pregap,
                        t.postgap,
                        t.subcode_type,
                    ));
                }
            }
            Ok(_) => {}
            Err(e) => {
                out.push_str(&format!("Tracks:       (failed to read: {:?})\n", e));
            }
        }
    }

    Ok(out)
}

fn fourcc_to_string(code: u32) -> String {
    let bytes = [
        ((code >> 24) & 0xff) as u8,
        ((code >> 16) & 0xff) as u8,
        ((code >> 8) & 0xff) as u8,
        (code & 0xff) as u8,
    ];
    if bytes.iter().all(|b| (0x20..=0x7e).contains(b)) {
        String::from_utf8(bytes.to_vec()).unwrap()
    } else {
        format!("{:08x}", code)
    }
}

fn hex_lower(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{:02x}", byte));
    }
    s
}

fn format_with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out.chars().rev().collect()
}

fn format_size_human(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    let b = bytes as f64;
    if b >= GIB {
        format!("{:.2} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.2} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.2} KiB", b / KIB)
    } else {
        format!("{} B", bytes)
    }
}

/// Probe a CHD file to determine if it is a CD CHD (vs HD/DVD).
///
/// CD CHDs store 2352-byte raw sectors + 96-byte subcode = 2448-byte frames,
/// which can't be parsed directly as a flat byte stream by ISO9660/UDF code.
/// Use [`CdCookedReader::open_path`] to expose the cooked 2048-byte user data.
pub fn chd_is_cd(path: &Path) -> Result<bool> {
    let path_str = path
        .to_str()
        .with_context(|| format!("CHD path is not valid UTF-8: {}", path.display()))?;
    let chd = libchdman_rs::Chd::open(path_str, false, None)
        .map_err(|e| anyhow::anyhow!("failed to open CHD {}: {:?}", path.display(), e))?;
    let info = chd
        .info()
        .map_err(|e| anyhow::anyhow!("failed to read CHD info: {:?}", e))?;
    Ok(info.is_cd)
}

/// Read+Seek adapter over a CD CHD's cooked MODE1 user data (2048 B/sector).
///
/// Single-track MODE1 / MODE1_RAW CHDs only — multi-track or audio mixes return
/// an error. Mirrors [`ChdReader`] but the underlying frames are decoded by
/// libchdman-rs's `cdrom` shim so ISO9660/UDF code sees a flat 2048-byte stream.
pub struct CdCookedReader {
    inner: libchdman_rs::cd::CdCookedReader,
}

// SAFETY: identical reasoning to ChdReader — single-threaded access via &mut self.
unsafe impl Send for CdCookedReader {}

impl CdCookedReader {
    pub fn open_path(path: &Path) -> Result<Self> {
        let path_str = path
            .to_str()
            .with_context(|| format!("CHD path is not valid UTF-8: {}", path.display()))?;
        let chd = libchdman_rs::Chd::open(path_str, false, None)
            .map_err(|e| anyhow::anyhow!("failed to open CHD {}: {:?}", path.display(), e))?;
        let inner = libchdman_rs::cd::CdCookedReader::open(chd).map_err(|e| {
            anyhow::anyhow!(
                "CHD is not a single-track MODE1 CD (multi-track CDs cannot be browsed in-place; extract to BIN/CUE first): {:?}",
                e
            )
        })?;
        Ok(Self { inner })
    }

    pub fn logical_size(&self) -> u64 {
        self.inner.len()
    }
}

impl Read for CdCookedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Seek for CdCookedReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl Read for ChdReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.logical_size {
            return Ok(0);
        }
        let remaining = self.logical_size - self.position;
        let to_read = (buf.len() as u64).min(remaining) as usize;
        if to_read == 0 {
            return Ok(0);
        }
        self.chd
            .read_bytes(self.position, &mut buf[..to_read])
            .map_err(|e| io::Error::other(format!("CHD read error: {:?}", e)))?;
        self.position += to_read as u64;
        Ok(to_read)
    }
}

impl Seek for ChdReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::Current(n) => self.position as i64 + n,
            SeekFrom::End(n) => self.logical_size as i64 + n,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start",
            ));
        }
        self.position = (new_pos as u64).min(self.logical_size);
        Ok(self.position)
    }
}

/// Compress raw partition data into a hard-disk CHD via libchdman-rs.
///
/// `logical_size` is the partition's natural byte length. It is rounded
/// up to the nearest unit (512 byte) boundary for the CHD; any tail
/// shortfall in `reader` is zero-padded by libchdman-rs internally.
/// `opts` selects hunk size + codecs; `None` falls back to chdman's HD
/// defaults (`lzma, zlib, huff, flac`, 4096-byte hunks).
pub(crate) fn compress_chd(
    reader: &mut impl Read,
    output_base: &Path,
    logical_size: u64,
    split_size: Option<u64>,
    opts: Option<ChdOptions>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<Vec<String>> {
    let chd_opts = opts.unwrap_or_else(|| ChdOptions::defaults_for(ChdProfile::Hd));

    // chdman uses 512-byte units for HD CHDs. Round logical_size up so
    // libchdman-rs's `logical_size % unit_size == 0` precondition holds;
    // any padding bytes are written by libchdman-rs as zeros.
    const UNIT_SIZE: u64 = 512;
    let padded_size = logical_size.div_ceil(UNIT_SIZE) * UNIT_SIZE;

    let chd_path = output_path(output_base, "chd", false, 0);
    log_cb(&format!(
        "Writing CHD {} (logical {} bytes, hunk {}, codecs {:?})",
        chd_path.display(),
        padded_size,
        chd_opts.hunk_size,
        chd_opts.codecs,
    ));

    let hd_opts = HdCreateOptions {
        logical_size: padded_size,
        hunk_size: chd_opts.hunk_size,
        unit_size: UNIT_SIZE as u32,
        codecs: chd_opts.codecs,
        geometry: None,
        ident: None,
    };

    let mut progress = |p: CompressionProgress| {
        // Clamp to the user-facing logical size so the progress bar never
        // reports more bytes than the caller knows about (CHD pads up to
        // the next hunk; the caller's "total" is `logical_size`).
        progress_cb(p.bytes_done.min(logical_size));
    };

    create_from_reader(reader, &chd_path, hd_opts, &mut progress, cancel_check).map_err(
        |e| match e {
            ChdError::Cancelled => anyhow::anyhow!("backup cancelled"),
            other => anyhow::anyhow!("CHD create failed: {:?}", other),
        },
    )?;

    // Final progress tick at logical_size — libchdman-rs reports in
    // terms of padded size, but the caller's total is `logical_size`.
    progress_cb(logical_size);

    if let Some(split_bytes) = split_size {
        let chd_size = fs::metadata(&chd_path)
            .with_context(|| format!("failed to stat CHD output: {}", chd_path.display()))?
            .len();

        if chd_size > split_bytes {
            return split_file(&chd_path, output_base, "chd", split_bytes);
        }
    }

    Ok(vec![file_name(&chd_path)])
}

/// Public wrapper around `compress_chd` for the CHD-expand worker
/// (Phase 6c of `docs/disk_expansion.md`). Same signature, just `pub` so
/// model-layer callers can re-encode an existing CHD with a new logical
/// size. The wrapper is intentionally thin so this stays the one place
/// that touches libchdman directly.
pub fn compress_chd_expand(
    reader: &mut impl Read,
    output_base: &Path,
    logical_size: u64,
    split_size: Option<u64>,
    opts: Option<ChdOptions>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<Vec<String>> {
    compress_chd(
        reader,
        output_base,
        logical_size,
        split_size,
        opts,
        progress_cb,
        cancel_check,
        log_cb,
    )
}

/// Report from [`shrink_sgi_disk_to_chd`].
pub struct ShrinkSgiReport {
    /// Logical size of the source (file length for raw, CHD logical size for CHD).
    pub source_logical_size: u64,
    /// Logical size of the newly written CHD = SGI floor in bytes.
    pub new_logical_size: u64,
    /// Highest (first + blocks) across non-empty SGI partition entries.
    pub partition_floor_sectors: u64,
}
impl ShrinkSgiReport {
    pub fn bytes_dropped(&self) -> u64 {
        self.source_logical_size
            .saturating_sub(self.new_logical_size)
    }
}

/// Re-encode an IRIX disk (raw `.img` or existing CHD) into a CHD whose
/// logical size equals the SGI volume header's used floor — i.e. drops
/// trailing zero padding past `max(first + blocks)` over all non-empty
/// partition entries. The on-disk SGI label and partition data are
/// copied verbatim; CHS geometry is auto-derived by libchdman from the
/// new (smaller) logical size, which is fine because IRIX reads its own
/// geometry from inside the SGI label.
///
/// Source detection is by extension: `*.chd` (case-insensitive) is read
/// via [`ChdReader`]; anything else is treated as a raw image and read
/// with a buffered file handle.
///
/// Path safety: refuses if `dst == src` (canonical paths compared),
/// if `dst` already exists, or if `dst` lacks a `.chd` extension. The
/// source is never opened for writing.
pub fn shrink_sgi_disk_to_chd(
    src: &Path,
    dst: &Path,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<ShrinkSgiReport> {
    use crate::partition::sgi::SgiVolumeHeader;

    // Path validation. See `feedback_path_resolution_for_destructive_ops`
    // in memory: this exact dance was missing in an earlier draft and a
    // 14.84 GiB IRIX CHD got truncated to a 1952-byte header because
    // `output_path` re-stemmed back onto the source.
    let src_canon = fs::canonicalize(src)
        .with_context(|| format!("source does not exist: {}", src.display()))?;
    if let Ok(dst_canon) = fs::canonicalize(dst) {
        if dst_canon == src_canon {
            anyhow::bail!(
                "output path {} resolves to the source file — refusing to overwrite",
                dst.display()
            );
        }
    }
    if dst.exists() {
        anyhow::bail!(
            "output already exists: {} (refusing to overwrite; pick a fresh name)",
            dst.display()
        );
    }
    if dst.extension().and_then(|s| s.to_str()) != Some("chd") {
        anyhow::bail!("output path must end in .chd (got {})", dst.display());
    }
    if let Some(parent) = dst.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            anyhow::bail!("output directory does not exist: {}", parent.display());
        }
    }

    let is_chd = src
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.eq_ignore_ascii_case("chd"))
        .unwrap_or(false);

    // Sector-0 read for SGI header parsing.
    let (source_logical, sector0) = if is_chd {
        let mut r = ChdReader::open(src)?;
        let logical = r.logical_size();
        let mut buf = vec![0u8; 512];
        r.read_exact(&mut buf)
            .context("reading sector 0 from source CHD")?;
        (logical, buf)
    } else {
        let meta = fs::metadata(src).with_context(|| format!("stat {}", src.display()))?;
        let mut f = File::open(src).with_context(|| format!("open {}", src.display()))?;
        let mut buf = vec![0u8; 512];
        f.read_exact(&mut buf)
            .context("reading sector 0 from source image")?;
        (meta.len(), buf)
    };

    let vh = SgiVolumeHeader::parse(&sector0)
        .map_err(|e| anyhow::anyhow!("sector 0 is not an SGI volume header: {e}"))?;
    let mut max_end_sectors: u64 = 0;
    for e in &vh.partitions {
        if e.is_empty() {
            continue;
        }
        let end = e.first as u64 + e.blocks as u64;
        if end > max_end_sectors {
            max_end_sectors = end;
        }
    }
    if max_end_sectors == 0 {
        anyhow::bail!("SGI volume header has no non-empty partitions");
    }
    let new_logical = max_end_sectors * 512;
    if new_logical >= source_logical {
        anyhow::bail!(
            "nothing to shrink: SGI floor ({} bytes) >= source size ({} bytes)",
            new_logical,
            source_logical
        );
    }

    log_cb(&format!(
        "SGI floor at sector {} ({} bytes); dropping {} bytes of trailing padding",
        max_end_sectors,
        new_logical,
        source_logical - new_logical,
    ));

    let inner: Box<dyn Read> = if is_chd {
        Box::new(ChdReader::open(src)?)
    } else {
        Box::new(BufReader::new(
            File::open(src).with_context(|| format!("open {}", src.display()))?,
        ))
    };
    let mut truncated = TruncatedReader::new(inner, new_logical);

    // Pass `dst` itself as the encoder's base — `output_path` calls
    // `file_stem()` (strips the trailing `.chd`) and re-appends `.chd`,
    // so the result is exactly `dst`. Critically, do NOT pre-strip the
    // extension; that's the bug that destroyed the original source.
    compress_chd_expand(
        &mut truncated,
        dst,
        new_logical,
        None,
        None,
        progress_cb,
        cancel_check,
        log_cb,
    )?;

    Ok(ShrinkSgiReport {
        source_logical_size: source_logical,
        new_logical_size: new_logical,
        partition_floor_sectors: max_end_sectors,
    })
}

/// Read adapter that caps the wrapped reader to at most `limit` bytes,
/// then signals EOF. Mirror of `ChainedZeroPadReader` in
/// `crate::model::chd_expand_runner`.
struct TruncatedReader {
    inner: Box<dyn Read>,
    remaining: u64,
}
impl TruncatedReader {
    fn new(inner: Box<dyn Read>, limit: u64) -> Self {
        Self {
            inner,
            remaining: limit,
        }
    }
}
impl Read for TruncatedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let cap = (buf.len() as u64).min(self.remaining) as usize;
        let n = self.inner.read(&mut buf[..cap])?;
        self.remaining -= n as u64;
        Ok(n)
    }
}

/// Compress raw data into a DVD-profile CHD (MAME 0.287+) via libchdman-rs.
///
/// Same shape as [`compress_chd`] but uses DVD's 2048-byte sector unit and
/// writes the `DVD ` metadata tag so MAME / `chdman info` recognises it as
/// a DVD CHD. `logical_size` is rounded up to a 2048-byte multiple; any
/// tail shortfall is zero-padded by libchdman-rs.
pub(crate) fn compress_chd_dvd(
    reader: &mut impl Read,
    output_base: &Path,
    logical_size: u64,
    split_size: Option<u64>,
    opts: Option<ChdOptions>,
    progress_cb: &mut impl FnMut(u64),
    cancel_check: &impl Fn() -> bool,
    log_cb: &mut impl FnMut(&str),
) -> Result<Vec<String>> {
    let chd_opts = opts.unwrap_or_else(|| ChdOptions::defaults_for(ChdProfile::Dvd));

    let unit_size = u64::from(DVD_SECTOR_SIZE);
    let padded_size = logical_size.div_ceil(unit_size) * unit_size;

    let chd_path = output_path(output_base, "chd", false, 0);
    log_cb(&format!(
        "Writing DVD CHD {} (logical {} bytes, hunk {}, codecs {:?})",
        chd_path.display(),
        padded_size,
        chd_opts.hunk_size,
        chd_opts.codecs,
    ));

    let dvd_opts = DvdCreateOptions {
        logical_size: padded_size,
        hunk_size: chd_opts.hunk_size,
        codecs: chd_opts.codecs,
    };

    let mut progress = |p: CompressionProgress| {
        progress_cb(p.bytes_done.min(logical_size));
    };

    dvd_create_from_reader(reader, &chd_path, dvd_opts, &mut progress, cancel_check).map_err(
        |e| match e {
            ChdError::Cancelled => anyhow::anyhow!("backup cancelled"),
            other => anyhow::anyhow!("DVD CHD create failed: {:?}", other),
        },
    )?;

    progress_cb(logical_size);

    if let Some(split_bytes) = split_size {
        let chd_size = fs::metadata(&chd_path)
            .with_context(|| format!("failed to stat CHD output: {}", chd_path.display()))?
            .len();

        if chd_size > split_bytes {
            return split_file(&chd_path, output_base, "chd", split_bytes);
        }
    }

    Ok(vec![file_name(&chd_path)])
}

/// Split an existing file into chunks, removing the original.
pub(crate) fn split_file(
    source: &Path,
    output_base: &Path,
    extension: &str,
    split_bytes: u64,
) -> Result<Vec<String>> {
    let mut reader = BufReader::new(
        File::open(source).with_context(|| format!("failed to open {}", source.display()))?,
    );
    let mut files = Vec::new();
    let mut part_index: u32 = 0;
    let mut buf = vec![0u8; CHUNK_SIZE];

    loop {
        let out_path = output_path(output_base, extension, true, part_index);
        let mut writer = BufWriter::new(
            File::create(&out_path)
                .with_context(|| format!("failed to create {}", out_path.display()))?,
        );
        let mut written: u64 = 0;
        let mut eof = false;

        while written < split_bytes {
            let to_read = ((split_bytes - written) as usize).min(CHUNK_SIZE);
            let n = reader.read(&mut buf[..to_read])?;
            if n == 0 {
                eof = true;
                break;
            }
            writer.write_all(&buf[..n])?;
            written += n as u64;
        }
        writer.flush()?;

        if written > 0 {
            files.push(file_name(&out_path));
        } else {
            // Empty chunk, remove it
            let _ = fs::remove_file(&out_path);
        }

        part_index += 1;
        if eof {
            break;
        }
    }

    // Remove the original unsplit file
    let _ = fs::remove_file(source);

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::TempDir;

    #[test]
    fn compress_chd_round_trip() {
        let tmp = TempDir::new().unwrap();
        let mut data = vec![0u8; 1024 * 1024];
        for (i, b) in data.iter_mut().enumerate() {
            *b = ((i * 31) ^ (i >> 7)) as u8;
        }
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("disk");

        let files = compress_chd(
            &mut reader,
            &base,
            data.len() as u64,
            None,
            None,
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(files, vec!["disk.chd"]);

        let chd_path = base.with_extension("chd");
        let mut chd_reader = ChdReader::open(&chd_path).unwrap();
        let mut decoded = vec![0u8; data.len()];
        chd_reader.read_exact(&mut decoded).unwrap();
        assert_eq!(decoded, data, "CHD round-trip mismatch");
    }

    #[test]
    fn compress_chd_dvd_round_trip() {
        // 4 MiB of pseudo-random data, sized to a 2048-byte multiple so
        // libchdman-rs's DVD validate() accepts it without padding.
        let tmp = TempDir::new().unwrap();
        let mut data = vec![0u8; 4 * 1024 * 1024];
        for (i, b) in data.iter_mut().enumerate() {
            *b = ((i * 17) ^ (i >> 9)) as u8;
        }
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("disk");

        let files = compress_chd_dvd(
            &mut reader,
            &base,
            data.len() as u64,
            None,
            None,
            &mut |_| {},
            &|| false,
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(files, vec!["disk.chd"]);

        let chd_path = base.with_extension("chd");
        let mut chd_reader = ChdReader::open(&chd_path).unwrap();
        let mut decoded = vec![0u8; data.len()];
        chd_reader.read_exact(&mut decoded).unwrap();
        assert_eq!(decoded, data, "DVD CHD round-trip mismatch");

        // Confirm the DVD metadata tag landed — libchdman-rs flags it as DVD
        // via Chd::info().is_dvd.
        let chd = libchdman_rs::Chd::open(chd_path.to_str().unwrap(), false, None).unwrap();
        assert!(chd.info().unwrap().is_dvd, "expected is_dvd flag");
    }
}
