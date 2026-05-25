//! VMDK flat reader + writer (split `monolithicFlat` / `twoGbMaxExtentFlat`,
//! and embedded `monolithicFlat`).
//!
//! Phase 3 / Session 3.1 of `docs/virtualization-formats.md`. Sparse VMDK
//! (`monolithicSparse`, `KDMV` magic) is Phase 4 and lives in a separate module
//! when it lands.
//!
//! ## Scope
//!
//! - **Read:** an ASCII descriptor (standalone `.vmdk` or embedded at the head
//!   of a flat file) with one or more `RW <sectors> FLAT "<filename>" <offset>`
//!   extents. `ZERO` extents are honoured (read as zeros). Reader is
//!   `Read + Seek` over the concatenated extents.
//! - **Write:** single-extent `monolithicFlat` — emit `<base>-flat.vmdk` (raw
//!   sector data) plus `<base>.vmdk` (descriptor referencing it).
//!
//! Detection: a `.vmdk` whose first bytes are ASCII `# Disk DescriptorFile`.
//! Sparse VMDKs lead with `KDMV` (little-endian magic `0x564d444b`) and are
//! deferred to Phase 4 — reader/detection there.
//!
//! ## Descriptor format (excerpt)
//!
//! ```text
//! # Disk DescriptorFile
//! version=1
//! CID=fffffffe
//! parentCID=ffffffff
//! createType="monolithicFlat"
//!
//! # Extent description
//! RW 2097152 FLAT "disk-flat.vmdk" 0
//!
//! # The Disk Data Base
//! ddb.geometry.cylinders = "1024"
//! ddb.geometry.heads = "16"
//! ddb.geometry.sectors = "63"
//! ddb.adapterType = "ide"
//! ```
//!
//! Each extent line is `<access> <size_sectors> <type> "<filename>"
//! [<offset_sectors>]`. We accept `RW`/`RDONLY`, types `FLAT`/`ZERO`, optional
//! quotes around the filename, and a trailing `offset_sectors` (defaulting to
//! 0). Unknown extent types (`SPARSE`, `VMFS`, `VMFSSPARSE`, …) error at open.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

use super::vhd::vhd_chs_geometry;

/// Sentinel string the descriptor always opens with.
pub const VMDK_DESCRIPTOR_MAGIC: &[u8] = b"# Disk DescriptorFile";
/// `KDMV` — VMDK sparse magic (little-endian `0x564d444b`). We detect it here
/// only to reject sparse images with a precise error until Phase 4 ships.
pub const VMDK_SPARSE_MAGIC: &[u8; 4] = b"KDMV";

/// One extent line from the descriptor, resolved to an absolute file path.
#[derive(Debug, Clone)]
struct ExtentSpec {
    kind: ExtentKind,
    /// Length in sectors as written in the descriptor.
    sectors: u64,
    /// Absolute path to the backing file (`None` for ZERO extents).
    path: Option<PathBuf>,
    /// Byte offset into `path` where this extent's data begins.
    file_byte_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtentKind {
    Flat,
    Zero,
}

const VMDK_SECTOR: u64 = 512;

/// Parsed descriptor — just enough to drive the reader and to round-trip a
/// flat image through the writer.
#[derive(Debug, Clone)]
struct Descriptor {
    extents: Vec<ExtentSpec>,
}

impl Descriptor {
    /// Parse the ASCII descriptor.
    ///
    /// `descriptor_dir` is the directory the descriptor lives in (or the
    /// directory of the embedded flat-with-descriptor file). Relative extent
    /// filenames are resolved against it.
    fn parse(text: &str, descriptor_dir: &Path) -> Result<Self> {
        let mut extents = Vec::new();
        for raw_line in text.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // Extent lines start with an access token (RW / RDONLY / NOACCESS).
            // ddb.* / createType=… / CID=… are all `key = value` shape — skip
            // them in this minimal parser (we don't currently need them).
            let mut tokens = tokenize_extent_line(line);
            let first = match tokens.first() {
                Some(t) => t.as_str(),
                None => continue,
            };
            if !matches!(first, "RW" | "RDONLY" | "NOACCESS") {
                continue;
            }
            if tokens.len() < 3 {
                bail!("VMDK descriptor: malformed extent line: {raw_line:?}");
            }
            let sectors: u64 = tokens[1]
                .parse()
                .with_context(|| format!("VMDK descriptor: bad sector count in {raw_line:?}"))?;
            let kind_str = tokens[2].to_ascii_uppercase();
            let kind = match kind_str.as_str() {
                "FLAT" => ExtentKind::Flat,
                "ZERO" => ExtentKind::Zero,
                "SPARSE" | "VMFSSPARSE" | "VMFS" | "VMFSRAW" | "VMFSRDM" | "VMFSRAWDEVICEMAP" => {
                    bail!(
                        "VMDK extent type {kind_str} is not supported by this reader \
                         (Phase 3 covers FLAT/ZERO only)"
                    );
                }
                other => bail!("VMDK descriptor: unknown extent type {other:?}"),
            };

            let path = if kind == ExtentKind::Zero {
                None
            } else {
                let fname = tokens.get(3).ok_or_else(|| {
                    anyhow!("VMDK descriptor: FLAT extent missing filename in {raw_line:?}")
                })?;
                let p = PathBuf::from(strip_quotes(fname));
                let resolved = if p.is_absolute() {
                    p
                } else {
                    descriptor_dir.join(p)
                };
                Some(resolved)
            };

            let file_offset_sectors: u64 = tokens
                .get(4)
                .map(|s| s.parse::<u64>())
                .transpose()
                .with_context(|| format!("VMDK descriptor: bad extent offset in {raw_line:?}"))?
                .unwrap_or(0);

            // Drain in case there are trailing tokens we don't care about.
            tokens.clear();

            extents.push(ExtentSpec {
                kind,
                sectors,
                path,
                file_byte_offset: file_offset_sectors * VMDK_SECTOR,
            });
        }
        if extents.is_empty() {
            bail!("VMDK descriptor: no extent lines found");
        }
        Ok(Descriptor { extents })
    }
}

/// Splits an extent line on whitespace, but keeps `"quoted filenames"` intact.
fn tokenize_extent_line(line: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for ch in line.chars() {
        if ch == '"' {
            in_quotes = !in_quotes;
            cur.push(ch);
        } else if ch.is_whitespace() && !in_quotes {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
        } else {
            cur.push(ch);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn strip_quotes(s: &str) -> &str {
    s.strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(s)
}

/// Locate and read the descriptor text from `path`. Supports the two shapes:
/// a standalone descriptor file, or a flat image with the descriptor embedded
/// in its first sector(s).
///
/// Returns `(descriptor_text, descriptor_byte_len, descriptor_dir)`. For
/// standalone descriptors `descriptor_byte_len == file_size`.
fn read_descriptor(path: &Path) -> Result<(String, u64, PathBuf)> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let file_size = file.metadata()?.len();

    // Sniff the first ~4 KiB — enough to (a) decide between embedded-descriptor
    // / standalone / sparse, and (b) cover any realistic descriptor length.
    let probe_len = file_size.min(4096) as usize;
    let mut probe = vec![0u8; probe_len];
    file.read_exact(&mut probe)?;

    if probe.len() >= 4 && &probe[0..4] == VMDK_SPARSE_MAGIC {
        bail!(
            "{} is a VMDK sparse (KDMV) image — flat reader cannot open it; \
             sparse support is Phase 4",
            path.display()
        );
    }

    if !probe.starts_with(VMDK_DESCRIPTOR_MAGIC) {
        bail!(
            "{} does not start with the VMDK descriptor sentinel",
            path.display()
        );
    }

    // The descriptor is ASCII, terminated by either EOF (standalone) or a NUL /
    // 0xFF pad byte (embedded). Read the whole thing (small) and find the cut.
    let descriptor_dir = path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    // For embedded descriptors the spec rounds the descriptor region to a
    // sector boundary; read enough to be sure we have it.
    let read_len = file_size.min(64 * 1024) as usize;
    let mut buf = vec![0u8; read_len];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;
    let end = buf
        .iter()
        .position(|&b| b == 0 || b == 0xFF)
        .unwrap_or(buf.len());
    let text =
        std::str::from_utf8(&buf[..end]).context("VMDK descriptor is not valid UTF-8/ASCII")?;
    let text_owned = text.to_string();

    // descriptor_byte_len: where flat data begins inside `path`, for embedded
    // descriptors (single self-contained `.vmdk`). For split layouts we return
    // file_size; callers don't use it since the extents reference *other* files.
    let mut descriptor_byte_len = end as u64;
    // Round up to a sector — embedded descriptors are padded to a sector edge.
    descriptor_byte_len = descriptor_byte_len.div_ceil(VMDK_SECTOR) * VMDK_SECTOR;

    Ok((text_owned, descriptor_byte_len, descriptor_dir))
}

/// Resolved extent ready for `Read + Seek` dispatch.
struct OpenExtent {
    kind: ExtentKind,
    /// Logical start byte within the concatenated virtual disk.
    logical_start: u64,
    /// Length in bytes.
    length: u64,
    /// Open file handle (None for ZERO extents).
    file: Option<File>,
    /// Byte offset within `file` where this extent's data starts.
    file_offset: u64,
}

/// `Read + Seek` reader over a VMDK flat (descriptor + one or more FLAT/ZERO
/// extents).
pub struct VmdkFlatReader {
    extents: Vec<OpenExtent>,
    total: u64,
    pos: u64,
}

impl VmdkFlatReader {
    /// Open `path` — a `.vmdk` descriptor (standalone or with embedded flat
    /// data) — and resolve every extent.
    pub fn open(path: &Path) -> Result<Self> {
        let (text, embedded_data_start, dir) = read_descriptor(path)?;
        let descriptor = Descriptor::parse(&text, &dir)?;

        let mut extents = Vec::with_capacity(descriptor.extents.len());
        let mut logical = 0u64;
        for spec in descriptor.extents {
            let length = spec.sectors * VMDK_SECTOR;
            let (file, file_offset) = match spec.kind {
                ExtentKind::Zero => (None, 0),
                ExtentKind::Flat => {
                    let extent_path = spec.path.as_ref().expect("FLAT extent has path");
                    // If the extent points back at the descriptor file itself
                    // (embedded layout), the data lives past the descriptor.
                    let same_file = same_file_path(path, extent_path);
                    let file = File::open(extent_path).with_context(|| {
                        format!("failed to open VMDK extent {}", extent_path.display())
                    })?;
                    let offset = if same_file && spec.file_byte_offset == 0 {
                        embedded_data_start
                    } else {
                        spec.file_byte_offset
                    };
                    (Some(file), offset)
                }
            };
            extents.push(OpenExtent {
                kind: spec.kind,
                logical_start: logical,
                length,
                file,
                file_offset,
            });
            logical += length;
        }

        Ok(Self {
            extents,
            total: logical,
            pos: 0,
        })
    }

    /// Logical disk size in bytes.
    pub fn len(&self) -> u64 {
        self.total
    }

    /// True if the logical disk is empty (no extents / zero-length).
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Find the index of the extent that covers `logical_pos`. Returns
    /// `extents.len()` for positions at or past EOF.
    fn locate(&self, logical_pos: u64) -> usize {
        // Linear scan — VMDKs in the wild have 1 (monolithic) or ~tens
        // (twoGbMaxExtentFlat splits a 2 TiB disk into ~1000) extents at most.
        for (i, e) in self.extents.iter().enumerate() {
            if logical_pos < e.logical_start + e.length {
                return i;
            }
        }
        self.extents.len()
    }
}

impl Read for VmdkFlatReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.total || buf.is_empty() {
            return Ok(0);
        }
        let idx = self.locate(self.pos);
        if idx >= self.extents.len() {
            return Ok(0);
        }
        let extent = &mut self.extents[idx];
        let intra = self.pos - extent.logical_start;
        let remaining_in_extent = extent.length - intra;
        let to_read = (buf.len() as u64).min(remaining_in_extent) as usize;

        match extent.kind {
            ExtentKind::Zero => {
                for b in &mut buf[..to_read] {
                    *b = 0;
                }
            }
            ExtentKind::Flat => {
                let file = extent.file.as_mut().expect("FLAT extent has file");
                file.seek(SeekFrom::Start(extent.file_offset + intra))?;
                file.read_exact(&mut buf[..to_read])?;
            }
        }
        self.pos += to_read as u64;
        Ok(to_read)
    }
}

impl Seek for VmdkFlatReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new = match pos {
            SeekFrom::Start(o) => o as i128,
            SeekFrom::End(o) => self.total as i128 + o as i128,
            SeekFrom::Current(o) => self.pos as i128 + o as i128,
        };
        if new < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek before start",
            ));
        }
        self.pos = new as u64;
        Ok(self.pos)
    }
}

fn same_file_path(a: &Path, b: &Path) -> bool {
    // Best-effort: canonicalize when possible, fall back to literal equality.
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(ca), Ok(cb)) => ca == cb,
        _ => a == b,
    }
}

// ---------------------------------------------------------------------------
// Writer — monolithicFlat
// ---------------------------------------------------------------------------

/// Build a `monolithicFlat` descriptor referencing `flat_filename`.
///
/// `disk_size_bytes` must be a non-zero multiple of 512.
pub fn build_monolithic_flat_descriptor(disk_size_bytes: u64, flat_filename: &str) -> String {
    assert!(
        disk_size_bytes > 0 && disk_size_bytes.is_multiple_of(VMDK_SECTOR),
        "disk size must be a non-zero multiple of 512"
    );
    let sectors = disk_size_bytes / VMDK_SECTOR;
    // Pick a CHS geometry that matches the VHD writer's convention (the same
    // 16-heads / 63-sectors layout VMware uses for IDE).
    let (cyl, heads, secs) = vhd_chs_geometry(disk_size_bytes);

    // Random-ish CID — not security-critical, just needs to differ across
    // copies so chained snapshots can detect parent mismatches. Use a low
    // 32-bit hash of (size, filename) so the output is byte-deterministic for
    // round-trip tests yet not constant across all disks.
    let mut cid: u32 = 0x9E37_79B9; // golden ratio seed
    for b in flat_filename.bytes().chain(disk_size_bytes.to_le_bytes()) {
        cid = cid.wrapping_mul(0x0100_0193) ^ b as u32;
    }

    format!(
        "# Disk DescriptorFile\n\
         version=1\n\
         CID={cid:08x}\n\
         parentCID=ffffffff\n\
         createType=\"monolithicFlat\"\n\
         \n\
         # Extent description\n\
         RW {sectors} FLAT \"{flat_filename}\" 0\n\
         \n\
         # The Disk Data Base\n\
         ddb.virtualHWVersion = \"4\"\n\
         ddb.geometry.cylinders = \"{cyl}\"\n\
         ddb.geometry.heads = \"{heads}\"\n\
         ddb.geometry.sectors = \"{secs}\"\n\
         ddb.adapterType = \"ide\"\n",
    )
}

/// Resolve the single FLAT extent of a `monolithicFlat` VMDK descriptor and
/// open the backing flat file with read+write access.
///
/// In-place edit for VMDK flat is trivial: the extent's bytes ARE the disk
/// image, so the editor just needs an `R + W + S` handle on `<base>-flat.vmdk`
/// (or whatever extent file the descriptor names). The descriptor itself
/// records only sector counts and geometry — nothing in it has to be rewritten
/// when bytes change inside the extent.
///
/// Errors when the descriptor names more than one FLAT extent (split layouts
/// would need the caller to pick *which* extent to edit), references a ZERO
/// extent (no backing file), or is a sparse `KDMV` image (Phase 4).
pub fn open_flat_extent_for_edit(descriptor_path: &Path) -> Result<File> {
    let (text, _embedded_data_start, dir) = read_descriptor(descriptor_path)?;
    let descriptor = Descriptor::parse(&text, &dir)?;
    let flat_extents: Vec<&ExtentSpec> = descriptor
        .extents
        .iter()
        .filter(|e| e.kind == ExtentKind::Flat)
        .collect();
    match flat_extents.len() {
        0 => bail!(
            "VMDK descriptor {} has no FLAT extent to edit",
            descriptor_path.display()
        ),
        1 => {
            let path = flat_extents[0]
                .path
                .as_ref()
                .expect("FLAT extent carries a path");
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .with_context(|| format!("failed to open VMDK extent {} R+W", path.display()))
        }
        n => bail!(
            "VMDK descriptor {} has {} FLAT extents — in-place edit only supports \
             single-extent (monolithicFlat) layouts; convert to monolithicFlat first",
            descriptor_path.display(),
            n
        ),
    }
}

/// Export a whole-disk source as a `monolithicFlat` VMDK at `dest_path` (the
/// `.vmdk` descriptor). The raw extent is written as `<stem>-flat.vmdk` next to
/// the descriptor.
///
/// `disk_size` must equal the total bytes available from `reader` and must be
/// a non-zero multiple of 512 (VMDK requires sector-aligned extents).
pub fn export_vmdk_flat(
    reader: &mut impl Read,
    descriptor_path: &Path,
    disk_size: u64,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
) -> Result<()> {
    if disk_size == 0 {
        bail!("VMDK flat export: source disk size is zero");
    }
    if !disk_size.is_multiple_of(VMDK_SECTOR) {
        bail!("VMDK flat export: source size {disk_size} is not a multiple of {VMDK_SECTOR} bytes");
    }

    let stem = descriptor_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| {
            anyhow!(
                "VMDK export: cannot derive stem from {}",
                descriptor_path.display()
            )
        })?;
    let flat_filename = format!("{stem}-flat.vmdk");
    let flat_path = descriptor_path.with_file_name(&flat_filename);

    // Stream the raw flat extent.
    let mut flat = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&flat_path)
        .with_context(|| format!("failed to create {}", flat_path.display()))?;

    let mut buf = vec![0u8; super::CHUNK_SIZE];
    let mut written: u64 = 0;
    while written < disk_size {
        if cancel_check() {
            bail!("VMDK flat export cancelled");
        }
        let want = (disk_size - written).min(buf.len() as u64) as usize;
        reader
            .read_exact(&mut buf[..want])
            .context("failed to read source for VMDK flat export")?;
        flat.write_all(&buf[..want])
            .context("failed to write VMDK flat extent")?;
        written += want as u64;
        progress_cb(written);
    }
    flat.flush()?;

    // Emit descriptor next to it.
    let descriptor = build_monolithic_flat_descriptor(disk_size, &flat_filename);
    std::fs::write(descriptor_path, descriptor.as_bytes())
        .with_context(|| format!("failed to write {}", descriptor_path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_bytes(p: &Path, b: &[u8]) {
        std::fs::write(p, b).unwrap();
    }

    #[test]
    fn tokenize_handles_quoted_filename() {
        let toks = tokenize_extent_line(r#"RW 2097152 FLAT "disk-flat.vmdk" 0"#);
        assert_eq!(
            toks,
            vec!["RW", "2097152", "FLAT", "\"disk-flat.vmdk\"", "0"]
        );
    }

    #[test]
    fn tokenize_handles_spaces_in_filename() {
        let toks = tokenize_extent_line(r#"RW 4096 FLAT "my disk-flat.vmdk""#);
        assert_eq!(toks, vec!["RW", "4096", "FLAT", "\"my disk-flat.vmdk\""]);
    }

    #[test]
    fn parse_minimal_descriptor() {
        let text = "# Disk DescriptorFile\nversion=1\nCID=fffffffe\nparentCID=ffffffff\n\
                    createType=\"monolithicFlat\"\n\
                    RW 2048 FLAT \"d-flat.vmdk\" 0\n";
        let d = Descriptor::parse(text, Path::new("/tmp")).unwrap();
        assert_eq!(d.extents.len(), 1);
        assert_eq!(d.extents[0].sectors, 2048);
        assert_eq!(d.extents[0].kind, ExtentKind::Flat);
        assert_eq!(
            d.extents[0].path.as_ref().unwrap(),
            Path::new("/tmp/d-flat.vmdk")
        );
    }

    #[test]
    fn parse_rejects_sparse_extent() {
        let text = "# Disk DescriptorFile\nRW 2048 SPARSE \"s.vmdk\" 0\n";
        let err = Descriptor::parse(text, Path::new("/")).unwrap_err();
        assert!(err.to_string().contains("SPARSE"), "{err}");
    }

    #[test]
    fn parse_zero_extent_needs_no_file() {
        let text = "# Disk DescriptorFile\nRW 1024 ZERO\n";
        let d = Descriptor::parse(text, Path::new("/")).unwrap();
        assert_eq!(d.extents[0].kind, ExtentKind::Zero);
        assert!(d.extents[0].path.is_none());
    }

    #[test]
    fn round_trip_split_flat() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("out.vmdk");

        // 4 sectors of pseudo-random data + 4 sectors of zero — total 8 sectors.
        let mut src: Vec<u8> = (0..8 * 512u32).map(|i| (i ^ (i >> 3)) as u8).collect();
        for b in &mut src[4 * 512..] {
            *b = 0;
        }

        let mut cursor = std::io::Cursor::new(&src);
        export_vmdk_flat(&mut cursor, &dst, src.len() as u64, &mut |_| {}, &|| false).unwrap();

        let flat = tmp.path().join("out-flat.vmdk");
        assert!(flat.exists(), "writer emits sibling flat extent");
        assert_eq!(std::fs::metadata(&flat).unwrap().len(), src.len() as u64);

        let mut reader = VmdkFlatReader::open(&dst).unwrap();
        assert_eq!(reader.len(), src.len() as u64);
        let mut got = vec![0u8; src.len()];
        reader.read_exact(&mut got).unwrap();
        assert_eq!(got, src);

        // Mid-extent seek works.
        reader.seek(SeekFrom::Start(1024)).unwrap();
        let mut tail = vec![0u8; 1024];
        reader.read_exact(&mut tail).unwrap();
        assert_eq!(tail, src[1024..2048]);
    }

    #[test]
    fn read_multi_extent_concatenation() {
        let tmp = tempfile::tempdir().unwrap();
        // Two flat files of 2 sectors each, plus a 2-sector ZERO extent.
        let a_path = tmp.path().join("a.bin");
        let b_path = tmp.path().join("b.bin");
        let a: Vec<u8> = (0..1024u32).map(|i| (i & 0xFF) as u8).collect();
        let b: Vec<u8> = (0..1024u32).map(|i| ((i + 100) & 0xFF) as u8).collect();
        write_bytes(&a_path, &a);
        write_bytes(&b_path, &b);

        let descriptor = "# Disk DescriptorFile\nversion=1\nCID=00000001\nparentCID=ffffffff\n\
             createType=\"twoGbMaxExtentFlat\"\n\
             RW 2 FLAT \"a.bin\" 0\n\
             RW 2 ZERO\n\
             RW 2 FLAT \"b.bin\" 0\n";
        let desc_path = tmp.path().join("multi.vmdk");
        std::fs::write(&desc_path, descriptor).unwrap();

        let mut r = VmdkFlatReader::open(&desc_path).unwrap();
        assert_eq!(r.len(), 6 * 512);
        let mut buf = vec![0u8; 6 * 512];
        r.read_exact(&mut buf).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&a);
        expected.extend(std::iter::repeat_n(0u8, 1024));
        expected.extend_from_slice(&b);
        assert_eq!(buf, expected);
    }

    #[test]
    fn rejects_sparse_kdmv_at_open() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("s.vmdk");
        let mut bytes = vec![0u8; 1024];
        bytes[..4].copy_from_slice(VMDK_SPARSE_MAGIC);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(&bytes).unwrap();
        let err = match VmdkFlatReader::open(&p) {
            Ok(_) => panic!("expected sparse open to fail"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("sparse"), "{err}");
    }

    #[test]
    fn open_flat_extent_for_edit_round_trip() {
        // Write a flat VMDK, open the extent R+W via the helper, mutate a
        // sector, reopen via VmdkFlatReader, and confirm the mutation lands.
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("edit.vmdk");

        let src = vec![0u8; 4 * 512];
        let mut cursor = std::io::Cursor::new(&src);
        export_vmdk_flat(&mut cursor, &dst, src.len() as u64, &mut |_| {}, &|| false).unwrap();

        {
            let mut handle = open_flat_extent_for_edit(&dst).unwrap();
            handle.seek(SeekFrom::Start(1024)).unwrap();
            handle.write_all(&[0xAB; 16]).unwrap();
            handle.flush().unwrap();
        }

        let mut reader = VmdkFlatReader::open(&dst).unwrap();
        reader.seek(SeekFrom::Start(1024)).unwrap();
        let mut got = [0u8; 16];
        reader.read_exact(&mut got).unwrap();
        assert_eq!(got, [0xAB; 16]);
    }

    #[test]
    fn open_flat_extent_for_edit_rejects_multi_extent() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a.bin");
        let b = tmp.path().join("b.bin");
        write_bytes(&a, &[0u8; 512]);
        write_bytes(&b, &[0u8; 512]);
        let desc = "# Disk DescriptorFile\nversion=1\ncreateType=\"twoGbMaxExtentFlat\"\n\
                    RW 1 FLAT \"a.bin\" 0\nRW 1 FLAT \"b.bin\" 0\n";
        let dp = tmp.path().join("d.vmdk");
        std::fs::write(&dp, desc).unwrap();
        let err = open_flat_extent_for_edit(&dp).unwrap_err();
        assert!(err.to_string().contains("monolithicFlat"), "{err}");
    }

    #[test]
    fn descriptor_contains_extent_line() {
        let d = build_monolithic_flat_descriptor(8 * 1024 * 1024, "disk-flat.vmdk");
        assert!(d.contains("RW 16384 FLAT \"disk-flat.vmdk\" 0"));
        assert!(d.contains("createType=\"monolithicFlat\""));
        assert!(d.contains("ddb.geometry.heads"));
    }
}
