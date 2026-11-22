//! VMDK descriptor reader + flat writer: `monolithicFlat` / `twoGbMaxExtentFlat` (FLAT extents),
//! embedded `monolithicFlat`, and `twoGbMaxExtentSparse` (hosted SPARSE extents, each its own
//! `KDMV` file read by [`crate::vmdk_sparse`]).
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
//! 0). `SPARSE` extents (no offset) are hosted sparse files whose header capacity must match
//! the line. The ESX types (`VMFS`, `VMFSSPARSE`, …) error at open, as does a snapshot delta
//! (`parentCID` other than `ffffffff`): its unwritten sectors live in a parent we do not read.

#[cfg(feature = "rust173-polyfill")]
use disk_core::rust173::IntIsMultipleOf as _;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

use crate::vhd::vhd_chs_geometry;

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
    /// A hosted sparse extent (`twoGbMaxExtentSparse`): a `KDMV` file of its own.
    Sparse,
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
            if let Some(parent) = line.strip_prefix("parentCID=") {
                if !parent.trim().eq_ignore_ascii_case("ffffffff") {
                    bail!(
                        "this VMDK is a snapshot delta (parentCID={}): its unchanged sectors \
                         are in the parent disk. Open the base disk, or consolidate the \
                         snapshots in VMware first",
                        parent.trim()
                    );
                }
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
                "SPARSE" => ExtentKind::Sparse,
                "VMFSSPARSE" | "VMFS" | "VMFSRAW" | "VMFSRDM" | "VMFSRAWDEVICEMAP" => {
                    bail!(
                        "VMDK extent type {kind_str} (an ESX datastore disk) is not supported; \
                         this reader covers FLAT, ZERO and hosted SPARSE extents"
                    );
                }
                other => bail!("VMDK descriptor: unknown extent type {other:?}"),
            };

            let path = if kind == ExtentKind::Zero {
                None
            } else {
                let fname = tokens.get(3).ok_or_else(|| {
                    anyhow!("VMDK descriptor: {kind_str} extent missing filename in {raw_line:?}")
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
/// Longest descriptor read; a disk split into thousands of extents fits easily.
const MAX_DESCRIPTOR_BYTES: u64 = 4 << 20;

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
    let read_len = file_size.min(MAX_DESCRIPTOR_BYTES) as usize;
    let mut buf = vec![0u8; read_len];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;
    let end = buf.iter().position(|&b| b == 0 || b == 0xFF);
    // A descriptor cut short would silently drop its later extents.
    if end.is_none() && file_size > read_len as u64 {
        bail!(
            "{}: VMDK descriptor is longer than {MAX_DESCRIPTOR_BYTES} bytes",
            path.display()
        );
    }
    let end = end.unwrap_or(buf.len());
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

/// Where an extent's bytes come from.
enum Backend {
    Zero,
    /// Raw sectors at `offset` in `file`.
    Flat {
        file: File,
        offset: u64,
    },
    /// A hosted sparse extent, addressed from its own sector 0.
    Sparse(Box<crate::vmdk_sparse::VmdkSparseReader<File>>),
}

/// Resolved extent ready for `Read + Seek` dispatch.
struct OpenExtent {
    /// Logical start byte within the concatenated virtual disk.
    logical_start: u64,
    /// Length in bytes.
    length: u64,
    backend: Backend,
}

/// `Read + Seek` (and, opened with [`VmdkFlatReader::open_rw`], `Write`) over a descriptor
/// VMDK: its FLAT, ZERO and hosted SPARSE extents back to back, each at its own size.
pub struct VmdkFlatReader {
    extents: Vec<OpenExtent>,
    total: u64,
    pos: u64,
    writable: bool,
}

impl VmdkFlatReader {
    /// Open `path` — a `.vmdk` descriptor (standalone or with embedded flat
    /// data) — and resolve every extent.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with(path, false)
    }

    /// As [`Self::open`], with every extent file opened read-write for in-place edits. A
    /// sparse extent allocates grains as it is written; a ZERO extent refuses writes.
    pub fn open_rw(path: &Path) -> Result<Self> {
        Self::open_with(path, true)
    }

    fn open_with(path: &Path, writable: bool) -> Result<Self> {
        let (text, embedded_data_start, dir) = read_descriptor(path)?;
        let descriptor = Descriptor::parse(&text, &dir)?;
        let open_file = |p: &Path| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(writable)
                .open(p)
                .with_context(|| format!("failed to open VMDK extent {}", p.display()))
        };

        // An edit must not reach a file outside the disk's own folder.
        if writable {
            for spec in &descriptor.extents {
                let Some(p) = &spec.path else { continue };
                let rel = p.strip_prefix(&dir).unwrap_or(p);
                if rel.is_absolute()
                    || rel
                        .components()
                        .any(|c| matches!(c, std::path::Component::ParentDir))
                {
                    bail!(
                        "VMDK extent {} is outside the descriptor's folder; refusing to open it \
                         for writing",
                        p.display()
                    );
                }
            }
        }
        let mut extents = Vec::with_capacity(descriptor.extents.len());
        let mut logical = 0u64;
        for spec in descriptor.extents {
            let length = spec.sectors * VMDK_SECTOR;
            let backend = match spec.kind {
                ExtentKind::Zero => Backend::Zero,
                ExtentKind::Flat => {
                    let extent_path = spec.path.as_ref().expect("FLAT extent has path");
                    // If the extent points back at the descriptor file itself
                    // (embedded layout), the data lives past the descriptor.
                    let same_file = same_file_path(path, extent_path);
                    let file = open_file(extent_path)?;
                    let offset = if same_file && spec.file_byte_offset == 0 {
                        embedded_data_start
                    } else {
                        spec.file_byte_offset
                    };
                    Backend::Flat { file, offset }
                }
                ExtentKind::Sparse => {
                    let extent_path = spec.path.as_ref().expect("SPARSE extent has path");
                    let sparse =
                        crate::vmdk_sparse::VmdkSparseReader::open(open_file(extent_path)?)
                            .with_context(|| format!("VMDK extent {}", extent_path.display()))?;
                    if sparse.len() != length {
                        bail!(
                            "VMDK extent {} holds {} sectors but the descriptor says {}",
                            extent_path.display(),
                            sparse.len() / VMDK_SECTOR,
                            spec.sectors
                        );
                    }
                    Backend::Sparse(Box::new(sparse))
                }
            };
            extents.push(OpenExtent {
                logical_start: logical,
                length,
                backend,
            });
            logical += length;
        }

        Ok(Self {
            extents,
            total: logical,
            pos: 0,
            writable,
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
        // Binary search: extents are sorted and contiguous, and a big split disk has thousands.
        let ends = self
            .extents
            .partition_point(|e| e.logical_start + e.length <= logical_pos);
        ends.min(self.extents.len())
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

        match &mut extent.backend {
            Backend::Zero => {
                for b in &mut buf[..to_read] {
                    *b = 0;
                }
            }
            Backend::Flat { file, offset } => {
                file.seek(SeekFrom::Start(*offset + intra))?;
                file.read_exact(&mut buf[..to_read])?;
            }
            Backend::Sparse(sparse) => {
                sparse.seek(SeekFrom::Start(intra))?;
                sparse.read_exact(&mut buf[..to_read])?;
            }
        }
        self.pos += to_read as u64;
        Ok(to_read)
    }
}

impl Write for VmdkFlatReader {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if !self.writable {
            return Err(disk_core::io_other("VMDK opened read-only"));
        }
        if buf.is_empty() {
            return Ok(0);
        }
        let idx = self.locate(self.pos);
        if idx >= self.extents.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write past the end of the VMDK",
            ));
        }
        let extent = &mut self.extents[idx];
        let intra = self.pos - extent.logical_start;
        let to_write = (buf.len() as u64).min(extent.length - intra) as usize;
        match &mut extent.backend {
            Backend::Zero => {
                return Err(disk_core::io_other("a VMDK ZERO extent cannot be written"))
            }
            Backend::Flat { file, offset } => {
                file.seek(SeekFrom::Start(*offset + intra))?;
                file.write_all(&buf[..to_write])?;
            }
            Backend::Sparse(sparse) => {
                sparse.seek(SeekFrom::Start(intra))?;
                sparse.write_all(&buf[..to_write])?;
            }
        }
        self.pos += to_write as u64;
        Ok(to_write)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        for e in &mut self.extents {
            match &mut e.backend {
                Backend::Zero => {}
                Backend::Flat { file, .. } => file.flush()?,
                Backend::Sparse(sparse) => sparse.flush()?,
            }
        }
        Ok(())
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

/// The descriptor beside `extent` that lists it, when `extent` is one piece of a split disk
/// (`Disk-s001.vmdk`): the piece alone is only a slice, so callers open the descriptor instead.
pub fn descriptor_for_extent(extent: &Path) -> Option<PathBuf> {
    let dir = extent
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let candidate = entry.path();
        let is_vmdk = candidate
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("vmdk"));
        let small = entry
            .metadata()
            .is_ok_and(|m| m.is_file() && m.len() < 64 * 1024);
        if !is_vmdk || !small || same_file_path(&candidate, extent) {
            continue;
        }
        let Ok((text, _, desc_dir)) = read_descriptor(&candidate) else {
            continue;
        };
        let Ok(desc) = Descriptor::parse(&text, &desc_dir) else {
            continue;
        };
        let lists = desc.extents.iter();
        if lists
            .filter_map(|e| e.path.as_deref())
            .any(|p| same_file_path(p, extent))
        {
            return Some(candidate);
        }
    }
    None
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
/// `disk_size` must equal the total bytes available from `reader` and be
/// non-zero. If it isn't a multiple of 512, the extent is zero-padded up to
/// the next sector boundary (VMDK requires sector-aligned extents).
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
    // VMDK extents must be sector-aligned. Sources whose size isn't a
    // multiple of 512 (e.g. some Ghost/raw images) get zero-padded up to the
    // next sector boundary; the descriptor reports the padded size.
    let aligned_size = disk_size.div_ceil(VMDK_SECTOR) * VMDK_SECTOR;

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

    let mut buf = vec![0u8; crate::CHUNK_SIZE];
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

    // Zero-pad the tail so the extent is a whole number of 512-byte sectors.
    if aligned_size > disk_size {
        let pad = (aligned_size - disk_size) as usize;
        flat.write_all(&vec![0u8; pad])
            .context("failed to write VMDK flat extent padding")?;
    }
    flat.flush()?;

    // Emit descriptor next to it.
    let descriptor = build_monolithic_flat_descriptor(aligned_size, &flat_filename);
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
    fn parse_takes_hosted_sparse_and_refuses_esx_extents() {
        let text = "# Disk DescriptorFile\nRW 2048 SPARSE \"s.vmdk\"\n";
        let d = Descriptor::parse(text, Path::new("/")).unwrap();
        assert_eq!(d.extents[0].kind, ExtentKind::Sparse);
        assert_eq!(d.extents[0].path.as_deref(), Some(Path::new("/s.vmdk")));
        let esx = "# Disk DescriptorFile\nRW 2048 VMFSSPARSE \"s.vmdk\"\n";
        let err = Descriptor::parse(esx, Path::new("/")).unwrap_err();
        assert!(err.to_string().contains("ESX"), "{err}");
    }

    #[test]
    fn parse_refuses_a_snapshot_delta() {
        let text = "# Disk DescriptorFile\nparentCID=83044315\nRW 2048 SPARSE \"d.vmdk\"\n";
        let err = Descriptor::parse(text, Path::new("/")).unwrap_err();
        assert!(err.to_string().contains("snapshot delta"), "{err}");
        let base = "# Disk DescriptorFile\nparentCID=ffffffff\nRW 2048 SPARSE \"d.vmdk\"\n";
        assert!(Descriptor::parse(base, Path::new("/")).is_ok());
    }

    /// A qemu-img `twoGbMaxExtentSparse` disk (2 GiB + 512 MiB extents): writes span the extents
    /// and qemu-img agrees with every byte. Skipped without qemu-img.
    #[test]
    fn split_sparse_reads_and_writes_across_extents() {
        use std::process::Command;
        let dir = tempfile::tempdir().unwrap();
        let disk = dir.path().join("d.vmdk");
        let made = Command::new("qemu-img")
            .args([
                "create",
                "-q",
                "-f",
                "vmdk",
                "-o",
                "subformat=twoGbMaxExtentSparse",
            ])
            .arg(&disk)
            .arg("2560M")
            .status();
        if !made.is_ok_and(|s| s.success()) {
            eprintln!("qemu-img not available; skipping");
            return;
        }
        let piece = dir.path().join("d-s002.vmdk");
        assert_eq!(
            descriptor_for_extent(&piece).as_deref(),
            Some(disk.as_path())
        );

        // A pattern across the 2 GiB boundary and near the end, mirrored into a raw file.
        let raw = dir.path().join("d.raw");
        let mut flat = File::create(&raw).unwrap();
        flat.set_len(2560 << 20).unwrap();
        let mut v = VmdkFlatReader::open_rw(&disk).unwrap();
        assert_eq!(v.len(), 2560 << 20);
        let writes: [(u64, usize); 3] = [
            ((2 << 30) - 100_000, 300_000),
            (1 << 20, 70_000),
            ((2560 << 20) - 4096, 4096),
        ];
        for (n, (at, len)) in writes.into_iter().enumerate() {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 7 + n * 13) as u8 | 1).collect();
            v.seek(SeekFrom::Start(at)).unwrap();
            v.write_all(&bytes).unwrap();
            flat.seek(SeekFrom::Start(at)).unwrap();
            flat.write_all(&bytes).unwrap();
        }
        v.flush().unwrap();
        drop(v);
        drop(flat);

        let mut back = VmdkFlatReader::open(&disk).unwrap();
        let mut got = vec![0u8; 300_000];
        back.seek(SeekFrom::Start((2 << 30) - 100_000)).unwrap();
        back.read_exact(&mut got).unwrap();
        assert!(got.iter().enumerate().all(|(i, b)| *b == (i * 7) as u8 | 1));

        let compare = Command::new("qemu-img")
            .args(["compare", "-q", "-f", "vmdk", "-F", "raw"])
            .arg(&disk)
            .arg(&raw)
            .status()
            .unwrap();
        assert!(compare.success(), "qemu-img compare found a difference");
        assert!(crate::qemu_img_test::qemu_img_check(&disk).unwrap());
        for piece in ["d-s001.vmdk", "d-s002.vmdk"] {
            assert_redundant_tables_match(&dir.path().join(piece));
        }
    }

    /// The primary and redundant grain tables of a hosted sparse extent agree entry for entry.
    fn assert_redundant_tables_match(path: &Path) {
        let mut f = File::open(path).unwrap();
        let mut h = [0u8; 512];
        f.read_exact(&mut h).unwrap();
        let header = crate::vmdk_sparse::VmdkSparseHeader::parse(&h).unwrap();
        assert!(
            header.rgd_offset_sectors != 0,
            "qemu writes a redundant directory"
        );
        let gts = header.num_grain_tables() as usize;
        let table = |f: &mut File, dir_sector: u64| -> Vec<u8> {
            let mut gd = vec![0u8; gts * 4];
            f.seek(SeekFrom::Start(dir_sector * 512)).unwrap();
            f.read_exact(&mut gd).unwrap();
            let mut all = Vec::new();
            for e in gd.chunks_exact(4) {
                let at = u32::from_le_bytes(e.try_into().unwrap()) as u64;
                let mut gt = vec![0u8; header.num_gtes_per_gt as usize * 4];
                f.seek(SeekFrom::Start(at * 512)).unwrap();
                f.read_exact(&mut gt).unwrap();
                all.extend(gt);
            }
            all
        };
        let primary = table(&mut f, header.gd_offset_sectors);
        let redundant = table(&mut f, header.rgd_offset_sectors);
        assert!(
            primary.iter().any(|b| *b != 0),
            "{} has grains",
            path.display()
        );
        assert_eq!(primary, redundant, "{}", path.display());
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
    fn flat_export_zero_pads_unaligned_source() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("out.vmdk");

        // 2 sectors + 186 trailing bytes (mirrors the real 2147462842 case:
        // 186 bytes past a sector boundary).
        let src: Vec<u8> = (0..2 * 512 + 186u32)
            .map(|i| (i ^ (i >> 3)) as u8)
            .collect();
        let aligned = 3 * 512u64;

        let mut cursor = std::io::Cursor::new(&src);
        export_vmdk_flat(&mut cursor, &dst, src.len() as u64, &mut |_| {}, &|| false).unwrap();

        let flat = tmp.path().join("out-flat.vmdk");
        assert_eq!(
            std::fs::metadata(&flat).unwrap().len(),
            aligned,
            "extent padded up to next sector"
        );

        let mut reader = VmdkFlatReader::open(&dst).unwrap();
        assert_eq!(reader.len(), aligned, "descriptor reports padded size");
        let mut got = vec![0u8; aligned as usize];
        reader.read_exact(&mut got).unwrap();
        assert_eq!(&got[..src.len()], &src[..], "real bytes preserved");
        assert!(
            got[src.len()..].iter().all(|&b| b == 0),
            "tail is zero-padded"
        );
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
        expected.extend(disk_core::repeat_n(0u8, 1024));
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
