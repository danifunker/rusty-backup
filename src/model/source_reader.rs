//! Path-based source openers that transparently unwrap container formats.
//!
//! For every code path that reads a partition out of a disk image by
//! `(path, partition_offset)`, the raw bytes at `partition_offset` are only
//! meaningful if the file is a flat image. CHD files store compressed hunks
//! and GHO files store a compressed sector stream, so callers have to swap
//! in [`ChdReader`] / [`GhoReader`] before applying the offset.
//!
//! `is_chd_path` / `is_gho_path` do cheap magic sniffs; `open_read` returns
//! a boxed `Read+Seek+Send` ready for `open_filesystem` /
//! `partition_minimum_size` and friends. Container writes are not supported,
//! so there is no `open_write`.

use std::fs::File;
use std::io::{BufReader, Read as _};
use std::path::Path;

use anyhow::{Context, Result};

use crate::rbformats::cbk::{
    cbk_member_content_reader, cbk_member_content_reader_at, read_cbk_index, CbkMember,
    CbkPayloadReader,
};
use crate::rbformats::chd::ChdReader;
use crate::rbformats::containers::atr::{decode_atr_bytes, looks_like_atr_header};
use crate::rbformats::containers::d88::{decode_d88_bytes, looks_like_d88_header};
use crate::rbformats::containers::dim::{decode_dim_bytes, looks_like_dim_header};
use crate::rbformats::containers::edsk::{decode_edsk_bytes, looks_like_edsk_header};
use crate::rbformats::containers::g64::{decode_g64_bytes, looks_like_g64_header};
use crate::rbformats::containers::hdf::{decode_hdf_bytes, detect_hdf_offset};
use crate::rbformats::containers::hdm::decode_hdm_bytes;
use crate::rbformats::containers::msa::{decode_msa_bytes, MSA_MAGIC};
use crate::rbformats::containers::sector_order::{open_apple_ii_dsk, APPLE_II_DISK_BYTES};
use crate::rbformats::containers::xdf::decode_xdf_bytes;
use crate::rbformats::gho::{GhoReader, GHO_MAGIC};
use crate::rbformats::imz::{ImzReader, IMZ_MAGIC};
use crate::rbformats::zip_disk::{ZipDiskReader, ZIP_MAGIC};
use crate::rbformats::{detect_image_format_with_path, wrap_image_reader, ImageFormat, ReadSeek};
use flate2::read::MultiGzDecoder;

/// Cheap magic sniff: returns true when `path` starts with `MComprHD`.
pub fn is_chd_path(path: &Path) -> bool {
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 8];
    matches!(f.read(&mut magic), Ok(n) if n == 8) && &magic == b"MComprHD"
}

/// Cheap sniff: returns true when `path` looks like a WinImage IMZ
/// archive. IMZ is just a ZIP, so the bare magic (`PK\x03\x04`) would
/// false-positive on arbitrary ZIPs — we additionally require the
/// extension to be `.imz` (case-insensitive). Both conditions must hold
/// before [`open_read`] swaps in an [`ImzReader`].
pub fn is_imz_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("imz"))
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 4];
    matches!(f.read(&mut magic), Ok(n) if n == 4) && &magic == IMZ_MAGIC
}

/// `.zip` holding a flat / raw disk image (a CF/SD card dump, hard-disk
/// image, etc.). A bare `PK\x03\x04` would false-positive on every ZIP, so
/// — like [`is_imz_path`] — we require the `.zip` extension AND the magic.
/// `.imz` (handled by [`is_imz_path`]) carries a different extension, so the
/// two openers never collide. The specific entry to open is resolved at
/// open time by [`ZipDiskReader`].
pub fn is_zip_image_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("zip"))
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 4];
    matches!(f.read(&mut magic), Ok(n) if n == 4) && &magic == ZIP_MAGIC
}

/// True when `path` is a 140 KB Apple-II disk image (`.do`, `.po`, or
/// `.dsk`). Used by [`open_read`] to decide whether to run
/// [`open_apple_ii_dsk`], which converts a ProDOS-order DOS 3.3 disk
/// to DOS-order at file-open time so the downstream `apple_dos` driver
/// reads bytes at the right offsets. Pure-ProDOS disks (any order) and
/// DOS-order DOS 3.3 disks pass through unchanged.
pub fn is_apple_ii_dsk_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| {
            let s = s.to_ascii_lowercase();
            s == "do" || s == "po" || s == "dsk"
        })
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    std::fs::metadata(path)
        .map(|m| m.len() as usize == APPLE_II_DISK_BYTES)
        .unwrap_or(false)
}

/// Cheap sniff: returns true when `path` looks like an Atari MSA floppy
/// image. MSA's `$0E 0F` magic is short; we additionally require the
/// extension to be `.msa` (case-insensitive) to keep the detector from
/// guessing on arbitrary files whose first two bytes coincide.
/// Cheap sniff: returns true when `path` looks like a CPCEMU DSK / EDSK
/// container (Amstrad CPC / PCW / Tatung Einstein CP/M floppies). Checks
/// the `.dsk` extension AND the first 34 bytes for one of the two CPCEMU
/// magic strings, so this never false-positives on raw `.dsk` files used
/// by other cores.
pub fn is_edsk_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("dsk"))
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut head = [0u8; 34];
    matches!(f.read(&mut head), Ok(n) if n >= 22) && looks_like_edsk_header(&head)
}

/// Cheap sniff for an Atari `.atr` disk image: requires the `.atr`
/// extension AND the 0x0296 magic.
pub fn is_atr_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("atr"))
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut head = [0u8; 2];
    matches!(f.read(&mut head), Ok(n) if n >= 2) && looks_like_atr_header(&head)
}

/// Cheap sniff for a Commodore G64 / G71 raw-GCR image: requires the
/// `.g64` / `.g71` extension AND the `GCR-1541` / `GCR-1571` signature, so
/// it never false-positives on a same-extension blob.
pub fn is_g64_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("g64") || s.eq_ignore_ascii_case("g71"))
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut head = [0u8; 8];
    matches!(f.read(&mut head), Ok(n) if n >= 8) && looks_like_g64_header(&head)
}

/// Cheap sniff: returns true when `path` looks like a Sharp `.d88` floppy
/// container (X68000, PC-88, PC-98, MSX, FM-7). The format has no magic
/// string, so we require the `.d88` extension AND a plausible-looking
/// disk-info header (media-type byte ∈ {0x00, 0x10, 0x20}, write-protect
/// byte ∈ {0x00, 0x10}, and the first track-offset table entry pointing
/// past the 656-byte offset table). False positives on arbitrary
/// `.d88`-named files are unlikely with those constraints combined.
pub fn is_d88_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("d88"))
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut head = [0u8; 0x24];
    matches!(f.read(&mut head), Ok(n) if n >= 0x24) && looks_like_d88_header(&head)
}

/// Cheap sniff: returns true when `path` is an X68000 `.xdf` floppy — a raw
/// headerless flat-sector dump whose geometry is inferred from the file
/// size. XDF has no header magic, so this is extension-only (case-
/// insensitive); `decode_xdf_bytes` validates the size and errors if it
/// doesn't match a supported floppy geometry.
pub fn is_xdf_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("xdf"))
        .unwrap_or(false)
}

/// Cheap sniff: returns true when `path` is a PC-98 `.hdm` floppy. HDM is
/// byte-identical to XDF on disk; like XDF it has no magic and is routed by
/// extension, with `decode_hdm_bytes` validating the geometry.
pub fn is_hdm_path(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("hdm"))
        .unwrap_or(false)
}

/// Cheap sniff: returns true when `path` is a DiskExplorer `.dim` floppy.
/// True for the `.dim` extension OR a `DIFC` signature at byte 0xAB (so a
/// mis-named DIFC image still decodes), mirroring `detect_container_kind`'s
/// DIM routing.
pub fn is_dim_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("dim"))
        .unwrap_or(false);
    if ext_ok {
        return true;
    }
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut head = [0u8; 256];
    let n = f.read(&mut head).unwrap_or(0);
    looks_like_dim_header(&head[..n])
}

/// True when `path` ends in `.hdf` (case-insensitive) AND the bytes
/// look like an Arculator-wrapped ADFS image. Pure bare `.hdf` files
/// (RPCEmu / MiSTer Archie style) do NOT trigger this — they pass
/// through as raw bytes since the partition layer's superfloppy probe
/// already finds the Disc Record at byte 0xDC0. Only Arculator's
/// 512-byte-header variant needs decoding, and only those return true.
pub fn is_arculator_hdf_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("hdf"))
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    // Read enough of the file to span the candidate Disc Record at
    // byte 0xFC0 (= ARCULATOR_HEADER_SIZE + 0xDC0 + 32).
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut head = vec![0u8; 0x200 + 0xDC0 + 32];
    let n = f.read(&mut head).unwrap_or(0);
    if n < head.len() {
        return false;
    }
    matches!(detect_hdf_offset(&head), Some(off) if off > 0)
}

pub fn is_msa_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("msa"))
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 2];
    matches!(f.read(&mut magic), Ok(n) if n == 2) && magic == MSA_MAGIC
}

/// Cheap magic sniff: returns true when `path` starts with the Norton
/// Ghost container magic `FE EF`. Both `.gho` (primary) and `.ghs` (span)
/// files carry the same magic, so the result is true for any file in a
/// Ghost span set.
///
/// This does NOT distinguish SECTOR-mode from file-aware: the latter
/// still has the magic but `GhoReader::open` will refuse it. Callers
/// must be prepared for `open_read` to fail on file-aware GHOs.
pub fn is_gho_path(path: &Path) -> bool {
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 2];
    matches!(f.read(&mut magic), Ok(n) if n == 2) && magic == GHO_MAGIC
}

/// `.adz` / `.hdz` — gzip-wrapped Amiga images (an `.adf` floppy or `.hdf`
/// hard-disk image inside a gzip stream). Matched by extension + gzip magic so
/// a mislabeled file isn't mistaken for one. Kept as the container (not
/// materialized to a separate `.adf`/`.hdf`) so edits re-gzip back in place.
pub fn is_gzip_image_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("adz") || e.eq_ignore_ascii_case("hdz"))
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    has_gzip_magic(path)
}

/// True for any gzip-wrapped disk image we transparently decompress on open:
/// the editable Amiga `.adz`/`.hdz` containers ([`is_gzip_image_path`]) plus a
/// generically `.gz`-suffixed image — a gzipped raw disk or a `.pdi.gz` Alto /
/// Pilot pack. Matched by extension + gzip magic so a mislabeled file isn't
/// taken for one. Unlike `.adz`/`.hdz`, a bare `.gz` image is read-only (there
/// is no re-gzip-on-commit path), so it is deliberately NOT part of
/// [`is_editable_container_path`].
pub fn is_gzip_wrapped_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| {
            e.eq_ignore_ascii_case("adz")
                || e.eq_ignore_ascii_case("hdz")
                || e.eq_ignore_ascii_case("gz")
        })
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    has_gzip_magic(path)
}

/// First two bytes equal the gzip magic `1f 8b`.
fn has_gzip_magic(path: &Path) -> bool {
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 2];
    matches!(f.read(&mut magic), Ok(n) if n == 2) && magic == [0x1f, 0x8b]
}

/// Fully decompress a gzip file into memory. Used for the small gzip-wrapped
/// Alto / Pilot packs (`.pdi.gz`, gzipped CopyDisk) that must be parsed as a
/// whole [`crate::fs::alto::Disk`] rather than streamed as flat sectors.
pub fn gunzip_to_vec(path: &Path) -> std::io::Result<Vec<u8>> {
    let input = File::open(path)?;
    let mut decoder = flate2::read::GzDecoder::new(BufReader::new(input));
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}

/// Decompress just the first `n` bytes of a gzip file, for cheap magic sniffing
/// without inflating the whole stream. Returns fewer than `n` bytes only if the
/// decompressed stream is shorter.
pub fn gunzip_prefix(path: &Path, n: usize) -> std::io::Result<Vec<u8>> {
    let input = File::open(path)?;
    let mut decoder = flate2::read::GzDecoder::new(BufReader::new(input));
    let mut out = vec![0u8; n];
    let mut filled = 0;
    while filled < n {
        match decoder.read(&mut out[filled..])? {
            0 => break,
            k => filled += k,
        }
    }
    out.truncate(filled);
    Ok(out)
}

/// A seekable reader over a gzip-decoded image held in a temp file, so a large
/// `.hdz` isn't decompressed entirely into RAM. The temp file is removed when
/// the reader is dropped.
struct GzipTempReader {
    file: File,
    _temp: tempfile::TempPath,
}

impl std::io::Read for GzipTempReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl std::io::Seek for GzipTempReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

/// A seekable reader over a `.cbk` backup container, presented as the whole
/// reconstructed disk so inspect/browse/restore treat it like any flat image
/// (no user-visible "extract first" step — cb_dos_network_and_state.md §2e).
///
/// v1 strategy (the `GzipTempReader`/`ZipDiskReader` precedent): materialize the
/// container to a temp folder, reconstruct the disk into a temp image at its
/// original geometry via the restore engine, and delegate `Read`/`Seek` to that
/// temp file. (A future lazy reader can decompress only the chunked members a
/// seek touches; that's a reader upgrade, no format change.)
struct CbkTempReader {
    file: File,
    _temp: tempfile::TempPath,
}

impl std::io::Read for CbkTempReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl std::io::Seek for CbkTempReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}

// ---- Lazy `.cbk` disk reader ----------------------------------------------
//
// Presents the reconstructed disk (Read+Seek) WITHOUT materializing the whole
// container or building a temp disk image: the small structural sectors (MBR +
// EBRs) are reconstructed eagerly, and each partition's bytes are decompressed
// on demand from its `.cbk` member only when a read lands in it. Engages only
// for plain MBR backups where the restore-engine's per-partition
// `hidden_sectors` patch is a provable no-op (so verbatim member bytes equal
// what the full reconstruct would write); everything else falls back to
// `open_cbk_as_disk`. The result is byte-identical to the full reconstruct.

enum CbkSrc {
    Bytes(Vec<u8>),
    /// Region backed by member `member_idx`; its decompressed content has length
    /// `logical_len`, and any region bytes past that are zero (compacted tail).
    Member {
        member_idx: usize,
        logical_len: u64,
    },
}

struct CbkRegion {
    start: u64,
    len: u64,
    src: CbkSrc,
}

enum Located {
    Zero(u64),                    // region_end
    Bytes(usize, u64),            // region index, region_end
    Member(usize, u64, u64, u64), // member_idx, region_start, logical_len, region_end
}

struct CbkLazyReader {
    path: std::path::PathBuf,
    members: Vec<CbkMember>,
    regions: Vec<CbkRegion>, // sorted by start, non-overlapping; gaps read as zero
    disk_size: u64,
    pos: u64,
    /// The partition currently being read sequentially: (member_idx, decoder,
    /// bytes already decoded). Reused for forward reads; recreated on a backward
    /// seek or a switch to another member.
    active: Option<(usize, MultiGzDecoder<CbkPayloadReader>, u64)>,
}

fn read_upto<R: std::io::Read>(r: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match r.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

fn to_io(e: anyhow::Error) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

impl CbkLazyReader {
    fn locate(&self, pos: u64) -> Located {
        let idx = self.regions.partition_point(|r| r.start <= pos);
        if idx > 0 {
            let r = &self.regions[idx - 1];
            if pos < r.start + r.len {
                return match &r.src {
                    CbkSrc::Bytes(_) => Located::Bytes(idx - 1, r.start + r.len),
                    CbkSrc::Member {
                        member_idx,
                        logical_len,
                    } => Located::Member(*member_idx, r.start, *logical_len, r.start + r.len),
                };
            }
        }
        let next = self
            .regions
            .get(idx)
            .map(|r| r.start)
            .unwrap_or(self.disk_size);
        Located::Zero(next)
    }

    /// Decompress `buf.len()` bytes of member `member_idx` starting at member
    /// offset `off`, advancing/recreating the active decoder as needed.
    fn read_member(
        &mut self,
        member_idx: usize,
        off: u64,
        buf: &mut [u8],
    ) -> std::io::Result<usize> {
        // Pick the chunk (source-span gzip member) covering `off`. For a
        // single-chunk member this is always 0 (`chunk_start` 0), so the behavior
        // is unchanged. For a multi-chunk member we start decode at that chunk
        // (its content begins at `chunk_start`), so we never decode more than one
        // span to reach `off` — fast random access, forward *and* backward.
        let k = self.members[member_idx].chunk_for_offset(off);
        let chunk_start = self.members[member_idx].chunk_src_offset(k);
        let recreate = match &self.active {
            // recreate on member switch, backward seek, or a forward jump into a
            // later chunk (decoding from `chunk_start` skips less than continuing).
            Some((m, _, decoded)) => *m != member_idx || *decoded > off || chunk_start > *decoded,
            None => true,
        };
        if recreate {
            let dec = cbk_member_content_reader_at(&self.path, &self.members[member_idx], k)
                .map_err(to_io)?;
            self.active = Some((member_idx, dec, chunk_start));
        }
        let (_, dec, decoded) = self.active.as_mut().unwrap();
        let mut skip = [0u8; 16384];
        while *decoded < off {
            let want = ((off - *decoded) as usize).min(skip.len());
            let got = read_upto(dec, &mut skip[..want])?;
            if got == 0 {
                break;
            }
            *decoded += got as u64;
        }
        let got = read_upto(dec, buf)?;
        *decoded += got as u64;
        Ok(got)
    }
}

impl std::io::Read for CbkLazyReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() || self.pos >= self.disk_size {
            return Ok(0);
        }
        let cap = (self.disk_size - self.pos).min(out.len() as u64);
        match self.locate(self.pos) {
            Located::Zero(end) => {
                let n = (end - self.pos).min(cap) as usize;
                out[..n].fill(0);
                self.pos += n as u64;
                Ok(n)
            }
            Located::Bytes(idx, end) => {
                let n = (end - self.pos).min(cap) as usize;
                let inner = (self.pos - self.regions[idx].start) as usize;
                if let CbkSrc::Bytes(b) = &self.regions[idx].src {
                    out[..n].copy_from_slice(&b[inner..inner + n]);
                }
                self.pos += n as u64;
                Ok(n)
            }
            Located::Member(member_idx, mstart, logical_len, end) => {
                let off = self.pos - mstart;
                let mut n = (end - self.pos).min(cap) as usize;
                if off >= logical_len {
                    out[..n].fill(0);
                    self.pos += n as u64;
                    return Ok(n);
                }
                n = n.min((logical_len - off) as usize);
                let got = self.read_member(member_idx, off, &mut out[..n])?;
                out[got..n].fill(0); // pad if the member ended early (shouldn't, within logical_len)
                self.pos += n as u64;
                Ok(n)
            }
        }
    }
}

impl std::io::Seek for CbkLazyReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let new = match pos {
            std::io::SeekFrom::Start(n) => n as i128,
            std::io::SeekFrom::Current(d) => self.pos as i128 + d as i128,
            std::io::SeekFrom::End(d) => self.disk_size as i128 + d as i128,
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

/// Read a `.cbk` member's full logical content (decompressed) into a Vec.
fn cbk_read_member_all(path: &Path, members: &[CbkMember], name: &str) -> Result<Option<Vec<u8>>> {
    let Some(m) = members.iter().find(|m| m.name == name) else {
        return Ok(None);
    };
    let mut dec = cbk_member_content_reader(path, m)?;
    let mut out = Vec::new();
    std::io::copy(&mut dec, &mut out)?;
    Ok(Some(out))
}

/// True if the restore engine's `hidden_sectors` patch for this partition would
/// be a no-op (so serving the member bytes verbatim is byte-identical to the
/// full reconstruct). Runs the real patch on a head buffer and checks nothing
/// changed; an FS whose field already equals `effective_lba` (the read-as-disk
/// case for a well-formed backup) leaves the head untouched.
fn cbk_partition_patch_is_noop(head: Vec<u8>, effective_lba: u64) -> bool {
    if head.len() < 512 {
        return true; // not a recognizable VBR; the patch functions skip it
    }
    let before = head.clone();
    let mut cur = std::io::Cursor::new(head);
    let mut nolog = |_: &str| {};
    if crate::fs::patch_hidden_sectors_for(&mut cur, 0, effective_lba, &mut nolog).is_err() {
        return false; // can't verify -> be safe, fall back
    }
    let after = cur.into_inner();
    after.len() == before.len() && after == before
}

/// Try to open a `.cbk` as a lazy disk reader. Returns `Ok(None)` for any case
/// the lazy path doesn't confidently handle (so the caller falls back to the
/// full reconstruct), guaranteeing byte-identical output whenever it succeeds.
fn try_open_cbk_lazy(path: &Path) -> Result<Option<CbkLazyReader>> {
    let members = read_cbk_index(path)?;

    let Some(meta_bytes) = cbk_read_member_all(path, &members, "metadata.json")? else {
        return Ok(None);
    };
    let metadata: crate::backup::metadata::BackupMetadata = serde_json::from_slice(&meta_bytes)?;

    // Lazy path covers plain per-partition MBR backups only.
    if metadata.partition_table_type != "MBR"
        || metadata.layout != crate::backup::metadata::BackupLayout::PerPartition
        || metadata.partitions.iter().any(|p| p.start_byte.is_some())
    {
        return Ok(None);
    }

    let Some(mbr_vec) = cbk_read_member_all(path, &members, "mbr.bin")? else {
        return Ok(None);
    };
    if mbr_vec.len() < 512 {
        return Ok(None);
    }
    let mut mbr = [0u8; 512];
    mbr.copy_from_slice(&mbr_vec[..512]);

    // Mirror reconstruct_disk_from_backup exactly: build the layout overrides
    // (no user resize), patch the MBR entries (start/count/CHS), then build the
    // EBR chain and patch the extended container's total_sectors. Same functions,
    // same inputs -> identical MBR/EBR/placement.
    let Ok(overrides) = crate::restore::calculate_restore_layout(
        &metadata,
        &crate::restore::RestoreAlignment::Original,
        &[],
        metadata.source_size_bytes,
    ) else {
        return Ok(None);
    };
    crate::partition::mbr::patch_mbr_entries(&mut mbr, &overrides);
    let ebr = crate::restore::build_restore_ebr_chain(&metadata, &overrides, Some(&mbr));

    let mut regions: Vec<CbkRegion> = Vec::new();

    if let Some(ref r) = ebr {
        let ext_start = r.extended_start_lba as u64;
        let last_end = r
            .logical_starts
            .iter()
            .filter_map(|(idx, start)| {
                let size = overrides
                    .iter()
                    .find(|o| o.index == *idx)
                    .map(|o| o.export_size / 512)
                    .or_else(|| {
                        metadata
                            .partitions
                            .iter()
                            .find(|p| p.index == *idx)
                            .map(|p| p.original_size_bytes / 512)
                    })?;
                Some(start + size)
            })
            .max();
        if let Some(end_lba) = last_end {
            let new_total = (end_lba - ext_start) as u32;
            for i in 0..4 {
                let e = 446 + i * 16;
                let t = mbr[e + 4];
                let start = u32::from_le_bytes([mbr[e + 8], mbr[e + 9], mbr[e + 10], mbr[e + 11]]);
                if (t == 0x05 || t == 0x0F || t == 0x85) && start == ext_start as u32 {
                    mbr[e + 12..e + 16].copy_from_slice(&new_total.to_le_bytes());
                    break;
                }
            }
        }
    }
    regions.push(CbkRegion {
        start: 0,
        len: 512,
        src: CbkSrc::Bytes(mbr.to_vec()),
    });
    if let Some(ref r) = ebr {
        for (off, sec) in &r.ebr_sectors {
            regions.push(CbkRegion {
                start: *off,
                len: 512,
                src: CbkSrc::Bytes(sec.to_vec()),
            });
        }
    }

    // One region per partition, backed by its member; verify the patch no-op.
    for pm in &metadata.partitions {
        let Some(name) = pm.compressed_files.first() else {
            return Ok(None);
        };
        let Some(member_idx) = members.iter().position(|m| &m.name == name) else {
            return Ok(None);
        };
        let override_start = overrides
            .iter()
            .find(|o| o.index == pm.index)
            .map(|o| o.effective_start_lba());
        let effective_lba = if pm.is_logical || pm.index >= 4 {
            ebr.as_ref()
                .and_then(|r| {
                    r.logical_starts
                        .iter()
                        .find(|(i, _)| *i == pm.index)
                        .map(|(_, l)| *l)
                })
                .or(override_start)
                .unwrap_or(pm.start_lba)
        } else {
            override_start.unwrap_or(pm.start_lba)
        };
        let part_offset = effective_lba * 512;
        let export_size = overrides
            .iter()
            .find(|o| o.index == pm.index)
            .map(|o| o.export_size)
            .unwrap_or(pm.original_size_bytes);
        let logical_len = if pm.imaged_size_bytes > 0 {
            pm.imaged_size_bytes.min(export_size)
        } else {
            export_size
        };

        // Guard: decompress the head and confirm the hidden_sectors patch is a
        // no-op (else verbatim bytes would diverge from the full reconstruct).
        let mut dec = cbk_member_content_reader(path, &members[member_idx])?;
        let head_len = logical_len.min(32 * 1024) as usize;
        let mut head = vec![0u8; head_len];
        let got = read_upto(&mut dec, &mut head)?;
        head.truncate(got);
        if !cbk_partition_patch_is_noop(head, effective_lba) {
            return Ok(None);
        }

        regions.push(CbkRegion {
            start: part_offset,
            len: export_size,
            src: CbkSrc::Member {
                member_idx,
                logical_len,
            },
        });
    }

    regions.sort_by_key(|r| r.start);
    // Reject overlaps (shouldn't happen for a well-formed MBR layout).
    for w in regions.windows(2) {
        if w[0].start + w[0].len > w[1].start {
            return Ok(None);
        }
    }

    Ok(Some(CbkLazyReader {
        path: path.to_path_buf(),
        members,
        regions,
        disk_size: metadata.source_size_bytes,
        pos: 0,
        active: None,
    }))
}

/// Reconstruct a `.cbk` container into a temp disk image and return a seekable
/// reader over it. The materialized folder is dropped once the disk is built.
fn open_cbk_as_disk(path: &Path) -> Result<CbkTempReader> {
    use crate::restore::{run_restore, RestoreAlignment, RestoreConfig, RestoreProgress};

    // 1. Materialize the container into a temp backup folder.
    let folder = tempfile::Builder::new()
        .prefix(".rb-cbk-folder-")
        .tempdir()
        .context("create temp folder for .cbk")?;
    crate::rbformats::cbk::materialize_cbk_to_folder(path, folder.path())
        .with_context(|| format!("materialize {}", path.display()))?;

    // 2. Read the disk size the backup recorded.
    let meta_str = std::fs::read_to_string(folder.path().join("metadata.json"))
        .context("read materialized metadata.json")?;
    let meta: crate::backup::metadata::BackupMetadata =
        serde_json::from_str(&meta_str).context("parse materialized metadata.json")?;

    // 3. Reconstruct the disk into a temp image at original geometry, reusing the
    //    restore engine (it builds the per-partition placement/overrides itself).
    let temp = tempfile::Builder::new()
        .prefix(".rb-cbk-disk-")
        .tempfile()
        .context("create .cbk reconstruct tempfile")?;
    let temp_path = temp.into_temp_path();
    let cfg = RestoreConfig {
        backup_folder: folder.path().to_path_buf(),
        target_path: temp_path.to_path_buf(),
        target_is_device: false,
        target_size: meta.source_size_bytes,
        alignment: RestoreAlignment::Original,
        partition_sizes: Vec::new(),
        write_zeros_to_unused: false,
    };
    let progress = std::sync::Arc::new(std::sync::Mutex::new(RestoreProgress::default()));
    run_restore(cfg, progress).context("reconstruct .cbk into a temp disk")?;
    drop(folder); // the materialized folder is no longer needed

    // 4. Read-only handle over the reconstructed disk.
    let file = File::open(&temp_path).context("reopen reconstructed .cbk disk")?;
    Ok(CbkTempReader {
        file,
        _temp: temp_path,
    })
}

/// Open `path` for reading, transparently wrapping CHD and SECTOR-mode
/// GHO files in their respective streaming readers.
///
/// Partition offsets passed to consumers (e.g. `open_filesystem`) match the
/// original raw image, so callers don't have to know whether the underlying
/// container is a CHD or GHO.
///
/// File-aware (`image_type=0x00`) and password-protected GHOs cause
/// `GhoReader::open` to error out; the caller can then fall back to the
/// legacy `materialize_gho_to_temp` path which decodes to a tempfile.
pub fn open_read(path: &Path) -> Result<Box<dyn ReadSeek>> {
    open_read_dispatch(path, None, None)
}

/// Variant of [`open_read`] that accepts an optional password for
/// container formats that support encryption (IMZ, GHO, and password-
/// protected `.zip` disks; CHD's scheme is not wired here). Pass `None` to
/// behave like [`open_read`].
pub fn open_read_with_password(path: &Path, password: Option<&[u8]>) -> Result<Box<dyn ReadSeek>> {
    open_read_dispatch(path, password, None)
}

/// As [`open_read_with_password`], but `inside` names a specific entry to
/// open when `path` is a `.zip` holding more than one disk image (the CLI
/// `--inside` flag). Ignored for every other source type.
pub fn open_read_with_password_and_entry(
    path: &Path,
    password: Option<&[u8]>,
    inside: Option<&str>,
) -> Result<Box<dyn ReadSeek>> {
    open_read_dispatch(path, password, inside)
}

/// The container-dispatch chain behind [`open_read`] /
/// [`open_read_with_password`]. `inside` names the specific entry to open
/// when `path` is a `.zip` holding more than one disk image (the CLI
/// `--inside` flag); it is ignored for every other source type.
fn open_read_dispatch(
    path: &Path,
    password: Option<&[u8]>,
    inside: Option<&str>,
) -> Result<Box<dyn ReadSeek>> {
    if is_chd_path(path) {
        let chd = ChdReader::open(path).with_context(|| format!("open CHD {}", path.display()))?;
        Ok(Box::new(chd))
    } else if crate::rbformats::cbk::is_cbk(path) {
        // .cbk backup container: present the reconstructed disk so partition
        // parse + filesystem open + browse/extract all work natively. Try the
        // lazy reader first (only decompresses the members a seek touches); fall
        // back to the full reconstruct-to-tempfile for any case it can't prove
        // byte-identical (non-MBR tables, hidden-sectors patches, etc.).
        if let Ok(Some(lazy)) = try_open_cbk_lazy(path) {
            return Ok(Box::new(lazy));
        }
        let reader = open_cbk_as_disk(path)
            .with_context(|| format!("open .cbk container {}", path.display()))?;
        Ok(Box::new(reader))
    } else if is_gho_path(path) {
        let gho = GhoReader::open_with_password(path, password)
            .with_context(|| format!("open GHO {}", path.display()))?;
        Ok(Box::new(gho))
    } else if is_imz_path(path) {
        let imz = ImzReader::open_with_password(path, password)
            .with_context(|| format!("open IMZ {}", path.display()))?;
        Ok(Box::new(imz))
    } else if is_edsk_path(path) {
        // EDSK / DSK floppies: read into memory, decode to a flat
        // sector stream, hand off as Cursor. Same shape as MSA — small
        // images (~1 MB max), no streaming needed.
        let bytes = std::fs::read(path).with_context(|| format!("read EDSK {}", path.display()))?;
        let flat =
            decode_edsk_bytes(&bytes).with_context(|| format!("decode EDSK {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_d88_path(path) {
        // Sharp .d88: X68000 / PC-88 / PC-98 / MSX / FM-7 floppy images.
        // 2HD disks decode to ~1.26 MB flat; in-memory like EDSK / MSA.
        let bytes = std::fs::read(path).with_context(|| format!("read D88 {}", path.display()))?;
        let flat =
            decode_d88_bytes(&bytes).with_context(|| format!("decode D88 {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_g64_path(path) {
        // Commodore G64 raw-GCR floppy: decode the GCR bitstream down to a
        // flat .d64 so the cbm engine (which speaks logical sectors) reads
        // it. Read-only — we don't re-encode GCR.
        let bytes = std::fs::read(path).with_context(|| format!("read G64 {}", path.display()))?;
        let flat =
            decode_g64_bytes(&bytes).with_context(|| format!("decode G64 {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_woz_path(path) {
        // WOZ 1/2 Apple/Mac floppy: decode the GCR bitstream to a flat sector
        // buffer (≤ 800 KB) and hand off as Cursor, like G64. Read-only here;
        // editing re-encodes via container_edit's EditFormat::Woz.
        let mut reader = crate::rbformats::woz::WozReader::open(path)
            .with_context(|| format!("decode WOZ {}", path.display()))?;
        let mut flat = Vec::with_capacity(reader.len() as usize);
        reader
            .read_to_end(&mut flat)
            .with_context(|| format!("read WOZ {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_atr_path(path) {
        // Atari .atr: strip the 16-byte header to expose the flat sector
        // body the atari_dos engine reads. `.xfd` is headerless and falls
        // through to the generic open-as-File path.
        let bytes = std::fs::read(path).with_context(|| format!("read ATR {}", path.display()))?;
        let flat =
            decode_atr_bytes(&bytes).with_context(|| format!("decode ATR {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_dim_path(path) {
        // DiskExplorer .dim floppy (X68000 / PC-98): 256-byte header + flat
        // payload, decodes to ~1.2 MB. In-memory like EDSK / MSA / D88.
        let bytes = std::fs::read(path).with_context(|| format!("read DIM {}", path.display()))?;
        let (flat, _media) =
            decode_dim_bytes(&bytes).with_context(|| format!("decode DIM {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_xdf_path(path) {
        // X68000 .xdf — raw headerless floppy dump; geometry from file size.
        let bytes = std::fs::read(path).with_context(|| format!("read XDF {}", path.display()))?;
        let (flat, _media) =
            decode_xdf_bytes(&bytes).with_context(|| format!("decode XDF {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_hdm_path(path) {
        // PC-98 .hdm — byte-identical to XDF on disk.
        let bytes = std::fs::read(path).with_context(|| format!("read HDM {}", path.display()))?;
        let (flat, _media) =
            decode_hdm_bytes(&bytes).with_context(|| format!("decode HDM {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_arculator_hdf_path(path) {
        // Arculator-style .hdf with 512-byte header before the ADFS
        // volume. Bare .hdf files (RPCEmu / MiSTer Archie) fall through
        // to the generic open-as-File path below; the partition layer's
        // superfloppy probe finds the Disc Record at byte 0xDC0
        // unaided.
        let bytes = std::fs::read(path).with_context(|| format!("read HDF {}", path.display()))?;
        let flat =
            decode_hdf_bytes(&bytes).with_context(|| format!("decode HDF {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_msa_path(path) {
        // MSA is a small floppy container (≤ 1.5 MiB raw); decoding into
        // memory and wrapping in a Cursor is simpler than a streaming reader
        // and matches how the container framework hands out flat sectors.
        let bytes = std::fs::read(path).with_context(|| format!("read MSA {}", path.display()))?;
        let flat =
            decode_msa_bytes(&bytes).with_context(|| format!("decode MSA {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_apple_ii_dsk_path(path) {
        // 140 KB Apple-II floppy. `open_apple_ii_dsk` converts a
        // ProDOS-order DOS 3.3 disk to DOS-order; pure-ProDOS and
        // already-DOS-order disks pass through unchanged.
        let bytes = std::fs::read(path)
            .with_context(|| format!("read Apple-II disk {}", path.display()))?;
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_string());
        let flat = open_apple_ii_dsk(bytes, ext.as_deref())
            .with_context(|| format!("decode Apple-II disk {}", path.display()))?;
        Ok(Box::new(std::io::Cursor::new(flat)))
    } else if is_gzip_wrapped_path(path) {
        // .adz/.hdz/.gz: gzip-wrapped image. Decode to a temp file (an .hdf
        // can be large, so don't hold it all in RAM) and read from there.
        let input =
            File::open(path).with_context(|| format!("open gzip image {}", path.display()))?;
        let mut decoder = flate2::read::GzDecoder::new(BufReader::new(input));
        let mut temp = tempfile::Builder::new()
            .prefix(".rb-gzip-image-")
            .tempfile()
            .context("create gzip-decode tempfile")?;
        std::io::copy(&mut decoder, temp.as_file_mut())
            .with_context(|| format!("decompress gzip image {}", path.display()))?;
        let temp_path = temp.into_temp_path();
        let file = File::open(&temp_path).context("reopen decoded gzip image")?;
        Ok(Box::new(GzipTempReader {
            file,
            _temp: temp_path,
        }))
    } else if is_zip_image_path(path) {
        // A plain .zip holding a RAW disk image. Pick the disk entry (or the
        // `--inside`-named one) and inflate it to a temp file; disk images
        // can be large, so we never hold the whole thing in RAM.
        let zip = ZipDiskReader::open_with(path, password, inside)
            .with_context(|| format!("open ZIP disk image {}", path.display()))?;
        Ok(Box::new(zip))
    } else {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        Ok(Box::new(BufReader::new(f)))
    }
}

/// Open `path` as a flat, seekable disk-image stream with **any** container or
/// image wrapper peeled off — the single "peel a picked source into a readable
/// disk image" primitive every front end's partition probe shares.
///
/// - CHD / GHO / IMZ / flat-floppy containers ([`is_container_path`]) decode
///   through [`open_read_with_password`].
/// - VHD / 2MG / DMG / DiskCopy 4.2 (and any other) image wrapper is unwrapped
///   via the format pipeline ([`detect_image_format_with_path`] +
///   [`wrap_image_reader`]).
/// - A raw image falls through to a plain buffered file.
///
/// The partition offsets a caller reads after this line up with the offsets a
/// later [`crate::model::browse_session::BrowseSession`] open produces, since
/// the browse session peels the same wrappers. Keep this the one place the peel
/// set is defined so CHD / GHO / IMZ / VHD / … probe identically everywhere.
pub fn open_peeled_read(path: &Path, password: Option<&[u8]>) -> Result<Box<dyn ReadSeek>> {
    open_peeled_read_with_entry(path, password, None)
}

/// As [`open_peeled_read`], but `inside` names a specific entry to open when
/// `path` is a `.zip` holding more than one disk image (the CLI `--inside`
/// flag). Ignored for every non-zip source.
pub fn open_peeled_read_with_entry(
    path: &Path,
    password: Option<&[u8]>,
    inside: Option<&str>,
) -> Result<Box<dyn ReadSeek>> {
    if is_container_path(path) {
        return open_read_dispatch(path, password, inside);
    }
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    match detect_image_format_with_path(file, Some(path)) {
        Ok(format) if !matches!(format, ImageFormat::Raw) => {
            let file2 = File::open(path).with_context(|| format!("open {}", path.display()))?;
            let (reader, _size) = wrap_image_reader(file2, format)
                .with_context(|| format!("unwrap image {}", path.display()))?;
            Ok(reader)
        }
        _ => {
            let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
            Ok(Box::new(BufReader::new(file)))
        }
    }
}

/// Subset of [`is_container_path`] that [`open_read`] decodes fully into an
/// in-memory flat-sector `Cursor` (no streaming, no password): the floppy
/// wrappers MSA / EDSK / D88 / DIM / XDF / HDM / Arculator-HDF and the
/// 140 KB Apple-II disks. Excludes the streaming / password formats (CHD /
/// GHO / IMZ); callers that special-case those (e.g. the browse session,
/// which threads a password through their dedicated readers) keep handling
/// them directly and use this to route only the plain floppy containers.
pub fn is_flat_floppy_container_path(path: &Path) -> bool {
    is_msa_path(path)
        || is_edsk_path(path)
        || is_d88_path(path)
        || is_g64_path(path)
        || is_atr_path(path)
        || is_dim_path(path)
        || is_xdf_path(path)
        || is_hdm_path(path)
        || is_arculator_hdf_path(path)
        || is_apple_ii_dsk_path(path)
}

/// True for a standalone WOZ 1/2 floppy image (`.woz` + WOZ magic). WOZ is an
/// editable container: it decodes to a flat 140K / 400K / 800K sector buffer
/// that [`crate::model::container_edit`] re-encodes via
/// [`crate::rbformats::woz_write::sectors_to_woz`] on commit.
pub fn is_woz_path(path: &Path) -> bool {
    let ext_ok = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("woz"))
        .unwrap_or(false);
    if !ext_ok {
        return false;
    }
    let Ok(mut f) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 12];
    match f.read(&mut magic) {
        Ok(n) if n >= 8 => crate::rbformats::woz::is_woz(&magic[..n]),
        _ => false,
    }
}

/// Containers that support in-place editing via
/// [`crate::model::container_edit`] (decode -> edit -> re-encode): the four
/// PC/Sharp floppy formats (XDF, HDM, DIM, D88), the Atari `.atr` (a trivial
/// header strip / wrap), the gzip `.adz`/`.hdz`, and the WOZ floppy. Subset of
/// [`is_flat_floppy_container_path`] (plus gzip + WOZ) excluding the
/// read-only-wrapped formats (MSA / EDSK / Apple-II / Arculator HDF) which have
/// no re-encoder wired up.
pub fn is_editable_container_path(path: &Path) -> bool {
    is_xdf_path(path)
        || is_hdm_path(path)
        || is_dim_path(path)
        || is_d88_path(path)
        || is_atr_path(path)
        || is_gzip_image_path(path)
        || is_woz_path(path)
}

/// True when [`open_read`] would transparently unwrap `path` into a decoded
/// flat-sector stream — any CHD / GHO / IMZ streaming reader or flat floppy
/// container. Callers that build their own reader use this to decide whether
/// to route through `open_read` instead of opening the file raw, so the
/// "what counts as a container" list lives in one place instead of being
/// re-listed (and drifting) at every call site.
pub fn is_container_path(path: &Path) -> bool {
    is_chd_path(path)
        || crate::rbformats::cbk::is_cbk(path)
        || is_gho_path(path)
        || is_imz_path(path)
        || is_zip_image_path(path)
        || is_gzip_wrapped_path(path)
        || is_woz_path(path)
        || is_flat_floppy_container_path(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    fn gz(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    fn mbr_with(entries: &[(u8, u32, u32)]) -> Vec<u8> {
        let mut m = vec![0u8; 512];
        for (i, (t, s, c)) in entries.iter().enumerate() {
            let e = 446 + i * 16;
            m[e + 4] = *t;
            m[e + 8..e + 12].copy_from_slice(&s.to_le_bytes());
            m[e + 12..e + 16].copy_from_slice(&c.to_le_bytes());
        }
        m[510] = 0x55;
        m[511] = 0xAA;
        m
    }

    fn pm(
        index: usize,
        start_lba: u64,
        orig: u64,
        imaged: u64,
        logical: bool,
    ) -> crate::backup::metadata::PartitionMetadata {
        crate::backup::metadata::PartitionMetadata {
            index,
            type_name: "FAT16".into(),
            partition_type_byte: 0x06,
            start_lba,
            start_byte: None,
            original_size_bytes: orig,
            imaged_size_bytes: imaged,
            compressed_files: vec![format!("partition-{index}.gz")],
            checksum: "0".into(),
            resized: false,
            compacted: true,
            is_logical: logical,
            partition_type_string: None,
            minimum_size_bytes: Some(imaged),
            defragmented_min_size_bytes: None,
            hfsplus_signature: None,
            defragmented_clone: false,
        }
    }

    /// The lazy `.cbk` reader must produce exactly the same disk bytes as the
    /// full reconstruct, including the MBR + EBR chain for a logical partition
    /// and the zero-filled compacted tails/gaps.
    #[test]
    fn cbk_lazy_reader_matches_full_reconstruct() {
        use crate::backup::metadata::*;
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("BK");
        std::fs::create_dir(&folder).unwrap();

        let imaged = 65_536u64;
        let orig = 1_048_576u64; // 2048 sectors
        let disk = 8_388_608u64; // 16384 sectors

        // Non-FS partition content -> the hidden_sectors patch is a no-op, so the
        // lazy path engages and serves verbatim.
        let p0: Vec<u8> = (0..imaged)
            .map(|i| i.wrapping_mul(7).wrapping_add(3) as u8)
            .collect();
        let p4: Vec<u8> = (0..imaged)
            .map(|i| i.wrapping_mul(13).wrapping_add(9) as u8)
            .collect();
        std::fs::write(folder.join("partition-0.gz"), gz(&p0)).unwrap();
        std::fs::write(folder.join("partition-4.gz"), gz(&p4)).unwrap();
        // primary (slot 0) + extended container (slot 1); the EBR is rebuilt.
        std::fs::write(
            folder.join("mbr.bin"),
            mbr_with(&[(0x06, 2048, 2048), (0x05, 8192, 8192)]),
        )
        .unwrap();

        let meta = BackupMetadata {
            version: 1,
            created: "2026-01-01T00:00:00Z".into(),
            source_device: "test".into(),
            source_size_bytes: disk,
            partition_table_type: "MBR".into(),
            checksum_type: "crc32".into(),
            compression_type: "gzip".into(),
            split_size_mib: None,
            sector_by_sector: false,
            layout: BackupLayout::PerPartition,
            container: None,
            container_logical_size: None,
            container_sha1: None,
            size_policy: None,
            alignment: AlignmentMetadata {
                detected_type: "Modern 1MB boundaries".into(),
                first_partition_lba: 2048,
                alignment_sectors: 2048,
                heads: 16,
                sectors_per_track: 63,
            },
            partitions: vec![
                pm(0, 2048, orig, imaged, false),
                pm(4, 10240, orig, imaged, true),
            ],
            bad_sectors: vec![],
            extended_container: Some(ExtendedContainerMetadata {
                mbr_index: 1,
                partition_type_byte: 5,
                start_lba: 8192,
                size_bytes: 8192 * 512,
            }),
        };
        std::fs::write(
            folder.join("metadata.json"),
            serde_json::to_vec_pretty(&meta).unwrap(),
        )
        .unwrap();

        let cbk = dir.path().join("BK.cbk");
        crate::rbformats::cbk::pack_folder_to_cbk(&folder, &cbk).unwrap();

        // The lazy path must engage (not fall back) for this plain MBR backup.
        let mut lazy = try_open_cbk_lazy(&cbk)
            .unwrap()
            .expect("lazy path should engage for a plain MBR .cbk");
        let mut lazy_all = Vec::new();
        lazy.read_to_end(&mut lazy_all).unwrap();

        let mut full = open_cbk_as_disk(&cbk).unwrap();
        let mut full_all = Vec::new();
        full.read_to_end(&mut full_all).unwrap();

        assert_eq!(lazy_all.len() as u64, disk, "lazy reader length");
        if let Some(i) = (0..lazy_all.len()).find(|&i| lazy_all[i] != full_all[i]) {
            let lo = i.saturating_sub(8);
            let hi = (i + 16).min(lazy_all.len());
            panic!(
                "lazy != full at offset {i} (sector {}): lazy={:?} full={:?}",
                i / 512,
                &lazy_all[lo..hi],
                &full_all[lo..hi]
            );
        }

        // Random-access seeks must also match the reconstructed disk.
        let mut lazy2 = try_open_cbk_lazy(&cbk).unwrap().unwrap();
        for off in [
            0u64,
            511,
            512,
            2048 * 512,
            2048 * 512 + imaged - 10,
            8192 * 512,
            10240 * 512,
            10240 * 512 + imaged + 5,
            disk - 1,
        ] {
            lazy2.seek(SeekFrom::Start(off)).unwrap();
            let mut b = [0u8; 64];
            let n = lazy2.read(&mut b).unwrap();
            let want = &full_all[off as usize..(off as usize + n).min(full_all.len())];
            assert_eq!(&b[..n], want, "seek/read mismatch at offset {off}");
        }
    }

    #[test]
    fn is_imz_path_requires_both_extension_and_magic() {
        let dir = tempfile::tempdir().unwrap();
        // Right magic, wrong extension.
        let zip_only = dir.path().join("regular.zip");
        std::fs::write(&zip_only, b"PK\x03\x04rest").unwrap();
        assert!(!is_imz_path(&zip_only), "ZIP without .imz extension");

        // Right extension, wrong magic.
        let wrong_magic = dir.path().join("fake.imz");
        std::fs::write(&wrong_magic, b"NOPE").unwrap();
        assert!(!is_imz_path(&wrong_magic), "imz extension without PK magic");

        // Both.
        let real = dir.path().join("real.imz");
        std::fs::write(&real, b"PK\x03\x04rest").unwrap();
        assert!(is_imz_path(&real));
    }

    #[test]
    fn open_read_routes_imz_through_imz_reader() {
        use std::io::Write as _;
        use zip::write::SimpleFileOptions;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.imz");
        let payload: Vec<u8> = (0..2048).map(|i| (i ^ 0x33) as u8).collect();
        {
            let f = File::create(&path).unwrap();
            let mut zw = zip::ZipWriter::new(f);
            zw.start_file(
                "disk.ima",
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated),
            )
            .unwrap();
            zw.write_all(&payload).unwrap();
            zw.finish().unwrap();
        }
        let mut r = open_read(&path).unwrap();
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn is_gho_path_rejects_non_gho() {
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("not_gho.bin");
        std::fs::write(&plain, b"hello world").unwrap();
        assert!(!is_gho_path(&plain));
    }

    #[test]
    fn is_gho_path_accepts_fe_ef_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fake.gho");
        let mut f = File::create(&path).unwrap();
        f.write_all(&[0xFE, 0xEF, 0x01]).unwrap();
        assert!(is_gho_path(&path));
    }

    #[test]
    fn is_msa_path_requires_both_extension_and_magic() {
        let dir = tempfile::tempdir().unwrap();
        // Right magic, wrong extension.
        let no_ext = dir.path().join("disk.bin");
        std::fs::write(&no_ext, b"\x0E\x0Frest").unwrap();
        assert!(!is_msa_path(&no_ext), "magic without .msa extension");

        // Right extension, wrong magic.
        let wrong_magic = dir.path().join("fake.msa");
        std::fs::write(&wrong_magic, b"NOPE").unwrap();
        assert!(
            !is_msa_path(&wrong_magic),
            ".msa extension without 0E 0F magic"
        );

        // Both.
        let real = dir.path().join("real.msa");
        std::fs::write(&real, b"\x0E\x0F\0\0").unwrap();
        assert!(is_msa_path(&real));
    }

    #[test]
    fn open_read_routes_msa_through_decoder_and_partition_detect_finds_fat() {
        use crate::partition::PartitionTable;
        use crate::rbformats::containers::msa::{encode_msa_bytes, MsaHeader};

        // Build a 720K raw FAT12 floppy in memory: BPB at byte 0, 0xAA55 at
        // 510-511. No partition table — `detect` should report
        // `PartitionTable::None { fs_hint: "FAT" }`.
        let header = MsaHeader {
            sectors_per_track: 9,
            sides: 1,
            start_track: 0,
            end_track: 79,
        };
        let total = 720 * 1024;
        let mut raw = vec![0u8; total];
        // Minimal valid BPB.
        raw[0] = 0xEB; // JMP short
        raw[1] = 0x3C;
        raw[2] = 0x90;
        raw[3..11].copy_from_slice(b"MSDOS5.0");
        raw[11] = 0x00; // 512 bps
        raw[12] = 0x02;
        raw[13] = 0x01; // 1 spc
        raw[14] = 0x01; // 1 reserved
        raw[15] = 0x00;
        raw[16] = 0x02; // 2 FATs
        raw[21] = 0xF9; // media descriptor for 720K
        raw[510] = 0x55;
        raw[511] = 0xAA;

        let msa = encode_msa_bytes(header, &raw);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("atari.msa");
        std::fs::write(&path, &msa).unwrap();
        assert!(is_msa_path(&path));

        let mut reader = open_read(&path).unwrap();
        // First 512 bytes should be the BPB-bearing boot sector.
        let mut sector = [0u8; 512];
        reader.read_exact(&mut sector).unwrap();
        assert_eq!(&sector[3..11], b"MSDOS5.0");
        assert_eq!(sector[510], 0x55);
        assert_eq!(sector[511], 0xAA);

        // And the partition detector should treat it as a FAT superfloppy.
        reader.seek(SeekFrom::Start(0)).unwrap();
        let table = PartitionTable::detect(&mut reader).unwrap();
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "FAT");
        assert_eq!(parts[0].size_bytes, total as u64);
    }

    /// SECTOR-mode uncompressed GHO opens as a GhoReader via the
    /// unified opener and reads through transparently.
    #[test]
    fn open_read_routes_sector_gho_through_gho_reader() {
        use crate::rbformats::gho::GHO_SECTOR_SIZE;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sector.gho");

        // Container header (FE EF version=1 comp=0 ... image_type=01 pw=0)
        // + 4 sectors of padding + FEEF sub-header + 2 sectors of data.
        let mut buf = vec![0u8; 512];
        buf[0] = 0xFE;
        buf[1] = 0xEF;
        buf[2] = 0x01; // version
        buf[3] = 0x00; // compression=none
        buf[10] = 0x01; // image_type=SECTOR
                        // Pad to sector 4 then FEEF sub-header at sector 4, data at sector 5.
        buf.resize(4 * GHO_SECTOR_SIZE as usize, 0);
        let mut feef = vec![0u8; 512];
        feef[0] = 0xFE;
        feef[1] = 0xEF;
        buf.extend_from_slice(&feef);
        // Two sectors of data.
        let mut s0 = vec![0u8; 512];
        s0[..16].copy_from_slice(b"sector zero data");
        let mut s1 = vec![0u8; 512];
        s1[..16].copy_from_slice(b"sector one  data");
        buf.extend_from_slice(&s0);
        buf.extend_from_slice(&s1);
        std::fs::write(&path, &buf).unwrap();

        let mut r = open_read(&path).unwrap();
        let mut sector = [0u8; 512];
        r.read_exact(&mut sector).unwrap();
        assert_eq!(&sector[..16], b"sector zero data");
        r.seek(SeekFrom::Start(512)).unwrap();
        r.read_exact(&mut sector).unwrap();
        assert_eq!(&sector[..16], b"sector one  data");
    }

    /// X68000 `.xdf` (headerless raw floppy) routes through `open_read` and
    /// the decoded stream is recognised as a FAT superfloppy. Regression
    /// guard for the bug where the inspect path MBR-parsed the raw container
    /// bytes and failed with "invalid boot signature".
    #[test]
    fn open_read_routes_xdf_and_partition_detect_finds_fat() {
        use crate::partition::PartitionTable;
        use crate::rbformats::containers::floppy_geom::FloppyMedia;

        // 720K geometry with a minimal FAT12 BPB + 0xAA55 boot signature —
        // no MBR, so detection should report a FAT superfloppy.
        let geom = FloppyMedia::Dd720.geometry();
        let total = geom.flat_size();
        let mut flat = vec![0u8; total];
        flat[0] = 0xEB; // JMP short
        flat[1] = 0x3C;
        flat[2] = 0x90;
        flat[3..11].copy_from_slice(b"MSDOS5.0");
        flat[11] = 0x00; // 512 bytes/sector
        flat[12] = 0x02;
        flat[13] = 0x01; // 1 sector/cluster
        flat[14] = 0x01; // 1 reserved sector
        flat[16] = 0x02; // 2 FATs
        flat[21] = 0xF9; // media descriptor for 720K
        flat[510] = 0x55;
        flat[511] = 0xAA;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.xdf");
        std::fs::write(&path, &flat).unwrap();

        assert!(is_xdf_path(&path));
        assert!(is_container_path(&path));

        let mut reader = open_read(&path).unwrap();
        let table = PartitionTable::detect(&mut reader).unwrap();
        assert_eq!(table.type_name(), "None"); // superfloppy, no partition table
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "FAT");
        assert_eq!(parts[0].size_bytes, total as u64);
    }

    /// DiskExplorer `.dim` (256-byte header + payload) routes through
    /// `open_read` and decodes back to the original flat sector stream.
    #[test]
    fn open_read_round_trips_dim_floppy() {
        use crate::rbformats::containers::dim::encode_dim_bytes;
        use crate::rbformats::containers::floppy_geom::FloppyMedia;

        let dir = tempfile::tempdir().unwrap();
        let geom = FloppyMedia::Hd1232.geometry();
        let flat: Vec<u8> = (0..geom.flat_size())
            .map(|i| ((i.wrapping_mul(7)) & 0xFF) as u8)
            .collect();
        let dim = encode_dim_bytes(&flat, geom).unwrap();
        let path = dir.path().join("disk.dim");
        std::fs::write(&path, &dim).unwrap();

        assert!(is_dim_path(&path));
        assert!(is_container_path(&path));
        let mut r = open_read(&path).unwrap();
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, flat);
    }

    /// A gzip-wrapped `.adz` is recognized as a container and `open_read` peels
    /// the gzip to the original flat image (so partition detection sees the
    /// real bytes, not the gzip header — the "Invalid MBR: 0xE148" bug).
    #[test]
    fn open_read_decodes_gzip_adz() {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("disk.adz");
        let payload: Vec<u8> = (0..8192usize)
            .map(|i| (i.wrapping_mul(7) & 0xFF) as u8)
            .collect();
        {
            let f = File::create(&path).unwrap();
            let mut enc = GzEncoder::new(f, Compression::default());
            enc.write_all(&payload).unwrap();
            enc.finish().unwrap();
        }

        assert!(is_gzip_image_path(&path));
        assert!(is_container_path(&path));

        let mut r = open_read(&path).unwrap();
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, payload, "gzip peeled to the original image");

        // Seekable (random access into the decoded image).
        r.seek(SeekFrom::Start(100)).unwrap();
        let mut b = [0u8; 4];
        r.read_exact(&mut b).unwrap();
        assert_eq!(&b, &payload[100..104]);

        // A .adz without gzip magic is not treated as a gzip image.
        let bogus = dir.path().join("bogus.adz");
        std::fs::write(&bogus, b"NOTGZIP").unwrap();
        assert!(!is_gzip_image_path(&bogus));
    }

    #[test]
    fn open_read_decodes_gzip_dot_gz_pack() {
        // A `.pdi.gz` (gzip-wrapped Alto/Pilot pack) must peel like `.adz`/`.hdz`
        // so inspect + browse see the inner container magic instead of parsing
        // the raw gzip stream as a bogus MBR. `is_gzip_image_path` (the editable
        // adz/hdz set) stays false; `is_gzip_wrapped_path` and the container/peel
        // path pick it up.
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("CedarDorado-boot.pdi.gz");
        let mut payload = b"PARCDISK".to_vec();
        payload.extend((0..4096usize).map(|i| (i & 0xFF) as u8));
        {
            let f = File::create(&path).unwrap();
            let mut enc = GzEncoder::new(f, Compression::default());
            enc.write_all(&payload).unwrap();
            enc.finish().unwrap();
        }

        assert!(
            !is_gzip_image_path(&path),
            "a bare .gz is not the editable set"
        );
        assert!(is_gzip_wrapped_path(&path));
        assert!(is_container_path(&path), "peeled through open_read");

        let mut r = open_read(&path).unwrap();
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, payload, "gz peeled to the original pack");

        // Prefix-sniff helper used by the browse-session Alto branch.
        let head = gunzip_prefix(&path, 8).unwrap();
        assert_eq!(&head, b"PARCDISK");

        // A `.gz` without gzip magic is not treated as a wrapped image.
        let bogus = dir.path().join("notes.gz");
        std::fs::write(&bogus, b"NOTGZIP").unwrap();
        assert!(!is_gzip_wrapped_path(&bogus));
    }

    #[test]
    fn is_zip_image_path_requires_both_extension_and_magic() {
        let dir = tempfile::tempdir().unwrap();
        // Right magic, wrong extension (a non-disk ZIP stays unhandled).
        let other = dir.path().join("archive.tar");
        std::fs::write(&other, b"PK\x03\x04rest").unwrap();
        assert!(
            !is_zip_image_path(&other),
            "PK magic without .zip extension"
        );

        // Right extension, wrong magic.
        let wrong_magic = dir.path().join("fake.zip");
        std::fs::write(&wrong_magic, b"NOPE").unwrap();
        assert!(!is_zip_image_path(&wrong_magic), ".zip without PK magic");

        // Both.
        let real = dir.path().join("real.zip");
        std::fs::write(&real, b"PK\x03\x04rest").unwrap();
        assert!(is_zip_image_path(&real));
        assert!(is_container_path(&real));
        // And an .imz is NOT treated as a .zip disk (different opener).
        let imz = dir.path().join("disk.imz");
        std::fs::write(&imz, b"PK\x03\x04rest").unwrap();
        assert!(!is_zip_image_path(&imz));
    }

    /// A `.zip` holding a raw FAT superfloppy routes through `open_read`, which
    /// inflates the inner image so partition detection sees the real bytes
    /// (not the ZIP local-file header) — the same contract as the gzip path.
    #[test]
    fn open_read_routes_zip_disk_and_partition_detect_finds_fat() {
        use crate::partition::PartitionTable;
        use std::io::Write as _;
        use zip::write::SimpleFileOptions;

        let flat = crate::fs::fat::create_blank_fat(737280, Some("ZIPD")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backup.zip");
        {
            let f = File::create(&path).unwrap();
            let mut zw = zip::ZipWriter::new(f);
            zw.start_file(
                "backup.img",
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated),
            )
            .unwrap();
            zw.write_all(&flat).unwrap();
            zw.finish().unwrap();
        }

        assert!(is_zip_image_path(&path));
        assert!(is_container_path(&path));

        let mut reader = open_read(&path).unwrap();
        let table = PartitionTable::detect(&mut reader).unwrap();
        assert_eq!(table.type_name(), "None"); // superfloppy, no partition table
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "FAT");
        assert_eq!(parts[0].size_bytes, flat.len() as u64);

        // open_peeled_read (the CLI's resolve primitive) peels it identically.
        let mut peeled = open_peeled_read(&path, None).unwrap();
        let table2 = PartitionTable::detect(&mut peeled).unwrap();
        assert_eq!(table2.partitions()[0].type_name, "FAT");
    }

    /// A raw (non-container, non-wrapped) image passes through `open_peeled_read`
    /// byte-for-byte — the contract the CLI's raw path relies on.
    #[test]
    fn open_peeled_read_passes_raw_image_through() {
        let flat = crate::fs::fat::create_blank_fat(737280, Some("PEEL")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("disk.img");
        std::fs::write(&img, &flat).unwrap();

        let mut r = open_peeled_read(&img, None).unwrap();
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, flat, "raw image read back unchanged");

        // Seekable random access into the (un-peeled) image.
        r.seek(SeekFrom::Start(512)).unwrap();
        let mut b = [0u8; 4];
        r.read_exact(&mut b).unwrap();
        assert_eq!(&b, &flat[512..516]);
    }
}
