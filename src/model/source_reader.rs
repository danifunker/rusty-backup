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

use crate::rbformats::chd::ChdReader;
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
use crate::rbformats::ReadSeek;

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
    open_read_with_password(path, None)
}

/// Variant of [`open_read`] that accepts an optional password for
/// container formats that support encryption (IMZ and GHO; CHD's scheme is
/// not wired here). Pass `None` to behave like [`open_read`].
pub fn open_read_with_password(path: &Path, password: Option<&[u8]>) -> Result<Box<dyn ReadSeek>> {
    if is_chd_path(path) {
        let chd = ChdReader::open(path).with_context(|| format!("open CHD {}", path.display()))?;
        Ok(Box::new(chd))
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
    } else {
        let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
        Ok(Box::new(BufReader::new(f)))
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
        || is_dim_path(path)
        || is_xdf_path(path)
        || is_hdm_path(path)
        || is_arculator_hdf_path(path)
        || is_apple_ii_dsk_path(path)
}

/// The four floppy containers that support in-place editing via
/// [`crate::model::container_edit`] (decode -> edit -> re-encode): XDF, HDM,
/// DIM, D88. Subset of [`is_flat_floppy_container_path`] excluding the
/// read-only-wrapped formats (MSA / EDSK / Apple-II / Arculator HDF) which
/// have no re-encoder wired up — `containers::is_floppy_container` is the
/// matching engine-side predicate.
pub fn is_editable_container_path(path: &Path) -> bool {
    is_xdf_path(path) || is_hdm_path(path) || is_dim_path(path) || is_d88_path(path)
}

/// True when [`open_read`] would transparently unwrap `path` into a decoded
/// flat-sector stream — any CHD / GHO / IMZ streaming reader or flat floppy
/// container. Callers that build their own reader use this to decide whether
/// to route through `open_read` instead of opening the file raw, so the
/// "what counts as a container" list lives in one place instead of being
/// re-listed (and drifting) at every call site.
pub fn is_container_path(path: &Path) -> bool {
    is_chd_path(path)
        || is_gho_path(path)
        || is_imz_path(path)
        || is_flat_floppy_container_path(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

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
}
