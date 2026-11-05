//! Microsoft VHD (Virtual PC / Hyper-V) format: footer, CHS geometry, the dynamic-disk header,
//! and [`DynamicVhdReader`] (read and in-place write).

#[cfg(feature = "rust173-polyfill")]
use disk_core::rust173::IntIsMultipleOf as _;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};

/// VHD cookie identifying a valid VHD footer.
pub const VHD_COOKIE: &[u8; 8] = b"conectix";

/// Build a 512-byte VHD (Fixed) footer for the given data size.
///
/// The footer follows the Microsoft VHD specification (v1.0):
/// - Cookie: `"conectix"`
/// - Features: `0x00000002` (reserved, must be set)
/// - File Format Version: `0x00010000` (1.0)
/// - Data Offset: `0xFFFFFFFFFFFFFFFF` (fixed disk, no dynamic header)
/// - Timestamp: seconds since 2000-01-01 00:00:00 UTC
/// - Creator Application: `"rsbk"` (Rusty Backup)
/// - Creator Version: `0x00010000`
/// - Creator Host OS: platform-dependent (`"Wi2k"` / `"Mac "`)
/// - Original / Current Size: the raw data size
/// - Disk Geometry: CHS per VHD spec algorithm
/// - Disk Type: `2` (Fixed)
/// - Unique ID: random 16 bytes
/// - Checksum: one's complement of the sum of all footer bytes (excluding checksum field)
pub fn build_vhd_footer(data_size: u64) -> [u8; 512] {
    // Fixed disk: disk type 2, no dynamic header (data offset = 0xFFFF…).
    build_vhd_footer_with_type(data_size, VHD_TYPE_FIXED, 0xFFFF_FFFF_FFFF_FFFF)
}

/// Build a 512-byte VHD footer, parameterized by disk type and data offset.
///
/// Fixed disks call this with `disk_type = VHD_TYPE_FIXED` and
/// `data_offset = 0xFFFFFFFFFFFFFFFF` (the [`build_vhd_footer`] default, kept
/// byte-identical for backward compatibility). Dynamic disks pass
/// `VHD_TYPE_DYNAMIC` and the byte offset of the dynamic disk header (512).
pub fn build_vhd_footer_with_type(data_size: u64, disk_type: u32, data_offset: u64) -> [u8; 512] {
    let mut footer = [0u8; 512];

    // Cookie (offset 0, 8 bytes)
    footer[0..8].copy_from_slice(VHD_COOKIE);

    // Features (offset 8, 4 bytes) — 0x00000002 = reserved bit
    footer[8..12].copy_from_slice(&0x0000_0002u32.to_be_bytes());

    // File Format Version (offset 12, 4 bytes) — 1.0
    footer[12..16].copy_from_slice(&0x0001_0000u32.to_be_bytes());

    // Data Offset (offset 16, 8 bytes) — 0xFFFFFFFFFFFFFFFF for fixed disks;
    // points at the dynamic disk header for dynamic/differencing disks.
    footer[16..24].copy_from_slice(&data_offset.to_be_bytes());

    // Timestamp (offset 24, 4 bytes) — seconds since 2000-01-01 00:00:00 UTC
    // VHD epoch: 2000-01-01 00:00:00 UTC = Unix timestamp 946684800
    let vhd_epoch: u64 = 946_684_800;
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let timestamp = now_unix.saturating_sub(vhd_epoch) as u32;
    footer[24..28].copy_from_slice(&timestamp.to_be_bytes());

    // Creator Application (offset 28, 4 bytes)
    footer[28..32].copy_from_slice(b"rsbk");

    // Creator Version (offset 32, 4 bytes) — 1.0
    footer[32..36].copy_from_slice(&0x0001_0000u32.to_be_bytes());

    // Creator Host OS (offset 36, 4 bytes)
    #[cfg(target_os = "windows")]
    footer[36..40].copy_from_slice(b"Wi2k");
    #[cfg(target_os = "macos")]
    footer[36..40].copy_from_slice(b"Mac ");
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    footer[36..40].copy_from_slice(b"Wi2k"); // Linux — use Windows ID (most compatible)

    // Original Size (offset 40, 8 bytes)
    footer[40..48].copy_from_slice(&data_size.to_be_bytes());

    // Current Size (offset 48, 8 bytes)
    footer[48..56].copy_from_slice(&data_size.to_be_bytes());

    // Disk Geometry (offset 56, 4 bytes): CHS packed as C(16) H(8) S(8)
    let (cylinders, heads, sectors_per_track) = vhd_chs_geometry(data_size);
    footer[56..58].copy_from_slice(&(cylinders as u16).to_be_bytes());
    footer[58] = heads as u8;
    footer[59] = sectors_per_track as u8;

    // Disk Type (offset 60, 4 bytes) — 2 = Fixed, 3 = Dynamic, 4 = Differencing
    footer[60..64].copy_from_slice(&disk_type.to_be_bytes());

    // Checksum (offset 64, 4 bytes) — computed after UUID
    // UUID (offset 68, 16 bytes) — random
    let uuid = random_uuid();
    footer[68..84].copy_from_slice(&uuid);

    // Saved State (offset 84, 1 byte) — 0
    // Reserved (offset 85..512) — already zero

    // Checksum: one's complement of the sum of all bytes with the checksum
    // field left zero (it still is here, so sum over the whole footer).
    let checksum = vhd_checksum(&footer);
    footer[64..68].copy_from_slice(&checksum.to_be_bytes());

    footer
}

/// VHD structure checksum: one's complement of the byte-wise sum of `bytes`.
///
/// Shared by the footer and the dynamic disk header. The caller must zero the
/// 4-byte checksum field before calling so it does not contribute to the sum.
pub fn vhd_checksum(bytes: &[u8]) -> u32 {
    let sum = bytes
        .iter()
        .fold(0u32, |acc, &b| acc.wrapping_add(b as u32));
    !sum
}

/// Compute VHD CHS geometry from total disk size (in bytes) per the VHD spec.
///
/// This follows the algorithm from the Microsoft VHD specification appendix.
pub fn vhd_chs_geometry(size_bytes: u64) -> (u32, u32, u32) {
    let total_sectors = (size_bytes / 512).min(65535 * 16 * 255) as u32;

    if total_sectors == 0 {
        return (0, 0, 0);
    }

    if total_sectors >= 65535 * 16 * 63 {
        // Maximum geometry
        let spt = 255u32;
        let heads = 16u32;
        let cylinders = total_sectors / (heads * spt);
        return (cylinders, heads, spt);
    }

    let mut spt = 17u32;
    let mut cyl_times_heads = total_sectors / spt;
    let mut heads = cyl_times_heads.div_ceil(1024);

    if heads < 4 {
        heads = 4;
    }

    if cyl_times_heads >= heads * 1024 || heads > 16 {
        spt = 31;
        heads = 16;
        cyl_times_heads = total_sectors / spt;
    }

    if cyl_times_heads >= heads * 1024 {
        spt = 63;
        heads = 16;
        cyl_times_heads = total_sectors / spt;
    }

    let cylinders = cyl_times_heads / heads;
    (cylinders, heads, spt)
}

/// Generate 16 random bytes for the VHD UUID field.
fn random_uuid() -> [u8; 16] {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut bytes = [0u8; 16];
    // Mix multiple entropy sources
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let mut hasher = DefaultHasher::new();
    now.as_nanos().hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    let h1 = hasher.finish();

    let mut hasher2 = DefaultHasher::new();
    (now.as_nanos() ^ 0xDEAD_BEEF_CAFE_BABE).hash(&mut hasher2);
    std::thread::current().id().hash(&mut hasher2);
    let h2 = hasher2.finish();

    bytes[0..8].copy_from_slice(&h1.to_le_bytes());
    bytes[8..16].copy_from_slice(&h2.to_le_bytes());
    bytes
}

/// Cookie identifying the VHD Dynamic Disk Header (`"cxsparse"`).
pub const VHD_DYNAMIC_COOKIE: &[u8; 8] = b"cxsparse";

/// VHD disk-type values (footer offset 60, big-endian).
pub const VHD_TYPE_FIXED: u32 = 2;
pub const VHD_TYPE_DYNAMIC: u32 = 3;
pub const VHD_TYPE_DIFFERENCING: u32 = 4;

/// Sentinel BAT entry marking an unallocated block (reads as zeros).
pub const BAT_UNUSED: u32 = 0xFFFF_FFFF;

/// Reader over a *dynamic* (sparse) VHD that presents the logical disk as a
/// flat, seekable byte stream. Unallocated blocks read as zeros.
///
/// Layout (all multi-byte fields big-endian):
/// - footer copy (512 B) at offset 0, canonical footer (512 B) at EOF-512
/// - Dynamic Disk Header (1024 B) at the footer's `data_offset`
/// - BAT: `max_table_entries` × u32 (each = sector offset of a block's bitmap,
///   or `0xFFFFFFFF` if unallocated)
/// - each block = a sector bitmap (padded to 512 B) followed by `block_size`
///   data bytes
pub struct DynamicVhdReader<R: Read + Seek> {
    inner: R,
    /// Logical disk size in bytes (footer "current size", offset 48).
    disk_size: u64,
    /// Bytes of data per block (dynamic header offset 32).
    block_size: u64,
    /// Size of the per-block sector bitmap, padded to a 512-byte boundary.
    bitmap_size: u64,
    /// Block Allocation Table: one entry per block, sector offset or `BAT_UNUSED`.
    bat: Vec<u32>,
    /// Host byte offset of the BAT — needed to patch individual entries during
    /// allocate-on-write without re-emitting the whole table.
    table_offset: u64,
    /// Host byte offset where the trailing footer currently starts. New blocks
    /// get appended here (overwriting the footer); the footer is then
    /// re-emitted at the new end-of-file.
    host_end: u64,
    /// Cached trailing-footer bytes so allocate-on-write re-appends the exact
    /// original footer (preserving UUID, timestamp, creator, etc.).
    footer: [u8; 512],
    /// Current logical read/write position.
    pos: u64,
}

impl<R: Read + Seek> DynamicVhdReader<R> {
    /// The underlying image, for callers that need the backing bytes back.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read + Seek> DynamicVhdReader<R> {
    /// Parse a dynamic VHD from `inner`. Fails if the footer is missing, the
    /// disk type is not dynamic (3), or the dynamic header is malformed.
    /// Differencing disks (type 4) are explicitly rejected.
    pub fn open(mut inner: R) -> Result<Self> {
        let total_len = inner.seek(SeekFrom::End(0))?;
        if total_len < 512 + 1024 {
            bail!("file too small to be a dynamic VHD");
        }

        // Canonical footer at EOF-512.
        inner.seek(SeekFrom::End(-512))?;
        let mut footer = [0u8; 512];
        inner.read_exact(&mut footer)?;
        if &footer[0..8] != VHD_COOKIE {
            bail!("missing VHD footer cookie");
        }
        let disk_type = u32::from_be_bytes(footer[60..64].try_into().unwrap());
        match disk_type {
            VHD_TYPE_DYNAMIC => {}
            VHD_TYPE_DIFFERENCING => bail!("differencing VHDs are not supported"),
            other => bail!("not a dynamic VHD (disk type {other})"),
        }
        let disk_size = u64::from_be_bytes(footer[48..56].try_into().unwrap());
        let header_offset = u64::from_be_bytes(footer[16..24].try_into().unwrap());
        if header_offset == u64::MAX || header_offset + 1024 > total_len {
            bail!("invalid dynamic header offset");
        }

        // Dynamic Disk Header.
        inner.seek(SeekFrom::Start(header_offset))?;
        let mut header = [0u8; 1024];
        inner.read_exact(&mut header)?;
        if &header[0..8] != VHD_DYNAMIC_COOKIE {
            bail!("missing dynamic header cookie");
        }
        let table_offset = u64::from_be_bytes(header[16..24].try_into().unwrap());
        let max_table_entries = u32::from_be_bytes(header[28..32].try_into().unwrap()) as usize;
        let block_size = u32::from_be_bytes(header[32..36].try_into().unwrap()) as u64;
        if block_size == 0 || !block_size.is_multiple_of(512) {
            bail!("invalid dynamic VHD block size {block_size}");
        }
        if table_offset + (max_table_entries as u64) * 4 > total_len {
            bail!("BAT extends past end of file");
        }

        // BAT: one big-endian u32 per block.
        inner.seek(SeekFrom::Start(table_offset))?;
        let mut bat_bytes = vec![0u8; max_table_entries * 4];
        inner.read_exact(&mut bat_bytes)?;
        let bat: Vec<u32> = bat_bytes
            .chunks_exact(4)
            .map(|c| u32::from_be_bytes(c.try_into().unwrap()))
            .collect();

        // Sector bitmap precedes each block, padded up to a 512-byte boundary.
        let sectors_per_block = block_size / 512;
        let bitmap_size = sectors_per_block.div_ceil(8).div_ceil(512) * 512;

        Ok(Self {
            inner,
            disk_size,
            block_size,
            bitmap_size,
            bat,
            table_offset,
            host_end: total_len - 512,
            footer,
            pos: 0,
        })
    }

    /// Logical disk size in bytes.
    pub fn len(&self) -> u64 {
        self.disk_size
    }

    /// True if the logical disk is empty.
    pub fn is_empty(&self) -> bool {
        self.disk_size == 0
    }
}

impl<R: Read + Seek> Read for DynamicVhdReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.disk_size || buf.is_empty() {
            return Ok(0);
        }
        let block_index = (self.pos / self.block_size) as usize;
        let intra = self.pos % self.block_size;
        // Never cross a block boundary (or the end of the disk) in one read.
        let remaining_in_block = self.block_size - intra;
        let remaining_in_disk = self.disk_size - self.pos;
        let to_read = (buf.len() as u64)
            .min(remaining_in_block)
            .min(remaining_in_disk) as usize;

        let entry = self.bat.get(block_index).copied().unwrap_or(BAT_UNUSED);
        if entry == BAT_UNUSED {
            // Unallocated → zeros.
            for b in buf[..to_read].iter_mut() {
                *b = 0;
            }
        } else {
            let host = entry as u64 * 512 + self.bitmap_size + intra;
            self.inner.seek(SeekFrom::Start(host))?;
            self.inner.read_exact(&mut buf[..to_read])?;
        }
        self.pos += to_read as u64;
        Ok(to_read)
    }
}

impl<R: Read + Seek> Seek for DynamicVhdReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(n) => self.disk_size as i64 + n,
            SeekFrom::Current(n) => self.pos as i64 + n,
        };
        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek before start of dynamic VHD",
            ));
        }
        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

/// Allocate-on-write `Write` for in-place editing of dynamic VHDs.
///
/// Writes that land in an already-allocated block update in place. Writes
/// that land in an unallocated block first append a fresh block to the file
/// (overwriting the trailing footer, growing the file, patching the BAT,
/// then re-emitting the cached footer at the new end-of-file). The bitmap
/// of any newly-allocated block is set all-ones — we always mark every
/// sector as written even when the caller only touches part of the block,
/// which is the conservative interpretation Microsoft's spec endorses
/// ("If the bitmap is unused, all data is valid").
///
/// Like [`Read`], one call never crosses a block boundary; the caller (or
/// `write_all`) loops to cover larger writes.
impl<R: Read + Write + Seek> Write for DynamicVhdReader<R> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.pos >= self.disk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write past end of dynamic VHD",
            ));
        }
        let block_index = (self.pos / self.block_size) as usize;
        let intra = self.pos % self.block_size;
        let remaining_in_block = self.block_size - intra;
        let remaining_in_disk = self.disk_size - self.pos;
        let to_write = (buf.len() as u64)
            .min(remaining_in_block)
            .min(remaining_in_disk) as usize;

        if self.bat[block_index] == BAT_UNUSED {
            self.allocate_block(block_index)?;
        }

        let entry = self.bat[block_index];
        let host = entry as u64 * 512 + self.bitmap_size + intra;
        self.inner.seek(SeekFrom::Start(host))?;
        self.inner.write_all(&buf[..to_write])?;
        self.pos += to_write as u64;
        Ok(to_write)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<R: Read + Write + Seek> DynamicVhdReader<R> {
    /// Allocate a fresh block for `block_index`: append (bitmap + zero-data)
    /// at the current `host_end`, patch the BAT entry on disk + in memory,
    /// re-emit the trailing footer at the new EOF.
    fn allocate_block(&mut self, block_index: usize) -> std::io::Result<()> {
        let new_host = self.host_end;
        // Overwrite the old footer with the new block's bitmap.
        self.inner.seek(SeekFrom::Start(new_host))?;
        let bitmap = vec![0xFFu8; self.bitmap_size as usize];
        self.inner.write_all(&bitmap)?;
        // Then a fully-zero block; subsequent partial writes update the parts
        // that the caller actually wants to change.
        let zeros = vec![0u8; self.block_size as usize];
        self.inner.write_all(&zeros)?;

        // Patch the BAT (on disk and in memory).
        let new_entry = (new_host / 512) as u32;
        self.bat[block_index] = new_entry;
        self.inner
            .seek(SeekFrom::Start(self.table_offset + block_index as u64 * 4))?;
        self.inner.write_all(&new_entry.to_be_bytes())?;

        // Re-emit the footer at the new end-of-file.
        self.host_end += self.bitmap_size + self.block_size;
        self.inner.seek(SeekFrom::Start(self.host_end))?;
        self.inner.write_all(&self.footer)?;
        Ok(())
    }
}

/// Default dynamic-VHD block size: 2 MiB (the value `qemu-img`, Hyper-V, and
/// Disk2vhd all emit).
pub const VHD_DEFAULT_BLOCK_SIZE: u32 = 0x0020_0000;

/// Build the 1024-byte VHD Dynamic Disk Header (`cxsparse`).
///
/// All fields big-endian. The dynamic header sits at byte offset 512 (right
/// after the leading footer copy); the BAT it points at via `table_offset`
/// follows at 1536.
pub fn build_vhd_dynamic_header(
    table_offset: u64,
    max_table_entries: u32,
    block_size: u32,
) -> [u8; 1024] {
    let mut h = [0u8; 1024];
    // Cookie (0, 8)
    h[0..8].copy_from_slice(VHD_DYNAMIC_COOKIE);
    // Data offset (8, 8) — none for non-differencing disks.
    h[8..16].copy_from_slice(&0xFFFF_FFFF_FFFF_FFFFu64.to_be_bytes());
    // Table (BAT) offset (16, 8)
    h[16..24].copy_from_slice(&table_offset.to_be_bytes());
    // Header version (24, 4) — 1.0
    h[24..28].copy_from_slice(&0x0001_0000u32.to_be_bytes());
    // Max table entries (28, 4)
    h[28..32].copy_from_slice(&max_table_entries.to_be_bytes());
    // Block size (32, 4)
    h[32..36].copy_from_slice(&block_size.to_be_bytes());
    // Checksum (36, 4) — over the whole header with this field zeroed (it is).
    // Parent UUID / timestamp / name / locators (40..1024) — all zero.
    let checksum = vhd_checksum(&h);
    h[36..40].copy_from_slice(&checksum.to_be_bytes());
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_vhd_footer_cookie_and_checksum() {
        let footer = build_vhd_footer(1024 * 1024); // 1 MB
        assert_eq!(&footer[0..8], b"conectix");

        // Verify checksum is valid
        let stored = u32::from_be_bytes(footer[64..68].try_into().unwrap());
        let mut sum: u32 = 0;
        for (i, &b) in footer.iter().enumerate() {
            if (64..68).contains(&i) {
                continue;
            }
            sum = sum.wrapping_add(b as u32);
        }
        assert_eq!(stored, !sum);
    }

    #[test]
    fn test_build_vhd_footer_fixed_unchanged() {
        // The refactored build_vhd_footer must produce structurally identical
        // results to build_vhd_footer_with_type(…, FIXED, 0xFF…). UUID and
        // timestamp differ between calls, so compare everything except those.
        let f1 = build_vhd_footer(1024 * 1024);
        let f2 = build_vhd_footer_with_type(1024 * 1024, VHD_TYPE_FIXED, u64::MAX);
        // Cookie, features, version, data_offset, creator, host, sizes, geom, disk_type.
        assert_eq!(&f1[0..24], &f2[0..24], "header fields");
        assert_eq!(&f1[28..64], &f2[28..64], "creator through disk_type");
        // Disk type must be 2 (Fixed).
        assert_eq!(
            u32::from_be_bytes(f1[60..64].try_into().unwrap()),
            VHD_TYPE_FIXED
        );
        assert_eq!(
            u32::from_be_bytes(f2[60..64].try_into().unwrap()),
            VHD_TYPE_FIXED
        );
        // Data offset must be 0xFFFF… (no dynamic header).
        assert_eq!(u64::from_be_bytes(f1[16..24].try_into().unwrap()), u64::MAX);
    }

    #[test]
    fn test_dynamic_vhd_reader_rejects_fixed() {
        // A fixed VHD is just data + footer; open() must reject it.
        let footer = build_vhd_footer(1024); // disk type 2
        let mut image = vec![0u8; 1024];
        image.extend_from_slice(&footer);
        let err = match DynamicVhdReader::open(std::io::Cursor::new(image)) {
            Ok(_) => panic!("fixed VHD should be rejected"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("not a dynamic VHD"));
    }

    #[test]
    fn test_vhd_checksum_helper() {
        let data = [0u8; 512];
        assert_eq!(vhd_checksum(&data), !0u32);
        let data2 = [1u8; 4];
        assert_eq!(vhd_checksum(&data2), !4u32);
    }

    #[test]
    fn test_vhd_chs_geometry() {
        // Small disk: 100 MB
        let (c, h, s) = vhd_chs_geometry(100 * 1024 * 1024);
        assert!(c > 0 && h > 0 && s > 0);

        // Large disk: 8 GB
        let (c, h, s) = vhd_chs_geometry(8 * 1024 * 1024 * 1024);
        assert!(c > 0 && h > 0 && s > 0);

        // Zero size
        let (c, h, s) = vhd_chs_geometry(0);
        assert_eq!((c, h, s), (0, 0, 0));
    }
}
