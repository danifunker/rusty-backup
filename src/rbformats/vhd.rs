use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use super::{decompress_to_writer, reconstruct_disk_from_backup, write_zeros, CHUNK_SIZE};
use crate::backup::metadata::BackupMetadata;
use crate::fs::{patch_hidden_sectors_for, resize_filesystem_for};
use crate::partition::mbr::{
    build_ebr_chain, parse_ebr_chain, patch_mbr_entries, LogicalPartitionInfo, Mbr,
};
use crate::partition::PartitionSizeOverride;

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
pub(crate) fn vhd_checksum(bytes: &[u8]) -> u32 {
    let sum = bytes
        .iter()
        .fold(0u32, |acc, &b| acc.wrapping_add(b as u32));
    !sum
}

/// Compute VHD CHS geometry from total disk size (in bytes) per the VHD spec.
///
/// This follows the algorithm from the Microsoft VHD specification appendix.
pub(crate) fn vhd_chs_geometry(size_bytes: u64) -> (u32, u32, u32) {
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
const BAT_UNUSED: u32 = 0xFFFF_FFFF;

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
pub(crate) fn build_vhd_dynamic_header(
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

/// Stream a `disk_size`-byte source into a *dynamic* (sparse) VHD on `out`.
///
/// Blocks whose source bytes are entirely zero are left unallocated (their BAT
/// entry stays `0xFFFFFFFF`, read back as zeros); non-zero blocks are appended
/// with an all-ones sector bitmap. Layout written:
///
/// ```text
/// +0      footer copy (512)
/// +512    dynamic disk header (1024)
/// +1536   BAT: max_entries * u32 BE, zero-padded to 512
/// ...     appended blocks (each = sector bitmap + block_size data)
/// EOF-512 footer
/// ```
///
/// `block_size` of 0 selects [`VHD_DEFAULT_BLOCK_SIZE`]; otherwise it must be a
/// non-zero multiple of 512. The BAT is held in RAM (a 2 TiB disk at 2 MiB
/// blocks is 4 MiB of table) and back-patched after the data is streamed.
pub fn export_whole_disk_vhd_dynamic(
    reader: &mut impl Read,
    out: &mut (impl Write + Seek),
    disk_size: u64,
    block_size: u32,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
) -> Result<()> {
    use super::sparse::{is_zero_unit, SparseAllocator};

    let block_size = if block_size == 0 {
        VHD_DEFAULT_BLOCK_SIZE
    } else {
        block_size
    };
    if !(block_size as u64).is_multiple_of(512) {
        bail!("dynamic VHD block size {block_size} is not a multiple of 512");
    }
    let bs = block_size as u64;

    let max_entries = disk_size.div_ceil(bs);
    let bat_bytes = max_entries * 4;
    let bat_padded = bat_bytes.div_ceil(512) * 512;

    let header_offset: u64 = 512;
    let table_offset: u64 = 512 + 1024;
    let data_start = table_offset + bat_padded;

    let sectors_per_block = bs / 512;
    let bitmap_size = sectors_per_block.div_ceil(8).div_ceil(512) * 512;
    let unit_size = bitmap_size + bs;

    // Header region: footer copy, dynamic header, zero-filled BAT placeholder.
    out.seek(SeekFrom::Start(0))?;
    let footer = build_vhd_footer_with_type(disk_size, VHD_TYPE_DYNAMIC, header_offset);
    out.write_all(&footer)
        .context("failed to write dynamic VHD footer copy")?;
    let header = build_vhd_dynamic_header(table_offset, max_entries as u32, block_size);
    out.write_all(&header)
        .context("failed to write dynamic disk header")?;
    super::write_zeros(out, bat_padded).context("failed to write BAT placeholder")?;

    // Stream blocks, allocating only the non-zero ones.
    let mut allocator = SparseAllocator::new(data_start, unit_size);
    let mut bat = vec![BAT_UNUSED; max_entries as usize];
    let bitmap = vec![0xFFu8; bitmap_size as usize];
    let mut block_buf = vec![0u8; bs as usize];

    for (i, slot) in bat.iter_mut().enumerate() {
        if cancel_check() {
            bail!("dynamic VHD export cancelled");
        }
        let block_off = i as u64 * bs;
        let this_block = (disk_size - block_off).min(bs) as usize;
        reader
            .read_exact(&mut block_buf[..this_block])
            .context("failed to read source block")?;
        // Zero any tail past the source's last partial block so the padding
        // (and the zero-skip test) are deterministic.
        for b in block_buf[this_block..].iter_mut() {
            *b = 0;
        }

        if !is_zero_unit(&block_buf[..this_block]) {
            let off = allocator.alloc();
            *slot = (off / 512) as u32;
            out.write_all(&bitmap)
                .context("failed to write block bitmap")?;
            out.write_all(&block_buf)
                .context("failed to write block data")?;
        }
        progress_cb(block_off + this_block as u64);
    }

    // Trailing footer after the last allocated block (or right after the BAT
    // placeholder if every block was sparse).
    out.seek(SeekFrom::Start(allocator.next_offset()))?;
    out.write_all(&footer)
        .context("failed to write trailing dynamic VHD footer")?;

    // Back-patch the BAT.
    out.seek(SeekFrom::Start(table_offset))?;
    let mut bat_bytes_buf = Vec::with_capacity(bat.len() * 4);
    for entry in &bat {
        bat_bytes_buf.extend_from_slice(&entry.to_be_bytes());
    }
    out.write_all(&bat_bytes_buf)
        .context("failed to write BAT")?;
    out.flush().context("failed to flush dynamic VHD")?;

    Ok(())
}

/// Write VHD (Fixed): stream raw data to a single file, then append a
/// 512-byte footer. `output_hasher`, when provided, is updated with
/// every byte that lands in the file (raw bytes + footer) so the
/// caller can use the finalised digest as the `.vhd` file's checksum
/// without a post-write read pass.
pub(crate) fn write_vhd(
    reader: &mut impl Read,
    output_base: &Path,
    skip_zeros: bool,
    output_hasher: Option<super::compress::OutputHasherHandle>,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
) -> Result<Vec<String>> {
    use super::raw::stream_with_split;

    // Write raw data (no splitting for VHD). The hasher receives the
    // raw body via stream_with_split's internal tee; we feed it the
    // footer below.
    let files = stream_with_split(
        reader,
        output_base,
        "vhd",
        None,
        skip_zeros,
        output_hasher.clone(),
        progress_cb,
        cancel_check,
    )?;

    // Re-open the file to determine data size and append the VHD footer
    let vhd_path = output_base
        .parent()
        .unwrap_or(Path::new("."))
        .join(&files[0]);

    let data_size = fs::metadata(&vhd_path)
        .with_context(|| format!("failed to stat VHD file: {}", vhd_path.display()))?
        .len();

    let footer = build_vhd_footer(data_size);

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&vhd_path)
        .with_context(|| format!("failed to reopen VHD file: {}", vhd_path.display()))?;
    file.write_all(&footer)
        .context("failed to write VHD footer")?;
    file.flush()?;

    // Footer is part of the on-disk file, so feed it to the hasher too.
    if let Some(h) = output_hasher.as_ref() {
        if let Ok(mut guard) = h.lock() {
            if let Some(running) = guard.as_mut() {
                running.update(&footer);
            }
        }
    }

    Ok(files)
}

/// Result of rebuilding the VHD EBR chain with repacked logical partitions.
struct VhdEbrResult {
    /// (byte_offset, 512-byte EBR sector) pairs.
    ebr_sectors: Vec<(u64, [u8; 512])>,
    /// Map from original start_lba to new start_lba for repacked logicals.
    logical_remap: Vec<(u64, u64)>,
    /// New total_sectors for the MBR extended container entry.
    new_extended_total_sectors: u32,
    /// The extended container start LBA.
    extended_start_lba: u32,
}

/// Parse the source MBR and EBR chain, then build an updated EBR chain where
/// logical partition sizes are replaced by the new sizes from `partition_sizes`.
/// When any logical partition is resized, repacks them contiguously.
///
/// Returns `None` if the source has no extended partition or its EBR cannot be
/// parsed (non-fatal: the export continues without updated EBR in that case).
fn rebuild_vhd_ebr_chain(
    source_mbr: &[u8; 512],
    source: &mut (impl Read + Seek),
    partition_sizes: &[PartitionSizeOverride],
    log_cb: &mut impl FnMut(&str),
) -> Option<VhdEbrResult> {
    // Find the extended container entry in the source MBR.
    let mbr = Mbr::parse(source_mbr).ok()?;
    let extended_entry = mbr
        .entries
        .iter()
        .find(|e| e.is_extended() && !e.is_empty())?;
    let extended_start_lba = extended_entry.start_lba;

    // Parse the source EBR chain to get current logical partition info
    // (positions and partition types).
    let source_logicals = match parse_ebr_chain(source, extended_start_lba) {
        Ok(v) if !v.is_empty() => v,
        Ok(_) => return None, // empty chain — nothing to update
        Err(e) => {
            log_cb(&format!("Warning: could not parse source EBR chain: {e}"));
            return None;
        }
    };

    // Check if any logical partition is resized
    let any_resized = source_logicals.iter().any(|src| {
        partition_sizes
            .iter()
            .find(|ps| ps.start_lba == src.start_lba as u64)
            .map(|ps| ps.export_size != src.total_sectors as u64 * 512)
            .unwrap_or(false)
    });

    // Build updated LogicalPartitionInfo entries.  When any logical is resized,
    // repack them contiguously (first keeps position, rest packed with 1-sector
    // EBR gap) — same algorithm as build_restore_ebr_chain in restore/mod.rs.
    let mut logical_infos: Vec<LogicalPartitionInfo> = Vec::with_capacity(source_logicals.len());
    let mut logical_remap: Vec<(u64, u64)> = Vec::with_capacity(source_logicals.len());
    let mut next_lba: u32 = 0;

    for (i, src) in source_logicals.iter().enumerate() {
        let new_sectors = partition_sizes
            .iter()
            .find(|ps| ps.start_lba == src.start_lba as u64)
            .map(|ps| (ps.export_size / 512) as u32)
            .unwrap_or(src.total_sectors);

        let start_lba = if i == 0 {
            // First logical: keep original position
            src.start_lba
        } else if any_resized {
            // Pack right after previous partition, leaving 1 sector for EBR
            next_lba + 1
        } else {
            src.start_lba
        };

        next_lba = start_lba + new_sectors;

        if start_lba != src.start_lba {
            logical_remap.push((src.start_lba as u64, start_lba as u64));
        }

        logical_infos.push(LogicalPartitionInfo {
            start_lba,
            total_sectors: new_sectors,
            partition_type: src.partition_type,
        });
    }

    let last_end_lba = next_lba;
    let new_extended_total_sectors = last_end_lba - extended_start_lba;

    let ebr_sectors = build_ebr_chain(extended_start_lba, &logical_infos);
    log_cb(&format!(
        "Rebuilt {} EBR sector(s) for logical partitions (extended container at LBA {}, new total_sectors {})",
        ebr_sectors.len(),
        extended_start_lba,
        new_extended_total_sectors,
    ));
    Some(VhdEbrResult {
        ebr_sectors,
        logical_remap,
        new_extended_total_sectors,
        extended_start_lba,
    })
}

/// Export a whole disk image as a Fixed VHD file.
///
/// For raw image files or devices: reconstructs the disk with partition size overrides.
/// For backup folders: reconstructs the disk from MBR + partition data files.
///
/// `source` is either a raw image/device path or a backup folder.
/// When `backup_metadata` is `Some`, the source is treated as a backup folder.
/// `partition_sizes` provides per-partition size overrides.
pub fn export_whole_disk_vhd(
    source_path: &Path,
    backup_metadata: Option<&BackupMetadata>,
    mbr_bytes: Option<&[u8; 512]>,
    partition_sizes: &[PartitionSizeOverride],
    dest_path: &Path,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    let mut total_written: u64 = 0;

    if let Some(meta) = backup_metadata {
        // Backup folder reconstruction — use File directly (not BufWriter)
        // because reconstruct_disk_from_backup needs Read + Write + Seek.
        // Must open with read+write (not O_WRONLY) because patch_hidden_sectors_for
        // and similar functions read back from the writer to detect filesystem type.
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?;

        total_written = reconstruct_disk_from_backup(
            source_path,
            meta,
            mbr_bytes,
            partition_sizes,
            meta.source_size_bytes,
            &mut file,
            false, // VHD export is to a file
            false, // No need to write zeros for VHD files
            None,  // VHD export doesn't write GPT structures
            None,  // VHD export doesn't write APM structures
            &mut progress_cb,
            &cancel_check,
            &mut log_cb,
        )?;

        // Append VHD footer
        let footer = build_vhd_footer(total_written);
        file.write_all(&footer)
            .context("failed to write VHD footer")?;
        file.flush()?;

        log_cb(&format!(
            "VHD export complete: {} ({} data bytes + 512 byte footer)",
            dest_path.display(),
            total_written,
        ));

        return Ok(());
    }

    // Raw image/device path. If the source has an APM partition table and
    // there are size overrides, reconstruct with patched APM + per-partition
    // resize; then append the VHD footer. Otherwise fall back to the MBR /
    // straight-stream path using BufWriter.
    if !partition_sizes.is_empty() {
        let mut probe_reader = BufReader::new(
            File::open(source_path)
                .with_context(|| format!("failed to open {}", source_path.display()))?,
        );
        let is_apm = super::detect_raw_apm(&mut probe_reader).is_some();
        let is_rdb = if is_apm {
            false
        } else {
            // Re-probe — detect_raw_apm consumed/seeked the reader.
            let mut probe2 = BufReader::new(
                File::open(source_path)
                    .with_context(|| format!("failed to open {}", source_path.display()))?,
            );
            super::detect_raw_rdb(&mut probe2).is_some()
        };
        if is_apm || is_rdb {
            let file_size = std::fs::metadata(source_path)?.len();
            // Strip trailing VHD footer if source is itself a VHD file.
            let source_data_size = {
                let mut f = File::open(source_path)?;
                if file_size >= 512 {
                    f.seek(SeekFrom::End(-512))?;
                    let mut cookie = [0u8; 8];
                    f.read_exact(&mut cookie)?;
                    if &cookie == VHD_COOKIE {
                        file_size - 512
                    } else {
                        file_size
                    }
                } else {
                    file_size
                }
            };

            let mut reader = BufReader::new(File::open(source_path)?);
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(dest_path)
                .with_context(|| format!("failed to create {}", dest_path.display()))?;

            total_written = if is_apm {
                super::reconstruct_raw_apm_disk(
                    &mut reader,
                    source_data_size,
                    &mut file,
                    partition_sizes,
                    &mut progress_cb,
                    &cancel_check,
                    &mut log_cb,
                )?
            } else {
                super::reconstruct_raw_rdb_disk(
                    &mut reader,
                    source_data_size,
                    &mut file,
                    partition_sizes,
                    Some(source_path),
                    &mut progress_cb,
                    &cancel_check,
                    &mut log_cb,
                )?
            };

            let footer = build_vhd_footer(total_written);
            file.write_all(&footer)
                .context("failed to write VHD footer")?;
            file.flush()?;

            let table_kind = if is_apm { "APM" } else { "RDB" };
            log_cb(&format!(
                "VHD export complete: {} ({} data bytes + 512 byte footer, {} reconstructed)",
                dest_path.display(),
                total_written,
                table_kind,
            ));
            return Ok(());
        }
    }

    // Raw image/device path — use BufWriter for streaming
    let mut writer = BufWriter::new(
        File::create(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?,
    );

    // Raw image/device: reconstruct with partition size overrides
    {
        let file = File::open(source_path)
            .with_context(|| format!("failed to open {}", source_path.display()))?;
        let file_size = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        // Check if this is a VHD file — if so, limit to data portion
        let source_data_size = if file_size >= 512 {
            let mut f = File::open(source_path)?;
            f.seek(SeekFrom::End(-512))?;
            let mut cookie = [0u8; 8];
            f.read_exact(&mut cookie)?;
            if &cookie == VHD_COOKIE {
                file_size - 512
            } else {
                file_size
            }
        } else {
            file_size
        };

        if partition_sizes.is_empty() {
            // No size overrides — stream the whole source
            let mut buf = vec![0u8; CHUNK_SIZE];
            let mut limited = (&mut reader).take(source_data_size);
            loop {
                if cancel_check() {
                    bail!("export cancelled");
                }
                let n = limited.read(&mut buf).context("failed to read source")?;
                if n == 0 {
                    break;
                }
                writer
                    .write_all(&buf[..n])
                    .context("failed to write VHD data")?;
                total_written += n as u64;
                progress_cb(total_written);
            }
        } else {
            // Reconstruct disk with partition size overrides
            let mut buf = vec![0u8; CHUNK_SIZE];

            // Read and patch MBR (first 512 bytes)
            let mut mbr_buf = [0u8; 512];
            reader
                .read_exact(&mut mbr_buf)
                .context("failed to read MBR from source")?;
            patch_mbr_entries(&mut mbr_buf, partition_sizes);

            // Build repacked EBR chain before writing so we know new positions.
            let ebr_result =
                rebuild_vhd_ebr_chain(&mbr_buf, &mut reader, partition_sizes, &mut log_cb);

            // Build a remap from original start_lba to new start_lba for logicals.
            let logical_remap: std::collections::HashMap<u64, u64> = ebr_result
                .as_ref()
                .map(|r| r.logical_remap.iter().copied().collect())
                .unwrap_or_default();

            // Patch extended container entry in MBR if logicals were repacked.
            if let Some(ref ebr) = ebr_result {
                let ext_start = ebr.extended_start_lba;
                let new_total = ebr.new_extended_total_sectors;
                for i in 0..4 {
                    let off = 446 + i * 16;
                    let type_byte = mbr_buf[off + 4];
                    if type_byte == 0x05 || type_byte == 0x0F || type_byte == 0x85 {
                        let entry_start = u32::from_le_bytes([
                            mbr_buf[off + 8],
                            mbr_buf[off + 9],
                            mbr_buf[off + 10],
                            mbr_buf[off + 11],
                        ]);
                        if entry_start == ext_start {
                            mbr_buf[off + 12..off + 16].copy_from_slice(&new_total.to_le_bytes());
                            log_cb(&format!(
                                "Patched MBR extended container: total_sectors = {}",
                                new_total,
                            ));
                            break;
                        }
                    }
                }
            }

            writer
                .write_all(&mbr_buf)
                .context("failed to write patched MBR")?;
            total_written += 512;
            log_cb("Patched MBR partition table with export sizes");

            // Build sorted partition list with remapped positions for logicals.
            struct PartWrite {
                source_lba: u64,
                dest_lba: u64,
                export_size: u64,
                original_size: u64,
                index: usize,
            }
            let mut sorted_parts: Vec<PartWrite> = partition_sizes
                .iter()
                .map(|ps| {
                    let dest_lba = logical_remap
                        .get(&ps.start_lba)
                        .copied()
                        .unwrap_or(ps.start_lba);
                    PartWrite {
                        source_lba: ps.start_lba,
                        dest_lba,
                        export_size: ps.export_size,
                        original_size: ps.original_size,
                        index: ps.index,
                    }
                })
                .collect();
            sorted_parts.sort_by_key(|p| p.dest_lba);

            for pw in &sorted_parts {
                if cancel_check() {
                    bail!("export cancelled");
                }

                let dest_offset = pw.dest_lba * 512;
                let source_offset = pw.source_lba * 512;

                // Fill gap between current position and this partition's dest
                if total_written < dest_offset {
                    let gap = dest_offset - total_written;
                    if pw.source_lba == pw.dest_lba {
                        // Not remapped: copy source data for the gap
                        reader.seek(SeekFrom::Start(total_written))?;
                        let mut gap_reader = (&mut reader).take(gap);
                        let mut gap_remaining = gap;
                        while gap_remaining > 0 {
                            let to_read = (gap_remaining as usize).min(CHUNK_SIZE);
                            let n = gap_reader.read(&mut buf[..to_read])?;
                            if n == 0 {
                                write_zeros(&mut writer, gap_remaining)?;
                                total_written += gap_remaining;
                                break;
                            }
                            writer.write_all(&buf[..n])?;
                            total_written += n as u64;
                            gap_remaining -= n as u64;
                            progress_cb(total_written);
                        }
                    } else {
                        // Remapped: zero-fill the gap (no meaningful source data)
                        write_zeros(&mut writer, gap)?;
                        total_written += gap;
                    }
                }

                // Write partition data (read from source position, write at dest)
                reader.seek(SeekFrom::Start(source_offset))?;
                let copy_size = pw.export_size.min(pw.original_size);
                let mut part_reader = (&mut reader).take(copy_size);
                let mut part_remaining = copy_size;
                while part_remaining > 0 {
                    if cancel_check() {
                        bail!("export cancelled");
                    }
                    let to_read = (part_remaining as usize).min(CHUNK_SIZE);
                    let n = part_reader.read(&mut buf[..to_read])?;
                    if n == 0 {
                        break;
                    }
                    writer.write_all(&buf[..n])?;
                    total_written += n as u64;
                    part_remaining -= n as u64;
                    progress_cb(total_written);
                }

                // If export_size > data copied (e.g. original), pad
                if pw.export_size > copy_size {
                    let pad = pw.export_size - copy_size;
                    write_zeros(&mut writer, pad)?;
                    total_written += pad;
                }

                // Update hidden sectors for all filesystem types
                {
                    writer.flush()?;
                    let end_pos = total_written;
                    patch_hidden_sectors_for(
                        writer.get_mut(),
                        dest_offset,
                        pw.dest_lba,
                        &mut log_cb,
                    )?;
                    writer.seek(SeekFrom::Start(end_pos))?;
                }

                // Resize filesystem if the partition size changed
                if pw.export_size != pw.original_size {
                    writer.flush()?;
                    let end_pos = total_written;
                    resize_filesystem_for(
                        writer.get_mut(),
                        dest_offset,
                        pw.export_size,
                        &mut log_cb,
                    )?;
                    writer.seek(SeekFrom::Start(end_pos))?;
                }

                log_cb(&format!(
                    "partition-{}: exported {} bytes at LBA {}",
                    pw.index, pw.export_size, pw.dest_lba,
                ));
            }

            // Compute the actual data end: max of all partition ends.
            let new_data_end = sorted_parts
                .iter()
                .map(|pw| pw.dest_lba * 512 + pw.export_size)
                .max()
                .unwrap_or(total_written);
            let effective_source_end = source_data_size.min(new_data_end);

            // Copy any remaining data after the last partition up to the
            // effective end (truncated when logicals were repacked).
            if total_written < effective_source_end {
                reader.seek(SeekFrom::Start(total_written))?;
                let remaining = effective_source_end - total_written;
                let mut tail_reader = (&mut reader).take(remaining);
                loop {
                    if cancel_check() {
                        bail!("export cancelled");
                    }
                    let n = tail_reader.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    writer.write_all(&buf[..n])?;
                    total_written += n as u64;
                    progress_cb(total_written);
                }
            }

            // Write the rebuilt EBR chain sectors into the output.
            if let Some(ref ebr) = ebr_result {
                for (ebr_offset, ebr_sector) in &ebr.ebr_sectors {
                    writer.seek(SeekFrom::Start(*ebr_offset))?;
                    writer.write_all(ebr_sector)?;
                }
                // Restore write position to data end for the footer.
                writer.seek(SeekFrom::Start(total_written))?;
            }
        }
    }

    writer.flush()?;

    // Append VHD footer
    let footer = build_vhd_footer(total_written);
    writer
        .write_all(&footer)
        .context("failed to write VHD footer")?;
    writer.flush()?;

    log_cb(&format!(
        "VHD export complete: {} ({} data bytes + 512 byte footer)",
        dest_path.display(),
        total_written,
    ));

    Ok(())
}

/// Export a single partition as a Fixed VHD file.
///
/// Handles raw, zstd, and CHD compressed partition files.
/// If `max_bytes` is `Some(n)`, the output is limited to at most `n` bytes of data.
pub fn export_partition_vhd(
    source_path: &Path,
    compression_type: &str,
    dest_path: &Path,
    max_bytes: Option<u64>,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    let mut writer = BufWriter::new(
        File::create(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?,
    );

    let bytes_written = decompress_to_writer(
        source_path,
        compression_type,
        &mut writer,
        max_bytes,
        &mut progress_cb,
        &cancel_check,
        &mut log_cb,
    )?;

    writer.flush()?;

    // Use the requested size for the VHD footer if specified (may be larger than
    // the decompressed data to create a properly-sized VHD image).
    let vhd_data_size = max_bytes.unwrap_or(bytes_written).max(bytes_written);

    // Pad with zeros if we wrote less than the requested size
    if bytes_written < vhd_data_size {
        let pad = vhd_data_size - bytes_written;
        write_zeros(&mut writer, pad)?;
    }

    // Append VHD footer
    let footer = build_vhd_footer(vhd_data_size);
    writer
        .write_all(&footer)
        .context("failed to write VHD footer")?;
    writer.flush()?;

    log_cb(&format!(
        "VHD partition export complete: {} ({} data bytes)",
        dest_path.display(),
        vhd_data_size,
    ));

    Ok(())
}

/// Export a whole disk from a Clonezilla image as a Fixed VHD file.
///
/// Reconstructs the disk by writing the MBR, hidden data gap, EBR for extended
/// partitions, and each partition's data from partclone, then appends a VHD footer.
pub fn export_clonezilla_disk_vhd(
    cz_image: &crate::clonezilla::metadata::ClonezillaImage,
    _backup_folder: &Path,
    output_path: &Path,
    partition_sizes: &[PartitionSizeOverride],
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    use crate::clonezilla::partclone::open_partclone_reader;

    let mut file = File::create(output_path)
        .with_context(|| format!("failed to create {}", output_path.display()))?;

    let mut total_written: u64 = 0;

    // Helper to look up export size
    let get_export_size = |index: usize, default: u64| -> u64 {
        partition_sizes
            .iter()
            .find(|ps| ps.index == index)
            .map(|ps| ps.export_size)
            .unwrap_or(default)
    };

    // Write MBR
    let mut mbr_buf = cz_image.mbr_bytes;
    if !partition_sizes.is_empty() {
        patch_mbr_entries(&mut mbr_buf, partition_sizes);
        log_cb("Patched MBR partition table with export sizes");
    }
    file.write_all(&mbr_buf).context("failed to write MBR")?;
    total_written += 512;

    // Write hidden data after MBR
    if !cz_image.hidden_data_after_mbr.is_empty() {
        file.write_all(&cz_image.hidden_data_after_mbr)
            .context("failed to write hidden data after MBR")?;
        total_written += cz_image.hidden_data_after_mbr.len() as u64;
        log_cb(&format!(
            "Wrote {} bytes of hidden data after MBR",
            cz_image.hidden_data_after_mbr.len()
        ));
    }

    // Sort partitions by start LBA
    let mut sorted_parts: Vec<&crate::clonezilla::metadata::ClonezillaPartition> =
        cz_image.partitions.iter().collect();
    sorted_parts.sort_by_key(|p| p.start_lba);

    for cz_part in &sorted_parts {
        if cancel_check() {
            bail!("export cancelled");
        }

        let part_offset = cz_part.start_lba * 512;
        let export_size = get_export_size(cz_part.index, cz_part.size_bytes());

        // Fill gap to partition start
        if total_written < part_offset {
            let gap = part_offset - total_written;

            // Write EBR if this is an extended partition and we have EBR data
            if let Some(ebr_data) = cz_image.ebr_data.get(&cz_part.device_name) {
                // EBR sits at the partition's start LBA
                let ebr_gap = part_offset - total_written - ebr_data.len().min(512) as u64;
                if ebr_gap > 0 {
                    super::write_zeros(&mut file, ebr_gap)?;
                    total_written += ebr_gap;
                }
                let write_len = ebr_data.len().min(512);
                file.write_all(&ebr_data[..write_len])?;
                total_written += write_len as u64;
            } else {
                super::write_zeros(&mut file, gap)?;
                total_written += gap;
            }
        }

        // Handle extended container partitions (no data to write)
        if cz_part.is_extended {
            continue;
        }

        // Write partition data from partclone
        if cz_part.partclone_files.is_empty() {
            log_cb(&format!(
                "partition-{}: no data files, filling with zeros",
                cz_part.index
            ));
            super::write_zeros(&mut file, export_size)?;
            total_written += export_size;
            continue;
        }

        log_cb(&format!(
            "partition-{}: decompressing partclone data...",
            cz_part.index
        ));

        let (_header, mut reader) = open_partclone_reader(&cz_part.partclone_files)?;

        let mut buf = vec![0u8; super::CHUNK_SIZE];
        let mut part_written: u64 = 0;
        let copy_limit = export_size.min(cz_part.size_bytes());
        let mut limited = (&mut reader).take(copy_limit);

        loop {
            if cancel_check() {
                bail!("export cancelled");
            }
            let n = limited.read(&mut buf)?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n])?;
            part_written += n as u64;
            total_written += n as u64;
            progress_cb(total_written);
        }

        // Pad if export_size > data written
        if part_written < export_size {
            let pad = export_size - part_written;
            super::write_zeros(&mut file, pad)?;
            total_written += pad;
        }

        // Patch hidden sectors and resize filesystem
        {
            let effective_lba = partition_sizes
                .iter()
                .find(|ps| ps.index == cz_part.index)
                .map(|ps| ps.effective_start_lba())
                .unwrap_or(cz_part.start_lba);

            patch_hidden_sectors_for(&mut file, part_offset, effective_lba, &mut log_cb)?;

            if export_size != cz_part.size_bytes() {
                resize_filesystem_for(&mut file, part_offset, export_size, &mut log_cb)?;
            }

            // Seek back to end for next partition
            file.seek(SeekFrom::Start(total_written))?;
        }

        log_cb(&format!(
            "partition-{}: wrote {} bytes",
            cz_part.index, export_size,
        ));
    }

    // Append VHD footer
    let footer = build_vhd_footer(total_written);
    file.write_all(&footer)
        .context("failed to write VHD footer")?;
    file.flush()?;

    log_cb(&format!(
        "VHD export complete: {} ({} data bytes + 512 byte footer)",
        output_path.display(),
        total_written,
    ));

    Ok(())
}

/// Export a single partition from a Clonezilla image as a Fixed VHD file.
pub fn export_clonezilla_partition_vhd(
    partclone_files: &[std::path::PathBuf],
    dest_path: &Path,
    export_size: Option<u64>,
    mut progress_cb: impl FnMut(u64),
    cancel_check: impl Fn() -> bool,
    mut log_cb: impl FnMut(&str),
) -> Result<()> {
    use crate::clonezilla::partclone::open_partclone_reader;

    let (header, mut reader) = open_partclone_reader(partclone_files)?;
    let partition_size = header.partition_size();
    let max_bytes = export_size.unwrap_or(partition_size);

    let mut writer = BufWriter::new(
        File::create(dest_path)
            .with_context(|| format!("failed to create {}", dest_path.display()))?,
    );

    let mut buf = vec![0u8; super::CHUNK_SIZE];
    let mut total_written: u64 = 0;
    let mut limited = (&mut reader).take(max_bytes);

    loop {
        if cancel_check() {
            bail!("export cancelled");
        }
        let n = limited.read(&mut buf)?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n])?;
        total_written += n as u64;
        progress_cb(total_written);
    }

    // Pad if needed
    if total_written < max_bytes {
        let pad = max_bytes - total_written;
        super::write_zeros(&mut writer, pad)?;
        total_written = max_bytes;
    }

    writer.flush()?;

    // Append VHD footer
    let footer = build_vhd_footer(total_written);
    writer
        .write_all(&footer)
        .context("failed to write VHD footer")?;
    writer.flush()?;

    log_cb(&format!(
        "VHD partition export complete: {} ({} data bytes)",
        dest_path.display(),
        total_written,
    ));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::CompressionType;
    use std::io::Cursor;
    use tempfile::TempDir;

    #[test]
    fn test_write_vhd() {
        let tmp = TempDir::new().unwrap();
        let data = vec![0xABu8; 4096];
        let mut reader = Cursor::new(&data);
        let base = tmp.path().join("partition-0");

        let files = super::super::compress_partition(
            &mut reader,
            &base,
            CompressionType::Vhd,
            data.len() as u64,
            None,
            false,
            None,
            |_| {},
            || false,
            |_| {},
        )
        .unwrap();

        assert_eq!(files, vec!["partition-0.vhd"]);
        let written = fs::read(tmp.path().join("partition-0.vhd")).unwrap();
        // Raw data (4096) + VHD footer (512)
        assert_eq!(written.len(), 4096 + 512);

        // Verify footer starts with "conectix" cookie
        let footer = &written[4096..];
        assert_eq!(&footer[0..8], b"conectix");

        // Verify data size fields (Original Size at offset 40, Current Size at offset 48)
        let orig_size = u64::from_be_bytes(footer[40..48].try_into().unwrap());
        assert_eq!(orig_size, 4096);
        let curr_size = u64::from_be_bytes(footer[48..56].try_into().unwrap());
        assert_eq!(curr_size, 4096);

        // Verify disk type = 2 (Fixed)
        let disk_type = u32::from_be_bytes(footer[60..64].try_into().unwrap());
        assert_eq!(disk_type, 2);

        // Verify checksum
        let stored_checksum = u32::from_be_bytes(footer[64..68].try_into().unwrap());
        let mut sum: u32 = 0;
        for (i, &b) in footer.iter().enumerate() {
            if (64..68).contains(&i) {
                continue;
            }
            sum = sum.wrapping_add(b as u32);
        }
        assert_eq!(stored_checksum, !sum);
    }

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

    /// Build a minimal in-memory dynamic VHD. `blocks[i] == Some(data)` is an
    /// allocated block (data must be `block_size` bytes); `None` is unallocated.
    fn make_dynamic_vhd(block_size: usize, blocks: &[Option<Vec<u8>>]) -> Vec<u8> {
        let n = blocks.len();
        let disk_size = (block_size * n) as u64;
        let sectors_per_block = block_size / 512;
        let bitmap_size = sectors_per_block.div_ceil(8).div_ceil(512) * 512;

        let header_offset: u64 = 512;
        let bat_offset: u64 = 1536;
        let data_start = (bat_offset + (n as u64) * 4).div_ceil(512) * 512;

        let mut footer = build_vhd_footer(disk_size);
        footer[16..24].copy_from_slice(&header_offset.to_be_bytes());
        footer[60..64].copy_from_slice(&VHD_TYPE_DYNAMIC.to_be_bytes());

        let mut header = [0u8; 1024];
        header[0..8].copy_from_slice(VHD_DYNAMIC_COOKIE);
        header[8..16].copy_from_slice(&u64::MAX.to_be_bytes());
        header[16..24].copy_from_slice(&bat_offset.to_be_bytes());
        header[24..28].copy_from_slice(&0x0001_0000u32.to_be_bytes());
        header[28..32].copy_from_slice(&(n as u32).to_be_bytes());
        header[32..36].copy_from_slice(&(block_size as u32).to_be_bytes());

        let mut bat = vec![BAT_UNUSED; n];
        let mut blocks_region: Vec<u8> = Vec::new();
        let mut next_sector = data_start / 512;
        for (i, b) in blocks.iter().enumerate() {
            if let Some(data) = b {
                assert_eq!(data.len(), block_size);
                bat[i] = next_sector as u32;
                blocks_region.extend(std::iter::repeat_n(0xFFu8, bitmap_size));
                blocks_region.extend_from_slice(data);
                next_sector += ((bitmap_size + block_size) as u64) / 512;
            }
        }

        let mut out = vec![0u8; data_start as usize];
        out[0..512].copy_from_slice(&footer);
        out[512..1536].copy_from_slice(&header);
        for (i, e) in bat.iter().enumerate() {
            let off = bat_offset as usize + i * 4;
            out[off..off + 4].copy_from_slice(&e.to_be_bytes());
        }
        out.extend_from_slice(&blocks_region);
        out.extend_from_slice(&footer);
        out
    }

    #[test]
    fn test_dynamic_vhd_reader_mixed_blocks() {
        // Block 0 allocated (0xAB filled), block 1 unallocated (reads zeros).
        let bs = 512usize;
        let block0 = vec![0xABu8; bs];
        let image = make_dynamic_vhd(bs, &[Some(block0.clone()), None]);

        let mut reader = DynamicVhdReader::open(std::io::Cursor::new(image)).unwrap();
        assert_eq!(reader.len(), (bs * 2) as u64);

        // Whole-disk read, looping because reads stop at block boundaries.
        let mut got = Vec::new();
        std::io::copy(&mut reader, &mut got).unwrap();
        assert_eq!(got.len(), bs * 2);
        assert_eq!(&got[..bs], &block0[..]);
        assert!(
            got[bs..].iter().all(|&b| b == 0),
            "unallocated block != zeros"
        );
    }

    #[test]
    fn test_dynamic_vhd_reader_seek_to_boundary() {
        let bs = 512usize;
        let block1 = vec![0x5Au8; bs];
        // Block 0 unallocated, block 1 allocated.
        let image = make_dynamic_vhd(bs, &[None, Some(block1.clone())]);
        let mut reader = DynamicVhdReader::open(std::io::Cursor::new(image)).unwrap();

        // Seek to the start of block 1 and read it back.
        reader.seek(SeekFrom::Start(bs as u64)).unwrap();
        let mut buf = vec![0u8; bs];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(buf, block1);

        // Seek back to block 0 (unallocated) — should read zeros.
        reader.seek(SeekFrom::Start(0)).unwrap();
        let mut buf0 = vec![0u8; bs];
        reader.read_exact(&mut buf0).unwrap();
        assert!(buf0.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_dynamic_vhd_detection_roundtrip() {
        // End-to-end: write a dynamic VHD to disk, let detect_image_format
        // recognize it, then wrap_image_reader and read it back.
        let bs = 512usize;
        let block0 = vec![0xCDu8; bs];
        let image = make_dynamic_vhd(bs, &[Some(block0.clone()), None]);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dyn.vhd");
        std::fs::write(&path, &image).unwrap();

        let file = File::open(&path).unwrap();
        let fmt = crate::rbformats::detect_image_format_with_path(file, Some(&path)).unwrap();
        match &fmt {
            crate::rbformats::ImageFormat::VhdDynamic { logical_size, .. } => {
                assert_eq!(*logical_size, (bs * 2) as u64);
            }
            _ => panic!("expected VhdDynamic"),
        }

        let file2 = File::open(&path).unwrap();
        let (mut reader, size) = crate::rbformats::wrap_image_reader(file2, fmt).unwrap();
        assert_eq!(size, (bs * 2) as u64);
        let mut got = Vec::new();
        std::io::copy(&mut reader, &mut got).unwrap();
        assert_eq!(&got[..bs], &block0[..]);
        assert!(got[bs..].iter().all(|&b| b == 0));
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
    fn test_dynamic_vhd_reader_rejects_differencing() {
        let bs = 512usize;
        let mut image = make_dynamic_vhd(bs, &[Some(vec![1u8; bs])]);
        // Patch both footer copies' disk type to 4 (differencing).
        let len = image.len();
        image[60..64].copy_from_slice(&VHD_TYPE_DIFFERENCING.to_be_bytes());
        image[len - 512 + 60..len - 512 + 64].copy_from_slice(&VHD_TYPE_DIFFERENCING.to_be_bytes());
        let err = match DynamicVhdReader::open(std::io::Cursor::new(image)) {
            Ok(_) => panic!("differencing VHD should be rejected"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("differencing"));
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

    // --- Session 1.2 tests: dynamic VHD writer ---

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
    fn test_vhd_checksum_helper() {
        let data = [0u8; 512];
        assert_eq!(vhd_checksum(&data), !0u32);
        let data2 = [1u8; 4];
        assert_eq!(vhd_checksum(&data2), !4u32);
    }

    #[test]
    fn test_dynamic_vhd_writer_all_zeros() {
        // A 64 KiB all-zero disk should produce a near-empty dynamic VHD:
        // footer copy(512) + header(1024) + BAT(512) + trailing footer(512)
        // = 2560 bytes, zero data blocks.
        let disk_size: u64 = 65536;
        let block_size = 65536u32;
        let source = vec![0u8; disk_size as usize];
        let mut out = Cursor::new(Vec::new());

        export_whole_disk_vhd_dynamic(
            &mut Cursor::new(&source),
            &mut out,
            disk_size,
            block_size,
            &mut |_| {},
            &|| false,
        )
        .unwrap();

        let written = out.into_inner();
        // Just: footer copy + header + BAT (1 entry=4 bytes, padded to 512) + footer
        let expected = 512 + 1024 + 512 + 512;
        assert_eq!(
            written.len(),
            expected,
            "all-zero disk should produce a minimal dynamic VHD, got {} bytes",
            written.len()
        );

        // Round-trip: reader should see all zeros.
        let mut reader = DynamicVhdReader::open(Cursor::new(written)).unwrap();
        assert_eq!(reader.len(), disk_size);
        let mut got = vec![0xFFu8; disk_size as usize];
        std::io::copy(&mut reader, &mut Cursor::new(&mut got[..])).unwrap();
        assert!(got.iter().all(|&b| b == 0), "round-trip should read zeros");
    }

    #[test]
    fn test_dynamic_vhd_writer_roundtrip() {
        // 8-block disk at 64 KiB block size (512 KiB total). Blocks 0 and 4
        // have data; the rest are zero. The dynamic file should be much
        // smaller than the flat equivalent because 6 of 8 blocks are skipped.
        let bs = 65536usize;
        let n = 8;
        let disk_size = (bs * n) as u64;
        let mut source = vec![0u8; disk_size as usize];
        // Fill block 0 with 0xAB.
        for b in source[..bs].iter_mut() {
            *b = 0xAB;
        }
        // Fill block 4 with a pattern.
        for (i, b) in source[bs * 4..bs * 5].iter_mut().enumerate() {
            *b = (i % 256) as u8;
        }

        let mut out = Cursor::new(Vec::new());
        export_whole_disk_vhd_dynamic(
            &mut Cursor::new(&source),
            &mut out,
            disk_size,
            bs as u32,
            &mut |_| {},
            &|| false,
        )
        .unwrap();

        let written = out.into_inner();

        // The file should be smaller than a fixed VHD of the same size because
        // 6 of 8 blocks are skipped.
        let fixed_size = disk_size as usize + 512; // raw + footer
        assert!(
            written.len() < fixed_size,
            "dynamic VHD ({}) should be smaller than fixed ({})",
            written.len(),
            fixed_size
        );

        // Round-trip through the reader.
        let mut reader = DynamicVhdReader::open(Cursor::new(written)).unwrap();
        assert_eq!(reader.len(), disk_size);
        let mut got = Vec::new();
        std::io::copy(&mut reader, &mut got).unwrap();
        assert_eq!(got.len(), disk_size as usize);
        assert_eq!(got, source, "round-trip mismatch");
    }

    #[test]
    fn test_dynamic_vhd_writer_default_block_size() {
        // Using block_size=0 should select the 2 MiB default and still round-trip.
        let disk_size: u64 = 4 * 1024 * 1024; // 4 MiB = 2 blocks at 2 MiB
        let mut source = vec![0u8; disk_size as usize];
        // Put some data in the second block.
        source[2 * 1024 * 1024] = 0x42;

        let mut out = Cursor::new(Vec::new());
        export_whole_disk_vhd_dynamic(
            &mut Cursor::new(&source),
            &mut out,
            disk_size,
            0, // default
            &mut |_| {},
            &|| false,
        )
        .unwrap();

        let written = out.into_inner();
        let mut reader = DynamicVhdReader::open(Cursor::new(written)).unwrap();
        assert_eq!(reader.len(), disk_size);
        let mut got = Vec::new();
        std::io::copy(&mut reader, &mut got).unwrap();
        assert_eq!(got, source);
    }

    // --- Session 1.3 tests: in-place edit (allocate-on-write) ---

    /// Write into a previously-sparse block, then reopen with a fresh reader
    /// and verify both the new data is readable and the rest of the disk
    /// (including blocks that were already allocated) is intact.
    #[test]
    fn test_dynamic_vhd_write_allocates_sparse_block() {
        // 4-block disk (512 B blocks). Start with block 0 allocated (0xAA) and
        // blocks 1..4 sparse.
        let bs = 512usize;
        let disk_size = (bs * 4) as u64;
        let mut source = vec![0u8; disk_size as usize];
        for b in source[..bs].iter_mut() {
            *b = 0xAA;
        }

        // Build it via the dynamic writer so the layout is canonical.
        let mut backing = Cursor::new(Vec::new());
        export_whole_disk_vhd_dynamic(
            &mut Cursor::new(&source),
            &mut backing,
            disk_size,
            bs as u32,
            &mut |_| {},
            &|| false,
        )
        .unwrap();
        let before_len = backing.get_ref().len();

        // Open read-write, mutate block 2 (currently sparse).
        backing.seek(SeekFrom::Start(0)).unwrap();
        let mut rw = DynamicVhdReader::open(backing).unwrap();
        rw.seek(SeekFrom::Start((bs * 2) as u64)).unwrap();
        let payload = vec![0x5Au8; bs];
        rw.write_all(&payload).unwrap();
        rw.flush().unwrap();
        // Recover the underlying Cursor so we can reopen via a fresh reader.
        let backing = {
            let mut inner = rw.inner;
            inner.seek(SeekFrom::Start(0)).unwrap();
            inner
        };

        // File grew by exactly one block (bitmap + data).
        let bitmap_size = 512u64;
        let after_len = backing.get_ref().len();
        assert_eq!(
            after_len as u64,
            before_len as u64 + bitmap_size + bs as u64
        );

        // Reopen with a fresh reader and verify everything.
        let mut reader = DynamicVhdReader::open(backing).unwrap();
        assert_eq!(reader.len(), disk_size);
        let mut got = vec![0u8; disk_size as usize];
        reader.read_exact(&mut got).unwrap();

        // Block 0 still 0xAA.
        assert!(got[..bs].iter().all(|&b| b == 0xAA), "block 0 corrupted");
        // Block 1 still zero (sparse).
        assert!(
            got[bs..bs * 2].iter().all(|&b| b == 0),
            "block 1 should still be sparse"
        );
        // Block 2 holds the new payload.
        assert_eq!(&got[bs * 2..bs * 3], &payload[..], "new write missing");
        // Block 3 still zero (sparse).
        assert!(
            got[bs * 3..].iter().all(|&b| b == 0),
            "block 3 should still be sparse"
        );
    }

    /// Partial write inside an unallocated block: only the touched bytes get
    /// the new value, the rest of the now-allocated block reads as zero.
    #[test]
    fn test_dynamic_vhd_partial_write_zero_fills_rest_of_block() {
        let bs = 1024usize;
        let disk_size = (bs * 2) as u64;
        let source = vec![0u8; disk_size as usize];

        let mut backing = Cursor::new(Vec::new());
        export_whole_disk_vhd_dynamic(
            &mut Cursor::new(&source),
            &mut backing,
            disk_size,
            bs as u32,
            &mut |_| {},
            &|| false,
        )
        .unwrap();

        backing.seek(SeekFrom::Start(0)).unwrap();
        let mut rw = DynamicVhdReader::open(backing).unwrap();
        // Write 4 bytes in the middle of block 1 (previously sparse).
        rw.seek(SeekFrom::Start(bs as u64 + 100)).unwrap();
        rw.write_all(&[0x11, 0x22, 0x33, 0x44]).unwrap();

        let mut backing = rw.inner;
        backing.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = DynamicVhdReader::open(backing).unwrap();
        let mut got = vec![0u8; disk_size as usize];
        reader.read_exact(&mut got).unwrap();

        // Block 0 untouched (zero).
        assert!(got[..bs].iter().all(|&b| b == 0));
        // First 100 bytes of block 1 still zero.
        assert!(got[bs..bs + 100].iter().all(|&b| b == 0));
        // The 4 payload bytes.
        assert_eq!(&got[bs + 100..bs + 104], &[0x11, 0x22, 0x33, 0x44]);
        // The rest of block 1 still zero.
        assert!(got[bs + 104..].iter().all(|&b| b == 0));
    }

    /// Writing into an already-allocated block updates in place (file size
    /// does not change) and reads back the new bytes.
    #[test]
    fn test_dynamic_vhd_write_in_place_for_allocated_block() {
        let bs = 512usize;
        let disk_size = (bs * 2) as u64;
        let mut source = vec![0u8; disk_size as usize];
        for b in source[..bs].iter_mut() {
            *b = 0xAA;
        }

        let mut backing = Cursor::new(Vec::new());
        export_whole_disk_vhd_dynamic(
            &mut Cursor::new(&source),
            &mut backing,
            disk_size,
            bs as u32,
            &mut |_| {},
            &|| false,
        )
        .unwrap();
        let before_len = backing.get_ref().len();

        backing.seek(SeekFrom::Start(0)).unwrap();
        let mut rw = DynamicVhdReader::open(backing).unwrap();
        rw.seek(SeekFrom::Start(0)).unwrap();
        rw.write_all(&[0xBBu8; 16]).unwrap();
        let mut backing = rw.inner;
        let after_len = backing.get_ref().len();
        assert_eq!(after_len, before_len, "in-place write should not grow file");

        backing.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = DynamicVhdReader::open(backing).unwrap();
        let mut got = vec![0u8; disk_size as usize];
        reader.read_exact(&mut got).unwrap();
        assert!(got[..16].iter().all(|&b| b == 0xBB));
        assert!(got[16..bs].iter().all(|&b| b == 0xAA));
    }

    /// FS-level integration: format a blank FAT volume into a dynamic VHD,
    /// open it read-write through the FAT layer, create a file, sync, then
    /// reopen via DynamicVhdReader and read the file back through the FAT
    /// layer on the immutable view.
    #[test]
    fn test_dynamic_vhd_fat_edit_roundtrip() {
        use crate::fs::fat::{create_blank_fat, FatFilesystem};
        use crate::fs::filesystem::{EditableFilesystem, Filesystem};
        use crate::fs::CreateFileOptions;

        // Format a 4 MiB FAT12 volume so it has cluster space to work with.
        let blank = create_blank_fat(4 * 1024 * 1024, Some("DYNVHD")).expect("format FAT");
        let disk_size = blank.len() as u64;

        // Wrap it in a dynamic VHD on disk.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fat.vhd");
        {
            let mut out = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            export_whole_disk_vhd_dynamic(
                &mut Cursor::new(&blank),
                &mut out,
                disk_size,
                65536,
                &mut |_| {},
                &|| false,
            )
            .unwrap();
        }

        // Open read-write through the dynamic-VHD reader, hand it to the FAT
        // filesystem layer, create a file, sync metadata.
        let payload = b"hello from a sparse-allocated FAT cluster";
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            let mut vhd = DynamicVhdReader::open(file).unwrap();
            // FAT inside the VHD: partition_offset = 0 (no MBR/GPT around it).
            let mut fs = FatFilesystem::open(&mut vhd, 0).expect("open FAT inside dyn VHD");
            let root = fs.root().expect("root");
            let mut src = Cursor::new(payload.to_vec());
            fs.create_file(
                &root,
                "edit.txt",
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create_file");
            fs.sync_metadata().expect("sync_metadata");
        }

        // Reopen via a fresh DynamicVhdReader and confirm the file is there.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut vhd = DynamicVhdReader::open(file).unwrap();
        let mut fs = FatFilesystem::open(&mut vhd, 0).expect("reopen FAT inside dyn VHD");
        let root = fs.root().expect("root");
        let entries = fs.list_directory(&root).expect("list");
        let file_entry = entries
            .iter()
            .find(|e| e.name == "edit.txt")
            .expect("edit.txt should exist after sync");
        let got = fs.read_file(file_entry, payload.len()).expect("read_file");
        assert_eq!(got, payload, "file contents survived the round-trip");
    }

    #[test]
    fn test_dynamic_vhd_writer_detection_roundtrip() {
        // Write to a file, let detect_image_format recognize it, then read back.
        let bs = 1024usize;
        let disk_size = (bs * 2) as u64;
        let mut source = vec![0u8; disk_size as usize];
        for b in source[..bs].iter_mut() {
            *b = 0x77;
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.vhd");
        {
            let mut out = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            export_whole_disk_vhd_dynamic(
                &mut Cursor::new(&source),
                &mut out,
                disk_size,
                bs as u32,
                &mut |_| {},
                &|| false,
            )
            .unwrap();
        }

        let file = File::open(&path).unwrap();
        let fmt = crate::rbformats::detect_image_format_with_path(file, Some(&path)).unwrap();
        match &fmt {
            crate::rbformats::ImageFormat::VhdDynamic { logical_size, .. } => {
                assert_eq!(*logical_size, disk_size);
            }
            _ => panic!("expected VhdDynamic, got something else"),
        }

        let file2 = File::open(&path).unwrap();
        let (mut reader, size) = crate::rbformats::wrap_image_reader(file2, fmt).unwrap();
        assert_eq!(size, disk_size);
        let mut got = Vec::new();
        std::io::copy(&mut reader, &mut got).unwrap();
        assert_eq!(got, source);
    }
}
