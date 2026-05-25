//! VMDK sparse reader + writer + in-place editor (`monolithicSparse` /
//! hosted `SparseExtent` extents).
//!
//! Phase 4 of `docs/virtualization-formats.md`: 4.1 reader, 4.2 writer,
//! 4.3 allocate-on-write edit.
//!
//! ## Scope
//!
//! - Reads the binary `SparseExtentHeader` (magic `KDMV`, little-endian) plus
//!   its grain directory (GD) and grain tables (GT).
//! - Unallocated grains (GT entry == 0) read as zeros.
//! - Single-extent grain map only. Markers / compressed grains (the
//!   `streamOptimized` variant) are detected and rejected — they are not
//!   randomly seekable, so the reader cannot serve them.
//! - The embedded descriptor (if present) is currently *ignored* by the
//!   reader; the disk geometry comes from the binary header's `capacity` /
//!   `grainSize` fields, which is what VMware itself authoritatively uses
//!   when the two disagree.
//!
//! Layout shorthand: a guest sector `s` lives in grain `g = s / grainSize`,
//! which lives in GT `gt = g / numGTEsPerGT` at entry `gte = g % numGTEsPerGT`.
//! `GD[gt]` (u32 sector offset) → that GT's host sector; `GT[gt][gte]` (u32
//! sector offset) → the grain's host sector, or `0` for an unallocated grain.

use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::{bail, Context, Result};

use super::sparse::{is_zero_unit, SparseAllocator};

const VMDK_SECTOR: u64 = 512;

/// `KDMV` — little-endian `0x564d444b`. Seam-A detection key.
pub const VMDK_SPARSE_MAGIC: &[u8; 4] = b"KDMV";

const HEADER_LEN: usize = 512;

/// Sparse-header flags bit 16 — grains are compressed (`streamOptimized`).
/// Random-access is not possible — reader rejects.
const FLAG_COMPRESSED: u32 = 1 << 16;
/// Sparse-header flags bit 17 — markers present (also `streamOptimized`).
/// Same rejection rationale.
const FLAG_MARKERS: u32 = 1 << 17;

/// Parsed `SparseExtentHeader`.
#[derive(Debug, Clone)]
pub struct VmdkSparseHeader {
    pub version: u32,
    pub flags: u32,
    /// Disk capacity in sectors.
    pub capacity_sectors: u64,
    /// Grain size in sectors (typically 128 → 64 KiB).
    pub grain_size_sectors: u64,
    pub descriptor_offset_sectors: u64,
    pub descriptor_size_sectors: u64,
    /// Entries per grain table (typically 512).
    pub num_gtes_per_gt: u32,
    /// Redundant GD offset, in sectors (`0` if absent).
    pub rgd_offset_sectors: u64,
    /// Primary GD offset, in sectors.
    pub gd_offset_sectors: u64,
    /// Sectors before grain data starts.
    pub overhead_sectors: u64,
    pub compress_algorithm: u16,
}

impl VmdkSparseHeader {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_LEN {
            bail!(
                "VMDK sparse header too short: got {} bytes, need {}",
                bytes.len(),
                HEADER_LEN
            );
        }
        if &bytes[0..4] != VMDK_SPARSE_MAGIC {
            bail!("VMDK sparse: bad magic (not `KDMV`)");
        }
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if !matches!(version, 1 | 2 | 3) {
            bail!("VMDK sparse: unsupported header version {version}");
        }
        let flags = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let capacity_sectors = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        let grain_size_sectors = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
        let descriptor_offset_sectors = u64::from_le_bytes(bytes[28..36].try_into().unwrap());
        let descriptor_size_sectors = u64::from_le_bytes(bytes[36..44].try_into().unwrap());
        let num_gtes_per_gt = u32::from_le_bytes(bytes[44..48].try_into().unwrap());
        let rgd_offset_sectors = u64::from_le_bytes(bytes[48..56].try_into().unwrap());
        let gd_offset_sectors = u64::from_le_bytes(bytes[56..64].try_into().unwrap());
        let overhead_sectors = u64::from_le_bytes(bytes[64..72].try_into().unwrap());
        let compress_algorithm = u16::from_le_bytes(bytes[77..79].try_into().unwrap());

        if grain_size_sectors == 0 || !grain_size_sectors.is_power_of_two() {
            bail!(
                "VMDK sparse: grain size {} sectors is not a positive power of two",
                grain_size_sectors
            );
        }
        if num_gtes_per_gt == 0 || !num_gtes_per_gt.is_power_of_two() {
            bail!(
                "VMDK sparse: numGTEsPerGT {} is not a positive power of two",
                num_gtes_per_gt
            );
        }
        if flags & (FLAG_COMPRESSED | FLAG_MARKERS) != 0 {
            bail!(
                "VMDK sparse: compressed/marker grains (streamOptimized) are \
                 not seekable and not supported (flags = 0x{flags:08x})"
            );
        }
        if compress_algorithm != 0 {
            bail!(
                "VMDK sparse: non-zero compressAlgorithm ({compress_algorithm}) \
                 is not supported"
            );
        }
        if gd_offset_sectors == 0 || gd_offset_sectors == u64::MAX {
            bail!(
                "VMDK sparse: missing or sentinel gdOffset ({gd_offset_sectors}); \
                 hosted/streamOptimized images that defer the GD are not supported"
            );
        }

        Ok(Self {
            version,
            flags,
            capacity_sectors,
            grain_size_sectors,
            descriptor_offset_sectors,
            descriptor_size_sectors,
            num_gtes_per_gt,
            rgd_offset_sectors,
            gd_offset_sectors,
            overhead_sectors,
            compress_algorithm,
        })
    }

    /// Number of grain tables (= length of the GD).
    pub fn num_grain_tables(&self) -> u64 {
        let grains_per_gt = self.grain_size_sectors * self.num_gtes_per_gt as u64;
        if grains_per_gt == 0 {
            0
        } else {
            self.capacity_sectors.div_ceil(grains_per_gt)
        }
    }

    /// Disk capacity in bytes.
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_sectors * VMDK_SECTOR
    }
}

/// `Read + Seek` over a `monolithicSparse` VMDK.
///
/// Holds the parsed header, the in-RAM grain directory, a single-entry LRU
/// over grain tables (mirrors `ChdReader`'s hunk cache / `Qcow2Reader`'s L2
/// cache), and a logical cursor. Unallocated grains read as zeros.
pub struct VmdkSparseReader<R: Read + Seek> {
    inner: R,
    header: VmdkSparseHeader,
    /// Grain directory in RAM: u32 sector offsets, one per GT.
    gd: Vec<u32>,
    /// `(gt_index, parsed_entries)` for the most recently loaded GT.
    gt_cache: Option<(usize, Vec<u32>)>,
    pos: u64,
    /// Next free host byte for grain allocation (= EOF rounded up to a
    /// grain boundary). Allocate-on-write bumps this by `grain_size_bytes`
    /// per new grain.
    host_end: u64,
}

impl<R: Read + Seek> VmdkSparseReader<R> {
    pub fn open(mut inner: R) -> Result<Self> {
        let mut hbuf = [0u8; HEADER_LEN];
        inner
            .seek(SeekFrom::Start(0))
            .context("VMDK sparse: seek to header")?;
        inner
            .read_exact(&mut hbuf)
            .context("VMDK sparse: read header")?;
        let header = VmdkSparseHeader::parse(&hbuf)?;

        let num_gts = header.num_grain_tables();
        // Cheap sanity ceiling. A 16 TiB disk at 64 KiB grains × 512 entries
        // per GT needs ~512 K GD entries → ~2 MiB. Anything above 64 MiB of
        // GD is almost certainly a malformed header.
        let max_gd_bytes: u64 = 64 * 1024 * 1024;
        if num_gts.saturating_mul(4) > max_gd_bytes {
            bail!(
                "VMDK sparse: implausible grain-directory size ({num_gts} entries); \
                 refusing to allocate"
            );
        }

        let gd_offset = header.gd_offset_sectors * VMDK_SECTOR;
        let mut gd_buf = vec![0u8; (num_gts as usize) * 4];
        inner.seek(SeekFrom::Start(gd_offset))?;
        inner.read_exact(&mut gd_buf).with_context(|| {
            format!(
                "VMDK sparse: read grain directory @ sector {} ({} entries)",
                header.gd_offset_sectors, num_gts
            )
        })?;
        let gd: Vec<u32> = gd_buf
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
            .collect();

        let file_end = inner.seek(SeekFrom::End(0))?;
        let grain_size_bytes = header.grain_size_sectors * VMDK_SECTOR;
        // Round host_end up to a grain boundary so future grain allocations
        // stay grain-aligned (matches the writer).
        let host_end = file_end.div_ceil(grain_size_bytes) * grain_size_bytes;

        Ok(Self {
            inner,
            header,
            gd,
            gt_cache: None,
            pos: 0,
            host_end,
        })
    }

    /// Disk capacity in bytes.
    pub fn len(&self) -> u64 {
        self.header.capacity_bytes()
    }

    /// Empty if no sectors.
    pub fn is_empty(&self) -> bool {
        self.header.capacity_bytes() == 0
    }

    /// Borrow the parsed header (for diagnostics, integration glue).
    pub fn header(&self) -> &VmdkSparseHeader {
        &self.header
    }

    /// Load (and single-entry cache) the GT at `gt_index`.
    /// Returns `None` if the GD entry is zero (GT not allocated).
    fn load_gt(&mut self, gt_index: usize) -> std::io::Result<Option<&[u32]>> {
        if self
            .gt_cache
            .as_ref()
            .map(|(i, _)| *i == gt_index)
            .unwrap_or(false)
        {
            return Ok(Some(&self.gt_cache.as_ref().unwrap().1));
        }
        let gd_entry = match self.gd.get(gt_index).copied() {
            Some(v) => v,
            None => return Ok(None),
        };
        if gd_entry == 0 {
            return Ok(None);
        }
        let gt_byte = (gd_entry as u64) * VMDK_SECTOR;
        let entries = self.header.num_gtes_per_gt as usize;
        let mut buf = vec![0u8; entries * 4];
        self.inner.seek(SeekFrom::Start(gt_byte))?;
        self.inner.read_exact(&mut buf)?;
        let gt: Vec<u32> = buf
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        self.gt_cache = Some((gt_index, gt));
        Ok(Some(&self.gt_cache.as_ref().unwrap().1))
    }
}

impl<R: Read + Seek> Read for VmdkSparseReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.header.capacity_bytes() || buf.is_empty() {
            return Ok(0);
        }
        let grain_size_bytes = self.header.grain_size_sectors * VMDK_SECTOR;
        let grain_index = self.pos / grain_size_bytes;
        let intra = self.pos % grain_size_bytes;
        let remaining_in_grain = grain_size_bytes - intra;
        let remaining_in_disk = self.header.capacity_bytes() - self.pos;
        let to_read = (buf.len() as u64)
            .min(remaining_in_grain)
            .min(remaining_in_disk) as usize;

        let entries_per_gt = self.header.num_gtes_per_gt as u64;
        let gt_index = (grain_index / entries_per_gt) as usize;
        let gte_index = (grain_index % entries_per_gt) as usize;

        let grain_sector = match self
            .load_gt(gt_index)
            .map_err(|e| std::io::Error::other(format!("VMDK sparse: load GT: {e}")))?
        {
            Some(gt) => gt.get(gte_index).copied().unwrap_or(0),
            None => 0,
        };

        if grain_sector == 0 {
            for b in &mut buf[..to_read] {
                *b = 0;
            }
        } else {
            let grain_byte = (grain_sector as u64) * VMDK_SECTOR;
            self.inner.seek(SeekFrom::Start(grain_byte + intra))?;
            self.inner.read_exact(&mut buf[..to_read])?;
        }
        self.pos += to_read as u64;
        Ok(to_read)
    }
}

// ---------------------------------------------------------------------------
// In-place edit (allocate-on-write)
// ---------------------------------------------------------------------------

impl<R: Read + Write + Seek> VmdkSparseReader<R> {
    /// Allocate a fresh grain at `host_end`: write a zero-filled grain into
    /// the new host region, set `GT[gt_index][gte_index]` to its host sector
    /// (both in RAM and on disk), bump `host_end`.
    ///
    /// Returns the host byte offset of the new grain. `gt_index` must point
    /// at an existing GT — our writer pre-allocates every GT (one per GD
    /// entry) so the "grow a GT when its GD slot is empty" branch the QCOW2
    /// editor needs doesn't arise here.
    fn allocate_grain(&mut self, gt_index: usize, gte_index: usize) -> std::io::Result<u64> {
        let grain_size_bytes = self.header.grain_size_sectors * VMDK_SECTOR;
        let new_host = self.host_end;
        // Zero-fill the grain so partial writes that come next don't expose
        // stale bytes from beyond EOF.
        self.inner.seek(SeekFrom::Start(new_host))?;
        let zeros = vec![0u8; grain_size_bytes as usize];
        self.inner.write_all(&zeros)?;

        let new_grain_sector: u32 = (new_host / VMDK_SECTOR).try_into().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "VMDK sparse: grain offset overflows u32 sector field",
            )
        })?;

        // Update GT entry on disk: GT host byte = gd[gt_index] * 512 + gte_index * 4.
        let gt_host_sector = self.gd[gt_index] as u64;
        let entry_byte = gt_host_sector * VMDK_SECTOR + (gte_index as u64) * 4;
        self.inner.seek(SeekFrom::Start(entry_byte))?;
        self.inner.write_all(&new_grain_sector.to_le_bytes())?;

        // Update cached GT if it's the one we just touched.
        if let Some((cached_idx, gt)) = &mut self.gt_cache {
            if *cached_idx == gt_index {
                gt[gte_index] = new_grain_sector;
            }
        }

        self.host_end += grain_size_bytes;
        Ok(new_host)
    }
}

impl<R: Read + Write + Seek> Write for VmdkSparseReader<R> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.pos >= self.header.capacity_bytes() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "write past end of VMDK sparse image",
            ));
        }
        let grain_size_bytes = self.header.grain_size_sectors * VMDK_SECTOR;
        let grain_index = self.pos / grain_size_bytes;
        let intra = self.pos % grain_size_bytes;
        let remaining_in_grain = grain_size_bytes - intra;
        let remaining_in_disk = self.header.capacity_bytes() - self.pos;
        let to_write = (buf.len() as u64)
            .min(remaining_in_grain)
            .min(remaining_in_disk) as usize;

        let entries_per_gt = self.header.num_gtes_per_gt as u64;
        let gt_index = (grain_index / entries_per_gt) as usize;
        let gte_index = (grain_index % entries_per_gt) as usize;

        // Look up current host sector.
        let grain_sector = match self
            .load_gt(gt_index)
            .map_err(|e| std::io::Error::other(format!("VMDK sparse: load GT for write: {e}")))?
        {
            Some(gt) => gt.get(gte_index).copied().unwrap_or(0),
            None => 0,
        };

        let host_byte = if grain_sector == 0 {
            self.allocate_grain(gt_index, gte_index)?
        } else {
            (grain_sector as u64) * VMDK_SECTOR
        };

        self.inner.seek(SeekFrom::Start(host_byte + intra))?;
        self.inner.write_all(&buf[..to_write])?;
        self.pos += to_write as u64;
        Ok(to_write)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<R: Read + Seek> Seek for VmdkSparseReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new = match pos {
            SeekFrom::Start(n) => n as i128,
            SeekFrom::End(n) => self.header.capacity_bytes() as i128 + n as i128,
            SeekFrom::Current(n) => self.pos as i128 + n as i128,
        };
        if new < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek before start of VMDK sparse image",
            ));
        }
        self.pos = new as u64;
        Ok(self.pos)
    }
}

/// Build a minimal `monolithicSparse` image in memory for tests / fixtures.
///
/// Layout: header @ sector 0, GD @ sector 1, GTs starting @ sector 2 (one
/// sector per GT — fits `numGTEsPerGT ≤ 128`; tests use 16), then grain
/// data. Only the grains listed in `allocated_grains` get host sectors; all
/// others stay unmapped (= read as zeros).
///
/// Returned as a single `Vec<u8>` that callers wrap in a `Cursor`.
#[cfg(test)]
pub fn build_test_image(
    capacity_sectors: u64,
    grain_size_sectors: u64,
    num_gtes_per_gt: u32,
    allocated_grains: &[(u64, Vec<u8>)],
) -> Vec<u8> {
    let mut image: Vec<u8> = Vec::new();

    let grains_per_gt = grain_size_sectors * num_gtes_per_gt as u64;
    let num_gts = capacity_sectors.div_ceil(grains_per_gt);

    let gd_sector: u64 = 1;
    let gt_first_sector: u64 = 2;
    let grain_data_first_sector: u64 = gt_first_sector + num_gts;

    // Reserve header + GD + GTs.
    let reserved_bytes = (grain_data_first_sector * VMDK_SECTOR) as usize;
    image.resize(reserved_bytes, 0);

    // Header.
    let mut hdr = [0u8; HEADER_LEN];
    hdr[0..4].copy_from_slice(VMDK_SPARSE_MAGIC);
    hdr[4..8].copy_from_slice(&1u32.to_le_bytes());
    hdr[8..12].copy_from_slice(&0u32.to_le_bytes());
    hdr[12..20].copy_from_slice(&capacity_sectors.to_le_bytes());
    hdr[20..28].copy_from_slice(&grain_size_sectors.to_le_bytes());
    hdr[28..36].copy_from_slice(&0u64.to_le_bytes());
    hdr[36..44].copy_from_slice(&0u64.to_le_bytes());
    hdr[44..48].copy_from_slice(&num_gtes_per_gt.to_le_bytes());
    hdr[48..56].copy_from_slice(&0u64.to_le_bytes());
    hdr[56..64].copy_from_slice(&gd_sector.to_le_bytes());
    hdr[64..72].copy_from_slice(&grain_data_first_sector.to_le_bytes());
    hdr[72] = 0;
    hdr[73] = b'\n';
    hdr[74] = b' ';
    hdr[75] = b'\r';
    hdr[76] = b'\n';
    hdr[77..79].copy_from_slice(&0u16.to_le_bytes());
    image[..HEADER_LEN].copy_from_slice(&hdr);

    // GD: each GT's host sector.
    for gt in 0..num_gts {
        let host_sec = (gt_first_sector + gt) as u32;
        let off = (gd_sector * VMDK_SECTOR) as usize + gt as usize * 4;
        image[off..off + 4].copy_from_slice(&host_sec.to_le_bytes());
    }

    // Grain data + GT entries.
    let grain_bytes = (grain_size_sectors * VMDK_SECTOR) as usize;
    let mut next_grain_sector = grain_data_first_sector;
    for (grain_idx, payload) in allocated_grains {
        assert_eq!(
            payload.len(),
            grain_bytes,
            "test grain payload must equal grain size ({grain_bytes} bytes)"
        );
        let gt_index = (grain_idx / num_gtes_per_gt as u64) as usize;
        let gte_index = (grain_idx % num_gtes_per_gt as u64) as usize;
        let gt_byte_off =
            ((gt_first_sector + gt_index as u64) * VMDK_SECTOR) as usize + gte_index * 4;
        image[gt_byte_off..gt_byte_off + 4]
            .copy_from_slice(&(next_grain_sector as u32).to_le_bytes());

        // Append the grain data.
        image.extend_from_slice(payload);
        next_grain_sector += grain_size_sectors;
    }

    image
}

// ---------------------------------------------------------------------------
// Writer — monolithicSparse
// ---------------------------------------------------------------------------

/// Default grain size in bytes when the caller passes `0` to
/// [`export_vmdk_sparse`]. Matches VMware's monolithicSparse default
/// (128 sectors = 64 KiB).
pub const VMDK_SPARSE_DEFAULT_GRAIN_BYTES: u64 = 64 * 1024;
/// Default `numGTEsPerGT` — entries per grain table.
pub const VMDK_SPARSE_DEFAULT_GTES_PER_GT: u32 = 512;
/// Default embedded-descriptor reservation, in sectors (10 KiB). VMware's
/// own writers use 20 sectors for monolithicSparse.
pub const VMDK_SPARSE_DESCRIPTOR_SECTORS: u64 = 20;

/// Stream a whole-disk source into a `monolithicSparse` VMDK at `out`.
///
/// Allocates a 64 KiB grain (configurable via `grain_size_bytes`, `0` =
/// default) on host only for non-zero source grains; runs of zeros stay
/// unmapped and read back as zeros via [`VmdkSparseReader`]. Mirrors
/// `export_qcow2` / `export_whole_disk_vhd_dynamic`: backfill the
/// header/descriptor/GD/GT region at the end, after the data stream is
/// known.
///
/// `disk_size_bytes` must be a non-zero multiple of 512 and equal to the
/// total bytes available from `reader`.
pub fn export_vmdk_sparse(
    reader: &mut impl Read,
    out: &mut (impl Write + Seek),
    disk_size_bytes: u64,
    grain_size_bytes: u64,
    progress_cb: &mut dyn FnMut(u64),
    cancel_check: &dyn Fn() -> bool,
) -> Result<()> {
    if disk_size_bytes == 0 {
        bail!("VMDK sparse export: source disk size is zero");
    }
    if !disk_size_bytes.is_multiple_of(VMDK_SECTOR) {
        bail!(
            "VMDK sparse export: source size {disk_size_bytes} is not a multiple of \
             {VMDK_SECTOR} bytes"
        );
    }

    let grain_size_bytes = if grain_size_bytes == 0 {
        VMDK_SPARSE_DEFAULT_GRAIN_BYTES
    } else {
        grain_size_bytes
    };
    if !grain_size_bytes.is_multiple_of(VMDK_SECTOR) || !grain_size_bytes.is_power_of_two() {
        bail!(
            "VMDK sparse export: grain size {grain_size_bytes} must be a power-of-two \
             multiple of 512"
        );
    }
    let grain_size_sectors = grain_size_bytes / VMDK_SECTOR;
    let num_gtes_per_gt: u32 = VMDK_SPARSE_DEFAULT_GTES_PER_GT;
    let capacity_sectors = disk_size_bytes / VMDK_SECTOR;
    let grains_per_gt = grain_size_sectors * num_gtes_per_gt as u64;
    let num_gts = capacity_sectors.div_ceil(grains_per_gt) as usize;

    // Layout:
    //   sector 0           — SparseExtentHeader
    //   sector 1..21       — embedded descriptor (20 sectors / 10 KiB)
    //   sector 21..        — GD (one u32 per GT)
    //   ... + GD len       — GTs (each numGTEsPerGT × 4 bytes, padded to a sector)
    //   overhead_sectors   — start of grain data (rounded up to grain boundary)
    let gd_bytes = (num_gts * 4) as u64;
    let gd_sectors = gd_bytes.div_ceil(VMDK_SECTOR);
    let gt_bytes_per_gt = (num_gtes_per_gt as u64) * 4;
    let gt_sectors_per_gt = gt_bytes_per_gt.div_ceil(VMDK_SECTOR);
    let gt_total_sectors = gt_sectors_per_gt * num_gts as u64;

    let descriptor_offset_sectors: u64 = 1;
    let descriptor_size_sectors: u64 = VMDK_SPARSE_DESCRIPTOR_SECTORS;
    let gd_first_sector: u64 = descriptor_offset_sectors + descriptor_size_sectors;
    let gt_first_sector: u64 = gd_first_sector + gd_sectors;
    let raw_overhead_sectors: u64 = gt_first_sector + gt_total_sectors;
    // VMware aligns overHead up to grain_size_sectors so grain data is grain-
    // aligned on host — keeps `qemu-img check` and read paths simple.
    let overhead_sectors: u64 =
        raw_overhead_sectors.div_ceil(grain_size_sectors) * grain_size_sectors;

    // Pre-fill GD with sequential GT host sectors; pre-allocate GT buffers (all
    // zero = no grains yet). GT[gt][gte] = host sector of the grain (or 0).
    let mut gd: Vec<u32> = Vec::with_capacity(num_gts);
    for i in 0..num_gts {
        let gt_host_sector = gt_first_sector + (i as u64) * gt_sectors_per_gt;
        gd.push(gt_host_sector as u32);
    }
    let mut gts: Vec<Vec<u32>> = vec![vec![0u32; num_gtes_per_gt as usize]; num_gts];

    // Zero-fill the reserved header/descriptor/GD/GT region in the output so
    // the file has a flat backing store before we start writing grain data
    // past it (Cursor<Vec<u8>> targets need explicit zero-fill; File would
    // grow on first write but we keep behaviour identical).
    let reserved_bytes: u64 = overhead_sectors * VMDK_SECTOR;
    out.seek(SeekFrom::Start(0))?;
    super::write_zeros(out, reserved_bytes)?;

    // Stream the source one grain at a time. Allocate at EOF for non-zero
    // grains; leave GT[..]==0 for zero grains.
    let mut allocator = SparseAllocator::new(reserved_bytes, grain_size_bytes);
    let mut buf = vec![0u8; grain_size_bytes as usize];
    let mut read_done: u64 = 0;
    let mut grain_idx: u64 = 0;
    while read_done < disk_size_bytes {
        if cancel_check() {
            bail!("VMDK sparse export cancelled");
        }
        let want = (disk_size_bytes - read_done).min(buf.len() as u64) as usize;
        // Zero-pad the tail of the final grain if the source isn't a clean
        // grain multiple.
        for b in &mut buf[want..] {
            *b = 0;
        }
        reader
            .read_exact(&mut buf[..want])
            .context("VMDK sparse export: read source grain")?;
        read_done += want as u64;
        let gt_index = (grain_idx / num_gtes_per_gt as u64) as usize;
        let gte_index = (grain_idx % num_gtes_per_gt as u64) as usize;
        if !is_zero_unit(&buf[..]) {
            let host_byte = allocator.alloc();
            assert!(
                host_byte.is_multiple_of(VMDK_SECTOR),
                "grain offset {host_byte} not sector-aligned"
            );
            out.seek(SeekFrom::Start(host_byte))?;
            out.write_all(&buf)?;
            gts[gt_index][gte_index] = (host_byte / VMDK_SECTOR) as u32;
        }
        grain_idx += 1;
        progress_cb(read_done);
    }

    // Backfill: header (sector 0), descriptor (sector 1+), GD, GTs.
    let mut hdr = [0u8; HEADER_LEN];
    hdr[0..4].copy_from_slice(VMDK_SPARSE_MAGIC);
    hdr[4..8].copy_from_slice(&3u32.to_le_bytes()); // version 3 — current
    hdr[8..12].copy_from_slice(&0u32.to_le_bytes()); // flags: no RGD, no markers
    hdr[12..20].copy_from_slice(&capacity_sectors.to_le_bytes());
    hdr[20..28].copy_from_slice(&grain_size_sectors.to_le_bytes());
    hdr[28..36].copy_from_slice(&descriptor_offset_sectors.to_le_bytes());
    hdr[36..44].copy_from_slice(&descriptor_size_sectors.to_le_bytes());
    hdr[44..48].copy_from_slice(&num_gtes_per_gt.to_le_bytes());
    hdr[48..56].copy_from_slice(&0u64.to_le_bytes()); // no RGD
    hdr[56..64].copy_from_slice(&gd_first_sector.to_le_bytes());
    hdr[64..72].copy_from_slice(&overhead_sectors.to_le_bytes());
    hdr[72] = 0; // uncleanShutdown
    hdr[73] = b'\n';
    hdr[74] = b' ';
    hdr[75] = b'\r';
    hdr[76] = b'\n';
    hdr[77..79].copy_from_slice(&0u16.to_le_bytes());
    out.seek(SeekFrom::Start(0))?;
    out.write_all(&hdr)?;

    // Embedded descriptor — see build_monolithic_sparse_descriptor below.
    // The reader ignores it (the binary header is authoritative); it's there
    // so qemu-img / VMware tooling have something to display for `info`.
    let descriptor = build_monolithic_sparse_descriptor(disk_size_bytes);
    let mut desc_buf = vec![0u8; (descriptor_size_sectors * VMDK_SECTOR) as usize];
    let dbytes = descriptor.as_bytes();
    let copy_len = dbytes.len().min(desc_buf.len() - 1);
    desc_buf[..copy_len].copy_from_slice(&dbytes[..copy_len]);
    out.seek(SeekFrom::Start(descriptor_offset_sectors * VMDK_SECTOR))?;
    out.write_all(&desc_buf)?;

    // GD.
    let mut gd_bytes_buf = vec![0u8; (gd_sectors * VMDK_SECTOR) as usize];
    for (i, entry) in gd.iter().enumerate() {
        let off = i * 4;
        gd_bytes_buf[off..off + 4].copy_from_slice(&entry.to_le_bytes());
    }
    out.seek(SeekFrom::Start(gd_first_sector * VMDK_SECTOR))?;
    out.write_all(&gd_bytes_buf)?;

    // GTs.
    let gt_pad_bytes = (gt_sectors_per_gt * VMDK_SECTOR) as usize;
    let mut gt_buf = vec![0u8; gt_pad_bytes];
    for (i, gt) in gts.iter().enumerate() {
        for b in &mut gt_buf {
            *b = 0;
        }
        for (j, entry) in gt.iter().enumerate() {
            let off = j * 4;
            gt_buf[off..off + 4].copy_from_slice(&entry.to_le_bytes());
        }
        let host_sector = gt_first_sector + (i as u64) * gt_sectors_per_gt;
        out.seek(SeekFrom::Start(host_sector * VMDK_SECTOR))?;
        out.write_all(&gt_buf)?;
    }

    out.flush()?;
    Ok(())
}

/// Build a minimal `monolithicSparse` embedded descriptor.
///
/// The descriptor is largely cosmetic — `VmdkSparseReader` and qemu-img both
/// take the binary `SparseExtentHeader` as authoritative — but VMware's own
/// tools display the `ddb.*` fields verbatim and `qemu-img info` expects a
/// well-formed extent line. We keep it sector-padded by the caller.
pub fn build_monolithic_sparse_descriptor(disk_size_bytes: u64) -> String {
    let sectors = disk_size_bytes / VMDK_SECTOR;
    let (cyl, heads, secs) = super::vhd::vhd_chs_geometry(disk_size_bytes);
    // Deterministic but disk-derived CID, same shape as the flat writer.
    let mut cid: u32 = 0x9E37_79B9;
    for b in disk_size_bytes.to_le_bytes() {
        cid = cid.wrapping_mul(0x0100_0193) ^ b as u32;
    }
    format!(
        "# Disk DescriptorFile\n\
         version=1\n\
         CID={cid:08x}\n\
         parentCID=ffffffff\n\
         createType=\"monolithicSparse\"\n\
         \n\
         # Extent description\n\
         RW {sectors} SPARSE \"image.vmdk\"\n\
         \n\
         # The Disk Data Base\n\
         #DDB\n\
         ddb.virtualHWVersion = \"4\"\n\
         ddb.geometry.cylinders = \"{cyl}\"\n\
         ddb.geometry.heads = \"{heads}\"\n\
         ddb.geometry.sectors = \"{secs}\"\n\
         ddb.adapterType = \"ide\"\n",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn make_grain(grain_size_sectors: u64, fill: u8) -> Vec<u8> {
        vec![fill; (grain_size_sectors * VMDK_SECTOR) as usize]
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = vec![0u8; HEADER_LEN];
        bytes[0..4].copy_from_slice(b"NOPE");
        let err = VmdkSparseHeader::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("bad magic"), "{err}");
    }

    #[test]
    fn rejects_compressed_flag() {
        let mut bytes = vec![0u8; HEADER_LEN];
        bytes[0..4].copy_from_slice(VMDK_SPARSE_MAGIC);
        bytes[4..8].copy_from_slice(&1u32.to_le_bytes());
        bytes[8..12].copy_from_slice(&FLAG_COMPRESSED.to_le_bytes());
        bytes[20..28].copy_from_slice(&128u64.to_le_bytes());
        bytes[44..48].copy_from_slice(&512u32.to_le_bytes());
        bytes[56..64].copy_from_slice(&1u64.to_le_bytes());
        let err = VmdkSparseHeader::parse(&bytes).unwrap_err();
        assert!(err.to_string().contains("compressed"), "{err}");
    }

    #[test]
    fn read_returns_zeros_for_unallocated_grain() {
        // 4 grains of 1 sector each, GT holds 16 entries.
        let image = build_test_image(4, 1, 16, &[(0, make_grain(1, 0xAB))]);
        let mut r = VmdkSparseReader::open(Cursor::new(image)).unwrap();
        let mut got = vec![0u8; (4 * VMDK_SECTOR) as usize];
        r.read_exact(&mut got).unwrap();
        assert_eq!(&got[..512], &[0xAB; 512][..], "grain 0 reads back");
        assert!(got[512..].iter().all(|&b| b == 0), "grains 1..3 read zeros");
    }

    #[test]
    fn read_crosses_grain_boundary() {
        // grainSize = 2 sectors = 1024 B. Two adjacent allocated grains, then
        // an unallocated grain. Read across all three.
        let gs: u64 = 2;
        let grain0: Vec<u8> = (0..1024u32).map(|i| (i & 0xFF) as u8).collect();
        let grain1: Vec<u8> = (0..1024u32).map(|i| ((i + 100) & 0xFF) as u8).collect();
        let image = build_test_image(6, gs, 8, &[(0, grain0.clone()), (1, grain1.clone())]);
        let mut r = VmdkSparseReader::open(Cursor::new(image)).unwrap();
        let mut got = vec![0u8; 6 * 512];
        r.read_exact(&mut got).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&grain0);
        expected.extend_from_slice(&grain1);
        expected.extend(std::iter::repeat_n(0u8, 2 * 512));
        assert_eq!(got, expected);
    }

    #[test]
    fn seek_then_read_inside_grain() {
        let gs: u64 = 1;
        let grain: Vec<u8> = (0..512u32).map(|i| (i & 0xFF) as u8).collect();
        let image = build_test_image(4, gs, 16, &[(2, grain.clone())]);
        let mut r = VmdkSparseReader::open(Cursor::new(image)).unwrap();
        r.seek(SeekFrom::Start(2 * 512 + 100)).unwrap();
        let mut buf = vec![0u8; 50];
        r.read_exact(&mut buf).unwrap();
        assert_eq!(buf, grain[100..150]);
    }

    #[test]
    fn export_round_trips_through_reader() {
        // 256 KiB source: alternating 64 KiB grains of pattern and zero, plus
        // a partial trailing grain. Reader must reproduce every byte we put
        // in, zero-fill the unallocated grain, and tolerate non-aligned tail.
        let grain = VMDK_SPARSE_DEFAULT_GRAIN_BYTES;
        let disk_size = grain * 4;
        let mut src = vec![0u8; disk_size as usize];
        for (i, b) in src[0..grain as usize].iter_mut().enumerate() {
            *b = ((i ^ (i >> 3)) & 0xFF) as u8;
        }
        for (i, b) in src[(grain * 2) as usize..(grain * 3) as usize]
            .iter_mut()
            .enumerate()
        {
            *b = ((i.wrapping_mul(31337)) & 0xFF) as u8;
        }
        let mut out: Vec<u8> = Vec::new();
        export_vmdk_sparse(
            &mut Cursor::new(&src),
            &mut Cursor::new(&mut out),
            disk_size,
            0,
            &mut |_| {},
            &|| false,
        )
        .unwrap();

        // Reader must round-trip every byte.
        let mut r = VmdkSparseReader::open(Cursor::new(&out)).unwrap();
        assert_eq!(r.len(), disk_size);
        let mut got = vec![0u8; disk_size as usize];
        r.read_exact(&mut got).unwrap();
        assert_eq!(got, src);

        // Sparse layout actually omitted the zero grains — file should be
        // much smaller than logical size (2 allocated grains + header/GD/GTs,
        // not 4 grains).
        assert!(
            (out.len() as u64) < disk_size,
            "sparse output ({}) should be smaller than logical disk ({})",
            out.len(),
            disk_size
        );
    }

    #[test]
    fn export_all_zero_input_skips_all_grains() {
        let grain = VMDK_SPARSE_DEFAULT_GRAIN_BYTES;
        let disk_size = grain * 8;
        let src = vec![0u8; disk_size as usize];
        let mut out: Vec<u8> = Vec::new();
        export_vmdk_sparse(
            &mut Cursor::new(&src),
            &mut Cursor::new(&mut out),
            disk_size,
            0,
            &mut |_| {},
            &|| false,
        )
        .unwrap();
        // Output is just the overhead (header + descriptor + GD + GTs, rounded
        // up to a grain boundary) — *less than one grain* of body.
        assert!(
            (out.len() as u64) <= grain,
            "all-zero export should fit in <=1 grain of overhead, got {} bytes",
            out.len()
        );
        let mut r = VmdkSparseReader::open(Cursor::new(&out)).unwrap();
        let mut got = vec![0u8; disk_size as usize];
        r.read_exact(&mut got).unwrap();
        assert!(got.iter().all(|&b| b == 0));
    }

    #[test]
    fn allocate_on_write_round_trip() {
        // Write a fully-zero source, then mutate two grains through the
        // editor and verify the reader sees the new bytes (allocate-on-write)
        // and qemu-img still considers it clean.
        let grain = VMDK_SPARSE_DEFAULT_GRAIN_BYTES;
        let disk_size = grain * 4;
        let src = vec![0u8; disk_size as usize];
        let tmp = tempfile::NamedTempFile::new().unwrap();
        {
            let mut out = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            export_vmdk_sparse(
                &mut Cursor::new(&src),
                &mut out,
                disk_size,
                0,
                &mut |_| {},
                &|| false,
            )
            .unwrap();
        }

        // Mutate grain 1 and grain 3 via the editor.
        let payload_a: Vec<u8> = (0..grain as u32).map(|i| (i & 0xFF) as u8).collect();
        let payload_b: Vec<u8> = (0..grain as u32).map(|i| ((i + 7) & 0xFF) as u8).collect();
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            let mut rw = VmdkSparseReader::open(f).unwrap();
            rw.seek(SeekFrom::Start(grain)).unwrap();
            rw.write_all(&payload_a).unwrap();
            rw.seek(SeekFrom::Start(grain * 3)).unwrap();
            rw.write_all(&payload_b).unwrap();
            rw.flush().unwrap();
        }

        // Reopen via reader and verify.
        let f = std::fs::File::open(tmp.path()).unwrap();
        let mut r = VmdkSparseReader::open(f).unwrap();
        let mut got = vec![0u8; disk_size as usize];
        r.read_exact(&mut got).unwrap();
        assert!(got[..grain as usize].iter().all(|&b| b == 0));
        assert_eq!(&got[grain as usize..(grain * 2) as usize], &payload_a[..]);
        assert!(got[(grain * 2) as usize..(grain * 3) as usize]
            .iter()
            .all(|&b| b == 0));
        assert_eq!(&got[(grain * 3) as usize..], &payload_b[..]);

        // qemu-img check should still be happy.
        match crate::rbformats::qemu_img_test::qemu_img_check(tmp.path()) {
            Ok(true) => {}
            Ok(false) => eprintln!("skipping qemu-img check — `qemu-img` not on PATH"),
            Err(e) => panic!("qemu-img check failed after allocate-on-write edit: {e}"),
        }
    }

    /// Gated `qemu-img check` on a freshly written sparse VMDK. Skips silently
    /// when `qemu-img` isn't on PATH (CI without QEMU still passes).
    #[test]
    fn qemu_img_check_round_trip() {
        let grain = VMDK_SPARSE_DEFAULT_GRAIN_BYTES;
        let disk_size = grain * 4;
        let mut src = vec![0u8; disk_size as usize];
        for (i, b) in src[grain as usize..(grain * 2) as usize]
            .iter_mut()
            .enumerate()
        {
            *b = (i & 0xFF) as u8;
        }
        let tmp = tempfile::NamedTempFile::new().unwrap();
        {
            let mut out = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            export_vmdk_sparse(
                &mut Cursor::new(&src),
                &mut out,
                disk_size,
                0,
                &mut |_| {},
                &|| false,
            )
            .unwrap();
        }
        match crate::rbformats::qemu_img_test::qemu_img_check(tmp.path()) {
            Ok(true) => {}
            Ok(false) => eprintln!("skipping qemu-img check — `qemu-img` not on PATH"),
            Err(e) => panic!("qemu-img check failed on monolithicSparse VMDK: {e}"),
        }
    }

    #[test]
    fn len_matches_header_capacity() {
        let image = build_test_image(10, 2, 8, &[]);
        let r = VmdkSparseReader::open(Cursor::new(image)).unwrap();
        assert_eq!(r.len(), 10 * VMDK_SECTOR);
        assert!(!r.is_empty());
    }
}
