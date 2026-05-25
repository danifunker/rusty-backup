//! VMDK sparse reader (`monolithicSparse` / hosted `SparseExtent` extents).
//!
//! Phase 4 / Session 4.1 of `docs/virtualization-formats.md`. Writer + edit
//! land in 4.2 / 4.3.
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

use std::io::{Read, Seek, SeekFrom};

use anyhow::{bail, Context, Result};

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

        Ok(Self {
            inner,
            header,
            gd,
            gt_cache: None,
            pos: 0,
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
    fn len_matches_header_capacity() {
        let image = build_test_image(10, 2, 8, &[]);
        let r = VmdkSparseReader::open(Cursor::new(image)).unwrap();
        assert_eq!(r.len(), 10 * VMDK_SECTOR);
        assert!(!r.is_empty());
    }
}
