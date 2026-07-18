//! Apple **sparse disk image** (`.sparseimage`, UDSP) reader.
//!
//! A sparse image is a single growable file that stores only the 1 MiB "bands"
//! that have actually been written. It is NOT a UDIF/koly image ([`super::dmg`])
//! — it has its own `sprs` header and a band map. Introduced in Mac OS X 10.3
//! (Panther, the first FileVault store).
//!
//! (Distinct from [`super::sparse`], which is the write-side allocator shared by
//! the dynamic-VHD / QCOW2 / VMDK-sparse *output* formats.)
//!
//! On-disk layout (all fields big-endian), pinned against `hdiutil`-produced
//! images:
//!
//! ```text
//! 0x00  u32  magic "sprs"
//! 0x04  u32  version (= 3)
//! 0x08  u32  sectors per band (always 2048 => 1 MiB bands, even for GB images)
//! 0x0C  u32  flags (observed 1)
//! 0x10  u32  total virtual size in 512-byte sectors
//! 0x20  u32  (copy of total sectors)
//! 0x40  ...  band map: u32[] indexed by PHYSICAL band slot. Each entry is
//!            `virtual_band_index + 1`, or 0 if the slot is unused.
//! ```
//!
//! Physical band slot `p` is stored at file offset `HEADER_SIZE + p * band_bytes`
//! (band data begins at `HEADER_SIZE` = 0x1000). To read virtual band `v`, we
//! invert the map: the physical slot whose entry equals `v + 1` holds it; a
//! virtual band with no physical slot reads as zeros.
//!
//! The band map lives in the header region `0x40..0x1000`, so it addresses at
//! most 1008 physical bands (~1 GiB of *actually written* data). Images with
//! more allocated bands than that use an index-continuation scheme we have not
//! reverse-engineered; [`detect_sparseimage`] returns a clear error for them
//! rather than risk a misread.

use std::io::{self, Read, Seek, SeekFrom};

use anyhow::{bail, Context, Result};

/// "sprs" magic at offset 0.
const SPRS_MAGIC: &[u8; 4] = b"sprs";
/// Supported version.
const SPRS_VERSION: u32 = 3;
/// Header size; also the file offset of physical band slot 0.
const HEADER_SIZE: u64 = 0x1000;
/// First band-map entry offset.
const BAND_MAP_OFFSET: usize = 0x40;
/// Max physical bands addressable by the in-header band map (`0x40..0x1000`).
const MAX_HEADER_BANDS: u64 = (HEADER_SIZE as usize - BAND_MAP_OFFSET) as u64 / 4;
const SECTOR_SIZE: u64 = 512;

/// A `Read + Seek` view over a decoded sparse image's virtual disk.
pub struct SparseImageReader<R: Read + Seek> {
    source: R,
    /// Total virtual size in bytes.
    total_size: u64,
    /// Bytes per band.
    band_bytes: u64,
    /// `virt_to_phys[v]` = Some(physical slot) if virtual band `v` is allocated.
    virt_to_phys: Vec<Option<u32>>,
    /// Current virtual read position.
    position: u64,
}

impl<R: Read + Seek> SparseImageReader<R> {
    /// Total virtual (uncompressed) size in bytes.
    pub fn total_size(&self) -> u64 {
        self.total_size
    }
}

/// If `source` is an Apple sparse image, build a [`SparseImageReader`] over its
/// virtual disk. Returns `Ok(None)` if the `sprs` magic is absent (not a sparse
/// image), and `Err` for a sparse image we recognize but can't fully decode.
pub fn detect_sparseimage<R: Read + Seek>(mut source: R) -> Result<Option<SparseImageReader<R>>> {
    let file_len = source.seek(SeekFrom::End(0))?;
    if file_len < HEADER_SIZE {
        return Ok(None);
    }
    source.seek(SeekFrom::Start(0))?;
    let mut header = vec![0u8; HEADER_SIZE as usize];
    source.read_exact(&mut header)?;

    if &header[0..4] != SPRS_MAGIC {
        return Ok(None);
    }
    let version = u32::from_be_bytes(header[4..8].try_into().unwrap());
    if version != SPRS_VERSION {
        bail!("unsupported sparse image version {version} (expected {SPRS_VERSION})");
    }
    let sectors_per_band = u32::from_be_bytes(header[8..12].try_into().unwrap()) as u64;
    let total_sectors = u32::from_be_bytes(header[0x10..0x14].try_into().unwrap()) as u64;
    if sectors_per_band == 0 {
        bail!("sparse image reports zero sectors per band");
    }
    let band_bytes = sectors_per_band * SECTOR_SIZE;
    let total_size = total_sectors * SECTOR_SIZE;

    // Physical bands actually present on disk (band data starts at HEADER_SIZE).
    let physical_bands = (file_len - HEADER_SIZE) / band_bytes;
    if physical_bands > MAX_HEADER_BANDS {
        bail!(
            "sparse image has {physical_bands} allocated bands (> {MAX_HEADER_BANDS}); \
             images with more than ~1 GiB of written data use a band-index \
             continuation scheme that is not yet supported"
        );
    }

    // Invert the physical-slot -> virtual-band map into virtual -> physical.
    let total_virtual_bands = total_size.div_ceil(band_bytes) as usize;
    let mut virt_to_phys = vec![None; total_virtual_bands];
    for p in 0..physical_bands as usize {
        let off = BAND_MAP_OFFSET + p * 4;
        let entry = u32::from_be_bytes(header[off..off + 4].try_into().unwrap());
        if entry == 0 {
            continue; // slot unused
        }
        let virt = (entry - 1) as usize;
        if virt >= total_virtual_bands {
            bail!("sparse band map entry {entry} exceeds virtual band count");
        }
        virt_to_phys[virt] = Some(p as u32);
    }

    Ok(Some(SparseImageReader {
        source,
        total_size,
        band_bytes,
        virt_to_phys,
        position: 0,
    }))
}

impl<R: Read + Seek> Read for SparseImageReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.total_size || buf.is_empty() {
            return Ok(0);
        }
        let band = self.position / self.band_bytes;
        let offset_in_band = self.position % self.band_bytes;
        // Never span a band boundary in one read.
        let remaining_in_band = self.band_bytes - offset_in_band;
        let remaining_in_disk = self.total_size - self.position;
        let n = buf
            .len()
            .min(remaining_in_band as usize)
            .min(remaining_in_disk as usize);

        match self.virt_to_phys.get(band as usize).copied().flatten() {
            Some(phys) => {
                let file_off = HEADER_SIZE + phys as u64 * self.band_bytes + offset_in_band;
                self.source.seek(SeekFrom::Start(file_off))?;
                self.source.read_exact(&mut buf[..n])?;
            }
            None => {
                buf[..n].fill(0); // unallocated band reads as zeros
            }
        }
        self.position += n as u64;
        Ok(n)
    }
}

impl<R: Read + Seek> Seek for SparseImageReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::Current(d) => self.position as i64 + d,
            SeekFrom::End(d) => self.total_size as i64 + d,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }
        self.position = new_pos as u64;
        Ok(self.position)
    }
}

/// Convenience wrapper mirroring [`super::dmg::detect_dmg`]: open a path and
/// detect a sparse image on it.
pub fn detect_sparseimage_path(
    path: &std::path::Path,
) -> Result<Option<SparseImageReader<std::fs::File>>> {
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    detect_sparseimage(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal 3-virtual-band sparse image in memory: band 0 and band 2
    /// allocated (out of physical order), band 1 a hole.
    fn build_sparse(band_bytes: usize) -> (Vec<u8>, Vec<u8>) {
        let total_bands = 3u32;
        let total_sectors = total_bands as u64 * (band_bytes as u64 / SECTOR_SIZE);
        let sectors_per_band = (band_bytes / SECTOR_SIZE as usize) as u32;

        let mut header = vec![0u8; HEADER_SIZE as usize];
        header[0..4].copy_from_slice(SPRS_MAGIC);
        header[4..8].copy_from_slice(&SPRS_VERSION.to_be_bytes());
        header[8..12].copy_from_slice(&sectors_per_band.to_be_bytes());
        header[0x10..0x14].copy_from_slice(&(total_sectors as u32).to_be_bytes());
        // Physical slot 0 -> virtual band 2 (entry = 3); slot 1 -> virtual band 0
        // (entry = 1). Virtual band 1 is a hole (no slot references it).
        header[BAND_MAP_OFFSET..BAND_MAP_OFFSET + 4].copy_from_slice(&3u32.to_be_bytes());
        header[BAND_MAP_OFFSET + 4..BAND_MAP_OFFSET + 8].copy_from_slice(&1u32.to_be_bytes());

        // Expected virtual disk: band0 = 0xAA, band1 = 0x00, band2 = 0xCC.
        let band0 = vec![0xAAu8; band_bytes];
        let band1 = vec![0x00u8; band_bytes];
        let band2 = vec![0xCCu8; band_bytes];
        let expected: Vec<u8> = [band0.clone(), band1, band2.clone()].concat();

        // Physical file: header, then phys slot 0 (= virtual band 2 => 0xCC),
        // then phys slot 1 (= virtual band 0 => 0xAA).
        let mut file = header;
        file.extend_from_slice(&band2); // physical slot 0
        file.extend_from_slice(&band0); // physical slot 1
        (file, expected)
    }

    #[test]
    fn sparse_round_trips_holes_and_reordered_bands() {
        let band_bytes = 4096; // small band for the test
        let (file, expected) = build_sparse(band_bytes);
        let mut reader = detect_sparseimage(Cursor::new(file)).unwrap().unwrap();
        assert_eq!(reader.total_size(), expected.len() as u64);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, expected);
    }

    #[test]
    fn sparse_seek_reads_correct_band() {
        let band_bytes = 4096;
        let (file, expected) = build_sparse(band_bytes);
        let mut reader = detect_sparseimage(Cursor::new(file)).unwrap().unwrap();
        // Seek into band 2 (0xCC region).
        reader
            .seek(SeekFrom::Start(2 * band_bytes as u64 + 10))
            .unwrap();
        let mut b = [0u8; 4];
        reader.read_exact(&mut b).unwrap();
        assert_eq!(b, [0xCC; 4]);
        assert_eq!(&expected[2 * band_bytes + 10..2 * band_bytes + 14], &b);
    }

    #[test]
    fn non_sparse_returns_none() {
        let junk = vec![0u8; HEADER_SIZE as usize + 16];
        assert!(detect_sparseimage(Cursor::new(junk)).unwrap().is_none());
    }
}
