//! Applesauce **MOOF** disk-image format (read + write).
//!
//! MOOF is the Macintosh sibling of WOZ: an Applesauce container storing the
//! GCR bitstream of a 3.5" Apple/Sony floppy (400K single-sided, 800K
//! double-sided). Snow, MAME, and the Applesauce tooling all read it, and it
//! preserves the exact physical track encoding — so it round-trips a Mac MFS /
//! HFS floppy at the flux-adjacent bitstream level, not just the logical
//! sectors.
//!
//! On disk MOOF is nearly identical to WOZ2:
//! - 8-byte signature `MOOF FF 0A 0D 0A`, then a little-endian CRC32
//!   (ISO-HDLC) over the rest of the file.
//! - Chunks: `INFO` (60 B), `TMAP` (160 B: `[side0, side1]` per track ×80),
//!   `TRKS` (160 × 8-byte descriptors then 512-byte-block bit data from block
//!   3), optional `META` / `FLUX`.
//! - The only meaningful differences from WOZ2 are the signature, the INFO
//!   chunk layout, and the `disk_type` byte semantics (**1 = 400K SSDD GCR,
//!   2 = 800K DSDD GCR**, 3 = 1.44M MFM). The 3.5" GCR track encoding is
//!   byte-for-byte identical, so the writer reuses `woz_write`'s
//!   [`tracks_from_sectors_35`] and the reader reuses `woz`'s
//!   [`decode_35_bitstream`].
//!
//! Reference: <https://applesaucefdc.com/moof-reference/>

use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{bail, Context, Result};

use super::woz::decode_35_bitstream;
use super::woz_write::{tracks_from_sectors_35, RAW_SIZE_35_400K, RAW_SIZE_35_800K};

/// 8-byte MOOF signature: `MOOF` + high-bit / line-ending integrity marker.
const SIGNATURE: [u8; 8] = [b'M', b'O', b'O', b'F', 0xFF, 0x0A, 0x0D, 0x0A];

const HEADER_LEN: usize = 12; // 8-byte signature + 4-byte CRC32
const CHUNK_HDR_LEN: usize = 8; // 4-byte id + 4-byte size

const INFO_CHUNK_START: usize = HEADER_LEN; // 12
const INFO_DATA_START: usize = INFO_CHUNK_START + CHUNK_HDR_LEN; // 20
const INFO_LENGTH: usize = 60;

const TMAP_CHUNK_START: usize = INFO_DATA_START + INFO_LENGTH; // 80
const TMAP_DATA_START: usize = TMAP_CHUNK_START + CHUNK_HDR_LEN; // 88
const TMAP_LENGTH: usize = 160;

const TRKS_CHUNK_START: usize = TMAP_DATA_START + TMAP_LENGTH; // 248
const TRKS_DATA_START: usize = TRKS_CHUNK_START + CHUNK_HDR_LEN; // 256
const TRKS_DESC_LEN: usize = 160 * 8; // 1280
const TRACK_BIT_DATA_START: usize = 1536; // block 3
const BLOCK_SIZE: usize = 512;

/// MOOF `disk_type` byte (INFO offset 1). Note this differs from WOZ, where
/// 1 = 5.25" and 2 = 3.5".
const DISK_TYPE_400K: u8 = 1; // SSDD GCR
const DISK_TYPE_800K: u8 = 2; // DSDD GCR

/// Optimal bit-cell timing for 3.5" GCR, in 125 ns units (16 → 2 µs).
const OPTIMAL_BIT_TIMING_GCR: u8 = 16;

fn write_u16_le(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

fn write_u32_le(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

/// True if `data` begins with the MOOF signature.
pub fn is_moof(data: &[u8]) -> bool {
    data.len() >= 8 && data[..8] == SIGNATURE
}

/// Write the 60-byte MOOF INFO chunk into `buf` at `INFO_CHUNK_START`.
fn write_info_chunk(buf: &mut [u8], disk_type: u8, largest_track_blocks: u16, creator: &str) {
    buf[INFO_CHUNK_START..INFO_CHUNK_START + 4].copy_from_slice(b"INFO");
    write_u32_le(buf, INFO_CHUNK_START + 4, INFO_LENGTH as u32);

    let p = INFO_DATA_START;
    buf[p] = 1; // MOOF INFO version
    buf[p + 1] = disk_type;
    buf[p + 2] = 0; // write-protected = false
    buf[p + 3] = 0; // synchronized = false (tracks generated independently)
    buf[p + 4] = OPTIMAL_BIT_TIMING_GCR;

    // Creator: 32 bytes, space-padded ASCII.
    let creator_bytes = creator.as_bytes();
    let copy_len = creator_bytes.len().min(32);
    for b in &mut buf[p + 5..p + 5 + 32] {
        *b = b' ';
    }
    buf[p + 5..p + 5 + copy_len].copy_from_slice(&creator_bytes[..copy_len]);

    buf[p + 37] = 0; // padding / reserved
    write_u16_le(buf, p + 38, largest_track_blocks);
    write_u16_le(buf, p + 40, 0); // flux_block (none)
    write_u16_le(buf, p + 42, 0); // largest_flux_track (none)
                                  // p+44 .. p+60 reserved → already zero.
}

/// Write the 160-byte MOOF TMAP chunk. Entry `track*2 + side` holds the TRKS
/// index for that track/side; side-1 entries are 0xFF (unmapped) on a
/// single-sided disk. Tracks are laid out in TRKS in `track*sides + side`
/// order, matching the assignment here.
fn write_tmap(buf: &mut [u8], num_tracks: usize, sides: u8) {
    buf[TMAP_CHUNK_START..TMAP_CHUNK_START + 4].copy_from_slice(b"TMAP");
    write_u32_le(buf, TMAP_CHUNK_START + 4, TMAP_LENGTH as u32);
    let p = TMAP_DATA_START;
    for b in &mut buf[p..p + TMAP_LENGTH] {
        *b = 0xFF;
    }
    let mut trk_idx: u8 = 0;
    for t in 0..num_tracks {
        for side in 0..sides as usize {
            buf[p + t * 2 + side] = trk_idx;
            trk_idx += 1;
        }
    }
}

/// Assemble a complete MOOF file from generated 3.5" track bitstreams
/// (`tracks[i] = (bytes, bit_count)`, in `track*sides + side` order).
fn build_moof(disk_type: u8, sides: u8, tracks: &[(Vec<u8>, u32)]) -> Vec<u8> {
    let mut block_counts = Vec::with_capacity(tracks.len());
    let mut largest_blocks: u16 = 0;
    let mut total_track_bytes = 0usize;
    for (bytes, _) in tracks {
        let blocks = bytes.len().div_ceil(BLOCK_SIZE) as u16;
        largest_blocks = largest_blocks.max(blocks);
        block_counts.push(blocks);
        total_track_bytes += blocks as usize * BLOCK_SIZE;
    }

    let trks_chunk_len = TRKS_DESC_LEN + total_track_bytes;
    let file_len = TRKS_DATA_START + trks_chunk_len;
    let mut buf = vec![0u8; file_len];

    buf[..8].copy_from_slice(&SIGNATURE);
    // CRC32 (bytes 8..12) written last.

    write_info_chunk(&mut buf, disk_type, largest_blocks, "rusty-backup");
    write_tmap(&mut buf, tracks.len() / sides as usize, sides);

    buf[TRKS_CHUNK_START..TRKS_CHUNK_START + 4].copy_from_slice(b"TRKS");
    write_u32_le(&mut buf, TRKS_CHUNK_START + 4, trks_chunk_len as u32);

    let first_block: u16 = (TRACK_BIT_DATA_START / BLOCK_SIZE) as u16;
    let mut next_block = first_block;
    for (i, ((bytes, bit_count), &blocks)) in tracks.iter().zip(block_counts.iter()).enumerate() {
        let desc_off = TRKS_DATA_START + i * 8;
        write_u16_le(&mut buf, desc_off, next_block);
        write_u16_le(&mut buf, desc_off + 2, blocks);
        write_u32_le(&mut buf, desc_off + 4, *bit_count);

        let data_off = next_block as usize * BLOCK_SIZE;
        buf[data_off..data_off + bytes.len()].copy_from_slice(bytes);
        next_block += blocks;
    }

    let crc = crc32fast::hash(&buf[HEADER_LEN..]);
    write_u32_le(&mut buf, 8, crc);
    buf
}

/// Encode a raw 3.5" floppy image (400K single-sided or 800K double-sided) as a
/// MOOF file. Input layout matches the WOZ / [`super::woz::WozReader`] 3.5"
/// order (per-track side-0 sectors then side-1 sectors), which is also a flat
/// logical-block image of an MFS / HFS 400K/800K floppy.
pub fn sectors_to_moof(sectors: &[u8]) -> Result<Vec<u8>> {
    let (sides, disk_type) = match sectors.len() {
        RAW_SIZE_35_400K => (1u8, DISK_TYPE_400K),
        RAW_SIZE_35_800K => (2u8, DISK_TYPE_800K),
        n => bail!(
            "MOOF export: unrecognised floppy size {n} bytes \
             (expected 409600 for 400K or 819200 for 800K 3.5\")"
        ),
    };
    let tracks = tracks_from_sectors_35(sectors, sides);
    Ok(build_moof(disk_type, sides, &tracks))
}

/// Write a raw 400K/800K sector buffer to `path` as a MOOF file.
pub fn write_moof(path: &Path, sectors: &[u8]) -> Result<()> {
    let bytes = sectors_to_moof(sectors)?;
    std::fs::write(path, &bytes)
        .with_context(|| format!("failed to write MOOF file: {}", path.display()))?;
    Ok(())
}

// ─────────────────────────────── reader ────────────────────────────────────

/// A `Read + Seek` adapter presenting a MOOF floppy's decoded logical sectors
/// as a flat byte stream (same order the WOZ reader uses). MOOF floppies are
/// ≤ 800 KB, so the whole image is decoded at open time and held in memory.
pub struct MoofReader {
    data: Vec<u8>,
    position: u64,
}

impl MoofReader {
    pub fn open(path: &Path) -> Result<Self> {
        let raw = std::fs::read(path)
            .with_context(|| format!("failed to read MOOF file: {}", path.display()))?;
        Self::from_bytes(raw)
    }

    pub fn from_bytes(raw: Vec<u8>) -> Result<Self> {
        if !is_moof(&raw) {
            bail!("not a MOOF file (bad signature)");
        }
        if raw.len() < HEADER_LEN {
            bail!("MOOF file truncated");
        }
        let stored_crc = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]);
        let actual_crc = crc32fast::hash(&raw[HEADER_LEN..]);
        if stored_crc != actual_crc {
            bail!("MOOF CRC32 mismatch: file {stored_crc:08X}, computed {actual_crc:08X}");
        }

        // Parse chunks to find INFO (disk type), TMAP, and TRKS.
        let mut disk_type: Option<u8> = None;
        let mut tmap = [0xFFu8; TMAP_LENGTH];
        let mut trks_data: Option<Vec<u8>> = None;
        let mut pos = HEADER_LEN;
        while pos + CHUNK_HDR_LEN <= raw.len() {
            let id = &raw[pos..pos + 4];
            let size = u32::from_le_bytes([raw[pos + 4], raw[pos + 5], raw[pos + 6], raw[pos + 7]])
                as usize;
            let data_start = pos + CHUNK_HDR_LEN;
            let data_end = data_start + size;
            if data_end > raw.len() {
                break; // truncated chunk
            }
            match id {
                b"INFO" if size >= 2 => disk_type = Some(raw[data_start + 1]),
                b"TMAP" if size >= TMAP_LENGTH => {
                    tmap.copy_from_slice(&raw[data_start..data_start + TMAP_LENGTH]);
                }
                b"TRKS" => trks_data = Some(raw[data_start..data_end].to_vec()),
                _ => {}
            }
            pos = data_end;
        }

        let disk_type = disk_type.context("MOOF file missing INFO chunk")?;
        if disk_type != DISK_TYPE_400K && disk_type != DISK_TYPE_800K {
            bail!("unsupported MOOF disk type {disk_type} (only 400K/800K GCR are supported)");
        }
        let trks_data = trks_data.context("MOOF file missing TRKS chunk")?;

        // Reuse the WOZ2 3.5" GCR decoder (identical TRKS block layout).
        let data = decode_35_bitstream(tmap, trks_data, raw)?;
        Ok(Self { data, position: 0 })
    }

    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Read for MoofReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let pos = self.position as usize;
        if pos >= self.data.len() {
            return Ok(0);
        }
        let n = buf.len().min(self.data.len() - pos);
        buf[..n].copy_from_slice(&self.data[pos..pos + n]);
        self.position += n as u64;
        Ok(n)
    }
}

impl Seek for MoofReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new = match pos {
            SeekFrom::Start(n) => n as i64,
            SeekFrom::End(n) => self.data.len() as i64 + n,
            SeekFrom::Current(n) => self.position as i64 + n,
        };
        if new < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start of MOOF stream",
            ));
        }
        self.position = new as u64;
        Ok(self.position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a deterministic 400K raw image: byte value = (offset * 7) as u8.
    fn synthetic_400k() -> Vec<u8> {
        (0..RAW_SIZE_35_400K)
            .map(|i| (i.wrapping_mul(7)) as u8)
            .collect()
    }

    fn synthetic_800k() -> Vec<u8> {
        (0..RAW_SIZE_35_800K)
            .map(|i| (i.wrapping_mul(11)) as u8)
            .collect()
    }

    #[test]
    fn moof_400k_round_trips_through_our_decoder() {
        let raw = synthetic_400k();
        let moof = sectors_to_moof(&raw).unwrap();
        assert!(is_moof(&moof));
        // CRC self-consistency (what Snow checks first).
        let stored = u32::from_le_bytes([moof[8], moof[9], moof[10], moof[11]]);
        assert_eq!(stored, crc32fast::hash(&moof[12..]));
        // Track data begins at block 3.
        assert_eq!(&moof[12..16], b"INFO");
        assert_eq!(moof[20 + 1], DISK_TYPE_400K);
        // Decode it back through the (independently-trusted) WOZ 3.5" decoder.
        let mut r = MoofReader::from_bytes(moof).unwrap();
        assert_eq!(r.len(), RAW_SIZE_35_400K as u64);
        let mut back = Vec::new();
        r.read_to_end(&mut back).unwrap();
        assert_eq!(back, raw, "400K MOOF round-trip mismatch");
    }

    #[test]
    fn moof_800k_round_trips_through_our_decoder() {
        let raw = synthetic_800k();
        let moof = sectors_to_moof(&raw).unwrap();
        assert_eq!(moof[20 + 1], DISK_TYPE_800K);
        let mut r = MoofReader::from_bytes(moof).unwrap();
        assert_eq!(r.len(), RAW_SIZE_35_800K as u64);
        let mut back = Vec::new();
        r.read_to_end(&mut back).unwrap();
        assert_eq!(back, raw, "800K MOOF round-trip mismatch");
    }

    #[test]
    fn rejects_wrong_size() {
        assert!(sectors_to_moof(&[0u8; 143_360]).is_err()); // 5.25" not allowed
        assert!(sectors_to_moof(&[0u8; 100]).is_err());
    }

    #[test]
    fn detects_crc_corruption() {
        let raw = synthetic_400k();
        let mut moof = sectors_to_moof(&raw).unwrap();
        let last = moof.len() - 1;
        moof[last] ^= 0xFF;
        assert!(MoofReader::from_bytes(moof).is_err());
    }
}
