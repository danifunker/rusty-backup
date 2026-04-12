//! WOZ 1.0 / 2.0 disk image reader.
//!
//! WOZ is the modern preservation format for Apple II floppy disks, used by
//! all major emulators.  It stores the physical nibble/bitstream representation
//! of the disk, which means we need to decode GCR encoding back to logical
//! sectors before the data can be used by filesystem readers.
//!
//! This module supports:
//! - WOZ 1.0 and WOZ 2.0 container parsing
//! - 5.25" disk denibblization (6-and-2 GCR, 16-sector format)
//! - 3.5" disk denibblization (GCR, variable-speed zones)
//! - Presents decoded data as a `Read + Seek` stream in ProDOS block order

use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{bail, Context, Result};

// ─────────────────────────── WOZ container parsing ──────────────────────────

const WOZ1_MAGIC: &[u8; 4] = b"WOZ1";
const WOZ2_MAGIC: &[u8; 4] = b"WOZ2";
/// High-bit verification + line-ending check bytes after magic.
const HIGH_BIT_CHECK: [u8; 4] = [0xFF, 0x0A, 0x0D, 0x0A];

const CHUNK_INFO: &[u8; 4] = b"INFO";
const CHUNK_TMAP: &[u8; 4] = b"TMAP";
const CHUNK_TRKS: &[u8; 4] = b"TRKS";

/// Disk type from INFO chunk.
const DISK_TYPE_525: u8 = 1;
const DISK_TYPE_35: u8 = 2;

/// Parsed WOZ file — we extract just what we need for sector decoding.
struct WozFile {
    version: u8, // 1 or 2
    disk_type: u8,
    tmap: [u8; 160],
    /// For WOZ1: raw track data (6656 bytes per track entry).
    /// For WOZ2: the entire TRKS chunk data (TRK descriptors + bit data).
    trks_data: Vec<u8>,
    /// For WOZ2: offset of the TRKS chunk within the file (to compute block offsets).
    #[allow(dead_code)]
    trks_chunk_offset: usize,
    /// Full file data (needed for WOZ2 block-based track references).
    file_data: Vec<u8>,
}

/// Parse a WOZ file from raw bytes.
fn parse_woz(data: Vec<u8>) -> Result<WozFile> {
    if data.len() < 12 {
        bail!("WOZ file too small");
    }

    let version = if &data[0..4] == WOZ1_MAGIC {
        1
    } else if &data[0..4] == WOZ2_MAGIC {
        2
    } else {
        bail!("not a WOZ file");
    };

    if data[4..8] != HIGH_BIT_CHECK {
        bail!("WOZ high-bit verification failed (file may be corrupted or text-mode transferred)");
    }

    // Parse chunks starting at offset 12
    let mut disk_type = 0u8;
    let mut tmap = [0xFFu8; 160];
    let mut trks_data = Vec::new();
    let mut trks_chunk_offset = 0usize;
    let mut found_info = false;
    let mut found_tmap = false;
    let mut found_trks = false;

    let mut pos = 12;
    while pos + 8 <= data.len() {
        let chunk_id = &data[pos..pos + 4];
        let chunk_size =
            u32::from_le_bytes([data[pos + 4], data[pos + 5], data[pos + 6], data[pos + 7]])
                as usize;
        let chunk_data_start = pos + 8;
        let chunk_data_end = chunk_data_start + chunk_size;

        if chunk_data_end > data.len() {
            break; // truncated chunk
        }

        if chunk_id == CHUNK_INFO {
            if chunk_size >= 2 {
                disk_type = data[chunk_data_start + 1]; // offset 1 within INFO = disk_type
                found_info = true;
            }
        } else if chunk_id == CHUNK_TMAP {
            if chunk_size >= 160 {
                tmap.copy_from_slice(&data[chunk_data_start..chunk_data_start + 160]);
                found_tmap = true;
            }
        } else if chunk_id == CHUNK_TRKS {
            trks_data = data[chunk_data_start..chunk_data_end].to_vec();
            trks_chunk_offset = chunk_data_start;
            found_trks = true;
        }

        pos = chunk_data_end;
    }

    if !found_info {
        bail!("WOZ file missing INFO chunk");
    }
    if !found_tmap {
        bail!("WOZ file missing TMAP chunk");
    }
    if !found_trks {
        bail!("WOZ file missing TRKS chunk");
    }

    Ok(WozFile {
        version,
        disk_type,
        tmap,
        trks_data,
        trks_chunk_offset,
        file_data: data,
    })
}

/// Get the bitstream for a given TMAP index.
/// Returns (bitstream_slice, bit_count).
fn get_track_bits<'a>(woz: &'a WozFile, tmap_idx: u8) -> Option<(&'a [u8], u32)> {
    if tmap_idx == 0xFF {
        return None;
    }
    let idx = tmap_idx as usize;

    if woz.version == 1 {
        // WOZ 1.0: each track is 6656 bytes within the TRKS chunk
        let track_offset = idx * 6656;
        if track_offset + 6656 > woz.trks_data.len() {
            return None;
        }
        let bytes_used = u16::from_le_bytes([
            woz.trks_data[track_offset + 6646],
            woz.trks_data[track_offset + 6647],
        ]) as usize;
        let bit_count = u16::from_le_bytes([
            woz.trks_data[track_offset + 6648],
            woz.trks_data[track_offset + 6649],
        ]) as u32;
        let end = track_offset + bytes_used.min(6646);
        Some((&woz.trks_data[track_offset..end], bit_count))
    } else {
        // WOZ 2.0: TRK descriptor array (160 × 8 bytes), then bit data at block offsets
        let desc_offset = idx * 8;
        if desc_offset + 8 > woz.trks_data.len() {
            return None;
        }
        let starting_block =
            u16::from_le_bytes([woz.trks_data[desc_offset], woz.trks_data[desc_offset + 1]])
                as usize;
        let block_count = u16::from_le_bytes([
            woz.trks_data[desc_offset + 2],
            woz.trks_data[desc_offset + 3],
        ]) as usize;
        let bit_count = u32::from_le_bytes([
            woz.trks_data[desc_offset + 4],
            woz.trks_data[desc_offset + 5],
            woz.trks_data[desc_offset + 6],
            woz.trks_data[desc_offset + 7],
        ]);

        if starting_block == 0 || block_count == 0 {
            return None;
        }

        // Block offsets are relative to the start of the file
        let byte_offset = starting_block * 512;
        let byte_length = block_count * 512;
        if byte_offset + byte_length > woz.file_data.len() {
            return None;
        }

        Some((
            &woz.file_data[byte_offset..byte_offset + byte_length],
            bit_count,
        ))
    }
}

// ──────────────────────── 5.25" GCR 6-and-2 decoding ───────────────────────

/// 6-and-2 GCR decode table.  Maps disk nibble values (0x00-0xFF) to 6-bit
/// values (0x00-0x3F).  Invalid nibbles map to 0xFF.
///
/// Valid disk nibbles have the property that they have no more than one pair of
/// consecutive zero bits when the high bit is 1.  There are exactly 64 such
/// values in the range 0x96-0xFF.
const DECODE_62: [u8; 256] = {
    let mut table = [0xFFu8; 256];
    // The 64 valid nibble values in order, mapping to 6-bit values 0x00..0x3F
    let valid: [u8; 64] = [
        0x96, 0x97, 0x9A, 0x9B, 0x9D, 0x9E, 0x9F, 0xA6, 0xA7, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB2,
        0xB3, 0xB4, 0xB5, 0xB6, 0xB7, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xCB, 0xCD, 0xCE,
        0xCF, 0xD3, 0xD6, 0xD7, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE5, 0xE6, 0xE7, 0xE9,
        0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF9, 0xFA, 0xFB,
        0xFC, 0xFD, 0xFE, 0xFF,
    ];
    let mut i = 0;
    while i < 64 {
        table[valid[i] as usize] = i as u8;
        i += 1;
    }
    table
};

/// A stream reader that reads individual bits from a byte buffer.
struct BitStream<'a> {
    data: &'a [u8],
    bit_count: u32,
    pos: u32, // current bit position
}

impl<'a> BitStream<'a> {
    fn new(data: &'a [u8], bit_count: u32) -> Self {
        Self {
            data,
            bit_count,
            pos: 0,
        }
    }

    /// Read the next bit, wrapping around if we hit the end.
    #[inline]
    fn next_bit(&mut self) -> u8 {
        if self.bit_count == 0 {
            return 0;
        }
        let byte_idx = (self.pos / 8) as usize;
        let bit_idx = 7 - (self.pos % 8); // MSB first
        self.pos = (self.pos + 1) % self.bit_count;
        if byte_idx < self.data.len() {
            (self.data[byte_idx] >> bit_idx) & 1
        } else {
            0
        }
    }

    /// Read the next byte by accumulating 8 bits, synchronizing on high bit.
    /// Returns None if we can't find a sync byte within a reasonable scan.
    fn next_nibble(&mut self) -> Option<u8> {
        // Read bits until we get a 1 bit (sync), then read 7 more
        let limit = self.bit_count * 2; // prevent infinite loop
        let mut scanned = 0u32;
        while scanned < limit {
            if self.next_bit() == 1 {
                let mut byte = 0x80u8;
                for i in (0..7).rev() {
                    byte |= self.next_bit() << i;
                }
                return Some(byte);
            }
            scanned += 1;
        }
        None
    }

    /// Read a raw byte (8 bits, no sync).
    #[allow(dead_code)]
    fn read_byte(&mut self) -> u8 {
        let mut byte = 0u8;
        for i in (0..8).rev() {
            byte |= self.next_bit() << i;
        }
        byte
    }
}

/// Decode a 4-and-4 encoded byte pair from the bitstream.
/// 4-and-4 stores each byte as two disk bytes: odd bits then even bits.
fn read_44(stream: &mut BitStream) -> Option<u8> {
    let odd = stream.next_nibble()?;
    let even = stream.next_nibble()?;
    Some(((odd << 1) | 1) & even)
}

/// Sector address field parsed from the bitstream.
struct AddressField {
    #[allow(dead_code)]
    track: u8,
    sector: u8,
}

/// Scan for an address field (D5 AA 96 prologue).
/// Returns the parsed address field or None if not found within the track.
fn find_address_field(stream: &mut BitStream) -> Option<AddressField> {
    let limit = stream.bit_count * 2;
    let mut scanned = 0u32;

    loop {
        if scanned > limit {
            return None;
        }
        let b = stream.next_nibble()?;
        scanned += 8;
        if b != 0xD5 {
            continue;
        }
        let b2 = stream.next_nibble()?;
        scanned += 8;
        if b2 != 0xAA {
            continue;
        }
        let b3 = stream.next_nibble()?;
        scanned += 8;
        if b3 != 0x96 {
            continue;
        }

        // Found address prologue — read 4-and-4 fields
        let _volume = read_44(stream)?;
        let track = read_44(stream)?;
        let sector = read_44(stream)?;
        let _checksum = read_44(stream)?;

        return Some(AddressField { track, sector });
    }
}

/// Find the data field (D5 AA AD prologue) that follows an address field.
fn find_data_prologue(stream: &mut BitStream) -> bool {
    // The data field follows shortly after the address field epilogue.
    // Scan for D5 AA AD within a reasonable distance.
    for _ in 0..200 {
        if let Some(b) = stream.next_nibble() {
            if b == 0xD5 {
                if let Some(b2) = stream.next_nibble() {
                    if b2 == 0xAA {
                        if let Some(b3) = stream.next_nibble() {
                            if b3 == 0xAD {
                                return true;
                            }
                        }
                    }
                }
            }
        } else {
            return false;
        }
    }
    false
}

/// Decode 342 disk nibbles into 256 data bytes using 6-and-2 decoding.
///
/// The disk stores 342 nibbles:
/// - First 86 nibbles: auxiliary buffer (low 2 bits of bytes 0-255, packed 3 per nibble)
/// - Next 256 nibbles: primary buffer (high 6 bits of bytes 0-255)
/// Then 1 checksum nibble.
///
/// Each nibble is XOR'd with the previous to form a running checksum.
fn decode_data_field(stream: &mut BitStream) -> Option<[u8; 256]> {
    // Read 342 + 1 nibbles (data + checksum)
    let mut raw = [0u8; 343];
    for byte in raw.iter_mut() {
        let nib = stream.next_nibble()?;
        let decoded = DECODE_62[nib as usize];
        if decoded == 0xFF {
            return None; // invalid nibble
        }
        *byte = decoded;
    }

    // De-XOR: each value was XOR'd with the previous
    let mut prev = 0u8;
    for byte in raw.iter_mut() {
        *byte ^= prev;
        prev = *byte;
    }

    // Checksum validation: final value should be 0
    if raw[342] != 0 {
        // Bad checksum — still try to return data (some copy-protected disks)
    }

    // Reconstruct 256 bytes from auxiliary (86) + primary (256) buffers
    let aux = &raw[..86];
    let primary = &raw[86..342];

    let mut result = [0u8; 256];
    for i in 0..256 {
        let hi6 = primary[i]; // high 6 bits
                              // Auxiliary byte index: each aux byte holds 2 bits for 3 data bytes
        let aux_idx = i % 86;
        let aux_shift = (i / 86) * 2; // 0, 2, or 4 (but only 0 and 2 are used for 256 bytes)
        let lo2 = (aux[aux_idx] >> aux_shift) & 0x03;
        result[i] = (hi6 << 2) | lo2;
    }

    Some(result)
}

/// Decode all sectors from a 5.25" track bitstream.
/// Returns an array of 16 sectors, each 256 bytes. Missing sectors are zeroed.
fn decode_525_track(track_data: &[u8], bit_count: u32) -> [Option<[u8; 256]>; 16] {
    let mut sectors: [Option<[u8; 256]>; 16] = [None; 16];
    let mut stream = BitStream::new(track_data, bit_count);
    let mut found = 0u8;

    // We need to find 16 sectors. Scan through the entire track bitstream.
    // Allow enough iterations for all sectors plus retries.
    for _ in 0..64 {
        if found == 16 {
            break;
        }

        let addr = match find_address_field(&mut stream) {
            Some(a) => a,
            None => break,
        };

        if addr.sector >= 16 {
            continue; // invalid sector number
        }

        if sectors[addr.sector as usize].is_some() {
            continue; // already decoded this sector
        }

        if !find_data_prologue(&mut stream) {
            continue;
        }

        if let Some(data) = decode_data_field(&mut stream) {
            sectors[addr.sector as usize] = Some(data);
            found += 1;
        }
    }

    sectors
}

/// Physical sector to ProDOS-order position mapping for 5.25" disks.
///
/// A `.po` (ProDOS-order) file stores sectors in this physical order:
///   position 0=phys0, 1=phys2, 2=phys4, ..., 7=phys14, 8=phys1, 9=phys3, ..., 15=phys15
///
/// So when we read physical sector N from a WOZ address field, we place its
/// 256 bytes at position PHYS_TO_PRODOS[N] within the track's output.
const PHYS_TO_PRODOS: [usize; 16] = [0, 8, 1, 9, 2, 10, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15];

/// Decode a complete 5.25" WOZ image into ProDOS-order sector data.
/// Returns 35 tracks × 16 sectors × 256 bytes = 143,360 bytes.
fn decode_525_woz(woz: &WozFile) -> Result<Vec<u8>> {
    let mut output = vec![0u8; 143_360];

    for track_num in 0u8..35 {
        // TMAP: whole tracks are at quarter-track indices 0, 4, 8, ...
        let tmap_idx = woz.tmap[track_num as usize * 4];
        let sectors = if let Some((track_data, bit_count)) = get_track_bits(woz, tmap_idx) {
            decode_525_track(track_data, bit_count)
        } else {
            // Empty track — leave as zeros
            continue;
        };

        // Arrange sectors in ProDOS order
        for phys_sector in 0..16u8 {
            if let Some(ref data) = sectors[phys_sector as usize] {
                let prodos_pos = PHYS_TO_PRODOS[phys_sector as usize];
                let out_offset = (track_num as usize * 16 + prodos_pos) * 256;
                output[out_offset..out_offset + 256].copy_from_slice(data);
            }
        }
    }

    Ok(output)
}

// ──────────────────────── 3.5" GCR decoding ─────────────────────────────────

/// Sectors per track for each speed zone on 3.5" disks.
/// Zone 0 (tracks 0-15): 12 sectors, zone 1 (16-31): 11, zone 2 (32-47): 10,
/// zone 3 (48-63): 9, zone 4 (64-79): 8.
const SECTORS_PER_ZONE: [usize; 5] = [12, 11, 10, 9, 8];

/// Get the speed zone for a given track number (0-79).
fn speed_zone(track: usize) -> usize {
    track / 16
}

/// Number of sectors per track for a 3.5" disk.
fn sectors_for_track(track: usize) -> usize {
    SECTORS_PER_ZONE[speed_zone(track).min(4)]
}

/// Total sectors on one side of an 80-track 3.5" disk.
const TOTAL_SECTORS_35: usize = 800; // 12*16 + 11*16 + 10*16 + 9*16 + 8*16

/// 3.5" GCR decode table — same as 5.25" (6-and-2 encoding).
/// The nibble encoding is identical; what differs is the sector format.

/// Find a 3.5" address field.
/// 3.5" address fields use prologue D5 AA 96, same as 5.25" but with different
/// field semantics: track, sector, side, format (each as single nibbles, not 4-and-4).
struct AddressField35 {
    #[allow(dead_code)]
    track: u8,
    sector: u8,
    #[allow(dead_code)]
    side: u8,
}

fn find_address_field_35(stream: &mut BitStream) -> Option<AddressField35> {
    let limit = stream.bit_count * 2;
    let mut scanned = 0u32;

    loop {
        if scanned > limit {
            return None;
        }
        let b = stream.next_nibble()?;
        scanned += 8;
        if b != 0xD5 {
            continue;
        }
        let b2 = stream.next_nibble()?;
        scanned += 8;
        if b2 != 0xAA {
            continue;
        }
        let b3 = stream.next_nibble()?;
        scanned += 8;
        if b3 != 0x96 {
            continue;
        }

        // 3.5" address field: track, sector, side, format, checksum
        let track_nib = stream.next_nibble()?;
        let sector_nib = stream.next_nibble()?;
        let side_nib = stream.next_nibble()?;
        let _format_nib = stream.next_nibble()?;
        let _checksum = stream.next_nibble()?;

        // Decode from disk nibbles
        let track = DECODE_62[track_nib as usize];
        let sector = DECODE_62[sector_nib as usize];
        let side = DECODE_62[side_nib as usize];

        if track == 0xFF || sector == 0xFF || side == 0xFF {
            continue;
        }

        return Some(AddressField35 {
            track,
            sector,
            side: side & 1,
        });
    }
}

/// Decode a 3.5" data field (524 bytes: 12 tag + 512 data).
///
/// Sony GCR 3.5" encoding stores 524 bytes as 700 GCR nibbles read in
/// 175 groups of 4.  Each group contains one auxiliary nibble (high 2 bits
/// for 3 bytes) followed by three primary nibbles (low 6 bits each).
/// A three-register rotating checksum (c1, c2, c3) is applied during
/// decoding — there is no simple XOR chain like 5.25" disks.
///
/// Returns the 512 data bytes (12-byte tag prefix is discarded).
fn decode_data_field_35(stream: &mut BitStream) -> Option<[u8; 512]> {
    // After D5 AA AD prologue, the first nibble is the sector number — skip it.
    let _sector_nib = stream.next_nibble()?;
    let _sector = DECODE_62[_sector_nib as usize];
    if _sector == 0xFF {
        return None;
    }

    // Read 700 nibbles as 175 groups of 4 (aux, primary0, primary1, primary2).
    let mut nibble_groups = Vec::with_capacity(175);
    for _ in 0..175 {
        let n0 = DECODE_62[stream.next_nibble()? as usize];
        let n1 = DECODE_62[stream.next_nibble()? as usize];
        let n2 = DECODE_62[stream.next_nibble()? as usize];
        let n3 = DECODE_62[stream.next_nibble()? as usize];
        if n0 == 0xFF || n1 == 0xFF || n2 == 0xFF || n3 == 0xFF {
            return None;
        }
        nibble_groups.push((n0, n1, n2, n3));
    }

    // Reconstruct bytes: each group of 4 nibbles → 3 data bytes.
    // aux bits [5:4] → high 2 of byte 0, [3:2] → byte 1, [1:0] → byte 2.
    // Three-register rotating checksum decodes each byte.
    let mut c1: u32 = 0;
    let mut c2: u32 = 0;
    let mut c3: u32 = 0;
    let mut sector_data = Vec::with_capacity(525);

    for &(aux, p0, p1, p2) in &nibble_groups {
        let d0 = (p0 & 0x3F) | ((aux << 2) & 0xC0);
        let d1 = (p1 & 0x3F) | ((aux << 4) & 0xC0);
        let d2 = (p2 & 0x3F) | ((aux << 6) & 0xC0);

        // Byte 0: XOR with c1
        c1 = (c1 & 0xFF) << 1;
        if c1 > 0xFF {
            c1 -= 0xFF;
            c3 += 1;
        }
        let b0 = (d0 as u32) ^ c1;
        c3 += b0;
        sector_data.push(b0 as u8);

        // Byte 1: XOR with c3
        if c3 > 0xFF {
            c3 &= 0xFF;
            c2 += 1;
        }
        let b1 = (d1 as u32) ^ c3;
        c2 += b1;
        sector_data.push(b1 as u8);

        // Byte 2: XOR with c2
        if c2 > 0xFF {
            c2 &= 0xFF;
            c1 += 1;
        }
        let b2 = (d2 as u32) ^ c2;
        c1 += b2;
        sector_data.push(b2 as u8);
    }

    // 175 groups × 3 bytes = 525; we need 524 (12 tag + 512 data).
    if sector_data.len() < 524 {
        return None;
    }

    // Return just the 512 data bytes (skip 12-byte tag prefix)
    let mut result = [0u8; 512];
    result.copy_from_slice(&sector_data[12..524]);
    Some(result)
}

/// Decode all sectors from a 3.5" track bitstream.
fn decode_35_track(
    track_data: &[u8],
    bit_count: u32,
    expected_sectors: usize,
) -> Vec<Option<[u8; 512]>> {
    let mut sectors: Vec<Option<[u8; 512]>> = vec![None; expected_sectors];
    let mut stream = BitStream::new(track_data, bit_count);
    let mut found = 0usize;

    for _ in 0..expected_sectors * 4 {
        if found >= expected_sectors {
            break;
        }

        let addr = match find_address_field_35(&mut stream) {
            Some(a) => a,
            None => break,
        };

        let sector = addr.sector as usize;
        if sector >= expected_sectors {
            continue;
        }

        if sectors[sector].is_some() {
            continue;
        }

        if !find_data_prologue(&mut stream) {
            continue;
        }

        if let Some(data) = decode_data_field_35(&mut stream) {
            sectors[sector] = Some(data);
            found += 1;
        }
    }

    sectors
}

/// Decode a complete 3.5" WOZ image into flat sector data.
/// Returns up to 80 tracks × 2 sides × variable sectors × 512 bytes.
/// Single-sided (400K) = 800 sectors, double-sided (800K) = 1600 sectors.
fn decode_35_woz(woz: &WozFile) -> Result<Vec<u8>> {
    // Allocate for both sides; we'll truncate if single-sided.
    let mut output = vec![0u8; TOTAL_SECTORS_35 * 2 * 512]; // 1600 sectors max
    let mut out_pos = 0usize;

    // Side 0
    for track_num in 0..80usize {
        let num_sectors = sectors_for_track(track_num);
        let tmap_idx = woz.tmap[track_num * 2];

        if let Some((track_data, bit_count)) = get_track_bits(woz, tmap_idx) {
            let sectors = decode_35_track(track_data, bit_count, num_sectors);
            for sector in &sectors {
                if let Some(ref data) = sector {
                    if out_pos + 512 <= output.len() {
                        output[out_pos..out_pos + 512].copy_from_slice(data);
                    }
                }
                out_pos += 512;
            }
        } else {
            out_pos += num_sectors * 512;
        }
    }

    // Side 1
    for track_num in 0..80usize {
        let num_sectors = sectors_for_track(track_num);
        let tmap_idx = woz.tmap[track_num * 2 + 1];

        if tmap_idx == 0xFF {
            out_pos += num_sectors * 512;
            continue;
        }

        if let Some((track_data, bit_count)) = get_track_bits(woz, tmap_idx) {
            let sectors = decode_35_track(track_data, bit_count, num_sectors);
            for sector in &sectors {
                if let Some(ref data) = sector {
                    if out_pos + 512 <= output.len() {
                        output[out_pos..out_pos + 512].copy_from_slice(data);
                    }
                }
                out_pos += 512;
            }
        } else {
            out_pos += num_sectors * 512;
        }
    }

    // Trim to actual output size
    output.truncate(out_pos.min(output.len()));

    Ok(output)
}

// ──────────────────────────── WozReader ─────────────────────────────────────

/// A `Read + Seek` adapter that presents decoded WOZ floppy data as a flat
/// byte stream in ProDOS/logical sector order.
///
/// Since floppy images are small (140KB-800KB), we decode the entire image at
/// open time and keep it in memory.
pub struct WozReader {
    data: Vec<u8>,
    position: u64,
    disk_type: u8,
}

impl WozReader {
    /// Open and decode a WOZ file.
    pub fn open(path: &Path) -> Result<Self> {
        let raw = std::fs::read(path)
            .with_context(|| format!("failed to read WOZ file: {}", path.display()))?;
        Self::from_bytes(raw)
    }

    /// Decode a WOZ file from raw bytes.
    pub fn from_bytes(raw: Vec<u8>) -> Result<Self> {
        let woz = parse_woz(raw)?;
        let disk_type = woz.disk_type;

        let data = match disk_type {
            DISK_TYPE_525 => decode_525_woz(&woz)?,
            DISK_TYPE_35 => decode_35_woz(&woz)?,
            _ => bail!("unsupported WOZ disk type: {}", disk_type),
        };

        Ok(Self {
            data,
            position: 0,
            disk_type,
        })
    }

    /// Total decoded data size in bytes.
    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    /// Human-readable disk type description.
    pub fn disk_type_name(&self) -> &'static str {
        match self.disk_type {
            DISK_TYPE_525 => "5.25\" (140K)",
            DISK_TYPE_35 => "3.5\" (800K)",
            _ => "Unknown",
        }
    }
}

impl Read for WozReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.data.len() as u64 - self.position;
        if remaining == 0 {
            return Ok(0);
        }
        let to_read = (buf.len() as u64).min(remaining) as usize;
        let pos = self.position as usize;
        buf[..to_read].copy_from_slice(&self.data[pos..pos + to_read]);
        self.position += to_read as u64;
        Ok(to_read)
    }
}

impl Seek for WozReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::Current(delta) => self.position as i64 + delta,
            SeekFrom::End(delta) => self.data.len() as i64 + delta,
        };
        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }
        self.position = (new_pos as u64).min(self.data.len() as u64);
        Ok(self.position)
    }
}

/// Check if raw bytes begin with a WOZ magic signature.
pub fn is_woz(data: &[u8]) -> bool {
    data.len() >= 8
        && (&data[0..4] == WOZ1_MAGIC || &data[0..4] == WOZ2_MAGIC)
        && data[4..8] == HIGH_BIT_CHECK
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_62_table() {
        // Verify the table has exactly 64 valid entries
        let valid_count = DECODE_62.iter().filter(|&&v| v != 0xFF).count();
        assert_eq!(valid_count, 64);

        // Verify each valid entry maps to a unique 6-bit value
        let mut seen = [false; 64];
        for &v in &DECODE_62 {
            if v != 0xFF {
                assert!(v < 64, "decoded value {} out of range", v);
                assert!(!seen[v as usize], "duplicate decoded value {}", v);
                seen[v as usize] = true;
            }
        }
    }

    #[test]
    fn test_is_woz() {
        let mut woz1 = vec![0u8; 12];
        woz1[..4].copy_from_slice(b"WOZ1");
        woz1[4..8].copy_from_slice(&HIGH_BIT_CHECK);
        assert!(is_woz(&woz1));

        let mut woz2 = vec![0u8; 12];
        woz2[..4].copy_from_slice(b"WOZ2");
        woz2[4..8].copy_from_slice(&HIGH_BIT_CHECK);
        assert!(is_woz(&woz2));

        assert!(!is_woz(b"WOZ3\xFF\x0A\x0D\x0A"));
        assert!(!is_woz(b"WOZ1\x00\x0A\x0D\x0A")); // bad high-bit
        assert!(!is_woz(b"WOZ")); // too short
    }

    #[test]
    fn test_bitstream_read() {
        // 0b10110100 = 0xB4
        let data = [0xB4];
        let mut stream = BitStream::new(&data, 8);
        assert_eq!(stream.next_bit(), 1);
        assert_eq!(stream.next_bit(), 0);
        assert_eq!(stream.next_bit(), 1);
        assert_eq!(stream.next_bit(), 1);
        assert_eq!(stream.next_bit(), 0);
        assert_eq!(stream.next_bit(), 1);
        assert_eq!(stream.next_bit(), 0);
        assert_eq!(stream.next_bit(), 0);
        // Wraps around
        assert_eq!(stream.next_bit(), 1);
    }

    #[test]
    fn test_phys_to_prodos_is_permutation() {
        let mut seen = [false; 16];
        for &s in &PHYS_TO_PRODOS {
            assert!(s < 16);
            assert!(!seen[s], "duplicate in PHYS_TO_PRODOS");
            seen[s] = true;
        }
    }

    #[test]
    fn test_woz_reader_seek() {
        let reader = WozReader {
            data: vec![0xAA; 1000],
            position: 0,
            disk_type: DISK_TYPE_525,
        };
        assert_eq!(reader.len(), 1000);
    }
}

/// Tests that require real disk images from ~/Downloads (skipped if absent).
#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_35_woz_decodes_filesystem() {
        let path = std::path::Path::new(env!("HOME")).join("Downloads/4th & Inches IIgs.woz");
        if !path.exists() {
            return;
        }

        let reader = WozReader::open(&path).expect("failed to open WOZ file");
        assert_eq!(
            reader.len(),
            819200,
            "expected 800K double-sided 3.5\" image"
        );

        let data = &reader.data;
        let non_zero = data.iter().filter(|&&b| b != 0).count();
        assert!(non_zero > 1000, "decoded data is mostly zeros");

        // ProDOS volume directory key block is at block 2 (offset 1024)
        assert!(
            crate::rbformats::interleave::has_prodos_volume_header(&data[1024..]),
            "expected ProDOS volume header at offset 1024"
        );
    }
}
