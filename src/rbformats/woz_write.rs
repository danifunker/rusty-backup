//! WOZ 2.0 disk image writer.
//!
//! Regenerates clean WOZ2 files from raw sector buffers.  Used both for
//! exporting compatible disk images to WOZ format and for saving edits back
//! to existing WOZ files.
//!
//! Unlike CiderPress2's bit-level patcher (`CircularBitBuffer`), this writer
//! always builds tracks from scratch.  That trades preservation of original
//! bitstream artifacts for a much simpler implementation — appropriate for a
//! backup tool that doesn't need emulator-perfect copy-protection support.
//!
//! See `docs/woz_writing.md` for the full implementation plan.

// ─────────────────────────────── BitWriter ─────────────────────────────────

/// Append-only bit writer.  Bits are packed MSB-first into a byte buffer.
///
/// The buffer grows as needed and freshly-extended bytes are zero-initialized,
/// which lets `write_byte` skip explicit zero-padding when widths exceed 8.
pub struct BitWriter {
    buf: Vec<u8>,
    bit_pos: usize,
}

impl BitWriter {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            bit_pos: 0,
        }
    }

    pub fn with_capacity(byte_capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(byte_capacity),
            bit_pos: 0,
        }
    }

    /// Total bits written so far.
    pub fn bit_count(&self) -> u32 {
        self.bit_pos as u32
    }

    /// Bytes used so far (rounded up to whole bytes).
    pub fn byte_count(&self) -> usize {
        (self.bit_pos + 7) / 8
    }

    /// Consume the writer and return the underlying buffer.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// Borrow the underlying buffer.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    fn ensure_byte(&mut self, byte_idx: usize) {
        if self.buf.len() <= byte_idx {
            self.buf.resize(byte_idx + 1, 0);
        }
    }

    /// Write a single bit (only the LSB of `bit` is used).
    pub fn write_bit(&mut self, bit: u8) {
        let byte_idx = self.bit_pos >> 3;
        let bit_idx = 7 - (self.bit_pos & 7);
        self.ensure_byte(byte_idx);
        if bit & 1 != 0 {
            self.buf[byte_idx] |= 1 << bit_idx;
        }
        self.bit_pos += 1;
    }

    /// Write an 8-bit byte followed by `width - 8` zero bits.  `width` must be
    /// 8, 9, or 10.  Width 10 with `value = 0xFF` is the WOZ self-sync pattern.
    pub fn write_byte(&mut self, value: u8, width: u32) {
        assert!((8..=10).contains(&width), "width must be 8..=10");
        let bit_off = self.bit_pos & 7;
        let byte_idx = self.bit_pos >> 3;
        if bit_off == 0 {
            self.ensure_byte(byte_idx);
            self.buf[byte_idx] = value;
        } else {
            self.ensure_byte(byte_idx + 1);
            self.buf[byte_idx] |= value >> bit_off;
            self.buf[byte_idx + 1] = value << (8 - bit_off);
        }
        self.bit_pos += 8;
        let pad = (width - 8) as usize;
        if pad > 0 {
            let end = self.bit_pos + pad;
            self.ensure_byte((end - 1) >> 3);
            self.bit_pos = end;
        }
    }

    /// Write an 8-bit byte (no padding).
    pub fn write_octet(&mut self, value: u8) {
        self.write_byte(value, 8);
    }

    /// Write a slice of bytes as 8-bit values.
    pub fn write_octets(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.write_octet(b);
        }
    }

    /// Write `count` self-sync FF patterns (10 bits each: 0xFF + two zero bits).
    pub fn write_self_sync(&mut self, count: usize) {
        for _ in 0..count {
            self.write_byte(0xFF, 10);
        }
    }

    /// Pad the buffer with self-sync until it reaches `total_bits` bits.
    /// Trailing bits that don't fit a full 10-bit pattern are filled with 1s.
    pub fn fill_self_sync(&mut self, total_bits: u32) {
        while (self.bit_pos as u32) + 10 <= total_bits {
            self.write_byte(0xFF, 10);
        }
        while (self.bit_pos as u32) < total_bits {
            self.write_bit(1);
        }
    }
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

// ───────────────────── 6-and-2 GCR encode table ────────────────────────────

/// 6-and-2 GCR encode table.  Maps 6-bit values (0x00..0x3F) to disk nibbles.
/// This is the inverse of `DECODE_62` in `woz.rs` — same 64 valid nibble
/// values, in the same order.
pub(crate) const ENCODE_62: [u8; 64] = [
    0x96, 0x97, 0x9A, 0x9B, 0x9D, 0x9E, 0x9F, 0xA6, 0xA7, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB2, 0xB3,
    0xB4, 0xB5, 0xB6, 0xB7, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF, 0xCB, 0xCD, 0xCE, 0xCF, 0xD3,
    0xD6, 0xD7, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE5, 0xE6, 0xE7, 0xE9, 0xEA, 0xEB, 0xEC,
    0xED, 0xEE, 0xEF, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF,
];

// ─────────────────────────── 4-and-4 helpers ──────────────────────────────

/// Encode the low (odd-indexed) bits of `val` into the first byte of a 4-and-4
/// pair.  Decode: `((lo << 1) | 0x01) & hi`.
#[inline]
pub(crate) fn to_44_lo(val: u8) -> u8 {
    (val >> 1) | 0xAA
}

/// Encode the high (even-indexed) bits of `val` into the second byte of a
/// 4-and-4 pair.
#[inline]
pub(crate) fn to_44_hi(val: u8) -> u8 {
    val | 0xAA
}

// ───────────────────── 5.25" track encoding ───────────────────────────────

/// Standard Apple II volume number for newly-formatted 5.25" disks.
pub const DEFAULT_VOLUME_525: u8 = 254;

/// Number of self-sync FF bytes between an address field and its data field.
const GAP2_525_SELF_SYNC: usize = 5;

/// Number of self-sync FF bytes preceding each sector (gap 3 / gap 1).
const GAP3_525_SELF_SYNC: usize = 20;

/// 5&2 chunk count: 86 auxiliary nibbles per 256-byte sector.
const CHUNK_62_256: usize = 86;

/// Default WOZ2 5.25" track buffer size (matches CiderPress2's DEFAULT_LENGTH_525).
/// 16 sectors of content occupy ~6308 bytes; remainder is gap fill past bit_count.
pub const DEFAULT_TRACK_LEN_525: usize = 6336;

/// Write the 5.25" address field: D5 AA 96 prologue, 4-and-4 volume/track/
/// sector/checksum, DE AA EB epilogue.  Address checksum seed is 0.
pub fn encode_address_field_525(w: &mut BitWriter, vol: u8, track: u8, sector: u8) {
    w.write_octet(0xD5);
    w.write_octet(0xAA);
    w.write_octet(0x96);
    w.write_octet(to_44_lo(vol));
    w.write_octet(to_44_hi(vol));
    w.write_octet(to_44_lo(track));
    w.write_octet(to_44_hi(track));
    w.write_octet(to_44_lo(sector));
    w.write_octet(to_44_hi(sector));
    let checksum = vol ^ track ^ sector;
    w.write_octet(to_44_lo(checksum));
    w.write_octet(to_44_hi(checksum));
    w.write_octet(0xDE);
    w.write_octet(0xAA);
    w.write_octet(0xEB);
}

/// Write the 5.25" data field: D5 AA AD prologue, 343 nibbles of 6-and-2
/// encoded data (86 auxiliary + 256 primary + 1 checksum) with XOR-chain
/// checksum, then DE AA EB epilogue.
///
/// Mirrors `EncodeSector62_256` in `CiderPress2/DiskArc/SectorCodec.cs:816`.
pub fn encode_data_field_525(w: &mut BitWriter, data: &[u8; 256]) {
    // Split each input byte into the high 6 bits (`top`) and the low 2 bits
    // packed (bit-reversed) into 86 auxiliary entries.
    let mut top = [0u8; 256];
    let mut twos = [0u8; CHUNK_62_256];

    let mut two_shift: u8 = 0;
    let mut two_posn: usize = CHUNK_62_256 - 1;
    for i in 0..256 {
        let val = data[i];
        top[i] = val >> 2;
        // The low 2 bits are written in swapped order (bit 0 → bit 1, bit 1 → bit 0).
        let swapped = ((val & 0x01) << 1) | ((val & 0x02) >> 1);
        twos[two_posn] |= swapped << two_shift;
        if two_posn == 0 {
            two_posn = CHUNK_62_256;
            two_shift += 2;
        }
        two_posn -= 1;
    }

    // Data prologue.
    w.write_octet(0xD5);
    w.write_octet(0xAA);
    w.write_octet(0xAD);

    // Auxiliary nibbles, XOR-chained from a zero seed (descending order).
    let mut checksum: u8 = 0;
    for i in (0..CHUNK_62_256).rev() {
        let chk_val = twos[i] ^ checksum;
        w.write_octet(ENCODE_62[(chk_val & 0x3F) as usize]);
        checksum = twos[i];
    }
    // Primary nibbles, XOR-chained continuing the same checksum.
    for i in 0..256 {
        w.write_octet(ENCODE_62[(top[i] ^ checksum) as usize]);
        checksum = top[i];
    }
    // Final checksum nibble.
    w.write_octet(ENCODE_62[checksum as usize]);

    // Data epilogue.
    w.write_octet(0xDE);
    w.write_octet(0xAA);
    w.write_octet(0xEB);
}

/// Generate a complete 5.25" track bitstream from 16 raw 256-byte sectors.
///
/// Layout per sector (in order written): gap3 (20 self-sync FF) → address
/// field → gap2 (5 self-sync FF) → data field.  Sectors are written in
/// physical (not logical) order — the caller is responsible for any
/// interleave or ProDOS↔physical mapping.
///
/// Returns the raw track bytes and the number of bits written (the track's
/// `bit_count` for the WOZ2 TRK descriptor).  The buffer length is rounded
/// up to whole bytes; the bitstream wraps at `bit_count`.
pub fn generate_track_525(vol: u8, track: u8, sectors: &[[u8; 256]; 16]) -> (Vec<u8>, u32) {
    let mut w = BitWriter::with_capacity(DEFAULT_TRACK_LEN_525);
    for sector in 0..16u8 {
        w.write_self_sync(GAP3_525_SELF_SYNC);
        encode_address_field_525(&mut w, vol, track, sector);
        w.write_self_sync(GAP2_525_SELF_SYNC);
        encode_data_field_525(&mut w, &sectors[sector as usize]);
    }
    let bit_count = w.bit_count();
    (w.into_bytes(), bit_count)
}

// ───────────────────── 3.5" track encoding ────────────────────────────────

/// Number of self-sync FF bytes in gap 1 (before the first sector).
const GAP1_35_SELF_SYNC: usize = 64;

/// Number of self-sync FF bytes between an address field and its data field.
const GAP2_35_SELF_SYNC: usize = 5;

/// Number of self-sync FF bytes preceding each sector (gap 3).
const GAP3_35_SELF_SYNC: usize = 36;

/// Number of 3-byte groups in 6-and-2 524-byte encoding: ceil(524 / 3) = 175.
const CHUNK_62_524: usize = 175;

/// Standard format byte for double-sided double-density 3.5" disks (interleave 2).
pub const FORMAT_35_DSDD: u8 = 0x22;

/// Standard format byte for single-sided double-density 3.5" disks (interleave 2).
pub const FORMAT_35_SSDD: u8 = 0x12;

/// Sectors per speed zone on a 3.5" disk.  Track / 16 selects the zone.
pub const SECTORS_PER_ZONE_35: [usize; 5] = [12, 11, 10, 9, 8];

/// Number of sectors on a given 3.5" track (0..=79).
pub fn sectors_for_track_35(track: u8) -> usize {
    SECTORS_PER_ZONE_35[(track as usize / 16).min(4)]
}

/// Generate the physical-to-logical sector mapping for a 3.5" track.
/// `table[physical_position] = logical_sector_number`.
///
/// Mirrors `GetInterleaveTable` in `CiderPress2/DiskArc/Disk/TrackInit.cs:252`.
pub fn interleave_table_35(num_sectors: usize, interleave: usize) -> Vec<u8> {
    assert!(interleave > 0, "interleave must be positive");
    let mut table = vec![0xFFu8; num_sectors];
    let mut offset = 0usize;
    for i in 0..num_sectors {
        if table[offset] != 0xFF {
            // Collision: shift one position forward.
            offset = (offset + 1) % num_sectors;
        }
        table[offset] = i as u8;
        offset = (offset + interleave) % num_sectors;
    }
    table
}

/// Write the 3.5" address field: D5 AA 96 prologue, 5 6-and-2 nibbles
/// (trkLow, sector, trkSide, format, checksum), DE AA epilog, pad byte.
///
/// Track number is split: low 6 bits → `trkLow`; high bits → low bits of
/// `trkSide`, with `side << 5` packed into bit 5.  Address checksum is the
/// XOR of the four field nibbles.
pub fn encode_address_field_35(w: &mut BitWriter, track: u8, sector: u8, side: u8, format: u8) {
    assert!(track < 80, "3.5\" track must be 0..80");
    assert!(sector < 64, "3.5\" sector must fit in 6 bits");
    assert!(side < 2, "3.5\" side must be 0 or 1");
    assert!(format < 64, "3.5\" format must fit in 6 bits");
    let trk_low = track & 0x3F;
    let trk_side = (side << 5) | (track >> 6);
    let checksum = trk_low ^ sector ^ trk_side ^ format;
    w.write_octet(0xD5);
    w.write_octet(0xAA);
    w.write_octet(0x96);
    w.write_octet(ENCODE_62[trk_low as usize]);
    w.write_octet(ENCODE_62[sector as usize]);
    w.write_octet(ENCODE_62[trk_side as usize]);
    w.write_octet(ENCODE_62[format as usize]);
    w.write_octet(ENCODE_62[(checksum & 0x3F) as usize]);
    w.write_octet(0xDE);
    w.write_octet(0xAA);
    w.write_octet(0xFF);
}

/// Write the 3.5" data field: D5 AA AD prologue, 6-and-2 sector nibble,
/// 699 GCR data nibbles (Sony three-register rotating checksum), 4 checksum
/// nibbles, DE AA epilog, pad byte.
///
/// Input is the full 524-byte tag+data buffer; the first 12 bytes are the
/// tag (typically zero for ProDOS/HFS) and the remaining 512 are user data.
///
/// Mirrors `EncodeSector62_524` in `CiderPress2/DiskArc/SectorCodec.cs:965`.
pub fn encode_data_field_35(w: &mut BitWriter, sector: u8, data: &[u8; 524]) {
    let mut part0 = [0u8; CHUNK_62_524];
    let mut part1 = [0u8; CHUNK_62_524];
    let mut part2 = [0u8; CHUNK_62_524];
    let (mut chk0, mut chk1, mut chk2): (u32, u32, u32) = (0, 0, 0);

    let mut idx = 0usize;
    let mut offset = 0usize;
    loop {
        // Rotate chk0 left by 1 (wrap-around carry).  If shift overflowed, the
        // rotated bit propagates into chk2 below.
        chk0 = (chk0 & 0xFF) << 1;
        if chk0 & 0x100 != 0 {
            chk0 += 1;
        }

        // Byte 0 of the group → part0, accumulates into chk2.
        let mut val = data[offset] as u32;
        offset += 1;
        chk2 += val;
        if chk0 & 0x100 != 0 {
            chk2 += 1;
            chk0 &= 0xFF;
        }
        part0[idx] = (val ^ chk0) as u8;

        // Byte 1 → part1, accumulates into chk1.
        val = data[offset] as u32;
        offset += 1;
        chk1 += val;
        if chk2 > 0xFF {
            chk1 += 1;
            chk2 &= 0xFF;
        }
        part1[idx] = (val ^ chk2) as u8;

        // Last group has only 2 input bytes (174 full groups × 3 + 2 = 524).
        if offset == 524 {
            break;
        }

        // Byte 2 → part2, accumulates into chk0.
        val = data[offset] as u32;
        offset += 1;
        chk0 += val;
        if chk1 > 0xFF {
            chk0 += 1;
            chk1 &= 0xFF;
        }
        part2[idx] = (val ^ chk1) as u8;
        idx += 1;
    }
    // The final group has no part2 input; encoder writes 3 nibbles instead of 4.
    part2[CHUNK_62_524 - 1] = 0;

    // Data prologue.
    w.write_octet(0xD5);
    w.write_octet(0xAA);
    w.write_octet(0xAD);

    // Sector number nibble (6-and-2 encoded).
    w.write_octet(ENCODE_62[(sector & 0x3F) as usize]);

    // 174 × 4-nibble groups + 1 × 3-nibble group = 699 nibbles total.
    for i in 0..CHUNK_62_524 {
        let twos = ((part0[i] & 0xC0) >> 2) | ((part1[i] & 0xC0) >> 4) | ((part2[i] & 0xC0) >> 6);
        w.write_octet(ENCODE_62[twos as usize]);
        w.write_octet(ENCODE_62[(part0[i] & 0x3F) as usize]);
        w.write_octet(ENCODE_62[(part1[i] & 0x3F) as usize]);
        if i != CHUNK_62_524 - 1 {
            w.write_octet(ENCODE_62[(part2[i] & 0x3F) as usize]);
        }
    }

    // 4-nibble checksum.
    let chktwos = (((chk0 & 0xC0) >> 6) | ((chk1 & 0xC0) >> 4) | ((chk2 & 0xC0) >> 2)) as u8;
    w.write_octet(ENCODE_62[chktwos as usize]);
    w.write_octet(ENCODE_62[(chk2 & 0x3F) as usize]);
    w.write_octet(ENCODE_62[(chk1 & 0x3F) as usize]);
    w.write_octet(ENCODE_62[(chk0 & 0x3F) as usize]);

    // Data epilog (3.5" uses 2-byte epilog) + pad byte.
    w.write_octet(0xDE);
    w.write_octet(0xAA);
    w.write_octet(0xFF);
}

/// Generate a complete 3.5" track bitstream from logical-order sector data.
///
/// `sectors[s]` provides the 512-byte payload of logical sector `s`; `None`
/// is encoded as 512 zeros (the 12-byte tag is always written as zero).
/// `format` low nibble is the interleave (2 for ProDOS/HFS, 4 for GS/OS).
///
/// Layout: gap1 (64 self-sync) → for each physical position p:
/// gap3 (36 self-sync) → addr field (carries logical sector itable[p]) →
/// gap2 (5 self-sync) → data field (contents of logical sector itable[p]).
///
/// Returns the raw track bytes and the bit count (the track's `bit_count`
/// for the WOZ2 TRK descriptor).
pub fn generate_track_35(
    track: u8,
    side: u8,
    format: u8,
    sectors: &[Option<[u8; 512]>],
) -> (Vec<u8>, u32) {
    let interleave = (format & 0x0F) as usize;
    assert!(
        interleave == 2 || interleave == 4,
        "format low nibble must be 2 or 4 (interleave)"
    );
    let num_sectors = sectors.len();
    assert!(
        (8..=12).contains(&num_sectors),
        "3.5\" sector count must be 8..=12"
    );

    // Each sector occupies ~772 bytes (with 10-bit self-sync); pre-size the buffer.
    let mut w = BitWriter::with_capacity(GAP1_35_SELF_SYNC + num_sectors * 800);
    w.write_self_sync(GAP1_35_SELF_SYNC);

    let itable = interleave_table_35(num_sectors, interleave);

    // Buffer that combines the 12-byte zero tag with the 512-byte sector data.
    let mut tagged = [0u8; 524];

    for &logical_sector in &itable {
        w.write_self_sync(GAP3_35_SELF_SYNC);
        encode_address_field_35(&mut w, track, logical_sector, side, format);
        w.write_self_sync(GAP2_35_SELF_SYNC);

        // Reset tag bytes (in case a previous iteration mutated them — defensive).
        for b in &mut tagged[..12] {
            *b = 0;
        }
        match &sectors[logical_sector as usize] {
            Some(data) => tagged[12..524].copy_from_slice(data),
            None => {
                for b in &mut tagged[12..524] {
                    *b = 0;
                }
            }
        }
        encode_data_field_35(&mut w, logical_sector, &tagged);
    }

    let bit_count = w.bit_count();
    (w.into_bytes(), bit_count)
}

// ───────────────────── WOZ2 container builder ─────────────────────────────

/// 8-byte WOZ2 file signature: "WOZ2" + high-bit/line-ending check.
const SIGNATURE_W2: [u8; 8] = [b'W', b'O', b'Z', b'2', 0xFF, 0x0A, 0x0D, 0x0A];

const HEADER_LEN: usize = 12;
const CHUNK_HDR_LEN: usize = 8;

const INFO_CHUNK_START: usize = HEADER_LEN; // 12
const INFO_DATA_START: usize = INFO_CHUNK_START + CHUNK_HDR_LEN; // 20
const INFO_LENGTH: usize = 60;

const TMAP_CHUNK_START: usize = INFO_DATA_START + INFO_LENGTH; // 80
const TMAP_DATA_START: usize = TMAP_CHUNK_START + CHUNK_HDR_LEN; // 88
const TMAP_LENGTH: usize = 160;

const TRKS_CHUNK_START: usize = TMAP_DATA_START + TMAP_LENGTH; // 248
const TRKS_DATA_START: usize = TRKS_CHUNK_START + CHUNK_HDR_LEN; // 256

/// 160 8-byte TRK descriptors.
const TRKS_DESC_LEN: usize = 160 * 8; // 1280
/// First track-data block lives at file byte 1536 (block 3).
const TRACK_BIT_DATA_START: usize = 1536;
const BLOCK_SIZE: usize = 512;

/// Disk type byte stored in the INFO chunk.
pub const DISK_TYPE_525: u8 = 1;
pub const DISK_TYPE_35: u8 = 2;

/// Maximum 5.25" track index that a TMAP entry may reference (40 = quarter-track of track 35).
const MAX_TRACK_525: u8 = 40;
/// Maximum 3.5" track index (80 tracks × 2 sides = 160 entries; per-side max is 80).
const MAX_TRACK_35: u8 = 80;

fn write_u16_le(buf: &mut [u8], offset: usize, value: u16) {
    buf[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn write_u32_le(buf: &mut [u8], offset: usize, value: u32) {
    buf[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

/// Write the INFO chunk (header + 60-byte payload) into `buf` at `INFO_CHUNK_START`.
///
/// `largest_track_blocks` is the largest per-track block count across the
/// generated tracks (1 block = 512 bytes); WOZ2 readers use this to size their
/// per-track buffers.  `creator` is a free-form 32-byte ASCII string; shorter
/// strings are right-padded with spaces.
fn write_info_chunk(
    buf: &mut [u8],
    disk_type: u8,
    sides: u8,
    largest_track_blocks: u16,
    creator: &str,
) {
    // Chunk header: ID = "INFO", length = 60.
    buf[INFO_CHUNK_START..INFO_CHUNK_START + 4].copy_from_slice(b"INFO");
    write_u32_le(buf, INFO_CHUNK_START + 4, INFO_LENGTH as u32);

    let p = INFO_DATA_START;
    buf[p] = 2; // version (WOZ2)
    buf[p + 1] = disk_type;
    buf[p + 2] = 0; // write_protected = false
    buf[p + 3] = 1; // synchronized = true (we emit consistent self-sync)
    buf[p + 4] = 1; // cleaned = true (regenerated, no random bits)

    // Creator: 32 bytes, space-padded ASCII.
    let creator_bytes = creator.as_bytes();
    let copy_len = creator_bytes.len().min(32);
    for b in &mut buf[p + 5..p + 5 + 32] {
        *b = b' ';
    }
    buf[p + 5..p + 5 + copy_len].copy_from_slice(&creator_bytes[..copy_len]);

    buf[p + 37] = sides;
    buf[p + 38] = 0; // boot_sector_format (0 = unknown / not boot disk)
    buf[p + 39] = if disk_type == DISK_TYPE_525 { 32 } else { 16 };
    write_u16_le(buf, p + 40, 1); // compatible_hardware bit 0 = Apple ][
    write_u16_le(buf, p + 42, 0); // required_ram (0 = unknown)
    write_u16_le(buf, p + 44, largest_track_blocks);
    write_u16_le(buf, p + 46, 0); // flux_block (none)
    write_u16_le(buf, p + 48, 0); // largest_flux_track (none)
                                  // Remaining bytes (p+50 .. p+60) reserved → already zero.
}

/// Write a 160-byte TMAP chunk for a 5.25" disk into `buf` at `TMAP_CHUNK_START`.
///
/// 5.25" TMAP layout: 4 quarter-track entries per whole track.  For each whole
/// track T at quarter-track index 4T, we write T at indices 4T-1, 4T, 4T+1
/// (so the head can find the same track from adjacent quarter-track positions);
/// half-tracks (4T+2) are 0xFF (unmapped).
fn write_tmap_525(buf: &mut [u8], num_tracks: u8) {
    buf[TMAP_CHUNK_START..TMAP_CHUNK_START + 4].copy_from_slice(b"TMAP");
    write_u32_le(buf, TMAP_CHUNK_START + 4, TMAP_LENGTH as u32);

    let p = TMAP_DATA_START;
    for b in &mut buf[p..p + TMAP_LENGTH] {
        *b = 0xFF;
    }
    for t in 0..num_tracks {
        let qi = t as usize * 4;
        buf[p + qi] = t;
        if qi + 1 < TMAP_LENGTH {
            buf[p + qi + 1] = t;
        }
        if qi >= 1 {
            buf[p + qi - 1] = t;
        }
    }
}

/// Write a 160-byte TMAP chunk for a 3.5" disk into `buf` at `TMAP_CHUNK_START`.
///
/// 3.5" TMAP layout: 2 entries per track (side 0, side 1).  Tracks are laid out
/// in TRKS in `track*sides + side` order, so entry index = track*2 + side.
/// Single-sided disks leave side-1 entries as 0xFF.
fn write_tmap_35(buf: &mut [u8], num_tracks: u8, sides: u8) {
    buf[TMAP_CHUNK_START..TMAP_CHUNK_START + 4].copy_from_slice(b"TMAP");
    write_u32_le(buf, TMAP_CHUNK_START + 4, TMAP_LENGTH as u32);

    let p = TMAP_DATA_START;
    for b in &mut buf[p..p + TMAP_LENGTH] {
        *b = 0xFF;
    }
    let mut trk_idx: u8 = 0;
    for t in 0..num_tracks as usize {
        for side in 0..sides as usize {
            buf[p + t * 2 + side] = trk_idx;
            trk_idx += 1;
        }
    }
}

/// Build a complete WOZ2 file from generated track bitstreams.
///
/// `tracks[i]` is `(bytes, bit_count)` for the i-th TMAP-referenced track.
/// For 5.25" disks, this is one entry per whole track (35 entries typical).
/// For 3.5" disks, entries are interleaved as `track*sides + side` (matching
/// `write_tmap_35`'s assignment).
///
/// The TRKS chunk holds 160 8-byte descriptors (`starting_block`, `block_count`,
/// `bit_count`) followed by track bit data starting at file byte 1536 and
/// padded to 512-byte boundaries.  The CRC32 over bytes [12..EOF] is written
/// to bytes [8..12].
pub fn build_woz2_file(disk_type: u8, sides: u8, tracks: &[(Vec<u8>, u32)]) -> Vec<u8> {
    assert!(
        disk_type == DISK_TYPE_525 || disk_type == DISK_TYPE_35,
        "disk_type must be DISK_TYPE_525 or DISK_TYPE_35"
    );
    let max_tracks = if disk_type == DISK_TYPE_525 {
        MAX_TRACK_525 as usize
    } else {
        MAX_TRACK_35 as usize * sides as usize
    };
    assert!(
        tracks.len() <= max_tracks,
        "too many tracks ({}) for disk type {} (max {})",
        tracks.len(),
        disk_type,
        max_tracks
    );

    // Compute per-track block counts and the file's total size.
    let mut block_counts = Vec::with_capacity(tracks.len());
    let mut largest_blocks: u16 = 0;
    let mut total_track_bytes = 0usize;
    for (bytes, _bit_count) in tracks {
        let blocks = ((bytes.len() + BLOCK_SIZE - 1) / BLOCK_SIZE) as u16;
        if blocks > largest_blocks {
            largest_blocks = blocks;
        }
        block_counts.push(blocks);
        total_track_bytes += blocks as usize * BLOCK_SIZE;
    }

    let trks_chunk_len = TRKS_DESC_LEN + total_track_bytes;
    let file_len = TRKS_DATA_START + trks_chunk_len;
    let mut buf = vec![0u8; file_len];

    // Header.
    buf[..8].copy_from_slice(&SIGNATURE_W2);
    // CRC32 written last; bytes 8..12 stay zero for now.

    // INFO + TMAP.
    write_info_chunk(&mut buf, disk_type, sides, largest_blocks, "rusty-backup");
    if disk_type == DISK_TYPE_525 {
        write_tmap_525(&mut buf, tracks.len() as u8);
    } else {
        write_tmap_35(&mut buf, (tracks.len() / sides as usize) as u8, sides);
    }

    // TRKS chunk header.
    buf[TRKS_CHUNK_START..TRKS_CHUNK_START + 4].copy_from_slice(b"TRKS");
    write_u32_le(&mut buf, TRKS_CHUNK_START + 4, trks_chunk_len as u32);

    // TRKS descriptors + per-track bit data.
    let first_track_block: u16 = (TRACK_BIT_DATA_START / BLOCK_SIZE) as u16;
    let mut next_block = first_track_block;
    for (i, ((bytes, bit_count), &blocks)) in tracks.iter().zip(block_counts.iter()).enumerate() {
        let desc_off = TRKS_DATA_START + i * 8;
        write_u16_le(&mut buf, desc_off, next_block);
        write_u16_le(&mut buf, desc_off + 2, blocks);
        write_u32_le(&mut buf, desc_off + 4, *bit_count);

        let data_off = next_block as usize * BLOCK_SIZE;
        buf[data_off..data_off + bytes.len()].copy_from_slice(bytes);
        // Padding bytes already zero from the initial vec! allocation.

        next_block += blocks;
    }

    // CRC32 over bytes [12..EOF].
    let crc = crc32fast::hash(&buf[HEADER_LEN..]);
    write_u32_le(&mut buf, 8, crc);

    buf
}

// ───────────────────────── Public conversion API ──────────────────────────

/// Size of a raw 140K 5.25" floppy image: 35 tracks × 16 sectors × 256 bytes.
pub const RAW_SIZE_525: usize = 35 * 16 * 256; // 143_360
/// Size of a raw 400K single-sided 3.5" floppy image.
/// 5 speed zones × 16 tracks per zone × (zone sectors) × 512 bytes.
pub const RAW_SIZE_35_400K: usize = 16 * (12 + 11 + 10 + 9 + 8) * 512; // 409_600
/// Size of a raw 800K double-sided 3.5" floppy image.
pub const RAW_SIZE_35_800K: usize = RAW_SIZE_35_400K * 2; // 819_200

/// 5.25" logical-to-physical sector map.
///
/// `WozReader` presents sectors in ProDOS order: track `t`, ProDOS index `p` is
/// at byte `(t * 16 + p) * 256`.  To regenerate a WOZ we need to write each
/// ProDOS-ordered sector at its physical position on the track.  For ProDOS
/// index `p`, the physical sector is `PRODOS_TO_PHYS_525[p]` (inverse of the
/// reader's `PHYS_TO_PRODOS` map in `woz.rs`).
const PRODOS_TO_PHYS_525: [usize; 16] = [0, 2, 4, 6, 8, 10, 12, 14, 1, 3, 5, 7, 9, 11, 13, 15];

/// Encode a raw 5.25" floppy image (140K ProDOS-order sectors) as a WOZ 2.0 file.
///
/// Input layout matches `WozReader`'s output: 35 tracks × 16 sectors × 256
/// bytes, with each track's sectors in ProDOS (logical) order.  This is also
/// the format of a `.po` image or a raw partition extracted from a ProDOS
/// filesystem.
pub fn sectors_to_woz_525(sectors: &[u8]) -> anyhow::Result<Vec<u8>> {
    if sectors.len() != RAW_SIZE_525 {
        anyhow::bail!(
            "5.25\" WOZ export expects {} bytes, got {}",
            RAW_SIZE_525,
            sectors.len()
        );
    }

    let mut tracks: Vec<(Vec<u8>, u32)> = Vec::with_capacity(35);
    for t in 0..35u8 {
        // Reorder ProDOS-ordered sectors into physical order for track generation.
        let mut phys = [[0u8; 256]; 16];
        let track_base = t as usize * 16 * 256;
        for p in 0..16 {
            let phys_idx = PRODOS_TO_PHYS_525[p];
            let src = track_base + p * 256;
            phys[phys_idx].copy_from_slice(&sectors[src..src + 256]);
        }
        tracks.push(generate_track_525(DEFAULT_VOLUME_525, t, &phys));
    }

    Ok(build_woz2_file(DISK_TYPE_525, 1, &tracks))
}

/// Encode a raw 3.5" floppy image as a WOZ 2.0 file.
///
/// `sides` must be 1 (400K) or 2 (800K).  Input layout matches `WozReader`'s
/// 3.5" output: sectors are in speed-zone/track/side/logical-sector order —
/// i.e. for each track 0..=79 we emit `sectors_for_track_35(t)` side-0 sectors
/// followed by the same count of side-1 sectors (when `sides == 2`).  The
/// interleave is 2:1 (standard for ProDOS / HFS).
pub fn sectors_to_woz_35(sectors: &[u8], sides: u8) -> anyhow::Result<Vec<u8>> {
    let expected = match sides {
        1 => RAW_SIZE_35_400K,
        2 => RAW_SIZE_35_800K,
        _ => anyhow::bail!("3.5\" WOZ export requires sides = 1 or 2, got {}", sides),
    };
    if sectors.len() != expected {
        anyhow::bail!(
            "3.5\" WOZ export expects {} bytes (sides={}), got {}",
            expected,
            sides,
            sectors.len()
        );
    }
    let format = if sides == 2 {
        FORMAT_35_DSDD
    } else {
        FORMAT_35_SSDD
    };

    let mut tracks: Vec<(Vec<u8>, u32)> = Vec::with_capacity(80 * sides as usize);
    let mut offset = 0usize;
    for t in 0..80u8 {
        let n = sectors_for_track_35(t);
        for side in 0..sides {
            let mut sector_data: Vec<Option<[u8; 512]>> = Vec::with_capacity(n);
            for _ in 0..n {
                let mut s = [0u8; 512];
                s.copy_from_slice(&sectors[offset..offset + 512]);
                sector_data.push(Some(s));
                offset += 512;
            }
            tracks.push(generate_track_35(t, side, format, &sector_data));
        }
    }

    Ok(build_woz2_file(DISK_TYPE_35, sides, &tracks))
}

/// Encode a raw floppy-sized sector buffer as a WOZ 2.0 file, auto-detecting
/// 5.25" vs 3.5" from the input size.
///
/// Recognised sizes:
/// - 143,360 bytes → 5.25" 140K (`sectors_to_woz_525`)
/// - 409,600 bytes → 3.5" 400K single-sided (`sectors_to_woz_35` with sides=1)
/// - 819,200 bytes → 3.5" 800K double-sided (`sectors_to_woz_35` with sides=2)
pub fn sectors_to_woz(sectors: &[u8]) -> anyhow::Result<Vec<u8>> {
    match sectors.len() {
        RAW_SIZE_525 => sectors_to_woz_525(sectors),
        RAW_SIZE_35_400K => sectors_to_woz_35(sectors, 1),
        RAW_SIZE_35_800K => sectors_to_woz_35(sectors, 2),
        n => anyhow::bail!(
            "WOZ export: unrecognised floppy size {} bytes (expected 143360, 409600, or 819200)",
            n
        ),
    }
}

/// Write a raw floppy-sized sector buffer to `path` as a WOZ 2.0 file.
/// Auto-detects disk type from the buffer size (see [`sectors_to_woz`]).
pub fn write_woz(path: &std::path::Path, sectors: &[u8]) -> anyhow::Result<()> {
    use anyhow::Context;
    let bytes = sectors_to_woz(sectors)?;
    std::fs::write(path, &bytes)
        .with_context(|| format!("failed to write WOZ file: {}", path.display()))?;
    Ok(())
}

// ─────────────────────────────── tests ─────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirror of `DECODE_62` in `woz.rs`, replicated here so tests can verify
    /// the encode/decode pair without crossing module privacy boundaries.
    const DECODE_62: [u8; 256] = {
        let mut table = [0xFFu8; 256];
        let mut i = 0;
        while i < 64 {
            table[ENCODE_62[i] as usize] = i as u8;
            i += 1;
        }
        table
    };

    #[test]
    fn encode_62_round_trip() {
        for v in 0u8..64 {
            let nib = ENCODE_62[v as usize];
            assert_eq!(
                DECODE_62[nib as usize], v,
                "round-trip failed for 0x{v:02x}"
            );
        }
    }

    #[test]
    fn encode_62_table_size_and_uniqueness() {
        let mut seen = [false; 256];
        for &nib in &ENCODE_62 {
            assert!(nib >= 0x96, "nibble 0x{nib:02x} out of range");
            assert!(!seen[nib as usize], "duplicate nibble 0x{nib:02x}");
            seen[nib as usize] = true;
        }
    }

    #[test]
    fn bit_writer_octet_aligned() {
        let mut w = BitWriter::new();
        w.write_octet(0xD5);
        w.write_octet(0xAA);
        w.write_octet(0x96);
        assert_eq!(w.bit_count(), 24);
        assert_eq!(w.as_bytes(), &[0xD5, 0xAA, 0x96]);
    }

    #[test]
    fn bit_writer_unaligned_byte_straddle() {
        // Write 3 bits of value 0b101, then a full byte 0xAA (10101010).
        // Combined bitstream MSB-first: 101 10101010 = 1011 0101 010_____
        // Packed: 0xB5, 0x40 (last 3 bits zero-padded)
        let mut w = BitWriter::new();
        w.write_bit(1);
        w.write_bit(0);
        w.write_bit(1);
        w.write_octet(0xAA);
        assert_eq!(w.bit_count(), 11);
        assert_eq!(w.as_bytes(), &[0xB5, 0x40]);
    }

    #[test]
    fn bit_writer_self_sync_pattern() {
        // One self-sync FF = 10 bits: 11111111 00
        // Two of them = 20 bits: 11111111 00 11111111 00
        // Packed MSB-first: 1111 1111 0011 1111 1100 ____ → 0xFF 0x3F 0xC0
        let mut w = BitWriter::new();
        w.write_self_sync(2);
        assert_eq!(w.bit_count(), 20);
        assert_eq!(w.as_bytes(), &[0xFF, 0x3F, 0xC0]);
    }

    #[test]
    fn bit_writer_fill_self_sync_to_target() {
        let mut w = BitWriter::new();
        w.fill_self_sync(25);
        assert_eq!(w.bit_count(), 25);
        // 2 full self-sync (20 bits) + 5 trailing 1-bits.
        // Bits: 11111111 00 11111111 00 11111
        // Packed: 0xFF 0x3F 0xCF 0x80
        assert_eq!(w.as_bytes(), &[0xFF, 0x3F, 0xCF, 0x80]);
    }

    #[test]
    fn bit_writer_byte_then_self_sync() {
        // A common pattern in track building: octet then self-sync gap.
        let mut w = BitWriter::new();
        w.write_octet(0xD5);
        w.write_self_sync(1);
        assert_eq!(w.bit_count(), 18);
        // 11010101 11111111 00 → 1101 0101 1111 1111 00__
        // Packed: 0xD5, 0xFF, 0x00
        assert_eq!(w.as_bytes(), &[0xD5, 0xFF, 0x00]);
    }

    #[test]
    fn track_525_round_trip_zeros() {
        let sectors = [[0u8; 256]; 16];
        let (track, bit_count) = generate_track_525(DEFAULT_VOLUME_525, 0, &sectors);
        let decoded = crate::rbformats::woz::decode_525_track(&track, bit_count);
        for (i, s) in decoded.iter().enumerate() {
            assert_eq!(s.as_ref(), Some(&[0u8; 256]), "sector {i} mismatch");
        }
    }

    #[test]
    fn track_525_round_trip_pattern() {
        // Build sectors with a per-sector, per-byte distinct pattern.
        let mut sectors = [[0u8; 256]; 16];
        for s in 0..16 {
            for b in 0..256 {
                sectors[s][b] = (s as u8).wrapping_mul(17).wrapping_add(b as u8);
            }
        }
        let (track, bit_count) = generate_track_525(DEFAULT_VOLUME_525, 5, &sectors);
        let decoded = crate::rbformats::woz::decode_525_track(&track, bit_count);
        for (i, s) in decoded.iter().enumerate() {
            assert_eq!(s.as_ref(), Some(&sectors[i]), "sector {i} mismatch");
        }
    }

    #[test]
    fn track_525_round_trip_all_bytes() {
        // Each sector contains 0..=255 to exercise every possible byte value
        // through 6-and-2 encoding.
        let mut sectors = [[0u8; 256]; 16];
        for s in 0..16 {
            for b in 0..256 {
                sectors[s][b] = b as u8;
            }
        }
        let (track, bit_count) = generate_track_525(DEFAULT_VOLUME_525, 17, &sectors);
        let decoded = crate::rbformats::woz::decode_525_track(&track, bit_count);
        for (i, s) in decoded.iter().enumerate() {
            assert_eq!(s.as_ref(), Some(&sectors[i]), "sector {i} mismatch");
        }
    }

    #[test]
    fn track_525_size_matches_expectation() {
        // Per-sector content: gap3 (200) + addr (112) + gap2 (50) + data (2792) = 3154 bits
        // 16 sectors = 50464 bits = 6308 bytes
        let sectors = [[0u8; 256]; 16];
        let (track, bit_count) = generate_track_525(DEFAULT_VOLUME_525, 0, &sectors);
        assert_eq!(bit_count, 50_464);
        assert_eq!(track.len(), (50_464 + 7) / 8);
    }

    fn make_pattern_sectors_35(count: usize, salt: u8) -> Vec<Option<[u8; 512]>> {
        (0..count)
            .map(|s| {
                let mut data = [0u8; 512];
                for b in 0..512 {
                    data[b] = (salt
                        .wrapping_add(s as u8)
                        .wrapping_mul(31)
                        .wrapping_add(b as u8)) as u8;
                }
                Some(data)
            })
            .collect()
    }

    fn assert_track_35_round_trip(track: u8, side: u8, format: u8, num_sectors: usize) {
        let sectors = make_pattern_sectors_35(num_sectors, track ^ side ^ format);
        let (data, bit_count) = generate_track_35(track, side, format, &sectors);
        let decoded = crate::rbformats::woz::decode_35_track(&data, bit_count, num_sectors);
        for (i, sec) in decoded.iter().enumerate() {
            let expected = sectors[i].as_ref().unwrap();
            let got = sec
                .as_ref()
                .unwrap_or_else(|| panic!("track {track} side {side} sector {i} not decoded"));
            assert_eq!(
                got, expected,
                "track {track} side {side} sector {i} mismatch"
            );
        }
    }

    #[test]
    fn track_35_round_trip_zone0_12_sectors() {
        assert_track_35_round_trip(0, 0, FORMAT_35_DSDD, 12);
    }

    #[test]
    fn track_35_round_trip_zone1_11_sectors() {
        assert_track_35_round_trip(16, 0, FORMAT_35_DSDD, 11);
    }

    #[test]
    fn track_35_round_trip_zone2_10_sectors() {
        assert_track_35_round_trip(32, 1, FORMAT_35_DSDD, 10);
    }

    #[test]
    fn track_35_round_trip_zone3_9_sectors() {
        assert_track_35_round_trip(48, 0, FORMAT_35_DSDD, 9);
    }

    #[test]
    fn track_35_round_trip_zone4_8_sectors() {
        assert_track_35_round_trip(64, 1, FORMAT_35_DSDD, 8);
    }

    #[test]
    fn track_35_round_trip_zeros() {
        // Empty sectors should still round-trip cleanly.
        let sectors: Vec<Option<[u8; 512]>> = vec![Some([0u8; 512]); 12];
        let (data, bit_count) = generate_track_35(0, 0, FORMAT_35_DSDD, &sectors);
        let decoded = crate::rbformats::woz::decode_35_track(&data, bit_count, 12);
        for (i, sec) in decoded.iter().enumerate() {
            assert_eq!(sec.as_ref(), Some(&[0u8; 512]), "sector {i}");
        }
    }

    #[test]
    fn interleave_table_35_2_to_1_12_sectors() {
        let table = interleave_table_35(12, 2);
        // Expected pattern: 0,6,1,7,2,8,3,9,4,10,5,11 (CP2-compatible).
        assert_eq!(table, vec![0, 6, 1, 7, 2, 8, 3, 9, 4, 10, 5, 11]);
    }

    #[test]
    fn interleave_table_35_is_permutation() {
        for &n in &[8usize, 9, 10, 11, 12] {
            for &iv in &[2usize, 4] {
                let table = interleave_table_35(n, iv);
                assert_eq!(table.len(), n);
                let mut seen = vec![false; n];
                for &v in &table {
                    assert!((v as usize) < n, "out-of-range entry: {v}");
                    assert!(!seen[v as usize], "duplicate entry: {v}");
                    seen[v as usize] = true;
                }
            }
        }
    }

    #[test]
    fn sectors_for_track_35_zones() {
        assert_eq!(sectors_for_track_35(0), 12);
        assert_eq!(sectors_for_track_35(15), 12);
        assert_eq!(sectors_for_track_35(16), 11);
        assert_eq!(sectors_for_track_35(31), 11);
        assert_eq!(sectors_for_track_35(32), 10);
        assert_eq!(sectors_for_track_35(48), 9);
        assert_eq!(sectors_for_track_35(64), 8);
        assert_eq!(sectors_for_track_35(79), 8);
    }

    // ───────────────────── container builder tests ────────────────────────

    fn blank_525_tracks() -> Vec<(Vec<u8>, u32)> {
        let zero = [[0u8; 256]; 16];
        (0..35)
            .map(|t| generate_track_525(DEFAULT_VOLUME_525, t as u8, &zero))
            .collect()
    }

    fn blank_35_tracks(sides: u8) -> Vec<(Vec<u8>, u32)> {
        let mut tracks = Vec::with_capacity(80 * sides as usize);
        for t in 0..80u8 {
            let n = sectors_for_track_35(t);
            let sectors: Vec<Option<[u8; 512]>> = vec![Some([0u8; 512]); n];
            for side in 0..sides {
                tracks.push(generate_track_35(t, side, FORMAT_35_DSDD, &sectors));
            }
        }
        tracks
    }

    #[test]
    fn build_blank_525_is_valid_woz() {
        let tracks = blank_525_tracks();
        let woz = build_woz2_file(DISK_TYPE_525, 1, &tracks);

        assert!(crate::rbformats::woz::is_woz(&woz), "is_woz should be true");
        assert_eq!(&woz[..8], &SIGNATURE_W2);
        // CRC32 must match what we wrote.
        let expected_crc = crc32fast::hash(&woz[HEADER_LEN..]);
        let stored_crc = u32::from_le_bytes([woz[8], woz[9], woz[10], woz[11]]);
        assert_eq!(stored_crc, expected_crc, "stored CRC should match payload");

        // INFO sanity.
        assert_eq!(&woz[INFO_CHUNK_START..INFO_CHUNK_START + 4], b"INFO");
        assert_eq!(woz[INFO_DATA_START], 2, "WOZ version 2");
        assert_eq!(woz[INFO_DATA_START + 1], DISK_TYPE_525);
        assert_eq!(woz[INFO_DATA_START + 37], 1, "sides");
        assert_eq!(woz[INFO_DATA_START + 39], 32, "5.25\" optimal_bit_timing");
    }

    #[test]
    fn build_blank_525_decodes_to_zeros() {
        let tracks = blank_525_tracks();
        let woz = build_woz2_file(DISK_TYPE_525, 1, &tracks);

        let reader = crate::rbformats::woz::WozReader::from_bytes(woz)
            .expect("WozReader should accept generated WOZ2");
        assert_eq!(reader.len(), 143_360);
    }

    #[test]
    fn build_blank_35_double_sided_decodes_to_zeros() {
        let tracks = blank_35_tracks(2);
        let woz = build_woz2_file(DISK_TYPE_35, 2, &tracks);

        assert!(crate::rbformats::woz::is_woz(&woz));
        let reader = crate::rbformats::woz::WozReader::from_bytes(woz)
            .expect("WozReader should accept generated 800K WOZ2");
        // 800K disk: 2 sides × (16×12 + 16×11 + 16×10 + 16×9 + 16×8) sectors × 512 = 819,200.
        assert_eq!(reader.len(), 819_200);
    }

    #[test]
    fn build_blank_35_single_sided_decodes_to_zeros() {
        let tracks = blank_35_tracks(1);
        let woz = build_woz2_file(DISK_TYPE_35, 1, &tracks);

        assert!(crate::rbformats::woz::is_woz(&woz));
        let reader = crate::rbformats::woz::WozReader::from_bytes(woz)
            .expect("WozReader should accept generated 400K WOZ2");
        // 400K disk: 1 side × 800 sectors × 512 = 409,600.
        assert_eq!(reader.len(), 409_600);
    }

    #[test]
    fn build_525_round_trip_preserves_data() {
        // Each sector gets a distinct pattern; verify reader recovers it.
        let mut sectors_per_track = Vec::new();
        for t in 0..35usize {
            let mut secs = [[0u8; 256]; 16];
            for s in 0..16 {
                for b in 0..256 {
                    secs[s][b] = ((t * 16 + s) ^ b) as u8;
                }
            }
            sectors_per_track.push(secs);
        }
        let tracks: Vec<_> = sectors_per_track
            .iter()
            .enumerate()
            .map(|(t, secs)| generate_track_525(DEFAULT_VOLUME_525, t as u8, secs))
            .collect();

        let woz = build_woz2_file(DISK_TYPE_525, 1, &tracks);
        let mut reader = crate::rbformats::woz::WozReader::from_bytes(woz).unwrap();
        let mut decoded = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut decoded).unwrap();
        assert_eq!(decoded.len(), 143_360);

        // WozReader presents data in ProDOS order: track t, ProDOS sector p →
        // byte (t*16 + p) * 256.  Encoder wrote physical sector s; reader
        // places it at ProDOS index PHYS_TO_PRODOS[s].  Inverse gives the
        // physical sector whose data lands at ProDOS position p.
        const PRODOS_TO_PHYS: [usize; 16] = [0, 2, 4, 6, 8, 10, 12, 14, 1, 3, 5, 7, 9, 11, 13, 15];
        for t in 0..35usize {
            for p in 0..16usize {
                let phys = PRODOS_TO_PHYS[p];
                let off = (t * 16 + p) * 256;
                assert_eq!(
                    &decoded[off..off + 256],
                    &sectors_per_track[t][phys][..],
                    "track {t} prodos {p}"
                );
            }
        }
    }

    #[test]
    fn build_woz2_layout_offsets() {
        // Construct a tiny artificial input: 1 track, 1 block of bit data.
        let tracks = vec![(vec![0xAAu8; 512], 4096u32)];
        let woz = build_woz2_file(DISK_TYPE_525, 1, &tracks);

        // INFO at 12, TMAP at 80, TRKS at 248.
        assert_eq!(&woz[12..16], b"INFO");
        assert_eq!(&woz[80..84], b"TMAP");
        assert_eq!(&woz[248..252], b"TRKS");

        // First TRK descriptor: starting_block=3, block_count=1, bit_count=4096.
        let desc = &woz[256..264];
        assert_eq!(u16::from_le_bytes([desc[0], desc[1]]), 3);
        assert_eq!(u16::from_le_bytes([desc[2], desc[3]]), 1);
        assert_eq!(
            u32::from_le_bytes([desc[4], desc[5], desc[6], desc[7]]),
            4096
        );

        // Track data lives at byte 1536.
        assert_eq!(woz[1536], 0xAA);
        assert_eq!(woz.len(), 1536 + 512);
    }

    #[test]
    fn build_woz2_pads_track_to_block_boundary() {
        // 513-byte track → 2 blocks (1024 bytes) reserved, 511 bytes zero pad.
        let tracks = vec![(vec![0x55u8; 513], 4104u32)];
        let woz = build_woz2_file(DISK_TYPE_525, 1, &tracks);

        let desc = &woz[256..264];
        assert_eq!(u16::from_le_bytes([desc[2], desc[3]]), 2, "block_count = 2");
        assert_eq!(woz.len(), 1536 + 1024);
        assert_eq!(woz[1536 + 512], 0x55, "second block starts with track data");
        assert_eq!(woz[1536 + 513], 0x00, "pad byte");
        assert_eq!(woz[1536 + 1023], 0x00, "final pad byte");
    }

    #[test]
    fn tmap_525_quarter_track_mapping() {
        let mut buf = vec![0u8; TMAP_DATA_START + TMAP_LENGTH];
        write_tmap_525(&mut buf, 35);
        let tmap = &buf[TMAP_DATA_START..TMAP_DATA_START + TMAP_LENGTH];

        // Whole tracks at quarter indices 0, 4, 8, ...
        assert_eq!(tmap[0], 0);
        assert_eq!(tmap[4], 1);
        assert_eq!(tmap[8], 2);
        // Adjacent quarter-track also points to whole track.
        assert_eq!(tmap[1], 0);
        assert_eq!(tmap[3], 1);
        assert_eq!(tmap[5], 1);
        // Half-tracks unmapped.
        assert_eq!(tmap[2], 0xFF);
        assert_eq!(tmap[6], 0xFF);
        // Beyond track 34, all 0xFF.
        assert!(tmap[140..].iter().all(|&v| v == 0xFF));
    }

    #[test]
    fn tmap_35_double_sided_layout() {
        let mut buf = vec![0u8; TMAP_DATA_START + TMAP_LENGTH];
        write_tmap_35(&mut buf, 80, 2);
        let tmap = &buf[TMAP_DATA_START..TMAP_DATA_START + TMAP_LENGTH];

        // Track 0 side 0, track 0 side 1, track 1 side 0, track 1 side 1, ...
        assert_eq!(tmap[0], 0);
        assert_eq!(tmap[1], 1);
        assert_eq!(tmap[2], 2);
        assert_eq!(tmap[3], 3);
        assert_eq!(tmap[158], 158);
        assert_eq!(tmap[159], 159);
    }

    #[test]
    fn tmap_35_single_sided_leaves_side1_unmapped() {
        let mut buf = vec![0u8; TMAP_DATA_START + TMAP_LENGTH];
        write_tmap_35(&mut buf, 80, 1);
        let tmap = &buf[TMAP_DATA_START..TMAP_DATA_START + TMAP_LENGTH];

        for t in 0..80 {
            assert_eq!(tmap[t * 2], t as u8, "side 0 entry for track {t}");
            assert_eq!(tmap[t * 2 + 1], 0xFF, "side 1 entry for track {t}");
        }
    }

    // ─────────────────────── public API round-trip tests ──────────────────

    fn pattern_525_image() -> Vec<u8> {
        // Each (track, prodos-sector, byte) slot gets a distinct value.
        let mut buf = vec![0u8; RAW_SIZE_525];
        for t in 0..35usize {
            for s in 0..16usize {
                for b in 0..256usize {
                    let off = (t * 16 + s) * 256 + b;
                    buf[off] = ((t * 37) ^ (s * 23) ^ b) as u8;
                }
            }
        }
        buf
    }

    fn pattern_35_image(total_bytes: usize) -> Vec<u8> {
        let mut buf = vec![0u8; total_bytes];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = ((i >> 3) ^ (i * 7)) as u8;
        }
        buf
    }

    #[test]
    fn sectors_to_woz_525_round_trip() {
        let input = pattern_525_image();
        let woz = sectors_to_woz_525(&input).unwrap();

        let mut reader = crate::rbformats::woz::WozReader::from_bytes(woz).unwrap();
        let mut decoded = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut decoded).unwrap();
        assert_eq!(decoded.len(), RAW_SIZE_525);
        assert_eq!(decoded, input, "ProDOS-order round-trip mismatch");
    }

    #[test]
    fn sectors_to_woz_525_rejects_wrong_size() {
        assert!(sectors_to_woz_525(&[0u8; 1024]).is_err());
        assert!(sectors_to_woz_525(&[0u8; RAW_SIZE_525 - 1]).is_err());
        assert!(sectors_to_woz_525(&[0u8; RAW_SIZE_525 + 1]).is_err());
    }

    #[test]
    fn sectors_to_woz_35_800k_round_trip() {
        let input = pattern_35_image(RAW_SIZE_35_800K);
        let woz = sectors_to_woz_35(&input, 2).unwrap();

        let mut reader = crate::rbformats::woz::WozReader::from_bytes(woz).unwrap();
        let mut decoded = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut decoded).unwrap();
        assert_eq!(decoded.len(), RAW_SIZE_35_800K);
        assert_eq!(decoded, input, "3.5\" 800K round-trip mismatch");
    }

    #[test]
    fn sectors_to_woz_35_400k_round_trip() {
        let input = pattern_35_image(RAW_SIZE_35_400K);
        let woz = sectors_to_woz_35(&input, 1).unwrap();

        let mut reader = crate::rbformats::woz::WozReader::from_bytes(woz).unwrap();
        let mut decoded = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut decoded).unwrap();
        assert_eq!(decoded.len(), RAW_SIZE_35_400K);
        assert_eq!(decoded, input, "3.5\" 400K round-trip mismatch");
    }

    #[test]
    fn sectors_to_woz_35_rejects_bad_args() {
        let buf = vec![0u8; RAW_SIZE_35_800K];
        assert!(sectors_to_woz_35(&buf, 0).is_err(), "sides=0 rejected");
        assert!(sectors_to_woz_35(&buf, 3).is_err(), "sides=3 rejected");
        assert!(
            sectors_to_woz_35(&buf[..RAW_SIZE_35_400K], 2).is_err(),
            "400K with sides=2 rejected"
        );
        assert!(
            sectors_to_woz_35(&buf, 1).is_err(),
            "800K bytes with sides=1 rejected"
        );
    }

    #[test]
    fn sectors_to_woz_auto_detects_size() {
        let img_525 = pattern_525_image();
        let woz_525 = sectors_to_woz(&img_525).unwrap();
        assert!(crate::rbformats::woz::is_woz(&woz_525));
        assert_eq!(woz_525[INFO_DATA_START + 1], DISK_TYPE_525);

        let img_400k = pattern_35_image(RAW_SIZE_35_400K);
        let woz_400k = sectors_to_woz(&img_400k).unwrap();
        assert_eq!(woz_400k[INFO_DATA_START + 1], DISK_TYPE_35);
        assert_eq!(woz_400k[INFO_DATA_START + 37], 1, "400K is single-sided");

        let img_800k = pattern_35_image(RAW_SIZE_35_800K);
        let woz_800k = sectors_to_woz(&img_800k).unwrap();
        assert_eq!(woz_800k[INFO_DATA_START + 1], DISK_TYPE_35);
        assert_eq!(woz_800k[INFO_DATA_START + 37], 2, "800K is double-sided");

        // Non-floppy sizes rejected.
        assert!(sectors_to_woz(&[0u8; 100_000]).is_err());
        assert!(sectors_to_woz(&[0u8; 1_048_576]).is_err());
    }

    #[test]
    fn write_woz_creates_valid_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let out = tmp.path().join("disk.woz");
        let img = pattern_525_image();
        write_woz(&out, &img).unwrap();

        let raw = std::fs::read(&out).unwrap();
        assert!(crate::rbformats::woz::is_woz(&raw));

        // Verify end-to-end: opening via WozReader::open yields the input bytes.
        let mut reader = crate::rbformats::woz::WozReader::open(&out).unwrap();
        let mut decoded = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut decoded).unwrap();
        assert_eq!(decoded, img);
    }

    #[test]
    fn to_44_round_trip() {
        for v in 0u8..=255 {
            let lo = to_44_lo(v);
            let hi = to_44_hi(v);
            // Reader formula from existing woz.rs decoding.
            let decoded = ((lo << 1) | 0x01) & hi;
            assert_eq!(decoded, v, "4-and-4 round-trip failed for 0x{v:02x}");
            // Both halves must have high bit set (valid disk nibble).
            assert!(lo & 0x80 != 0, "lo missing high bit for 0x{v:02x}");
            assert!(hi & 0x80 != 0, "hi missing high bit for 0x{v:02x}");
        }
    }
}
