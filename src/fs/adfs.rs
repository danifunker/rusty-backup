//! Acorn ADFS / FileCore — the filesystem used by the MiSTer Archie
//! (Acorn Archimedes) core. Also seen on later BBC Micro / Electron via
//! ADFS expansion ROMs.
//!
//! **Scope: extract floor.** FileCore's full storage model (old-map vs
//! new-map FSMs, indirect zone allocation on E/F-format disks) is genuinely
//! intricate; the long tail of write-side work lives behind this stage. For
//! now we parse the boot block, recognize D / E / F formats, walk the `$`
//! root directory, and read files under the assumption their extents are
//! contiguous from `start_sector` (true for freshly-written disks and for
//! virtually every Archimedes RISC OS distribution disk). Fragmented files
//! return an `Unsupported` error.
//!
//! ## On-disk layout (FileCore spec; Acorn TechRef vol I)
//!
//! - **Boot block** at sector 0xC00 / 1024-B-sector 0xC: contains the
//!   "Disc Record" (a 64-byte struct describing format, sector size,
//!   tracks, density, FSM layout).
//! - **Disc Record** (at boot-block offset 0x1C0, big-endian inside the
//!   sector but the constituent fields are little-endian per the spec —
//!   ARM is LE):
//!
//! ```text
//! 0x00  log2(sector_size)        (8 = 256 B, 10 = 1024 B)
//! 0x01  sectors_per_track
//! 0x02  heads
//! 0x03  density                  (1 = single, 2 = double, 3 = high)
//! 0x04  id_len
//! 0x05  log2(map_bits)
//! 0x06  skew
//! 0x07  boot_option
//! 0x08  low_sector
//! 0x09  zones                    (new-map only)
//! 0x0A..0x0C  zone_spare         (LE u16)
//! 0x0C..0x10  root              (LE u32, indirect disc address of $)
//! 0x10..0x14  disc_size         (LE u32, total sectors)
//! 0x14..0x16  disc_id           (LE u16, randomly chosen)
//! 0x16..0x26  disc_name         (10 chars, space-padded)
//! ```
//!
//! - **Directory `$`** — the root. Layout is identical for both small
//!   ("D-format", 26-B entries, max 47 entries) and big ("E-format",
//!   26-B entries, max 77 entries) directory variants:
//!
//! ```text
//! header (5 bytes): "Hugo" magic (or "Nick" for big-format), unused
//!
//! 26 entries × 26 bytes each:
//!   0..10   name (space-padded; first byte 0 = end of directory)
//!   10..14  load_addr     LE u32
//!   14..18  exec_addr     LE u32
//!   18..22  file_length   LE u32
//!   22..25  indirect_disc_address (24-bit LE — physical sector address
//!           multiplied by sector size)
//!   25      attrs (0x01 = R, 0x02 = W, 0x04 = locked, 0x08 = directory,
//!                  0x10 = E (execute), 0x20 = pub R, 0x40 = pub W,
//!                  0x80 = pub locked)
//!
//! trailer: tail-marker ("Hugo" again) + cycle counter
//! ```
//!
//! All multi-byte fields are little-endian (ARM native).

use std::io::{Read, Seek, SeekFrom};

use byteorder::{ByteOrder, LittleEndian};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// Boot-block offset candidates for the Disc Record. Real-world ADFS
/// samples surveyed so far:
///   * Linux kernel `adfs_validate_bblk` reads the DR from byte 0xDC0
///     (= boot block at 0xC00 + DR offset 0x1C0). HD discs (CROS42 +
///     ICEBIRD) carry the DR here.
///   * Linux kernel `adfs_validate_dr0` reads single-zone discs' DR
///     from byte 0x04 (= 4-byte zone header + DR), used when the bblk
///     path fails. 800K E-format floppies (arc-04 + arc-05) use this.
///   * marutan.net blank HD samples (blank256E.hdf 256 MB E-format,
///     blank1024Eplus.hdf 1 GB E+ format) put the DR at byte 0xFC0 —
///     zone 0 = 4096 bytes (sector size 512), DR in its last 64 B.
///   * 8bs.com Acorn archive 800K E-format floppies expose a duplicate
///     DR at byte 0x404 (zone 0 at sector 1 with DR after 4-B header).
///
/// The 64-B-aligned probe in [`find_disc_record`] tries each in turn
/// and accepts the first that yields a syntactically valid record.
/// Order matches the kernel's two-path preference (bblk first, dr0
/// fallback), then the historical marutan / 8bs locations.
const DISC_RECORD_CANDIDATES: &[u64] = &[
    0xDC0,  // canonical HD (kernel adfs_validate_bblk)
    0x004,  // single-zone E-format floppy (kernel adfs_validate_dr0)
    0xFC0,  // marutan.net blank E/E+ HDDs (zone_size 4096)
    0x404,  // 8bs.com 800K E-format duplicate DR copy
    0x1FC0, // some doubled-zone layouts
    0x3FC0, // 16 KB zone
];

/// Directory entries are always 26 bytes regardless of format.
const DIR_ENTRY_SIZE: usize = 26;

/// 5-byte directory header; 5-byte tail too. Limits are 47 entries for
/// small format, 77 for big format.
const DIR_SMALL_HEADER: usize = 5;
const DIR_SMALL_MAX_ENTRIES: usize = 47;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdfsFormat {
    /// D-format: 800 KB floppy, 256-B sectors, old-map FSM.
    DFormat,
    /// E-format: 800 KB floppy, 1024-B sectors, new-map FSM.
    EFormat,
    /// F-format: 1.6 MB floppy, 1024-B sectors, new-map FSM.
    FFormat,
    /// HD: variable size; new-map FSM.
    Hard,
}

#[derive(Debug, Clone)]
pub struct DiscRecord {
    pub log2_sector_size: u8,
    pub sectors_per_track: u8,
    pub heads: u8,
    pub density: u8,
    pub id_len: u8,
    /// log2(bytes per map bit). Named `log2bpmb` in the Linux kernel
    /// (`fs/adfs/adfs.h`). Older Rusty-Backup code mislabeled this as
    /// `log2_map_bits`. Combined with `log2_sector_size` it yields
    /// `map2blk = log2bpmb - log2secsize`, the bit shift that converts
    /// a fragment's map-bit count to a physical sector count.
    pub log2bpmb: u8,
    pub skew: u8,
    pub boot_option: u8,
    pub low_sector: u8,
    /// Low byte of the zone count. The full count is
    /// `(nzones_high << 8) | nzones`, populated from disc-record bytes
    /// 0x09 (low) and 0x2A (high). Single-byte HD layouts fit in `nzones`
    /// alone; HD+ ("big") layouts use both.
    pub nzones: u8,
    pub zone_spare: u16,
    pub root: u32,
    /// Disc size in BYTES (low 32 bits). Confusingly named in older
    /// docs as "size in sectors" — verified against marutan.net's
    /// blank256E.hdf (256 MB) and blank1024Eplus.hdf (1 GB): the field
    /// value equals the byte length of the file, not its sector count.
    pub disc_size_bytes: u32,
    pub disc_id: u16,
    pub disc_name: String,
    /// High 32 bits of disc size; non-zero only on big (HD+) discs.
    /// Disc record byte 0x24.
    pub disc_size_high: u32,
    /// Low nibble of byte 0x28. Controls the within-fragment block
    /// addressing scale used by `__adfs_block_map`:
    /// `block += ((indaddr & 0xFF) - 1) << log2sharesize`.
    pub log2sharesize: u8,
    /// Bit 0 of byte 0x29. Set on big-format (HD+) discs that use
    /// `nzones_high` and `disc_size_high`.
    pub big_flag: u8,
    /// High byte of nzones (byte 0x2A).
    pub nzones_high: u8,
    /// Format version at byte 0x2C; non-zero on F+ (big) layouts where
    /// the root directory size is variable.
    pub format_version: u32,
    /// Root directory size in bytes (byte 0x30). Meaningful only on F+
    /// (`format_version != 0`); otherwise the root is the fixed-size F-
    /// layout small directory.
    pub root_size: u32,
}

impl DiscRecord {
    pub fn parse(buf: &[u8]) -> Result<Self, FilesystemError> {
        if buf.len() < 0x26 {
            return Err(FilesystemError::InvalidData(
                "ADFS Disc Record buffer too small".into(),
            ));
        }
        let log2_sector_size = buf[0x00];
        if !(8..=11).contains(&log2_sector_size) {
            return Err(FilesystemError::InvalidData(format!(
                "ADFS log2(sector_size) {log2_sector_size} not 8..=11"
            )));
        }
        let sectors_per_track = buf[0x01];
        let heads = buf[0x02];
        let density = buf[0x03];
        let id_len = buf[0x04];
        let log2bpmb = buf[0x05];
        let skew = buf[0x06];
        let boot_option = buf[0x07];
        let low_sector = buf[0x08];
        let nzones = buf[0x09];
        let zone_spare = LittleEndian::read_u16(&buf[0x0A..0x0C]);
        let root = LittleEndian::read_u32(&buf[0x0C..0x10]);
        let disc_size_bytes = LittleEndian::read_u32(&buf[0x10..0x14]);
        let disc_id = LittleEndian::read_u16(&buf[0x14..0x16]);
        let name_bytes = &buf[0x16..0x20];
        let disc_name = name_bytes
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| {
                if (0x20..=0x7E).contains(&b) {
                    b as char
                } else {
                    ' '
                }
            })
            .collect::<String>()
            .trim_end()
            .to_string();
        // Extended fields (kernel's struct adfs_discrecord extends to 60 bytes;
        // only the first 32 bytes are mandatory for E-format floppies).
        let disc_size_high = if buf.len() >= 0x28 {
            LittleEndian::read_u32(&buf[0x24..0x28])
        } else {
            0
        };
        let log2sharesize = if buf.len() >= 0x29 {
            buf[0x28] & 0x0F
        } else {
            0
        };
        let big_flag = if buf.len() >= 0x2A {
            buf[0x29] & 0x01
        } else {
            0
        };
        let nzones_high = if buf.len() >= 0x2B { buf[0x2A] } else { 0 };
        let format_version = if buf.len() >= 0x30 {
            LittleEndian::read_u32(&buf[0x2C..0x30])
        } else {
            0
        };
        let root_size = if buf.len() >= 0x34 {
            LittleEndian::read_u32(&buf[0x30..0x34])
        } else {
            0
        };
        Ok(Self {
            log2_sector_size,
            sectors_per_track,
            heads,
            density,
            id_len,
            log2bpmb,
            skew,
            boot_option,
            low_sector,
            nzones,
            zone_spare,
            root,
            disc_size_bytes,
            disc_id,
            disc_name,
            disc_size_high,
            log2sharesize,
            big_flag,
            nzones_high,
            format_version,
            root_size,
        })
    }

    pub fn sector_size(&self) -> u32 {
        1u32 << self.log2_sector_size
    }

    /// Total zone count (combines `nzones` low byte with `nzones_high`).
    pub fn total_zones(&self) -> u32 {
        (self.nzones as u32) | ((self.nzones_high as u32) << 8)
    }

    /// Total disc size in bytes (combines low + high words).
    pub fn total_disc_size(&self) -> u64 {
        ((self.disc_size_high as u64) << 32) | (self.disc_size_bytes as u64)
    }

    /// `map2blk = log2bpmb - log2secsize`: signed shift applied to a
    /// fragment's map-bit count to obtain its physical sector count.
    /// Positive means 1 map bit > 1 sector (typical for HD); negative
    /// means 1 map bit < 1 sector (typical for floppies).
    pub fn map2blk(&self) -> i32 {
        self.log2bpmb as i32 - self.log2_sector_size as i32
    }

    /// `(8 << log2secsize) - zone_spare`: the number of map bits one
    /// zone covers.
    pub fn zone_size_bits(&self) -> u32 {
        (8u32 << self.log2_sector_size) - self.zone_spare as u32
    }

    pub fn ids_per_zone(&self) -> u32 {
        self.zone_size_bits() / (self.id_len as u32 + 1)
    }

    pub fn classify(&self) -> AdfsFormat {
        let ss = self.sector_size();
        let total = self.disc_size_bytes as u64;
        let _ = ss; // sector size kept in the match arms below

        match (ss, total) {
            (256, _) => AdfsFormat::DFormat,
            (1024, n) if n <= 800 * 1024 => AdfsFormat::EFormat,
            (1024, n) if n <= 2 * 1024 * 1024 => AdfsFormat::FFormat,
            _ => AdfsFormat::Hard,
        }
    }
}

/// 32-bit signed-arithmetic-shift-left used throughout the ADFS map
/// math. Mirrors the kernel's `signed_asl` (`fs/adfs/adfs.h`): positive
/// shift is left, negative is right. Done on i64 to keep room for the
/// sub-2-TB sector-address space the kernel asserts in
/// `adfs_checkdiscrecord`.
fn signed_asl(val: i64, shift: i32) -> i64 {
    if shift >= 0 {
        val << shift
    } else {
        val >> (-shift)
    }
}

/// The map-bit position of `ADFS_ROOT_FRAG` (frag 2). The root frag is
/// special-cased by `adfs_map_lookup` — its zone search starts at
/// `nzones >> 1` rather than `frag_id / ids_per_zone`. See kernel
/// `fs/adfs/map.c::adfs_map_lookup`.
const ADFS_ROOT_FRAG: u32 = 2;

/// Filecore "new-map" Free Space Map (FSM) walker.
///
/// The FSM lives in `nzones` sectors at the physical disc address
/// `map_addr` (derived from the disc record). Every fragment carved
/// from the disc is described by a `(frag_id, length)` bitstream entry
/// inside one or more zones — see `fs/adfs/map.c` and the head comment
/// of that file for the bit-layout. The kernel's primitives are
/// faithfully mirrored here: `lookup_zone`, `scan_map`, `map_lookup`.
///
/// **Address formula** (verified against `CROS42.hdf` and `ICEBIRD.hdf`):
///
/// ```text
/// zone_size_bits = (8 << log2secsize) - zone_spare
/// ids_per_zone   = zone_size_bits / (idlen + 1)
/// map_addr_bits  = (nzones>>1) * zone_size_bits - ((nzones > 1) ? 480 : 0)
/// map_addr_sec   = signed_asl(map_addr_bits, map2blk)
/// ```
///
/// where `map2blk = log2bpmb - log2secsize`. Each zone occupies one
/// sector; the FSM as a whole is at `map_addr_sec`.
///
/// For an `indaddr` from a directory entry (kernel
/// `__adfs_block_map`):
///
/// ```text
/// frag_id     = indaddr >> 8
/// secoff_lo   = (indaddr & 0xFF) - 1  (zero when low byte is zero)
/// block_sec  += secoff_lo << log2sharesize
/// ```
///
/// `block_sec` is the in-frag sector offset; `map_lookup` walks the
/// zones starting at `zone = (frag_id == ADFS_ROOT_FRAG ? nzones>>1 :
/// frag_id / ids_per_zone)` and returns the physical sector.
pub struct AdfsFsm {
    /// Raw zone bytes, one entry per zone. Length equals
    /// `disc_record.total_zones()`. Each Vec has exactly `sector_size`
    /// bytes (or fewer on the last zone — kernel clamps `dm_endbit`).
    pub zones: Vec<Vec<u8>>,
    /// Physical sector address of the FSM (zone 0).
    pub map_addr_sec: u64,
    idlen: u32,
    log2bpmb: u8,
    log2sharesize: u8,
    nzones: u32,
    ids_per_zone: u32,
    zone_size_bits: u32,
    map2blk: i32,
    /// Map-bit length of the whole disc minus the trailing-zone clamp.
    /// Used to clamp `dm_endbit` on the last zone.
    disc_size_map_bits: u64,
}

impl AdfsFsm {
    /// Read the FSM from `reader` at `partition_offset`.  This eagerly
    /// loads every zone into memory; the largest legal HD disc has
    /// `nzones * sector_size` ≤ a few MiB.
    pub fn read<R: Read + Seek>(
        reader: &mut R,
        partition_offset: u64,
        dr: &DiscRecord,
    ) -> Result<Self, FilesystemError> {
        let nzones = dr.total_zones();
        if nzones == 0 {
            return Err(FilesystemError::InvalidData(
                "ADFS FSM: nzones is zero".into(),
            ));
        }
        let sector_size = dr.sector_size();
        let zone_size_bits = dr.zone_size_bits();
        let ids_per_zone = dr.ids_per_zone();
        let map2blk = dr.map2blk();

        // map_addr_bits = (nzones>>1) * zone_size_bits - ((nzones > 1) ? 480 : 0)
        let map_addr_bits =
            (nzones as u64 >> 1) * zone_size_bits as u64 - if nzones > 1 { 60 * 8 } else { 0 };
        let map_addr_sec = signed_asl(map_addr_bits as i64, map2blk) as u64;
        let map_addr_byte = partition_offset + map_addr_sec * sector_size as u64;

        // disc_size_map_bits: total disc size shifted to map-bit units.
        // Mirrors `size = adfs_disc_size(dr) >> log2bpmb` in the kernel.
        let disc_size_map_bits = dr.total_disc_size() >> dr.log2bpmb;

        let mut zones = Vec::with_capacity(nzones as usize);
        for zi in 0..nzones {
            let off = map_addr_byte + (zi as u64) * sector_size as u64;
            reader.seek(SeekFrom::Start(off))?;
            let mut buf = vec![0u8; sector_size as usize];
            // Use read (not read_exact) so a truncated tail zone doesn't
            // fail open — kernel clamps `dm_endbit` on the last zone.
            let n = reader.read(&mut buf)?;
            buf.truncate(n);
            zones.push(buf);
        }

        Ok(Self {
            zones,
            map_addr_sec,
            idlen: dr.id_len as u32,
            log2bpmb: dr.log2bpmb,
            log2sharesize: dr.log2sharesize,
            nzones,
            ids_per_zone,
            zone_size_bits,
            map2blk,
            disc_size_map_bits,
        })
    }

    /// `(dm_startbit, dm_endbit, dm_startblk)` for zone `zi`.
    ///
    /// Per kernel `adfs_map_layout`:
    /// - zone 0:    startbit = 32 + 480; startblk = 0
    /// - zone N>0:  startbit = 32;       startblk = N * zone_size - 480
    /// - last zone: endbit clamped to `32 + (disc_size_map_bits -
    ///   ((nzones-1) * zone_size - 480))`
    fn zone_metadata(&self, zi: u32) -> (u32, u32, i64) {
        let zone_size = self.zone_size_bits;
        let (startbit, startblk) = if zi == 0 {
            (32 + 60 * 8, 0i64)
        } else {
            (32, zi as i64 * zone_size as i64 - 60 * 8)
        };
        let mut endbit = 32 + zone_size;
        if zi == self.nzones - 1 {
            // Clamp endbit on the last zone the way the kernel does:
            //   size = disc_size_map_bits - ((nzones-1) * zone_size - 480)
            //   dm[nzones-1].dm_endbit = 32 + size
            let consumed = (self.nzones - 1) as i64 * zone_size as i64 - 60 * 8;
            let remaining = self.disc_size_map_bits as i64 - consumed;
            if remaining >= 0 {
                let clamped = 32 + remaining as u64;
                if clamped < endbit as u64 {
                    endbit = clamped as u32;
                }
            }
        }
        (startbit, endbit, startblk)
    }

    /// Read up to 19 bits at an arbitrary bit alignment, little-endian.
    /// Mirrors the kernel's `GET_FRAG_ID` macro.
    fn get_frag_id(zone: &[u8], start: u32, idmask: u32) -> u32 {
        let byte_off = (start >> 3) as usize;
        // Fetch up to 4 bytes — frag IDs are ≤ 19 bits; the shift below
        // accounts for the within-byte offset.
        let mut acc: u32 = 0;
        for k in 0..4 {
            if byte_off + k < zone.len() {
                acc |= (zone[byte_off + k] as u32) << (k * 8);
            }
        }
        (acc >> (start & 7)) & idmask
    }

    /// `find_next_bit_le`-style scan: return the position of the next
    /// `1` bit on or after `start`, capped at `end`. Returns `end` if
    /// no set bit is found in the range.
    fn find_next_set_bit(zone: &[u8], start: u32, end: u32) -> u32 {
        let mut p = start;
        while p < end {
            let byte_off = (p >> 3) as usize;
            if byte_off >= zone.len() {
                return end;
            }
            if (zone[byte_off] >> (p & 7)) & 1 == 1 {
                return p;
            }
            p += 1;
        }
        end
    }

    /// Walk a single zone looking for `frag_id`. Mirrors the kernel
    /// `lookup_zone`:
    /// - track the freelist via the leading 15-bit pointer at bit 8.
    /// - iterate fragments. On a match, if `*offset < length`, return
    ///   the bit position. Otherwise consume the length and continue.
    fn lookup_zone(&self, zi: u32, frag_id: u32, offset: &mut i64) -> Option<u32> {
        let (startbit, endbit, _startblk) = self.zone_metadata(zi);
        let zone = self.zones.get(zi as usize)?;
        let idmask = (1u32 << self.idlen) - 1;

        // freelink at bit 8 — kernel uses idmask & 0x7fff for the
        // freelist pointer. A zero pointer means no free fragments.
        let mut freelink: u32 = {
            let f = Self::get_frag_id(zone, 8, idmask & 0x7fff);
            if f != 0 {
                8 + f
            } else {
                0
            }
        };

        let mut start = startbit;
        // Safety cap: a fragment must be at least `idlen + 1` bits, so
        // the zone can hold at most `zone_size_bits / (idlen + 1)`
        // entries. Cap at 2x that to absorb degenerate cases without
        // looping forever.
        let cap = 2 * (self.zone_size_bits / (self.idlen + 1)).max(1) + 16;
        let mut iter = 0;
        while start + self.idlen < endbit && iter < cap {
            iter += 1;
            let frag = Self::get_frag_id(zone, start, idmask);
            let fragend = Self::find_next_set_bit(zone, start + self.idlen, endbit);
            if fragend >= endbit {
                // Oversized fragment — corrupt zone; bail out.
                return None;
            }
            let length = fragend + 1 - start;

            if start == freelink {
                // Free-list link: low bits are the gap to the next free
                // fragment, NOT a frag_id we care about.
                freelink += frag & 0x7fff;
            } else if frag == frag_id {
                if (*offset) < length as i64 {
                    return Some(start + *offset as u32);
                }
                *offset -= length as i64;
            }
            start = fragend + 1;
        }
        None
    }

    /// Equivalent of kernel `scan_map`: starting at `zone`, look for
    /// `frag_id`. If absent in that zone, wrap to the next zone (mod
    /// `nzones`) until exhausted. Returns a *disc-wide* map-bit position
    /// `(start_in_zone - dm_startbit + dm_startblk)`.
    fn scan_map(&self, mut zone: u32, frag_id: u32, mapoff: i64) -> Option<i64> {
        let mut offset = mapoff;
        let mut zones_left = self.nzones;
        while zones_left > 0 {
            if let Some(hit) = self.lookup_zone(zone, frag_id, &mut offset) {
                let (startbit, _endbit, startblk) = self.zone_metadata(zone);
                return Some(hit as i64 - startbit as i64 + startblk);
            }
            zone = (zone + 1) % self.nzones;
            zones_left -= 1;
        }
        None
    }

    /// Translate a 24-bit-or-larger ADFS indaddr + in-file sector offset
    /// to a physical sector address. Mirrors `__adfs_block_map` followed
    /// by `adfs_map_lookup`.
    ///
    /// Returns `None` if the fragment isn't present in the map (a
    /// corrupt indaddr or a deleted file).
    pub fn map_lookup(&self, indaddr: u32, block_sec_in_file: u64) -> Option<u64> {
        let frag_id = indaddr >> 8;
        let mut block = block_sec_in_file as i64;
        // `__adfs_block_map`: add (indaddr&0xFF - 1) << log2sharesize
        // when the low byte is non-zero.
        let lo = (indaddr & 0xFF) as i32;
        if lo != 0 {
            block += ((lo - 1) as i64) << self.log2sharesize;
        }

        let zone = if frag_id == ADFS_ROOT_FRAG {
            self.nzones >> 1
        } else if self.ids_per_zone > 0 {
            (frag_id / self.ids_per_zone) % self.nzones
        } else {
            0
        };
        if zone >= self.nzones {
            return None;
        }

        // Convert in-file sector offset to map-bit offset.
        let mapoff = signed_asl(block, -self.map2blk);
        let result = self.scan_map(zone, frag_id, mapoff)?;
        // secoff = block - (mapoff << map2blk)  (sub-map-bit remainder)
        let secoff = block - signed_asl(mapoff, self.map2blk);
        let sector_addr = secoff + signed_asl(result, self.map2blk);
        if sector_addr < 0 {
            None
        } else {
            Some(sector_addr as u64)
        }
    }

    /// Total free space in bytes — sum of free-list fragment lengths
    /// across every zone times `2^log2bpmb`. Mirrors the kernel
    /// `scan_free_map` x `adfs_map_statfs` path.
    pub fn free_bytes(&self) -> u64 {
        let mut total_map_bits: u64 = 0;
        let frag_idlen = self.idlen.min(15);
        let frag_idmask = (1u32 << frag_idlen) - 1;
        for zi in 0..self.nzones {
            let (_startbit, endbit, _) = self.zone_metadata(zi);
            let Some(zone) = self.zones.get(zi as usize) else {
                continue;
            };
            // Kernel `scan_free_map` starts at bit 8.
            let mut start = 8u32;
            let first = Self::get_frag_id(zone, start, frag_idmask);
            if first == 0 {
                continue; // no free frags in this zone
            }
            // Walk the free chain.
            let cap = 2 * (self.zone_size_bits / (self.idlen + 1)).max(1) + 16;
            let mut iter = 0;
            let mut frag = first;
            loop {
                if iter > cap {
                    break;
                }
                iter += 1;
                start += frag;
                frag = Self::get_frag_id(zone, start, frag_idmask);
                let fragend = Self::find_next_set_bit(zone, start + self.idlen, endbit);
                if fragend >= endbit {
                    break;
                }
                total_map_bits += (fragend + 1 - start) as u64;
                if frag < self.idlen + 1 {
                    break;
                }
            }
        }
        total_map_bits << self.log2bpmb
    }
}

#[derive(Debug, Clone)]
pub struct AdfsDirEntry {
    pub name: String,
    pub load_addr: u32,
    pub exec_addr: u32,
    pub file_length: u32,
    /// 24-bit indirect disc address. For new-map formats this is the
    /// ADFS `(frag_id, in-frag-sector-offset)` indaddr in the kernel
    /// sense (`frag_id = indaddr >> 8`, see `AdfsFsm::map_lookup`).
    /// For D-format old-map discs it's a byte offset.
    pub indirect_disc_addr: u32,
    pub attrs: u8,
}

impl AdfsDirEntry {
    pub fn is_directory(&self) -> bool {
        self.attrs & 0x08 != 0
    }
    pub fn is_locked(&self) -> bool {
        self.attrs & 0x04 != 0
    }
}

/// Parse one directory entry. Returns `None` when the first byte == 0
/// (end of directory).
pub fn parse_dir_entry(buf: &[u8; DIR_ENTRY_SIZE]) -> Option<AdfsDirEntry> {
    if buf[0] == 0 {
        return None;
    }
    let name_bytes = &buf[0..10];
    let name: String = name_bytes
        .iter()
        .take_while(|&&b| b != 0 && b != 0x20)
        .map(|&b| {
            if (0x20..=0x7E).contains(&b) {
                b as char
            } else {
                '_'
            }
        })
        .collect();
    if name.is_empty() {
        return None;
    }
    let load_addr = LittleEndian::read_u32(&buf[10..14]);
    let exec_addr = LittleEndian::read_u32(&buf[14..18]);
    let file_length = LittleEndian::read_u32(&buf[18..22]);
    // 24-bit indirect disc address (little-endian).
    let indirect_disc_addr =
        u32::from(buf[22]) | (u32::from(buf[23]) << 8) | (u32::from(buf[24]) << 16);
    let attrs = buf[25];
    Some(AdfsDirEntry {
        name,
        load_addr,
        exec_addr,
        file_length,
        indirect_disc_addr,
        attrs,
    })
}

/// Scan for the Disc Record across the four well-known candidate
/// offsets. Returns `(byte_offset, parsed_dr)` on first hit. Each
/// candidate is read into a 64-byte buffer and run through
/// [`DiscRecord::parse`]; the first one that produces a syntactically
/// valid record (plausible sector size, sane geometry, non-zero root
/// and disc size) wins. Real-world samples we've cross-checked use
/// `0xFC0` (the canonical zone-0-size-4096 location); older sources
/// claimed `0xDC0` but neither marutan.net blank disc puts it there.
fn find_disc_record<R: Read + Seek + Send>(
    reader: &mut R,
    partition_offset: u64,
) -> Result<(u64, DiscRecord), FilesystemError> {
    let mut last_err: Option<FilesystemError> = None;
    for &cand in DISC_RECORD_CANDIDATES {
        if reader
            .seek(SeekFrom::Start(partition_offset + cand))
            .is_err()
        {
            continue;
        }
        let mut buf = [0u8; 64];
        if reader.read_exact(&mut buf).is_err() {
            continue;
        }
        match DiscRecord::parse(&buf) {
            Ok(dr) => {
                // Extra sanity: root pointer + disc size must be non-zero
                // (a zone of zeros happens to satisfy the per-byte range
                // checks in `parse` if those fields aren't checked).
                if dr.root != 0 && dr.disc_size_bytes != 0 {
                    return Ok((cand, dr));
                }
            }
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        FilesystemError::InvalidData("ADFS: no Disc Record at any candidate offset".into())
    }))
}

pub struct AdfsFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    pub disc_record: DiscRecord,
    pub format: AdfsFormat,
    /// FSM walker. `Some` for new-map formats (E, F, HD); `None` for
    /// D-format (old-map) which addresses fragments by raw sector
    /// number rather than by frag id.
    pub fsm: Option<AdfsFsm>,
}

/// Fixed F-format directory size (`ADFS_NEWDIR_SIZE` in the kernel).
/// Both Hugo (HD/E/F root) and Nick (later RISC OS) flavours occupy
/// 2048 bytes on disc when `format_version == 0`.
const ADFS_NEWDIR_SIZE: u64 = 2048;

impl<R: Read + Seek + Send> AdfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let (_offset, disc_record) = find_disc_record(&mut reader, partition_offset)?;
        let format = disc_record.classify();
        // For new-map formats build the FSM eagerly so subsequent
        // directory / file reads can resolve fragment IDs. D-format
        // (old-map) skips this — fragments are addressed by raw sector
        // number on those discs.
        let fsm = match format {
            AdfsFormat::DFormat => None,
            _ => Some(AdfsFsm::read(&mut reader, partition_offset, &disc_record)?),
        };
        Ok(Self {
            reader,
            partition_offset,
            disc_record,
            format,
            fsm,
        })
    }

    /// Translate an ADFS indirect disc address (24- or 32-bit) plus an
    /// in-file sector offset to an absolute byte offset on the backing
    /// reader.
    ///
    /// For new-map formats this walks the FSM. For D-format (no FSM)
    /// the indaddr is treated as a byte offset directly, matching the
    /// behaviour the original layout-preserving reader relied on.
    fn resolve_byte(&self, indaddr: u32, block_sec_in_file: u64) -> Result<u64, FilesystemError> {
        let sector_size = self.disc_record.sector_size() as u64;
        let abs_byte = if let Some(fsm) = self.fsm.as_ref() {
            let sec = fsm.map_lookup(indaddr, block_sec_in_file).ok_or_else(|| {
                FilesystemError::InvalidData(format!(
                    "ADFS: fragment {} (block {}) not in FSM",
                    indaddr >> 8,
                    block_sec_in_file
                ))
            })?;
            self.partition_offset + sec * sector_size
        } else {
            // D-format fallback: indaddr is the byte offset.
            self.partition_offset + indaddr as u64 + block_sec_in_file * sector_size
        };
        Ok(abs_byte)
    }

    /// Read a directory block: the fixed 2048-byte F-format layout. The
    /// kernel calls into `adfs_f_dir_ops::read` for this — we replicate
    /// the read by walking sector-by-sector through `resolve_byte`.
    fn read_dir_block(&mut self, indaddr: u32) -> Result<Vec<u8>, FilesystemError> {
        let sector_size = self.disc_record.sector_size() as u64;
        let dir_size = if self.disc_record.format_version != 0 {
            self.disc_record.root_size as u64
        } else {
            ADFS_NEWDIR_SIZE
        };
        // Round up to a whole-sector count.
        let n_sectors = dir_size.div_ceil(sector_size);
        let mut out = Vec::with_capacity((n_sectors * sector_size) as usize);
        for s in 0..n_sectors {
            let byte_off = self.resolve_byte(indaddr, s)?;
            self.reader.seek(SeekFrom::Start(byte_off))?;
            let mut buf = vec![0u8; sector_size as usize];
            self.reader.read_exact(&mut buf)?;
            out.extend_from_slice(&buf);
        }
        out.truncate(dir_size as usize);
        Ok(out)
    }

    /// Parse a fixed-layout F-format directory block (small / Hugo /
    /// Nick — all 26-byte-entry variants). Returns the list of valid
    /// entries; stops at the first zero-name entry (kernel convention).
    fn parse_dir_block(block: &[u8]) -> Result<Vec<AdfsDirEntry>, FilesystemError> {
        if block.len() < DIR_SMALL_HEADER + DIR_ENTRY_SIZE {
            return Err(FilesystemError::InvalidData(
                "ADFS directory block too small".into(),
            ));
        }
        if !matches!(&block[1..5], b"Hugo" | b"Nick") {
            return Err(FilesystemError::InvalidData(format!(
                "ADFS directory magic mismatch: {:?}",
                &block[1..5]
            )));
        }
        let mut entries = Vec::new();
        let mut off = DIR_SMALL_HEADER;
        for _ in 0..DIR_SMALL_MAX_ENTRIES.max(77) {
            if off + DIR_ENTRY_SIZE > block.len() {
                break;
            }
            let buf: &[u8; DIR_ENTRY_SIZE] = block[off..off + DIR_ENTRY_SIZE]
                .try_into()
                .expect("checked size above");
            match parse_dir_entry(buf) {
                Some(e) => entries.push(e),
                None => break,
            }
            off += DIR_ENTRY_SIZE;
        }
        Ok(entries)
    }

    /// Read and parse the root directory.
    fn read_root_directory(&mut self) -> Result<Vec<AdfsDirEntry>, FilesystemError> {
        let block = self.read_dir_block(self.disc_record.root)?;
        Self::parse_dir_block(&block)
    }
}

impl<R: Read + Seek + Send> Filesystem for AdfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new()); // subdir traversal deferred
        }
        let entries = self.read_root_directory()?;
        let mut out = Vec::with_capacity(entries.len());
        for de in entries {
            let path = format!("/{}", de.name);
            let mut fe = if de.is_directory() {
                FileEntry::new_directory(de.name.clone(), path, de.indirect_disc_addr as u64)
            } else {
                FileEntry::new_file(
                    de.name.clone(),
                    path,
                    de.file_length as u64,
                    de.indirect_disc_addr as u64,
                )
            };
            if de.is_locked() {
                fe.special_type = Some("Locked".into());
            }
            out.push(fe);
        }
        out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let indaddr = entry.location as u32;
        let want = (entry.size as usize).min(max_bytes);
        let sector_size = self.disc_record.sector_size() as usize;
        let mut out = Vec::with_capacity(want);
        let mut remaining = want;
        let mut block_idx: u64 = 0;
        while remaining > 0 {
            let byte_off = self.resolve_byte(indaddr, block_idx)?;
            self.reader.seek(SeekFrom::Start(byte_off))?;
            let take = remaining.min(sector_size);
            let mut buf = vec![0u8; take];
            self.reader.read_exact(&mut buf)?;
            out.extend_from_slice(&buf);
            remaining -= take;
            block_idx += 1;
        }
        Ok(out)
    }

    fn fs_type(&self) -> &str {
        match self.format {
            AdfsFormat::DFormat => "ADFS (D-format)",
            AdfsFormat::EFormat => "ADFS (E-format)",
            AdfsFormat::FFormat => "ADFS (F-format)",
            AdfsFormat::Hard => "ADFS (HD)",
        }
    }

    fn volume_label(&self) -> Option<&str> {
        if self.disc_record.disc_name.is_empty() {
            None
        } else {
            Some(&self.disc_record.disc_name)
        }
    }

    fn total_size(&self) -> u64 {
        self.disc_record.total_disc_size()
    }

    fn used_size(&self) -> u64 {
        // FSM-driven free-space accounting (kernel `adfs_map_statfs`).
        // D-format old-map discs have no FSM — return 0 (unknown).
        match self.fsm.as_ref() {
            Some(fsm) => self.total_size().saturating_sub(fsm.free_bytes()),
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Write `value` as `nbits` little-endian bits into `buf` starting
    /// at bit position `start`. Used to populate the FSM zone bitstream
    /// the same way the kernel reads it via `GET_FRAG_ID`.
    fn write_bits_le(buf: &mut [u8], start: u32, nbits: u32, value: u64) {
        for k in 0..nbits {
            let pos = start + k;
            let bit = ((value >> k) & 1) as u8;
            if bit == 1 {
                buf[(pos >> 3) as usize] |= 1 << (pos & 7);
            }
        }
    }

    /// Write a Filecore fragment of `length_bits` map bits into the
    /// zone bitstream at bit `start`. The fragment is `idlen` bits of
    /// id, then `(length_bits - idlen - 1)` zero bits, then one
    /// terminating 1-bit.
    fn write_frag(buf: &mut [u8], start: u32, idlen: u32, length_bits: u32, frag_id: u32) {
        assert!(length_bits > idlen, "frag too short");
        write_bits_le(buf, start, idlen, frag_id as u64);
        // Terminator at start + length_bits - 1.
        let term = start + length_bits - 1;
        buf[(term >> 3) as usize] |= 1 << (term & 7);
    }

    /// Build a synthetic 12-sector E-format disc with a populated FSM
    /// and a single test file. Layout (sector 1024 B):
    ///
    /// ```text
    /// sector 0   FSM zone 0 (zone hdr + DR + bitstream)
    /// sector 1   unused (covered by frag 0)
    /// sector 2-3 root directory (Hugo magic, 1 entry: HELLO)
    /// sector 4-5 file payload
    /// sector 6+  free
    /// ```
    ///
    /// FSM params: log2_secsize=10, idlen=15, log2bpmb=7, nzones=1,
    /// zone_spare=1312. Matches the real arc-04 E-format DR exactly so
    /// the same formula validates: `map_addr_sec = 0`,
    /// `dm_startbit(0) = 512`, `map2blk = -3` (1 map bit = 128 B).
    ///
    /// Fragments in zone 0 bitstream (each 16 map bits = 2 sectors):
    /// - bit 512: frag 0 length 16  (FSM + sector 1 padding)
    /// - bit 528: frag 2 length 16  (root dir at sector 2..3)
    /// - bit 544: frag 5 length 16  (file payload at sector 4..5)
    /// - bit 560: frag 0 length 6352 (rest free; closes the bitstream)
    fn build_eformat_with_one_file() -> Vec<u8> {
        const SECTOR_SIZE: usize = 1024;
        const TOTAL_BYTES: usize = 12 * SECTOR_SIZE;
        const ROOT_SECTOR: u32 = 2;
        const FILE_SECTOR: u32 = 4;
        const IDLEN: u32 = 15;

        let mut disk = vec![0u8; TOTAL_BYTES];

        // Disc Record at byte 4 (zone 0 starts at byte 0; the 4-byte
        // zone header is at bytes 0..3 and the DR follows at byte 4 —
        // matches the kernel's adfs_validate_dr0 path).
        let dr_off = 0x04usize;
        disk[dr_off] = 10; // log2(1024)
        disk[dr_off + 0x01] = 5;
        disk[dr_off + 0x02] = 2;
        disk[dr_off + 0x03] = 2;
        disk[dr_off + 0x04] = IDLEN as u8;
        disk[dr_off + 0x05] = 7; // log2bpmb -> 1 map bit = 128 B
        disk[dr_off + 0x06] = 0;
        disk[dr_off + 0x07] = 0;
        disk[dr_off + 0x08] = 0;
        disk[dr_off + 0x09] = 1; // nzones
        LittleEndian::write_u16(&mut disk[dr_off + 0x0A..dr_off + 0x0C], 1312);
        // dr.root = (frag 2, lo=0) = 0x200
        LittleEndian::write_u32(&mut disk[dr_off + 0x0C..dr_off + 0x10], 0x200);
        // disc_size in bytes.
        LittleEndian::write_u32(&mut disk[dr_off + 0x10..dr_off + 0x14], TOTAL_BYTES as u32);
        LittleEndian::write_u16(&mut disk[dr_off + 0x14..dr_off + 0x16], 0xABCD);
        disk[dr_off + 0x16..dr_off + 0x20].copy_from_slice(b"TestDisc  ");

        // Zone bitstream — sector 0 bytes 64..863 hold bits 512..6911.
        // dm_startbit = 32 + ADFS_DR_SIZE_BITS = 512.
        // Free-list link at bit 8 stays zero (no free chain).
        // sectors 0..1 (FSM itself + 1 padding sector)
        write_frag(&mut disk[..SECTOR_SIZE], 512, IDLEN, 16, 0);
        // root dir at sectors 2..3
        write_frag(&mut disk[..SECTOR_SIZE], 528, IDLEN, 16, 2);
        // HELLO payload at sectors 4..5
        write_frag(&mut disk[..SECTOR_SIZE], 544, IDLEN, 16, 5);
        // Tail "free" fragment closes the bitstream up to dm_endbit
        // (32 + 6880 = 6912). length = 6912 - 560 = 6352.
        write_frag(&mut disk[..SECTOR_SIZE], 560, IDLEN, 6352, 0);

        // Root directory at byte 2048 (sector 2). Hugo magic + 1 entry.
        let root_off = ROOT_SECTOR as usize * SECTOR_SIZE;
        disk[root_off] = 0;
        disk[root_off + 1] = b'H';
        disk[root_off + 2] = b'u';
        disk[root_off + 3] = b'g';
        disk[root_off + 4] = b'o';
        let e_off = root_off + DIR_SMALL_HEADER;
        disk[e_off..e_off + 10].copy_from_slice(b"HELLO\x00\x00\x00\x00\x00");
        LittleEndian::write_u32(&mut disk[e_off + 10..e_off + 14], 0xFFFFFFFF);
        LittleEndian::write_u32(&mut disk[e_off + 14..e_off + 18], 0);
        LittleEndian::write_u32(&mut disk[e_off + 18..e_off + 22], 32);
        // indaddr = (frag 5, lo=0) = 0x500
        disk[e_off + 22] = 0x00;
        disk[e_off + 23] = 0x05;
        disk[e_off + 24] = 0x00;
        disk[e_off + 25] = 0x03; // R/W attributes

        // File payload at byte 4096 (sector 4).
        let file_off = FILE_SECTOR as usize * SECTOR_SIZE;
        let payload = b"adfs synthetic test file content";
        disk[file_off..file_off + payload.len()].copy_from_slice(payload);

        disk
    }

    #[test]
    fn parses_disc_record_and_classifies_eformat() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let fs = AdfsFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.disc_record.sector_size(), 1024);
        assert_eq!(fs.disc_record.total_disc_size(), 12 * 1024);
        assert_eq!(fs.format, AdfsFormat::EFormat);
        assert_eq!(fs.fs_type(), "ADFS (E-format)");
        assert_eq!(fs.volume_label(), Some("TestDisc"));
    }

    #[test]
    fn fsm_resolves_root_indaddr_to_correct_sector() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let fs = AdfsFilesystem::open(cur, 0).unwrap();
        let fsm = fs.fsm.as_ref().expect("E-format must build FSM");
        // dr.root=0x200 -> (frag 2, lo=0) -> sector 2.
        let sec = fsm.map_lookup(0x200, 0).expect("frag 2 in map");
        assert_eq!(sec, 2);
        // Second sector of root dir: block 1 lookup.
        let sec1 = fsm.map_lookup(0x200, 1).expect("frag 2 in map");
        assert_eq!(sec1, 3);
    }

    #[test]
    fn fsm_resolves_file_indaddr_to_correct_sector() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let fs = AdfsFilesystem::open(cur, 0).unwrap();
        let fsm = fs.fsm.as_ref().unwrap();
        let sec0 = fsm.map_lookup(0x500, 0).expect("frag 5 in map");
        let sec1 = fsm.map_lookup(0x500, 1).expect("frag 5 in map");
        assert_eq!(sec0, 4);
        assert_eq!(sec1, 5);
    }

    #[test]
    fn fsm_lookup_returns_none_for_unknown_fragment() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let fs = AdfsFilesystem::open(cur, 0).unwrap();
        let fsm = fs.fsm.as_ref().unwrap();
        assert!(fsm.map_lookup(0xABCD00, 0).is_none());
    }

    #[test]
    fn lists_root_directory_with_one_file() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let mut fs = AdfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "HELLO");
        assert_eq!(entries[0].size, 32);
    }

    #[test]
    fn reads_file_via_fsm_walker_byte_exact() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let mut fs = AdfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let data = fs.read_file(&entries[0], 4096).unwrap();
        assert_eq!(&data, b"adfs synthetic test file content");
    }

    #[test]
    fn parse_dir_entry_returns_none_on_zero_first_byte() {
        let buf = [0u8; DIR_ENTRY_SIZE];
        assert!(parse_dir_entry(&buf).is_none());
    }

    #[test]
    fn parse_dir_entry_extracts_24bit_indirect_address() {
        let mut buf = [0u8; DIR_ENTRY_SIZE];
        buf[0..5].copy_from_slice(b"FOO  ");
        LittleEndian::write_u32(&mut buf[18..22], 100);
        // 24-bit indirect = 0x123456
        buf[22] = 0x56;
        buf[23] = 0x34;
        buf[24] = 0x12;
        buf[25] = 0x03;
        let entry = parse_dir_entry(&buf).unwrap();
        assert_eq!(entry.indirect_disc_addr, 0x123456);
        assert_eq!(entry.file_length, 100);
    }

    #[test]
    fn disc_record_parses_extended_60_byte_form() {
        // Validate that log2sharesize, nzones_high, disc_size_high,
        // format_version, and root_size are pulled from the extended
        // (HD/HD+) disc record layout. Mirrors CROS42's actual DR
        // header but with a synthetic small disc size.
        let mut buf = [0u8; 64];
        buf[0x00] = 9; // log2_secsize -> 512 B
        buf[0x01] = 127;
        buf[0x02] = 9;
        buf[0x03] = 0;
        buf[0x04] = 14; // idlen
        buf[0x05] = 12; // log2bpmb
        buf[0x09] = 33; // nzones (low)
        LittleEndian::write_u16(&mut buf[0x0A..0x0C], 110);
        LittleEndian::write_u32(&mut buf[0x0C..0x10], 0x243); // dr.root
        LittleEndian::write_u32(&mut buf[0x10..0x14], 0xC000_0000); // disc_size low
        LittleEndian::write_u32(&mut buf[0x24..0x28], 0x1); // disc_size high
        buf[0x28] = 0x04; // log2sharesize (low nibble)
        buf[0x29] = 0x01; // big_flag (bit 0)
        buf[0x2A] = 0x02; // nzones_high
        LittleEndian::write_u32(&mut buf[0x2C..0x30], 1); // format_version
        LittleEndian::write_u32(&mut buf[0x30..0x34], 4096); // root_size
        let dr = DiscRecord::parse(&buf).unwrap();
        assert_eq!(dr.log2bpmb, 12);
        assert_eq!(dr.log2sharesize, 4);
        assert_eq!(dr.big_flag, 1);
        assert_eq!(dr.total_zones(), 33 | (2 << 8));
        assert_eq!(dr.total_disc_size(), 0x1_C000_0000);
        assert_eq!(dr.format_version, 1);
        assert_eq!(dr.root_size, 4096);
        assert_eq!(dr.map2blk(), 3);
    }
}
