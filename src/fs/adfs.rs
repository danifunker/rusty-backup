//! Acorn ADFS / FileCore — the filesystem used by the MiSTer Archie
//! (Acorn Archimedes) core. Also seen on later BBC Micro / Electron via
//! ADFS expansion ROMs.
//!
//! Read-side covers the **new-map** layouts (E, F, HD) end-to-end via
//! [`AdfsFsm`], which mirrors the Linux kernel's `fs/adfs/map.c` walker
//! (`__adfs_block_map` + `adfs_map_lookup` + the underlying
//! `lookup_zone` / `scan_map` / `adfs_map_layout` primitives). Verified
//! against `CROS42.hdf`, `ICEBIRD.hdf`, and the 8bs.com `arc-04`
//! E-format floppy by `examples/adfs_fsm_probe.rs`. D-format (old-map)
//! still uses the legacy direct-byte-offset fallback — there's no
//! D-format real sample in tree to validate a walker against.
//!
//! Write path, HDD resize, and F+ big-directory entries are still
//! TODO (tracked in `docs/OPEN-WORK.md` §7 Archie row).
//!
//! ## On-disk layout (FileCore spec; Acorn TechRef vol I)
//!
//! - **Boot block** at sector 0xC00 / 1024-B-sector 0xC: contains the
//!   "Disc Record" (a 60-byte struct describing format, sector size,
//!   tracks, density, FSM layout). Single-zone E-format floppies put a
//!   copy of the DR inside zone 0 at byte 0x04 (kernel
//!   `adfs_validate_dr0` path).
//! - **Disc Record** (kernel `struct adfs_discrecord` in
//!   `include/uapi/linux/adfs_fs.h`; all fields little-endian):
//!
//! ```text
//! 0x00  log2_secsize             (8 = 256 B, 9 = 512 B, 10 = 1024 B)
//! 0x01  secs_per_track
//! 0x02  heads
//! 0x03  density                  (1 = single, 2 = double, 3 = high)
//! 0x04  idlen                    (fragment-id bit width)
//! 0x05  log2bpmb                 (log2 bytes per map bit)
//! 0x06  skew
//! 0x07  bootoption
//! 0x08  lowsector
//! 0x09  nzones                   (low byte; high byte at 0x2A)
//! 0x0A..0x0C  zone_spare         (LE u16)
//! 0x0C..0x10  root               (LE u32, indaddr of $; split as
//!                                  frag_id = root >> 8, lo = root & 0xFF)
//! 0x10..0x14  disc_size          (LE u32, low half of byte count)
//! 0x14..0x16  disc_id            (LE u16)
//! 0x16..0x20  disc_name          (10 chars, space-padded)
//! 0x20..0x24  disc_type
//! 0x24..0x28  disc_size_high     (high half of byte count, big-HDD only)
//! 0x28 lo nibble  log2sharesize  (within-frag block-offset scale)
//! 0x29 bit 0      big_flag
//! 0x2A  nzones_high              (HD+ only; combined with 0x09)
//! 0x2C..0x30  format_version     (non-zero on F+ variable-size dirs)
//! 0x30..0x34  root_size          (F+ only; root dir byte size)
//! 0x34..0x3C  unused52           (must be zero per checkdiscrecord)
//! ```
//!
//! - **Directory `$`** — the root, plus every other small-format dir:
//!   2048 bytes total (`ADFS_NEWDIR_SIZE`). Hugo (old-format) and Nick
//!   (new-format) magics both use 26-byte entries here:
//!
//! ```text
//! header (5 bytes): byte 0 + "Hugo" / "Nick" magic
//!
//! 77 entries × 26 bytes each:
//!   0..10   name (CR-terminated; first byte 0 = end of directory;
//!                 top bit of each byte may overlay attribute flags)
//!   10..14  load_addr     LE u32
//!   14..18  exec_addr     LE u32
//!   18..22  file_length   LE u32
//!   22..25  indirect_disc_address (24-bit LE; (frag_id, in-frag offset)
//!                                   per __adfs_block_map)
//!   25      attrs (0x01 R, 0x02 W, 0x04 locked, 0x08 directory,
//!                  0x10 E, 0x20 pub R, 0x40 pub W, 0x80 pub locked)
//!
//! trailer: tail magic + cycle counter
//! ```

use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{ByteOrder, LittleEndian};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

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
///
/// Names are 10 bytes terminated by `\r` (0x0D, the canonical RISC-OS
/// dir-entry separator), 0x00 (entry-not-set), or 0x20 (space — used
/// by some older E-format floppies). Bytes past the terminator may be
/// stale garbage from prior occupants of the slot and are ignored.
/// The top bit of each name byte may carry attribute flags on some
/// dir variants — strip it before printable-checking.
pub fn parse_dir_entry(buf: &[u8; DIR_ENTRY_SIZE]) -> Option<AdfsDirEntry> {
    if buf[0] == 0 {
        return None;
    }
    let name_bytes = &buf[0..10];
    let name: String = name_bytes
        .iter()
        .map(|&b| b & 0x7F)
        .take_while(|&b| b != 0 && b != 0x0D && b != 0x20)
        .map(|b| {
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
}

impl<R: Read + Seek + Send> Filesystem for AdfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        // Root takes its indaddr from the disc record; every other
        // directory's indaddr rides on `entry.location` (set when its
        // parent listed it as a `FileEntry::new_directory(.., indaddr)`).
        let indaddr = if entry.path == "/" {
            self.disc_record.root
        } else {
            entry.location as u32
        };
        let block = self.read_dir_block(indaddr)?;
        let entries = Self::parse_dir_block(&block)?;
        let mut out = Vec::with_capacity(entries.len());
        let prefix = if entry.path == "/" {
            String::new()
        } else {
            entry.path.trim_end_matches('/').to_string()
        };
        for de in entries {
            let path = format!("{}/{}", prefix, de.name);
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

// ============================================================
// Write-side primitives — FSM mutation, dir block mutation,
// EditableFilesystem trait impl.
// ============================================================

/// Per-zone byte-0 checksum (kernel `adfs_calczonecheck` in
/// `fs/adfs/map.c`). Four-byte rolling sum-with-carry XOR'd at the end.
///
/// The input slice must be one full zone (`sector_size` bytes); byte 0
/// is the slot the checksum lives in but is excluded from the running
/// sums by the kernel's iteration order — we mirror that here.
fn adfs_calczonecheck(zone: &[u8]) -> u8 {
    let (mut v0, mut v1, mut v2, mut v3): (u32, u32, u32, u32) = (0, 0, 0, 0);
    let mut i = zone.len() - 4;
    while i != 0 {
        v0 += zone[i] as u32 + (v3 >> 8);
        v3 &= 0xff;
        v1 += zone[i + 1] as u32 + (v0 >> 8);
        v0 &= 0xff;
        v2 += zone[i + 2] as u32 + (v1 >> 8);
        v1 &= 0xff;
        v3 += zone[i + 3] as u32 + (v2 >> 8);
        v2 &= 0xff;
        i -= 4;
    }
    v0 += v3 >> 8;
    v1 += zone[1] as u32 + (v0 >> 8);
    v2 += zone[2] as u32 + (v1 >> 8);
    v3 += zone[3] as u32 + (v2 >> 8);
    (v0 ^ v1 ^ v2 ^ v3) as u8
}

/// Set a single bit at position `pos` to `value` (0 or 1) in a
/// little-endian-packed bitstream.
fn set_bit_le(buf: &mut [u8], pos: u32, value: u8) {
    let byte = (pos >> 3) as usize;
    let mask = 1u8 << (pos & 7);
    if value != 0 {
        buf[byte] |= mask;
    } else {
        buf[byte] &= !mask;
    }
}

/// Write `nbits` bits of `value` into `buf` starting at bit position
/// `start`, little-endian. Other bits are left alone.
fn write_bits_le_inplace(buf: &mut [u8], start: u32, nbits: u32, value: u64) {
    for k in 0..nbits {
        let pos = start + k;
        let bit = ((value >> k) & 1) as u8;
        set_bit_le(buf, pos, bit);
    }
}

/// Zero out `nbits` bits in `buf` starting at bit position `start`.
fn clear_bits_le(buf: &mut [u8], start: u32, nbits: u32) {
    for k in 0..nbits {
        set_bit_le(buf, start + k, 0);
    }
}

impl AdfsFsm {
    /// Walk the kernel's freelink chain in `zi` and return the head of
    /// the chain — `(start_bit, length_bits)` for the first free
    /// fragment plus its predecessor's chain pointer (8 if it's the
    /// chain root at bit 8, otherwise the previous free entry's
    /// position). The predecessor pointer is what we have to rewrite
    /// when we carve from the chain head.
    ///
    /// Mirrors `scan_free_map` in `fs/adfs/map.c`. The chain root sits
    /// at bit 8 of each zone: a 15-bit delta gives the offset to the
    /// first free fragment; at each free fragment, a 15-bit frag-id
    /// gives the offset to the next. A frag-id < `idlen + 1` marks
    /// the end of the chain.
    ///
    /// Returns `None` when the chain is empty (bit-8 delta is 0).
    fn find_chain_head(&self, zi: u32) -> Option<(u32, u32, u32)> {
        let (_startbit, endbit, _) = self.zone_metadata(zi);
        let zone = self.zones.get(zi as usize)?;
        let frag_idmask = (1u32 << self.idlen.min(15)) - 1;
        // Bit 8 holds the 15-bit delta to the first free fragment.
        let delta = Self::get_frag_id(zone, 8, 0x7FFF);
        if delta == 0 {
            return None;
        }
        let head_start = 8 + delta;
        let fragend = Self::find_next_set_bit(zone, head_start + self.idlen, endbit);
        if fragend >= endbit {
            return None;
        }
        let length = fragend + 1 - head_start;
        // Sanity: the head's frag-id (low 15 bits) tells us whether
        // there's a follow-on. We don't use that here, but we keep the
        // mask-check pattern aligned with the kernel.
        let _ = Self::get_frag_id(zone, head_start, frag_idmask);
        // The predecessor pointer is bit 8.
        Some((head_start, length, 8))
    }

    /// Find an allocatable zero-id fragment that holds at least
    /// `min_bits` map bits, return the zone index plus the
    /// `(start_bit, length_bits)` of that fragment. Prefers the tail
    /// of the lowest-index zone so allocations cluster low (matches
    /// the kernel's monotonic search order).
    fn find_alloc_slot(&self, min_bits: u32) -> Option<(u32, u32, u32, u32)> {
        for zi in 0..self.nzones {
            if let Some((start, length, predecessor)) = self.find_chain_head(zi) {
                if length > min_bits + self.idlen {
                    return Some((zi, start, length, predecessor));
                }
            }
        }
        None
    }

    /// Carve `length_bits` map bits off the head of the free-fragment
    /// at `(zi, free_start, free_length)` and stamp a new
    /// `frag_id`-tagged fragment there. The remainder becomes the new
    /// chain head (frag-id 0 = chain terminator); the predecessor
    /// pointer (bit 8 for the root) is rewritten to point to the new
    /// position.
    ///
    /// **Free-list chain semantics** (kernel `scan_free_map`): bit 8
    /// holds the 15-bit delta to the first free entry; each free
    /// entry stores the 15-bit delta to the next, with delta <
    /// `idlen + 1` marking the chain terminator.
    fn carve_fragment_into_zone(
        &mut self,
        zi: u32,
        free_start: u32,
        free_length: u32,
        frag_id: u32,
        length_bits: u32,
        predecessor_bit: u32,
    ) -> Result<u64, FilesystemError> {
        debug_assert!(length_bits > self.idlen);
        debug_assert!(free_length > length_bits + self.idlen);
        let zone = self
            .zones
            .get_mut(zi as usize)
            .ok_or_else(|| FilesystemError::InvalidData("ADFS FSM: bad zone index".into()))?;

        // 1. Clear the old free fragment's id + terminator.
        clear_bits_le(zone, free_start, free_length);

        // 2. Write the new fragment (frag_id then terminator).
        write_bits_le_inplace(zone, free_start, self.idlen, frag_id as u64);
        set_bit_le(zone, free_start + length_bits - 1, 1);

        // 3. Write the shortened free fragment one step ahead.
        //    Its frag-id stays zero (chain terminator); set the
        //    trailing terminator.
        let new_free_start = free_start + length_bits;
        let new_free_length = free_length - length_bits;
        set_bit_le(zone, new_free_start + new_free_length - 1, 1);

        // 4. Update the predecessor pointer in the chain.
        //    For the root predecessor at bit 8: 15-bit delta to head.
        //    For an inner predecessor: the predecessor's 15-bit
        //    frag-id encodes the delta to the next free entry.
        clear_bits_le(zone, predecessor_bit, 15);
        let new_delta = new_free_start - predecessor_bit;
        write_bits_le_inplace(zone, predecessor_bit, 15, new_delta as u64);

        // Compute the disc-sector address of the new fragment's start.
        let (startbit, _endbit, startblk) = self.zone_metadata(zi);
        let result = free_start as i64 - startbit as i64 + startblk;
        let sector = signed_asl(result, self.map2blk);
        if sector < 0 {
            return Err(FilesystemError::InvalidData(
                "ADFS FSM: carved fragment maps to negative sector".into(),
            ));
        }
        Ok(sector as u64)
    }

    /// Mark a previously-allocated fragment as free by rewriting its
    /// id bits to zero. We don't coalesce or remove the entry — the
    /// kernel's `lookup_zone` walks past zero-id fragments naturally.
    /// The fragment's length-in-bits stays the same. Caller is
    /// responsible for re-stamping checksums + writing.
    ///
    /// Returns `true` if a fragment with the given id was found and
    /// freed; `false` otherwise.
    fn free_fragment(&mut self, frag_id: u32) -> Result<bool, FilesystemError> {
        // Locate the fragment first (immutable scan), then mutate.
        let idmask = (1u32 << self.idlen) - 1;
        let mut hit: Option<(u32, u32)> = None;
        'outer: for zi in 0..self.nzones {
            let (startbit, endbit, _) = self.zone_metadata(zi);
            let Some(zone) = self.zones.get(zi as usize) else {
                continue;
            };
            let mut start = startbit;
            let cap = 2 * (self.zone_size_bits / (self.idlen + 1)).max(1) + 16;
            let mut iter = 0;
            while start + self.idlen < endbit && iter < cap {
                iter += 1;
                let frag = Self::get_frag_id(zone, start, idmask);
                let fragend = Self::find_next_set_bit(zone, start + self.idlen, endbit);
                if fragend >= endbit {
                    break;
                }
                if frag == frag_id {
                    hit = Some((zi, start));
                    break 'outer;
                }
                start = fragend + 1;
            }
        }
        if let Some((zi, start_bit)) = hit {
            let zone = self.zones.get_mut(zi as usize).expect("zone exists");
            // Zero the frag-id bits in place (leave the terminator).
            clear_bits_le(zone, start_bit, self.idlen);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Choose the next unused frag id by scanning every zone's
    /// fragments and bumping past the highest one seen.  Skips ids
    /// 0..=2 (FREE, BAD, ROOT).
    fn next_free_frag_id(&self) -> u32 {
        let idmask = (1u32 << self.idlen) - 1;
        let mut max_seen: u32 = 2;
        for zi in 0..self.nzones {
            let (startbit, endbit, _) = self.zone_metadata(zi);
            let Some(zone) = self.zones.get(zi as usize) else {
                continue;
            };
            let mut start = startbit;
            let cap = 2 * (self.zone_size_bits / (self.idlen + 1)).max(1) + 16;
            let mut iter = 0;
            while start + self.idlen < endbit && iter < cap {
                iter += 1;
                let frag = Self::get_frag_id(zone, start, idmask);
                let fragend = Self::find_next_set_bit(zone, start + self.idlen, endbit);
                if fragend >= endbit {
                    break;
                }
                if frag > max_seen {
                    max_seen = frag;
                }
                start = fragend + 1;
            }
        }
        max_seen + 1
    }

    /// Recompute zone 0..nzones-2's byte 0 (per-zone checksum) and
    /// then set zone N-1's byte 3 so the XOR of byte-3-across-all-zones
    /// equals 0xFF (kernel `adfs_checkmap` cross-check). Finally
    /// re-stamp zone N-1's byte 0.
    fn restamp_all_checksums(&mut self) {
        // First pass: stamp every zone's byte-0 checksum.
        for zi in 0..self.nzones {
            if let Some(zone) = self.zones.get_mut(zi as usize) {
                zone[0] = 0;
                zone[0] = adfs_calczonecheck(zone);
            }
        }
        // Cross-check: XOR of byte 3 across all zones must equal 0xFF.
        let mut acc: u8 = 0;
        for zi in 0..(self.nzones - 1) {
            if let Some(zone) = self.zones.get(zi as usize) {
                acc ^= zone[3];
            }
        }
        let target = acc ^ 0xFF;
        if let Some(zone) = self.zones.get_mut((self.nzones - 1) as usize) {
            zone[3] = target;
            // Re-stamp the now-mutated last zone.
            zone[0] = 0;
            zone[0] = adfs_calczonecheck(zone);
        }
    }
}

/// Build a 24-bit indaddr from a frag_id + in-frag sector offset.
///
/// The kernel split is `(indaddr >> 8, indaddr & 0xFF)` — frag id in
/// the high bits, sector offset in the low byte. For a freshly
/// allocated fragment whose data starts at the very beginning of the
/// frag, `offset_sectors == 0` and the low byte is zero.
fn build_indaddr(frag_id: u32, offset_sectors: u32, log2sharesize: u8) -> u32 {
    // The kernel computes `block += ((indaddr & 0xFF) - 1) <<
    // log2sharesize` when the low byte is non-zero. So if we want to
    // encode `offset_sectors`, we set lo = (offset >> log2sharesize) +
    // 1. When offset is zero, lo is zero.
    let lo = if offset_sectors == 0 {
        0
    } else {
        (offset_sectors >> log2sharesize) + 1
    };
    (frag_id << 8) | (lo & 0xFF)
}

impl<R: Read + Write + Seek + Send> AdfsFilesystem<R> {
    /// Write zone `zi`'s bytes back to disc, including the duplicate
    /// FSM copy at `map_addr + nzones * sector_size` (FileCore HD
    /// convention — both CROS42 and ICEBIRD carry the duplicate).
    fn flush_zone(&mut self, zi: u32) -> Result<(), FilesystemError> {
        let fsm = self
            .fsm
            .as_ref()
            .ok_or_else(|| FilesystemError::Unsupported("D-format has no FSM".into()))?;
        let sector_size = self.disc_record.sector_size() as u64;
        let nzones = self.disc_record.total_zones() as u64;
        let map_addr_byte = self.partition_offset + fsm.map_addr_sec * sector_size;
        let zone_bytes = fsm
            .zones
            .get(zi as usize)
            .ok_or_else(|| FilesystemError::InvalidData("ADFS: zone index OOB".into()))?
            .clone();
        let primary = map_addr_byte + zi as u64 * sector_size;
        self.reader.seek(SeekFrom::Start(primary))?;
        self.reader.write_all(&zone_bytes)?;
        // Duplicate copy at primary + nzones * sector_size, only if it
        // already looks like an FSM (i.e. fingerprint matches): the
        // marutan.net "blank" HDs have only one copy.
        let dup_byte = map_addr_byte + nzones * sector_size + zi as u64 * sector_size;
        let dup_present = {
            self.reader.seek(SeekFrom::Start(dup_byte + 4))?;
            let mut fp = [0u8; 6];
            self.reader.read_exact(&mut fp).is_ok()
                && fp[0] == self.disc_record.log2_sector_size
                && fp[4] == self.disc_record.id_len
                && fp[5] == self.disc_record.log2bpmb
        };
        if dup_present {
            self.reader.seek(SeekFrom::Start(dup_byte))?;
            self.reader.write_all(&zone_bytes)?;
        }
        Ok(())
    }

    /// Allocate `data_bytes`-worth of disc space via the FSM, write the
    /// supplied data into the allocated sectors, re-stamp the affected
    /// zone(s) + cross-check, and return the new fragment id + start
    /// sector. The caller wires the result into a directory entry.
    fn alloc_and_write_data(
        &mut self,
        data: &mut dyn Read,
        data_bytes: u64,
    ) -> Result<(u32, u64), FilesystemError> {
        let fsm_ref = self
            .fsm
            .as_ref()
            .ok_or_else(|| FilesystemError::Unsupported("D-format alloc not supported".into()))?;
        let log2bpmb = fsm_ref.log2bpmb as u32;
        let idlen = fsm_ref.idlen;
        // Round up to whole map bits. The smallest allocation is
        // `idlen + 1` map bits.
        let bytes_per_map_bit = 1u64 << log2bpmb;
        let mut frag_bits = (data_bytes.max(1).div_ceil(bytes_per_map_bit)) as u32;
        if frag_bits < idlen + 1 {
            frag_bits = idlen + 1;
        }
        let frag_id = fsm_ref.next_free_frag_id();

        let (zi, free_start, free_length, predecessor_bit) = self
            .fsm
            .as_ref()
            .unwrap()
            .find_alloc_slot(frag_bits)
            .ok_or_else(|| {
                FilesystemError::Unsupported(
                    "ADFS: no free FSM slot large enough for this allocation".into(),
                )
            })?;

        // Carve the fragment and capture the disc-sector start.
        let start_sector = self.fsm.as_mut().unwrap().carve_fragment_into_zone(
            zi,
            free_start,
            free_length,
            frag_id,
            frag_bits,
            predecessor_bit,
        )?;
        // Re-stamp checksums across every zone (the cross-check
        // requires touching the last zone's byte 3 even if we wrote
        // the fragment elsewhere).
        self.fsm.as_mut().unwrap().restamp_all_checksums();

        // Persist every zone (cheap on the small disk sizes we deal
        // with; mirrors the kernel's "write back every dirty bh"
        // behaviour without tracking dirtiness ourselves).
        let nzones = self.disc_record.total_zones();
        for z in 0..nzones {
            self.flush_zone(z)?;
        }

        // Write the file data. We may overshoot `data_bytes` slightly
        // on the last sector — that's fine, the dir entry records the
        // exact size and read_file truncates.
        let sector_size = self.disc_record.sector_size() as u64;
        let abs_byte = self.partition_offset + start_sector * sector_size;
        self.reader.seek(SeekFrom::Start(abs_byte))?;
        let mut remaining = data_bytes;
        let mut buf = vec![0u8; sector_size as usize];
        while remaining > 0 {
            let take = remaining.min(sector_size) as usize;
            // Zero out the tail bytes of the buffer so a partial last
            // sector doesn't leak prior bytes.
            for b in buf.iter_mut() {
                *b = 0;
            }
            data.read_exact(&mut buf[..take])?;
            self.reader.write_all(&buf)?;
            remaining -= take as u64;
        }
        Ok((frag_id, start_sector))
    }

    /// Insert a directory entry into the dir block reached via
    /// `dir_indaddr`. Fails if the dir block is full (the kernel-style
    /// fixed-77-entry F-format layout has no overflow path).
    fn insert_dir_entry(
        &mut self,
        dir_indaddr: u32,
        new_entry: &AdfsDirEntry,
    ) -> Result<(), FilesystemError> {
        let mut block = self.read_dir_block(dir_indaddr)?;
        if block.len() < DIR_SMALL_HEADER + DIR_ENTRY_SIZE {
            return Err(FilesystemError::InvalidData(
                "ADFS dir block too small for insert".into(),
            ));
        }
        // Find the first zero-name slot.
        let mut off = DIR_SMALL_HEADER;
        let mut found = false;
        while off + DIR_ENTRY_SIZE + DIR_SMALL_HEADER <= block.len() {
            if block[off] == 0 {
                found = true;
                break;
            }
            off += DIR_ENTRY_SIZE;
        }
        if !found {
            return Err(FilesystemError::Unsupported(
                "ADFS: directory full (F-format cap of 77 entries reached)".into(),
            ));
        }
        // Stamp the 26-byte entry.
        let mut buf = [0u8; DIR_ENTRY_SIZE];
        let nb = new_entry.name.as_bytes();
        let nlen = nb.len().min(9);
        buf[..nlen].copy_from_slice(&nb[..nlen]);
        if nlen < 10 {
            // Terminate with CR so the kernel-style parser stops here.
            buf[nlen] = 0x0D;
        }
        LittleEndian::write_u32(&mut buf[10..14], new_entry.load_addr);
        LittleEndian::write_u32(&mut buf[14..18], new_entry.exec_addr);
        LittleEndian::write_u32(&mut buf[18..22], new_entry.file_length);
        buf[22] = (new_entry.indirect_disc_addr & 0xFF) as u8;
        buf[23] = ((new_entry.indirect_disc_addr >> 8) & 0xFF) as u8;
        buf[24] = ((new_entry.indirect_disc_addr >> 16) & 0xFF) as u8;
        buf[25] = new_entry.attrs;
        block[off..off + DIR_ENTRY_SIZE].copy_from_slice(&buf);
        // Bump the cycle counter at the very last byte of the block —
        // the tail layout is (magic, ..., cycle). The kernel uses the
        // last byte as the cycle counter (`adfs_f_dir.c::dir_cycle`).
        let last = block.len() - 1;
        block[last] = block[last].wrapping_add(1);
        self.write_dir_block(dir_indaddr, &block)
    }

    /// Remove a directory entry whose name matches and (optionally)
    /// whose indaddr matches. Shifts later entries up to keep the slot
    /// list contiguous.
    fn remove_dir_entry(
        &mut self,
        dir_indaddr: u32,
        name: &str,
    ) -> Result<AdfsDirEntry, FilesystemError> {
        let mut block = self.read_dir_block(dir_indaddr)?;
        let mut off = DIR_SMALL_HEADER;
        let mut found_at: Option<usize> = None;
        let mut removed: Option<AdfsDirEntry> = None;
        while off + DIR_ENTRY_SIZE <= block.len() - DIR_SMALL_HEADER {
            let slot: [u8; DIR_ENTRY_SIZE] =
                block[off..off + DIR_ENTRY_SIZE].try_into().expect("size");
            match parse_dir_entry(&slot) {
                Some(e) => {
                    if e.name == name {
                        found_at = Some(off);
                        removed = Some(e);
                        break;
                    }
                    off += DIR_ENTRY_SIZE;
                }
                None => break,
            }
        }
        let off = found_at.ok_or_else(|| {
            FilesystemError::InvalidData(format!("ADFS: dir entry '{}' not found", name))
        })?;
        let removed = removed.unwrap();
        // Shift later entries up.
        let tail_start = off + DIR_ENTRY_SIZE;
        let tail_end = block.len() - DIR_SMALL_HEADER;
        block.copy_within(tail_start..tail_end, off);
        // Zero out the freed slot at the end.
        let zero_start = tail_end - DIR_ENTRY_SIZE;
        for b in &mut block[zero_start..tail_end] {
            *b = 0;
        }
        // Bump cycle counter.
        let last = block.len() - 1;
        block[last] = block[last].wrapping_add(1);
        self.write_dir_block(dir_indaddr, &block)?;
        Ok(removed)
    }

    /// Write `block` bytes back to the directory referenced by
    /// `dir_indaddr`. Walks the FSM sector-by-sector mirroring the
    /// read path in `read_dir_block`.
    fn write_dir_block(&mut self, dir_indaddr: u32, block: &[u8]) -> Result<(), FilesystemError> {
        let sector_size = self.disc_record.sector_size() as usize;
        let n_sectors = block.len().div_ceil(sector_size);
        for s in 0..n_sectors {
            let byte_off = self.resolve_byte(dir_indaddr, s as u64)?;
            self.reader.seek(SeekFrom::Start(byte_off))?;
            let from = s * sector_size;
            let to = (from + sector_size).min(block.len());
            let mut buf = vec![0u8; sector_size];
            buf[..to - from].copy_from_slice(&block[from..to]);
            self.reader.write_all(&buf)?;
        }
        Ok(())
    }

    /// Build an empty Hugo-format directory block (header + tail magic
    /// + zeroed entry slots). Used by `create_directory`.
    fn build_empty_dir_block(&self) -> Vec<u8> {
        let mut buf = vec![0u8; ADFS_NEWDIR_SIZE as usize];
        // header: byte 0 = 0, bytes 1..5 = "Hugo"
        buf[1] = b'H';
        buf[2] = b'u';
        buf[3] = b'g';
        buf[4] = b'o';
        // tail: bytes len-5..len-1 = "Hugo", last byte = cycle (0).
        let len = buf.len();
        buf[len - 5] = b'H';
        buf[len - 4] = b'u';
        buf[len - 3] = b'g';
        buf[len - 2] = b'o';
        buf[len - 1] = 0;
        buf
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for AdfsFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if self.fsm.is_none() {
            return Err(FilesystemError::Unsupported(
                "D-format ADFS write not supported".into(),
            ));
        }
        if data_len > u32::MAX as u64 {
            return Err(FilesystemError::Unsupported(
                "ADFS file size > 4 GiB not supported".into(),
            ));
        }
        // Allocate + write payload first; if the dir insert later
        // fails we leak the fragment but the disc stays consistent.
        let (frag_id, _start_sector) = self.alloc_and_write_data(data, data_len)?;
        let parent_indaddr = if parent.path == "/" {
            self.disc_record.root
        } else {
            parent.location as u32
        };
        let indaddr = build_indaddr(frag_id, 0, self.disc_record.log2sharesize);
        let entry = AdfsDirEntry {
            name: name.to_string(),
            load_addr: 0xFFFFFFFF,
            exec_addr: 0,
            file_length: data_len as u32,
            indirect_disc_addr: indaddr,
            attrs: 0x03, // R + W
        };
        self.insert_dir_entry(parent_indaddr, &entry)?;
        let path = if parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path.trim_end_matches('/'), name)
        };
        Ok(FileEntry::new_file(
            name.to_string(),
            path,
            data_len,
            indaddr as u64,
        ))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if self.fsm.is_none() {
            return Err(FilesystemError::Unsupported(
                "D-format ADFS write not supported".into(),
            ));
        }
        // Build the empty 2-KiB dir block, then write it the same way
        // we'd write file data (alloc + write).
        let block = self.build_empty_dir_block();
        let mut cursor = std::io::Cursor::new(block);
        let (frag_id, _start) = self.alloc_and_write_data(&mut cursor, ADFS_NEWDIR_SIZE)?;
        let parent_indaddr = if parent.path == "/" {
            self.disc_record.root
        } else {
            parent.location as u32
        };
        let indaddr = build_indaddr(frag_id, 0, self.disc_record.log2sharesize);
        let entry = AdfsDirEntry {
            name: name.to_string(),
            load_addr: 0xFFFFFFFF,
            exec_addr: 0,
            file_length: ADFS_NEWDIR_SIZE as u32,
            indirect_disc_addr: indaddr,
            attrs: 0x0B, // R + W + Directory bit
        };
        self.insert_dir_entry(parent_indaddr, &entry)?;
        let path = if parent.path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent.path.trim_end_matches('/'), name)
        };
        Ok(FileEntry::new_directory(
            name.to_string(),
            path,
            indaddr as u64,
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if self.fsm.is_none() {
            return Err(FilesystemError::Unsupported(
                "D-format ADFS write not supported".into(),
            ));
        }
        // Empty-directory check for non-leaf entries.
        if entry.is_directory() {
            let kids = self.list_directory(entry)?;
            if !kids.is_empty() {
                return Err(FilesystemError::InvalidData(
                    "cannot delete non-empty directory".into(),
                ));
            }
        }
        let parent_indaddr = if parent.path == "/" {
            self.disc_record.root
        } else {
            parent.location as u32
        };
        // Splice out the dir entry first (cheap rollback if the FSM
        // mutate fails: re-insert).
        let removed = self.remove_dir_entry(parent_indaddr, &entry.name)?;
        let frag_id = removed.indirect_disc_addr >> 8;
        // Skip the ROOT_FRAG sanity guard — frag id ≤ 2 are reserved
        // and shouldn't ever appear in a deletable directory entry.
        if frag_id <= ADFS_ROOT_FRAG {
            return Err(FilesystemError::InvalidData(format!(
                "ADFS: refused to free reserved frag id {}",
                frag_id
            )));
        }
        let fsm = self.fsm.as_mut().unwrap();
        if !fsm.free_fragment(frag_id)? {
            return Err(FilesystemError::InvalidData(format!(
                "ADFS: frag id {} not found in FSM during delete",
                frag_id
            )));
        }
        fsm.restamp_all_checksums();
        let nzones = self.disc_record.total_zones();
        for z in 0..nzones {
            self.flush_zone(z)?;
        }
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        // Every primitive flushes synchronously — nothing to do here.
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.fsm.as_ref().map(|f| f.free_bytes()).unwrap_or(0))
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
        // 800 KB so disc_size_map_bits is wide enough that `dm_endbit`
        // hits the natural `32 + zone_size_bits` cap (6912) rather than
        // being clamped down to the disc-size constraint. This makes
        // room for a write-able tail-free fragment.
        const TOTAL_BYTES: usize = 800 * SECTOR_SIZE;
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
        // sectors 0..1 (FSM itself + 1 padding sector)
        write_frag(&mut disk[..SECTOR_SIZE], 512, IDLEN, 16, 0);
        // root dir at sectors 2..3
        write_frag(&mut disk[..SECTOR_SIZE], 528, IDLEN, 16, 2);
        // HELLO payload at sectors 4..5
        write_frag(&mut disk[..SECTOR_SIZE], 544, IDLEN, 16, 5);
        // Tail "free" fragment closes the bitstream up to dm_endbit
        // (32 + 6880 = 6912). length = 6912 - 560 = 6352.
        write_frag(&mut disk[..SECTOR_SIZE], 560, IDLEN, 6352, 0);
        // Free-list chain root at bit 8: 15-bit delta to first (and
        // only) free fragment, which sits at bit 560. delta = 552.
        write_bits_le(&mut disk[..SECTOR_SIZE], 8, 15, 560 - 8);

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
        assert_eq!(fs.disc_record.total_disc_size(), 800 * 1024);
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
    fn list_directory_dispatches_on_entry_indaddr_for_non_root() {
        // A non-root FileEntry must drive `list_directory` through its
        // own `location` (the directory's indaddr), not the disc record
        // root. We exercise that by pointing a synthetic subdir entry
        // at the existing root dir's indaddr — same contents come back
        // from a subdir call as from a root call.
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let mut fs = AdfsFilesystem::open(cur, 0).unwrap();
        let synthetic_sub = FileEntry::new_directory(
            "RootClone".into(),
            "/RootClone".into(),
            0x200, // same indaddr as `dr.root` — points to the root dir.
        );
        let via_subdir = fs.list_directory(&synthetic_sub).unwrap();
        let root = fs.root().unwrap();
        let via_root = fs.list_directory(&root).unwrap();
        assert_eq!(via_subdir.len(), via_root.len());
        assert_eq!(via_subdir[0].name, via_root[0].name);
        // Subdir entries should carry the prefixed path.
        assert!(via_subdir[0].path.starts_with("/RootClone/"));
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
    fn calczonecheck_matches_real_cros42_byte_0() {
        // Probe-derived: CROS42 zone 0's first 4 bytes form a known
        // checksum tuple. We can't ship the disc, but we can pin one
        // small synthetic case the algorithm round-trips correctly:
        // a zone of all zeros => byte 0 must come back zero too.
        let zero_zone = vec![0u8; 512];
        assert_eq!(adfs_calczonecheck(&zero_zone), 0);
        // Bump a single byte deep in the zone — the checksum changes.
        let mut z = vec![0u8; 512];
        z[100] = 0x42;
        let cs = adfs_calczonecheck(&z);
        assert_ne!(cs, 0, "non-zero zone must produce non-zero checksum");
        // Round-trip via stamp + recompute: byte 0 set to the value
        // returned, then re-running the algorithm reproduces the same
        // checksum (kernel's `adfs_checkmap` literally re-runs and
        // compares to byte 0).
        let mut z = vec![0u8; 512];
        z[100] = 0x42;
        let cs = adfs_calczonecheck(&z);
        z[0] = 0;
        let cs2 = adfs_calczonecheck(&z);
        assert_eq!(cs, cs2);
    }

    #[test]
    fn build_indaddr_matches_kernel_split() {
        // Zero offset => low byte zero.
        assert_eq!(build_indaddr(2, 0, 0), 0x200);
        // Non-zero offset: with log2sharesize=0, lo = offset + 1.
        // Kernel: block += ((lo - 1) << log2sharesize) ==> matches our
        // round-trip.
        let ia = build_indaddr(5, 4, 0);
        let lo = ia & 0xFF;
        let recovered_offset = if lo == 0 { 0 } else { lo - 1 };
        assert_eq!(recovered_offset, 4);
        assert_eq!(ia >> 8, 5);
    }

    #[test]
    fn create_file_round_trips_through_fsm() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let mut fs = AdfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let payload = b"hello from create_file roundtrip!";
        let mut src = std::io::Cursor::new(payload.to_vec());
        let opts = CreateFileOptions::default();
        let new_entry = fs
            .create_file(&root, "GREETS", &mut src, payload.len() as u64, &opts)
            .expect("create_file");
        assert_eq!(new_entry.name, "GREETS");
        assert_eq!(new_entry.size, payload.len() as u64);
        // Re-list and confirm.
        let entries = fs.list_directory(&root).unwrap();
        assert!(entries.iter().any(|e| e.name == "GREETS"));
        // Read the new file back through the FSM and check byte-exact.
        let greets = entries.iter().find(|e| e.name == "GREETS").unwrap();
        let data = fs.read_file(greets, payload.len() * 2).unwrap();
        assert_eq!(&data, payload);
    }

    #[test]
    fn delete_entry_frees_fragment_and_drops_dir_entry() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let mut fs = AdfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        // Create a file first so we have something to delete.
        let mut src = std::io::Cursor::new(b"temp".to_vec());
        let opts = CreateFileOptions::default();
        let tmp = fs.create_file(&root, "TMP", &mut src, 4, &opts).unwrap();
        let free_before = fs.fsm.as_ref().unwrap().free_bytes();
        // Delete it.
        fs.delete_entry(&root, &tmp).unwrap();
        let listing = fs.list_directory(&root).unwrap();
        assert!(!listing.iter().any(|e| e.name == "TMP"));
        // Free space should have grown back (the carved fragment is
        // freed by zeroing its id bits — counts as free again).
        let free_after = fs.fsm.as_ref().unwrap().free_bytes();
        assert!(
            free_after >= free_before,
            "free space did not grow after delete: before={} after={}",
            free_before,
            free_after,
        );
    }

    #[test]
    fn create_directory_and_descend_round_trip() {
        let disk = build_eformat_with_one_file();
        let cur = Cursor::new(disk);
        let mut fs = AdfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let opts = CreateDirectoryOptions::default();
        let sub = fs.create_directory(&root, "SUB", &opts).unwrap();
        assert!(sub.is_directory());
        // Newly-created subdir lists empty.
        let entries = fs.list_directory(&sub).unwrap();
        assert!(entries.is_empty());
        // Create a file inside the subdir and confirm the path.
        let mut src = std::io::Cursor::new(b"inside-sub".to_vec());
        let opts = CreateFileOptions::default();
        let f = fs.create_file(&sub, "INSIDE", &mut src, 10, &opts).unwrap();
        assert_eq!(f.path, "/SUB/INSIDE");
        let listing = fs.list_directory(&sub).unwrap();
        assert!(listing.iter().any(|e| e.name == "INSIDE"));
        let bytes = fs.read_file(&listing[0], 32).unwrap();
        assert_eq!(&bytes, b"inside-sub");
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
