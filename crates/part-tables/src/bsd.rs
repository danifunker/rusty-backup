//! BSD `struct disklabel` (NetBSD / OpenBSD / FreeBSD / 4.4BSD), read-only.
//!
//! Used on its own, without an MBR, by NetBSD on HP 9000/300 and on other
//! "dedicated" disks. The label is 148 bytes plus 16 bytes per partition, in the
//! machine's byte order, and found by its magic `0x82564557` (`d_magic` at 0 and
//! again as `d_magic2` at 132). Its 16-bit XOR checksum (`d_checksum` at 136)
//! covers the header and every partition entry.
//!
//! Where ports put it (byte offsets from the start of the disk): hp300 at 1024,
//! behind a LIF boot volume at sector 0; i386 / amd64 dedicated disks at 512;
//! vax at 64; sparc at 128. All are probed, in both byte orders.
//!
//! Partition entry (16 bytes): `p_size`, `p_offset` (sectors of `d_secsize`),
//! `p_fsize`, `p_fstype`, `p_frag`, `p_cpg`. The raw whole-disk slot (`c` or `d`,
//! covering sector 0 to `d_secperunit`) is an alias, not a volume, and is
//! excluded from the browsable list the same way the Sun label's slot 2 is.
//!
//! Read + browse + whole-disk backup; [`BsdLabelSpec`] writes a fresh label (FreeBSD/Alpha's
//! little-endian one at byte 64 of sector 0, see `docs/bsd_alpha_disks.md`), with the
//! Alpha SRM boot-sector checksum and boot-block descriptor. Editing an existing label and
//! resizing on restore are not implemented yet (see `docs/stubs.md`).

#[cfg(feature = "rust173-polyfill")]
use disk_core::rust173::IntIsMultipleOf as _;
use std::io::{Read, Seek, SeekFrom};

use serde::{Deserialize, Serialize};

/// `DISKMAGIC`, at `d_magic` and `d_magic2`.
pub const BSD_DISKMAGIC: u32 = 0x8256_4557;
/// Byte offsets from the start of the disk where a port may keep the label.
const LABEL_OFFSETS: [u64; 4] = [1024, 512, 64, 128];
/// Header bytes before the partition array.
const HEADER_LEN: usize = 148;
/// Bytes per partition entry.
const PART_LEN: usize = 16;
/// `MAXPARTITIONS` on the ports that use the largest table.
const MAX_PARTITIONS: usize = 22;

/// One slot of a BSD disk label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BsdPartition {
    /// Slot letter, `a` upward.
    pub letter: char,
    /// Start, in `d_secsize` sectors.
    pub offset: u32,
    /// Length, in `d_secsize` sectors.
    pub size: u32,
    /// `p_fstype`: 7 is 4.2BSD FFS, 1 swap, 0 unused.
    pub fstype: u8,
}

/// A parsed BSD disk label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BsdDiskLabel {
    pub big_endian: bool,
    /// Byte offset of the label from the start of the disk.
    pub label_offset: u64,
    pub sector_size: u32,
    pub sectors_per_unit: u32,
    /// `d_typename`, e.g. the drive model (`C2203A`).
    pub type_name: String,
    pub partitions: Vec<BsdPartition>,
    /// The disk starts with an HP LIF boot volume (hp300 / hppa).
    pub has_lif: bool,
}

impl BsdPartition {
    /// The `p_fstype` name from NetBSD's `fstypenames`.
    pub fn fstype_name(&self) -> &'static str {
        match self.fstype {
            0 => "unused",
            1 => "swap",
            2 => "Version 6",
            3 => "Version 7",
            4 => "System V",
            5 => "4.1BSD",
            6 => "Eighth Edition",
            7 => "4.2BSD",
            8 => "MSDOS",
            9 => "4.4LFS",
            11 => "HPFS",
            12 => "ISO9660",
            13 => "boot",
            14 => "ADOS",
            15 => "HFS",
            17 => "ext2fs",
            18 => "NTFS",
            19 => "RAID",
            21 => "jfs",
            22 => "Apple UFS",
            _ => "unknown",
        }
    }
}

/// The `p_fstype` a name stands for: `disklabel`'s own names, a number, or `ufs` / `ffs`.
pub fn fstype_from_text(text: &str) -> Option<u8> {
    let t = text.trim();
    if let Ok(n) = t.parse::<u8>() {
        return Some(n);
    }
    if t.eq_ignore_ascii_case("ufs") || t.eq_ignore_ascii_case("ffs") {
        return Some(FS_BSDFFS);
    }
    (0..=u8::MAX).find(|&n| {
        let p = BsdPartition {
            letter: 'a',
            offset: 0,
            size: 0,
            fstype: n,
        };
        let name = p.fstype_name();
        name != "unknown" && name.eq_ignore_ascii_case(t)
    })
}

impl BsdDiskLabel {
    /// Find and parse a BSD label at any of the known offsets, either byte order.
    pub fn detect<R: Read + Seek>(reader: &mut R) -> Option<Self> {
        let mut head = vec![0u8; 2048];
        reader.seek(SeekFrom::Start(0)).ok()?;
        reader.read_exact(&mut head).ok()?;
        let has_lif = crate::lif::looks_like_lif(&head);
        LABEL_OFFSETS
            .iter()
            .find_map(|&off| Self::parse_at(&head, off as usize, has_lif))
    }

    /// Parse the label at `off` within `head` (which starts at disk byte 0).
    pub fn parse_at(head: &[u8], off: usize, has_lif: bool) -> Option<Self> {
        let buf = head.get(off..)?;
        if buf.len() < HEADER_LEN {
            return None;
        }
        let big_endian = if buf[0..4] == BSD_DISKMAGIC.to_be_bytes() {
            true
        } else if buf[0..4] == BSD_DISKMAGIC.to_le_bytes() {
            false
        } else {
            return None;
        };
        let u16_at = |o: usize| {
            let b = [buf[o], buf[o + 1]];
            if big_endian {
                u16::from_be_bytes(b)
            } else {
                u16::from_le_bytes(b)
            }
        };
        let u32_at = |o: usize| {
            let b = [buf[o], buf[o + 1], buf[o + 2], buf[o + 3]];
            if big_endian {
                u32::from_be_bytes(b)
            } else {
                u32::from_le_bytes(b)
            }
        };
        if u32_at(132) != BSD_DISKMAGIC {
            return None;
        }
        let npart = u16_at(138) as usize;
        let end = HEADER_LEN + npart * PART_LEN;
        if npart == 0 || npart > MAX_PARTITIONS || buf.len() < end {
            return None;
        }
        if (0..end).step_by(2).fold(0u16, |x, o| x ^ u16_at(o)) != 0 {
            return None;
        }
        let sector_size = u32_at(40);
        if sector_size == 0 || !sector_size.is_multiple_of(256) {
            return None;
        }
        let partitions = (0..npart)
            .map(|i| {
                let p = HEADER_LEN + i * PART_LEN;
                BsdPartition {
                    letter: (b'a' + i as u8) as char,
                    size: u32_at(p),
                    offset: u32_at(p + 4),
                    fstype: buf[p + 12],
                }
            })
            .collect();
        let type_name = String::from_utf8_lossy(&buf[8..24])
            .trim_end_matches('\0')
            .trim()
            .to_string();
        Some(Self {
            big_endian,
            label_offset: off as u64,
            sector_size,
            sectors_per_unit: u32_at(60),
            type_name,
            partitions,
            has_lif,
        })
    }

    /// Slots that hold a volume: used, and not the raw whole-disk alias.
    pub fn browsable_partitions(&self) -> impl Iterator<Item = (usize, &BsdPartition)> {
        self.partitions.iter().enumerate().filter(move |(_, p)| {
            let whole_disk = p.offset == 0 && p.size >= self.sectors_per_unit;
            p.size != 0 && p.fstype != 0 && !whole_disk
        })
    }

    /// Byte offset of a slot from the start of the disk.
    pub fn start_byte(&self, p: &BsdPartition) -> u64 {
        p.offset as u64 * self.sector_size as u64
    }

    /// Byte length of a slot.
    pub fn size_bytes(&self, p: &BsdPartition) -> u64 {
        p.size as u64 * self.sector_size as u64
    }

    /// Bytes ahead of the first slot (LIF volume, label, boot code), copied verbatim by backup.
    pub fn head_bytes(&self) -> u64 {
        self.browsable_partitions()
            .map(|(_, p)| self.start_byte(p))
            .min()
            .unwrap_or(self.label_offset + HEADER_LEN as u64)
    }
}

/// `p_fstype` of a 4.2BSD fast filesystem.
pub const FS_BSDFFS: u8 = 7;
/// `p_fstype` of a swap partition.
pub const FS_SWAP: u8 = 1;
/// `d_type` of a SCSI disk (`DTYPE_SCSI`).
const DTYPE_SCSI: u16 = 4;
/// Where FreeBSD/Alpha keeps its label: `LABELSECTOR 0`, `LABELOFFSET 64`.
pub const ALPHA_LABEL_OFFSET: usize = 64;
/// The Alpha SRM boot-block descriptor: block count, start LBA, flags (one quadword each).
const SRM_DESCRIPTOR: usize = 480;
/// The Alpha SRM boot-sector checksum: the sum of the 63 quadwords ahead of it.
const SRM_CHECKSUM: usize = 504;

/// One slot of a label to write, its sizes in sectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BsdSlotSpec {
    pub size: u32,
    pub offset: u32,
    pub fstype: u8,
    /// `p_fsize`, `p_frag` and `p_cpg`; zero where the slot holds no FFS.
    pub fsize: u32,
    pub frag: u8,
    pub cpg: u16,
}

/// A little-endian 4.4BSD label to write, as FreeBSD/Alpha's `disklabel` lays it out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BsdLabelSpec {
    pub sectors_per_unit: u32,
    pub sectors_per_track: u32,
    pub tracks_per_cylinder: u32,
    /// Slots `a` upward; `c` is the whole-disk alias the caller fills in.
    pub slots: [BsdSlotSpec; 8],
}

impl BsdLabelSpec {
    /// The label's bytes, `d_checksum` set so the label XORs to zero.
    pub fn to_bytes(&self) -> Vec<u8> {
        let npart = self.slots.len();
        let mut b = vec![0u8; HEADER_LEN + npart * PART_LEN];
        let put16 =
            |b: &mut Vec<u8>, o: usize, v: u16| b[o..o + 2].copy_from_slice(&v.to_le_bytes());
        let put32 =
            |b: &mut Vec<u8>, o: usize, v: u32| b[o..o + 4].copy_from_slice(&v.to_le_bytes());
        let spc = self.sectors_per_track * self.tracks_per_cylinder;
        put32(&mut b, 0, BSD_DISKMAGIC);
        put16(&mut b, 4, DTYPE_SCSI);
        b[8..11].copy_from_slice(b"da0");
        put32(&mut b, 40, 512);
        put32(&mut b, 44, self.sectors_per_track);
        put32(&mut b, 48, self.tracks_per_cylinder);
        put32(&mut b, 52, self.sectors_per_unit / spc.max(1));
        put32(&mut b, 56, spc);
        put32(&mut b, 60, self.sectors_per_unit);
        put16(&mut b, 72, 3600);
        put16(&mut b, 74, 1);
        put32(&mut b, 132, BSD_DISKMAGIC);
        put16(&mut b, 138, npart as u16);
        put32(&mut b, 140, 8192);
        put32(&mut b, 144, 8192);
        for (i, s) in self.slots.iter().enumerate() {
            let p = HEADER_LEN + i * PART_LEN;
            put32(&mut b, p, s.size);
            put32(&mut b, p + 4, s.offset);
            put32(&mut b, p + 8, s.fsize);
            b[p + 12] = s.fstype;
            b[p + 13] = s.frag;
            put16(&mut b, p + 14, s.cpg);
        }
        let ck = (0..b.len())
            .step_by(2)
            .fold(0u16, |x, o| x ^ u16::from_le_bytes([b[o], b[o + 1]]));
        put16(&mut b, 136, ck);
        b
    }

    /// Sector 0 of a FreeBSD/Alpha disk: zeros, this label at byte 64, the SRM checksum.
    pub fn alpha_sector0(&self) -> [u8; 512] {
        let mut sector = [0u8; 512];
        let label = self.to_bytes();
        sector[ALPHA_LABEL_OFFSET..ALPHA_LABEL_OFFSET + label.len()].copy_from_slice(&label);
        set_alpha_srm_checksum(&mut sector);
        sector
    }
}

/// The Alpha SRM console's boot-sector checksum: the sum of the first 63 quadwords.
pub fn alpha_srm_checksum(sector: &[u8; 512]) -> u64 {
    (0..SRM_CHECKSUM)
        .step_by(8)
        .map(|o| {
            let mut q = [0u8; 8];
            q.copy_from_slice(&sector[o..o + 8]);
            u64::from_le_bytes(q)
        })
        .fold(0u64, u64::wrapping_add)
}

/// Store the SRM checksum at byte 504; anything that changes sector 0 calls this after.
pub fn set_alpha_srm_checksum(sector: &mut [u8; 512]) {
    let sum = alpha_srm_checksum(sector);
    sector[SRM_CHECKSUM..SRM_CHECKSUM + 8].copy_from_slice(&sum.to_le_bytes());
}

/// Point the SRM console at `blocks` sectors of boot code from `start`, as `disklabel -B` does.
pub fn set_alpha_boot_descriptor(sector: &mut [u8; 512], blocks: u64, start: u64) {
    for (i, v) in [blocks, start, 0].iter().enumerate() {
        let o = SRM_DESCRIPTOR + i * 8;
        sector[o..o + 8].copy_from_slice(&v.to_le_bytes());
    }
    set_alpha_srm_checksum(sector);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// The label FreeBSD 4.6's sysinstall wrote on a 2 GiB disk (a/b/e/f/g), as read off it.
    fn freebsd46_spec() -> BsdLabelSpec {
        let ffs = |size, offset, cpg| BsdSlotSpec {
            size,
            offset,
            fstype: FS_BSDFFS,
            fsize: 2048,
            frag: 8,
            cpg,
        };
        let mut slots = [BsdSlotSpec::default(); 8];
        slots[0] = ffs(262_144, 0, 94);
        slots[1] = BsdSlotSpec {
            size: 516_288,
            offset: 262_144,
            fstype: FS_SWAP,
            ..Default::default()
        };
        slots[2] = BsdSlotSpec {
            size: 4_194_304,
            ..Default::default()
        };
        slots[4] = ffs(524_288, 778_432, 94);
        slots[5] = ffs(524_288, 1_302_720, 94);
        slots[6] = ffs(2_367_296, 1_827_008, 90);
        BsdLabelSpec {
            sectors_per_unit: 4_194_304,
            sectors_per_track: 63,
            tracks_per_cylinder: 255,
            slots,
        }
    }

    #[test]
    fn alpha_writes_the_46_reference() {
        let sector = freebsd46_spec().alpha_sector0();
        assert_eq!(&sector[..64], &[0u8; 64]);
        // Values read off the reference disk: 261 cylinders, checksum 0x4d28.
        assert_eq!(&sector[64 + 52..64 + 56], &261u32.to_le_bytes());
        assert_eq!(&sector[64 + 136..64 + 138], &0x4d28u16.to_le_bytes());
        let stored = u64::from_le_bytes(sector[504..512].try_into().unwrap());
        assert_eq!(stored, alpha_srm_checksum(&sector));
        let mut disk = sector.to_vec();
        disk.resize(4096, 0);
        let label = BsdDiskLabel::detect(&mut Cursor::new(disk)).expect("label");
        assert!(!label.big_endian);
        assert_eq!(label.label_offset, 64);
        let found: Vec<char> = label
            .browsable_partitions()
            .map(|(_, p)| p.letter)
            .collect();
        assert_eq!(found, ['a', 'b', 'e', 'f', 'g']);
    }

    #[test]
    fn the_boot_descriptor_keeps_the_checksum() {
        let mut sector = freebsd46_spec().alpha_sector0();
        set_alpha_boot_descriptor(&mut sector, 15, 1);
        assert_eq!(&sector[480..488], &15u64.to_le_bytes());
        assert_eq!(&sector[488..496], &1u64.to_le_bytes());
        let stored = u64::from_le_bytes(sector[504..512].try_into().unwrap());
        assert_eq!(stored, alpha_srm_checksum(&sector));
    }

    /// hp300-shaped disk: LIF header at 0, big-endian label at 1024, slots a (FFS), b (swap), c (raw).
    fn hp300_disk() -> Vec<u8> {
        let mut d = vec![0u8; 4096];
        d[0] = 0x80;
        d[8..12].copy_from_slice(&2u32.to_be_bytes());
        d[16..20].copy_from_slice(&1u32.to_be_bytes());
        let l = 1024;
        let put32 = |d: &mut Vec<u8>, o: usize, v: u32| {
            d[l + o..l + o + 4].copy_from_slice(&v.to_be_bytes())
        };
        put32(&mut d, 0, BSD_DISKMAGIC);
        put32(&mut d, 132, BSD_DISKMAGIC);
        d[l + 8..l + 14].copy_from_slice(b"C2203A");
        put32(&mut d, 40, 512);
        put32(&mut d, 60, 1_309_896);
        d[l + 138..l + 140].copy_from_slice(&3u16.to_be_bytes());
        for (i, (size, off, ty)) in [
            (1_218_592u32, 904u32, 7u8),
            (90_400, 1_219_496, 1),
            (1_309_896, 0, 13),
        ]
        .iter()
        .enumerate()
        {
            let p = 148 + i * 16;
            put32(&mut d, p, *size);
            put32(&mut d, p + 4, *off);
            d[l + p + 12] = *ty;
        }
        let ck = (0..148 + 3 * 16).step_by(2).fold(0u16, |x, o| {
            x ^ u16::from_be_bytes([d[l + o], d[l + o + 1]])
        });
        d[l + 136..l + 138].copy_from_slice(&ck.to_be_bytes());
        d
    }

    #[test]
    fn detects_hp300_label_behind_lif() {
        let label = BsdDiskLabel::detect(&mut Cursor::new(hp300_disk())).expect("label");
        assert!(label.big_endian && label.has_lif);
        assert_eq!(
            (label.label_offset, label.type_name.as_str()),
            (1024, "C2203A")
        );
        let slots: Vec<char> = label
            .browsable_partitions()
            .map(|(_, p)| p.letter)
            .collect();
        assert_eq!(slots, vec!['a', 'b'], "raw whole-disk c is an alias");
        let a = &label.partitions[0];
        assert_eq!(
            (label.start_byte(a), a.fstype_name()),
            (904 * 512, "4.2BSD")
        );
        assert_eq!(label.head_bytes(), 904 * 512);
    }

    #[test]
    fn bad_checksum_or_magic_is_rejected() {
        let mut d = hp300_disk();
        d[1024 + 148] ^= 1;
        assert!(BsdDiskLabel::detect(&mut Cursor::new(d)).is_none());
        assert!(BsdDiskLabel::detect(&mut Cursor::new(vec![0u8; 4096])).is_none());
    }
}
