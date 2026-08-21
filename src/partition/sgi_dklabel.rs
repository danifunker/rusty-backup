//! SGI disk label — the partition scheme on IRIS 2000 / 3000 series disks,
//! a decade before the IRIX volume header in [`sgi`](super::sgi).
//!
//! Block 0 of the drive holds one `struct disk_label` (274 bytes): drive
//! geometry, the alternate-block region, and eight `{d_base, d_size}` slots.
//! Blocks 1-4 hold the bad-block map (64 × `{d_bad, d_good}` per block, 256
//! max). Everything is big-endian, but the struct was compiled for the 68020
//! with **2-byte natural alignment**, so `long` fields land on offsets that
//! are only even, not 4-aligned — `d_altstart` at 0x0E is the first one.
//!
//! Layout, verbatim from `<sys/dklabel.h>` on the source disk (RCS revision
//! 1.5, 87/11/20):
//!
//! | Off  | Field | |
//! |------|-------|-|
//! | 0x00 | `d_magic` be32 | [`SGI_DKLABEL_MAGIC`] |
//! | 0x04 | `d_type` be16 | drive type, see [`drive_type_name`] |
//! | 0x06 | `d_controller` be16 | see [`controller_name`] |
//! | 0x08 | `d_cylinders` / `d_heads` / `d_sectors` be16 | geometry |
//! | 0x0E | `d_altstart` be32 | first block of the alternates region |
//! | 0x12 | `d_nalternates` be16 | blocks reserved there |
//! | 0x14 | `d_bootfs` / `d_swapfs` u8 | slot numbers |
//! | 0x16 | `d_map[8]` | 8 × `{be32 d_base, be32 d_size}` |
//! | 0x56 | `d_interleave` / `d_trackskew` / `d_cylskew` i8 | |
//! | 0x5A | `d_badspots` be16 | |
//! | 0x5C | `d_name[50]` | drive model, e.g. `Priam V170` |
//! | 0x8E | `d_serial[50]` | |
//! | 0xC0 | `d_misc[20]` be32 | gap / group sizes |
//! | 0x110 | `d_rootnotboot` / `d_rootfs` u8 | |
//!
//! ## Byte order on the medium
//!
//! The 68020 is big-endian, but the disk controllers of that era (DSD 5217,
//! Interphase 2190) transferred 16-bit words with the two bytes reversed, and
//! images taken off those drives come out **byte-swapped within every 16-bit
//! word**. That swap covers the whole medium — label, superblocks, inodes and
//! file data alike — so it is a property of the image, not of any one
//! structure. [`SgiLabelByteOrder`] records which way round a given image is;
//! [`detect`] decides by trying the magic both ways, and `fs/efs_v1.rs` makes
//! the same call independently from the EFS superblock magic so a bare
//! partition image works without a label.
//!
//! We deliberately do **not** normalise the swap at the reader level: a backup
//! must reproduce the source image byte for byte, so only the *interpretation*
//! is swapped, never the stored bytes.

use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};

use crate::error::RustyBackupError;

/// `D_MAGIC` at byte 0 of block 0, big-endian.
pub const SGI_DKLABEL_MAGIC: u32 = 0x0007_2959;

/// On-disk size of `struct disk_label`.
pub const SGI_DKLABEL_SIZE: usize = 0x112;

/// `NFS` — partition slots in the label.
pub const SGI_DKLABEL_NFS: usize = 8;

/// Blocks 1-4 hold the bad-block map, so no filesystem may start before 5.
pub const SGI_DKLABEL_RESERVED_BLOCKS: u64 = 5;

/// Synthetic MBR-style type byte for a slot on an SGI disk label. The label has
/// no per-slot type field, so this is what routes a slot at
/// `open_filesystem` time — the same trick `PartitionTable::Sgi` plays with
/// 0xA0 / 0xA1. Only this table produces it.
pub const SGI_TYPE_BYTE_EFS_V1: u8 = 0xA2;

const OFF_MAP: usize = 0x16;
const OFF_NAME: usize = 0x5C;
const OFF_SERIAL: usize = 0x8E;
const NAME_LEN: usize = 50;

/// Which way round the 16-bit words of an image sit. See the module header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SgiLabelByteOrder {
    /// Big-endian as the 68020 saw it; no fixup needed.
    Native,
    /// Every 16-bit word has its two bytes reversed.
    Swabbed,
}

impl SgiLabelByteOrder {
    /// Plain-ASCII name for logs and the inspect view.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Swabbed => "byte-swapped",
        }
    }
}

/// Reverse the two bytes of every 16-bit word in `buf`. A trailing odd byte is
/// left alone, which only matters for callers that read unaligned tails.
pub fn swab16_in_place(buf: &mut [u8]) {
    for pair in buf.chunks_exact_mut(2) {
        pair.swap(0, 1);
    }
}

/// Apply `order` to a freshly-read buffer so its 16-bit words read as the
/// 68020 wrote them.
pub fn apply_byte_order(order: SgiLabelByteOrder, buf: &mut [u8]) {
    if order == SgiLabelByteOrder::Swabbed {
        swab16_in_place(buf);
    }
}

/// One `struct disk_map` slot, in 512-byte blocks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct SgiDiskMap {
    /// `d_base` — first logical block.
    pub base: u32,
    /// `d_size` — length in blocks.
    pub size: u32,
}

impl SgiDiskMap {
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
    pub fn end(&self) -> u64 {
        self.base as u64 + self.size as u64
    }
    pub fn size_bytes(&self) -> u64 {
        self.size as u64 * 512
    }
    pub fn start_offset(&self) -> u64 {
        self.base as u64 * 512
    }
    /// True when `self` covers all of `other` and is strictly larger — the
    /// shape of the whole-disk wrapper slots.
    fn contains(&self, other: &SgiDiskMap) -> bool {
        !self.is_empty()
            && !other.is_empty()
            && self.base as u64 <= other.base as u64
            && self.end() >= other.end()
            && self.size > other.size
    }
}

/// A parsed SGI disk label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SgiDiskLabel {
    /// How the image's 16-bit words are ordered; see [`SgiLabelByteOrder`].
    pub byte_order: SgiLabelByteOrder,
    pub drive_type: u16,
    pub controller: u16,
    pub cylinders: u16,
    pub heads: u16,
    /// Sectors per track.
    pub sectors: u16,
    /// First block of the alternates region — also the end of usable space.
    pub altstart: u32,
    pub nalternates: u16,
    /// Slot number the PROM boots from.
    pub bootfs: u8,
    /// Slot number used as swap.
    pub swapfs: u8,
    pub map: Vec<SgiDiskMap>,
    pub interleave: i8,
    pub trackskew: i8,
    pub cylskew: i8,
    pub badspots: u16,
    /// Drive model, e.g. `Priam V170`.
    pub name: String,
    pub serial: String,
    /// Nonzero when root and boot are different slots.
    pub rootnotboot: u8,
    /// Slot number of the root filesystem when `rootnotboot` is set.
    pub rootfs: u8,
}

/// Decide whether `sector0` holds a disk label, and in which byte order.
/// `None` when neither orientation shows the magic.
pub fn detect(sector0: &[u8]) -> Option<SgiLabelByteOrder> {
    if sector0.len() < SGI_DKLABEL_SIZE {
        return None;
    }
    if BigEndian::read_u32(&sector0[0..4]) == SGI_DKLABEL_MAGIC {
        return Some(SgiLabelByteOrder::Native);
    }
    let swabbed = u32::from_be_bytes([sector0[1], sector0[0], sector0[3], sector0[2]]);
    if swabbed == SGI_DKLABEL_MAGIC {
        return Some(SgiLabelByteOrder::Swabbed);
    }
    None
}

impl SgiDiskLabel {
    /// Parse block 0. `sector0` is the raw image bytes; the byte order is
    /// detected and the working copy fixed up before any field is read.
    pub fn parse(sector0: &[u8]) -> Result<Self, RustyBackupError> {
        let order = detect(sector0).ok_or_else(|| {
            RustyBackupError::InvalidMbr("not an SGI disk label (bad magic)".to_string())
        })?;
        Self::parse_with_order(sector0, order)
    }

    /// Parse block 0 in a known byte order.
    pub fn parse_with_order(
        sector0: &[u8],
        order: SgiLabelByteOrder,
    ) -> Result<Self, RustyBackupError> {
        if sector0.len() < SGI_DKLABEL_SIZE {
            return Err(RustyBackupError::InvalidMbr(format!(
                "SGI disk label needs {SGI_DKLABEL_SIZE} bytes, got {}",
                sector0.len()
            )));
        }
        let mut buf = sector0[..SGI_DKLABEL_SIZE].to_vec();
        apply_byte_order(order, &mut buf);
        if BigEndian::read_u32(&buf[0..4]) != SGI_DKLABEL_MAGIC {
            return Err(RustyBackupError::InvalidMbr(
                "not an SGI disk label (bad magic)".to_string(),
            ));
        }

        let cylinders = BigEndian::read_u16(&buf[0x08..0x0A]);
        let heads = BigEndian::read_u16(&buf[0x0A..0x0C]);
        let sectors = BigEndian::read_u16(&buf[0x0C..0x0E]);
        if cylinders == 0 || heads == 0 || sectors == 0 {
            return Err(RustyBackupError::InvalidMbr(format!(
                "SGI disk label has degenerate geometry {cylinders}c/{heads}h/{sectors}s"
            )));
        }

        let map = (0..SGI_DKLABEL_NFS)
            .map(|i| {
                let o = OFF_MAP + i * 8;
                SgiDiskMap {
                    base: BigEndian::read_u32(&buf[o..o + 4]),
                    size: BigEndian::read_u32(&buf[o + 4..o + 8]),
                }
            })
            .collect();

        Ok(SgiDiskLabel {
            byte_order: order,
            drive_type: BigEndian::read_u16(&buf[0x04..0x06]),
            controller: BigEndian::read_u16(&buf[0x06..0x08]),
            cylinders,
            heads,
            sectors,
            altstart: BigEndian::read_u32(&buf[0x0E..0x12]),
            nalternates: BigEndian::read_u16(&buf[0x12..0x14]),
            bootfs: buf[0x14],
            swapfs: buf[0x15],
            map,
            interleave: buf[0x56] as i8,
            trackskew: buf[0x57] as i8,
            cylskew: buf[0x58] as i8,
            badspots: BigEndian::read_u16(&buf[0x5A..0x5C]),
            name: trim_c_string(&buf[OFF_NAME..OFF_NAME + NAME_LEN]),
            serial: trim_c_string(&buf[OFF_SERIAL..OFF_SERIAL + NAME_LEN]),
            rootnotboot: buf[0x110],
            rootfs: buf[0x111],
        })
    }

    /// Total blocks the drive's geometry describes.
    pub fn total_blocks(&self) -> u64 {
        self.cylinders as u64 * self.heads as u64 * self.sectors as u64
    }

    /// Slot number holding the root filesystem.
    pub fn root_slot(&self) -> u8 {
        if self.rootnotboot != 0 {
            self.rootfs
        } else {
            self.bootfs
        }
    }

    /// The role the label assigns to slot `index`, for display.
    pub fn slot_role(&self, index: usize) -> &'static str {
        let i = index as u8;
        if i == self.swapfs {
            "swap"
        } else if i == self.root_slot() {
            "root"
        } else if i == self.bootfs {
            "boot"
        } else {
            "slice"
        }
    }

    /// True when slot `index` is a whole-disk wrapper — it swallows another
    /// slot entirely, or duplicates an earlier one. Those overlap the real
    /// partitions and would only confuse the browse list, exactly like the
    /// SGI VOLHDR / VOLUME and Sun `backup` slices.
    pub fn is_wrapper_slot(&self, index: usize) -> bool {
        let Some(me) = self.map.get(index) else {
            return true;
        };
        if me.is_empty() {
            return true;
        }
        self.map.iter().enumerate().any(|(j, other)| {
            if j == index {
                return false;
            }
            me.contains(other) || (j < index && other.base == me.base && other.size == me.size)
        })
    }

    /// Slots worth offering as browsable partitions, with their label index.
    pub fn browsable_slots(&self) -> impl Iterator<Item = (usize, &SgiDiskMap)> {
        self.map
            .iter()
            .enumerate()
            .filter(move |(i, _)| !self.is_wrapper_slot(*i))
    }
}

/// Trim a fixed-width C string field to its printable prefix.
fn trim_c_string(raw: &[u8]) -> String {
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    String::from_utf8_lossy(&raw[..end])
        .trim()
        .replace(|c: char| c.is_control(), "")
}

/// `d_type` decoded, from the `DT_*` table in `<sys/dklabel.h>`.
pub fn drive_type_name(t: u16) -> &'static str {
    match t {
        0 => "Atasi 3046",
        1 => "Vertex V170",
        2 => "Fujitsu 2312K",
        3 => "Fujitsu 2351 Eagle",
        4 => "Maxtor 1085",
        5 => "CDC Wren II",
        6 => "Vertex V185",
        7 => "Hitachi 512-8",
        8 => "Maxtor 1140",
        9 => "Micropolis 1325",
        10 => "Vertex V130",
        11 => "Fujitsu 2243",
        12 => "Memorex 514 / NEC 1055",
        13 => "Tandon T101 floppy",
        14 => "Tandon TM-252",
        15 => "Qume 592 floppy",
        16 => "AST 96202 ESDI",
        17 => "AST 96203 ESDI",
        18 => "Cynthia D570",
        19 => "Miniscribe 3212",
        20 => "CDC Wren II ESDI",
        21 => "Tandon TM-362",
        22 => "Maxtor 4175 ESDI",
        23 => "Hitachi 512-8 ESDI",
        24 => "Hitachi 512-2 ESDI",
        25 => "Hitachi 512-7 ESDI",
        26 => "Aim Dart 130 SMD",
        27 => "CMI 3426",
        28 => "NEC D5126",
        29 => "Fujitsu 2246 ESDI",
        30 => "Micropolis 1550-15",
        31 => "Siemens 1100",
        32 => "Siemens 1200",
        33 => "Siemens 1300",
        34 => "Maxtor 2085",
        35 => "Toshiba 156FA ESDI",
        36 => "Toshiba MK56FB",
        37 => "CDC Wren III ESDI",
        38 => "CDC 9766 SMD",
        39 => "Fujitsu 2249 ESDI",
        40 => "Hitachi 51438 ESDI",
        41 => "AMS 513 Century Data SMD",
        _ => "unknown drive",
    }
}

/// `d_controller` decoded, from the `DC_*` table in `<sys/dklabel.h>`.
pub fn controller_name(c: u16) -> &'static str {
    match c {
        0 => "DSD 5217",
        1 => "Xylogics 450",
        2 => "Interphase 2190",
        3 => "Interphase Storager",
        _ => "unknown controller",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The label from the real IRIS 3130 `Priam V170` disk, native order.
    fn sample_label() -> Vec<u8> {
        let mut b = vec![0u8; 512];
        BigEndian::write_u32(&mut b[0..4], SGI_DKLABEL_MAGIC);
        BigEndian::write_u16(&mut b[0x04..0x06], 1); // DT_V170
        BigEndian::write_u16(&mut b[0x06..0x08], 0); // DC_DSD5217
        BigEndian::write_u16(&mut b[0x08..0x0A], 987);
        BigEndian::write_u16(&mut b[0x0A..0x0C], 7);
        BigEndian::write_u16(&mut b[0x0C..0x0E], 17);
        BigEndian::write_u32(&mut b[0x0E..0x12], 115_430);
        BigEndian::write_u16(&mut b[0x12..0x14], 2023);
        b[0x14] = 0; // bootfs
        b[0x15] = 1; // swapfs
        let slots: [(u32, u32); 8] = [
            (119, 17850),
            (17969, 17731),
            (35700, 79730),
            (119, 115_311),
            (0, 0),
            (0, 0),
            (119, 115_311),
            (0, 115_430),
        ];
        for (i, (base, size)) in slots.iter().enumerate() {
            let o = OFF_MAP + i * 8;
            BigEndian::write_u32(&mut b[o..o + 4], *base);
            BigEndian::write_u32(&mut b[o + 4..o + 8], *size);
        }
        b[0x56] = 1; // interleave
        b[0x58] = 11; // cylskew
        b[OFF_NAME..OFF_NAME + 10].copy_from_slice(b"Priam V170");
        b[OFF_SERIAL..OFF_SERIAL + 4].copy_from_slice(b"0000");
        b
    }

    fn swabbed(mut b: Vec<u8>) -> Vec<u8> {
        swab16_in_place(&mut b);
        b
    }

    #[test]
    fn parses_geometry_and_slots() {
        let l = SgiDiskLabel::parse(&sample_label()).unwrap();
        assert_eq!(l.byte_order, SgiLabelByteOrder::Native);
        assert_eq!((l.cylinders, l.heads, l.sectors), (987, 7, 17));
        assert_eq!(l.total_blocks(), 117_453);
        assert_eq!(l.altstart as u64 + l.nalternates as u64, l.total_blocks());
        assert_eq!(l.name, "Priam V170");
        assert_eq!(l.serial, "0000");
        assert_eq!(drive_type_name(l.drive_type), "Vertex V170");
        assert_eq!(controller_name(l.controller), "DSD 5217");
        assert_eq!(l.map[0].base, 119);
        assert_eq!(l.map[2].size, 79_730);
    }

    #[test]
    fn detects_and_parses_byte_swapped_image() {
        let raw = swabbed(sample_label());
        assert_eq!(detect(&raw), Some(SgiLabelByteOrder::Swabbed));
        let l = SgiDiskLabel::parse(&raw).unwrap();
        assert_eq!(l.byte_order, SgiLabelByteOrder::Swabbed);
        assert_eq!((l.cylinders, l.heads, l.sectors), (987, 7, 17));
        assert_eq!(l.name, "Priam V170");
        assert_eq!(l.map[1].base, 17_969);
    }

    #[test]
    fn wrapper_slots_are_filtered_out() {
        let l = SgiDiskLabel::parse(&sample_label()).unwrap();
        let kept: Vec<usize> = l.browsable_slots().map(|(i, _)| i).collect();
        assert_eq!(kept, vec![0, 1, 2]);
    }

    #[test]
    fn slot_roles_follow_bootfs_and_swapfs() {
        let l = SgiDiskLabel::parse(&sample_label()).unwrap();
        assert_eq!(l.slot_role(0), "root");
        assert_eq!(l.slot_role(1), "swap");
        assert_eq!(l.slot_role(2), "slice");
    }

    #[test]
    fn rejects_foreign_sector() {
        let mut b = vec![0u8; 512];
        b[510] = 0x55;
        b[511] = 0xAA;
        assert!(detect(&b).is_none());
        assert!(SgiDiskLabel::parse(&b).is_err());
    }

    #[test]
    fn rejects_degenerate_geometry() {
        let mut b = sample_label();
        BigEndian::write_u16(&mut b[0x0A..0x0C], 0);
        assert!(SgiDiskLabel::parse(&b).is_err());
    }

    #[test]
    fn partition_table_reports_the_word_order() {
        use crate::partition::PartitionTable;
        let native = PartitionTable::SgiDkLabel(SgiDiskLabel::parse(&sample_label()).unwrap());
        let swapped =
            PartitionTable::SgiDkLabel(SgiDiskLabel::parse(&swabbed(sample_label())).unwrap());
        assert_eq!(native.byte_order_name(), Some("native"));
        assert_eq!(swapped.byte_order_name(), Some("byte-swapped"));
    }

    #[test]
    fn tables_without_a_word_order_report_none() {
        use crate::partition::PartitionTable;
        let sf = PartitionTable::None {
            size_bytes: 1_474_560,
            fs_hint: "FAT".to_string(),
        };
        assert_eq!(sf.byte_order_name(), None);
    }

    #[test]
    fn swab_is_its_own_inverse() {
        let orig = sample_label();
        let mut round = orig.clone();
        swab16_in_place(&mut round);
        swab16_in_place(&mut round);
        assert_eq!(orig, round);
    }
}
