//! What a filesystem asks of the machine that will read it, and where it stops growing.
//!
//! One table for every front end: Restore, Build Disk and `new volume` all read [`caveats`], so a
//! fact added to [`RULES`] reaches each of them. See docs/fs_limits.md.

use std::io::Cursor;

use disk_core::display::format_size;

use crate::model::format::{FormatOptions, FsKind};
use crate::restore::PartitionFsType;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Bytes of a volume's start that [`VolumeFacts::from_head`] needs.
pub const HEAD_BYTES: usize = 4096;

/// How much a caveat matters to the person restoring or building.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CaveatLevel {
    Info,
    Warning,
    /// The operation cannot produce this volume, so it must not start.
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caveat {
    pub level: CaveatLevel,
    pub text: String,
}

/// The filesystems the table has something to say about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsFamily {
    Hfs,
    HfsPlus,
    Fat12,
    Fat16,
    Fat32,
    Exfat,
    Ntfs,
    Prodos,
    /// AFFS in international mode, `DOS\2` and `DOS\3`.
    AffsInternational,
}

/// One volume, as far as the caveats care.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeFacts {
    pub family: FsFamily,
    /// The largest size this volume reaches without being rebuilt, when that is limited.
    pub in_place_max: Option<u64>,
}

/// A fact that applies from `from_size` up.
struct Rule {
    family: FsFamily,
    from_size: u64,
    level: CaveatLevel,
    text: &'static str,
}

const fn info(family: FsFamily, from_size: u64, text: &'static str) -> Rule {
    Rule {
        family,
        from_size,
        level: CaveatLevel::Info,
        text,
    }
}

const fn warn(family: FsFamily, from_size: u64, text: &'static str) -> Rule {
    Rule {
        family,
        from_size,
        level: CaveatLevel::Warning,
        text,
    }
}

const RULES: &[Rule] = &[
    info(
        FsFamily::HfsPlus,
        0,
        "HFS+ needs Mac OS 8.1 or later, and a 68k Mac cannot start up from it",
    ),
    info(
        FsFamily::Fat16,
        32 * MIB + 1,
        "FAT16 over 32 MB needs MS-DOS 3.31 or later",
    ),
    warn(
        FsFamily::Fat16,
        2 * GIB + 1,
        "FAT16 over 2 GB uses 64 KB clusters, which only Windows NT reads; DOS and Windows 9x \
         cannot",
    ),
    info(
        FsFamily::Fat32,
        0,
        "FAT32 needs Windows 95 OSR2 or later; MS-DOS 6.22 and Windows NT 4 cannot read it",
    ),
    info(
        FsFamily::Exfat,
        0,
        "exFAT needs Windows Vista SP1 or later (XP with update KB955704), Mac OS X 10.6.5 or \
         Linux 5.4",
    ),
    info(
        FsFamily::Ntfs,
        7 * GIB + 800 * MIB,
        "Windows NT 4 only starts from files in the first 7.8 GB of its system partition",
    ),
    info(
        FsFamily::AffsInternational,
        0,
        "This AFFS mode needs Kickstart 2.0 or later",
    ),
];

impl VolumeFacts {
    fn plain(family: FsFamily) -> Self {
        VolumeFacts {
            family,
            in_place_max: None,
        }
    }

    /// Read a volume's first [`HEAD_BYTES`]; `None` when the table has nothing on it.
    pub fn from_head(head: &[u8], partition_type_string: Option<&str>) -> Option<Self> {
        if matches!(partition_type_string, Some("DOS\\2" | "DOS\\3")) {
            return Some(Self::plain(FsFamily::AffsInternational));
        }
        let mut reader = Cursor::new(head);
        match crate::restore::detect_partition_fs_type(&mut reader, 0) {
            PartitionFsType::Hfs => Some(VolumeFacts {
                family: FsFamily::Hfs,
                in_place_max: hfs_in_place_max(head, &mut reader),
            }),
            PartitionFsType::HfsPlus => Some(Self::plain(FsFamily::HfsPlus)),
            PartitionFsType::Ntfs => Some(Self::plain(FsFamily::Ntfs)),
            PartitionFsType::ProDos => Some(VolumeFacts {
                family: FsFamily::Prodos,
                in_place_max: Some(PRODOS_MAX_BYTES),
            }),
            PartitionFsType::Exfat => Some(VolumeFacts {
                family: FsFamily::Exfat,
                in_place_max: exfat_in_place_max(head),
            }),
            PartitionFsType::Fat => fat_facts(head),
            _ => None,
        }
    }

    /// A blank volume of `kind` at `size`, which has no block-size history to limit it.
    pub fn of_kind(kind: FsKind, size: u64, opts: &FormatOptions) -> Option<Self> {
        use fs_pc::fat::FatType;
        let family = match kind {
            FsKind::Hfs | FsKind::Hfv => FsFamily::Hfs,
            FsKind::Hfsplus => FsFamily::HfsPlus,
            FsKind::Exfat => FsFamily::Exfat,
            FsKind::Ntfs => FsFamily::Ntfs,
            FsKind::Prodos => FsFamily::Prodos,
            FsKind::Fat => {
                let layout =
                    fs_pc::fat::compute_fat_blank_layout_forced(size, 512, opts.fat32).ok()?;
                match layout.fat_type {
                    FatType::Fat12 => FsFamily::Fat12,
                    FatType::Fat16 => FsFamily::Fat16,
                    FatType::Fat32 => FsFamily::Fat32,
                }
            }
            _ => return None,
        };
        Some(Self::plain(family))
    }
}

/// 65535 blocks of 512 bytes.
const PRODOS_MAX_BYTES: u64 = 65535 * 512;

fn le16(b: &[u8], at: usize) -> u64 {
    u16::from_le_bytes([b[at], b[at + 1]]) as u64
}

fn le32(b: &[u8], at: usize) -> u64 {
    u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]]) as u64
}

/// The FAT width from the BPB, and where FAT12 or FAT16 stops; the resizer caps at that size.
fn fat_facts(head: &[u8]) -> Option<VolumeFacts> {
    if head.len() < 512 {
        return None;
    }
    let bps = le16(head, 11);
    let spc = head[13] as u64;
    if !matches!(bps, 512 | 1024 | 2048 | 4096) || spc == 0 {
        return None;
    }
    let sectors_per_fat = le16(head, 22);
    if sectors_per_fat == 0 {
        return Some(VolumeFacts::plain(FsFamily::Fat32));
    }
    let total = match le16(head, 19) {
        0 => le32(head, 32),
        n => n,
    };
    let root_sectors = (le16(head, 17) * 32).div_ceil(bps);
    let data_start = le16(head, 14) + head[16] as u64 * sectors_per_fat + root_sectors;
    let clusters = total.saturating_sub(data_start) / spc;
    let (family, max_clusters, fat_bytes) = if clusters < 4085 {
        (FsFamily::Fat12, 4084, (4086u64 * 3).div_ceil(2))
    } else {
        (FsFamily::Fat16, 65524, 65526 * 2)
    };
    // A compacted backup holds a shrunken FAT, which the resizer regrows ahead of the data.
    let full_fat = fat_bytes.div_ceil(bps).max(sectors_per_fat);
    let data_at_limit = data_start + head[16] as u64 * (full_fat - sectors_per_fat);
    // A partition less than a cluster past the last one is a remainder, and the volume's own size
    // is always within reach.
    let reach = (data_at_limit + (max_clusters + 1) * spc).max(total);
    Some(VolumeFacts {
        family,
        in_place_max: Some(reach * bps),
    })
}

/// The last size that stays in place; under one block past the limit is a remainder, as in
/// `hfs_outgrows_in_place`.
fn hfs_in_place_max(head: &[u8], reader: &mut Cursor<&[u8]>) -> Option<u64> {
    let block = u32::from_be_bytes(head.get(1044..1048)?.try_into().ok()?) as u64;
    fs_mac::hfs::hfs_max_growable_size(reader, 0).map(|max| max + block - 1)
}

/// exFAT's FAT is sized at format time; a lone FAT may still grow into the gap before the heap.
fn exfat_in_place_max(head: &[u8]) -> Option<u64> {
    if head.len() < 512 || head[0x6C] > 12 || head[0x6D] > 25 {
        return None;
    }
    let bps = 1u64 << head[0x6C];
    let spc = 1u64 << head[0x6D];
    let (fat_offset, fat_length, heap) = (le32(head, 0x50), le32(head, 0x54), le32(head, 0x58));
    let usable = if head[0x6E] == 1 {
        heap.saturating_sub(fat_offset).max(fat_length)
    } else {
        fat_length
    };
    let clusters = (usable * bps / 4).saturating_sub(2);
    let volume_sectors = u64::from_le_bytes(head[0x48..0x50].try_into().ok()?);
    Some((heap + (clusters + 1) * spc).max(volume_sectors) * bps)
}

fn family_name(family: FsFamily) -> &'static str {
    match family {
        FsFamily::Hfs => "HFS",
        FsFamily::HfsPlus => "HFS+",
        FsFamily::Fat12 => "FAT12",
        FsFamily::Fat16 => "FAT16",
        FsFamily::Fat32 => "FAT32",
        FsFamily::Exfat => "exFAT",
        FsFamily::Ntfs => "NTFS",
        FsFamily::Prodos => "ProDOS",
        FsFamily::AffsInternational => "AFFS",
    }
}

/// Everything worth saying about this volume at `size`, the worst first.
pub fn caveats(facts: &VolumeFacts, size: u64) -> Vec<Caveat> {
    let mut out: Vec<Caveat> = RULES
        .iter()
        .filter(|r| r.family == facts.family && size >= r.from_size)
        .map(|r| Caveat {
            level: r.level,
            text: r.text.to_string(),
        })
        .collect();
    if facts.family == FsFamily::Hfs {
        hfs_caveats(facts, size, &mut out);
    } else if let Some(max) = facts.in_place_max.filter(|&max| size > max) {
        out.push(Caveat {
            level: CaveatLevel::Warning,
            text: format!(
                "{} stops at {}, so the volume ends there and the rest of its {} partition goes \
                 unused",
                family_name(facts.family),
                format_size(max),
                format_size(size),
            ),
        });
    }
    out.sort_by_key(|c| std::cmp::Reverse(c.level));
    out
}

fn hfs_caveats(facts: &VolumeFacts, size: u64, out: &mut Vec<Caveat>) {
    let limit = fs_mac::hfs::HFS_MAX_VOLUME_BYTES;
    if size > limit {
        out.push(Caveat {
            level: CaveatLevel::Blocked,
            text: format!(
                "Classic HFS stops at {}; this partition is {}",
                format_size(limit),
                format_size(size),
            ),
        });
        return;
    }
    if let Some(note) = fs_mac::hfs::mac_os_size_note(size) {
        out.push(Caveat {
            level: CaveatLevel::Info,
            text: note.to_string(),
        });
    }
    if facts.in_place_max.is_some_and(|max| size > max) {
        out.push(Caveat {
            level: CaveatLevel::Info,
            text: format!(
                "This HFS volume is rebuilt with bigger blocks to reach {}, which needs temporary \
                 disk space for its files",
                format_size(size),
            ),
        });
    }
}

/// The first [`Caveat`] that stops the operation.
pub fn blocked(caveats: &[Caveat]) -> Option<&Caveat> {
    caveats.iter().find(|c| c.level == CaveatLevel::Blocked)
}

/// A caveat as one line of log or Review text.
pub fn line(caveat: &Caveat) -> String {
    let tag = match caveat.level {
        CaveatLevel::Info => "Info",
        CaveatLevel::Warning => "Warning",
        CaveatLevel::Blocked => "Error",
    };
    format!("{tag}: {}", caveat.text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fat_head(total: u16, spf: u16, spc: u8) -> Vec<u8> {
        let mut h = vec![0u8; HEAD_BYTES];
        h[0] = 0xEB;
        h[11..13].copy_from_slice(&512u16.to_le_bytes());
        h[13] = spc;
        h[14] = 1;
        h[16] = 2;
        h[17..19].copy_from_slice(&512u16.to_le_bytes());
        h[19..21].copy_from_slice(&total.to_le_bytes());
        h[22..24].copy_from_slice(&spf.to_le_bytes());
        h
    }

    fn hfs(in_place_max: Option<u64>) -> VolumeFacts {
        VolumeFacts {
            family: FsFamily::Hfs,
            in_place_max,
        }
    }

    #[test]
    fn hfs_past_its_limit_is_blocked_and_the_worst_comes_first() {
        let c = caveats(&hfs(None), fs_mac::hfs::HFS_MAX_VOLUME_BYTES + 512);
        assert_eq!(c[0].level, CaveatLevel::Blocked);
        assert!(blocked(&c).is_some());
        assert!(blocked(&caveats(&hfs(None), fs_mac::hfs::HFS_MAX_VOLUME_BYTES)).is_none());
    }

    #[test]
    fn hfs_says_when_it_will_be_rebuilt_and_which_mac_os_it_needs() {
        let c = caveats(&hfs(Some(GIB)), 3 * GIB);
        assert!(c.iter().any(|c| c.text.contains("rebuilt")));
        assert!(c.iter().any(|c| c.text.contains("System 7.5")));
        assert!(caveats(&hfs(Some(GIB)), GIB).is_empty());
    }

    #[test]
    fn fat_width_comes_from_the_bpb_and_fat16_warns_where_it_stops() {
        let f = VolumeFacts::from_head(&fat_head(40_000, 40, 4), None).unwrap();
        assert_eq!(f.family, FsFamily::Fat16);
        let max = f.in_place_max.unwrap();
        assert!(caveats(&f, max).iter().all(|c| !c.text.contains("stops")));
        assert!(caveats(&f, max + 512)
            .iter()
            .any(|c| c.level == CaveatLevel::Warning && c.text.contains("FAT16 stops")));
        // A compacted head has a short FAT, and the limit still counts the full one.
        let packed = VolumeFacts::from_head(&fat_head(100, 1, 2), None).unwrap();
        assert_eq!(packed.in_place_max, Some((1 + 24 + 32 + 4085 * 2) * 512));
        // A volume is never warned about at its own size, however full its clusters are.
        let full = VolumeFacts::from_head(&fat_head(8_227, 13, 2), None).unwrap();
        assert_eq!(full.family, FsFamily::Fat12);
        assert!(caveats(&full, 8_227 * 512).is_empty());
        let small = VolumeFacts::from_head(&fat_head(2_000, 6, 1), None).unwrap();
        assert_eq!(small.family, FsFamily::Fat12);
        let wide = VolumeFacts::from_head(&fat_head(0, 0, 8), None).unwrap();
        assert_eq!(wide.family, FsFamily::Fat32);
        assert!(caveats(&wide, MIB)[0].text.contains("OSR2"));
    }

    #[test]
    fn a_blank_format_gets_the_same_table() {
        let opts = FormatOptions::default();
        let f = VolumeFacts::of_kind(FsKind::Hfs, 3 * GIB, &opts).unwrap();
        assert!(caveats(&f, 3 * GIB)[0].text.contains("System 7.5"));
        assert_eq!(VolumeFacts::of_kind(FsKind::Minix, MIB, &opts), None);
    }

    #[test]
    fn amiga_modes_come_from_the_dos_type() {
        let f = VolumeFacts::from_head(&[0u8; 16], Some("DOS\\3")).unwrap();
        assert!(caveats(&f, MIB)[0].text.contains("Kickstart 2.0"));
        assert_eq!(VolumeFacts::from_head(&[0u8; 16], Some("DOS\\1")), None);
    }

    #[test]
    fn prodos_ends_at_32_mib() {
        let f = VolumeFacts {
            family: FsFamily::Prodos,
            in_place_max: Some(PRODOS_MAX_BYTES),
        };
        assert!(caveats(&f, 64 * MIB)[0].text.contains("ProDOS stops"));
    }
}
