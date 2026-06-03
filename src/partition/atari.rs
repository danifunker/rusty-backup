//! Atari HD File System Driver (AHDI) partition table.
//!
//! AHDI is the MBR-equivalent used by Atari ST / TT / Falcon hard-disk images
//! (`.vhd`-renamed and bare `.img` files for the MiSTer AtariST core). It is
//! laid out at byte 0 of the disk:
//!
//! ```text
//! 0x000..0x1C6  (454 B)  bootstrap code
//! 0x1C6..0x1F6   (48 B)  4 partition entries (12 B each, big-endian)
//! 0x1F6..0x1FA    (4 B)  total disk size in 512-byte sectors (BE u32, optional)
//! 0x1FA..0x1FE    (4 B)  bad-sector list start (BE u32, optional, often 0)
//! 0x1FE..0x200    (2 B)  checksum (BE u16, big-endian word-sum = 0x1234)
//! ```
//!
//! Each 12-byte slot:
//!
//! ```text
//! +0       flags   (0x01 = exists, 0x80 = bootable, other bits reserved)
//! +1..4    id      (3 ASCII bytes — see IDs below)
//! +4..8    start   (BE u32, first 512-byte sector of the partition)
//! +8..12   blocks  (BE u32, sector count)
//! ```
//!
//! IDs we recognize:
//! - `GEM` — GEMDOS partition, FAT12 / FAT16 (≤ 16 MiB conventionally)
//! - `BGM` — Big-GEM, FAT16 (> 16 MiB)
//! - `XGM` — extended container, linked-list of further root sectors (like MBR
//!   extended), nested entries describe logical partitions
//! - `RAW` — raw / non-FAT data; skipped from browse (no filesystem to dispatch)
//! - others (`F32`, `MAC`, etc.) — fall through as raw with a passthrough type
//!
//! Notable distinctions from MBR:
//! - No `0xAA55` boot signature at bytes 510-511. Detection relies on at least
//!   one entry having a recognized ID with in-bounds geometry.
//! - All multi-byte fields are **big-endian** (m68k native).
//! - Sectors are always 512 bytes (no logical-sector-size field).
//!
//! Reference: DrCoolZic, *Atari Hard Disk File Systems Reference Guide*, §3
//! (AHDI Root Sector). Confirmed §8 of `docs/mister_filesystem_implementation_plan.md`.

use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};

use crate::error::RustyBackupError;

/// AHDI root sector is exactly one 512-byte sector at disk offset 0.
pub const AHDI_ROOT_SIZE: usize = 512;

/// Number of primary partition slots in the root sector.
pub const AHDI_NUM_SLOTS: usize = 4;

/// Byte offset of the first partition entry.
const AHDI_ENTRY_OFFSET: usize = 0x1C6;

/// Each partition entry is 12 bytes.
const AHDI_ENTRY_SIZE: usize = 12;

/// Maximum XGM extended-chain follow-ups to walk. Real AHDI disks never have
/// more than a handful; the cap is a runaway guard.
const AHDI_MAX_XGM_FOLLOWUPS: usize = 64;

/// Maximum partitions we surface from the table (primary + logical).
const AHDI_MAX_TOTAL_PARTITIONS: usize = 128;

/// Synthetic MBR type byte for GEM partitions (FAT12). The FAT driver also
/// auto-detects from BPB so this is just the dispatch hint into `fs/mod.rs`.
pub const AHDI_TYPE_BYTE_GEM: u8 = 0x01;

/// Synthetic MBR type byte for BGM partitions (FAT16 > 16 MiB).
pub const AHDI_TYPE_BYTE_BGM: u8 = 0x06;

/// Synthetic MBR type byte for XGM extended containers (mirrors MBR 0x05).
pub const AHDI_TYPE_BYTE_XGM: u8 = 0x05;

/// Synthetic MBR type byte for RAW / unknown — 0 keeps dispatch quiet.
pub const AHDI_TYPE_BYTE_RAW: u8 = 0x00;

/// Recognized AHDI partition kind. Unknown IDs round-trip through `Other`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AhdiPartitionKind {
    /// GEMDOS partition. FAT12 or small FAT16.
    Gem,
    /// "Big GEM" — FAT16 partition larger than 16 MiB.
    Bgm,
    /// Extended container. Its `start` sector holds further root-sector-shaped
    /// blocks whose entries describe logical partitions; analogous to MBR's
    /// extended-partition EBR chain.
    Xgm,
    /// Raw / non-filesystem.
    Raw,
    /// `F32` (some Atari tools) / `MAC` / etc. — pass through, route as raw.
    Other([u8; 3]),
}

impl AhdiPartitionKind {
    pub fn from_bytes(id: [u8; 3]) -> Self {
        match &id {
            b"GEM" => Self::Gem,
            b"BGM" => Self::Bgm,
            b"XGM" => Self::Xgm,
            b"RAW" => Self::Raw,
            _ => Self::Other(id),
        }
    }

    pub fn to_bytes(self) -> [u8; 3] {
        match self {
            Self::Gem => *b"GEM",
            Self::Bgm => *b"BGM",
            Self::Xgm => *b"XGM",
            Self::Raw => *b"RAW",
            Self::Other(b) => b,
        }
    }

    /// Plain-ASCII display name (no Unicode glyphs).
    pub fn display_name(&self) -> String {
        match self {
            Self::Gem => "GEM".to_string(),
            Self::Bgm => "BGM".to_string(),
            Self::Xgm => "XGM".to_string(),
            Self::Raw => "RAW".to_string(),
            Self::Other(b) => String::from_utf8_lossy(b).to_string(),
        }
    }

    /// Synthetic MBR-style type byte for routing through `fs/mod.rs`.
    pub fn synthetic_type_byte(&self) -> u8 {
        match self {
            Self::Gem => AHDI_TYPE_BYTE_GEM,
            Self::Bgm => AHDI_TYPE_BYTE_BGM,
            Self::Xgm => AHDI_TYPE_BYTE_XGM,
            Self::Raw | Self::Other(_) => AHDI_TYPE_BYTE_RAW,
        }
    }

    /// True if the ID is a known/printable shape — used during detection to
    /// reject random noise from a non-AHDI bootstrap block.
    pub fn is_recognized_or_printable(&self) -> bool {
        match self {
            Self::Gem | Self::Bgm | Self::Xgm | Self::Raw => true,
            Self::Other(b) => b
                .iter()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
        }
    }
}

/// One 12-byte AHDI partition slot, decoded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhdiPartitionEntry {
    pub flags: u8,
    pub kind: AhdiPartitionKind,
    /// 512-byte sector number of the first sector. For XGM-nested entries this
    /// is absolute (we add the XGM container's start during parse).
    pub start_sector: u32,
    /// Sector count.
    pub sector_count: u32,
    /// True if this entry came from an XGM extended-chain follow-up sector
    /// rather than the root. Logical partitions only.
    pub is_logical: bool,
}

impl AhdiPartitionEntry {
    fn parse(buf: &[u8; AHDI_ENTRY_SIZE], is_logical: bool) -> Self {
        let flags = buf[0];
        let id = [buf[1], buf[2], buf[3]];
        let start = BigEndian::read_u32(&buf[4..8]);
        let blocks = BigEndian::read_u32(&buf[8..12]);
        AhdiPartitionEntry {
            flags,
            kind: AhdiPartitionKind::from_bytes(id),
            start_sector: start,
            sector_count: blocks,
            is_logical,
        }
    }

    fn to_bytes(&self) -> [u8; AHDI_ENTRY_SIZE] {
        let mut buf = [0u8; AHDI_ENTRY_SIZE];
        buf[0] = self.flags;
        let id = self.kind.to_bytes();
        buf[1..4].copy_from_slice(&id);
        BigEndian::write_u32(&mut buf[4..8], self.start_sector);
        BigEndian::write_u32(&mut buf[8..12], self.sector_count);
        buf
    }

    pub fn is_empty(&self) -> bool {
        self.flags == 0
            && self.start_sector == 0
            && self.sector_count == 0
            && matches!(self.kind, AhdiPartitionKind::Other(b) if b == [0, 0, 0])
    }

    /// Bit 0 of `flags` — exists / in-use marker.
    pub fn exists(&self) -> bool {
        self.flags & 0x01 != 0
    }

    /// Bit 7 of `flags` — bootable.
    pub fn bootable(&self) -> bool {
        self.flags & 0x80 != 0
    }

    pub fn size_bytes(&self) -> u64 {
        self.sector_count as u64 * 512
    }

    pub fn start_offset(&self) -> u64 {
        self.start_sector as u64 * 512
    }
}

/// Parsed AHDI partition table (root sector plus any walked XGM follow-ups).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AhdiTable {
    /// Up to 4 primary entries from the root sector. Empty slots are kept so
    /// `to_bytes` round-trips byte-for-byte.
    pub primary: [AhdiPartitionEntry; AHDI_NUM_SLOTS],
    /// Logical partitions walked out of XGM extended containers.
    pub logical: Vec<AhdiPartitionEntry>,
    /// Optional disk-size field (BE u32 at 0x1F6) — many AHDI tools leave 0.
    pub disk_size_sectors: u32,
    /// Optional bad-sector list start at 0x1FA.
    pub bad_sector_list_start: u32,
    /// On-disk checksum at 0x1FE. AHDI's checksum is the big-endian word-sum
    /// of the 256 u16 words of the root sector, expected to equal 0x1234.
    pub checksum: u16,
    /// True if the on-disk checksum verified.
    pub checksum_valid: bool,
}

impl AhdiTable {
    /// Parse the AHDI root sector. `buf` must be at least 512 bytes; only the
    /// first 512 are consulted. Validates entry shape; does NOT walk XGM
    /// chains (use [`AhdiTable::detect_and_walk`] for that).
    pub fn parse_root(buf: &[u8]) -> Result<Self, RustyBackupError> {
        if buf.len() < AHDI_ROOT_SIZE {
            return Err(RustyBackupError::InvalidAhdi(format!(
                "root buffer too small: {} bytes (need {AHDI_ROOT_SIZE})",
                buf.len()
            )));
        }
        let sector = &buf[..AHDI_ROOT_SIZE];

        let primary: [AhdiPartitionEntry; AHDI_NUM_SLOTS] = std::array::from_fn(|i| {
            let off = AHDI_ENTRY_OFFSET + i * AHDI_ENTRY_SIZE;
            AhdiPartitionEntry::parse(
                sector[off..off + AHDI_ENTRY_SIZE]
                    .try_into()
                    .expect("12-byte slice"),
                false,
            )
        });

        let disk_size_sectors = BigEndian::read_u32(&sector[0x1F6..0x1FA]);
        let bad_sector_list_start = BigEndian::read_u32(&sector[0x1FA..0x1FE]);
        let checksum = BigEndian::read_u16(&sector[0x1FE..0x200]);
        let checksum_valid = ahdi_checksum_matches(sector);

        Ok(AhdiTable {
            primary,
            logical: Vec::new(),
            disk_size_sectors,
            bad_sector_list_start,
            checksum,
            checksum_valid,
        })
    }

    /// Serialize the root sector back to 512 bytes. Recomputes the checksum
    /// so the big-endian word-sum equals 0x1234. The leading 454-byte
    /// bootstrap area is zeroed — round-trip callers (write paths) should
    /// preserve the original bootstrap separately and graft it back in.
    pub fn root_to_bytes(&self) -> [u8; AHDI_ROOT_SIZE] {
        let mut buf = [0u8; AHDI_ROOT_SIZE];
        for (i, e) in self.primary.iter().enumerate() {
            let off = AHDI_ENTRY_OFFSET + i * AHDI_ENTRY_SIZE;
            buf[off..off + AHDI_ENTRY_SIZE].copy_from_slice(&e.to_bytes());
        }
        BigEndian::write_u32(&mut buf[0x1F6..0x1FA], self.disk_size_sectors);
        BigEndian::write_u32(&mut buf[0x1FA..0x1FE], self.bad_sector_list_start);
        let cksum = ahdi_compute_checksum(&buf);
        BigEndian::write_u16(&mut buf[0x1FE..0x200], cksum);
        buf
    }

    /// Total size of the disk (bytes) derived from the table's largest
    /// `start + count` end, in 512-byte sectors. Used when the `disk_size_sectors`
    /// field is zero.
    pub fn derived_disk_size_bytes(&self) -> u64 {
        let mut max_end: u64 = 0;
        for e in self.primary.iter().chain(self.logical.iter()) {
            if e.exists() || e.sector_count > 0 {
                let end = e.start_sector as u64 + e.sector_count as u64;
                if end > max_end {
                    max_end = end;
                }
            }
        }
        max_end * 512
    }

    /// Probe + parse + walk XGM extended chains. Returns `Err(InvalidAhdi)`
    /// if validation fails. Caller is responsible for restricting this to
    /// disks that have already been rejected as MBR (no 0xAA55 signature).
    pub fn detect_and_walk<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        disk_size_bytes: u64,
    ) -> Result<Self, RustyBackupError> {
        use std::io::SeekFrom;

        reader
            .seek(SeekFrom::Start(0))
            .map_err(RustyBackupError::Io)?;
        let mut root = [0u8; AHDI_ROOT_SIZE];
        reader
            .read_exact(&mut root)
            .map_err(|e| RustyBackupError::InvalidAhdi(format!("cannot read root sector: {e}")))?;

        let mut table = Self::parse_root(&root)?;

        // Validate primary entries before walking XGM. At least one must
        // carry a recognized ID + plausible geometry. This is the
        // detection backstop since AHDI has no magic number.
        let disk_sectors = disk_size_bytes / 512;
        let primaries_valid: Vec<&AhdiPartitionEntry> = table
            .primary
            .iter()
            .filter(|e| {
                e.exists()
                    && e.sector_count > 0
                    && e.kind.is_recognized_or_printable()
                    && (e.start_sector as u64).saturating_add(e.sector_count as u64) <= disk_sectors
            })
            .collect();
        if primaries_valid.is_empty() {
            return Err(RustyBackupError::InvalidAhdi(
                "no valid AHDI primary entries".to_string(),
            ));
        }

        // Walk every XGM container in the primary table. The XGM chain
        // mirrors MBR's EBR semantics:
        //   - The primary XGM entry's `start_sector` points to the first
        //     extended root sector.
        //   - Inside each extended root sector, slot 0 describes a logical
        //     partition whose `start_sector` is relative to the *current*
        //     extended root sector.
        //   - Slot 1 (if it's another XGM) links to the next extended root
        //     sector whose `start_sector` is relative to the *outermost*
        //     XGM container.
        for primary in &table.primary {
            if !primary.exists() {
                continue;
            }
            if !matches!(primary.kind, AhdiPartitionKind::Xgm) {
                continue;
            }
            let container_start = primary.start_sector;
            let mut current = container_start;
            let mut visited = std::collections::HashSet::new();
            for _hop in 0..AHDI_MAX_XGM_FOLLOWUPS {
                if !visited.insert(current) {
                    break;
                }
                let offset = current as u64 * 512;
                if reader.seek(SeekFrom::Start(offset)).is_err() {
                    break;
                }
                let mut sec = [0u8; AHDI_ROOT_SIZE];
                if reader.read_exact(&mut sec).is_err() {
                    break;
                }

                // Slot 0 = logical entry, relative to the CURRENT extended
                // root sector.
                let entry0 = AhdiPartitionEntry::parse(
                    sec[AHDI_ENTRY_OFFSET..AHDI_ENTRY_OFFSET + AHDI_ENTRY_SIZE]
                        .try_into()
                        .expect("12-byte slice"),
                    true,
                );
                if entry0.exists() && entry0.sector_count > 0 {
                    let abs_start = current.saturating_add(entry0.start_sector);
                    if (abs_start as u64).saturating_add(entry0.sector_count as u64) <= disk_sectors
                    {
                        table.logical.push(AhdiPartitionEntry {
                            start_sector: abs_start,
                            ..entry0
                        });
                        if table.primary.len() + table.logical.len() >= AHDI_MAX_TOTAL_PARTITIONS {
                            break;
                        }
                    }
                }

                // Slot 1 = XGM link, relative to the OUTERMOST XGM container.
                let entry1_off = AHDI_ENTRY_OFFSET + AHDI_ENTRY_SIZE;
                let entry1 = AhdiPartitionEntry::parse(
                    sec[entry1_off..entry1_off + AHDI_ENTRY_SIZE]
                        .try_into()
                        .expect("12-byte slice"),
                    false,
                );
                if !entry1.exists() || !matches!(entry1.kind, AhdiPartitionKind::Xgm) {
                    break;
                }
                let next = container_start.saturating_add(entry1.start_sector);
                if next == current {
                    break;
                }
                current = next;
            }
        }

        Ok(table)
    }
}

/// Compute the AHDI big-endian word-sum checksum for a 512-byte root sector.
///
/// The target value is `0x1234`. The checksum field at 0x1FE is included in
/// the sum, so to make the sum hit 0x1234 the field is set to:
///   `0x1234 - (sum_of_all_other_words)`.
fn ahdi_compute_checksum(buf: &[u8]) -> u16 {
    assert_eq!(buf.len(), AHDI_ROOT_SIZE);
    let mut sum: u32 = 0;
    for chunk in buf[..0x1FE].chunks_exact(2) {
        sum = sum.wrapping_add(u16::from_be_bytes([chunk[0], chunk[1]]) as u32);
    }
    (0x1234u32.wrapping_sub(sum) & 0xFFFF) as u16
}

/// True if the on-disk word-sum of the 512-byte root sector equals 0x1234.
fn ahdi_checksum_matches(buf: &[u8]) -> bool {
    assert_eq!(buf.len(), AHDI_ROOT_SIZE);
    let mut sum: u32 = 0;
    for chunk in buf.chunks_exact(2) {
        sum = sum.wrapping_add(u16::from_be_bytes([chunk[0], chunk[1]]) as u32);
    }
    (sum & 0xFFFF) as u16 == 0x1234
}

/// True if the buffer looks like a plausible AHDI root sector. This is the
/// detection gate run from `PartitionTable::detect`; it accepts only sectors
/// without an MBR 0xAA55 signature and with at least one validly-shaped
/// primary entry whose geometry fits the disk size.
pub fn looks_like_ahdi_root(buf: &[u8; AHDI_ROOT_SIZE], disk_size_bytes: u64) -> bool {
    // An MBR boot signature means this is an MBR, not AHDI.
    if buf[510] == 0x55 && buf[511] == 0xAA {
        return false;
    }
    let disk_sectors = disk_size_bytes / 512;
    if disk_sectors < 2 {
        return false;
    }
    for i in 0..AHDI_NUM_SLOTS {
        let off = AHDI_ENTRY_OFFSET + i * AHDI_ENTRY_SIZE;
        let entry = AhdiPartitionEntry::parse(
            buf[off..off + AHDI_ENTRY_SIZE]
                .try_into()
                .expect("12-byte slice"),
            false,
        );
        if !entry.exists() || entry.sector_count == 0 {
            continue;
        }
        if !entry.kind.is_recognized_or_printable() {
            continue;
        }
        let end = (entry.start_sector as u64).saturating_add(entry.sector_count as u64);
        if end > disk_sectors {
            continue;
        }
        // Reject obvious junk: a partition starting at sector 0 would collide
        // with the root sector itself.
        if entry.start_sector == 0 {
            continue;
        }
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a synthetic AHDI root sector with up to 4 entries. Entries are
    /// `(id, start, count, flags)`. Pads bootstrap with zeros.
    fn make_ahdi_root(entries: &[(&[u8; 3], u32, u32, u8)]) -> [u8; AHDI_ROOT_SIZE] {
        let mut buf = [0u8; AHDI_ROOT_SIZE];
        for (i, (id, start, count, flags)) in entries.iter().enumerate().take(AHDI_NUM_SLOTS) {
            let off = AHDI_ENTRY_OFFSET + i * AHDI_ENTRY_SIZE;
            buf[off] = *flags;
            buf[off + 1..off + 4].copy_from_slice(*id);
            BigEndian::write_u32(&mut buf[off + 4..off + 8], *start);
            BigEndian::write_u32(&mut buf[off + 8..off + 12], *count);
        }
        // Set disk size to fit the largest end.
        let mut max_end: u32 = 0;
        for (_, start, count, _) in entries {
            max_end = max_end.max(start.saturating_add(*count));
        }
        BigEndian::write_u32(&mut buf[0x1F6..0x1FA], max_end);
        // Stamp checksum.
        let ck = ahdi_compute_checksum(&buf);
        BigEndian::write_u16(&mut buf[0x1FE..0x200], ck);
        buf
    }

    #[test]
    fn parses_single_gem_partition() {
        let root = make_ahdi_root(&[(b"GEM", 2, 1024, 0x01)]);
        let table = AhdiTable::parse_root(&root).unwrap();
        assert_eq!(table.primary[0].kind, AhdiPartitionKind::Gem);
        assert_eq!(table.primary[0].start_sector, 2);
        assert_eq!(table.primary[0].sector_count, 1024);
        assert!(table.primary[0].exists());
        assert!(table.checksum_valid);
        assert!(table.primary[1].is_empty());
    }

    #[test]
    fn parses_two_partitions_gem_bgm() {
        let root = make_ahdi_root(&[
            (b"GEM", 2, 32_768, 0x01),       // 16 MiB GEM
            (b"BGM", 32_770, 200_000, 0x01), // ~100 MiB BGM
        ]);
        let table = AhdiTable::parse_root(&root).unwrap();
        assert_eq!(table.primary[0].kind, AhdiPartitionKind::Gem);
        assert_eq!(table.primary[1].kind, AhdiPartitionKind::Bgm);
        assert_eq!(table.primary[1].size_bytes(), 200_000 * 512);
    }

    #[test]
    fn checksum_round_trip() {
        let table = AhdiTable::parse_root(&make_ahdi_root(&[(b"GEM", 2, 1024, 0x01)])).unwrap();
        let bytes = table.root_to_bytes();
        assert!(ahdi_checksum_matches(&bytes));
        let reparsed = AhdiTable::parse_root(&bytes).unwrap();
        assert_eq!(reparsed.primary[0].start_sector, 2);
        assert_eq!(reparsed.primary[0].sector_count, 1024);
        assert!(reparsed.checksum_valid);
    }

    #[test]
    fn synthetic_type_byte_routing() {
        assert_eq!(
            AhdiPartitionKind::Gem.synthetic_type_byte(),
            AHDI_TYPE_BYTE_GEM
        );
        assert_eq!(
            AhdiPartitionKind::Bgm.synthetic_type_byte(),
            AHDI_TYPE_BYTE_BGM
        );
        assert_eq!(
            AhdiPartitionKind::Xgm.synthetic_type_byte(),
            AHDI_TYPE_BYTE_XGM
        );
        assert_eq!(
            AhdiPartitionKind::Raw.synthetic_type_byte(),
            AHDI_TYPE_BYTE_RAW
        );
    }

    #[test]
    fn looks_like_ahdi_rejects_mbr() {
        let mut buf = [0u8; AHDI_ROOT_SIZE];
        buf[510] = 0x55;
        buf[511] = 0xAA;
        assert!(!looks_like_ahdi_root(&buf, 1 << 20));
    }

    #[test]
    fn looks_like_ahdi_rejects_zero_sector() {
        let buf = [0u8; AHDI_ROOT_SIZE];
        assert!(!looks_like_ahdi_root(&buf, 1 << 20));
    }

    #[test]
    fn looks_like_ahdi_rejects_out_of_bounds_entry() {
        // Entry says it starts at sector 100 with 1_000_000 sectors, but the
        // disk is only 1 MiB (2048 sectors). Should fail bounds check.
        let root = make_ahdi_root(&[(b"GEM", 100, 1_000_000, 0x01)]);
        assert!(!looks_like_ahdi_root(&root, 1 << 20));
    }

    #[test]
    fn looks_like_ahdi_accepts_valid_entry() {
        let root = make_ahdi_root(&[(b"GEM", 2, 1024, 0x01)]);
        assert!(looks_like_ahdi_root(&root, 1024 * 1024));
    }

    #[test]
    fn looks_like_ahdi_rejects_garbage_id() {
        // Non-ASCII-uppercase ID → not recognizable. Use lowercase letters,
        // which would never appear in a real AHDI table.
        let root = make_ahdi_root(&[(b"abc", 2, 1024, 0x01)]);
        assert!(!looks_like_ahdi_root(&root, 1 << 20));
    }

    #[test]
    fn walks_xgm_extended_chain() {
        // Disk layout (4 KiB sectors of zero except where written):
        //   - Sector 0: root with primary GEM @ sector 2 (size 1000),
        //               and XGM container @ sector 1100 (size 5000)
        //   - Sector 1100: XGM follow-up with logical BGM @ rel 2 (size 2000),
        //                  and XGM link @ rel 2010 → absolute sector 3110
        //   - Sector 3110: XGM follow-up with logical GEM @ rel 2 (size 100),
        //                  no further link
        let total_sectors: u64 = 4000;
        let mut disk = vec![0u8; total_sectors as usize * 512];

        // Build root sector at 0.
        let root = make_ahdi_root(&[(b"GEM", 2, 1000, 0x01), (b"XGM", 1100, 5000, 0x01)]);
        disk[..AHDI_ROOT_SIZE].copy_from_slice(&root);

        // Build first XGM follow-up at sector 1100.
        let xgm1_off = 1100usize * 512;
        // Slot 0: logical BGM at rel 2 (= abs 1102), size 2000.
        let s0 = AHDI_ENTRY_OFFSET;
        disk[xgm1_off + s0] = 0x01;
        disk[xgm1_off + s0 + 1..xgm1_off + s0 + 4].copy_from_slice(b"BGM");
        BigEndian::write_u32(&mut disk[xgm1_off + s0 + 4..xgm1_off + s0 + 8], 2);
        BigEndian::write_u32(&mut disk[xgm1_off + s0 + 8..xgm1_off + s0 + 12], 2000);
        // Slot 1: XGM link at rel 2010 (= abs 3110).
        let s1 = AHDI_ENTRY_OFFSET + AHDI_ENTRY_SIZE;
        disk[xgm1_off + s1] = 0x01;
        disk[xgm1_off + s1 + 1..xgm1_off + s1 + 4].copy_from_slice(b"XGM");
        BigEndian::write_u32(&mut disk[xgm1_off + s1 + 4..xgm1_off + s1 + 8], 2010);
        BigEndian::write_u32(&mut disk[xgm1_off + s1 + 8..xgm1_off + s1 + 12], 5000);

        // Second XGM follow-up at sector 3110.
        let xgm2_off = 3110usize * 512;
        disk[xgm2_off + s0] = 0x01;
        disk[xgm2_off + s0 + 1..xgm2_off + s0 + 4].copy_from_slice(b"GEM");
        BigEndian::write_u32(&mut disk[xgm2_off + s0 + 4..xgm2_off + s0 + 8], 2);
        BigEndian::write_u32(&mut disk[xgm2_off + s0 + 8..xgm2_off + s0 + 12], 100);
        // Slot 1: empty (end of chain).

        let mut cur = Cursor::new(disk);
        let table = AhdiTable::detect_and_walk(&mut cur, total_sectors * 512).unwrap();

        assert_eq!(table.primary[0].kind, AhdiPartitionKind::Gem);
        assert_eq!(table.primary[1].kind, AhdiPartitionKind::Xgm);
        assert_eq!(table.logical.len(), 2);
        assert_eq!(table.logical[0].kind, AhdiPartitionKind::Bgm);
        assert_eq!(table.logical[0].start_sector, 1102);
        assert_eq!(table.logical[0].sector_count, 2000);
        assert_eq!(table.logical[1].kind, AhdiPartitionKind::Gem);
        assert_eq!(table.logical[1].start_sector, 3112);
    }

    #[test]
    fn xgm_chain_cycle_terminates() {
        // Build a degenerate XGM chain whose link points back to itself.
        let total_sectors: u64 = 4000;
        let mut disk = vec![0u8; total_sectors as usize * 512];

        let root = make_ahdi_root(&[(b"XGM", 1100, 2000, 0x01)]);
        disk[..AHDI_ROOT_SIZE].copy_from_slice(&root);

        // Self-referential XGM link at sector 1100. Without a cycle guard
        // this would loop forever; the test asserts detect_and_walk returns.
        let xgm1_off = 1100usize * 512;
        let s0 = AHDI_ENTRY_OFFSET;
        let s1 = AHDI_ENTRY_OFFSET + AHDI_ENTRY_SIZE;
        // Slot 0: logical entry (placeholder to satisfy "exists" check).
        disk[xgm1_off + s0] = 0x01;
        disk[xgm1_off + s0 + 1..xgm1_off + s0 + 4].copy_from_slice(b"GEM");
        BigEndian::write_u32(&mut disk[xgm1_off + s0 + 4..xgm1_off + s0 + 8], 2);
        BigEndian::write_u32(&mut disk[xgm1_off + s0 + 8..xgm1_off + s0 + 12], 100);
        // Slot 1: XGM link at rel 0 → absolute 1100 (self).
        disk[xgm1_off + s1] = 0x01;
        disk[xgm1_off + s1 + 1..xgm1_off + s1 + 4].copy_from_slice(b"XGM");
        BigEndian::write_u32(&mut disk[xgm1_off + s1 + 4..xgm1_off + s1 + 8], 0);
        BigEndian::write_u32(&mut disk[xgm1_off + s1 + 8..xgm1_off + s1 + 12], 5000);

        // Primary must validate before XGM walk runs — set disk size sane.
        let mut cur = Cursor::new(disk);
        // Should terminate without hanging.
        let table = AhdiTable::detect_and_walk(&mut cur, total_sectors * 512).unwrap();
        assert_eq!(table.logical.len(), 1);
    }
}
