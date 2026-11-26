//! Record type codes and the FAT directory-entry records.

use super::*;

// ============================================================================
// 5.6 — Record-stream walker + block-stream decoder + decode-to-temp
// ============================================================================
//
// **Record taxonomy used by our fixture corpus** (verified against both
// Ghost 7.5 and 11.5 file-aware backups, 2026-05-26):
//
// | type   | body_len            | meaning                              |
// |--------|---------------------|--------------------------------------|
// | 0x0017 | 512                 | boot sector (start of a partition)   |
// | 0x0004 | 56                  | FAT entry / cluster-chain record     |
// | 0x0102 | varies (125, 720…)  | directory header                     |
// | 0x0103 | 20                  | directory descriptor                 |
// | 0x0104 | 56                  | file extent                          |
// | 0x0002 | block size (≤32768) | **data block** (1 chunk of cluster   |
// |        |                     | bytes; compressed per container.cmp) |
// | 0x0017 |                     | next partition's boot sector         |
//
// Decoding the partition image = concat the bodies of every `0x0002`
// record across the inner record stream, decompressing each per the
// container's compression byte:
//   - `0x00` (None) → body IS 32 KiB of raw cluster bytes
//   - `0x02` (Fast) → body is a Fast-LZ block (4-byte prefix + LZ token stream)
//   - `0x03` (High) → body is a zlib block (4-byte prefix + deflate stream)
//
// **Important:** the Go reference `nyarime/gho` describes a different
// hierarchy (Track0 0x0006 + Partition 0x0603 + Continuation 0x0703 +
// End 0x0023 + per-span `[u16 stored_len][data]` block frames). That
// taxonomy does **not** appear in any fixture in our corpus — neither
// 7.5 nor 11.5. The dialect documented here matches every fixture we
// have (PART.GHO, gh11part.GHO, High.GHO, FULLDISK.GHO, fulldisk.GHS,
// HPVectra95C.gho). 5.6's walker targets this dialect.
//
// Multi-partition (multiple `0x0017` boot-sector markers in one file)
// support is partial — we just concatenate every `0x0002` block we see,
// which yields the partitions stacked back-to-back rather than placed
// at MBR-declared LBAs. Full-disk reconstruction with MBR + LBA-positioned
// partitions is the next slice.

/// Inner record type carrying one data block (compressed per the
/// container's compression byte). Body_len is the on-disk block size.
pub const GHO_REC_DATA_BLOCK: u16 = 0x0002;

/// Boot-sector record on Ghost 7.5 (body = 512 bytes). Marks the start
/// of a partition.
pub const GHO_REC_BOOT_SECTOR: u16 = 0x0017;

/// Boot-sector record on Ghost 11.5 (body = 512 bytes). Same body shape
/// as [`GHO_REC_BOOT_SECTOR`]; the high bits seem to encode "header
/// section" framing — records in this section carry marker `0xC01E`.
pub const GHO_REC_BOOT_SECTOR_V2: u16 = 0x0717;

/// Boot-sector record on Ghost 7.5 **full-disk** mode (body = 512
/// bytes). Same body shape as [`GHO_REC_BOOT_SECTOR`]; the `0xae` high
/// byte tags the disk-level header section. Observed as the very first
/// record in `FULLDISK.GHO`, `HPVectra95C.gho`,
/// `fromdanilaptop.GHO`, and `XP_SP2FU.GHO`.
pub const GHO_REC_BOOT_SECTOR_FULLDISK: u16 = 0xae17;

/// Subsequent boot-sector record observed in `FULLDISK.GHO` after the
/// disk-level header section ends. Bytes are still a verbatim 512-byte
/// FAT BPB. Likely the partition boot sector (vs `0xae17` which carries
/// the same content as a disk-level header).
pub const GHO_REC_BOOT_SECTOR_PARTITION: u16 = 0x0117;

/// Root-directory entry record on Ghost 7.5 (body = 56 bytes). Each
/// record holds one 32-byte FAT directory entry (LFN segment, 8.3
/// entry, dot/dotdot, or empty slot) followed by a 24-byte trailer
/// (4-byte hash + 20 zero bytes). The root dir's entries are stored
/// as a contiguous run of these.
pub const GHO_REC_DIR_ENTRY_ROOT: u16 = 0x0004;

/// Root-directory entry record on Ghost 11.5 (body = 56 bytes). Same
/// 56-byte payload as [`GHO_REC_DIR_ENTRY_ROOT`]; appears once at the
/// start of the inner stream on 11.5 fixtures (instead of the
/// multi-entry run we see on 7.5).
pub const GHO_REC_DIR_ENTRY_ROOT_V2: u16 = 0x0704;

/// Subdirectory entry record (body = 56 bytes). Same on-disk layout as
/// [`GHO_REC_DIR_ENTRY_ROOT`]. Used for every directory below the root
/// on both Ghost 7.5 and 11.5.
pub const GHO_REC_DIR_ENTRY_SUB: u16 = 0x0104;

/// Directory-entry record in Ghost 7.5 **full-disk** mode, disk-level
/// header section (body = 56 bytes). Same body shape as
/// [`GHO_REC_DIR_ENTRY_ROOT`]; `0xae` high byte tags the header
/// section. Observed at the start of `FULLDISK.GHO` directly after the
/// `0xae17` boot sector.
pub const GHO_REC_DIR_ENTRY_FULLDISK: u16 = 0xae04;

/// File-content record (body = variable length). Carries either the
/// entire content of a small file (≤ 32 KiB) or the trailing fragment
/// of a larger file (`file_size mod 32768` bytes after N `0x0002`
/// blocks).
pub const GHO_REC_FILE_TAIL: u16 = 0x0102;

/// Per-file checksum record (body = 20 bytes). Layout
/// `[u32 cksum][u32 cksum_dup][12 zero bytes]` — the same 32-bit
/// value stored twice for integrity.
pub const GHO_REC_FILE_CHECKSUM: u16 = 0x0103;

/// Reserved-sectors blob for a FAT32 partition. Body = VBR + FSInfo +
/// backup VBR + padding. `body_len == reserved_sectors × bytes_per_sector`.
pub const GHO_REC_RESERVED_SECTORS: u16 = 0x0118;

/// Zlib-compressed per-partition file catalog. One record per partition
/// at the end of a multi-partition file-aware backup.
pub const GHO_REC_FILE_CATALOG: u16 = 0x0005;

/// End-of-image marker (body = 24 bytes). Last record in both
/// file-aware and sector-mode backups.
pub const GHO_REC_END: u16 = 0x0023;

/// Sector-mode continuation / partition-boundary marker (body = 20
/// bytes: `[u32 checksum][u32 checksum_dup][12 zero bytes]`). Separates
/// per-partition block streams in multi-partition SECTOR-mode backups.
pub const GHO_REC_CONTINUATION: u16 = 0x0703;

/// Returns true if `type_code` is a boot-sector marker.
///
/// Both Ghost 7.5 (`0x0017`) and 11.5 (`0x0717`) use a 512-byte body
/// that's a verbatim copy of the partition's first sector.
pub fn is_boot_sector_record(type_code: u16) -> bool {
    matches!(
        type_code,
        GHO_REC_BOOT_SECTOR
            | GHO_REC_BOOT_SECTOR_V2
            | GHO_REC_BOOT_SECTOR_FULLDISK
            | GHO_REC_BOOT_SECTOR_PARTITION
    )
}

/// Returns true if `type_code` carries one 32-byte FAT directory entry
/// (root or subdirectory variant, 7.5 or 11.5). All variants share the
/// same 56-byte body layout — see [`parse_fat_dir_entry_body`].
pub fn is_dir_entry_record(type_code: u16) -> bool {
    matches!(
        type_code,
        GHO_REC_DIR_ENTRY_ROOT
            | GHO_REC_DIR_ENTRY_ROOT_V2
            | GHO_REC_DIR_ENTRY_SUB
            | GHO_REC_DIR_ENTRY_FULLDISK
    )
}

/// Returns true if `type_code` carries cluster / file content bytes
/// (full 32 KiB block for `0x0002`, variable-length tail for `0x0102`).
pub fn is_data_block_record(type_code: u16) -> bool {
    type_code == GHO_REC_DATA_BLOCK || type_code == GHO_REC_FILE_TAIL
}

/// Returns true if `type_code` is the per-file checksum record.
pub fn is_checksum_record(type_code: u16) -> bool {
    type_code == GHO_REC_FILE_CHECKSUM
}

/// One parsed directory-entry record (`0x0004` / `0x0104` / `0x0704`).
///
/// The first 32 bytes are the verbatim FAT directory entry (LFN
/// segment, 8.3 entry, dot/dotdot, or empty slot). Whether it's an
/// LFN slot or an 8.3 entry is determined by the attribute byte at
/// offset 11 (`0x0F == LFN slot`). The 24-byte trailer carries a
/// per-entry 32-bit hash followed by 20 reserved bytes (all zero in
/// every fixture observed).
#[derive(Debug, Clone, Copy)]
pub struct GhoDirEntryRecord {
    /// The 32-byte FAT directory entry, ready to be written verbatim
    /// into a rebuilt directory cluster.
    pub fat_entry: [u8; 32],
    /// 32-bit per-entry hash (purpose not yet reverse-engineered;
    /// possibly CRC-32 of the entry). Preserved for diagnostics +
    /// future integrity checks.
    pub entry_hash: u32,
}

impl GhoDirEntryRecord {
    /// FAT attribute byte at offset 11.
    pub fn attr_byte(&self) -> u8 {
        self.fat_entry[11]
    }

    /// True when the entry is a long-filename slot (`attr == 0x0F`).
    pub fn is_lfn_slot(&self) -> bool {
        self.attr_byte() == 0x0F
    }

    /// True when the entry is a directory (`attr & 0x10`). Not valid
    /// for LFN slots.
    pub fn is_directory(&self) -> bool {
        !self.is_lfn_slot() && (self.attr_byte() & 0x10) != 0
    }

    /// True when the entry is a volume label (`attr & 0x08`). Not
    /// valid for LFN slots.
    pub fn is_volume_label(&self) -> bool {
        !self.is_lfn_slot() && (self.attr_byte() & 0x08) != 0
    }

    /// True when the entry is empty (first byte 0x00) — FAT directory
    /// "end of entries" sentinel.
    pub fn is_empty_slot(&self) -> bool {
        self.fat_entry[0] == 0x00
    }

    /// True when the entry is deleted (first byte 0xE5).
    pub fn is_deleted(&self) -> bool {
        self.fat_entry[0] == 0xE5
    }

    /// 8.3 entry's 32-bit starting cluster (high 16 + low 16 = full
    /// 32-bit cluster for FAT32; high is 0 for FAT12/16). Returns 0
    /// for LFN / empty / deleted slots.
    pub fn first_cluster(&self) -> u32 {
        if self.is_lfn_slot() || self.is_empty_slot() || self.is_deleted() {
            return 0;
        }
        let lo = u16::from_le_bytes([self.fat_entry[26], self.fat_entry[27]]) as u32;
        let hi = u16::from_le_bytes([self.fat_entry[20], self.fat_entry[21]]) as u32;
        (hi << 16) | lo
    }

    /// 8.3 entry's 32-bit file size in bytes. Returns 0 for LFN /
    /// empty / deleted slots and for directories.
    pub fn file_size(&self) -> u32 {
        if self.is_lfn_slot() || self.is_empty_slot() || self.is_deleted() || self.is_directory() {
            return 0;
        }
        u32::from_le_bytes([
            self.fat_entry[28],
            self.fat_entry[29],
            self.fat_entry[30],
            self.fat_entry[31],
        ])
    }
}

/// Parse a `0x0004` / `0x0104` / `0x0704` directory-entry record body.
///
/// `body.len()` must be at least 36 (32 FAT bytes + 4 hash bytes); the
/// remaining bytes are silently ignored (every fixture has the 24-byte
/// trailer with hash at offset 32, but the reserved tail isn't
/// required by the parser).
pub fn parse_fat_dir_entry_body(body: &[u8]) -> Result<GhoDirEntryRecord> {
    if body.len() < 36 {
        bail!(
            "GHO dir-entry record body must be >= 36 bytes, got {}",
            body.len()
        );
    }
    let mut fat_entry = [0u8; 32];
    fat_entry.copy_from_slice(&body[..32]);
    let entry_hash = u32::from_le_bytes([body[32], body[33], body[34], body[35]]);
    Ok(GhoDirEntryRecord {
        fat_entry,
        entry_hash,
    })
}
