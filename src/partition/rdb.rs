//! Rigid Disk Block (RDB) — Amiga hard-disk partition table.
//!
//! The RDB lives in one of the first 16 sectors of the disk (sector 0 in
//! practice). All multi-byte fields are big-endian; the table is built from
//! 512-byte blocks (per `rdb.block_size`, which is always 512 in practice).
//! Each block is checksummed by treating the block as an array of i32s — the
//! sum, including the stored checksum, must equal zero (mod 2^32).
//!
//! Partition (`PART`) blocks form a singly-linked list whose head is in the
//! RDSK's `part_list` field; the list terminates when `next == 0xFFFFFFFF`.
//! Each PART carries a DosEnvVec that includes the geometry needed to compute
//! the partition's byte offset and the 4-byte `dos_type` tag (e.g. `DOS\0`,
//! `PFS\3`, `SFS\0`) that selects the filesystem.

use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};

use crate::error::RustyBackupError;
use crate::partition::PartitionSizeOverride;

pub const RDSK_SIGNATURE: &[u8; 4] = b"RDSK";
pub const PART_SIGNATURE: &[u8; 4] = b"PART";
pub const RDB_SCAN_BLOCKS: u64 = 16;
pub const NO_BLOCK: u32 = 0xFFFFFFFF;

/// PART block flag bits.
pub const PART_FLAG_BOOTABLE: u32 = 1;
pub const PART_FLAG_NO_AUTOMOUNT: u32 = 2;

/// Parsed RDB Rigid Disk Block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdbHeader {
    /// Block number (in 512-byte sectors) where the RDSK was found.
    pub block_num: u64,
    /// Declared size in longs.
    pub size_longs: u32,
    pub host_id: u32,
    /// Hardware block size in bytes (always 512 in practice).
    pub block_size: u32,
    pub flags: u32,
    pub badblk_list: u32,
    pub part_list: u32,
    pub fs_list: u32,
    pub init_code: u32,
    pub cylinders: u32,
    pub sectors: u32,
    pub heads: u32,
    pub rdb_blk_lo: u32,
    pub rdb_blk_hi: u32,
    pub lo_cyl: u32,
    pub hi_cyl: u32,
    pub cyl_blks: u32,
    pub disk_vendor: String,
    pub disk_product: String,
    pub disk_revision: String,
}

/// Parsed RDB `PART` block + its DosEnv.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdbPartition {
    /// Block number (in 512-byte sectors) where this PART was read.
    pub block_num: u64,
    pub size_longs: u32,
    pub host_id: u32,
    pub next: u32,
    pub flags: u32,
    pub dev_flags: u32,
    /// AmigaDOS device name, e.g. "DH0".
    pub drv_name: String,

    // DosEnv fields ------------------------------------------------------
    pub env_size: u32,
    /// Block size in **longs** (so * 4 = bytes). Typically 128 = 512 bytes.
    pub env_block_size_longs: u32,
    pub sec_org: u32,
    pub surfaces: u32,
    pub sec_per_blk: u32,
    pub blk_per_trk: u32,
    pub reserved: u32,
    pub pre_alloc: u32,
    pub interleave: u32,
    pub low_cyl: u32,
    pub high_cyl: u32,
    pub num_buffer: u32,
    pub buf_mem_type: u32,
    pub max_transfer: u32,
    pub mask: u32,
    pub boot_pri: i32,
    /// 4-byte DosType tag (e.g. `DOS\0`, `PFS\3`, `SFS\0`).
    pub dos_type: u32,
    pub baud: u32,
    pub control: u32,
    pub boot_blocks: u32,
}

impl RdbPartition {
    /// FS block size in bytes (DosEnv stores it in longs, so * 4).
    pub fn fs_block_size(&self) -> u64 {
        self.env_block_size_longs as u64 * 4
    }

    /// Byte offset of this partition from the start of the disk.
    pub fn start_byte_offset(&self) -> u64 {
        self.low_cyl as u64 * self.surfaces as u64 * self.blk_per_trk as u64 * self.fs_block_size()
    }

    /// Partition size in bytes.
    pub fn size_bytes(&self) -> u64 {
        let cyls = (self.high_cyl as u64).saturating_sub(self.low_cyl as u64) + 1;
        cyls * self.surfaces as u64 * self.blk_per_trk as u64 * self.fs_block_size()
    }

    /// Partition start LBA in 512-byte sectors (the rest of rusty-backup
    /// works in 512-byte LBAs regardless of the FS block size).
    pub fn start_lba_512(&self) -> u64 {
        self.start_byte_offset() / 512
    }

    /// Bootable flag (bit 0 of `flags`).
    pub fn is_bootable(&self) -> bool {
        self.flags & PART_FLAG_BOOTABLE != 0
    }

    /// DosType tag as a 4-byte string with non-printable bytes shown as
    /// backslash-escapes (e.g. `DOS\0`, `PFS\3`, `SFS\0`). Matches the
    /// convention used in `partition_type_string` for routing.
    pub fn dos_type_string(&self) -> String {
        format_dos_type(self.dos_type)
    }
}

/// Top-level RDB partition table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rdb {
    pub header: RdbHeader,
    pub partitions: Vec<RdbPartition>,
}

impl Rdb {
    /// Scan the first 16 sectors for an `RDSK` signature, parse it, and walk
    /// the PART list. Returns `Err(InvalidRdb)` if no RDSK is found or it
    /// fails the checksum.
    pub fn parse(reader: &mut (impl Read + Seek)) -> Result<Self, RustyBackupError> {
        let (rdsk_block_num, rdsk_buf) = find_rdsk(reader)?;
        let header = parse_rdsk(&rdsk_buf, rdsk_block_num)?;

        let mut partitions = Vec::new();
        let mut next = header.part_list;
        let mut seen = std::collections::HashSet::new();
        while next != NO_BLOCK && next != 0 {
            if !seen.insert(next) {
                return Err(RustyBackupError::InvalidRdb(format!(
                    "PART list contains a cycle at block {next}"
                )));
            }
            if partitions.len() >= 128 {
                return Err(RustyBackupError::InvalidRdb(
                    "PART list exceeds 128 entries".to_string(),
                ));
            }
            let offset = next as u64 * 512;
            reader.seek(SeekFrom::Start(offset))?;
            let mut buf = [0u8; 512];
            reader.read_exact(&mut buf).map_err(|e| {
                RustyBackupError::InvalidRdb(format!("cannot read PART at block {next}: {e}"))
            })?;
            let part = parse_part(&buf, next as u64)?;
            next = part.next;
            partitions.push(part);
        }

        Ok(Rdb { header, partitions })
    }
}

/// Result of `Rdb::patch_for_restore`. Carries the patched RDSK + each
/// modified PART block as raw 512-byte buffers (already checksummed and
/// ready to write at the recorded `block_num`), plus the new total disk
/// size in bytes derived from the highest partition end after patching.
///
/// Unmodified RDB-region blocks (FSHD/LSEG driver chain, bad-block list,
/// etc.) are NOT re-emitted here — the caller is expected to copy the
/// first `RDB_SCAN_BLOCKS` sectors verbatim from the source and then
/// overlay these patched blocks on top, so anything we don't understand
/// survives the round-trip byte-for-byte.
pub struct RdbRestorePlan {
    /// `(block_num, 512-byte buffer)` for the patched RDSK header.
    pub rdsk_block: (u64, [u8; 512]),
    /// `(block_num, 512-byte buffer)` for each patched PART entry,
    /// in the same order as `Rdb::partitions`.
    pub part_blocks: Vec<(u64, [u8; 512])>,
    /// Per-partition copy plan in source-order: (override-resolved
    /// source/dest LBAs, copy size from source, requested export size,
    /// dos-type string, drv_name). Used by the reconstruct path so it
    /// doesn't need to re-derive geometry from the patched blocks.
    pub partition_plans: Vec<RdbPartitionPlan>,
    /// New total disk size in bytes = max(partition end LBA × 512).
    /// At least `RDB_SCAN_BLOCKS * 512` so the RDB region fits.
    pub new_disk_size_bytes: u64,
}

/// Per-partition copy plan emitted by `Rdb::patch_for_restore`.
#[derive(Debug, Clone)]
pub struct RdbPartitionPlan {
    pub source_lba: u64,
    pub dest_lba: u64,
    pub copy_size: u64,
    pub export_size: u64,
    pub dos_type_string: String,
    pub drv_name: String,
}

impl Rdb {
    /// Patch the RDB for restore with the given partition-size overrides.
    ///
    /// Match overrides to partitions by their source `start_lba_512()`.
    /// When an override is present:
    /// - Use `effective_start_lba()` for the partition's new start position
    ///   (falls back to original if `new_start_lba` is unset).
    /// - Recompute `low_cyl` / `high_cyl` from the new start LBA and
    ///   export size, using the partition's existing `(surfaces,
    ///   blk_per_trk, fs_block_size)` geometry. Resize must preserve
    ///   `surfaces` / `blk_per_trk` — they define how many 512-byte LBAs
    ///   live in one Amiga cylinder, and any FS / driver that consults
    ///   them across the resize would be confused otherwise.
    ///
    /// When no override matches, the partition keeps its original
    /// geometry verbatim — including a potential gap if a preceding
    /// partition shrank. (Sliding subsequent partitions down is left to
    /// the caller via `new_start_lba`.)
    ///
    /// The RDSK header's `cylinders` / `hi_cyl` are recomputed from the
    /// highest patched partition end so a shrunk last partition produces
    /// a smaller output disk.
    ///
    /// Re-stamps all 32-bit checksums.
    pub fn patch_for_restore(
        &self,
        overrides: &[PartitionSizeOverride],
        source: &mut (impl Read + Seek),
    ) -> Result<RdbRestorePlan, RustyBackupError> {
        // --- Re-read raw RDSK + PART blocks from source ---
        let mut rdsk_buf = [0u8; 512];
        source.seek(SeekFrom::Start(self.header.block_num * 512))?;
        source.read_exact(&mut rdsk_buf)?;
        if &rdsk_buf[0..4] != RDSK_SIGNATURE {
            return Err(RustyBackupError::InvalidRdb(format!(
                "expected RDSK signature at block {}",
                self.header.block_num
            )));
        }

        let mut part_bufs: Vec<(u64, [u8; 512])> = Vec::with_capacity(self.partitions.len());
        for p in &self.partitions {
            let mut buf = [0u8; 512];
            source.seek(SeekFrom::Start(p.block_num * 512))?;
            source.read_exact(&mut buf)?;
            if &buf[0..4] != PART_SIGNATURE {
                return Err(RustyBackupError::InvalidRdb(format!(
                    "expected PART signature at block {}",
                    p.block_num
                )));
            }
            part_bufs.push((p.block_num, buf));
        }

        // --- Per-partition patch + plan ---
        let mut plans: Vec<RdbPartitionPlan> = Vec::with_capacity(self.partitions.len());
        let mut max_end_lba: u64 = (RDB_SCAN_BLOCKS).max(self.header.block_num + 1);
        for (i, p) in self.partitions.iter().enumerate() {
            let source_lba = p.start_lba_512();
            let original_size = p.size_bytes();
            let (dest_lba, export_size) =
                if let Some(ov) = overrides.iter().find(|o| o.start_lba == source_lba) {
                    (ov.effective_start_lba(), ov.export_size)
                } else {
                    (source_lba, original_size)
                };

            // Compute new low_cyl / high_cyl from dest_lba + export_size,
            // preserving (surfaces, blk_per_trk, fs_block_size).
            let cyl_lbas = p.surfaces as u64 * p.blk_per_trk as u64 * p.fs_block_size() / 512;
            if cyl_lbas == 0 {
                return Err(RustyBackupError::InvalidRdb(format!(
                    "partition {} has zero cyl_lbas (surfaces={} blk_per_trk={} fs_block_size={})",
                    i,
                    p.surfaces,
                    p.blk_per_trk,
                    p.fs_block_size()
                )));
            }
            if dest_lba % cyl_lbas != 0 {
                return Err(RustyBackupError::InvalidRdb(format!(
                    "partition {} dest LBA {} not cylinder-aligned (cyl_lbas={})",
                    i, dest_lba, cyl_lbas
                )));
            }
            let new_low_cyl = (dest_lba / cyl_lbas) as u32;
            // Round the requested export size UP to the next cylinder boundary.
            // RDB partition geometry is cylinder-granular (low_cyl/high_cyl are
            // u32 cylinder counts), so an MB-aligned size from the GUI almost
            // never lands exactly on a cyl boundary on real Amiga disks where
            // cyl_lbas can be e.g. 1008. Rounding up keeps the partition at
            // least the requested size — for shrink this still produces a
            // smaller output disk, for grow it slightly exceeds the request.
            let requested_lbas = export_size.div_ceil(512);
            if requested_lbas == 0 {
                return Err(RustyBackupError::InvalidRdb(format!(
                    "partition {} export_size {} resolves to zero LBAs",
                    i, export_size
                )));
            }
            let new_cyls_u64 = requested_lbas.div_ceil(cyl_lbas);
            let new_cyls = u32::try_from(new_cyls_u64).map_err(|_| {
                RustyBackupError::InvalidRdb(format!(
                    "partition {} new cyl count {} overflows u32",
                    i, new_cyls_u64
                ))
            })?;
            let aligned_export_lbas = new_cyls_u64 * cyl_lbas;
            let aligned_export_size = aligned_export_lbas * 512;
            let new_high_cyl = new_low_cyl + new_cyls - 1;

            // Patch in place.
            let (_, buf) = &mut part_bufs[i];
            BigEndian::write_u32(&mut buf[41 * 4..41 * 4 + 4], new_low_cyl);
            BigEndian::write_u32(&mut buf[42 * 4..42 * 4 + 4], new_high_cyl);
            restamp_checksum(buf);

            plans.push(RdbPartitionPlan {
                source_lba,
                dest_lba,
                copy_size: original_size.min(aligned_export_size),
                // Surface the rounded size so resize_filesystem_for and the
                // copy/zero-fill loop work in sync with what's recorded in
                // the patched PART block.
                export_size: aligned_export_size,
                dos_type_string: p.dos_type_string(),
                drv_name: p.drv_name.clone(),
            });

            let end_lba = dest_lba + aligned_export_lbas;
            if end_lba > max_end_lba {
                max_end_lba = end_lba;
            }
        }

        // --- Patch RDSK header: cylinders + hi_cyl reflect new disk size ---
        let cyl_blks = self.header.cyl_blks as u64;
        if cyl_blks == 0 {
            return Err(RustyBackupError::InvalidRdb(
                "RDSK cyl_blks = 0".to_string(),
            ));
        }
        // Round max_end_lba up to a cylinder boundary so cylinders is a
        // whole number that covers all partitions.
        let cyl_lbas_disk = cyl_blks; // RDSK cyl_blks is already in 512-byte LBAs.
        let new_total_lbas = max_end_lba.div_ceil(cyl_lbas_disk) * cyl_lbas_disk;
        let new_cylinders = (new_total_lbas / cyl_lbas_disk) as u32;
        let new_hi_cyl = new_cylinders - 1;
        BigEndian::write_u32(&mut rdsk_buf[16 * 4..16 * 4 + 4], new_cylinders);
        BigEndian::write_u32(&mut rdsk_buf[35 * 4..35 * 4 + 4], new_hi_cyl);
        restamp_checksum(&mut rdsk_buf);

        Ok(RdbRestorePlan {
            rdsk_block: (self.header.block_num, rdsk_buf),
            part_blocks: part_bufs,
            partition_plans: plans,
            new_disk_size_bytes: new_total_lbas * 512,
        })
    }
}

/// Zero the checksum slot at long 2, sum all longs, write the
/// 2's-complement negation back into long 2 so the full block sums to 0.
fn restamp_checksum(buf: &mut [u8; 512]) {
    BigEndian::write_u32(&mut buf[2 * 4..2 * 4 + 4], 0);
    let mut sum: i32 = 0;
    for i in 0..128 {
        sum = sum.wrapping_add(BigEndian::read_i32(&buf[i * 4..i * 4 + 4]));
    }
    BigEndian::write_i32(&mut buf[2 * 4..2 * 4 + 4], sum.wrapping_neg());
}

/// Toggle the bootable flag on the PART block at `block_num` (in 512-byte
/// sectors). Reads the existing block, flips bit 0 of `flags`, recomputes the
/// 32-bit block checksum, and writes the block back in place. Returns the new
/// bootable state.
///
/// The PART block is the only block touched — other RDB structures are
/// untouched. The Amiga ROM picks the actual boot partition by sorting
/// bootable-flag-set partitions by `boot_pri` (highest wins), so flipping the
/// bootable bit on multiple partitions is legal — only one will boot at a
/// time but the user can change priority later.
pub fn set_partition_bootable<W: Read + std::io::Write + Seek>(
    rw: &mut W,
    part_block_num: u64,
    bootable: bool,
) -> Result<bool, RustyBackupError> {
    let mut buf = [0u8; 512];
    rw.seek(SeekFrom::Start(part_block_num * 512))?;
    rw.read_exact(&mut buf)?;
    if &buf[0..4] != PART_SIGNATURE {
        return Err(RustyBackupError::InvalidRdb(format!(
            "expected PART signature at block {part_block_num}"
        )));
    }
    verify_checksum(&buf, "PART")?;

    let flags = get_long(&buf, 5);
    let new_flags = if bootable {
        flags | PART_FLAG_BOOTABLE
    } else {
        flags & !PART_FLAG_BOOTABLE
    };
    BigEndian::write_u32(&mut buf[5 * 4..5 * 4 + 4], new_flags);

    // Recompute checksum: zero the checksum word, sum the rest, store the
    // 2's-complement negation. The PART (and RDSK) checksum lives at long 2.
    BigEndian::write_u32(&mut buf[2 * 4..2 * 4 + 4], 0);
    let mut sum: i32 = 0;
    for i in 0..128 {
        sum = sum.wrapping_add(BigEndian::read_i32(&buf[i * 4..i * 4 + 4]));
    }
    BigEndian::write_i32(&mut buf[2 * 4..2 * 4 + 4], sum.wrapping_neg());

    rw.seek(SeekFrom::Start(part_block_num * 512))?;
    rw.write_all(&buf)?;
    rw.flush()?;

    Ok(new_flags & PART_FLAG_BOOTABLE != 0)
}

fn find_rdsk(reader: &mut (impl Read + Seek)) -> Result<(u64, [u8; 512]), RustyBackupError> {
    let mut buf = [0u8; 512];
    for block in 0..RDB_SCAN_BLOCKS {
        reader.seek(SeekFrom::Start(block * 512))?;
        if reader.read_exact(&mut buf).is_err() {
            continue;
        }
        if &buf[0..4] == RDSK_SIGNATURE {
            return Ok((block, buf));
        }
    }
    Err(RustyBackupError::InvalidRdb(
        "no RDSK signature in first 16 sectors".to_string(),
    ))
}

fn verify_checksum(buf: &[u8; 512], kind: &str) -> Result<(), RustyBackupError> {
    let mut sum: i32 = 0;
    for i in 0..128 {
        sum = sum.wrapping_add(BigEndian::read_i32(&buf[i * 4..i * 4 + 4]));
    }
    if sum != 0 {
        return Err(RustyBackupError::InvalidRdb(format!(
            "{kind} checksum mismatch (sum = 0x{:08x})",
            sum as u32
        )));
    }
    Ok(())
}

fn get_long(buf: &[u8; 512], long_idx: usize) -> u32 {
    BigEndian::read_u32(&buf[long_idx * 4..long_idx * 4 + 4])
}

fn get_slong(buf: &[u8; 512], long_idx: usize) -> i32 {
    BigEndian::read_i32(&buf[long_idx * 4..long_idx * 4 + 4])
}

/// Read a BSTR (Pascal-style: 1 length byte + chars) starting at `long_idx`.
/// `max_chars` is the maximum number of character bytes that follow.
fn get_bstr(buf: &[u8; 512], long_idx: usize, max_chars: usize) -> String {
    let start = long_idx * 4;
    if start >= buf.len() {
        return String::new();
    }
    let len = buf[start] as usize;
    let len = len.min(max_chars).min(buf.len() - start - 1);
    String::from_utf8_lossy(&buf[start + 1..start + 1 + len]).into_owned()
}

/// Read a fixed-length string field starting at `long_idx` covering
/// `num_longs` longs. Trims trailing NULs and spaces.
fn get_cstr(buf: &[u8; 512], long_idx: usize, num_longs: usize) -> String {
    let start = long_idx * 4;
    let end = (start + num_longs * 4).min(buf.len());
    let slice = &buf[start..end];
    let trimmed_end = slice
        .iter()
        .rposition(|&b| b != 0 && b != b' ')
        .map(|p| p + 1)
        .unwrap_or(0);
    String::from_utf8_lossy(&slice[..trimmed_end]).into_owned()
}

fn parse_rdsk(buf: &[u8; 512], block_num: u64) -> Result<RdbHeader, RustyBackupError> {
    if &buf[0..4] != RDSK_SIGNATURE {
        return Err(RustyBackupError::InvalidRdb(
            "bad RDSK signature".to_string(),
        ));
    }
    verify_checksum(buf, "RDSK")?;
    Ok(RdbHeader {
        block_num,
        size_longs: get_long(buf, 1),
        host_id: get_long(buf, 3),
        block_size: get_long(buf, 4),
        flags: get_long(buf, 5),
        badblk_list: get_long(buf, 6),
        part_list: get_long(buf, 7),
        fs_list: get_long(buf, 8),
        init_code: get_long(buf, 9),
        cylinders: get_long(buf, 16),
        sectors: get_long(buf, 17),
        heads: get_long(buf, 18),
        rdb_blk_lo: get_long(buf, 32),
        rdb_blk_hi: get_long(buf, 33),
        lo_cyl: get_long(buf, 34),
        hi_cyl: get_long(buf, 35),
        cyl_blks: get_long(buf, 36),
        disk_vendor: get_cstr(buf, 40, 2),
        disk_product: get_cstr(buf, 42, 4),
        disk_revision: get_cstr(buf, 46, 1),
    })
}

fn parse_part(buf: &[u8; 512], block_num: u64) -> Result<RdbPartition, RustyBackupError> {
    if &buf[0..4] != PART_SIGNATURE {
        return Err(RustyBackupError::InvalidRdb(format!(
            "bad PART signature at block {block_num}"
        )));
    }
    verify_checksum(buf, "PART")?;
    Ok(RdbPartition {
        block_num,
        size_longs: get_long(buf, 1),
        host_id: get_long(buf, 3),
        next: get_long(buf, 4),
        flags: get_long(buf, 5),
        dev_flags: get_long(buf, 8),
        drv_name: get_bstr(buf, 9, 31),
        env_size: get_long(buf, 32),
        env_block_size_longs: get_long(buf, 33),
        sec_org: get_long(buf, 34),
        surfaces: get_long(buf, 35),
        sec_per_blk: get_long(buf, 36),
        blk_per_trk: get_long(buf, 37),
        reserved: get_long(buf, 38),
        pre_alloc: get_long(buf, 39),
        interleave: get_long(buf, 40),
        low_cyl: get_long(buf, 41),
        high_cyl: get_long(buf, 42),
        num_buffer: get_long(buf, 43),
        buf_mem_type: get_long(buf, 44),
        max_transfer: get_long(buf, 45),
        mask: get_long(buf, 46),
        boot_pri: get_slong(buf, 47),
        dos_type: get_long(buf, 48),
        baud: get_long(buf, 49),
        control: get_long(buf, 50),
        boot_blocks: get_long(buf, 51),
    })
}

/// Human-readable name for an AmigaDOS DosType tag. Falls back to the raw
/// `format_dos_type` string for unrecognized tags so the user still sees
/// something meaningful.
///
/// Examples:
/// - `DOS\0` → "AmigaDOS OFS"
/// - `DOS\3` → "AmigaDOS FFS-Intl"
/// - `PFS\3` → "PFS3 (Amiga)"
/// - `SFS\0` → "SFS (Amiga)"
pub fn dos_type_display_name(dos_type: u32) -> String {
    let tag = format_dos_type(dos_type);
    let pretty = match tag.as_str() {
        "DOS\\0" => "AmigaDOS OFS",
        "DOS\\1" => "AmigaDOS FFS",
        "DOS\\2" => "AmigaDOS OFS-Intl",
        "DOS\\3" => "AmigaDOS FFS-Intl",
        "DOS\\4" => "AmigaDOS OFS-DC",
        "DOS\\5" => "AmigaDOS FFS-DC",
        "DOS\\6" => "AmigaDOS OFS-LNFS",
        "DOS\\7" => "AmigaDOS FFS-LNFS",
        "PFS\\0" | "PFS\\1" | "PFS\\2" | "PFS\\3" | "PDS\\3" => "PFS (Amiga)",
        "muFS" => "muFS (Amiga PFS)",
        "SFS\\0" => "SFS (Amiga)",
        "SFS\\2" => "SFS2 (Amiga)",
        "JXF\\4" => "JXFS (Amiga)",
        _ => return tag,
    };
    pretty.to_string()
}

/// Format a 4-byte DosType tag using AmigaDOS convention: three ASCII
/// characters followed by a backslash-escaped version byte (e.g. `DOS\0`,
/// `PFS\3`, `SFS\2`). This is the form used as `partition_type_string`.
pub fn format_dos_type(dos_type: u32) -> String {
    let b = dos_type.to_be_bytes();
    let mut s = String::with_capacity(5);
    for &c in &b[0..3] {
        if (0x20..0x7F).contains(&c) {
            s.push(c as char);
        } else {
            s.push('?');
        }
    }
    let v = b[3];
    if (0x20..0x7F).contains(&v) {
        s.push(v as char);
    } else {
        s.push('\\');
        s.push_str(&v.to_string());
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a 512-byte block with the given long-indexed fields, then
    /// store the appropriate checksum at long index 2 so the block validates.
    fn make_block(magic: &[u8; 4], fields: &[(usize, u32)]) -> [u8; 512] {
        let mut buf = [0u8; 512];
        buf[0..4].copy_from_slice(magic);
        for &(idx, v) in fields {
            BigEndian::write_u32(&mut buf[idx * 4..idx * 4 + 4], v);
        }
        // Compute checksum: sum of all i32s with checksum slot = 0 should
        // be negated and stored.
        BigEndian::write_i32(&mut buf[2 * 4..2 * 4 + 4], 0);
        let mut sum: i32 = 0;
        for i in 0..128 {
            sum = sum.wrapping_add(BigEndian::read_i32(&buf[i * 4..i * 4 + 4]));
        }
        BigEndian::write_i32(&mut buf[2 * 4..2 * 4 + 4], sum.wrapping_neg());
        buf
    }

    #[test]
    fn dos_type_format() {
        // DOS\0 = 0x444F5300
        assert_eq!(format_dos_type(0x444F5300), "DOS\\0");
        // DOS\3 = 0x444F5303
        assert_eq!(format_dos_type(0x444F5303), "DOS\\3");
        // PFS\3 = 0x50465303
        assert_eq!(format_dos_type(0x50465303), "PFS\\3");
        // SFS\0
        assert_eq!(format_dos_type(0x53465300), "SFS\\0");
        // muFS = 0x6D754653 — all printable
        assert_eq!(format_dos_type(0x6D754653), "muFS");
    }

    #[test]
    fn parse_minimal_rdb() {
        // RDSK at block 0 → PART at block 1 → terminator.
        // Geometry: 4 heads × 32 sectors × cylinders 2..=3 = 2 cyls × 128 blk
        // = 256 sectors of 512 bytes = 128 KiB partition starting at byte
        // offset 2 × 4 × 32 × 512 = 131072.
        let mut disk = vec![0u8; 16 * 512];

        let rdsk = make_block(
            RDSK_SIGNATURE,
            &[
                (1, 64),       // size
                (4, 512),      // block_size
                (7, 1),        // part_list -> block 1
                (8, NO_BLOCK), // fs_list
                (6, NO_BLOCK), // badblk_list
                (9, NO_BLOCK), // init_code
            ],
        );
        disk[0..512].copy_from_slice(&rdsk);

        // PART for "DH0" / DOS\3
        let mut bstr = [0u8; 4 * 8];
        bstr[0] = 3;
        bstr[1..4].copy_from_slice(b"DH0");
        let part = {
            let mut buf = [0u8; 512];
            buf[0..4].copy_from_slice(PART_SIGNATURE);
            // size
            BigEndian::write_u32(&mut buf[4..8], 64);
            // next = NO_BLOCK
            BigEndian::write_u32(&mut buf[16..20], NO_BLOCK);
            // flags = 1 (bootable)
            BigEndian::write_u32(&mut buf[20..24], 1);
            // drv_name BSTR at long 9 (byte 36): length 3 + "DH0"
            buf[36] = 3;
            buf[37..40].copy_from_slice(b"DH0");
            // DosEnv
            BigEndian::write_u32(&mut buf[32 * 4..32 * 4 + 4], 16); // env_size
            BigEndian::write_u32(&mut buf[33 * 4..33 * 4 + 4], 128); // block_size in longs (=512 B)
            BigEndian::write_u32(&mut buf[35 * 4..35 * 4 + 4], 4); // surfaces
            BigEndian::write_u32(&mut buf[36 * 4..36 * 4 + 4], 1); // sec_per_blk
            BigEndian::write_u32(&mut buf[37 * 4..37 * 4 + 4], 32); // blk_per_trk
            BigEndian::write_u32(&mut buf[41 * 4..41 * 4 + 4], 2); // low_cyl
            BigEndian::write_u32(&mut buf[42 * 4..42 * 4 + 4], 3); // high_cyl
            BigEndian::write_u32(&mut buf[48 * 4..48 * 4 + 4], 0x444F5303); // DOS\3
                                                                            // checksum
            BigEndian::write_i32(&mut buf[8..12], 0);
            let mut sum: i32 = 0;
            for i in 0..128 {
                sum = sum.wrapping_add(BigEndian::read_i32(&buf[i * 4..i * 4 + 4]));
            }
            BigEndian::write_i32(&mut buf[8..12], sum.wrapping_neg());
            buf
        };
        disk[512..1024].copy_from_slice(&part);

        let mut cursor = Cursor::new(disk);
        let rdb = Rdb::parse(&mut cursor).expect("parse");
        assert_eq!(rdb.header.part_list, 1);
        assert_eq!(rdb.partitions.len(), 1);
        let p = &rdb.partitions[0];
        assert_eq!(p.drv_name, "DH0");
        assert_eq!(p.dos_type_string(), "DOS\\3");
        assert!(p.is_bootable());
        assert_eq!(p.surfaces, 4);
        assert_eq!(p.blk_per_trk, 32);
        assert_eq!(p.low_cyl, 2);
        assert_eq!(p.high_cyl, 3);
        assert_eq!(p.fs_block_size(), 512);
        assert_eq!(p.start_byte_offset(), 2 * 4 * 32 * 512);
        assert_eq!(p.size_bytes(), 2 * 4 * 32 * 512);
        assert_eq!(p.start_lba_512(), 2 * 4 * 32);
    }

    #[test]
    fn set_bootable_round_trip() {
        // Reuse the parse_minimal_rdb fixture inline so the toggle test
        // has its own block of disk to mutate.
        let mut disk = vec![0u8; 16 * 512];
        let rdsk = make_block(
            RDSK_SIGNATURE,
            &[
                (1, 64),
                (4, 512),
                (7, 1),
                (8, NO_BLOCK),
                (6, NO_BLOCK),
                (9, NO_BLOCK),
            ],
        );
        disk[0..512].copy_from_slice(&rdsk);

        let mut bstr = [0u8; 4 * 8];
        bstr[0] = 3;
        bstr[1..4].copy_from_slice(b"DH0");
        let part = {
            let mut buf = [0u8; 512];
            buf[0..4].copy_from_slice(PART_SIGNATURE);
            BigEndian::write_u32(&mut buf[4..8], 64);
            BigEndian::write_u32(&mut buf[4 * 4..4 * 4 + 4], NO_BLOCK);
            BigEndian::write_u32(&mut buf[5 * 4..5 * 4 + 4], 1); // bootable
            buf[9 * 4..9 * 4 + bstr.len()].copy_from_slice(&bstr);
            BigEndian::write_u32(&mut buf[32 * 4..32 * 4 + 4], 16); // env_size
            BigEndian::write_u32(&mut buf[33 * 4..33 * 4 + 4], 128); // env_block_size_longs
            BigEndian::write_u32(&mut buf[35 * 4..35 * 4 + 4], 4); // surfaces
            BigEndian::write_u32(&mut buf[37 * 4..37 * 4 + 4], 32); // blk_per_trk
            BigEndian::write_u32(&mut buf[41 * 4..41 * 4 + 4], 2); // low_cyl
            BigEndian::write_u32(&mut buf[42 * 4..42 * 4 + 4], 3); // high_cyl
            BigEndian::write_u32(&mut buf[48 * 4..48 * 4 + 4], 0x444F5303); // DOS\3
            let mut sum: i32 = 0;
            for i in 0..128 {
                sum = sum.wrapping_add(BigEndian::read_i32(&buf[i * 4..i * 4 + 4]));
            }
            BigEndian::write_i32(&mut buf[2 * 4..2 * 4 + 4], sum.wrapping_neg());
            buf
        };
        disk[512..1024].copy_from_slice(&part);

        let mut cursor = Cursor::new(disk);
        // Toggle off.
        let now = set_partition_bootable(&mut cursor, 1, false).expect("toggle off");
        assert!(!now);
        // Re-parse — checksum must validate, flag must be clear.
        let rdb = Rdb::parse(&mut cursor).expect("re-parse after toggle off");
        assert!(!rdb.partitions[0].is_bootable());
        // Toggle back on.
        let now = set_partition_bootable(&mut cursor, 1, true).expect("toggle on");
        assert!(now);
        let rdb = Rdb::parse(&mut cursor).expect("re-parse after toggle on");
        assert!(rdb.partitions[0].is_bootable());
    }

    #[test]
    fn rdsk_not_found() {
        let disk = vec![0u8; 16 * 512];
        let mut cursor = Cursor::new(disk);
        let err = Rdb::parse(&mut cursor).unwrap_err();
        match err {
            RustyBackupError::InvalidRdb(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
