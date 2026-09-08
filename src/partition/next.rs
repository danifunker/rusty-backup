//! NeXT disk label — the partition scheme NeXTSTEP / OPENSTEP disks use
//! instead of MBR/GPT, on both the black m68k hardware and NeXTSTEP/Intel.
//!
//! One `struct disk_label` (NeXT's `<sys/disk.h>`) describes the drive
//! geometry and up to **8 partitions**. Every field is **big-endian even on
//! Intel** — NeXTSTEP kept the m68k byte order for its on-disk structures, and
//! so does the FFS inside the partitions (`src/fs/ufs.rs` already handles the
//! big-endian variant). This module is the missing piece that says where the
//! partitions live.
//!
//! Layout, offsets relative to the start of a label copy:
//! - `dl_version` @0 — `"NeXT"` (v1), `"dlV2"`, or `"dlV3"`,
//! - `dl_label_blkno` @4 (this copy's own 512-byte block number),
//! - `dl_size` @8, `dl_label[24]` @0xC, `dl_flags` @0x24, `dl_tag` @0x28,
//! - then an inline `struct disktab`: `d_name[24]` @0x2C, `d_type[24]` @0x44,
//!   `d_secsize` @0x5C, `d_ntracks` @0x60, `d_nsectors` @0x64,
//!   `d_ncylinders` @0x68, `d_rpm` @0x6C, `d_front` @0x70, `d_back` @0x72,
//!   `d_ngroups` @0x74, `d_ag_size` @0x76, `d_ag_alts` @0x78, `d_ag_off` @0x7A,
//!   `d_boot0_blkno[2]` @0x7C, `d_bootfile[24]` @0x84, `d_hostname[32]` @0x9C,
//!   `d_rootpartition` @0xBC, `d_rwpartition` @0xBD,
//! - `d_partitions[8]` @0xBE — 46 bytes each, unpadded,
//! - v3 checksum @0x22E; v1/v2 put `dl_bad[1670]` there and the checksum after.
//!
//! **Partition units are `d_secsize`, not 512**, and `p_base` is measured from
//! the end of the front porch — so a partition's byte offset is
//! `(d_front + p_base) * d_secsize`. NeXTSTEP's own installer uses 1024-byte
//! sectors on the drives we have fixtures for, which is why `PartitionInfo`
//! carries an explicit `start_byte` for this table.
//!
//! Four copies are written 15 × 512 bytes apart (blocks 0, 15, 30, 45); each
//! stamps its own `dl_label_blkno` but they otherwise share one checksum,
//! because the checksum is computed with `dl_label_blkno` read as zero. See
//! [`checksum`].

use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::error::RustyBackupError;

/// `"NeXT"` — the original v1 label signature.
pub const NEXT_LABEL_V1: u32 = 0x4e65_5854;
/// `"dlV2"`.
pub const NEXT_LABEL_V2: u32 = 0x646c_5632;
/// `"dlV3"`.
pub const NEXT_LABEL_V3: u32 = 0x646c_5633;

/// 512-byte block numbers NeXTSTEP writes the four label copies at.
pub const LABEL_BLOCKS: [u64; 4] = [0, 15, 30, 45];

/// Bytes one label copy owns (15 × 512) — the spacing between copies.
pub const LABEL_SPAN: usize = 15 * 512;

/// Partition slots in a NeXT label.
pub const N_PARTITIONS: usize = 8;

/// Offset of `d_partitions` inside a label copy.
pub const PART_TABLE_OFF: usize = 0xBE;
/// Bytes one `struct partition` occupies, its 2-byte alignment pad included.
pub const PART_ENTRY_SIZE: usize = 46;
/// v3 stores its checksum immediately after the partition table.
const V3_CKSUM_OFF: usize = PART_TABLE_OFF + N_PARTITIONS * PART_ENTRY_SIZE;
/// v1/v2 interpose `dl_bad[1670]` (int32) before theirs.
const V12_CKSUM_OFF: usize = V3_CKSUM_OFF + 1670 * 4;

/// An unused slot stores `p_base == -1`.
const P_BASE_UNUSED: i32 = -1;

/// One NeXT partition, resolved to absolute bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextPartition {
    /// `p_base` as stored: sectors past the front porch, in `sector_size` units.
    pub base: i32,
    /// `p_size` as stored, in `sector_size` units.
    pub size: i32,
    /// `p_bsize` — filesystem block size (8192 on a stock NeXTSTEP newfs).
    pub block_size: u16,
    /// `p_fsize` — filesystem fragment size (1024 on a stock newfs).
    pub frag_size: u16,
    /// `p_mountpt` — where NeXTSTEP mounts it (`/`, `/private`, ...).
    pub mount_point: String,
    /// `p_type` — the filesystem name NeXTSTEP records, e.g. `"4.3BSD"`.
    pub fs_type: String,
    /// Absolute byte offset: `(front_porch + base) * sector_size`.
    pub start_byte: u64,
    /// `size * sector_size`.
    pub size_bytes: u64,
}

impl NextPartition {
    pub fn is_empty(&self) -> bool {
        self.base == P_BASE_UNUSED || self.size <= 0
    }
    /// Partition letter NeXTSTEP names the slot by (`a`..`h`).
    pub fn letter(index: usize) -> char {
        (b'a' + index as u8) as char
    }
}

/// A parsed NeXT disk label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextDiskLabel {
    /// One of [`NEXT_LABEL_V1`] / [`NEXT_LABEL_V2`] / [`NEXT_LABEL_V3`].
    pub version: u32,
    /// `dl_label` — the volume name shown in the NeXTSTEP disk panel.
    pub label: String,
    /// `d_name` — drive model string the formatter recorded.
    pub drive_name: String,
    /// `d_type` — drive class, e.g. `"fixed_rw_scsi"`.
    pub drive_type: String,
    /// `d_secsize` — the unit every partition offset is counted in.
    pub sector_size: u32,
    pub ntracks: u32,
    pub nsectors: u32,
    pub ncylinders: u32,
    pub rpm: u32,
    /// `d_front` — sectors reserved before partition `a`, in `sector_size` units.
    pub front_porch: u16,
    /// `d_back` — sectors reserved after the last partition.
    pub back_porch: u16,
    /// `d_bootfile` — kernel NeXTSTEP boots (`mach_kernel`, `sdmach`).
    pub boot_file: String,
    pub hostname: String,
    /// `d_rootpartition` — letter of the root slot.
    pub root_partition: char,
    /// `d_rwpartition` — letter of the read/write slot.
    pub rw_partition: char,
    pub partitions: Vec<NextPartition>,
    /// Byte offset the copy we parsed was found at.
    pub label_offset: u64,
}

impl NextDiskLabel {
    /// Human name for the label revision.
    pub fn version_name(&self) -> &'static str {
        match self.version {
            NEXT_LABEL_V1 => "NeXT v1",
            NEXT_LABEL_V2 => "dlV2",
            NEXT_LABEL_V3 => "dlV3",
            _ => "NeXT",
        }
    }

    /// Non-empty partitions in slot order.
    pub fn browsable_partitions(&self) -> impl Iterator<Item = (usize, &NextPartition)> {
        self.partitions
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.is_empty())
    }

    /// Parse one label copy. `offset` is only recorded, not read from.
    pub fn parse(buf: &[u8], offset: u64) -> Result<Self, RustyBackupError> {
        if buf.len() < V3_CKSUM_OFF + 2 {
            return Err(RustyBackupError::InvalidMbr(
                "NeXT label: buffer shorter than one label".into(),
            ));
        }
        let version = BigEndian::read_u32(&buf[0..4]);
        if !matches!(version, NEXT_LABEL_V1 | NEXT_LABEL_V2 | NEXT_LABEL_V3) {
            return Err(RustyBackupError::InvalidMbr(
                "NeXT label: unrecognized version signature".into(),
            ));
        }

        let sector_size = BigEndian::read_u32(&buf[0x5C..0x60]);
        if !is_plausible_sector_size(sector_size) {
            return Err(RustyBackupError::InvalidMbr(format!(
                "NeXT label: implausible sector size {sector_size}"
            )));
        }
        let front_porch = BigEndian::read_u16(&buf[0x70..0x72]);

        let mut partitions = Vec::with_capacity(N_PARTITIONS);
        for i in 0..N_PARTITIONS {
            let base = PART_TABLE_OFF + i * PART_ENTRY_SIZE;
            let end = base + PART_ENTRY_SIZE;
            if end > buf.len() {
                break;
            }
            let e = &buf[base..end];
            let p_base = BigEndian::read_i32(&e[0..4]);
            let p_size = BigEndian::read_i32(&e[4..8]);
            let start_sector = front_porch as i64 + p_base as i64;
            partitions.push(NextPartition {
                base: p_base,
                size: p_size,
                block_size: BigEndian::read_u16(&e[8..10]),
                frag_size: BigEndian::read_u16(&e[10..12]),
                mount_point: c_string(&e[20..36]),
                fs_type: c_string(&e[37..45]),
                start_byte: start_sector.max(0) as u64 * sector_size as u64,
                size_bytes: p_size.max(0) as u64 * sector_size as u64,
            });
        }

        Ok(NextDiskLabel {
            version,
            label: c_string(&buf[0x0C..0x24]),
            drive_name: c_string(&buf[0x2C..0x44]),
            drive_type: c_string(&buf[0x44..0x5C]),
            sector_size,
            ntracks: BigEndian::read_u32(&buf[0x60..0x64]),
            nsectors: BigEndian::read_u32(&buf[0x64..0x68]),
            ncylinders: BigEndian::read_u32(&buf[0x68..0x6C]),
            rpm: BigEndian::read_u32(&buf[0x6C..0x70]),
            front_porch,
            back_porch: BigEndian::read_u16(&buf[0x72..0x74]),
            boot_file: c_string(&buf[0x84..0x9C]),
            hostname: c_string(&buf[0x9C..0xBC]),
            root_partition: printable_letter(buf[0xBC]),
            rw_partition: printable_letter(buf[0xBD]),
            partitions,
            label_offset: offset,
        })
    }

    /// Recompute and stamp this label's checksum into `buf` in place.
    pub fn stamp_checksum(buf: &mut [u8], version: u32) {
        let off = checksum_offset(version);
        if buf.len() < off + 2 {
            return;
        }
        let sum = checksum(&buf[..off]);
        BigEndian::write_u16(&mut buf[off..off + 2], sum);
    }
}

/// The `struct partition` fields a writer sets. The rest of the entry is
/// fixed at what a stock NeXTSTEP `newfs` records, which both reference disks
/// carry verbatim.
#[derive(Debug, Clone)]
pub struct NextPartitionSpec {
    /// `p_base` — sectors past the front porch, in `d_secsize` units.
    pub base: i32,
    /// `p_size` in the same units.
    pub size: i32,
    /// `p_bsize` — filesystem block size.
    pub block_size: u16,
    /// `p_fsize` — filesystem fragment size.
    pub frag_size: u16,
    /// `p_mountpt` — up to 16 bytes.
    pub mount_point: String,
    /// `p_type` — up to 8 bytes.
    pub fs_type: String,
}

impl Default for NextPartitionSpec {
    fn default() -> Self {
        Self {
            base: 0,
            size: 0,
            block_size: 8192,
            frag_size: 1024,
            mount_point: String::new(),
            fs_type: "4.3BSD".to_string(),
        }
    }
}

/// Everything a fresh label carries besides its partitions.
#[derive(Debug, Clone)]
pub struct NextLabelSpec {
    /// `dl_label` — the volume name the NeXTSTEP disk panel shows.
    pub label: String,
    /// `d_name` — drive model string.
    pub drive_name: String,
    /// `d_type` — drive class.
    pub drive_type: String,
    /// `d_secsize` — the unit `p_base` / `p_size` are counted in.
    pub sector_size: u32,
    pub ntracks: u32,
    /// `d_nsectors` — sectors per track, in `sector_size` units.
    pub nsectors: u32,
    pub ncylinders: u32,
    pub rpm: u32,
    /// `d_front` — sectors reserved ahead of partition `a`.
    pub front_porch: u16,
    /// `d_bootfile` — kernel NeXTSTEP boots.
    pub boot_file: String,
    pub hostname: String,
    pub root_partition: char,
    pub rw_partition: char,
    /// One entry per slot; `None` writes the unused pattern.
    pub partitions: Vec<Option<NextPartitionSpec>>,
}

impl Default for NextLabelSpec {
    fn default() -> Self {
        Self {
            label: "Disk".to_string(),
            drive_name: "rusty-backup".to_string(),
            drive_type: "fixed_rw_scsi".to_string(),
            sector_size: 1024,
            ntracks: 16,
            nsectors: 32,
            ncylinders: 0,
            rpm: 3600,
            front_porch: 160,
            boot_file: "sdmach".to_string(),
            hostname: "localhost".to_string(),
            root_partition: 'a',
            rw_partition: 'b',
            partitions: Vec::new(),
        }
    }
}

/// `d_boot0_blkno` — the two boot-block copies, in `d_secsize` units. Both
/// reference disks record 32 and 96, which sit past the four label copies and
/// still inside a 160-sector front porch.
pub const BOOT0_BLOCKS: [u32; 2] = [32, 96];

/// Serialize one label copy, checksum stamped, as a [`LABEL_SPAN`] buffer.
pub fn build_label(spec: &NextLabelSpec) -> Vec<u8> {
    let mut buf = vec![0u8; LABEL_SPAN];
    BigEndian::write_u32(&mut buf[0..4], NEXT_LABEL_V3);
    // `dl_size` is zero on both reference disks; the stamped block number is
    // written per copy by `write_copies`.
    put_str(&mut buf[0x0C..0x24], &spec.label);
    put_str(&mut buf[0x2C..0x44], &spec.drive_name);
    put_str(&mut buf[0x44..0x5C], &spec.drive_type);
    BigEndian::write_u32(&mut buf[0x5C..0x60], spec.sector_size);
    BigEndian::write_u32(&mut buf[0x60..0x64], spec.ntracks);
    BigEndian::write_u32(&mut buf[0x64..0x68], spec.nsectors);
    BigEndian::write_u32(&mut buf[0x68..0x6C], spec.ncylinders);
    BigEndian::write_u32(&mut buf[0x6C..0x70], spec.rpm);
    BigEndian::write_u16(&mut buf[0x70..0x72], spec.front_porch);
    BigEndian::write_u32(&mut buf[0x7C..0x80], BOOT0_BLOCKS[0]);
    BigEndian::write_u32(&mut buf[0x80..0x84], BOOT0_BLOCKS[1]);
    put_str(&mut buf[0x84..0x9C], &spec.boot_file);
    put_str(&mut buf[0x9C..0xBC], &spec.hostname);
    buf[0xBC] = spec.root_partition as u8;
    buf[0xBD] = spec.rw_partition as u8;
    for slot in 0..N_PARTITIONS {
        match spec.partitions.get(slot).and_then(|p| p.as_ref()) {
            Some(p) => write_partition(&mut buf, slot, p),
            None => clear_partition(&mut buf, slot),
        }
    }
    NextDiskLabel::stamp_checksum(&mut buf, NEXT_LABEL_V3);
    buf
}

/// Fill slot `slot` of a label copy from `spec`.
pub fn write_partition(buf: &mut [u8], slot: usize, spec: &NextPartitionSpec) {
    let Some(e) = entry_mut(buf, slot) else {
        return;
    };
    for b in e.iter_mut() {
        *b = 0;
    }
    BigEndian::write_i32(&mut e[0..4], spec.base);
    BigEndian::write_i32(&mut e[4..8], spec.size);
    BigEndian::write_u16(&mut e[8..10], spec.block_size);
    BigEndian::write_u16(&mut e[10..12], spec.frag_size);
    e[12] = b't';
    BigEndian::write_u16(&mut e[14..16], 16);
    BigEndian::write_u16(&mut e[16..18], 4096);
    e[18] = 10;
    e[19] = 1;
    put_str(&mut e[20..36], &spec.mount_point);
    e[36] = 1;
    put_str(&mut e[37..45], &spec.fs_type);
}

/// Stamp slot `slot` as unused, in the all-ones form NeXTSTEP writes.
pub fn clear_partition(buf: &mut [u8], slot: usize) {
    let Some(e) = entry_mut(buf, slot) else {
        return;
    };
    for b in e.iter_mut() {
        *b = 0;
    }
    for b in e[0..12].iter_mut() {
        *b = 0xFF;
    }
    for b in e[14..19].iter_mut() {
        *b = 0xFF;
    }
}

/// Move slot `slot` without disturbing the filesystem geometry beside it.
pub fn set_partition_extent(buf: &mut [u8], slot: usize, base: i32, size: i32) {
    let Some(e) = entry_mut(buf, slot) else {
        return;
    };
    BigEndian::write_i32(&mut e[0..4], base);
    BigEndian::write_i32(&mut e[4..8], size);
}

/// Rewrite slot `slot`'s `p_type`.
pub fn set_partition_type(buf: &mut [u8], slot: usize, fs_type: &str) {
    let Some(e) = entry_mut(buf, slot) else {
        return;
    };
    put_str(&mut e[37..45], fs_type);
}

fn entry_mut(buf: &mut [u8], slot: usize) -> Option<&mut [u8]> {
    let start = PART_TABLE_OFF + slot * PART_ENTRY_SIZE;
    buf.get_mut(start..start + PART_ENTRY_SIZE)
}

/// NUL-padded fixed-width string field.
fn put_str(field: &mut [u8], text: &str) {
    for b in field.iter_mut() {
        *b = 0;
    }
    let n = text.len().min(field.len().saturating_sub(1));
    field[..n].copy_from_slice(&text.as_bytes()[..n]);
}

/// The 512-byte blocks that already hold a valid copy. NeXTSTEP/Intel has none
/// at block 0 — the PC boot sector lives there — so a rewrite must land on
/// exactly the copies the disk was made with.
pub fn present_copies<R: Read + Seek>(reader: &mut R) -> Vec<u64> {
    let mut out = Vec::new();
    let Ok(disk_size) = reader.seek(SeekFrom::End(0)) else {
        return out;
    };
    for block in LABEL_BLOCKS {
        let offset = block * 512;
        if offset + LABEL_SPAN as u64 > disk_size || reader.seek(SeekFrom::Start(offset)).is_err() {
            continue;
        }
        let mut buf = vec![0u8; LABEL_SPAN];
        if reader.read_exact(&mut buf).is_ok() && validates(&buf) {
            out.push(block);
        }
    }
    out
}

/// Write `copy` to each of `blocks`, stamping every copy's own
/// `dl_label_blkno`. The checksum reads that field as zero, so one stamp on
/// `copy` covers all four.
pub fn write_copies<W: Write + Seek>(
    out: &mut W,
    copy: &[u8],
    blocks: &[u64],
) -> std::io::Result<()> {
    let mut buf = copy.to_vec();
    for &block in blocks {
        BigEndian::write_u32(&mut buf[4..8], block as u32);
        out.seek(SeekFrom::Start(block * 512))?;
        out.write_all(&buf)?;
    }
    out.flush()
}

/// Probe the four label copies and return the first that validates.
pub fn detect<R: Read + Seek>(reader: &mut R) -> Option<NextDiskLabel> {
    let disk_size = reader.seek(SeekFrom::End(0)).ok()?;
    for block in LABEL_BLOCKS {
        let offset = block * 512;
        if offset + LABEL_SPAN as u64 > disk_size {
            continue;
        }
        if reader.seek(SeekFrom::Start(offset)).is_err() {
            continue;
        }
        let mut buf = vec![0u8; LABEL_SPAN];
        if reader.read_exact(&mut buf).is_err() {
            continue;
        }
        if !validates(&buf) {
            continue;
        }
        if let Ok(label) = NextDiskLabel::parse(&buf, offset) {
            if label_fits(&label, disk_size) {
                return Some(label);
            }
        }
    }
    None
}

/// Signature, sector size, and checksum all have to agree before we claim a
/// disk — a NeXTSTEP/Intel disk also carries a valid `0xAA55` boot sector, so
/// this probe runs ahead of MBR parsing and must never fire on a PC disk.
fn validates(buf: &[u8]) -> bool {
    if buf.len() < 8 {
        return false;
    }
    let version = BigEndian::read_u32(&buf[0..4]);
    if !matches!(version, NEXT_LABEL_V1 | NEXT_LABEL_V2 | NEXT_LABEL_V3) {
        return false;
    }
    if buf.len() < 0x60 || !is_plausible_sector_size(BigEndian::read_u32(&buf[0x5C..0x60])) {
        return false;
    }
    let off = checksum_offset(version);
    if buf.len() < off + 2 {
        return false;
    }
    checksum(&buf[..off]) == BigEndian::read_u16(&buf[off..off + 2])
}

/// A label whose partitions run off the end of the medium is a false positive.
fn label_fits(label: &NextDiskLabel, disk_size: u64) -> bool {
    let mut any = false;
    for (_, p) in label.browsable_partitions() {
        any = true;
        if p.start_byte.saturating_add(p.size_bytes) > disk_size {
            return false;
        }
    }
    any
}

fn checksum_offset(version: u32) -> usize {
    if version == NEXT_LABEL_V3 {
        V3_CKSUM_OFF
    } else {
        V12_CKSUM_OFF
    }
}

/// End-around-carry sum of big-endian 16-bit words, with `dl_label_blkno`
/// (bytes 4..8) read as zero — that is what lets all four copies carry the
/// same checksum while each stamps its own block number.
pub fn checksum(buf: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    for (i, w) in buf.chunks_exact(2).enumerate() {
        let byte_off = i * 2;
        let word = if (4..8).contains(&byte_off) {
            0
        } else {
            BigEndian::read_u16(w) as u32
        };
        sum += word;
        if sum > 0xFFFF {
            sum -= 0xFFFF;
        }
    }
    sum as u16
}

fn is_plausible_sector_size(v: u32) -> bool {
    matches!(v, 256 | 512 | 1024 | 2048 | 4096 | 8192)
}

fn c_string(raw: &[u8]) -> String {
    let end = raw.iter().position(|&c| c == 0).unwrap_or(raw.len());
    String::from_utf8_lossy(&raw[..end]).trim().to_string()
}

fn printable_letter(b: u8) -> char {
    if b.is_ascii_alphanumeric() {
        b as char
    } else {
        '?'
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal v3 label: one 4.3BSD partition on a 1024-byte-sector drive.
    fn synth_label() -> Vec<u8> {
        let mut buf = vec![0u8; LABEL_SPAN];
        BigEndian::write_u32(&mut buf[0..4], NEXT_LABEL_V3);
        BigEndian::write_i32(&mut buf[4..8], 15);
        buf[0x0C..0x10].copy_from_slice(b"Disk");
        buf[0x2C..0x31].copy_from_slice(b"Drive");
        buf[0x44..0x51].copy_from_slice(b"fixed_rw_scsi");
        BigEndian::write_u32(&mut buf[0x5C..0x60], 1024);
        BigEndian::write_u32(&mut buf[0x60..0x64], 16);
        BigEndian::write_u32(&mut buf[0x64..0x68], 32);
        BigEndian::write_u32(&mut buf[0x68..0x6C], 100);
        BigEndian::write_u16(&mut buf[0x70..0x72], 160);
        buf[0xBC] = b'a';
        buf[0xBD] = b'b';
        let p = PART_TABLE_OFF;
        BigEndian::write_i32(&mut buf[p..p + 4], 0);
        BigEndian::write_i32(&mut buf[p + 4..p + 8], 1000);
        BigEndian::write_u16(&mut buf[p + 8..p + 10], 8192);
        BigEndian::write_u16(&mut buf[p + 10..p + 12], 1024);
        buf[p + 37..p + 43].copy_from_slice(b"4.3BSD");
        for slot in 1..N_PARTITIONS {
            let off = PART_TABLE_OFF + slot * PART_ENTRY_SIZE;
            BigEndian::write_i32(&mut buf[off..off + 4], P_BASE_UNUSED);
        }
        NextDiskLabel::stamp_checksum(&mut buf, NEXT_LABEL_V3);
        buf
    }

    #[test]
    fn parses_a_synthetic_v3_label() {
        let buf = synth_label();
        assert!(validates(&buf));
        let label = NextDiskLabel::parse(&buf, 0x1E00).unwrap();
        assert_eq!(label.sector_size, 1024);
        assert_eq!(label.front_porch, 160);
        assert_eq!(label.root_partition, 'a');
        let live: Vec<_> = label.browsable_partitions().collect();
        assert_eq!(live.len(), 1);
        // Partition base is relative to the front porch, in 1024-byte sectors.
        assert_eq!(live[0].1.start_byte, 160 * 1024);
        assert_eq!(live[0].1.size_bytes, 1000 * 1024);
        assert_eq!(live[0].1.fs_type, "4.3BSD");
    }

    #[test]
    fn checksum_ignores_the_per_copy_block_number() {
        let mut a = synth_label();
        let stored = BigEndian::read_u16(&a[V3_CKSUM_OFF..V3_CKSUM_OFF + 2]);
        BigEndian::write_i32(&mut a[4..8], 45);
        assert!(validates(&a), "a copy at another block still validates");
        assert_eq!(
            BigEndian::read_u16(&a[V3_CKSUM_OFF..V3_CKSUM_OFF + 2]),
            stored
        );
    }

    #[test]
    fn a_flipped_byte_fails_the_checksum() {
        let mut buf = synth_label();
        buf[0x0C] ^= 0xFF;
        assert!(!validates(&buf));
    }

    #[test]
    fn detect_rejects_a_plain_mbr() {
        let mut img = vec![0u8; 64 * 1024];
        img[510] = 0x55;
        img[511] = 0xAA;
        let mut cur = std::io::Cursor::new(img);
        assert!(detect(&mut cur).is_none());
    }

    #[test]
    fn a_built_label_parses_back_and_keeps_the_front_porch_offset() {
        let spec = NextLabelSpec {
            ncylinders: 670,
            partitions: vec![Some(NextPartitionSpec {
                base: 864,
                size: 674816,
                mount_point: "/".to_string(),
                ..Default::default()
            })],
            ..Default::default()
        };
        let buf = build_label(&spec);
        assert!(validates(&buf), "a freshly built label must checksum");
        let label = NextDiskLabel::parse(&buf, 0).unwrap();
        let live: Vec<_> = label.browsable_partitions().collect();
        assert_eq!(live.len(), 1, "the other seven slots are unused");
        assert_eq!(live[0].1.fs_type, "4.3BSD");
        assert_eq!(live[0].1.mount_point, "/");
        // (160 + 864) sectors of 1024 — not 864 * 512, the trap this guards.
        assert_eq!(live[0].1.start_byte, (160 + 864) * 1024);
        assert_eq!(live[0].1.size_bytes, 674816 * 1024);
    }

    /// An unused slot has to read back unused, or a fresh label grows seven
    /// phantom partitions at block -1.
    #[test]
    fn empty_slots_are_written_in_the_unused_form() {
        let buf = build_label(&NextLabelSpec::default());
        let label = NextDiskLabel::parse(&buf, 0).unwrap();
        assert!(label.partitions.iter().all(|p| p.is_empty()));
        let slot = &buf[PART_TABLE_OFF..PART_TABLE_OFF + 12];
        assert!(slot.iter().all(|&b| b == 0xFF), "got {slot:02x?}");
    }

    #[test]
    fn every_copy_carries_its_own_block_number_and_one_checksum() {
        let mut img = std::io::Cursor::new(vec![0u8; 4 * 1024 * 1024]);
        let label = build_label(&NextLabelSpec {
            partitions: vec![Some(NextPartitionSpec {
                base: 0,
                size: 1000,
                ..Default::default()
            })],
            ..Default::default()
        });
        write_copies(&mut img, &label, &LABEL_BLOCKS).unwrap();
        assert_eq!(present_copies(&mut img), LABEL_BLOCKS.to_vec());
        for block in LABEL_BLOCKS {
            let at = (block * 512) as usize;
            let copy = &img.get_ref()[at..at + LABEL_SPAN];
            assert_eq!(BigEndian::read_u32(&copy[4..8]), block as u32);
            assert_eq!(
                BigEndian::read_u16(&copy[V3_CKSUM_OFF..V3_CKSUM_OFF + 2]),
                BigEndian::read_u16(&label[V3_CKSUM_OFF..V3_CKSUM_OFF + 2]),
            );
        }
    }

    #[test]
    fn detect_finds_the_copy_at_block_fifteen() {
        let mut img = vec![0u8; 4 * 1024 * 1024];
        img[510] = 0x55;
        img[511] = 0xAA;
        let label = synth_label();
        img[15 * 512..15 * 512 + LABEL_SPAN].copy_from_slice(&label);
        let mut cur = std::io::Cursor::new(img);
        let found = detect(&mut cur).expect("label at block 15");
        assert_eq!(found.label_offset, 15 * 512);
        assert_eq!(found.label, "Disk");
    }
}
