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
