use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};

use crate::error::RustyBackupError;

/// SGI Volume Header magic at byte 0 of sector 0. Big-endian.
pub const SGI_VOLHDR_MAGIC: u32 = 0x0BE5A941;

/// Volume header is one 512-byte sector. Defined as a constant so callers
/// passing larger buffers (the plan uses 4 KiB) can document intent.
pub const SGI_VOLHDR_SIZE: usize = 512;

/// Number of partition entries in the volume header.
pub const SGI_NUM_PARTITIONS: usize = 16;

/// Number of volume directory entries.
pub const SGI_NUM_VOL_DIR: usize = 15;

/// Synthetic MBR-style type byte we emit from `PartitionTable::Sgi` for XFS
/// partitions. Type bytes don't exist on SGI disks; we mint these so the
/// existing type-byte routing in `fs/mod.rs` can dispatch without a new
/// trait. Only `PartitionTable::Sgi` produces them — a real MBR with type
/// 0xA0 (laptop hibernation) is unrelated.
pub const SGI_TYPE_BYTE_XFS: u8 = 0xA0;

/// Synthetic MBR-style type byte for SGI EFS partitions.
pub const SGI_TYPE_BYTE_EFS: u8 = 0xA1;

/// SGI partition type codes (big-endian u32 at offset 8 of each entry).
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SgiPartitionType {
    VolHdr = 0,
    TrkRepl = 1,
    SecRepl = 2,
    Raw = 3,
    Bsd = 4,
    SysV = 5,
    Volume = 6,
    Efs = 7,
    LVol = 8,
    RLVol = 9,
    Xfs = 10,
    XfsLog = 11,
    Xlv = 12,
    Xvm = 13,
    Unknown(u32),
}

impl SgiPartitionType {
    pub fn from_raw(v: u32) -> Self {
        match v {
            0 => Self::VolHdr,
            1 => Self::TrkRepl,
            2 => Self::SecRepl,
            3 => Self::Raw,
            4 => Self::Bsd,
            5 => Self::SysV,
            6 => Self::Volume,
            7 => Self::Efs,
            8 => Self::LVol,
            9 => Self::RLVol,
            10 => Self::Xfs,
            11 => Self::XfsLog,
            12 => Self::Xlv,
            13 => Self::Xvm,
            other => Self::Unknown(other),
        }
    }

    pub fn as_u32(self) -> u32 {
        match self {
            Self::VolHdr => 0,
            Self::TrkRepl => 1,
            Self::SecRepl => 2,
            Self::Raw => 3,
            Self::Bsd => 4,
            Self::SysV => 5,
            Self::Volume => 6,
            Self::Efs => 7,
            Self::LVol => 8,
            Self::RLVol => 9,
            Self::Xfs => 10,
            Self::XfsLog => 11,
            Self::Xlv => 12,
            Self::Xvm => 13,
            Self::Unknown(v) => v,
        }
    }

    /// Plain-ASCII display name. No Unicode glyphs.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::VolHdr => "VOLHDR",
            Self::TrkRepl => "TRKREPL",
            Self::SecRepl => "SECREPL",
            Self::Raw => "RAW",
            Self::Bsd => "BSD",
            Self::SysV => "SYSV",
            Self::Volume => "VOLUME",
            Self::Efs => "EFS",
            Self::LVol => "LVOL",
            Self::RLVol => "RLVOL",
            Self::Xfs => "XFS",
            Self::XfsLog => "XFSLOG",
            Self::Xlv => "XLV",
            Self::Xvm => "XVM",
            Self::Unknown(_) => "Unknown",
        }
    }

    /// True if this partition is browseable as a filesystem in our GUI. Step
    /// 2 surfaces only EFS and XFS; other types (VOLHDR, VOLUME, container
    /// types, swap/raw, log) are listed but not opened.
    pub fn is_browsable(self) -> bool {
        matches!(self, Self::Efs | Self::Xfs)
    }

    /// True if the partition is a wrapper around the entire disk or a
    /// non-file region that doesn't represent user data on its own. These
    /// are filtered out of the displayed partition list.
    pub fn is_disk_wide_wrapper(self) -> bool {
        matches!(self, Self::VolHdr | Self::Volume)
    }

    /// True if the partition is non-browse — disk-wide wrappers, swap/raw
    /// regions, log volumes, and logical-volume manager wrappers. These are
    /// filtered out of `PartitionTable::partitions()` per the plan in
    /// `docs/SGI_Filesystems.md` Step 2.
    pub fn is_skipped_from_browse(self) -> bool {
        matches!(
            self,
            Self::VolHdr | Self::Volume | Self::Raw | Self::XfsLog | Self::Xlv | Self::Xvm
        )
    }
}

/// 16-byte volume directory entry: name[8] + block_num(u32 BE) + bytes(u32 BE).
/// Used by SGI PROM to locate standalone executables (`sash`, `ide`, `/unix`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SgiVolumeDirEntry {
    pub name: String,
    pub block_num: u32,
    pub bytes: u32,
}

impl SgiVolumeDirEntry {
    fn parse(buf: &[u8; 16]) -> Self {
        let name = parse_fixed_ascii(&buf[0..8]);
        let block_num = BigEndian::read_u32(&buf[8..12]);
        let bytes = BigEndian::read_u32(&buf[12..16]);
        SgiVolumeDirEntry {
            name,
            block_num,
            bytes,
        }
    }

    fn to_bytes(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        write_fixed_ascii(&mut out[0..8], &self.name);
        BigEndian::write_u32(&mut out[8..12], self.block_num);
        BigEndian::write_u32(&mut out[12..16], self.bytes);
        out
    }

    pub fn is_empty(&self) -> bool {
        self.name.is_empty() && self.block_num == 0 && self.bytes == 0
    }
}

/// 12-byte partition entry: blocks(u32 BE) + first(u32 BE) + type(u32 BE).
/// Sectors are always 512 bytes regardless of physical sector size.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SgiPartitionEntry {
    /// Sector count (512-byte sectors).
    pub blocks: u32,
    /// First sector (512-byte sectors).
    pub first: u32,
    /// Raw partition type code. Kept as u32 for round-trip fidelity even
    /// when we don't recognize the value.
    pub partition_type_raw: u32,
}

impl SgiPartitionEntry {
    fn parse(buf: &[u8; 12]) -> Self {
        SgiPartitionEntry {
            blocks: BigEndian::read_u32(&buf[0..4]),
            first: BigEndian::read_u32(&buf[4..8]),
            partition_type_raw: BigEndian::read_u32(&buf[8..12]),
        }
    }

    fn to_bytes(&self) -> [u8; 12] {
        let mut out = [0u8; 12];
        BigEndian::write_u32(&mut out[0..4], self.blocks);
        BigEndian::write_u32(&mut out[4..8], self.first);
        BigEndian::write_u32(&mut out[8..12], self.partition_type_raw);
        out
    }

    pub fn partition_type(&self) -> SgiPartitionType {
        SgiPartitionType::from_raw(self.partition_type_raw)
    }

    pub fn is_empty(&self) -> bool {
        self.blocks == 0 && self.first == 0 && self.partition_type_raw == 0
    }

    /// Partition size in bytes (sectors are always 512 bytes on SGI).
    pub fn size_bytes(&self) -> u64 {
        self.blocks as u64 * 512
    }

    /// Start byte offset from the beginning of the disk.
    pub fn start_offset(&self) -> u64 {
        self.first as u64 * 512
    }
}

/// Parsed SGI Volume Header (sector 0 of an SGI disk).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SgiVolumeHeader {
    pub magic: u32,
    pub root_part_num: u16,
    pub swap_part_num: u16,
    /// Null-terminated bootfile path (e.g. `/unix`).
    pub bootfile: String,
    pub volume_directory: Vec<SgiVolumeDirEntry>,
    pub partitions: Vec<SgiPartitionEntry>,
    pub checksum: u32,
    /// True if the on-disk checksum did not match a recomputation of the
    /// sector. We log and continue (many IRIX install media have stale
    /// checksums after image-tool round-trips), but the flag is preserved
    /// for the inspect tab to surface if we ever want it.
    pub checksum_valid: bool,
}

impl SgiVolumeHeader {
    /// Parse from sector 0. `buf` must be at least 512 bytes; only the first
    /// 512 are consulted. Returns `Err(InvalidSgi)` on a magic mismatch or
    /// any partition-entry overflow.
    pub fn parse(buf: &[u8]) -> Result<Self, RustyBackupError> {
        if buf.len() < SGI_VOLHDR_SIZE {
            return Err(RustyBackupError::InvalidSgi(format!(
                "buffer too small: {} bytes, need at least {}",
                buf.len(),
                SGI_VOLHDR_SIZE
            )));
        }
        let buf = &buf[..SGI_VOLHDR_SIZE];

        let magic = BigEndian::read_u32(&buf[0..4]);
        if magic != SGI_VOLHDR_MAGIC {
            return Err(RustyBackupError::InvalidSgi(format!(
                "bad SGI volume header magic: 0x{magic:08X} (expected 0x{SGI_VOLHDR_MAGIC:08X})"
            )));
        }

        let root_part_num = BigEndian::read_u16(&buf[4..6]);
        let swap_part_num = BigEndian::read_u16(&buf[6..8]);
        let bootfile = parse_fixed_ascii(&buf[8..24]);

        // Volume directory: 15 × 16-byte entries at 0x048..0x138.
        let mut volume_directory = Vec::with_capacity(SGI_NUM_VOL_DIR);
        for i in 0..SGI_NUM_VOL_DIR {
            let off = 0x048 + i * 16;
            let entry =
                SgiVolumeDirEntry::parse(buf[off..off + 16].try_into().expect("16-byte slice"));
            volume_directory.push(entry);
        }

        // Partition entries: 16 × 12-byte entries at 0x138..0x1F8.
        let mut partitions = Vec::with_capacity(SGI_NUM_PARTITIONS);
        for i in 0..SGI_NUM_PARTITIONS {
            let off = 0x138 + i * 12;
            let entry =
                SgiPartitionEntry::parse(buf[off..off + 12].try_into().expect("12-byte slice"));

            // Overflow check: `first + blocks` must fit in u32. Sanity for
            // image corruption; the SGI on-disk layout uses 32-bit block
            // counts, so a wraparound here is unambiguously bad.
            if !entry.is_empty() && entry.first.checked_add(entry.blocks).is_none() {
                return Err(RustyBackupError::InvalidSgi(format!(
                    "partition[{i}] first={} blocks={} overflows u32",
                    entry.first, entry.blocks
                )));
            }
            partitions.push(entry);
        }

        let checksum = BigEndian::read_u32(&buf[0x1F8..0x1FC]);
        let checksum_valid = volume_checksum_zero(buf);

        Ok(SgiVolumeHeader {
            magic,
            root_part_num,
            swap_part_num,
            bootfile,
            volume_directory,
            partitions,
            checksum,
            checksum_valid,
        })
    }

    /// Serialize back to a 512-byte sector. The checksum field is
    /// recomputed so the sector's u32-sum is zero. Used for restore and for
    /// round-trip tests; not yet wired into a write path.
    pub fn to_bytes(&self) -> [u8; SGI_VOLHDR_SIZE] {
        let mut buf = [0u8; SGI_VOLHDR_SIZE];
        BigEndian::write_u32(&mut buf[0..4], self.magic);
        BigEndian::write_u16(&mut buf[4..6], self.root_part_num);
        BigEndian::write_u16(&mut buf[6..8], self.swap_part_num);
        write_fixed_ascii(&mut buf[8..24], &self.bootfile);

        for (i, entry) in self.volume_directory.iter().enumerate() {
            if i >= SGI_NUM_VOL_DIR {
                break;
            }
            let off = 0x048 + i * 16;
            buf[off..off + 16].copy_from_slice(&entry.to_bytes());
        }

        for (i, entry) in self.partitions.iter().enumerate() {
            if i >= SGI_NUM_PARTITIONS {
                break;
            }
            let off = 0x138 + i * 12;
            buf[off..off + 12].copy_from_slice(&entry.to_bytes());
        }

        // Recompute checksum so the u32-sum of the sector is zero.
        let cksum = compute_volume_checksum(&buf);
        BigEndian::write_u32(&mut buf[0x1F8..0x1FC], cksum);
        buf
    }
}

/// Verify the SGI volume header checksum. The sector is treated as 128 BE
/// u32s; their two's-complement sum must equal zero. The checksum field at
/// 0x1F8 carries whatever value makes that true.
fn volume_checksum_zero(buf: &[u8]) -> bool {
    let mut sum: u32 = 0;
    for chunk in buf[..SGI_VOLHDR_SIZE].chunks_exact(4) {
        sum = sum.wrapping_add(BigEndian::read_u32(chunk));
    }
    sum == 0
}

/// Compute the checksum value that, written at offset 0x1F8, makes the
/// sector's u32-sum equal zero. Assumes the field at 0x1F8 is currently 0
/// in `buf` (the `to_bytes` writer zeroes the whole buffer first).
fn compute_volume_checksum(buf: &[u8; SGI_VOLHDR_SIZE]) -> u32 {
    let mut sum: u32 = 0;
    for chunk in buf.chunks_exact(4) {
        sum = sum.wrapping_add(BigEndian::read_u32(chunk));
    }
    // Want sum_with_checksum == 0 => checksum = 0 - current_sum (mod 2^32)
    0u32.wrapping_sub(sum)
}

fn parse_fixed_ascii(buf: &[u8]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end])
        .trim_end_matches(' ')
        .to_string()
}

fn write_fixed_ascii(dst: &mut [u8], s: &str) {
    for b in dst.iter_mut() {
        *b = 0;
    }
    let bytes = s.as_bytes();
    let n = bytes.len().min(dst.len());
    dst[..n].copy_from_slice(&bytes[..n]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_irix_volhdr_fixture() -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sgi/irix_volhdr.bin");
        std::fs::read(&path).expect("fixture present")
    }

    #[test]
    fn parses_irix_6_5_volhdr_fixture() {
        let buf = load_irix_volhdr_fixture();
        let vh = SgiVolumeHeader::parse(&buf).expect("valid SGI volhdr");
        assert_eq!(vh.magic, SGI_VOLHDR_MAGIC);
        assert_eq!(vh.root_part_num, 0);
        assert_eq!(vh.swap_part_num, 1);
        assert_eq!(vh.bootfile, "/unix");

        // Partition 0 is XFS at first-block 0x00041000, blocks 0x0BB3F000
        // per docs/SGI_Filesystems.md (Known images on hand).
        let p0 = &vh.partitions[0];
        assert_eq!(p0.first, 0x0004_1000);
        assert_eq!(p0.blocks, 0x0BB3_F000);
        assert_eq!(p0.partition_type(), SgiPartitionType::Xfs);

        // Partition 10 is the disk-wide VOLUME wrapper (type 6).
        let p10 = &vh.partitions[10];
        assert_eq!(p10.first, 0);
        assert_eq!(p10.partition_type(), SgiPartitionType::Volume);

        // Partition 8 is the volume header itself (type 0, VOLHDR).
        let p8 = &vh.partitions[8];
        assert_eq!(p8.first, 0);
        assert_eq!(p8.partition_type(), SgiPartitionType::VolHdr);
    }

    #[test]
    fn rejects_wrong_magic() {
        let mut buf = vec![0u8; SGI_VOLHDR_SIZE];
        buf[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
        let err = SgiVolumeHeader::parse(&buf).unwrap_err();
        assert!(
            format!("{err}").contains("magic"),
            "error should mention magic: {err}"
        );
    }

    #[test]
    fn rejects_short_buffer() {
        let buf = vec![0u8; 256];
        let err = SgiVolumeHeader::parse(&buf).unwrap_err();
        assert!(
            format!("{err}").contains("too small"),
            "error should mention size: {err}"
        );
    }

    #[test]
    fn rejects_partition_overflow() {
        // Magic, but partition[0] has first = u32::MAX and blocks = 1.
        let mut buf = vec![0u8; SGI_VOLHDR_SIZE];
        buf[0..4].copy_from_slice(&SGI_VOLHDR_MAGIC.to_be_bytes());
        let off = 0x138;
        buf[off..off + 4].copy_from_slice(&1u32.to_be_bytes()); // blocks
        buf[off + 4..off + 8].copy_from_slice(&u32::MAX.to_be_bytes()); // first
        buf[off + 8..off + 12].copy_from_slice(&(SgiPartitionType::Xfs.as_u32()).to_be_bytes());
        let err = SgiVolumeHeader::parse(&buf).unwrap_err();
        assert!(format!("{err}").contains("overflow"));
    }

    #[test]
    fn round_trips_synthetic_header() {
        let mut vh = SgiVolumeHeader {
            magic: SGI_VOLHDR_MAGIC,
            root_part_num: 0,
            swap_part_num: 1,
            bootfile: "/unix".to_string(),
            volume_directory: vec![SgiVolumeDirEntry {
                name: "sash".to_string(),
                block_num: 4,
                bytes: 512_000,
            }],
            partitions: Vec::new(),
            checksum: 0,
            checksum_valid: true,
        };
        // Pad to SGI_NUM_VOL_DIR with empties so to_bytes serializes a full
        // table; parse() always produces 15 entries on read.
        while vh.volume_directory.len() < SGI_NUM_VOL_DIR {
            vh.volume_directory.push(SgiVolumeDirEntry {
                name: String::new(),
                block_num: 0,
                bytes: 0,
            });
        }
        vh.partitions.push(SgiPartitionEntry {
            blocks: 0x0BB3_F000,
            first: 0x0004_1000,
            partition_type_raw: SgiPartitionType::Xfs.as_u32(),
        });
        while vh.partitions.len() < SGI_NUM_PARTITIONS {
            vh.partitions.push(SgiPartitionEntry {
                blocks: 0,
                first: 0,
                partition_type_raw: 0,
            });
        }

        let bytes = vh.to_bytes();
        assert!(volume_checksum_zero(&bytes), "checksum recompute failed");

        let round = SgiVolumeHeader::parse(&bytes).unwrap();
        assert_eq!(round.magic, vh.magic);
        assert_eq!(round.root_part_num, vh.root_part_num);
        assert_eq!(round.swap_part_num, vh.swap_part_num);
        assert_eq!(round.bootfile, vh.bootfile);
        assert!(round.checksum_valid);
        assert_eq!(round.partitions[0].first, 0x0004_1000);
        assert_eq!(round.partitions[0].blocks, 0x0BB3_F000);
        assert_eq!(round.partitions[0].partition_type(), SgiPartitionType::Xfs);
        assert_eq!(round.volume_directory[0].name, "sash");
        assert_eq!(round.volume_directory[0].block_num, 4);
    }

    #[test]
    fn synthetic_type_bytes_do_not_collide_with_each_other() {
        assert_ne!(SGI_TYPE_BYTE_XFS, SGI_TYPE_BYTE_EFS);
    }

    #[test]
    fn fixture_checksum_logs_but_does_not_reject() {
        // Some SGI images have stale checksums after image-tool round-trips.
        // The parser must accept the fixture regardless of whether its
        // checksum verifies. This test just exercises the path; we don't
        // assert a specific checksum_valid value, only that parsing succeeded.
        let buf = load_irix_volhdr_fixture();
        let _vh = SgiVolumeHeader::parse(&buf).expect("should parse");
    }
}
