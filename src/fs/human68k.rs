//! Human68k filesystem — the FAT-derived OS of the Sharp X68000.
//!
//! The MiSTer X68000 core mounts both 1.2 MB floppy `.d88` / `.xdf`
//! images and SASI HDD `.hdf` images. Both use Human68k's filesystem:
//! a FAT12 (floppy) or FAT16 (HDD) BPB-with-extensions, but the
//! directory entries are **18.3** rather than FAT's 8.3 (Sharp packed
//! 10 extra name bytes into FAT-reserved fields) and filenames are
//! **Shift-JIS** rather than CP437.
//!
//! ## On-disk layout
//!
//! - BPB at the partition's first sector, in one of two conventions
//!   (see [`Human68kBpb::parse`]):
//!     - **Standard FAT** — little-endian fields at offset 11 after a
//!       3-byte jump + 8-byte OEM. X68000 floppies use this.
//!     - **Sharp / Keisoku Giken SCSI-HDD** — *big-endian* fields at
//!       offset 0x12 after a 2-byte BRA.S + 16-byte OEM (e.g.
//!       "SHARP/KG    1.00"). Real SCSI hard-disk images (`X68SCSI1`
//!       signature, 1024-byte logical sectors) use this. The FAT table
//!       and directory entries stay little-endian (MS-DOS compatible);
//!       only the BPB header is big-endian.
//! - FAT table immediately after the reserved sectors (DPB style).
//! - Root directory immediately after the FAT (FAT12/16 convention).
//! - Data area after that.
//!
//! ## Directory entry (32 B, little-endian)
//!
//! ```text
//! 0..8    name      8 bytes (Shift-JIS, space-padded)
//! 8..11   ext       3 bytes (Shift-JIS, space-padded)
//! 11      attr      0x10 = dir, 0x08 = volume label, 0x01 = R/O,
//!                   0x20 = archive
//! 12..22  name_ext  10 bytes — extra filename chars (extending the
//!                   8-char name to 18). FAT-equivalent fields are
//!                   reserved+create-time-tenths+create-time+
//!                   create-date+last-access-date.
//! 22..24  time      LE u16 — FAT-style HH:MM:SS (5/6/5 bits)
//! 24..26  date      LE u16 — FAT-style Y:M:D (7/4/5 bits, year+1980)
//! 26..28  cluster   LE u16 — first data cluster
//! 28..32  size      LE u32 — file size in bytes
//! ```
//!
//! Entry 0 first byte == 0x00 means "no more entries", 0xE5 means
//! "deleted slot" (same as FAT).
//!
//! Filenames are Shift-JIS: ASCII, single-byte half-width katakana
//! (`0xA1..0xDF`), and double-byte kanji/kana (first byte `0x81..0x9F` /
//! `0xE0..0xFC`, second `0x40..0xFC` excl. `0x7F`). We decode to UTF-8 for
//! display and encode UTF-8 back to Shift-JIS on write via `encoding_rs`,
//! so Japanese names round-trip; the raw bytes are also retained on the
//! parsed entry. Length limits are counted in Shift-JIS bytes (18 for the
//! name, 3 for the extension).

use std::io::{Read, Seek, SeekFrom, Write};

use anyhow::Context as _;
use byteorder::{BigEndian, ByteOrder, LittleEndian};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

/// 18.3 filename: 8 name + 10 name-extension + 3 ext = 21 + dot.
pub const MAX_NAME_LEN: usize = 21;

/// Bytes per FAT directory entry. Same as FAT.
const DIR_ENTRY_SIZE: usize = 32;

/// File attribute bits (FAT-compatible). Some are kept for future
/// use even when our extract floor doesn't surface them yet.
#[allow(dead_code)]
mod attr {
    pub const READ_ONLY: u8 = 0x01;
    pub const HIDDEN: u8 = 0x02;
    pub const SYSTEM: u8 = 0x04;
    pub const VOLUME_LABEL: u8 = 0x08;
    pub const DIRECTORY: u8 = 0x10;
    pub const ARCHIVE: u8 = 0x20;
}

/// Parsed BPB (BIOS Parameter Block) — shape matches FAT12/16 since
/// Human68k is FAT-derived.
#[derive(Debug, Clone, Copy)]
pub struct Human68kBpb {
    pub bytes_per_sector: u16,
    pub sectors_per_cluster: u8,
    pub reserved_sectors: u16,
    pub num_fats: u8,
    pub root_entries: u16,
    pub total_sectors: u32,
    pub fat_sectors: u16,
    pub fat_kind: FatKind,
    /// True when FAT entries are stored **big-endian** (the Sharp / Keisoku
    /// Giken SCSI/SASI HDD convention). Floppies and standard-FAT BPBs use
    /// little-endian. Directory entries are little-endian on every variant;
    /// only the FAT table itself differs. A FAT16 entry read with the wrong
    /// endianness byteswaps the next-cluster link, truncating multi-cluster
    /// files — short files appear fine because `read_file` returns `size`
    /// bytes regardless of chain length.
    pub fat_big_endian: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FatKind {
    Fat12,
    Fat16,
}

impl Human68kBpb {
    pub fn parse(bpb_buf: &[u8; 512]) -> Result<Self, FilesystemError> {
        // Human68k has two on-disk BPB conventions:
        //
        // 1. Standard MS-DOS FAT BPB — little-endian, fields at offset 11
        //    (after a 3-byte jump + 8-byte OEM). X68000 floppies and our
        //    synthetic test images use this.
        //
        // 2. Sharp / Keisoku Giken SCSI-HDD BPB — big-endian, fields at
        //    offset 0x12 (after a 2-byte BRA.S + 16-byte OEM such as
        //    "SHARP/KG    1.00"). Real BlueSCSI / SxSI hard-disk images
        //    (`X68SCSI1` signature, 1024-byte sectors) use this.
        //
        // The two layouts are incompatible (different field offsets *and*
        // endianness), so the SCSI-HDD form was rejected with
        // "bytes_per_sector 0 / 8224 not in {...}" before this path
        // existed. Prefer the standard layout whenever its
        // bytes-per-sector field is already valid — that keeps every
        // floppy / test image on the exact code path it had before — and
        // only fall back to the Sharp/KG layout when the standard field is
        // invalid and the disk carries the 0x60 (BRA.S) boot opcode with a
        // sane big-endian sector size at 0x12.
        let std_bps = LittleEndian::read_u16(&bpb_buf[11..13]);
        if !matches!(std_bps, 256 | 512 | 1024 | 2048) && bpb_buf[0] == 0x60 {
            let be_bps = BigEndian::read_u16(&bpb_buf[0x12..0x14]);
            if matches!(be_bps, 256 | 512 | 1024 | 2048) {
                return Self::parse_sharp_kg(bpb_buf);
            }
        }
        Self::from_fields(
            std_bps,
            bpb_buf[13],
            LittleEndian::read_u16(&bpb_buf[14..16]),
            bpb_buf[16],
            LittleEndian::read_u16(&bpb_buf[17..19]),
            LittleEndian::read_u16(&bpb_buf[19..21]) as u32,
            LittleEndian::read_u16(&bpb_buf[22..24]),
            LittleEndian::read_u32(&bpb_buf[32..36]),
            false, // standard FAT BPB -> little-endian FAT table
        )
    }

    /// Parse the Sharp / Keisoku Giken SCSI-HDD BPB (big-endian, fields at
    /// offset 0x12). Layout verified byte-for-byte against real BlueSCSI
    /// `X68SCSI1` images (HD10/HD20):
    ///
    /// ```text
    /// 0x12  u16 BE  bytes per sector      (1024)
    /// 0x14  u8      sectors per cluster   (16 / 32)
    /// 0x15  u8      number of FATs        (2)
    /// 0x16  u16 BE  reserved sectors      (1)
    /// 0x18  u16 BE  root dir entries      (512)
    /// 0x1A  u16 BE  total sectors (16-bit, 0 when 32-bit form is used)
    /// 0x1C  u8      media descriptor      (0xF7)
    /// 0x1D  u8      sectors per FAT       (118 / 120)
    /// 0x1E  u32 BE  total sectors (32-bit) == X68K partition length
    /// ```
    ///
    /// The FAT table on these disks is **big-endian** too (verified against
    /// real images: contiguous files give sequential BE next-cluster
    /// links); directory entries stay little-endian.
    fn parse_sharp_kg(bpb_buf: &[u8; 512]) -> Result<Self, FilesystemError> {
        Self::from_fields(
            BigEndian::read_u16(&bpb_buf[0x12..0x14]),
            bpb_buf[0x14],
            BigEndian::read_u16(&bpb_buf[0x16..0x18]),
            bpb_buf[0x15],
            BigEndian::read_u16(&bpb_buf[0x18..0x1A]),
            BigEndian::read_u16(&bpb_buf[0x1A..0x1C]) as u32,
            bpb_buf[0x1D] as u16,
            BigEndian::read_u32(&bpb_buf[0x1E..0x22]),
            true, // Sharp/KG HDD -> big-endian FAT table
        )
    }

    /// Validate the raw BPB fields (shared by both layouts) and derive the
    /// FAT12/16 sub-type from the cluster count.
    #[allow(clippy::too_many_arguments)]
    fn from_fields(
        bytes_per_sector: u16,
        sectors_per_cluster: u8,
        reserved_sectors: u16,
        num_fats: u8,
        root_entries: u16,
        small_total: u32,
        fat_sectors: u16,
        big_total: u32,
        fat_big_endian: bool,
    ) -> Result<Self, FilesystemError> {
        if !matches!(bytes_per_sector, 256 | 512 | 1024 | 2048) {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k BPB bytes_per_sector {bytes_per_sector} not in {{256,512,1024,2048}}"
            )));
        }
        if sectors_per_cluster == 0 || !sectors_per_cluster.is_power_of_two() {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k BPB sectors_per_cluster {sectors_per_cluster} must be a power of two"
            )));
        }
        if num_fats == 0 || num_fats > 2 {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k BPB num_fats {num_fats} not 1 or 2"
            )));
        }
        let total_sectors = if small_total != 0 {
            small_total
        } else {
            big_total
        };

        // Cluster count drives FAT12 vs FAT16 decision (same as FAT spec).
        let root_dir_sectors = (root_entries as u32 * 32).div_ceil(bytes_per_sector as u32);
        let data_sectors = total_sectors
            .saturating_sub(reserved_sectors as u32)
            .saturating_sub(num_fats as u32 * fat_sectors as u32)
            .saturating_sub(root_dir_sectors);
        let cluster_count = data_sectors / sectors_per_cluster as u32;
        let fat_kind = if cluster_count < 4085 {
            FatKind::Fat12
        } else {
            FatKind::Fat16
        };
        Ok(Self {
            bytes_per_sector,
            sectors_per_cluster,
            reserved_sectors,
            num_fats,
            root_entries,
            total_sectors,
            fat_sectors,
            fat_kind,
            fat_big_endian,
        })
    }

    /// First sector of the FAT (1-based).
    pub fn fat_start_sector(&self) -> u32 {
        self.reserved_sectors as u32
    }

    /// First sector of the root directory.
    pub fn root_dir_sector(&self) -> u32 {
        self.fat_start_sector() + self.num_fats as u32 * self.fat_sectors as u32
    }

    /// First sector of the data area.
    pub fn data_start_sector(&self) -> u32 {
        let root_sectors = (self.root_entries as u32 * 32).div_ceil(self.bytes_per_sector as u32);
        self.root_dir_sector() + root_sectors
    }

    /// Bytes per cluster.
    pub fn cluster_size(&self) -> u32 {
        self.sectors_per_cluster as u32 * self.bytes_per_sector as u32
    }
}

/// Decoded directory entry.
#[derive(Debug, Clone)]
pub struct Human68kDirEntry {
    /// Raw filename bytes (8 name + 10 ext-name + 3 ext = 21 bytes
    /// max), trimmed of trailing 0x00 / 0x20 padding.
    pub raw_name: Vec<u8>,
    /// 4-byte combined name + 3-byte ext (the trailing 3 bytes only).
    pub raw_ext: Vec<u8>,
    /// Display form: ASCII verbatim, Shift-JIS pairs as `_` placeholders.
    pub display_name: String,
    pub attr: u8,
    pub time: u16,
    pub date: u16,
    pub first_cluster: u16,
    pub size: u32,
}

impl Human68kDirEntry {
    /// True if this slot is in use (not deleted and not end-of-list).
    pub fn is_active(&self) -> bool {
        !self.raw_name.is_empty()
    }

    pub fn is_directory(&self) -> bool {
        self.attr & attr::DIRECTORY != 0
    }

    pub fn is_volume_label(&self) -> bool {
        self.attr & attr::VOLUME_LABEL != 0
    }
}

/// Parse a 32-byte directory entry. Returns `None` for end-of-list
/// (first byte == 0x00) and for deleted slots (first byte == 0xE5).
pub fn parse_dir_entry(buf: &[u8; DIR_ENTRY_SIZE]) -> Option<Human68kDirEntry> {
    if buf[0] == 0x00 || buf[0] == 0xE5 {
        return None;
    }
    let name_bytes = &buf[0..8];
    let name_ext_bytes = &buf[12..22];
    let ext_bytes = &buf[8..11];
    let attr_byte = buf[11];
    let time = LittleEndian::read_u16(&buf[22..24]);
    let date = LittleEndian::read_u16(&buf[24..26]);
    let first_cluster = LittleEndian::read_u16(&buf[26..28]);
    let size = LittleEndian::read_u32(&buf[28..32]);

    // Combine the 8-byte name + 10-byte extended name. Trim trailing
    // spaces and zeros from the right.
    let mut name = name_bytes.to_vec();
    name.extend_from_slice(name_ext_bytes);
    while name.last().is_some_and(|&b| b == 0x20 || b == 0x00) {
        name.pop();
    }
    let mut ext = ext_bytes.to_vec();
    while ext.last().is_some_and(|&b| b == 0x20 || b == 0x00) {
        ext.pop();
    }

    let display_name = shift_jis_lossy_display(&name, &ext);

    Some(Human68kDirEntry {
        raw_name: name,
        raw_ext: ext,
        display_name,
        attr: attr_byte,
        time,
        date,
        first_cluster,
        size,
    })
}

/// Decode a Human68k name + extension from Shift-JIS to UTF-8 for display.
/// Handles the full Shift-JIS range — ASCII, single-byte half-width
/// katakana (0xA1..0xDF), and double-byte kanji/kana — via `encoding_rs`.
/// Undecodable bytes become U+FFFD. Round-trip writes still go through the
/// raw `raw_name` / `raw_ext` bytes preserved on the entry.
fn shift_jis_lossy_display(name: &[u8], ext: &[u8]) -> String {
    let n = decode_shift_jis(name);
    let e = decode_shift_jis(ext);
    if e.is_empty() {
        n
    } else {
        format!("{n}.{e}")
    }
}

/// Decode a Shift-JIS byte string to a UTF-8 `String`.
fn decode_shift_jis(bytes: &[u8]) -> String {
    encoding_rs::SHIFT_JIS.decode(bytes).0.into_owned()
}

/// Live Human68k reader.
pub struct Human68kFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    pub(crate) bpb: Human68kBpb,
    fat_bytes: Vec<u8>,
}

impl<R: Read + Seek + Send> Human68kFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;
        let mut bpb_buf = [0u8; 512];
        reader.read_exact(&mut bpb_buf)?;
        let bpb = Human68kBpb::parse(&bpb_buf)?;

        let fat_byte_len = bpb.fat_sectors as u64 * bpb.bytes_per_sector as u64;
        let fat_off =
            partition_offset + bpb.fat_start_sector() as u64 * bpb.bytes_per_sector as u64;
        reader.seek(SeekFrom::Start(fat_off))?;
        let mut fat_bytes = vec![0u8; fat_byte_len as usize];
        reader.read_exact(&mut fat_bytes)?;

        Ok(Self {
            reader,
            partition_offset,
            bpb,
            fat_bytes,
        })
    }

    /// FAT chain follow: given a starting cluster, return the list of
    /// cluster numbers until we hit the end-of-chain sentinel.
    fn cluster_chain(&self, first: u16) -> Result<Vec<u16>, FilesystemError> {
        let mut out = Vec::new();
        let mut cluster = first;
        let mut visited = std::collections::HashSet::new();
        let cap = self.bpb.total_sectors as usize / self.bpb.sectors_per_cluster as usize + 16;
        while cluster >= 2 && (cluster as usize) < cap {
            if !visited.insert(cluster) {
                return Err(FilesystemError::InvalidData(format!(
                    "Human68k FAT cycle at cluster {cluster}"
                )));
            }
            out.push(cluster);
            let next = self.fat_lookup(cluster);
            if self.is_end_of_chain(next) {
                break;
            }
            cluster = next;
        }
        Ok(out)
    }

    fn fat_lookup(&self, cluster: u16) -> u16 {
        match self.bpb.fat_kind {
            FatKind::Fat12 => {
                let off = (cluster as usize * 3) / 2;
                if off + 1 >= self.fat_bytes.len() {
                    return 0xFFF;
                }
                let lo = self.fat_bytes[off] as u16;
                let hi = self.fat_bytes[off + 1] as u16;
                if cluster.is_multiple_of(2) {
                    lo | ((hi & 0x0F) << 8)
                } else {
                    (lo >> 4) | (hi << 4)
                }
            }
            FatKind::Fat16 => {
                let off = cluster as usize * 2;
                if off + 1 >= self.fat_bytes.len() {
                    return 0xFFFF;
                }
                let bytes = [self.fat_bytes[off], self.fat_bytes[off + 1]];
                if self.bpb.fat_big_endian {
                    u16::from_be_bytes(bytes)
                } else {
                    u16::from_le_bytes(bytes)
                }
            }
        }
    }

    fn is_end_of_chain(&self, v: u16) -> bool {
        match self.bpb.fat_kind {
            FatKind::Fat12 => v >= 0xFF8,
            FatKind::Fat16 => v >= 0xFFF8,
        }
    }

    /// Byte offset of a data cluster's first sector.
    fn cluster_byte_offset(&self, cluster: u16) -> u64 {
        let data_sector = self.bpb.data_start_sector() as u64
            + (cluster as u64 - 2) * self.bpb.sectors_per_cluster as u64;
        self.partition_offset + data_sector * self.bpb.bytes_per_sector as u64
    }

    /// Consume the filesystem and hand back the underlying reader. Used by
    /// the defragmenting clone's streaming wrapper to drain a cloned
    /// tempfile after the volume has been written.
    pub(crate) fn into_reader(self) -> R {
        self.reader
    }

    /// Capture this volume's on-disk format (the reserved region — boot
    /// sector + BPB — plus the parsed BPB) so [`create_blank_human68k`]
    /// can re-emit a blank target in the exact same shape (sector size,
    /// FAT count, endianness, OEM string, boot stub) at a new size.
    pub fn format_template(&mut self) -> Result<Human68kFormatTemplate, FilesystemError> {
        let reserved_len = self.bpb.reserved_sectors as usize * self.bpb.bytes_per_sector as usize;
        self.reader.seek(SeekFrom::Start(self.partition_offset))?;
        let mut reserved_region = vec![0u8; reserved_len.max(512)];
        self.reader.read_exact(&mut reserved_region)?;
        Ok(Human68kFormatTemplate {
            reserved_region,
            bpb: self.bpb,
        })
    }

    /// Read root-directory sectors and parse all 32-byte entries.
    fn read_root_directory(&mut self) -> Result<Vec<Human68kDirEntry>, FilesystemError> {
        let root_off = self.partition_offset
            + self.bpb.root_dir_sector() as u64 * self.bpb.bytes_per_sector as u64;
        let root_byte_len = self.bpb.root_entries as u64 * DIR_ENTRY_SIZE as u64;
        self.reader.seek(SeekFrom::Start(root_off))?;
        let mut buf = vec![0u8; root_byte_len as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(parse_directory_buffer(&buf))
    }

    /// Read a subdirectory by following its cluster chain from
    /// `first_cluster`, parsing 32-byte entries across cluster boundaries.
    /// Stops at the end-of-directory marker (`0x00` first byte) or when the
    /// chain terminates.
    fn read_subdirectory(
        &mut self,
        first_cluster: u16,
    ) -> Result<Vec<Human68kDirEntry>, FilesystemError> {
        let chain = self.cluster_chain(first_cluster)?;
        let csize = self.bpb.cluster_size() as usize;
        let mut buf = Vec::with_capacity(chain.len() * csize);
        for cluster in chain {
            let off = self.cluster_byte_offset(cluster);
            self.reader.seek(SeekFrom::Start(off))?;
            let mut cbuf = vec![0u8; csize];
            self.reader.read_exact(&mut cbuf)?;
            buf.extend_from_slice(&cbuf);
        }
        Ok(parse_directory_buffer(&buf))
    }
}

/// Write a `.` or `..` self/parent link into a 32-byte directory slot.
/// `name8` is the space-padded 8-byte name (`b".       "` / `b"..      "`),
/// `cluster` the first cluster it points at (0 for the parent of a
/// root-level directory, per FAT convention).
fn write_dot_entry(slot: &mut [u8], name8: &[u8; 8], cluster: u16) {
    slot[..DIR_ENTRY_SIZE].fill(0);
    slot[0..8].copy_from_slice(name8);
    slot[8..11].copy_from_slice(b"   ");
    slot[11] = attr::DIRECTORY;
    LittleEndian::write_u16(&mut slot[26..28], cluster);
    LittleEndian::write_u32(&mut slot[28..32], 0);
}

/// Parse a directory region (root or subdirectory) into active entries.
/// Skips deleted slots (`0xE5`), volume labels, and the `.` / `..`
/// self/parent links; stops at the end-of-directory marker (`0x00`).
fn parse_directory_buffer(buf: &[u8]) -> Vec<Human68kDirEntry> {
    let mut out = Vec::new();
    for slot in buf.chunks_exact(DIR_ENTRY_SIZE) {
        if slot[0] == 0x00 {
            break; // end of directory
        }
        if slot[0] == 0xE5 {
            continue; // deleted
        }
        let arr: &[u8; DIR_ENTRY_SIZE] = slot.try_into().unwrap();
        if let Some(e) = parse_dir_entry(arr) {
            if e.is_volume_label() || e.display_name == "." || e.display_name == ".." {
                continue;
            }
            out.push(e);
        }
    }
    out
}

impl<R: Read + Seek + Send> Filesystem for Human68kFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        // Root directory lives in the fixed root region (location 0);
        // subdirectories are walked through their cluster chain.
        let dir = if entry.path == "/" {
            self.read_root_directory()?
        } else {
            self.read_subdirectory(entry.location as u16)?
        };
        let parent = if entry.path == "/" {
            String::new()
        } else {
            entry.path.trim_end_matches('/').to_string()
        };
        let mut out = Vec::with_capacity(dir.len());
        for de in dir {
            let name = de.display_name.clone();
            let path = format!("{parent}/{name}");
            let mut fe = if de.is_directory() {
                FileEntry::new_directory(name.clone(), path, de.first_cluster as u64)
            } else {
                FileEntry::new_file(name.clone(), path, de.size as u64, de.first_cluster as u64)
            };
            if de.attr & attr::READ_ONLY != 0 {
                fe.special_type = Some("R/O".to_string());
            }
            out.push(fe);
        }
        out.sort_by_key(|a| a.name.to_lowercase());
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let first = entry.location as u16;
        let chain = self.cluster_chain(first)?;
        let size = entry.size as usize;
        let mut data = Vec::with_capacity(size);
        let csize = self.bpb.cluster_size() as usize;
        let mut remaining = size;
        for cluster in chain {
            if remaining == 0 {
                break;
            }
            let want = csize.min(remaining);
            let off = self.cluster_byte_offset(cluster);
            self.reader.seek(SeekFrom::Start(off))?;
            let mut buf = vec![0u8; want];
            self.reader.read_exact(&mut buf)?;
            data.extend_from_slice(&buf);
            remaining = remaining.saturating_sub(want);
        }
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        match self.bpb.fat_kind {
            FatKind::Fat12 => "Human68k (FAT12)",
            FatKind::Fat16 => "Human68k (FAT16)",
        }
    }

    fn volume_label(&self) -> Option<&str> {
        None // Volume label entry decoding deferred — extract floor.
    }

    fn total_size(&self) -> u64 {
        self.bpb.total_sectors as u64 * self.bpb.bytes_per_sector as u64
    }

    fn used_size(&self) -> u64 {
        // Conservative: count clusters whose FAT entry is non-zero.
        let csize = self.bpb.cluster_size() as u64;
        let max_cluster =
            ((self.bpb.total_sectors / self.bpb.sectors_per_cluster as u32) + 2).min(0xFFF8) as u16;
        let mut used: u64 = 0;
        for c in 2..max_cluster {
            if self.fat_lookup(c) != 0 {
                used += csize;
            }
        }
        used
    }
}

// ============================================================================
// EditableFilesystem — Add/Delete on the root directory only
// ============================================================================

/// 8 + 10 + 3 byte encoded name slot returned by `encode_human68k_name`.
type EncodedName = ([u8; 8], [u8; 10], [u8; 3]);

/// Encode a UTF-8 filename into the 8 + 10 + 3 byte fields of a Human68k
/// directory entry, transcoding to Shift-JIS. Accepts Japanese (the CLI /
/// GUI pass names as UTF-8); rejects path separators and names that don't
/// fit the 18-byte name / 3-byte extension SJIS budget. Length limits are
/// in **Shift-JIS bytes** (a kanji is 2 bytes, half-width katakana 1).
fn encode_human68k_name(name: &str) -> Result<EncodedName, FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData(
            "Human68k filename is empty".into(),
        ));
    }
    if name.contains('/') || name.contains('\\') || name.contains(':') {
        return Err(FilesystemError::InvalidData(format!(
            "Human68k filename '{name}' contains a path separator"
        )));
    }
    // Split at the LAST '.' for the extension (rightmost-dot convention).
    // '.' is ASCII so splitting the &str is byte-safe.
    let (name_part, ext_part) = match name.rfind('.') {
        Some(idx) => (&name[..idx], &name[idx + 1..]),
        None => (name, ""),
    };
    let name_sjis = encode_shift_jis(name_part, name)?;
    let ext_sjis = encode_shift_jis(ext_part, name)?;

    if name_sjis.is_empty() || name_sjis.len() > 18 {
        return Err(FilesystemError::InvalidData(format!(
            "Human68k name '{name}' must be 1..=18 Shift-JIS bytes before the extension"
        )));
    }
    if ext_sjis.len() > 3 {
        return Err(FilesystemError::InvalidData(format!(
            "Human68k extension of '{name}' exceeds 3 Shift-JIS bytes (got {})",
            ext_sjis.len()
        )));
    }
    let mut n8 = [b' '; 8];
    let mut ne10 = [b' '; 10];
    let take = name_sjis.len().min(8);
    n8[..take].copy_from_slice(&name_sjis[..take]);
    if name_sjis.len() > 8 {
        let etake = name_sjis.len() - 8;
        ne10[..etake].copy_from_slice(&name_sjis[8..8 + etake]);
    }
    let mut e3 = [b' '; 3];
    e3[..ext_sjis.len()].copy_from_slice(&ext_sjis);
    Ok((n8, ne10, e3))
}

/// Transcode a UTF-8 fragment to Shift-JIS, rejecting characters that have
/// no Shift-JIS mapping (so we never silently write `?` substitutions into
/// an on-disk name). `full` is the original name, for the error message.
fn encode_shift_jis(part: &str, full: &str) -> Result<Vec<u8>, FilesystemError> {
    let (bytes, _, had_unmappable) = encoding_rs::SHIFT_JIS.encode(part);
    if had_unmappable {
        return Err(FilesystemError::InvalidData(format!(
            "Human68k filename '{full}' contains characters with no Shift-JIS mapping"
        )));
    }
    Ok(bytes.into_owned())
}

impl<R: Read + Write + Seek + Send> Human68kFilesystem<R> {
    /// Write the cached FAT bytes back to both FAT copies on disk.
    fn fat_write_back(&mut self) -> Result<(), FilesystemError> {
        let fat_byte_len = self.fat_bytes.len();
        let bps = self.bpb.bytes_per_sector as u64;
        for fat_idx in 0..self.bpb.num_fats {
            let off = self.partition_offset
                + (self.bpb.fat_start_sector() as u64
                    + fat_idx as u64 * self.bpb.fat_sectors as u64)
                    * bps;
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&self.fat_bytes[..fat_byte_len])?;
        }
        Ok(())
    }

    /// Set a 12- or 16-bit FAT entry.
    fn fat_set(&mut self, cluster: u16, value: u16) {
        match self.bpb.fat_kind {
            FatKind::Fat12 => {
                let off = (cluster as usize * 3) / 2;
                if off + 1 >= self.fat_bytes.len() {
                    return;
                }
                let v = value & 0x0FFF;
                if cluster.is_multiple_of(2) {
                    self.fat_bytes[off] = (v & 0xFF) as u8;
                    self.fat_bytes[off + 1] =
                        (self.fat_bytes[off + 1] & 0xF0) | ((v >> 8) as u8 & 0x0F);
                } else {
                    self.fat_bytes[off] = (self.fat_bytes[off] & 0x0F) | ((v & 0x0F) as u8) << 4;
                    self.fat_bytes[off + 1] = (v >> 4) as u8;
                }
            }
            FatKind::Fat16 => {
                let off = cluster as usize * 2;
                if off + 1 >= self.fat_bytes.len() {
                    return;
                }
                let encoded = if self.bpb.fat_big_endian {
                    value.to_be_bytes()
                } else {
                    value.to_le_bytes()
                };
                self.fat_bytes[off..off + 2].copy_from_slice(&encoded);
            }
        }
    }

    /// Allocate a chain of `count` clusters. Returns the first cluster
    /// or `InvalidData` if the volume is full.
    fn alloc_chain(&mut self, count: u32) -> Result<u16, FilesystemError> {
        if count == 0 {
            return Ok(0);
        }
        let max_cluster =
            ((self.bpb.total_sectors / self.bpb.sectors_per_cluster as u32) + 2).min(0xFFF8) as u16;
        let mut picked: Vec<u16> = Vec::with_capacity(count as usize);
        for c in 2..max_cluster {
            if self.fat_lookup(c) == 0 {
                picked.push(c);
                if picked.len() as u32 == count {
                    break;
                }
            }
        }
        if (picked.len() as u32) < count {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k volume full: need {count} clusters, found {}",
                picked.len()
            )));
        }
        for w in picked.windows(2) {
            self.fat_set(w[0], w[1]);
        }
        let last = *picked.last().unwrap();
        let end_marker = match self.bpb.fat_kind {
            FatKind::Fat12 => 0xFFF,
            FatKind::Fat16 => 0xFFFF,
        };
        self.fat_set(last, end_marker);
        Ok(picked[0])
    }

    /// Walk a chain and zero every entry along the way.
    fn free_chain(&mut self, first: u16) -> Result<(), FilesystemError> {
        let chain = self.cluster_chain(first)?;
        for c in chain {
            self.fat_set(c, 0);
        }
        Ok(())
    }

    /// Write payload bytes into clusters starting at `first`.
    fn write_chain(&mut self, first: u16, data: &[u8]) -> Result<(), FilesystemError> {
        let cs = self.bpb.cluster_size() as usize;
        let mut written = 0usize;
        let chain = self.cluster_chain(first)?;
        for c in chain {
            if written >= data.len() {
                break;
            }
            let want = cs.min(data.len() - written);
            let off = self.cluster_byte_offset(c);
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&data[written..written + want])?;
            // Pad the rest of this cluster with zeros.
            if want < cs {
                let pad = vec![0u8; cs - want];
                self.reader.write_all(&pad)?;
            }
            written += want;
        }
        Ok(())
    }

    /// Byte offsets of every 32-byte directory slot for the directory
    /// whose first cluster is `first_cluster` (0 = the fixed root
    /// directory). Subdirectories are walked through their cluster chain.
    fn dir_slot_offsets(&self, first_cluster: u16) -> Result<Vec<u64>, FilesystemError> {
        if first_cluster == 0 {
            let root_off = self.partition_offset
                + self.bpb.root_dir_sector() as u64 * self.bpb.bytes_per_sector as u64;
            let n = self.bpb.root_entries as u64;
            Ok((0..n)
                .map(|i| root_off + i * DIR_ENTRY_SIZE as u64)
                .collect())
        } else {
            let chain = self.cluster_chain(first_cluster)?;
            let per_cluster = self.bpb.cluster_size() as u64 / DIR_ENTRY_SIZE as u64;
            let mut out = Vec::with_capacity(chain.len() * per_cluster as usize);
            for c in chain {
                let base = self.cluster_byte_offset(c);
                for i in 0..per_cluster {
                    out.push(base + i * DIR_ENTRY_SIZE as u64);
                }
            }
            Ok(out)
        }
    }

    /// Grow a subdirectory by one cluster: allocate it, link the current
    /// last cluster to it, and zero-fill it on disk. Returns the new
    /// cluster. The FAT mutations are in-memory until `fat_write_back`.
    fn grow_dir_chain(&mut self, first_cluster: u16) -> Result<u16, FilesystemError> {
        let chain = self.cluster_chain(first_cluster)?;
        let last = *chain.last().ok_or_else(|| {
            FilesystemError::InvalidData("Human68k directory has an empty cluster chain".into())
        })?;
        let new_cluster = self.alloc_chain(1)?;
        self.fat_set(last, new_cluster);
        let off = self.cluster_byte_offset(new_cluster);
        self.reader.seek(SeekFrom::Start(off))?;
        let zeros = vec![0u8; self.bpb.cluster_size() as usize];
        self.reader.write_all(&zeros)?;
        Ok(new_cluster)
    }

    /// Find a free 32-byte directory slot in `first_cluster` (0 = root).
    /// Subdirectories that are full are grown by one cluster; the root
    /// directory is fixed-size and errors when full.
    fn find_free_slot_in_dir(&mut self, first_cluster: u16) -> Result<u64, FilesystemError> {
        for off in self.dir_slot_offsets(first_cluster)? {
            self.reader.seek(SeekFrom::Start(off))?;
            let mut b = [0u8; 1];
            self.reader.read_exact(&mut b)?;
            if b[0] == 0x00 || b[0] == 0xE5 {
                return Ok(off);
            }
        }
        if first_cluster == 0 {
            return Err(FilesystemError::InvalidData(
                "Human68k root directory is full".into(),
            ));
        }
        let new_cluster = self.grow_dir_chain(first_cluster)?;
        Ok(self.cluster_byte_offset(new_cluster))
    }

    /// Find the slot byte offset of the entry named `name` in directory
    /// `first_cluster` (0 = root). Case-insensitive, matches the browse /
    /// list display name.
    fn find_entry_slot_in_dir(
        &mut self,
        first_cluster: u16,
        name: &str,
    ) -> Result<Option<u64>, FilesystemError> {
        for off in self.dir_slot_offsets(first_cluster)? {
            self.reader.seek(SeekFrom::Start(off))?;
            let mut buf = [0u8; DIR_ENTRY_SIZE];
            self.reader.read_exact(&mut buf)?;
            if buf[0] == 0x00 {
                break;
            }
            if buf[0] == 0xE5 {
                continue;
            }
            if let Some(de) = parse_dir_entry(&buf) {
                if de.display_name.eq_ignore_ascii_case(name) {
                    return Ok(Some(off));
                }
            }
        }
        Ok(None)
    }

    /// True if directory `first_cluster` contains no live entries other
    /// than the `.` / `..` self/parent links and volume label.
    fn dir_is_empty(&mut self, first_cluster: u16) -> Result<bool, FilesystemError> {
        let chain = self.cluster_chain(first_cluster)?;
        let cs = self.bpb.cluster_size() as usize;
        for c in chain {
            self.reader
                .seek(SeekFrom::Start(self.cluster_byte_offset(c)))?;
            let mut buf = vec![0u8; cs];
            self.reader.read_exact(&mut buf)?;
            for slot in buf.chunks_exact(DIR_ENTRY_SIZE) {
                if slot[0] == 0x00 {
                    return Ok(true); // end of directory
                }
                if slot[0] == 0xE5 {
                    continue;
                }
                let arr: &[u8; DIR_ENTRY_SIZE] = slot.try_into().unwrap();
                if let Some(de) = parse_dir_entry(arr) {
                    if !de.is_volume_label() && de.display_name != "." && de.display_name != ".." {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    /// Resolve a parent `FileEntry` to its directory's first cluster
    /// (0 for the root). Builds child paths consistently for both.
    fn parent_cluster_and_path(parent: &FileEntry) -> (u16, String) {
        if parent.path == "/" {
            (0, String::new())
        } else {
            (
                parent.location as u16,
                parent.path.trim_end_matches('/').to_string(),
            )
        }
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for Human68kFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let (parent_cluster, parent_path) = Self::parent_cluster_and_path(parent);
        let (n8, ne10, e3) = encode_human68k_name(name)?;
        // Reject duplicates within the target directory.
        if self.find_entry_slot_in_dir(parent_cluster, name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(format!(
                "Human68k file '{name}' already exists"
            )));
        }
        if data_len > u32::MAX as u64 {
            return Err(FilesystemError::InvalidData(
                "Human68k file exceeds u32::MAX bytes".into(),
            ));
        }
        let mut payload = Vec::with_capacity(data_len as usize);
        data.read_to_end(&mut payload)?;
        if payload.len() as u64 != data_len {
            return Err(FilesystemError::InvalidData(format!(
                "data_len {data_len} != actual {}",
                payload.len()
            )));
        }
        let cs = self.bpb.cluster_size() as u64;
        let cluster_count = if payload.is_empty() {
            0u32
        } else {
            (payload.len() as u64).div_ceil(cs) as u32
        };
        let first_cluster = if cluster_count > 0 {
            self.alloc_chain(cluster_count)?
        } else {
            0
        };
        if first_cluster != 0 {
            self.write_chain(first_cluster, &payload)?;
        }

        // Stamp a new directory entry into the first free slot of the
        // target directory (growing a subdirectory's chain if needed).
        let slot_off = self.find_free_slot_in_dir(parent_cluster)?;
        let mut entry = [0u8; DIR_ENTRY_SIZE];
        entry[0..8].copy_from_slice(&n8);
        entry[8..11].copy_from_slice(&e3);
        entry[11] = attr::ARCHIVE;
        entry[12..22].copy_from_slice(&ne10);
        LittleEndian::write_u16(&mut entry[26..28], first_cluster);
        LittleEndian::write_u32(&mut entry[28..32], payload.len() as u32);
        self.reader.seek(SeekFrom::Start(slot_off))?;
        self.reader.write_all(&entry)?;

        self.fat_write_back()?;
        self.reader.flush()?;
        Ok(FileEntry::new_file(
            name.to_string(),
            format!("{parent_path}/{name}"),
            payload.len() as u64,
            first_cluster as u64,
        ))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let (parent_cluster, parent_path) = Self::parent_cluster_and_path(parent);
        let (n8, ne10, e3) = encode_human68k_name(name)?;
        if self.find_entry_slot_in_dir(parent_cluster, name)?.is_some() {
            return Err(FilesystemError::AlreadyExists(format!(
                "Human68k entry '{name}' already exists"
            )));
        }

        // A directory occupies one cluster, holding its `.` / `..` links.
        let dir_cluster = self.alloc_chain(1)?;
        let cs = self.bpb.cluster_size() as usize;
        let mut dir_buf = vec![0u8; cs];
        // `.` -> itself, `..` -> parent (0 for root, per FAT convention).
        write_dot_entry(&mut dir_buf[0..DIR_ENTRY_SIZE], b".       ", dir_cluster);
        write_dot_entry(
            &mut dir_buf[DIR_ENTRY_SIZE..2 * DIR_ENTRY_SIZE],
            b"..      ",
            parent_cluster,
        );
        let dir_off = self.cluster_byte_offset(dir_cluster);
        self.reader.seek(SeekFrom::Start(dir_off))?;
        self.reader.write_all(&dir_buf)?;

        // Stamp the directory entry into the parent.
        let slot_off = self.find_free_slot_in_dir(parent_cluster)?;
        let mut entry = [0u8; DIR_ENTRY_SIZE];
        entry[0..8].copy_from_slice(&n8);
        entry[8..11].copy_from_slice(&e3);
        entry[11] = attr::DIRECTORY;
        entry[12..22].copy_from_slice(&ne10);
        LittleEndian::write_u16(&mut entry[26..28], dir_cluster);
        // Directories report size 0 in the entry; the chain is authoritative.
        LittleEndian::write_u32(&mut entry[28..32], 0);
        self.reader.seek(SeekFrom::Start(slot_off))?;
        self.reader.write_all(&entry)?;

        self.fat_write_back()?;
        self.reader.flush()?;
        Ok(FileEntry::new_directory(
            name.to_string(),
            format!("{parent_path}/{name}"),
            dir_cluster as u64,
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let (parent_cluster, _) = Self::parent_cluster_and_path(parent);
        // Directories must be empty (the default `delete_recursive` empties
        // them first); refuse a non-empty directory to match the trait
        // contract.
        if entry.entry_type == EntryType::Directory {
            let dir_cluster = entry.location as u16;
            if dir_cluster != 0 && !self.dir_is_empty(dir_cluster)? {
                return Err(FilesystemError::InvalidData(format!(
                    "Human68k directory '{}' is not empty",
                    entry.name
                )));
            }
        }

        let slot_off = self
            .find_entry_slot_in_dir(parent_cluster, &entry.name)?
            .ok_or_else(|| {
                FilesystemError::NotFound(format!(
                    "Human68k entry '{}' not found in '{}'",
                    entry.name, parent.path
                ))
            })?;

        let first = entry.location as u16;
        if first != 0 {
            self.free_chain(first)?;
        }
        self.reader.seek(SeekFrom::Start(slot_off))?;
        self.reader.write_all(&[0xE5])?;
        self.fat_write_back()?;
        self.reader.flush()?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let max_cluster =
            ((self.bpb.total_sectors / self.bpb.sectors_per_cluster as u32) + 2).min(0xFFF8) as u16;
        let mut free: u64 = 0;
        for c in 2..max_cluster {
            if self.fat_lookup(c) == 0 {
                free += self.bpb.cluster_size() as u64;
            }
        }
        Ok(free)
    }
}

// ============================================================================
// In-place filesystem resize (SHARP / Keisoku Giken big-endian HDD BPB)
// ============================================================================

/// Sectors-per-FAT needed to address every cluster of a FAT16 volume of
/// `total_sectors`. Closed form, matching `fat::compute_fat_sectors` for
/// `fat_bits == 16`.
pub(crate) fn human68k_fat16_sectors(
    total_sectors: u32,
    reserved: u32,
    num_fats: u32,
    root_dir_sectors: u32,
    spc: u32,
    bps: u32,
) -> u32 {
    let avail = total_sectors.saturating_sub(reserved + root_dir_sectors) as u64;
    let spc = spc as u64;
    // ceil(2 * (avail + 2*spc) / (bps*spc + 2*num_fats))
    let num = 2 * (avail + 2 * spc);
    let den = bps as u64 * spc + 2 * num_fats as u64;
    num.div_ceil(den) as u32
}

/// Shift the byte region `[src_start, src_end)` of `file` forward by `shift`
/// bytes, copying from the end backward so the overlapping source/destination
/// ranges stay intact, then zero-filling the gap left behind. Local peer of
/// `fat::shift_region_forward` — kept here so the Human68k resize is
/// self-contained over the big-endian BPB/FAT the FAT resizer rejects.
fn shift_region_forward(
    file: &mut (impl Read + Write + Seek),
    src_start: u64,
    src_end: u64,
    shift: u64,
) -> anyhow::Result<()> {
    let data_len = src_end.saturating_sub(src_start);
    if data_len == 0 || shift == 0 {
        return Ok(());
    }
    const CHUNK: usize = 1 << 20; // 1 MiB
    let mut buf = vec![0u8; CHUNK];
    let mut remaining = data_len;
    while remaining > 0 {
        let chunk = remaining.min(CHUNK as u64);
        let read_pos = src_start + remaining - chunk;
        file.seek(SeekFrom::Start(read_pos))?;
        file.read_exact(&mut buf[..chunk as usize])?;
        file.seek(SeekFrom::Start(read_pos + shift))?;
        file.write_all(&buf[..chunk as usize])?;
        remaining -= chunk;
    }
    // Zero the gap left in front of the shifted region (the freshly enlarged
    // FAT copies are written over part of it afterward).
    let zeros = vec![0u8; CHUNK];
    let mut gap = shift;
    file.seek(SeekFrom::Start(src_start))?;
    while gap > 0 {
        let n = (gap as usize).min(CHUNK);
        file.write_all(&zeros[..n])?;
        gap -= n as u64;
    }
    Ok(())
}

/// Patch the big-endian SHARP/KG BPB header for a resize: total sectors
/// (16-bit at 0x1A and/or 32-bit at 0x1E) and sectors-per-FAT (one byte at
/// 0x1D). Every other field is left untouched. Takes a `&mut [u8]` (not a
/// fixed array) so both the resize path (a `[u8; 512]` boot buffer) and
/// the blank-volume formatter (a `Vec<u8>` reserved region) can call it.
pub(crate) fn patch_sharp_kg_bpb(bpb_buf: &mut [u8], new_total: u32, new_spf: u8) {
    // Preserve which total-sectors field the disk used: real HDDs are large
    // and keep the 16-bit field zero with the count in the 32-bit field, but
    // honor a small-volume layout that populated the 16-bit field instead.
    let old_total16 = BigEndian::read_u16(&bpb_buf[0x1A..0x1C]);
    if old_total16 != 0 && new_total <= u16::MAX as u32 {
        BigEndian::write_u16(&mut bpb_buf[0x1A..0x1C], new_total as u16);
        BigEndian::write_u32(&mut bpb_buf[0x1E..0x22], 0);
    } else {
        BigEndian::write_u16(&mut bpb_buf[0x1A..0x1C], 0);
        BigEndian::write_u32(&mut bpb_buf[0x1E..0x22], new_total);
    }
    bpb_buf[0x1D] = new_spf;
}

/// Resize the Human68k (SHARP / Keisoku Giken big-endian HDD) filesystem at
/// `partition_offset` so it inhabits `new_size_bytes`.
///
/// This is the Human68k peer of [`crate::fs::fat::resize_fat_in_place`],
/// needed because real X68000 SCSI/SASI hard disks use the Sharp/KG BPB
/// convention that the FAT resizer rejects:
///   - the boot sector starts with `0x60` (BRA.S), not `0xEB` / `0xE9`;
///   - the BPB fields are **big-endian** at offset `0x12`;
///   - the FAT table itself is **big-endian** (FAT16).
///
/// Standard little-endian Human68k floppies are left to
/// `resize_fat_in_place` (this returns `Ok(false)` for them), so the two
/// coexist in the [`crate::fs::resize_filesystem_for`] dispatch chain without
/// double-handling.
///
/// **Grow** extends the FAT — shifting the root directory + data region
/// forward when a larger FAT is needed — and bumps `total_sectors`. **Shrink**
/// keeps the FAT size (so every cluster keeps its byte offset and existing
/// files stay byte-exact) and only reduces `total_sectors`. Returns
/// `Ok(true)` when a resize was performed, `Ok(false)` when the BPB isn't a
/// SHARP/KG HDD BPB or the size already matches.
pub fn resize_human68k_in_place(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_size_bytes: u64,
    log_cb: &mut impl FnMut(&str),
) -> anyhow::Result<bool> {
    // --- 1. Read + validate the SHARP/KG big-endian HDD BPB ---
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut bpb_buf = [0u8; 512];
    file.read_exact(&mut bpb_buf)?;
    // Only the SHARP/KG HDD convention (0x60 BRA.S + big-endian fields +
    // big-endian FAT) is handled here; LE floppy BPBs flow through
    // resize_fat_in_place.
    if bpb_buf[0] != 0x60 {
        return Ok(false);
    }
    let bpb = match Human68kBpb::parse(&bpb_buf) {
        Ok(b) if b.fat_big_endian => b,
        _ => return Ok(false),
    };
    // The big-endian FAT path is FAT16 on every real HDD; FAT12-BE isn't a
    // shape we read, so we don't resize it either.
    if bpb.fat_kind != FatKind::Fat16 {
        return Ok(false);
    }

    let bps = bpb.bytes_per_sector as u32;
    let bps64 = bps as u64;
    let new_total = (new_size_bytes / bps64) as u32;
    let old_total = bpb.total_sectors;
    if new_total == old_total {
        return Ok(false);
    }

    let spc = bpb.sectors_per_cluster as u32;
    let num_fats = bpb.num_fats as u32;
    let reserved = bpb.reserved_sectors as u32;
    let root_entries = bpb.root_entries as u32;
    let root_dir_sectors = (root_entries * 32).div_ceil(bps);
    let old_spf = bpb.fat_sectors as u32;

    let growing = new_total > old_total;

    // Choose the new sectors-per-FAT. Only ever GROW the FAT: on shrink we
    // keep the old (now slightly oversized) FAT so the data-region start
    // sector is unchanged and every existing cluster keeps its byte offset.
    let needed_spf =
        human68k_fat16_sectors(new_total, reserved, num_fats, root_dir_sectors, spc, bps);
    let new_spf = if growing {
        needed_spf.max(old_spf)
    } else {
        old_spf
    };

    // The SHARP/KG BPB stores sectors-per-FAT in a single byte (offset 0x1D).
    if new_spf > u8::MAX as u32 {
        anyhow::bail!(
            "Human68k resize: FAT would need {new_spf} sectors, but the SHARP/KG BPB \
             sectors-per-FAT field is only one byte (max 255)"
        );
    }

    // Refuse a shrink that would drop the cluster count below the FAT16 floor:
    // the on-disk FAT would then be reinterpreted as FAT12 and every chain
    // would decode wrong on the next mount.
    let new_data_start = reserved + num_fats * new_spf + root_dir_sectors;
    let new_clusters = new_total.saturating_sub(new_data_start) / spc;
    if new_clusters < 4085 {
        anyhow::bail!(
            "Human68k resize: target size {new_size_bytes} yields {new_clusters} clusters, \
             below the FAT16 minimum (4085); shrinking a big-endian Human68k volume into \
             FAT12 territory is not supported"
        );
    }

    log_cb(&format!(
        "Human68k FAT16: total sectors {old_total} -> {new_total} ({bps} B/sector), \
         spf {old_spf} -> {new_spf}, clusters -> {new_clusters}"
    ));

    // --- 2. Grow the FAT (shift root+data forward) when more FAT is needed ---
    if growing && new_spf > old_spf {
        let shift_sectors = (new_spf - old_spf) * num_fats;
        let shift_bytes = shift_sectors as u64 * bps64;

        // Read FAT copy 0 (all copies are identical) before moving anything.
        let fat_start = partition_offset + reserved as u64 * bps64;
        file.seek(SeekFrom::Start(fat_start))?;
        let mut fat_data = vec![0u8; old_spf as usize * bps as usize];
        file.read_exact(&mut fat_data)?;

        // Shift the root directory + data region forward to make room for the
        // larger FAT copies. This moves every cluster by exactly `shift_bytes`,
        // which matches the new data-region start, so chains stay valid.
        let move_start = partition_offset + (reserved + num_fats * old_spf) as u64 * bps64;
        let move_end = partition_offset + old_total as u64 * bps64;
        if move_end > move_start {
            shift_region_forward(file, move_start, move_end, shift_bytes)?;
            log_cb(&format!(
                "Shifted root+data region forward by {shift_sectors} sectors"
            ));
        }

        // Extend the FAT with free (zero) entries and write every copy at the
        // new stride. Free == 0x0000 is endian-agnostic, so the big-endian
        // table tolerates zero-padding directly.
        fat_data.resize(new_spf as usize * bps as usize, 0);
        for i in 0..num_fats as u64 {
            let pos = partition_offset + reserved as u64 * bps64 + i * new_spf as u64 * bps64;
            file.seek(SeekFrom::Start(pos))?;
            file.write_all(&fat_data)
                .with_context(|| format!("failed to write Human68k FAT copy {i}"))?;
        }
        file.flush()
            .context("failed to flush Human68k FAT writes")?;
        log_cb(&format!(
            "Extended FAT: {old_spf} -> {new_spf} sectors per copy"
        ));
    } else if growing {
        log_cb("Human68k FAT has spare capacity, no table extension needed");
    }

    // --- 3. Patch the big-endian BPB header (total sectors + spf) ---
    patch_sharp_kg_bpb(&mut bpb_buf, new_total, new_spf as u8);
    file.seek(SeekFrom::Start(partition_offset))?;
    file.write_all(&bpb_buf)
        .context("failed to write updated Human68k BPB")?;
    file.flush().context("failed to flush Human68k BPB")?;
    log_cb("Human68k resize complete");
    Ok(true)
}

// ============================================================================
// Blank-volume formatting (for the defragmenting clone)
// ============================================================================

/// A reusable Human68k format descriptor captured from a source volume via
/// [`Human68kFilesystem::format_template`]. Carries the volume's reserved
/// region (boot sector + BPB, verbatim — so the OEM string, boot stub, and
/// BPB convention survive) plus the parsed BPB. [`create_blank_human68k`]
/// re-emits a blank volume in the same shape at a new size, re-patching
/// only the total-sectors and sectors-per-FAT fields.
#[derive(Debug, Clone)]
pub struct Human68kFormatTemplate {
    /// The reserved region (`reserved_sectors × bytes_per_sector`, at least
    /// 512 bytes), captured verbatim. The BPB in the first 512 bytes is
    /// re-patched for the target size; any boot code after it survives.
    reserved_region: Vec<u8>,
    bpb: Human68kBpb,
}

impl Human68kFormatTemplate {
    /// Sector size of the captured volume, in bytes.
    pub fn bytes_per_sector(&self) -> u32 {
        self.bpb.bytes_per_sector as u32
    }
}

/// Patch a standard little-endian FAT BPB for a new size: total sectors
/// (16-bit at 19, 32-bit at 32) and sectors-per-FAT (16-bit at 22). The
/// peer of [`patch_sharp_kg_bpb`] for the non-SHARP/KG convention.
fn patch_std_bpb(bpb_buf: &mut [u8], new_total: u32, new_spf: u16) {
    let old_total16 = LittleEndian::read_u16(&bpb_buf[19..21]);
    if old_total16 != 0 && new_total <= u16::MAX as u32 {
        LittleEndian::write_u16(&mut bpb_buf[19..21], new_total as u16);
        LittleEndian::write_u32(&mut bpb_buf[32..36], 0);
    } else {
        LittleEndian::write_u16(&mut bpb_buf[19..21], 0);
        LittleEndian::write_u32(&mut bpb_buf[32..36], new_total);
    }
    LittleEndian::write_u16(&mut bpb_buf[22..24], new_spf);
}

/// Format a blank Human68k volume of exactly `target_size_bytes`, matching
/// the conventions captured in `template` (sector size, sectors-per-cluster,
/// FAT count, root-entry count, FAT endianness, OEM string, boot stub).
///
/// FAT16 only — every real X68000 SCSI/SASI HDD volume is FAT16, and FAT12
/// floppies route through the floppy-container converter rather than the
/// defragmenting clone. The returned image carries the patched BPB, the FAT
/// reserved entries (cluster 0 = `0xFF00 | media`, cluster 1 = end-of-chain)
/// in each FAT copy, and a zeroed root directory + data region — ready to be
/// opened with [`Human68kFilesystem::open`] for the clone replay.
///
/// Errors when the template is FAT12, when `target_size_bytes` isn't a
/// non-zero multiple of the sector size, when the FAT would overflow the
/// BPB's sectors-per-FAT field (one byte on SHARP/KG, two on standard), or
/// when the target is too small to keep the cluster count at the FAT16
/// floor (4085).
pub fn create_blank_human68k(
    template: &Human68kFormatTemplate,
    target_size_bytes: u64,
) -> Result<Vec<u8>, FilesystemError> {
    let bpb = &template.bpb;
    if bpb.fat_kind != FatKind::Fat16 {
        return Err(FilesystemError::InvalidData(
            "create_blank_human68k: only FAT16 Human68k volumes are supported \
             (FAT12 floppies use the floppy-container converter)"
                .into(),
        ));
    }
    let bps = bpb.bytes_per_sector as u32;
    let bps64 = bps as u64;
    if target_size_bytes == 0 || !target_size_bytes.is_multiple_of(bps64) {
        return Err(FilesystemError::InvalidData(format!(
            "create_blank_human68k: target size {target_size_bytes} is not a non-zero \
             multiple of the sector size {bps}"
        )));
    }
    let new_total = (target_size_bytes / bps64) as u32;
    let spc = bpb.sectors_per_cluster as u32;
    let num_fats = bpb.num_fats as u32;
    let reserved = bpb.reserved_sectors as u32;
    let root_entries = bpb.root_entries as u32;
    let root_dir_sectors = (root_entries * 32).div_ceil(bps);

    let new_spf = human68k_fat16_sectors(new_total, reserved, num_fats, root_dir_sectors, spc, bps);

    // The SHARP/KG BPB stores sectors-per-FAT in one byte (offset 0x1D);
    // the standard BPB has a two-byte field (offset 22).
    let spf_max = if bpb.fat_big_endian {
        u8::MAX as u32
    } else {
        u16::MAX as u32
    };
    if new_spf > spf_max {
        return Err(FilesystemError::InvalidData(format!(
            "create_blank_human68k: FAT needs {new_spf} sectors, exceeding the {}-byte \
             sectors-per-FAT field",
            if bpb.fat_big_endian { 1 } else { 2 }
        )));
    }

    let data_start = reserved + num_fats * new_spf + root_dir_sectors;
    let clusters = new_total.saturating_sub(data_start) / spc;
    if clusters < 4085 {
        return Err(FilesystemError::InvalidData(format!(
            "create_blank_human68k: target size {target_size_bytes} yields {clusters} \
             clusters, below the FAT16 minimum (4085)"
        )));
    }

    let mut image = vec![0u8; target_size_bytes as usize];

    // Reserved region: copy the template verbatim (boot stub + OEM), then
    // re-patch the size fields of the BPB in the first 512 bytes.
    let mut reserved_region = template.reserved_region.clone();
    let media = if bpb.fat_big_endian {
        reserved_region[0x1C]
    } else {
        reserved_region[21]
    };
    if bpb.fat_big_endian {
        patch_sharp_kg_bpb(&mut reserved_region, new_total, new_spf as u8);
    } else {
        patch_std_bpb(&mut reserved_region, new_total, new_spf as u16);
    }
    let copy_len = reserved_region.len().min(image.len());
    image[..copy_len].copy_from_slice(&reserved_region[..copy_len]);

    // FAT reserved entries in each FAT copy, honoring the table endianness.
    let entry0 = 0xFF00u16 | media as u16;
    let entry1 = 0xFFFFu16;
    let (e0, e1) = if bpb.fat_big_endian {
        (entry0.to_be_bytes(), entry1.to_be_bytes())
    } else {
        (entry0.to_le_bytes(), entry1.to_le_bytes())
    };
    for i in 0..num_fats {
        let fat_off = (reserved + i * new_spf) as usize * bps as usize;
        image[fat_off..fat_off + 2].copy_from_slice(&e0);
        image[fat_off + 2..fat_off + 4].copy_from_slice(&e1);
    }

    Ok(image)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a tiny synthetic Human68k FAT12 floppy:
    ///   - 720 KB (9 spt × 80 trk × 2 sides × 512 B)
    ///   - one root file "DOC.TXT" with 32 bytes of content in cluster 2
    fn build_fat12_synthetic() -> Vec<u8> {
        const TOTAL_SECTORS: u32 = 1440;
        const BYTES_PER_SECTOR: u16 = 512;
        const SECTORS_PER_CLUSTER: u8 = 1;
        const RESERVED_SECTORS: u16 = 1;
        const NUM_FATS: u8 = 2;
        const ROOT_ENTRIES: u16 = 112;
        const FAT_SECTORS: u16 = 3;

        let mut disk = vec![0u8; TOTAL_SECTORS as usize * BYTES_PER_SECTOR as usize];
        // BPB
        disk[0] = 0xEB; // JMP
        disk[1] = 0x3C;
        disk[2] = 0x90;
        disk[3..11].copy_from_slice(b"X68KFS  ");
        LittleEndian::write_u16(&mut disk[11..13], BYTES_PER_SECTOR);
        disk[13] = SECTORS_PER_CLUSTER;
        LittleEndian::write_u16(&mut disk[14..16], RESERVED_SECTORS);
        disk[16] = NUM_FATS;
        LittleEndian::write_u16(&mut disk[17..19], ROOT_ENTRIES);
        LittleEndian::write_u16(&mut disk[19..21], TOTAL_SECTORS as u16);
        disk[21] = 0xF9; // media descriptor (3.5" 720K)
        LittleEndian::write_u16(&mut disk[22..24], FAT_SECTORS);

        // FAT entry for cluster 2 — end of chain.
        let fat0_off = RESERVED_SECTORS as usize * BYTES_PER_SECTOR as usize;
        // Reserved: cluster 0 = 0xFF9, cluster 1 = 0xFFF.
        disk[fat0_off] = 0xF9;
        disk[fat0_off + 1] = 0xFF;
        disk[fat0_off + 2] = 0xFF;
        // Cluster 2 entry (FAT12 packing for even cluster): low 12 bits.
        // 0xFFF = end of chain.
        disk[fat0_off + 3] = 0xFF;
        disk[fat0_off + 4] = 0x0F;

        // Root directory at reserved + fat_sectors * num_fats.
        let root_off = RESERVED_SECTORS as usize * BYTES_PER_SECTOR as usize
            + FAT_SECTORS as usize * NUM_FATS as usize * BYTES_PER_SECTOR as usize;
        // Entry 0: "DOC.TXT" — first cluster 2, size 32.
        let entry = &mut disk[root_off..root_off + 32];
        entry[0..8].copy_from_slice(b"DOC     ");
        entry[8..11].copy_from_slice(b"TXT");
        entry[11] = attr::ARCHIVE;
        for b in &mut entry[12..22] {
            *b = 0x20; // padded name-extension area
        }
        LittleEndian::write_u16(&mut entry[26..28], 2); // first cluster
        LittleEndian::write_u32(&mut entry[28..32], 32); // size

        // File data at cluster 2.
        let root_byte_len = ROOT_ENTRIES as usize * 32;
        let data_off = root_off + root_byte_len;
        let payload = b"hello human68k synthetic disk!\n\0";
        disk[data_off..data_off + payload.len()].copy_from_slice(payload);
        disk
    }

    #[test]
    fn parses_bpb_and_identifies_fat12() {
        let disk = build_fat12_synthetic();
        let cur = Cursor::new(disk);
        let fs = Human68kFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.bpb.bytes_per_sector, 512);
        assert_eq!(fs.bpb.total_sectors, 1440);
        assert_eq!(fs.bpb.fat_kind, FatKind::Fat12);
        assert_eq!(fs.fs_type(), "Human68k (FAT12)");
    }

    /// Build a minimal Sharp/KG FAT16 volume (big-endian BPB *and* FAT)
    /// with one 2-cluster file `BIG.BIN`, whose FAT chain is stored
    /// big-endian. 512-byte sectors, 1 sector/cluster, ~4090 clusters
    /// (the FAT16 floor).
    fn build_sharp_kg_fat16_with_2cluster_file() -> Vec<u8> {
        const BPS: usize = 512;
        const NFATS: usize = 1;
        const FATSZ: usize = 16;
        const RESERVED: usize = 1;
        const ROOT_ENTRIES: usize = 16;
        const TOTAL: usize = 4108; // -> 4090 data clusters -> FAT16

        let mut disk = vec![0u8; TOTAL * BPS];
        // BPB: 2-byte BRA.S + 16-byte OEM, then big-endian fields at 0x12.
        disk[0] = 0x60;
        disk[1] = 0x24;
        disk[2..18].copy_from_slice(b"SHARP/KG TEST   ");
        BigEndian::write_u16(&mut disk[0x12..0x14], BPS as u16);
        disk[0x14] = 1; // sectors per cluster
        disk[0x15] = NFATS as u8;
        BigEndian::write_u16(&mut disk[0x16..0x18], RESERVED as u16);
        BigEndian::write_u16(&mut disk[0x18..0x1A], ROOT_ENTRIES as u16);
        BigEndian::write_u16(&mut disk[0x1A..0x1C], TOTAL as u16);
        disk[0x1C] = 0xF8;
        disk[0x1D] = FATSZ as u8;

        // Big-endian FAT16: cluster 2 -> 3, cluster 3 -> EOC.
        let fat = RESERVED * BPS;
        BigEndian::write_u16(&mut disk[fat..fat + 2], 0xFFF8); // entry 0
        BigEndian::write_u16(&mut disk[fat + 2..fat + 4], 0xFFFF); // entry 1
        BigEndian::write_u16(&mut disk[fat + 4..fat + 6], 3); // cluster 2 -> 3
        BigEndian::write_u16(&mut disk[fat + 6..fat + 8], 0xFFFF); // cluster 3 EOC

        // Root entry for BIG.BIN, first_cluster + size little-endian.
        let root = (RESERVED + NFATS * FATSZ) * BPS;
        let e = &mut disk[root..root + 32];
        e[0..8].copy_from_slice(b"BIG     ");
        e[8..11].copy_from_slice(b"BIN");
        e[11] = attr::ARCHIVE;
        LittleEndian::write_u16(&mut e[26..28], 2);
        LittleEndian::write_u32(&mut e[28..32], 768); // spans 2 clusters

        // Data: cluster 2 = 512 * 0xAA, cluster 3 = 256 * 0xBB.
        let data_start = (RESERVED + NFATS * FATSZ + 1) * BPS;
        for b in &mut disk[data_start..data_start + 512] {
            *b = 0xAA;
        }
        for b in &mut disk[data_start + 512..data_start + 512 + 256] {
            *b = 0xBB;
        }
        disk
    }

    #[test]
    fn reads_big_endian_fat_chain_across_clusters() {
        let mut fs =
            Human68kFilesystem::open(Cursor::new(build_sharp_kg_fat16_with_2cluster_file()), 0)
                .unwrap();
        assert!(fs.bpb.fat_big_endian, "Sharp/KG BPB -> big-endian FAT");
        assert_eq!(fs.bpb.fat_kind, FatKind::Fat16);
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "BIG.BIN");
        assert_eq!(entries[0].size, 768);
        // The whole file must come back: the second cluster is only
        // reachable by decoding the FAT link big-endian. Reading it
        // little-endian truncates the chain to a single cluster.
        let data = fs.read_file(&entries[0], 4096).unwrap();
        assert_eq!(data.len(), 768, "big-endian chain must yield both clusters");
        assert!(data[..512].iter().all(|&b| b == 0xAA));
        assert!(data[512..768].iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn parses_sharp_kg_scsi_hdd_bpb() {
        // Real BPB bytes from a BlueSCSI `X68SCSI1` image (HD10_512.hda):
        // 2-byte BRA.S + 16-byte "SHARP/KG    1.00" OEM, then big-endian
        // BPB fields at offset 0x12. bytes/sector 1024, 16 sec/cluster,
        // 2 FATs, 1 reserved, 512 root entries, FAT size 118, total
        // 966656 sectors (the X68K partition length).
        let mut bpb = [0u8; 512];
        bpb[0..18].copy_from_slice(b"\x60\x24SHARP/KG    1.00");
        bpb[0x12..0x14].copy_from_slice(&1024u16.to_be_bytes()); // bytes/sector
        bpb[0x14] = 16; // sectors per cluster
        bpb[0x15] = 2; // num FATs
        bpb[0x16..0x18].copy_from_slice(&1u16.to_be_bytes()); // reserved
        bpb[0x18..0x1A].copy_from_slice(&512u16.to_be_bytes()); // root entries
        bpb[0x1A..0x1C].copy_from_slice(&0u16.to_be_bytes()); // total16 (unused)
        bpb[0x1C] = 0xF7; // media descriptor
        bpb[0x1D] = 118; // sectors per FAT
        bpb[0x1E..0x22].copy_from_slice(&966_656u32.to_be_bytes()); // total32

        let parsed = Human68kBpb::parse(&bpb).unwrap();
        assert_eq!(parsed.bytes_per_sector, 1024);
        assert_eq!(parsed.sectors_per_cluster, 16);
        assert_eq!(parsed.num_fats, 2);
        assert_eq!(parsed.reserved_sectors, 1);
        assert_eq!(parsed.root_entries, 512);
        assert_eq!(parsed.fat_sectors, 118);
        assert_eq!(parsed.total_sectors, 966_656);
        // ~60k clusters of 16 KiB -> FAT16.
        assert_eq!(parsed.fat_kind, FatKind::Fat16);
    }

    #[test]
    fn lists_root_directory_with_18_3_filename() {
        let disk = build_fat12_synthetic();
        let cur = Cursor::new(disk);
        let mut fs = Human68kFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "DOC.TXT");
        assert_eq!(entries[0].size, 32);
    }

    #[test]
    fn reads_file_byte_exact_through_fat_chain() {
        let disk = build_fat12_synthetic();
        let cur = Cursor::new(disk);
        let mut fs = Human68kFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let data = fs.read_file(&entries[0], 4096).unwrap();
        // Synthetic payload has a final null byte; full 32 B read returns it.
        assert_eq!(data.len(), 32);
        assert_eq!(&data[..30], b"hello human68k synthetic disk!");
    }

    /// Build a synthetic FAT12 disk with one subdirectory `SUB` (cluster 2)
    /// holding `.`, `..`, and a file `CHILD.TXT` (cluster 3).
    fn build_fat12_with_subdir() -> Vec<u8> {
        const BYTES_PER_SECTOR: usize = 512;
        const NUM_FATS: usize = 2;
        const FAT_SECTORS: usize = 3;
        const RESERVED_SECTORS: usize = 1;
        const ROOT_ENTRIES: usize = 112;

        let mut disk = build_fat12_synthetic();
        // Re-point root entry 0 to be the directory "SUB" at cluster 2.
        let root_off =
            RESERVED_SECTORS * BYTES_PER_SECTOR + FAT_SECTORS * NUM_FATS * BYTES_PER_SECTOR;
        let entry = &mut disk[root_off..root_off + 32];
        entry.fill(0);
        entry[0..8].copy_from_slice(b"SUB     ");
        entry[8..11].copy_from_slice(b"   ");
        entry[11] = attr::DIRECTORY;
        for b in &mut entry[12..22] {
            *b = 0x20;
        }
        LittleEndian::write_u16(&mut entry[26..28], 2); // first cluster
        LittleEndian::write_u32(&mut entry[28..32], 0); // dirs report size 0

        // FAT: cluster 2 and cluster 3 both end-of-chain (0xFFF each).
        let fat0 = RESERVED_SECTORS * BYTES_PER_SECTOR;
        disk[fat0 + 3] = 0xFF; // cluster 2 low byte
        disk[fat0 + 4] = 0xFF; // cluster 2 high nibble + cluster 3 low nibble
        disk[fat0 + 5] = 0xFF; // cluster 3 high byte

        // Cluster 2 data = subdirectory contents.
        let root_byte_len = ROOT_ENTRIES * 32;
        let clus2_off = root_off + root_byte_len;
        let write_dirent = |disk: &mut [u8],
                            off: usize,
                            name: &[u8; 8],
                            ext: &[u8; 3],
                            attr: u8,
                            clus: u16,
                            size: u32| {
            let e = &mut disk[off..off + 32];
            e.fill(0);
            e[0..8].copy_from_slice(name);
            e[8..11].copy_from_slice(ext);
            e[11] = attr;
            LittleEndian::write_u16(&mut e[26..28], clus);
            LittleEndian::write_u32(&mut e[28..32], size);
        };
        write_dirent(
            &mut disk,
            clus2_off,
            b".       ",
            b"   ",
            attr::DIRECTORY,
            2,
            0,
        );
        write_dirent(
            &mut disk,
            clus2_off + 32,
            b"..      ",
            b"   ",
            attr::DIRECTORY,
            0,
            0,
        );
        write_dirent(
            &mut disk,
            clus2_off + 64,
            b"CHILD   ",
            b"TXT",
            attr::ARCHIVE,
            3,
            5,
        );

        // Cluster 3 data = the child file payload.
        let clus3_off = clus2_off + BYTES_PER_SECTOR;
        disk[clus3_off..clus3_off + 5].copy_from_slice(b"child");
        disk
    }

    #[test]
    fn lists_subdirectory_via_cluster_chain() {
        let disk = build_fat12_with_subdir();
        let mut fs = Human68kFilesystem::open(Cursor::new(disk), 0).unwrap();
        let root = fs.root().unwrap();
        let root_entries = fs.list_directory(&root).unwrap();
        assert_eq!(root_entries.len(), 1);
        let sub = &root_entries[0];
        assert_eq!(sub.name, "SUB");
        assert_eq!(sub.path, "/SUB");

        // Listing the subdirectory follows its cluster chain and skips
        // the `.` / `..` self/parent links.
        let kids = fs.list_directory(sub).unwrap();
        assert_eq!(kids.len(), 1, "expected only CHILD.TXT, not . / ..");
        assert_eq!(kids[0].name, "CHILD.TXT");
        assert_eq!(kids[0].path, "/SUB/CHILD.TXT");
        assert_eq!(kids[0].size, 5);

        let data = fs.read_file(&kids[0], 4096).unwrap();
        assert_eq!(&data, b"child");
    }

    #[test]
    fn parse_dir_entry_combines_name_and_name_extension_bytes() {
        let mut buf = [0u8; 32];
        // 8 base + 10 ext-name + 3 ext to demonstrate 18.3.
        buf[0..8].copy_from_slice(b"LONGNAME");
        buf[8..11].copy_from_slice(b"TXT");
        buf[11] = attr::ARCHIVE;
        // 10 more chars in the name-ext area, space-padded for the tail.
        buf[12..22].copy_from_slice(b"_EXTRABIT ");
        LittleEndian::write_u16(&mut buf[26..28], 5);
        LittleEndian::write_u32(&mut buf[28..32], 256);
        let parsed = parse_dir_entry(&buf).unwrap();
        assert_eq!(parsed.first_cluster, 5);
        assert_eq!(parsed.size, 256);
        // Display = ASCII chars only; the underscore in the synthetic
        // name-ext rides through verbatim, the trailing space trims.
        assert_eq!(parsed.display_name, "LONGNAME_EXTRABIT.TXT");
    }

    #[test]
    fn shift_jis_double_byte_decodes_to_japanese() {
        // One ASCII char + one Shift-JIS double-byte pair (0x82 0xA0 = 'あ').
        let mut buf = [0u8; 32];
        buf[0] = b'A';
        buf[1] = 0x82;
        buf[2] = 0xA0;
        for b in &mut buf[3..8] {
            *b = 0x20;
        }
        for b in &mut buf[8..11] {
            *b = 0x20;
        }
        for b in &mut buf[12..22] {
            *b = 0x20;
        }
        buf[11] = attr::ARCHIVE;
        LittleEndian::write_u16(&mut buf[26..28], 2);
        let parsed = parse_dir_entry(&buf).unwrap();
        // Decoded to real Unicode, not a placeholder.
        assert_eq!(parsed.display_name, "Aあ");
        // Raw bytes preserved for round-trip.
        assert_eq!(&parsed.raw_name, b"A\x82\xA0");
    }

    #[test]
    fn half_width_katakana_decodes_to_unicode() {
        // 0xBC 0xAC 0xB0 0xCD 0xDF 0xDD = "ｼｬｰﾍﾟﾝ" (real disk: "Sharpen").
        let mut buf = [0u8; 32];
        buf[0..6].copy_from_slice(&[0xBC, 0xAC, 0xB0, 0xCD, 0xDF, 0xDD]);
        for b in &mut buf[6..8] {
            *b = 0x20;
        }
        for b in &mut buf[8..11] {
            *b = 0x20;
        }
        for b in &mut buf[12..22] {
            *b = 0x20;
        }
        buf[11] = attr::DIRECTORY;
        let parsed = parse_dir_entry(&buf).unwrap();
        assert_eq!(parsed.display_name, "ｼｬｰﾍﾟﾝ");
    }

    #[test]
    fn encode_decode_japanese_name_round_trips() {
        // Encode a Japanese name to SJIS fields, stamp into a dir entry,
        // parse it back, and confirm the decoded display matches.
        let (n8, ne10, e3) = encode_human68k_name("アクセサリ").unwrap();
        let mut buf = [0u8; 32];
        buf[0..8].copy_from_slice(&n8);
        buf[8..11].copy_from_slice(&e3);
        buf[11] = attr::DIRECTORY;
        buf[12..22].copy_from_slice(&ne10);
        let parsed = parse_dir_entry(&buf).unwrap();
        assert_eq!(parsed.display_name, "アクセサリ");
    }

    #[test]
    fn encode_rejects_unmappable_characters() {
        // An emoji has no Shift-JIS mapping.
        assert!(matches!(
            encode_human68k_name("hi😀.txt"),
            Err(FilesystemError::InvalidData(_))
        ));
    }

    #[test]
    fn encode_human68k_name_handles_8_3_and_18_3() {
        let (n, ne, e) = encode_human68k_name("SHORT.TXT").unwrap();
        assert_eq!(&n, b"SHORT   ");
        assert_eq!(&ne, b"          ");
        assert_eq!(&e, b"TXT");
        let (n2, ne2, e2) = encode_human68k_name("LONGFILENAME1234.X").unwrap();
        assert_eq!(&n2, b"LONGFILE");
        assert_eq!(&ne2, b"NAME1234  ");
        assert_eq!(&e2, b"X  ");
    }

    #[test]
    fn encode_human68k_name_rejects_path_separator_and_oversize() {
        assert!(encode_human68k_name("foo/bar").is_err());
        let too_long = "X".repeat(19);
        assert!(encode_human68k_name(&too_long).is_err());
        assert!(encode_human68k_name("name.TOOLONGEXT").is_err());
        assert!(encode_human68k_name("").is_err());
    }

    #[test]
    fn create_file_persists_and_round_trips_through_sync() {
        let disk = build_fat12_synthetic();
        let cur = Cursor::new(disk);
        let mut fs = Human68kFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let before_free = EditableFilesystem::free_space(&mut fs).unwrap();
        let payload = b"created via human68k edit path".to_vec();
        let mut src = Cursor::new(payload.clone());
        let _fe = fs
            .create_file(
                &root,
                "NEW.TXT",
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();

        let inner = fs.reader.clone();
        let mut fs2 = Human68kFilesystem::open(inner, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        let new = entries.iter().find(|e| e.name == "NEW.TXT").unwrap();
        assert_eq!(new.size, payload.len() as u64);
        let got = fs2.read_file(new, 4096).unwrap();
        assert_eq!(got, payload);

        let after_free = EditableFilesystem::free_space(&mut fs2).unwrap();
        // 512 B cluster, 30 B payload → one cluster consumed.
        assert_eq!(after_free, before_free - 512);
    }

    #[test]
    fn delete_entry_frees_chain_and_marks_slot_deleted() {
        let disk = build_fat12_synthetic();
        let cur = Cursor::new(disk);
        let mut fs = Human68kFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let doc = entries
            .iter()
            .find(|e| e.name == "DOC.TXT")
            .unwrap()
            .clone();
        let before_free = EditableFilesystem::free_space(&mut fs).unwrap();
        fs.delete_entry(&root, &doc).unwrap();

        let inner = fs.reader.clone();
        let mut fs2 = Human68kFilesystem::open(inner, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        assert!(entries.iter().all(|e| e.name != "DOC.TXT"));
        let after_free = EditableFilesystem::free_space(&mut fs2).unwrap();
        // One 512-B cluster gets freed.
        assert_eq!(after_free, before_free + 512);
    }

    #[test]
    fn create_file_rejects_duplicate_names() {
        let disk = build_fat12_synthetic();
        let mut fs = Human68kFilesystem::open(Cursor::new(disk), 0).unwrap();
        let root = fs.root().unwrap();
        let mut src = Cursor::new(b"x".to_vec());
        let err = fs
            .create_file(&root, "DOC.TXT", &mut src, 1, &CreateFileOptions::default())
            .unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }

    #[test]
    fn create_directory_then_file_inside_round_trips() {
        let mut fs = Human68kFilesystem::open(Cursor::new(build_fat12_synthetic()), 0).unwrap();
        let root = fs.root().unwrap();
        // mkdir /SUB
        let sub = fs
            .create_directory(&root, "SUB", &CreateDirectoryOptions::default())
            .unwrap();
        assert_eq!(sub.path, "/SUB");
        assert!(sub.is_directory());
        // A brand-new directory lists empty (the `.`/`..` links are hidden).
        assert!(fs.list_directory(&sub).unwrap().is_empty());

        // Create a file inside the subdirectory.
        let payload = b"inside the subdir".to_vec();
        let mut src = Cursor::new(payload.clone());
        let child = fs
            .create_file(
                &sub,
                "INNER.TXT",
                &mut src,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        assert_eq!(child.path, "/SUB/INNER.TXT");

        // List the subdir and read the file back byte-exact.
        let kids = fs.list_directory(&sub).unwrap();
        assert_eq!(kids.len(), 1);
        assert_eq!(kids[0].name, "INNER.TXT");
        assert_eq!(fs.read_file(&kids[0], 4096).unwrap(), payload);

        // The root still shows exactly the SUB directory plus the original
        // synthetic DOC.TXT.
        let root_names: Vec<_> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(root_names.contains(&"SUB".to_string()));
        assert!(root_names.contains(&"DOC.TXT".to_string()));
    }

    #[test]
    fn delete_entry_refuses_nonempty_directory() {
        let mut fs = Human68kFilesystem::open(Cursor::new(build_fat12_synthetic()), 0).unwrap();
        let root = fs.root().unwrap();
        let sub = fs
            .create_directory(&root, "SUB", &CreateDirectoryOptions::default())
            .unwrap();
        let mut src = Cursor::new(b"x".to_vec());
        fs.create_file(&sub, "F.TXT", &mut src, 1, &CreateFileOptions::default())
            .unwrap();
        // delete_entry must refuse the non-empty directory.
        let err = fs.delete_entry(&root, &sub).unwrap_err();
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    #[test]
    fn delete_recursive_removes_subtree_and_frees_clusters() {
        let mut fs = Human68kFilesystem::open(Cursor::new(build_fat12_synthetic()), 0).unwrap();
        let root = fs.root().unwrap();
        let free_before = EditableFilesystem::free_space(&mut fs).unwrap();

        // Build /SUB/{NESTED/, A.TXT}
        let sub = fs
            .create_directory(&root, "SUB", &CreateDirectoryOptions::default())
            .unwrap();
        let nested = fs
            .create_directory(&sub, "NESTED", &CreateDirectoryOptions::default())
            .unwrap();
        let mut a = Cursor::new(b"aaaa".to_vec());
        fs.create_file(&sub, "A.TXT", &mut a, 4, &CreateFileOptions::default())
            .unwrap();
        let mut b = Cursor::new(b"bbbb".to_vec());
        fs.create_file(&nested, "B.TXT", &mut b, 4, &CreateFileOptions::default())
            .unwrap();

        // Recursively delete /SUB.
        fs.delete_recursive(&root, &sub).unwrap();

        // SUB is gone from the root and all clusters are reclaimed.
        let root_names: Vec<_> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(!root_names.contains(&"SUB".to_string()));
        let free_after = EditableFilesystem::free_space(&mut fs).unwrap();
        assert_eq!(
            free_after, free_before,
            "every cluster allocated for the subtree must be freed"
        );
    }

    #[test]
    fn create_file_grows_subdirectory_chain_past_one_cluster() {
        // 512-byte cluster holds 16 dir slots; `.`/`..` use 2, so the 15th
        // created file forces the subdirectory to grow a second cluster.
        let mut fs = Human68kFilesystem::open(Cursor::new(build_fat12_synthetic()), 0).unwrap();
        let root = fs.root().unwrap();
        let sub = fs
            .create_directory(&root, "BIG", &CreateDirectoryOptions::default())
            .unwrap();
        for i in 0..20 {
            let name = format!("F{i:02}.TXT");
            let mut src = Cursor::new(vec![i as u8]);
            fs.create_file(&sub, &name, &mut src, 1, &CreateFileOptions::default())
                .unwrap();
        }
        let kids = fs.list_directory(&sub).unwrap();
        assert_eq!(kids.len(), 20, "all 20 files survive the chain growth");
        // The subdirectory chain must now span more than one cluster.
        let chain = fs.cluster_chain(sub.location as u16).unwrap();
        assert!(
            chain.len() >= 2,
            "directory should have grown a 2nd cluster"
        );
    }

    // Spine-stage-7 confirmation: resize_fat_in_place on a Human68k disk
    // must extend the volume in place and Human68kFilesystem::open must
    // still find every file on the resized image. Human68k is FAT12/16
    // BPB-compatible so the same FAT machinery applies — these tests
    // pin the contract so a future FAT refactor can't silently break
    // X68000 HDD resize.
    #[test]
    fn resize_fat_in_place_grows_human68k_volume() {
        use crate::fs::fat::resize_fat_in_place;
        use std::io::Cursor;

        let mut disk = build_fat12_synthetic();
        // Extend the backing buffer to the larger target size so the
        // resize call can write into it (resize doesn't allocate disk
        // bytes itself — it requires the writer to already be large
        // enough).
        let old_sectors: u32 = 1440;
        let new_sectors: u32 = 2880; // 720K -> 1.44M shape
        let bps = 512usize;
        disk.resize(new_sectors as usize * bps, 0);

        let mut cur = Cursor::new(&mut disk);
        let did_resize =
            resize_fat_in_place(&mut cur, 0, new_sectors, &mut |_| {}).expect("resize ok");
        assert!(did_resize, "resize should succeed on Human68k FAT12 BPB");

        // BPB.total_sectors (offset 19..21 for u16) now reflects the new size.
        assert_eq!(
            u16::from_le_bytes([disk[19], disk[20]]) as u32,
            new_sectors,
            "BPB total_sectors should be patched in-place"
        );

        // The file we seeded into the FAT12 disk must still be reachable
        // after the resize via the Human68k reader (BPB shape unchanged,
        // FAT extended with free entries).
        let mut fs = Human68kFilesystem::open(Cursor::new(&disk), 0).expect("reopen");
        assert_eq!(fs.bpb.total_sectors, new_sectors);
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1, "DOC.TXT should survive resize");
        assert_eq!(entries[0].name, "DOC.TXT");

        let payload = fs.read_file(&entries[0], 1024).unwrap();
        assert_eq!(&payload[..14], b"hello human68k");

        // FAT12 stays FAT12 — old_sectors and new_sectors both
        // produce <4085 clusters with 1 SPC + 1 reserved + 2 FATs at
        // 3 sectors each. The resize path uses the spare-FAT-capacity
        // branch (no data shift, BPB-only update).
        let _ = old_sectors;
    }

    #[test]
    fn resize_fat_in_place_shrinks_human68k_volume() {
        use crate::fs::fat::resize_fat_in_place;
        use std::io::Cursor;

        // Build a FAT16 Human68k disk (16 MiB) so the resize_fat_in_place
        // path has room to shrink without changing FAT type.
        let total_sectors_old: u32 = 32_768; // 16 MiB / 512
        let total_sectors_new: u32 = 16_384; // 8 MiB / 512
        let bps = 512usize;
        let mut disk =
            crate::fs::fat::create_blank_fat(total_sectors_old as u64 * bps as u64, Some("X68K"))
                .expect("blank FAT16");

        // Sanity: Human68k can open the fresh blank.
        let bpb_check_disk = disk.clone();
        let _ = Human68kFilesystem::open(Cursor::new(bpb_check_disk), 0)
            .expect("Human68k opens blank FAT16");

        let mut cur = Cursor::new(&mut disk);
        let did_resize =
            resize_fat_in_place(&mut cur, 0, total_sectors_new, &mut |_| {}).expect("shrink ok");
        assert!(did_resize, "shrink should succeed");

        // BPB total_sectors slot for FAT16 with total > u16::MAX is in
        // bytes 32..36 (large total). 16384 fits in u16 so it lives in
        // bytes 19..21 instead — match how the FAT resize writer chose
        // the field.
        let small_total = u16::from_le_bytes([disk[19], disk[20]]) as u32;
        let big_total = u32::from_le_bytes([disk[32], disk[33], disk[34], disk[35]]);
        let observed = if small_total != 0 {
            small_total
        } else {
            big_total
        };
        assert_eq!(observed, total_sectors_new);

        // The blank disk has no files, but reopen + root listing must
        // succeed end-to-end.
        let mut fs = Human68kFilesystem::open(Cursor::new(&disk), 0).expect("reopen");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert!(
            entries.is_empty(),
            "freshly-formatted volume has no files after shrink"
        );
    }

    // --- SHARP/KG big-endian-FAT HDD resize (slice 4) -----------------------

    /// FAT16 sub-type floor (cluster count) — must stay above this through a
    /// resize or the on-disk big-endian FAT would be reinterpreted as FAT12.
    const FAT16_FLOOR: u32 = 4085;

    /// Build a SHARP/KG FAT16 HDD volume (big-endian BPB *and* FAT) sized so
    /// it has headroom to grow (forcing a FAT extension + data shift) and to
    /// shrink while staying comfortably FAT16. Returns
    /// `(disk, command_x_payload, readme_payload)`. 512-byte sectors, 1
    /// sector/cluster, 2 FATs. `COMMAND.X` spans three clusters and starts
    /// with the Human68k `HU` (0x4855) executable header; `README.TXT` is a
    /// single-cluster bystander file used to prove unrelated data survives.
    fn build_sharp_kg_fat16_resizable() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        const BPS: usize = 512;
        const NFATS: usize = 2;
        const RESERVED: usize = 1;
        const ROOT_ENTRIES: usize = 16; // -> 1 root-dir sector
        const FATSZ: usize = 32; // covers 8200-sector volume (verified below)
        const TOTAL: usize = 8200; // -> ~8134 data clusters -> FAT16

        let mut disk = vec![0u8; TOTAL * BPS];

        // BPB: 2-byte BRA.S + 16-byte OEM, then big-endian fields at 0x12.
        disk[0] = 0x60;
        disk[1] = 0x24;
        disk[2..18].copy_from_slice(b"SHARP/KG    1.00");
        BigEndian::write_u16(&mut disk[0x12..0x14], BPS as u16);
        disk[0x14] = 1; // sectors per cluster
        disk[0x15] = NFATS as u8;
        BigEndian::write_u16(&mut disk[0x16..0x18], RESERVED as u16);
        BigEndian::write_u16(&mut disk[0x18..0x1A], ROOT_ENTRIES as u16);
        BigEndian::write_u16(&mut disk[0x1A..0x1C], 0); // total16 unused
        disk[0x1C] = 0xF8; // media descriptor
        disk[0x1D] = FATSZ as u8;
        BigEndian::write_u32(&mut disk[0x1E..0x22], TOTAL as u32); // total32

        // Big-endian FAT: clusters 2->3->4 (COMMAND.X), 5 EOC (README.TXT).
        let write_fat = |disk: &mut [u8], fat_base: usize| {
            BigEndian::write_u16(&mut disk[fat_base..fat_base + 2], 0xFFF8); // entry 0
            BigEndian::write_u16(&mut disk[fat_base + 2..fat_base + 4], 0xFFFF); // entry 1
            BigEndian::write_u16(&mut disk[fat_base + 4..fat_base + 6], 3); // 2 -> 3
            BigEndian::write_u16(&mut disk[fat_base + 6..fat_base + 8], 4); // 3 -> 4
            BigEndian::write_u16(&mut disk[fat_base + 8..fat_base + 10], 0xFFFF); // 4 EOC
            BigEndian::write_u16(&mut disk[fat_base + 10..fat_base + 12], 0xFFFF);
            // 5 EOC
        };
        let fat0 = RESERVED * BPS;
        write_fat(&mut disk, fat0);
        write_fat(&mut disk, fat0 + FATSZ * BPS);

        // Root directory entries.
        let root = (RESERVED + NFATS * FATSZ) * BPS;
        let cmd_size = 2 * BPS + 200; // spans clusters 2,3,4
        {
            let e = &mut disk[root..root + 32];
            e[0..8].copy_from_slice(b"COMMAND ");
            e[8..11].copy_from_slice(b"X  ");
            e[11] = attr::ARCHIVE;
            LittleEndian::write_u16(&mut e[26..28], 2);
            LittleEndian::write_u32(&mut e[28..32], cmd_size as u32);
        }
        let readme_size = 100usize;
        {
            let e = &mut disk[root + 32..root + 64];
            e[0..8].copy_from_slice(b"README  ");
            e[8..11].copy_from_slice(b"TXT");
            e[11] = attr::ARCHIVE;
            LittleEndian::write_u16(&mut e[26..28], 5);
            LittleEndian::write_u32(&mut e[28..32], readme_size as u32);
        }

        // Data area. Cluster N starts at data_start_sector + (N-2).
        let data_start = (RESERVED + NFATS * FATSZ + 1) * BPS;
        let mut command = vec![0u8; cmd_size];
        for (i, b) in command.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        command[0] = 0x48; // 'H'
        command[1] = 0x55; // 'U' — Human68k .X executable header
        disk[data_start..data_start + cmd_size].copy_from_slice(&command);

        let readme_off = data_start + 3 * BPS; // cluster 5
        let mut readme = vec![0u8; readme_size];
        for (i, b) in readme.iter_mut().enumerate() {
            *b = (0xC0u8) ^ (i as u8);
        }
        disk[readme_off..readme_off + readme_size].copy_from_slice(&readme);

        (disk, command, readme)
    }

    /// Read both seeded files back and assert byte-exact, including the `HU`
    /// header. Shared by the grow / shrink / round-trip resize tests.
    fn assert_seeded_files_intact(disk: &[u8], command: &[u8], readme: &[u8]) {
        let mut fs = Human68kFilesystem::open(Cursor::new(disk), 0).expect("reopen");
        assert!(fs.bpb.fat_big_endian, "must stay big-endian FAT");
        assert_eq!(fs.bpb.fat_kind, FatKind::Fat16, "must stay FAT16");
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let names: Vec<_> = entries.iter().map(|e| e.name.clone()).collect();
        assert!(
            names.contains(&"COMMAND.X".to_string()),
            "COMMAND.X survives"
        );
        assert!(
            names.contains(&"README.TXT".to_string()),
            "unrelated README.TXT survives"
        );

        let cmd = entries.iter().find(|e| e.name == "COMMAND.X").unwrap();
        let got = fs.read_file(cmd, command.len() + 16).unwrap();
        assert_eq!(got.len(), command.len(), "COMMAND.X full size");
        assert_eq!(&got[..2], b"HU", "Human68k .X header intact");
        assert_eq!(got, command, "multi-cluster COMMAND.X is byte-exact");

        let rd = entries.iter().find(|e| e.name == "README.TXT").unwrap();
        let got_rd = fs.read_file(rd, readme.len() + 16).unwrap();
        assert_eq!(got_rd, readme, "README.TXT is byte-exact");
    }

    #[test]
    fn resize_human68k_grows_sharp_kg_volume_and_preserves_multicluster_file() {
        let (mut disk, command, readme) = build_sharp_kg_fat16_resizable();
        let new_total: usize = 16_400; // forces spf 32 -> 64 (FAT extension + data shift)
        disk.resize(new_total * 512, 0);

        let mut cur = Cursor::new(&mut disk);
        let did = resize_human68k_in_place(&mut cur, 0, new_total as u64 * 512, &mut |_| {})
            .expect("grow ok");
        assert!(did, "SHARP/KG grow performs a resize");

        // BPB now reports the new size, the FAT grew, and clusters stay FAT16.
        let fs = Human68kFilesystem::open(Cursor::new(&disk), 0).expect("reopen");
        assert_eq!(fs.bpb.total_sectors, new_total as u32);
        assert!(fs.bpb.fat_sectors > 32, "FAT must have grown");
        let clusters =
            (new_total as u32 - fs.bpb.data_start_sector()) / fs.bpb.sectors_per_cluster as u32;
        assert!(clusters > FAT16_FLOOR);
        drop(fs);

        assert_seeded_files_intact(&disk, &command, &readme);
    }

    #[test]
    fn resize_human68k_shrinks_sharp_kg_volume_and_preserves_files() {
        let (mut disk, command, readme) = build_sharp_kg_fat16_resizable();
        // Grow first (so the FAT is oversized), then shrink to a size still
        // well above the FAT16 floor.
        let grown: usize = 16_400;
        disk.resize(grown * 512, 0);
        {
            let mut cur = Cursor::new(&mut disk);
            resize_human68k_in_place(&mut cur, 0, grown as u64 * 512, &mut |_| {}).unwrap();
        }
        let shrunk: usize = 9_000;
        {
            let mut cur = Cursor::new(&mut disk);
            let did = resize_human68k_in_place(&mut cur, 0, shrunk as u64 * 512, &mut |_| {})
                .expect("shrink ok");
            assert!(did, "SHARP/KG shrink performs a resize");
        }
        let fs = Human68kFilesystem::open(Cursor::new(&disk), 0).unwrap();
        assert_eq!(fs.bpb.total_sectors, shrunk as u32);
        drop(fs);
        assert_seeded_files_intact(&disk, &command, &readme);
    }

    #[test]
    fn resize_human68k_round_trip_grow_then_shrink_is_byte_exact() {
        // Mirrors the manual verification: grow then shrink, files survive.
        let (mut disk, command, readme) = build_sharp_kg_fat16_resizable();
        disk.resize(20_000 * 512, 0);
        {
            let mut cur = Cursor::new(&mut disk);
            resize_human68k_in_place(&mut cur, 0, 18_000 * 512, &mut |_| {}).expect("grow");
        }
        {
            let mut cur = Cursor::new(&mut disk);
            resize_human68k_in_place(&mut cur, 0, 8_300 * 512, &mut |_| {}).expect("shrink");
        }
        let fs = Human68kFilesystem::open(Cursor::new(&disk), 0).unwrap();
        assert_eq!(fs.bpb.total_sectors, 8_300);
        drop(fs);
        assert_seeded_files_intact(&disk, &command, &readme);
    }

    #[test]
    fn resize_human68k_refuses_shrink_below_fat16_floor() {
        let (mut disk, _command, _readme) = build_sharp_kg_fat16_resizable();
        let mut cur = Cursor::new(&mut disk);
        // 256 KiB / 512 = 512 sectors -> far below 4085 clusters.
        let err = resize_human68k_in_place(&mut cur, 0, 256 * 1024, &mut |_| {}).unwrap_err();
        assert!(
            err.to_string().contains("FAT16 minimum"),
            "shrink into FAT12 territory must be refused, got: {err}"
        );
    }

    #[test]
    fn resize_human68k_ignores_little_endian_floppy_bpb() {
        // A standard LE Human68k floppy (0xEB jump) is resize_fat_in_place's
        // job; resize_human68k_in_place must decline it.
        let mut disk = build_fat12_synthetic();
        disk.resize(2880 * 512, 0);
        let mut cur = Cursor::new(&mut disk);
        let did = resize_human68k_in_place(&mut cur, 0, 2880 * 512, &mut |_| {}).unwrap();
        assert!(
            !did,
            "LE floppy BPB is declined (handled by resize_fat_in_place)"
        );
    }

    #[test]
    fn resize_human68k_noop_when_size_unchanged() {
        let (mut disk, _c, _r) = build_sharp_kg_fat16_resizable();
        let same = disk.len() as u64;
        let mut cur = Cursor::new(&mut disk);
        let did = resize_human68k_in_place(&mut cur, 0, same, &mut |_| {}).unwrap();
        assert!(!did, "same-size resize is a no-op");
    }
}
