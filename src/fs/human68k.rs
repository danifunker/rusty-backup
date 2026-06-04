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
//! - BPB at sector 0 — identical layout to FAT12/16 (jump, OEM, bytes
//!   per sector, etc). The X68000's IPL is in the boot bytes.
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
//! Multi-byte Shift-JIS bytes occupy two slots: the first byte is in
//! the range `0x81..0x9F` or `0xE0..0xFC`, the second is `0x40..0xFC`
//! (excluding `0x7F`). For display we transcode ASCII verbatim and
//! emit Shift-JIS pairs as `\uXXXX` placeholders; the raw bytes are
//! retained for round-trip writes.

use std::io::{Read, Seek, SeekFrom};

use byteorder::{ByteOrder, LittleEndian};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FatKind {
    Fat12,
    Fat16,
}

impl Human68kBpb {
    pub fn parse(bpb_buf: &[u8; 512]) -> Result<Self, FilesystemError> {
        let bytes_per_sector = LittleEndian::read_u16(&bpb_buf[11..13]);
        if !matches!(bytes_per_sector, 256 | 512 | 1024 | 2048) {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k BPB bytes_per_sector {bytes_per_sector} not in {{256,512,1024,2048}}"
            )));
        }
        let sectors_per_cluster = bpb_buf[13];
        if sectors_per_cluster == 0 || !sectors_per_cluster.is_power_of_two() {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k BPB sectors_per_cluster {sectors_per_cluster} must be a power of two"
            )));
        }
        let reserved_sectors = LittleEndian::read_u16(&bpb_buf[14..16]);
        let num_fats = bpb_buf[16];
        if num_fats == 0 || num_fats > 2 {
            return Err(FilesystemError::InvalidData(format!(
                "Human68k BPB num_fats {num_fats} not 1 or 2"
            )));
        }
        let root_entries = LittleEndian::read_u16(&bpb_buf[17..19]);
        let small_total = LittleEndian::read_u16(&bpb_buf[19..21]) as u32;
        let fat_sectors = LittleEndian::read_u16(&bpb_buf[22..24]);
        let big_total = LittleEndian::read_u32(&bpb_buf[32..36]);
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

/// Lossy Shift-JIS decode for display only. ASCII verbatim, double-byte
/// pairs become `?` so users see "something is non-ASCII here" instead
/// of garbage. Round-trip writes go through `raw_name` / `raw_ext`.
fn shift_jis_lossy_display(name: &[u8], ext: &[u8]) -> String {
    let n = lossy_one(name);
    let e = lossy_one(ext);
    if e.is_empty() {
        n
    } else {
        format!("{n}.{e}")
    }
}

fn lossy_one(bytes: &[u8]) -> String {
    let mut s = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if (0x20..=0x7E).contains(&b) {
            s.push(b as char);
            i += 1;
        } else if is_sjis_first_byte(b) && i + 1 < bytes.len() {
            // Shift-JIS double-byte; emit placeholder.
            s.push('?');
            i += 2;
        } else {
            s.push('_');
            i += 1;
        }
    }
    s
}

fn is_sjis_first_byte(b: u8) -> bool {
    matches!(b, 0x81..=0x9F | 0xE0..=0xFC)
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
                u16::from_le_bytes([self.fat_bytes[off], self.fat_bytes[off + 1]])
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

    /// Read root-directory sectors and parse all 32-byte entries.
    fn read_root_directory(&mut self) -> Result<Vec<Human68kDirEntry>, FilesystemError> {
        let root_off = self.partition_offset
            + self.bpb.root_dir_sector() as u64 * self.bpb.bytes_per_sector as u64;
        let root_byte_len = self.bpb.root_entries as u64 * DIR_ENTRY_SIZE as u64;
        self.reader.seek(SeekFrom::Start(root_off))?;
        let mut buf = vec![0u8; root_byte_len as usize];
        self.reader.read_exact(&mut buf)?;
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
                if !e.is_volume_label() {
                    out.push(e);
                }
            }
        }
        Ok(out)
    }
}

impl<R: Read + Seek + Send> Filesystem for Human68kFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new()); // subdirs left as future work
        }
        let dir = self.read_root_directory()?;
        let mut out = Vec::with_capacity(dir.len());
        for de in dir {
            let name = de.display_name.clone();
            let mut fe = if de.is_directory() {
                FileEntry::new_directory(name.clone(), format!("/{name}"), de.first_cluster as u64)
            } else {
                FileEntry::new_file(
                    name.clone(),
                    format!("/{name}"),
                    de.size as u64,
                    de.first_cluster as u64,
                )
            };
            if de.attr & attr::READ_ONLY != 0 {
                fe.special_type = Some("R/O".to_string());
            }
            out.push(fe);
        }
        out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
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
    fn shift_jis_double_byte_renders_as_placeholder() {
        // Build a name with one ASCII char + one Shift-JIS double-byte
        // pair (0x82 0xA0 is 'あ'). Expect "A?" display + "A?" extension.
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
        // Name byte 0 = 'A' (ASCII), bytes 1..3 = SJIS pair = '?'.
        assert_eq!(parsed.display_name, "A?");
        // Raw bytes preserved for round-trip.
        assert_eq!(&parsed.raw_name, b"A\x82\xA0");
    }
}
