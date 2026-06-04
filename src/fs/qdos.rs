//! QDOS — the Sinclair QL filesystem. Big-endian throughout (m68k).
//!
//! The MiSTer QL core mounts two media:
//!
//! - **Microdrive cartridges** (`.mdv`, ~100 KB) — a stream of 60-byte
//!   records with per-sector headers. Not handled in this extract floor;
//!   it requires walking the per-sector record headers and is intricate.
//! - **QXL.WIN HDD containers** — Sinclair's hard-disk volume format,
//!   shipped with the QL Gold Card and the Trump Card. **This** is what
//!   we implement here.
//!
//! ## QXL.WIN layout
//!
//! ```text
//! Header (64 B, BE):
//!   0..4    signature "QLWA"
//!   4..6    random
//!   6..8    file count
//!   8..10   directory length (in sectors)
//!   10..12  directory free
//!   12..14  total free
//!   14..16  total blocks
//!   16..18  sectors per group
//!   18..20  sectors per track
//!   20..22  tracks
//!   22..24  heads
//!   24..38  volume name (Pascal-style, 1 length + 13 bytes)
//!
//! FAT — 16-bit big-endian entries, one per block. 0xFFF8..0xFFFF =
//! end of chain; 0 = free.
//!
//! Directory entries (64 B each):
//!   0..4    file length
//!   4..6    file access keys
//!   6..8    file type (0 = data, 1 = exec, 2 = relocatable, 3 = device)
//!   8..16   data area (8 bytes reserved by QDOS)
//!   16..18  reserved
//!   18..22  date (Mac-era seconds since 1961-01-01)
//!   22..24  name length (BE u16)
//!   24..60  name (≤ 36 bytes ASCII)
//!   60..64  trailer
//! ```
//!
//! Multiple-sector files chain via the FAT; the first cluster is in the
//! directory entry. Block size is `sectors_per_group * 512`.

use std::io::{Read, Seek, SeekFrom};

use byteorder::{BigEndian, ByteOrder};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// QXL.WIN signature at byte 0.
pub const QXLWIN_SIGNATURE: &[u8; 4] = b"QLWA";

const HEADER_BYTES: usize = 64;
const FAT_END_LO: u16 = 0xFFF8;
const DIR_ENTRY_SIZE: usize = 64;
/// Logical sector size for QDOS volumes.
const SECTOR_BYTES: u64 = 512;
const MAX_CHAIN_HOPS: usize = 65536;

#[derive(Debug, Clone)]
pub struct QxlWinHeader {
    pub file_count: u16,
    pub dir_length_sectors: u16,
    pub dir_free: u16,
    pub total_free: u16,
    pub total_blocks: u16,
    pub sectors_per_group: u16,
    pub sectors_per_track: u16,
    pub tracks: u16,
    pub heads: u16,
    pub volume_name: String,
}

impl QxlWinHeader {
    pub fn parse(buf: &[u8; HEADER_BYTES]) -> Result<Self, FilesystemError> {
        if &buf[0..4] != QXLWIN_SIGNATURE {
            return Err(FilesystemError::InvalidData(format!(
                "QXL.WIN signature mismatch: {:?}",
                &buf[0..4]
            )));
        }
        Ok(Self {
            file_count: BigEndian::read_u16(&buf[6..8]),
            dir_length_sectors: BigEndian::read_u16(&buf[8..10]),
            dir_free: BigEndian::read_u16(&buf[10..12]),
            total_free: BigEndian::read_u16(&buf[12..14]),
            total_blocks: BigEndian::read_u16(&buf[14..16]),
            sectors_per_group: BigEndian::read_u16(&buf[16..18]),
            sectors_per_track: BigEndian::read_u16(&buf[18..20]),
            tracks: BigEndian::read_u16(&buf[20..22]),
            heads: BigEndian::read_u16(&buf[22..24]),
            volume_name: parse_pascal_name(&buf[24..38]),
        })
    }

    pub fn block_size(&self) -> u64 {
        self.sectors_per_group as u64 * SECTOR_BYTES
    }

    /// FAT starts immediately after the 64-byte header.
    pub fn fat_byte_offset(&self) -> u64 {
        HEADER_BYTES as u64
    }

    pub fn fat_byte_length(&self) -> u64 {
        self.total_blocks as u64 * 2
    }

    /// Directory starts after the FAT.
    pub fn dir_byte_offset(&self) -> u64 {
        self.fat_byte_offset() + self.fat_byte_length()
    }

    pub fn dir_byte_length(&self) -> u64 {
        self.dir_length_sectors as u64 * SECTOR_BYTES
    }

    pub fn data_byte_offset(&self) -> u64 {
        self.dir_byte_offset() + self.dir_byte_length()
    }
}

fn parse_pascal_name(buf: &[u8]) -> String {
    if buf.is_empty() {
        return String::new();
    }
    let len = (buf[0] as usize).min(buf.len() - 1);
    let s: String = buf[1..1 + len]
        .iter()
        .map(|&b| {
            if (0x20..=0x7E).contains(&b) {
                b as char
            } else {
                '_'
            }
        })
        .collect();
    s.trim_end().to_string()
}

#[derive(Debug, Clone)]
pub struct QdosDirEntry {
    pub file_length: u32,
    pub access_keys: u16,
    pub file_type: u16,
    pub name: String,
    pub first_block: u16,
}

pub fn parse_dir_entry(buf: &[u8; DIR_ENTRY_SIZE]) -> Option<QdosDirEntry> {
    let name_len = BigEndian::read_u16(&buf[22..24]) as usize;
    if name_len == 0 || name_len > 36 {
        return None;
    }
    let file_length = BigEndian::read_u32(&buf[0..4]);
    let access_keys = BigEndian::read_u16(&buf[4..6]);
    let file_type = BigEndian::read_u16(&buf[6..8]);
    // The QXL.WIN "data area" at bytes 8..16 holds the first allocation
    // block index in the last two bytes (big-endian) per Sinclair's
    // hard-disk format docs.
    let first_block = BigEndian::read_u16(&buf[14..16]);
    let name_bytes = &buf[24..24 + name_len.min(36)];
    let name: String = name_bytes
        .iter()
        .map(|&b| {
            if (0x20..=0x7E).contains(&b) {
                b as char
            } else {
                '_'
            }
        })
        .collect();
    Some(QdosDirEntry {
        file_length,
        access_keys,
        file_type,
        name,
        first_block,
    })
}

pub struct QdosFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    pub header: QxlWinHeader,
    fat_bytes: Vec<u8>,
}

impl<R: Read + Seek + Send> QdosFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset))?;
        let mut hbuf = [0u8; HEADER_BYTES];
        reader.read_exact(&mut hbuf)?;
        let header = QxlWinHeader::parse(&hbuf)?;
        // Read the FAT.
        reader.seek(SeekFrom::Start(partition_offset + header.fat_byte_offset()))?;
        let mut fat = vec![0u8; header.fat_byte_length() as usize];
        reader.read_exact(&mut fat)?;
        Ok(Self {
            reader,
            partition_offset,
            header,
            fat_bytes: fat,
        })
    }

    fn fat_lookup(&self, block: u16) -> u16 {
        let off = block as usize * 2;
        if off + 1 >= self.fat_bytes.len() {
            return 0xFFFF;
        }
        u16::from_be_bytes([self.fat_bytes[off], self.fat_bytes[off + 1]])
    }

    fn block_byte_offset(&self, block: u16) -> u64 {
        self.partition_offset
            + self.header.data_byte_offset()
            + block as u64 * self.header.block_size()
    }

    fn chain(&self, first: u16) -> Result<Vec<u16>, FilesystemError> {
        let mut out = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut block = first;
        for _ in 0..MAX_CHAIN_HOPS {
            if block == 0 || block >= FAT_END_LO {
                break;
            }
            if !visited.insert(block) {
                return Err(FilesystemError::InvalidData(format!(
                    "QDOS FAT cycle at block {block}"
                )));
            }
            out.push(block);
            block = self.fat_lookup(block);
        }
        Ok(out)
    }

    fn read_directory(&mut self) -> Result<Vec<QdosDirEntry>, FilesystemError> {
        let dir_off = self.partition_offset + self.header.dir_byte_offset();
        let dir_len = self.header.dir_byte_length();
        self.reader.seek(SeekFrom::Start(dir_off))?;
        let mut buf = vec![0u8; dir_len as usize];
        self.reader.read_exact(&mut buf)?;
        let mut entries = Vec::new();
        for slot in buf.chunks_exact(DIR_ENTRY_SIZE) {
            let arr: &[u8; DIR_ENTRY_SIZE] = slot.try_into().unwrap();
            if let Some(e) = parse_dir_entry(arr) {
                entries.push(e);
            }
        }
        Ok(entries)
    }
}

impl<R: Read + Seek + Send> Filesystem for QdosFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new()); // QDOS subdirs deferred
        }
        let dir = self.read_directory()?;
        let mut out = Vec::with_capacity(dir.len());
        for de in dir {
            let path = format!("/{}", de.name);
            let mut fe = FileEntry::new_file(
                de.name.clone(),
                path,
                de.file_length as u64,
                de.first_block as u64,
            );
            match de.file_type {
                1 => fe.special_type = Some("Exec".into()),
                2 => fe.special_type = Some("Reloc".into()),
                3 => fe.special_type = Some("Dev".into()),
                _ => {}
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
        let chain = self.chain(first)?;
        let mut data = Vec::with_capacity(entry.size as usize);
        let bs = self.header.block_size() as usize;
        let mut remaining = entry.size as usize;
        for block in chain {
            if remaining == 0 {
                break;
            }
            let want = bs.min(remaining);
            let off = self.block_byte_offset(block);
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
        "QDOS (QXL.WIN)"
    }

    fn volume_label(&self) -> Option<&str> {
        if self.header.volume_name.is_empty() {
            None
        } else {
            Some(&self.header.volume_name)
        }
    }

    fn total_size(&self) -> u64 {
        self.header.total_blocks as u64 * self.header.block_size()
    }

    fn used_size(&self) -> u64 {
        (self.header.total_blocks - self.header.total_free) as u64 * self.header.block_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a tiny synthetic QXL.WIN volume:
    ///   - 16 blocks of 512 B each = 8 KB
    ///   - one file "HELLO_QL" pointing at block 5 with 32 bytes
    fn build_synthetic_qxlwin() -> Vec<u8> {
        const BLOCKS: u16 = 16;
        const SECTORS_PER_GROUP: u16 = 1; // 512-B blocks
        const DIR_SECTORS: u16 = 1;

        let mut disk = vec![
            0u8;
            HEADER_BYTES
                + BLOCKS as usize * 2
                + DIR_SECTORS as usize * SECTOR_BYTES as usize
                + BLOCKS as usize * SECTOR_BYTES as usize
        ];

        // Header
        disk[0..4].copy_from_slice(QXLWIN_SIGNATURE);
        BigEndian::write_u16(&mut disk[6..8], 1); // file count
        BigEndian::write_u16(&mut disk[8..10], DIR_SECTORS);
        BigEndian::write_u16(&mut disk[10..12], 7);
        BigEndian::write_u16(&mut disk[12..14], BLOCKS - 1);
        BigEndian::write_u16(&mut disk[14..16], BLOCKS);
        BigEndian::write_u16(&mut disk[16..18], SECTORS_PER_GROUP);
        BigEndian::write_u16(&mut disk[18..20], 16);
        BigEndian::write_u16(&mut disk[20..22], 4);
        BigEndian::write_u16(&mut disk[22..24], 1);
        // Pascal-style name: 1 length byte + chars.
        disk[24] = 8;
        disk[25..33].copy_from_slice(b"DemoQL  ");

        // FAT at byte 64. Mark block 5 -> 0xFFFF (end of chain).
        let fat_off = HEADER_BYTES;
        BigEndian::write_u16(&mut disk[fat_off + 10..fat_off + 12], 0xFFFF);

        // Directory at byte 64 + 16*2 = 96.
        let dir_off = HEADER_BYTES + BLOCKS as usize * 2;
        BigEndian::write_u32(&mut disk[dir_off..dir_off + 4], 32); // file length
        BigEndian::write_u16(&mut disk[dir_off + 6..dir_off + 8], 0); // type = data
        BigEndian::write_u16(&mut disk[dir_off + 14..dir_off + 16], 5); // first block
        BigEndian::write_u16(&mut disk[dir_off + 22..dir_off + 24], 8); // name length
        disk[dir_off + 24..dir_off + 32].copy_from_slice(b"HELLO_QL");

        // File data at block 5 = byte data_off + 5*512.
        let data_off =
            HEADER_BYTES + BLOCKS as usize * 2 + DIR_SECTORS as usize * SECTOR_BYTES as usize;
        let block_5 = data_off + 5 * SECTOR_BYTES as usize;
        let payload = b"qdos qxlwin synthetic content 32";
        disk[block_5..block_5 + payload.len()].copy_from_slice(payload);
        disk
    }

    #[test]
    fn parses_qxlwin_header_and_recognises_signature() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let fs = QdosFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.header.file_count, 1);
        assert_eq!(fs.header.total_blocks, 16);
        assert_eq!(fs.volume_label(), Some("DemoQL"));
        assert_eq!(fs.fs_type(), "QDOS (QXL.WIN)");
    }

    #[test]
    fn lists_directory_with_synthetic_file() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let mut fs = QdosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "HELLO_QL");
        assert_eq!(entries[0].size, 32);
    }

    #[test]
    fn reads_synthetic_file_byte_exact() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let mut fs = QdosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let data = fs.read_file(&entries[0], 1024).unwrap();
        assert_eq!(&data, b"qdos qxlwin synthetic content 32");
    }

    #[test]
    fn rejects_non_qxlwin_signature() {
        let disk = vec![0u8; 256];
        let cur = Cursor::new(disk);
        let result = QdosFilesystem::open(cur, 0);
        let err = match result {
            Ok(_) => panic!("expected error for non-QXL.WIN bytes"),
            Err(e) => e,
        };
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }
}
