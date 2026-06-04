//! QDOS — the Sinclair QL filesystem. Big-endian throughout (m68k).
//!
//! The MiSTer QL core mounts two media:
//!
//! - **Microdrive cartridges** (`.mdv`, ~100 KB) — covered in
//!   [`super::qdos_mdv`]: per-sector records, completely different
//!   layout from QXL.WIN.
//! - **QXL.WIN HDD containers** — Sinclair's hard-disk volume format,
//!   shipped with the QL Gold Card, Trump Card, and the MiSTer QL
//!   core's QLSD interface. **This** is what we implement here.
//!
//! ## QXL.WIN layout (canonical — matches sQLux `QDisk.c` + MiSTer-QL
//! samples downloaded 2026-06-04 from <https://www.kilgus.net/ql/mister/>)
//!
//! ```text
//! Header (64 B, BE):
//!   0x00..0x04  signature "QLWA"
//!   0x04..0x06  random / interleave (typically 0x0005)
//!   0x06..0x1A  volume name (20 bytes ASCII, space-padded)
//!   0x1A..0x1E  unused (4 bytes)
//!   0x1E..0x22  random seed / timestamp (4 bytes)
//!   0x22..0x24  spc — sectors per cluster (cluster_size = spc × 512)
//!   0x24..0x2A  ignored geometry hint (sectors per track etc.)
//!   0x2A..0x2C  cc  — total cluster count
//!   0x2C..0x2E  fc  — free clusters
//!   0x2E..0x30  spm — sectors per allocation map
//!   0x30..0x32  number of allocation maps
//!   0x32..0x34  ffc — first free cluster (head of free chain)
//!   0x34..0x36  root_cluster — first cluster of root directory
//!   0x36..0x3A  rlen — root directory length in BYTES (u32)
//!   0x3A..0x40  padding (6 bytes)
//!   0x40..      FAT begins (cc × u16 BE entries; cluster N's slot at
//!               0x40 + N × 2)
//! ```
//!
//! ### FAT semantics
//!
//! Each cluster has one 16-bit BE slot in the FAT. The chain ends when
//! the slot reads back as 0 (no-next-cluster). Clusters 0..K hold the
//! header + FAT itself; their FAT slots form a self-chain
//! (0→1→2→...→K→0) by convention.
//!
//! ### Directory layout
//!
//! The directory is a **file like any other**: starting cluster is
//! `header.root_cluster`, total byte length is `header.rlen`, follow
//! the FAT chain to read all entries. Each entry is 64 bytes:
//!
//! ```text
//!   0x00..0x04  file length (u32 BE)
//!   0x04..0x06  access keys / lock flags (u16)
//!   0x06..0x08  file type (0=data, 1=exec, 2=reloc, 3=device, ...)
//!   0x08..0x0E  data area (6 bytes, file-type-specific)
//!   0x0E..0x10  name length (u16 BE)
//!   0x10..0x34  name (36 bytes ASCII, truncated to name length)
//!   0x34..0x38  date last modified (u32 — QDOS epoch 1961-01-01)
//!   0x38..0x3A  file version (u16)
//!   0x3A..0x3C  first cluster (u16 BE) — sQLux QWDE_FNUM
//!   0x3C..0x40  backup date (u32) / trailer
//! ```
//!
//! Cluster N's first byte lives at byte `N × cluster_size` from the
//! start of the partition. Cluster 0 holds the header. The first
//! entry (slot 0) is conventionally a self-reference and may carry
//! length 0 — skip it during enumeration.

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
    /// Volume name (20 bytes on disk, space-padded; trailing spaces
    /// trimmed for display).
    pub volume_name: String,
    /// Sectors per cluster — cluster_size_bytes = `spc × 512`.
    pub spc: u16,
    /// Total cluster count (FAT entry count).
    pub cc: u16,
    /// Free clusters (header bookkeeping; not authoritative — walk the
    /// FAT to confirm).
    pub fc: u16,
    /// First cluster of the root directory.
    pub root_cluster: u16,
    /// Root directory length in bytes.
    pub rlen: u32,
}

impl QxlWinHeader {
    pub fn parse(buf: &[u8; HEADER_BYTES]) -> Result<Self, FilesystemError> {
        if &buf[0..4] != QXLWIN_SIGNATURE {
            return Err(FilesystemError::InvalidData(format!(
                "QXL.WIN signature mismatch: {:?}",
                &buf[0..4]
            )));
        }
        let spc = BigEndian::read_u16(&buf[0x22..0x24]);
        let cc = BigEndian::read_u16(&buf[0x2A..0x2C]);
        let root_cluster = BigEndian::read_u16(&buf[0x34..0x36]);
        let rlen = BigEndian::read_u32(&buf[0x36..0x3A]);
        if spc == 0 {
            return Err(FilesystemError::InvalidData(
                "QXL.WIN spc (sectors per cluster) is zero".into(),
            ));
        }
        if cc == 0 {
            return Err(FilesystemError::InvalidData(
                "QXL.WIN cc (total cluster count) is zero".into(),
            ));
        }
        if root_cluster == 0 {
            return Err(FilesystemError::InvalidData(
                "QXL.WIN root_cluster is zero (no directory)".into(),
            ));
        }
        Ok(Self {
            volume_name: parse_space_padded_name(&buf[0x06..0x1A]),
            spc,
            cc,
            fc: BigEndian::read_u16(&buf[0x2C..0x2E]),
            root_cluster,
            rlen,
        })
    }

    pub fn cluster_size(&self) -> u64 {
        self.spc as u64 * SECTOR_BYTES
    }

    /// FAT starts immediately after the 64-byte header.
    pub fn fat_byte_offset(&self) -> u64 {
        HEADER_BYTES as u64
    }

    pub fn fat_byte_length(&self) -> u64 {
        self.cc as u64 * 2
    }
}

fn parse_space_padded_name(buf: &[u8]) -> String {
    buf.iter()
        .map(|&b| {
            if (0x20..=0x7E).contains(&b) {
                b as char
            } else if b == 0x00 {
                ' '
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_end()
        .to_string()
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
    // Name length at offset 0x0E (per sQLux QDisk.c + verified against
    // kilgus QXL.WIN sample: entries show name length in this slot, name
    // body at 0x10).
    let name_len = BigEndian::read_u16(&buf[0x0E..0x10]) as usize;
    if name_len == 0 || name_len > 36 {
        return None;
    }
    let file_length = BigEndian::read_u32(&buf[0x00..0x04]);
    let access_keys = BigEndian::read_u16(&buf[0x04..0x06]);
    let file_type = BigEndian::read_u16(&buf[0x06..0x08]);
    // First cluster of file at offset 0x3A (sQLux QWDE_FNUM).
    let first_block = BigEndian::read_u16(&buf[0x3A..0x3C]);
    let name_bytes = &buf[0x10..0x10 + name_len.min(36)];
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

    /// Byte offset (from start of partition) of cluster `c`'s first
    /// byte. Cluster 0 lives at byte 0 (the header itself).
    fn cluster_byte_offset(&self, c: u16) -> u64 {
        self.partition_offset + c as u64 * self.header.cluster_size()
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
                    "QDOS FAT cycle at cluster {block}"
                )));
            }
            out.push(block);
            block = self.fat_lookup(block);
        }
        Ok(out)
    }

    /// Read `len` bytes from the file chain starting at `first_cluster`.
    /// Stops early if the chain terminates.
    fn read_chain_bytes(
        &mut self,
        first_cluster: u16,
        len: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let chain = self.chain(first_cluster)?;
        let cluster_size = self.header.cluster_size() as usize;
        let mut buf = Vec::with_capacity(len);
        let mut remaining = len;
        for cluster in chain {
            if remaining == 0 {
                break;
            }
            let want = cluster_size.min(remaining);
            let off = self.cluster_byte_offset(cluster);
            self.reader.seek(SeekFrom::Start(off))?;
            let mut chunk = vec![0u8; want];
            self.reader.read_exact(&mut chunk)?;
            buf.extend_from_slice(&chunk);
            remaining = remaining.saturating_sub(want);
        }
        Ok(buf)
    }

    fn read_directory(&mut self) -> Result<Vec<QdosDirEntry>, FilesystemError> {
        let rlen = self.header.rlen as usize;
        let root = self.header.root_cluster;
        let raw = self.read_chain_bytes(root, rlen)?;
        // Slot 0 of the root directory is a self-reference on real
        // QXL.WIN volumes (length 0, name length 0, all-zero data area).
        // `parse_dir_entry` returns None for it via the name-length=0
        // check, so we don't need a special-case here.
        let mut entries = Vec::new();
        for slot in raw.chunks_exact(DIR_ENTRY_SIZE) {
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
        let want = (entry.size as usize).min(max_bytes);
        self.read_chain_bytes(first, want)
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
        self.header.cc as u64 * self.header.cluster_size()
    }

    fn used_size(&self) -> u64 {
        self.header.cc.saturating_sub(self.header.fc) as u64 * self.header.cluster_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a tiny synthetic QXL.WIN volume that mirrors the canonical
    /// on-disk layout (verified against kilgus QXL.WIN + sQLux source):
    ///   - 1-sector-per-cluster (cluster size = 512 B)
    ///   - 16 clusters total
    ///   - cluster 0 holds header + FAT
    ///   - cluster 1 = root directory (rlen = 128 B = 2 entries)
    ///   - cluster 5 = file "HELLO_QL" data (32 B)
    pub fn build_synthetic_qxlwin() -> Vec<u8> {
        const CC: u16 = 16;
        const SPC: u16 = 1; // 512-B clusters
        const CLUSTER_SIZE: usize = SPC as usize * SECTOR_BYTES as usize;
        const ROOT_CLUSTER: u16 = 1;
        const ROOT_LEN: u32 = 128; // 2 entries × 64 B
        const FILE_CLUSTER: u16 = 5;

        let mut disk = vec![0u8; CC as usize * CLUSTER_SIZE];

        // Header
        disk[0..4].copy_from_slice(QXLWIN_SIGNATURE);
        BigEndian::write_u16(&mut disk[0x04..0x06], 0x0005);
        // Volume name at offset 0x06, 20 bytes space-padded.
        let mut name = [b' '; 20];
        name[..6].copy_from_slice(b"DemoQL");
        disk[0x06..0x1A].copy_from_slice(&name);
        BigEndian::write_u16(&mut disk[0x22..0x24], SPC);
        BigEndian::write_u16(&mut disk[0x2A..0x2C], CC);
        BigEndian::write_u16(&mut disk[0x2C..0x2E], 8); // fc — arbitrary
        BigEndian::write_u16(&mut disk[0x32..0x34], 6); // ffc — arbitrary
        BigEndian::write_u16(&mut disk[0x34..0x36], ROOT_CLUSTER);
        BigEndian::write_u32(&mut disk[0x36..0x3A], ROOT_LEN);

        // FAT at byte 0x40 (cluster 0's data area). Mark root chain
        // terminator (cluster ROOT_CLUSTER → 0) and file chain
        // terminator (cluster FILE_CLUSTER → 0). All other entries
        // already 0 from the zero-fill — interpreted as free.
        let fat_off = HEADER_BYTES;
        // ROOT_CLUSTER's slot is at fat_off + ROOT_CLUSTER * 2.
        BigEndian::write_u16(
            &mut disk[fat_off + ROOT_CLUSTER as usize * 2..fat_off + ROOT_CLUSTER as usize * 2 + 2],
            0,
        );
        BigEndian::write_u16(
            &mut disk[fat_off + FILE_CLUSTER as usize * 2..fat_off + FILE_CLUSTER as usize * 2 + 2],
            0,
        );

        // Root directory: lives in cluster ROOT_CLUSTER = byte 512.
        // Slot 0 = volume self-reference (leave zeroed; parser skips
        // it via name_len == 0).
        // Slot 1 = HELLO_QL file.
        let dir_off = ROOT_CLUSTER as usize * CLUSTER_SIZE;
        let slot1 = dir_off + DIR_ENTRY_SIZE;
        BigEndian::write_u32(&mut disk[slot1..slot1 + 4], 32); // file length
        BigEndian::write_u16(&mut disk[slot1 + 4..slot1 + 6], 0); // access keys
        BigEndian::write_u16(&mut disk[slot1 + 6..slot1 + 8], 0); // type = data
        BigEndian::write_u16(&mut disk[slot1 + 0x0E..slot1 + 0x10], 8); // name len
        disk[slot1 + 0x10..slot1 + 0x18].copy_from_slice(b"HELLO_QL");
        BigEndian::write_u16(&mut disk[slot1 + 0x3A..slot1 + 0x3C], FILE_CLUSTER);

        // File data at cluster FILE_CLUSTER = byte 5 * 512 = 2560.
        let file_off = FILE_CLUSTER as usize * CLUSTER_SIZE;
        let payload = b"qdos qxlwin synthetic content 32";
        disk[file_off..file_off + payload.len()].copy_from_slice(payload);
        disk
    }

    #[test]
    fn parses_qxlwin_header_and_recognises_signature() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let fs = QdosFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.header.spc, 1);
        assert_eq!(fs.header.cc, 16);
        assert_eq!(fs.header.root_cluster, 1);
        assert_eq!(fs.header.rlen, 128);
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
