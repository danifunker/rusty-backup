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
//! Free clusters form a SECOND linked list rooted at `header.ffc`:
//! `ffc` points to the first free cluster, and each free cluster's FAT
//! slot points to the next free cluster. Last free cluster's slot is 0.
//! Both file chains and the free chain terminate with slot=0 — they
//! are distinguished only by their entry point (free-list root vs. a
//! directory entry's FNUM). Allocation pops from the head of the free
//! list; deletion pushes back to the head. Source: sQLux QDisk.c
//! `QLWA_GetFreeBlock` / `QLWA_KillFile`.
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

use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

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
    /// Free clusters (header bookkeeping). Mutated by allocator /
    /// deletion; re-written to disk via `header_write_back`.
    pub fc: u16,
    /// First free cluster — head of the free-cluster linked list. 0
    /// means the free list is empty (volume full).
    pub ffc: u16,
    /// First cluster of the root directory.
    pub root_cluster: u16,
    /// Root directory length in bytes. Mutated when the directory
    /// grows on file creation.
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
        let ffc = BigEndian::read_u16(&buf[0x32..0x34]);
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
            ffc,
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

// ----------------------------------------------------------------------------
// Write-path primitives + EditableFilesystem impl.
//
// Allocation rule: pop from the head of the free list (clusters linked via
// FAT slots starting at `header.ffc`). Deletion: push freed chain back to
// the head. Directory growth: chain a new cluster onto the root chain and
// bump `header.rlen`. All on-disk mutations are in-memory + per-call
// FAT/header write-back; callers must still hit `sync_metadata` to flush.
//
// Source-of-truth: sQLux `QDisk.c` (`QLWA_GetFreeBlock` /
// `QLWA_CreateNewFile` / `QLWA_KillFile`).
// ----------------------------------------------------------------------------

impl<R: Read + Write + Seek + Send> QdosFilesystem<R> {
    /// Write a 16-bit value into FAT slot for `cluster`.
    fn fat_set(&mut self, cluster: u16, value: u16) {
        let off = cluster as usize * 2;
        if off + 2 <= self.fat_bytes.len() {
            self.fat_bytes[off..off + 2].copy_from_slice(&value.to_be_bytes());
        }
    }

    /// Pop `count` clusters from the free list, link them into a new chain,
    /// and return the first cluster. The tail's FAT slot is set to 0 (end of
    /// chain) and `header.{ffc,fc}` are updated.
    fn alloc_chain(&mut self, count: u16) -> Result<u16, FilesystemError> {
        if count == 0 {
            return Ok(0);
        }
        if self.header.fc < count {
            return Err(FilesystemError::DiskFull(format!(
                "QDOS: need {} clusters, only {} free",
                count, self.header.fc
            )));
        }
        let mut picked: Vec<u16> = Vec::with_capacity(count as usize);
        let mut cur = self.header.ffc;
        for _ in 0..count {
            if cur == 0 {
                return Err(FilesystemError::DiskFull(
                    "QDOS free list exhausted mid-allocation".into(),
                ));
            }
            picked.push(cur);
            cur = self.fat_lookup(cur);
        }
        // Link picked clusters together in order; tail terminates at 0.
        for w in picked.windows(2) {
            self.fat_set(w[0], w[1]);
        }
        let last = *picked.last().unwrap();
        self.fat_set(last, 0);
        // `cur` now points at the next free cluster after our picks (or 0).
        self.header.ffc = cur;
        self.header.fc -= count;
        Ok(picked[0])
    }

    /// Free a file chain back to the free list (push to head). Returns the
    /// number of clusters returned to the free list.
    fn free_chain_to_freelist(&mut self, first: u16) -> Result<u16, FilesystemError> {
        if first == 0 {
            return Ok(0);
        }
        let chain = self.chain(first)?;
        let count = chain.len() as u16;
        if count == 0 {
            return Ok(0);
        }
        let tail = *chain.last().unwrap();
        // Splice: tail.next = old ffc; new ffc = head of freed chain.
        self.fat_set(tail, self.header.ffc);
        self.header.ffc = first;
        self.header.fc = self.header.fc.saturating_add(count);
        Ok(count)
    }

    /// Write `data` into the clusters of `first`'s chain, zero-padding the
    /// remainder of the last cluster.
    fn write_chain(&mut self, first: u16, data: &[u8]) -> Result<(), FilesystemError> {
        let cs = self.header.cluster_size() as usize;
        let chain = self.chain(first)?;
        let mut written = 0usize;
        for c in chain {
            if written >= data.len() {
                break;
            }
            let want = cs.min(data.len() - written);
            let off = self.cluster_byte_offset(c);
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&data[written..written + want])?;
            if want < cs {
                let pad = vec![0u8; cs - want];
                self.reader.write_all(&pad)?;
            }
            written += want;
        }
        Ok(())
    }

    fn fat_write_back(&mut self) -> Result<(), FilesystemError> {
        let off = self.partition_offset + HEADER_BYTES as u64;
        self.reader.seek(SeekFrom::Start(off))?;
        self.reader.write_all(&self.fat_bytes)?;
        Ok(())
    }

    /// Re-write the three mutable header fields: fc (0x2C), ffc (0x32),
    /// rlen (0x36). Field offsets per sQLux QDisk.c.
    fn header_write_back(&mut self) -> Result<(), FilesystemError> {
        let base = self.partition_offset;
        self.reader.seek(SeekFrom::Start(base + 0x2C))?;
        self.reader.write_all(&self.header.fc.to_be_bytes())?;
        self.reader.seek(SeekFrom::Start(base + 0x32))?;
        self.reader.write_all(&self.header.ffc.to_be_bytes())?;
        self.reader.seek(SeekFrom::Start(base + 0x36))?;
        self.reader.write_all(&self.header.rlen.to_be_bytes())?;
        Ok(())
    }

    /// Extend the root directory by one cluster (allocate from free list,
    /// link onto the dir chain tail). Does NOT touch rlen — caller is
    /// responsible for that, since rlen tracks logical entry bytes rather
    /// than allocated cluster bytes.
    fn extend_dir_one_cluster(&mut self) -> Result<u16, FilesystemError> {
        let new_cluster = self.alloc_chain(1)?;
        let chain = self.chain(self.header.root_cluster)?;
        let old_tail = *chain
            .last()
            .ok_or_else(|| FilesystemError::InvalidData("QDOS root dir chain is empty".into()))?;
        self.fat_set(old_tail, new_cluster);
        Ok(new_cluster)
    }

    /// Locate the byte offset on disk of dir slot `slot_index`. Walks the
    /// root chain to find the physical cluster containing this slot.
    fn dir_slot_byte_offset(&mut self, slot_index: usize) -> Result<u64, FilesystemError> {
        let cluster_size = self.header.cluster_size() as usize;
        let byte_in_dir = slot_index * DIR_ENTRY_SIZE;
        let cluster_idx_in_chain = byte_in_dir / cluster_size;
        let off_in_cluster = byte_in_dir % cluster_size;
        let chain = self.chain(self.header.root_cluster)?;
        let cluster_no = *chain.get(cluster_idx_in_chain).ok_or_else(|| {
            FilesystemError::InvalidData(format!(
                "QDOS dir slot {} maps to chain index {} but chain only has {} clusters",
                slot_index,
                cluster_idx_in_chain,
                chain.len()
            ))
        })?;
        Ok(self.cluster_byte_offset(cluster_no) + off_in_cluster as u64)
    }

    /// Find or grow the directory to provide a slot for a new entry.
    /// Returns the slot index (>=1, since slot 0 is the volume self-
    /// reference). Strategy: first scan slots in `[1, rlen/64)` for an
    /// empty one (name_len == 0 && file_length == 0). If none, append a
    /// new slot at index `rlen/64`, growing the cluster chain when the
    /// new slot would cross a cluster boundary.
    fn allocate_dir_slot(&mut self) -> Result<usize, FilesystemError> {
        let rlen = self.header.rlen as usize;
        let max_slot = rlen / DIR_ENTRY_SIZE;
        let raw = self.read_chain_bytes(self.header.root_cluster, rlen)?;
        for i in 1..max_slot {
            let s = i * DIR_ENTRY_SIZE;
            let slot = &raw[s..s + DIR_ENTRY_SIZE];
            let name_len = BigEndian::read_u16(&slot[0x0E..0x10]);
            let file_len = BigEndian::read_u32(&slot[0x00..0x04]);
            if name_len == 0 && file_len == 0 {
                return Ok(i);
            }
        }
        // No reusable empty slot — append at max_slot.
        let new_slot = max_slot;
        let cluster_size = self.header.cluster_size() as usize;
        let needed_bytes = (new_slot + 1) * DIR_ENTRY_SIZE;
        let needed_clusters = needed_bytes.div_ceil(cluster_size);
        let have_clusters = self.chain(self.header.root_cluster)?.len();
        if needed_clusters > have_clusters {
            self.extend_dir_one_cluster()?;
        }
        let new_rlen = needed_bytes as u32;
        if new_rlen > self.header.rlen {
            self.header.rlen = new_rlen;
        }
        Ok(new_slot)
    }

    /// Locate the slot index of `name` in the root directory.
    fn find_dir_slot_by_name(&mut self, name: &str) -> Result<usize, FilesystemError> {
        let raw = self.read_chain_bytes(self.header.root_cluster, self.header.rlen as usize)?;
        let max_slot = raw.len() / DIR_ENTRY_SIZE;
        for i in 1..max_slot {
            let s = i * DIR_ENTRY_SIZE;
            let arr: &[u8; DIR_ENTRY_SIZE] = (&raw[s..s + DIR_ENTRY_SIZE]).try_into().unwrap();
            if let Some(de) = parse_dir_entry(arr) {
                if de.name == name {
                    return Ok(i);
                }
            }
        }
        Err(FilesystemError::NotFound(name.into()))
    }
}

/// QDOS filename: 1..36 bytes, no NULs, no slashes. QDOS does not enforce
/// a charset on disk but we reject control bytes to keep CLI behaviour
/// sane. ASCII printable is the safe lane.
fn validate_qdos_name(name: &str) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData(
            "QDOS name cannot be empty".into(),
        ));
    }
    if name.len() > 36 {
        return Err(FilesystemError::InvalidData(format!(
            "QDOS name '{}' exceeds 36-byte limit ({} bytes)",
            name,
            name.len()
        )));
    }
    for &b in name.as_bytes() {
        if b == 0 || b == b'/' || !(0x20..=0x7E).contains(&b) {
            return Err(FilesystemError::InvalidData(format!(
                "QDOS name '{name}' contains invalid byte 0x{b:02X}"
            )));
        }
    }
    Ok(())
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for QdosFilesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn std::io::Read,
        data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "QDOS subdir writes deferred — only root supported".into(),
            ));
        }
        validate_qdos_name(name)?;
        // Reject duplicates (QDOS names are case-sensitive on disk; we
        // mirror sQLux behaviour by comparing literally).
        let root = self.root()?;
        let existing = self.list_directory(&root)?;
        if existing.iter().any(|e| e.name == name) {
            return Err(FilesystemError::AlreadyExists(name.into()));
        }
        if data_len > u32::MAX as u64 {
            return Err(FilesystemError::InvalidData(
                "QDOS file exceeds u32::MAX bytes".into(),
            ));
        }
        let mut payload = Vec::with_capacity(data_len as usize);
        data.read_to_end(&mut payload)?;
        if payload.len() as u64 != data_len {
            return Err(FilesystemError::InvalidData(format!(
                "QDOS create_file: declared data_len {} != actual {}",
                data_len,
                payload.len()
            )));
        }
        let cluster_size = self.header.cluster_size() as usize;
        let cluster_count: u16 = if payload.is_empty() {
            0
        } else {
            let n = payload.len().div_ceil(cluster_size);
            if n > u16::MAX as usize {
                return Err(FilesystemError::InvalidData(
                    "QDOS file requires more than 65535 clusters".into(),
                ));
            }
            n as u16
        };
        let first_cluster = if cluster_count > 0 {
            self.alloc_chain(cluster_count)?
        } else {
            0
        };
        if first_cluster != 0 {
            self.write_chain(first_cluster, &payload)?;
        }

        // Stamp the directory entry.
        let slot_index = self.allocate_dir_slot()?;
        let slot_off = self.dir_slot_byte_offset(slot_index)?;
        let mut entry = [0u8; DIR_ENTRY_SIZE];
        BigEndian::write_u32(&mut entry[0x00..0x04], payload.len() as u32);
        // access_keys (0x04..0x06), file_type (0x06..0x08): leave 0 (data).
        BigEndian::write_u16(&mut entry[0x0E..0x10], name.len() as u16);
        entry[0x10..0x10 + name.len()].copy_from_slice(name.as_bytes());
        BigEndian::write_u16(&mut entry[0x3A..0x3C], first_cluster);
        self.reader.seek(SeekFrom::Start(slot_off))?;
        self.reader.write_all(&entry)?;

        // Flush FAT + header to disk so the volume is consistent even if the
        // caller forgets sync_metadata.
        self.fat_write_back()?;
        self.header_write_back()?;
        Ok(FileEntry::new_file(
            name.to_string(),
            format!("/{name}"),
            payload.len() as u64,
            first_cluster as u64,
        ))
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "QDOS subdir creation deferred — needs subdir-cluster semantics".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "QDOS subdir deletion deferred".into(),
            ));
        }
        let slot_index = self.find_dir_slot_by_name(&entry.name)?;
        let first_cluster = entry.location as u16;
        if first_cluster != 0 {
            self.free_chain_to_freelist(first_cluster)?;
        }
        // Mark the dir slot empty by zeroing it. Subsequent allocate_dir_slot
        // scans will reuse it.
        let slot_off = self.dir_slot_byte_offset(slot_index)?;
        let zeros = [0u8; DIR_ENTRY_SIZE];
        self.reader.seek(SeekFrom::Start(slot_off))?;
        self.reader.write_all(&zeros)?;
        self.fat_write_back()?;
        self.header_write_back()?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.header.fc as u64 * self.header.cluster_size())
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
    ///   - free list: ffc=2, linked 2→3→4→6→7→8→9→10→11→12→13→14→15→0
    ///     (skips reserved/allocated clusters 0,1,5), fc=12
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
        let mut name = [b' '; 20];
        name[..6].copy_from_slice(b"DemoQL");
        disk[0x06..0x1A].copy_from_slice(&name);
        BigEndian::write_u16(&mut disk[0x22..0x24], SPC);
        BigEndian::write_u16(&mut disk[0x2A..0x2C], CC);
        BigEndian::write_u16(&mut disk[0x2C..0x2E], 12); // fc (free count)
        BigEndian::write_u16(&mut disk[0x32..0x34], 2); // ffc (head of free list)
        BigEndian::write_u16(&mut disk[0x34..0x36], ROOT_CLUSTER);
        BigEndian::write_u32(&mut disk[0x36..0x3A], ROOT_LEN);

        // FAT at byte 0x40. Set up:
        //   - cluster 1 (root dir) → 0 (single-cluster chain ends here)
        //   - cluster 5 (file) → 0
        //   - free list: 2→3, 3→4, 4→6, 6→7, 7→8, ..., 14→15, 15→0
        let fat_off = HEADER_BYTES;
        let set = |d: &mut [u8], cl: u16, val: u16| {
            let o = fat_off + cl as usize * 2;
            BigEndian::write_u16(&mut d[o..o + 2], val);
        };
        set(&mut disk, ROOT_CLUSTER, 0);
        set(&mut disk, FILE_CLUSTER, 0);
        // Free list links — skipping reserved/allocated clusters.
        let free_order: &[u16] = &[2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        for w in free_order.windows(2) {
            set(&mut disk, w[0], w[1]);
        }
        set(&mut disk, *free_order.last().unwrap(), 0); // tail = end of free list

        // Root directory: cluster 1 = byte 512. Slot 0 = self-ref (zeros).
        let dir_off = ROOT_CLUSTER as usize * CLUSTER_SIZE;
        let slot1 = dir_off + DIR_ENTRY_SIZE;
        BigEndian::write_u32(&mut disk[slot1..slot1 + 4], 32);
        BigEndian::write_u16(&mut disk[slot1 + 0x0E..slot1 + 0x10], 8);
        disk[slot1 + 0x10..slot1 + 0x18].copy_from_slice(b"HELLO_QL");
        BigEndian::write_u16(&mut disk[slot1 + 0x3A..slot1 + 0x3C], FILE_CLUSTER);

        // File data at cluster 5 = byte 2560.
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

    #[test]
    fn parses_ffc_from_header() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let fs = QdosFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.header.ffc, 2, "ffc should be parsed from offset 0x32");
        assert_eq!(fs.header.fc, 12, "fc should be parsed from offset 0x2C");
    }

    #[test]
    fn validate_qdos_name_accepts_and_rejects() {
        assert!(validate_qdos_name("hello_ql").is_ok());
        assert!(validate_qdos_name("a").is_ok());
        let max = "a".repeat(36);
        assert!(validate_qdos_name(&max).is_ok());

        assert!(validate_qdos_name("").is_err());
        assert!(validate_qdos_name(&"a".repeat(37)).is_err());
        assert!(validate_qdos_name("has/slash").is_err());
        assert!(validate_qdos_name("has\0nul").is_err());
        assert!(validate_qdos_name("has\nctrl").is_err());
    }

    #[test]
    fn create_file_round_trips_through_directory_listing() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let mut fs = QdosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let pre = fs.list_directory(&root).unwrap();
        assert_eq!(pre.len(), 1);

        let payload = b"new content here";
        let new_entry = fs
            .create_file(
                &root,
                "NEWFILE",
                &mut payload.as_slice(),
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        assert_eq!(new_entry.name, "NEWFILE");
        assert_eq!(new_entry.size, payload.len() as u64);
        assert_ne!(new_entry.location, 0, "should have allocated a cluster");

        let post = fs.list_directory(&root).unwrap();
        assert_eq!(post.len(), 2);
        assert!(post.iter().any(|e| e.name == "NEWFILE"));
        assert!(post.iter().any(|e| e.name == "HELLO_QL"));

        // Round-trip the file contents.
        let new_in_listing = post.iter().find(|e| e.name == "NEWFILE").unwrap();
        let got = fs.read_file(new_in_listing, 1024).unwrap();
        assert_eq!(&got, payload);

        // Re-open from the underlying buffer to confirm on-disk sync.
        let raw = fs.reader.into_inner();
        let cur2 = Cursor::new(raw);
        let mut fs2 = QdosFilesystem::open(cur2, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let again = fs2.list_directory(&root2).unwrap();
        assert!(again.iter().any(|e| e.name == "NEWFILE"));
        // Counter bookkeeping persisted.
        assert_eq!(fs2.header.fc, 11, "fc should have decremented by 1");
        // ffc moved past cluster 2.
        assert_eq!(fs2.header.ffc, 3);
    }

    #[test]
    fn delete_entry_frees_chain_and_clears_slot() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let mut fs = QdosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let pre = fs.list_directory(&root).unwrap();
        assert_eq!(pre.len(), 1);
        let hello = pre[0].clone();
        let pre_fc = fs.header.fc;

        fs.delete_entry(&root, &hello).unwrap();
        let post = fs.list_directory(&root).unwrap();
        assert!(post.is_empty(), "HELLO_QL should be gone after delete");
        assert_eq!(
            fs.header.fc,
            pre_fc + 1,
            "fc should have grown by 1 (file was 1 cluster)"
        );
        assert_eq!(
            fs.header.ffc, 5,
            "freed cluster 5 should be at head of free list"
        );
    }

    #[test]
    fn create_after_delete_reuses_dir_slot() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let mut fs = QdosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let pre = fs.list_directory(&root).unwrap();
        let rlen_before = fs.header.rlen;

        fs.delete_entry(&root, &pre[0]).unwrap();
        fs.create_file(
            &root,
            "ANOTHER",
            &mut (&b"x"[..]),
            1,
            &CreateFileOptions::default(),
        )
        .unwrap();

        assert_eq!(
            fs.header.rlen, rlen_before,
            "dir should not have grown — slot 1 was reused"
        );
        let post = fs.list_directory(&root).unwrap();
        assert_eq!(post.len(), 1);
        assert_eq!(post[0].name, "ANOTHER");
    }

    #[test]
    fn create_file_rejects_duplicate_name() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let mut fs = QdosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let err = fs
            .create_file(
                &root,
                "HELLO_QL",
                &mut (&b"data"[..]),
                4,
                &CreateFileOptions::default(),
            )
            .unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }

    #[test]
    fn create_file_multi_cluster_chains_correctly() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let mut fs = QdosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        // 512-byte cluster × 3 = 1536-byte payload (will need 3 clusters).
        let payload: Vec<u8> = (0..1536u32).map(|i| (i & 0xFF) as u8).collect();
        let entry = fs
            .create_file(
                &root,
                "BIGFILE",
                &mut payload.as_slice(),
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        assert_eq!(entry.size, 1536);
        let got = fs.read_file(&entry, 4096).unwrap();
        assert_eq!(got, payload);
        // 3 clusters allocated → fc should drop from 12 to 9.
        assert_eq!(fs.header.fc, 9);
    }

    #[test]
    fn create_file_grows_directory_when_full() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let mut fs = QdosFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        // Dir initially has rlen=128 = 2 slots (slot 0 self-ref + slot 1
        // HELLO_QL). cluster_size=512 = 8 slots per cluster. Add 8 files
        // to push past the first cluster — that forces an extend.
        for i in 0..8 {
            let name = format!("F{i:03}");
            fs.create_file(
                &root,
                &name,
                &mut (&b"x"[..]),
                1,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }
        let post = fs.list_directory(&root).unwrap();
        assert_eq!(post.len(), 9, "HELLO_QL + 8 new files");
        // rlen must have grown past 512 (the original single cluster).
        assert!(
            fs.header.rlen > 512,
            "rlen should have grown past one cluster, got {}",
            fs.header.rlen
        );
        // Root dir chain should now span >= 2 clusters.
        assert!(
            fs.chain(fs.header.root_cluster).unwrap().len() >= 2,
            "root chain should have extended"
        );
    }

    #[test]
    fn alloc_chain_errors_when_volume_full() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let mut fs = QdosFilesystem::open(cur, 0).unwrap();
        // fc=12; ask for 13 — must fail without mutating header state.
        let err = fs.alloc_chain(13).unwrap_err();
        assert!(matches!(err, FilesystemError::DiskFull(_)));
        assert_eq!(fs.header.fc, 12, "fc should not have been mutated on error");
        assert_eq!(
            fs.header.ffc, 2,
            "ffc should not have been mutated on error"
        );
    }

    #[test]
    fn free_space_reports_clusters_times_cluster_size() {
        let disk = build_synthetic_qxlwin();
        let cur = Cursor::new(disk);
        let mut fs = QdosFilesystem::open(cur, 0).unwrap();
        let free = fs.free_space().unwrap();
        assert_eq!(free, 12 * 512);
    }
}
