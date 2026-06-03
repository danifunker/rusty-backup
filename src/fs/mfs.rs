//! Macintosh File System (MFS) — the pre-HFS filesystem shipped on 400 KB
//! single-sided 3.5" floppies for the original Mac, Mac 512K, and the
//! Macintosh Plus (until System 3.0 brought HFS).
//!
//! Used by the MiSTer **MacPlus** core when a `.dsk` is a 400 KB MFS
//! floppy rather than an HFS volume. The plan ([`docs/mister_filesystem_
//! implementation_plan.md`] Wave 1) targets an **extract-only floor**:
//! browse + read data fork (+ surface resource-fork size). Add/Delete and
//! resize are intentionally out of scope — the core itself rarely writes
//! MFS, and `BasiliskII` is the natural authoring path when one does.
//!
//! ## On-disk layout (Inside Macintosh: Files, Volume II, ch. 2)
//!
//! - **Sectors 0..1** (offset 0..1024) — boot blocks (zeroed on most non-
//!   bootable disks)
//! - **Sector 2** (offset 1024) — Master Directory Block (MDB):
//!
//! ```text
//! 0x00  u16   drSigWord     0xD2D7  (MFS magic)
//! 0x02  u32   drCrDate      create date (Mac epoch 1904-01-01)
//! 0x06  u32   drLsBkUp      last backup date
//! 0x0A  u16   drAtrb        volume attributes
//! 0x0C  u16   drNmFls       number of files
//! 0x0E  u16   drDirSt       first logical (512 B) block of the file directory
//! 0x10  u16   drBlLen       length of file directory in logical blocks
//! 0x12  u16   drNmAlBlks    number of allocation blocks
//! 0x14  u32   drAlBlkSiz    allocation-block size in bytes
//! 0x18  u32   drClpSiz      clump size (unused for extract)
//! 0x1C  u16   drAlBlSt      first allocation block in logical sectors
//! 0x1E  u32   drNxtFNum     next free file number
//! 0x22  u16   drFreeBks     free allocation blocks
//! 0x24  pstr  drVN          volume name (Pascal string, ≤27 chars)
//! ```
//!
//! - **Volume map** — starts immediately after the volume-name Pascal
//!   string and continues into subsequent sectors as needed. 12 bits per
//!   allocation block:
//!     - entries 0, 1 reserved
//!     - entry N ≥ 2 = pointer to next allocation block in the file's
//!       chain, or `0` (free) or `1` (last block of chain)
//!
//! - **File Directory** — `drBlLen` × 512 B sectors starting at logical
//!   sector `drDirSt`. Variable-length entries (rounded to even):
//!
//! ```text
//! 0x00  u8    flFlags       bit7 set = entry used; bit0 = locked
//! 0x01  u8    flTyp         version (=0)
//! 0x02  16    flUsrWds      Finder info (type/creator at +0/+4)
//! 0x12  u32   flFlNum       file number
//! 0x16  u16   flStBlk       first data-fork allocation block (0 = empty)
//! 0x18  u32   flLgLen       logical data-fork length
//! 0x1C  u32   flPyLen       physical data-fork length (rounded)
//! 0x20  u16   flRStBlk      first resource-fork allocation block
//! 0x22  u32   flRLgLen      logical resource-fork length
//! 0x26  u32   flRPyLen      physical resource-fork length
//! 0x2A  u32   flCrDat       create date
//! 0x2E  u32   flMdDat       modify date
//! 0x32  pstr  flNam         Pascal string filename (≤255 in spec; in
//!                           practice ≤63 because System 1.0 limited it)
//! ```
//!
//! Entry length = `0x32 + 1 + name_len`, padded up to the next even
//! offset. An entry where `flFlags & 0x80 == 0` ends the directory in
//! its sector (and the next sector starts a fresh entry).
//!
//! All multi-byte fields are **big-endian** (m68k native).

use std::io::{Read, Seek, SeekFrom};

use byteorder::{BigEndian, ByteOrder};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

/// MFS magic — big-endian u16 at byte 1024.
pub const MFS_SIGNATURE: u16 = 0xD2D7;

/// MDB offset within the volume.
const MDB_OFFSET: u64 = 1024;

/// 400 KB single-sided 3.5" floppy — the canonical MFS volume size.
pub const MFS_400K_BYTES: u64 = 409_600;

/// 800 KB double-sided 3.5" floppy — supported by MFS too, though rare.
pub const MFS_800K_BYTES: u64 = 819_200;

/// Maximum allocation blocks per the on-disk u16 field.
const MAX_ALLOC_BLOCKS: u32 = 0xFFFF;

/// Maximum allocation-block chain length we walk while reading one fork.
/// MFS volumes are tiny (≤ 800 KB) so this is generous; the cap is a
/// runaway guard, not a real limit.
const MAX_ALLOC_CHAIN: usize = 4096;

/// Parsed MDB fields the rest of the module needs. Stored on the FS so
/// every list/read call can use them without re-reading the boot sector.
#[derive(Debug, Clone)]
pub struct MfsMdb {
    pub create_date: u32,
    pub last_backup_date: u32,
    pub volume_attributes: u16,
    pub num_files: u16,
    pub dir_start_sector: u16,
    pub dir_length_sectors: u16,
    pub num_alloc_blocks: u16,
    pub alloc_block_size: u32,
    pub clump_size: u32,
    pub first_alloc_block_sector: u16,
    pub next_file_number: u32,
    pub free_blocks: u16,
    pub volume_name: String,
    /// Volume-map base byte offset within the partition (right after the
    /// MDB header + volume name Pascal string).
    map_offset: u64,
}

impl MfsMdb {
    /// Parse from a 512-byte buffer assumed to be the MDB sector.
    fn parse(buf: &[u8; 512]) -> Result<Self, FilesystemError> {
        let sig = BigEndian::read_u16(&buf[0..2]);
        if sig != MFS_SIGNATURE {
            return Err(FilesystemError::InvalidData(format!(
                "bad MFS signature: 0x{sig:04X} (expected 0x{MFS_SIGNATURE:04X})"
            )));
        }

        let create_date = BigEndian::read_u32(&buf[2..6]);
        let last_backup_date = BigEndian::read_u32(&buf[6..10]);
        let volume_attributes = BigEndian::read_u16(&buf[10..12]);
        let num_files = BigEndian::read_u16(&buf[12..14]);
        let dir_start_sector = BigEndian::read_u16(&buf[14..16]);
        let dir_length_sectors = BigEndian::read_u16(&buf[16..18]);
        let num_alloc_blocks = BigEndian::read_u16(&buf[18..20]);
        let alloc_block_size = BigEndian::read_u32(&buf[20..24]);
        let clump_size = BigEndian::read_u32(&buf[24..28]);
        let first_alloc_block_sector = BigEndian::read_u16(&buf[28..30]);
        let next_file_number = BigEndian::read_u32(&buf[30..34]);
        let free_blocks = BigEndian::read_u16(&buf[34..36]);

        // Volume name: Pascal string at offset 36 (1 length byte + chars).
        let name_len = buf[36] as usize;
        if 37 + name_len > 64 {
            // Spec caps Pascal-string name length at 27 in MFS; we tolerate
            // up to 27, defensively reject anything past 64 - 37 = 27 bytes.
            return Err(FilesystemError::InvalidData(format!(
                "MFS volume name length {name_len} exceeds 27"
            )));
        }
        let volume_name = super::hfs::mac_roman_to_utf8(&buf[37..37 + name_len]);

        // Volume map starts right after the volume name Pascal string,
        // still inside the MDB sector. The bit-packed map continues into
        // subsequent sectors when num_alloc_blocks * 12 bits overflows.
        let map_offset = MDB_OFFSET + 36 + 1 + name_len as u64;

        // Sanity: alloc_block_size must be a positive multiple of 512.
        if alloc_block_size == 0 || alloc_block_size % 512 != 0 {
            return Err(FilesystemError::InvalidData(format!(
                "MFS allocation block size {alloc_block_size} is not a positive multiple of 512"
            )));
        }
        if num_alloc_blocks as u32 > MAX_ALLOC_BLOCKS {
            return Err(FilesystemError::InvalidData(format!(
                "MFS allocation-block count {num_alloc_blocks} exceeds 65535"
            )));
        }
        if dir_length_sectors == 0 {
            return Err(FilesystemError::InvalidData(
                "MFS file directory length is 0".into(),
            ));
        }

        Ok(MfsMdb {
            create_date,
            last_backup_date,
            volume_attributes,
            num_files,
            dir_start_sector,
            dir_length_sectors,
            num_alloc_blocks,
            alloc_block_size,
            clump_size,
            first_alloc_block_sector,
            next_file_number,
            free_blocks,
            volume_name,
            map_offset,
        })
    }
}

/// One File Directory entry, decoded.
#[derive(Debug, Clone)]
#[allow(dead_code)] // dates + finder_info kept for round-trip / future write
struct MfsDirEntry {
    /// Bit 7 = entry is in use; bit 0 = file locked.
    flags: u8,
    /// 16-byte Finder info — type at +0, creator at +4.
    finder_info: [u8; 16],
    file_number: u32,
    data_first_block: u16,
    data_logical_length: u32,
    rsrc_first_block: u16,
    rsrc_logical_length: u32,
    create_date: u32,
    modify_date: u32,
    name: String,
}

impl MfsDirEntry {
    fn is_in_use(&self) -> bool {
        self.flags & 0x80 != 0
    }

    /// Mac OSType — 4 ASCII chars from Finder info bytes 0..4. Non-printable
    /// bytes are rendered as `.` so the display string stays sane.
    fn type_code_str(&self) -> String {
        ostype_to_string(&self.finder_info[0..4])
    }

    /// Mac creator code — 4 ASCII chars from Finder info bytes 4..8.
    fn creator_code_str(&self) -> String {
        ostype_to_string(&self.finder_info[4..8])
    }
}

fn ostype_to_string(b: &[u8]) -> String {
    debug_assert_eq!(b.len(), 4);
    b.iter()
        .map(|&c| {
            if (0x20..=0x7E).contains(&c) {
                c as char
            } else {
                '.'
            }
        })
        .collect()
}

/// Live MFS reader. Holds the parsed MDB, the volume bitmap, and a
/// re-decoded directory cache (small — at most a few hundred entries on
/// a 400 KB floppy).
pub struct MfsFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    pub(crate) mdb: MfsMdb,
    /// Cached directory entries, keyed by file number for O(1) lookup
    /// from `read_file`.
    entries: Vec<MfsDirEntry>,
    /// Allocation-block-map bytes (12-bit packed). Cached on open.
    map_bytes: Vec<u8>,
}

impl<R: Read + Seek + Send> MfsFilesystem<R> {
    /// Open an MFS volume at the given partition offset.
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        reader.seek(SeekFrom::Start(partition_offset + MDB_OFFSET))?;
        let mut mdb_buf = [0u8; 512];
        reader.read_exact(&mut mdb_buf)?;
        let mdb = MfsMdb::parse(&mdb_buf)?;

        // Read the allocation-block map. We need (num_alloc_blocks * 12 + 7) / 8
        // bytes starting at `mdb.map_offset`, possibly straddling sectors.
        let map_bits = mdb.num_alloc_blocks as usize * 12;
        let map_byte_len = map_bits.div_ceil(8);
        let mut map_bytes = vec![0u8; map_byte_len];
        reader.seek(SeekFrom::Start(partition_offset + mdb.map_offset))?;
        reader.read_exact(&mut map_bytes)?;

        // Read the file directory.
        let mut entries = Vec::new();
        for sector_idx in 0..mdb.dir_length_sectors {
            let sector_no = mdb.dir_start_sector as u64 + sector_idx as u64;
            let sector_off = partition_offset + sector_no * 512;
            reader.seek(SeekFrom::Start(sector_off))?;
            let mut sec = [0u8; 512];
            if reader.read_exact(&mut sec).is_err() {
                break;
            }
            parse_dir_sector(&sec, &mut entries);
        }

        Ok(MfsFilesystem {
            reader,
            partition_offset,
            mdb,
            entries,
            map_bytes,
        })
    }

    /// Look up entry index by file number.
    fn entry_by_file_number(&self, fnum: u32) -> Option<&MfsDirEntry> {
        self.entries
            .iter()
            .find(|e| e.is_in_use() && e.file_number == fnum)
    }

    /// Look up a 12-bit volume-map entry. Returns 0 if `block` is out of
    /// range (allocation blocks 0 and 1 are reserved and always read as 0).
    fn map_get(&self, block: u16) -> u16 {
        if block < 2 {
            return 0;
        }
        let bit_off = block as usize * 12;
        let byte_off = bit_off / 8;
        if byte_off + 1 >= self.map_bytes.len() {
            return 0;
        }
        // 12-bit big-endian-style packing. Two 12-bit entries fit in 3 bytes:
        //   even-indexed entry N: bits [byte N*3/2 high nibble, byte N*3/2+1 high nibble]
        //   actually MFS uses straight bit-packing — high-order first.
        if block.is_multiple_of(2) {
            // even block: high nibble of byte_off (which is block*12/8) is the LOW
            // 4 bits of the entry, byte_off+1 high 8 bits... Actually it's
            // simpler: 12 bits big-endian-packed: bits b*12 .. b*12+11 of the map.
            let hi = self.map_bytes[byte_off] as u16;
            let lo = (self.map_bytes[byte_off + 1] >> 4) as u16;
            (hi << 4) | lo
        } else {
            let hi = (self.map_bytes[byte_off] & 0x0F) as u16;
            let lo = self.map_bytes[byte_off + 1] as u16;
            (hi << 8) | lo
        }
    }

    /// Byte offset of allocation block N in the volume.
    fn alloc_block_offset(&self, block: u16) -> u64 {
        // Allocation block 2 maps to logical sector `first_alloc_block_sector`.
        // Block N maps to logical sector first_alloc_block_sector + (N - 2) * (alloc_block_size / 512).
        debug_assert!(block >= 2);
        let sectors_per_block = (self.mdb.alloc_block_size / 512) as u64;
        let sector =
            self.mdb.first_alloc_block_sector as u64 + (block as u64 - 2) * sectors_per_block;
        sector * 512
    }

    /// Walk an allocation-block chain starting at `first` and read
    /// `logical_len` bytes. Returns the chained bytes (truncated to
    /// `logical_len`).
    fn read_chain(&mut self, first: u16, logical_len: u32) -> Result<Vec<u8>, FilesystemError> {
        if first == 0 || logical_len == 0 {
            return Ok(Vec::new());
        }
        let mut data = Vec::with_capacity(logical_len as usize);
        let bs = self.mdb.alloc_block_size as usize;
        let mut block = first;
        for _hop in 0..MAX_ALLOC_CHAIN {
            if block < 2 || block as u32 - 2 >= self.mdb.num_alloc_blocks as u32 {
                return Err(FilesystemError::InvalidData(format!(
                    "MFS chain references out-of-range alloc block {block}"
                )));
            }
            let off = self.partition_offset + self.alloc_block_offset(block);
            self.reader.seek(SeekFrom::Start(off))?;
            let want = (logical_len as usize).saturating_sub(data.len()).min(bs);
            let mut chunk = vec![0u8; bs];
            self.reader.read_exact(&mut chunk)?;
            data.extend_from_slice(&chunk[..want]);
            if data.len() >= logical_len as usize {
                break;
            }
            let next = self.map_get(block);
            if next == 1 {
                // End-of-chain. Data may be shorter than logical_len if the
                // catalog is inconsistent; truncate to what we have.
                break;
            }
            if next == 0 {
                return Err(FilesystemError::InvalidData(format!(
                    "MFS chain hits a free block at {block}"
                )));
            }
            block = next;
        }
        Ok(data)
    }

    /// Read the resource fork of a directory entry. Returns an empty `Vec`
    /// if the file has no resource fork. Surfaced as a side channel
    /// because the trait's `read_file` only exposes the data fork; classic
    /// Mac apps often live in the resource fork and consumers (e.g. the
    /// `mac_archive` exporter) need access.
    pub fn read_resource_fork(&mut self, entry: &FileEntry) -> Result<Vec<u8>, FilesystemError> {
        let fnum = entry.location as u32;
        let de = self
            .entry_by_file_number(fnum)
            .ok_or_else(|| FilesystemError::NotFound(format!("file number {fnum} not found")))?
            .clone();
        self.read_chain(de.rsrc_first_block, de.rsrc_logical_length)
    }
}

/// Parse every used entry out of one 512-byte directory sector. Stops at
/// the first unused entry (per spec, an unused entry terminates the
/// current sector's entry list).
fn parse_dir_sector(sec: &[u8; 512], out: &mut Vec<MfsDirEntry>) {
    let mut off = 0usize;
    while off + 50 < sec.len() {
        let flags = sec[off];
        if flags & 0x80 == 0 {
            // Unused entry — terminates the sector per spec.
            break;
        }
        let mut finder_info = [0u8; 16];
        finder_info.copy_from_slice(&sec[off + 2..off + 18]);
        let file_number = BigEndian::read_u32(&sec[off + 18..off + 22]);
        let data_first_block = BigEndian::read_u16(&sec[off + 22..off + 24]);
        let data_logical_length = BigEndian::read_u32(&sec[off + 24..off + 28]);
        let _data_phys_length = BigEndian::read_u32(&sec[off + 28..off + 32]);
        let rsrc_first_block = BigEndian::read_u16(&sec[off + 32..off + 34]);
        let rsrc_logical_length = BigEndian::read_u32(&sec[off + 34..off + 38]);
        let _rsrc_phys_length = BigEndian::read_u32(&sec[off + 38..off + 42]);
        let create_date = BigEndian::read_u32(&sec[off + 42..off + 46]);
        let modify_date = BigEndian::read_u32(&sec[off + 46..off + 50]);
        let name_len = sec[off + 50] as usize;
        if off + 51 + name_len > sec.len() {
            // Truncated entry — stop here.
            break;
        }
        let name = super::hfs::mac_roman_to_utf8(&sec[off + 51..off + 51 + name_len]);

        out.push(MfsDirEntry {
            flags,
            finder_info,
            file_number,
            data_first_block,
            data_logical_length,
            rsrc_first_block,
            rsrc_logical_length,
            create_date,
            modify_date,
            name,
        });

        // Advance past header + name (Pascal byte + chars) padded to even.
        let entry_len = 51 + name_len;
        let padded = entry_len + (entry_len & 1);
        off += padded;
    }
}

impl<R: Read + Seek + Send> Filesystem for MfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            // MFS is a flat filesystem — only the root has children.
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(self.entries.len());
        for de in &self.entries {
            if !de.is_in_use() {
                continue;
            }
            let path = format!("/{}", de.name);
            let mut fe = FileEntry::new_file(
                de.name.clone(),
                path,
                de.data_logical_length as u64,
                de.file_number as u64,
            );
            fe.type_code = Some(de.type_code_str());
            fe.creator_code = Some(de.creator_code_str());
            if de.rsrc_logical_length > 0 {
                fe.resource_fork_size = Some(de.rsrc_logical_length as u64);
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
        let fnum = entry.location as u32;
        let de = self
            .entry_by_file_number(fnum)
            .ok_or_else(|| FilesystemError::NotFound(format!("file number {fnum} not found")))?
            .clone();
        let mut data = self.read_chain(de.data_first_block, de.data_logical_length)?;
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        "MFS"
    }

    fn volume_label(&self) -> Option<&str> {
        if self.mdb.volume_name.is_empty() {
            None
        } else {
            Some(&self.mdb.volume_name)
        }
    }

    fn total_size(&self) -> u64 {
        self.mdb.num_alloc_blocks as u64 * self.mdb.alloc_block_size as u64
    }

    fn used_size(&self) -> u64 {
        (self.mdb.num_alloc_blocks - self.mdb.free_blocks) as u64 * self.mdb.alloc_block_size as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a minimal valid MFS volume in memory. Two files:
    ///   - "Hello"   data fork = b"hello mfs"      (no resource fork)
    ///   - "Doc"     data fork = b"first line\n"   resource fork = b"RSRC"
    ///
    /// Layout (all 512 B sectors):
    ///   sector 0..1     boot blocks (zero)
    ///   sector 2        MDB + volume name + map
    ///   sector 3..4     file directory (2 sectors)
    ///   sector 5+       allocation blocks (1 KiB each — 2 sectors per block)
    ///
    /// Allocation block size = 1024 B (2 sectors). 8 alloc blocks total →
    /// 8 KiB volume of file data. Plenty for the test files.
    fn build_test_mfs() -> Vec<u8> {
        const SECTOR: usize = 512;
        const NUM_SECTORS: usize = 32; // 16 KiB volume
        const ALLOC_BLOCK_SIZE: u32 = 1024;
        const FIRST_ALLOC_BLOCK_SECTOR: u16 = 5;
        const DIR_START: u16 = 3;
        const DIR_LEN: u16 = 2;
        const NUM_ALLOC_BLOCKS: u16 = 8;

        let mut disk = vec![0u8; NUM_SECTORS * SECTOR];

        // --- MDB ---
        let mdb = &mut disk[1024..1024 + 512];
        BigEndian::write_u16(&mut mdb[0..2], MFS_SIGNATURE);
        BigEndian::write_u32(&mut mdb[2..6], 0); // create date
        BigEndian::write_u32(&mut mdb[6..10], 0); // last backup
        BigEndian::write_u16(&mut mdb[10..12], 0); // attributes
        BigEndian::write_u16(&mut mdb[12..14], 2); // num files
        BigEndian::write_u16(&mut mdb[14..16], DIR_START);
        BigEndian::write_u16(&mut mdb[16..18], DIR_LEN);
        BigEndian::write_u16(&mut mdb[18..20], NUM_ALLOC_BLOCKS);
        BigEndian::write_u32(&mut mdb[20..24], ALLOC_BLOCK_SIZE);
        BigEndian::write_u32(&mut mdb[24..28], ALLOC_BLOCK_SIZE);
        BigEndian::write_u16(&mut mdb[28..30], FIRST_ALLOC_BLOCK_SECTOR);
        BigEndian::write_u32(&mut mdb[30..34], 3); // next file number
        BigEndian::write_u16(&mut mdb[34..36], NUM_ALLOC_BLOCKS - 3); // free blocks
        let vname = b"Tester";
        mdb[36] = vname.len() as u8;
        mdb[37..37 + vname.len()].copy_from_slice(vname);

        // --- Volume map ---
        // Map starts at byte 1024 + 36 + 1 + 6 = 1067. 8 alloc blocks × 12 bits = 96 bits = 12 bytes.
        // Blocks 0 & 1 reserved. Layout:
        //   Block 2 → 1 (end of chain for "Hello")
        //   Block 3 → 1 (end of chain for "Doc" data fork)
        //   Block 4 → 1 (end of chain for "Doc" rsrc fork)
        //   Blocks 5..9 → 0 (free)
        let map_start = 1024 + 36 + 1 + vname.len();
        let mut map = vec![0u8; 12];
        write_map_entry(&mut map, 2, 1);
        write_map_entry(&mut map, 3, 1);
        write_map_entry(&mut map, 4, 1);
        disk[map_start..map_start + map.len()].copy_from_slice(&map);

        // --- File directory ---
        // Build two entries in sector 3 (first dir sector).
        let mut dir_sec = vec![0u8; SECTOR];
        let off = encode_dir_entry(
            &mut dir_sec,
            0,
            DirEntryIn {
                flags: 0x80,
                file_number: 1,
                data_first_block: 2,
                data_logical_length: 9, // "hello mfs"
                data_physical_length: ALLOC_BLOCK_SIZE,
                rsrc_first_block: 0,
                rsrc_logical_length: 0,
                rsrc_physical_length: 0,
                type_code: *b"TEXT",
                creator_code: *b"ttxt",
                name: b"Hello",
            },
        );
        let off = encode_dir_entry(
            &mut dir_sec,
            off,
            DirEntryIn {
                flags: 0x80,
                file_number: 2,
                data_first_block: 3,
                data_logical_length: 11, // "first line\n"
                data_physical_length: ALLOC_BLOCK_SIZE,
                rsrc_first_block: 4,
                rsrc_logical_length: 4, // "RSRC"
                rsrc_physical_length: ALLOC_BLOCK_SIZE,
                type_code: *b"TEXT",
                creator_code: *b"ttxt",
                name: b"Doc",
            },
        );
        let _ = off;
        disk[3 * SECTOR..3 * SECTOR + SECTOR].copy_from_slice(&dir_sec);

        // --- File data ---
        let alloc_start = FIRST_ALLOC_BLOCK_SECTOR as usize * SECTOR;
        // Block 2 — "Hello"'s data
        disk[alloc_start..alloc_start + 9].copy_from_slice(b"hello mfs");
        // Block 3 — "Doc"'s data (1 alloc block later)
        let b3 = alloc_start + ALLOC_BLOCK_SIZE as usize;
        disk[b3..b3 + 11].copy_from_slice(b"first line\n");
        // Block 4 — "Doc"'s rsrc
        let b4 = alloc_start + 2 * ALLOC_BLOCK_SIZE as usize;
        disk[b4..b4 + 4].copy_from_slice(b"RSRC");

        disk
    }

    struct DirEntryIn<'a> {
        flags: u8,
        file_number: u32,
        data_first_block: u16,
        data_logical_length: u32,
        data_physical_length: u32,
        rsrc_first_block: u16,
        rsrc_logical_length: u32,
        rsrc_physical_length: u32,
        type_code: [u8; 4],
        creator_code: [u8; 4],
        name: &'a [u8],
    }

    fn encode_dir_entry(buf: &mut [u8], off: usize, e: DirEntryIn) -> usize {
        buf[off] = e.flags;
        buf[off + 1] = 0; // version
        buf[off + 2..off + 6].copy_from_slice(&e.type_code);
        buf[off + 6..off + 10].copy_from_slice(&e.creator_code);
        // finder_info bytes 8..16 zeroed.
        BigEndian::write_u32(&mut buf[off + 18..off + 22], e.file_number);
        BigEndian::write_u16(&mut buf[off + 22..off + 24], e.data_first_block);
        BigEndian::write_u32(&mut buf[off + 24..off + 28], e.data_logical_length);
        BigEndian::write_u32(&mut buf[off + 28..off + 32], e.data_physical_length);
        BigEndian::write_u16(&mut buf[off + 32..off + 34], e.rsrc_first_block);
        BigEndian::write_u32(&mut buf[off + 34..off + 38], e.rsrc_logical_length);
        BigEndian::write_u32(&mut buf[off + 38..off + 42], e.rsrc_physical_length);
        // dates at +42 / +46 — leave zero.
        buf[off + 50] = e.name.len() as u8;
        buf[off + 51..off + 51 + e.name.len()].copy_from_slice(e.name);
        let entry_len = 51 + e.name.len();
        off + entry_len + (entry_len & 1)
    }

    /// Write a 12-bit volume-map entry into the packed map buffer.
    /// Mirrors the high-nibble-first packing used by `map_get`.
    fn write_map_entry(map: &mut [u8], block: u16, value: u16) {
        let value = value & 0x0FFF;
        let bit_off = block as usize * 12;
        let byte_off = bit_off / 8;
        if block.is_multiple_of(2) {
            map[byte_off] = (value >> 4) as u8;
            map[byte_off + 1] = (map[byte_off + 1] & 0x0F) | (((value & 0x0F) as u8) << 4);
        } else {
            map[byte_off] = (map[byte_off] & 0xF0) | ((value >> 8) as u8 & 0x0F);
            map[byte_off + 1] = (value & 0xFF) as u8;
        }
    }

    #[test]
    fn parses_mdb_from_synthetic_volume() {
        let disk = build_test_mfs();
        let cur = Cursor::new(disk);
        let fs = MfsFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.mdb.num_files, 2);
        assert_eq!(fs.mdb.alloc_block_size, 1024);
        assert_eq!(fs.mdb.num_alloc_blocks, 8);
        assert_eq!(fs.mdb.first_alloc_block_sector, 5);
        assert_eq!(fs.volume_label(), Some("Tester"));
        assert_eq!(fs.fs_type(), "MFS");
    }

    #[test]
    fn lists_root_directory() {
        let disk = build_test_mfs();
        let cur = Cursor::new(disk);
        let mut fs = MfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        assert_eq!(entries.len(), 2);
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"Doc"));
        assert!(names.contains(&"Hello"));
        let doc = entries.iter().find(|e| e.name == "Doc").unwrap();
        assert_eq!(doc.resource_fork_size, Some(4));
        let hello = entries.iter().find(|e| e.name == "Hello").unwrap();
        assert_eq!(hello.resource_fork_size, None);
        assert_eq!(hello.type_code.as_deref(), Some("TEXT"));
        assert_eq!(hello.creator_code.as_deref(), Some("ttxt"));
    }

    #[test]
    fn reads_data_fork() {
        let disk = build_test_mfs();
        let cur = Cursor::new(disk);
        let mut fs = MfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let hello = entries.iter().find(|e| e.name == "Hello").unwrap();
        let data = fs.read_file(hello, 1024).unwrap();
        assert_eq!(&data, b"hello mfs");
        let doc = entries.iter().find(|e| e.name == "Doc").unwrap();
        let data = fs.read_file(doc, 1024).unwrap();
        assert_eq!(&data, b"first line\n");
    }

    #[test]
    fn reads_resource_fork() {
        let disk = build_test_mfs();
        let cur = Cursor::new(disk);
        let mut fs = MfsFilesystem::open(cur, 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let doc = entries.iter().find(|e| e.name == "Doc").unwrap();
        let rsrc = fs.read_resource_fork(doc).unwrap();
        assert_eq!(&rsrc, b"RSRC");
        let hello = entries.iter().find(|e| e.name == "Hello").unwrap();
        let empty = fs.read_resource_fork(hello).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn rejects_non_mfs_signature() {
        let mut disk = build_test_mfs();
        disk[1024] = 0xFF;
        disk[1025] = 0xFF;
        let cur = Cursor::new(disk);
        match MfsFilesystem::open(cur, 0) {
            Ok(_) => panic!("expected open to fail on bad signature"),
            Err(FilesystemError::InvalidData(msg)) => {
                assert!(msg.contains("bad MFS signature"), "got: {msg}");
            }
            Err(other) => panic!("expected InvalidData, got {other:?}"),
        }
    }

    #[test]
    fn map_get_round_trip() {
        // Confirm our 12-bit packing round-trips through `MfsFilesystem::map_get`.
        let disk = build_test_mfs();
        let cur = Cursor::new(disk);
        let fs = MfsFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.map_get(2), 1, "block 2 marked end-of-chain");
        assert_eq!(fs.map_get(3), 1);
        assert_eq!(fs.map_get(4), 1);
        assert_eq!(fs.map_get(5), 0, "block 5 marked free");
        assert_eq!(fs.map_get(0), 0, "reserved block 0");
        assert_eq!(fs.map_get(1), 0, "reserved block 1");
    }

    #[test]
    fn total_and_used_size_reflect_mdb_counts() {
        let disk = build_test_mfs();
        let cur = Cursor::new(disk);
        let fs = MfsFilesystem::open(cur, 0).unwrap();
        assert_eq!(fs.total_size(), 8 * 1024);
        // 3 allocation blocks used (Hello data, Doc data, Doc rsrc).
        assert_eq!(fs.used_size(), 3 * 1024);
    }
}
