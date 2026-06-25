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

use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

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

    /// Finder flags (`FInfo.fdFlags`) — the big-endian 2-byte field at offset 8
    /// of the 16-byte Finder info. Surfaced on `FileEntry` so copy /
    /// `get-binhex` round-trips preserve them.
    fn finder_flags(&self) -> u16 {
        u16::from_be_bytes([self.finder_info[8], self.finder_info[9]])
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

/// Maximum length of an MFS filename in bytes (Mac Roman). The on-disk
/// field is a 28-byte Pascal string, so 1 length byte + up to 27 chars.
pub const MFS_MAX_NAME_LEN: usize = 27;

/// Validate a candidate filename for `create_file` / `set_type_creator`.
/// MFS uses Mac Roman encoding and disallows the path-separator ':' (a
/// Classic Mac OS convention also enforced by HFS).
fn validate_mfs_name(name: &str) -> Result<Vec<u8>, FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData("MFS filename is empty".into()));
    }
    if name.contains(':') {
        return Err(FilesystemError::InvalidData(
            "MFS filename contains ':' (Mac path separator) — use '-' or '_'".into(),
        ));
    }
    let bytes = super::hfs::utf8_to_mac_roman(name).map_err(|_| {
        FilesystemError::InvalidData(
            "MFS filename contains characters not representable in Mac Roman".into(),
        )
    })?;
    if bytes.len() > MFS_MAX_NAME_LEN {
        return Err(FilesystemError::InvalidData(format!(
            "MFS filename is {} bytes; max is {MFS_MAX_NAME_LEN}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

impl MfsDirEntry {
    /// Serialize a single directory entry into a fresh `Vec<u8>` padded to
    /// even length (the on-disk constraint — entries align to even byte
    /// offsets within a directory sector). Caller stamps the result into
    /// the sector buffer.
    fn encode(&self) -> Vec<u8> {
        let name_bytes = super::hfs::utf8_to_mac_roman(&self.name).unwrap_or_default();
        let name_len = name_bytes.len().min(MFS_MAX_NAME_LEN) as u8;
        let total = 51 + name_len as usize;
        let padded = total + (total & 1);
        let mut buf = vec![0u8; padded];
        buf[0] = self.flags;
        // byte 1 = version (=0); already zeroed
        buf[2..18].copy_from_slice(&self.finder_info);
        BigEndian::write_u32(&mut buf[18..22], self.file_number);
        BigEndian::write_u16(&mut buf[22..24], self.data_first_block);
        BigEndian::write_u32(&mut buf[24..28], self.data_logical_length);
        // physical length: round logical up to allocation-block boundary.
        // Caller will overwrite if a more accurate value is known.
        BigEndian::write_u32(&mut buf[28..32], self.data_logical_length);
        BigEndian::write_u16(&mut buf[32..34], self.rsrc_first_block);
        BigEndian::write_u32(&mut buf[34..38], self.rsrc_logical_length);
        BigEndian::write_u32(&mut buf[38..42], self.rsrc_logical_length);
        BigEndian::write_u32(&mut buf[42..46], self.create_date);
        BigEndian::write_u32(&mut buf[46..50], self.modify_date);
        buf[50] = name_len;
        buf[51..51 + name_len as usize].copy_from_slice(&name_bytes[..name_len as usize]);
        buf
    }
}

impl<R: Read + Write + Seek + Send> MfsFilesystem<R> {
    // --- low-level block-map editing ---------------------------------------

    /// Set a 12-bit volume-map entry to `value`. Mirrors `map_get`'s
    /// high-nibble-first packing. Panics on out-of-range `block`.
    fn map_set(&mut self, block: u16, value: u16) {
        debug_assert!(
            block as u32 >= 2 && (block as u32) < self.mdb.num_alloc_blocks as u32 + 2,
            "map_set out-of-range block {block}"
        );
        let value = value & 0x0FFF;
        let bit_off = block as usize * 12;
        let byte_off = bit_off / 8;
        if block.is_multiple_of(2) {
            self.map_bytes[byte_off] = (value >> 4) as u8;
            self.map_bytes[byte_off + 1] =
                (self.map_bytes[byte_off + 1] & 0x0F) | (((value & 0x0F) as u8) << 4);
        } else {
            self.map_bytes[byte_off] =
                (self.map_bytes[byte_off] & 0xF0) | ((value >> 8) as u8 & 0x0F);
            self.map_bytes[byte_off + 1] = (value & 0xFF) as u8;
        }
    }

    /// Find `count` free allocation blocks, link them into a chain
    /// (block_i -> block_{i+1}, last block -> 1 = end-of-chain), and
    /// return the first block number. Bumps `mdb.free_blocks` down.
    fn alloc_chain(&mut self, count: u32) -> Result<u16, FilesystemError> {
        if count == 0 {
            return Ok(0);
        }
        if (count as u16) > self.mdb.free_blocks {
            return Err(FilesystemError::InvalidData(format!(
                "out of space: need {count} alloc blocks, have {} free",
                self.mdb.free_blocks
            )));
        }
        // First pass: gather the indices of `count` free blocks.
        let mut picked = Vec::with_capacity(count as usize);
        // Allocation blocks are numbered starting at 2 (blocks 0 and 1
        // reserved by the spec). num_alloc_blocks counts the
        // user-addressable blocks, so valid range is 2 ..= num_alloc_blocks + 1.
        for b in 2..(self.mdb.num_alloc_blocks as u32 + 2) {
            if self.map_get(b as u16) == 0 {
                picked.push(b as u16);
                if picked.len() == count as usize {
                    break;
                }
            }
        }
        if picked.len() < count as usize {
            return Err(FilesystemError::InvalidData(format!(
                "fragmentation: {} free per MDB but only {} blocks found in map",
                self.mdb.free_blocks,
                picked.len()
            )));
        }
        // Second pass: link them.
        for w in picked.windows(2) {
            let cur = w[0];
            let nxt = w[1];
            self.map_set(cur, nxt);
        }
        // Last block: end-of-chain marker (1).
        let last = *picked.last().expect("picked non-empty");
        self.map_set(last, 1);
        self.mdb.free_blocks -= count as u16;
        Ok(picked[0])
    }

    /// Walk an allocation-block chain starting at `first` and mark every
    /// block free (map entry = 0). Bumps `mdb.free_blocks` up by the
    /// number of blocks released.
    fn free_chain(&mut self, first: u16) -> Result<(), FilesystemError> {
        if first == 0 {
            return Ok(());
        }
        let mut block = first;
        let mut visited = std::collections::HashSet::new();
        for _hop in 0..MAX_ALLOC_CHAIN {
            if block < 2 || block as u32 - 2 >= self.mdb.num_alloc_blocks as u32 {
                return Err(FilesystemError::InvalidData(format!(
                    "free_chain hit out-of-range block {block}"
                )));
            }
            if !visited.insert(block) {
                return Err(FilesystemError::InvalidData(format!(
                    "free_chain cycle at block {block}"
                )));
            }
            let next = self.map_get(block);
            self.map_set(block, 0);
            self.mdb.free_blocks += 1;
            if next == 1 || next == 0 {
                return Ok(());
            }
            block = next;
        }
        Err(FilesystemError::InvalidData(
            "free_chain ran past MAX_ALLOC_CHAIN".into(),
        ))
    }

    /// Write `data` across the allocation-block chain starting at `first`.
    /// The chain must have been pre-allocated by `alloc_chain`. Each block
    /// holds `mdb.alloc_block_size` bytes; the last block is zero-padded
    /// to its full size.
    fn write_data_chain(&mut self, first: u16, data: &[u8]) -> Result<(), FilesystemError> {
        if first == 0 || data.is_empty() {
            return Ok(());
        }
        let bs = self.mdb.alloc_block_size as usize;
        let mut written = 0usize;
        let mut block = first;
        for _hop in 0..MAX_ALLOC_CHAIN {
            let chunk_off = written;
            let chunk_end = (written + bs).min(data.len());
            let abs = self.partition_offset + self.alloc_block_offset(block);
            self.reader.seek(SeekFrom::Start(abs))?;
            let chunk = &data[chunk_off..chunk_end];
            self.reader.write_all(chunk)?;
            // Zero-pad the rest of the block.
            if chunk.len() < bs {
                let pad = vec![0u8; bs - chunk.len()];
                self.reader.write_all(&pad)?;
            }
            written = chunk_end;
            if written >= data.len() {
                return Ok(());
            }
            let next = self.map_get(block);
            if next == 1 || next == 0 {
                if written < data.len() {
                    return Err(FilesystemError::InvalidData(format!(
                        "write_data_chain short-chained: {} bytes unwritten",
                        data.len() - written
                    )));
                }
                return Ok(());
            }
            block = next;
        }
        Err(FilesystemError::InvalidData(
            "write_data_chain ran past MAX_ALLOC_CHAIN".into(),
        ))
    }

    // --- on-disk write-back -----------------------------------------------

    /// Re-encode the in-memory `entries` Vec into the directory sectors
    /// and write them out. Used by `sync_metadata`.
    fn dir_write_back(&mut self) -> Result<(), FilesystemError> {
        let sector_count = self.mdb.dir_length_sectors as usize;
        let mut packed = vec![vec![0u8; 512]; sector_count];
        let mut sector_idx = 0usize;
        let mut byte_off = 0usize;
        for e in self.entries.iter().filter(|e| e.is_in_use()) {
            let bytes = e.encode();
            // If this entry doesn't fit in the remainder of the current
            // sector (after leaving 1 byte for the terminator), advance.
            if byte_off + bytes.len() + 1 > 512 {
                sector_idx += 1;
                byte_off = 0;
                if sector_idx >= sector_count {
                    return Err(FilesystemError::InvalidData(format!(
                        "directory full: {} sectors of {sector_count} can't hold {} entries",
                        sector_count,
                        self.entries.iter().filter(|e| e.is_in_use()).count()
                    )));
                }
            }
            packed[sector_idx][byte_off..byte_off + bytes.len()].copy_from_slice(&bytes);
            byte_off += bytes.len();
        }
        for (i, sec) in packed.into_iter().enumerate() {
            let sector_no = self.mdb.dir_start_sector as u64 + i as u64;
            let off = self.partition_offset + sector_no * 512;
            self.reader.seek(SeekFrom::Start(off))?;
            self.reader.write_all(&sec)?;
        }
        Ok(())
    }

    /// Serialize MDB fields back into a 512-byte buffer and write.
    fn mdb_write_back(&mut self) -> Result<(), FilesystemError> {
        // Re-read the original MDB sector to preserve any spare bytes we
        // don't model (e.g. the post-volume-name volume-map area, which
        // we write separately via `map_write_back`).
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + MDB_OFFSET))?;
        let mut buf = [0u8; 512];
        self.reader.read_exact(&mut buf)?;
        // Re-stamp the fields we own.
        BigEndian::write_u16(&mut buf[0..2], MFS_SIGNATURE);
        BigEndian::write_u32(&mut buf[2..6], self.mdb.create_date);
        BigEndian::write_u32(&mut buf[6..10], self.mdb.last_backup_date);
        BigEndian::write_u16(&mut buf[10..12], self.mdb.volume_attributes);
        BigEndian::write_u16(&mut buf[12..14], self.mdb.num_files);
        BigEndian::write_u16(&mut buf[14..16], self.mdb.dir_start_sector);
        BigEndian::write_u16(&mut buf[16..18], self.mdb.dir_length_sectors);
        BigEndian::write_u16(&mut buf[18..20], self.mdb.num_alloc_blocks);
        BigEndian::write_u32(&mut buf[20..24], self.mdb.alloc_block_size);
        BigEndian::write_u32(&mut buf[24..28], self.mdb.clump_size);
        BigEndian::write_u16(&mut buf[28..30], self.mdb.first_alloc_block_sector);
        BigEndian::write_u32(&mut buf[30..34], self.mdb.next_file_number);
        BigEndian::write_u16(&mut buf[34..36], self.mdb.free_blocks);
        let name_bytes = super::hfs::utf8_to_mac_roman(&self.mdb.volume_name).unwrap_or_default();
        let name_len = name_bytes.len().min(MFS_MAX_NAME_LEN);
        buf[36] = name_len as u8;
        // Zero the old name area before stamping the new one.
        for b in buf[37..37 + MFS_MAX_NAME_LEN].iter_mut() {
            *b = 0;
        }
        buf[37..37 + name_len].copy_from_slice(&name_bytes[..name_len]);
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + MDB_OFFSET))?;
        self.reader.write_all(&buf)?;
        Ok(())
    }

    /// Write the in-memory `map_bytes` back to its on-disk position
    /// (right after the volume name Pascal string within the MDB sector,
    /// possibly straddling subsequent sectors).
    fn map_write_back(&mut self) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + self.mdb.map_offset))?;
        let bytes = self.map_bytes.clone();
        self.reader.write_all(&bytes)?;
        Ok(())
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
            fe.finder_flags = Some(de.finder_flags());
            if de.rsrc_logical_length > 0 {
                fe.resource_fork_size = Some(de.rsrc_logical_length as u64);
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

impl<R: Read + Write + Seek + Send> EditableFilesystem for MfsFilesystem<R> {
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
                "MFS is flat; only the root directory accepts new files".into(),
            ));
        }
        let _ = validate_mfs_name(name)?;
        // Reject duplicates.
        if self.entries.iter().any(|e| e.is_in_use() && e.name == name) {
            return Err(FilesystemError::InvalidData(format!(
                "MFS file '{name}' already exists"
            )));
        }
        if data_len > u32::MAX as u64 {
            return Err(FilesystemError::InvalidData(
                "MFS file size capped at u32::MAX bytes".into(),
            ));
        }
        // Read all data into memory. MFS volumes top out at 800 KB so
        // this is fine.
        let mut bytes = Vec::with_capacity(data_len as usize);
        data.read_to_end(&mut bytes)?;
        if bytes.len() as u64 != data_len {
            return Err(FilesystemError::InvalidData(format!(
                "data_len {data_len} != actual {} bytes",
                bytes.len()
            )));
        }
        let bs = self.mdb.alloc_block_size;
        let block_count = if bytes.is_empty() {
            0u32
        } else {
            bytes.len().div_ceil(bs as usize) as u32
        };
        let first_block = self.alloc_chain(block_count)?;
        // Commit data + map.
        if first_block != 0 {
            self.write_data_chain(first_block, &bytes)?;
        }

        let file_number = self.mdb.next_file_number;
        self.mdb.next_file_number += 1;
        self.mdb.num_files += 1;

        let entry = MfsDirEntry {
            flags: 0x80, // in use, not locked
            finder_info: [0u8; 16],
            file_number,
            data_first_block: first_block,
            data_logical_length: bytes.len() as u32,
            rsrc_first_block: 0,
            rsrc_logical_length: 0,
            create_date: 0,
            modify_date: 0,
            name: name.to_string(),
        };
        self.entries.push(entry);
        // Surface a freshly-built FileEntry for the caller.
        let mut fe = FileEntry::new_file(
            name.to_string(),
            format!("/{name}"),
            bytes.len() as u64,
            file_number as u64,
        );
        fe.type_code = Some("    ".to_string());
        fe.creator_code = Some("    ".to_string());
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "MFS has no directory hierarchy — all files live in the volume root".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "MFS is flat; only files in the root are deletable".into(),
            ));
        }
        if entry.entry_type != EntryType::File {
            return Err(FilesystemError::InvalidData(
                "MFS only stores files; cannot delete a directory".into(),
            ));
        }
        let fnum = entry.location as u32;
        // Snapshot the chain heads before we touch the entries vec.
        let (data_head, rsrc_head) = {
            let de = self
                .entries
                .iter()
                .find(|e| e.is_in_use() && e.file_number == fnum)
                .ok_or_else(|| {
                    FilesystemError::NotFound(format!("file number {fnum} not in directory"))
                })?;
            (de.data_first_block, de.rsrc_first_block)
        };
        self.free_chain(data_head)?;
        self.free_chain(rsrc_head)?;
        // Remove the entry from the in-memory directory.
        self.entries
            .retain(|e| !(e.is_in_use() && e.file_number == fnum));
        self.mdb.num_files = self.mdb.num_files.saturating_sub(1);
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.dir_write_back()?;
        self.map_write_back()?;
        self.mdb_write_back()?;
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.mdb.free_blocks as u64 * self.mdb.alloc_block_size as u64)
    }

    fn set_type_creator(
        &mut self,
        entry: &FileEntry,
        type_code: &str,
        creator_code: &str,
    ) -> Result<(), FilesystemError> {
        if type_code.len() != 4 || creator_code.len() != 4 {
            return Err(FilesystemError::InvalidData(
                "Mac OSType / creator must be exactly 4 chars".into(),
            ));
        }
        let fnum = entry.location as u32;
        let de = self
            .entries
            .iter_mut()
            .find(|e| e.is_in_use() && e.file_number == fnum)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!("file number {fnum} not in directory"))
            })?;
        de.finder_info[0..4].copy_from_slice(type_code.as_bytes());
        de.finder_info[4..8].copy_from_slice(creator_code.as_bytes());
        Ok(())
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        if new_name == entry.name {
            return Ok(());
        }
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "MFS is flat; only files in the root are renamable".into(),
            ));
        }
        if entry.entry_type != EntryType::File {
            return Err(FilesystemError::InvalidData(
                "MFS only stores files; cannot rename a directory".into(),
            ));
        }
        let _ = validate_mfs_name(new_name)?;

        let fnum = entry.location as u32;
        // Reject a collision with a *different* in-use entry. MFS names are
        // case-sensitive on disk, so plain equality is the right comparison.
        if self
            .entries
            .iter()
            .any(|e| e.is_in_use() && e.file_number != fnum && e.name == new_name)
        {
            return Err(FilesystemError::AlreadyExists(new_name.to_string()));
        }

        // Overwrite only the name; flags, Finder info, file number, fork heads,
        // lengths and dates are untouched, so the file keeps its identity and
        // contents. Persistence happens via sync_metadata -> dir_write_back.
        let de = self
            .entries
            .iter_mut()
            .find(|e| e.is_in_use() && e.file_number == fnum)
            .ok_or_else(|| {
                FilesystemError::NotFound(format!("file number {fnum} not in directory"))
            })?;
        de.name = new_name.to_string();
        Ok(())
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

    // ------------------------------------------------------------------
    // EditableFilesystem — create / delete / round-trip
    // ------------------------------------------------------------------

    /// Build a fresh test volume already inside a `Cursor<Vec<u8>>`. The
    /// cursor is the in-memory backing store the edit tests mutate
    /// through `EditableFilesystem`, then reopen for read-side
    /// verification.
    fn edit_fixture() -> Cursor<Vec<u8>> {
        Cursor::new(build_test_mfs())
    }

    #[test]
    fn create_file_writes_data_and_round_trips_through_sync() {
        let mut fs = MfsFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let before_free = EditableFilesystem::free_space(&mut fs).unwrap();
        let payload = b"freshly-created via EditableFilesystem".to_vec();
        let mut src = Cursor::new(payload.clone());
        let opts = CreateFileOptions::default();
        let fe = fs
            .create_file(&root, "NewFile", &mut src, payload.len() as u64, &opts)
            .unwrap();
        assert_eq!(fe.name, "NewFile");
        assert_eq!(fe.size, payload.len() as u64);

        fs.sync_metadata().unwrap();
        // Reopen and prove the file persisted across a fresh parse.
        let inner = fs.reader.clone();
        let mut fs2 = MfsFilesystem::open(inner, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        let new = entries.iter().find(|e| e.name == "NewFile").unwrap();
        let read_back = fs2.read_file(new, 4096).unwrap();
        assert_eq!(read_back, payload);

        // Free-space accounting: one alloc-block consumed (payload < 1024 B).
        let after_free = EditableFilesystem::free_space(&mut fs2).unwrap();
        assert_eq!(after_free, before_free - 1024);
    }

    #[test]
    fn delete_entry_releases_blocks_and_clears_directory() {
        let mut fs = MfsFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let before_free = EditableFilesystem::free_space(&mut fs).unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let doc = entries.iter().find(|e| e.name == "Doc").unwrap().clone();

        fs.delete_entry(&root, &doc).unwrap();
        fs.sync_metadata().unwrap();

        let inner = fs.reader.clone();
        let mut fs2 = MfsFilesystem::open(inner, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        assert!(
            entries.iter().all(|e| e.name != "Doc"),
            "Doc should be gone after delete"
        );

        // "Doc" used 2 blocks (data fork + resource fork) at 1024 B each.
        let after_free = EditableFilesystem::free_space(&mut fs2).unwrap();
        assert_eq!(after_free, before_free + 2 * 1024);
    }

    #[test]
    fn rename_in_place_round_trips_through_sync() {
        let mut fs = MfsFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let doc = entries.iter().find(|e| e.name == "Doc").unwrap().clone();
        let fnum = doc.location;

        fs.rename(&root, &doc, "Report").unwrap();
        fs.sync_metadata().unwrap();

        // Reopen and prove the rename persisted with identity + contents intact.
        let inner = fs.reader.clone();
        let mut fs2 = MfsFilesystem::open(inner, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries2 = fs2.list_directory(&root2).unwrap();
        let names: Vec<&str> = entries2.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"Report"));
        assert!(!names.contains(&"Doc"));

        let renamed = entries2.iter().find(|e| e.name == "Report").unwrap();
        // Same file number (identity); both forks + resource size preserved.
        assert_eq!(renamed.location, fnum);
        assert_eq!(renamed.resource_fork_size, Some(4));
        assert_eq!(fs2.read_file(renamed, 1024).unwrap(), b"first line\n");
        assert_eq!(fs2.read_resource_fork(renamed).unwrap(), b"RSRC");
    }

    #[test]
    fn rename_rejects_collision() {
        let mut fs = MfsFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let doc = entries.iter().find(|e| e.name == "Doc").unwrap().clone();
        // "Hello" already exists; renaming "Doc" onto it must be rejected.
        let err = fs.rename(&root, &doc, "Hello").unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }

    #[test]
    fn create_file_rejects_duplicate_names() {
        let mut fs = MfsFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let mut src = Cursor::new(b"x".to_vec());
        let err = fs
            .create_file(&root, "Hello", &mut src, 1, &CreateFileOptions::default())
            .unwrap_err();
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    #[test]
    fn create_file_rejects_oversize_data() {
        let mut fs = MfsFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        // The fixture has 5 free blocks of 1024 B each = 5 KiB free.
        // Try to write 8 KiB — must fail with InvalidData.
        let huge = vec![0u8; 8 * 1024];
        let mut src = Cursor::new(huge);
        let err = fs
            .create_file(
                &root,
                "TooBig",
                &mut src,
                8 * 1024,
                &CreateFileOptions::default(),
            )
            .unwrap_err();
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    #[test]
    fn create_then_delete_returns_to_original_free_count() {
        let mut fs = MfsFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let before = EditableFilesystem::free_space(&mut fs).unwrap();
        let mut src = Cursor::new(b"abc".to_vec());
        let fe = fs
            .create_file(&root, "Tmp", &mut src, 3, &CreateFileOptions::default())
            .unwrap();
        let mid = EditableFilesystem::free_space(&mut fs).unwrap();
        assert_eq!(mid, before - 1024);
        // delete_entry needs a parent + the synthesized FileEntry from create.
        fs.delete_entry(&root, &fe).unwrap();
        let after = EditableFilesystem::free_space(&mut fs).unwrap();
        assert_eq!(after, before, "free count must return after create+delete");
    }

    #[test]
    fn create_directory_is_unsupported() {
        let mut fs = MfsFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let err = fs
            .create_directory(&root, "subdir", &CreateDirectoryOptions::default())
            .unwrap_err();
        assert!(matches!(err, FilesystemError::Unsupported(_)));
    }

    #[test]
    fn set_type_creator_updates_finder_info_and_persists() {
        let mut fs = MfsFilesystem::open(edit_fixture(), 0).unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let hello = entries.iter().find(|e| e.name == "Hello").unwrap().clone();
        fs.set_type_creator(&hello, "PICT", "8BIM").unwrap();
        fs.sync_metadata().unwrap();

        let inner = fs.reader.clone();
        let mut fs2 = MfsFilesystem::open(inner, 0).unwrap();
        let root2 = fs2.root().unwrap();
        let entries = fs2.list_directory(&root2).unwrap();
        let hello2 = entries.iter().find(|e| e.name == "Hello").unwrap();
        assert_eq!(hello2.type_code.as_deref(), Some("PICT"));
        assert_eq!(hello2.creator_code.as_deref(), Some("8BIM"));
    }
}
