//! UCSD p-System filesystem — read driver.
//!
//! The filesystem of UCSD Pascal (Apple II/III, PC, and many 1970s-80s
//! machines). It is deliberately tiny: a single **flat** directory (no
//! subdirectories) of at most 77 files, each stored as a **contiguous** run of
//! 512-byte blocks. There is no allocation bitmap — the sorted directory *is*
//! the allocation map, and free space is the gaps between files.
//!
//! Layout (from `ucsdpsys_fs(5)`):
//!
//! ```text
//!   block 0,1     bootstrap
//!   block 2..5    directory (4 blocks = 78 x 26-byte entries)
//!   block 6..     file data
//! ```
//!
//! Directory entry 0 is the **volume label**; entries 1..77 are files. Every
//! multi-byte word takes the host CPU's byte order; it is recovered from the
//! volume label's `DLASTBLK` field, which is always 6 or 10 (so its high byte
//! is zero). All offsets below were verified byte-for-byte against a clean-room
//! oracle (`scripts/ucsd-oracle.py`) built from the same spec.
//!
//! This is the read half of the UCSD quartet (Browse); edit / create / fsck
//! build on it (`docs/filesystem_completion_plan.md` Part 2).

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::fsck::{FsckIssue, FsckResult, FsckStats, RepairReport};

const BLOCK: u64 = 512;
/// Directory starts at block 2 and spans 4 blocks.
const DIR_BLOCK: u64 = 2;
const DIR_BLOCKS: u64 = 4;
/// Directory-entry stride.
const ENTRY: usize = 26;
/// First data block after a single (non-duplicated) directory.
const DIR_END: u16 = 6;
/// First data block after a duplicated (backed-up) directory.
const DIR_END_DUP: u16 = 10;

const KIND_NAMES: [&str; 9] = [
    "untyped",
    "xdsk",
    "code",
    "text",
    "info",
    "data",
    "graf",
    "foto",
    "securedir",
];

/// A parsed file directory entry.
#[derive(Clone, Debug)]
pub struct UcsdEntry {
    pub first_block: u16,
    pub last_block: u16, // first block past the file
    pub kind: u8,
    pub name: String,
    pub last_byte: u16, // bytes used in the final block (1..=512)
    pub date: u16,
}

impl UcsdEntry {
    fn byte_size(&self) -> u64 {
        if self.last_block <= self.first_block {
            return 0;
        }
        (self.last_block - self.first_block - 1) as u64 * BLOCK + self.last_byte as u64
    }
}

pub struct UcsdFilesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    little_endian: bool,
    volume_name: String,
    /// Blocks on the volume, from the label (`DEOVBLK`).
    eov_blocks: u16,
    /// First block past the directory (`DLASTBLK` of the label): 6 or 10.
    dir_end: u16,
    /// Volume label date word (preserved across writes).
    vol_date: u16,
    entries: Vec<UcsdEntry>,
}

fn read_u16(buf: &[u8], off: usize, little: bool) -> u16 {
    if little {
        u16::from_le_bytes([buf[off], buf[off + 1]])
    } else {
        u16::from_be_bytes([buf[off], buf[off + 1]])
    }
}

/// A UCSD name field: byte `off` is the length, then 7-bit ASCII. `max` is 7
/// for the volume label, 15 for files. Returns None if the length is invalid.
fn read_name(buf: &[u8], off: usize, max: usize) -> Option<String> {
    let n = buf[off] as usize;
    if n == 0 || n > max {
        return None;
    }
    let bytes = &buf[off + 1..off + 1 + n];
    if bytes.iter().any(|&b| !(0x20..=0x7E).contains(&b)) {
        return None;
    }
    Some(String::from_utf8_lossy(bytes).into_owned())
}

/// Recover byte order from the volume-label DLASTBLK (bytes 2..3 of block 2),
/// which is 6 or 10. Returns `Some(little_endian)` or None if it looks wrong.
fn detect_byte_sex(dir: &[u8]) -> Option<bool> {
    let (b2, b3) = (dir[2], dir[3]);
    if (b2 == 6 || b2 == 10) && b3 == 0 {
        Some(true)
    } else if (b3 == 6 || b3 == 10) && b2 == 0 {
        Some(false)
    } else {
        None
    }
}

/// True if the block-2 buffer looks like a UCSD volume label. Used for content
/// detection. Requires ≥ 32 bytes (one directory entry).
pub fn looks_like_ucsd(dir: &[u8]) -> bool {
    if dir.len() < ENTRY + 6 {
        return false;
    }
    let Some(little) = detect_byte_sex(dir) else {
        return false;
    };
    // FIRSTBLK == 0, kind == 0, DLASTBLK in {6,10}, a valid 1..7 volume name,
    // and a plausible non-zero volume block count.
    if read_u16(dir, 0, little) != 0 {
        return false;
    }
    let dlast = read_u16(dir, 2, little);
    if dlast != DIR_END && dlast != DIR_END_DUP {
        return false;
    }
    if read_u16(dir, 4, little) != 0 {
        return false;
    }
    if read_name(dir, 6, 7).is_none() {
        return false;
    }
    let eov = read_u16(dir, 14, little);
    // At least the directory + one block, and not absurdly small.
    eov > DIR_END
}

impl<R: Read + Seek> UcsdFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let mut dir = vec![0u8; (DIR_BLOCKS * BLOCK) as usize];
        reader.seek(SeekFrom::Start(partition_offset + DIR_BLOCK * BLOCK))?;
        reader.read_exact(&mut dir)?;
        if !looks_like_ucsd(&dir) {
            return Err(FilesystemError::InvalidData(
                "not a UCSD p-System volume label".into(),
            ));
        }
        let little = detect_byte_sex(&dir).unwrap();
        let volume_name = read_name(&dir, 6, 7).unwrap_or_default();
        let dir_end = read_u16(&dir, 2, little);
        let eov_blocks = read_u16(&dir, 14, little);
        let vol_date = read_u16(&dir, 20, little);
        let num_files = read_u16(&dir, 16, little) as usize;

        let mut entries = Vec::new();
        // Entries 1..=num_files (bounded by the 77-file / directory capacity).
        let max_entries = ((DIR_BLOCKS * BLOCK) as usize / ENTRY).saturating_sub(1);
        for i in 1..=num_files.min(max_entries) {
            let off = i * ENTRY;
            let first_block = read_u16(&dir, off, little);
            let last_block = read_u16(&dir, off + 2, little);
            let kind = (read_u16(&dir, off + 4, little) & 0xF) as u8;
            let Some(name) = read_name(&dir, off + 6, 15) else {
                continue;
            };
            let last_byte = read_u16(&dir, off + 22, little);
            let date = read_u16(&dir, off + 24, little);
            entries.push(UcsdEntry {
                first_block,
                last_block,
                kind,
                name,
                last_byte,
                date,
            });
        }
        // Keep entries ordered by first block so gap-finding and write-back
        // (which the format assumes is sorted) are correct even on a foreign
        // volume whose directory order drifted.
        entries.sort_by_key(|e| e.first_block);

        Ok(UcsdFilesystem {
            reader,
            partition_offset,
            little_endian: little,
            volume_name,
            eov_blocks,
            dir_end,
            vol_date,
            entries,
        })
    }

    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.read_exact(buf)?;
        Ok(())
    }

    fn entry_to_file(&self, e: &UcsdEntry) -> FileEntry {
        let mut fe = FileEntry::new_file(
            e.name.clone(),
            format!("/{}", e.name),
            e.byte_size(),
            e.first_block as u64,
        );
        fe.modified = decode_date(e.date);
        fe.modified_unix = crate::fs::times::ucsd_date_to_unix(e.date);
        fe
    }
}

/// UCSD date word: bits 0-4 day (1..31), 5-8 month (1..12), 9-15 year (0..99).
fn decode_date(word: u16) -> Option<String> {
    if word == 0 {
        return None;
    }
    let day = word & 0x1F;
    let month = (word >> 5) & 0x0F;
    let year = (word >> 9) & 0x7F;
    if day == 0 || month == 0 || month > 12 {
        return None;
    }
    // Two-digit year: the p-System treats 0..=79 as 20xx conventionally, but we
    // just render the raw fields (the era is ambiguous on disk).
    Some(format!("{:04}-{:02}-{:02}", 1900 + year as u32, month, day))
}

impl<R: Read + Seek + Send> Filesystem for UcsdFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        // The filesystem is flat: only the root directory has entries.
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        Ok(self
            .entries
            .clone()
            .iter()
            .map(|e| self.entry_to_file(e))
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "UCSD read_file on directory: {}",
                entry.path
            )));
        }
        let want = (entry.size as usize).min(max_bytes);
        if want == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; want];
        self.read_at(entry.location * BLOCK, &mut buf)?;
        Ok(buf)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.volume_name.is_empty() {
            None
        } else {
            Some(&self.volume_name)
        }
    }

    fn fs_type(&self) -> &str {
        "UCSD p-System"
    }

    fn total_size(&self) -> u64 {
        self.eov_blocks as u64 * BLOCK
    }

    fn used_size(&self) -> u64 {
        // Boot + directory, plus every file's block span.
        let meta = DIR_END as u64 * BLOCK;
        let files: u64 = self
            .entries
            .iter()
            .map(|e| (e.last_block.saturating_sub(e.first_block)) as u64 * BLOCK)
            .sum();
        meta + files
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(BLOCK)
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let last = self
            .entries
            .iter()
            .map(|e| e.last_block)
            .max()
            .unwrap_or(DIR_END);
        Ok(last as u64 * BLOCK)
    }

    fn fsck(&mut self) -> Option<Result<FsckResult, FilesystemError>> {
        Some(fsck_ucsd(self))
    }
}

/// Human-readable file-kind name (for callers that want the type).
pub fn kind_name(kind: u8) -> &'static str {
    KIND_NAMES.get(kind as usize).copied().unwrap_or("unknown")
}

/// Sanitize a volume name to the UCSD rules: uppercase, ≤ 7 printable ASCII
/// chars (no `/` `:`), non-empty. Falls back to `RBVOL`.
fn sanitize_volume_name(name: &str) -> String {
    let up: String = name
        .to_ascii_uppercase()
        .chars()
        .filter(|&c| c.is_ascii_graphic() && c != '/' && c != ':')
        .take(7)
        .collect();
    if up.is_empty() {
        "RBVOL".to_string()
    } else {
        up
    }
}

/// Format a blank UCSD p-System volume of `size_bytes` (little-endian, the
/// Apple II / PC convention): zeroed boot + a volume label at block 2 with an
/// empty directory. Returns the raw image.
pub fn create_blank_ucsd(size_bytes: u64, name: &str) -> Result<Vec<u8>, FilesystemError> {
    let blocks = size_bytes / BLOCK;
    if blocks <= DIR_END as u64 {
        return Err(FilesystemError::Unsupported(
            "UCSD volume too small (needs more than 6 blocks)".into(),
        ));
    }
    if blocks > u16::MAX as u64 {
        return Err(FilesystemError::Unsupported(
            "UCSD volume too large (max 65535 blocks / 32 MiB)".into(),
        ));
    }
    let vname = sanitize_volume_name(name);
    let mut img = vec![0u8; (blocks * BLOCK) as usize];
    let base = (DIR_BLOCK * BLOCK) as usize;
    let d = &mut img[base..base + (DIR_BLOCKS * BLOCK) as usize];
    write_u16(d, 0, 0, true); // FIRSTBLK
    write_u16(d, 2, DIR_END, true); // DLASTBLK = 6
    write_u16(d, 4, 0, true); // kind = untyped
    write_name(d, 6, &vname, 7);
    write_u16(d, 14, blocks as u16, true); // DEOVBLK
    write_u16(d, 16, 0, true); // DNUMFILES
    write_u16(d, 20, 0, true); // DLASTBOOT date
    Ok(img)
}

fn write_u16(buf: &mut [u8], off: usize, val: u16, little: bool) {
    let b = if little {
        val.to_le_bytes()
    } else {
        val.to_be_bytes()
    };
    buf[off..off + 2].copy_from_slice(&b);
}

fn write_name(buf: &mut [u8], off: usize, name: &str, max: usize) {
    let nb = name.to_ascii_uppercase().into_bytes();
    let n = nb.len().min(max);
    buf[off] = n as u8;
    buf[off + 1..off + 1 + max].fill(0);
    buf[off + 1..off + 1 + n].copy_from_slice(&nb[..n]);
}

/// Guess a UCSD file kind from the name suffix; defaults to datafile.
fn kind_from_name(name: &str) -> u8 {
    let upper = name.to_ascii_uppercase();
    if upper.ends_with(".TEXT") {
        3
    } else if upper.ends_with(".CODE") {
        2
    } else if upper.ends_with(".INFO") {
        4
    } else if upper.ends_with(".GRAF") {
        6
    } else if upper.ends_with(".FOTO") {
        7
    } else {
        5 // datafile
    }
}

fn valid_ucsd_name(name: &str) -> bool {
    let up = name.to_ascii_uppercase();
    !up.is_empty()
        && up.len() <= 15
        && up
            .bytes()
            .all(|b| (0x20..=0x7E).contains(&b) && b != b'/' && b != b':')
}

impl<R: Read + Write + Seek> UcsdFilesystem<R> {
    fn write_at(&mut self, off: u64, data: &[u8]) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.partition_offset + off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Re-serialize the volume label + file entries and write the directory
    /// (blocks 2..5). Entries are assumed sorted by first block.
    fn write_directory(&mut self) -> Result<(), FilesystemError> {
        let little = self.little_endian;
        let mut dir = vec![0u8; (DIR_BLOCKS * BLOCK) as usize];
        write_u16(&mut dir, 0, 0, little); // FIRSTBLK
        write_u16(&mut dir, 2, self.dir_end, little); // DLASTBLK
        write_u16(&mut dir, 4, 0, little); // kind
        write_name(&mut dir, 6, &self.volume_name, 7);
        write_u16(&mut dir, 14, self.eov_blocks, little); // DEOVBLK
        write_u16(&mut dir, 16, self.entries.len() as u16, little); // DNUMFILES
        write_u16(&mut dir, 20, self.vol_date, little); // DLASTBOOT
        for (i, e) in self.entries.iter().enumerate() {
            let off = (i + 1) * ENTRY;
            write_u16(&mut dir, off, e.first_block, little);
            write_u16(&mut dir, off + 2, e.last_block, little);
            write_u16(&mut dir, off + 4, e.kind as u16, little);
            write_name(&mut dir, off + 6, &e.name, 15);
            write_u16(&mut dir, off + 22, e.last_byte, little);
            write_u16(&mut dir, off + 24, e.date, little);
        }
        self.write_at(DIR_BLOCK * BLOCK, &dir)
    }

    /// First-fit contiguous free run of `need` blocks (entries kept sorted).
    fn find_gap(&self, need: u16) -> Option<u16> {
        let mut cursor = self.dir_end;
        for e in &self.entries {
            if e.first_block >= cursor && e.first_block - cursor >= need {
                return Some(cursor);
            }
            cursor = cursor.max(e.last_block);
        }
        if self.eov_blocks >= cursor && self.eov_blocks - cursor >= need {
            Some(cursor)
        } else {
            None
        }
    }

    fn total_free_blocks(&self) -> u64 {
        let mut free = 0u64;
        let mut cursor = self.dir_end;
        for e in &self.entries {
            if e.first_block > cursor {
                free += (e.first_block - cursor) as u64;
            }
            cursor = cursor.max(e.last_block);
        }
        if self.eov_blocks > cursor {
            free += (self.eov_blocks - cursor) as u64;
        }
        free
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for UcsdFilesystem<R> {
    fn as_filesystem(&self) -> &dyn crate::fs::filesystem::Filesystem {
        self
    }
    fn as_filesystem_mut(&mut self) -> &mut dyn crate::fs::filesystem::Filesystem {
        self
    }
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if !parent.is_directory() || parent.path != "/" {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        if !valid_ucsd_name(name) {
            return Err(FilesystemError::InvalidData(format!(
                "invalid UCSD file name {name:?} (1-15 printable chars, no / :)"
            )));
        }
        let uname = name.to_ascii_uppercase();
        if self.entries.iter().any(|e| e.name == uname) {
            return Err(FilesystemError::AlreadyExists(uname));
        }
        if self.entries.len() >= 77 {
            return Err(FilesystemError::DiskFull(
                "UCSD directory full (77 files)".into(),
            ));
        }
        let need = if data_len == 0 {
            1
        } else {
            data_len.div_ceil(BLOCK) as u16
        };
        let start = self.find_gap(need).ok_or_else(|| {
            FilesystemError::DiskFull(format!(
                "no contiguous free run of {need} blocks (UCSD files are contiguous)"
            ))
        })?;
        // Write the data, zero-padding the final block.
        let mut buf = vec![0u8; need as usize * BLOCK as usize];
        if data_len > 0 {
            data.read_exact(&mut buf[..data_len as usize])?;
        }
        self.write_at(start as u64 * BLOCK, &buf)?;
        let last_byte = if data_len == 0 {
            BLOCK as u16
        } else {
            (data_len - (need as u64 - 1) * BLOCK) as u16
        };
        // UCSD packs the date as a single 16-bit word (day | month | year),
        // year 0..99 -> 1900..1999. Cross-fs copy passes source mtime through
        // options.unix_times; a genuinely new file leaves it None and stamps
        // zero (matching pre-existing behaviour — UCSD doesn't have a "now"
        // convention and generators here forbid wall-clock reads).
        let date = options
            .unix_times
            .map(|t| crate::fs::times::unix_to_ucsd_date(t.mtime_or_now()))
            .unwrap_or(0);
        let entry = UcsdEntry {
            first_block: start,
            last_block: start + need,
            kind: kind_from_name(&uname),
            name: uname,
            last_byte,
            date,
        };
        let fe = self.entry_to_file(&entry);
        self.entries.push(entry);
        self.entries.sort_by_key(|e| e.first_block);
        self.write_directory()?;
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "UCSD p-System is a flat filesystem — no subdirectories".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let before = self.entries.len();
        self.entries.retain(|e| e.name != entry.name);
        if self.entries.len() == before {
            return Err(FilesystemError::NotFound(entry.name.clone()));
        }
        // Contiguous allocation has no bitmap; removing the entry frees the run.
        self.write_directory()
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        if !parent.is_directory() {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        if new_name == entry.name {
            return Ok(());
        }
        if !valid_ucsd_name(new_name) {
            return Err(FilesystemError::InvalidData(format!(
                "invalid UCSD file name {new_name:?}"
            )));
        }
        let uname = new_name.to_ascii_uppercase();
        if self.entries.iter().any(|e| e.name == uname) {
            return Err(FilesystemError::AlreadyExists(uname));
        }
        let target = self
            .entries
            .iter_mut()
            .find(|e| e.name == entry.name)
            .ok_or_else(|| FilesystemError::NotFound(entry.name.clone()))?;
        target.name = uname;
        self.write_directory()
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.total_free_blocks() * BLOCK)
    }

    fn repair(&mut self) -> Result<RepairReport, FilesystemError> {
        repair_ucsd(self)
    }
}

// ---- fsck (check + repair) ----
//
// UCSD has no allocation bitmap — the sorted directory *is* the allocation
// map — so the check is directory self-consistency: a valid volume label,
// each file within bounds, and non-overlapping contiguous runs. The safely
// recomputable fixes (re-sort entries, correct DNUMFILES, drop invalid-name
// entries) are repaired by re-serializing the directory; structural damage
// (overlaps, past-end / into-directory runs) is surfaced read-only.

fn issue(code: &str, message: String, repairable: bool) -> FsckIssue {
    FsckIssue {
        code: code.into(),
        message,
        repairable,
        debug: false,
    }
}

pub fn fsck_ucsd<R: Read + Seek>(
    fs: &mut UcsdFilesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let little = fs.little_endian;
    let dir_end = fs.dir_end;
    let eov = fs.eov_blocks;
    let end = fs.reader.seek(SeekFrom::End(0))?;
    let disk_blocks = end.saturating_sub(fs.partition_offset) / BLOCK;

    let mut dir = vec![0u8; (DIR_BLOCKS * BLOCK) as usize];
    fs.read_at(DIR_BLOCK * BLOCK, &mut dir)?;

    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    if read_u16(&dir, 0, little) != 0 {
        errors.push(issue(
            "VolFirstBlk",
            "volume label FIRSTBLK is not 0".into(),
            false,
        ));
    }
    let dlast = read_u16(&dir, 2, little);
    if dlast != DIR_END && dlast != DIR_END_DUP {
        errors.push(issue(
            "VolDirEnd",
            format!("volume label DLASTBLK is {dlast}, expected 6 or 10"),
            false,
        ));
    }
    if read_name(&dir, 6, 7).is_none() {
        errors.push(issue(
            "VolName",
            "volume label name length is invalid".into(),
            false,
        ));
    }
    if eov as u64 > disk_blocks {
        errors.push(issue(
            "VolSize",
            format!("DEOVBLK {eov} exceeds the {disk_blocks}-block image"),
            false,
        ));
    }

    let dnumfiles = read_u16(&dir, 16, little) as usize;
    let max_entries = (DIR_BLOCKS * BLOCK) as usize / ENTRY - 1;
    let mut spans: Vec<(u16, u16, String)> = Vec::new();
    let mut files = 0u32;
    for i in 1..=dnumfiles.min(max_entries) {
        let off = i * ENTRY;
        let first = read_u16(&dir, off, little);
        let last = read_u16(&dir, off + 2, little);
        let kind = read_u16(&dir, off + 4, little) & 0xF;
        let last_byte = read_u16(&dir, off + 22, little);
        let Some(name) = read_name(&dir, off + 6, 15) else {
            errors.push(issue(
                "BadName",
                format!("directory entry {i} has an invalid name length"),
                true,
            ));
            continue;
        };
        files += 1;
        if first >= last {
            errors.push(issue(
                "BadRange",
                format!("file '{name}' FIRSTBLK {first} >= DLASTBLK {last}"),
                false,
            ));
        }
        if first < dir_end {
            errors.push(issue(
                "IntoDir",
                format!("file '{name}' overlaps the directory (first block {first})"),
                false,
            ));
        }
        if last as u64 > eov as u64 {
            errors.push(issue(
                "PastEnd",
                format!("file '{name}' DLASTBLK {last} is past DEOVBLK {eov}"),
                false,
            ));
        }
        if !(1..=512).contains(&last_byte) {
            errors.push(issue(
                "BadLastByte",
                format!("file '{name}' DLASTBYTE {last_byte} out of range"),
                false,
            ));
        }
        if kind > 8 {
            warnings.push(issue(
                "BadKind",
                format!("file '{name}' has invalid kind {kind}"),
                false,
            ));
        }
        spans.push((first, last, name));
    }

    // Overlap check over the block-sorted runs.
    let mut sorted = spans.clone();
    sorted.sort_by_key(|s| s.0);
    for w in sorted.windows(2) {
        if w[1].0 < w[0].1 {
            errors.push(issue(
                "Overlap",
                format!(
                    "files '{}' ({}..{}) and '{}' ({}..{}) overlap",
                    w[0].2, w[0].0, w[0].1, w[1].2, w[1].0, w[1].1
                ),
                false,
            ));
        }
    }

    // Recomputable inconsistencies.
    if spans.iter().map(|s| s.0).ne(sorted.iter().map(|s| s.0)) {
        warnings.push(issue(
            "Unsorted",
            "directory entries are not sorted by first block".into(),
            true,
        ));
    }
    if dnumfiles != fs.entries.len() {
        warnings.push(issue(
            "FileCount",
            format!(
                "DNUMFILES {dnumfiles} disagrees with {} valid entries",
                fs.entries.len()
            ),
            true,
        ));
    }

    let repairable = errors.iter().any(|e| e.repairable) || warnings.iter().any(|w| w.repairable);
    Ok(FsckResult {
        stats: FsckStats {
            files_checked: files,
            directories_checked: 1,
            extra: vec![
                ("volume".into(), fs.volume_name.clone()),
                ("blocks".into(), eov.to_string()),
            ],
        },
        repairable,
        errors,
        warnings,
        orphaned_entries: Vec::new(),
    })
}

pub fn repair_ucsd<R: Read + Write + Seek>(
    fs: &mut UcsdFilesystem<R>,
) -> Result<RepairReport, FilesystemError> {
    let check = fsck_ucsd(fs)?;
    let mut report = RepairReport {
        fixes_applied: Vec::new(),
        fixes_failed: Vec::new(),
        unrepairable_count: check.errors.iter().filter(|e| !e.repairable).count(),
    };
    let has_fixable =
        check.errors.iter().any(|e| e.repairable) || check.warnings.iter().any(|w| w.repairable);
    if has_fixable {
        // `fs.entries` is the parsed, valid, block-sorted set; re-serializing
        // corrects DNUMFILES and sort order and drops invalid-name entries.
        fs.write_directory()?;
        fs.reader.flush()?;
        report.fixes_applied.push(
            "rewrote the directory: sorted entries, corrected the file count, dropped \
             invalid entries"
                .into(),
        );
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::process::Command;

    fn oracle() -> Option<PathBuf> {
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/ucsd-oracle.py");
        p.exists().then_some(p)
    }

    /// Build a real UCSD volume with the clean-room oracle: mkfs + a couple of
    /// files. Returns the raw image bytes, or None if python3 is unavailable.
    fn oracle_volume() -> Option<Vec<u8>> {
        let script = oracle()?;
        if Command::new("python3").arg("--version").output().is_err() {
            return None;
        }
        let dir = std::env::temp_dir();
        let vol = dir.join(format!("rb_ucsd_{}.vol", std::process::id()));
        let run = |args: &[&str]| {
            Command::new("python3")
                .arg(&script)
                .args(args)
                .output()
                .ok()
                .map(|o| o.status.success())
                .unwrap_or(false)
        };
        let volp = vol.to_str()?;
        if !run(&["mkfs", volp, "280", "MYVOL"]) {
            return None;
        }
        let hi = dir.join(format!("rb_ucsd_hi_{}.txt", std::process::id()));
        std::fs::write(&hi, b"HELLO UCSD\n").ok()?;
        let big = dir.join(format!("rb_ucsd_big_{}.dat", std::process::id()));
        std::fs::write(&big, vec![0x41u8; 3000]).ok()?;
        run(&["addfile", volp, "HELLO.TEXT", "3", hi.to_str()?]);
        run(&["addfile", volp, "BIG.DATA", "5", big.to_str()?]);
        let bytes = std::fs::read(&vol).ok();
        let _ = std::fs::remove_file(&vol);
        let _ = std::fs::remove_file(&hi);
        let _ = std::fs::remove_file(&big);
        bytes
    }

    #[test]
    fn reads_oracle_volume() {
        let Some(img) = oracle_volume() else {
            eprintln!("skipping reads_oracle_volume: python3/oracle unavailable");
            return;
        };
        let mut fs = UcsdFilesystem::open(Cursor::new(img), 0).expect("open");
        assert_eq!(fs.volume_label(), Some("MYVOL"));
        assert_eq!(fs.total_size(), 280 * BLOCK);
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = files.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"HELLO.TEXT"), "{names:?}");
        assert!(names.contains(&"BIG.DATA"), "{names:?}");

        let big = files.iter().find(|e| e.name == "BIG.DATA").unwrap();
        assert_eq!(big.size, 3000);
        let data = fs.read_file(big, usize::MAX).unwrap();
        assert_eq!(data.len(), 3000);
        assert!(data.iter().all(|&b| b == 0x41));
    }

    #[test]
    fn detection_and_byte_sex() {
        // Minimal little-endian volume label.
        let mut dir = [0u8; ENTRY * 2];
        dir[0..2].copy_from_slice(&0u16.to_le_bytes()); // FIRSTBLK
        dir[2..4].copy_from_slice(&6u16.to_le_bytes()); // DLASTBLK
        dir[4..6].copy_from_slice(&0u16.to_le_bytes()); // kind
        dir[6] = 5;
        dir[7..12].copy_from_slice(b"MYVOL");
        dir[14..16].copy_from_slice(&280u16.to_le_bytes()); // DEOVBLK
        assert!(looks_like_ucsd(&dir));
        assert_eq!(detect_byte_sex(&dir), Some(true));

        // Big-endian: DLASTBLK high byte holds the 6.
        let mut be = [0u8; ENTRY * 2];
        be[2..4].copy_from_slice(&6u16.to_be_bytes());
        be[6] = 3;
        be[7..10].copy_from_slice(b"VOL");
        be[14..16].copy_from_slice(&280u16.to_be_bytes());
        assert_eq!(detect_byte_sex(&be), Some(false));
        assert!(looks_like_ucsd(&be));

        // Random data is rejected.
        assert!(!looks_like_ucsd(&[0xABu8; ENTRY * 2]));
    }

    #[test]
    fn date_decode() {
        // year 84, month 6, day 15 -> (84<<9)|(6<<5)|15
        let word = (84u16 << 9) | (6 << 5) | 15;
        assert_eq!(decode_date(word).as_deref(), Some("1984-06-15"));
        assert_eq!(decode_date(0), None);
    }

    // ---- Edit ----

    /// A blank little-endian UCSD volume (empty directory) built in memory.
    fn blank_volume(blocks: u16, name: &str) -> Vec<u8> {
        let mut img = vec![0u8; blocks as usize * BLOCK as usize];
        let base = (DIR_BLOCK * BLOCK) as usize;
        img[base..base + 2].copy_from_slice(&0u16.to_le_bytes()); // FIRSTBLK
        img[base + 2..base + 4].copy_from_slice(&DIR_END.to_le_bytes()); // DLASTBLK
        img[base + 6] = name.len() as u8;
        img[base + 7..base + 7 + name.len()].copy_from_slice(name.as_bytes());
        img[base + 14..base + 16].copy_from_slice(&blocks.to_le_bytes()); // DEOVBLK
        img
    }

    #[test]
    fn edit_create_rename_delete_roundtrip() {
        let mut fs =
            UcsdFilesystem::open(Cursor::new(blank_volume(280, "EDITVOL")), 0).expect("open blank");
        let root = fs.root().unwrap();
        let a = [0x2Au8; 100];
        fs.create_file(
            &root,
            "ALPHA.TEXT",
            &mut &a[..],
            100,
            &CreateFileOptions::default(),
        )
        .expect("create ALPHA");
        let b = [0x5Bu8; 2000]; // 4 blocks
        fs.create_file(
            &root,
            "BETA.DATA",
            &mut &b[..],
            2000,
            &CreateFileOptions::default(),
        )
        .expect("create BETA");

        let alpha = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "ALPHA.TEXT")
            .unwrap();
        fs.rename(&root, &alpha, "GAMMA.TEXT").expect("rename");

        fs.create_file(
            &root,
            "TMP.DATA",
            &mut &b"x"[..],
            1,
            &CreateFileOptions::default(),
        )
        .expect("create TMP");
        let tmp = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "TMP.DATA")
            .unwrap();
        fs.delete_entry(&root, &tmp).expect("delete TMP");

        let files = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = files.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"GAMMA.TEXT"), "{names:?}");
        assert!(names.contains(&"BETA.DATA"), "{names:?}");
        assert!(!names.contains(&"ALPHA.TEXT"));
        assert!(!names.contains(&"TMP.DATA"));

        let beta = files.iter().find(|e| e.name == "BETA.DATA").unwrap();
        assert_eq!(beta.size, 2000);
        let data = fs.read_file(beta, usize::MAX).unwrap();
        assert_eq!(data.len(), 2000);
        assert!(data.iter().all(|&x| x == 0x5B));

        // Subdirectories are not a thing in UCSD.
        assert!(matches!(
            fs.create_directory(&root, "sub", &CreateDirectoryOptions::default()),
            Err(FilesystemError::Unsupported(_))
        ));
    }

    #[test]
    fn edit_reads_back_through_reopen() {
        // Edit an in-memory volume, then reopen it to prove the directory was
        // persisted to disk (not just held in memory).
        let mut cur = Cursor::new(blank_volume(280, "REOPEN"));
        {
            let mut fs = UcsdFilesystem::open(&mut cur, 0).unwrap();
            let root = fs.root().unwrap();
            let data = [0x7Eu8; 1500];
            fs.create_file(
                &root,
                "KEEP.DATA",
                &mut &data[..],
                1500,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        cur.set_position(0);
        let mut fs = UcsdFilesystem::open(&mut cur, 0).unwrap();
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "KEEP.DATA");
        assert_eq!(files[0].size, 1500);
        assert_eq!(fs.read_file(&files[0], usize::MAX).unwrap().len(), 1500);
    }

    #[test]
    fn oracle_edit_is_fsck_clean() {
        let Some(script) = oracle() else {
            eprintln!("skipping oracle_edit_is_fsck_clean: oracle unavailable");
            return;
        };
        if Command::new("python3").arg("--version").output().is_err() {
            return;
        }
        let vol = std::env::temp_dir().join(format!("rb_ucsd_edit_{}.vol", std::process::id()));
        let volp = vol.to_str().unwrap();
        let run = |args: &[&str]| {
            Command::new("python3")
                .arg(&script)
                .args(args)
                .output()
                .expect("python3")
        };
        assert!(run(&["mkfs", volp, "280", "OVOL"]).status.success());

        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            let mut fs = UcsdFilesystem::open(file, 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "ALPHA.TEXT",
                &mut &[0x41u8; 100][..],
                100,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.create_file(
                &root,
                "BETA.DATA",
                &mut &[0x42u8; 2000][..],
                2000,
                &CreateFileOptions::default(),
            )
            .unwrap();
            let alpha = fs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "ALPHA.TEXT")
                .unwrap();
            fs.rename(&root, &alpha, "GAMMA.TEXT").unwrap();
            fs.sync_metadata().unwrap();
        }

        let out = run(&["fsck", volp]);
        let clean = out.status.success();
        let log = String::from_utf8_lossy(&out.stdout).into_owned();
        let _ = std::fs::remove_file(&vol);
        assert!(clean, "oracle fsck flagged our edits:\n{log}");
    }

    // ---- Create-blank ----

    #[test]
    fn create_blank_opens_and_edits() {
        let img = create_blank_ucsd(140 * 1024, "APPLE1").expect("format");
        let mut fs = UcsdFilesystem::open(Cursor::new(img), 0).expect("open blank");
        assert_eq!(fs.volume_label(), Some("APPLE1"));
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
        assert_eq!(fs.total_size(), 280 * BLOCK);
        // The blank is a valid substrate for edits.
        fs.create_file(
            &root,
            "X.DATA",
            &mut &[1u8; 500][..],
            500,
            &CreateFileOptions::default(),
        )
        .unwrap();
        assert_eq!(fs.list_directory(&root).unwrap().len(), 1);
    }

    #[test]
    fn create_blank_name_sanitized() {
        // Lowercase, too long, and illegal chars are cleaned to ≤7 upper ASCII.
        let img = create_blank_ucsd(140 * 1024, "my/vol:name").unwrap();
        let fs = UcsdFilesystem::open(Cursor::new(img), 0).unwrap();
        assert_eq!(fs.volume_label(), Some("MYVOLNA"));
    }

    #[test]
    fn oracle_create_blank_is_fsck_clean() {
        let Some(script) = oracle() else {
            eprintln!("skipping oracle_create_blank_is_fsck_clean: oracle unavailable");
            return;
        };
        if Command::new("python3").arg("--version").output().is_err() {
            return;
        }
        let vol = std::env::temp_dir().join(format!("rb_ucsd_blank_{}.vol", std::process::id()));
        std::fs::write(&vol, create_blank_ucsd(140 * 1024, "BLANKV").unwrap()).unwrap();
        let volp = vol.to_str().unwrap();
        let fsck = |v: &str| {
            Command::new("python3")
                .arg(&script)
                .args(["fsck", v])
                .output()
                .expect("python3")
                .status
                .success()
        };
        assert!(fsck(volp), "oracle fsck flagged our blank");
        // Still clean after writing a file through our editor.
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            let mut fs = UcsdFilesystem::open(file, 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "R.TEXT",
                &mut &[0x20u8; 900][..],
                900,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        let ok = fsck(volp);
        let _ = std::fs::remove_file(&vol);
        assert!(ok, "oracle fsck flagged our blank after an edit");
    }

    // ---- fsck ----

    fn forge_entry(img: &mut [u8], off: usize, first: u16, last: u16, kind: u16, name: &str) {
        write_u16(img, off, first, true);
        write_u16(img, off + 2, last, true);
        write_u16(img, off + 4, kind, true);
        write_name(img, off + 6, name, 15);
        write_u16(img, off + 22, 512, true); // DLASTBYTE
    }

    #[test]
    fn fsck_on_blank_and_populated_is_clean() {
        let mut fs = UcsdFilesystem::open(
            Cursor::new(create_blank_ucsd(140 * 1024, "CLEAN").unwrap()),
            0,
        )
        .unwrap();
        let r = fsck_ucsd(&mut fs).unwrap();
        assert!(r.is_clean() && r.warnings.is_empty(), "{r:?}");
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            "A.DATA",
            &mut &[1u8; 1000][..],
            1000,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.create_file(
            &root,
            "B.TEXT",
            &mut &[2u8; 500][..],
            500,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let r2 = fsck_ucsd(&mut fs).unwrap();
        assert!(
            r2.is_clean() && r2.warnings.is_empty(),
            "errors={:?} warnings={:?}",
            r2.errors,
            r2.warnings
        );
        assert_eq!(r2.stats.files_checked, 2);
    }

    #[test]
    fn fsck_detects_and_repairs_bad_file_count() {
        let mut cur = Cursor::new(create_blank_ucsd(140 * 1024, "COUNT").unwrap());
        {
            let mut fs = UcsdFilesystem::open(&mut cur, 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "X.DATA",
                &mut &[9u8; 300][..],
                300,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        // Corrupt DNUMFILES (block 2 + 16) to 7; only one real file exists.
        cur.get_mut()[1024 + 16] = 7;

        cur.set_position(0);
        let mut fs = UcsdFilesystem::open(&mut cur, 0).unwrap();
        let r = fsck_ucsd(&mut fs).unwrap();
        assert!(
            r.warnings.iter().any(|w| w.code == "FileCount"),
            "{:?}",
            r.warnings
        );
        let rep = repair_ucsd(&mut fs).unwrap();
        assert!(!rep.fixes_applied.is_empty());

        cur.set_position(0);
        let mut fs2 = UcsdFilesystem::open(&mut cur, 0).unwrap();
        let r2 = fsck_ucsd(&mut fs2).unwrap();
        assert!(r2.is_clean() && r2.warnings.is_empty(), "{r2:?}");
        let root = fs2.root().unwrap();
        assert_eq!(fs2.list_directory(&root).unwrap().len(), 1);
    }

    #[test]
    fn fsck_flags_overlap_unrepairable() {
        let mut img = create_blank_ucsd(140 * 1024, "OVL").unwrap();
        write_u16(&mut img, 1024 + 16, 2, true); // DNUMFILES = 2
        forge_entry(&mut img, 1024 + ENTRY, 6, 10, 5, "A.DATA");
        forge_entry(&mut img, 1024 + 2 * ENTRY, 8, 12, 5, "B.DATA"); // overlaps 6..10
        let mut fs = UcsdFilesystem::open(Cursor::new(img), 0).unwrap();
        let r = fsck_ucsd(&mut fs).unwrap();
        assert!(
            r.errors.iter().any(|e| e.code == "Overlap"),
            "{:?}",
            r.errors
        );
        let rep = repair_ucsd(&mut fs).unwrap();
        assert!(rep.unrepairable_count >= 1, "{rep:?}");
    }

    #[test]
    fn oracle_repair_of_corrupt_count_is_fsck_clean() {
        let Some(script) = oracle() else {
            eprintln!("skipping oracle_repair_of_corrupt_count: oracle unavailable");
            return;
        };
        if Command::new("python3").arg("--version").output().is_err() {
            return;
        }
        let vol = std::env::temp_dir().join(format!("rb_ucsd_rep_{}.vol", std::process::id()));
        std::fs::write(&vol, create_blank_ucsd(140 * 1024, "REPV").unwrap()).unwrap();
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            let mut fs = UcsdFilesystem::open(file, 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "K.DATA",
                &mut &[7u8; 700][..],
                700,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        // Corrupt DNUMFILES on disk, then repair through our code.
        {
            use std::io::{Read as _, Seek as _, Write as _};
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            f.seek(SeekFrom::Start(1024 + 16)).unwrap();
            f.write_all(&9u16.to_le_bytes()).unwrap();
            let _ = f.read(&mut [0u8; 0]);
        }
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            let mut fs = UcsdFilesystem::open(file, 0).unwrap();
            let rep = repair_ucsd(&mut fs).unwrap();
            assert_eq!(rep.unrepairable_count, 0, "{rep:?}");
        }
        let clean = Command::new("python3")
            .arg(&script)
            .args(["fsck", vol.to_str().unwrap()])
            .output()
            .unwrap()
            .status
            .success();
        let _ = std::fs::remove_file(&vol);
        assert!(clean, "oracle fsck flagged the repaired volume");
    }
}
