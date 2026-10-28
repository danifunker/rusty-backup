//! HP LIF (Logical Interchange Format): the boot area of HP 9000 disks, tapes and CDs.
//!
//! An HP-UX disk, a NetBSD/hp300 disk and an HP-UX or NetBSD/hppa install CD all
//! start with a LIF volume: the boot ROM reads its directory to find `ISL`, `HPUX`,
//! `SYS_UBOOT`, `CDBOOT` and friends. Everything is big-endian and counted in
//! 256-byte LIF sectors.
//!
//! Volume header (sector 0): `u16` magic `0x8000`, 6-byte label, `u32` directory
//! start sector, `u16` `0x1000` (the System 3000 id), `u32` directory length in
//! sectors, `u16` version.
//!
//! Directory entry (32 bytes): 10-byte space-padded name, `i16` file type
//! (`0` = purged, `-1` = end of directory), `u32` start sector, `u32` length in
//! sectors, 6-byte BCD date `YYMMDDhhmmss`, `u16` volume flag, `u32`
//! implementation word. Entries may alias the same extent: the HP-UX 11.11 DVD
//! lists `INSTALLFS`, `VINSTALLFS` and `WINSTALLFS` over one range.
//!
//! Header bytes 240-251 hold the PA-RISC IPL pointer (`u32` byte address, size and
//! entry offset) that the boot ROM follows to `ISL`; NetBSD/hp300 leaves it zero.
//!
//! Editing: files are contiguous and placed on 2 KiB boundaries, as HP's `mkboot`
//! does, between the lowest existing file (or 8 KiB past the directory) and the
//! volume end. Space below that floor holds the HP-UX LVM records or a BSD label,
//! so it is never allocated. The volume grows only within the partition row, and
//! the IPL pointer follows its file when that file is replaced or removed.

use std::io::{Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ByteOrder};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

/// LIF counts everything in 256-byte sectors.
pub const LIF_SECTOR: u64 = 256;
/// Volume header magic at byte 0.
const LIF_MAGIC: u16 = 0x8000;
/// Directory sizes seen in the wild are a handful of sectors; anything past this is not LIF.
const MAX_DIR_SECTORS: u32 = 4096;
const ENTRY: usize = 32;
/// File alignment in sectors (2 KiB), which the IPL address needs.
const ALIGN: u64 = 8;
/// Floor for new files on a volume with none: 8 KiB past the directory.
const EMPTY_FLOOR_ALIGN: u64 = 32;
/// HP-UX's BIN type (-23951, what `lifcp -r` writes); NetBSD's boot files use `0xE942`.
const TYPE_BIN: u16 = 0xA271;
const IPL_ADDR: usize = 240;

/// One file in a LIF directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifFile {
    pub name: String,
    /// LIF file type, e.g. `0xCE00` ISL, `0xCD80` HP-UX boot, `0xCFFF` AUTO.
    pub file_type: u16,
    /// Byte offset of the file from the start of the LIF volume.
    pub start_byte: u64,
    pub len_bytes: u64,
    /// `YYYY-MM-DD hh:mm:ss`, or `None` when the BCD date is not valid.
    pub modified: Option<String>,
    /// Directory slot, stable across edits.
    pub slot: usize,
}

/// A parsed LIF volume header and directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifVolume {
    pub label: String,
    pub files: Vec<LifFile>,
}

/// Whether the first sector of `head` is a LIF volume header.
pub fn looks_like_lif(head: &[u8]) -> bool {
    if head.len() < 22 || BigEndian::read_u16(&head[0..2]) != LIF_MAGIC {
        return false;
    }
    let dir_start = BigEndian::read_u32(&head[8..12]);
    let dir_len = BigEndian::read_u32(&head[16..20]);
    dir_start >= 1 && (1..=MAX_DIR_SECTORS).contains(&dir_len)
}

/// Parse the LIF volume at byte `base` of `reader`, or `None` when there is none.
pub fn parse_lif<R: Read + Seek>(reader: &mut R, base: u64) -> Option<LifVolume> {
    let (head, dir) = read_raw(reader, base)?;
    Some(volume_from(&head, &dir))
}

fn read_raw<R: Read + Seek>(reader: &mut R, base: u64) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut head = vec![0u8; LIF_SECTOR as usize];
    reader.seek(SeekFrom::Start(base)).ok()?;
    reader.read_exact(&mut head).ok()?;
    if !looks_like_lif(&head) {
        return None;
    }
    let dir_start = BigEndian::read_u32(&head[8..12]) as u64;
    let dir_len = BigEndian::read_u32(&head[16..20]) as usize;
    let mut dir = vec![0u8; dir_len * LIF_SECTOR as usize];
    reader
        .seek(SeekFrom::Start(base + dir_start * LIF_SECTOR))
        .ok()?;
    reader.read_exact(&mut dir).ok()?;
    Some((head, dir))
}

fn volume_from(head: &[u8], dir: &[u8]) -> LifVolume {
    let label = String::from_utf8_lossy(&head[2..8]).trim_end().to_string();
    let mut files = Vec::new();
    for (slot, entry) in dir.chunks_exact(ENTRY).enumerate() {
        let file_type = BigEndian::read_i16(&entry[10..12]);
        if file_type == -1 {
            break;
        }
        if file_type == 0 {
            continue;
        }
        let name = entry_name(entry);
        if name.is_empty() {
            continue;
        }
        files.push(LifFile {
            name,
            file_type: file_type as u16,
            start_byte: BigEndian::read_u32(&entry[12..16]) as u64 * LIF_SECTOR,
            len_bytes: BigEndian::read_u32(&entry[16..20]) as u64 * LIF_SECTOR,
            modified: bcd_date(&entry[20..26]),
            slot,
        });
    }
    LifVolume { label, files }
}

/// The 10-byte name, padded with spaces by HP and with NULs by NetBSD's `mkboot`.
fn entry_name(entry: &[u8]) -> String {
    String::from_utf8_lossy(&entry[0..10])
        .trim_end_matches([' ', '\0'])
        .to_string()
}

/// Decode LIF's 6-byte BCD `YYMMDDhhmmss`; years below 70 are 20xx.
fn bcd_date(b: &[u8]) -> Option<String> {
    let mut v = [0u32; 6];
    for (i, &byte) in b.iter().take(6).enumerate() {
        let (hi, lo) = ((byte >> 4) as u32, (byte & 0x0F) as u32);
        if hi > 9 || lo > 9 {
            return None;
        }
        v[i] = hi * 10 + lo;
    }
    let [yy, mo, dd, hh, mi, ss] = v;
    if !(1..=12).contains(&mo) || !(1..=31).contains(&dd) || hh > 23 || mi > 59 || ss > 59 {
        return None;
    }
    let year = if yy < 70 { 2000 + yy } else { 1900 + yy };
    Some(format!("{year:04}-{mo:02}-{dd:02} {hh:02}:{mi:02}:{ss:02}"))
}

/// A valid LIF name: 1-10 of `A-Z 0-9 _`, starting with a letter (after upper-casing).
fn lif_name(name: &str) -> Result<String, FilesystemError> {
    let up = name.to_ascii_uppercase();
    let ok = (1..=10).contains(&up.len())
        && up.starts_with(|c: char| c.is_ascii_uppercase())
        && up
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    if ok {
        Ok(up)
    } else {
        Err(FilesystemError::InvalidData(format!(
            "invalid LIF name {name:?} (1-10 of A-Z 0-9 _, starting with a letter)"
        )))
    }
}

/// The type HP's tools give a boot file by its name; everything else is BIN.
fn type_for_name(name: &str) -> u16 {
    match name {
        "ISL" => 0xCE00,
        "HPUX" => 0xCD80,
        "AUTO" => 0xCFFF,
        _ => TYPE_BIN,
    }
}

/// A LIF volume browsed as a flat directory of its files.
pub struct LifFilesystem<R: Read + Seek + Send> {
    reader: R,
    base: u64,
    volume: LifVolume,
    head: Vec<u8>,
    dir: Vec<u8>,
    /// Lowest sector new files may use.
    floor: u64,
    /// Sectors the volume may grow to; equal to its size when it may not grow.
    limit: u64,
    /// The IPL file's name and `(start, len)` sectors at open, when the IPL points at one.
    ipl: Option<(String, u64, u64)>,
    /// `(name, type, implementation word)` of names freed by delete or rename, newest last.
    vacated: Vec<(String, u16, u32)>,
}

impl<R: Read + Seek + Send> LifFilesystem<R> {
    pub fn open(mut reader: R, base: u64) -> Result<Self, FilesystemError> {
        let (head, dir) = read_raw(&mut reader, base)
            .ok_or_else(|| FilesystemError::Parse("not a LIF volume".into()))?;
        let volume = volume_from(&head, &dir);
        let mut fs = Self {
            reader,
            base,
            volume,
            head,
            dir,
            floor: 0,
            limit: 0,
            ipl: None,
            vacated: Vec::new(),
        };
        let dir_end = fs.dir_start() + fs.dir.len() as u64 / LIF_SECTOR;
        fs.floor = fs
            .volume
            .files
            .iter()
            .filter(|f| f.len_bytes > 0)
            .map(|f| f.start_byte / LIF_SECTOR)
            .min()
            .unwrap_or_else(|| dir_end.next_multiple_of(EMPTY_FLOOR_ALIGN))
            .max(dir_end);
        fs.limit = fs.volume_sectors();
        let addr = BigEndian::read_u32(&fs.head[IPL_ADDR..IPL_ADDR + 4]) as u64;
        fs.ipl = (addr != 0)
            .then(|| {
                fs.volume
                    .files
                    .iter()
                    .find(|f| f.start_byte == addr)
                    .map(|f| {
                        (
                            f.name.clone(),
                            f.start_byte / LIF_SECTOR,
                            f.len_bytes / LIF_SECTOR,
                        )
                    })
            })
            .flatten();
        // A cleared pointer that kept its entry offset is waiting for a new ISL.
        if addr == 0 && BigEndian::read_u32(&fs.head[IPL_ADDR + 8..IPL_ADDR + 12]) != 0 {
            fs.ipl = Some(("ISL".to_string(), 0, 0));
        }
        Ok(fs)
    }

    /// The parsed directory.
    pub fn volume(&self) -> &LifVolume {
        &self.volume
    }

    fn dir_start(&self) -> u64 {
        BigEndian::read_u32(&self.head[8..12]) as u64
    }

    /// Whether the size is one field (tracks and surfaces at most 1), so it can grow.
    fn size_is_flat(&self) -> bool {
        BigEndian::read_u32(&self.head[24..28]) <= 1 && BigEndian::read_u32(&self.head[28..32]) <= 1
    }

    /// Declared size in sectors (tracks x surfaces x sectors), never below the last file's end.
    fn volume_sectors(&self) -> u64 {
        let f = |o: usize| (BigEndian::read_u32(&self.head[o..o + 4]) as u64).max(1);
        let declared = f(24) * f(28) * BigEndian::read_u32(&self.head[32..36]) as u64;
        self.files_end().max(declared)
    }

    fn files_end(&self) -> u64 {
        self.volume
            .files
            .iter()
            .map(|f| (f.start_byte + f.len_bytes) / LIF_SECTOR)
            .max()
            .unwrap_or(0)
    }

    fn entry_for(&self, file: &LifFile) -> FileEntry {
        let mut e = FileEntry::new_file(
            file.name.clone(),
            format!("/{}", file.name),
            file.len_bytes,
            file.slot as u64,
        );
        e.modified = file.modified.clone();
        e.special_type = Some(format!("LIF type {:04X}", file.file_type));
        e
    }

    fn file_by_entry(&self, entry: &FileEntry) -> Result<&LifFile, FilesystemError> {
        self.volume
            .files
            .iter()
            .find(|f| f.slot == entry.location as usize && f.name == entry.name)
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))
    }

    /// Used `(start, end)` sector runs, sorted.
    fn used_runs(&self) -> Vec<(u64, u64)> {
        let mut runs: Vec<(u64, u64)> = self
            .volume
            .files
            .iter()
            .filter(|f| f.len_bytes > 0)
            .map(|f| {
                let s = f.start_byte / LIF_SECTOR;
                (s, s + f.len_bytes / LIF_SECTOR)
            })
            .collect();
        runs.sort_unstable();
        runs
    }

    /// Aligned free `(start, end)` gaps between the floor and `limit`.
    fn free_gaps(&self) -> Vec<(u64, u64)> {
        let mut gaps = Vec::new();
        let mut at = self.floor;
        for (s, e) in self
            .used_runs()
            .into_iter()
            .chain([(self.limit, self.limit)])
        {
            let start = at.next_multiple_of(ALIGN);
            let end = s.min(self.limit);
            if end > start {
                gaps.push((start, end));
            }
            at = at.max(e);
        }
        gaps
    }

    /// Re-derive the file list after a directory edit.
    fn reparse(&mut self) {
        self.volume = volume_from(&self.head, &self.dir);
    }

    /// A slot for a new entry: a purged one, or the end marker (moved one slot on).
    fn free_slot(&self) -> Option<usize> {
        for (slot, e) in self.dir.chunks_exact(ENTRY).enumerate() {
            match BigEndian::read_i16(&e[10..12]) {
                0 | -1 => return Some(slot),
                _ => {}
            }
        }
        None
    }
}

impl<R: Read + Seek + Send> Filesystem for LifFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::root())
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        Ok(self
            .volume
            .files
            .iter()
            .map(|f| self.entry_for(f))
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let file = self.file_by_entry(entry)?;
        let (start, len) = (file.start_byte, (file.len_bytes as usize).min(max_bytes));
        let mut out = vec![0u8; len];
        self.reader.seek(SeekFrom::Start(self.base + start))?;
        self.reader.read_exact(&mut out)?;
        Ok(out)
    }

    fn volume_label(&self) -> Option<&str> {
        Some(self.volume.label.as_str()).filter(|l| !l.is_empty())
    }

    fn fs_type(&self) -> &str {
        "LIF (HP boot area)"
    }

    fn case_insensitive_lookup(&self) -> bool {
        true
    }

    fn total_size(&self) -> u64 {
        self.volume_sectors() * LIF_SECTOR
    }

    fn used_size(&self) -> u64 {
        let runs = self.used_runs();
        let mut used = 0;
        let mut at = 0;
        for (s, e) in runs {
            used += e.saturating_sub(s.max(at));
            at = at.max(e);
        }
        (self.dir_start() + self.dir.len() as u64 / LIF_SECTOR + used) * LIF_SECTOR
    }
}

impl<R: Read + Write + Seek + Send> LifFilesystem<R> {
    /// Open for editing; `partition_len` caps how far the volume may grow.
    pub fn open_editable(
        reader: R,
        base: u64,
        partition_len: Option<u64>,
    ) -> Result<Self, FilesystemError> {
        let mut fs = Self::open(reader, base)?;
        if let Some(len) = partition_len.filter(|_| fs.size_is_flat()) {
            fs.limit = fs.limit.max(len / LIF_SECTOR);
        }
        Ok(fs)
    }

    fn write_entry(
        &mut self,
        slot: usize,
        name: &str,
        (ty, impl_word): (u16, u32),
        start: u64,
        len: u64,
        date: [u8; 6],
    ) {
        let was_end = BigEndian::read_i16(&self.dir[slot * ENTRY + 10..slot * ENTRY + 12]) == -1;
        let e = &mut self.dir[slot * ENTRY..(slot + 1) * ENTRY];
        e.fill(0);
        e[0..10].fill(b' ');
        e[0..name.len()].copy_from_slice(name.as_bytes());
        BigEndian::write_u16(&mut e[10..12], ty);
        BigEndian::write_u32(&mut e[12..16], start as u32);
        BigEndian::write_u32(&mut e[16..20], len as u32);
        e[20..26].copy_from_slice(&date);
        BigEndian::write_u16(&mut e[26..28], 0x8001);
        BigEndian::write_u32(&mut e[28..32], impl_word);
        let next = (slot + 1) * ENTRY;
        if was_end && next + ENTRY <= self.dir.len() {
            self.dir[next..next + ENTRY].fill(0);
            BigEndian::write_i16(&mut self.dir[next + 10..next + 12], -1);
        }
    }

    /// Remember the type and implementation word of the name at `slot` before it goes.
    fn vacate(&mut self, slot: usize) {
        let e = &self.dir[slot * ENTRY..(slot + 1) * ENTRY];
        let kind = (
            BigEndian::read_u16(&e[10..12]),
            BigEndian::read_u32(&e[28..32]),
        );
        self.vacated.push((entry_name(e), kind.0, kind.1));
    }

    /// Type and implementation word for a new `name`: what it last had, else its boot type.
    fn kind_for(&self, name: &str) -> (u16, u32) {
        self.vacated
            .iter()
            .rev()
            .find(|v| v.0 == name)
            .map_or((type_for_name(name), 0), |v| (v.1, v.2))
    }

    /// Point the IPL at its file again, or clear it when that file is gone.
    fn fix_ipl(&mut self) {
        let Some((name, start, len)) = self.ipl.clone() else {
            return;
        };
        let now = self
            .volume
            .files
            .iter()
            .find(|f| f.name == name)
            .map(|f| (f.start_byte / LIF_SECTOR, f.len_bytes / LIF_SECTOR));
        if now == Some((start, len)) {
            return;
        }
        let (addr, size) = now.map_or((0, 0), |(s, l)| {
            (s * LIF_SECTOR, (l * LIF_SECTOR).next_multiple_of(2048))
        });
        BigEndian::write_u32(&mut self.head[IPL_ADDR..IPL_ADDR + 4], addr as u32);
        BigEndian::write_u32(&mut self.head[IPL_ADDR + 4..IPL_ADDR + 8], size as u32);
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for LifFilesystem<R> {
    fn as_filesystem(&self) -> &dyn Filesystem {
        self
    }

    fn as_filesystem_mut(&mut self) -> &mut dyn Filesystem {
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
        if parent.path != "/" {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let name = lif_name(name)?;
        if self.volume.files.iter().any(|f| f.name == name) {
            return Err(FilesystemError::AlreadyExists(name));
        }
        let slot = self.free_slot().ok_or_else(|| {
            FilesystemError::DiskFull(format!(
                "LIF directory full ({} entries)",
                self.dir.len() / ENTRY
            ))
        })?;
        let need = data_len.div_ceil(LIF_SECTOR);
        if need > u32::MAX as u64 {
            return Err(FilesystemError::DiskFull("file too large for LIF".into()));
        }
        let start = self
            .free_gaps()
            .into_iter()
            .find(|(s, e)| e - s >= need)
            .map(|(s, _)| s)
            .ok_or_else(|| {
                FilesystemError::DiskFull(format!(
                    "no contiguous free run of {need} LIF sectors (LIF files are contiguous)"
                ))
            })?;
        self.reader
            .seek(SeekFrom::Start(self.base + start * LIF_SECTOR))?;
        let mut left = data_len;
        let mut buf = vec![0u8; 64 * 1024];
        while left > 0 {
            let n = left.min(buf.len() as u64) as usize;
            data.read_exact(&mut buf[..n])?;
            self.reader.write_all(&buf[..n])?;
            left -= n as u64;
        }
        let pad = (need * LIF_SECTOR - data_len) as usize;
        self.reader.write_all(&vec![0u8; pad])?;
        let secs = crate::fs::times::resolve_or_now(options.unix_times).mtime_or_now();
        let date = crate::fs::times::unix_to_lif_bcd(secs);
        let kind = self.kind_for(&name);
        self.write_entry(slot, &name, kind, start, need, date);
        if self.size_is_flat() && start + need > self.volume_sectors() {
            BigEndian::write_u32(&mut self.head[32..36], (start + need) as u32);
        }
        self.reparse();
        let file = self.volume.files.iter().find(|f| f.slot == slot).cloned();
        file.map(|f| self.entry_for(&f))
            .ok_or(FilesystemError::NotFound(name))
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "LIF is a flat filesystem: no subdirectories".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        _parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let slot = self.file_by_entry(entry)?.slot;
        self.vacate(slot);
        // Purged entries keep their bytes, as HP's `lifrm` leaves them.
        BigEndian::write_u16(&mut self.dir[slot * ENTRY + 10..slot * ENTRY + 12], 0);
        self.reparse();
        Ok(())
    }

    fn rename(
        &mut self,
        _parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        let slot = self.file_by_entry(entry)?.slot;
        let name = lif_name(new_name)?;
        if self
            .volume
            .files
            .iter()
            .any(|f| f.name == name && f.slot != slot)
        {
            return Err(FilesystemError::AlreadyExists(name));
        }
        self.vacate(slot);
        let at = slot * ENTRY;
        let fresh = BigEndian::read_u16(&self.dir[at + 10..at + 12]) == TYPE_BIN
            && BigEndian::read_u32(&self.dir[at + 28..at + 32]) == 0;
        if fresh {
            let (ty, impl_word) = self.kind_for(&name);
            BigEndian::write_u16(&mut self.dir[at + 10..at + 12], ty);
            BigEndian::write_u32(&mut self.dir[at + 28..at + 32], impl_word);
        }
        let e = &mut self.dir[at..at + 10];
        e.fill(b' ');
        e[..name.len()].copy_from_slice(name.as_bytes());
        self.reparse();
        Ok(())
    }

    fn set_volume_name(&mut self, new_name: &str) -> Result<(), FilesystemError> {
        let up = new_name.to_ascii_uppercase();
        if up.len() > 6
            || !up
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(FilesystemError::InvalidData(format!(
                "invalid LIF volume label {new_name:?} (up to 6 of A-Z 0-9 _)"
            )));
        }
        self.head[2..8].fill(b' ');
        self.head[2..2 + up.len()].copy_from_slice(up.as_bytes());
        self.reparse();
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.fix_ipl();
        let dir_at = self.base + self.dir_start() * LIF_SECTOR;
        self.reader.seek(SeekFrom::Start(dir_at))?;
        self.reader.write_all(&self.dir)?;
        self.reader.seek(SeekFrom::Start(self.base))?;
        self.reader.write_all(&self.head)?;
        self.reader.flush()?;
        if let Some(ipl) = self.ipl.as_mut() {
            if let Some(f) = self.volume.files.iter().find(|f| f.name == ipl.0) {
                (ipl.1, ipl.2) = (f.start_byte / LIF_SECTOR, f.len_bytes / LIF_SECTOR);
            }
        }
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_gaps().iter().map(|(s, e)| e - s).sum::<u64>() * LIF_SECTOR)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A LIF volume "BOOT43" with ISL (2 sectors of 0xAA) and a purged entry, NetBSD/hp300 style.
    fn sample_lif() -> Vec<u8> {
        let mut img = vec![0u8; 16 * 256];
        BigEndian::write_u16(&mut img[0..2], LIF_MAGIC);
        img[2..8].copy_from_slice(b"BOOT43");
        BigEndian::write_u32(&mut img[8..12], 2);
        BigEndian::write_u16(&mut img[12..14], 0x1000);
        BigEndian::write_u32(&mut img[16..20], 1);
        BigEndian::write_u16(&mut img[20..22], 1);
        let dir = 2 * 256;
        let mut put = |slot: usize, name: &[u8], ty: i16, start: u32, len: u32, date: [u8; 6]| {
            let e = &mut img[dir + slot * 32..dir + slot * 32 + 32];
            e[0..10].fill(b' ');
            e[0..name.len()].copy_from_slice(name);
            BigEndian::write_i16(&mut e[10..12], ty);
            BigEndian::write_u32(&mut e[12..16], start);
            BigEndian::write_u32(&mut e[16..20], len);
            e[20..26].copy_from_slice(&date);
        };
        put(0, b"OLD", 0, 0, 0, [0; 6]);
        put(
            1,
            b"ISL",
            0xCE00u16 as i16,
            4,
            2,
            [0x04, 0x11, 0x04, 0x14, 0x27, 0x53],
        );
        put(2, b"BAD", -1, 0, 0, [0; 6]);
        img[4 * 256..6 * 256].fill(0xAA);
        img
    }

    #[test]
    fn parses_directory_and_skips_purged_entries() {
        let vol = parse_lif(&mut Cursor::new(sample_lif()), 0).unwrap();
        assert_eq!(vol.label, "BOOT43");
        assert_eq!(
            vol.files.len(),
            1,
            "purged entry skipped, end marker stops the walk"
        );
        let isl = &vol.files[0];
        assert_eq!((isl.name.as_str(), isl.file_type), ("ISL", 0xCE00));
        assert_eq!((isl.start_byte, isl.len_bytes), (1024, 512));
        assert_eq!(isl.modified.as_deref(), Some("2004-11-04 14:27:53"));
    }

    #[test]
    fn filesystem_lists_and_reads_files() {
        let mut fs = LifFilesystem::open(Cursor::new(sample_lif()), 0).unwrap();
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        assert_eq!(files.len(), 1);
        let data = fs.read_file(&files[0], usize::MAX).unwrap();
        assert_eq!(data, vec![0xAA; 512]);
        assert_eq!(fs.volume_label(), Some("BOOT43"));
    }

    #[test]
    fn rejects_non_lif_and_bad_dates() {
        assert!(!looks_like_lif(&[0u8; 256]));
        let mut iso_like = sample_lif();
        BigEndian::write_u32(&mut iso_like[16..20], 0);
        assert!(!looks_like_lif(&iso_like));
        assert_eq!(bcd_date(&[0x08, 0x17, 0xC1, 0x15, 0x13, 0x05]), None);
    }

    /// An HP-UX-style volume: 64-slot directory at sector 16, ISL at 32 (IPL points at it), 128 sectors.
    fn hpux_lif() -> Vec<u8> {
        let mut img = vec![0u8; 256 * 256];
        BigEndian::write_u16(&mut img[0..2], LIF_MAGIC);
        img[2..8].copy_from_slice(b"ISL10 ");
        BigEndian::write_u32(&mut img[8..12], 16);
        BigEndian::write_u16(&mut img[12..14], 0x1000);
        BigEndian::write_u32(&mut img[16..20], 8);
        BigEndian::write_u16(&mut img[20..22], 1);
        BigEndian::write_u32(&mut img[24..28], 1);
        BigEndian::write_u32(&mut img[28..32], 1);
        BigEndian::write_u32(&mut img[32..36], 128);
        BigEndian::write_u32(&mut img[240..244], 32 * 256);
        BigEndian::write_u32(&mut img[244..248], 2048);
        BigEndian::write_u32(&mut img[248..252], 0x100);
        // Something below the files that must survive, like an LVM record.
        img[4 * 256..4 * 256 + 8].copy_from_slice(b"LVMREC01");
        let d = 16 * 256;
        img[d..d + 10].copy_from_slice(b"ISL       ");
        BigEndian::write_u16(&mut img[d + 10..d + 12], 0xCE00);
        BigEndian::write_u32(&mut img[d + 12..d + 16], 32);
        BigEndian::write_u32(&mut img[d + 16..d + 20], 4);
        BigEndian::write_u16(&mut img[d + 26..d + 28], 0x8001);
        BigEndian::write_i16(&mut img[d + 42..d + 44], -1);
        img[32 * 256..36 * 256].fill(0x11);
        img
    }

    fn put(fs: &mut LifFilesystem<Cursor<Vec<u8>>>, name: &str, data: &[u8]) -> FileEntry {
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            name,
            &mut &data[..],
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap()
    }

    #[test]
    fn create_delete_rename_round_trip() {
        let mut fs = LifFilesystem::open_editable(Cursor::new(hpux_lif()), 0, None).unwrap();
        let free = fs.free_space().unwrap();
        let e = put(&mut fs, "auto", b"hpux");
        assert_eq!(e.name, "AUTO");
        assert_eq!(fs.free_space().unwrap(), free - 2048, "one 2 KiB slot");
        let root = fs.root().unwrap();
        fs.rename(&root, &e, "boot_1").unwrap();
        fs.sync_metadata().unwrap();
        let img = fs.reader.into_inner();
        assert_eq!(
            &img[4 * 256..4 * 256 + 8],
            b"LVMREC01",
            "below the floor is untouched"
        );

        let mut fs = LifFilesystem::open_editable(Cursor::new(img), 0, None).unwrap();
        let files = fs.list_directory(&FileEntry::root()).unwrap();
        let names: Vec<_> = files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["ISL", "BOOT_1"]);
        let boot = &files[1];
        assert_eq!(boot.location, 1);
        assert_eq!(
            fs.volume.files[1].start_byte,
            40 * 256,
            "first 2 KiB boundary after ISL"
        );
        assert_eq!(
            fs.volume.files[1].file_type, 0xCFFF,
            "typed as AUTO when created"
        );
        let data = fs.read_file(boot, usize::MAX).unwrap();
        assert_eq!(&data[..4], b"hpux");
        assert!(data[4..].iter().all(|&b| b == 0));
        fs.delete_entry(&FileEntry::root(), boot).unwrap();
        assert_eq!(fs.free_space().unwrap(), free);
        assert!(matches!(
            put_err(&mut fs, "1BAD"),
            FilesystemError::InvalidData(_)
        ));
        assert!(matches!(
            put_err(&mut fs, "ISL"),
            FilesystemError::AlreadyExists(_)
        ));
    }

    fn put_err(fs: &mut LifFilesystem<Cursor<Vec<u8>>>, name: &str) -> FilesystemError {
        let root = fs.root().unwrap();
        fs.create_file(&root, name, &mut &b""[..], 0, &CreateFileOptions::default())
            .unwrap_err()
    }

    #[test]
    fn ipl_follows_a_replaced_isl() {
        let mut fs = LifFilesystem::open_editable(Cursor::new(hpux_lif()), 0, None).unwrap();
        let isl = fs.list_directory(&FileEntry::root()).unwrap().remove(0);
        fs.delete_entry(&FileEntry::root(), &isl).unwrap();
        put(&mut fs, "PAD", &[0u8; 256]);
        let new = put(&mut fs, "ISL", &[0x22; 3000]);
        fs.sync_metadata().unwrap();
        let start = fs
            .volume
            .files
            .iter()
            .find(|f| f.slot == new.location as usize)
            .unwrap()
            .start_byte;
        let img = fs.reader.into_inner();
        assert_eq!(start, 40 * 256, "the old ISL run is reused by PAD");
        assert_eq!(BigEndian::read_u32(&img[240..244]) as u64, start);
        assert_eq!(
            BigEndian::read_u32(&img[244..248]),
            4096,
            "size rounds up to 2 KiB"
        );
        assert_eq!(
            BigEndian::read_u32(&img[248..252]),
            0x100,
            "entry offset kept"
        );
        assert_eq!(
            BigEndian::read_u16(&img[16 * 256 + 32 + 10..]),
            0xCE00,
            "ISL typed as ISL"
        );
    }

    #[test]
    fn grows_only_within_the_partition() {
        let big = vec![0x33u8; 100 * 256];
        let mut fs = LifFilesystem::open_editable(Cursor::new(hpux_lif()), 0, None).unwrap();
        let root = fs.root().unwrap();
        let err = fs
            .create_file(
                &root,
                "BIG",
                &mut &big[..],
                big.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap_err();
        assert!(matches!(err, FilesystemError::DiskFull(_)));

        let mut fs =
            LifFilesystem::open_editable(Cursor::new(hpux_lif()), 0, Some(200 * 256)).unwrap();
        put(&mut fs, "BIG", &big);
        fs.sync_metadata().unwrap();
        let img = fs.reader.into_inner();
        assert_eq!(
            BigEndian::read_u32(&img[32..36]),
            140,
            "size field covers the new file"
        );
    }

    #[test]
    fn netbsd_nul_padded_names_and_empty_floor() {
        let mut img = sample_lif();
        img[2 * 256 + 32..2 * 256 + 42].copy_from_slice(b"SYS_UBOOT\0");
        let vol = parse_lif(&mut Cursor::new(img.clone()), 0).unwrap();
        assert_eq!(vol.files[0].name, "SYS_UBOOT");
        let fs = LifFilesystem::open(Cursor::new(img), 0).unwrap();
        assert_eq!(fs.floor, 4, "lowest file start");
        let mut empty = sample_lif();
        BigEndian::write_i16(&mut empty[2 * 256 + 42..2 * 256 + 44], 0);
        let fs = LifFilesystem::open(Cursor::new(empty), 0).unwrap();
        assert_eq!(fs.floor, 32, "8 KiB past the directory with no files");
    }

    fn replace(fs: &mut LifFilesystem<Cursor<Vec<u8>>>, name: &str, data: &[u8]) {
        use crate::fs::replace::{create_or_replace, OnConflict, ReplacePolicy};
        let root = fs.root().unwrap();
        let policy = ReplacePolicy {
            on_conflict: OnConflict::Replace,
            ..Default::default()
        };
        let opts = CreateFileOptions::default();
        create_or_replace(
            fs,
            &root,
            name,
            &mut &data[..],
            data.len() as u64,
            &opts,
            policy,
        )
        .unwrap();
        fs.sync_metadata().unwrap();
    }

    #[test]
    fn staged_replace_keeps_type_impl_word_and_ipl() {
        let mut img = hpux_lif();
        BigEndian::write_u32(&mut img[16 * 256 + 28..], 0xFFF0_0000);
        let mut fs = LifFilesystem::open_editable(Cursor::new(img), 0, None).unwrap();
        put(&mut fs, "LABEL", b"x");
        let label = fs
            .volume
            .files
            .iter()
            .find(|f| f.name == "LABEL")
            .unwrap()
            .slot;
        BigEndian::write_u16(&mut fs.dir[label * ENTRY + 10..], 0xA271);
        fs.reparse();
        replace(&mut fs, "label", b"y");
        replace(&mut fs, "isl", &[0x44; 5000]);
        let names: Vec<_> = fs.volume.files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["ISL", "LABEL"], "no temporaries left behind");
        let get = |n: &str| {
            fs.volume
                .files
                .iter()
                .find(|f| f.name == n)
                .unwrap()
                .clone()
        };
        assert_eq!(get("LABEL").file_type, 0xA271);
        let isl = get("ISL");
        assert_eq!(isl.file_type, 0xCE00);
        let at = isl.slot * ENTRY;
        assert_eq!(BigEndian::read_u32(&fs.dir[at + 28..]), 0xFFF0_0000);
        assert_eq!(BigEndian::read_u32(&fs.head[240..]) as u64, isl.start_byte);
        assert_eq!(BigEndian::read_u32(&fs.head[244..]), 6144);
    }

    #[test]
    fn a_cleared_ipl_reattaches_to_the_next_isl() {
        let mut fs = LifFilesystem::open_editable(Cursor::new(hpux_lif()), 0, None).unwrap();
        let isl = fs.list_directory(&FileEntry::root()).unwrap().remove(0);
        fs.delete_entry(&FileEntry::root(), &isl).unwrap();
        fs.sync_metadata().unwrap();
        assert_eq!(
            BigEndian::read_u32(&fs.head[240..]),
            0,
            "cleared with its file gone"
        );
        let img = fs.reader.into_inner();
        let mut fs = LifFilesystem::open_editable(Cursor::new(img), 0, None).unwrap();
        let new = put(&mut fs, "ISL", &[1; 300]);
        fs.sync_metadata().unwrap();
        let start = fs
            .volume
            .files
            .iter()
            .find(|f| f.slot == new.location as usize)
            .unwrap()
            .start_byte;
        assert_eq!(BigEndian::read_u32(&fs.head[240..]) as u64, start);
        assert_eq!(BigEndian::read_u32(&fs.head[248..]), 0x100);
    }
}
