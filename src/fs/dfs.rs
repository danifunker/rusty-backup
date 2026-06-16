//! Acorn DFS (Disc Filing System) — the flat floppy filesystem for the BBC
//! Micro, BBC Master and Acorn Electron (and the MiSTer `BBCMicro` /
//! `AcornElectron` cores).
//!
//! The host OS can't mount these disks, so file extract + add + delete is the
//! whole point (Axis 1 of the MiSTer plan — floppy-only, no resize).
//!
//! ## Geometry
//!
//! DFS disks are flat sector images in logical sector order (`.ssd` = single
//! sided; `.dsd` track-interleaving is a follow-up). Sectors are 256 bytes,
//! ten to a track, numbered linearly from 0 (`LSN = track*10 + sector`), so a
//! sector's byte offset is simply `lsn * 256`. Two single-sided geometries:
//!
//! | Body bytes | Sectors | Tracks |
//! |------------|---------|--------|
//! | 102400     | 400     | 40     |
//! | 204800     | 800     | 80     |
//!
//! ## On-disk layout (Acorn DFS catalogue, sectors 0 and 1 of track 0)
//!
//! The catalogue is two 256-byte sectors. Up to **31 files**; each file has an
//! 8-byte half-entry in *each* of the two sectors at the same offset
//! `8 + i*8`. Files are physically **contiguous** (no fragmentation) and the
//! catalogue lists them in **descending order of start sector**.
//!
//! ```text
//! Sector 0:
//!   0x00..0x08  disc title, chars 0..7 (12-char title, NUL/space padded)
//!   0x08+i*8    file i: 7-byte name
//!   0x0F+i*8    file i: dir char in bits 0..6, lock in bit 7
//!
//! Sector 1:
//!   0x00..0x04  disc title, chars 8..11
//!   0x04        cycle number (BCD), bumped on each catalogue rewrite
//!   0x05        number of files * 8
//!   0x06        bits 5..4 = *OPT boot option; bits 1..0 = total-sectors hi
//!   0x07        total sectors, low 8 bits
//!   0x08+i*8    file i: load addr lo/mid (2), exec addr lo/mid (2),
//!               length lo/mid (2), packed-hi byte, start-sector lo
//!   packed-hi (offset +6 within the entry):
//!     bits 0..1 = start-sector bits 9..8
//!     bits 2..3 = load   bits 17..16
//!     bits 4..5 = length bits 17..16
//!     bits 6..7 = exec   bits 17..16
//! ```
//!
//! Load / exec / length are 18-bit; start sector is 10-bit. A file occupies a
//! single contiguous run of `ceil(length / 256)` sectors starting at its start
//! sector; there is no free-space bitmap — free space is whatever no live file
//! claims (sectors 0 and 1 are always the catalogue).

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

pub const SECTOR_SIZE: usize = 256;
pub const MAX_FILES: usize = 31;
/// Default DFS directory character (the "current" directory).
pub const DEFAULT_DIR: u8 = b'$';

const LOCK_BIT: u8 = 0x80;

/// Disk geometry, derived from the flat-body byte length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DfsGeometry {
    pub total_sectors: u16,
}

impl DfsGeometry {
    /// Recognize a flat single-sided DFS body by exact size.
    pub fn from_body_len(len: u64) -> Option<DfsGeometry> {
        match len {
            102_400 => Some(DfsGeometry { total_sectors: 400 }),
            204_800 => Some(DfsGeometry { total_sectors: 800 }),
            _ => None,
        }
    }
}

/// One parsed catalogue entry.
#[derive(Debug, Clone)]
pub struct DfsEntry {
    /// Single-character directory namespace (e.g. `$`).
    pub dir: u8,
    /// File name (1..7 chars, trailing spaces stripped).
    pub name: String,
    pub locked: bool,
    pub load_addr: u32,
    pub exec_addr: u32,
    pub length: u32,
    pub start_sector: u16,
}

impl DfsEntry {
    /// Sectors physically occupied by this file's contiguous run.
    fn sectors(&self) -> u16 {
        ((self.length as usize).div_ceil(SECTOR_SIZE)) as u16
    }

    /// Display name: bare for the `$` directory, `dir.name` otherwise — the
    /// BBC convention. This is what `ls` shows and `get` matches against.
    pub fn display_name(&self) -> String {
        if self.dir == DEFAULT_DIR {
            self.name.clone()
        } else {
            format!("{}.{}", self.dir as char, self.name)
        }
    }

    /// Case-insensitive identity (DFS matches names case-insensitively).
    fn same_file(&self, dir: u8, name: &str) -> bool {
        self.dir.eq_ignore_ascii_case(&dir) && self.name.eq_ignore_ascii_case(name)
    }
}

/// Split a requested name into `(dir, name)`. A leading `X.` (single char +
/// dot) selects directory `X`; otherwise the default `$` directory is used.
fn split_dir_name(input: &str) -> Result<(u8, String), FilesystemError> {
    let (dir, name) = match input.as_bytes() {
        [d, b'.', ..] => (*d, &input[2..]),
        _ => (DEFAULT_DIR, input),
    };
    if !(0x21..=0x7e).contains(&dir) || dir == b'.' {
        return Err(FilesystemError::InvalidData(format!(
            "DFS directory '{}' must be a single printable character",
            dir as char
        )));
    }
    if name.is_empty() || name.len() > 7 {
        return Err(FilesystemError::InvalidData(format!(
            "DFS filename '{name}' must be 1-7 characters"
        )));
    }
    for c in name.bytes() {
        // Reject control chars, space, and the DFS metacharacters.
        if !(0x21..=0x7e).contains(&c) || matches!(c, b'.' | b':' | b'*' | b'#' | b'"') {
            return Err(FilesystemError::InvalidData(format!(
                "DFS filename '{name}' contains an invalid character"
            )));
        }
    }
    Ok((dir, name.to_string()))
}

/// Decode the 12-byte raw disc title (NUL/space padded) into a trimmed string.
fn decode_title(raw: &[u8; 12]) -> String {
    let end = raw
        .iter()
        .rposition(|&b| b != 0x00 && b != b' ')
        .map_or(0, |p| p + 1);
    String::from_utf8_lossy(&raw[..end]).trim().to_string()
}

/// Parse the two catalogue sectors into entries + metadata.
struct Catalogue {
    title_raw: [u8; 12],
    title: String,
    boot_option: u8,
    cycle: u8,
    declared_total: u16,
    entries: Vec<DfsEntry>,
}

impl Catalogue {
    fn parse(s0: &[u8; SECTOR_SIZE], s1: &[u8; SECTOR_SIZE]) -> Catalogue {
        let mut title_raw = [0u8; 12];
        title_raw[..8].copy_from_slice(&s0[0..8]);
        title_raw[8..].copy_from_slice(&s1[0..4]);
        let num_files = (s1[5] / 8) as usize;
        let declared_total = (((s1[6] & 0x03) as u16) << 8) | s1[7] as u16;
        let mut entries = Vec::with_capacity(num_files);
        for i in 0..num_files {
            let no = 8 + i * 8; // name half-entry in sector 0
            let io = 8 + i * 8; // info half-entry in sector 1
            let name = String::from_utf8_lossy(&s0[no..no + 7])
                .trim_end()
                .to_string();
            let dir_byte = s0[no + 7];
            let hi = s1[io + 6];
            let load =
                u16::from_le_bytes([s1[io], s1[io + 1]]) as u32 | (((hi >> 2) & 0x03) as u32) << 16;
            let exec = u16::from_le_bytes([s1[io + 2], s1[io + 3]]) as u32
                | (((hi >> 6) & 0x03) as u32) << 16;
            let length = u16::from_le_bytes([s1[io + 4], s1[io + 5]]) as u32
                | (((hi >> 4) & 0x03) as u32) << 16;
            let start_sector = s1[io + 7] as u16 | (((hi & 0x03) as u16) << 8);
            entries.push(DfsEntry {
                dir: dir_byte & 0x7f,
                name,
                locked: dir_byte & LOCK_BIT != 0,
                load_addr: load,
                exec_addr: exec,
                length,
                start_sector,
            });
        }
        Catalogue {
            title: decode_title(&title_raw),
            title_raw,
            boot_option: (s1[6] >> 4) & 0x03,
            cycle: s1[4],
            declared_total,
            entries,
        }
    }

    /// Serialize back into the two catalogue sectors. Entries are emitted in
    /// descending start-sector order (the DFS convention).
    fn serialize(&self, total_sectors: u16) -> (Vec<u8>, Vec<u8>) {
        let mut s0 = vec![0u8; SECTOR_SIZE];
        let mut s1 = vec![0u8; SECTOR_SIZE];
        s0[0..8].copy_from_slice(&self.title_raw[..8]);
        s1[0..4].copy_from_slice(&self.title_raw[8..]);
        s1[4] = self.cycle;
        s1[5] = (self.entries.len() as u8) * 8;
        s1[6] = (self.boot_option << 4) | ((total_sectors >> 8) & 0x03) as u8;
        s1[7] = (total_sectors & 0xff) as u8;

        let mut ordered = self.entries.clone();
        ordered.sort_by_key(|e| std::cmp::Reverse(e.start_sector));
        for (i, e) in ordered.iter().enumerate() {
            let no = 8 + i * 8;
            let io = 8 + i * 8;
            let mut nbuf = [b' '; 7];
            let nb = e.name.as_bytes();
            nbuf[..nb.len().min(7)].copy_from_slice(&nb[..nb.len().min(7)]);
            s0[no..no + 7].copy_from_slice(&nbuf);
            s0[no + 7] = (e.dir & 0x7f) | if e.locked { LOCK_BIT } else { 0 };
            let load = e.load_addr;
            let exec = e.exec_addr;
            let len = e.length;
            let start = e.start_sector;
            s1[io] = (load & 0xff) as u8;
            s1[io + 1] = ((load >> 8) & 0xff) as u8;
            s1[io + 2] = (exec & 0xff) as u8;
            s1[io + 3] = ((exec >> 8) & 0xff) as u8;
            s1[io + 4] = (len & 0xff) as u8;
            s1[io + 5] = ((len >> 8) & 0xff) as u8;
            s1[io + 6] = (((start >> 8) & 0x03) as u8)
                | ((((load >> 16) & 0x03) as u8) << 2)
                | ((((len >> 16) & 0x03) as u8) << 4)
                | ((((exec >> 16) & 0x03) as u8) << 6);
            s1[io + 7] = (start & 0xff) as u8;
        }
        (s0, s1)
    }
}

/// Sniff whether the flat body at `partition_offset` is an Acorn DFS volume:
/// exact single-sided geometry AND a structurally consistent catalogue whose
/// declared sector count matches the disk size (this is what separates a true
/// single-sided `.ssd` from a track-interleaved `.dsd`, whose side-0 catalogue
/// declares half the sectors).
pub fn looks_like_dfs<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<DfsGeometry> {
    let len = reader
        .seek(SeekFrom::End(0))
        .ok()?
        .checked_sub(partition_offset)?;
    let geom = DfsGeometry::from_body_len(len)?;
    reader.seek(SeekFrom::Start(partition_offset)).ok()?;
    let mut s0 = [0u8; SECTOR_SIZE];
    let mut s1 = [0u8; SECTOR_SIZE];
    reader.read_exact(&mut s0).ok()?;
    reader.read_exact(&mut s1).ok()?;

    let num_files_byte = s1[5];
    if num_files_byte % 8 != 0 || (num_files_byte / 8) as usize > MAX_FILES {
        return None;
    }
    let cat = Catalogue::parse(&s0, &s1);
    // The declared sector count MUST equal the disk's real geometry. This is
    // the discriminator that rejects a flat 40-track double-sided `.dsd`
    // (side-0 catalogue declares 400 on a 204800-byte / 800-sector file).
    if cat.declared_total != geom.total_sectors {
        return None;
    }
    if cat.boot_option > 3 {
        return None;
    }
    for e in &cat.entries {
        if e.name.is_empty() {
            return None;
        }
        for c in e.name.bytes() {
            if !(0x20..=0x7e).contains(&c) {
                return None;
            }
        }
        // Files live after the catalogue and within the disk.
        if e.start_sector < 2 || e.start_sector >= geom.total_sectors {
            return None;
        }
        if e.start_sector as u32 + e.sectors() as u32 > geom.total_sectors as u32 {
            return None;
        }
    }
    Some(geom)
}

/// An Acorn DFS filesystem over a flat single-sided sector body.
pub struct DfsFilesystem<R: Read + Seek + Send> {
    reader: R,
    partition_offset: u64,
    geom: DfsGeometry,
    cat: Catalogue,
}

impl<R: Read + Seek + Send> DfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let len = reader
            .seek(SeekFrom::End(0))?
            .checked_sub(partition_offset)
            .ok_or_else(|| FilesystemError::InvalidData("partition offset past EOF".into()))?;
        let geom = DfsGeometry::from_body_len(len).ok_or_else(|| {
            FilesystemError::InvalidData(format!("{len} bytes is not a known DFS geometry"))
        })?;
        let (s0, s1) = read_two_sectors(&mut reader, partition_offset)?;
        let cat = Catalogue::parse(&s0, &s1);
        if (cat.entries.len()) > MAX_FILES {
            return Err(FilesystemError::InvalidData(
                "DFS catalogue claims more than 31 files".into(),
            ));
        }
        Ok(DfsFilesystem {
            reader,
            partition_offset,
            geom,
            cat,
        })
    }

    pub fn into_inner(self) -> R {
        self.reader
    }

    fn read_sectors(&mut self, start: u16, count: usize) -> Result<Vec<u8>, FilesystemError> {
        let end = start as u32 + count as u32;
        if end > self.geom.total_sectors as u32 {
            return Err(FilesystemError::InvalidData(format!(
                "sector run {start}..{end} out of range 0..{}",
                self.geom.total_sectors
            )));
        }
        let off = self.partition_offset + start as u64 * SECTOR_SIZE as u64;
        self.reader.seek(SeekFrom::Start(off))?;
        let mut buf = vec![0u8; count * SECTOR_SIZE];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn find_entry(&self, display_name: &str) -> Option<usize> {
        let (dir, name) = match split_dir_name(display_name) {
            Ok(v) => v,
            Err(_) => return None,
        };
        self.cat
            .entries
            .iter()
            .position(|e| e.same_file(dir, &name))
    }
}

/// Read sectors 0 and 1 (the catalogue) from `partition_offset`.
fn read_two_sectors<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Result<([u8; SECTOR_SIZE], [u8; SECTOR_SIZE]), FilesystemError> {
    reader.seek(SeekFrom::Start(partition_offset))?;
    let mut s0 = [0u8; SECTOR_SIZE];
    let mut s1 = [0u8; SECTOR_SIZE];
    reader.read_exact(&mut s0)?;
    reader.read_exact(&mut s1)?;
    Ok((s0, s1))
}

impl<R: Read + Seek + Send> Filesystem for DfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if entry.path != "/" {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for (idx, e) in self.cat.entries.iter().enumerate() {
            let disp = e.display_name();
            let mut fe = FileEntry::new_file(
                disp.clone(),
                format!("/{disp}"),
                e.length as u64,
                idx as u64,
            );
            if e.locked {
                fe.special_type = Some("locked".into());
            }
            out.push(fe);
        }
        Ok(out)
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        let idx = entry.location as usize;
        let de = self
            .cat
            .entries
            .get(idx)
            .ok_or_else(|| FilesystemError::NotFound(format!("entry idx {idx} out of range")))?
            .clone();
        let count = (de.length as usize).div_ceil(SECTOR_SIZE).max(1);
        let raw = self.read_sectors(de.start_sector, count)?;
        let mut data = raw;
        data.truncate(de.length as usize);
        data.truncate(max_bytes);
        Ok(data)
    }

    fn fs_type(&self) -> &str {
        "Acorn DFS"
    }

    fn volume_label(&self) -> Option<&str> {
        if self.cat.title.is_empty() {
            None
        } else {
            Some(&self.cat.title)
        }
    }

    fn total_size(&self) -> u64 {
        self.geom.total_sectors as u64 * SECTOR_SIZE as u64
    }

    fn used_size(&self) -> u64 {
        // Catalogue (2 sectors) plus the contiguous extent of every file.
        let file_sectors: u64 = self.cat.entries.iter().map(|e| e.sectors() as u64).sum();
        (file_sectors + 2) * SECTOR_SIZE as u64
    }
}

// ---------------------------------------------------------------------------
// Write side
// ---------------------------------------------------------------------------

impl<R: Read + Write + Seek + Send> DfsFilesystem<R> {
    fn write_at(&mut self, sector: u16, data: &[u8]) -> Result<(), FilesystemError> {
        let off = self.partition_offset + sector as u64 * SECTOR_SIZE as u64;
        self.reader.seek(SeekFrom::Start(off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    /// Re-serialize the in-memory catalogue and flush both sectors to disk.
    fn flush_catalogue(&mut self) -> Result<(), FilesystemError> {
        let (s0, s1) = self.cat.serialize(self.geom.total_sectors);
        self.write_at(0, &s0)?;
        self.write_at(1, &s1)?;
        Ok(())
    }

    /// First-fit a contiguous run of `needed` free sectors. Sectors 0 and 1
    /// are the catalogue; every live file occupies a contiguous extent. DFS
    /// requires files be unfragmented, so we return the lowest gap that fits.
    fn allocate_contiguous(&self, needed: u16) -> Result<u16, FilesystemError> {
        if needed == 0 {
            return Ok(2);
        }
        let mut occupied: Vec<(u16, u16)> = self
            .cat
            .entries
            .iter()
            .map(|e| (e.start_sector, e.start_sector + e.sectors()))
            .collect();
        occupied.sort_by_key(|&(s, _)| s);
        let mut cursor = 2u16; // first sector after the catalogue
        for &(start, end) in &occupied {
            if start >= cursor && start - cursor >= needed {
                return Ok(cursor);
            }
            cursor = cursor.max(end);
        }
        if self.geom.total_sectors >= cursor && self.geom.total_sectors - cursor >= needed {
            return Ok(cursor);
        }
        Err(FilesystemError::DiskFull(format!(
            "no contiguous run of {needed} sectors free"
        )))
    }

    fn refresh(&mut self) -> Result<(), FilesystemError> {
        let (s0, s1) = read_two_sectors(&mut self.reader, self.partition_offset)?;
        self.cat = Catalogue::parse(&s0, &s1);
        Ok(())
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for DfsFilesystem<R> {
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
                "DFS is flat; files live in single-character directories under the root".into(),
            ));
        }
        let (dir, fname) = split_dir_name(name)?;
        if self.cat.entries.iter().any(|e| e.same_file(dir, &fname)) {
            return Err(FilesystemError::AlreadyExists(format!(
                "DFS file '{name}' already exists"
            )));
        }
        if self.cat.entries.len() >= MAX_FILES {
            return Err(FilesystemError::DiskFull(
                "DFS catalogue is full (31 files)".into(),
            ));
        }

        let mut payload = Vec::with_capacity(data_len as usize);
        data.read_to_end(&mut payload)?;
        if payload.len() as u64 != data_len {
            return Err(FilesystemError::InvalidData(format!(
                "data_len {data_len} != actual {} bytes",
                payload.len()
            )));
        }
        if payload.len() > 0x3ffff {
            return Err(FilesystemError::InvalidData(
                "DFS file length exceeds the 18-bit (256 KB) limit".into(),
            ));
        }

        let needed = (payload.len().div_ceil(SECTOR_SIZE)) as u16;
        let start = self.allocate_contiguous(needed)?;

        // Write the payload as whole zero-padded sectors so the tail of the
        // last sector is deterministic.
        if needed > 0 {
            let mut buf = vec![0u8; needed as usize * SECTOR_SIZE];
            buf[..payload.len()].copy_from_slice(&payload);
            self.write_at(start, &buf)?;
        }

        self.cat.cycle = bcd_increment(self.cat.cycle);
        self.cat.entries.push(DfsEntry {
            dir,
            name: fname.clone(),
            locked: false,
            load_addr: 0,
            exec_addr: 0,
            length: payload.len() as u32,
            start_sector: start,
        });
        self.flush_catalogue()?;
        self.reader.flush()?;
        self.refresh()?;

        let idx = self
            .cat
            .entries
            .iter()
            .position(|e| e.same_file(dir, &fname))
            .ok_or_else(|| {
                FilesystemError::InvalidData("internal: created file not visible".into())
            })?;
        let disp = self.cat.entries[idx].display_name();
        Ok(FileEntry::new_file(
            disp.clone(),
            format!("/{disp}"),
            payload.len() as u64,
            idx as u64,
        ))
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "DFS directories are single-character name prefixes, not creatable folders".into(),
        ))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        if parent.path != "/" {
            return Err(FilesystemError::Unsupported(
                "DFS is flat; only root files are deletable".into(),
            ));
        }
        // Resolve by display name so a stale index can't delete the wrong file.
        let idx = self
            .find_entry(&entry.name)
            .ok_or_else(|| FilesystemError::NotFound(format!("'{}' not found", entry.name)))?;
        if self.cat.entries[idx].locked {
            return Err(FilesystemError::Unsupported(format!(
                "'{}' is locked",
                self.cat.entries[idx].display_name()
            )));
        }
        self.cat.entries.remove(idx);
        self.cat.cycle = bcd_increment(self.cat.cycle);
        self.flush_catalogue()?;
        self.reader.flush()?;
        self.refresh()?;
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
                "DFS is flat; only root files are renamable".into(),
            ));
        }
        let (new_dir, new_fname) = split_dir_name(new_name)?;

        // Resolve the target by display name so a stale index can't rename the
        // wrong file (matches delete_entry).
        let idx = self
            .find_entry(&entry.name)
            .ok_or_else(|| FilesystemError::NotFound(format!("'{}' not found", entry.name)))?;
        if self.cat.entries[idx].locked {
            return Err(FilesystemError::Unsupported(format!(
                "'{}' is locked",
                self.cat.entries[idx].display_name()
            )));
        }

        // Reject a collision with a *different* entry. DFS folds case, so a
        // case-only rename resolves to the same slot and is allowed.
        if self
            .cat
            .entries
            .iter()
            .enumerate()
            .any(|(i, e)| i != idx && e.same_file(new_dir, &new_fname))
        {
            return Err(FilesystemError::AlreadyExists(new_name.to_string()));
        }

        // Overwrite only the dir/name; start_sector, length, addresses and the
        // lock flag are untouched, so the file keeps its identity and contents.
        self.cat.entries[idx].dir = new_dir;
        self.cat.entries[idx].name = new_fname;
        self.cat.cycle = bcd_increment(self.cat.cycle);
        self.flush_catalogue()?;
        self.reader.flush()?;
        self.refresh()?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let used: u16 = self.cat.entries.iter().map(|e| e.sectors()).sum();
        let free_sectors = self
            .geom
            .total_sectors
            .saturating_sub(2)
            .saturating_sub(used);
        Ok(free_sectors as u64 * SECTOR_SIZE as u64)
    }
}

/// Increment a packed-BCD cycle byte, wrapping 0x99 -> 0x00 (DFS convention).
fn bcd_increment(b: u8) -> u8 {
    let lo = b & 0x0f;
    let hi = b >> 4;
    if lo < 9 {
        (hi << 4) | (lo + 1)
    } else if hi < 9 {
        (hi + 1) << 4
    } else {
        0
    }
}

/// Build a blank single-sided DFS image with the given geometry + title.
pub fn create_blank_dfs(total_sectors: u16, title: &str) -> Vec<u8> {
    let mut img = vec![0u8; total_sectors as usize * SECTOR_SIZE];
    let mut title_raw = [0u8; 12];
    let tb = title.as_bytes();
    title_raw[..tb.len().min(12)].copy_from_slice(&tb[..tb.len().min(12)]);
    img[0..8].copy_from_slice(&title_raw[..8]);
    img[SECTOR_SIZE..SECTOR_SIZE + 4].copy_from_slice(&title_raw[8..]);
    img[SECTOR_SIZE + 5] = 0; // zero files
    img[SECTOR_SIZE + 6] = ((total_sectors >> 8) & 0x03) as u8;
    img[SECTOR_SIZE + 7] = (total_sectors & 0xff) as u8;
    img
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn open_mem(img: Vec<u8>) -> DfsFilesystem<Cursor<Vec<u8>>> {
        DfsFilesystem::open(Cursor::new(img), 0).expect("open")
    }

    #[test]
    fn split_dir_name_parses() {
        assert_eq!(split_dir_name("HELLO").unwrap(), (b'$', "HELLO".into()));
        assert_eq!(split_dir_name("A.PROG").unwrap(), (b'A', "PROG".into()));
        assert!(split_dir_name("").is_err());
        assert!(split_dir_name("TOOLONGX").is_err());
        assert!(split_dir_name("BAD NAME").is_err());
        assert!(split_dir_name("A.B.C").is_err());
    }

    #[test]
    fn blank_disk_detects_and_is_empty() {
        let img = create_blank_dfs(400, "RUSTYBKP");
        assert_eq!(img.len(), 102_400);
        let mut cur = Cursor::new(img.clone());
        assert!(looks_like_dfs(&mut cur, 0).is_some());
        let mut fs = open_mem(img);
        assert_eq!(fs.fs_type(), "Acorn DFS");
        assert_eq!(fs.volume_label(), Some("RUSTYBKP"));
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
        assert_eq!(fs.free_space().unwrap(), (400 - 2) * 256);
    }

    #[test]
    fn create_read_delete_round_trip() {
        let mut fs = open_mem(create_blank_dfs(400, "TEST"));
        let root = fs.root().unwrap();

        let hello = b"HELLO BBC MICRO\r".to_vec();
        fs.create_file(
            &root,
            "HELLO",
            &mut Cursor::new(hello.clone()),
            hello.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        // A multi-sector file (1000 bytes spans 4 sectors of 256).
        let big: Vec<u8> = (0..1000).map(|i| (i * 7 % 256) as u8).collect();
        fs.create_file(
            &root,
            "B.DATA",
            &mut Cursor::new(big.clone()),
            big.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();

        let listing = fs.list_directory(&root).unwrap();
        assert_eq!(listing.len(), 2);
        let h = listing.iter().find(|e| e.name == "HELLO").unwrap();
        assert_eq!(fs.read_file(h, usize::MAX).unwrap(), hello);
        let d = listing.iter().find(|e| e.name == "B.DATA").unwrap();
        assert_eq!(d.size, 1000);
        assert_eq!(fs.read_file(d, usize::MAX).unwrap(), big);

        let free_before = fs.free_space().unwrap();
        fs.delete_entry(&root, &h.clone()).unwrap();
        let listing = fs.list_directory(&root).unwrap();
        assert_eq!(listing.len(), 1);
        assert_eq!(listing[0].name, "B.DATA");
        assert!(fs.free_space().unwrap() > free_before);

        // Re-open from bytes to prove persistence.
        let bytes = fs.into_inner().into_inner();
        let mut cur = Cursor::new(bytes.clone());
        assert!(looks_like_dfs(&mut cur, 0).is_some());
        let mut fs2 = DfsFilesystem::open(Cursor::new(bytes), 0).unwrap();
        let r2 = fs2.root().unwrap();
        let l2 = fs2.list_directory(&r2).unwrap();
        assert_eq!(l2.len(), 1);
        assert_eq!(fs2.read_file(&l2[0], usize::MAX).unwrap(), big);
    }

    #[test]
    fn rename_in_place_round_trip() {
        let mut fs = open_mem(create_blank_dfs(400, "TEST"));
        let root = fs.root().unwrap();

        let hello = b"HELLO BBC MICRO\r".to_vec();
        fs.create_file(
            &root,
            "HELLO",
            &mut Cursor::new(hello.clone()),
            hello.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let h = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "HELLO")
            .unwrap();
        let start = fs.cat.entries[h.location as usize].start_sector;

        // Rename into the 'B' directory with a new base name.
        fs.rename(&root, &h, "B.GREET").unwrap();

        // Re-open from bytes to prove persistence.
        let bytes = fs.into_inner().into_inner();
        let mut fs2 = DfsFilesystem::open(Cursor::new(bytes), 0).unwrap();
        let r2 = fs2.root().unwrap();
        let l2 = fs2.list_directory(&r2).unwrap();
        let names: Vec<&str> = l2.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"B.GREET"));
        assert!(!names.contains(&"HELLO"));

        // Identity (start sector) + contents preserved.
        let renamed = l2.iter().find(|e| e.name == "B.GREET").unwrap();
        assert_eq!(
            fs2.cat.entries[renamed.location as usize].start_sector,
            start
        );
        assert_eq!(fs2.read_file(renamed, usize::MAX).unwrap(), hello);
    }

    #[test]
    fn rename_collision_rejected() {
        let mut fs = open_mem(create_blank_dfs(400, "COL"));
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            "ONE",
            &mut Cursor::new(vec![1u8; 10]),
            10,
            &CreateFileOptions::default(),
        )
        .unwrap();
        fs.create_file(
            &root,
            "TWO",
            &mut Cursor::new(vec![2u8; 10]),
            10,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let two = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "TWO")
            .unwrap();
        // Case-insensitive collision with the *other* file.
        let err = fs.rename(&root, &two, "one").unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }

    #[test]
    fn catalogue_kept_in_descending_start_order() {
        let mut fs = open_mem(create_blank_dfs(400, "ORD"));
        let root = fs.root().unwrap();
        for (i, n) in ["ONE", "TWO", "THREE"].iter().enumerate() {
            let body = vec![i as u8 + 1; 300]; // 2 sectors each
            fs.create_file(
                &root,
                n,
                &mut Cursor::new(body.clone()),
                body.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }
        // Inspect the raw catalogue: start sectors must be strictly descending.
        let bytes = fs.into_inner().into_inner();
        let starts: Vec<u16> = (0..3)
            .map(|i| {
                let io = SECTOR_SIZE + 8 + i * 8;
                bytes[io + 7] as u16 | (((bytes[io + 6] & 0x03) as u16) << 8)
            })
            .collect();
        assert!(
            starts.windows(2).all(|w| w[0] > w[1]),
            "catalogue not descending: {starts:?}"
        );
    }

    #[test]
    fn duplicate_name_rejected_case_insensitively() {
        let mut fs = open_mem(create_blank_dfs(400, "DUP"));
        let root = fs.root().unwrap();
        fs.create_file(
            &root,
            " One",
            &mut Cursor::new(vec![1u8; 10]),
            10,
            &CreateFileOptions::default(),
        )
        .ok(); // "One" with a leading space is actually invalid; ignore.
        fs.create_file(
            &root,
            "ONE",
            &mut Cursor::new(vec![1u8; 10]),
            10,
            &CreateFileOptions::default(),
        )
        .unwrap();
        let err = fs
            .create_file(
                &root,
                "one",
                &mut Cursor::new(vec![2u8; 10]),
                10,
                &CreateFileOptions::default(),
            )
            .unwrap_err();
        assert!(matches!(err, FilesystemError::AlreadyExists(_)));
    }

    #[test]
    fn rejects_double_sided_declared_total_mismatch() {
        // An 80-track-sized file whose catalogue declares 400 sectors (the
        // shape a flat 40-track .dsd presents) must be rejected.
        let mut img = create_blank_dfs(800, "DSD");
        img.resize(204_800, 0);
        // Stamp a 40-track (400) declared total into the 800-sector body.
        img[SECTOR_SIZE + 6] = ((400u16 >> 8) & 0x03) as u8;
        img[SECTOR_SIZE + 7] = (400u16 & 0xff) as u8;
        let mut cur = Cursor::new(img);
        assert!(looks_like_dfs(&mut cur, 0).is_none());
    }

    #[test]
    fn bcd_increment_wraps() {
        assert_eq!(bcd_increment(0x00), 0x01);
        assert_eq!(bcd_increment(0x09), 0x10);
        assert_eq!(bcd_increment(0x19), 0x20);
        assert_eq!(bcd_increment(0x99), 0x00);
    }
}
