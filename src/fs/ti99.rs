//! TI-99/4A disk filesystem — read driver.
//!
//! The filesystem of the TI-99/4A's disk controller (and the MiSTer
//! **TI-99_4A** core), stored in the ubiquitous flat V9T9 `.dsk` sector image.
//! Unlike the contiguous CoCo/ZX floppies it is a proper little filesystem: a
//! Volume Information Block with an allocation bitmap, a sorted index of File
//! Descriptor Records, and **extent-based** files (a packed cluster chain).
//!
//! Layout:
//!
//! ```text
//!   sector 0   Volume Information Block (VIB): name, geometry, alloc bitmap
//!   sector 1   File Descriptor Index Record (FDIR): sorted FDR pointers
//!   sector 2+  File Descriptor Records + file data
//! ```
//!
//! Every multi-byte field is **big-endian** (the TMS9900 convention). Geometry
//! is read from the VIB, not guessed from the file size. The allocation bitmap
//! at VIB offset 0x38 uses **bit set = used** (LSB of byte 0x38 = sector 0);
//! sectors 0 and 1, and every sector past the disk end, are marked used.
//!
//! A file's data is an extent list packed three bytes per cluster at FDR offset
//! 0x1C: the 12-bit start AU is `b0 | ((b1 & 0x0F) << 8)` and the 12-bit
//! file-relative index of the cluster's last AU is `(b1 >> 4) | (b2 << 4)`
//! (AU = 1 sector on floppies). All offsets were validated byte-for-byte
//! against MAME's `imgtool` reader and an independent clean-room oracle
//! (`scripts/ti99-oracle.py`).
//!
//! This is the read half of the TI-99 quartet (Browse); edit / create / fsck
//! build on it (`docs/filesystem_completion_plan.md` Part 2).

use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

pub(crate) const SECTOR: u64 = 256;
/// The VIB is sector 0 (image start); the FDIR is sector 1.
pub(crate) const FDIR_SECTOR: u64 = 1;
/// First sector available for FDRs / file data.
pub(crate) const FIRST_DATA: u16 = 2;
/// Allocation bitmap offset within the VIB.
pub(crate) const BITMAP_OFF: usize = 0x38;
/// The bitmap spans 0x38..0xEC (180 bytes = 1440 bits).
pub(crate) const BITMAP_BITS: u16 = 1440;
/// Cluster chain offset within an FDR.
pub(crate) const FDR_CLUSTERS_OFF: usize = 0x1C;
/// FDIR capacity (127 two-byte pointers + a 0x0000 terminator).
pub(crate) const MAX_FILES: usize = 127;

fn be16(buf: &[u8], off: usize) -> u16 {
    u16::from_be_bytes([buf[off], buf[off + 1]])
}

/// Disk geometry, read from the VIB.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ti99Geometry {
    pub total_sectors: u16,
    pub sectors_per_track: u8,
    pub tracks_per_side: u8,
    pub sides: u8,
    pub density: u8,
}

impl Ti99Geometry {
    pub fn total_bytes(&self) -> u64 {
        self.total_sectors as u64 * SECTOR
    }

    pub fn describe(&self) -> String {
        let dens = match self.density {
            1 => "SD",
            2 => "DD",
            3 => "HD",
            _ => "?D",
        };
        format!(
            "{} side(s), {} tracks, {} sec/track, {}",
            self.sides, self.tracks_per_side, self.sectors_per_track, dens
        )
    }
}

/// A parsed File Descriptor Record.
#[derive(Clone, Debug)]
pub struct Ti99Entry {
    /// Sector holding this file's FDR.
    pub fdr_sector: u16,
    pub name: String,
    /// FDR status flags (0x0C): bit0 program, bit1 internal, bit3 protected,
    /// bit7 variable-length records.
    pub flags: u8,
    /// Sectors of file data (excludes the FDR sector).
    pub sectors_alloc: u16,
    /// Bytes used in the final data sector (0 = a full 256-byte sector).
    pub eof_offset: u8,
    /// Physical data sectors, resolved from the cluster chain at open.
    pub data_sectors: Vec<u16>,
}

impl Ti99Entry {
    pub fn byte_size(&self) -> u64 {
        if self.sectors_alloc == 0 {
            return 0;
        }
        let last = if self.eof_offset == 0 {
            SECTOR
        } else {
            self.eof_offset as u64
        };
        (self.sectors_alloc as u64 - 1) * SECTOR + last
    }

    pub fn is_program(&self) -> bool {
        self.flags & 0x01 != 0
    }
}

/// Decode the FDR cluster chain into the file's physical data sectors.
/// `fdr` is the 256-byte FDR sector. Stops at the declared sector count, a zero
/// cluster, or the end of the chain area — whichever comes first.
pub(crate) fn walk_clusters(fdr: &[u8], sectors_alloc: u16) -> Vec<u16> {
    let mut out = Vec::new();
    let mut prev_off: i32 = -1;
    let mut c = FDR_CLUSTERS_OFF;
    while c + 3 <= SECTOR as usize && out.len() < sectors_alloc as usize {
        let (b0, b1, b2) = (fdr[c] as u16, fdr[c + 1] as u16, fdr[c + 2] as u16);
        let start = b0 | ((b1 & 0x0F) << 8);
        let last_off = ((b1 >> 4) | (b2 << 4)) as i32;
        if start == 0 && last_off == 0 {
            break;
        }
        let count = last_off - prev_off;
        if count <= 0 {
            break; // malformed / non-increasing offset
        }
        for k in 0..count {
            out.push(start.wrapping_add(k as u16));
        }
        prev_off = last_off;
        c += 3;
    }
    out.truncate(sectors_alloc as usize);
    out
}

fn trim_name(name: &[u8]) -> String {
    let end = name.iter().rposition(|&b| b != b' ').map_or(0, |i| i + 1);
    name[..end]
        .iter()
        .map(|&b| {
            if (0x20..0x7F).contains(&b) {
                b as char
            } else {
                '?'
            }
        })
        .collect()
}

pub struct Ti99Filesystem<R: Read + Seek> {
    reader: R,
    partition_offset: u64,
    geometry: Ti99Geometry,
    volume_label: String,
    /// The VIB (sector 0), kept for the bitmap.
    vib: Vec<u8>,
    entries: Vec<Ti99Entry>,
}

/// Structural detector: a TI-99 disk has the ASCII "DSK" marker at VIB offset
/// 0x0D plus a self-consistent geometry. Returns the parsed geometry on a match.
pub fn looks_like_ti99<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<Ti99Geometry> {
    let len = reader
        .seek(SeekFrom::End(0))
        .ok()?
        .checked_sub(partition_offset)?;
    if len < FIRST_DATA as u64 * SECTOR || len % SECTOR != 0 {
        return None;
    }
    reader.seek(SeekFrom::Start(partition_offset)).ok()?;
    let mut vib = [0u8; SECTOR as usize];
    reader.read_exact(&mut vib).ok()?;

    if &vib[0x0D..0x10] != b"DSK" {
        return None;
    }
    let total_sectors = be16(&vib, 0x0A);
    let sectors_per_track = vib[0x0C];
    let sides = vib[0x12];
    let density = vib[0x13];
    // Geometry sanity — a confident discriminator alongside the DSK marker.
    if !(FIRST_DATA..=BITMAP_BITS).contains(&total_sectors)
        || total_sectors as u64 * SECTOR > len
        || !matches!(sectors_per_track, 9 | 16 | 18 | 36)
        || !matches!(sides, 1 | 2)
        || !(1..=3).contains(&density)
    {
        return None;
    }
    Some(Ti99Geometry {
        total_sectors,
        sectors_per_track,
        tracks_per_side: vib[0x11],
        sides,
        density,
    })
}

impl<R: Read + Seek> Ti99Filesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        let geometry = looks_like_ti99(&mut reader, partition_offset)
            .ok_or_else(|| FilesystemError::InvalidData("not a TI-99 disk".into()))?;

        let mut vib = vec![0u8; SECTOR as usize];
        reader.seek(SeekFrom::Start(partition_offset))?;
        reader.read_exact(&mut vib)?;
        let volume_label = trim_name(&vib[0..10]);

        // FDIR (sector 1): up to 127 big-endian FDR-sector pointers, 0-terminated.
        let mut fdir = vec![0u8; SECTOR as usize];
        reader.seek(SeekFrom::Start(partition_offset + FDIR_SECTOR * SECTOR))?;
        reader.read_exact(&mut fdir)?;

        let mut entries = Vec::new();
        for i in 0..MAX_FILES {
            let ptr = be16(&fdir, i * 2);
            if ptr == 0 {
                break;
            }
            if ptr < FIRST_DATA || ptr >= geometry.total_sectors {
                // Out-of-range pointer: skip (fsck surfaces it).
                continue;
            }
            let mut fdr = vec![0u8; SECTOR as usize];
            reader.seek(SeekFrom::Start(partition_offset + ptr as u64 * SECTOR))?;
            if reader.read_exact(&mut fdr).is_err() {
                continue;
            }
            let name = trim_name(&fdr[0..10]);
            if name.is_empty() {
                continue;
            }
            let sectors_alloc = be16(&fdr, 0x0E);
            let data_sectors = walk_clusters(&fdr, sectors_alloc);
            entries.push(Ti99Entry {
                fdr_sector: ptr,
                name,
                flags: fdr[0x0C],
                sectors_alloc,
                eof_offset: fdr[0x10],
                data_sectors,
            });
        }

        Ok(Ti99Filesystem {
            reader,
            partition_offset,
            geometry,
            volume_label,
            vib,
            entries,
        })
    }

    fn read_sector(&mut self, sector: u16, buf: &mut [u8]) -> Result<(), FilesystemError> {
        self.reader.seek(SeekFrom::Start(
            self.partition_offset + sector as u64 * SECTOR,
        ))?;
        self.reader.read_exact(buf)?;
        Ok(())
    }

    fn bitmap_used(&self, sector: u16) -> bool {
        let byte = BITMAP_OFF + (sector / 8) as usize;
        byte < self.vib.len() && (self.vib[byte] >> (sector % 8)) & 1 != 0
    }

    fn used_sectors(&self) -> u64 {
        (0..self.geometry.total_sectors)
            .filter(|&s| self.bitmap_used(s))
            .count() as u64
    }

    fn entry_to_file(&self, e: &Ti99Entry) -> FileEntry {
        FileEntry::new_file(
            e.name.clone(),
            format!("/{}", e.name),
            e.byte_size(),
            e.fdr_sector as u64,
        )
    }
}

impl<R: Read + Seek + Send> Filesystem for Ti99Filesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        Ok(FileEntry::new_directory("/".into(), "/".into(), 0))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
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
                "TI-99 read_file on directory: {}",
                entry.path
            )));
        }
        let want = (entry.size as usize).min(max_bytes);
        if want == 0 {
            return Ok(Vec::new());
        }
        // Find the entry by its FDR sector (entry.location) to get the chain.
        let sectors = self
            .entries
            .iter()
            .find(|e| e.fdr_sector as u64 == entry.location)
            .map(|e| e.data_sectors.clone())
            .ok_or_else(|| FilesystemError::NotFound(entry.path.clone()))?;

        let mut out = Vec::with_capacity(want);
        let mut sec = [0u8; SECTOR as usize];
        for s in sectors {
            if out.len() >= want {
                break;
            }
            self.read_sector(s, &mut sec)?;
            let take = (want - out.len()).min(SECTOR as usize);
            out.extend_from_slice(&sec[..take]);
        }
        Ok(out)
    }

    fn volume_label(&self) -> Option<&str> {
        if self.volume_label.is_empty() {
            None
        } else {
            Some(&self.volume_label)
        }
    }

    fn fs_type(&self) -> &str {
        "TI-99"
    }

    fn total_size(&self) -> u64 {
        self.geometry.total_bytes()
    }

    fn used_size(&self) -> u64 {
        self.used_sectors() * SECTOR
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(SECTOR)
    }

    fn last_data_byte(&mut self) -> Result<u64, FilesystemError> {
        let last = (0..self.geometry.total_sectors)
            .rev()
            .find(|&s| self.bitmap_used(s))
            .map_or(FIRST_DATA, |s| s + 1);
        Ok(last as u64 * SECTOR)
    }

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        validate_ti99_name(name).map(|_| ())
    }
}

/// Validate and normalize a TI-99 file name: uppercased, 1-10 printable ASCII
/// bytes, no `.` / `/` / space (space is the on-disk pad/terminator). Returns
/// the 10-byte, space-padded on-disk name.
fn validate_ti99_name(name: &str) -> Result<[u8; 10], FilesystemError> {
    let up = name.to_ascii_uppercase();
    let bytes = up.as_bytes();
    if bytes.is_empty() || bytes.len() > 10 {
        return Err(FilesystemError::InvalidData(format!(
            "invalid TI-99 file name {name:?} (1-10 characters)"
        )));
    }
    if bytes
        .iter()
        .any(|&b| !(0x21..0x7F).contains(&b) || b == b'.' || b == b'/')
    {
        return Err(FilesystemError::InvalidData(format!(
            "invalid TI-99 file name {name:?} (printable ASCII, no '.' '/' or space)"
        )));
    }
    let mut n = [b' '; 10];
    n[..bytes.len()].copy_from_slice(bytes);
    Ok(n)
}

/// The 10-byte, space-padded form of a display name (for FDIR sort order).
fn padded_name(name: &str) -> [u8; 10] {
    let mut n = [b' '; 10];
    let b = name.as_bytes();
    let k = b.len().min(10);
    n[..k].copy_from_slice(&b[..k]);
    n
}

/// Sanitize a volume name to 10 space-padded printable-ASCII bytes (uppercased,
/// no `.`). Volume names may contain spaces, unlike file names.
fn sanitize_ti99_volname(name: &str) -> [u8; 10] {
    let up = name.to_ascii_uppercase();
    let filtered: Vec<u8> = up
        .bytes()
        .filter(|&b| (0x20..0x7F).contains(&b) && b != b'.')
        .take(10)
        .collect();
    let mut n = [b' '; 10];
    n[..filtered.len()].copy_from_slice(&filtered);
    n
}

/// Build a blank TI-99 volume of the given geometry: a VIB (name, geometry,
/// "DSK" marker) with an allocation bitmap marking sectors 0-1 (VIB + FDIR) and
/// every past-the-end sector used, plus an empty FDIR.
pub(crate) fn build_blank_ti99(sides: u8, tracks: u8, spt: u8, density: u8, name: &str) -> Vec<u8> {
    let total = sides as u16 * tracks as u16 * spt as u16;
    let mut img = vec![0u8; total as usize * SECTOR as usize];
    img[0..10].copy_from_slice(&sanitize_ti99_volname(name));
    img[0x0A..0x0C].copy_from_slice(&total.to_be_bytes());
    img[0x0C] = spt;
    img[0x0D..0x10].copy_from_slice(b"DSK");
    img[0x10] = 0x20; // unprotected
    img[0x11] = tracks;
    img[0x12] = sides;
    img[0x13] = density;
    img[0xEC..0x100].fill(0xFF); // reserved bitmap tail
    for s in total..BITMAP_BITS {
        img[BITMAP_OFF + (s / 8) as usize] |= 1 << (s % 8); // beyond-disk = used
    }
    img[BITMAP_OFF] |= 0b11; // sectors 0 (VIB) + 1 (FDIR) used
    img
}

/// Format a blank TI-99 volume. Geometry is the smallest standard floppy that
/// covers `size_bytes`, capped at DSDD (360 KiB): ≤ 90 KiB → SSSD, ≤ 180 KiB →
/// DSSD, else DSDD. `name` becomes the 10-char disk name.
pub fn create_blank_ti99(size_bytes: u64, name: &str) -> Result<Vec<u8>, FilesystemError> {
    let (sides, tracks, spt, density) = if size_bytes <= 90 * 1024 {
        (1, 40, 9, 1) // SSSD, 360 sectors
    } else if size_bytes <= 180 * 1024 {
        (2, 40, 9, 1) // DSSD, 720 sectors
    } else {
        (2, 40, 18, 2) // DSDD, 1440 sectors (the floppy maximum)
    };
    Ok(build_blank_ti99(sides, tracks, spt, density, name))
}

/// Pack a list of physical data sectors into 3-byte cluster entries, grouping
/// contiguous runs. Inverse of [`walk_clusters`].
pub(crate) fn pack_clusters(sectors: &[u16]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut file_au: u16 = 0;
    while i < sectors.len() {
        let start = sectors[i];
        let mut j = i;
        while j + 1 < sectors.len() && sectors[j + 1] == sectors[j] + 1 {
            j += 1;
        }
        let run_len = (j - i + 1) as u16;
        let last_off = file_au + run_len - 1;
        out.push((start & 0xFF) as u8);
        out.push((((start >> 8) & 0x0F) | ((last_off & 0x0F) << 4)) as u8);
        out.push(((last_off >> 4) & 0xFF) as u8);
        file_au += run_len;
        i = j + 1;
    }
    out
}

impl<R: Read + Write + Seek> Ti99Filesystem<R> {
    fn write_sector(&mut self, sector: u16, data: &[u8]) -> Result<(), FilesystemError> {
        self.reader.seek(SeekFrom::Start(
            self.partition_offset + sector as u64 * SECTOR,
        ))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    fn bitmap_set(&mut self, sector: u16, used: bool) {
        let byte = BITMAP_OFF + (sector / 8) as usize;
        if byte >= self.vib.len() {
            return;
        }
        let mask = 1u8 << (sector % 8);
        if used {
            self.vib[byte] |= mask;
        } else {
            self.vib[byte] &= !mask;
        }
    }

    /// Allocate `count` free data sectors (a single contiguous run when one
    /// exists, else the lowest scattered sectors), marking them used. Returns
    /// `None` when the disk lacks `count` free sectors.
    fn alloc_sectors(&mut self, count: u16) -> Option<Vec<u16>> {
        if count == 0 {
            return Some(Vec::new());
        }
        let total = self.geometry.total_sectors;
        let free: Vec<u16> = (FIRST_DATA..total)
            .filter(|&s| !self.bitmap_used(s))
            .collect();
        if free.len() < count as usize {
            return None;
        }
        let need = count as usize;
        // Prefer a contiguous run so the cluster chain stays a single entry.
        let chosen = (0..=free.len() - need)
            .map(|i| &free[i..i + need])
            .find(|run| run[need - 1] - run[0] == count - 1)
            .map(<[u16]>::to_vec)
            .unwrap_or_else(|| free[..need].to_vec());
        for &s in &chosen {
            self.bitmap_set(s, true);
        }
        Some(chosen)
    }

    fn write_vib(&mut self) -> Result<(), FilesystemError> {
        let vib = self.vib.clone();
        self.write_sector(0, &vib)
    }

    /// Serialize the (sorted) entry list into the FDIR sector (sector 1).
    fn write_fdir(&mut self) -> Result<(), FilesystemError> {
        let mut fdir = vec![0u8; SECTOR as usize];
        for (i, e) in self.entries.iter().enumerate() {
            fdir[i * 2..i * 2 + 2].copy_from_slice(&e.fdr_sector.to_be_bytes());
        }
        self.write_sector(FDIR_SECTOR as u16, &fdir)
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for Ti99Filesystem<R> {
    fn create_file(
        &mut self,
        parent: &FileEntry,
        name: &str,
        data: &mut dyn Read,
        data_len: u64,
        _options: &CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        if !parent.is_directory() || parent.path != "/" {
            return Err(FilesystemError::NotADirectory(parent.path.clone()));
        }
        let padded = validate_ti99_name(name)?;
        let display = trim_name(&padded);
        if self.entries.iter().any(|e| padded_name(&e.name) == padded) {
            return Err(FilesystemError::AlreadyExists(display));
        }
        if self.entries.len() >= MAX_FILES {
            return Err(FilesystemError::DiskFull(
                "TI-99 directory full (127 files)".into(),
            ));
        }
        let n_data = data_len.div_ceil(SECTOR) as u16;
        if n_data as u64 > BITMAP_BITS as u64 {
            return Err(FilesystemError::InvalidData(
                "file too large for TI-99".into(),
            ));
        }
        // Allocate the FDR sector, then the data sectors.
        let fdr_sector = self.alloc_sectors(1).ok_or_else(|| {
            FilesystemError::DiskFull("no free sector for the file descriptor".into())
        })?[0];
        let data_sectors = match self.alloc_sectors(n_data) {
            Some(v) => v,
            None => {
                self.bitmap_set(fdr_sector, false);
                return Err(FilesystemError::DiskFull(format!(
                    "not enough free space (need {n_data} data sectors)"
                )));
            }
        };
        // Write the payload across the data sectors, zero-padding the last one.
        let mut remaining = data_len;
        for &s in &data_sectors {
            let mut buf = [0u8; SECTOR as usize];
            let take = remaining.min(SECTOR) as usize;
            if take > 0 {
                data.read_exact(&mut buf[..take])?;
            }
            self.write_sector(s, &buf)?;
            remaining -= take as u64;
        }
        // Build the FDR (a PROGRAM file: raw bytes).
        let mut fdr = vec![0u8; SECTOR as usize];
        fdr[0..10].copy_from_slice(&padded);
        fdr[0x0C] = 0x01; // program
        fdr[0x0E..0x10].copy_from_slice(&n_data.to_be_bytes());
        fdr[0x10] = (data_len % SECTOR) as u8; // EOF offset (0 = full last sector)
        let clusters = pack_clusters(&data_sectors);
        fdr[FDR_CLUSTERS_OFF..FDR_CLUSTERS_OFF + clusters.len()].copy_from_slice(&clusters);
        self.write_sector(fdr_sector, &fdr)?;

        let entry = Ti99Entry {
            fdr_sector,
            name: display,
            flags: 0x01,
            sectors_alloc: n_data,
            eof_offset: (data_len % SECTOR) as u8,
            data_sectors,
        };
        let fe = self.entry_to_file(&entry);
        self.entries.push(entry);
        self.entries.sort_by_key(|e| padded_name(&e.name));
        self.write_vib()?;
        self.write_fdir()?;
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        _parent: &FileEntry,
        _name: &str,
        _options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        Err(FilesystemError::Unsupported(
            "TI-99 subdirectories are not supported (flat root filesystem)".into(),
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
        let idx = self
            .entries
            .iter()
            .position(|e| e.name == entry.name)
            .ok_or_else(|| FilesystemError::NotFound(entry.name.clone()))?;
        let e = self.entries.remove(idx);
        // Free the data sectors + the FDR sector in the bitmap.
        for s in &e.data_sectors {
            self.bitmap_set(*s, false);
        }
        self.bitmap_set(e.fdr_sector, false);
        self.write_vib()?;
        self.write_fdir()
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
        let padded = validate_ti99_name(new_name)?;
        let display = trim_name(&padded);
        if self.entries.iter().any(|e| padded_name(&e.name) == padded) {
            return Err(FilesystemError::AlreadyExists(display));
        }
        let idx = self
            .entries
            .iter()
            .position(|e| e.name == entry.name)
            .ok_or_else(|| FilesystemError::NotFound(entry.name.clone()))?;
        let fdr_sector = self.entries[idx].fdr_sector;
        // Rewrite the name in the FDR (identity/contents keep).
        let mut fdr = vec![0u8; SECTOR as usize];
        self.read_sector(fdr_sector, &mut fdr)?;
        fdr[0..10].copy_from_slice(&padded);
        self.write_sector(fdr_sector, &fdr)?;
        self.entries[idx].name = display;
        self.entries.sort_by_key(|e| padded_name(&e.name));
        self.write_fdir()
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        let free = (FIRST_DATA..self.geometry.total_sectors)
            .filter(|&s| !self.bitmap_used(s))
            .count() as u64;
        Ok(free * SECTOR)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::process::Command;

    fn oracle() -> Option<PathBuf> {
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/ti99-oracle.py");
        p.exists().then_some(p)
    }

    fn have_python() -> bool {
        Command::new("python3").arg("--version").output().is_ok()
    }

    /// Build a TI-99 SSSD volume with the clean-room oracle + a couple of files.
    fn oracle_volume() -> Option<Vec<u8>> {
        let script = oracle()?;
        if !have_python() {
            return None;
        }
        let dir = std::env::temp_dir();
        let vol = dir.join(format!("rb_ti99_{}.dsk", std::process::id()));
        let volp = vol.to_str()?;
        let run = |args: &[&str]| {
            Command::new("python3")
                .arg(&script)
                .args(args)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        };
        if !run(&["mkfs", volp, "sssd", "MYDISK"]) {
            return None;
        }
        let small = dir.join(format!("rb_ti99_s_{}.bin", std::process::id()));
        std::fs::write(&small, b"HELLO TI-99\n").ok()?;
        let big = dir.join(format!("rb_ti99_b_{}.bin", std::process::id()));
        std::fs::write(&big, vec![0x5Au8; 3000]).ok()?;
        run(&["addfile", volp, "GREETING", small.to_str()?]);
        run(&["addfile", volp, "BIGFILE", big.to_str()?]);
        let bytes = std::fs::read(&vol).ok();
        let _ = std::fs::remove_file(&vol);
        let _ = std::fs::remove_file(&small);
        let _ = std::fs::remove_file(&big);
        bytes
    }

    #[test]
    fn reads_oracle_volume() {
        let Some(img) = oracle_volume() else {
            eprintln!("skipping reads_oracle_volume: python3/oracle unavailable");
            return;
        };
        let mut fs = Ti99Filesystem::open(Cursor::new(img), 0).expect("open oracle volume");
        assert_eq!(fs.volume_label(), Some("MYDISK"));
        assert_eq!(fs.total_size(), 360 * SECTOR); // SSSD = 90 KiB
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = files.iter().map(|e| e.name.as_str()).collect();
        // FDIR is alphabetically sorted.
        assert_eq!(names, vec!["BIGFILE", "GREETING"], "{names:?}");

        let big = files.iter().find(|e| e.name == "BIGFILE").unwrap();
        assert_eq!(big.size, 3000);
        let data = fs.read_file(big, usize::MAX).unwrap();
        assert_eq!(data.len(), 3000);
        assert!(data.iter().all(|&b| b == 0x5A));

        let greet = files.iter().find(|e| e.name == "GREETING").unwrap();
        assert_eq!(fs.read_file(greet, usize::MAX).unwrap(), b"HELLO TI-99\n");
    }

    #[test]
    fn cluster_decode() {
        // A 3-sector file starting at sector 34, single contiguous cluster:
        // b0=0x22, b1=(0)|(2<<4)=0x20, b2=0.
        let mut fdr = vec![0u8; SECTOR as usize];
        fdr[FDR_CLUSTERS_OFF] = 0x22;
        fdr[FDR_CLUSTERS_OFF + 1] = 0x20;
        fdr[FDR_CLUSTERS_OFF + 2] = 0x00;
        assert_eq!(walk_clusters(&fdr, 3), vec![34, 35, 36]);

        // Two clusters: sectors 10,11 then 20,21,22 (file AUs 0..4).
        // cluster A: start=10, last_off=1 -> b0=10,b1=(0)|(1<<4)=0x10,b2=0.
        // cluster B: start=20, last_off=4 -> b0=20,b1=(0)|(4<<4)=0x40,b2=0.
        let mut fdr2 = vec![0u8; SECTOR as usize];
        fdr2[FDR_CLUSTERS_OFF..FDR_CLUSTERS_OFF + 6].copy_from_slice(&[10, 0x10, 0, 20, 0x40, 0]);
        assert_eq!(walk_clusters(&fdr2, 5), vec![10, 11, 20, 21, 22]);
    }

    #[test]
    fn rejects_non_ti99() {
        assert!(looks_like_ti99(&mut Cursor::new(vec![0u8; 92160]), 0).is_none());
        // DSK marker but an implausible geometry.
        let mut img = vec![0u8; 92160];
        img[0x0D..0x10].copy_from_slice(b"DSK");
        img[0x0A..0x0C].copy_from_slice(&360u16.to_be_bytes());
        img[0x0C] = 7; // invalid sectors/track
        assert!(looks_like_ti99(&mut Cursor::new(img), 0).is_none());
    }

    /// Cross-check: the disk our oracle writes lists through MAME's imgtool.
    #[test]
    fn oracle_disk_reads_through_imgtool() {
        let Some(script) = oracle() else {
            return;
        };
        if !have_python() || Command::new("imgtool").arg("--version").output().is_err() {
            eprintln!("skipping imgtool cross-check: imgtool/python unavailable");
            return;
        }
        let dir = std::env::temp_dir();
        let vol = dir.join(format!("rb_ti99_im_{}.dsk", std::process::id()));
        let volp = vol.to_str().unwrap();
        let py = |args: &[&str]| {
            Command::new("python3")
                .arg(&script)
                .args(args)
                .output()
                .expect("python3")
                .status
                .success()
        };
        assert!(py(&["mkfs", volp, "sssd", "IMGCHK"]));
        let src = dir.join(format!("rb_ti99_src_{}.bin", std::process::id()));
        std::fs::write(&src, vec![0x33u8; 500]).unwrap();
        assert!(py(&["addfile", volp, "PROG", src.to_str().unwrap()]));
        let out = Command::new("imgtool")
            .args(["dir", "v9t9", volp])
            .output()
            .expect("imgtool");
        let listing = String::from_utf8_lossy(&out.stdout).into_owned();
        let _ = std::fs::remove_file(&vol);
        let _ = std::fs::remove_file(&src);
        assert!(
            out.status.success() && listing.contains("PROG"),
            "imgtool did not list our oracle disk:\n{listing}"
        );
    }

    // ---- Edit ----

    /// A blank writable TI-99 volume built in memory (delegates to the shared
    /// formatter used by `create_blank_ti99`).
    fn blank_image(sides: u8, tracks: u8, spt: u8, density: u8, name: &str) -> Vec<u8> {
        build_blank_ti99(sides, tracks, spt, density, name)
    }

    #[test]
    fn edit_create_rename_delete_roundtrip() {
        let mut fs = Ti99Filesystem::open(Cursor::new(blank_image(1, 40, 9, 1, "EDITVOL")), 0)
            .expect("open blank");
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());

        let a = vec![0x41u8; 300]; // 2 sectors
        fs.create_file(
            &root,
            "ALPHA",
            &mut &a[..],
            300,
            &CreateFileOptions::default(),
        )
        .expect("create ALPHA");
        let b = vec![0x42u8; 5000]; // 20 sectors
        fs.create_file(
            &root,
            "BETA",
            &mut &b[..],
            5000,
            &CreateFileOptions::default(),
        )
        .expect("create BETA");

        let alpha = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "ALPHA")
            .unwrap();
        fs.rename(&root, &alpha, "GAMMA").expect("rename");

        fs.create_file(
            &root,
            "TEMP",
            &mut &b"xy"[..],
            2,
            &CreateFileOptions::default(),
        )
        .expect("create TEMP");
        let temp = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "TEMP")
            .unwrap();
        fs.delete_entry(&root, &temp).expect("delete TEMP");

        let files = fs.list_directory(&root).unwrap();
        let names: Vec<&str> = files.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["BETA", "GAMMA"], "{names:?}"); // sorted, ALPHA renamed, TEMP gone

        let beta = files.iter().find(|e| e.name == "BETA").unwrap();
        assert_eq!(beta.size, 5000);
        let d = fs.read_file(beta, usize::MAX).unwrap();
        assert_eq!(d.len(), 5000);
        assert!(d.iter().all(|&x| x == 0x42));

        assert!(matches!(
            fs.create_directory(&root, "sub", &CreateDirectoryOptions::default()),
            Err(FilesystemError::Unsupported(_))
        ));
    }

    #[test]
    fn edit_persists_across_reopen() {
        let mut cur = Cursor::new(blank_image(1, 40, 9, 1, "REOPEN"));
        {
            let mut fs = Ti99Filesystem::open(&mut cur, 0).unwrap();
            let root = fs.root().unwrap();
            let data = vec![0x7Eu8; 1500];
            fs.create_file(
                &root,
                "KEEP",
                &mut &data[..],
                1500,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        cur.set_position(0);
        let mut fs = Ti99Filesystem::open(&mut cur, 0).unwrap();
        let root = fs.root().unwrap();
        let files = fs.list_directory(&root).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "KEEP");
        assert_eq!(files[0].size, 1500);
        assert_eq!(fs.read_file(&files[0], usize::MAX).unwrap().len(), 1500);
    }

    #[test]
    fn oracle_validates_our_edits() {
        let Some(script) = oracle() else {
            eprintln!("skipping oracle_validates_our_edits: oracle unavailable");
            return;
        };
        if !have_python() {
            return;
        }
        let vol = std::env::temp_dir().join(format!("rb_ti99_edit_{}.dsk", std::process::id()));
        let volp = vol.to_str().unwrap();
        let py = |args: &[&str]| {
            Command::new("python3")
                .arg(&script)
                .args(args)
                .output()
                .expect("python3")
        };
        assert!(py(&["mkfs", volp, "dssd", "OVOL"]).status.success());
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            let mut fs = Ti99Filesystem::open(file, 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "ALPHA",
                &mut &[0x41u8; 400][..],
                400,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.create_file(
                &root,
                "BETA",
                &mut &[0x42u8; 5000][..],
                5000,
                &CreateFileOptions::default(),
            )
            .unwrap();
            let alpha = fs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "ALPHA")
                .unwrap();
            fs.rename(&root, &alpha, "GAMMA").unwrap();
            fs.sync_metadata().unwrap();
        }
        // The oracle's fsck must find our create + rename bitmap-consistent.
        let out = py(&["fsck", volp]);
        let clean = out.status.success();
        let log = String::from_utf8_lossy(&out.stdout).into_owned();
        let _ = std::fs::remove_file(&vol);
        assert!(clean, "oracle fsck flagged our edits:\n{log}");
    }

    #[test]
    fn imgtool_reads_our_writes() {
        if Command::new("imgtool").arg("--version").output().is_err() {
            eprintln!("skipping imgtool_reads_our_writes: imgtool unavailable");
            return;
        }
        let vol = std::env::temp_dir().join(format!("rb_ti99_wr_{}.dsk", std::process::id()));
        std::fs::write(&vol, blank_image(1, 40, 9, 1, "WRITTEN")).unwrap();
        let payload: Vec<u8> = (0..1000).map(|i| (i * 3) as u8).collect();
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&vol)
                .unwrap();
            let mut fs = Ti99Filesystem::open(file, 0).unwrap();
            let root = fs.root().unwrap();
            fs.create_file(
                &root,
                "MYPROG",
                &mut &payload[..],
                1000,
                &CreateFileOptions::default(),
            )
            .unwrap();
            fs.sync_metadata().unwrap();
        }
        // Real tool lists + extracts our written file (TIFILES: 128-byte header).
        let got = std::env::temp_dir().join(format!("rb_ti99_got_{}.tfi", std::process::id()));
        let dir = Command::new("imgtool")
            .args(["dir", "v9t9", vol.to_str().unwrap()])
            .output()
            .expect("imgtool");
        let listing = String::from_utf8_lossy(&dir.stdout).into_owned();
        let get = Command::new("imgtool")
            .args([
                "get",
                "v9t9",
                vol.to_str().unwrap(),
                "MYPROG",
                got.to_str().unwrap(),
            ])
            .output()
            .expect("imgtool");
        let extracted = std::fs::read(&got).unwrap_or_default();
        let _ = std::fs::remove_file(&vol);
        let _ = std::fs::remove_file(&got);
        assert!(
            listing.contains("MYPROG"),
            "imgtool dir missing our file:\n{listing}"
        );
        assert!(get.status.success(), "imgtool get failed");
        // Skip the 128-byte TIFILES header; the next 1000 bytes are our payload.
        assert!(
            extracted.len() >= 128 + 1000,
            "short extraction ({} bytes)",
            extracted.len()
        );
        assert_eq!(
            &extracted[128..128 + 1000],
            &payload[..],
            "imgtool read different bytes"
        );
    }

    // ---- Create-blank ----

    #[test]
    fn create_blank_geometry_by_size() {
        // Size selects the smallest covering geometry, capped at DSDD (360 KiB).
        let checks = [
            (90 * 1024, 360u16),     // SSSD
            (120 * 1024, 720),       // -> DSSD
            (180 * 1024, 720),       // DSSD
            (360 * 1024, 1440),      // DSDD
            (2 * 1024 * 1024, 1440), // capped
        ];
        for (req, want_sectors) in checks {
            let img = create_blank_ti99(req, "SZ").unwrap();
            assert_eq!(img.len() as u64, want_sectors as u64 * SECTOR, "size {req}");
            let fs = Ti99Filesystem::open(Cursor::new(img), 0).unwrap();
            assert_eq!(fs.geometry.total_sectors, want_sectors, "size {req}");
        }
    }

    #[test]
    fn create_blank_opens_and_edits() {
        let img = create_blank_ti99(90 * 1024, "TIDISK").unwrap();
        let mut fs = Ti99Filesystem::open(Cursor::new(img), 0).expect("open blank");
        assert_eq!(fs.volume_label(), Some("TIDISK"));
        assert_eq!(fs.total_size(), 360 * SECTOR);
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
        fs.create_file(
            &root,
            "X",
            &mut &[1u8; 500][..],
            500,
            &CreateFileOptions::default(),
        )
        .unwrap();
        assert_eq!(fs.list_directory(&root).unwrap().len(), 1);
    }

    #[test]
    fn oracle_and_imgtool_accept_our_blank() {
        // Our blank must fsck-clean in the oracle AND list in imgtool.
        let vol = std::env::temp_dir().join(format!("rb_ti99_blank_{}.dsk", std::process::id()));
        std::fs::write(&vol, create_blank_ti99(180 * 1024, "BLANKV").unwrap()).unwrap();
        let volp = vol.to_str().unwrap();
        let mut ok = true;
        if let Some(script) = oracle() {
            if have_python() {
                ok &= Command::new("python3")
                    .arg(&script)
                    .args(["fsck", volp])
                    .output()
                    .unwrap()
                    .status
                    .success();
            }
        }
        if Command::new("imgtool").arg("--version").output().is_ok() {
            let out = Command::new("imgtool")
                .args(["dir", "v9t9", volp])
                .output()
                .unwrap();
            ok &= out.status.success();
        }
        let _ = std::fs::remove_file(&vol);
        assert!(ok, "oracle/imgtool rejected our blank");
    }
}
