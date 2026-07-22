//! HPFS — OS/2 High Performance File System (read + browse).
//!
//! HPFS is the defining OS/2 filesystem (OS/2 1.2 → Warp 4, eComStation,
//! ArcaOS). It uses 512-byte sectors, little-endian fields, B-tree directories
//! (dnodes), and a B+ tree extent allocator (fnodes → anodes). Free space is
//! tracked by per-8-MiB-band bitmaps where **a set bit means free** (inverted
//! vs. most filesystems — a common bug source, shared with the Amiga FS).
//!
//! On-disk layout (partition-relative sectors):
//! - sector 0   boot block   ("HPFS    " sig @54, `sig_28h`=0x28 @38)
//! - sector 16  super block   magic `f995e849`, points at root fnode + bitmap
//!   directory + directory band
//! - sector 17  spare block   magic `f9911849`, dirty flag / hotfix / codepage
//! - free-space bitmaps: 4 sectors per band, set-bit = free, LE bit order
//! - directory band: preallocated dnode region + its own dnode bitmap
//! - dnode (4 sectors, magic `77e40aae`): B-tree directory node
//! - fnode (1 sector, magic `f7e40aae`): per-object allocation B+ tree + EAs
//! - anode (1 sector, magic `37e40aae`): overflow allocation B+ tree
//!
//! This module mirrors the Linux kernel `fs/hpfs` on-disk structures and is
//! cross-validated by the clean-room `scripts/hpfs-oracle.py`.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};

const SECTOR: u64 = 512;
const BAND_SECTORS: u32 = 0x4000; // 16384 sectors = 8 MiB per band

const SB_MAGIC: u32 = 0xF995_E849;
const SP_MAGIC: u32 = 0xF991_1849;
const DNODE_MAGIC: u32 = 0x77E4_0AAE;
const FNODE_MAGIC: u32 = 0xF7E4_0AAE;
const ANODE_MAGIC: u32 = 0x37E4_0AAE;

const FNODE_FLAG_DIR: u16 = 0x0100;

// dirent flags byte (offset 2)
const DE_FIRST: u8 = 0x01;
const DE_DOWN: u8 = 0x04;
const DE_LAST: u8 = 0x08;
// dirent attribute byte (offset 3) = DOS attributes
const AT_READONLY: u8 = 0x01;
const AT_HIDDEN: u8 = 0x02;
const AT_SYSTEM: u8 = 0x04;
const AT_DIRECTORY: u8 = 0x10;
const AT_ARCHIVE: u8 = 0x20;

const DNODE_BYTES: usize = 4 * SECTOR as usize; // 2048

#[inline]
fn u16le(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([b[off], b[off + 1]])
}

#[inline]
fn u32le(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

/// Trim trailing NUL/space padding from a fixed-width name field.
fn trim_label(b: &[u8]) -> String {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    let s = &b[..end];
    String::from_utf8_lossy(s).trim_end().to_string()
}

/// One decoded directory entry from a dnode.
#[derive(Clone)]
struct Dirent {
    name: String,
    fnode: u32,
    size: u32,
    attrib: u8,
    write_date: u32,
    down: u32,
    first: bool,
    last: bool,
}

/// A B+ tree leaf extent: `length` file sectors starting at file sector
/// `file_secno` map to disk sectors starting at `disk_secno`.
#[derive(Clone, Copy)]
struct Extent {
    file_secno: u32,
    length: u32,
    disk_secno: u32,
}

/// Structural detector: valid HPFS super block (sector 16) + spare block
/// (sector 17) magics. Type-0x07 partitions share NTFS/exFAT, so this is what
/// distinguishes HPFS from them.
pub fn looks_like_hpfs<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> bool {
    let mut buf = [0u8; SECTOR as usize];
    if reader
        .seek(SeekFrom::Start(partition_offset + 16 * SECTOR))
        .is_err()
        || reader.read_exact(&mut buf).is_err()
    {
        return false;
    }
    if u32le(&buf, 0) != SB_MAGIC {
        return false;
    }
    if reader
        .seek(SeekFrom::Start(partition_offset + 17 * SECTOR))
        .is_err()
        || reader.read_exact(&mut buf).is_err()
    {
        return false;
    }
    u32le(&buf, 0) == SP_MAGIC
}

pub struct HpfsFilesystem<R> {
    reader: R,
    partition_offset: u64,
    pub(crate) total_sectors: u32,
    pub(crate) root_fnode: u32,
    pub(crate) dirband_start: u32,
    pub(crate) dirband_size: u32,
    pub(crate) dmap: u32,
    /// Per-band free-space bitmap sector numbers (from the bitmap directory).
    pub(crate) band_bmp: Vec<u32>,
    volume_label: String,
    /// Free-sector count cached at open (refreshed by `free_space`).
    free_sectors: u64,
}

impl<R: Read + Seek> HpfsFilesystem<R> {
    pub fn open(mut reader: R, partition_offset: u64) -> Result<Self, FilesystemError> {
        if !looks_like_hpfs(&mut reader, partition_offset) {
            return Err(FilesystemError::InvalidData("not an HPFS volume".into()));
        }

        // Super block (sector 16).
        let sb = read_sector_at(&mut reader, partition_offset, 16)?;
        let root_fnode = u32le(&sb, 12);
        let total_sectors = u32le(&sb, 16);
        let bitmap_dir = u32le(&sb, 24);
        let dirband_size = u32le(&sb, 48);
        let dirband_start = u32le(&sb, 52);
        let dmap = u32le(&sb, 60);

        if total_sectors == 0 || total_sectors >= 0x8000_0000 {
            return Err(FilesystemError::InvalidData(
                "HPFS: invalid volume size".into(),
            ));
        }

        // Volume label from the boot block (sector 0, vol_label @ offset 43).
        let boot = read_sector_at(&mut reader, partition_offset, 0)?;
        let mut volume_label = trim_label(&boot[43..54]);
        if volume_label.is_empty() || volume_label == "NO NAME" {
            volume_label.clear();
        }

        // Bitmap directory: an array of per-band bitmap sector pointers. It
        // spans ceil(n_bands*4 / 512) sectors starting at `bitmap_dir`.
        let n_bands = total_sectors.div_ceil(BAND_SECTORS) as usize;
        let dir_sectors = (n_bands * 4).div_ceil(SECTOR as usize).max(1);
        let mut band_bmp = Vec::with_capacity(n_bands);
        for s in 0..dir_sectors {
            let buf = read_sector_at(&mut reader, partition_offset, bitmap_dir + s as u32)?;
            for i in 0..(SECTOR as usize / 4) {
                if band_bmp.len() >= n_bands {
                    break;
                }
                band_bmp.push(u32le(&buf, i * 4));
            }
        }

        let mut fs = HpfsFilesystem {
            reader,
            partition_offset,
            total_sectors,
            root_fnode,
            dirband_start,
            dirband_size,
            dmap,
            band_bmp,
            volume_label,
            free_sectors: 0,
        };
        fs.free_sectors = fs.count_free_sectors().unwrap_or(0);
        Ok(fs)
    }

    fn read_sectors(&mut self, sector: u32, count: u32) -> Result<Vec<u8>, FilesystemError> {
        let mut buf = vec![0u8; count as usize * SECTOR as usize];
        self.reader.seek(SeekFrom::Start(
            self.partition_offset + sector as u64 * SECTOR,
        ))?;
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Parse the dirents in one dnode (4 sectors) in on-disk (sorted) order.
    fn dnode_dirents(&mut self, dno: u32) -> Result<Vec<Dirent>, FilesystemError> {
        if dno < 0x12 || dno as u64 + 4 > self.total_sectors as u64 || dno & 3 != 0 {
            return Err(FilesystemError::Parse(format!("HPFS: bad dnode {dno:#x}")));
        }
        let d = self.read_sectors(dno, 4)?;
        if u32le(&d, 0) != DNODE_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "HPFS: bad dnode magic at {dno:#x}"
            )));
        }
        let first_free = u32le(&d, 4) as usize;
        if first_free > DNODE_BYTES {
            return Err(FilesystemError::Parse(format!(
                "HPFS: dnode {dno:#x} first_free {first_free} out of range"
            )));
        }
        let mut out = Vec::new();
        let mut off = 20usize;
        while off + 32 <= first_free {
            let length = u16le(&d, off) as usize;
            if length < 32 || length & 3 != 0 || off + length > DNODE_BYTES {
                break;
            }
            let flags = d[off + 2];
            let attrib = d[off + 3];
            let fnode = u32le(&d, off + 4);
            let write_date = u32le(&d, off + 8);
            let size = u32le(&d, off + 12);
            let namelen = d[off + 30] as usize;
            let mut down = 0u32;
            if flags & DE_DOWN != 0 && length >= 4 {
                down = u32le(&d, off + length - 4);
            }
            let name = if off + 31 + namelen <= DNODE_BYTES {
                String::from_utf8_lossy(&d[off + 31..off + 31 + namelen]).to_string()
            } else {
                String::new()
            };
            out.push(Dirent {
                name,
                fnode,
                size,
                attrib,
                write_date,
                down,
                first: flags & DE_FIRST != 0,
                last: flags & DE_LAST != 0,
            });
            off += length;
        }
        Ok(out)
    }

    /// In-order B-tree traversal of a directory's dnode tree, returning the
    /// real (non-sentinel) dirents. Guards against cyclic/duplicate dnode
    /// pointers on corrupt images.
    fn walk_dir(&mut self, root_dno: u32) -> Result<Vec<Dirent>, FilesystemError> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        self.walk_dnode(root_dno, &mut out, &mut seen, 0)?;
        Ok(out)
    }

    fn walk_dnode(
        &mut self,
        dno: u32,
        out: &mut Vec<Dirent>,
        seen: &mut HashSet<u32>,
        depth: u32,
    ) -> Result<(), FilesystemError> {
        if depth > 64 || !seen.insert(dno) {
            return Ok(()); // cycle / too deep — stop, fsck reports it
        }
        let dirents = self.dnode_dirents(dno)?;
        for de in dirents {
            if de.down != 0 {
                self.walk_dnode(de.down, out, seen, depth + 1)?;
            }
            if de.first || de.last {
                continue;
            }
            out.push(de);
        }
        Ok(())
    }

    /// Read an fnode's magic + file_size + leaf/anode-mapped extents.
    fn fnode_extents(&mut self, fnode: u32) -> Result<(u32, Vec<Extent>), FilesystemError> {
        if fnode < 0x12 || fnode as u64 >= self.total_sectors as u64 {
            return Err(FilesystemError::Parse(format!(
                "HPFS: bad fnode {fnode:#x}"
            )));
        }
        let f = self.read_sectors(fnode, 1)?;
        if u32le(&f, 0) != FNODE_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "HPFS: bad fnode magic at {fnode:#x}"
            )));
        }
        let file_size = u32le(&f, 160);
        let mut extents = Vec::new();
        let mut seen = HashSet::new();
        // btree header at offset 56: flags@56, n_free@60, n_used@61, first_free@62
        self.collect_extents(
            &f[56..],
            /*header base within buf*/ 64,
            &mut extents,
            &mut seen,
            0,
        )?;
        Ok((file_size, extents))
    }

    /// Recurse a B+ tree (fnode or anode). `hdr` is the 8-byte bplus_header
    /// slice; `nodes` are the entries immediately after it in the same buffer.
    /// For internal nodes we descend into anodes.
    fn collect_extents(
        &mut self,
        buf_from_header: &[u8],
        _nodes_off: usize,
        out: &mut Vec<Extent>,
        seen: &mut HashSet<u32>,
        depth: u32,
    ) -> Result<(), FilesystemError> {
        if depth > 32 {
            return Ok(());
        }
        let flags = buf_from_header[0];
        let n_used = buf_from_header[5] as usize;
        let internal = flags & 0x80 != 0;
        // Entries start 8 bytes into the header slice.
        let base = 8usize;
        if internal {
            for i in 0..n_used {
                let off = base + i * 8;
                if off + 8 > buf_from_header.len() {
                    break;
                }
                let down = u32le(buf_from_header, off + 4);
                if down < 0x12 || down as u64 >= self.total_sectors as u64 || !seen.insert(down) {
                    continue;
                }
                let a = self.read_sectors(down, 1)?;
                if u32le(&a, 0) != ANODE_MAGIC {
                    return Err(FilesystemError::Parse(format!(
                        "HPFS: bad anode magic at {down:#x}"
                    )));
                }
                // anode btree header at offset 12.
                self.collect_extents(&a[12..], 20, out, seen, depth + 1)?;
            }
        } else {
            for i in 0..n_used {
                let off = base + i * 12;
                if off + 12 > buf_from_header.len() {
                    break;
                }
                out.push(Extent {
                    file_secno: u32le(buf_from_header, off),
                    length: u32le(buf_from_header, off + 4),
                    disk_secno: u32le(buf_from_header, off + 8),
                });
            }
        }
        Ok(())
    }

    /// Stream a file's bytes (bounded by `max`) to `sink`, walking extents so
    /// no full copy is materialized.
    fn stream_file(
        &mut self,
        fnode: u32,
        max: u64,
        mut sink: impl FnMut(&[u8]) -> Result<(), FilesystemError>,
    ) -> Result<u64, FilesystemError> {
        let (file_size, mut extents) = self.fnode_extents(fnode)?;
        let want = (file_size as u64).min(max);
        extents.sort_by_key(|e| e.file_secno);
        let mut written = 0u64;
        for e in extents {
            if written >= want {
                break;
            }
            // Read the extent in bounded chunks to cap memory.
            let mut remaining = e.length;
            let mut disk = e.disk_secno;
            let mut file_off = e.file_secno as u64 * SECTOR;
            while remaining > 0 && written < want {
                let chunk = remaining.min(256); // 128 KiB
                if disk as u64 + chunk as u64 > self.total_sectors as u64 {
                    break;
                }
                let buf = self.read_sectors(disk, chunk)?;
                let avail = want.saturating_sub(file_off);
                let take = (buf.len() as u64).min(avail) as usize;
                if take > 0 {
                    sink(&buf[..take])?;
                    written += take as u64;
                }
                remaining -= chunk;
                disk += chunk;
                file_off += chunk as u64 * SECTOR;
            }
        }
        Ok(written)
    }

    /// Count free sectors = set bits across all band bitmaps.
    pub(crate) fn count_free_sectors(&mut self) -> Result<u64, FilesystemError> {
        let mut free = 0u64;
        let bands = self.band_bmp.clone();
        for base in bands {
            if base < 0x12 || base as u64 + 4 > self.total_sectors as u64 {
                continue;
            }
            let buf = self.read_sectors(base, 4)?;
            for chunk in buf.chunks_exact(4) {
                free += u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]).count_ones()
                    as u64;
            }
        }
        Ok(free)
    }

    fn dirent_to_entry(&self, de: &Dirent, parent_path: &str) -> FileEntry {
        let path = if parent_path == "/" {
            format!("/{}", de.name)
        } else {
            format!("{}/{}", parent_path, de.name)
        };
        let is_dir = de.attrib & AT_DIRECTORY != 0;
        let mut e = if is_dir {
            FileEntry::new_directory(de.name.clone(), path, de.fnode as u64)
        } else {
            FileEntry::new_file(de.name.clone(), path, de.size as u64, de.fnode as u64)
        };
        // DOS attribute bits (excluding directory, which entry_type carries).
        let mut dos = 0u16;
        if de.attrib & AT_READONLY != 0 {
            dos |= 0x01;
        }
        if de.attrib & AT_HIDDEN != 0 {
            dos |= 0x02;
        }
        if de.attrib & AT_SYSTEM != 0 {
            dos |= 0x04;
        }
        if de.attrib & AT_ARCHIVE != 0 {
            dos |= 0x20;
        }
        e.dos_attributes = Some(dos);
        e.modified = format_hpfs_date(de.write_date);
        e
    }

    /// Resolve the directory fnode → its root dnode.
    fn dir_root_dnode(&mut self, fnode: u32) -> Result<u32, FilesystemError> {
        let f = self.read_sectors(fnode, 1)?;
        if u32le(&f, 0) != FNODE_MAGIC {
            return Err(FilesystemError::Parse(format!(
                "HPFS: bad dir fnode magic at {fnode:#x}"
            )));
        }
        if u16le(&f, 54) & FNODE_FLAG_DIR == 0 {
            return Err(FilesystemError::NotADirectory(format!("fnode {fnode:#x}")));
        }
        // external[0].disk_secno @ offset 72.
        Ok(u32le(&f, 72))
    }
}

/// Format an HPFS timestamp (seconds since 1970, local) as an ISO-ish string.
fn format_hpfs_date(secs: u32) -> Option<String> {
    if secs == 0 {
        return None;
    }
    // Minimal civil-date conversion (UTC) — good enough for display.
    let days = secs / 86400;
    let rem = secs % 86400;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // days since 1970-01-01 -> y/m/d (Howard Hinnant's algorithm)
    let z = days as i64 + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    Some(format!("{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}"))
}

fn read_sector_at<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    sector: u32,
) -> Result<[u8; SECTOR as usize], FilesystemError> {
    let mut buf = [0u8; SECTOR as usize];
    reader.seek(SeekFrom::Start(partition_offset + sector as u64 * SECTOR))?;
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

impl<R: Read + Seek + Send> Filesystem for HpfsFilesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        // Root directory entry carries the root fnode as its location so
        // list_directory can find the tree.
        Ok(FileEntry::new_directory(
            "/".into(),
            "/".into(),
            self.root_fnode as u64,
        ))
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        if !entry.is_directory() {
            return Err(FilesystemError::NotADirectory(entry.path.clone()));
        }
        let fnode = if entry.path == "/" {
            self.root_fnode
        } else {
            entry.location as u32
        };
        let root_dno = self.dir_root_dnode(fnode)?;
        let dirents = self.walk_dir(root_dno)?;
        Ok(dirents
            .iter()
            .map(|de| self.dirent_to_entry(de, &entry.path))
            .collect())
    }

    fn read_file(
        &mut self,
        entry: &FileEntry,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "HPFS read_file on directory: {}",
                entry.path
            )));
        }
        let mut out = Vec::new();
        self.stream_file(entry.location as u32, max_bytes as u64, |chunk| {
            out.extend_from_slice(chunk);
            Ok(())
        })?;
        Ok(out)
    }

    fn write_file_to(
        &mut self,
        entry: &FileEntry,
        writer: &mut dyn Write,
    ) -> Result<u64, FilesystemError> {
        if entry.is_directory() {
            return Err(FilesystemError::InvalidData(format!(
                "HPFS write_file_to on directory: {}",
                entry.path
            )));
        }
        self.stream_file(entry.location as u32, u64::MAX, |chunk| {
            writer.write_all(chunk).map_err(FilesystemError::Io)
        })
    }

    fn volume_label(&self) -> Option<&str> {
        if self.volume_label.is_empty() {
            None
        } else {
            Some(&self.volume_label)
        }
    }

    fn fs_type(&self) -> &str {
        "HPFS"
    }

    fn total_size(&self) -> u64 {
        self.total_sectors as u64 * SECTOR
    }

    fn used_size(&self) -> u64 {
        (self.total_sectors as u64).saturating_sub(self.free_sectors) * SECTOR
    }

    fn allocation_unit(&self) -> Option<u64> {
        Some(SECTOR)
    }

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        validate_hpfs_name(name)
    }

    fn fsck(&mut self) -> Option<Result<super::fsck::FsckResult, FilesystemError>> {
        Some(self.run_fsck())
    }
}

// =============================== create ===============================

/// Directory-entry attribute helpers for building on-disk dirents.
#[inline]
fn put_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

#[inline]
fn sector_mut(img: &mut [u8], s: u32) -> &mut [u8] {
    let o = s as usize * SECTOR as usize;
    &mut img[o..o + SECTOR as usize]
}

/// Fixed reproducible timestamp for freshly-formatted volumes (the harness
/// forbids wall-clock reads in generators). 2004-01-10, arbitrary but valid.
const FIXED_TIME: u32 = 0x4000_0000;

/// Round-up-to-4 dirent length = round_up(31 + namelen, 4) + (down ? 4 : 0).
fn de_size(namelen: usize, down: bool) -> usize {
    ((0x1f + namelen + 3) & !3) + if down { 4 } else { 0 }
}

/// Build an on-disk dirent (without the down pointer set; caller adds it).
fn make_dirent(name: &[u8], fnode: u32, size: u32, attrib: u8) -> Vec<u8> {
    let length = de_size(name.len(), false);
    let mut de = vec![0u8; length];
    put_u16(&mut de, 0, length as u16);
    de[2] = 0; // flags
    de[3] = attrib | if is_name_long(name) { 0x40 } else { 0 };
    put_u32(&mut de, 4, fnode);
    put_u32(&mut de, 8, FIXED_TIME); // write_date
    put_u32(&mut de, 12, size); // file_size
    put_u32(&mut de, 16, FIXED_TIME); // read_date
    put_u32(&mut de, 20, FIXED_TIME); // creation_date
    de[30] = name.len() as u8;
    de[31..31 + name.len()].copy_from_slice(name);
    de
}

/// Mirror of kernel `hpfs_is_name_long` — sets the not-8.3 dirent flag.
fn is_name_long(name: &[u8]) -> bool {
    let n = name.len();
    let mut i = 0;
    while i < n && name[i] != b'.' {
        if matches!(name[i], b'+' | b',' | b';' | b'=' | b'[' | b']') {
            return true;
        }
        i += 1;
    }
    if i == 0 || i > 8 {
        return true;
    }
    if i == n {
        return false;
    }
    for &c in &name[i + 1..] {
        if c == b'.' || matches!(name[i], b'+' | b',' | b';' | b'=' | b'[' | b']') {
            return true;
        }
    }
    (n - i) > 4
}

/// Format a blank HPFS volume of `size_bytes` (>= 2 MiB), returning the image
/// bytes. Layout is byte-identical to `scripts/hpfs-oracle.py mkfs` so the two
/// implementations cross-validate.
pub fn create_blank_hpfs(size_bytes: u64, label: &str) -> Result<Vec<u8>, FilesystemError> {
    let ts = (size_bytes / SECTOR) as u32;
    if (ts as u64) < 2048 {
        return Err(FilesystemError::InvalidData(
            "HPFS volume must be at least 1 MiB".into(),
        ));
    }
    let n_bands = ts.div_ceil(BAND_SECTORS) as usize;

    // Bump-allocate metadata sectors from 0x14.
    let mut cur = 0x14u32;
    let bitmap_dir = cur;
    cur += 4;
    let mut band_bmp = Vec::with_capacity(n_bands);
    for _ in 0..n_bands {
        band_bmp.push(cur);
        cur += 4;
    }
    let dmap = cur;
    cur += 4;
    let user_id = cur;
    cur += 8;
    let hotfix = cur;
    cur += 4;
    let root_fnode = cur;
    cur += 1;

    let dnodes = (ts / 32 / 4).clamp(8, 0x1000);
    let dirband_size = dnodes * 4;
    cur = (cur + 3) & !3;
    let dirband_start = cur;
    let dirband_end = dirband_start + dirband_size - 1;
    cur += dirband_size;
    if cur > ts {
        return Err(FilesystemError::InvalidData(
            "HPFS volume too small for metadata".into(),
        ));
    }
    let root_dno = dirband_start;

    let mut img = vec![0u8; ts as usize * SECTOR as usize];

    // ---- band bitmaps (bit set = free): start all-free, then mark used ----
    let mut bands: Vec<Vec<u8>> = (0..n_bands)
        .map(|_| vec![0xffu8; 4 * SECTOR as usize])
        .collect();
    let set_used = |bands: &mut Vec<Vec<u8>>, s: u32| {
        let band = (s >> 14) as usize;
        let idx = ((s & 0x3fff) >> 5) as usize;
        let bit = s & 0x1f;
        let base = idx * 4;
        let mut w = u32::from_le_bytes([
            bands[band][base],
            bands[band][base + 1],
            bands[band][base + 2],
            bands[band][base + 3],
        ]);
        w &= !(1u32 << bit);
        bands[band][base..base + 4].copy_from_slice(&w.to_le_bytes());
    };
    for s in ts..(n_bands as u32 * BAND_SECTORS) {
        set_used(&mut bands, s);
    }
    for s in 0..0x14u32 {
        set_used(&mut bands, s);
    }
    for s in bitmap_dir..bitmap_dir + 4 {
        set_used(&mut bands, s);
    }
    for &base in &band_bmp {
        for s in base..base + 4 {
            set_used(&mut bands, s);
        }
    }
    for s in dmap..dmap + 4 {
        set_used(&mut bands, s);
    }
    for s in user_id..user_id + 8 {
        set_used(&mut bands, s);
    }
    for s in hotfix..hotfix + 4 {
        set_used(&mut bands, s);
    }
    set_used(&mut bands, root_fnode);
    for s in dirband_start..dirband_start + dirband_size {
        set_used(&mut bands, s);
    }

    // ---- dnode bitmap (bit set = free), 1 bit / dnode ----
    let mut dmap_buf = vec![0u8; 4 * SECTOR as usize];
    for d in 0..dnodes {
        let idx = (d >> 5) as usize;
        let mut w = u32::from_le_bytes([
            dmap_buf[idx * 4],
            dmap_buf[idx * 4 + 1],
            dmap_buf[idx * 4 + 2],
            dmap_buf[idx * 4 + 3],
        ]);
        w |= 1u32 << (d & 0x1f);
        dmap_buf[idx * 4..idx * 4 + 4].copy_from_slice(&w.to_le_bytes());
    }
    // root dnode (index 0) is used.
    dmap_buf[0] &= !1u8;

    // ---- boot block ----
    {
        let b = sector_mut(&mut img, 0);
        b[0..3].copy_from_slice(&[0xeb, 0x3c, 0x90]);
        b[3..11].copy_from_slice(b"IBM 4.50");
        put_u16(b, 11, SECTOR as u16);
        b[13] = 1;
        put_u16(b, 19, if ts < 0x10000 { ts as u16 } else { 0 });
        b[21] = 0xf8;
        put_u16(b, 24, 63);
        put_u16(b, 26, 16);
        put_u32(b, 32, if ts >= 0x10000 { ts } else { 0 });
        b[36] = 0x80;
        b[38] = 0x28; // sig_28h (HPFS marker)
        put_u32(b, 39, 0x1234_5678);
        let mut lbl = [b' '; 11];
        let lb = label.as_bytes();
        let n = lb.len().min(11);
        lbl[..n].copy_from_slice(&lb[..n]);
        b[43..54].copy_from_slice(&lbl);
        b[54..62].copy_from_slice(b"HPFS    ");
        put_u16(b, 510, 0xaa55);
    }

    // ---- super block ----
    {
        let b = sector_mut(&mut img, 16);
        put_u32(b, 0, SB_MAGIC);
        put_u32(b, 4, 0xFA53_E9C5);
        b[8] = 2; // version
        b[9] = 2; // funcversion
        put_u32(b, 12, root_fnode);
        put_u32(b, 16, ts);
        put_u32(b, 24, bitmap_dir);
        put_u32(b, 48, dirband_size);
        put_u32(b, 52, dirband_start);
        put_u32(b, 56, dirband_end);
        put_u32(b, 60, dmap);
        put_u32(b, 96, user_id);
    }

    // ---- spare block ----
    {
        let b = sector_mut(&mut img, 17);
        put_u32(b, 0, SP_MAGIC);
        put_u32(b, 4, 0xFA52_29C5);
        put_u32(b, 12, hotfix);
        // all counts zero (clean, no hotfixes/spares/codepages)
    }

    // ---- bitmap directory ----
    {
        let b = sector_mut(&mut img, bitmap_dir);
        for (i, &bm) in band_bmp.iter().enumerate() {
            put_u32(b, i * 4, bm);
        }
    }

    // ---- flush band bitmaps + dnode bitmap ----
    for (i, &base) in band_bmp.iter().enumerate() {
        let o = base as usize * SECTOR as usize;
        img[o..o + 4 * SECTOR as usize].copy_from_slice(&bands[i]);
    }
    {
        let o = dmap as usize * SECTOR as usize;
        img[o..o + 4 * SECTOR as usize].copy_from_slice(&dmap_buf);
    }

    // ---- root fnode (directory) ----
    {
        let b = sector_mut(&mut img, root_fnode);
        put_u32(b, 0, FNODE_MAGIC);
        put_u32(b, 28, root_fnode); // up = self
        put_u16(b, 54, FNODE_FLAG_DIR);
        b[60] = 7; // n_free_nodes
        b[61] = 1; // n_used_nodes
        put_u16(b, 62, 0x14); // first_free
        put_u32(b, 64, 0xFFFF_FFFF); // external[0].file_secno = -1
        put_u32(b, 72, root_dno); // external[0].disk_secno
        put_u16(b, 184, 0xC4); // ea_offs
    }

    // ---- root dnode: \001\001 (first) + \377 (last) ----
    {
        let de1 = {
            let mut de = make_dirent(b"\x01\x01", root_fnode, 0, AT_DIRECTORY);
            de[2] = DE_FIRST; // flags: first
            de
        };
        let mut off = 20usize;
        let b = sector_mut(&mut img, root_dno);
        put_u32(b, 0, DNODE_MAGIC);
        b[8] = 0x01; // root_dnode
        put_u32(b, 12, root_fnode); // up
        put_u32(b, 16, root_dno); // self
        b[off..off + de1.len()].copy_from_slice(&de1);
        off += de1.len();
        // \377 last entry
        put_u16(b, off, 32);
        b[off + 2] = DE_LAST;
        b[off + 30] = 1;
        b[off + 31] = 0xff;
        off += 32;
        put_u32(b, 4, off as u32); // first_free
    }

    Ok(img)
}

// =============================== fsck ===============================

impl<R: Read + Seek> HpfsFilesystem<R> {
    fn run_fsck(&mut self) -> Result<super::fsck::FsckResult, FilesystemError> {
        use super::fsck::{FsckIssue, FsckResult, FsckStats};
        let mut errors = Vec::new();
        let warnings = Vec::new();
        let mut files_checked = 0u32;
        let mut dirs_checked = 0u32;

        let err = |code: &str, msg: String| FsckIssue {
            code: code.into(),
            message: msg,
            repairable: false,
            debug: false,
        };

        // Dir band consistency (kernel checks these on mount).
        if self.dirband_size > 0x4000 {
            errors.push(err(
                "DirBandTooLarge",
                format!("directory band size {} exceeds 0x4000", self.dirband_size),
            ));
        }

        // Walk the directory tree, tracking which sectors each object owns to
        // detect cross-links (a sector claimed twice).
        let mut owned: HashSet<u32> = HashSet::new();
        let claim = |errors: &mut Vec<FsckIssue>, owned: &mut HashSet<u32>, s: u32, what: &str| {
            if !owned.insert(s) {
                errors.push(FsckIssue {
                    code: "CrossLink".into(),
                    message: format!("sector {s:#x} used by more than one object ({what})"),
                    repairable: false,
                    debug: false,
                });
            }
        };

        // Recursive descent using an explicit stack of (dir_fnode, path).
        let mut stack = vec![(self.root_fnode, String::from("/"))];
        let mut visited_dirs = HashSet::new();
        let mut dnode_starts: Vec<u32> = Vec::new();
        while let Some((dir_fnode, path)) = stack.pop() {
            if !visited_dirs.insert(dir_fnode) {
                errors.push(err(
                    "DirCycle",
                    format!("directory fnode {dir_fnode:#x} reached twice ({path})"),
                ));
                continue;
            }
            dirs_checked += 1;
            claim(&mut errors, &mut owned, dir_fnode, "dir fnode");
            let root_dno = match self.dir_root_dnode(dir_fnode) {
                Ok(d) => d,
                Err(e) => {
                    errors.push(err("BadDirFnode", format!("{path}: {e}")));
                    continue;
                }
            };
            // Claim all dnodes of this directory + validate their structure.
            let mut dstack = vec![root_dno];
            let mut dseen = HashSet::new();
            while let Some(dno) = dstack.pop() {
                if !dseen.insert(dno) {
                    continue;
                }
                dnode_starts.push(dno);
                claim(&mut errors, &mut owned, dno, "dnode");
                claim(&mut errors, &mut owned, dno + 1, "dnode");
                claim(&mut errors, &mut owned, dno + 2, "dnode");
                claim(&mut errors, &mut owned, dno + 3, "dnode");
                match self.validate_dnode(dno) {
                    Ok(children) => dstack.extend(children),
                    Err(e) => errors.push(err("BadDnode", format!("{path}: {e}"))),
                }
            }
            // Enumerate entries.
            let dirents = match self.walk_dir(root_dno) {
                Ok(d) => d,
                Err(e) => {
                    errors.push(err("DirWalk", format!("{path}: {e}")));
                    continue;
                }
            };
            for de in dirents {
                let child_path = if path == "/" {
                    format!("/{}", de.name)
                } else {
                    format!("{}/{}", path, de.name)
                };
                if de.attrib & AT_DIRECTORY != 0 {
                    stack.push((de.fnode, child_path));
                } else {
                    files_checked += 1;
                    claim(&mut errors, &mut owned, de.fnode, "file fnode");
                    // Validate extents are in range + claim data sectors.
                    match self.fnode_extents(de.fnode) {
                        Ok((_size, extents)) => {
                            for e in extents {
                                if e.disk_secno as u64 + e.length as u64 > self.total_sectors as u64
                                {
                                    errors.push(err(
                                        "ExtentOutOfRange",
                                        format!(
                                            "{child_path}: extent {:#x}+{} exceeds volume",
                                            e.disk_secno, e.length
                                        ),
                                    ));
                                } else {
                                    for s in e.disk_secno..e.disk_secno + e.length {
                                        claim(&mut errors, &mut owned, s, "file data");
                                    }
                                }
                            }
                        }
                        Err(e) => errors.push(err("BadFileFnode", format!("{child_path}: {e}"))),
                    }
                }
            }
        }

        // Bitmap consistency: every owned sector must be marked used (bit 0)
        // in the free-space bitmap.
        let mut allocated_but_free = 0u32;
        for &s in &owned {
            if self.bitmap_is_free(s)? {
                allocated_but_free += 1;
            }
        }
        if allocated_but_free > 0 {
            errors.push(err(
                "AllocatedButFree",
                format!("{allocated_but_free} in-use sector(s) marked free in the bitmap"),
            ));
        }

        // Dnode-bitmap consistency: every in-use dnode inside the directory
        // band must be marked used (bit clear) in the dnode bitmap.
        let band_lo = self.dirband_start;
        let band_hi = self.dirband_start + self.dirband_size;
        let mut dnode_free_mismatch = 0u32;
        for &dno in &dnode_starts {
            if dno >= band_lo && dno < band_hi && self.dnode_bitmap_is_free(dno)? {
                dnode_free_mismatch += 1;
            }
        }
        if dnode_free_mismatch > 0 {
            errors.push(err(
                "DnodeMarkedFree",
                format!("{dnode_free_mismatch} in-use dnode(s) marked free in the dnode bitmap"),
            ));
        }

        let free = self.count_free_sectors()?;
        let stats = FsckStats {
            files_checked,
            directories_checked: dirs_checked,
            extra: vec![
                ("free_sectors".into(), free.to_string()),
                (
                    "used_sectors".into(),
                    (self.total_sectors as u64 - free).to_string(),
                ),
            ],
        };

        Ok(FsckResult {
            errors,
            warnings,
            stats,
            repairable: false,
            orphaned_entries: Vec::new(),
        })
    }

    /// Validate one dnode's structure (mirrors kernel `hpfs_map_dnode` checks)
    /// and return its child dnode pointers.
    fn validate_dnode(&mut self, dno: u32) -> Result<Vec<u32>, FilesystemError> {
        if dno & 3 != 0 {
            return Err(FilesystemError::Parse(format!(
                "dnode {dno:#x} not 4-sector aligned"
            )));
        }
        let d = self.read_sectors(dno, 4)?;
        if u32le(&d, 0) != DNODE_MAGIC {
            return Err(FilesystemError::Parse(format!("dnode {dno:#x} bad magic")));
        }
        if u32le(&d, 16) != dno {
            return Err(FilesystemError::Parse(format!(
                "dnode {dno:#x} bad self pointer"
            )));
        }
        let first_free = u32le(&d, 4) as usize;
        if first_free > DNODE_BYTES {
            return Err(FilesystemError::Parse(format!(
                "dnode {dno:#x} first_free {first_free} > 2048"
            )));
        }
        let mut children = Vec::new();
        let mut off = 20usize;
        let mut last_off = 20usize;
        while off < first_free {
            let length = u16le(&d, off) as usize;
            if !(32..=292).contains(&length) || length & 3 != 0 || off + length > DNODE_BYTES {
                return Err(FilesystemError::Parse(format!(
                    "dnode {dno:#x} bad dirent size at +{off:#x}"
                )));
            }
            let namelen = d[off + 30] as usize;
            let down = if d[off + 2] & DE_DOWN != 0 { 4 } else { 0 };
            if ((0x1f + namelen + down + 3) & !3) != length {
                return Err(FilesystemError::Parse(format!(
                    "dnode {dno:#x} namelen/size mismatch at +{off:#x}"
                )));
            }
            if down != 0 {
                children.push(u32le(&d, off + length - 4));
            }
            last_off = off;
            off += length;
        }
        if off != first_free {
            return Err(FilesystemError::Parse(format!(
                "dnode {dno:#x} dirents don't reach first_free"
            )));
        }
        // Must end with the \377 sentinel.
        if d[last_off + 30] != 1 || d[last_off + 31] != 0xff {
            return Err(FilesystemError::Parse(format!(
                "dnode {dno:#x} does not end with \\377 entry"
            )));
        }
        Ok(children)
    }

    fn bitmap_is_free(&mut self, s: u32) -> Result<bool, FilesystemError> {
        let band = (s >> 14) as usize;
        if band >= self.band_bmp.len() {
            return Ok(false);
        }
        let base = self.band_bmp[band];
        let word_idx = ((s & 0x3fff) >> 5) as usize;
        let buf = self.read_sectors(base, 4)?;
        let w = u32le(&buf, word_idx * 4);
        Ok((w >> (s & 0x1f)) & 1 == 1)
    }

    /// True when the dnode at sector `dno` (dir-band relative) has its bit set
    /// (free) in the dnode bitmap. `dno` must be within the directory band.
    fn dnode_bitmap_is_free(&mut self, dno: u32) -> Result<bool, FilesystemError> {
        let d = (dno - self.dirband_start) / 4;
        let buf = self.read_sectors(self.dmap, 4)?;
        let w = u32le(&buf, (d as usize >> 5) * 4);
        Ok((w >> (d & 0x1f)) & 1 == 1)
    }
}

/// Validate a filename against HPFS rules (mirrors kernel `hpfs_chk_name`).
pub fn validate_hpfs_name(name: &str) -> Result<(), FilesystemError> {
    if name.is_empty() {
        return Err(FilesystemError::InvalidData("name cannot be empty".into()));
    }
    // OS/2 strips trailing dots/spaces; a name that's all dots/spaces is empty.
    let trimmed = name.trim_end_matches(['.', ' ']);
    let effective = if name == "." || name == ".." {
        name
    } else {
        trimmed
    };
    if effective.is_empty() {
        return Err(FilesystemError::InvalidData("name cannot be empty".into()));
    }
    if effective.len() > 254 {
        return Err(FilesystemError::InvalidData(
            "name too long (max 254)".into(),
        ));
    }
    if effective == "." || effective == ".." {
        return Err(FilesystemError::InvalidData(
            "'.' and '..' are reserved".into(),
        ));
    }
    for c in effective.bytes() {
        if c < 0x20
            || matches!(
                c,
                b'"' | b'*' | b'/' | b':' | b'<' | b'>' | b'?' | b'\\' | b'|'
            )
        {
            return Err(FilesystemError::InvalidData(format!(
                "illegal character {:?} in HPFS name",
                c as char
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::process::Command;

    fn oracle() -> Option<PathBuf> {
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/hpfs-oracle.py");
        p.exists().then_some(p)
    }

    fn have_python() -> bool {
        Command::new("python3").arg("--version").output().is_ok()
    }

    fn run_oracle(args: &[&str]) -> Option<std::process::Output> {
        let script = oracle()?;
        if !have_python() {
            return None;
        }
        Command::new("python3")
            .arg(&script)
            .args(args)
            .output()
            .ok()
    }

    /// Build a populated HPFS volume with the clean-room oracle. Returns the
    /// image bytes and the temp paths (kept alive by the caller via drop).
    fn oracle_volume() -> Option<Vec<u8>> {
        let dir = std::env::temp_dir().join(format!("rb_hpfs_src_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("DOCS")).ok()?;
        std::fs::create_dir_all(dir.join("SUB/NESTED")).ok()?;
        std::fs::write(dir.join("README.TXT"), b"Hello from OS/2 HPFS!\n").ok()?;
        std::fs::write(dir.join("CONFIG.SYS"), b"REM config\n").ok()?;
        std::fs::write(dir.join("DOCS/BIG.DAT"), vec![0x5Au8; 9000]).ok()?;
        std::fs::write(dir.join("SUB/NESTED/DEEP.TXT"), b"deep\n").ok()?;
        let img = std::env::temp_dir().join(format!("rb_hpfs_{}.img", std::process::id()));
        let ok = run_oracle(&["build", img.to_str()?, "4", dir.to_str()?])
            .map(|o| o.status.success())
            .unwrap_or(false);
        let bytes = if ok { std::fs::read(&img).ok() } else { None };
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_file(&img);
        bytes
    }

    /// Recursively collect "path -> size" for files, and dir paths.
    fn collect(
        fs: &mut HpfsFilesystem<Cursor<Vec<u8>>>,
        entry: &FileEntry,
        files: &mut Vec<(String, u64)>,
        dirs: &mut Vec<String>,
    ) {
        for e in fs.list_directory(entry).unwrap() {
            if e.is_directory() {
                dirs.push(e.path.clone());
                collect(fs, &e, files, dirs);
            } else {
                files.push((e.path.clone(), e.size));
            }
        }
    }

    #[test]
    fn reads_oracle_volume() {
        let Some(img) = oracle_volume() else {
            eprintln!("skipping reads_oracle_volume: python3/oracle unavailable");
            return;
        };
        let mut fs = HpfsFilesystem::open(Cursor::new(img), 0).expect("open oracle HPFS volume");
        assert_eq!(fs.fs_type(), "HPFS");
        let root = fs.root().unwrap();
        let mut files = Vec::new();
        let mut dirs = Vec::new();
        collect(&mut fs, &root, &mut files, &mut dirs);
        files.sort();
        dirs.sort();
        assert_eq!(
            dirs,
            vec!["/DOCS", "/SUB", "/SUB/NESTED"],
            "dirs = {dirs:?}"
        );
        assert_eq!(
            files,
            vec![
                ("/CONFIG.SYS".to_string(), 11),
                ("/DOCS/BIG.DAT".to_string(), 9000),
                ("/README.TXT".to_string(), 22),
                ("/SUB/NESTED/DEEP.TXT".to_string(), 5),
            ],
            "files = {files:?}"
        );

        // File contents byte-exact.
        let root_list = fs.list_directory(&root).unwrap();
        let readme = root_list.iter().find(|e| e.name == "README.TXT").unwrap();
        assert_eq!(
            fs.read_file(readme, usize::MAX).unwrap(),
            b"Hello from OS/2 HPFS!\n"
        );
        let docs = root_list.iter().find(|e| e.name == "DOCS").unwrap();
        let big = fs
            .list_directory(docs)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "BIG.DAT")
            .unwrap();
        let data = fs.read_file(&big, usize::MAX).unwrap();
        assert_eq!(data.len(), 9000);
        assert!(data.iter().all(|&b| b == 0x5A));
    }

    #[test]
    fn fsck_oracle_volume_is_clean() {
        let Some(img) = oracle_volume() else {
            eprintln!("skipping fsck_oracle_volume_is_clean: python3/oracle unavailable");
            return;
        };
        let mut fs = HpfsFilesystem::open(Cursor::new(img), 0).unwrap();
        let result = fs.fsck().unwrap().unwrap();
        assert!(
            result.is_clean(),
            "expected clean fsck, got errors: {:?}",
            result.errors
        );
        assert!(result.stats.files_checked >= 4);
        assert!(result.stats.directories_checked >= 4); // root + 3 dirs
    }

    #[test]
    fn create_blank_roundtrips() {
        let img = create_blank_hpfs(4 * 1024 * 1024, "BLANKVOL").unwrap();
        let mut fs = HpfsFilesystem::open(Cursor::new(img), 0).unwrap();
        assert_eq!(fs.volume_label(), Some("BLANKVOL"));
        assert_eq!(fs.total_size(), 4 * 1024 * 1024);
        let root = fs.root().unwrap();
        assert!(fs.list_directory(&root).unwrap().is_empty());
        let result = fs.fsck().unwrap().unwrap();
        assert!(result.is_clean(), "blank fsck errors: {:?}", result.errors);
        // used < total (metadata occupies some sectors)
        assert!(fs.used_size() > 0 && fs.used_size() < fs.total_size());
    }

    #[test]
    fn oracle_reads_our_blank() {
        let Some(script) = oracle() else { return };
        if !have_python() {
            return;
        }
        let img = create_blank_hpfs(4 * 1024 * 1024, "RUSTMADE").unwrap();
        let path = std::env::temp_dir().join(format!("rb_hpfs_blank_{}.img", std::process::id()));
        std::fs::write(&path, &img).unwrap();
        let fsck = Command::new("python3")
            .arg(&script)
            .args(["fsck", path.to_str().unwrap()])
            .output()
            .unwrap();
        let _ = std::fs::remove_file(&path);
        assert!(
            fsck.status.success(),
            "oracle fsck rejected our blank volume: {}",
            String::from_utf8_lossy(&fsck.stdout)
        );
    }

    #[test]
    fn rejects_non_hpfs() {
        let junk = vec![0u8; 4 * 1024 * 1024];
        assert!(HpfsFilesystem::open(Cursor::new(junk), 0).is_err());
    }

    #[test]
    fn name_validation() {
        assert!(validate_hpfs_name("FILE.TXT").is_ok());
        assert!(validate_hpfs_name("long name with spaces.doc").is_ok());
        assert!(validate_hpfs_name("").is_err());
        assert!(validate_hpfs_name(".").is_err());
        assert!(validate_hpfs_name("bad/slash").is_err());
        assert!(validate_hpfs_name("bad:colon").is_err());
    }
}
