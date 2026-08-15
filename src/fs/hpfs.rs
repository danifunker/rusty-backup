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

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
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
    /// Free-sector count (kept in sync with `bands`).
    free_sectors: u64,
    /// Per-band free-space bitmaps (2048 bytes each), cached for fast
    /// free-counting and in-place editing. Bit set = free.
    bands: Vec<Vec<u8>>,
    /// Directory-band dnode bitmap (2048 bytes; bit set = free).
    dmap_data: Vec<u8>,
    /// Band indices whose cached bitmap changed since the last flush.
    dirty_bands: HashSet<usize>,
    dmap_dirty: bool,
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

        // Cache the free-space bitmaps + the dnode bitmap for fast
        // free-counting and in-place editing.
        let mut bands = Vec::with_capacity(n_bands);
        for &base in &band_bmp {
            if base >= 0x12 && base as u64 + 4 <= total_sectors as u64 {
                bands.push(read_run(&mut reader, partition_offset, base, 4)?);
            } else {
                bands.push(vec![0u8; 4 * SECTOR as usize]); // treat as all-used
            }
        }
        let dmap_data = if dmap >= 0x12 && dmap as u64 + 4 <= total_sectors as u64 {
            read_run(&mut reader, partition_offset, dmap, 4)?
        } else {
            vec![0u8; 4 * SECTOR as usize]
        };
        let free_sectors = bands
            .iter()
            .flat_map(|b| b.chunks_exact(4))
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]).count_ones() as u64)
            .sum();

        Ok(HpfsFilesystem {
            reader,
            partition_offset,
            total_sectors,
            root_fnode,
            dirband_start,
            dirband_size,
            dmap,
            band_bmp,
            volume_label,
            free_sectors,
            bands,
            dmap_data,
            dirty_bands: HashSet::new(),
            dmap_dirty: false,
        })
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
    /// Free-sector count, maintained in sync with the cached `bands` bitmaps.
    pub(crate) fn count_free_sectors(&self) -> u64 {
        self.free_sectors
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
        // HPFS stores write_date as u32 Unix seconds directly (no encoding).
        if de.write_date != 0 {
            e.modified_unix = Some(de.write_date as u64);
        }
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

fn read_run<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    sector: u32,
    count: u32,
) -> Result<Vec<u8>, FilesystemError> {
    let mut buf = vec![0u8; count as usize * SECTOR as usize];
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
            if self.bitmap_is_free(s) {
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
            if dno >= band_lo && dno < band_hi && self.dnode_bitmap_is_free(dno) {
                dnode_free_mismatch += 1;
            }
        }
        if dnode_free_mismatch > 0 {
            errors.push(err(
                "DnodeMarkedFree",
                format!("{dnode_free_mismatch} in-use dnode(s) marked free in the dnode bitmap"),
            ));
        }

        let free = self.count_free_sectors();
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

    fn bitmap_is_free(&self, s: u32) -> bool {
        let band = (s >> 14) as usize;
        if band >= self.bands.len() {
            return false;
        }
        let word_idx = ((s & 0x3fff) >> 5) as usize;
        let w = u32le(&self.bands[band], word_idx * 4);
        (w >> (s & 0x1f)) & 1 == 1
    }

    /// True when the dnode at sector `dno` (dir-band relative) has its bit set
    /// (free) in the dnode bitmap. `dno` must be within the directory band.
    fn dnode_bitmap_is_free(&self, dno: u32) -> bool {
        let d = (dno - self.dirband_start) / 4;
        let w = u32le(&self.dmap_data, (d as usize >> 5) * 4);
        (w >> (d & 0x1f)) & 1 == 1
    }
}

// ============================= editing =============================

impl<R: Read + Write + Seek> HpfsFilesystem<R> {
    fn write_sectors(&mut self, sector: u32, data: &[u8]) -> Result<(), FilesystemError> {
        self.reader.seek(SeekFrom::Start(
            self.partition_offset + sector as u64 * SECTOR,
        ))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    // ---- free-space + dnode bitmap mutation (cached; flushed on sync) ----
    fn mark_used(&mut self, s: u32) {
        let band = (s >> 14) as usize;
        if band >= self.bands.len() {
            return;
        }
        let widx = ((s & 0x3fff) >> 5) as usize * 4;
        let mut w = u32le(&self.bands[band], widx);
        if (w >> (s & 0x1f)) & 1 == 1 {
            w &= !(1u32 << (s & 0x1f));
            put_u32(&mut self.bands[band], widx, w);
            self.dirty_bands.insert(band);
            self.free_sectors = self.free_sectors.saturating_sub(1);
        }
    }

    fn mark_free(&mut self, s: u32) {
        let band = (s >> 14) as usize;
        if band >= self.bands.len() {
            return;
        }
        let widx = ((s & 0x3fff) >> 5) as usize * 4;
        let mut w = u32le(&self.bands[band], widx);
        if (w >> (s & 0x1f)) & 1 == 0 {
            w |= 1u32 << (s & 0x1f);
            put_u32(&mut self.bands[band], widx, w);
            self.dirty_bands.insert(band);
            self.free_sectors += 1;
        }
    }

    fn dmap_set_used(&mut self, dno: u32) {
        let d = (dno - self.dirband_start) / 4;
        let widx = (d as usize >> 5) * 4;
        let mut w = u32le(&self.dmap_data, widx);
        w &= !(1u32 << (d & 0x1f));
        put_u32(&mut self.dmap_data, widx, w);
        self.dmap_dirty = true;
    }

    fn dmap_set_free(&mut self, dno: u32) {
        let d = (dno - self.dirband_start) / 4;
        let widx = (d as usize >> 5) * 4;
        let mut w = u32le(&self.dmap_data, widx);
        w |= 1u32 << (d & 0x1f);
        put_u32(&mut self.dmap_data, widx, w);
        self.dmap_dirty = true;
    }

    /// Allocate `n` contiguous free sectors near `near` (align 1). First-fit
    /// from `near` forward, then wrapping to the start of the data area.
    fn alloc_run(&mut self, near: u32, n: u32) -> Option<u32> {
        let start = near.max(0x14).min(self.total_sectors);
        for (lo, hi) in [(start, self.total_sectors), (0x14, start)] {
            let mut run_start = 0u32;
            let mut run_len = 0u32;
            for s in lo..hi {
                if self.bitmap_is_free(s) {
                    if run_len == 0 {
                        run_start = s;
                    }
                    run_len += 1;
                    if run_len == n {
                        for x in run_start..run_start + n {
                            self.mark_used(x);
                        }
                        return Some(run_start);
                    }
                } else {
                    run_len = 0;
                }
            }
        }
        None
    }

    /// Allocate a 4-sector dnode: prefer a free slot in the directory band
    /// (tracked by the dnode bitmap), else a 4-aligned run elsewhere.
    fn alloc_dnode(&mut self, near: u32) -> Option<u32> {
        let n_dnodes = self.dirband_size / 4;
        for d in 0..n_dnodes {
            let dno = self.dirband_start + d * 4;
            if self.dnode_bitmap_is_free(dno) && self.bitmap_is_free(dno) {
                // (dir-band sectors are marked used in the main bitmap already,
                //  but guard anyway) mark used in the dnode bitmap.
                self.dmap_set_used(dno);
                return Some(dno);
            }
            // Dir-band sector is normally allocated in the main bitmap; the
            // dnode bitmap is the real free indicator there.
            if self.dnode_bitmap_is_free(dno) {
                self.dmap_set_used(dno);
                return Some(dno);
            }
        }
        // Band full: allocate 4 contiguous, 4-aligned sectors elsewhere.
        let start = near.max(0x14);
        for (lo, hi) in [(start, self.total_sectors), (0x14, start)] {
            let mut s = lo.div_ceil(4) * 4;
            while s + 4 <= hi {
                if (s..s + 4).all(|x| self.bitmap_is_free(x)) {
                    for x in s..s + 4 {
                        self.mark_used(x);
                    }
                    return Some(s);
                }
                s += 4;
            }
        }
        None
    }

    fn free_run(&mut self, start: u32, n: u32) {
        for s in start..start + n {
            self.mark_free(s);
        }
    }

    fn free_dnode(&mut self, dno: u32) {
        if dno >= self.dirband_start && dno < self.dirband_start + self.dirband_size {
            self.dmap_set_free(dno);
        } else {
            self.free_run(dno, 4);
        }
    }

    fn read_dnode(&mut self, dno: u32) -> Result<Vec<u8>, FilesystemError> {
        self.read_sectors(dno, 4)
    }

    fn dnode_up(d: &[u8]) -> u32 {
        u32le(d, 12)
    }

    // ---- fnode writers ----
    fn write_file_fnode(
        &mut self,
        fno: u32,
        up: u32,
        name: &[u8],
        size: u32,
        extents: &[(u32, u32, u32)],
    ) -> Result<(), FilesystemError> {
        let mut f = vec![0u8; SECTOR as usize];
        put_u32(&mut f, 0, FNODE_MAGIC);
        let nm = &name[..name.len().min(15)];
        f[12] = nm.len() as u8;
        f[13..13 + nm.len()].copy_from_slice(nm);
        put_u32(&mut f, 28, up);
        put_u16(&mut f, 184, 0xC4);
        let n_used = extents.len();
        f[60] = (8 - n_used) as u8;
        f[61] = n_used as u8;
        put_u16(&mut f, 62, (8 + n_used * 12) as u16);
        for (i, &(fs, ln, ds)) in extents.iter().enumerate() {
            let b = 64 + i * 12;
            put_u32(&mut f, b, fs);
            put_u32(&mut f, b + 4, ln);
            put_u32(&mut f, b + 8, ds);
        }
        put_u32(&mut f, 160, size);
        self.write_sectors(fno, &f)
    }

    fn write_dir_fnode(
        &mut self,
        fno: u32,
        up: u32,
        name: &[u8],
        root_dno: u32,
    ) -> Result<(), FilesystemError> {
        let mut f = vec![0u8; SECTOR as usize];
        put_u32(&mut f, 0, FNODE_MAGIC);
        let nm = &name[..name.len().min(15)];
        f[12] = nm.len() as u8;
        f[13..13 + nm.len()].copy_from_slice(nm);
        put_u32(&mut f, 28, up);
        put_u16(&mut f, 54, FNODE_FLAG_DIR);
        f[60] = 7;
        f[61] = 1;
        put_u16(&mut f, 62, 0x14);
        put_u32(&mut f, 64, 0xFFFF_FFFF);
        put_u32(&mut f, 72, root_dno);
        put_u16(&mut f, 184, 0xC4);
        self.write_sectors(fno, &f)
    }

    fn set_dir_root_dnode(&mut self, dir_fnode: u32, new_dno: u32) -> Result<(), FilesystemError> {
        let mut f = self.read_sectors(dir_fnode, 1)?;
        put_u32(&mut f, 72, new_dno);
        self.write_sectors(dir_fnode, &f)
    }

    /// Set children's `up` pointers to `dno` after they were moved under it.
    fn fix_up_ptrs(&mut self, dno: u32) -> Result<(), FilesystemError> {
        let d = self.read_dnode(dno)?;
        let ff = u32le(&d, 4) as usize;
        let mut off = 20;
        let mut kids = Vec::new();
        while off < ff {
            let len = u16le(&d, off) as usize;
            if d[off + 2] & DE_DOWN != 0 {
                kids.push(u32le(&d, off + len - 4));
            }
            off += len;
        }
        for child in kids {
            let mut c = self.read_dnode(child)?;
            if u32le(&c, 12) != dno || c[8] & 1 != 0 {
                put_u32(&mut c, 12, dno);
                c[8] &= !1;
                self.write_sectors(child, &c[..DNODE_BYTES])?;
            }
        }
        Ok(())
    }

    /// Insert a dirent (name + metadata template) into a directory's dnode
    /// tree, descending to the correct leaf then splitting upward as needed.
    fn add_dirent(
        &mut self,
        dir_fnode: u32,
        name: &[u8],
        meta: &[u8],
    ) -> Result<(), FilesystemError> {
        use std::cmp::Ordering;
        let root_dno = self.dir_root_dnode(dir_fnode)?;
        // Descend to the leaf dnode where `name` belongs.
        let mut dno = root_dno;
        let mut guard = 0;
        loop {
            guard += 1;
            if guard > 10_000 {
                return Err(FilesystemError::Parse("HPFS: dnode descent cycle".into()));
            }
            let d = self.read_dnode(dno)?;
            let ff = u32le(&d, 4) as usize;
            let mut off = 20;
            let mut descend = None;
            let mut found_dup = false;
            while off < ff {
                let len = u16le(&d, off) as usize;
                let last = d[off + 2] & DE_LAST != 0;
                match compare_names(name, de_name_at(&d, off), last) {
                    Ordering::Equal => {
                        found_dup = true;
                        break;
                    }
                    Ordering::Less => {
                        if d[off + 2] & DE_DOWN != 0 {
                            descend = Some(u32le(&d, off + len - 4));
                        }
                        break;
                    }
                    Ordering::Greater => {}
                }
                off += len;
            }
            if found_dup {
                return Err(FilesystemError::AlreadyExists(
                    String::from_utf8_lossy(name).into_owned(),
                ));
            }
            match descend {
                Some(child) => dno = child,
                None => break,
            }
        }
        self.add_to_dnode(dir_fnode, dno, name.to_vec(), meta.to_vec(), 0)
    }

    /// Add a dirent to `dno`, splitting the dnode tree upward when full.
    /// Mirrors kernel `hpfs_add_to_dnode`.
    fn add_to_dnode(
        &mut self,
        dir_fnode: u32,
        mut dno: u32,
        mut name: Vec<u8>,
        mut meta: Vec<u8>,
        mut down_ptr: u32,
    ) -> Result<(), FilesystemError> {
        let mut guard = 0;
        loop {
            guard += 1;
            if guard > 10_000 {
                return Err(FilesystemError::Parse("HPFS: dnode split cycle".into()));
            }
            let mut d = self.read_dnode(dno)?;
            let ff = u32le(&d, 4) as usize;
            if ff + de_size(name.len(), down_ptr != 0) <= DNODE_BYTES {
                let off = dn_add_de(&mut d, &name, down_ptr);
                dn_copy_meta(&mut d, off, &meta);
                d.truncate(DNODE_BYTES);
                self.write_sectors(dno, &d)?;
                return Ok(());
            }

            // ---- split ----
            let d_up = Self::dnode_up(&d);
            let root_dnode = d[8] & 1 != 0;
            let mut nd = d.clone();
            let off = dn_add_de(&mut nd, &name, down_ptr);
            dn_copy_meta(&mut nd, off, &meta);
            let nd_ff = u32le(&nd, 4) as usize;
            let last_off = dn_last_off(&nd);
            let h = last_off / 2 + 10;

            let adno = self
                .alloc_dnode(d_up)
                .ok_or_else(|| FilesystemError::DiskFull("HPFS: no free dnode".into()))?;
            let mut ad = blank_dnode(adno, d_up);

            // Move dirents [20, median) into `ad`.
            let mut cur = 20;
            loop {
                let len = u16le(&nd, cur) as usize;
                if cur + len >= h {
                    break;
                }
                let child = if nd[cur + 2] & DE_DOWN != 0 {
                    u32le(&nd, cur + len - 4)
                } else {
                    0
                };
                let nm = de_name_at(&nd, cur).to_vec();
                let ao = dn_add_de(&mut ad, &nm, child);
                dn_copy_meta(&mut ad, ao, &nd[cur..]);
                cur += len;
            }
            // `cur` = median dirent (goes up to the parent).
            let med_len = u16le(&nd, cur) as usize;
            let med_name = de_name_at(&nd, cur).to_vec();
            let med_meta = nd[cur..cur + 30].to_vec();
            let med_down = if nd[cur + 2] & DE_DOWN != 0 {
                u32le(&nd, cur + med_len - 4)
            } else {
                0
            };
            dn_set_last_pointer(&mut ad, med_down);

            // Remaining dirents (after the median) become the right half `d`.
            let after = cur + med_len;
            let tail_len = nd_ff - after;
            let mut newd = vec![0u8; DNODE_BYTES];
            newd[..20].copy_from_slice(&nd[..20]);
            newd[20..20 + tail_len].copy_from_slice(&nd[after..nd_ff]);
            put_u32(&mut newd, 4, (20 + tail_len) as u32);
            d = newd;

            ad.truncate(DNODE_BYTES);
            self.write_sectors(adno, &ad)?;
            self.fix_up_ptrs(adno)?;

            if !root_dnode {
                self.write_sectors(dno, &d)?;
                name = med_name;
                meta = med_meta;
                down_ptr = adno;
                dno = d_up;
                continue;
            }

            // Root split: create a new root dnode `rd`.
            let rdno = self
                .alloc_dnode(d_up)
                .ok_or_else(|| FilesystemError::DiskFull("HPFS: no free dnode".into()))?;
            let mut rd = blank_dnode(rdno, d_up);
            rd[8] |= 1; // root_dnode
            self.set_dir_root_dnode(dir_fnode, rdno)?;
            put_u32(&mut d, 12, rdno);
            d[8] &= !1;
            put_u32(&mut ad, 12, rdno);
            ad[8] &= !1;
            self.write_sectors(adno, &ad[..DNODE_BYTES])?;
            self.write_sectors(dno, &d)?;
            dn_set_last_pointer(&mut rd, dno); // rd's rightmost child = right half
            rd.truncate(DNODE_BYTES);
            self.write_sectors(rdno, &rd)?;
            name = med_name;
            meta = med_meta;
            down_ptr = adno;
            dno = rdno;
        }
    }

    // ---- file/dir creation ----
    fn alloc_file_data(
        &mut self,
        near: u32,
        data: &mut dyn Read,
        size: u64,
    ) -> Result<Vec<(u32, u32, u32)>, FilesystemError> {
        if size == 0 {
            return Ok(Vec::new());
        }
        let n_sec = size.div_ceil(SECTOR) as u32;
        let mut extents = Vec::new();
        // Prefer a single contiguous run.
        let starts = if let Some(start) = self.alloc_run(near, n_sec) {
            vec![(start, n_sec)]
        } else {
            // Fragmented fallback: up to 8 runs (fnode leaf capacity).
            let mut runs = Vec::new();
            let mut remaining = n_sec;
            while remaining > 0 {
                let mut got = None;
                let mut take = remaining;
                while take > 0 {
                    if let Some(s) = self.alloc_run(near, take) {
                        got = Some((s, take));
                        break;
                    }
                    take -= 1;
                }
                let (s, t) = got.ok_or_else(|| {
                    FilesystemError::DiskFull("HPFS: not enough free space".into())
                })?;
                runs.push((s, t));
                remaining -= t;
                if runs.len() > 8 {
                    for (s, t) in &runs {
                        self.free_run(*s, *t);
                    }
                    return Err(FilesystemError::DiskFull(
                        "HPFS: file too fragmented (writer emits <=8 extents)".into(),
                    ));
                }
            }
            runs
        };
        // Write data into the runs.
        let mut file_sec = 0u32;
        let mut buf = vec![0u8; SECTOR as usize];
        for (start, len) in starts {
            for i in 0..len {
                buf.iter_mut().for_each(|b| *b = 0);
                let mut filled = 0;
                while filled < SECTOR as usize {
                    let r = data.read(&mut buf[filled..])?;
                    if r == 0 {
                        break;
                    }
                    filled += r;
                }
                self.write_sectors(start + i, &buf)?;
            }
            extents.push((file_sec, len, start));
            file_sec += len;
        }
        Ok(extents)
    }

    fn create_entry(
        &mut self,
        parent_fnode: u32,
        name: &str,
        is_dir: bool,
        data: &mut dyn Read,
        size: u64,
        mtime_secs: Option<u64>,
    ) -> Result<u32, FilesystemError> {
        validate_hpfs_name(name)?;
        let nb = name.as_bytes().to_vec();
        // Duplicate check up front (avoids leaking allocations on conflict).
        let root_dno = self.dir_root_dnode(parent_fnode)?;
        if self
            .walk_dir(root_dno)?
            .iter()
            .any(|de| compare_names(&nb, de.name.as_bytes(), false) == std::cmp::Ordering::Equal)
        {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }

        let mut attrib = if is_dir { AT_DIRECTORY } else { AT_ARCHIVE };
        if name.starts_with('.') {
            attrib |= AT_HIDDEN;
        }

        if is_dir {
            let fno = self
                .alloc_run(parent_fnode, 1)
                .ok_or_else(|| FilesystemError::DiskFull("HPFS: no free sector".into()))?;
            let dno = self
                .alloc_dnode(parent_fnode)
                .ok_or_else(|| FilesystemError::DiskFull("HPFS: no free dnode".into()))?;
            self.write_dir_fnode(fno, parent_fnode, &nb, dno)?;
            // Empty dir dnode: \001\001 (first) + \377.
            let mut d = blank_dnode(dno, fno);
            d[8] |= 1; // root_dnode
            let off = dn_add_de(&mut d, b"\x01\x01", 0);
            let mut m = de_meta_template(fno, 0, AT_DIRECTORY, mtime_secs);
            m[2] = DE_FIRST;
            dn_copy_meta(&mut d, off, &m);
            d.truncate(DNODE_BYTES);
            self.write_sectors(dno, &d)?;
            let meta = de_meta_template(fno, 0, attrib, mtime_secs);
            self.add_dirent(parent_fnode, &nb, &meta)?;
            Ok(fno)
        } else {
            let extents = self.alloc_file_data(parent_fnode, data, size)?;
            let fno = match self.alloc_run(parent_fnode, 1) {
                Some(f) => f,
                None => {
                    for (_, ln, ds) in &extents {
                        self.free_run(*ds, *ln);
                    }
                    return Err(FilesystemError::DiskFull("HPFS: no free sector".into()));
                }
            };
            self.write_file_fnode(fno, parent_fnode, &nb, size as u32, &extents)?;
            let meta = de_meta_template(fno, size as u32, attrib, mtime_secs);
            self.add_dirent(parent_fnode, &nb, &meta)?;
            Ok(fno)
        }
    }

    // ---- deletion ----
    /// Free a file's data extents (and any anode tree) plus its fnode.
    fn free_file_fnode(&mut self, fno: u32) -> Result<(), FilesystemError> {
        let (_, extents) = self.fnode_extents(fno)?;
        for e in extents {
            self.free_run(e.disk_secno, e.length);
        }
        // Free any anode sectors referenced by an internal fnode btree.
        let f = self.read_sectors(fno, 1)?;
        if f[56] & 0x80 != 0 {
            let n_used = f[61] as usize;
            for i in 0..n_used {
                let down = u32le(&f, 64 + i * 8 + 4);
                self.free_anode_tree(down)?;
            }
        }
        self.free_run(fno, 1);
        Ok(())
    }

    fn free_anode_tree(&mut self, ano: u32) -> Result<(), FilesystemError> {
        if ano < 0x12 || ano as u64 >= self.total_sectors as u64 {
            return Ok(());
        }
        let a = self.read_sectors(ano, 1)?;
        if u32le(&a, 0) != ANODE_MAGIC {
            return Ok(());
        }
        if a[12] & 0x80 != 0 {
            let n_used = a[13] as usize;
            for i in 0..n_used {
                let down = u32le(&a, 20 + i * 8 + 4);
                self.free_anode_tree(down)?;
            }
        }
        self.free_run(ano, 1);
        Ok(())
    }

    /// Free all dnodes of an (already-empty) directory tree.
    /// Mirrors kernel `hpfs_remove_dtree` for the common shapes.
    fn free_dtree(&mut self, root_dno: u32) -> Result<(), FilesystemError> {
        let mut stack = vec![root_dno];
        let mut seen = HashSet::new();
        while let Some(dno) = stack.pop() {
            if !seen.insert(dno) {
                continue;
            }
            let d = self.read_dnode(dno)?;
            let ff = u32le(&d, 4) as usize;
            let mut off = 20;
            while off < ff {
                let len = u16le(&d, off) as usize;
                if d[off + 2] & DE_DOWN != 0 {
                    stack.push(u32le(&d, off + len - 4));
                }
                off += len;
            }
            self.free_dnode(dno);
        }
        Ok(())
    }

    /// Locate the (dnode, offset) of `name` in a directory tree, or None.
    fn find_dirent_loc(
        &mut self,
        dir_fnode: u32,
        name: &[u8],
    ) -> Result<Option<(u32, usize)>, FilesystemError> {
        use std::cmp::Ordering;
        let mut dno = self.dir_root_dnode(dir_fnode)?;
        let mut guard = 0;
        loop {
            guard += 1;
            if guard > 10_000 {
                return Ok(None);
            }
            let d = self.read_dnode(dno)?;
            let ff = u32le(&d, 4) as usize;
            let mut off = 20;
            let mut descend = None;
            let mut hit = None;
            while off < ff {
                let len = u16le(&d, off) as usize;
                let last = d[off + 2] & DE_LAST != 0;
                match compare_names(name, de_name_at(&d, off), last) {
                    Ordering::Equal => {
                        hit = Some(off);
                        break;
                    }
                    Ordering::Less => {
                        if d[off + 2] & DE_DOWN != 0 {
                            descend = Some(u32le(&d, off + len - 4));
                        }
                        break;
                    }
                    Ordering::Greater => {}
                }
                off += len;
            }
            if let Some(off) = hit {
                return Ok(Some((dno, off)));
            }
            match descend {
                Some(child) => dno = child,
                None => return Ok(None),
            }
        }
    }

    /// Remove a dirent (identified by name) from a directory tree, rebalancing.
    /// Mirrors kernel `hpfs_remove_dirent` + `move_to_top` + delete-empty.
    fn remove_dirent(&mut self, dir_fnode: u32, name: &[u8]) -> Result<(), FilesystemError> {
        let Some((dno, off)) = self.find_dirent_loc(dir_fnode, name)? else {
            return Err(FilesystemError::NotFound(
                String::from_utf8_lossy(name).into_owned(),
            ));
        };
        let mut d = self.read_dnode(dno)?;
        let len = u16le(&d, off) as usize;
        let down = if d[off + 2] & DE_DOWN != 0 {
            u32le(&d, off + len - 4)
        } else {
            0
        };
        dn_delete_de(&mut d, off);
        self.write_sectors(dno, &d[..DNODE_BYTES])?;
        if down != 0 {
            let a = self.move_to_top(dir_fnode, dno, down)?;
            if a != 0 {
                self.delete_empty_dnode(dir_fnode, a)?;
            }
        } else {
            self.delete_empty_dnode(dir_fnode, dno)?;
        }
        Ok(())
    }

    /// Pull the largest dirent out of the `from` subtree up into `to`, filling
    /// the hole left by a deleted internal dirent. Returns the dnode the entry
    /// came from (to be checked for emptiness), or 0 on failure.
    /// Mirrors kernel `move_to_top`.
    fn move_to_top(&mut self, dir_fnode: u32, to: u32, from: u32) -> Result<u32, FilesystemError> {
        // Descend to the rightmost leaf of `from`.
        let mut dno = from;
        let mut guard = 0;
        loop {
            guard += 1;
            if guard > 10_000 {
                return Ok(0);
            }
            let d = self.read_dnode(dno)?;
            let last = dn_last_off(&d);
            if d[last + 2] & DE_DOWN == 0 {
                break;
            }
            dno = u32le(&d, last + u16le(&d, last) as usize - 4);
        }
        // Walk up while the current dnode has only the \377 sentinel.
        let mut d = self.read_dnode(dno)?;
        loop {
            let pre_last = dnode_pre_last_off(&d);
            if pre_last.is_some() {
                break;
            }
            // dnode holds only \377: free it and drop the parent's down-pointer.
            let up = Self::dnode_up(&d);
            self.free_dnode(dno);
            if up == to {
                return Ok(to);
            }
            let mut ud = self.read_dnode(up)?;
            let last = dn_last_off(&ud);
            if ud[last + 2] & DE_DOWN == 0 {
                return Ok(0);
            }
            let shrunk = u16le(&ud, last) - 4;
            put_u16(&mut ud, last, shrunk);
            ud[last + 2] &= !DE_DOWN;
            let ff2 = u32le(&ud, 4) - 4;
            put_u32(&mut ud, 4, ff2);
            self.write_sectors(up, &ud[..DNODE_BYTES])?;
            dno = up;
            d = self.read_dnode(dno)?;
        }
        // `d` (dno) has a real pre-last dirent; move it up to `to`.
        let de_off = dnode_pre_last_off(&d).unwrap();
        let de_len = u16le(&d, de_off) as usize;
        let nde = d[de_off..de_off + de_len].to_vec();
        let ddown = if d[de_off + 2] & DE_DOWN != 0 {
            u32le(&d, de_off + de_len - 4)
        } else {
            0
        };
        dn_delete_de(&mut d, de_off);
        let mut dv = d.clone();
        dn_set_last_pointer(&mut dv, ddown);
        dv.truncate(DNODE_BYTES);
        self.write_sectors(dno, &dv)?;
        // Re-insert into `to` with down_ptr = `from`.
        let nl = nde[30] as usize;
        let nname = nde[31..31 + nl].to_vec();
        self.add_to_dnode(dir_fnode, to, nname, nde, from)?;
        Ok(dno)
    }

    /// Collapse a dnode that has become (near) empty, mirroring the common
    /// paths of kernel `delete_empty_dnode`.
    fn delete_empty_dnode(&mut self, dir_fnode: u32, dno: u32) -> Result<(), FilesystemError> {
        let d = self.read_dnode(dno)?;
        let ff = u32le(&d, 4) as usize;
        if ff > 56 {
            return Ok(());
        }
        if ff != 52 && ff != 56 {
            return Ok(());
        }
        let root = d[8] & 1 != 0;
        let up = Self::dnode_up(&d);
        // The single remaining dirent is at offset 20 (the \377, possibly with
        // a down pointer to a lone child subtree).
        let first_len = u16le(&d, 20) as usize;
        let down = if d[20 + 2] & DE_DOWN != 0 {
            u32le(&d, 20 + first_len - 4)
        } else {
            0
        };
        self.free_dnode(dno);
        if root {
            // Root dnode emptied: promote its single child, or reset fnode.
            if down != 0 {
                let mut c = self.read_dnode(down)?;
                put_u32(&mut c, 12, up);
                c[8] |= 1;
                self.write_sectors(down, &c[..DNODE_BYTES])?;
                self.set_dir_root_dnode(dir_fnode, down)?;
            }
            return Ok(());
        }
        // Non-root: find and drop the parent's pointer to `dno`.
        let mut ud = self.read_dnode(up)?;
        let uff = u32le(&ud, 4) as usize;
        let mut off = 20;
        let mut found = None;
        while off < uff {
            let len = u16le(&ud, off) as usize;
            if ud[off + 2] & DE_DOWN != 0 && u32le(&ud, off + len - 4) == dno {
                found = Some(off);
                break;
            }
            off += len;
        }
        let Some(off) = found else {
            return Ok(());
        };
        let len = u16le(&ud, off) as usize;
        if down == 0 {
            // Drop the down-pointer from the parent's dirent.
            ud[off + 2] &= !DE_DOWN;
            put_u16(&mut ud, off, (len - 4) as u16);
            let new_ff = uff - 4;
            ud.copy_within(off + len..uff, off + len - 4);
            for b in &mut ud[new_ff..uff] {
                *b = 0;
            }
            put_u32(&mut ud, 4, new_ff as u32);
        } else {
            // Re-point the parent's dirent at the lone child, and fix its up.
            put_u32(&mut ud, off + len - 4, down);
            let mut c = self.read_dnode(down)?;
            put_u32(&mut c, 12, up);
            self.write_sectors(down, &c[..DNODE_BYTES])?;
        }
        self.write_sectors(up, &ud[..DNODE_BYTES])?;
        Ok(())
    }

    fn flush_bitmaps(&mut self) -> Result<(), FilesystemError> {
        let dirty: Vec<usize> = self.dirty_bands.iter().copied().collect();
        for band in dirty {
            let base = self.band_bmp[band];
            let data = self.bands[band].clone();
            self.write_sectors(base, &data)?;
        }
        self.dirty_bands.clear();
        if self.dmap_dirty {
            let data = self.dmap_data.clone();
            self.write_sectors(self.dmap, &data)?;
            self.dmap_dirty = false;
        }
        self.reader.flush().map_err(FilesystemError::Io)?;
        Ok(())
    }
}

/// The offset of the second-to-last dirent, or None if the dnode holds only
/// the `\377` sentinel. Mirrors kernel `dnode_pre_last_de`.
fn dnode_pre_last_off(d: &[u8]) -> Option<usize> {
    let ff = u32le(d, 4) as usize;
    let mut off = 20;
    let mut prev = None;
    let mut cur = None;
    while off < ff {
        prev = cur;
        cur = Some(off);
        off += u16le(d, off) as usize;
    }
    prev
}

impl<R: Read + Write + Seek + Send> super::filesystem::EditableFilesystem for HpfsFilesystem<R> {
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
        options: &super::filesystem::CreateFileOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let parent_fnode = if parent.path == "/" {
            self.root_fnode
        } else {
            parent.location as u32
        };
        let mtime = options.unix_times.map(|t| t.mtime_or_now());
        let fno = self.create_entry(parent_fnode, name, false, data, data_len, mtime)?;
        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{}", parent.path, name)
        };
        let mut fe = FileEntry::new_file(name.to_string(), path, data_len, fno as u64);
        fe.modified_unix = mtime;
        Ok(fe)
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &super::filesystem::CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        let parent_fnode = if parent.path == "/" {
            self.root_fnode
        } else {
            parent.location as u32
        };
        let mut empty = std::io::empty();
        let mtime = options.unix_times.map(|t| t.mtime_or_now());
        let fno = self.create_entry(parent_fnode, name, true, &mut empty, 0, mtime)?;
        let path = if parent.path == "/" {
            format!("/{name}")
        } else {
            format!("{}/{}", parent.path, name)
        };
        let mut fe = FileEntry::new_directory(name.to_string(), path, fno as u64);
        fe.modified_unix = mtime;
        Ok(fe)
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let parent_fnode = if parent.path == "/" {
            self.root_fnode
        } else {
            parent.location as u32
        };
        let fno = entry.location as u32;
        if entry.is_directory() {
            // Must be empty.
            let root_dno = self.dir_root_dnode(fno)?;
            if !self.walk_dir(root_dno)?.is_empty() {
                return Err(FilesystemError::InvalidData(format!(
                    "directory not empty: {}",
                    entry.path
                )));
            }
            self.free_dtree(root_dno)?;
            self.free_run(fno, 1);
        } else {
            self.free_file_fnode(fno)?;
        }
        self.remove_dirent(parent_fnode, entry.name.as_bytes())?;
        Ok(())
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.flush_bitmaps()
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_sectors * SECTOR)
    }
}

// ============================= edit helpers =============================

#[inline]
fn upcase(c: u8) -> u8 {
    if c.is_ascii_lowercase() {
        c - 0x20
    } else {
        c
    }
}

/// HPFS case-insensitive name comparison (mirrors kernel `hpfs_compare_names`).
/// `n2_last` is the `\377` sentinel, which sorts after every real name.
fn compare_names(n1: &[u8], n2: &[u8], n2_last: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if n2_last {
        return Ordering::Less;
    }
    let l = n1.len().min(n2.len());
    for i in 0..l {
        let (c1, c2) = (upcase(n1[i]), upcase(n2[i]));
        if c1 != c2 {
            return c1.cmp(&c2);
        }
    }
    n1.len().cmp(&n2.len())
}

/// dnode dirent name at offset `off`.
fn de_name_at(d: &[u8], off: usize) -> &[u8] {
    let nl = d[off + 30] as usize;
    &d[off + 31..off + 31 + nl]
}

/// The offset of the last dirent (the `\377` sentinel) in a dnode.
fn dn_last_off(d: &[u8]) -> usize {
    let ff = u32le(d, 4) as usize;
    let mut off = 20;
    let mut last = 20;
    while off < ff {
        last = off;
        off += u16le(d, off) as usize;
    }
    last
}

/// Insert a dirent (name + optional down pointer) into a dnode buffer in sorted
/// order, returning the new dirent's offset. Grows `d` if needed (used with an
/// over-2048 scratch buffer during splits); callers guaranteeing fit pass a
/// plain 2048-byte dnode. Metadata (fnode/dates/size/attr) is filled by a
/// subsequent [`dn_copy_meta`]. Mirrors kernel `hpfs_add_de`.
fn dn_add_de(d: &mut Vec<u8>, name: &[u8], down_ptr: u32) -> usize {
    use std::cmp::Ordering;
    let dsize = de_size(name.len(), down_ptr != 0);
    let ff = u32le(d, 4) as usize;
    let mut off = 20;
    while off < ff {
        let last = d[off + 2] & DE_LAST != 0;
        if compare_names(name, de_name_at(d, off), last) == Ordering::Less {
            break;
        }
        off += u16le(d, off) as usize;
    }
    if d.len() < ff + dsize {
        d.resize(ff + dsize, 0);
    }
    d.copy_within(off..ff, off + dsize);
    for b in &mut d[off..off + dsize] {
        *b = 0;
    }
    put_u16(d, off, dsize as u16);
    if down_ptr != 0 {
        put_u32(d, off + dsize - 4, down_ptr);
        d[off + 2] |= DE_DOWN;
    }
    d[off + 30] = name.len() as u8;
    d[off + 31..off + 31 + name.len()].copy_from_slice(name);
    if is_name_long(name) {
        d[off + 3] |= 0x40; // not_8x3
    }
    put_u32(d, 4, (ff + dsize) as u32);
    off
}

/// Copy a dirent's 28 metadata bytes (offsets 2..30 — flags/attr/fnode/dates/
/// size/ea) from `src[0..30]` into the dirent at `dst`, preserving the
/// destination's own `down` and `not_8x3` bits. Mirrors kernel `copy_de`.
fn dn_copy_meta(d: &mut [u8], dst: usize, src: &[u8]) {
    let down = d[dst + 2] & DE_DOWN;
    let not8 = d[dst + 3] & 0x40;
    d[dst + 2..dst + 30].copy_from_slice(&src[2..30]);
    d[dst + 2] = (d[dst + 2] & !DE_DOWN) | down;
    d[dst + 3] = (d[dst + 3] & !0x40) | not8;
}

/// Remove the dirent at `off` from a dnode, shifting later dirents down.
/// Mirrors kernel `hpfs_delete_de`.
fn dn_delete_de(d: &mut [u8], off: usize) {
    let len = u16le(d, off) as usize;
    let ff = u32le(d, 4) as usize;
    d.copy_within(off + len..ff, off);
    for b in &mut d[ff - len..ff] {
        *b = 0;
    }
    put_u32(d, 4, (ff - len) as u32);
}

/// Give the last (`\377`) dirent a down pointer (rightmost subtree child).
/// Mirrors kernel `set_last_pointer`.
fn dn_set_last_pointer(d: &mut Vec<u8>, ptr: u32) {
    if ptr == 0 {
        return;
    }
    let last = dn_last_off(d);
    let ff = u32le(d, 4) as usize;
    if d.len() < ff + 4 {
        d.resize(ff + 4, 0);
    }
    put_u16(d, last, 36);
    d[last + 2] |= DE_DOWN;
    put_u32(d, last + 32, ptr);
    put_u32(d, 4, (ff + 4) as u32);
}

/// Build a fresh (empty) dnode buffer: magic, `\377` sentinel, self, up.
/// Mirrors kernel `hpfs_alloc_dnode` initialization.
fn blank_dnode(dno: u32, up: u32) -> Vec<u8> {
    let mut d = vec![0u8; DNODE_BYTES];
    put_u32(&mut d, 0, DNODE_MAGIC);
    put_u32(&mut d, 4, 52);
    d[20] = 32; // \377 dirent length
    d[22] = DE_LAST;
    d[50] = 1; // namelen
    d[51] = 0xff; // name[0]
    put_u32(&mut d, 12, up);
    put_u32(&mut d, 16, dno);
    d
}

/// Build a 32-byte dirent metadata template for [`dn_copy_meta`].
/// `mtime_secs = Some(secs)` stamps that Unix time into the write/read/creation
/// date fields (HPFS already stores u32 Unix seconds, so no conversion needed);
/// `None` stamps the reproducible `FIXED_TIME` sentinel — matching the
/// generator-forbids-clocks convention this format harness uses.
fn de_meta_template(fnode: u32, size: u32, attrib: u8, mtime_secs: Option<u64>) -> [u8; 32] {
    let mut m = [0u8; 32];
    let stamp = mtime_secs
        .map(|s| s.min(u32::MAX as u64) as u32)
        .unwrap_or(FIXED_TIME);
    m[3] = attrib;
    put_u32(&mut m, 4, fnode);
    put_u32(&mut m, 8, stamp);
    put_u32(&mut m, 12, size);
    put_u32(&mut m, 16, stamp);
    put_u32(&mut m, 20, stamp);
    m
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
        // Per-call suffix, as in oracle_cross_check: both callers run in
        // parallel and a pid-only path had them clobbering each other's image.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rb_hpfs_src_{}_{}", std::process::id(), uniq));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("DOCS")).ok()?;
        std::fs::create_dir_all(dir.join("SUB/NESTED")).ok()?;
        std::fs::write(dir.join("README.TXT"), b"Hello from OS/2 HPFS!\n").ok()?;
        std::fs::write(dir.join("CONFIG.SYS"), b"REM config\n").ok()?;
        std::fs::write(dir.join("DOCS/BIG.DAT"), vec![0x5Au8; 9000]).ok()?;
        std::fs::write(dir.join("SUB/NESTED/DEEP.TXT"), b"deep\n").ok()?;
        let img = std::env::temp_dir().join(format!("rb_hpfs_{}_{}.img", std::process::id(), uniq));
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

    // ---- editing ----
    use super::super::filesystem::{CreateDirectoryOptions, CreateFileOptions, EditableFilesystem};

    fn blank_fs(mb: u64, label: &str) -> HpfsFilesystem<Cursor<Vec<u8>>> {
        let img = create_blank_hpfs(mb * 1024 * 1024, label).unwrap();
        HpfsFilesystem::open(Cursor::new(img), 0).unwrap()
    }

    fn mkfile(
        fs: &mut HpfsFilesystem<Cursor<Vec<u8>>>,
        parent: &FileEntry,
        name: &str,
        data: &[u8],
    ) {
        let mut cur = Cursor::new(data.to_vec());
        fs.create_file(
            parent,
            name,
            &mut cur,
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }

    /// Run the clean-room oracle over the current image bytes: fsck must pass,
    /// and every expected name must appear in the oracle's recursive listing.
    fn oracle_cross_check(img: &[u8], expected_names: &[&str]) {
        let Some(script) = oracle() else { return };
        if !have_python() {
            return;
        }
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("rb_hpfs_edit_{}_{}.img", std::process::id(), uniq));
        std::fs::write(&path, img).unwrap();
        let fsck = Command::new("python3")
            .arg(&script)
            .args(["fsck", path.to_str().unwrap()])
            .output()
            .unwrap();
        let ls = Command::new("python3")
            .arg(&script)
            .args(["ls", path.to_str().unwrap()])
            .output()
            .unwrap();
        let _ = std::fs::remove_file(&path);
        assert!(
            fsck.status.success(),
            "oracle fsck rejected our edited image: {}",
            String::from_utf8_lossy(&fsck.stdout)
        );
        let listing = String::from_utf8_lossy(&ls.stdout);
        for n in expected_names {
            assert!(
                listing.contains(&format!("\"{n}\"")),
                "oracle listing missing {n}\n{listing}"
            );
        }
    }

    #[test]
    fn edit_create_read_roundtrip() {
        let mut fs = blank_fs(4, "EDIT1");
        let root = fs.root().unwrap();
        mkfile(&mut fs, &root, "HELLO.TXT", b"hello hpfs\n");
        mkfile(&mut fs, &root, "BINARY.DAT", &vec![0xABu8; 5000]);
        let sub = fs
            .create_directory(&root, "SUBDIR", &CreateDirectoryOptions::default())
            .unwrap();
        mkfile(&mut fs, &sub, "NESTED.TXT", b"deep\n");
        fs.sync_metadata().unwrap();

        // Reopen from the edited bytes.
        let img = fs.reader.get_ref().clone();
        let mut fs2 = HpfsFilesystem::open(Cursor::new(img.clone()), 0).unwrap();
        let root2 = fs2.root().unwrap();
        let mut names: Vec<String> = fs2
            .list_directory(&root2)
            .unwrap()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["BINARY.DAT", "HELLO.TXT", "SUBDIR"]);
        let hello = fs2
            .list_directory(&root2)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "HELLO.TXT")
            .unwrap();
        assert_eq!(fs2.read_file(&hello, usize::MAX).unwrap(), b"hello hpfs\n");
        let subdir = fs2
            .list_directory(&root2)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "SUBDIR")
            .unwrap();
        let nested = fs2.list_directory(&subdir).unwrap();
        assert_eq!(nested.len(), 1);
        assert_eq!(nested[0].name, "NESTED.TXT");

        assert!(fs2.fsck().unwrap().unwrap().is_clean());
        oracle_cross_check(&img, &["HELLO.TXT", "BINARY.DAT", "SUBDIR", "NESTED.TXT"]);
    }

    #[test]
    fn edit_forces_dnode_split() {
        // ~46 short-named dirents fill a 2048-byte dnode; 120 forces multiple
        // splits and a multi-level tree.
        let mut fs = blank_fs(8, "SPLIT");
        let root = fs.root().unwrap();
        for i in 0..120 {
            mkfile(
                &mut fs,
                &root,
                &format!("FILE{i:04}.TXT"),
                format!("body {i}\n").as_bytes(),
            );
        }
        fs.sync_metadata().unwrap();
        let img = fs.reader.get_ref().clone();

        let mut fs2 = HpfsFilesystem::open(Cursor::new(img.clone()), 0).unwrap();
        let root2 = fs2.root().unwrap();
        let listing = fs2.list_directory(&root2).unwrap();
        assert_eq!(listing.len(), 120, "expected 120 files after splits");
        // Spot-check contents survive the tree reshaping.
        let f77 = listing.iter().find(|e| e.name == "FILE0077.TXT").unwrap();
        assert_eq!(fs2.read_file(f77, usize::MAX).unwrap(), b"body 77\n");
        let result = fs2.fsck().unwrap().unwrap();
        assert!(result.is_clean(), "fsck after splits: {:?}", result.errors);

        // Oracle must read the split tree too.
        oracle_cross_check(&img, &["FILE0000.TXT", "FILE0077.TXT", "FILE0119.TXT"]);
    }

    #[test]
    fn edit_delete_files() {
        let mut fs = blank_fs(4, "DELF");
        let root = fs.root().unwrap();
        mkfile(&mut fs, &root, "KEEP1.TXT", b"a");
        mkfile(&mut fs, &root, "GONE.DAT", &vec![0x11u8; 3000]);
        mkfile(&mut fs, &root, "KEEP2.TXT", b"b");
        fs.sync_metadata().unwrap();
        let free_before = fs.free_space().unwrap();

        let root = fs.root().unwrap();
        let gone = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "GONE.DAT")
            .unwrap();
        fs.delete_entry(&root, &gone).unwrap();
        fs.sync_metadata().unwrap();

        // Freed the 3000-byte file's data + fnode.
        assert!(fs.free_space().unwrap() > free_before);
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        assert_eq!(names, vec!["KEEP1.TXT", "KEEP2.TXT"]);
        assert!(fs.fsck().unwrap().unwrap().is_clean());
        let img = fs.reader.get_ref().clone();
        oracle_cross_check(&img, &["KEEP1.TXT", "KEEP2.TXT"]);
    }

    #[test]
    fn edit_delete_with_rebalance() {
        // Build a multi-dnode tree, then delete most entries (exercising
        // move_to_top / delete_empty_dnode), and verify the survivors.
        let mut fs = blank_fs(8, "REBAL");
        let root = fs.root().unwrap();
        for i in 0..90 {
            mkfile(
                &mut fs,
                &root,
                &format!("R{i:03}.BIN"),
                format!("{i}").as_bytes(),
            );
        }
        // Delete every entry whose number is not a multiple of 7.
        let root = fs.root().unwrap();
        let victims: Vec<FileEntry> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .filter(|e| {
                let n: u32 = e.name[1..4].parse().unwrap();
                !n.is_multiple_of(7)
            })
            .collect();
        for v in &victims {
            fs.delete_entry(&root, v).unwrap();
        }
        fs.sync_metadata().unwrap();

        let mut survivors: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .iter()
            .map(|e| e.name.clone())
            .collect();
        survivors.sort();
        let expected: Vec<String> = (0u32..90)
            .filter(|n| n.is_multiple_of(7))
            .map(|n| format!("R{n:03}.BIN"))
            .collect();
        assert_eq!(survivors, expected, "survivors after rebalance");
        let result = fs.fsck().unwrap().unwrap();
        assert!(
            result.is_clean(),
            "fsck after rebalance: {:?}",
            result.errors
        );
        let img = fs.reader.get_ref().clone();
        oracle_cross_check(&img, &["R000.BIN", "R070.BIN"]);
    }

    #[test]
    fn edit_rejects_duplicate_and_bad_name() {
        let mut fs = blank_fs(4, "DUP");
        let root = fs.root().unwrap();
        mkfile(&mut fs, &root, "A.TXT", b"x");
        let mut cur = Cursor::new(b"y".to_vec());
        assert!(fs
            .create_file(&root, "A.TXT", &mut cur, 1, &CreateFileOptions::default())
            .is_err());
        let mut cur2 = Cursor::new(b"z".to_vec());
        assert!(fs
            .create_file(
                &root,
                "bad/name",
                &mut cur2,
                1,
                &CreateFileOptions::default()
            )
            .is_err());
    }
}
