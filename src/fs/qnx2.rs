//! QNX 2 filesystem (QNX 2.x, and the Unisys ICON's System Software and QDOS), read-only.
//!
//! The layout comes from Mitchell Schoenbrun's public-domain `q2files.c` /
//! `q2read.c` and was checked against the ICON floppies (800 KiB, 1600 blocks).
//! Everything is little-endian with 16-bit `int`s.
//!
//! - 512-byte blocks numbered from 1: block 1 is the first sector of the volume.
//!   Block 1 holds boot data and, at byte 68, the root directory's entry; the
//!   free-block bitmap is the root's `bitmap` file (one bit per block, LSB first,
//!   set = in use). Boot disks keep their loader in blocks 3 up to the root.
//! - Directory entry, 48 bytes: status +0 (`0x40` in use, `0x10` modified), first
//!   extent +1, last extent +5, blocks held minus one +9, extent count +13, member
//!   +15, group +16 (the owner is `group.member`), unused bytes in the last block
//!   +17, seconds +19 (0 on every ICON file), type +23, group and other
//!   permissions +24 / +25, owner attributes +26 (`0x20` = directory), a two-word
//!   date +27, a 16-character name + NUL +31.
//! - Attribute and permission bits: files `r w a e m` = `0x01 0x02 0x04 0x08 0x10`;
//!   directories `r c b m` = `0x01 0x04 0x08 0x10` (`c` and `b` inferred from the
//!   ICON disks, where every directory is owner `r c m`, others `r`).
//! - Date: word 0 is year-1980 in bits 15-12, month 11-8, PM 5, day 4-0; word 1 is
//!   seconds since 12 o'clock. The ICON clock wrote day 0 and the 31st of 30-day
//!   months around midnight, so the day counts on from the 1st of the month.
//! - A file is a chain of extents. Each extent's first block starts with
//!   `{u32 prev, u32 next, u32 size, u32 bound}`: `size` is the data bytes the
//!   extent holds (after the 16-byte header, running on into its following
//!   blocks) and `bound` the blocks it spans beyond the first. A file's length
//!   is the sum of its extents' `size`.
//! - A directory's data starts `{u16 parent_xtnt, u16 dir_index}` (plus a u16
//!   when `dir_index`'s top bit is set), then 48-byte entries. A deleted entry
//!   keeps its name and extents with the `0x40` status bit clear.
//! - Text files end lines with RS (`0x1E`); [`rs_to_lf`] converts them on extract.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom};

use super::entry::FileEntry;
use super::filesystem::{Filesystem, FilesystemError};
use super::fsck::{FsckIssue, FsckResult, FsckStats};

const BLOCK: u64 = 512;
const ROOT_AT: usize = 68;
const ENTRY: usize = 48;
const XTNT_HDR: u64 = 16;
const USED: u8 = 0x40;
const MODIFIED: u8 = 0x10;
const DIRECTORY: u8 = 0x20;
/// QNX 2's line separator.
pub const RS: u8 = 0x1E;
/// Longest chain a well-formed volume can hold; a loop or a wild pointer stops here.
const MAX_EXTENTS: usize = 1 << 16;
/// Suffix that marks a deleted entry in listings when deleted entries are shown.
const DELETED_TAG: &str = " [deleted]";

/// One decoded directory entry, in use or deleted.
#[derive(Debug, Clone)]
struct Dirent {
    status: u8,
    name: String,
    first: u32,
    blocks_less_one: u32,
    member: u8,
    group: u8,
    gperms: u8,
    perms: u8,
    attr: u8,
    date: [u16; 2],
}

impl Dirent {
    /// A named entry; slots with an empty name are unused.
    fn parse(b: &[u8]) -> Option<Self> {
        let d = Self::decode(b);
        (!d.name.is_empty()).then_some(d)
    }

    fn decode(b: &[u8]) -> Self {
        let raw = &b[31..47];
        let end = raw.iter().position(|&c| c == 0).unwrap_or(raw.len());
        let w = |o: usize| u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
        Self {
            status: b[0],
            name: String::from_utf8_lossy(&raw[..end]).into_owned(),
            first: w(1),
            blocks_less_one: w(9),
            member: b[15],
            group: b[16],
            gperms: b[24],
            perms: b[25],
            attr: b[26],
            date: [
                u16::from_le_bytes([b[27], b[28]]),
                u16::from_le_bytes([b[29], b[30]]),
            ],
        }
    }

    fn in_use(&self) -> bool {
        self.status & USED != 0
    }

    fn is_dir(&self) -> bool {
        self.attr & DIRECTORY != 0
    }
}

/// Decode an ICON date pair to Unix seconds (local time, as the ICON kept it).
pub fn decode_date(w0: u16, w1: u16) -> Option<i64> {
    use chrono::Datelike;
    let month = u32::from((w0 >> 8) & 15);
    if w0 == 0 || !(1..=12).contains(&month) || w1 >= 43200 {
        return None;
    }
    let first = chrono::NaiveDate::from_ymd_opt(1980 + i32::from(w0 >> 12), month, 1)?;
    // 719_163 is 1970-01-01 counted from 0001-01-01.
    let days = i64::from(first.num_days_from_ce()) - 719_163 + i64::from(w0 & 31) - 1;
    let pm = if w0 & 0x20 != 0 { 43200 } else { 0 };
    Some(days * 86400 + pm + i64::from(w1))
}

/// Unix permission bits for one class from QNX 2 `rwaem` / `rcbm` bits.
fn unix_class(bits: u8, dir: bool) -> u32 {
    let mut m = 0;
    if bits & 0x01 != 0 {
        m |= if dir { 0o5 } else { 0o4 };
    }
    if (dir && bits & 0x04 != 0) || (!dir && bits & 0x06 != 0) {
        m |= 0o2;
    }
    if !dir && bits & 0x08 != 0 {
        m |= 0o1;
    }
    m
}

/// The QNX 2 letters for one class: `rwaem` for files, `rcbm` for directories.
fn letters(bits: u8, dir: bool) -> String {
    let table: &[(u8, char)] = if dir {
        &[(0x01, 'r'), (0x04, 'c'), (0x08, 'b'), (0x10, 'm')]
    } else {
        &[
            (0x01, 'r'),
            (0x02, 'w'),
            (0x04, 'a'),
            (0x08, 'e'),
            (0x10, 'm'),
        ]
    };
    table
        .iter()
        .map(|&(bit, c)| if bits & bit != 0 { c } else { '-' })
        .collect()
}

/// Whether `data` reads as QNX 2 text: RS-separated lines, no NULs, hardly any control bytes.
pub fn looks_like_rs_text(data: &[u8]) -> bool {
    if !data.contains(&RS) || data.contains(&0) {
        return false;
    }
    let odd = data
        .iter()
        .filter(|&&b| b < 0x20 && !matches!(b, RS | b'\t' | b'\n' | b'\r' | 0x0C | 0x1B))
        .count();
    odd * 100 <= data.len()
}

/// Replace QNX 2's RS line separators with LF.
pub fn rs_to_lf(data: &[u8]) -> Vec<u8> {
    data.iter()
        .map(|&b| if b == RS { b'\n' } else { b })
        .collect()
}

/// Whether block 1 at `offset` carries a QNX 2 root entry pointing at a sane extent.
pub fn looks_like_qnx2<R: Read + Seek>(reader: &mut R, offset: u64) -> bool {
    let mut b1 = [0u8; BLOCK as usize];
    if reader.seek(SeekFrom::Start(offset)).is_err() || reader.read_exact(&mut b1).is_err() {
        return false;
    }
    let root = &b1[ROOT_AT..ROOT_AT + ENTRY];
    let first = u32::from_le_bytes([root[1], root[2], root[3], root[4]]) as u64;
    if root[0] & USED == 0
        || root[26] & DIRECTORY == 0
        || root[31] != 0
        || !(2..65536).contains(&first)
    {
        return false;
    }
    let mut hdr = [0u8; 16];
    if reader
        .seek(SeekFrom::Start(offset + (first - 1) * BLOCK))
        .is_err()
        || reader.read_exact(&mut hdr).is_err()
    {
        return false;
    }
    let word = |o: usize| u32::from_le_bytes([hdr[o], hdr[o + 1], hdr[o + 2], hdr[o + 3]]) as u64;
    let (prev, size, bound) = (word(0), word(8), word(12));
    prev == 0 && size >= 4 && bound < 4096 && size <= (bound + 1) * BLOCK - XTNT_HDR
}

/// One extent: first block, blocks spanned, data bytes, and the header's prev pointer.
#[derive(Debug, Clone, Copy)]
struct Extent {
    block: u64,
    span: u64,
    size: u64,
    prev: u64,
}

/// A QNX 2 volume, browsed read-only.
pub struct Qnx2Filesystem<R: Read + Seek + Send> {
    reader: R,
    offset: u64,
    total_blocks: u64,
    free_blocks: u64,
    root_first: u32,
    bitmap: Vec<u8>,
    show_deleted: bool,
}

impl<R: Read + Seek + Send> Qnx2Filesystem<R> {
    pub fn open(mut reader: R, offset: u64) -> Result<Self, FilesystemError> {
        if !looks_like_qnx2(&mut reader, offset) {
            return Err(FilesystemError::Parse("not a QNX 2 volume".into()));
        }
        let mut b1 = [0u8; BLOCK as usize];
        reader.seek(SeekFrom::Start(offset))?;
        reader.read_exact(&mut b1)?;
        let root = Dirent::decode(&b1[ROOT_AT..ROOT_AT + ENTRY]);
        let end = reader.seek(SeekFrom::End(0))?;
        let mut fs = Self {
            reader,
            offset,
            total_blocks: end.saturating_sub(offset) / BLOCK,
            free_blocks: 0,
            root_first: root.first,
            bitmap: Vec::new(),
            show_deleted: false,
        };
        // The bitmap file sizes the volume (one bit per block) and counts the free blocks.
        if let Ok(entries) = fs.read_dir(root.first) {
            if let Some(bm) = entries
                .iter()
                .find(|e| e.in_use() && e.name == "bitmap" && !e.is_dir())
            {
                if let Ok(map) = fs.read_chain(bm.first, usize::MAX) {
                    fs.total_blocks = (map.len() as u64 * 8).min(fs.total_blocks.max(1));
                    fs.free_blocks = map
                        .iter()
                        .map(|b| b.count_zeros() as u64)
                        .sum::<u64>()
                        .min(fs.total_blocks);
                    fs.bitmap = map;
                }
            }
        }
        Ok(fs)
    }

    /// Whether the bitmap marks block `n` in use; `None` past its end.
    fn marked(&self, n: u64) -> Option<bool> {
        let i = n.checked_sub(1)?;
        self.bitmap
            .get((i / 8) as usize)
            .map(|b| b >> (i % 8) & 1 != 0)
    }

    /// Every extent of the chain starting at `first`.
    fn extents(&mut self, first: u32) -> Result<Vec<Extent>, FilesystemError> {
        let mut out = Vec::new();
        let mut at = first as u64;
        let mut seen = HashSet::new();
        while at != 0 {
            if at > self.total_blocks.max(1) || !seen.insert(at) || out.len() >= MAX_EXTENTS {
                return Err(FilesystemError::InvalidData(format!(
                    "qnx2: extent chain breaks at block {at}"
                )));
            }
            let mut hdr = [0u8; 16];
            self.reader
                .seek(SeekFrom::Start(self.offset + (at - 1) * BLOCK))?;
            self.reader.read_exact(&mut hdr)?;
            let w =
                |o: usize| u32::from_le_bytes([hdr[o], hdr[o + 1], hdr[o + 2], hdr[o + 3]]) as u64;
            let (prev, next, size, bound) = (w(0), w(4), w(8), w(12));
            out.push(Extent {
                block: at,
                span: bound + 1,
                size: size.min((bound + 1) * BLOCK - XTNT_HDR),
                prev,
            });
            at = next;
        }
        Ok(out)
    }

    /// Up to `max` bytes of the file whose chain starts at `first`.
    fn read_chain(&mut self, first: u32, max: usize) -> Result<Vec<u8>, FilesystemError> {
        let mut out = Vec::new();
        for x in self.extents(first)? {
            if out.len() >= max {
                break;
            }
            let take = (x.size as usize).min(max - out.len());
            let start = out.len();
            out.resize(start + take, 0);
            self.reader.seek(SeekFrom::Start(
                self.offset + (x.block - 1) * BLOCK + XTNT_HDR,
            ))?;
            self.reader.read_exact(&mut out[start..])?;
        }
        Ok(out)
    }

    fn file_len(&mut self, first: u32) -> Result<u64, FilesystemError> {
        Ok(self.extents(first)?.iter().map(|e| e.size).sum())
    }

    /// Every named entry of a directory, deleted ones included.
    fn read_dir(&mut self, first: u32) -> Result<Vec<Dirent>, FilesystemError> {
        let data = self.read_chain(first, 1 << 22)?;
        if data.len() < 4 {
            return Ok(Vec::new());
        }
        let index = u16::from_le_bytes([data[2], data[3]]);
        let start = if index & 0x8000 != 0 { 6 } else { 4 };
        Ok(data[start..]
            .chunks_exact(ENTRY)
            .filter_map(Dirent::parse)
            .collect())
    }

    /// What became of a deleted entry's blocks, for its attribute line.
    fn deleted_state(&mut self, d: &Dirent) -> &'static str {
        match self.extents(d.first) {
            Err(_) => "deleted, chain lost",
            Ok(xs) => {
                let reused = xs
                    .iter()
                    .flat_map(|x| x.block..x.block + x.span)
                    .any(|b| self.marked(b) == Some(true));
                if reused {
                    "deleted, blocks reused"
                } else if xs.iter().all(|x| x.size == 0) {
                    "deleted, no data"
                } else {
                    "deleted, recoverable"
                }
            }
        }
    }

    fn entry_for(&mut self, parent: &str, d: &Dirent, name: String) -> FileEntry {
        let path = format!("{}/{}", parent.trim_end_matches('/'), name);
        let dir = d.is_dir();
        let mut e = if dir {
            FileEntry::new_directory(name, path, d.first as u64)
        } else {
            let len = self.file_len(d.first).unwrap_or(0);
            FileEntry::new_file(name, path, len, d.first as u64)
        };
        let kind = if dir { 0o040000 } else { 0o100000 };
        e.mode = Some(
            kind | unix_class(d.attr, dir) << 6
                | unix_class(d.gperms, dir) << 3
                | unix_class(d.perms, dir),
        );
        e.uid = Some(d.member as u32);
        e.gid = Some(d.group as u32);
        e.owner_label = Some(format!("{}.{}", d.group, d.member));
        if let Some(t) = decode_date(d.date[0], d.date[1]) {
            if let Ok(secs) = u64::try_from(t) {
                e.modified_unix = Some(secs);
                e.modified = Some(super::unix_common::inode::format_unix_timestamp(t));
            }
        }
        let mut attrs = format!(
            "owner {} group {} other {}",
            letters(d.attr, dir),
            letters(d.gperms, dir),
            letters(d.perms, dir)
        );
        if !d.in_use() {
            attrs = format!("{attrs}; {}", self.deleted_state(d));
        } else if d.status & MODIFIED != 0 {
            attrs.push_str("; modified");
        }
        let odd = d.status & !(USED | MODIFIED);
        if odd != 0 {
            attrs.push_str(&format!("; status 0x{odd:02X}"));
        }
        e.native_attrs = Some(attrs);
        e
    }

    /// Walk every reachable file, recording which blocks each holds.
    fn walk(
        &mut self,
        first: u32,
        path: &str,
        depth: usize,
        owners: &mut HashMap<u64, String>,
        report: &mut Walk,
    ) {
        self.claim(first, path, owners, report, None);
        let dirents = match self.read_dir(first) {
            Ok(d) => d,
            Err(e) => {
                report
                    .errors
                    .push(issue("UnreadableDir", format!("{path}: {e}")));
                return;
            }
        };
        report.dirs += 1;
        for d in dirents {
            let child = format!("{}/{}", path.trim_end_matches('/'), d.name);
            if !d.in_use() {
                let state = self.deleted_state(&d);
                report.deleted += 1;
                if state == "deleted, recoverable" {
                    report.recoverable += 1;
                }
                continue;
            }
            if d.is_dir() {
                if depth >= 64 {
                    report.errors.push(issue(
                        "DirTooDeep",
                        format!("{child}: directories nest past 64 levels"),
                    ));
                } else {
                    self.walk(d.first, &child, depth + 1, owners, report);
                }
            } else {
                report.files += 1;
                self.claim(d.first, &child, owners, report, Some(d.blocks_less_one));
            }
        }
    }

    /// Record one chain's blocks, reporting breaks, cross-links and unmarked blocks.
    fn claim(
        &mut self,
        first: u32,
        path: &str,
        owners: &mut HashMap<u64, String>,
        report: &mut Walk,
        blocks_less_one: Option<u32>,
    ) {
        let xs = match self.extents(first) {
            Ok(xs) => xs,
            Err(e) => {
                report
                    .errors
                    .push(issue("BrokenChain", format!("{path}: {e}")));
                return;
            }
        };
        let mut prev = 0;
        let mut held = 0u64;
        for x in &xs {
            if x.prev != prev {
                report.warnings.push(issue(
                    "ExtentLink",
                    format!(
                        "{path}: extent at block {} points back to {} (expected {prev})",
                        x.block, x.prev
                    ),
                ));
            }
            prev = x.block;
            for b in x.block..x.block + x.span {
                held += 1;
                if b > self.total_blocks {
                    report.errors.push(issue(
                        "BlockOutOfRange",
                        format!("{path}: block {b} is past the end of the volume"),
                    ));
                    continue;
                }
                if let Some(other) = owners.insert(b, path.to_string()) {
                    report.errors.push(issue(
                        "CrossLinked",
                        format!("block {b} is held by both {other} and {path}"),
                    ));
                }
                if self.marked(b) == Some(false) {
                    report.unmarked.push(b);
                }
            }
        }
        if let Some(n) = blocks_less_one {
            if u64::from(n) + 1 != held {
                report.warnings.push(issue(
                    "BlockCount",
                    format!(
                        "{path}: entry says {} blocks, its extents hold {held}",
                        u64::from(n) + 1
                    ),
                ));
            }
        }
    }
}

/// Findings gathered while walking the tree.
#[derive(Default)]
struct Walk {
    errors: Vec<FsckIssue>,
    warnings: Vec<FsckIssue>,
    unmarked: Vec<u64>,
    files: u32,
    dirs: u32,
    deleted: u32,
    recoverable: u32,
}

fn issue(code: &str, message: String) -> FsckIssue {
    FsckIssue {
        code: code.into(),
        message,
        repairable: false,
        debug: false,
    }
}

/// `1-3, 7, 9-12` for a sorted block list.
fn ranges(blocks: &[u64]) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    while i < blocks.len() {
        let mut j = i;
        while j + 1 < blocks.len() && blocks[j + 1] == blocks[j] + 1 {
            j += 1;
        }
        out.push(if i == j {
            blocks[i].to_string()
        } else {
            format!("{}-{}", blocks[i], blocks[j])
        });
        i = j + 1;
    }
    out.join(", ")
}

/// Check a QNX 2 volume: chains, cross-links, and the bitmap against the blocks files hold.
pub fn fsck_qnx2<R: Read + Seek + Send>(
    fs: &mut Qnx2Filesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let mut owners = HashMap::new();
    let mut walk = Walk::default();
    let root = fs.root_first;
    fs.walk(root, "/", 0, &mut owners, &mut walk);
    let mut errors = walk.errors;
    let mut warnings = walk.warnings;
    if fs.bitmap.is_empty() {
        warnings.push(issue(
            "NoBitmap",
            "the root has no readable bitmap file, so free space was not checked".into(),
        ));
    } else {
        walk.unmarked.sort_unstable();
        walk.unmarked.dedup();
        if !walk.unmarked.is_empty() {
            errors.push(issue(
                "UnmarkedBlocks",
                format!(
                    "{} blocks held by files are free in the bitmap: {}",
                    walk.unmarked.len(),
                    ranges(&walk.unmarked)
                ),
            ));
        }
        // Block 1 and a boot loader (blocks 3 up to the root) belong to no file.
        let reserved = |b: u64| b == 1 || (b >= 3 && b < u64::from(root));
        let orphans: Vec<u64> = (1..=fs.total_blocks)
            .filter(|&b| fs.marked(b) == Some(true) && !owners.contains_key(&b) && !reserved(b))
            .collect();
        // The 3.00 formatter marks the volume's last block in use; nothing is lost there.
        let (last, lost): (Vec<u64>, Vec<u64>) =
            orphans.into_iter().partition(|&b| b == fs.total_blocks);
        if !lost.is_empty() {
            warnings.push(issue(
                "OrphanedBlocks",
                format!(
                    "{} blocks are marked in use but no file holds them: {}",
                    lost.len(),
                    ranges(&lost)
                ),
            ));
        }
        if !last.is_empty() {
            let mut i = issue(
                "LastBlockReserved",
                format!("the last block ({}) is marked in use", fs.total_blocks),
            );
            i.debug = true;
            warnings.push(i);
        }
    }
    Ok(FsckResult {
        errors,
        warnings,
        stats: FsckStats {
            files_checked: walk.files,
            directories_checked: walk.dirs,
            extra: vec![
                ("Blocks".into(), fs.total_blocks.to_string()),
                ("Free blocks".into(), fs.free_blocks.to_string()),
                (
                    "Deleted entries".into(),
                    format!("{} ({} recoverable)", walk.deleted, walk.recoverable),
                ),
            ],
        },
        repairable: false,
        orphaned_entries: Vec::new(),
    })
}

impl<R: Read + Seek + Send> Filesystem for Qnx2Filesystem<R> {
    fn root(&mut self) -> Result<FileEntry, FilesystemError> {
        let mut root = FileEntry::root();
        root.location = self.root_first as u64;
        Ok(root)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let dirents = self.read_dir(entry.location as u32)?;
        let mut out = Vec::new();
        let mut taken: HashSet<String> = dirents
            .iter()
            .filter(|d| d.in_use())
            .map(|d| d.name.clone())
            .collect();
        for d in &dirents {
            if d.in_use() {
                out.push(self.entry_for(&entry.path, d, d.name.clone()));
            } else if self.show_deleted {
                let mut name = format!("{}{DELETED_TAG}", d.name);
                let mut n = 2;
                while !taken.insert(name.clone()) {
                    name = format!("{} [deleted {n}]", d.name);
                    n += 1;
                }
                out.push(self.entry_for(&entry.path, d, name));
            }
        }
        Ok(out)
    }

    fn read_file(&mut self, entry: &FileEntry, max: usize) -> Result<Vec<u8>, FilesystemError> {
        self.read_chain(entry.location as u32, max)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "QNX 2"
    }

    fn total_size(&self) -> u64 {
        self.total_blocks * BLOCK
    }

    fn used_size(&self) -> u64 {
        self.total_blocks.saturating_sub(self.free_blocks) * BLOCK
    }

    fn fsck(&mut self) -> Option<Result<FsckResult, FilesystemError>> {
        Some(fsck_qnx2(self))
    }

    fn set_show_deleted(&mut self, on: bool) -> bool {
        self.show_deleted = on;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A 16-block volume: root at block 3 holding `HELLO` (block 5, 6 bytes) and dir `sub` (block 4).
    fn sample() -> Vec<u8> {
        let mut img = vec![0u8; 16 * 512];
        let entry = |img: &mut Vec<u8>, at: usize, name: &[u8], first: u32, attr: u8| {
            img[at] = USED;
            img[at + 1..at + 5].copy_from_slice(&first.to_le_bytes());
            img[at + 26] = attr;
            img[at + 24] = 0x21;
            img[at + 25] = 0x01;
            img[at + 31..at + 31 + name.len()].copy_from_slice(name);
        };
        entry(&mut img, ROOT_AT, b"", 3, 0x25);
        let hdr = |img: &mut Vec<u8>, block: usize, size: u32| {
            let at = (block - 1) * 512;
            img[at + 8..at + 12].copy_from_slice(&size.to_le_bytes());
        };
        hdr(&mut img, 3, 4 + 2 * ENTRY as u32);
        let dir = 2 * 512 + 16 + 4;
        entry(&mut img, dir, b"HELLO", 5, 0x01);
        entry(&mut img, dir + ENTRY, b"sub", 4, 0x25);
        hdr(&mut img, 4, 4);
        hdr(&mut img, 5, 6);
        img[4 * 512 + 16..4 * 512 + 22].copy_from_slice(b"hello\n");
        img
    }

    /// `sample()` plus a deleted `GONE` (block 6) in the root and a bitmap file at block 2.
    fn sample_with_bitmap() -> Vec<u8> {
        let mut img = sample();
        let dir = 2 * 512 + 16 + 4;
        let at = dir + 2 * ENTRY;
        img[at + 1..at + 5].copy_from_slice(&6u32.to_le_bytes());
        img[at + 26] = 0x11;
        img[at + 31..at + 35].copy_from_slice(b"GONE");
        let at = dir + 3 * ENTRY;
        img[at] = USED;
        img[at + 1..at + 5].copy_from_slice(&2u32.to_le_bytes());
        img[at + 26] = 0x01;
        img[at + 31..at + 37].copy_from_slice(b"bitmap");
        img[2 * 512 + 8..2 * 512 + 12].copy_from_slice(&(4 + 4 * ENTRY as u32).to_le_bytes());
        img[512 + 8..512 + 12].copy_from_slice(&2u32.to_le_bytes());
        // Blocks 1-5 in use (bits 0-4); 6 (the deleted file) free.
        img[512 + 16] = 0x1F;
        img[5 * 512 + 8..5 * 512 + 12].copy_from_slice(&3u32.to_le_bytes());
        img[5 * 512 + 16..5 * 512 + 19].copy_from_slice(b"old");
        img
    }

    #[test]
    fn detects_and_lists_a_volume() {
        let img = sample();
        assert!(looks_like_qnx2(&mut Cursor::new(img.clone()), 0));
        let mut fs = Qnx2Filesystem::open(Cursor::new(img), 0).expect("open");
        let root = fs.root().unwrap();
        let list = fs.list_directory(&root).unwrap();
        let names: Vec<_> = list
            .iter()
            .map(|e| (e.name.as_str(), e.is_directory()))
            .collect();
        assert_eq!(names, [("HELLO", false), ("sub", true)]);
        assert_eq!(list[0].size, 6);
        assert_eq!(fs.read_file(&list[0], usize::MAX).unwrap(), b"hello\n");
        assert!(fs.list_directory(&list[1]).unwrap().is_empty());
    }

    #[test]
    fn rejects_fat_and_blank_disks() {
        assert!(!looks_like_qnx2(&mut Cursor::new(vec![0u8; 8192]), 0));
        let mut fat = vec![0u8; 8192];
        fat[0..3].copy_from_slice(&[0xEB, 0x3C, 0x90]);
        assert!(!looks_like_qnx2(&mut Cursor::new(fat), 0));
    }

    #[test]
    fn a_looping_chain_is_an_error_not_a_hang() {
        let mut img = sample();
        img[4 * 512 + 4..4 * 512 + 8].copy_from_slice(&5u32.to_le_bytes());
        let mut fs = Qnx2Filesystem::open(Cursor::new(img), 0).unwrap();
        assert!(fs.read_chain(5, usize::MAX).is_err());
    }

    #[test]
    fn icon_dates_decode_with_the_pm_bit_and_month_rollover() {
        // 0xbb02 / 0x9906: 2 Nov 1991, AM, 39174 s after midnight = 10:52:54.
        let t = decode_date(0xBB02, 0x9906).unwrap();
        assert_eq!(
            crate::fs::unix_common::inode::format_unix_timestamp(t),
            "1991-11-02 10:52:54"
        );
        // 0xba3f: 31 Oct 1991, PM.
        let t = decode_date(0xBA3F, 0).unwrap();
        assert_eq!(
            crate::fs::unix_common::inode::format_unix_timestamp(t),
            "1991-10-31 12:00:00"
        );
        // Day 0 of May at 22:36 is the last day of April.
        let t = decode_date(0xA520, 38175).unwrap();
        assert_eq!(
            crate::fs::unix_common::inode::format_unix_timestamp(t),
            "1990-04-30 22:36:15"
        );
        assert_eq!(decode_date(0, 0), None);
        assert_eq!(decode_date(0x7027, 0), None);
    }

    #[test]
    fn owner_permissions_and_attributes_are_decoded() {
        let mut fs = Qnx2Filesystem::open(Cursor::new(sample()), 0).unwrap();
        let root = fs.root().unwrap();
        let list = fs.list_directory(&root).unwrap();
        assert_eq!(list[0].owner_label.as_deref(), Some("0.0"));
        assert_eq!(list[0].mode, Some(0o100444));
        assert_eq!(
            list[0].native_attrs.as_deref(),
            Some("owner r---- group r---- other r----")
        );
        assert_eq!(list[1].mode, Some(0o040755));
        assert_eq!(letters(0x35, true), "rc-m");
        assert_eq!(letters(0x19, false), "r--em");
        assert_eq!(unix_class(0x19, false), 0o5);
    }

    #[test]
    fn deleted_entries_show_only_when_asked_and_read_back() {
        let mut fs = Qnx2Filesystem::open(Cursor::new(sample_with_bitmap()), 0).unwrap();
        let root = fs.root().unwrap();
        assert!(!fs
            .list_directory(&root)
            .unwrap()
            .iter()
            .any(|e| e.name.contains("GONE")));
        assert!(fs.set_show_deleted(true));
        let list = fs.list_directory(&root).unwrap();
        let gone = list
            .iter()
            .find(|e| e.name == "GONE [deleted]")
            .expect("deleted entry listed");
        assert_eq!(fs.read_file(gone, usize::MAX).unwrap(), b"old");
        assert!(gone
            .native_attrs
            .as_deref()
            .unwrap()
            .ends_with("deleted, recoverable"));
    }

    #[test]
    fn fsck_reports_orphans_and_unmarked_blocks() {
        let img = sample_with_bitmap();
        let mut fs = Qnx2Filesystem::open(Cursor::new(img.clone()), 0).unwrap();
        let r = fsck_qnx2(&mut fs).unwrap();
        assert!(r.is_clean(), "{:?}", r.errors);
        assert!(r.warnings.iter().all(|w| w.debug), "{:?}", r.warnings);
        // Mark block 8 used (orphan) and free block 5 (HELLO's).
        let mut bad = img;
        bad[512 + 16] = 0x8F;
        let mut fs = Qnx2Filesystem::open(Cursor::new(bad), 0).unwrap();
        let r = fsck_qnx2(&mut fs).unwrap();
        assert!(r.errors.iter().any(|e| e.code == "UnmarkedBlocks"));
        assert!(r
            .warnings
            .iter()
            .any(|w| w.code == "OrphanedBlocks" && w.message.ends_with(": 8")));
    }

    #[test]
    fn rs_text_converts_and_binaries_are_left_alone() {
        assert!(looks_like_rs_text(b"line one\x1eline two\x1e"));
        assert_eq!(rs_to_lf(b"a\x1eb\x1e"), b"a\nb\n");
        assert!(!looks_like_rs_text(b"\x01\x84\x9f\x00\x1e"));
        assert!(!looks_like_rs_text(b"plain unix text\n"));
    }
}
