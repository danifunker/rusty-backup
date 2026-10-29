//! QNX 2 filesystem (QNX 2.x, and the Unisys ICON's System Software and QDOS): read and edit.
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
//! - Date: word 0 is year-1980 in bits 15-12, month 11-8, day 5-1, PM 0; word 1 is
//!   seconds since 12 o'clock. Checked against QNX 4.10.01's `files` in icon-emu.
//! - A file is a chain of extents. Each extent's first block starts with
//!   `{u32 prev, u32 next, u32 size, u32 bound}`: `size` is the data bytes the
//!   extent holds (after the 16-byte header, running on into its following
//!   blocks) and `bound` the blocks it spans beyond the first. A file's length
//!   is the sum of its extents' `size`.
//! - A directory's data starts `{u16 parent_xtnt, u16 dir_index}` (plus a u16
//!   when `dir_index`'s top bit is set), then 48-byte entries. A deleted entry
//!   keeps its name and extents with the `0x40` status bit clear.
//! - Text files end lines with RS (`0x1E`); [`rs_to_lf`] converts them on extract.
//!
//! Writing follows what QNX 4.10.01 itself does, learned by diffing disks it wrote
//! in icon-emu and checked with its `chkfsys` (see `docs/PLAN-qnx.md`):
//!
//! - Block 1 is laid out like a directory extent: its root entry is slot 1 at
//!   20 + 48. Every directory's data header points at its own entry: slot
//!   `dir_index` of the directory whose first extent is `parent_xtnt` (the root's
//!   says block 1, slot 1), which is where its date and extent counts live.
//! - `mkdir` allocates one block holding the header and ten empty slots
//!   (`size` 484). A directory's entries are one stream across its extents, so a
//!   48-byte slot may straddle two; a new slot reuses the first one not in use,
//!   else the stream grows by 48, a new one-block extent taking the overflow.
//! - New files: status `0x50`, owner attributes `0x17` (`rwa-m`), group and other
//!   `0x01`; directories `0x35` / `0x21`. A changed directory's date is updated.
//! - `frel` (delete) clears the status byte, zeroes the first extent's `size` and
//!   frees the bitmap bits; the name and extent pointers stay.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
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
/// QNX keeps an extent's byte count in 16 bits (the high word is checked as zero), so an
/// extent spans at most 128 blocks (65520 data bytes).
const MAX_EXTENT_BLOCKS: u64 = 128;

/// Blocks a file of `len` bytes needs in full-size extents.
fn blocks_for(len: u64) -> u64 {
    let per = MAX_EXTENT_BLOCKS * BLOCK - XTNT_HDR;
    let (full, rest) = (len / per, len % per);
    full * MAX_EXTENT_BLOCKS
        + if rest > 0 || full == 0 {
            (rest + XTNT_HDR).div_ceil(BLOCK)
        } else {
            0
        }
}

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
    // An unset clock writes month and day 0 (QNX shows `0-----80`): count it as 1 January.
    let month = u32::from((w0 >> 8) & 15).max(1);
    let day = u32::from((w0 >> 1) & 31).max(1);
    if (w0 == 0 && w1 == 0) || month > 12 || w1 >= 43200 {
        return None;
    }
    let date = chrono::NaiveDate::from_ymd_opt(1980 + i32::from(w0 >> 12), month, day)?;
    // 719_163 is 1970-01-01 counted from 0001-01-01.
    let days = i64::from(date.num_days_from_ce()) - 719_163;
    let pm = if w0 & 1 != 0 { 43200 } else { 0 };
    Some(days * 86400 + pm + i64::from(w1))
}

/// Encode Unix seconds as an ICON date pair; the 4-bit year holds 1980-1995, so other times clamp.
pub fn encode_date(unix: i64) -> [u16; 2] {
    use chrono::{Datelike, NaiveDate};
    let day0 = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).map_or(0, days_since_epoch);
    let t = unix.clamp(day0(1980, 1, 1) * 86400, day0(1995, 12, 31) * 86400 + 86399);
    let date = NaiveDate::from_num_days_from_ce_opt((t.div_euclid(86400) + 719_163) as i32)
        .unwrap_or_default();
    let s = t.rem_euclid(86400) as u32;
    let w0 = ((date.year() - 1980) as u16) << 12
        | (date.month() as u16) << 8
        | (date.day() as u16) << 1
        | u16::from(s >= 43200);
    [w0, (s % 43200) as u16]
}

fn days_since_epoch(d: chrono::NaiveDate) -> i64 {
    use chrono::Datelike;
    i64::from(d.num_days_from_ce()) - 719_163
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
    /// First extent of the `bitmap` file, where allocation changes are written back.
    bm_first: u32,
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
            bm_first: 0,
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
                    fs.bm_first = bm.first;
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

/// Where a directory entry lives: slot `slot` of the directory whose first extent is `dir`.
#[derive(Debug, Clone, Copy)]
struct EntryLoc {
    dir: u32,
    slot: usize,
}

/// A valid QNX 2 name: 1-16 of `A-Z a-z 0-9 _ . $`, not starting with `$`.
fn validate_qnx2_name(name: &str) -> Result<(), FilesystemError> {
    let ok = !name.is_empty()
        && name.len() <= 16
        && !name.starts_with('$')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'$'));
    if ok {
        Ok(())
    } else {
        Err(FilesystemError::InvalidData(format!(
            "qnx2: {name:?} is not a valid name (1-16 of A-Z a-z 0-9 _ . $, not starting with $)"
        )))
    }
}

/// QNX 2 owner attributes and group / other permissions for a Unix mode.
fn qnx_bits(mode: u32, dir: bool) -> (u8, u8, u8) {
    let class = |m: u32| -> u8 {
        let mut b = 0;
        if m & 4 != 0 {
            b |= 0x01;
        }
        if m & 2 != 0 {
            b |= if dir { 0x04 } else { 0x06 };
        }
        if !dir && m & 1 != 0 {
            b |= 0x08;
        }
        b
    };
    // The owner always keeps `m`, so chattr can still change the entry.
    let attr = class(mode >> 6) | 0x10 | if dir { DIRECTORY } else { 0 };
    (attr, class(mode >> 3), class(mode))
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl<R: Read + Seek + Send> Qnx2Filesystem<R> {
    /// Volume byte ranges holding `len` bytes of the stream of chain `first` from `off`.
    /// Block 1 acts as a one-block stream whose data starts at byte 16, like an extent.
    fn stream_spans(
        &mut self,
        first: u32,
        mut off: u64,
        len: u64,
    ) -> Result<Vec<(u64, u64)>, FilesystemError> {
        let pieces: Vec<(u64, u64)> = if first == 1 {
            vec![(1, BLOCK - XTNT_HDR)]
        } else {
            self.extents(first)?
                .iter()
                .map(|x| (x.block, x.size))
                .collect()
        };
        let mut out = Vec::new();
        let mut left = len;
        for (block, size) in pieces {
            if left == 0 {
                break;
            }
            if off >= size {
                off -= size;
                continue;
            }
            let take = (size - off).min(left);
            out.push((self.offset + (block - 1) * BLOCK + XTNT_HDR + off, take));
            left -= take;
            off = 0;
        }
        if left > 0 {
            return Err(FilesystemError::InvalidData(format!(
                "qnx2: stream of block {first} is shorter than expected"
            )));
        }
        Ok(out)
    }

    fn read_stream(&mut self, first: u32, off: u64, buf: &mut [u8]) -> Result<(), FilesystemError> {
        let mut at = 0usize;
        for (pos, n) in self.stream_spans(first, off, buf.len() as u64)? {
            self.reader.seek(SeekFrom::Start(pos))?;
            self.reader.read_exact(&mut buf[at..at + n as usize])?;
            at += n as usize;
        }
        Ok(())
    }

    fn header_len(&mut self, dir: u32) -> Result<u64, FilesystemError> {
        if dir == 1 {
            return Ok(4);
        }
        let mut h = [0u8; 4];
        self.read_stream(dir, 0, &mut h)?;
        Ok(if u16::from_le_bytes([h[2], h[3]]) & 0x8000 != 0 {
            6
        } else {
            4
        })
    }

    fn read_entry(&mut self, loc: EntryLoc) -> Result<[u8; ENTRY], FilesystemError> {
        let mut e = [0u8; ENTRY];
        let at = self.header_len(loc.dir)? + (loc.slot * ENTRY) as u64;
        self.read_stream(loc.dir, at, &mut e)?;
        Ok(e)
    }

    /// Every 48-byte slot of a directory, in order.
    fn dir_slots(&mut self, dir: u32) -> Result<Vec<[u8; ENTRY]>, FilesystemError> {
        let data = self.read_chain(dir, 1 << 22)?;
        let hdr = self.header_len(dir)? as usize;
        Ok(data
            .get(hdr..)
            .unwrap_or_default()
            .chunks_exact(ENTRY)
            .map(|c| {
                let mut e = [0u8; ENTRY];
                e.copy_from_slice(c);
                e
            })
            .collect())
    }

    /// The entry describing directory `dir`, from the `{parent_xtnt, dir_index}` header.
    fn self_loc(&mut self, dir: u32) -> Result<EntryLoc, FilesystemError> {
        let mut h = [0u8; 4];
        self.read_stream(dir, 0, &mut h)?;
        let index = u16::from_le_bytes([h[2], h[3]]);
        if index & 0x8000 != 0 {
            return Err(FilesystemError::Unsupported(
                "qnx2: extended directory headers are not supported for editing".into(),
            ));
        }
        Ok(EntryLoc {
            dir: u32::from(u16::from_le_bytes([h[0], h[1]])),
            slot: index as usize,
        })
    }

    /// The in-use entry named `name` with first extent `first` in directory `dir`.
    fn find_slot(&mut self, dir: u32, name: &str, first: u32) -> Result<usize, FilesystemError> {
        self.dir_slots(dir)?
            .iter()
            .position(|e| {
                let d = Dirent::decode(e);
                d.in_use() && d.name == name && d.first == first
            })
            .ok_or_else(|| FilesystemError::NotFound(format!("qnx2: {name}")))
    }

    fn name_taken(&mut self, dir: u32, name: &str) -> Result<bool, FilesystemError> {
        Ok(self.dir_slots(dir)?.iter().any(|e| {
            let d = Dirent::decode(e);
            d.in_use() && d.name == name
        }))
    }
}

impl<R: Read + Write + Seek + Send> Qnx2Filesystem<R> {
    fn write_stream(&mut self, first: u32, off: u64, data: &[u8]) -> Result<(), FilesystemError> {
        let mut at = 0usize;
        for (pos, n) in self.stream_spans(first, off, data.len() as u64)? {
            self.reader.seek(SeekFrom::Start(pos))?;
            self.reader.write_all(&data[at..at + n as usize])?;
            at += n as usize;
        }
        Ok(())
    }

    fn write_entry(&mut self, loc: EntryLoc, e: &[u8; ENTRY]) -> Result<(), FilesystemError> {
        let at = self.header_len(loc.dir)? + (loc.slot * ENTRY) as u64;
        self.write_stream(loc.dir, at, e)
    }

    fn write_block_bytes(
        &mut self,
        block: u64,
        off: u64,
        data: &[u8],
    ) -> Result<(), FilesystemError> {
        self.reader
            .seek(SeekFrom::Start(self.offset + (block - 1) * BLOCK + off))?;
        self.reader.write_all(data)?;
        Ok(())
    }

    fn write_header(&mut self, block: u64, hdr: [u32; 4]) -> Result<(), FilesystemError> {
        let mut b = [0u8; 16];
        for (i, w) in hdr.iter().enumerate() {
            b[i * 4..i * 4 + 4].copy_from_slice(&w.to_le_bytes());
        }
        self.write_block_bytes(block, 0, &b)
    }

    /// Set or clear the bitmap bits of `count` blocks from `start`, in memory and on disk.
    fn mark(&mut self, start: u64, count: u64, used: bool) -> Result<(), FilesystemError> {
        if self.bm_first == 0 {
            return Err(FilesystemError::Unsupported(
                "qnx2: the volume has no bitmap file".into(),
            ));
        }
        let (lo, hi) = (
            ((start - 1) / 8) as usize,
            ((start + count - 2) / 8) as usize,
        );
        for b in start..start + count {
            let (i, bit) = (((b - 1) / 8) as usize, (b - 1) % 8);
            let was = self.bitmap[i] >> bit & 1 != 0;
            if used {
                self.bitmap[i] |= 1 << bit;
            } else {
                self.bitmap[i] &= !(1 << bit);
            }
            match (was, used) {
                (false, true) => self.free_blocks -= 1,
                (true, false) => self.free_blocks += 1,
                _ => {}
            }
        }
        let bytes = self.bitmap[lo..=hi].to_vec();
        self.write_stream(self.bm_first, lo as u64, &bytes)
    }

    /// Extents for `len` data bytes, at most [`MAX_EXTENT_BLOCKS`] each as QNX writes them: from
    /// one contiguous run when one is big enough, else from the largest free runs first.
    fn allocate(&mut self, len: u64) -> Result<Vec<(u64, u64)>, FilesystemError> {
        let mut runs: Vec<(u64, u64)> = Vec::new();
        let mut b = 2;
        while b <= self.total_blocks {
            if self.marked(b) == Some(false) {
                let start = b;
                while b <= self.total_blocks && self.marked(b) == Some(false) {
                    b += 1;
                }
                runs.push((start, b - start));
            } else {
                b += 1;
            }
        }
        let need = blocks_for(len);
        if let Some(&(start, _)) = runs.iter().find(|r| r.1 >= need) {
            runs = vec![(start, need)];
        } else {
            runs.sort_by_key(|r| std::cmp::Reverse(r.1));
        }
        // Carve the runs into extents until the data fits.
        let mut out = Vec::new();
        let mut left = len;
        'runs: for (start, n) in runs {
            let mut at = start;
            while at < start + n {
                let take = (start + n - at).min(MAX_EXTENT_BLOCKS);
                let cap = take * BLOCK - XTNT_HDR;
                if cap >= left {
                    out.push((at, (left + XTNT_HDR).div_ceil(BLOCK).max(1)));
                    left = 0;
                    break 'runs;
                }
                out.push((at, take));
                left -= cap;
                at += take;
            }
        }
        if left > 0 || out.is_empty() {
            return Err(FilesystemError::DiskFull(format!(
                "qnx2: {len} bytes do not fit in {} free blocks",
                self.free_blocks
            )));
        }
        out.sort_unstable();
        for &(start, n) in &out {
            self.mark(start, n, true)?;
        }
        Ok(out)
    }

    /// Write `len` bytes from `data` as an extent chain over `runs`; returns (last extent, blocks).
    fn write_chain(
        &mut self,
        runs: &[(u64, u64)],
        data: &mut dyn Read,
        mut len: u64,
    ) -> Result<(u64, u64), FilesystemError> {
        let mut buf = vec![0u8; 64 * 1024];
        for (i, &(start, n)) in runs.iter().enumerate() {
            let size = len.min(n * BLOCK - XTNT_HDR);
            let prev = if i == 0 { 0 } else { runs[i - 1].0 };
            let next = runs.get(i + 1).map_or(0, |r| r.0);
            self.write_header(
                start,
                [prev as u32, next as u32, size as u32, (n - 1) as u32],
            )?;
            // Zero the extent so no stale bytes follow the data.
            let zeros = vec![0u8; (n * BLOCK - XTNT_HDR) as usize];
            self.write_block_bytes(start, XTNT_HDR, &zeros)?;
            self.reader.seek(SeekFrom::Start(
                self.offset + (start - 1) * BLOCK + XTNT_HDR,
            ))?;
            let mut left = size;
            while left > 0 {
                let k = left.min(buf.len() as u64) as usize;
                data.read_exact(&mut buf[..k])?;
                self.reader.write_all(&buf[..k])?;
                left -= k as u64;
            }
            len -= size;
        }
        let blocks = runs.iter().map(|r| r.1).sum();
        Ok((runs.last().map_or(0, |r| r.0), blocks))
    }

    /// A free slot in directory `dir`, growing its stream by one slot when none is free.
    fn free_slot(&mut self, dir: u32) -> Result<usize, FilesystemError> {
        let slots = self.dir_slots(dir)?;
        if let Some(i) = slots.iter().position(|e| e[0] & USED == 0) {
            return Ok(i);
        }
        if dir == 1 {
            return Err(FilesystemError::DiskFull(
                "qnx2: block 1 has no free slot".into(),
            ));
        }
        let xs = self.extents(dir)?;
        let last = *xs.last().expect("a directory has an extent");
        let room = last.span * BLOCK - XTNT_HDR - last.size;
        let header = |x: &Extent, next: u64, size: u64| {
            [x.prev as u32, next as u32, size as u32, (x.span - 1) as u32]
        };
        if room >= ENTRY as u64 {
            self.write_header(last.block, header(&last, 0, last.size + ENTRY as u64))?;
        } else {
            // The slot straddles: fill this extent and spill the rest into a new one-block extent.
            let nb = self.allocate(ENTRY as u64)?[0].0;
            self.write_header(last.block, header(&last, nb, last.size + room))?;
            let spill = ENTRY as u64 - room;
            self.write_header(nb, [last.block as u32, 0, spill as u32, 0])?;
            self.write_block_bytes(nb, XTNT_HDR, &[0u8; (BLOCK - XTNT_HDR) as usize])?;
            let loc = self.self_loc(dir)?;
            let mut e = self.read_entry(loc)?;
            let d = Dirent::decode(&e);
            e[5..9].copy_from_slice(&(nb as u32).to_le_bytes());
            e[9..13].copy_from_slice(&(d.blocks_less_one + 1).to_le_bytes());
            let nxt = u16::from_le_bytes([e[13], e[14]]) + 1;
            e[13..15].copy_from_slice(&nxt.to_le_bytes());
            self.write_entry(loc, &e)?;
        }
        let i = slots.len();
        self.write_entry(EntryLoc { dir, slot: i }, &[0u8; ENTRY])?;
        Ok(i)
    }

    /// Stamp directory `dir`'s own entry with the current date, as QNX does on a change.
    fn touch_dir(&mut self, dir: u32) -> Result<(), FilesystemError> {
        let loc = self.self_loc(dir)?;
        // A header that doesn't name its entry leaves nothing to date; the edit itself stands.
        if loc.dir == 0 {
            return Ok(());
        }
        let mut e = self.read_entry(loc)?;
        let [w0, w1] = encode_date(now_unix());
        e[27..29].copy_from_slice(&w0.to_le_bytes());
        e[29..31].copy_from_slice(&w1.to_le_bytes());
        self.write_entry(loc, &e)
    }

    #[allow(clippy::too_many_arguments)]
    fn new_entry(
        name: &str,
        first: u64,
        last: u64,
        blocks: u64,
        extents: usize,
        owner: (u8, u8),
        bits: (u8, u8, u8),
        date: [u16; 2],
    ) -> [u8; ENTRY] {
        let mut e = [0u8; ENTRY];
        e[0] = USED | MODIFIED;
        e[1..5].copy_from_slice(&(first as u32).to_le_bytes());
        e[5..9].copy_from_slice(&(last as u32).to_le_bytes());
        e[9..13].copy_from_slice(&(blocks.saturating_sub(1) as u32).to_le_bytes());
        e[13..15].copy_from_slice(&(extents as u16).to_le_bytes());
        e[15] = owner.0;
        e[16] = owner.1;
        e[24] = bits.1;
        e[25] = bits.2;
        e[26] = bits.0;
        e[27..29].copy_from_slice(&date[0].to_le_bytes());
        e[29..31].copy_from_slice(&date[1].to_le_bytes());
        e[31..31 + name.len()].copy_from_slice(name.as_bytes());
        e
    }

    /// Where `entry` lives, found from its parent directory and checked against its location.
    fn loc_of(&mut self, entry: &FileEntry) -> Result<EntryLoc, FilesystemError> {
        if entry.is_directory() && entry.location as u32 != self.root_first {
            return self.self_loc(entry.location as u32);
        }
        let parent_path = match entry.path.rsplit_once('/') {
            Some(("", _)) | None => "/".to_string(),
            Some((p, _)) => p.to_string(),
        };
        let mut dir = self.root_first;
        for part in parent_path.split('/').filter(|p| !p.is_empty()) {
            let slots = self.dir_slots(dir)?;
            dir = slots
                .iter()
                .map(|e| Dirent::decode(e))
                .find(|d| d.in_use() && d.is_dir() && d.name == part)
                .map(|d| d.first)
                .ok_or_else(|| FilesystemError::NotFound(parent_path.clone()))?;
        }
        let slot = self.find_slot(dir, &entry.name, entry.location as u32)?;
        Ok(EntryLoc { dir, slot })
    }

    fn owner_of(opts_uid: Option<u32>, opts_gid: Option<u32>) -> (u8, u8) {
        (
            opts_uid.map_or(255, |u| u.min(255) as u8),
            opts_gid.map_or(255, |g| g.min(255) as u8),
        )
    }
}

impl<R: Read + Write + Seek + Send> EditableFilesystem for Qnx2Filesystem<R> {
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
        validate_qnx2_name(name)?;
        let dir = parent.location as u32;
        if self.name_taken(dir, name)? {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }
        let runs = self.allocate(data_len)?;
        let (last, blocks) = self.write_chain(&runs, data, data_len)?;
        let bits = options
            .mode
            .map_or((0x17, 0x01, 0x01), |m| qnx_bits(m, false));
        let when = options
            .unix_times
            .and_then(|t| t.mtime)
            .map_or_else(now_unix, |t| t as i64);
        let e = Self::new_entry(
            name,
            runs[0].0,
            last,
            blocks,
            runs.len(),
            Self::owner_of(options.uid, options.gid),
            bits,
            encode_date(when),
        );
        let slot = self.free_slot(dir)?;
        self.write_entry(EntryLoc { dir, slot }, &e)?;
        self.touch_dir(dir)?;
        Ok(self.entry_for(&parent.path, &Dirent::decode(&e), name.to_string()))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        validate_qnx2_name(name)?;
        let dir = parent.location as u32;
        if self.name_taken(dir, name)? {
            return Err(FilesystemError::AlreadyExists(name.to_string()));
        }
        let parent_xtnt = u16::try_from(dir).map_err(|_| {
            FilesystemError::Unsupported("qnx2: parent directory past block 65535".into())
        })?;
        let slot = self.free_slot(dir)?;
        let nb = self.allocate(ENTRY as u64 * 10 + 4)?[0].0;
        // A new directory is one block: the header and ten empty slots, as `mkdir` writes it.
        self.write_header(nb, [0, 0, 4 + 10 * ENTRY as u32, 0])?;
        let mut body = vec![0u8; (BLOCK - XTNT_HDR) as usize];
        body[0..2].copy_from_slice(&parent_xtnt.to_le_bytes());
        body[2..4].copy_from_slice(&(slot as u16).to_le_bytes());
        self.write_block_bytes(nb, XTNT_HDR, &body)?;
        let bits = options
            .mode
            .map_or((0x35, 0x21, 0x21), |m| qnx_bits(m, true));
        let when = options
            .unix_times
            .and_then(|t| t.mtime)
            .map_or_else(now_unix, |t| t as i64);
        let e = Self::new_entry(
            name,
            nb,
            nb,
            1,
            1,
            Self::owner_of(options.uid, options.gid),
            bits,
            encode_date(when),
        );
        self.write_entry(EntryLoc { dir, slot }, &e)?;
        self.touch_dir(dir)?;
        Ok(self.entry_for(&parent.path, &Dirent::decode(&e), name.to_string()))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        let dir = parent.location as u32;
        let slot = self.find_slot(dir, &entry.name, entry.location as u32)?;
        let loc = EntryLoc { dir, slot };
        let mut e = self.read_entry(loc)?;
        let d = Dirent::decode(&e);
        if d.is_dir() && self.dir_slots(d.first)?.iter().any(|s| s[0] & USED != 0) {
            return Err(FilesystemError::InvalidData(format!(
                "qnx2: directory {} is not empty",
                entry.path
            )));
        }
        let xs = self.extents(d.first)?;
        for x in &xs {
            self.mark(x.block, x.span, false)?;
        }
        // As `frel`: the slot is released and the first extent emptied; names and pointers stay.
        if let Some(x) = xs.first() {
            self.write_block_bytes(x.block, 8, &0u32.to_le_bytes())?;
        }
        e[0] = 0;
        let [w0, w1] = encode_date(now_unix());
        e[27..29].copy_from_slice(&w0.to_le_bytes());
        e[29..31].copy_from_slice(&w1.to_le_bytes());
        self.write_entry(loc, &e)?;
        self.touch_dir(dir)
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        validate_qnx2_name(new_name)?;
        let dir = parent.location as u32;
        if self.name_taken(dir, new_name)? {
            return Err(FilesystemError::AlreadyExists(new_name.to_string()));
        }
        let slot = self.find_slot(dir, &entry.name, entry.location as u32)?;
        let loc = EntryLoc { dir, slot };
        let mut e = self.read_entry(loc)?;
        e[31..ENTRY].fill(0);
        e[31..31 + new_name.len()].copy_from_slice(new_name.as_bytes());
        self.write_entry(loc, &e)?;
        self.touch_dir(dir)
    }

    fn set_permissions(&mut self, entry: &FileEntry, mode: u32) -> Result<(), FilesystemError> {
        let loc = self.loc_of(entry)?;
        let mut e = self.read_entry(loc)?;
        let (attr, g, o) = qnx_bits(mode, e[26] & DIRECTORY != 0);
        e[24] = g;
        e[25] = o;
        e[26] = attr;
        self.write_entry(loc, &e)
    }

    fn set_owner(&mut self, entry: &FileEntry, uid: u32, gid: u32) -> Result<(), FilesystemError> {
        if uid > 255 || gid > 255 {
            return Err(FilesystemError::InvalidData(
                "qnx2: group and member are 0-255".into(),
            ));
        }
        let loc = self.loc_of(entry)?;
        let mut e = self.read_entry(loc)?;
        e[15] = uid as u8;
        e[16] = gid as u8;
        self.write_entry(loc, &e)
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        self.reader.flush()?;
        Ok(())
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.free_blocks * BLOCK)
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

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        validate_qnx2_name(name)
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
    fn icon_dates_match_what_qnx_files_prints() {
        let show = |w0, w1| {
            crate::fs::unix_common::inode::format_unix_timestamp(decode_date(w0, w1).unwrap())
        };
        // Each pair as QNX 4.10.01's `files` lists it in icon-emu.
        assert_eq!(show(0x5539, 0x2A91), "1985-05-28 15:01:37"); // 28-May-85 3:01pm
        assert_eq!(show(0xBA3A, 0x0642), "1991-10-29 00:26:42"); // 29-Oct-91 12:26am
        assert_eq!(show(0x832F, 0x3976), "1988-03-23 16:05:10"); // 23-Mar-88 4:05pm
        assert_eq!(decode_date(0, 0), None);
        // A real ST-251 installed with the clock unset: QNX lists `0-----80 12:12am`.
        assert_eq!(show(0x0000, 0x02D0), "1980-01-01 00:12:00");
        assert_eq!(decode_date(0x0D00, 0), None, "month 13 is not a date");
        for t in [
            decode_date(0x5539, 0x2A91).unwrap(),
            decode_date(0xBA3A, 0x0642).unwrap(),
        ] {
            let [w0, w1] = encode_date(t);
            assert_eq!(decode_date(w0, w1), Some(t));
        }
        // The 4-bit year stops at 1995.
        assert_eq!(encode_date(2_000_000_000)[0] >> 12, 15);
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

    /// `sample_with_bitmap()` with the root's header naming its entry (block 1, slot 1) as QNX writes it.
    fn editable_sample() -> Cursor<Vec<u8>> {
        let mut img = sample_with_bitmap();
        img[2 * 512 + 16..2 * 512 + 20].copy_from_slice(&[1, 0, 1, 0]);
        Cursor::new(img)
    }

    #[test]
    fn edits_round_trip_and_check_clean() {
        let mut fs = Qnx2Filesystem::open(editable_sample(), 0).unwrap();
        let root = fs.root().unwrap();
        let body: Vec<u8> = (0..1500u32).map(|i| i as u8).collect();
        let f = fs
            .create_file(
                &root,
                "data.bin",
                &mut &body[..],
                body.len() as u64,
                &Default::default(),
            )
            .unwrap();
        assert_eq!(fs.read_file(&f, usize::MAX).unwrap(), body);
        let d = fs
            .create_directory(&root, "newdir", &Default::default())
            .unwrap();
        let inner = fs
            .create_file(&d, "x", &mut &b"hi\x1e"[..], 3, &Default::default())
            .unwrap();
        assert_eq!(fs.read_file(&inner, usize::MAX).unwrap(), b"hi\x1e");
        // The new directory's header points back at its slot in the root.
        let loc = fs.self_loc(d.location as u32).unwrap();
        assert_eq!(loc.dir, fs.root_first);
        let r = fsck_qnx2(&mut fs).unwrap();
        assert!(r.is_clean(), "{:?}", r.errors);
        assert!(r.warnings.iter().all(|w| w.debug), "{:?}", r.warnings);

        fs.rename(&root, &f, "renamed").unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.contains(&"renamed".to_string()) && !names.contains(&"data.bin".to_string()));
        let free_before = fs.free_space().unwrap();
        let renamed = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "renamed")
            .unwrap();
        fs.delete_entry(&root, &renamed).unwrap();
        assert!(fs.free_space().unwrap() > free_before);
        assert!(
            fs.delete_entry(&root, &d).is_err(),
            "a non-empty directory stays"
        );
        fs.delete_entry(&d, &inner).unwrap();
        fs.delete_entry(&root, &d).unwrap();
        let r = fsck_qnx2(&mut fs).unwrap();
        assert!(r.is_clean(), "{:?}", r.errors);
    }

    #[test]
    fn names_and_extent_limits_follow_qnx() {
        assert!(validate_qnx2_name("ok_name.1$").is_ok());
        for bad in ["", "$lead", "has space", "seventeen_chars_x", "slash/"] {
            assert!(validate_qnx2_name(bad).is_err(), "{bad}");
        }
        // 65520 data bytes fill one 128-block extent; one more byte needs a second extent.
        assert_eq!(blocks_for(65520), 128);
        assert_eq!(blocks_for(65521), 129);
        assert_eq!(blocks_for(0), 1);
        assert_eq!(qnx_bits(0o644, false), (0x17, 0x01, 0x01));
        assert_eq!(qnx_bits(0o755, true), (0x35, 0x01, 0x01));
    }
}
