//! Read and write QNX 2 filesystems: QNX 2.x, and the Unisys ICON's System
//! Software and QDOS volumes (diskettes and LEXICON hard-disk partitions).
//!
//! ```no_run
//! use qnx2fs::{NewEntry, Volume};
//! # fn main() -> qnx2fs::Result<()> {
//! let image = std::fs::read("disk.img")?;
//! let mut vol = Volume::open(std::io::Cursor::new(image), 0)?;
//! for e in vol.list("/")? {
//!     println!("{} {}", e.name, e.size);
//! }
//! vol.put("/hello", b"hi\x1e", &NewEntry::default())?;
//! let image = vol.into_inner().into_inner();
//! # Ok(()) }
//! ```
//!
//! # The format
//!
//! The layout comes from Mitchell Schoenbrun's public-domain `q2files.c` /
//! `q2read.c` and was checked against the ICON floppies. Everything is
//! little-endian with 16-bit `int`s.
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
//! - Text files end lines with RS (`0x1E`); [`rs_to_lf`] and [`lf_to_rs`] convert.
//!
//! Writing follows what QNX 4.10.01 itself does, learned by diffing disks it wrote
//! in icon-emu and checked with its `chkfsys`:
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
//!
//! # Time
//!
//! Writes stamp dates. The crate takes "now" from [`Volume::set_clock`] when it
//! has been set, otherwise from the system clock. On `wasm32-unknown-unknown`,
//! which has no clock, set it, or dates come out as 1 January 1994.
//!
//! An ICON date holds only the years 1980-1995. Dates outside that range, for
//! "now" and for `NewEntry::mtime`, are **folded** ([`fold_date`]): same month,
//! day and time, in 1994 (29 February in 1992), as icon-emu's clock does.
//! Copied [`Attributes`] keep their stored date exactly.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{self, Read, Seek, SeekFrom, Write};

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
/// Longest chain a well-formed volume can hold; a loop or a wild pointer stops here.
const MAX_EXTENTS: usize = 1 << 16;

/// Everything that can go wrong.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    /// The bytes at the given offset are not a QNX 2 volume.
    NotQnx2,
    InvalidData(String),
    NotFound(String),
    AlreadyExists(String),
    DiskFull(String),
    Unsupported(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::NotQnx2 => write!(f, "not a QNX 2 volume"),
            Error::InvalidData(m) => write!(f, "{m}"),
            Error::NotFound(m) => write!(f, "not found: {m}"),
            Error::AlreadyExists(m) => write!(f, "already exists: {m}"),
            Error::DiskFull(m) => write!(f, "disk full: {m}"),
            Error::Unsupported(m) => write!(f, "unsupported: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Blocks a file of `len` bytes needs in full-size extents.
pub fn blocks_for(len: u64) -> u64 {
    let per = MAX_EXTENT_BLOCKS * BLOCK - XTNT_HDR;
    let (full, rest) = (len / per, len % per);
    full * MAX_EXTENT_BLOCKS
        + if rest > 0 || full == 0 {
            (rest + XTNT_HDR).div_ceil(BLOCK)
        } else {
            0
        }
}

// ---- Dates

/// Days from 1970-01-01 to `y-m-d` (proleptic Gregorian).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (i64::from(m) + 9) % 12;
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `(year, month, day)` of the day `z` days after 1970-01-01.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Decode an ICON date pair to Unix seconds (local time, as the ICON kept it).
pub fn decode_date(w0: u16, w1: u16) -> Option<i64> {
    // An unset clock writes month and day 0 (QNX shows `0-----80`): count it as 1 January.
    let month = u32::from((w0 >> 8) & 15).max(1);
    let day = u32::from((w0 >> 1) & 31).max(1);
    if (w0 == 0 && w1 == 0) || month > 12 || w1 >= 43200 {
        return None;
    }
    let year = 1980 + i64::from(w0 >> 12);
    if day > days_in_month(year, month) {
        return None;
    }
    let pm = if w0 & 1 != 0 { 43200 } else { 0 };
    Some(days_from_civil(year, month, day) * 86400 + pm + i64::from(w1))
}

/// Move a date the ICON can't store into its 1980-1995 range: the same month,
/// day and time of day, in **1994**. 29 February becomes 29 February **1992**,
/// the last leap year the ICON can store. Dates already in 1980-1995 are kept.
///
/// 1994 is late enough that these dates sort after all the ICON system
/// software (the last release is from 1991), and early enough that a clock
/// running past New Year's Eve moves into 1995, which can still be stored.
/// icon-emu's emulated clock reads the host clock the same way. The year itself
/// is lost: an ICON's login screen shows the date it is using, worth a look at
/// start-up.
pub fn fold_date(unix: i64) -> i64 {
    let (y, m, d) = civil_from_days(unix.div_euclid(86400));
    if (1980..=1995).contains(&y) {
        return unix;
    }
    let year = if (m, d) == (2, 29) { 1992 } else { 1994 };
    days_from_civil(year, m, d) * 86400 + unix.rem_euclid(86400)
}

/// Encode Unix seconds as an ICON date pair; the 4-bit year holds 1980-1995, so
/// other times clamp. To write a modern date the way the crate does, fold it
/// first: `encode_date(fold_date(t))`.
pub fn encode_date(unix: i64) -> [u16; 2] {
    let lo = days_from_civil(1980, 1, 1) * 86400;
    let hi = days_from_civil(1995, 12, 31) * 86400 + 86399;
    let t = unix.clamp(lo, hi);
    let (y, m, d) = civil_from_days(t.div_euclid(86400));
    let s = t.rem_euclid(86400) as u32;
    let w0 = ((y - 1980) as u16) << 12 | (m as u16) << 8 | (d as u16) << 1 | u16::from(s >= 43200);
    [w0, (s % 43200) as u16]
}

/// `YYYY-MM-DD HH:MM:SS` for Unix seconds.
pub fn format_date(unix: i64) -> String {
    let (y, m, d) = civil_from_days(unix.div_euclid(86400));
    let s = unix.rem_euclid(86400);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        s / 3600,
        s / 60 % 60,
        s % 60
    )
}

fn system_now() -> i64 {
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        0
    }
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }
}

// ---- Permissions and names

/// Unix permission bits for one class from QNX 2 `rwaem` / `rcbm` bits.
pub fn unix_class(bits: u8, dir: bool) -> u32 {
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
pub fn letters(bits: u8, dir: bool) -> String {
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

/// QNX 2 owner attributes and group / other permissions for a Unix mode.
pub fn qnx_bits(mode: u32, dir: bool) -> (u8, u8, u8) {
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

/// A valid QNX 2 name: 1-16 of `A-Z a-z 0-9 _ . $`, not starting with `$`.
pub fn validate_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 16
        && !name.starts_with('$')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'$'));
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidData(format!(
            "{name:?} is not a valid QNX 2 name (1-16 of A-Z a-z 0-9 _ . $, not starting with $)"
        )))
    }
}

/// The closest valid QNX 2 name to `name`: other characters become `_`, a leading
/// `$` is dropped, and the result is cut to 16 characters, keeping the extension
/// when there is room.
pub fn sanitize_name(name: &str) -> String {
    let clean: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '$') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let clean = clean.trim_start_matches('$');
    if clean.len() <= 16 {
        return if clean.is_empty() {
            "file".into()
        } else {
            clean.into()
        };
    }
    match clean.rfind('.') {
        Some(dot) if clean.len() - dot <= 5 && dot > 0 => {
            let ext = &clean[dot..];
            format!("{}{ext}", &clean[..16 - ext.len()])
        }
        _ => clean[..16].into(),
    }
}

// ---- Text

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

/// Replace LF and CR LF line endings with QNX 2's RS.
pub fn lf_to_rs(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len());
    let mut i = 0;
    while i < data.len() {
        match data[i] {
            b'\r' if data.get(i + 1) == Some(&b'\n') => {
                out.push(RS);
                i += 1;
            }
            b'\n' => out.push(RS),
            b => out.push(b),
        }
        i += 1;
    }
    out
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

// ---- Entries

/// What became of a deleted entry's blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletedState {
    /// The data is still there and its blocks are free: it can be read back.
    Recoverable,
    /// Its extents hold no data (QNX empties the first one on delete).
    NoData,
    /// Some of its blocks now belong to something else.
    BlocksReused,
    /// Its extent chain can't be followed.
    ChainLost,
}

impl DeletedState {
    pub fn as_str(self) -> &'static str {
        match self {
            DeletedState::Recoverable => "deleted, recoverable",
            DeletedState::NoData => "deleted, no data",
            DeletedState::BlocksReused => "deleted, blocks reused",
            DeletedState::ChainLost => "deleted, chain lost",
        }
    }
}

/// One file or directory, as listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    /// Absolute path, `/` for the root.
    pub path: String,
    /// The first block of its extent chain, which identifies it on the volume.
    pub first: u32,
    pub is_dir: bool,
    /// Data bytes (0 for directories).
    pub size: u64,
    /// Blocks its extents hold, as the entry records them.
    pub blocks: u32,
    pub member: u8,
    pub group: u8,
    /// Owner attributes (`rwaem` / `rcbm`, plus `0x20` for a directory).
    pub attr: u8,
    pub group_perms: u8,
    pub other_perms: u8,
    /// The raw date pair; see [`Entry::modified`].
    pub date: [u16; 2],
    pub status: u8,
    /// `Some` for a deleted entry.
    pub deleted: Option<DeletedState>,
}

impl Entry {
    /// Its QNX metadata (permissions, owner, date), to give another entry.
    pub fn attributes(&self) -> Attributes {
        Attributes {
            attr: self.attr & !DIRECTORY,
            group_perms: self.group_perms,
            other_perms: self.other_perms,
            member: self.member,
            group: self.group,
            date: self.date,
        }
    }

    /// A reference to an entry by path and first block, enough for the edit calls.
    pub fn reference(path: &str, first: u32, is_dir: bool) -> Self {
        let name = path.rsplit('/').next().unwrap_or_default().to_string();
        Entry {
            name,
            path: path.to_string(),
            first,
            is_dir,
            size: 0,
            blocks: 0,
            member: 0,
            group: 0,
            attr: 0,
            group_perms: 0,
            other_perms: 0,
            date: [0, 0],
            status: USED,
            deleted: None,
        }
    }

    /// Modification time as Unix seconds (the ICON kept local time).
    pub fn modified(&self) -> Option<i64> {
        decode_date(self.date[0], self.date[1])
    }

    /// Unix mode bits, file type included.
    pub fn unix_mode(&self) -> u32 {
        let kind = if self.is_dir { 0o040000 } else { 0o100000 };
        kind | unix_class(self.attr, self.is_dir) << 6
            | unix_class(self.group_perms, self.is_dir) << 3
            | unix_class(self.other_perms, self.is_dir)
    }

    /// The owner as QNX writes it, `group.member`.
    pub fn owner_label(&self) -> String {
        format!("{}.{}", self.group, self.member)
    }

    /// `owner rwa-m group r---- other r----`, plus deleted / modified / odd-status notes.
    pub fn describe(&self) -> String {
        let dir = self.is_dir;
        let mut s = format!(
            "owner {} group {} other {}",
            letters(self.attr, dir),
            letters(self.group_perms, dir),
            letters(self.other_perms, dir)
        );
        if let Some(d) = self.deleted {
            s = format!("{s}; {}", d.as_str());
        } else if self.status & MODIFIED != 0 {
            s.push_str("; modified");
        }
        let odd = self.status & !(USED | MODIFIED);
        if odd != 0 {
            s.push_str(&format!("; status 0x{odd:02X}"));
        }
        s
    }
}

/// An entry's QNX 2 metadata exactly as stored, so it can be copied without
/// going through Unix modes: owner attributes, group and other permissions,
/// owner, and the raw date pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attributes {
    /// Owner attributes (`rwaem` for files, `rcbm` for directories). The
    /// directory bit (0x20) is ignored: the creating call sets it.
    pub attr: u8,
    pub group_perms: u8,
    pub other_perms: u8,
    pub member: u8,
    pub group: u8,
    /// The raw date pair (see [`decode_date`]).
    pub date: [u16; 2],
}

/// What [`Volume::copy_from`] copied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CopyReport {
    pub files: usize,
    pub dirs: usize,
}

/// Settings for a new file or directory. Defaults match what QNX writes.
#[derive(Debug, Clone, Copy, Default)]
pub struct NewEntry {
    /// Unix permission bits; default owner `rwa-m`, others `r` (directories `rc-m` / `r`).
    pub mode: Option<u32>,
    /// Member number (0-255); default 255.
    pub uid: Option<u32>,
    /// Group number (0-255); default 255.
    pub gid: Option<u32>,
    /// Modification time, Unix seconds; default now. Dates after 1995 (or
    /// before 1980) are folded: see [`fold_date`].
    pub mtime: Option<i64>,
    /// Copy these exactly instead (permissions, owner and date); `mode`,
    /// `uid` and `gid` are then ignored, and `mtime`, if set, replaces the date.
    pub attributes: Option<Attributes>,
}

impl NewEntry {
    /// The same metadata as `entry`.
    pub fn like(entry: &Entry) -> Self {
        NewEntry {
            attributes: Some(entry.attributes()),
            ..Default::default()
        }
    }
}

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

/// One extent: first block, blocks spanned, data bytes, and the header's prev pointer.
#[derive(Debug, Clone, Copy)]
struct Extent {
    block: u64,
    span: u64,
    size: u64,
    prev: u64,
}

/// Where a directory entry lives: slot `slot` of the directory whose first extent is `dir`.
#[derive(Debug, Clone, Copy)]
struct EntryLoc {
    dir: u32,
    slot: usize,
}

fn join(parent: &str, name: &str) -> String {
    format!("{}/{}", parent.trim_end_matches('/'), name)
}

/// Split `/a/b/c` into (`/a/b`, `c`).
fn split_path(path: &str) -> Result<(String, String)> {
    let p = path.trim_end_matches('/');
    match p.rsplit_once('/') {
        Some((parent, name)) if !name.is_empty() => Ok((
            if parent.is_empty() {
                "/".into()
            } else {
                parent.into()
            },
            name.into(),
        )),
        None if !p.is_empty() => Ok(("/".into(), p.into())),
        _ => Err(Error::InvalidData(format!("{path:?} names no entry"))),
    }
}

// ---- The volume

/// A QNX 2 volume at `offset` in `dev`. Reads need `Read + Seek`; edits also `Write`.
pub struct Volume<D> {
    dev: D,
    offset: u64,
    total_blocks: u64,
    free_blocks: u64,
    root_first: u32,
    bitmap: Vec<u8>,
    /// First extent of the `bitmap` file, where allocation changes are written back.
    bm_first: u32,
    clock: Option<i64>,
}

impl<D: Read + Seek> Volume<D> {
    pub fn open(mut dev: D, offset: u64) -> Result<Self> {
        if !looks_like_qnx2(&mut dev, offset) {
            return Err(Error::NotQnx2);
        }
        let mut b1 = [0u8; BLOCK as usize];
        dev.seek(SeekFrom::Start(offset))?;
        dev.read_exact(&mut b1)?;
        let root = Dirent::decode(&b1[ROOT_AT..ROOT_AT + ENTRY]);
        let end = dev.seek(SeekFrom::End(0))?;
        let mut vol = Self {
            dev,
            offset,
            total_blocks: end.saturating_sub(offset) / BLOCK,
            free_blocks: 0,
            root_first: root.first,
            bitmap: Vec::new(),
            bm_first: 0,
            clock: None,
        };
        // The bitmap file sizes the volume (one bit per block) and counts the free blocks.
        if let Ok(entries) = vol.read_dir(root.first) {
            if let Some(bm) = entries
                .iter()
                .find(|e| e.in_use() && e.name == "bitmap" && !e.is_dir())
            {
                if let Ok(map) = vol.read_chain(bm.first, usize::MAX) {
                    vol.total_blocks = (map.len() as u64 * 8).min(vol.total_blocks.max(1));
                    vol.free_blocks = map
                        .iter()
                        .map(|b| b.count_zeros() as u64)
                        .sum::<u64>()
                        .min(vol.total_blocks);
                    vol.bitmap = map;
                    vol.bm_first = bm.first;
                }
            }
        }
        Ok(vol)
    }

    /// Give back the device.
    pub fn into_inner(self) -> D {
        self.dev
    }

    /// Use `unix` seconds as "now" for the dates edits write.
    pub fn set_clock(&mut self, unix: i64) {
        self.clock = Some(unix);
    }

    /// "Now" for the dates edits write, folded into the ICON's range.
    fn now(&self) -> i64 {
        fold_date(self.clock.unwrap_or_else(system_now))
    }

    /// Blocks in the volume (512 bytes each).
    pub fn total_blocks(&self) -> u64 {
        self.total_blocks
    }

    /// Blocks the bitmap marks free.
    pub fn free_blocks(&self) -> u64 {
        self.free_blocks
    }

    /// First block of the root directory's extent chain.
    pub fn root_first(&self) -> u32 {
        self.root_first
    }

    /// The root directory.
    pub fn root(&self) -> Entry {
        let mut e = Entry::reference("/", self.root_first, true);
        e.name = String::new();
        e
    }

    /// Entries of a directory; deleted ones too when `include_deleted`.
    pub fn list_dir(&mut self, dir: &Entry, include_deleted: bool) -> Result<Vec<Entry>> {
        let dirents = self.read_dir(dir.first)?;
        let mut out = Vec::new();
        for d in &dirents {
            if d.in_use() || include_deleted {
                out.push(self.entry_for(&dir.path, d));
            }
        }
        Ok(out)
    }

    /// Up to `max` bytes of a file.
    pub fn read_file(&mut self, entry: &Entry, max: usize) -> Result<Vec<u8>> {
        self.read_chain(entry.first, max)
    }

    /// The entry at `path` (`/` is the root).
    pub fn lookup(&mut self, path: &str) -> Result<Entry> {
        let mut cur = self.root();
        for part in path.split('/').filter(|p| !p.is_empty()) {
            if !cur.is_dir {
                return Err(Error::NotFound(path.into()));
            }
            cur = self
                .list_dir(&cur, false)?
                .into_iter()
                .find(|e| e.name == part)
                .ok_or_else(|| Error::NotFound(path.into()))?;
        }
        Ok(cur)
    }

    /// The entries of the directory at `path`.
    pub fn list(&mut self, path: &str) -> Result<Vec<Entry>> {
        let dir = self.lookup(path)?;
        if !dir.is_dir {
            return Err(Error::InvalidData(format!("{path} is not a directory")));
        }
        self.list_dir(&dir, false)
    }

    /// The whole file at `path`.
    pub fn get(&mut self, path: &str) -> Result<Vec<u8>> {
        let e = self.lookup(path)?;
        if e.is_dir {
            return Err(Error::InvalidData(format!("{path} is a directory")));
        }
        self.read_file(&e, usize::MAX)
    }

    /// Whether the bitmap marks block `n` in use; `None` past its end.
    fn marked(&self, n: u64) -> Option<bool> {
        let i = n.checked_sub(1)?;
        self.bitmap
            .get((i / 8) as usize)
            .map(|b| b >> (i % 8) & 1 != 0)
    }

    /// Every extent of the chain starting at `first`.
    fn extents(&mut self, first: u32) -> Result<Vec<Extent>> {
        let mut out = Vec::new();
        let mut at = first as u64;
        let mut seen = HashSet::new();
        while at != 0 {
            if at > self.total_blocks.max(1) || !seen.insert(at) || out.len() >= MAX_EXTENTS {
                return Err(Error::InvalidData(format!(
                    "qnx2: extent chain breaks at block {at}"
                )));
            }
            let mut hdr = [0u8; 16];
            self.dev
                .seek(SeekFrom::Start(self.offset + (at - 1) * BLOCK))?;
            self.dev.read_exact(&mut hdr)?;
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
    fn read_chain(&mut self, first: u32, max: usize) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        for x in self.extents(first)? {
            if out.len() >= max {
                break;
            }
            let take = (x.size as usize).min(max - out.len());
            let start = out.len();
            out.resize(start + take, 0);
            self.dev.seek(SeekFrom::Start(
                self.offset + (x.block - 1) * BLOCK + XTNT_HDR,
            ))?;
            self.dev.read_exact(&mut out[start..])?;
        }
        Ok(out)
    }

    fn file_len(&mut self, first: u32) -> Result<u64> {
        Ok(self.extents(first)?.iter().map(|e| e.size).sum())
    }

    /// Every named entry of a directory, deleted ones included.
    fn read_dir(&mut self, first: u32) -> Result<Vec<Dirent>> {
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

    /// What became of a deleted entry's blocks.
    fn deleted_state(&mut self, d: &Dirent) -> DeletedState {
        match self.extents(d.first) {
            Err(_) => DeletedState::ChainLost,
            Ok(xs) => {
                let reused = xs
                    .iter()
                    .flat_map(|x| x.block..x.block + x.span)
                    .any(|b| self.marked(b) == Some(true));
                if reused {
                    DeletedState::BlocksReused
                } else if xs.iter().all(|x| x.size == 0) {
                    DeletedState::NoData
                } else {
                    DeletedState::Recoverable
                }
            }
        }
    }

    fn entry_for(&mut self, parent: &str, d: &Dirent) -> Entry {
        let dir = d.is_dir();
        let size = if dir {
            0
        } else {
            self.file_len(d.first).unwrap_or(0)
        };
        let deleted = (!d.in_use()).then(|| self.deleted_state(d));
        Entry {
            name: d.name.clone(),
            path: join(parent, &d.name),
            first: d.first,
            is_dir: dir,
            size,
            blocks: d.blocks_less_one.saturating_add(1),
            member: d.member,
            group: d.group,
            attr: d.attr,
            group_perms: d.gperms,
            other_perms: d.perms,
            date: d.date,
            status: d.status,
            deleted,
        }
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
            let child = join(path, &d.name);
            if !d.in_use() {
                report.deleted += 1;
                if self.deleted_state(&d) == DeletedState::Recoverable {
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

    /// Volume byte ranges holding `len` bytes of the stream of chain `first` from `off`.
    /// Block 1 acts as a one-block stream whose data starts at byte 16, like an extent.
    fn stream_spans(&mut self, first: u32, mut off: u64, len: u64) -> Result<Vec<(u64, u64)>> {
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
            return Err(Error::InvalidData(format!(
                "qnx2: stream of block {first} is shorter than expected"
            )));
        }
        Ok(out)
    }

    fn read_stream(&mut self, first: u32, off: u64, buf: &mut [u8]) -> Result<()> {
        let mut at = 0usize;
        for (pos, n) in self.stream_spans(first, off, buf.len() as u64)? {
            self.dev.seek(SeekFrom::Start(pos))?;
            self.dev.read_exact(&mut buf[at..at + n as usize])?;
            at += n as usize;
        }
        Ok(())
    }

    fn header_len(&mut self, dir: u32) -> Result<u64> {
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

    fn read_entry(&mut self, loc: EntryLoc) -> Result<[u8; ENTRY]> {
        let mut e = [0u8; ENTRY];
        let at = self.header_len(loc.dir)? + (loc.slot * ENTRY) as u64;
        self.read_stream(loc.dir, at, &mut e)?;
        Ok(e)
    }

    /// Every 48-byte slot of a directory, in order.
    fn dir_slots(&mut self, dir: u32) -> Result<Vec<[u8; ENTRY]>> {
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
    fn self_loc(&mut self, dir: u32) -> Result<EntryLoc> {
        let mut h = [0u8; 4];
        self.read_stream(dir, 0, &mut h)?;
        let index = u16::from_le_bytes([h[2], h[3]]);
        if index & 0x8000 != 0 {
            return Err(Error::Unsupported(
                "qnx2: extended directory headers are not supported for editing".into(),
            ));
        }
        Ok(EntryLoc {
            dir: u32::from(u16::from_le_bytes([h[0], h[1]])),
            slot: index as usize,
        })
    }

    /// The in-use entry named `name` with first extent `first` in directory `dir`.
    fn find_slot(&mut self, dir: u32, name: &str, first: u32) -> Result<usize> {
        self.dir_slots(dir)?
            .iter()
            .position(|e| {
                let d = Dirent::decode(e);
                d.in_use() && d.name == name && d.first == first
            })
            .ok_or_else(|| Error::NotFound(format!("qnx2: {name}")))
    }

    fn name_taken(&mut self, dir: u32, name: &str) -> Result<bool> {
        Ok(self.dir_slots(dir)?.iter().any(|e| {
            let d = Dirent::decode(e);
            d.in_use() && d.name == name
        }))
    }

    /// Check the volume: chains, cross-links, and the bitmap against the blocks files hold.
    pub fn check(&mut self) -> Result<Report> {
        let mut owners = HashMap::new();
        let mut walk = Walk::default();
        let root = self.root_first;
        self.walk(root, "/", 0, &mut owners, &mut walk);
        let mut errors = walk.errors;
        let mut warnings = walk.warnings;
        if self.bitmap.is_empty() {
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
            let orphans: Vec<u64> = (1..=self.total_blocks)
                .filter(|&b| {
                    self.marked(b) == Some(true) && !owners.contains_key(&b) && !reserved(b)
                })
                .collect();
            // The 3.00 formatter marks the volume's last block in use; nothing is lost there.
            let (last, lost): (Vec<u64>, Vec<u64>) =
                orphans.into_iter().partition(|&b| b == self.total_blocks);
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
                    format!("the last block ({}) is marked in use", self.total_blocks),
                );
                i.debug = true;
                warnings.push(i);
            }
        }
        Ok(Report {
            errors,
            warnings,
            files: walk.files,
            dirs: walk.dirs,
            deleted: walk.deleted,
            recoverable: walk.recoverable,
            total_blocks: self.total_blocks,
            free_blocks: self.free_blocks,
        })
    }
}

impl<D: Read + Write + Seek> Volume<D> {
    fn write_stream(&mut self, first: u32, off: u64, data: &[u8]) -> Result<()> {
        let mut at = 0usize;
        for (pos, n) in self.stream_spans(first, off, data.len() as u64)? {
            self.dev.seek(SeekFrom::Start(pos))?;
            self.dev.write_all(&data[at..at + n as usize])?;
            at += n as usize;
        }
        Ok(())
    }

    fn write_entry(&mut self, loc: EntryLoc, e: &[u8; ENTRY]) -> Result<()> {
        let at = self.header_len(loc.dir)? + (loc.slot * ENTRY) as u64;
        self.write_stream(loc.dir, at, e)
    }

    fn write_block_bytes(&mut self, block: u64, off: u64, data: &[u8]) -> Result<()> {
        self.dev
            .seek(SeekFrom::Start(self.offset + (block - 1) * BLOCK + off))?;
        self.dev.write_all(data)?;
        Ok(())
    }

    fn write_header(&mut self, block: u64, hdr: [u32; 4]) -> Result<()> {
        let mut b = [0u8; 16];
        for (i, w) in hdr.iter().enumerate() {
            b[i * 4..i * 4 + 4].copy_from_slice(&w.to_le_bytes());
        }
        self.write_block_bytes(block, 0, &b)
    }

    /// Set or clear the bitmap bits of `count` blocks from `start`, in memory and on disk.
    fn mark(&mut self, start: u64, count: u64, used: bool) -> Result<()> {
        if self.bm_first == 0 {
            return Err(Error::Unsupported(
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
    fn allocate(&mut self, len: u64) -> Result<Vec<(u64, u64)>> {
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
            return Err(Error::DiskFull(format!(
                "{len} bytes do not fit in {} free blocks",
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
    ) -> Result<(u64, u64)> {
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
            self.dev.seek(SeekFrom::Start(
                self.offset + (start - 1) * BLOCK + XTNT_HDR,
            ))?;
            let mut left = size;
            while left > 0 {
                let k = left.min(buf.len() as u64) as usize;
                data.read_exact(&mut buf[..k])?;
                self.dev.write_all(&buf[..k])?;
                left -= k as u64;
            }
            len -= size;
        }
        let blocks = runs.iter().map(|r| r.1).sum();
        Ok((runs.last().map_or(0, |r| r.0), blocks))
    }

    /// A free slot in directory `dir`, growing its stream by one slot when none is free.
    fn free_slot(&mut self, dir: u32) -> Result<usize> {
        let slots = self.dir_slots(dir)?;
        if let Some(i) = slots.iter().position(|e| e[0] & USED == 0) {
            return Ok(i);
        }
        if dir == 1 {
            return Err(Error::DiskFull("qnx2: block 1 has no free slot".into()));
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
    fn touch_dir(&mut self, dir: u32) -> Result<()> {
        let loc = self.self_loc(dir)?;
        // A header that doesn't name its entry leaves nothing to date; the edit itself stands.
        if loc.dir == 0 {
            return Ok(());
        }
        let mut e = self.read_entry(loc)?;
        let [w0, w1] = encode_date(self.now());
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
    fn loc_of(&mut self, entry: &Entry) -> Result<EntryLoc> {
        if entry.is_dir && entry.first != self.root_first {
            return self.self_loc(entry.first);
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
                .ok_or_else(|| Error::NotFound(parent_path.clone()))?;
        }
        let slot = self.find_slot(dir, &entry.name, entry.first)?;
        Ok(EntryLoc { dir, slot })
    }

    fn owner_of(opts: &NewEntry) -> (u8, u8) {
        if let Some(a) = opts.attributes {
            return (a.member, a.group);
        }
        (
            opts.uid.map_or(255, |u| u.min(255) as u8),
            opts.gid.map_or(255, |g| g.min(255) as u8),
        )
    }

    /// (owner attributes, group, other) for a new entry: copied, from a mode, or QNX's defaults.
    fn bits_of(opts: &NewEntry, dir: bool) -> (u8, u8, u8) {
        let flag = if dir { DIRECTORY } else { 0 };
        match (opts.attributes, opts.mode) {
            (Some(a), _) => ((a.attr & !DIRECTORY) | flag, a.group_perms, a.other_perms),
            (None, Some(m)) => qnx_bits(m, dir),
            (None, None) if dir => (0x35, 0x21, 0x21),
            (None, None) => (0x17, 0x01, 0x01),
        }
    }

    /// The date pair for a new entry: `mtime`, else copied, else now.
    fn date_of(&self, opts: &NewEntry) -> [u16; 2] {
        match (opts.mtime, opts.attributes) {
            (Some(t), _) => encode_date(fold_date(t)),
            (None, Some(a)) => a.date,
            (None, None) => encode_date(self.now()),
        }
    }

    /// Create a file of `len` bytes read from `data` in directory `parent`.
    pub fn create_file(
        &mut self,
        parent: &Entry,
        name: &str,
        data: &mut dyn Read,
        len: u64,
        opts: &NewEntry,
    ) -> Result<Entry> {
        validate_name(name)?;
        let dir = parent.first;
        if self.name_taken(dir, name)? {
            return Err(Error::AlreadyExists(name.to_string()));
        }
        let runs = self.allocate(len)?;
        let (last, blocks) = self.write_chain(&runs, data, len)?;
        let bits = Self::bits_of(opts, false);
        let date = self.date_of(opts);
        let e = Self::new_entry(
            name,
            runs[0].0,
            last,
            blocks,
            runs.len(),
            Self::owner_of(opts),
            bits,
            date,
        );
        let slot = self.free_slot(dir)?;
        self.write_entry(EntryLoc { dir, slot }, &e)?;
        self.touch_dir(dir)?;
        Ok(self.entry_for(&parent.path, &Dirent::decode(&e)))
    }

    /// Create an empty directory in `parent`.
    pub fn create_dir(&mut self, parent: &Entry, name: &str, opts: &NewEntry) -> Result<Entry> {
        validate_name(name)?;
        let dir = parent.first;
        if self.name_taken(dir, name)? {
            return Err(Error::AlreadyExists(name.to_string()));
        }
        let parent_xtnt = u16::try_from(dir)
            .map_err(|_| Error::Unsupported("qnx2: parent directory past block 65535".into()))?;
        let slot = self.free_slot(dir)?;
        let nb = self.allocate(ENTRY as u64 * 10 + 4)?[0].0;
        // A new directory is one block: the header and ten empty slots, as `mkdir` writes it.
        self.write_header(nb, [0, 0, 4 + 10 * ENTRY as u32, 0])?;
        let mut body = vec![0u8; (BLOCK - XTNT_HDR) as usize];
        body[0..2].copy_from_slice(&parent_xtnt.to_le_bytes());
        body[2..4].copy_from_slice(&(slot as u16).to_le_bytes());
        self.write_block_bytes(nb, XTNT_HDR, &body)?;
        let bits = Self::bits_of(opts, true);
        let date = self.date_of(opts);
        let e = Self::new_entry(name, nb, nb, 1, 1, Self::owner_of(opts), bits, date);
        self.write_entry(EntryLoc { dir, slot }, &e)?;
        self.touch_dir(dir)?;
        Ok(self.entry_for(&parent.path, &Dirent::decode(&e)))
    }

    /// Delete `entry` from `parent` (a directory must be empty), as QNX's `frel` does.
    pub fn delete(&mut self, parent: &Entry, entry: &Entry) -> Result<()> {
        let dir = parent.first;
        let slot = self.find_slot(dir, &entry.name, entry.first)?;
        let loc = EntryLoc { dir, slot };
        let mut e = self.read_entry(loc)?;
        let d = Dirent::decode(&e);
        if d.is_dir() && self.dir_slots(d.first)?.iter().any(|s| s[0] & USED != 0) {
            return Err(Error::InvalidData(format!(
                "directory {} is not empty",
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
        let [w0, w1] = encode_date(self.now());
        e[27..29].copy_from_slice(&w0.to_le_bytes());
        e[29..31].copy_from_slice(&w1.to_le_bytes());
        self.write_entry(loc, &e)?;
        self.touch_dir(dir)
    }

    /// Rename `entry` within `parent`.
    pub fn rename(&mut self, parent: &Entry, entry: &Entry, new_name: &str) -> Result<()> {
        validate_name(new_name)?;
        let dir = parent.first;
        if self.name_taken(dir, new_name)? {
            return Err(Error::AlreadyExists(new_name.to_string()));
        }
        let slot = self.find_slot(dir, &entry.name, entry.first)?;
        let loc = EntryLoc { dir, slot };
        let mut e = self.read_entry(loc)?;
        e[31..ENTRY].fill(0);
        e[31..31 + new_name.len()].copy_from_slice(new_name.as_bytes());
        self.write_entry(loc, &e)?;
        self.touch_dir(dir)
    }

    /// Set permissions from Unix mode bits.
    pub fn set_permissions(&mut self, entry: &Entry, mode: u32) -> Result<()> {
        let loc = self.loc_of(entry)?;
        let mut e = self.read_entry(loc)?;
        let (attr, g, o) = qnx_bits(mode, e[26] & DIRECTORY != 0);
        e[24] = g;
        e[25] = o;
        e[26] = attr;
        self.write_entry(loc, &e)
    }

    /// Set the owner: `uid` is the member, `gid` the group, each 0-255.
    pub fn set_owner(&mut self, entry: &Entry, uid: u32, gid: u32) -> Result<()> {
        if uid > 255 || gid > 255 {
            return Err(Error::InvalidData(
                "qnx2: group and member are 0-255".into(),
            ));
        }
        let loc = self.loc_of(entry)?;
        let mut e = self.read_entry(loc)?;
        e[15] = uid as u8;
        e[16] = gid as u8;
        self.write_entry(loc, &e)
    }

    /// Flush the device.
    pub fn flush(&mut self) -> Result<()> {
        self.dev.flush()?;
        Ok(())
    }

    /// Write `data` as a new file at `path`.
    pub fn put(&mut self, path: &str, data: &[u8], opts: &NewEntry) -> Result<Entry> {
        let (parent, name) = split_path(path)?;
        let dir = self.lookup(&parent)?;
        self.create_file(&dir, &name, &mut &data[..], data.len() as u64, opts)
    }

    /// Create the directory `path`.
    pub fn mkdir(&mut self, path: &str, opts: &NewEntry) -> Result<Entry> {
        let (parent, name) = split_path(path)?;
        let dir = self.lookup(&parent)?;
        self.create_dir(&dir, &name, opts)
    }

    /// Delete the file or empty directory at `path`.
    pub fn remove(&mut self, path: &str) -> Result<()> {
        let (parent, _) = split_path(path)?;
        let dir = self.lookup(&parent)?;
        let e = self.lookup(path)?;
        self.delete(&dir, &e)
    }

    /// Rename the entry at `path` to `new_name`, in the same directory.
    pub fn rename_path(&mut self, path: &str, new_name: &str) -> Result<()> {
        let (parent, _) = split_path(path)?;
        let dir = self.lookup(&parent)?;
        let e = self.lookup(path)?;
        self.rename(&dir, &e, new_name)
    }

    /// Give `entry` exactly these permissions, owner and date.
    pub fn set_attributes(&mut self, entry: &Entry, a: Attributes) -> Result<()> {
        let loc = self.loc_of(entry)?;
        let mut e = self.read_entry(loc)?;
        let dir = e[26] & DIRECTORY;
        e[15] = a.member;
        e[16] = a.group;
        e[24] = a.group_perms;
        e[25] = a.other_perms;
        e[26] = (a.attr & !DIRECTORY) | dir;
        e[27..29].copy_from_slice(&a.date[0].to_le_bytes());
        e[29..31].copy_from_slice(&a.date[1].to_le_bytes());
        self.write_entry(loc, &e)
    }

    /// Write `data` at `path`. A file already there is replaced and its
    /// permissions and owner kept, dated now (or `opts.mtime`); otherwise the
    /// file is created with `opts`. The space is checked before the old file
    /// goes, so a full disk leaves it in place.
    pub fn replace(&mut self, path: &str, data: &[u8], opts: &NewEntry) -> Result<Entry> {
        let old = match self.lookup(path) {
            Ok(e) if e.is_dir => {
                return Err(Error::AlreadyExists(format!("{path} is a directory")))
            }
            Ok(e) => e,
            Err(Error::NotFound(_)) => return self.put(path, data, opts),
            Err(e) => return Err(e),
        };
        let need = blocks_for(data.len() as u64);
        if need > self.free_blocks + u64::from(old.blocks) {
            return Err(Error::DiskFull(format!(
                "{} bytes do not fit in {} free blocks",
                data.len(),
                self.free_blocks + u64::from(old.blocks)
            )));
        }
        let mut o = *opts;
        if o.attributes.is_none() {
            o.attributes = Some(old.attributes());
        }
        if o.mtime.is_none() {
            o.mtime = Some(self.now());
        }
        self.remove(path)?;
        self.put(path, data, &o)
    }

    /// Copy the file or folder at `src_path` on another volume into the folder
    /// `dst_dir` here, under the same name; a folder comes with everything in
    /// it. Every entry keeps its permissions, owner and date. A folder that
    /// already exists here is merged into; a file that exists is an error
    /// unless `replace`. Copying a volume's root (`/`) copies its contents,
    /// except the free-space `bitmap`.
    pub fn copy_from<S: Read + Seek>(
        &mut self,
        src: &mut Volume<S>,
        src_path: &str,
        dst_dir: &str,
        replace: bool,
    ) -> Result<CopyReport> {
        let mut report = CopyReport::default();
        let dir = self.lookup(dst_dir)?;
        if !dir.is_dir {
            return Err(Error::InvalidData(format!("{dst_dir} is not a directory")));
        }
        let e = src.lookup(src_path)?;
        if src_path.trim_matches('/').is_empty() {
            for child in src.list("/")? {
                if child.name != "bitmap" {
                    self.copy_entry(src, &child, &dir.path, replace, &mut report)?;
                }
            }
        } else {
            self.copy_entry(src, &e, &dir.path, replace, &mut report)?;
        }
        Ok(report)
    }

    fn copy_entry<S: Read + Seek>(
        &mut self,
        src: &mut Volume<S>,
        e: &Entry,
        dst_dir: &str,
        replace: bool,
        report: &mut CopyReport,
    ) -> Result<()> {
        let target = join(dst_dir, &e.name);
        let existing = match self.lookup(&target) {
            Ok(t) => Some(t),
            Err(Error::NotFound(_)) => None,
            Err(x) => return Err(x),
        };
        if e.is_dir {
            let created = match existing {
                Some(t) if t.is_dir => false,
                Some(_) => return Err(Error::AlreadyExists(target)),
                None => {
                    self.mkdir(&target, &NewEntry::like(e))?;
                    report.dirs += 1;
                    true
                }
            };
            for child in src.list(&e.path)? {
                self.copy_entry(src, &child, &target, replace, report)?;
            }
            // Adding entries re-dated the folder; give a copied folder its own date back.
            if created {
                let t = self.lookup(&target)?;
                self.set_attributes(&t, e.attributes())?;
            }
        } else {
            match existing {
                Some(t) if t.is_dir => {
                    return Err(Error::AlreadyExists(format!("{target} is a directory")))
                }
                Some(_) if !replace => return Err(Error::AlreadyExists(target)),
                Some(_) => self.remove(&target)?,
                None => {}
            }
            let data = src.read_file(e, usize::MAX)?;
            self.put(&target, &data, &NewEntry::like(e))?;
            report.files += 1;
        }
        Ok(())
    }
}

// ---- Checking

/// One finding of [`Volume::check`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    pub code: String,
    pub message: String,
    /// Noise a casual check can hide (such as the formatter's reserved last block).
    pub debug: bool,
}

/// What [`Volume::check`] found.
#[derive(Debug, Clone)]
pub struct Report {
    pub errors: Vec<Issue>,
    pub warnings: Vec<Issue>,
    pub files: u32,
    pub dirs: u32,
    pub deleted: u32,
    pub recoverable: u32,
    pub total_blocks: u64,
    pub free_blocks: u64,
}

impl Report {
    /// No errors.
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }
}

/// Findings gathered while walking the tree.
#[derive(Default)]
struct Walk {
    errors: Vec<Issue>,
    warnings: Vec<Issue>,
    unmarked: Vec<u64>,
    files: u32,
    dirs: u32,
    deleted: u32,
    recoverable: u32,
}

fn issue(code: &str, message: String) -> Issue {
    Issue {
        code: code.into(),
        message,
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

#[cfg(feature = "disk-core")]
pub mod adapter;

#[cfg(test)]
mod tests;
