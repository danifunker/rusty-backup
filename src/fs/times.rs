//! Per-file Unix timestamps shared across the create / import / export paths,
//! plus the format-specific encoders every non-Unix driver needs to write those
//! same times into its own on-disk shape.
//!
//! The goal is date-preservation on copy: when rb-cli copies a file from a host
//! folder, a tar archive, or another disk image into a filesystem, the
//! destination should record the *source's* mtime, not "now". And a
//! `tar_export` (or any host-side extract) of that image should carry the same
//! mtime back out.
//!
//! ## The rule
//!
//! `create_file` / `create_directory` / `create_symlink` use these times when
//! `options.unix_times.is_some()`; otherwise they stamp `now`. So:
//!
//! - **Genuine new file** (user typed `put`, GUI created a blank file) —
//!   caller leaves `unix_times = None`, driver stamps `now`.
//! - **Copy / extract / import** — caller (dir_import / tar_import /
//!   Commander stage_copy) captures the source's mtime and passes it through;
//!   the destination inode records it verbatim.
//!
//! ## Why per-field Options
//!
//! Different sources carry different subsets: a tar Header has only `mtime`,
//! an HFS entry has creation + modification + backup dates, an EFS inode has
//! all three (atime/mtime/ctime). Missing fields aren't fabricated — the
//! driver falls back to `now` (or `mtime` where sensible) for each.
//!
//! Values are u64 seconds since UNIX epoch 1970-01-01. Sub-second precision
//! is intentionally dropped: FAT is 2-second granular, EFS/AFFS/PFS3 are 1-
//! second, only ext/xfs/jfs carry nanoseconds — and the tar exporter only
//! writes seconds anyway, so nanos wouldn't round-trip end-to-end.
//!
//! ## Format encoders
//!
//! Every filesystem below rusty-backup edits carries dates in *some* shape:
//! DOS packed (FAT / exFAT / Human68k), NTFS FILETIME, Mac epoch (HFS /
//! HFS+ / MFS), ProDOS packed, UCSD packed, ADFS 40-bit centiseconds, OS-9
//! Y-M-D bytes, QDOS 1961 epoch. Each encoder in this module turns a Unix
//! `u64` seconds value into that shape; each decoder is the inverse and
//! returns `None` on the format's "no date set" sentinel (usually a
//! literal zero on disk).
//!
//! Encoders clamp to the format's earliest representable date rather than
//! underflowing — a FAT file dated 1975 becomes 1980-01-01, not garbage.
//! Decoders return `None` for zero and for values that would land before
//! 1970, so a `modified_unix` never carries a suspicious 1969 / 1970-01-01
//! for what is on disk actually "no timestamp".

use std::time::{SystemTime, UNIX_EPOCH};

/// A snapshot of a file's Unix timestamps, in seconds since 1970-01-01.
///
/// Every field is `Option<u64>` so partial sources (tar has only mtime) don't
/// have to fabricate the others. When a field is `None` the destination
/// driver falls back to whatever it would use for a genuinely new file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UnixTimes {
    /// Modification time — the "when the content last changed" one, the
    /// number `ls -l` and `tar` display. This is the field that matters
    /// most; both dir_import and tar_import populate it.
    pub mtime: Option<u64>,
    /// Access time — often equal to mtime on write. dir_import captures
    /// the host file's atime; tar has no access-time field so tar_import
    /// leaves it None (driver falls back to mtime or now).
    pub atime: Option<u64>,
    /// Metadata-change time — bumped by chmod / rename on POSIX. Almost
    /// never available at import time (tar doesn't carry it, hosts don't
    /// usually expose it portably), so this is usually None → driver uses
    /// mtime or now.
    pub ctime: Option<u64>,
}

impl UnixTimes {
    /// Snapshot with all three fields set to the same value. What
    /// dir_import uses when the host has only `st_mtime` to hand.
    pub fn all(secs: u64) -> Self {
        Self {
            mtime: Some(secs),
            atime: Some(secs),
            ctime: Some(secs),
        }
    }

    /// Snapshot with only mtime set. What tar_import produces (tar has no
    /// atime/ctime fields).
    pub fn mtime_only(secs: u64) -> Self {
        Self {
            mtime: Some(secs),
            atime: None,
            ctime: None,
        }
    }

    /// Convenience: mtime if set, else atime, else ctime, else `now`. The
    /// pattern most drivers want when writing a single-field on-disk time
    /// (e.g. UFS's `di_mtime` only) — pick the most user-meaningful value
    /// the caller supplied without overwriting it with `now`.
    pub fn mtime_or_now(&self) -> u64 {
        self.mtime.or(self.atime).or(self.ctime).unwrap_or_else(now)
    }

    /// Convenience: atime if set, else mtime, else ctime, else `now`.
    pub fn atime_or_now(&self) -> u64 {
        self.atime.or(self.mtime).or(self.ctime).unwrap_or_else(now)
    }

    /// Convenience: ctime if set, else mtime, else atime, else `now`.
    pub fn ctime_or_now(&self) -> u64 {
        self.ctime.or(self.mtime).or(self.atime).unwrap_or_else(now)
    }

    /// True when every field is None (equivalent to `Default::default`).
    pub fn is_empty(&self) -> bool {
        self.mtime.is_none() && self.atime.is_none() && self.ctime.is_none()
    }
}

/// Current wall-clock time as seconds since UNIX epoch. Clamped to 0 on the
/// (impossible) pre-1970 system clock. Replaces every driver's
/// `SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()`
/// dance with one function.
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `now()` clamped into a u32 (used by drivers with 32-bit time fields —
/// EFS, UFS-v1, XFS-v4, FAT/exFAT date halves). u32 overflows in 2106; we
/// document but do not guard against it (no user has a 2106-vintage disk).
pub fn now_u32() -> u32 {
    now() as u32
}

/// The `now`-fallback rule the drivers use in one place: return the given
/// `UnixTimes` if any field is set, otherwise a fresh all-fields `now`.
///
/// Not every driver wants this shape — some pick fields à la carte via
/// [`UnixTimes::mtime_or_now`] — but for the common "stamp all three
/// identically" case it captures the "preserve if given, else now" rule
/// so no driver open-codes the branch.
pub fn resolve_or_now(supplied: Option<UnixTimes>) -> UnixTimes {
    match supplied {
        Some(t) if !t.is_empty() => t,
        _ => UnixTimes::all(now()),
    }
}

// ---------------------------------------------------------------------------
// Civil-date helpers (no chrono dep so the vintage build stays lean)
// ---------------------------------------------------------------------------

/// Break Unix seconds into (year, month 1..=12, day 1..=31, hour, minute,
/// second). Uses Howard Hinnant's `civil_from_days` (same one
/// `format_unix_timestamp` in unix_common::inode uses); safe for any u64
/// value we can actually store on disk. Deliberately not exported — the
/// format-specific encoders below are the public API.
fn ymd_hms(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let sec_of_day = secs % 86400;
    let hour = (sec_of_day / 3600) as u32;
    let minute = ((sec_of_day % 3600) / 60) as u32;
    let second = (sec_of_day % 60) as u32;

    let mut days = (secs / 86400) as i64;
    days += 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d, hour, minute, second)
}

/// Inverse of `ymd_hms` — pack (year, month, day, hour, minute, second) back
/// into Unix seconds. Uses Howard Hinnant's `days_from_civil` companion.
/// Any (year, month, day) triple the encoders emit round-trips exactly.
/// Invalid inputs (month 0, day > 31, etc.) still return *something*, but the
/// decoders always range-check first.
fn secs_from_ymd_hms(year: i64, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> u64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let m = month;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe as i64 - 719468;
    (days as u64) * 86400 + hour as u64 * 3600 + minute as u64 * 60 + second as u64
}

/// True when a Gregorian year is a leap year.
fn is_leap(year: i64) -> bool {
    (year % 4 == 0) && (year % 100 != 0 || year % 400 == 0)
}

/// Days in month for a given Gregorian year.
fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// FAT / exFAT / Human68k — DOS packed date + time (16-bit each)
// ---------------------------------------------------------------------------

/// Seconds between 1970-01-01 and 1980-01-01 (DOS epoch). 3652 days.
const DOS_EPOCH_SECS: u64 = 315_532_800;

/// Encode Unix seconds as (fat_date, fat_time) — the two 16-bit words FAT
/// dirent slots hold. `fat_date` layout: `year_since_1980 (7) | month (4)
/// | day (5)`; `fat_time` layout: `hour (5) | minute (6) | second/2 (5)`.
///
/// Any Unix time before 1980-01-01 clamps to 1980-01-01 00:00:00 (the
/// earliest representable DOS date) so a pre-1980 mtime doesn't become
/// year-2107. Any time past 2107-12-31 is truncated to the year mod 128,
/// which is the same behaviour as every DOS filesystem tool going back to
/// MS-DOS 2.0 — the year field is only 7 bits and there is nowhere else
/// to put a bigger value.
pub fn unix_to_dos_datetime(secs: u64) -> (u16, u16) {
    let secs = secs.max(DOS_EPOCH_SECS);
    let (year, month, day, hour, minute, second) = ymd_hms(secs);
    let year_since_1980 = ((year - 1980).clamp(0, 127)) as u16;
    let date = (year_since_1980 << 9) | ((month as u16 & 0x0F) << 5) | (day as u16 & 0x1F);
    let time =
        ((hour as u16 & 0x1F) << 11) | ((minute as u16 & 0x3F) << 5) | ((second as u16 / 2) & 0x1F);
    (date, time)
}

/// Decode a FAT (date, time) pair back to Unix seconds. Returns `None` for
/// a zero `date` (the "no date set" sentinel every DOS filesystem tool
/// uses) or for a date field whose components don't make sense (month 0,
/// day 0, day > days-in-month, etc). Values are treated as UTC — FAT
/// stores wall-clock time with no timezone, and every tool we round-trip
/// against does the same.
pub fn dos_datetime_to_unix(date: u16, time: u16) -> Option<u64> {
    if date == 0 {
        return None;
    }
    let day = (date & 0x1F) as u32;
    let month = ((date >> 5) & 0x0F) as u32;
    let year = ((date >> 9) & 0x7F) as i64 + 1980;
    let second = ((time & 0x1F) as u32) * 2;
    let minute = ((time >> 5) & 0x3F) as u32;
    let hour = ((time >> 11) & 0x1F) as u32;
    if month == 0 || month > 12 || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(secs_from_ymd_hms(year, month, day, hour, minute, second))
}

/// exFAT packs the two 16-bit DOS words into a single u32 (date in the
/// upper half, time in the lower). Layer over [`unix_to_dos_datetime`].
pub fn unix_to_exfat_timestamp(secs: u64) -> u32 {
    let (date, time) = unix_to_dos_datetime(secs);
    ((date as u32) << 16) | time as u32
}

/// Inverse of [`unix_to_exfat_timestamp`].
pub fn exfat_timestamp_to_unix(ts: u32) -> Option<u64> {
    dos_datetime_to_unix((ts >> 16) as u16, ts as u16)
}

// ---------------------------------------------------------------------------
// NTFS FILETIME — 100-ns intervals since 1601-01-01
// ---------------------------------------------------------------------------

/// Seconds between the NTFS epoch (1601-01-01) and the Unix epoch (1970-01-01).
const NTFS_EPOCH_OFFSET_SECS: u64 = 11_644_473_600;

/// Encode Unix seconds as an NTFS FILETIME (100-nanosecond intervals since
/// 1601-01-01). Sub-second precision is lost, which is fine — every other
/// source in the import chain (host stat, tar Header, cross-fs copy) is
/// second-granular anyway.
pub fn unix_to_filetime(secs: u64) -> u64 {
    secs.saturating_add(NTFS_EPOCH_OFFSET_SECS)
        .saturating_mul(10_000_000)
}

/// Decode an NTFS FILETIME back to Unix seconds. Returns `None` for a
/// zero FILETIME (NTFS's "unset" sentinel) or for a FILETIME that would
/// land before 1970 (the tar oracle would render those as 1969 either
/// way, and no real NTFS volume dates a file before 1970).
pub fn filetime_to_unix(ft: u64) -> Option<u64> {
    if ft == 0 {
        return None;
    }
    let total_secs = ft / 10_000_000;
    if total_secs < NTFS_EPOCH_OFFSET_SECS {
        return None;
    }
    Some(total_secs - NTFS_EPOCH_OFFSET_SECS)
}

// ---------------------------------------------------------------------------
// HFS / HFS+ / MFS — Mac epoch (u32 seconds since 1904-01-01)
// ---------------------------------------------------------------------------

/// Seconds between 1904-01-01 (Mac epoch) and 1970-01-01 (Unix epoch).
/// Duplicated from hfs_common so the encoder is reachable outside the
/// HFS/HFS+ modules (MFS uses the same epoch and lives elsewhere).
const MAC_EPOCH_DELTA: u64 = 2_082_844_800;

/// Encode Unix seconds as a Mac epoch u32, clamping at both ends: a bogus
/// future mtime on an extracted archive used to wrap into 1904..1970.
pub fn unix_to_mac_epoch(secs: u64) -> u32 {
    u32::try_from(secs.saturating_add(MAC_EPOCH_DELTA)).unwrap_or(u32::MAX)
}

/// Decode a Mac epoch u32 back to Unix seconds. Mirrors
/// `hfs_common::mac_date_to_unix` — returns `None` for zero (the "no date"
/// sentinel) and for pre-1970 Mac dates (which would tar out as 1969).
pub fn mac_epoch_to_unix(mac_secs: u32) -> Option<u64> {
    if mac_secs == 0 || (mac_secs as u64) < MAC_EPOCH_DELTA {
        return None;
    }
    Some(mac_secs as u64 - MAC_EPOCH_DELTA)
}

// ---------------------------------------------------------------------------
// ProDOS — packed date + time (2×u16)
// ---------------------------------------------------------------------------

/// Encode Unix seconds as a ProDOS (date, time) pair. Date layout: `year
/// (7) | month (4) | day (5)` where year is `year - 2000` when < 40 else
/// `year - 1900` (ProDOS's own two-digit convention — years 40..99 mean
/// 1940..1999, years 0..39 mean 2000..2039). Time layout: `hour (5) |
/// minute (6)` (no seconds — ProDOS is minute-granular).
///
/// Clamps to 1940-01-01 for pre-1940 input (the earliest representable
/// ProDOS date under its own convention) and to 2039-12-31 for
/// post-2039 input.
pub fn unix_to_prodos_datetime(secs: u64) -> (u16, u16) {
    let (mut year, month, day, hour, minute, _second) = ymd_hms(secs);
    if year < 1940 {
        return (
            ((40u16) << 9) | (1u16 << 5) | 1u16, // 1940-01-01
            0,
        );
    }
    if year > 2039 {
        year = 2039;
    }
    let year_bits = if year >= 2000 {
        (year - 2000) as u16
    } else {
        (year - 1900) as u16
    };
    let date = ((year_bits & 0x7F) << 9) | ((month as u16 & 0x0F) << 5) | (day as u16 & 0x1F);
    let time = ((hour as u16 & 0x1F) << 8) | (minute as u16 & 0x3F);
    (date, time)
}

/// Decode a ProDOS (date, time) pair back to Unix seconds. Returns `None`
/// for a zero `date` ("no date set" — ProDOS Technical Note #28 formalises
/// this).
pub fn prodos_datetime_to_unix(date: u16, time: u16) -> Option<u64> {
    if date == 0 {
        return None;
    }
    let day = (date & 0x1F) as u32;
    let month = ((date >> 5) & 0x0F) as u32;
    let raw_year = ((date >> 9) & 0x7F) as i64;
    let year = if raw_year < 40 {
        2000 + raw_year
    } else {
        1900 + raw_year
    };
    if year < 1970 {
        return None;
    }
    let minute = (time & 0x3F) as u32;
    let hour = ((time >> 8) & 0x1F) as u32;
    if month == 0 || month > 12 || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 {
        return None;
    }
    Some(secs_from_ymd_hms(year, month, day, hour, minute, 0))
}

// ---------------------------------------------------------------------------
// UCSD Pascal — packed date (u16: day | month | year)
// ---------------------------------------------------------------------------

/// Encode Unix seconds as a UCSD Pascal packed date. Layout (little-endian
/// as it sits in the dirent, but treated here as a plain u16):
/// `year (7) | month (4) | day (5)` — years are 2-digit like ProDOS, but
/// UCSD's convention is different: 0..99 all map to 1900..1999.
///
/// Clamps to 1900-01-01 for pre-1900 input and to 1999-12-31 for
/// post-1999 input. Time-of-day is dropped — UCSD is day-granular.
pub fn unix_to_ucsd_date(secs: u64) -> u16 {
    let (year, month, day, _h, _m, _s) = ymd_hms(secs);
    let year = year.clamp(1900, 1999);
    let year_bits = (year - 1900) as u16;
    (day as u16 & 0x1F) | ((month as u16 & 0x0F) << 5) | ((year_bits & 0x7F) << 9)
}

/// Decode a UCSD Pascal packed date to Unix seconds (00:00:00 of that day).
/// Returns `None` for zero, and for pre-1970 dates (which would round-trip
/// through tar as 1969 anyway; the year field's 1900..1999 range means any
/// UCSD-native date can be pre-1970, so this guard matters).
pub fn ucsd_date_to_unix(word: u16) -> Option<u64> {
    if word == 0 {
        return None;
    }
    let day = (word & 0x1F) as u32;
    let month = ((word >> 5) & 0x0F) as u32;
    let year = 1900 + ((word >> 9) & 0x7F) as i64;
    if year < 1970 {
        return None;
    }
    if month == 0 || month > 12 || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    Some(secs_from_ymd_hms(year, month, day, 0, 0, 0))
}

// ---------------------------------------------------------------------------
// ADFS RISC OS — 40-bit centiseconds since 1900-01-01, packed into
// (load_addr low 8 bits, exec_addr all 32 bits) with load_addr high bits
// `0xFFFtt000` (filetype `tt`, plus the 0xFFF marker)
// ---------------------------------------------------------------------------

/// Centiseconds between 1900-01-01 and 1970-01-01. 70 years × 365.2425
/// days × 86400 s × 100 = 220_898_880_000 cs. (Includes 17 leap days:
/// 70/4 = 17.5, minus 1900 not being a leap year = 17.)
const ADFS_EPOCH_OFFSET_CS: u64 = 220_898_880_000;

/// Encode Unix seconds as an ADFS timestamped (load_addr, exec_addr)
/// pair carrying the given RISC OS filetype (12 bits: 0xFFF = Data,
/// 0xFEB = Obey, etc.). `load_addr = 0xFFF_00000 | ((ft & 0xFFF) <<
/// 8) | ((cs40 >> 32) as u8)`; `exec_addr = cs40 as u32`. The 0xFFF-in-
/// high-12-bits pattern is the "this is a datestamp, not a load
/// address" marker every RISC OS tool checks for.
pub fn unix_to_adfs_time(secs: u64, filetype: u16) -> (u32, u32) {
    let cs = secs
        .saturating_mul(100)
        .saturating_add(ADFS_EPOCH_OFFSET_CS);
    let load_addr = 0xFFF0_0000u32 | ((filetype as u32 & 0xFFF) << 8) | ((cs >> 32) as u32 & 0xFF);
    let exec_addr = cs as u32;
    (load_addr, exec_addr)
}

/// Decode an ADFS (load_addr, exec_addr) pair to Unix seconds. Returns
/// `None` when load_addr's high 12 bits aren't `0xFFF` (which means the
/// pair is a real load/exec address, not a datestamp) or when the
/// resulting timestamp would land before 1970.
pub fn adfs_time_to_unix(load: u32, exec: u32) -> Option<u64> {
    if load & 0xFFF0_0000 != 0xFFF0_0000 {
        return None;
    }
    let cs = ((load as u64 & 0xFF) << 32) | exec as u64;
    if cs < ADFS_EPOCH_OFFSET_CS {
        return None;
    }
    Some((cs - ADFS_EPOCH_OFFSET_CS) / 100)
}

// ---------------------------------------------------------------------------
// OS-9 — FD.DAT (5-byte last-modified Y-M-D-H-M) + FD.DCR (3-byte
// creation Y-M-D). Each byte is a plain binary component; year is
// offset from 1900.
// ---------------------------------------------------------------------------

/// Encode Unix seconds as OS-9 FD.DAT (5 bytes: year-1900, month, day,
/// hour, minute). OS-9's year is a single unsigned byte — 1900..2155
/// representable. Pre-1900 clamps to 1900-01-01; post-2155 clamps to
/// 2155-12-31.
pub fn unix_to_os9_dat(secs: u64) -> [u8; 5] {
    let (year, month, day, hour, minute, _s) = ymd_hms(secs);
    let year = year.clamp(1900, 2155);
    [
        (year - 1900) as u8,
        month as u8,
        day as u8,
        hour as u8,
        minute as u8,
    ]
}

/// Encode Unix seconds as OS-9 FD.DCR (3 bytes: year-1900, month, day).
pub fn unix_to_os9_dcr(secs: u64) -> [u8; 3] {
    let dat = unix_to_os9_dat(secs);
    [dat[0], dat[1], dat[2]]
}

/// Decode an OS-9 FD.DAT to Unix seconds. Returns `None` for all-zero
/// (unwritten) or nonsensical component values.
pub fn os9_dat_to_unix(dat: &[u8; 5]) -> Option<u64> {
    if dat == &[0u8; 5] {
        return None;
    }
    let year = 1900i64 + dat[0] as i64;
    if year < 1970 {
        return None;
    }
    let month = dat[1] as u32;
    let day = dat[2] as u32;
    let hour = dat[3] as u32;
    let minute = dat[4] as u32;
    if month == 0 || month > 12 || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 {
        return None;
    }
    Some(secs_from_ymd_hms(year, month, day, hour, minute, 0))
}

/// Decode an OS-9 FD.DCR (creation date only) to Unix seconds. Returns
/// `None` for all-zero or pre-1970 dates.
pub fn os9_dcr_to_unix(dcr: &[u8; 3]) -> Option<u64> {
    if dcr == &[0u8; 3] {
        return None;
    }
    let year = 1900i64 + dcr[0] as i64;
    if year < 1970 {
        return None;
    }
    let month = dcr[1] as u32;
    let day = dcr[2] as u32;
    if month == 0 || month > 12 || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    Some(secs_from_ymd_hms(year, month, day, 0, 0, 0))
}

// ---------------------------------------------------------------------------
// QDOS (Sinclair QL) — u32 seconds since 1961-01-01
// ---------------------------------------------------------------------------

/// Seconds between 1961-01-01 and 1970-01-01 (Unix epoch). 9 years:
/// (365 * 9) + 2 leap days (1964, 1968) = 3287 days × 86400 = 283_996_800 s.
const QDOS_EPOCH_OFFSET_SECS: u64 = 283_996_800;

/// Encode Unix seconds as a QDOS timestamp (u32 seconds since 1961-01-01,
/// big-endian on disk — but the encoding is byte-order-agnostic here).
/// Pre-1961 clamps to 0 (1961-01-01); post-2097-02-06 truncates by u32
/// wraparound. No QDOS tool guards against either.
pub fn unix_to_qdos_date(secs: u64) -> u32 {
    (secs.saturating_add(QDOS_EPOCH_OFFSET_SECS)) as u32
}

/// Decode a QDOS timestamp to Unix seconds. Returns `None` for zero (the
/// "no date set" sentinel) or for QDOS values that would land before 1970.
pub fn qdos_date_to_unix(ts: u32) -> Option<u64> {
    if ts == 0 || (ts as u64) < QDOS_EPOCH_OFFSET_SECS {
        return None;
    }
    Some(ts as u64 - QDOS_EPOCH_OFFSET_SECS)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 2020-06-15 12:34:56 UTC — a mid-range date every encoder can hold.
    const T_2020: u64 = 1_592_224_496;

    #[test]
    fn dos_datetime_round_trips_and_clamps() {
        let (d, t) = unix_to_dos_datetime(T_2020);
        // 2020 = 1980 + 40, month=6, day=15, hour=12, minute=34, second=56 (56/2=28)
        assert_eq!(d, (40 << 9) | (6 << 5) | 15);
        assert_eq!(t, (12 << 11) | (34 << 5) | 28);
        // Round-trip: 2-second granularity means we get 56 back (not 57).
        assert_eq!(dos_datetime_to_unix(d, t), Some(T_2020));

        // Zero -> None (the "no date" sentinel).
        assert_eq!(dos_datetime_to_unix(0, 0), None);
        // Pre-DOS-epoch clamps to 1980-01-01 00:00:00.
        assert_eq!(unix_to_dos_datetime(0), ((1 << 5) | 1, 0));
        // Nonsense date (month 0) -> None. date=1 encodes day=1, month=0, year=1980.
        assert_eq!(dos_datetime_to_unix(0x0001, 0), None);
    }

    #[test]
    fn exfat_timestamp_wraps_dos_helpers() {
        let ts = unix_to_exfat_timestamp(T_2020);
        assert_eq!(exfat_timestamp_to_unix(ts), Some(T_2020));
        assert_eq!(exfat_timestamp_to_unix(0), None);
    }

    #[test]
    fn filetime_round_trips_and_rejects_zero() {
        let ft = unix_to_filetime(T_2020);
        assert_eq!(filetime_to_unix(ft), Some(T_2020));
        assert_eq!(filetime_to_unix(0), None);
        // A FILETIME whose value would decode to pre-1970 is refused.
        assert_eq!(filetime_to_unix(1), None);
    }

    #[test]
    fn mac_epoch_round_trips() {
        let m = unix_to_mac_epoch(T_2020);
        assert_eq!(mac_epoch_to_unix(m), Some(T_2020));
        assert_eq!(mac_epoch_to_unix(0), None);
        // A Mac-epoch value that would decode before 1970 is refused.
        assert_eq!(mac_epoch_to_unix(100), None);
    }

    #[test]
    fn prodos_datetime_round_trips() {
        // ProDOS drops seconds; use a time on a minute boundary for round-trip.
        let t = 1_592_224_440; // 2020-06-15 12:34:00 UTC
        let (d, t_word) = unix_to_prodos_datetime(t);
        assert_eq!(prodos_datetime_to_unix(d, t_word), Some(t));
        assert_eq!(prodos_datetime_to_unix(0, 0), None);
    }

    #[test]
    fn prodos_year_convention_bounds() {
        // Year 20 (0..39) -> 2020.
        let (d, _) = unix_to_prodos_datetime(1_592_224_440);
        assert_eq!((d >> 9) & 0x7F, 20);
        // Year 90 (40..99) -> 1990. 1990-06-15 12:34:00 UTC.
        let t_1990 = 645_453_240;
        let (d90, t90) = unix_to_prodos_datetime(t_1990);
        assert_eq!((d90 >> 9) & 0x7F, 90);
        assert_eq!(prodos_datetime_to_unix(d90, t90), Some(t_1990));
    }

    #[test]
    fn ucsd_date_round_trips_day_granular() {
        let t = 1_592_179_200; // 2020-06-15 00:00:00 UTC — but UCSD only holds 1900..1999.
                               // For UCSD, use 1990.
        let t_1990 = 645_408_000; // 1990-06-15 00:00:00
        let w = unix_to_ucsd_date(t_1990);
        assert_eq!(ucsd_date_to_unix(w), Some(t_1990));
        assert_eq!(ucsd_date_to_unix(0), None);
        // Silence the 2020 unused-warning.
        let _ = t;
    }

    #[test]
    fn adfs_time_round_trips_with_filetype() {
        let (load, exec) = unix_to_adfs_time(T_2020, 0xFFF);
        // High 12 bits must be 0xFFF (the datestamp marker).
        assert_eq!(load & 0xFFF0_0000, 0xFFF0_0000);
        // Filetype 0xFFF stored in bits 8..20.
        assert_eq!((load >> 8) & 0xFFF, 0xFFF);
        assert_eq!(adfs_time_to_unix(load, exec), Some(T_2020));
        // A non-datestamp load address (high bits != 0xFFF) -> None.
        assert_eq!(adfs_time_to_unix(0x0000_8000, 0), None);
    }

    #[test]
    fn os9_dat_and_dcr_round_trip() {
        // OS-9 is minute-granular; use a boundary time.
        let t = 1_592_224_440; // 2020-06-15 12:34:00
        let dat = unix_to_os9_dat(t);
        assert_eq!(dat, [120, 6, 15, 12, 34]);
        assert_eq!(os9_dat_to_unix(&dat), Some(t));

        let dcr = unix_to_os9_dcr(t);
        assert_eq!(dcr, [120, 6, 15]);
        // Creation-date decode is 00:00 of the day.
        assert_eq!(os9_dcr_to_unix(&dcr), Some(1_592_179_200));

        assert_eq!(os9_dat_to_unix(&[0; 5]), None);
        assert_eq!(os9_dcr_to_unix(&[0; 3]), None);
    }

    #[test]
    fn qdos_date_round_trips() {
        let q = unix_to_qdos_date(T_2020);
        assert_eq!(qdos_date_to_unix(q), Some(T_2020));
        assert_eq!(qdos_date_to_unix(0), None);
    }

    /// Every non-DOS encoder is expected to preserve the exact input we
    /// give it. This is the tar-round-trip guarantee: a source mtime that
    /// went in must come back out on the other side, and any format-
    /// granularity loss is documented.
    #[test]
    fn encoders_preserve_mid_range_values_verbatim() {
        // NTFS: nanosecond precision, so a whole-second input round-trips
        // exactly.
        assert_eq!(filetime_to_unix(unix_to_filetime(T_2020)), Some(T_2020));
        // Mac epoch: second-granular.
        assert_eq!(mac_epoch_to_unix(unix_to_mac_epoch(T_2020)), Some(T_2020));
        // QDOS: second-granular.
        assert_eq!(qdos_date_to_unix(unix_to_qdos_date(T_2020)), Some(T_2020));
        // ADFS: centisecond-granular but we input whole seconds.
        let (l, e) = unix_to_adfs_time(T_2020, 0xFFF);
        assert_eq!(adfs_time_to_unix(l, e), Some(T_2020));
    }
}
