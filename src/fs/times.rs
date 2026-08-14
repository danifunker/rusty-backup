//! Per-file Unix timestamps shared across the create / import / export paths.
//!
//! The goal is date-preservation on copy: when rb-cli copies a file from a host
//! folder, a tar archive, or another disk image into a Unix-flavoured
//! filesystem, the destination should record the *source's* mtime, not "now".
//! And a `tar_export` of that image should carry the same mtime back out.
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
