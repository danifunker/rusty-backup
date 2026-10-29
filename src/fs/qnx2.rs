//! QNX 2 filesystem (QNX 2.x, and the Unisys ICON's System Software and QDOS): read and edit.
//!
//! The on-disk reader, writer and checker live in the MIT-licensed [`qnx2fs`] crate,
//! shared with icon-emu; format fixes go there. This module adapts its `Volume` to the
//! `Filesystem` / `EditableFilesystem` traits: deleted entries are listed on request as
//! `NAME [deleted]`, owners as `group.member`, and the ICON attribute letters go into
//! `native_attrs`. On-disk facts and how the writes were checked: `docs/PLAN-qnx.md`.
//!
//! Dates are the ICON's local wall clock, so they are read and written in the host's
//! time zone (like classic HFS): `modified_unix` is that wall time as a host instant,
//! and an incoming Unix time becomes the host's wall clock before the crate stores it.
//! QNX 2 holds only 1980-1995: the crate folds other years into 1994 (29 February
//! into 1992), and `sync_metadata` logs how many new entries that moved. Directories
//! created with an explicit date get it back at sync, after their children were added.

use std::collections::HashSet;
use std::io::{Read, Seek, Write};

use chrono::{DateTime, Local, Offset, TimeZone, Utc};
use qnx2fs::{Entry, NewEntry, Volume};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::fsck::{FsckIssue, FsckResult, FsckStats};

pub use qnx2fs::{decode_date, fold_date, looks_like_qnx2, looks_like_rs_text, rs_to_lf, RS};

const BLOCK: u64 = 512;
const DELETED_TAG: &str = " [deleted]";

impl From<qnx2fs::Error> for FilesystemError {
    fn from(e: qnx2fs::Error) -> Self {
        match e {
            qnx2fs::Error::Io(e) => FilesystemError::Io(e),
            qnx2fs::Error::NotQnx2 => FilesystemError::Parse("not a QNX 2 volume".into()),
            qnx2fs::Error::InvalidData(m) => FilesystemError::InvalidData(format!("qnx2: {m}")),
            qnx2fs::Error::NotFound(m) => FilesystemError::NotFound(m),
            qnx2fs::Error::AlreadyExists(m) => FilesystemError::AlreadyExists(m),
            qnx2fs::Error::DiskFull(m) => FilesystemError::DiskFull(m),
            qnx2fs::Error::Unsupported(m) => FilesystemError::Unsupported(format!("qnx2: {m}")),
        }
    }
}

pub struct Qnx2Filesystem<R: Read + Seek + Send> {
    vol: Volume<R>,
    show_deleted: bool,
    /// New entries whose date fell outside 1980-1995 since the last sync.
    folded: u32,
    /// Directories created with an explicit date: (path, first block, date words).
    dir_dates: Vec<(String, u32, [u16; 2])>,
}

impl<R: Read + Seek + Send> Qnx2Filesystem<R> {
    pub fn open(reader: R, offset: u64) -> Result<Self, FilesystemError> {
        Ok(Self {
            vol: Volume::open(reader, offset)?,
            show_deleted: false,
            folded: 0,
            dir_dates: Vec::new(),
        })
    }
}

impl<R: Read + Write + Seek + Send> Qnx2Filesystem<R> {
    /// Directory and deletion dates use the host's wall clock, as the ICON's own clock would.
    fn stamp_now(&mut self) {
        self.vol.set_clock(unix_to_wall(Utc::now().timestamp()));
    }
}

/// ICON wall-clock seconds as a host instant, in the zone rules of that date.
pub fn wall_to_unix(wall: i64) -> i64 {
    let Some(utc) = DateTime::<Utc>::from_timestamp(wall, 0) else {
        return wall;
    };
    let naive = utc.naive_utc();
    match Local.from_local_datetime(&naive).earliest() {
        Some(t) => t.timestamp(),
        // A wall time skipped by a DST change: use the offset just after the gap.
        None => {
            wall - i64::from(
                Local
                    .offset_from_utc_datetime(&naive)
                    .fix()
                    .local_minus_utc(),
            )
        }
    }
}

/// A host instant as the wall-clock seconds the ICON would show.
pub fn unix_to_wall(unix: i64) -> i64 {
    match Local.timestamp_opt(unix, 0).single() {
        Some(t) => t.naive_local().and_utc().timestamp(),
        None => unix,
    }
}

/// The wall-clock date to store for an optional source mtime (else now), counting a fold.
fn wall_date(mtime: Option<u64>, folded: &mut u32) -> i64 {
    let wall = unix_to_wall(mtime.map_or_else(|| Utc::now().timestamp(), |t| t as i64));
    if fold_date(wall) != wall {
        *folded += 1;
    }
    wall
}

/// The crate's handle for a listed entry: its path, first extent and (possibly tagged) name.
fn entry_ref(e: &FileEntry) -> Entry {
    let mut r = Entry::reference(&e.path, e.location as u32, e.is_directory());
    r.name = e.name.clone();
    r
}

fn file_entry(parent: &str, e: &Entry, name: String) -> FileEntry {
    let path = format!("{}/{}", parent.trim_end_matches('/'), name);
    let mut f = if e.is_dir {
        FileEntry::new_directory(name, path, u64::from(e.first))
    } else {
        FileEntry::new_file(name, path, e.size, u64::from(e.first))
    };
    f.mode = Some(e.unix_mode());
    f.uid = Some(u32::from(e.member));
    f.gid = Some(u32::from(e.group));
    f.owner_label = Some(e.owner_label());
    if let Some(wall) = e.modified() {
        if let Ok(secs) = u64::try_from(wall_to_unix(wall)) {
            f.modified_unix = Some(secs);
        }
        f.modified = Some(super::unix_common::inode::format_unix_timestamp(wall));
    }
    f.native_attrs = Some(e.describe());
    f.qnx2_attrs = Some(e.attributes());
    f
}

fn new_entry(mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, wall: i64) -> NewEntry {
    NewEntry {
        mode,
        uid,
        gid,
        mtime: Some(wall),
        ..Default::default()
    }
}

impl<R: Read + Seek + Send> Qnx2Filesystem<R> {
    /// Settings for a new entry: another QNX 2 entry's attributes and date words exactly, else
    /// the Unix mode / owner and a (folded) host date.
    fn new_entry_for(
        &mut self,
        attrs: Option<qnx2fs::Attributes>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        mtime: Option<u64>,
    ) -> NewEntry {
        match attrs {
            Some(a) => NewEntry {
                attributes: Some(a),
                ..Default::default()
            },
            None => new_entry(mode, uid, gid, wall_date(mtime, &mut self.folded)),
        }
    }
}

/// Check a QNX 2 volume: chains, cross-links, and the bitmap against the blocks files hold.
pub fn fsck_qnx2<R: Read + Seek + Send>(
    fs: &mut Qnx2Filesystem<R>,
) -> Result<FsckResult, FilesystemError> {
    let r = fs.vol.check()?;
    let issue = |i: qnx2fs::Issue| FsckIssue {
        code: i.code,
        message: i.message,
        repairable: false,
        debug: i.debug,
    };
    Ok(FsckResult {
        errors: r.errors.into_iter().map(issue).collect(),
        warnings: r.warnings.into_iter().map(issue).collect(),
        stats: FsckStats {
            files_checked: r.files,
            directories_checked: r.dirs,
            extra: vec![
                ("Blocks".into(), r.total_blocks.to_string()),
                ("Free blocks".into(), r.free_blocks.to_string()),
                (
                    "Deleted entries".into(),
                    format!("{} ({} recoverable)", r.deleted, r.recoverable),
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
        root.location = u64::from(self.vol.root_first());
        Ok(root)
    }

    fn list_directory(&mut self, entry: &FileEntry) -> Result<Vec<FileEntry>, FilesystemError> {
        let entries = self.vol.list_dir(&entry_ref(entry), self.show_deleted)?;
        let mut taken: HashSet<String> = entries
            .iter()
            .filter(|e| e.deleted.is_none())
            .map(|e| e.name.clone())
            .collect();
        let mut out = Vec::with_capacity(entries.len());
        for e in &entries {
            let name = if e.deleted.is_none() {
                e.name.clone()
            } else {
                let mut name = format!("{}{DELETED_TAG}", e.name);
                let mut n = 2;
                while !taken.insert(name.clone()) {
                    name = format!("{} [deleted {n}]", e.name);
                    n += 1;
                }
                name
            };
            out.push(file_entry(&entry.path, e, name));
        }
        Ok(out)
    }

    fn read_file(&mut self, entry: &FileEntry, max: usize) -> Result<Vec<u8>, FilesystemError> {
        Ok(self.vol.read_file(&entry_ref(entry), max)?)
    }

    fn volume_label(&self) -> Option<&str> {
        None
    }

    fn fs_type(&self) -> &str {
        "QNX 2"
    }

    fn validate_name(&self, name: &str) -> Result<(), FilesystemError> {
        Ok(qnx2fs::validate_name(name)?)
    }

    fn total_size(&self) -> u64 {
        self.vol.total_blocks() * BLOCK
    }

    fn used_size(&self) -> u64 {
        self.vol
            .total_blocks()
            .saturating_sub(self.vol.free_blocks())
            * BLOCK
    }

    fn fsck(&mut self) -> Option<Result<FsckResult, FilesystemError>> {
        Some(fsck_qnx2(self))
    }

    fn set_show_deleted(&mut self, on: bool) -> bool {
        self.show_deleted = on;
        true
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
        self.stamp_now();
        let mtime = options.unix_times.and_then(|t| t.mtime);
        let opts = self.new_entry_for(
            options.qnx2_attrs,
            options.mode,
            options.uid,
            options.gid,
            mtime,
        );
        let e = self
            .vol
            .create_file(&entry_ref(parent), name, data, data_len, &opts)?;
        Ok(file_entry(&parent.path, &e, e.name.clone()))
    }

    fn create_directory(
        &mut self,
        parent: &FileEntry,
        name: &str,
        options: &CreateDirectoryOptions,
    ) -> Result<FileEntry, FilesystemError> {
        self.stamp_now();
        let mtime = options.unix_times.and_then(|t| t.mtime);
        let opts = self.new_entry_for(
            options.qnx2_attrs,
            options.mode,
            options.uid,
            options.gid,
            mtime,
        );
        let e = self.vol.create_dir(&entry_ref(parent), name, &opts)?;
        if mtime.is_some() || options.qnx2_attrs.is_some() {
            self.dir_dates.push((e.path.clone(), e.first, e.date));
        }
        Ok(file_entry(&parent.path, &e, e.name.clone()))
    }

    fn delete_entry(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
    ) -> Result<(), FilesystemError> {
        self.stamp_now();
        Ok(self.vol.delete(&entry_ref(parent), &entry_ref(entry))?)
    }

    fn rename(
        &mut self,
        parent: &FileEntry,
        entry: &FileEntry,
        new_name: &str,
    ) -> Result<(), FilesystemError> {
        self.stamp_now();
        Ok(self
            .vol
            .rename(&entry_ref(parent), &entry_ref(entry), new_name)?)
    }

    fn set_permissions(&mut self, entry: &FileEntry, mode: u32) -> Result<(), FilesystemError> {
        Ok(self.vol.set_permissions(&entry_ref(entry), mode)?)
    }

    fn set_owner(&mut self, entry: &FileEntry, uid: u32, gid: u32) -> Result<(), FilesystemError> {
        Ok(self.vol.set_owner(&entry_ref(entry), uid, gid)?)
    }

    fn sync_metadata(&mut self) -> Result<(), FilesystemError> {
        for (path, first, date) in std::mem::take(&mut self.dir_dates) {
            if let Ok(e) = self.vol.lookup(&path) {
                if e.is_dir && e.first == first {
                    let mut a = e.attributes();
                    a.date = date;
                    self.vol.set_attributes(&e, a)?;
                }
            }
        }
        if self.folded > 0 {
            log::warn!(
                "QNX 2 stores years 1980-1995: {} {} dated with the same month, day and time in 1994 (29 February in 1992)",
                self.folded,
                if self.folded == 1 { "entry was" } else { "entries were" }
            );
            self.folded = 0;
        }
        Ok(self.vol.flush()?)
    }

    fn free_space(&mut self) -> Result<u64, FilesystemError> {
        Ok(self.vol.free_blocks() * BLOCK)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    const ROOT_AT: usize = 68;
    const ENTRY: usize = 48;

    /// A 16-block volume: root at block 3 with `HELLO` (block 5), dir `sub` (block 4), a deleted
    /// `GONE` (block 6) and a bitmap file at block 2; the root's header names its own entry.
    fn sample() -> Cursor<Vec<u8>> {
        let mut img = vec![0u8; 16 * 512];
        let entry = |img: &mut Vec<u8>, at: usize, status: u8, name: &[u8], first: u32, attr| {
            img[at] = status;
            img[at + 1..at + 5].copy_from_slice(&first.to_le_bytes());
            img[at + 26] = attr;
            img[at + 24] = 0x21;
            img[at + 25] = 0x01;
            img[at + 31..at + 31 + name.len()].copy_from_slice(name);
        };
        let hdr = |img: &mut Vec<u8>, block: usize, size: u32| {
            let at = (block - 1) * 512;
            img[at + 8..at + 12].copy_from_slice(&size.to_le_bytes());
        };
        entry(&mut img, ROOT_AT, 0x40, b"", 3, 0x25);
        let dir = 2 * 512 + 16;
        img[dir..dir + 4].copy_from_slice(&[1, 0, 1, 0]);
        entry(&mut img, dir + 4, 0x40, b"HELLO", 5, 0x01);
        entry(&mut img, dir + 4 + ENTRY, 0x40, b"sub", 4, 0x25);
        entry(&mut img, dir + 4 + 2 * ENTRY, 0, b"GONE", 6, 0x11);
        entry(&mut img, dir + 4 + 3 * ENTRY, 0x40, b"bitmap", 2, 0x01);
        hdr(&mut img, 3, 4 + 4 * ENTRY as u32);
        hdr(&mut img, 4, 4);
        hdr(&mut img, 5, 6);
        img[4 * 512 + 16..4 * 512 + 22].copy_from_slice(b"hello\n");
        hdr(&mut img, 2, 2);
        // Blocks 1-5 in use; 6 (the deleted file) free.
        img[512 + 16] = 0x1F;
        hdr(&mut img, 6, 3);
        img[5 * 512 + 16..5 * 512 + 19].copy_from_slice(b"old");
        Cursor::new(img)
    }

    #[test]
    fn lists_with_owner_attrs_and_tags_deleted_entries() {
        let mut fs = Qnx2Filesystem::open(sample(), 0).unwrap();
        let root = fs.root().unwrap();
        let list = fs.list_directory(&root).unwrap();
        let hello = list.iter().find(|e| e.name == "HELLO").unwrap();
        assert_eq!(fs.read_file(hello, usize::MAX).unwrap(), b"hello\n");
        assert_eq!(hello.path, "/HELLO");
        assert_eq!(hello.owner_label.as_deref(), Some("0.0"));
        assert!(hello.native_attrs.as_deref().unwrap().starts_with("owner "));
        assert!(!list.iter().any(|e| e.name.contains("GONE")));
        assert!(fs.set_show_deleted(true));
        let list = fs.list_directory(&root).unwrap();
        let gone = list.iter().find(|e| e.name == "GONE [deleted]").unwrap();
        assert_eq!(fs.read_file(gone, usize::MAX).unwrap(), b"old");
        assert!(gone
            .native_attrs
            .as_deref()
            .unwrap()
            .ends_with("deleted, recoverable"));
    }

    #[test]
    fn trait_edits_round_trip_and_check_clean() {
        let mut fs = Qnx2Filesystem::open(sample(), 0).unwrap();
        let root = fs.root().unwrap();
        let body: Vec<u8> = (0..1500u32).map(|i| i as u8).collect();
        let f = fs
            .create_file(&root, "data.bin", &mut &body[..], 1500, &Default::default())
            .unwrap();
        assert_eq!(fs.read_file(&f, usize::MAX).unwrap(), body);
        let d = fs
            .create_directory(&root, "newdir", &Default::default())
            .unwrap();
        let inner = fs
            .create_file(&d, "x", &mut &b"hi\x1e"[..], 3, &Default::default())
            .unwrap();
        fs.set_permissions(&inner, 0o750).unwrap();
        fs.set_owner(&inner, 4, 12).unwrap();
        let listed = fs.list_directory(&d).unwrap();
        assert_eq!(listed[0].mode.map(|m| m & 0o777), Some(0o750));
        assert_eq!(listed[0].owner_label.as_deref(), Some("12.4"));
        let r = fsck_qnx2(&mut fs).unwrap();
        assert!(r.is_clean(), "{:?}", r.errors);
        fs.rename(&root, &f, "renamed").unwrap();
        let renamed = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "renamed")
            .unwrap();
        let free_before = fs.free_space().unwrap();
        fs.delete_entry(&root, &renamed).unwrap();
        assert!(fs.free_space().unwrap() > free_before);
        assert!(fs.delete_entry(&root, &d).is_err(), "not empty");
        fs.delete_entry(&d, &listed[0]).unwrap();
        fs.delete_entry(&root, &d).unwrap();
        assert!(fs.validate_name("$lead").is_err());
        let r = fsck_qnx2(&mut fs).unwrap();
        assert!(r.is_clean(), "{:?}", r.errors);
    }

    /// Unix seconds for a host-local wall-clock time.
    fn local(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> u64 {
        let naive = chrono::NaiveDate::from_ymd_opt(y, mo, d)
            .and_then(|x| x.and_hms_opt(h, mi, 0))
            .unwrap();
        wall_to_unix(naive.and_utc().timestamp()) as u64
    }

    fn put(fs: &mut Qnx2Filesystem<Cursor<Vec<u8>>>, name: &str, mtime: u64) -> FileEntry {
        let root = fs.root().unwrap();
        let opts = CreateFileOptions {
            unix_times: Some(crate::fs::times::UnixTimes::mtime_only(mtime)),
            ..Default::default()
        };
        fs.create_file(&root, name, &mut &b"x"[..], 1, &opts)
            .unwrap()
    }

    #[test]
    fn cp_between_qnx2_volumes_keeps_raw_attributes() {
        // Bits a Unix mode cannot hold: write-only owner, group r--e-, other a+m, a directory's b.
        let mut img = sample().into_inner();
        let dir = 2 * 512 + 16 + 4;
        img[dir + 26] = 0x12;
        img[dir + 24] = 0x09;
        img[dir + 25] = 0x14;
        img[dir + 15] = 7;
        img[dir + 16] = 3;
        img[dir + 27..dir + 31].copy_from_slice(&[0x45, 0x2C, 0x10, 0x0E]);
        img[dir + ENTRY + 26] = 0x2D;
        let mut src = Qnx2Filesystem::open(Cursor::new(img), 0).unwrap();
        let mut dst = Qnx2Filesystem::open(sample(), 0).unwrap();
        let (sroot, droot) = (src.root().unwrap(), dst.root().unwrap());
        let listed = src.list_directory(&sroot).unwrap();
        let dsub = dst
            .list_directory(&droot)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "sub")
            .unwrap();
        let opts = crate::fs::copy::CopyOptions {
            recursive: true,
            ..Default::default()
        };
        let mut stats = Default::default();
        for name in ["HELLO", "sub"] {
            let e = listed.iter().find(|e| e.name == name).unwrap();
            crate::fs::copy::copy_into(
                &mut src,
                e,
                &mut dst,
                &dsub,
                name,
                &opts,
                &mut stats,
                &|_| {},
            )
            .unwrap();
        }
        dst.sync_metadata().unwrap();
        let copied = dst.list_directory(&dsub).unwrap();
        for name in ["HELLO", "sub"] {
            let a = listed.iter().find(|e| e.name == name).unwrap();
            let b = copied.iter().find(|e| e.name == name).unwrap();
            assert_eq!(a.qnx2_attrs, b.qnx2_attrs, "{name}");
            // A written entry is flagged modified, as QNX flags it.
            let letters = |e: &FileEntry| e.native_attrs.clone().unwrap().replace("; modified", "");
            assert_eq!(letters(a), letters(b), "{name}");
        }
        assert_eq!(stats.dropped_perms, 0);
        assert!(fsck_qnx2(&mut dst).unwrap().is_clean());
    }

    #[test]
    fn modern_dates_fold_into_1994_in_local_time() {
        let mut fs = Qnx2Filesystem::open(sample(), 0).unwrap();
        let cases = [
            ("a", local(2026, 3, 14, 10, 0), "1994-03-14 10:00:00"),
            ("b", local(2024, 2, 29, 9, 0), "1992-02-29 09:00:00"),
            ("c", local(2024, 3, 1, 8, 0), "1994-03-01 08:00:00"),
            ("d", local(1988, 3, 23, 16, 5), "1988-03-23 16:05:00"),
        ];
        for (name, t, _) in cases {
            put(&mut fs, name, t);
        }
        assert_eq!(fs.folded, 3);
        fs.sync_metadata().unwrap();
        assert_eq!(fs.folded, 0);
        let root = fs.root().unwrap();
        let list = fs.list_directory(&root).unwrap();
        for (name, t, shown) in cases {
            let e = list.iter().find(|e| e.name == name).unwrap();
            assert_eq!(e.modified.as_deref(), Some(shown), "{name}");
            if name == "d" {
                assert_eq!(
                    e.modified_unix,
                    Some(t),
                    "an in-range date reads back as the same instant"
                );
            }
        }
    }

    #[test]
    fn dates_round_trip_and_directories_keep_theirs() {
        let mut src = Qnx2Filesystem::open(sample(), 0).unwrap();
        let t = local(1985, 10, 3, 23, 41);
        let f = put(&mut src, "old", t);
        assert_eq!(f.modified.as_deref(), Some("1985-10-03 23:41:00"));
        assert_eq!(f.modified_unix, Some(t));
        // Feeding a listed date back in, as cp / tar + untar do, stores the same words.
        let again = put(&mut src, "copy", f.modified_unix.unwrap());
        assert_eq!(again.modified, f.modified);
        let root = src.root().unwrap();
        let dopts = CreateDirectoryOptions {
            unix_times: Some(crate::fs::times::UnixTimes::mtime_only(t)),
            ..Default::default()
        };
        let d = src.create_directory(&root, "dir", &dopts).unwrap();
        src.create_file(&d, "child", &mut &b"y"[..], 1, &Default::default())
            .unwrap();
        src.sync_metadata().unwrap();
        let d = src
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "dir")
            .unwrap();
        assert_eq!(d.modified.as_deref(), Some("1985-10-03 23:41:00"));
        assert!(fsck_qnx2(&mut src).unwrap().is_clean());
    }
}
