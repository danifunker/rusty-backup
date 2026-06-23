//! Defragmenting NTFS clone: walk a source NTFS volume and replay every
//! directory and file onto a freshly-formatted target via the
//! `EditableFilesystem` write path (which allocates fresh clusters + rewrites
//! the MFT data runs correctly). The NTFS peer of [`crate::fs::exfat_clone`].
//!
//! This is the correct way to pack NTFS: the old dense compaction emitted
//! clusters without rewriting the absolute-cluster data runs and silently
//! corrupted non-resident files; it was replaced with a layout-preserving
//! stream (safe but unshrinkable). The clone packs by re-creating every file in
//! a fresh, smaller volume so a fragmented source shrinks to ~its real data.
//!
//! Fidelity: NTFS reparse points / named streams / security descriptors beyond
//! the default are not replayed (the `EditableFilesystem` write path doesn't
//! create them); file modification dates use the write path's stamp. The blank
//! target is built by [`crate::fs::ntfs_format::create_blank_ntfs`] (4096-byte
//! clusters), validated to mount under ntfs-3g.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::ntfs::NtfsFilesystem;
use super::ntfs_format::create_blank_ntfs;

/// Per-clone result: counters + non-fatal warnings.
#[derive(Debug, Default, Clone)]
pub struct NtfsCloneReport {
    pub files_copied: u64,
    pub dirs_copied: u64,
    pub bytes_copied: u64,
    pub warnings: Vec<String>,
}

struct CloneProgress<'a> {
    log_cb: &'a mut dyn FnMut(&str),
    last_emit: Instant,
    interval: Duration,
}

impl<'a> CloneProgress<'a> {
    fn new(log_cb: &'a mut dyn FnMut(&str)) -> Self {
        Self {
            log_cb,
            last_emit: Instant::now() - Duration::from_secs(60),
            interval: Duration::from_secs(2),
        }
    }
    fn tick(&mut self, report: &NtfsCloneReport, force: bool) {
        let now = Instant::now();
        if !force && now.duration_since(self.last_emit) < self.interval {
            return;
        }
        self.last_emit = now;
        (self.log_cb)(&format!(
            "NTFS clone progress: {} dirs / {} files / {} MiB",
            report.dirs_copied,
            report.files_copied,
            report.bytes_copied / (1024 * 1024)
        ));
    }
}

/// Clone every reachable directory and file from `source` to `target`.
pub fn clone_ntfs_volume<RS, RT>(
    source: &mut NtfsFilesystem<RS>,
    target: &mut NtfsFilesystem<RT>,
    log_cb: &mut dyn FnMut(&str),
) -> Result<NtfsCloneReport, FilesystemError>
where
    RS: Read + Seek + Send,
    RT: Read + Write + Seek + Send,
{
    let mut report = NtfsCloneReport::default();
    let mut progress = CloneProgress::new(log_cb);
    let src_root = source.root()?;
    let tgt_root = target.root()?;
    walk(
        source,
        target,
        &src_root,
        &tgt_root,
        &mut report,
        &mut progress,
    )?;
    progress.tick(&report, true);
    EditableFilesystem::sync_metadata(target)?;
    Ok(report)
}

fn walk<RS, RT>(
    source: &mut NtfsFilesystem<RS>,
    target: &mut NtfsFilesystem<RT>,
    src_dir: &FileEntry,
    tgt_dir: &FileEntry,
    report: &mut NtfsCloneReport,
    progress: &mut CloneProgress<'_>,
) -> Result<(), FilesystemError>
where
    RS: Read + Seek + Send,
    RT: Read + Write + Seek + Send,
{
    for k in source.list_directory(src_dir)? {
        // Skip the NTFS system metafiles ($MFT, $Secure, ...) that live under
        // the root — they're recreated by the formatter, not cloned.
        if k.name.starts_with('$') && src_dir.path == "/" {
            continue;
        }
        if k.is_directory() {
            let new_dir =
                target.create_directory(tgt_dir, &k.name, &CreateDirectoryOptions::default())?;
            report.dirs_copied += 1;
            progress.tick(report, false);
            walk(source, target, &k, &new_dir, report, progress)?;
        } else if k.is_file() {
            // read_file caps at max_bytes, so pass the exact size.
            let body = source.read_file(&k, k.size as usize)?;
            let mut cur = io::Cursor::new(&body);
            target.create_file(
                tgt_dir,
                &k.name,
                &mut cur,
                body.len() as u64,
                &CreateFileOptions::default(),
            )?;
            report.files_copied += 1;
            report.bytes_copied += body.len() as u64;
            progress.tick(report, false);
        } else {
            report.warnings.push(format!(
                "Skipped entry {}: unsupported entry type {:?}",
                k.path, k.entry_type
            ));
        }
    }
    Ok(())
}

/// Streaming wrapper: format a blank NTFS of `target_size` (same volume label,
/// MFT sized from the source's capacity), clone `source` into it via a
/// tempfile, then drain to `dst`.
pub fn stream_defragmented_ntfs<R, W>(
    source: &mut NtfsFilesystem<R>,
    target_size: u64,
    dst: &mut W,
    log_cb: &mut dyn FnMut(&str),
    progress_cb: &mut dyn FnMut(u64),
) -> Result<NtfsCloneReport, FilesystemError>
where
    R: Read + Seek + Send,
    W: Write,
{
    let mft_records = source.mft_record_capacity();
    let label = source.volume_label().map(|s| s.to_string());

    log_cb(&format!(
        "NTFS clone: formatting blank {} MiB target volume",
        target_size / (1024 * 1024)
    ));

    let mut tmp = tempfile::tempfile().map_err(|e| {
        FilesystemError::Io(io::Error::other(format!(
            "create tempfile for NTFS clone: {e}"
        )))
    })?;
    create_blank_ntfs(&mut tmp, target_size, mft_records, label.as_deref())?;
    tmp.seek(SeekFrom::Start(0))?;

    let mut target = NtfsFilesystem::open(tmp, 0)?;
    let report = clone_ntfs_volume(source, &mut target, log_cb)?;

    log_cb("NTFS clone: draining cloned image to output");
    let mut tmp = target.into_reader();
    tmp.seek(SeekFrom::Start(0))?;
    let mut buf = vec![0u8; 1024 * 1024];
    let mut emitted: u64 = 0;
    while emitted < target_size {
        let want = ((target_size - emitted) as usize).min(buf.len());
        tmp.read_exact(&mut buf[..want])?;
        dst.write_all(&buf[..want])?;
        emitted += want as u64;
        progress_cb(emitted);
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ntfs_format::{create_blank_ntfs, ntfs_min_packed_size};
    use std::io::Cursor;

    fn blank(size: u64) -> Cursor<Vec<u8>> {
        let mut cur = Cursor::new(Vec::<u8>::new());
        create_blank_ntfs(&mut cur, size, 128, Some("SRC")).unwrap();
        cur.seek(SeekFrom::Start(0)).unwrap();
        cur
    }

    fn write_file(
        fs: &mut NtfsFilesystem<Cursor<Vec<u8>>>,
        parent: &FileEntry,
        name: &str,
        body: &[u8],
    ) {
        let mut cur = Cursor::new(body.to_vec());
        fs.create_file(
            parent,
            name,
            &mut cur,
            body.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file");
    }

    #[test]
    fn stream_defragmented_ntfs_round_trips() {
        let mut src = NtfsFilesystem::open(blank(48 * 1024 * 1024), 0).unwrap();
        let root = src.root().unwrap();
        write_file(&mut src, &root, "readme.txt", b"hello ntfs");
        let big = vec![0x5Au8; 20_000]; // multi-cluster, non-resident
        write_file(&mut src, &root, "big.bin", &big);
        let sub = src
            .create_directory(&root, "sub", &CreateDirectoryOptions::default())
            .unwrap();
        write_file(&mut src, &sub, "nested.bin", &[1, 2, 3, 4, 5, 6, 7, 8]);
        EditableFilesystem::sync_metadata(&mut src).unwrap();

        let target_size = ntfs_min_packed_size(src.used_size(), 64);
        let mut out = Cursor::new(Vec::<u8>::new());
        let report =
            stream_defragmented_ntfs(&mut src, target_size, &mut out, &mut |_| {}, &mut |_| {})
                .expect("stream clone");
        assert_eq!(report.files_copied, 3);
        assert_eq!(report.dirs_copied, 1);
        assert_eq!(out.get_ref().len() as u64, target_size);

        out.seek(SeekFrom::Start(0)).unwrap();
        let mut dst = NtfsFilesystem::open(out, 0).unwrap();
        let droot = dst.root().unwrap();
        let entries = dst.list_directory(&droot).unwrap();
        let e = entries
            .iter()
            .find(|e| e.name == "big.bin")
            .expect("big present");
        assert_eq!(dst.read_file(e, e.size as usize).unwrap(), big);
        let sub = entries
            .iter()
            .find(|e| e.name == "sub" && e.is_directory())
            .expect("subdir present");
        let nested = dst.list_directory(sub).unwrap();
        let n = nested
            .iter()
            .find(|e| e.name == "nested.bin")
            .expect("nested");
        assert_eq!(
            dst.read_file(n, n.size as usize).unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
    }
}
