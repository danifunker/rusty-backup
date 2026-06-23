//! Defragmenting exFAT clone: walk a source exFAT volume and replay every
//! directory and file onto a freshly-formatted target volume, allocating
//! clusters contiguously as it goes.
//!
//! This is the exFAT peer of [`crate::fs::human68k_clone`] and the practical
//! "repack / shrink my partition" path. The in-place resizer
//! ([`crate::fs::resize_exfat_in_place`]) keeps every cluster at its original
//! byte offset so existing files stay byte-exact, which means it can only trim
//! trailing free space — it can't reclaim holes left by deleted files, and a
//! single allocated cluster near the end of the volume pins the minimum size
//! near full. The clone rebuilds the volume packed, so a 9%-full-but-fragmented
//! card shrinks to ~its real data size.
//!
//! Why this exists at all: exFAT (and NTFS) smart compaction used to densely
//! pack clusters into the backup stream WITHOUT rewriting the FAT chains, which
//! reference absolute cluster numbers — silently corrupting non-resident files
//! on restore. That packer was replaced with a layout-preserving stream (safe
//! but unshrinkable). This clone is the correct way to pack: it goes through the
//! `EditableFilesystem` write path, which allocates fresh clusters and writes
//! correct chains, so every file stays byte-exact.
//!
//! Fidelity notes:
//! - exFAT has no symlinks or hardlinks to replay.
//! - The `EditableFilesystem` write path stamps a fixed timestamp, so file
//!   modification dates are not carried across (the same limitation the
//!   interactive edit path has).
//! - Names are preserved; the freshly formatted volume's up-case table folds
//!   only ASCII a-z (see [`crate::fs::exfat::create_blank_exfat`]).
//!
//! Memory / scope notes:
//! - File contents are spooled through a `Vec<u8>` per file, then written via
//!   `target.create_file`. Peak buffer is one file's data. The streaming
//!   wrapper keeps the target image in a tempfile, not RAM.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

use super::entry::FileEntry;
use super::exfat::{create_blank_exfat, ExfatFilesystem};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};

/// Per-clone result: counters + non-fatal warnings to surface in the GUI / CLI
/// log.
#[derive(Debug, Default, Clone)]
pub struct ExfatCloneReport {
    pub files_copied: u64,
    pub dirs_copied: u64,
    pub bytes_copied: u64,
    pub warnings: Vec<String>,
}

/// Throttled progress channel for the clone walk. Fires at most every ~2s.
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
    fn tick(&mut self, report: &ExfatCloneReport, force: bool) {
        let now = Instant::now();
        if !force && now.duration_since(self.last_emit) < self.interval {
            return;
        }
        self.last_emit = now;
        (self.log_cb)(&format!(
            "exFAT clone progress: {} dirs / {} files / {} MiB",
            report.dirs_copied,
            report.files_copied,
            report.bytes_copied / (1024 * 1024)
        ));
    }
}

/// Clone every reachable directory and file from `source` to `target`.
/// `target` must be freshly formatted (empty root) via [`create_blank_exfat`].
pub fn clone_exfat_volume<RS, RT>(
    source: &mut ExfatFilesystem<RS>,
    target: &mut ExfatFilesystem<RT>,
    log_cb: &mut dyn FnMut(&str),
) -> Result<ExfatCloneReport, FilesystemError>
where
    RS: Read + Seek + Send,
    RT: Read + Write + Seek + Send,
{
    let mut report = ExfatCloneReport::default();
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

/// DFS walker. exFAT trees from cards/cameras are shallow; recursion stays
/// bounded in practice.
fn walk<RS, RT>(
    source: &mut ExfatFilesystem<RS>,
    target: &mut ExfatFilesystem<RT>,
    src_dir: &FileEntry,
    tgt_dir: &FileEntry,
    report: &mut ExfatCloneReport,
    progress: &mut CloneProgress<'_>,
) -> Result<(), FilesystemError>
where
    RS: Read + Seek + Send,
    RT: Read + Write + Seek + Send,
{
    let kids = source.list_directory(src_dir)?;
    for k in kids {
        if k.is_directory() {
            let new_dir =
                target.create_directory(tgt_dir, &k.name, &CreateDirectoryOptions::default())?;
            report.dirs_copied += 1;
            progress.tick(report, false);
            walk(source, target, &k, &new_dir, report, progress)?;
        } else if k.is_file() {
            // read_file caps at max_bytes (cluster-rounded), so pass the file's
            // exact DataLength to get a byte-exact body.
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

/// Streaming wrapper around [`clone_exfat_volume`]: format a blank target of
/// exactly `target_size` bytes (same cluster size + label as `source`), clone
/// `source` into it via a tempfile (to bound RAM), then drain the tempfile into
/// `dst`. The entry point the backup defrag-clone path calls.
///
/// `target_size` MUST be a multiple of the sector size and large enough to hold
/// the packed contents plus format overhead — the caller sizes it.
pub fn stream_defragmented_exfat<R, W>(
    source: &mut ExfatFilesystem<R>,
    target_size: u64,
    dst: &mut W,
    log_cb: &mut dyn FnMut(&str),
    progress_cb: &mut dyn FnMut(u64),
) -> Result<ExfatCloneReport, FilesystemError>
where
    R: Read + Seek + Send,
    W: Write,
{
    let template = source.format_template();
    if !target_size.is_multiple_of(template.bytes_per_sector) {
        return Err(FilesystemError::InvalidData(format!(
            "stream_defragmented_exfat: target_size {target_size} is not a multiple of the \
             sector size {}",
            template.bytes_per_sector
        )));
    }

    log_cb(&format!(
        "exFAT clone: formatting blank {} MiB target volume",
        target_size / (1024 * 1024)
    ));

    // Tempfile keeps peak memory bounded by one source-file buffer plus the
    // I/O ring instead of the whole target image.
    let mut tmp = tempfile::tempfile().map_err(|e| {
        FilesystemError::Io(io::Error::other(format!(
            "create tempfile for exFAT clone: {e}"
        )))
    })?;
    create_blank_exfat(&mut tmp, &template, target_size)?;
    tmp.seek(SeekFrom::Start(0))?;

    let mut target = ExfatFilesystem::open(tmp, 0)?;
    let report = clone_exfat_volume(source, &mut target, log_cb)?;

    // Drain the cloned tempfile into the caller's writer.
    log_cb("exFAT clone: draining cloned image to output");
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
    use crate::fs::exfat::ExfatFormatTemplate;
    use std::io::Cursor;

    fn template() -> ExfatFormatTemplate {
        ExfatFormatTemplate {
            bytes_per_sector: 512,
            sectors_per_cluster: 8, // 4 KiB clusters
            label: Some("SRC".to_string()),
        }
    }

    fn blank(size: u64) -> Cursor<Vec<u8>> {
        let mut cur = Cursor::new(Vec::<u8>::new());
        create_blank_exfat(&mut cur, &template(), size).expect("format blank");
        cur.seek(SeekFrom::Start(0)).unwrap();
        cur
    }

    fn write_file(
        fs: &mut ExfatFilesystem<Cursor<Vec<u8>>>,
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
    fn stream_defragmented_round_trips() {
        // -- Build a populated source: a subdir + several files, including a
        //    multi-cluster (non-resident) file and an empty file. --
        let mut src = ExfatFilesystem::open(blank(16 * 1024 * 1024), 0).unwrap();
        let root = src.root().unwrap();
        write_file(&mut src, &root, "readme.txt", b"hello exfat");
        let big = vec![0x5Au8; 20_000]; // ~5 clusters at 4 KiB
        write_file(&mut src, &root, "big.bin", &big);
        write_file(&mut src, &root, "empty.dat", b"");
        let sub = src
            .create_directory(&root, "sub", &CreateDirectoryOptions::default())
            .unwrap();
        write_file(&mut src, &sub, "nested.bin", &[1, 2, 3, 4, 5, 6, 7, 8]);
        EditableFilesystem::sync_metadata(&mut src).unwrap();

        // -- Clone-stream into a fresh, smaller target. --
        let target_size: u64 = 12 * 1024 * 1024;
        let mut out = Cursor::new(Vec::<u8>::new());
        let report =
            stream_defragmented_exfat(&mut src, target_size, &mut out, &mut |_| {}, &mut |_| {})
                .expect("stream clone");
        assert_eq!(report.files_copied, 4);
        assert_eq!(report.dirs_copied, 1);
        assert_eq!(out.get_ref().len() as u64, target_size);

        // -- Verify the cloned volume: tree + byte-exact contents. --
        out.seek(SeekFrom::Start(0)).unwrap();
        let mut dst = ExfatFilesystem::open(out, 0).unwrap();
        let droot = dst.root().unwrap();
        let entries = dst.list_directory(&droot).unwrap();
        for (name, want) in [("readme.txt", &b"hello exfat"[..]), ("big.bin", &big[..])] {
            let e = entries
                .iter()
                .find(|e| e.name == name)
                .expect("file present");
            assert_eq!(
                dst.read_file(e, e.size as usize).unwrap(),
                want,
                "{name} bytes"
            );
        }
        assert!(entries.iter().any(|e| e.name == "empty.dat" && e.size == 0));
        let sub = entries
            .iter()
            .find(|e| e.name == "sub" && e.is_directory())
            .expect("subdir present");
        let nested = dst.list_directory(sub).unwrap();
        let n = nested
            .iter()
            .find(|e| e.name == "nested.bin")
            .expect("nested file");
        assert_eq!(
            dst.read_file(n, n.size as usize).unwrap(),
            vec![1, 2, 3, 4, 5, 6, 7, 8]
        );
    }

    #[test]
    fn tight_sizing_packs_a_mostly_empty_volume_small() {
        use crate::fs::exfat::exfat_min_packed_size;

        // A large (64 MiB) source holding only ~40 KiB of data, like the
        // real card: lots of free space, a little data.
        let mut src = ExfatFilesystem::open(blank(64 * 1024 * 1024), 0).unwrap();
        let root = src.root().unwrap();
        let payload = vec![0xC3u8; 40_000];
        write_file(&mut src, &root, "payload.bin", &payload);
        EditableFilesystem::sync_metadata(&mut src).unwrap();

        // Size the target tightly from the source's used clusters.
        let target_size = exfat_min_packed_size(&src.format_template(), src.used_clusters());
        // The packed image is a small fraction of the 64 MiB source.
        assert!(
            target_size < 4 * 1024 * 1024,
            "packed size {target_size} should be a few MiB, not ~64 MiB"
        );

        let mut out = Cursor::new(Vec::<u8>::new());
        stream_defragmented_exfat(&mut src, target_size, &mut out, &mut |_| {}, &mut |_| {})
            .expect("clone must fit in the tight target");
        assert_eq!(out.get_ref().len() as u64, target_size);

        // Contents survive the shrink byte-exact.
        out.seek(SeekFrom::Start(0)).unwrap();
        let mut dst = ExfatFilesystem::open(out, 0).unwrap();
        let droot = dst.root().unwrap();
        let e = dst
            .list_directory(&droot)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "payload.bin")
            .expect("payload present");
        assert_eq!(dst.read_file(&e, e.size as usize).unwrap(), payload);
    }

    #[test]
    fn defrag_clone_packs_a_fragmented_volume_and_detects_shape() {
        use crate::fs::exfat::exfat_min_packed_size;

        // 32 MiB volume: write a big file, then a small tail after it, then
        // delete the big one. The freed region leaves a large hole so the
        // in-place trim (last allocated cluster = tail, high up) stays large
        // while the packed size only needs the tail + metadata.
        let mut fs = ExfatFilesystem::open(blank(32 * 1024 * 1024), 0).unwrap();
        let root = fs.root().unwrap();
        let big = vec![0u8; 4 * 1024 * 1024]; // 1024 clusters at 4 KiB
        write_file(&mut fs, &root, "big.bin", &big);
        let tail = vec![0x7Eu8; 5000];
        write_file(&mut fs, &root, "tail.bin", &tail);
        EditableFilesystem::sync_metadata(&mut fs).unwrap();

        let root = fs.root().unwrap();
        let big_e = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "big.bin")
            .unwrap();
        fs.delete_entry(&root, &big_e).unwrap();
        EditableFilesystem::sync_metadata(&mut fs).unwrap();

        let in_place = fs.last_data_byte().unwrap();
        let packed = fs.defragmented_minimum_size().unwrap();
        assert!(
            packed < in_place,
            "packed {packed} must beat the in-place trim {in_place} on a fragmented volume"
        );
        assert_eq!(
            packed,
            exfat_min_packed_size(&fs.format_template(), fs.used_clusters())
        );

        // Backup preflight seam: the volume is detected as an exFAT clone shape.
        let mut reader = fs.into_reader();
        reader.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(
            crate::fs::detect_defrag_clone_shape(&mut reader, 0, None).unwrap(),
            crate::fs::DefragCloneShape::Exfat
        );

        // Streaming the packed clone yields the small size, and the surviving
        // tail file is byte-exact.
        reader.seek(SeekFrom::Start(0)).unwrap();
        let mut src = ExfatFilesystem::open(reader, 0).unwrap();
        let mut out = Cursor::new(Vec::<u8>::new());
        stream_defragmented_exfat(&mut src, packed, &mut out, &mut |_| {}, &mut |_| {})
            .expect("packed clone");
        assert_eq!(out.get_ref().len() as u64, packed);

        out.seek(SeekFrom::Start(0)).unwrap();
        let mut dst = ExfatFilesystem::open(out, 0).unwrap();
        let droot = dst.root().unwrap();
        let e = dst
            .list_directory(&droot)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "tail.bin")
            .expect("tail survives");
        assert_eq!(dst.read_file(&e, e.size as usize).unwrap(), tail);
    }
}
