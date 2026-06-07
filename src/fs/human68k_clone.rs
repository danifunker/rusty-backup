//! Defragmenting Human68k clone: walk a source Human68k (X68000) volume
//! and replay every directory and file onto a freshly-formatted target
//! volume, allocating clusters contiguously as it goes.
//!
//! This is the Human68k peer of [`crate::fs::pfs3_clone::clone_pfs3_volume`]
//! and the practical "repack / shrink my partition" path: the in-place
//! resizer ([`crate::fs::human68k::resize_human68k_in_place`]) keeps every
//! cluster at its original byte offset so existing files stay byte-exact,
//! which means it can only trim trailing free space — it can't reclaim
//! holes left by deleted files. The clone rebuilds the volume packed.
//!
//! Pre-conditions:
//! - `source` is an opened `Human68kFilesystem` (read-only is enough).
//! - `target` is freshly built via [`create_blank_human68k`] and opened
//!   for read+write; its root is empty.
//! - The target is large enough to hold every file's data plus the new
//!   FAT / root-directory overhead. The streaming wrapper sizes it.
//!
//! Post-conditions:
//! - Every dir/file from `source` exists at the same path on `target`,
//!   contiguously allocated, with names (Shift-JIS) preserved byte-exact.
//! - `target.sync_metadata()` has been called.
//!
//! Fidelity notes (Human68k is FAT-derived — much simpler than PFS3):
//! - There are no symlinks or hardlinks to replay.
//! - The `EditableFilesystem` write path stamps a fresh ARCHIVE attribute
//!   and a zero timestamp, so file **modification dates** and the
//!   **read-only** attribute are not carried across (the same limitation
//!   the interactive edit path already has). A single summary warning is
//!   emitted when read-only entries are dropped to ARCHIVE so the user
//!   knows.
//!
//! Memory / scope notes:
//! - File contents are spooled through a `Vec<u8>` per file, then written
//!   via `target.create_file`. Peak buffer is one file's data. X68000 HDD
//!   payloads (games, dev tools) are comfortably within RAM budgets.

use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::human68k::{create_blank_human68k, Human68kFilesystem};

/// Per-clone result: counters + non-fatal warnings to surface in the
/// GUI / CLI log.
#[derive(Debug, Default, Clone)]
pub struct Human68kCloneReport {
    pub files_copied: u64,
    pub dirs_copied: u64,
    pub bytes_copied: u64,
    pub warnings: Vec<String>,
}

/// Throttled progress channel for the clone walk. `log_cb` fires at most
/// every ~2 seconds with a running summary so the user sees the process
/// is alive even though no output bytes are emitted yet.
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
    fn tick(&mut self, report: &Human68kCloneReport, force: bool) {
        let now = Instant::now();
        if !force && now.duration_since(self.last_emit) < self.interval {
            return;
        }
        self.last_emit = now;
        (self.log_cb)(&format!(
            "Human68k clone progress: {} dirs / {} files / {} MiB",
            report.dirs_copied,
            report.files_copied,
            report.bytes_copied / (1024 * 1024)
        ));
    }
}

/// Clone every reachable directory and file from `source` to `target`.
/// `log_cb` receives throttled progress lines (no leading newline).
pub fn clone_human68k_volume<RS, RT>(
    source: &mut Human68kFilesystem<RS>,
    target: &mut Human68kFilesystem<RT>,
    log_cb: &mut dyn FnMut(&str),
) -> Result<Human68kCloneReport, FilesystemError>
where
    RS: Read + Seek + Send,
    RT: Read + Write + Seek + Send,
{
    let mut report = Human68kCloneReport::default();
    let mut readonly_dropped: u64 = 0;
    let mut progress = CloneProgress::new(log_cb);

    let src_root = source.root()?;
    let tgt_root = target.root()?;
    walk(
        source,
        target,
        &src_root,
        &tgt_root,
        &mut report,
        &mut readonly_dropped,
        &mut progress,
    )?;
    progress.tick(&report, true);

    if readonly_dropped > 0 {
        report.warnings.push(format!(
            "{readonly_dropped} read-only entr{} re-created with the default ARCHIVE \
             attribute (the Human68k write path does not preserve the read-only flag)",
            if readonly_dropped == 1 {
                "y was"
            } else {
                "ies were"
            }
        ));
    }

    EditableFilesystem::sync_metadata(target)?;
    Ok(report)
}

/// DFS walker. Recurses into directories in source-listing order; Human68k
/// trees are shallow, so the recursion stack stays bounded.
fn walk<RS, RT>(
    source: &mut Human68kFilesystem<RS>,
    target: &mut Human68kFilesystem<RT>,
    src_dir: &FileEntry,
    tgt_dir: &FileEntry,
    report: &mut Human68kCloneReport,
    readonly_dropped: &mut u64,
    progress: &mut CloneProgress<'_>,
) -> Result<(), FilesystemError>
where
    RS: Read + Seek + Send,
    RT: Read + Write + Seek + Send,
{
    let kids = source.list_directory(src_dir)?;
    for k in kids {
        // list_directory tags read-only entries with special_type "R/O";
        // count them so we can warn once that the flag isn't carried over.
        if k.special_type.as_deref() == Some("R/O") {
            *readonly_dropped += 1;
        }
        if k.is_directory() {
            let new_dir =
                target.create_directory(tgt_dir, &k.name, &CreateDirectoryOptions::default())?;
            report.dirs_copied += 1;
            progress.tick(report, false);
            walk(
                source,
                target,
                &k,
                &new_dir,
                report,
                readonly_dropped,
                progress,
            )?;
        } else if k.is_file() {
            // Spool the file body through a Vec. Peak RAM is one file.
            let body = source.read_file(&k, usize::MAX)?;
            let mut cur = Cursor::new(&body);
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

/// Streaming wrapper around [`clone_human68k_volume`]: format a blank
/// target of exactly `target_size` bytes (matching `source`'s format),
/// clone `source` into it via a tempfile (to bound RAM use), then drain
/// the tempfile into `dst`.
///
/// This is the entry point the CLI `repack` verb (and any future export
/// pipeline) calls. `target_size` MUST be a multiple of the source's
/// sector size and large enough to hold the packed contents — the caller
/// is responsible for validation; [`create_blank_human68k`] enforces the
/// FAT16 floor.
///
/// The function never holds the full image in RAM: peak buffer is one
/// file's data (within the clone) plus the I/O ring around the tempfile.
pub fn stream_defragmented_human68k<R, W>(
    source: &mut Human68kFilesystem<R>,
    target_size: u64,
    dst: &mut W,
    log_cb: &mut dyn FnMut(&str),
    progress_cb: &mut dyn FnMut(u64),
) -> Result<Human68kCloneReport, FilesystemError>
where
    R: Read + Seek + Send,
    W: Write,
{
    let template = source.format_template()?;
    let bps = template.bytes_per_sector() as u64;
    if !target_size.is_multiple_of(bps) {
        return Err(FilesystemError::InvalidData(format!(
            "stream_defragmented_human68k: target_size {target_size} is not a multiple \
             of the sector size {bps}"
        )));
    }
    log_cb(&format!(
        "Human68k clone: formatting blank {} MiB target volume",
        target_size / (1024 * 1024)
    ));
    let blank = create_blank_human68k(&template, target_size)?;
    debug_assert_eq!(blank.len() as u64, target_size);

    // Tempfile keeps peak memory bounded by one source-file buffer plus
    // the I/O ring instead of the whole target image.
    let mut tmp = tempfile::tempfile().map_err(|e| {
        FilesystemError::Io(io::Error::other(format!(
            "create tempfile for Human68k clone: {e}"
        )))
    })?;
    tmp.write_all(&blank)?;
    drop(blank);
    tmp.seek(SeekFrom::Start(0))?;

    let mut target = Human68kFilesystem::open(tmp, 0)?;
    let report = clone_human68k_volume(source, &mut target, log_cb)?;

    // Drain the cloned tempfile into the caller's writer.
    log_cb("Human68k clone: draining cloned image to output");
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
    use byteorder::{BigEndian, ByteOrder};

    /// Build a blank, valid SHARP/KG big-endian FAT16 Human68k image with
    /// `total` 512-byte sectors. 1 sector/cluster, 1 FAT, 16 root entries,
    /// 16-sector FAT — mirrors the `human68k.rs` test fixtures. `total`
    /// must be large enough to clear the 4085-cluster FAT16 floor.
    fn blank_sharp_kg_fat16(total: usize) -> Vec<u8> {
        const BPS: usize = 512;
        const NFATS: usize = 1;
        const FATSZ: usize = 16;
        const RESERVED: usize = 1;
        const ROOT_ENTRIES: usize = 16;
        let mut disk = vec![0u8; total * BPS];
        // BPB: 2-byte BRA.S + 16-byte OEM, big-endian fields at 0x12.
        disk[0] = 0x60;
        disk[1] = 0x24;
        disk[2..18].copy_from_slice(b"SHARP/KG TEST   ");
        BigEndian::write_u16(&mut disk[0x12..0x14], BPS as u16);
        disk[0x14] = 1; // sectors per cluster
        disk[0x15] = NFATS as u8;
        BigEndian::write_u16(&mut disk[0x16..0x18], RESERVED as u16);
        BigEndian::write_u16(&mut disk[0x18..0x1A], ROOT_ENTRIES as u16);
        BigEndian::write_u16(&mut disk[0x1A..0x1C], 0); // total16 unused
        disk[0x1C] = 0xF8; // media descriptor
        disk[0x1D] = FATSZ as u8;
        BigEndian::write_u32(&mut disk[0x1E..0x22], total as u32);
        // FAT reserved entries (big-endian): cluster 0 = 0xFFF8, 1 = EOC.
        let fat = RESERVED * BPS;
        BigEndian::write_u16(&mut disk[fat..fat + 2], 0xFFF8);
        BigEndian::write_u16(&mut disk[fat + 2..fat + 4], 0xFFFF);
        disk
    }

    /// 4108 sectors -> 4090 data clusters -> just over the FAT16 floor.
    const TOTAL_SECTORS: usize = 4108;

    fn open_rw(bytes: Vec<u8>) -> Human68kFilesystem<Cursor<Vec<u8>>> {
        Human68kFilesystem::open(Cursor::new(bytes), 0).expect("open volume")
    }

    fn write_file(fs: &mut Human68kFilesystem<Cursor<Vec<u8>>>, name: &str, body: &[u8]) {
        let root = fs.root().unwrap();
        let mut cur = Cursor::new(body.to_vec());
        fs.create_file(
            &root,
            name,
            &mut cur,
            body.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create_file");
    }

    /// End-to-end: populate a source volume (a subdir + several files,
    /// including a multi-cluster file and a fragmentation hole), clone it
    /// into a fresh blank, and verify the tree + every file's bytes
    /// survive on the packed target.
    #[test]
    fn clone_replays_tree_and_contents() {
        let mut src = open_rw(blank_sharp_kg_fat16(TOTAL_SECTORS));

        // Multi-cluster file (3 clusters @ 512 B).
        let big: Vec<u8> = (0..1500u32).map(|i| (i % 256) as u8).collect();
        write_file(&mut src, "BIG.BIN", &big);
        // Small file we'll delete to leave a hole, fragmenting later allocs.
        write_file(&mut src, "TMP.DAT", &[0xAB; 400]);
        write_file(&mut src, "KEEP.TXT", b"keep me");
        // Delete TMP.DAT so the next file scatters into the freed cluster.
        {
            let root = src.root().unwrap();
            let tmp = src
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "TMP.DAT")
                .unwrap();
            src.delete_entry(&root, &tmp).unwrap();
        }
        // Another multi-cluster file — reuses the hole + tail (fragmented).
        let frag: Vec<u8> = (0..1200u32).map(|i| (i % 251) as u8).collect();
        write_file(&mut src, "FRAG.BIN", &frag);
        // A subdirectory holding a file.
        {
            let root = src.root().unwrap();
            let sub = src
                .create_directory(&root, "SUB", &CreateDirectoryOptions::default())
                .unwrap();
            let mut cur = Cursor::new(b"child payload".to_vec());
            src.create_file(
                &sub,
                "CHILD.TXT",
                &mut cur,
                13,
                &CreateFileOptions::default(),
            )
            .unwrap();
        }
        EditableFilesystem::sync_metadata(&mut src).unwrap();

        // Reopen source read-only and clone into a fresh blank target.
        let src_bytes = src.into_reader().into_inner();
        let mut source = open_rw(src_bytes);
        let template = source.format_template().unwrap();
        let blank = create_blank_human68k(&template, (TOTAL_SECTORS * 512) as u64).unwrap();
        let mut target = open_rw(blank);

        let report = clone_human68k_volume(&mut source, &mut target, &mut |_| {}).unwrap();
        assert_eq!(report.files_copied, 4, "BIG/KEEP/FRAG + SUB/CHILD");
        assert_eq!(report.dirs_copied, 1);
        assert_eq!(
            report.bytes_copied,
            big.len() as u64 + 7 + frag.len() as u64 + 13
        );
        assert!(
            report.warnings.is_empty(),
            "warnings: {:?}",
            report.warnings
        );

        // Verify the cloned target round-trips byte-exact.
        let dst_bytes = target.into_reader().into_inner();
        let mut tgt = open_rw(dst_bytes);
        let root = tgt.root().unwrap();
        let kids = tgt.list_directory(&root).unwrap();
        let names: Vec<&str> = kids.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"BIG.BIN"));
        assert!(names.contains(&"KEEP.TXT"));
        assert!(names.contains(&"FRAG.BIN"));
        assert!(names.contains(&"SUB"));
        assert!(
            !names.contains(&"TMP.DAT"),
            "deleted file must not reappear"
        );

        let big_fe = kids.iter().find(|e| e.name == "BIG.BIN").unwrap();
        assert_eq!(tgt.read_file(big_fe, usize::MAX).unwrap(), big);
        let frag_fe = kids.iter().find(|e| e.name == "FRAG.BIN").unwrap();
        assert_eq!(tgt.read_file(frag_fe, usize::MAX).unwrap(), frag);

        let sub_fe = kids.iter().find(|e| e.name == "SUB").unwrap();
        let sub_kids = tgt.list_directory(sub_fe).unwrap();
        let child = sub_kids.iter().find(|e| e.name == "CHILD.TXT").unwrap();
        assert_eq!(tgt.read_file(child, usize::MAX).unwrap(), b"child payload");
    }

    /// The streaming wrapper writes a complete image into the caller's
    /// writer that re-opens and lists the cloned file.
    #[test]
    fn stream_defragmented_round_trips() {
        let mut src = open_rw(blank_sharp_kg_fat16(TOTAL_SECTORS));
        write_file(&mut src, "DATA.BIN", b"streaming defrag works");
        EditableFilesystem::sync_metadata(&mut src).unwrap();
        let src_bytes = src.into_reader().into_inner();
        let mut source = open_rw(src_bytes);

        let target_size = (TOTAL_SECTORS * 512) as u64;
        let mut dst: Vec<u8> = Vec::new();
        let report = stream_defragmented_human68k(
            &mut source,
            target_size,
            &mut dst,
            &mut |_| {},
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(report.files_copied, 1);
        assert_eq!(dst.len() as u64, target_size);

        let mut fs2 = open_rw(dst);
        let root = fs2.root().unwrap();
        let kids = fs2.list_directory(&root).unwrap();
        let fe = kids.iter().find(|e| e.name == "DATA.BIN").unwrap();
        assert_eq!(
            fs2.read_file(fe, usize::MAX).unwrap(),
            b"streaming defrag works"
        );
    }

    /// FAT12 templates are rejected — the clone is FAT16-only.
    #[test]
    fn create_blank_rejects_fat12() {
        // A standard LE FAT12 floppy BPB (720 KB, like the human68k.rs
        // fixture) parses as FAT12.
        const TOTAL: usize = 1440;
        let mut disk = vec![0u8; TOTAL * 512];
        disk[0] = 0xEB;
        disk[1] = 0x3C;
        disk[2] = 0x90;
        disk[3..11].copy_from_slice(b"X68KFS  ");
        byteorder::LittleEndian::write_u16(&mut disk[11..13], 512);
        disk[13] = 1;
        byteorder::LittleEndian::write_u16(&mut disk[14..16], 1);
        disk[16] = 2;
        byteorder::LittleEndian::write_u16(&mut disk[17..19], 112);
        byteorder::LittleEndian::write_u16(&mut disk[19..21], TOTAL as u16);
        disk[21] = 0xF9;
        byteorder::LittleEndian::write_u16(&mut disk[22..24], 3);
        // FAT reserved entries.
        let fat = 512;
        disk[fat] = 0xF9;
        disk[fat + 1] = 0xFF;
        disk[fat + 2] = 0xFF;

        let mut fs = open_rw(disk);
        let template = fs.format_template().unwrap();
        let err = create_blank_human68k(&template, (TOTAL * 512) as u64).unwrap_err();
        assert!(
            format!("{err}").contains("FAT16"),
            "expected FAT16-only error, got: {err}"
        );
    }
}
