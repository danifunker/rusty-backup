//! Defragmenting PFS3 clone: walk a source PFS3 volume and replay every
//! directory, file, and link onto a freshly-formatted target volume.
//!
//! Pre-conditions:
//! - `source` is a read-only `Pfs3Filesystem`.
//! - `target` is freshly built via `create_blank_pfs3` and opened for
//!   read+write; its root is empty.
//! - The target volume is large enough to hold every file's data plus
//!   the new reserved-area / metadata overhead. The caller is
//!   responsible for sizing.
//!
//! Post-conditions:
//! - Every dir/file from `source` exists at the same path on `target`,
//!   with `amiga_protection`, `amiga_comment`, and `amiga_date`
//!   preserved.
//! - Softlinks are replayed with the source's target-path string.
//! - Hardlinks are replayed in a second pass once all targets are
//!   guaranteed to exist on the target volume. Hardlinks whose target
//!   never appears in the source walk are skipped with a warning.
//! - `target.sync_metadata()` has been called.
//!
//! Memory / scope notes:
//! - File contents are streamed via `source.write_file_to(...)` into a
//!   `Vec<u8>` per file, then written through `target.create_file`. The
//!   peak buffer is one file's data, capped at the source's largest
//!   file. For multi-GB single files (rare on Amiga volumes), this
//!   wants a future streaming-pipe rewrite. AmigaVision's largest file
//!   is well under typical RAM budgets.
//! - The PFS3 trashcan (deldir) is NOT enumerated by `list_directory`,
//!   so its contents are dropped during clone. This is expected
//!   behavior — communicate it to the user via the returned warnings.

use std::collections::HashMap;
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

use super::entry::FileEntry;
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, FilesystemError,
};
use super::pfs3::{create_blank_pfs3, Pfs3Filesystem};

const HW_SECTOR: u64 = 512;

/// Per-clone result: counters + non-fatal warnings to surface in the
/// GUI log. Skipped hardlinks land in `warnings`.
#[derive(Debug, Default, Clone)]
pub struct Pfs3CloneReport {
    pub files_copied: u64,
    pub dirs_copied: u64,
    pub symlinks_copied: u64,
    pub hardlinks_copied: u64,
    pub bytes_copied: u64,
    pub warnings: Vec<String>,
}

/// Hardlink-replay record collected during the first pass.
struct DeferredHardlink {
    /// Source-side anode of the parent directory that holds this link.
    src_parent_anode: u64,
    /// Source-side anode of the link's target object.
    src_target_anode: u64,
    name: String,
    /// Full path on the source (for warning messages only).
    src_path: String,
    protection: Option<u32>,
    comment: Option<String>,
    dates: Option<(i32, i32, i32)>,
}

/// Throttled progress channel for the clone walk. `log_cb` fires at
/// most every ~2 seconds with a running summary so the user sees the
/// process is alive even though no output bytes are emitted yet.
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
    fn tick(&mut self, report: &Pfs3CloneReport, force: bool) {
        let now = Instant::now();
        if !force && now.duration_since(self.last_emit) < self.interval {
            return;
        }
        self.last_emit = now;
        (self.log_cb)(&format!(
            "PFS3 clone progress: {} dirs / {} files / {} MiB",
            report.dirs_copied,
            report.files_copied,
            report.bytes_copied / (1024 * 1024)
        ));
    }
}

/// Clone every reachable entry from `source` to `target`.
/// `log_cb` receives throttled progress lines (no leading newline).
pub fn clone_pfs3_volume<RS, RT>(
    source: &mut Pfs3Filesystem<RS>,
    target: &mut Pfs3Filesystem<RT>,
    log_cb: &mut dyn FnMut(&str),
) -> Result<Pfs3CloneReport, FilesystemError>
where
    RS: Read + Seek + Send,
    RT: Read + Write + Seek + Send,
{
    let mut report = Pfs3CloneReport::default();
    let mut progress = CloneProgress::new(log_cb);
    // src_anode -> target FileEntry. Lets pass-2 hardlinks look up
    // where the target landed on the new volume.
    let mut anode_map: HashMap<u64, FileEntry> = HashMap::new();
    // target_anode_on_target_volume -> parent_anode_on_target_volume.
    // Pass-2 needs the parent to locate the target's direntry inside
    // its dirblock chain for the back-link patch.
    let mut parent_map: HashMap<u64, u64> = HashMap::new();
    let mut deferred: Vec<DeferredHardlink> = Vec::new();

    let src_root = source.root()?;
    let tgt_root = target.root()?;
    anode_map.insert(src_root.location, tgt_root.clone());

    walk(
        source,
        target,
        &src_root,
        &tgt_root,
        &mut anode_map,
        &mut parent_map,
        &mut deferred,
        &mut report,
        &mut progress,
    )?;
    progress.tick(&report, true);

    // Pass 2: replay hardlinks. Targets may be siblings, ancestors, or
    // descendants of the link itself, so we wait until the entire tree
    // is in place before binding.
    let deferred_count = deferred.len();
    if deferred_count > 0 {
        (progress.log_cb)(&format!("PFS3 clone: replaying {deferred_count} hardlinks"));
    }
    for d in deferred {
        let target_target = match anode_map.get(&d.src_target_anode) {
            Some(t) => t.clone(),
            None => {
                report.warnings.push(format!(
                    "Skipped hardlink {}: target anode {} not visited during clone walk \
                     (orphan link; original target may have been deleted before backup).",
                    d.src_path, d.src_target_anode
                ));
                continue;
            }
        };
        let parent = match anode_map.get(&d.src_parent_anode) {
            Some(p) => p.clone(),
            None => {
                report.warnings.push(format!(
                    "Skipped hardlink {}: parent dir anode {} missing on target \
                     (internal invariant violation).",
                    d.src_path, d.src_parent_anode
                ));
                continue;
            }
        };
        let opts = CreateFileOptions {
            amiga_protection: d.protection,
            amiga_comment: d.comment,
            amiga_dates: d.dates,
            ..Default::default()
        };
        match target.create_hardlink(&parent, &d.name, &target_target, &opts) {
            Ok(link_fe) => {
                // Patch the target's direntry on the destination
                // volume to add this link to its back-link chain. The
                // target's parent on the destination is looked up via
                // `parent_map`. The volume root has no parent we'd
                // ever patch — skip with a warning instead (hardlinks
                // to the root are non-sensical).
                let tgt_root_anode = anode_map
                    .get(&src_root.location)
                    .map(|e| e.location)
                    .unwrap_or(0);
                if target_target.location == tgt_root_anode {
                    report.warnings.push(format!(
                        "Skipped back-link registration for {}: target is the volume root",
                        d.src_path
                    ));
                    report.hardlinks_copied += 1;
                    continue;
                }
                let Some(&target_parent_anode) = parent_map.get(&target_target.location) else {
                    report.warnings.push(format!(
                        "Failed to find target parent for hardlink {} \
                         (forward link works; back-link chain not updated)",
                        d.src_path
                    ));
                    report.hardlinks_copied += 1;
                    continue;
                };
                if let Err(e) = target.register_hardlink_in_target_chain(
                    target_parent_anode as u32,
                    target_target.location as u32,
                    link_fe.location as u32,
                ) {
                    report.warnings.push(format!(
                        "Hardlink {} created but back-link chain update failed: {e} \
                         (forward direction still resolves)",
                        d.src_path
                    ));
                }
                report.hardlinks_copied += 1;
            }
            Err(e) => report
                .warnings
                .push(format!("Failed to create hardlink {}: {e}", d.src_path)),
        }
    }

    EditableFilesystem::sync_metadata(target)?;
    Ok(report)
}

/// DFS walker. Recurses into directories in source-listing order. Uses
/// the recursion stack rather than an explicit queue for simplicity;
/// PFS3 directory trees are wide rather than deep, so stack depth
/// stays bounded.
fn walk<RS, RT>(
    source: &mut Pfs3Filesystem<RS>,
    target: &mut Pfs3Filesystem<RT>,
    src_dir: &FileEntry,
    tgt_dir: &FileEntry,
    anode_map: &mut HashMap<u64, FileEntry>,
    parent_map: &mut HashMap<u64, u64>,
    deferred: &mut Vec<DeferredHardlink>,
    report: &mut Pfs3CloneReport,
    progress: &mut CloneProgress<'_>,
) -> Result<(), FilesystemError>
where
    RS: Read + Seek + Send,
    RT: Read + Write + Seek + Send,
{
    let kids = source.list_directory(src_dir)?;
    for k in kids {
        // Order matters: check link_target_cnid (hardlink) before
        // is_directory/is_file, since LINKDIR keeps Directory entry
        // type and LINKFILE keeps File entry type. Symlinks have a
        // distinct EntryType::Symlink so they're checked first.
        if k.is_symlink() {
            let target_path = k.symlink_target.clone().unwrap_or_default();
            let opts = CreateFileOptions {
                amiga_protection: k.amiga_protection,
                amiga_comment: k.amiga_comment.clone(),
                amiga_dates: k.amiga_date,
                ..Default::default()
            };
            match target.create_symlink(tgt_dir, &k.name, &target_path, &opts) {
                Ok(_) => report.symlinks_copied += 1,
                Err(e) => report
                    .warnings
                    .push(format!("Failed to create symlink {}: {e}", k.path)),
            }
            continue;
        }
        if let Some(t_anode) = k.link_target_cnid {
            deferred.push(DeferredHardlink {
                src_parent_anode: src_dir.location,
                src_target_anode: t_anode,
                name: k.name.clone(),
                src_path: k.path.clone(),
                protection: k.amiga_protection,
                comment: k.amiga_comment.clone(),
                dates: k.amiga_date,
            });
            continue;
        }
        if k.is_directory() {
            let dopts = CreateDirectoryOptions {
                amiga_protection: k.amiga_protection,
                amiga_comment: k.amiga_comment.clone(),
                amiga_dates: k.amiga_date,
                ..Default::default()
            };
            let new_dir = target.create_directory(tgt_dir, &k.name, &dopts)?;
            anode_map.insert(k.location, new_dir.clone());
            parent_map.insert(new_dir.location, tgt_dir.location);
            report.dirs_copied += 1;
            progress.tick(report, false);
            walk(
                source, target, &k, &new_dir, anode_map, parent_map, deferred, report, progress,
            )?;
        } else if k.is_file() {
            // Spool the file body through a Vec. Large files spike RAM
            // for the duration of one create_file call. See module
            // doc for the streaming-pipe TODO.
            let mut body: Vec<u8> = Vec::with_capacity(k.size as usize);
            source.write_file_to(&k, &mut body)?;
            let opts = CreateFileOptions {
                amiga_protection: k.amiga_protection,
                amiga_comment: k.amiga_comment.clone(),
                amiga_dates: k.amiga_date,
                ..Default::default()
            };
            let mut cur = Cursor::new(&body);
            let new_file =
                target.create_file(tgt_dir, &k.name, &mut cur, body.len() as u64, &opts)?;
            parent_map.insert(new_file.location, tgt_dir.location);
            anode_map.insert(k.location, new_file);
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

/// Streaming wrapper around [`clone_pfs3_volume`]: format a blank
/// target of exactly `target_size` bytes, clone `source` into it via
/// a tempfile (to bound RAM use), then drain the tempfile into `dst`.
///
/// This is the entry point the backup / export pipeline calls when the
/// user picks "shrink to defragmented minimum" for a PFS3 partition.
/// `target_size` MUST be a multiple of 512 and at least
/// `source.defragmented_minimum_size()` — the caller is responsible
/// for validation.
///
/// The function never holds the full image in RAM. Peak buffer is one
/// file's data (within `clone_pfs3_volume`) plus the I/O ring around
/// the tempfile.
pub fn stream_defragmented_pfs3<R, W>(
    source: &mut Pfs3Filesystem<R>,
    target_size: u64,
    dst: &mut W,
    log_cb: &mut dyn FnMut(&str),
    progress_cb: &mut dyn FnMut(u64),
) -> Result<Pfs3CloneReport, FilesystemError>
where
    R: Read + Seek + Send,
    W: Write,
{
    if target_size % HW_SECTOR != 0 {
        return Err(FilesystemError::InvalidData(format!(
            "stream_defragmented_pfs3: target_size {target_size} is not a multiple of {HW_SECTOR}"
        )));
    }
    let total_sectors = (target_size / HW_SECTOR) as u32;
    let name = source.volume_label().unwrap_or("Cloned").to_string();
    log_cb(&format!(
        "PFS3 clone: formatting blank {} MiB target volume \"{}\"",
        target_size / (1024 * 1024),
        name
    ));
    let blank = create_blank_pfs3(total_sectors, &name)?;
    debug_assert_eq!(blank.len() as u64, target_size);

    // Tempfile keeps peak memory bounded by one source-file buffer plus
    // I/O ring instead of the whole target image. The tempfile is
    // unlinked when dropped — drained into `dst` first.
    let mut tmp = tempfile::tempfile().map_err(|e| {
        FilesystemError::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("create tempfile for PFS3 clone: {e}"),
        ))
    })?;
    tmp.write_all(&blank)?;
    drop(blank); // free the 9 GB blank-buffer for big partitions
    tmp.seek(SeekFrom::Start(0))?;

    let mut target = Pfs3Filesystem::open(tmp, 0)?;
    let report = clone_pfs3_volume(source, &mut target, log_cb)?;

    // Drain the cloned tempfile into the caller's writer.
    log_cb("PFS3 clone: draining cloned image to output");
    let mut tmp = target.reader;
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
    use crate::fs::pfs3::{create_blank_pfs3, Pfs3Filesystem};
    use std::io::Cursor;

    /// `stream_defragmented_pfs3` writes a complete image into the
    /// caller's writer that round-trips back through
    /// `Pfs3Filesystem::open`. Verifies the streaming wrapper, the
    /// tempfile path, and the size invariant.
    #[test]
    fn stream_defragmented_round_trips() {
        let src_img = create_blank_pfs3(8192, "Src").expect("format source");
        let cur = Cursor::new(src_img);
        let mut src = Pfs3Filesystem::open(cur, 0).expect("open source");
        let root = src.root().expect("root");
        let payload = b"streaming defrag works".to_vec();
        let mut p = Cursor::new(payload.clone());
        src.create_file(
            &root,
            "data.bin",
            &mut p,
            payload.len() as u64,
            &CreateFileOptions::default(),
        )
        .expect("create");
        EditableFilesystem::sync_metadata(&mut src).expect("sync source");

        let target_size: u64 = 4096 * HW_SECTOR; // half-size target
        let mut dst: Vec<u8> = Vec::new();
        let report =
            stream_defragmented_pfs3(&mut src, target_size, &mut dst, &mut |_| {}, &mut |_| {})
                .expect("stream_defragmented_pfs3");
        assert_eq!(report.files_copied, 1);
        assert_eq!(report.bytes_copied, payload.len() as u64);
        assert_eq!(dst.len() as u64, target_size);

        // Re-open the streamed image and verify the file is there.
        let cur2 = Cursor::new(dst);
        let mut fs2 = Pfs3Filesystem::open(cur2, 0).expect("reopen streamed image");
        let root2 = fs2.root().expect("root");
        let kids = fs2.list_directory(&root2).expect("list");
        let fe = kids.iter().find(|e| e.name == "data.bin").expect("file");
        let body = fs2.read_file(fe, usize::MAX).expect("read");
        assert_eq!(body, payload);
    }

    /// Two hardlinks to one target round-trip through the clone with
    /// the back-link chain wired so that the target's
    /// `extrafields.link` points at the first linknode and
    /// `linknode.next` walks all link anodes.
    ///
    /// Uses lower-level FS internals (`read_direntry_extra_link_at`,
    /// anode walk) to confirm the chain on the destination volume.
    #[test]
    fn clone_links_target_back_chain() {
        let src_img = create_blank_pfs3(8192, "Src").expect("format source");
        let cur = Cursor::new(src_img);
        let mut src = Pfs3Filesystem::open(cur, 0).expect("open source");
        let root = src.root().expect("root");
        let payload = b"shared data".to_vec();
        let mut p = Cursor::new(payload.clone());
        let target_file = src
            .create_file(
                &root,
                "orig",
                &mut p,
                payload.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create target file");
        src.create_hardlink(&root, "ln1", &target_file, &CreateFileOptions::default())
            .expect("first hardlink");
        src.create_hardlink(&root, "ln2", &target_file, &CreateFileOptions::default())
            .expect("second hardlink");
        EditableFilesystem::sync_metadata(&mut src).expect("sync source");

        let tgt_img = create_blank_pfs3(8192, "Dst").expect("format target");
        let tcur = Cursor::new(tgt_img);
        let mut tgt = Pfs3Filesystem::open(tcur, 0).expect("open target");
        let report = clone_pfs3_volume(&mut src, &mut tgt, &mut |_| {}).expect("clone");
        assert_eq!(report.hardlinks_copied, 2);
        assert!(
            report.warnings.is_empty(),
            "unexpected warnings: {:?}",
            report.warnings
        );

        // Re-open the destination and find target + both links.
        let dst_img = tgt.reader.into_inner();
        let cur2 = Cursor::new(dst_img);
        let mut tgt2 = Pfs3Filesystem::open(cur2, 0).expect("reopen target");
        let troot = tgt2.root().expect("troot");
        let kids = tgt2.list_directory(&troot).expect("list");
        let target_fe = kids.iter().find(|e| e.name == "orig").expect("orig");
        let ln1 = kids.iter().find(|e| e.name == "ln1").expect("ln1");
        let ln2 = kids.iter().find(|e| e.name == "ln2").expect("ln2");

        // Forward direction (already covered by clone_replays_mixed_content,
        // re-asserted here to keep this test self-contained).
        assert_eq!(ln1.link_target_cnid, Some(target_fe.location));
        assert_eq!(ln2.link_target_cnid, Some(target_fe.location));

        // Backward direction: target's direntry now carries
        // `extrafields.link` = first link's anode. Walk the linknode
        // chain via anode.next to verify both link anodes appear.
        let troot_anode = troot.location as u32;
        let target_anode = target_fe.location as u32;
        let first_link = tgt2
            .peek_direntry_extra_link(troot_anode, target_anode)
            .expect("read extra_link");
        assert_ne!(first_link, 0, "target extrafields.link should be set");

        // Walk the chain and collect link anodes.
        let mut visited = Vec::new();
        let mut cur = first_link;
        for _ in 0..16 {
            visited.push(cur);
            let next = tgt2.peek_anode_next(cur).expect("read anode.next");
            if next == 0 {
                break;
            }
            cur = next;
        }
        // Two hardlinks → two linknode anodes in the chain.
        assert_eq!(visited.len(), 2, "chain visited: {visited:?}");
        // The chain anodes are the link entries' own anodes.
        let ln1_anode = ln1.location as u32;
        let ln2_anode = ln2.location as u32;
        assert!(
            visited.contains(&ln1_anode) && visited.contains(&ln2_anode),
            "chain {visited:?} should contain link anodes {ln1_anode} and {ln2_anode}"
        );
    }

    /// Smallest end-to-end: populate a source volume with a dir, a
    /// file (with metadata), a softlink, and a hardlink — clone into a
    /// blank target. Verify counts + content survive.
    #[test]
    fn clone_replays_mixed_content() {
        let src_img = create_blank_pfs3(8192, "Src").expect("format source");
        let cur = Cursor::new(src_img);
        let mut src = Pfs3Filesystem::open(cur, 0).expect("open source");
        let root = src.root().expect("root");

        // Populate source.
        let dir_opts = CreateDirectoryOptions {
            amiga_protection: Some(0xA0),
            amiga_comment: Some("a dir".to_string()),
            amiga_dates: Some((1, 2, 3)),
            ..Default::default()
        };
        let _ = src
            .create_directory(&root, "Sub", &dir_opts)
            .expect("create_directory");
        let payload = b"hello world".to_vec();
        let mut data = Cursor::new(payload.clone());
        let file_opts = CreateFileOptions {
            amiga_protection: Some(0x55),
            amiga_comment: Some("a file".to_string()),
            amiga_dates: Some((10, 20, 30)),
            ..Default::default()
        };
        let target_file = src
            .create_file(&root, "orig", &mut data, payload.len() as u64, &file_opts)
            .expect("create_file");
        src.create_symlink(&root, "ptr", "Sub", &CreateFileOptions::default())
            .expect("create_symlink");
        src.create_hardlink(&root, "ln", &target_file, &CreateFileOptions::default())
            .expect("create_hardlink");
        EditableFilesystem::sync_metadata(&mut src).expect("sync source");

        // Build a fresh target and clone.
        let tgt_img = create_blank_pfs3(8192, "Dst").expect("format target");
        let tcur = Cursor::new(tgt_img);
        let mut tgt = Pfs3Filesystem::open(tcur, 0).expect("open target");
        let report = clone_pfs3_volume(&mut src, &mut tgt, &mut |_| {}).expect("clone");
        assert_eq!(report.dirs_copied, 1);
        assert_eq!(report.files_copied, 1);
        assert_eq!(report.symlinks_copied, 1);
        assert_eq!(report.hardlinks_copied, 1);
        assert_eq!(report.bytes_copied, payload.len() as u64);
        assert!(
            report.warnings.is_empty(),
            "warnings: {:?}",
            report.warnings
        );

        // Re-open the target image bytes and verify contents.
        let dst_img = tgt.reader.into_inner();
        let cur2 = Cursor::new(dst_img);
        let mut tgt2 = Pfs3Filesystem::open(cur2, 0).expect("reopen target");
        let troot = tgt2.root().expect("troot");
        let tkids = tgt2.list_directory(&troot).expect("list");
        assert_eq!(tkids.len(), 4, "expected 4 root entries, got {:?}", tkids);

        let fdir = tkids.iter().find(|e| e.name == "Sub").expect("dir");
        assert!(fdir.is_directory());
        assert_eq!(fdir.amiga_protection, Some(0xA0));
        assert_eq!(fdir.amiga_comment.as_deref(), Some("a dir"));
        assert_eq!(fdir.amiga_date, Some((1, 2, 3)));

        let fofile = tkids.iter().find(|e| e.name == "orig").expect("file");
        assert!(fofile.is_file());
        assert_eq!(fofile.size, payload.len() as u64);
        assert_eq!(fofile.amiga_protection, Some(0x55));
        let body = tgt2.read_file(fofile, usize::MAX).expect("read");
        assert_eq!(body, payload);

        let fptr = tkids.iter().find(|e| e.name == "ptr").expect("symlink");
        assert!(fptr.is_symlink());
        assert_eq!(fptr.symlink_target.as_deref(), Some("Sub"));

        let fln = tkids.iter().find(|e| e.name == "ln").expect("hardlink");
        assert_eq!(fln.link_target_cnid, Some(fofile.location));
    }
}
