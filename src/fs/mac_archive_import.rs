//! Unpack a classic-Mac archive straight into an open filesystem — the Mac
//! counterpart of [`crate::fs::tar_import`], and what `--expand-archives` means
//! for a `.sit` / `.cpt` / `.hqx` / `.sea` / `.mar` instead of a `.tardist`.
//!
//! Everything is already in the tree: [`crate::macarchive::extract`] decodes the
//! archive, [`crate::fs::archive_fs::ArchiveFilesystem`] presents it as a
//! read-only [`Filesystem`], and [`crate::fs::copy`] copies one filesystem's
//! tree into another. This module is the ~three-call join between them, so an
//! expanded StuffIt lands on the target with **both forks and its Finder
//! type/creator intact** — which is the whole point on a Mac volume, and what
//! copying the `.sit` in verbatim would leave the target machine to do itself.
//!
//! Detection is deliberately two-stage, matching the convention in
//! [`crate::model::file_types`]: an extension gate ([`MAC_ARCHIVE_EXTS`]) so a
//! tree walk never reads a multi-GB disk image just to classify it, then a
//! content sniff that has the final say. `.bin` and `.zip` stay out of the gate
//! for the same reason they stay out of the routing list — they are
//! overwhelmingly something else.

use std::path::Path;

use anyhow::{anyhow, Context, Result};

use super::archive_fs::ArchiveFilesystem;
use super::copy::{copy_into, ConflictMode, CopyOptions, CopyStats};
use super::entry::{EntryType, FileEntry};
use super::filesystem::{EditableFilesystem, Filesystem};
use super::import_sink::{ImportConflict, ImportOptions, ImportStats};
use crate::macarchive::detect::detect_mac_archive;
use crate::model::file_types::MAC_ARCHIVE_EXTS;

/// Does `path` name a Mac archive this module can expand?
///
/// Extension gate first (`.sit`, `.hqx`, `.sea`, `.cpt`, `.mar`), then a
/// content sniff on the bytes. The gate is what keeps a tree walk cheap; the
/// sniff is what keeps a mis-named file from being mangled.
pub fn looks_like_mac_archive(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
        return false;
    };
    if !MAC_ARCHIVE_EXTS.iter().any(|e| e.eq_ignore_ascii_case(ext)) {
        return false;
    }
    match std::fs::read(path) {
        Ok(bytes) => detect_mac_archive(&bytes).is_some(),
        Err(_) => false,
    }
}

/// What the archive expands to: `(files, dirs, bytes)`, with **both forks**
/// counted in `bytes` — a classic Mac app can carry all of its code in the
/// resource fork, so ignoring it under-sizes the volume by the whole payload.
///
/// The counterpart of [`crate::fs::tar_import::measure_tar_expanded`], used by
/// `--size auto`.
pub fn measure_mac_archive_expanded(path: &Path) -> Result<(u64, u64, u64)> {
    let mut fs = ArchiveFilesystem::open_path(path)
        .map_err(|e| anyhow!("opening {}: {e}", path.display()))?;
    let root = fs
        .root()
        .map_err(|e| anyhow!("reading {}: {e}", path.display()))?;
    let mut totals = (0u64, 0u64, 0u64);
    measure_dir_into(&mut fs, &root, &mut totals)?;
    Ok(totals)
}

fn measure_dir_into(
    fs: &mut ArchiveFilesystem,
    dir: &FileEntry,
    totals: &mut (u64, u64, u64),
) -> Result<()> {
    for child in fs
        .list_directory(dir)
        .map_err(|e| anyhow!("listing {}: {e}", dir.path))?
    {
        match child.entry_type {
            EntryType::Directory => {
                totals.1 += 1;
                measure_dir_into(fs, &child, totals)?;
            }
            EntryType::File => {
                totals.0 += 1;
                totals.2 = totals
                    .2
                    .saturating_add(child.size)
                    .saturating_add(child.resource_fork_size.unwrap_or(0));
            }
            EntryType::Symlink | EntryType::Special => {}
        }
    }
    Ok(())
}

/// Unpack `path`'s contents into `dest` on an open filesystem. Returns the
/// import tally so a caller mid-walk can fold it into its own totals.
///
/// The counterpart of [`crate::fs::tar_import::import_tar_from_path_into`].
/// `progress` fires once per top-level archive member; Mac archives are small
/// enough (tens of MB at the outside) that finer granularity buys nothing.
pub fn import_mac_archive_from_path_into(
    efs: &mut dyn EditableFilesystem,
    dest: &FileEntry,
    path: &Path,
    opts: &ImportOptions,
    progress: &dyn Fn(&ImportStats),
) -> Result<ImportStats> {
    let mut src = ArchiveFilesystem::open_path(path)
        .map_err(|e| anyhow!("opening {}: {e}", path.display()))?;
    let root = src
        .root()
        .map_err(|e| anyhow!("reading {}: {e}", path.display()))?;
    let children = src
        .list_directory(&root)
        .map_err(|e| anyhow!("listing {}: {e}", path.display()))?;

    let copy_opts = CopyOptions {
        recursive: true,
        conflict: match opts.conflict {
            ImportConflict::Overwrite => ConflictMode::Force,
            ImportConflict::Skip => ConflictMode::SkipExisting,
            ImportConflict::Error => ConflictMode::Error,
        },
        // A flat destination (MFS, CP/M, …) can still receive an archive's
        // files; collapsing beats erroring out mid-import.
        flatten: true,
        ..CopyOptions::default()
    };

    let mut stats = ImportStats::default();
    let mut copied = CopyStats::default();
    for child in &children {
        copy_into(
            &mut src,
            child,
            efs,
            dest,
            &child.name,
            &copy_opts,
            &mut copied,
            &|msg: &str| log::debug!("expand {}: {msg}", path.display()),
        )
        .with_context(|| format!("copying {} out of {}", child.name, path.display()))?;
        fold(&copied, &mut stats);
        progress(&stats);
    }
    fold(&copied, &mut stats);
    Ok(stats)
}

/// Project a copy tally onto the import tally the walk reports. Assignment,
/// not accumulation: `copied` is itself a running total across the archive's
/// members.
fn fold(copied: &CopyStats, stats: &mut ImportStats) {
    stats.files = copied.files;
    stats.dirs_created = copied.dirs;
    stats.symlinks = copied.symlinks;
    stats.total_bytes = copied.bytes;
    stats.skipped_existing = copied.skipped_existing;
    stats.invalid_names_skipped = copied.skipped_names;
    stats.other_skipped = copied.skipped_special;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macarchive::stuffit::{
        build_archive_tree, StuffItInput, StuffItInputNode, WriteMethod,
    };

    fn file_node(name: &str, data: &[u8], rsrc: &[u8]) -> StuffItInputNode {
        StuffItInputNode::File(StuffItInput {
            name: name.into(),
            type_code: *b"APPL",
            creator_code: *b"RBAK",
            finder_flags: 0,
            create_date: 0,
            mod_date: 0,
            data_fork: data.to_vec(),
            resource_fork: rsrc.to_vec(),
        })
    }

    /// A `.sit` holding a root file plus a folder with a forked "application".
    fn sample_sit(dir: &Path) -> std::path::PathBuf {
        let tree = vec![
            file_node("readme.txt", b"hello", b""),
            StuffItInputNode::Folder {
                name: "App".into(),
                finder_flags: 0,
                create_date: 0,
                mod_date: 0,
                children: vec![file_node("Tool", b"DATA", b"RESOURCEFORKBYTES")],
            },
        ];
        let bytes = build_archive_tree(&tree, WriteMethod::Store).expect("build archive");
        let path = dir.join("sample.sit");
        std::fs::write(&path, bytes).expect("write archive");
        path
    }

    /// A `.mar` wrapping the same two files in a root folder, which is the
    /// shape `rb-cli archive create` emits for a multi-file MAR.
    fn sample_mar(dir: &Path) -> std::path::PathBuf {
        let tree = vec![
            file_node("readme.txt", b"hello", b""),
            file_node("Tool", b"DATA", b"RESOURCEFORKBYTES"),
        ];
        let bytes = crate::macarchive::mar::build_archive("Bundle", &tree).expect("build mar");
        let path = dir.join("Bundle.mar");
        std::fs::write(&path, bytes).expect("write archive");
        path
    }

    /// Open a blank HFS volume in an image file, for expansion targets.
    fn blank_hfs(dir: &Path) -> (std::path::PathBuf, Box<dyn EditableFilesystem>) {
        let img = dir.join("vol.dsk");
        std::fs::write(
            &img,
            crate::fs::hfs::create_blank_hfs(16 * 1024 * 1024, 512, "EXPAND").expect("format"),
        )
        .expect("write image");
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&img)
            .expect("open image");
        let efs = crate::fs::open_editable_filesystem(f, 0, 0, None).expect("open hfs");
        (img, efs)
    }

    /// The gate is extension-then-content: a non-archive `.sit` fails the
    /// sniff, and an extension outside the routing list is never read at all.
    #[test]
    fn gate_is_extension_then_content() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real = sample_sit(dir.path());
        assert!(looks_like_mac_archive(&real));

        let fake = dir.path().join("not-really.sit");
        std::fs::write(&fake, b"this is not a StuffIt archive at all").unwrap();
        assert!(
            !looks_like_mac_archive(&fake),
            "content sniff has final say"
        );

        // `.bin` is a disk image by default and stays out of the gate, as does
        // an extension-less file.
        let as_bin = dir.path().join("sample.bin");
        std::fs::copy(&real, &as_bin).unwrap();
        assert!(!looks_like_mac_archive(&as_bin));
        let no_ext = dir.path().join("payload");
        std::fs::write(&no_ext, b"whatever").unwrap();
        assert!(!looks_like_mac_archive(&no_ext));
    }

    /// Expanding into HFS reproduces the tree, and — the whole point — carries
    /// the resource fork and Finder type/creator that copying the `.sit` in
    /// verbatim would have left packed.
    #[test]
    fn expands_into_hfs_with_forks_and_type_creator() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sit = sample_sit(dir.path());
        let (_img, mut efs) = blank_hfs(dir.path());
        let root = efs.root().expect("root");

        let stats = import_mac_archive_from_path_into(
            &mut *efs,
            &root,
            &sit,
            &ImportOptions::default(),
            &|_| {},
        )
        .expect("expand");
        efs.sync_metadata().expect("sync");

        assert_eq!(stats.files, 2);
        assert_eq!(stats.dirs_created, 1);

        let fs = efs.as_filesystem_mut();
        let mut names: Vec<String> = fs
            .list_directory(&root)
            .expect("list")
            .iter()
            .map(|e| e.name.clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["App".to_string(), "readme.txt".to_string()]);

        let app = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "App")
            .expect("App folder");
        let tool = fs
            .list_directory(&app)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "Tool")
            .expect("Tool file");
        assert_eq!(tool.size, 4, "data fork");
        assert_eq!(
            tool.resource_fork_size,
            Some(b"RESOURCEFORKBYTES".len() as u64),
            "resource fork crossed into HFS"
        );
        assert_eq!(tool.type_code, Some(*b"APPL"));
        assert_eq!(tool.creator_code, Some(*b"RBAK"));
    }

    /// MAR is in the same family and rides the same gate and expander — worth
    /// pinning because it is rusty-backup's own archive format, so a `.mar`
    /// round-tripping through `archive create` and back onto a volume is a
    /// path users will actually take. `archive create` wraps a multi-file MAR
    /// in a root folder, which survives the copy as a folder on the target.
    #[test]
    fn expands_a_mar_archive() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mar = sample_mar(dir.path());
        assert!(looks_like_mac_archive(&mar), "MAR passes the gate");
        assert_eq!(
            measure_mac_archive_expanded(&mar).expect("measure"),
            (2, 1, 5 + 4 + 17),
            "two files inside one wrapper folder, both forks counted"
        );

        let (_img, mut efs) = blank_hfs(dir.path());
        let root = efs.root().expect("root");
        let stats = import_mac_archive_from_path_into(
            &mut *efs,
            &root,
            &mar,
            &ImportOptions::default(),
            &|_| {},
        )
        .expect("expand");
        efs.sync_metadata().expect("sync");
        assert_eq!((stats.files, stats.dirs_created), (2, 1));

        let fs = efs.as_filesystem_mut();
        let bundle = fs
            .list_directory(&root)
            .expect("list")
            .into_iter()
            .find(|e| e.name == "Bundle")
            .expect("MAR's wrapper folder");
        let tool = fs
            .list_directory(&bundle)
            .expect("list")
            .into_iter()
            .find(|e| e.name == "Tool")
            .expect("Tool file");
        assert_eq!(tool.resource_fork_size, Some(17), "fork crossed from MAR");
    }

    /// `--size auto` keys off the measurement, so it has to count both forks
    /// and match what the expansion actually writes.
    #[test]
    fn measurement_covers_both_forks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sit = sample_sit(dir.path());
        let (files, dirs, bytes) = measure_mac_archive_expanded(&sit).expect("measure");
        assert_eq!((files, dirs), (2, 1));
        // readme (5) + Tool data (4) + Tool resource fork (17).
        assert_eq!(bytes, 5 + 4 + 17);
    }
}
