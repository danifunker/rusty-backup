//! Edit/copy engine for Commander Mode panes.
//!
//! An image pane stages [`StagedEdit`]s into an
//! [`EditQueue`](crate::model::edit_queue::EditQueue) (delete + copy-in);
//! nothing touches disk until Apply. [`apply_edits`] is the leaf that opens the
//! pane's source read-write, replays the queue, syncs, and commits the container
//! — the same shape as the browse view's `apply_staged_edits`; [`spawn_apply`]
//! threads it behind an [`ApplyStatus`] so a slow write never blocks the frame.
//!
//! The cross-pane copy has four backends by source/destination kind:
//! [`stage_copy`] (image->image, extract to temp + stage) and
//! [`stage_host_to_image`] (host->image, stage real host paths) feed the
//! destination queue; [`spawn_host_copy`] performs the immediate image->host and
//! host->host writes on a worker thread.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{Context, Result};

use crate::fs::entry::{EntryType, FileEntry};
use crate::fs::filesystem::Filesystem;
use crate::fs::hfs_common::encode_fourcc;
use crate::fs::resource_fork::ImportedResourceFork;
use crate::model::browse_session::BrowseSession;
use crate::model::edit_queue::{apply_edit, StagedEdit};

/// Apply `edits` to the source described by `session`, in order.
///
/// Opens the filesystem read-write via [`BrowseSession::open_editable`],
/// replays each edit through [`apply_edit`], calls `sync_metadata`, then commits
/// the container (a no-op for raw images / devices, a re-encode for floppy
/// containers). Errors abort the batch — the queue is left intact by the caller
/// so the user can retry.
pub fn apply_edits(session: &BrowseSession, edits: &[StagedEdit]) -> Result<()> {
    let (mut efs, commit) = session
        .open_editable()
        .context("opening source for editing")?;
    for edit in edits {
        apply_edit(efs.as_mut(), edit).context("applying a staged edit")?;
    }
    efs.sync_metadata().context("writing filesystem metadata")?;
    drop(efs);
    commit.commit().context("committing container edits")?;
    Ok(())
}

/// Stage a cross-volume (image -> image) copy of `entries` from `src_fs` into
/// the directory `dest_parent` on another volume, returning the [`StagedEdit`]s
/// to push onto the destination pane's queue.
///
/// Files are extracted (data fork, plus resource fork and HFS type/creator when
/// present) to uniquely-named blobs under `temp_dir`; the staged `AddFile`
/// references the blob by path, so `temp_dir` must outlive the destination queue
/// (Commander keeps it alive until Apply/close). Directories are recreated and
/// their contents recursed. Symlinks and special files are skipped.
///
/// The destination filesystem silently drops metadata it can't hold (e.g. a
/// resource fork copied onto FAT) at apply time — `create_file` ignores
/// unsupported options.
/// When `keep_dates` is set, each copied file carries its source timestamps
/// (HFS catalog dates / Amiga datestamp) so the destination reproduces them
/// instead of stamping the current time.
pub fn stage_copy(
    src_fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    dest_parent: &FileEntry,
    temp_dir: &Path,
    keep_dates: bool,
) -> Result<Vec<StagedEdit>> {
    let mut edits = Vec::new();
    stage_copy_into(
        src_fs,
        entries,
        dest_parent,
        temp_dir,
        keep_dates,
        &mut edits,
    )?;
    Ok(edits)
}

fn stage_copy_into(
    src_fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    dest_parent: &FileEntry,
    temp_dir: &Path,
    keep_dates: bool,
    edits: &mut Vec<StagedEdit>,
) -> Result<()> {
    for entry in entries {
        match entry.entry_type {
            EntryType::Directory => {
                edits.push(StagedEdit::CreateDirectory {
                    parent: dest_parent.clone(),
                    name: entry.name.clone(),
                });
                let child_parent = FileEntry::new_directory(
                    entry.name.clone(),
                    join_path(&dest_parent.path, &entry.name),
                    0,
                );
                let children = src_fs
                    .list_directory(entry)
                    .with_context(|| format!("listing '{}' to copy", entry.name))?;
                stage_copy_into(
                    src_fs,
                    &children,
                    &child_parent,
                    temp_dir,
                    keep_dates,
                    edits,
                )?;
            }
            EntryType::File => {
                let (mut blob, blob_path) = tempfile::Builder::new()
                    .prefix("blob")
                    .tempfile_in(temp_dir)
                    .context("creating temp file for copy")?
                    .keep()
                    .context("persisting temp file for copy")?;
                let size = src_fs
                    .write_file_to(entry, &mut blob)
                    .with_context(|| format!("extracting '{}'", entry.name))?;
                drop(blob);

                let mut rsrc = Vec::new();
                let _ = src_fs.write_resource_fork_to(entry, &mut rsrc);
                let resource_fork = if rsrc.is_empty() {
                    None
                } else {
                    Some(ImportedResourceFork {
                        data: rsrc,
                        data_fork: None,
                        type_code: None,
                        creator_code: None,
                    })
                };

                edits.push(StagedEdit::AddFile {
                    parent: dest_parent.clone(),
                    name: entry.name.clone(),
                    host_path: blob_path,
                    size,
                    prodos_type: None,
                    prodos_aux: entry.aux_type,
                    resource_fork,
                    hfs_type_override: entry.type_code.as_deref().map(encode_fourcc),
                    hfs_creator_override: entry.creator_code.as_deref().map(encode_fourcc),
                    dates: if keep_dates {
                        crate::model::edit_queue::PreservedDates::from_entry(entry)
                    } else {
                        None
                    },
                });
            }
            EntryType::Symlink | EntryType::Special => {
                // Not representable as a plain cross-volume copy; skip.
            }
        }
    }
    Ok(())
}

/// Join a parent directory path and a child name, anchored at root.
fn join_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

/// Stage a **host -> image** copy of `entries` into `dest_parent` on the image
/// pane's queue. Unlike the image->image path this needs no temp extraction:
/// each `AddFile` references the real host file by path; directories recurse
/// over the host tree. Symlinks / special files are skipped.
pub fn stage_host_to_image(entries: &[FileEntry], dest_parent: &FileEntry) -> Vec<StagedEdit> {
    let mut edits = Vec::new();
    stage_host_into(entries, dest_parent, &mut edits);
    edits
}

fn stage_host_into(entries: &[FileEntry], dest_parent: &FileEntry, edits: &mut Vec<StagedEdit>) {
    for entry in entries {
        if entry.is_directory() {
            edits.push(StagedEdit::CreateDirectory {
                parent: dest_parent.clone(),
                name: entry.name.clone(),
            });
            let child_parent = FileEntry::new_directory(
                entry.name.clone(),
                join_path(&dest_parent.path, &entry.name),
                0,
            );
            let children = host_children(&entry.path);
            stage_host_into(&children, &child_parent, edits);
        } else if entry.is_file() {
            edits.push(StagedEdit::AddFile {
                parent: dest_parent.clone(),
                name: entry.name.clone(),
                host_path: PathBuf::from(&entry.path),
                size: entry.size,
                prodos_type: None,
                prodos_aux: None,
                resource_fork: None,
                hfs_type_override: None,
                hfs_creator_override: None,
                // Host files carry no Amiga/HFS raw dates to preserve.
                dates: None,
            });
        }
    }
}

/// Minimal host directory listing (name / path / dir-or-file / size) for the
/// copy walkers. Symlinks and special files are dropped.
fn host_children(path: &str) -> Vec<FileEntry> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(path) else {
        return out;
    };
    for dent in rd.flatten() {
        let name = dent.file_name().to_string_lossy().to_string();
        let full = dent.path().to_string_lossy().to_string();
        let Ok(meta) = dent.path().symlink_metadata() else {
            continue;
        };
        let ft = meta.file_type();
        if ft.is_dir() {
            out.push(FileEntry::new_directory(name, full, 0));
        } else if ft.is_file() {
            out.push(FileEntry::new_file(name, full, meta.len(), 0));
        }
    }
    out
}

/// An immediate (unstaged) host-write copy, run on a worker thread.
pub enum HostCopyJob {
    /// Extract image-volume `entries` into the host directory `dest_dir`.
    ImageToHost {
        session: BrowseSession,
        entries: Vec<FileEntry>,
        dest_dir: PathBuf,
    },
    /// Copy host `entries` into the host directory `dest_dir`.
    HostToHost {
        entries: Vec<FileEntry>,
        dest_dir: PathBuf,
    },
}

/// Shared state between the GUI and the [`spawn_host_copy`] worker.
#[derive(Default)]
pub struct HostCopyStatus {
    pub finished: bool,
    pub error: Option<String>,
    /// Number of files written (directories aren't counted).
    pub copied: usize,
}

/// Run a [`HostCopyJob`] on a worker thread (host writes are immediate, not
/// staged). The returned status flips `finished` when done.
pub fn spawn_host_copy(job: HostCopyJob) -> Arc<Mutex<HostCopyStatus>> {
    let status = Arc::new(Mutex::new(HostCopyStatus::default()));
    let status_thread = Arc::clone(&status);
    thread::spawn(move || {
        let result = run_host_copy(job);
        if let Ok(mut g) = status_thread.lock() {
            match result {
                Ok(n) => g.copied = n,
                Err(e) => g.error = Some(format!("{e:#}")),
            }
            g.finished = true;
        }
    });
    status
}

fn run_host_copy(job: HostCopyJob) -> Result<usize> {
    match job {
        HostCopyJob::ImageToHost {
            session,
            entries,
            dest_dir,
        } => {
            let mut fs = session.open().context("opening source image")?;
            copy_image_entries_to_host(fs.as_mut(), &entries, &dest_dir)
        }
        HostCopyJob::HostToHost { entries, dest_dir } => {
            copy_host_entries_to_host(&entries, &dest_dir)
        }
    }
}

/// Extract image `entries` (data fork) into `dest_dir`, recursing directories.
fn copy_image_entries_to_host(
    fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    dest_dir: &Path,
) -> Result<usize> {
    let mut count = 0;
    for entry in entries {
        if entry.is_directory() {
            let sub = dest_dir.join(&entry.name);
            std::fs::create_dir_all(&sub).with_context(|| format!("creating {}", sub.display()))?;
            let children = fs
                .list_directory(entry)
                .with_context(|| format!("listing '{}'", entry.name))?;
            count += copy_image_entries_to_host(fs, &children, &sub)?;
        } else if entry.is_file() {
            let out = dest_dir.join(&entry.name);
            let mut f = std::fs::File::create(&out)
                .with_context(|| format!("creating {}", out.display()))?;
            fs.write_file_to(entry, &mut f)
                .with_context(|| format!("extracting '{}'", entry.name))?;
            count += 1;
        }
    }
    Ok(count)
}

/// Copy host `entries` into `dest_dir`, recursing directories.
fn copy_host_entries_to_host(entries: &[FileEntry], dest_dir: &Path) -> Result<usize> {
    let mut count = 0;
    for entry in entries {
        let src = PathBuf::from(&entry.path);
        let dst = dest_dir.join(&entry.name);
        if entry.is_directory() {
            count += copy_host_dir(&src, &dst)?;
        } else if entry.is_file() {
            std::fs::copy(&src, &dst)
                .with_context(|| format!("copying {} -> {}", src.display(), dst.display()))?;
            count += 1;
        }
    }
    Ok(count)
}

fn copy_host_dir(src: &Path, dst: &Path) -> Result<usize> {
    std::fs::create_dir_all(dst).with_context(|| format!("creating {}", dst.display()))?;
    let mut count = 0;
    for dent in std::fs::read_dir(src)
        .with_context(|| format!("reading {}", src.display()))?
        .flatten()
    {
        let meta = match dent.path().symlink_metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        let ft = meta.file_type();
        let child_dst = dst.join(dent.file_name());
        if ft.is_dir() {
            count += copy_host_dir(&dent.path(), &child_dst)?;
        } else if ft.is_file() {
            std::fs::copy(dent.path(), &child_dst)
                .with_context(|| format!("copying {}", dent.path().display()))?;
            count += 1;
        }
    }
    Ok(count)
}

/// Shared state between the GUI and the [`spawn_apply`] worker.
#[derive(Default)]
pub struct ApplyStatus {
    pub finished: bool,
    /// Set when the apply failed; the pane surfaces it and keeps the queue.
    pub error: Option<String>,
}

/// Run [`apply_edits`] on a worker thread. The returned status flips `finished`
/// when done, with `error` set on failure.
pub fn spawn_apply(session: BrowseSession, edits: Vec<StagedEdit>) -> Arc<Mutex<ApplyStatus>> {
    let status = Arc::new(Mutex::new(ApplyStatus::default()));
    let status_thread = Arc::clone(&status);
    thread::spawn(move || {
        let result = apply_edits(&session, &edits);
        if let Ok(mut g) = status_thread.lock() {
            if let Err(e) = result {
                g.error = Some(format!("{e:#}"));
            }
            g.finished = true;
        }
    });
    status
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::entry::FileEntry;

    /// Build a blank FAT12 floppy on disk with one file, then apply a staged
    /// delete-plus-add batch through `apply_edits` and confirm the result lands.
    #[test]
    fn apply_edits_deletes_and_adds_on_a_real_image() {
        use crate::fs::filesystem::CreateFileOptions;
        use std::fs::OpenOptions;
        use std::io::Cursor;

        // Seed image: KEEP.TXT + GONE.TXT at the root.
        let flat = crate::fs::fat::create_blank_fat(737280, Some("OPS")).unwrap();
        let tmp = tempfile::Builder::new().suffix(".img").tempfile().unwrap();
        std::fs::write(tmp.path(), &flat).unwrap();
        {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0x01, None).unwrap();
            let root = efs.root().unwrap();
            for name in ["KEEP.TXT", "GONE.TXT"] {
                let mut data = Cursor::new(b"x".to_vec());
                efs.create_file(&root, name, &mut data, 1, &CreateFileOptions::default())
                    .unwrap();
            }
            efs.sync_metadata().unwrap();
        }

        // A host file to import.
        let host = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(host.path(), b"new contents").unwrap();

        let session = BrowseSession {
            source_path: Some(tmp.path().to_path_buf()),
            partition_type: 0x01,
            ..Default::default()
        };

        // Resolve the live entries to delete.
        let mut fs = session.open().unwrap();
        let root = fs.root().unwrap();
        let gone = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "GONE.TXT")
            .unwrap();
        drop(fs);

        let edits = vec![
            StagedEdit::DeleteEntry {
                parent: root.clone(),
                entry: gone,
            },
            StagedEdit::AddFile {
                parent: FileEntry::root(),
                name: "ADDED.TXT".to_string(),
                host_path: host.path().to_path_buf(),
                size: 12,
                prodos_type: None,
                prodos_aux: None,
                resource_fork: None,
                hfs_type_override: None,
                hfs_creator_override: None,
                dates: None,
            },
        ];
        apply_edits(&session, &edits).expect("apply");

        // Re-open: GONE.TXT removed, ADDED.TXT present, KEEP.TXT untouched.
        let mut fs = session.open().unwrap();
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.iter().any(|n| n == "KEEP.TXT"), "{names:?}");
        assert!(names.iter().any(|n| n == "ADDED.TXT"), "{names:?}");
        assert!(!names.iter().any(|n| n == "GONE.TXT"), "{names:?}");
    }

    /// A staged `Rename` applied through `apply_edits` renames the entry in
    /// place on a real FAT image (the M6.3 staging path).
    #[test]
    fn apply_edits_renames_in_place_on_a_real_image() {
        use crate::fs::filesystem::CreateFileOptions;
        use std::fs::OpenOptions;
        use std::io::Cursor;

        let flat = crate::fs::fat::create_blank_fat(737280, Some("REN")).unwrap();
        let tmp = tempfile::Builder::new().suffix(".img").tempfile().unwrap();
        std::fs::write(tmp.path(), &flat).unwrap();
        {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(tmp.path())
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0x01, None).unwrap();
            let root = efs.root().unwrap();
            let mut data = Cursor::new(b"keep my bytes".to_vec());
            efs.create_file(
                &root,
                "BEFORE.TXT",
                &mut data,
                13,
                &CreateFileOptions::default(),
            )
            .unwrap();
            efs.sync_metadata().unwrap();
        }

        let session = BrowseSession {
            source_path: Some(tmp.path().to_path_buf()),
            partition_type: 0x01,
            ..Default::default()
        };

        let mut fs = session.open().unwrap();
        let root = fs.root().unwrap();
        let before = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "BEFORE.TXT")
            .unwrap();
        drop(fs);

        let edits = vec![StagedEdit::Rename {
            parent: FileEntry::root(),
            entry: before,
            new_name: "After File.txt".to_string(),
        }];
        apply_edits(&session, &edits).expect("apply rename");

        let mut fs = session.open().unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();
        let renamed = entries
            .iter()
            .find(|e| e.name == "After File.txt")
            .expect("renamed entry present");
        assert!(!entries.iter().any(|e| e.name == "BEFORE.TXT"));
        assert_eq!(
            &fs.read_file(renamed, usize::MAX).unwrap(),
            b"keep my bytes"
        );
    }

    /// Image -> image copy: stage a file and a directory subtree from one FAT
    /// volume, apply onto a blank one, and confirm the whole subtree lands.
    #[test]
    fn stage_copy_recreates_a_subtree_on_another_volume() {
        use crate::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions};
        use std::fs::OpenOptions;
        use std::io::Cursor;

        // Source: /TOP.TXT and /SUB/INNER.TXT.
        let src_flat = crate::fs::fat::create_blank_fat(737280, Some("SRC")).unwrap();
        let src_img = tempfile::Builder::new().suffix(".img").tempfile().unwrap();
        std::fs::write(src_img.path(), &src_flat).unwrap();
        {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(src_img.path())
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0x01, None).unwrap();
            let root = efs.root().unwrap();
            let mut top = Cursor::new(b"top".to_vec());
            efs.create_file(&root, "TOP.TXT", &mut top, 3, &CreateFileOptions::default())
                .unwrap();
            efs.create_directory(&root, "SUB", &CreateDirectoryOptions::default())
                .unwrap();
            let sub = efs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "SUB")
                .unwrap();
            let mut inner = Cursor::new(b"inner!".to_vec());
            efs.create_file(
                &sub,
                "INNER.TXT",
                &mut inner,
                6,
                &CreateFileOptions::default(),
            )
            .unwrap();
            efs.sync_metadata().unwrap();
        }

        // Destination: a blank FAT volume.
        let dst_flat = crate::fs::fat::create_blank_fat(737280, Some("DST")).unwrap();
        let dst_img = tempfile::Builder::new().suffix(".img").tempfile().unwrap();
        std::fs::write(dst_img.path(), &dst_flat).unwrap();
        let dst_session = BrowseSession {
            source_path: Some(dst_img.path().to_path_buf()),
            partition_type: 0x01,
            ..Default::default()
        };

        // Stage a copy of [TOP.TXT, SUB] into the destination root.
        let src_session = BrowseSession {
            source_path: Some(src_img.path().to_path_buf()),
            partition_type: 0x01,
            ..Default::default()
        };
        let mut src_fs = src_session.open().unwrap();
        let src_root = src_fs.root().unwrap();
        let to_copy: Vec<FileEntry> = src_fs.list_directory(&src_root).unwrap();
        assert_eq!(to_copy.len(), 2);

        let temp = tempfile::tempdir().unwrap();
        let edits = stage_copy(
            src_fs.as_mut(),
            &to_copy,
            &FileEntry::root(),
            temp.path(),
            false,
        )
        .expect("stage");
        drop(src_fs);

        apply_edits(&dst_session, &edits).expect("apply copy");

        // Destination now mirrors the source tree.
        let mut fs = dst_session.open().unwrap();
        let root = fs.root().unwrap();
        let names: Vec<String> = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.iter().any(|n| n == "TOP.TXT"), "{names:?}");
        let sub = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "SUB")
            .expect("SUB copied");
        let inner: Vec<String> = fs
            .list_directory(&sub)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(inner.iter().any(|n| n == "INNER.TXT"), "{inner:?}");
    }

    /// host -> image: stage real host files (a file + a subtree) onto an image
    /// queue and apply; the tree must land on the volume.
    #[test]
    fn stage_host_to_image_recreates_a_subtree() {
        // Host tree: base/TOP.TXT and base/SUB/INNER.TXT.
        let base = tempfile::tempdir().unwrap();
        std::fs::write(base.path().join("TOP.TXT"), b"top").unwrap();
        std::fs::create_dir(base.path().join("SUB")).unwrap();
        std::fs::write(base.path().join("SUB").join("INNER.TXT"), b"inner!").unwrap();

        let host_entries = host_children(&base.path().to_string_lossy());
        assert_eq!(host_entries.len(), 2);

        let edits = stage_host_to_image(&host_entries, &FileEntry::root());

        // Apply onto a blank FAT image.
        let flat = crate::fs::fat::create_blank_fat(737280, Some("DST")).unwrap();
        let dst = tempfile::Builder::new().suffix(".img").tempfile().unwrap();
        std::fs::write(dst.path(), &flat).unwrap();
        let session = BrowseSession {
            source_path: Some(dst.path().to_path_buf()),
            partition_type: 0x01,
            ..Default::default()
        };
        apply_edits(&session, &edits).expect("apply host->image");

        let mut fs = session.open().unwrap();
        let root = fs.root().unwrap();
        let kids = fs.list_directory(&root).unwrap();
        assert!(kids.iter().any(|e| e.name == "TOP.TXT"), "{kids:?}");
        let sub = kids.into_iter().find(|e| e.name == "SUB").expect("SUB");
        assert!(fs
            .list_directory(&sub)
            .unwrap()
            .iter()
            .any(|e| e.name == "INNER.TXT"));
    }

    /// image -> host: extract a file + subtree from a FAT volume to a host dir.
    #[test]
    fn copy_image_to_host_extracts_tree() {
        use crate::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions};
        use std::fs::OpenOptions;
        use std::io::Cursor;

        let flat = crate::fs::fat::create_blank_fat(737280, Some("SRC")).unwrap();
        let img = tempfile::Builder::new().suffix(".img").tempfile().unwrap();
        std::fs::write(img.path(), &flat).unwrap();
        {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(img.path())
                .unwrap();
            let mut efs = crate::fs::open_editable_filesystem(f, 0, 0x01, None).unwrap();
            let root = efs.root().unwrap();
            let mut top = Cursor::new(b"hello".to_vec());
            efs.create_file(&root, "A.TXT", &mut top, 5, &CreateFileOptions::default())
                .unwrap();
            efs.create_directory(&root, "D", &CreateDirectoryOptions::default())
                .unwrap();
            let d = efs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .find(|e| e.name == "D")
                .unwrap();
            let mut inner = Cursor::new(b"deep".to_vec());
            efs.create_file(&d, "B.TXT", &mut inner, 4, &CreateFileOptions::default())
                .unwrap();
            efs.sync_metadata().unwrap();
        }

        let session = BrowseSession {
            source_path: Some(img.path().to_path_buf()),
            partition_type: 0x01,
            ..Default::default()
        };
        let mut fs = session.open().unwrap();
        let root = fs.root().unwrap();
        let entries = fs.list_directory(&root).unwrap();

        let out = tempfile::tempdir().unwrap();
        let n = copy_image_entries_to_host(fs.as_mut(), &entries, out.path()).expect("extract");
        assert_eq!(n, 2);
        assert_eq!(std::fs::read(out.path().join("A.TXT")).unwrap(), b"hello");
        assert_eq!(
            std::fs::read(out.path().join("D").join("B.TXT")).unwrap(),
            b"deep"
        );
    }

    /// host -> host: copy a file and a directory subtree between host folders.
    #[test]
    fn copy_host_to_host_copies_tree() {
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("f.txt"), b"data").unwrap();
        std::fs::create_dir(src.path().join("dir")).unwrap();
        std::fs::write(src.path().join("dir").join("g.txt"), b"more").unwrap();

        let entries = host_children(&src.path().to_string_lossy());
        let dest = tempfile::tempdir().unwrap();
        let n = copy_host_entries_to_host(&entries, dest.path()).expect("copy");
        assert_eq!(n, 2);
        assert_eq!(std::fs::read(dest.path().join("f.txt")).unwrap(), b"data");
        assert_eq!(
            std::fs::read(dest.path().join("dir").join("g.txt")).unwrap(),
            b"more"
        );
    }
}
