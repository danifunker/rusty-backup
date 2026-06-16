//! Edit-application engine for Commander Mode panes.
//!
//! A Commander image pane stages [`StagedEdit`]s into an
//! [`EditQueue`](crate::model::edit_queue::EditQueue) (delete, and later copy);
//! nothing touches disk until the user clicks Apply. [`apply_edits`] is the leaf
//! that opens the pane's source read-write, replays the queue in order, syncs,
//! and commits the container — the same shape as the browse view's
//! `apply_staged_edits`. [`spawn_apply`] runs it on a worker thread behind an
//! [`ApplyStatus`] the pane polls, so a slow write never blocks the egui frame.

use std::path::Path;
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
pub fn stage_copy(
    src_fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    dest_parent: &FileEntry,
    temp_dir: &Path,
) -> Result<Vec<StagedEdit>> {
    let mut edits = Vec::new();
    stage_copy_into(src_fs, entries, dest_parent, temp_dir, &mut edits)?;
    Ok(edits)
}

fn stage_copy_into(
    src_fs: &mut dyn Filesystem,
    entries: &[FileEntry],
    dest_parent: &FileEntry,
    temp_dir: &Path,
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
                stage_copy_into(src_fs, &children, &child_parent, temp_dir, edits)?;
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
        let edits =
            stage_copy(src_fs.as_mut(), &to_copy, &FileEntry::root(), temp.path()).expect("stage");
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
}
