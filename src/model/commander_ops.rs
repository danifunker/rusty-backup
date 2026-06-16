//! Edit-application engine for Commander Mode panes.
//!
//! A Commander image pane stages [`StagedEdit`]s into an
//! [`EditQueue`](crate::model::edit_queue::EditQueue) (delete, and later copy);
//! nothing touches disk until the user clicks Apply. [`apply_edits`] is the leaf
//! that opens the pane's source read-write, replays the queue in order, syncs,
//! and commits the container — the same shape as the browse view's
//! `apply_staged_edits`. [`spawn_apply`] runs it on a worker thread behind an
//! [`ApplyStatus`] the pane polls, so a slow write never blocks the egui frame.

use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{Context, Result};

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
}
