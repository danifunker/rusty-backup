//! Start editing a CHD: back it up, then open an edit session over it.
//!
//! The backup is a full copy of the CHD (`<name>.chd_backup`, made once), so the
//! browse view runs this as a [`crate::model::job::Job`] instead of freezing on it
//! (`docs/long_operations_inventory.md`, item 3).

use std::path::{Path, PathBuf};

use crate::model::job::JobCtx;
use crate::rbformats::chd_edit::{
    backup_path_for, is_compressed_chd, make_backup_copy, ChdEditSession,
};

/// An open CHD edit session.
pub struct ChdEditOpen {
    pub session: ChdEditSession,
    /// The diff CHD the writes go to; `None` for an uncompressed CHD, which is written in place.
    pub diff_path: Option<PathBuf>,
    pub compressed: bool,
}

/// Where a compressed CHD's edits accumulate: `<stem>.edit-diff.chd` beside it.
pub fn diff_path_for(parent: &Path) -> PathBuf {
    let parent_dir = parent.parent().unwrap_or(Path::new("."));
    let mut diff_name = parent
        .file_stem()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    diff_name.push(".edit-diff.chd");
    parent_dir.join(diff_name)
}

/// Back up `parent` (once) and open it for editing; the error is the message the view shows.
pub fn open_for_editing(parent: &Path, job: &JobCtx) -> Result<ChdEditOpen, String> {
    job.set_can_cancel(false);
    let backup = backup_path_for(parent);
    if !backup.exists() {
        job.set_phase(format!(
            "Backing up {} before editing...",
            parent
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default()
        ));
        copy_with_progress(parent, &backup, job)
            .map_err(|e| format!("Error creating backup: {e}"))?;
    }
    make_backup_copy(parent).map_err(|e| format!("Error creating backup: {e}"))?;
    job.set_phase("Opening the CHD for editing...");
    let compressed = is_compressed_chd(parent).map_err(|e| format!("Error inspecting CHD: {e}"))?;
    if compressed {
        let diff = diff_path_for(parent);
        let session = if diff.exists() {
            ChdEditSession::reopen_with_diff(parent, &diff)
        } else {
            ChdEditSession::open_with_diff(parent, &diff)
        }
        .map_err(|e| format!("Error opening CHD diff: {e}"))?;
        Ok(ChdEditOpen {
            session,
            diff_path: Some(diff),
            compressed,
        })
    } else {
        let session = ChdEditSession::open_uncompressed(parent)
            .map_err(|e| format!("Error opening CHD: {e}"))?;
        Ok(ChdEditOpen {
            session,
            diff_path: None,
            compressed,
        })
    }
}

/// Merge a compressed CHD's diff into its parent as a job; a backup's metadata is refreshed after.
pub fn flatten_in_job(
    parent: &Path,
    diff: &Path,
    backup_folder: Option<&Path>,
    job: &JobCtx,
) -> Result<String, String> {
    let _wake = crate::os::wakelock::acquire("Rusty Backup: CHD diff flatten");
    job.set_can_cancel(false);
    let total = std::fs::metadata(diff).map(|m| m.len()).unwrap_or(0);
    let mut log = |m: &str| log::info!("{m}");
    crate::rbformats::chd_edit::flatten_to_parent(
        parent,
        diff,
        None,
        &mut |b| job.set_bytes(b, total),
        &|| false,
        &mut log,
    )
    .map_err(|e| format!("Error flattening CHD: {e:#}"))?;
    let mut said = String::from("Changes saved successfully.");
    // The bytes are already right; a stale metadata.json is a warning, not a rollback.
    if let Some(folder) = backup_folder {
        let mut log = |m: &str| log::info!("{m}");
        match crate::backup::single_file_chd::refresh_metadata_after_edit(folder, &mut log) {
            Ok(()) => said.push_str(" Backup metadata.json refreshed."),
            Err(e) => said.push_str(&format!(
                " (Warning: failed to refresh backup metadata.json: {e:#})"
            )),
        }
    }
    Ok(said)
}

/// Copy `from` to `to` in 4 MiB chunks, reporting bytes; a partial copy is removed on failure.
fn copy_with_progress(from: &Path, to: &Path, job: &JobCtx) -> std::io::Result<()> {
    use std::io::{Read, Write};
    let run = || -> std::io::Result<()> {
        let mut src = std::fs::File::open(from)?;
        let total = src.metadata()?.len();
        let mut dst = std::fs::File::create(to)?;
        let mut buf = vec![0u8; 4 << 20];
        let mut done = 0u64;
        loop {
            let n = src.read(&mut buf)?;
            if n == 0 {
                break;
            }
            dst.write_all(&buf[..n])?;
            done += n as u64;
            job.set_bytes(done, total);
        }
        dst.sync_all()
    };
    let result = run();
    if result.is_err() {
        let _ = std::fs::remove_file(to);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::job::{Job, NoNotify, Status};
    use libchdman_rs::hd::{create_from_reader, HdCreateOptions};
    use std::io::Cursor;
    use std::sync::Arc;

    fn open(parent: PathBuf) -> Result<ChdEditOpen, String> {
        let job = Job::spawn("open", Arc::new(NoNotify), move |job| {
            Ok(open_for_editing(&parent, job))
        });
        assert_eq!(job.wait().status, Status::Done);
        job.take_result().unwrap()
    }

    #[test]
    fn an_uncompressed_chd_is_backed_up_and_opened_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let chd = dir.path().join("disk.chd");
        let data = vec![0x5Au8; 64 * 1024];
        let opts = HdCreateOptions {
            logical_size: data.len() as u64,
            hunk_size: 4096,
            unit_size: 512,
            codecs: [0u32; 4],
            geometry: None,
            ident: None,
        };
        create_from_reader(&mut Cursor::new(&data), &chd, opts, &mut |_| {}, &|| false).unwrap();
        let opened = open(chd.clone()).unwrap();
        assert!(!opened.compressed);
        assert!(opened.diff_path.is_none());
        assert!(backup_path_for(&chd).exists());
    }

    #[test]
    fn a_missing_chd_reports_the_backup_error() {
        let err = open(PathBuf::from("/nonexistent/disk.chd")).err().unwrap();
        assert!(err.starts_with("Error creating backup:"), "{err}");
    }

    #[test]
    fn the_diff_sits_beside_the_parent() {
        assert_eq!(
            diff_path_for(Path::new("/x/game.chd")),
            PathBuf::from("/x/game.edit-diff.chd")
        );
    }
}
