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
    if !backup_path_for(parent).exists() {
        job.set_phase(format!(
            "Backing up {} before editing...",
            parent
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default()
        ));
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
