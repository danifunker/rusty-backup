//! Decode a picked image off the UI thread when it needs it (`.hdz`, `.adz`, `.msa`, ...).
//!
//! `prepare_disk_image_path` can gunzip a whole hard disk, which froze the
//! Backup and Restore tabs (`docs/long_operations_inventory.md`, item 4). A
//! plain image is ready at once; one that needs decoding runs as a job, and the
//! tab takes the result from [`PendingImage::poll`].

use std::path::PathBuf;
use std::sync::Arc;

use rusty_backup::model::image_prep::{needs_decoding, prepare_disk_image_path};
use rusty_backup::model::job::{Job, Status};

/// A flat image path, and the tempdir that must outlive it when it was decoded.
pub type Prepared = (PathBuf, Option<tempfile::TempDir>);

pub enum Prepare {
    Ready(Prepared),
    Pending(PendingImage),
}

/// A decode in progress.
pub struct PendingImage {
    original: PathBuf,
    job: Job<Prepared>,
}

/// Start preparing `path` for Backup / Restore, which need a flat sector stream.
pub fn prepare(path: PathBuf) -> Prepare {
    if !needs_decoding(&path, true) {
        return Prepare::Ready((path, None));
    }
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let worker_path = path.clone();
    let job = Job::spawn(
        "Decompress image",
        Arc::new(super::ui_logger::EguiWake),
        move |ctx| {
            ctx.set_can_cancel(false);
            ctx.set_phase(format!("Decompressing {name}..."));
            Ok(prepare_disk_image_path(&worker_path, true)?)
        },
    );
    Prepare::Pending(PendingImage {
        original: path,
        job,
    })
}

impl PendingImage {
    pub fn show(&self, ui: &mut egui::Ui) {
        super::progress::show_job(ui, &self.job.snapshot());
    }

    /// The prepared path once the decode ends; a failed decode falls back to the original file.
    pub fn poll(&self) -> Option<Prepared> {
        let snap = self.job.snapshot();
        match snap.status {
            Status::Running => None,
            Status::Done => Some(
                self.job
                    .take_result()
                    .unwrap_or_else(|| (self.original.clone(), None)),
            ),
            Status::Failed(e) => {
                log::error!("Failed to decompress {}: {}", self.original.display(), e);
                Some((self.original.clone(), None))
            }
            Status::Cancelled => Some((self.original.clone(), None)),
        }
    }
}
