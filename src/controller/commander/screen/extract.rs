//! The screen's Extract: one entry of a pane (a file, or a folder and its contents) to a host folder.

use std::path::PathBuf;

use disk_core::entry::FileEntry;

use super::{purpose, CommanderController, Side};
use crate::controller::{Effect, PickSpec};
use crate::fs::replace::OnConflict;
use crate::model::commander_ops::HostCopyJob;
use crate::model::extract::{extraction_conflicts, ExtractJob, ExtractOptions};
use crate::model::job::Resource;

/// An extraction waiting for its destination.
pub(super) struct PendingExtract {
    from: Side,
    entry: FileEntry,
    options: ExtractOptions,
}

impl CommanderController {
    /// Check an extraction of `path` and ask for the folder it goes to (unless the caller chose it).
    pub(super) fn request_extract(
        &mut self,
        from: Side,
        path: &str,
        options: ExtractOptions,
        dest: Option<PathBuf>,
    ) -> Vec<Effect> {
        if self.copy.is_some() {
            self.status = "A copy is already in progress; wait for it to finish.".to_string();
            return Vec::new();
        }
        let pane = self.pane(from);
        let Some(entry) = pane.entry_at(path).filter(|_| !pane.is_host_pane()) else {
            self.status = format!("Nothing to extract at '{path}'.");
            return Vec::new();
        };
        self.extract = Some(PendingExtract {
            from,
            entry,
            options,
        });
        if dest.is_some() {
            self.status = self.extract_picked(dest);
            return Vec::new();
        }
        vec![Effect::PickFolder(PickSpec {
            purpose: purpose::EXTRACT,
            title: "Extract to folder".into(),
            filters: Vec::new(),
            save: false,
            file_name: String::new(),
        })]
    }

    /// Start the extraction into `dest`; files it would replace go to the conflict review first.
    pub(super) fn extract_picked(&mut self, dest: Option<PathBuf>) -> String {
        let Some(PendingExtract {
            from,
            entry,
            options,
        }) = self.extract.take()
        else {
            return String::new();
        };
        let Some(dest) = dest else {
            return "Extraction cancelled.".to_string();
        };
        let pane = self.pane(from);
        let Some(source) = pane.base_stage_source() else {
            return "Source volume is not open.".to_string();
        };
        let fs_type = pane.volume().fs_type.clone();
        let options = options.for_fs(&fs_type);
        let forks = matches!(fs_type.as_str(), "HFS" | "HFS+" | "HFSX");
        let conflicts: Vec<String> = extraction_conflicts(&entry, &dest, &options, forks)
            .iter()
            .map(|p| p.display().to_string())
            .collect();
        let mut resources = self.reads(from);
        resources.push(Resource::Folder(dest.clone()));
        let job = HostCopyJob::Extract(Box::new(ExtractJob {
            source,
            entry,
            dest,
            options,
            forks,
            on_conflict: OnConflict::Fail,
        }));
        let where_to = "extract folder".to_string();
        self.start_host_copy(job, None, conflicts, resources, where_to)
    }
}
