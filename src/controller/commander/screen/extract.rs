//! The screen's Extract: one entry of a pane (a file, or a folder and its contents) to a host folder.

use std::path::PathBuf;

use disk_core::entry::FileEntry;

use super::{purpose, CommanderController, ResourceForkMode, Side};
use crate::controller::{Effect, PickSpec};
use crate::fs::replace::OnConflict;
use crate::model::commander_ops::HostCopyJob;
use crate::model::extract::{extraction_conflicts, ExtractJob, ExtractOptions};
use crate::model::job::Resource;

/// Which extract options a volume offers (the GUI's fork, ProDOS name and QNX text controls).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtractApplies {
    pub forks: bool,
    pub prodos_suffix: bool,
    pub rs_to_lf: bool,
}

/// The extract options that apply to a volume of `fs_type`.
pub fn extract_applies(fs_type: &str) -> ExtractApplies {
    ExtractApplies {
        forks: matches!(fs_type, "HFS" | "HFS+" | "HFSX"),
        prodos_suffix: fs_type == "ProDOS",
        rs_to_lf: fs_type == "QNX 2",
    }
}

/// The fork modes an extract offers here: Native writes a macOS resource fork, so only there.
pub fn extract_fork_modes() -> Vec<ResourceForkMode> {
    let here = |m: &ResourceForkMode| cfg!(target_os = "macos") || *m != ResourceForkMode::Native;
    ResourceForkMode::ALL.iter().copied().filter(here).collect()
}

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
        let forks = extract_applies(fs_type.as_str()).forks;
        let label = Some(pane.volume().label.as_str());
        let conflicts: Vec<String> = extraction_conflicts(&entry, &dest, &options, forks, label)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_volume_offers_its_own_extract_options() {
        let hfs = extract_applies("HFS+");
        assert!(hfs.forks && !hfs.prodos_suffix && !hfs.rs_to_lf);
        assert!(extract_applies("ProDOS").prodos_suffix);
        assert!(extract_applies("QNX 2").rs_to_lf);
        let iso = extract_applies("ISO 9660");
        assert!(!iso.forks && !iso.prodos_suffix && !iso.rs_to_lf);
        let modes = extract_fork_modes();
        assert_eq!(
            modes[0] == ResourceForkMode::Native,
            cfg!(target_os = "macos")
        );
        assert!(modes.contains(&ResourceForkMode::AppleDouble));
    }
}
