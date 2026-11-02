//! Work between the panes and out to the host: copy, export, the conflict review and checksums.

use std::path::PathBuf;
use std::sync::Arc;

use disk_core::display::format_size;
use disk_core::entry::FileEntry;

use super::{
    ChecksumRow, Checksums, CommanderController, CopyRun, ExportFormat, PendingConflict,
    PendingExport, RenameNotice, Side,
};
use crate::controller::browser::pane::StageOutcome;
use crate::controller::commander::OnConflict;
use crate::controller::{Effect, PickSpec};
use crate::model::checksum::{self, ChecksumJob, FileChecksum};
use crate::model::commander_ops::{self, ExportJob, HostCopyJob};
use crate::model::job::{Resource, Status};

impl CommanderController {
    /// Copy the `from` pane's selection into the other pane's folder; returns the status line.
    pub(super) fn copy(&mut self, from: Side) -> String {
        if self.copy.is_some() {
            return "A copy is already in progress; wait for it to finish.".to_string();
        }
        let to = from.other();
        let other = to.label();
        let (src, dest) = (self.pane(from), self.pane(to));
        let entries = src.selected_entries();
        if entries.is_empty() {
            return format!("Nothing selected in the {} pane to copy.", from.label());
        }
        if !dest.can_receive() {
            return format!(
                "The {other} pane can't receive a copy (open a volume or folder there first)."
            );
        }
        let Some(dest_parent) = dest.cwd_entry() else {
            return "Open a destination first.".to_string();
        };
        if dest.is_remote_host() {
            return self.copy_to_remote(from, entries, dest_parent);
        }
        let src_host = src.is_host_pane();
        let reads = self.reads(from);
        match (src_host, dest.is_host_pane()) {
            // host -> image: stage the host paths themselves, nothing to extract.
            (true, false) => {
                let edits = commander_ops::stage_host_to_image(&entries, &dest_parent);
                let out = self.pane_mut(to).stage_edits(edits);
                self.note_staged(to, out)
            }
            (false, false) => {
                let Some(source) = self.pane(from).copy_stage_source() else {
                    return "Source volume is not open.".to_string();
                };
                let Some(temp_dir) = self.temp_dir() else {
                    return "Could not create a temp directory for the copy.".to_string();
                };
                let keep_dates = self.keep_dates;
                let spawned = self.jobs.spawn(
                    "Copying to other pane",
                    reads,
                    Arc::clone(&self.notify),
                    move |ctx| {
                        commander_ops::stage_copy_in_job(
                            source,
                            entries,
                            dest_parent,
                            temp_dir,
                            keep_dates,
                            ctx,
                        )
                    },
                );
                match spawned {
                    Ok(job) => {
                        self.copy = Some(CopyRun::Stage { job, dest: to });
                        format!("Copying to the {other} pane...")
                    }
                    Err(busy) => format!("Copy refused: {busy}"),
                }
            }
            (false, true) => {
                let Some(source) = self.pane(from).copy_stage_source() else {
                    return "Source volume is not open.".to_string();
                };
                let dest_dir = PathBuf::from(&dest_parent.path);
                let fork_mode = self.export_fork_mode;
                let conflicts =
                    commander_ops::host_copy_conflicts(&entries, &dest_dir, Some(fork_mode));
                let mut resources = reads;
                resources.push(Resource::Folder(dest_dir.clone()));
                let job = HostCopyJob::ImageToHost {
                    source,
                    entries,
                    dest_dir,
                    fork_mode,
                    on_conflict: OnConflict::Fail,
                };
                let where_to = format!("{other} folder");
                self.start_host_copy(job, Some(to), conflicts, resources, where_to)
            }
            (true, true) => {
                let dest_dir = PathBuf::from(&dest_parent.path);
                let conflicts = commander_ops::host_copy_conflicts(&entries, &dest_dir, None);
                let resources = vec![Resource::Folder(dest_dir.clone())];
                let job = HostCopyJob::HostToHost {
                    entries,
                    dest_dir,
                    on_conflict: OnConflict::Fail,
                };
                let where_to = format!("{other} folder");
                self.start_host_copy(job, Some(to), conflicts, resources, where_to)
            }
        }
    }

    /// Upload to a daemon's host folder, as an immediate write.
    #[cfg(feature = "remote")]
    fn copy_to_remote(&mut self, from: Side, entries: Vec<FileEntry>, parent: FileEntry) -> String {
        let to = from.other();
        let Some(dest_addr) = self.pane(to).remote_addr() else {
            return "The remote connection is unavailable; reconnect first.".to_string();
        };
        let dest_parent = parent.path;
        let job = if self.pane(from).is_host_pane() {
            HostCopyJob::HostToRemoteHost {
                entries,
                dest_addr,
                dest_parent,
            }
        } else if let Some(source) = self.pane(from).copy_stage_source() {
            HostCopyJob::ImageToRemoteHost {
                source,
                entries,
                dest_addr,
                dest_parent,
                fork_mode: self.export_fork_mode,
            }
        } else {
            return "Source volume is not open.".to_string();
        };
        let reads = self.reads(from);
        let where_to = format!("{} (remote) folder", to.label());
        match self.spawn_host(job, Some(to), reads, where_to.clone()) {
            Ok(()) => format!("Uploading to the {where_to}..."),
            Err(e) => e,
        }
    }

    #[cfg(not(feature = "remote"))]
    fn copy_to_remote(&mut self, _: Side, _: Vec<FileEntry>, _: FileEntry) -> String {
        "remote browsing is not built in.".to_string()
    }

    /// What a job reading the `side` pane's source holds; a host folder needs nothing.
    pub(super) fn reads(&self, side: Side) -> Vec<Resource> {
        let pane = self.pane(side);
        if pane.is_host_pane() {
            Vec::new()
        } else {
            pane.source_resources()
        }
    }

    fn temp_dir(&mut self) -> Option<PathBuf> {
        if self.temp.is_none() {
            self.temp = tempfile::tempdir().ok();
        }
        self.temp.as_ref().map(|t| t.path().to_path_buf())
    }

    /// The status line for a staged copy; raises the rename notice when names changed.
    fn note_staged(&mut self, dest: Side, out: StageOutcome) -> String {
        let mut msg = format!(
            "Staged copy of {} item(s) into the {} pane. Apply to write.",
            out.staged,
            dest.label()
        );
        if !out.renamed.is_empty() {
            msg.push_str(&format!(
                " {} renamed for {}.",
                out.renamed.len(),
                out.fs_type
            ));
        }
        if !out.dropped.is_empty() {
            msg.push_str(&format!(" {} left out.", out.dropped.len()));
        }
        if !out.renamed.is_empty() || !out.dropped.is_empty() {
            self.rename_notice = Some(RenameNotice { dest, outcome: out });
        }
        msg
    }

    pub(super) fn undo_rename_notice(&mut self) {
        let Some(notice) = self.rename_notice.take() else {
            return;
        };
        let msg = if self
            .pane_mut(notice.dest)
            .unstage_since(notice.outcome.queue_start)
        {
            "Copy removed from the staged edits."
        } else {
            "Can't undo the copy while an apply is running."
        };
        self.report(msg.to_string());
    }

    // --- immediate host writes --------------------------------------------

    /// Run `job` now, or hold it for the conflict review when it would overwrite `conflicts`.
    fn start_host_copy(
        &mut self,
        job: HostCopyJob,
        dest: Option<Side>,
        conflicts: Vec<String>,
        resources: Vec<Resource>,
        where_to: String,
    ) -> String {
        if conflicts.is_empty() {
            return match self.spawn_host(job, dest, resources, where_to.clone()) {
                Ok(()) => format!("Copying to the {where_to}..."),
                Err(e) => e,
            };
        }
        let n = conflicts.len();
        let msg = format!("{n} item(s) already exist in the {where_to}; choose what to do.");
        self.conflict = Some(PendingConflict {
            job,
            dest,
            names: conflicts,
            resources,
            where_to,
        });
        msg
    }

    fn spawn_host(
        &mut self,
        job: HostCopyJob,
        dest: Option<Side>,
        resources: Vec<Resource>,
        where_to: String,
    ) -> Result<(), String> {
        let spawned = self
            .jobs
            .spawn("Copying", resources, Arc::clone(&self.notify), move |ctx| {
                commander_ops::host_copy_in_job(job, ctx)
            });
        match spawned {
            Ok(job) => {
                self.copy = Some(CopyRun::Host {
                    job,
                    dest,
                    where_to,
                });
                Ok(())
            }
            Err(busy) => Err(format!("Copy refused: {busy}")),
        }
    }

    pub(super) fn resolve_conflict(&mut self, decision: Option<OnConflict>) {
        let Some(p) = self.conflict.take() else {
            return;
        };
        let msg = match decision {
            Some(on) => {
                match self.spawn_host(p.job.with_conflict(on), p.dest, p.resources, p.where_to) {
                    Ok(()) => "Copying...".to_string(),
                    Err(e) => e,
                }
            }
            None => "Copy cancelled; nothing was written.".to_string(),
        };
        self.report(msg);
    }

    pub(super) fn poll_copy(&mut self) {
        let finished = match &self.copy {
            Some(CopyRun::Host { job, .. }) => !job.is_running(),
            Some(CopyRun::Stage { job, .. }) => !job.is_running(),
            None => false,
        };
        if !finished {
            return;
        }
        match self.copy.take() {
            Some(CopyRun::Host {
                job,
                dest,
                where_to,
            }) => {
                if let Some(side) = dest {
                    self.reload(side);
                }
                let msg = match (job.snapshot().status, job.take_result()) {
                    (Status::Done, Some(done)) => {
                        let skipped = done.skipped.len();
                        let msg = if skipped == 0 {
                            format!("Copied {} file(s) to the {where_to}.", done.copied)
                        } else {
                            format!(
                                "Copied {} file(s) to the {where_to}; skipped {skipped} (see log).",
                                done.copied
                            )
                        };
                        self.report(msg);
                        for s in done.skipped {
                            self.record_log(format!("Skipped {s}"));
                        }
                        return;
                    }
                    (Status::Failed(e), _) => format!("Copy to the {where_to} failed: {e}"),
                    _ => format!("Copy to the {where_to} cancelled."),
                };
                self.report(msg);
            }
            Some(CopyRun::Stage { job, dest }) => {
                let msg = match (job.snapshot().status, job.take_result()) {
                    (Status::Done, Some(edits)) => {
                        let out = self.pane_mut(dest).stage_edits(edits);
                        self.note_staged(dest, out)
                    }
                    (Status::Failed(e), _) => {
                        format!("Copy to the {} pane failed: {e}", dest.label())
                    }
                    _ => "Copy cancelled; nothing was staged.".to_string(),
                };
                self.report(msg);
            }
            None => {}
        }
    }

    // --- export -------------------------------------------------------------

    /// Check an export and ask where it goes: a folder, or an archive file for single-file formats.
    pub(super) fn request_export(
        &mut self,
        from: Side,
        format: Option<ExportFormat>,
    ) -> Vec<Effect> {
        if self.copy.is_some() {
            self.status = "A copy is already in progress; wait for it to finish.".to_string();
            return Vec::new();
        }
        let format = format.unwrap_or(self.export_format);
        let src = self.pane(from);
        let entries = without_nested(src.selected_entries());
        if entries.is_empty() {
            self.status = format!("Nothing selected in the {} pane to export.", from.label());
            return Vec::new();
        }
        // A host folder has no filesystem to build an archive from.
        if src.is_host_pane() && format != ExportFormat::LooseFiles {
            self.status = format!(
                "'{}' export runs from an image/volume source; a host folder can only \
                 export as loose files.",
                format.label()
            );
            return Vec::new();
        }
        let spec = if format.is_single_file() {
            let ext = format.file_extension().unwrap_or("out");
            PickSpec {
                purpose: super::purpose::EXPORT,
                title: format!("Export as {}", format.label()),
                filters: vec![(format.label().to_string(), vec![ext.to_string()])],
                save: true,
                file_name: export_archive_name(&entries, ext),
            }
        } else {
            PickSpec {
                purpose: super::purpose::EXPORT,
                title: "Export to folder".into(),
                filters: Vec::new(),
                save: false,
                file_name: String::new(),
            }
        };
        self.export = Some(PendingExport {
            from,
            format,
            entries,
        });
        if format.is_single_file() {
            vec![Effect::PickFile(spec)]
        } else {
            vec![Effect::PickFolder(spec)]
        }
    }

    pub(super) fn export_picked(&mut self, path: Option<PathBuf>) -> String {
        let Some(PendingExport {
            from,
            format,
            entries,
        }) = self.export.take()
        else {
            return String::new();
        };
        let Some(dest) = path else {
            return "Export cancelled.".to_string();
        };
        let where_to = format!("export folder ({} pane)", from.label());
        if self.pane(from).is_host_pane() {
            let conflicts = commander_ops::host_copy_conflicts(&entries, &dest, None);
            let resources = vec![Resource::Folder(dest.clone())];
            let job = HostCopyJob::HostToHost {
                entries,
                dest_dir: dest,
                on_conflict: OnConflict::Fail,
            };
            return self.start_host_copy(job, None, conflicts, resources, where_to);
        }
        let Some(source) = self.pane(from).copy_stage_source() else {
            return "Source volume is not open.".to_string();
        };
        let mut resources = self.reads(from);
        resources.push(Resource::Folder(dest.clone()));
        let job = HostCopyJob::ExportSelection(Box::new(ExportJob {
            source,
            entries,
            dest,
            format,
            fork_mode: self.export_fork_mode,
        }));
        match self.spawn_host(job, None, resources, where_to) {
            Ok(()) => format!(
                "Exporting the {} pane selection as {}...",
                from.label(),
                format.label()
            ),
            Err(e) => e,
        }
    }

    // --- checksums ----------------------------------------------------------

    /// Hash the `from` pane's selected files (folders are skipped) in a job.
    pub(super) fn start_checksums(&mut self, from: Side) -> String {
        let src = self.pane(from);
        let entries = src.selected_entries();
        let files: Vec<&FileEntry> = entries.iter().filter(|e| e.is_file()).collect();
        let file_count = files.len();
        if file_count == 0 {
            return format!(
                "Select one or more files in the {} pane to checksum (directories are skipped).",
                from.label()
            );
        }
        let total_bytes: u64 = files.iter().map(|e| e.size).sum();
        let job = if src.is_host_pane() {
            ChecksumJob::Host { entries }
        } else if let Some(source) = src.copy_stage_source() {
            ChecksumJob::Image { source, entries }
        } else {
            return "Source volume is not open.".to_string();
        };
        let reads = self.reads(from);
        self.close_checksums();
        let spawned = self
            .jobs
            .spawn("Hashing", reads, Arc::clone(&self.notify), move |ctx| {
                checksum::checksum_in_job(job, total_bytes, ctx)
            });
        let job = match spawned {
            Ok(job) => job,
            Err(busy) => return format!("Checksums refused: {busy}"),
        };
        let title = if file_count == 1 {
            "Checksums".to_string()
        } else {
            format!("Checksums ({file_count} files)")
        };
        self.checksums = Some(Checksums {
            title,
            job: Some(job),
            rows: Vec::new(),
            error: None,
        });
        format!("Calculating checksums for {file_count} file(s)...")
    }

    pub(super) fn poll_checksums(&mut self) {
        let Some(c) = self.checksums.as_mut() else {
            return;
        };
        let Some(job) = c.job.as_ref().filter(|j| !j.is_running()) else {
            return;
        };
        match (job.snapshot().status, job.take_result()) {
            (Status::Done, Some(results)) => {
                c.rows = results.into_iter().map(checksum_row).collect()
            }
            (Status::Failed(e), _) => c.error = Some(e),
            _ => c.error = Some("cancelled".into()),
        }
        c.job = None;
    }

    /// Every finished row as text: the name and size, then one line per algorithm.
    pub(super) fn checksums_text(&self) -> Option<String> {
        let c = self.checksums.as_ref()?;
        let mut out = Vec::new();
        for row in &c.rows {
            out.push(format!("{}  ({})", row.name, format_size(row.size)));
            match (&row.values, &row.error) {
                (Some(values), _) => {
                    for (algo, v) in super::HashAlgo::ALL.iter().zip(values) {
                        out.push(format!("  {:<7}{v}", algo.label()));
                    }
                }
                (None, Some(e)) => out.push(format!("  failed: {e}")),
                (None, None) => {}
            }
        }
        (!out.is_empty()).then(|| out.join("\n"))
    }
}

fn checksum_row(fc: FileChecksum) -> ChecksumRow {
    let values = fc
        .set
        .map(|s| [s.crc32_hex(), s.md5_hex(), s.sha1_hex(), s.sha256_hex()]);
    ChecksumRow {
        name: fc.name,
        size: fc.size,
        error: fc.error,
        values,
    }
}

/// `<stem>.<ext>` for one selected entry (its name minus any extension), else `export.<ext>`.
fn export_archive_name(entries: &[FileEntry], ext: &str) -> String {
    let stem = match entries {
        [only] => only
            .name
            .rsplit_once('.')
            .map(|(s, _)| s)
            .filter(|s| !s.is_empty())
            .unwrap_or(&only.name),
        _ => "export",
    };
    format!("{stem}.{ext}")
}

/// The selection without entries inside another selected folder, which the folder's walk covers.
fn without_nested(entries: Vec<FileEntry>) -> Vec<FileEntry> {
    let inside = |p: &str, dir: &str| {
        let dir = dir.trim_end_matches('/');
        p.len() > dir.len() + 1 && p.starts_with(dir) && p.as_bytes()[dir.len()] == b'/'
    };
    let dirs: Vec<String> = entries
        .iter()
        .filter(|e| e.is_directory())
        .map(|e| e.path.clone())
        .collect();
    let nested = |e: &FileEntry| dirs.iter().any(|d| inside(&e.path, d));
    entries.into_iter().filter(|e| !nested(e)).collect()
}
