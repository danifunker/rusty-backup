//! A pane's volume tools (profile `tools`): check and repair, bless, the text editor, import, metadata.
//!
//! Check, repair and reading a file for the editor are jobs; their results wait in the pane
//! (`tool_report`, `take_text_doc`). Everything that writes goes through the staged-edit queue,
//! so a profile without staging writes it at once through the usual preflight and apply jobs:
//! bless is a `BlessFolder`, an import and a saved text are `AddFile`s (the text from a temp file,
//! replacing the original and keeping its metadata), and the metadata form's fields become
//! `MetadataEdit`s. Repair writes in its own job, then reopens the partition like an apply.

use std::path::PathBuf;

use disk_core::entry::FileEntry;

use super::source::purpose;
use super::{BrowserPaneAction, BrowserPaneController};
use crate::controller::{ConfirmSpec, Effect, PickSpec};
use crate::model::browse_session::BrowseSession;
use crate::model::commander_ops::{stage_host_for, stage_host_to_image, StageSource};
use crate::model::edit_queue::{MetadataEdit, StagedEdit};
use crate::model::job::{Job, Status};
use crate::model::text_edit::{decode_for_edit, encode_after_edit, TextEditError, TextShape};

/// Largest file the text editor opens: it holds the whole file as lines.
pub const MAX_TEXT_EDIT: usize = 1024 * 1024;

/// Seconds between the Mac (1904) and Unix (1970) epochs.
const MAC_EPOCH_OFFSET: i64 = 2_082_844_800;

/// A check or repair result, as lines for a scrolling window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolReport {
    pub title: String,
    pub lines: Vec<String>,
}

/// A file read for the text editor: UTF-8 with LF endings, and how to write it back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextDoc {
    pub name: String,
    /// The folder holding it, where `SaveText` writes it back.
    pub folder: String,
    pub text: String,
    pub shape: TextShape,
}

/// The metadata form's fields as text; a blank field leaves that value alone.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataFields {
    pub type_code: String,
    pub creator: String,
    /// `YYYY-MM-DD HH:MM:SS`, UTC.
    pub modified: String,
    pub created: String,
    /// Octal permission bits.
    pub mode: String,
    /// `uid:gid`.
    pub owner: String,
}

/// The open metadata form: which entry, the values it started from, and why a submit was refused.
#[derive(Debug, Clone)]
pub struct MetadataForm {
    pub entry: FileEntry,
    pub initial: MetadataFields,
    pub error: Option<String>,
}

enum ToolJob {
    Check {
        title: String,
        job: Job<Vec<String>>,
    },
    Repair {
        title: String,
        job: Job<Vec<String>>,
    },
    Read(Job<TextDoc>),
}

#[derive(Default)]
pub(super) struct ToolState {
    job: Option<ToolJob>,
    report: Option<ToolReport>,
    text: Option<TextDoc>,
    form: Option<MetadataForm>,
    bless: Option<FileEntry>,
    /// Saved editor texts wait here until their apply has read them.
    temp: Option<tempfile::TempDir>,
    /// Host items an `AddHostPaths` could not stage: (path, why).
    staging_errors: Vec<(String, String)>,
}

fn msg(text: impl Into<String>) -> Vec<Effect> {
    vec![Effect::Message(text.into())]
}

/// A Mac-epoch timestamp as `YYYY-MM-DD HH:MM:SS` (UTC).
pub fn format_mac_date(mac: u32) -> String {
    let unix = mac as i64 - MAC_EPOCH_OFFSET;
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|dt| dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

/// `YYYY-MM-DD HH:MM:SS` back to a Mac-epoch timestamp.
pub fn parse_mac_date(s: &str) -> Option<u32> {
    let ndt = chrono::NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S").ok()?;
    let mac = ndt.and_utc().timestamp() + MAC_EPOCH_OFFSET;
    (0..=u32::MAX as i64).contains(&mac).then_some(mac as u32)
}

/// A 4-byte Mac OSType as editable text (non-printable bytes become `?`).
fn code_to_string(code: Option<[u8; 4]>) -> String {
    let printable = |&c: &u8| {
        if (0x20..0x7f).contains(&c) {
            c as char
        } else {
            '?'
        }
    };
    code.map(|b| b.iter().map(printable).collect())
        .unwrap_or_default()
}

/// Up to four Latin-1 characters as an OSType, space-padded.
fn parse_code(s: &str) -> Result<[u8; 4], String> {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() > 4 || chars.iter().any(|c| (*c as u32) > 0xFF) {
        return Err(format!("'{s}' is not a four-character code"));
    }
    let mut out = [b' '; 4];
    for (i, c) in chars.iter().enumerate() {
        out[i] = *c as u32 as u8;
    }
    Ok(out)
}

impl MetadataFields {
    /// The form's starting values for `e`.
    pub fn of(e: &FileEntry) -> Self {
        MetadataFields {
            type_code: code_to_string(e.type_code),
            creator: code_to_string(e.creator_code),
            modified: e
                .mac_dates
                .map(|d| format_mac_date(d.1))
                .unwrap_or_default(),
            created: e
                .mac_dates
                .map(|d| format_mac_date(d.0))
                .unwrap_or_default(),
            mode: e
                .mode
                .map(|m| format!("{:o}", m & 0o7777))
                .unwrap_or_default(),
            owner: match (e.uid, e.gid) {
                (Some(u), Some(g)) => format!("{u}:{g}"),
                _ => String::new(),
            },
        }
    }

    /// The edits that turn `initial` into these values; a blank or unchanged field stages nothing.
    pub fn edits(&self, entry: &FileEntry, initial: &Self) -> Result<Vec<MetadataEdit>, String> {
        let changed = |new: &str, old: &str| {
            let new = new.trim();
            (!new.is_empty() && new != old.trim()).then(|| new.to_string())
        };
        let mut out = Vec::new();
        let ty = changed(&self.type_code, &initial.type_code);
        let cr = changed(&self.creator, &initial.creator);
        if (ty.is_some() || cr.is_some())
            && (entry.type_code.is_some() || entry.creator_code.is_some())
        {
            out.push(MetadataEdit::TypeCreator {
                entry: entry.clone(),
                type_code: parse_code(&ty.unwrap_or_else(|| self.type_code.clone()))?,
                creator_code: parse_code(&cr.unwrap_or_else(|| self.creator.clone()))?,
            });
        }
        let modified = changed(&self.modified, &initial.modified);
        let created = changed(&self.created, &initial.created);
        if modified.is_some() || created.is_some() {
            let parse = |s: Option<String>, what: &str| match s {
                None => Ok(None),
                Some(s) => parse_mac_date(&s)
                    .map(Some)
                    .ok_or_else(|| format!("Bad {what} date - use YYYY-MM-DD HH:MM:SS")),
            };
            let (m, c) = (parse(modified, "modified")?, parse(created, "created")?);
            // Start from what the file has, so editing one date never resets the other.
            let (cur_c, cur_m, cur_b) = entry.mac_dates.unwrap_or_else(|| {
                let now = m.or(c).unwrap_or(0);
                (now, now, now)
            });
            out.push(MetadataEdit::Dates {
                entry: entry.clone(),
                create: c.unwrap_or(cur_c),
                modify: m.unwrap_or(cur_m),
                backup: cur_b,
            });
        }
        if let Some(mode) = changed(&self.mode, &initial.mode) {
            match u32::from_str_radix(&mode, 8) {
                Ok(v) if v <= 0o7777 => out.push(MetadataEdit::Permissions {
                    entry: entry.clone(),
                    mode: v,
                }),
                _ => return Err("Bad mode - octal, 1-4 digits, <= 7777".into()),
            }
        }
        if let Some(owner) = changed(&self.owner, &initial.owner) {
            let pair = owner
                .split_once(':')
                .and_then(|(u, g)| Some((u.trim().parse().ok()?, g.trim().parse().ok()?)));
            match pair {
                Some((uid, gid)) => out.push(MetadataEdit::Owner {
                    entry: entry.clone(),
                    uid,
                    gid,
                }),
                None => return Err("Bad owner - want uid:gid, e.g. 0:0".into()),
            }
        }
        Ok(out)
    }
}

/// A check's result as report lines.
pub fn fsck_lines(result: Option<Result<crate::fs::FsckResult, String>>) -> Vec<String> {
    let result = match result {
        Some(Ok(r)) => r,
        Some(Err(e)) => return vec![format!("Check failed: {e}")],
        None => {
            return vec![
                "This filesystem has no integrity checker.".to_string(),
                String::new(),
                "(fsck is available for FAT/exFAT, ext*, NTFS, HFS/HFS+, and the".to_string(),
                " supported retro filesystems.)".to_string(),
            ]
        }
    };
    let mut lines = Vec::new();
    let s = &result.stats;
    lines.push(format!(
        "{} files / {} dirs checked",
        s.files_checked, s.directories_checked
    ));
    for (label, value) in &s.extra {
        lines.push(format!("  {label}: {value}"));
    }
    lines.push(String::new());
    if result.is_clean() {
        lines.push("Result: CLEAN - no errors found.".to_string());
    } else {
        lines.push(format!("Result: {} error(s).", result.errors.len()));
    }
    if result.repairable {
        lines.push("Some errors are repairable - use Repair.".to_string());
    }
    let issues = [("Errors:", &result.errors), ("Warnings:", &result.warnings)];
    for (heading, list) in issues {
        if !list.is_empty() {
            lines.push(String::new());
            lines.push(heading.to_string());
            for e in list.iter().filter(|e| !e.debug) {
                lines.push(format!("  [{}] {}", e.code, e.message));
            }
        }
    }
    if !result.orphaned_entries.is_empty() {
        lines.push(String::new());
        lines.push(format!(
            "Orphaned entries (unrepairable): {}",
            result.orphaned_entries.len()
        ));
        for o in result.orphaned_entries.iter().take(20) {
            let kind = if o.is_directory { "dir " } else { "file" };
            lines.push(format!(
                "  {kind} {} (parent {})",
                o.name, o.missing_parent_id
            ));
        }
    }
    lines
}

/// A repair's result as report lines.
fn repair_lines(report: &crate::fs::RepairReport) -> Vec<String> {
    let mut lines = vec![format!(
        "Applied {} fix(es); {} failed; {} unrepairable.",
        report.fixes_applied.len(),
        report.fixes_failed.len(),
        report.unrepairable_count
    )];
    for (heading, list, mark) in [
        ("Fixes applied:", &report.fixes_applied, '+'),
        ("Fixes failed:", &report.fixes_failed, '-'),
    ] {
        if !list.is_empty() {
            lines.push(String::new());
            lines.push(heading.to_string());
            lines.extend(list.iter().map(|f| format!("  {mark} {f}")));
        }
    }
    lines
}

fn check_source(source: StageSource) -> Vec<String> {
    match source.open() {
        Ok(mut fs) => fsck_lines(fs.fsck().map(|r| r.map_err(|e| e.to_string()))),
        Err(e) => vec![format!("Check failed: {e:#}")],
    }
}

fn repair_session(session: &BrowseSession) -> anyhow::Result<Vec<String>> {
    use anyhow::Context as _;
    let (mut efs, commit) = session
        .open_editable()
        .context("opening the volume for repair")?;
    let report = efs.repair().context("repair")?;
    efs.sync_metadata()
        .context("writing the repaired metadata")?;
    drop(efs);
    commit.commit().context("committing the repair")?;
    let mut lines = repair_lines(&report);
    lines.push(String::new());
    lines.push("--- re-check after repair ---".to_string());
    lines.extend(check_source(StageSource::Session(session.clone())));
    Ok(lines)
}

impl BrowserPaneController {
    // --- read accessors ---------------------------------------------------

    /// The last check or repair report, until `DismissToolReport`.
    pub fn tool_report(&self) -> Option<&ToolReport> {
        self.tl.report.as_ref()
    }

    /// The check, repair or editor read in progress.
    pub fn tool_working(&self) -> Option<crate::model::job::Snapshot> {
        match self.tl.job.as_ref()? {
            ToolJob::Check { job, .. } | ToolJob::Repair { job, .. } => Some(job.snapshot()),
            ToolJob::Read(job) => Some(job.snapshot()),
        }
    }

    pub(super) fn tool_busy(&self) -> bool {
        self.tl.job.is_some()
    }

    /// A file read for the text editor, handed over once; the view keeps the buffer.
    pub fn take_text_doc(&mut self) -> Option<TextDoc> {
        self.tl.text.take()
    }

    /// Host items the last host staging could not stage, as (path, why), until dismissed.
    pub fn staging_errors(&self) -> &[(String, String)] {
        &self.tl.staging_errors
    }

    /// The open metadata form.
    pub fn metadata_form(&self) -> Option<&MetadataForm> {
        self.tl.form.as_ref()
    }

    /// Whole-image commands for the shell's command line, with this source's path filled in.
    pub fn transforms(&self) -> Vec<(&'static str, String)> {
        let Some(img) = self.source_path().filter(|_| self.profile.tools.transforms) else {
            return Vec::new();
        };
        let img = img.display().to_string();
        let stem = std::path::Path::new(&img)
            .with_extension("")
            .display()
            .to_string();
        let list = [
            // `convert`'s second argument is a folder the converted file lands in.
            (
                "Convert to another format",
                "convert \"{IMG}\" \"{STEM}-converted\" --format vhd",
            ),
            (
                "Resize a partition in place",
                "resize --size <SIZE> \"{IMG}\"",
            ),
            (
                "Expand into a larger new image",
                "expand --size <SIZE> --output \"{STEM}-expanded.img\" \"{IMG}\"",
            ),
            ("Partition table (inspect / partmap)", "inspect \"{IMG}\""),
        ];
        list.iter()
            .map(|(label, t)| (*label, t.replace("{IMG}", &img).replace("{STEM}", &stem)))
            .collect()
    }

    // --- actions ----------------------------------------------------------

    /// Handles a tool action, or hands it back.
    pub(super) fn apply_tool(
        &mut self,
        action: BrowserPaneAction,
    ) -> Result<Vec<Effect>, BrowserPaneAction> {
        use BrowserPaneAction as A;
        let fx = match action {
            A::Fsck => self.start_fsck(),
            A::Repair => self.ask_repair(),
            A::Confirmed { purpose, yes } if purpose == purpose::REPAIR => {
                if yes {
                    self.start_repair()
                } else {
                    Vec::new()
                }
            }
            A::DismissToolReport => {
                self.tl.report = None;
                Vec::new()
            }
            A::Bless => self.ask_bless(),
            A::Confirmed { purpose, yes } if purpose == purpose::BLESS => {
                match self.tl.bless.take() {
                    Some(entry) if yes => self.stage_tool(StagedEdit::BlessFolder { entry }),
                    _ => Vec::new(),
                }
            }
            A::EditText => self.start_read(),
            A::SaveText {
                name,
                folder,
                text,
                shape,
            } => self.save_text(&name, &folder, &text, &shape),
            A::Import => self.pick_import(),
            A::Picked { purpose, path } if purpose == purpose::IMPORT => match path {
                Some(p) => self.import(p),
                None => Vec::new(),
            },
            A::AddHostPaths { paths, folder } => self.add_host_paths(&paths, folder.as_deref()),
            A::DismissStagingErrors => {
                self.tl.staging_errors.clear();
                Vec::new()
            }
            A::EditMetadata => self.open_form(),
            A::SubmitMetadata(fields) => self.submit_form(&fields),
            A::CancelMetadata => {
                self.tl.form = None;
                Vec::new()
            }
            other => return Err(other),
        };
        Ok(fx)
    }

    /// The listing entry under the cursor (not `..`, a pending add or a wrapper's contents).
    fn cursor_entry(&self) -> Option<FileEntry> {
        let row = self.cursor_row()?;
        if row.is_parent() || row.is_tree || row.kind == super::RowKind::PendingAdd {
            return None;
        }
        let deep = self.dirs.index.get(&row.node_id).map(|r| r.entry.clone());
        deep.or_else(|| self.entry_named(&row.name))
    }

    /// The folder the cursor row is in: its combined-tree folder, else the listing's.
    fn cursor_folder(&self) -> String {
        let row = self.cursor_row();
        let deep = row.and_then(|r| self.dirs.index.get(&r.node_id));
        match deep {
            Some(r) => r.parent.path.clone(),
            None => self.listing.cwd_path().to_string(),
        }
    }

    fn writable_volume(&self) -> Result<(), Vec<Effect>> {
        if !self.stages_edits() || self.is_read_only() || self.is_remote() {
            return Err(msg("this volume is read-only here."));
        }
        if self.is_busy() {
            return Err(msg("wait for the current operation to finish."));
        }
        Ok(())
    }

    fn stage_tool(&mut self, edit: StagedEdit) -> Vec<Effect> {
        let label = crate::model::commander_ops::edit_label(&edit);
        self.queue.push(edit);
        self.auto_apply();
        let then = if self.profile.staging {
            " Apply to write."
        } else {
            ""
        };
        msg(format!("staged {label}.{then}"))
    }

    fn start_fsck(&mut self) -> Vec<Effect> {
        if self.is_busy() {
            return msg("wait for the current operation to finish.");
        }
        let Some(source) = self.local_stage_source() else {
            return msg("no volume is open.");
        };
        let title = format!("fsck - {}", self.volume().name());
        let resources = self.apply_resources();
        let (jobs, notify) = self.job_services();
        let spawned = jobs.spawn("Checking the filesystem", resources, notify, move |ctx| {
            ctx.set_can_cancel(false);
            Ok(check_source(source))
        });
        match spawned {
            Ok(job) => {
                self.tl.job = Some(ToolJob::Check { title, job });
                msg("checking the filesystem...")
            }
            Err(busy) => msg(format!("cannot check: {busy}")),
        }
    }

    fn ask_repair(&mut self) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        if self.session().is_none() {
            return msg("no volume is open.");
        }
        let staged = if self.queue.is_empty() {
            String::new()
        } else {
            format!(
                " The {} staged edit(s) are dropped first.",
                self.queue.len()
            )
        };
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::REPAIR,
            title: "Repair filesystem?".into(),
            message: format!(
                "Repair {} in place? This writes to the image now.{staged}",
                self.volume().name()
            ),
            confirm_label: "Repair".into(),
            destructive: true,
        })]
    }

    fn start_repair(&mut self) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        let Some(session) = self.session().cloned() else {
            return msg("no volume is open.");
        };
        self.queue.clear();
        let title = format!("repair - {}", self.volume().name());
        let resources = self.apply_resources();
        let (jobs, notify) = self.job_services();
        let spawned = jobs.spawn("Repairing the filesystem", resources, notify, move |ctx| {
            ctx.set_can_cancel(false);
            repair_session(&session)
        });
        match spawned {
            Ok(job) => {
                self.tl.job = Some(ToolJob::Repair { title, job });
                msg("repairing the filesystem...")
            }
            Err(busy) => msg(format!("cannot repair: {busy}")),
        }
    }

    /// The folder Bless acts on: the cursor folder in the listing, else the current one (never the root).
    fn bless_target(&self) -> Option<FileEntry> {
        if !self.tree_focused() {
            if let Some(e) = self.cursor_entry().filter(|e| e.is_directory()) {
                return Some(e);
            }
        }
        self.listing
            .cwd()
            .filter(|_| !self.listing.at_root())
            .cloned()
    }

    fn ask_bless(&mut self) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        let Some(target) = self.bless_target() else {
            return msg("select a folder to bless as the System Folder.");
        };
        let name = target.name.clone();
        self.tl.bless = Some(target);
        vec![Effect::Confirm(ConfirmSpec {
            purpose: purpose::BLESS,
            title: "Bless System Folder?".into(),
            message: format!("Make '{name}' the folder this volume boots from?"),
            confirm_label: "Bless".into(),
            destructive: false,
        })]
    }

    fn start_read(&mut self) -> Vec<Effect> {
        if self.is_busy() {
            return msg("wait for the current operation to finish.");
        }
        let Some(entry) = self.cursor_entry() else {
            return msg("select a file to edit.");
        };
        if !entry.is_file() {
            return msg("not a file.");
        }
        if entry.size > MAX_TEXT_EDIT as u64 {
            return msg(format!(
                "too large to edit here ({} KiB); use rb-cli edit.",
                entry.size / 1024
            ));
        }
        let Some(source) = self.local_stage_source() else {
            return msg("no volume is open.");
        };
        let folder = self.cursor_folder();
        let resources = self.apply_resources();
        let (jobs, notify) = self.job_services();
        let spawned = jobs.spawn(format!("Reading {}", entry.name), resources, notify, {
            move |_| {
                let mut fs = source.open()?;
                let bytes = fs.read_file(&entry, MAX_TEXT_EDIT)?;
                match decode_for_edit(&bytes, fs.fs_type(), None) {
                    Ok(d) => Ok(TextDoc {
                        name: entry.name.clone(),
                        folder,
                        text: d.text,
                        shape: d.shape,
                    }),
                    // Refuse rather than mangle: a text round trip does not keep arbitrary bytes.
                    Err(TextEditError::NotText { reason }) => {
                        anyhow::bail!("not a text file ({reason})")
                    }
                    Err(e) => anyhow::bail!("cannot edit: {e}"),
                }
            }
        });
        match spawned {
            Ok(job) => {
                self.tl.job = Some(ToolJob::Read(job));
                Vec::new()
            }
            Err(busy) => msg(format!("cannot read: {busy}")),
        }
    }

    fn save_text(
        &mut self,
        name: &str,
        folder: &str,
        text: &str,
        shape: &TextShape,
    ) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        let bytes = match encode_after_edit(text, shape, false) {
            Ok(b) => b,
            // Named and nothing written, as the CLI does: substituting silently changes the file.
            Err(e @ TextEditError::Unrepresentable { .. }) => {
                return msg(format!("cannot save as {}: {e}", shape.encoding.label()))
            }
            Err(e) => return msg(format!("cannot save: {e}")),
        };
        let folder = self.entry_at(folder).filter(|e| e.is_directory());
        let Some(parent) = folder.or_else(|| self.listing.cwd().cloned()) else {
            return msg("no folder open.");
        };
        if self.tl.temp.is_none() {
            self.tl.temp = tempfile::TempDir::new().ok();
        }
        let Some(dir) = self.tl.temp.as_ref().map(|t| t.path().to_path_buf()) else {
            return msg("cannot save: no temporary folder.");
        };
        let host_path = dir.join(format!("text-{}", self.queue.len()));
        if let Err(e) = std::fs::write(&host_path, &bytes) {
            return msg(format!("cannot save: {e}"));
        }
        self.stage_tool(StagedEdit::AddFile {
            parent,
            name: name.to_string(),
            host_path,
            size: bytes.len() as u64,
            prodos_type: None,
            prodos_aux: None,
            resource_fork: None,
            hfs_type_override: None,
            hfs_creator_override: None,
            dates: None,
            on_conflict: crate::fs::replace::OnConflict::Replace,
        })
    }

    fn pick_import(&mut self) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::IMPORT,
            title: "Import host file".into(),
            filters: Vec::new(),
            save: false,
            file_name: String::new(),
        })]
    }

    fn import(&mut self, path: PathBuf) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        let Some(parent) = self.listing.cwd().cloned() else {
            return msg("no folder open.");
        };
        let meta = match std::fs::metadata(&path) {
            Ok(m) if m.is_file() => m,
            _ => return msg(format!("not a file: {}", path.display())),
        };
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let host = FileEntry::new_file(name, path.display().to_string(), meta.len(), 0);
        let out = self.stage_edits(stage_host_to_image(&[host], &parent));
        if out.staged == 0 {
            return msg("nothing to import.");
        }
        let fx = std::mem::take(&mut self.out);
        let mut said = msg(format!("staged import of {}.", path.display()));
        said.extend(fx);
        said
    }

    /// Stage host files and folders into the target folder; names the volume cannot hold are listed.
    fn add_host_paths(&mut self, paths: &[PathBuf], folder: Option<&str>) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        let parent = match folder {
            Some(path) => self.entry_at(path).or_else(|| self.staged_folder(path)),
            None => self.target_folder(),
        };
        let Some(parent) = parent else {
            return msg("no folder open.");
        };
        let mut errors = Vec::new();
        let mut hosts = Vec::new();
        for path in paths {
            let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
            let full = path.display().to_string();
            match (name, std::fs::metadata(path)) {
                (Some(name), Ok(m)) if m.is_dir() => {
                    hosts.push(FileEntry::new_directory(name, full, 0))
                }
                (Some(name), Ok(m)) if m.is_file() => {
                    hosts.push(FileEntry::new_file(name, full, m.len(), 0))
                }
                (_, Err(e)) => errors.push((full, e.to_string())),
                _ => errors.push((full, "not a file or folder".to_string())),
            }
        }
        let fs_type = self.volume().fs_type.clone();
        let out = self.stage_edits(stage_host_for(&hosts, &parent, &fs_type));
        errors.extend(out.dropped.iter().cloned());
        let mut said = vec![match (out.staged, errors.len()) {
            (n, 0) => format!("staged {n} item(s)."),
            (n, bad) => format!("staged {n} item(s); {bad} could not be staged."),
        }];
        for r in &out.renamed {
            said.push(format!(
                "renamed '{}' to '{}' for this volume.",
                r.from, r.to
            ));
        }
        self.tl.staging_errors = errors;
        let mut fx: Vec<Effect> = said.into_iter().map(Effect::Message).collect();
        fx.append(&mut self.out);
        fx
    }

    /// A folder that exists only as a staged add.
    fn staged_folder(&self, path: &str) -> Option<FileEntry> {
        let parent = match path.rsplit_once('/') {
            Some(("", _)) | None => "/",
            Some((p, _)) => p,
        };
        let mut adds = self.queue.pending_adds_for(parent).into_iter();
        adds.find(|e| e.path == path && e.is_directory())
    }

    fn open_form(&mut self) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        let editable = |e: &FileEntry| {
            (e.is_file() && (e.type_code.is_some() || e.mac_dates.is_some())) || e.mode.is_some()
        };
        match self.cursor_entry() {
            Some(e) if editable(&e) => {
                self.tl.form = Some(MetadataForm {
                    initial: MetadataFields::of(&e),
                    entry: e,
                    error: None,
                });
                Vec::new()
            }
            _ => msg("metadata editing needs an HFS/HFS+ file or POSIX metadata."),
        }
    }

    fn submit_form(&mut self, fields: &MetadataFields) -> Vec<Effect> {
        let Some(form) = self.tl.form.as_mut() else {
            return Vec::new();
        };
        let edits = match fields.edits(&form.entry, &form.initial) {
            Ok(e) => e,
            Err(why) => {
                form.error = Some(why);
                return Vec::new();
            }
        };
        self.tl.form = None;
        if edits.is_empty() {
            return msg("nothing changed.");
        }
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        let mut said = String::new();
        for e in edits {
            said = self.queue.stage_metadata(e);
        }
        self.auto_apply();
        msg(said)
    }

    /// Stage a File Info metadata edit; a pane without staging writes it at once.
    pub fn stage_metadata(&mut self, edit: MetadataEdit) -> String {
        let said = self.queue.stage_metadata(edit);
        self.auto_apply();
        said
    }

    /// Installs a finished tool job; called from `tick`.
    pub(super) fn poll_tools(&mut self) -> Vec<Effect> {
        let done = match &self.tl.job {
            Some(ToolJob::Check { job, .. } | ToolJob::Repair { job, .. }) => !job.is_running(),
            Some(ToolJob::Read(job)) => !job.is_running(),
            None => false,
        };
        if !done {
            return Vec::new();
        }
        match self.tl.job.take() {
            Some(ToolJob::Check { title, job }) => {
                let lines = job.take_result().unwrap_or_default();
                self.tl.report = Some(ToolReport { title, lines });
                Vec::new()
            }
            Some(ToolJob::Repair { title, job }) => {
                let lines = match (job.snapshot().status, job.take_result()) {
                    (Status::Done, Some(lines)) => lines,
                    (Status::Failed(e), _) => vec![format!("Repair failed: {e}")],
                    _ => vec!["Repair stopped.".to_string()],
                };
                self.ed_log(format!("{title}: {}", lines[0]));
                self.tl.report = Some(ToolReport { title, lines });
                let mut fx = msg("repair finished.");
                fx.extend(self.reopen_after_apply());
                fx
            }
            Some(ToolJob::Read(job)) => match (job.snapshot().status, job.take_result()) {
                (Status::Done, Some(doc)) => {
                    self.tl.text = Some(doc);
                    Vec::new()
                }
                (Status::Failed(e), _) => msg(e),
                _ => Vec::new(),
            },
            None => Vec::new(),
        }
    }
}
