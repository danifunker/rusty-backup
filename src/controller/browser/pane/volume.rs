//! Whole-volume read tools of the pane: check results as plain data, the journal, the tree listing
//! and boot blocks.
//!
//! Check, journal and tree jobs borrow the reader the combined tree keeps for its folder listings
//! (or open one from the pane's source) and hand it back, so a large catalog is read once. A check
//! or repair result becomes a [`CheckSummary`] / [`RepairSummary`] beside the report's lines, which
//! is what a view needs for a richer window (debug toggle, orphans by parent). Boot blocks are an
//! HFS concern: whether the volume has them is probed in a job, and a donor's are read in a job
//! and staged as `WriteBootBlocks`.

use std::any::Any;
use std::path::PathBuf;

use disk_core::filesystem::Filesystem;

use super::source::purpose;
use super::BrowserPaneController;
use crate::controller::{Effect, PickSpec};
use crate::fs::{FsckResult, RepairReport};
use crate::model::commander_ops::StageSource;
use crate::model::edit_queue::StagedEdit;
use crate::model::job::{Job, Status};

pub use crate::model::mac_archive_read::{ArchiveRead, ArchiveUse};
pub use arc_mac::detect::MacArchiveKind;
pub use fs_mac::hfsplus_journal::JournalDetail;

/// The metadata structure a journal block's byte offset falls in.
pub fn journal_block_class(detail: &JournalDetail, target: u64) -> &'static str {
    fs_mac::hfsplus_journal::classify_target(detail.regions.as_slice(), target)
}

/// Which subsystems a journal transaction's blocks touch, as one line ("empty" for none).
pub fn journal_txn_classes(
    detail: &JournalDetail,
    txn: &fs_mac::hfsplus_journal::JournalTxnDetail,
) -> String {
    let mut names: Vec<&'static str> = Vec::new();
    for b in &txn.blocks {
        let c = journal_block_class(detail, b.target);
        if !names.contains(&c) {
            names.push(c);
        }
    }
    if names.is_empty() {
        "empty".to_string()
    } else {
        names.join(", ")
    }
}

use crate::model::mac_archive_read;
use disk_core::entry::FileEntry;

/// What a check found, drawn as a window or printed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckSummary {
    pub clean: bool,
    /// "Files: n  Directories: n" plus each filesystem-specific statistic.
    pub stats_line: String,
    /// `[code] message` per error.
    pub errors: Vec<String>,
    /// Each warning's text and whether it is debug-level (hidden unless asked for).
    pub warnings: Vec<(String, bool)>,
    /// Orphans grouped by the missing parent's id: `kind "name" (ID n)` each.
    pub orphans: Vec<(u64, Vec<String>)>,
    pub repairable: bool,
    pub repairable_count: usize,
}

impl CheckSummary {
    pub(super) fn of(r: &FsckResult) -> Self {
        let mut stats_line = format!(
            "Files: {}  Directories: {}",
            r.stats.files_checked, r.stats.directories_checked
        );
        for (label, value) in &r.stats.extra {
            stats_line.push_str(&format!("  {label}: {value}"));
        }
        let mut orphans: Vec<(u64, Vec<String>)> = Vec::new();
        for o in &r.orphaned_entries {
            let kind = if o.is_directory { "dir" } else { "file" };
            let line = format!("{kind} \"{}\" (ID {})", o.name, o.id);
            match orphans.iter_mut().find(|(p, _)| *p == o.missing_parent_id) {
                Some((_, lines)) => lines.push(line),
                None => orphans.push((o.missing_parent_id, vec![line])),
            }
        }
        orphans.sort_by_key(|(p, _)| *p);
        let warning = |w: &disk_core::fsck::FsckIssue| match w.debug {
            true => (format!("[DEBUG] {}", w.message), true),
            false => (format!("[{}] {}", w.code, w.message), false),
        };
        CheckSummary {
            clean: r.is_clean(),
            stats_line,
            errors: r
                .errors
                .iter()
                .map(|e| format!("[{}] {}", e.code, e.message))
                .collect(),
            warnings: r.warnings.iter().map(warning).collect(),
            orphans,
            repairable: r.repairable,
            repairable_count: r.errors.iter().filter(|e| e.repairable).count(),
        }
    }
}

/// What a repair did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairSummary {
    pub applied: Vec<String>,
    pub failed: Vec<String>,
    pub unrepairable: usize,
}

impl RepairSummary {
    pub(super) fn of(r: &RepairReport) -> Self {
        RepairSummary {
            applied: r.fixes_applied.clone(),
            failed: r.fixes_failed.clone(),
            unrepairable: r.unrepairable_count,
        }
    }
}

/// The volume's tree as text, and whether it carries filesystem ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeText {
    pub text: String,
    pub ids: bool,
}

/// What a reading tool found; the reader rides back with it.
pub(super) enum Found {
    Check(Box<Option<Result<FsckResult, String>>>),
    Tree(TreeText),
    Journal(Option<Box<JournalDetail>>),
    Sniffed(String, Option<MacArchiveKind>),
    Archive(Box<(FileEntry, ArchiveUse, Result<ArchiveRead, String>)>),
}

pub(super) struct ReadOut {
    reader: Option<Box<dyn Filesystem>>,
    generation: u64,
    pub(super) found: Result<Found, String>,
}

/// The volume's reader, or one opened from `source`.
fn reader_for(
    source: StageSource,
    reader: &mut Option<Box<dyn Filesystem>>,
) -> Result<&mut Box<dyn Filesystem>, String> {
    if reader.is_none() {
        *reader = Some(source.open().map_err(|e| format!("{e:#}"))?);
    }
    reader.as_mut().ok_or_else(|| "no reader".to_string())
}

fn journal_of(any: Box<dyn Any + Send>) -> Result<JournalDetail, String> {
    match any.downcast::<JournalDetail>() {
        Ok(d) => Ok(*d),
        Err(_) => Err("unsupported journal format.".to_string()),
    }
}

/// Run one reading tool on the volume.
fn read_volume(
    source: StageSource,
    reader: &mut Option<Box<dyn Filesystem>>,
    what: &Want,
) -> Result<Found, String> {
    // A disc's tree is the Optical tab's listing (type / creator, resource forks), not the generic one.
    #[cfg(feature = "optical")]
    if let (Want::Tree(_), Some(disc)) = (what, source.disc()) {
        let text = disc.tree_listing()?;
        return Ok(Found::Tree(TreeText { text, ids: false }));
    }
    let fs = reader_for(source, reader)?;
    Ok(match what {
        Want::Check => Found::Check(Box::new(fs.fsck().map(|r| r.map_err(|e| e.to_string())))),
        Want::Tree(ids) => {
            let text = if *ids {
                crate::fs::tree::format_tree_with_ids(fs.as_mut())
            } else {
                crate::fs::tree::format_tree(fs.as_mut())
            };
            let text = text.map_err(|e| format!("cannot list the tree: {e}"))?;
            Found::Tree(TreeText { text, ids: *ids })
        }
        Want::Sniff(entry) => Found::Sniffed(
            entry.path.clone(),
            mac_archive_read::sniff(fs.as_mut(), entry),
        ),
        Want::Archive(entry, purpose) => {
            let read = mac_archive_read::read(fs.as_mut(), entry, *purpose);
            Found::Archive(Box::new((entry.clone(), *purpose, read)))
        }
        Want::Journal => match fs.journal_detail() {
            Ok(Some(any)) => Found::Journal(Some(Box::new(journal_of(any)?))),
            Ok(None) => Found::Journal(None),
            Err(e) => return Err(format!("cannot read the journal: {e}")),
        },
    })
}

pub(super) enum Want {
    Check,
    Tree(bool),
    Journal,
    Sniff(FileEntry),
    Archive(FileEntry, ArchiveUse),
}

#[derive(Default)]
pub(super) struct VolumeState {
    tree: Option<TreeText>,
    /// The last tree listing's text, kept for `SaveTree`.
    last_tree: Option<String>,
    journal: Option<JournalDetail>,
    boot_probe: Option<Job<Option<bool>>>,
    /// Whether the open volume has HFS boot blocks; `None` until probed.
    boot_blocks: Option<Option<bool>>,
    donor: Option<Job<Box<[u8; 1024]>>>,
    /// The Mac-archive sniff in flight (its own slot: it follows the cursor and must not block Apply).
    sniff: Option<Job<ReadOut>>,
    /// The last sniff's verdict, by path.
    sniffed: Option<(String, Option<MacArchiveKind>)>,
}

impl BrowserPaneController {
    /// The last tree listing, handed over once (the view keeps the text).
    pub fn take_tree_text(&mut self) -> Option<TreeText> {
        self.vol.tree.take()
    }

    /// The HFS+ journal read by `ShowJournal`, handed over once.
    pub fn take_journal(&mut self) -> Option<JournalDetail> {
        self.vol.journal.take()
    }

    /// Whether the volume has HFS boot blocks (`None`: not known yet, or not an HFS volume on a file).
    pub fn boot_blocks_present(&self) -> Option<bool> {
        self.vol.boot_blocks.flatten()
    }

    /// True while a donor read or the boot-block probe runs (they do not block Apply).
    pub fn volume_probing(&self) -> bool {
        self.vol.donor.is_some() || self.vol.boot_probe.is_some() || self.vol.sniff.is_some()
    }

    /// Whether the file at `path` is a Mac archive: `None` until `SniffArchive` for it has landed.
    pub fn archive_sniff(&self, path: &str) -> Option<Option<MacArchiveKind>> {
        let sniffed = self.vol.sniffed.as_ref().filter(|(p, _)| p == path);
        sniffed.map(|(_, kind)| *kind)
    }

    /// True while a sniff runs (the view asks again for its path once it lands).
    pub fn is_sniffing(&self) -> bool {
        self.vol.sniff.is_some()
    }

    /// Sniff the file at `path` for a Mac archive, unless known or already sniffing.
    pub(super) fn sniff_archive(&mut self, path: &str) -> Vec<Effect> {
        if self.archive_sniff(path).is_some() || self.vol.sniff.is_some() {
            return Vec::new();
        }
        let Some(entry) = self.entry_at(path).filter(|e| e.is_file()) else {
            self.vol.sniffed = Some((path.to_string(), None));
            return Vec::new();
        };
        match self.spawn_read("Checking for an archive", Want::Sniff(entry)) {
            Ok(job) => self.vol.sniff = Some(job),
            // The source is busy; a failed sniff reads as "not an archive" rather than retrying.
            Err(_) => self.vol.sniffed = Some((path.to_string(), None)),
        }
        Vec::new()
    }

    /// Spawn a reading tool on the shared reader.
    pub(super) fn spawn_read(&mut self, label: &str, what: Want) -> Result<Job<ReadOut>, String> {
        let Some(source) = self.local_stage_source() else {
            return Err("no volume is open.".to_string());
        };
        let (mut reader, generation) = self.take_reader();
        let resources = self.apply_resources();
        let (jobs, notify) = self.job_services();
        jobs.spawn(label.to_string(), resources, notify, move |ctx| {
            ctx.set_can_cancel(false);
            let found = read_volume(source, &mut reader, &what);
            Ok(ReadOut {
                reader,
                generation,
                found,
            })
        })
        .map_err(|busy| busy.to_string())
    }

    /// Hand a finished read's reader back and return what it found.
    pub(super) fn read_done(&mut self, job: Job<ReadOut>) -> Result<Found, String> {
        match (job.snapshot().status, job.take_result()) {
            (Status::Done, Some(out)) => {
                self.give_reader(out.reader, out.generation);
                out.found
            }
            (Status::Failed(e), _) => Err(e),
            _ => Err("stopped.".to_string()),
        }
    }

    /// The last tree listing's text.
    pub fn last_tree(&self) -> Option<&str> {
        self.vol.last_tree.as_deref()
    }

    pub(super) fn found_tree(&mut self, tree: TreeText) {
        self.vol.last_tree = Some(tree.text.clone());
        self.vol.tree = Some(tree);
    }

    pub(super) fn found_journal(&mut self, journal: Option<Box<JournalDetail>>) -> Vec<Effect> {
        match journal {
            Some(j) => {
                self.vol.journal = Some(*j);
                Vec::new()
            }
            None => msg("this volume is not journaled."),
        }
    }

    /// Ask for a bootable donor disk whose boot blocks to stage.
    pub(super) fn pick_boot_donor(&mut self) -> Vec<Effect> {
        if let Err(fx) = self.writable_volume() {
            return fx;
        }
        let ext = ["dsk", "hfv", "img", "hda", "raw"];
        vec![Effect::PickFile(PickSpec {
            purpose: purpose::BOOT_DONOR,
            title: "Select a bootable donor disk to copy boot blocks from".into(),
            filters: vec![(
                "Disk images".into(),
                ext.iter().map(|e| e.to_string()).collect(),
            )],
            save: false,
            file_name: String::new(),
        })]
    }

    /// Read the donor's boot blocks in a job; they are staged when it lands.
    pub(super) fn read_boot_donor(&mut self, donor: PathBuf) -> Vec<Effect> {
        if self.vol.donor.is_some() {
            return msg("still reading the last donor.");
        }
        let (jobs, notify) = self.job_services();
        let label = "Reading boot blocks";
        let spawned = jobs.spawn(label, Vec::new(), notify, move |_| {
            let d = crate::fs::hfs_boot::read_donor_boot_blocks_from_image(&donor)?;
            Ok(d.blocks)
        });
        match spawned {
            Ok(job) => {
                self.vol.donor = Some(job);
                Vec::new()
            }
            Err(busy) => msg(format!("cannot read the donor: {busy}")),
        }
    }

    /// Land a donor read and the boot-block probe; start the probe on an HFS volume.
    pub(super) fn poll_volume(&mut self) -> Vec<Effect> {
        let mut fx = Vec::new();
        if self.vol.donor.as_ref().is_some_and(|j| !j.is_running()) {
            let job = self.vol.donor.take().expect("checked");
            match (job.snapshot().status, job.take_result()) {
                (Status::Done, Some(blocks)) => {
                    fx.extend(self.stage_tool(StagedEdit::WriteBootBlocks { blocks }));
                }
                (Status::Failed(e), _) => fx.extend(msg(format!("boot blocks: {e}"))),
                _ => {}
            }
        }
        if self.vol.sniff.as_ref().is_some_and(|j| !j.is_running()) {
            let job = self.vol.sniff.take().expect("checked");
            if let Ok(Found::Sniffed(path, kind)) = self.read_done(job) {
                self.vol.sniffed = Some((path, kind));
            }
        }
        if self
            .vol
            .boot_probe
            .as_ref()
            .is_some_and(|j| !j.is_running())
        {
            let job = self.vol.boot_probe.take().expect("checked");
            self.vol.boot_blocks = Some(job.take_result().flatten());
        }
        self.probe_boot_blocks();
        fx
    }

    /// Forget what was probed about the volume (it was reopened or written).
    pub(super) fn volume_changed(&mut self) {
        self.vol.boot_blocks = None;
        self.vol.boot_probe = None;
        self.vol.sniffed = None;
    }

    fn probe_boot_blocks(&mut self) {
        let hfs = self.volume().fs_type == "HFS";
        if !hfs || self.vol.boot_blocks.is_some() || self.vol.boot_probe.is_some() {
            return;
        }
        let Some(session) = self.session() else {
            return;
        };
        let (Some(path), offset) = (session.source_path.clone(), session.partition_offset) else {
            self.vol.boot_blocks = Some(None);
            return;
        };
        let (jobs, notify) = self.job_services();
        let spawned = jobs.spawn("Boot blocks", Vec::new(), notify, move |_| {
            let mut f = std::fs::File::open(&path)?;
            Ok(fs_mac::hfs_boot::has_boot_blocks(&mut f, offset).ok())
        });
        if let Ok(job) = spawned {
            self.vol.boot_probe = Some(job);
        }
    }
}

fn msg(text: impl Into<String>) -> Vec<Effect> {
    vec![Effect::Message(text.into())]
}
