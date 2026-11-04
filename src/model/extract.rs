//! Extract one entry of a volume (a file, a folder with everything in it, or the whole volume)
//! to a host folder, through the `rb-fsexporter` crate.
//!
//! The Inspect browser's and Commander's Extract, discs included: forks per a
//! `ResourceForkMode`, ProDOS files named with the CiderPress `#TTAAAA` suffix so type and aux
//! survive a round trip, and QNX 2 text files with RS line separators turned into LF. It runs
//! as a [`super::commander_ops::HostCopyJob::Extract`] so the screen's copy job reports its
//! progress, cancels it and settles existing files. Every source, discs included, writes an
//! AppleDouble sidecar for Finder info alone, and an entry that fails asks the user (through
//! [`JobCtx::ask`], [`ASK_ENTRY_FAILED`]): skip it, skip every failure, or stop
//! (docs/phase2_commander_plan.md § "Slice 21").

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use disk_core::entry::FileEntry;
use disk_core::filesystem::Filesystem;
use fs_mac::resource_fork::{sanitize_filename, ResourceForkMode};
use rb_fsexporter::{Host, Options, Summary};

use super::commander_ops::{HostCopyStatus, StageSource};
use super::job::JobCtx;
use crate::fs::replace::OnConflict;

pub use rb_fsexporter::ErrorChoice;

/// The [`super::job::Question::kind`] of an entry that failed: subject its path, detail the error.
pub const ASK_ENTRY_FAILED: &str = "extract.entry-failed";

/// The answers to [`ASK_ENTRY_FAILED`], in the order the answer's index counts them.
pub const ENTRY_FAILED_CHOICES: [ErrorChoice; 3] =
    [ErrorChoice::Skip, ErrorChoice::SkipAll, ErrorChoice::Stop];

/// How files land on the host.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExtractOptions {
    pub fork_mode: ResourceForkMode,
    /// ProDOS only: append `#TTAAAA` to file names.
    pub prodos_suffix: bool,
    /// QNX 2 only: write text files with LF instead of RS.
    pub rs_to_lf: bool,
}

impl Default for ExtractOptions {
    fn default() -> Self {
        ExtractOptions {
            fork_mode: ResourceForkMode::AppleDouble,
            prodos_suffix: true,
            rs_to_lf: false,
        }
    }
}

impl ExtractOptions {
    /// The options as they apply to a volume of `fs_type` (the others are switched off).
    pub fn for_fs(self, fs_type: &str) -> Self {
        ExtractOptions {
            fork_mode: self.fork_mode,
            prodos_suffix: self.prodos_suffix && fs_type == "ProDOS",
            rs_to_lf: self.rs_to_lf && fs_type == "QNX 2",
        }
    }
}

/// One extraction for [`super::commander_ops::HostCopyJob::Extract`].
pub struct ExtractJob {
    pub source: StageSource,
    pub entry: FileEntry,
    pub dest: PathBuf,
    /// Already narrowed to the volume with [`ExtractOptions::for_fs`].
    pub options: ExtractOptions,
    /// Whether forks exist on this volume (HFS / HFS+).
    pub forks: bool,
    /// What to do when the entry's own output already exists.
    pub on_conflict: OnConflict,
}

/// The name an entry gets on the host.
pub fn host_name(entry: &FileEntry, options: &ExtractOptions) -> String {
    suffixed(entry, options, sanitize_filename(entry.name.as_str()))
}

fn suffixed(entry: &FileEntry, options: &ExtractOptions, base: String) -> String {
    if options.prodos_suffix && entry.is_file() {
        let tt = entry.prodos_file_type.unwrap_or(0x06);
        let aux = entry.aux_type.unwrap_or(0);
        return format!(
            "{base}{}",
            fs_apple2::prodos_types::encode_cp_suffix(tt, aux)
        );
    }
    base
}

/// The host paths the entry's own output would replace (its file and any fork sidecar, or the
/// folder; a volume root's folder is named after `volume_label`).
pub fn extraction_conflicts(
    entry: &FileEntry,
    dest: &Path,
    options: &ExtractOptions,
    forks: bool,
    volume_label: Option<&str>,
) -> Vec<PathBuf> {
    let name = match rb_fsexporter::is_volume_root(entry) {
        true => rb_fsexporter::top_name(entry, volume_label),
        false => host_name(entry, options),
    };
    rb_fsexporter::existing_outputs(entry, dest, name.as_str(), options.fork_mode, forks)
}

/// Run an extraction, counting first for the progress bar; returns the files written.
/// A failed entry is put to `ask`'s user; without one it is skipped.
pub(crate) fn run(
    job: ExtractJob,
    status: &Arc<Mutex<HostCopyStatus>>,
    ask: Option<&JobCtx>,
) -> Result<usize> {
    let ExtractJob {
        source,
        entry,
        dest,
        options,
        forks,
        on_conflict,
    } = job;
    let plan = Plan {
        dest,
        options,
        forks,
        on_conflict,
        ask,
    };
    #[cfg(feature = "optical")]
    if let Some(disc) = source.disc() {
        let path = disc.src.path.clone();
        return run_disc(disc, entry.path.as_str(), plan, status).map_err(|e| {
            let msg = crate::cli::optical_hint::with_nkit_hint(e, path.as_path());
            anyhow::anyhow!("{msg:#}")
        });
    }
    let mut fs = source.open()?;
    plan.run(fs.as_mut(), &entry, status)
}

/// A disc: its fresh filesystem carries the Mac dates, and the entry is looked up again in it.
#[cfg(feature = "optical")]
fn run_disc(
    disc: &crate::model::optical_browse::DiscChoice,
    path: &str,
    plan: Plan,
    status: &Arc<Mutex<HostCopyStatus>>,
) -> Result<usize> {
    let mut fs = disc.extract_filesystem().map_err(|e| anyhow::anyhow!(e))?;
    let entry = entry_at(fs.as_mut(), path)?;
    plan.run(fs.as_mut(), &entry, status)
}

/// An [`ExtractJob`] without its source.
struct Plan<'a> {
    dest: PathBuf,
    options: ExtractOptions,
    forks: bool,
    on_conflict: OnConflict,
    ask: Option<&'a JobCtx>,
}

impl Plan<'_> {
    fn run(
        self,
        fs: &mut dyn Filesystem,
        entry: &FileEntry,
        status: &Arc<Mutex<HostCopyStatus>>,
    ) -> Result<usize> {
        let opts = Options::new(self.options.fork_mode);
        let label = fs.volume_label().map(str::to_string);
        let conflicts = extraction_conflicts(
            entry,
            &self.dest,
            &self.options,
            self.forks,
            label.as_deref(),
        );
        if !conflicts.is_empty() && self.on_conflict == OnConflict::Skip {
            if let Ok(mut g) = status.lock() {
                g.skipped.push(format!("{} (already exists)", entry.name));
            }
            return Ok(0);
        }
        let (files, bytes) = rb_fsexporter::count(fs, entry)?;
        if let Ok(mut g) = status.lock() {
            g.files_total = files;
            g.bytes_total = bytes;
        }
        let mut host = StatusHost {
            status,
            options: self.options,
            asker: ErrorAsker::new(self.ask, status),
        };
        match rb_fsexporter::extract(fs, entry, &self.dest, &opts, &mut host) {
            Ok(done) => Ok(done.files),
            // The user chose Stop: the run ends with what it wrote, not as a failure.
            Err(_) if host.asker.stopped() => Ok(crate::model::worker::lock_status(status).copied),
            Err(e) => Err(e),
        }
    }
}

/// The entry at `path`, found by listing each folder on the way from the root.
#[cfg(feature = "optical")]
fn entry_at(fs: &mut dyn Filesystem, path: &str) -> Result<FileEntry> {
    let mut cur = fs.root()?;
    if path == "/" || path.is_empty() {
        return Ok(cur);
    }
    loop {
        let mut next = None;
        for k in fs.list_directory(&cur)? {
            if k.path == path {
                return Ok(k);
            }
            let ancestor = k.is_directory()
                && path.len() > k.path.len()
                && path.starts_with(k.path.as_str())
                && path.as_bytes().get(k.path.len()) == Some(&b'/');
            if ancestor && next.is_none() {
                next = Some(k);
            }
        }
        match next {
            Some(dir) => cur = dir,
            None => anyhow::bail!("{path} is not on the disc"),
        }
    }
}

/// The copy job's status as the extractor's host.
struct StatusHost<'a> {
    status: &'a Arc<Mutex<HostCopyStatus>>,
    options: ExtractOptions,
    asker: ErrorAsker<'a>,
}

/// Puts an entry that failed to the user through the copy's job, and remembers "Continue with
/// all errors" for the rest of the run (across top-level entries, which the crate can't).
pub(crate) struct ErrorAsker<'a> {
    ctx: Option<&'a JobCtx>,
    status: &'a Arc<Mutex<HostCopyStatus>>,
    skip_all: bool,
}

impl<'a> ErrorAsker<'a> {
    pub(crate) fn new(ctx: Option<&'a JobCtx>, status: &'a Arc<Mutex<HostCopyStatus>>) -> Self {
        ErrorAsker {
            ctx,
            status,
            skip_all: false,
        }
    }

    /// The user's answer for the entry at `path`; `None` with no job to ask through.
    pub(crate) fn decide(&mut self, path: &str, err: &anyhow::Error) -> Option<ErrorChoice> {
        let ctx = self.ctx?;
        if self.skip_all {
            return Some(ErrorChoice::SkipAll);
        }
        let answer = ctx.ask(ASK_ENTRY_FAILED, path, format!("{err:#}"));
        // A cancel while asking stops too; the job then ends cancelled.
        let choice = answer
            .and_then(|i| ENTRY_FAILED_CHOICES.get(i).copied())
            .unwrap_or(ErrorChoice::Stop);
        match choice {
            ErrorChoice::SkipAll => self.skip_all = true,
            ErrorChoice::Stop if answer.is_some() => {
                crate::model::worker::lock_status(self.status).stopped =
                    Some(format!("{path} ({err:#})"));
            }
            _ => {}
        }
        Some(choice)
    }

    /// Record the entry at `path` as skipped, for the end-of-run list.
    pub(crate) fn skipped(&self, path: &str, err: &anyhow::Error) {
        log::warn!("Skipped {path}: {err:#}");
        crate::model::worker::lock_status(self.status)
            .skipped
            .push(format!("{path} ({err:#})"));
    }

    /// Whether the user chose Stop.
    pub(crate) fn stopped(&self) -> bool {
        crate::model::worker::lock_status(self.status)
            .stopped
            .is_some()
    }
}

impl Host for StatusHost<'_> {
    fn cancelled(&self) -> bool {
        self.status
            .lock()
            .map(|g| g.cancel_requested)
            .unwrap_or(false)
    }

    fn started(&mut self, entry: &FileEntry) {
        if let Ok(mut g) = self.status.lock() {
            g.current_file = entry.path.clone();
        }
    }

    fn wrote(&mut self, _entry: &FileEntry, bytes: u64, total: &Summary) {
        if let Ok(mut g) = self.status.lock() {
            g.copied = total.files;
            g.bytes_done += bytes;
        }
    }

    fn on_error(&mut self, entry: &FileEntry, err: &anyhow::Error) -> Option<ErrorChoice> {
        self.asker.decide(entry.path.as_str(), err)
    }

    // Skipped by the user's answer, or (with no one to ask) an unreadable entry skipped.
    fn failed(&mut self, entry: &FileEntry, err: &anyhow::Error) {
        self.asker.skipped(entry.path.as_str(), err);
    }

    fn host_name(&self, entry: &FileEntry, base: String) -> String {
        suffixed(entry, &self.options, base)
    }

    fn write_file(
        &mut self,
        fs: &mut dyn Filesystem,
        entry: &FileEntry,
        dir: &Path,
        name: &str,
    ) -> Option<Result<u64>> {
        if !self.options.rs_to_lf || entry.size > 16 << 20 {
            return None;
        }
        let data = match fs.read_file(entry, usize::MAX) {
            Ok(d) => d,
            Err(e) => return Some(Err(e.into())),
        };
        if !crate::fs::qnx2::looks_like_rs_text(&data) {
            return None;
        }
        let out = std::fs::write(dir.join(name), crate::fs::qnx2::rs_to_lf(&data));
        Some(out.map(|_| data.len() as u64).map_err(anyhow::Error::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prodos_names_carry_type_and_aux_and_options_narrow_to_the_volume() {
        let mut e = FileEntry::new_file("PROG".into(), "/PROG".into(), 1, 0);
        e.prodos_file_type = Some(0x06);
        e.aux_type = Some(0x0800);
        let opts = ExtractOptions {
            rs_to_lf: true,
            ..ExtractOptions::default()
        };
        let prodos = opts.for_fs("ProDOS");
        assert!(prodos.prodos_suffix && !prodos.rs_to_lf);
        assert_eq!(host_name(&e, &prodos), "PROG#060800");
        let fat = opts.for_fs("FAT16");
        assert_eq!(host_name(&e, &fat), "PROG");
        assert!(opts.for_fs("QNX 2").rs_to_lf);
    }
}
