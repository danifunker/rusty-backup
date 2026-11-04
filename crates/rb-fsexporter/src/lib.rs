//! Extract a tree from any `disk-core` filesystem to host files.
//!
//! - [`extract`] writes one entry into a host folder: a file, a folder with everything in it,
//!   or a whole volume (a folder named after the volume, or its contents straight into the
//!   destination, per [`Options::volume_folder`]).
//! - [`extract_contents`] writes a folder's children into the destination.
//! - [`count`] totals what an extraction will write, for a progress bar; [`existing_outputs`]
//!   lists the host paths an entry's own output would replace, so a caller can ask first.
//!
//! Files go through `fs_mac::fork_export`, so a resource fork lands as AppleDouble, MacBinary,
//! BinHex, a `.rsrc` sidecar or the macOS named fork ([`ResourceForkMode`]); the encoders live
//! in `fs-mac`, none here. Symlinks become host symlinks, or a small text file holding the
//! target (`target\n`) where the host refuses one. [`Options`] also carries an existing-file
//! policy, case-collision handling for case-insensitive hosts, POSIX modes and an error policy.
//!
//! Progress, cancellation and the per-entry reports go through the [`Host`] trait, which also
//! lets a caller rename files (e.g. a ProDOS `#TTAAAA` suffix) or write one itself (a text
//! conversion, per-file compression). The crate holds no UI or application types.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use disk_core::entry::{EntryType, FileEntry};
use disk_core::filesystem::Filesystem;
use fs_mac::fork_export::{self, ForkExportOptions};

pub use fs_mac::resource_fork::{sanitize_filename, ResourceForkMode};

/// What a symlink on the source becomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Symlinks {
    /// A host symlink; a text file holding the target where the host refuses one.
    Create,
    /// Nothing.
    Skip,
}

/// What an entry that fails to extract does to the rest of the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnError {
    /// Report it through [`Host::failed`] and carry on with its siblings.
    SkipEntry,
    /// Stop the extraction with the error.
    Stop,
}

/// What a file whose output already exists on the host does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Existing {
    Overwrite,
    /// Leave it, report it through [`Host::exists`].
    Skip,
    /// Stop with an error.
    Fail,
}

/// How two names that differ only by case are told apart on a case-insensitive host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseCollision {
    /// The later name gets a `~N` suffix.
    Rename,
    /// The later entry is skipped.
    Skip,
    /// The later entry is refused (reported as [`Collision::Refused`]).
    Fail,
}

/// What [`Host::collision`] is told about a name that collided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Collision<'a> {
    Renamed(&'a str),
    Skipped,
    Refused,
}

/// How an extraction writes to the host.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Options {
    pub fork_mode: ResourceForkMode,
    /// AppleDouble: a `._name` sidecar for type / creator codes alone (no resource fork).
    pub finder_info_sidecar: bool,
    pub symlinks: Symlinks,
    pub on_error: OnError,
    pub existing: Existing,
    /// `None` writes names verbatim (a case-sensitive host, or the caller doesn't care).
    pub case_collision: Option<CaseCollision>,
    /// Enter folders below the starting point.
    pub recursive: bool,
    /// Apply each entry's POSIX permission bits (Unix hosts only).
    pub posix_modes: bool,
    /// A volume root becomes a folder named after the volume; otherwise its contents land in
    /// the destination itself.
    pub volume_folder: bool,
}

impl Options {
    /// Every choice at its default: forks per `fork_mode`, symlinks created, a bad entry
    /// skipped, existing files overwritten, names verbatim, recursive, a volume folder.
    pub fn new(fork_mode: ResourceForkMode) -> Self {
        Options {
            fork_mode,
            finder_info_sidecar: true,
            symlinks: Symlinks::Create,
            on_error: OnError::SkipEntry,
            existing: Existing::Overwrite,
            case_collision: None,
            recursive: true,
            posix_modes: false,
            volume_folder: true,
        }
    }
}

/// The error an extraction stops with when [`Host::cancelled`] says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cancelled;

impl fmt::Display for Cancelled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Extraction cancelled")
    }
}

impl std::error::Error for Cancelled {}

/// What an extraction wrote: files (symlinks included) and the bytes read for them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Summary {
    pub files: usize,
    pub bytes: u64,
}

/// The caller's side of an extraction. Every method has a do-nothing default.
pub trait Host {
    /// Checked before each entry; `true` stops the run with [`Cancelled`].
    fn cancelled(&self) -> bool {
        false
    }
    /// A file or symlink is about to be written.
    fn started(&mut self, _entry: &FileEntry) {}
    /// A file or symlink was written: `bytes` for it, `total` so far.
    fn wrote(&mut self, _entry: &FileEntry, _bytes: u64, _total: &Summary) {}
    /// An entry failed and was skipped ([`OnError::SkipEntry`]).
    fn failed(&mut self, _entry: &FileEntry, _err: &anyhow::Error) {}
    /// A file was left because its output exists ([`Existing::Skip`]).
    fn exists(&mut self, _path: &Path) {}
    /// A name collided by case on a case-insensitive host.
    fn collision(&mut self, _name: &str, _outcome: Collision<'_>) {}
    /// The host name for `entry`; `base` is its sanitised name.
    fn host_name(&self, _entry: &FileEntry, base: String) -> String {
        base
    }
    /// Write a file yourself (into `dir` as `name`) and return the bytes read, or `None` for
    /// the fork writer.
    fn write_file(
        &mut self,
        _fs: &mut dyn Filesystem,
        _entry: &FileEntry,
        _dir: &Path,
        _name: &str,
    ) -> Option<Result<u64>> {
        None
    }
}

/// A [`Host`] that reports nothing and never cancels.
pub struct NoHost;

impl Host for NoHost {}

/// Whether `entry` is a volume root (a folder with no name of its own).
pub fn is_volume_root(entry: &FileEntry) -> bool {
    entry.is_directory() && entry.archive_name().is_none()
}

/// The host folder name for a volume labelled `label`, or `None` when it has no usable name.
pub fn volume_folder_name(label: Option<&str>) -> Option<String> {
    let trimmed = label?.trim_matches(|c: char| c.is_whitespace() || c == '\0');
    let trimmed = trimmed.trim_end_matches('.');
    match trimmed.is_empty() {
        true => None,
        false => Some(sanitize_filename(trimmed)),
    }
}

/// The name `entry` takes in the destination when it is the one extracted: a volume root is
/// named after `volume_label` (`_` without one), anything else after itself.
pub fn top_name(entry: &FileEntry, volume_label: Option<&str>) -> String {
    if is_volume_root(entry) {
        if let Some(name) = volume_folder_name(volume_label) {
            return name;
        }
    }
    sanitize_filename(entry.name.as_str())
}

/// The host paths that already exist and that `entry`'s own output (named `name` in `dest`)
/// would replace: the file and its fork sidecar, or the folder.
pub fn existing_outputs(
    entry: &FileEntry,
    dest: &Path,
    name: &str,
    mode: ResourceForkMode,
    forks: bool,
) -> Vec<PathBuf> {
    let has_rsrc = forks && entry.is_file() && entry.resource_fork_size.is_some_and(|s| s > 0);
    let mut out = Vec::new();
    match mode {
        ResourceForkMode::MacBinary if has_rsrc => out.push(dest.join(format!("{name}.bin"))),
        // BinHex packs both forks and the Finder info into one file, fork or not.
        ResourceForkMode::BinHex if entry.is_file() => out.push(dest.join(format!("{name}.hqx"))),
        mode => {
            out.push(dest.join(name));
            match mode {
                ResourceForkMode::AppleDouble if has_rsrc => {
                    out.push(dest.join(format!("._{name}")))
                }
                ResourceForkMode::SeparateRsrc if has_rsrc => {
                    out.push(dest.join(format!("{name}.rsrc")))
                }
                _ => {}
            }
        }
    }
    out.into_iter()
        .filter(|p| p.symlink_metadata().is_ok())
        .collect()
}

/// Files (symlinks included) and bytes (both forks) below and including `entry`.
pub fn count(fs: &mut dyn Filesystem, entry: &FileEntry) -> Result<(usize, u64)> {
    Ok(match entry.entry_type {
        EntryType::File => (1, entry.size + entry.resource_fork_size.unwrap_or(0)),
        EntryType::Symlink => (1, 0),
        EntryType::Special => (0, 0),
        EntryType::Directory => {
            let mut total = (0, 0);
            for child in fs.list_directory(entry)? {
                let (f, b) = count(fs, &child)?;
                total = (total.0 + f, total.1 + b);
            }
            total
        }
    })
}

/// Extract `entry` into the host folder `dest`. An error in the entry itself is returned;
/// one below it follows [`Options::on_error`].
pub fn extract(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    dest: &Path,
    opts: &Options,
    host: &mut dyn Host,
) -> Result<Summary> {
    let mut w = Walker {
        fs,
        opts,
        host,
        summary: Summary::default(),
    };
    if is_volume_root(entry) {
        if !opts.volume_folder {
            w.children(entry, dest)?;
            return Ok(w.summary);
        }
        let name = top_name(entry, w.fs.volume_label());
        w.cancel_point()?;
        w.dir(entry, dest, name.as_str())?;
        return Ok(w.summary);
    }
    let mut used = HashSet::new();
    w.entry(entry, dest, &mut used, true)?;
    Ok(w.summary)
}

/// Extract the children of the folder `dir` into the host folder `dest`.
pub fn extract_contents(
    fs: &mut dyn Filesystem,
    dir: &FileEntry,
    dest: &Path,
    opts: &Options,
    host: &mut dyn Host,
) -> Result<Summary> {
    let mut w = Walker {
        fs,
        opts,
        host,
        summary: Summary::default(),
    };
    w.children(dir, dest)?;
    Ok(w.summary)
}

struct Walker<'a> {
    fs: &'a mut dyn Filesystem,
    opts: &'a Options,
    host: &'a mut dyn Host,
    summary: Summary,
}

impl Walker<'_> {
    fn cancel_point(&self) -> Result<()> {
        match self.host.cancelled() {
            true => Err(Cancelled.into()),
            false => Ok(()),
        }
    }

    fn children(&mut self, dir: &FileEntry, host_dir: &Path) -> Result<()> {
        let kids = self.fs.list_directory(dir)?;
        // Each folder is its own case-collision namespace.
        let mut used = HashSet::new();
        for child in &kids {
            match self.entry(child, host_dir, &mut used, false) {
                Ok(()) => {}
                Err(e) if self.opts.on_error == OnError::Stop => return Err(e),
                Err(e) if e.is::<Cancelled>() || self.host.cancelled() => return Err(e),
                Err(e) => self.host.failed(child, &e),
            }
        }
        Ok(())
    }

    fn entry(
        &mut self,
        entry: &FileEntry,
        dest: &Path,
        used: &mut HashSet<String>,
        top: bool,
    ) -> Result<()> {
        self.cancel_point()?;
        let base = sanitize_filename(entry.name.as_str());
        let name = self.host.host_name(entry, base);
        let Some(name) = self.resolve_case(name, used) else {
            return Ok(());
        };
        match entry.entry_type {
            EntryType::File => self.file(entry, dest, name.as_str()),
            // A nameless folder (".") merges into its parent, as a volume root would.
            EntryType::Directory if is_volume_root(entry) && !self.opts.volume_folder => {
                self.children(entry, dest)
            }
            EntryType::Directory if top || self.opts.recursive => {
                self.dir(entry, dest, name.as_str())
            }
            EntryType::Directory => Ok(()),
            EntryType::Symlink => self.symlink(entry, dest, name.as_str()),
            EntryType::Special => Ok(()),
        }
    }

    fn resolve_case(&mut self, name: String, used: &mut HashSet<String>) -> Option<String> {
        let Some(policy) = self.opts.case_collision else {
            return Some(name);
        };
        let key = name.to_lowercase();
        if used.insert(key) {
            return Some(name);
        }
        match policy {
            CaseCollision::Fail => {
                self.host.collision(name.as_str(), Collision::Refused);
                None
            }
            CaseCollision::Skip => {
                self.host.collision(name.as_str(), Collision::Skipped);
                None
            }
            CaseCollision::Rename => {
                for n in 1..10_000u32 {
                    let candidate = format!("{name}~{n}");
                    if used.insert(candidate.to_lowercase()) {
                        self.host
                            .collision(name.as_str(), Collision::Renamed(candidate.as_str()));
                        return Some(candidate);
                    }
                }
                self.host.collision(name.as_str(), Collision::Skipped);
                None
            }
        }
    }

    fn dir(&mut self, entry: &FileEntry, dest: &Path, name: &str) -> Result<()> {
        let path = dest.join(name);
        std::fs::create_dir_all(&path).with_context(|| format!("creating {}", path.display()))?;
        if self.opts.posix_modes {
            apply_mode(path.as_path(), entry);
        }
        self.children(entry, path.as_path())
    }

    /// Whether to write over `path`; `false` leaves it.
    fn settle_existing(&mut self, path: &Path) -> Result<bool> {
        if self.opts.existing == Existing::Overwrite || path.symlink_metadata().is_err() {
            return Ok(true);
        }
        match self.opts.existing {
            Existing::Skip => {
                self.host.exists(path);
                Ok(false)
            }
            _ => bail!("{} already exists", path.display()),
        }
    }

    fn file(&mut self, entry: &FileEntry, dest: &Path, name: &str) -> Result<()> {
        let mode = self.opts.fork_mode;
        let target = fork_export::primary_output_path(entry, dest, name, mode);
        if !self.settle_existing(target.as_path())? {
            return Ok(());
        }
        self.host.started(entry);
        let bytes = match self.host.write_file(&mut *self.fs, entry, dest, name) {
            Some(r) => r?,
            None => {
                let fork = ForkExportOptions {
                    finder_info_sidecar: self.opts.finder_info_sidecar,
                };
                fork_export::export_file_with_fork_opts(self.fs, entry, dest, name, mode, fork)?
            }
        };
        if self.opts.posix_modes {
            apply_mode(dest.join(name).as_path(), entry);
        }
        self.done(entry, bytes);
        Ok(())
    }

    fn symlink(&mut self, entry: &FileEntry, dest: &Path, name: &str) -> Result<()> {
        if self.opts.symlinks == Symlinks::Skip {
            return Ok(());
        }
        let path = dest.join(name);
        if !self.settle_existing(path.as_path())? {
            return Ok(());
        }
        if path.symlink_metadata().is_ok_and(|m| !m.is_dir()) {
            std::fs::remove_file(&path).with_context(|| format!("replacing {}", path.display()))?;
        }
        self.host.started(entry);
        let target = entry.symlink_target.as_deref().unwrap_or("");
        write_symlink(target, path.as_path())?;
        self.done(entry, 0);
        Ok(())
    }

    fn done(&mut self, entry: &FileEntry, bytes: u64) {
        self.summary.files += 1;
        self.summary.bytes += bytes;
        self.host.wrote(entry, bytes, &self.summary);
    }
}

/// A symlink at `link` pointing to `target`, or a text file holding the target if the host
/// refuses the link.
pub fn write_symlink(target: &str, link: &Path) -> Result<()> {
    if !target.is_empty() && host_symlink(target, link).is_ok() {
        return Ok(());
    }
    let body = match target.is_empty() {
        true => String::new(),
        false => format!("{target}\n"),
    };
    std::fs::write(link, body.as_bytes()).with_context(|| format!("writing {}", link.display()))
}

#[cfg(unix)]
fn host_symlink(target: &str, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn host_symlink(target: &str, link: &Path) -> std::io::Result<()> {
    let target = target.replace('/', "\\");
    let to_dir = link
        .parent()
        .map(|p| p.join(target.as_str()).is_dir())
        .unwrap_or(false);
    match to_dir {
        true => std::os::windows::fs::symlink_dir(target.as_str(), link),
        false => std::os::windows::fs::symlink_file(target.as_str(), link),
    }
}

#[cfg(not(any(unix, windows)))]
fn host_symlink(_target: &str, _link: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "no symlinks on this host",
    ))
}

#[cfg(unix)]
fn apply_mode(path: &Path, entry: &FileEntry) {
    use std::os::unix::fs::PermissionsExt;
    if let Some(mode) = entry.mode {
        // Best effort, as `cp -p`: a host that refuses the bits keeps its own.
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode & 0o7777));
    }
}

#[cfg(not(unix))]
fn apply_mode(_path: &Path, _entry: &FileEntry) {}

#[cfg(test)]
mod tests;
