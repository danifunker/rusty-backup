//! Extract one entry of a volume (a file, or a folder with everything in it) to a host folder.
//!
//! The Inspect browser's Extract: forks per a `ResourceForkMode`, ProDOS files named with the
//! CiderPress `#TTAAAA` suffix so type and aux survive a round trip, and QNX 2 text files with
//! RS line separators turned into LF. It runs as a [`super::commander_ops::HostCopyJob::Extract`]
//! so the screen's copy job reports its progress, cancels it and settles existing files.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use disk_core::entry::{EntryType, FileEntry};
use disk_core::filesystem::Filesystem;
use fs_mac::fork_export;
use fs_mac::resource_fork::{sanitize_filename, ResourceForkMode};

use super::commander_ops::{HostCopyStatus, StageSource};
use crate::fs::replace::OnConflict;

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
    let base = sanitize_filename(&entry.name);
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

/// The host paths the entry's own output would replace (its file and any fork sidecar).
pub fn extraction_conflicts(
    entry: &FileEntry,
    dest: &Path,
    options: &ExtractOptions,
    forks: bool,
) -> Vec<PathBuf> {
    let name = host_name(entry, options);
    let has_rsrc = forks && entry.is_file() && entry.resource_fork_size.is_some_and(|s| s > 0);
    let mut out = Vec::new();
    match options.fork_mode {
        ResourceForkMode::MacBinary if has_rsrc => out.push(dest.join(format!("{name}.bin"))),
        // BinHex packs both forks and the Finder info into one file, fork or not.
        ResourceForkMode::BinHex if entry.is_file() => out.push(dest.join(format!("{name}.hqx"))),
        mode => {
            out.push(dest.join(&name));
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
    out.into_iter().filter(|p| p.exists()).collect()
}

/// Run an extraction, counting first for the progress bar; returns the files written.
pub(crate) fn run(job: ExtractJob, status: &Arc<Mutex<HostCopyStatus>>) -> Result<usize> {
    let conflicts = extraction_conflicts(&job.entry, &job.dest, &job.options, job.forks);
    if !conflicts.is_empty() && job.on_conflict == OnConflict::Skip {
        if let Ok(mut g) = status.lock() {
            g.skipped
                .push(format!("{} (already exists)", job.entry.name));
        }
        return Ok(0);
    }
    let mut fs = job.source.open()?;
    let (files, bytes) = count(fs.as_mut(), &job.entry)?;
    if let Ok(mut g) = status.lock() {
        g.files_total = files;
        g.bytes_total = bytes;
    }
    let mut out = Extractor {
        fs: fs.as_mut(),
        options: job.options,
        status,
        written: 0,
    };
    out.entry(&job.entry, &job.dest)?;
    Ok(out.written)
}

fn count(fs: &mut dyn Filesystem, entry: &FileEntry) -> Result<(usize, u64)> {
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

struct Extractor<'a> {
    fs: &'a mut dyn Filesystem,
    options: ExtractOptions,
    status: &'a Arc<Mutex<HostCopyStatus>>,
    written: usize,
}

impl Extractor<'_> {
    fn cancelled(&self) -> bool {
        self.status
            .lock()
            .map(|g| g.cancel_requested)
            .unwrap_or(false)
    }

    fn wrote(&mut self, bytes: u64) {
        self.written += 1;
        if let Ok(mut g) = self.status.lock() {
            g.copied = self.written;
            g.bytes_done += bytes;
        }
    }

    fn entry(&mut self, entry: &FileEntry, dest: &Path) -> Result<()> {
        if self.cancelled() {
            anyhow::bail!("Extraction cancelled");
        }
        let name = host_name(entry, &self.options);
        match entry.entry_type {
            EntryType::File => {
                if let Ok(mut g) = self.status.lock() {
                    g.current_file = entry.path.clone();
                }
                if self.options.rs_to_lf && entry.size <= 16 << 20 {
                    let data = self.fs.read_file(entry, usize::MAX)?;
                    if crate::fs::qnx2::looks_like_rs_text(&data) {
                        std::fs::write(dest.join(&name), crate::fs::qnx2::rs_to_lf(&data))?;
                        self.wrote(data.len() as u64);
                        return Ok(());
                    }
                }
                let mode = self.options.fork_mode;
                let bytes = fork_export::export_file_with_fork(self.fs, entry, dest, &name, mode)?;
                self.wrote(bytes);
            }
            EntryType::Directory => {
                let dir = dest.join(&name);
                std::fs::create_dir_all(&dir)
                    .with_context(|| format!("creating {}", dir.display()))?;
                for child in self.fs.list_directory(entry)? {
                    match self.entry(&child, &dir) {
                        Ok(()) => {}
                        Err(e) if self.cancelled() => return Err(e),
                        // One unreadable entry (an NTFS metafile, a bad file) is skipped, not fatal.
                        Err(e) => {
                            log::warn!("Skipped {}: {e:#}", child.path);
                            if let Ok(mut g) = self.status.lock() {
                                g.skipped.push(format!("{} ({e:#})", child.path));
                            }
                        }
                    }
                }
            }
            EntryType::Symlink => {
                #[cfg(unix)]
                {
                    let target = entry.symlink_target.as_deref().unwrap_or("");
                    // The target may not exist on the host; the link is still worth writing.
                    let _ = std::os::unix::fs::symlink(target, dest.join(&name));
                }
                self.wrote(0);
            }
            EntryType::Special => {}
        }
        Ok(())
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
