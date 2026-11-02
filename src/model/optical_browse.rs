//! A disc as a browser-pane source: open an image or a drive, offer its filesystems (hybrid
//! sides, HP LIF / PReP boot areas), and the two bodies kept from the Optical tab's old disc
//! browser so their bytes stay the same: the extraction walker and the tree listing.
//!
//! The pane lists and exports through [`DiscChoice::filesystem`]; extraction and the tree go
//! through [`DiscChoice::extract_path`] and [`DiscChoice::tree_listing`] (docs/phase2_commander_plan.md
//! § "Slice 18"). The disc is reopened for every operation, honouring the choice.

use std::io::{BufWriter, Write};
use std::path::Path;

use opticaldiscs::browse::filesystem::{Filesystem, FilesystemError};
use opticaldiscs::browse::{open_disc_filesystem, open_hybrid_filesystem};
use opticaldiscs::detect::DiscImageInfo;
use opticaldiscs::formats::FilesystemType;

use crate::optical::boot_area::{self, BootArea};
use disk_core::filesystem::Filesystem as RbFilesystem;
use fs_mac::resource_fork;

pub use fs_mac::resource_fork::ResourceForkMode;
pub use opticaldiscs::browse::entry::{EntryType, FileEntry};

pub use crate::model::commander_descend::DiscSource;

/// A disc source opened for the browser pane: its filesystem choices and the first one, opened.
pub struct DiscOpen {
    pub disc: DiscChoice,
    pub labels: Vec<String>,
    /// "Game: ..." for a recognised console disc.
    pub game: Option<String>,
    /// The first choice's filesystem, or why it did not open (the other choices stay offered).
    pub fs: Result<Box<dyn RbFilesystem>, String>,
}

/// Probe `src` and open its first filesystem; the picker labels are the pane's (Commander's).
pub fn open_source(src: &DiscSource) -> Result<DiscOpen, String> {
    let path = src.path.as_path();
    if src.physical {
        crate::os::ensure_physical_enabled().map_err(|e| e.to_string())?;
    }
    // A drive is not a file: its raw node is read as flat cooked sectors (EBUSY on macOS otherwise).
    let probe = if src.physical {
        DiscImageInfo::open_physical(path)
    } else {
        DiscImageInfo::open(path)
    };
    let info = probe.map_err(|e| format!("Failed to open disc image: {e}"))?;
    let boot_areas = match src.physical {
        true => Vec::new(),
        false => boot_area::detect(path),
    };
    let mut labels =
        crate::model::commander_descend::optical_choice_labels(&info, boot_areas.as_slice());
    // A drive always presents its primary volume.
    if src.physical {
        labels.truncate(1);
    }
    let game = info.game.as_ref().map(crate::optical::format_game_identity);
    let disc = DiscChoice {
        src: src.clone(),
        info: std::sync::Arc::new(info),
        selected: 0,
        boot_areas,
    };
    let fs = disc.filesystem().map_err(|e| {
        // An NKit image that reaches here could not be reconstructed: add the convert-it hint.
        let msg = crate::cli::optical_hint::with_nkit_hint(
            anyhow::anyhow!("Cannot open filesystem: {e}"),
            path,
        );
        format!("{msg:#}")
    });
    Ok(DiscOpen {
        disc,
        labels,
        game,
        fs,
    })
}

/// Find the entry at `path` by listing each folder on the way from the root.
fn entry_at(fs: &mut dyn Filesystem, path: &str) -> Result<FileEntry, FilesystemError> {
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
            None => return Err(FilesystemError::NotFound(path.to_string())),
        }
    }
}

/// The disc opened on picker choice `selected`, plus what it was opened with.
#[derive(Clone)]
pub struct DiscChoice {
    pub src: DiscSource,
    pub info: std::sync::Arc<DiscImageInfo>,
    pub selected: usize,
    pub boot_areas: Vec<BootArea>,
}

impl DiscChoice {
    fn open(&self) -> Result<Box<dyn Filesystem>, FilesystemError> {
        open_choice(
            self.info.as_ref(),
            self.src.path.as_path(),
            self.src.physical,
            self.selected,
            self.boot_areas.as_slice(),
        )
    }

    /// The GNU tree-style listing of the whole volume.
    pub fn tree_listing(&self) -> Result<String, String> {
        let mut fs = self
            .open()
            .map_err(|e| format!("Failed to open filesystem: {e}"))?;
        generate_optical_tree(&mut *fs).map_err(|e| format!("Failed to generate tree: {e}"))
    }

    /// The selected filesystem's type (the primary, or the chosen hybrid side).
    pub fn fs_type(&self) -> Option<FilesystemType> {
        selected_fs_type(self.info.as_ref(), self.selected)
    }

    /// The same disc on picker choice `selected`.
    pub fn with_choice(&self, selected: usize) -> DiscChoice {
        DiscChoice {
            selected,
            ..self.clone()
        }
    }

    /// The choice's name as the pane shows it: "ISO 9660", "HFS+", or the boot area's label.
    pub fn fs_name(&self) -> String {
        match self.fs_type() {
            Some(t) => t.display_name().to_string(),
            None => {
                let hybrids = self.info.hybrid_filesystems.len();
                let area = self.selected.checked_sub(1 + hybrids);
                let area = area.and_then(|i| self.boot_areas.get(i));
                area.map(|a| a.label.clone()).unwrap_or_default()
            }
        }
    }

    /// The browsed filesystem keeps resource forks (HFS / HFS+).
    pub fn has_forks(&self) -> bool {
        matches!(
            self.fs_type(),
            Some(FilesystemType::Hfs | FilesystemType::HfsPlus)
        )
    }

    /// The choice as a rusty-backup filesystem, for the browser pane's listing and copies.
    pub fn filesystem(&self) -> Result<Box<dyn RbFilesystem>, String> {
        let inner = self.open().map_err(|e| e.to_string())?;
        let label = self
            .src
            .path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned());
        let fs = crate::fs::optical_fs::OpticalFilesystem::from_inner(inner, self.fs_name(), label)
            .map_err(|e| e.to_string())?;
        Ok(Box::new(fs))
    }

    /// Extract the entry at `path` with the slice-16 walker, reporting through a host-copy status.
    pub fn extract_path(
        &self,
        path: &str,
        dest: &Path,
        mode: ResourceForkMode,
        status: &std::sync::Arc<std::sync::Mutex<crate::model::commander_ops::HostCopyStatus>>,
    ) -> Result<u32, String> {
        let src = &self.src;
        let mut tally = Tally::default();
        let result = (|| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            // Re-probe as the extraction always has: a file-based reopen of a drive is EBUSY.
            let info = if src.physical {
                DiscImageInfo::open_physical(src.path.as_path())?
            } else {
                DiscImageInfo::open(src.path.as_path())?
            };
            let reopen = || {
                open_choice(
                    &info,
                    src.path.as_path(),
                    src.physical,
                    self.selected,
                    self.boot_areas.as_slice(),
                )
            };
            let mut counting_fs = reopen()?;
            let entry = entry_at(&mut *counting_fs, path)?;
            let (total_files, total_bytes) = count_entry(&mut *counting_fs, &entry)?;
            tally.total_files = total_files;
            tally.total_bytes = total_bytes;
            if let Ok(mut g) = status.lock() {
                g.files_total = total_files as usize;
                g.bytes_total = total_bytes;
            }
            drop(counting_fs);
            let mut fs = reopen()?;
            let report = StatusReport(status);
            extract_entry(&mut *fs, &entry, dest, mode, &mut tally, &report)?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(tally.files_extracted),
            Err(e) => {
                let msg = crate::cli::optical_hint::with_nkit_hint(
                    anyhow::anyhow!("{e}"),
                    src.path.as_path(),
                );
                Err(format!("{msg:#}"))
            }
        }
    }
}

/// The filesystem type of picker choice `selected`; a boot area has none.
pub fn selected_fs_type(info: &DiscImageInfo, selected: usize) -> Option<FilesystemType> {
    if selected == 0 {
        Some(info.filesystem)
    } else {
        info.hybrid_filesystems
            .get(selected - 1)
            .map(|h| h.filesystem)
    }
}

/// Human-friendly size string.
pub fn format_size(bytes: u64) -> String {
    match bytes {
        s if s < 1_024 => format!("{} B", s),
        s if s < 1_024 * 1_024 => format!("{:.1} KB", s as f64 / 1_024.0),
        s if s < 1_024 * 1_024 * 1_024 => format!("{:.1} MB", s as f64 / (1_024.0 * 1_024.0)),
        s => format!("{:.2} GB", s as f64 / (1_024.0 * 1_024.0 * 1_024.0)),
    }
}

/// Open picker choice `selected`: 0 the primary, then the hybrid volumes, then the boot areas.
fn open_choice(
    info: &DiscImageInfo,
    disc_path: &Path,
    physical: bool,
    selected: usize,
    boot_areas: &[BootArea],
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    if physical {
        // A drive always presents its primary volume.
        return opticaldiscs::browse::open_physical_filesystem(disc_path);
    }
    let hybrids = info.hybrid_filesystems.len();
    match selected {
        0 => open_disc_filesystem(info),
        i if i <= hybrids => open_hybrid_filesystem(info, i - 1),
        i => match boot_areas.get(i - 1 - hybrids) {
            Some(area) => boot_area::open(disc_path, area),
            None => Err(FilesystemError::NotFound(format!("filesystem choice {i}"))),
        },
    }
}

/// Generate a GNU tree-style listing of all files and directories.
fn generate_optical_tree(fs: &mut dyn Filesystem) -> Result<String, FilesystemError> {
    let root = fs.root()?;
    let label = fs.volume_name().unwrap_or("/").to_owned();
    let mut out = String::new();
    out.push_str(&label);
    out.push('\n');
    let mut dir_count: u64 = 0;
    let mut file_count: u64 = 0;
    walk_optical_tree(fs, &root, "", &mut out, &mut dir_count, &mut file_count)?;
    out.push_str(&format!(
        "\n{} directories, {} files\n",
        dir_count, file_count
    ));
    Ok(out)
}

fn walk_optical_tree(
    fs: &mut dyn Filesystem,
    dir: &FileEntry,
    prefix: &str,
    out: &mut String,
    dir_count: &mut u64,
    file_count: &mut u64,
) -> Result<(), FilesystemError> {
    let children = fs.list_directory(dir)?;
    let count = children.len();
    for (i, child) in children.iter().enumerate() {
        let is_last = i == count - 1;
        // ASCII connectors: the default egui font has no box-drawing glyphs.
        let connector = if is_last { "\\-- " } else { "|-- " };
        out.push_str(prefix);
        out.push_str(connector);
        out.push_str(&child.name);
        if let Some(ref target) = child.symlink_target {
            out.push_str(&format!(" -> {target}"));
        }
        if child.is_file() {
            let total = child.total_size();
            out.push_str(&format!("  [{}]", format_size(total)));
            if let Some(rsrc) = child.resource_fork_size {
                if rsrc > 0 {
                    out.push_str(&format!(" (rsrc: {})", format_size(rsrc)));
                }
            }
            if let Some(tc) = child.type_code_string() {
                out.push_str(&format!("  {tc}"));
                if let Some(cc) = child.creator_code_string() {
                    out.push_str(&format!("/{cc}"));
                }
            }
        }
        out.push('\n');
        if child.is_directory() {
            *dir_count += 1;
            let child_prefix = if is_last {
                format!("{prefix}    ")
            } else {
                format!("{prefix}|   ")
            };
            walk_optical_tree(fs, child, &child_prefix, out, dir_count, file_count)?;
        } else {
            *file_count += 1;
        }
    }
    Ok(())
}

/// Recursively count files and total bytes for progress tracking.
fn count_entry(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
    match entry.entry_type {
        EntryType::File => {
            let rsrc = entry.resource_fork_size.unwrap_or(0);
            Ok((1, entry.size + rsrc))
        }
        EntryType::Directory => {
            let children = fs.list_directory(entry)?;
            let mut total_files = 0u32;
            let mut total_bytes = 0u64;
            for child in &children {
                let (f, b) = count_entry(fs, child)?;
                total_files += f;
                total_bytes += b;
            }
            Ok((total_files, total_bytes))
        }
    }
}

/// An extraction's running counts.
#[derive(Default)]
struct Tally {
    current_bytes: u64,
    total_bytes: u64,
    files_extracted: u32,
    total_files: u32,
}

/// The browser pane's copy job reads an extraction's progress from its host-copy status.
struct StatusReport<'a>(
    &'a std::sync::Arc<std::sync::Mutex<crate::model::commander_ops::HostCopyStatus>>,
);

impl StatusReport<'_> {
    fn report(&self, tally: &Tally, current_file: &str) {
        if let Ok(mut g) = self.0.lock() {
            g.current_file = current_file.to_string();
            g.copied = tally.files_extracted as usize;
            g.bytes_done = tally.current_bytes;
        }
    }

    fn cancelled(&self) -> bool {
        self.0.lock().map(|g| g.cancel_requested).unwrap_or(false)
    }
}

/// Recursively extract an entry to the destination path.
fn extract_entry(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    dest: &Path,
    resource_fork_mode: ResourceForkMode,
    tally: &mut Tally,
    ctx: &StatusReport,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if ctx.cancelled() {
        return Err("Extraction cancelled".into());
    }
    let safe_name = resource_fork::sanitize_filename(entry.name.as_str());
    match entry.entry_type {
        EntryType::File => {
            ctx.report(tally, entry.path.as_str());
            // Only fork-capable filesystems (HFS / HFS+) report a non-zero resource fork.
            let has_rsrc = entry.resource_fork_size.map(|s| s > 0).unwrap_or(false);
            if has_rsrc && resource_fork_mode == ResourceForkMode::MacBinary {
                let data = fs.read_file(entry)?;
                let rsrc_data = fs.read_resource_fork(entry)?.unwrap_or_default();
                let type_code = entry.type_code.unwrap_or([0; 4]);
                let creator_code = entry.creator_code.unwrap_or([0; 4]);
                let dates = crate::optical::mac_dates_from(&entry.timestamps);
                let mb = resource_fork::build_macbinary(
                    &safe_name,
                    &type_code,
                    &creator_code,
                    dates,
                    &data,
                    &rsrc_data,
                );
                let out_path = dest.join(format!("{safe_name}.bin"));
                let mut f = BufWriter::new(std::fs::File::create(&out_path)?);
                f.write_all(&mb)?;
                f.flush()?;
                tally.current_bytes += data.len() as u64 + rsrc_data.len() as u64;
                tally.files_extracted += 1;
            } else {
                let data = fs.read_file(entry)?;
                let out_path = dest.join(&safe_name);
                let mut f = BufWriter::new(std::fs::File::create(&out_path)?);
                f.write_all(&data)?;
                f.flush()?;
                tally.current_bytes += data.len() as u64;
                if has_rsrc && resource_fork_mode != ResourceForkMode::DataForkOnly {
                    let type_code = entry.type_code.unwrap_or([0; 4]);
                    let creator_code = entry.creator_code.unwrap_or([0; 4]);
                    let dates = crate::optical::mac_dates_from(&entry.timestamps);
                    let rsrc_data = fs.read_resource_fork(entry)?.unwrap_or_default();
                    match resource_fork_mode {
                        ResourceForkMode::Native => {
                            let rsrc_path = out_path.join("..namedfork/rsrc");
                            let mut rf = BufWriter::new(std::fs::File::create(&rsrc_path)?);
                            rf.write_all(&rsrc_data)?;
                            rf.flush()?;
                        }
                        ResourceForkMode::AppleDouble => {
                            let ad = resource_fork::build_appledouble(
                                &type_code,
                                &creator_code,
                                dates,
                                &rsrc_data,
                            );
                            let ad_path = dest.join(format!("._{safe_name}"));
                            let mut af = BufWriter::new(std::fs::File::create(&ad_path)?);
                            af.write_all(&ad)?;
                            af.flush()?;
                        }
                        ResourceForkMode::SeparateRsrc => {
                            let rsrc_path = dest.join(format!("{safe_name}.rsrc"));
                            let mut rf = BufWriter::new(std::fs::File::create(&rsrc_path)?);
                            rf.write_all(&rsrc_data)?;
                            rf.flush()?;
                        }
                        _ => {}
                    }
                    tally.current_bytes += rsrc_data.len() as u64;
                }
                tally.files_extracted += 1;
            }
            ctx.report(tally, entry.path.as_str());
        }
        EntryType::Directory => {
            let dir_path = dest.join(&safe_name);
            std::fs::create_dir_all(&dir_path)?;
            let children = fs.list_directory(entry)?;
            for child in &children {
                extract_entry(fs, child, &dir_path, resource_fork_mode, tally, ctx)?;
            }
        }
    }
    Ok(())
}
