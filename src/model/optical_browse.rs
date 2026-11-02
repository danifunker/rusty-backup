//! The Optical tab's disc browser: open a disc image or a drive, list folders, preview a file,
//! extract, export the marked entries and build the tree listing.
//!
//! These are the bodies the egui widget (`optical::browse_view`) ran inline or on its own
//! thread, moved here unchanged so the files they write are the same;
//! `controller::optical::browse` runs each as a `model::job`. The disc is reopened for every
//! operation (nothing is cached across calls), honouring the hybrid / boot-area choice.

use std::collections::BTreeMap;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use opticaldiscs::browse::filesystem::{Filesystem, FilesystemError};
use opticaldiscs::browse::{open_disc_filesystem, open_hybrid_filesystem};
use opticaldiscs::detect::DiscImageInfo;
use opticaldiscs::formats::FilesystemType;

use crate::model::job::{JobCtx, Progress, Unit};
use crate::optical::boot_area::{self, BootArea};
use fs_mac::resource_fork;

pub use crate::fs::export_selection::ExportFormat;
pub use fs_mac::resource_fork::ResourceForkMode;
pub use opticaldiscs::browse::entry::{EntryType, FileEntry};

/// Files larger than this are not read for the preview.
pub const MAX_PREVIEW_SIZE: usize = 1024 * 1024;

/// A disc image file, or the disc in a drive (`physical`, read through the device-backed reader).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscSource {
    pub path: PathBuf,
    pub physical: bool,
}

/// What opening a disc found; `error` is shown above the tree.
pub struct OpenedDisc {
    pub info: Option<DiscImageInfo>,
    pub boot_areas: Vec<BootArea>,
    pub root: RootRead,
}

/// The selected filesystem's root and its listing.
#[derive(Default)]
pub struct RootRead {
    pub root: Option<FileEntry>,
    pub entries: Option<Vec<FileEntry>>,
    pub error: Option<String>,
}

/// A previewed file's content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileContent {
    Binary(Vec<u8>),
    Text(String),
}

/// Display order for a disc listing: directories first, then files, each case-insensitive by name.
fn sort_for_display(entries: &mut [FileEntry]) {
    entries.sort_by_cached_key(|e| (!e.is_directory(), e.name.to_lowercase()));
}

/// Open `src`: probe it, open its primary filesystem and list the root.
pub fn open_disc(src: &DiscSource) -> OpenedDisc {
    let path = src.path.as_path();
    let mut out = OpenedDisc {
        info: None,
        boot_areas: Vec::new(),
        root: RootRead::default(),
    };
    if src.physical {
        if let Err(e) = crate::os::ensure_physical_enabled() {
            out.root.error = Some(e.to_string());
            return out;
        }
    }
    // A drive is not a file: its raw node is read as flat cooked sectors (EBUSY on macOS otherwise).
    let probe = if src.physical {
        DiscImageInfo::open_physical(path)
    } else {
        DiscImageInfo::open(path)
    };
    let info = match probe {
        Ok(info) => info,
        Err(e) => {
            out.root.error = Some(format!("Failed to open disc image: {e}"));
            return out;
        }
    };
    let opened = if src.physical {
        opticaldiscs::browse::open_physical_filesystem(path)
    } else {
        open_disc_filesystem(&info)
    };
    match opened {
        Ok(mut fs) => out.root = list_root(&mut *fs),
        Err(e) => {
            // An NKit image that reaches here could not be reconstructed: add the convert-it hint.
            let msg = crate::cli::optical_hint::with_nkit_hint(
                anyhow::anyhow!("Cannot open filesystem: {e}"),
                path,
            );
            out.root.error = Some(format!("{msg:#}"));
        }
    }
    if !src.physical {
        out.boot_areas = boot_area::detect(path);
    }
    out.info = Some(info);
    out
}

fn list_root(fs: &mut dyn Filesystem) -> RootRead {
    let mut out = RootRead::default();
    match fs.root() {
        Ok(root) => {
            match fs.list_directory(&root) {
                Ok(mut entries) => {
                    sort_for_display(&mut entries);
                    out.entries = Some(entries);
                }
                Err(e) => out.error = Some(format!("Failed to read root directory: {e}")),
            }
            out.root = Some(root);
        }
        Err(e) => out.error = Some(format!("Failed to get root: {e}")),
    }
    out
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

    /// The root of this choice (a hybrid side or boot area switched to).
    pub fn read_root(&self) -> RootRead {
        match self.open() {
            Ok(mut fs) => list_root(&mut *fs),
            Err(e) => RootRead {
                error: Some(format!("Cannot open filesystem: {e}")),
                ..RootRead::default()
            },
        }
    }

    /// One folder's entries, sorted for display.
    pub fn list_dir(&self, entry: &FileEntry) -> Result<Vec<FileEntry>, String> {
        let mut fs = self
            .open()
            .map_err(|e| format!("Failed to reopen disc for {}: {e}", entry.path))?;
        let mut entries = fs
            .list_directory(entry)
            .map_err(|e| format!("Failed to read {}: {e}", entry.path))?;
        sort_for_display(&mut entries);
        Ok(entries)
    }

    /// A file's content for the preview (the caller skips files over `MAX_PREVIEW_SIZE`).
    pub fn preview(&self, entry: &FileEntry) -> Result<FileContent, String> {
        let mut fs = self
            .open()
            .map_err(|e| format!("Cannot open filesystem: {e}"))?;
        fs.read_file(entry)
            .map(|data| detect_content_type(data.as_slice()))
            .map_err(|e| format!("Failed to read file: {e}"))
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

    /// Extract `entry` (a file or a folder) into `dest`; the number of files written.
    pub fn extract(
        &self,
        entry: &FileEntry,
        dest: &Path,
        mode: ResourceForkMode,
        ctx: &JobCtx,
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
            let (total_files, total_bytes) = count_entry(&mut *counting_fs, entry)?;
            tally.total_files = total_files;
            tally.total_bytes = total_bytes;
            drop(counting_fs);
            let mut fs = reopen()?;
            extract_entry(&mut *fs, entry, dest, mode, &mut tally, ctx)?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(tally.files_extracted),
            Err(e) => {
                let msg =
                    crate::cli::optical_hint::with_nkit_hint(anyhow::anyhow!("{e}"), &src.path);
                Err(format!("{msg:#}"))
            }
        }
    }

    /// Export `entries` together as one `format` output at `dest` (a folder for loose formats).
    pub fn export(
        &self,
        entries: &[FileEntry],
        dest: &Path,
        format: ExportFormat,
        fork_mode: ResourceForkMode,
    ) -> Result<usize, String> {
        use crate::fs::export_selection::{export_to_file, export_to_folder};
        let rb_entries: Vec<disk_core::entry::FileEntry> = entries
            .iter()
            .map(crate::fs::optical_fs::translate)
            .collect();
        let fs_type = self.fs_type().map(|t| format!("{t:?}")).unwrap_or_default();
        let noop = |_: &str, _: usize, _: u64| {};
        let nocancel = || false;
        let inner = self.open().map_err(|e| e.to_string())?;
        let mut rb_fs = crate::fs::optical_fs::OpticalFilesystem::from_inner(inner, fs_type, None)
            .map_err(|e| e.to_string())?;
        let summary = if format.is_single_file() {
            export_to_file(&mut rb_fs, &rb_entries, dest, format, &noop, &nocancel)
        } else {
            export_to_folder(
                &mut rb_fs,
                &rb_entries,
                dest,
                format,
                fork_mode,
                &noop,
                &nocancel,
            )
        };
        summary.map(|s| s.files).map_err(|e| e.to_string())
    }
}

/// Picker labels: the primary filesystem, then the hybrid sides, then the boot areas.
pub fn fs_choice_labels(info: &DiscImageInfo, boot_areas: &[BootArea]) -> Vec<String> {
    let mut out = vec![match &info.volume_label {
        Some(l) => format!("{} ({l})", info.filesystem.display_name()),
        None => info.filesystem.display_name().to_string(),
    }];
    for h in &info.hybrid_filesystems {
        let name = h.filesystem.display_name();
        out.push(match &h.volume_label {
            Some(l) => format!("{name} ({l})"),
            None => name.to_string(),
        });
    }
    out.extend(boot_areas.iter().map(|a| a.label.clone()));
    out
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

/// Marked entries to export, without any that live under another marked folder.
pub fn marked_export_entries(marked: &BTreeMap<String, FileEntry>) -> Vec<FileEntry> {
    fn is_ancestor(a: &str, p: &str) -> bool {
        if a == p {
            return false;
        }
        if a == "/" {
            return p.len() > 1 && p.starts_with('/');
        }
        p.starts_with(a) && p.as_bytes().get(a.len()) == Some(&b'/')
    }
    let marked_paths: Vec<&str> = marked.keys().map(String::as_str).collect();
    marked
        .iter()
        .filter(|(p, _)| !marked_paths.iter().any(|a| is_ancestor(a, p)))
        .map(|(_, e)| e.clone())
        .collect()
}

/// User-facing name: opticaldiscs names the disc root "" (empty), shown as "/".
pub fn display_name(entry: &FileEntry) -> &str {
    if entry.name.is_empty() && entry.path == "/" {
        "/"
    } else {
        entry.name.as_str()
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

/// Text when the bytes read as text (UTF-8 or 80% printable), else binary.
pub fn detect_content_type(data: &[u8]) -> FileContent {
    if data.is_empty() {
        return FileContent::Text(String::new());
    }
    if let Ok(text) = std::str::from_utf8(data) {
        let non_printable = text
            .chars()
            .filter(|c| !c.is_ascii_graphic() && !c.is_ascii_whitespace())
            .count();
        if non_printable * 10 < text.len() {
            return FileContent::Text(text.to_string());
        }
    }
    let printable = data
        .iter()
        .filter(|&&b| b.is_ascii_graphic() || b.is_ascii_whitespace())
        .count();
    if printable * 10 >= data.len() * 8 {
        let text: String = data
            .iter()
            .map(|&b| {
                if b.is_ascii_graphic() || b.is_ascii_whitespace() {
                    b as char
                } else {
                    '.'
                }
            })
            .collect();
        return FileContent::Text(text);
    }
    FileContent::Binary(data.to_vec())
}

/// The hex view's text: 16 bytes a line, at most 256 lines.
pub fn hex_dump(data: &[u8]) -> String {
    let bytes_per_line = 16;
    let lines = data.len().div_ceil(bytes_per_line);
    let max_lines = 256;
    let display_lines = lines.min(max_lines);
    let mut hex_text = String::new();
    for i in 0..display_lines {
        let offset = i * bytes_per_line;
        hex_text.push_str(&format!("{offset:08X}  "));
        for j in 0..bytes_per_line {
            if offset + j < data.len() {
                hex_text.push_str(&format!("{:02X} ", data[offset + j]));
            } else {
                hex_text.push_str("   ");
            }
            if j == 7 {
                hex_text.push(' ');
            }
        }
        hex_text.push_str(" |");
        for j in 0..bytes_per_line {
            if offset + j < data.len() {
                let b = data[offset + j];
                hex_text.push(if b.is_ascii_graphic() || b == b' ' {
                    b as char
                } else {
                    '.'
                });
            }
        }
        hex_text.push_str("|\n");
    }
    if lines > max_lines {
        hex_text.push_str(&format!("... ({} more lines)\n", lines - max_lines));
    }
    hex_text
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

impl Tally {
    fn report(&self, ctx: &JobCtx, current_file: &str) {
        ctx.set_phase(format!(
            "Extracting {}/{} files: {}",
            self.files_extracted, self.total_files, current_file
        ));
        if self.total_bytes > 0 {
            ctx.set_bytes(self.current_bytes, self.total_bytes);
        } else {
            ctx.set_progress(Progress::Determinate {
                done: u64::from(self.files_extracted),
                total: u64::from(self.total_files),
                unit: Unit::Items,
            });
        }
    }
}

/// Recursively extract an entry to the destination path.
fn extract_entry(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    dest: &Path,
    resource_fork_mode: ResourceForkMode,
    tally: &mut Tally,
    ctx: &JobCtx,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if ctx.is_cancelled() {
        return Err("Extraction cancelled".into());
    }
    let safe_name = resource_fork::sanitize_filename(&entry.name);
    match entry.entry_type {
        EntryType::File => {
            tally.report(ctx, entry.path.as_str());
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
            tally.report(ctx, entry.path.as_str());
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
