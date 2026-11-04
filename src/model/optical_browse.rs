//! A disc as a browser-pane source: open an image or a drive, offer its filesystems (hybrid
//! sides, HP LIF / PReP boot areas), and the tree listing kept from the Optical tab's old disc
//! browser so its text stays the same.
//!
//! The pane lists and exports through [`DiscChoice::filesystem`] and extracts through
//! [`DiscChoice::extract_filesystem`] (`model::extract`, the `rb-fsexporter` crate); the tree goes
//! through [`DiscChoice::tree_listing`] (docs/phase2_commander_plan.md § "Slice 18", "Slice
//! 19"). The disc is reopened for every operation, honouring the choice.

use std::path::Path;

use opticaldiscs::browse::filesystem::{Filesystem, FilesystemError};
use opticaldiscs::browse::{open_disc_filesystem, open_hybrid_filesystem};
use opticaldiscs::detect::DiscImageInfo;
use opticaldiscs::formats::FilesystemType;

use crate::optical::boot_area::{self, BootArea};
use disk_core::filesystem::Filesystem as RbFilesystem;

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

    /// The choice as a rusty-backup filesystem, for the browser pane's listing and copies.
    pub fn filesystem(&self) -> Result<Box<dyn RbFilesystem>, String> {
        Ok(Box::new(self.optical_filesystem()?))
    }

    /// The choice for the host extractor: its entries carry Mac dates and POSIX modes.
    pub fn extract_filesystem(&self) -> Result<Box<dyn RbFilesystem>, String> {
        Ok(Box::new(self.optical_filesystem()?.with_extract_metadata()))
    }

    fn optical_filesystem(&self) -> Result<crate::fs::optical_fs::OpticalFilesystem, String> {
        let inner = self.open().map_err(|e| e.to_string())?;
        let label = self
            .src
            .path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned());
        crate::fs::optical_fs::OpticalFilesystem::from_inner(inner, self.fs_name(), label)
            .map_err(|e| e.to_string())
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
