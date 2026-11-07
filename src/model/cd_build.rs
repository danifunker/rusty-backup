//! CD-ROM images this tool can master: an IRIX EFS disc, a classic Mac HFS or HFS+ disc, and a
//! NeXTSTEP UFS disc, each blank or filled from a host folder in the same step.
//!
//! `rb-cli optical new {sgi-efs,mac-hfs,mac-hfsplus,next-ufs}` and Build Disk's CD-ROM kind both
//! run [`build_cd`]. No ISO 9660 side is written for any of them: the engine has no ISO 9660
//! mastering, so these are the native-filesystem discs each platform reads. Every layout streams
//! to the file: only labels and filesystem metadata are written, the rest stays a sparse hole.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::fs::dir_import::DirImportOptions;
use crate::partition::mac_cd_builder::{write_mac_hfs_cd, MacCdFs, MacCdOptions};
use crate::partition::next_cd_builder::{
    plan_next_ufs_cd, write_next_ufs_cd, NextCdOptions, NEXT_CD_FRONT_PORCH, NEXT_CD_SECTOR,
};
use crate::partition::sgi_hdd_builder::{write_sgi_efs_hdd, SgiHddOptions};
use fs_tar::import_sink::{ImportConflict, ImportOptions, ImportStats};

/// A 700 MB CD-R's capacity; larger images still work in an emulator.
pub const CD_R_BYTES: u64 = 360_000 * 2048;

/// The disc layouts, in the order every shell lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdLayout {
    SgiEfs,
    MacHfs,
    MacHfsPlus,
    NextUfs,
}

impl CdLayout {
    pub const ALL: [CdLayout; 4] = [
        CdLayout::SgiEfs,
        CdLayout::MacHfs,
        CdLayout::MacHfsPlus,
        CdLayout::NextUfs,
    ];

    pub fn label(self) -> &'static str {
        match self {
            CdLayout::SgiEfs => "IRIX EFS CD-ROM",
            CdLayout::MacHfs => "Classic Mac CD-ROM (HFS)",
            CdLayout::MacHfsPlus => "Mac CD-ROM (HFS+, Mac OS 8.1 and later)",
            CdLayout::NextUfs => "NeXTSTEP CD-ROM (UFS)",
        }
    }

    pub fn hint(self) -> &'static str {
        match self {
            CdLayout::SgiEfs => {
                "An SGI volume header with EFS in slot 7, as IRIX discs are. Mounts with \
                 mount -t efs <dev>s7."
            }
            CdLayout::MacHfs => {
                "An Apple Partition Map with one HFS volume: what every Mac since System 3 \
                 mounts. Mac-only, no ISO 9660 side."
            }
            CdLayout::MacHfsPlus => "As the HFS disc, with HFS+ (Mac OS Extended) instead.",
            CdLayout::NextUfs => {
                "A NeXT label in 2048-byte sectors around one 4.3BSD UFS, as NeXT's own discs \
                 are. A data disc, no boot blocks."
            }
        }
    }

    /// The volume name a new disc gets.
    pub fn default_name(self) -> &'static str {
        match self {
            CdLayout::SgiEfs => "rusty",
            CdLayout::MacHfs | CdLayout::MacHfsPlus | CdLayout::NextUfs => "rusty-backup",
        }
    }

    /// The longest name the layout keeps, in bytes.
    pub fn name_limit(self) -> usize {
        match self {
            CdLayout::SgiEfs => 6,
            CdLayout::MacHfs | CdLayout::MacHfsPlus => 27,
            CdLayout::NextUfs => 23,
        }
    }

    /// Unix permissions land on the disc (EFS and UFS); HFS has none.
    pub fn keeps_permissions(self) -> bool {
        matches!(self, CdLayout::SgiEfs | CdLayout::NextUfs)
    }

    /// Archives may be unpacked into one shared root, the shape IRIX `inst` wants.
    pub fn can_flatten(self) -> bool {
        matches!(self, CdLayout::SgiEfs | CdLayout::NextUfs)
    }
}

/// How a host folder is copied onto the disc.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CdImport {
    /// Unpack tar and classic Mac archives into a folder named after each.
    pub expand_archives: bool,
    /// Strip the gzip layer off `.gz` files.
    pub expand_gunzip: bool,
    /// Copy the files out of disk images rather than the image.
    pub expand_images: bool,
    /// With `expand_archives`: unpack every archive into the volume root (EFS and UFS only).
    pub flatten_folders: bool,
    /// Overwrite an entry that already exists rather than stopping on it.
    pub force: bool,
    /// Ignore the host's Unix mode and ownership.
    pub no_permissions: bool,
    /// Import macOS AppleDouble sidecars (`._*`) too.
    pub include_appledouble: bool,
}

/// One disc to master.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdRequest {
    pub layout: CdLayout,
    /// Bytes, or `None` to size the disc to `from_dir`.
    pub size: Option<u64>,
    pub name: String,
    pub from_dir: Option<PathBuf>,
    pub import: CdImport,
    /// Mac only: the allocation block size, picked by the volume size when `None`.
    pub mac_block_size: Option<u32>,
    /// EFS and UFS only: inode density in bytes per inode.
    pub bytes_per_inode: Option<u64>,
}

impl CdRequest {
    pub fn new(layout: CdLayout) -> Self {
        CdRequest {
            layout,
            size: Some(600 * 1024 * 1024),
            name: layout.default_name().to_string(),
            from_dir: None,
            import: CdImport::default(),
            mac_block_size: None,
            bytes_per_inode: None,
        }
    }
}

/// The importer's settings for `layout`, shared by sizing and filling so the disc fits what lands.
pub fn import_options(layout: CdLayout, import: &CdImport) -> DirImportOptions {
    let flatten = import.flatten_folders && layout.can_flatten();
    DirImportOptions {
        shared: ImportOptions {
            // Flattened archives overlap by design, so they skip rather than stop.
            conflict: match (import.force, flatten) {
                (true, _) => ImportConflict::Overwrite,
                (false, true) => ImportConflict::Skip,
                (false, false) => ImportConflict::Error,
            },
            apply_permissions: layout.keeps_permissions() && !import.no_permissions,
            skip_appledouble: !import.include_appledouble,
        },
        expand_archives: import.expand_archives,
        flatten_archives: flatten,
        expand_gunzip: import.expand_gunzip,
        expand_disk_images: import.expand_images,
    }
}

/// The disc size `req` asks for, measuring `from_dir` when the size is automatic.
pub fn resolve_size(req: &CdRequest) -> Result<u64> {
    if let Some(size) = req.size {
        return Ok(size);
    }
    let Some(dir) = &req.from_dir else {
        anyhow::bail!("an automatic size needs a folder to measure; give a size instead");
    };
    let opts = import_options(req.layout, &req.import);
    let (files, dirs, bytes) = crate::fs::dir_import::measure_dir_for(dir, &opts)
        .with_context(|| format!("measuring {}", dir.display()))?;
    let projected =
        |block: u64| crate::fs::dir_import::projected_volume_bytes(files, dirs, bytes, block);
    Ok(match req.layout {
        // EFS allocates in 512-byte blocks.
        CdLayout::SgiEfs => projected(512),
        // HFS's allocation block scales with the volume, so a rough pass picks it first.
        CdLayout::MacHfs => projected(fs_mac::hfs::pick_cd_block_size(projected(512)) as u64),
        CdLayout::MacHfsPlus => projected(4096),
        CdLayout::NextUfs => {
            // Our UFS writer gives each file's tail a whole 8 KiB block.
            let size = projected(8192) + NEXT_CD_FRONT_PORCH * NEXT_CD_SECTOR;
            let probe = plan_next_ufs_cd(&NextCdOptions {
                bytes_per_inode: req.bytes_per_inode,
                ..NextCdOptions::new(size, "")
            })?;
            let inodes = probe.geometry.ncg * probe.geometry.ipg;
            let wanted = files + dirs + 16;
            if inodes >= wanted {
                size
            } else {
                size * wanted.div_ceil(inodes.max(1))
            }
        }
    })
}

/// What [`write_blank`] laid down.
#[derive(Debug, Clone)]
pub struct CdWritten {
    pub disk_bytes: u64,
    /// Where the disc's one filesystem starts.
    pub fs_offset: u64,
    /// One line per fact worth reporting.
    pub lines: Vec<String>,
}

/// Lay a blank disc of `size` bytes into `path`, replacing it.
pub fn write_blank(path: &Path, req: &CdRequest, size: u64) -> Result<CdWritten> {
    let create = || {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("creating {}", path.display()))
    };
    let mib = |b: u64| b / (1024 * 1024);
    let written = match req.layout {
        CdLayout::SgiEfs => {
            let mut opts = SgiHddOptions::new_cdrom(size, req.name.clone());
            opts.bytes_per_inode =
                fs_unix::efs::resolve_bytes_per_inode(size, None, req.bytes_per_inode);
            let mut file = create()?;
            let l = write_sgi_efs_hdd(&mut file, &opts)?;
            file.set_len(l.disk_bytes)?;
            CdWritten {
                disk_bytes: l.disk_bytes,
                fs_offset: l.efs_first_sector * 512,
                lines: vec![
                    format!(
                        "SGI volume header + EFS CD-ROM, {} cyls x {} heads x {} secs/trk",
                        l.cylinders, l.heads, l.sectors_per_track
                    ),
                    format!(
                        "slot 7 EFS (type SYSV): {} MiB; on IRIX: mount -t efs -o ro \
                         /dev/dsk/dks0d<N>s7 /CDROM",
                        mib(l.efs_sectors * 512)
                    ),
                ],
            }
        }
        CdLayout::MacHfs | CdLayout::MacHfsPlus => {
            let fs = match req.layout {
                CdLayout::MacHfsPlus => MacCdFs::HfsPlus,
                _ => MacCdFs::Hfs,
            };
            let mut opts = MacCdOptions::new(size, req.name.clone(), fs);
            opts.block_size = req.mac_block_size;
            let mut file = create()?;
            let l = write_mac_hfs_cd(&mut file, &opts)?;
            file.set_len(l.disk_bytes)?;
            CdWritten {
                disk_bytes: l.disk_bytes,
                fs_offset: l.hfs_first_sector * 512,
                lines: vec![format!(
                    "Apple Partition Map + {} CD-ROM: Apple_HFS {} MiB, {}-byte allocation blocks",
                    fs.label(),
                    mib(l.hfs_bytes()),
                    l.block_size
                )],
            }
        }
        CdLayout::NextUfs => {
            let mut opts = NextCdOptions::new(size, req.name.clone());
            opts.bytes_per_inode = req.bytes_per_inode;
            // Refuse an impossible size before the output file is created or truncated.
            plan_next_ufs_cd(&opts)?;
            let mut file = create()?;
            let l = write_next_ufs_cd(&mut file, &opts)?;
            let g = &l.geometry;
            CdWritten {
                disk_bytes: l.disk_bytes,
                fs_offset: l.fs_offset,
                lines: vec![format!(
                    "NeXT disk label + 4.3BSD UFS CD-ROM: {} MiB, bsize={} fsize={}, {} \
                     cylinder group(s), {} inodes",
                    mib(l.fs_bytes),
                    g.bsize,
                    g.fsize,
                    g.ncg,
                    g.ncg * g.ipg
                )],
            }
        }
    };
    Ok(written)
}

/// Copy `dir`'s contents onto the blank disc at `path`, its filesystem at `fs_offset`.
pub fn fill(
    path: &Path,
    req: &CdRequest,
    fs_offset: u64,
    dir: &Path,
    progress: &dyn Fn(&ImportStats),
) -> Result<ImportStats> {
    if !dir.is_dir() {
        anyhow::bail!("not a folder: {}", dir.display());
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    let mut fs = crate::fs::open_editable_filesystem(file, fs_offset, 0, None)
        .map_err(|e| anyhow::anyhow!("opening the new disc's volume for writing: {e}"))?;
    let root = fs
        .as_filesystem_mut()
        .root()
        .map_err(|e| anyhow::anyhow!("resolving the volume root: {e}"))?;
    let opts = import_options(req.layout, &req.import);
    let stats = crate::fs::dir_import::import_dir(&mut *fs, &root, dir, &opts, progress)
        .map_err(|e| anyhow::anyhow!("importing {}: {e}", dir.display()))?;
    fs.sync_metadata()
        .map_err(|e| anyhow::anyhow!("writing the volume's metadata: {e}"))?;
    Ok(stats)
}

/// Master `req` into `path`: size it, lay the blank disc, then fill it from its folder.
pub fn build_cd(
    path: &Path,
    req: &CdRequest,
    log: &mut dyn FnMut(&str),
    progress: &dyn Fn(&ImportStats),
) -> Result<CdWritten> {
    if req.name.len() > req.layout.name_limit() {
        log(&format!(
            "Warning: the name is {} bytes; this disc keeps the first {}",
            req.name.len(),
            req.layout.name_limit()
        ));
    }
    let size = resolve_size(req)?;
    if req.size.is_none() {
        log(&format!(
            "Sized to the folder: {} MiB",
            size / (1024 * 1024)
        ));
    }
    let written = write_blank(path, req, size)?;
    log(&format!(
        "Wrote {} ({} MiB)",
        path.display(),
        written.disk_bytes / (1024 * 1024)
    ));
    for line in &written.lines {
        log(line);
    }
    if written.disk_bytes > CD_R_BYTES {
        log("Info: larger than a 700 MB CD-R - fine for an emulator, too big to burn");
    }
    if let Some(dir) = &req.from_dir {
        let stats = fill(path, req, written.fs_offset, dir, progress)?;
        log(&format!(
            "Copied {}: {} files, {} folders, {} bytes",
            dir.display(),
            stats.files,
            stats.dirs_created,
            stats.total_bytes
        ));
        if stats.symlinks_skipped > 0 {
            log(&format!(
                "{} symlink(s) skipped (this filesystem can't store symlinks)",
                stats.symlinks_skipped
            ));
        }
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every layout masters a small disc from a folder, and the files read back off it.
    #[test]
    fn every_layout_masters_a_disc_from_a_folder() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("README"), b"hello disc\n").unwrap();
        std::fs::write(src.join("sub/data"), vec![7u8; 5000]).unwrap();
        for layout in CdLayout::ALL {
            let out = dir.path().join(format!("{layout:?}.iso"));
            let mut req = CdRequest::new(layout);
            req.size = None;
            req.from_dir = Some(src.clone());
            let written = build_cd(&out, &req, &mut |_| {}, &|_| {})
                .unwrap_or_else(|e| panic!("{layout:?}: {e:#}"));
            let file = std::fs::File::open(&out).unwrap();
            let mut fs = crate::fs::open_filesystem(file, written.fs_offset, 0, None)
                .unwrap_or_else(|e| panic!("{layout:?}: reopening: {e}"));
            let root = fs.root().unwrap();
            let names: Vec<String> = fs
                .list_directory(&root)
                .unwrap()
                .into_iter()
                .map(|e| e.name)
                .collect();
            assert!(names.iter().any(|n| n == "README"), "{layout:?}: {names:?}");
            assert!(names.iter().any(|n| n == "sub"), "{layout:?}: {names:?}");
        }
    }

    /// An automatic size with nothing to measure is refused before any file is made.
    #[test]
    fn an_automatic_size_needs_a_folder() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("x.iso");
        let mut req = CdRequest::new(CdLayout::MacHfs);
        req.size = None;
        assert!(build_cd(&out, &req, &mut |_| {}, &|_| {}).is_err());
        assert!(!out.exists());
    }
}
