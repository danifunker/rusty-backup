//! `rb-cli optical new sgi-efs IMG.iso --size 600M [--name LABEL]` — Build an
//! IRIX EFS CD-ROM image: an SGI volume header + partition table at sector 0
//! with the EFS filesystem in **slot 7 typed SYSV** (the IRIX EFS-CD
//! convention; the kernel's `IS_EFS()` accepts SYSV as well as EFS), using CD
//! geometry (1 head × 32 sectors). Mounted on IRIX with
//! `mount -t efs <dev>s7 /CDROM`.
//!
//! Verified against real IRIX 5.3 / 6.5 distribution CDs, which use exactly this
//! shape. Unlike `new hd sgi-efs` (slot 0, hard-disk geometry) or
//! `new volume efs` (a bare headerless EFS), this matches what IRIX expects on
//! optical media. The image is streamed to the file, so even a full CD never
//! materializes in RAM.
//!
//! `--from-dir` formats and then populates in one step, which is the usual way
//! to want it; `--size auto` then sizes the disc to the tree instead of to a
//! media size. Without `--from-dir` the disc comes out blank and `rb-cli
//! import` / `put` fill it.

use anyhow::{Context, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::partition::sgi_hdd_builder::{write_sgi_efs_hdd, SgiHddOptions};
use fs_unix::efs::resolve_bytes_per_inode;

#[derive(Debug, Args)]
pub struct NewSgiCdromArgs {
    /// Image file to create (conventionally `.iso`). Overwritten if it exists.
    pub image: PathBuf,

    /// Disc size (plain bytes or `K`/`M`/`G` suffixes, e.g. `600M`), or `auto`
    /// to size it to `--from-dir` plus filesystem overhead and headroom.
    /// Rounded up to a whole 32-sector CD cylinder. Defaults to 600M (a CD-R);
    /// keep an explicit size at or below your target media (~650-700 MiB for a
    /// CD), and use `auto` for an image you intend to mount rather than burn.
    #[arg(long, default_value = "600M")]
    pub size: String,

    /// Populate the disc from this host directory after formatting it. The
    /// directory's *contents* land at the volume root.
    #[arg(long = "from-dir")]
    pub from_dir: Option<PathBuf>,

    /// With `--from-dir`: unpack archives found in the tree into a directory
    /// named after each, instead of copying them in verbatim. Tar is detected
    /// by content, so IRIX `.tardist` files count; classic Mac archives
    /// (`.sit`, `.cpt`, `.hqx`, …) and ZIP are unpacked too.
    ///
    /// Off by default: a software-distribution disc usually wants its
    /// archives left intact so the target system's installer can consume
    /// them (on IRIX, `inst` reads a `.tardist` as-is).
    #[arg(long = "expand-archives", requires = "from_dir")]
    pub expand_archives: bool,

    /// With `--from-dir`: strip the gzip layer off `.gz` files (a `.tgz` lands as `.tar`).
    /// With `--expand-archives` too, tarballs unpack fully and this takes the other `.gz` files.
    #[arg(long = "expand-gunzip", requires = "from_dir")]
    pub expand_gunzip: bool,

    /// With `--from-dir`: copy the files out of disk images (`.dsk`, `.img`, DiskCopy, `.dmg`,
    /// `.hfv`, ...) into a folder named after each, forks included, instead of the image.
    #[arg(long = "expand-images", requires = "from_dir")]
    pub expand_images: bool,

    /// With `--expand-archives`: unpack each archive into the volume root
    /// rather than a subdirectory named after it, so every archive shares one
    /// root. This is the shape IRIX `inst` wants — point it at one directory
    /// holding every `.tardist`'s product images instead of re-pointing it per
    /// archive. Overlapping entries are then expected (SGI freeware tardists
    /// all ship the same `fw_common*` product), so this skips entries that
    /// already exist unless `--force` is given.
    #[arg(long = "flatten-folders", requires = "expand_archives")]
    pub flatten_folders: bool,

    /// With `--from-dir`: overwrite entries that already exist rather than
    /// skipping them. Only meaningful alongside `--flatten-folders`, where
    /// archives can legitimately carry the same entry.
    #[arg(long, requires = "from_dir")]
    pub force: bool,

    /// With `--from-dir`: ignore the host's Unix mode and ownership.
    #[arg(long = "no-permissions", requires = "from_dir")]
    pub no_permissions: bool,

    /// With `--from-dir`: import macOS AppleDouble sidecars (`._*`) too.
    #[arg(long = "include-appledouble", requires = "from_dir")]
    pub include_appledouble: bool,
    /// Leave macOS `._*` sidecars out entirely: neither copied as files nor taken as a file's
    /// resource fork. For a folder that only has them because it came off a modern Mac.
    #[arg(
        long = "exclude-appledouble",
        requires = "from_dir",
        conflicts_with = "include_appledouble"
    )]
    pub exclude_appledouble: bool,

    /// EFS volume label (up to 6 bytes; longer is truncated). Defaults to
    /// `rusty`.
    #[arg(long, default_value = "rusty")]
    pub name: String,

    /// Approximate total inode count for the EFS filesystem. Mutually exclusive
    /// with `--bytes-per-inode`. Default density is ~1 inode/4 KiB; real IRIX
    /// CDs are sparser (~32 KiB/inode), so pass a larger `--bytes-per-inode` (or
    /// fewer `--inodes`) if you only have a handful of large files.
    #[arg(long, conflicts_with = "bytes_per_inode")]
    pub inodes: Option<u64>,

    /// EFS inode density, in bytes per inode (smaller = more inodes). Floored at
    /// one inode per 512-byte block. Mutually exclusive with `--inodes`.
    #[arg(long)]
    pub bytes_per_inode: Option<u64>,
}

pub fn run(args: NewSgiCdromArgs) -> Result<()> {
    let size_bytes = resolve_disc_size(&args)?;
    let mut opts = SgiHddOptions::new_cdrom(size_bytes, args.name.clone());
    opts.bytes_per_inode = resolve_bytes_per_inode(size_bytes, args.inodes, args.bytes_per_inode);

    // Stream straight to the file: only the volume header + EFS metadata are
    // written; the rest stays sparse, so a full CD never materializes in RAM.
    let mut file = std::fs::File::create(&args.image)
        .with_context(|| format!("creating {}", args.image.display()))?;
    let layout = write_sgi_efs_hdd(&mut file, &opts)
        .with_context(|| format!("writing {}", args.image.display()))?;
    file.set_len(layout.disk_bytes)
        .with_context(|| format!("sizing {}", args.image.display()))?;

    log_stderr(format!(
        "wrote {} ({} bytes, {} MiB) - SGI volume header + EFS CD-ROM",
        args.image.display(),
        layout.disk_bytes,
        layout.disk_bytes / (1024 * 1024),
    ));
    log_stderr(format!(
        "  geometry: {} cyls x {} heads x {} secs/trk x 512 (CD geometry)",
        layout.cylinders, layout.heads, layout.sectors_per_track,
    ));
    log_stderr(format!(
        "  slot 8 VOLHDR: sectors 0..{}   slot 10 VOLUME: sectors 0..{} (whole disc)",
        layout.volhdr_sectors, layout.disk_sectors,
    ));
    log_stderr(format!(
        "  slot 7 EFS (type SYSV): sectors {}..{} ({} MiB)",
        layout.efs_first_sector,
        layout.efs_first_sector + layout.efs_sectors,
        (layout.efs_sectors * 512) / (1024 * 1024),
    ));
    if let Some(dir) = &args.from_dir {
        // Drop the handle before reopening the image through the normal
        // partition-resolution path.
        drop(file);
        populate(&args, dir, layout.efs_first_sector * 512)?;
    } else {
        log_stderr(format!(
            "  populate it with: rb-cli import {} <dir>   (or put ...@1 host/file /file)",
            args.image.display(),
        ));
    }
    log_stderr("  on IRIX: mount -t efs -o ro /dev/dsk/dks0d<N>s7 /CDROM");
    Ok(())
}

/// Resolve `--size`, including `auto` (`model::cd_build` measures `--from-dir`).
fn resolve_disc_size(args: &NewSgiCdromArgs) -> Result<u64> {
    if !args.size.eq_ignore_ascii_case("auto") {
        return parse_size(&args.size).context("parsing --size");
    }
    let size = crate::model::cd_build::resolve_size(&request(args, None))?;
    log_stderr(format!("size auto: {} MiB disc", size / (1024 * 1024)));
    Ok(size)
}

/// The shared disc request these flags describe.
fn request(args: &NewSgiCdromArgs, size: Option<u64>) -> crate::model::cd_build::CdRequest {
    use crate::model::cd_build::{CdImport, CdLayout, CdRequest};
    CdRequest {
        size,
        name: args.name.clone(),
        from_dir: args.from_dir.clone(),
        import: CdImport {
            expand_archives: args.expand_archives,
            expand_gunzip: args.expand_gunzip,
            expand_images: args.expand_images,
            flatten_folders: args.flatten_folders,
            force: args.force,
            no_permissions: args.no_permissions,
            include_appledouble: args.include_appledouble,
            ignore_appledouble: args.exclude_appledouble,
        },
        bytes_per_inode: args.bytes_per_inode,
        ..CdRequest::new(CdLayout::SgiEfs)
    }
}

/// Import `dir` into the freshly-formatted disc's EFS partition at `fs_offset`.
fn populate(args: &NewSgiCdromArgs, dir: &std::path::Path, fs_offset: u64) -> Result<()> {
    let req = request(args, None);
    let stats = crate::model::cd_build::fill(
        &args.image,
        &req,
        fs_offset,
        dir,
        &super::import::progress_cb,
    )?;
    super::import::summarize(dir, "/", &stats, args.expand_archives);
    Ok(())
}
