//! `rb-cli optical new next-ufs IMG.iso --size 600M [--name LABEL]` — Build a
//! NeXTSTEP / OPENSTEP CD-ROM image: a NeXT `dlV3` disk label in 2048-byte
//! sectors with one `4.3BSD` UFS partition, the shape every NeXTSTEP 3.x CISC
//! (m68k + Intel) distribution disc has. Not ISO 9660 — NeXT never used it.
//!
//! The layout lives in [`crate::partition::next_cd_builder`], pinned to the
//! NeXTSTEP 3.3 CDs. The image is streamed, so a full disc never sits in RAM.
//!
//! `--from-dir` formats and populates in one step; `--size auto` then sizes the
//! disc to the tree. Two archive depths, both off by default because NeXT's
//! Installer wants `.pkg` folders (and their `.tar.Z`) left alone:
//! `--expand-archives` unpacks tarballs fully, `--expand-gunzip` only strips the
//! gzip layer (`x.tar.gz` -> `x.tar`). The disc is a data disc: no boot blocks.

use anyhow::{Context, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::partition::next_cd_builder::{
    plan_next_ufs_cd, write_next_ufs_cd, NextCdOptions, NEXT_CD_FRONT_PORCH, NEXT_CD_SECTOR,
};

/// Bytes a 700 MB CD-R holds; past this the image is for an emulator, not a burner.
const CD_R_BYTES: u64 = 360_000 * 2048;
/// `dl_label` is 24 bytes including its NUL.
const LABEL_MAX: usize = 23;

#[derive(Debug, Args)]
pub struct NewNextCdromArgs {
    /// Image file to create (conventionally `.iso`). Overwritten if it exists.
    pub image: PathBuf,

    /// Disc size (`600M`, `650M`, ...) or `auto` to fit `--from-dir`; rounded up to a 2048-byte
    /// sector. Past ~700M suits an emulator, not a burner.
    #[arg(long, default_value = "600M")]
    pub size: String,

    /// Populate the disc from this host directory after formatting it. The
    /// directory's *contents* land at the volume root.
    #[arg(long = "from-dir")]
    pub from_dir: Option<PathBuf>,

    /// With `--from-dir`: unpack tarballs (`.tar`, `.tar.gz`, `.tgz`, `.tar.bz2`, pre-POSIX
    /// tars included), ZIP and classic Mac archives into a directory named after each.
    #[arg(long = "expand-archives", requires = "from_dir")]
    pub expand_archives: bool,

    /// With `--from-dir`: strip one gzip layer only (`x.tar.gz` -> `x.tar`, `f.gz` -> `f`).
    /// With `--expand-archives` too, tarballs unpack fully and this takes the other `.gz` files.
    #[arg(long = "expand-gunzip", requires = "from_dir")]
    pub expand_gunzip: bool,

    /// With `--from-dir`: copy the files out of disk images (`.dsk`, `.img`, DiskCopy, `.dmg`,
    /// `.hfv`, ...) into a folder named after each, forks included, instead of the image.
    #[arg(long = "expand-images", requires = "from_dir")]
    pub expand_images: bool,

    /// With `--expand-archives`: unpack every archive into the volume root, not one folder each.
    /// Entries that already exist are skipped unless `--force` is given.
    #[arg(long = "flatten-folders", requires = "expand_archives")]
    pub flatten_folders: bool,

    /// With `--from-dir`: overwrite entries that already exist rather than
    /// skipping them. Only meaningful alongside `--flatten-folders`.
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

    /// Disc name NeXTSTEP shows (`dl_label`, up to 23 bytes; longer is truncated).
    #[arg(long, default_value = "rusty-backup")]
    pub name: String,

    /// UFS inode density in bytes per inode (smaller = more inodes). Defaults to
    /// 4096, the density NeXT's own CDs use.
    #[arg(long = "bytes-per-inode")]
    pub bytes_per_inode: Option<u64>,
}

pub fn run(args: NewNextCdromArgs) -> Result<()> {
    if args.name.len() > LABEL_MAX {
        log_stderr(format!(
            "Warning: --name is {} bytes; the NeXT label keeps the first {LABEL_MAX}",
            args.name.len()
        ));
    }
    let mut opts = NextCdOptions::new(resolve_disc_size(&args)?, args.name.clone());
    opts.bytes_per_inode = args.bytes_per_inode;
    // Refuse an impossible size before the output file is created or truncated.
    plan_next_ufs_cd(&opts)?;

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&args.image)
        .with_context(|| format!("creating {}", args.image.display()))?;
    let layout = write_next_ufs_cd(&mut file, &opts)
        .with_context(|| format!("writing {}", args.image.display()))?;

    let g = &layout.geometry;
    log_stderr(format!(
        "wrote {} ({} bytes, {} MiB) - NeXT disk label + 4.3BSD UFS CD-ROM",
        args.image.display(),
        layout.disk_bytes,
        layout.disk_bytes / (1024 * 1024),
    ));
    log_stderr(format!(
        "  label: {NEXT_CD_SECTOR}-byte sectors, {NEXT_CD_FRONT_PORCH}-sector front porch; \
         partition a (4.3BSD) at byte {} ({} MiB)",
        layout.fs_offset,
        layout.fs_bytes / (1024 * 1024),
    ));
    log_stderr(format!(
        "  UFS: bsize={} fsize={} {} cylinder group(s), {} inodes",
        g.bsize,
        g.fsize,
        g.ncg,
        g.ncg * g.ipg,
    ));
    if layout.disk_bytes > CD_R_BYTES {
        log_stderr("  Info: larger than a 700 MB CD-R - fine for an emulator, too big to burn");
    }
    if let Some(dir) = &args.from_dir {
        drop(file);
        populate(&args, dir, layout.fs_offset)?;
    } else {
        log_stderr(format!(
            "  populate it with: rb-cli import {} <dir>   (or put ...@1 host/file /file)",
            args.image.display(),
        ));
    }
    Ok(())
}

/// Resolve `--size`, including `auto` (`model::cd_build` measures `--from-dir`).
fn resolve_disc_size(args: &NewNextCdromArgs) -> Result<u64> {
    if !args.size.eq_ignore_ascii_case("auto") {
        return parse_size(&args.size).context("parsing --size");
    }
    let size = crate::model::cd_build::resolve_size(&request(args))?;
    log_stderr(format!("size auto: {} MiB disc", size / (1024 * 1024)));
    Ok(size)
}

/// The shared disc request these flags describe.
fn request(args: &NewNextCdromArgs) -> crate::model::cd_build::CdRequest {
    use crate::model::cd_build::{CdImport, CdLayout, CdRequest};
    CdRequest {
        size: None,
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
        ..CdRequest::new(CdLayout::NextUfs)
    }
}

/// Import `dir` into the freshly-formatted disc's UFS at `fs_offset`.
fn populate(args: &NewNextCdromArgs, dir: &std::path::Path, fs_offset: u64) -> Result<()> {
    let stats = crate::model::cd_build::fill(
        &args.image,
        &request(args),
        fs_offset,
        dir,
        &super::import::progress_cb,
    )?;
    super::import::summarize(dir, "/", &stats, args.expand_archives);
    Ok(())
}
