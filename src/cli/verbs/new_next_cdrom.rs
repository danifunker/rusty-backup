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

    /// With `--from-dir`: unpack tarballs (`.tar`, `.tar.gz`, `.tgz`, pre-POSIX
    /// tars included) into a directory named after each, instead of copying them.
    #[arg(long = "expand-archives", requires = "from_dir")]
    pub expand_archives: bool,

    /// With `--from-dir`: strip one gzip layer only (`x.tar.gz` -> `x.tar`, `f.gz` -> `f`).
    /// With `--expand-archives` too, tarballs unpack fully and this takes the other `.gz` files.
    #[arg(long = "expand-gunzip", requires = "from_dir")]
    pub expand_gunzip: bool,

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
        populate(&args, dir)?;
    } else {
        log_stderr(format!(
            "  populate it with: rb-cli import {} <dir>   (or put ...@1 host/file /file)",
            args.image.display(),
        ));
    }
    Ok(())
}

fn import_options(args: &NewNextCdromArgs) -> crate::fs::dir_import::DirImportOptions {
    crate::fs::dir_import::DirImportOptions {
        shared: super::import::shared_options(
            args.force,
            false,
            args.no_permissions,
            args.include_appledouble,
            args.flatten_folders,
        ),
        expand_archives: args.expand_archives,
        flatten_archives: args.flatten_folders,
        expand_gunzip: args.expand_gunzip,
    }
}

/// Resolve `--size`, including `auto`, which measures `--from-dir` (archives as
/// they will land) and needs it: with nothing to measure it is refused.
fn resolve_disc_size(args: &NewNextCdromArgs) -> Result<u64> {
    if !args.size.eq_ignore_ascii_case("auto") {
        return parse_size(&args.size).context("parsing --size");
    }
    let Some(dir) = &args.from_dir else {
        anyhow::bail!("--size auto needs --from-dir to measure; pass an explicit size instead");
    };
    let (files, dirs, bytes) = crate::fs::dir_import::measure_dir_for(dir, &import_options(args))
        .with_context(|| format!("measuring {}", dir.display()))?;
    // Our UFS writer gives each file's tail a whole 8 KiB block, so budget per block, not per fragment.
    let projected = crate::fs::dir_import::projected_volume_bytes(files, dirs, bytes, 8192)
        + NEXT_CD_FRONT_PORCH * NEXT_CD_SECTOR;
    // Enough inodes for every entry, at the density the disc will be formatted with.
    let probe = plan_next_ufs_cd(&NextCdOptions {
        bytes_per_inode: args.bytes_per_inode,
        ..NextCdOptions::new(projected, "")
    })?;
    let inodes = probe.geometry.ncg * probe.geometry.ipg;
    let wanted = files + dirs + 16;
    let size = if inodes >= wanted {
        projected
    } else {
        projected * wanted.div_ceil(inodes.max(1))
    };
    log_stderr(format!(
        "size auto: {files} file(s), {dirs} dir(s), {} MiB of content{} -> {} MiB disc",
        bytes / (1024 * 1024),
        if args.expand_archives || args.expand_gunzip {
            " (measured as it will land)"
        } else {
            ""
        },
        size / (1024 * 1024),
    ));
    Ok(size)
}

/// Import `dir` into the freshly-formatted disc's UFS partition.
fn populate(args: &NewNextCdromArgs, dir: &std::path::Path) -> Result<()> {
    use crate::cli::resolve::resolve_partition_rw_forced;
    use crate::fs::dir_import::import_dir;

    if !dir.is_dir() {
        anyhow::bail!("--from-dir: not a directory: {}", dir.display());
    }
    // Partition `a` is the disc's only partition; `@1` is how every other verb addresses it.
    let (file, ctx, commit) = resolve_partition_rw_forced(&args.image, Some(1u32.into()), None)?;
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| anyhow::anyhow!("opening the new UFS filesystem for writing: {e}"))?;
    let dest = crate::fs::filesystem::Filesystem::root(fs.as_filesystem_mut())
        .map_err(|e| anyhow::anyhow!("resolving volume root: {e}"))?;

    let stats = import_dir(
        &mut *fs,
        &dest,
        dir,
        &import_options(args),
        &super::import::progress_cb,
    )
    .map_err(|e| anyhow::anyhow!("importing {}: {e}", dir.display()))?;
    fs.sync_metadata()
        .map_err(|e| anyhow::anyhow!("sync_metadata: {e}"))?;
    commit.commit()?;

    super::import::summarize(dir, "/", &stats, args.expand_archives);
    Ok(())
}
