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
use crate::fs::efs::resolve_bytes_per_inode;
use crate::partition::sgi_hdd_builder::{write_sgi_efs_hdd, SgiHddOptions};

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

    /// With `--from-dir`: unpack tar archives found in the tree into a
    /// directory named after each, instead of copying them in verbatim.
    /// Detected by content, so IRIX `.tardist` files count.
    ///
    /// Off by default: a software-distribution disc usually wants its
    /// archives left intact so the target system's installer can consume
    /// them (on IRIX, `inst` reads a `.tardist` as-is).
    #[arg(long = "expand-archives", requires = "from_dir")]
    pub expand_archives: bool,

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
        populate(&args, dir)?;
    } else {
        log_stderr(format!(
            "  populate it with: rb-cli import {} <dir>   (or put ...@1 host/file /file)",
            args.image.display(),
        ));
    }
    log_stderr("  on IRIX: mount -t efs -o ro /dev/dsk/dks0d<N>s7 /CDROM");
    Ok(())
}

/// Resolve `--size`, including `auto` (measure `--from-dir` and project what
/// the volume needs). `auto` without `--from-dir` has nothing to measure and
/// is rejected rather than silently defaulting.
fn resolve_disc_size(args: &NewSgiCdromArgs) -> Result<u64> {
    if !args.size.eq_ignore_ascii_case("auto") {
        return parse_size(&args.size).context("parsing --size");
    }
    let Some(dir) = &args.from_dir else {
        anyhow::bail!("--size auto needs --from-dir to measure; pass an explicit size instead");
    };
    let (files, dirs, bytes) =
        crate::fs::dir_import::measure_dir(dir, args.expand_archives, args.flatten_folders)
            .with_context(|| format!("measuring {}", dir.display()))?;
    // EFS allocates in 512-byte blocks.
    let projected = crate::fs::dir_import::projected_volume_bytes(files, dirs, bytes, 512);
    log_stderr(format!(
        "size auto: {files} file(s), {dirs} dir(s), {} MiB of content{} -> {} MiB disc",
        bytes / (1024 * 1024),
        if args.expand_archives {
            " (archives measured expanded)"
        } else {
            ""
        },
        projected / (1024 * 1024),
    ));
    Ok(projected)
}

/// Import `dir` into the freshly-formatted disc's EFS partition.
fn populate(args: &NewSgiCdromArgs, dir: &std::path::Path) -> Result<()> {
    use crate::cli::resolve::resolve_partition_rw_forced;
    use crate::fs::dir_import::{import_dir, DirImportOptions};

    if !dir.is_dir() {
        anyhow::bail!("--from-dir: not a directory: {}", dir.display());
    }
    // The EFS filesystem is the disc's only mountable partition; `@1` is how
    // every other verb addresses it.
    let (file, ctx, commit) = resolve_partition_rw_forced(&args.image, Some(1u32.into()), None)?;
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| anyhow::anyhow!("opening the new EFS filesystem for writing: {e}"))?;
    let dest = crate::fs::filesystem::Filesystem::root(fs.as_filesystem_mut())
        .map_err(|e| anyhow::anyhow!("resolving volume root: {e}"))?;

    let opts = DirImportOptions {
        shared: super::import::shared_options(
            args.force,
            false,
            args.no_permissions,
            args.include_appledouble,
            args.flatten_folders,
        ),
        expand_archives: args.expand_archives,
        flatten_archives: args.flatten_folders,
    };
    let stats = import_dir(&mut *fs, &dest, dir, &opts, &super::import::progress_cb)
        .map_err(|e| anyhow::anyhow!("importing {}: {e}", dir.display()))?;
    fs.sync_metadata()
        .map_err(|e| anyhow::anyhow!("sync_metadata: {e}"))?;
    commit.commit()?;

    super::import::summarize(dir, "/", &stats, args.expand_archives);
    Ok(())
}
