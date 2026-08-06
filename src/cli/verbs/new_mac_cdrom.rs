//! `rb-cli optical new {mac-hfs|mac-hfsplus} IMG.iso --size 600M [--name LABEL]`
//! — Build a classic-Mac CD-ROM image: an Apple Partition Map at block 0 with
//! one `Apple_HFS` partition holding a blank HFS or HFS+ volume. That is what a
//! Mac CD-ROM is on the wire — the Apple CD-ROM driver presents 512-byte
//! logical blocks to the ROM, so the map uses 512-byte blocks even though the
//! medium's sectors are 2048 bytes.
//!
//! No ISO 9660 volume descriptor is written, so the disc is Mac-only (a hybrid
//! HFS / ISO 9660 disc needs an ISO 9660 mastering engine, which this tool
//! doesn't have). HFS+ discs are readable by Mac OS 8.1 and later; pick
//! `mac-hfs` for anything older.
//!
//! Same shape as `optical new sgi-efs`: `--from-dir` formats and populates in
//! one step, `--size auto` then sizes the disc to the tree, and the image is
//! streamed to the file so a full CD never materializes in RAM. Without
//! `--from-dir` the disc comes out blank and `rb-cli import` / `put` fill it.

use anyhow::{Context, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::partition::mac_cd_builder::{write_mac_hfs_cd, MacCdFs, MacCdOptions};

#[derive(Debug, Args)]
pub struct NewMacCdromArgs {
    /// Image file to create (conventionally `.iso`). Overwritten if it exists.
    pub image: PathBuf,

    /// Disc size (plain bytes or `K`/`M`/`G` suffixes, e.g. `600M`), or `auto`
    /// to size it to `--from-dir` plus filesystem overhead and headroom.
    /// Defaults to 600M (a CD-R); keep an explicit size at or below your target
    /// media (~650-700 MiB for a CD), and use `auto` for an image you intend to
    /// mount rather than burn.
    #[arg(long, default_value = "600M")]
    pub size: String,

    /// Populate the disc from this host directory after formatting it. The
    /// directory's *contents* land at the volume root.
    #[arg(long = "from-dir")]
    pub from_dir: Option<PathBuf>,

    /// With `--from-dir`: unpack archives found in the tree into a directory
    /// named after each, instead of copying them in verbatim. Classic Mac
    /// archives (`.sit`, `.sea`, `.cpt`, `.hqx`, `.mar`) land with both forks
    /// and their Finder type/creator intact — a disc the target Mac can run
    /// straight off, rather than one it has to unstuff first. Tar archives are
    /// unpacked too. `--size auto` measures what they unpack to.
    #[arg(long = "expand-archives", requires = "from_dir")]
    pub expand_archives: bool,

    /// With `--from-dir`: overwrite entries that already exist rather than
    /// failing on the collision.
    #[arg(long, requires = "from_dir")]
    pub force: bool,

    /// With `--from-dir`: import macOS AppleDouble sidecars (`._*`) too. Off by
    /// default because the resource fork they carry is imported into the Mac
    /// volume's own resource fork instead.
    #[arg(long = "include-appledouble", requires = "from_dir")]
    pub include_appledouble: bool,

    /// Volume name, as it appears on the Mac desktop. HFS truncates at 27 Mac
    /// Roman bytes. Defaults to `rusty-backup`.
    #[arg(long, default_value = "rusty-backup")]
    pub name: String,

    /// Allocation block size in bytes. HFS wants a multiple of 512 and auto-
    /// picks the smallest that keeps the volume inside its 65535-block ceiling;
    /// HFS+ wants a power of two in [512, 4096] and defaults to 4096.
    #[arg(long = "block-size")]
    pub block_size: Option<u32>,
}

pub fn run(fs: MacCdFs, args: NewMacCdromArgs) -> Result<()> {
    let size_bytes = resolve_disc_size(&args, fs)?;
    let mut opts = MacCdOptions::new(size_bytes, args.name.clone(), fs);
    opts.block_size = args.block_size;

    // Stream straight to the file: only the partition map and the volume's
    // metadata regions are written, so the free space stays a sparse hole and a
    // full CD never materializes in RAM.
    let mut file = std::fs::File::create(&args.image)
        .with_context(|| format!("creating {}", args.image.display()))?;
    let layout = write_mac_hfs_cd(&mut file, &opts)
        .with_context(|| format!("writing {}", args.image.display()))?;
    file.set_len(layout.disk_bytes)
        .with_context(|| format!("sizing {}", args.image.display()))?;

    log_stderr(format!(
        "wrote {} ({} bytes, {} MiB) - Apple Partition Map + {} CD-ROM",
        args.image.display(),
        layout.disk_bytes,
        layout.disk_bytes / (1024 * 1024),
        fs.label(),
    ));
    log_stderr("  block 0 DDR + blocks 1..3 partition map (512-byte blocks)");
    log_stderr(format!(
        "  Apple_HFS partition: sectors {}..{} ({} MiB, {}-byte allocation blocks)",
        layout.hfs_first_sector,
        layout.hfs_first_sector + layout.hfs_sectors,
        layout.hfs_bytes() / (1024 * 1024),
        layout.block_size,
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
    Ok(())
}

/// Resolve `--size`, including `auto` (measure `--from-dir` and project what
/// the volume needs). `auto` without `--from-dir` has nothing to measure and
/// is rejected rather than silently defaulting.
fn resolve_disc_size(args: &NewMacCdromArgs, fs: MacCdFs) -> Result<u64> {
    if !args.size.eq_ignore_ascii_case("auto") {
        return parse_size(&args.size).context("parsing --size");
    }
    let Some(dir) = &args.from_dir else {
        anyhow::bail!("--size auto needs --from-dir to measure; pass an explicit size instead");
    };
    let (files, dirs, bytes) = crate::fs::dir_import::measure_dir(dir, args.expand_archives, false)
        .with_context(|| format!("measuring {}", dir.display()))?;
    // Project twice: HFS's allocation block scales with the volume, and on a
    // disc-sized volume it is what drives per-file slack, so a 512-byte first
    // pass only exists to pick the block size for the real one.
    let block = match fs {
        MacCdFs::Hfs => {
            let rough = crate::fs::dir_import::projected_volume_bytes(files, dirs, bytes, 512);
            crate::fs::hfs::pick_block_size(rough) as u64
        }
        MacCdFs::HfsPlus => 4096,
    };
    let projected = crate::fs::dir_import::projected_volume_bytes(files, dirs, bytes, block);
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

/// Import `dir` into the freshly-formatted disc's Mac volume.
fn populate(args: &NewMacCdromArgs, dir: &std::path::Path) -> Result<()> {
    use crate::cli::resolve::resolve_partition_rw_forced;
    use crate::fs::dir_import::{import_dir, DirImportOptions};

    if !dir.is_dir() {
        anyhow::bail!("--from-dir: not a directory: {}", dir.display());
    }
    // The Mac volume is the disc's only mountable partition; `@1` is how every
    // other verb addresses it.
    let (file, ctx, commit) = resolve_partition_rw_forced(&args.image, Some(1u32.into()), None)?;
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| anyhow::anyhow!("opening the new Mac volume for writing: {e}"))?;
    let dest = crate::fs::filesystem::Filesystem::root(fs.as_filesystem_mut())
        .map_err(|e| anyhow::anyhow!("resolving volume root: {e}"))?;

    let opts = DirImportOptions {
        // Host Unix modes are never applied: HFS and HFS+ have no Unix
        // permissions to receive them, which is why there is no flag for it.
        shared: super::import::shared_options(
            args.force,
            false,
            true,
            args.include_appledouble,
            false,
        ),
        expand_archives: args.expand_archives,
        flatten_archives: false,
    };
    let stats = import_dir(&mut *fs, &dest, dir, &opts, &super::import::progress_cb)
        .map_err(|e| anyhow::anyhow!("importing {}: {e}", dir.display()))?;
    fs.sync_metadata()
        .map_err(|e| anyhow::anyhow!("sync_metadata: {e}"))?;
    commit.commit()?;

    super::import::summarize(dir, "/", &stats, args.expand_archives);
    Ok(())
}
