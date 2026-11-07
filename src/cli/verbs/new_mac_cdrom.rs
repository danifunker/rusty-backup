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
    /// archives (`.sit`, `.sea`, `.cpt`, `.hqx`, `.mar`, Mac `.zip`) land with both forks
    /// and their Finder type/creator intact — a disc the target Mac can run
    /// straight off, rather than one it has to unstuff first. Tar archives are
    /// unpacked too. `--size auto` measures what they unpack to.
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

    /// With `--from-dir`: overwrite entries that already exist rather than
    /// failing on the collision.
    #[arg(long, requires = "from_dir")]
    pub force: bool,

    /// With `--from-dir`: import macOS AppleDouble sidecars (`._*`) too. Off by
    /// default because the resource fork they carry is imported into the Mac
    /// volume's own resource fork instead.
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
        populate(&args, fs, dir, layout.hfs_first_sector * 512)?;
    } else {
        log_stderr(format!(
            "  populate it with: rb-cli import {} <dir>   (or put ...@1 host/file /file)",
            args.image.display(),
        ));
    }
    Ok(())
}

/// Resolve `--size`, including `auto` (`model::cd_build` measures `--from-dir`).
fn resolve_disc_size(args: &NewMacCdromArgs, fs: MacCdFs) -> Result<u64> {
    if !args.size.eq_ignore_ascii_case("auto") {
        return parse_size(&args.size).context("parsing --size");
    }
    let size = crate::model::cd_build::resolve_size(&request(args, fs))?;
    log_stderr(format!("size auto: {} MiB disc", size / (1024 * 1024)));
    Ok(size)
}

/// The shared disc request these flags describe; HFS keeps no Unix permissions.
fn request(args: &NewMacCdromArgs, fs: MacCdFs) -> crate::model::cd_build::CdRequest {
    use crate::model::cd_build::{CdImport, CdLayout, CdRequest};
    let layout = match fs {
        MacCdFs::Hfs => CdLayout::MacHfs,
        MacCdFs::HfsPlus => CdLayout::MacHfsPlus,
    };
    CdRequest {
        size: None,
        name: args.name.clone(),
        from_dir: args.from_dir.clone(),
        import: CdImport {
            expand_archives: args.expand_archives,
            expand_gunzip: args.expand_gunzip,
            expand_images: args.expand_images,
            force: args.force,
            include_appledouble: args.include_appledouble,
            ignore_appledouble: args.exclude_appledouble,
            ..CdImport::default()
        },
        mac_block_size: args.block_size,
        ..CdRequest::new(layout)
    }
}

/// Import `dir` into the freshly-formatted disc's Mac volume at `fs_offset`.
fn populate(
    args: &NewMacCdromArgs,
    fs: MacCdFs,
    dir: &std::path::Path,
    fs_offset: u64,
) -> Result<()> {
    let stats = crate::model::cd_build::fill(
        &args.image,
        &request(args, fs),
        fs_offset,
        dir,
        &super::import::progress_cb,
    )?;
    super::import::summarize(dir, "/", &stats, args.expand_archives);
    Ok(())
}
