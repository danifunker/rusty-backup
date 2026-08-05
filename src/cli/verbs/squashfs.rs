//! `rb-cli squashfs <plan|put|rm>` — SquashFS edits with an explicit size
//! budget.
//!
//! SquashFS is the one filesystem here with no in-place write: committing an
//! edit rebuilds the whole image, so its size afterwards is not knowable until
//! the rebuild has run, and the image usually lives somewhere with a fixed size
//! (`docs/squashfs_edit.md` §2). That makes "how large may the result be?" a
//! question only the user can answer, and one the ordinary `put` / `rm` verbs
//! have no place asking — hence a verb group of its own rather than SquashFS-
//! only flags bolted onto every edit verb.
//!
//! The edits themselves are the plain verbs: `squashfs put` flattens
//! [`super::put::PutArgs`] and `squashfs rm` flattens [`super::rm::RmArgs`], so
//! there is exactly one implementation of each operation and the budget rides
//! alongside rather than forking it.

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};

use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::parse::parse_size;
use crate::cli::resolve::resolve_partition_streaming;
use crate::fs::squashfs_edit::{plan_size, SizeBudget};
use crate::partition::format_size;

#[derive(Debug, Subcommand)]
pub enum SquashfsCommand {
    /// Report what the image occupies, what it may grow into, and how well it
    /// compressed — the numbers a size budget is chosen from. Writes nothing.
    Plan(SquashfsPlanArgs),
    /// Copy a host file into the image, rebuilding it within a size budget.
    Put(SquashfsPutArgs),
    /// Delete a path from the image, rebuilding it within a size budget.
    Rm(SquashfsRmArgs),
    /// Build a new SquashFS image from a host directory, as `mksquashfs` does.
    Create(SquashfsCreateArgs),
    /// Structurally verify an image: walk every inode, dirent and data block,
    /// decompressing everything. Not an fsck — SquashFS has no checksums, so
    /// this finds broken structure, not altered content.
    Verify(SquashfsVerifyArgs),
}

/// `--size` / `--grow`, shared by the editing subcommands.
#[derive(Debug, Args)]
pub struct BudgetArgs {
    /// Ceiling on the rebuilt image, e.g. `512M`. `fit` accepts whatever the
    /// rebuild produces. Omitted, the container decides: a bare `.squashfs`
    /// grows freely, a partition-hosted image may not outgrow its partition.
    #[arg(long, value_name = "SIZE|fit", conflicts_with = "grow")]
    pub size: Option<String>,
    /// Allow the rebuilt image to exceed its *current* size by at most this
    /// much, e.g. `64M`. Resolved against the image once it is opened.
    #[arg(long, value_name = "SIZE")]
    pub grow: Option<String>,
}

impl BudgetArgs {
    /// `None` when the user asked for nothing, leaving the container to bind.
    fn to_budget(&self) -> Result<Option<SizeBudget>> {
        match (&self.size, &self.grow) {
            (None, None) => Ok(None),
            (Some(s), None) if s.eq_ignore_ascii_case("fit") => Ok(Some(SizeBudget::Fit)),
            (Some(s), None) => Ok(Some(SizeBudget::Limit(parse_size(s)?))),
            (None, Some(g)) => Ok(Some(SizeBudget::Grow(parse_size(g)?))),
            // clap's conflicts_with rejects both; belt and braces.
            (Some(_), Some(_)) => bail!("pass either --size or --grow, not both"),
        }
    }
}

#[derive(Debug, Args)]
pub struct SquashfsPlanArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
}

#[derive(Debug, Args)]
pub struct SquashfsPutArgs {
    #[command(flatten)]
    pub put: super::put::PutArgs,
    #[command(flatten)]
    pub budget: BudgetArgs,
}

#[derive(Debug, Args)]
pub struct SquashfsRmArgs {
    #[command(flatten)]
    pub rm: super::rm::RmArgs,
    #[command(flatten)]
    pub budget: BudgetArgs,
}

#[derive(Debug, Args)]
pub struct SquashfsCreateArgs {
    /// Host directory to pack. Its *contents* become the image root, like
    /// `mksquashfs DIR IMAGE` (the directory itself is not a top-level entry).
    pub dir: std::path::PathBuf,
    /// Image file to create. Overwritten if it already exists.
    pub image: std::path::PathBuf,
    /// Compressor for the new image. `mksquashfs` defaults to gzip; we match it.
    #[arg(long, value_enum, default_value = "gzip")]
    pub compressor: CreateCompressor,
    /// Data block size (a power of two, 4 KiB..=1 MiB). `mksquashfs`'s default
    /// is 128 KiB.
    #[arg(long = "block-size", default_value = "128K")]
    pub block_size: String,
}

/// The compressors the writer can emit (a subset of what the reader accepts —
/// LZMA / LZ4 / LZO have no mature pure-Rust encoder; see decision D5).
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum CreateCompressor {
    Gzip,
    Xz,
    Zstd,
}

#[derive(Debug, Args)]
pub struct SquashfsVerifyArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,
}

pub fn run(cmd: SquashfsCommand) -> Result<()> {
    match cmd {
        SquashfsCommand::Plan(args) => run_plan(args),
        SquashfsCommand::Put(args) => {
            super::put::run_with_budget(args.put, args.budget.to_budget()?)
        }
        SquashfsCommand::Rm(args) => super::rm::run_with_budget(args.rm, args.budget.to_budget()?),
        SquashfsCommand::Create(args) => run_create(args),
        SquashfsCommand::Verify(args) => run_verify(args),
    }
}

fn run_create(args: SquashfsCreateArgs) -> Result<()> {
    use crate::fs::squashfs::Compressor;
    use crate::fs::squashfs_write::{write_squashfs, BuildNode, BuildOptions};

    if !args.dir.is_dir() {
        bail!("{} is not a directory", args.dir.display());
    }
    let block_size = parse_size(&args.block_size)? as u32;
    let compressor = match args.compressor {
        CreateCompressor::Gzip => Compressor::Gzip,
        CreateCompressor::Xz => Compressor::Xz,
        CreateCompressor::Zstd => Compressor::Zstd,
    };
    let opts = BuildOptions {
        compressor,
        block_size,
        mod_time: 0,
        use_fragments: true,
    };

    log_stderr(format!("Scanning {}...", args.dir.display()));
    let tree = BuildNode::from_host_dir(&args.dir)
        .map_err(|e| anyhow!("reading {}: {e}", args.dir.display()))?;

    // Write to a sibling temp and rename, so an interrupted build never leaves a
    // half-written image where the user might mistake it for a finished one —
    // the same discipline the in-place editor commits with.
    let dir = args
        .image
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let mut tmp = tempfile::Builder::new()
        .prefix(".rb-mksquashfs-")
        .tempfile_in(dir)
        .with_context(|| format!("creating a temp beside {}", args.image.display()))?;
    log_stderr(format!("Building {} image...", compressor.name()));
    let bytes_used = write_squashfs(tmp.as_file_mut(), &tree, &opts)
        .map_err(|e| anyhow!("writing squashfs: {e}"))?;
    tmp.as_file().sync_all().ok();
    tmp.persist(&args.image)
        .map_err(|e| anyhow!("finishing {}: {e}", args.image.display()))?;

    log_stderr(format!(
        "Wrote {} ({})",
        args.image.display(),
        format_size(bytes_used)
    ));
    Ok(())
}

fn run_verify(args: SquashfsVerifyArgs) -> Result<()> {
    let (reader, ctx) =
        resolve_partition_streaming(&args.image.path, args.image.partition.clone())?;
    log_stderr(&ctx.label);
    // Open the reader concretely rather than through the trait object, because
    // the verifier drives the SquashFS reader's own traversal.
    let fs_type = {
        let probe = crate::fs::open_filesystem(
            reader,
            ctx.offset,
            ctx.type_byte,
            ctx.type_string.as_deref(),
        )
        .map_err(|e| anyhow!("opening filesystem: {e}"))?;
        probe.fs_type().to_string()
    };
    if fs_type != "SquashFS" {
        bail!(
            "squashfs verify: {} is a {fs_type} volume, not SquashFS",
            args.image.path.display()
        );
    }

    // Reopen concretely for the verifier.
    let (reader, _ctx) =
        resolve_partition_streaming(&args.image.path, args.image.partition.clone())?;
    let mut fs = crate::fs::squashfs::SquashfsFilesystem::open(reader, ctx.offset)
        .map_err(|e| anyhow!("opening SquashFS: {e}"))?;
    let report =
        crate::fs::squashfs_verify::verify_squashfs(&mut fs).map_err(|e| anyhow!("{e}"))?;

    out_stdout(format!(
        "OK: {} file(s), {} dir(s), {} symlink(s), {} special; {} of data, {} xattr(s) — every block decompressed and cross-referenced",
        report.files,
        report.directories,
        report.symlinks,
        report.special,
        format_size(report.data_bytes),
        report.xattrs,
    ));
    Ok(())
}

fn run_plan(args: SquashfsPlanArgs) -> Result<()> {
    let (reader, ctx) =
        resolve_partition_streaming(&args.image.path, args.image.partition.clone())?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_filesystem(
        reader,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem: {e}"))?;
    if fs.fs_type() != "SquashFS" {
        bail!(
            "squashfs plan: {} is a {} volume, not SquashFS",
            args.image.path.display(),
            fs.fs_type()
        );
    }

    // A partition bounds the image; a bare file at byte 0 is the image and
    // simply grows, so it has no capacity to report.
    let capacity = (ctx.offset != 0 && ctx.size > 0).then_some(ctx.size);
    // `total_size` is the superblock's `bytes_used`; pad it the way the image
    // actually sits on disk, so this figure and the one the budget is enforced
    // against are the same number.
    let image_len = crate::fs::squashfs_write::image_footprint(fs.total_size());
    let plan = plan_size(fs.as_mut(), image_len, capacity)
        .map_err(|e| anyhow!("measuring the image: {e}"))?;

    out_stdout(format!("Image size:      {}", format_size(plan.image_len)));
    out_stdout(format!(
        "File content:    {} uncompressed",
        format_size(plan.content_len)
    ));
    out_stdout(format!("Compressed to:   {}", plan.describe_ratio()));
    match (plan.capacity, plan.headroom) {
        (Some(cap), Some(head)) => {
            out_stdout(format!("Room available:  {}", format_size(cap)));
            out_stdout(format!("Headroom:        {} unused", format_size(head)));
        }
        _ => {
            out_stdout("Room available:  unbounded (a bare .squashfs file is the filesystem)");
        }
    }
    // The point of the numbers above: turning them into a budget.
    out_stdout(String::new());
    match plan.added_content_ratio() {
        Some(r) => out_stdout(format!(
            "Adding roughly N bytes of similar content would add about {:.0}% of N \
             to the image.",
            r * 100.0
        )),
        None => out_stdout("Too little file content here to predict from; budget generously."),
    }
    out_stdout(
        "Set a ceiling with `--size 512M` (absolute), `--grow 64M` (headroom), \
         or `--size fit`.",
    );
    Ok(())
}
