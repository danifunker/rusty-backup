//! `rb-cli put` — copy a host file (or zero-fill) into a filesystem.
//!
//! Three shapes:
//! - `put IMG[@N] HOST DST [opts]` — cp-like copy.
//! - `put IMG[@N] --zero BYTES --dst DST [opts]` — pre-allocate zero
//!   bytes (the `--dst` flag avoids positional ambiguity).
//! - `put IMG[@N] --boot BB_FILE` — write the 1024-byte boot block
//!   region of the image verbatim. HFS-specific; other filesystems
//!   return an error.
//!
//! `--type` / `--creator` apply only to filesystems that carry per-file
//! type/creator codes (HFS, HFS+, ProDOS); on other filesystems the
//! flags are accepted but ignored with a warning.

use anyhow::{anyhow, bail, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::parse::{split_mac_path, ZeroReader};
use crate::cli::resolve::resolve_partition_rw;
use crate::fs::filesystem::CreateFileOptions;

#[derive(Debug, Args)]
pub struct PutArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Host file to copy. Required when not using `--zero` or `--boot`.
    pub host_file: Option<PathBuf>,

    /// Destination path inside the filesystem (cp-like positional).
    pub dst: Option<String>,

    /// Pre-allocate N zero bytes instead of copying a host file. Pair
    /// with `--dst`.
    #[arg(long, conflicts_with_all = ["host_file", "boot"])]
    pub zero: Option<u64>,

    /// Explicit destination flag; use this with `--zero` where the
    /// positional `DST` slot is awkward.
    #[arg(long = "dst", conflicts_with_all = ["dst", "boot"])]
    pub dst_flag: Option<String>,

    /// Write the 1024-byte boot-block region of the image verbatim.
    /// HFS-only today.
    #[arg(long, conflicts_with_all = ["host_file", "dst", "dst_flag", "zero", "type_code", "creator", "force"])]
    pub boot: Option<PathBuf>,

    /// 4-character type code (HFS / HFS+ / ProDOS). Defaults to `BINA`,
    /// or `[put] type` from the config file when set.
    #[arg(long = "type")]
    pub type_code: Option<String>,

    /// 4-character creator code (HFS / HFS+ only). Defaults to `????`,
    /// or `[put] creator` from the config file when set.
    #[arg(long)]
    pub creator: Option<String>,

    /// Overwrite an existing entry at the destination path.
    #[arg(long)]
    pub force: bool,

    /// After writing the file, also print the same JSON envelope
    /// `locate` would have produced — absolute byte offset, length,
    /// fragmented flag. One-shot for build scripts that need to patch
    /// disk offsets immediately after placing a payload. HFS-only,
    /// matches the locate verb's scope; ignored (with a warning) for
    /// the `--zero` and `--boot` shapes since there's no host file to
    /// describe.
    #[arg(long = "print-offset")]
    pub print_offset: bool,
}

pub fn run(args: PutArgs) -> Result<()> {
    if let Some(bb_file) = args.boot {
        // Boot-block write is a raw byte-0 sector smash — keep the
        // existing HFS-only path. Phase D may generalize this to FAT
        // BPB / NTFS boot loader as needed.
        return crate::cli::api::hfs::cmd_put_boot(args.image.path, bb_file);
    }

    let dst = match (args.dst, args.dst_flag) {
        (Some(d), None) | (None, Some(d)) => d,
        (None, None) => bail!(
            "destination path required (positional DST or --dst PATH; or pass --boot for boot blocks)"
        ),
        (Some(_), Some(_)) => unreachable!("clap conflicts_with prevents both"),
    };

    let (parent_path, name) = split_mac_path(&dst)?;
    if name.is_empty() {
        bail!("destination path has no filename");
    }

    let (file, ctx) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let parent = super::ls::resolve_path(&mut *fs, &parent_path)?;
    if !parent.is_directory() {
        bail!("parent is not a directory: {parent_path}");
    }

    // Duplicate check so we can honor --force consistently.
    let existing = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?
        .into_iter()
        .find(|e| e.name == name);
    if let Some(ref e) = existing {
        if !args.force {
            bail!("{dst} already exists (pass --force to overwrite)");
        }
        fs.delete_entry(&parent, e)
            .map_err(|e| anyhow!("delete existing: {e}"))?;
    }

    let type_code = args
        .type_code
        .clone()
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("put", "type"))
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "BINA".to_string());
    let creator = args
        .creator
        .clone()
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("put", "creator"))
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "????".to_string());
    let options = CreateFileOptions {
        type_code: Some(type_code),
        creator_code: Some(creator),
        ..Default::default()
    };

    if let Some(n) = args.zero {
        let mut zr = ZeroReader { remaining: n };
        fs.create_file(&parent, &name, &mut zr, n, &options)
            .map_err(|e| anyhow!("create_file: {e}"))?;
    } else {
        let host = args.host_file.ok_or_else(|| {
            anyhow!(
                "host file required (or pass --zero N for zero-fill, --boot FILE for boot blocks)"
            )
        })?;
        let meta = std::fs::metadata(&host).map_err(|e| anyhow!("stat {}: {e}", host.display()))?;
        let len = meta.len();
        let mut hf =
            std::fs::File::open(&host).map_err(|e| anyhow!("open {}: {e}", host.display()))?;
        fs.create_file(&parent, &name, &mut hf, len, &options)
            .map_err(|e| anyhow!("create_file: {e}"))?;
    }

    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;

    if args.print_offset {
        // Drop the editable handle before re-opening read-only via the
        // locate path — we want the post-sync on-disk state.
        drop(fs);
        let payload = super::locate::locate_payload(&args.image, &dst)?;
        super::locate::emit_locate(crate::cli::output::OutputFormat::Json, &payload)?;
    }

    Ok(())
}
