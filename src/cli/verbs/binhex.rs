//! `rb-cli put-binhex IMG[@N] HOST.hqx [--dst-dir …]` — decode a BinHex 4.0
//! file and write it (both forks + Finder info) into a filesystem.
//!
//! `rb-cli get-binhex IMG[@N] SRC OUT.hqx` — extract a file from a filesystem
//! and encode it as a BinHex 4.0 (`.hqx`) document, preserving both forks and
//! the type/creator codes.
//!
//! BinHex is the natural single-file, 7-bit-safe container for preserving a
//! Mac file's full stream data. The encode/decode engine lives in
//! `crate::fs::binhex`; these verbs are the thin CLI glue around it, mirroring
//! `put-macbinary`.

use anyhow::{anyhow, bail, Context, Result};
use byteorder::{BigEndian, ByteOrder};
use clap::Args;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::resolve::{resolve_partition_rw, resolve_partition_streaming_with_password};
use crate::fs::binhex::{self, BinHexFile};
use crate::fs::filesystem::{CreateFileOptions, ResourceForkSource};

#[derive(Debug, Args)]
pub struct PutBinHexArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,

    /// BinHex 4.0 (`.hqx`) file on the host.
    pub host_file: PathBuf,

    /// Destination directory inside the filesystem (`/` for root). The
    /// filename comes from the BinHex header. Defaults to `/`. A literal `/`
    /// in a directory name is written `\/`; on HFS / HFS+ a `:`-separated path
    /// also works (so `/` is plain data). Defaults to `/`.
    #[arg(long = "dst-dir", default_value = "/")]
    pub dst_dir: String,

    /// Override the filename from the BinHex header.
    #[arg(long)]
    pub rename: Option<String>,

    /// Overwrite an existing entry at the destination path.
    #[arg(long)]
    pub force: bool,

    /// Clear the `hasBeenInited` Finder flag (0x0100) on the written file. Use
    /// when injecting an app onto a fresh disk so the Finder re-reads its `BNDL`
    /// and registers real icons (a file copied with `hasBeenInited` already set is
    /// treated as already-catalogued, so it shows a generic icon until a desktop
    /// rebuild). Mirrors what a MacBinary install does to byte 73.
    #[arg(long = "clear-inited")]
    pub clear_inited: bool,
}

#[derive(Debug, Args)]
pub struct GetBinHexArgs {
    /// Image reference (`path` or `path@N`).
    pub image: ImageRef,

    /// Source path inside the filesystem.
    pub src: String,

    /// Destination `.hqx` path on the host.
    pub dst: PathBuf,

    /// Password for encrypted containers (currently: WinImage IMZ).
    #[arg(long)]
    pub password: Option<String>,

    /// Accepted for consistency with `ls`/`get`/`rm`; `get-binhex` always
    /// treats the source as an exact literal path (it never globs), so glob
    /// metacharacters in a name are addressed verbatim with or without it.
    /// A literal `/` in a name is written `\/` (or use a `:`-separated path on
    /// HFS / HFS+).
    #[arg(short = 'L', long = "literal", alias = "no-glob")]
    pub literal: bool,
}

pub fn run_put(args: PutBinHexArgs) -> Result<()> {
    let raw = std::fs::read(&args.host_file)
        .with_context(|| format!("reading {}", args.host_file.display()))?;
    let decoded = binhex::parse_binhex(&raw)
        .with_context(|| format!("decoding {}", args.host_file.display()))?;

    let target_name = args.rename.clone().unwrap_or_else(|| decoded.name.clone());
    if target_name.is_empty() {
        bail!("BinHex: empty filename");
    }

    let (file, ctx, commit) = resolve_partition_rw(&args.image.path, args.image.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    // `--dst-dir` is the destination *directory* (the filename comes from the
    // BinHex header), resolved with the shared escape / colon grammar so a
    // donor folder named with a literal `/` is addressable.
    let parent = super::ls::resolve_path(&mut *fs, &args.dst_dir)?;
    if !parent.is_directory() {
        bail!("destination is not a directory: {}", args.dst_dir);
    }

    let existing = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?
        .into_iter()
        .find(|e| e.name == target_name);
    if let Some(ref e) = existing {
        if !args.force {
            bail!(
                "{}/{} already exists (pass --force to overwrite)",
                args.dst_dir.trim_end_matches('/'),
                target_name
            );
        }
        fs.delete_entry(&parent, e)
            .map_err(|e| anyhow!("delete existing: {e}"))?;
    }

    let type_str = String::from_utf8_lossy(&decoded.type_code).into_owned();
    let creator_str = String::from_utf8_lossy(&decoded.creator_code).into_owned();

    let resource_fork = if decoded.resource_fork.is_empty() {
        None
    } else {
        Some(ResourceForkSource::Data(decoded.resource_fork.clone()))
    };
    let options = CreateFileOptions {
        type_code: Some(type_str.clone()),
        creator_code: Some(creator_str.clone()),
        resource_fork,
        ..Default::default()
    };

    let data_len = decoded.data_fork.len() as u64;
    let mut reader: &[u8] = &decoded.data_fork;
    fs.create_file(&parent, &target_name, &mut reader, data_len, &options)
        .map_err(|e| anyhow!("create_file: {e}"))?;

    // Apply Finder flags (BinHex carries them; type/creator already set above).
    let created = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory after create: {e}"))?
        .into_iter()
        .find(|e| e.name == target_name)
        .ok_or_else(|| anyhow!("created entry not found after create_file"))?;

    let mut finfo = [0u8; 16];
    finfo[0..4].copy_from_slice(&decoded.type_code);
    finfo[4..8].copy_from_slice(&decoded.creator_code);
    // Optionally clear hasBeenInited so the Finder re-registers the app's BNDL
    // (real icons) on a fresh disk instead of treating it as already-catalogued.
    let out_flags = if args.clear_inited {
        decoded.flags & !0x0100
    } else {
        decoded.flags
    };
    BigEndian::write_u16(&mut finfo[8..10], out_flags);
    let fxinfo = [0u8; 16];
    if let Err(e) = fs.set_finder_info(&created, finfo, fxinfo) {
        log_stderr(format!("warning: set_finder_info skipped: {e}"));
    }

    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    drop(fs);
    commit.commit()?;

    log_stderr(format!(
        "put-binhex: {} ({} data, {} rsrc, type={} creator={} flags=0x{:04x})",
        target_name,
        data_len,
        decoded.resource_fork.len(),
        type_str,
        creator_str,
        decoded.flags,
    ));
    Ok(())
}

pub fn run_get(args: GetBinHexArgs) -> Result<()> {
    let pw_bytes = args.password.as_deref().map(|s| s.as_bytes());
    let (reader, ctx) = resolve_partition_streaming_with_password(
        &args.image.path,
        args.image.partition,
        pw_bytes,
    )?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_filesystem(
        reader,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem: {e}"))?;

    let entry = super::ls::resolve_path(&mut *fs, &args.src)?;
    if entry.is_directory() {
        bail!("{} is a directory; BinHex encodes a single file", args.src);
    }

    let mut data_fork = Vec::new();
    fs.write_file_to(&entry, &mut data_fork)
        .map_err(|e| anyhow!("read data fork: {e}"))?;

    let mut resource_fork = Vec::new();
    fs.write_resource_fork_to(&entry, &mut resource_fork)
        .map_err(|e| anyhow!("read resource fork: {e}"))?;

    // Use the raw 4-byte Mac OSType verbatim so high-bit type/creator bytes —
    // e.g. the MacRoman florin 0xC4 in Prince of Persia's `PoƒP` creator —
    // survive into the .hqx. (Reconstructing them from the lossy display string
    // mapped non-ASCII to `.`, corrupting the creator: generic icons, and PoP
    // couldn't find its colour data so fell back to B&W.)
    let type_code = entry.type_code.unwrap_or(*b"????");
    let creator_code = entry.creator_code.unwrap_or(*b"????");

    let file = BinHexFile {
        name: entry.name.clone(),
        type_code,
        creator_code,
        // Preserve the source's Finder flags verbatim (hasBundle 0x2000,
        // hasCustomIcon 0x0400, isInvisible, ...). The previously hard-coded 0
        // dropped them, so harvested apps lost their BNDL registration and
        // showed generic icons (docs/bug_binhex_finder_flags.md). put-binhex
        // reads the same word back, so the high byte round-trips intact.
        flags: entry.finder_flags.unwrap_or(0),
        data_fork,
        resource_fork,
    };

    let text = binhex::build_binhex(&file);
    std::fs::write(&args.dst, text.as_bytes())
        .with_context(|| format!("writing {}", args.dst.display()))?;

    log_stderr(format!(
        "get-binhex: {} ({} data, {} rsrc, type={} creator={}) -> {}",
        file.name,
        file.data_fork.len(),
        file.resource_fork.len(),
        String::from_utf8_lossy(&type_code),
        String::from_utf8_lossy(&creator_code),
        args.dst.display(),
    ));
    Ok(())
}
