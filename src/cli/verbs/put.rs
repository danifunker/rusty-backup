//! `rb-cli put` — copy a host file (or zero-fill) into a filesystem.
//!
//! Four shapes:
//! - `put IMG[@N] HOST DST [opts]` — cp-like copy.
//! - `put IMG[@N] --zero BYTES --dst DST [opts]` — pre-allocate zero
//!   bytes (the `--dst` flag avoids positional ambiguity).
//! - `put IMG[@N] --boot BB_FILE` — write the 1024-byte boot block
//!   region of the image verbatim. HFS-specific; other filesystems
//!   return an error.
//! - `put IMG[@N] --boot-from DONOR[@N]` — copy the boot-block region
//!   from a donor disk that already boots (its classic-HFS volume is
//!   auto-located and its `'LK'` signature validated). Makes a bare
//!   HFS volume bootable. HFS-specific.
//!
//! `--type` / `--creator` apply only to filesystems that carry per-file
//! type/creator codes (HFS, HFS+, ProDOS); on other filesystems the
//! flags are accepted but ignored with a warning.

use anyhow::{anyhow, bail, Result};
use clap::Args;
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
#[cfg(feature = "remote")]
use crate::cli::parse::split_mac_path;
use crate::cli::parse::ZeroReader;
use crate::cli::resolve::{resolve_partition_rw, resolve_partition_rw_forced, FsDispatchOverride};
use crate::fs::filesystem::CreateFileOptions;

#[derive(Debug, Args)]
pub struct PutArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Host file to copy. Required when not using `--zero` or `--boot`.
    pub host_file: Option<PathBuf>,

    /// Destination path inside the filesystem (cp-like positional). A literal
    /// `/` in the name is written `\/`; on HFS / HFS+ a `:`-separated path also
    /// works (so `/` is plain data).
    pub dst: Option<String>,

    /// Accepted for consistency with `ls`/`get`/`rm`; `put` always treats the
    /// destination as an exact literal path (it never globs), so glob
    /// metacharacters in a name are used verbatim with or without it.
    #[arg(short = 'L', long = "literal", alias = "no-glob")]
    pub literal: bool,

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

    /// Copy the 1024-byte boot-block region from a donor disk that already
    /// boots (`path` or `path@N`), instead of from a raw file. The donor's
    /// classic-HFS volume is auto-located (flat `.hfv`/`.dsk` at byte 0, or
    /// an `Apple_HFS` partition) and its `'LK'` signature validated. The
    /// region is written to the *target partition's* first sector, so this
    /// works on a flat HFV and on the HFS partition of a full (APM) disk
    /// alike — target the HFS partition with `IMG@N` (the DDR / partition
    /// map / drivers ahead of it are never touched). Use it to make a bare
    /// HFS volume (e.g. an edited infinite-mac disk) bootable. HFS-only today.
    #[arg(long = "boot-from", conflicts_with_all = ["host_file", "dst", "dst_flag", "zero", "type_code", "creator", "force", "boot"])]
    pub boot_from: Option<ImageRef>,

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

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

pub fn run(args: PutArgs) -> Result<()> {
    if let Some(bb_file) = args.boot {
        // Boot-block write: 1024 bytes at the *partition's* first
        // sector, not the image's. For raw superfloppies that's byte 0;
        // for APM-wrapped disks (`IMG@N`) it's the Apple_HFS
        // partition's start_lba * 512 (typically 0xC000), and
        // overwriting byte 0 would smash the APM Driver Descriptor
        // Record. We resolve the partition through the same dispatch
        // every other verb uses so `--boot` honors `@N`.
        return put_boot(&args.image.path, args.image.partition, &bb_file);
    }
    if let Some(donor) = args.boot_from {
        return put_boot_from(&args.image.path, args.image.partition, &donor);
    }

    let dst = match (args.dst, args.dst_flag) {
        (Some(d), None) | (None, Some(d)) => d,
        (None, None) => bail!(
            "destination path required (positional DST or --dst PATH; or pass --boot for boot blocks)"
        ),
        (Some(_), Some(_)) => unreachable!("clap conflicts_with prevents both"),
    };

    // Remote destination: `rb-cli put rb://host:port/img@N HOST /DEST`. Upload
    // the host file into the daemon's staging area and apply it. Remote
    // addressing is slash-only (the daemon resolves the path itself).
    #[cfg(feature = "remote")]
    if let Some(rref) = crate::remote::RemoteRef::parse(&args.image.path.to_string_lossy()) {
        let (parent_path, name) = split_mac_path(&dst)?;
        if name.is_empty() {
            bail!("destination path has no filename");
        }
        return remote_put(
            &rref,
            args.image.partition,
            &parent_path,
            &name,
            args.host_file,
            args.zero,
            args.force,
            args.type_code,
            args.creator,
        );
    }

    let (file, mut ctx, commit) = resolve_partition_rw_forced(
        &args.image.path,
        args.image.partition,
        args.fs_override.fs_type.as_deref(),
    )?;
    args.fs_override.apply(&mut ctx);
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    // Resolve parent + leaf with the shared escape / colon grammar so a file
    // whose name contains a literal `/` can be written.
    let (parent, name) = super::ls::resolve_parent(&mut *fs, &dst)?;
    if name.is_empty() {
        bail!("destination path has no filename");
    }
    if !parent.is_directory() {
        bail!("parent is not a directory: {dst}");
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

    // Drop the editable handle, then persist. For a floppy container this
    // re-encodes the temp flat back into the .d88/.xdf/.hdm/.dim file; for a
    // raw image it's a no-op. Must happen before the print_offset re-open so
    // locate sees the post-edit on-disk state.
    drop(fs);
    commit.commit()?;

    if args.print_offset {
        let payload = super::locate::locate_payload(&args.image, &dst)?;
        super::locate::emit_locate(crate::cli::output::OutputFormat::Json, &payload)?;
    }

    Ok(())
}

/// `rb-cli put rb://host:port/img@N HOST /DEST` — stage a host file into a
/// remote image and apply it. Phase 1: a single host file (not `--zero` /
/// `--boot`, which are deferred over `rb://`).
#[cfg(feature = "remote")]
#[allow(clippy::too_many_arguments)]
fn remote_put(
    rref: &crate::remote::RemoteRef,
    partition: Option<u32>,
    parent_path: &str,
    name: &str,
    host_file: Option<PathBuf>,
    zero: Option<u64>,
    force: bool,
    type_code: Option<String>,
    creator: Option<String>,
) -> Result<()> {
    if zero.is_some() {
        bail!("--zero isn't supported over rb:// yet (Phase 1 copies a host file)");
    }
    let host = host_file.ok_or_else(|| anyhow!("host file required (positional HOST argument)"))?;

    // Type/creator defaults mirror the local path: flag, else config, else BINA/????.
    let type_code = type_code
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("put", "type"))
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "BINA".to_string());
    let creator = creator
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("put", "creator"))
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "????".to_string());

    let mut session = crate::remote::RemoteSession::connect(&rref.addr())?;
    let sid = session.open_session(&rref.path, partition)?;
    session.stage_upload(
        sid,
        parent_path,
        name,
        &host,
        force,
        Some(type_code),
        Some(creator),
    )?;
    let n = session.apply(sid)?;
    session.close_session(sid)?;
    crate::cli::logging::out_stdout(format!("Wrote {name} ({n} edit applied over rb://)"));
    Ok(())
}

/// Write a boot block (up to 1024 bytes, zero-padded if shorter) at the
/// selected partition's first sector. HFS-only today, but the partition
/// resolution is generic so future FAT/NTFS boot-loader writes can drop
/// in by relaxing the type-byte check.
fn put_boot(
    image: &std::path::Path,
    partition: Option<u32>,
    bb_file: &std::path::Path,
) -> Result<()> {
    let bb = std::fs::read(bb_file).map_err(|e| anyhow!("reading {}: {e}", bb_file.display()))?;
    if bb.len() > 1024 {
        bail!(
            "boot block source is {} bytes; HFS boot region is 1024 bytes max",
            bb.len()
        );
    }
    write_boot_region(image, partition, &bb)
}

/// `put IMG[@N] --boot-from DONOR[@N]` — copy the donor's validated 1024-byte
/// boot-block region into the target's first sector. The donor's classic-HFS
/// volume is auto-located and its `'LK'` signature checked before anything is
/// written to the target.
fn put_boot_from(image: &std::path::Path, partition: Option<u32>, donor: &ImageRef) -> Result<()> {
    use crate::cli::resolve::resolve_partition_streaming;
    use crate::fs::hfs_boot::read_donor_boot_blocks;

    // Read + validate from the donor first; never touch the target if the
    // donor isn't actually bootable.
    let (mut reader, donor_ctx) = resolve_partition_streaming(&donor.path, donor.partition)?;
    let blocks = read_donor_boot_blocks(&mut reader, donor_ctx.offset).map_err(|e| {
        anyhow!(
            "reading boot blocks from donor {}: {e}",
            donor.path.display()
        )
    })?;
    drop(reader);

    log_stderr(format!(
        "copying boot blocks from {} (offset {})",
        donor.path.display(),
        donor_ctx.offset
    ));
    write_boot_region(image, partition, blocks.as_slice())
}

/// Whether the partition described by `type_byte` / `type_string` is a valid
/// target for a raw boot-block write. Only an HFS volume qualifies.
///
/// The subtlety this guards is full APM disks: every APM partition — drivers
/// and the partition map included — reports MBR type byte `0x00`, so the
/// "`0x00` means raw superfloppy, accept it" shortcut is only sound when there
/// is **no** partition table (i.e. no type string). When a type string is
/// present we require it to be `Apple_HFS`; otherwise `IMG@N` aimed at a
/// driver partition would silently overwrite its first 1024 bytes.
fn is_boot_block_target(type_byte: u8, type_string: Option<&str>) -> bool {
    match type_string {
        Some(s) => s.eq_ignore_ascii_case("Apple_HFS"),
        None => type_byte == 0xAF || type_byte == 0x00,
    }
}

/// Shared write side for `--boot` / `--boot-from`: place `bb` (up to 1024
/// bytes, zero-padded if shorter) at the selected partition's first sector.
fn write_boot_region(image: &std::path::Path, partition: Option<u32>, bb: &[u8]) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let (mut file, ctx, commit) = resolve_partition_rw(image, partition)?;
    log_stderr(&ctx.label);

    if !is_boot_block_target(ctx.type_byte, ctx.type_string.as_deref()) {
        bail!(
            "boot-block writes are HFS-only today; partition is type 0x{:02x} {}. \
             On a full disk, target the HFS partition explicitly with IMG@N \
             (see `rb-cli inspect IMG` for the index).",
            ctx.type_byte,
            ctx.type_string.as_deref().unwrap_or("(no type string)")
        );
    }

    file.seek(SeekFrom::Start(ctx.offset))?;
    file.write_all(bb)?;
    file.flush()?;
    drop(file);
    commit.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::is_boot_block_target;

    #[test]
    fn boot_target_accepts_hfs_and_raw_superfloppy() {
        // APM / typed Apple_HFS partition (full disk's HFS volume).
        assert!(is_boot_block_target(0x00, Some("Apple_HFS")));
        assert!(is_boot_block_target(0x00, Some("apple_hfs"))); // case-insensitive
                                                                // MBR HFS partition.
        assert!(is_boot_block_target(0xAF, None));
        // Raw superfloppy / freshly built flat HFS image (no partition table).
        assert!(is_boot_block_target(0x00, None));
    }

    #[test]
    fn boot_target_rejects_non_hfs_apm_partitions() {
        // The regression: a driver / map partition on a full APM disk also
        // reports type byte 0x00. It must NOT be accepted just because of the
        // byte — the type string disqualifies it.
        assert!(!is_boot_block_target(0x00, Some("Apple_Driver_IOKit")));
        assert!(!is_boot_block_target(0x00, Some("Apple_Driver43")));
        assert!(!is_boot_block_target(0x00, Some("Apple_partition_map")));
        assert!(!is_boot_block_target(0x00, Some("Apple_HFSX")));
        // And a non-HFS MBR partition (e.g. FAT) is rejected.
        assert!(!is_boot_block_target(0x0c, None));
    }
}
