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

    /// Host file to copy. Required when not using `--zero` or `--boot`. On a
    /// filesystem that stores resource forks (HFS / HFS+ / MFS / ProDOS) the
    /// fork is picked up from whichever host container carries it — a macOS
    /// native fork, a `._name` / `name.rsrc` sidecar beside the file, or a
    /// whole-file `.bin` MacBinary / `.hqx` BinHex wrapper, whose data fork is
    /// unwrapped so the container is not written verbatim. Finder type/creator
    /// ride along unless `--type` / `--creator` say otherwise.
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

    /// Pre-allocate N zero bytes instead of copying a host file. Pair with
    /// `--dst`; a `{A..B}` range in its last component makes one file per number.
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

    /// 4-character type code (HFS / HFS+ / ProDOS). Falls back to `[put] type`
    /// from the config file, then — on HFS / HFS+ / MFS — to the file
    /// extension (same list as the GUI's type/creator picker), and finally to
    /// `BINA` for names the list doesn't recognize.
    #[arg(long = "type")]
    pub type_code: Option<String>,

    /// 4-character creator code (HFS / HFS+ only). Falls back to
    /// `[put] creator` from the config file, then to the file extension, and
    /// finally to `????`.
    #[arg(long)]
    pub creator: Option<String>,

    /// Overwrite an existing entry at the destination path.
    #[arg(long)]
    pub force: bool,

    /// Give the replacement fresh metadata instead of the replaced file's.
    ///
    /// Replacing a file normally keeps what it carried - permissions, owner,
    /// timestamps, type/creator, DOS attribute bits, Amiga protection and
    /// extended attributes - because a replace changes contents, not who may
    /// read the file or when it was made. Pass this to start clean instead.
    /// Only meaningful with `--force`.
    #[arg(long = "no-preserve-meta")]
    pub no_preserve_meta: bool,

    /// Unix permission bits for the new file, as octal (e.g. `755`, `0644`).
    /// Unix filesystems only (ext / UFS / XFS / EFS / Minix / SquashFS);
    /// ignored on FAT / HFS / exFAT, which have no such concept.
    ///
    /// When omitted the mode is inherited from the file being replaced, then
    /// from the host file's own bits, then `0644`. Replacing a file therefore
    /// keeps its permissions -- overwriting a `0600` secret does not widen it.
    #[arg(long = "mode", value_parser = parse_octal_mode)]
    pub mode: Option<u32>,

    /// Owner UID for the new file. Unix filesystems only.
    ///
    /// When omitted it is inherited from the file being replaced, then from the
    /// parent directory, then `0`. The *host* file's owner is deliberately
    /// never used -- a macOS 501 means nothing inside a Linux image.
    #[arg(long = "uid")]
    pub uid: Option<u32>,

    /// Owning GID for the new file. Unix filesystems only. Same precedence as
    /// `--uid`.
    #[arg(long = "gid")]
    pub gid: Option<u32>,

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

/// Parse `--mode` as octal, with or without a leading `0`/`0o`.
///
/// Octal is the only sane reading of a Unix mode, so `755` means `0o755` — a
/// decimal reading would silently produce `0o1363`. Rejects anything above
/// `0o7777`, which is the whole permission space including setuid/setgid/sticky.
fn parse_octal_mode(s: &str) -> Result<u32, String> {
    let body = s
        .strip_prefix("0o")
        .or_else(|| s.strip_prefix("0O"))
        .unwrap_or(s);
    if body.is_empty() || !body.bytes().all(|b| (b'0'..=b'7').contains(&b)) {
        return Err(format!(
            "invalid mode {s:?}: expected octal permission bits, e.g. 755 or 0644"
        ));
    }
    let v = u32::from_str_radix(body, 8).map_err(|e| format!("invalid mode {s:?}: {e}"))?;
    if v > 0o7777 {
        return Err(format!(
            "invalid mode {s:?}: {v:o} exceeds 7777 (permission bits only; \
             the file-type bits are not yours to set)"
        ));
    }
    Ok(v)
}

pub fn run(args: PutArgs) -> Result<()> {
    run_with_budget(args, None)
}

/// As [`run`], with a size ceiling for the filesystems that rebuild their whole
/// image on commit (SquashFS). `rb-cli squashfs put` is the only caller that
/// passes one; see [`super::squashfs`].
pub fn run_with_budget(
    args: PutArgs,
    budget: Option<crate::fs::squashfs_edit::SizeBudget>,
) -> Result<()> {
    if let Some(bb_file) = args.boot {
        // Boot-block write: 1024 bytes at the *partition's* first
        // sector, not the image's. For raw superfloppies that's byte 0;
        // for APM-wrapped disks (`IMG@N`) it's the Apple_HFS
        // partition's start_lba * 512 (typically 0xC000), and
        // overwriting byte 0 would smash the APM Driver Descriptor
        // Record. We resolve the partition through the same dispatch
        // every other verb uses so `--boot` honors `@N`.
        return put_boot(&args.image.path, args.image.partition.clone(), &bb_file);
    }
    if let Some(donor) = args.boot_from {
        return put_boot_from(&args.image.path, args.image.partition.clone(), &donor);
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
            args.image.partition.clone(),
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
        args.image.partition.clone(),
        args.fs_override.fs_type.as_deref(),
    )?;
    args.fs_override.apply(&mut ctx);
    ctx.rebuild_budget = budget;
    log_stderr(&ctx.label);
    let mut fs = ctx
        .open_editable(file)
        .map_err(|e| crate::cli::resolve::write_open_error("opening filesystem for write", e))?;

    // Resolve parent + leaf with the shared escape / colon grammar so a file
    // whose name contains a literal `/` can be written.
    let (parent, name) = super::ls::resolve_parent(fs.as_filesystem_mut(), &dst)?;
    if name.is_empty() {
        bail!("destination path has no filename");
    }
    if !parent.is_directory() {
        bail!("parent is not a directory: {dst}");
    }

    // Duplicate check so we can honor --force consistently, matched the way
    // the filesystem matches (FAT and friends ignore case).
    let fold_case = fs.case_insensitive_lookup();
    let existing = crate::fs::copy::select_child(
        &fs.list_directory(&parent)
            .map_err(|e| anyhow!("list_directory: {e}"))?,
        fold_case,
        &name,
    )
    .cloned();
    // Capture before the delete: afterwards there is nothing left to ask.
    // Everything the replaced file carried, not just its xattrs. A replace
    // writes new *contents*; it is not a request to reset permissions, owner,
    // timestamps or type/creator. Must be captured before the delete below.
    let preserved = if args.no_preserve_meta {
        crate::fs::attrs::PreservedMeta::default()
    } else {
        crate::fs::attrs::preserved_meta(fs.as_filesystem_mut(), existing.as_ref())
    };
    if existing.as_ref().is_some_and(|e| e.is_directory()) {
        bail!("{dst} is a directory; put replaces files only (rm -r it first)");
    }
    if existing.is_some() && !args.force {
        bail!("{dst} already exists (pass --force to overwrite)");
    }
    // The delete is NOT done here. `create_or_replace` stages the swap so a
    // failure mid-write leaves the original intact, which delete-then-create
    // could not promise.
    let on_conflict = if existing.is_some() {
        crate::fs::replace::OnConflict::Replace
    } else {
        crate::fs::replace::OnConflict::Fail
    };

    // A Mac file arriving in one of the host containers — a `._name` /
    // `name.rsrc` sidecar, or a whole-file `.bin` / `.hqx` wrapper. Without
    // this, `archive extract` followed by `put` dropped every resource fork
    // (R-040) and only the dedicated `put-macbinary` verb carried one.
    let rsrc_import = if crate::fs::copy::Capabilities::infer(fs.fs_type()).resource_forks {
        args.host_file
            .as_ref()
            .and_then(|p| crate::fs::resource_fork::detect_resource_fork(p))
    } else {
        None
    };
    if let Some(imp) = &rsrc_import {
        log_stderr(format!(
            "Resource fork: {} bytes{}",
            imp.data.len(),
            if imp.data_fork.is_some() {
                " (unwrapped from the host container)"
            } else {
                " (from the host sidecar)"
            }
        ));
    }
    let container_type = rsrc_import.as_ref().and_then(|i| i.type_code);
    let container_creator = rsrc_import.as_ref().and_then(|i| i.creator_code);

    // An explicit --type / --creator (or the config default) always wins. Failing
    // that, on the classic-Mac filesystems we leave both unset so `create_file`
    // consults the shared extension dictionary -- the same list the GUI's
    // type/creator picker offers -- and a `.txt` lands as TEXT/ttxt instead of a
    // generic BINA blob. BINA/???? stays the fallback for names the dictionary
    // doesn't recognize, and for every other filesystem (ProDOS types are `$XX`,
    // a different space entirely).
    //
    // The generic BINA/???? fallback must rank *below* the type the replaced
    // file carried, or replacing `DOC` (TEXT/MSWD) turns it into a BINA blob -
    // the fallback would fire first and leave nothing for preservation to fill
    // in. Precedence, highest first: explicit flag, config default, the
    // replaced file's own type, the extension dictionary, then BINA/????.
    let auto_from_extension = crate::fs::hfs_common::uses_hfs_type_dictionary(fs.fs_type())
        && crate::fs::hfs_common::type_creator_for_filename(&name).is_some();
    let preserves_type = preserved.os_type.is_some();
    let type_code = args
        .type_code
        .clone()
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("put", "type"))
                .map(|s| s.to_string())
        })
        .or_else(|| {
            (!auto_from_extension && !preserves_type && container_type.is_none())
                .then(|| "BINA".to_string())
        });
    let preserves_creator = preserved.os_creator.is_some();
    let creator = args
        .creator
        .clone()
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("put", "creator"))
                .map(|s| s.to_string())
        })
        .or_else(|| {
            (!auto_from_extension && !preserves_creator && container_creator.is_none())
                .then(|| "????".to_string())
        });
    // POSIX attributes. Every editable Unix filesystem honours these; until now
    // nothing set them, so each driver's `unwrap_or` silently made every added
    // file root:root 0644. Resolution (and its precedence rules) lives in
    // `fs::attrs` so the GUI and TUI cannot drift from the CLI.
    let host_mode = args.host_file.as_ref().and_then(|p| {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::metadata(p).ok().map(|m| m.permissions().mode())
        }
        #[cfg(not(unix))]
        {
            let _ = p;
            None
        }
    });
    let attrs = crate::fs::attrs::resolve_attrs(
        &crate::fs::attrs::AttrOverrides {
            mode: args.mode,
            uid: args.uid,
            gid: args.gid,
            unix_times: None,
        },
        // `--no-preserve-meta` has to be withheld here too, not just from
        // `preserved` above. These are two independent inheritance paths and
        // zeroing only one left the opt-out half-done: the POSIX triple kept
        // coming from the replaced entry (`AttrSource::Replaced`) while the log
        // line promised the previous file's permissions and owner were "not
        // carried over". Type/creator went through `preserved` and did reset,
        // so the two halves of one flag disagreed.
        if args.no_preserve_meta {
            None
        } else {
            existing.as_ref()
        },
        Some(&parent),
        host_mode,
        0o644,
    );
    // Only worth a line when the filesystem actually stores these; on FAT/HFS
    // it would be noise about fields that go nowhere.
    if parent.mode.is_some() || existing.as_ref().map(|e| e.mode.is_some()) == Some(true) {
        log_stderr(format!("Attributes: {}", attrs.describe()));
    }

    if existing.is_some() {
        if args.no_preserve_meta {
            log_stderr(
                "Replacing with fresh metadata (--no-preserve-meta); the previous \
                 file's permissions, owner, dates and type/creator are not carried over",
            );
        } else if !preserved.is_empty() {
            log_stderr(format!(
                "Preserving from the replaced file: {}",
                preserved.summary()
            ));
        }
    }
    let mut options = CreateFileOptions {
        type_code,
        creator_code: creator,
        mode: Some(attrs.file_mode()),
        uid: Some(attrs.uid),
        gid: Some(attrs.gid),
        ..Default::default()
    };
    // Carried as raw `os_type` bytes rather than the text form so high-bit
    // OSTypes survive; an explicit --type above already won, and preservation
    // below only fills what is still unset.
    if let Some(imp) = &rsrc_import {
        if !imp.data.is_empty() {
            options.resource_fork = Some(crate::fs::filesystem::ResourceForkSource::Data(
                imp.data.clone(),
            ));
        }
        if options.type_code.is_none() {
            options.os_type = container_type;
        }
        if options.creator_code.is_none() {
            options.os_creator = container_creator;
        }
        options.finder_flags = imp.finder_flags;
    }
    // Fills only what nobody set explicitly, so --type / --mode still win.
    preserved.apply_to_options(&mut options);

    let write_through = |fs: &mut dyn crate::fs::filesystem::EditableFilesystem,
                         reader: &mut dyn std::io::Read,
                         len: u64|
     -> anyhow::Result<()> {
        let outcome = crate::fs::replace::create_or_replace(
            fs,
            &parent,
            &name,
            reader,
            len,
            &options,
            crate::fs::replace::ReplacePolicy {
                on_conflict,
                preserve_meta: !args.no_preserve_meta,
            },
        )
        // Through write_open_error so an `Unsupported` from the driver — "this
        // filesystem is readable and this build will not write it" — exits 4
        // rather than the catch-all 1. The write-open path has done this since
        // R-034; create_file can refuse for the same reason and did not.
        .map_err(|e| crate::cli::resolve::write_open_error("create_file", e))?;
        if outcome.unsafe_fallback {
            log_stderr(
                "Note: this filesystem cannot stage a replace (no rename), so the original \
                 was removed before the new contents were written",
            );
        }
        Ok(())
    };

    if let Some(names) = expand_brace_range(&name) {
        let n = args
            .zero
            .ok_or_else(|| anyhow!("a {{A..B}} destination range needs --zero N"))?;
        if args.force {
            bail!("--force does not apply to a {{A..B}} destination range");
        }
        for nm in &names {
            let mut zr = ZeroReader { remaining: n };
            fs.create_file(&parent, nm, &mut zr, n, &options)
                .map_err(|e| crate::cli::resolve::write_open_error("create_file", e))?;
        }
        log_stderr(format!("Created {} files", names.len()));
    } else if let Some(n) = args.zero {
        let mut zr = ZeroReader { remaining: n };
        write_through(fs.as_mut(), &mut zr, n)?;
    } else {
        let host = args.host_file.ok_or_else(|| {
            anyhow!(
                "host file required (or pass --zero N for zero-fill, --boot FILE for boot blocks)"
            )
        })?;
        // A whole-file container (MacBinary / BinHex) holds the data fork
        // inside it; writing the wrapper's own bytes would land the container
        // rather than the file it carries.
        match rsrc_import.as_ref().and_then(|i| i.data_fork.as_ref()) {
            Some(df) => {
                let len = df.len() as u64;
                write_through(fs.as_mut(), &mut std::io::Cursor::new(df), len)?;
            }
            None => {
                let meta = std::fs::metadata(&host)
                    .map_err(|e| anyhow!("stat {}: {e}", host.display()))?;
                let len = meta.len();
                let mut hf = std::fs::File::open(&host)
                    .map_err(|e| anyhow!("open {}: {e}", host.display()))?;
                write_through(fs.as_mut(), &mut hf, len)?;
            }
        }
    }

    // Timestamps are the one preserved field no filesystem here accepts at
    // creation time, so they go back once the entry exists. Best-effort: a
    // filesystem with no date setter answers Unsupported, and failing an
    // otherwise-good write over a timestamp would be the worse outcome.
    if preserved.mac_dates.is_some() {
        match super::ls::resolve_path(fs.as_filesystem_mut(), &dst) {
            Ok(new_entry) => {
                if !preserved.reapply_dates(fs.as_mut(), &new_entry) {
                    log_stderr(
                        "Note: this filesystem cannot set timestamps, so the replacement \
                         carries today's date",
                    );
                }
            }
            Err(e) => log_stderr(format!("Note: could not restore timestamps ({e})")),
        }
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
    partition: Option<crate::cli::img_at::PartSelector>,
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

    // Type/creator defaults mirror the local path: flag, else config, else the
    // extension dictionary, else BINA/????. Unlike the local path we can't see
    // the remote filesystem from here, so the dictionary check is name-only: a
    // recognized extension sends `None` and lets the server's `create_file`
    // resolve it (which consults the same dictionary on HFS/HFS+/MFS, and picks
    // its own sensible default elsewhere).
    let auto_from_extension = crate::fs::hfs_common::type_creator_for_filename(name).is_some();
    let type_code = type_code
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("put", "type"))
                .map(|s| s.to_string())
        })
        .or_else(|| (!auto_from_extension).then(|| "BINA".to_string()));
    let creator = creator
        .or_else(|| {
            crate::cli::logging::loaded_config()
                .and_then(|c| c.get("put", "creator"))
                .map(|s| s.to_string())
        })
        .or_else(|| (!auto_from_extension).then(|| "????".to_string()));

    let mut session = crate::remote::RemoteSession::connect(&rref.addr())?;
    let sid = session.open_session(&rref.path, partition)?;
    session.stage_upload(sid, parent_path, name, &host, force, type_code, creator)?;
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
    partition: Option<crate::cli::img_at::PartSelector>,
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
fn put_boot_from(
    image: &std::path::Path,
    partition: Option<crate::cli::img_at::PartSelector>,
    donor: &ImageRef,
) -> Result<()> {
    use crate::cli::resolve::resolve_partition_streaming;
    use crate::fs::hfs_boot::read_donor_boot_blocks;

    // Read + validate from the donor first; never touch the target if the
    // donor isn't actually bootable.
    let (mut reader, donor_ctx) =
        resolve_partition_streaming(&donor.path, donor.partition.clone())?;
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
fn write_boot_region(
    image: &std::path::Path,
    partition: Option<crate::cli::img_at::PartSelector>,
    bb: &[u8],
) -> Result<()> {
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

/// Expand a `{A..B}` range (decimal, ascending, zero-padded to A's width
/// when A has a leading zero) in `name`; `None` when there is no range.
fn expand_brace_range(name: &str) -> Option<Vec<String>> {
    let open = name.find('{')?;
    let close = name[open..].find('}')? + open;
    let (lo, hi) = name[open + 1..close].split_once("..")?;
    if lo.is_empty()
        || !lo.bytes().all(|b| b.is_ascii_digit())
        || !hi.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let (a, b): (u64, u64) = (lo.parse().ok()?, hi.parse().ok()?);
    if b < a || b - a > 10_000_000 {
        return None;
    }
    let width = if lo.starts_with('0') { lo.len() } else { 0 };
    let (prefix, suffix) = (&name[..open], &name[close + 1..]);
    Some(
        (a..=b)
            .map(|i| format!("{prefix}{i:0width$}{suffix}"))
            .collect(),
    )
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

    #[test]
    fn brace_range_expands_zero_padded_runs() {
        assert_eq!(
            super::expand_brace_range("f{0008..0010}.txt").unwrap(),
            ["f0008.txt", "f0009.txt", "f0010.txt"]
        );
        assert_eq!(
            super::expand_brace_range("f{9..11}").unwrap(),
            ["f9", "f10", "f11"]
        );
        assert!(super::expand_brace_range("plain.txt").is_none());
        assert!(super::expand_brace_range("f{5..3}").is_none());
        assert!(super::expand_brace_range("f{a..b}").is_none());
    }
}
