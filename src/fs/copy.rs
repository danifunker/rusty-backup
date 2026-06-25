//! Cross-image file copy engine.
//!
//! Copies files and directory trees between two open filesystems —
//! source (`Filesystem`, read) and destination (`EditableFilesystem`,
//! write) — without staging through host files. The headline use case is
//! consolidating many small media images (floppies, cartridges) onto a
//! single larger disk image, but it works between any read source and any
//! writable destination the engine supports.
//!
//! This module is GUI-/CLI-agnostic on purpose: the `rb-cli cp` verb is a
//! thin wrapper, and a future browse-view "copy between volumes" action
//! plus the `batch` `from_image` op will call the same entry points.
//!
//! ## Fidelity
//!
//! Per-file metadata is carried across the boundary on a best-effort
//! basis: whatever the *destination* filesystem can store is translated
//! from the source's [`FileEntry`] into [`CreateFileOptions`]; whatever it
//! can't is dropped with a warning (counted in [`CopyStats`]). The axes:
//! resource forks, HFS type/creator, ProDOS aux, Unix mode/uid/gid, Amiga
//! protection/comment/dates, and DOS attribute bits (FAT/exFAT). NTFS
//! security descriptors / alternate data streams are out of scope — when
//! Unix perms are dropped onto an NTFS destination, new files simply
//! inherit the parent folder's ACL (NTFS's own default).
//!
//! ## Streaming
//!
//! Source files are bridged to the destination through a
//! [`tempfile::SpooledTempFile`]: the bytes stay in RAM for small files
//! (the common vintage case — whole floppies never touch disk) and spill
//! to a temp file only past [`SPOOL_THRESHOLD`], so a rare multi-GB file
//! can't exhaust memory.

use std::collections::HashSet;
use std::io::{Seek, SeekFrom};

use anyhow::{anyhow, bail, Result};

use super::entry::{EntryType, FileEntry};
use super::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, ResourceForkSource,
};

/// Files up to this size are bridged entirely in RAM; larger files spill
/// to a temp file. 16 MiB comfortably holds any floppy / cartridge image's
/// individual files while bounding memory on the occasional huge file.
pub const SPOOL_THRESHOLD: usize = 16 * 1024 * 1024;

/// What to do when a destination entry of the same name already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictMode {
    /// Hard error (default).
    Error,
    /// Delete the existing entry and write the new one.
    Force,
    /// Leave the existing entry, skip the source.
    SkipExisting,
}

/// What to do when a source name is illegal on the destination filesystem
/// (too long, illegal characters, wrong encoding).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamePolicy {
    /// Mangle the name to fit (truncate + `~N` uniquifier), warn (default).
    Truncate,
    /// Skip the offending entry, warn.
    Skip,
    /// Hard error.
    Error,
}

/// Whether to carry filesystem-specific attributes (type/creator, Unix
/// perms, DOS attribute bits, Amiga bits). Resource forks are treated as
/// data, not attributes, and are always copied when the destination
/// supports them regardless of this setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrPolicy {
    /// Carry every attribute the destination supports (default).
    Preserve,
    /// Don't carry any FS-specific attribute. On NTFS this means new files
    /// inherit the parent folder's ACL.
    Skip,
}

/// Tunables for a copy run.
#[derive(Debug, Clone)]
pub struct CopyOptions {
    pub recursive: bool,
    pub conflict: ConflictMode,
    pub names: NamePolicy,
    pub attrs: AttrPolicy,
    /// Collapse a source directory tree into the destination directory when
    /// the destination filesystem has no subdirectory support (CP/M, DFS,
    /// CBM, Apple/Atari DOS, MFS, …). Without it, copying a tree into a
    /// flat filesystem is a hard error.
    pub flatten: bool,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            conflict: ConflictMode::Error,
            names: NamePolicy::Truncate,
            attrs: AttrPolicy::Preserve,
            flatten: false,
        }
    }
}

/// Running totals for a copy, returned for the CLI summary line and the
/// (future) GUI report.
#[derive(Debug, Default, Clone)]
pub struct CopyStats {
    pub files: u64,
    pub dirs: u64,
    pub symlinks: u64,
    pub bytes: u64,
    pub renamed: u64,
    pub skipped_existing: u64,
    pub skipped_special: u64,
    pub skipped_names: u64,
    pub dropped_forks: u64,
    pub dropped_type_creator: u64,
    pub dropped_perms: u64,
    pub dropped_attrs: u64,
    pub dropped_amiga: u64,
}

/// Per-axis record of metadata that couldn't cross to the destination,
/// folded into [`CopyStats`] and surfaced as warnings.
#[derive(Debug, Default, Clone, Copy)]
struct Dropped {
    forks: bool,
    type_creator: bool,
    perms: bool,
    dos_attrs: bool,
    amiga: bool,
}

/// Coarse per-filesystem capability flags, used to decide which metadata
/// axes can cross and whether directories are supported.
///
/// Inferred from [`Filesystem::fs_type`] in [`Capabilities::infer`]. This
/// is the single place to update when a new filesystem gains relevant
/// support; an incomplete entry degrades to spurious "dropped" warnings or
/// a loud `create_directory` error, never to silent data loss. (Promoting
/// this to a `Filesystem` trait method later is mechanical.)
#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    pub subdirectories: bool,
    pub resource_forks: bool,
    pub type_creator: bool,
    pub unix_permissions: bool,
    pub dos_attributes: bool,
    pub amiga_metadata: bool,
    pub symlinks: bool,
}

impl Capabilities {
    pub fn infer(fs_type: &str) -> Self {
        let t = fs_type;
        // Hierarchical baseline; flip individual axes below.
        let mut c = Capabilities {
            subdirectories: true,
            resource_forks: false,
            type_creator: false,
            unix_permissions: false,
            dos_attributes: false,
            amiga_metadata: false,
            symlinks: false,
        };

        // Classic Mac forks + Finder type/creator.
        if t.starts_with("HFS") {
            c.resource_forks = true;
            c.type_creator = true;
        } else if t == "MFS" {
            c.resource_forks = true;
            c.type_creator = true;
            c.subdirectories = false; // MFS is a flat volume
        } else if t == "ProDOS" {
            c.resource_forks = true; // extended (forked) files
            c.type_creator = true; // file type + aux
        }

        // Unix families: mode/uid/gid + symlinks.
        if t.starts_with("ext")
            || t == "btrfs"
            || t.starts_with("XFS")
            || t.starts_with("UFS")
            || t.starts_with("ReiserFS")
            || t == "EFS"
            || t.starts_with("JFS")
        {
            c.unix_permissions = true;
            c.symlinks = true;
        }

        // DOS attribute bits.
        if t.starts_with("FAT") || t == "exFAT" || t.starts_with("Human68k") {
            c.dos_attributes = true;
        }

        // AmigaDOS family: protection / comment / datestamp + symlinks.
        if t.starts_with("OFS")
            || t.starts_with("FFS")
            || t.starts_with("PFS")
            || t == "AFS"
            || t == "SFS"
        {
            c.amiga_metadata = true;
            c.symlinks = true;
        }

        // Flat filesystems with no subdirectory concept.
        if t == "CP/M"
            || t == "Acorn DFS"
            || t == "DOS 3.3"
            || t == "Atari DOS 2"
            || t == "RS-DOS"
            || t == "DragonDOS"
            || t == "QDOS Microdrive"
            || t.starts_with("ANDOS")
            || t.starts_with("CBM DOS")
        {
            c.subdirectories = false;
        }

        c
    }
}

/// Project the on-disk bytes a source entry tree will consume on a
/// destination with allocation unit `alloc_unit` and capabilities
/// `dst_caps`. Conservative-approximate: rounds each fork up to the
/// allocation unit and adds one unit per directory. The caller compares
/// the sum against `EditableFilesystem::free_space()` before copying; the
/// `DiskFull` runtime error remains a backstop.
pub fn project(
    src: &mut dyn Filesystem,
    entry: &FileEntry,
    dst_caps: &Capabilities,
    alloc_unit: Option<u64>,
    recursive: bool,
) -> Result<u64> {
    let unit = alloc_unit.unwrap_or(512).max(1);
    let round = |n: u64| n.div_ceil(unit) * unit;
    match entry.entry_type {
        EntryType::Directory => {
            if !recursive {
                return Ok(0);
            }
            let mut total = unit; // the directory itself
            for child in src
                .list_directory(entry)
                .map_err(|e| anyhow!("list_directory {}: {e}", entry.path))?
            {
                total =
                    total.saturating_add(project(src, &child, dst_caps, alloc_unit, recursive)?);
            }
            Ok(total)
        }
        EntryType::File => {
            let mut total = round(entry.size);
            if dst_caps.resource_forks {
                let rsrc = entry.resource_fork_size.unwrap_or(0);
                if rsrc > 0 {
                    total = total.saturating_add(round(rsrc));
                }
            }
            Ok(total)
        }
        // Symlinks/specials are tiny; ignore for projection.
        EntryType::Symlink | EntryType::Special => Ok(0),
    }
}

/// Copy a single source entry (file, directory, symlink, or special) into
/// `dst_parent` on the destination under the name `desired_name`.
/// Directories recurse when `opts.recursive` is set. Accumulates into
/// `stats` and emits human-readable warnings via `log`.
#[allow(clippy::too_many_arguments)]
pub fn copy_into(
    src: &mut dyn Filesystem,
    src_entry: &FileEntry,
    dst: &mut dyn EditableFilesystem,
    dst_parent: &FileEntry,
    desired_name: &str,
    opts: &CopyOptions,
    stats: &mut CopyStats,
    log: &dyn Fn(&str),
) -> Result<()> {
    let src_caps = Capabilities::infer(src.fs_type());
    let dst_caps = Capabilities::infer(dst.fs_type());
    copy_recursive(
        src,
        &src_caps,
        src_entry,
        dst,
        &dst_caps,
        dst_parent,
        desired_name,
        opts,
        stats,
        log,
    )
}

#[allow(clippy::too_many_arguments)]
fn copy_recursive(
    src: &mut dyn Filesystem,
    src_caps: &Capabilities,
    src_entry: &FileEntry,
    dst: &mut dyn EditableFilesystem,
    dst_caps: &Capabilities,
    dst_parent: &FileEntry,
    desired_name: &str,
    opts: &CopyOptions,
    stats: &mut CopyStats,
    log: &dyn Fn(&str),
) -> Result<()> {
    match src_entry.entry_type {
        EntryType::Directory => {
            if !opts.recursive {
                log(&format!("skip dir (no -r): {}", src_entry.path));
                return Ok(());
            }
            // Flat destination: either flatten children into dst_parent, or
            // refuse with an actionable message.
            if !dst_caps.subdirectories {
                if opts.flatten {
                    for child in list_children(src, src_entry)? {
                        copy_recursive(
                            src,
                            src_caps,
                            &child,
                            dst,
                            dst_caps,
                            dst_parent,
                            &child.name,
                            opts,
                            stats,
                            log,
                        )?;
                    }
                    return Ok(());
                }
                bail!(
                    "destination filesystem ({}) has no subdirectories; pass --flatten to collapse \
                     the tree into the destination directory",
                    dst.fs_type()
                );
            }

            let name = match resolve_name(dst, dst_parent, desired_name, opts, stats, log)? {
                Some(n) => n,
                None => return Ok(()),
            };
            let dir_entry = ensure_directory(src_caps, src_entry, dst, dst_parent, &name)?;
            stats.dirs += 1;
            for child in list_children(src, src_entry)? {
                copy_recursive(
                    src,
                    src_caps,
                    &child,
                    dst,
                    dst_caps,
                    &dir_entry,
                    &child.name,
                    opts,
                    stats,
                    log,
                )?;
            }
            Ok(())
        }
        EntryType::File => copy_file(
            src,
            src_caps,
            src_entry,
            dst,
            dst_caps,
            dst_parent,
            desired_name,
            opts,
            stats,
            log,
        ),
        EntryType::Symlink => {
            if !dst_caps.symlinks {
                log(&format!(
                    "Warning: dropped symlink {} (destination has no symlink support)",
                    src_entry.path
                ));
                return Ok(());
            }
            let name = match resolve_name(dst, dst_parent, desired_name, opts, stats, log)? {
                Some(n) => n,
                None => return Ok(()),
            };
            if let Some(existing) = handle_conflict(dst, dst_parent, &name, opts, stats, log)? {
                if existing {
                    return Ok(());
                }
            }
            let target = src_entry.symlink_target.clone().unwrap_or_default();
            dst.create_symlink(dst_parent, &name, &target, &CreateFileOptions::default())
                .map_err(|e| anyhow!("create_symlink {name}: {e}"))?;
            stats.symlinks += 1;
            Ok(())
        }
        EntryType::Special => {
            log(&format!(
                "skip special file: {} ({})",
                src_entry.path,
                src_entry.special_type.as_deref().unwrap_or("unknown kind")
            ));
            stats.skipped_special += 1;
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn copy_file(
    src: &mut dyn Filesystem,
    src_caps: &Capabilities,
    src_entry: &FileEntry,
    dst: &mut dyn EditableFilesystem,
    dst_caps: &Capabilities,
    dst_parent: &FileEntry,
    desired_name: &str,
    opts: &CopyOptions,
    stats: &mut CopyStats,
    log: &dyn Fn(&str),
) -> Result<()> {
    let name = match resolve_name(dst, dst_parent, desired_name, opts, stats, log)? {
        Some(n) => n,
        None => return Ok(()),
    };
    if let Some(skip) = handle_conflict(dst, dst_parent, &name, opts, stats, log)? {
        if skip {
            return Ok(());
        }
    }

    // Translate whatever metadata the destination can carry.
    let (mut options, mut dropped) = translate_metadata(src_entry, src_caps, dst_caps, opts.attrs);

    // Resource fork is data, not an attribute: copy it whenever the
    // destination supports forks, regardless of AttrPolicy.
    let rsrc_size = src_entry.resource_fork_size.unwrap_or(0);
    if rsrc_size > 0 {
        if dst_caps.resource_forks {
            let mut buf = Vec::with_capacity(rsrc_size as usize);
            src.write_resource_fork_to(src_entry, &mut buf)
                .map_err(|e| anyhow!("reading resource fork of {}: {e}", src_entry.path))?;
            options.resource_fork = Some(ResourceForkSource::Data(buf));
        } else {
            dropped.forks = true;
        }
    }

    // Bridge the data fork: source pushes into a spooled temp (RAM-first),
    // destination pulls from it. Use the bytes actually produced as the
    // length rather than trusting entry.size (hardlink stubs etc. lie).
    let mut spool = tempfile::spooled_tempfile(SPOOL_THRESHOLD);
    let len = src
        .write_file_to(src_entry, &mut spool)
        .map_err(|e| anyhow!("reading {}: {e}", src_entry.path))?;
    spool
        .seek(SeekFrom::Start(0))
        .map_err(|e| anyhow!("rewinding spool for {}: {e}", src_entry.path))?;

    let created = dst
        .create_file(dst_parent, &name, &mut spool, len, &options)
        .map_err(|e| anyhow!("writing {name}: {e}"))?;

    // Finder flags (hasBundle, hasCustomIcon, isInvisible, ...) don't ride in
    // CreateFileOptions; stamp them after creation via set_finder_info — the
    // same path put-binhex / put-macbinary use. Only on Mac destinations
    // (type/creator capable) under Preserve, and only when the source carried
    // non-zero flags. set_finder_info rewrites the whole FInfo, so re-supply
    // the type/creator create_file just wrote. See docs/bug_binhex_finder_flags.md.
    if opts.attrs == AttrPolicy::Preserve && dst_caps.type_creator {
        if let Some(flags) = src_entry.finder_flags.filter(|&f| f != 0) {
            use crate::fs::hfs_common::encode_fourcc;
            let type_code = options
                .type_code
                .as_deref()
                .or(created.type_code.as_deref())
                .map(encode_fourcc)
                .unwrap_or([0; 4]);
            let creator_code = options
                .creator_code
                .as_deref()
                .or(created.creator_code.as_deref())
                .map(encode_fourcc)
                .unwrap_or([0; 4]);
            let mut finfo = [0u8; 16];
            finfo[0..4].copy_from_slice(&type_code);
            finfo[4..8].copy_from_slice(&creator_code);
            finfo[8..10].copy_from_slice(&flags.to_be_bytes());
            if let Err(e) = dst.set_finder_info(&created, finfo, [0u8; 16]) {
                log(&format!(
                    "Warning: could not set Finder flags on {name}: {e}"
                ));
            }
        }
    }

    stats.files += 1;
    stats.bytes = stats.bytes.saturating_add(len);
    if name != desired_name {
        stats.renamed += 1;
        log(&format!(
            "Warning: renamed \"{desired_name}\" -> \"{name}\" (destination name limits)"
        ));
    }
    record_dropped(&dropped, &src_entry.path, stats, log);
    Ok(())
}

/// Build the [`CreateFileOptions`] carrying every metadata axis the
/// destination supports, recording which axes had to be dropped.
fn translate_metadata(
    src: &FileEntry,
    _src_caps: &Capabilities,
    dst_caps: &Capabilities,
    policy: AttrPolicy,
) -> (CreateFileOptions, Dropped) {
    let mut o = CreateFileOptions::default();
    let mut d = Dropped::default();
    if policy == AttrPolicy::Skip {
        // Caller asked to drop FS-specific attributes entirely. On NTFS the
        // new file then inherits the parent folder's ACL (no explicit DACL).
        // Resource forks are handled by the caller (data, not attribute).
        return (o, d);
    }

    // HFS/HFS+ type & creator, ProDOS file type.
    if src.type_code.is_some() || src.creator_code.is_some() {
        if dst_caps.type_creator {
            o.type_code = src.type_code.clone();
            o.creator_code = src.creator_code.clone();
            o.aux_type = src.aux_type;
        } else {
            d.type_creator = true;
        }
    }

    // Unix mode/uid/gid.
    if src.mode.is_some() {
        if dst_caps.unix_permissions {
            o.mode = src.mode;
            o.uid = src.uid;
            o.gid = src.gid;
        } else {
            d.perms = true;
        }
    }

    // DOS attribute bits.
    if src.dos_attributes.is_some() {
        if dst_caps.dos_attributes {
            o.dos_attributes = src.dos_attributes;
        } else {
            d.dos_attrs = true;
        }
    }

    // AmigaDOS protection / comment / datestamp.
    if src.amiga_protection.is_some() || src.amiga_comment.is_some() || src.amiga_date.is_some() {
        if dst_caps.amiga_metadata {
            o.amiga_protection = src.amiga_protection;
            o.amiga_comment = src.amiga_comment.clone();
            o.amiga_dates = src.amiga_date;
        } else {
            d.amiga = true;
        }
    }

    (o, d)
}

fn record_dropped(d: &Dropped, path: &str, stats: &mut CopyStats, log: &dyn Fn(&str)) {
    if d.forks {
        stats.dropped_forks += 1;
        log(&format!(
            "Warning: dropped resource fork of {path} (destination has no fork support)"
        ));
    }
    if d.type_creator {
        stats.dropped_type_creator += 1;
    }
    if d.perms {
        stats.dropped_perms += 1;
    }
    if d.dos_attrs {
        stats.dropped_attrs += 1;
    }
    if d.amiga {
        stats.dropped_amiga += 1;
    }
}

/// Create the destination directory `name` under `parent`, or reuse it if
/// a directory of that name already exists (tree merge). Carries the
/// source directory's Amiga/Unix metadata where supported.
fn ensure_directory(
    src_caps: &Capabilities,
    src_dir: &FileEntry,
    dst: &mut dyn EditableFilesystem,
    parent: &FileEntry,
    name: &str,
) -> Result<FileEntry> {
    if let Some(existing) = find_child(dst, parent, name)? {
        if existing.is_directory() {
            return Ok(existing);
        }
        bail!("cannot create directory {name}: a file of that name already exists");
    }
    let mut opts = CreateDirectoryOptions::default();
    let dst_caps = Capabilities::infer(dst.fs_type());
    if src_caps.unix_permissions && dst_caps.unix_permissions {
        opts.mode = src_dir.mode;
        opts.uid = src_dir.uid;
        opts.gid = src_dir.gid;
    }
    if src_caps.amiga_metadata && dst_caps.amiga_metadata {
        opts.amiga_protection = src_dir.amiga_protection;
        opts.amiga_comment = src_dir.amiga_comment.clone();
        opts.amiga_dates = src_dir.amiga_date;
    }
    dst.create_directory(parent, name, &opts)
        .map_err(|e| anyhow!("create_directory {name}: {e}"))
}

/// Resolve the destination name for `desired`, applying the name policy
/// when the destination filesystem rejects it. Returns `None` when the
/// entry should be skipped (NamePolicy::Skip on an invalid name).
fn resolve_name(
    dst: &mut dyn EditableFilesystem,
    parent: &FileEntry,
    desired: &str,
    opts: &CopyOptions,
    stats: &mut CopyStats,
    log: &dyn Fn(&str),
) -> Result<Option<String>> {
    if dst.validate_name(desired).is_ok() {
        return Ok(Some(desired.to_string()));
    }
    match opts.names {
        NamePolicy::Error => bail!(
            "name \"{desired}\" is invalid for the destination filesystem ({}); \
             pass --names truncate or --names skip",
            dst.fs_type()
        ),
        NamePolicy::Skip => {
            stats.skipped_names += 1;
            log(&format!(
                "skip \"{desired}\": invalid name for destination ({})",
                dst.fs_type()
            ));
            Ok(None)
        }
        NamePolicy::Truncate => {
            let base = mangle_to_fit(dst, desired).ok_or_else(|| {
                anyhow!("could not derive a valid destination name from \"{desired}\"")
            })?;
            let unique = uniquify(dst, parent, &base)?;
            Ok(Some(unique))
        }
    }
}

/// Find the shortest-truncation name the destination accepts, preserving a
/// (possibly shortened) extension. Uses `validate_name` as the oracle so
/// we never hardcode per-filesystem limits.
fn mangle_to_fit(dst: &mut dyn EditableFilesystem, desired: &str) -> Option<String> {
    if dst.validate_name(desired).is_ok() {
        return Some(desired.to_string());
    }
    let (stem, ext) = match desired.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() && !e.is_empty() => (s, Some(e)),
        _ => (desired, None),
    };
    let stem_chars: Vec<char> = stem.chars().collect();
    let ext_chars: Vec<char> = ext.map(|e| e.chars().collect()).unwrap_or_default();

    // Try keeping as much of the extension as possible, then shrink the stem.
    let ext_lens: Vec<usize> = (0..=ext_chars.len()).rev().collect();
    for &elen in &ext_lens {
        for slen in (1..=stem_chars.len()).rev() {
            let mut cand: String = stem_chars[..slen].iter().collect();
            if elen > 0 {
                cand.push('.');
                cand.extend(&ext_chars[..elen]);
            }
            if dst.validate_name(&cand).is_ok() {
                return Some(cand);
            }
        }
    }
    None
}

/// Ensure `base` doesn't collide with an existing destination entry; if it
/// does, insert a `~N` suffix (re-validated against the filesystem) until
/// it's unique. Used only on the mangled-name path so self-collisions from
/// truncation never silently clobber.
fn uniquify(dst: &mut dyn EditableFilesystem, parent: &FileEntry, base: &str) -> Result<String> {
    let existing: HashSet<String> = dst
        .list_directory(parent)
        .map_err(|e| anyhow!("list_directory {}: {e}", parent.path))?
        .into_iter()
        .map(|e| e.name)
        .collect();
    if !existing.contains(base) {
        return Ok(base.to_string());
    }
    let (stem, ext) = match base.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() && !e.is_empty() => (s.to_string(), Some(e.to_string())),
        _ => (base.to_string(), None),
    };
    for n in 1..10000u32 {
        let suffix = format!("~{n}");
        // Trim the stem so stem+suffix(+ext) still validates.
        let mut keep = stem.chars().count();
        loop {
            let trimmed: String = stem.chars().take(keep).collect();
            let mut cand = format!("{trimmed}{suffix}");
            if let Some(e) = &ext {
                cand.push('.');
                cand.push_str(e);
            }
            if !existing.contains(&cand) && dst.validate_name(&cand).is_ok() {
                return Ok(cand);
            }
            if keep == 0 {
                break;
            }
            keep -= 1;
        }
    }
    bail!("could not find a unique destination name for \"{base}\"")
}

/// Apply the conflict policy for an existing entry named `name`. Returns
/// `Some(true)` to skip the source, `Some(false)` after deleting the
/// existing entry (proceed), or `None` when no conflict exists.
fn handle_conflict(
    dst: &mut dyn EditableFilesystem,
    parent: &FileEntry,
    name: &str,
    opts: &CopyOptions,
    stats: &mut CopyStats,
    log: &dyn Fn(&str),
) -> Result<Option<bool>> {
    let Some(existing) = find_child(dst, parent, name)? else {
        return Ok(None);
    };
    match opts.conflict {
        ConflictMode::Error => bail!(
            "{}/{name} already exists on the destination (pass --force or --skip-existing)",
            parent.path.trim_end_matches('/')
        ),
        ConflictMode::SkipExisting => {
            stats.skipped_existing += 1;
            log(&format!(
                "skip (exists): {}/{name}",
                parent.path.trim_end_matches('/')
            ));
            Ok(Some(true))
        }
        ConflictMode::Force => {
            if existing.is_directory() {
                dst.delete_recursive(parent, &existing)
                    .map_err(|e| anyhow!("removing existing {name}: {e}"))?;
            } else {
                dst.delete_entry(parent, &existing)
                    .map_err(|e| anyhow!("removing existing {name}: {e}"))?;
            }
            Ok(Some(false))
        }
    }
}

fn find_child(
    dst: &mut dyn EditableFilesystem,
    parent: &FileEntry,
    name: &str,
) -> Result<Option<FileEntry>> {
    Ok(dst
        .list_directory(parent)
        .map_err(|e| anyhow!("list_directory {}: {e}", parent.path))?
        .into_iter()
        .find(|e| e.name == name))
}

fn list_children(src: &mut dyn Filesystem, dir: &FileEntry) -> Result<Vec<FileEntry>> {
    src.list_directory(dir)
        .map_err(|e| anyhow!("list_directory {}: {e}", dir.path))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Disk-to-disk copy of a Mac file must preserve its Finder flags
    /// (`fdFlags`), so harvested apps keep their hasBundle/custom-icon bits.
    /// Regression for docs/bug_binhex_finder_flags.md (the `cp` half).
    #[test]
    fn copy_preserves_finder_flags_hfs_to_hfs() {
        use crate::fs::hfs::{create_blank_hfs, HfsFilesystem};
        use std::io::Cursor;

        let mut src = HfsFilesystem::open(
            Cursor::new(create_blank_hfs(4 * 1024 * 1024, 512, "Src").unwrap()),
            0,
        )
        .unwrap();
        let sroot = src.root().unwrap();
        let body = b"app";
        let fe = src
            .create_file(
                &sroot,
                "Game",
                &mut Cursor::new(body.as_slice()),
                body.len() as u64,
                &CreateFileOptions::default(),
            )
            .unwrap();
        let mut finfo = [0u8; 16];
        finfo[0..4].copy_from_slice(b"APPL");
        finfo[4..8].copy_from_slice(b"Po.P");
        finfo[8..10].copy_from_slice(&0x2000u16.to_be_bytes()); // hasBundle
        src.set_finder_info(&fe, finfo, [0u8; 16]).unwrap();

        // Read back so the FileEntry carries finder_flags, as a real copy would.
        let src_entry = src
            .list_directory(&sroot)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "Game")
            .unwrap();
        assert_eq!(src_entry.finder_flags, Some(0x2000));

        let mut dst = HfsFilesystem::open(
            Cursor::new(create_blank_hfs(4 * 1024 * 1024, 512, "Dst").unwrap()),
            0,
        )
        .unwrap();
        let droot = dst.root().unwrap();

        let mut stats = CopyStats::default();
        copy_into(
            &mut src,
            &src_entry,
            &mut dst,
            &droot,
            "Game",
            &CopyOptions::default(),
            &mut stats,
            &|_| {},
        )
        .unwrap();

        let copied = dst
            .list_directory(&droot)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "Game")
            .unwrap();
        assert_eq!(
            copied.finder_flags,
            Some(0x2000),
            "fdFlags must survive disk-to-disk cp"
        );
    }

    #[test]
    fn caps_hfs_has_forks_and_type_creator() {
        let c = Capabilities::infer("HFS");
        assert!(c.resource_forks && c.type_creator && c.subdirectories);
        assert!(!c.unix_permissions && !c.dos_attributes);
        let p = Capabilities::infer("HFS+");
        assert!(p.resource_forks && p.type_creator);
    }

    #[test]
    fn caps_fat_has_dos_attrs_no_forks() {
        for t in ["FAT12", "FAT16", "FAT32", "exFAT", "Human68k (FAT12)"] {
            let c = Capabilities::infer(t);
            assert!(c.dos_attributes, "{t} should carry dos attrs");
            assert!(!c.resource_forks, "{t} has no forks");
            assert!(c.subdirectories, "{t} is hierarchical");
        }
    }

    #[test]
    fn caps_unix_families() {
        for t in [
            "ext2",
            "ext4",
            "btrfs",
            "XFS v5",
            "UFS2",
            "ReiserFS 3.6",
            "EFS",
            "JFS2",
        ] {
            let c = Capabilities::infer(t);
            assert!(c.unix_permissions && c.symlinks, "{t} should be unix-y");
        }
    }

    #[test]
    fn caps_flat_filesystems() {
        for t in [
            "CP/M",
            "Acorn DFS",
            "DOS 3.3",
            "Atari DOS 2",
            "RS-DOS",
            "CBM DOS (1541)",
            "MFS",
        ] {
            assert!(!Capabilities::infer(t).subdirectories, "{t} should be flat");
        }
    }

    #[test]
    fn caps_amiga_family() {
        for t in ["OFS", "FFS Intl", "PFS3", "SFS", "AFS"] {
            let c = Capabilities::infer(t);
            assert!(c.amiga_metadata, "{t} should carry amiga metadata");
        }
    }

    #[test]
    fn translate_drops_unsupported_axes() {
        // HFS-ish source file → FAT-ish dest: type/creator dropped.
        let mut src = FileEntry::new_file("doc.txt".into(), "/doc.txt".into(), 10, 0);
        src.type_code = Some("TEXT".into());
        src.creator_code = Some("ttxt".into());
        src.resource_fork_size = Some(40);
        let dst = Capabilities::infer("FAT16");
        let (opts, dropped) = translate_metadata(
            &src,
            &Capabilities::infer("HFS"),
            &dst,
            AttrPolicy::Preserve,
        );
        assert!(opts.type_code.is_none());
        assert!(dropped.type_creator);
        // forks are handled in copy_file, not translate_metadata
        assert!(!dropped.forks);
    }

    #[test]
    fn translate_carries_dos_attrs_fat_to_fat() {
        let mut src = FileEntry::new_file("a.sys".into(), "/a.sys".into(), 1, 0);
        src.dos_attributes = Some(0x01 | 0x04); // RO+SYS
        let (opts, dropped) = translate_metadata(
            &src,
            &Capabilities::infer("FAT16"),
            &Capabilities::infer("FAT32"),
            AttrPolicy::Preserve,
        );
        assert_eq!(opts.dos_attributes, Some(0x05));
        assert!(!dropped.dos_attrs);
    }

    #[test]
    fn translate_skip_policy_carries_nothing() {
        let mut src = FileEntry::new_file("a".into(), "/a".into(), 1, 0);
        src.mode = Some(0o100644);
        src.dos_attributes = Some(0x20);
        let (opts, dropped) = translate_metadata(
            &src,
            &Capabilities::infer("ext4"),
            &Capabilities::infer("ext4"),
            AttrPolicy::Skip,
        );
        assert!(opts.mode.is_none() && opts.dos_attributes.is_none());
        // Skip means "intentionally not carried", not "dropped because unsupported".
        assert!(!dropped.perms && !dropped.dos_attrs);
    }
}
