//! Resolve `IMG[@N]` references to a (file handle, partition context)
//! pair that the filesystem-dispatch helpers in `src/fs/mod.rs` can
//! consume directly.
//!
//! The CLI verbs use this to support every filesystem the engine layer
//! knows about — instead of each verb hand-rolling APM/MBR/GPT/RDB
//! parsing and HFS-specific dispatch, they go through
//! [`resolve_partition`] and get back the metadata `open_filesystem` /
//! `open_editable_filesystem` need.

use anyhow::{anyhow, bail, Context, Result};
use std::fs::File;
use std::io::{Read, Seek};

use crate::cli::backup_edit;
use crate::cli::io::{open_image_ro, open_image_rw};
use crate::model::source_reader;
use crate::partition::{PartitionInfo, PartitionTable};
use crate::rbformats::{BoxReadSeek, BoxRwSeek};

#[cfg(feature = "chd")]
use crate::cli::logging::log_stderr;
#[cfg(feature = "chd")]
use crate::rbformats::chd_edit;
#[cfg(feature = "chd")]
use std::path::{Path, PathBuf};

/// Resolved partition context — what to pass to
/// [`crate::fs::open_filesystem`] / [`crate::fs::open_editable_filesystem`].
#[derive(Debug, Clone)]
pub struct PartitionContext {
    /// Byte offset of the filesystem inside the image.
    pub offset: u64,
    /// Partition type byte (MBR-style). `0x00` for "auto-detect via
    /// superfloppy probing" — `open_filesystem` walks the magic
    /// signatures itself.
    pub type_byte: u8,
    /// APM / RDB type string (e.g. `"Apple_HFS"`, `"PFS\\3"`). `None`
    /// for MBR / GPT / raw superfloppy.
    pub type_string: Option<String>,
    /// Partition size in bytes. For raw superfloppies, the image's
    /// total length.
    pub size: u64,
    /// Human-readable label, intended for `eprintln!` so the user can
    /// confirm which partition the verb is operating on.
    pub label: String,
}

/// Open `path` read-only and resolve which partition to use.
///
/// - When `selector` is `None` and the image has no partition table,
///   returns `offset=0, type_byte=0` (auto-detect at byte 0).
/// - When `selector` is `None` and there's a single FS-shaped partition,
///   that partition is selected.
/// - When `selector` is `None` and multiple FS partitions exist, errors
///   with a list of indices.
/// - When `selector` is `Some(n)`, the 1-based index is used; out-of-
///   range fires `NOT_FOUND`.
pub fn resolve_partition_ro(
    path: &std::path::Path,
    selector: Option<u32>,
) -> Result<(File, PartitionContext)> {
    let mut file = open_image_ro(path)?;
    let ctx = resolve(&mut file, selector)?;
    Ok((file, ctx))
}

/// Returned by [`resolve_partition_rw`] alongside the read+write handle.
/// Call [`RwCommit::commit`] once a mutation has succeeded (after the
/// `EditableFilesystem` has been synced) to persist it.
///
/// Three shapes, matching how the GUI persists each source kind:
/// - [`RwCommit::None`] — raw image / device: writes already landed; no-op.
/// - [`RwCommit::Container`] — floppy (.d88 / .xdf / .hdm / .dim / .atr) or
///   gzip (.adz / .hdz) container: the handle points at a decoded temp flat,
///   and `commit` re-encodes it back into the container, atomically replacing
///   the original (the same `ContainerEditSession` path the GUI uses).
/// - [`RwCommit::Chd`] — a CHD edited via `chd_edit`: an uncompressed CHD was
///   mutated in place (`diff: None`, no-op commit); a compressed CHD routed
///   writes into a diff (`diff: Some`) that `commit` merges + recompresses
///   back over the original, logging each step.
///
/// **Dropping without committing discards container / compressed-CHD edits**,
/// so a verb that errors out before `commit` leaves the original untouched.
#[must_use = "call commit() to persist edits made to a container"]
pub enum RwCommit {
    /// Raw image / device — writes already landed in place.
    None,
    /// Floppy or gzip container — re-encode the temp flat on commit.
    Container(crate::model::container_edit::ContainerEditSession),
    /// CHD edited through `chd_edit`. `diff: None` is an in-place
    /// uncompressed edit (nothing to do on commit); `diff: Some(path)` is a
    /// compressed-CHD diff that must be flattened back over `parent`.
    #[cfg(feature = "chd")]
    Chd {
        parent: PathBuf,
        diff: Option<PathBuf>,
    },
    /// A partition inside a backup folder, edited via its decompressed temp
    /// flat — recompressed back over the archive + `metadata.json` rewritten on
    /// commit. See [`crate::cli::backup_edit`].
    BackupArchive(crate::cli::backup_edit::BackupArchiveCommit),
    /// A partition inside a `.cbk` container: the container was materialized to
    /// `temp_folder`, the partition edited via the inner backup-folder commit,
    /// and on commit the inner commit runs (recompress + metadata) and the
    /// folder is repacked over the original `.cbk` (write-to-temp + rename).
    Cbk {
        inner: Box<RwCommit>,
        temp_folder: tempfile::TempDir,
        cbk_path: PathBuf,
    },
}

impl RwCommit {
    /// Persist the edit. No-op for raw images / in-place CHDs; re-encodes for
    /// containers; merges + recompresses for compressed CHDs.
    pub fn commit(self) -> Result<()> {
        match self {
            RwCommit::None => Ok(()),
            RwCommit::Container(session) => {
                let fmt = session.format_name();
                session
                    .commit()
                    .map_err(|e| anyhow!("re-encoding {fmt} container: {e:#}"))
            }
            #[cfg(feature = "chd")]
            RwCommit::Chd { diff: None, .. } => Ok(()),
            #[cfg(feature = "chd")]
            RwCommit::Chd {
                parent,
                diff: Some(diff),
            } => flatten_chd_with_progress(&parent, &diff),
            RwCommit::BackupArchive(commit) => commit.commit(),
            RwCommit::Cbk {
                inner,
                temp_folder,
                cbk_path,
            } => {
                // Persist the partition edit into the materialized folder
                // (recompress partition-N.gz + rewrite metadata.json), then
                // repack the folder over the original .cbk atomically.
                inner.commit()?;
                let tmp_out = cbk_path.with_extension("cbk.tmp");
                crate::rbformats::cbk::pack_folder_to_cbk(temp_folder.path(), &tmp_out)
                    .with_context(|| format!("repacking {}", cbk_path.display()))?;
                std::fs::rename(&tmp_out, &cbk_path).with_context(|| {
                    format!(
                        "replacing {} with the repacked container",
                        cbk_path.display()
                    )
                })?;
                Ok(())
            }
        }
    }
}

/// Merge a compressed CHD's edit diff back over the original via
/// [`chd_edit::flatten_to_parent`], surfacing each phase + recompression
/// progress to stderr so a multi-GB flatten doesn't look hung. Mirrors the
/// GUI's background flatten, minus the worker thread (the CLI is one-shot).
#[cfg(feature = "chd")]
fn flatten_chd_with_progress(parent: &Path, diff: &Path) -> Result<()> {
    log_stderr("Saving CHD edits: merging diff and recompressing the image...");
    let mut last_logged_mb: u64 = 0;
    let mut progress_cb = |bytes: u64| {
        let mb = bytes / (1024 * 1024);
        if mb >= last_logged_mb + 256 {
            last_logged_mb = mb;
            log_stderr(format!("  recompressed {mb} MB so far..."));
        }
    };
    let cancel = || false;
    let mut log_cb = |msg: &str| log_stderr(format!("  {msg}"));
    chd_edit::flatten_to_parent(parent, diff, None, &mut progress_cb, &cancel, &mut log_cb)
        .map_err(|e| anyhow!("recompressing edited CHD: {e:#}"))?;
    log_stderr("CHD edits saved.");
    Ok(())
}

/// Sibling diff path for a compressed CHD edit: `<stem>.edit-diff.chd`.
/// Matches the GUI's naming (`browse_view::diff_path_for`) so a diff left by
/// an interrupted edit is recognizable.
#[cfg(feature = "chd")]
fn chd_diff_path(parent: &Path) -> PathBuf {
    let mut name = parent
        .file_stem()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    name.push(".edit-diff.chd");
    parent.with_file_name(name)
}

/// Same as [`resolve_partition_ro`] but the handle is open read+write.
/// Caller is responsible for ensuring the path resolves to a regular file
/// (device-write safety lives in Phase C).
///
/// Returns a boxed `Read + Write + Seek` handle plus a [`RwCommit`] the
/// caller must `commit()` after a successful mutation. The handle is boxed
/// (rather than a bare [`File`]) so a CHD edit session — a `Read + Write +
/// Seek` adapter, not a `File` — can flow through the same path. For a
/// container or compressed CHD the handle is backed by a temp flat / diff and
/// `commit` re-encodes / flattens. See [`RwCommit`].
pub fn resolve_partition_rw(
    path: &std::path::Path,
    selector: Option<u32>,
) -> Result<(BoxRwSeek, PartitionContext, RwCommit)> {
    resolve_partition_rw_forced(path, selector, None)
}

/// As [`resolve_partition_rw`], but a `--fs-type` override (`fs_override`)
/// makes a partition-table detection failure non-fatal — see
/// [`resolve_with_override`]. Used by `put` / `rm`, which then call
/// [`FsDispatchOverride::apply`] to install the override.
pub fn resolve_partition_rw_forced(
    path: &std::path::Path,
    selector: Option<u32>,
    fs_override: Option<&str>,
) -> Result<(BoxRwSeek, PartitionContext, RwCommit)> {
    // A `.cbk` container: materialize it to a temp folder, edit the partition
    // there via the backup-folder path, and repack over the original `.cbk` on
    // commit (the "additional legwork" edit path — cb_dos_network_and_state.md
    // §2e). Read access to a `.cbk` is native (source_reader); editing repacks.
    if crate::rbformats::cbk::is_cbk(path) {
        let temp = tempfile::Builder::new()
            .prefix(".rb-cbk-edit-")
            .tempdir()
            .context("creating temp folder for .cbk edit")?;
        crate::rbformats::cbk::materialize_cbk_to_folder(path, temp.path())
            .with_context(|| format!("materializing {} for edit", path.display()))?;
        let (handle, ctx, inner) = backup_edit::open_backup_partition_rw(temp.path(), selector)?;
        return Ok((
            handle,
            ctx,
            RwCommit::Cbk {
                inner: Box::new(inner),
                temp_folder: temp,
                cbk_path: path.to_path_buf(),
            },
        ));
    }

    // A backup folder stores each partition as a compressed file governed by
    // metadata.json, not as a partition inside one image — handle it before
    // the whole-image path (which would try to detect a partition table).
    if backup_edit::is_backup_folder(path) {
        return backup_edit::open_backup_partition_rw(path, selector);
    }

    // Open the whole image read-write (decoding CHD / container as needed),
    // then resolve which partition inside it the caller wants.
    let (mut reader, commit) = resolve_image_rw(path)?;
    let ctx = resolve_with_override(&mut reader, selector, fs_override)?;
    Ok((reader, ctx, commit))
}

/// Open `path` read-write as a boxed whole-image `Read + Write + Seek` handle,
/// decoding a CHD or editable container to its editable backing (a `chd_edit`
/// diff / in-place session, or a `ContainerEditSession` temp flat) with a
/// [`RwCommit`] that flattens / re-encodes on commit. Unlike
/// [`resolve_partition_rw`] this does NOT resolve a partition — the caller
/// works in absolute image offsets. Used by whole-disk verbs (`partmap`,
/// `mac_scsi_bless`) so they edit a CHD / container the same way `put` does.
pub fn resolve_image_rw(path: &std::path::Path) -> Result<(BoxRwSeek, RwCommit)> {
    if let Some(chd) = try_open_chd_rw(path)? {
        return Ok(chd);
    }
    if source_reader::is_editable_container_path(path) {
        // Floppy / gzip / WOZ container: decode to a temp flat, edit that,
        // re-encode on commit. open_image_rw on the temp gives the same File
        // the raw path would, so downstream dispatch is identical.
        let session = crate::model::container_edit::ContainerEditSession::open(path)
            .map_err(|e| anyhow!("opening container for edit: {e:#}"))?;
        let file = open_image_rw(session.flat_path())?;
        Ok((Box::new(file), RwCommit::Container(session)))
    } else {
        let file = open_image_rw(path)?;
        Ok((Box::new(file), RwCommit::None))
    }
}

/// CHD read-write open: an uncompressed CHD is mutated in place; a compressed
/// CHD is opened read-only with writes routed into a fresh diff that is
/// flattened back over the original on commit. Mirrors the GUI
/// (`browse_view::enter_chd_edit_mode`). `Ok(None)` when `path` is not a CHD
/// (caller falls through to the container / raw branches).
#[cfg(feature = "chd")]
fn try_open_chd_rw(path: &Path) -> Result<Option<(BoxRwSeek, RwCommit)>> {
    if !source_reader::is_chd_path(path) {
        return Ok(None);
    }
    let compressed = chd_edit::is_compressed_chd(path)
        .map_err(|e| anyhow!("inspecting CHD {}: {e:#}", path.display()))?;
    let resolved: (BoxRwSeek, RwCommit) = if compressed {
        let backup = chd_edit::make_backup_copy(path)
            .map_err(|e| anyhow!("backing up CHD before edit: {e:#}"))?;
        log_stderr(format!(
            "Editing compressed CHD (original preserved at {})",
            backup.display()
        ));
        let diff = chd_diff_path(path);
        if diff.exists() {
            // Leftover diff from an interrupted edit — the parent was never
            // touched, so discard it and start clean.
            let _ = std::fs::remove_file(&diff);
        }
        let session = chd_edit::ChdEditSession::open_with_diff(path, &diff)
            .map_err(|e| anyhow!("opening CHD {} for edit: {e:#}", path.display()))?;
        (
            Box::new(session),
            RwCommit::Chd {
                parent: path.to_path_buf(),
                diff: Some(diff),
            },
        )
    } else {
        let session = chd_edit::ChdEditSession::open_uncompressed(path)
            .map_err(|e| anyhow!("opening CHD {} for edit: {e:#}", path.display()))?;
        (
            Box::new(session),
            RwCommit::Chd {
                parent: path.to_path_buf(),
                diff: None,
            },
        )
    };
    Ok(Some(resolved))
}

/// No-CHD-feature stub: a CHD path falls through to the raw branch (which then
/// fails at partition detection, same as before — a binary without the `chd`
/// feature can't read CHDs at all).
#[cfg(not(feature = "chd"))]
fn try_open_chd_rw(_path: &std::path::Path) -> Result<Option<(BoxRwSeek, RwCommit)>> {
    Ok(None)
}

/// Like [`resolve_partition_ro`] but returns a boxed `Read + Seek`,
/// transparently routing GHO / IMZ / CHD containers through their
/// streaming readers so the caller sees a raw disk image.
pub fn resolve_partition_streaming(
    path: &std::path::Path,
    selector: Option<u32>,
) -> Result<(BoxReadSeek, PartitionContext)> {
    resolve_partition_streaming_with_password(path, selector, None)
}

/// Variant of [`resolve_partition_streaming`] that accepts an optional
/// password for encrypted IMZ files. Pass `None` to behave like
/// [`resolve_partition_streaming`].
pub fn resolve_partition_streaming_with_password(
    path: &std::path::Path,
    selector: Option<u32>,
    password: Option<&[u8]>,
) -> Result<(BoxReadSeek, PartitionContext)> {
    resolve_partition_streaming_forced(path, selector, password, None)
}

/// As [`resolve_partition_streaming_with_password`], but a `--fs-type`
/// override (`fs_override`) makes a partition-table detection failure
/// non-fatal — see [`resolve_with_override`]. Used by `ls` / `get`, which
/// then call [`FsDispatchOverride::apply`] to install the override.
pub fn resolve_partition_streaming_forced(
    path: &std::path::Path,
    selector: Option<u32>,
    password: Option<&[u8]>,
    fs_override: Option<&str>,
) -> Result<(BoxReadSeek, PartitionContext)> {
    resolve_partition_streaming_forced_inside(path, selector, password, fs_override, None)
}

/// As [`resolve_partition_streaming_forced`], but `inside` names a specific
/// entry to open when `path` is a `.zip` holding more than one disk image
/// (the CLI `--inside` flag). Ignored for every non-zip source.
pub fn resolve_partition_streaming_forced_inside(
    path: &std::path::Path,
    selector: Option<u32>,
    password: Option<&[u8]>,
    fs_override: Option<&str>,
    inside: Option<&str>,
) -> Result<(BoxReadSeek, PartitionContext)> {
    // A backup folder stores each partition as a compressed file governed by
    // metadata.json; decompress the selected one to a temp flat (read-only) so
    // get / ls / inspect see it like any other raw partition.
    if backup_edit::is_backup_folder(path) {
        return backup_edit::open_backup_partition_ro(path, selector);
    }

    // Peel any container *and* any image wrapper through the one shared
    // primitive so the CLI probes a source identically to the GUI: CHD / GHO /
    // IMZ / .zip-wrapped / flat-floppy containers decode to a flat stream, and
    // VHD / 2MG / DMG / DiskCopy 4.2 wrappers are unwrapped (previously the CLI
    // streaming path saw the wrapped bytes for those and mis-detected the
    // partition table). A raw image falls through to a buffered file.
    let mut reader = source_reader::open_peeled_read_with_entry(path, password, inside)?;
    let ctx = resolve_with_override(&mut reader, selector, fs_override)?;
    Ok((reader, ctx))
}

fn resolve<R: Read + Seek>(reader: &mut R, selector: Option<u32>) -> Result<PartitionContext> {
    resolve_with_override(reader, selector, None)
}

/// Resolve which partition a `Read + Seek` source's `IMG[@N]` selector points at,
/// returning the offset / type / size context — the reader-based sibling of
/// [`resolve_partition_ro`]. Used for sources that aren't a local file handle
/// (e.g. a [`crate::remote::RemoteBlockReader`] over the block tier), so a remote
/// image gets the **exact same** `@N` semantics as a local one.
pub fn resolve_partition_in_reader<R: Read + Seek>(
    reader: &mut R,
    selector: Option<u32>,
) -> Result<PartitionContext> {
    resolve(reader, selector)
}

/// As [`resolve`], but `fs_override` (the `--fs-type` value, if any) makes
/// a partition-table detection *failure* non-fatal: when the user has
/// explicitly declared a filesystem, an image with neither a partition
/// table nor an on-disk FS signature (the defining shape of a flat CP/M
/// floppy — the BIOS holds the DPB out-of-band) is treated as a raw FS at
/// byte 0. The override itself is applied by the caller afterwards via
/// [`FsDispatchOverride::apply`].
fn resolve_with_override<R: Read + Seek>(
    reader: &mut R,
    selector: Option<u32>,
    fs_override: Option<&str>,
) -> Result<PartitionContext> {
    let total = reader_size(reader)?;
    let pt = match PartitionTable::detect(reader) {
        Ok(pt) => pt,
        Err(e) => {
            if fs_override.is_some() {
                if let Some(idx) = selector {
                    bail!(
                        "--partition / IMG@{idx} was given with --fs-type, but the image has no \
                         partition table; drop the suffix to operate on the raw filesystem at byte 0"
                    );
                }
                return Ok(PartitionContext {
                    offset: 0,
                    type_byte: 0x00,
                    type_string: None,
                    size: total,
                    label: "Partition: raw filesystem @ byte 0 (forced via --fs-type)".to_string(),
                });
            }
            return Err(anyhow!("detecting partition table: {e}"));
        }
    };
    let partitions = pt.partitions();

    // No partition table: treat as superfloppy / raw FS at byte 0.
    if partitions.is_empty() {
        if let Some(idx) = selector {
            bail!(
                "--partition / IMG@N specified ({idx}) but image has no partition table; \
                 drop the suffix to operate on the raw filesystem at byte 0"
            );
        }
        return Ok(PartitionContext {
            offset: 0,
            type_byte: 0x00,
            type_string: None,
            size: total,
            label: format!("Partition: raw filesystem @ byte 0 ({})", pt.type_name()),
        });
    }

    let info = match selector {
        Some(idx) => {
            let i = idx as usize;
            if i == 0 || i > partitions.len() {
                bail!(
                    "partition index {idx} out of range (image has {} partition(s))",
                    partitions.len()
                );
            }
            // PartitionInfo `index` may be 0- or 1-based depending on PT
            // type, so we trust the `Vec` ordering instead.
            partitions[i - 1].clone()
        }
        None => pick_default_partition(&partitions)?,
    };

    Ok(PartitionContext {
        offset: info.byte_offset(),
        type_byte: info.partition_type_byte,
        type_string: info.partition_type_string.clone(),
        size: info.size_bytes,
        label: format_label(&info, pt.type_name()),
    })
}

/// Default-partition picker.
///
/// - If exactly one partition has a filesystem-shaped type (i.e. not an
///   extended container, not bootable-only, not Apple_Driver*), pick it.
/// - Otherwise raise an error listing the candidates.
fn pick_default_partition(partitions: &[PartitionInfo]) -> Result<PartitionInfo> {
    let candidates: Vec<&PartitionInfo> = partitions
        .iter()
        .filter(|p| {
            !p.is_extended_container
                && !p
                    .partition_type_string
                    .as_deref()
                    .map(|s| s.starts_with("Apple_Driver") || s == "Apple_partition_map")
                    .unwrap_or(false)
        })
        .collect();
    match candidates.len() {
        0 => bail!("no usable partition found in image"),
        1 => Ok(candidates[0].clone()),
        _ => {
            let summary: Vec<String> = candidates
                .iter()
                .map(|p| {
                    format!(
                        "  {}  {}  ({})",
                        p.index, p.type_name, p.partition_type_byte
                    )
                })
                .collect();
            bail!(
                "image has multiple FS partitions; pass `IMG@N` or `--partition N` to pick one:\n{}",
                summary.join("\n")
            )
        }
    }
}

fn format_label(info: &PartitionInfo, pt_name: &str) -> String {
    match &info.partition_type_string {
        Some(s) => format!(
            "Partition {} ({pt_name}): {} {s} @ LBA {}, {} bytes",
            info.index, info.type_name, info.start_lba, info.size_bytes
        ),
        None => format!(
            "Partition {} ({pt_name}): {} 0x{:02x} @ LBA {}, {} bytes",
            info.index, info.type_name, info.partition_type_byte, info.start_lba, info.size_bytes
        ),
    }
}

fn reader_size<R: Seek>(reader: &mut R) -> Result<u64> {
    let cur = reader.stream_position()?;
    let end = reader.seek(std::io::SeekFrom::End(0))?;
    reader.seek(std::io::SeekFrom::Start(cur))?;
    Ok(end)
}

/// Flatten-able CLI flag group: an explicit filesystem-dispatch override.
///
/// CP/M floppies (Altair, Amstrad, PCW, Einstein, SVI328, MultiComp, ZX
/// +3) carry NO on-disk signature for their FS — the BIOS knows the
/// Disk Parameter Block (DPB) out-of-band. So `open_filesystem` can't
/// autodetect them, and every rb-cli verb operating on a CP/M image
/// needs the user to declare the DPB via `--fs-type cpm:<preset_name>`.
///
/// The same flag can in principle override dispatch for any other
/// `partition_type_string` the engine recognises (e.g. `human68k`,
/// `qdos`, `Apple_HFS`), but the headline use is the CP/M family.
///
/// Flatten into a verb's `Args` and call
/// [`FsDispatchOverride::apply`] right after `resolve_partition_*`.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct FsDispatchOverride {
    /// Force a specific filesystem dispatch. The main use is `cpm:<preset>`
    /// for CP/M images (which have no on-disk signature). Valid CP/M
    /// presets: `amstrad_data`, `amstrad_sys`, `amstrad_pcw`, `einstein`,
    /// `svi328_cpm`, `altair_8in`, `altair_cf`, `multicomp`, `zx_plus3`.
    /// Other strings (e.g. `human68k`, `qdos`) are also accepted and
    /// forwarded to the partition_type_string dispatch.
    #[arg(long = "fs-type", value_name = "TYPE")]
    pub fs_type: Option<String>,

    /// Scan the **entire** image for recoverable text in the synthetic carve
    /// view (used for disks with no recognized filesystem — e.g. custom
    /// bootblock Amiga "NDOS" disks). By default the carve view only scans
    /// the first 10 MB. No effect on disks with a real filesystem.
    #[arg(long = "carve-full")]
    pub carve_full: bool,
}

impl FsDispatchOverride {
    /// Apply the override to a resolved [`PartitionContext`] in place.
    /// No-op when `--fs-type` wasn't passed.
    ///
    /// The override replaces `ctx.type_string` and clears `ctx.type_byte`
    /// so the string-dispatch branch in `open_filesystem` wins. Updates
    /// `ctx.label` so the user can confirm the override is in effect.
    ///
    /// Also installs the process-wide carve scan policy from `--carve-full`
    /// so a subsequent carve open scans the whole image instead of the
    /// default first-10-MB window.
    pub fn apply(&self, ctx: &mut PartitionContext) {
        crate::fs::carve::set_full_scan(self.carve_full);
        if let Some(t) = &self.fs_type {
            ctx.type_string = Some(t.clone());
            ctx.type_byte = 0; // Force string-dispatch
            ctx.label = format!("{} [--fs-type {}]", ctx.label, t);
        }
    }
}
