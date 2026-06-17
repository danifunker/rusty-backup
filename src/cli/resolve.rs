//! Resolve `IMG[@N]` references to a (file handle, partition context)
//! pair that the filesystem-dispatch helpers in `src/fs/mod.rs` can
//! consume directly.
//!
//! The CLI verbs use this to support every filesystem the engine layer
//! knows about — instead of each verb hand-rolling APM/MBR/GPT/RDB
//! parsing and HFS-specific dispatch, they go through
//! [`resolve_partition`] and get back the metadata `open_filesystem` /
//! `open_editable_filesystem` need.

use anyhow::{anyhow, bail, Result};
use std::fs::File;
use std::io::{Read, Seek};

use crate::cli::io::{open_image_ro, open_image_rw};
use crate::model::source_reader;
use crate::partition::{PartitionInfo, PartitionTable};
use crate::rbformats::BoxReadSeek;

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

/// Returned by [`resolve_partition_rw`] alongside the read+write file handle.
/// Call [`RwCommit::commit`] once a mutation has succeeded (after the
/// `EditableFilesystem` has been synced) to persist it.
///
/// For a raw image this is a no-op — writes already landed in the file. For a
/// floppy container (.d88 / .xdf / .hdm / .dim) the file handle points at a
/// decoded temp flat, and `commit` re-encodes that flat back into the
/// container format, atomically replacing the original. **Dropping without
/// committing discards container edits**, so a verb that errors out before
/// calling `commit` leaves the original container untouched.
#[must_use = "call commit() to persist edits made to a container"]
pub struct RwCommit {
    session: Option<crate::model::container_edit::ContainerEditSession>,
}

impl RwCommit {
    /// Persist the edit. No-op for raw images; re-encodes for containers.
    pub fn commit(self) -> Result<()> {
        match self.session {
            Some(session) => {
                let fmt = session.format_name();
                session
                    .commit()
                    .map_err(|e| anyhow!("re-encoding {fmt} container: {e:#}"))
            }
            None => Ok(()),
        }
    }
}

/// Same as [`resolve_partition_ro`] but the file handle is open
/// read+write. Caller is responsible for ensuring the path resolves to
/// a regular file (device-write safety lives in Phase C).
///
/// Returns a [`RwCommit`] the caller must `commit()` after a successful
/// mutation. For floppy containers the returned file handle is a decoded temp
/// flat; `commit` re-encodes it back into the container. See [`RwCommit`].
pub fn resolve_partition_rw(
    path: &std::path::Path,
    selector: Option<u32>,
) -> Result<(File, PartitionContext, RwCommit)> {
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
) -> Result<(File, PartitionContext, RwCommit)> {
    if source_reader::is_editable_container_path(path) {
        // Floppy container: decode to a temp flat, edit that, re-encode on
        // commit. open_image_rw on the temp gives the same File the raw path
        // would, so downstream dispatch is identical.
        let session = crate::model::container_edit::ContainerEditSession::open(path)
            .map_err(|e| anyhow!("opening container for edit: {e:#}"))?;
        let mut file = open_image_rw(session.flat_path())?;
        let ctx = resolve_with_override(&mut file, selector, fs_override)?;
        Ok((
            file,
            ctx,
            RwCommit {
                session: Some(session),
            },
        ))
    } else {
        let mut file = open_image_rw(path)?;
        let ctx = resolve_with_override(&mut file, selector, fs_override)?;
        Ok((file, ctx, RwCommit { session: None }))
    }
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
    // Peel any container *and* any image wrapper through the one shared
    // primitive so the CLI probes a source identically to the GUI: CHD / GHO /
    // IMZ / flat-floppy containers decode to a flat stream, and VHD / 2MG / DMG
    // / DiskCopy 4.2 wrappers are unwrapped (previously the CLI streaming path
    // saw the wrapped bytes for those and mis-detected the partition table). A
    // raw image falls through to a buffered file.
    let mut reader = source_reader::open_peeled_read(path, password)?;
    let ctx = resolve_with_override(&mut reader, selector, fs_override)?;
    Ok((reader, ctx))
}

fn resolve<R: Read + Seek>(reader: &mut R, selector: Option<u32>) -> Result<PartitionContext> {
    resolve_with_override(reader, selector, None)
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
