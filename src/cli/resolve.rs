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

/// Same as [`resolve_partition_ro`] but the file handle is open
/// read+write. Caller is responsible for ensuring the path resolves to
/// a regular file (device-write safety lives in Phase C).
pub fn resolve_partition_rw(
    path: &std::path::Path,
    selector: Option<u32>,
) -> Result<(File, PartitionContext)> {
    let mut file = open_image_rw(path)?;
    let ctx = resolve(&mut file, selector)?;
    Ok((file, ctx))
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
    // Anything `source_reader::open_read_with_password` would unwrap
    // (CHD / GHO / IMZ streaming readers, MSA decoded floppies, 140 KB
    // Apple-II .do/.po/.dsk floppies) must NOT skip source_reader and go
    // straight to `open_image_ro`, or the consumer would see the wrapped
    // bytes instead of the decoded flat-sector stream.
    let is_streaming = source_reader::is_gho_path(path)
        || source_reader::is_imz_path(path)
        || source_reader::is_chd_path(path)
        || source_reader::is_msa_path(path)
        || source_reader::is_apple_ii_dsk_path(path);
    if is_streaming {
        let mut reader = source_reader::open_read_with_password(path, password)?;
        let ctx = resolve(&mut reader, selector)?;
        Ok((reader, ctx))
    } else {
        let mut file = open_image_ro(path)?;
        let ctx = resolve(&mut file, selector)?;
        Ok((Box::new(std::io::BufReader::new(file)), ctx))
    }
}

fn resolve<R: Read + Seek>(reader: &mut R, selector: Option<u32>) -> Result<PartitionContext> {
    let total = reader_size(reader)?;
    let pt =
        PartitionTable::detect(reader).map_err(|e| anyhow!("detecting partition table: {e}"))?;
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
        offset: info.start_lba * 512,
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
