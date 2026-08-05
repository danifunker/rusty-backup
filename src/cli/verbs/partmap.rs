//! `rb-cli partmap <SUBCOMMAND>` — partition-table editor.
//!
//! Mirrors the GUI's "Edit Partition Table" surface using
//! [`crate::partition::editor::apply_edits`]. Each subcommand emits a
//! single [`PartitionTableEdit`]; for batched / atomic changes, write
//! a JSON script and feed it to `partmap apply` (one transaction).
//!
//! Edits operate on the on-disk partition table only — partition
//! *data* is never moved. Resizing a partition past its filesystem
//! contents leaves the filesystem unchanged; pair with `rb-cli resize`
//! to bring the filesystem along.

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::partition::editor::{apply_edits, validate_edits, PartitionTableEdit};
use crate::partition::type_catalog::{self, TableKind};
use crate::partition::PartitionTable;
use crate::rbformats::BoxReadSeek;

#[derive(Debug, Subcommand)]
pub enum PartmapCommand {
    /// Add a new partition entry.
    Add(AddArgs),
    /// Resize an existing partition entry (changes size only — data
    /// is not moved).
    Resize(ResizeArgs),
    /// Move a partition entry to a new start LBA (does not move data).
    Move(MoveArgs),
    /// Delete a partition entry (zeroes the slot).
    Delete(DeleteArgs),
    /// Change a partition's type byte / GUID / APM type string.
    SetType(SetTypeArgs),
    /// Toggle the bootable flag (MBR active-partition bit; RDB flag).
    SetBootable(SetBootableArgs),
    /// Apply a JSON script of edits as one transaction.
    Apply(ApplyArgs),
    /// List the well-known partition type values for a table flavor.
    Types(TypesArgs),
}

#[derive(Debug, Args)]
pub struct AddArgs {
    /// Image to modify.
    pub image: PathBuf,
    /// Start LBA (512-byte sector). MBR / GPT: linear LBA. APM: block #.
    #[arg(long)]
    pub start_lba: u64,
    /// Partition size in bytes (accepts `K`/`M`/`G` suffixes).
    #[arg(long)]
    pub size: String,
    /// MBR type byte (decimal or `0xNN`). Ignored for non-MBR tables.
    /// See `partmap types --table mbr`.
    #[arg(long, default_value_t = 0x83, value_parser = parse_type_byte)]
    pub type_byte: u8,
    /// GPT type GUID string, or APM type string (`"Apple_HFS"`, etc.).
    /// See `partmap types --table gpt|apm`.
    #[arg(long)]
    pub type_string: Option<String>,
    /// Mark active/bootable.
    #[arg(long)]
    pub bootable: bool,
}

#[derive(Debug, Args)]
pub struct ResizeArgs {
    pub image: PathBuf,
    /// 1-based partition index.
    pub index: u32,
    #[arg(long)]
    pub size: String,
}

#[derive(Debug, Args)]
pub struct MoveArgs {
    pub image: PathBuf,
    pub index: u32,
    #[arg(long)]
    pub start_lba: u64,
}

#[derive(Debug, Args)]
pub struct DeleteArgs {
    pub image: PathBuf,
    pub index: u32,
}

#[derive(Debug, Args)]
pub struct SetTypeArgs {
    pub image: PathBuf,
    pub index: u32,
    /// MBR type byte (decimal or `0xNN`). See `partmap types`.
    #[arg(long, value_parser = parse_type_byte)]
    pub type_byte: Option<u8>,
    /// GPT type GUID / APM type string. See `partmap types`.
    #[arg(long)]
    pub type_string: Option<String>,
}

/// Parse an MBR type byte as decimal, or as hex given an explicit `0x`
/// prefix. Clap's stock `u8` parser rejected the `0xNN` form the help text
/// has always advertised.
fn parse_type_byte(s: &str) -> Result<u8, String> {
    let t = s.trim();
    let parsed = match t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        Some(hex) => u8::from_str_radix(hex, 16),
        None => t.parse::<u8>(),
    };
    parsed.map_err(|_| format!("'{}' is not a partition type byte (try 0x83 or 131)", t))
}

#[derive(Debug, Args)]
pub struct SetBootableArgs {
    pub image: PathBuf,
    pub index: u32,
    #[arg(long)]
    pub bootable: bool,
}

#[derive(Debug, Args)]
pub struct TypesArgs {
    /// Table flavor to list types for. Omit to read it from an image.
    #[arg(long, value_parser = ["mbr", "gpt", "apm", "rdb", "sgi"])]
    pub table: Option<String>,
    /// Image whose partition table decides which list to print.
    #[arg(long)]
    pub image: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ApplyArgs {
    pub image: PathBuf,
    /// JSON script with the same schema as `PartitionEditScript` below
    /// (an `edits` array of typed entries).
    pub script: PathBuf,
    /// Validate + print the plan, don't apply.
    #[arg(long = "dry-run")]
    pub dry_run: bool,
}

/// Wire format for batched partmap edits. One edit per element.
#[derive(Debug, Deserialize, Serialize)]
pub struct PartitionEditScript {
    pub edits: Vec<JsonEdit>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum JsonEdit {
    Add {
        start_lba: u64,
        size: String,
        #[serde(default)]
        type_byte: u8,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        type_string: Option<String>,
        #[serde(default)]
        bootable: bool,
    },
    Resize {
        index: u32,
        size: String,
    },
    Move {
        index: u32,
        start_lba: u64,
    },
    Delete {
        index: u32,
    },
    SetType {
        index: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        type_byte: Option<u8>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        type_string: Option<String>,
    },
    SetBootable {
        index: u32,
        bootable: bool,
    },
}

pub fn run(cmd: PartmapCommand) -> Result<()> {
    match cmd {
        PartmapCommand::Add(a) => single_edit(
            &a.image,
            PartitionTableEdit::AddEntry {
                start_lba: a.start_lba,
                size_bytes: parse_size(&a.size)?,
                partition_type: a.type_byte,
                type_string: a.type_string,
                bootable: a.bootable,
            },
        ),
        PartmapCommand::Resize(a) => single_edit(
            &a.image,
            PartitionTableEdit::ResizeEntry {
                index: idx_1based(a.index)?,
                new_size_bytes: parse_size(&a.size)?,
            },
        ),
        PartmapCommand::Move(a) => single_edit(
            &a.image,
            PartitionTableEdit::MoveEntry {
                index: idx_1based(a.index)?,
                new_start_lba: a.start_lba,
            },
        ),
        PartmapCommand::Delete(a) => single_edit(
            &a.image,
            PartitionTableEdit::DeleteEntry {
                index: idx_1based(a.index)?,
            },
        ),
        PartmapCommand::SetType(a) => {
            if a.type_byte.is_none() && a.type_string.is_none() {
                bail!("set-type: pass at least one of --type-byte / --type-string");
            }
            single_edit(
                &a.image,
                PartitionTableEdit::ChangeType {
                    index: idx_1based(a.index)?,
                    new_type_byte: a.type_byte.unwrap_or(0),
                    new_type_string: a.type_string,
                },
            )
        }
        PartmapCommand::SetBootable(a) => single_edit(
            &a.image,
            PartitionTableEdit::SetBootable {
                index: idx_1based(a.index)?,
                bootable: a.bootable,
            },
        ),
        PartmapCommand::Apply(a) => run_apply(a),
        PartmapCommand::Types(a) => run_types(a),
    }
}

fn run_types(a: TypesArgs) -> Result<()> {
    let kind = match (a.table.as_deref(), a.image.as_ref()) {
        (Some(t), _) => match t {
            "mbr" => TableKind::Mbr,
            "gpt" => TableKind::Gpt,
            "apm" => TableKind::Apm,
            "rdb" => TableKind::Rdb,
            "sgi" => TableKind::Sgi,
            other => bail!("unknown table flavor '{}'", other),
        },
        (None, Some(image)) => type_catalog::kind_of(&open_table(image)?),
        (None, None) => bail!("types: pass --table <mbr|gpt|apm|rdb|sgi> or --image <FILE>"),
    };

    let choices = type_catalog::choices(kind);
    if choices.is_empty() {
        bail!("no partition type catalog for {} tables", kind.label());
    }
    // MBR values print `0x`-prefixed: the catalog stores the bare hex the GUI
    // type field wants, but `--type-byte` reads an unprefixed number as
    // decimal, so copying a bare `83` off this list would silently mean 0x53.
    let (flag, values): (&str, Vec<String>) = if kind == TableKind::Mbr {
        (
            "--type-byte",
            choices.iter().map(|c| format!("0x{}", c.value)).collect(),
        )
    } else {
        (
            "--type-string",
            choices.iter().map(|c| c.value.to_string()).collect(),
        )
    };

    println!("{} partition types ({}):", kind.label(), kind.field_hint());
    let width = values.iter().map(|v| v.len()).max().unwrap_or(0);
    for (value, choice) in values.iter().zip(choices) {
        println!("  {:<width$}  {}", value, choice.label, width = width);
    }
    println!("\nPass one to `partmap add {} <VALUE>`.", flag);
    println!("Any other value is accepted verbatim -- the list is not exhaustive.");
    Ok(())
}

fn idx_1based(idx: u32) -> Result<usize> {
    if idx == 0 {
        bail!("partition index is 1-based");
    }
    Ok((idx - 1) as usize)
}

fn single_edit(image: &std::path::Path, edit: PartitionTableEdit) -> Result<()> {
    apply_batch(image, vec![edit], false)
}

fn run_apply(a: ApplyArgs) -> Result<()> {
    let text = std::fs::read_to_string(&a.script)
        .with_context(|| format!("reading {}", a.script.display()))?;
    let script: PartitionEditScript =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", a.script.display()))?;
    let edits = script
        .edits
        .into_iter()
        .map(json_edit_to_edit)
        .collect::<Result<Vec<_>>>()?;
    apply_batch(&a.image, edits, a.dry_run)
}

fn json_edit_to_edit(j: JsonEdit) -> Result<PartitionTableEdit> {
    Ok(match j {
        JsonEdit::Add {
            start_lba,
            size,
            type_byte,
            type_string,
            bootable,
        } => PartitionTableEdit::AddEntry {
            start_lba,
            size_bytes: parse_size(&size)?,
            partition_type: type_byte,
            type_string,
            bootable,
        },
        JsonEdit::Resize { index, size } => PartitionTableEdit::ResizeEntry {
            index: idx_1based(index)?,
            new_size_bytes: parse_size(&size)?,
        },
        JsonEdit::Move { index, start_lba } => PartitionTableEdit::MoveEntry {
            index: idx_1based(index)?,
            new_start_lba: start_lba,
        },
        JsonEdit::Delete { index } => PartitionTableEdit::DeleteEntry {
            index: idx_1based(index)?,
        },
        JsonEdit::SetType {
            index,
            type_byte,
            type_string,
        } => PartitionTableEdit::ChangeType {
            index: idx_1based(index)?,
            new_type_byte: type_byte.unwrap_or(0),
            new_type_string: type_string,
        },
        JsonEdit::SetBootable { index, bootable } => PartitionTableEdit::SetBootable {
            index: idx_1based(index)?,
            bootable,
        },
    })
}

/// Read-only whole-disk probe, peeling a CHD / container as needed.
///
/// `partmap` edits the disk's table, so it opens the *disk*, not a partition
/// within it — going through `resolve_partition_streaming` made every verb
/// here refuse a multi-partition image with "select one by appending `@N`",
/// which is both wrong and the exact shape of disk these verbs are for.
fn open_disk(image: &std::path::Path) -> Result<BoxReadSeek> {
    crate::model::source_reader::open_peeled_read_with_entry(image, None, None)
}

fn open_table(image: &std::path::Path) -> Result<PartitionTable> {
    let mut probe = open_disk(image)?;
    PartitionTable::detect(&mut probe)
        .map_err(|e| anyhow::anyhow!("detecting partition table: {e}"))
}

/// Is `image` a raw device node rather than a file on disk?
fn is_device_path(image: &std::path::Path) -> bool {
    let s = image.to_string_lossy();
    s.starts_with("/dev/") || s.starts_with(r"\\.\")
}

/// Total addressable size of the disk behind `image`.
///
/// A device node answers `seek(End)` with 0 on macOS (and Windows physical
/// drives aren't seekable either), so asking the stream would hand validation a
/// zero-sector disk and make every edit look like it runs past the end. Ask the
/// OS for a device; for an image *file* keep using the stream, whose length is
/// the decoded size of a CHD / container rather than the file's byte count.
fn disk_size_of(image: &std::path::Path, probe: &mut BoxReadSeek) -> Result<u64> {
    use std::io::Seek;
    if is_device_path(image) {
        let file = std::fs::File::open(image)
            .with_context(|| format!("opening {} to measure it", image.display()))?;
        return crate::os::get_file_size(&file, image)
            .with_context(|| format!("cannot determine the size of {}", image.display()));
    }
    Ok(probe.seek(std::io::SeekFrom::End(0))?)
}

fn apply_batch(
    image: &std::path::Path,
    edits: Vec<PartitionTableEdit>,
    dry_run: bool,
) -> Result<()> {
    use std::io::Write;

    // `partmap` rewrites the whole disk's table and takes a plain path, so an
    // `@N` carried over from `ls` / `get` ends up inside the filename and the
    // open fails with a bare ENOENT. Name the real problem: here the partition
    // is chosen by the positional index argument, not by a path suffix.
    if let Some((real, n)) = crate::cli::img_at::stray_selector(image) {
        anyhow::bail!(
            "`partmap` edits the whole partition table, so `@{n}` was taken as \
             part of the filename. Drop it and pass the partition as the index \
             argument instead, e.g. `partmap resize {} {n} <SIZE>`.",
            real.display()
        );
    }

    // Probe the table + disk size read-only first (peels a CHD / container),
    // so `--dry-run` never opens a write handle — which, for a compressed CHD,
    // would make a backup copy + diff for nothing.
    let mut probe = open_disk(image)?;
    let table = PartitionTable::detect(&mut probe)
        .map_err(|e| anyhow::anyhow!("detecting partition table: {e}"))?;
    let disk_size = disk_size_of(image, &mut probe)?;
    drop(probe);

    let warnings = validate_edits(&table, &edits, disk_size)
        .map_err(|e| anyhow::anyhow!("validate_edits: {e}"))?;
    for w in &warnings {
        log_stderr(format!("warning: {w}"));
    }
    if dry_run {
        log_stderr(format!("dry-run: {} edit(s) would apply", edits.len()));
        return Ok(());
    }

    // Apply for real on a read-write handle (decoding a CHD / container as
    // needed; commit flattens / re-encodes on success).
    let (mut file, commit, _shape) = crate::cli::resolve::resolve_image_rw(image)?;
    let mut log_cb = |s: &str| log_stderr(format!("  {s}"));
    apply_edits(&mut file, &table, &edits, disk_size, &mut log_cb)
        .map_err(|e| anyhow::anyhow!("apply_edits: {e}"))?;
    file.flush().ok();
    drop(file);
    commit.commit()?;
    log_stderr(format!(
        "applied {} edit(s) to {}",
        edits.len(),
        image.display()
    ));
    Ok(())
}
