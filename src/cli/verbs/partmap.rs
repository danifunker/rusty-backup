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

use crate::cli::io::open_image_rw;
use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::partition::editor::{apply_edits, validate_edits, PartitionTableEdit};
use crate::partition::PartitionTable;

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
    #[arg(long, default_value_t = 0x83)]
    pub type_byte: u8,
    /// GPT type GUID string, or APM type string (`"Apple_HFS"`, etc.).
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
    /// MBR type byte (decimal or `0xNN`).
    #[arg(long)]
    pub type_byte: Option<u8>,
    /// GPT type GUID / APM type string.
    #[arg(long)]
    pub type_string: Option<String>,
}

#[derive(Debug, Args)]
pub struct SetBootableArgs {
    pub image: PathBuf,
    pub index: u32,
    #[arg(long)]
    pub bootable: bool,
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
    }
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

fn apply_batch(
    image: &std::path::Path,
    edits: Vec<PartitionTableEdit>,
    dry_run: bool,
) -> Result<()> {
    let mut file = open_image_rw(image)?;
    let table = PartitionTable::detect(&mut file)
        .map_err(|e| anyhow::anyhow!("detecting partition table: {e}"))?;
    let disk_size = {
        use std::io::Seek;
        file.seek(std::io::SeekFrom::End(0))?
    };

    let warnings = validate_edits(&table, &edits, disk_size)
        .map_err(|e| anyhow::anyhow!("validate_edits: {e}"))?;
    for w in &warnings {
        log_stderr(format!("warning: {w}"));
    }
    if dry_run {
        log_stderr(format!("dry-run: {} edit(s) would apply", edits.len()));
        return Ok(());
    }
    let mut log_cb = |s: &str| log_stderr(format!("  {s}"));
    apply_edits(&mut file, &table, &edits, disk_size, &mut log_cb)
        .map_err(|e| anyhow::anyhow!("apply_edits: {e}"))?;
    log_stderr(format!(
        "applied {} edit(s) to {}",
        edits.len(),
        image.display()
    ));
    Ok(())
}
