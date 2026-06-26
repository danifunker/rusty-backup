//! `rb-cli batch SCRIPT.json` — apply a JSON-described sequence of
//! filesystem operations to an image as one transaction-like batch.
//!
//! Matches the GUI's staged-edits semantics: collect every op,
//! preflight, then apply all of them with a single `sync_metadata` at
//! the end.
//!
//! Phase D scope: single-target scripts (one image per file), all the
//! per-file verbs (`mkdir`, `put`, `rm`, `bless`, set-type/creator),
//! single-fs / single-partition only. The whole-volume `format` op +
//! multi-partition `disk` block (see docs/cli-todo.md § "Whole-volume
//! creation via `format` op") layers on once the FAT/EFS/AFFS blank
//! creators land. The pre-flight is intentionally simple — it walks
//! the op list and validates paths against the target's current
//! state. Free-space projection is approximate (sum of host file
//! sizes) and lands per-FS as the metadata surface matures.

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::parse::split_mac_path;
use crate::cli::resolve::resolve_partition_rw;
use crate::fs::filesystem::{CreateDirectoryOptions, CreateFileOptions};

#[derive(Debug, Args)]
pub struct BatchArgs {
    /// Path to the batch JSON script.
    pub script: PathBuf,

    /// Override the script's `target` field (`path` or `path@N`).
    #[arg(long)]
    pub target: Option<ImageRef>,

    /// Validate + print the plan, don't apply.
    #[arg(long = "dry-run")]
    pub dry_run: bool,

    /// Continue with remaining ops after a non-fatal failure. Default
    /// is stop-on-first-error.
    #[arg(long = "continue-on-error")]
    pub continue_on_error: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Script {
    #[serde(default = "default_schema")]
    pub schema: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_options: Option<DefaultOptions>,
    /// Optional multi-partition disk layout. When present, the target
    /// file is created with the named partition table and one partition
    /// per entry; subsequent `operations` target partition 1 unless
    /// they carry their own `@N` selector in the future.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk: Option<DiskBlock>,
    pub operations: Vec<Operation>,
}

/// Multi-partition layout for a freshly-created image.
#[derive(Debug, Deserialize, Serialize)]
pub struct DiskBlock {
    /// Partition table kind: `mbr` (only kind supported today).
    pub table: String,
    pub partitions: Vec<DiskPartition>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DiskPartition {
    pub fs: String,
    pub size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_size: Option<u32>,
    /// AFFS variant byte (only meaningful for `fs = "affs"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affs_variant: Option<u8>,
    /// Whether to mark this partition active/bootable (MBR only).
    #[serde(default)]
    pub bootable: bool,
}

fn default_schema() -> String {
    "rb-cli-batch/1".to_string()
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DefaultOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<String>,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_code: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Operation {
    Mkdir(MkdirOp),
    Put(PutOp),
    Rm(RmOp),
    /// Create a fresh single-partition image at the script's `target`
    /// path. Must be the first operation if present. Mutually exclusive
    /// with a top-level `disk` block.
    Format(FormatOp),
    /// Mark an HFS / HFS+ folder as the bootable System Folder.
    Bless(BlessOp),
    /// Change the type/creator codes on an existing file.
    Chmeta(ChmetaOp),
    /// Replace the resource fork of an existing file with host bytes.
    Setrsrc(SetrsrcOp),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BlessOp {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ChmetaOp {
    pub path: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SetrsrcOp {
    pub path: String,
    pub from: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FormatOp {
    pub fs: String,
    pub size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_size: Option<u32>,
    /// AFFS variant byte (only meaningful for `fs = "affs"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affs_variant: Option<u8>,
    /// Optional partition-table wrapper: `none` (default, raw FS) or
    /// `mbr` (single-partition MBR wrap at LBA 2048; FAT only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_table: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MkdirOp {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PutOp {
    pub src: PathBuf,
    pub dst: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<String>,
    #[serde(default)]
    pub force: bool,
    /// Allocate a zero-filled file of this size instead of copying
    /// `src`. When set, `src` is ignored (use `"src": "/dev/null"` to
    /// keep the JSON schema happy).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zero: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RmOp {
    pub path: String,
    #[serde(default)]
    pub recursive: bool,
}

pub fn run(args: BatchArgs) -> Result<()> {
    let text = std::fs::read_to_string(&args.script)
        .with_context(|| format!("reading {}", args.script.display()))?;
    let script: Script = serde_json::from_str(&text)
        .with_context(|| format!("parsing {}", args.script.display()))?;

    if !script.schema.starts_with("rb-cli-batch/") {
        bail!(
            "unknown batch schema {:?}; expected rb-cli-batch/<n>",
            script.schema
        );
    }

    let target = match args.target {
        Some(t) => t,
        None => script
            .target
            .parse::<ImageRef>()
            .with_context(|| format!("parsing target {:?}", script.target))?,
    };
    let defaults = script.default_options.unwrap_or_default();

    // Validation: `format` op (if present) must be at index 0, and
    // can't coexist with a top-level `disk` block.
    let has_format = matches!(script.operations.first(), Some(Operation::Format(_)));
    if let Some(idx) = script
        .operations
        .iter()
        .skip(1)
        .position(|o| matches!(o, Operation::Format(_)))
    {
        bail!(
            "`format` op must be the first operation (found at index {})",
            idx + 1
        );
    }
    if has_format && script.disk.is_some() {
        bail!("script cannot carry both a `disk` block and a `format` op");
    }

    if args.dry_run {
        out_stdout(format!(
            "dry-run plan: {} operation(s) on {}",
            script.operations.len(),
            target.path.display()
        ));
        if let Some(d) = &script.disk {
            out_stdout(format!(
                "  disk: {} table, {} partition(s)",
                d.table,
                d.partitions.len()
            ));
        }
        for (i, op) in script.operations.iter().enumerate() {
            out_stdout(format!("  {:>3}  {}", i + 1, describe(op)));
        }
        return Ok(());
    }

    // Apply the disk block (creates a fresh multi-partition image at target.path).
    if let Some(d) = &script.disk {
        apply_disk_block(&target.path, d)?;
        log_stderr(format!(
            "disk: created {} ({} partition(s))",
            target.path.display(),
            d.partitions.len()
        ));
    }

    // Apply a leading `format` op (creates a single-partition image).
    let fs_ops_start = if has_format {
        if let Some(Operation::Format(fop)) = script.operations.first() {
            apply_format_op(&target.path, fop)?;
            log_stderr(format!(
                "format: created {} (fs={}, size={})",
                target.path.display(),
                fop.fs,
                fop.size
            ));
        }
        1
    } else {
        0
    };

    // If the script only contained a `disk` / `format` op and no FS ops, we're done.
    if script.operations.len() == fs_ops_start && script.disk.is_none() && !has_format {
        out_stdout("Applied 0/0 op(s); 0 failure(s)");
        return Ok(());
    }
    if script.operations.len() == fs_ops_start {
        out_stdout(format!(
            "Applied {}/{} op(s); 0 failure(s)",
            fs_ops_start,
            script.operations.len()
        ));
        return Ok(());
    }

    // Archive-edit path: when target is a backup folder, decompress
    // partition N to a temp file, run ops on the temp, then recompress
    // + update metadata.json checksum. Detected by the presence of a
    // sibling metadata.json.
    let archive_ctx = detect_archive_target(&target.path, target.partition)?;

    let (applied, failures) = if let Some(arch) = archive_ctx {
        log_stderr(format!(
            "archive-edit: decompressing partition {} from {}",
            arch.partition_index,
            arch.folder.display()
        ));
        let temp_path = arch
            .folder
            .join(format!(".batch-partition-{}.tmp", arch.partition_index));
        let progress = crate::model::archive_edit::start_extract(&arch.edit_ctx, temp_path.clone());
        drain_archive(progress)?;

        let (applied, failures) = run_fs_ops_on_path(
            &temp_path,
            target.partition,
            &script.operations,
            fs_ops_start,
            &defaults,
            args.continue_on_error,
        )?;

        log_stderr("archive-edit: recompressing");
        let cprog = crate::model::archive_edit::start_compress(&arch.edit_ctx, temp_path.clone());
        drain_archive(cprog)?;
        (applied, failures)
    } else {
        run_fs_ops_on_path(
            &target.path,
            target.partition,
            &script.operations,
            fs_ops_start,
            &defaults,
            args.continue_on_error,
        )?
    };

    out_stdout(format!(
        "Applied {applied}/{} op(s); {} failure(s)",
        script.operations.len(),
        failures.len()
    ));
    if !failures.is_empty() {
        for (i, m) in &failures {
            out_stdout(format!("  failure at op {i}: {m}"));
        }
        bail!("batch completed with failures");
    }
    Ok(())
}

struct ArchiveTarget {
    folder: std::path::PathBuf,
    partition_index: usize,
    edit_ctx: crate::model::archive_edit::ArchiveEditContext,
}

/// Inspect `target_path`. If it's a regular folder containing
/// `metadata.json`, treat it as a backup-archive target and build the
/// [`ArchiveEditContext`] for partition `partition` (1-based). Returns
/// `None` for plain image files.
fn detect_archive_target(
    target_path: &std::path::Path,
    partition: Option<u32>,
) -> Result<Option<ArchiveTarget>> {
    if !target_path.is_dir() {
        return Ok(None);
    }
    let metadata_path = target_path.join("metadata.json");
    if !metadata_path.exists() {
        return Ok(None);
    }
    let part = partition.ok_or_else(|| {
        anyhow!(
            "{} is a backup folder; pass IMG@N to pick a partition",
            target_path.display()
        )
    })? as usize;
    let outcome = crate::model::backup_loader::load_backup_metadata(target_path)?;
    let meta = outcome.metadata;
    let pmeta = meta
        .partitions
        .iter()
        .find(|p| p.index == part)
        .or_else(|| meta.partitions.get(part.saturating_sub(1)))
        .ok_or_else(|| anyhow!("partition {part} not found in backup metadata"))?;
    if pmeta.compressed_files.is_empty() {
        bail!("partition {part} has no compressed_files entry in metadata.json");
    }
    let archive_path = target_path.join(&pmeta.compressed_files[0]);
    if !archive_path.exists() {
        bail!("archive {} missing", archive_path.display());
    }
    if !matches!(meta.compression_type.as_str(), "zstd" | "none") {
        bail!(
            "archive-edit currently supports zstd / raw partition archives; got {}",
            meta.compression_type
        );
    }
    let edit_ctx = crate::model::archive_edit::ArchiveEditContext {
        archive_path,
        compression_type: meta.compression_type.clone(),
        original_size: pmeta.original_size_bytes,
        compacted: pmeta.compacted,
        metadata_path,
        // metadata.json keeps partitions keyed by their `index` field
        // (often 0-based), not by user-facing 1-based selector position.
        partition_index: pmeta.index,
        checksum_type: meta.checksum_type.clone(),
    };
    Ok(Some(ArchiveTarget {
        folder: target_path.to_path_buf(),
        partition_index: part,
        edit_ctx,
    }))
}

fn drain_archive(
    progress: std::sync::Arc<std::sync::Mutex<crate::model::archive_edit::ArchiveEditProgress>>,
) -> Result<()> {
    use std::time::Duration;
    let mut last_pct: i32 = -1;
    loop {
        std::thread::sleep(Duration::from_millis(250));
        let (phase, cur, total, finished, error) = match progress.lock() {
            Ok(p) => (
                p.phase.clone(),
                p.current,
                p.total,
                p.finished,
                p.error.clone(),
            ),
            Err(_) => bail!("archive_edit worker poisoned its status mutex"),
        };
        if total > 0 {
            let pct = ((cur as f64 / total as f64) * 100.0) as i32;
            if pct / 10 != last_pct / 10 {
                log_stderr(format!(
                    "  {phase}: {pct:>3}% ({}/{})",
                    crate::partition::format_size(cur),
                    crate::partition::format_size(total)
                ));
                last_pct = pct;
            }
        }
        if finished {
            if let Some(e) = error {
                bail!("{phase}: {e}");
            }
            return Ok(());
        }
    }
}

fn run_fs_ops_on_path(
    path: &std::path::Path,
    partition: Option<u32>,
    operations: &[Operation],
    fs_ops_start: usize,
    defaults: &DefaultOptions,
    continue_on_error: bool,
) -> Result<(usize, Vec<(usize, String)>)> {
    let (file, ctx, commit) = resolve_partition_rw(path, partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let mut applied = fs_ops_start;
    let mut failures: Vec<(usize, String)> = Vec::new();
    // Set when an op fails without --continue-on-error: we still sync + persist
    // the ops that succeeded so far (matching the raw-image behavior where the
    // synced writes land), then report the abort after committing.
    let mut abort_msg: Option<String> = None;
    for (i, op) in operations.iter().enumerate().skip(fs_ops_start) {
        let res = apply_op(&mut *fs, op, defaults);
        match res {
            Ok(()) => {
                applied += 1;
                log_stderr(format!(
                    "  + [{}/{}] {}",
                    i + 1,
                    operations.len(),
                    describe(op)
                ));
            }
            Err(e) => {
                let msg = format!("[{}/{}] {}: {e}", i + 1, operations.len(), describe(op));
                log_stderr(format!("  ! {msg}"));
                failures.push((i + 1, msg));
                if !continue_on_error {
                    abort_msg = Some(format!(
                        "batch aborted after {applied} successful op(s); rerun with --continue-on-error to keep going"
                    ));
                    break;
                }
            }
        }
    }
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;
    // Persist: re-encode the temp flat back into the container (no-op for raw
    // images). Done before reporting an abort so partial work is saved.
    drop(fs);
    commit.commit()?;
    if let Some(msg) = abort_msg {
        bail!("{msg}");
    }
    Ok((applied, failures))
}

fn describe(op: &Operation) -> String {
    match op {
        Operation::Mkdir(MkdirOp { path }) => format!("mkdir {path}"),
        Operation::Put(PutOp { src, dst, zero, .. }) => match zero {
            Some(n) => format!("put --zero {n} -> {dst}"),
            None => format!("put {} -> {dst}", src.display()),
        },
        Operation::Rm(RmOp { path, recursive }) => {
            if *recursive {
                format!("rm -r {path}")
            } else {
                format!("rm {path}")
            }
        }
        Operation::Format(FormatOp { fs, size, name, .. }) => {
            format!(
                "format --fs {fs} --size {size}{}",
                match name {
                    Some(n) => format!(" --name {n:?}"),
                    None => String::new(),
                }
            )
        }
        Operation::Bless(BlessOp { path }) => format!("bless {path}"),
        Operation::Chmeta(ChmetaOp {
            path,
            type_code,
            creator,
        }) => {
            let mut bits = Vec::new();
            if let Some(t) = type_code {
                bits.push(format!("--type {t}"));
            }
            if let Some(c) = creator {
                bits.push(format!("--creator {c}"));
            }
            format!("chmeta {path} {}", bits.join(" "))
        }
        Operation::Setrsrc(SetrsrcOp { path, from }) => {
            format!("setrsrc {path} --from {}", from.display())
        }
    }
}

/// Materialize a single-partition image at `path` based on a leading
/// `format` op. Currently writes a raw filesystem at the start of the
/// file; `partition_table = "mbr"` wraps a FAT volume in a 1-MiB-
/// aligned MBR partition. Other PT wrappers are deferred.
fn apply_format_op(path: &std::path::Path, op: &FormatOp) -> Result<()> {
    let size = crate::cli::parse::parse_size(&op.size)
        .with_context(|| format!("parsing format.size {:?}", op.size))?;
    let name = op.name.as_deref().unwrap_or("rusty-backup");
    let pt_kind = op
        .partition_table
        .as_deref()
        .unwrap_or("none")
        .to_lowercase();
    if pt_kind != "none" && pt_kind != "mbr" {
        bail!("unsupported partition_table {pt_kind:?}; only 'none' or 'mbr' for now");
    }
    let fs_bytes = build_blank_fs(&op.fs, size, name, op.block_size, op.affs_variant)?;
    let final_bytes = if pt_kind == "mbr" {
        if op.fs != "fat" {
            bail!("partition_table = 'mbr' currently requires fs = 'fat'");
        }
        wrap_in_single_partition_mbr(&fs_bytes, fat_type_byte_for(size))?
    } else {
        fs_bytes
    };
    std::fs::write(path, &final_bytes).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Dispatch to the right per-FS `create_blank_*` based on the JSON `fs` string.
fn build_blank_fs(
    fs: &str,
    size: u64,
    name: &str,
    block_size: Option<u32>,
    affs_variant: Option<u8>,
) -> Result<Vec<u8>> {
    match fs {
        "fat" => crate::fs::fat::create_blank_fat(size, Some(name)),
        "efs" => crate::fs::efs::create_blank_efs(size, name),
        "affs" => crate::fs::affs::create_blank_affs(size, affs_variant.unwrap_or(1), name),
        "hfs" => {
            let bs = block_size.unwrap_or_else(|| crate::cli::parse::pick_block_size(size));
            crate::fs::hfs::create_blank_hfs_sized(size, bs, name, 0, 0)
                .map_err(|e| anyhow!("create_blank_hfs_sized: {e}"))
        }
        other => bail!("unsupported fs {other:?}; expected one of fat, efs, affs, hfs"),
    }
}

/// Best-guess FAT type byte from volume size (mirrors `create_blank_fat`).
fn fat_type_byte_for(size_bytes: u64) -> u8 {
    if size_bytes <= 32 * 1024 * 1024 {
        0x01 // FAT12
    } else if size_bytes <= 2u64 * 1024 * 1024 * 1024 {
        0x06 // FAT16 BIG (CHS-LBA mix; the parser accepts all FAT16 variants)
    } else {
        0x0C // FAT32 LBA
    }
}

/// Wrap a single FAT filesystem image in a 1-MiB-aligned MBR.
/// Returns a new buffer with [MBR + zero-padding + fs_bytes] sized to a
/// 512-byte boundary. The FS's BPB `hidden_sectors` field is patched
/// to the partition start LBA so DOS-era tools see consistent geometry.
fn wrap_in_single_partition_mbr(fs_bytes: &[u8], type_byte: u8) -> Result<Vec<u8>> {
    const ALIGN_SECTORS: u32 = 2048; // 1 MiB at 512-byte sectors
    let fs_sectors = fs_bytes.len().div_ceil(512);
    if fs_sectors as u64 > u32::MAX as u64 - ALIGN_SECTORS as u64 {
        bail!("filesystem too large for an MBR wrapper");
    }
    let total_sectors = ALIGN_SECTORS + fs_sectors as u32;
    let mut out = vec![0u8; total_sectors as usize * 512];

    // Patch a copy of the FS bytes so its hidden_sectors field reflects the LBA offset.
    let mut fs = fs_bytes.to_vec();
    if fs.len() >= 32 && (fs[0] == 0xEB || fs[0] == 0xE9) {
        fs[28..32].copy_from_slice(&ALIGN_SECTORS.to_le_bytes());
    }
    out[ALIGN_SECTORS as usize * 512..ALIGN_SECTORS as usize * 512 + fs.len()].copy_from_slice(&fs);

    let mbr = crate::partition::mbr::build_minimal_mbr(
        0xDEADBEEF,
        &[(type_byte, ALIGN_SECTORS, fs_sectors as u32, true)],
        16,
        63,
    );
    out[..512].copy_from_slice(&mbr);
    Ok(out)
}

/// Materialize a multi-partition image from a `disk` block.
/// Currently supports `table: "mbr"` only.
fn apply_disk_block(path: &std::path::Path, disk: &DiskBlock) -> Result<()> {
    let table = disk.table.to_lowercase();
    if table != "mbr" {
        bail!(
            "unsupported disk.table {:?}; only 'mbr' for now",
            disk.table
        );
    }
    if disk.partitions.is_empty() {
        bail!("disk.partitions is empty");
    }
    if disk.partitions.len() > 4 {
        bail!(
            "MBR supports up to 4 primary partitions; got {}",
            disk.partitions.len()
        );
    }

    const ALIGN_SECTORS: u32 = 2048;
    let mut entries: Vec<(u8, u32, u32, bool)> = Vec::new();
    let mut sections: Vec<(u32, Vec<u8>)> = Vec::new();
    let mut cursor_lba: u32 = ALIGN_SECTORS;
    for p in &disk.partitions {
        let size = crate::cli::parse::parse_size(&p.size)
            .with_context(|| format!("parsing disk.partitions[].size {:?}", p.size))?;
        let name = p.name.as_deref().unwrap_or("rusty-backup");
        let fs_bytes = build_blank_fs(&p.fs, size, name, p.block_size, p.affs_variant)?;
        let fs_sectors_u64 = (fs_bytes.len() as u64).div_ceil(512);
        if fs_sectors_u64 > u32::MAX as u64 {
            bail!("partition too large for 32-bit LBA");
        }
        let fs_sectors = fs_sectors_u64 as u32;
        let mut patched = fs_bytes;
        if p.fs == "fat" && patched.len() >= 32 && (patched[0] == 0xEB || patched[0] == 0xE9) {
            patched[28..32].copy_from_slice(&cursor_lba.to_le_bytes());
        }
        let type_byte = match p.fs.as_str() {
            "fat" => fat_type_byte_for(size),
            "efs" => 0x83, // Linux native; close enough — IRIX uses its own SGI table normally
            "affs" => 0x83,
            "hfs" => 0xAF, // Apple HFS
            other => bail!("unsupported fs in disk.partitions: {other:?}"),
        };
        entries.push((type_byte, cursor_lba, fs_sectors, p.bootable));
        sections.push((cursor_lba, patched));
        cursor_lba = cursor_lba
            .checked_add(fs_sectors)
            .ok_or_else(|| anyhow!("partition layout overflowed u32 LBA"))?;
        // 1-MiB alignment between partitions.
        cursor_lba = cursor_lba.div_ceil(ALIGN_SECTORS) * ALIGN_SECTORS;
    }

    let total_sectors = cursor_lba;
    let mut out = vec![0u8; total_sectors as usize * 512];
    let mbr = crate::partition::mbr::build_minimal_mbr(0xDEADBEEF, &entries, 16, 63);
    out[..512].copy_from_slice(&mbr);
    for (lba, bytes) in sections {
        let off = lba as usize * 512;
        out[off..off + bytes.len()].copy_from_slice(&bytes);
    }
    std::fs::write(path, &out).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

fn apply_op(
    fs: &mut dyn crate::fs::filesystem::EditableFilesystem,
    op: &Operation,
    defaults: &DefaultOptions,
) -> Result<()> {
    match op {
        Operation::Mkdir(MkdirOp { path }) => apply_mkdir(fs, path),
        Operation::Put(op) => apply_put(fs, op, defaults),
        Operation::Rm(RmOp { path, recursive }) => apply_rm(fs, path, *recursive),
        // Format ops are applied before the FS is opened (see `fs_ops_start`
        // in `run`). Reaching here means the validation slipped — treat as
        // a bug.
        Operation::Format(_) => bail!("`format` op must be the first operation"),
        Operation::Bless(BlessOp { path }) => apply_bless(fs, path),
        Operation::Chmeta(op) => apply_chmeta(fs, op),
        Operation::Setrsrc(op) => apply_setrsrc(fs, op),
    }
}

fn apply_bless(fs: &mut dyn crate::fs::filesystem::EditableFilesystem, path: &str) -> Result<()> {
    let entry = super::ls::resolve_path(&mut *fs, path)?;
    if !entry.is_directory() {
        bail!("bless: {path} is not a directory");
    }
    fs.set_blessed_folder(&entry)
        .map_err(|e| anyhow!("set_blessed_folder: {e}"))?;
    Ok(())
}

fn apply_chmeta(
    fs: &mut dyn crate::fs::filesystem::EditableFilesystem,
    op: &ChmetaOp,
) -> Result<()> {
    if op.type_code.is_none() && op.creator.is_none() {
        bail!("chmeta: pass at least one of type / creator");
    }
    let entry = super::ls::resolve_path(&mut *fs, &op.path)?;
    // Default the un-overridden half to the file's current code (display form;
    // `set_type_creator` is text-based).
    let entry_type = entry.type_code_display();
    let entry_creator = entry.creator_code_display();
    let new_type = op
        .type_code
        .as_deref()
        .or(entry_type.as_deref())
        .unwrap_or("BINA");
    let new_creator = op
        .creator
        .as_deref()
        .or(entry_creator.as_deref())
        .unwrap_or("????");
    fs.set_type_creator(&entry, new_type, new_creator)
        .map_err(|e| anyhow!("set_type_creator: {e}"))?;
    Ok(())
}

fn apply_setrsrc(
    fs: &mut dyn crate::fs::filesystem::EditableFilesystem,
    op: &SetrsrcOp,
) -> Result<()> {
    let entry = super::ls::resolve_path(&mut *fs, &op.path)?;
    let meta =
        std::fs::metadata(&op.from).map_err(|e| anyhow!("stat {}: {e}", op.from.display()))?;
    let len = meta.len();
    let mut hf =
        std::fs::File::open(&op.from).map_err(|e| anyhow!("open {}: {e}", op.from.display()))?;
    fs.write_resource_fork(&entry, &mut hf, len)
        .map_err(|e| anyhow!("write_resource_fork: {e}"))?;
    Ok(())
}

fn apply_mkdir(fs: &mut dyn crate::fs::filesystem::EditableFilesystem, path: &str) -> Result<()> {
    let (parent_path, name) = split_mac_path(path)?;
    if name.is_empty() {
        bail!("mkdir: empty basename for {path:?}");
    }
    let parent = super::ls::resolve_path(&mut *fs, &parent_path)?;
    fs.create_directory(&parent, &name, &CreateDirectoryOptions::default())
        .map_err(|e| anyhow!("{e}"))?;
    Ok(())
}

fn apply_put(
    fs: &mut dyn crate::fs::filesystem::EditableFilesystem,
    op: &PutOp,
    defaults: &DefaultOptions,
) -> Result<()> {
    let (parent_path, name) = split_mac_path(&op.dst)?;
    if name.is_empty() {
        bail!("put: empty basename for {:?}", op.dst);
    }
    let parent = super::ls::resolve_path(&mut *fs, &parent_path)?;
    let force = op.force || defaults.force.unwrap_or(false);

    let existing = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?
        .into_iter()
        .find(|e| e.name == name);
    if let Some(ref e) = existing {
        if !force {
            bail!("{} already exists (set \"force\": true)", op.dst);
        }
        fs.delete_entry(&parent, e)
            .map_err(|e| anyhow!("delete existing: {e}"))?;
    }

    let type_code = op
        .type_code
        .clone()
        .or_else(|| defaults.type_code.clone())
        .unwrap_or_else(|| "BINA".to_string());
    let creator = op
        .creator
        .clone()
        .or_else(|| defaults.creator.clone())
        .unwrap_or_else(|| "????".to_string());
    let options = CreateFileOptions {
        type_code: Some(type_code),
        creator_code: Some(creator),
        ..Default::default()
    };

    if let Some(n) = op.zero {
        let mut zr = crate::cli::parse::ZeroReader { remaining: n };
        fs.create_file(&parent, &name, &mut zr, n, &options)
            .map_err(|e| anyhow!("create_file: {e}"))?;
    } else {
        let meta =
            std::fs::metadata(&op.src).map_err(|e| anyhow!("stat {}: {e}", op.src.display()))?;
        let len = meta.len();
        let mut hf =
            std::fs::File::open(&op.src).map_err(|e| anyhow!("open {}: {e}", op.src.display()))?;
        fs.create_file(&parent, &name, &mut hf, len, &options)
            .map_err(|e| anyhow!("create_file: {e}"))?;
    }
    Ok(())
}

fn apply_rm(
    fs: &mut dyn crate::fs::filesystem::EditableFilesystem,
    path: &str,
    recursive: bool,
) -> Result<()> {
    let (parent_path, name) = split_mac_path(path)?;
    let parent = super::ls::resolve_path(&mut *fs, &parent_path)?;
    let entry = fs
        .list_directory(&parent)
        .map_err(|e| anyhow!("list_directory: {e}"))?
        .into_iter()
        .find(|c| c.name == name)
        .ok_or_else(|| anyhow!("not found: {path}"))?;
    if entry.is_directory() {
        if !recursive {
            bail!("{path} is a directory; set \"recursive\": true to delete");
        }
        fs.delete_recursive(&parent, &entry)
            .map_err(|e| anyhow!("delete_recursive: {e}"))?;
    } else {
        fs.delete_entry(&parent, &entry)
            .map_err(|e| anyhow!("delete_entry: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_script() {
        let json = r#"{
            "schema": "rb-cli-batch/1",
            "target": "disk.hda@1",
            "operations": [
                { "op": "mkdir", "path": "/Apps" },
                { "op": "put", "src": "./foo.bin", "dst": "/Apps/foo.bin" }
            ]
        }"#;
        let s: Script = serde_json::from_str(json).unwrap();
        assert_eq!(s.operations.len(), 2);
        assert!(matches!(s.operations[0], Operation::Mkdir(_)));
    }

    #[test]
    fn parses_format_op() {
        let json = r#"{
            "schema": "rb-cli-batch/1",
            "target": "out.img",
            "operations": [
                { "op": "format", "fs": "fat", "size": "4M", "name": "X" },
                { "op": "mkdir", "path": "/X" }
            ]
        }"#;
        let s: Script = serde_json::from_str(json).unwrap();
        match &s.operations[0] {
            Operation::Format(f) => {
                assert_eq!(f.fs, "fat");
                assert_eq!(f.size, "4M");
            }
            _ => panic!("expected format"),
        }
    }

    #[test]
    fn parses_disk_block() {
        let json = r#"{
            "schema": "rb-cli-batch/1",
            "target": "out.img@1",
            "disk": {
                "table": "mbr",
                "partitions": [
                    { "fs": "fat", "size": "4M", "name": "A", "bootable": true },
                    { "fs": "fat", "size": "8M", "name": "B" }
                ]
            },
            "operations": []
        }"#;
        let s: Script = serde_json::from_str(json).unwrap();
        let d = s.disk.expect("disk block");
        assert_eq!(d.partitions.len(), 2);
        assert!(d.partitions[0].bootable);
    }

    #[test]
    fn put_zero_form() {
        let json = r#"{
            "schema": "rb-cli-batch/1",
            "target": "disk.hda",
            "operations": [
                { "op": "put", "src": "/dev/null", "dst": "/Results.bin", "zero": 4096 }
            ]
        }"#;
        let s: Script = serde_json::from_str(json).unwrap();
        match &s.operations[0] {
            Operation::Put(p) => assert_eq!(p.zero, Some(4096)),
            _ => panic!("expected put"),
        }
    }
}
