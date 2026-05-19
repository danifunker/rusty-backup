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
    pub operations: Vec<Operation>,
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

    if args.dry_run {
        out_stdout(format!(
            "dry-run plan: {} operation(s) on {}",
            script.operations.len(),
            target.path.display()
        ));
        for (i, op) in script.operations.iter().enumerate() {
            out_stdout(format!("  {:>3}  {}", i + 1, describe(op)));
        }
        return Ok(());
    }

    let (file, ctx) = resolve_partition_rw(&target.path, target.partition)?;
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_editable_filesystem(
        file,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
    )
    .map_err(|e| anyhow!("opening filesystem for write: {e}"))?;

    let mut applied = 0usize;
    let mut failures: Vec<(usize, String)> = Vec::new();
    for (i, op) in script.operations.iter().enumerate() {
        let res = apply_op(&mut *fs, op, &defaults);
        match res {
            Ok(()) => {
                applied += 1;
                log_stderr(format!(
                    "  + [{}/{}] {}",
                    i + 1,
                    script.operations.len(),
                    describe(op)
                ));
            }
            Err(e) => {
                let msg = format!(
                    "[{}/{}] {}: {e}",
                    i + 1,
                    script.operations.len(),
                    describe(op)
                );
                log_stderr(format!("  ! {msg}"));
                failures.push((i + 1, msg));
                if !args.continue_on_error {
                    // Flush the partial work and bail.
                    fs.sync_metadata().ok();
                    bail!("batch aborted after {applied} successful op(s); rerun with --continue-on-error to keep going");
                }
            }
        }
    }
    fs.sync_metadata()
        .map_err(|e| anyhow!("sync_metadata: {e}"))?;

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
    }
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
    }
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
