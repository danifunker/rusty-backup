//! `rb-cli du IMG[@N] PATH...` — recursive disk usage of paths inside a
//! filesystem, counting **both forks** (data + resource) on classic-Mac
//! volumes.
//!
//! Unlike `ls`, which only reports data-fork sizes, `du` sums the resource
//! fork too. Classic Mac applications keep their code in the resource fork
//! and frequently have a zero-byte data fork, so an `ls`-based size estimate
//! undercounts a real app folder by ~100%. `du` closes that gap: a build tool
//! sizing a destination volume before copying (e.g. MacAtrium) can ask for the
//! true both-fork bytes of each source folder in one call.
//!
//! Generic across every read-only filesystem the engine layer opens; the
//! resource-fork columns are simply zero on filesystems without forks.

use anyhow::{anyhow, Result};
use clap::Args;
use serde::Serialize;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::{log_stderr, out_stdout};
use crate::cli::output::{emit_envelope, Envelope, OutputFormat};
use crate::cli::resolve::{resolve_partition_streaming_forced_inside, FsDispatchOverride};
use crate::fs::entry::{EntryType, FileEntry};
use crate::fs::filesystem::Filesystem;
use crate::partition::format_size;

#[derive(Debug, Args)]
pub struct DuArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// One or more paths inside the filesystem (use `/` as the separator).
    /// Each is measured independently. Defaults to the volume root when
    /// none are given. A literal `/` inside a name is written `\/` (and a
    /// literal `\` as `\\`); on HFS / HFS+ a `:`-separated path also works.
    #[arg(value_name = "PATH")]
    pub paths: Vec<String>,

    /// Report subdirectory totals down to this many levels below each PATH
    /// (like `du --max-depth`). `0` (default) prints only the totals for the
    /// path itself; `1` adds its immediate children, and so on. The full
    /// subtree is always summed regardless — depth only controls how much
    /// detail is printed.
    #[arg(long, default_value_t = 0)]
    pub depth: u32,

    /// Emit machine-readable JSON. Shorthand for `--format json`.
    #[arg(long, conflicts_with = "format")]
    pub json: bool,

    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub format: OutputFormat,

    /// Password for encrypted containers / filesystems (see `ls`).
    #[arg(long)]
    pub password: Option<String>,

    /// For a `.zip` holding more than one disk image, the archive entry to open.
    #[arg(long = "inside", value_name = "NAME")]
    pub inside: Option<String>,

    #[command(flatten)]
    pub fs_override: FsDispatchOverride,
}

/// Recursive both-fork totals for a subtree.
#[derive(Debug, Clone, Copy, Default)]
struct Totals {
    data_bytes: u64,
    rsrc_bytes: u64,
    /// Bytes as actually allocated on this volume (each fork rounded up to
    /// the allocation block). `0` when the allocation unit is unknown; the
    /// JSON field is then suppressed.
    alloc_bytes: u64,
    files: u64,
    dirs: u64,
}

impl Totals {
    fn merge(&mut self, other: &Totals) {
        self.data_bytes += other.data_bytes;
        self.rsrc_bytes += other.rsrc_bytes;
        self.alloc_bytes += other.alloc_bytes;
        self.files += other.files;
        self.dirs += other.dirs;
    }

    fn apparent_bytes(&self) -> u64 {
        self.data_bytes + self.rsrc_bytes
    }
}

/// Round `bytes` up to a whole allocation block. A zero-length fork occupies
/// no blocks, so it contributes nothing.
fn round_up(bytes: u64, unit: u64) -> u64 {
    if unit == 0 || bytes == 0 {
        bytes
    } else {
        bytes.div_ceil(unit) * unit
    }
}

/// One reported node in the `du` tree. Always carries the recursive subtree
/// totals; `children` is populated only for nodes within the reporting depth.
struct Node {
    path: String,
    kind: EntryType,
    totals: Totals,
    children: Vec<Node>,
}

/// The per-path result: either a measured node, or a clean "not found" marker
/// so a missing path is distinguishable from an empty folder rather than
/// aborting the whole run.
enum PathResult {
    Found(Node),
    Missing(String),
}

/// Walk `entry`'s subtree, summing both forks. `report_remaining` gates how
/// many levels of child detail are retained (the walk always recurses fully to
/// compute totals). `alloc_unit` is `Some` only when the volume has a fixed
/// allocation block.
fn walk(
    fs: &mut dyn Filesystem,
    entry: &FileEntry,
    alloc_unit: Option<u64>,
    report_remaining: u32,
) -> Result<Node> {
    // A symlink / special file, or any non-directory, is a leaf: count its
    // own forks and stop. We never follow a symlink (avoids alias loops).
    if !entry.is_directory() {
        let data = entry.size;
        let rsrc = entry.resource_fork_size.unwrap_or(0);
        let alloc = alloc_unit
            .map(|u| round_up(data, u) + round_up(rsrc, u))
            .unwrap_or(0);
        return Ok(Node {
            path: entry.path.clone(),
            kind: entry.entry_type,
            totals: Totals {
                data_bytes: data,
                rsrc_bytes: rsrc,
                alloc_bytes: alloc,
                files: 1,
                dirs: 0,
            },
            children: Vec::new(),
        });
    }

    // Directory: count itself, then fold in every child.
    let mut totals = Totals {
        dirs: 1,
        ..Totals::default()
    };
    let mut children = Vec::new();
    let listing = fs
        .list_directory(entry)
        .map_err(|e| anyhow!("list_directory {}: {e}", entry.path))?;
    for child in &listing {
        let node = walk(fs, child, alloc_unit, report_remaining.saturating_sub(1))?;
        totals.merge(&node.totals);
        if report_remaining > 0 {
            children.push(node);
        }
    }
    Ok(Node {
        path: entry.path.clone(),
        kind: EntryType::Directory,
        totals,
        children,
    })
}

pub fn run(args: DuArgs) -> Result<()> {
    let format = if args.json {
        OutputFormat::Json
    } else {
        args.format
    };
    // du produces a nested per-path shape; CSV/TSV don't fit it.
    crate::cli::output::require_non_flat(format, "du")?;

    let pw_bytes = args.password.as_deref().map(|s| s.as_bytes());
    let (reader, mut ctx) = resolve_partition_streaming_forced_inside(
        &args.image.path,
        args.image.partition,
        pw_bytes,
        args.fs_override.fs_type.as_deref(),
        args.inside.as_deref(),
    )
    .map_err(|e| crate::cli::optical_hint::with_optical_hint(e, &args.image.path))?;
    args.fs_override.apply(&mut ctx);
    log_stderr(&ctx.label);
    let mut fs = crate::fs::open_filesystem_with_passphrase(
        reader,
        ctx.offset,
        ctx.type_byte,
        ctx.type_string.as_deref(),
        args.password.as_deref(),
    )
    .map_err(|e| {
        crate::cli::optical_hint::with_optical_hint(
            anyhow!("opening filesystem: {e}"),
            &args.image.path,
        )
    })?;

    emit_du(&mut *fs, args.paths, args.depth, format)
}

/// The `du` engine over any opened [`Filesystem`]: resolve each path, sum both
/// forks recursively (rounding to the volume's allocation block where known),
/// and emit text or JSON/YAML. Front-ends (the flat `du` verb over block-image
/// filesystems, `optical du` over a disc adapter) open the filesystem their own
/// way and hand it here so the walk / output shape is identical.
///
/// `format` must already be non-flat (callers run `require_non_flat`).
pub(crate) fn emit_du(
    fs: &mut dyn Filesystem,
    mut paths: Vec<String>,
    depth: u32,
    format: OutputFormat,
) -> Result<()> {
    let alloc_unit = fs.allocation_unit();

    // Default to the volume root when no path is given.
    if paths.is_empty() {
        paths.push("/".to_string());
    }

    let mut results = Vec::with_capacity(paths.len());
    for path in &paths {
        match super::ls::resolve_path(fs, path) {
            Ok(entry) => {
                let mut node = walk(fs, &entry, alloc_unit, depth)?;
                // Report under the exact path the user asked for (resolved
                // entries carry their canonical on-volume path, which may
                // differ in separator/case from the argument).
                node.path = path.clone();
                results.push(PathResult::Found(node));
            }
            Err(_) => results.push(PathResult::Missing(path.clone())),
        }
    }

    match format {
        OutputFormat::Text => emit_text(&results, alloc_unit),
        OutputFormat::Json | OutputFormat::Yaml => {
            let payload = DuPayload {
                allocation_unit: alloc_unit,
                paths: results.iter().map(|r| entry_json(r, alloc_unit)).collect(),
            };
            emit_envelope(format, &Envelope::ok(payload))
        }
        _ => unreachable!("require_non_flat rejects csv/tsv above"),
    }
}

// ---------------------------------------------------------------------------
// text rendering
// ---------------------------------------------------------------------------

fn emit_text(results: &[PathResult], alloc_unit: Option<u64>) -> Result<()> {
    let show_alloc = alloc_unit.is_some();
    // Header. Sizes render human-readable (exact bytes live in --format json).
    if show_alloc {
        out_stdout(format!(
            "{:>10}  {:>10}  {:>10}  {:>10}  {:>6}  {:>5}  path",
            "data", "rsrc", "apparent", "alloc", "files", "dirs"
        ));
    } else {
        out_stdout(format!(
            "{:>10}  {:>10}  {:>10}  {:>6}  {:>5}  path",
            "data", "rsrc", "apparent", "files", "dirs"
        ));
    }
    for r in results {
        match r {
            PathResult::Missing(path) => {
                // Clean, greppable marker — not an error dump. Goes to stdout
                // so a caller parsing rows sees it inline with the rest.
                out_stdout(format!("{:>10}  (not found)  {path}", "-"));
            }
            PathResult::Found(node) => print_node_text(node, show_alloc),
        }
    }
    Ok(())
}

fn print_node_text(node: &Node, show_alloc: bool) {
    let t = &node.totals;
    if show_alloc {
        out_stdout(format!(
            "{:>10}  {:>10}  {:>10}  {:>10}  {:>6}  {:>5}  {}",
            format_size(t.data_bytes),
            format_size(t.rsrc_bytes),
            format_size(t.apparent_bytes()),
            format_size(t.alloc_bytes),
            t.files,
            t.dirs,
            node.path,
        ));
    } else {
        out_stdout(format!(
            "{:>10}  {:>10}  {:>10}  {:>6}  {:>5}  {}",
            format_size(t.data_bytes),
            format_size(t.rsrc_bytes),
            format_size(t.apparent_bytes()),
            t.files,
            t.dirs,
            node.path,
        ));
    }
    for child in &node.children {
        print_node_text(child, show_alloc);
    }
}

// ---------------------------------------------------------------------------
// JSON shape
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct DuPayload {
    /// Volume allocation block size in bytes, when the filesystem has a fixed
    /// one. `alloc_bytes` on each entry is meaningless without it.
    #[serde(skip_serializing_if = "Option::is_none")]
    allocation_unit: Option<u64>,
    paths: Vec<DuEntry>,
}

#[derive(Debug, Serialize)]
struct DuEntry {
    path: String,
    /// `true` for a measured path, `false` when the path does not exist. A
    /// found-but-empty directory has `found: true` with zero counts, so the
    /// two are distinguishable.
    found: bool,
    /// `"dir"` or `"file"` (files include symlinks / special files). Absent
    /// when `found` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rsrc_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    apparent_bytes: Option<u64>,
    /// Bytes as allocated on this volume (each fork rounded up to the
    /// allocation block). Absent when the allocation unit is unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    alloc_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    files: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dirs: Option<u64>,
    /// Subdirectory / child detail, present only when `--depth` > 0.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    entries: Vec<DuEntry>,
}

fn kind_str(kind: EntryType) -> &'static str {
    match kind {
        EntryType::Directory => "dir",
        _ => "file",
    }
}

fn node_json(node: &Node, alloc_unit: Option<u64>) -> DuEntry {
    let t = &node.totals;
    DuEntry {
        path: node.path.clone(),
        found: true,
        kind: Some(kind_str(node.kind)),
        data_bytes: Some(t.data_bytes),
        rsrc_bytes: Some(t.rsrc_bytes),
        apparent_bytes: Some(t.apparent_bytes()),
        alloc_bytes: alloc_unit.map(|_| t.alloc_bytes),
        files: Some(t.files),
        dirs: Some(t.dirs),
        entries: node
            .children
            .iter()
            .map(|c| node_json(c, alloc_unit))
            .collect(),
    }
}

fn entry_json(r: &PathResult, alloc_unit: Option<u64>) -> DuEntry {
    match r {
        PathResult::Found(node) => node_json(node, alloc_unit),
        PathResult::Missing(path) => DuEntry {
            path: path.clone(),
            found: false,
            kind: None,
            data_bytes: None,
            rsrc_bytes: None,
            apparent_bytes: None,
            alloc_bytes: None,
            files: None,
            dirs: None,
            entries: Vec::new(),
        },
    }
}
