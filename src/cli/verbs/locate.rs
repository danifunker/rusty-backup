//! `rb-cli locate IMG[@N] PATH` — print where a file's data fork lives
//! inside a disk image, by absolute byte offset.
//!
//! Output is a JSON envelope with the same `schema_version` / `status` /
//! `result` shape used by `show`, so downstream build scripts can parse
//! it with `jq` (or `serde_json`) instead of grepping for magic-byte
//! markers. The result object carries:
//!
//! ```json
//! { "path": "/Payload", "offset": 27136, "length": 4476,
//!   "fragmented": false, "partition_offset": 49152 }
//! ```
//!
//! `offset` is **absolute within the image file** — it already includes
//! the partition base for APM-wrapped disks, so the caller never needs
//! to know the partition layout.
//!
//! Today this only handles classic HFS (the use case is injecting test
//! payloads into 800K floppies and APM hard-disk images for retro Mac
//! boot work). HFS+ / FAT / NTFS / ext can grow their own
//! `locate_file_by_*` engine methods later; the verb dispatches by
//! partition type.

use anyhow::{anyhow, bail, Result};
use clap::Args;
use serde::Serialize;

use crate::cli::img_at::ImageRef;
use crate::cli::logging::log_stderr;
use crate::cli::output::{emit_envelope, Envelope, OutputFormat};
use crate::cli::resolve::resolve_partition_ro;
use crate::fs::entry::FileEntry;
use crate::fs::hfs::{HfsFileLocation, HfsFilesystem};

#[derive(Debug, Args)]
pub struct LocateArgs {
    /// Image reference (`path` or `path@N` for the 1-based partition index).
    pub image: ImageRef,

    /// Path inside the filesystem (Mac path conventions; `/` is the
    /// separator — `:` is rejected for the same reason as the other verbs).
    pub path: String,

    /// Output format. `json` is the default because the load-bearing
    /// consumer is a build script.
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Debug, Serialize)]
pub struct LocatePayload {
    pub path: String,
    pub offset: u64,
    pub length: u64,
    pub fragmented: bool,
    /// Byte offset of the containing partition within the image; `0` for
    /// raw superfloppies. Surfaced so downstream tools can sanity-check
    /// `offset >= partition_offset` without re-parsing the partition map.
    pub partition_offset: u64,
}

pub fn run(args: LocateArgs) -> Result<()> {
    let payload = locate_payload(&args.image, &args.path)?;
    emit_locate(args.format, &payload)
}

/// Shared implementation used by `locate` and by `put --print-offset`.
/// Opens the image read-only, resolves the partition, walks the path to
/// the file's CNID, and asks the HFS engine for its first-extent offset.
pub fn locate_payload(image: &ImageRef, path: &str) -> Result<LocatePayload> {
    let (file, ctx) = resolve_partition_ro(&image.path, image.partition)?;
    log_stderr(&ctx.label);

    // Filesystem dispatch. Classic HFS is the only locator wired up
    // today; anything else gets a clear error rather than a silent
    // fallthrough.
    let is_hfs = matches!(ctx.type_byte, 0xaf)
        || ctx
            .type_string
            .as_deref()
            .map(|s| s == "Apple_HFS")
            .unwrap_or(false)
        // Raw superfloppy: ctx.type_byte == 0; sniff the MDB magic to
        // confirm it's HFS before opening, otherwise HfsFilesystem::open
        // would error with a less useful "bad MDB signature" message.
        || ctx.type_byte == 0x00 && sniff_hfs(&image.path, ctx.offset)?;
    if !is_hfs {
        bail!(
            "locate is only implemented for classic HFS today; got type 0x{:02x} {}",
            ctx.type_byte,
            ctx.type_string.as_deref().unwrap_or("(unknown)")
        );
    }

    // We open HfsFilesystem directly (not via the trait dispatcher)
    // because locate_file_by_cnid is HFS-specific; the trait surface
    // intentionally doesn't carry offset arithmetic.
    let mut fs =
        HfsFilesystem::open(file, ctx.offset).map_err(|e| anyhow!("opening HFS volume: {e}"))?;

    let entry = resolve_hfs_path(&mut fs, path)?;
    if !entry.is_file() {
        bail!("not a file: {path}");
    }
    let cnid = entry.location as u32;

    let HfsFileLocation {
        offset_in_image,
        length,
        fragmented,
    } = fs.locate_file_by_cnid(cnid).ok_or_else(|| {
        anyhow!("file has no allocated data-fork extents (zero-length or corrupt): {path}")
    })?;

    if fragmented {
        // Per the spec: don't error, but make the caller aware that
        // `offset` only describes the first run.
        log_stderr(format!(
            "warning: {path} is fragmented across multiple extents; \
             the printed offset covers the first extent only"
        ));
    }

    Ok(LocatePayload {
        path: path.to_string(),
        offset: offset_in_image,
        length,
        fragmented,
        partition_offset: ctx.offset,
    })
}

/// Emit a locate payload in the requested format. Text form prints the
/// same JSON envelope (compact) — we deliberately don't expose a TSV /
/// CSV shape, because every downstream consumer we care about is a
/// script that wants structured data.
pub fn emit_locate(format: OutputFormat, payload: &LocatePayload) -> Result<()> {
    match format {
        OutputFormat::Text | OutputFormat::Json | OutputFormat::Yaml => {
            // Text falls through to JSON intentionally; see fn doc.
            let fmt = if format == OutputFormat::Text {
                OutputFormat::Json
            } else {
                format
            };
            emit_envelope(fmt, &Envelope::ok(payload))
        }
        OutputFormat::Csv | OutputFormat::Tsv => {
            bail!("locate does not support csv/tsv; use --format json (default)")
        }
    }
}

/// Walk a `/`-separated path through an HFS volume. We don't use the
/// generic `ls::resolve_path` because that takes `&mut dyn Filesystem`,
/// and we want the concrete `HfsFilesystem` here so the caller can hand
/// the CNID back to `locate_file_by_cnid` without going through the
/// trait object.
fn resolve_hfs_path<R: std::io::Read + std::io::Seek + Send>(
    fs: &mut HfsFilesystem<R>,
    path: &str,
) -> Result<FileEntry> {
    use crate::fs::filesystem::Filesystem;
    let mut current = fs.root().map_err(|e| anyhow!("root: {e}"))?;
    let trimmed = path.trim_start_matches('/').trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok(current);
    }
    for component in trimmed.split('/') {
        if component.is_empty() {
            continue;
        }
        let children = fs
            .list_directory(&current)
            .map_err(|e| anyhow!("list_directory: {e}"))?;
        let next = children
            .into_iter()
            .find(|c| c.name == component)
            .ok_or_else(|| anyhow!("path component not found: {component}"))?;
        current = next;
    }
    Ok(current)
}

/// Probe an HFS MDB signature at `partition_offset + 1024` without
/// opening the full filesystem. Returns `true` iff the magic matches.
fn sniff_hfs(path: &std::path::Path, partition_offset: u64) -> Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = crate::cli::io::open_image_ro(path)?;
    f.seek(SeekFrom::Start(partition_offset + 1024))?;
    let mut sig = [0u8; 2];
    if f.read_exact(&mut sig).is_err() {
        return Ok(false);
    }
    Ok(&sig == b"BD")
}
