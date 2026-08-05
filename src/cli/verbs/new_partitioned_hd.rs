//! `rb-cli new hd {mbr|gpt|apm|sgi|x68k-table} IMG` — a blank disk image
//! carrying a real partition table with partitions you size and type yourself.
//!
//! This is the CLI grammar only; the layout maths and the five table writers
//! live in [`crate::partition::provision`], shared with the GUI's Build Disk
//! mode. Sun, RDB and AHDI are parse-only for now; see
//! `docs/partition_table_writers_backlog.md` for what each writer needs.
//!
//! The existing `new hd` targets (`x68k`, `sgi-efs`) each build one specific
//! platform's bootable disk. This one is the generic counterpart: it lays down
//! a table with N partitions, optionally pours an image into each with
//! `--fill`, and leaves the rest empty for `rb-cli write IMG DEV --partition N`
//! or `rb-cli reformat`.
//!
//! Partition types are the same strings the partition-table editor takes —
//! `rb-cli partmap types --table {mbr|gpt|apm}` lists them, and
//! [`crate::partition::type_catalog`] is the shared source for both.
//!
//! Sizes are laid out in order from `--align` (1 MiB by default), each rounded
//! up to the alignment. A single `rest` size claims whatever is left, which is
//! how you fill a disk without doing the arithmetic yourself.

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::model::provision_runner::{self, ProvisionRequest};
use crate::partition::format_size;
use crate::partition::provision::{self, Geometry, PartSpec};
use crate::partition::type_catalog::TableKind;

#[derive(Debug, Subcommand)]
pub enum PartitionedHdCommand {
    /// MBR (DOS / PC). Up to 4 primary partitions.
    Mbr(PartitionedHdArgs),
    /// GPT (UEFI, modern macOS and Linux).
    Gpt(PartitionedHdArgs),
    /// APM (Apple Partition Map) — classic Mac OS and PowerPC.
    Apm(PartitionedHdArgs),
    /// SGI volume header (IRIX). Partitions are cylinder-aligned.
    Sgi(SgiHdArgs),
    /// Sharp X68000 SCSI/SASI table. Up to 8 partitions.
    X68k(PartitionedHdArgs),
}

#[derive(Debug, Args)]
pub struct SgiHdArgs {
    #[command(flatten)]
    pub common: PartitionedHdArgs,

    /// Disk geometry: heads. IRIX places partitions on cylinder boundaries.
    #[arg(long, default_value_t = crate::partition::sgi_hdd_builder::DEFAULT_HEADS)]
    pub heads: u16,

    /// Disk geometry: sectors per track.
    #[arg(long, default_value_t = crate::partition::sgi_hdd_builder::DEFAULT_SECTORS_PER_TRACK)]
    pub sectors: u16,
}

#[derive(Debug, Args)]
pub struct PartitionedHdArgs {
    /// Image file to create.
    pub image: PathBuf,

    /// Total disk size (accepts `K`/`M`/`G` suffixes).
    #[arg(long)]
    pub size: String,

    /// A partition, repeatable, in disk order: `SIZE[:TYPE[:NAME]]`.
    /// SIZE accepts `K`/`M`/`G`, or `rest` for the remaining space (once).
    /// TYPE is a value from `partmap types`; NAME is APM/GPT only.
    #[arg(long = "partition", required = true)]
    pub partitions: Vec<String>,

    /// Pour an image into a partition as it is created: `N=PATH`, 1-based,
    /// repeatable. Any format the engine can read is decoded on the way in.
    #[arg(long = "fill", value_name = "N=PATH")]
    pub fills: Vec<String>,

    /// Alignment for partition starts. Default 1 MiB; use 63s for
    /// DOS-era cylinder alignment on vintage machines.
    #[arg(long, default_value = "1M")]
    pub align: String,

    /// Overwrite `image` if it already exists.
    #[arg(long)]
    pub force: bool,
}

pub fn run(cmd: PartitionedHdCommand) -> Result<()> {
    let mut geometry = None;
    let (kind, args) = match cmd {
        PartitionedHdCommand::Mbr(a) => (TableKind::Mbr, a),
        PartitionedHdCommand::Gpt(a) => (TableKind::Gpt, a),
        PartitionedHdCommand::Apm(a) => (TableKind::Apm, a),
        PartitionedHdCommand::X68k(a) => (TableKind::X68k, a),
        PartitionedHdCommand::Sgi(a) => {
            geometry = Some(Geometry {
                heads: a.heads,
                sectors_per_track: a.sectors,
            });
            (TableKind::Sgi, a.common)
        }
    };
    let geometry = geometry.unwrap_or_default();

    let disk_size = parse_size(&args.size)?;
    // IRIX wants partitions on cylinder boundaries, so the geometry sets the
    // alignment unless the user overrode it.
    let align = if args.align == "1M" {
        provision::default_align(kind, geometry)
    } else {
        provision::parse_align(&args.align)?
    };
    let specs = args
        .partitions
        .iter()
        .map(|s| parse_spec(s))
        .collect::<Result<Vec<_>>>()?;

    let placed = provision::place(&specs, kind, disk_size, align)?;
    let sources = parse_fills(&args.fills, placed.len())?;

    if args.image.exists() && !args.force {
        bail!(
            "{} already exists; pass --force to overwrite",
            args.image.display()
        );
    }

    let req = ProvisionRequest {
        target_path: args.image.clone(),
        target_size_bytes: disk_size,
        kind,
        geometry,
        partitions: placed.clone(),
        sources,
    };
    let status = Arc::new(Mutex::new(
        crate::model::physical_write_runner::PhysicalWriteStatus {
            finished: false,
            error: None,
            log_messages: Vec::new(),
            current_bytes: 0,
            total_bytes: 0,
            cancel_requested: false,
        },
    ));
    provision_runner::run_worker(&req, Arc::clone(&status))
        .with_context(|| format!("building {}", args.image.display()))?;

    log_stderr(format!(
        "created {} ({} {}, {} partition(s))",
        args.image.display(),
        format_size(disk_size),
        kind.label(),
        placed.len(),
    ));
    for (i, p) in placed.iter().enumerate() {
        log_stderr(provision::describe_placed(kind, i, p));
    }
    if args.fills.is_empty() {
        log_stderr(
            "partitions are empty; format with `reformat` or fill with `write --partition N`",
        );
    }
    Ok(())
}

fn parse_spec(s: &str) -> Result<PartSpec> {
    let mut parts = s.splitn(3, ':');
    let size_text = parts.next().unwrap_or("").trim();
    if size_text.is_empty() {
        bail!("empty --partition spec");
    }
    let size = if size_text.eq_ignore_ascii_case("rest") {
        None
    } else {
        Some(parse_size(size_text)?)
    };
    let type_text = parts
        .next()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    let name = parts
        .next()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    Ok(PartSpec {
        size,
        type_text,
        name,
    })
}

/// Turn `--fill N=PATH` arguments into a per-partition source slot list.
///
/// Split on the *first* `=` only, so a Windows path keeps its drive letter.
fn parse_fills(fills: &[String], partition_count: usize) -> Result<Vec<Option<PathBuf>>> {
    let mut out = vec![None; partition_count];
    for spec in fills {
        let (n, path) = spec
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--fill wants N=PATH, got '{spec}'"))?;
        let idx: usize = n
            .trim()
            .parse()
            .with_context(|| format!("bad partition number in --fill '{spec}'"))?;
        if idx == 0 || idx > partition_count {
            bail!("--fill {idx} names partition {idx}, but only {partition_count} were defined",);
        }
        if out[idx - 1].is_some() {
            bail!("--fill names partition {idx} more than once");
        }
        let path = PathBuf::from(path.trim());
        if !path.exists() {
            bail!("--fill source {} does not exist", path.display());
        }
        out[idx - 1] = Some(path);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(s: &str) -> PartSpec {
        parse_spec(s).expect("spec parses")
    }

    #[test]
    fn spec_parses_size_type_and_name() {
        let s = spec("20M:Apple_HFS:MacOS");
        assert_eq!(s.size, Some(20 * 1024 * 1024));
        assert_eq!(s.type_text.as_deref(), Some("Apple_HFS"));
        assert_eq!(s.name.as_deref(), Some("MacOS"));

        let bare = spec("1G");
        assert_eq!(bare.size, Some(1024 * 1024 * 1024));
        assert!(bare.type_text.is_none() && bare.name.is_none());

        assert!(spec("rest").size.is_none());
        assert!(spec("REST").size.is_none());
    }

    #[test]
    fn fills_map_one_based_numbers_to_slots() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let path = f.path().display().to_string();
        let out = parse_fills(&[format!("2={path}")], 3).unwrap();
        assert!(out[0].is_none() && out[2].is_none());
        assert_eq!(out[1].as_deref(), Some(f.path()));
    }

    #[test]
    fn fills_reject_bad_numbers_duplicates_and_missing_files() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let path = f.path().display().to_string();
        assert!(parse_fills(&[format!("0={path}")], 2).is_err(), "1-based");
        assert!(parse_fills(&[format!("3={path}")], 2).is_err(), "past end");
        assert!(parse_fills(&["1".to_string()], 2).is_err(), "no '='");
        assert!(
            parse_fills(&[format!("1={path}"), format!("1={path}")], 2).is_err(),
            "duplicate",
        );
        assert!(
            parse_fills(&["1=/no/such/image.img".to_string()], 2).is_err(),
            "missing source",
        );
    }

    /// A Windows source path keeps its drive letter — `split_once`, not `split`.
    #[test]
    fn fills_split_on_the_first_equals_only() {
        let err = parse_fills(&[r"1=C:\images\dos.img".to_string()], 1)
            .expect_err("path does not exist here");
        assert!(format!("{err:#}").contains(r"C:\images\dos.img"), "{err:#}");
    }
}
