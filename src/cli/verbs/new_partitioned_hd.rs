//! `rb-cli new hd
//! {mbr|gpt|apm|sgi|sgi-dklabel|x68k-table|rdb|sun|next|solaris-x86|atari}
//! IMG` — a blank disk image
//! carrying a real partition table with partitions you size and type yourself.
//!
//! This is the CLI grammar only; the layout maths and the table writers
//! live in [`crate::partition::provision`], shared with the GUI's Build Disk
//! mode. See `docs/partition_table_writers_backlog.md` for how each writer is
//! put together.
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
    Sgi(CylinderHdArgs),
    /// SGI disk label (IRIS 2000 / 3000, the pre-IRIX scheme). Eight slots
    /// carrying a role rather than a type; partitions are cylinder-aligned.
    #[command(name = "sgi-dklabel")]
    SgiDklabel(CylinderHdArgs),
    /// Amiga Rigid Disk Block. Partitions are cylinder-aligned.
    Rdb(CylinderHdArgs),
    /// Sun disk label / SMI VTOC (SPARC Solaris / SunOS). Slices are
    /// cylinder-aligned.
    Sun(CylinderHdArgs),
    /// NeXT disk label (NeXTSTEP / OPENSTEP). Up to 8 partitions counted in
    /// the label's own 1024-byte sectors, so --sectors is in those units too.
    Next(CylinderHdArgs),
    /// Solaris x86: an MBR partition holding a 16-slice VTOC. Slices are
    /// cylinder-aligned and the first three cylinders are the label's own.
    #[command(name = "solaris-x86")]
    SolarisX86(CylinderHdArgs),
    /// Sharp X68000 SCSI/SASI table. Up to 8 partitions.
    X68k(PartitionedHdArgs),
    /// Atari ST AHDI root sector. Up to 4 partitions.
    Atari(PartitionedHdArgs),
}

#[derive(Debug, Args)]
pub struct CylinderHdArgs {
    #[command(flatten)]
    pub common: PartitionedHdArgs,

    /// Disk geometry: heads. These tables place partitions on cylinder
    /// boundaries, so the geometry sets the default alignment.
    #[arg(long, default_value_t = crate::partition::sgi_hdd_builder::DEFAULT_HEADS)]
    pub heads: u16,

    /// Disk geometry: sectors per track.
    #[arg(long, default_value_t = crate::partition::sgi_hdd_builder::DEFAULT_SECTORS_PER_TRACK)]
    pub sectors: u16,
}

#[derive(Debug, Args)]
pub struct PartitionedHdArgs {
    /// Image file to create. A `.vhd` name gets a fixed-VHD footer, so
    /// Windows Disk Management and Hyper-V attach the file as it is.
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

    /// Embed an Amiga filesystem handler in the RDB, `DOSTYPE=PATH`,
    /// repeatable. PATH is the handler's AmigaDOS load file (`L:SmartFilesystem`,
    /// `L:PFS3`). A DosType with no ROM handler needs this to mount unaided:
    /// the strap loads it from the RDB. RDB only.
    #[arg(long = "filesystem", value_name = "DOSTYPE=PATH")]
    pub filesystems: Vec<String>,

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
        PartitionedHdCommand::Atari(a) => (TableKind::Atari, a),
        PartitionedHdCommand::Sgi(a) => {
            geometry = Some(Geometry {
                heads: a.heads,
                sectors_per_track: a.sectors,
            });
            (TableKind::Sgi, a.common)
        }
        PartitionedHdCommand::SgiDklabel(a) => {
            geometry = Some(Geometry {
                heads: a.heads,
                sectors_per_track: a.sectors,
            });
            (TableKind::SgiDkLabel, a.common)
        }
        PartitionedHdCommand::Rdb(a) => {
            geometry = Some(Geometry {
                heads: a.heads,
                sectors_per_track: a.sectors,
            });
            (TableKind::Rdb, a.common)
        }
        PartitionedHdCommand::Sun(a) => {
            geometry = Some(Geometry {
                heads: a.heads,
                sectors_per_track: a.sectors,
            });
            (TableKind::Sun, a.common)
        }
        PartitionedHdCommand::Next(a) => {
            geometry = Some(Geometry {
                heads: a.heads,
                sectors_per_track: a.sectors,
            });
            (TableKind::Next, a.common)
        }
        PartitionedHdCommand::SolarisX86(a) => {
            geometry = Some(Geometry {
                heads: a.heads,
                sectors_per_track: a.sectors,
            });
            (TableKind::SolarisX86, a.common)
        }
    };
    let geometry = geometry.unwrap_or_default();

    let disk_size = parse_size(&args.size)?;
    // IRIX, AmigaDOS and SunOS want partitions on cylinder boundaries, so the
    // geometry sets the alignment unless the user overrode it.
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

    let placed = provision::place(&specs, kind, disk_size, align, geometry)?;
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

    // After the table, because the FSHD chain lands past the PART blocks the
    // provisioner has just laid down.
    let handlers = parse_filesystems(&args.filesystems, kind)?;
    if !handlers.is_empty() {
        let mut img = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&args.image)
            .with_context(|| format!("reopening {}", args.image.display()))?;
        provision::write_rdb_filesystems(&mut img, &handlers)
            .context("embedding filesystem handlers in the RDB")?;
        for (dostype, image) in &handlers {
            log_stderr(format!(
                "embedded {} handler ({}) in the RDB FileSystemHeader chain",
                crate::partition::rdb::format_dos_type(*dostype),
                format_size(image.len() as u64),
            ));
        }
    }

    // A `.vhd` name promises a disk Windows and Hyper-V attach directly; a raw
    // image under that name is refused by both, so give it the fixed-VHD footer.
    let is_vhd = args
        .image
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("vhd"));
    if is_vhd {
        let mut img = std::fs::OpenOptions::new()
            .append(true)
            .open(&args.image)
            .with_context(|| format!("reopening {}", args.image.display()))?;
        std::io::Write::write_all(
            &mut img,
            &crate::rbformats::vhd::build_vhd_footer(disk_size),
        )
        .context("appending the VHD footer")?;
        log_stderr("appended a fixed-VHD footer (Disk Management can attach the file)");
    }

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

/// Turn `--filesystem DOSTYPE=PATH` arguments into `(dostype, load-file)` pairs.
/// Keyed by DosType rather than partition, because one `FSHD` serves every
/// partition carrying that type — which is how a real multi-filesystem RDB is
/// laid out.
fn parse_filesystems(specs: &[String], kind: TableKind) -> Result<Vec<(u32, Vec<u8>)>> {
    if specs.is_empty() {
        return Ok(Vec::new());
    }
    if kind != TableKind::Rdb {
        bail!(
            "--filesystem is an Amiga RDB feature; this table is {}",
            kind.label()
        );
    }
    let mut out = Vec::new();
    let mut seen = Vec::new();
    for spec in specs {
        let (type_text, path) = spec
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("--filesystem wants DOSTYPE=PATH, got '{spec}'"))?;
        let dostype = crate::partition::rdb::parse_dos_type(type_text).ok_or_else(|| {
            anyhow::anyhow!(
                "bad Amiga DosType '{type_text}' in --filesystem (try `partmap types --table rdb`)"
            )
        })?;
        if seen.contains(&dostype) {
            bail!("--filesystem names DosType '{type_text}' more than once");
        }
        let image =
            std::fs::read(path).with_context(|| format!("reading filesystem handler {path}"))?;
        // An AmigaDOS load file starts with HUNK_HEADER; anything else would
        // be loaded as code by the strap and hang the machine rather than fail.
        if image.get(..4) != Some(&[0x00, 0x00, 0x03, 0xF3]) {
            bail!(
                "{path} is not an AmigaDOS load file (no HUNK_HEADER);                  --filesystem wants the handler binary, e.g. L:SmartFilesystem"
            );
        }
        seen.push(dostype);
        out.push((dostype, image));
    }
    Ok(out)
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
