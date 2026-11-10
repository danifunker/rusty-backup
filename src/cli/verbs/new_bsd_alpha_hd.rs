//! `rb-cli new hd bsd-alpha IMG --size N PARTS...`: a FreeBSD/Alpha disk with its 4.4BSD label
//! in sector 0, ready for FreeBSD 4.x / 5.x's installer or, formatted, to boot or be filled.
//!
//! Each partition is `LETTER:SIZE[:FSTYPE[:MOUNT]]` in disk order, like `a:512M:4.2BSD:/`;
//! `c` is the whole disk and always written. Partition `a` starts at sector 0 around the label.
//! `--format` makes each 4.2BSD partition a little-endian UFS1 with 16 KiB blocks and 2 KiB
//! fragments, as FreeBSD's `newfs` did, and the label takes its geometry from each superblock.
//! `--bootblock` installs `/boot/boot1` from sector 1 and points the SRM console at it. The
//! table maths and writer are `crate::partition::provision` (`TableKind::Bsd`); see
//! `docs/bsd_alpha_disks.md`.

use anyhow::{bail, Context, Result};
use clap::Args;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::model::disk_builder::RowFormat;
use crate::model::format::FsKind;
use crate::model::provision_runner::{self, ProvisionRequest};
use crate::partition::provision::{self, Geometry, PartSpec};
use crate::partition::type_catalog::TableKind;
use disk_core::display::format_size;

/// Sectors between the label's sector and the 8 KiB UFS leaves free at the head of `a`.
const BOOT_SECTORS_MAX: u64 = 15;

#[derive(Debug, Args)]
pub struct BsdAlphaHdArgs {
    /// Image file to create.
    pub image: PathBuf,

    /// Total disk size (accepts `K`/`M`/`G` suffixes).
    #[arg(long)]
    pub size: String,

    /// Partitions in disk order, `LETTER:SIZE[:FSTYPE[:MOUNT]]` (letter a-h but not c; `rest` once;
    /// fstype `4.2BSD`, `swap` or `unused`). MOUNT is a note: `/etc/fstab` holds mount points.
    #[arg(required = true, num_args = 1..)]
    pub partitions: Vec<String>,

    /// Format each 4.2BSD partition as little-endian UFS1, 16 KiB blocks / 2 KiB fragments.
    #[arg(long)]
    pub format: bool,

    /// FreeBSD/Alpha's `/boot/boot1`, written from sector 1 (at most 7680 bytes); sector 0
    /// then points the SRM console at it.
    #[arg(long, value_name = "FILE")]
    pub bootblock: Option<PathBuf>,

    /// Alignment for partition starts. Default 1 MiB.
    #[arg(long, default_value = "1M")]
    pub align: String,

    /// Overwrite `image` if it already exists.
    #[arg(long)]
    pub force: bool,
}

/// One `LETTER:SIZE[:FSTYPE[:MOUNT]]` argument.
fn parse_part(s: &str) -> Result<(PartSpec, Option<String>)> {
    let mut f = s.splitn(4, ':').map(str::trim);
    let letter = f.next().unwrap_or("");
    let size_text = f.next().unwrap_or("");
    if letter.len() != 1 || size_text.is_empty() {
        bail!("partition '{s}' is not LETTER:SIZE[:FSTYPE[:MOUNT]] (e.g. a:512M:4.2BSD:/)");
    }
    let size = match size_text.eq_ignore_ascii_case("rest") {
        true => None,
        false => Some(parse_size(size_text)?),
    };
    let fstype = f.next().filter(|t| !t.is_empty()).map(str::to_string);
    if let Some(t) = &fstype {
        if part_tables::bsd::fstype_from_text(t).is_none() {
            bail!("partition '{s}': unknown fstype '{t}' (4.2BSD, swap, unused)");
        }
    }
    let mount = f.next().filter(|t| !t.is_empty()).map(str::to_string);
    let spec = PartSpec {
        size,
        type_text: fstype,
        name: Some(letter.to_ascii_lowercase()),
    };
    Ok((spec, mount))
}

/// The UFS1 a 4.2BSD partition gets: FreeBSD `newfs`'s 16 KiB / 2 KiB, little-endian, no name.
fn ufs_format() -> RowFormat {
    let mut f = RowFormat::new(FsKind::Ufs);
    f.opts.block_size = Some(16 * 1024);
    f.opts.big_endian = false;
    f.opts.label = String::new();
    f
}

/// Write `boot` from sector 1 and point sector 0's SRM descriptor at it.
fn install_bootblock(image: &std::path::Path, boot: &[u8]) -> Result<u64> {
    let blocks = (boot.len() as u64).div_ceil(512);
    if blocks == 0 || blocks > BOOT_SECTORS_MAX {
        bail!(
            "a boot block takes 1 to {BOOT_SECTORS_MAX} sectors ahead of the UFS superblock; \
             this one is {} bytes",
            boot.len()
        );
    }
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(image)
        .with_context(|| format!("reopening {}", image.display()))?;
    f.seek(SeekFrom::Start(512))?;
    f.write_all(boot)?;
    let mut sector = [0u8; 512];
    f.seek(SeekFrom::Start(0))?;
    f.read_exact(&mut sector)?;
    part_tables::bsd::set_alpha_boot_descriptor(&mut sector, blocks, 1);
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&sector)?;
    f.sync_all()?;
    Ok(blocks)
}

pub fn run(args: BsdAlphaHdArgs) -> Result<()> {
    let kind = TableKind::Bsd;
    let disk_size = parse_size(&args.size)?;
    let align = provision::parse_align(&args.align)?;
    let parsed = args
        .partitions
        .iter()
        .map(|s| parse_part(s))
        .collect::<Result<Vec<_>>>()?;
    let specs: Vec<PartSpec> = parsed.iter().map(|(s, _)| s.clone()).collect();
    let placed = provision::place(&specs, kind, disk_size, align, Geometry::default())?;
    let boot = match &args.bootblock {
        Some(p) => Some(std::fs::read(p).with_context(|| format!("reading {}", p.display()))?),
        None => None,
    };
    if args.image.exists() && !args.force {
        bail!(
            "{} already exists; pass --force to overwrite",
            args.image.display()
        );
    }
    let ffs = |p: &provision::Placed| {
        part_tables::bsd::fstype_from_text(&p.type_text) == Some(part_tables::bsd::FS_BSDFFS)
    };
    let formats = placed
        .iter()
        .map(|p| (args.format && ffs(p)).then(ufs_format))
        .collect();
    let req = ProvisionRequest {
        target_path: args.image.clone(),
        target_size_bytes: disk_size,
        kind,
        geometry: Geometry::default(),
        partitions: placed.clone(),
        sources: vec![None; placed.len()],
        formats,
        full_write: false,
        rdb_handlers: Vec::new(),
        mac_driver: provision_runner::MacDriver::Off,
        head: None,
        donor: None,
        copies: Vec::new(),
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
    if let Some(boot) = &boot {
        let blocks = install_bootblock(&args.image, boot)?;
        log_stderr(format!(
            "installed a {}-byte boot block in sectors 1-{blocks}",
            boot.len()
        ));
    }
    log_stderr(format!(
        "created {} ({} {}, {} partition(s))",
        args.image.display(),
        format_size(disk_size),
        kind.label(),
        placed.len(),
    ));
    for (p, (_, mount)) in placed.iter().zip(&parsed) {
        let at = mount
            .as_deref()
            .map(|m| format!(" ({m})"))
            .unwrap_or_default();
        log_stderr(format!(
            "  {}: {} at LBA {}, {} sectors{at}",
            p.name,
            p.type_text,
            p.start_lba,
            p.size_bytes / 512
        ));
    }
    if !args.format {
        log_stderr("partitions are empty; pass --format for UFS, or let FreeBSD's newfs do it");
    }
    Ok(())
}
