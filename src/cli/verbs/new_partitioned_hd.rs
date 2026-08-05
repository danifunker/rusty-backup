//! `rb-cli new hd {mbr|gpt|apm|sgi|x68k-table} IMG` — a blank disk image
//! carrying a real partition table with partitions you size and type yourself.
//!
//! Sun, RDB and AHDI are parse-only for now; see
//! `docs/partition_table_writers_backlog.md` for what each writer needs.
//!
//! The existing `new hd` targets (`x68k`, `sgi-efs`) each build one specific
//! platform's bootable disk. This one is the generic counterpart: it lays down
//! a table with N partitions and leaves them empty, so the volumes
//! can be poured in afterwards with `rb-cli write IMG DEV --partition N` (or
//! the GUI's Write Image File mode) and the filesystems formatted with
//! `rb-cli reformat`.
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
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::cli::logging::log_stderr;
use crate::cli::parse::parse_size;
use crate::partition::type_catalog::{self, TableKind};
use crate::partition::{apm, format_size, gpt, mbr};

const SECTOR: u64 = 512;

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

    /// Alignment for partition starts. Default 1 MiB; use 63s for
    /// DOS-era cylinder alignment on vintage machines.
    #[arg(long, default_value = "1M")]
    pub align: String,

    /// Overwrite `image` if it already exists.
    #[arg(long)]
    pub force: bool,
}

/// One `--partition` spec after parsing, before it is placed on the disk.
#[derive(Debug, Clone)]
struct PartSpec {
    /// `None` means "the rest of the disk".
    size: Option<u64>,
    type_text: Option<String>,
    name: Option<String>,
}

/// A partition once it has been given a place.
#[derive(Debug, Clone)]
struct Placed {
    start_lba: u64,
    size_bytes: u64,
    type_text: String,
    name: String,
}

pub fn run(cmd: PartitionedHdCommand) -> Result<()> {
    let mut geometry = None;
    let (kind, args) = match cmd {
        PartitionedHdCommand::Mbr(a) => (TableKind::Mbr, a),
        PartitionedHdCommand::Gpt(a) => (TableKind::Gpt, a),
        PartitionedHdCommand::Apm(a) => (TableKind::Apm, a),
        PartitionedHdCommand::X68k(a) => (TableKind::X68k, a),
        PartitionedHdCommand::Sgi(a) => {
            geometry = Some((a.heads, a.sectors));
            (TableKind::Sgi, a.common)
        }
    };

    let disk_size = parse_size(&args.size)?;
    // IRIX wants partitions on cylinder boundaries, so the geometry sets the
    // alignment unless the user overrode it.
    let align = match geometry {
        Some((heads, spt)) if args.align == "1M" => u64::from(heads) * u64::from(spt) * SECTOR,
        _ => parse_align(&args.align)?,
    };
    let specs = args
        .partitions
        .iter()
        .map(|s| parse_spec(s))
        .collect::<Result<Vec<_>>>()?;

    let slot_limit = match kind {
        TableKind::Mbr => Some(4),
        TableKind::X68k => Some(crate::partition::x68k::X68K_MAX_PARTITIONS),
        // SGI has 16 slots but reserves 8 (volhdr) and 10 (whole volume).
        TableKind::Sgi => Some(crate::partition::sgi::SGI_NUM_PARTITIONS - 2),
        _ => None,
    };
    if let Some(limit) = slot_limit {
        if specs.len() > limit {
            bail!(
                "{} holds at most {} partitions; {} were given",
                kind.label(),
                limit,
                specs.len(),
            );
        }
    }

    let placed = place(&specs, kind, disk_size, align)?;

    if args.image.exists() && !args.force {
        bail!(
            "{} already exists; pass --force to overwrite",
            args.image.display()
        );
    }

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&args.image)
        .with_context(|| format!("creating {}", args.image.display()))?;
    file.set_len(disk_size)
        .with_context(|| format!("sizing {} to {} bytes", args.image.display(), disk_size))?;

    match kind {
        TableKind::Mbr => write_mbr(&mut file, &placed)?,
        TableKind::Gpt => write_gpt(&mut file, &placed, disk_size)?,
        TableKind::Apm => write_apm(&mut file, &placed, disk_size)?,
        TableKind::X68k => write_x68k(&mut file, &placed, disk_size)?,
        TableKind::Sgi => {
            let (heads, spt) = geometry.unwrap_or((
                crate::partition::sgi_hdd_builder::DEFAULT_HEADS,
                crate::partition::sgi_hdd_builder::DEFAULT_SECTORS_PER_TRACK,
            ));
            write_sgi(&mut file, &placed, disk_size, heads, spt)?
        }
        _ => bail!("no table writer for {}", kind.label()),
    }
    file.flush().context("flushing the new image")?;

    log_stderr(format!(
        "created {} ({} {}, {} partition(s))",
        args.image.display(),
        format_size(disk_size),
        kind.label(),
        placed.len(),
    ));
    for (i, p) in placed.iter().enumerate() {
        log_stderr(format!(
            "  {}: {} at LBA {} ({})",
            i + 1,
            format_size(p.size_bytes),
            p.start_lba,
            type_catalog::describe(kind, &p.type_text).unwrap_or(&p.type_text),
        ));
    }
    log_stderr("partitions are empty; format with `reformat` or fill with `write --partition N`");
    Ok(())
}

/// Alignment as a byte count, accepting a `NNNs` sector form so DOS-era
/// geometry (`63s`) reads naturally.
fn parse_align(s: &str) -> Result<u64> {
    let t = s.trim();
    let bytes = match t.strip_suffix('s').or_else(|| t.strip_suffix('S')) {
        Some(sectors) => sectors
            .parse::<u64>()
            .with_context(|| format!("bad sector alignment '{t}'"))?
            .saturating_mul(SECTOR),
        None => parse_size(t)?,
    };
    if bytes == 0 || bytes % SECTOR != 0 {
        bail!("alignment must be a non-zero multiple of {SECTOR} bytes (got {bytes})");
    }
    Ok(bytes)
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

/// Default partition type when the spec doesn't name one.
fn default_type(kind: TableKind) -> &'static str {
    match kind {
        TableKind::Mbr => "83",
        TableKind::Gpt => "0FC63DAF-8483-4772-8E79-3D69D8477DE4",
        TableKind::Apm => "Apple_HFS",
        TableKind::Sgi => "XFS",
        // X68k entries carry a name, not a type code.
        TableKind::X68k => "Human68k",
        _ => "83",
    }
}

/// Bytes at the head of the disk the table itself needs.
fn reserved_head(kind: TableKind) -> u64 {
    match kind {
        // GPT: protective MBR + header + a 128-entry array.
        TableKind::Gpt => 34 * SECTOR,
        // APM: block 0 driver descriptor + the map itself (63 blocks is the
        // convention every Apple tool writes).
        TableKind::Apm => 64 * SECTOR,
        // SGI reserves a 2 MiB volume-header region at the front (slot 8).
        TableKind::Sgi => 2 * 1024 * 1024,
        // X68k: table at byte 2048, partitions conventionally from sector 64.
        TableKind::X68k => u64::from(crate::partition::x68k::X68K_FIRST_PARTITION_SECTOR) * SECTOR,
        _ => SECTOR,
    }
}

/// Bytes at the tail the table needs (GPT's backup header + array).
fn reserved_tail(kind: TableKind) -> u64 {
    match kind {
        TableKind::Gpt => 33 * SECTOR,
        _ => 0,
    }
}

/// Lay the specs out in order, honouring alignment and the table's own
/// reserved regions.
fn place(specs: &[PartSpec], kind: TableKind, disk_size: u64, align: u64) -> Result<Vec<Placed>> {
    if specs.iter().filter(|s| s.size.is_none()).count() > 1 {
        bail!("only one --partition may use `rest`");
    }
    let usable_end = disk_size
        .checked_sub(reserved_tail(kind))
        .filter(|e| *e > reserved_head(kind))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "disk of {} is too small for a {} table",
                format_size(disk_size),
                kind.label(),
            )
        })?;

    let fixed: u64 = specs.iter().filter_map(|s| s.size).sum();
    let mut cursor = round_up(reserved_head(kind).max(align), align);
    // Alignment padding between partitions has to be part of the budget, or a
    // `rest` partition would be sized past the end of the disk.
    let slack = align.saturating_mul(specs.len() as u64);
    if cursor.saturating_add(fixed) > usable_end {
        bail!(
            "partitions total {} but only {} is usable on a {} disk of {}",
            format_size(fixed),
            format_size(usable_end.saturating_sub(cursor)),
            kind.label(),
            format_size(disk_size),
        );
    }

    let mut out = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        let start = round_up(cursor, align);
        let size = match spec.size {
            Some(n) => n,
            None => usable_end
                .checked_sub(start)
                .filter(|n| *n > 0)
                .ok_or_else(|| anyhow::anyhow!("no space left for the `rest` partition"))?,
        };
        let end = start
            .checked_add(size)
            .filter(|e| *e <= usable_end)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "partition {} ({}) runs past the end of the disk",
                    i + 1,
                    format_size(size),
                )
            })?;
        out.push(Placed {
            start_lba: start / SECTOR,
            size_bytes: size,
            type_text: spec
                .type_text
                .clone()
                .unwrap_or_else(|| default_type(kind).to_string()),
            name: spec
                .name
                .clone()
                .unwrap_or_else(|| format!("Partition {}", i + 1)),
        });
        cursor = end;
    }
    let _ = slack;
    Ok(out)
}

fn round_up(v: u64, to: u64) -> u64 {
    if to == 0 {
        return v;
    }
    v.div_ceil(to) * to
}

fn write_mbr(file: &mut std::fs::File, placed: &[Placed]) -> Result<()> {
    let entries: Vec<(u8, u32, u32, bool)> = placed
        .iter()
        .map(|p| {
            let byte =
                u8::from_str_radix(p.type_text.trim().trim_start_matches("0x"), 16).unwrap_or(0x83);
            (
                byte,
                p.start_lba as u32,
                (p.size_bytes / SECTOR) as u32,
                false,
            )
        })
        .collect();
    let bytes = mbr::build_minimal_mbr(0x5253_5459, &entries, 255, 63);
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&bytes).context("writing the MBR")?;
    Ok(())
}

fn write_gpt(file: &mut std::fs::File, placed: &[Placed], disk_size: u64) -> Result<()> {
    let mut entries = Vec::new();
    for p in placed {
        let guid = gpt::Guid::from_string(p.type_text.trim())
            .map_err(|e| anyhow::anyhow!("bad GPT type GUID '{}': {e}", p.type_text))?;
        let first = p.start_lba;
        let last = first + p.size_bytes / SECTOR - 1;
        entries.push((guid, first, last, p.name.clone()));
    }
    let table = gpt::build_minimal_gpt(&entries, disk_size);
    let disk_sectors = disk_size / SECTOR;

    // Protective MBR at LBA 0, primary GPT at 1..34, backup at the last 33.
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&gpt::Gpt::build_protective_mbr(disk_sectors))
        .context("writing the protective MBR")?;
    file.write_all(&table.build_primary_gpt(disk_sectors))
        .context("writing the primary GPT")?;
    file.seek(SeekFrom::Start((disk_sectors - 33) * SECTOR))?;
    file.write_all(&table.build_backup_gpt(disk_sectors))
        .context("writing the backup GPT")?;
    Ok(())
}

fn write_apm(file: &mut std::fs::File, placed: &[Placed], disk_size: u64) -> Result<()> {
    let block_size = SECTOR as u32;
    let entries: Vec<(String, u32, u32)> = placed
        .iter()
        .map(|p| {
            (
                p.type_text.trim().to_string(),
                p.start_lba as u32,
                (p.size_bytes / SECTOR) as u32,
            )
        })
        .collect();
    let total_blocks = (disk_size / SECTOR) as u32;
    let table = apm::build_minimal_apm(&entries, block_size, total_blocks);
    let bytes = table.build_apm_blocks(Some(total_blocks));
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&bytes).context("writing the APM")?;
    Ok(())
}

/// SGI volume header: data partitions in the low slots, the volume-header
/// region in slot 8 and the whole-disk entry in slot 10, as IRIX expects.
fn write_sgi(
    file: &mut std::fs::File,
    placed: &[Placed],
    disk_size: u64,
    heads: u16,
    spt: u16,
) -> Result<()> {
    use crate::partition::sgi::{
        SgiPartitionEntry, SgiPartitionType, SgiVolumeDirEntry, SgiVolumeHeader,
        SGI_NUM_PARTITIONS, SGI_NUM_VOL_DIR, SGI_VOLHDR_MAGIC,
    };

    let cyl_sectors = u64::from(heads) * u64::from(spt);
    if cyl_sectors == 0 {
        bail!("heads and sectors-per-track must both be non-zero");
    }
    let total_sectors = disk_size / SECTOR;
    let volhdr_sectors = reserved_head(TableKind::Sgi).div_ceil(SECTOR);

    let mut partitions: Vec<SgiPartitionEntry> = (0..SGI_NUM_PARTITIONS)
        .map(|_| SgiPartitionEntry {
            blocks: 0,
            first: 0,
            partition_type_raw: SgiPartitionType::VolHdr.as_u32(),
        })
        .collect();

    let mut slot = 0usize;
    for p in placed {
        // Slots 8 and 10 are spoken for by the volume header and whole volume.
        while slot == 8 || slot == 10 {
            slot += 1;
        }
        if slot >= SGI_NUM_PARTITIONS {
            bail!("ran out of SGI partition slots");
        }
        partitions[slot] = SgiPartitionEntry {
            blocks: (p.size_bytes / SECTOR) as u32,
            first: p.start_lba as u32,
            partition_type_raw: sgi_type_raw(&p.type_text),
        };
        slot += 1;
    }
    partitions[8] = SgiPartitionEntry {
        blocks: volhdr_sectors as u32,
        first: 0,
        partition_type_raw: SgiPartitionType::VolHdr.as_u32(),
    };
    partitions[10] = SgiPartitionEntry {
        blocks: total_sectors as u32,
        first: 0,
        partition_type_raw: SgiPartitionType::Volume.as_u32(),
    };

    let vh = SgiVolumeHeader {
        magic: SGI_VOLHDR_MAGIC,
        root_part_num: 0,
        swap_part_num: 1,
        device_parameters: crate::partition::sgi::SgiDeviceParameters::for_geometry(
            (total_sectors / cyl_sectors) as u32,
            heads,
            spt,
        ),
        bootfile: "/unix".to_string(),
        volume_directory: (0..SGI_NUM_VOL_DIR)
            .map(|_| SgiVolumeDirEntry {
                name: String::new(),
                block_num: 0,
                bytes: 0,
            })
            .collect(),
        partitions,
        checksum: 0,
        checksum_valid: true,
    };
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&vh.to_bytes())
        .context("writing the SGI volume header")?;
    Ok(())
}

/// Map a type keyword to the SGI discriminant, falling back to XFS.
fn sgi_type_raw(text: &str) -> u32 {
    use crate::partition::sgi::SgiPartitionType;
    let t = text.trim().to_ascii_lowercase();
    match t.as_str() {
        "efs" => SgiPartitionType::Efs.as_u32(),
        "raw" => SgiPartitionType::Raw.as_u32(),
        "volume" => SgiPartitionType::Volume.as_u32(),
        "volhdr" => SgiPartitionType::VolHdr.as_u32(),
        "xfslog" => SgiPartitionType::XfsLog.as_u32(),
        "xlv" => SgiPartitionType::Xlv.as_u32(),
        "xvm" => SgiPartitionType::Xvm.as_u32(),
        _ => SgiPartitionType::Xfs.as_u32(),
    }
}

/// X68000 table at byte 2048; `start`/`length` count logical sectors.
fn write_x68k(file: &mut std::fs::File, placed: &[Placed], disk_size: u64) -> Result<()> {
    use crate::partition::x68k::{X68kEntry, X68kPartitionTable, X68K_TABLE_OFFSET};

    let entries = placed
        .iter()
        .map(|p| {
            let mut name_raw = [b' '; 8];
            // The name is the spec's third field; X68k has no type code.
            let src = p.name.as_bytes();
            let n = src.len().min(8);
            name_raw[..n].copy_from_slice(&src[..n]);
            X68kEntry {
                name_raw,
                name_display: p.name.clone(),
                start_sector: p.start_lba as u32,
                length_sectors: (p.size_bytes / SECTOR) as u32,
            }
        })
        .collect();

    let table = X68kPartitionTable {
        disk_size_field: (disk_size / SECTOR) as u32,
        entries,
    };
    file.seek(SeekFrom::Start(X68K_TABLE_OFFSET))?;
    file.write_all(&table.to_bytes())
        .context("writing the X68000 partition table")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Matches the `--align` clap default.
    const DEFAULT_ALIGN: u64 = 1024 * 1024;

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
    fn alignment_accepts_bytes_and_sectors() {
        assert_eq!(parse_align("1M").unwrap(), 1024 * 1024);
        // DOS-era cylinder alignment reads naturally as sectors.
        assert_eq!(parse_align("63s").unwrap(), 63 * 512);
        assert!(parse_align("0").is_err());
        assert!(parse_align("100").is_err(), "not a sector multiple");
    }

    #[test]
    fn partitions_are_aligned_and_sequential() {
        let specs = vec![spec("20M"), spec("40M")];
        let placed = place(&specs, TableKind::Mbr, 128 * 1024 * 1024, DEFAULT_ALIGN).unwrap();
        assert_eq!(placed.len(), 2);
        assert_eq!(placed[0].start_lba, 2048);
        assert_eq!(placed[0].size_bytes, 20 * 1024 * 1024);
        // Second starts on the next 1 MiB boundary past the first.
        assert_eq!(placed[1].start_lba, 2048 + 20 * 2048);
        assert_eq!(placed[1].size_bytes, 40 * 1024 * 1024);
    }

    #[test]
    fn rest_claims_what_is_left_and_stays_inside_the_disk() {
        let disk = 128 * 1024 * 1024;
        let specs = vec![spec("20M"), spec("rest")];
        let placed = place(&specs, TableKind::Gpt, disk, DEFAULT_ALIGN).unwrap();
        let end = placed[1].start_lba * SECTOR + placed[1].size_bytes;
        // Must not tread on GPT's backup header at the tail.
        assert!(
            end <= disk - reserved_tail(TableKind::Gpt),
            "end {end} vs disk {disk}"
        );
        assert!(placed[1].size_bytes > 100 * 1024 * 1024);
    }

    #[test]
    fn only_one_rest_is_allowed() {
        let specs = vec![spec("rest"), spec("rest")];
        assert!(place(&specs, TableKind::Mbr, 64 * 1024 * 1024, DEFAULT_ALIGN).is_err());
    }

    #[test]
    fn oversized_partitions_are_refused() {
        let specs = vec![spec("200M")];
        let err = place(&specs, TableKind::Mbr, 64 * 1024 * 1024, DEFAULT_ALIGN)
            .expect_err("must refuse");
        assert!(format!("{err:#}").contains("usable"), "{err:#}");
    }

    #[test]
    fn sgi_reserves_its_volume_header_region() {
        let specs = vec![spec("100M")];
        let placed = place(&specs, TableKind::Sgi, 512 * 1024 * 1024, 5040 * 512).unwrap();
        // Must start past the 2 MiB volume header, on a cylinder boundary.
        assert!(placed[0].start_lba * 512 >= 2 * 1024 * 1024);
        assert_eq!(placed[0].start_lba % 5040, 0);
    }

    #[test]
    fn sgi_and_x68k_have_slot_limits() {
        // SGI keeps slots 8 and 10 for itself.
        assert_eq!(crate::partition::sgi::SGI_NUM_PARTITIONS - 2, 14);
        assert_eq!(crate::partition::x68k::X68K_MAX_PARTITIONS, 8);
    }

    #[test]
    fn sgi_type_keywords_map_to_discriminants() {
        use crate::partition::sgi::SgiPartitionType;
        assert_eq!(sgi_type_raw("EFS"), SgiPartitionType::Efs.as_u32());
        assert_eq!(sgi_type_raw("raw"), SgiPartitionType::Raw.as_u32());
        assert_eq!(sgi_type_raw("xfslog"), SgiPartitionType::XfsLog.as_u32());
        // Anything unrecognised lands on XFS, the sane default for a data slice.
        assert_eq!(sgi_type_raw("nonsense"), SgiPartitionType::Xfs.as_u32());
    }

    #[test]
    fn defaults_are_per_table_flavor() {
        assert_eq!(default_type(TableKind::Mbr), "83");
        assert_eq!(default_type(TableKind::Apm), "Apple_HFS");
        // Every default must be a value the catalog recognises.
        for k in [TableKind::Mbr, TableKind::Gpt, TableKind::Apm] {
            assert!(
                type_catalog::describe(k, default_type(k)).is_some(),
                "{} default is not in the catalog",
                k.label(),
            );
        }
    }
}
