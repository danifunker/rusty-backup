//! Lay out and write a fresh partition table on a blank disk.
//!
//! Shared by `rb-cli new hd {mbr|gpt|apm|sgi|x68k|rdb|sun|atari}`, the TUI's
//! New wizard and the GUI's Build Disk mode, so all three place partitions
//! identically and emit byte-for-byte the same tables. See
//! `docs/partition_table_writers_backlog.md` for how each writer is built.
//!
//! Sizes are laid out in order from `align` (1 MiB by default), each rounded up
//! to the alignment, past whatever head/tail region the table itself reserves.
//! A single `rest` size claims the remainder.
//!
//! The writers take any `Write + Seek`, so the same code fills an image file,
//! a raw device handle, or an in-memory buffer in tests.

#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
use anyhow::{bail, Context, Result};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::partition::type_catalog::{self, TableKind};
use crate::partition::{apm, format_size, gpt, mbr, parse_size, sgi_dklabel};

const SECTOR: u64 = 512;

/// One requested partition, before it is given a place on the disk.
#[derive(Debug, Clone, Default)]
pub struct PartSpec {
    /// `None` means "the rest of the disk".
    pub size: Option<u64>,
    pub type_text: Option<String>,
    pub name: Option<String>,
}

/// A partition once it has been given a place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    pub start_lba: u64,
    pub size_bytes: u64,
    pub type_text: String,
    pub name: String,
}

impl Placed {
    pub fn start_byte(&self) -> u64 {
        self.start_lba.saturating_mul(SECTOR)
    }

    pub fn end_byte(&self) -> u64 {
        self.start_byte().saturating_add(self.size_bytes)
    }
}

/// Disk geometry for the tables that lay partitions on cylinder boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    pub heads: u16,
    pub sectors_per_track: u16,
}

impl Default for Geometry {
    fn default() -> Self {
        Self {
            heads: crate::partition::sgi_hdd_builder::DEFAULT_HEADS,
            sectors_per_track: crate::partition::sgi_hdd_builder::DEFAULT_SECTORS_PER_TRACK,
        }
    }
}

impl Geometry {
    /// Bytes per cylinder, which is the alignment IRIX expects.
    pub fn cylinder_bytes(self) -> u64 {
        u64::from(self.heads) * u64::from(self.sectors_per_track) * SECTOR
    }
}

/// The tables this module can write, in the order the pickers list them.
pub const WRITABLE_TABLES: &[TableKind] = &[
    TableKind::Mbr,
    TableKind::Gpt,
    TableKind::Apm,
    TableKind::Sgi,
    TableKind::SgiDkLabel,
    TableKind::X68k,
    TableKind::Rdb,
    TableKind::Sun,
    TableKind::Next,
    TableKind::SolarisX86,
    TableKind::Atari,
];

/// A `GEM` partition cannot exceed 16 MiB; past that TOS wants `BGM`.
const AHDI_GEM_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// The slice a Sun label reserves for the whole-disk "backup" alias, which is
/// conventionally slice 2 and overlaps every real slice.
const SUN_BACKUP_SLICE: usize = 2;

/// `d_secsize` every NeXTSTEP label we have counts its partitions in.
const NEXT_SECTOR_SIZE: u64 = 1024;

/// `d_front` — NeXT sectors reserved ahead of partition `a`, which is where
/// the four label copies and the two boot blocks live.
const NEXT_FRONT_PORCH: u64 = 160;

/// Cylinders a Solaris x86 disk keeps ahead of the first user slice: the MBR's
/// own cylinder, then the boot (`s8`) and alternates (`s9`) slices inside the
/// Solaris partition. See [`write_solaris_x86`].
const SOLARIS_HEAD_CYLINDERS: u64 = 4;

/// Cylinder the Solaris MBR partition itself starts at.
const SOLARIS_PART_START_CYLINDER: u64 = 1;

/// Cylinders `format(1M)` leaves past `dkl_ncyl` for alternate-sector remaps.
const SOLARIS_ALT_CYLINDERS: u64 = 2;

/// How many partitions the table can hold, or `None` when it is unbounded in
/// any practical sense.
pub fn slot_limit(kind: TableKind) -> Option<usize> {
    match kind {
        TableKind::Mbr => Some(4),
        TableKind::X68k => Some(crate::partition::x68k::X68K_MAX_PARTITIONS),
        // SGI has 16 slots but reserves 8 (volhdr) and 10 (whole volume).
        TableKind::Sgi => Some(crate::partition::sgi::SGI_NUM_PARTITIONS - 2),
        // Eight `d_map` slots; the last is the whole-disk wrapper the era's
        // labels always carry, so it is not offered.
        TableKind::SgiDkLabel => Some(sgi_dklabel::SGI_DKLABEL_NFS - 1),
        // RDB is a linked list, but keeping every PART block inside the first
        // 16 sectors is what every Amiga tool scans for.
        TableKind::Rdb => Some(crate::partition::rdb::RDB_SCAN_BLOCKS as usize - 1),
        // Sun has 8 slices; slice 2 is the whole-disk backup alias.
        TableKind::Sun => Some(7),
        TableKind::Next => Some(crate::partition::next::N_PARTITIONS),
        // 16 slices less the backup / boot / alternates trio the label owns.
        TableKind::SolarisX86 => Some(solaris_user_slices().count()),
        // Four primary slots. XGM extended chains parse, but we don't create
        // them — see docs/partition_table_writers_backlog.md.
        TableKind::Atari => Some(crate::partition::atari::AHDI_NUM_SLOTS),
        _ => None,
    }
}

/// Default partition type when a spec doesn't name one.
pub fn default_type(kind: TableKind) -> &'static str {
    match kind {
        TableKind::Mbr => "83",
        TableKind::Gpt => "0FC63DAF-8483-4772-8E79-3D69D8477DE4",
        TableKind::Apm => "Apple_HFS",
        TableKind::Sgi => "XFS",
        TableKind::SgiDkLabel => "root",
        // X68k entries carry a name, not a type code.
        TableKind::X68k => "Human68k",
        TableKind::Rdb => "DOS\\3",
        TableKind::Sun | TableKind::SolarisX86 => "root",
        TableKind::Next => "4.3BSD",
        TableKind::Atari => "GEM",
        _ => "83",
    }
}

/// Default entry name when a spec doesn't give one. RDB names are AmigaDOS
/// device names, so they get the conventional `DH0`, `DH1`, ... instead.
pub fn default_name(kind: TableKind, index: usize) -> String {
    match kind {
        TableKind::Rdb => format!("DH{index}"),
        // A NeXT entry's name is its `p_mountpt`, and both reference disks
        // leave that empty, so an invented one would be worse than none.
        TableKind::Next => String::new(),
        _ => format!("Partition {}", index + 1),
    }
}

/// Adjust a requested type to what the table can express at this size — only
/// AHDI needs it. Applied in [`place`] so the log, the picker and the writer
/// cannot disagree about the effective type.
fn effective_type(kind: TableKind, text: String, size_bytes: u64) -> String {
    match kind {
        TableKind::Atari if size_bytes > AHDI_GEM_MAX_BYTES && text.eq_ignore_ascii_case("GEM") => {
            "BGM".to_string()
        }
        _ => text,
    }
}

/// Slice numbers a Sun label offers to user partitions, in order: everything
/// but the whole-disk backup alias.
fn sun_user_slices() -> impl Iterator<Item = usize> {
    (0..8).filter(|&i| i != SUN_BACKUP_SLICE)
}

/// Slice numbers a Solaris x86 VTOC offers to user slices: all 16 less the
/// whole-partition backup alias and the boot / alternates pair.
fn solaris_user_slices() -> impl Iterator<Item = usize> {
    (0..crate::partition::solaris_x86::N_SLICES).filter(|&i| !matches!(i, 2 | 8 | 9))
}

/// Bytes at the head of the disk the table itself needs. `geometry` matters
/// only where the reserve is counted in cylinders (Solaris x86).
pub fn reserved_head(kind: TableKind, geometry: Geometry) -> u64 {
    match kind {
        // GPT: protective MBR + header + a 128-entry array.
        TableKind::Gpt => 34 * SECTOR,
        // APM: block 0 driver descriptor + the map itself (63 blocks is the
        // convention every Apple tool writes).
        TableKind::Apm => 64 * SECTOR,
        // SGI reserves a 2 MiB volume-header region at the front (slot 8).
        TableKind::Sgi => 2 * 1024 * 1024,
        // Block 0 is the disk label and blocks 1-4 the bad-block map;
        // cylinder alignment then pushes slot 0 out to cylinder 1.
        TableKind::SgiDkLabel => sgi_dklabel::SGI_DKLABEL_RESERVED_BLOCKS * SECTOR,
        // X68k: table at byte 2048, partitions conventionally from sector 64.
        TableKind::X68k => u64::from(crate::partition::x68k::X68K_FIRST_PARTITION_SECTOR) * SECTOR,
        // RDB: the RDSK plus its PART chain. Cylinder alignment then pushes
        // the first partition to cylinder 1, where Amiga tools put it.
        TableKind::Rdb => crate::partition::rdb::RDB_SCAN_BLOCKS * SECTOR,
        // Sun keeps only the 512-byte label at sector 0, but slices start on
        // cylinder boundaries, so alignment pushes slice 0 out to cylinder 1.
        TableKind::Sun => SECTOR,
        // NeXT counts partitions from the end of the front porch, which is
        // where the four label copies and the boot blocks live.
        TableKind::Next => NEXT_FRONT_PORCH * NEXT_SECTOR_SIZE,
        // MBR cylinder, then the Solaris partition's boot and alternates
        // slices; the VTOC lives in sector 1 of the first of those.
        TableKind::SolarisX86 => SOLARIS_HEAD_CYLINDERS * geometry.cylinder_bytes().max(SECTOR),
        // AHDI's root sector is sector 0; TOS tools conventionally leave
        // sector 1 free too.
        TableKind::Atari => 2 * SECTOR,
        _ => SECTOR,
    }
}

/// Bytes at the tail the table needs: GPT's backup header + array, and the
/// alternate cylinders a Solaris x86 label keeps past `dkl_ncyl`.
pub fn reserved_tail(kind: TableKind, geometry: Geometry) -> u64 {
    match kind {
        TableKind::Gpt => 33 * SECTOR,
        TableKind::SolarisX86 => SOLARIS_ALT_CYLINDERS * geometry.cylinder_bytes().max(SECTOR),
        _ => 0,
    }
}

/// True for the tables that place partitions on cylinder boundaries, which is
/// what makes the cylinder size their default alignment.
pub fn uses_cylinder_geometry(kind: TableKind) -> bool {
    matches!(
        kind,
        TableKind::Sgi
            | TableKind::SgiDkLabel
            | TableKind::Rdb
            | TableKind::Sun
            | TableKind::SolarisX86
    )
}

/// True for the tables that need a heads / sectors-per-track control, which is
/// every cylinder-aligned one plus NeXT — its label records the geometry even
/// though it cuts partitions on its own 1024-byte sectors instead.
pub fn records_geometry(kind: TableKind) -> bool {
    uses_cylinder_geometry(kind) || kind == TableKind::Next
}

/// True for the tables whose entries carry a name the user can set — GPT and
/// APM labels, X68000's 8-byte name, and RDB's AmigaDOS device name.
pub fn carries_entry_name(kind: TableKind) -> bool {
    matches!(
        kind,
        TableKind::Gpt | TableKind::Apm | TableKind::X68k | TableKind::Rdb | TableKind::Next
    )
}

/// The alignment to lay partitions on when the caller has no preference.
/// The cylinder-based tables want one cylinder; everything else gets 1 MiB.
pub fn default_align(kind: TableKind, geometry: Geometry) -> u64 {
    if uses_cylinder_geometry(kind) {
        geometry.cylinder_bytes().max(SECTOR)
    } else {
        1024 * 1024
    }
}

/// Granularity a partition's *size* must land on, not just its start: the
/// cylinder-based tables cannot express a part-cylinder size.
fn size_granularity(kind: TableKind, align: u64) -> u64 {
    match kind {
        TableKind::Rdb | TableKind::Sun | TableKind::SolarisX86 | TableKind::SgiDkLabel => {
            align.max(SECTOR)
        }
        // `p_size` counts NeXT sectors, so a part-sector size cannot be said.
        TableKind::Next => NEXT_SECTOR_SIZE,
        _ => SECTOR,
    }
}

/// Alignment as a byte count, accepting a `NNNs` sector form so DOS-era
/// geometry (`63s`) reads naturally.
pub fn parse_align(s: &str) -> Result<u64> {
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

/// Lay the specs out in order, honouring alignment and the table's own
/// reserved regions.
pub fn place(
    specs: &[PartSpec],
    kind: TableKind,
    disk_size: u64,
    align: u64,
    geometry: Geometry,
) -> Result<Vec<Placed>> {
    let head = reserved_head(kind, geometry);
    if let Some(limit) = slot_limit(kind) {
        if specs.len() > limit {
            bail!(
                "{} holds at most {} partitions; {} were given",
                kind.label(),
                limit,
                specs.len(),
            );
        }
    }
    if specs.iter().filter(|s| s.size.is_none()).count() > 1 {
        bail!("only one partition may use `rest`");
    }
    let usable_end = disk_size
        .checked_sub(reserved_tail(kind, geometry))
        .filter(|e| *e > head)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "disk of {} is too small for a {} table",
                format_size(disk_size),
                kind.label(),
            )
        })?;

    let gran = size_granularity(kind, align);
    let fixed: u64 = specs
        .iter()
        .filter_map(|s| s.size)
        .map(|n| round_up(n, gran))
        .sum();
    let mut cursor = round_up(head.max(align), align);
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
            Some(n) => {
                // Check what was asked for, not what it rounds to, so a
                // sub-sector request is refused instead of becoming a sector.
                if n < SECTOR {
                    bail!("partition {} is smaller than one sector", i + 1);
                }
                round_up(n, gran)
            }
            // `rest` rounds down instead: rounding it up would run past the
            // end of the disk it is defined as filling.
            None => usable_end
                .checked_sub(start)
                .map(|n| (n / gran) * gran)
                .filter(|n| *n >= SECTOR)
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
            size_bytes: (size / SECTOR) * SECTOR,
            type_text: effective_type(
                kind,
                spec.type_text
                    .clone()
                    .filter(|t| !t.trim().is_empty())
                    .unwrap_or_else(|| default_type(kind).to_string()),
                size,
            ),
            name: spec
                .name
                .clone()
                .filter(|n| !n.trim().is_empty())
                .unwrap_or_else(|| default_name(kind, i)),
        });
        cursor = end;
    }
    Ok(out)
}

fn round_up(v: u64, to: u64) -> u64 {
    if to == 0 {
        return v;
    }
    v.div_ceil(to) * to
}

/// Write `kind`'s table describing `placed` onto a `disk_size`-byte disk.
///
/// `geometry` matters only to the cylinder-based tables (see
/// [`uses_cylinder_geometry`]); the rest ignore it.
pub fn write_table<W: Write + Seek>(
    out: &mut W,
    kind: TableKind,
    placed: &[Placed],
    disk_size: u64,
    geometry: Geometry,
) -> Result<()> {
    match kind {
        TableKind::Mbr => write_mbr(out, placed),
        TableKind::Gpt => write_gpt(out, placed, disk_size),
        TableKind::Apm => write_apm(out, placed, disk_size),
        TableKind::X68k => write_x68k(out, placed, disk_size),
        TableKind::Sgi => write_sgi(out, placed, disk_size, geometry),
        TableKind::SgiDkLabel => write_sgi_dklabel(out, placed, disk_size, geometry),
        TableKind::Rdb => write_rdb(out, placed, disk_size, geometry),
        TableKind::Sun => write_sun(out, placed, disk_size, geometry),
        TableKind::Next => write_next(out, placed, disk_size, geometry),
        TableKind::SolarisX86 => write_solaris_x86(out, placed, disk_size, geometry),
        TableKind::Atari => write_ahdi(out, placed, disk_size),
        _ => bail!("no table writer for {}", kind.label()),
    }
}

fn write_mbr<W: Write + Seek>(out: &mut W, placed: &[Placed]) -> Result<()> {
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
    out.seek(SeekFrom::Start(0))?;
    out.write_all(&bytes).context("writing the MBR")?;
    Ok(())
}

fn write_gpt<W: Write + Seek>(out: &mut W, placed: &[Placed], disk_size: u64) -> Result<()> {
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
    out.seek(SeekFrom::Start(0))?;
    out.write_all(&gpt::Gpt::build_protective_mbr(disk_sectors))
        .context("writing the protective MBR")?;
    out.write_all(&table.build_primary_gpt(disk_sectors))
        .context("writing the primary GPT")?;
    out.seek(SeekFrom::Start((disk_sectors - 33) * SECTOR))?;
    out.write_all(&table.build_backup_gpt(disk_sectors))
        .context("writing the backup GPT")?;
    Ok(())
}

fn write_apm<W: Write + Seek>(out: &mut W, placed: &[Placed], disk_size: u64) -> Result<()> {
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
    let mut table = apm::build_minimal_apm(&entries, block_size, total_blocks);
    // `build_minimal_apm` names entries "Partition N"; entry 0 is its own
    // self-referencing map, so user entries start at 1.
    for (slot, p) in table.entries.iter_mut().skip(1).zip(placed.iter()) {
        if !p.name.trim().is_empty() {
            slot.name = p.name.trim().to_string();
        }
    }
    let bytes = table.build_apm_blocks(Some(total_blocks));
    out.seek(SeekFrom::Start(0))?;
    out.write_all(&bytes).context("writing the APM")?;
    Ok(())
}

/// SGI volume header: data partitions in the low slots, the volume-header
/// region in slot 8 and the whole-disk entry in slot 10, as IRIX expects.
fn write_sgi<W: Write + Seek>(
    out: &mut W,
    placed: &[Placed],
    disk_size: u64,
    geometry: Geometry,
) -> Result<()> {
    use crate::partition::sgi::{
        SgiPartitionEntry, SgiPartitionType, SgiVolumeDirEntry, SgiVolumeHeader,
        SGI_NUM_PARTITIONS, SGI_NUM_VOL_DIR, SGI_VOLHDR_MAGIC,
    };

    let cyl_sectors = u64::from(geometry.heads) * u64::from(geometry.sectors_per_track);
    if cyl_sectors == 0 {
        bail!("heads and sectors-per-track must both be non-zero");
    }
    let total_sectors = disk_size / SECTOR;
    let volhdr_sectors = reserved_head(TableKind::Sgi, geometry).div_ceil(SECTOR);

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
            geometry.heads,
            geometry.sectors_per_track,
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
    out.seek(SeekFrom::Start(0))?;
    out.write_all(&vh.to_bytes())
        .context("writing the SGI volume header")?;
    Ok(())
}

/// Map a type keyword to the SGI discriminant, falling back to XFS.
pub fn sgi_type_raw(text: &str) -> u32 {
    use crate::partition::sgi::SgiPartitionType;
    sgi_type_from_text(text).unwrap_or_else(|| SgiPartitionType::Xfs.as_u32())
}

/// Resolve an SGI slot type — a keyword as `partmap types --table sgi` lists
/// it, or a raw decimal / `0x` discriminant. `None` when it is neither, which
/// is what lets a caller decide its own default.
pub fn sgi_type_from_text(text: &str) -> Option<u32> {
    use crate::partition::sgi::SgiPartitionType;
    let trimmed = text.trim();
    let named = match trimmed.to_ascii_lowercase().as_str() {
        "volhdr" => Some(SgiPartitionType::VolHdr),
        "trkrepl" => Some(SgiPartitionType::TrkRepl),
        "secrepl" => Some(SgiPartitionType::SecRepl),
        "raw" => Some(SgiPartitionType::Raw),
        "bsd" => Some(SgiPartitionType::Bsd),
        "sysv" => Some(SgiPartitionType::SysV),
        "volume" => Some(SgiPartitionType::Volume),
        "efs" => Some(SgiPartitionType::Efs),
        "lvol" => Some(SgiPartitionType::LVol),
        "rlvol" => Some(SgiPartitionType::RLVol),
        "xfs" => Some(SgiPartitionType::Xfs),
        "xfslog" => Some(SgiPartitionType::XfsLog),
        "xlv" => Some(SgiPartitionType::Xlv),
        "xvm" => Some(SgiPartitionType::Xvm),
        _ => None,
    };
    if let Some(t) = named {
        return Some(t.as_u32());
    }
    match trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        Some(hex) => u32::from_str_radix(hex, 16).ok(),
        None => trimmed.parse::<u32>().ok(),
    }
}

/// Amiga RDB: an `RDSK` at sector 0 plus a `PART` chain. Big-endian, bounds in
/// cylinders, every block's longs summing to zero; fields per `amitools`.
fn write_rdb<W: Write + Seek>(
    out: &mut W,
    placed: &[Placed],
    disk_size: u64,
    geometry: Geometry,
) -> Result<()> {
    use crate::partition::rdb::{parse_dos_type, stamp_checksum, NO_BLOCK, RDB_SCAN_BLOCKS};

    let heads = u64::from(geometry.heads);
    let sectors = u64::from(geometry.sectors_per_track);
    let cyl_blks = heads * sectors;
    if cyl_blks == 0 {
        bail!("heads and sectors-per-track must both be non-zero");
    }
    let cylinders = disk_size / SECTOR / cyl_blks;
    if cylinders < 2 {
        bail!(
            "an RDB disk needs at least two cylinders ({} each); {} is too small",
            format_size(cyl_blks * SECTOR),
            format_size(disk_size),
        );
    }

    let mut rdsk = [0u8; 512];
    rdsk[0..4].copy_from_slice(crate::partition::rdb::RDSK_SIGNATURE);
    let part_list = if placed.is_empty() { NO_BLOCK } else { 1 };
    for (long, value) in [
        (1u32, 64u32),      // rdb_SummedLongs
        (3, 7),             // rdb_HostID
        (4, SECTOR as u32), // rdb_BlockBytes
        (5, 0x17),          // last disk/lun/tid + disk ID valid
        (6, NO_BLOCK),      // rdb_BadBlockList
        (7, part_list),     // rdb_PartitionList
        (8, NO_BLOCK),      // rdb_FileSysHeaderList
        (9, NO_BLOCK),      // rdb_DriveInit
        (16, cylinders as u32),
        (17, sectors as u32),
        (18, heads as u32),
        (19, 1),                      // interleave
        (20, cylinders as u32),       // parking zone
        (24, cylinders as u32),       // write pre-comp
        (25, cylinders as u32),       // reduced write
        (26, 3),                      // step rate
        (32, 0),                      // rdb_RDBBlocksLo
        (33, (cyl_blks - 1) as u32),  // rdb_RDBBlocksHi: cylinder 0 is ours
        (34, 1),                      // rdb_LoCylinder
        (35, (cylinders - 1) as u32), // rdb_HiCylinder
        (36, cyl_blks as u32),
        (38, placed.len() as u32), // rdb_HighRDSKBlock
    ] {
        put_long(&mut rdsk, long as usize, value);
    }
    put_cstr(&mut rdsk, 40, 8, "RUSTYBK");
    put_cstr(&mut rdsk, 42, 16, "RB IMAGE");
    put_cstr(&mut rdsk, 46, 4, "0001");
    stamp_checksum(&mut rdsk);
    out.seek(SeekFrom::Start(0))?;
    out.write_all(&rdsk).context("writing the RDSK block")?;

    for (i, p) in placed.iter().enumerate() {
        let block = 1 + i as u32;
        if u64::from(block) >= RDB_SCAN_BLOCKS {
            bail!("more RDB partitions than fit in the first {RDB_SCAN_BLOCKS} sectors");
        }
        let low_cyl = p.start_lba / cyl_blks;
        let high_cyl = (p.start_lba + p.size_bytes / SECTOR) / cyl_blks - 1;
        let dos_type = parse_dos_type(&p.type_text)
            .ok_or_else(|| anyhow::anyhow!("bad Amiga DosType '{}'", p.type_text))?;

        let mut part = [0u8; 512];
        part[0..4].copy_from_slice(crate::partition::rdb::PART_SIGNATURE);
        let next = if i + 1 < placed.len() {
            block + 1
        } else {
            NO_BLOCK
        };
        for (long, value) in [
            (1u32, 64u32), // pb_SummedLongs
            (3, 7),        // pb_HostID
            (4, next),
            (32, 16),  // de_TableSize: the classic 16-long DosEnvec
            (33, 128), // de_SizeBlock, in longs
            (35, heads as u32),
            (36, 1), // de_SectorPerBlock
            (37, sectors as u32),
            (38, 2),           // de_Reserved: the two AmigaDOS boot blocks
            (43, 30),          // de_NumBuffers
            (45, 0x00FF_FFFF), // de_MaxTransfer
            (46, 0x7FFF_FFFE), // de_Mask
            (41, low_cyl as u32),
            (42, high_cyl as u32),
            (48, dos_type),
        ] {
            put_long(&mut part, long as usize, value);
        }
        put_bstr(&mut part, 9, 31, &p.name);
        stamp_checksum(&mut part);
        out.seek(SeekFrom::Start(u64::from(block) * SECTOR))?;
        out.write_all(&part)
            .with_context(|| format!("writing PART block {block}"))?;
    }
    Ok(())
}

/// Atari AHDI: four 12-byte entries at 0x1C6 in a root sector whose word-sum
/// must be 0x1234. `AhdiTable::root_to_bytes` serialises; this fills slots.
fn write_ahdi<W: Write + Seek>(out: &mut W, placed: &[Placed], disk_size: u64) -> Result<()> {
    use crate::partition::atari::{
        AhdiPartitionEntry, AhdiPartitionKind, AhdiTable, AHDI_NUM_SLOTS,
    };

    let empty = || AhdiPartitionEntry {
        flags: 0,
        kind: AhdiPartitionKind::Other([0, 0, 0]),
        start_sector: 0,
        sector_count: 0,
        is_logical: false,
    };
    let mut primary: [AhdiPartitionEntry; AHDI_NUM_SLOTS] = std::array::from_fn(|_| empty());

    for (slot, p) in primary.iter_mut().zip(placed.iter()) {
        let tag = p.type_text.trim().to_ascii_uppercase();
        let id: [u8; 3] = tag
            .as_bytes()
            .try_into()
            .map_err(|_| anyhow::anyhow!("AHDI type tags are exactly 3 characters: '{tag}'"))?;
        let kind = AhdiPartitionKind::from_bytes(id);
        if !kind.is_recognized_or_printable() {
            bail!("AHDI type tag '{tag}' is not upper-case alphanumeric");
        }
        *slot = AhdiPartitionEntry {
            flags: 0x01, // bit 0 = exists
            kind,
            start_sector: p.start_lba as u32,
            sector_count: (p.size_bytes / SECTOR) as u32,
            is_logical: false,
        };
    }

    let table = AhdiTable {
        primary,
        logical: Vec::new(),
        disk_size_sectors: (disk_size / SECTOR) as u32,
        bad_sector_list_start: 0,
        checksum: 0,
        checksum_valid: true,
    };
    out.seek(SeekFrom::Start(0))?;
    out.write_all(&table.root_to_bytes())
        .context("writing the AHDI root sector")?;
    Ok(())
}

/// One `struct disk_label` at block 0. Slots carry a role, not a type, so
/// `type_text` is a keyword; see `docs/partition_table_writers_backlog.md`.
fn write_sgi_dklabel<W: Write + Seek>(
    out: &mut W,
    placed: &[Placed],
    disk_size: u64,
    geometry: Geometry,
) -> Result<()> {
    use crate::partition::sgi_dklabel::{
        SgiDiskLabel, SgiDiskMap, SgiLabelByteOrder, SGI_DKLABEL_NFS,
    };

    let spc = u64::from(geometry.heads) * u64::from(geometry.sectors_per_track);
    if spc == 0 {
        bail!("heads and sectors-per-track must both be non-zero");
    }
    let ncyl = disk_size / SECTOR / spc;
    if ncyl < 2 {
        bail!(
            "an SGI disk label needs at least two cylinders ({} each); {} is too small",
            format_size(spc * SECTOR),
            format_size(disk_size),
        );
    }
    if ncyl > u64::from(u16::MAX) {
        bail!(
            "the label counts cylinders in 16 bits; {ncyl} is too many - raise --heads/--sectors"
        );
    }
    let total_blocks = ncyl * spc;

    let mut map = vec![SgiDiskMap { base: 0, size: 0 }; SGI_DKLABEL_NFS];
    let mut boot: Option<u8> = None;
    let mut root: Option<u8> = None;
    let mut swap: Option<u8> = None;
    for (i, p) in placed.iter().enumerate() {
        let end = p.start_lba + p.size_bytes / SECTOR;
        if end > total_blocks {
            bail!(
                "partition {} ends at block {end}, past the {total_blocks} blocks the \
                 {ncyl}-cylinder geometry describes",
                i + 1,
            );
        }
        map[i] = SgiDiskMap {
            base: p.start_lba as u32,
            size: (p.size_bytes / SECTOR) as u32,
        };
        let slot = i as u8;
        match p.type_text.trim().to_ascii_lowercase().as_str() {
            "swap" => swap.get_or_insert(slot),
            "boot" => boot.get_or_insert(slot),
            "root" => root.get_or_insert(slot),
            "slice" | "" => continue,
            other => {
                bail!("unknown SGI disk-label slot role '{other}' - use root, swap, boot or slice")
            }
        };
    }
    // The wrapper slot spans the whole drive and deliberately overlaps the
    // real ones; `is_wrapper_slot` filters it back out on read.
    map[SGI_DKLABEL_NFS - 1] = SgiDiskMap {
        base: 0,
        size: total_blocks as u32,
    };

    let bootfs = boot.or(root).unwrap_or(0);
    let rootfs = root.unwrap_or(bootfs);
    let label = SgiDiskLabel {
        // Written the way the 68020 sees it; `rb-cli swab16` produces the
        // controller's reversed-word order when a period machine needs it.
        byte_order: SgiLabelByteOrder::Native,
        drive_type: 1, // DT_V170, the pairing on the reference IRIS 3130
        controller: 0, // DC_DSD5217
        cylinders: ncyl as u16,
        heads: geometry.heads,
        sectors: geometry.sectors_per_track,
        // No sparing on a synthetic image, so the alternates region is empty
        // and starts where the geometry ends.
        altstart: total_blocks as u32,
        nalternates: 0,
        bootfs,
        // Past `d_map`, so no slot reads back as swap when none was asked for.
        swapfs: swap.unwrap_or(SGI_DKLABEL_NFS as u8),
        map,
        interleave: 1,
        trackskew: 0,
        cylskew: 0,
        badspots: 0,
        name: "rusty-backup".to_string(),
        serial: "0000".to_string(),
        rootnotboot: u8::from(rootfs != bootfs),
        rootfs,
    };

    let mut sector = [0u8; SECTOR as usize];
    label.write_into(&mut sector)?;
    out.seek(SeekFrom::Start(0))?;
    out.write_all(&sector)
        .context("writing the SGI disk label")?;
    Ok(())
}

/// Sun SMI VTOC: one 512-byte label of geometry, tags and 8 cylinder-based
/// slices, laid out per the kernel's `struct sun_disklabel`. Slice 2 is the
/// whole-disk alias, so user partitions fill the other seven.
fn write_sun<W: Write + Seek>(
    out: &mut W,
    placed: &[Placed],
    disk_size: u64,
    geometry: Geometry,
) -> Result<()> {
    use crate::partition::sun::{
        tag_from_text, SUN_LABEL_MAGIC, SUN_TAG_WHOLE_DISK, SUN_VTOC_SANITY,
    };

    let heads = u64::from(geometry.heads);
    let sectors = u64::from(geometry.sectors_per_track);
    let spc = heads * sectors;
    if spc == 0 {
        bail!("heads and sectors-per-track must both be non-zero");
    }
    let ncyl = disk_size / SECTOR / spc;
    if ncyl < 2 {
        bail!(
            "a Sun disk needs at least two cylinders ({} each); {} is too small",
            format_size(spc * SECTOR),
            format_size(disk_size),
        );
    }
    if ncyl > u64::from(u16::MAX) {
        bail!(
            "Sun labels count cylinders in 16 bits; {ncyl} is too many — raise --heads/--sectors"
        );
    }

    let mut label = [0u8; 512];
    // The `info` text is free-form, but every Sun tool writes the geometry
    // into it and `fdisk` shows it verbatim.
    let info = format!("rusty-backup cyl {ncyl} alt 0 hd {heads} sec {sectors}");
    let n = info.len().min(128);
    label[..n].copy_from_slice(&info.as_bytes()[..n]);

    let put16 = |b: &mut [u8; 512], off: usize, v: u16| {
        b[off..off + 2].copy_from_slice(&v.to_be_bytes());
    };
    let put32 = |b: &mut [u8; 512], off: usize, v: u32| {
        b[off..off + 4].copy_from_slice(&v.to_be_bytes());
    };

    put32(&mut label, 128, 1); // vtoc.version
    put16(&mut label, 140, 8); // vtoc.nparts
    put32(&mut label, 188, SUN_VTOC_SANITY);
    put16(&mut label, 420, 5400); // rspeed
    put16(&mut label, 422, ncyl as u16); // pcylcount
    put16(&mut label, 430, 1); // ilfact
    put16(&mut label, 432, ncyl as u16);
    put16(&mut label, 436, heads as u16); // ntrks
    put16(&mut label, 438, sectors as u16); // nsect

    // Slice 2 spans the whole disk and deliberately overlaps the real slices.
    put16(&mut label, 142 + SUN_BACKUP_SLICE * 4, SUN_TAG_WHOLE_DISK);
    put32(&mut label, 444 + SUN_BACKUP_SLICE * 8, 0);
    put32(&mut label, 448 + SUN_BACKUP_SLICE * 8, (ncyl * spc) as u32);

    for (p, slice) in placed.iter().zip(sun_user_slices()) {
        let tag = tag_from_text(&p.type_text)
            .ok_or_else(|| anyhow::anyhow!("bad Sun slice tag '{}'", p.type_text))?;
        put16(&mut label, 142 + slice * 4, tag);
        put32(&mut label, 444 + slice * 8, (p.start_lba / spc) as u32);
        put32(&mut label, 448 + slice * 8, (p.size_bytes / SECTOR) as u32);
    }

    put16(&mut label, 508, SUN_LABEL_MAGIC);
    // The label's checksum is a 16-bit XOR over all 256 big-endian words that
    // has to come out zero, so the stored word is the XOR of the other 255.
    let mut csum = 0u16;
    for w in label[..510].chunks_exact(2) {
        csum ^= u16::from_be_bytes([w[0], w[1]]);
    }
    put16(&mut label, 510, csum);

    out.seek(SeekFrom::Start(0))?;
    out.write_all(&label)
        .context("writing the Sun disk label")?;
    Ok(())
}

/// NeXT disk label: four checksummed copies at 512-byte blocks 0/15/30/45,
/// each describing up to 8 partitions counted in the label's own 1024-byte
/// sectors and measured from the end of the front porch.
fn write_next<W: Write + Seek>(
    out: &mut W,
    placed: &[Placed],
    disk_size: u64,
    geometry: Geometry,
) -> Result<()> {
    use crate::partition::next::{
        build_label, write_copies, NextLabelSpec, NextPartitionSpec, LABEL_BLOCKS, N_PARTITIONS,
    };

    let secsize = NEXT_SECTOR_SIZE;
    let front = NEXT_FRONT_PORCH;
    let per_cylinder = u64::from(geometry.heads) * u64::from(geometry.sectors_per_track);
    if per_cylinder == 0 {
        bail!("heads and sectors-per-track must both be non-zero");
    }

    let mut slots: Vec<Option<NextPartitionSpec>> = vec![None; N_PARTITIONS];
    for (i, p) in placed.iter().enumerate() {
        let start = p.start_byte();
        if !start.is_multiple_of(secsize) || !p.size_bytes.is_multiple_of(secsize) {
            bail!(
                "NeXT counts partitions in {secsize}-byte sectors; partition {} at {} for {} is \
                 not a whole number of them (try --align 1M)",
                i + 1,
                format_size(start),
                format_size(p.size_bytes),
            );
        }
        let base = start / secsize;
        if base < front {
            bail!(
                "partition {} starts at {} , inside the {} front porch the label copies live in",
                i + 1,
                format_size(start),
                format_size(front * secsize),
            );
        }
        slots[i] = Some(NextPartitionSpec {
            base: (base - front) as i32,
            size: (p.size_bytes / secsize) as i32,
            mount_point: p.name.clone(),
            fs_type: p.type_text.trim().to_string(),
            ..Default::default()
        });
    }

    // `d_ntracks` / `d_nsectors` count the label's own sectors, which is why
    // `new hd next` documents --sectors in 1024-byte units.
    let spec = NextLabelSpec {
        ntracks: u32::from(geometry.heads),
        nsectors: u32::from(geometry.sectors_per_track),
        ncylinders: (disk_size / secsize / per_cylinder) as u32,
        sector_size: secsize as u32,
        front_porch: front as u16,
        partitions: slots,
        ..Default::default()
    };

    let label = build_label(&spec);
    write_copies(out, &label, &LABEL_BLOCKS).context("writing the NeXT disk label")?;
    Ok(())
}

/// Solaris x86: an MBR whose one `0x82` entry holds a 16-slice VTOC in its
/// sector 1. Cylinder 0 of the disk is the MBR's; the partition then keeps its
/// own cylinder 0 for the boot slice and cylinders 1-2 for alternates, which
/// is the layout `format(1M)` lays down and what [`SOLARIS_HEAD_CYLINDERS`]
/// reserves.
fn write_solaris_x86<W: Write + Seek>(
    out: &mut W,
    placed: &[Placed],
    disk_size: u64,
    geometry: Geometry,
) -> Result<()> {
    use crate::partition::solaris_x86::{
        build_label, write_label, SolarisLabelSpec, MBR_TYPE_SUNIXOS, V_UNMNT,
    };
    use crate::partition::sun::{tag_from_text, SUN_TAG_WHOLE_DISK};

    let spc = u64::from(geometry.heads) * u64::from(geometry.sectors_per_track);
    if spc == 0 {
        bail!("heads and sectors-per-track must both be non-zero");
    }
    let part_start = SOLARIS_PART_START_CYLINDER * spc;
    let disk_sectors = disk_size / SECTOR;
    let pcyl = disk_sectors
        .checked_sub(part_start)
        .map(|s| s / spc)
        .filter(|c| *c > SOLARIS_ALT_CYLINDERS + SOLARIS_HEAD_CYLINDERS)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "a Solaris x86 disk needs more than {} cylinders of {}; {} is too small",
                SOLARIS_ALT_CYLINDERS + SOLARIS_HEAD_CYLINDERS,
                format_size(spc * SECTOR),
                format_size(disk_size),
            )
        })?;
    let ncyl = pcyl - SOLARIS_ALT_CYLINDERS;

    let mut slices = vec![
        (2usize, SUN_TAG_WHOLE_DISK, 0u16, 0u32, (ncyl * spc) as u32),
        (8, 1, V_UNMNT, 0, spc as u32),
        (9, 9, V_UNMNT, spc as u32, (2 * spc) as u32),
    ];
    for (p, slot) in placed.iter().zip(solaris_user_slices()) {
        let tag = tag_from_text(&p.type_text)
            .ok_or_else(|| anyhow::anyhow!("bad Solaris slice tag '{}'", p.type_text))?;
        let start = p.start_lba;
        if !start.is_multiple_of(spc) || !(p.size_bytes / SECTOR).is_multiple_of(spc) {
            bail!(
                "Solaris x86 lays slices out in whole cylinders of {}; slice {slot} at LBA {start} \
                 for {} is not one (leave --align at the cylinder size)",
                format_size(spc * SECTOR),
                format_size(p.size_bytes),
            );
        }
        if start < SOLARIS_HEAD_CYLINDERS * spc {
            bail!(
                "slice {slot} at LBA {start} starts inside the boot and alternates cylinders the \
                 Solaris partition reserves"
            );
        }
        let rel = start - part_start;
        if rel + p.size_bytes / SECTOR > ncyl * spc {
            bail!("slice {slot} runs past the {ncyl}-cylinder data area of the Solaris partition");
        }
        // `format(1M)` marks swap unmountable; everything else is a filesystem.
        let flag = if tag == 3 { V_UNMNT } else { 0 };
        slices.push((slot, tag, flag, rel as u32, (p.size_bytes / SECTOR) as u32));
    }

    let label = build_label(&SolarisLabelSpec {
        pcyl: pcyl as u32,
        ncyl: ncyl as u32,
        acyl: SOLARIS_ALT_CYLINDERS as u16,
        nhead: u32::from(geometry.heads),
        nsect: u32::from(geometry.sectors_per_track),
        rpm: 3600,
        ascii_label: format!(
            "DEFAULT cyl {ncyl} alt {SOLARIS_ALT_CYLINDERS} hd {} sec {}",
            geometry.heads, geometry.sectors_per_track,
        ),
        slices,
    });

    let mbr = mbr::build_minimal_mbr(
        0x5253_5459,
        &[(
            MBR_TYPE_SUNIXOS,
            part_start as u32,
            (pcyl * spc) as u32,
            true,
        )],
        geometry.heads,
        geometry.sectors_per_track,
    );
    out.seek(SeekFrom::Start(0))?;
    out.write_all(&mbr).context("writing the Solaris MBR")?;
    write_label(out, part_start, &label).context("writing the Solaris x86 VTOC")?;
    Ok(())
}

fn put_long(buf: &mut [u8; 512], long_idx: usize, value: u32) {
    buf[long_idx * 4..long_idx * 4 + 4].copy_from_slice(&value.to_be_bytes());
}

/// Fixed-width, NUL-padded string field, as the RDSK's drive-ID block uses.
fn put_cstr(buf: &mut [u8; 512], long_idx: usize, max_bytes: usize, text: &str) {
    let start = long_idx * 4;
    let n = text.len().min(max_bytes);
    buf[start..start + n].copy_from_slice(&text.as_bytes()[..n]);
}

/// BSTR: one length byte then the characters, as PART's drive name uses.
fn put_bstr(buf: &mut [u8; 512], long_idx: usize, max_chars: usize, text: &str) {
    let start = long_idx * 4;
    let n = text.len().min(max_chars);
    buf[start] = n as u8;
    buf[start + 1..start + 1 + n].copy_from_slice(&text.as_bytes()[..n]);
}

/// X68000 table at byte 2048; `start`/`length` count logical sectors.
fn write_x68k<W: Write + Seek>(out: &mut W, placed: &[Placed], disk_size: u64) -> Result<()> {
    use crate::partition::x68k::{X68kEntry, X68kPartitionTable, X68K_TABLE_OFFSET};

    let entries = placed
        .iter()
        .map(|p| {
            let mut name_raw = [b' '; 8];
            // X68k has no type code, so the name is the only label an entry carries.
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
    out.seek(SeekFrom::Start(X68K_TABLE_OFFSET))?;
    out.write_all(&table.to_bytes())
        .context("writing the X68000 partition table")?;
    Ok(())
}

/// One-line summary of a placed partition, shared by the CLI log and the GUI.
pub fn describe_placed(kind: TableKind, index: usize, p: &Placed) -> String {
    format!(
        "  {}: {} at LBA {} ({})",
        index + 1,
        format_size(p.size_bytes),
        p.start_lba,
        type_catalog::describe(kind, &p.type_text).unwrap_or(&p.type_text),
    )
}

/// Embed filesystem handlers in an RDB's `FileSystemHeader` chain.
///
/// A DosType with no ROM handler — `SFS\0`, `PFS\3` — cannot be mounted from
/// the partition table alone: the ROM's strap loads the handler out of the RDB
/// itself. This writes what HDToolBox writes, and what the `PFS\3` and `DOS\1`
/// chains on real reference disks contain: one `FSHD` per DosType, chained
/// through `fhb_Next`, each pointing at an `LSEG` chain holding the handler's
/// AmigaDOS load file verbatim.
///
/// Call after [`write_table`] on an RDB disk; blocks land after the `PART`
/// chain, inside the reserved area the RDSK already declares.
pub fn write_rdb_filesystems<W: Read + Write + Seek>(
    out: &mut W,
    filesystems: &[(u32, Vec<u8>)],
) -> Result<()> {
    use crate::partition::rdb::{stamp_checksum, stamp_checksum_longs, NO_BLOCK};

    if filesystems.is_empty() {
        return Ok(());
    }
    let mut rdsk = [0u8; 512];
    out.seek(SeekFrom::Start(0))?;
    out.read_exact(&mut rdsk)
        .context("reading the RDSK block")?;
    if &rdsk[0..4] != crate::partition::rdb::RDSK_SIGNATURE {
        bail!("not an RDB disk: no RDSK at block 0");
    }
    // The reserved area the RDSK itself declares; anything we add lives there.
    let rdb_hi = get_long(&rdsk, 33);
    // Walk the PART chain rather than trusting a count field: the first free
    // block is one past the highest PART we actually wrote.
    let mut next_free = 1u32;
    let mut part = get_long(&rdsk, 7);
    let mut guard = 0;
    while part != NO_BLOCK && part != 0 && guard < 64 {
        next_free = next_free.max(part + 1);
        let mut buf = [0u8; 512];
        out.seek(SeekFrom::Start(u64::from(part) * SECTOR))?;
        out.read_exact(&mut buf)?;
        part = get_long(&buf, 4);
        guard += 1;
    }

    // Lay each handler out as FSHD + LSEG chain, back to front, so every
    // block's `next` is known before it is written.
    let data_per_lseg = (SECTOR as usize) - 20;
    let mut first_fshd = NO_BLOCK;
    let mut planned: Vec<(u32, u32, u32, &[u8])> = Vec::new(); // fshd, seglist, dostype, image
    for (dostype, image) in filesystems {
        if image.is_empty() {
            bail!("filesystem handler for DosType {dostype:#010x} is empty");
        }
        let lsegs = image.len().div_ceil(data_per_lseg) as u32;
        let fshd_blk = next_free;
        let seglist = fshd_blk + 1;
        next_free = seglist + lsegs;
        if u64::from(next_free) > u64::from(rdb_hi) + 1 {
            bail!(
                "the RDB reserved area (blocks 0..{rdb_hi}) has no room for {} bytes of \
                 filesystem handlers; give the disk a larger geometry so cylinder 0 is bigger",
                filesystems.iter().map(|(_, i)| i.len()).sum::<usize>()
            );
        }
        if first_fshd == NO_BLOCK {
            first_fshd = fshd_blk;
        }
        planned.push((fshd_blk, seglist, *dostype, image));
    }

    for (i, (fshd_blk, seglist, dostype, image)) in planned.iter().enumerate() {
        let next_fshd = planned.get(i + 1).map(|p| p.0).unwrap_or(NO_BLOCK);
        let mut fshd = [0u8; 512];
        fshd[0..4].copy_from_slice(b"FSHD");
        for (long, value) in [
            (1u32, 64u32),  // fhb_SummedLongs: FSHD sums 64, not the block
            (3, 7),         // fhb_HostID
            (4, next_fshd), // fhb_Next
            (8, *dostype),
            (10, 0x180), // fhb_PatchFlags: patch dn_SegList + dn_GlobalVec
            (18, *seglist),
            (19, NO_BLOCK), // dn_GlobalVec: -1, as both reference disks write
        ] {
            put_long(&mut fshd, long as usize, value);
        }
        stamp_checksum_longs(&mut fshd, 64);
        out.seek(SeekFrom::Start(u64::from(*fshd_blk) * SECTOR))?;
        out.write_all(&fshd).context("writing an FSHD block")?;

        for (n, chunk) in image.chunks(data_per_lseg).enumerate() {
            let blk = seglist + n as u32;
            let last = (n + 1) * data_per_lseg >= image.len();
            let mut lseg = [0u8; 512];
            lseg[0..4].copy_from_slice(b"LSEG");
            // SummedLongs carries the payload length: bytes = longs * 4 - 20.
            let longs = (20 + chunk.len()).div_ceil(4) as u32;
            put_long(&mut lseg, 1, longs);
            put_long(&mut lseg, 3, 7);
            put_long(&mut lseg, 4, if last { NO_BLOCK } else { blk + 1 });
            lseg[20..20 + chunk.len()].copy_from_slice(chunk);
            stamp_checksum_longs(&mut lseg, longs as usize);
            out.seek(SeekFrom::Start(u64::from(blk) * SECTOR))?;
            out.write_all(&lseg).context("writing an LSEG block")?;
        }
    }

    put_long(&mut rdsk, 8, first_fshd); // rdb_FileSysHeaderList
    stamp_checksum(&mut rdsk);
    out.seek(SeekFrom::Start(0))?;
    out.write_all(&rdsk).context("rewriting the RDSK block")?;
    out.flush()?;
    Ok(())
}

/// Read a big-endian long by index, the way [`put_long`] writes one.
fn get_long(buf: &[u8; 512], long: usize) -> u32 {
    u32::from_be_bytes([
        buf[long * 4],
        buf[long * 4 + 1],
        buf[long * 4 + 2],
        buf[long * 4 + 3],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    const DEFAULT_ALIGN: u64 = 1024 * 1024;
    /// The geometry only the cylinder-based tables consult.
    const GEOM: Geometry = Geometry {
        heads: 16,
        sectors_per_track: 63,
    };

    fn spec(size: Option<u64>) -> PartSpec {
        PartSpec {
            size,
            ..Default::default()
        }
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
        let specs = vec![spec(Some(20 * 1024 * 1024)), spec(Some(40 * 1024 * 1024))];
        let placed = place(
            &specs,
            TableKind::Mbr,
            128 * 1024 * 1024,
            DEFAULT_ALIGN,
            GEOM,
        )
        .unwrap();
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
        let specs = vec![spec(Some(20 * 1024 * 1024)), spec(None)];
        let placed = place(&specs, TableKind::Gpt, disk, DEFAULT_ALIGN, GEOM).unwrap();
        // Must not tread on GPT's backup header at the tail.
        assert!(
            placed[1].end_byte() <= disk - reserved_tail(TableKind::Gpt, GEOM),
            "end {} vs disk {disk}",
            placed[1].end_byte(),
        );
        assert!(placed[1].size_bytes > 100 * 1024 * 1024);
    }

    /// Rounding a requested size up to the table's granularity must not turn
    /// a nonsense request into a valid one-sector partition.
    #[test]
    fn a_sub_sector_request_is_still_refused() {
        let specs = vec![spec(Some(300))];
        for kind in [TableKind::Mbr, TableKind::Rdb] {
            let err = place(&specs, kind, 200 * 1024 * 1024, DEFAULT_ALIGN, GEOM)
                .expect_err("300 bytes is not a partition");
            assert!(format!("{err:#}").contains("one sector"), "{err:#}");
        }
    }

    #[test]
    fn only_one_rest_is_allowed() {
        let specs = vec![spec(None), spec(None)];
        assert!(place(
            &specs,
            TableKind::Mbr,
            64 * 1024 * 1024,
            DEFAULT_ALIGN,
            GEOM
        )
        .is_err());
    }

    #[test]
    fn oversized_partitions_are_refused() {
        let specs = vec![spec(Some(200 * 1024 * 1024))];
        let err = place(
            &specs,
            TableKind::Mbr,
            64 * 1024 * 1024,
            DEFAULT_ALIGN,
            GEOM,
        )
        .expect_err("must refuse");
        assert!(format!("{err:#}").contains("usable"), "{err:#}");
    }

    #[test]
    fn place_enforces_the_table_slot_limit() {
        // The CLI used to check this itself; it belongs with the layout maths
        // so the GUI and TUI get the same refusal.
        let specs: Vec<PartSpec> = (0..5).map(|_| spec(Some(1024 * 1024))).collect();
        let err = place(
            &specs,
            TableKind::Mbr,
            128 * 1024 * 1024,
            DEFAULT_ALIGN,
            GEOM,
        )
        .expect_err("MBR holds 4");
        assert!(format!("{err:#}").contains("at most 4"), "{err:#}");
    }

    #[test]
    fn sgi_reserves_its_volume_header_region() {
        let specs = vec![spec(Some(100 * 1024 * 1024))];
        let placed = place(&specs, TableKind::Sgi, 512 * 1024 * 1024, 5040 * 512, GEOM).unwrap();
        // Must start past the 2 MiB volume header, on a cylinder boundary.
        assert!(placed[0].start_byte() >= 2 * 1024 * 1024);
        assert_eq!(placed[0].start_lba % 5040, 0);
    }

    /// Asking for the reference IRIS 3130's three sizes on its geometry has to
    /// put the slots on the blocks that disk uses, or it is not that shape.
    #[test]
    fn sgi_dklabel_reproduces_the_reference_iris_layout() {
        use crate::partition::sgi_dklabel::SgiDiskLabel;

        let geom = Geometry {
            heads: 7,
            sectors_per_track: 17,
        };
        let disk = 987 * 7 * 17 * SECTOR;
        let role = |bytes: u64, role: &str| PartSpec {
            size: Some(bytes),
            type_text: Some(role.to_string()),
            name: None,
        };
        let specs = vec![
            role(17850 * SECTOR, "root"),
            role(17731 * SECTOR, "swap"),
            role(79730 * SECTOR, "slice"),
        ];
        let align = default_align(TableKind::SgiDkLabel, geom);
        let placed = place(&specs, TableKind::SgiDkLabel, disk, align, geom).unwrap();
        assert_eq!(
            placed.iter().map(|p| p.start_lba).collect::<Vec<_>>(),
            vec![119, 17969, 35700],
        );

        let mut file = table_on_temp_disk(TableKind::SgiDkLabel, &placed, disk, geom);
        let mut sector = [0u8; 512];
        file.read_exact(&mut sector).unwrap();
        let label = SgiDiskLabel::parse(&sector).unwrap();
        assert_eq!((label.cylinders, label.heads, label.sectors), (987, 7, 17));
        assert_eq!(label.total_blocks(), 987 * 7 * 17);
        assert_eq!(label.bootfs, 0, "the root slot is also the boot slot");
        assert_eq!(label.swapfs, 1);
        assert_eq!(label.rootnotboot, 0);
        assert_eq!(label.map[0].base, 119);
        assert_eq!(label.map[0].size, 17850);
        // The last slot is the whole-disk wrapper every label of the era
        // carries, and it must not show up as a browsable partition.
        assert_eq!(label.map[7].base, 0);
        assert_eq!(label.map[7].size, 987 * 7 * 17);
        assert_eq!(label.browsable_slots().count(), 3);
        assert_eq!(label.slot_role(0), "root");
        assert_eq!(label.slot_role(1), "swap");
        assert_eq!(label.slot_role(2), "slice");
    }

    /// `d_swapfs` names a slot, so leaving it at 0 when no swap slice was asked
    /// for would make slot 0 read back as swap and lose its type byte.
    #[test]
    fn sgi_dklabel_without_a_swap_slot_names_no_slot_as_swap() {
        use crate::partition::sgi_dklabel::SgiDiskLabel;

        let geom = Geometry {
            heads: 4,
            sectors_per_track: 32,
        };
        let disk = 64 * 1024 * 1024;
        let specs = vec![
            PartSpec {
                size: Some(8 * 1024 * 1024),
                type_text: Some("boot".to_string()),
                name: None,
            },
            PartSpec {
                size: None,
                type_text: Some("root".to_string()),
                name: None,
            },
        ];
        let align = default_align(TableKind::SgiDkLabel, geom);
        let placed = place(&specs, TableKind::SgiDkLabel, disk, align, geom).unwrap();
        let mut file = table_on_temp_disk(TableKind::SgiDkLabel, &placed, disk, geom);
        let mut sector = [0u8; 512];
        file.read_exact(&mut sector).unwrap();
        let label = SgiDiskLabel::parse(&sector).unwrap();
        assert_eq!(label.bootfs, 0);
        assert_eq!(label.rootnotboot, 1, "root is a different slot from boot");
        assert_eq!(label.rootfs, 1);
        assert!(
            (label.swapfs as usize) >= crate::partition::sgi_dklabel::SGI_DKLABEL_NFS,
            "d_swapfs must not name a real slot when there is no swap slice",
        );
        assert!((0..2).all(|i| label.slot_role(i) != "swap"));
    }

    /// The geometry describes whole cylinders only, so a `rest` slot on a disk
    /// with a part-cylinder tail must stop short rather than pass `d_altstart`.
    #[test]
    fn sgi_dklabel_rest_slot_stops_at_the_last_whole_cylinder() {
        use crate::partition::sgi_dklabel::SgiDiskLabel;

        let geom = Geometry {
            heads: 7,
            sectors_per_track: 17,
        };
        let cyl = geom.cylinder_bytes();
        // Two and a half sectors past a whole number of cylinders.
        let disk = 400 * cyl + 3 * SECTOR;
        let placed = place(
            &[spec(None)],
            TableKind::SgiDkLabel,
            disk,
            default_align(TableKind::SgiDkLabel, geom),
            geom,
        )
        .unwrap();
        let mut file = table_on_temp_disk(TableKind::SgiDkLabel, &placed, disk, geom);
        let mut sector = [0u8; 512];
        file.read_exact(&mut sector).unwrap();
        let label = SgiDiskLabel::parse(&sector).unwrap();
        let end = u64::from(label.map[0].base) + u64::from(label.map[0].size);
        assert_eq!(end, label.total_blocks());
        assert_eq!(u64::from(label.altstart), label.total_blocks());
    }

    #[test]
    fn sgi_dklabel_refuses_an_unknown_slot_role() {
        let geom = Geometry::default();
        let disk = 64 * 1024 * 1024;
        let specs = vec![PartSpec {
            size: Some(8 * 1024 * 1024),
            type_text: Some("usr".to_string()),
            name: None,
        }];
        let align = default_align(TableKind::SgiDkLabel, geom);
        let placed = place(&specs, TableKind::SgiDkLabel, disk, align, geom).unwrap();
        let mut file = tempfile::tempfile().unwrap();
        file.set_len(disk).unwrap();
        let err = write_table(&mut file, TableKind::SgiDkLabel, &placed, disk, geom).unwrap_err();
        assert!(
            err.to_string().contains("slot role 'usr'"),
            "unexpected error: {err:#}"
        );
    }

    /// The GUI hides its geometry controls and Name column behind these, so a
    /// table that needs either and doesn't declare it loses the input.
    #[test]
    fn geometry_and_name_predicates_match_what_the_writers_use() {
        let geometry = Geometry {
            heads: 8,
            sectors_per_track: 32,
        };
        for &kind in WRITABLE_TABLES {
            assert_eq!(
                uses_cylinder_geometry(kind),
                default_align(kind, geometry) == geometry.cylinder_bytes(),
                "{} disagrees about cylinder alignment",
                kind.label(),
            );
        }
        assert!(
            records_geometry(TableKind::Next),
            "NeXT records its geometry"
        );
        assert!(
            !uses_cylinder_geometry(TableKind::Next),
            "but cuts on sectors"
        );
        assert!(carries_entry_name(TableKind::Rdb), "RDB drive names");
        assert!(!carries_entry_name(TableKind::Mbr));
        assert!(!carries_entry_name(TableKind::Atari));
    }

    #[test]
    fn sgi_and_x68k_have_slot_limits() {
        assert_eq!(slot_limit(TableKind::Sgi), Some(14));
        assert_eq!(slot_limit(TableKind::X68k), Some(8));
        assert_eq!(slot_limit(TableKind::Rdb), Some(15));
        assert_eq!(slot_limit(TableKind::Gpt), None);
    }

    /// RDB bounds are low/high cylinder numbers, so both ends of a partition
    /// must be cylinder-aligned — not just the start, as elsewhere.
    #[test]
    fn rdb_sizes_round_up_to_whole_cylinders() {
        let geometry = Geometry::default();
        let cyl = geometry.cylinder_bytes();
        let align = default_align(TableKind::Rdb, geometry);
        assert_eq!(align, cyl);

        // 60 MiB is not a whole number of 504 KiB cylinders.
        let specs = vec![spec(Some(60 * 1024 * 1024)), spec(None)];
        let placed = place(&specs, TableKind::Rdb, 200 * 1024 * 1024, align, GEOM).unwrap();
        for p in &placed {
            assert_eq!(p.start_byte() % cyl, 0, "start {p:?}");
            assert_eq!(p.size_bytes % cyl, 0, "size {p:?}");
        }
        // The RDB itself owns cylinder 0, so the first partition starts at 1.
        assert_eq!(placed[0].start_byte(), cyl);
        assert!(placed[0].size_bytes >= 60 * 1024 * 1024);
    }

    /// The AmigaDOS DosType tag and drive name are the two fields that make an
    /// RDB entry mountable, and neither is checked by the generic round-trip.
    #[test]
    fn rdb_carries_dos_types_and_drive_names() {
        use crate::partition::PartitionTable;

        let disk = 200 * 1024 * 1024;
        let geometry = Geometry::default();
        let specs = vec![
            PartSpec {
                size: Some(60 * 1024 * 1024),
                type_text: Some("DOS\\3".to_string()),
                name: Some("WORK".to_string()),
            },
            PartSpec {
                size: None,
                type_text: Some("PFS\\3".to_string()),
                name: None,
            },
        ];
        let placed = place(
            &specs,
            TableKind::Rdb,
            disk,
            default_align(TableKind::Rdb, geometry),
            geometry,
        )
        .unwrap();
        let mut reader = table_on_temp_disk(TableKind::Rdb, &placed, disk, geometry);
        let table = PartitionTable::detect(&mut reader).unwrap();
        let PartitionTable::Rdb(rdb) = &table else {
            panic!("did not reparse as RDB: {table:?}");
        };
        assert_eq!(rdb.partitions.len(), 2);
        assert_eq!(rdb.partitions[0].dos_type_string(), "DOS\\3");
        assert_eq!(rdb.partitions[0].drv_name, "WORK");
        assert_eq!(rdb.partitions[1].dos_type_string(), "PFS\\3");
        // An unnamed entry gets the conventional AmigaDOS device name.
        assert_eq!(rdb.partitions[1].drv_name, "DH1");
        // Partition geometry has to agree with the RDSK's, or AmigaOS computes
        // a different offset than we wrote.
        assert_eq!(rdb.partitions[0].surfaces, u32::from(geometry.heads));
        assert_eq!(
            rdb.partitions[0].blk_per_trk,
            u32::from(geometry.sectors_per_track)
        );
        assert_eq!(rdb.header.cyl_blks, geometry.cylinder_bytes() as u32 / 512);
    }

    /// A Sun label is only valid if its 256 big-endian words XOR to zero, and
    /// slice 2 has to carry the whole-disk alias every Sun tool looks for.
    #[test]
    fn sun_label_checksums_and_reserves_the_backup_slice() {
        use crate::partition::sun::{SunDiskLabel, SUN_TAG_WHOLE_DISK};

        let disk = 200 * 1024 * 1024;
        let geometry = Geometry::default();
        let specs = vec![
            PartSpec {
                size: Some(20 * 1024 * 1024),
                type_text: Some("root".to_string()),
                name: None,
            },
            PartSpec {
                size: None,
                // A bare tag number has to be accepted alongside the name.
                type_text: Some("4".to_string()),
                name: None,
            },
        ];
        let align = default_align(TableKind::Sun, geometry);
        let placed = place(&specs, TableKind::Sun, disk, align, geometry).unwrap();
        let sector = first_sector_of(table_on_temp_disk(TableKind::Sun, &placed, disk, geometry));

        assert!(SunDiskLabel::detect(&sector), "checksum or magic wrong");
        let label = SunDiskLabel::parse(&sector).unwrap();
        assert!(label.vtoc_valid);
        assert_eq!(label.ntrks, geometry.heads);
        assert_eq!(label.nsect, geometry.sectors_per_track);
        assert_eq!(label.slices[2].tag, SUN_TAG_WHOLE_DISK);
        assert_eq!(
            label.slices[2].num_sectors as u64,
            u64::from(label.ncyl) * label.sectors_per_cylinder,
        );
        // User slices skip 2 and keep the tags we asked for.
        assert_eq!(label.slices[0].tag, 2, "root");
        assert_eq!(label.slices[1].tag, 4, "usr");
        let browsable: Vec<u64> = label
            .browsable_slices()
            .map(|(_, s)| s.start_sector)
            .collect();
        assert_eq!(browsable, vec![placed[0].start_lba, placed[1].start_lba]);
    }

    /// AHDI has no magic number — only the 0x1234 word-sum and plausible
    /// entries — and GEM cannot describe a partition over 16 MiB.
    #[test]
    fn ahdi_stamps_its_checksum_and_promotes_oversized_gem_to_bgm() {
        use crate::partition::atari::{AhdiPartitionKind, AhdiTable};

        let disk = 64 * 1024 * 1024;
        let specs = vec![
            spec(Some(8 * 1024 * 1024)),
            spec(Some(24 * 1024 * 1024)),
            PartSpec {
                size: None,
                type_text: Some("RAW".to_string()),
                name: None,
            },
        ];
        let placed = place(&specs, TableKind::Atari, disk, DEFAULT_ALIGN, GEOM).unwrap();
        assert_eq!(placed[0].type_text, "GEM", "8 MiB still fits GEM");
        assert_eq!(placed[1].type_text, "BGM", "24 MiB must be promoted");
        assert_eq!(placed[2].type_text, "RAW", "an explicit tag is left alone");

        let sector = first_sector_of(table_on_temp_disk(
            TableKind::Atari,
            &placed,
            disk,
            Geometry::default(),
        ));

        let table = AhdiTable::parse_root(&sector).unwrap();
        assert!(table.checksum_valid, "root sector word-sum is not 0x1234");
        assert!(matches!(table.primary[0].kind, AhdiPartitionKind::Gem));
        assert!(matches!(table.primary[1].kind, AhdiPartitionKind::Bgm));
        assert!(matches!(table.primary[2].kind, AhdiPartitionKind::Raw));
        assert!(table.primary[3].is_empty(), "unused slot must stay zeroed");
        for (want, got) in placed.iter().zip(table.primary.iter()) {
            assert!(got.exists());
            assert_eq!(u64::from(got.start_sector), want.start_lba);
            assert_eq!(got.size_bytes(), want.size_bytes);
        }
        assert_eq!(u64::from(table.disk_size_sectors), disk / 512);
    }

    #[test]
    fn rdb_refuses_a_disk_smaller_than_two_cylinders() {
        let geometry = Geometry::default();
        let mut buf = std::io::Cursor::new(vec![0u8; 4096]);
        let err = write_table(&mut buf, TableKind::Rdb, &[], 4096, geometry)
            .expect_err("one cylinder is 504 KiB");
        assert!(format!("{err:#}").contains("two cylinders"), "{err:#}");
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

    /// Write `placed` onto a sparse temp file sized `disk`, rewound and ready
    /// to re-parse.
    ///
    /// A file, not a `Vec` — a realistic disk size is hundreds of MiB, and that
    /// much contiguous heap fails outright on 32-bit Windows (it broke the
    /// i686 CI leg). `set_len` costs no RAM and nothing but a hole on disk.
    fn table_on_temp_disk(
        kind: TableKind,
        placed: &[Placed],
        disk: u64,
        geometry: Geometry,
    ) -> std::fs::File {
        let mut file = tempfile::tempfile().expect("temp disk");
        file.set_len(disk).expect("size the temp disk");
        write_table(&mut file, kind, placed, disk, geometry)
            .unwrap_or_else(|e| panic!("{} write: {e:#}", kind.label()));
        file.seek(SeekFrom::Start(0)).expect("rewind");
        file
    }

    /// The first 512 bytes of a temp disk, for the tables that keep their whole
    /// table in sector 0.
    fn first_sector_of(mut disk: std::fs::File) -> [u8; 512] {
        let mut sector = [0u8; 512];
        disk.read_exact(&mut sector).expect("read the table sector");
        sector
    }

    /// The NeXT trap: `p_base` counts 1024-byte sectors from the end of the
    /// front porch, so an editor that assumes 512-byte LBAs puts a partition
    /// at half its intended offset.
    #[test]
    fn next_partitions_are_measured_from_the_front_porch() {
        use crate::partition::next::{NextDiskLabel, LABEL_SPAN};
        use std::io::Read;

        let disk = 660 * 1024 * 1024;
        let geometry = Geometry::default();
        let specs = vec![spec(None)];
        let placed = place(&specs, TableKind::Next, disk, 1024 * 1024, geometry).unwrap();
        assert_eq!(placed[0].start_lba, 2048, "1 MiB alignment, past the porch");

        let mut file = table_on_temp_disk(TableKind::Next, &placed, disk, geometry);
        let mut buf = vec![0u8; LABEL_SPAN];
        file.read_exact(&mut buf).unwrap();
        let label = NextDiskLabel::parse(&buf, 0).unwrap();
        assert_eq!(label.sector_size, 1024);
        assert_eq!(label.front_porch, 160);
        let (_, p) = label.browsable_partitions().next().expect("one partition");
        assert_eq!(p.base, (1024 * 1024 / 1024) - 160);
        assert_eq!(p.start_byte, placed[0].start_byte());
        assert_eq!(p.size_bytes, placed[0].size_bytes);
    }

    /// A Solaris x86 slice is relative to the Solaris MBR partition, and the
    /// label owns the first three cylinders of it plus the disk's cylinder 0.
    #[test]
    fn solaris_slices_start_past_the_labels_own_cylinders() {
        use crate::partition::PartitionTable;

        let geometry = Geometry {
            heads: 128,
            sectors_per_track: 63,
        };
        let spc = u64::from(geometry.heads) * u64::from(geometry.sectors_per_track);
        let disk = 3 * 1024 * 1024 * 1024;
        let align = default_align(TableKind::SolarisX86, geometry);
        let specs = vec![spec(Some(64 * 1024 * 1024)), spec(None)];
        let placed = place(&specs, TableKind::SolarisX86, disk, align, geometry).unwrap();
        assert_eq!(placed[0].start_lba, SOLARIS_HEAD_CYLINDERS * spc);

        let mut reader = table_on_temp_disk(TableKind::SolarisX86, &placed, disk, geometry);
        let table = PartitionTable::detect(&mut reader).unwrap();
        let PartitionTable::SolarisX86 { label, mbr } = &table else {
            panic!("not a Solaris x86 disk: {table:?}");
        };
        assert_eq!(label.partition_start_lba, spc, "partition at cylinder 1");
        assert_eq!(mbr.entries[0].partition_type, 0x82);
        assert_eq!(
            u64::from(label.slices[0].relative_start),
            placed[0].start_lba - spc,
        );
        // The backup alias covers ncyl, and the two alternate cylinders past
        // it are why `rest` must stop short of the end of the disk.
        assert_eq!(
            label.slices[2].num_sectors as u64,
            disk / SECTOR / spc * spc - spc - SOLARIS_ALT_CYLINDERS * spc,
        );
        assert!(placed[1].end_byte() <= disk - SOLARIS_ALT_CYLINDERS * spc * SECTOR);
    }

    /// Round-trip every writable table through `write_table` and re-parse it,
    /// which is what proves the GUI's in-memory path matches the CLI's file one.
    #[test]
    fn every_writable_table_writes_and_reparses() {
        use crate::partition::PartitionTable;

        let disk = 512 * 1024 * 1024;
        for &kind in WRITABLE_TABLES {
            let geometry = Geometry::default();
            let align = default_align(kind, geometry);
            let specs = vec![spec(Some(64 * 1024 * 1024)), spec(None)];
            let placed = place(&specs, kind, disk, align, geometry)
                .unwrap_or_else(|e| panic!("{} place: {e:#}", kind.label()));

            let mut reader = table_on_temp_disk(kind, &placed, disk, geometry);
            let table = PartitionTable::detect(&mut reader)
                .unwrap_or_else(|e| panic!("{} reparse: {e:#}", kind.label()));
            assert_eq!(
                type_catalog::kind_of(&table),
                kind,
                "{} round-tripped as a different table",
                kind.label(),
            );
            let parsed = table.partitions();
            // A Solaris x86 label also lists its own boot and alternates slices.
            let want = if kind == TableKind::SolarisX86 { 4 } else { 2 };
            assert_eq!(parsed.len(), want, "{} partition count", kind.label());
            for (want, got) in placed.iter().zip(parsed.iter()) {
                assert_eq!(got.start_lba, want.start_lba, "{} start", kind.label());
                assert_eq!(got.size_bytes, want.size_bytes, "{} size", kind.label());
            }
        }
    }

    /// The name column in the Build Disk modal has to reach the disk on the
    /// tables that carry one. APM ignored it until `write_apm` re-stamped the
    /// entries `build_minimal_apm` had already named "Partition N".
    #[test]
    fn named_entries_survive_the_round_trip() {
        use crate::partition::PartitionTable;

        let disk = 128 * 1024 * 1024;
        let named = |n: &str| PartSpec {
            size: Some(32 * 1024 * 1024),
            type_text: None,
            name: Some(n.to_string()),
        };
        for (kind, wanted) in [
            (TableKind::Apm, "MacVolume"),
            (TableKind::Gpt, "MyData"),
            // X68000 truncates names to 8 bytes.
            (TableKind::X68k, "HUMAN68K"),
            (TableKind::Rdb, "DH4"),
        ] {
            let specs = vec![named(wanted)];
            let geometry = Geometry::default();
            let placed =
                place(&specs, kind, disk, default_align(kind, geometry), geometry).unwrap();
            let mut reader = table_on_temp_disk(kind, &placed, disk, geometry);
            let table = PartitionTable::detect(&mut reader).unwrap();
            let names = format!("{:?}", table.partitions());
            assert!(
                names.contains(wanted),
                "{} lost the entry name; got {names}",
                kind.label(),
            );
        }
    }
}
