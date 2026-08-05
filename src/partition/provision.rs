//! Lay out and write a fresh partition table on a blank disk.
//!
//! Shared by `rb-cli new hd {mbr|gpt|apm|sgi|x68k|rdb|sun}`, the TUI's New
//! wizard and the GUI's Build Disk mode, so all three place partitions
//! identically and emit byte-for-byte the same tables. AHDI is parse-only; see
//! `docs/partition_table_writers_backlog.md` for what that writer still needs.
//!
//! Sizes are laid out in order from `align` (1 MiB by default), each rounded up
//! to the alignment, past whatever head/tail region the table itself reserves.
//! A single `rest` size claims the remainder.
//!
//! The writers take any `Write + Seek`, so the same code fills an image file,
//! a raw device handle, or an in-memory buffer in tests.

use anyhow::{bail, Context, Result};
use std::io::{Seek, SeekFrom, Write};

use crate::partition::type_catalog::{self, TableKind};
use crate::partition::{apm, format_size, gpt, mbr, parse_size};

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
    TableKind::X68k,
    TableKind::Rdb,
    TableKind::Sun,
];

/// The slice a Sun label reserves for the whole-disk "backup" alias, which is
/// conventionally slice 2 and overlaps every real slice.
const SUN_BACKUP_SLICE: usize = 2;

/// How many partitions the table can hold, or `None` when it is unbounded in
/// any practical sense.
pub fn slot_limit(kind: TableKind) -> Option<usize> {
    match kind {
        TableKind::Mbr => Some(4),
        TableKind::X68k => Some(crate::partition::x68k::X68K_MAX_PARTITIONS),
        // SGI has 16 slots but reserves 8 (volhdr) and 10 (whole volume).
        TableKind::Sgi => Some(crate::partition::sgi::SGI_NUM_PARTITIONS - 2),
        // RDB is a linked list, but keeping every PART block inside the first
        // 16 sectors is what every Amiga tool scans for.
        TableKind::Rdb => Some(crate::partition::rdb::RDB_SCAN_BLOCKS as usize - 1),
        // Sun has 8 slices; slice 2 is the whole-disk backup alias.
        TableKind::Sun => Some(7),
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
        // X68k entries carry a name, not a type code.
        TableKind::X68k => "Human68k",
        TableKind::Rdb => "DOS\\3",
        TableKind::Sun => "root",
        _ => "83",
    }
}

/// Default entry name when a spec doesn't give one. RDB names are AmigaDOS
/// device names, so they get the conventional `DH0`, `DH1`, ... instead.
pub fn default_name(kind: TableKind, index: usize) -> String {
    match kind {
        TableKind::Rdb => format!("DH{index}"),
        _ => format!("Partition {}", index + 1),
    }
}

/// Slice numbers a Sun label offers to user partitions, in order: everything
/// but the whole-disk backup alias.
fn sun_user_slices() -> impl Iterator<Item = usize> {
    (0..8).filter(|&i| i != SUN_BACKUP_SLICE)
}

/// Bytes at the head of the disk the table itself needs.
pub fn reserved_head(kind: TableKind) -> u64 {
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
        // RDB: the RDSK plus its PART chain. Cylinder alignment then pushes
        // the first partition out to cylinder 1, which is where Amiga tools
        // put it.
        TableKind::Rdb => crate::partition::rdb::RDB_SCAN_BLOCKS * SECTOR,
        // Sun keeps only the 512-byte label at sector 0, but slices start on
        // cylinder boundaries, so alignment pushes slice 0 out to cylinder 1.
        TableKind::Sun => SECTOR,
        _ => SECTOR,
    }
}

/// Bytes at the tail the table needs (GPT's backup header + array).
pub fn reserved_tail(kind: TableKind) -> u64 {
    match kind {
        TableKind::Gpt => 33 * SECTOR,
        _ => 0,
    }
}

/// The alignment to lay partitions on when the caller has no preference.
/// SGI and RDB want cylinder boundaries; everything else gets 1 MiB.
pub fn default_align(kind: TableKind, geometry: Geometry) -> u64 {
    match kind {
        TableKind::Sgi | TableKind::Rdb | TableKind::Sun => geometry.cylinder_bytes().max(SECTOR),
        _ => 1024 * 1024,
    }
}

/// Granularity a partition's *size* has to land on, not just its start. RDB
/// stores bounds as low/high cylinder numbers, so a size that isn't a whole
/// number of cylinders cannot be expressed; every other table counts sectors.
fn size_granularity(kind: TableKind, align: u64) -> u64 {
    match kind {
        TableKind::Rdb | TableKind::Sun => align.max(SECTOR),
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
) -> Result<Vec<Placed>> {
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
        .checked_sub(reserved_tail(kind))
        .filter(|e| *e > reserved_head(kind))
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
    let mut cursor = round_up(reserved_head(kind).max(align), align);
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
            Some(n) => round_up(n, gran),
            None => usable_end
                .checked_sub(start)
                .map(|n| (n / gran) * gran)
                .filter(|n| *n > 0)
                .ok_or_else(|| anyhow::anyhow!("no space left for the `rest` partition"))?,
        };
        if size < SECTOR {
            bail!("partition {} is smaller than one sector", i + 1);
        }
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
            type_text: spec
                .type_text
                .clone()
                .filter(|t| !t.trim().is_empty())
                .unwrap_or_else(|| default_type(kind).to_string()),
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
/// `geometry` only matters for SGI; every other table ignores it.
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
        TableKind::Rdb => write_rdb(out, placed, disk_size, geometry),
        TableKind::Sun => write_sun(out, placed, disk_size, geometry),
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

/// Amiga Rigid Disk Block: an `RDSK` at sector 0 followed by one `PART` block
/// per partition, chained through `pb_Next`. Every field is big-endian, bounds
/// are cylinders, and each block's longs must sum to zero.
///
/// Field placement follows `amitools`' `RDBlock.py` / `PartitionBlock.py`,
/// which is the reference AmigaOS-compatible writer.
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

/// Sun disk label (SMI VTOC): one 512-byte label at sector 0 holding geometry,
/// an 8-entry VTOC tag table and 8 `{start_cylinder, num_sectors}` slices.
///
/// Slice 2 is the whole-disk "backup" alias every Sun tool expects, so user
/// partitions fill the other seven. Field offsets follow the kernel's
/// `struct sun_disklabel` (`block/partitions/sun.c`), which is also what our
/// parser reads.
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

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_ALIGN: u64 = 1024 * 1024;

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
        let specs = vec![spec(Some(20 * 1024 * 1024)), spec(None)];
        let placed = place(&specs, TableKind::Gpt, disk, DEFAULT_ALIGN).unwrap();
        // Must not tread on GPT's backup header at the tail.
        assert!(
            placed[1].end_byte() <= disk - reserved_tail(TableKind::Gpt),
            "end {} vs disk {disk}",
            placed[1].end_byte(),
        );
        assert!(placed[1].size_bytes > 100 * 1024 * 1024);
    }

    #[test]
    fn only_one_rest_is_allowed() {
        let specs = vec![spec(None), spec(None)];
        assert!(place(&specs, TableKind::Mbr, 64 * 1024 * 1024, DEFAULT_ALIGN).is_err());
    }

    #[test]
    fn oversized_partitions_are_refused() {
        let specs = vec![spec(Some(200 * 1024 * 1024))];
        let err = place(&specs, TableKind::Mbr, 64 * 1024 * 1024, DEFAULT_ALIGN)
            .expect_err("must refuse");
        assert!(format!("{err:#}").contains("usable"), "{err:#}");
    }

    #[test]
    fn place_enforces_the_table_slot_limit() {
        // The CLI used to check this itself; it belongs with the layout maths
        // so the GUI and TUI get the same refusal.
        let specs: Vec<PartSpec> = (0..5).map(|_| spec(Some(1024 * 1024))).collect();
        let err = place(&specs, TableKind::Mbr, 128 * 1024 * 1024, DEFAULT_ALIGN)
            .expect_err("MBR holds 4");
        assert!(format!("{err:#}").contains("at most 4"), "{err:#}");
    }

    #[test]
    fn sgi_reserves_its_volume_header_region() {
        let specs = vec![spec(Some(100 * 1024 * 1024))];
        let placed = place(&specs, TableKind::Sgi, 512 * 1024 * 1024, 5040 * 512).unwrap();
        // Must start past the 2 MiB volume header, on a cylinder boundary.
        assert!(placed[0].start_byte() >= 2 * 1024 * 1024);
        assert_eq!(placed[0].start_lba % 5040, 0);
    }

    #[test]
    fn sgi_and_x68k_have_slot_limits() {
        assert_eq!(slot_limit(TableKind::Sgi), Some(14));
        assert_eq!(slot_limit(TableKind::X68k), Some(8));
        assert_eq!(slot_limit(TableKind::Rdb), Some(15));
        assert_eq!(slot_limit(TableKind::Gpt), None);
    }

    /// RDB bounds are low/high cylinder numbers, so both ends of every
    /// partition have to be cylinder-aligned — not just the start, which is
    /// all the other tables need.
    #[test]
    fn rdb_sizes_round_up_to_whole_cylinders() {
        let geometry = Geometry::default();
        let cyl = geometry.cylinder_bytes();
        let align = default_align(TableKind::Rdb, geometry);
        assert_eq!(align, cyl);

        // 60 MiB is not a whole number of 504 KiB cylinders.
        let specs = vec![spec(Some(60 * 1024 * 1024)), spec(None)];
        let placed = place(&specs, TableKind::Rdb, 200 * 1024 * 1024, align).unwrap();
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
        use std::io::Cursor;

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
        )
        .unwrap();
        let mut buf = Cursor::new(vec![0u8; disk as usize]);
        write_table(&mut buf, TableKind::Rdb, &placed, disk, geometry).unwrap();

        let mut reader = Cursor::new(buf.into_inner());
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
        use std::io::Cursor;

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
        let placed = place(&specs, TableKind::Sun, disk, align).unwrap();
        let mut buf = Cursor::new(vec![0u8; disk as usize]);
        write_table(&mut buf, TableKind::Sun, &placed, disk, geometry).unwrap();
        let img = buf.into_inner();

        assert!(SunDiskLabel::detect(&img[..512]), "checksum or magic wrong");
        let label = SunDiskLabel::parse(&img[..512]).unwrap();
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

    #[test]
    fn rdb_refuses_a_disk_smaller_than_two_cylinders() {
        use std::io::Cursor;
        let geometry = Geometry::default();
        let mut buf = Cursor::new(vec![0u8; 4096]);
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

    /// Round-trip every writable table through `write_table` and re-parse it,
    /// which is what proves the GUI's in-memory path matches the CLI's file one.
    #[test]
    fn every_writable_table_writes_and_reparses() {
        use crate::partition::PartitionTable;
        use std::io::Cursor;

        let disk = 512 * 1024 * 1024;
        for &kind in WRITABLE_TABLES {
            let geometry = Geometry::default();
            let align = default_align(kind, geometry);
            let specs = vec![spec(Some(64 * 1024 * 1024)), spec(None)];
            let placed = place(&specs, kind, disk, align)
                .unwrap_or_else(|e| panic!("{} place: {e:#}", kind.label()));

            let mut buf = Cursor::new(vec![0u8; disk as usize]);
            write_table(&mut buf, kind, &placed, disk, geometry)
                .unwrap_or_else(|e| panic!("{} write: {e:#}", kind.label()));

            let mut reader = Cursor::new(buf.into_inner());
            let table = PartitionTable::detect(&mut reader)
                .unwrap_or_else(|e| panic!("{} reparse: {e:#}", kind.label()));
            assert_eq!(
                type_catalog::kind_of(&table),
                kind,
                "{} round-tripped as a different table",
                kind.label(),
            );
            let parsed = table.partitions();
            assert_eq!(parsed.len(), 2, "{} partition count", kind.label());
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
        use std::io::Cursor;

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
            let placed = place(&specs, kind, disk, default_align(kind, geometry)).unwrap();
            let mut buf = Cursor::new(vec![0u8; disk as usize]);
            write_table(&mut buf, kind, &placed, disk, geometry).unwrap();

            let mut reader = Cursor::new(buf.into_inner());
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
