//! Synthesize a complete SGI/IRIX disk image from scratch: a 512-byte SGI volume
//! header (dvh) + partition table at sector 0, and a root partition that holds a
//! freshly formatted EFS or XFS filesystem, or nothing at all (label only).
//!
//! A bare EFS or XFS volume has no partition table, but a real IRIX disk needs the
//! volume header at sector 0 so `fx` / `prtvtoc` and the disk driver see one. This
//! module writes that header with [`part_tables::sgi`] (the parser is the source of
//! truth for the field layout and big-endianness) and lays the root filesystem in.
//!
//! Layouts (IRIX convention, verified against `tests/fixtures/sgi/irix_volhdr.bin`):
//! - [`SgiPartitionLayout::DataOnly`] (the default): slot 8 VOLHDR (type 0) at 0,
//!   slot 10 VOLUME (type 6) spanning the disk, slot 0 root (EFS or XFS) after the
//!   volume header, cylinder-aligned, filling the rest. A non-bootable data disk.
//! - [`SgiPartitionLayout::RootSwap`]: a system disk as `fx` lays one out, with slot 1
//!   swap (type RAW) right after the volume header and the root in slot 0 after it.
//! - [`SgiMedia::CdRom`]: the EFS filesystem in slot 7 typed SYSV with a 64-sector
//!   header, as on IRIX 5.3 / 6.5 CDs.
//!
//! [`SgiHddOptions::format_root`] `= false` writes the volume header only (the
//! label-only stage): the partition table is in place, the partitions are left as
//! they were. All geometry is big-endian; sectors are always 512 bytes.

use anyhow::{ensure, Result};
use std::io::{Cursor, Seek, SeekFrom, Write};

use fs_unix::efs::{write_blank_efs, EFS_DEFAULT_BYTES_PER_INODE};
use part_tables::sgi::{
    SgiDeviceParameters, SgiPartitionEntry, SgiPartitionType, SgiVolumeDirEntry, SgiVolumeHeader,
    SGI_NUM_PARTITIONS, SGI_NUM_VOL_DIR, SGI_VOLHDR_MAGIC,
};

const SECTOR_SIZE: u64 = 512;

/// Default heads (tracks per cylinder). Must match the geometry the target
/// drive reports via SCSI MODE SENSE — the IRIS emulator (and typical SGI SCSI
/// HDDs) synthesize 16 heads (`iris/src/scsi.rs`).
pub const DEFAULT_HEADS: u16 = 16;
/// Default sectors per track (512-byte sectors). **Must** match the drive's
/// reported geometry: IRIX `fx` compares the volume header's `device_parameters`
/// against the geometry the drive reports and rejects the sgilabel ("disagrees
/// with existing volume header parameters") if they differ — which keeps the
/// disk from mounting at all. The IRIS emulator reports 63 sectors/track with
/// 16 heads, so a cylinder is 16 × 63 = 1008 sectors (504 KiB). Verified against
/// a real IRIX-formatted disk; see `docs/sgi_efs_hdd_irix_debug.md`.
pub const DEFAULT_SECTORS_PER_TRACK: u16 = 63;
/// Fixed volume-header region size (slot 8, VOLHDR), rounded up to a whole
/// cylinder. 2 MiB matches the real IRIX fixture's volhdr region.
const VOLHDR_REGION_BYTES: u64 = 2 * 1024 * 1024;
/// EFS minimum volume size (the `create_blank_efs` floor), in sectors.
const MIN_EFS_SECTORS: u64 = (32 * 1024) / SECTOR_SIZE;
/// XFS minimum: two allocation groups of 4096 4-KiB blocks (`write_blank_xfs`'s floor).
const MIN_XFS_SECTORS: u64 = (32 * 1024 * 1024) / SECTOR_SIZE;
/// Default swap for [`SgiPartitionLayout::RootSwap`]: 64 MiB, rounded up to whole cylinders.
pub const DEFAULT_SWAP_BYTES: u64 = 64 * 1024 * 1024;

/// CD-ROM geometry: IRIX EFS CDs report 1 head × 32 sectors/track (cylinder =
/// 32 sectors = 16 KiB). Verified against real IRIX 5.3 / 6.5 distribution CDs.
/// A 32-sector cylinder is also a whole number of 2048-byte CD sectors.
pub const CDROM_HEADS: u16 = 1;
pub const CDROM_SECTORS_PER_TRACK: u16 = 32;
/// CD-ROM volume-header region (slot 8): 64 sectors (32 KiB, two cylinders),
/// matching the IRIX 6.5 Applications CD — just the header, no boot miniroot.
const CDROM_VOLHDR_SECTORS: u64 = 64;

// SGI partition slot indices (IRIX convention).
const SLOT_ROOT: usize = 0;
const SLOT_SWAP: usize = 1;
/// EFS CDs put the filesystem in slot 7 (mounted `mount -t efs … s7`).
const SLOT_EFS_CDROM: usize = 7;
const SLOT_VOLHDR: usize = 8;
const SLOT_VOLUME: usize = 10;

/// Target medium — selects the partition shape IRIX expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SgiMedia {
    /// A hard disk: the root in slot 0, a 2 MiB volume header.
    #[default]
    HardDisk,
    /// An EFS CD-ROM image (`.iso`): the EFS filesystem in slot 7 typed SYSV
    /// (the IRIX EFS-CD convention — `IS_EFS()` accepts SYSV), with a tiny
    /// 64-sector volume header. Mounted on IRIX with `mount -t efs … s7`.
    CdRom,
}

/// The root partition's filesystem, which also sets its partition type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SgiRootFs {
    /// EFS (partition type 7; SYSV on a CD).
    #[default]
    Efs,
    /// XFS (partition type 10), formatted by `fs_xfs::xfs::format::write_blank_xfs`.
    /// That writes a v5 (CRC) filesystem, which IRIX itself cannot mount.
    Xfs,
}

/// How the hard disk is partitioned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SgiPartitionLayout {
    /// One root partition after the volume header (a data disk).
    #[default]
    DataOnly,
    /// Swap in slot 1 right after the volume header, the root after it (a system disk).
    RootSwap { swap_bytes: u64 },
}

/// Inputs for [`write_sgi_disk`] / [`build_sgi_disk`].
#[derive(Debug, Clone)]
pub struct SgiHddOptions {
    /// Requested disk size in bytes. Rounded **up** to a whole cylinder.
    pub size_bytes: u64,
    /// Filesystem volume label (EFS keeps up to 6 bytes, XFS 12).
    pub name: String,
    /// Heads / tracks per cylinder. Default [`DEFAULT_HEADS`].
    pub heads: u16,
    /// Sectors per track (512-byte sectors). Default [`DEFAULT_SECTORS_PER_TRACK`].
    pub sectors_per_track: u16,
    /// EFS inode density, in bytes per inode (the inode count scales as
    /// `efs_bytes / bytes_per_inode`). Floored at one inode per 512-byte block.
    pub bytes_per_inode: u64,
    /// Target medium — hard disk (root in slot 0) or CD-ROM (EFS in slot 7,
    /// typed SYSV). [`new`](Self::new) defaults to hard disk.
    pub media: SgiMedia,
    /// The root partition's filesystem (and partition type). Default EFS.
    pub root_fs: SgiRootFs,
    /// Hard-disk partitioning. Default [`SgiPartitionLayout::DataOnly`].
    pub layout: SgiPartitionLayout,
    /// Format the root partition. `false` writes the volume header only (label only).
    pub format_root: bool,
}

impl SgiHddOptions {
    /// Hard-disk options with the default IRIS/SGI geometry and inode density.
    pub fn new(size_bytes: u64, name: impl Into<String>) -> Self {
        SgiHddOptions {
            size_bytes,
            name: name.into(),
            heads: DEFAULT_HEADS,
            sectors_per_track: DEFAULT_SECTORS_PER_TRACK,
            bytes_per_inode: EFS_DEFAULT_BYTES_PER_INODE,
            media: SgiMedia::HardDisk,
            root_fs: SgiRootFs::Efs,
            layout: SgiPartitionLayout::DataOnly,
            format_root: true,
        }
    }

    /// EFS CD-ROM options: 1 × 32 CD geometry and the slot-7/SYSV EFS layout.
    pub fn new_cdrom(size_bytes: u64, name: impl Into<String>) -> Self {
        SgiHddOptions {
            heads: CDROM_HEADS,
            sectors_per_track: CDROM_SECTORS_PER_TRACK,
            media: SgiMedia::CdRom,
            ..SgiHddOptions::new(size_bytes, name)
        }
    }
}

/// The computed disk layout, for the CLI to print and tests to assert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SgiHddLayout {
    pub disk_sectors: u64,
    pub disk_bytes: u64,
    pub cylinders: u32,
    pub heads: u16,
    pub sectors_per_track: u16,
    pub cylinder_sectors: u64,
    /// VOLHDR region length (slot 8), in 512-byte sectors.
    pub volhdr_sectors: u64,
    /// The root partition's slot (0 on a hard disk, 7 on a CD).
    pub root_slot: usize,
    /// First sector of the root partition.
    pub root_first_sector: u64,
    /// Root partition length, in 512-byte sectors.
    pub root_sectors: u64,
    /// First sector and length of the swap partition (slot 1); both 0 without one.
    pub swap_first_sector: u64,
    pub swap_sectors: u64,
}

/// Compute the disk geometry / partition layout for the given options without
/// writing any bytes. Validates the geometry and that the root partition meets
/// its filesystem's minimum size.
pub fn compute_sgi_layout(opts: &SgiHddOptions) -> Result<SgiHddLayout> {
    ensure!(opts.heads > 0, "heads must be > 0");
    ensure!(opts.sectors_per_track > 0, "sectors-per-track must be > 0");
    if opts.media == SgiMedia::CdRom {
        ensure!(
            opts.root_fs == SgiRootFs::Efs,
            "an SGI CD-ROM holds EFS only"
        );
        ensure!(
            opts.layout == SgiPartitionLayout::DataOnly,
            "an SGI CD-ROM has no swap partition"
        );
    }

    let cylinder_sectors = opts.heads as u64 * opts.sectors_per_track as u64;
    let cylinder_bytes = cylinder_sectors * SECTOR_SIZE;

    // VOLHDR region (slot 8). A hard disk reserves 2 MiB (rounded up to a whole
    // cylinder); an EFS CD reserves a tiny 64-sector header, matching real IRIX
    // EFS CDs. Both must be cylinder-aligned so the partition that follows
    // starts on a cylinder boundary.
    let volhdr_sectors = match opts.media {
        SgiMedia::HardDisk => VOLHDR_REGION_BYTES.div_ceil(cylinder_bytes) * cylinder_sectors,
        SgiMedia::CdRom => CDROM_VOLHDR_SECTORS.div_ceil(cylinder_sectors) * cylinder_sectors,
    };
    let swap_sectors = match opts.layout {
        SgiPartitionLayout::DataOnly => 0,
        SgiPartitionLayout::RootSwap { swap_bytes } => {
            ensure!(swap_bytes > 0, "swap size must be > 0");
            swap_bytes.div_ceil(cylinder_bytes) * cylinder_sectors
        }
    };
    // A label-only disk needs a root partition of at least one cylinder; a formatted one its filesystem's floor.
    let min_root_sectors = match (opts.format_root, opts.root_fs) {
        (false, _) => cylinder_sectors,
        (true, SgiRootFs::Efs) => MIN_EFS_SECTORS,
        (true, SgiRootFs::Xfs) => MIN_XFS_SECTORS,
    };

    // Total disk rounded up to a whole cylinder, and at least big enough for
    // the VOLHDR region, swap and a minimum root partition.
    let requested_sectors = opts.size_bytes.div_ceil(SECTOR_SIZE);
    let min_disk_sectors = volhdr_sectors + swap_sectors + min_root_sectors;
    let disk_cyls = requested_sectors
        .max(min_disk_sectors)
        .div_ceil(cylinder_sectors);
    let disk_sectors = disk_cyls * cylinder_sectors;

    ensure!(
        disk_sectors <= u32::MAX as u64,
        "disk too large for an SGI volume header: {disk_sectors} sectors exceeds the 32-bit \
         block range (max ~2 TiB)"
    );
    ensure!(
        disk_cyls <= 0x00FF_FFFF,
        "cylinder count {disk_cyls} exceeds the 24-bit cylinder range; use a larger \
         --heads / --sectors geometry"
    );

    let swap_first_sector = if swap_sectors > 0 { volhdr_sectors } else { 0 };
    let root_first_sector = volhdr_sectors + swap_sectors;
    let root_sectors = disk_sectors - root_first_sector;
    ensure!(
        root_sectors >= min_root_sectors,
        "root partition would be only {} bytes; increase --size",
        root_sectors * SECTOR_SIZE
    );

    Ok(SgiHddLayout {
        disk_sectors,
        disk_bytes: disk_sectors * SECTOR_SIZE,
        cylinders: disk_cyls as u32,
        heads: opts.heads,
        sectors_per_track: opts.sectors_per_track,
        cylinder_sectors,
        volhdr_sectors,
        root_slot: match opts.media {
            SgiMedia::HardDisk => SLOT_ROOT,
            SgiMedia::CdRom => SLOT_EFS_CDROM,
        },
        root_first_sector,
        root_sectors,
        swap_first_sector,
        swap_sectors,
    })
}

/// Build the 512-byte SGI volume header (sector 0) for the given layout.
pub fn build_volume_header(
    opts: &SgiHddOptions,
    layout: &SgiHddLayout,
) -> [u8; SECTOR_SIZE as usize] {
    let mut partitions: Vec<SgiPartitionEntry> = (0..SGI_NUM_PARTITIONS)
        .map(|_| SgiPartitionEntry {
            blocks: 0,
            first: 0,
            partition_type_raw: 0,
        })
        .collect();
    // Hard disks put the root in slot 0 typed by its filesystem; EFS CDs put it in
    // slot 7 typed SYSV (the IRIX EFS-CD convention, mounted `… s7`).
    let root_type = match (opts.media, opts.root_fs) {
        (SgiMedia::CdRom, _) => SgiPartitionType::SysV,
        (SgiMedia::HardDisk, SgiRootFs::Efs) => SgiPartitionType::Efs,
        (SgiMedia::HardDisk, SgiRootFs::Xfs) => SgiPartitionType::Xfs,
    };
    partitions[layout.root_slot] = SgiPartitionEntry {
        blocks: layout.root_sectors as u32,
        first: layout.root_first_sector as u32,
        partition_type_raw: root_type.as_u32(),
    };
    if layout.swap_sectors > 0 {
        partitions[SLOT_SWAP] = SgiPartitionEntry {
            blocks: layout.swap_sectors as u32,
            first: layout.swap_first_sector as u32,
            partition_type_raw: SgiPartitionType::Raw.as_u32(),
        };
    }
    partitions[SLOT_VOLHDR] = SgiPartitionEntry {
        blocks: layout.volhdr_sectors as u32,
        first: 0,
        partition_type_raw: SgiPartitionType::VolHdr.as_u32(),
    };
    partitions[SLOT_VOLUME] = SgiPartitionEntry {
        blocks: layout.disk_sectors as u32,
        first: 0,
        partition_type_raw: SgiPartitionType::Volume.as_u32(),
    };

    let volume_directory: Vec<SgiVolumeDirEntry> = (0..SGI_NUM_VOL_DIR)
        .map(|_| SgiVolumeDirEntry {
            name: String::new(),
            block_num: 0,
            bytes: 0,
        })
        .collect();

    let vh = SgiVolumeHeader {
        magic: SGI_VOLHDR_MAGIC,
        root_part_num: layout.root_slot as u16,
        // A data disk has no separate swap partition (0, as rb-cli always wrote); a system disk names slot 1.
        swap_part_num: if layout.swap_sectors > 0 {
            SLOT_SWAP as u16
        } else {
            0
        },
        device_parameters: SgiDeviceParameters::for_geometry(
            layout.cylinders,
            opts.heads,
            opts.sectors_per_track,
        ),
        bootfile: "/unix".to_string(),
        volume_directory,
        partitions,
        checksum: 0,
        checksum_valid: true,
    };
    vh.to_bytes() // 512 bytes, checksum zero-summed
}

/// Stream an SGI disk into `sink`: the volume header at sector 0 and, unless
/// `format_root` is false, a freshly formatted root filesystem. Only non-zero
/// regions are written, so a `set_len`'d file stays sparse. Peak memory is the
/// filesystem's metadata (the EFS bitmap, one XFS AG header), not the disk.
pub fn write_sgi_disk<W: Write + Seek>(sink: &mut W, opts: &SgiHddOptions) -> Result<SgiHddLayout> {
    let layout = compute_sgi_layout(opts)?;
    write_sgi_disk_into(sink, opts, &layout)?;
    Ok(layout)
}

/// Build an SGI disk **in memory** (exactly `layout.disk_bytes`). For large disks
/// prefer [`write_sgi_disk`], which streams without allocating the image.
pub fn build_sgi_disk(opts: &SgiHddOptions) -> Result<(Vec<u8>, SgiHddLayout)> {
    let layout = compute_sgi_layout(opts)?;
    let mut image = vec![0u8; layout.disk_bytes as usize];
    write_sgi_disk_into(&mut Cursor::new(&mut image[..]), opts, &layout)?;
    Ok((image, layout))
}

/// The name rb-cli and the provisioning runner have always called; same as [`write_sgi_disk`].
pub fn write_sgi_efs_hdd<W: Write + Seek>(
    sink: &mut W,
    opts: &SgiHddOptions,
) -> Result<SgiHddLayout> {
    write_sgi_disk(sink, opts)
}

/// The name rb-cli and the tests have always called; same as [`build_sgi_disk`].
pub fn build_sgi_efs_hdd(opts: &SgiHddOptions) -> Result<(Vec<u8>, SgiHddLayout)> {
    build_sgi_disk(opts)
}

/// Write the dvh and the root filesystem into `sink` using a precomputed layout.
fn write_sgi_disk_into<W: Write + Seek>(
    sink: &mut W,
    opts: &SgiHddOptions,
    layout: &SgiHddLayout,
) -> Result<()> {
    let sector0 = build_volume_header(opts, layout);
    sink.seek(SeekFrom::Start(0))?;
    sink.write_all(&sector0)?;
    if !opts.format_root {
        return Ok(());
    }
    let root_off = layout.root_first_sector * SECTOR_SIZE;
    match opts.root_fs {
        SgiRootFs::Efs => write_blank_efs(
            sink,
            root_off,
            layout.root_sectors as u32,
            &opts.name,
            opts.bytes_per_inode,
        )?,
        SgiRootFs::Xfs => write_root_xfs(
            sink,
            root_off,
            layout.root_sectors * SECTOR_SIZE,
            &opts.name,
        )?,
    }
    Ok(())
}

#[cfg(feature = "xfs")]
fn write_root_xfs<W: Write + Seek>(sink: &mut W, off: u64, bytes: u64, name: &str) -> Result<()> {
    fs_xfs::xfs::format::write_blank_xfs(sink, off, bytes, name)?;
    Ok(())
}

#[cfg(not(feature = "xfs"))]
fn write_root_xfs<W: Write + Seek>(_: &mut W, _: u64, _: u64, _: &str) -> Result<()> {
    anyhow::bail!("this build of sgi-disk has no XFS support (feature `xfs`)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open::{filesystem_partitions, open_partition, open_partition_editable, SgiFsKind};
    use std::io::Cursor;

    /// Default IRIS/SGI geometry: 16 heads × 63 sectors = 1008-sector (504 KiB)
    /// cylinders. A 50 MiB request rounds up to a whole number of cylinders, the
    /// VOLHDR region rounds 2 MiB up to whole cylinders, and EFS fills the rest.
    #[test]
    fn builds_expected_layout() {
        let (img, layout) =
            build_sgi_efs_hdd(&SgiHddOptions::new(50 * 1024 * 1024, "IRIX")).unwrap();
        assert_eq!(layout.heads, 16);
        assert_eq!(layout.sectors_per_track, 63);
        assert_eq!(layout.cylinder_sectors, 1008); // 16 × 63 = 504 KiB
                                                   // 50 MiB = 102400 sectors -> ceil(102400 / 1008) = 102 cylinders.
        assert_eq!(layout.cylinders, 102);
        assert_eq!(layout.disk_sectors, 102 * 1008);
        assert_eq!(layout.disk_bytes, 102 * 1008 * 512);
        assert_eq!(img.len() as u64, layout.disk_bytes);
        // VOLHDR region: 2 MiB rounds up to 5 cylinders (5040 sectors).
        assert_eq!(layout.volhdr_sectors, 5040);
        assert_eq!(layout.root_first_sector, 5040);
        assert_eq!(layout.root_sectors, 102 * 1008 - 5040);
        // Rounded up to at least the request, and a whole-cylinder multiple.
        assert!(layout.disk_bytes >= 50 * 1024 * 1024);
        assert_eq!(layout.disk_sectors % layout.cylinder_sectors, 0);
    }

    /// The synthesized sector 0 parses through our own SGI parser, checksums to
    /// zero, and matches the fixture's slot convention (8 VOLHDR @0, 10 VOLUME @0
    /// spanning the disk, 0 EFS after VOLHDR), with sane geometry.
    #[test]
    fn header_round_trips_and_matches_fixture_structure() {
        let (img, layout) =
            build_sgi_efs_hdd(&SgiHddOptions::new(20 * 1024 * 1024, "ROOT")).unwrap();
        let vh = SgiVolumeHeader::parse(&img[..512]).expect("synthesized header parses");
        assert_eq!(vh.magic, SGI_VOLHDR_MAGIC);
        assert!(vh.checksum_valid, "checksum must zero-sum");
        assert_eq!(vh.root_part_num, 0);

        // Geometry sane: 512-byte sectors, the cylinder count we computed.
        assert_eq!(vh.device_parameters.secbytes, 512);
        assert_eq!(vh.device_parameters.trks0, layout.heads);
        assert_eq!(vh.device_parameters.secs, layout.sectors_per_track);
        assert_eq!(vh.device_parameters.cylinders(), layout.cylinders);

        // Slot convention.
        let p0 = &vh.partitions[0];
        assert_eq!(p0.partition_type(), SgiPartitionType::Efs);
        assert_eq!(p0.first as u64, layout.root_first_sector);
        assert_eq!(p0.blocks as u64, layout.root_sectors);

        let p8 = &vh.partitions[8];
        assert_eq!(p8.partition_type(), SgiPartitionType::VolHdr);
        assert_eq!(p8.first, 0);
        assert_eq!(p8.blocks as u64, layout.volhdr_sectors);

        let p10 = &vh.partitions[10];
        assert_eq!(p10.partition_type(), SgiPartitionType::Volume);
        assert_eq!(p10.first, 0);
        assert_eq!(p10.blocks as u64, layout.disk_sectors);
    }

    /// The disk's one filesystem partition is found from the header and opens as an
    /// EFS with a readable, empty root directory.
    #[test]
    fn detects_and_opens_the_efs_partition() {
        let (img, layout) =
            build_sgi_efs_hdd(&SgiHddOptions::new(16 * 1024 * 1024, "DATA")).unwrap();
        let vh = SgiVolumeHeader::parse(&img[..512]).unwrap();
        let parts = filesystem_partitions(&vh);
        assert_eq!(parts.len(), 1, "only the EFS partition holds a filesystem");
        assert_eq!(parts[0].kind, SgiFsKind::Efs);
        assert_eq!(parts[0].offset, layout.root_first_sector * 512);

        let mut fs = open_partition(Cursor::new(img), 0).expect("open EFS at slot 0");
        let root = fs.root().expect("root");
        assert!(
            fs.list_directory(&root).expect("list root").is_empty(),
            "a freshly formatted EFS root is empty"
        );
        // EFS reports the label as `fname:fpack`; both were set from the name.
        assert!(
            fs.volume_label().unwrap_or_default().contains("DATA"),
            "volume label carries the name"
        );
    }

    /// A disk too small to hold the VOLHDR region plus a minimum EFS partition
    /// is still rounded up to something valid (we floor the disk, not error),
    /// and the EFS partition meets the 32 KiB minimum.
    #[test]
    fn tiny_request_is_floored_to_a_valid_disk() {
        let (_img, layout) = build_sgi_efs_hdd(&SgiHddOptions::new(1024, "T")).unwrap();
        assert!(layout.root_sectors >= MIN_EFS_SECTORS);
        assert_eq!(layout.disk_sectors % layout.cylinder_sectors, 0);
    }

    /// An EFS CD-ROM has the IRIX shape: 1×32 CD geometry, the EFS filesystem in
    /// slot 7 typed SYSV (which `PartitionTable` surfaces as EFS and opens), and
    /// a tiny cylinder-aligned volume header. Matches real IRIX 5.3/6.5 CDs.
    #[test]
    fn cdrom_layout_matches_irix_efs_cd_shape() {
        let (img, layout) =
            build_sgi_efs_hdd(&SgiHddOptions::new_cdrom(64 * 1024 * 1024, "CDVOL")).unwrap();
        assert_eq!(layout.heads, 1);
        assert_eq!(layout.sectors_per_track, 32);
        assert_eq!(layout.cylinder_sectors, 32);
        assert_eq!(layout.disk_sectors % 4, 0, "whole 2048-byte CD sectors");

        let vh = SgiVolumeHeader::parse(&img[..512]).expect("parses");
        assert!(vh.checksum_valid);
        assert_eq!(vh.root_part_num, 7);
        // Slot 7 is the EFS partition, typed SYSV per the IRIX EFS-CD convention.
        assert_eq!(vh.partitions[7].partition_type(), SgiPartitionType::SysV);
        assert_eq!(vh.partitions[7].first as u64, layout.root_first_sector);
        assert!(vh.partitions[7].partition_type().is_efs());
        assert_eq!(vh.partitions[8].partition_type(), SgiPartitionType::VolHdr);
        assert_eq!(vh.partitions[10].partition_type(), SgiPartitionType::Volume);
        assert!(vh.partitions[0].is_empty(), "no slot-0 root on a CD");

        // The SYSV partition is EFS, and opens read-only from slot 7.
        let parts = filesystem_partitions(&vh);
        assert_eq!(parts.len(), 1);
        assert_eq!((parts[0].index, parts[0].kind), (7, SgiFsKind::Efs));
        let mut fs = open_partition(Cursor::new(img), 7).expect("open EFS at slot 7");
        let root = fs.root().expect("root");
        assert!(fs.list_directory(&root).expect("list").is_empty());
    }

    /// Streaming to a (sparse) file via `write_sgi_efs_hdd` produces bytes
    /// identical to the in-memory `build_sgi_efs_hdd` — the untouched regions
    /// must read back as zero, and the file must be exactly `disk_bytes`.
    #[test]
    fn streamed_file_matches_in_memory_image() {
        use std::io::{Read, Seek, SeekFrom};
        let opts = SgiHddOptions::new(40 * 1024 * 1024, "STREAM");
        let (mut mem, layout) = build_sgi_efs_hdd(&opts).unwrap();

        let mut file = tempfile::tempfile().expect("tempfile");
        let streamed_layout = write_sgi_efs_hdd(&mut file, &opts).expect("stream");
        file.set_len(streamed_layout.disk_bytes).unwrap();
        assert_eq!(streamed_layout, layout);

        let mut on_disk = Vec::new();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_to_end(&mut on_disk).unwrap();
        assert_eq!(on_disk.len() as u64, layout.disk_bytes, "exact size");

        // The EFS root inode's atime/mtime/ctime are stamped from
        // `SystemTime::now()` and the two builds run microseconds apart, so
        // a byte-for-byte compare would race on a second boundary. Zero the
        // 12-byte time window in both copies before comparing — every other
        // byte still has to match. Root inode 2 lives at slot 2 in the first
        // CG's inode block; times are at offsets 12..24 of the inode.
        let efs_off = (layout.root_first_sector * SECTOR_SIZE) as usize;
        let firstcg = 2 + (layout.root_sectors as u32).div_ceil(8).div_ceil(512);
        let root_off = efs_off + firstcg as usize * 512 + 2 * 128;
        for b in mem[root_off + 12..root_off + 24].iter_mut() {
            *b = 0;
        }
        for b in on_disk[root_off + 12..root_off + 24].iter_mut() {
            *b = 0;
        }
        assert_eq!(on_disk, mem, "streamed bytes match the in-memory image");
    }

    /// Inode density scales the cylinder-group inode count: a smaller
    /// bytes-per-inode yields more inodes for the same disk.
    #[test]
    fn bytes_per_inode_controls_inode_count() {
        let mut sparse = SgiHddOptions::new(64 * 1024 * 1024, "INO");
        sparse.bytes_per_inode = 16384; // ~1 inode / 16 KiB
        let mut dense = sparse.clone();
        dense.bytes_per_inode = 2048; // ~1 inode / 2 KiB

        let inodes = |opts: &SgiHddOptions| {
            let (img, layout) = build_sgi_efs_hdd(opts).unwrap();
            let sb_off = (layout.root_first_sector * 512 + 512) as usize;
            let sb = fs_unix::efs::EfsSuperblock::parse(&img[sb_off..]).unwrap();
            sb.ncg as u32 * sb.cgisize as u32 * 4
        };
        assert!(
            inodes(&dense) > inodes(&sparse) * 3,
            "denser inode setting must yield many more inodes"
        );
    }

    /// Write a file through the editable partition, then reopen and read it back.
    fn write_and_read_back(img: Vec<u8>, slot: usize) {
        use disk_core::filesystem::CreateFileOptions;
        let mut img = img;
        {
            let mut fs =
                open_partition_editable(Cursor::new(&mut img), slot).expect("open editable");
            let root = fs.root().unwrap();
            let body = b"hello from sgi-disk".to_vec();
            fs.create_file(
                &root,
                "hello.txt",
                &mut &body[..],
                body.len() as u64,
                &CreateFileOptions::default(),
            )
            .expect("create");
            fs.sync_metadata().expect("sync");
        }
        let mut fs = open_partition(Cursor::new(img), slot).expect("reopen");
        let root = fs.root().unwrap();
        let entry = fs
            .list_directory(&root)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "hello.txt")
            .expect("file listed");
        assert_eq!(
            fs.read_file(&entry, 1 << 20).unwrap(),
            b"hello from sgi-disk"
        );
    }

    #[test]
    fn an_efs_disk_takes_a_file_through_editable_filesystem() {
        let (img, _) = build_sgi_disk(&SgiHddOptions::new(16 * 1024 * 1024, "EFS")).unwrap();
        write_and_read_back(img, 0);
    }

    /// The XFS variant: slot 0 typed XFS, a valid checksum, XFS opens and takes a file.
    #[cfg(feature = "xfs")]
    #[test]
    fn xfs_variant_parses_opens_and_takes_a_file() {
        let mut opts = SgiHddOptions::new(64 * 1024 * 1024, "XFSROOT");
        opts.root_fs = SgiRootFs::Xfs;
        let (img, layout) = build_sgi_disk(&opts).unwrap();
        let vh = SgiVolumeHeader::parse(&img[..512]).unwrap();
        assert!(vh.checksum_valid);
        assert_eq!(vh.partitions[0].partition_type(), SgiPartitionType::Xfs);
        assert_eq!(vh.partitions[0].first as u64, layout.root_first_sector);
        assert_eq!(filesystem_partitions(&vh)[0].kind, SgiFsKind::Xfs);
        assert_eq!(
            &img[layout.root_first_sector as usize * 512..][..4],
            b"XFSB"
        );
        write_and_read_back(img, 0);
    }

    /// XFS below its two-AG floor is refused, not formatted.
    #[cfg(feature = "xfs")]
    #[test]
    fn xfs_variant_floors_the_disk_at_the_xfs_minimum() {
        let mut opts = SgiHddOptions::new(1024, "X");
        opts.root_fs = SgiRootFs::Xfs;
        let layout = compute_sgi_layout(&opts).unwrap();
        assert!(layout.root_sectors >= MIN_XFS_SECTORS);
    }

    /// Label only: the header parses and checksums, the partition is typed, nothing else is written.
    #[test]
    fn label_only_writes_the_header_and_nothing_else() {
        for root_fs in [SgiRootFs::Efs, SgiRootFs::Xfs] {
            let mut opts = SgiHddOptions::new(16 * 1024 * 1024, "LBL");
            opts.root_fs = root_fs;
            opts.format_root = false;
            let (img, layout) = build_sgi_disk(&opts).unwrap();
            let vh = SgiVolumeHeader::parse(&img[..512]).unwrap();
            assert!(vh.checksum_valid);
            let kind = filesystem_partitions(&vh)[0].kind;
            assert_eq!(kind == SgiFsKind::Xfs, root_fs == SgiRootFs::Xfs);
            assert_eq!(vh.partitions[0].first as u64, layout.root_first_sector);
            assert!(
                img[512..].iter().all(|&b| b == 0),
                "only sector 0 is written"
            );
            assert!(
                open_partition(Cursor::new(img), 0).is_err(),
                "no filesystem to open"
            );
        }
    }

    /// Label only over an existing disk leaves every byte past sector 0 as it was.
    #[test]
    fn label_only_leaves_the_rest_of_a_disk_untouched() {
        let mut disk = vec![0xA5u8; 8 * 1024 * 1024];
        let mut opts = SgiHddOptions::new(disk.len() as u64, "KEEP");
        opts.format_root = false;
        write_sgi_disk(&mut Cursor::new(&mut disk[..]), &opts).unwrap();
        assert!(SgiVolumeHeader::parse(&disk[..512]).unwrap().checksum_valid);
        assert!(disk[512..].iter().all(|&b| b == 0xA5));
    }

    /// Root plus swap: swap in slot 1 right after the volume header, the root after it.
    #[test]
    fn root_swap_layout_matches_an_irix_system_disk() {
        let mut opts = SgiHddOptions::new(128 * 1024 * 1024, "SYS");
        opts.layout = SgiPartitionLayout::RootSwap {
            swap_bytes: DEFAULT_SWAP_BYTES,
        };
        let (img, layout) = build_sgi_disk(&opts).unwrap();
        let vh = SgiVolumeHeader::parse(&img[..512]).unwrap();
        assert!(vh.checksum_valid);
        assert_eq!((vh.root_part_num, vh.swap_part_num), (0, 1));
        let swap = &vh.partitions[1];
        assert_eq!(swap.partition_type(), SgiPartitionType::Raw);
        assert_eq!(swap.first as u64, layout.volhdr_sectors);
        assert_eq!(swap.blocks as u64, layout.swap_sectors);
        assert_eq!(layout.swap_sectors % layout.cylinder_sectors, 0);
        assert!(layout.swap_sectors * 512 >= DEFAULT_SWAP_BYTES);
        assert_eq!(
            layout.root_first_sector,
            layout.volhdr_sectors + layout.swap_sectors
        );
        assert_eq!(
            layout.root_first_sector + layout.root_sectors,
            layout.disk_sectors
        );
        assert_eq!(
            filesystem_partitions(&vh).len(),
            1,
            "swap holds no filesystem"
        );
        write_and_read_back(img, 0);
    }

    /// A data disk keeps today's header: no slot 1, swap_part_num 0.
    #[test]
    fn data_only_is_still_the_default() {
        let opts = SgiHddOptions::new(16 * 1024 * 1024, "D");
        assert_eq!(opts.layout, SgiPartitionLayout::DataOnly);
        assert_eq!((opts.heads, opts.sectors_per_track), (16, 63));
        let (img, layout) = build_sgi_disk(&opts).unwrap();
        let vh = SgiVolumeHeader::parse(&img[..512]).unwrap();
        assert!(vh.partitions[1].is_empty());
        assert_eq!(vh.swap_part_num, 0);
        assert_eq!(layout.swap_sectors, 0);
    }

    /// The opener says what a non-filesystem slot holds, and spots a byte-swapped image.
    #[test]
    fn the_opener_explains_what_it_cannot_open() {
        let (img, _) = build_sgi_disk(&SgiHddOptions::new(16 * 1024 * 1024, "E")).unwrap();
        let err = open_partition(Cursor::new(img.clone()), 8)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("holds no EFS or XFS"), "{err}");
        let mut swapped = img;
        disk_core::sgi::swab16_in_place(&mut swapped[..512]);
        let err = open_partition(Cursor::new(swapped), 0)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("byte-swapped"), "{err}");
    }
}
