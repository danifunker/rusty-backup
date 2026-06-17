//! Synthesize a complete SGI/IRIX hard-disk image from scratch: a 512-byte SGI
//! volume header (dvh) + partition table at sector 0 wrapping a freshly
//! formatted EFS root partition, mountable by IRIX 5.3–6.5 as a SCSI HDD.
//!
//! `rb-cli new --fs efs` produces a *bare* EFS superfloppy (fine for an EFS
//! CD-ROM that IRIX reads with `mount -t efs`), but a real IRIX hard disk needs
//! the volume header at sector 0 so `fx` / `prtvtoc` and the disk driver see a
//! partition table. This module writes that header — mirroring the field
//! layout / big-endianness of [`crate::partition::sgi`], the parser used as the
//! source of truth — and lays an EFS filesystem in partition 0.
//!
//! Layout (IRIX convention, verified against `tests/fixtures/sgi/irix_volhdr.bin`):
//! - slot 8 VOLHDR (type 0): first=0, blocks = volume-header region.
//! - slot 10 VOLUME (type 6): first=0, blocks = entire disk.
//! - slot 0 EFS (type 7): first = after VOLHDR (cylinder-aligned), blocks = remainder; an EFS filesystem lives here.
//!
//! Swap (slot 1, RAW) is omitted — this is a non-bootable data disk. All
//! geometry is big-endian; sectors are always 512 bytes.

use anyhow::{ensure, Context, Result};

use crate::fs::efs::create_blank_efs;
use crate::partition::sgi::{
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

// SGI partition slot indices (IRIX convention).
const SLOT_EFS_ROOT: usize = 0;
const SLOT_VOLHDR: usize = 8;
const SLOT_VOLUME: usize = 10;

/// Inputs for [`build_sgi_efs_hdd`].
#[derive(Debug, Clone)]
pub struct SgiHddOptions {
    /// Requested disk size in bytes. Rounded **up** to a whole cylinder.
    pub size_bytes: u64,
    /// EFS volume label (up to 6 bytes; longer is truncated by the formatter).
    pub name: String,
    /// Heads / tracks per cylinder.
    pub heads: u16,
    /// Sectors per track (512-byte sectors).
    pub sectors_per_track: u16,
}

impl SgiHddOptions {
    /// Options with the default 1 MiB-cylinder geometry.
    pub fn new(size_bytes: u64, name: impl Into<String>) -> Self {
        SgiHddOptions {
            size_bytes,
            name: name.into(),
            heads: DEFAULT_HEADS,
            sectors_per_track: DEFAULT_SECTORS_PER_TRACK,
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
    /// First sector of the EFS root partition (slot 0).
    pub efs_first_sector: u64,
    /// EFS root partition length, in 512-byte sectors.
    pub efs_sectors: u64,
}

/// Build a dvh-wrapped EFS hard-disk image in memory. Returns the full disk
/// bytes (exactly `layout.disk_bytes`) plus the computed [`SgiHddLayout`].
///
/// The result is addressable by every partition-aware verb: `PartitionTable::
/// detect` recognizes the volume header, `partitions()` exposes the EFS root as
/// a `0xA1`-typed partition, and the EFS lives at `efs_first_sector × 512`.
pub fn build_sgi_efs_hdd(opts: &SgiHddOptions) -> Result<(Vec<u8>, SgiHddLayout)> {
    ensure!(opts.heads > 0, "heads must be > 0");
    ensure!(opts.sectors_per_track > 0, "sectors-per-track must be > 0");

    let cylinder_sectors = opts.heads as u64 * opts.sectors_per_track as u64;
    let cylinder_bytes = cylinder_sectors * SECTOR_SIZE;

    // VOLHDR region: fixed 2 MiB, rounded up to a whole cylinder.
    let volhdr_sectors = VOLHDR_REGION_BYTES.div_ceil(cylinder_bytes) * cylinder_sectors;

    // Total disk rounded up to a whole cylinder, and at least big enough for
    // the VOLHDR region plus a minimum EFS partition.
    let requested_sectors = opts.size_bytes.div_ceil(SECTOR_SIZE);
    let min_disk_sectors = volhdr_sectors + MIN_EFS_SECTORS;
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

    let efs_first_sector = volhdr_sectors;
    let efs_sectors = disk_sectors - volhdr_sectors;
    let efs_bytes = efs_sectors * SECTOR_SIZE;
    ensure!(
        efs_sectors >= MIN_EFS_SECTORS,
        "EFS partition would be only {efs_bytes} bytes (< 32 KiB); increase --size"
    );

    // --- Build the volume header ------------------------------------------
    let mut partitions: Vec<SgiPartitionEntry> = (0..SGI_NUM_PARTITIONS)
        .map(|_| SgiPartitionEntry {
            blocks: 0,
            first: 0,
            partition_type_raw: 0,
        })
        .collect();
    partitions[SLOT_EFS_ROOT] = SgiPartitionEntry {
        blocks: efs_sectors as u32,
        first: efs_first_sector as u32,
        partition_type_raw: SgiPartitionType::Efs.as_u32(),
    };
    partitions[SLOT_VOLHDR] = SgiPartitionEntry {
        blocks: volhdr_sectors as u32,
        first: 0,
        partition_type_raw: SgiPartitionType::VolHdr.as_u32(),
    };
    partitions[SLOT_VOLUME] = SgiPartitionEntry {
        blocks: disk_sectors as u32,
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
        root_part_num: SLOT_EFS_ROOT as u16,
        swap_part_num: 0, // no separate swap partition on a data disk
        device_parameters: SgiDeviceParameters::for_geometry(
            disk_cyls as u32,
            opts.heads,
            opts.sectors_per_track,
        ),
        bootfile: "/unix".to_string(),
        volume_directory,
        partitions,
        checksum: 0,
        checksum_valid: true,
    };
    let sector0 = vh.to_bytes(); // 512 bytes, checksum zero-summed

    // --- Assemble the disk ------------------------------------------------
    let mut image = vec![0u8; disk_sectors as usize * SECTOR_SIZE as usize];
    image[..sector0.len()].copy_from_slice(&sector0);

    let efs =
        create_blank_efs(efs_bytes, &opts.name).context("formatting the EFS root partition")?;
    debug_assert_eq!(efs.len() as u64, efs_bytes, "EFS image size mismatch");
    let efs_off = efs_first_sector as usize * SECTOR_SIZE as usize;
    image[efs_off..efs_off + efs.len()].copy_from_slice(&efs);

    let layout = SgiHddLayout {
        disk_sectors,
        disk_bytes: disk_sectors * SECTOR_SIZE,
        cylinders: disk_cyls as u32,
        heads: opts.heads,
        sectors_per_track: opts.sectors_per_track,
        cylinder_sectors,
        volhdr_sectors,
        efs_first_sector,
        efs_sectors,
    };
    Ok((image, layout))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::sgi::SGI_TYPE_BYTE_EFS;
    use crate::partition::PartitionTable;
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
        assert_eq!(layout.efs_first_sector, 5040);
        assert_eq!(layout.efs_sectors, 102 * 1008 - 5040);
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
        assert_eq!(p0.first as u64, layout.efs_first_sector);
        assert_eq!(p0.blocks as u64, layout.efs_sectors);

        let p8 = &vh.partitions[8];
        assert_eq!(p8.partition_type(), SgiPartitionType::VolHdr);
        assert_eq!(p8.first, 0);
        assert_eq!(p8.blocks as u64, layout.volhdr_sectors);

        let p10 = &vh.partitions[10];
        assert_eq!(p10.partition_type(), SgiPartitionType::Volume);
        assert_eq!(p10.first, 0);
        assert_eq!(p10.blocks as u64, layout.disk_sectors);
    }

    /// `PartitionTable::detect` recognizes the disk as SGI, surfaces the EFS
    /// root at the right offset (wrappers filtered), and the EFS filesystem at
    /// that offset opens with a readable, empty root directory.
    #[test]
    fn detects_and_opens_the_efs_partition() {
        let (img, layout) =
            build_sgi_efs_hdd(&SgiHddOptions::new(16 * 1024 * 1024, "DATA")).unwrap();

        let table = PartitionTable::detect(&mut Cursor::new(&img[..])).expect("SGI detected");
        assert_eq!(table.type_name(), "SGI");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1, "only the EFS partition is browsable");
        let efs = &parts[0];
        assert_eq!(efs.partition_type_byte, SGI_TYPE_BYTE_EFS);
        assert_eq!(efs.start_lba, layout.efs_first_sector);

        let offset = efs.start_lba * 512;
        let mut fs = crate::fs::open_filesystem(
            Cursor::new(img.clone()),
            offset,
            efs.partition_type_byte,
            None,
        )
        .expect("open EFS at partition offset");
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
        assert!(layout.efs_sectors >= MIN_EFS_SECTORS);
        assert_eq!(layout.disk_sectors % layout.cylinder_sectors, 0);
    }
}
