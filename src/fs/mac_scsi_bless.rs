//! Make an APM disk SCSI-bootable on a classic-Mac ROM by installing an
//! Apple SCSI driver and a valid Driver Descriptor Record (DDR).
//!
//! A Macintosh ROM (e.g. Quadra 800) finds a SCSI disk's driver through the
//! DDR's driver-descriptor map (block 0: `sbDrvrCount` + `ddBlock`/`ddSize`/
//! `ddType`), which points at an `Apple_Driver43` partition. Disks built
//! from scratch — e.g. via [`crate::fs::hfs_clone::emit_apm_disk_with_hfs`]
//! from a driverless source — have `sbDrvrCount = 0` and no driver partition,
//! so the ROM never registers a drive. This module fills that gap.
//!
//! ## What it is NOT
//!
//! This installs the *driver* so the ROM can read the disk. It does **not**
//! touch the HFS boot blocks; whether the volume actually boots a System (or
//! a custom `bbEntry`) is independent of driver registration.
//!
//! ## Ground truth, not the spec's guesses
//!
//! The field values written here were read off real Apple-formatted disks
//! (Apple HD SC / BlueSCSI output), not derived from documentation:
//!
//! * The working mechanism is the **DDR driver map** plus the
//!   `Apple_Driver43` partition entry's boot fields. `pmBootAddr`,
//!   `pmBootEntry` and `pmLgBootStart` are **0** on every working disk we
//!   examined — contrary to a literal reading of *Inside Macintosh: Devices*.
//! * `pmBootCksum` **is** validated by the Quadra 800 ROM (corrupting it
//!   drops the disk back to the flashing-`?` state), but the value is not a
//!   recomputable checksum of the on-disk driver bytes — it is opaque
//!   metadata that ships *with* a given driver. So we copy the whole driver
//!   package (code + `pmBootSize`/`pmBootCksum`/`pmProcessor`/`pmPad` + DDR
//!   `ddType`/`ddSize`) verbatim from a donor or the bundled blob, and never
//!   recompute anything.
//!
//! Partition *data* is never moved; the operation is idempotent.

use std::io::{Read, Seek, SeekFrom, Write};

use crate::fs::filesystem::FilesystemError;
use crate::partition::apm::{Apm, ApmPartitionEntry, DriverInfo};

const APM_BLOCK_SIZE: u64 = 512;
const APM_ENTRY_SIGNATURE: u16 = 0x504D;

/// Bundled, known-good Mac SCSI driver: 19 blocks (9728 bytes), the
/// `ddSize`-extent of the driver found byte-identical in the `Apple_Driver43`
/// partition of many independently-formatted disks (the driver that
/// BlueSCSI/RaSCSI and common disk-imaging tools install). Paired with its
/// precomputed `pmPad` driver-descriptor blob and `pmBoot*` values, all
/// carried verbatim (the ROM validates `pmBootCksum`; see the module docs).
///
/// PROVENANCE / LICENSING: this is a redistributed third-party SCSI driver
/// (like `x68k_hero_soft_boot.bin` elsewhere in the tree). Verified to load on
/// the real Quadra 800 ROM under QEMU. Users who prefer not to rely on the
/// bundled blob can point `--driver-from <donor.hda>` at any Apple-formatted
/// disk they own to copy that disk's driver instead.
const BUILTIN_DRIVER: &[u8] = include_bytes!("apple_scsi_driver.bin");
const BUILTIN_DRIVER_PAD: &[u8] = include_bytes!("apple_scsi_driver_pad.bin");
const BUILTIN_BOOT_SIZE: u32 = 9392;
const BUILTIN_BOOT_CKSUM: u32 = 0xf624;
const BUILTIN_DD_TYPE: u16 = 1;
const DRIVER_PROCESSOR: &str = "68000";

/// `pmPartStatus` for a bootable driver entry: valid | allocated | in-use |
/// bootable | readable | writable | "boot code in pmPad" (bits 0..=6 = 0x7f),
/// matching real `Apple_Driver43` entries.
const DRIVER_STATUS: u32 = 0x7f;

/// `pmPad` is the 376-byte tail (offset 136..512) of a partition-map entry.
const PMPAD_LEN: usize = 376;

/// Partition-type strings that denote a SCSI/ATA driver partition.
fn is_driver_type(t: &str) -> bool {
    matches!(
        t,
        "Apple_Driver"
            | "Apple_Driver43"
            | "Apple_Driver43_CD"
            | "Apple_Driver_ATA"
            | "Apple_Driver_ATAPI"
            | "Apple_Patches"
            | "Apple_FWDriver"
    )
}

/// Round a byte length up to a whole number of 512-byte blocks.
fn blocks_for(bytes: usize) -> u32 {
    (bytes as u64).div_ceil(APM_BLOCK_SIZE) as u32
}

/// A complete, self-consistent SCSI-driver package: the raw driver image plus
/// the APM/DDR metadata the ROM needs to load and validate it. Every field is
/// carried verbatim from its source (a donor disk or the bundled blob); none
/// is recomputed (see the module docs on `pmBootCksum`).
#[derive(Debug, Clone)]
pub struct MacScsiDriver {
    /// Raw driver image written into the driver partition. Length is a
    /// 512-byte multiple (the ROM reads `ddSize` whole blocks).
    pub code: Vec<u8>,
    /// `pmBootSize` — driver code length in bytes.
    pub boot_size: u32,
    /// `pmBootCksum` — opaque, ROM-validated. Copied verbatim, not recomputed.
    pub boot_cksum: u32,
    /// `pmProcessor` — e.g. `"68000"`.
    pub processor: String,
    /// `pmPad` (376 bytes) — driver-descriptor blob some ROMs require.
    pub pad: Vec<u8>,
    /// `ddType` — DDR driver kind (1 = Mac OS).
    pub dd_type: u16,
    /// Partition-type string to stamp when creating a driver partition.
    pub partition_type: String,
}

impl MacScsiDriver {
    /// The bundled known-good Apple SCSI driver. Works out of the box; no
    /// donor disk required.
    pub fn builtin() -> Self {
        MacScsiDriver {
            code: BUILTIN_DRIVER.to_vec(),
            boot_size: BUILTIN_BOOT_SIZE,
            boot_cksum: BUILTIN_BOOT_CKSUM,
            processor: DRIVER_PROCESSOR.to_string(),
            pad: BUILTIN_DRIVER_PAD.to_vec(),
            dd_type: BUILTIN_DD_TYPE,
            partition_type: "Apple_Driver43".to_string(),
        }
    }

    /// Extract the driver package from a donor Apple-formatted disk's first
    /// `Apple_Driver*` partition, carrying its boot metadata verbatim.
    pub fn from_donor<R: Read + Seek>(donor: &mut R) -> Result<Self, FilesystemError> {
        let apm = Apm::parse(donor)
            .map_err(|e| FilesystemError::InvalidData(format!("donor is not an APM disk: {e}")))?;
        let entry = apm
            .entries
            .iter()
            .find(|e| is_driver_type(&e.partition_type))
            .ok_or_else(|| {
                FilesystemError::NotFound("donor has no Apple_Driver* partition".to_string())
            })?
            .clone();

        // ddType / ddSize from the DDR descriptor that points at this driver,
        // falling back to sensible defaults.
        let dd = apm
            .ddr
            .driver_info
            .iter()
            .find(|d| d.block == entry.start_block);
        let dd_type = dd.map(|d| d.kind).unwrap_or(1);
        let read_blocks = dd
            .map(|d| d.size as u32)
            .filter(|&s| s > 0)
            .unwrap_or(entry.block_count)
            .min(entry.block_count)
            .max(1);

        // Read the driver extent, tolerating a donor file shorter than the
        // declared partition (zero-fill the remainder).
        let mut code = vec![0u8; read_blocks as usize * APM_BLOCK_SIZE as usize];
        donor.seek(SeekFrom::Start(entry.start_block as u64 * APM_BLOCK_SIZE))?;
        let mut filled = 0usize;
        while filled < code.len() {
            let n = donor.read(&mut code[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }

        let boot_size = if entry.boot_size > 0 {
            entry.boot_size
        } else {
            code.len() as u32
        };
        let mut pad = entry.pad.clone();
        pad.resize(PMPAD_LEN, 0);
        Ok(MacScsiDriver {
            code,
            boot_size,
            boot_cksum: entry.boot_checksum,
            processor: if entry.processor.is_empty() {
                DRIVER_PROCESSOR.to_string()
            } else {
                entry.processor.clone()
            },
            pad,
            dd_type,
            partition_type: entry.partition_type.clone(),
        })
    }

    /// Wrap arbitrary raw driver bytes (advanced). `pmBootCksum` is unknown for
    /// an arbitrary driver, so it is set to 0 — the ROM is likely to reject it
    /// unless that ROM skips checksum verification when the field is 0. Prefer
    /// [`MacScsiDriver::builtin`] or [`MacScsiDriver::from_donor`].
    pub fn from_raw(bytes: &[u8]) -> Self {
        let mut code = bytes.to_vec();
        let padded = blocks_for(code.len()) as usize * APM_BLOCK_SIZE as usize;
        code.resize(padded, 0);
        MacScsiDriver {
            boot_size: bytes.len() as u32,
            boot_cksum: 0,
            processor: DRIVER_PROCESSOR.to_string(),
            pad: BUILTIN_DRIVER_PAD.to_vec(),
            dd_type: 1,
            partition_type: "Apple_Driver43".to_string(),
            code,
        }
    }

    /// Driver span in 512-byte blocks (`ddSize` and the partition span needed).
    pub fn size_blocks(&self) -> u32 {
        blocks_for(self.code.len())
    }
}

/// Tunables for [`bless_apm_disk`].
#[derive(Debug, Clone, Default)]
pub struct BlessOptions {
    /// Force `pmBootCksum = 0`. Some ROMs skip checksum verification then;
    /// useful with [`MacScsiDriver::from_raw`].
    pub force_cksum_zero: bool,
}

/// Outcome of a successful bless.
#[derive(Debug, Clone)]
pub struct BlessReport {
    /// Block where the driver code now lives (`ddBlock`).
    pub driver_block: u32,
    /// Driver span in 512-byte blocks (`ddSize`).
    pub driver_blocks: u32,
    /// True if a new `Apple_Driver43` partition was created in free space.
    pub created_partition: bool,
    /// True if a driver descriptor for this block was already registered
    /// (the operation refreshed it rather than adding it).
    pub was_already_present: bool,
}

/// Install `driver` into the APM disk in `disk`, making it SCSI-bootable.
///
/// * Requires an APM disk with 512-byte blocks.
/// * If an `Apple_Driver*` partition already exists, the driver is written
///   into it (it must be large enough); otherwise a new `Apple_Driver43`
///   partition is created in the free space between the partition map and the
///   first data partition.
/// * Partition *data* is never moved. Re-running is safe (idempotent).
pub fn bless_apm_disk<T>(
    disk: &mut T,
    disk_len: u64,
    driver: &MacScsiDriver,
    opts: &BlessOptions,
) -> Result<BlessReport, FilesystemError>
where
    T: Read + Write + Seek,
{
    let mut apm = Apm::parse(disk)
        .map_err(|e| FilesystemError::InvalidData(format!("not an APM disk: {e}")))?;
    if apm.ddr.block_size != 512 {
        return Err(FilesystemError::Unsupported(format!(
            "APM block size {} is unsupported (only 512-byte blocks)",
            apm.ddr.block_size
        )));
    }
    if driver.code.is_empty() {
        return Err(FilesystemError::InvalidData(
            "driver image is empty".to_string(),
        ));
    }
    let needed_blocks = driver.size_blocks();

    // Locate or create the driver partition.
    let (driver_block, created) = match apm
        .entries
        .iter()
        .position(|e| is_driver_type(&e.partition_type))
    {
        Some(idx) => {
            let e = &apm.entries[idx];
            if e.block_count < needed_blocks {
                return Err(FilesystemError::InvalidData(format!(
                    "existing {} partition is {} blocks but the driver needs {}; \
                     rebuild the disk with a larger driver slot",
                    e.partition_type, e.block_count, needed_blocks
                )));
            }
            (e.start_block, false)
        }
        None => {
            let block = find_driver_gap(&apm, needed_blocks)?;
            insert_driver_entry(&mut apm, block, needed_blocks, &driver.partition_type);
            (block, true)
        }
    };

    let was_already_present =
        apm.ddr.driver_count >= 1 && apm.ddr.driver_info.iter().any(|d| d.block == driver_block);

    // Stamp the driver partition entry's boot fields.
    let idx = apm
        .entries
        .iter()
        .position(|e| e.start_block == driver_block && is_driver_type(&e.partition_type))
        .expect("driver entry must exist after locate/create");
    {
        let e = &mut apm.entries[idx];
        e.boot_start = 0;
        e.boot_size = driver.boot_size;
        e.boot_load = 0;
        e.boot_entry = 0;
        e.boot_checksum = if opts.force_cksum_zero {
            0
        } else {
            driver.boot_cksum
        };
        e.processor = driver.processor.clone();
        let mut pad = driver.pad.clone();
        pad.resize(PMPAD_LEN, 0);
        e.pad = pad;
        e.status |= DRIVER_STATUS;
    }

    // One DDR driver descriptor pointing at the driver.
    apm.ddr.driver_count = 1;
    apm.ddr.driver_info = vec![DriverInfo {
        block: driver_block,
        size: needed_blocks as u16,
        kind: driver.dd_type,
    }];

    // Guard: the rewritten DDR + partition map must not collide with the
    // driver or any partition data (it never should, given placement above).
    let apm_region_blocks = 1 + apm.entries.len() as u32; // DDR + entries
    if driver_block < apm_region_blocks {
        return Err(FilesystemError::InvalidData(format!(
            "driver block {driver_block} overlaps the {apm_region_blocks}-block partition map"
        )));
    }

    // Write the driver bytes (does not disturb other partitions).
    let off = driver_block as u64 * APM_BLOCK_SIZE;
    if off + driver.code.len() as u64 > disk_len {
        return Err(FilesystemError::InvalidData(
            "driver would extend past the end of the disk image".to_string(),
        ));
    }
    disk.seek(SeekFrom::Start(off))?;
    disk.write_all(&driver.code)?;

    // Rewrite DDR + partition map (blocks 0..=N — all before any data).
    let apm_blocks = apm.build_apm_blocks(None);
    disk.seek(SeekFrom::Start(0))?;
    disk.write_all(&apm_blocks)?;
    disk.flush()?;

    Ok(BlessReport {
        driver_block,
        driver_blocks: needed_blocks,
        created_partition: created,
        was_already_present,
    })
}

/// Find a free block run before the first data partition large enough to hold
/// the driver. Placement is immediately after the partition-map region —
/// respecting both the blocks the DDR+entries physically occupy and the
/// region the `Apple_partition_map` self-entry declares (real Apple disks
/// reserve 63 blocks there).
fn find_driver_gap(apm: &Apm, needed_blocks: u32) -> Result<u32, FilesystemError> {
    // After inserting one entry, the DDR + entries occupy blocks 0..=(entries+1),
    // so the first physically-free block is entries.len()+2.
    let first_free = apm.entries.len() as u32 + 2;
    // End of the region the partition map declares for itself.
    let map_end = apm
        .entries
        .iter()
        .find(|e| e.partition_type == "Apple_partition_map")
        .map(|e| e.start_block.saturating_add(e.block_count))
        .unwrap_or(first_free);
    let candidate = first_free.max(map_end);
    // First block claimed by real partition data (ignore the map self-entry
    // and free space).
    let first_data = apm
        .entries
        .iter()
        .filter(|e| e.is_data_partition())
        .map(|e| e.start_block)
        .min()
        .unwrap_or(u32::MAX);
    if candidate.saturating_add(needed_blocks) <= first_data {
        Ok(candidate)
    } else {
        Err(FilesystemError::InvalidData(format!(
            "no free space before the first partition (block {first_data}) for a \
             {needed_blocks}-block driver; rebuild the disk with a driver slot \
             (e.g. emit_apm_disk_with_hfs from a donor that has one)"
        )))
    }
}

/// Insert a new driver partition-map entry right after the `Apple_partition_map`
/// self-entry and re-stamp every entry's `pmMapBlkCnt`.
fn insert_driver_entry(apm: &mut Apm, block: u32, blocks: u32, partition_type: &str) {
    let new_count = apm.entries.len() as u32 + 1;
    let entry = ApmPartitionEntry {
        signature: APM_ENTRY_SIGNATURE,
        map_entries: new_count,
        start_block: block,
        block_count: blocks,
        name: "Macintosh".to_string(),
        partition_type: partition_type.to_string(),
        data_start: 0,
        data_count: blocks,
        status: DRIVER_STATUS,
        boot_start: 0,
        boot_size: 0,
        boot_load: 0,
        boot_entry: 0,
        boot_checksum: 0,
        processor: String::new(),
        pad: Vec::new(),
    };
    let insert_at = apm
        .entries
        .iter()
        .position(|e| e.partition_type == "Apple_partition_map")
        .map(|i| i + 1)
        .unwrap_or(apm.entries.len());
    apm.entries.insert(insert_at, entry);
    for e in &mut apm.entries {
        e.map_entries = new_count;
    }
    apm.map_entry_count = new_count;
    // Keep the partition-map self-entry's declared span covering every entry
    // block (it must be >= the entry count; real disks declare 63).
    if let Some(map_self) = apm
        .entries
        .iter_mut()
        .find(|e| e.partition_type == "Apple_partition_map")
    {
        if map_self.block_count < new_count {
            map_self.block_count = new_count;
            map_self.data_count = new_count;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::apm::build_minimal_apm;
    use std::io::Cursor;

    /// Build a driverless APM disk: partition map + one Apple_HFS at block 64.
    fn driverless_disk(total_blocks: u32, hfs_blocks: u32) -> Vec<u8> {
        let apm = build_minimal_apm(
            &[("Apple_HFS".to_string(), 64, hfs_blocks)],
            512,
            total_blocks,
        );
        let blocks = apm.build_apm_blocks(Some(total_blocks));
        let mut disk = vec![0u8; total_blocks as usize * 512];
        disk[..blocks.len()].copy_from_slice(&blocks);
        disk
    }

    #[test]
    fn bless_driverless_creates_partition_and_ddr() {
        let total = 200u32;
        let mut cur = Cursor::new(driverless_disk(total, 100));
        let driver = MacScsiDriver::builtin();
        let report = bless_apm_disk(
            &mut cur,
            total as u64 * 512,
            &driver,
            &BlessOptions::default(),
        )
        .unwrap();
        assert!(report.created_partition);
        assert!(!report.was_already_present);
        assert_eq!(report.driver_blocks, 19);

        cur.set_position(0);
        let apm = Apm::parse(&mut cur).unwrap();
        assert_eq!(apm.ddr.driver_count, 1);
        assert_eq!(apm.ddr.driver_info.len(), 1);
        assert_eq!(apm.ddr.driver_info[0].block, report.driver_block);
        assert_eq!(apm.ddr.driver_info[0].size, 19);
        assert_eq!(apm.ddr.driver_info[0].kind, 1);

        let drv = apm
            .entries
            .iter()
            .find(|e| e.partition_type == "Apple_Driver43")
            .unwrap();
        assert_eq!(drv.start_block, report.driver_block);
        assert_eq!(drv.boot_size, 9392);
        assert_eq!(drv.boot_checksum, 0xf624);
        assert_eq!(drv.processor, "68000");
        assert_eq!(drv.boot_load, 0);
        assert_eq!(drv.boot_entry, 0);
        assert_eq!(drv.status & 0x7f, 0x7f);
        // pmPad descriptor preserved.
        assert_eq!(&drv.pad[..14], &BUILTIN_DRIVER_PAD[..14]);

        // Driver bytes landed at ddBlock.
        let off = report.driver_block as usize * 512;
        let disk = cur.into_inner();
        assert_eq!(&disk[off..off + 4], &BUILTIN_DRIVER[..4]);

        // HFS partition entry untouched.
        let hfs = Apm::parse(&mut Cursor::new(disk))
            .unwrap()
            .entries
            .into_iter()
            .find(|e| e.partition_type == "Apple_HFS")
            .unwrap();
        assert_eq!(hfs.start_block, 64);
        assert_eq!(hfs.block_count, 100);
    }

    #[test]
    fn bless_is_idempotent() {
        let total = 200u32;
        let mut cur = Cursor::new(driverless_disk(total, 100));
        let driver = MacScsiDriver::builtin();
        let r1 = bless_apm_disk(
            &mut cur,
            total as u64 * 512,
            &driver,
            &BlessOptions::default(),
        )
        .unwrap();
        cur.set_position(0);
        let r2 = bless_apm_disk(
            &mut cur,
            total as u64 * 512,
            &driver,
            &BlessOptions::default(),
        )
        .unwrap();
        assert!(r1.created_partition);
        assert!(!r2.created_partition);
        assert!(r2.was_already_present);
        assert_eq!(r1.driver_block, r2.driver_block);

        cur.set_position(0);
        let apm = Apm::parse(&mut cur).unwrap();
        assert_eq!(apm.ddr.driver_count, 1);
        assert_eq!(
            apm.entries
                .iter()
                .filter(|e| e.partition_type == "Apple_Driver43")
                .count(),
            1
        );
    }

    #[test]
    fn bless_fills_existing_empty_driver_partition() {
        // Map + empty Apple_Driver43 (32 blocks @ 64) + HFS (@ 96).
        let total = 400u32;
        let apm = build_minimal_apm(
            &[
                ("Apple_Driver43".to_string(), 64, 32),
                ("Apple_HFS".to_string(), 96, 100),
            ],
            512,
            total,
        );
        let blocks = apm.build_apm_blocks(Some(total));
        let mut disk = vec![0u8; total as usize * 512];
        disk[..blocks.len()].copy_from_slice(&blocks);
        let mut cur = Cursor::new(disk);

        let driver = MacScsiDriver::builtin();
        let report = bless_apm_disk(
            &mut cur,
            total as u64 * 512,
            &driver,
            &BlessOptions::default(),
        )
        .unwrap();
        assert!(!report.created_partition);
        assert_eq!(report.driver_block, 64);

        cur.set_position(0);
        let apm = Apm::parse(&mut cur).unwrap();
        assert_eq!(apm.ddr.driver_info[0].block, 64);
        let drv = apm
            .entries
            .iter()
            .find(|e| e.partition_type == "Apple_Driver43")
            .unwrap();
        assert_eq!(drv.boot_checksum, 0xf624);
    }

    #[test]
    fn force_cksum_zero_zeros_the_field() {
        let total = 200u32;
        let mut cur = Cursor::new(driverless_disk(total, 100));
        let driver = MacScsiDriver::builtin();
        bless_apm_disk(
            &mut cur,
            total as u64 * 512,
            &driver,
            &BlessOptions {
                force_cksum_zero: true,
            },
        )
        .unwrap();
        cur.set_position(0);
        let apm = Apm::parse(&mut cur).unwrap();
        let drv = apm
            .entries
            .iter()
            .find(|e| e.partition_type == "Apple_Driver43")
            .unwrap();
        assert_eq!(drv.boot_checksum, 0);
    }

    #[test]
    fn from_raw_pads_to_block_and_sets_boot_size() {
        let raw = vec![0xABu8; 1000];
        let d = MacScsiDriver::from_raw(&raw);
        assert_eq!(d.boot_size, 1000);
        assert_eq!(d.code.len(), 1024); // padded to 2 blocks
        assert_eq!(d.size_blocks(), 2);
        assert_eq!(d.boot_cksum, 0);
    }

    #[test]
    fn rejects_non_apm() {
        let mut cur = Cursor::new(vec![0u8; 4096]);
        let err = bless_apm_disk(
            &mut cur,
            4096,
            &MacScsiDriver::builtin(),
            &BlessOptions::default(),
        )
        .unwrap_err();
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    #[test]
    fn rejects_no_room_for_driver() {
        // HFS starts at block 5 — no room after the 3-block map for 19 blocks.
        let total = 200u32;
        let apm = build_minimal_apm(&[("Apple_HFS".to_string(), 5, 100)], 512, total);
        let blocks = apm.build_apm_blocks(Some(total));
        let mut disk = vec![0u8; total as usize * 512];
        disk[..blocks.len()].copy_from_slice(&blocks);
        let mut cur = Cursor::new(disk);
        let err = bless_apm_disk(
            &mut cur,
            total as u64 * 512,
            &MacScsiDriver::builtin(),
            &BlessOptions::default(),
        )
        .unwrap_err();
        assert!(matches!(err, FilesystemError::InvalidData(_)));
    }

    #[test]
    fn donor_round_trip() {
        // Build a donor with a driver, extract it, bless a fresh disk, compare.
        let total = 400u32;
        let mut cur = Cursor::new(driverless_disk(total, 100));
        let builtin = MacScsiDriver::builtin();
        bless_apm_disk(
            &mut cur,
            total as u64 * 512,
            &builtin,
            &BlessOptions::default(),
        )
        .unwrap();
        let donor_bytes = cur.into_inner();

        let mut donor = Cursor::new(donor_bytes);
        let extracted = MacScsiDriver::from_donor(&mut donor).unwrap();
        assert_eq!(extracted.boot_cksum, 0xf624);
        assert_eq!(extracted.boot_size, 9392);
        assert_eq!(extracted.dd_type, 1);
        assert_eq!(&extracted.code[..4], &BUILTIN_DRIVER[..4]);
    }
}
