//! Build self-bootable Sharp X68000 HDD images (SASI / SCSI) with an
//! optional donor-floppy clone.
//!
//! Used by both [`rb-cli new-x68k-hdd`](crate::cli) and the
//! `examples/build_x68k_hdd` development scaffold. The builder produces a
//! disk that the Sharp IPL ROM recognises and chains into:
//!
//! - **Byte-0 IPL stub** — halt-loop ([`IplStub::Halt`]) or banner-print
//!   ([`IplStub::Print`]) variants. See [`crate::partition::x68k_ipl`].
//! - **X68K partition table** at the byte offset matching the controller
//!   convention (`0x400` SASI, `0x800` SCSI). One `Human   ` partition
//!   starting at logical sector 64.
//! - **Human68k FAT12/16 partition body** — either three seed text files
//!   (`HELLO.TXT`, `MISTER.TXT`, `README.TXT`) for engine-validation, or a
//!   full clone of every file + directory from a donor Human68k system
//!   floppy when `system_disk` is `Some`.
//!
//! ### Donor-floppy notes
//!
//! `system_disk` accepts a flat `.img` / `.dim` / `.D88` / `.xdf` / `.hdm`
//! — the floppy-container layer at
//! [`crate::rbformats::containers::decode_floppy_container_file`]
//! auto-detects and decodes to a flat sector stream. Standard 1.2 MB
//! Human68k 3.x install floppies (e.g. `Human 68k v3.02 (Sharp - Hudson).dim`)
//! work directly.
//!
//! ### Partition boot sector — `boot_sector_donor`
//!
//! Optional second donor (`boot_sector_donor`) extracts the Sharp-format
//! partition boot sector from a *real* Sharp X68000 SCSI HDD and overlays
//! it onto the generated partition. Result: HDD self-boots straight to
//! `C:>` on every power-on, no FDD0 floppy ever required.
//!
//! **Well-known donor**: `hd0.hds` — the canonical 100 MB Sharp /
//! Keisoku Giken SCSI HDD image shipped widely on the X68000 abandonware
//! scene (file size **104,857,600 bytes** = 100 × 1024 × 1024, SHA1
//! `5b2c8c5a...`-class images circulate; the exact bytes vary but the
//! Sharp IPL Copyright 1990 SHARP boot block at byte `0x8000` is
//! standardised). Searches for the literal filename **`hd0.hds`** turn
//! it up on most retro-archive sites. Other common names: `HD0.HDS`,
//! `system.hds`, `Human68k.hdf` (rare).
//!
//! At build time, [`extract_partition_boot_sector`] opens the donor,
//! locates the first Human68k partition via the X68K table at byte
//! `0x800` (SCSI convention), and reads the donor's first partition
//! sector (1024 bytes for SCSI). Sharp's boot CODE (bytes `0x00..0x12`
//! BRA.S + OEM marker and bytes `0x22..end`, including the strings and
//! the actual 68000 instructions) is written verbatim over the first
//! sector of the generated output partition. The Sharp/KG **BPB region**
//! at offsets `0x12..0x22` is rewritten by
//! [`patch_sharp_kg_bpb_from_pc_bpb`] with the geometry of *our*
//! freshly-formatted partition — so the donor's boot code reads the
//! right FAT/root/data sectors regardless of how the donor was sized.
//!
//! License footprint: the boot-sector bytes flow user → user — they
//! never live in the rusty-backup repo or shipping binaries. The user
//! already owns their donor HDD; the builder just orchestrates the
//! transfer (and patches it to match our partition's geometry). Same
//! legal pattern as `--system-disk` (you provide a Human68k floppy, we
//! extract files from it) or as running Sharp's own `SWITCH.X` tool
//! (which writes the same bytes from inside Human68k).
//!
//! **No size constraint**: the BPB-patch step decouples our output
//! partition's size from the donor's. `--size 32M` with the 100 MB
//! `hd0.hds` donor works exactly the same as `--size 100M` — the
//! donor's boot code reads our partition's actual BPB. The only
//! limit is the Sharp/KG BPB's u8 `sectors_per_fat` field (offset
//! `0x1D`), which caps the partition at roughly 512 MiB for FAT16.
//!
//! **SCSI only today**: SASI donors (256-byte sectors) aren't supported
//! here yet — the validated SASI Human68k HDD donors in the wild are
//! rare, and our existing test fixtures (`Bomberman.hdf`) use a custom
//! Hudson Soft IPL rather than the standard Sharp/KG one. Fall back to
//! the `SWITCH.X` workflow (no `--boot-sector-donor`) for SASI output.
//!
//! ### Without `boot_sector_donor` — the SWITCH.X fallback
//!
//! When `boot_sector_donor` is `None`, the generated HDD has a working
//! FAT partition + IPL stub but no partition boot sector. The user
//! boots the same `--system-disk` donor floppy from FDD0 once and runs
//! `A:\BIN\SWITCH.X /HD` — SWITCH.X writes the Sharp boot sector at
//! that point. One manual step, no donor HDD required.

use std::io::Cursor;
use std::path::Path;

use byteorder::{BigEndian, ByteOrder};

use crate::fs::entry::{EntryType, FileEntry};
use crate::fs::fat::create_blank_fat_with_sector_size;
use crate::fs::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
};
use crate::fs::human68k::Human68kFilesystem;
use crate::partition::x68k::{
    X68kPartitionTable, X68K_ENTRY_SIZE, X68K_FIRST_PARTITION_SECTOR, X68K_MAGIC,
    X68K_MAX_PARTITIONS, X68K_TABLE_HEADER_SIZE,
};
use crate::partition::x68k_ipl::{
    build_sasi_boot_block, build_scsi_boot_block, IplStub, SASI_BOOT_BLOCK_BYTES,
    SCSI_BOOT_BLOCK_BYTES,
};
use crate::rbformats::containers::decode_floppy_container_file;

/// SASI logical sector size (matches real Sharp / Hudson SASI HDDs +
/// `Bomberman.hdf` reference).
const SASI_SECTOR_SIZE: u64 = 256;

/// SCSI logical sector size (matches the `SCSI_NetBSD.hds` /
/// BlueSCSI / ZuluSCSI / Keisoku Giken convention).
const SCSI_SECTOR_SIZE: u64 = 1024;

/// Geometry/capacity descriptor that follows the `X68SCSI1` signature on
/// generated SCSI HDDs. Bytes lifted from `SCSI_NetBSD.hds` byte
/// `0x08..0x10`. Real Sharp HDDs vary the inner fields per drive; this
/// is the canonical NetBSD-installer value that MAME accepts.
const SCSI_DESCRIPTOR: [u8; 8] = [0x02, 0x00, 0x00, 0x1d, 0xaf, 0xff, 0x01, 0x00];

/// Which Sharp HDD controller convention to emit.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HddVariant {
    /// 256-byte logical sectors. Byte 0 carries an IPL stub directly
    /// (no signature header). Table at byte `0x400`. Matches real
    /// Hudson Soft self-bootable game discs (e.g. `Bomberman.hdf`) and
    /// BlueSCSI in SASI mode.
    Sasi,
    /// 1024-byte logical sectors. Byte 0 carries the `X68SCSI1` +
    /// Keisoku Giken signature header; IPL stub at byte `0x400`; table
    /// at byte `0x800`. Matches BlueSCSI / ZuluSCSI / modern dev
    /// setups + the `SCSI_NetBSD.hds` reference fixture.
    Scsi,
}

impl HddVariant {
    fn sector_size(self) -> u64 {
        match self {
            Self::Sasi => SASI_SECTOR_SIZE,
            Self::Scsi => SCSI_SECTOR_SIZE,
        }
    }

    fn boot_block_bytes(self) -> usize {
        match self {
            Self::Sasi => SASI_BOOT_BLOCK_BYTES,
            Self::Scsi => SCSI_BOOT_BLOCK_BYTES,
        }
    }

    /// Human-readable name for log output.
    pub fn name(self) -> &'static str {
        match self {
            Self::Sasi => "SASI",
            Self::Scsi => "SCSI",
        }
    }
}

/// Summary of a successful build — counts for the caller to render.
#[derive(Debug, Clone)]
pub struct BuildSummary {
    pub variant: HddVariant,
    pub total_bytes: u64,
    pub disk_sectors: u64,
    pub sector_size: u64,
    pub boot_block_bytes: usize,
    /// Number of Human68k partitions carved out (1-8).
    pub partition_count: usize,
    pub partition_start_sector: u64,
    pub partition_start_byte: u64,
    /// Per-partition size in logical sectors (same for every partition;
    /// disk's data area is split evenly).
    pub partition_sectors: u64,
    /// Per-partition size in bytes (= [`Self::partition_sectors`] × [`Self::sector_size`]).
    pub partition_bytes: u64,
    /// Number of files written into the partition (3 for the seed-only
    /// path, however many the donor had otherwise).
    pub files_written: usize,
    /// Number of subdirectories created (0 for seed-only).
    pub dirs_written: usize,
    /// `true` when the partition was populated from a `--system-disk`
    /// donor rather than the engine-validation seed files.
    pub from_donor: bool,
    /// `true` when `--boot-sector-donor` overlaid a real Sharp partition
    /// boot sector onto the output, eliminating the post-build
    /// `SWITCH.X /HD` step.
    pub boot_sector_donor_applied: bool,
}

/// Build a self-bootable X68000 HDD image and write it to `out_path`.
///
/// `size_mib` is the total disk size in MiB. `variant` chooses SASI vs.
/// SCSI byte-0 / sector-size / table-offset conventions. `stub` selects
/// the IPL code variant (halt loop vs. printed banner). `partitions`
/// is the number of Human68k partitions to carve out (1-8, default 1);
/// they split the disk's data area into equal-size slots. If
/// `system_disk` is `Some(p)`, partition 0 is populated by recursively
/// cloning every file + directory from the donor at `p` (decoded
/// through the floppy-container layer if it's `.dim`/`.D88`/`.xdf`/
/// `.hdm`); other partitions stay blank. Without `system_disk`,
/// partition 0 gets three seed text files for engine validation.
///
/// If `boot_sector_donor` is `Some(p)`, the donor's partition boot
/// sector (Sharp IPL Copyright 1990 SHARP) is extracted via
/// [`extract_partition_boot_sector`] and overlaid onto partition 0's
/// first sector, eliminating the post-build `SWITCH.X /HD` step. SCSI
/// only; size must match the donor's partition size. See the module
/// docs for the well-known `hd0.hds` donor pattern.
pub fn build_x68k_hdd(
    out_path: &Path,
    size_mib: u64,
    variant: HddVariant,
    stub: IplStub,
    partitions: usize,
    system_disk: Option<&Path>,
    boot_sector_donor: Option<&Path>,
) -> anyhow::Result<BuildSummary> {
    anyhow::ensure!(
        (1..=X68K_MAX_PARTITIONS).contains(&partitions),
        "partitions must be between 1 and {} (got {partitions})",
        X68K_MAX_PARTITIONS,
    );
    let sector_size = variant.sector_size();
    let boot_block_bytes = variant.boot_block_bytes();
    let part_start_byte = u64::from(X68K_FIRST_PARTITION_SECTOR) * sector_size;
    anyhow::ensure!(
        part_start_byte >= boot_block_bytes as u64,
        "partition body would overlap the boot block (start={part_start_byte}, \
         boot block ends at {boot_block_bytes})",
    );
    let total_bytes = size_mib * 1024 * 1024;
    anyhow::ensure!(
        total_bytes > part_start_byte,
        "size_mib={size_mib} leaves no room for the partition body \
         (boot block occupies {part_start_byte} bytes)",
    );

    let disk_sectors = total_bytes / sector_size;
    let part_start_sector = u64::from(X68K_FIRST_PARTITION_SECTOR);
    let data_sectors = disk_sectors - part_start_sector;
    let per_partition_sectors = data_sectors / partitions as u64;
    let per_partition_bytes = per_partition_sectors * sector_size;
    anyhow::ensure!(
        per_partition_bytes >= 64 * 1024,
        "each of the {partitions} partitions would be only {per_partition_bytes} bytes \
         — bump --size or reduce --partitions",
    );

    // Pre-read the donor tree (if any) before opening any output
    // filesystem to avoid double-borrowing partition bodies.
    let donor_tree: Option<Vec<DonorEntry>> = system_disk.map(read_donor_tree).transpose()?;
    let from_donor = donor_tree.is_some();

    // Create + populate each partition body. Partition 0 gets the
    // donor system tree (or seed files); partitions 1..N stay blank.
    let mut partition_bodies: Vec<Vec<u8>> = Vec::with_capacity(partitions);
    let mut total_files_written: usize = 0;
    let mut total_dirs_written: usize = 0;
    for idx in 0..partitions {
        // Human68k volume labels are limited to 11 ASCII chars; tag
        // each as RBHDD-N so the user can tell partitions apart in
        // multi-partition layouts.
        let label_buf = format!("RBHDD-{}", idx + 1);
        let label = if partitions == 1 {
            Some("RBHDD")
        } else {
            Some(label_buf.as_str())
        };
        // When we'll overlay a SCSI `--boot-sector-donor`, the donor's
        // Sharp/KG BPB expects 1024-byte FAT sectors (real Sharp SCSI
        // HDDs use 1024-B BPB sectors). Force the FAT layout to match
        // so the donor's boot code reads our FAT at the right byte
        // offsets after the BPB-patch step. Otherwise stick with the
        // default 512-byte FAT BPB — it interops cleanly with the
        // X68000 IPL ROM on any sector size, and our existing tests
        // assume it.
        let fat_bps: u32 =
            if matches!(variant, HddVariant::Scsi) && boot_sector_donor.is_some() && idx == 0 {
                1024
            } else {
                512
            };
        let mut body = create_blank_fat_with_sector_size(per_partition_bytes, fat_bps, label)
            .map_err(|e| {
                anyhow::anyhow!(
                    "create_blank_fat on partition {} body (bps={fat_bps}): {e}",
                    idx + 1
                )
            })?;
        if idx == 0 {
            let mut fs = Human68kFilesystem::open(Cursor::new(&mut body), 0)
                .map_err(|e| anyhow::anyhow!("open partition 0 as Human68k: {e}"))?;
            let (files, dirs) = if let Some(tree) = &donor_tree {
                apply_donor_tree(&mut fs, tree)
                    .map_err(|e| anyhow::anyhow!("apply donor tree to partition 0: {e}"))?
            } else {
                seed_validation_files(&mut fs)
                    .map_err(|e| anyhow::anyhow!("seed validation files in partition 0: {e}"))?;
                (3, 0)
            };
            fs.sync_metadata()
                .map_err(|e| anyhow::anyhow!("sync partition 0 metadata: {e}"))?;
            total_files_written += files;
            total_dirs_written += dirs;
        } else {
            // Partitions 1..N are formatted FAT12/16 but otherwise
            // empty. We still open + sync to make sure the blank FS
            // structures land correctly.
            let mut fs = Human68kFilesystem::open(Cursor::new(&mut body), 0)
                .map_err(|e| anyhow::anyhow!("open partition {} as Human68k: {e}", idx + 1))?;
            fs.sync_metadata()
                .map_err(|e| anyhow::anyhow!("sync partition {} metadata: {e}", idx + 1))?;
        }
        partition_bodies.push(body);
    }

    // Optional Phase D.2: overlay the donor HDD's Sharp partition boot
    // sector onto partition 0's first sector. Other partitions keep our
    // own generated BPB (the donor's boot code is single-partition by
    // design, so multi-partition layouts only use the donor on slot 0).
    //
    // Critical: the donor's boot CODE survives intact, but the donor's
    // Sharp/KG BPB at offset 0x12..0x26 is REPLACED by our partition's
    // actual FAT geometry so the boot code reads the right FAT/root/data
    // sectors. Without this patch the donor's BPB (configured for its
    // own size, e.g. 100 MB for hd0.hds) would silently steer the boot
    // code at our smaller / different-shaped partition — non-bootable.
    let boot_sector_donor_applied = if let Some(donor_path) = boot_sector_donor {
        let mut donor_sector = extract_partition_boot_sector(donor_path, variant)?;
        anyhow::ensure!(
            donor_sector.len() as u64 == sector_size,
            "donor partition boot sector is {} bytes but variant {} expects {}",
            donor_sector.len(),
            variant.name(),
            sector_size,
        );
        let p0 = &partition_bodies[0];
        anyhow::ensure!(
            (p0.len() as u64) >= sector_size,
            "partition 0 is smaller than one sector — can't overlay boot sector",
        );
        // Read our generated FAT BPB (standard PC layout, little-endian
        // at offsets 0x0B..0x24) and re-encode the same fields into the
        // donor's Sharp/KG layout (big-endian at offsets 0x12..0x22).
        patch_sharp_kg_bpb_from_pc_bpb(&mut donor_sector, p0)?;
        partition_bodies[0][..donor_sector.len()].copy_from_slice(&donor_sector);
        true
    } else {
        false
    };

    // Build the X68K partition table — populate `partitions` slots,
    // each pointing at the right disk-relative start sector.
    let mut table = [0u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE];
    BigEndian::write_u32(&mut table[0..4], X68K_MAGIC);
    BigEndian::write_u32(&mut table[4..8], disk_sectors as u32);
    BigEndian::write_u32(&mut table[8..12], disk_sectors as u32);
    for idx in 0..partitions {
        let e_off = X68K_TABLE_HEADER_SIZE + idx * X68K_ENTRY_SIZE;
        table[e_off..e_off + 8].copy_from_slice(b"Human   ");
        let entry_start = part_start_sector + (idx as u64) * per_partition_sectors;
        BigEndian::write_u32(&mut table[e_off + 8..e_off + 12], entry_start as u32);
        BigEndian::write_u32(
            &mut table[e_off + 12..e_off + 16],
            per_partition_sectors as u32,
        );
    }

    // Assemble the disk: boot block + N partition bodies, contiguous.
    let mut disk = vec![0u8; total_bytes as usize];
    let boot_block: Vec<u8> = match variant {
        HddVariant::Sasi => build_sasi_boot_block(stub, &table).to_vec(),
        HddVariant::Scsi => build_scsi_boot_block(&SCSI_DESCRIPTOR, stub, &table).to_vec(),
    };
    disk[..boot_block.len()].copy_from_slice(&boot_block);
    for (idx, body) in partition_bodies.iter().enumerate() {
        let part_off = (part_start_byte + (idx as u64) * per_partition_bytes) as usize;
        disk[part_off..part_off + body.len()].copy_from_slice(body);
    }

    std::fs::write(out_path, &disk)
        .map_err(|e| anyhow::anyhow!("write {}: {e}", out_path.display()))?;

    Ok(BuildSummary {
        variant,
        total_bytes,
        disk_sectors,
        sector_size,
        boot_block_bytes,
        partition_count: partitions,
        partition_start_sector: part_start_sector,
        partition_start_byte: part_start_byte,
        partition_sectors: per_partition_sectors,
        partition_bytes: per_partition_bytes,
        files_written: total_files_written,
        dirs_written: total_dirs_written,
        from_donor,
        boot_sector_donor_applied,
    })
}

/// Extract the partition boot sector from a real Sharp X68000 HDD donor.
///
/// Opens `donor_path`, validates that it carries a Sharp `X68SCSI1`
/// signature header + a parseable X68K partition table, locates the
/// first Human68k partition, and reads its first sector. The returned
/// bytes are the donor's Sharp IPL Copyright 1990 SHARP boot block —
/// the same bytes Sharp's `SWITCH.X` writes when run from inside
/// Human68k.
///
/// `variant` controls the expected donor convention:
///
/// - [`HddVariant::Scsi`] — donor must have `X68SCSI1` at byte 0,
///   X68K table at byte `0x800`, 1024-byte sectors. The canonical
///   donor is **`hd0.hds`** (100 MB Sharp/Keisoku Giken SCSI HDD,
///   `file size = 104,857,600`). Other widely-mirrored variants include
///   `HD0.HDS` and `system.hds`.
/// - [`HddVariant::Sasi`] — not currently supported. SASI Human68k
///   HDDs in the wild are rare and our test fixtures (`Bomberman.hdf`)
///   use a custom Hudson Soft IPL rather than the standard Sharp/KG
///   one. Returns an error pointing at the SWITCH.X workflow.
///
/// **License footprint**: the returned bytes flow user → user — they
/// originated in `donor_path` (which the user provided), and they end
/// up in the user's output HDD. Sharp's boot bytes never live in the
/// rusty-backup repo or shipping binaries. Same legal pattern as
/// `--system-disk` (you provide a Human68k floppy, we extract files
/// from it) or as running Sharp's own `SWITCH.X /HD` from inside
/// Human68k (which writes the same bytes from the inside).
pub fn extract_partition_boot_sector(
    donor_path: &Path,
    variant: HddVariant,
) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        matches!(variant, HddVariant::Scsi),
        "boot-sector-donor extraction is SCSI only today. For SASI output, \
         omit --boot-sector-donor and use the SWITCH.X /HD workflow on \
         first boot (see module docs for the rationale)."
    );

    let donor_bytes = std::fs::read(donor_path)
        .map_err(|e| anyhow::anyhow!("read boot-sector donor {}: {e}", donor_path.display()))?;
    anyhow::ensure!(
        donor_bytes.len() >= 0x9000,
        "donor {} is too small ({} bytes) — expected at least 36 KiB to \
         cover the SCSI signature header + table + first partition sector",
        donor_path.display(),
        donor_bytes.len(),
    );

    // Validate the SCSI signature at byte 0 — guards against extracting
    // garbage from a non-X68000 file the user pointed us at by mistake.
    anyhow::ensure!(
        &donor_bytes[0..8] == b"X68SCSI1",
        "donor {} does not start with `X68SCSI1` magic — is it really a \
         Sharp X68000 SCSI HDD? (well-known donor: hd0.hds, ~100 MB)",
        donor_path.display(),
    );

    // Parse the X68K partition table and find the first Human68k slot.
    let mut cursor = Cursor::new(&donor_bytes);
    let table = X68kPartitionTable::detect_at(&mut cursor, 0x800)
        .map_err(|e| anyhow::anyhow!("parse X68K table at 0x800 in {}: {e}", donor_path.display()))?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no X68K partition table found at byte 0x800 in {}",
                donor_path.display()
            )
        })?;
    let human68k_entry = table
        .entries
        .iter()
        .find(|e| e.name_display.starts_with("Human"))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no Human68k partition found in {} — table has {} active entries: {:?}",
                donor_path.display(),
                table.entries.len(),
                table
                    .entries
                    .iter()
                    .map(|e| &e.name_display)
                    .collect::<Vec<_>>()
            )
        })?;

    // SCSI sector size is 1024 bytes; the partition's first sector is at
    // start_sector * 1024 within the donor.
    let sector_size = variant.sector_size() as usize;
    let part_byte_offset = human68k_entry.start_sector as usize * sector_size;
    anyhow::ensure!(
        part_byte_offset + sector_size <= donor_bytes.len(),
        "Human68k partition at sector {} would read past end of donor {} ({} bytes)",
        human68k_entry.start_sector,
        donor_path.display(),
        donor_bytes.len(),
    );

    let mut sector = vec![0u8; sector_size];
    sector.copy_from_slice(&donor_bytes[part_byte_offset..part_byte_offset + sector_size]);

    // Sanity-check the extracted sector: byte 0 should be 0x60 (Sharp's
    // BRA.S over the BPB), and offset 0x02 should be the SHARP/KG OEM
    // marker. Catches "user pointed us at a non-Human68k partition" cases.
    anyhow::ensure!(
        sector[0] == 0x60,
        "donor partition boot sector at byte 0x{:x} of {} doesn't start \
         with 0x60 (BRA.S) — expected Sharp/KG Human68k boot sector. Is \
         this partition formatted Human68k?",
        part_byte_offset,
        donor_path.display(),
    );
    anyhow::ensure!(
        &sector[0x02..0x0E] == b"SHARP/KG    ",
        "donor partition boot sector at byte 0x{:x} of {} doesn't have \
         the Sharp/KG OEM marker at offset 0x02 — extracted bytes 0..14: {:?}",
        part_byte_offset,
        donor_path.display(),
        &sector[..14],
    );

    Ok(sector)
}

/// Patch a donor partition boot sector's Sharp/KG BPB (big-endian fields
/// at offsets `0x12..0x22`) with the FAT geometry of an output partition
/// generated by [`crate::fs::fat::create_blank_fat_with_sector_size`].
/// This is the bridge that lets a real Sharp `hd0.hds`-style boot sector
/// boot from any size/shape partition we produce — without the patch,
/// donor's boot code would walk its own embedded BPB and step off the
/// end of a smaller partition.
///
/// `donor_sector` is the donor's boot sector (length = disk sector size,
/// e.g. 1024 for SCSI). Bytes `0x00..0x12` (BRA.S + OEM marker) and
/// `0x22..end` (boot code + strings) are left unchanged. Only the BPB
/// field region at offsets `0x12..0x22` is rewritten.
///
/// `partition_body` is our generated FAT partition. Its first sector
/// carries a standard PC FAT BPB (little-endian at offsets `0x0B..0x24`).
/// We read those fields and re-encode them into Sharp/KG big-endian
/// format at the canonical offsets.
///
/// Returns an error if our partition's `sectors_per_fat` exceeds 255
/// (Sharp/KG stores it in a single byte at offset `0x1D`). That cap
/// covers any vintage X68000 setup — a FAT16 partition with 1024-byte
/// sectors hits the limit around ~512 MiB, which is past the practical
/// usable HDD size on the Sharp IPL ROM.
fn patch_sharp_kg_bpb_from_pc_bpb(
    donor_sector: &mut [u8],
    partition_body: &[u8],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        donor_sector.len() >= 0x22,
        "donor sector too small ({} bytes) to hold a Sharp/KG BPB",
        donor_sector.len(),
    );
    anyhow::ensure!(
        partition_body.len() >= 0x40,
        "partition body too small ({} bytes) to hold a PC FAT BPB",
        partition_body.len(),
    );

    // ---- Read our generated PC BPB (little-endian) ----
    // Standard FAT BPB layout (see `src/fs/fat.rs::write_fat_boot_sector`):
    //   0x0B (u16 LE)  bytes_per_sector
    //   0x0D (u8)      sectors_per_cluster
    //   0x0E (u16 LE)  reserved_sectors
    //   0x10 (u8)      num_fats
    //   0x11 (u16 LE)  root_entries
    //   0x13 (u16 LE)  total_sectors_16 (zero if 32-bit form used)
    //   0x15 (u8)      media_descriptor
    //   0x16 (u16 LE)  sectors_per_fat
    //   0x20 (u32 LE)  total_sectors_32 (zero if 16-bit form used)
    let bps = u16::from_le_bytes([partition_body[0x0B], partition_body[0x0C]]);
    let spc = partition_body[0x0D];
    let reserved = u16::from_le_bytes([partition_body[0x0E], partition_body[0x0F]]);
    let num_fats = partition_body[0x10];
    let root_entries = u16::from_le_bytes([partition_body[0x11], partition_body[0x12]]);
    let total_sectors_16 = u16::from_le_bytes([partition_body[0x13], partition_body[0x14]]);
    let media = partition_body[0x15];
    let sectors_per_fat_16 = u16::from_le_bytes([partition_body[0x16], partition_body[0x17]]);
    let total_sectors_32 = u32::from_le_bytes([
        partition_body[0x20],
        partition_body[0x21],
        partition_body[0x22],
        partition_body[0x23],
    ]);
    let total_sectors = if total_sectors_16 != 0 {
        total_sectors_16 as u32
    } else {
        total_sectors_32
    };

    anyhow::ensure!(
        sectors_per_fat_16 <= u8::MAX as u16,
        "partition's sectors_per_fat ({sectors_per_fat_16}) exceeds Sharp/KG \
         BPB's u8 field at offset 0x1D — pick a smaller partition or larger \
         sectors_per_cluster",
    );
    anyhow::ensure!(
        sectors_per_fat_16 > 0,
        "partition has zero sectors_per_fat — refusing to write a degenerate BPB",
    );

    // ---- Write the Sharp/KG BPB (big-endian) ----
    // Layout (see `src/fs/human68k.rs::parse_sharp_kg`):
    //   0x12 (u16 BE)  bytes_per_sector
    //   0x14 (u8)      sectors_per_cluster
    //   0x15 (u8)      num_fats
    //   0x16 (u16 BE)  reserved_sectors
    //   0x18 (u16 BE)  root_entries
    //   0x1A (u16 BE)  total_sectors_16 (zero when 32-bit form used)
    //   0x1C (u8)      media_descriptor
    //   0x1D (u8)      sectors_per_fat
    //   0x1E (u32 BE)  total_sectors_32
    donor_sector[0x12..0x14].copy_from_slice(&bps.to_be_bytes());
    donor_sector[0x14] = spc;
    donor_sector[0x15] = num_fats;
    donor_sector[0x16..0x18].copy_from_slice(&reserved.to_be_bytes());
    donor_sector[0x18..0x1A].copy_from_slice(&root_entries.to_be_bytes());
    // Sharp/KG uses the 16-bit total_sectors slot when it fits, 32-bit
    // otherwise. The unused slot is zeroed — same convention `hd0.hds`
    // itself follows (donor disk: 0x1A-0x1B = 0x0000, 0x1E-0x21 carries
    // the full 32-bit count). Mirroring this guards against boot code
    // that picks the wrong slot when both are non-zero.
    let (total_16, total_32) = if total_sectors <= u16::MAX as u32 {
        (total_sectors as u16, 0u32)
    } else {
        (0u16, total_sectors)
    };
    donor_sector[0x1A..0x1C].copy_from_slice(&total_16.to_be_bytes());
    donor_sector[0x1C] = media;
    donor_sector[0x1D] = sectors_per_fat_16 as u8;
    donor_sector[0x1E..0x22].copy_from_slice(&total_32.to_be_bytes());

    Ok(())
}

/// File or directory captured from a donor floppy, ready to replay onto
/// the output partition.
pub struct DonorEntry {
    /// Path components, e.g. `["BIN", "SWITCH.X"]` for `/BIN/SWITCH.X`.
    pub path: Vec<String>,
    pub kind: DonorKind,
}

pub enum DonorKind {
    File(Vec<u8>),
    Directory,
}

fn read_donor_tree(path: &Path) -> anyhow::Result<Vec<DonorEntry>> {
    // Donors are typically .dim / .D88 / .xdf / .hdm floppy containers
    // — decode via the container layer to get a flat FAT stream. Fall
    // back to raw bytes if the file isn't a recognised container (so a
    // pre-flat .img also works).
    let donor_bytes = match decode_floppy_container_file(path) {
        Ok((_kind, flat)) => flat,
        Err(_) => std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("read donor {}: {e}", path.display()))?,
    };
    let donor_slice: &[u8] = &donor_bytes;
    let mut donor_fs = Human68kFilesystem::open(Cursor::new(donor_slice), 0)
        .map_err(|e| anyhow::anyhow!("open donor as Human68k: {e}"))?;
    collect_donor_tree(&mut donor_fs)
}

/// Recursively walk a donor Human68k filesystem and collect every
/// file and directory in DFS pre-order. Files come with their full
/// byte contents — donors are typically 1-2 MiB (Human68k 3.x system
/// floppy) so eager-loading is fine.
fn collect_donor_tree(
    donor: &mut Human68kFilesystem<Cursor<&[u8]>>,
) -> anyhow::Result<Vec<DonorEntry>> {
    fn walk(
        fs: &mut Human68kFilesystem<Cursor<&[u8]>>,
        dir: &FileEntry,
        prefix: &[String],
        out: &mut Vec<DonorEntry>,
    ) -> anyhow::Result<()> {
        let entries = fs
            .list_directory(dir)
            .map_err(|e| anyhow::anyhow!("list: {e}"))?;
        for entry in entries {
            // Human68k surfaces "." and ".." on subdir listings; skip them.
            if entry.name == "." || entry.name == ".." {
                continue;
            }
            let mut path = prefix.to_vec();
            path.push(entry.name.clone());
            match entry.entry_type {
                EntryType::Directory => {
                    out.push(DonorEntry {
                        path: path.clone(),
                        kind: DonorKind::Directory,
                    });
                    walk(fs, &entry, &path, out)?;
                }
                EntryType::File => {
                    let bytes = fs
                        .read_file(&entry, usize::MAX)
                        .map_err(|e| anyhow::anyhow!("read {}: {e}", entry.name))?;
                    out.push(DonorEntry {
                        path,
                        kind: DonorKind::File(bytes),
                    });
                }
                _ => { /* symlinks / specials don't exist on Human68k FAT */ }
            }
        }
        Ok(())
    }
    let root = donor.root().map_err(|e| anyhow::anyhow!("root: {e}"))?;
    let mut out = Vec::new();
    walk(donor, &root, &[], &mut out)?;
    Ok(out)
}

/// Apply a previously-collected donor tree onto the output partition's
/// Human68k filesystem, creating directories before the files inside
/// them. Returns `(files_written, dirs_written)`.
fn apply_donor_tree<W: std::io::Read + std::io::Write + std::io::Seek + Send>(
    out_fs: &mut Human68kFilesystem<W>,
    tree: &[DonorEntry],
) -> anyhow::Result<(usize, usize)> {
    let mut files = 0;
    let mut dirs = 0;
    for entry in tree {
        // split_last returns (last_element, rest_slice) — the last
        // component is the new entry's name, everything before it is the
        // parent directory chain we walk from the root.
        let (name, parent_components) = match entry.path.split_last() {
            Some(s) => s,
            None => continue,
        };
        let mut parent = out_fs.root().map_err(|e| anyhow::anyhow!("root: {e}"))?;
        for comp in parent_components {
            let children = out_fs
                .list_directory(&parent)
                .map_err(|e| anyhow::anyhow!("list {comp}: {e}"))?;
            parent = children
                .into_iter()
                .find(|c| &c.name == comp && c.entry_type == EntryType::Directory)
                .ok_or_else(|| anyhow::anyhow!("intermediate dir {comp} missing"))?;
        }
        match &entry.kind {
            DonorKind::Directory => {
                out_fs
                    .create_directory(&parent, name, &CreateDirectoryOptions::default())
                    .map_err(|e| anyhow::anyhow!("mkdir {name}: {e}"))?;
                dirs += 1;
            }
            DonorKind::File(bytes) => {
                let mut reader: &[u8] = bytes;
                out_fs
                    .create_file(
                        &parent,
                        name,
                        &mut reader,
                        bytes.len() as u64,
                        &CreateFileOptions::default(),
                    )
                    .map_err(|e| anyhow::anyhow!("create {name}: {e}"))?;
                files += 1;
            }
        }
    }
    Ok((files, dirs))
}

fn seed_validation_files<W: std::io::Read + std::io::Write + std::io::Seek + Send>(
    fs: &mut Human68kFilesystem<W>,
) -> anyhow::Result<()> {
    let root = fs.root().map_err(|e| anyhow::anyhow!("root: {e}"))?;
    let seeds: &[(&str, &[u8])] = &[
        (
            "HELLO.TXT",
            b"hello from rusty-backup\r\n\
              This file lives on the self-bootable X68000 HDD that\r\n\
              rusty-backup's `rb-cli new-x68k-hdd` built.\r\n",
        ),
        (
            "MISTER.TXT",
            b"rusty-backup MiSTer X68000 verification fixture\r\n",
        ),
        (
            "README.TXT",
            b"To verify in MAME or on real / MiSTer hardware:\r\n\
              1. Boot Human68k from FDD0 (BLANK_disk_X68000.D88).\r\n\
              2. At the A> prompt, type: dir C:\r\n\
              3. You should see HELLO.TXT, MISTER.TXT, README.TXT.\r\n\
              4. Type: type C:HELLO.TXT\r\n",
        ),
    ];
    for (name, data) in seeds {
        let mut reader: &[u8] = data;
        fs.create_file(
            &root,
            name,
            &mut reader,
            data.len() as u64,
            &CreateFileOptions::default(),
        )
        .map_err(|e| anyhow::anyhow!("create_file {name}: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn builds_sasi_image_with_seed_files() {
        let tmp = NamedTempFile::new().unwrap();
        let summary = build_x68k_hdd(
            tmp.path(),
            4,
            HddVariant::Sasi,
            IplStub::Halt,
            1,
            None,
            None,
        )
        .unwrap();
        assert_eq!(summary.variant, HddVariant::Sasi);
        assert_eq!(summary.sector_size, 256);
        assert_eq!(summary.partition_start_sector, 64);
        assert_eq!(summary.partition_start_byte, 64 * 256);
        assert_eq!(summary.files_written, 3);
        assert_eq!(summary.dirs_written, 0);
        assert!(!summary.from_donor);
        assert!(!summary.boot_sector_donor_applied);

        // Verify byte-0 IPL stub + table at 0x400.
        let bytes = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&bytes[0..2], &[0x60, 0xFE]); // BRA.S self
        assert_eq!(&bytes[0x400..0x404], b"X68K");
    }

    #[test]
    fn builds_scsi_image_with_seed_files() {
        let tmp = NamedTempFile::new().unwrap();
        let summary = build_x68k_hdd(
            tmp.path(),
            4,
            HddVariant::Scsi,
            IplStub::Halt,
            1,
            None,
            None,
        )
        .unwrap();
        assert_eq!(summary.variant, HddVariant::Scsi);
        assert_eq!(summary.sector_size, 1024);
        assert_eq!(summary.partition_start_sector, 64);
        assert_eq!(summary.partition_start_byte, 64 * 1024);

        // Verify SCSI signature at byte 0 + table at 0x800.
        let bytes = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&bytes[0..8], b"X68SCSI1");
        assert_eq!(&bytes[0x800..0x804], b"X68K");
    }

    #[test]
    fn print_stub_lands_in_built_image() {
        let tmp = NamedTempFile::new().unwrap();
        let _ = build_x68k_hdd(
            tmp.path(),
            4,
            HddVariant::Sasi,
            IplStub::Print,
            1,
            None,
            None,
        )
        .unwrap();
        let bytes = std::fs::read(tmp.path()).unwrap();
        // The Print stub starts with 0x60 0x02 (BRA.S +2 = fall through)
        // rather than 0x60 0xFE.
        assert_eq!(&bytes[0..2], &[0x60, 0x02]);
        // Banner text appears somewhere in the boot block.
        assert!(bytes[..0x400]
            .windows(23)
            .any(|w| w == b"Rusty Backup X68000 HDD"));
    }

    #[test]
    fn extract_partition_boot_sector_refuses_non_scsi_donor() {
        // A SASI-tagged variant should be rejected at the API surface
        // even before we look at the donor — the early error tells the
        // user to use SWITCH.X instead.
        let tmp = NamedTempFile::new().unwrap();
        let err = extract_partition_boot_sector(tmp.path(), HddVariant::Sasi).unwrap_err();
        assert!(err.to_string().contains("SCSI only"));
    }

    #[test]
    fn extract_partition_boot_sector_validates_magic() {
        // A file without `X68SCSI1` at byte 0 should be rejected with a
        // clear "is this really a Sharp X68000 SCSI HDD?" error.
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), vec![0u8; 0x10000]).unwrap();
        let err = extract_partition_boot_sector(tmp.path(), HddVariant::Scsi).unwrap_err();
        assert!(err.to_string().contains("X68SCSI1"));
    }

    #[test]
    fn rejects_size_smaller_than_boot_block() {
        // SCSI boot block is 0xC00 (3072 bytes) = under 1 MiB but the
        // partition body would be 0 bytes — should fail clean.
        let tmp = NamedTempFile::new().unwrap();
        let err = build_x68k_hdd(
            tmp.path(),
            0,
            HddVariant::Scsi,
            IplStub::Halt,
            1,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("leaves no room") || err.to_string().contains("overlap"));
    }

    #[test]
    fn builds_multi_partition_scsi_image() {
        // 12 MiB / 3 partitions = ~4 MiB each. Verify the X68K table
        // has 3 Human entries with monotonically increasing start LBAs.
        let tmp = NamedTempFile::new().unwrap();
        let summary = build_x68k_hdd(
            tmp.path(),
            12,
            HddVariant::Scsi,
            IplStub::Halt,
            3,
            None,
            None,
        )
        .unwrap();
        assert_eq!(summary.partition_count, 3);

        let bytes = std::fs::read(tmp.path()).unwrap();
        let table_off = 0x800;
        assert_eq!(&bytes[table_off..table_off + 4], b"X68K");
        for slot in 0..3 {
            let e_off = table_off + 0x10 + slot * 0x10;
            assert_eq!(&bytes[e_off..e_off + 8], b"Human   ", "slot {slot} name");
        }
        let slot3_off = table_off + 0x10 + 3 * 0x10;
        assert!(
            bytes[slot3_off..slot3_off + 16].iter().all(|&b| b == 0),
            "slot 3 should be unused"
        );

        let per_part = summary.partition_sectors as u32;
        for slot in 0..3 {
            let e_off = table_off + 0x10 + slot * 0x10;
            let start = u32::from_be_bytes(bytes[e_off + 8..e_off + 12].try_into().unwrap());
            let length = u32::from_be_bytes(bytes[e_off + 12..e_off + 16].try_into().unwrap());
            assert_eq!(start, 64 + (slot as u32) * per_part);
            assert_eq!(length, per_part);
        }
    }

    #[test]
    fn bpb_patcher_translates_pc_le_to_sharp_kg_be() {
        // Build a minimal donor sector: just a 1024-byte zeroed buffer
        // with the `0x60 0x24` BRA.S marker at byte 0 + SHARP/KG OEM —
        // enough for the patcher to write the BPB region at 0x12..0x22
        // without touching the surrounding code/strings.
        let mut donor = vec![0u8; 1024];
        donor[0] = 0x60;
        donor[1] = 0x24;
        donor[0x02..0x0E].copy_from_slice(b"SHARP/KG    ");
        // Sentinel bytes after the BPB region — the patcher must NOT touch them.
        donor[0x22..0x30].fill(0xAB);

        // Build a synthetic PC FAT partition body with known BPB values.
        let mut body = vec![0u8; 0x40];
        // 0x0B-0x0C: bps = 1024 (LE)
        body[0x0B..0x0D].copy_from_slice(&1024u16.to_le_bytes());
        body[0x0D] = 8; // spc
        body[0x0E..0x10].copy_from_slice(&1u16.to_le_bytes()); // reserved
        body[0x10] = 2; // num_fats
        body[0x11..0x13].copy_from_slice(&224u16.to_le_bytes()); // root_entries
        body[0x13..0x15].copy_from_slice(&32704u16.to_le_bytes()); // total_sectors_16
        body[0x15] = 0xF8; // media
        body[0x16..0x18].copy_from_slice(&6u16.to_le_bytes()); // sectors_per_fat

        patch_sharp_kg_bpb_from_pc_bpb(&mut donor, &body).unwrap();

        // Verify Sharp/KG BPB fields are now in big-endian at the right offsets.
        assert_eq!(u16::from_be_bytes([donor[0x12], donor[0x13]]), 1024);
        assert_eq!(donor[0x14], 8);
        assert_eq!(donor[0x15], 2);
        assert_eq!(u16::from_be_bytes([donor[0x16], donor[0x17]]), 1);
        assert_eq!(u16::from_be_bytes([donor[0x18], donor[0x19]]), 224);
        assert_eq!(u16::from_be_bytes([donor[0x1A], donor[0x1B]]), 32704);
        assert_eq!(donor[0x1C], 0xF8);
        assert_eq!(donor[0x1D], 6);
        // 32-bit slot zeroed because 16-bit slot is live (mirrors hd0.hds convention).
        assert_eq!(
            u32::from_be_bytes([donor[0x1E], donor[0x1F], donor[0x20], donor[0x21]]),
            0
        );

        // Sentinels untouched — the patch is strictly 0x12..0x22.
        assert!(donor[0x22..0x30].iter().all(|&b| b == 0xAB));
        // BRA.S + OEM untouched.
        assert_eq!(donor[0], 0x60);
        assert_eq!(donor[1], 0x24);
        assert_eq!(&donor[0x02..0x0E], b"SHARP/KG    ");
    }

    #[test]
    fn bpb_patcher_uses_32bit_slot_for_large_partitions() {
        // total_sectors > u16::MAX should land in the 32-bit slot with the
        // 16-bit slot zeroed.
        let mut donor = vec![0u8; 1024];
        donor[0] = 0x60;
        donor[1] = 0x24;
        donor[0x02..0x0E].copy_from_slice(b"SHARP/KG    ");

        let mut body = vec![0u8; 0x40];
        body[0x0B..0x0D].copy_from_slice(&1024u16.to_le_bytes());
        body[0x0D] = 4;
        body[0x0E..0x10].copy_from_slice(&1u16.to_le_bytes());
        body[0x10] = 2;
        body[0x11..0x13].copy_from_slice(&512u16.to_le_bytes());
        body[0x13..0x15].copy_from_slice(&0u16.to_le_bytes()); // small_total = 0
        body[0x15] = 0xF8;
        body[0x16..0x18].copy_from_slice(&50u16.to_le_bytes());
        // Large total via 32-bit slot
        body[0x20..0x24].copy_from_slice(&200_000u32.to_le_bytes());

        patch_sharp_kg_bpb_from_pc_bpb(&mut donor, &body).unwrap();

        assert_eq!(u16::from_be_bytes([donor[0x1A], donor[0x1B]]), 0);
        assert_eq!(
            u32::from_be_bytes([donor[0x1E], donor[0x1F], donor[0x20], donor[0x21]]),
            200_000
        );
    }

    #[test]
    fn bpb_patcher_refuses_oversized_sectors_per_fat() {
        // sectors_per_fat > 255 doesn't fit in the Sharp/KG u8 slot at 0x1D.
        let mut donor = vec![0u8; 1024];
        let mut body = vec![0u8; 0x40];
        body[0x0B..0x0D].copy_from_slice(&1024u16.to_le_bytes());
        body[0x0D] = 1;
        body[0x0E..0x10].copy_from_slice(&1u16.to_le_bytes());
        body[0x10] = 2;
        body[0x11..0x13].copy_from_slice(&512u16.to_le_bytes());
        body[0x13..0x15].copy_from_slice(&0u16.to_le_bytes());
        body[0x15] = 0xF8;
        body[0x16..0x18].copy_from_slice(&256u16.to_le_bytes()); // > 255
        body[0x20..0x24].copy_from_slice(&500_000u32.to_le_bytes());

        let err = patch_sharp_kg_bpb_from_pc_bpb(&mut donor, &body).unwrap_err();
        assert!(err.to_string().contains("exceeds Sharp/KG"));
    }

    #[test]
    fn rejects_partition_count_outside_1_to_8() {
        let tmp = NamedTempFile::new().unwrap();
        for bad in [0usize, 9, 10, 100] {
            let err = build_x68k_hdd(
                tmp.path(),
                8,
                HddVariant::Scsi,
                IplStub::Halt,
                bad,
                None,
                None,
            )
            .unwrap_err();
            assert!(
                err.to_string().contains("partitions must be between"),
                "expected count-range error for {bad}, got: {err}"
            );
        }
    }
}
