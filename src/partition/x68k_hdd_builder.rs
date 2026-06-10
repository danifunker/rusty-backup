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
//! What we **don't** ship: the partition's Human68k boot sector. Cloning
//! the system files is mechanical; the boot sector is a Sharp-specific
//! FAT12 loader that we'd need to reverse-engineer from `SWITCH.X`
//! (Sharp's HDD-install utility) or port from the `dis68k` libfat-human68k
//! sources. The practical workaround: boot the same donor floppy from
//! FDD0 once, run `A:\BIN\SWITCH.X /HD`, then the HDD self-boots.

use std::io::Cursor;
use std::path::Path;

use byteorder::{BigEndian, ByteOrder};

use crate::fs::entry::{EntryType, FileEntry};
use crate::fs::fat::create_blank_fat;
use crate::fs::filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem,
};
use crate::fs::human68k::Human68kFilesystem;
use crate::partition::x68k::{
    X68K_ENTRY_SIZE, X68K_FIRST_PARTITION_SECTOR, X68K_MAGIC, X68K_MAX_PARTITIONS,
    X68K_TABLE_HEADER_SIZE,
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
    pub partition_start_sector: u64,
    pub partition_start_byte: u64,
    pub partition_sectors: u64,
    pub partition_bytes: u64,
    /// Number of files written into the partition (3 for the seed-only
    /// path, however many the donor had otherwise).
    pub files_written: usize,
    /// Number of subdirectories created (0 for seed-only).
    pub dirs_written: usize,
    /// `true` when the partition was populated from a `--system-disk`
    /// donor rather than the engine-validation seed files.
    pub from_donor: bool,
}

/// Build a self-bootable X68000 HDD image and write it to `out_path`.
///
/// `size_mib` is the total disk size in MiB. `variant` chooses SASI vs.
/// SCSI byte-0 / sector-size / table-offset conventions. `stub` selects
/// the IPL code variant (halt loop vs. printed banner). If `system_disk`
/// is `Some(p)`, the partition is populated by recursively cloning every
/// file + directory from the donor at `p` (decoded through the floppy-
/// container layer if it's `.dim`/`.D88`/`.xdf`/`.hdm`). Otherwise three
/// seed text files land in the root.
pub fn build_x68k_hdd(
    out_path: &Path,
    size_mib: u64,
    variant: HddVariant,
    stub: IplStub,
    system_disk: Option<&Path>,
) -> anyhow::Result<BuildSummary> {
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
    let part_bytes_target = total_bytes - part_start_byte;

    let label = Some("RBHDD");
    let mut body = create_blank_fat(part_bytes_target, label)
        .map_err(|e| anyhow::anyhow!("create_blank_fat on partition body: {e}"))?;

    // Pre-read the donor tree (if any) before opening the output
    // filesystem to avoid double-borrowing the partition body.
    let donor_tree: Option<Vec<DonorEntry>> = system_disk.map(read_donor_tree).transpose()?;
    let from_donor = donor_tree.is_some();

    let (files_written, dirs_written) = {
        let mut fs = Human68kFilesystem::open(Cursor::new(&mut body), 0)
            .map_err(|e| anyhow::anyhow!("open blank partition as Human68k: {e}"))?;
        let counts = if let Some(tree) = donor_tree {
            apply_donor_tree(&mut fs, &tree)
                .map_err(|e| anyhow::anyhow!("apply donor tree: {e}"))?
        } else {
            seed_validation_files(&mut fs)
                .map_err(|e| anyhow::anyhow!("seed validation files: {e}"))?;
            (3, 0)
        };
        fs.sync_metadata()
            .map_err(|e| anyhow::anyhow!("sync metadata: {e}"))?;
        counts
    };

    let disk_sectors = total_bytes / sector_size;
    let part_sectors = body.len() as u64 / sector_size;
    let part_start_sector = u64::from(X68K_FIRST_PARTITION_SECTOR);

    // Build the X68K partition table — slot 0 = Human68k, slots 1..7
    // remain zero-filled (unused).
    let mut table = [0u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE];
    BigEndian::write_u32(&mut table[0..4], X68K_MAGIC);
    BigEndian::write_u32(&mut table[4..8], disk_sectors as u32);
    BigEndian::write_u32(&mut table[8..12], disk_sectors as u32);
    let e_off = X68K_TABLE_HEADER_SIZE;
    table[e_off..e_off + 8].copy_from_slice(b"Human   ");
    BigEndian::write_u32(&mut table[e_off + 8..e_off + 12], part_start_sector as u32);
    BigEndian::write_u32(&mut table[e_off + 12..e_off + 16], part_sectors as u32);

    // Assemble the disk: boot block + zero padding + partition body.
    let mut disk = vec![0u8; total_bytes as usize];
    let boot_block: Vec<u8> = match variant {
        HddVariant::Sasi => build_sasi_boot_block(stub, &table).to_vec(),
        HddVariant::Scsi => build_scsi_boot_block(&SCSI_DESCRIPTOR, stub, &table).to_vec(),
    };
    disk[..boot_block.len()].copy_from_slice(&boot_block);
    let part_off = part_start_byte as usize;
    disk[part_off..part_off + body.len()].copy_from_slice(&body);

    std::fs::write(out_path, &disk)
        .map_err(|e| anyhow::anyhow!("write {}: {e}", out_path.display()))?;

    Ok(BuildSummary {
        variant,
        total_bytes,
        disk_sectors,
        sector_size,
        boot_block_bytes,
        partition_start_sector: part_start_sector,
        partition_start_byte: part_start_byte,
        partition_sectors: part_sectors,
        partition_bytes: body.len() as u64,
        files_written,
        dirs_written,
        from_donor,
    })
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
        let summary = build_x68k_hdd(tmp.path(), 4, HddVariant::Sasi, IplStub::Halt, None).unwrap();
        assert_eq!(summary.variant, HddVariant::Sasi);
        assert_eq!(summary.sector_size, 256);
        assert_eq!(summary.partition_start_sector, 64);
        assert_eq!(summary.partition_start_byte, 64 * 256);
        assert_eq!(summary.files_written, 3);
        assert_eq!(summary.dirs_written, 0);
        assert!(!summary.from_donor);

        // Verify byte-0 IPL stub + table at 0x400.
        let bytes = std::fs::read(tmp.path()).unwrap();
        assert_eq!(&bytes[0..2], &[0x60, 0xFE]); // BRA.S self
        assert_eq!(&bytes[0x400..0x404], b"X68K");
    }

    #[test]
    fn builds_scsi_image_with_seed_files() {
        let tmp = NamedTempFile::new().unwrap();
        let summary = build_x68k_hdd(tmp.path(), 4, HddVariant::Scsi, IplStub::Halt, None).unwrap();
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
        let _ = build_x68k_hdd(tmp.path(), 4, HddVariant::Sasi, IplStub::Print, None).unwrap();
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
    fn rejects_size_smaller_than_boot_block() {
        // SCSI boot block is 0xC00 (3072 bytes) = under 1 MiB but the
        // partition body would be 0 bytes — should fail clean.
        let tmp = NamedTempFile::new().unwrap();
        let err = build_x68k_hdd(tmp.path(), 0, HddVariant::Scsi, IplStub::Halt, None).unwrap_err();
        assert!(err.to_string().contains("leaves no room") || err.to_string().contains("overlap"));
    }
}
