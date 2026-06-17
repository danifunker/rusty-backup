//! Classic-HFS boot-block helpers shared by the CLI and GUI "make bootable"
//! paths.
//!
//! A classic-HFS volume boots only if sector 0 holds valid **boot blocks** —
//! the 1024-byte (sectors 0–1) region beginning with the `'LK'` (`0x4C4B`)
//! signature, containing the boot loader the Mac ROM runs before it even
//! looks at the blessed System Folder. A bare data volume (e.g. a flat HFS
//! image freshly built, or infinite-mac's "Infinite HD" data disk) has a
//! zeroed sector 0 and will not boot no matter which folder is blessed.
//!
//! Boot-block code is coupled to the System version, so we never synthesize
//! it. Instead we **copy** the region verbatim from a donor volume that
//! already boots that System (e.g. the matching stock infinite-mac
//! `System x.y HD.dsk`). These helpers are the read + validate primitives;
//! the write side is [`crate::fs::filesystem::EditableFilesystem::write_boot_blocks`].
//!
//! Everything here is **partition-offset relative**: the boot region is read
//! from / written to the start of the *HFS volume*, which is byte 0 for a flat
//! HFV and the `Apple_HFS` partition start for a full (APM) disk. So the same
//! code makes a flat HFV and the HFS partition of a full disk bootable — the
//! DDR / partition map / drivers ahead of an APM HFS partition are never
//! touched.

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::fs::filesystem::FilesystemError;
use crate::partition::PartitionTable;

/// Classic-HFS boot-block signature: `'LK'` (`0x4C4B`) at the first two bytes
/// of sector 0 of a bootable volume.
pub const HFS_BOOT_SIGNATURE: u16 = 0x4C4B;

/// Size of the HFS boot-block region: sectors 0–1, two 512-byte sectors.
pub const HFS_BOOT_REGION_LEN: usize = 1024;

/// Whether the volume at `offset` begins with the `'LK'` boot-block
/// signature. A `false` here means the volume has no boot loader; blessing a
/// System Folder on it is not enough to make it boot.
pub fn has_boot_blocks<R: Read + Seek>(
    reader: &mut R,
    offset: u64,
) -> Result<bool, FilesystemError> {
    reader.seek(SeekFrom::Start(offset))?;
    let mut sig = [0u8; 2];
    reader.read_exact(&mut sig)?;
    Ok(u16::from_be_bytes(sig) == HFS_BOOT_SIGNATURE)
}

/// A donor's boot-block region plus where it was read from, for confirmation
/// messages.
#[derive(Debug, Clone)]
pub struct DonorBootBlocks {
    /// The validated 1024-byte boot-block region, ready to stage / write.
    pub blocks: Box<[u8; HFS_BOOT_REGION_LEN]>,
    /// Byte offset within the donor image the region was read from (0 for a
    /// flat HFS superfloppy, the Apple_HFS partition start for an APM disk).
    pub offset: u64,
}

/// Read and validate the 1024-byte boot-block region of a donor volume whose
/// HFS volume starts at `offset`. Errors with [`FilesystemError::InvalidData`]
/// if the region lacks the `'LK'` signature — a donor that isn't itself
/// bootable has nothing useful to copy.
pub fn read_donor_boot_blocks<R: Read + Seek>(
    reader: &mut R,
    offset: u64,
) -> Result<Box<[u8; HFS_BOOT_REGION_LEN]>, FilesystemError> {
    reader.seek(SeekFrom::Start(offset))?;
    let mut buf = Box::new([0u8; HFS_BOOT_REGION_LEN]);
    reader.read_exact(buf.as_mut_slice())?;
    if u16::from_be_bytes([buf[0], buf[1]]) != HFS_BOOT_SIGNATURE {
        return Err(FilesystemError::InvalidData(
            "donor volume has no HFS boot blocks ('LK' signature absent at sector 0); \
             pick a bootable classic-HFS disk to copy them from"
                .to_string(),
        ));
    }
    Ok(buf)
}

/// Locate the classic-HFS volume inside a donor image and read its boot
/// blocks. Handles the two shapes a Mac disk takes: a flat HFS superfloppy
/// (the volume is at byte 0 — a BasiliskII `.hfv` or an infinite-mac `.dsk`)
/// and an APM-wrapped disk (the `Apple_HFS` partition's start). MBR `0xAF`
/// volumes are accepted too.
///
/// Errors if the image carries no classic-HFS volume, or the located volume
/// isn't bootable (see [`read_donor_boot_blocks`]).
pub fn read_donor_boot_blocks_from_image(path: &Path) -> Result<DonorBootBlocks, FilesystemError> {
    let mut file = std::fs::File::open(path).map_err(FilesystemError::Io)?;
    let table = PartitionTable::detect(&mut file)
        .map_err(|e| FilesystemError::Parse(format!("reading donor partition table: {e}")))?;

    let offset = match &table {
        // Flat HFS superfloppy: the volume is the whole file, at byte 0.
        PartitionTable::None { fs_hint, .. } if fs_hint.eq_ignore_ascii_case("HFS") => 0,
        _ => table
            .partitions()
            .into_iter()
            .find(is_classic_hfs_partition)
            .map(|p| p.byte_offset())
            .ok_or_else(|| {
                FilesystemError::InvalidData(
                    "donor image has no classic-HFS volume to copy boot blocks from".to_string(),
                )
            })?,
    };

    let blocks = read_donor_boot_blocks(&mut file, offset)?;
    Ok(DonorBootBlocks { blocks, offset })
}

/// Whether a partition holds a *classic* HFS volume (not HFS+/HFSX), by APM
/// type string, MBR type byte, or `type_name`.
fn is_classic_hfs_partition(p: &crate::partition::PartitionInfo) -> bool {
    if p.type_name.contains("HFS+") || p.type_name.contains("HFSX") {
        return false;
    }
    let apm_hfs = p
        .partition_type_string
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("Apple_HFS"))
        .unwrap_or(false);
    apm_hfs || p.partition_type_byte == 0xAF || p.type_name == "HFS"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::hfs::create_blank_hfs;
    use std::io::{Cursor, Write};

    const MIB: u64 = 1024 * 1024;

    /// A blank HFS volume from `create_blank_hfs` has zeroed boot blocks, so
    /// it reads as "not bootable" and is rejected as a donor.
    #[test]
    fn blank_volume_has_no_boot_blocks() {
        let img = create_blank_hfs(8 * MIB, 4096, "Blank").unwrap();
        let mut cur = Cursor::new(img);
        assert!(!has_boot_blocks(&mut cur, 0).unwrap());
        let err = read_donor_boot_blocks(&mut cur, 0).unwrap_err();
        assert!(err.to_string().contains("no HFS boot blocks"));
    }

    /// Stamping the `'LK'` signature flips the detection and lets the region
    /// be read as a donor.
    #[test]
    fn lk_signature_makes_a_valid_donor() {
        let mut img = create_blank_hfs(8 * MIB, 4096, "Booty").unwrap();
        // Write a recognizable boot-block region: 'LK' + a marker byte.
        img[0] = 0x4C;
        img[1] = 0x4B;
        img[2] = 0x60; // typical bbID branch byte on real boot blocks
        let mut cur = Cursor::new(&mut img);
        assert!(has_boot_blocks(&mut cur, 0).unwrap());

        let donor = read_donor_boot_blocks(&mut cur, 0).unwrap();
        assert_eq!(&donor[0..3], &[0x4C, 0x4B, 0x60]);
        assert_eq!(donor.len(), HFS_BOOT_REGION_LEN);
    }

    /// The whole-image entry point auto-detects a flat HFS superfloppy at
    /// offset 0.
    #[test]
    fn from_image_detects_flat_superfloppy() {
        let mut img = create_blank_hfs(8 * MIB, 4096, "Flat").unwrap();
        img[0] = 0x4C;
        img[1] = 0x4B;

        let dir = std::env::temp_dir();
        let path = dir.join("rb_hfs_boot_donor_test.hfv");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&img).unwrap();
        }
        let donor = read_donor_boot_blocks_from_image(&path).unwrap();
        assert_eq!(donor.offset, 0);
        assert_eq!(&donor.blocks[0..2], &[0x4C, 0x4B]);
        let _ = std::fs::remove_file(&path);
    }
}
