//! Moved to the `fs-mac` crate; re-exported so `crate::fs::hfs_boot` paths keep working.

pub use fs_mac::hfs_boot::*;

use std::path::Path;

use crate::fs::filesystem::FilesystemError;
use crate::partition::PartitionTable;

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
    use std::io::Write;

    const MIB: u64 = 1024 * 1024;

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
