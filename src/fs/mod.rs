pub mod entry;
pub mod exfat;
pub mod fat;
pub mod filesystem;
pub mod hfs;
pub mod hfsplus;
pub mod ntfs;

use std::io::{Read, Seek, SeekFrom};

pub use exfat::{
    patch_exfat_hidden_sectors, resize_exfat_in_place, validate_exfat_integrity, CompactExfatReader,
};
pub use fat::{
    patch_bpb_hidden_sectors, resize_fat_in_place, set_fat_clean_flags, validate_fat_integrity,
    CompactFatReader, CompactInfo,
};
pub use hfs::{
    patch_hfs_hidden_sectors, resize_hfs_in_place, validate_hfs_integrity, CompactHfsReader,
};
pub use hfsplus::{
    patch_hfsplus_hidden_sectors, resize_hfsplus_in_place, validate_hfsplus_integrity,
    CompactHfsPlusReader,
};
use filesystem::{Filesystem, FilesystemError};
pub use ntfs::{
    patch_ntfs_hidden_sectors, resize_ntfs_in_place, validate_ntfs_integrity, CompactNtfsReader,
};

/// Result of filesystem compaction.
pub struct CompactResult {
    pub original_size: u64,
    pub compacted_size: u64,
    pub clusters_used: u32,
}

/// Detect whether a type-0x07 partition is NTFS or exFAT by reading the OEM ID.
/// Returns `"ntfs"`, `"exfat"`, or `"unknown"`.
fn detect_0x07_type<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> &'static str {
    if reader.seek(SeekFrom::Start(partition_offset)).is_err() {
        return "unknown";
    }
    let mut oem = [0u8; 11];
    if reader.read_exact(&mut oem).is_err() {
        return "unknown";
    }
    if &oem[3..11] == b"NTFS    " {
        "ntfs"
    } else if &oem[3..11] == b"EXFAT   " {
        "exfat"
    } else {
        "unknown"
    }
}

/// Try to create a compacted reader for a partition.
///
/// Returns `None` for unsupported filesystem types. On success, returns a
/// boxed `Read` implementation and a `CompactResult` with sizing information.
pub fn compact_partition_reader<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    partition_type: u8,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    match partition_type {
        // FAT types
        0x01 | 0x04 | 0x06 | 0x0E | 0x14 | 0x16 | 0x1E | 0x0B | 0x0C | 0x1B | 0x1C => {
            let (reader, info) = CompactFatReader::new(reader, partition_offset).ok()?;
            Some((
                Box::new(reader),
                CompactResult {
                    original_size: info.original_size,
                    compacted_size: info.compacted_size,
                    clusters_used: info.clusters_used,
                },
            ))
        }
        // NTFS / exFAT
        0x07 => {
            let fs_type = detect_0x07_type(&mut reader, partition_offset);
            match fs_type {
                "ntfs" => {
                    let (reader, info) = CompactNtfsReader::new(reader, partition_offset).ok()?;
                    Some((
                        Box::new(reader),
                        CompactResult {
                            original_size: info.original_size,
                            compacted_size: info.compacted_size,
                            clusters_used: info.clusters_used,
                        },
                    ))
                }
                "exfat" => {
                    let (reader, info) = CompactExfatReader::new(reader, partition_offset).ok()?;
                    Some((
                        Box::new(reader),
                        CompactResult {
                            original_size: info.original_size,
                            compacted_size: info.compacted_size,
                            clusters_used: info.clusters_used,
                        },
                    ))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Calculate the effective data size for a partition — the number of bytes
/// from the partition start that actually contain filesystem data.
///
/// Returns `None` if the filesystem type is unsupported or cannot be parsed,
/// in which case the caller should fall back to the full partition size.
pub fn effective_partition_size<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
    partition_type: u8,
) -> Option<u64> {
    let mut fs = open_filesystem(reader, partition_offset, partition_type).ok()?;
    fs.last_data_byte().ok()
}

/// Open a filesystem for browsing within a partition.
///
/// `reader` must be seekable and positioned at the partition start.
/// `partition_type` is the MBR partition type byte.
pub fn open_filesystem<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    partition_type: u8,
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    match partition_type {
        // FAT12
        0x01 => Ok(Box::new(fat::FatFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // FAT16
        0x04 | 0x06 | 0x0E | 0x14 | 0x16 | 0x1E => Ok(Box::new(fat::FatFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // FAT32
        0x0B | 0x0C | 0x1B | 0x1C => Ok(Box::new(fat::FatFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // NTFS/exFAT — distinguish by superblock magic
        0x07 => {
            let fs_type = detect_0x07_type(&mut reader, partition_offset);
            match fs_type {
                "ntfs" => Ok(Box::new(ntfs::NtfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "exfat" => Ok(Box::new(exfat::ExfatFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => Err(FilesystemError::Unsupported(
                    "type 0x07 partition is neither NTFS nor exFAT".into(),
                )),
            }
        }
        // Linux
        0x83 => Err(FilesystemError::Unsupported(
            "ext2/3/4 browsing not yet supported".into(),
        )),
        _ => Err(FilesystemError::Unsupported(format!(
            "filesystem type 0x{:02X} not supported for browsing",
            partition_type
        ))),
    }
}
