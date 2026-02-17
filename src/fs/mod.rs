pub mod btrfs;
pub mod entry;
pub mod exfat;
pub mod ext;
pub mod fat;
pub mod filesystem;
pub mod hfs;
pub mod hfsplus;
pub mod ntfs;
pub mod resource_fork;
pub mod unix_common;

use std::io::{Read, Seek, SeekFrom};

pub use btrfs::{resize_btrfs_in_place, validate_btrfs_integrity, CompactBtrfsReader};
pub use exfat::{
    patch_exfat_hidden_sectors, resize_exfat_in_place, validate_exfat_integrity, CompactExfatReader,
};
pub use ext::{resize_ext_in_place, validate_ext_integrity, CompactExtReader, ExtFilesystem};
pub use fat::{
    patch_bpb_hidden_sectors, resize_fat_in_place, set_fat_clean_flags, validate_fat_integrity,
    CompactFatReader, CompactInfo,
};
use filesystem::{Filesystem, FilesystemError};
pub use hfs::{
    patch_hfs_hidden_sectors, resize_hfs_in_place, validate_hfs_integrity, CompactHfsReader,
};
pub use hfsplus::{
    patch_hfsplus_hidden_sectors, resize_hfsplus_in_place, validate_hfsplus_integrity,
    CompactHfsPlusReader,
};
pub use ntfs::{
    patch_ntfs_hidden_sectors, resize_ntfs_in_place, validate_ntfs_integrity, CompactNtfsReader,
};

/// Result of filesystem compaction.
pub struct CompactResult {
    pub original_size: u64,
    pub compacted_size: u64,
    pub clusters_used: u32,
}

/// Auto-detect the filesystem type at a given offset by probing magic bytes.
/// Returns a string hint: "fat", "ntfs", "exfat", "hfs", "hfsplus", "ext", or "unknown".
fn detect_filesystem_type<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> &'static str {
    // Read first 12 bytes for FAT/NTFS/exFAT detection
    if reader.seek(SeekFrom::Start(partition_offset)).is_err() {
        return "unknown";
    }
    let mut buf = [0u8; 12];
    if reader.read_exact(&mut buf).is_err() {
        return "unknown";
    }
    if &buf[3..11] == b"NTFS    " {
        return "ntfs";
    }
    if &buf[3..11] == b"EXFAT   " {
        return "exfat";
    }
    if buf[0] == 0xEB || buf[0] == 0xE9 {
        return "fat";
    }

    // Check for HFS/HFS+ at partition_offset + 1024
    if reader
        .seek(SeekFrom::Start(partition_offset + 1024))
        .is_ok()
    {
        let mut hfs_buf = [0u8; 2];
        if reader.read_exact(&mut hfs_buf).is_ok() {
            let sig = u16::from_be_bytes(hfs_buf);
            match sig {
                0x4244 => {
                    // Check for embedded HFS+ at offset 124-125 from MDB start
                    let mut embed = [0u8; 2];
                    if reader
                        .seek(SeekFrom::Start(partition_offset + 1024 + 124))
                        .is_ok()
                        && reader.read_exact(&mut embed).is_ok()
                    {
                        let embed_sig = u16::from_be_bytes(embed);
                        if embed_sig == 0x482B {
                            return "hfsplus";
                        }
                    }
                    return "hfs";
                }
                0x482B | 0x4858 => return "hfsplus",
                _ => {}
            }
        }
    }

    // Check for ext2/3/4 magic (0xEF53 LE) at partition_offset + 1024 + 0x38
    if reader
        .seek(SeekFrom::Start(partition_offset + 1024 + 0x38))
        .is_ok()
    {
        let mut magic = [0u8; 2];
        if reader.read_exact(&mut magic).is_ok() && magic == [0x53, 0xEF] {
            return "ext";
        }
    }

    // Check for btrfs magic "_BHRfS_M" at partition_offset + 0x10000 + 0x40
    if reader
        .seek(SeekFrom::Start(partition_offset + 0x10000 + 0x40))
        .is_ok()
    {
        let mut magic = [0u8; 8];
        if reader.read_exact(&mut magic).is_ok() && &magic == b"_BHRfS_M" {
            return "btrfs";
        }
    }

    "unknown"
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
    partition_type_string: Option<&str>,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    // Check string-based type first (APM partitions)
    if let Some(type_str) = partition_type_string {
        return compact_partition_reader_by_string(reader, partition_offset, type_str);
    }
    match partition_type {
        // Auto-detect (superfloppy / type byte 0)
        0x00 => {
            let fs_type = detect_filesystem_type(&mut reader, partition_offset);
            match fs_type {
                "fat" => {
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
                "ext" => {
                    let (reader, info) = CompactExtReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "btrfs" => {
                    let (reader, info) = CompactBtrfsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                _ => None,
            }
        }
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
        // Linux (ext2/3/4, btrfs)
        0x83 => {
            let fs_type = detect_filesystem_type(&mut reader, partition_offset);
            match fs_type {
                "ext" => {
                    let (reader, info) = CompactExtReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "btrfs" => {
                    let (reader, info) = CompactBtrfsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
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
    partition_type_string: Option<&str>,
) -> Option<u64> {
    let mut fs = open_filesystem(
        reader,
        partition_offset,
        partition_type,
        partition_type_string,
    )
    .ok()?;
    fs.last_data_byte().ok()
}

/// Open a filesystem for browsing within a partition.
///
/// `reader` must be seekable and positioned at the partition start.
/// `partition_type` is the MBR partition type byte.
/// `partition_type_string` is the APM partition type string (e.g. "Apple_HFS").
pub fn open_filesystem<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    // Check string-based type first (APM partitions)
    if let Some(type_str) = partition_type_string {
        return open_filesystem_by_string(reader, partition_offset, type_str);
    }
    match partition_type {
        // Auto-detect (superfloppy / type byte 0)
        0x00 => {
            let fs_type = detect_filesystem_type(&mut reader, partition_offset);
            match fs_type {
                "fat" => Ok(Box::new(fat::FatFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ntfs" => Ok(Box::new(ntfs::NtfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "exfat" => Ok(Box::new(exfat::ExfatFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "hfs" => Ok(Box::new(hfs::HfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "hfsplus" => Ok(Box::new(hfsplus::HfsPlusFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ext" => Ok(Box::new(ext::ExtFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "btrfs" => Ok(Box::new(btrfs::BtrfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => Err(FilesystemError::Unsupported(
                    "could not detect filesystem type on superfloppy".into(),
                )),
            }
        }
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
        // Linux — detect ext vs btrfs by magic bytes
        0x83 => {
            let fs_type = detect_filesystem_type(&mut reader, partition_offset);
            match fs_type {
                "ext" => Ok(Box::new(ext::ExtFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "btrfs" => Ok(Box::new(btrfs::BtrfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => Err(FilesystemError::Unsupported(
                    "type 0x83 partition: unrecognized Linux filesystem".into(),
                )),
            }
        }
        _ => Err(FilesystemError::Unsupported(format!(
            "filesystem type 0x{:02X} not supported for browsing",
            partition_type
        ))),
    }
}

/// Open a filesystem by APM partition type string.
fn open_filesystem_by_string<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    type_str: &str,
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    match type_str {
        "Apple_HFS" => {
            // Check if this is an HFS wrapper around an embedded HFS+ volume
            if detect_embedded_hfs_plus(&mut reader, partition_offset) {
                Ok(Box::new(hfsplus::HfsPlusFilesystem::open(
                    reader,
                    partition_offset,
                )?))
            } else {
                Ok(Box::new(hfs::HfsFilesystem::open(
                    reader,
                    partition_offset,
                )?))
            }
        }
        "Apple_HFSX" | "Apple_HFS+" => Ok(Box::new(hfsplus::HfsPlusFilesystem::open(
            reader,
            partition_offset,
        )?)),
        _ => Err(FilesystemError::Unsupported(format!(
            "APM partition type '{}' not supported for browsing",
            type_str
        ))),
    }
}

/// Try to create a compacted reader by APM partition type string.
fn compact_partition_reader_by_string<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    type_str: &str,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    match type_str {
        "Apple_HFS" => {
            if detect_embedded_hfs_plus(&mut reader, partition_offset) {
                let (compact, info) = CompactHfsPlusReader::new(reader, partition_offset).ok()?;
                Some((Box::new(compact), info))
            } else {
                let (compact, info) = CompactHfsReader::new(reader, partition_offset).ok()?;
                Some((Box::new(compact), info))
            }
        }
        "Apple_HFSX" | "Apple_HFS+" => {
            let (compact, info) = CompactHfsPlusReader::new(reader, partition_offset).ok()?;
            Some((Box::new(compact), info))
        }
        _ => None,
    }
}

/// Detect whether an Apple_HFS partition contains an embedded HFS+ volume.
/// Reads the MDB signature at offset+1024 and checks offset 124 for the
/// HFS+ embedded signature (0x482B).
fn detect_embedded_hfs_plus<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> bool {
    if reader
        .seek(SeekFrom::Start(partition_offset + 1024))
        .is_err()
    {
        return false;
    }
    let mut buf = [0u8; 130];
    if reader.read_exact(&mut buf).is_err() {
        return false;
    }
    let sig = u16::from_be_bytes([buf[0], buf[1]]);
    if sig != 0x4244 {
        return false; // Not HFS
    }
    let embedded_sig = u16::from_be_bytes([buf[124], buf[125]]);
    embedded_sig == 0x482B
}
