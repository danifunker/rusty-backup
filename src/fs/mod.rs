pub mod entry;
pub mod fat;
pub mod filesystem;

use std::io::{Read, Seek};

pub use fat::{
    patch_bpb_hidden_sectors, resize_fat_in_place, validate_fat_integrity, CompactFatReader,
    CompactInfo,
};
use filesystem::{Filesystem, FilesystemError};

/// Result of filesystem compaction.
pub struct CompactResult {
    pub original_size: u64,
    pub compacted_size: u64,
    pub clusters_used: u32,
}

/// Try to create a compacted reader for a FAT partition.
///
/// Returns `None` for unsupported filesystem types. On success, returns the
/// `CompactFatReader` (which implements `Read`) and a `CompactResult` with
/// sizing information.
pub fn compact_partition_reader<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
    partition_type: u8,
) -> Option<(CompactFatReader<R>, CompactResult)> {
    // Only FAT types are supported for compaction
    match partition_type {
        0x01 | 0x04 | 0x06 | 0x0E | 0x14 | 0x16 | 0x1E | 0x0B | 0x0C | 0x1B | 0x1C => {}
        _ => return None,
    }

    let (reader, info) = CompactFatReader::new(reader, partition_offset).ok()?;
    Some((
        reader,
        CompactResult {
            original_size: info.original_size,
            compacted_size: info.compacted_size,
            clusters_used: info.clusters_used,
        },
    ))
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
    reader: R,
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
        // NTFS/exFAT - try FAT first (exFAT uses same type byte)
        0x07 => Err(FilesystemError::Unsupported(
            "NTFS/exFAT browsing not yet supported".into(),
        )),
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
