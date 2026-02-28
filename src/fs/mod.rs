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
pub mod zstd_stream;

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
    /// Actual bytes that the compact reader will emit (= `original_size` for
    /// layout-preserving readers; < `original_size` for packed readers).
    pub compacted_size: u64,
    /// Logical data bytes: allocated clusters × block_size (+ pre-alloc for HFS).
    /// For packed readers this equals `compacted_size`.
    /// For layout-preserving readers this is less than `compacted_size` because
    /// free clusters are zero-filled in-memory rather than read from disk.
    pub data_size: u64,
    pub clusters_used: u32,
}

/// Auto-detect the filesystem type at a given offset by probing magic bytes.
/// Returns a string hint: "fat", "ntfs", "exfat", "hfs", "hfsplus", "ext", or "unknown".
///
/// All reads are done at 512-byte-aligned offsets with 512-byte buffers so this
/// function works on both regular files and raw character devices (e.g. /dev/rdiskN
/// on macOS, which requires sector-aligned I/O).
fn detect_filesystem_type<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> &'static str {
    // Sector 0: FAT/NTFS/exFAT detection (boot sector, OEM ID at bytes 3-10).
    // Read a full 512-byte sector so the underlying raw-device read is aligned.
    if reader.seek(SeekFrom::Start(partition_offset)).is_err() {
        return "unknown";
    }
    let mut sector0 = [0u8; 512];
    if reader.read_exact(&mut sector0).is_err() {
        return "unknown";
    }
    if &sector0[3..11] == b"NTFS    " {
        return "ntfs";
    }
    if &sector0[3..11] == b"EXFAT   " {
        return "exfat";
    }
    if sector0[0] == 0xEB || sector0[0] == 0xE9 {
        return "fat";
    }

    // Sectors 2-3 (offset 1024): HFS/HFS+ volume header / MDB and ext superblock.
    //   HFS/HFS+ signature is at byte 0 of this block.
    //   ext superblock magic (0xEF53 LE) is at byte 0x38 = 56 of this block.
    //   All in one sector-aligned, sector-sized read.
    if reader
        .seek(SeekFrom::Start(partition_offset + 1024))
        .is_ok()
    {
        let mut sb_buf = [0u8; 512];
        if reader.read_exact(&mut sb_buf).is_ok() {
            let sig = u16::from_be_bytes([sb_buf[0], sb_buf[1]]);
            match sig {
                0x4244 => {
                    // HFS MDB — check for embedded HFS+ (drEmbedSigWord at MDB offset 124)
                    let embed_sig = u16::from_be_bytes([sb_buf[124], sb_buf[125]]);
                    if embed_sig == 0x482B {
                        return "hfsplus";
                    }
                    return "hfs";
                }
                0x482B | 0x4858 => return "hfsplus",
                _ => {}
            }
            // ext superblock magic at offset 0x38 (56) within this sector
            if sb_buf[0x38] == 0x53 && sb_buf[0x39] == 0xEF {
                return "ext";
            }
        }
    }

    // Sector 128 (offset 65536 = 0x10000): btrfs superblock.
    // btrfs magic "_BHRfS_M" is at offset 0x40 (64) within the superblock,
    // i.e. byte 64 of a sector-aligned read from offset 65536.
    if reader
        .seek(SeekFrom::Start(partition_offset + 0x10000))
        .is_ok()
    {
        let mut btrfs_buf = [0u8; 512];
        if reader.read_exact(&mut btrfs_buf).is_ok() && &btrfs_buf[0x40..0x48] == b"_BHRfS_M" {
            return "btrfs";
        }
    }

    "unknown"
}

/// Detect whether a type-0x07 partition is NTFS or exFAT by reading the OEM ID.
/// Returns `"ntfs"`, `"exfat"`, or `"unknown"`.
///
/// Reads a full 512-byte sector for compatibility with raw character devices.
fn detect_0x07_type<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> &'static str {
    if reader.seek(SeekFrom::Start(partition_offset)).is_err() {
        return "unknown";
    }
    let mut sector0 = [0u8; 512];
    if reader.read_exact(&mut sector0).is_err() {
        return "unknown";
    }
    if &sector0[3..11] == b"NTFS    " {
        "ntfs"
    } else if &sector0[3..11] == b"EXFAT   " {
        "exfat"
    } else {
        "unknown"
    }
}

/// Like `compact_partition_reader` but returns a `Result` with a diagnostic
/// error string so callers can log why compaction was unavailable.
///
/// Distinguishes between:
/// - `Ok((reader, result))` — compaction succeeded
/// - `Err(msg)` — the type is supported but the reader constructor failed (e.g.
///   bad superblock); the message explains the failure
/// - `Err("unsupported: ...")` — the filesystem type has no compact reader
pub fn try_compact_partition_reader<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
) -> Result<(Box<dyn Read + Send>, CompactResult), String> {
    if let Some(type_str) = partition_type_string {
        return compact_partition_reader_by_string(reader, partition_offset, type_str).and_then(
            |opt| {
                opt.ok_or_else(|| {
                    format!("unsupported: APM type '{type_str}' has no compact reader")
                })
            },
        );
    }
    compact_partition_reader(reader, partition_offset, partition_type, None).ok_or_else(|| {
        format!(
            "unsupported: no compact reader for MBR type 0x{partition_type:02X} \
             at offset {partition_offset}"
        )
    })
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
        return compact_partition_reader_by_string(reader, partition_offset, type_str)
            .unwrap_or(None);
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
                            data_size: info.compacted_size,
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
                            data_size: info.compacted_size,
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
                            data_size: info.compacted_size,
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
                    data_size: info.compacted_size,
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
                            data_size: info.compacted_size,
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
                            data_size: info.compacted_size,
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
        // Apple HFS/HFS+ on MBR disks
        0xAF => {
            let (fs_type, hfsplus_offset) = resolve_apple_hfs(&mut reader, partition_offset);
            match fs_type {
                "hfsplus" => {
                    let (compact, info) = CompactHfsPlusReader::new(reader, hfsplus_offset).ok()?;
                    Some((Box::new(compact), info))
                }
                _ => {
                    let (compact, info) = CompactHfsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(compact), info))
                }
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
        // Apple HFS/HFS+ on MBR disks
        0xAF => {
            let (fs_type, hfsplus_offset) = resolve_apple_hfs(&mut reader, partition_offset);
            match fs_type {
                "hfsplus" => Ok(Box::new(hfsplus::HfsPlusFilesystem::open(
                    reader,
                    hfsplus_offset,
                )?)),
                _ => Ok(Box::new(hfs::HfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
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
            let (fs_type, hfsplus_offset) = resolve_apple_hfs(&mut reader, partition_offset);
            match fs_type {
                "hfsplus" => Ok(Box::new(hfsplus::HfsPlusFilesystem::open(
                    reader,
                    hfsplus_offset,
                )?)),
                _ => Ok(Box::new(hfs::HfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
            }
        }
        "Apple_HFSX" | "Apple_HFS+" => Ok(Box::new(hfsplus::HfsPlusFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // "Apple_UNIX_SVR2" is the standard APM type for Unix partitions (System V R2).
        // Some tools also write "Apple_UNIX_SRVR2"; handle both.
        "Apple_UNIX_SVR2" | "Apple_UNIX_SRVR2" => {
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
                _ => Err(FilesystemError::Unsupported(format!(
                    "{type_str} partition: unrecognized filesystem (detected: {fs_type})"
                ))),
            }
        }
        _ => Err(FilesystemError::Unsupported(format!(
            "APM partition type '{}' not supported for browsing",
            type_str
        ))),
    }
}

/// Try to create a compacted reader by APM partition type string.
/// Returns `Ok(None)` when the type is unsupported, `Ok(Some(...))` on success,
/// and `Err(msg)` when the type is recognised but the reader constructor fails.
fn compact_partition_reader_by_string<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    type_str: &str,
) -> Result<Option<(Box<dyn Read + Send>, CompactResult)>, String> {
    match type_str {
        "Apple_HFS" => {
            let (fs_type, hfsplus_offset) = resolve_apple_hfs(&mut reader, partition_offset);
            match fs_type {
                "hfsplus" => {
                    let (compact, info) = CompactHfsPlusReader::new(reader, hfsplus_offset)
                        .map_err(|e| {
                            format!(
                                "CompactHfsPlusReader::new failed at offset {hfsplus_offset}: {e}"
                            )
                        })?;
                    Ok(Some((Box::new(compact), info)))
                }
                _ => {
                    let (compact, info) =
                        CompactHfsReader::new(reader, partition_offset).map_err(|e| {
                            format!(
                                "CompactHfsReader::new failed at offset {partition_offset}: {e}"
                            )
                        })?;
                    Ok(Some((Box::new(compact), info)))
                }
            }
        }
        "Apple_HFSX" | "Apple_HFS+" => {
            let (compact, info) =
                CompactHfsPlusReader::new(reader, partition_offset).map_err(|e| {
                    format!("CompactHfsPlusReader::new failed at offset {partition_offset}: {e}")
                })?;
            Ok(Some((Box::new(compact), info)))
        }
        "Apple_UNIX_SVR2" | "Apple_UNIX_SRVR2" => {
            let fs_type = detect_filesystem_type(&mut reader, partition_offset);
            match fs_type {
                "ext" => {
                    let (compact, info) =
                        CompactExtReader::new(reader, partition_offset).map_err(|e| {
                            format!(
                                "CompactExtReader::new failed at offset {partition_offset}: {e}"
                            )
                        })?;
                    Ok(Some((Box::new(compact), info)))
                }
                "btrfs" => {
                    let (compact, info) = CompactBtrfsReader::new(reader, partition_offset)
                        .map_err(|e| {
                            format!(
                                "CompactBtrfsReader::new failed at offset {partition_offset}: {e}"
                            )
                        })?;
                    Ok(Some((Box::new(compact), info)))
                }
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// Resolve the actual HFS filesystem variant for an "Apple_HFS" APM partition.
///
/// Returns `(fs_type, hfsplus_offset)` where `fs_type` is `"hfs"`, `"hfsplus"`,
/// or `"unknown"`, and `hfsplus_offset` is the partition_offset to pass to
/// `HfsPlusFilesystem::open` (accounts for the embedded volume's position).
///
/// Three cases are handled:
/// - Native HFS+ (0x482B/0x4858 at partition_offset+1024): `hfsplus_offset == partition_offset`.
/// - Embedded HFS+ (HFS wrapper with 0x4244 MDB, drEmbedSigWord == 0x482B): `hfsplus_offset`
///   is calculated from the MDB's drAlBlSt/drAlBlkSiz/drEmbedExtent fields.
/// - Pure HFS (0x4244, no embedded HFS+): returns `"hfs"`.
fn resolve_apple_hfs<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> (&'static str, u64) {
    // The HFS MDB / HFS+ volume header sits at partition_offset + 1024 (sector-aligned).
    // Read 512 bytes (one sector) — all required fields are within the first 512 bytes
    // of the MDB (largest field needed is drEmbedExtent.startBlock at MDB offset 127).
    // This keeps the read sector-aligned for raw character device compatibility.
    if reader
        .seek(SeekFrom::Start(partition_offset + 1024))
        .is_err()
    {
        return ("unknown", partition_offset);
    }
    let mut buf = [0u8; 512];
    if reader.read_exact(&mut buf).is_err() {
        return ("unknown", partition_offset);
    }
    let sig = u16::from_be_bytes([buf[0], buf[1]]);
    match sig {
        0x4244 => {
            // HFS MDB — check for embedded HFS+ (drEmbedSigWord at MDB offset 124)
            let embedded_sig = u16::from_be_bytes([buf[124], buf[125]]);
            if embedded_sig == 0x482B {
                // Embedded HFS+: calculate the embedded volume's starting offset.
                // drAlBlkSiz (allocation block size, bytes) at MDB offset 20 (u32 BE).
                // drAlBlSt (first alloc block in 512-byte sectors) at MDB offset 28 (u16 BE).
                // drEmbedExtent.startBlock at MDB offset 126 (u16 BE).
                let block_size = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]) as u64;
                let first_alloc_block = u16::from_be_bytes([buf[28], buf[29]]) as u64;
                let embedded_start = u16::from_be_bytes([buf[126], buf[127]]) as u64;
                // HFS+ volume starts at: partition_offset + drAlBlSt*512 + startBlock*drAlBlkSiz
                let hfsplus_offset =
                    partition_offset + first_alloc_block * 512 + embedded_start * block_size;
                ("hfsplus", hfsplus_offset)
            } else {
                ("hfs", partition_offset)
            }
        }
        // Native HFS+ or HFSX — volume header is directly at partition_offset+1024
        0x482B | 0x4858 => ("hfsplus", partition_offset),
        _ => ("unknown", partition_offset),
    }
}

/// Probe an "Apple_HFS" APM partition to detect the actual filesystem type.
///
/// Returns `"HFS+"`, `"HFSX"`, `"HFS"`, or `"unknown"`. Useful for updating
/// display names after partition table detection.
pub fn probe_apple_hfs_type<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> &'static str {
    let (fs_type, hfsplus_offset) = resolve_apple_hfs(reader, partition_offset);
    match fs_type {
        "hfsplus" => {
            // Distinguish HFS+ from HFSX by reading the volume header signature
            // (signature at bytes 0-1 of a sector-aligned read at hfsplus_offset + 1024).
            if reader.seek(SeekFrom::Start(hfsplus_offset + 1024)).is_ok() {
                let mut sig_buf = [0u8; 512];
                if reader.read_exact(&mut sig_buf).is_ok()
                    && u16::from_be_bytes([sig_buf[0], sig_buf[1]]) == 0x4858
                {
                    return "HFSX";
                }
            }
            "HFS+"
        }
        "hfs" => "HFS",
        _ => "unknown",
    }
}
