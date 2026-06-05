pub mod adfs;
pub mod affs;
pub mod affs_common;
pub mod affs_fsck;
pub mod andos;
pub mod apple_dos;
pub mod binhex;
pub mod btrfs;
pub mod cpm;
pub mod cpm_diskdefs;
pub mod efs;
pub mod efs_fsck;
pub mod efs_resize;
pub mod entry;
pub mod exfat;
pub mod ext;
pub mod fat;
pub mod filesystem;
pub mod fsck;
pub mod hfs;
pub mod hfs_clone;
pub mod hfs_common;
pub mod hfs_fsck;
pub mod hfsplus;
pub mod hfsplus_clone;
pub mod hfsplus_defrag;
pub mod hfsplus_fsck;
pub mod hfsplus_journal;
pub mod hfsplus_wrapper_clone;
pub mod hfv;
pub mod human68k;
pub mod jfs;
pub mod jfs_fsck;
pub mod layout_preserving;
pub mod mac_alias;
pub mod mfs;
pub mod ntfs;
pub mod patch;
pub mod pfs3;
pub mod pfs3_clone;
pub mod prodos;
pub mod prodos_types;
pub mod qdos;
pub mod qdos_mdv;
pub mod reiserfs;
pub mod resource_fork;
pub mod sfs;
pub mod tree;
pub mod ufs;
pub mod ufs_fsck;
pub mod unix_common;
pub mod xfs;
pub mod zstd_stream;

use std::io::{Read, Seek, SeekFrom, Write};

pub use btrfs::{resize_btrfs_in_place, validate_btrfs_integrity, CompactBtrfsReader};
pub use exfat::{
    patch_exfat_hidden_sectors, resize_exfat_in_place, validate_exfat_integrity, CompactExfatReader,
};
pub use ext::{resize_ext_in_place, validate_ext_integrity, CompactExtReader, ExtFilesystem};
pub use fat::{
    patch_bpb_hidden_sectors, resize_fat_in_place, set_fat_clean_flags, validate_fat_integrity,
    CompactFatReader,
};
use filesystem::FilesystemError;
pub use filesystem::{
    CreateDirectoryOptions, CreateFileOptions, EditableFilesystem, Filesystem, ResourceForkSource,
};
pub use fsck::{FsckIssue, FsckResult, FsckStats, OrphanedEntry, RepairReport};
pub use hfs::{
    hfs_max_growable_size, resize_hfs_in_place, validate_hfs_integrity, CompactHfsReader,
};
pub use hfsplus::{resize_hfsplus_in_place, validate_hfsplus_integrity, CompactHfsPlusReader};
pub use jfs::CompactJfsReader;
pub use ntfs::{
    patch_ntfs_hidden_sectors, resize_ntfs_in_place, validate_ntfs_integrity, CompactNtfsReader,
};
pub use prodos::{resize_prodos_in_place, validate_prodos_integrity, CompactProDosReader};
pub use qdos::resize_qdos_in_place;
pub use reiserfs::CompactReiserFsReader;
pub use ufs::CompactUfsReader;

/// Update the BPB/VBR hidden-sectors / partition-offset field for whichever
/// filesystem is present at `partition_offset`. Each per-FS patcher checks
/// its own magic and is a no-op on mismatch, so this is safe to call
/// unconditionally during restore. HFS / HFS+ have no LBA-dependent VBR
/// field and are intentionally absent from the dispatch list.
pub fn patch_hidden_sectors_for(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    start_lba: u64,
    log_cb: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    fat::patch_bpb_hidden_sectors(file, partition_offset, start_lba, log_cb)?;
    ntfs::patch_ntfs_hidden_sectors(file, partition_offset, start_lba, log_cb)?;
    exfat::patch_exfat_hidden_sectors(file, partition_offset, start_lba, log_cb)?;
    Ok(())
}

/// Resize whichever filesystem is present at `partition_offset` to
/// `new_size_bytes`. Each per-FS resize is a no-op when its magic doesn't
/// match, so this is safe to call without first probing the type. Use this
/// for code paths (like VHD export reconstruction) that don't already track
/// the filesystem type — code paths that *do* know the type should call
/// the per-FS resize directly.
pub fn resize_filesystem_for(
    file: &mut (impl Read + Write + Seek),
    partition_offset: u64,
    new_size_bytes: u64,
    log_cb: &mut impl FnMut(&str),
) -> anyhow::Result<()> {
    let new_sectors_u32 = (new_size_bytes / 512) as u32;
    let new_sectors_u64 = new_size_bytes / 512;
    fat::resize_fat_in_place(file, partition_offset, new_sectors_u32, log_cb)?;
    ntfs::resize_ntfs_in_place(file, partition_offset, new_sectors_u64, log_cb)?;
    exfat::resize_exfat_in_place(file, partition_offset, new_sectors_u64, log_cb)?;
    hfs::resize_hfs_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    hfsplus::resize_hfsplus_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    ext::resize_ext_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    btrfs::resize_btrfs_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    sfs::resize_sfs_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    pfs3::resize_pfs3_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    affs::resize_affs_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    efs_resize::resize_efs_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    qdos::resize_qdos_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    Ok(())
}

/// Result of filesystem compaction.
///
/// See `src/fs/README.md` ("Compact reader sizing model") for the full
/// description of how `original_size`, `compacted_size`, and `data_size`
/// relate for packed vs. layout-preserving readers.
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
    // XFS superblock magic ("XFSB") at byte 0 of the partition. Both v4
    // (IRIX-compatible) and v5/CRC superblocks share this magic and are
    // fully supported for read + edit + fsck (§2.1 hole (E)).
    if &sector0[0..4] == b"XFSB" {
        return "xfs";
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
                // MFS — pre-HFS, used by Mac 128K/512K and Mac Plus on 400 KB
                // single-sided floppies. Same byte-1024 MDB convention as HFS.
                0xD2D7 => return "mfs",
                _ => {}
            }
            // ext superblock magic at offset 0x38 (56) within this sector
            if sb_buf[0x38] == 0x53 && sb_buf[0x39] == 0xEF {
                return "ext";
            }
            // ProDOS volume directory key block: prev_block==0, storage_type nibble==0xF,
            // entry_length==39, entries_per_block==13.
            // The directory header entry starts at offset 4 (after the 4-byte
            // prev/next block pointers), so entry_length and entries_per_block
            // (offsets 31 and 32 within the 39-byte header entry) land at
            // block bytes 35 and 36.
            if sb_buf[0] == 0
                && sb_buf[1] == 0
                && (sb_buf[4] >> 4) == 0xF
                && (sb_buf[4] & 0xF) >= 1
                && sb_buf[35] == 39
                && sb_buf[36] == 13
            {
                return "prodos";
            }
        }
    }

    // Sector 1 (offset 512): EFS superblock. EFS magic is at offset 28
    // of the sector (0x00072959 / 0x0007295A, big-endian).
    if reader.seek(SeekFrom::Start(partition_offset + 512)).is_ok() {
        let mut efs_buf = [0u8; 512];
        if reader.read_exact(&mut efs_buf).is_ok() {
            let magic = u32::from_be_bytes([efs_buf[28], efs_buf[29], efs_buf[30], efs_buf[31]]);
            if magic == 0x0007_2959 || magic == 0x0007_295A {
                return "efs";
            }
        }
    }

    // Sector 0 again: AmigaDOS "DOS\x" boot block (variants 0..7).
    if &sector0[0..3] == b"DOS" && sector0[3] <= 7 {
        return "affs";
    }

    // Sector 128 (offset 65536 = 0x10000): btrfs superblock AND ReiserFS
    // superblock share this offset. btrfs magic "_BHRfS_M" sits at offset
    // 0x40 within the superblock; ReiserFS magic sits at offset 52
    // (0x34) of the same superblock. One sector-aligned 512-byte read
    // disambiguates both. UFS2's modern superblock lives at the same
    // offset; its magic lands at +1372 (= byte 66908 absolute) so the
    // sector-aligned 512-byte read at 66560 covers it too.
    if reader
        .seek(SeekFrom::Start(partition_offset + 0x10000))
        .is_ok()
    {
        let mut sb64k = [0u8; 512];
        if reader.read_exact(&mut sb64k).is_ok() {
            if &sb64k[0x40..0x48] == b"_BHRfS_M" {
                return "btrfs";
            }
            // ReiserFS magics live at offset 52. v3.5 = "ReIsErFs",
            // v3.6 = "ReIsEr2Fs", reiser4 = "ReIsEr4" (rejected at open).
            let rmagic = &sb64k[52..62];
            if rmagic.starts_with(b"ReIsErFs")
                || rmagic.starts_with(b"ReIsEr2Fs")
                || rmagic.starts_with(b"ReIsEr4")
            {
                return "reiserfs";
            }
        }
    }

    // UFS magic probes. UFS1 lives at byte 8192 (SBLOCK_UFS1) with magic
    // 0x00011954 at +1372 → absolute byte 9564; UFS2 may live at byte
    // 8192 (NetBSD makefs default for small images) OR byte 65536
    // (FreeBSD newfs default) with magic 0x19540119 at the same offset.
    // We probe both candidate locations with one 4-byte read each.
    let mut ufs_magic = [0u8; 4];
    for &cand in &[8192u64, 65536u64] {
        if reader
            .seek(SeekFrom::Start(partition_offset + cand + 1372))
            .is_err()
        {
            continue;
        }
        if reader.read_exact(&mut ufs_magic).is_err() {
            continue;
        }
        let le = u32::from_le_bytes(ufs_magic);
        let be = u32::from_be_bytes(ufs_magic);
        if le == 0x0001_1954 || le == 0x1954_0119 || be == 0x0001_1954 || be == 0x1954_0119 {
            return "ufs";
        }
    }

    // JFS2 magic probe. The primary aggregate superblock lives at byte
    // 32768 (`SUPER1_OFF`) and starts with the 4-byte ASCII magic
    // "JFS1" (Linux JFS2; AIX JFS1 is a different on-disk format with
    // different magic — rejected implicitly).
    if reader
        .seek(SeekFrom::Start(partition_offset + 0x8000))
        .is_ok()
    {
        let mut jfs_magic = [0u8; 4];
        if reader.read_exact(&mut jfs_magic).is_ok() && &jfs_magic == b"JFS1" {
            return "jfs";
        }
    }

    // Apple DOS 3.3 VTOC at byte 0x11000 (track 17, sector 0). Same gate
    // as `partition::detect_superfloppy`: only fire on the exact 140 KB
    // Apple-II floppy geometry, since the VTOC offset would otherwise be
    // mid-stream on a different filesystem.
    let partition_size = reader
        .seek(SeekFrom::End(0))
        .ok()
        .and_then(|end| end.checked_sub(partition_offset))
        .unwrap_or(0);
    if partition_size == 143_360
        && reader
            .seek(SeekFrom::Start(partition_offset + 0x11000))
            .is_ok()
    {
        let mut vtoc = [0u8; 256];
        if reader.read_exact(&mut vtoc).is_ok()
            && vtoc[0x01] == 17
            && vtoc[0x02] == 15
            && (1..=4).contains(&vtoc[0x03])
            && vtoc[0x27] == 122
            && vtoc[0x34] == 35
            && vtoc[0x35] == 16
            && vtoc[0x36] == 0x00
            && vtoc[0x37] == 0x01
        {
            return "applesdos33";
        }
    }

    // Sinclair QL QXL.WIN container: signature "QLWA" at byte 0.
    // Re-read sector 0 (sector0 is already on hand from the top of
    // this function but we re-seek for clarity / safety).
    if &sector0[0..4] == b"QLWA" {
        return "qdos";
    }

    // Acorn ADFS — Disc Record at byte 0xC00 + 0x1C0 = 0xDC0 (HD /
    // legacy floppy bblk path) or byte 0x04 (single-zone E-format
    // floppy dr0 path). Probe just enough to discriminate from random
    // data: log2(sec_size) in 8..=11, heads >= 1 (HD discs report up
    // to 9+; the field is u8), density 0..=3, nzones >= 1. Matches the
    // looser superfloppy probe in `partition::detect_superfloppy` —
    // every disc that surfaces as "ADFS" there must also route here.
    for cand in [0xDC0u64, 0x004u64] {
        if reader
            .seek(SeekFrom::Start(partition_offset + cand))
            .is_ok()
        {
            let mut dr = [0u8; 60];
            if reader.read_exact(&mut dr).is_ok() {
                let log2_sz = dr[0];
                let secs_per_track = dr[1];
                let heads = dr[2];
                let density = dr[3];
                let idlen = dr[4];
                let nzones = dr[9];
                // Bytes 52..60 (`unused52` in the kernel struct) must
                // be zero per `adfs_checkdiscrecord`.
                let reserved_zero = dr[52..60].iter().all(|&b| b == 0);
                if (8..=11).contains(&log2_sz)
                    && secs_per_track >= 1
                    && heads >= 1
                    && density <= 3
                    && nzones >= 1
                    && idlen >= log2_sz + 3
                    && reserved_zero
                {
                    return "adfs";
                }
            }
        }
    }

    // BK0011M ANDOS: signature "ANDOS" at one of several boot-block
    // offsets per src/fs/andos.rs. Restrict to sector 0 to keep this
    // cheap.
    if andos::detect_andos_signature(&sector0).is_some() {
        return "andos";
    }

    // QDOS Microdrive cartridge: exact file size 174,930 bytes
    // (255 × 686) AND a recognisable sector-0 cartridge header. The
    // exact-size constraint keeps this from false-positiving on other
    // formats — every real MiSTer-distributed `.mdv` we've seen lands
    // exactly there.
    if let Ok(end) = reader.seek(SeekFrom::End(0)) {
        if end == qdos_mdv::MDV_CART_BYTES as u64 && qdos_mdv::looks_like_mdv_sector_zero(&sector0)
        {
            return "qdos_mdv";
        }
    }

    "unknown"
}

/// Probe the filesystem inside an MBR type-0x83 partition.
///
/// 0x83 is officially "Linux native" but some MSX HDD formatters (Nextor
/// and similar) reuse it for FAT12/16 partitions. Callers that show the
/// type-name column use this to replace the generic "Linux" label with the
/// actual filesystem family.
///
/// Returns one of: `"FAT"`, `"ext"`, `"btrfs"`, `"XFS"`, `"ReiserFS"`,
/// `"UFS"`, or `None` when the content isn't a filesystem this function
/// recognizes.
pub fn probe_0x83_fs_type<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<&'static str> {
    match detect_filesystem_type(reader, partition_offset) {
        "fat" => Some("FAT"),
        "ext" => Some("ext"),
        "btrfs" => Some("btrfs"),
        "xfs" => Some("XFS"),
        "reiserfs" => Some("ReiserFS"),
        "ufs" => Some("UFS"),
        "jfs" => Some("JFS2"),
        _ => None,
    }
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
    reader: R,
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
                    Some((Box::new(reader), info))
                }
                "ntfs" => {
                    let (reader, info) = CompactNtfsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "exfat" => {
                    let (reader, info) = CompactExfatReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "ext" => {
                    let (reader, info) = CompactExtReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "btrfs" => {
                    let (reader, info) = CompactBtrfsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "reiserfs" => {
                    let (reader, info) =
                        CompactReiserFsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "ufs" => {
                    let (reader, info) = CompactUfsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "jfs" => {
                    let (reader, info) = CompactJfsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "prodos" => {
                    let (reader, info) = CompactProDosReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                _ => None,
            }
        }
        // FAT types
        0x01 | 0x04 | 0x06 | 0x0E | 0x14 | 0x16 | 0x1E | 0x0B | 0x0C | 0x1B | 0x1C => {
            let (reader, info) = CompactFatReader::new(reader, partition_offset).ok()?;
            Some((Box::new(reader), info))
        }
        // NTFS / exFAT
        0x07 => {
            let fs_type = detect_0x07_type(&mut reader, partition_offset);
            match fs_type {
                "ntfs" => {
                    let (reader, info) = CompactNtfsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "exfat" => {
                    let (reader, info) = CompactExfatReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                _ => None,
            }
        }
        // Linux (ext2/3/4, btrfs, reiserfs). Also FAT for MSX HDDs that
        // mis-stamp the type byte (Nextor / similar write 0x83 for FAT
        // partitions).
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
                "reiserfs" => {
                    let (reader, info) =
                        CompactReiserFsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "ufs" => {
                    let (reader, info) = CompactUfsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "jfs" => {
                    let (reader, info) = CompactJfsReader::new(reader, partition_offset).ok()?;
                    Some((Box::new(reader), info))
                }
                "fat" => {
                    let (reader, info) = CompactFatReader::new(reader, partition_offset).ok()?;
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
        // ProDOS on MBR disks
        0xA8 => {
            let (compact, info) = CompactProDosReader::new(reader, partition_offset).ok()?;
            Some((Box::new(compact), info))
        }
        _ => None,
    }
}

/// `Read` adapter that emits an inner reader's bytes followed by zero-fill
/// up to a fixed total length. Used by `packed_partition_reader_padded` to
/// place a packed FS volume at the start of a partition extent and zero-fill
/// the trailing slack so the partition table's extent size still matches.
struct ZeroPaddedReader<R> {
    inner: R,
    inner_remaining: u64,
    pad_remaining: u64,
}

impl<R: Read> Read for ZeroPaddedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.inner_remaining > 0 {
            let want = (self.inner_remaining as usize).min(buf.len());
            let n = self.inner.read(&mut buf[..want])?;
            if n == 0 {
                // Inner exhausted early — convert the rest of inner_remaining
                // into pad_remaining so the caller still sees a stream of the
                // declared total length. This mirrors LayoutPreservingReader's
                // short-source handling.
                self.pad_remaining = self.pad_remaining.saturating_add(self.inner_remaining);
                self.inner_remaining = 0;
                return self.read(buf);
            }
            self.inner_remaining -= n as u64;
            Ok(n)
        } else if self.pad_remaining > 0 {
            let n = (self.pad_remaining as usize).min(buf.len());
            buf[..n].fill(0);
            self.pad_remaining -= n as u64;
            Ok(n)
        } else {
            Ok(0)
        }
    }
}

/// Like `layout_preserving_partition_reader`, but for FAT/NTFS/exFAT it
/// returns the *packed* compact reader (allocated clusters at the start,
/// FS metadata shrunk to fit) padded with zeros up to `original_size`.
///
/// The resulting stream still has length == `original_size`, so it slots
/// into the partition's extent inside a synthesised disk image without
/// changing the partition table. Inside that extent, the OS sees a smaller
/// FAT/NTFS/exFAT volume at offset 0 (BPB / boot sector reflects the
/// shrunken total_sectors) followed by a zero-filled tail. CHD compresses
/// the tail to nothing.
///
/// HFS/HFS+/ext/btrfs/ProDOS keep their existing layout-preserving stream
/// (those readers are already byte-faithful at `original_size`).
///
/// Used by single-file CHD backup when not in sector-by-sector mode: the
/// FAT-family partitions emerge defragmented in place, and the streaming
/// pattern is sequential rather than seek-heavy.
pub fn packed_partition_reader_padded<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    // APM and HFS/ext/btrfs/ProDOS go through the existing dispatcher —
    // those readers are already layout-preserving (compacted_size ==
    // original_size), so no padding is needed.
    if partition_type_string.is_some() {
        return compact_partition_reader(
            reader,
            partition_offset,
            partition_type,
            partition_type_string,
        );
    }
    match partition_type {
        0x83 | 0xAF | 0xA8 => {
            return compact_partition_reader(
                reader,
                partition_offset,
                partition_type,
                partition_type_string,
            );
        }
        _ => {}
    }

    // For FAT/NTFS/exFAT: build the packed reader (compacted_size <
    // original_size) and pad it with zeros to original_size.
    let (compact_reader, info): (Box<dyn Read + Send>, CompactResult) = match partition_type {
        0x00 => {
            let fs_type = detect_filesystem_type(&mut reader, partition_offset);
            match fs_type {
                "fat" => {
                    let (r, info) = CompactFatReader::new(reader, partition_offset).ok()?;
                    (Box::new(r), info)
                }
                "ntfs" => {
                    let (r, info) = CompactNtfsReader::new(reader, partition_offset).ok()?;
                    (Box::new(r), info)
                }
                "exfat" => {
                    let (r, info) = CompactExfatReader::new(reader, partition_offset).ok()?;
                    (Box::new(r), info)
                }
                _ => {
                    return compact_partition_reader(
                        reader,
                        partition_offset,
                        partition_type,
                        partition_type_string,
                    );
                }
            }
        }
        0x01 | 0x04 | 0x06 | 0x0E | 0x14 | 0x16 | 0x1E | 0x0B | 0x0C | 0x1B | 0x1C => {
            let (r, info) = CompactFatReader::new(reader, partition_offset).ok()?;
            (Box::new(r), info)
        }
        0x07 => {
            let fs_type = detect_0x07_type(&mut reader, partition_offset);
            match fs_type {
                "ntfs" => {
                    let (r, info) = CompactNtfsReader::new(reader, partition_offset).ok()?;
                    (Box::new(r), info)
                }
                "exfat" => {
                    let (r, info) = CompactExfatReader::new(reader, partition_offset).ok()?;
                    (Box::new(r), info)
                }
                _ => return None,
            }
        }
        _ => return None,
    };

    let original_size = info.original_size;
    let compacted_size = info.compacted_size;
    let pad = original_size.saturating_sub(compacted_size);
    let padded: Box<dyn Read + Send> = Box::new(ZeroPaddedReader {
        inner: compact_reader,
        inner_remaining: compacted_size,
        pad_remaining: pad,
    });
    let padded_info = CompactResult {
        original_size,
        // Stream length now equals original_size; downstream guards in
        // single_file_chd assert this for layout-preserving correctness.
        compacted_size: original_size,
        data_size: info.data_size,
        clusters_used: info.clusters_used,
    };
    Some((padded, padded_info))
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

/// Calculate the defragmented minimum size for a partition — the smallest
/// size the partition could shrink to **after a clone-into-blank**.
///
/// For most filesystems this matches `effective_partition_size` (no clone
/// path); for HFS+ on fragmented volumes it can be substantially smaller.
/// Returns `None` if the filesystem can't be opened or doesn't override
/// the trait default. See `Filesystem::defragmented_minimum_size`.
pub fn defragmented_partition_size<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
) -> Option<u64> {
    // Wrapped HFS+ detection happens before the generic FS open so we can
    // account for the outer wrapper overhead when reporting the partition-
    // level minimum. `open_filesystem` for Apple_HFS already resolves the
    // wrapper and returns an inner HfsPlusFilesystem, but loses the
    // wrapper-shell sizing — which we need for callers (sizes.rs, the
    // shrink-to-minimum picker) to target the right partition extent.
    //
    // Use a 1 MiB source-extent ceiling for the source partition size
    // probe: detect_wrapped_hfsplus only needs `partition_size` to bound
    // its sanity check (inner extent must fit inside the partition). The
    // real source partition size isn't known here — callers pass it in
    // at the slot level. Use u64::MAX so the check always passes; the
    // real bound is enforced by the eventual resize plan.
    let wrapper_info =
        hfsplus_wrapper_clone::detect_wrapped_hfsplus(&mut reader, partition_offset, u64::MAX);
    let mut fs = open_filesystem(
        reader,
        partition_offset,
        partition_type,
        partition_type_string,
    )
    .ok()?;
    let inner_min = fs.defragmented_minimum_size().ok()?;
    if let Some(info) = wrapper_info {
        let plan = hfsplus_wrapper_clone::plan_wrapped_clone(&info, inner_min).ok()?;
        Some(plan.new_partition_size)
    } else {
        Some(inner_min)
    }
}

/// Outcome of a `partition_minimum_size` call.
pub enum MinimumResult {
    /// The minimum was computed (or determined to be unavailable for this FS type).
    ///
    /// `in_place` is the smallest size achievable without moving any data
    /// (trim only). `defragmented` is the smallest size achievable if the
    /// volume were cloned into a fresh, packed target — for HFS+ this can
    /// be substantially smaller on aged/fragmented volumes; for every other
    /// filesystem it equals `in_place`.
    Computed {
        in_place: Option<u64>,
        defragmented: Option<u64>,
        /// Percent of files with data forks whose data spans more than one
        /// extent. Drives the per-partition Defrag checkbox in the GUI:
        /// auto-checked when this is >= 90.0. `None` when the filesystem
        /// doesn't compute fragmentation (FAT/NTFS/exFAT — their packing
        /// CompactReader makes the toggle irrelevant) or when no files
        /// have data forks (empty volume).
        fragmentation_percent: Option<f32>,
    },
    /// This filesystem requires an expensive volume walk and the caller did not
    /// opt in via `allow_expensive`. The caller should surface a UI affordance
    /// (e.g. a "Calculate minimum size" button) and re-invoke with `true`.
    Deferred {
        /// Human-readable filesystem name (e.g. "HFS", "ext", "btrfs"), suitable
        /// for log messages like "Need to calculate minimum size due to filesystem X".
        fs_name: &'static str,
    },
}

/// Human-readable name of the filesystem associated with a partition type.
pub fn fs_name_for(partition_type: u8, partition_type_string: Option<&str>) -> &'static str {
    if let Some(s) = partition_type_string {
        if is_amiga_dos_type(s) {
            return "AmigaDOS";
        }
        if is_amiga_pfs3_type(s) {
            return "PFS3";
        }
        if is_amiga_sfs_type(s) {
            return "SFS";
        }
        return match s {
            "Apple_HFS" => "HFS",
            "Apple_HFSX" => "HFSX",
            "Apple_UNIX_SVR2" => "ext/btrfs/xfs/reiserfs/UFS/JFS",
            "Linux" => "ext/btrfs/xfs/reiserfs/UFS/JFS",
            _ => "unknown",
        };
    }
    match partition_type {
        0xAF => "HFS/HFS+",
        0x83 => "ext/btrfs/xfs/reiserfs/UFS/JFS",
        0xA8 => "ProDOS",
        0x07 => "NTFS/exFAT",
        0x01 | 0x04 | 0x06 | 0x0E | 0x14 | 0x16 | 0x1E | 0x0B | 0x0C | 0x1B | 0x1C => "FAT",
        // SGI synthetic type bytes (PartitionTable::Sgi).
        0xA0 => "XFS",
        0xA1 => "SGI EFS",
        _ => "unknown",
    }
}

/// True if this filesystem's `CompactReader` returns a layout-preserving
/// stream (allocated blocks at their original byte offsets, free blocks
/// zeroed) rather than a packed stream (allocated blocks repacked at the
/// start with the FS metadata shrunk to match).
///
/// Used by the resize-to-minimum picker: layout-preserving filesystems
/// cannot be shrunk below their `last_data_byte` without an actual data
/// move (which the backup pipeline doesn't perform), so the defragmented
/// minimum is **not** achievable for them — only the in-place trim is.
/// Packing filesystems can be safely shrunk to their defragmented minimum
/// because the reader does the packing during the backup write.
pub fn is_layout_preserving_fs(partition_type: u8, partition_type_string: Option<&str>) -> bool {
    if let Some(s) = partition_type_string {
        if is_amiga_dos_type(s) {
            return true;
        }
        if is_amiga_pfs3_type(s) {
            return true;
        }
        if is_amiga_sfs_type(s) {
            return true;
        }
        return matches!(
            s,
            "Apple_HFS"
                | "Apple_HFSX"
                | "Apple_HFS+"
                | "Apple_UNIX_SVR2"
                | "Apple_UNIX_SRVR2"
                | "Apple_PRODOS"
                | "Apple_ProDOS"
                | "Linux",
        );
    }
    // 0xA1 is our synthetic byte for SGI EFS (PartitionTable::Sgi).
    matches!(partition_type, 0xAF | 0x83 | 0xA8 | 0xA1)
}

/// True if this filesystem has a true defragmenting writer (clone pipeline)
/// that the shrink-to-minimum backup path can use to relocate data blocks.
/// When `true`, the defragmented minimum is genuinely achievable — the
/// clone re-emits the volume packed at the smaller size — so picking it as
/// the shrink target is safe.
///
/// Currently: HFS+/HFSX (via `stream_defragmented_hfsplus`) and PFS3
/// (via `clone_pfs3_volume`). HFS (classic), ext, btrfs, ProDOS, AFFS,
/// and SFS still rely on the layout-preserving reader and must use the
/// in-place trim instead.
pub fn has_defragmenting_writer(partition_type: u8, partition_type_string: Option<&str>) -> bool {
    if let Some(s) = partition_type_string {
        if is_amiga_pfs3_type(s) {
            return true;
        }
        return matches!(s, "Apple_HFS" | "Apple_HFSX" | "Apple_HFS+");
    }
    matches!(partition_type, 0xAF)
}

/// Pick the achievable shrink-to-minimum target for a partition given both
/// the in-place trim point and the defragmented (used-only) minimum.
///
/// - **Packing filesystems** (FAT, NTFS, exFAT): the CompactReader repacks
///   allocated clusters at the partition start, so the defragmented value
///   is what the backup actually emits — use it.
/// - **HFS+/HFSX**: the defrag-clone pipeline
///   ([`hfsplus_defrag::stream_defragmented_hfsplus`]) packs the volume at
///   the smaller size during the backup write — use the defragmented value.
/// - **Other layout-preserving filesystems** (HFS, ext, btrfs, ProDOS):
///   the backup pipeline does not yet relocate blocks, so anything below
///   `in_place` would silently drop allocated blocks. Use the in-place
///   value; the defragmented value is informational only until a true
///   defragmenting writer lands.
pub fn pick_shrink_target(
    partition_type: u8,
    partition_type_string: Option<&str>,
    in_place: Option<u64>,
    defragmented: Option<u64>,
) -> Option<u64> {
    if has_defragmenting_writer(partition_type, partition_type_string) {
        defragmented.or(in_place)
    } else if is_layout_preserving_fs(partition_type, partition_type_string) {
        in_place.or(defragmented)
    } else {
        defragmented.or(in_place)
    }
}

/// True if computing the minimum size for this partition type requires an
/// expensive filesystem walk (catalog/extent tree traversal).
///
/// Cheap path (allocation-bitmap reads): FAT, NTFS, exFAT.
/// Expensive path (full volume walk): HFS, HFS+, ext, btrfs, ProDOS.
pub fn is_expensive_minimum(partition_type: u8, partition_type_string: Option<&str>) -> bool {
    if let Some(s) = partition_type_string {
        if is_amiga_dos_type(s) {
            // AFFS minimum is a cheap bitmap scan — last allocated block.
            return false;
        }
        if is_amiga_pfs3_type(s) {
            // PFS3 last_data_byte is a bitmap-index walk — cheap.
            return false;
        }
        if is_amiga_sfs_type(s) {
            // SFS last_data_byte is a bitmap walk — cheap.
            return false;
        }
        return matches!(s, "Apple_HFS" | "Apple_HFSX" | "Apple_UNIX_SVR2" | "Linux");
    }
    // 0xA1 (SGI EFS): conservative floor requires an inode-table walk.
    matches!(partition_type, 0xAF | 0x83 | 0xA8 | 0xA1)
}

/// Compute the minimum size for a partition, optionally gated behind an
/// expensive-walk opt-in.
///
/// When `allow_expensive` is `false` and the filesystem requires a volume
/// walk, returns `Deferred { fs_name }`. The caller is expected to log a
/// message such as "Need to calculate minimum size due to filesystem {fs_name}"
/// and surface a UI affordance to re-invoke with `allow_expensive=true`.
///
/// `progress` receives short phase strings ("Opening filesystem...",
/// "Computing last data byte...") so a worker thread can update its status.
#[allow(clippy::too_many_arguments)] // unified dispatcher takes reader, fs identification, sizing modes, callbacks
pub fn partition_minimum_size<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
    partition_size: u64,
    allow_expensive: bool,
    wrapper_hint: Option<hfsplus_wrapper_clone::WrappedHfsPlusInfo>,
    progress: &dyn Fn(&str),
) -> MinimumResult {
    if !allow_expensive && is_expensive_minimum(partition_type, partition_type_string) {
        return MinimumResult::Deferred {
            fs_name: fs_name_for(partition_type, partition_type_string),
        };
    }
    // Detect wrapper layout BEFORE consuming the reader into open_filesystem.
    // For wrapped HFS+ we keep the parsed `WrappedHfsPlusInfo` so that the
    // reported defragmented value can be routed through
    // `plan_wrapped_clone`, matching exactly what the clone-time pipeline
    // will produce. Inflating with a raw "inner + overhead" sum would round
    // the inner up to the outer wrapper's allocation-block boundary at
    // clone time and disagree with the precomputed value (the engine then
    // bails with "planned size disagrees with resize plan size").
    //
    // When `wrapper_hint` is supplied, the caller has already probed the
    // MDB via a race-safe path (positioned read on a non-shared handle)
    // and we skip our own seek+read probe. This matters when the reader
    // is a `try_clone`'d fd that shares its seek offset with other workers:
    // a concurrent seek can clobber the probe and leak another partition's
    // wrapper params. See min_size_runner.
    let mut reader = reader;
    let (wrapper_info, wrapper_source) = if let Some(hint) = wrapper_hint {
        (Some(hint), "hint")
    } else {
        progress("Probing for HFS wrapper...");
        let info =
            hfsplus_wrapper_clone::detect_wrapped_hfsplus(&mut reader, partition_offset, u64::MAX);
        (info, "probe")
    };
    match wrapper_info.as_ref() {
        Some(info) => progress(&format!(
            "Wrapper detected ({wrapper_source}): al_block_size={} drAlBlSt={} drNmAlBlks={} \
             embed_start_block={} embed_block_count={} inner_offset={} inner_size={}",
            info.al_block_size,
            info.al_block_start_sector,
            info.source_total_blocks,
            info.embed_start_block,
            info.embed_block_count,
            info.inner_offset,
            info.inner_size,
        )),
        None => progress("No HFS wrapper detected (flat HFS+ or non-HFS)"),
    }
    progress("Opening filesystem...");
    let mut fs = match open_filesystem(
        reader,
        partition_offset,
        partition_type,
        partition_type_string,
    ) {
        Ok(fs) => fs,
        Err(e) => {
            // Pipe the actual error through the progress callback so the
            // caller (min_size_runner -> backup_tab/inspect_tab) can log
            // why the volume couldn't be opened. Surfaces issues like
            // sector-aligned read failures, embedded HFS+ resolution
            // mismatches, or VH parse errors that would otherwise be
            // silently turned into "filesystem could not be opened".
            progress(&format!("open_filesystem failed: {e}"));
            return MinimumResult::Computed {
                in_place: None,
                defragmented: None,
                fragmentation_percent: None,
            };
        }
    };
    progress("Computing last data byte...");
    let in_place = fs.last_data_byte().ok().map(|m| m.min(partition_size));
    progress("Computing fragmentation...");
    let fragmentation_percent = match fs.fragmentation_stats() {
        Some(Ok(stats)) => {
            let p = stats.percent();
            if let Some(pv) = p {
                progress(&format!(
                    "fragmentation: {}/{} files with multiple extents ({:.1}%)",
                    stats.fragmented_files, stats.files_with_data, pv,
                ));
            } else {
                progress("fragmentation: no files with data forks");
            }
            p
        }
        Some(Err(e)) => {
            progress(&format!("fragmentation stats failed: {e}"));
            None
        }
        None => None,
    };
    progress("Computing defragmented minimum...");
    let inner_defrag = fs.defragmented_minimum_size().ok();
    let defragmented = inner_defrag.and_then(|m| {
        progress(&format!("inner_defragmented_minimum_size={m}"));
        let partition_level = match wrapper_info.as_ref() {
            // For wrapped HFS+, the partition extent must be:
            //   wrapper_overhead + ceil(inner_min / outer_al_block_size) * outer_al_block_size
            // Routing through plan_wrapped_clone guarantees the cached
            // minimum equals what the clone pipeline will emit — matching
            // both `new_partition_size` and the resize plan exactly.
            Some(info) => match hfsplus_wrapper_clone::plan_wrapped_clone(info, m) {
                Ok(plan) => {
                    progress(&format!(
                        "plan_wrapped_clone OK: new_partition_size={} new_inner_size={} \
                         new_embed_block_count={} new_total_blocks={}",
                        plan.new_partition_size,
                        plan.new_inner_size,
                        plan.new_embed_block_count,
                        plan.new_total_blocks,
                    ));
                    plan.new_partition_size
                }
                Err(e) => {
                    progress(&format!("plan_wrapped_clone failed: {e}"));
                    return None;
                }
            },
            None => m,
        };
        let clamped = partition_level.min(partition_size);
        progress(&format!(
            "partition-level defragmented={} (clamped from {} by partition_size={})",
            clamped, partition_level, partition_size,
        ));
        Some(clamped)
    });
    MinimumResult::Computed {
        in_place,
        defragmented,
        fragmentation_percent,
    }
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
                "mfs" => Ok(Box::new(mfs::MfsFilesystem::open(
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
                "prodos" => Ok(Box::new(prodos::ProDosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "applesdos33" => Ok(Box::new(apple_dos::AppleDosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "adfs" => Ok(Box::new(adfs::AdfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "qdos" => Ok(Box::new(qdos::QdosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "qdos_mdv" => Ok(Box::new(qdos_mdv::QdosMdvFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "andos" => Ok(Box::new(andos::AndosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "xfs" => Ok(Box::new(xfs::XfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "reiserfs" => Ok(Box::new(reiserfs::ReiserFsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ufs" => Ok(Box::new(ufs::UfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "jfs" => Ok(Box::new(jfs::JfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "efs" => Ok(Box::new(efs::EfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "affs" => Ok(Box::new(affs::AffsFilesystem::open(
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
        // Linux — detect ext / btrfs / xfs by magic bytes.
        // Also accept FAT: some MSX HDD formatters (Nextor and friends) write
        // type 0x83 for FAT12/16 partitions instead of the standard 0x01/0x06.
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
                "xfs" => Ok(Box::new(xfs::XfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "reiserfs" => Ok(Box::new(reiserfs::ReiserFsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ufs" => Ok(Box::new(ufs::UfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "jfs" => Ok(Box::new(jfs::JfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "fat" => Ok(Box::new(fat::FatFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => Err(FilesystemError::Unsupported(
                    "type 0x83 partition: unrecognized filesystem".into(),
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
        // ProDOS on MBR disks (type byte 0xA8)
        0xA8 => Ok(Box::new(prodos::ProDosFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // SGI EFS — synthetic type byte emitted by PartitionTable::Sgi.
        0xA1 => Ok(Box::new(efs::EfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // SGI XFS — synthetic type byte emitted by PartitionTable::Sgi.
        0xA0 => Ok(Box::new(xfs::XfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        _ => Err(FilesystemError::Unsupported(format!(
            "filesystem type 0x{:02X} not supported for browsing",
            partition_type
        ))),
    }
}

/// Open a filesystem for editing (read + write access).
///
/// Same dispatch logic as `open_filesystem` but requires a writable reader and
/// returns a `Box<dyn EditableFilesystem>`. Currently only FAT is supported;
/// other filesystems will be added in subsequent phases.
pub fn open_editable_filesystem<R: Read + Write + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
) -> Result<Box<dyn EditableFilesystem>, FilesystemError> {
    // Check string-based type first (APM partitions)
    if let Some(type_str) = partition_type_string {
        match type_str {
            "Apple_HFS" => {
                let (fs_type, hfsplus_offset) = resolve_apple_hfs(&mut reader, partition_offset);
                return match fs_type {
                    "hfsplus" => {
                        let mut fs = hfsplus::HfsPlusFilesystem::open(reader, hfsplus_offset)?;
                        fs.prepare_for_edit()?;
                        Ok(Box::new(fs))
                    }
                    "hfs" => Ok(Box::new(hfs::HfsFilesystem::open(
                        reader,
                        partition_offset,
                    )?)),
                    _ => Err(FilesystemError::Unsupported(
                        "unrecognized Apple_HFS variant".into(),
                    )),
                };
            }
            "Apple_HFSX" => {
                let mut fs = hfsplus::HfsPlusFilesystem::open(reader, partition_offset)?;
                fs.prepare_for_edit()?;
                return Ok(Box::new(fs));
            }
            "Apple_UNIX_SVR2" | "Apple_UNIX_SRVR2" => {
                let fs_type = detect_filesystem_type(&mut reader, partition_offset);
                return match fs_type {
                    "ext" => Ok(Box::new(ext::ExtFilesystem::open(
                        reader,
                        partition_offset,
                    )?)),
                    "xfs" => Ok(Box::new(xfs::XfsFilesystem::open(
                        reader,
                        partition_offset,
                    )?)),
                    _ => Err(FilesystemError::Unsupported(format!(
                        "editing not yet supported for APM Unix filesystem type '{fs_type}'"
                    ))),
                };
            }
            "Apple_PRODOS" | "Apple_ProDOS" => {
                return Ok(Box::new(prodos::ProDosFilesystem::open(
                    reader,
                    partition_offset,
                )?));
            }
            s if is_amiga_dos_type(s) => {
                return Ok(Box::new(affs::AffsFilesystem::open(
                    reader,
                    partition_offset,
                )?));
            }
            s if is_amiga_pfs3_type(s) => {
                return Ok(Box::new(pfs3::Pfs3Filesystem::open(
                    reader,
                    partition_offset,
                )?));
            }
            s if is_amiga_sfs_type(s) => {
                return Ok(Box::new(sfs::SfsFilesystem::open(
                    reader,
                    partition_offset,
                )?));
            }
            s if s.starts_with("cpm:") => {
                let preset_name = &s[4..];
                let dpb = cpm_diskdefs::preset_by_name(preset_name).ok_or_else(|| {
                    FilesystemError::Unsupported(format!("unknown CP/M DPB preset '{preset_name}'"))
                })?;
                return Ok(Box::new(cpm::CpmFilesystem::open_with_dpb(
                    reader,
                    partition_offset,
                    *dpb,
                )?));
            }
            "human68k" => {
                return Ok(Box::new(human68k::Human68kFilesystem::open(
                    reader,
                    partition_offset,
                )?));
            }
            "qdos" | "qxlwin" | "QDOS" => {
                return Ok(Box::new(qdos::QdosFilesystem::open(
                    reader,
                    partition_offset,
                )?));
            }
            _ => {
                return Err(FilesystemError::Unsupported(format!(
                    "editing not yet supported for APM type '{type_str}'"
                )));
            }
        }
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
                "exfat" => Ok(Box::new(exfat::ExfatFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ntfs" => Ok(Box::new(ntfs::NtfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ext" => Ok(Box::new(ext::ExtFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "hfs" => Ok(Box::new(hfs::HfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "hfsplus" => {
                    let mut fs = hfsplus::HfsPlusFilesystem::open(reader, partition_offset)?;
                    fs.prepare_for_edit()?;
                    Ok(Box::new(fs))
                }
                "mfs" => Ok(Box::new(mfs::MfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "prodos" => Ok(Box::new(prodos::ProDosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "applesdos33" => Ok(Box::new(apple_dos::AppleDosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "efs" => Ok(Box::new(efs::EfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "affs" => Ok(Box::new(affs::AffsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "xfs" => Ok(Box::new(xfs::XfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "qdos" => Ok(Box::new(qdos::QdosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => Err(FilesystemError::Unsupported(format!(
                    "editing not yet supported for filesystem type '{fs_type}'"
                ))),
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
                "exfat" => Ok(Box::new(exfat::ExfatFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ntfs" => Ok(Box::new(ntfs::NtfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => Err(FilesystemError::Unsupported(
                    "type 0x07 partition is neither NTFS nor exFAT".into(),
                )),
            }
        }
        // Linux — detect ext2/3/4. Also FAT for MSX HDDs that mis-stamp the
        // type byte (Nextor / similar write 0x83 for FAT partitions).
        0x83 => {
            let fs_type = detect_filesystem_type(&mut reader, partition_offset);
            match fs_type {
                "ext" => Ok(Box::new(ext::ExtFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "fat" => Ok(Box::new(fat::FatFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "xfs" => Ok(Box::new(xfs::XfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => Err(FilesystemError::Unsupported(format!(
                    "editing not yet supported for type 0x83 filesystem '{fs_type}'"
                ))),
            }
        }
        // ProDOS
        0xA8 => Ok(Box::new(prodos::ProDosFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // SGI EFS — synthetic type byte emitted by PartitionTable::Sgi.
        0xA1 => Ok(Box::new(efs::EfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // SGI XFS — synthetic type byte emitted by PartitionTable::Sgi.
        0xA0 => Ok(Box::new(xfs::XfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // HFS+ (MBR type 0xAF)
        0xAF => {
            let (fs_type, hfsplus_offset) = resolve_apple_hfs(&mut reader, partition_offset);
            match fs_type {
                "hfsplus" => {
                    let mut fs = hfsplus::HfsPlusFilesystem::open(reader, hfsplus_offset)?;
                    fs.prepare_for_edit()?;
                    Ok(Box::new(fs))
                }
                "hfs" => Ok(Box::new(hfs::HfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => Err(FilesystemError::Unsupported(
                    "unrecognized HFS variant at type 0xAF".into(),
                )),
            }
        }
        _ => Err(FilesystemError::Unsupported(format!(
            "editing not yet supported for filesystem type 0x{partition_type:02X}"
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
                "xfs" => Ok(Box::new(xfs::XfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => Err(FilesystemError::Unsupported(format!(
                    "{type_str} partition: unrecognized filesystem (detected: {fs_type})"
                ))),
            }
        }
        "Apple_PRODOS" | "Apple_ProDOS" => Ok(Box::new(prodos::ProDosFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // AmigaDOS Fast/Original File System — DosType DOS\0..DOS\7. PFS and
        // SFS share the same string convention via RDB but route to other
        // modules (Phase 5/7); we only claim the DOS\ prefix here.
        s if is_amiga_dos_type(s) => Ok(Box::new(affs::AffsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // PFS3 family — `PFS\3`, `PDS\3`, `muFS`. Read-only browse +
        // backup (Phase 5); editing arrives in Phase 6.
        s if is_amiga_pfs3_type(s) => Ok(Box::new(pfs3::Pfs3Filesystem::open(
            reader,
            partition_offset,
        )?)),
        // SFS family — `SFS\0`, `SFS\2`. Read-only browse + backup
        // (Phase 7); editing arrives in Phase 8.
        s if is_amiga_sfs_type(s) => Ok(Box::new(sfs::SfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // CP/M with explicit DPB preset. CP/M floppies have no on-disk
        // signature, so callers must declare which DPB applies via the
        // `cpm:<preset_name>` partition_type_string convention. The
        // preset list lives in src/fs/cpm_diskdefs.rs.
        s if s.starts_with("cpm:") => {
            let preset_name = &s[4..];
            let dpb = cpm_diskdefs::preset_by_name(preset_name).ok_or_else(|| {
                FilesystemError::Unsupported(format!(
                    "unknown CP/M DPB preset '{preset_name}' — \
                     valid names: {}",
                    cpm_diskdefs::ALL_PRESETS
                        .iter()
                        .map(|d| d.name)
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?;
            Ok(Box::new(cpm::CpmFilesystem::open_with_dpb(
                reader,
                partition_offset,
                *dpb,
            )?))
        }
        // X68000 Human68k — FAT-derived BPB. Same dispatch shape as
        // CP/M (caller declares the FS via partition_type_string)
        // because the BPB alone can't reliably distinguish Human68k
        // from a regular FAT12/16 volume without an X68000-specific
        // OEM ID heuristic.
        "human68k" => Ok(Box::new(human68k::Human68kFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // Acorn ADFS / FileCore (Archimedes core). Auto-detected via
        // the Disc Record probe in detect_filesystem_type, but the
        // dispatch arm is also reachable via an explicit string.
        "adfs" => Ok(Box::new(adfs::AdfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // Sinclair QL QXL.WIN container. Auto-detect / superfloppy hint
        // returns "QDOS" uppercase; explicit CLI flag uses lowercase.
        "qdos" | "qxlwin" | "QDOS" => Ok(Box::new(qdos::QdosFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // Soviet BK0011M ANDOS scaffold (detect-only). "andos" =
        // explicit CLI/code call; "ANDOS" = auto-detect superfloppy hint.
        "andos" | "ANDOS" => Ok(Box::new(andos::AndosFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // GPT Linux Filesystem GUID — host any of ext, btrfs, or xfs.
        "0FC63DAF-8483-4772-8E79-3D69D8477DE4" => {
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
                "xfs" => Ok(Box::new(xfs::XfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => Err(FilesystemError::Unsupported(format!(
                    "Linux Filesystem GPT partition: unrecognized filesystem (detected: {fs_type})"
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
#[allow(clippy::type_complexity)] // returns Result<Option<(reader, info)>, msg> — the nested optionality is load-bearing
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
        "Apple_PRODOS" | "Apple_ProDOS" => {
            let (compact, info) =
                CompactProDosReader::new(reader, partition_offset).map_err(|e| {
                    format!("CompactProDosReader::new failed at offset {partition_offset}: {e}")
                })?;
            Ok(Some((Box::new(compact), info)))
        }
        s if is_amiga_dos_type(s) => {
            let (compact, info) =
                affs::CompactAffsReader::new(reader, partition_offset).map_err(|e| {
                    format!("CompactAffsReader::new failed at offset {partition_offset}: {e}")
                })?;
            Ok(Some((Box::new(compact), info)))
        }
        s if is_amiga_pfs3_type(s) => {
            let (compact, info) =
                pfs3::CompactPfs3Reader::new(reader, partition_offset).map_err(|e| {
                    format!("CompactPfs3Reader::new failed at offset {partition_offset}: {e}")
                })?;
            Ok(Some((Box::new(compact), info)))
        }
        s if is_amiga_sfs_type(s) => {
            let (compact, info) =
                sfs::CompactSfsReader::new(reader, partition_offset).map_err(|e| {
                    format!("CompactSfsReader::new failed at offset {partition_offset}: {e}")
                })?;
            Ok(Some((Box::new(compact), info)))
        }
        _ => Ok(None),
    }
}

/// True for AmigaDOS Fast/Original File System DosType tags (`DOS\0`..`DOS\7`).
/// PFS / SFS share the DosType-string convention but route to different
/// modules and are intentionally excluded here.
pub fn is_amiga_dos_type(s: &str) -> bool {
    let bytes = s.as_bytes();
    bytes.len() == 5 && &bytes[0..4] == b"DOS\\" && matches!(bytes[4], b'0'..=b'7')
}

/// True for Professional File System 3 (PFS3) DosType tags. The on-disk
/// format is identical for all three: `PFS\3` (classic), `PDS\3` (modern
/// pfs3-aio), and `muFS` (multi-user PFS3, RDB type `muAF` / `muPF`).
pub fn is_amiga_pfs3_type(s: &str) -> bool {
    matches!(s, "PFS\\3" | "PDS\\3" | "muFS")
}

/// True for Smart File System (SFS) DosType tags: `SFS\0` (original) and
/// `SFS\2` (newer journal format). Both share the same on-disk
/// structures for read.
pub fn is_amiga_sfs_type(s: &str) -> bool {
    matches!(s, "SFS\\0" | "SFS\\2")
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
pub fn resolve_apple_hfs<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> (&'static str, u64) {
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

/// Probe an HFS or HFS+ partition for its allocation block size in bytes.
///
/// Returns the volume's `drAlBlkSiz` (HFS) or `blockSize` (HFS+/HFSX) field —
/// the unit used by every catalog/extents record on the volume. For an
/// HFS-wrapped HFS+ volume, the inner HFS+ block size is returned (that's
/// what the catalog records use).
///
/// Returns `None` if no recognizable HFS volume header is found at
/// `partition_offset + 1024`.
pub fn hfs_block_size_at_offset<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<u32> {
    let (fs_type, hfsplus_offset) = resolve_apple_hfs(reader, partition_offset);
    match fs_type {
        "hfs" => {
            // Classic HFS: drAlBlkSiz at MDB offset 20 (u32 BE).
            reader.seek(SeekFrom::Start(partition_offset + 1024)).ok()?;
            let mut buf = [0u8; 512];
            reader.read_exact(&mut buf).ok()?;
            Some(u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]))
        }
        "hfsplus" => {
            // HFS+/HFSX: blockSize at VH offset 40 (u32 BE).
            reader.seek(SeekFrom::Start(hfsplus_offset + 1024)).ok()?;
            let mut buf = [0u8; 512];
            reader.read_exact(&mut buf).ok()?;
            Some(u32::from_be_bytes([buf[40], buf[41], buf[42], buf[43]]))
        }
        _ => None,
    }
}

/// Probe an "Apple_HFS" APM partition to detect the actual filesystem type.
///
/// Read the HFS+/HFSX volume header signature at `partition_offset`. Returns
/// `Some(0x482B)` for HFS+, `Some(0x4858)` for HFSX, or `None` when the
/// partition isn't an HFS+/HFSX volume (including pure classic HFS — those
/// carry an MDB rather than a VH at offset+1024). Handles the embedded /
/// wrapped HFS+ case via `resolve_apple_hfs`.
pub fn probe_hfsplus_signature<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Option<u16> {
    let (fs_type, hfsplus_offset) = resolve_apple_hfs(reader, partition_offset);
    if fs_type != "hfsplus" {
        return None;
    }
    reader.seek(SeekFrom::Start(hfsplus_offset + 1024)).ok()?;
    let mut sig = [0u8; 2];
    reader.read_exact(&mut sig).ok()?;
    Some(u16::from_be_bytes(sig))
}

/// Shape of HFS+ defrag-clone the backup pipeline should use for a
/// partition. Returned by [`defrag_clone_shape`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefragCloneShape {
    /// Native (flat) HFS+/HFSX volume at the partition root. Use
    /// [`hfsplus_defrag::stream_defragmented_hfsplus`].
    Flat,
    /// HFS wrapper with an embedded HFS+/HFSX inside (Mac OS 8/9 style).
    /// Use [`hfsplus_wrapper_clone::stream_wrapped_defragmented_hfsplus`].
    Wrapped,
    /// PFS3 volume (DosType `PFS\3`, `PDS\3`, `muFS`). Use
    /// [`pfs3_clone::stream_defragmented_pfs3`]. Two-pass walk via the
    /// EditableFilesystem trait; tempfile-backed to bound RAM.
    Pfs3,
}

/// Pre-flight check for the streamed defrag-clone backup path. Returns
/// `Ok(shape)` describing how the partition should be cloned, or
/// `Err(reason)` with a human-readable explanation when it cannot.
///
/// Refused cases:
/// - **Dirty journaled volume** — `kHFSVolumeJournaledBit` set with
///   `kHFSVolumeUnmountedBit` clear. The journal carries pending metadata
///   changes the catalog hasn't yet absorbed; cloning the catalog as-is
///   would lose them.
///
/// A cleanly-unmounted journaled volume is *accepted* — the journal is
/// empty / replayed, so the catalog is authoritative and the cloned
/// target (built without the journaled bit) effectively nullifies the
/// journal.
///
/// Embedded HFS+ inside an HFS wrapper is now supported — it returns
/// [`DefragCloneShape::Wrapped`] and the wrapper-aware streamer rebuilds
/// the outer HFS shell around the shrunken inner volume.
///
/// Volumes that aren't HFS+/HFSX at all return `Err` with a generic
/// "not an HFS+ volume" reason — callers should typically branch on
/// fs-type before reaching this helper.
pub fn defrag_clone_shape<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Result<DefragCloneShape, String> {
    let (fs_type, hfsplus_offset) = resolve_apple_hfs(reader, partition_offset);
    if fs_type != "hfsplus" {
        return Err("not an HFS+/HFSX volume".into());
    }
    let shape = if hfsplus_offset == partition_offset {
        DefragCloneShape::Flat
    } else {
        DefragCloneShape::Wrapped
    };
    // Read the VH attributes word (offset 4, 4 bytes). Raw block devices
    // (macOS /dev/diskN, Linux /dev/sdX) reject sub-sector reads and reads
    // at non-sector-aligned offsets, so do a 512-byte sector-aligned read
    // and slice out the four bytes we need.
    if reader.seek(SeekFrom::Start(hfsplus_offset + 1024)).is_err() {
        return Err("failed to seek to VH sector".into());
    }
    let mut sector = [0u8; 512];
    if reader.read_exact(&mut sector).is_err() {
        return Err("failed to read VH sector".into());
    }
    let attributes = u32::from_be_bytes([sector[4], sector[5], sector[6], sector[7]]);
    let journaled = attributes & 0x2000 != 0;
    let unmounted_clean = attributes & 0x100 != 0;
    if journaled && !unmounted_clean {
        return Err(
            "journaled HFS+ volume is dirty (kHFSVolumeUnmountedBit clear) — the \
             on-disk journal carries pending metadata changes that haven't been \
             applied to the catalog. Mount + cleanly unmount the volume first \
             (or run `fsck_hfs -f` against it), then re-run the backup."
                .into(),
        );
    }
    Ok(shape)
}

/// Back-compat wrapper around [`defrag_clone_shape`] that flattens the
/// shape into a unit result. Used by callers that don't care which
/// variant of the clone pipeline runs, only whether *some* clone is
/// possible.
pub fn can_defrag_clone_hfsplus<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
) -> Result<(), String> {
    defrag_clone_shape(reader, partition_offset).map(|_| ())
}

/// Unified defrag-clone preflight that dispatches by partition type
/// string. Returns the shape (`Flat` / `Wrapped` for HFS+/HFSX, `Pfs3`
/// for PFS3) or `Err(reason)` when no clone path is available. The
/// backup pipeline calls this once per partition that opted in to
/// shrink-to-defragmented-minimum.
pub fn detect_defrag_clone_shape<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    partition_type_string: Option<&str>,
) -> Result<DefragCloneShape, String> {
    if let Some(s) = partition_type_string {
        if is_amiga_pfs3_type(s) {
            // PFS3 sanity check: rootblock id at block 2.
            if reader
                .seek(SeekFrom::Start(partition_offset + 2 * 512))
                .is_err()
            {
                return Err("failed to seek to PFS3 rootblock".into());
            }
            let mut id = [0u8; 4];
            if reader.read_exact(&mut id).is_err() {
                return Err("failed to read PFS3 rootblock id".into());
            }
            if &id == b"PFS\x01"
                || &id == b"PFS\x02"
                || &id == b"AFS\x01"
                || &id == b"muPF"
                || &id == b"muAF"
            {
                return Ok(DefragCloneShape::Pfs3);
            }
            return Err(format!(
                "partition tagged {s} but rootblock id {:?} is not PFS3",
                id
            ));
        }
    }
    defrag_clone_shape(reader, partition_offset)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a 4 KiB buffer whose first 4 bytes are the XFS superblock magic.
    /// Enough to exercise `detect_filesystem_type` without needing a valid
    /// XFS sb past byte 4.
    fn xfsb_sector() -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        buf[0..4].copy_from_slice(b"XFSB");
        buf
    }

    #[test]
    fn detect_filesystem_type_recognises_xfsb_magic() {
        let buf = xfsb_sector();
        let mut cursor = Cursor::new(buf);
        assert_eq!(detect_filesystem_type(&mut cursor, 0), "xfs");
    }

    #[test]
    fn open_filesystem_routes_mbr_0x83_xfs_to_xfs_module() {
        // Step A routing wiring: MBR type 0x83 with XFSB magic at sector 0
        // must route to XfsFilesystem (not "unrecognized Linux filesystem").
        // The parse will fail later — we only have 4 bytes of valid sb —
        // but the error message must mention XFS, proving we reached
        // XfsFilesystem::open.
        let buf = xfsb_sector();
        match open_filesystem(Cursor::new(buf), 0, 0x83, None) {
            Err(FilesystemError::Parse(msg)) => assert!(
                msg.to_lowercase().contains("xfs"),
                "expected XFS-flavored parse error, got {msg}"
            ),
            Err(e) => panic!("expected XFS Parse error, got {e}"),
            Ok(_) => panic!("expected error from stub sb"),
        }
    }

    /// Build a 4 KiB buffer that looks like a FAT VBR (EB jump + minimal BPB).
    /// Enough for `detect_filesystem_type` to return "fat"; the FAT parser
    /// will then take it from there and either succeed or surface a Parse
    /// error mentioning FAT-specific fields. Either outcome proves we routed
    /// through the FAT module rather than the previous ext/btrfs/xfs-only
    /// dispatch which silently returned `Unsupported`.
    fn fat_vbr_sector() -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        buf[0] = 0xEB;
        buf[1] = 0x3C;
        buf[2] = 0x90;
        buf[3..11].copy_from_slice(b"MTOO4032"); // mimic MSX OEM ID
        buf[11..13].copy_from_slice(&512u16.to_le_bytes()); // bytes/sector
        buf[13] = 32; // sectors/cluster
        buf[14..16].copy_from_slice(&1u16.to_le_bytes()); // reserved
        buf[16] = 2; // num FATs
        buf[17..19].copy_from_slice(&512u16.to_le_bytes()); // root entries
        buf[21] = 0xF8; // media ID
        buf[22..24].copy_from_slice(&250u16.to_le_bytes()); // sectors/FAT
        buf[32..36].copy_from_slice(&2_047_999u32.to_le_bytes()); // total_sec_32
        buf[510] = 0x55;
        buf[511] = 0xAA;
        buf
    }

    #[test]
    fn open_filesystem_routes_mbr_0x83_fat_to_fat_module() {
        // MSX HDDs (Nextor / similar) write MBR type 0x83 for FAT partitions.
        // The 0x83 dispatch must fall through to the FAT module rather than
        // erroring out as "unrecognized Linux filesystem".
        let buf = fat_vbr_sector();
        let fs = open_filesystem(Cursor::new(buf), 0, 0x83, None)
            .expect("0x83 with FAT VBR should open via FAT module");
        // Spot-check: the resulting filesystem must be browsable as FAT.
        // We deliberately don't poke at the trait — just opening successfully
        // is the regression signal.
        drop(fs);
    }

    #[test]
    fn open_filesystem_routes_gpt_linux_filesystem_guid_xfs() {
        // Step A routing wiring: GPT "Linux Filesystem" GUID with XFSB
        // magic must also reach XfsFilesystem.
        let buf = xfsb_sector();
        match open_filesystem(
            Cursor::new(buf),
            0,
            0,
            Some("0FC63DAF-8483-4772-8E79-3D69D8477DE4"),
        ) {
            Err(FilesystemError::Parse(msg)) => assert!(
                msg.to_lowercase().contains("xfs"),
                "expected XFS-flavored parse error, got {msg}"
            ),
            Err(e) => panic!("expected XFS Parse error, got {e}"),
            Ok(_) => panic!("expected error from stub sb"),
        }
    }
}
