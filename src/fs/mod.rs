pub mod adfs;
pub mod affs;
pub mod affs_common;
pub mod affs_fsck;
pub mod alto;
pub mod andos;
pub mod apfs;
#[cfg(feature = "crypto")]
pub mod apfs_crypto;
pub mod apple_dos;
pub mod archive_fs;
pub mod atari_dos;
pub mod attrs;
pub mod bfs;
pub mod bfs_fsck;
pub mod bfs_write;
pub mod binhex;
pub mod btrfs;
pub mod carve;
pub mod cbm;
pub mod copy;
pub mod cpm;
pub mod cpm_diskdefs;
pub mod dfs;
pub mod dir_import;
pub mod dragondos;
pub mod efs;
pub mod efs_fsck;
pub mod efs_resize;
pub mod efs_v1;
pub mod efs_v1_fsck;
pub mod efs_v1_resize;
pub mod entry;
pub mod exfat;
pub mod exfat_clone;
pub mod exfat_fsck;
pub mod export_selection;
pub mod ext;
pub mod ext_csum;
pub mod ext_format;
pub mod ext_fsck;
pub mod fat;
pub mod fat_fsck;
pub mod filesystem;
pub mod fork_export;
pub mod fsck;
pub mod hfs;
pub mod hfs_boot;
pub mod hfs_clone;
pub mod hfs_common;
pub mod hfs_fsck;
pub mod hfs_unicode;
pub mod hfsplus;
pub mod hfsplus_clone;
pub mod hfsplus_defrag;
pub mod hfsplus_fsck;
pub mod hfsplus_journal;
pub mod hfsplus_wrapper_clone;
pub mod hfv;
pub mod hpfs;
pub mod human68k;
pub mod human68k_clone;
pub mod id_names;
pub mod import_sink;
pub mod jfs;
pub mod jfs_fsck;
pub mod layout_preserving;
pub mod lisa;
pub mod mac_alias;
pub mod mac_archive_import;
pub mod mac_scsi_bless;
pub mod make_bootable;
pub mod mem_archive;
pub mod mfs;
pub mod minix;
pub mod minix_fsck;
pub mod ntfs;
pub mod ntfs_clone;
pub mod ntfs_format;
pub mod ntfs_fsck;
mod ntfs_tables;
pub mod ofs;
pub mod ofs_fsck;
pub mod ofs_write;
#[cfg(feature = "optical")]
pub mod optical_fs;
pub mod oric;
pub mod os9;
pub mod patch;
pub mod pfs3;
pub mod pfs3_clone;
pub mod pfs3_fsck;
pub mod prodos;
pub mod prodos_types;
pub mod qdos;
pub mod qdos_mdv;
pub mod reiserfs;
pub mod replace;
pub mod resource_fork;
pub mod rsdos;
pub mod sfs;
pub mod sfs_fsck;
pub mod squashfs;
pub mod squashfs_edit;
pub mod squashfs_verify;
pub mod squashfs_write;
pub mod tar_export;
pub mod tar_import;
pub mod ti99;
pub mod times;
pub mod trdos;
pub mod tree;
pub mod ucsd;
pub mod ufs;
pub mod ufs_format;
pub mod ufs_fsck;
pub mod unix_common;
pub mod xattr;
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
    // Human68k SHARP/KG HDD volumes (0x60 BRA.S jump, big-endian BPB + FAT)
    // are rejected by resize_fat_in_place; route them to the Human68k resizer.
    // It takes a byte size because these disks use 1024-byte logical sectors,
    // not the 512-byte unit the FAT path assumes. No-op for non-SHARP/KG BPBs.
    human68k::resize_human68k_in_place(file, partition_offset, new_size_bytes, log_cb)?;
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
    efs_v1_resize::resize_efs_v1_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    qdos::resize_qdos_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    prodos::resize_prodos_in_place(file, partition_offset, new_size_bytes, log_cb)?;
    Ok(())
}

/// Whether [`resize_filesystem_for`] can actually patch what lives at
/// `partition_offset`.
///
/// Every per-FS resizer silently no-ops when its magic doesn't match, which is
/// what makes that function safe to call blind — but it also means a caller
/// cannot tell "patched" from "did nothing". A partition resize that moves the
/// table entry while leaving the filesystem's own idea of its size behind
/// produces a volume whose two halves disagree, so a destructive caller has to
/// ask *first*.
pub enum InPlaceResize {
    /// A filesystem [`resize_filesystem_for`] will patch.
    Supported(&'static str),
    /// A filesystem we recognise but have no in-place resizer for. Resizing
    /// the partition around it would leave it inconsistent.
    Unsupported(&'static str),
    /// Nothing recognisable — a raw / swap / boot-blob partition (the MiSTer's
    /// 0xA2 SPL region, say). There is no filesystem metadata to fall out of
    /// step, so resizing the table entry alone is safe.
    NoFilesystem,
}

/// Filesystems `resize_filesystem_for` knows how to patch, by the name
/// [`detect_filesystem_type`] reports. Amiga SFS / PFS3 are not detected by
/// magic at the partition offset — they arrive with an RDB type string, which
/// [`in_place_resize_support`] checks separately.
const IN_PLACE_RESIZABLE: &[&str] = &[
    "fat", "ntfs", "exfat", "hfs", "hfsplus", "ext", "btrfs", "efs", "efs_v1", "qdos", "affs",
    "prodos", "human68k",
];

/// Classify the filesystem at `partition_offset` for in-place resizing.
///
/// `partition_type_string` is the RDB / APM type when the caller has one; the
/// Amiga filesystems are identified that way rather than by a superblock magic
/// at the partition's first sector.
pub fn in_place_resize_support<R: Read + Seek>(
    reader: &mut R,
    partition_offset: u64,
    partition_type_string: Option<&str>,
) -> InPlaceResize {
    if let Some(s) = partition_type_string {
        if is_amiga_dos_type(s) {
            return InPlaceResize::Supported("AFFS");
        }
        if is_amiga_pfs3_type(s) {
            return InPlaceResize::Supported("PFS3");
        }
        if is_amiga_sfs_type(s) {
            return InPlaceResize::Supported("SFS");
        }
        if s == "human68k" {
            return InPlaceResize::Supported("Human68k");
        }
    }
    let detected = detect_filesystem_type(reader, partition_offset);
    if detected == "unknown" {
        return InPlaceResize::NoFilesystem;
    }
    if IN_PLACE_RESIZABLE.contains(&detected) {
        // The pretty name if we have one, else the detector's own token.
        return InPlaceResize::Supported(fs_display_name(detected));
    }
    InPlaceResize::Unsupported(fs_display_name(detected))
}

/// A human-facing name for a `detect_filesystem_type` token, for messages.
fn fs_display_name(detected: &str) -> &'static str {
    match detected {
        "fat" => "FAT",
        "ntfs" => "NTFS",
        "exfat" => "exFAT",
        "hfs" => "HFS",
        "hfsplus" => "HFS+",
        "ext" => "ext2/3/4",
        "btrfs" => "btrfs",
        "efs" => "SGI EFS",
        "efs_v1" => "SGI EFS v1",
        "qdos" => "QDOS",
        "affs" => "AFFS",
        "prodos" => "ProDOS",
        "human68k" => "Human68k",
        "xfs" => "XFS",
        "jfs" => "JFS",
        "reiserfs" => "ReiserFS",
        "ufs" => "UFS",
        "hpfs" => "HPFS",
        "apfs" => "APFS",
        "squashfs" => "SquashFS",
        "mfs" => "MFS",
        _ => "an unsupported filesystem",
    }
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
    // SquashFS: "hsqs" at offset 0. Probed early because the magic is
    // distinctive and sits where a boot sector's JMP would be, so leaving it
    // until after the FAT heuristics risks a misclassification.
    if &sector0[0..4] == b"hsqs" {
        return "squashfs";
    }
    // OS/2 HPFS: its boot block also begins with a JMP and a plausible 512/1
    // BPB, so it would be misclassified by the FAT heuristic below. Detect it
    // first via the super block (sector 16) + spare block (sector 17) magics.
    if hpfs::looks_like_hpfs(reader, partition_offset) {
        return "hpfs";
    }
    // Oric Jasmin: exact 178432/356864-byte flat 256-byte-sector image with the
    // free-map format markers at block 340. Very specific, so probe early.
    if oric::looks_like_oric_jasmin(reader, partition_offset) {
        return "oric_jasmin";
    }
    // X68000 Human68k HDD: BRA.S at byte 0 and a big-endian BPB, which the FAT
    // heuristic below cannot read; the resizer and min-size paths key off it.
    if human68k::looks_like_human68k_hdd(&sector0) {
        return "human68k";
    }
    // FAT boot sectors begin with a JMP (0xEB short or 0xE9 near). But a JMP
    // opcode alone is a weak signal: syslinux/extlinux install their boot code
    // into the reserved first 1024 bytes of an ext2/3/4 volume, and that code
    // also begins `EB 58 90`. Only treat this as FAT when the BPB is plausible
    // — a nonzero bytes-per-sector within range and a nonzero
    // sectors-per-cluster — so an ext partition with a boot loader falls
    // through to the ext2 superblock-magic check below. The gate mirrors
    // `fat::FatFilesystem::open`, which rejects bytes_per_sector 0 / >4096 and
    // sectors_per_cluster 0.
    if sector0[0] == 0xEB || sector0[0] == 0xE9 {
        let bytes_per_sector = u16::from_le_bytes([sector0[11], sector0[12]]);
        let sectors_per_cluster = sector0[13];
        if bytes_per_sector != 0 && bytes_per_sector <= 4096 && sectors_per_cluster != 0 {
            return "fat";
        }
        // Otherwise the JMP is a boot loader, not a FAT BPB — fall through.
    }
    // XFS superblock magic ("XFSB") at byte 0 of the partition. Both v4
    // (IRIX-compatible) and v5/CRC superblocks share this magic and are
    // fully supported for read + edit + fsck (§2.1 hole (E)).
    if &sector0[0..4] == b"XFSB" {
        return "xfs";
    }
    // APFS container superblock (NXSB) magic at offset 32 of block 0. The full
    // checksum-validated checkpoint scan happens in ApfsFilesystem::open; here
    // we only sniff the magic + a plausible block size on the single probe read.
    if apfs::detect_apfs(&sector0) {
        return "apfs";
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
            // Minix superblock: block 1 (this offset-1024 sector). Magic at
            // +16 (V1/V2: 0x137F/0x138F/0x2468/0x2478) or +24 (V3: 0x4D5A).
            if let Some(name) = minix::detect_magic(&sb_buf) {
                return name;
            }
            // UCSD p-System volume label at block 2 (this offset-1024 read).
            // No magic — a structural signature — so it is checked last here.
            if ucsd::looks_like_ucsd(&sb_buf) {
                return "ucsd";
            }
        }
    }

    // Sector 1 (offset 512): EFS superblock. EFS magic is at offset 28
    // of the sector (0x00072959 / 0x0007295A, big-endian). BeOS BFS puts its
    // superblock in the same sector with `BFS1` at offset 32, in either order.
    if reader.seek(SeekFrom::Start(partition_offset + 512)).is_ok() {
        let mut efs_buf = [0u8; 512];
        if reader.read_exact(&mut efs_buf).is_ok() {
            let magic = u32::from_be_bytes([efs_buf[28], efs_buf[29], efs_buf[30], efs_buf[31]]);
            if magic == 0x0007_2959 || magic == 0x0007_295A {
                return "efs";
            }
            if bfs::BfsSuperBlock::parse(&efs_buf).is_ok() {
                return "bfs";
            }
            // BeOS/PPC has no boot block, so its superblock starts at byte 0.
            if reader.seek(SeekFrom::Start(partition_offset)).is_ok() {
                let mut head = [0u8; 512];
                if reader.read_exact(&mut head).is_ok() && bfs::BfsSuperBlock::parse(&head).is_ok()
                {
                    return "bfs";
                }
            }
        }
    }

    // Same sector, different filesystem: the original SGI EFS (IRIS 2000 /
    // 3000) puts `fs_magic` 0x041755 at offset 0x26 instead, and the image
    // may be byte-swapped, so the probe tries both orders.
    if efs_v1::detect(reader, partition_offset).is_some() {
        return "efs_v1";
    }

    // Sector 0 again: AmigaDOS "DOS\x" boot block (variants 0..7).
    if &sector0[0..3] == b"DOS" && sector0[3] <= 7 {
        return "affs";
    }

    // BeOS OFS (pre-BFS, DR1..DR8): a table of contents at sector 0 whose
    // bitmap geometry has to cover the volume it claims. No magic number, so
    // this runs after every format that has one.
    if ofs::OfsToc::parse(&sector0).is_ok() {
        return "ofs";
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

    // Commodore CBM DOS (1541/1571/1581 .d64/.d71/.d81). Flat sector
    // dumps with no partition table; `looks_like_cbm` gates on the exact
    // geometry length AND the header-sector signature so we don't
    // false-positive on a same-sized blob.
    if cbm::looks_like_cbm(reader, partition_offset).is_some() {
        return "cbmdos";
    }

    // Atari DOS 2 (.atr stripped to its body, or headerless .xfd). Gated on
    // the exact disk geometry + a plausible VTOC at sector 360.
    if atari_dos::looks_like_atari_dos(reader, partition_offset).is_some() {
        return "ataridos";
    }

    // DragonDOS (flat .dsk, 40-track single- or double-sided). Same byte size
    // as a 40-track RS-DOS disk, but its directory track carries a
    // one's-complement geometry signature, so probe it first among the CoCo
    // family — the signature is a confident discriminator.
    if dragondos::looks_like_dragondos(reader, partition_offset).is_some() {
        return "dragondos";
    }

    // Acorn DFS (flat single-sided .ssd, 40- or 80-track). No magic; gated on
    // exact single-sided geometry AND a catalogue whose declared sector count
    // matches the disk size, which separates a real .ssd from a flat .dsd.
    if dfs::looks_like_dfs(reader, partition_offset).is_some() {
        return "acorndfs";
    }

    // OS-9 / NitrOS-9 RBF (flat .dsk/.vdk). Same byte size as a 35-track
    // RS-DOS disk, so it must be probed first: `looks_like_os9` validates the
    // LSN-0 identification sector against the image length and confirms the
    // root FD is a directory, which RS-DOS disks never satisfy.
    if os9::looks_like_os9(reader, partition_offset).is_some() {
        return "os9";
    }

    // RS-DOS / CoCo Disk BASIC (flat .dsk/.jvc, 35- or 40-track). No magic
    // number; `looks_like_rsdos` gates on exact geometry AND a structurally
    // consistent granule table + directory so OS-9 (same byte size) and
    // random blobs are rejected.
    if rsdos::looks_like_rsdos(reader, partition_offset).is_some() {
        return "rsdos";
    }

    // TR-DOS (ZX Spectrum Beta Disk, flat .trd). No partition table; gated on
    // the disk-info sector's id byte (0x10) + a valid disk-type byte at fixed
    // offsets, plus a size/geometry sanity check — a confident signature that
    // random same-sized blobs won't satisfy.
    if trdos::looks_like_trdos(reader, partition_offset).is_some() {
        return "trdos";
    }

    // TI-99/4A disk (flat V9T9 `.dsk`). The ASCII "DSK" marker at VIB offset
    // 0x0D plus a self-consistent geometry is a confident signature; distinct
    // from the CoCo/Dragon `.dsk` families even at a shared byte size.
    if ti99::looks_like_ti99(reader, partition_offset).is_some() {
        return "ti99";
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

    // Old-map ADFS (D-format, and S/M/L): no Disc Record, so the probe above
    // can't find it. Detect via the checksum-valid old free-space map + a Hugo
    // root directory at byte 1024.
    if adfs::detect_old_map_dformat(reader, partition_offset) {
        return "adfs";
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
        // Appliance images (Buildroot and friends) ship a SquashFS root in a
        // plain 0x83 partition.
        "squashfs" => Some("SquashFS"),
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
    } else if hpfs::looks_like_hpfs(reader, partition_offset) {
        // OS/2 HPFS: type 0x07 shares NTFS/exFAT; confirmed by the super block
        // (sector 16) + spare block (sector 17) magics.
        "hpfs"
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
        return compact_partition_reader_by_string(reader, partition_offset, type_str, true)
            .and_then(|opt| {
                opt.ok_or_else(|| {
                    format!("unsupported: APM type '{type_str}' has no compact reader")
                })
            });
    }
    // Used for size estimation; swap content doesn't affect the packed size, so
    // keep_swap=true (no need to walk for swap files here).
    compact_partition_reader(reader, partition_offset, partition_type, None, true).ok_or_else(
        || {
            format!(
                "unsupported: no compact reader for MBR type 0x{partition_type:02X} \
             at offset {partition_offset}"
            )
        },
    )
}

/// Build the **layout-preserving** compact reader for an NTFS partition:
/// allocated clusters stream verbatim at their true byte offsets; free clusters
/// emit zeros (which compress away in zstd/CHD).
///
/// We deliberately do NOT use the dense-packed `CompactNtfsReader` stream
/// directly: it prepends the boot region (LCN 0) and *also* lists LCN 0 in its
/// allocated-cluster set, so the boot cluster is emitted twice and every
/// subsequent cluster lands one slot off its real LCN. Since the packer never
/// rewrites the MFT's data runs (which reference absolute LCNs) and restore
/// writes the stream verbatim, that misalignment corrupts non-resident files.
/// The layout-preserving variant keeps every cluster at its LCN, so the MFT's
/// runs stay valid. See the `ntfs-exfat-packer-audit` memory note.
fn ntfs_compact_reader<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    let (packed, _) = CompactNtfsReader::new(reader, partition_offset).ok()?;
    let (lp, info) = packed.into_layout_preserving();
    Some((Box::new(lp), info))
}

/// Build the **layout-preserving** compact reader for an exFAT partition — the
/// exFAT sibling of [`ntfs_compact_reader`], for the same reason (the dense
/// packer would move clusters off the offsets the directory/FAT entries point
/// at). Allocated clusters stay at their true offsets; free clusters emit zeros.
fn exfat_compact_reader<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    let (packed, _) = CompactExfatReader::new(reader, partition_offset).ok()?;
    let (lp, info) = packed.into_layout_preserving();
    Some((Box::new(lp), info))
}

/// Build the **defrag** compact reader for a FAT partition: like the FAT path of
/// [`compact_partition_reader`] but each file's clusters are relocated into a
/// contiguous run (boot files first), so the restored disk is defragmented. The
/// output is the same *size* as plain compaction — only the cluster order
/// changes. Returns `None` if the partition isn't a FAT volume, so the backup
/// path falls back to ordinary compaction for non-FAT filesystems.
pub fn defrag_fat_partition_reader<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
    keep_swap: bool,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    let (reader, info) = if keep_swap {
        CompactFatReader::new_defrag(reader, partition_offset).ok()?
    } else {
        CompactFatReader::new_excluding_swap(reader, partition_offset, true).ok()?
    };
    Some((Box::new(reader), info))
}

/// Build the plain FAT compact reader, honoring the `keep_swap` opt-out: when
/// `false`, allowlisted swap/page files are Level-1 excluded (allocation kept,
/// content zeroed; §6). The single place the FAT-compaction `keep_swap` choice
/// is made for the non-defrag path.
fn fat_compact_reader<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
    keep_swap: bool,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    let (reader, info) = if keep_swap {
        CompactFatReader::new(reader, partition_offset).ok()?
    } else {
        CompactFatReader::new_excluding_swap(reader, partition_offset, false).ok()?
    };
    Some((Box::new(reader), info))
}

/// Try to create a compacted reader for a partition.
///
/// Returns `None` for unsupported filesystem types. On success, returns a
/// boxed `Read` implementation and a `CompactResult` with sizing information.
pub fn compact_partition_reader<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
    keep_swap: bool,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    // Check string-based type first (APM partitions)
    if let Some(type_str) = partition_type_string {
        return compact_partition_reader_by_string(reader, partition_offset, type_str, keep_swap)
            .unwrap_or(None);
    }
    match partition_type {
        0x01 | 0x04 | 0x06 | 0x0E | 0x11 | 0x14 | 0x16 | 0x1E | 0x0B | 0x0C | 0x1B | 0x1C => {
            fat_compact_reader(reader, partition_offset, keep_swap)
        }
        0xAF => apple_hfs_compact_reader(reader, partition_offset),
        0xA8 => {
            let (compact, info) = CompactProDosReader::new(reader, partition_offset).ok()?;
            Some((Box::new(compact), info))
        }
        // Type byte 0 (superfloppy), 0x07 and 0x83 each name several
        // filesystems, and any other byte is a wrong label: the content decides.
        _ => compact_reader_for_detected(reader, partition_offset, keep_swap),
    }
}

/// Compact reader for whatever `detect_filesystem_type` finds at the offset;
/// the type byte or GUID was only a label, and the bytes on disk get packed.
fn compact_reader_for_detected<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    keep_swap: bool,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    match detect_filesystem_type(&mut reader, partition_offset) {
        "fat" => fat_compact_reader(reader, partition_offset, keep_swap),
        "ntfs" => ntfs_compact_reader(reader, partition_offset),
        "exfat" => exfat_compact_reader(reader, partition_offset),
        "ext" => {
            let (reader, info) = CompactExtReader::new(reader, partition_offset).ok()?;
            Some((Box::new(reader), info))
        }
        "btrfs" => {
            let (reader, info) = CompactBtrfsReader::new(reader, partition_offset).ok()?;
            Some((Box::new(reader), info))
        }
        "reiserfs" => {
            let (reader, info) = CompactReiserFsReader::new(reader, partition_offset).ok()?;
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
        "hfs" | "hfsplus" => apple_hfs_compact_reader(reader, partition_offset),
        _ => None,
    }
}

/// HFS or HFS+ at the offset, as MBR type 0xAF and the Apple HFS GUID carry it.
/// A wrapped HFS+ volume is left to the wrapper-aware clone path (`None`).
fn apple_hfs_compact_reader<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
) -> Option<(Box<dyn Read + Send>, CompactResult)> {
    let (fs_type, hfsplus_offset) = resolve_apple_hfs(&mut reader, partition_offset);
    match fs_type {
        "hfsplus" if hfsplus_offset != partition_offset => None,
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
    keep_swap: bool,
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
            keep_swap,
        );
    }
    match partition_type {
        0x83 | 0xAF | 0xA8 => {
            return compact_partition_reader(
                reader,
                partition_offset,
                partition_type,
                partition_type_string,
                keep_swap,
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
                "fat" => fat_compact_reader(reader, partition_offset, keep_swap)?,
                "ntfs" => ntfs_compact_reader(reader, partition_offset)?,
                "exfat" => exfat_compact_reader(reader, partition_offset)?,
                _ => {
                    return compact_partition_reader(
                        reader,
                        partition_offset,
                        partition_type,
                        partition_type_string,
                        keep_swap,
                    );
                }
            }
        }
        0x01 | 0x04 | 0x06 | 0x0E | 0x14 | 0x16 | 0x1E | 0x0B | 0x0C | 0x1B | 0x1C => {
            fat_compact_reader(reader, partition_offset, keep_swap)?
        }
        0x07 => {
            let fs_type = detect_0x07_type(&mut reader, partition_offset);
            match fs_type {
                "ntfs" => ntfs_compact_reader(reader, partition_offset)?,
                "exfat" => exfat_compact_reader(reader, partition_offset)?,
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
    effective_partition_size_reported(
        reader,
        partition_offset,
        partition_type,
        partition_type_string,
    )
    .ok()
}

/// [`effective_partition_size`] keeping the reason it failed.
///
/// The `Option`-returning form discards both the open error and the
/// `last_data_byte` error, and a caller that treats `None` as "no trim
/// possible" then silently images the whole partition. That is how a
/// PowerPC ext backup came out at 32 MiB where the desktop produced 4.5 MiB:
/// identical compact analysis, identical `used_size`, but `minimum_size_bytes`
/// missing from the metadata entirely, with nothing anywhere saying why. Give
/// callers that can log a way to say what went wrong.
pub fn effective_partition_size_reported<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
) -> Result<u64, String> {
    let mut fs = open_filesystem(
        reader,
        partition_offset,
        partition_type,
        partition_type_string,
    )
    .map_err(|e| format!("cannot open filesystem: {e}"))?;
    fs.last_data_byte()
        .map_err(|e| format!("last_data_byte failed: {e}"))
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
            // Apple HFS/HFS+ GPT partition GUID (UDIF DMG / hdiutil disks).
            "48465300-0000-11AA-AA11-00306543ECAC" => "HFS/HFS+",
            // Apple APFS GPT partition GUID.
            "7C3457EF-0000-11AA-AA11-00306543ECAC" => "APFS",
            "Apple_UNIX_SVR2" => "ext/btrfs/xfs/reiserfs/UFS/JFS",
            "Linux" => "ext/btrfs/xfs/reiserfs/UFS/JFS",
            // GPT Linux Filesystem / Linux Home GUIDs.
            "0FC63DAF-8483-4772-8E79-3D69D8477DE4" | "933AC7E1-2EB4-4F13-B844-0E14E2AEF915" => {
                "ext/btrfs/xfs/reiserfs/UFS/JFS"
            }
            // GPT EFI System Partition: FAT by specification.
            "C12A7328-F81F-11D2-BA4B-00A0C93EC93B" => "FAT",
            // GPT Microsoft Basic Data and Windows Recovery.
            "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7" => "NTFS/exFAT/FAT",
            "DE94BBA4-06D1-4D40-A16A-BFD50179D6AC" => "NTFS",
            // Amiga boot block present, no AmigaDOS filesystem (custom
            // bootblock / diagnostic disk). Browsable via the carve view.
            "Amiga-NDOS" => "Amiga NDOS (no filesystem)",
            // Container-identified, so content probing cannot name them and
            // the write path called both "unknown" (R-034).
            "lisafs" => "Apple Lisa File System",
            "Alto BFS" => "Alto BFS",
            "Be_BFS" => "BFS (BeOS)",
            "human68k" => "Human68k",
            _ => "unknown",
        };
    }
    match partition_type {
        0xAF => "HFS/HFS+",
        0x83 => "ext/btrfs/xfs/reiserfs/UFS/JFS",
        0xA8 => "ProDOS",
        0x07 => "NTFS/exFAT/HPFS",
        // 0x11 is hidden FAT12; 0xEF is an EFI System Partition on an MBR disk.
        0x01 | 0x04 | 0x06 | 0x0E | 0x11 | 0x14 | 0x16 | 0x1E | 0x0B | 0x0C | 0x1B | 0x1C
        | 0xEF => "FAT",
        // Windows Recovery Environment.
        0x27 => "NTFS",
        // SGI synthetic type bytes (PartitionTable::Sgi).
        0xA0 => "XFS",
        0xA1 => "SGI EFS",
        0xA2 => "SGI EFS v1",
        // Minix (0x81) and old Minix (0x80).
        0x80 | 0x81 => "Minix",
        0xEB => "BFS (BeOS)",
        0xEC => "BeOS OFS",
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
                | "Linux"
                // GPT Apple HFS/HFS+, Linux Filesystem and Linux Home GUIDs.
                | "48465300-0000-11AA-AA11-00306543ECAC"
                | "0FC63DAF-8483-4772-8E79-3D69D8477DE4"
                | "933AC7E1-2EB4-4F13-B844-0E14E2AEF915",
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
        return matches!(
            s,
            "Apple_HFS" | "Apple_HFSX" | "Apple_HFS+" | "48465300-0000-11AA-AA11-00306543ECAC"
        );
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
        return matches!(
            s,
            "Apple_HFS"
                | "Apple_HFSX"
                | "Apple_UNIX_SVR2"
                | "Linux"
                | "48465300-0000-11AA-AA11-00306543ECAC"
                | "0FC63DAF-8483-4772-8E79-3D69D8477DE4"
                | "933AC7E1-2EB4-4F13-B844-0E14E2AEF915"
        );
    }
    // 0xA1 (SGI EFS) and 0xA2 (SGI EFS v1): the conservative floor requires
    // an inode-table walk.
    matches!(partition_type, 0xAF | 0x83 | 0xA8 | 0xA1 | 0xA2)
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
    let mut fs = match open_filesystem_sized(
        reader,
        partition_offset,
        Some(partition_size),
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
    reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    open_filesystem_with_passphrase(
        reader,
        partition_offset,
        partition_type,
        partition_type_string,
        None,
    )
}

/// Like [`open_filesystem`], but tells the driver how long the partition is.
///
/// Most filesystems record their own size and ignore this. AFFS does not — its
/// root block sits at the volume's midpoint, so a driver handed a whole-disk
/// reader infers the midpoint of the *disk* and fails on any partition that is
/// not last (R-042). Callers that have a partition table should prefer this;
/// `None` is the honest answer for a bare image, where the reader's end really
/// is the volume's end.
pub fn open_filesystem_sized<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
    partition_size: Option<u64>,
    partition_type: u8,
    partition_type_string: Option<&str>,
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    open_filesystem_full(
        reader,
        partition_offset,
        partition_size,
        partition_type,
        partition_type_string,
        None,
    )
}

/// Like [`open_filesystem`], but carries an optional filesystem-level
/// `passphrase` for volumes that encrypt their own contents (APFS FileVault).
/// The passphrase is ignored by every filesystem that isn't encrypted; on an
/// encrypted APFS volume, `None` opens it locked (browse then reports
/// "passphrase required") and a wrong passphrase is an error.
pub fn open_filesystem_with_passphrase<R: Read + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
    passphrase: Option<&str>,
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    open_filesystem_full(
        reader,
        partition_offset,
        None,
        partition_type,
        partition_type_string,
        passphrase,
    )
}

/// The dispatch every other opener delegates to. `partition_size` is threaded
/// to the drivers that cannot derive it themselves; see [`open_filesystem_sized`].
pub fn open_filesystem_full<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    partition_size: Option<u64>,
    partition_type: u8,
    partition_type_string: Option<&str>,
    passphrase: Option<&str>,
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    // Check string-based type first (APM partitions)
    if let Some(type_str) = partition_type_string {
        return open_filesystem_by_string(
            reader,
            partition_offset,
            partition_size,
            type_str,
            passphrase,
        );
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
                "hpfs" => Ok(Box::new(hpfs::HpfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "oric_jasmin" => Ok(Box::new(oric::OricFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "hfs" => Ok(Box::new(hfs::HfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                // A bare HFS-wrapped HFS+ image (an OS 9 "Extended" volume) keeps
                // its real volume behind the wrapper; opening at byte 0 saw an MDB.
                "hfsplus" => {
                    let (_, hfsplus_offset) = resolve_apple_hfs(&mut reader, partition_offset);
                    Ok(Box::new(hfsplus::HfsPlusFilesystem::open(
                        reader,
                        hfsplus_offset,
                    )?))
                }
                "mfs" => Ok(Box::new(mfs::MfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ext" => Ok(Box::new(ext::ExtFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "squashfs" => Ok(Box::new(squashfs::SquashfsFilesystem::open(
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
                "cbmdos" => Ok(Box::new(cbm::CbmFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ataridos" => Ok(Box::new(atari_dos::AtariDosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "rsdos" => Ok(Box::new(rsdos::RsdosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "dragondos" => Ok(Box::new(dragondos::DragonDosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "acorndfs" => Ok(Box::new(dfs::DfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "os9" => Ok(Box::new(os9::Os9Filesystem::open(
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
                "bfs" => Ok(Box::new(bfs::BfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ofs" => Ok(Box::new(ofs::OfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "efs" => Ok(Box::new(efs::EfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "efs_v1" => Ok(Box::new(efs_v1::EfsV1Filesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "minix" => Ok(Box::new(minix::MinixFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ucsd" => Ok(Box::new(ucsd::UcsdFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "trdos" => Ok(Box::new(trdos::TrdosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ti99" => Ok(Box::new(ti99::Ti99Filesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "affs" => Ok(Box::new(affs::AffsFilesystem::open_sized(
                    reader,
                    partition_offset,
                    partition_size,
                )?)),
                "apfs" => Ok(Box::new(apfs::ApfsFilesystem::open_with_passphrase(
                    reader,
                    partition_offset,
                    passphrase,
                )?)),
                // No filesystem recognized. Rather than erroring, fall back to
                // the synthetic carve view so the user can still pull a raw
                // copy and any recoverable text/JSON content off the image.
                _ => Ok(Box::new(carve::CarveFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
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
                "hpfs" => Ok(Box::new(hpfs::HpfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                // `detect_0x07_type` only ever answers ntfs / exfat / hpfs, so
                // anything else means the type byte is simply wrong about its
                // own contents - FAT under 0x07 is the common case. Hand it to
                // full detection rather than refusing on the strength of a
                // label the disk has already contradicted.
                _ => open_filesystem_with_passphrase(reader, partition_offset, 0x00, None, None),
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
                "squashfs" => Ok(Box::new(squashfs::SquashfsFilesystem::open(
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
                // fdisk's default byte hosts Minix, HFS+ and more; the wrong
                // label is not proof there is nothing here (see the `_` arm).
                _ => open_filesystem_with_passphrase(
                    reader,
                    partition_offset,
                    0x00,
                    None,
                    passphrase,
                ),
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
        // BeOS BFS (type byte 0xEB). The BeOS installer also writes 0xEB for
        // its swap partition, so `open` validates the superblock magic.
        0xEB => Ok(Box::new(bfs::BfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // SGI EFS — synthetic type byte emitted by PartitionTable::Sgi.
        0xA1 => Ok(Box::new(efs::EfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // SGI EFS v1 — synthetic byte from PartitionTable::SgiDkLabel.
        0xA2 => Ok(Box::new(efs_v1::EfsV1Filesystem::open(
            reader,
            partition_offset,
        )?)),
        // SGI XFS — synthetic type byte emitted by PartitionTable::Sgi.
        0xA0 => Ok(Box::new(xfs::XfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // Minix (MBR type 0x81) and old Minix (0x80). The superblock magic is
        // validated in open(), so a mislabeled partition fails cleanly.
        0x80 | 0x81 => Ok(Box::new(minix::MinixFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // An unhandled type byte is not proof there is nothing here. MBR type
        // codes are a sprawl - the "hidden" FAT variants (0x11/0x14/0x1B/0x1C),
        // 0x27 for Windows RE, 0xEF for an ESP on an MBR disk - and no table
        // will ever have all of them. If the superblock names a filesystem we
        // support, open it; the type byte was simply wrong.
        //
        // The original error is kept for the case that matters: content we
        // genuinely do not recognize. Naming the type byte there is more use
        // than silently handing back the carve view auto-detect would end at.
        _ => {
            if detect_filesystem_type(&mut reader, partition_offset) == "unknown" {
                return Err(FilesystemError::Unsupported(format!(
                    "filesystem type 0x{:02X} not supported for browsing",
                    partition_type
                )));
            }
            open_filesystem_with_passphrase(reader, partition_offset, 0x00, None, passphrase)
        }
    }
}

/// What a caller knows about *where* a filesystem lives, beyond its offset.
///
/// Almost every driver writes surgically inside structures it read from the
/// partition, and needs none of this. The ones that rewrite their whole image
/// do: SquashFS has no in-place write at all, so committing an edit means
/// replacing every byte, and that raises two questions an offset cannot answer
/// — how far it may grow, and whether there is a file it could be swapped in
/// for atomically instead of overwritten in place.
///
/// Every field defaults to "unknown", which is always the safe reading.
#[derive(Default, Clone, Copy)]
pub struct EditContext<'a> {
    /// Bytes the filesystem may occupy where it sits. `None` when the caller
    /// doesn't know, which a driver must treat as "assume it may not grow".
    pub partition_len: Option<u64>,
    /// Path of the backing file, set **only** when the handle is a plain file
    /// on disk *and* the filesystem occupies all of it — so replacing the file
    /// replaces exactly the filesystem and nothing else.
    ///
    /// Leave it `None` for a partition inside a larger image, a decoded
    /// container temp, a CHD/QCOW2 session, or a remote handle: renaming over
    /// any of those would destroy the surrounding image.
    pub whole_file_path: Option<&'a std::path::Path>,
    /// A ceiling the *user* asked for, on top of whatever the container
    /// imposes. `None` means "no request" — the container still binds.
    ///
    /// Unlike the other two fields this is a preference rather than a fact
    /// about placement, and it exists for the same reason they do: only a
    /// driver that rewrites its whole image can meaningfully be given one.
    pub rebuild_budget: Option<squashfs_edit::SizeBudget>,
}

/// Open a filesystem for editing (read + write access).
///
/// Same dispatch logic as `open_filesystem` but requires a writable reader and
/// returns a `Box<dyn EditableFilesystem>`.
///
/// Nothing is known about the surrounding container, so a driver that needs
/// that falls back to its safest assumption. Prefer
/// [`open_editable_filesystem_with`] when the caller can fill in an
/// [`EditContext`].
pub fn open_editable_filesystem<R: Read + Write + Seek + Send + 'static>(
    reader: R,
    partition_offset: u64,
    partition_type: u8,
    partition_type_string: Option<&str>,
) -> Result<Box<dyn EditableFilesystem>, FilesystemError> {
    open_editable_filesystem_with(
        reader,
        partition_offset,
        EditContext::default(),
        partition_type,
        partition_type_string,
    )
}

/// [`open_editable_filesystem`], told what the caller knows about the
/// filesystem's surroundings. See [`EditContext`].
pub fn open_editable_filesystem_with<R: Read + Write + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    edit_ctx: EditContext<'_>,
    partition_type: u8,
    partition_type_string: Option<&str>,
) -> Result<Box<dyn EditableFilesystem>, FilesystemError> {
    let partition_len = edit_ctx.partition_len;
    // Set when the type string is one we have no arm for, so the type-byte
    // match below is entered in auto-detect mode instead of erroring out.
    let mut auto_detect = false;
    // Check string-based type first (APM partitions)
    if let Some(type_str) = partition_type_string {
        match type_str {
            // "Apple_HFS" (APM) and "48465300-..." (GPT "Apple HFS/HFS+" type
            // GUID, from `hdiutil create` / UDIF DMGs) both resolve through the
            // same HFS-vs-HFS+ probe.
            "Apple_HFS" | "48465300-0000-11AA-AA11-00306543ECAC" => {
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
            // BeOS/PPC APM volumes.
            "Be_BFS" => {
                return Ok(Box::new(bfs::BfsFilesystem::open(
                    reader,
                    partition_offset,
                )?));
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
                    "jfs" => Ok(Box::new(jfs::JfsFilesystem::open(
                        reader,
                        partition_offset,
                    )?)),
                    // A/UX and NetBSD/mac68k slices: the read arm already opened them.
                    "ufs" => Ok(Box::new(ufs::UfsFilesystem::open(
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
                return Ok(Box::new(affs::AffsFilesystem::open_sized(
                    reader,
                    partition_offset,
                    partition_len,
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
            "adfs" | "ADFS" => {
                return Ok(Box::new(adfs::AdfsFilesystem::open(
                    reader,
                    partition_offset,
                )?));
            }
            "acorndfs" => {
                // A side of a double-sided `.dsd`: length comes from this
                // side's own catalogue, not the (two-side) stream length.
                let geom =
                    dfs::dfs_side_geometry(&mut reader, partition_offset).ok_or_else(|| {
                        FilesystemError::InvalidData(
                            "no Acorn DFS catalogue at this offset (expected a .dsd side)".into(),
                        )
                    })?;
                return Ok(Box::new(dfs::DfsFilesystem::open_within(
                    reader,
                    partition_offset,
                    geom.body_len(),
                )?));
            }
            // Unrecognized type string: fall through to content detection
            // below, exactly as the read path does. A GPT type GUID describes
            // what a partition is *for*, not what is in it - an EFI System
            // Partition is plain FAT32 - so refusing here made the ESP
            // read-only for no reason, while an identical FAT32 in an MBR
            // partition edited fine.
            _ => {
                auto_detect = true;
            }
        }
    }
    // Only forced when a type string was present and unrecognized; a partition
    // with no type string keeps whatever type byte it came with.
    let partition_type = if auto_detect { 0x00 } else { partition_type };
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
                "hpfs" => Ok(Box::new(hpfs::HpfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "oric_jasmin" => Ok(Box::new(oric::OricFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ext" => Ok(Box::new(ext::ExtFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                // SquashFS is read-only on disk, so "editing" is a whole-image
                // rebuild: the editor reads the tree into memory, mutates it,
                // and rebuilds on sync. See `squashfs_edit`.
                //
                // A bare `.squashfs` at offset 0 is the file itself and may
                // grow, so it gets no capacity; reaching this arm at a non-zero
                // offset means the image is hosted in something, and then the
                // declared length is a hard boundary.
                "squashfs" => Ok(Box::new(squashfs_edit::SquashfsEditor::open_within(
                    reader,
                    partition_offset,
                    if partition_offset == 0 {
                        None
                    } else {
                        partition_len
                    },
                    edit_ctx
                        .rebuild_budget
                        .unwrap_or(squashfs_edit::SizeBudget::Fit),
                    edit_ctx.whole_file_path,
                )?)),
                "hfs" => Ok(Box::new(hfs::HfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "hfsplus" => {
                    let (_, hfsplus_offset) = resolve_apple_hfs(&mut reader, partition_offset);
                    let mut fs = hfsplus::HfsPlusFilesystem::open(reader, hfsplus_offset)?;
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
                "cbmdos" => Ok(Box::new(cbm::CbmFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ataridos" => Ok(Box::new(atari_dos::AtariDosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "rsdos" => Ok(Box::new(rsdos::RsdosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "dragondos" => Ok(Box::new(dragondos::DragonDosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "acorndfs" => Ok(Box::new(dfs::DfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "os9" => Ok(Box::new(os9::Os9Filesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "efs" => Ok(Box::new(efs::EfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "efs_v1" => Ok(Box::new(efs_v1::EfsV1Filesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "minix" => Ok(Box::new(minix::MinixFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ucsd" => Ok(Box::new(ucsd::UcsdFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "trdos" => Ok(Box::new(trdos::TrdosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ti99" => Ok(Box::new(ti99::Ti99Filesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "affs" => Ok(Box::new(affs::AffsFilesystem::open_sized(
                    reader,
                    partition_offset,
                    partition_len,
                )?)),
                "xfs" => Ok(Box::new(xfs::XfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "qdos" => Ok(Box::new(qdos::QdosFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "adfs" => Ok(Box::new(adfs::AdfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                // UFS/FFS edit mirrors the read dispatch above (0x00 arm of
                // open_filesystem). The EditableFilesystem impl is fixture-
                // tested (create/rename/delete + fsck-clean); this arm is what
                // makes it reachable.
                "ufs" => Ok(Box::new(ufs::UfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                // JFS is already editable through the 0x83 arm below; a bare
                // JFS image is the same filesystem, so it opens here too.
                // Its edit surface is metadata-only (`set_permissions` /
                // `set_owner` / `repair`) — create/delete still report
                // Unsupported, which is the caller's cue.
                "jfs" => Ok(Box::new(jfs::JfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "ofs" => Ok(Box::new(ofs::OfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                "bfs" => Ok(Box::new(bfs::BfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                _ => {
                    // Name it the way the read path does. Detection returns
                    // "unknown" for filesystems identified by their container
                    // rather than a superblock, so reporting that told the user
                    // the disk was unreadable moments after `ls` read it (R-034).
                    let named = match fs_name_for(partition_type, partition_type_string) {
                        "unknown" => partition_type_string.unwrap_or(fs_type),
                        n => n,
                    };
                    Err(FilesystemError::Unsupported(format!(
                        "editing not yet supported for filesystem type '{named}'"
                    )))
                }
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
                "hpfs" => Ok(Box::new(hpfs::HpfsFilesystem::open(
                    reader,
                    partition_offset,
                )?)),
                // As on the read path: the type byte has already been
                // contradicted by the contents, so let the contents decide.
                // Refusing here made FAT under 0x07 browsable but not editable.
                _ => open_editable_filesystem_with(reader, partition_offset, edit_ctx, 0x00, None),
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
                // Appliance images ship their SquashFS root in a plain 0x83
                // partition. Editing rebuilds the whole image, so the partition
                // length is a real boundary here, not a formality — see
                // `squashfs_edit::SquashfsEditor::open_within`.
                // A partition is never the whole file, so no atomic-rename path
                // is offered here even if the caller named one.
                "squashfs" => Ok(Box::new(squashfs_edit::SquashfsEditor::open_within(
                    reader,
                    partition_offset,
                    partition_len,
                    edit_ctx
                        .rebuild_budget
                        .unwrap_or(squashfs_edit::SizeBudget::Fit),
                    None,
                )?)),
                "xfs" => Ok(Box::new(xfs::XfsFilesystem::open(
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
                _ => open_editable_filesystem_with(reader, partition_offset, edit_ctx, 0x00, None),
            }
        }
        // ProDOS
        0xA8 => Ok(Box::new(prodos::ProDosFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // BeOS BFS
        0xEB => Ok(Box::new(bfs::BfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // SGI EFS — synthetic type byte emitted by PartitionTable::Sgi.
        0xA1 => Ok(Box::new(efs::EfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // SGI EFS v1 — synthetic type byte emitted by PartitionTable::SgiDkLabel.
        0xA2 => Ok(Box::new(efs_v1::EfsV1Filesystem::open(
            reader,
            partition_offset,
        )?)),
        // SGI XFS — synthetic type byte emitted by PartitionTable::Sgi.
        0xA0 => Ok(Box::new(xfs::XfsFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // Minix (MBR type 0x81) and old Minix (0x80).
        0x80 | 0x81 => Ok(Box::new(minix::MinixFilesystem::open(
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
        // Same reasoning as the read path: an unhandled type byte is a wrong
        // label, not an absent filesystem. Without this, a FAT partition
        // stamped 0x27 (Windows RE) or 0x11 (hidden FAT12) browsed fine and
        // then refused every write - the partition type quietly deciding that
        // a volume is read-only.
        _ => {
            if detect_filesystem_type(&mut reader, partition_offset) == "unknown" {
                return Err(FilesystemError::Unsupported(format!(
                    "editing not yet supported for filesystem type 0x{partition_type:02X}"
                )));
            }
            open_editable_filesystem_with(reader, partition_offset, edit_ctx, 0x00, None)
        }
    }
}

/// Open a filesystem by APM partition type string.
fn open_filesystem_by_string<R: Read + Seek + Send + 'static>(
    mut reader: R,
    partition_offset: u64,
    partition_size: Option<u64>,
    type_str: &str,
    passphrase: Option<&str>,
) -> Result<Box<dyn Filesystem>, FilesystemError> {
    match type_str {
        // "Apple_HFS" is the APM type string; "48465300-..." is the GPT
        // "Apple HFS/HFS+" type GUID (an HFS or HFS+ volume in a GPT-wrapped
        // disk, as produced by `hdiutil create` / UDIF DMGs). Both resolve
        // through the same HFS-vs-HFS+ probe.
        "Apple_HFS" | "48465300-0000-11AA-AA11-00306543ECAC" => {
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
        // BeOS/PPC writes an APM whose BFS partitions carry "Be_BFS".
        "Be_BFS" => Ok(Box::new(bfs::BfsFilesystem::open(
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
                // Same as the Linux Filesystem GUID: detection already named
                // it, so open it rather than reporting what we just identified
                // as "unrecognized". A UFS or JFS A/UX partition lands here.
                _ if fs_type != "unknown" => open_filesystem_with_passphrase(
                    reader,
                    partition_offset,
                    0x00,
                    None,
                    passphrase,
                ),
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
        s if is_amiga_dos_type(s) => Ok(Box::new(affs::AffsFilesystem::open_sized(
            reader,
            partition_offset,
            partition_size,
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
        // Acorn DFS side of a double-sided `.dsd` (PartitionTable::Dsd).
        // The two sides live in one `side0 ‖ side1` buffer, so side 0's
        // distance-to-end spans both sides; derive this side's length from
        // its own catalogue and open it bounded to that.
        "acorndfs" => {
            let geom = dfs::dfs_side_geometry(&mut reader, partition_offset).ok_or_else(|| {
                FilesystemError::InvalidData(
                    "no Acorn DFS catalogue at this offset (expected a .dsd side)".into(),
                )
            })?;
            Ok(Box::new(dfs::DfsFilesystem::open_within(
                reader,
                partition_offset,
                geom.body_len(),
            )?))
        }
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
                // This arm knew the answer and threw it away: it reported
                // "unrecognized filesystem (detected: fat)" for a FAT volume in
                // a Linux Filesystem partition. Anything detection can name,
                // the auto-detect path can open.
                _ if fs_type != "unknown" => open_filesystem_with_passphrase(
                    reader,
                    partition_offset,
                    0x00,
                    None,
                    passphrase,
                ),
                _ => Err(FilesystemError::Unsupported(format!(
                    "Linux Filesystem GPT partition: unrecognized filesystem (detected: {fs_type})"
                ))),
            }
        }
        // NDOS Amiga disk: a valid boot block (so the ROM runs its code) with
        // no AmigaDOS root block — a custom bootblock disk (demo / diagnostic)
        // that writes raw sectors instead of using a filesystem. There's
        // nothing to mount, so fall back to the synthetic carve view, which
        // surfaces the whole disk plus any recoverable text/JSON payloads.
        "Amiga-NDOS" => Ok(Box::new(carve::CarveFilesystem::open(
            reader,
            partition_offset,
        )?)),
        // Apple APFS GPT partition GUID. The partition holds an APFS
        // *container* (which may host several volumes); the driver opens the
        // container and browses its first non-empty volume. Read-only.
        "7C3457EF-0000-11AA-AA11-00306543ECAC" => Ok(Box::new(
            apfs::ApfsFilesystem::open_with_passphrase(reader, partition_offset, passphrase)?,
        )),
        // Apple Lisa File System: the tag-bearing DiskCopy 4.2 / DART container
        // is opened as a whole (the driver parses the header + 12-byte sector
        // tags itself), so `partition_offset` is ignored. Read-only.
        "lisafs" => Ok(Box::new(lisa::LisaFilesystem::open(reader)?)),
        // An unrecognized partition *type* is not a reason to refuse. On GPT the
        // type GUID says what a partition is **for**, not what is inside it: the
        // EFI System Partition is plain FAT32, "Microsoft Basic Data" is
        // NTFS / exFAT / FAT, "Windows Recovery" is NTFS. Only three GUIDs ever
        // had arms here, so browsing the ESP on any ordinary PC failed with
        // "APM partition type 'C12A7328-...' not supported" - naming the wrong
        // partition scheme, about a filesystem we have supported all along.
        //
        // So fall through to the superblock, which is the real authority on
        // what a partition holds. Auto-detect ends at the carve view rather
        // than an error, so an genuinely unknown payload still opens as
        // something inspectable instead of a dead end.
        _ => open_filesystem_with_passphrase(reader, partition_offset, 0x00, None, passphrase),
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
    keep_swap: bool,
) -> Result<Option<(Box<dyn Read + Send>, CompactResult)>, String> {
    match type_str {
        "Apple_HFS" | "48465300-0000-11AA-AA11-00306543ECAC" => {
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
        // A GPT GUID says what a partition is for, not what is in it (the ESP
        // is FAT, Basic Data is NTFS/exFAT/FAT): ask the superblock, as reads do.
        _ => Ok(compact_reader_for_detected(
            reader,
            partition_offset,
            keep_swap,
        )),
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

// --- partition-capability gates --------------------------------------------
// Pure `(type byte / type string / hint name) -> bool` predicates that every
// presentation layer (the Inspect tab, Commander Mode panes, the CLI, a future
// TUI) uses to decide which actions a partition row supports. They live here in
// the engine — not in any one view — so all UIs share one source of truth.
// `partition_is_browsable` is the combined gate the partition grids actually
// call; the others gate the more specific Check / Expand actions.

/// True for an MBR partition type byte whose filesystem the browser can open.
pub fn is_browsable_type(ptype: u8) -> bool {
    matches!(
        ptype,
        0x01 | 0x04
            | 0x06
            | 0x07
            | 0x0B
            | 0x0C
            | 0x0E
            | 0x11
            | 0x14
            | 0x16
            | 0x1B
            | 0x1C
            | 0x1E
            | 0x83
            | 0xA0
            | 0xA1
            | 0xA2
            // BSD / Solaris UFS slices; 0xA8 (Apple UFS) used to be the only one.
            | 0xA5
            | 0xA6
            | 0xA8
            | 0xA9
            | 0xAF
            | 0xBF
            // BeOS BFS.
            | 0xEB
            // Minix; `open_filesystem` has handled both bytes since the driver landed.
            | 0x80
            | 0x81
    )
}

/// True for an APM/GPT partition type *string* whose filesystem the browser can
/// open (AmigaDOS/PFS3/SFS DosType tags, the Apple_* APM types, the GPT Linux
/// GUID, and the synthetic `Amiga-NDOS` carve view). Notably excludes APM
/// driver/partition-map entries like `Apple_Driver_IOKit`, which carry no
/// filesystem.
pub fn is_browsable_type_string(type_str: Option<&str>) -> bool {
    let Some(s) = type_str else {
        return false;
    };
    if is_amiga_dos_type(s) || is_amiga_pfs3_type(s) || is_amiga_sfs_type(s) {
        return true;
    }
    matches!(
        s,
        "Apple_HFS"
            | "Apple_HFSX"
            | "Apple_HFS+"
            // BeOS/PPC writes an APM whose BFS partitions carry "Be_BFS".
            | "Be_BFS"
            | "Apple_UNIX_SVR2"
            | "Apple_UNIX_SRVR2"
            | "Apple_PRODOS"
            | "Apple_ProDOS"
            // GPT "Linux Filesystem" GUID — ext, btrfs, or xfs at runtime.
            | "0FC63DAF-8483-4772-8E79-3D69D8477DE4"
            // GPT "Apple HFS/HFS+" GUID — HFS or HFS+ volume in a GPT-wrapped
            // disk (UDIF DMG / hdiutil create).
            | "48465300-0000-11AA-AA11-00306543ECAC"
            // GPT "Apple APFS" container GUID — read-only browse of the
            // container's first unencrypted volume.
            | "7C3457EF-0000-11AA-AA11-00306543ECAC"
            // Custom bootblock Amiga disk with no filesystem — browsable via
            // the synthetic carve view (whole-disk + recoverable text/JSON).
            | "Amiga-NDOS"
            // Apple Lisa File System (tag-bearing DiskCopy 4.2 / DART container).
            | "lisafs"
    )
}

/// True for a partition-less (superfloppy, type byte 0) image whose detected
/// filesystem hint name is browsable.
///
/// This MUST cover every `fs_hint` that `partition::detect_superfloppy` can
/// emit: a superfloppy opens with type byte 0, which makes `open_filesystem`
/// auto-detect the filesystem from the superblock — so *any* hint
/// `detect_superfloppy` produces is one the browser can open. Keep this in sync
/// with that function; an omission silently makes the GUI refuse a filesystem
/// the engine handles (e.g. EFS / MFS / QDOS / human68k / ADFS CD-ROms and
/// floppies). `"Unknown"` covers the partition-table-present-but-unrecognized
/// case.
pub fn is_browsable_superfloppy(ptype: u8, type_name: &str) -> bool {
    if ptype != 0 {
        return false;
    }
    matches!(
        type_name,
        "FAT"
            | "HFS"
            | "HFS+"
            | "NTFS"
            | "exFAT"
            // Raw APFS container image (dd of an Apple_APFS partition, or a
            // partition-less container); auto-detected at byte 0 by the NXSB
            // magic and opened read-only.
            | "APFS"
            | "ProDOS"
            // Apple DOS 3.3 (the `detect_superfloppy` hint for a 140 KB Apple II
            // floppy / WOZ); open_filesystem auto-detects it as `applesdos33`.
            | "DOS 3.3"
            | "XFS"
            | "ext"
            | "btrfs"
            // Bare JFS / UFS / ReiserFS dumps. `detect_filesystem_type` has
            // always known these; only `detect_superfloppy` was missing the
            // probes, so a raw partition dump could not be opened at all.
            | "JFS"
            | "UFS"
            | "ReiserFS"
            // Appliance / live-CD SquashFS root, shipped with no partition
            // table.
            | "squashfs"
            | "EFS"
            // A bare EFS v1 volume names itself in full; "EFS" above is IRIX 5.3+.
            | "SGI EFS v1"
            | "MFS"
            // Bare BeOS and HPFS volumes; all three drivers read, edit and fsck.
            | "BFS"
            | "BeOS OFS"
            | "HPFS"
            // Minix (raw floppy / hard-disk superfloppy); auto-detected at
            // byte 1024 by both detect_superfloppy and detect_filesystem_type.
            | "minix"
            // UCSD p-System (Apple II/III Pascal floppies); block-2 volume label.
            | "ucsd"
            | "ADFS"
            | "ANDOS"
            | "QDOS"
            | "human68k"
            | "Amiga-NDOS"
            | "Alto BFS"
            | "Pilot/Cedar"
            | "lisafs"
            // 8-bit / retro floppy filesystems the engine auto-detects at
            // byte 0. These were previously omitted, so the inspect grid and
            // Commander silently refused to browse them.
            | "Acorn DFS"
            | "Atari DOS"
            | "CBM DOS"
            | "DragonDOS"
            | "OS-9"
            | "RS-DOS"
            // TR-DOS (ZX Spectrum Beta Disk, flat .trd); auto-detected at the
            // disk-info sector (offset 0x800) as `trdos`.
            | "TR-DOS"
            // TI-99/4A (flat V9T9 .dsk); auto-detected via the VIB "DSK" marker.
            | "TI-99"
            // Oric Jasmin (flat 256-byte-sector .dsk); free-map markers at
            // block 340.
            | "Oric Jasmin"
            | "Unknown"
    )
}

/// Fallback: detect FAT from the human-readable type name (older backups that
/// didn't store a `partition_type_byte`).
fn is_fat_name(name: &str) -> bool {
    name.to_ascii_lowercase().contains("fat")
}

/// Roles that never hold a filesystem: swap, boot, alternate-sector and
/// replacement areas, and unassigned slots. Shared by all four disk labels.
fn is_reserved_slice_role(role: &str) -> bool {
    matches!(
        role.trim().to_ascii_lowercase().as_str(),
        "swap"
            | "boot"
            | "altsctr"
            | "unassigned"
            | "reserved"
            | "backup"
            | "volhdr"
            | "trkrepl"
            | "secrepl"
            | "volume"
            | "xfslog"
            | "raw"
    )
}

/// True for a slice of a Unix disk label (NeXT, Sun VTOC, Solaris x86 VTOC,
/// SGI volume header) whose entries carry no type byte and no type string.
pub fn is_browsable_scheme_slice(type_name: &str) -> bool {
    // The role sits in the parentheses for NeXT and Solaris.
    let paren_role = type_name.rfind('(').and_then(|i| {
        type_name[i + 1..]
            .find(')')
            .map(|j| &type_name[i + 1..i + 1 + j])
    });

    if let Some(rest) = type_name.strip_prefix("Sun ") {
        // "Sun <tag> (UFS?)" — the tag is before the parenthesis here.
        let tag = rest.split(" (").next().unwrap_or("");
        return !is_reserved_slice_role(tag);
    }
    if type_name.starts_with("Solaris s") || type_name.starts_with("NeXT ") {
        return match paren_role {
            Some(r) => !is_reserved_slice_role(r),
            None => false,
        };
    }
    if let Some(rest) = type_name.strip_prefix("SGI ") {
        // "SGI BSD" (volume header) and "SGI swap" (disk label) both lead
        // with the deciding word.
        let head = rest.split(" (").next().unwrap_or("");
        return !is_reserved_slice_role(head);
    }
    false
}

/// The combined "can this partition be opened in the browser?" gate — the OR of
/// the type-byte, type-string, FAT-name fallback, superfloppy-hint and
/// disk-label-slice checks.
/// This is the single predicate the Inspect grid and the Commander pane both
/// call to decide whether to offer Browse (and, for Commander, whether to
/// auto-open a partition at all).
pub fn partition_is_browsable(ptype: u8, type_string: Option<&str>, type_name: &str) -> bool {
    is_browsable_type(ptype)
        || is_fat_name(type_name)
        || is_browsable_type_string(type_string)
        || is_browsable_superfloppy(ptype, type_name)
        || is_browsable_scheme_slice(type_name)
}

/// True when a partition row is a classic-HFS **superfloppy** — a flat,
/// partition-less HFS volume at LBA 0 (a BasiliskII `.hfv`), carrying
/// `partition_type_byte == 0` with `type_name == "HFS"` because no partition
/// table assigned a type byte. The classic-HFS fsck / edit / expand paths are
/// offset-driven and work at offset 0, but the `0xAF` / APM `Apple_HFS` gates
/// skip these rows — this lets callers OR them back in.
pub fn is_superfloppy_hfs(ptype: u8, type_name: &str) -> bool {
    ptype == 0 && type_name == "HFS"
}

/// Heuristic: is this partition classic HFS (not HFS+/HFSX)? Gates the
/// "Expand HFS Volume…" action, which only handles classic HFS. APM rows that
/// `probe_apple_hfs_type` flagged HFS+/HFSX carry a `type_name` tag like
/// `Apple_HFS (HFS+)`; those are excluded.
pub fn is_classic_hfs(ptype: u8, type_string: Option<&str>, type_name: &str) -> bool {
    let apm_hfs = type_string
        .map(|s| s.eq_ignore_ascii_case("Apple_HFS"))
        .unwrap_or(false);
    let mbr_hfs = ptype == 0xAF;
    // A partition-less HFS superfloppy (a BasiliskII .hfv) is classic HFS too.
    let superfloppy = is_superfloppy_hfs(ptype, type_name);
    if !(apm_hfs || mbr_hfs || superfloppy) {
        return false;
    }
    !(type_name.contains("HFS+") || type_name.contains("HFSX"))
}

/// True for a partition type that supports filesystem checking (fsck): classic
/// HFS (`0xAF` or APM `Apple_HFS`), the FAT12/16/32 type bytes, and the
/// AmigaDOS OFS/FFS variants.
///
/// This only covers the cases identifiable from the partition-type byte / APM
/// string alone. The Unix filesystems whose family is only known after a
/// content probe (SGI EFS, UFS/FFS, XFS, JFS, and HFS+) are matched by
/// [`is_checkable_fs_name`] against the resolved `type_name` instead.
///
/// Note `0x07` (exFAT / NTFS / HPFS) is deliberately excluded — neither exFAT
/// nor NTFS has an fsck driver yet, and their shared type byte can't tell them
/// apart anyway.
pub fn is_checkable_type(ptype: u8, type_str: Option<&str>) -> bool {
    if ptype == 0xAF || matches!(type_str, Some("Apple_HFS")) {
        return true;
    }
    // Minix (0x80/0x81), the BSD / Solaris UFS bytes, BeOS BFS, and the APM
    // Unix slices all have fsck drivers the byte-only gate never reached.
    if matches!(ptype, 0x80 | 0x81 | 0xA5 | 0xA6 | 0xA9 | 0xBF | 0xEB)
        || matches!(
            type_str,
            Some("Apple_UNIX_SVR2") | Some("Apple_UNIX_SRVR2") | Some("Be_BFS")
        )
    {
        return true;
    }
    // ProDOS (`prodos::fsck`): MBR type byte 0xA8, or the APM DosType strings.
    if ptype == 0xA8 || matches!(type_str, Some("Apple_PRODOS") | Some("Apple_ProDOS")) {
        return true;
    }
    // FAT12/16/32 partition types (same set as `fs_name_for`'s FAT arm).
    if matches!(
        ptype,
        0x01 | 0x04 | 0x06 | 0x0B | 0x0C | 0x0E | 0x14 | 0x16 | 0x1B | 0x1C | 0x1E
    ) {
        return true;
    }
    // Amiga RDB filesystems identified by their 4-byte DosType string: AFFS
    // (`affs_fsck`), PFS3 (`pfs3_fsck`), and SFS (`sfs_fsck`) implement `fsck()`.
    type_str
        .map(|s| is_amiga_dos_type(s) || is_amiga_pfs3_type(s) || is_amiga_sfs_type(s))
        .unwrap_or(false)
}

/// True when a *resolved* filesystem-family name (the `type_name` the inspect
/// grid shows after content-probing — e.g. `"SGI EFS"`, `"XFS"`, `"UFS"`,
/// `"JFS2"`, `"HFS+"`) names a filesystem whose driver implements `fsck()`.
///
/// These all reach the "Check" button through the browsable path but are not
/// identifiable by partition-type byte alone (0x83 "Linux" and the SGI dvh
/// bytes are shared across ext / btrfs / xfs / ufs / jfs), so the button gate
/// consults this in addition to [`is_checkable_type`]. Every driver named here
/// returns `Some` from `Filesystem::fsck()`: EFS (`efs_fsck`), UFS (`ufs_fsck`),
/// XFS (R1–R8), JFS (check-only), HFS/HFS+, FAT12/16/32, and exFAT.
///
/// The FAT family and exFAT reach the button through the *name* path: FAT has
/// several partition-type bytes but exFAT shares `0x07` with NTFS/HPFS, so the
/// byte alone can't distinguish them — the resolved `type_name` (`"FAT16"`,
/// `"exFAT"`, …) does. Both `"FAT"` and `"EXFAT"` contain "FAT", so a single
/// token covers them. NTFS also shares `0x07` and needs its own token here
/// (its resolved name is `"NTFS"` / `"NTFS 3.1"`).
pub fn is_checkable_fs_name(type_name: &str) -> bool {
    let n = type_name.to_ascii_uppercase();
    // "SOLARIS S0 (ROOT)" rows are UFS; the GPT ESP / basic-data rows hold
    // FAT, NTFS or exFAT; the rest name drivers that implement `fsck()`.
    [
        "HFS",
        "EFS",
        "UFS",
        "XFS",
        "JFS",
        "FAT",
        "EXT",
        "NTFS",
        "HPFS",
        "MINIX",
        "UCSD",
        "BFS",
        "OFS",
        "SOLARIS",
        "EFI SYSTEM",
        "MICROSOFT BASIC DATA",
    ]
    .iter()
    .any(|tok| n.contains(tok))
}

/// True for the retro superfloppy / X68k filesystems whose driver implements
/// `fsck()` but which are identified only by their resolved `type_name` /
/// dispatch string (not a partition-type byte or APM string): CBM DOS,
/// DragonDOS, RS-DOS, Acorn DFS, Human68k, ProDOS, Atari DOS, Apple
/// DOS 3.3, OS-9, and CP/M (matched by its `cpm:<preset>` dispatch string).
///
/// Kept separate from [`is_checkable_fs_name`] because those tokens
/// substring-match (e.g. `"DFS"` would also match `"ADFS"`, which has no fsck),
/// so these use exact names. Human68k (floppy `"Human68k (FAT)"` and X68k HDD
/// `"X68k Human68k (…)"`) is matched by its `"human68k"` dispatch string, which
/// both shapes carry. ProDOS reaches this path as a bare `.po`/`.hdv`/`.2mg`
/// superfloppy (`ptype == 0`, `type_name == "ProDOS"`); partition-hosted ProDOS
/// is covered by [`is_checkable_type`]. Every filesystem named here is
/// factory-reachable, so the generic `fsck_runner` opens and checks/repairs it.
pub fn is_checkable_retro_fs(ptype: u8, type_string: Option<&str>, type_name: &str) -> bool {
    if type_string == Some("human68k") {
        return true;
    }
    // CP/M has no on-disk signature, so it is dispatched by a `cpm:<preset>`
    // type string (via `--fs-type`); its `fsck()` is directory-based.
    if type_string.is_some_and(|s| s.starts_with("cpm:")) {
        return true;
    }
    ptype == 0
        && matches!(
            type_name,
            "CBM DOS"
                | "DragonDOS"
                | "RS-DOS"
                | "Acorn DFS"
                | "ProDOS"
                | "Atari DOS"
                | "DOS 3.3"
                | "OS-9"
                | "TR-DOS"
                | "TI-99"
                | "MFS"
                | "ADFS"
                | "Oric Jasmin"
        )
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

/// True when the partition's VBR carries the exFAT filesystem signature.
/// Used by the backup defrag-clone preflight to gate the packed-clone path
/// (the MBR type byte 0x07 is shared with NTFS, so signature-probe).
pub fn probe_exfat_signature<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> bool {
    if reader.seek(SeekFrom::Start(partition_offset)).is_err() {
        return false;
    }
    let mut vbr = [0u8; 512];
    reader.read_exact(&mut vbr).is_ok() && &vbr[3..11] == b"EXFAT   "
}

/// True when the partition's VBR carries the NTFS OEM signature. Gates the
/// backup defrag-clone preflight (MBR type 0x07 is shared with exFAT).
pub fn probe_ntfs_signature<R: Read + Seek>(reader: &mut R, partition_offset: u64) -> bool {
    if reader.seek(SeekFrom::Start(partition_offset)).is_err() {
        return false;
    }
    let mut vbr = [0u8; 512];
    reader.read_exact(&mut vbr).is_ok() && &vbr[3..11] == b"NTFS    "
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
    /// exFAT volume. Use [`exfat_clone::stream_defragmented_exfat`]. Tree-walk
    /// replay via the EditableFilesystem trait into a freshly-formatted blank;
    /// tempfile-backed to bound RAM.
    Exfat,
    /// NTFS volume. Use [`ntfs_clone::stream_defragmented_ntfs`]. Tree-walk
    /// replay into a freshly-formatted blank (`ntfs_format::create_blank_ntfs`);
    /// tempfile-backed.
    Ntfs,
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
    // exFAT / NTFS (MBR type 0x07, no type string): probe the VBR signature so
    // the backup streams a packed clone instead of the unshrinkable
    // layout-preserving image.
    if reader.seek(SeekFrom::Start(partition_offset)).is_ok() {
        let mut vbr = [0u8; 512];
        if reader.read_exact(&mut vbr).is_ok() {
            if &vbr[3..11] == b"EXFAT   " {
                return Ok(DefragCloneShape::Exfat);
            }
            if &vbr[3..11] == b"NTFS    " {
                return Ok(DefragCloneShape::Ntfs);
            }
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

    /// The EFI System Partition on every UEFI PC is plain FAT32, but its GPT
    /// type GUID had no arm in the dispatch, so browsing it failed with
    /// "APM partition type 'C12A7328-...' not supported for browsing" - the
    /// wrong partition scheme, about a filesystem supported all along. A GPT
    /// type GUID says what a partition is *for*; the superblock says what is in
    /// it, and that is what has to decide.
    #[test]
    fn an_efi_system_partition_opens_as_the_fat_it_holds() {
        const ESP: &str = "C12A7328-F81F-11D2-BA4B-00A0C93EC93B";
        let img = crate::fs::fat::create_blank_fat(64 * 1024 * 1024, Some("ESP")).unwrap();
        let fs = open_filesystem_with_passphrase(Cursor::new(img), 0, 0, Some(ESP), None)
            .expect("an ESP must open as the FAT volume it is");
        assert!(
            fs.fs_type().starts_with("FAT"),
            "expected a FAT variant, got {}",
            fs.fs_type()
        );
    }

    /// And it must be writable: refusing the write path made the ESP read-only
    /// while an identical FAT32 in an MBR partition edited fine.
    #[test]
    fn an_efi_system_partition_is_editable() {
        const ESP: &str = "C12A7328-F81F-11D2-BA4B-00A0C93EC93B";
        let img = crate::fs::fat::create_blank_fat(64 * 1024 * 1024, Some("ESP")).unwrap();
        let mut fs = open_editable_filesystem_with(
            Cursor::new(img),
            0,
            EditContext::default(),
            0,
            Some(ESP),
        )
        .expect("an ESP must open for write");
        let root = fs.as_filesystem_mut().root().unwrap();
        fs.create_directory(&root, "EFI", &Default::default())
            .expect("mkdir on an ESP must work");
    }

    /// The same reasoning covers every other GPT GUID we have no arm for -
    /// "Microsoft Basic Data" is the one on every Windows data partition.
    #[test]
    fn an_unknown_gpt_type_guid_falls_back_to_the_superblock() {
        const MS_BASIC_DATA: &str = "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7";
        let img = crate::fs::fat::create_blank_fat(64 * 1024 * 1024, Some("DATA")).unwrap();
        let fs = open_filesystem_with_passphrase(Cursor::new(img), 0, 0, Some(MS_BASIC_DATA), None)
            .expect("an unknown GPT GUID must fall back to content detection");
        assert!(fs.fs_type().starts_with("FAT"));
    }

    /// A FAT32 volume must open no matter what the partition table calls it.
    ///
    /// The partition type is a *label*, and in the wild it is wrong constantly:
    /// an ESP is FAT32 under an EFI GUID, Windows RE is FAT32 under 0x27, MSX
    /// formatters write FAT under 0x83, and "hidden" variants (0x11/0x1B/0x1C)
    /// are the ordinary types with a bit flipped. The bytes on disk are the only
    /// thing that actually knows, so every route into the dispatch has to end at
    /// the superblock rather than at a table of blessed type codes.
    ///
    /// This is the regression net for that: each entry is a real shape someone
    /// has a disk of, and any of them failing means a user cannot read a
    /// perfectly ordinary FAT32 partition.
    ///
    /// The fixture is a real FAT32 at 64 MiB - an ESP's shape, not a 2 GiB one.
    /// `FatFilesystem::open` takes the type from the BPB (`sectors_per_fat_16`
    /// and `root_entry_count` both zero) *before* falling back to cluster
    /// counts, precisely so an under-clustered FAT32 like this reads back as
    /// FAT32 rather than being misread as FAT16.
    #[test]
    fn fat32_opens_behind_every_partition_type_that_carries_it() {
        let img = crate::fs::fat::create_blank_fat32(64 * 1024 * 1024, Some("DATA")).unwrap();
        let probe = open_filesystem_with_passphrase(Cursor::new(img.clone()), 0, 0, None, None)
            .expect("the fixture itself must open");
        assert_eq!(probe.fs_type(), "FAT32", "fixture is not FAT32");

        // MBR type bytes, with why each one turns up holding FAT32.
        for (ty, why) in [
            (0x00u8, "auto-detect / superfloppy"),
            (0x01, "FAT12"),
            (0x04, "FAT16 <32M"),
            (0x06, "FAT16"),
            (0x07, "usually NTFS/exFAT, but tools do write FAT here"),
            (0x0B, "FAT32 CHS"),
            (0x0C, "FAT32 LBA"),
            (0x0E, "FAT16 LBA"),
            (0x11, "hidden FAT12"),
            (0x14, "hidden FAT16 <32M"),
            (0x16, "hidden FAT16"),
            (0x1B, "hidden FAT32"),
            (0x1C, "hidden FAT32 LBA"),
            (0x1E, "hidden FAT16 LBA"),
            (0x27, "Windows Recovery"),
            (0x83, "Linux, but MSX Nextor writes FAT here"),
            (0xEF, "EFI System Partition on an MBR disk"),
        ] {
            let fs = open_filesystem_with_passphrase(Cursor::new(img.clone()), 0, ty, None, None)
                .unwrap_or_else(|e| panic!("MBR type 0x{ty:02X} ({why}) must open FAT32: {e}"));
            assert_eq!(
                fs.fs_type(),
                "FAT32",
                "MBR type 0x{ty:02X} ({why}) opened as the wrong filesystem"
            );
        }

        // GPT type GUIDs. The GUID says what the partition is *for*; all of
        // these are found holding FAT32.
        for (guid, why) in [
            ("C12A7328-F81F-11D2-BA4B-00A0C93EC93B", "EFI System"),
            (
                "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7",
                "Microsoft Basic Data",
            ),
            ("DE94BBA4-06D1-4D40-A16A-BFD50179D6AC", "Windows Recovery"),
            ("0FC63DAF-8483-4772-8E79-3D69D8477DE4", "Linux Filesystem"),
            ("933AC7E1-2EB4-4F13-B844-0E14E2AEF915", "Linux Home"),
            ("21686148-6449-6E6F-7468-656564454649", "BIOS Boot"),
        ] {
            let fs =
                open_filesystem_with_passphrase(Cursor::new(img.clone()), 0, 0, Some(guid), None)
                    .unwrap_or_else(|e| panic!("GPT {why} must open FAT32: {e}"));
            assert_eq!(
                fs.fs_type(),
                "FAT32",
                "GPT {why} opened as the wrong filesystem"
            );
        }
    }

    /// And the same set has to be *writable*, or the partition type quietly
    /// decides whether a volume is read-only - which is how the ESP ended up
    /// browsable but not editable.
    #[test]
    fn fat32_is_editable_behind_the_same_partition_types() {
        let img = crate::fs::fat::create_blank_fat32(64 * 1024 * 1024, Some("DATA")).unwrap();
        for ty in [0x00u8, 0x07, 0x0C, 0x1B, 0x27, 0x83, 0xEF] {
            let mut fs = open_editable_filesystem_with(
                Cursor::new(img.clone()),
                0,
                EditContext::default(),
                ty,
                None,
            )
            .unwrap_or_else(|e| panic!("MBR type 0x{ty:02X} must open FAT32 for write: {e}"));
            let root = fs.as_filesystem_mut().root().unwrap();
            fs.create_directory(&root, "T", &Default::default())
                .unwrap_or_else(|e| panic!("MBR type 0x{ty:02X}: mkdir failed: {e}"));
        }
        for guid in [
            "C12A7328-F81F-11D2-BA4B-00A0C93EC93B",
            "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7",
            "0FC63DAF-8483-4772-8E79-3D69D8477DE4",
        ] {
            let mut fs = open_editable_filesystem_with(
                Cursor::new(img.clone()),
                0,
                EditContext::default(),
                0,
                Some(guid),
            )
            .unwrap_or_else(|e| panic!("GPT {guid} must open FAT32 for write: {e}"));
            let root = fs.as_filesystem_mut().root().unwrap();
            fs.create_directory(&root, "T", &Default::default())
                .unwrap_or_else(|e| panic!("GPT {guid}: mkdir failed: {e}"));
        }
    }

    /// The formatter has to be able to *make* an ESP, not just read one.
    /// Size-based selection tops out at FAT16 below 2 GiB, so before `--fat32`
    /// there was no way to produce the 100-512 MiB FAT32 that firmware expects.
    #[test]
    fn a_small_fat32_volume_can_be_formatted_and_reopened() {
        for mb in [33u64, 64, 100, 512] {
            let img = crate::fs::fat::create_blank_fat32(mb * 1024 * 1024, Some("ESP")).unwrap();
            let fs = open_filesystem_with_passphrase(Cursor::new(img), 0, 0, None, None)
                .unwrap_or_else(|e| panic!("{mb} MiB forced FAT32 must reopen: {e}"));
            assert_eq!(
                fs.fs_type(),
                "FAT32",
                "{mb} MiB came back as the wrong type"
            );
        }
        // Size-based selection is unchanged: the same capacity without the flag
        // still resolves by cluster count.
        let img = crate::fs::fat::create_blank_fat(100 * 1024 * 1024, Some("DATA")).unwrap();
        let fs = open_filesystem_with_passphrase(Cursor::new(img), 0, 0, None, None).unwrap();
        assert_eq!(fs.fs_type(), "FAT16");
    }

    #[test]
    fn browsable_gate_excludes_apm_driver_partitions() {
        // The bug Commander hit: an APM driver / partition-map entry carries no
        // filesystem and must NOT be browsable.
        assert!(!partition_is_browsable(0, Some("Apple_Driver_IOKit"), ""));
        assert!(!partition_is_browsable(0, Some("Apple_Driver_ATA"), ""));
        assert!(!partition_is_browsable(0, Some("Apple_partition_map"), ""));
        assert!(!is_browsable_type_string(Some("Apple_Driver_IOKit")));
    }

    #[test]
    fn browsable_gate_accepts_real_filesystems() {
        assert!(partition_is_browsable(0x0B, None, "FAT32")); // FAT32 type byte
        assert!(partition_is_browsable(0xAF, None, "HFS")); // MBR HFS
        assert!(partition_is_browsable(0, Some("Apple_HFS"), "Apple_HFS")); // APM HFS
        assert!(partition_is_browsable(0, Some("DOS\\1"), "")); // AmigaDOS FFS
        assert!(partition_is_browsable(0, None, "HFS")); // HFS superfloppy
        assert!(partition_is_browsable(0, None, "exFAT")); // superfloppy hint
                                                           // FAT-name fallback for older backups with no type byte.
        assert!(partition_is_browsable(0x00, None, "FAT16 (no type byte)"));
    }

    #[test]
    fn browsable_superfloppy_covers_every_detect_hint() {
        // Regression guard: every fs_hint `partition::detect_superfloppy` emits
        // must be browsable (a type-byte-0 superfloppy auto-detects + opens).
        // These were silently refused by Commander after the gate moved to fs/
        // — notably the SGI EFS CD-ROM case the user reported.
        for hint in [
            "FAT",
            "HFS",
            "HFS+",
            "NTFS",
            "exFAT",
            "ext",
            "btrfs",
            "XFS",
            "ProDOS",
            "DOS 3.3",
            "EFS",
            "MFS",
            "ADFS",
            "ANDOS",
            "QDOS",
            "human68k",
            "Amiga-NDOS",
            "Alto BFS",
            "Pilot/Cedar",
            "lisafs",
            "Acorn DFS",
            "Atari DOS",
            "CBM DOS",
            "DragonDOS",
            "OS-9",
            "RS-DOS",
            "minix",
            "ucsd",
            "TR-DOS",
            "TI-99",
            "Oric Jasmin",
            "squashfs",
            // R-009 / R-017: `detect_superfloppy` had no probe for these, so a
            // bare dump failed partition-table detection outright.
            "JFS",
            "UFS",
            "ReiserFS",
        ] {
            assert!(
                is_browsable_superfloppy(0, hint),
                "superfloppy hint {hint:?} must be browsable"
            );
            assert!(partition_is_browsable(0, None, hint), "hint {hint:?}");
        }
        // SFS reports its DosType and routes through `partition_type_string`,
        // exactly as an AmigaDOS superfloppy does.
        assert!(partition_is_browsable(0, Some("SFS\\0"), "SFS (Amiga)"));
    }

    /// Every shape the corpus produces for a filesystem we can open; see
    /// OUTSTANDINGWORK.md section 8 for how the list was surveyed.
    #[test]
    fn every_openable_filesystem_offers_browse() {
        // Bare volumes: no partition table, identified by name alone.
        for name in [
            "HPFS",
            "BFS",
            "BeOS OFS",
            "SGI EFS v1",
            "UFS",
            "EFS",
            "XFS",
            "JFS",
            "minix",
            "ext",
            "btrfs",
            "NTFS",
            "HFS",
            "HFS+",
            "APFS",
            "FAT",
            "ProDOS",
            "exFAT",
            "ReiserFS",
            "squashfs",
            "MFS",
            "ucsd",
        ] {
            assert!(
                partition_is_browsable(0, None, name),
                "bare volume {name:?} must offer Browse"
            );
        }

        // Partition-hosted, identified by type byte.
        assert!(partition_is_browsable(0xEB, None, "BeOS BFS"));
        assert!(partition_is_browsable(0xA5, None, "FreeBSD"));
        assert!(partition_is_browsable(0xA9, None, "NetBSD"));
        assert!(partition_is_browsable(0xBF, None, "Solaris"));

        // Partition-hosted, identified by APM type string.
        assert!(partition_is_browsable(
            0,
            Some("Be_BFS"),
            "Be_BFS (untitled 2)"
        ));

        // Unix disk labels: no type byte, no type string, role in the name.
        for name in [
            "NeXT a (4.3BSD)",
            "Sun root (UFS?)",
            "Sun usr (UFS?)",
            "Sun home (UFS?)",
            "Solaris s0 (root)",
            "SGI BSD",
            "SGI root (EFS v1)",
        ] {
            assert!(
                partition_is_browsable(0, None, name),
                "label slice {name:?} must offer Browse"
            );
        }
    }

    /// The other half: rows that must NOT offer Browse. Swap is load-bearing —
    /// it yields a carve view or a stale tree, never the volume asked for.
    #[test]
    fn reserved_and_container_rows_do_not_offer_browse() {
        for name in [
            "Sun swap (UFS?)",
            "Solaris s1 (swap)",
            "Solaris s8 (boot)",
            "Solaris s9 (altsctr)",
            "SGI swap",
            "SGI SECREPL",
            "SGI VOLHDR",
            "SGI TRKREPL",
        ] {
            assert!(
                !partition_is_browsable(0, None, name),
                "reserved slice {name:?} must NOT offer Browse"
            );
        }
        // Extended containers hold no filesystem of their own.
        assert!(!partition_is_browsable(0x0F, None, "Extended (LBA)"));
        assert!(!partition_is_browsable(0x05, None, "Extended"));
        // QDOS microdrive has no directory walk yet, so Browse would error.
        assert!(!partition_is_browsable(0, None, "qdos_mdv"));
    }

    #[test]
    fn checkable_retro_fs_gate() {
        // Factory-reachable retro filesystems that implement fsck().
        assert!(is_checkable_retro_fs(0, None, "CBM DOS"));
        assert!(is_checkable_retro_fs(0, None, "DragonDOS"));
        assert!(is_checkable_retro_fs(0, None, "RS-DOS"));
        assert!(is_checkable_retro_fs(0, None, "Acorn DFS"));
        // Human68k (floppy + X68k HDD) via its dispatch string.
        assert!(is_checkable_retro_fs(0, Some("human68k"), "Human68k (FAT)"));
        assert!(is_checkable_retro_fs(
            0x01,
            Some("human68k"),
            "X68k Human68k (MYVOL)"
        ));
        // ProDOS as a bare superfloppy (partition-hosted ProDOS goes through
        // is_checkable_type via 0xA8 / Apple_PRODOS).
        assert!(is_checkable_retro_fs(0, None, "ProDOS"));
        // Atari DOS 2 (single-density floppy) now fscks.
        assert!(is_checkable_retro_fs(0, None, "Atari DOS"));
        // Apple DOS 3.3 (140 KB floppy) now fscks.
        assert!(is_checkable_retro_fs(0, None, "DOS 3.3"));
        // OS-9 / NitrOS-9 RBF now fscks.
        assert!(is_checkable_retro_fs(0, None, "OS-9"));
        // MFS (Macintosh File System, 400/800 KB floppy) now fscks.
        assert!(is_checkable_retro_fs(0, None, "MFS"));
        // ADFS new-map (E/F/HD) now fscks (zone-checksum + FSM reconciliation).
        assert!(is_checkable_retro_fs(0, None, "ADFS"));
    }

    #[test]
    fn classic_hfs_excludes_hfs_plus() {
        assert!(is_classic_hfs(0xAF, None, "HFS"));
        assert!(is_classic_hfs(0, Some("Apple_HFS"), "Apple_HFS"));
        assert!(is_classic_hfs(0, None, "HFS")); // .hfv superfloppy
                                                 // HFS+/HFSX (incl. APM rows tagged by probe_apple_hfs_type) are excluded.
        assert!(!is_classic_hfs(0, Some("Apple_HFS"), "Apple_HFS (HFS+)"));
        assert!(!is_classic_hfs(0xAF, None, "HFSX"));
        assert!(!is_classic_hfs(0x0B, None, "FAT32"));
    }

    #[test]
    fn checkable_covers_classic_hfs_and_amiga_dos() {
        assert!(is_checkable_type(0xAF, None));
        assert!(is_checkable_type(0, Some("Apple_HFS")));
        assert!(is_checkable_type(0, Some("DOS\\3")));
        assert!(is_checkable_type(0, Some("PFS\\3"))); // PFS3 fsck driver
        assert!(is_checkable_type(0, Some("PDS\\3")));
        assert!(is_checkable_type(0, Some("SFS\\0"))); // SFS fsck driver
        assert!(is_checkable_type(0, Some("SFS\\2")));
        assert!(is_checkable_type(0x0B, None)); // FAT32 now checkable
        assert!(is_checkable_type(0x06, None)); // FAT16 now checkable
        assert!(is_checkable_type(0xA8, None)); // ProDOS (MBR) now checkable
        assert!(is_checkable_type(0, Some("Apple_PRODOS"))); // ProDOS (APM)
        assert!(is_checkable_type(0, Some("Apple_ProDOS")));
        assert!(!is_checkable_type(0x07, None)); // exFAT/NTFS: no fsck driver
        assert!(!is_checkable_type(0, Some("Apple_Driver_IOKit")));
    }

    #[test]
    fn checkable_fs_name_covers_probed_unix_families() {
        // The resolved family names the inspect grid shows for content-probed
        // Linux (0x83) and SGI (dvh) partitions, plus HFS+ and NTFS.
        for name in [
            "SGI EFS", "EFS", "SGI XFS", "XFS", "UFS", "JFS2", "HFS+", "HFS/HFS+", "FAT12",
            "FAT16", "FAT32", "exFAT", "ext2", "ext3", "ext4", "NTFS", "NTFS 3.1",
        ] {
            assert!(is_checkable_fs_name(name), "{name} should be checkable");
        }
        // Filesystems without an fsck() driver stay off the button.
        for name in ["btrfs", "ReiserFS", "ProDOS"] {
            assert!(
                !is_checkable_fs_name(name),
                "{name} has no fsck; must not enable the button"
            );
        }
    }

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

    /// Build a 4 KiB buffer that looks like an ext2/3/4 partition with a
    /// boot loader (syslinux/extlinux) installed in the reserved first 1024
    /// bytes. The boot code begins `EB 58 90 "SYSLINUX"` — the same JMP a FAT
    /// VBR starts with — but the BPB is degenerate (sectors_per_cluster == 0).
    /// The ext2 superblock magic (0xEF53 LE) sits at byte 1080 (1024 + 0x38).
    /// Mirrors the real `shork-486` disk.
    fn extlinux_ext2_sector() -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        buf[0] = 0xEB;
        buf[1] = 0x58;
        buf[2] = 0x90;
        buf[3..11].copy_from_slice(b"SYSLINUX");
        buf[11..13].copy_from_slice(&512u16.to_le_bytes()); // bytes/sector (valid)
        buf[13] = 0; // sectors/cluster == 0 -> not a real FAT BPB
                     // ext2 superblock magic (0xEF53 LE) at offset 1024 + 0x38.
        buf[1024 + 0x38] = 0x53;
        buf[1024 + 0x39] = 0xEF;
        buf
    }

    #[test]
    fn detect_filesystem_type_extlinux_boot_block_is_ext_not_fat() {
        // Regression: an ext2 partition with extlinux in the reserved boot
        // block leads with an `EB 58 90` JMP. The old code short-circuited to
        // "fat" on the jump byte alone and the FAT parser then died with
        // "invalid sectors per cluster: 0". The BPB sanity gate must let it
        // fall through to the ext2 magic check.
        let buf = extlinux_ext2_sector();
        let mut cursor = Cursor::new(buf);
        assert_eq!(detect_filesystem_type(&mut cursor, 0), "ext");
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

/// Whether `type_name` names an actual filesystem, or is the engine admitting
/// it did not recognise one.
///
/// `inspect` opens anything — a disk with no recognisable filesystem still
/// yields a carve view, which is the point of a universal tool. That made
/// "`inspect` opened it" useless as a verification: an Apple DOS fixture that
/// was really a bare bootloader passed it and sat in the corpus for weeks
/// (R-031). The distinction has to be legible, so it lives here rather than in
/// any one caller.
pub fn is_identified_fs(type_name: &str) -> bool {
    !matches!(
        type_name.trim(),
        "" | "Unknown" | "unknown" | "Unformatted" | "Free space" | "Empty"
    )
}

/// Compare a filesystem name to a caller's expectation.
///
/// Normalises case, spacing and punctuation, then compares **exactly** — a
/// substring rule would let `FAT` satisfy `exFAT`, which would make the flag
/// worse than no flag.
///
/// Two wrinkles the type names force:
///
/// - `+` becomes `plus`, so `HFS+` and `hfsplus` are the same answer and
///   neither is `HFS`. Stripping `+` as punctuation instead would collapse
///   `HFS+` onto `HFS` and quietly accept the wrong volume.
/// - A type byte shared by several filesystems is named for all of them
///   (`NTFS/HPFS/exFAT`, `HFS/HFS+`). Each alternative is matched separately,
///   so `--expect-fs HPFS` is satisfied by an `NTFS/HPFS/exFAT` partition.
///   Numeric shorthands like `ext2/3/4` only match their first alternative;
///   that misses rather than over-matches, which is the safe direction.
pub fn fs_name_matches(type_name: &str, expected: &str) -> bool {
    fn norm(s: &str) -> String {
        let mut out = String::new();
        for c in s.chars() {
            if c == '+' {
                out.push_str("plus");
            } else if c.is_ascii_alphanumeric() {
                out.push(c.to_ascii_lowercase());
            }
        }
        out
    }
    let want = norm(expected);
    if want.is_empty() {
        return false;
    }
    // Whole name first: a caller who asks for exactly what `inspect` printed
    // must always be satisfied. Splitting alone broke that — an ambiguous
    // type-byte name like `NTFS/HPFS/exFAT` stopped matching itself, which the
    // corpus identity check found the first time it ran.
    if norm(type_name) == want {
        return true;
    }
    // Then each alternative, so `HPFS` satisfies `NTFS/HPFS/exFAT`: type byte
    // 0x07 names three filesystems and the table cannot say which.
    type_name.split('/').any(|alt| norm(alt) == want)
}

#[cfg(test)]
mod identification_tests {
    use super::*;

    #[test]
    fn unknown_is_not_an_identification() {
        assert!(!is_identified_fs("Unknown"));
        assert!(!is_identified_fs(""));
        assert!(!is_identified_fs("  "));
        assert!(is_identified_fs("DOS 3.3"));
        assert!(is_identified_fs("HPFS"));
    }

    #[test]
    fn expectations_ignore_case_and_punctuation() {
        assert!(fs_name_matches("DOS 3.3", "dos3.3"));
        assert!(fs_name_matches("DOS 3.3", "DOS 3.3"));
        assert!(fs_name_matches("Apple DOS 3.3", "appledos33"));
    }

    #[test]
    fn a_name_always_matches_itself() {
        // Regression: `NTFS/HPFS/exFAT` did not match itself, because only the
        // `/`-alternatives were compared.
        for n in [
            "NTFS/HPFS/exFAT",
            "DOS 3.3",
            "HPFS",
            "Amiga NDOS (no filesystem)",
        ] {
            assert!(fs_name_matches(n, n), "{n} must match itself");
        }
    }

    #[test]
    fn an_alternative_satisfies_an_ambiguous_type_byte() {
        // Type byte 0x07 names three filesystems; the table cannot say which.
        assert!(fs_name_matches("NTFS/HPFS/exFAT", "HPFS"));
        assert!(fs_name_matches("NTFS/HPFS/exFAT", "ntfs"));
        assert!(!fs_name_matches("NTFS/HPFS/exFAT", "ext4"));
    }

    #[test]
    fn a_substring_is_not_a_match() {
        // The reason this is an exact compare: FAT must not satisfy exFAT.
        assert!(!fs_name_matches("exFAT", "FAT"));
        assert!(!fs_name_matches("FAT", "exFAT"));
        assert!(!fs_name_matches("DOS 3.3", ""));
    }

    #[test]
    fn plus_is_a_letter_not_punctuation() {
        // The first version stripped `+` as punctuation, which collapsed HFS+
        // onto HFS — so `--expect-fs HFS` accepted an HFS+ volume, the exact
        // false positive the exact-match rule exists to prevent.
        assert!(fs_name_matches("HFS+", "hfsplus"));
        assert!(fs_name_matches("HFS+", "HFS+"));
        assert!(!fs_name_matches("HFS+", "HFS"));
        assert!(!fs_name_matches("HFS", "HFS+"));
    }

    #[test]
    fn a_shared_type_byte_matches_any_of_its_names() {
        // MBR 0x07 is named for everything it can be; asking for one of them
        // is a fair question. This is what the OS/2 HPFS fixture reports.
        assert!(fs_name_matches("NTFS/HPFS/exFAT", "HPFS"));
        assert!(fs_name_matches("NTFS/HPFS/exFAT", "ntfs"));
        assert!(fs_name_matches("HFS/HFS+", "HFS"));
        assert!(fs_name_matches("HFS/HFS+", "hfsplus"));
        assert!(!fs_name_matches("NTFS/HPFS/exFAT", "FAT"));
    }
}

#[cfg(test)]
mod gpt_guid_dispatch_tests {
    use super::*;
    use std::io::Cursor;

    const HFS_GUID: &str = "48465300-0000-11AA-AA11-00306543ECAC";
    const LINUX_FS_GUID: &str = "0FC63DAF-8483-4772-8E79-3D69D8477DE4";
    const LINUX_HOME_GUID: &str = "933AC7E1-2EB4-4F13-B844-0E14E2AEF915";
    const ESP_GUID: &str = "C12A7328-F81F-11D2-BA4B-00A0C93EC93B";
    const MS_BASIC_DATA_GUID: &str = "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7";
    const WIN_RE_GUID: &str = "DE94BBA4-06D1-4D40-A16A-BFD50179D6AC";

    /// F7: the four gates graded a GPT disk's partitions as "unknown" and
    /// therefore packing, so an ext or HFS+ partition got the wrong minimum.
    #[test]
    fn gpt_guids_are_named_and_graded_like_their_mbr_bytes() {
        for guid in [LINUX_FS_GUID, LINUX_HOME_GUID] {
            assert_eq!(fs_name_for(0, Some(guid)), fs_name_for(0x83, None));
            assert!(is_layout_preserving_fs(0, Some(guid)));
            assert!(is_expensive_minimum(0, Some(guid)));
            assert!(!has_defragmenting_writer(0, Some(guid)));
            assert_eq!(
                pick_shrink_target(0, Some(guid), Some(500), Some(100)),
                Some(500),
                "a layout-preserving GPT partition must keep the in-place trim"
            );
        }
        assert_eq!(fs_name_for(0, Some(HFS_GUID)), "HFS/HFS+");
        assert!(is_layout_preserving_fs(0, Some(HFS_GUID)));
        assert!(is_expensive_minimum(0, Some(HFS_GUID)));
        assert!(has_defragmenting_writer(0, Some(HFS_GUID)));
        assert_eq!(fs_name_for(0, Some(ESP_GUID)), "FAT");
        assert_eq!(fs_name_for(0, Some(MS_BASIC_DATA_GUID)), "NTFS/exFAT/FAT");
        assert_eq!(fs_name_for(0, Some(WIN_RE_GUID)), "NTFS");
        for guid in [ESP_GUID, MS_BASIC_DATA_GUID, WIN_RE_GUID] {
            assert!(!is_layout_preserving_fs(0, Some(guid)));
            assert!(!is_expensive_minimum(0, Some(guid)));
        }
    }

    #[test]
    fn hidden_and_esp_type_bytes_are_named() {
        assert_eq!(fs_name_for(0x11, None), "FAT");
        assert_eq!(fs_name_for(0xEF, None), "FAT");
        assert_eq!(fs_name_for(0x27, None), "NTFS");
        for ty in [0x11u8, 0x27, 0xEF] {
            assert!(!is_layout_preserving_fs(ty, None));
            assert!(!is_expensive_minimum(ty, None));
        }
    }

    /// The compact path used to hand back "unsupported" for every one of these,
    /// so a backup of an ESP or a Basic Data partition was stored unpacked.
    #[test]
    fn fat32_compacts_behind_every_label_that_carries_it() {
        let img = crate::fs::fat::create_blank_fat32(64 * 1024 * 1024, Some("DATA")).unwrap();
        for ty in [0x07u8, 0x11, 0x27, 0xEF] {
            try_compact_partition_reader(Cursor::new(img.clone()), 0, ty, None)
                .unwrap_or_else(|e| panic!("MBR type 0x{ty:02X} must compact FAT32: {e}"));
        }
        for guid in [ESP_GUID, MS_BASIC_DATA_GUID, WIN_RE_GUID, LINUX_FS_GUID] {
            try_compact_partition_reader(Cursor::new(img.clone()), 0, 0, Some(guid))
                .unwrap_or_else(|e| panic!("GPT {guid} must compact FAT32: {e}"));
        }
        // An APM driver slot has no filesystem, and the probe must say so.
        let err = try_compact_partition_reader(
            Cursor::new(vec![0u8; 1024 * 1024]),
            0,
            0,
            Some("Apple_Driver43"),
        )
        .err()
        .expect("random zeros must not compact");
        assert!(err.starts_with("unsupported"), "{err}");
    }

    #[test]
    fn ext2_and_hfsplus_compact_behind_their_gpt_guids() {
        let ext = crate::fs::ext_format::create_blank_ext2(32 * 1024 * 1024, "T").unwrap();
        for guid in [LINUX_FS_GUID, LINUX_HOME_GUID] {
            try_compact_partition_reader(Cursor::new(ext.clone()), 0, 0, Some(guid))
                .unwrap_or_else(|e| panic!("GPT {guid} must compact ext2: {e}"));
        }
        let hfsp = crate::fs::hfsplus::create_blank_hfsplus(32 * 1024 * 1024, 4096, "T", false);
        try_compact_partition_reader(Cursor::new(hfsp.clone()), 0, 0, Some(HFS_GUID))
            .unwrap_or_else(|e| panic!("GPT Apple HFS must compact HFS+: {e}"));
        // A bare (superfloppy) HFS+ volume is the same bytes under type byte 0.
        try_compact_partition_reader(Cursor::new(hfsp), 0, 0, None)
            .unwrap_or_else(|e| panic!("type byte 0 must compact HFS+: {e}"));
    }
}

#[cfg(test)]
mod human68k_dispatch_tests {
    use super::*;
    use std::io::Cursor;

    fn human68k_sector() -> Vec<u8> {
        let mut disk = vec![0u8; 64 * 1024];
        disk[0] = 0x60; // BRA.S
        disk[11..13].copy_from_slice(&1024u16.to_be_bytes());
        disk[13] = 1;
        disk
    }

    /// F10: the dispatch helpers called a Human68k volume "unknown" and
    /// the detector never saw its BRA.S boot sector.
    #[test]
    fn human68k_is_named_detected_and_resizable() {
        assert_eq!(fs_name_for(0, Some("human68k")), "Human68k");
        assert_eq!(
            detect_filesystem_type(&mut Cursor::new(human68k_sector()), 0),
            "human68k"
        );
        assert!(matches!(
            in_place_resize_support(&mut Cursor::new(vec![0u8; 4096]), 0, Some("human68k")),
            InPlaceResize::Supported("Human68k")
        ));
        assert!(matches!(
            in_place_resize_support(&mut Cursor::new(human68k_sector()), 0, None),
            InPlaceResize::Supported("Human68k")
        ));
        // A PC boot sector is still not mistaken for one.
        let mut pc = human68k_sector();
        pc[0] = 0xEB;
        assert_ne!(detect_filesystem_type(&mut Cursor::new(pc), 0), "human68k");
    }
}
