pub mod alignment;
pub mod apm;
pub mod atari;
pub mod editor;
pub mod gpt;
pub mod mbr;
pub mod rdb;
pub mod resize;
pub mod sgi;
pub mod x68k;
pub mod x68k_hdd_builder;
pub mod x68k_ipl;

use std::io::{Read, Seek, SeekFrom};

use crate::error::RustyBackupError;
use apm::Apm;
use atari::{looks_like_ahdi_root, AhdiPartitionKind, AhdiTable, AHDI_NUM_SLOTS};
use gpt::Gpt;
use mbr::Mbr;
use rdb::{Rdb, RDSK_SIGNATURE};
use sgi::{SgiVolumeHeader, SGI_TYPE_BYTE_EFS, SGI_TYPE_BYTE_XFS, SGI_VOLHDR_MAGIC};
use x68k::X68kPartitionTable;

pub use alignment::{detect_alignment, AlignmentType, PartitionAlignment};

/// Detected partition table type with parsed data.
#[derive(Debug, Clone)]
pub enum PartitionTable {
    Mbr(Mbr),
    Gpt {
        protective_mbr: Mbr,
        gpt: Gpt,
    },
    Apm(Apm),
    /// Amiga Rigid Disk Block — `RDSK` table in the first 16 sectors of a
    /// hard-disk image, used by AmigaDOS, PFS, SFS. See `src/partition/rdb.rs`.
    Rdb(Rdb),
    /// SGI Volume Header (IRIX disks). Has 16 fixed-position partition slots
    /// plus a volume directory of standalone executables (`sash`, `ide`,
    /// `/unix`); see `src/partition/sgi.rs`.
    Sgi(SgiVolumeHeader),
    /// Atari HD File System Driver — MBR-equivalent for Atari ST / TT / Falcon
    /// hard-disk images. 4 primary slots + XGM extended chains, big-endian,
    /// no boot signature; see `src/partition/atari.rs`.
    Ahdi(AhdiTable),
    /// Sharp X68000 SASI/SCSI HDD partition table — Human68k's native scheme.
    /// 16-byte header + 8 × 16-byte partition entries at byte 2048, big-endian,
    /// no boot signature; see `src/partition/x68k.rs`.
    X68k {
        table: X68kPartitionTable,
        /// Total disk size in bytes (the in-table `size` field is in an
        /// undefined unit and unreliable; this comes from the source
        /// reader directly).
        size_bytes: u64,
        /// Logical sector size in bytes, derived from the Sharp boot
        /// signature (`X68SCSI1` -> 1024, `\x82w68000W` -> 256, else 512).
        /// The table's `start_sector` / `length_sectors` are counted in
        /// these sectors; partition byte offsets are `start_sector *
        /// sector_size`. See [`x68k::detect_sector_size`].
        sector_size: u64,
    },
    /// Superfloppy / floppy: no partition table, filesystem at sector 0.
    None {
        /// Total disk size in bytes (needed to synthesize partition info).
        size_bytes: u64,
        /// Detected filesystem hint: "FAT", "HFS", "HFS+", or "Unknown".
        fs_hint: String,
    },
}

/// Unified partition info for display purposes.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub index: usize,
    pub type_name: String,
    /// Raw partition type byte (MBR type ID; 0 for GPT).
    pub partition_type_byte: u8,
    pub start_lba: u64,
    /// Authoritative partition start in bytes. `None` for the overwhelming
    /// majority of tables, where the start is exactly `start_lba * 512`.
    /// Set explicitly only when that identity does not hold — X68000 SASI
    /// disks use 256-byte logical sectors, so a partition can begin on a
    /// byte that is not 512-aligned (e.g. sector 33 -> byte 8448). Read
    /// the start through [`PartitionInfo::byte_offset`], never by
    /// recomputing `start_lba * 512`.
    pub start_byte: Option<u64>,
    pub size_bytes: u64,
    pub bootable: bool,
    /// True for logical partitions inside an extended container.
    pub is_logical: bool,
    /// True for the extended container entry itself (not backed up individually).
    pub is_extended_container: bool,
    /// APM partition type string (e.g. "Apple_HFS"). None for MBR/GPT.
    pub partition_type_string: Option<String>,
    /// Allocation block size in bytes for HFS / HFS+ partitions. `None` for
    /// non-HFS partitions or when the volume header could not be probed.
    pub hfs_block_size: Option<u32>,
    /// 512-byte sector number of the PART block on disk, for RDB partitions
    /// only. Used by GUI actions that need to mutate the partition entry
    /// in place (e.g. toggling the bootable flag). `None` for all other
    /// partition table types.
    pub rdb_part_block: Option<u64>,
    /// AmigaDOS drive name (e.g. "DH0") from the RDB PART block. Distinct
    /// from the volume label, which is the disk name in the AFFS/PFS3/SFS
    /// root block. `None` for non-RDB partitions.
    pub drv_name: Option<String>,
}

impl PartitionInfo {
    /// Byte offset of the partition's first sector. Use this everywhere an
    /// absolute partition offset is needed (filesystem open, backup,
    /// export) instead of recomputing `start_lba * 512` — it honors the
    /// `start_byte` override that X68000 SASI (256-byte-sector) disks set.
    pub fn byte_offset(&self) -> u64 {
        self.start_byte.unwrap_or(self.start_lba * 512)
    }
}

/// Standard floppy disk image sizes (bytes).
///
/// Images matching one of these sizes that lack both a recognized filesystem
/// and a valid MBR/GPT signature are treated as superfloppies with an unknown
/// filesystem rather than producing a confusing partition-table error.
fn is_floppy_size(size: u64) -> bool {
    matches!(
        size,
        143_360     // 5.25" 140K (35 tracks × 16 sectors × 256 bytes)
        | 255_488   // 8" SSSD CP/M data area (Altair `altair_8in` DPB: 2 reserved trk + 243 × 1024 B blocks)
        | 256_256   // 8" SSSD 256K (77 trk × 26 spt × 128 B) — MITS Altair / IBM 3740 CP/M (raw)
        | 409_600   // 3.5" 400K single-sided GCR
        | 819_200   // 3.5" 800K double-sided GCR
        | 737_280   // 3.5" 720K MFM (PC double-density)
        | 901_120   // 3.5" 880K Amiga DD (.adf)
        | 1_474_560 // 3.5" 1.44MB MFM (PC/Mac high-density)
        | 1_802_240 // 3.5" 1.76MB Amiga HD (.adf)
    )
}

/// Detect whether a disk image is a superfloppy (no partition table, filesystem at sector 0).
///
/// Checks the first sector for a valid FAT BPB, and offset 1024 for HFS/HFS+.
/// Returns `Some(fs_hint)` where `fs_hint` is `"FAT"`, `"HFS"`, `"HFS+"`,
/// or `None` if not a superfloppy.
fn detect_superfloppy(first_sector: &[u8; 512], reader: &mut (impl Read + Seek)) -> Option<String> {
    // Check for FAT VBR: JMP instruction + valid BPB fields.
    // We validate multiple BPB fields to reliably distinguish a FAT boot sector
    // from an MBR. The partition table area (offsets 446-509) of a FAT VBR
    // contains bootstrap code which often has non-zero bytes that would look
    // like partition entries, so we do NOT check that area.
    // NTFS / exFAT: detect via the 8-byte OEM ID at offset 3. Like FAT VBRs,
    // these boot sectors carry the 0xAA55 signature at bytes 510-511 and
    // would otherwise be misparsed as an MBR with junk partition entries
    // sourced from the bootstrap code.
    if first_sector[0] == 0xEB || first_sector[0] == 0xE9 {
        let oem_id = &first_sector[3..11];
        if oem_id == b"NTFS    " {
            return Some("NTFS".to_string());
        }
        if oem_id == b"EXFAT   " {
            return Some("exFAT".to_string());
        }
    }

    if first_sector[0] == 0xEB || first_sector[0] == 0xE9 {
        let bytes_per_sector = u16::from_le_bytes([first_sector[11], first_sector[12]]);
        let sectors_per_cluster = first_sector[13];
        let reserved_sectors = u16::from_le_bytes([first_sector[14], first_sector[15]]);
        let num_fats = first_sector[16];
        let media_descriptor = first_sector[21];

        let valid_bps = matches!(bytes_per_sector, 512 | 1024 | 2048 | 4096);
        let valid_spc = sectors_per_cluster.is_power_of_two() && sectors_per_cluster > 0;
        let valid_reserved = reserved_sectors >= 1;
        let valid_fats = num_fats == 1 || num_fats == 2;
        let valid_media = media_descriptor == 0xF0 || media_descriptor >= 0xF8;

        if valid_bps && valid_spc && valid_reserved && valid_fats && valid_media {
            // The combined probability of a non-FAT sector having all five
            // valid BPB fields by coincidence is ~10^-9, so this is a very
            // strong detection signal. We intentionally do NOT check the
            // partition table area (bytes 446-509) because on FAT VBRs that
            // area contains bootstrap code which often has bytes that look
            // like valid MBR partition entries.
            return Some("FAT".to_string());
        }
    }

    // X68000 Human68k floppy. The IPL boot sector starts with a 68000 BRA.S
    // (0x60) rather than an x86 JMP (0xEB / 0xE9), so the FAT check above
    // skips it. The BPB is either a standard little-endian one at offset 11
    // ("X68IPL30" disks) or the Sharp/KG big-endian layout at offset 0x12
    // ("Hudson soft" game disks); Human68kBpb::parse handles both and
    // validates bytes-per-sector / sectors-per-cluster / FAT count, so a
    // successful parse together with the 0x60 opcode is a strong signal.
    // These floppies (.d88 / .xdf / .hdm / .dim, decoded upstream) carry no
    // partition table — they're a bare Human68k FAT volume rooted at byte 0.
    if first_sector[0] == 0x60 {
        if let Ok(bpb) = crate::fs::human68k::Human68kBpb::parse(first_sector) {
            if bpb.total_sectors > 0 {
                return Some("human68k".to_string());
            }
        }
    }

    // XFS superblock magic "XFSB" at byte 0 of the partition. v4 and v5/CRC
    // share the magic; the XfsFilesystem parser handles version routing.
    if &first_sector[0..4] == b"XFSB" {
        return Some("XFS".to_string());
    }

    // Sinclair QL QXL.WIN container: 4-byte "QLWA" signature at byte 0.
    // Raw .win images carry no partition table — they're a single QDOS
    // volume rooted at byte 0 — so the superfloppy path is the right
    // route for them.
    if &first_sector[0..4] == b"QLWA" {
        return Some("QDOS".to_string());
    }

    // Soviet BK0011M ANDOS: signature "ANDOS" at one of several
    // well-known boot-block slots in sector 0. See src/fs/andos.rs.
    for &cand in &[0x1F8usize, 0x1B0, 0xE0, 0x00] {
        if first_sector.len() >= cand + 5 && &first_sector[cand..cand + 5] == b"ANDOS" {
            return Some("ANDOS".to_string());
        }
    }

    // AmigaDOS boot block: "DOS\x?" at offset 0 (variants 0..7 = OFS/FFS,
    // Intl, DirCache, Long Names). PFS / SFS use the same shape for their
    // RDB DosType but never appear on bare ADFs — those are RDB-partitioned
    // hard-disk images instead. Returning the DosType string here lets the
    // dispatcher route to the AFFS driver.
    if &first_sector[0..3] == b"DOS" && first_sector[3] <= 7 {
        return Some(format!("DOS\\{}", first_sector[3]));
    }

    // Check for HFS / HFS+ / HFSX / ext / ProDOS at offset 1024.
    // Read a full 512-byte sector for raw device compatibility (macOS /dev/rdiskN
    // requires sector-aligned reads).
    if reader.seek(SeekFrom::Start(1024)).is_ok() {
        let mut buf = [0u8; 512];
        if reader.read_exact(&mut buf).is_ok() {
            let sig = u16::from_be_bytes([buf[0], buf[1]]);
            match sig {
                0x4244 => return Some("HFS".to_string()),
                0x482B | 0x4858 => return Some("HFS+".to_string()),
                // MFS — pre-HFS, used by Mac 128K/512K and Mac Plus on 400 KB
                // single-sided floppies. Same byte-1024 MDB convention as HFS.
                0xD2D7 => return Some("MFS".to_string()),
                _ => {}
            }
            // ext2/3/4 superblock magic 0xEF53 (LE u16) at byte 0x38 of the
            // 1024-offset superblock.
            if buf[0x38] == 0x53 && buf[0x39] == 0xEF {
                return Some("ext".to_string());
            }
            // ProDOS volume directory key block: prev_block==0, storage_type nibble==0xF,
            // entry_length==39, entries_per_block==13.
            // The directory header entry starts at offset 4 (after the 4-byte
            // prev/next block pointers), so entry_length (offset 31) and
            // entries_per_block (offset 32) within the entry land at block
            // bytes 35 and 36.
            if buf[0] == 0
                && buf[1] == 0
                && (buf[4] >> 4) == 0xF
                && (buf[4] & 0xF) >= 1
                && buf[35] == 39
                && buf[36] == 13
            {
                return Some("ProDOS".to_string());
            }
        }
    }

    // btrfs superblock magic "_BHRfS_M" at offset 0x40 of the superblock
    // at byte offset 0x10000 (64 KiB) from the partition start.
    if reader.seek(SeekFrom::Start(0x10000)).is_ok() {
        let mut buf = [0u8; 512];
        if reader.read_exact(&mut buf).is_ok() && &buf[0x40..0x48] == b"_BHRfS_M" {
            return Some("btrfs".to_string());
        }
    }

    // EFS (IRIX) superblock at sector 1: 4-byte big-endian magic
    // 0x00072959 (legacy) or 0x0007295A (new) at offset 28 of the sector.
    if reader.seek(SeekFrom::Start(512)).is_ok() {
        let mut buf = [0u8; 512];
        if reader.read_exact(&mut buf).is_ok() {
            let magic = u32::from_be_bytes([buf[28], buf[29], buf[30], buf[31]]);
            if magic == 0x0007_2959 || magic == 0x0007_295A {
                return Some("EFS".to_string());
            }
        }
    }

    // Acorn ADFS / FileCore Disc Record. Two probe sites match the
    // Linux kernel paths:
    //   * byte 0xDC0 (= 0xC00 + 0x1C0) — boot block, used by HD discs
    //     and legacy floppy boot-block layouts (kernel
    //     `adfs_validate_bblk`). CROS42 / ICEBIRD use this.
    //   * byte 0x04 — DR embedded inside zone 0 after the 4-byte zone
    //     header, used by single-zone E-format floppies (kernel
    //     `adfs_validate_dr0`). 8bs.com arc-04 / arc-05 use this.
    // FileCore's spec: byte 0 = log2 sector size (8..11 → 256..2048 B),
    // byte 1 = secs/track (>= 1), byte 2 = heads (>= 1; up to 9+ on
    // emulator-side HDDs — CROS42 / ICEBIRD report heads=9, real
    // hardware tops out lower but the field is u8 and FileCore HDs
    // don't constrain it the way floppies do), byte 9 = nzones (>= 1).
    // A coincidental match in random bytes is very unlikely given these
    // constraints combined.
    for cand in [0xDC0u64, 0x004u64] {
        if reader.seek(SeekFrom::Start(cand)).is_ok() {
            let mut dr = [0u8; 60];
            if reader.read_exact(&mut dr).is_ok() {
                let log2_sec = dr[0];
                let secs_per_track = dr[1];
                let heads = dr[2];
                let idlen = dr[4];
                let nzones = dr[9];
                // Bytes 52..60 (`unused52` in the kernel struct) must
                // be zero per `adfs_checkdiscrecord` — this is the
                // strongest single false-positive guard.
                let reserved_zero = dr[52..60].iter().all(|&b| b == 0);
                if (8..=11).contains(&log2_sec)
                    && secs_per_track >= 1
                    && heads >= 1
                    && nzones >= 1
                    && idlen >= log2_sec + 3
                    && reserved_zero
                {
                    return Some("ADFS".to_string());
                }
            }
        }
    }

    // Acorn DFS catalog at sectors 0-1 (the small flat catalog FS used
    // by BBC Micro / AcornElectron 5.25" disks). Title bytes 0..8 at
    // sector 0 are ASCII, byte 0x105 holds nsectors-low, byte 0x106 the
    // catalog cycle/version. We don't ship a DFS engine yet — this stub
    // would return "DFS" once src/fs/dfs.rs lands.

    // Apple DOS 3.3 VTOC at byte 0x11000 (track 17, sector 0). The same
    // byte offset is a fixed point of the .do/.po sector interleave, so a
    // PO-ordered .dsk would also surface here — by the time bytes reach
    // this function the source_reader has already converted PO to DO.
    // Skip the lookup unless the disk is exactly 140 KB (the only Apple-II
    // floppy geometry); otherwise the offset might be inside a different
    // filesystem and false-positive.
    if let Ok(size) = reader.seek(SeekFrom::End(0)) {
        if size == 143_360 && reader.seek(SeekFrom::Start(0x11000)).is_ok() {
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
                return Some("DOS 3.3".to_string());
            }
        }
    }

    // Commodore CBM DOS (1541/1571/1581 .d64/.d71/.d81). Flat sector
    // dumps with no partition table; `looks_like_cbm` gates on the exact
    // geometry length AND the header-sector signature, so the same-size
    // false-positive risk is negligible.
    if crate::fs::cbm::looks_like_cbm(reader, 0).is_some() {
        return Some("CBM DOS".to_string());
    }

    // Atari DOS 2 (.atr body / headerless .xfd). Gated on exact geometry
    // size + a plausible VTOC at sector 360.
    if crate::fs::atari_dos::looks_like_atari_dos(reader, 0).is_some() {
        return Some("Atari DOS".to_string());
    }

    // DragonDOS (flat .dsk). Same byte size as a 40-track RS-DOS disk, but the
    // directory track carries a one's-complement geometry signature, so probe
    // it first among the CoCo family.
    if crate::fs::dragondos::looks_like_dragondos(reader, 0).is_some() {
        return Some("DragonDOS".to_string());
    }

    // OS-9 / NitrOS-9 RBF (flat .dsk/.vdk). Same byte size as a 35-track
    // RS-DOS disk, so probe it first: the LSN-0 identification sector plus a
    // directory root FD discriminate it cleanly.
    if crate::fs::os9::looks_like_os9(reader, 0).is_some() {
        return Some("OS-9".to_string());
    }

    // RS-DOS / CoCo Disk BASIC (flat .dsk/.jvc). No magic; gated on exact
    // geometry + a structurally consistent granule table + directory.
    if crate::fs::rsdos::looks_like_rsdos(reader, 0).is_some() {
        return Some("RS-DOS".to_string());
    }

    // Acorn DFS (flat single-sided .ssd, 40-/80-track). Gated on exact
    // single-sided geometry and a catalogue whose declared sector count
    // matches the disk size (separates a real .ssd from a flat .dsd).
    if crate::fs::dfs::looks_like_dfs(reader, 0).is_some() {
        return Some("Acorn DFS".to_string());
    }

    None
}

impl PartitionTable {
    /// Detect and parse the partition table from a readable+seekable source.
    pub fn detect(reader: &mut (impl Read + Seek)) -> Result<Self, RustyBackupError> {
        // Read first 512 bytes (MBR / protective MBR / DDR)
        reader
            .seek(SeekFrom::Start(0))
            .map_err(RustyBackupError::Io)?;
        let mut mbr_data = [0u8; 512];
        reader
            .read_exact(&mut mbr_data)
            .map_err(|e| RustyBackupError::InvalidMbr(format!("cannot read first sector: {e}")))?;

        // Check for SGI Volume Header (IRIX) before MBR / APM. The 32-bit
        // big-endian magic 0x0BE5A941 at offset 0 is unambiguous — no MBR,
        // GPT-protective-MBR, or APM Driver Descriptor Record starts with
        // those bytes.
        let sgi_sig = u32::from_be_bytes([mbr_data[0], mbr_data[1], mbr_data[2], mbr_data[3]]);
        if sgi_sig == SGI_VOLHDR_MAGIC {
            if let Ok(vh) = SgiVolumeHeader::parse(&mbr_data) {
                return Ok(PartitionTable::Sgi(vh));
            }
            // Magic matched but parsing failed — fall through to surface
            // the InvalidSgi error path. Re-parsing here rather than caching
            // the prior error keeps the control flow obvious.
            return Err(SgiVolumeHeader::parse(&mbr_data).unwrap_err());
        }

        // Check for APM (Driver Descriptor Record signature 0x4552 at offset 0)
        let ddr_sig = u16::from_be_bytes([mbr_data[0], mbr_data[1]]);
        if ddr_sig == 0x4552 {
            reader
                .seek(SeekFrom::Start(0))
                .map_err(RustyBackupError::Io)?;
            if let Ok(apm) = Apm::parse(reader) {
                return Ok(PartitionTable::Apm(apm));
            }
            // Fall through to MBR/GPT parsing on failure
            reader
                .seek(SeekFrom::Start(0))
                .map_err(RustyBackupError::Io)?;
        }

        // Check for an Amiga Rigid Disk Block. RDSK can be at any of the
        // first 16 sectors (in practice it sits at sector 0). The signature
        // 'RDSK' doesn't clash with MBR/GPT/APM/SGI signatures, so we can
        // probe before falling through to MBR parsing.
        let rdsk_present = {
            let mut scan = [0u8; 4];
            let mut found = false;
            for block in 0..rdb::RDB_SCAN_BLOCKS {
                if reader.seek(SeekFrom::Start(block * 512)).is_err() {
                    break;
                }
                if reader.read_exact(&mut scan).is_err() {
                    continue;
                }
                if &scan == RDSK_SIGNATURE {
                    found = true;
                    break;
                }
            }
            found
        };
        if rdsk_present {
            reader.seek(SeekFrom::Start(0))?;
            if let Ok(parsed) = Rdb::parse(reader) {
                return Ok(PartitionTable::Rdb(parsed));
            }
            // Fall through if parsing failed (bad checksum etc.) — surface
            // the lower-level error from MBR/superfloppy paths instead.
            reader.seek(SeekFrom::Start(0))?;
        }

        // Check for AHDI (Atari ST/TT/Falcon hard disks). AHDI has no magic
        // number; the probe runs only on sectors that lack the 0xAA55 MBR
        // signature and have at least one validly-shaped partition entry
        // whose geometry fits the disk size. This must come BEFORE the
        // superfloppy probe because AHDI disks are HDD-shaped (start at
        // sector > 0) — superfloppy would not match them anyway, but the
        // ordering keeps the intent clear.
        let disk_size_bytes = reader
            .seek(SeekFrom::End(0))
            .map_err(RustyBackupError::Io)?;
        if looks_like_ahdi_root(&mbr_data, disk_size_bytes) {
            reader
                .seek(SeekFrom::Start(0))
                .map_err(RustyBackupError::Io)?;
            if let Ok(table) = AhdiTable::detect_and_walk(reader, disk_size_bytes) {
                return Ok(PartitionTable::Ahdi(table));
            }
            // Fall through if walking failed (XGM cycle, etc.) — the
            // superfloppy / MBR paths still get a chance below.
            reader
                .seek(SeekFrom::Start(0))
                .map_err(RustyBackupError::Io)?;
        } else {
            // Restore reader position for the superfloppy probe.
            reader
                .seek(SeekFrom::Start(0))
                .map_err(RustyBackupError::Io)?;
        }

        // Check for X68000 / Human68k partition table. Magic "X68K" lives
        // at byte 2048 (sector 4 with 512-B sectors, sector 2 with 1024-B
        // sectors). No boot signature in sector 0, so this MUST run
        // before the superfloppy probe — a FAT BPB at byte 0 would
        // otherwise short-circuit detection on a multi-partition HDD.
        reader
            .seek(SeekFrom::Start(0))
            .map_err(RustyBackupError::Io)?;
        match X68kPartitionTable::detect_with_geometry(reader) {
            Ok(Some((table, _table_offset, sector_size))) => {
                let size_bytes = reader
                    .seek(SeekFrom::End(0))
                    .map_err(RustyBackupError::Io)?;
                return Ok(PartitionTable::X68k {
                    table,
                    size_bytes,
                    sector_size,
                });
            }
            Ok(None) | Err(_) => {
                // Restore reader for the superfloppy / MBR paths below.
                reader
                    .seek(SeekFrom::Start(0))
                    .map_err(RustyBackupError::Io)?;
            }
        }

        // Check for superfloppy (no partition table) before MBR parsing
        if let Some(fs_hint) = detect_superfloppy(&mbr_data, reader) {
            // Get disk size via seek to end
            let size_bytes = reader
                .seek(SeekFrom::End(0))
                .map_err(RustyBackupError::Io)?;
            return Ok(PartitionTable::None {
                size_bytes,
                fs_hint,
            });
        }

        reader
            .seek(SeekFrom::Start(0))
            .map_err(RustyBackupError::Io)?;

        // Try MBR parsing.  If it fails and the image is a standard floppy
        // size, treat it as a superfloppy with an unrecognized filesystem
        // (e.g. Apple DOS 3.3) rather than surfacing a confusing MBR error.
        let mut mbr = match Mbr::parse(&mbr_data) {
            Ok(m) => m,
            Err(e) => {
                let size_bytes = reader
                    .seek(SeekFrom::End(0))
                    .map_err(RustyBackupError::Io)?;
                if is_floppy_size(size_bytes) {
                    return Ok(PartitionTable::None {
                        size_bytes,
                        fs_hint: "Unknown".to_string(),
                    });
                }
                return Err(e);
            }
        };

        if mbr.is_protective_gpt() {
            // Try parsing GPT
            match Gpt::parse(reader) {
                Ok(gpt) => Ok(PartitionTable::Gpt {
                    protective_mbr: mbr,
                    gpt,
                }),
                Err(e) => {
                    // A protective 0xEE MBR means this IS a GPT disk.
                    // If GPT parsing fails, surface the error instead of
                    // falling back to MBR (which would expose a fake 0xEE
                    // partition spanning the entire disk).
                    Err(e)
                }
            }
        } else {
            // Parse EBR chain for any extended partition entries
            for entry in &mbr.entries {
                if entry.is_extended() && !entry.is_empty() {
                    match mbr::parse_ebr_chain(reader, entry.start_lba) {
                        Ok(logicals) => {
                            mbr.logical_partitions = logicals;
                        }
                        Err(_) => {
                            // EBR parsing failure is non-fatal; just skip logical partitions
                        }
                    }
                    break; // Only one extended partition is valid per MBR
                }
            }
            Ok(PartitionTable::Mbr(mbr))
        }
    }

    /// Get a unified list of partition info for display.
    pub fn partitions(&self) -> Vec<PartitionInfo> {
        match self {
            PartitionTable::Mbr(mbr) => {
                let mut result: Vec<PartitionInfo> = mbr
                    .entries
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| !e.is_empty())
                    .map(|(i, e)| PartitionInfo {
                        index: i,
                        type_name: e.partition_type_name().to_string(),
                        partition_type_byte: e.partition_type,
                        start_lba: e.start_lba as u64,
                        start_byte: None,
                        size_bytes: e.size_bytes(),
                        bootable: e.bootable,
                        is_logical: false,
                        is_extended_container: e.is_extended(),
                        partition_type_string: None,
                        hfs_block_size: None,
                        rdb_part_block: None,
                        drv_name: None,
                    })
                    .collect();

                // Append logical partitions from EBR chain (index 4+)
                for (j, e) in mbr.logical_partitions.iter().enumerate() {
                    result.push(PartitionInfo {
                        index: 4 + j,
                        type_name: e.partition_type_name().to_string(),
                        partition_type_byte: e.partition_type,
                        start_lba: e.start_lba as u64,
                        start_byte: None,
                        size_bytes: e.size_bytes(),
                        bootable: e.bootable,
                        is_logical: true,
                        is_extended_container: false,
                        partition_type_string: None,
                        hfs_block_size: None,
                        rdb_part_block: None,
                        drv_name: None,
                    });
                }

                result
            }
            PartitionTable::Gpt { gpt, .. } => gpt
                .entries
                .iter()
                .enumerate()
                .map(|(i, e)| PartitionInfo {
                    index: i,
                    type_name: format!("{} ({})", e.type_name(), e.name),
                    partition_type_byte: 0,
                    start_lba: e.first_lba,
                    start_byte: None,
                    size_bytes: e.size_bytes(),
                    bootable: false,
                    is_logical: false,
                    is_extended_container: false,
                    partition_type_string: Some(e.type_guid.to_string_formatted()),
                    hfs_block_size: None,
                    rdb_part_block: None,
                    drv_name: None,
                })
                .collect(),
            PartitionTable::Apm(apm) => {
                let block_size = apm.ddr.block_size;
                apm.entries
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| e.is_data_partition())
                    .map(|(i, e)| PartitionInfo {
                        index: i,
                        type_name: format!("{} ({})", e.partition_type, e.name),
                        partition_type_byte: 0,
                        start_lba: e.start_block as u64 * block_size as u64 / 512,
                        start_byte: None,
                        size_bytes: e.size_bytes(block_size),
                        bootable: e.is_bootable(),
                        is_logical: false,
                        is_extended_container: false,
                        partition_type_string: Some(e.partition_type.clone()),
                        hfs_block_size: None,
                        rdb_part_block: None,
                        drv_name: None,
                    })
                    .collect()
            }
            PartitionTable::Sgi(vh) => {
                vh.partitions
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| !e.is_empty())
                    .filter_map(|(i, e)| {
                        let ptype = e.partition_type();
                        // Per the plan, skip VOLHDR, VOLUME, XFSLOG, XLV,
                        // XVM, and RAW — none are browseable filesystems and
                        // the disk-wide wrappers (VOLHDR, VOLUME) overlap
                        // real partitions, which would confuse the list.
                        if ptype.is_skipped_from_browse() {
                            return None;
                        }
                        let partition_type_byte = match ptype {
                            sgi::SgiPartitionType::Xfs => SGI_TYPE_BYTE_XFS,
                            sgi::SgiPartitionType::Efs => SGI_TYPE_BYTE_EFS,
                            _ => 0,
                        };
                        let type_name = match ptype {
                            sgi::SgiPartitionType::Xfs => "SGI XFS".to_string(),
                            sgi::SgiPartitionType::Efs => "SGI EFS".to_string(),
                            other => format!("SGI {}", other.display_name()),
                        };
                        Some(PartitionInfo {
                            index: i,
                            type_name,
                            partition_type_byte,
                            start_lba: e.first as u64,
                            start_byte: None,
                            size_bytes: e.size_bytes(),
                            bootable: false,
                            is_logical: false,
                            is_extended_container: false,
                            // SGI partitions route by synthetic byte
                            // (0xA0 / 0xA1) per the plan; setting a type
                            // string here would short-circuit
                            // `open_filesystem` into the APM string router.
                            partition_type_string: None,
                            hfs_block_size: None,
                            rdb_part_block: None,
                            drv_name: None,
                        })
                    })
                    .collect()
            }
            PartitionTable::Rdb(rdb) => rdb
                .partitions
                .iter()
                .enumerate()
                .map(|(i, p)| PartitionInfo {
                    index: i,
                    // Show both the human name and the device name, e.g.
                    // "AmigaDOS FFS-Intl (DH0)" or "SFS (Amiga) (DH1)".
                    type_name: format!(
                        "{} ({})",
                        rdb::dos_type_display_name(p.dos_type),
                        p.drv_name,
                    ),
                    partition_type_byte: 0,
                    start_lba: p.start_lba_512(),
                    start_byte: None,
                    size_bytes: p.size_bytes(),
                    bootable: p.is_bootable(),
                    is_logical: false,
                    is_extended_container: false,
                    partition_type_string: Some(p.dos_type_string()),
                    hfs_block_size: None,
                    rdb_part_block: Some(p.block_num),
                    drv_name: Some(p.drv_name.clone()),
                })
                .collect(),
            PartitionTable::Ahdi(table) => {
                let mut out: Vec<PartitionInfo> = Vec::new();
                for (i, e) in table.primary.iter().enumerate() {
                    if e.is_empty() || !e.exists() {
                        continue;
                    }
                    // XGM is a container, not a filesystem — surface it so the
                    // user sees the table shape but don't emit a separate row
                    // when its logical children are about to follow.
                    out.push(PartitionInfo {
                        index: i,
                        type_name: format!("AHDI {}", e.kind.display_name()),
                        partition_type_byte: e.kind.synthetic_type_byte(),
                        start_lba: e.start_sector as u64,
                        start_byte: None,
                        size_bytes: e.size_bytes(),
                        bootable: e.bootable(),
                        is_logical: false,
                        is_extended_container: matches!(e.kind, AhdiPartitionKind::Xgm),
                        partition_type_string: None,
                        hfs_block_size: None,
                        rdb_part_block: None,
                        drv_name: None,
                    });
                }
                for (j, e) in table.logical.iter().enumerate() {
                    out.push(PartitionInfo {
                        index: AHDI_NUM_SLOTS + j,
                        type_name: format!("AHDI {}", e.kind.display_name()),
                        partition_type_byte: e.kind.synthetic_type_byte(),
                        start_lba: e.start_sector as u64,
                        start_byte: None,
                        size_bytes: e.size_bytes(),
                        bootable: e.bootable(),
                        is_logical: true,
                        is_extended_container: false,
                        partition_type_string: None,
                        hfs_block_size: None,
                        rdb_part_block: None,
                        drv_name: None,
                    });
                }
                out
            }
            PartitionTable::X68k {
                table, sector_size, ..
            } => {
                // The table counts in logical sectors of `sector_size`
                // (1024 B for SCSI, 256 B for SASI, 512 B for the synthetic
                // test convention). The true partition offset is
                // `start_sector * sector_size`; carry it in `start_byte`
                // (read via `byte_offset()`) because 256-byte SASI disks can
                // start on a non-512-aligned byte that `start_lba * 512`
                // cannot express. `start_lba` keeps the floored 512-LBA
                // value for the inspect display and legacy consumers.
                table
                    .entries
                    .iter()
                    .enumerate()
                    .map(|(i, e)| {
                        let start_byte = e.start_sector as u64 * *sector_size;
                        let is_human = e.is_human68k();
                        let type_name = if is_human {
                            format!("X68k Human68k ({})", e.name_display)
                        } else {
                            format!("X68k {}", e.name_display)
                        };
                        PartitionInfo {
                            index: i,
                            type_name,
                            // Reuse the FAT12 byte (0x01) for Human68k so the
                            // existing FAT auto-detect arm finds the
                            // partition_type_string-less fallback. Human68k's
                            // BPB is FAT-compatible so the read path Just
                            // Works once the offset is right.
                            partition_type_byte: if is_human { 0x01 } else { 0 },
                            start_lba: start_byte / 512,
                            start_byte: Some(start_byte),
                            size_bytes: e.length_sectors as u64 * *sector_size,
                            bootable: false,
                            is_logical: false,
                            is_extended_container: false,
                            // String dispatch into the Human68k engine for
                            // accurate filesystem reporting (18.3 names +
                            // Shift-JIS lossy display).
                            partition_type_string: if is_human {
                                Some("human68k".to_string())
                            } else {
                                None
                            },
                            hfs_block_size: None,
                            rdb_part_block: None,
                            drv_name: None,
                        }
                    })
                    .collect()
            }
            PartitionTable::None {
                size_bytes,
                fs_hint,
            } => {
                // AmigaDOS superfloppies report their DosType as the fs_hint
                // (e.g. "DOS\\1"); route that through `partition_type_string`
                // so the AFFS driver gets dispatched. X68000 Human68k floppies
                // report "human68k" and route the same way so the Human68k
                // driver is dispatched (the FAT byte alone can't distinguish
                // the big-endian Sharp/KG variant). Other hints (FAT, HFS,
                // ext, etc.) keep `partition_type_string: None` so dispatch
                // falls back to MBR type-byte detection.
                let is_amiga_dos = fs_hint.starts_with("DOS\\") && fs_hint.len() == 5;
                let is_human68k = fs_hint == "human68k";
                let display_name = if is_amiga_dos {
                    let variant = fs_hint.as_bytes()[4] - b'0';
                    let raw = u32::from_be_bytes([b'D', b'O', b'S', variant]);
                    rdb::dos_type_display_name(raw)
                } else if is_human68k {
                    "Human68k (FAT)".to_string()
                } else {
                    fs_hint.clone()
                };
                vec![PartitionInfo {
                    index: 0,
                    type_name: display_name,
                    partition_type_byte: 0,
                    start_lba: 0,
                    start_byte: None,
                    size_bytes: *size_bytes,
                    bootable: false,
                    is_logical: false,
                    is_extended_container: false,
                    partition_type_string: if is_amiga_dos || is_human68k {
                        Some(fs_hint.clone())
                    } else {
                        None
                    },
                    hfs_block_size: None,
                    rdb_part_block: None,
                    drv_name: None,
                }]
            }
        }
    }

    /// Get a human-readable name for the partition table type.
    pub fn type_name(&self) -> &'static str {
        match self {
            PartitionTable::Mbr(_) => "MBR",
            PartitionTable::Gpt { .. } => "GPT",
            PartitionTable::Apm(_) => "APM",
            PartitionTable::Rdb(_) => "RDB",
            PartitionTable::Sgi(_) => "SGI",
            PartitionTable::Ahdi(_) => "AHDI",
            PartitionTable::X68k { .. } => "X68k",
            PartitionTable::None { .. } => "None",
        }
    }

    /// Get the MBR disk signature (available for both MBR and GPT via protective MBR).
    /// APM, SGI, and superfloppy have no disk signature, returns 0.
    pub fn disk_signature(&self) -> u32 {
        match self {
            PartitionTable::Mbr(mbr) => mbr.disk_signature,
            PartitionTable::Gpt { protective_mbr, .. } => protective_mbr.disk_signature,
            PartitionTable::Apm(_)
            | PartitionTable::Rdb(_)
            | PartitionTable::Sgi(_)
            | PartitionTable::Ahdi(_)
            | PartitionTable::X68k { .. }
            | PartitionTable::None { .. } => 0,
        }
    }
}

/// Format a byte count as a human-readable size string using binary (base-1024) units.
pub fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    if bytes >= TIB {
        format!("{:.1} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Partition size override for VHD export and restore.
pub struct PartitionSizeOverride {
    pub index: usize,
    pub start_lba: u64,
    pub original_size: u64,
    pub export_size: u64,
    /// New start LBA for this partition (for restore with alignment changes).
    /// When `None`, the original `start_lba` is kept.
    pub new_start_lba: Option<u64>,
    /// Heads for CHS recalculation (0 = don't touch CHS fields).
    pub heads: u16,
    /// Sectors per track for CHS recalculation (0 = don't touch CHS fields).
    pub sectors_per_track: u16,
}

impl PartitionSizeOverride {
    /// Create a simple size-only override (backward compatible with VHD export).
    pub fn size_only(index: usize, start_lba: u64, original_size: u64, export_size: u64) -> Self {
        Self {
            index,
            start_lba,
            original_size,
            export_size,
            new_start_lba: None,
            heads: 0,
            sectors_per_track: 0,
        }
    }

    /// The effective start LBA (new if set, otherwise original).
    pub fn effective_start_lba(&self) -> u64 {
        self.new_start_lba.unwrap_or(self.start_lba)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn byte_offset_honors_start_byte_override() {
        let mut p = PartitionInfo {
            index: 0,
            type_name: "t".into(),
            partition_type_byte: 0,
            start_lba: 64,
            start_byte: None,
            size_bytes: 0,
            bootable: false,
            is_logical: false,
            is_extended_container: false,
            partition_type_string: None,
            hfs_block_size: None,
            rdb_part_block: None,
            drv_name: None,
        };
        // No override -> start_lba * 512.
        assert_eq!(p.byte_offset(), 64 * 512);
        // Override -> the exact (possibly non-512-aligned) byte.
        p.start_byte = Some(8448);
        assert_eq!(p.byte_offset(), 8448);
    }

    #[test]
    fn x68k_sasi_partition_info_carries_non_aligned_byte_offset() {
        // SASI table at byte 0x400, 256-byte sectors, partition at sector 33.
        let table = X68kPartitionTable {
            disk_size_field: 0,
            entries: vec![x68k::X68kEntry {
                name_raw: *b"Human68k",
                name_display: "Human68k".into(),
                start_sector: 33,
                length_sectors: 40750,
            }],
        };
        let pt = PartitionTable::X68k {
            table,
            size_bytes: 10_441_728,
            sector_size: 256,
        };
        let infos = pt.partitions();
        assert_eq!(infos.len(), 1);
        // byte_offset() must be the exact 256-byte-sector offset, which is
        // NOT recoverable from start_lba * 512.
        assert_eq!(infos[0].byte_offset(), 33 * 256);
        assert_eq!(infos[0].size_bytes, 40750 * 256);
        assert_ne!(infos[0].start_lba * 512, infos[0].byte_offset());
    }

    /// (type, start_lba, sectors, start_head, start_sec, start_cyl,
    /// end_head, end_sec, end_cyl) — test-only MBR entry tuple.
    type MbrChsEntry = (u8, u32, u32, u8, u8, u16, u8, u8, u16);

    /// Build MBR bytes with specific partition start LBAs and CHS values.
    fn make_mbr_with_chs(entries: &[MbrChsEntry]) -> [u8; 512] {
        let mut data = [0u8; 512];
        data[440..444].copy_from_slice(&0x12345678u32.to_le_bytes());

        for (i, &(ptype, start_lba, sectors, sh, ss, sc, eh, es, ec)) in entries.iter().enumerate()
        {
            let offset = 446 + i * 16;
            data[offset] = 0x80; // bootable
            data[offset + 1] = sh;
            data[offset + 2] = (ss & 0x3F) | ((sc >> 2) as u8 & 0xC0);
            data[offset + 3] = sc as u8;
            data[offset + 4] = ptype;
            data[offset + 5] = eh;
            data[offset + 6] = (es & 0x3F) | ((ec >> 2) as u8 & 0xC0);
            data[offset + 7] = ec as u8;
            data[offset + 8..offset + 12].copy_from_slice(&start_lba.to_le_bytes());
            data[offset + 12..offset + 16].copy_from_slice(&sectors.to_le_bytes());
        }

        data[510] = 0x55;
        data[511] = 0xAA;
        data
    }

    #[test]
    fn test_detect_mbr_from_reader() {
        let mbr_data = make_mbr_with_chs(&[(0x0C, 2048, 1048576, 0, 1, 0, 254, 63, 100)]);
        let mut cursor = Cursor::new(mbr_data.to_vec());
        let table = PartitionTable::detect(&mut cursor).unwrap();

        assert_eq!(table.type_name(), "MBR");
        assert_eq!(table.partitions().len(), 1);
        assert_eq!(table.partitions()[0].type_name, "FAT32 (LBA)");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1024), "1.0 KiB");
        assert_eq!(format_size(1048576), "1.0 MiB");
        assert_eq!(format_size(1073741824), "1.0 GiB");
        assert_eq!(format_size(1099511627776), "1.0 TiB");
        assert_eq!(format_size(536870912), "512.0 MiB");
    }

    #[test]
    fn test_detect_superfloppy_fat12() {
        // Build a realistic FAT12 VBR with non-zero boot code in partition table area
        let mut data = vec![0u8; 1474560]; // 1.44 MB floppy
        data[0] = 0xEB; // JMP short
        data[1] = 0x3C;
        data[2] = 0x90; // NOP
                        // OEM ID
        data[3..11].copy_from_slice(b"MSDOS5.0");
        // bytes_per_sector = 512
        data[11] = 0x00;
        data[12] = 0x02;
        // sectors_per_cluster = 1
        data[13] = 0x01;
        // reserved_sectors = 1
        data[14] = 0x01;
        data[15] = 0x00;
        // num_fats = 2
        data[16] = 0x02;
        // media descriptor = 0xF0 (floppy)
        data[21] = 0xF0;
        // Boot signature
        data[510] = 0x55;
        data[511] = 0xAA;
        // Simulate non-zero bootstrap code in the partition table area (bytes 446-509)
        // This is common in real floppy VBRs and should NOT prevent superfloppy detection
        for (i, slot) in data[446..510].iter_mut().enumerate() {
            *slot = 0xCD + ((446 + i) as u8 % 17); // arbitrary non-zero boot code
        }

        let mut cursor = Cursor::new(data);
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].start_lba, 0);
        assert_eq!(parts[0].size_bytes, 1474560);
        assert_eq!(parts[0].type_name, "FAT");
    }

    #[test]
    fn test_detect_superfloppy_ntfs() {
        // Build a minimal NTFS boot sector. The OEM ID "NTFS    " at offset 3
        // is the discriminator; the bootstrap code (and the 0xAA55 signature)
        // would otherwise be misread as an MBR with junk partition entries.
        let mut data = vec![0u8; 1024 * 1024];
        data[0] = 0xEB;
        data[1] = 0x52;
        data[2] = 0x90;
        data[3..11].copy_from_slice(b"NTFS    ");
        // Realistic bootstrap bytes in the would-be partition table area.
        for (i, slot) in data[446..510].iter_mut().enumerate() {
            *slot = 0x4E + ((446 + i) as u8 % 13);
        }
        data[510] = 0x55;
        data[511] = 0xAA;

        let mut cursor = Cursor::new(data);
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "NTFS");
    }

    #[test]
    fn test_detect_superfloppy_exfat() {
        // exFAT boot sector: OEM ID "EXFAT   " at offset 3.
        let mut data = vec![0u8; 1024 * 1024];
        data[0] = 0xEB;
        data[1] = 0x76;
        data[2] = 0x90;
        data[3..11].copy_from_slice(b"EXFAT   ");
        for (i, slot) in data[446..510].iter_mut().enumerate() {
            *slot = 0x4E + ((446 + i) as u8 % 13);
        }
        data[510] = 0x55;
        data[511] = 0xAA;

        let mut cursor = Cursor::new(data);
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "exFAT");
    }

    #[test]
    fn test_detect_superfloppy_xfs() {
        // XFS superblock magic "XFSB" at byte 0 of the partition.
        let mut data = vec![0u8; 1024 * 1024];
        data[0..4].copy_from_slice(b"XFSB");
        // No 0xAA55 signature; bare-FS probe must fire before MBR parse.
        let mut cursor = Cursor::new(data);
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "XFS");
    }

    #[test]
    fn test_detect_superfloppy_ext() {
        // ext2/3/4: magic 0xEF53 (LE) at byte 0x38 of the superblock at offset 1024.
        let mut data = vec![0u8; 1024 * 1024];
        data[1024 + 0x38] = 0x53;
        data[1024 + 0x39] = 0xEF;
        let mut cursor = Cursor::new(data);
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "ext");
    }

    #[test]
    fn test_detect_superfloppy_btrfs() {
        // btrfs: magic "_BHRfS_M" at offset 0x40 of the superblock at offset 0x10000.
        let mut data = vec![0u8; 0x10000 + 4096];
        data[0x10000 + 0x40..0x10000 + 0x48].copy_from_slice(b"_BHRfS_M");
        let mut cursor = Cursor::new(data);
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "btrfs");
    }

    #[test]
    fn test_detect_superfloppy_hfs() {
        // Build a minimal HFS floppy: signature 0x4244 at offset 1024
        let mut data = vec![0u8; 819200]; // 800K floppy
                                          // No JMP instruction at byte 0, no MBR signature
        data[1024] = 0x42;
        data[1025] = 0x44;

        let mut cursor = Cursor::new(data);
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "HFS");
    }

    #[test]
    fn test_hfs_superfloppy_backup_flow() {
        use std::io::BufReader;

        // Simulate the EXACT backup flow for an HFS floppy using a real
        // temp file (not Cursor) to catch File+BufReader interaction bugs.
        let mut data = vec![0xAAu8; 819200]; // 800K floppy, filled with 0xAA
                                             // HFS MDB signature at offset 1024
        data[1024] = 0x42;
        data[1025] = 0x44;

        // Write to a real temp file
        let tmp_dir = std::env::temp_dir();
        let tmp_path = tmp_dir.join("rusty_backup_test_hfs_floppy.img");
        std::fs::write(&tmp_path, &data).unwrap();

        let file = std::fs::File::open(&tmp_path).unwrap();
        let mut source = BufReader::new(file);

        // Step 1: Read first 512 bytes for MBR export (same as backup)
        let mut mbr_bytes = [0u8; 512];
        source.read_exact(&mut mbr_bytes).unwrap();
        source.seek(SeekFrom::Start(0)).unwrap();

        // Step 2: Detect partition table
        let table = PartitionTable::detect(&mut source).unwrap();
        assert_eq!(table.type_name(), "None");
        let partitions = table.partitions();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].start_lba, 0);
        let image_size = partitions[0].size_bytes;
        assert_eq!(image_size, 819200);

        // Step 3: Simulate get_file_size bypassing BufReader
        // (In real code: get_file_size(source.get_ref(), ...) seeks the
        // underlying File directly via &File impl Seek)
        {
            let inner: &std::fs::File = source.get_ref();
            let mut file_ref = inner;
            let _sz = std::io::Seek::seek(&mut file_ref, SeekFrom::End(0)).unwrap();
            std::io::Seek::seek(&mut file_ref, SeekFrom::Start(0)).unwrap();
        }

        // Step 4: Seek back and read data (trim-based path)
        let part_offset = partitions[0].start_lba * 512; // 0
        source.seek(SeekFrom::Start(part_offset)).unwrap();
        let part_reader = (&mut source).take(image_size);

        let mut output = Vec::new();
        let mut buf = [0u8; 65536]; // 64K chunks
        let mut limited = part_reader;
        loop {
            let n = limited.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            output.extend_from_slice(&buf[..n]);
        }

        // Cleanup
        let _ = std::fs::remove_file(&tmp_path);

        // Verify we read the full floppy
        assert_eq!(
            output.len(),
            819200,
            "Expected 819200 bytes but got {}",
            output.len()
        );
        assert_eq!(&output, &data);
    }

    #[test]
    fn test_real_mbr_not_superfloppy() {
        // A real MBR with valid partition entries should NOT be detected as superfloppy
        let mbr_data = make_mbr_with_chs(&[(0x0C, 2048, 1048576, 0, 1, 0, 254, 63, 100)]);
        let mut cursor = Cursor::new(mbr_data.to_vec());
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "MBR");
    }

    /// Build a minimal GPT disk image with a protective MBR + GPT header + entries.
    fn make_gpt_disk_image(entries: &[(gpt::Guid, u64, u64, &str)]) -> Vec<u8> {
        let num_entries = 128u32;
        let entry_size = 128u32;
        let total_sectors: u64 = 34 + 1024;
        let mut image = vec![0u8; total_sectors as usize * 512];

        // Protective MBR at LBA 0
        image[446] = 0x00;
        image[450] = 0xEE; // GPT protective type
        image[454..458].copy_from_slice(&1u32.to_le_bytes());
        image[458..462].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes());
        image[510] = 0x55;
        image[511] = 0xAA;

        // GPT header at LBA 1
        let hdr = 512usize;
        image[hdr..hdr + 8].copy_from_slice(&0x5452415020494645u64.to_le_bytes());
        image[hdr + 8..hdr + 12].copy_from_slice(&0x00010000u32.to_le_bytes());
        image[hdr + 12..hdr + 16].copy_from_slice(&92u32.to_le_bytes());
        image[hdr + 24..hdr + 32].copy_from_slice(&1u64.to_le_bytes());
        image[hdr + 32..hdr + 40].copy_from_slice(&(total_sectors - 1).to_le_bytes());
        image[hdr + 40..hdr + 48].copy_from_slice(&34u64.to_le_bytes());
        image[hdr + 48..hdr + 56].copy_from_slice(&(total_sectors - 34).to_le_bytes());
        image[hdr + 56..hdr + 72].copy_from_slice(&[0xAAu8; 16]);
        image[hdr + 72..hdr + 80].copy_from_slice(&2u64.to_le_bytes());
        image[hdr + 80..hdr + 84].copy_from_slice(&num_entries.to_le_bytes());
        image[hdr + 84..hdr + 88].copy_from_slice(&entry_size.to_le_bytes());

        // Partition entries at LBA 2
        for (i, (type_guid, first_lba, last_lba, name)) in entries.iter().enumerate() {
            let off = 1024 + i * entry_size as usize;
            image[off..off + 16].copy_from_slice(type_guid.as_bytes());
            // unique GUID
            image[off + 16..off + 32].copy_from_slice(&[
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                (i as u8) + 1,
            ]);
            image[off + 32..off + 40].copy_from_slice(&first_lba.to_le_bytes());
            image[off + 40..off + 48].copy_from_slice(&last_lba.to_le_bytes());
            let name_off = off + 56;
            for (j, ch) in name.encode_utf16().enumerate() {
                let o = name_off + j * 2;
                if o + 2 <= off + entry_size as usize {
                    image[o..o + 2].copy_from_slice(&ch.to_le_bytes());
                }
            }
        }

        image
    }

    #[test]
    fn test_detect_valid_gpt_disk() {
        let ms_basic = gpt::Guid::from_string("EBD0A0A2-B9E5-4433-87C0-68B6B72699C7").unwrap();
        let image = make_gpt_disk_image(&[(ms_basic, 2048, 1050623, "DATA")]);
        let mut cursor = Cursor::new(image);
        let table = PartitionTable::detect(&mut cursor).unwrap();

        assert_eq!(table.type_name(), "GPT");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].start_lba, 2048);
        assert_eq!(parts[0].size_bytes, (1050623 - 2048 + 1) * 512);
        // GPT entries should have partition_type_string set
        assert!(parts[0].partition_type_string.is_some());
        assert_eq!(
            parts[0].partition_type_string.as_deref().unwrap(),
            "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7"
        );
    }

    #[test]
    fn test_protective_mbr_with_corrupt_gpt_returns_error() {
        // Build a disk with protective 0xEE MBR but no valid GPT header
        let mut image = vec![0u8; 34 * 512];
        // Protective MBR
        image[450] = 0xEE;
        image[454..458].copy_from_slice(&1u32.to_le_bytes());
        image[458..462].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes());
        image[510] = 0x55;
        image[511] = 0xAA;
        // LBA 1 is all zeros — invalid GPT signature

        let mut cursor = Cursor::new(image);
        let result = PartitionTable::detect(&mut cursor);
        assert!(
            result.is_err(),
            "Expected error when GPT header is corrupt, not a fallback to MBR"
        );
    }

    #[test]
    fn test_detect_sgi_volhdr_fixture() {
        // The fixture is just 4 KiB; detect() expects a seekable source it
        // can also probe for size, so pad to a plausible disk image size.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/sgi/irix_volhdr.bin");
        let mut buf = std::fs::read(&path).expect("fixture present");
        buf.resize(1 << 20, 0); // 1 MiB padding — the partition list math
                                // doesn't need real data past the header.

        let mut cursor = Cursor::new(buf);
        let table = PartitionTable::detect(&mut cursor).expect("SGI detected");
        assert_eq!(table.type_name(), "SGI");

        let parts = table.partitions();
        // Disk-wide wrappers (VOLHDR, VOLUME) are filtered out of the
        // surfaced list; the IRIX 6.5 fixture has exactly one real
        // partition (XFS at index 0).
        assert_eq!(parts.len(), 1, "expected one displayed partition");
        let xfs = &parts[0];
        assert_eq!(xfs.type_name, "SGI XFS");
        assert_eq!(xfs.partition_type_byte, sgi::SGI_TYPE_BYTE_XFS);
        assert_eq!(xfs.start_lba, 0x0004_1000);
        assert_eq!(xfs.size_bytes, 0x0BB3_F000u64 * 512);
        assert_eq!(table.disk_signature(), 0);
        // SGI partitions must NOT set `partition_type_string` — that field
        // is the APM/GPT string-route channel and would short-circuit
        // `open_filesystem` past the synthetic type-byte dispatch.
        assert!(
            xfs.partition_type_string.is_none(),
            "SGI partitions must route by type byte, not string"
        );
    }

    #[test]
    fn test_detect_ahdi_two_partitions() {
        use byteorder::{BigEndian, ByteOrder};
        // Build a synthetic AHDI disk: 32 MiB, with GEM @ sector 2 (8 MiB)
        // and BGM @ sector 16386 (8 MiB). No 0xAA55 trailing signature.
        let disk_sectors: usize = 65_536;
        let mut disk = vec![0u8; disk_sectors * 512];

        // GEM at slot 0
        let s0 = 0x1C6;
        disk[s0] = 0x01;
        disk[s0 + 1..s0 + 4].copy_from_slice(b"GEM");
        BigEndian::write_u32(&mut disk[s0 + 4..s0 + 8], 2);
        BigEndian::write_u32(&mut disk[s0 + 8..s0 + 12], 16_384);

        // BGM at slot 1
        let s1 = 0x1C6 + 12;
        disk[s1] = 0x01;
        disk[s1 + 1..s1 + 4].copy_from_slice(b"BGM");
        BigEndian::write_u32(&mut disk[s1 + 4..s1 + 8], 16_386);
        BigEndian::write_u32(&mut disk[s1 + 8..s1 + 12], 16_384);

        // Disk size field
        BigEndian::write_u32(&mut disk[0x1F6..0x1FA], disk_sectors as u32);

        // Stamp the AHDI checksum so checksum_valid is true.
        let mut sum: u32 = 0;
        for chunk in disk[..512].chunks_exact(2) {
            sum = sum.wrapping_add(u16::from_be_bytes([chunk[0], chunk[1]]) as u32);
        }
        let cksum = ((0x1234u32.wrapping_sub(sum)) & 0xFFFF) as u16;
        BigEndian::write_u16(&mut disk[0x1FE..0x200], cksum);

        let mut cursor = Cursor::new(disk);
        let table = PartitionTable::detect(&mut cursor).expect("AHDI detected");
        assert_eq!(table.type_name(), "AHDI");
        let parts = table.partitions();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].type_name, "AHDI GEM");
        assert_eq!(parts[0].start_lba, 2);
        assert_eq!(parts[0].size_bytes, 16_384u64 * 512);
        assert_eq!(parts[0].partition_type_byte, 0x01); // FAT12
        assert_eq!(parts[1].type_name, "AHDI BGM");
        assert_eq!(parts[1].partition_type_byte, 0x06); // FAT16
        assert!(!parts[0].is_logical);
        assert!(!parts[0].is_extended_container);
    }

    #[test]
    fn test_detect_ahdi_does_not_fire_on_fat_superfloppy() {
        // A FAT12 floppy must still resolve to PartitionTable::None — the
        // AHDI probe should reject when the BPB is valid (`0xEB` JMP + valid
        // BPB fields) and there is no clean AHDI partition entry shape at
        // 0x1C6+.
        let mut data = vec![0u8; 1_474_560];
        data[0] = 0xEB;
        data[1] = 0x3C;
        data[2] = 0x90;
        data[3..11].copy_from_slice(b"MSDOS5.0");
        data[11] = 0x00; // 512 bps
        data[12] = 0x02;
        data[13] = 0x01;
        data[14] = 0x01;
        data[15] = 0x00;
        data[16] = 0x02;
        data[21] = 0xF0;
        // Leave bytes 446..510 zero so the AHDI entries at 0x1C6 are all
        // empty. AHDI would reject — no exists() entry.
        data[510] = 0x55;
        data[511] = 0xAA;

        let mut cursor = Cursor::new(data);
        let table = PartitionTable::detect(&mut cursor).unwrap();
        assert_eq!(table.type_name(), "None");
        assert_eq!(table.partitions()[0].type_name, "FAT");
    }

    #[test]
    fn test_sgi_magic_routed_to_sgi_not_mbr() {
        // Build a 1 MiB cursor whose sector 0 has the SGI magic and a
        // 0xAA55 trailing signature too. Detection must pick SGI, not
        // misparse as a borked MBR.
        let mut buf = vec![0u8; 1 << 20];
        buf[0..4].copy_from_slice(&sgi::SGI_VOLHDR_MAGIC.to_be_bytes());
        // Put a single XFS partition entry so partitions() yields a row.
        let off = 0x138;
        let blocks: u32 = 16 * 1024 * 2; // 16 MiB
        let first: u32 = 64;
        buf[off..off + 4].copy_from_slice(&blocks.to_be_bytes());
        buf[off + 4..off + 8].copy_from_slice(&first.to_be_bytes());
        buf[off + 8..off + 12].copy_from_slice(&sgi::SgiPartitionType::Xfs.as_u32().to_be_bytes());
        // Boot-signature bytes are deliberately left as 0 — but even if a
        // disk's bytes happened to look like a valid MBR signature, the SGI
        // magic check runs first.
        buf[510] = 0x55;
        buf[511] = 0xAA;

        let mut cursor = Cursor::new(buf);
        let table = PartitionTable::detect(&mut cursor).expect("SGI detected");
        assert_eq!(table.type_name(), "SGI");
        assert_eq!(table.partitions().len(), 1);
    }

    /// X68000 Human68k floppy with the standard little-endian BPB at offset
    /// 11 ("X68IPL30" disks) and a 68000 BRA.S (0x60) boot opcode. Must be
    /// detected as a `human68k` superfloppy, not misparsed as an MBR — the
    /// regression behind "invalid boot signature: expected 0xAA55".
    #[test]
    fn human68k_floppy_le_bpb_detected_as_superfloppy() {
        // 1232 sectors x 1024 B = 1,261,568 B (X68000 2HD), matching the
        // real MasterDisk_V2.xdf geometry.
        let total = 1232usize * 1024;
        let mut buf = vec![0u8; total];
        buf[0] = 0x60; // BRA.S (68000), not x86 JMP
        buf[1] = 0x3c;
        buf[3..11].copy_from_slice(b"X68IPL30");
        buf[11] = 0x00; // bytes/sector LE = 1024
        buf[12] = 0x04;
        buf[13] = 0x01; // sectors/cluster
        buf[14] = 0x01; // reserved sectors LE = 1
        buf[15] = 0x00;
        buf[16] = 0x02; // num FATs
        buf[17] = 0xc0; // root entries LE = 192
        buf[18] = 0x00;
        buf[19] = 0xd0; // total sectors (16-bit) LE = 1232
        buf[20] = 0x04;
        buf[21] = 0xfe; // media descriptor
        buf[22] = 0x02; // sectors/FAT LE = 2
        buf[23] = 0x00;
        // No 0xAA55 signature — Human68k floppies don't carry one.

        let mut cursor = Cursor::new(buf);
        let table = PartitionTable::detect(&mut cursor).expect("human68k detected");
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "Human68k (FAT)");
        assert_eq!(parts[0].partition_type_string.as_deref(), Some("human68k"));
        assert_eq!(parts[0].size_bytes, total as u64);
    }

    /// Same, but with the Sharp/KG big-endian BPB at offset 0x12 ("Hudson
    /// soft" game disks). Human68kBpb::parse handles both layouts.
    #[test]
    fn human68k_floppy_be_bpb_detected_as_superfloppy() {
        let total = 1232usize * 1024;
        let mut buf = vec![0u8; total];
        buf[0] = 0x60; // BRA.S
        buf[1] = 0x1c;
        buf[2..16].copy_from_slice(b"Hudson soft 2.");
        // Big-endian BPB at 0x12.
        buf[0x12] = 0x04; // bytes/sector BE = 1024
        buf[0x13] = 0x00;
        buf[0x14] = 0x01; // sectors/cluster
        buf[0x15] = 0x02; // num FATs
        buf[0x16] = 0x00; // reserved sectors BE = 1
        buf[0x17] = 0x01;
        buf[0x18] = 0x00; // root entries BE = 192
        buf[0x19] = 0xc0;
        buf[0x1a] = 0x04; // total sectors (16-bit) BE = 1232
        buf[0x1b] = 0xd0;
        buf[0x1c] = 0xfe; // media descriptor
        buf[0x1d] = 0x02; // sectors/FAT

        let mut cursor = Cursor::new(buf);
        let table = PartitionTable::detect(&mut cursor).expect("human68k detected");
        assert_eq!(table.type_name(), "None");
        let parts = table.partitions();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].type_name, "Human68k (FAT)");
        assert_eq!(parts[0].partition_type_string.as_deref(), Some("human68k"));
    }
}
