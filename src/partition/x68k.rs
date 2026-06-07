//! Sharp X68000 SASI/SCSI HDD partition table.
//!
//! Used by Human68k on every X68000 hard-disk image (.hds / .hdf / .hdm
//! files, also raw `.img` files for the MiSTer X68000 core). Distinct
//! from MBR, AHDI, APM, and the X68000 floppy `.d88` container.
//!
//! ## On-disk layout
//!
//! The partition table location and the logical sector size depend on the
//! disk's controller convention (see [`X68kPartitionTable::detect_with_geometry`]):
//!
//! | controller (byte-0 signature) | table offset | sector size |
//! |-------------------------------|--------------|-------------|
//! | SCSI / SxSI (`X68SCSI1`)       | 2048 (0x800) | 1024 B      |
//! | SASI (`\x82w68000W` or custom IPL) | 1024 (0x400) | 256 B  |
//! | synthetic test default        | 2048 (0x800) | 512 B       |
//!
//! `start_sector` / `length_sectors` count in those logical sectors, so a
//! partition's byte offset is `start_sector * sector_size` — which can be
//! non-512-aligned on SASI disks. The table itself is a 16-byte header
//! followed by 8 fixed-size 16-byte partition entries:
//!
//! ```text
//! 0x00..0x04  magic   "X68K" (BE u32 0x5836384B)
//! 0x04..0x08  size    BE u32 — disk size in some unit (we don't rely
//!                              on this)
//! 0x08..0x0C  size2   BE u32 — copy of size
//! 0x0C..0x10  unknown BE u32 — reserved
//! 0x10..0x90  entries 8 × 16 bytes (see below)
//! ```
//!
//! Each 16-byte partition entry:
//!
//! ```text
//! 0x00..0x08  name    8 bytes Shift-JIS (space-padded). Real disks
//!                     use "Human   " (or "Human68k" — full 8 chars)
//!                     for Human68k partitions.
//! 0x08..0x0C  start   BE u32 — first sector (lower 24 bits used; top
//!                     byte carries "active" / flags bits in some
//!                     refs, we mask to 24 bits for safety)
//! 0x0C..0x10  length  BE u32 — partition length in sectors
//! ```
//!
//! An entry with `name` all-zeros or all-space AND `length == 0` is
//! treated as "unused". Conventionally, partitions start at sector 64
//! (32,768 bytes with 512-B sectors).
//!
//! Reference: Aaru/DiscImageChef `Aaru.Partitions/Human68k.cs` (GPL-3,
//! parser logic), reproduced here from the on-disk spec. All multi-
//! byte fields are big-endian (M68k native).

use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};

use crate::error::RustyBackupError;

use super::PartitionSizeOverride;

/// `"X68K"` in big-endian u32.
pub const X68K_MAGIC: u32 = 0x5836_384B;

/// Byte offset of the X68k partition table on SCSI (`X68SCSI1`, 1024-byte
/// sectors -> sector 2) and synthetic 512-byte (sector 4) disks.
pub const X68K_TABLE_OFFSET: u64 = 2048;

/// Byte offset of the X68k partition table on SASI disks (256-byte sectors
/// -> sector 4). Real SASI game images (e.g. Bomberman) place it here.
pub const X68K_TABLE_OFFSET_SASI: u64 = 1024;

/// Number of partition slots in the table.
pub const X68K_MAX_PARTITIONS: usize = 8;

/// Size of one partition entry in the table.
pub const X68K_ENTRY_SIZE: usize = 16;

/// Size of the table header before the entries.
pub const X68K_TABLE_HEADER_SIZE: usize = 16;

/// Conventional sector-0 of the first user partition.
pub const X68K_FIRST_PARTITION_SECTOR: u32 = 64;

/// Sector size assumed when no Sharp HDD signature is present (e.g. the
/// synthetic 512-byte test images). Real disks are detected via
/// [`detect_sector_size`].
pub const X68K_DEFAULT_SECTOR_SIZE: u64 = 512;

/// Keisoku Giken SCSI HDD boot signature at byte 0 (`"X68SCSI1"`). These
/// disks use 1024-byte logical sectors. The Human68k FAT BPB lives at the
/// partition start with a big-endian Sharp/KG layout.
pub const X68K_SCSI_SIGNATURE: &[u8; 8] = b"X68SCSI1";

/// SASI HDD boot signature at byte 0 (`\x82w68000W`). These disks use
/// 256-byte logical sectors.
pub const X68K_SASI_SIGNATURE: &[u8; 8] = &[0x82, b'w', b'6', b'8', b'0', b'0', b'0', b'W'];

/// Derive the logical sector size of an X68000 HDD image from the Sharp
/// boot signature at byte 0.
///
/// - `X68SCSI1` (Keisoku Giken SCSI BIOS / SxSI / BlueSCSI) -> **1024 B**.
/// - `\x82w68000W` (SASI) -> **256 B**.
/// - anything else -> [`X68K_DEFAULT_SECTOR_SIZE`] (512), the synthetic
///   test convention.
///
/// The partition table's `start_sector` / `length_sectors` fields are in
/// these logical sectors, so the byte offset of a partition is
/// `start_sector * sector_size`. Getting this wrong points the filesystem
/// reader at a zero-filled gap and surfaces as
/// `Human68k BPB bytes_per_sector 0 not in {...}`.
pub fn detect_sector_size<R: Read + Seek>(reader: &mut R) -> u64 {
    let mut sig = [0u8; 8];
    if reader.seek(SeekFrom::Start(0)).is_err() || reader.read_exact(&mut sig).is_err() {
        return X68K_DEFAULT_SECTOR_SIZE;
    }
    if &sig == X68K_SCSI_SIGNATURE {
        1024
    } else if &sig == X68K_SASI_SIGNATURE {
        256
    } else {
        X68K_DEFAULT_SECTOR_SIZE
    }
}

/// One decoded partition entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct X68kEntry {
    /// 8-byte name field (Shift-JIS, space-padded). Held as raw bytes
    /// so the dispatch layer can route on the un-decoded name (most
    /// real disks have ASCII "Human   ").
    #[serde(with = "serde_bytes_array")]
    pub name_raw: [u8; 8],
    /// Lossy ASCII rendering of the name for display (Shift-JIS
    /// double-byte sequences surface as `?`). Trimmed of trailing
    /// spaces.
    pub name_display: String,
    /// Start sector (lower 24 bits of the on-disk u32; high byte is
    /// flags in some references — we ignore it).
    pub start_sector: u32,
    /// Length in sectors. 0 indicates an unused slot.
    pub length_sectors: u32,
}

mod serde_bytes_array {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8; 8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 8], D::Error> {
        let v: Vec<u8> = Deserialize::deserialize(d)?;
        if v.len() != 8 {
            return Err(serde::de::Error::custom(format!(
                "expected 8 bytes for X68k name_raw, got {}",
                v.len()
            )));
        }
        let mut out = [0u8; 8];
        out.copy_from_slice(&v);
        Ok(out)
    }
}

impl X68kEntry {
    /// Parse a single 16-byte entry. Returns `None` for "unused"
    /// entries (all-zero name AND length == 0).
    pub fn parse(buf: &[u8; X68K_ENTRY_SIZE]) -> Option<Self> {
        let mut name_raw = [0u8; 8];
        name_raw.copy_from_slice(&buf[0..8]);
        let start_raw = BigEndian::read_u32(&buf[8..12]);
        let length_sectors = BigEndian::read_u32(&buf[12..16]);
        if name_raw.iter().all(|b| *b == 0 || *b == b' ') && length_sectors == 0 {
            return None;
        }
        let name_display = String::from_utf8_lossy(&name_raw)
            .trim_end()
            .trim_end_matches('\0')
            .to_string();
        Some(X68kEntry {
            name_raw,
            name_display,
            start_sector: start_raw & 0x00FF_FFFF,
            length_sectors,
        })
    }

    /// True if the name field starts with "Human" (case-insensitive
    /// against the ASCII bytes). This is the canonical Human68k
    /// partition marker, used both as "Human   " and "Human68k".
    pub fn is_human68k(&self) -> bool {
        self.name_raw.starts_with(b"Human") || self.name_raw.starts_with(b"HUMAN")
    }
}

/// Parsed X68k partition table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct X68kPartitionTable {
    /// `size` field from the on-disk header. Reported raw for inspect.
    pub disk_size_field: u32,
    /// Active partition entries (unused slots filtered out).
    pub entries: Vec<X68kEntry>,
}

impl X68kPartitionTable {
    /// True if `head` (≥ 4 bytes) starts with the X68k magic.
    pub fn has_magic(head: &[u8]) -> bool {
        head.len() >= 4 && BigEndian::read_u32(&head[0..4]) == X68K_MAGIC
    }

    /// Read and parse the partition table from a seekable reader at the
    /// canonical SCSI/synthetic offset [`X68K_TABLE_OFFSET`] (byte 2048).
    pub fn detect<R: Read + Seek>(reader: &mut R) -> Result<Option<Self>, RustyBackupError> {
        Self::detect_at(reader, X68K_TABLE_OFFSET)
    }

    /// Read and parse the partition table from `table_offset`. Returns
    /// `None` (not an error) when the X68K magic isn't present there.
    pub fn detect_at<R: Read + Seek>(
        reader: &mut R,
        table_offset: u64,
    ) -> Result<Option<Self>, RustyBackupError> {
        reader
            .seek(SeekFrom::Start(table_offset))
            .map_err(RustyBackupError::Io)?;
        let mut buf = [0u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE];
        match reader.read_exact(&mut buf) {
            Ok(()) => {}
            Err(_) => return Ok(None),
        }
        if !Self::has_magic(&buf[..4]) {
            return Ok(None);
        }
        let disk_size_field = BigEndian::read_u32(&buf[4..8]);
        let mut entries = Vec::with_capacity(X68K_MAX_PARTITIONS);
        for i in 0..X68K_MAX_PARTITIONS {
            let off = X68K_TABLE_HEADER_SIZE + i * X68K_ENTRY_SIZE;
            let mut entry_buf = [0u8; X68K_ENTRY_SIZE];
            entry_buf.copy_from_slice(&buf[off..off + X68K_ENTRY_SIZE]);
            if let Some(e) = X68kEntry::parse(&entry_buf) {
                entries.push(e);
            }
        }
        Ok(Some(X68kPartitionTable {
            disk_size_field,
            entries,
        }))
    }

    /// Detect the table *and* the disk geometry (table byte offset +
    /// logical sector size), probing the two real Sharp conventions:
    ///
    /// | byte-0 signature | table offset | sector size |
    /// |------------------|--------------|-------------|
    /// | `X68SCSI1`       | 0x800        | 1024        |
    /// | `\x82w68000W`    | 0x400        | 256         |
    /// | (none)           | 0x800 or 0x400 | 512 / 256 |
    ///
    /// The table *location* is the reliable SASI marker: real SASI game
    /// disks (e.g. Bomberman) carry a custom IPL with no Sharp signature
    /// but place the `X68K` table at byte 0x400 with 256-byte sectors. The
    /// 0x800 / 512 combination is the synthetic-test convention.
    ///
    /// Returns `(table, table_offset, sector_size)`.
    pub fn detect_with_geometry<R: Read + Seek>(
        reader: &mut R,
    ) -> Result<Option<(Self, u64, u64)>, RustyBackupError> {
        let mut sig = [0u8; 8];
        reader
            .seek(SeekFrom::Start(0))
            .map_err(RustyBackupError::Io)?;
        let have_sig = reader.read_exact(&mut sig).is_ok();
        // (table_offset, sector_size) candidates in priority order.
        let candidates: &[(u64, u64)] = if have_sig && &sig == X68K_SCSI_SIGNATURE {
            &[(X68K_TABLE_OFFSET, 1024)]
        } else if have_sig && &sig == X68K_SASI_SIGNATURE {
            &[(X68K_TABLE_OFFSET_SASI, 256), (X68K_TABLE_OFFSET, 256)]
        } else {
            &[
                (X68K_TABLE_OFFSET, X68K_DEFAULT_SECTOR_SIZE),
                (X68K_TABLE_OFFSET_SASI, 256),
            ]
        };
        for &(off, sector_size) in candidates {
            if let Some(table) = Self::detect_at(reader, off)? {
                return Ok(Some((table, off, sector_size)));
            }
        }
        Ok(None)
    }

    /// Serialize the parsed table back into the canonical on-disk 144-byte
    /// block (16-byte header + 8 × 16-byte entries). Inactive slots are
    /// emitted as all-zeros; the live entries are placed in slot order.
    ///
    /// Used by restore-side reconstruction (we read the parsed
    /// `x68k.json` sidecar, optionally patch it with size overrides via
    /// [`patch_x68k_entries`], then write the resulting block to byte
    /// [`X68K_TABLE_OFFSET`] of the target image).
    pub fn to_bytes(&self) -> [u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE] {
        let mut buf = [0u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE];
        BigEndian::write_u32(&mut buf[0..4], X68K_MAGIC);
        BigEndian::write_u32(&mut buf[4..8], self.disk_size_field);
        BigEndian::write_u32(&mut buf[8..12], self.disk_size_field);
        // Bytes 12..16 (`unknown`) stay zero — matches the convention in
        // every MiSTer X68000 image we've seen.
        for (i, e) in self.entries.iter().take(X68K_MAX_PARTITIONS).enumerate() {
            let off = X68K_TABLE_HEADER_SIZE + i * X68K_ENTRY_SIZE;
            buf[off..off + 8].copy_from_slice(&e.name_raw);
            BigEndian::write_u32(&mut buf[off + 8..off + 12], e.start_sector & 0x00FF_FFFF);
            BigEndian::write_u32(&mut buf[off + 12..off + 16], e.length_sectors);
        }
        buf
    }
}

/// Patch an in-memory X68k table block (144 bytes, as produced by
/// [`X68kPartitionTable::to_bytes`]) with restore-time partition size
/// overrides. Mirrors the shape of
/// [`crate::partition::mbr::patch_mbr_entries`].
///
/// For each override we look up the active entry whose `start_sector`
/// matches `start_lba` and rewrite its `length` field (and `start_sector`
/// if `new_start_lba` is set). Overrides for entries that don't match
/// any live slot are silently ignored — same defensive shape as the
/// MBR helper.
pub fn patch_x68k_entries(
    buf: &mut [u8; X68K_TABLE_HEADER_SIZE + X68K_MAX_PARTITIONS * X68K_ENTRY_SIZE],
    overrides: &[PartitionSizeOverride],
) {
    for ps in overrides {
        if ps.index >= X68K_MAX_PARTITIONS {
            continue;
        }
        let entry_off = X68K_TABLE_HEADER_SIZE + ps.index * X68K_ENTRY_SIZE;
        let current_start = BigEndian::read_u32(&buf[entry_off + 8..entry_off + 12]) & 0x00FF_FFFF;
        if current_start as u64 != ps.start_lba {
            // Slot doesn't match — refuse to patch (same safety check
            // as patch_mbr_entries). This catches sidecar drift.
            continue;
        }
        let effective_start = ps.effective_start_lba() as u32 & 0x00FF_FFFF;
        let new_sectors = (ps.export_size / X68K_DEFAULT_SECTOR_SIZE) as u32;
        if ps.new_start_lba.is_some() {
            BigEndian::write_u32(&mut buf[entry_off + 8..entry_off + 12], effective_start);
        }
        BigEndian::write_u32(&mut buf[entry_off + 12..entry_off + 16], new_sectors);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn build_image_with_x68k_table(entries: &[(&[u8; 8], u32, u32)]) -> Vec<u8> {
        // Headroom past the table so the dispatch layer can be tested
        // against a "data area".
        let total_size = (X68K_FIRST_PARTITION_SECTOR as u64 + 16) * X68K_DEFAULT_SECTOR_SIZE;
        let mut buf = vec![0u8; total_size as usize];
        let off = X68K_TABLE_OFFSET as usize;
        // Magic + size + size2 + unknown.
        BigEndian::write_u32(&mut buf[off..off + 4], X68K_MAGIC);
        BigEndian::write_u32(&mut buf[off + 4..off + 8], 0x01_FF_FF_FF);
        BigEndian::write_u32(&mut buf[off + 8..off + 12], 0x01_FF_FF_FF);
        BigEndian::write_u32(&mut buf[off + 12..off + 16], 0);
        for (i, (name, start, length)) in entries.iter().enumerate() {
            let e_off = off + X68K_TABLE_HEADER_SIZE + i * X68K_ENTRY_SIZE;
            buf[e_off..e_off + 8].copy_from_slice(*name);
            BigEndian::write_u32(&mut buf[e_off + 8..e_off + 12], *start);
            BigEndian::write_u32(&mut buf[e_off + 12..e_off + 16], *length);
        }
        buf
    }

    /// Build an image with the X68K table at an arbitrary byte offset and
    /// an optional 8-byte boot signature at byte 0.
    fn build_image_with_table_at(
        table_off: usize,
        signature: Option<&[u8; 8]>,
        entries: &[(&[u8; 8], u32, u32)],
    ) -> Vec<u8> {
        let mut buf = vec![0u8; table_off + 4096];
        if let Some(sig) = signature {
            buf[0..8].copy_from_slice(sig);
        }
        BigEndian::write_u32(&mut buf[table_off..table_off + 4], X68K_MAGIC);
        BigEndian::write_u32(&mut buf[table_off + 4..table_off + 8], 0x01_FF_FF_FF);
        BigEndian::write_u32(&mut buf[table_off + 8..table_off + 12], 0x01_FF_FF_FF);
        for (i, (name, start, length)) in entries.iter().enumerate() {
            let e_off = table_off + X68K_TABLE_HEADER_SIZE + i * X68K_ENTRY_SIZE;
            buf[e_off..e_off + 8].copy_from_slice(*name);
            BigEndian::write_u32(&mut buf[e_off + 8..e_off + 12], *start);
            BigEndian::write_u32(&mut buf[e_off + 12..e_off + 16], *length);
        }
        buf
    }

    #[test]
    fn detect_returns_none_on_random_bytes() {
        let mut cur = Cursor::new(vec![0xCDu8; 4096]);
        assert!(X68kPartitionTable::detect(&mut cur).unwrap().is_none());
    }

    #[test]
    fn geometry_scsi_signature_is_1024_at_0x800() {
        let img = build_image_with_table_at(
            X68K_TABLE_OFFSET as usize,
            Some(X68K_SCSI_SIGNATURE),
            &[(b"Human68k", 32, 0x1000)],
        );
        let (_, off, ss) = X68kPartitionTable::detect_with_geometry(&mut Cursor::new(img))
            .unwrap()
            .unwrap();
        assert_eq!(off, X68K_TABLE_OFFSET);
        assert_eq!(ss, 1024);
    }

    #[test]
    fn geometry_no_signature_at_0x800_is_512() {
        let img = build_image_with_table_at(
            X68K_TABLE_OFFSET as usize,
            None,
            &[(b"Human68k", 64, 0x1000)],
        );
        let (_, off, ss) = X68kPartitionTable::detect_with_geometry(&mut Cursor::new(img))
            .unwrap()
            .unwrap();
        assert_eq!(off, X68K_TABLE_OFFSET);
        assert_eq!(ss, X68K_DEFAULT_SECTOR_SIZE);
    }

    #[test]
    fn geometry_sasi_table_at_0x400_is_256() {
        // Bomberman shape: custom IPL (no Sharp signature), table at 0x400.
        let img = build_image_with_table_at(
            X68K_TABLE_OFFSET_SASI as usize,
            None,
            &[(b"Human68k", 33, 40750)],
        );
        let (table, off, ss) = X68kPartitionTable::detect_with_geometry(&mut Cursor::new(img))
            .unwrap()
            .unwrap();
        assert_eq!(off, X68K_TABLE_OFFSET_SASI);
        assert_eq!(ss, 256);
        assert_eq!(table.entries[0].start_sector, 33);
        // The true partition offset is start_sector * sector_size = a
        // non-512-aligned byte that start_lba * 512 cannot express.
        assert_eq!(33u64 * ss, 8448);
        assert_ne!(8448 % 512, 0);
    }

    #[test]
    fn detect_sector_size_reads_sharp_signatures() {
        // SCSI (Keisoku Giken / SxSI / BlueSCSI) -> 1024-byte sectors.
        let mut scsi = vec![0u8; 4096];
        scsi[0..8].copy_from_slice(X68K_SCSI_SIGNATURE);
        assert_eq!(detect_sector_size(&mut Cursor::new(scsi)), 1024);

        // SASI -> 256-byte sectors.
        let mut sasi = vec![0u8; 4096];
        sasi[0..8].copy_from_slice(X68K_SASI_SIGNATURE);
        assert_eq!(detect_sector_size(&mut Cursor::new(sasi)), 256);

        // No recognized signature -> the 512-byte synthetic default.
        let plain = vec![0u8; 4096];
        assert_eq!(
            detect_sector_size(&mut Cursor::new(plain)),
            X68K_DEFAULT_SECTOR_SIZE
        );
    }

    #[test]
    fn detect_parses_single_human68k_partition() {
        let img = build_image_with_x68k_table(&[(b"Human   ", 64, 0x1000)]);
        let mut cur = Cursor::new(img);
        let table = X68kPartitionTable::detect(&mut cur).unwrap().unwrap();
        assert_eq!(table.entries.len(), 1);
        let e = &table.entries[0];
        assert_eq!(e.name_display, "Human");
        assert!(e.is_human68k());
        assert_eq!(e.start_sector, 64);
        assert_eq!(e.length_sectors, 0x1000);
    }

    #[test]
    fn detect_filters_unused_slots() {
        // Three entries; middle one is unused (zero name + length 0).
        let img = build_image_with_x68k_table(&[
            (b"Human   ", 64, 0x1000),
            (&[0u8; 8], 0, 0),
            (b"Human68k", 0x1040, 0x800),
        ]);
        let mut cur = Cursor::new(img);
        let table = X68kPartitionTable::detect(&mut cur).unwrap().unwrap();
        assert_eq!(table.entries.len(), 2);
        assert_eq!(table.entries[0].start_sector, 64);
        assert_eq!(table.entries[1].start_sector, 0x1040);
    }

    #[test]
    fn entry_parse_returns_none_for_unused_slot() {
        let mut buf = [0u8; X68K_ENTRY_SIZE];
        // All zeros, length 0 — definition of "unused".
        assert!(X68kEntry::parse(&buf).is_none());
        // Space-name + length 0 also counts as unused.
        for b in &mut buf[0..8] {
            *b = b' ';
        }
        assert!(X68kEntry::parse(&buf).is_none());
        // Non-zero length resurrects the entry.
        BigEndian::write_u32(&mut buf[12..16], 1);
        assert!(X68kEntry::parse(&buf).is_some());
    }

    #[test]
    fn start_sector_masks_high_byte() {
        let mut buf = [0u8; X68K_ENTRY_SIZE];
        buf[0..8].copy_from_slice(b"Human   ");
        // 0xFF in the high byte (some refs use it as "active" / flag).
        // The masked start_sector must be 64.
        BigEndian::write_u32(&mut buf[8..12], 0xFF00_0040);
        BigEndian::write_u32(&mut buf[12..16], 16);
        let e = X68kEntry::parse(&buf).unwrap();
        assert_eq!(e.start_sector, 64);
    }

    #[test]
    fn to_bytes_round_trips_through_detect() {
        let img =
            build_image_with_x68k_table(&[(b"Human   ", 64, 0x1000), (b"Human68k", 0x1040, 0x800)]);
        let mut cur = Cursor::new(img);
        let table = X68kPartitionTable::detect(&mut cur).unwrap().unwrap();
        let bytes = table.to_bytes();

        // Place the serialized block into a fresh image and re-detect.
        let total_size = (X68K_FIRST_PARTITION_SECTOR as u64 + 16) * X68K_DEFAULT_SECTOR_SIZE;
        let mut img2 = vec![0u8; total_size as usize];
        let off = X68K_TABLE_OFFSET as usize;
        img2[off..off + bytes.len()].copy_from_slice(&bytes);
        let mut cur2 = Cursor::new(img2);
        let table2 = X68kPartitionTable::detect(&mut cur2).unwrap().unwrap();
        assert_eq!(table2.entries.len(), 2);
        assert_eq!(table2.entries[0].start_sector, 64);
        assert_eq!(table2.entries[0].length_sectors, 0x1000);
        assert_eq!(&table2.entries[0].name_raw, b"Human   ");
        assert_eq!(table2.entries[1].start_sector, 0x1040);
        assert_eq!(table2.entries[1].length_sectors, 0x800);
    }

    #[test]
    fn to_bytes_emits_x68k_magic_at_offset_0() {
        let table = X68kPartitionTable {
            disk_size_field: 0x00FF_0000,
            entries: vec![],
        };
        let bytes = table.to_bytes();
        assert_eq!(BigEndian::read_u32(&bytes[0..4]), X68K_MAGIC);
        assert_eq!(BigEndian::read_u32(&bytes[4..8]), 0x00FF_0000);
        // Header.size2 mirrors size — consistent with on-disk images.
        assert_eq!(BigEndian::read_u32(&bytes[8..12]), 0x00FF_0000);
        // Header `unknown` and all 8 entry slots are zero.
        for &b in &bytes[12..] {
            assert_eq!(b, 0);
        }
    }

    #[test]
    fn patch_x68k_entries_resizes_matching_entry() {
        let table = X68kPartitionTable {
            disk_size_field: 0x4000,
            entries: vec![
                X68kEntry {
                    name_raw: *b"Human   ",
                    name_display: "Human".into(),
                    start_sector: 64,
                    length_sectors: 0x1000,
                },
                X68kEntry {
                    name_raw: *b"Human68k",
                    name_display: "Human68k".into(),
                    start_sector: 0x1040,
                    length_sectors: 0x800,
                },
            ],
        };
        let mut buf = table.to_bytes();
        let overrides = vec![
            PartitionSizeOverride::size_only(0, 64, 0x1000 * 512, 0x2000 * 512),
            PartitionSizeOverride::size_only(1, 0x1040, 0x800 * 512, 0x800 * 512),
        ];
        patch_x68k_entries(&mut buf, &overrides);

        // Slot 0 grew to 0x2000 sectors; slot 1 unchanged.
        let off0 = X68K_TABLE_HEADER_SIZE;
        assert_eq!(
            BigEndian::read_u32(&buf[off0 + 12..off0 + 16]),
            0x2000,
            "slot 0 length should be patched"
        );
        let off1 = X68K_TABLE_HEADER_SIZE + X68K_ENTRY_SIZE;
        assert_eq!(
            BigEndian::read_u32(&buf[off1 + 12..off1 + 16]),
            0x800,
            "slot 1 length should be unchanged"
        );
    }

    #[test]
    fn patch_x68k_entries_skips_when_start_lba_mismatches() {
        let table = X68kPartitionTable {
            disk_size_field: 0x4000,
            entries: vec![X68kEntry {
                name_raw: *b"Human   ",
                name_display: "Human".into(),
                start_sector: 64,
                length_sectors: 0x1000,
            }],
        };
        let mut buf = table.to_bytes();
        // Override claims start_lba = 999, but the live slot is at 64.
        let overrides = vec![PartitionSizeOverride::size_only(
            0,
            999,
            0x1000 * 512,
            0x2000 * 512,
        )];
        patch_x68k_entries(&mut buf, &overrides);

        // Slot 0 length stays at 0x1000 (refused).
        let off0 = X68K_TABLE_HEADER_SIZE;
        assert_eq!(
            BigEndian::read_u32(&buf[off0 + 12..off0 + 16]),
            0x1000,
            "slot 0 should be untouched on start_lba mismatch"
        );
    }

    #[test]
    fn patch_x68k_entries_honors_new_start_lba() {
        let table = X68kPartitionTable {
            disk_size_field: 0x4000,
            entries: vec![X68kEntry {
                name_raw: *b"Human   ",
                name_display: "Human".into(),
                start_sector: 64,
                length_sectors: 0x1000,
            }],
        };
        let mut buf = table.to_bytes();
        let overrides = vec![PartitionSizeOverride {
            index: 0,
            start_lba: 64,
            original_size: 0x1000 * 512,
            export_size: 0x1000 * 512,
            new_start_lba: Some(128),
            heads: 0,
            sectors_per_track: 0,
        }];
        patch_x68k_entries(&mut buf, &overrides);

        let off0 = X68K_TABLE_HEADER_SIZE;
        assert_eq!(
            BigEndian::read_u32(&buf[off0 + 8..off0 + 12]) & 0x00FF_FFFF,
            128,
            "slot 0 start_sector should be rewritten"
        );
    }

    #[test]
    fn entry_round_trips_through_json() {
        let entry = X68kEntry {
            name_raw: *b"Human68k",
            name_display: "Human68k".into(),
            start_sector: 64,
            length_sectors: 0x1234,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let entry2: X68kEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry.name_raw, entry2.name_raw);
        assert_eq!(entry.name_display, entry2.name_display);
        assert_eq!(entry.start_sector, entry2.start_sector);
        assert_eq!(entry.length_sectors, entry2.length_sectors);
    }

    #[test]
    fn table_round_trips_through_json() {
        let table = X68kPartitionTable {
            disk_size_field: 0x00FF_0000,
            entries: vec![X68kEntry {
                name_raw: *b"Human   ",
                name_display: "Human".into(),
                start_sector: 64,
                length_sectors: 0x1000,
            }],
        };
        let json = serde_json::to_string(&table).unwrap();
        let table2: X68kPartitionTable = serde_json::from_str(&json).unwrap();
        assert_eq!(table.disk_size_field, table2.disk_size_field);
        assert_eq!(table.entries.len(), table2.entries.len());
        // Bytes round-trip the same.
        assert_eq!(table.to_bytes(), table2.to_bytes());
    }
}
