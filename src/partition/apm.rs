use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};

use crate::error::RustyBackupError;
use crate::partition::PartitionSizeOverride;

const DDR_SIGNATURE: u16 = 0x4552;
const APM_ENTRY_SIGNATURE: u16 = 0x504D;

/// Driver Descriptor Record — block 0 of an APM disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverDescriptorRecord {
    pub signature: u16,
    pub block_size: u16,
    pub block_count: u32,
    /// `sbDevType` at offset 8.
    #[serde(default)]
    pub dev_type: u16,
    /// `sbDevId` at offset 10.
    #[serde(default)]
    pub dev_id: u16,
    /// `sbData` at offset 12.
    #[serde(default)]
    pub sb_data: u32,
    pub driver_count: u16,
    /// `drDriverInfo` array starting at offset 18 — one entry per driver.
    /// Mac ROMs use these to load SCSI drivers at boot, so they must be
    /// preserved verbatim when rebuilding a disk.
    #[serde(default)]
    pub driver_info: Vec<DriverInfo>,
}

/// One entry in the DDR's `drDriverInfo` array (8 bytes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverInfo {
    /// `ddBlock` — first block of the driver code on the disk.
    pub block: u32,
    /// `ddSize` — driver size in 512-byte blocks.
    pub size: u16,
    /// `ddType` — driver type (1 = Mac OS, etc.).
    pub kind: u16,
}

/// A single Apple Partition Map entry (one per block, starting at block 1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApmPartitionEntry {
    pub signature: u16,
    pub map_entries: u32,
    pub start_block: u32,
    pub block_count: u32,
    pub name: String,
    pub partition_type: String,
    pub data_start: u32,
    pub data_count: u32,
    pub status: u32,
    pub boot_start: u32,
    pub boot_size: u32,
    pub boot_load: u64,
    pub boot_entry: u64,
    pub boot_checksum: u32,
    pub processor: String,
    /// Bytes 136..512 of the entry (`pmPad`). Apple's HD SC and similar
    /// formatters stash a driver-descriptor table here for `Apple_Driver43`
    /// entries; without it the ROM cannot load the SCSI driver and the
    /// emulator/host doesn't even see the disk. Preserved verbatim from the
    /// source on parse, written back unmodified by `to_bytes`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pad: Vec<u8>,
}

impl ApmPartitionEntry {
    /// Parse a single APM entry from a 512-byte block.
    fn parse(data: &[u8; 512]) -> Result<Self, RustyBackupError> {
        let sig = BigEndian::read_u16(&data[0..2]);
        if sig != APM_ENTRY_SIGNATURE {
            return Err(RustyBackupError::InvalidApm(format!(
                "bad partition entry signature: 0x{sig:04X}"
            )));
        }

        Ok(ApmPartitionEntry {
            signature: sig,
            map_entries: BigEndian::read_u32(&data[4..8]),
            start_block: BigEndian::read_u32(&data[8..12]),
            block_count: BigEndian::read_u32(&data[12..16]),
            name: parse_c_string(&data[16..48]),
            partition_type: parse_c_string(&data[48..80]),
            data_start: BigEndian::read_u32(&data[80..84]),
            data_count: BigEndian::read_u32(&data[84..88]),
            status: BigEndian::read_u32(&data[88..92]),
            boot_start: BigEndian::read_u32(&data[92..96]),
            boot_size: BigEndian::read_u32(&data[96..100]),
            boot_load: BigEndian::read_u64(&data[100..108]),
            boot_entry: BigEndian::read_u64(&data[108..116]),
            boot_checksum: BigEndian::read_u32(&data[116..120]),
            processor: parse_c_string(&data[120..136]),
            pad: data[136..512].to_vec(),
        })
    }

    /// Serialize this entry into a 512-byte block.
    fn to_bytes(&self) -> [u8; 512] {
        let mut buf = [0u8; 512];
        BigEndian::write_u16(&mut buf[0..2], APM_ENTRY_SIGNATURE);
        // reserved u16 at 2..4
        BigEndian::write_u32(&mut buf[4..8], self.map_entries);
        BigEndian::write_u32(&mut buf[8..12], self.start_block);
        BigEndian::write_u32(&mut buf[12..16], self.block_count);
        write_c_string(&mut buf[16..48], &self.name);
        write_c_string(&mut buf[48..80], &self.partition_type);
        BigEndian::write_u32(&mut buf[80..84], self.data_start);
        BigEndian::write_u32(&mut buf[84..88], self.data_count);
        BigEndian::write_u32(&mut buf[88..92], self.status);
        BigEndian::write_u32(&mut buf[92..96], self.boot_start);
        BigEndian::write_u32(&mut buf[96..100], self.boot_size);
        BigEndian::write_u64(&mut buf[100..108], self.boot_load);
        BigEndian::write_u64(&mut buf[108..116], self.boot_entry);
        BigEndian::write_u32(&mut buf[116..120], self.boot_checksum);
        write_c_string(&mut buf[120..136], &self.processor);
        // Preserve `pmPad` verbatim from source. Apple_Driver43 entries
        // carry driver-descriptor metadata here; zeroing it makes the ROM
        // skip the disk entirely.
        let pad_len = self.pad.len().min(512 - 136);
        buf[136..136 + pad_len].copy_from_slice(&self.pad[..pad_len]);
        buf
    }

    /// True if this is a "data" partition (not the partition map itself, not free space, not drivers).
    pub fn is_data_partition(&self) -> bool {
        let t = self.partition_type.as_str();
        !matches!(
            t,
            "Apple_partition_map"
                | "Apple_Free"
                | "Apple_Driver"
                | "Apple_Driver43"
                | "Apple_Driver43_CD"
                | "Apple_Driver_ATA"
                | "Apple_Driver_ATAPI"
                // Drive Setup reserves this on every ATA disk it formats and
                // leaves it zeroed; without it here it lists as a data partition.
                | "Apple_Driver_IOKit"
                | "Apple_Patches"
                | "Apple_FWDriver"
                | "Apple_Void"
        )
    }

    /// True if the bootable status flag (bit 3) is set.
    pub fn is_bootable(&self) -> bool {
        self.status & 0x08 != 0
    }

    /// Size in bytes using the DDR block size.
    pub fn size_bytes(&self, block_size: u16) -> u64 {
        self.block_count as u64 * block_size as u64
    }
}

/// Top-level Apple Partition Map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Apm {
    pub ddr: DriverDescriptorRecord,
    pub entries: Vec<ApmPartitionEntry>,
    pub map_entry_count: u32,
    /// `false` when block 0 (the Driver Descriptor Record) was absent or all
    /// zeros and the partition map was located by probing for the `PM`
    /// signature — a very common mastering pattern on classic-Mac CD-ROMs,
    /// which need no SCSI driver block. `true` for a normal APM with an intact
    /// `ER` DDR. Defaults to `true` so persisted-JSON round-trips predate the
    /// field.
    #[serde(default = "default_true")]
    pub ddr_present: bool,
}

fn default_true() -> bool {
    true
}

impl Apm {
    /// Parse an APM from a readable+seekable source.
    /// The reader should be positioned at the start of the disk.
    pub fn parse(
        reader: &mut (impl std::io::Read + std::io::Seek),
    ) -> Result<Self, RustyBackupError> {
        use std::io::SeekFrom;

        // Read DDR (block 0)
        reader
            .seek(SeekFrom::Start(0))
            .map_err(RustyBackupError::Io)?;
        let mut ddr_buf = [0u8; 512];
        reader
            .read_exact(&mut ddr_buf)
            .map_err(|e| RustyBackupError::InvalidApm(format!("cannot read DDR: {e}")))?;

        let sig = BigEndian::read_u16(&ddr_buf[0..2]);
        if sig != DDR_SIGNATURE {
            return Err(RustyBackupError::InvalidApm(format!(
                "bad DDR signature: 0x{sig:04X}"
            )));
        }

        let driver_count = BigEndian::read_u16(&ddr_buf[16..18]);
        let mut driver_info = Vec::with_capacity(driver_count as usize);
        for i in 0..driver_count as usize {
            let off = 18 + i * 8;
            if off + 8 > ddr_buf.len() {
                break;
            }
            driver_info.push(DriverInfo {
                block: BigEndian::read_u32(&ddr_buf[off..off + 4]),
                size: BigEndian::read_u16(&ddr_buf[off + 4..off + 6]),
                kind: BigEndian::read_u16(&ddr_buf[off + 6..off + 8]),
            });
        }
        let ddr = DriverDescriptorRecord {
            signature: sig,
            block_size: BigEndian::read_u16(&ddr_buf[2..4]),
            block_count: BigEndian::read_u32(&ddr_buf[4..8]),
            dev_type: BigEndian::read_u16(&ddr_buf[8..10]),
            dev_id: BigEndian::read_u16(&ddr_buf[10..12]),
            sb_data: BigEndian::read_u32(&ddr_buf[12..16]),
            driver_count,
            driver_info,
        };

        // Read first partition entry to get map_entries count
        let mut entry_buf = [0u8; 512];
        reader.read_exact(&mut entry_buf).map_err(|e| {
            RustyBackupError::InvalidApm(format!("cannot read first APM entry: {e}"))
        })?;
        let first_entry = ApmPartitionEntry::parse(&entry_buf)?;
        let map_entry_count = first_entry.map_entries;

        if map_entry_count == 0 || map_entry_count > 128 {
            return Err(RustyBackupError::InvalidApm(format!(
                "invalid map entry count: {map_entry_count}"
            )));
        }

        let mut entries = vec![first_entry];

        // Read remaining entries
        for i in 1..map_entry_count {
            reader
                .seek(SeekFrom::Start((1 + i as u64) * 512))
                .map_err(RustyBackupError::Io)?;
            reader.read_exact(&mut entry_buf).map_err(|e| {
                RustyBackupError::InvalidApm(format!("cannot read APM entry {}: {e}", i + 1))
            })?;
            entries.push(ApmPartitionEntry::parse(&entry_buf)?);
        }

        Ok(Apm {
            ddr,
            entries,
            map_entry_count,
            ddr_present: true,
        })
    }

    /// Parse an APM whose block 0 (Driver Descriptor Record) is absent or all
    /// zeros — the common classic-Mac CD-ROM pattern. The caller has already
    /// confirmed a `PM` partition-map entry at byte offset `block_size` (i.e.
    /// block 1); this reads the map at that block stride and synthesizes a DDR
    /// carrying the detected block size so downstream byte math
    /// (`start_block * block_size / 512`, `size_bytes`) still works. The
    /// resulting [`Apm`] has `ddr_present == false`.
    pub fn parse_no_ddr(
        reader: &mut (impl std::io::Read + std::io::Seek),
        block_size: u32,
    ) -> Result<Self, RustyBackupError> {
        use std::io::SeekFrom;

        if !matches!(block_size, 512 | 1024 | 2048) {
            return Err(RustyBackupError::InvalidApm(format!(
                "unsupported APM block size {block_size}"
            )));
        }

        let mut entry_buf = [0u8; 512];
        // The first map entry lives at block 1 = byte offset `block_size`.
        reader
            .seek(SeekFrom::Start(block_size as u64))
            .map_err(RustyBackupError::Io)?;
        reader.read_exact(&mut entry_buf).map_err(|e| {
            RustyBackupError::InvalidApm(format!("cannot read first APM entry: {e}"))
        })?;
        let first_entry = ApmPartitionEntry::parse(&entry_buf)?;
        let map_entry_count = first_entry.map_entries;
        if map_entry_count == 0 || map_entry_count > 128 {
            return Err(RustyBackupError::InvalidApm(format!(
                "invalid map entry count: {map_entry_count}"
            )));
        }

        let mut entries = vec![first_entry];
        for i in 1..map_entry_count {
            reader
                .seek(SeekFrom::Start((1 + i as u64) * block_size as u64))
                .map_err(RustyBackupError::Io)?;
            reader.read_exact(&mut entry_buf).map_err(|e| {
                RustyBackupError::InvalidApm(format!("cannot read APM entry {}: {e}", i + 1))
            })?;
            entries.push(ApmPartitionEntry::parse(&entry_buf)?);
        }

        // No real DDR exists; synthesize one carrying the detected block size
        // and a block count derived from the map's extent.
        let block_count = entries
            .iter()
            .map(|e| e.start_block as u64 + e.block_count as u64)
            .max()
            .unwrap_or(0)
            .min(u32::MAX as u64) as u32;
        let ddr = DriverDescriptorRecord {
            signature: 0,
            block_size: block_size as u16,
            block_count,
            dev_type: 0,
            dev_id: 0,
            sb_data: 0,
            driver_count: 0,
            driver_info: Vec::new(),
        };

        Ok(Apm {
            ddr,
            entries,
            map_entry_count,
            ddr_present: false,
        })
    }

    /// Build the raw bytes for DDR + all partition map entries.
    /// Returns `(1 + map_entry_count) * 512` bytes.
    pub fn build_apm_blocks(&self, target_block_count: Option<u32>) -> Vec<u8> {
        let count = self.entries.len();
        let total_blocks = 1 + count; // DDR + entries
        let mut buf = vec![0u8; total_blocks * 512];

        // Write DDR
        BigEndian::write_u16(&mut buf[0..2], DDR_SIGNATURE);
        BigEndian::write_u16(&mut buf[2..4], self.ddr.block_size);
        BigEndian::write_u32(
            &mut buf[4..8],
            target_block_count.unwrap_or(self.ddr.block_count),
        );
        BigEndian::write_u16(&mut buf[8..10], self.ddr.dev_type);
        BigEndian::write_u16(&mut buf[10..12], self.ddr.dev_id);
        BigEndian::write_u32(&mut buf[12..16], self.ddr.sb_data);
        BigEndian::write_u16(&mut buf[16..18], self.ddr.driver_count);
        for (i, di) in self.ddr.driver_info.iter().enumerate() {
            let off = 18 + i * 8;
            if off + 8 > 512 {
                break;
            }
            BigEndian::write_u32(&mut buf[off..off + 4], di.block);
            BigEndian::write_u16(&mut buf[off + 4..off + 6], di.size);
            BigEndian::write_u16(&mut buf[off + 6..off + 8], di.kind);
        }

        // Write each partition entry
        for (i, entry) in self.entries.iter().enumerate() {
            let offset = (1 + i) * 512;
            let entry_bytes = entry.to_bytes();
            buf[offset..offset + 512].copy_from_slice(&entry_bytes);
        }

        buf
    }

    /// Clone this APM with partition sizes adjusted according to overrides.
    /// Matches partitions by `start_block`. Updates `block_count` and `data_count`.
    pub fn patch_for_restore(
        &self,
        overrides: &[PartitionSizeOverride],
        target_block_count: u32,
    ) -> Apm {
        let block_size = self.ddr.block_size as u64;
        let mut patched = self.clone();
        patched.ddr.block_count = target_block_count;

        for entry in &mut patched.entries {
            // Match override by start_block (converted to LBA via block_size/512)
            let entry_lba = entry.start_block as u64 * block_size / 512;
            if let Some(ov) = overrides.iter().find(|o| o.start_lba == entry_lba) {
                let new_blocks = (ov.export_size / block_size) as u32;
                entry.block_count = new_blocks;
                entry.data_count = new_blocks;

                if let Some(new_lba) = ov.new_start_lba {
                    entry.start_block = (new_lba * 512 / block_size) as u32;
                    entry.data_start = 0; // data starts at beginning of partition
                }
            }
        }

        patched
    }
}

/// Parse a null-terminated C string from a fixed-size buffer.
fn parse_c_string(data: &[u8]) -> String {
    let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
    String::from_utf8_lossy(&data[..end]).into_owned()
}

/// Write a string into a fixed-size buffer, null-terminated.
fn write_c_string(buf: &mut [u8], s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len().min(buf.len() - 1);
    buf[..len].copy_from_slice(&bytes[..len]);
    // Rest is already zero from initialization
}

/// Build a minimal APM from scratch with the given partition entries.
///
/// Each entry is `(type_string, start_block, block_count)`.
/// Returns an `Apm` with DDR + self-referencing partition map entry + user entries.
pub fn build_minimal_apm(
    entries: &[(String, u32, u32)],
    block_size: u32,
    total_blocks: u32,
) -> Apm {
    // Total map entries = 1 (self-referencing) + user entries
    let map_count = 1 + entries.len() as u32;

    // Self-referencing partition map entry (covers blocks 1..map_count)
    let map_entry = ApmPartitionEntry {
        signature: APM_ENTRY_SIGNATURE,
        map_entries: map_count,
        start_block: 1,
        block_count: map_count,
        name: "Apple".to_string(),
        partition_type: "Apple_partition_map".to_string(),
        data_start: 0,
        data_count: map_count,
        status: 0x03, // valid + allocated
        boot_start: 0,
        boot_size: 0,
        boot_load: 0,
        boot_entry: 0,
        boot_checksum: 0,
        processor: String::new(),
        pad: Vec::new(),
    };

    let mut apm_entries = vec![map_entry];

    for (i, (type_string, start_block, block_count)) in entries.iter().enumerate() {
        apm_entries.push(ApmPartitionEntry {
            signature: APM_ENTRY_SIGNATURE,
            map_entries: map_count,
            start_block: *start_block,
            block_count: *block_count,
            name: format!("Partition {}", i + 1),
            partition_type: type_string.clone(),
            data_start: 0,
            data_count: *block_count,
            status: 0x33, // valid + allocated + readable + writable
            boot_start: 0,
            boot_size: 0,
            boot_load: 0,
            boot_entry: 0,
            boot_checksum: 0,
            processor: String::new(),
            pad: Vec::new(),
        });
    }

    Apm {
        ddr: DriverDescriptorRecord {
            signature: DDR_SIGNATURE,
            block_size: block_size as u16,
            block_count: total_blocks,
            dev_type: 0,
            dev_id: 0,
            sb_data: 0,
            driver_count: 0,
            driver_info: Vec::new(),
        },
        entries: apm_entries,
        map_entry_count: map_count,
        ddr_present: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a synthetic APM disk with DDR + N partition entries.
    fn build_synthetic_apm(entries: &[(&str, &str, u32, u32)]) -> Vec<u8> {
        let total_blocks = 1 + entries.len();
        let mut data = vec![0u8; total_blocks * 512];

        // DDR
        BigEndian::write_u16(&mut data[0..2], DDR_SIGNATURE);
        BigEndian::write_u16(&mut data[2..4], 512); // block_size
        BigEndian::write_u32(&mut data[4..8], 100_000); // block_count

        // Entries
        for (i, (name, ptype, start, count)) in entries.iter().enumerate() {
            let offset = (1 + i) * 512;
            BigEndian::write_u16(&mut data[offset..offset + 2], APM_ENTRY_SIGNATURE);
            BigEndian::write_u32(&mut data[offset + 4..offset + 8], entries.len() as u32);
            BigEndian::write_u32(&mut data[offset + 8..offset + 12], *start);
            BigEndian::write_u32(&mut data[offset + 12..offset + 16], *count);
            write_c_string(&mut data[offset + 16..offset + 48], name);
            write_c_string(&mut data[offset + 48..offset + 80], ptype);
            BigEndian::write_u32(&mut data[offset + 80..offset + 84], 0); // data_start
            BigEndian::write_u32(&mut data[offset + 84..offset + 88], *count); // data_count
            BigEndian::write_u32(&mut data[offset + 88..offset + 92], 0x33); // status (valid + allocated + bootable)
        }

        data
    }

    #[test]
    fn test_parse_synthetic_apm() {
        let data = build_synthetic_apm(&[
            ("Apple", "Apple_partition_map", 1, 3),
            ("MacOS", "Apple_HFS", 64, 50000),
            ("Untitled", "Apple_Free", 50064, 49936),
        ]);

        let mut cursor = Cursor::new(data);
        let apm = Apm::parse(&mut cursor).unwrap();

        assert_eq!(apm.ddr.signature, DDR_SIGNATURE);
        assert_eq!(apm.ddr.block_size, 512);
        assert_eq!(apm.map_entry_count, 3);
        assert_eq!(apm.entries.len(), 3);
        assert_eq!(apm.entries[0].partition_type, "Apple_partition_map");
        assert_eq!(apm.entries[1].partition_type, "Apple_HFS");
        assert_eq!(apm.entries[1].start_block, 64);
        assert_eq!(apm.entries[1].block_count, 50000);
        assert_eq!(apm.entries[2].partition_type, "Apple_Free");
    }

    /// Build an APM whose block 0 (DDR) is all zeros and whose `PM` entries
    /// live at `block_size` stride — the classic-Mac CD-ROM mastering pattern.
    fn build_zeroed_ddr_apm(entries: &[(&str, &str, u32, u32)], block_size: usize) -> Vec<u8> {
        let total_blocks = 1 + entries.len();
        let mut data = vec![0u8; total_blocks * block_size];
        // Block 0 stays all zeros — no Driver Descriptor Record.
        for (i, (name, ptype, start, count)) in entries.iter().enumerate() {
            let offset = (1 + i) * block_size;
            BigEndian::write_u16(&mut data[offset..offset + 2], APM_ENTRY_SIGNATURE);
            BigEndian::write_u32(&mut data[offset + 4..offset + 8], entries.len() as u32);
            BigEndian::write_u32(&mut data[offset + 8..offset + 12], *start);
            BigEndian::write_u32(&mut data[offset + 12..offset + 16], *count);
            write_c_string(&mut data[offset + 16..offset + 48], name);
            write_c_string(&mut data[offset + 48..offset + 80], ptype);
            BigEndian::write_u32(&mut data[offset + 84..offset + 88], *count); // data_count
            BigEndian::write_u32(&mut data[offset + 88..offset + 92], 0x33); // status
        }
        data
    }

    #[test]
    fn test_parse_no_ddr_at_1024() {
        // The ~70-disc bucket: zeroed block 0, PM map at byte 1024.
        let data = build_zeroed_ddr_apm(
            &[
                ("Apple", "Apple_partition_map", 1, 3),
                ("MacOS", "Apple_HFS", 64, 50000),
                ("Extra", "Apple_Free", 50064, 49936),
            ],
            1024,
        );
        let mut cursor = Cursor::new(data);
        let apm = Apm::parse_no_ddr(&mut cursor, 1024).unwrap();
        assert!(
            !apm.ddr_present,
            "zeroed-DDR parse must report ddr_present=false"
        );
        assert_eq!(apm.ddr.block_size, 1024);
        assert_eq!(apm.map_entry_count, 3);
        assert_eq!(apm.entries.len(), 3);
        assert_eq!(apm.entries[1].partition_type, "Apple_HFS");
        assert_eq!(apm.entries[1].start_block, 64);
        assert_eq!(apm.entries[1].block_count, 50000);
    }

    #[test]
    fn test_ddr_present_true_on_normal_parse() {
        let data = build_synthetic_apm(&[
            ("Apple", "Apple_partition_map", 1, 2),
            ("MacOS", "Apple_HFS", 64, 50000),
        ]);
        let apm = Apm::parse(&mut Cursor::new(data)).unwrap();
        assert!(
            apm.ddr_present,
            "an intact ER DDR must report ddr_present=true"
        );
    }

    #[test]
    fn test_ddr_present_defaults_true_on_legacy_json() {
        // Persisted APM JSON from before the field existed must deserialize
        // with ddr_present=true.
        let json = r#"{"ddr":{"signature":17746,"block_size":512,"block_count":100000,
            "driver_count":0},"entries":[],"map_entry_count":0}"#;
        let apm: Apm = serde_json::from_str(json).unwrap();
        assert!(apm.ddr_present);
    }

    #[test]
    fn test_data_partition_filter() {
        let data = build_synthetic_apm(&[
            ("Apple", "Apple_partition_map", 1, 3),
            ("Macintosh HD", "Apple_HFS", 64, 50000),
            ("", "Apple_Free", 50064, 49936),
        ]);

        let mut cursor = Cursor::new(data);
        let apm = Apm::parse(&mut cursor).unwrap();

        let data_parts: Vec<_> = apm
            .entries
            .iter()
            .filter(|e| e.is_data_partition())
            .collect();
        assert_eq!(data_parts.len(), 1);
        assert_eq!(data_parts[0].name, "Macintosh HD");
    }

    #[test]
    fn test_json_round_trip() {
        let data = build_synthetic_apm(&[
            ("Apple", "Apple_partition_map", 1, 3),
            ("MacOS", "Apple_HFS", 64, 50000),
        ]);

        let mut cursor = Cursor::new(data);
        let apm = Apm::parse(&mut cursor).unwrap();

        let json = serde_json::to_string_pretty(&apm).unwrap();
        let parsed: Apm = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.ddr.block_size, 512);
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[1].start_block, 64);
    }

    #[test]
    fn test_build_apm_blocks_round_trip() {
        let data = build_synthetic_apm(&[
            ("Apple", "Apple_partition_map", 1, 3),
            ("MacOS", "Apple_HFS", 64, 50000),
        ]);

        let mut cursor = Cursor::new(data);
        let apm = Apm::parse(&mut cursor).unwrap();

        let rebuilt = apm.build_apm_blocks(None);
        let mut cursor2 = Cursor::new(rebuilt);
        let apm2 = Apm::parse(&mut cursor2).unwrap();

        assert_eq!(apm2.entries.len(), 2);
        assert_eq!(apm2.entries[1].start_block, 64);
        assert_eq!(apm2.entries[1].block_count, 50000);
    }

    #[test]
    fn test_patch_for_restore() {
        let data = build_synthetic_apm(&[
            ("Apple", "Apple_partition_map", 1, 3),
            ("MacOS", "Apple_HFS", 64, 50000),
        ]);

        let mut cursor = Cursor::new(data);
        let apm = Apm::parse(&mut cursor).unwrap();

        let overrides = vec![PartitionSizeOverride::size_only(
            0,
            64, // start_lba (same as start_block since block_size=512)
            50000 * 512,
            60000 * 512,
        )];

        let patched = apm.patch_for_restore(&overrides, 120000);
        assert_eq!(patched.ddr.block_count, 120000);
        assert_eq!(patched.entries[1].block_count, 60000);
        assert_eq!(patched.entries[1].data_count, 60000);
    }

    #[test]
    fn test_build_minimal_apm_roundtrip() {
        let apm = build_minimal_apm(&[("Apple_HFS".to_string(), 64, 100000)], 512, 200000);

        // Serialize to bytes
        let bytes = apm.build_apm_blocks(Some(200000));

        // Parse back
        let mut cursor = std::io::Cursor::new(bytes);
        let parsed = Apm::parse(&mut cursor).unwrap();

        assert_eq!(parsed.ddr.block_size, 512);
        assert_eq!(parsed.ddr.block_count, 200000);
        // Should have 2 entries: partition map + user partition
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[0].partition_type, "Apple_partition_map");
        assert_eq!(parsed.entries[1].partition_type, "Apple_HFS");
        assert_eq!(parsed.entries[1].start_block, 64);
        assert_eq!(parsed.entries[1].block_count, 100000);
    }
}
