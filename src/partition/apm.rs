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
    pub driver_count: u16,
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
}

impl Apm {
    /// Parse an APM from a readable+seekable source.
    /// The reader should be positioned at the start of the disk.
    pub fn parse(reader: &mut (impl std::io::Read + std::io::Seek)) -> Result<Self, RustyBackupError> {
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

        let ddr = DriverDescriptorRecord {
            signature: sig,
            block_size: BigEndian::read_u16(&ddr_buf[2..4]),
            block_count: BigEndian::read_u32(&ddr_buf[4..8]),
            driver_count: BigEndian::read_u16(&ddr_buf[16..18]),
        };

        // Read first partition entry to get map_entries count
        let mut entry_buf = [0u8; 512];
        reader
            .read_exact(&mut entry_buf)
            .map_err(|e| RustyBackupError::InvalidApm(format!("cannot read first APM entry: {e}")))?;
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
        BigEndian::write_u16(&mut buf[16..18], self.ddr.driver_count);

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

    #[test]
    fn test_data_partition_filter() {
        let data = build_synthetic_apm(&[
            ("Apple", "Apple_partition_map", 1, 3),
            ("Macintosh HD", "Apple_HFS", 64, 50000),
            ("", "Apple_Free", 50064, 49936),
        ]);

        let mut cursor = Cursor::new(data);
        let apm = Apm::parse(&mut cursor).unwrap();

        let data_parts: Vec<_> = apm.entries.iter().filter(|e| e.is_data_partition()).collect();
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
            64,     // start_lba (same as start_block since block_size=512)
            50000 * 512,
            60000 * 512,
        )];

        let patched = apm.patch_for_restore(&overrides, 120000);
        assert_eq!(patched.ddr.block_count, 120000);
        assert_eq!(patched.entries[1].block_count, 60000);
        assert_eq!(patched.entries[1].data_count, 60000);
    }
}
