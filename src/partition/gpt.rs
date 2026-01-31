use serde::Serialize;
use std::io::{Read, Seek, SeekFrom};

use crate::error::RustyBackupError;

const GPT_SIGNATURE: u64 = 0x5452415020494645; // "EFI PART"
const GPT_HEADER_LBA: u64 = 1;
const SECTOR_SIZE: u64 = 512;

/// A 128-bit GUID stored as raw bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Guid([u8; 16]);

impl Guid {
    pub fn is_zero(&self) -> bool {
        self.0 == [0u8; 16]
    }

    /// Format as standard GUID string (mixed-endian per UEFI spec).
    pub fn to_string_formatted(&self) -> String {
        let d = &self.0;
        format!(
            "{:02X}{:02X}{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}",
            d[3], d[2], d[1], d[0], // time_low (LE)
            d[5], d[4],             // time_mid (LE)
            d[7], d[6],             // time_hi (LE)
            d[8], d[9],             // clock_seq (BE)
            d[10], d[11], d[12], d[13], d[14], d[15] // node (BE)
        )
    }

    /// Look up a well-known partition type name from GUID.
    pub fn partition_type_name(&self) -> &'static str {
        let s = self.to_string_formatted();
        match s.as_str() {
            "00000000-0000-0000-0000-000000000000" => "Unused",
            "C12A7328-F81F-11D2-BA4B-00A0C93EC93B" => "EFI System",
            "21686148-6449-6E6F-7468-656564454649" => "BIOS Boot",
            "E3C9E316-0B5C-4DB8-817D-F92DF00215AE" => "Microsoft Reserved",
            "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7" => "Microsoft Basic Data",
            "5808C8AA-7E8F-42E0-85D2-E1E90434CFB3" => "Microsoft LDM Metadata",
            "AF9B60A0-1431-4F62-BC68-3311714A69AD" => "Microsoft LDM Data",
            "DE94BBA4-06D1-4D40-A16A-BFD50179D6AC" => "Windows Recovery",
            "0FC63DAF-8483-4772-8E79-3D69D8477DE4" => "Linux Filesystem",
            "0657FD6D-A4AB-43C4-84E5-0933C84B4F4F" => "Linux Swap",
            "E6D6D379-F507-44C2-A23C-238F2A3DF928" => "Linux LVM",
            "A19D880F-05FC-4D3B-A006-743F0F84911E" => "Linux RAID",
            "933AC7E1-2EB4-4F13-B844-0E14E2AEF915" => "Linux Home",
            "48465300-0000-11AA-AA11-00306543ECAC" => "Apple HFS/HFS+",
            "7C3457EF-0000-11AA-AA11-00306543ECAC" => "Apple APFS",
            "55465300-0000-11AA-AA11-00306543ECAC" => "Apple UFS",
            "516E7CB4-6ECF-11D6-8FF8-00022D09712B" => "FreeBSD Data",
            "83BD6B9D-7F41-11DC-BE0B-001560B84F0F" => "FreeBSD Boot",
            _ => "Unknown",
        }
    }
}

impl Serialize for Guid {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string_formatted())
    }
}

/// GPT header (at LBA 1).
#[derive(Debug, Clone, Serialize)]
pub struct GptHeader {
    pub revision: u32,
    pub header_size: u32,
    pub header_crc32: u32,
    pub my_lba: u64,
    pub alternate_lba: u64,
    pub first_usable_lba: u64,
    pub last_usable_lba: u64,
    pub disk_guid: Guid,
    pub partition_entry_lba: u64,
    pub num_partition_entries: u32,
    pub partition_entry_size: u32,
    pub partition_array_crc32: u32,
}

/// A single GPT partition entry.
#[derive(Debug, Clone, Serialize)]
pub struct GptPartitionEntry {
    pub type_guid: Guid,
    pub unique_guid: Guid,
    pub first_lba: u64,
    pub last_lba: u64,
    pub attributes: u64,
    pub name: String,
}

impl GptPartitionEntry {
    pub fn is_empty(&self) -> bool {
        self.type_guid.is_zero()
    }

    pub fn size_bytes(&self) -> u64 {
        if self.is_empty() {
            return 0;
        }
        (self.last_lba - self.first_lba + 1) * SECTOR_SIZE
    }

    pub fn type_name(&self) -> &'static str {
        self.type_guid.partition_type_name()
    }
}

/// Parsed GPT (GUID Partition Table).
#[derive(Debug, Clone, Serialize)]
pub struct Gpt {
    pub header: GptHeader,
    pub entries: Vec<GptPartitionEntry>,
}

impl Gpt {
    /// Parse a GPT from a readable+seekable source.
    /// Assumes sector size is 512 bytes.
    pub fn parse(reader: &mut (impl Read + Seek)) -> Result<Self, RustyBackupError> {
        // Seek to LBA 1 (byte offset 512)
        reader
            .seek(SeekFrom::Start(GPT_HEADER_LBA * SECTOR_SIZE))
            .map_err(|e| RustyBackupError::InvalidGpt(format!("cannot seek to GPT header: {e}")))?;

        let header = Self::parse_header(reader)?;
        let entries = Self::parse_entries(reader, &header)?;

        Ok(Self { header, entries })
    }

    fn parse_header(reader: &mut impl Read) -> Result<GptHeader, RustyBackupError> {
        let mut buf = [0u8; 92];
        reader
            .read_exact(&mut buf)
            .map_err(|e| RustyBackupError::InvalidGpt(format!("cannot read GPT header: {e}")))?;

        let mut c = Cursor(&buf);

        let signature = c.read_u64();
        if signature != GPT_SIGNATURE {
            return Err(RustyBackupError::InvalidGpt(format!(
                "invalid GPT signature: expected 0x{GPT_SIGNATURE:016X}, got 0x{signature:016X}"
            )));
        }

        let revision = c.read_u32();
        let header_size = c.read_u32();
        let header_crc32 = c.read_u32();
        let _reserved = c.read_u32();
        let my_lba = c.read_u64();
        let alternate_lba = c.read_u64();
        let first_usable_lba = c.read_u64();
        let last_usable_lba = c.read_u64();
        let disk_guid = c.read_guid();
        let partition_entry_lba = c.read_u64();
        let num_partition_entries = c.read_u32();
        let partition_entry_size = c.read_u32();
        let partition_array_crc32 = c.read_u32();

        Ok(GptHeader {
            revision,
            header_size,
            header_crc32,
            my_lba,
            alternate_lba,
            first_usable_lba,
            last_usable_lba,
            disk_guid,
            partition_entry_lba,
            num_partition_entries,
            partition_entry_size,
            partition_array_crc32,
        })
    }

    fn parse_entries(
        reader: &mut (impl Read + Seek),
        header: &GptHeader,
    ) -> Result<Vec<GptPartitionEntry>, RustyBackupError> {
        reader
            .seek(SeekFrom::Start(header.partition_entry_lba * SECTOR_SIZE))
            .map_err(|e| {
                RustyBackupError::InvalidGpt(format!("cannot seek to partition entries: {e}"))
            })?;

        let mut entries = Vec::new();
        let entry_size = header.partition_entry_size as usize;
        let mut entry_buf = vec![0u8; entry_size];

        for _ in 0..header.num_partition_entries {
            reader.read_exact(&mut entry_buf).map_err(|e| {
                RustyBackupError::InvalidGpt(format!("cannot read partition entry: {e}"))
            })?;

            let entry = Self::parse_one_entry(&entry_buf)?;
            if !entry.is_empty() {
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    fn parse_one_entry(data: &[u8]) -> Result<GptPartitionEntry, RustyBackupError> {
        if data.len() < 128 {
            return Err(RustyBackupError::InvalidGpt(format!(
                "partition entry too small: {} bytes",
                data.len()
            )));
        }

        let mut c = Cursor(data);
        let type_guid = c.read_guid();
        let unique_guid = c.read_guid();
        let first_lba = c.read_u64();
        let last_lba = c.read_u64();
        let attributes = c.read_u64();

        // Name is UTF-16LE, up to 72 bytes (36 UTF-16 code units)
        let name_bytes = &data[56..128];
        let name = parse_utf16le_name(name_bytes);

        Ok(GptPartitionEntry {
            type_guid,
            unique_guid,
            first_lba,
            last_lba,
            attributes,
            name,
        })
    }
}

/// Minimal cursor for reading little-endian fields from a byte slice.
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn read_u32(&mut self) -> u32 {
        let val = u32::from_le_bytes(self.0[..4].try_into().unwrap());
        self.0 = &self.0[4..];
        val
    }

    fn read_u64(&mut self) -> u64 {
        let val = u64::from_le_bytes(self.0[..8].try_into().unwrap());
        self.0 = &self.0[8..];
        val
    }

    fn read_guid(&mut self) -> Guid {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&self.0[..16]);
        self.0 = &self.0[16..];
        Guid(bytes)
    }
}

fn parse_utf16le_name(bytes: &[u8]) -> String {
    let u16s: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .take_while(|&c| c != 0)
        .collect();
    String::from_utf16_lossy(&u16s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor as StdCursor;

    /// Build a minimal GPT disk image in memory for testing.
    fn make_gpt_image(entries: &[(Guid, Guid, u64, u64, &str)]) -> Vec<u8> {
        let num_entries = 128u32;
        let entry_size = 128u32;
        // We need: LBA 0 (protective MBR) + LBA 1 (GPT header) + LBA 2..33 (partition entries)
        let total_sectors = 34 + 1024; // enough space
        let mut image = vec![0u8; total_sectors as usize * 512];

        // Protective MBR at LBA 0
        image[446] = 0x00; // not bootable
        image[450] = 0xEE; // GPT protective type
        image[454..458].copy_from_slice(&1u32.to_le_bytes()); // start LBA
        image[458..462].copy_from_slice(&0xFFFFFFFFu32.to_le_bytes()); // size
        image[510] = 0x55;
        image[511] = 0xAA;

        // GPT header at LBA 1 (offset 512)
        let hdr_offset = 512usize;
        image[hdr_offset..hdr_offset + 8]
            .copy_from_slice(&GPT_SIGNATURE.to_le_bytes()); // signature
        image[hdr_offset + 8..hdr_offset + 12]
            .copy_from_slice(&0x00010000u32.to_le_bytes()); // revision 1.0
        image[hdr_offset + 12..hdr_offset + 16]
            .copy_from_slice(&92u32.to_le_bytes()); // header size
        // header CRC32 = 0 (skip validation for test)
        image[hdr_offset + 24..hdr_offset + 32]
            .copy_from_slice(&1u64.to_le_bytes()); // my_lba
        image[hdr_offset + 32..hdr_offset + 40]
            .copy_from_slice(&((total_sectors - 1) as u64).to_le_bytes()); // alternate_lba
        image[hdr_offset + 40..hdr_offset + 48]
            .copy_from_slice(&34u64.to_le_bytes()); // first_usable_lba
        image[hdr_offset + 48..hdr_offset + 56]
            .copy_from_slice(&((total_sectors - 34) as u64).to_le_bytes()); // last_usable_lba
        // disk GUID
        let disk_guid = [0xAA; 16];
        image[hdr_offset + 56..hdr_offset + 72].copy_from_slice(&disk_guid);
        image[hdr_offset + 72..hdr_offset + 80]
            .copy_from_slice(&2u64.to_le_bytes()); // partition_entry_lba
        image[hdr_offset + 80..hdr_offset + 84]
            .copy_from_slice(&num_entries.to_le_bytes());
        image[hdr_offset + 84..hdr_offset + 88]
            .copy_from_slice(&entry_size.to_le_bytes());

        // Partition entries at LBA 2 (offset 1024)
        for (i, (type_guid, unique_guid, first_lba, last_lba, name)) in entries.iter().enumerate() {
            let entry_offset = 1024 + i * entry_size as usize;
            image[entry_offset..entry_offset + 16].copy_from_slice(&type_guid.0);
            image[entry_offset + 16..entry_offset + 32].copy_from_slice(&unique_guid.0);
            image[entry_offset + 32..entry_offset + 40]
                .copy_from_slice(&first_lba.to_le_bytes());
            image[entry_offset + 40..entry_offset + 48]
                .copy_from_slice(&last_lba.to_le_bytes());
            // attributes = 0
            // name as UTF-16LE
            let name_offset = entry_offset + 56;
            for (j, ch) in name.encode_utf16().enumerate() {
                let off = name_offset + j * 2;
                if off + 2 <= entry_offset + entry_size as usize {
                    image[off..off + 2].copy_from_slice(&ch.to_le_bytes());
                }
            }
        }

        image
    }

    /// Microsoft Basic Data GUID in mixed-endian byte order
    fn ms_basic_data_guid() -> Guid {
        Guid([
            0xA2, 0xA0, 0xD0, 0xEB, 0xE5, 0xB9, 0x33, 0x44, 0x87, 0xC0, 0x68, 0xB6, 0xB7, 0x26,
            0x99, 0xC7,
        ])
    }

    /// Linux filesystem GUID in mixed-endian byte order
    fn linux_fs_guid() -> Guid {
        Guid([
            0xAF, 0x3D, 0xC6, 0x0F, 0x83, 0x84, 0x72, 0x47, 0x8E, 0x79, 0x3D, 0x69, 0xD8, 0x47,
            0x7D, 0xE4,
        ])
    }

    fn random_guid() -> Guid {
        Guid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    }

    #[test]
    fn test_parse_gpt_single_partition() {
        let image = make_gpt_image(&[(ms_basic_data_guid(), random_guid(), 2048, 1050623, "DATA")]);

        let mut cursor = StdCursor::new(&image);
        let gpt = Gpt::parse(&mut cursor).unwrap();

        assert_eq!(gpt.header.my_lba, 1);
        assert_eq!(gpt.header.first_usable_lba, 34);
        assert_eq!(gpt.entries.len(), 1);
        assert_eq!(gpt.entries[0].first_lba, 2048);
        assert_eq!(gpt.entries[0].last_lba, 1050623);
        assert_eq!(gpt.entries[0].name, "DATA");
        assert_eq!(gpt.entries[0].type_name(), "Microsoft Basic Data");
        assert_eq!(gpt.entries[0].size_bytes(), (1050623 - 2048 + 1) * 512);
    }

    #[test]
    fn test_parse_gpt_multiple_partitions() {
        let image = make_gpt_image(&[
            (ms_basic_data_guid(), random_guid(), 2048, 1050623, "Windows"),
            (linux_fs_guid(), random_guid(), 1050624, 2099199, "Linux"),
        ]);

        let mut cursor = StdCursor::new(&image);
        let gpt = Gpt::parse(&mut cursor).unwrap();

        assert_eq!(gpt.entries.len(), 2);
        assert_eq!(gpt.entries[0].type_name(), "Microsoft Basic Data");
        assert_eq!(gpt.entries[0].name, "Windows");
        assert_eq!(gpt.entries[1].type_name(), "Linux Filesystem");
        assert_eq!(gpt.entries[1].name, "Linux");
    }

    #[test]
    fn test_parse_gpt_empty() {
        let image = make_gpt_image(&[]);
        let mut cursor = StdCursor::new(&image);
        let gpt = Gpt::parse(&mut cursor).unwrap();
        assert!(gpt.entries.is_empty());
    }

    #[test]
    fn test_invalid_gpt_signature() {
        let mut image = vec![0u8; 2048];
        // Write wrong signature at LBA 1
        image[512..520].copy_from_slice(&0u64.to_le_bytes());

        let mut cursor = StdCursor::new(&image);
        let result = Gpt::parse(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn test_guid_formatting() {
        let guid = ms_basic_data_guid();
        assert_eq!(
            guid.to_string_formatted(),
            "EBD0A0A2-B9E5-4433-87C0-68B6B72699C7"
        );
    }

    #[test]
    fn test_utf16_name_parsing() {
        let name = "Test Partition";
        let mut bytes = vec![0u8; 72];
        for (i, ch) in name.encode_utf16().enumerate() {
            bytes[i * 2] = ch as u8;
            bytes[i * 2 + 1] = (ch >> 8) as u8;
        }
        assert_eq!(parse_utf16le_name(&bytes), "Test Partition");
    }
}
