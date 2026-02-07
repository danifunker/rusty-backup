use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};

use crate::error::RustyBackupError;

const GPT_SIGNATURE: u64 = 0x5452415020494645; // "EFI PART"
const GPT_HEADER_LBA: u64 = 1;
const SECTOR_SIZE: u64 = 512;

/// A 128-bit GUID stored as raw bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Guid([u8; 16]);

impl Guid {
    /// Create a Guid from raw bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Guid(bytes)
    }

    /// Return a reference to the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Parse a GUID string like `"EBD0A0A2-B9E5-4433-87C0-68B6B72699C7"` back
    /// to mixed-endian bytes (per UEFI spec).
    pub fn from_string(s: &str) -> Result<Self, RustyBackupError> {
        let s = s.trim();
        // Expected format: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX (36 chars)
        let hex: String = s.chars().filter(|&c| c != '-').collect();
        if hex.len() != 32 {
            return Err(RustyBackupError::InvalidGpt(format!(
                "invalid GUID string length: {s}"
            )));
        }
        let parse_byte = |offset: usize| -> Result<u8, RustyBackupError> {
            u8::from_str_radix(&hex[offset..offset + 2], 16)
                .map_err(|_| RustyBackupError::InvalidGpt(format!("invalid hex in GUID: {s}")))
        };
        // The GUID is stored in mixed-endian: first 3 components are LE, last 2 are BE.
        // String format: AABBCCDD-EEFF-GGHH-IIJJ-KKLLMMNNOOPP
        // Bytes (mixed-endian): DD CC BB AA FF EE HH GG II JJ KK LL MM NN OO PP
        let mut bytes = [0u8; 16];
        // time_low (LE): bytes 0-3 from hex[0..8] reversed
        bytes[0] = parse_byte(6)?;
        bytes[1] = parse_byte(4)?;
        bytes[2] = parse_byte(2)?;
        bytes[3] = parse_byte(0)?;
        // time_mid (LE): bytes 4-5 from hex[8..12] reversed
        bytes[4] = parse_byte(10)?;
        bytes[5] = parse_byte(8)?;
        // time_hi (LE): bytes 6-7 from hex[12..16] reversed
        bytes[6] = parse_byte(14)?;
        bytes[7] = parse_byte(12)?;
        // clock_seq (BE): bytes 8-9 from hex[16..20]
        bytes[8] = parse_byte(16)?;
        bytes[9] = parse_byte(18)?;
        // node (BE): bytes 10-15 from hex[20..32]
        for i in 0..6 {
            bytes[10 + i] = parse_byte(20 + i * 2)?;
        }
        Ok(Guid(bytes))
    }

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

impl<'de> Deserialize<'de> for Guid {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Guid::from_string(&s).map_err(serde::de::Error::custom)
    }
}

/// GPT header (at LBA 1).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Serialize this entry to bytes, zero-padded to `entry_size`.
    pub fn serialize_to_bytes(&self, entry_size: u32) -> Vec<u8> {
        let mut buf = vec![0u8; entry_size as usize];
        buf[0..16].copy_from_slice(&self.type_guid.0);
        buf[16..32].copy_from_slice(&self.unique_guid.0);
        buf[32..40].copy_from_slice(&self.first_lba.to_le_bytes());
        buf[40..48].copy_from_slice(&self.last_lba.to_le_bytes());
        buf[48..56].copy_from_slice(&self.attributes.to_le_bytes());
        // Name as UTF-16LE (up to 36 code units = 72 bytes)
        let name_area = &mut buf[56..128.min(entry_size as usize)];
        for (i, ch) in self.name.encode_utf16().take(36).enumerate() {
            let off = i * 2;
            if off + 2 <= name_area.len() {
                name_area[off..off + 2].copy_from_slice(&ch.to_le_bytes());
            }
        }
        buf
    }
}

impl GptHeader {
    /// Serialize the header fields to a 92-byte buffer (no CRC computation).
    pub fn serialize_to_bytes(&self) -> [u8; 92] {
        let mut buf = [0u8; 92];
        buf[0..8].copy_from_slice(&GPT_SIGNATURE.to_le_bytes());
        buf[8..12].copy_from_slice(&self.revision.to_le_bytes());
        buf[12..16].copy_from_slice(&self.header_size.to_le_bytes());
        buf[16..20].copy_from_slice(&self.header_crc32.to_le_bytes());
        // bytes 20..24 reserved (zeros)
        buf[24..32].copy_from_slice(&self.my_lba.to_le_bytes());
        buf[32..40].copy_from_slice(&self.alternate_lba.to_le_bytes());
        buf[40..48].copy_from_slice(&self.first_usable_lba.to_le_bytes());
        buf[48..56].copy_from_slice(&self.last_usable_lba.to_le_bytes());
        buf[56..72].copy_from_slice(&self.disk_guid.0);
        buf[72..80].copy_from_slice(&self.partition_entry_lba.to_le_bytes());
        buf[80..84].copy_from_slice(&self.num_partition_entries.to_le_bytes());
        buf[84..88].copy_from_slice(&self.partition_entry_size.to_le_bytes());
        buf[88..92].copy_from_slice(&self.partition_array_crc32.to_le_bytes());
        buf
    }
}

/// Parsed GPT (GUID Partition Table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gpt {
    pub header: GptHeader,
    pub entries: Vec<GptPartitionEntry>,
}

impl Gpt {
    /// Serialize all partition entries (including empty slots) to bytes.
    /// The parser filters empty entries, so this zero-fills remaining slots
    /// up to `num_partition_entries`.
    pub fn serialize_partition_array(&self) -> Vec<u8> {
        let entry_size = self.header.partition_entry_size;
        let num = self.header.num_partition_entries as usize;
        let mut buf = vec![0u8; num * entry_size as usize];
        // Write non-empty entries at the start, remaining slots are already zero
        for (i, entry) in self.entries.iter().enumerate() {
            let offset = i * entry_size as usize;
            let serialized = entry.serialize_to_bytes(entry_size);
            buf[offset..offset + entry_size as usize].copy_from_slice(&serialized);
        }
        buf
    }

    /// Compute CRC32 of the serialized partition array.
    pub fn compute_partition_array_crc32(&self) -> u32 {
        let array = self.serialize_partition_array();
        crc32fast::hash(&array)
    }

    /// Compute CRC32 of a 92-byte GPT header with the header_crc32 field zeroed.
    pub fn compute_header_crc32(header_bytes: &[u8; 92]) -> u32 {
        let mut buf = *header_bytes;
        // Zero out the header CRC32 field (bytes 16..20)
        buf[16..20].copy_from_slice(&0u32.to_le_bytes());
        crc32fast::hash(&buf)
    }

    /// Build 33 sectors for the primary GPT (header at LBA 1, entries at LBAs 2-33).
    /// Returns 33 * 512 = 16896 bytes.
    pub fn build_primary_gpt(&self, target_disk_sectors: u64) -> Vec<u8> {
        let last_lba = target_disk_sectors - 1;
        let mut header = self.header.clone();
        header.my_lba = 1;
        header.alternate_lba = last_lba;
        header.first_usable_lba = 34;
        header.last_usable_lba = last_lba - 33;
        header.partition_entry_lba = 2;

        // Compute partition array CRC32
        let array = self.serialize_partition_array();
        header.partition_array_crc32 = crc32fast::hash(&array);

        // Serialize header, compute header CRC32, then re-serialize
        header.header_crc32 = 0;
        let mut hdr_bytes = header.serialize_to_bytes();
        header.header_crc32 = Self::compute_header_crc32(&hdr_bytes);
        hdr_bytes = header.serialize_to_bytes();

        let mut result = vec![0u8; 33 * 512];
        // Header at sector 0 of this block (LBA 1 on disk)
        result[..92].copy_from_slice(&hdr_bytes);
        // Entries at sectors 1-32 of this block (LBAs 2-33 on disk)
        let entries_len = array.len().min(32 * 512);
        result[512..512 + entries_len].copy_from_slice(&array[..entries_len]);
        result
    }

    /// Build 33 sectors for the backup GPT (entries followed by header at last LBA).
    /// Returns 33 * 512 = 16896 bytes.
    pub fn build_backup_gpt(&self, target_disk_sectors: u64) -> Vec<u8> {
        let last_lba = target_disk_sectors - 1;
        let mut header = self.header.clone();
        header.my_lba = last_lba;
        header.alternate_lba = 1;
        header.first_usable_lba = 34;
        header.last_usable_lba = last_lba - 33;
        header.partition_entry_lba = last_lba - 32;

        // Compute partition array CRC32
        let array = self.serialize_partition_array();
        header.partition_array_crc32 = crc32fast::hash(&array);

        // Serialize header with CRC32
        header.header_crc32 = 0;
        let mut hdr_bytes = header.serialize_to_bytes();
        header.header_crc32 = Self::compute_header_crc32(&hdr_bytes);
        hdr_bytes = header.serialize_to_bytes();

        let mut result = vec![0u8; 33 * 512];
        // Entries at sectors 0-31 (LBAs last-32 .. last-1)
        let entries_len = array.len().min(32 * 512);
        result[..entries_len].copy_from_slice(&array[..entries_len]);
        // Header at sector 32 (last LBA)
        result[32 * 512..32 * 512 + 92].copy_from_slice(&hdr_bytes);
        result
    }

    /// Build a protective MBR for a GPT disk.
    pub fn build_protective_mbr(total_sectors: u64) -> [u8; 512] {
        let mut mbr = [0u8; 512];
        // Single partition entry at offset 446
        mbr[446] = 0x00; // not bootable
                         // CHS start: 0/0/2 (sector 1 in CHS = LBA 1)
        mbr[447] = 0x00;
        mbr[448] = 0x02;
        mbr[449] = 0x00;
        mbr[450] = 0xEE; // GPT protective type
                         // CHS end: fill with max values
        mbr[451] = 0xFF;
        mbr[452] = 0xFF;
        mbr[453] = 0xFF;
        // Start LBA = 1
        mbr[454..458].copy_from_slice(&1u32.to_le_bytes());
        // Size = min(total_sectors - 1, 0xFFFFFFFF)
        let size = (total_sectors - 1).min(0xFFFF_FFFF) as u32;
        mbr[458..462].copy_from_slice(&size.to_le_bytes());
        // Boot signature
        mbr[510] = 0x55;
        mbr[511] = 0xAA;
        mbr
    }

    /// Create a patched copy of this GPT for restore, updating entry LBAs
    /// from overrides and adjusting header usable LBA range.
    pub fn patch_for_restore(
        &self,
        overrides: &[crate::partition::PartitionSizeOverride],
        target_disk_sectors: u64,
    ) -> Gpt {
        let mut patched = self.clone();
        let last_lba = target_disk_sectors - 1;

        patched.header.first_usable_lba = 34;
        patched.header.last_usable_lba = last_lba - 33;
        patched.header.alternate_lba = last_lba;

        for entry in &mut patched.entries {
            // Match by original start_lba
            if let Some(ov) = overrides.iter().find(|o| o.start_lba == entry.first_lba) {
                let new_start = ov.effective_start_lba();
                let new_sectors = ov.export_size / SECTOR_SIZE;
                entry.first_lba = new_start;
                entry.last_lba = new_start + new_sectors - 1;
            }
        }

        patched
    }

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
        image[hdr_offset..hdr_offset + 8].copy_from_slice(&GPT_SIGNATURE.to_le_bytes()); // signature
        image[hdr_offset + 8..hdr_offset + 12].copy_from_slice(&0x00010000u32.to_le_bytes()); // revision 1.0
        image[hdr_offset + 12..hdr_offset + 16].copy_from_slice(&92u32.to_le_bytes()); // header size
                                                                                       // header CRC32 = 0 (skip validation for test)
        image[hdr_offset + 24..hdr_offset + 32].copy_from_slice(&1u64.to_le_bytes()); // my_lba
        image[hdr_offset + 32..hdr_offset + 40]
            .copy_from_slice(&((total_sectors - 1) as u64).to_le_bytes()); // alternate_lba
        image[hdr_offset + 40..hdr_offset + 48].copy_from_slice(&34u64.to_le_bytes()); // first_usable_lba
        image[hdr_offset + 48..hdr_offset + 56]
            .copy_from_slice(&((total_sectors - 34) as u64).to_le_bytes()); // last_usable_lba
                                                                            // disk GUID
        let disk_guid = [0xAA; 16];
        image[hdr_offset + 56..hdr_offset + 72].copy_from_slice(&disk_guid);
        image[hdr_offset + 72..hdr_offset + 80].copy_from_slice(&2u64.to_le_bytes()); // partition_entry_lba
        image[hdr_offset + 80..hdr_offset + 84].copy_from_slice(&num_entries.to_le_bytes());
        image[hdr_offset + 84..hdr_offset + 88].copy_from_slice(&entry_size.to_le_bytes());

        // Partition entries at LBA 2 (offset 1024)
        for (i, (type_guid, unique_guid, first_lba, last_lba, name)) in entries.iter().enumerate() {
            let entry_offset = 1024 + i * entry_size as usize;
            image[entry_offset..entry_offset + 16].copy_from_slice(&type_guid.0);
            image[entry_offset + 16..entry_offset + 32].copy_from_slice(&unique_guid.0);
            image[entry_offset + 32..entry_offset + 40].copy_from_slice(&first_lba.to_le_bytes());
            image[entry_offset + 40..entry_offset + 48].copy_from_slice(&last_lba.to_le_bytes());
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
            (
                ms_basic_data_guid(),
                random_guid(),
                2048,
                1050623,
                "Windows",
            ),
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
    fn test_build_primary_gpt_valid_crc32() {
        let image = make_gpt_image(&[(ms_basic_data_guid(), random_guid(), 2048, 1050623, "DATA")]);
        let mut cursor = StdCursor::new(&image);
        let gpt = Gpt::parse(&mut cursor).unwrap();

        let target_sectors = 2097152u64; // 1 GB disk
        let primary = gpt.build_primary_gpt(target_sectors);
        assert_eq!(primary.len(), 33 * 512);

        // Parse the built GPT back
        let mut built_image = vec![0u8; 512]; // protective MBR placeholder
        built_image.extend_from_slice(&primary);
        let mut cursor = StdCursor::new(&built_image);
        let parsed = Gpt::parse(&mut cursor).unwrap();

        assert_eq!(parsed.header.my_lba, 1);
        assert_eq!(parsed.header.alternate_lba, target_sectors - 1);
        assert_eq!(parsed.header.first_usable_lba, 34);
        assert_eq!(parsed.header.last_usable_lba, target_sectors - 34);
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0].first_lba, 2048);

        // Verify CRC32 of the partition array
        let array_crc = parsed.compute_partition_array_crc32();
        assert_eq!(array_crc, parsed.header.partition_array_crc32);

        // Verify header CRC32
        let hdr_bytes = parsed.header.serialize_to_bytes();
        let computed_crc = Gpt::compute_header_crc32(&hdr_bytes);
        assert_eq!(computed_crc, parsed.header.header_crc32);
    }

    #[test]
    fn test_build_backup_gpt_valid() {
        let image = make_gpt_image(&[(ms_basic_data_guid(), random_guid(), 2048, 1050623, "DATA")]);
        let mut cursor = StdCursor::new(&image);
        let gpt = Gpt::parse(&mut cursor).unwrap();

        let target_sectors = 2097152u64;
        let backup = gpt.build_backup_gpt(target_sectors);
        assert_eq!(backup.len(), 33 * 512);

        // The header is at sector 32 in the backup block
        let hdr_bytes: [u8; 92] = backup[32 * 512..32 * 512 + 92].try_into().unwrap();
        let parsed_header = Gpt::parse_header(&mut &hdr_bytes[..]).unwrap();
        assert_eq!(parsed_header.my_lba, target_sectors - 1);
        assert_eq!(parsed_header.alternate_lba, 1);
        assert_eq!(parsed_header.partition_entry_lba, target_sectors - 1 - 32);
    }

    #[test]
    fn test_protective_mbr() {
        let mbr = Gpt::build_protective_mbr(2097152);
        assert_eq!(mbr[510], 0x55);
        assert_eq!(mbr[511], 0xAA);
        assert_eq!(mbr[450], 0xEE);
        let start_lba = u32::from_le_bytes(mbr[454..458].try_into().unwrap());
        assert_eq!(start_lba, 1);
    }

    #[test]
    fn test_patch_for_restore() {
        let image = make_gpt_image(&[
            (ms_basic_data_guid(), random_guid(), 2048, 1050623, "Win"),
            (linux_fs_guid(), random_guid(), 1050624, 2099199, "Linux"),
        ]);
        let mut cursor = StdCursor::new(&image);
        let gpt = Gpt::parse(&mut cursor).unwrap();

        let overrides = vec![crate::partition::PartitionSizeOverride {
            index: 0,
            start_lba: 2048,
            original_size: (1050623 - 2048 + 1) * 512,
            export_size: 2097152 * 512, // grow the partition
            new_start_lba: Some(4096),
            heads: 0,
            sectors_per_track: 0,
        }];

        let patched = gpt.patch_for_restore(&overrides, 4194304);
        assert_eq!(patched.entries[0].first_lba, 4096);
        assert_eq!(patched.entries[0].last_lba, 4096 + 2097152 - 1);
        // Second entry unchanged
        assert_eq!(patched.entries[1].first_lba, 1050624);
    }

    #[test]
    fn test_serialize_parse_roundtrip() {
        // Create a GPT via parsing
        let image = make_gpt_image(&[
            (
                ms_basic_data_guid(),
                random_guid(),
                2048,
                1050623,
                "Windows",
            ),
            (linux_fs_guid(), random_guid(), 1050624, 2099199, "Linux"),
        ]);
        let mut cursor = StdCursor::new(&image);
        let gpt = Gpt::parse(&mut cursor).unwrap();

        // Serialize partition array and re-parse entries
        let array_bytes = gpt.serialize_partition_array();
        assert_eq!(
            array_bytes.len(),
            gpt.header.num_partition_entries as usize * gpt.header.partition_entry_size as usize
        );

        // Parse entries back from the serialized array
        let entry_size = gpt.header.partition_entry_size as usize;
        let mut parsed_entries = Vec::new();
        for i in 0..gpt.header.num_partition_entries as usize {
            let offset = i * entry_size;
            let entry = Gpt::parse_one_entry(&array_bytes[offset..offset + entry_size]).unwrap();
            if !entry.is_empty() {
                parsed_entries.push(entry);
            }
        }
        assert_eq!(parsed_entries.len(), 2);
        assert_eq!(parsed_entries[0].first_lba, 2048);
        assert_eq!(parsed_entries[0].name, "Windows");
        assert_eq!(parsed_entries[0].type_guid, ms_basic_data_guid());
        assert_eq!(parsed_entries[1].first_lba, 1050624);
        assert_eq!(parsed_entries[1].name, "Linux");

        // Serialize header and re-parse
        let hdr_bytes = gpt.header.serialize_to_bytes();
        assert_eq!(hdr_bytes.len(), 92);
        let parsed_header = Gpt::parse_header(&mut &hdr_bytes[..]).unwrap();
        assert_eq!(parsed_header.my_lba, gpt.header.my_lba);
        assert_eq!(parsed_header.disk_guid, gpt.header.disk_guid);
        assert_eq!(
            parsed_header.num_partition_entries,
            gpt.header.num_partition_entries
        );
    }

    #[test]
    fn test_guid_string_roundtrip() {
        let guid = ms_basic_data_guid();
        let s = guid.to_string_formatted();
        let parsed = Guid::from_string(&s).unwrap();
        assert_eq!(guid, parsed);
        assert_eq!(parsed.to_string_formatted(), s);

        // Zero GUID
        let zero = Guid([0u8; 16]);
        let zero_s = zero.to_string_formatted();
        let zero_parsed = Guid::from_string(&zero_s).unwrap();
        assert_eq!(zero, zero_parsed);

        // Linux filesystem GUID
        let linux = linux_fs_guid();
        let linux_s = linux.to_string_formatted();
        let linux_parsed = Guid::from_string(&linux_s).unwrap();
        assert_eq!(linux, linux_parsed);
    }

    #[test]
    fn test_guid_serde_roundtrip() {
        let guid = ms_basic_data_guid();
        let json = serde_json::to_string(&guid).unwrap();
        let parsed: Guid = serde_json::from_str(&json).unwrap();
        assert_eq!(guid, parsed);
    }

    #[test]
    fn test_gpt_json_roundtrip() {
        let image = make_gpt_image(&[(ms_basic_data_guid(), random_guid(), 2048, 1050623, "DATA")]);
        let mut cursor = StdCursor::new(&image);
        let gpt = Gpt::parse(&mut cursor).unwrap();

        let json = serde_json::to_string_pretty(&gpt).unwrap();
        let parsed: Gpt = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.header.my_lba, gpt.header.my_lba);
        assert_eq!(parsed.header.disk_guid, gpt.header.disk_guid);
        assert_eq!(parsed.entries.len(), gpt.entries.len());
        assert_eq!(parsed.entries[0].first_lba, gpt.entries[0].first_lba);
        assert_eq!(parsed.entries[0].type_guid, gpt.entries[0].type_guid);
        assert_eq!(parsed.entries[0].name, gpt.entries[0].name);
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
