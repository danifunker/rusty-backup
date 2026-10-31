//! Small shared helpers for VBR/BPB patching.
//!
//! The hidden-sectors / partition-offset patchers in `fat.rs`, `ntfs.rs`,
//! and `exfat.rs` all begin with the same read-modify-write pattern on the
//! 512-byte boot sector. Backup-region handling differs per filesystem
//! (FAT32 mirrors at sector 6; NTFS mirrors at the last sector; exFAT
//! mirrors a 12-sector boot region with a recomputed checksum) so each
//! patcher keeps its tail; only the common preamble lives here.
//!
//! See §4 of `docs/codecleanup.md`.

use anyhow::Result;
use std::io::{Read, Seek, SeekFrom, Write};

/// Read the 512-byte boot sector at `partition_offset`.
pub fn read_boot_sector(file: &mut (impl Read + Seek), partition_offset: u64) -> Result<[u8; 512]> {
    file.seek(SeekFrom::Start(partition_offset))?;
    let mut buf = [0u8; 512];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

/// If the u32 LE at `field_offset` in `vbr` differs from `new_value`, patch
/// it in place and return `Some(old_value)`. Returns `None` if the field
/// already matched (i.e. no write needed).
pub fn patch_u32_le_in_buf(vbr: &mut [u8], field_offset: usize, new_value: u32) -> Option<u32> {
    let old = u32::from_le_bytes([
        vbr[field_offset],
        vbr[field_offset + 1],
        vbr[field_offset + 2],
        vbr[field_offset + 3],
    ]);
    if old == new_value {
        None
    } else {
        vbr[field_offset..field_offset + 4].copy_from_slice(&new_value.to_le_bytes());
        Some(old)
    }
}

/// Write a 512-byte buffer back to `partition_offset`.
pub fn write_sector_at(file: &mut (impl Write + Seek), offset: u64, buf: &[u8; 512]) -> Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(buf)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn patch_u32_le_in_buf_detects_change() {
        let mut buf = [0u8; 32];
        buf[4..8].copy_from_slice(&100u32.to_le_bytes());

        assert_eq!(patch_u32_le_in_buf(&mut buf, 4, 100), None);
        assert_eq!(patch_u32_le_in_buf(&mut buf, 4, 200), Some(100));
        assert_eq!(&buf[4..8], &200u32.to_le_bytes());
    }

    #[test]
    fn read_boot_sector_reads_512() {
        let mut data = vec![0u8; 4096];
        data[2048] = 0xEB;
        data[2049] = 0x58;
        let mut cur = Cursor::new(data);
        let sector = read_boot_sector(&mut cur, 2048).unwrap();
        assert_eq!(sector[0], 0xEB);
        assert_eq!(sector[1], 0x58);
    }
}
