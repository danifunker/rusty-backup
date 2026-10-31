//! Moved to the `part-tables` crate; re-exported so `crate::partition::rdb` paths keep working.

pub use part_tables::rdb::*;

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Cursor;

    fn make_block(magic: &[u8; 4], fields: &[(usize, u32)]) -> [u8; 512] {
        let mut buf = [0u8; 512];
        buf[0..4].copy_from_slice(magic);
        for &(idx, v) in fields {
            BigEndian::write_u32(&mut buf[idx * 4..idx * 4 + 4], v);
        }
        // Compute checksum: sum of all i32s with checksum slot = 0 should
        // be negated and stored.
        BigEndian::write_i32(&mut buf[2 * 4..2 * 4 + 4], 0);
        let mut sum: i32 = 0;
        for i in 0..128 {
            sum = sum.wrapping_add(BigEndian::read_i32(&buf[i * 4..i * 4 + 4]));
        }
        BigEndian::write_i32(&mut buf[2 * 4..2 * 4 + 4], sum.wrapping_neg());
        buf
    }

    /// One-PART RDB ("SS1", cylinders 2..=3 at 4x32) whose partition starts with `content`.
    fn rdb_with_content(dos_type: u32, content: &[u8], disk_len: usize) -> Vec<u8> {
        let mut disk = vec![0u8; disk_len];
        let rdsk = make_block(
            RDSK_SIGNATURE,
            &[
                (1, 64),
                (4, 512),
                (7, 1),
                (8, NO_BLOCK),
                (6, NO_BLOCK),
                (9, NO_BLOCK),
            ],
        );
        disk[0..512].copy_from_slice(&rdsk);
        let part = make_block(
            PART_SIGNATURE,
            &[
                (1, 64),
                (4, NO_BLOCK),
                (9, u32::from_be_bytes([3, b'S', b'S', b'1'])),
                (32, 16),
                (33, 128),
                (35, 4),
                (36, 1),
                (37, 32),
                (41, 2),
                (42, 3),
                (48, dos_type),
            ],
        );
        disk[512..1024].copy_from_slice(&part);
        let start = 2 * 4 * 32 * 512;
        if disk.len() >= start + content.len() {
            disk[start..start + content.len()].copy_from_slice(content);
        }
        disk
    }

    #[test]
    fn dos_type_parses_back_from_every_catalogued_tag() {
        for choice in
            crate::partition::type_catalog::choices(crate::partition::type_catalog::TableKind::Rdb)
        {
            let parsed = parse_dos_type(choice.value)
                .unwrap_or_else(|| panic!("{} did not parse", choice.value));
            assert_eq!(format_dos_type(parsed), choice.value);
        }
        assert_eq!(parse_dos_type("muFS"), Some(0x6D75_4653));
        assert_eq!(parse_dos_type("DOS"), None);
        assert_eq!(parse_dos_type("DOS\\x"), None);
    }

    /// ShapeShifter's raw Mac partitions: an HFS volume under a PFS\3 DosType routes as HFS.
    #[test]
    fn hfs_under_a_pfs_dostype_routes_as_hfs() {
        let mut hfs = vec![0u8; 1536];
        hfs[0..2].copy_from_slice(b"LK");
        hfs[1024..1026].copy_from_slice(b"BD");
        hfs[1044..1048].copy_from_slice(&4096u32.to_be_bytes());
        let len = 2 * 4 * 32 * 512 * 2;
        let rdb = Rdb::parse(&mut Cursor::new(rdb_with_content(0x5046_5303, &hfs, len))).unwrap();
        let info = &crate::partition::PartitionTable::Rdb(rdb.clone()).partitions()[0];
        assert_eq!(info.partition_type_string.as_deref(), Some("Apple_HFS"));
        assert!(
            info.type_name.contains("declared PFS\\3"),
            "{}",
            info.type_name
        );
    }
}
