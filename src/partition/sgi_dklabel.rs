//! Moved to the `part-tables` crate; re-exported so `crate::partition::sgi_dklabel` paths keep working.

pub use part_tables::sgi_dklabel::*;

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};

    /// The label from the real IRIS 3130 `Priam V170` disk, native order.
    fn sample_label() -> Vec<u8> {
        let mut b = vec![0u8; 512];
        BigEndian::write_u32(&mut b[0..4], part_tables::sgi_dklabel::SGI_DKLABEL_MAGIC);
        BigEndian::write_u16(&mut b[0x04..0x06], 1); // DT_V170
        BigEndian::write_u16(&mut b[0x06..0x08], 0); // DC_DSD5217
        BigEndian::write_u16(&mut b[0x08..0x0A], 987);
        BigEndian::write_u16(&mut b[0x0A..0x0C], 7);
        BigEndian::write_u16(&mut b[0x0C..0x0E], 17);
        BigEndian::write_u32(&mut b[0x0E..0x12], 115_430);
        BigEndian::write_u16(&mut b[0x12..0x14], 2023);
        b[0x14] = 0; // bootfs
        b[0x15] = 1; // swapfs
        let slots: [(u32, u32); 8] = [
            (119, 17850),
            (17969, 17731),
            (35700, 79730),
            (119, 115_311),
            (0, 0),
            (0, 0),
            (119, 115_311),
            (0, 115_430),
        ];
        for (i, (base, size)) in slots.iter().enumerate() {
            let o = 0x16 + i * 8;
            BigEndian::write_u32(&mut b[o..o + 4], *base);
            BigEndian::write_u32(&mut b[o + 4..o + 8], *size);
        }
        b[0x56] = 1; // interleave
        b[0x58] = 11; // cylskew
        b[0x5C..0x5C + 10].copy_from_slice(b"Priam V170");
        b[0x8E..0x8E + 4].copy_from_slice(b"0000");
        b
    }

    fn swabbed(mut b: Vec<u8>) -> Vec<u8> {
        swab16_in_place(&mut b);
        b
    }

    #[test]
    fn partition_table_reports_the_word_order() {
        use crate::partition::PartitionTable;
        let native = PartitionTable::SgiDkLabel(SgiDiskLabel::parse(&sample_label()).unwrap());
        let swapped =
            PartitionTable::SgiDkLabel(SgiDiskLabel::parse(&swabbed(sample_label())).unwrap());
        assert_eq!(native.byte_order_name(), Some("native"));
        assert_eq!(swapped.byte_order_name(), Some("byte-swapped"));
    }

    #[test]
    fn tables_without_a_word_order_report_none() {
        use crate::partition::PartitionTable;
        let sf = PartitionTable::None {
            size_bytes: 1_474_560,
            fs_hint: "FAT".to_string(),
        };
        assert_eq!(sf.byte_order_name(), None);
    }
}
