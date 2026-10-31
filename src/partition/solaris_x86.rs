//! Moved to the `part-tables` crate; re-exported so `crate::partition::solaris_x86` paths keep working.

pub use part_tables::solaris_x86::*;

#[cfg(test)]
mod tests {
    /// A Solaris disk is still an MBR disk: the other primaries and the EBR
    /// logicals list after the slices instead of vanishing (P3, 2026-09-01).
    #[test]
    fn the_rest_of_the_mbr_lists_after_the_slices() {
        use crate::partition::provision::{default_align, place, write_table, Geometry, PartSpec};
        use crate::partition::type_catalog::TableKind;
        use crate::partition::{PartitionTable, SOLARIS_MBR_INDEX_BASE};
        use std::io::Cursor;
        let disk = 256u64 * 1024 * 1024;
        let geometry = Geometry {
            heads: 16,
            sectors_per_track: 63,
        };
        let kind = TableKind::SolarisX86;
        let align = default_align(kind, geometry);
        let specs = vec![PartSpec {
            size: Some(disk / 4),
            type_text: None,
            name: None,
        }];
        let placed = place(&specs, kind, disk, align, geometry).unwrap();
        let mut cur = Cursor::new(vec![0u8; disk as usize]);
        write_table(&mut cur, kind, &placed, disk, geometry).unwrap();
        let mut img = cur.into_inner();

        // Slot 1: a FAT16 primary; slot 2: an extended container holding one
        // Linux logical 63 sectors in. Both sit past the Solaris partition.
        let mut entry = |slot: usize, kind: u8, start: u32, sectors: u32| {
            let e = 446 + slot * 16;
            img[e + 4] = kind;
            img[e + 8..e + 12].copy_from_slice(&start.to_le_bytes());
            img[e + 12..e + 16].copy_from_slice(&sectors.to_le_bytes());
        };
        let fat_start = 96 * 2048u32;
        let ext_start = 160 * 2048u32;
        entry(1, 0x06, fat_start, 32 * 2048);
        entry(2, 0x05, ext_start, 64 * 2048);
        let ebr = ext_start as usize * 512;
        img[ebr + 446 + 4] = 0x83;
        img[ebr + 446 + 8..ebr + 446 + 12].copy_from_slice(&63u32.to_le_bytes());
        img[ebr + 446 + 12..ebr + 446 + 16].copy_from_slice(&(32 * 2048u32).to_le_bytes());
        img[ebr + 510] = 0x55;
        img[ebr + 511] = 0xAA;

        let table = PartitionTable::detect(&mut Cursor::new(img)).unwrap();
        let PartitionTable::SolarisX86 { mbr, label } = &table else {
            panic!("not a Solaris x86 disk: {table:?}");
        };
        assert_eq!(mbr.logical_partitions.len(), 1, "the EBR walk still runs");
        let parts = table.partitions();
        let slices = label.browsable_slices().count();
        assert!(slices >= 1 && parts.len() == slices + 3, "{parts:?}");
        let rest: Vec<_> = parts[slices..]
            .iter()
            .map(|p| (p.index, p.partition_type_byte, p.start_lba, p.is_logical))
            .collect();
        let b = SOLARIS_MBR_INDEX_BASE;
        assert_eq!(
            rest,
            vec![
                (b + 1, 0x06, fat_start as u64, false),
                (b + 2, 0x05, ext_start as u64, false),
                (b + 4, 0x83, ext_start as u64 + 63, true),
            ]
        );
        assert!(parts.iter().all(|p| p.index != b + label.mbr_slot));
        assert_eq!(table.native_slot(&parts[0]), Some(parts[0].index as u32));
        assert_eq!(
            table.native_slot(&parts[slices]),
            None,
            "no `sN` for MBR entries"
        );
    }
}
