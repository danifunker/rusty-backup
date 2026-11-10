//! Tests of app behaviour (dispatch, detection, builders) over code that moved into the
//! `crates/` workspace; each module is named after the crate module it exercises.

mod rdb {
    use byteorder::{BigEndian, ByteOrder};
    use part_tables::rdb::*;
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

mod sgi_dklabel {
    use byteorder::{BigEndian, ByteOrder};
    use part_tables::sgi_dklabel::*;

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

mod solaris_x86 {
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

mod bsd_dedicated {
    //! FreeBSD/alpha "dangerously dedicated": the label at byte 64 inside slice a, which starts at 0.
    use crate::partition::PartitionTable;
    use fs_unix::ufs_format::{create_blank_ufs1, Ufs1FormatParams};
    use std::io::Cursor;

    const MIB: usize = 1024 * 1024;
    const SLICE: usize = 4 * MIB;

    fn ufs(label: &str) -> Vec<u8> {
        let params = Ufs1FormatParams {
            size_bytes: SLICE as u64,
            label: Some(label.to_string()),
            ..Default::default()
        };
        create_blank_ufs1(&params).expect("format")
    }

    /// Slots a (FFS at 0), b (swap), c (raw), e (FFS), little-endian, label at byte 64.
    fn disk() -> Vec<u8> {
        let sectors = (3 * SLICE / 512) as u32;
        let mut d = vec![0u8; 3 * SLICE];
        d[..SLICE].copy_from_slice(&ufs("slice-a"));
        d[2 * SLICE..].copy_from_slice(&ufs("slice-e"));
        let l = 64;
        let put32 = |d: &mut Vec<u8>, o: usize, v: u32| {
            d[l + o..l + o + 4].copy_from_slice(&v.to_le_bytes())
        };
        put32(&mut d, 0, part_tables::bsd::BSD_DISKMAGIC);
        put32(&mut d, 132, part_tables::bsd::BSD_DISKMAGIC);
        d[l + 8..l + 11].copy_from_slice(b"da0");
        put32(&mut d, 40, 512);
        put32(&mut d, 60, sectors);
        d[l + 138..l + 140].copy_from_slice(&5u16.to_le_bytes());
        let slot = (SLICE / 512) as u32;
        let slots = [
            (slot, 0, 7u8),
            (slot, slot, 1),
            (sectors, 0, 0),
            (0, 0, 0),
            (slot, 2 * slot, 7),
        ];
        for (i, (size, off, ty)) in slots.iter().enumerate() {
            let p = 148 + i * 16;
            put32(&mut d, p, *size);
            put32(&mut d, p + 4, *off);
            d[l + p + 12] = *ty;
        }
        let ck = (0..148 + 5 * 16).step_by(2).fold(0u16, |x, o| {
            x ^ u16::from_le_bytes([d[l + o], d[l + o + 1]])
        });
        d[l + 136..l + 138].copy_from_slice(&ck.to_le_bytes());
        d
    }

    #[test]
    fn the_label_wins_over_the_ufs_at_sector_zero() {
        let d = disk();
        let pt = PartitionTable::detect(&mut Cursor::new(&d)).expect("detect");
        assert!(matches!(pt, PartitionTable::Bsd(_)), "{}", pt.type_name());
        let parts = pt.partitions();
        let names: Vec<&str> = parts.iter().map(|p| p.type_name.as_str()).collect();
        assert_eq!(names, ["BSD a (4.2BSD)", "BSD b (swap)", "BSD e (4.2BSD)"]);
        for (at, label) in [(0, "slice-a"), (2, "slice-e")] {
            let p = &parts[at];
            let fs = crate::fs::open_filesystem_sized(
                Cursor::new(d.clone()),
                p.byte_offset(),
                Some(p.size_bytes),
                p.partition_type_byte,
                p.partition_type_string.as_deref(),
            );
            let mut fs = fs.expect("open slice");
            assert_eq!(fs.volume_label(), Some(label));
            let root = fs.root().expect("root");
            fs.list_directory(&root).expect("ls");
        }
    }

    #[test]
    fn a_bad_checksum_leaves_the_whole_disk_ufs() {
        let mut d = disk();
        d[64 + 136] ^= 1;
        let pt = PartitionTable::detect(&mut Cursor::new(&d)).expect("detect");
        assert!(!matches!(pt, PartitionTable::Bsd(_)));
        assert!(
            matches!(pt, PartitionTable::None { .. }),
            "{}",
            pt.type_name()
        );
    }

    #[test]
    fn the_swap_slice_raw_view_ends_with_the_slice() {
        let d = disk();
        let pt = PartitionTable::detect(&mut Cursor::new(&d)).expect("detect");
        let p = &pt.partitions()[1];
        let mut fs = crate::fs::open_filesystem_sized(
            Cursor::new(d.clone()),
            p.byte_offset(),
            Some(p.size_bytes),
            p.partition_type_byte,
            p.partition_type_string.as_deref(),
        )
        .expect("carve view");
        let root = fs.root().expect("root");
        let whole = fs.list_directory(&root).expect("ls");
        let whole = whole
            .iter()
            .find(|e| e.name == "whole-disk.img")
            .expect("raw");
        assert_eq!(whole.size, SLICE as u64);
    }
}
