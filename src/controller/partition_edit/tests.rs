use std::collections::HashMap;

use super::*;

use crate::model::disk_builder::DiskBuilder;
use crate::model::partition_editor::PartitionEditor;
use crate::partition::PartitionTable;

const MIB: u64 = 1024 * 1024;

/// A 64 MiB MBR disk with one 2 MiB FAT16 partition at LBA 2048.
fn mbr_table() -> PartitionTable {
    let mut d = vec![0u8; 4096];
    d[0x1BE + 4] = 0x06;
    d[0x1BE + 8..0x1BE + 12].copy_from_slice(&2048u32.to_le_bytes());
    d[0x1BE + 12..0x1BE + 16].copy_from_slice(&4096u32.to_le_bytes());
    d[510] = 0x55;
    d[511] = 0xAA;
    PartitionTable::detect(&mut std::io::Cursor::new(d)).expect("an MBR")
}

#[test]
fn the_table_editor_stamps_sizes_fills_a_gap_adds_and_validates() {
    let table = mbr_table();
    let parts = table.partitions();
    let mut mins = HashMap::new();
    mins.insert(parts[0].index, MIB);
    let mut ed = PartitionEditor::new();
    ed.seed_from_with_minimums(parts.as_slice(), &mins, Some(64 * MIB));

    let v = table_view(&ed, parts.as_slice(), Some(&table), true);
    assert_eq!(v.rows.len(), 1);
    assert_eq!(v.boot_style, BootStyle::Checkbox);
    assert!(v.add_bootable_shown);
    assert_eq!(v.current.segments.len(), 3, "free, the partition, free");

    apply_table_edit(
        &mut ed,
        Some(&table),
        TableEditAction::SetSizeMode {
            row: 0,
            mode: SizeMode::Minimum,
        },
    );
    assert_eq!(ed.entries[0].size_text, "1.00");

    let (start, size, _) = v
        .gaps
        .last()
        .cloned()
        .expect("free space after the partition");
    apply_table_edit(
        &mut ed,
        Some(&table),
        TableEditAction::FillGap {
            start_lba: start,
            size_bytes: size,
        },
    );
    apply_table_edit(
        &mut ed,
        Some(&table),
        TableEditAction::SetAddType("0x83".into()),
    );
    apply_table_edit(&mut ed, Some(&table), TableEditAction::Add);
    apply_table_edit(&mut ed, Some(&table), TableEditAction::Validate);
    let v = table_view(&ed, parts.as_slice(), Some(&table), true);
    assert_eq!(v.rows.len(), 2);
    assert!(v.errors.is_empty(), "{:?}", v.errors);
    assert!(v.can_apply);
    assert!(!ed.edits.is_empty());
    assert!(!table_view(&ed, parts.as_slice(), Some(&table), false).can_apply);

    apply_table_edit(&mut ed, Some(&table), TableEditAction::Delete(0));
    assert!(ed.entries[0].deleted);
    apply_table_edit(&mut ed, Some(&table), TableEditAction::Undo(0));
    assert!(!ed.entries[0].deleted);
}

#[test]
fn the_build_plan_reports_its_layout_and_rows() {
    let mut b = DiskBuilder::new(TableKind::Mbr, 64 * MIB);
    let v = build_view(&b);
    assert_eq!(v.rows.len(), 1);
    assert!(v.layout.is_ok() && v.can_create);
    assert!(!v.names);
    apply_build(
        &mut b,
        BuildAction::SetRowSize {
            row: 0,
            text: "900M".into(),
        },
    );
    let v = build_view(&b);
    assert!(v.layout.is_err() && !v.can_create);
    apply_build(
        &mut b,
        BuildAction::SetRowSize {
            row: 0,
            text: "8M".into(),
        },
    );
    apply_build(&mut b, BuildAction::AddRow);
    assert_eq!(build_view(&b).rows.len(), 2);
    apply_build(&mut b, BuildAction::RemoveRow(1));
    apply_build(&mut b, BuildAction::RemoveRow(0));
    assert_eq!(b.rows.len(), 1, "the last row stays");
    apply_build(&mut b, BuildAction::SetKind(TableKind::Gpt));
    let v = build_view(&b);
    assert!(v.names);
    assert_eq!(v.rows[0].start_lba.is_some(), v.layout.is_ok());
}

#[test]
fn table_rows_offer_their_modes_delete_and_type_note() {
    let table = mbr_table();
    let parts = table.partitions();
    let mut ed = PartitionEditor::new();
    ed.seed_from_with_minimums(parts.as_slice(), &HashMap::new(), Some(64 * MIB));
    let row = table_view(&ed, parts.as_slice(), Some(&table), true).rows[0].clone();
    assert_eq!(
        row.mode_choices(),
        vec![SizeMode::Original, SizeMode::Custom]
    );
    assert!(row.editable() && row.can_delete());
    let mut mins = HashMap::new();
    mins.insert(parts[0].index, MIB);
    ed.seed_from_with_minimums(parts.as_slice(), &mins, Some(64 * MIB));
    let row = table_view(&ed, parts.as_slice(), Some(&table), true).rows[0].clone();
    assert_eq!(
        row.mode_choices(),
        vec![SizeMode::Original, SizeMode::Minimum, SizeMode::Custom]
    );
    apply_table_edit(&mut ed, Some(&table), TableEditAction::Delete(0));
    let row = table_view(&ed, parts.as_slice(), Some(&table), true).rows[0].clone();
    assert!(!row.editable() && !row.can_delete());
    let logical = TableRowView {
        deleted: false,
        logical: true,
        ..row
    };
    assert!(logical.editable() && !logical.can_delete());
    let known = type_choices(TableKind::Mbr)[0];
    assert_eq!(type_note(TableKind::Mbr, known.value), Some(known.label));
    assert_eq!(type_note(TableKind::Mbr, "zz"), Some("custom"));
    assert_eq!(type_note(TableKind::Mbr, " "), None);
}

#[test]
fn alignment_is_picked_by_name_and_typed_only_for_custom() {
    let mut b = DiskBuilder::new(TableKind::Mbr, 64 * MIB);
    let v = build_view(&b);
    assert_eq!((v.align_pick, v.align_custom), (0, false));
    assert_eq!(v.align_choices[0], "Table default (1.0 MiB)");
    let custom = v.align_choices.len() - 1;
    // The DOS track preset sets the text the layout reads.
    apply_build(&mut b, BuildAction::PickAlign(2));
    assert_eq!((b.align_text.as_str(), b.align_bytes()), ("63s", 63 * 512));
    assert_eq!(build_view(&b).align_pick, 2);
    // Custom keeps what was there and opens the field; typed text no preset names stays custom.
    apply_build(&mut b, BuildAction::PickAlign(custom));
    assert!(build_view(&b).align_custom);
    apply_build(&mut b, BuildAction::SetAlign("4K".into()));
    assert_eq!((build_view(&b).align_pick, b.align_bytes()), (custom, 4096));
    apply_build(&mut b, BuildAction::PickAlign(0));
    let v = build_view(&b);
    assert_eq!(
        (v.align_pick, v.align_custom, b.align_text.as_str()),
        (0, false, "")
    );
}
