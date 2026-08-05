//! Partition table editor state.
//!
//! Holds the working copy of partition entries the user is editing in the
//! "Edit Partition Table" popup, plus the derived list of
//! [`PartitionTableEdit`]s and validation results. The view renders text-edits
//! against these fields and calls [`PartitionEditor::build_and_validate`] on
//! the Validate / Apply buttons.
//!
//! Extracted from `gui/inspect_tab.rs` per §5 of `docs/codecleanup.md`.

use crate::partition::editor::{self as pt_editor, PartitionTableEdit};
use crate::partition::type_catalog::{self, TableKind};
use crate::partition::{PartitionInfo, PartitionTable};

/// Working copy of one partition entry in the editor.
#[derive(Debug, Clone)]
pub struct EditorEntry {
    pub index: usize,
    pub type_name: String,
    pub partition_type_byte: u8,
    pub partition_type_string: Option<String>,
    pub start_lba: u64,
    pub size_bytes: u64,
    pub bootable: bool,
    pub is_logical: bool,
    pub is_extended_container: bool,
    /// User-edited size text (in MiB).
    pub size_text: String,
    /// User-edited type text (hex for MBR, string for APM, GUID for GPT).
    pub type_text: String,
    /// Marked for deletion.
    pub deleted: bool,
    /// Added in this editing session, so it has no counterpart in the loaded
    /// table. Tracked explicitly rather than inferred from `index`, because a
    /// new entry may legitimately reclaim the index of one deleted in the
    /// same session — and inferring would turn the add into a resize of the
    /// partition being deleted.
    pub is_new: bool,
    /// Minimum size in bytes (from filesystem analysis). 0 when unknown —
    /// in that case the Minimum radio is hidden so the user can't pick a
    /// value the editor can't honor.
    pub minimum_size: u64,
    /// Quick-pick radio state mirroring the restore tab and resize popup:
    /// `Original` / `Minimum` / `Custom`. Selecting Original/Minimum
    /// re-stamps `size_text`; Custom keeps the existing text editable.
    pub choice: crate::model::size_mode::SizeMode,
}

impl EditorEntry {
    pub fn from_partition(p: &PartitionInfo) -> Self {
        Self::from_partition_with_minimum(p, 0)
    }

    pub fn from_partition_with_minimum(p: &PartitionInfo, minimum_size: u64) -> Self {
        let size_mib = p.size_bytes as f64 / (1024.0 * 1024.0);
        Self {
            index: p.index,
            type_name: p.type_name.clone(),
            partition_type_byte: p.partition_type_byte,
            partition_type_string: p.partition_type_string.clone(),
            start_lba: p.start_lba,
            size_bytes: p.size_bytes,
            bootable: p.bootable,
            is_logical: p.is_logical,
            is_extended_container: p.is_extended_container,
            size_text: format!("{:.2}", size_mib),
            type_text: if let Some(ref ts) = p.partition_type_string {
                ts.clone()
            } else {
                format!("{:02X}", p.partition_type_byte)
            },
            deleted: false,
            is_new: false,
            minimum_size,
            choice: crate::model::size_mode::SizeMode::Original,
        }
    }
}

/// One partition as it will exist once the pending edits apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkingPartition {
    pub index: usize,
    pub start_lba: u64,
    pub size_bytes: u64,
    pub type_name: String,
    pub is_new: bool,
}

impl WorkingPartition {
    pub fn start_byte(&self) -> u64 {
        self.start_lba.saturating_mul(512)
    }

    pub fn end_byte(&self) -> u64 {
        self.start_byte().saturating_add(self.size_bytes)
    }
}

/// Parse a MiB size field into whole sectors. `None` for blank, malformed, or
/// non-positive input.
fn parse_mib(text: &str) -> Option<u64> {
    text.trim()
        .parse::<f64>()
        .ok()
        .filter(|v| *v > 0.0)
        .map(|v| ((v * 1024.0 * 1024.0) as u64 / 512) * 512)
}

/// Mutable state for the partition table editor popup.
#[derive(Default)]
pub struct PartitionEditor {
    /// Working copy of partition entries.
    pub entries: Vec<EditorEntry>,
    /// Edits derived from `entries` on Validate / Apply.
    pub edits: Vec<PartitionTableEdit>,
    /// Validation errors / warnings.
    pub errors: Vec<String>,
    /// Disk size in bytes for validation.
    pub disk_size: u64,
    /// Status message after validate / apply.
    pub status: Option<String>,
    /// Add-partition input fields.
    pub add_start_lba: String,
    pub add_size_mb: String,
    pub add_type: String,
    pub add_bootable: bool,
}

impl PartitionEditor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset state and seed `entries` from the current partition list.
    pub fn seed_from(&mut self, partitions: &[PartitionInfo]) {
        self.seed_from_with_minimums(partitions, &std::collections::HashMap::new(), None);
    }

    /// Reset state and seed entries with per-partition minimum sizes
    /// available. Minimums populate the Min Size column and let the
    /// editor's size-mode radios surface the Minimum option.
    ///
    /// `disk_size` is the real container size. Pass it whenever the caller
    /// knows it — validation, the free-space bar, and GPT's backup-header
    /// placement all key off `self.disk_size`, and the fallback (the end of
    /// the last partition) makes every byte of trailing free space look like
    /// it is past the end of the disk.
    pub fn seed_from_with_minimums(
        &mut self,
        partitions: &[PartitionInfo],
        minimums: &std::collections::HashMap<usize, u64>,
        disk_size: Option<u64>,
    ) {
        self.entries = partitions
            .iter()
            .map(|p| {
                let min = minimums.get(&p.index).copied().unwrap_or(0);
                EditorEntry::from_partition_with_minimum(p, min)
            })
            .collect();
        self.edits.clear();
        self.errors.clear();
        self.status = None;
        self.add_start_lba.clear();
        self.add_size_mb.clear();
        self.add_type.clear();
        self.add_bootable = false;
        let partitions_end = partitions
            .iter()
            .map(|p| (p.start_lba * 512) + p.size_bytes)
            .max()
            .unwrap_or(0);
        self.disk_size = disk_size.filter(|d| *d > 0).unwrap_or(partitions_end);
    }

    /// The index a newly added partition will take, matching what the apply
    /// path actually does on this table flavor.
    ///
    /// MBR and SGI have fixed positional slots and delete by zeroing one, so
    /// a new entry reclaims the lowest free slot — including one freed
    /// earlier in this same editing session. GPT and APM delete by removing
    /// the entry and add by appending, so a new entry lands past the
    /// surviving ones.
    pub fn next_entry_index(&self, kind: TableKind) -> usize {
        let live: Vec<usize> = self
            .entries
            .iter()
            .filter(|e| !e.deleted)
            .map(|e| e.index)
            .collect();
        match kind {
            TableKind::Mbr | TableKind::Sgi => {
                (0..).find(|i| !live.contains(i)).unwrap_or(live.len())
            }
            _ => live.len(),
        }
    }

    /// Append a new entry parsed from the add-partition input fields. Returns
    /// `true` if the inputs parsed and an entry was appended; `false` if the
    /// inputs were malformed (in which case nothing is changed).
    ///
    /// `kind` controls whether the type field is interpreted as a hex byte
    /// (MBR) or an opaque string (everything else), and supplies the friendly
    /// name shown for the pending row in the layout bar.
    pub fn add_entry_from_inputs(&mut self, kind: TableKind) -> bool {
        let (Ok(start), Ok(size_mib)) = (
            self.add_start_lba.trim().parse::<u64>(),
            self.add_size_mb.trim().parse::<f64>(),
        ) else {
            return false;
        };

        let is_mbr = kind == TableKind::Mbr;
        let type_text = self.add_type.trim().to_string();
        let size_bytes = (size_mib * 1024.0 * 1024.0) as u64;
        let type_byte = u8::from_str_radix(type_text.trim_start_matches("0x"), 16).unwrap_or(0x83);
        let next_idx = self.next_entry_index(kind);
        let type_string = if !is_mbr {
            Some(type_text.clone())
        } else {
            None
        };
        let type_name = type_catalog::describe(kind, &type_text)
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                if is_mbr {
                    format!("0x{:02X}", type_byte)
                } else {
                    type_text.clone()
                }
            });

        self.entries.push(EditorEntry {
            index: next_idx,
            type_name,
            partition_type_byte: type_byte,
            partition_type_string: type_string,
            start_lba: start,
            size_bytes,
            bootable: self.add_bootable,
            is_logical: false,
            is_extended_container: false,
            size_text: format!("{:.2}", size_mib),
            type_text,
            deleted: false,
            is_new: true,
            minimum_size: 0,
            choice: crate::model::size_mode::SizeMode::Custom,
        });
        self.add_start_lba.clear();
        self.add_size_mb.clear();
        self.add_type.clear();
        self.add_bootable = false;
        true
    }

    /// The disk layout as it will stand once the pending edits apply:
    /// deleted rows dropped, edited sizes parsed, in disk order.
    ///
    /// Extended containers are omitted so their logical children account for
    /// the space instead of double-counting it. A size field that doesn't
    /// parse falls back to the entry's current size, so a half-typed number
    /// doesn't make the layout jump around.
    pub fn working_layout(&self) -> Vec<WorkingPartition> {
        let mut layout: Vec<WorkingPartition> = self
            .entries
            .iter()
            .filter(|e| !e.deleted && !e.is_extended_container)
            .map(|e| WorkingPartition {
                index: e.index,
                start_lba: e.start_lba,
                size_bytes: parse_mib(&e.size_text).unwrap_or(e.size_bytes),
                type_name: e.type_name.clone(),
                is_new: e.is_new,
            })
            .collect();
        layout.sort_by_key(|p| p.start_lba);
        layout
    }

    /// The not-yet-added partition described by the Add-Partition inputs, so
    /// the layout bar can preview it before the user commits with Add.
    pub fn pending_add(&self, kind: TableKind) -> Option<WorkingPartition> {
        let start_lba = self.add_start_lba.trim().parse::<u64>().ok()?;
        let size_bytes = parse_mib(&self.add_size_mb)?;
        let type_text = self.add_type.trim();
        Some(WorkingPartition {
            index: self.next_entry_index(kind),
            start_lba,
            size_bytes,
            type_name: type_catalog::describe(kind, type_text)
                .unwrap_or(type_text)
                .to_string(),
            is_new: true,
        })
    }

    /// Unallocated gaps left by the pending layout, in disk order. Drives the
    /// editor's free-space shortcuts, so a hole opened by marking a partition
    /// Delete becomes fillable without leaving the popup.
    pub fn free_gaps(&self) -> Vec<crate::partition::FreeRegion> {
        crate::partition::free_regions_from_ranges(
            self.working_layout()
                .iter()
                .filter(|p| p.size_bytes > 0)
                .map(|p| {
                    let start = p.start_lba.saturating_mul(512);
                    (start, start.saturating_add(p.size_bytes))
                }),
            self.disk_size,
        )
    }

    /// Diff `entries` against `table`'s original partitions to produce the
    /// list of `PartitionTableEdit`s, then validate against the disk size.
    /// Populates `edits`, `errors`, and `status`.
    pub fn build_and_validate(&mut self, table: &PartitionTable) {
        self.edits.clear();
        self.errors.clear();
        self.status = None;

        let orig_partitions = table.partitions();

        for entry in &self.entries {
            let orig = if entry.is_new {
                None
            } else {
                orig_partitions.iter().find(|p| p.index == entry.index)
            };

            if entry.deleted {
                if orig.is_some() {
                    self.edits
                        .push(PartitionTableEdit::DeleteEntry { index: entry.index });
                }
                continue;
            }

            if let Some(orig) = orig {
                // Skip the size diff entirely when the displayed text still
                // matches the original. `size_text` is initialized as
                // `"{:.2}"` MiB, which throws away precision on partitions
                // whose byte count isn't a clean MiB multiple (notably RDB /
                // Amiga geometries). Round-tripping that string back to
                // bytes shifts the partition's end by up to ~5 KiB, which is
                // enough to push a tail partition past the disk and emit a
                // spurious "extends beyond disk" validation error when the
                // user hasn't actually touched anything.
                let orig_size_text = format!("{:.2}", orig.size_bytes as f64 / (1024.0 * 1024.0));
                if entry.size_text.trim() != orig_size_text {
                    let new_size_mib: f64 = entry.size_text.trim().parse().unwrap_or(-1.0);
                    if new_size_mib > 0.0 {
                        let new_size_bytes = (new_size_mib * 1024.0 * 1024.0) as u64;
                        let new_size_bytes = (new_size_bytes / 512) * 512;
                        if new_size_bytes != orig.size_bytes {
                            self.edits.push(PartitionTableEdit::ResizeEntry {
                                index: entry.index,
                                new_size_bytes,
                            });
                        }
                    } else {
                        self.errors.push(format!(
                            "Invalid size for partition {}: '{}'",
                            entry.index, entry.size_text
                        ));
                    }
                }

                match table {
                    PartitionTable::Mbr(_) => {
                        let new_byte =
                            u8::from_str_radix(entry.type_text.trim().trim_start_matches("0x"), 16);
                        match new_byte {
                            Ok(b) if b != orig.partition_type_byte => {
                                self.edits.push(PartitionTableEdit::ChangeType {
                                    index: entry.index,
                                    new_type_byte: b,
                                    new_type_string: None,
                                });
                            }
                            Err(_) => {
                                self.errors.push(format!(
                                    "Invalid hex type for partition {}: '{}'",
                                    entry.index, entry.type_text
                                ));
                            }
                            _ => {}
                        }
                    }
                    PartitionTable::Gpt { .. } | PartitionTable::Apm(_)
                        if entry.type_text.trim()
                            != orig.partition_type_string.as_deref().unwrap_or("") =>
                    {
                        self.edits.push(PartitionTableEdit::ChangeType {
                            index: entry.index,
                            new_type_byte: 0,
                            new_type_string: Some(entry.type_text.trim().to_string()),
                        });
                    }
                    PartitionTable::Rdb(_) if entry.bootable != orig.bootable => {
                        self.edits.push(PartitionTableEdit::SetBootable {
                            index: entry.index,
                            bootable: entry.bootable,
                        });
                    }
                    _ => {}
                }
            } else {
                let type_byte =
                    u8::from_str_radix(entry.type_text.trim().trim_start_matches("0x"), 16)
                        .unwrap_or(0x83);
                let type_string = if entry.partition_type_string.is_some() {
                    Some(entry.type_text.trim().to_string())
                } else {
                    None
                };
                self.edits.push(PartitionTableEdit::AddEntry {
                    start_lba: entry.start_lba,
                    size_bytes: entry.size_bytes,
                    partition_type: type_byte,
                    type_string,
                    bootable: entry.bootable,
                });
            }
        }

        if self.errors.is_empty() {
            match pt_editor::validate_edits(table, &self.edits, self.disk_size) {
                Ok(warnings) => {
                    for w in warnings {
                        self.errors.push(format!("Warning: {}", w));
                    }
                    if self.edits.is_empty() {
                        self.status = Some("No changes detected.".to_string());
                    } else {
                        self.status = Some(format!(
                            "Validation OK — {} edit(s) ready to apply.",
                            self.edits.len()
                        ));
                    }
                }
                Err(e) => {
                    self.errors.push(format!("Validation error: {}", e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::mbr::Mbr;

    const MIB: u64 = 1024 * 1024;

    fn info(index: usize, start_lba: u64, size_bytes: u64, type_byte: u8) -> PartitionInfo {
        PartitionInfo {
            index,
            type_name: format!("0x{:02X}", type_byte),
            partition_type_byte: type_byte,
            start_lba,
            start_byte: None,
            size_bytes,
            bootable: false,
            is_logical: false,
            is_extended_container: false,
            partition_type_string: None,
            hfs_block_size: None,
            rdb_part_block: None,
            drv_name: None,
        }
    }

    /// Three 10 MiB partitions at 1 / 21 / 31 MiB on a 100 MiB disk: a 10 MiB
    /// hole between #1 and #2, #2 and #3 adjacent, 59 MiB at the tail.
    fn seeded_editor() -> (PartitionEditor, Vec<PartitionInfo>) {
        let parts = vec![
            info(0, 2048, 10 * MIB, 0x83),
            info(1, 2048 + (20 * MIB / 512), 10 * MIB, 0x83),
            info(2, 2048 + (30 * MIB / 512), 10 * MIB, 0x83),
        ];
        let mut editor = PartitionEditor::new();
        editor.seed_from_with_minimums(&parts, &Default::default(), Some(100 * MIB));
        (editor, parts)
    }

    #[test]
    fn seeded_disk_size_uses_the_container_not_the_last_partition() {
        let (editor, _) = seeded_editor();
        assert_eq!(editor.disk_size, 100 * MIB);

        // Without a container size we still fall back to the partitions.
        let parts = vec![info(0, 2048, 10 * MIB, 0x83)];
        let mut fallback = PartitionEditor::new();
        fallback.seed_from_with_minimums(&parts, &Default::default(), None);
        assert_eq!(fallback.disk_size, MIB + 10 * MIB);
    }

    #[test]
    fn free_gaps_sees_the_hole_between_partitions() {
        let (editor, _) = seeded_editor();
        let gaps = editor.free_gaps();
        // Head-of-disk table area, the 10 MiB hole, and the tail.
        assert_eq!(gaps.len(), 3);
        assert_eq!(gaps[1].start_lba, 2048 + (10 * MIB / 512));
        assert_eq!(gaps[1].size_bytes, 10 * MIB);
        assert_eq!(gaps[2].size_bytes, 59 * MIB);
    }

    #[test]
    fn deleting_a_middle_partition_opens_a_bigger_gap() {
        let (mut editor, _) = seeded_editor();
        editor.entries[1].deleted = true;
        let gaps = editor.free_gaps();
        // The existing hole and the freed partition merge into one 20 MiB gap.
        let middle = gaps
            .iter()
            .find(|g| g.start_lba == 2048 + (10 * MIB / 512))
            .expect("merged middle gap");
        assert_eq!(middle.size_bytes, 20 * MIB);
        let biggest = gaps.iter().max_by_key(|g| g.size_bytes).unwrap();
        assert_eq!(biggest.size_bytes, 59 * MIB);
    }

    #[test]
    fn working_layout_is_in_disk_order_not_entry_order() {
        let (mut editor, _) = seeded_editor();
        // Add a partition into the middle hole; it is appended to `entries`
        // but belongs second on the disk.
        editor.add_start_lba = format!("{}", 2048 + (10 * MIB / 512));
        editor.add_size_mb = "10".to_string();
        editor.add_type = "83".to_string();
        assert!(editor.add_entry_from_inputs(TableKind::Mbr));

        let layout = editor.working_layout();
        assert_eq!(layout.len(), 4);
        assert_eq!(
            layout.iter().map(|p| p.start_lba).collect::<Vec<_>>(),
            vec![
                2048,
                2048 + (10 * MIB / 512),
                2048 + (20 * MIB / 512),
                2048 + (30 * MIB / 512),
            ],
        );
        // The new row is the one in the hole, and it filled it exactly.
        assert!(layout[1].is_new);
        assert_eq!(layout[1].size_bytes, 10 * MIB);
        assert!(!editor.free_gaps().iter().any(|g| g.size_bytes == 10 * MIB));
    }

    #[test]
    fn pending_add_previews_at_its_real_offset() {
        let (mut editor, _) = seeded_editor();
        editor.add_start_lba = format!("{}", 2048 + (10 * MIB / 512));
        editor.add_size_mb = "10".to_string();
        editor.add_type = "AF".to_string();

        let pending = editor.pending_add(TableKind::Mbr).expect("preview");
        assert_eq!(pending.start_lba, 2048 + (10 * MIB / 512));
        assert_eq!(pending.size_bytes, 10 * MIB);
        assert_eq!(pending.type_name, "Apple HFS / HFS+");

        // A half-typed size yields no preview rather than a zero-width one.
        editor.add_size_mb = String::new();
        assert!(editor.pending_add(TableKind::Mbr).is_none());
    }

    #[test]
    fn mbr_reuses_a_deleted_slot_and_gpt_appends() {
        let (mut editor, _) = seeded_editor();
        editor.entries[1].deleted = true;
        assert_eq!(editor.next_entry_index(TableKind::Mbr), 1);
        // GPT / APM compact on delete, so a new entry lands after the rest.
        assert_eq!(editor.next_entry_index(TableKind::Gpt), 2);
    }

    fn mbr_table(parts: &[PartitionInfo]) -> PartitionTable {
        let mut raw = [0u8; 512];
        raw[510] = 0x55;
        raw[511] = 0xAA;
        for p in parts {
            let off = 446 + p.index * 16;
            raw[off + 4] = p.partition_type_byte;
            raw[off + 8..off + 12].copy_from_slice(&(p.start_lba as u32).to_le_bytes());
            raw[off + 12..off + 16].copy_from_slice(&((p.size_bytes / 512) as u32).to_le_bytes());
        }
        PartitionTable::Mbr(Mbr::parse(&raw).expect("test MBR parses"))
    }

    #[test]
    fn refilling_a_deleted_slot_emits_delete_plus_add_not_a_resize() {
        // The regression this guards: a new partition reclaiming index 1 used
        // to match the deleted partition 1 and be diffed as an edit of it.
        let (mut editor, parts) = seeded_editor();
        let table = mbr_table(&parts);

        editor.entries[1].deleted = true;
        editor.add_start_lba = format!("{}", 2048 + (10 * MIB / 512));
        editor.add_size_mb = "20".to_string();
        editor.add_type = "83".to_string();
        assert!(editor.add_entry_from_inputs(TableKind::Mbr));
        assert_eq!(editor.entries.last().unwrap().index, 1);

        editor.build_and_validate(&table);
        assert!(editor.errors.is_empty(), "errors: {:?}", editor.errors);
        assert_eq!(editor.edits.len(), 2);
        assert!(matches!(
            editor.edits[0],
            PartitionTableEdit::DeleteEntry { index: 1 },
        ));
        match &editor.edits[1] {
            PartitionTableEdit::AddEntry {
                start_lba,
                size_bytes,
                ..
            } => {
                assert_eq!(*start_lba, 2048 + (10 * MIB / 512));
                assert_eq!(*size_bytes, 20 * MIB);
            }
            other => panic!("expected AddEntry, got {other:?}"),
        }
    }

    #[test]
    fn adding_into_trailing_free_space_validates() {
        // With the old partitions-derived disk size this failed with
        // "extends beyond disk" for every trailing byte.
        let (mut editor, parts) = seeded_editor();
        let table = mbr_table(&parts);
        editor.add_start_lba = format!("{}", 2048 + (50 * MIB / 512));
        editor.add_size_mb = "40".to_string();
        editor.add_type = "AF".to_string();
        assert!(editor.add_entry_from_inputs(TableKind::Mbr));

        editor.build_and_validate(&table);
        assert!(editor.errors.is_empty(), "errors: {:?}", editor.errors);
        assert_eq!(editor.edits.len(), 1);
    }
}
