//! Editing the table already on a disk: Inspect's "Edit Partition Table" window.

use super::{allocated, place_segments, LayoutBar, Placed, SizeMode, TableKind};
use crate::model::partition_editor::PartitionEditor;
use crate::partition::{PartitionInfo, PartitionTable};
use disk_core::display::format_size;

/// User intents in the table editor; `row` is the position in [`TableEditView::rows`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableEditAction {
    SetType {
        row: usize,
        text: String,
    },
    /// Original / Minimum stamp the size field; Custom makes it editable.
    SetSizeMode {
        row: usize,
        mode: SizeMode,
    },
    SetSizeText {
        row: usize,
        text: String,
    },
    SetBootable {
        row: usize,
        on: bool,
    },
    /// Mark for deletion.
    Delete(usize),
    Undo(usize),
    /// Put a free gap's start and size into the Add fields.
    FillGap {
        start_lba: u64,
        size_bytes: u64,
    },
    SetAddStart(String),
    SetAddSize(String),
    SetAddType(String),
    SetAddBootable(bool),
    Add,
    Validate,
}

/// How a row's boot flag is drawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootStyle {
    /// MBR's active flag.
    Checkbox,
    /// RDB's boot / no boot, explicit so it cannot flip by accident.
    Radios,
    /// No editable flag: "Yes" or nothing.
    Label,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRowView {
    pub index: usize,
    pub deleted: bool,
    pub extended: bool,
    pub logical: bool,
    pub start_lba: u64,
    pub size_bytes: u64,
    /// Offered as Minimum only when known and smaller than the size.
    pub minimum_size: u64,
    pub type_name: String,
    pub type_text: String,
    pub size_text: String,
    pub size_mode: SizeMode,
    pub bootable: bool,
}

impl TableRowView {
    /// The size modes the row offers: Minimum only when known and smaller than the size.
    pub fn mode_choices(&self) -> Vec<SizeMode> {
        let mut modes = vec![SizeMode::Original];
        if self.minimum_size > 0 && self.minimum_size < self.size_bytes {
            modes.push(SizeMode::Minimum);
        }
        modes.push(SizeMode::Custom);
        modes
    }

    /// Type, size and boot flag are editable: not deleted, not an extended container.
    pub fn editable(&self) -> bool {
        !self.deleted && !self.extended
    }

    /// Delete applies: a primary partition (a logical one or the container stays).
    pub fn can_delete(&self) -> bool {
        !self.deleted && !self.extended && !self.logical
    }
}

/// Everything the editor window draws.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableEditView {
    pub kind: TableKind,
    pub rows: Vec<TableRowView>,
    pub boot_style: BootStyle,
    /// Free gaps as (start LBA, size, "size at LBA n").
    pub gaps: Vec<(u64, u64, String)>,
    pub add_start: String,
    pub add_size: String,
    pub add_type: String,
    pub add_bootable: bool,
    /// MBR offers a Bootable box beside Add.
    pub add_bootable_shown: bool,
    /// 1-based slot the next Add takes.
    pub next_slot: usize,
    pub current: LayoutBar,
    pub after: LayoutBar,
    pub errors: Vec<String>,
    pub status: Option<String>,
    /// Apply Changes is enabled (the last validation passed and the target is writable).
    pub can_apply: bool,
}

/// The table kind the editor works in.
pub(crate) fn kind_of(table: Option<&PartitionTable>) -> TableKind {
    table
        .map(crate::partition::type_catalog::kind_of)
        .unwrap_or(TableKind::Other)
}

/// Carry out one intent on the editor (`table` is what Validate checks against).
pub fn apply_table_edit(
    editor: &mut PartitionEditor,
    table: Option<&PartitionTable>,
    action: TableEditAction,
) {
    let kind = kind_of(table);
    let mib = |b: u64| format!("{:.2}", b as f64 / (1024.0 * 1024.0));
    match action {
        TableEditAction::SetType { row, text } => {
            if let Some(e) = editor.entries.get_mut(row) {
                e.type_text = text;
            }
        }
        TableEditAction::SetSizeMode { row, mode } => {
            if let Some(e) = editor.entries.get_mut(row) {
                if e.choice != mode {
                    e.choice = mode;
                    match mode {
                        SizeMode::Original => e.size_text = mib(e.size_bytes),
                        SizeMode::Minimum => e.size_text = mib(e.minimum_size),
                        SizeMode::Custom | SizeMode::MinPlus20 | SizeMode::FillRemaining => {}
                    }
                }
            }
        }
        TableEditAction::SetSizeText { row, text } => {
            if let Some(e) = editor.entries.get_mut(row) {
                e.size_text = text;
            }
        }
        TableEditAction::SetBootable { row, on } => {
            if let Some(e) = editor.entries.get_mut(row) {
                e.bootable = on;
            }
        }
        TableEditAction::Delete(row) => {
            if let Some(e) = editor.entries.get_mut(row) {
                if !e.is_logical && !e.is_extended_container {
                    e.deleted = true;
                }
            }
        }
        TableEditAction::Undo(row) => {
            if let Some(e) = editor.entries.get_mut(row) {
                e.deleted = false;
            }
        }
        TableEditAction::FillGap {
            start_lba,
            size_bytes,
        } => {
            editor.add_start_lba = start_lba.to_string();
            editor.add_size_mb = format!("{}", (size_bytes / (1024 * 1024)).max(1));
        }
        TableEditAction::SetAddStart(t) => editor.add_start_lba = t,
        TableEditAction::SetAddSize(t) => editor.add_size_mb = t,
        TableEditAction::SetAddType(t) => editor.add_type = t,
        TableEditAction::SetAddBootable(on) => editor.add_bootable = on,
        TableEditAction::Add => {
            editor.add_entry_from_inputs(kind);
        }
        TableEditAction::Validate => {
            if let Some(t) = table {
                editor.build_and_validate(t);
            }
        }
    }
}

/// The editor as plain data; `writable` is false for a target that cannot be written.
pub fn table_view(
    editor: &PartitionEditor,
    partitions: &[PartitionInfo],
    table: Option<&PartitionTable>,
    writable: bool,
) -> TableEditView {
    let kind = kind_of(table);
    let rows = editor
        .entries
        .iter()
        .map(|e| TableRowView {
            index: e.index,
            deleted: e.deleted,
            extended: e.is_extended_container,
            logical: e.is_logical,
            start_lba: e.start_lba,
            size_bytes: e.size_bytes,
            minimum_size: e.minimum_size,
            type_name: e.type_name.clone(),
            type_text: e.type_text.clone(),
            size_text: e.size_text.clone(),
            size_mode: e.choice,
            bootable: e.bootable,
        })
        .collect();
    let boot_style = match kind.label() {
        "MBR" => BootStyle::Checkbox,
        "RDB" => BootStyle::Radios,
        _ => BootStyle::Label,
    };
    let gaps = editor
        .free_gaps()
        .into_iter()
        .map(|g| {
            let label = format!("{} at LBA {}", format_size(g.size_bytes), g.start_lba);
            (g.start_lba, g.size_bytes, label)
        })
        .collect();
    let (current, after) = layout_bars(editor, partitions, kind);
    TableEditView {
        kind,
        rows,
        boot_style,
        gaps,
        add_start: editor.add_start_lba.clone(),
        add_size: editor.add_size_mb.clone(),
        add_type: editor.add_type.clone(),
        add_bootable: editor.add_bootable,
        add_bootable_shown: kind == TableKind::Mbr,
        next_slot: editor.next_entry_index(kind) + 1,
        current,
        after,
        errors: editor.errors.clone(),
        status: editor.status.clone(),
        can_apply: editor.errors.is_empty() && table.is_some() && writable,
    }
}

/// The Current and After bars, both scaled to the wider of the disk and either layout.
fn layout_bars(
    editor: &PartitionEditor,
    partitions: &[PartitionInfo],
    kind: TableKind,
) -> (LayoutBar, LayoutBar) {
    // Colour by partition index, so a row keeps its colour between the bars.
    let mut current: Vec<Placed> = partitions
        .iter()
        .filter(|p| !p.is_extended_container)
        .map(|p| Placed {
            label: format!("Partition {}", p.index + 1),
            fs: p.type_name.clone(),
            start_byte: p.byte_offset(),
            size_bytes: p.size_bytes,
            color: p.index,
        })
        .collect();
    current.sort_by_key(|p| p.start_byte);
    let mut after: Vec<Placed> = editor
        .working_layout()
        .into_iter()
        .map(|p| Placed {
            label: if p.is_new {
                format!("New (part {})", p.index + 1)
            } else {
                format!("Partition {}", p.index + 1)
            },
            fs: p.type_name.clone(),
            start_byte: p.start_byte(),
            size_bytes: p.size_bytes,
            color: p.index,
        })
        .collect();
    // Pending Add inputs preview in place, before Add commits them.
    if let Some(pending) = editor.pending_add(kind) {
        after.push(Placed {
            label: "New".to_string(),
            fs: pending.type_name.clone(),
            start_byte: pending.start_byte(),
            size_bytes: pending.size_bytes,
            color: pending.index,
        });
    }
    after.sort_by_key(|p| p.start_byte);
    let extent = |v: &[Placed]| {
        v.iter()
            .map(|p| p.start_byte.saturating_add(p.size_bytes))
            .max()
            .unwrap_or(0)
    };
    // Overlaps or entries past the end widen the bars, so the overflow shows instead of clipping.
    let disk = editor
        .disk_size
        .max(extent(current.as_slice()))
        .max(extent(after.as_slice()))
        .max(1);
    let (ca, aa) = (allocated(current.as_slice()), allocated(after.as_slice()));
    let current_bar = LayoutBar {
        caption: format!(
            "Current  (disk {}, {} allocated, {} free):",
            format_size(disk),
            format_size(ca),
            format_size(disk.saturating_sub(ca)),
        ),
        segments: place_segments(current.as_slice(), disk),
        legend: false,
    };
    let after_bar = LayoutBar {
        caption: format!(
            "After  ({} allocated, {} free):",
            format_size(aa),
            format_size(disk.saturating_sub(aa)),
        ),
        segments: place_segments(after.as_slice(), disk),
        legend: true,
    };
    (current_bar, after_bar)
}
