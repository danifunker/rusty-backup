//! The partition window's rules, for its two jobs: editing the table on a loaded disk (Inspect's
//! "Edit Partition Table") and planning a fresh one (Restore's Build Disk).
//!
//! The editing state lives in the model objects the hosting controllers already own and hand to
//! their apply jobs: `model::partition_editor::PartitionEditor` (held by `InspectController`) and
//! `model::disk_builder::DiskBuilder` (held by `RestoreController`). This module turns user intents
//! ([`TableEditAction`], [`BuildAction`]) into edits of those objects and turns the objects into
//! plain data a view draws ([`TableEditView`], [`BuildView`]): rows, free gaps, the layout bars'
//! segments, the type catalog. The hosts forward `InspectAction::Table` / `RestoreAction::Build`
//! here. The egui window (`src/gui/partition_editor_modal.rs`) is its shell; see
//! docs/phase2_commander_plan.md § "Slice 14b".

mod build;
mod table;

#[cfg(test)]
mod tests;

pub use crate::model::size_mode::SizeMode;
pub use crate::partition::type_catalog::{TableKind, TypeChoice};
pub use build::{apply_build, BuildAction, BuildRowView, BuildView};
pub(crate) use build::{build_view, set_row_source};
pub use disk_core::display::format_size;
pub(crate) use table::table_view;
pub use table::{apply_table_edit, BootStyle, TableEditAction, TableEditView, TableRowView};

/// Stable widget ids, shared by every shell's accessibility identifiers.
pub mod ids {
    pub const TABLE_ADD: &str = "partition_edit.add";
    pub const TABLE_VALIDATE: &str = "partition_edit.validate";
    pub const TABLE_APPLY: &str = "partition_edit.apply";
    pub const TABLE_ROW_TYPE: &str = "partition_edit.row_type";
    pub const TABLE_ROW_MODE: &str = "partition_edit.row_mode";
    pub const TABLE_ROW_SIZE: &str = "partition_edit.row_size";
    pub const TABLE_ROW_BOOT: &str = "partition_edit.row_boot";
    pub const TABLE_ROW_DELETE: &str = "partition_edit.row_delete";
    pub const TABLE_ROW_UNDO: &str = "partition_edit.row_undo";
    pub const TABLE_FREE_GAP: &str = "partition_edit.free_gap";
    pub const TABLE_ADD_START: &str = "partition_edit.add_start";
    pub const TABLE_ADD_SIZE: &str = "partition_edit.add_size";
    pub const TABLE_ADD_TYPE: &str = "partition_edit.add_type";
    pub const TABLE_ADD_BOOT: &str = "partition_edit.add_boot";
    pub const BUILD_KIND: &str = "partition_edit.build_kind";
    pub const BUILD_ADD_ROW: &str = "partition_edit.build_add_row";
    pub const BUILD_VALIDATE: &str = "partition_edit.build_validate";
    pub const BUILD_CREATE: &str = "partition_edit.build_create";
    pub const CANCEL: &str = "partition_edit.cancel";
}

/// The well-known types a table kind offers in its picker.
pub fn type_choices(kind: TableKind) -> &'static [TypeChoice] {
    crate::partition::type_catalog::choices(kind)
}

/// The catalog's name for a typed value, if it knows it.
pub fn describe_type(kind: TableKind, text: &str) -> Option<&'static str> {
    crate::partition::type_catalog::describe(kind, text)
}

/// The note beside a type field: the catalog's name, "custom" for an unknown value, or nothing.
pub fn type_note(kind: TableKind, text: &str) -> Option<&'static str> {
    match describe_type(kind, text) {
        Some(label) => Some(label),
        None if !text.trim().is_empty() => Some("custom"),
        None => None,
    }
}

/// One run of a layout bar: a partition (with its colour slot) or free space (`color: None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarSegment {
    pub label: String,
    pub fs: String,
    pub size_bytes: u64,
    pub color: Option<usize>,
}

/// A layout bar: its caption and segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayoutBar {
    pub caption: String,
    pub segments: Vec<BarSegment>,
    /// Draw the legend under the bar.
    pub legend: bool,
}

impl LayoutBar {
    /// The bar's segments as Inspect draws them: a colour is a partition, none is free space.
    pub fn layout_segments(&self) -> Vec<crate::controller::inspect::LayoutSegment> {
        use crate::controller::inspect::{LayoutSegment, SegmentKind};
        let each = self.segments.iter().map(|s| LayoutSegment {
            label: s.label.clone(),
            fs: s.fs.clone(),
            size_bytes: s.size_bytes,
            kind: match s.color {
                Some(color_index) => SegmentKind::Partition { color_index },
                None => SegmentKind::Free,
            },
            partition: None,
        });
        each.collect()
    }

    /// The bar's total size in bytes, for drawing two bars to one scale.
    pub fn total_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.size_bytes).sum()
    }
}

/// A partition's place on the disk, before the gaps are filled in.
struct Placed {
    label: String,
    fs: String,
    start_byte: u64,
    size_bytes: u64,
    color: usize,
}

/// Lay `placed` (sorted by start) across the disk, with free segments for every gap; an overlap
/// is drawn straight after the previous entry rather than as a negative gap.
fn place_segments(placed: &[Placed], disk_size: u64) -> Vec<BarSegment> {
    let free = |size_bytes| BarSegment {
        label: String::new(),
        fs: String::new(),
        size_bytes,
        color: None,
    };
    let mut segments = Vec::new();
    let mut cursor = 0u64;
    for p in placed {
        if p.start_byte > cursor {
            segments.push(free(p.start_byte - cursor));
        }
        segments.push(BarSegment {
            label: p.label.clone(),
            fs: p.fs.clone(),
            size_bytes: p.size_bytes,
            color: Some(p.color),
        });
        cursor = cursor.max(p.start_byte.saturating_add(p.size_bytes));
    }
    if disk_size > cursor {
        segments.push(free(disk_size - cursor));
    }
    segments
}

fn allocated(placed: &[Placed]) -> u64 {
    placed.iter().map(|p| p.size_bytes).sum()
}
