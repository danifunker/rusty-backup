//! Planning a fresh table on a blank target: Restore's Build Disk window.

use std::path::PathBuf;

use super::{allocated, place_segments, LayoutBar, Placed, TableKind};
use crate::model::disk_builder::DiskBuilder;
use crate::partition::provision;
use disk_core::display::format_size;

/// User intents in the Build Disk window; `row` is the position in [`BuildView::rows`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildAction {
    SetKind(TableKind),
    SetAlign(String),
    SetHeads(u16),
    SetSectorsPerTrack(u16),
    SetRowSize {
        row: usize,
        text: String,
    },
    SetRowType {
        row: usize,
        text: String,
    },
    SetRowName {
        row: usize,
        text: String,
    },
    /// "Choose..." for a row's source image: a picker, then its decoded size in a job.
    PickRowSource(usize),
    ClearRowSource(usize),
    MoveRow {
        row: usize,
        delta: isize,
    },
    RemoveRow(usize),
    AddRow,
    Validate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildRowView {
    pub size_text: String,
    pub type_text: String,
    pub name: String,
    /// The planned start, when the layout places.
    pub start_lba: Option<u64>,
    pub planned_size: Option<u64>,
    pub source: Option<PathBuf>,
    pub source_size: u64,
}

/// Everything the Build Disk window draws.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildView {
    pub kind: TableKind,
    /// The table kinds a disk can be built with.
    pub kinds: &'static [TableKind],
    pub disk_size: u64,
    pub align_text: String,
    /// The alignment a blank field means.
    pub align_hint: String,
    /// Heads / sectors per track are recorded in this table.
    pub geometry: Option<(u16, u16)>,
    /// The Name column is shown.
    pub names: bool,
    pub rows: Vec<BuildRowView>,
    pub can_add_row: bool,
    /// "N of M slots used", for tables with a fixed slot count.
    pub slots: Option<(usize, usize)>,
    /// The planned layout, or why it cannot be placed.
    pub layout: Result<LayoutBar, String>,
    pub errors: Vec<String>,
    pub status: Option<String>,
    /// Create Disk is enabled (the layout places).
    pub can_create: bool,
}

/// Carry out one intent on the plan; `PickRowSource` is the host's (it needs a picker and a job).
pub fn apply_build(builder: &mut DiskBuilder, action: BuildAction) {
    match action {
        BuildAction::SetKind(kind) => builder.set_kind(kind),
        BuildAction::SetAlign(t) => builder.align_text = t,
        BuildAction::SetHeads(h) => builder.geometry.heads = h.clamp(1, 255),
        BuildAction::SetSectorsPerTrack(s) => builder.geometry.sectors_per_track = s.clamp(1, 1024),
        BuildAction::SetRowSize { row, text } => {
            if let Some(r) = builder.rows.get_mut(row) {
                r.size_text = text;
            }
        }
        BuildAction::SetRowType { row, text } => {
            if let Some(r) = builder.rows.get_mut(row) {
                r.type_text = text;
            }
        }
        BuildAction::SetRowName { row, text } => {
            if let Some(r) = builder.rows.get_mut(row) {
                r.name = text;
            }
        }
        BuildAction::PickRowSource(_) => {}
        BuildAction::ClearRowSource(row) => {
            if let Some(r) = builder.rows.get_mut(row) {
                r.source = None;
                r.source_size = 0;
            }
        }
        BuildAction::MoveRow { row, delta } => builder.move_row(row, delta),
        BuildAction::RemoveRow(row) => {
            if builder.rows.len() > 1 {
                builder.remove_row(row);
            }
        }
        BuildAction::AddRow => {
            if builder.can_add_row() {
                builder.add_row();
            }
        }
        BuildAction::Validate => {
            builder.validate();
        }
    }
}

/// A picked source image and its decoded size land on the row.
pub(crate) fn set_row_source(builder: &mut DiskBuilder, row: usize, path: PathBuf, size: u64) {
    if let Some(r) = builder.rows.get_mut(row) {
        r.source = Some(path);
        r.source_size = size;
    }
}

/// The plan as plain data.
pub(crate) fn build_view(builder: &DiskBuilder) -> BuildView {
    let planned = builder.plan();
    let placed = planned.as_ref().ok();
    let rows = builder
        .rows
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let p = placed.and_then(|v| v.get(i));
            BuildRowView {
                size_text: r.size_text.clone(),
                type_text: r.type_text.clone(),
                name: r.name.clone(),
                start_lba: p.map(|p| p.start_lba),
                planned_size: p.map(|p| p.size_bytes),
                source: r.source.clone(),
                source_size: r.source_size,
            }
        })
        .collect::<Vec<_>>();
    let layout = match &planned {
        Ok(v) => {
            let segs: Vec<Placed> = v
                .iter()
                .enumerate()
                .map(|(i, p)| Placed {
                    label: format!("Partition {}", i + 1),
                    fs: super::describe_type(builder.kind, p.type_text.as_str())
                        .unwrap_or(p.type_text.as_str())
                        .to_string(),
                    start_byte: p.start_byte(),
                    size_bytes: p.size_bytes,
                    color: i,
                })
                .collect();
            let used = allocated(segs.as_slice());
            Ok(LayoutBar {
                caption: format!(
                    "Layout  (disk {}, {} allocated, {} free):",
                    format_size(builder.disk_size),
                    format_size(used),
                    format_size(builder.disk_size.saturating_sub(used)),
                ),
                segments: place_segments(segs.as_slice(), builder.disk_size.max(1)),
                legend: true,
            })
        }
        Err(e) => Err(format!("Layout: {e:#}")),
    };
    let n = rows.len();
    BuildView {
        kind: builder.kind,
        kinds: provision::WRITABLE_TABLES,
        disk_size: builder.disk_size,
        align_text: builder.align_text.clone(),
        align_hint: format_size(builder.align_bytes()),
        geometry: provision::records_geometry(builder.kind)
            .then_some((builder.geometry.heads, builder.geometry.sectors_per_track)),
        names: provision::carries_entry_name(builder.kind),
        rows,
        can_add_row: builder.can_add_row(),
        slots: builder.remaining_slots().map(|left| (n, n + left)),
        can_create: planned.is_ok(),
        layout,
        errors: builder.errors.clone(),
        status: builder.status.clone(),
    }
}
