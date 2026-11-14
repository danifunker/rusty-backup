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
    /// An entry of [`BuildView::align_choices`]; the last one opens the typed field.
    PickAlign(usize),
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
    /// The MBR active flag or the RDB bootable flag.
    SetRowBootable {
        row: usize,
        on: bool,
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
    pub bootable: bool,
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
    /// The named alignments, the table's default first and "Custom..." last.
    pub align_choices: Vec<String>,
    pub align_pick: usize,
    /// The typed alignment field is shown (the pick is "Custom...").
    pub align_custom: bool,
    /// Heads / sectors per track are recorded in this table.
    pub geometry: Option<(u16, u16)>,
    /// The Name column is shown.
    pub names: bool,
    /// The Boot column is shown: `Some(true)` when only one row may be bootable.
    pub boot: Option<bool>,
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
    /// The layout comes from a donor's label: the table, slots, types and names are fixed.
    pub fixed: bool,
}

/// The named alignments after the table's default: the `--align` text and what it suits.
pub const ALIGN_PRESETS: [(&str, &str); 3] = [
    ("1M", "1 MiB (Windows Vista and later, Linux, flash cards)"),
    ("63s", "63 sectors, one track (DOS, Windows 9x)"),
    ("1s", "None (any sector)"),
];

/// The alignment list: the table's default, the presets, then "Custom...".
fn align_choices(builder: &DiskBuilder) -> Vec<String> {
    let bytes = provision::default_align(builder.kind, builder.geometry);
    let default = match provision::uses_cylinder_geometry(builder.kind) {
        true => format!("Table default (one cylinder, {})", format_size(bytes)),
        false => format!("Table default ({})", format_size(bytes)),
    };
    let presets = ALIGN_PRESETS.iter().map(|(_, label)| label.to_string());
    std::iter::once(default)
        .chain(presets)
        .chain(std::iter::once("Custom...".to_string()))
        .collect()
}

/// Where the plan's alignment sits in [`align_choices`]: typed text no preset names is custom.
fn align_pick(builder: &DiskBuilder) -> usize {
    let custom = ALIGN_PRESETS.len() + 1;
    let text = builder.align_text.trim();
    if builder.align_custom {
        return custom;
    }
    if text.is_empty() {
        return 0;
    }
    let preset = ALIGN_PRESETS
        .iter()
        .position(|(t, _)| t.eq_ignore_ascii_case(text));
    preset.map_or(custom, |i| i + 1)
}

/// Carry out one intent on the plan; `PickRowSource` is the host's (it needs a picker and a job).
pub fn apply_build(builder: &mut DiskBuilder, action: BuildAction) {
    // A label donor keeps its table and slots; only sizes, bootable flags and content change.
    if builder.fixed_slots()
        && matches!(
            action,
            BuildAction::SetKind(_)
                | BuildAction::SetRowType { .. }
                | BuildAction::SetRowName { .. }
                | BuildAction::MoveRow { .. }
                | BuildAction::RemoveRow(_)
                | BuildAction::AddRow
                | BuildAction::SetAlign(_)
                | BuildAction::PickAlign(_)
                | BuildAction::SetHeads(_)
                | BuildAction::SetSectorsPerTrack(_)
        )
    {
        return;
    }
    match action {
        BuildAction::SetKind(kind) => builder.set_kind(kind),
        BuildAction::SetAlign(t) => builder.align_text = t,
        BuildAction::PickAlign(i) => match i {
            0 => (builder.align_text, builder.align_custom) = (String::new(), false),
            i if i <= ALIGN_PRESETS.len() => {
                builder.align_text = ALIGN_PRESETS[i - 1].0.to_string();
                builder.align_custom = false;
            }
            _ => builder.align_custom = true,
        },
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
        BuildAction::SetRowBootable { row, on } => {
            // MBR has one active flag: setting one clears the others.
            if on && provision::bootable_rule(builder.kind) == Some(true) {
                builder.rows.iter_mut().for_each(|r| r.bootable = false);
            }
            if let Some(r) = builder.rows.get_mut(row) {
                r.bootable = on;
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
                bootable: r.bootable,
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
        align_choices: align_choices(builder),
        align_pick: align_pick(builder),
        align_custom: align_pick(builder) == ALIGN_PRESETS.len() + 1,
        geometry: provision::records_geometry(builder.kind)
            .then_some((builder.geometry.heads, builder.geometry.sectors_per_track)),
        names: provision::carries_entry_name(builder.kind),
        boot: provision::bootable_rule(builder.kind),
        rows,
        can_add_row: builder.can_add_row() && !builder.fixed_slots(),
        fixed: builder.fixed_slots(),
        slots: builder.remaining_slots().map(|left| (n, n + left)),
        can_create: planned.is_ok(),
        layout,
        errors: builder.errors.clone(),
        status: builder.status.clone(),
    }
}
