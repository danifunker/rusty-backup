//! The partition window, a shell over `controller::partition_edit`, for two jobs.
//!
//! [`show_table`] is the Inspect tab's "Edit Partition Table" window; [`show_build`] is the
//! Restore tab's Build Disk window (a table-type picker and a source image per row). They share the
//! type field, the layout bars and the button row. Each draws the controller's plain view and
//! returns the edits the user made this frame plus what the host should do next; the host sends
//! the edits to its controller (`InspectAction::Table`, `RestoreAction::Build`). Both bars place
//! partitions at their real byte offsets with free space between, so they answer "where on the
//! disk does this land". See docs/phase2_commander_plan.md § "Slice 14b".

use eframe::egui;

use rusty_backup::controller::partition_edit::{
    describe_type, format_size, type_choices, type_note, BarSegment, BootStyle, BuildAction,
    BuildView, LayoutBar, SizeMode, TableEditAction, TableEditView, TableKind,
};

/// What the caller should do after a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Stay,
    Close,
    /// Apply Changes / Create Disk: the controller validates, then writes.
    Apply,
}

/// The Inspect tab's table editor. `Apply` comes only while the view says it can.
pub fn show_table(ui: &mut egui::Ui, view: &TableEditView) -> (Vec<TableEditAction>, Action) {
    let mut open = true;
    let mut action = Action::Stay;
    let mut edits = Vec::new();
    let kind = view.kind;
    let muted = super::theme::muted(ui.visuals());

    egui::Window::new("Edit Partition Table")
        .open(&mut open)
        .resizable(true)
        .default_width(760.0)
        .show(ui.ctx(), |ui| {
            ui.label(format!("Table type: {}", kind.label()));
            ui.add_space(4.0);
            show_bar(ui, &view.current);
            ui.add_space(4.0);
            show_bar(ui, &view.after);
            ui.add_space(8.0);

            egui::Grid::new("editor_grid")
                .striped(true)
                .min_col_width(50.0)
                .show(ui, |ui| {
                    for head in [
                        "#",
                        "Type",
                        "Start LBA",
                        "Size Mode",
                        "Size (MiB)",
                        "Boot",
                        "",
                    ] {
                        ui.label(egui::RichText::new(head).strong());
                    }
                    ui.end_row();

                    for (i, row) in view.rows.iter().enumerate() {
                        if row.deleted {
                            ui.label(
                                egui::RichText::new(format!("{}", row.index))
                                    .color(muted)
                                    .strikethrough(),
                            );
                            ui.label(
                                egui::RichText::new(&row.type_name)
                                    .color(muted)
                                    .strikethrough(),
                            );
                            ui.label(
                                egui::RichText::new(format!("{}", row.start_lba)).color(muted),
                            );
                            ui.label("");
                            ui.label(egui::RichText::new("deleted").color(muted));
                            ui.label("");
                            if ui.small_button("Undo").clicked() {
                                edits.push(TableEditAction::Undo(i));
                            }
                            ui.end_row();
                            continue;
                        }
                        if row.extended {
                            ui.label(
                                egui::RichText::new(format!("{} (ext)", row.index)).color(muted),
                            );
                            ui.label(egui::RichText::new(&row.type_name).color(muted));
                            ui.label(
                                egui::RichText::new(format!("{}", row.start_lba)).color(muted),
                            );
                            ui.label("");
                            let mib = row.size_bytes as f64 / (1024.0 * 1024.0);
                            ui.label(egui::RichText::new(format!("{:.2}", mib)).color(muted));
                            ui.label("");
                            ui.label("");
                            ui.end_row();
                            continue;
                        }

                        ui.label(if row.logical {
                            format!("  {}", row.index)
                        } else {
                            format!("{}", row.index)
                        });
                        let mut text = row.type_text.clone();
                        type_field(ui, kind, &format!("ed_type_{}", i), &mut text);
                        if text != row.type_text {
                            edits.push(TableEditAction::SetType { row: i, text });
                        }
                        ui.label(format!("{}", row.start_lba));

                        // Minimum shows only when known; the editor does no filesystem analysis.
                        let mut mode = row.size_mode;
                        ui.horizontal(|ui| {
                            for choice in row.mode_choices() {
                                ui.radio_value(&mut mode, choice, choice.label());
                            }
                        });
                        if mode != row.size_mode {
                            edits.push(TableEditAction::SetSizeMode { row: i, mode });
                        }

                        // Editable only under Custom; the radios stamp the rest.
                        let mut size = row.size_text.clone();
                        ui.add_enabled(
                            row.size_mode == SizeMode::Custom,
                            egui::TextEdit::singleline(&mut size)
                                .desired_width(80.0)
                                .id(egui::Id::unique(format!("ed_size_{}", i))),
                        );
                        if size != row.size_text {
                            edits.push(TableEditAction::SetSizeText { row: i, text: size });
                        }

                        match view.boot_style {
                            BootStyle::Checkbox => {
                                let mut on = row.bootable;
                                if ui.checkbox(&mut on, "").changed() {
                                    edits.push(TableEditAction::SetBootable { row: i, on });
                                }
                            }
                            BootStyle::Radios => {
                                ui.horizontal(|ui| {
                                    let tip = "Whether this RDB partition is eligible to boot. \
                                               Multiple partitions can be set to boot at once - \
                                               the Amiga ROM picks the one with the highest boot \
                                               priority among them.";
                                    for (on, label) in [(true, "boot"), (false, "no boot")] {
                                        if ui
                                            .radio(row.bootable == on, label)
                                            .on_hover_text(tip)
                                            .clicked()
                                        {
                                            edits.push(TableEditAction::SetBootable { row: i, on });
                                        }
                                    }
                                });
                            }
                            BootStyle::Label => {
                                ui.label(if row.bootable { "Yes" } else { "" });
                            }
                        }

                        if !row.can_delete() {
                            ui.label("");
                        } else if ui
                            .small_button("Delete")
                            .on_hover_text("Mark partition for deletion")
                            .clicked()
                        {
                            edits.push(TableEditAction::Delete(i));
                        }
                        ui.end_row();
                    }
                });

            ui.add_space(8.0);
            // Open by default: reaching this window usually means allocating something.
            egui::CollapsingHeader::new("Add Partition")
                .default_open(true)
                .show(ui, |ui| {
                    if view.gaps.is_empty() {
                        ui.label(egui::RichText::new("No unallocated space on this disk.").weak());
                    } else {
                        ui.horizontal_wrapped(|ui| {
                            ui.label("Free space:");
                            for (start_lba, size_bytes, label) in &view.gaps {
                                if ui
                                    .button(label.as_str())
                                    .on_hover_text("Fill this gap with the new partition")
                                    .clicked()
                                {
                                    edits.push(TableEditAction::FillGap {
                                        start_lba: *start_lba,
                                        size_bytes: *size_bytes,
                                    });
                                }
                            }
                        });
                    }
                    ui.horizontal(|ui| {
                        ui.label("Start LBA:");
                        let mut start = view.add_start.clone();
                        ui.add(egui::TextEdit::singleline(&mut start).desired_width(90.0))
                            .on_hover_text(
                                "First 512-byte sector of the new partition. It does not \
                                 have to be past the existing partitions -- any free gap works.",
                            );
                        if start != view.add_start {
                            edits.push(TableEditAction::SetAddStart(start));
                        }
                        ui.label("Size (MiB):");
                        let mut size = view.add_size.clone();
                        ui.add(egui::TextEdit::singleline(&mut size).desired_width(80.0));
                        if size != view.add_size {
                            edits.push(TableEditAction::SetAddSize(size));
                        }
                        ui.label("Type:");
                        let mut ty = view.add_type.clone();
                        type_field(ui, kind, "ed_add_type", &mut ty);
                        if ty != view.add_type {
                            edits.push(TableEditAction::SetAddType(ty));
                        }
                        if view.add_bootable_shown {
                            let mut on = view.add_bootable;
                            if ui.checkbox(&mut on, "Bootable").changed() {
                                edits.push(TableEditAction::SetAddBootable(on));
                            }
                        }
                        if ui.button("Add").clicked() {
                            edits.push(TableEditAction::Add);
                        }
                    });
                    ui.label(
                        egui::RichText::new(format!(
                            "Will be added as partition {} (slot assigned automatically; \
                             a slot freed by a Delete above is reused).",
                            view.next_slot,
                        ))
                        .weak(),
                    );
                });

            ui.add_space(8.0);
            for err in &view.errors {
                ui.colored_label(super::theme::danger(ui.visuals()), err);
            }
            if let Some(status) = &view.status {
                ui.colored_label(super::theme::success(ui.visuals()), status);
            }
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if ui.button("Validate").clicked() {
                    edits.push(TableEditAction::Validate);
                }
                if ui
                    .add_enabled(view.can_apply, egui::Button::new("Apply Changes"))
                    .clicked()
                {
                    action = Action::Apply;
                }
                if ui.button("Cancel").clicked() {
                    action = Action::Close;
                }
            });
        });

    if !open {
        action = Action::Close;
    }
    (edits, action)
}

/// The Build Disk window: table type, one row per partition, a source image per row. Rows carry
/// a size, not a start: the controller places them, so the bar is what will be written.
pub fn show_build(
    ui: &mut egui::Ui,
    view: &BuildView,
    probing: bool,
) -> (Vec<BuildAction>, Action) {
    let mut open = true;
    let mut action = Action::Stay;
    let mut edits = Vec::new();
    let kind = view.kind;

    egui::Window::new("Build Disk")
        .open(&mut open)
        .resizable(true)
        .default_width(880.0)
        .show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                ui.label("Table type:");
                for &k in view.kinds {
                    if ui.radio(kind == k, k.label()).clicked() && k != kind {
                        edits.push(BuildAction::SetKind(k));
                    }
                }
            });
            ui.horizontal(|ui| {
                ui.label("Target size:");
                ui.label(format_size(view.disk_size));
                ui.add_space(12.0);
                ui.label("Alignment:");
                let mut align = view.align_text.clone();
                ui.add(
                    egui::TextEdit::singleline(&mut align)
                        .desired_width(80.0)
                        .hint_text(view.align_hint.as_str()),
                )
                .on_hover_text(
                    "Partition starts are rounded up to this. Blank uses the \
                     table default -- 1 MiB, or one cylinder on SGI / RDB / \
                     Sun. Accepts 1M / 63s (sectors).",
                );
                if align != view.align_text {
                    edits.push(BuildAction::SetAlign(align));
                }
                if let Some((heads, sectors)) = view.geometry {
                    ui.label("Heads:");
                    let mut h = heads;
                    if ui
                        .add(egui::DragValue::new(&mut h).range(1..=255))
                        .changed()
                    {
                        edits.push(BuildAction::SetHeads(h));
                    }
                    ui.label("Sectors/track:");
                    let mut s = sectors;
                    if ui
                        .add(egui::DragValue::new(&mut s).range(1..=1024))
                        .changed()
                    {
                        edits.push(BuildAction::SetSectorsPerTrack(s));
                    }
                }
            });

            ui.add_space(6.0);
            match &view.layout {
                Ok(bar) => show_bar(ui, bar),
                Err(e) => {
                    ui.colored_label(super::theme::danger(ui.visuals()), e);
                }
            }
            ui.add_space(8.0);

            let row_count = view.rows.len();
            egui::Grid::new("builder_grid")
                .striped(true)
                .min_col_width(50.0)
                .show(ui, |ui| {
                    ui.label(egui::RichText::new("#").strong());
                    ui.label(egui::RichText::new("Size").strong());
                    ui.label(egui::RichText::new("Type").strong());
                    if view.names {
                        ui.label(egui::RichText::new("Name").strong());
                    }
                    ui.label(egui::RichText::new("Start LBA").strong());
                    ui.label(egui::RichText::new("Source image").strong());
                    ui.label(egui::RichText::new("").strong());
                    ui.end_row();

                    for (i, row) in view.rows.iter().enumerate() {
                        ui.label(format!("{}", i + 1));
                        let mut size = row.size_text.clone();
                        ui.add(
                            egui::TextEdit::singleline(&mut size)
                                .desired_width(80.0)
                                .id(egui::Id::unique(format!("bld_size_{}", i))),
                        )
                        .on_hover_text(
                            "20M / 1G / a plain byte count, or `rest` for whatever \
                             is left. Only one partition may claim the rest.",
                        );
                        if size != row.size_text {
                            edits.push(BuildAction::SetRowSize { row: i, text: size });
                        }
                        let mut ty = row.type_text.clone();
                        type_field(ui, kind, &format!("bld_type_{}", i), &mut ty);
                        if ty != row.type_text {
                            edits.push(BuildAction::SetRowType { row: i, text: ty });
                        }
                        if view.names {
                            let mut name = row.name.clone();
                            ui.add(
                                egui::TextEdit::singleline(&mut name)
                                    .desired_width(110.0)
                                    .id(egui::Id::unique(format!("bld_name_{}", i))),
                            )
                            .on_hover_text("Entry name. X68000 truncates it to 8 characters.");
                            if name != row.name {
                                edits.push(BuildAction::SetRowName { row: i, text: name });
                            }
                        }
                        match (row.start_lba, row.planned_size) {
                            (Some(lba), Some(size)) => {
                                ui.label(format!("{}", lba))
                                    .on_hover_text(format_size(size));
                            }
                            _ => {
                                ui.label(egui::RichText::new("-").weak());
                            }
                        }
                        ui.horizontal(|ui| {
                            match &row.source {
                                Some(path) => {
                                    let name = path
                                        .file_name()
                                        .map(|n| n.to_string_lossy().to_string())
                                        .unwrap_or_else(|| path.display().to_string());
                                    ui.label(format!(
                                        "{} ({})",
                                        name,
                                        format_size(row.source_size)
                                    ))
                                    .on_hover_text(path.display().to_string());
                                    if ui.small_button("Clear").clicked() {
                                        edits.push(BuildAction::ClearRowSource(i));
                                    }
                                }
                                None => {
                                    ui.label(egui::RichText::new("(empty)").weak());
                                }
                            }
                            if ui
                                .add_enabled(!probing, egui::Button::new("Choose...").small())
                                .on_hover_text(
                                    "Image poured into this partition after the table is \
                                     written. Any format Rusty Backup can read works.",
                                )
                                .clicked()
                            {
                                edits.push(BuildAction::PickRowSource(i));
                            }
                        });
                        ui.horizontal(|ui| {
                            if ui
                                .add_enabled(i > 0, egui::Button::new("Up").small())
                                .clicked()
                            {
                                edits.push(BuildAction::MoveRow { row: i, delta: -1 });
                            }
                            if ui
                                .add_enabled(i + 1 < row_count, egui::Button::new("Down").small())
                                .clicked()
                            {
                                edits.push(BuildAction::MoveRow { row: i, delta: 1 });
                            }
                            if ui
                                .add_enabled(row_count > 1, egui::Button::new("Remove").small())
                                .clicked()
                            {
                                edits.push(BuildAction::RemoveRow(i));
                            }
                        });
                        ui.end_row();
                    }
                });

            ui.add_space(6.0);
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(view.can_add_row, egui::Button::new("Add Partition"))
                    .clicked()
                {
                    edits.push(BuildAction::AddRow);
                }
                if let Some((used, total)) = view.slots {
                    ui.label(egui::RichText::new(format!("{used} of {total} slots used")).weak());
                }
                if probing {
                    ui.label(egui::RichText::new("Measuring the source image...").weak());
                }
            });

            ui.add_space(8.0);
            for err in &view.errors {
                let color = if err.starts_with("Warning:") {
                    super::theme::warning(ui.visuals())
                } else {
                    super::theme::danger(ui.visuals())
                };
                ui.colored_label(color, err);
            }
            if let Some(status) = &view.status {
                ui.colored_label(super::theme::success(ui.visuals()), status);
            }
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if ui.button("Validate").clicked() {
                    edits.push(BuildAction::Validate);
                }
                if ui
                    .add_enabled(
                        view.can_create && !probing,
                        egui::Button::new("Create Disk"),
                    )
                    .on_hover_text(
                        "Writes the partition table, then pours each assigned \
                         image into its partition. This erases the target.",
                    )
                    .clicked()
                {
                    action = Action::Apply;
                }
                if ui.button("Cancel").clicked() {
                    action = Action::Close;
                }
            });
        });

    if !open {
        action = Action::Close;
    }
    (edits, action)
}

/// One layout bar with its caption.
fn show_bar(ui: &mut egui::Ui, bar: &LayoutBar) {
    use super::partition_bar::{PartitionBar, Segment, SegmentKind};
    ui.label(bar.caption.as_str());
    let segments = bar
        .segments
        .iter()
        .map(|s: &BarSegment| Segment {
            label: s.label.clone(),
            fs: s.fs.clone(),
            size_bytes: s.size_bytes,
            kind: match s.color {
                Some(color_index) => SegmentKind::Partition { color_index },
                None => SegmentKind::Free,
            },
        })
        .collect();
    let width = ui.available_width().max(120.0);
    ui.scope(|ui| {
        ui.set_width(width);
        PartitionBar {
            segments,
            show_inline_labels: true,
            show_legend: bar.legend,
        }
        .show(ui);
    });
}

/// Partition-type field: free text plus a catalog dropdown and the resolved type name. The text
/// stays authoritative (the dropdown only stamps a value), so an uncatalogued type is still typed.
fn type_field(ui: &mut egui::Ui, kind: TableKind, id: &str, text: &mut String) {
    ui.horizontal(|ui| {
        ui.add(
            egui::TextEdit::singleline(text)
                .desired_width(if kind == TableKind::Gpt { 250.0 } else { 90.0 })
                .id(egui::Id::unique(id)),
        )
        .on_hover_text(kind.field_hint());

        let choices = type_choices(kind);
        if !choices.is_empty() {
            egui::ComboBox::from_id_salt(format!("{}_pick", id))
                .selected_text("Pick...")
                .width(230.0)
                .show_ui(ui, |ui| {
                    for choice in choices {
                        let selected = describe_type(kind, text).is_some_and(|l| l == choice.label);
                        if ui
                            .selectable_label(
                                selected,
                                format!("{}  -  {}", choice.value, choice.label),
                            )
                            .clicked()
                        {
                            *text = choice.value.to_string();
                        }
                    }
                })
                .response
                .on_hover_text("Choose a well-known partition type");
        }

        if let Some(note) = type_note(kind, text) {
            let custom = describe_type(kind, text).is_none();
            let note = egui::RichText::new(note).weak();
            ui.label(if custom { note.italics() } else { note });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_backup::controller::partition_edit::{BuildRowView, TableRowView};

    fn bar() -> LayoutBar {
        LayoutBar {
            caption: "Layout".into(),
            segments: vec![
                BarSegment {
                    label: String::new(),
                    fs: String::new(),
                    size_bytes: 1 << 20,
                    color: None,
                },
                BarSegment {
                    label: "Partition 1".into(),
                    fs: "FAT16".into(),
                    size_bytes: 8 << 20,
                    color: Some(0),
                },
            ],
            legend: true,
        }
    }

    /// Renders headlessly: guards against egui's panics for a Grid whose rows disagree on cell
    /// count, or an id clash between the two windows' widgets, which no compile catches.
    #[test]
    fn both_windows_render_every_row_shape() {
        let row = |deleted, extended, logical| TableRowView {
            index: 1,
            deleted,
            extended,
            logical,
            start_lba: 2048,
            size_bytes: 8 << 20,
            minimum_size: 1 << 20,
            type_name: "FAT16".into(),
            type_text: "0x06".into(),
            size_text: "8.00".into(),
            size_mode: SizeMode::Original,
            bootable: false,
        };
        for boot_style in [BootStyle::Checkbox, BootStyle::Radios, BootStyle::Label] {
            let view = TableEditView {
                kind: TableKind::Mbr,
                rows: vec![
                    row(false, false, false),
                    row(true, false, false),
                    row(false, true, false),
                    row(false, false, true),
                ],
                boot_style,
                gaps: vec![(18432, 4 << 20, "4 MiB at LBA 18432".into())],
                add_start: String::new(),
                add_size: String::new(),
                add_type: String::new(),
                add_bootable: false,
                add_bootable_shown: true,
                next_slot: 2,
                current: bar(),
                after: bar(),
                errors: vec!["an error".into()],
                status: Some("ok".into()),
                can_apply: false,
            };
            egui::__run_test_ui(|ui| {
                let (edits, action) = show_table(ui, &view);
                assert!(edits.is_empty());
                assert_eq!(action, Action::Stay);
            });
        }
        for (kind, names, geometry, layout) in [
            (TableKind::Mbr, false, None, Ok(bar())),
            (
                TableKind::Gpt,
                true,
                None,
                Err("Layout: too big".to_string()),
            ),
            (TableKind::Sgi, false, Some((16, 63)), Ok(bar())),
        ] {
            let view = BuildView {
                kind,
                kinds: &[TableKind::Mbr, TableKind::Gpt, TableKind::Sgi],
                disk_size: 64 << 20,
                align_text: String::new(),
                align_hint: "1 MiB".into(),
                geometry,
                names,
                rows: vec![BuildRowView {
                    size_text: "rest".into(),
                    type_text: "0x06".into(),
                    name: String::new(),
                    start_lba: Some(2048),
                    planned_size: Some(8 << 20),
                    source: Some(std::path::PathBuf::from("/tmp/example.img")),
                    source_size: 8 << 20,
                }],
                can_add_row: true,
                slots: Some((1, 4)),
                layout,
                errors: vec!["Warning: x".into()],
                status: None,
                can_create: true,
            };
            egui::__run_test_ui(|ui| {
                let (edits, action) = show_build(ui, &view, false);
                assert!(edits.is_empty(), "{}", kind.label());
                assert_eq!(action, Action::Stay);
            });
        }
    }
}
