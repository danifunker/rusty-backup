//! The partition window, a shell over `controller::partition_edit`.
//!
//! [`show_table`] is the Inspect tab's "Edit Partition Table" window. It draws the controller's
//! plain view and returns the edits the user made this frame plus what the host should do next;
//! the host sends the edits to `InspectAction::Table`. The layout bar and the type field are shared
//! with the Build Disk tab. Bars place partitions at their real byte offsets with free space
//! between, so they answer "where on the disk does this land". See
//! docs/phase2_commander_plan.md § "Slice 14b".

use eframe::egui;

use rusty_backup::controller::partition_edit::{
    describe_type, type_choices, type_note, BarSegment, BootStyle, LayoutBar, SizeMode,
    TableEditAction, TableEditView, TableKind,
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
pub fn show_table(
    ui: &mut egui::Ui,
    view: &TableEditView,
    target: Option<&str>,
) -> (Vec<TableEditAction>, Action) {
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
            if let Some(target) = target {
                ui.label(egui::RichText::new(format!("Writes to: {target}")).strong());
            }
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

/// One layout bar with its caption.
pub(crate) fn show_bar(ui: &mut egui::Ui, bar: &LayoutBar) {
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
            partition: None,
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
pub(crate) fn type_field(ui: &mut egui::Ui, kind: TableKind, id: &str, text: &mut String) {
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
    use rusty_backup::controller::partition_edit::TableRowView;

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
    /// count, which no compile catches.
    #[test]
    fn the_table_window_renders_every_row_shape() {
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
                let (edits, action) = show_table(ui, &view, Some("disk.img"));
                assert!(edits.is_empty());
                assert_eq!(action, Action::Stay);
            });
        }
    }
}
