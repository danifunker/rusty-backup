//! The "Edit Partition Table" modal, shared across tabs.
//!
//! Extracted verbatim from `gui/inspect_tab.rs` so a second caller (the Restore
//! tab's build-a-disk flow) can reuse the same window rather than growing a
//! parallel partition UI — the pattern `gui/size_mode_row.rs` already follows.
//!
//! The widget owns no state: it renders a caller-held
//! [`PartitionEditor`](rusty_backup::model::partition_editor::PartitionEditor)
//! against the loaded partition list and returns an [`Action`] for the caller
//! to act on. Applying edits stays with the caller, which owns the device
//! handle and the worker thread.
//!
//! Both layout bars place partitions at their real byte offsets with
//! unallocated gaps drawn as free space, so the bar answers "where on the disk
//! does this land", not merely "in what order are the entries".

use eframe::egui;

use rusty_backup::model::partition_editor::PartitionEditor;
use rusty_backup::partition::type_catalog;
use rusty_backup::partition::{self, PartitionInfo, PartitionTable};

/// What the caller should do after a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Stay,
    Close,
    /// Validation passed and edits are pending; write them to the target.
    Apply,
}

/// Render the modal. `allow_apply` gates the Apply button for callers that
/// have no writable target (the inspect tab passes `false` for a backup folder).
pub fn show(
    ui: &mut egui::Ui,
    editor: &mut PartitionEditor,
    partitions: &[PartitionInfo],
    table: Option<&PartitionTable>,
    allow_apply: bool,
) -> Action {
    let mut open = true;
    let mut action = Action::Stay;

    let kind = table
        .map(type_catalog::kind_of)
        .unwrap_or(type_catalog::TableKind::Other);
    let table_type = kind.label();

    egui::Window::new("Edit Partition Table")
        .open(&mut open)
        .resizable(true)
        .default_width(760.0)
        .show(ui.ctx(), |ui| {
            ui.label(format!("Table type: {}", table_type));
            ui.add_space(4.0);

            show_disk_layout_bars(ui, partitions, editor, kind);
            ui.add_space(8.0);

            egui::Grid::new("editor_grid")
                .striped(true)
                .min_col_width(50.0)
                .show(ui, |ui| {
                    ui.label(egui::RichText::new("#").strong());
                    ui.label(egui::RichText::new("Type").strong());
                    ui.label(egui::RichText::new("Start LBA").strong());
                    ui.label(egui::RichText::new("Size Mode").strong());
                    ui.label(egui::RichText::new("Size (MiB)").strong());
                    ui.label(egui::RichText::new("Boot").strong());
                    ui.label(egui::RichText::new("").strong());
                    ui.end_row();

                    for i in 0..editor.entries.len() {
                        // Copy values we need for display to avoid holding an
                        // immutable borrow across mutable TextEdit borrows.
                        let idx = editor.entries[i].index;
                        let deleted = editor.entries[i].deleted;
                        let is_ext = editor.entries[i].is_extended_container;
                        let is_logical = editor.entries[i].is_logical;
                        let start_lba = editor.entries[i].start_lba;
                        let size_bytes = editor.entries[i].size_bytes;
                        let bootable = editor.entries[i].bootable;
                        let type_name = editor.entries[i].type_name.clone();

                        if deleted {
                            ui.label(
                                egui::RichText::new(format!("{}", idx))
                                    .color(egui::Color32::GRAY)
                                    .strikethrough(),
                            );
                            ui.label(
                                egui::RichText::new(&type_name)
                                    .color(egui::Color32::GRAY)
                                    .strikethrough(),
                            );
                            ui.label(
                                egui::RichText::new(format!("{}", start_lba))
                                    .color(egui::Color32::GRAY),
                            );
                            ui.label("");
                            ui.label(egui::RichText::new("deleted").color(egui::Color32::GRAY));
                            ui.label("");
                            if ui.small_button("Undo").clicked() {
                                editor.entries[i].deleted = false;
                            }
                            ui.end_row();
                            continue;
                        }

                        if is_ext {
                            ui.label(
                                egui::RichText::new(format!("{} (ext)", idx))
                                    .color(egui::Color32::GRAY),
                            );
                            ui.label(egui::RichText::new(&type_name).color(egui::Color32::GRAY));
                            ui.label(
                                egui::RichText::new(format!("{}", start_lba))
                                    .color(egui::Color32::GRAY),
                            );
                            ui.label("");
                            let size_mib = size_bytes as f64 / (1024.0 * 1024.0);
                            ui.label(
                                egui::RichText::new(format!("{:.2}", size_mib))
                                    .color(egui::Color32::GRAY),
                            );
                            ui.label("");
                            ui.label("");
                            ui.end_row();
                            continue;
                        }

                        let label = if is_logical {
                            format!("  {}", idx)
                        } else {
                            format!("{}", idx)
                        };
                        ui.label(label);

                        // Type field: free-form text plus a catalog picker.
                        type_field(
                            ui,
                            kind,
                            &format!("ed_type_{}", i),
                            &mut editor.entries[i].type_text,
                        );

                        // Start LBA (read-only)
                        ui.label(format!("{}", start_lba));

                        // Minimum is hidden when the per-partition min size
                        // isn't known; the editor does no FS analysis itself.
                        {
                            use rusty_backup::model::size_mode::SizeMode;
                            let minimum_size = editor.entries[i].minimum_size;
                            let original_size = size_bytes;
                            let prev = editor.entries[i].choice;
                            ui.horizontal(|ui| {
                                ui.radio_value(
                                    &mut editor.entries[i].choice,
                                    SizeMode::Original,
                                    "Original",
                                );
                                if minimum_size > 0 && minimum_size < original_size {
                                    ui.radio_value(
                                        &mut editor.entries[i].choice,
                                        SizeMode::Minimum,
                                        "Minimum",
                                    );
                                }
                                ui.radio_value(
                                    &mut editor.entries[i].choice,
                                    SizeMode::Custom,
                                    "Custom",
                                );
                            });
                            if editor.entries[i].choice != prev {
                                match editor.entries[i].choice {
                                    SizeMode::Original => {
                                        editor.entries[i].size_text = format!(
                                            "{:.2}",
                                            original_size as f64 / (1024.0 * 1024.0),
                                        );
                                    }
                                    SizeMode::Minimum => {
                                        editor.entries[i].size_text = format!(
                                            "{:.2}",
                                            minimum_size as f64 / (1024.0 * 1024.0),
                                        );
                                    }
                                    _ => {}
                                }
                            }
                        }

                        // Editable only under Custom; the radios stamp the rest.
                        let size_editable = matches!(
                            editor.entries[i].choice,
                            rusty_backup::model::size_mode::SizeMode::Custom,
                        );
                        let size_id = format!("ed_size_{}", i);
                        ui.add_enabled(
                            size_editable,
                            egui::TextEdit::singleline(&mut editor.entries[i].size_text)
                                .desired_width(80.0)
                                .id(egui::Id::new(&size_id)),
                        );

                        // MBR gets a checkbox, RDB explicit radios so the choice
                        // can't be flipped by accident; GPT/APM have no such bit.
                        if table_type == "MBR" {
                            ui.checkbox(&mut editor.entries[i].bootable, "");
                        } else if table_type == "RDB" {
                            ui.horizontal(|ui| {
                                let tip = "Whether this RDB partition is eligible to boot. \
                                           Multiple partitions can be set to boot at once — \
                                           the Amiga ROM picks the one with the highest boot \
                                           priority among them.";
                                let mut val = editor.entries[i].bootable;
                                if ui
                                    .radio_value(&mut val, true, "boot")
                                    .on_hover_text(tip)
                                    .changed()
                                {
                                    editor.entries[i].bootable = val;
                                }
                                if ui
                                    .radio_value(&mut val, false, "no boot")
                                    .on_hover_text(tip)
                                    .changed()
                                {
                                    editor.entries[i].bootable = val;
                                }
                            });
                        } else {
                            ui.label(if bootable { "Yes" } else { "" });
                        }

                        if !is_logical {
                            if ui
                                .small_button("Delete")
                                .on_hover_text("Mark partition for deletion")
                                .clicked()
                            {
                                editor.entries[i].deleted = true;
                            }
                        } else {
                            ui.label("");
                        }

                        ui.end_row();
                    }
                });

            ui.add_space(8.0);

            // Open by default — reaching this popup usually means the user
            // wants to allocate something, and collapsed hid the only way to.
            egui::CollapsingHeader::new("Add Partition")
                .default_open(true)
                .show(ui, |ui| {
                    // Free gaps, so a hole left by a delete is fillable without
                    // hand-computing the LBA.
                    let gaps = editor.free_gaps();
                    if gaps.is_empty() {
                        ui.label(egui::RichText::new("No unallocated space on this disk.").weak());
                    } else {
                        ui.horizontal_wrapped(|ui| {
                            ui.label("Free space:");
                            for gap in &gaps {
                                let label = format!(
                                    "{} at LBA {}",
                                    partition::format_size(gap.size_bytes),
                                    gap.start_lba,
                                );
                                if ui
                                    .button(label)
                                    .on_hover_text("Fill this gap with the new partition")
                                    .clicked()
                                {
                                    editor.add_start_lba = gap.start_lba.to_string();
                                    editor.add_size_mb =
                                        format!("{}", (gap.size_bytes / (1024 * 1024)).max(1));
                                }
                            }
                        });
                    }

                    ui.horizontal(|ui| {
                        ui.label("Start LBA:");
                        ui.add(
                            egui::TextEdit::singleline(&mut editor.add_start_lba)
                                .desired_width(90.0),
                        )
                        .on_hover_text(
                            "First 512-byte sector of the new partition. It does not \
                             have to be past the existing partitions -- any free gap works.",
                        );
                        ui.label("Size (MiB):");
                        ui.add(
                            egui::TextEdit::singleline(&mut editor.add_size_mb).desired_width(80.0),
                        );
                        ui.label("Type:");
                        type_field(ui, kind, "ed_add_type", &mut editor.add_type);
                        if kind == type_catalog::TableKind::Mbr {
                            ui.checkbox(&mut editor.add_bootable, "Bootable");
                        }
                        if ui.button("Add").clicked() {
                            editor.add_entry_from_inputs(kind);
                        }
                    });

                    ui.label(
                        egui::RichText::new(format!(
                            "Will be added as partition {} (slot assigned automatically; \
                             a slot freed by a Delete above is reused).",
                            editor.next_entry_index(kind) + 1,
                        ))
                        .weak(),
                    );
                });

            ui.add_space(8.0);

            for err in &editor.errors {
                ui.colored_label(egui::Color32::from_rgb(255, 100, 100), err);
            }
            if let Some(status) = &editor.status {
                ui.colored_label(egui::Color32::from_rgb(100, 255, 100), status);
            }

            ui.add_space(4.0);

            ui.horizontal(|ui| {
                if ui.button("Validate").clicked() {
                    if let Some(t) = table {
                        editor.build_and_validate(t);
                    }
                }

                let can_apply = editor.errors.is_empty() && table.is_some() && allow_apply;
                if ui
                    .add_enabled(can_apply, egui::Button::new("Apply Changes"))
                    .clicked()
                {
                    if let Some(t) = table {
                        editor.build_and_validate(t);
                    }
                    if editor.errors.is_empty() && !editor.edits.is_empty() {
                        action = Action::Apply;
                    }
                }

                if ui.button("Cancel").clicked() {
                    action = Action::Close;
                }
            });
        });

    if !open {
        return Action::Close;
    }
    action
}

/// Partition-type entry field: free-form text plus a catalog dropdown and the
/// resolved type name.
///
/// The text field stays authoritative — the dropdown only stamps a value into
/// it — so a type that isn't in the catalog is still reachable by typing it.
fn type_field(ui: &mut egui::Ui, kind: type_catalog::TableKind, id: &str, text: &mut String) {
    ui.horizontal(|ui| {
        ui.add(
            egui::TextEdit::singleline(text)
                .desired_width(if kind == type_catalog::TableKind::Gpt {
                    250.0
                } else {
                    90.0
                })
                .id(egui::Id::new(id)),
        )
        .on_hover_text(kind.field_hint());

        let choices = type_catalog::choices(kind);
        if !choices.is_empty() {
            egui::ComboBox::from_id_salt(format!("{}_pick", id))
                .selected_text("Pick...")
                .width(230.0)
                .show_ui(ui, |ui| {
                    for choice in choices {
                        let selected =
                            type_catalog::describe(kind, text).is_some_and(|l| l == choice.label);
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

        match type_catalog::describe(kind, text) {
            Some(label) => {
                ui.label(egui::RichText::new(label).weak());
            }
            None if !text.trim().is_empty() => {
                ui.label(egui::RichText::new("custom").weak().italics());
            }
            None => {}
        }
    });
}

/// A partition's place on the disk, for the editor's layout bars.
struct PlacedSegment {
    label: String,
    fs: String,
    start_byte: u64,
    size_bytes: u64,
    color_index: usize,
}

/// Lay `placed` out across a `disk_size`-byte disk, inserting `Free` segments
/// for every unallocated gap.
///
/// Input must be sorted by `start_byte`; an entry that starts before the
/// previous one ends (an overlap not yet validated away) is drawn straight
/// after it rather than producing a negative gap.
fn place_segments(placed: &[PlacedSegment], disk_size: u64) -> Vec<super::partition_bar::Segment> {
    use super::partition_bar::{Segment, SegmentKind};

    let mut segments = Vec::new();
    let mut cursor = 0u64;
    for p in placed {
        if p.start_byte > cursor {
            segments.push(Segment {
                label: String::new(),
                fs: String::new(),
                size_bytes: p.start_byte - cursor,
                kind: SegmentKind::Free,
            });
        }
        segments.push(Segment {
            label: p.label.clone(),
            fs: p.fs.clone(),
            size_bytes: p.size_bytes,
            kind: SegmentKind::Partition {
                color_index: p.color_index,
            },
        });
        cursor = cursor.max(p.start_byte.saturating_add(p.size_bytes));
    }
    if disk_size > cursor {
        segments.push(Segment {
            label: String::new(),
            fs: String::new(),
            size_bytes: disk_size - cursor,
            kind: SegmentKind::Free,
        });
    }
    segments
}

/// Render the Current vs After PartitionBar pair.
pub fn show_disk_layout_bars(
    ui: &mut egui::Ui,
    partitions: &[PartitionInfo],
    editor: &PartitionEditor,
    kind: type_catalog::TableKind,
) {
    use super::partition_bar::PartitionBar;

    // Color by partition index, not position, so a row keeps its color between
    // the two bars even when an insertion shifts disk order.
    let color_of = |index: usize| index;

    let mut current: Vec<PlacedSegment> = partitions
        .iter()
        .filter(|p| !p.is_extended_container)
        .map(|p| PlacedSegment {
            label: format!("Partition {}", p.index + 1),
            fs: p.type_name.clone(),
            start_byte: p.byte_offset(),
            size_bytes: p.size_bytes,
            color_index: color_of(p.index),
        })
        .collect();
    current.sort_by_key(|p| p.start_byte);

    let mut after: Vec<PlacedSegment> = editor
        .working_layout()
        .into_iter()
        .map(|p| PlacedSegment {
            label: if p.is_new {
                format!("New (part {})", p.index + 1)
            } else {
                format!("Partition {}", p.index + 1)
            },
            fs: p.type_name.clone(),
            start_byte: p.start_byte(),
            size_bytes: p.size_bytes,
            color_index: color_of(p.index),
        })
        .collect();
    // Pending Add-Partition inputs preview in place, before Add commits them.
    if let Some(pending) = editor.pending_add(kind) {
        after.push(PlacedSegment {
            label: "New".to_string(),
            fs: pending.type_name.clone(),
            start_byte: pending.start_byte(),
            size_bytes: pending.size_bytes,
            color_index: color_of(pending.index),
        });
    }
    after.sort_by_key(|p| p.start_byte);

    // Overlaps or entries past the end make the layout wider than the disk;
    // scale both bars to that so the overflow stays visible, not clipped.
    let allocated = |segs: &[PlacedSegment]| -> u64 { segs.iter().map(|p| p.size_bytes).sum() };
    let extent = |segs: &[PlacedSegment]| -> u64 {
        segs.iter()
            .map(|p| p.start_byte.saturating_add(p.size_bytes))
            .max()
            .unwrap_or(0)
    };
    let disk_size = editor
        .disk_size
        .max(extent(&current))
        .max(extent(&after))
        .max(1);

    let available_width = ui.available_width().max(120.0);
    let current_free = disk_size.saturating_sub(allocated(&current));
    let after_free = disk_size.saturating_sub(allocated(&after));

    ui.label(format!(
        "Current  (disk {}, {} allocated, {} free):",
        partition::format_size(disk_size),
        partition::format_size(allocated(&current)),
        partition::format_size(current_free),
    ));
    ui.scope(|ui| {
        ui.set_width(available_width);
        PartitionBar {
            segments: place_segments(&current, disk_size),
            show_inline_labels: true,
            show_legend: false,
        }
        .show(ui);
    });

    ui.add_space(4.0);
    ui.label(format!(
        "After  ({} allocated, {} free):",
        partition::format_size(allocated(&after)),
        partition::format_size(after_free),
    ));
    ui.scope(|ui| {
        ui.set_width(available_width);
        PartitionBar {
            segments: place_segments(&after, disk_size),
            show_inline_labels: true,
            show_legend: true,
        }
        .show(ui);
    });
}
