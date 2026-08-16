//! The partition modal, shared across tabs and across two jobs.
//!
//! [`Mode::EditExisting`] is the Inspect tab's "Edit Partition Table" window,
//! extracted from `gui/inspect_tab.rs`. [`Mode::BuildNew`] is the Restore tab's
//! Build Disk flow: the same window, plus a table-type picker and a source
//! image per row, laying a fresh table on a blank target. They share the type
//! field, the layout bar and the button row rather than growing a second
//! partition UI — the pattern `gui/size_mode_row.rs` already follows.
//!
//! The widget owns no state: it renders a caller-held
//! [`PartitionEditor`](rusty_backup::model::partition_editor::PartitionEditor)
//! or [`DiskBuilder`](rusty_backup::model::disk_builder::DiskBuilder) and
//! returns an [`Action`] for the caller to act on. Applying stays with the
//! caller, which owns the device handle and the worker thread.
//!
//! Both layout bars place partitions at their real byte offsets with
//! unallocated gaps drawn as free space, so the bar answers "where on the disk
//! does this land", not merely "in what order are the entries".

use eframe::egui;

use rusty_backup::model::disk_builder::{BuilderRow, DiskBuilder};
use rusty_backup::model::partition_editor::PartitionEditor;
use rusty_backup::partition::provision;
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

/// Which job the modal is doing this frame.
pub enum Mode<'a> {
    /// Edit the table already on a loaded disk.
    EditExisting {
        editor: &'a mut PartitionEditor,
        partitions: &'a [PartitionInfo],
        table: Option<&'a PartitionTable>,
    },
    /// Lay a fresh table on a blank target, optionally filling partitions.
    BuildNew { builder: &'a mut DiskBuilder },
}

/// Render the modal. `allow_apply` gates the Apply button for callers that
/// have no writable target (the inspect tab passes `false` for a backup folder).
pub fn show(ui: &mut egui::Ui, mode: Mode<'_>, allow_apply: bool) -> Action {
    match mode {
        Mode::EditExisting {
            editor,
            partitions,
            table,
        } => show_edit_existing(ui, editor, partitions, table, allow_apply),
        Mode::BuildNew { builder } => show_build_new(ui, builder, allow_apply),
    }
}

fn show_edit_existing(
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
                                    .color(super::theme::muted(ui.visuals()))
                                    .strikethrough(),
                            );
                            ui.label(
                                egui::RichText::new(&type_name)
                                    .color(super::theme::muted(ui.visuals()))
                                    .strikethrough(),
                            );
                            ui.label(
                                egui::RichText::new(format!("{}", start_lba))
                                    .color(super::theme::muted(ui.visuals())),
                            );
                            ui.label("");
                            ui.label(
                                egui::RichText::new("deleted")
                                    .color(super::theme::muted(ui.visuals())),
                            );
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
                                    .color(super::theme::muted(ui.visuals())),
                            );
                            ui.label(
                                egui::RichText::new(&type_name)
                                    .color(super::theme::muted(ui.visuals())),
                            );
                            ui.label(
                                egui::RichText::new(format!("{}", start_lba))
                                    .color(super::theme::muted(ui.visuals())),
                            );
                            ui.label("");
                            let size_mib = size_bytes as f64 / (1024.0 * 1024.0);
                            ui.label(
                                egui::RichText::new(format!("{:.2}", size_mib))
                                    .color(super::theme::muted(ui.visuals())),
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
                ui.colored_label(super::theme::danger(ui.visuals()), err);
            }
            if let Some(status) = &editor.status {
                ui.colored_label(super::theme::success(ui.visuals()), status);
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

/// The Build Disk window: table-type picker, one row per partition, and a
/// source image per row.
///
/// Rows carry a size *string*, not a start LBA — `provision::place` derives the
/// layout every frame, so the preview bar is always what will actually be
/// written and the user never hand-computes an offset.
fn show_build_new(ui: &mut egui::Ui, builder: &mut DiskBuilder, allow_apply: bool) -> Action {
    let mut open = true;
    let mut action = Action::Stay;
    // Deferred so the row list isn't mutated while it is being iterated.
    let mut remove_row: Option<usize> = None;
    let mut move_row: Option<(usize, isize)> = None;

    egui::Window::new("Build Disk")
        .open(&mut open)
        .resizable(true)
        .default_width(880.0)
        .show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                ui.label("Table type:");
                let mut kind = builder.kind;
                for &k in provision::WRITABLE_TABLES {
                    ui.radio_value(&mut kind, k, k.label());
                }
                builder.set_kind(kind);
            });
            ui.horizontal(|ui| {
                ui.label("Target size:");
                ui.label(partition::format_size(builder.disk_size));
                ui.add_space(12.0);
                ui.label("Alignment:");
                let align_hint = partition::format_size(builder.align_bytes());
                ui.add(
                    egui::TextEdit::singleline(&mut builder.align_text)
                        .desired_width(80.0)
                        .hint_text(align_hint),
                )
                .on_hover_text(
                    "Partition starts are rounded up to this. Blank uses the \
                     table default -- 1 MiB, or one cylinder on SGI / RDB / \
                     Sun. Accepts 1M / 63s (sectors).",
                );
                if provision::uses_cylinder_geometry(builder.kind) {
                    ui.label("Heads:");
                    ui.add(egui::DragValue::new(&mut builder.geometry.heads).range(1..=255));
                    ui.label("Sectors/track:");
                    ui.add(
                        egui::DragValue::new(&mut builder.geometry.sectors_per_track)
                            .range(1..=1024),
                    );
                }
            });

            ui.add_space(6.0);
            show_planned_layout_bar(ui, builder);
            ui.add_space(8.0);

            let shows_name = provision::carries_entry_name(builder.kind);
            let planned = builder.plan().unwrap_or_default();
            let row_count = builder.rows.len();

            egui::Grid::new("builder_grid")
                .striped(true)
                .min_col_width(50.0)
                .show(ui, |ui| {
                    ui.label(egui::RichText::new("#").strong());
                    ui.label(egui::RichText::new("Size").strong());
                    ui.label(egui::RichText::new("Type").strong());
                    if shows_name {
                        ui.label(egui::RichText::new("Name").strong());
                    }
                    ui.label(egui::RichText::new("Start LBA").strong());
                    ui.label(egui::RichText::new("Source image").strong());
                    ui.label(egui::RichText::new("").strong());
                    ui.end_row();

                    for i in 0..row_count {
                        ui.label(format!("{}", i + 1));

                        ui.add(
                            egui::TextEdit::singleline(&mut builder.rows[i].size_text)
                                .desired_width(80.0)
                                .id(egui::Id::new(format!("bld_size_{}", i))),
                        )
                        .on_hover_text(
                            "20M / 1G / a plain byte count, or `rest` for whatever \
                             is left. Only one partition may claim the rest.",
                        );

                        type_field(
                            ui,
                            builder.kind,
                            &format!("bld_type_{}", i),
                            &mut builder.rows[i].type_text,
                        );

                        if shows_name {
                            ui.add(
                                egui::TextEdit::singleline(&mut builder.rows[i].name)
                                    .desired_width(110.0)
                                    .id(egui::Id::new(format!("bld_name_{}", i))),
                            )
                            .on_hover_text("Entry name. X68000 truncates it to 8 characters.");
                        }

                        match planned.get(i) {
                            Some(p) => {
                                ui.label(format!("{}", p.start_lba))
                                    .on_hover_text(partition::format_size(p.size_bytes));
                            }
                            None => {
                                ui.label(egui::RichText::new("-").weak());
                            }
                        }

                        source_cell(ui, &mut builder.rows[i], i);

                        ui.horizontal(|ui| {
                            if ui
                                .add_enabled(i > 0, egui::Button::new("Up").small())
                                .clicked()
                            {
                                move_row = Some((i, -1));
                            }
                            if ui
                                .add_enabled(i + 1 < row_count, egui::Button::new("Down").small())
                                .clicked()
                            {
                                move_row = Some((i, 1));
                            }
                            if ui
                                .add_enabled(row_count > 1, egui::Button::new("Remove").small())
                                .clicked()
                            {
                                remove_row = Some(i);
                            }
                        });

                        ui.end_row();
                    }
                });

            ui.add_space(6.0);
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(builder.can_add_row(), egui::Button::new("Add Partition"))
                    .clicked()
                {
                    builder.add_row();
                }
                if let Some(left) = builder.remaining_slots() {
                    ui.label(
                        egui::RichText::new(format!(
                            "{} of {} slots used",
                            row_count,
                            row_count + left,
                        ))
                        .weak(),
                    );
                }
            });

            ui.add_space(8.0);
            for err in &builder.errors {
                let color = if err.starts_with("Warning:") {
                    super::theme::warning(ui.visuals())
                } else {
                    super::theme::danger(ui.visuals())
                };
                ui.colored_label(color, err);
            }
            if let Some(status) = &builder.status {
                ui.colored_label(super::theme::success(ui.visuals()), status);
            }

            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if ui.button("Validate").clicked() {
                    builder.validate();
                }
                let can_apply = allow_apply && builder.plan().is_ok();
                if ui
                    .add_enabled(can_apply, egui::Button::new("Create Disk"))
                    .on_hover_text(
                        "Writes the partition table, then pours each assigned \
                         image into its partition. This erases the target.",
                    )
                    .clicked()
                    && builder.validate().is_some()
                {
                    action = Action::Apply;
                }
                if ui.button("Cancel").clicked() {
                    action = Action::Close;
                }
            });
        });

    if let Some((i, delta)) = move_row {
        builder.move_row(i, delta);
    }
    if let Some(i) = remove_row {
        builder.remove_row(i);
    }

    if !open {
        return Action::Close;
    }
    action
}

/// Source-image cell: pick / clear, with the decoded size cached on the row so
/// validation doesn't reopen the container every frame.
fn source_cell(ui: &mut egui::Ui, row: &mut BuilderRow, index: usize) {
    ui.horizontal(|ui| {
        match row.source.clone() {
            Some(path) => {
                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| path.display().to_string());
                ui.label(format!(
                    "{} ({})",
                    name,
                    partition::format_size(row.source_size),
                ))
                .on_hover_text(path.display().to_string());
                if ui.small_button("Clear").clicked() {
                    row.source = None;
                    row.source_size = 0;
                }
            }
            None => {
                ui.label(egui::RichText::new("(empty)").weak());
            }
        }
        if ui
            .small_button("Choose...")
            .on_hover_text(
                "Image poured into this partition after the table is written. \
                 Any format Rusty Backup can read works.",
            )
            .clicked()
        {
            let picked = rfd::FileDialog::new()
                .add_filter(
                    "Disk images",
                    rusty_backup::model::file_types::DISK_IMAGE_EXTS,
                )
                .add_filter("All Files", &["*"])
                .set_title(format!("Source image for partition {}", index + 1))
                .pick_file();
            if let Some(path) = picked {
                row.source_size =
                    rusty_backup::model::source_reader::decoded_image_size(&path).unwrap_or(0);
                row.source = Some(path);
            }
        }
    });
}

/// One bar showing the planned layout, or the reason there isn't one.
fn show_planned_layout_bar(ui: &mut egui::Ui, builder: &DiskBuilder) {
    use super::partition_bar::PartitionBar;

    let planned = match builder.plan() {
        Ok(p) => p,
        Err(e) => {
            ui.colored_label(super::theme::danger(ui.visuals()), format!("Layout: {e:#}"));
            return;
        }
    };

    let placed: Vec<PlacedSegment> = planned
        .iter()
        .enumerate()
        .map(|(i, p)| PlacedSegment {
            label: format!("Partition {}", i + 1),
            fs: type_catalog::describe(builder.kind, &p.type_text)
                .unwrap_or(&p.type_text)
                .to_string(),
            start_byte: p.start_byte(),
            size_bytes: p.size_bytes,
            color_index: i,
        })
        .collect();

    let allocated: u64 = placed.iter().map(|p| p.size_bytes).sum();
    ui.label(format!(
        "Layout  (disk {}, {} allocated, {} free):",
        partition::format_size(builder.disk_size),
        partition::format_size(allocated),
        partition::format_size(builder.disk_size.saturating_sub(allocated)),
    ));
    let available_width = ui.available_width().max(120.0);
    ui.scope(|ui| {
        ui.set_width(available_width);
        PartitionBar {
            segments: place_segments(&placed, builder.disk_size.max(1)),
            show_inline_labels: true,
            show_legend: true,
        }
        .show(ui);
    });
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_backup::partition::type_catalog::TableKind;

    const MIB: u64 = 1024 * 1024;

    fn info(index: usize, start_lba: u64, size_bytes: u64) -> PartitionInfo {
        PartitionInfo {
            index,
            type_name: "0x83".to_string(),
            partition_type_byte: 0x83,
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

    /// Renders headlessly. Guards the modal against the panics egui raises for
    /// a Grid whose rows disagree on cell count, or an id clash between the two
    /// modes' widgets — neither of which a compile catches.
    #[test]
    fn both_modes_render_without_panicking() {
        let parts = vec![info(0, 2048, 10 * MIB), info(1, 2048 + 20480, 10 * MIB)];
        let mut editor = PartitionEditor::new();
        editor.seed_from_with_minimums(&parts, &Default::default(), Some(100 * MIB));

        egui::__run_test_ui(|ui| {
            let action = show(
                ui,
                Mode::EditExisting {
                    editor: &mut editor,
                    partitions: &parts,
                    table: None,
                },
                false,
            );
            assert_eq!(action, Action::Stay);
        });

        // Every writable table, since the row shape varies with the kind (the
        // Name column only exists on GPT / APM / X68000).
        for &kind in provision::WRITABLE_TABLES {
            let mut builder = DiskBuilder::new(kind, 512 * MIB);
            builder.add_row();
            builder.rows[0].source = Some(std::path::PathBuf::from("/tmp/example.img"));
            builder.rows[0].source_size = 8 * MIB;
            egui::__run_test_ui(|ui| {
                let action = show(
                    ui,
                    Mode::BuildNew {
                        builder: &mut builder,
                    },
                    true,
                );
                assert_eq!(action, Action::Stay, "{}", kind.label());
            });
        }
    }

    /// A layout that cannot be placed must render the reason, not panic or
    /// silently draw an empty bar.
    #[test]
    fn an_unplaceable_layout_still_renders() {
        let mut builder = DiskBuilder::new(TableKind::Mbr, 16 * MIB);
        builder.rows[0].size_text = "900M".to_string();
        assert!(builder.plan().is_err());
        egui::__run_test_ui(|ui| {
            show(
                ui,
                Mode::BuildNew {
                    builder: &mut builder,
                },
                true,
            );
        });
    }
}
