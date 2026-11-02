//! The Optical tab's disc browser: draws `controller::optical::browse` and returns the frame's
//! [`DiscBrowseAction`]s (as `OpticalAction::Browse`). The tree's open folders are egui's
//! collapsing state; everything else is the controller's.

use rusty_backup::controller::optical::browse::{
    display_name, format_size, hex_dump, DiscBrowseController, EntryType, ExportFormat,
    FileContent, FileEntry, ResourceForkMode, MAX_PREVIEW_SIZE,
};
use rusty_backup::controller::optical::{DiscBrowseAction, OpticalAction};

/// Draw the browser (nothing while no disc is open); the frame's actions land in `out`.
pub fn show(ui: &mut egui::Ui, ctl: &DiscBrowseController, out: &mut Vec<OpticalAction>) {
    if !ctl.is_active() {
        return;
    }
    let mut actions = Vec::new();
    header(ui, ctl, &mut actions);
    tree_window(ui, ctl, &mut actions);

    if let Some(snap) = ctl.opening() {
        super::progress::show_job(ui, &snap);
    }
    if let Some(err) = ctl.error() {
        ui.colored_label(super::theme::danger(ui.visuals()), err);
    }
    if let Some(msg) = ctl.result() {
        ui.colored_label(super::theme::success(ui.visuals()), msg);
    }

    ui.separator();

    let available = ui.available_size();
    let tree_width = (available.x * 0.4).clamp(200.0, 400.0);
    let panel_height = available.y;

    ui.horizontal(|ui| {
        ui.set_min_height(panel_height);
        ui.vertical(|ui| {
            ui.set_width(tree_width);
            ui.set_min_height(panel_height);
            selection_bar(ui, ctl, &mut actions);
            egui::ScrollArea::vertical()
                .id_salt("optical_browse_tree")
                .max_height(panel_height)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    if let Some(root) = ctl.root() {
                        tree_entry(ui, ctl, root, &mut actions);
                    }
                });
        });
        ui.separator();
        ui.vertical(|ui| {
            ui.set_min_width(available.x - tree_width - 20.0);
            ui.set_min_height(panel_height);
            content_panel(ui, ctl, panel_height, &mut actions);
        });
    });

    out.extend(actions.into_iter().map(OpticalAction::Browse));
}

fn header(ui: &mut egui::Ui, ctl: &DiscBrowseController, actions: &mut Vec<DiscBrowseAction>) {
    let labels = ctl.fs_choices();
    let selected = ctl.selected_fs();
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new("Disc Browser").strong());
        // A hybrid Mac/PC disc (or one with boot areas) offers a filesystem picker.
        if labels.len() > 1 {
            ui.label("Filesystem:");
            egui::ComboBox::from_id_salt("optical_fs_picker")
                .selected_text(labels.get(selected).cloned().unwrap_or_default())
                .show_ui(ui, |ui| {
                    for (idx, label) in labels.iter().enumerate() {
                        if ui.selectable_label(idx == selected, label).clicked() {
                            actions.push(DiscBrowseAction::SelectFs(idx));
                        }
                    }
                });
        } else if let Some(label) = labels.first() {
            ui.label(format!("[{label}]"));
        }
        if let Some(g) = ctl.game() {
            ui.label(format!("Game: {g}"));
        }
        if ui.button("Tree").clicked() {
            actions.push(DiscBrowseAction::Tree);
        }
        if ui.button("Close").clicked() {
            actions.push(DiscBrowseAction::Close);
        }
    });
}

fn tree_entry(
    ui: &mut egui::Ui,
    ctl: &DiscBrowseController,
    entry: &FileEntry,
    actions: &mut Vec<DiscBrowseAction>,
) {
    let is_selected = ctl.selected().is_some_and(|s| s.path == entry.path);
    match entry.entry_type {
        EntryType::Directory => {
            // A toggle plus a separately clickable label, so a folder (the root too) can be selected.
            let path = entry.path.as_str();
            let id = ui.make_persistent_id(path);
            let mut state = egui::collapsing_header::CollapsingState::load_with_default_open(
                ui.ctx(),
                id,
                path == "/",
            );
            let header_res = ui.horizontal(|ui| {
                state.show_toggle_button(ui, egui::collapsing_header::paint_default_icon);
                mark_checkbox(ui, ctl, entry, actions);
                if ui
                    .selectable_label(is_selected, display_name(entry))
                    .clicked()
                {
                    actions.push(DiscBrowseAction::Select(entry.clone()));
                }
            });
            state.show_body_indented(&header_res.response, ui, |ui| match ctl.children(path) {
                Some(children) => {
                    for child in children {
                        tree_entry(ui, ctl, child, actions);
                    }
                }
                None => {
                    ui.label("Loading...");
                }
            });
            if state.is_open() && ctl.children(path).is_none() {
                actions.push(DiscBrowseAction::Expand(entry.clone()));
            }
        }
        EntryType::File => {
            let size_str = entry.size_string();
            // A symlink's target is shown as the filesystem stores it.
            let label = match &entry.symlink_target {
                Some(target) => format!("{} -> {}  ({})", entry.name, target, size_str),
                None => format!("{}  ({})", entry.name, size_str),
            };
            ui.horizontal(|ui| {
                // The indent a folder row spends on its toggle, so the checkboxes line up.
                ui.add_space(ui.spacing().indent);
                mark_checkbox(ui, ctl, entry, actions);
                if ui.selectable_label(is_selected, &label).clicked() {
                    actions.push(DiscBrowseAction::Select(entry.clone()));
                }
            });
        }
    }
}

fn mark_checkbox(
    ui: &mut egui::Ui,
    ctl: &DiscBrowseController,
    entry: &FileEntry,
    actions: &mut Vec<DiscBrowseAction>,
) {
    let mut marked = ctl.is_marked(entry.path.as_str());
    if ui
        .checkbox(&mut marked, "")
        .on_hover_text("Mark for multi-select export")
        .changed()
    {
        actions.push(DiscBrowseAction::SetMarked {
            entry: entry.clone(),
            marked,
        });
    }
}

/// The bar above the tree while entries are marked: the count, the format, Export and Clear.
fn selection_bar(
    ui: &mut egui::Ui,
    ctl: &DiscBrowseController,
    actions: &mut Vec<DiscBrowseAction>,
) {
    if ctl.marked_count() == 0 {
        return;
    }
    let mut fmt = ctl.export_format();
    ui.horizontal_wrapped(|ui| {
        ui.label(format!("{} selected", ctl.marked_count()));
        ui.label("Export as:");
        egui::ComboBox::from_id_salt("optical_export_fmt")
            .selected_text(fmt.label())
            .show_ui(ui, |ui| {
                for f in ExportFormat::ALL {
                    ui.selectable_value(&mut fmt, f, f.label());
                }
            });
        if ui.button("Export...").clicked() {
            actions.push(DiscBrowseAction::Export);
        }
        if ui.button("Clear").clicked() {
            actions.push(DiscBrowseAction::ClearMarks);
        }
    });
    ui.separator();
    if fmt != ctl.export_format() {
        actions.insert(0, DiscBrowseAction::SetExportFormat(fmt));
    }
}

fn content_panel(
    ui: &mut egui::Ui,
    ctl: &DiscBrowseController,
    panel_height: f32,
    actions: &mut Vec<DiscBrowseAction>,
) {
    if let Some(snap) = ctl.working() {
        if super::progress::show_job(ui, &snap) {
            actions.push(DiscBrowseAction::CancelExtract);
        }
        ui.separator();
    }

    let Some(entry) = ctl.selected() else {
        ui.colored_label(
            super::theme::muted(ui.visuals()),
            "Select a file to view its contents.",
        );
        return;
    };

    ui.label(egui::RichText::new(display_name(entry)).strong());
    ui.horizontal(|ui| {
        ui.label(format!("Size: {}", entry.size_string()));
        if let Some(tc) = entry.type_code_string() {
            ui.label(format!("Type: {tc}"));
        }
        if let Some(cc) = entry.creator_code_string() {
            ui.label(format!("Creator: {cc}"));
        }
        if let Some(rsrc) = entry.resource_fork_size {
            if rsrc > 0 {
                ui.label(format!("Rsrc: {}", format_size(rsrc)));
            }
        }
        if let Some(ref target) = entry.symlink_target {
            ui.label(format!("-> {target}"));
        }
        ui.label(format!("Path: {}", entry.path));
    });

    if !ctl.is_extracting() {
        ui.horizontal(|ui| {
            if ctl.is_hfs() {
                ui.label("Resource forks:");
                let mut mode = ctl.fork_mode();
                egui::ComboBox::from_id_salt("optical_rsrc_mode")
                    .selected_text(mode.label())
                    .show_ui(ui, |ui| {
                        for m in &ResourceForkMode::ALL {
                            ui.selectable_value(&mut mode, *m, m.label());
                        }
                    });
                if mode != ctl.fork_mode() {
                    actions.push(DiscBrowseAction::SetForkMode(mode));
                }
                ui.add_space(8.0);
            }
            let label = if entry.is_directory() {
                "Extract Folder..."
            } else {
                "Extract File..."
            };
            if ui.button(label).clicked() {
                actions.push(DiscBrowseAction::Extract);
            }
        });
    }

    ui.separator();

    if entry.size > MAX_PREVIEW_SIZE as u64 && entry.is_file() {
        ui.label(format!(
            "File too large to preview ({}).",
            entry.size_string()
        ));
        return;
    }
    if entry.is_directory() {
        return;
    }

    let content_height = ui.available_height().min(panel_height);
    match ctl.content() {
        None => {
            if ctl.error().is_none() {
                ui.spinner();
                ui.label("Loading...");
            }
        }
        Some(FileContent::Text(text)) => {
            egui::ScrollArea::vertical()
                .id_salt("optical_file_content")
                .max_height(content_height)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.add(
                        egui::TextEdit::multiline(&mut text.as_str())
                            .desired_width(f32::INFINITY)
                            .font(egui::TextStyle::Monospace),
                    );
                });
        }
        Some(FileContent::Binary(data)) => {
            let hex = hex_dump(data.as_slice());
            egui::ScrollArea::vertical()
                .id_salt("optical_file_content")
                .max_height(content_height)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.add(
                        egui::TextEdit::multiline(&mut hex.as_str())
                            .desired_width(f32::INFINITY)
                            .font(egui::TextStyle::Monospace),
                    );
                });
        }
    }
}

/// The tree listing window, while the controller holds its text.
fn tree_window(ui: &mut egui::Ui, ctl: &DiscBrowseController, actions: &mut Vec<DiscBrowseAction>) {
    let Some(text) = ctl.tree_text() else {
        return;
    };
    let mut open = true;
    egui::Window::new("Tree View")
        .open(&mut open)
        .resizable(true)
        .default_width(600.0)
        .default_height(500.0)
        .show(ui.ctx(), |ui| {
            ui.horizontal(|ui| {
                if ui.button("Copy to Clipboard").clicked() {
                    actions.push(DiscBrowseAction::CopyTree);
                }
                if ui.button("Save to File...").clicked() {
                    actions.push(DiscBrowseAction::SaveTree);
                }
            });
            ui.separator();
            egui::ScrollArea::both()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.add(
                        egui::TextEdit::multiline(&mut { text })
                            .font(egui::TextStyle::Monospace)
                            .desired_width(f32::INFINITY),
                    );
                });
        });
    if !open {
        actions.push(DiscBrowseAction::CloseTree);
    }
}
