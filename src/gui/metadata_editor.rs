//! Shared metadata-editor widgets: the HFS/HFS+ type/creator row and the
//! ProDOS type/aux row.
//!
//! Extracted from `browse_view` (R0 of the Commander Mode plan, see
//! `docs/commander_mode.md` §3) so the classic browse view and Commander
//! Mode's File Info window stage identical edits. Each widget is read-only
//! when `edit_mode` is false and a full editor (text fields + dictionary
//! pulldown + Set/Reset) when true. Instead of touching a `BrowseView`, they
//! take the staging queue, the per-entry editor scratch state, and a
//! human-readable result string by mutable reference — so any caller that
//! owns an `EditQueue` can host them.

use rusty_backup::fs::entry::FileEntry;
use rusty_backup::model::edit_queue::{EditQueue, StagedEdit};

/// Inline editor state for an HFS/HFS+ file's type and creator codes.
#[derive(Debug, Clone)]
pub struct HfsTypeEditorState {
    /// Path of the entry being edited — invalidates when selection changes.
    pub entry_path: String,
    /// 4-char freeform input for the type code (clamped on every frame).
    pub type_input: String,
    /// 4-char freeform input for the creator code.
    pub creator_input: String,
}

/// Inline editor state for a ProDOS file's type byte and aux type.
pub struct ProdosTypeEditorState {
    /// Path of the entry being edited — invalidates when selection changes.
    pub entry_path: String,
    /// 2-char hex input for the type byte.
    pub type_input: String,
    /// 4-char hex input for the aux type.
    pub aux_input: String,
}

/// Render the HFS/HFS+ type/creator row beneath the file-info header.
/// Read-only when not in edit mode; full editor (text + pulldown + Set
/// button) when edit mode is on. For directories, callers should not invoke
/// this (no-op behavior is the caller's responsibility).
pub fn render_hfs_type_row(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    edit_mode: bool,
    queue: &mut EditQueue,
    editor: &mut Option<HfsTypeEditorState>,
    result: &mut Option<String>,
) {
    let (cur_t, cur_c) = queue.resolved_hfs_type_creator(entry);
    let cur_t_str = String::from_utf8_lossy(&cur_t).to_string();
    let cur_c_str = String::from_utf8_lossy(&cur_c).to_string();
    let cur_desc = rusty_backup::fs::hfs_common::describe_type_creator(&cur_t, &cur_c);

    if !edit_mode {
        ui.horizontal(|ui| {
            if cur_t != [0; 4] || cur_c != [0; 4] {
                let label = match cur_desc {
                    Some(d) => format!("Type: {cur_t_str}  Creator: {cur_c_str}  ({d})"),
                    None => format!("Type: {cur_t_str}  Creator: {cur_c_str}"),
                };
                ui.label(label);
            } else {
                ui.colored_label(
                    egui::Color32::from_rgb(120, 160, 220),
                    "Type: (none)  Creator: (none)",
                );
            }
        });
        return;
    }

    // Edit mode — seed editor state if it's a different entry.
    let needs_seed = editor
        .as_ref()
        .map(|s| s.entry_path != entry.path)
        .unwrap_or(true);
    if needs_seed {
        *editor = Some(HfsTypeEditorState {
            entry_path: entry.path.clone(),
            type_input: cur_t_str.clone(),
            creator_input: cur_c_str.clone(),
        });
    }

    let mut stage_action: Option<([u8; 4], [u8; 4])> = None;
    let mut reset_action = false;

    // Borrow editor mutably inside one ui.horizontal closure.
    if let Some(state) = editor.as_mut() {
        ui.horizontal(|ui| {
            ui.label("Type:");
            ui.add(
                egui::TextEdit::singleline(&mut state.type_input)
                    .desired_width(48.0)
                    .char_limit(4)
                    .font(egui::TextStyle::Monospace),
            );
            ui.label("Creator:");
            ui.add(
                egui::TextEdit::singleline(&mut state.creator_input)
                    .desired_width(48.0)
                    .char_limit(4)
                    .font(egui::TextStyle::Monospace),
            );

            // Pulldown with current FInfo first, then dictionary by name.
            let combo_label = match cur_desc {
                Some(d) => format!("(current) {cur_t_str}/{cur_c_str} — {d}"),
                None if cur_t != [0; 4] || cur_c != [0; 4] => {
                    format!("(current) {cur_t_str}/{cur_c_str}")
                }
                None => "Pick from dictionary…".to_string(),
            };
            let mut picked: Option<([u8; 4], [u8; 4])> = None;
            egui::ComboBox::from_id_salt(("hfs_tc_dict", &entry.path))
                .selected_text(combo_label)
                .width(280.0)
                .show_ui(ui, |ui| {
                    if cur_t != [0; 4] || cur_c != [0; 4] {
                        let label = match cur_desc {
                            Some(d) => {
                                format!("(current) {cur_t_str}/{cur_c_str} — {d}")
                            }
                            None => format!("(current) {cur_t_str}/{cur_c_str}"),
                        };
                        if ui.selectable_label(false, label).clicked() {
                            picked = Some((cur_t, cur_c));
                        }
                        ui.separator();
                    }
                    for e in rusty_backup::fs::hfs_common::known_type_creators() {
                        let label =
                            format!("{}/{} — {}", e.type_str(), e.creator_str(), e.description);
                        if ui.selectable_label(false, label).clicked() {
                            picked = Some((e.type_code, e.creator_code));
                        }
                    }
                });
            if let Some((t, c)) = picked {
                state.type_input = String::from_utf8_lossy(&t).to_string();
                state.creator_input = String::from_utf8_lossy(&c).to_string();
            }

            // Encode current text into 4-byte arrays (space-padded).
            let typed_t = rusty_backup::fs::hfs_common::encode_fourcc(&state.type_input);
            let typed_c = rusty_backup::fs::hfs_common::encode_fourcc(&state.creator_input);
            let changed = typed_t != cur_t || typed_c != cur_c;

            if ui
                .add_enabled(changed, egui::Button::new("Set"))
                .on_hover_text("Stage type/creator change")
                .clicked()
            {
                stage_action = Some((typed_t, typed_c));
            }
            if ui
                .button("Reset")
                .on_hover_text("Revert editor to current values")
                .clicked()
            {
                reset_action = true;
            }
        });
    }

    if reset_action {
        if let Some(state) = editor.as_mut() {
            state.type_input = cur_t_str;
            state.creator_input = cur_c_str;
        }
    }

    if let Some((t, c)) = stage_action {
        if queue.set_pending_hfs_override(&entry.path, t, c) {
            *result = Some(format!(
                "Updated pending '{}' to {}/{}",
                entry.name,
                String::from_utf8_lossy(&t),
                String::from_utf8_lossy(&c),
            ));
        } else {
            queue.replace_set_type_creator(entry, t, c);
            *result = Some(format!(
                "Staged type/creator {}/{} on '{}'",
                String::from_utf8_lossy(&t),
                String::from_utf8_lossy(&c),
                entry.name,
            ));
        }
    }
}

/// Resolve the effective ProDOS (type_byte, aux_type) for an entry,
/// considering any pending `AddFile` overrides. Returns `(0x00, 0x0000)`
/// when nothing is known.
pub fn resolved_prodos_type(queue: &EditQueue, entry: &FileEntry) -> (u8, u16) {
    // 1) Pending AddFile override
    for edit in queue.iter() {
        if let StagedEdit::AddFile {
            parent,
            name,
            prodos_type,
            prodos_aux,
            ..
        } = edit
        {
            let path = if parent.path == "/" {
                format!("/{name}")
            } else {
                format!("{}/{name}", parent.path)
            };
            if path != entry.path {
                continue;
            }
            let t = prodos_type.unwrap_or(0);
            let a = prodos_aux.unwrap_or(0);
            return (t, a);
        }
    }
    // 2) On-disk catalog values: parse "$XX ABC" out of entry.type_code
    let t = entry
        .type_code
        .as_deref()
        .and_then(|tc| {
            tc.split_whitespace()
                .next()
                .and_then(|s| u8::from_str_radix(s.trim_start_matches('$'), 16).ok())
        })
        .unwrap_or(0);
    let a = entry.aux_type.unwrap_or(0);
    (t, a)
}

/// Render the ProDOS type/aux row beneath the file-info header. Read-only
/// when not in edit mode; full editor (type pulldown + 2-char/4-char hex
/// inputs + Set/Reset) when edit mode is on.
pub fn render_prodos_type_row(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    edit_mode: bool,
    queue: &mut EditQueue,
    editor: &mut Option<ProdosTypeEditorState>,
    result: &mut Option<String>,
) {
    use rusty_backup::fs::prodos_types as pt;

    let (cur_t, cur_a) = resolved_prodos_type(queue, entry);
    let cur_known = pt::is_known_type(cur_t);
    let cur_abbr = pt::type_abbr(cur_t);
    let cur_desc = pt::type_description(cur_t);

    if !edit_mode {
        ui.horizontal(|ui| {
            let label = format!("Type: ${cur_t:02X} {cur_abbr}  Aux: ${cur_a:04X}  ({cur_desc})");
            if cur_known {
                ui.label(label);
            } else {
                ui.colored_label(egui::Color32::from_rgb(120, 160, 220), label);
            }
        });
        return;
    }

    // Edit mode — seed editor state if it's a different entry.
    let needs_seed = editor
        .as_ref()
        .map(|s| s.entry_path != entry.path)
        .unwrap_or(true);
    if needs_seed {
        *editor = Some(ProdosTypeEditorState {
            entry_path: entry.path.clone(),
            type_input: format!("{cur_t:02X}"),
            aux_input: format!("{cur_a:04X}"),
        });
    }

    let mut stage_action: Option<(u8, u16)> = None;
    let mut reset_action = false;

    if let Some(state) = editor.as_mut() {
        ui.horizontal(|ui| {
            ui.label("Type pulldown:");
            let combo_label = format!("${cur_t:02X} {cur_abbr} — {cur_desc}");
            let mut picked: Option<u8> = None;
            egui::ComboBox::from_id_salt(("prodos_type_combo", &entry.path))
                .selected_text(combo_label)
                .width(320.0)
                .show_ui(ui, |ui| {
                    for (byte, info) in &pt::all_types() {
                        let label = format!("${:02X} {} — {}", byte, info.abbr, info.description);
                        if ui.selectable_label(*byte == cur_t, label).clicked() {
                            picked = Some(*byte);
                        }
                    }
                });
            if let Some(b) = picked {
                state.type_input = format!("{b:02X}");
            }
        });

        ui.horizontal(|ui| {
            ui.label("Type ($):");
            ui.add(
                egui::TextEdit::singleline(&mut state.type_input)
                    .desired_width(40.0)
                    .char_limit(2)
                    .font(egui::TextStyle::Monospace),
            );
            ui.label("Aux ($):");
            ui.add(
                egui::TextEdit::singleline(&mut state.aux_input)
                    .desired_width(60.0)
                    .char_limit(4)
                    .font(egui::TextStyle::Monospace),
            );

            let parsed_t = u8::from_str_radix(state.type_input.trim(), 16).ok();
            let parsed_a = u16::from_str_radix(state.aux_input.trim(), 16).ok();
            let valid = parsed_t.is_some() && parsed_a.is_some();
            let changed = match (parsed_t, parsed_a) {
                (Some(t), Some(a)) => t != cur_t || a != cur_a,
                _ => false,
            };

            if ui
                .add_enabled(valid && changed, egui::Button::new("Set"))
                .on_hover_text("Stage type/aux change")
                .clicked()
            {
                if let (Some(t), Some(a)) = (parsed_t, parsed_a) {
                    stage_action = Some((t, a));
                }
            }
            if ui
                .button("Reset")
                .on_hover_text("Revert editor to current values")
                .clicked()
            {
                reset_action = true;
            }
            if !valid {
                ui.colored_label(
                    egui::Color32::from_rgb(255, 120, 120),
                    "Type must be 2 hex digits, Aux must be 4",
                );
            }
        });
    }

    if reset_action {
        if let Some(state) = editor.as_mut() {
            state.type_input = format!("{cur_t:02X}");
            state.aux_input = format!("{cur_a:04X}");
        }
    }

    if let Some((t, a)) = stage_action {
        if queue.set_pending_prodos_override(&entry.path, t, a) {
            *result = Some(format!(
                "Updated pending '{}' to ${t:02X}/${a:04X}",
                entry.name,
            ));
        } else {
            queue.replace_set_prodos_type(entry, t, a);
            *result = Some(format!("Staged type ${t:02X}/${a:04X} on '{}'", entry.name,));
        }
    }
}

/// Inline editor state for an HFS/HFS+ file's catalog dates.
pub struct HfsDatesEditorState {
    /// Path of the entry being edited — invalidates when selection changes.
    pub entry_path: String,
    /// `YYYY-MM-DD HH:MM:SS` (UTC) inputs; empty == "no date set".
    pub create: String,
    pub modify: String,
    pub backup: String,
}

/// Render the HFS create/modify/backup date rows. Read-only labels when not in
/// edit mode; three editable `YYYY-MM-DD HH:MM:SS` (UTC) fields + a Set button
/// staging a [`StagedEdit::SetDates`] when edit mode is on. Dates are sourced
/// from `entry.mac_dates` (overlaid by any pending staged value).
pub fn render_hfs_dates_row(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    edit_mode: bool,
    queue: &mut EditQueue,
    editor: &mut Option<HfsDatesEditorState>,
    result: &mut Option<String>,
) {
    use rusty_backup::fs::hfs_common::{format_mac_date, parse_mac_date};

    // Effective current dates: a staged edit wins over the on-disk values.
    let (cur_c, cur_m, cur_b) = queue
        .pending_dates_for(&entry.path)
        .or(entry.mac_dates)
        .unwrap_or((0, 0, 0));
    let fmt = |secs: u32| format_mac_date(secs).unwrap_or_else(|| "(none)".to_string());

    if !edit_mode {
        ui.label(format!(
            "Created: {}   Modified: {}   Backup: {}",
            fmt(cur_c),
            fmt(cur_m),
            fmt(cur_b)
        ));
        return;
    }

    // Seed editor for a freshly-selected entry. Zero -> empty field.
    let blank = |secs: u32| format_mac_date(secs).unwrap_or_default();
    let needs_seed = editor
        .as_ref()
        .map(|s| s.entry_path != entry.path)
        .unwrap_or(true);
    if needs_seed {
        *editor = Some(HfsDatesEditorState {
            entry_path: entry.path.clone(),
            create: blank(cur_c),
            modify: blank(cur_m),
            backup: blank(cur_b),
        });
    }

    let mut stage_action: Option<(u32, u32, u32)> = None;
    let mut reset_action = false;

    if let Some(state) = editor.as_mut() {
        ui.label("Dates (YYYY-MM-DD HH:MM:SS, UTC; blank = none):");
        egui::Grid::new(("hfs_dates_grid", &entry.path))
            .num_columns(2)
            .spacing([8.0, 4.0])
            .show(ui, |ui| {
                for (label, field) in [
                    ("Created", &mut state.create),
                    ("Modified", &mut state.modify),
                    ("Backup", &mut state.backup),
                ] {
                    ui.label(label);
                    ui.add(
                        egui::TextEdit::singleline(field)
                            .desired_width(170.0)
                            .font(egui::TextStyle::Monospace),
                    );
                    ui.end_row();
                }
            });

        let parsed_c = parse_mac_date(&state.create);
        let parsed_m = parse_mac_date(&state.modify);
        let parsed_b = parse_mac_date(&state.backup);
        let valid = parsed_c.is_some() && parsed_m.is_some() && parsed_b.is_some();
        let changed = match (parsed_c, parsed_m, parsed_b) {
            (Some(c), Some(m), Some(b)) => (c, m, b) != (cur_c, cur_m, cur_b),
            _ => false,
        };

        ui.horizontal(|ui| {
            if ui
                .add_enabled(valid && changed, egui::Button::new("Set dates"))
                .on_hover_text("Stage create/modify/backup date change")
                .clicked()
            {
                if let (Some(c), Some(m), Some(b)) = (parsed_c, parsed_m, parsed_b) {
                    stage_action = Some((c, m, b));
                }
            }
            if ui.button("Reset").clicked() {
                reset_action = true;
            }
            if !valid {
                ui.colored_label(
                    egui::Color32::from_rgb(255, 120, 120),
                    "Use YYYY-MM-DD HH:MM:SS or leave blank",
                );
            }
        });
    }

    if reset_action {
        if let Some(state) = editor.as_mut() {
            state.create = blank(cur_c);
            state.modify = blank(cur_m);
            state.backup = blank(cur_b);
        }
    }

    if let Some((c, m, b)) = stage_action {
        queue.replace_set_dates(entry, c, m, b);
        *result = Some(format!("Staged date change on '{}'", entry.name));
    }
}

/// Inline editor state for an ext file's Unix permission bits.
pub struct ExtPermsEditorState {
    /// Path of the entry being edited — invalidates when selection changes.
    pub entry_path: String,
    /// Octal permission string (e.g. `755`); the low 12 bits.
    pub octal: String,
}

/// Render the ext Unix-permission editor. Read-only `rwx` label when not in
/// edit mode; an octal field + Set button staging a
/// [`StagedEdit::SetPermissions`] when edit mode is on. Sources the current
/// mode from `entry.mode` (overlaid by any pending staged value).
pub fn render_ext_permissions_row(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    edit_mode: bool,
    queue: &mut EditQueue,
    editor: &mut Option<ExtPermsEditorState>,
    result: &mut Option<String>,
) {
    // Effective current perms: a staged edit wins over the on-disk value.
    let cur = queue
        .pending_permissions_for(&entry.path)
        .or(entry.mode)
        .map(|m| m & 0o7777)
        .unwrap_or(0);

    if !edit_mode {
        if let Some(mode_str) = entry.mode_string() {
            ui.label(format!("Permissions: {mode_str}  ({cur:o})"));
        }
        return;
    }

    let needs_seed = editor
        .as_ref()
        .map(|s| s.entry_path != entry.path)
        .unwrap_or(true);
    if needs_seed {
        *editor = Some(ExtPermsEditorState {
            entry_path: entry.path.clone(),
            octal: format!("{cur:o}"),
        });
    }

    let mut stage_action: Option<u32> = None;
    let mut reset_action = false;

    if let Some(state) = editor.as_mut() {
        ui.horizontal(|ui| {
            ui.label("Permissions (octal):");
            ui.add(
                egui::TextEdit::singleline(&mut state.octal)
                    .desired_width(56.0)
                    .char_limit(4)
                    .font(egui::TextStyle::Monospace),
            );
            let parsed = u32::from_str_radix(state.octal.trim(), 8)
                .ok()
                .filter(|m| *m <= 0o7777);
            let valid = parsed.is_some();
            let changed = parsed.map(|m| m != cur).unwrap_or(false);
            if ui
                .add_enabled(valid && changed, egui::Button::new("Set"))
                .on_hover_text("Stage permission change (ext only)")
                .clicked()
            {
                stage_action = parsed;
            }
            if ui.button("Reset").clicked() {
                reset_action = true;
            }
            if !valid {
                ui.colored_label(egui::Color32::from_rgb(255, 120, 120), "1-4 octal digits");
            }
        });
    }

    if reset_action {
        if let Some(state) = editor.as_mut() {
            state.octal = format!("{cur:o}");
        }
    }

    if let Some(mode) = stage_action {
        queue.replace_set_permissions(entry, mode);
        *result = Some(format!("Staged permissions {mode:o} on '{}'", entry.name));
    }
}
