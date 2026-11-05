//! Shared metadata-editor widgets: the HFS/HFS+ type/creator row and the
//! ProDOS type/aux row.
//!
//! Extracted from `browse_view` (R0 of the Commander Mode plan, see
//! `docs/commander_mode.md` §3) so the classic browse view and Commander
//! Mode's File Info window stage identical edits. Each widget is read-only
//! when `edit_mode` is false and a full editor (text fields + dictionary
//! pulldown + Set/Reset) when true. Instead of touching a `BrowseView`, they
//! take a [`MetadataSink`] (an `EditQueue` stages directly; Commander's sends the
//! [`MetadataEdit`] to its controller), the per-entry editor scratch state, and a
//! human-readable result string by mutable reference.

use rusty_backup::controller::commander::metadata as rules;
use rusty_backup::controller::commander::{EditQueue, FileEntry, IdNameMap, MetadataEdit, Xattr};

/// Where an editor's change goes: straight onto a queue, or back to a controller as an action.
pub trait MetadataSink {
    fn queue(&self) -> &EditQueue;
    /// Stage `edit`; `Some` is the result line to show now.
    fn stage(&mut self, edit: MetadataEdit) -> Option<String>;
}

impl MetadataSink for EditQueue {
    fn queue(&self) -> &EditQueue {
        self
    }

    fn stage(&mut self, edit: MetadataEdit) -> Option<String> {
        Some(self.stage_metadata(edit))
    }
}

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
    sink: &mut dyn MetadataSink,
    editor: &mut Option<HfsTypeEditorState>,
    result: &mut Option<String>,
) {
    let cur = rules::hfs_type_creator(sink.queue(), entry);
    let (cur_t, cur_c) = (cur.type_code, cur.creator_code);
    let cur_t_str = cur.type_text;
    let cur_c_str = cur.creator_text;
    let cur_desc = cur.description;

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
                    super::theme::info(ui.visuals()),
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
                Some(d) => format!("(current) {cur_t_str}/{cur_c_str} - {d}"),
                None if cur_t != [0; 4] || cur_c != [0; 4] => {
                    format!("(current) {cur_t_str}/{cur_c_str}")
                }
                None => "Pick from dictionary...".to_string(),
            };
            let mut picked: Option<([u8; 4], [u8; 4])> = None;
            egui::ComboBox::from_id_salt(("hfs_tc_dict", &entry.path))
                .selected_text(combo_label)
                .width(280.0)
                .show_ui(ui, |ui| {
                    if cur_t != [0; 4] || cur_c != [0; 4] {
                        let label = match cur_desc {
                            Some(d) => {
                                format!("(current) {cur_t_str}/{cur_c_str} - {d}")
                            }
                            None => format!("(current) {cur_t_str}/{cur_c_str}"),
                        };
                        if ui.selectable_label(false, label).clicked() {
                            picked = Some((cur_t, cur_c));
                        }
                        ui.separator();
                    }
                    for (label, t, c) in rules::known_type_creators() {
                        if ui.selectable_label(false, label).clicked() {
                            picked = Some((t, c));
                        }
                    }
                });
            if let Some((t, c)) = picked {
                state.type_input = String::from_utf8_lossy(&t).to_string();
                state.creator_input = String::from_utf8_lossy(&c).to_string();
            }

            // Encode current text into 4-byte arrays (space-padded).
            let typed_t = rules::encode_fourcc(&state.type_input);
            let typed_c = rules::encode_fourcc(&state.creator_input);
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
        let edit = MetadataEdit::TypeCreator {
            entry: entry.clone(),
            type_code: t,
            creator_code: c,
        };
        if let Some(msg) = sink.stage(edit) {
            *result = Some(msg);
        }
    }
}

pub use rules::resolved_prodos_type;

/// Render the ProDOS type/aux row beneath the file-info header. Read-only
/// when not in edit mode; full editor (type pulldown + 2-char/4-char hex
/// inputs + Set/Reset) when edit mode is on.
pub fn render_prodos_type_row(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    edit_mode: bool,
    sink: &mut dyn MetadataSink,
    editor: &mut Option<ProdosTypeEditorState>,
    result: &mut Option<String>,
) {
    let cur = rules::prodos_type(sink.queue(), entry);
    let (cur_t, cur_a) = (cur.type_byte, cur.aux_type);
    let cur_known = cur.known;
    let cur_abbr = cur.abbr;
    let cur_desc = cur.description;

    if !edit_mode {
        ui.horizontal(|ui| {
            let label = format!("Type: ${cur_t:02X} {cur_abbr}  Aux: ${cur_a:04X}  ({cur_desc})");
            if cur_known {
                ui.label(label);
            } else {
                ui.colored_label(super::theme::info(ui.visuals()), label);
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
            let combo_label = format!("${cur_t:02X} {cur_abbr} - {cur_desc}");
            let mut picked: Option<u8> = None;
            egui::ComboBox::from_id_salt(("prodos_type_combo", &entry.path))
                .selected_text(combo_label)
                .width(320.0)
                .show_ui(ui, |ui| {
                    for (byte, label) in rules::prodos_type_choices() {
                        if ui.selectable_label(byte == cur_t, label).clicked() {
                            picked = Some(byte);
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

            let parsed = rules::parse_prodos_type(&state.type_input, &state.aux_input);
            let valid = parsed.is_some();
            let changed = parsed.is_some_and(|(t, a)| t != cur_t || a != cur_a);

            if ui
                .add_enabled(valid && changed, egui::Button::new("Set"))
                .on_hover_text("Stage type/aux change")
                .clicked()
            {
                stage_action = parsed;
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
                    super::theme::danger(ui.visuals()),
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
        let edit = MetadataEdit::ProdosType {
            entry: entry.clone(),
            type_byte: t,
            aux_type: a,
        };
        if let Some(msg) = sink.stage(edit) {
            *result = Some(msg);
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
/// staging a [`MetadataEdit::Dates`] when edit mode is on. Dates are sourced
/// from `entry.mac_dates` (overlaid by any pending staged value).
pub fn render_hfs_dates_row(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    edit_mode: bool,
    sink: &mut dyn MetadataSink,
    editor: &mut Option<HfsDatesEditorState>,
    result: &mut Option<String>,
) {
    let (cur_c, cur_m, cur_b) = rules::hfs_dates(sink.queue(), entry);
    let fmt = |secs: u32| rules::hfs_date_text(secs).unwrap_or_else(|| "(none)".to_string());

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
    let blank = |secs: u32| rules::hfs_date_text(secs).unwrap_or_default();
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

        let parsed = rules::parse_hfs_dates(&state.create, &state.modify, &state.backup);
        let field_err = parsed.as_ref().err().cloned();
        let valid = field_err.is_none();
        let changed = parsed
            .as_ref()
            .is_ok_and(|dates| *dates != (cur_c, cur_m, cur_b));

        ui.horizontal(|ui| {
            if ui
                .add_enabled(valid && changed, egui::Button::new("Set dates"))
                .on_hover_text("Stage create/modify/backup date change")
                .clicked()
            {
                stage_action = parsed.as_ref().ok().copied();
            }
            if ui.button("Reset").clicked() {
                reset_action = true;
            }
        });
        if let Some(msg) = &field_err {
            ui.colored_label(super::theme::danger(ui.visuals()), msg);
        }
    }

    if reset_action {
        if let Some(state) = editor.as_mut() {
            state.create = blank(cur_c);
            state.modify = blank(cur_m);
            state.backup = blank(cur_b);
        }
    }

    if let Some((c, m, b)) = stage_action {
        let edit = MetadataEdit::Dates {
            entry: entry.clone(),
            create: c,
            modify: m,
            backup: b,
        };
        if let Some(msg) = sink.stage(edit) {
            *result = Some(msg);
        }
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
/// [`MetadataEdit::Permissions`] when edit mode is on. Sources the current
/// mode from `entry.mode` (overlaid by any pending staged value).
pub fn render_ext_permissions_row(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    edit_mode: bool,
    sink: &mut dyn MetadataSink,
    editor: &mut Option<ExtPermsEditorState>,
    result: &mut Option<String>,
) {
    let cur = rules::permissions(sink.queue(), entry);

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
            let parsed = rules::parse_permissions(&state.octal);
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
                ui.colored_label(super::theme::danger(ui.visuals()), "1-4 octal digits");
            }
        });
    }

    if reset_action {
        if let Some(state) = editor.as_mut() {
            state.octal = format!("{cur:o}");
        }
    }

    if let Some(mode) = stage_action {
        let edit = MetadataEdit::Permissions {
            entry: entry.clone(),
            mode,
        };
        if let Some(msg) = sink.stage(edit) {
            *result = Some(msg);
        }
    }
}

/// Inline editor state for an entry's owning uid/gid.
#[derive(Default)]
pub struct OwnerEditorState {
    entry_path: String,
    uid: String,
    gid: String,
}

/// Render the owner (uid/gid) row: read-only text when not in edit mode
/// (resolved to names via `id_names` when available), two numeric fields + Set
/// when editing. Stages a [`MetadataEdit::Owner`]. Unix filesystems only.
pub fn render_owner_row(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    edit_mode: bool,
    id_names: Option<&IdNameMap>,
    sink: &mut dyn MetadataSink,
    editor: &mut Option<OwnerEditorState>,
    result: &mut Option<String>,
) {
    let (cur_uid, cur_gid) = rules::owner(sink.queue(), entry);

    if !edit_mode {
        let shown = match (&entry.owner_label, id_names) {
            (Some(label), _) => label.clone(),
            (None, Some(n)) => n.format_owner(cur_uid, cur_gid),
            (None, None) => format!("{cur_uid}:{cur_gid}"),
        };
        ui.label(format!("Owner: {shown}"));
        if let Some(attrs) = &entry.native_attrs {
            ui.label(format!("Attributes: {attrs}"));
        }
        return;
    }

    let needs_seed = editor
        .as_ref()
        .map(|s| s.entry_path != entry.path)
        .unwrap_or(true);
    if needs_seed {
        *editor = Some(OwnerEditorState {
            entry_path: entry.path.clone(),
            uid: cur_uid.to_string(),
            gid: cur_gid.to_string(),
        });
    }

    let mut stage: Option<(u32, u32)> = None;
    let mut reset = false;
    if let Some(state) = editor.as_mut() {
        ui.horizontal(|ui| {
            ui.label("Owner uid:");
            ui.add(
                egui::TextEdit::singleline(&mut state.uid)
                    .desired_width(64.0)
                    .font(egui::TextStyle::Monospace),
            );
            ui.label("gid:");
            ui.add(
                egui::TextEdit::singleline(&mut state.gid)
                    .desired_width(64.0)
                    .font(egui::TextStyle::Monospace),
            );
            let parsed = rules::parse_owner(&state.uid, &state.gid);
            let changed = parsed.map(|p| p != (cur_uid, cur_gid)).unwrap_or(false);
            if ui
                .add_enabled(parsed.is_some() && changed, egui::Button::new("Set"))
                .clicked()
            {
                stage = parsed;
            }
            if ui.button("Reset").clicked() {
                reset = true;
            }
            if parsed.is_none() {
                ui.colored_label(super::theme::danger(ui.visuals()), "numbers");
            }
        });
    }

    if reset {
        if let Some(state) = editor.as_mut() {
            state.uid = cur_uid.to_string();
            state.gid = cur_gid.to_string();
        }
    }
    if let Some((uid, gid)) = stage {
        let edit = MetadataEdit::Owner {
            entry: entry.clone(),
            uid,
            gid,
        };
        if let Some(msg) = sink.stage(edit) {
            *result = Some(msg);
        }
    }
}

/// Inline editor state for an entry's extended attributes: a per-name scratch
/// value being edited, plus the "add new" name/value fields.
#[derive(Default)]
pub struct XattrEditorState {
    entry_path: String,
    /// name -> the value string currently in its edit field (hex or text).
    values: std::collections::HashMap<String, String>,
    new_name: String,
    new_value: String,
}

/// Render the extended-attributes section: a list of `name = value` rows
/// (read-only), and in edit mode a hex/text value field + Set + Delete per
/// attribute plus an Add row. `xattrs` is the on-disk set; pending staged edits
/// are overlaid so the user sees their queued changes. Stages
/// [`MetadataEdit::SetXattr`] / [`MetadataEdit::RemoveXattr`].
pub fn render_xattr_section(
    ui: &mut egui::Ui,
    entry: &FileEntry,
    edit_mode: bool,
    xattrs: &[Xattr],
    sink: &mut dyn MetadataSink,
    editor: &mut Option<XattrEditorState>,
    result: &mut Option<String>,
) {
    let effective = rules::effective_xattrs(sink.queue(), entry, xattrs);

    if effective.is_empty() && !edit_mode {
        return;
    }

    ui.separator();
    ui.label(egui::RichText::new("Extended attributes").strong());

    if !edit_mode {
        for x in &effective {
            ui.label(format!("  {} = {}", x.name, x.value_display()));
        }
        return;
    }

    // Seed per-entry scratch when the selection changes.
    let needs_seed = editor
        .as_ref()
        .map(|s| s.entry_path != entry.path)
        .unwrap_or(true);
    if needs_seed {
        let mut values = std::collections::HashMap::new();
        for x in &effective {
            values.insert(x.name.clone(), x.value_display());
        }
        *editor = Some(XattrEditorState {
            entry_path: entry.path.clone(),
            values,
            new_name: String::new(),
            new_value: String::new(),
        });
    }

    let mut stage_set: Option<(String, Vec<u8>)> = None;
    let mut stage_remove: Option<String> = None;
    let mut err: Option<String> = None;

    if let Some(state) = editor.as_mut() {
        for x in &effective {
            ui.horizontal(|ui| {
                ui.monospace(&x.name);
                let field = state
                    .values
                    .entry(x.name.clone())
                    .or_insert_with(|| x.value_display());
                ui.add(
                    egui::TextEdit::singleline(field)
                        .desired_width(240.0)
                        .font(egui::TextStyle::Monospace),
                );
                if ui.button("Set").clicked() {
                    match rules::parse_xattr_value(field) {
                        Ok(v) => stage_set = Some((x.name.clone(), v)),
                        Err(e) => err = Some(e),
                    }
                }
                if ui.button("Delete").clicked() {
                    stage_remove = Some(x.name.clone());
                }
            });
        }
        // Add-new row.
        ui.horizontal(|ui| {
            ui.label("Add:");
            ui.add(
                egui::TextEdit::singleline(&mut state.new_name)
                    .hint_text("user.name")
                    .desired_width(160.0)
                    .font(egui::TextStyle::Monospace),
            );
            ui.add(
                egui::TextEdit::singleline(&mut state.new_value)
                    .hint_text("value or 0xHEX")
                    .desired_width(180.0)
                    .font(egui::TextStyle::Monospace),
            );
            let name_ok = rules::valid_xattr_name(state.new_name.trim());
            if ui
                .add_enabled(name_ok, egui::Button::new("Add"))
                .on_hover_text("Name must start with user. / trusted. / security. / system.")
                .clicked()
            {
                match rules::parse_xattr_value(&state.new_value) {
                    Ok(v) => stage_set = Some((state.new_name.trim().to_string(), v)),
                    Err(e) => err = Some(e),
                }
            }
        });
        if let Some(e) = &err {
            ui.colored_label(super::theme::danger(ui.visuals()), e);
        }
    }

    if let Some((name, value)) = stage_set {
        let edit = MetadataEdit::SetXattr {
            entry: entry.clone(),
            name,
            value,
        };
        if let Some(msg) = sink.stage(edit) {
            *result = Some(msg);
        }
        if let Some(state) = editor.as_mut() {
            state.new_name.clear();
            state.new_value.clear();
        }
    }
    if let Some(name) = stage_remove {
        let edit = MetadataEdit::RemoveXattr {
            entry: entry.clone(),
            name,
        };
        if let Some(msg) = sink.stage(edit) {
            *result = Some(msg);
        }
    }
}
