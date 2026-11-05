//! Reusable "Advanced CHD options" widget.
//!
//! Draws `controller::app::chd::ChdOptionsControl`: the preset modes, and in Custom the hunk-size
//! and codec-slot dropdowns. The choices and the validity rule are the controller's.

use rusty_backup::controller::app::chd::{
    codec_choices, codec_label, codec_long_name, hunk_presets, modes_for, unit_size_for, ChdProfile,
};
pub use rusty_backup::controller::app::chd::{ChdOptionsControl, ChdOptionsMode};

/// Render the widget. `id_prefix` must be unique across the page (e.g.
/// `"backup_chd"`, `"optical_chd"`, `"bulk_chd"`) so combo-box ids don't collide.
pub fn show(
    ui: &mut egui::Ui,
    id_prefix: &str,
    profile: ChdProfile,
    state: &mut ChdOptionsControl,
) {
    ui.horizontal(|ui| {
        ui.label("Advanced:");
        for mode in modes_for(profile) {
            let r = ui.radio_value(&mut state.mode, mode, mode.label());
            if mode == ChdOptionsMode::MisterCdOptimized {
                r.on_hover_text("cdzs,cdfl with 9792-byte (4-frame) hunks");
            }
        }
    });

    if state.mode != ChdOptionsMode::Custom {
        return;
    }

    let opts = &mut state.custom;
    let unit = unit_size_for(profile);
    ui.indent(format!("{id_prefix}_indent"), |ui| {
        ui.horizontal(|ui| {
            ui.label("Hunk size:");
            let presets = hunk_presets(profile);
            let current = opts.hunk_size;
            let label = if presets.contains(&current) {
                format!("{current}")
            } else {
                format!("{current} (custom)")
            };
            egui::ComboBox::from_id_salt(format!("{id_prefix}_hunk"))
                .selected_text(label)
                .show_ui(ui, |ui| {
                    for &p in presets {
                        ui.selectable_value(&mut opts.hunk_size, p, format!("{p}"));
                    }
                });
            ui.label(format!("(unit {unit} bytes)"));
            let check = ChdOptionsControl {
                mode: ChdOptionsMode::Custom,
                custom: opts.clone(),
            };
            if let Err(e) = check.validate(profile) {
                ui.colored_label(super::theme::danger(ui.visuals()), e);
            }
        });

        ui.horizontal_wrapped(|ui| {
            ui.label("Codecs:");
            let choices = codec_choices(profile);
            for slot in 0..4 {
                let current = opts.codecs[slot];
                let combo_id = format!("{id_prefix}_codec_{slot}");
                egui::ComboBox::from_id_salt(combo_id)
                    .selected_text(codec_label(current))
                    .show_ui(ui, |ui| {
                        for &c in &choices {
                            ui.selectable_value(&mut opts.codecs[slot], c, codec_long_name(c));
                        }
                    });
            }
        });
    });
}
