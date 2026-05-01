//! Reusable "Advanced CHD options" widget.
//!
//! Three preset modes: `Default` (chdman per-profile defaults),
//! `MisterCdOptimized` (`cdzs,cdfl` + 9792-byte hunks, CD profile only), and
//! `Custom` (exposes hunk-size + codec-slot dropdowns for hand tuning).
//! Backup, optical-convert, and the upcoming bulk-convert flows all embed
//! the same widget so the UX stays consistent.

use libchdman_rs::{
    CHD_CODEC_CD_FLAC, CHD_CODEC_CD_LZMA, CHD_CODEC_CD_ZLIB, CHD_CODEC_CD_ZSTD, CHD_CODEC_FLAC,
    CHD_CODEC_HUFF, CHD_CODEC_LZMA, CHD_CODEC_NONE, CHD_CODEC_ZLIB, CHD_CODEC_ZSTD,
};
use rusty_backup::rbformats::chd_options::{
    codec_label, codec_long_name, is_codec_supported, ChdOptions, ChdProfile,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChdOptionsMode {
    /// Per-profile chdman defaults — what `ChdOptions::defaults_for(profile)` returns.
    Default,
    /// MiSTer FPGA CD-ROM target: `cdzs,cdfl` codecs + 9792-byte hunks
    /// (4 frames * 2448). Only meaningful for `ChdProfile::Cd`.
    MisterCdOptimized,
    /// Hand-tuned values from `custom`.
    Custom,
}

/// Holds the user's mode selection and any custom hunk/codec choice.
/// Re-used across backup_tab, optical_tab, and bulk-convert.
#[derive(Debug, Clone)]
pub struct ChdOptionsControl {
    pub mode: ChdOptionsMode,
    /// User-tuned values, used only when `mode == Custom`. Pre-populated
    /// with the profile defaults on `new()` so the dropdowns have sensible
    /// starting positions if/when the user flips into Custom.
    pub custom: ChdOptions,
}

impl ChdOptionsControl {
    pub fn new(profile: ChdProfile) -> Self {
        Self {
            mode: ChdOptionsMode::Default,
            custom: ChdOptions::defaults_for(profile),
        }
    }

    /// Resolve the active preset into a concrete `ChdOptions` for the given profile.
    pub fn effective(&self, profile: ChdProfile) -> ChdOptions {
        match self.mode {
            ChdOptionsMode::Default => ChdOptions::defaults_for(profile),
            ChdOptionsMode::MisterCdOptimized => mister_cd_preset(),
            ChdOptionsMode::Custom => self.custom.clone(),
        }
    }

    /// True when the active preset is valid for the given profile (chiefly:
    /// hunk size aligns to the format unit). Default + MiSTer presets are
    /// always valid by construction; only Custom can be misaligned.
    pub fn is_valid(&self, profile: ChdProfile) -> bool {
        let opts = self.effective(profile);
        opts.hunk_size % unit_size_for(profile) == 0
    }
}

/// MiSTer CD-ROM hardware decodes `cdzs` + `cdfl` natively at the lowest
/// possible CPU cost; 4-frame (9792-byte) hunks keep seek latency low.
fn mister_cd_preset() -> ChdOptions {
    ChdOptions {
        hunk_size: 9792,
        codecs: [CHD_CODEC_CD_ZSTD, CHD_CODEC_CD_FLAC, 0, 0],
    }
}

fn unit_size_for(profile: ChdProfile) -> u32 {
    match profile {
        ChdProfile::Hd => 512,
        ChdProfile::Cd => 2448,
        ChdProfile::Dvd => 2048,
    }
}

fn hunk_presets(profile: ChdProfile) -> &'static [u32] {
    match profile {
        ChdProfile::Hd => &[4096, 8192, 16384, 32768],
        ChdProfile::Dvd => &[4096, 16384, 32768, 65536],
        ChdProfile::Cd => &[19584, 9792, 9408, 39168],
    }
}

fn codec_choices(profile: ChdProfile) -> Vec<u32> {
    let candidates: &[u32] = match profile {
        ChdProfile::Hd | ChdProfile::Dvd => &[
            CHD_CODEC_LZMA,
            CHD_CODEC_ZLIB,
            CHD_CODEC_ZSTD,
            CHD_CODEC_HUFF,
            CHD_CODEC_FLAC,
        ],
        ChdProfile::Cd => &[
            CHD_CODEC_CD_LZMA,
            CHD_CODEC_CD_ZLIB,
            CHD_CODEC_CD_ZSTD,
            CHD_CODEC_CD_FLAC,
        ],
    };
    let mut out: Vec<u32> = candidates
        .iter()
        .copied()
        .filter(|c| is_codec_supported(*c))
        .collect();
    out.push(CHD_CODEC_NONE);
    out
}

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
        ui.radio_value(&mut state.mode, ChdOptionsMode::Default, "Default");
        if profile == ChdProfile::Cd {
            ui.radio_value(
                &mut state.mode,
                ChdOptionsMode::MisterCdOptimized,
                "MiSTer CD-ROM Optimized",
            )
            .on_hover_text("cdzs,cdfl with 9792-byte (4-frame) hunks");
        }
        ui.radio_value(&mut state.mode, ChdOptionsMode::Custom, "Custom");
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
            if opts.hunk_size % unit != 0 {
                ui.colored_label(
                    egui::Color32::from_rgb(220, 80, 80),
                    format!("hunk must be a multiple of {unit}"),
                );
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
