//! The CHD codec and hunk choices the shared CHD options control offers, and its rules.
//!
//! [`ChdOptionsControl`] is the state of the "Advanced CHD options" control every shell embeds
//! (Backup, Optical convert, Bulk convert): a preset mode and the Custom values. The presets,
//! the hunk and codec lists and the validity rule live here so the egui widget and the TUI rows
//! offer the same choices.

pub use crate::rbformats::chd_options::{
    codec_label, codec_long_name, is_codec_supported, ChdOptions, ChdProfile,
};
#[cfg(feature = "rust173-polyfill")]
use crate::rust173_compat::IntIsMultipleOf as _;
pub use libchdman_rs::{
    CHD_CODEC_CD_FLAC, CHD_CODEC_CD_LZMA, CHD_CODEC_CD_ZLIB, CHD_CODEC_CD_ZSTD, CHD_CODEC_FLAC,
    CHD_CODEC_HUFF, CHD_CODEC_LZMA, CHD_CODEC_NONE, CHD_CODEC_ZLIB, CHD_CODEC_ZSTD,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChdOptionsMode {
    /// Per-profile chdman defaults (`ChdOptions::defaults_for`).
    Default,
    /// MiSTer CD-ROM target: `cdzs,cdfl` and 9792-byte (4-frame) hunks; CD profile only.
    MisterCdOptimized,
    /// Hand-tuned values from `custom`.
    Custom,
}

impl ChdOptionsMode {
    pub fn label(self) -> &'static str {
        match self {
            ChdOptionsMode::Default => "Default",
            ChdOptionsMode::MisterCdOptimized => "MiSTer CD-ROM Optimized",
            ChdOptionsMode::Custom => "Custom",
        }
    }
}

/// The user's preset choice and any custom hunk / codec values.
#[derive(Debug, Clone)]
pub struct ChdOptionsControl {
    pub mode: ChdOptionsMode,
    /// Used only in Custom mode; seeded with the profile defaults.
    pub custom: ChdOptions,
}

impl ChdOptionsControl {
    pub fn new(profile: ChdProfile) -> Self {
        Self {
            mode: ChdOptionsMode::Default,
            custom: ChdOptions::defaults_for(profile),
        }
    }

    /// The active preset as concrete options for `profile`.
    pub fn effective(&self, profile: ChdProfile) -> ChdOptions {
        match self.mode {
            ChdOptionsMode::Default => ChdOptions::defaults_for(profile),
            ChdOptionsMode::MisterCdOptimized => mister_cd_preset(),
            ChdOptionsMode::Custom => self.custom.clone(),
        }
    }

    /// Only a Custom hunk can be misaligned with the format's unit.
    pub fn is_valid(&self, profile: ChdProfile) -> bool {
        self.validate(profile).is_ok()
    }

    /// Why the active preset cannot be used, or `Ok`.
    pub fn validate(&self, profile: ChdProfile) -> Result<(), String> {
        let unit = unit_size_for(profile);
        if self.effective(profile).hunk_size.is_multiple_of(unit) {
            Ok(())
        } else {
            Err(format!("hunk must be a multiple of {unit}"))
        }
    }
}

/// The modes offered for `profile` (the MiSTer preset is a CD one).
pub fn modes_for(profile: ChdProfile) -> Vec<ChdOptionsMode> {
    if profile == ChdProfile::Cd {
        vec![
            ChdOptionsMode::Default,
            ChdOptionsMode::MisterCdOptimized,
            ChdOptionsMode::Custom,
        ]
    } else {
        vec![ChdOptionsMode::Default, ChdOptionsMode::Custom]
    }
}

/// MiSTer decodes `cdzs` + `cdfl` natively; 4-frame hunks keep seeks short.
fn mister_cd_preset() -> ChdOptions {
    ChdOptions {
        hunk_size: 9792,
        codecs: [CHD_CODEC_CD_ZSTD, CHD_CODEC_CD_FLAC, 0, 0],
    }
}

/// The format's unit in bytes; a hunk is a multiple of it.
pub fn unit_size_for(profile: ChdProfile) -> u32 {
    match profile {
        ChdProfile::Hd => 512,
        ChdProfile::Cd => 2448,
        ChdProfile::Dvd => 2048,
    }
}

/// The hunk sizes offered for `profile`.
pub fn hunk_presets(profile: ChdProfile) -> &'static [u32] {
    match profile {
        ChdProfile::Hd => &[4096, 8192, 16384, 32768],
        ChdProfile::Dvd => &[4096, 16384, 32768, 65536],
        ChdProfile::Cd => &[19584, 9792, 9408, 39168],
    }
}

/// The codecs a slot offers for `profile`: those this build supports, then none.
pub fn codec_choices(profile: ChdProfile) -> Vec<u32> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_custom_hunk_off_the_unit_is_refused() {
        let mut c = ChdOptionsControl::new(ChdProfile::Hd);
        assert!(c.is_valid(ChdProfile::Hd));
        c.mode = ChdOptionsMode::Custom;
        c.custom.hunk_size = 1000;
        assert_eq!(
            c.validate(ChdProfile::Hd),
            Err("hunk must be a multiple of 512".to_string())
        );
        c.mode = ChdOptionsMode::Default;
        assert!(c.is_valid(ChdProfile::Hd));
    }

    #[test]
    fn the_mister_preset_is_offered_for_cds_only() {
        assert_eq!(modes_for(ChdProfile::Hd).len(), 2);
        assert!(modes_for(ChdProfile::Cd).contains(&ChdOptionsMode::MisterCdOptimized));
        assert_eq!(codec_choices(ChdProfile::Hd).last(), Some(&CHD_CODEC_NONE));
    }
}
