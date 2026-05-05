//! CHD creation options: hunk size + codec slot configuration.
//!
//! Single source of truth for "how to write a CHD" — every backup,
//! optical convert, and bulk-convert call site takes a `ChdOptions`
//! and translates it into the appropriate `libchdman_rs::{hd,cd,dvd}::*CreateOptions`.
//!
//! Defaults match `chdman`'s built-in defaults so that switching to the
//! native pipeline produces byte-equivalent outputs without UI changes.

use anyhow::{anyhow, Result};
use libchdman_rs::{
    codec_exists, codec_name, parse_codec_spec, CHD_CODEC_CD_FLAC, CHD_CODEC_CD_LZMA,
    CHD_CODEC_CD_ZLIB, CHD_CODEC_FLAC, CHD_CODEC_HUFF, CHD_CODEC_LZMA, CHD_CODEC_NONE,
    CHD_CODEC_ZLIB,
};

/// Which CHD format we're creating. Picks the right hunk size, codec
/// defaults, and unit size at the libchdman-rs layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChdProfile {
    /// Raw hard-disk CHD (`createraw`/`createhd`). Sector-stream of
    /// arbitrary size, 512-byte units, no track structure.
    Hd,
    /// CD-ROM CHD (`createcd`). 2448-byte frames, track metadata.
    Cd,
    /// DVD CHD (`createdvd`, MAME 0.287+). Flat 2048-byte sectors.
    Dvd,
}

/// Knobs the user can set when we create a CHD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChdOptions {
    /// Hunk size in bytes. Must be a multiple of the format's unit size
    /// (512 for HD, 2448 for CD, 2048 for DVD).
    pub hunk_size: u32,
    /// Codec slot configuration (FourCCs, slot 0..=3). `0` = unused.
    /// Use `parse_codec_string` to build from chdman-style mnemonics.
    pub codecs: [u32; 4],
}

impl ChdOptions {
    /// chdman's defaults for each profile, matched exactly so output
    /// stays byte-equivalent against `chdman` for the same input.
    pub fn defaults_for(profile: ChdProfile) -> Self {
        match profile {
            // chdman.cpp `s_default_hd_compression`.
            ChdProfile::Hd => Self {
                hunk_size: 4096,
                codecs: [
                    CHD_CODEC_LZMA,
                    CHD_CODEC_ZLIB,
                    CHD_CODEC_HUFF,
                    CHD_CODEC_FLAC,
                ],
            },
            // chdman.cpp `s_default_cd_compression` is `cdlz, cdzl, cdfl`;
            // libchdman-rs and chdman both default the trailing slot to NONE.
            ChdProfile::Cd => Self {
                hunk_size: 8 * 2448, // 19584; FRAMES_PER_HUNK * (2352 + 96)
                codecs: [
                    CHD_CODEC_CD_LZMA,
                    CHD_CODEC_CD_ZLIB,
                    CHD_CODEC_CD_FLAC,
                    CHD_CODEC_NONE,
                ],
            },
            // chdman.cpp `do_create_dvd` reuses the HD default codec set.
            ChdProfile::Dvd => Self {
                hunk_size: 2 * 2048, // chdman default
                codecs: [
                    CHD_CODEC_LZMA,
                    CHD_CODEC_ZLIB,
                    CHD_CODEC_HUFF,
                    CHD_CODEC_FLAC,
                ],
            },
        }
    }
}

/// Parse chdman's `-c` syntax (`"cdlz,cdzl,cdfl"`, `"none"`, etc.) into
/// a 4-slot codec array. Validates each codec via libchdman-rs's
/// `codec_exists`. Errors are surfaced with the offending input so UI
/// validation can show a useful message.
pub fn parse_codec_string(s: &str) -> Result<[u32; 4]> {
    parse_codec_spec(s).map_err(|_| anyhow!("invalid codec spec: {s:?}"))
}

/// FourCC → 4-character mnemonic for UI display, e.g. `0x63647a73 -> "cdzs"`.
/// Returns `"none"` for `CHD_CODEC_NONE`. Falls back to a hex string if any
/// of the four bytes isn't printable ASCII.
pub fn codec_label(code: u32) -> String {
    if code == CHD_CODEC_NONE {
        return "none".to_string();
    }
    let bytes = [
        ((code >> 24) & 0xff) as u8,
        ((code >> 16) & 0xff) as u8,
        ((code >> 8) & 0xff) as u8,
        (code & 0xff) as u8,
    ];
    if bytes.iter().all(|b| (0x20..=0x7e).contains(b)) {
        // SAFETY: all four bytes are printable ASCII.
        String::from_utf8(bytes.to_vec()).unwrap()
    } else {
        format!("{code:08x}")
    }
}

/// Long human-readable name from MAME's codec table (e.g. `"CD FLAC"`).
/// Falls back to the FourCC label when MAME has no entry — useful for
/// the codec dropdown's tooltip text.
pub fn codec_long_name(code: u32) -> String {
    if code == CHD_CODEC_NONE {
        return "None (uncompressed)".to_string();
    }
    codec_name(code)
        .map(str::to_owned)
        .unwrap_or_else(|| codec_label(code))
}

/// True when the build's MAME core supports `code`. UI codec pickers
/// should filter against this so users can't pick a codec that the
/// linked library can't actually handle.
pub fn is_codec_supported(code: u32) -> bool {
    code == CHD_CODEC_NONE || codec_exists(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_chdman_hd() {
        let opts = ChdOptions::defaults_for(ChdProfile::Hd);
        assert_eq!(opts.hunk_size, 4096);
        assert_eq!(
            opts.codecs,
            [
                CHD_CODEC_LZMA,
                CHD_CODEC_ZLIB,
                CHD_CODEC_HUFF,
                CHD_CODEC_FLAC
            ]
        );
    }

    #[test]
    fn defaults_match_chdman_cd() {
        let opts = ChdOptions::defaults_for(ChdProfile::Cd);
        assert_eq!(opts.hunk_size, 19584);
        assert_eq!(opts.codecs[0], CHD_CODEC_CD_LZMA);
        assert_eq!(opts.codecs[1], CHD_CODEC_CD_ZLIB);
        assert_eq!(opts.codecs[2], CHD_CODEC_CD_FLAC);
        assert_eq!(opts.codecs[3], CHD_CODEC_NONE);
    }

    #[test]
    fn defaults_match_chdman_dvd() {
        let opts = ChdOptions::defaults_for(ChdProfile::Dvd);
        assert_eq!(opts.hunk_size, 4096);
        assert_eq!(
            opts.codecs,
            [
                CHD_CODEC_LZMA,
                CHD_CODEC_ZLIB,
                CHD_CODEC_HUFF,
                CHD_CODEC_FLAC
            ]
        );
    }

    #[test]
    fn parse_codec_string_basic() {
        let codecs = parse_codec_string("cdlz,cdzl,cdfl").unwrap();
        assert_eq!(codecs[0], CHD_CODEC_CD_LZMA);
        assert_eq!(codecs[1], CHD_CODEC_CD_ZLIB);
        assert_eq!(codecs[2], CHD_CODEC_CD_FLAC);
        assert_eq!(codecs[3], CHD_CODEC_NONE);
    }

    #[test]
    fn parse_codec_string_none() {
        assert_eq!(parse_codec_string("none").unwrap(), [CHD_CODEC_NONE; 4]);
    }

    #[test]
    fn parse_codec_string_rejects_garbage() {
        assert!(parse_codec_string("xxxx").is_err());
        assert!(parse_codec_string("").is_err());
        assert!(parse_codec_string("cdlz,cdzl,cdfl,zlib,zstd").is_err()); // > 4 slots
    }

    #[test]
    fn codec_label_roundtrip() {
        assert_eq!(codec_label(CHD_CODEC_CD_LZMA), "cdlz");
        assert_eq!(codec_label(CHD_CODEC_CD_ZLIB), "cdzl");
        assert_eq!(codec_label(CHD_CODEC_CD_FLAC), "cdfl");
        assert_eq!(codec_label(CHD_CODEC_LZMA), "lzma");
        assert_eq!(codec_label(CHD_CODEC_NONE), "none");
    }

    #[test]
    fn known_codecs_are_supported() {
        assert!(is_codec_supported(CHD_CODEC_NONE));
        assert!(is_codec_supported(CHD_CODEC_LZMA));
        assert!(is_codec_supported(CHD_CODEC_ZLIB));
        assert!(is_codec_supported(CHD_CODEC_CD_LZMA));
    }
}
