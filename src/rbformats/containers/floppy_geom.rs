//! Floppy geometry common to the X68000 / PC-98 corpus.
//!
//! Used by the [`xdf`](super::xdf), [`hdm`](super::hdm), and [`dim`](super::dim)
//! container decoders/encoders, and by the [`d88`](super::d88) bridge helper.
//! A geometry tuple is `(cyls, heads, sec/track, sec_size)`; the four media
//! kinds we support each map to a unique total byte count, so size-based
//! inference is unambiguous within this set.

use anyhow::{anyhow, bail, Result};

/// Concrete floppy media kind supported by the floppy-container family
/// (XDF / HDM / DIM / D88) in this crate. Each variant has exactly one
/// `(cyls, heads, sec/track, sec_size)` tuple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloppyMedia {
    /// Sharp X68000 2HD: 77 cyls x 2 heads x 8 sec x 1024 B = 1,261,568 B.
    /// Common label: "1.2 MB 2HD".
    Hd1232,
    /// IBM/PC 2HD: 80 x 2 x 18 x 512 = 1,474,560 B. Common label: "1.44 MB 2HD".
    Hd1440,
    /// PC-98 / generic 2DD: 80 x 2 x 9 x 512 = 737,280 B. Common label: "720 KB 2DD".
    Dd720,
    /// PC-98 2DD: 80 x 2 x 8 x 512 = 655,360 B. Common label: "640 KB 2DD".
    Dd640,
}

/// Decoded floppy geometry. Used by every floppy-container encoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloppyGeometry {
    pub cyls: u8,
    pub heads: u8,
    pub spt: u8,
    pub sec_size: u16,
}

impl FloppyGeometry {
    pub const fn flat_size(self) -> usize {
        (self.cyls as usize)
            * (self.heads as usize)
            * (self.spt as usize)
            * (self.sec_size as usize)
    }
}

impl FloppyMedia {
    pub const fn geometry(self) -> FloppyGeometry {
        match self {
            FloppyMedia::Hd1232 => FloppyGeometry {
                cyls: 77,
                heads: 2,
                spt: 8,
                sec_size: 1024,
            },
            FloppyMedia::Hd1440 => FloppyGeometry {
                cyls: 80,
                heads: 2,
                spt: 18,
                sec_size: 512,
            },
            FloppyMedia::Dd720 => FloppyGeometry {
                cyls: 80,
                heads: 2,
                spt: 9,
                sec_size: 512,
            },
            FloppyMedia::Dd640 => FloppyGeometry {
                cyls: 80,
                heads: 2,
                spt: 8,
                sec_size: 512,
            },
        }
    }

    /// CLI flag value (e.g. `--media 2hd1200`).
    pub const fn flag(self) -> &'static str {
        match self {
            FloppyMedia::Hd1232 => "2hd1200",
            FloppyMedia::Hd1440 => "2hd1440",
            FloppyMedia::Dd720 => "2dd720",
            FloppyMedia::Dd640 => "2dd640",
        }
    }

    /// Plain-ASCII display label for log / inspect rows.
    pub const fn display_label(self) -> &'static str {
        match self {
            FloppyMedia::Hd1232 => "1.2 MB 2HD (X68000)",
            FloppyMedia::Hd1440 => "1.44 MB 2HD",
            FloppyMedia::Dd720 => "720 KB 2DD",
            FloppyMedia::Dd640 => "640 KB 2DD",
        }
    }

    /// Parse from the `--media` CLI flag.
    pub fn from_flag(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "2hd1200" | "2hd1232" => Ok(Self::Hd1232),
            "2hd1440" => Ok(Self::Hd1440),
            "2dd720" => Ok(Self::Dd720),
            "2dd640" => Ok(Self::Dd640),
            other => bail!(
                "unknown floppy media flag {other:?} \
                 (expected 2hd1200|2hd1440|2dd720|2dd640)"
            ),
        }
    }
}

/// All supported media kinds. Iterated by size-based inference.
pub const ALL_MEDIA: &[FloppyMedia] = &[
    FloppyMedia::Hd1232,
    FloppyMedia::Hd1440,
    FloppyMedia::Dd720,
    FloppyMedia::Dd640,
];

/// Infer media kind from a raw byte length. `None` if no supported geometry
/// matches exactly.
pub fn infer_media_from_size(size: usize) -> Option<FloppyMedia> {
    ALL_MEDIA
        .iter()
        .copied()
        .find(|m| m.geometry().flat_size() == size)
}

/// Same as [`infer_media_from_size`] but returns a typed error.
pub fn require_media_from_size(size: usize) -> Result<FloppyMedia> {
    infer_media_from_size(size).ok_or_else(|| {
        anyhow!(
            "no supported floppy geometry matches {size} bytes \
             (expected one of: 1,261,568 / 1,474,560 / 737,280 / 655,360)"
        )
    })
}

/// Shared encode/decode of a raw flat stream — XDF and HDM both use this.
/// Returns `(bytes, inferred-media)` on decode and `bytes` verbatim on encode
/// after a geometry-vs-length sanity check.
pub fn decode_raw_floppy_bytes(bytes: &[u8], format_label: &str) -> Result<(Vec<u8>, FloppyMedia)> {
    let media = require_media_from_size(bytes.len())
        .map_err(|e| anyhow!("{format_label}: {e}", format_label = format_label, e = e))?;
    Ok((bytes.to_vec(), media))
}

pub fn encode_raw_floppy_bytes(
    flat: &[u8],
    geom: FloppyGeometry,
    format_label: &str,
) -> Result<Vec<u8>> {
    let expected = geom.flat_size();
    if flat.len() != expected {
        bail!(
            "{format_label} encode: flat size {} mismatches geometry-derived {} \
             ({}cyl x {}heads x {}sec x {}B)",
            flat.len(),
            expected,
            geom.cyls,
            geom.heads,
            geom.spt,
            geom.sec_size,
        );
    }
    Ok(flat.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_media_kind_has_unique_size() {
        let mut sizes: Vec<usize> = ALL_MEDIA.iter().map(|m| m.geometry().flat_size()).collect();
        sizes.sort();
        sizes.dedup();
        assert_eq!(sizes.len(), ALL_MEDIA.len());
    }

    #[test]
    fn x68k_2hd_geometry_matches_77x2x8x1024() {
        let g = FloppyMedia::Hd1232.geometry();
        assert_eq!(g.cyls, 77);
        assert_eq!(g.heads, 2);
        assert_eq!(g.spt, 8);
        assert_eq!(g.sec_size, 1024);
        assert_eq!(g.flat_size(), 1_261_568);
    }

    #[test]
    fn infer_media_recognises_all_supported_sizes() {
        for m in ALL_MEDIA {
            assert_eq!(infer_media_from_size(m.geometry().flat_size()), Some(*m));
        }
    }

    #[test]
    fn infer_media_rejects_unknown_size() {
        assert!(infer_media_from_size(12345).is_none());
        assert!(infer_media_from_size(0).is_none());
    }

    #[test]
    fn from_flag_round_trips() {
        for m in ALL_MEDIA {
            assert_eq!(FloppyMedia::from_flag(m.flag()).unwrap(), *m);
        }
        assert!(FloppyMedia::from_flag("garbage").is_err());
    }
}
