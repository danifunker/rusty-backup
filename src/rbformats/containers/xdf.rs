//! XDF — X68000 Disk File. Raw headerless sector dump.
//!
//! Layout: bytes 0..N are the concatenated track-major sector stream. No
//! header, no per-track records, no sparse encoding. Geometry is inferred
//! from the file size via [`floppy_geom::infer_media_from_size`].
//!
//! HDM uses an identical on-disk format (see the [`super::hdm`] module);
//! both delegate to [`floppy_geom::decode_raw_floppy_bytes`] /
//! [`floppy_geom::encode_raw_floppy_bytes`] so the only thing that differs
//! between the two is the file extension and a log-line label.

use std::path::Path;

use anyhow::Result;

use super::floppy_geom::{
    decode_raw_floppy_bytes, encode_raw_floppy_bytes, infer_media_from_size, FloppyGeometry,
    FloppyMedia,
};

const FORMAT_LABEL: &str = "XDF";

/// Cheap detection: path extension is `.xdf` (case-insensitive) AND the byte
/// length matches one of the supported floppy geometries. Returning `true`
/// here just routes the file to [`decode_xdf_bytes`], which is where any
/// final validation errors surface.
pub fn looks_like_xdf(path: Option<&Path>, size: usize) -> bool {
    let ext_matches = path
        .and_then(|p| p.extension())
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("xdf"))
        .unwrap_or(false);
    ext_matches && infer_media_from_size(size).is_some()
}

/// Decode XDF bytes. Returns `(flat_sector_stream, inferred_media)`. Fails
/// if the byte count doesn't match a supported floppy geometry.
pub fn decode_xdf_bytes(bytes: &[u8]) -> Result<(Vec<u8>, FloppyMedia)> {
    decode_raw_floppy_bytes(bytes, FORMAT_LABEL)
}

/// Encode a flat sector stream as XDF. XDF has no header so the payload is
/// returned unchanged after a geometry-vs-length sanity check.
pub fn encode_xdf_bytes(flat: &[u8], geom: FloppyGeometry) -> Result<Vec<u8>> {
    encode_raw_floppy_bytes(flat, geom, FORMAT_LABEL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn looks_like_xdf_requires_ext_and_size() {
        let valid_size = FloppyMedia::Hd1232.geometry().flat_size();
        assert!(looks_like_xdf(Some(Path::new("disk.xdf")), valid_size));
        assert!(looks_like_xdf(Some(Path::new("DISK.XDF")), valid_size));
        // Right size, wrong extension -> no.
        assert!(!looks_like_xdf(Some(Path::new("disk.bin")), valid_size));
        // Right extension, wrong size -> no.
        assert!(!looks_like_xdf(Some(Path::new("disk.xdf")), 12345));
        // No path -> no (we'd misclassify size-matching raw blobs otherwise).
        assert!(!looks_like_xdf(None, valid_size));
    }

    #[test]
    fn decode_recognises_each_supported_geometry() {
        for media in super::super::floppy_geom::ALL_MEDIA {
            let flat = vec![0xAAu8; media.geometry().flat_size()];
            let (decoded, m) = decode_xdf_bytes(&flat).unwrap();
            assert_eq!(decoded, flat);
            assert_eq!(m, *media);
        }
    }

    #[test]
    fn decode_rejects_wrong_size() {
        let err = decode_xdf_bytes(&[0u8; 1000]).unwrap_err().to_string();
        assert!(
            err.contains("no supported floppy geometry"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn encode_round_trips_a_pattern() {
        let geom = FloppyMedia::Hd1232.geometry();
        let pattern: Vec<u8> = (0..geom.flat_size()).map(|i| (i & 0xFF) as u8).collect();
        let encoded = encode_xdf_bytes(&pattern, geom).unwrap();
        let (decoded, m) = decode_xdf_bytes(&encoded).unwrap();
        assert_eq!(decoded, pattern);
        assert_eq!(m, FloppyMedia::Hd1232);
    }

    #[test]
    fn encode_rejects_geometry_mismatch() {
        let geom = FloppyMedia::Hd1232.geometry();
        let err = encode_xdf_bytes(&[0u8; 1000], geom)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("mismatches geometry-derived"),
            "unexpected error message: {err}"
        );
    }
}
