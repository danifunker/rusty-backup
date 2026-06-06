//! HDM — PC-98 / DiskExplorer headerless floppy dump.
//!
//! On-disk layout is identical to [`super::xdf`] (raw flat sector stream,
//! no header, no per-track records). The two formats are kept as separate
//! modules so the extension dispatch and log lines stay self-explanatory;
//! both delegate to the shared [`super::floppy_geom`] raw-flat helper.

use std::path::Path;

use anyhow::Result;

use super::floppy_geom::{
    decode_raw_floppy_bytes, encode_raw_floppy_bytes, infer_media_from_size, FloppyGeometry,
    FloppyMedia,
};

const FORMAT_LABEL: &str = "HDM";

/// Cheap detection: path extension is `.hdm` (case-insensitive) AND the byte
/// length matches one of the supported floppy geometries.
pub fn looks_like_hdm(path: Option<&Path>, size: usize) -> bool {
    let ext_matches = path
        .and_then(|p| p.extension())
        .and_then(|e| e.to_str())
        .map(|s| s.eq_ignore_ascii_case("hdm"))
        .unwrap_or(false);
    ext_matches && infer_media_from_size(size).is_some()
}

/// Decode HDM bytes. Returns `(flat_sector_stream, inferred_media)`. Fails
/// if the byte count doesn't match a supported floppy geometry.
pub fn decode_hdm_bytes(bytes: &[u8]) -> Result<(Vec<u8>, FloppyMedia)> {
    decode_raw_floppy_bytes(bytes, FORMAT_LABEL)
}

/// Encode a flat sector stream as HDM. Passthrough after a geometry-vs-length
/// sanity check.
pub fn encode_hdm_bytes(flat: &[u8], geom: FloppyGeometry) -> Result<Vec<u8>> {
    encode_raw_floppy_bytes(flat, geom, FORMAT_LABEL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn looks_like_hdm_requires_ext_and_size() {
        let valid_size = FloppyMedia::Hd1232.geometry().flat_size();
        assert!(looks_like_hdm(Some(Path::new("disk.hdm")), valid_size));
        assert!(looks_like_hdm(Some(Path::new("DISK.HDM")), valid_size));
        assert!(!looks_like_hdm(Some(Path::new("disk.xdf")), valid_size));
        assert!(!looks_like_hdm(Some(Path::new("disk.hdm")), 12345));
        assert!(!looks_like_hdm(None, valid_size));
    }

    #[test]
    fn decode_recognises_each_supported_geometry() {
        for media in super::super::floppy_geom::ALL_MEDIA {
            let flat = vec![0x55u8; media.geometry().flat_size()];
            let (decoded, m) = decode_hdm_bytes(&flat).unwrap();
            assert_eq!(decoded, flat);
            assert_eq!(m, *media);
        }
    }

    #[test]
    fn decode_rejects_wrong_size() {
        let err = decode_hdm_bytes(&[0u8; 100]).unwrap_err().to_string();
        assert!(err.contains("no supported floppy geometry"));
    }

    #[test]
    fn encode_round_trips_a_pattern() {
        let geom = FloppyMedia::Dd720.geometry();
        let pattern: Vec<u8> = (0..geom.flat_size()).map(|i| (i & 0xFF) as u8).collect();
        let encoded = encode_hdm_bytes(&pattern, geom).unwrap();
        let (decoded, m) = decode_hdm_bytes(&encoded).unwrap();
        assert_eq!(decoded, pattern);
        assert_eq!(m, FloppyMedia::Dd720);
    }

    #[test]
    fn xdf_and_hdm_payloads_are_byte_compatible() {
        // The two formats are byte-identical on disk; only the extension
        // routing and log label differ. Encoded outputs must round-trip
        // through the other decoder.
        let geom = FloppyMedia::Hd1232.geometry();
        let pattern: Vec<u8> = (0..geom.flat_size()).map(|i| (i & 0xFF) as u8).collect();
        let xdf_bytes = super::super::xdf::encode_xdf_bytes(&pattern, geom).unwrap();
        let (decoded_via_hdm, _) = decode_hdm_bytes(&xdf_bytes).unwrap();
        assert_eq!(decoded_via_hdm, pattern);
        let hdm_bytes = encode_hdm_bytes(&pattern, geom).unwrap();
        let (decoded_via_xdf, _) = super::super::xdf::decode_xdf_bytes(&hdm_bytes).unwrap();
        assert_eq!(decoded_via_xdf, pattern);
    }
}
