//! End-to-end test for the XDF / HDM / DIM / D88 conversion matrix.
//!
//! Builds a deterministic 1.2 MB 2HD pattern, then runs every (source,
//! target) cell of the 4×4 matrix through the
//! [`convert_floppy_container`] engine. Identity cells (e.g. XDF -> XDF)
//! must produce a byte-identical copy; cross-format cells must round-trip
//! to byte-equal flat content.

use std::path::{Path, PathBuf};

use rusty_backup::rbformats::containers::{
    convert_floppy_container, floppy_geom::FloppyMedia, ContainerKind,
};

const FORMATS: &[(&str, ContainerKind)] = &[
    ("xdf", ContainerKind::Xdf),
    ("hdm", ContainerKind::Hdm),
    ("dim", ContainerKind::Dim),
    ("d88", ContainerKind::D88),
];

/// Deterministic 1.2 MB pattern. Byte 0x1B is forced to 0xFE so the D88
/// magic sniffer doesn't claim the raw flat patterns as D88 files when the
/// dispatch consults the first 256 bytes.
fn build_pattern() -> Vec<u8> {
    let geom = FloppyMedia::Hd1232.geometry();
    let mut out: Vec<u8> = (0..geom.flat_size())
        .map(|i| (i.wrapping_mul(31) & 0xFF) as u8)
        .collect();
    out[0x1B] = 0xFE;
    out
}

fn write_format(tempdir: &Path, ext: &str, pattern: &[u8]) -> PathBuf {
    use rusty_backup::rbformats::containers::{
        d88::{encode_d88_bytes, D88Media},
        dim::encode_dim_bytes,
        hdm::encode_hdm_bytes,
        xdf::encode_xdf_bytes,
    };
    let geom = FloppyMedia::Hd1232.geometry();
    let bytes = match ext {
        "xdf" => encode_xdf_bytes(pattern, geom).unwrap(),
        "hdm" => encode_hdm_bytes(pattern, geom).unwrap(),
        "dim" => encode_dim_bytes(pattern, geom).unwrap(),
        "d88" => encode_d88_bytes(
            pattern,
            geom.cyls,
            geom.heads,
            geom.spt,
            geom.sec_size as usize,
            D88Media::Dd2hd,
        )
        .unwrap(),
        other => panic!("unknown format {other}"),
    };
    let path = tempdir.join(format!("source.{ext}"));
    std::fs::write(&path, &bytes).unwrap();
    path
}

fn decode_to_flat(path: &Path) -> Vec<u8> {
    let bytes = std::fs::read(path).unwrap();
    let head_window = bytes.len().min(256);
    let kind = rusty_backup::rbformats::containers::detect_container_kind(
        &bytes[..head_window],
        Some(path),
    );
    match kind {
        ContainerKind::Xdf => {
            rusty_backup::rbformats::containers::xdf::decode_xdf_bytes(&bytes)
                .unwrap()
                .0
        }
        ContainerKind::Hdm => {
            rusty_backup::rbformats::containers::hdm::decode_hdm_bytes(&bytes)
                .unwrap()
                .0
        }
        ContainerKind::Dim => {
            rusty_backup::rbformats::containers::dim::decode_dim_bytes(&bytes)
                .unwrap()
                .0
        }
        ContainerKind::D88 => {
            rusty_backup::rbformats::containers::d88::decode_d88_bytes(&bytes).unwrap()
        }
        other => panic!("decode_to_flat: not a floppy container ({:?})", other),
    }
}

#[test]
fn sixteen_cell_round_trip_all_formats() {
    let tempdir = tempfile::tempdir().unwrap();
    let temp_path = tempdir.path().to_path_buf();
    let pattern = build_pattern();

    for (src_ext, src_kind) in FORMATS {
        // Build a fresh source file in each format.
        let src_path = write_format(&temp_path, src_ext, &pattern);

        for (dst_ext, dst_kind) in FORMATS {
            let dst_path = temp_path.join(format!("out_{src_ext}_to_{dst_ext}.{dst_ext}"));
            let report = convert_floppy_container(&src_path, &dst_path, *dst_kind)
                .unwrap_or_else(|e| panic!("{src_ext} -> {dst_ext} failed: {e:#}"));
            assert_eq!(report.source, *src_kind);
            assert_eq!(report.target, *dst_kind);
            assert_eq!(report.media, FloppyMedia::Hd1232);

            if src_ext == dst_ext {
                // Identity: byte-equal copy.
                let src_bytes = std::fs::read(&src_path).unwrap();
                let dst_bytes = std::fs::read(&dst_path).unwrap();
                assert_eq!(
                    src_bytes, dst_bytes,
                    "{src_ext} -> {dst_ext}: identity byte mismatch"
                );
                assert!(report.identity);
            } else {
                // Cross-format: decoded flat must match the original pattern.
                let decoded = decode_to_flat(&dst_path);
                assert_eq!(
                    decoded, pattern,
                    "{src_ext} -> {dst_ext}: flat round-trip mismatch"
                );
                assert!(!report.identity);
            }
        }
    }
}

#[test]
fn dim_target_rejects_dd640_geometry() {
    // 640 KB 2DD is not representable in DIFC DIM. Build a 640 KB pattern
    // (size unique among the four geometries), wrap it as XDF, then try to
    // convert -> DIM. The conversion must fail with a clear error.
    let geom = FloppyMedia::Dd640.geometry();
    let mut pattern: Vec<u8> = (0..geom.flat_size())
        .map(|i| (i.wrapping_mul(7) & 0xFF) as u8)
        .collect();
    // Keep byte 0x1B != D88 media values for safety.
    pattern[0x1B] = 0xFE;

    let tempdir = tempfile::tempdir().unwrap();
    let xdf_path = tempdir.path().join("dd640.xdf");
    std::fs::write(
        &xdf_path,
        rusty_backup::rbformats::containers::xdf::encode_xdf_bytes(&pattern, geom).unwrap(),
    )
    .unwrap();

    let dim_path = tempdir.path().join("dd640.dim");
    let err = convert_floppy_container(&xdf_path, &dim_path, ContainerKind::Dim)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("640 KB"),
        "expected 640 KB rejection, got: {err}"
    );
}
