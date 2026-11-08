//! End-to-end test for the XDF / HDM / DIM / D88 conversion matrix.
//!
//! Builds a deterministic 1.2 MB 2HD pattern, then runs every (source,
//! target) cell of the 4×4 matrix through the
//! [`convert_floppy_container`] engine. Identity cells (e.g. XDF -> XDF)
//! must produce a byte-identical copy; cross-format cells must round-trip
//! to byte-equal flat content.

use std::path::{Path, PathBuf};

use dsk_floppy::floppy_geom::FloppyMedia;
use rusty_backup::rbformats::containers::{convert_floppy_container, ContainerKind};

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
    write_format_with_stem(tempdir, "source", ext, pattern)
}

fn write_format_with_stem(tempdir: &Path, stem: &str, ext: &str, pattern: &[u8]) -> PathBuf {
    use dsk_floppy::d88::{encode_d88_bytes, D88Media};
    use dsk_floppy::dim::encode_dim_bytes;
    use dsk_floppy::hdm::encode_hdm_bytes;
    use dsk_floppy::xdf::encode_xdf_bytes;
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
    let path = tempdir.join(format!("{stem}.{ext}"));
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
        ContainerKind::Xdf => dsk_floppy::xdf::decode_xdf_bytes(&bytes).unwrap().0,
        ContainerKind::Hdm => dsk_floppy::hdm::decode_hdm_bytes(&bytes).unwrap().0,
        ContainerKind::Dim => dsk_floppy::dim::decode_dim_bytes(&bytes).unwrap().0,
        ContainerKind::D88 => dsk_floppy::d88::decode_d88_bytes(&bytes).unwrap(),
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
fn convert_routes_floppy_targets() {
    // A folder of XDF / DIM / HDM floppies converted to D88 through the Convert controller must
    // come out as real D88 track tables (not raw blobs) that decode back to the same sectors.
    use rusty_backup::controller::convert::{
        purpose, ChdChoices, ConvertAction, ConvertController, Format, ImageKind,
    };
    use rusty_backup::controller::Controller;

    let tempdir = tempfile::tempdir().unwrap();
    let temp_path = tempdir.path().to_path_buf();
    let pattern = build_pattern();
    let src_dir = temp_path.join("src");
    let out_dir = temp_path.join("out");
    std::fs::create_dir_all(&src_dir).unwrap();
    write_format_with_stem(&src_dir, "disk_a", "xdf", &pattern);
    write_format_with_stem(&src_dir, "disk_b", "dim", &pattern);
    write_format_with_stem(&src_dir, "disk_c", "hdm", &pattern);

    let mut ctl = ConvertController::new(std::sync::Arc::new(rusty_backup::model::job::NoNotify));
    ctl.apply(ConvertAction::SetSource(src_dir.clone()));
    ctl.wait_idle();
    ctl.tick();
    assert_eq!(ctl.items().len(), 3);
    ctl.apply(ConvertAction::SetFormat(ImageKind::Floppy, Format::D88));
    ctl.apply(ConvertAction::Picked {
        purpose: purpose::OUTPUT_FOLDER,
        path: Some(out_dir.clone()),
    });
    ctl.apply(ConvertAction::Start(ChdChoices::default()));
    ctl.wait_idle();
    ctl.tick();
    let log: Vec<String> = ctl.take_log().into_iter().map(|(_, m)| m).collect();
    let r = ctl
        .last_result()
        .cloned()
        .expect("a result")
        .expect("it ran");
    assert_eq!((r.succeeded, r.failed), (3, 0), "{log:?}");

    let mut count = 0;
    for entry in std::fs::read_dir(&out_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) == Some("d88") {
            let bytes = std::fs::read(&path).unwrap();
            let flat = dsk_floppy::d88::decode_d88_bytes(&bytes).unwrap();
            assert_eq!(flat, pattern, "converted D88 didn't decode to pattern");
            count += 1;
        }
    }
    assert_eq!(count, 3, "expected 3 D88 outputs, got {count}");
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
        dsk_floppy::xdf::encode_xdf_bytes(&pattern, geom).unwrap(),
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
