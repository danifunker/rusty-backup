//! CBM DOS fsck (VALIDATE) cross-validated against **real `c1541 validate`
//! output** — the same offline oracle the `.d64`/`.g71` fixtures already use.
//!
//! Each fixture pair was produced by VICE `c1541` (see
//! `scratchpad/cbmfsck/gen.py` in the working notes):
//!   1. `c1541 -format ... <variant> base.img -write file1 -write file2`
//!      → the clean `*_base.*` image. `c1541 validate` is a byte-exact no-op
//!      on it, so it is ground truth.
//!   2. a defined BAM corruption applied to a copy → the `*_<kind>.*` image.
//!      `c1541 validate` restores each corruption to `*_base.*` byte-for-byte.
//!
//! So the contract this test asserts is exactly the oracle's:
//!   - our checker flags the corruption,
//!   - our repair rewrites the image to `*_base.*` — i.e. identical to what
//!     `c1541 validate` produces — sector for sector.
//!
//! Variants covered: 1541 (`.d64`), 1571 (`.d71`), 1581 (`.d81`),
//! 8050 (`.d80`), 8250 (`.d82`). The 1541 fixtures also exercise all three
//! issue kinds (leaked block, used-but-free block, wrong free count).

use std::io::{Cursor, Read};

use rusty_backup::fs::cbm::CbmFilesystem;
use rusty_backup::fs::filesystem::{EditableFilesystem, Filesystem};

fn load(name: &str) -> Vec<u8> {
    let path = format!("tests/fixtures/{name}");
    let compressed = std::fs::read(&path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let mut dec = zstd::stream::read::Decoder::new(Cursor::new(compressed)).expect("zstd");
    let mut out = Vec::new();
    dec.read_to_end(&mut out).expect("decompress");
    out
}

/// The clean image checks clean — no false positives from our recompute.
fn assert_clean(base_fixture: &str) {
    let base = load(base_fixture);
    let mut fs = CbmFilesystem::open(Cursor::new(base), 0).expect("open base");
    let report = fs.fsck().expect("fsck supported").expect("fsck ran");
    assert!(
        report.errors.is_empty(),
        "{base_fixture}: clean image reported errors: {:?}",
        report.errors
    );
}

/// A corrupt image is flagged, then repaired to the exact bytes `c1541
/// validate` produces (== the committed base image).
fn assert_repairs_to_base(corrupt_fixture: &str, base_fixture: &str) {
    let corrupt = load(corrupt_fixture);
    let base = load(base_fixture);
    assert_ne!(
        corrupt, base,
        "{corrupt_fixture} must actually differ from its base"
    );

    // 1) Detection.
    {
        let mut fs = CbmFilesystem::open(Cursor::new(corrupt.clone()), 0).expect("open corrupt");
        let report = fs.fsck().expect("fsck supported").expect("fsck ran");
        assert!(
            !report.errors.is_empty() && report.repairable,
            "{corrupt_fixture}: expected repairable errors, got {report:?}"
        );
    }

    // 2) Repair rewrites the BAM to the c1541-validated bytes.
    let mut fs = CbmFilesystem::open(Cursor::new(corrupt), 0).expect("reopen corrupt");
    let rep = fs.repair().expect("repair ran");
    assert!(
        !rep.fixes_applied.is_empty() && rep.fixes_failed.is_empty(),
        "{corrupt_fixture}: repair report unexpected: {rep:?}"
    );
    let repaired = fs.into_inner().into_inner();
    assert_eq!(
        repaired, base,
        "{corrupt_fixture}: repaired image must be byte-identical to c1541 \
         validate output ({base_fixture})"
    );

    // 3) The repaired image now checks clean.
    let mut fs2 = CbmFilesystem::open(Cursor::new(repaired), 0).expect("open repaired");
    let report = fs2.fsck().expect("fsck supported").expect("fsck ran");
    assert!(
        report.errors.is_empty(),
        "{corrupt_fixture}: repaired image still has errors: {:?}",
        report.errors
    );
}

/// Assert a specific issue code appears for a corruption kind.
fn assert_flags_code(corrupt_fixture: &str, code: &str) {
    let corrupt = load(corrupt_fixture);
    let mut fs = CbmFilesystem::open(Cursor::new(corrupt), 0).expect("open corrupt");
    let report = fs.fsck().expect("fsck supported").expect("fsck ran");
    assert!(
        report.errors.iter().any(|e| e.code == code),
        "{corrupt_fixture}: expected issue code {code}, got {:?}",
        report.errors.iter().map(|e| &e.code).collect::<Vec<_>>()
    );
}

#[test]
fn clean_images_check_clean_all_variants() {
    for base in [
        "cbm_fsck_1541_base.d64.zst",
        "cbm_fsck_1571_base.d71.zst",
        "cbm_fsck_1581_base.d81.zst",
        "cbm_fsck_8050_base.d80.zst",
        "cbm_fsck_8250_base.d82.zst",
    ] {
        assert_clean(base);
    }
}

#[test]
fn repair_matches_c1541_validate_all_variants() {
    let pairs = [
        (
            "cbm_fsck_1541_freecount.d64.zst",
            "cbm_fsck_1541_base.d64.zst",
        ),
        (
            "cbm_fsck_1571_freecount.d71.zst",
            "cbm_fsck_1571_base.d71.zst",
        ),
        (
            "cbm_fsck_1581_freecount.d81.zst",
            "cbm_fsck_1581_base.d81.zst",
        ),
        (
            "cbm_fsck_8050_freecount.d80.zst",
            "cbm_fsck_8050_base.d80.zst",
        ),
        (
            "cbm_fsck_8250_freecount.d82.zst",
            "cbm_fsck_8250_base.d82.zst",
        ),
    ];
    for (corrupt, base) in pairs {
        assert_repairs_to_base(corrupt, base);
    }
}

#[test]
fn d64_exercises_all_three_issue_kinds() {
    let base = "cbm_fsck_1541_base.d64.zst";
    assert_repairs_to_base("cbm_fsck_1541_leak.d64.zst", base);
    assert_repairs_to_base("cbm_fsck_1541_usedfree.d64.zst", base);
    assert_repairs_to_base("cbm_fsck_1541_freecount.d64.zst", base);

    assert_flags_code("cbm_fsck_1541_leak.d64.zst", "BamBlockLeaked");
    assert_flags_code("cbm_fsck_1541_usedfree.d64.zst", "BamBlockUsedButFree");
    assert_flags_code("cbm_fsck_1541_freecount.d64.zst", "BamFreeCountMismatch");
}
