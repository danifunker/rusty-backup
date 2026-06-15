//! `rb-cli` regression test for CP/M **floppy** cores (Wave 3 spillover).
//!
//! Drives the committed `test_cpm_amstrad_data.dsk.zst` fixture — a flat
//! Amstrad CPC data-format CP/M disc written by `cpmtools` (the byte-truth
//! oracle), 40 trk × 9 spt × 512 B = 184320 bytes. It holds two known
//! files: HELLO.TXT and NOTE.TXT.
//!
//! ## What this regresses
//!
//! CP/M discs carry no on-disk FS signature, so dispatch needs an explicit
//! `--fs-type cpm:<preset>`. The Altair test (`cli_altair.rs`) already
//! covered that, but only because the Altair 8" geometry (256256 B) is a
//! whitelisted "floppy size" in `partition::detect`, so detection fell
//! through gracefully. The Amstrad data format (184320 B) is **not**
//! whitelisted, so `PartitionTable::detect` hard-errored ("invalid MBR")
//! before the `--fs-type` override could apply — breaking the Wave-3 CP/M
//! floppy cores. The fix makes a detection failure non-fatal when the user
//! forces `--fs-type` (see `cli::resolve::resolve_with_override`); this
//! test pins that the Amstrad disc now round-trips end-to-end.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_rb-cli"))
}

fn run(args: &[&str]) -> std::process::Output {
    let out = Command::new(cli_bin())
        .args(args)
        .output()
        .expect("spawn rb-cli");
    if !out.status.success() {
        panic!(
            "command {args:?} failed: status={:?}\nstdout:\n{}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
    out
}

fn fixture_to(dst: &Path) {
    let compressed =
        std::fs::read("tests/fixtures/test_cpm_amstrad_data.dsk.zst").expect("read fixture");
    let mut decoder =
        zstd::stream::read::Decoder::new(std::io::Cursor::new(compressed)).expect("zstd decoder");
    let mut bytes = Vec::new();
    decoder.read_to_end(&mut bytes).expect("decompress");
    std::fs::write(dst, &bytes).expect("write fixture out");
}

const FS: &str = "cpm:amstrad_data";

#[test]
fn ls_lists_files_on_non_whitelisted_floppy_size() {
    let tmp = tempfile::tempdir().unwrap();
    let img = tmp.path().join("amstrad.dsk");
    fixture_to(&img);
    let out = run(&["ls", "--fs-type", FS, img.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("HELLO.TXT"),
        "ls missing HELLO.TXT:\n{stdout}"
    );
    assert!(
        stdout.contains("NOTE.TXT"),
        "ls missing NOTE.TXT:\n{stdout}"
    );
}

#[test]
fn ls_without_fs_type_still_errors_clearly() {
    // No `--fs-type`: detection must still fail (CP/M has no signature),
    // and we must NOT silently mis-open it. The point of the fix is that
    // the bypass only kicks in when the user forces a type.
    let tmp = tempfile::tempdir().unwrap();
    let img = tmp.path().join("amstrad.dsk");
    fixture_to(&img);
    let out = Command::new(cli_bin())
        .args(["ls", img.to_str().unwrap()])
        .output()
        .expect("spawn rb-cli");
    assert!(
        !out.status.success(),
        "ls without --fs-type should fail on a signatureless CP/M disc"
    );
}

#[test]
fn put_get_rm_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let img = tmp.path().join("amstrad.dsk");
    fixture_to(&img);

    // The cpmtools-written HELLO.TXT reads back as authored.
    let hello = tmp.path().join("hello.out");
    run(&[
        "get",
        "--fs-type",
        FS,
        img.to_str().unwrap(),
        "HELLO.TXT",
        hello.to_str().unwrap(),
    ]);
    assert_eq!(
        std::fs::read(&hello).unwrap(),
        b"Hello from CP/M cpmtools fixture."
    );

    // Add a file, read it back byte-exact, delete it; seeded files survive.
    let payload = b"GREETINGS FROM RB-CLI\r\n".to_vec();
    let host = tmp.path().join("greet.txt");
    std::fs::write(&host, &payload).unwrap();
    run(&[
        "put",
        "--fs-type",
        FS,
        img.to_str().unwrap(),
        host.to_str().unwrap(),
        "GREET.TXT",
    ]);

    let back = tmp.path().join("greet.out");
    run(&[
        "get",
        "--fs-type",
        FS,
        img.to_str().unwrap(),
        "GREET.TXT",
        back.to_str().unwrap(),
    ]);
    assert_eq!(std::fs::read(&back).unwrap(), payload);

    run(&["rm", "--fs-type", FS, img.to_str().unwrap(), "GREET.TXT"]);
    let after =
        String::from_utf8_lossy(&run(&["ls", "--fs-type", FS, img.to_str().unwrap()]).stdout)
            .into_owned();
    assert!(
        !after.contains("GREET.TXT"),
        "rm left file behind:\n{after}"
    );
    assert!(
        after.contains("HELLO.TXT"),
        "rm clobbered HELLO.TXT:\n{after}"
    );
}
